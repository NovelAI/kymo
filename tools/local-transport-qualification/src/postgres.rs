use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use kymo_local_runtime_core::artifacts::{SupportedTarget, for_target};
use kymo_local_runtime_core::profile::postgresql_configuration;
use postgresql_embedded::{PostgreSQL, Settings, SettingsBuilder, VersionReq};
use sqlx::postgres::PgPoolOptions;
use tokio::net::TcpStream;

const MARKER_PREFIX: &str = "kymo-local-transport-qualification-v1";

pub async fn qualify(root: &Path) -> Result<String> {
    // A retained --state-dir may contain an earlier qualification row. Use a fresh value so only this invocation's pre-restart write can satisfy the post-restart persistence check.
    let marker = qualification_marker(
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("qualification clock is before the Unix epoch")?
            .as_nanos(),
    );
    super::uds::make_private_dir(root)?;
    let socket_dir = root.join("socket");
    super::uds::make_private_dir(&socket_dir)?;
    let version = for_target(SupportedTarget::current()?)?.postgresql.version;
    let settings = SettingsBuilder::new()
        .version(VersionReq::parse(&format!("={version}"))?)
        .installation_dir(root.join("installation"))
        .password_file(root.join(".pgpass"))
        .data_dir(root.join("data"))
        .socket_dir(&socket_dir)
        .username("postgres")
        .password("kymo-qualification-only")
        .temporary(false)
        .timeout(Some(Duration::from_secs(180)))
        .configuration(postgresql_configuration().into_iter().collect())
        .build();

    let mut postgres = PostgreSQL::new(settings);
    postgres
        .setup()
        .await
        .context("install and initialize PostgreSQL")?;
    postgres
        .start()
        .await
        .context("start socket-only PostgreSQL")?;
    let restart_settings = postgres.settings().clone();
    let first_result =
        exercise_started(&restart_settings, &socket_dir, &version, &marker, true).await;
    let first_stop = postgres
        .stop()
        .await
        .context("stop first PostgreSQL process");
    first_result?;
    first_stop?;

    let mut postgres = PostgreSQL::new(restart_settings);
    postgres
        .start()
        .await
        .context("restart persistent PostgreSQL data")?;
    let restart_settings = postgres.settings().clone();
    let second_result =
        exercise_started(&restart_settings, &socket_dir, &version, &marker, false).await;
    let second_stop = postgres
        .stop()
        .await
        .context("stop restarted PostgreSQL process");
    second_result?;
    second_stop?;
    reject_tcp(restart_settings.port).await?;
    Ok(version)
}

async fn exercise_started(
    settings: &Settings,
    socket_dir: &Path,
    catalog_version: &str,
    marker: &str,
    initialize: bool,
) -> Result<()> {
    reject_tcp(settings.port).await?;
    let socket = socket_dir.join(format!(".s.PGSQL.{}", settings.port));
    let metadata = std::fs::symlink_metadata(&socket)
        .with_context(|| format!("inspect PostgreSQL socket {}", socket.display()))?;
    ensure!(
        metadata.file_type().is_socket(),
        "PostgreSQL endpoint is not a Unix socket"
    );
    ensure!(
        metadata.permissions().mode() & 0o077 == 0,
        "PostgreSQL socket grants group/other permissions"
    );
    ensure!(
        std::fs::metadata(socket_dir)?.permissions().mode() & 0o077 == 0,
        "PostgreSQL socket directory grants group/other permissions"
    );

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&settings.url("postgres"))
        .await
        .context("connect to PostgreSQL through generated Unix-socket URL")?;
    let socket_connection: bool = sqlx::query_scalar("SELECT inet_server_addr() IS NULL")
        .fetch_one(&pool)
        .await?;
    ensure!(
        socket_connection,
        "PostgreSQL connection unexpectedly used TCP"
    );
    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&pool)
        .await?;
    ensure!(
        version == displayed_version(catalog_version),
        "unexpected PostgreSQL version {version}; catalog selects {catalog_version}"
    );
    if initialize {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS kymo_transport_qualification (marker TEXT PRIMARY KEY)",
        )
        .execute(&pool)
        .await?;
        sqlx::query("INSERT INTO kymo_transport_qualification (marker) VALUES ($1)")
            .bind(marker)
            .execute(&pool)
            .await?;
    }
    let persisted: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM kymo_transport_qualification WHERE marker = $1)",
    )
    .bind(marker)
    .fetch_one(&pool)
    .await?;
    ensure!(persisted, "PostgreSQL marker did not survive restart");
    if !initialize {
        sqlx::query("DELETE FROM kymo_transport_qualification WHERE marker = $1")
            .bind(marker)
            .execute(&pool)
            .await?;
    }
    pool.close().await;
    Ok(())
}

fn qualification_marker(pid: u32, unix_nanos: u128) -> String {
    format!("{MARKER_PREFIX}-{pid}-{unix_nanos}")
}

fn displayed_version(catalog_version: &str) -> &str {
    catalog_version
        .strip_suffix(".0")
        .unwrap_or(catalog_version)
}

async fn reject_tcp(port: u16) -> Result<()> {
    let attempt = tokio::time::timeout(
        Duration::from_millis(500),
        TcpStream::connect(("127.0.0.1", port)),
    )
    .await;
    ensure!(
        !matches!(attempt, Ok(Ok(_))),
        "PostgreSQL accepted TCP on 127.0.0.1:{port}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{displayed_version, qualification_marker};

    #[test]
    fn catalog_version_matches_postgres_display_version() {
        assert_eq!(displayed_version("17.10.0"), "17.10");
        assert_eq!(displayed_version("18.0.0"), "18.0");
        assert_eq!(displayed_version("18.1"), "18.1");
    }

    #[test]
    fn persistence_markers_distinguish_qualification_invocations() {
        assert_ne!(qualification_marker(7, 10), qualification_marker(7, 11));
        assert_ne!(qualification_marker(7, 10), qualification_marker(8, 10));
    }
}
