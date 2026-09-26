use std::fs::File;
use std::io::Write;
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use clickhouse::Client;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use kymo_local_runtime_core::artifacts::{SupportedTarget, for_target};
use kymo_local_runtime_core::profile::{
    CLICKHOUSE_USER, clickhouse_config_xml, clickhouse_users_xml,
};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::pki_types::CertificateDer;
use rustls::{ClientConfig, RootCertStore};
use tokio::process::{Child, Command};

const PASSWORD: &str = "kymo-qualification-only";

pub async fn qualify(root: &Path, binary: &Path) -> Result<()> {
    ensure!(
        binary.is_file(),
        "ClickHouse binary does not exist: {}",
        binary.display()
    );
    super::uds::make_private_dir(root)?;
    for name in ["data", "tmp", "user-files", "format-schemas"] {
        super::uds::make_private_dir(&root.join(name))?;
    }
    let port = unused_loopback_port()?;
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let cert_path = root.join("server.pem");
    let key_path = root.join("server.key");
    write_private(&cert_path, cert.pem().as_bytes())?;
    write_private(&key_path, signing_key.serialize_pem().as_bytes())?;
    write_private(
        &root.join("users.xml"),
        clickhouse_users_xml(PASSWORD).as_bytes(),
    )?;
    write_private(
        &root.join("config.xml"),
        clickhouse_config_xml(root, port, &cert_path, &key_path, &root.join("users.xml"))?
            .as_bytes(),
    )?;

    let log_path = root.join("clickhouse.log");
    let log = File::create(&log_path)?;
    let mut command = Command::new(binary);
    command
        .arg("server")
        .arg(format!(
            "--config-file={}",
            root.join("config.xml").display()
        ))
        .env("CLICKHOUSE_WATCHDOG_ENABLE", "0")
        .current_dir(root)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .kill_on_drop(true);
    command.as_std_mut().process_group(0);
    let mut child = command
        .spawn()
        .context("start ClickHouse qualification server")?;
    let client = pinned_client(cert.der().clone(), port, Some((CLICKHOUSE_USER, PASSWORD)))?;
    let result = exercise_started(&mut child, &client, cert.der().clone(), port, &log_path).await;
    let _ = client.query("SYSTEM SHUTDOWN").execute().await;
    let stopped = wait_for_exit(&mut child, Duration::from_secs(20)).await;
    if stopped.is_err() {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
    result?;
    stopped?;
    Ok(())
}

async fn exercise_started(
    child: &mut Child,
    client: &Client,
    cert: CertificateDer<'static>,
    port: u16,
    log_path: &Path,
) -> Result<()> {
    let mut version = None;
    for _ in 0..240 {
        if let Some(status) = child.try_wait()? {
            bail!(
                "ClickHouse exited during startup with {status}:\n{}",
                std::fs::read_to_string(log_path).unwrap_or_default()
            );
        }
        match client.query("SELECT version()").fetch_one::<String>().await {
            Ok(value) => {
                version = Some(value);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
    let version = version.context("ClickHouse pinned HTTPS endpoint did not become ready")?;
    let expected_version = for_target(SupportedTarget::current()?)?.clickhouse.version;
    ensure!(
        version == expected_version,
        "unexpected ClickHouse version {version}"
    );

    let endpoints = listening_tcp_endpoints(child).await?;
    ensure!(
        endpoints == [format!("127.0.0.1:{port}")],
        "ClickHouse exposed unexpected TCP listeners: {endpoints:?}"
    );

    let unauthenticated = pinned_client(cert, port, None)?
        .query("SELECT 1")
        .fetch_one::<u8>()
        .await;
    ensure!(
        unauthenticated.is_err(),
        "ClickHouse accepted an unauthenticated query"
    );

    let wrong_cert = generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let unpinned = pinned_client(
        wrong_cert.cert.der().clone(),
        port,
        Some((CLICKHOUSE_USER, PASSWORD)),
    )?
    .query("SELECT 1")
    .fetch_one::<u8>()
    .await;
    let certificate_error = match unpinned {
        Ok(_) => bail!("ClickHouse accepted the wrong certificate"),
        Err(error) => anyhow::Error::new(error),
    };
    let certificate_error_chain = format!("{certificate_error:#}");
    ensure!(
        certificate_error_chain.contains("invalid peer certificate"),
        "wrong ClickHouse certificate failed for an unexpected reason: {certificate_error_chain}"
    );

    let plaintext = plaintext_client(port, Some((CLICKHOUSE_USER, PASSWORD)))
        .query("SELECT 1")
        .fetch_one::<u8>()
        .await;
    ensure!(
        plaintext.is_err(),
        "ClickHouse TLS endpoint accepted plaintext HTTP"
    );
    Ok(())
}

fn pinned_client(
    cert: CertificateDer<'static>,
    port: u16,
    credentials: Option<(&str, &str)>,
) -> Result<Client> {
    let mut roots = RootCertStore::empty();
    roots
        .add(cert)
        .context("add pinned ClickHouse certificate")?;
    pinned_client_for_url(roots, format!("https://localhost:{port}"), credentials)
}

fn pinned_client_for_url(
    roots: RootCertStore,
    url: String,
    credentials: Option<(&str, &str)>,
) -> Result<Client> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let tls = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let mut http = HttpConnector::new();
    http.enforce_http(false);
    let connector = HttpsConnectorBuilder::new()
        .with_tls_config(tls)
        .https_only()
        .enable_http1()
        .wrap_connector(http);
    let hyper = HyperClient::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(2))
        .build(connector);
    let mut client = Client::with_http_client(hyper).with_url(url);
    if let Some((user, password)) = credentials {
        client = client.with_user(user).with_password(password);
    }
    Ok(client)
}

fn plaintext_client(port: u16, credentials: Option<(&str, &str)>) -> Client {
    let mut client = Client::default().with_url(format!("http://localhost:{port}"));
    if let Some((user, password)) = credentials {
        client = client.with_user(user).with_password(password);
    }
    client
}

fn write_private(path: &Path, contents: &[u8]) -> Result<()> {
    let mut file = File::create(path)?;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    file.write_all(contents)?;
    file.sync_all()?;
    Ok(())
}

fn unused_loopback_port() -> Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    Ok(listener.local_addr()?.port())
}

async fn wait_for_exit(child: &mut Child, timeout: Duration) -> Result<()> {
    match tokio::time::timeout(timeout, child.wait()).await {
        Ok(status) => {
            let status = status?;
            ensure!(
                status.success(),
                "ClickHouse exited unsuccessfully: {status}"
            );
            Ok(())
        }
        Err(_) => bail!("ClickHouse did not stop within {timeout:?}"),
    }
}

async fn listening_tcp_endpoints(child: &Child) -> Result<Vec<String>> {
    let pid = child.id().context("ClickHouse child has no process id")?;
    let output = Command::new("lsof")
        .args([
            "-nP",
            "-a",
            "-p",
            &pid.to_string(),
            "-iTCP",
            "-sTCP:LISTEN",
            "-Fn",
        ])
        .output()
        .await
        .context("run lsof for ClickHouse listener qualification")?;
    ensure!(
        output.status.success(),
        "lsof failed while inspecting ClickHouse:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.strip_prefix('n'))
        .map(str::to_owned)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn plaintext_probe_reaches_the_target_listener() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let query = tokio::spawn(async move {
            plaintext_client(port, Some(("user", "password")))
                .query("SELECT 1")
                .fetch_one::<u8>()
                .await
        });

        let (stream, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .expect("plaintext query never reached the TCP listener")
            .unwrap();
        drop(stream);

        let result = tokio::time::timeout(Duration::from_secs(1), query)
            .await
            .expect("plaintext query did not finish after disconnect")
            .unwrap();
        assert!(result.is_err());
    }
}
