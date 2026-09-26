#[cfg(not(unix))]
compile_error!("local transport qualification requires a Unix target");

mod clickhouse;
mod postgres;
mod uds;

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

struct Arguments {
    clickhouse_binary: PathBuf,
    python: PathBuf,
    state_dir: Option<PathBuf>,
}

fn arguments() -> Result<Arguments> {
    let mut clickhouse_binary = None;
    let mut python = PathBuf::from("python3");
    let mut state_dir = None;
    let mut args = std::env::args_os().skip(1);
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--clickhouse-binary") => {
                clickhouse_binary = Some(PathBuf::from(
                    args.next().context("--clickhouse-binary requires a path")?,
                ));
            }
            Some("--python") => {
                python = PathBuf::from(args.next().context("--python requires a path")?);
            }
            Some("--state-dir") => {
                state_dir = Some(PathBuf::from(
                    args.next().context("--state-dir requires a path")?,
                ));
            }
            Some("--help" | "-h") => {
                println!(
                    "usage: kymo-local-transport-qualification --clickhouse-binary PATH [--python PATH] [--state-dir PATH]"
                );
                std::process::exit(0);
            }
            _ => bail!("unknown argument: {}", arg.to_string_lossy()),
        }
    }
    Ok(Arguments {
        clickhouse_binary: clickhouse_binary.context("--clickhouse-binary is required")?,
        python,
        state_dir,
    })
}

fn canonicalize_existing(path: &Path, label: &str) -> Result<PathBuf> {
    std::fs::canonicalize(path).with_context(|| format!("resolve {label} {}", path.display()))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = arguments()?;
    let clickhouse_binary = canonicalize_existing(&args.clickhouse_binary, "ClickHouse binary")?;
    anyhow::ensure!(
        clickhouse_binary.is_file(),
        "ClickHouse binary does not exist: {}",
        clickhouse_binary.display()
    );
    let temporary;
    let root = if let Some(root) = args.state_dir {
        std::fs::create_dir_all(&root)
            .with_context(|| format!("create qualification state root {}", root.display()))?;
        canonicalize_existing(&root, "qualification state root")?
    } else {
        temporary = tempfile::Builder::new()
            .prefix("m2q")
            .tempdir_in("/tmp")
            .context("create short qualification state root under /tmp")?;
        canonicalize_existing(temporary.path(), "qualification state root")?
    };
    uds::make_private_dir(&root)?;

    let postgres_version = postgres::qualify(&root.join("postgres")).await?;
    println!(
        "PASS PostgreSQL {postgres_version} socket-only startup, persistence, and permissions"
    );

    clickhouse::qualify(&root.join("clickhouse"), &clickhouse_binary).await?;
    println!("PASS ClickHouse pinned HTTPS, authentication, listener isolation, and shutdown");

    uds::qualify(&root.join("uds"), &args.python).await?;
    println!("PASS tonic/Python gRPC and Axum/httpx Unix-socket transport");

    println!(
        "QUALIFIED {} {}",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caller_relative_existing_paths_become_absolute() {
        let current = std::env::current_dir().unwrap();
        let temporary = tempfile::Builder::new()
            .prefix("qualification-paths-")
            .tempdir_in(&current)
            .unwrap();
        let binary = temporary.path().join("clickhouse");
        std::fs::write(&binary, b"clickhouse").unwrap();
        let relative_root = temporary.path().strip_prefix(&current).unwrap();
        let relative_binary = binary.strip_prefix(&current).unwrap();

        let root = canonicalize_existing(relative_root, "qualification state root").unwrap();
        let binary = canonicalize_existing(relative_binary, "ClickHouse binary").unwrap();

        assert!(root.is_absolute());
        assert!(binary.is_absolute());
        assert_eq!(root, std::fs::canonicalize(temporary.path()).unwrap());
        assert_eq!(
            binary,
            std::fs::canonicalize(temporary.path().join("clickhouse")).unwrap()
        );
    }
}
