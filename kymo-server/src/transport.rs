use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::io::{FromRawFd, RawFd};
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};

const SERVER_MODE_ENV: &str = "KYMO_SERVER_MODE";
const LOCAL_GRPC_SOCKET_ENV: &str = "LOCAL_GRPC_SOCKET";
const LOCAL_CDN_UPLOAD_SOCKET_ENV: &str = "LOCAL_CDN_UPLOAD_SOCKET";
const LOCAL_AUTH_SECRET_PATH_ENV: &str = "LOCAL_AUTH_SECRET_PATH";
const LOCAL_SERVER_LIFECYCLE_SECRET_PATH_ENV: &str = "LOCAL_SERVER_LIFECYCLE_SECRET_PATH";
const CLICKHOUSE_URL_ENV: &str = "CLICKHOUSE_URL";
const CLICKHOUSE_USER_ENV: &str = "CLICKHOUSE_USER";
const CLICKHOUSE_PASSWORD_ENV: &str = "CLICKHOUSE_PASSWORD";
const CLICKHOUSE_SERVER_CERT_PATH_ENV: &str = "CLICKHOUSE_SERVER_CERT_PATH";
const DASHBOARD_LISTEN_ADDR_ENV: &str = "DASHBOARD_LISTEN_ADDR";
const DASHBOARD_LISTENER_FD_ENV: &str = "DASHBOARD_LISTENER_FD";
const CDN_LISTEN_ADDR_ENV: &str = "CDN_LISTEN_ADDR";
const CDN_LISTENER_FD_ENV: &str = "CDN_LISTENER_FD";
const METRICS_LISTEN_ADDR_ENV: &str = "METRICS_LISTEN_ADDR";
const METRICS_LISTENER_FD_ENV: &str = "METRICS_LISTENER_FD";
const LISTEN_ADDR_ENV: &str = "LISTEN_ADDR";

#[derive(Eq, PartialEq)]
pub(crate) struct LocalClickHouseConfig {
    pub(crate) url: String,
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) server_cert_path: PathBuf,
}

impl std::fmt::Debug for LocalClickHouseConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalClickHouseConfig")
            .field("url", &self.url)
            .field("user", &self.user)
            .field("password", &"[redacted]")
            .field("server_cert_path", &self.server_cert_path)
            .finish()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum TransportConfig {
    Hosted {
        native_addr: SocketAddr,
        cdn_addr: SocketAddr,
        metrics_addr: SocketAddr,
    },
    Local {
        native_socket: PathBuf,
        upload_socket: PathBuf,
        dashboard_addr: SocketAddr,
        dashboard_listener_fd: Option<RawFd>,
        cdn_addr: SocketAddr,
        cdn_listener_fd: Option<RawFd>,
        auth_secret_path: PathBuf,
        lifecycle_secret_path: PathBuf,
        clickhouse: Box<LocalClickHouseConfig>,
    },
}

impl TransportConfig {
    pub(crate) fn from_env() -> Result<Self> {
        Self::from_lookup(|name| match std::env::var(name) {
            Ok(value) if !value.trim().is_empty() => Ok(Some(value)),
            Ok(_) | Err(std::env::VarError::NotPresent) => Ok(None),
            Err(error) => Err(anyhow::anyhow!(error)).context(format!("reading {name}")),
        })
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Result<Option<String>>) -> Result<Self> {
        let mode = lookup(SERVER_MODE_ENV)?;
        match mode.as_deref() {
            None | Some("hosted") => {
                for name in [
                    LOCAL_GRPC_SOCKET_ENV,
                    LOCAL_CDN_UPLOAD_SOCKET_ENV,
                    LOCAL_AUTH_SECRET_PATH_ENV,
                    LOCAL_SERVER_LIFECYCLE_SECRET_PATH_ENV,
                    CLICKHOUSE_SERVER_CERT_PATH_ENV,
                    DASHBOARD_LISTEN_ADDR_ENV,
                    DASHBOARD_LISTENER_FD_ENV,
                    CDN_LISTENER_FD_ENV,
                    METRICS_LISTENER_FD_ENV,
                ] {
                    ensure!(
                        lookup(name)?.is_none(),
                        "{name} requires {SERVER_MODE_ENV}=local"
                    );
                }
                Ok(Self::Hosted {
                    native_addr: parse_addr(
                        LISTEN_ADDR_ENV,
                        lookup(LISTEN_ADDR_ENV)?
                            .as_deref()
                            .unwrap_or("0.0.0.0:50051"),
                    )?,
                    cdn_addr: parse_addr(
                        CDN_LISTEN_ADDR_ENV,
                        lookup(CDN_LISTEN_ADDR_ENV)?
                            .as_deref()
                            .unwrap_or("0.0.0.0:8080"),
                    )?,
                    metrics_addr: parse_addr(
                        METRICS_LISTEN_ADDR_ENV,
                        lookup(METRICS_LISTEN_ADDR_ENV)?
                            .as_deref()
                            .unwrap_or("0.0.0.0:9090"),
                    )?,
                })
            }
            Some("local") => {
                ensure!(
                    lookup(LISTEN_ADDR_ENV)?.is_none(),
                    "{LISTEN_ADDR_ENV} is not used in local mode; configure {LOCAL_GRPC_SOCKET_ENV}"
                );
                // Local mode has no Prometheus scraper.
                for name in [METRICS_LISTEN_ADDR_ENV, METRICS_LISTENER_FD_ENV] {
                    ensure!(lookup(name)?.is_none(), "{name} is not used in local mode");
                }
                let native = required_path(&mut lookup, LOCAL_GRPC_SOCKET_ENV)?;
                let upload = required_path(&mut lookup, LOCAL_CDN_UPLOAD_SOCKET_ENV)?;
                ensure!(native != upload, "local Unix socket paths must be distinct");
                validate_socket_path(&native)?;
                validate_socket_path(&upload)?;
                let dashboard = required_addr(&mut lookup, DASHBOARD_LISTEN_ADDR_ENV)?;
                let cdn = required_addr(&mut lookup, CDN_LISTEN_ADDR_ENV)?;
                ensure_loopback(DASHBOARD_LISTEN_ADDR_ENV, dashboard)?;
                ensure_loopback(CDN_LISTEN_ADDR_ENV, cdn)?;
                ensure!(
                    dashboard != cdn,
                    "local TCP listener addresses must be distinct"
                );
                Ok(Self::Local {
                    native_socket: native,
                    cdn_addr: cdn,
                    cdn_listener_fd: optional_fd(&mut lookup, CDN_LISTENER_FD_ENV)?,
                    dashboard_addr: dashboard,
                    dashboard_listener_fd: optional_fd(&mut lookup, DASHBOARD_LISTENER_FD_ENV)?,
                    upload_socket: upload,
                    auth_secret_path: required_path(&mut lookup, LOCAL_AUTH_SECRET_PATH_ENV)?,
                    lifecycle_secret_path: required_path(
                        &mut lookup,
                        LOCAL_SERVER_LIFECYCLE_SECRET_PATH_ENV,
                    )?,
                    clickhouse: Box::new(LocalClickHouseConfig {
                        url: required_string(&mut lookup, CLICKHOUSE_URL_ENV)?,
                        user: required_string(&mut lookup, CLICKHOUSE_USER_ENV)?,
                        password: required_string(&mut lookup, CLICKHOUSE_PASSWORD_ENV)?,
                        server_cert_path: required_path(
                            &mut lookup,
                            CLICKHOUSE_SERVER_CERT_PATH_ENV,
                        )?,
                    }),
                })
            }
            Some(value) => bail!("{SERVER_MODE_ENV} must be hosted or local; got {value:?}"),
        }
    }
}

fn parse_addr(name: &str, value: &str) -> Result<SocketAddr> {
    value
        .parse()
        .with_context(|| format!("{name} must be an IP socket address; got {value:?}"))
}

fn required_addr(
    lookup: &mut impl FnMut(&str) -> Result<Option<String>>,
    name: &str,
) -> Result<SocketAddr> {
    let value = lookup(name)?.with_context(|| format!("{name} is required in local mode"))?;
    parse_addr(name, &value)
}

fn required_string(
    lookup: &mut impl FnMut(&str) -> Result<Option<String>>,
    name: &str,
) -> Result<String> {
    let value = lookup(name)?.with_context(|| format!("{name} is required in local mode"))?;
    ensure!(
        !value.trim().is_empty(),
        "{name} must not be blank in local mode"
    );
    Ok(value)
}

fn required_path(
    lookup: &mut impl FnMut(&str) -> Result<Option<String>>,
    name: &str,
) -> Result<PathBuf> {
    let path =
        PathBuf::from(lookup(name)?.with_context(|| format!("{name} is required in local mode"))?);
    ensure!(path.is_absolute(), "{name} must be an absolute path");
    ensure!(
        path.file_name().is_some(),
        "{name} must name a socket or file"
    );
    Ok(path)
}

fn optional_fd(
    lookup: &mut impl FnMut(&str) -> Result<Option<String>>,
    name: &str,
) -> Result<Option<RawFd>> {
    lookup(name)?
        .map(|value| {
            let descriptor = value
                .parse::<RawFd>()
                .with_context(|| format!("{name} must be a file descriptor; got {value:?}"))?;
            ensure!(descriptor >= 3, "{name} must not use a standard descriptor");
            Ok(descriptor)
        })
        .transpose()
}

pub(crate) fn local_tcp_listener(
    name: &str,
    expected: SocketAddr,
    inherited_fd: Option<RawFd>,
) -> Result<tokio::net::TcpListener> {
    let Some(fd) = inherited_fd else {
        return std::net::TcpListener::bind(expected)
            .with_context(|| format!("bind {name} listener at {expected}"))
            .and_then(|listener| {
                listener.set_nonblocking(true)?;
                tokio::net::TcpListener::from_std(listener).map_err(Into::into)
            });
    };
    ensure!(
        unsafe { libc::fcntl(fd, libc::F_GETFD) } != -1,
        "inherited {name} listener descriptor {fd} is not open"
    );
    // The launcher duplicates a retained listener onto this descriptor in the
    // child immediately before exec. Taking ownership here closes only the
    // server's duplicate; the supervisor keeps its reservation copy.
    let listener = unsafe { std::net::TcpListener::from_raw_fd(fd) };
    let actual = listener
        .local_addr()
        .context("inspect inherited listener")?;
    ensure!(
        actual == expected,
        "inherited {name} listener is {actual}, expected {expected}"
    );
    listener.set_nonblocking(true)?;
    tokio::net::TcpListener::from_std(listener).map_err(Into::into)
}

fn ensure_loopback(name: &str, address: SocketAddr) -> Result<()> {
    ensure!(
        address.ip() == IpAddr::V4(Ipv4Addr::LOCALHOST),
        "{name} must bind 127.0.0.1 in local mode"
    );
    ensure!(
        address.port() >= 1024,
        "{name} must name a fixed, non-system port in local mode"
    );
    Ok(())
}

fn validate_socket_path(path: &Path) -> Result<()> {
    #[cfg(target_os = "macos")]
    const MAX_SOCKET_PATH_BYTES: usize = 103;
    #[cfg(not(target_os = "macos"))]
    const MAX_SOCKET_PATH_BYTES: usize = 107;
    ensure!(
        path.as_os_str().as_bytes().len() <= MAX_SOCKET_PATH_BYTES,
        "Unix socket path exceeds the platform limit: {}",
        path.display()
    );
    Ok(())
}

pub(crate) struct UnixSocketGuard {
    // Device and inode prevent cleanup from unlinking a different socket that was installed at the same path after this listener stopped.
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl Drop for UnixSocketGuard {
    fn drop(&mut self) {
        let Ok(metadata) = std::fs::symlink_metadata(&self.path) else {
            return;
        };
        if metadata.file_type().is_socket()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
        {
            if let Err(error) = std::fs::remove_file(&self.path) {
                tracing::warn!(
                    path = %self.path.display(),
                    %error,
                    "failed to remove Unix socket after listener stopped"
                );
            }
        }
    }
}

pub(crate) fn bind_unix_listener(
    path: &Path,
) -> Result<(tokio::net::UnixListener, UnixSocketGuard)> {
    // The launcher must pass a canonical socket root (not macOS aliases such as /tmp); keep these trust checks aligned with local-runtime/core/src/paths.rs, where sharing source is blocked by the workspaces' different Rust editions.
    validate_socket_path(path)?;
    let parent = path.parent().context("Unix socket path has no parent")?;
    let metadata = std::fs::symlink_metadata(parent)
        .with_context(|| format!("inspect Unix socket parent {}", parent.display()))?;
    ensure!(
        metadata.is_dir(),
        "Unix socket parent is not a real directory: {}",
        parent.display()
    );
    ensure!(
        metadata.uid() == unsafe { libc::geteuid() },
        "Unix socket parent is not owned by the current user: {}",
        parent.display()
    );
    ensure!(
        metadata.permissions().mode() & 0o077 == 0,
        "Unix socket parent grants group or other access: {}",
        parent.display()
    );
    ensure!(
        parent.canonicalize()? == parent,
        "Unix socket parent resolves through a symlink: {}",
        parent.display()
    );
    ensure!(
        std::fs::symlink_metadata(path)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound),
        // A clean exit removes its own socket through UnixSocketGuard. After a crash, only the supervisor may validate and remove the stale entry before starting a new generation.
        "refusing existing Unix socket path {}",
        path.display()
    );
    let listener = tokio::net::UnixListener::bind(path)
        .with_context(|| format!("bind Unix socket {}", path.display()))?;
    let socket_metadata = std::fs::symlink_metadata(path)?;
    let guard = UnixSocketGuard {
        path: path.to_owned(),
        device: socket_metadata.dev(),
        inode: socket_metadata.ino(),
    };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("restrict Unix socket {}", path.display()))?;
    Ok((listener, guard))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::os::fd::IntoRawFd;

    use super::*;

    fn config(values: &[(&str, &str)]) -> Result<TransportConfig> {
        let values = values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect::<BTreeMap<_, _>>();
        TransportConfig::from_lookup(|name| Ok(values.get(name).cloned()))
    }

    #[test]
    fn hosted_defaults_preserve_existing_endpoints() {
        let parsed = config(&[]).unwrap();
        assert_eq!(
            parsed,
            TransportConfig::Hosted {
                native_addr: "0.0.0.0:50051".parse().unwrap(),
                cdn_addr: "0.0.0.0:8080".parse().unwrap(),
                metrics_addr: "0.0.0.0:9090".parse().unwrap(),
            }
        );
        assert!(config(&[(LOCAL_GRPC_SOCKET_ENV, "/tmp/grpc.sock")]).is_err());
    }

    #[test]
    fn local_mode_requires_distinct_loopback_and_private_endpoints() {
        let values = [
            (SERVER_MODE_ENV, "local"),
            (LOCAL_GRPC_SOCKET_ENV, "/tmp/kymo-test/grpc.sock"),
            (LOCAL_CDN_UPLOAD_SOCKET_ENV, "/tmp/kymo-test/upload.sock"),
            (LOCAL_AUTH_SECRET_PATH_ENV, "/tmp/kymo-test/auth.json"),
            (
                LOCAL_SERVER_LIFECYCLE_SECRET_PATH_ENV,
                "/tmp/kymo-test/lifecycle.json",
            ),
            (
                CLICKHOUSE_SERVER_CERT_PATH_ENV,
                "/tmp/kymo-test/clickhouse.pem",
            ),
            (CLICKHOUSE_URL_ENV, "https://localhost:18123"),
            (CLICKHOUSE_USER_ENV, "mkdb2"),
            (CLICKHOUSE_PASSWORD_ENV, "secret"),
            (DASHBOARD_LISTEN_ADDR_ENV, "127.0.0.1:18080"),
            (CDN_LISTEN_ADDR_ENV, "127.0.0.1:18081"),
        ];
        let parsed = config(&values).unwrap();
        assert!(matches!(
            parsed,
            TransportConfig::Local {
                dashboard_addr,
                ..
            } if dashboard_addr == "127.0.0.1:18080".parse().unwrap()
        ));
        for missing in [
            CLICKHOUSE_URL_ENV,
            CLICKHOUSE_USER_ENV,
            CLICKHOUSE_PASSWORD_ENV,
            CLICKHOUSE_SERVER_CERT_PATH_ENV,
            LOCAL_SERVER_LIFECYCLE_SECRET_PATH_ENV,
        ] {
            let without = values
                .into_iter()
                .filter(|(name, _)| *name != missing)
                .collect::<Vec<_>>();
            assert!(config(&without).is_err(), "{missing} was not required");
        }
        let blank_password = values.map(|(name, value)| {
            if name == CLICKHOUSE_PASSWORD_ENV {
                (name, "   ")
            } else {
                (name, value)
            }
        });
        assert!(config(&blank_password).is_err());

        let with_listener_fds = values
            .into_iter()
            .chain([
                (DASHBOARD_LISTENER_FD_ENV, "200"),
                (CDN_LISTENER_FD_ENV, "201"),
            ])
            .collect::<Vec<_>>();
        assert!(matches!(
            config(&with_listener_fds).unwrap(),
            TransportConfig::Local {
                dashboard_listener_fd: Some(200),
                cdn_listener_fd: Some(201),
                ..
            }
        ));
        let invalid_fd = values
            .into_iter()
            .chain([(DASHBOARD_LISTENER_FD_ENV, "2")])
            .collect::<Vec<_>>();
        assert!(config(&invalid_fd).is_err());
        for name in [METRICS_LISTEN_ADDR_ENV, METRICS_LISTENER_FD_ENV] {
            let with_metrics = values
                .into_iter()
                .chain([(name, "127.0.0.1:18082")])
                .collect::<Vec<_>>();
            assert!(config(&with_metrics).is_err(), "{name} was accepted");
        }

        for (name, value) in [
            (DASHBOARD_LISTEN_ADDR_ENV, "0.0.0.0:18080"),
            (DASHBOARD_LISTEN_ADDR_ENV, "[::1]:18080"),
            (DASHBOARD_LISTEN_ADDR_ENV, "127.0.0.1:0"),
            (DASHBOARD_LISTEN_ADDR_ENV, "127.0.0.1:80"),
            (CDN_LISTEN_ADDR_ENV, "192.0.2.10:18081"),
        ] {
            let changed = values.map(|(key, original)| {
                if key == name {
                    (key, value)
                } else {
                    (key, original)
                }
            });
            assert!(config(&changed).is_err());
        }
    }

    #[tokio::test]
    async fn unix_listener_requires_a_private_parent_and_uses_mode_0600() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let socket = root.join("server.sock");
        let (listener, guard) = bind_unix_listener(&socket).unwrap();
        assert_eq!(
            std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(bind_unix_listener(&socket).is_err());
        drop(listener);
        drop(guard);
        assert!(!socket.exists());

        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(bind_unix_listener(&socket).is_err());
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();

        let real_parent = root.join("real");
        std::fs::create_dir(&real_parent).unwrap();
        std::fs::set_permissions(&real_parent, std::fs::Permissions::from_mode(0o700)).unwrap();
        let linked_parent = root.join("linked");
        std::os::unix::fs::symlink(&real_parent, &linked_parent).unwrap();
        assert!(bind_unix_listener(&linked_parent.join("server.sock")).is_err());
    }

    #[tokio::test]
    async fn inherited_tcp_listener_must_match_the_declared_endpoint() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let inherited = local_tcp_listener("test", address, Some(listener.into_raw_fd())).unwrap();
        assert_eq!(inherited.local_addr().unwrap(), address);

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let wrong: SocketAddr = "127.0.0.1:1".parse().unwrap();
        assert!(local_tcp_listener("test", wrong, Some(listener.into_raw_fd())).is_err());
    }
}
