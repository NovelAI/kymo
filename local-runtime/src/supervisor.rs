use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, bail, ensure};
use fs2::FileExt;
use hyper_util::rt::TokioIo;
use kymo_local_runtime_core::artifacts::{SupportedTarget, for_target};
use kymo_local_runtime_core::manifest::{
    LaunchIntent, ProcessIdentity, RunningStack, RuntimeManifest, validate_browser_ports,
};
use kymo_local_runtime_core::paths::{
    RuntimePaths, ensure_private_dir, reject_symlink, validate_confined_regular_file,
    validate_private_file,
};
use kymo_local_runtime_core::profile::{CLICKHOUSE_USER, postgresql_configuration};
use kymo_server::local_proto::local_runtime_control_client::LocalRuntimeControlClient;
use kymo_server::local_proto::{GetActivityRequest, GetActivityResponse, ShutdownLocalRequest};
use kymo_server::proto::ListProjectsRequest;
use kymo_server::proto::kymo_client::KymoClient;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use sysinfo::{Pid, System};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::process::{Child, Command};
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;
use uuid::Uuid;

use crate::generation::{self, PreparedGeneration};

const SUPERVISOR_LOCK_FD: RawFd = 190;
const SUPERVISOR_READY_FD: RawFd = 191;
const DASHBOARD_LISTENER_FD: RawFd = 200;
const CDN_LISTENER_FD: RawFd = 201;
const POSTGRESQL_PORT: u16 = 15432;
const START_TIMEOUT: Duration = Duration::from_secs(120);
// A launch can initialize PostgreSQL and then wait out each component's readiness budget in turn.
const LAUNCH_TIMEOUT: Duration = Duration::from_secs(8 * 60);
const STOP_TIMEOUT: Duration = Duration::from_secs(20);
const CONTROL_IO_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_ACCEPT_FAILURES: u32 = 50;
const UNREACHABLE_TIMEOUT: Duration = Duration::from_secs(60);
const ACTIVITY_POLL_INTERVAL: Duration = Duration::from_secs(1);
const INIT_HOLD_TIMEOUT: Duration = START_TIMEOUT;
const SHORT_HOLD: Duration = Duration::from_secs(10);
const SUSPEND_GAP: Duration = Duration::from_secs(5);
const COMPONENT_LOG_MAX_BYTES: u64 = 8 * 1024 * 1024;
const COMPONENT_LOG_BACKUPS: usize = 3;
const TEST_IDLE_TIMEOUT_ENV: &str = "KYMO_LOCAL_TEST_IDLE_TIMEOUT_MS";
// Everything else in the waking process's environment (server feature switches, cache sizes, listener overrides, log filters) must not reach the stack.
const SUPERVISOR_ENVIRONMENT: &[&str] = &[
    "HOME",
    "PATH",
    "TMPDIR",
    "TZ",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "USER",
    "LOGNAME",
    "XDG_RUNTIME_DIR",
    "XDG_DATA_HOME",
    "XDG_STATE_HOME",
    "XDG_CACHE_HOME",
    "RUST_BACKTRACE",
    "KYMO_LOCAL_ROOT",
    TEST_IDLE_TIMEOUT_ENV,
];

#[derive(Serialize, Deserialize)]
struct ReadyMessage {
    ok: bool,
    detail: String,
}

#[derive(Serialize, Deserialize)]
struct ControlRequest {
    supervisor_bearer: String,
    command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    init_hold_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    open_hold_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize)]
struct ControlResponse {
    ok: bool,
    detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    endpoints: Option<EnsureOutput>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct EnsureOutput {
    pub(crate) protocol_min: u32,
    pub(crate) protocol_max: u32,
    pub(crate) installation_uuid: Uuid,
    pub(crate) endpoint_generation: Uuid,
    pub(crate) native_socket: PathBuf,
    pub(crate) upload_socket: PathBuf,
    pub(crate) dashboard_origin: String,
    pub(crate) cdn_origin: String,
    pub(crate) server_bearer: String,
}

enum ControlOutcome {
    Continue,
    Stopped,
}

#[derive(Default)]
struct HoldSet {
    deadlines: HashMap<String, tokio::time::Instant>,
}

impl HoldSet {
    fn add_init(&mut self, id: Uuid, now: tokio::time::Instant) {
        self.deadlines
            .insert(id.to_string(), now + INIT_HOLD_TIMEOUT);
    }

    fn add_open(&mut self, id: Uuid, now: tokio::time::Instant) {
        self.deadlines
            .insert(format!("open:{id}"), now + INIT_HOLD_TIMEOUT);
    }

    fn add_post_wake(&mut self, now: tokio::time::Instant) {
        self.deadlines
            .insert("post-wake".to_owned(), now + SHORT_HOLD);
    }

    fn add_bare_ensure(&mut self, now: tokio::time::Instant) {
        self.deadlines.insert("ensure".to_owned(), now + SHORT_HOLD);
    }

    fn observe_snapshot(&mut self, snapshot: &GetActivityResponse, now: tokio::time::Instant) {
        for id in &snapshot.fulfilled_hold_ids {
            self.deadlines.remove(id);
        }
        self.deadlines.retain(|_, deadline| *deadline > now);
    }

    fn blocks_idle(&self, now: tokio::time::Instant) -> bool {
        self.hold_until().is_some_and(|deadline| deadline > now)
    }

    fn hold_until(&self) -> Option<tokio::time::Instant> {
        self.deadlines.values().copied().max()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SupervisorSecret {
    format_version: u32,
    supervisor_bearer: String,
}

struct Children {
    postgresql: Child,
    postgresql_group: u32,
    clickhouse: Child,
    clickhouse_group: u32,
    server: Child,
    server_group: u32,
    server_sockets: Vec<OwnedSocket>,
    postgresql_socket_dir: PathBuf,
    clickhouse_client: reqwest::Client,
    _dashboard_listener: TcpListener,
    _cdn_listener: TcpListener,
}

struct OwnedSocket {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl OwnedSocket {
    fn capture(path: &Path) -> Result<Self> {
        let metadata = std::fs::symlink_metadata(path)
            .with_context(|| format!("inspect owned socket {}", path.display()))?;
        ensure!(
            metadata.file_type().is_socket(),
            "{} is not a Unix socket",
            path.display()
        );
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() },
            "{} is owned by another account",
            path.display()
        );
        Ok(Self {
            path: path.to_owned(),
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    fn remove_if_unchanged(&self) -> Result<()> {
        let metadata = match std::fs::symlink_metadata(&self.path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("inspect stopped socket {}", self.path.display()));
            }
        };
        ensure!(
            metadata.file_type().is_socket()
                && metadata.dev() == self.device
                && metadata.ino() == self.inode,
            "owned socket changed before cleanup: {}",
            self.path.display()
        );
        std::fs::remove_file(&self.path)
            .with_context(|| format!("remove stopped socket {}", self.path.display()))?;
        sync_parent(&self.path)
    }
}

struct RuntimeEndpoints {
    native_socket: PathBuf,
    upload_socket: PathBuf,
    postgresql_socket_dir: PathBuf,
    clickhouse_addr: SocketAddr,
}

/// Start the stack unless one is already running or starting, and return its client endpoints.
pub(crate) async fn ensure_running(
    paths: &RuntimePaths,
    init_hold_id: Option<Uuid>,
    open_hold_id: Option<Uuid>,
) -> Result<EnsureOutput> {
    let artifacts = for_target(SupportedTarget::current()?)?;
    let deadline = tokio::time::Instant::now() + LAUNCH_TIMEOUT;
    let mut started = false;
    loop {
        let manifest =
            RuntimeManifest::read_validated(paths, &artifacts, env!("CARGO_PKG_VERSION"))?;
        let mut waiting_for = "a concurrent launch to publish its endpoints".to_owned();
        if let Some(running) = &manifest.running {
            match request_control(
                running,
                "ensure",
                CONTROL_IO_TIMEOUT,
                init_hold_id,
                open_hold_id,
            )
            .await
            {
                Ok(response) => {
                    ensure!(
                        response.ok,
                        "local supervisor rejected ensure: {}",
                        response.detail
                    );
                    let endpoints = response
                        .endpoints
                        .context("local supervisor omitted ensure endpoints")?;
                    ensure!(
                        endpoints.installation_uuid == manifest.installation_uuid
                            && endpoints.endpoint_generation == running.generation_uuid,
                        "local supervisor returned endpoints for a different generation"
                    );
                    return Ok(endpoints);
                }
                Err(control_error) => {
                    waiting_for = format!("the local supervisor after: {control_error:#}");
                }
            }
        }
        // A live supervisor holds the runtime lock for its whole life, so acquiring it means nothing is running or starting.
        if let Some(lock) = try_acquire_runtime_lock(paths)? {
            // `kymo stop` closes the start gate for its whole run, so a waking client cannot restart the stack under it.
            if let Some(_gate) = try_acquire_start_gate(paths)? {
                // One start per call: a stack that stops again right away is reported, not restarted in a loop.
                ensure!(
                    !started,
                    "the local stack stopped right after it started; see `kymo status`"
                );
                start_with_lock(paths, lock).await?;
                started = true;
                continue;
            }
            // Holding the runtime lock rules out a launcher, so only `kymo stop` can hold the gate.
            waiting_for = "`kymo stop` to release the local stack".to_owned();
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {waiting_for}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn start_with_lock(paths: &RuntimePaths, lock: File) -> Result<()> {
    let artifacts = for_target(SupportedTarget::current()?)?;
    let mut manifest =
        RuntimeManifest::read_validated(paths, &artifacts, env!("CARGO_PKG_VERSION"))?;
    clear_stopped_remnants(paths, &mut manifest)?;

    let (ready_parent, ready_child) = StdUnixStream::pair().context("create readiness channel")?;
    ready_parent.set_read_timeout(Some(LAUNCH_TIMEOUT))?;
    let executable = std::env::current_exe()?.canonicalize()?;
    let log_path = paths.state.join("supervisor.log");
    let log = open_log(&log_path)?;
    let lock_source = lock.as_raw_fd();
    let ready_source = ready_child.as_raw_fd();
    let mut command = Command::new(&executable);
    command
        .arg("__supervise")
        .arg("--lock-fd")
        .arg(SUPERVISOR_LOCK_FD.to_string())
        .arg("--ready-fd")
        .arg(SUPERVISOR_READY_FD.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log));
    // Children inherit this environment, so the whole stack behaves the same whichever process woke it.
    command.env_clear().envs(
        SUPERVISOR_ENVIRONMENT
            .iter()
            .filter_map(|name| std::env::var_os(name).map(|value| (*name, value))),
    );
    unsafe {
        command.as_std_mut().pre_exec(move || {
            dup_to(lock_source, SUPERVISOR_LOCK_FD)?;
            dup_to(ready_source, SUPERVISOR_READY_FD)?;
            if libc::setpgid(0, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn().context("spawn local supervisor")?;
    drop(ready_child);
    drop(lock);
    let message = tokio::task::spawn_blocking(move || read_ready(ready_parent))
        .await
        .context("join supervisor readiness reader")??;
    if !message.ok {
        let _ = child.wait().await;
        bail!("supervisor startup failed: {}", message.detail);
    }
    Ok(())
}

/// With the runtime lock held, no recorded supervisor is alive; clear what it left behind once every process it started is proven gone.
pub(crate) fn clear_stopped_remnants(
    paths: &RuntimePaths,
    manifest: &mut RuntimeManifest,
) -> Result<()> {
    let mut changed = manifest.record_launcher_version(env!("CARGO_PKG_VERSION"));
    if let Some(launching) = &manifest.launching {
        let live = live_launch_groups(launching);
        ensure!(
            live.is_empty(),
            "an interrupted launch left live process groups {live:?}; stop them, then retry"
        );
        manifest.launching = None;
        manifest
            .degraded_reason
            .get_or_insert_with(|| "a previous launch was interrupted".to_owned());
        changed = true;
    }
    if let Some(running) = &manifest.running {
        ensure!(
            recorded_stack_is_quiescent(running),
            "the recorded local stack has no supervisor but is not provably stopped; run `kymo doctor`"
        );
        manifest.running = None;
        changed = true;
    }
    if changed {
        manifest.write_atomic(&paths.manifest())?;
    }
    Ok(())
}

/// Publishes each child's process group as soon as it exists, so a launch interrupted at any point stays provably recoverable.
struct LaunchRecord<'a> {
    paths: &'a RuntimePaths,
    manifest: &'a mut RuntimeManifest,
}

impl LaunchRecord<'_> {
    /// Spawn a child in its own process group and publish that group before anything else can fail.
    async fn spawn(&mut self, command: &mut Command, name: &str) -> Result<(Child, u32)> {
        command.as_std_mut().process_group(0);
        let mut child = command.spawn().with_context(|| format!("start {name}"))?;
        match self.record(&child, name) {
            Ok(group) => Ok((child, group)),
            Err(error) => abort_startup_child(&mut child, name, error).await,
        }
    }

    fn record(&mut self, child: &Child, name: &str) -> Result<u32> {
        let group = child_group(child, name)?;
        self.manifest
            .launching
            .as_mut()
            .context("launch intent is missing")?
            .child_process_groups
            .push(group);
        self.manifest.write_atomic(&self.paths.manifest())?;
        Ok(group)
    }
}

pub(crate) async fn run(paths: &RuntimePaths, lock_fd: RawFd, ready_fd: RawFd) -> Result<()> {
    ensure!(
        lock_fd == SUPERVISOR_LOCK_FD && ready_fd == SUPERVISOR_READY_FD,
        "invalid inherited supervisor descriptors"
    );
    let _runtime_lock = unsafe { File::from_raw_fd(lock_fd) };
    let ready = unsafe { StdUnixStream::from_raw_fd(ready_fd) };
    set_close_on_exec(lock_fd)?;
    set_close_on_exec(ready_fd)?;
    install_rotating_process_log(&paths.state.join("supervisor.log"))?;
    let result = run_inner(&ready).await;
    if let Err(error) = &result {
        let _ = write_ready(
            &ready,
            &ReadyMessage {
                ok: false,
                detail: format!("{error:#}"),
            },
        );
    }
    result
}

async fn run_inner(ready: &StdUnixStream) -> Result<()> {
    let paths = RuntimePaths::discover()?;
    let artifacts = for_target(SupportedTarget::current()?)?;
    let mut manifest =
        RuntimeManifest::read_validated(&paths, &artifacts, env!("CARGO_PKG_VERSION"))?;
    ensure!(
        manifest.running.is_none(),
        "runtime manifest already records a running stack"
    );
    let runtime_root = runtime_root(&paths)?;
    clean_reconstructible_state(&paths, &runtime_root)?;
    ensure_private_dir(&runtime_root)?;
    let native_socket = runtime_root.join("grpc.sock");
    let upload_socket = runtime_root.join("upload.sock");
    let control_socket = runtime_root.join("control.sock");
    let postgresql_socket_dir = runtime_root.join("pg");
    ensure_private_dir(&postgresql_socket_dir)?;
    let postgresql_socket = postgresql_socket_dir.join(format!(".s.PGSQL.{POSTGRESQL_PORT}"));
    for socket in [
        &native_socket,
        &upload_socket,
        &control_socket,
        &postgresql_socket,
    ] {
        ensure!(
            socket.as_os_str().len() < 100,
            "runtime socket path is too long: {}",
            socket.display()
        );
    }

    let dashboard_listener = bind_browser_port("dashboard", manifest.dashboard_port)?;
    let dashboard_addr = dashboard_listener.local_addr()?;
    let cdn_listener = bind_browser_port("CDN", manifest.cdn_port)?;
    let cdn_addr = cdn_listener.local_addr()?;
    let clickhouse_addr = unused_loopback_addr()?;
    let endpoints = RuntimeEndpoints {
        native_socket: native_socket.clone(),
        upload_socket: upload_socket.clone(),
        postgresql_socket_dir: postgresql_socket_dir.clone(),
        clickhouse_addr,
    };
    let generation = generation::prepare(&paths, clickhouse_addr.port())?;
    let control_listener = bind_control(&control_socket)?;

    manifest.launching = Some(LaunchIntent {
        supervisor: process_identity(std::process::id(), std::env::current_exe()?)?,
        started_at_unix_ms: unix_time_ms(),
        child_process_groups: Vec::new(),
    });
    manifest.write_atomic(&paths.manifest())?;

    let children = start_children(
        &mut LaunchRecord {
            paths: &paths,
            manifest: &mut manifest,
        },
        &generation,
        &endpoints,
        dashboard_listener,
        cdn_listener,
    )
    .await;
    let mut children = match children {
        Ok(children) => children,
        Err(error) => {
            // A cleanup that could not prove a child gone keeps the launch record, so the next start fails closed instead of reusing live data.
            if manifest.launching.as_ref().is_some_and(|launching| {
                launching
                    .child_process_groups
                    .iter()
                    .copied()
                    .all(process_group_gone)
            }) {
                manifest.launching = None;
            }
            manifest.degraded_reason = Some(format!("startup failed: {error:#}"));
            manifest.write_atomic(&paths.manifest())?;
            return Err(error);
        }
    };
    let running = (|| -> Result<RunningStack> {
        Ok(RunningStack {
            generation_uuid: generation.id,
            supervisor: process_identity(std::process::id(), std::env::current_exe()?)?,
            postgresql: child_identity(
                &children.postgresql,
                paths.postgresql_binary(&manifest.postgresql.version),
            )?,
            clickhouse: child_identity(
                &children.clickhouse,
                paths.clickhouse_binary(&manifest.clickhouse.version),
            )?,
            server: child_identity(&children.server, server_binary()?)?,
            native_socket: native_socket.clone(),
            upload_socket,
            control_socket: control_socket.clone(),
            dashboard_addr,
            cdn_addr,
            supervisor_secret: generation.supervisor_secret.clone(),
        })
    })();
    let running = match running {
        Ok(running) => running,
        Err(error) => {
            let error = error.context("record started process identities");
            return stop_after_failure(
                &paths,
                &mut manifest,
                &generation,
                &native_socket,
                &mut children,
                error,
            )
            .await;
        }
    };
    let endpoints = ensure_output(&manifest, &running, &generation);
    manifest.launching = None;
    manifest.running = Some(running);
    manifest.degraded_reason = None;
    if let Err(error) = manifest.write_atomic(&paths.manifest()) {
        let error = error.context("publish running stack");
        return stop_after_failure(
            &paths,
            &mut manifest,
            &generation,
            &native_socket,
            &mut children,
            error,
        )
        .await;
    }
    // The published manifest, not this pipe, is the readiness contract: a launcher that gave up or was killed must not take a healthy stack down with it.
    if let Err(error) = write_ready(
        ready,
        &ReadyMessage {
            ok: true,
            detail: "local stack is ready".to_owned(),
        },
    ) {
        eprintln!("launcher left before readiness was reported; continuing: {error:#}");
    }
    let result = supervise_control(
        control_listener,
        &generation,
        &endpoints,
        &native_socket,
        &paths,
        &mut children,
    )
    .await;
    if let Err(error) = result {
        return stop_after_failure(
            &paths,
            &mut manifest,
            &generation,
            &native_socket,
            &mut children,
            error,
        )
        .await;
    }
    let _ = std::fs::remove_file(&control_socket);
    let _ = std::fs::remove_dir_all(&generation.root);
    let _ = std::fs::remove_dir_all(&runtime_root);
    Ok(())
}

/// Stop the components after a failure, clearing the record only once they are proven stopped, and keep the reason for `status`.
async fn stop_after_failure(
    paths: &RuntimePaths,
    manifest: &mut RuntimeManifest,
    generation: &PreparedGeneration,
    native_socket: &Path,
    children: &mut Children,
    error: anyhow::Error,
) -> Result<()> {
    let cleanup = stop_children(generation, native_socket, paths, children).await;
    if cleanup.is_ok() {
        manifest.launching = None;
        manifest.running = None;
    }
    manifest.degraded_reason = Some(match cleanup {
        Ok(()) => format!("{error:#}"),
        Err(cleanup) => format!("{error:#}; cleanup failed: {cleanup:#}"),
    });
    manifest.write_atomic(&paths.manifest())?;
    Err(error)
}

async fn supervise_control(
    listener: UnixListener,
    generation: &PreparedGeneration,
    endpoints: &EnsureOutput,
    native_socket: &Path,
    paths: &RuntimePaths,
    children: &mut Children,
) -> Result<()> {
    let idle_timeout = local_idle_timeout()?;
    let mut holds = HoldSet::default();
    // The launcher that started this stack asks for its endpoints (and its own hold) right after readiness; the first idle poll must not beat that request.
    holds.add_bare_ensure(tokio::time::Instant::now());
    let mut activity_tick = tokio::time::interval(ACTIVITY_POLL_INTERVAL);
    activity_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_poll = tokio::time::Instant::now();
    let mut last_wall = SystemTime::now();
    let mut accept_failures = 0;
    let mut unreachable_since: Option<tokio::time::Instant> = None;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        tokio::select! {
            _ = terminate.recv() => {
                eprintln!("supervisor received SIGTERM; stopping the local stack");
                stop_running_generation(generation, native_socket, paths, children).await?;
                return Ok(());
            }
            connection = listener.accept() => {
                let stream = match connection {
                    Ok((stream, _)) => {
                        accept_failures = 0;
                        stream
                    }
                    // Ride out a transient error, but a supervisor that cannot be reached cannot be stopped either: fail and stop the stack.
                    Err(error) if accept_failures < MAX_ACCEPT_FAILURES => {
                        accept_failures += 1;
                        eprintln!("supervisor control accept failed: {error}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    Err(error) => bail!("supervisor control socket keeps failing: {error}"),
                };
                if matches!(handle_control(stream, generation, endpoints, native_socket, paths, children, &mut holds).await?, ControlOutcome::Stopped) {
                    return Ok(());
                }
            }
            _ = activity_tick.tick() => {
                let now = tokio::time::Instant::now();
                let wall = SystemTime::now();
                let monotonic_gap = now.saturating_duration_since(last_poll);
                if polling_discontinuity(monotonic_gap, wall.duration_since(last_wall).ok()) {
                    holds.add_post_wake(now);
                }
                last_poll = now;
                last_wall = wall;
                match get_activity(generation, native_socket).await {
                    Ok(snapshot) => {
                        unreachable_since = None;
                        holds.observe_snapshot(&snapshot, now);
                        if idle_eligible(&snapshot, &holds, now, idle_timeout) {
                            let last_ingest = snapshot
                                .last_committed_ingest_ago_ms
                                .map_or_else(|| "none since ready".to_owned(), |age| format!("{age}ms ago"));
                            eprintln!(
                                "local stack reached idle shutdown after {}ms without keepalive activity (last committed ingest {last_ingest})",
                                snapshot.keepalive_idle_for_ms,
                            );
                            stop_running_generation(generation, native_socket, paths, children).await?;
                            return Ok(());
                        }
                    }
                    // A server no client can reach either (its socket directory removed, say) must not pin the stack forever.
                    Err(error) if now.saturating_duration_since(*unreachable_since.get_or_insert(now)) >= UNREACHABLE_TIMEOUT => {
                        bail!("local server has been unreachable for {UNREACHABLE_TIMEOUT:?}: {error:#}")
                    }
                    Err(error) => eprintln!("local activity poll failed; idle shutdown deferred: {error:#}"),
                }
            }
            status = children.server.wait() => bail!("kymo-server exited unexpectedly: {}", status?),
            status = children.clickhouse.wait() => bail!("ClickHouse exited unexpectedly: {}", status?),
            status = children.postgresql.wait() => bail!("PostgreSQL exited unexpectedly: {}", status?),
        }
    }
}

fn polling_discontinuity(monotonic_gap: Duration, wall_gap: Option<Duration>) -> bool {
    let Some(wall_gap) = wall_gap else {
        return true;
    };
    monotonic_gap > SUSPEND_GAP
        || wall_gap > monotonic_gap.saturating_add(SUSPEND_GAP)
        || monotonic_gap > wall_gap.saturating_add(SUSPEND_GAP)
}

async fn handle_control(
    stream: UnixStream,
    generation: &PreparedGeneration,
    endpoints: &EnsureOutput,
    native_socket: &Path,
    paths: &RuntimePaths,
    children: &mut Children,
    holds: &mut HoldSet,
) -> Result<ControlOutcome> {
    let (read, mut write) = stream.into_split();
    let mut line = String::new();
    let mut bounded = tokio::io::BufReader::new(read).take(4097);
    if !matches!(
        tokio::time::timeout(CONTROL_IO_TIMEOUT, bounded.read_line(&mut line)).await,
        Ok(Ok(_))
    ) {
        return Ok(ControlOutcome::Continue);
    }
    let request = (line.len() <= 4096)
        .then(|| serde_json::from_str::<ControlRequest>(&line).ok())
        .flatten()
        .filter(|request| {
            request
                .supervisor_bearer
                .as_bytes()
                .ct_eq(generation.supervisor_bearer.as_bytes())
                .into()
        });
    let Some(request) = request else {
        send_control_response(
            &mut write,
            &ControlResponse {
                ok: false,
                detail: "authentication required".to_owned(),
                endpoints: None,
            },
        )
        .await;
        return Ok(ControlOutcome::Continue);
    };
    match request.command.as_str() {
        "ensure" => {
            let now = tokio::time::Instant::now();
            if let Some(id) = request.init_hold_id {
                holds.add_init(id, now);
            } else if let Some(id) = request.open_hold_id {
                holds.add_open(id, now);
            } else {
                holds.add_bare_ensure(now);
            }
            send_control_response(
                &mut write,
                &ControlResponse {
                    ok: true,
                    detail: "local stack is ready".to_owned(),
                    endpoints: Some(endpoints.clone()),
                },
            )
            .await;
            Ok(ControlOutcome::Continue)
        }
        "stop" => {
            let result = stop_running_generation(generation, native_socket, paths, children).await;
            let response = ControlResponse {
                ok: result.is_ok(),
                detail: match &result {
                    Ok(()) => "local stack stopped".to_owned(),
                    Err(error) => format!("{error:#}"),
                },
                endpoints: None,
            };
            send_control_response(&mut write, &response).await;
            result.map(|()| ControlOutcome::Stopped)
        }
        _ => {
            send_control_response(
                &mut write,
                &ControlResponse {
                    ok: false,
                    detail: "unsupported supervisor command".to_owned(),
                    endpoints: None,
                },
            )
            .await;
            Ok(ControlOutcome::Continue)
        }
    }
}

/// A client that disconnects before its reply only loses that reply; it must not stop the stack.
async fn send_control_response(
    write: &mut tokio::net::unix::OwnedWriteHalf,
    response: &ControlResponse,
) {
    if let Err(error) = write_control_response(write, response).await {
        eprintln!("supervisor control reply was not delivered: {error:#}");
    }
}

async fn stop_running_generation(
    generation: &PreparedGeneration,
    native_socket: &Path,
    paths: &RuntimePaths,
    children: &mut Children,
) -> Result<()> {
    stop_children(generation, native_socket, paths, children).await?;
    let artifacts = for_target(SupportedTarget::current()?)?;
    let mut manifest =
        RuntimeManifest::read_validated(paths, &artifacts, env!("CARGO_PKG_VERSION"))?;
    ensure!(
        manifest
            .running
            .as_ref()
            .is_some_and(|running| running.generation_uuid == generation.id),
        "runtime manifest changed before stopped-state publication"
    );
    manifest.running = None;
    manifest.degraded_reason = None;
    manifest.write_atomic(&paths.manifest())
}

fn idle_eligible(
    snapshot: &GetActivityResponse,
    holds: &HoldSet,
    now: tokio::time::Instant,
    idle_timeout: Duration,
) -> bool {
    Duration::from_millis(snapshot.keepalive_idle_for_ms) >= idle_timeout
        && !holds.blocks_idle(now)
        && snapshot.frontend_reconnect_grace_remaining_ms == 0
        && snapshot.frontend_connections == 0
        && snapshot.in_flight_work == 0
        && !snapshot.ingest_bookkeeping_draining
}

fn local_idle_timeout() -> Result<Duration> {
    #[cfg(feature = "test-idle-timeout")]
    if let Some(raw) = std::env::var_os(TEST_IDLE_TIMEOUT_ENV) {
        let raw = raw
            .to_str()
            .context("test idle timeout is not valid UTF-8")?;
        let millis = raw
            .parse::<u64>()
            .context("test idle timeout must be an unsigned integer")?;
        ensure!(millis > 0, "test idle timeout must be greater than zero");
        let timeout = Duration::from_millis(millis);
        eprintln!("using CI-only local idle timeout of {millis}ms");
        return Ok(timeout);
    }
    Ok(crate::liveness::LOCAL_IDLE_TIMEOUT)
}

async fn write_control_response(
    write: &mut tokio::net::unix::OwnedWriteHalf,
    response: &ControlResponse,
) -> Result<()> {
    write.write_all(&serde_json::to_vec(response)?).await?;
    write.write_all(b"\n").await?;
    write.shutdown().await?;
    Ok(())
}

fn ensure_output(
    manifest: &RuntimeManifest,
    running: &RunningStack,
    generation: &PreparedGeneration,
) -> EnsureOutput {
    EnsureOutput {
        protocol_min: manifest.protocol_min,
        protocol_max: manifest.protocol_max,
        installation_uuid: manifest.installation_uuid,
        endpoint_generation: running.generation_uuid,
        native_socket: running.native_socket.clone(),
        upload_socket: running.upload_socket.clone(),
        dashboard_origin: format!("http://{}", running.dashboard_addr),
        cdn_origin: format!("http://{}", running.cdn_addr),
        server_bearer: generation.server_bearer.clone(),
    }
}

async fn get_activity(
    generation: &PreparedGeneration,
    native_socket: &Path,
) -> Result<GetActivityResponse> {
    lifecycle_call(native_socket, CONTROL_IO_TIMEOUT, async |mut client| {
        let request = bearer_request(&generation.lifecycle_bearer, GetActivityRequest {})?;
        Ok(client.get_activity(request).await?.into_inner())
    })
    .await
}

/// One lifecycle RPC to the server with its connect inside the deadline, so a wedged socket cannot stall the supervisor.
async fn lifecycle_call<T>(
    native_socket: &Path,
    deadline: Duration,
    call: impl AsyncFnOnce(LocalRuntimeControlClient<Channel>) -> Result<T>,
) -> Result<T> {
    tokio::time::timeout(deadline, async {
        call(LocalRuntimeControlClient::new(
            native_channel(native_socket).await?,
        ))
        .await
    })
    .await
    .context("server lifecycle request timed out")?
}

fn bearer_request<T>(token: &str, message: T) -> Result<tonic::Request<T>> {
    let mut request = tonic::Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}")
            .parse()
            .context("encode bearer metadata")?,
    );
    Ok(request)
}

async fn request_control(
    running: &RunningStack,
    command: &str,
    timeout: Duration,
    init_hold_id: Option<Uuid>,
    open_hold_id: Option<Uuid>,
) -> Result<ControlResponse> {
    validate_private_file(&running.supervisor_secret)?;
    let secret: SupervisorSecret =
        serde_json::from_slice(&std::fs::read(&running.supervisor_secret)?)?;
    ensure!(
        secret.format_version == 1,
        "unsupported supervisor secret format"
    );
    let request = ControlRequest {
        supervisor_bearer: secret.supervisor_bearer,
        command: command.to_owned(),
        init_hold_id,
        open_hold_id,
    };
    let mut response = String::new();
    tokio::time::timeout(timeout, async {
        let mut stream = UnixStream::connect(&running.control_socket)
            .await
            .context("connect to local supervisor")?;
        stream.write_all(&serde_json::to_vec(&request)?).await?;
        stream.write_all(b"\n").await?;
        tokio::io::BufReader::new(stream)
            .take(4097)
            .read_line(&mut response)
            .await?;
        Result::<()>::Ok(())
    })
    .await
    .context("supervisor request timed out")??;
    ensure!(
        !response.is_empty() && response.len() <= 4096,
        "supervisor returned an invalid response"
    );
    serde_json::from_str(&response).context("parse supervisor response")
}

/// Stop the stack and prove it stopped. With `hold`, keep the runtime lock until interrupted so nothing, including a process with queued data, can restart it meanwhile.
pub(crate) async fn stop(paths: &RuntimePaths, hold: bool) -> Result<()> {
    let artifacts = for_target(SupportedTarget::current()?)?;
    let deadline = tokio::time::Instant::now() + LAUNCH_TIMEOUT;
    // Closing the start gate first means that once this stack stops, nothing restarts it until this command ends; a launch already holding the gate finishes and is then stopped.
    let _gate = loop {
        if let Some(gate) = try_acquire_start_gate(paths)? {
            break gate;
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for a local stack launch or another `kymo stop` to finish"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let mut signalled = false;
    let lock = loop {
        // The supervisor holds the runtime lock for its whole life; owning it proves nothing is running or starting.
        if let Some(lock) = try_acquire_runtime_lock(paths)? {
            break lock;
        }
        let manifest =
            RuntimeManifest::read_validated(paths, &artifacts, env!("CARGO_PKG_VERSION"))?;
        if let Some(running) = &manifest.running {
            match request_control(running, "stop", STOP_TIMEOUT * 4, None, None).await {
                Ok(response) => {
                    ensure!(response.ok, "supervisor stop failed: {}", response.detail)
                }
                // A supervisor whose control socket is gone (for example a runtime directory removed at logout) still stops on SIGTERM.
                Err(error) if !signalled && recorded_process_alive(&running.supervisor) => {
                    eprintln!("local supervisor did not answer stop ({error:#}); sending SIGTERM");
                    unsafe { libc::kill(running.supervisor.pid as i32, libc::SIGTERM) };
                    signalled = true;
                }
                Err(error) => eprintln!("local supervisor did not answer stop: {error:#}"),
            }
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the local stack to stop"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let mut manifest =
        RuntimeManifest::read_validated(paths, &artifacts, env!("CARGO_PKG_VERSION"))?;
    if let Some(running) = &manifest.running {
        terminate_orphaned_stack(running).await;
    }
    clear_stopped_remnants(paths, &mut manifest)?;
    if hold {
        eprintln!("local stack is stopped and held; press Ctrl-C to release it");
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {}
        }
    }
    drop(lock);
    Ok(())
}

/// The supervisor is gone (its lock is ours) but some components may outlive it. Ask each one whose recorded identity, including its start time, still matches to shut down; anything else is left for `clear_stopped_remnants` to refuse.
async fn terminate_orphaned_stack(running: &RunningStack) {
    // Supervised-stop signals first (PostgreSQL's SIGINT is its fast shutdown); SIGINT escalates for anything still alive, since kymo-server does not handle it.
    for (name, identity, signals) in [
        (
            "kymo-server",
            &running.server,
            &[libc::SIGTERM, libc::SIGINT][..],
        ),
        (
            "ClickHouse",
            &running.clickhouse,
            &[libc::SIGTERM, libc::SIGINT][..],
        ),
        ("PostgreSQL", &running.postgresql, &[libc::SIGINT][..]),
    ] {
        for &signal in signals {
            if !recorded_process_alive(identity) {
                break;
            }
            eprintln!("stopping orphaned {name} (pid {})", identity.pid);
            unsafe { libc::kill(-(identity.pid as i32), signal) };
            let deadline = tokio::time::Instant::now() + STOP_TIMEOUT;
            while !process_group_gone(identity.pid) && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Replace the pinned browser ports of a stopped installation.
pub(crate) fn set_browser_ports(paths: &RuntimePaths, dashboard: u16, cdn: u16) -> Result<()> {
    let artifacts = for_target(SupportedTarget::current()?)?;
    let _lock = try_acquire_runtime_lock(paths)?.with_context(|| {
        if start_gate_is_held(paths) {
            "the local stack is held by `kymo stop --hold`; release it first"
        } else {
            "the local stack is running or starting; stop it with `kymo stop` first"
        }
    })?;
    let mut manifest =
        RuntimeManifest::read_validated(paths, &artifacts, env!("CARGO_PKG_VERSION"))?;
    clear_stopped_remnants(paths, &mut manifest)?;
    validate_browser_ports(dashboard, cdn)?;
    let _dashboard = bind_browser_port("dashboard", dashboard)?;
    let _cdn = bind_browser_port("CDN", cdn)?;
    manifest.dashboard_port = dashboard;
    manifest.cdn_port = cdn;
    manifest.write_atomic(&paths.manifest())
}

fn bind_browser_port(name: &str, port: u16) -> Result<TcpListener> {
    TcpListener::bind(("127.0.0.1", port)).with_context(|| {
        format!(
            "bind pinned {name} port {port}; free it, or pick new ports with `kymo ports --dashboard <port> --cdn <port>`"
        )
    })
}

async fn start_children(
    launch: &mut LaunchRecord<'_>,
    generation: &PreparedGeneration,
    endpoints: &RuntimeEndpoints,
    dashboard_listener: TcpListener,
    cdn_listener: TcpListener,
) -> Result<Children> {
    let paths = launch.paths;
    // Clone before any child exists, so this cannot fail with components left running.
    let server_listeners = (dashboard_listener.try_clone()?, cdn_listener.try_clone()?);
    let (mut postgresql, postgresql_group, database_url) =
        start_postgresql(launch, &endpoints.postgresql_socket_dir).await?;
    let (mut clickhouse, clickhouse_group, clickhouse_client) =
        match start_clickhouse(launch, generation, endpoints.clickhouse_addr).await {
            Ok(started) => started,
            Err(error) => {
                if let Err(cleanup) = stop_postgresql(
                    paths,
                    &mut postgresql,
                    postgresql_group,
                    &endpoints.postgresql_socket_dir,
                )
                .await
                {
                    retain_ambiguous_startup(format!(
                    "ClickHouse startup failed: {error:#}; PostgreSQL cleanup failed: {cleanup:#}"
                ))
                .await;
                }
                return Err(error);
            }
        };
    let (server, server_group, server_sockets) = match start_server(
        launch,
        generation,
        endpoints,
        &database_url,
        server_listeners,
    )
    .await
    {
        Ok(server) => server,
        Err(error) => {
            let clickhouse_cleanup = stop_clickhouse(
                generation,
                &clickhouse_client,
                &mut clickhouse,
                clickhouse_group,
                paths,
            )
            .await;
            let postgresql_cleanup = stop_postgresql(
                paths,
                &mut postgresql,
                postgresql_group,
                &endpoints.postgresql_socket_dir,
            )
            .await;
            if clickhouse_cleanup.is_err() || postgresql_cleanup.is_err() {
                retain_ambiguous_startup(format!(
                    "server startup failed: {error:#}; ClickHouse cleanup: {}; PostgreSQL cleanup: {}",
                    result_detail(&clickhouse_cleanup),
                    result_detail(&postgresql_cleanup)
                ))
                .await;
            }
            return Err(error);
        }
    };
    Ok(Children {
        postgresql,
        postgresql_group,
        clickhouse,
        clickhouse_group,
        server,
        server_group,
        server_sockets,
        postgresql_socket_dir: endpoints.postgresql_socket_dir.clone(),
        clickhouse_client,
        _dashboard_listener: dashboard_listener,
        _cdn_listener: cdn_listener,
    })
}

fn result_detail(result: &Result<()>) -> String {
    result
        .as_ref()
        .map(|()| "stopped".to_owned())
        .unwrap_or_else(|error| format!("failed ({error:#})"))
}

async fn retain_ambiguous_startup(detail: String) -> ! {
    eprintln!("{detail}; retaining child handles and the runtime lock for fail-closed diagnosis");
    std::future::pending::<()>().await;
    unreachable!("ambiguous startup state must retain its supervisor")
}

async fn start_postgresql(
    launch: &mut LaunchRecord<'_>,
    socket_dir: &Path,
) -> Result<(Child, u32, String)> {
    let paths = launch.paths;
    let postgres = paths.postgresql_binary(&launch.manifest.postgresql.version);
    validate_confined_regular_file(&paths.state, &postgres)?;
    let bin = postgres
        .parent()
        .context("PostgreSQL binary has no parent")?;
    ensure_private_dir(&paths.postgresql_data())?;
    if !paths.postgresql_data().join("PG_VERSION").exists() {
        // initdb writes the data directory, so it is a recorded launch child like the servers.
        let (initdb, _) = launch
            .spawn(
                Command::new(bin.join("initdb"))
                    .arg("-D")
                    .arg(paths.postgresql_data())
                    .arg("--username=mkdb2")
                    .arg("--auth-local=trust")
                    .arg("--auth-host=reject")
                    .arg("--encoding=UTF8")
                    .arg("--no-locale")
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::piped()),
                "initdb",
            )
            .await?;
        let output = initdb.wait_with_output().await?;
        ensure!(
            output.status.success(),
            "initialize PostgreSQL failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let (stdout, stderr) = component_log_stdio(&paths.state.join("postgresql.log"))?;
    let mut command = Command::new(&postgres);
    command
        .arg("-D")
        .arg(paths.postgresql_data())
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr);
    for (key, value) in postgresql_configuration() {
        command.arg("-c").arg(format!("{key}={value}"));
    }
    command
        .arg("-c")
        .arg(format!("unix_socket_directories={}", socket_dir.display()))
        .arg("-c")
        .arg(format!("port={POSTGRESQL_PORT}"));
    let (mut child, group) = launch.spawn(&mut command, "PostgreSQL").await?;
    let setup = async {
        wait_for_postgresql(&mut child, &bin.join("psql"), socket_dir).await?;
        let query = Command::new(bin.join("psql"))
            .arg("-h")
            .arg(socket_dir)
            .arg("-p")
            .arg(POSTGRESQL_PORT.to_string())
            .arg("-U")
            .arg("mkdb2")
            .arg("-d")
            .arg("postgres")
            .arg("-Atc")
            .arg("SELECT 1 FROM pg_database WHERE datname = 'mkdb2'")
            .output()
            .await?;
        ensure!(
            query.status.success(),
            "inspect PostgreSQL databases failed: {}",
            String::from_utf8_lossy(&query.stderr)
        );
        if String::from_utf8_lossy(&query.stdout).trim() != "1" {
            run_checked(
                Command::new(bin.join("createdb"))
                    .arg("-h")
                    .arg(socket_dir)
                    .arg("-p")
                    .arg(POSTGRESQL_PORT.to_string())
                    .arg("-U")
                    .arg("mkdb2")
                    .arg("mkdb2"),
                "create kymo PostgreSQL database (legacy database name mkdb2)",
            )
            .await?;
        }
        Result::<String>::Ok(format!(
            "postgresql://mkdb2@localhost/mkdb2?host={}&port={POSTGRESQL_PORT}",
            socket_dir.display()
        ))
    }
    .await;
    match setup {
        Ok(database_url) => Ok((child, group, database_url)),
        Err(error) => abort_startup_child(&mut child, "PostgreSQL", error).await,
    }
}

async fn start_clickhouse(
    launch: &mut LaunchRecord<'_>,
    generation: &PreparedGeneration,
    address: SocketAddr,
) -> Result<(Child, u32, reqwest::Client)> {
    let paths = launch.paths;
    let binary = paths.clickhouse_binary(&launch.manifest.clickhouse.version);
    validate_confined_regular_file(&paths.state, &binary)?;
    let (stdout, stderr) = component_log_stdio(&paths.state.join("clickhouse.log"))?;
    let (mut child, group) = launch
        .spawn(
            Command::new(binary)
                .arg("server")
                .arg(format!(
                    "--config-file={}",
                    generation.root.join("config.xml").display()
                ))
                .env("CLICKHOUSE_WATCHDOG_ENABLE", "0")
                .current_dir(&generation.root)
                .stdin(Stdio::null())
                .stdout(stdout)
                .stderr(stderr),
            "ClickHouse",
        )
        .await?;
    let setup = async {
        let certificate =
            reqwest::Certificate::from_pem(&std::fs::read(&generation.clickhouse_certificate)?)?;
        let client = reqwest::Client::builder()
            .https_only(true)
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5))
            .add_root_certificate(certificate)
            .build()?;
        wait_for_clickhouse(&mut child, &client, generation, address).await?;
        Result::<reqwest::Client>::Ok(client)
    }
    .await;
    match setup {
        Ok(client) => Ok((child, group, client)),
        Err(error) => abort_startup_child(&mut child, "ClickHouse", error).await,
    }
}

async fn start_server(
    launch: &mut LaunchRecord<'_>,
    generation: &PreparedGeneration,
    endpoints: &RuntimeEndpoints,
    database_url: &str,
    (dashboard_listener, cdn_listener): (TcpListener, TcpListener),
) -> Result<(Child, u32, Vec<OwnedSocket>)> {
    let paths = launch.paths;
    let binary = server_binary()?;
    let dashboard_addr = dashboard_listener.local_addr()?;
    let cdn_addr = cdn_listener.local_addr()?;
    let dashboard_source = dashboard_listener.as_raw_fd();
    let cdn_source = cdn_listener.as_raw_fd();
    let (stdout, stderr) = component_log_stdio(&paths.state.join("server.log"))?;
    let mut command = Command::new(binary);
    command
        .env("KYMO_SERVER_MODE", "local")
        .env("DATABASE_URL", database_url)
        .env(
            "CLICKHOUSE_URL",
            format!("https://localhost:{}", endpoints.clickhouse_addr.port()),
        )
        .env("CLICKHOUSE_USER", CLICKHOUSE_USER)
        .env("CLICKHOUSE_PASSWORD", &generation.clickhouse_password)
        .env(
            "CLICKHOUSE_SERVER_CERT_PATH",
            &generation.clickhouse_certificate,
        )
        .env("LOCAL_GRPC_SOCKET", &endpoints.native_socket)
        .env("LOCAL_CDN_UPLOAD_SOCKET", &endpoints.upload_socket)
        .env("LOCAL_AUTH_SECRET_PATH", &generation.auth_secret)
        .env(
            "LOCAL_SERVER_LIFECYCLE_SECRET_PATH",
            &generation.lifecycle_secret,
        )
        .env("DASHBOARD_LISTEN_ADDR", dashboard_addr.to_string())
        .env("DASHBOARD_LISTENER_FD", DASHBOARD_LISTENER_FD.to_string())
        .env("CDN_LISTEN_ADDR", cdn_addr.to_string())
        .env("CDN_LISTENER_FD", CDN_LISTENER_FD.to_string())
        .env("KYMO_CDN_ROOT", paths.cdn_data())
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr);
    unsafe {
        command.as_std_mut().pre_exec(move || {
            dup_to(dashboard_source, DASHBOARD_LISTENER_FD)?;
            dup_to(cdn_source, CDN_LISTENER_FD)
        });
    }
    let (mut child, group) = launch.spawn(&mut command, "kymo-server").await?;
    let ready = async {
        wait_for_server(
            &mut child,
            &endpoints.native_socket,
            &generation.server_bearer,
        )
        .await?;
        [
            endpoints.native_socket.as_path(),
            endpoints.upload_socket.as_path(),
        ]
        .into_iter()
        .map(OwnedSocket::capture)
        .collect::<Result<Vec<_>>>()
    }
    .await;
    match ready {
        Ok(sockets) => Ok((child, group, sockets)),
        Err(error) => abort_startup_child(&mut child, "kymo-server", error).await,
    }
}

async fn abort_startup_child<T>(child: &mut Child, name: &str, error: anyhow::Error) -> Result<T> {
    let process_group = child_group(child, name)?;
    if child.try_wait()?.is_none() {
        // These are still direct, unreaped startup children. SIGTERM is the
        // native initial stop path for all three.
        let result = unsafe { libc::kill(-(process_group as i32), libc::SIGTERM) };
        ensure!(result == 0, "failed to terminate {name} after: {error:#}");
        match tokio::time::timeout(STOP_TIMEOUT, child.wait()).await {
            Ok(status) => {
                let _ = status?;
            }
            Err(_) => {
                retain_ambiguous_startup(format!(
                    "{name} ignored SIGTERM after startup failure: {error:#}"
                ))
                .await
            }
        }
    }
    ensure_process_group_gone(process_group, name)?;
    Err(error)
}

async fn stop_children(
    generation: &PreparedGeneration,
    native_socket: &Path,
    paths: &RuntimePaths,
    children: &mut Children,
) -> Result<()> {
    let server = stop_server(
        generation,
        native_socket,
        &mut children.server,
        children.server_group,
        &children.server_sockets,
    )
    .await;
    let clickhouse = stop_clickhouse(
        generation,
        &children.clickhouse_client,
        &mut children.clickhouse,
        children.clickhouse_group,
        paths,
    )
    .await;
    let postgresql = stop_postgresql(
        paths,
        &mut children.postgresql,
        children.postgresql_group,
        &children.postgresql_socket_dir,
    )
    .await;
    let failures = [server, clickhouse, postgresql]
        .into_iter()
        .filter_map(Result::err)
        .map(|error| format!("{error:#}"))
        .collect::<Vec<_>>();
    ensure!(failures.is_empty(), "{}", failures.join("; "));
    Ok(())
}

async fn stop_server(
    generation: &PreparedGeneration,
    native_socket: &Path,
    child: &mut Child,
    process_group: u32,
    sockets: &[OwnedSocket],
) -> Result<()> {
    if !child_has_exited(child, process_group, "kymo-server")? {
        // The process handle is the shutdown proof: a successful self-fence can close the channel before tonic receives the response.
        let request = lifecycle_call(native_socket, STOP_TIMEOUT, async |mut client| {
            client
                .shutdown_local(bearer_request(
                    &generation.lifecycle_bearer,
                    ShutdownLocalRequest {},
                )?)
                .await?;
            Ok(())
        })
        .await;
        if let Err(error) = request {
            eprintln!("kymo-server shutdown request failed: {error:#}");
        }
        if tokio::time::timeout(STOP_TIMEOUT, child.wait())
            .await
            .is_err()
        {
            // SIGTERM runs the same bounded drain as ShutdownLocal, for a server whose control socket is gone or stuck.
            eprintln!("kymo-server did not stop after its shutdown request; sending SIGTERM");
            let result = unsafe { libc::kill(-(process_group as i32), libc::SIGTERM) };
            ensure!(
                result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH),
                "failed to send SIGTERM to kymo-server"
            );
        }
        wait_for_child(child, process_group, "kymo-server").await?;
    }
    for socket in sockets {
        socket.remove_if_unchanged()?;
    }
    Ok(())
}

async fn stop_clickhouse(
    generation: &PreparedGeneration,
    client: &reqwest::Client,
    child: &mut Child,
    process_group: u32,
    paths: &RuntimePaths,
) -> Result<()> {
    if !child_has_exited(child, process_group, "ClickHouse")? {
        let response = client
            .post(format!("https://localhost:{}/", generation.clickhouse_port))
            .header("X-ClickHouse-User", CLICKHOUSE_USER)
            .header("X-ClickHouse-Key", &generation.clickhouse_password)
            .body("SYSTEM SHUTDOWN")
            .send()
            .await;
        // ClickHouse may close the HTTP connection while SYSTEM SHUTDOWN is being
        // answered. The retained child handle, not an HTTP response, proves stop.
        let _ = response;
        wait_for_child(child, process_group, "ClickHouse").await?;
    }
    ensure!(
        !paths.clickhouse_data().join("clickhouse.pid").exists(),
        "ClickHouse pidfile remains after shutdown"
    );
    let endpoint_closed = client
        .get(format!("https://localhost:{}/", generation.clickhouse_port))
        .send()
        .await
        .is_err();
    ensure!(
        endpoint_closed,
        "ClickHouse endpoint remains after shutdown"
    );
    Ok(())
}

async fn stop_postgresql(
    paths: &RuntimePaths,
    child: &mut Child,
    process_group: u32,
    socket_dir: &Path,
) -> Result<()> {
    if !child_has_exited(child, process_group, "PostgreSQL")? {
        let pg_ctl = paths
            .postgresql_binary(&for_target(SupportedTarget::current()?)?.postgresql.version)
            .parent()
            .context("PostgreSQL binary has no parent")?
            .join("pg_ctl");
        run_checked(
            Command::new(pg_ctl)
                .arg("-D")
                .arg(paths.postgresql_data())
                .arg("stop")
                .arg("-m")
                .arg("fast")
                .arg("-w"),
            "stop PostgreSQL",
        )
        .await?;
        wait_for_child(child, process_group, "PostgreSQL").await?;
    }
    ensure!(
        !paths.postgresql_data().join("postmaster.pid").exists(),
        "PostgreSQL postmaster.pid remains after shutdown"
    );
    ensure!(
        !socket_dir
            .join(format!(".s.PGSQL.{POSTGRESQL_PORT}"))
            .exists(),
        "PostgreSQL socket remains after shutdown"
    );
    Ok(())
}

async fn wait_for_server(child: &mut Child, socket: &Path, bearer: &str) -> Result<()> {
    let deadline = tokio::time::Instant::now() + START_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait()? {
            bail!("kymo-server exited during startup: {status}");
        }
        if let Ok(channel) = native_channel(socket).await {
            let mut client = KymoClient::new(channel);
            if client
                .list_projects(bearer_request(bearer, ListProjectsRequest {})?)
                .await
                .is_ok()
            {
                return Ok(());
            }
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "kymo-server did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn native_channel(socket: &Path) -> Result<Channel> {
    let socket = socket.to_owned();
    Endpoint::try_from("http://[::]:50051")?
        .connect_with_connector(service_fn(move |_| {
            let socket = socket.clone();
            async move { UnixStream::connect(socket).await.map(TokioIo::new) }
        }))
        .await
        .context("connect to local native gRPC socket")
}

async fn wait_for_clickhouse(
    child: &mut Child,
    client: &reqwest::Client,
    generation: &PreparedGeneration,
    address: SocketAddr,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + START_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait()? {
            bail!("ClickHouse exited during startup: {status}");
        }
        let ready = match client
            .get(format!("https://localhost:{}/", address.port()))
            .header("X-ClickHouse-User", CLICKHOUSE_USER)
            .header("X-ClickHouse-Key", &generation.clickhouse_password)
            .query(&[("query", "SELECT 1")])
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                response.text().await.is_ok_and(|body| body.trim() == "1")
            }
            _ => false,
        };
        if ready {
            return Ok(());
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "ClickHouse did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_postgresql(child: &mut Child, psql: &Path, socket_dir: &Path) -> Result<()> {
    let deadline = tokio::time::Instant::now() + START_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait()? {
            bail!("PostgreSQL exited during startup: {status}");
        }
        let output = Command::new(psql)
            .arg("-h")
            .arg(socket_dir)
            .arg("-p")
            .arg(POSTGRESQL_PORT.to_string())
            .arg("-U")
            .arg("mkdb2")
            .arg("-d")
            .arg("postgres")
            .arg("-Atc")
            .arg("SELECT 1")
            .output()
            .await?;
        let ready =
            output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "1";
        if ready {
            return Ok(());
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "PostgreSQL did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_child(child: &mut Child, process_group: u32, name: &str) -> Result<()> {
    let status = tokio::time::timeout(STOP_TIMEOUT, child.wait())
        .await
        .with_context(|| format!("{name} did not stop within {STOP_TIMEOUT:?}"))??;
    ensure_process_group_gone(process_group, name)?;
    if !status.success() {
        eprintln!("{name} exited with {status} after its shutdown request");
    }
    Ok(())
}

fn child_group(child: &Child, name: &str) -> Result<u32> {
    child
        .id()
        .with_context(|| format!("{name} has no process id"))
}

fn child_has_exited(child: &mut Child, process_group: u32, name: &str) -> Result<bool> {
    let Some(status) = child.try_wait()? else {
        return Ok(false);
    };
    ensure_process_group_gone(process_group, name)?;
    if !status.success() {
        eprintln!("{name} had already exited with {status}");
    }
    Ok(true)
}

fn ensure_process_group_gone(process_group: u32, name: &str) -> Result<()> {
    ensure!(
        process_group_gone(process_group),
        "{name} process group {process_group} still has a live member"
    );
    Ok(())
}

// Every recorded process leads its own group, so an empty group proves its descendants are gone too.
// Database PID files are reconstructible crash residue, not process-ownership evidence: new database processes validate or replace them only after this proof.
fn process_group_gone(process_group: u32) -> bool {
    let result = unsafe { libc::kill(-(process_group as i32), 0) };
    result == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
}

async fn run_checked(command: &mut Command, name: &str) -> Result<()> {
    let output = command.output().await.with_context(|| name.to_owned())?;
    ensure!(
        output.status.success(),
        "{name} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

/// Keyed on the state root, not the installation UUID: a copied installation shares the UUID, and its start must never clean the original's live sockets.
fn runtime_root(paths: &RuntimePaths) -> Result<PathBuf> {
    let base = runtime_base()?;
    let user = base.join(format!("kymo-{}", unsafe { libc::geteuid() }));
    ensure_private_dir(&user)?;
    let digest = Sha256::digest(paths.state.as_os_str().as_bytes());
    Ok(user.join(&format!("{digest:x}")[..12]))
}

#[cfg(target_os = "linux")]
fn runtime_base() -> Result<PathBuf> {
    if let Some(value) = std::env::var_os("XDG_RUNTIME_DIR") {
        let path = PathBuf::from(value);
        ensure!(path.is_absolute(), "XDG_RUNTIME_DIR must be absolute");
        let canonical = path
            .canonicalize()
            .context("canonicalize XDG_RUNTIME_DIR")?;
        ensure!(canonical == path, "XDG_RUNTIME_DIR must be canonical");
        kymo_local_runtime_core::paths::validate_private_dir(&canonical)?;
        return Ok(canonical);
    }
    PathBuf::from("/tmp")
        .canonicalize()
        .context("canonicalize /tmp runtime fallback")
}

#[cfg(target_os = "macos")]
fn runtime_base() -> Result<PathBuf> {
    // Rust's macOS temp_dir implementation uses confstr(_CS_DARWIN_USER_TEMP_DIR).
    // Canonicalizing once here keeps every generated socket out of /tmp aliases.
    std::env::temp_dir()
        .canonicalize()
        .context("canonicalize macOS per-user runtime directory")
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("the kymo local supervisor supports only Linux and macOS");

fn clean_reconstructible_state(paths: &RuntimePaths, runtime_root: &Path) -> Result<()> {
    let generations = paths.generations();
    for root in [runtime_root, generations.as_path()] {
        match std::fs::symlink_metadata(root) {
            Ok(_) => {
                kymo_local_runtime_core::paths::validate_private_dir(root)?;
                std::fs::remove_dir_all(root).with_context(|| {
                    format!("remove stale reconstructible state {}", root.display())
                })?;
                sync_parent(root)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().context("managed path has no parent")?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn bind_control(path: &Path) -> Result<UnixListener> {
    reject_symlink(path)?;
    let listener = std::os::unix::net::UnixListener::bind(path)
        .with_context(|| format!("bind supervisor control socket {}", path.display()))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    listener.set_nonblocking(true)?;
    UnixListener::from_std(listener).map_err(Into::into)
}

fn unused_loopback_addr() -> Result<SocketAddr> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    Ok(listener.local_addr()?)
}

pub(crate) fn acquire_runtime_lock(paths: &RuntimePaths) -> Result<File> {
    try_acquire_runtime_lock(paths)?.context("local runtime is already being supervised or started")
}

pub(crate) fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

/// A launch past its whole budget is stuck in a fail-closed hold, not starting.
pub(crate) fn launch_is_overdue(launching: &LaunchIntent) -> bool {
    unix_time_ms().saturating_sub(launching.started_at_unix_ms) > LAUNCH_TIMEOUT.as_millis() as u64
}

pub(crate) fn runtime_lock_is_held(paths: &RuntimePaths) -> bool {
    matches!(try_acquire_runtime_lock(paths), Ok(None))
}

pub(crate) fn start_gate_is_held(paths: &RuntimePaths) -> bool {
    matches!(try_acquire_start_gate(paths), Ok(None))
}

fn try_acquire_runtime_lock(paths: &RuntimePaths) -> Result<Option<File>> {
    try_lock(&paths.state.join("runtime.lock"))
}

/// Held by a starter only while it launches (never inherited by the supervisor), and by `kymo stop` for its whole run.
fn try_acquire_start_gate(paths: &RuntimePaths) -> Result<Option<File>> {
    try_lock(&paths.state.join("start.lock"))
}

fn try_lock(path: &Path) -> Result<Option<File>> {
    let lock = open_lock_file(path)?;
    match lock.try_lock_exclusive() {
        Ok(()) => Ok(Some(lock)),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
        Err(error) => Err(error).with_context(|| format!("lock {}", path.display())),
    }
}

pub(crate) fn open_lock_file(path: &Path) -> Result<File> {
    reject_symlink(path)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;
    ensure!(
        !on_network_filesystem(&file)?,
        "{} is on NFS, where the lock cannot pass to the supervisor, so two stacks could share one data directory; set KYMO_LOCAL_ROOT to a local disk",
        path.display()
    );
    Ok(file)
}

/// The lifecycle proof is an flock inherited by the supervisor; NFS emulates flock with per-process locks that do not survive that hand-off.
fn on_network_filesystem(file: &File) -> Result<bool> {
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    ensure!(
        unsafe { libc::fstatfs(file.as_raw_fd(), stats.as_mut_ptr()) } == 0,
        "inspect lock filesystem: {}",
        std::io::Error::last_os_error()
    );
    let stats = unsafe { stats.assume_init() };
    #[cfg(target_os = "linux")]
    return Ok(stats.f_type == libc::NFS_SUPER_MAGIC);
    #[cfg(target_os = "macos")]
    return Ok(
        unsafe { std::ffi::CStr::from_ptr(stats.f_fstypename.as_ptr()) }
            .to_bytes()
            .starts_with(b"nfs"),
    );
}

fn open_log(path: &Path) -> Result<File> {
    prepare_log_family(path, COMPONENT_LOG_MAX_BYTES, COMPONENT_LOG_BACKUPS)?;
    open_log_file(path)
}

fn open_log_file(path: &Path) -> Result<File> {
    reject_symlink(path)?;
    if path.exists() {
        validate_private_file(path)?;
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .with_context(|| format!("open log {}", path.display()))
}

struct RotatingLog {
    path: PathBuf,
    file: File,
    bytes: u64,
    max_bytes: u64,
    backups: usize,
}

impl RotatingLog {
    fn open(path: &Path) -> Result<Self> {
        Self::open_with_limits(path, COMPONENT_LOG_MAX_BYTES, COMPONENT_LOG_BACKUPS)
    }

    fn open_with_limits(path: &Path, max_bytes: u64, backups: usize) -> Result<Self> {
        ensure!(max_bytes > 0, "component log limit must be nonzero");
        ensure!(backups > 0, "component log backup count must be nonzero");
        prepare_log_family(path, max_bytes, backups)?;
        let file = open_log_file(path)?;
        let bytes = file.metadata()?.len();
        Ok(Self {
            path: path.to_owned(),
            file,
            bytes,
            max_bytes,
            backups,
        })
    }

    fn write_bounded(&mut self, mut bytes: &[u8]) -> Result<()> {
        while !bytes.is_empty() {
            if self.bytes == self.max_bytes {
                self.rotate()?;
            }
            let available = (self.max_bytes - self.bytes) as usize;
            let count = available.min(bytes.len());
            self.file.write_all(&bytes[..count])?;
            self.bytes += count as u64;
            bytes = &bytes[count..];
        }
        Ok(())
    }

    fn rotate(&mut self) -> Result<()> {
        self.file.flush()?;
        rotate_log_family(&self.path, self.max_bytes, self.backups)?;
        self.file = open_log_file(&self.path)?;
        self.bytes = 0;
        Ok(())
    }
}

fn component_log_stdio(path: &Path) -> Result<(Stdio, Stdio)> {
    let log = RotatingLog::open(path)?;
    let (reader, writer) = StdUnixStream::pair().context("create component log pipe")?;
    let stdout = Stdio::from(OwnedFd::from(writer.try_clone()?));
    let stderr = Stdio::from(OwnedFd::from(writer));
    spawn_log_writer(
        reader,
        log,
        format!(
            "kymo-log-{}",
            path.file_stem()
                .and_then(|name| name.to_str())
                .unwrap_or("component")
        ),
        true,
    )?;
    Ok((stdout, stderr))
}

fn install_rotating_process_log(path: &Path) -> Result<()> {
    let log = RotatingLog::open(path)?;
    let (reader, writer) = StdUnixStream::pair().context("create supervisor log pipe")?;
    spawn_log_writer(reader, log, "kymo-log-supervisor".to_owned(), false)?;
    unsafe {
        dup_to(writer.as_raw_fd(), libc::STDOUT_FILENO)?;
        dup_to(writer.as_raw_fd(), libc::STDERR_FILENO)?;
    }
    Ok(())
}

fn spawn_log_writer(
    mut reader: StdUnixStream,
    mut log: RotatingLog,
    thread_name: String,
    report_errors: bool,
) -> Result<()> {
    std::thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let mut buffer = [0_u8; 16 * 1024];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(count) => {
                        if let Err(error) = log.write_bounded(&buffer[..count]) {
                            if report_errors {
                                eprintln!("kymo: component log writer failed: {error:#}");
                            }
                            break;
                        }
                    }
                    Err(error) => {
                        if report_errors {
                            eprintln!("kymo: component log pipe failed: {error}");
                        }
                        break;
                    }
                }
            }
            if let Err(error) = log.file.flush()
                && report_errors
            {
                eprintln!("kymo: component log flush failed: {error}");
            }
        })
        .context("spawn component log writer")?;
    Ok(())
}

fn prepare_log_family(path: &Path, max_bytes: u64, backups: usize) -> Result<()> {
    for index in 1..=backups {
        cap_log_file(&rotated_log_path(path, index), max_bytes)?;
    }
    cap_log_file(path, max_bytes)?;
    if path.exists() && path.metadata()?.len() >= max_bytes {
        rotate_log_family(path, max_bytes, backups)?;
    }
    Ok(())
}

fn rotate_log_family(path: &Path, max_bytes: u64, backups: usize) -> Result<()> {
    let oldest = rotated_log_path(path, backups);
    if oldest.exists() {
        validate_private_file(&oldest)?;
        std::fs::remove_file(&oldest)
            .with_context(|| format!("remove old log {}", oldest.display()))?;
    }
    for index in (1..=backups).rev() {
        let source = if index == 1 {
            path.to_owned()
        } else {
            rotated_log_path(path, index - 1)
        };
        if !source.exists() {
            continue;
        }
        validate_private_file(&source)?;
        let destination = rotated_log_path(path, index);
        reject_symlink(&destination)?;
        std::fs::rename(&source, &destination).with_context(|| {
            format!(
                "rotate component log {} to {}",
                source.display(),
                destination.display()
            )
        })?;
        cap_log_file(&destination, max_bytes)?;
    }
    Ok(())
}

fn cap_log_file(path: &Path, max_bytes: u64) -> Result<()> {
    reject_symlink(path)?;
    if !path.exists() {
        return Ok(());
    }
    validate_private_file(path)?;
    if path.metadata()?.len() <= max_bytes {
        return Ok(());
    }
    OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .with_context(|| format!("open oversized log {}", path.display()))?
        .set_len(max_bytes)
        .with_context(|| format!("cap oversized log {}", path.display()))
}

fn rotated_log_path(path: &Path, index: usize) -> PathBuf {
    let mut name = path
        .file_name()
        .expect("component log path has a file name")
        .to_os_string();
    name.push(format!(".{index}"));
    path.with_file_name(name)
}

fn server_binary() -> Result<PathBuf> {
    let current = std::env::current_exe()?.canonicalize()?;
    let binary = current.with_file_name("kymo-server");
    ensure!(
        binary.is_file(),
        "bundled kymo-server is missing: {}",
        binary.display()
    );
    Ok(binary)
}

fn child_identity(child: &Child, executable: PathBuf) -> Result<ProcessIdentity> {
    process_identity(child.id().context("child has no process id")?, executable)
}

fn process_identity(pid: u32, executable: PathBuf) -> Result<ProcessIdentity> {
    let system = System::new_all();
    let process = system
        .process(Pid::from_u32(pid))
        .with_context(|| format!("process {pid} is not running"))?;
    let actual_executable = process
        .exe()
        .with_context(|| format!("process {pid} has no executable path"))?
        .canonicalize()?;
    let executable = executable.canonicalize()?;
    let owner_uid = process
        .effective_user_id()
        .context("process has no effective user identity")?;
    ensure!(
        **owner_uid == unsafe { libc::geteuid() },
        "process {pid} is owned by a different OS account"
    );
    ensure!(
        actual_executable == executable,
        "process {pid} executable is {}, expected {}",
        actual_executable.display(),
        executable.display()
    );
    Ok(ProcessIdentity {
        pid,
        owner_uid: **owner_uid,
        executable,
        start_identity: process.start_time().to_string(),
    })
}

/// A recorded process is alive only if its PID still has its recorded start time. The executable path is not compared: a live process whose binary an upgrade replaced no longer resolves it.
pub(crate) fn recorded_process_alive(identity: &ProcessIdentity) -> bool {
    process_start_identity(identity.pid).as_deref() == Some(identity.start_identity.as_str())
}

/// A recorded group leader's PID now held by a process with a different start time proves reuse, and the kernel never reuses a number that still names a process group; otherwise the group itself must be empty.
fn recorded_group_gone(identity: &ProcessIdentity) -> bool {
    match process_start_identity(identity.pid) {
        Some(start) => start != identity.start_identity,
        None => process_group_gone(identity.pid),
    }
}

fn process_start_identity(pid: u32) -> Option<String> {
    let mut system = System::new();
    system.refresh_processes(
        sysinfo::ProcessesToUpdate::Some(&[Pid::from_u32(pid)]),
        true,
    );
    system
        .process(Pid::from_u32(pid))
        .map(|process| process.start_time().to_string())
}

pub(crate) fn recorded_stack_is_quiescent(running: &RunningStack) -> bool {
    running.processes().into_iter().all(recorded_group_gone)
}

fn live_launch_groups(launching: &LaunchIntent) -> Vec<u32> {
    let mut live: Vec<u32> = launching
        .child_process_groups
        .iter()
        .copied()
        .filter(|&group| !process_group_gone(group))
        .collect();
    if !recorded_group_gone(&launching.supervisor) {
        live.insert(0, launching.supervisor.pid);
    }
    live
}

fn read_ready(stream: StdUnixStream) -> Result<ReadyMessage> {
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line)?;
    ensure!(
        !line.is_empty(),
        "supervisor exited without a readiness result"
    );
    serde_json::from_str(&line).context("parse supervisor readiness result")
}

fn write_ready(mut stream: &StdUnixStream, message: &ReadyMessage) -> Result<()> {
    serde_json::to_writer(&mut stream, message)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    Ok(())
}

unsafe fn dup_to(source: RawFd, target: RawFd) -> std::io::Result<()> {
    if source == target {
        let flags = unsafe { libc::fcntl(target, libc::F_GETFD) };
        if flags == -1
            || unsafe { libc::fcntl(target, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } == -1
        {
            return Err(std::io::Error::last_os_error());
        }
    } else if unsafe { libc::dup2(source, target) } == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn set_close_on_exec(fd: RawFd) -> std::io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags == -1 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn idle_snapshot(idle_for: Duration) -> GetActivityResponse {
        GetActivityResponse {
            keepalive_idle_for_ms: idle_for.as_millis() as u64,
            last_committed_ingest_ago_ms: None,
            frontend_reconnect_grace_remaining_ms: 0,
            frontend_connections: 0,
            in_flight_work: 0,
            ingest_bookkeeping_draining: false,
            fulfilled_hold_ids: Vec::new(),
        }
    }

    fn default_idle_eligible(
        snapshot: &GetActivityResponse,
        holds: &HoldSet,
        now: tokio::time::Instant,
    ) -> bool {
        idle_eligible(snapshot, holds, now, crate::liveness::LOCAL_IDLE_TIMEOUT)
    }

    #[test]
    fn idle_requires_the_full_shared_threshold_and_every_quiescence_signal() {
        let now = tokio::time::Instant::now();
        let holds = HoldSet::default();
        let mut snapshot = idle_snapshot(crate::liveness::LOCAL_IDLE_TIMEOUT);
        assert!(default_idle_eligible(&snapshot, &holds, now));

        snapshot.keepalive_idle_for_ms -= 1;
        assert!(!default_idle_eligible(&snapshot, &holds, now));
        snapshot.keepalive_idle_for_ms += 1;

        snapshot.frontend_reconnect_grace_remaining_ms = 1;
        assert!(!default_idle_eligible(&snapshot, &holds, now));
        snapshot.frontend_reconnect_grace_remaining_ms = 0;
        snapshot.frontend_connections = 1;
        assert!(!default_idle_eligible(&snapshot, &holds, now));
        snapshot.frontend_connections = 0;
        snapshot.in_flight_work = 1;
        assert!(!default_idle_eligible(&snapshot, &holds, now));
        snapshot.in_flight_work = 0;
        snapshot.ingest_bookkeeping_draining = true;
        assert!(!default_idle_eligible(&snapshot, &holds, now));
    }

    #[test]
    fn fulfilled_init_ids_clear_only_the_matching_supervisor_hold() {
        let now = tokio::time::Instant::now();
        let first = Uuid::from_u128(1);
        let second = Uuid::from_u128(2);
        let mut holds = HoldSet::default();
        holds.add_init(first, now);
        holds.add_init(second, now);
        assert!(holds.blocks_idle(now));

        let mut snapshot = idle_snapshot(crate::liveness::LOCAL_IDLE_TIMEOUT);
        snapshot.fulfilled_hold_ids = vec![first.to_string()];
        holds.observe_snapshot(&snapshot, now);
        assert!(!holds.deadlines.contains_key(&first.to_string()));
        assert!(holds.deadlines.contains_key(&second.to_string()));
        assert!(holds.blocks_idle(now));

        snapshot.fulfilled_hold_ids = vec![second.to_string()];
        holds.observe_snapshot(&snapshot, now);
        assert!(!holds.blocks_idle(now));
    }

    #[test]
    fn post_wake_hold_is_bounded_on_the_supervisor_clock() {
        let now = tokio::time::Instant::now();
        let mut holds = HoldSet::default();
        holds.add_post_wake(now);
        assert_eq!(holds.hold_until(), Some(now + SHORT_HOLD));
        assert!(holds.blocks_idle(now + SHORT_HOLD - Duration::from_millis(1)));

        holds.observe_snapshot(
            &idle_snapshot(crate::liveness::LOCAL_IDLE_TIMEOUT),
            now + SHORT_HOLD,
        );
        assert!(!holds.blocks_idle(now + SHORT_HOLD));
    }

    #[test]
    fn bare_ensure_gets_one_short_non_init_hold() {
        let now = tokio::time::Instant::now();
        let mut holds = HoldSet::default();
        holds.add_bare_ensure(now);
        assert_eq!(holds.hold_until(), Some(now + SHORT_HOLD));
        assert!(holds.blocks_idle(now + SHORT_HOLD - Duration::from_millis(1)));
        holds.observe_snapshot(
            &idle_snapshot(crate::liveness::LOCAL_IDLE_TIMEOUT),
            now + SHORT_HOLD,
        );
        assert!(!holds.blocks_idle(now + SHORT_HOLD));
    }

    #[test]
    fn open_gets_one_timed_browser_start_hold() {
        let now = tokio::time::Instant::now();
        let mut holds = HoldSet::default();
        let id = Uuid::from_u128(7);
        holds.add_open(id, now);
        assert_eq!(holds.hold_until(), Some(now + INIT_HOLD_TIMEOUT));
        assert!(holds.deadlines.contains_key(&format!("open:{id}")));
        holds.observe_snapshot(
            &idle_snapshot(crate::liveness::LOCAL_IDLE_TIMEOUT),
            now + INIT_HOLD_TIMEOUT,
        );
        assert!(!holds.blocks_idle(now + INIT_HOLD_TIMEOUT));
    }

    #[test]
    fn suspend_and_wall_clock_discontinuities_are_detected_without_aging_idle_time() {
        assert!(!polling_discontinuity(
            Duration::from_secs(1),
            Some(Duration::from_secs(1))
        ));
        assert!(polling_discontinuity(
            SUSPEND_GAP + Duration::from_millis(1),
            Some(SUSPEND_GAP + Duration::from_millis(1))
        ));
        assert!(polling_discontinuity(
            Duration::from_secs(1),
            Some(Duration::from_secs(10))
        ));
        assert!(polling_discontinuity(Duration::from_secs(1), None));
    }

    #[test]
    fn ensure_output_contains_only_client_scoped_generation_data() {
        let output = EnsureOutput {
            protocol_min: 2,
            protocol_max: 2,
            installation_uuid: Uuid::nil(),
            endpoint_generation: Uuid::from_u128(u128::MAX),
            native_socket: PathBuf::from("/private/grpc.sock"),
            upload_socket: PathBuf::from("/private/upload.sock"),
            dashboard_origin: "http://127.0.0.1:10001".to_owned(),
            cdn_origin: "http://127.0.0.1:10002".to_owned(),
            server_bearer: "client-only".to_owned(),
        };
        let value = serde_json::to_value(output).unwrap();
        let mut fields = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();
        fields.sort();
        assert_eq!(
            fields,
            [
                "cdn_origin",
                "dashboard_origin",
                "endpoint_generation",
                "installation_uuid",
                "native_socket",
                "protocol_max",
                "protocol_min",
                "server_bearer",
                "upload_socket",
            ]
        );
    }

    #[test]
    fn process_identity_includes_current_owner_and_start_time() {
        let identity = process_identity(std::process::id(), std::env::current_exe().unwrap())
            .expect("identify current test process");
        assert_eq!(identity.owner_uid, unsafe { libc::geteuid() });
        assert!(!identity.start_identity.is_empty());
        assert!(recorded_process_alive(&identity));
    }

    #[test]
    fn inherited_supervisor_descriptors_are_closed_for_child_execs() {
        let file = tempfile::tempfile().unwrap();
        let fd = file.as_raw_fd();
        set_close_on_exec(fd).unwrap();
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        assert_ne!(flags, -1);
        assert_ne!(flags & libc::FD_CLOEXEC, 0);
    }

    #[test]
    fn component_logs_rotate_under_a_fixed_aggregate_cap() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().canonicalize().unwrap().join("server.log");
        let mut log = RotatingLog::open_with_limits(&path, 16, 3).unwrap();
        log.write_bounded(&[b'x'; 82]).unwrap();
        log.file.flush().unwrap();
        drop(log);

        let paths = std::iter::once(path.clone())
            .chain((1..=3).map(|index| rotated_log_path(&path, index)))
            .collect::<Vec<_>>();
        let mut total = 0;
        for path in paths {
            let metadata = std::fs::metadata(path).unwrap();
            assert!(metadata.len() <= 16);
            assert_eq!(metadata.mode() & 0o777, 0o600);
            total += metadata.len();
        }
        assert!(total <= 64);
    }

    fn installed_paths() -> (tempfile::TempDir, RuntimePaths, RuntimeManifest) {
        let temporary = tempfile::tempdir().unwrap();
        let paths =
            RuntimePaths::under(temporary.path().canonicalize().unwrap().join("kymo")).unwrap();
        paths.prepare().unwrap();
        let artifacts = for_target(SupportedTarget::current().unwrap()).unwrap();
        let installation_uuid =
            kymo_local_runtime_core::manifest::initialize_installation_uuid(&paths).unwrap();
        let manifest = RuntimeManifest::installed(
            &artifacts,
            installation_uuid,
            env!("CARGO_PKG_VERSION"),
            40_001,
            40_002,
        );
        manifest.write_atomic(&paths.manifest()).unwrap();
        (temporary, paths, manifest)
    }

    fn exited_process_group() -> u32 {
        let mut child = std::process::Command::new("true")
            .process_group(0)
            .spawn()
            .unwrap();
        let pid = child.id();
        child.wait().unwrap();
        pid
    }

    fn interrupted_launch(child_process_groups: Vec<u32>) -> LaunchIntent {
        let pid = exited_process_group();
        LaunchIntent {
            supervisor: ProcessIdentity {
                pid,
                owner_uid: unsafe { libc::geteuid() },
                executable: std::env::current_exe().unwrap(),
                start_identity: "exited".to_owned(),
            },
            started_at_unix_ms: 0,
            child_process_groups,
        }
    }

    #[test]
    fn an_interrupted_launch_is_cleared_once_every_recorded_group_is_gone() {
        let _serial = crate::serialize_process_test();
        let (_temporary, paths, mut manifest) = installed_paths();
        manifest.launching = Some(interrupted_launch(vec![exited_process_group()]));
        clear_stopped_remnants(&paths, &mut manifest).unwrap();
        assert!(manifest.launching.is_none());
        assert!(manifest.degraded_reason.is_some());
        assert_eq!(RuntimeManifest::read(&paths.manifest()).unwrap(), manifest);
    }

    #[test]
    fn an_interrupted_launch_with_a_live_child_fails_closed() {
        let _serial = crate::serialize_process_test();
        let (_temporary, paths, mut manifest) = installed_paths();
        let mut orphan = std::process::Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .unwrap();
        manifest.launching = Some(interrupted_launch(vec![orphan.id()]));
        let error = clear_stopped_remnants(&paths, &mut manifest).unwrap_err();
        orphan.kill().unwrap();
        orphan.wait().unwrap();
        assert!(error.to_string().contains("live process"));
        assert!(manifest.launching.is_some());
    }

    #[test]
    fn clearing_remnants_records_a_newer_launcher() {
        let (_temporary, paths, mut manifest) = installed_paths();
        manifest.launcher_version = "0.0.0-0".to_owned();
        clear_stopped_remnants(&paths, &mut manifest).unwrap();
        assert_eq!(
            RuntimeManifest::read(&paths.manifest())
                .unwrap()
                .launcher_version,
            env!("CARGO_PKG_VERSION")
        );
    }

    #[test]
    fn browser_ports_change_only_to_free_ports_while_stopped() {
        let _serial = crate::serialize_process_test();
        let (_temporary, paths, _manifest) = installed_paths();
        let occupied = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let occupied_port = occupied.local_addr().unwrap().port();
        let (dashboard, cdn) =
            kymo_local_runtime_core::manifest::allocate_browser_ports(Uuid::new_v4()).unwrap();
        assert!(set_browser_ports(&paths, dashboard, occupied_port).is_err());
        assert!(set_browser_ports(&paths, dashboard, dashboard).is_err());
        set_browser_ports(&paths, dashboard, cdn).unwrap();
        let manifest = RuntimeManifest::read(&paths.manifest()).unwrap();
        assert_eq!(
            (manifest.dashboard_port, manifest.cdn_port),
            (dashboard, cdn)
        );

        let _supervisor = try_acquire_runtime_lock(&paths).unwrap().unwrap();
        assert!(runtime_lock_is_held(&paths));
        assert!(set_browser_ports(&paths, cdn, dashboard).is_err());
    }

    #[test]
    fn a_reused_leader_pid_proves_its_old_group_empty() {
        let _serial = crate::serialize_process_test();
        let live = process_identity(std::process::id(), std::env::current_exe().unwrap()).unwrap();
        assert!(!recorded_group_gone(&live));
        let reused = ProcessIdentity {
            start_identity: "0".to_owned(),
            ..live.clone()
        };
        assert!(recorded_group_gone(&reused));
        let exited = ProcessIdentity {
            pid: exited_process_group(),
            ..live
        };
        assert!(recorded_group_gone(&exited));
    }

    #[test]
    fn stop_terminates_orphaned_components_whose_identity_still_matches() {
        let _serial = crate::serialize_process_test();
        let child = std::process::Command::new("/bin/sleep")
            .arg("60")
            .process_group(0)
            .spawn()
            .unwrap();
        let orphan = process_identity(child.id(), PathBuf::from("/bin/sleep")).unwrap();
        // Reap concurrently: an unreaped zombie still counts as a live group member.
        let reaper = std::thread::spawn(move || {
            let mut child = child;
            child.wait()
        });
        let gone = ProcessIdentity {
            pid: exited_process_group(),
            ..orphan.clone()
        };
        let running = RunningStack {
            generation_uuid: Uuid::new_v4(),
            supervisor: gone.clone(),
            postgresql: gone.clone(),
            clickhouse: gone,
            server: orphan,
            native_socket: PathBuf::from("/nonexistent/grpc.sock"),
            upload_socket: PathBuf::from("/nonexistent/upload.sock"),
            control_socket: PathBuf::from("/nonexistent/control.sock"),
            dashboard_addr: "127.0.0.1:1".parse().unwrap(),
            cdn_addr: "127.0.0.1:2".parse().unwrap(),
            supervisor_secret: PathBuf::from("/nonexistent/supervisor.json"),
        };
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(terminate_orphaned_stack(&running));
        assert!(!reaper.join().unwrap().unwrap().success());
        assert!(recorded_stack_is_quiescent(&running));
    }

    #[test]
    fn socket_cleanup_requires_the_captured_device_and_inode() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("owned.sock");
        let replacement_path = temporary.path().join("replacement.sock");
        let first = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let owned = OwnedSocket::capture(&path).unwrap();
        let replacement = std::os::unix::net::UnixListener::bind(&replacement_path).unwrap();
        let replacement_metadata = std::fs::symlink_metadata(&replacement_path).unwrap();
        assert_ne!(owned.inode, replacement_metadata.ino());
        drop(first);
        std::fs::remove_file(&path).unwrap();
        std::fs::rename(&replacement_path, &path).unwrap();
        assert!(owned.remove_if_unchanged().is_err());
        assert!(path.exists());
        drop(replacement);

        let current = OwnedSocket::capture(&path).unwrap();
        current.remove_if_unchanged().unwrap();
        assert!(!path.exists());
    }
}
