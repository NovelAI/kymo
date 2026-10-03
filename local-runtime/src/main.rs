mod generation;
mod install;
#[path = "../../shared/liveness.rs"]
mod liveness;
#[path = "../../shared/retired_env.rs"]
mod retired_env;
mod supervisor;

use std::process::ExitCode;

use anyhow::{Context as _, Result};
use clap::{Parser, Subcommand};
use kymo_local_runtime_core::artifacts::{SupportedTarget, for_target};
use kymo_local_runtime_core::manifest::RuntimeManifest;
use kymo_local_runtime_core::paths::{
    RuntimePaths, validate_confined_regular_file, validate_private_dir,
};
use serde::Serialize;

#[derive(Debug, Parser)]
#[command(name = "kymo", version, about = "Manage the kymo local runtime")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Download and verify the pinned database runtime.
    Install,
    /// Start the local database and server stack if it is not running.
    Start,
    /// Install if needed, start if stopped, and return current client endpoints.
    Ensure {
        #[arg(long)]
        json: bool,
        /// Protect a local InitRun endpoint handoff until this exact id is acknowledged.
        #[arg(long, hide = true)]
        init_hold_id: Option<uuid::Uuid>,
    },
    /// Start the stack, hold it for browser startup, and open the dashboard, a project, or a run.
    Open {
        project_id: Option<String>,
        run_id: Option<String>,
        /// Refuse to open if the durable installation identity changed.
        #[arg(long, hide = true)]
        expected_installation_uuid: Option<uuid::Uuid>,
        /// Print the URL without invoking the operating-system browser helper.
        #[arg(long)]
        no_browser: bool,
    },
    /// Stop the local stack through component-native shutdown paths.
    Stop {
        /// Keep the stack stopped until interrupted, for copying or deleting its data.
        #[arg(long)]
        hold: bool,
    },
    /// Show the pinned dashboard and CDN ports, or replace them while the stack is stopped.
    Ports {
        #[arg(long, requires = "cdn")]
        dashboard: Option<u16>,
        #[arg(long, requires = "dashboard")]
        cdn: Option<u16>,
    },
    /// Report installed runtime state without changing it.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Validate the local installation without changing it.
    Doctor {
        #[arg(long)]
        json: bool,
    },
    #[command(name = "__supervise", hide = true)]
    Supervise,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum StatusState {
    NotInstalled,
    Invalid,
    Stopped,
    Starting,
    Running,
    Stopping,
    /// Stopped after a failure; the reason is diagnostic and the next start retries.
    Degraded,
}

#[derive(Debug, Serialize)]
struct StatusOutput {
    state: StatusState,
    manifest: Option<RuntimeManifest>,
    detail: String,
}

#[derive(Debug, Serialize)]
struct DoctorOutput {
    healthy: bool,
    checks: Vec<DoctorCheck>,
}

#[derive(Debug, Serialize)]
struct DoctorCheck {
    name: &'static str,
    healthy: bool,
    detail: String,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    supervisor::close_inherited_descriptors_on_exec()?;
    validate_environment_namespace()?;
    let paths = RuntimePaths::discover()?;
    match cli.command {
        Command::Install => {
            let manifest = install::install(&paths).await?;
            println!(
                "installed local database runtime for {}",
                manifest.installation_uuid
            );
        }
        Command::Start => {
            let output = supervisor::ensure_running(&paths, None, None).await?;
            println!("local stack is ready at {}", output.dashboard_origin);
        }
        Command::Ensure { json, init_hold_id } => {
            let output = supervisor::ensure_running(&paths, init_hold_id, None).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&output)?);
            } else {
                println!("local stack is ready at {}", output.dashboard_origin);
            }
        }
        Command::Open {
            project_id,
            run_id,
            expected_installation_uuid,
            no_browser,
        } => {
            if let Some(expected) = expected_installation_uuid {
                let artifacts = for_target(SupportedTarget::current()?)?;
                let manifest = RuntimeManifest::read_validated(
                    &paths,
                    &artifacts,
                    env!("CARGO_PKG_VERSION"),
                )
                .context(
                    "the expected local installation is unavailable; refusing to install a replacement",
                )?;
                anyhow::ensure!(
                    manifest.installation_uuid == expected,
                    "local kymo installation identity changed; refusing to open the dashboard"
                );
            }
            let output =
                supervisor::ensure_running(&paths, None, Some(uuid::Uuid::new_v4())).await?;
            if let Some(expected) = expected_installation_uuid {
                anyhow::ensure!(
                    output.installation_uuid == expected,
                    "local kymo installation identity changed; refusing to open the dashboard"
                );
            }
            let mut url = output.dashboard_origin;
            for segment in [project_id, run_id].into_iter().flatten() {
                url.push('/');
                url.push_str(&encode_path_segment(&segment));
            }
            // Print first: on a headless server the URL is the useful result and a browser helper may be missing.
            println!("{url}");
            if !no_browser && let Err(error) = launch_browser(&url) {
                eprintln!("warning: could not open a browser ({error:#}); open {url} yourself");
            }
        }
        Command::Stop { hold } => supervisor::stop(&paths, hold).await?,
        Command::Ports { dashboard, cdn } => {
            if let (Some(dashboard), Some(cdn)) = (dashboard, cdn) {
                supervisor::set_browser_ports(&paths, dashboard, cdn)?;
            }
            let artifacts = for_target(SupportedTarget::current()?)?;
            let manifest =
                RuntimeManifest::read_validated(&paths, &artifacts, env!("CARGO_PKG_VERSION"))?;
            println!("dashboard: http://127.0.0.1:{}", manifest.dashboard_port);
            println!("CDN: http://127.0.0.1:{}", manifest.cdn_port);
        }
        Command::Status { json } => print_status(status(&paths), json)?,
        Command::Doctor { json } => {
            let output = doctor(&paths);
            let healthy = output.healthy;
            print_doctor(output, json)?;
            anyhow::ensure!(healthy, "local runtime has failed checks");
        }
        Command::Supervise => supervisor::run(&paths).await?,
    }
    Ok(())
}

fn validate_environment_namespace() -> Result<()> {
    for (legacy_name, canonical_name) in retired_env::RETIRED_LOCAL_ENV
        .iter()
        .chain(retired_env::RETIRED_HOSTED_ENV)
    {
        anyhow::ensure!(
            std::env::var_os(legacy_name).is_none(),
            "{legacy_name} is no longer supported; use {canonical_name}"
        );
    }
    anyhow::ensure!(
        std::env::var_os("MKDB2_RUN_REAPER_INTERVAL_SECONDS").is_none(),
        "MKDB2_RUN_REAPER_INTERVAL_SECONDS is no longer supported; remove it (the production cadence is fixed at one hour)"
    );
    anyhow::ensure!(
        std::env::var_os("KYMO_RUN_REAPER_INTERVAL_SECONDS").is_none(),
        "KYMO_RUN_REAPER_INTERVAL_SECONDS is not supported; remove it (the production cadence is fixed at one hour)"
    );
    Ok(())
}

fn encode_path_segment(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            write!(&mut encoded, "%{byte:02X}").expect("writing to a String cannot fail");
        }
    }
    encoded
}

fn launch_browser(url: &str) -> Result<()> {
    #[cfg(target_os = "macos")]
    let command = "open";
    #[cfg(target_os = "linux")]
    let command = "xdg-open";
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    anyhow::bail!("browser launch is unsupported on this platform");

    // Detached, with no inherited output: some xdg-open setups run the browser in the foreground, and a browser holding our stdout or stderr would keep the caller waiting until it exits.
    std::process::Command::new(command)
        .arg(url)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .with_context(|| format!("launch browser with {command}"))?;
    Ok(())
}

fn status(paths: &RuntimePaths) -> StatusOutput {
    let artifacts = match SupportedTarget::current().and_then(for_target) {
        Ok(artifacts) => artifacts,
        Err(error) => {
            return StatusOutput {
                state: StatusState::Invalid,
                manifest: None,
                detail: format!("runtime state is invalid: {error:#}"),
            };
        }
    };
    let roots_present = match validate_runtime_roots(paths) {
        Ok(present) => present,
        Err(error) => {
            return StatusOutput {
                state: StatusState::Invalid,
                manifest: None,
                detail: format!("runtime state is invalid: {error:#}"),
            };
        }
    };
    match RuntimeManifest::read_optional_validated(paths, &artifacts, env!("CARGO_PKG_VERSION")) {
        Ok(None) if !roots_present => StatusOutput {
            state: StatusState::NotInstalled,
            manifest: None,
            detail: "local runtime is not installed; run `kymo install`".to_owned(),
        },
        Ok(None) => StatusOutput {
            state: StatusState::Invalid,
            manifest: None,
            detail: "runtime state is invalid: partial installation has no manifest; run `kymo doctor` before resuming with `kymo install`"
                .to_owned(),
        },
        Ok(Some(manifest)) => {
            let binaries = [
                paths.postgresql_binary(&manifest.postgresql.version),
                paths.clickhouse_binary(&manifest.clickhouse.version),
            ];
            if let Some(path) = binaries.iter().find(|path| !is_regular_file(path)) {
                StatusOutput {
                    state: StatusState::Invalid,
                    manifest: None,
                    detail: format!(
                        "runtime state is invalid: installed binary is missing or non-regular: {}",
                        path.display()
                    ),
                }
            } else {
                let (state, detail) = runtime_state(paths, &manifest);
                StatusOutput {
                    state,
                    manifest: Some(manifest),
                    detail,
                }
            }
        }
        Err(error) => StatusOutput {
            state: StatusState::Invalid,
            manifest: None,
            detail: format!("runtime state is invalid: {error:#}"),
        },
    }
}

fn runtime_state(paths: &RuntimePaths, manifest: &RuntimeManifest) -> (StatusState, String) {
    if let Some(running) = &manifest.running {
        let live = running
            .processes()
            .iter()
            .filter(|identity| supervisor::recorded_process_alive(identity))
            .count();
        return match live {
            4 => (
                StatusState::Running,
                format!("local stack is running at {}", running.dashboard_addr),
            ),
            0 => (
                StatusState::Degraded,
                "the recorded local stack exited; the next start recovers it".to_owned(),
            ),
            _ if supervisor::start_gate_is_held(paths) => {
                (StatusState::Stopping, "local stack is stopping".to_owned())
            }
            _ => (
                StatusState::Invalid,
                "the recorded local stack is only partly running; run `kymo doctor`".to_owned(),
            ),
        };
    }
    if let Some(launching) = &manifest.launching {
        // The launching supervisor holds the runtime lock; a free lock means that launch was interrupted.
        return if !supervisor::runtime_lock_is_held(paths) {
            (
                StatusState::Degraded,
                "a launch was interrupted; the next start retries once its processes are gone"
                    .to_owned(),
            )
        } else if supervisor::launch_is_overdue(launching) {
            (
                StatusState::Invalid,
                "a launch has not finished within its time budget; its supervisor is holding a component it could not stop. Run `kymo doctor`"
                    .to_owned(),
            )
        } else {
            (StatusState::Starting, "local stack is starting".to_owned())
        };
    }
    match &manifest.degraded_reason {
        Some(reason) => (
            StatusState::Degraded,
            format!("local stack stopped after a failure: {reason}; the next start retries"),
        ),
        None if supervisor::start_gate_is_held(paths) => (
            StatusState::Stopped,
            "local stack is stopped and held by `kymo stop`".to_owned(),
        ),
        None => (
            StatusState::Stopped,
            "database artifacts are installed and the local stack is stopped".to_owned(),
        ),
    }
}

fn is_regular_file(path: &std::path::Path) -> bool {
    std::fs::symlink_metadata(path)
        .map(|metadata| metadata.is_file() && !metadata.file_type().is_symlink())
        .unwrap_or(false)
}

fn validate_runtime_roots(paths: &RuntimePaths) -> Result<bool> {
    let mut present = Vec::new();
    for path in [&paths.data, &paths.state, &paths.cache] {
        let exists = match std::fs::symlink_metadata(path) {
            Ok(_) => {
                validate_private_dir(path)?;
                true
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        present.push(exists);
    }
    anyhow::ensure!(
        present[0] == present[1] && (present[0] || !present[2]),
        "managed runtime roots are only partially present"
    );
    Ok(present[0])
}

fn doctor(paths: &RuntimePaths) -> DoctorOutput {
    let mut checks = Vec::new();
    let artifacts = SupportedTarget::current().and_then(for_target);
    checks.push(match &artifacts {
        Ok(_) => DoctorCheck {
            name: "platform",
            healthy: true,
            detail: format!(
                "{}-{} is supported",
                std::env::consts::OS,
                std::env::consts::ARCH
            ),
        },
        Err(error) => DoctorCheck {
            name: "platform",
            healthy: false,
            detail: error.to_string(),
        },
    });
    let mut artifact_roots_healthy = true;
    for (name, path) in [("data-root", &paths.data), ("state-root", &paths.state)] {
        let check = doctor_root(name, path, false);
        if !check.healthy {
            artifact_roots_healthy = false;
        }
        checks.push(check);
    }
    checks.push(doctor_root("cache-root", &paths.cache, true));
    let manifest = match &artifacts {
        Ok(artifacts) if artifact_roots_healthy => Some(RuntimeManifest::read_validated(
            paths,
            artifacts,
            env!("CARGO_PKG_VERSION"),
        )),
        _ => None,
    };
    checks.push(DoctorCheck {
        name: "manifest-identity",
        healthy: matches!(manifest, Some(Ok(_))),
        detail: match (&manifest, &artifacts) {
            (Some(Ok(_)), _) => paths.manifest().display().to_string(),
            (Some(Err(error)), _) => format!("{error:#}"),
            (None, Err(error)) => error.to_string(),
            (None, Ok(_)) => "skipped because a managed root is unsafe".to_owned(),
        },
    });
    if let (Some(Ok(manifest)), Ok(artifacts)) = (&manifest, &artifacts) {
        // Check the builds the installation records; PostgreSQL may be an earlier one until a start replaces it.
        let postgresql_version = &manifest.postgresql.version;
        let postgresql = paths.postgresql_binary(postgresql_version);
        checks.push(
            match validate_confined_regular_file(&paths.state, &postgresql)
                .and_then(|()| install::validate_postgresql(&postgresql, postgresql_version))
            {
                Ok(()) => DoctorCheck {
                    name: "postgresql-binary",
                    healthy: true,
                    detail: postgresql.display().to_string(),
                },
                Err(error) => DoctorCheck {
                    name: "postgresql-binary",
                    healthy: false,
                    detail: format!("{error:#}"),
                },
            },
        );
        let clickhouse = paths.clickhouse_binary(&manifest.clickhouse.version);
        checks.push(
            match validate_confined_regular_file(&paths.state, &clickhouse).and_then(|()| {
                install::validate_clickhouse(
                    &clickhouse,
                    manifest.clickhouse.installed_sha256.as_deref(),
                )
            }) {
                Ok(()) => DoctorCheck {
                    name: "clickhouse-binary",
                    healthy: true,
                    detail: clickhouse.display().to_string(),
                },
                Err(error) => DoctorCheck {
                    name: "clickhouse-binary",
                    healthy: false,
                    detail: format!("{error:#}"),
                },
            },
        );
        if manifest.postgresql.version != artifacts.postgresql.version {
            checks.push(DoctorCheck {
                name: "database-builds",
                healthy: true,
                detail: format!(
                    "the next start that can install replaces PostgreSQL {} with {}",
                    manifest.postgresql.version, artifacts.postgresql.version
                ),
            });
        }
    }
    let runtime = status(paths);
    checks.push(DoctorCheck {
        name: "runtime-state",
        healthy: matches!(
            runtime.state,
            StatusState::Stopped
                | StatusState::Starting
                | StatusState::Running
                | StatusState::Stopping
        ),
        detail: runtime.detail,
    });
    DoctorOutput {
        healthy: checks.iter().all(|check| check.healthy),
        checks,
    }
}

fn doctor_root(
    name: &'static str,
    path: &std::path::Path,
    missing_is_healthy: bool,
) -> DoctorCheck {
    match std::fs::symlink_metadata(path) {
        Ok(_) => match validate_private_dir(path) {
            Ok(()) => DoctorCheck {
                name,
                healthy: true,
                detail: path.display().to_string(),
            },
            Err(error) => DoctorCheck {
                name,
                healthy: false,
                detail: error.to_string(),
            },
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && missing_is_healthy => {
            DoctorCheck {
                name,
                healthy: true,
                detail: format!(
                    "{} is absent and will be recreated on demand",
                    path.display()
                ),
            }
        }
        Err(error) => DoctorCheck {
            name,
            healthy: false,
            detail: format!("{}: {error}", path.display()),
        },
    }
}

fn print_status(output: StatusOutput, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("{}", output.detail);
        if let Some(manifest) = output.manifest {
            println!("installation: {}", manifest.installation_uuid);
            println!("PostgreSQL: {}", manifest.postgresql.version);
            println!("ClickHouse: {}", manifest.clickhouse.version);
        }
    }
    Ok(())
}

fn print_doctor(output: DoctorOutput, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        for check in output.checks {
            println!(
                "{} {:<20} {}",
                if check.healthy { "ok" } else { "FAIL" },
                check.name,
                check.detail
            );
        }
    }
    Ok(())
}

/// A child forked by a concurrent test briefly shares every open file description, including another test's lock file, so tests that spawn processes and tests that observe lock files take turns.
#[cfg(test)]
pub(crate) fn serialize_process_test() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[test]
    fn browser_route_segments_are_utf8_percent_encoded() {
        assert_eq!(encode_path_segment("plain-._~"), "plain-._~");
        assert_eq!(
            encode_path_segment("project/one π"),
            "project%2Fone%20%CF%80"
        );
    }

    #[test]
    fn open_accepts_identities_equal_to_option_names_after_double_dash() {
        let expected = "11111111-1111-4111-8111-111111111111";
        let cli = Cli::try_parse_from([
            "kymo",
            "open",
            "--expected-installation-uuid",
            expected,
            "--",
            "--no-browser",
            "--expected-installation-uuid",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Open {
                project_id: Some(project_id),
                run_id: Some(run_id),
                expected_installation_uuid: Some(installation_uuid),
                no_browser: false,
            } if project_id == "--no-browser"
                && run_id == "--expected-installation-uuid"
                && installation_uuid.to_string() == expected
        ));
    }

    #[test]
    fn open_takes_the_dashboard_a_project_or_a_run() {
        let parse = |args: &[&str]| match Cli::try_parse_from(["kymo", "open"].iter().chain(args))
            .unwrap()
            .command
        {
            Command::Open {
                project_id,
                run_id,
                no_browser,
                ..
            } => (project_id, run_id, no_browser),
            _ => unreachable!(),
        };
        let owned = |value: &str| Some(value.to_owned());
        assert_eq!(parse(&[]), (None, None, false));
        assert_eq!(parse(&["--no-browser"]), (None, None, true));
        assert_eq!(parse(&["demo"]), (owned("demo"), None, false));
        assert_eq!(
            parse(&["demo", "--no-browser"]),
            (owned("demo"), None, true)
        );
        assert_eq!(
            parse(&["--no-browser", "demo", "first run"]),
            (owned("demo"), owned("first run"), true)
        );
        assert_eq!(
            parse(&["demo", "first run", "--no-browser"]),
            (owned("demo"), owned("first run"), true)
        );
        assert!(Cli::try_parse_from(["kymo", "open", "a", "b", "c"]).is_err());
        // A mistyped flag is an error, not a project named after it; names starting with a hyphen go after `--`.
        assert!(Cli::try_parse_from(["kymo", "open", "--no-browsr"]).is_err());
        assert!(Cli::try_parse_from(["kymo", "open", "demo", "--no-browsr"]).is_err());
    }

    #[test]
    fn status_distinguishes_missing_and_invalid_installations() {
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        assert_eq!(status(&paths).state, StatusState::NotInstalled);
        assert!(!paths.data.exists());

        paths.prepare().unwrap();
        assert_eq!(status(&paths).state, StatusState::Invalid);
        std::fs::write(paths.manifest(), b"invalid manifest").unwrap();
        assert_eq!(status(&paths).state, StatusState::Invalid);

        std::fs::remove_file(paths.manifest()).unwrap();
        std::os::unix::fs::symlink("missing", paths.manifest()).unwrap();
        assert_eq!(status(&paths).state, StatusState::Invalid);
    }

    #[test]
    fn doctor_does_not_execute_binaries_beneath_an_unsafe_root() {
        let _serial = serialize_process_test();
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        paths.prepare().unwrap();
        let artifacts = for_target(SupportedTarget::current().unwrap()).unwrap();
        let marker = temporary.path().join("executed");
        for binary in [
            paths.postgresql_binary(&artifacts.postgresql.version),
            paths.clickhouse_binary(&artifacts.clickhouse.version),
        ] {
            std::fs::create_dir_all(binary.parent().unwrap()).unwrap();
            std::fs::write(&binary, format!("#!/bin/sh\ntouch {}\n", marker.display())).unwrap();
            let mut permissions = std::fs::metadata(&binary).unwrap().permissions();
            permissions.set_mode(0o700);
            std::fs::set_permissions(binary, permissions).unwrap();
        }
        let mut state_permissions = std::fs::metadata(&paths.state).unwrap().permissions();
        state_permissions.set_mode(0o755);
        std::fs::set_permissions(&paths.state, state_permissions).unwrap();

        let output = doctor(&paths);
        assert!(!output.healthy);
        assert!(!marker.exists());
        assert!(
            output
                .checks
                .iter()
                .any(|check| check.name == "manifest-identity" && check.detail.contains("skipped"))
        );
    }

    #[test]
    fn runtime_state_reports_starting_and_recoverable_failures() {
        let _serial = serialize_process_test();
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        paths.prepare().unwrap();
        let artifacts = for_target(SupportedTarget::current().unwrap()).unwrap();
        let installation_uuid =
            kymo_local_runtime_core::manifest::initialize_installation_uuid(&paths).unwrap();
        let mut manifest = RuntimeManifest::installed(
            &artifacts,
            installation_uuid,
            env!("CARGO_PKG_VERSION"),
            40_001,
            40_002,
        );
        assert_eq!(runtime_state(&paths, &manifest).0, StatusState::Stopped);

        manifest.degraded_reason = Some("ClickHouse exited unexpectedly".to_owned());
        let (state, detail) = runtime_state(&paths, &manifest);
        assert_eq!(state, StatusState::Degraded);
        assert!(detail.contains("next start retries"));

        manifest.launching = Some(kymo_local_runtime_core::manifest::LaunchIntent {
            supervisor: kymo_local_runtime_core::manifest::ProcessIdentity {
                pid: std::process::id(),
                owner_uid: 0,
                executable: std::env::current_exe().unwrap(),
                start_identity: String::new(),
            },
            started_at_unix_ms: supervisor::unix_time_ms(),
            child_process_groups: Vec::new(),
        });
        assert_eq!(runtime_state(&paths, &manifest).0, StatusState::Degraded);
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(paths.state.join("runtime.lock"))
            .unwrap();
        fs2::FileExt::lock_exclusive(&lock).unwrap();
        assert_eq!(runtime_state(&paths, &manifest).0, StatusState::Starting);
        manifest.launching.as_mut().unwrap().started_at_unix_ms = 0;
        assert_eq!(runtime_state(&paths, &manifest).0, StatusState::Invalid);
    }

    #[test]
    fn status_rejects_manifest_with_missing_binaries() {
        let _serial = serialize_process_test();
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        paths.prepare().unwrap();
        let artifacts = for_target(SupportedTarget::current().unwrap()).unwrap();
        let installation_uuid =
            kymo_local_runtime_core::manifest::initialize_installation_uuid(&paths).unwrap();
        RuntimeManifest::installed(
            &artifacts,
            installation_uuid,
            env!("CARGO_PKG_VERSION"),
            40_001,
            40_002,
        )
        .write_atomic(&paths.manifest())
        .unwrap();

        assert_eq!(status(&paths).state, StatusState::Invalid);

        for binary in [
            paths.postgresql_binary(&artifacts.postgresql.version),
            paths.clickhouse_binary(&artifacts.clickhouse.version),
        ] {
            kymo_local_runtime_core::paths::ensure_private_dir(binary.parent().unwrap()).unwrap();
            std::fs::write(binary, b"placeholder").unwrap();
        }
        assert_eq!(status(&paths).state, StatusState::Stopped);

        std::fs::remove_dir_all(&paths.cache).unwrap();
        assert_eq!(status(&paths).state, StatusState::Stopped);
        let output = doctor(&paths);
        let cache = output
            .checks
            .iter()
            .find(|check| check.name == "cache-root")
            .unwrap();
        assert!(cache.healthy);
        assert!(cache.detail.contains("recreated on demand"));
    }
}
