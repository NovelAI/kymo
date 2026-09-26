use std::fs::{File, OpenOptions};
use std::io::{BufReader, Write};
use std::net::SocketAddr;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use semver::Version;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::artifacts::TargetArtifacts;
use crate::paths::{RuntimePaths, reject_symlink, remove_private_file_if_exists, sync_dir};

// After the first release, additive fields require explicit serde defaults to retain this format.
// Removing, renaming, or changing the meaning of a persisted field requires a format-version bump.
pub const MANIFEST_FORMAT_VERSION: u32 = 1;
pub const DATA_SCHEMA_GENERATION: u32 = 1;
pub const PROTOCOL_MIN: u32 = 2;
pub const PROTOCOL_MAX: u32 = 2;
// Browser ports come from below every supported kernel's ephemeral range (Linux 32768+, macOS 49152+), so outgoing connections rarely hold a pinned port.
const BROWSER_PORT_RANGE: std::ops::Range<u16> = 20_000..32_768;
const INSTALLATION_UUID_TEMP_FILE: &str = ".installation.uuid.tmp";
const RUNTIME_MANIFEST_TEMP_FILE: &str = ".runtime.json.tmp";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InstalledArtifact {
    pub version: String,
    pub archive_sha256: String,
    pub installed_sha256: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub owner_uid: u32,
    pub executable: PathBuf,
    pub start_identity: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunningStack {
    pub generation_uuid: Uuid,
    pub supervisor: ProcessIdentity,
    pub postgresql: ProcessIdentity,
    pub clickhouse: ProcessIdentity,
    pub server: ProcessIdentity,
    pub native_socket: PathBuf,
    pub upload_socket: PathBuf,
    pub control_socket: PathBuf,
    pub dashboard_addr: SocketAddr,
    pub cdn_addr: SocketAddr,
    pub supervisor_secret: PathBuf,
}

impl RunningStack {
    pub fn processes(&self) -> [&ProcessIdentity; 4] {
        [
            &self.supervisor,
            &self.postgresql,
            &self.clickhouse,
            &self.server,
        ]
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LaunchIntent {
    pub supervisor: ProcessIdentity,
    pub started_at_unix_ms: u64,
    /// Each child's dedicated process group, published right after its spawn, so recovery can prove an interrupted launch quiescent.
    pub child_process_groups: Vec<u32>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RuntimeManifest {
    pub format_version: u32,
    pub launcher_version: String,
    pub protocol_min: u32,
    pub protocol_max: u32,
    pub data_schema_generation: u32,
    pub installation_uuid: Uuid,
    pub postgresql: InstalledArtifact,
    pub clickhouse: InstalledArtifact,
    /// Stable loopback ports make printed run URLs valid across idle restarts.
    pub dashboard_port: u16,
    pub cdn_port: u16,
    #[serde(default)]
    pub launching: Option<LaunchIntent>,
    #[serde(default)]
    pub running: Option<RunningStack>,
    /// Why the last start or run of the stack failed. Diagnostic only: the next start retries once the recorded processes are proven gone.
    #[serde(default)]
    pub degraded_reason: Option<String>,
}

impl RuntimeManifest {
    pub fn installed(
        artifacts: &TargetArtifacts,
        installation_uuid: Uuid,
        launcher_version: &str,
        dashboard_port: u16,
        cdn_port: u16,
    ) -> Self {
        Self {
            format_version: MANIFEST_FORMAT_VERSION,
            launcher_version: launcher_version.to_owned(),
            protocol_min: PROTOCOL_MIN,
            protocol_max: PROTOCOL_MAX,
            data_schema_generation: DATA_SCHEMA_GENERATION,
            installation_uuid,
            postgresql: InstalledArtifact {
                version: artifacts.postgresql.version.clone(),
                archive_sha256: artifacts.postgresql.sha256.clone(),
                installed_sha256: artifacts.postgresql.installed_sha256.clone(),
            },
            clickhouse: InstalledArtifact {
                version: artifacts.clickhouse.version.clone(),
                archive_sha256: artifacts.clickhouse.sha256.clone(),
                installed_sha256: artifacts.clickhouse.installed_sha256.clone(),
            },
            dashboard_port,
            cdn_port,
            launching: None,
            running: None,
            degraded_reason: None,
        }
    }

    pub fn read(path: &Path) -> Result<Self> {
        reject_symlink(path)?;
        let file = File::open(path).with_context(|| format!("open manifest {}", path.display()))?;
        let manifest: Self = serde_json::from_reader(BufReader::new(file))
            .with_context(|| format!("parse manifest {}", path.display()))?;
        ensure!(
            manifest.format_version == MANIFEST_FORMAT_VERSION,
            "unsupported runtime manifest format {}",
            manifest.format_version
        );
        Ok(manifest)
    }

    pub fn write_atomic(&self, path: &Path) -> Result<()> {
        let parent = path.parent().context("manifest has no parent")?;
        reject_symlink(path)?;
        let temporary_path = parent.join(RUNTIME_MANIFEST_TEMP_FILE);
        remove_private_file_if_exists(&temporary_path)?;
        let mut temporary = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temporary_path)
            .context("create temporary manifest")?;
        serde_json::to_writer_pretty(&mut temporary, self).context("serialize runtime manifest")?;
        temporary.write_all(b"\n")?;
        temporary
            .sync_all()
            .context("sync temporary runtime manifest")?;
        drop(temporary);
        std::fs::rename(&temporary_path, path).context("publish runtime manifest")?;
        sync_dir(parent)
    }

    pub fn read_validated(
        paths: &RuntimePaths,
        artifacts: &TargetArtifacts,
        launcher_version: &str,
    ) -> Result<Self> {
        let manifest = Self::read(&paths.manifest())?;
        manifest.validate_identity(paths)?;
        manifest.validate_compatibility(artifacts, launcher_version)?;
        Ok(manifest)
    }

    pub fn read_optional_validated(
        paths: &RuntimePaths,
        artifacts: &TargetArtifacts,
        launcher_version: &str,
    ) -> Result<Option<Self>> {
        match std::fs::symlink_metadata(paths.manifest()) {
            Ok(_) => Self::read_validated(paths, artifacts, launcher_version).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).context("inspect runtime manifest"),
        }
    }

    /// The installation UUID inside the data root is the whole identity: a restored or relocated copy of the data keeps it, while another installation's data cannot adopt this manifest.
    pub fn validate_identity(&self, paths: &RuntimePaths) -> Result<()> {
        ensure!(
            self.installation_uuid == read_installation_uuid(&paths.installation_id())?,
            "installation UUID does not match runtime manifest"
        );
        Ok(())
    }

    pub fn validate_compatibility(
        &self,
        artifacts: &TargetArtifacts,
        launcher_version: &str,
    ) -> Result<()> {
        let active_version = Version::parse(&self.launcher_version)
            .context("active launcher version is not valid semver")?;
        let current_version = Version::parse(launcher_version)
            .context("current launcher version is not valid semver")?;
        ensure!(
            current_version >= active_version,
            "refusing launcher downgrade from {} to {}",
            self.launcher_version,
            launcher_version
        );
        ensure!(
            self.protocol_min <= PROTOCOL_MAX && self.protocol_max >= PROTOCOL_MIN,
            "runtime protocol {}..={} is incompatible with {}..={}; delete this pre-release installation and reinstall",
            self.protocol_min,
            self.protocol_max,
            PROTOCOL_MIN,
            PROTOCOL_MAX
        );
        ensure!(
            self.data_schema_generation == DATA_SCHEMA_GENERATION,
            "active data schema generation {} does not match {}",
            self.data_schema_generation,
            DATA_SCHEMA_GENERATION
        );
        validate_artifact("PostgreSQL", &self.postgresql, &artifacts.postgresql)?;
        validate_artifact("ClickHouse", &self.clickhouse, &artifacts.clickhouse)?;
        validate_browser_ports(self.dashboard_port, self.cdn_port)
    }

    /// Record the running launcher so an older environment can never drive this installation again; `validate_compatibility` has already refused a downgrade.
    pub fn record_launcher_version(&mut self, launcher_version: &str) -> bool {
        let changed = self.launcher_version != launcher_version;
        self.launcher_version = launcher_version.to_owned();
        changed
    }
}

pub fn validate_browser_ports(dashboard_port: u16, cdn_port: u16) -> Result<()> {
    ensure!(
        dashboard_port >= 1024 && cdn_port >= 1024 && dashboard_port != cdn_port,
        "browser ports must be distinct non-system ports"
    );
    Ok(())
}

/// Choose two free loopback ports, starting from an installation-derived offset so separate installations rarely collide.
pub fn allocate_browser_ports(installation_uuid: Uuid) -> Result<(u16, u16)> {
    let span = BROWSER_PORT_RANGE.end - BROWSER_PORT_RANGE.start;
    let offset = (installation_uuid.as_u128() % u128::from(span)) as u16;
    let mut free = (0..span)
        .map(|step| BROWSER_PORT_RANGE.start + (offset + step) % span)
        .filter(|&port| std::net::TcpListener::bind(("127.0.0.1", port)).is_ok());
    match (free.next(), free.next()) {
        (Some(dashboard), Some(cdn)) => Ok((dashboard, cdn)),
        _ => anyhow::bail!(
            "no two free loopback ports in {}..{}",
            BROWSER_PORT_RANGE.start,
            BROWSER_PORT_RANGE.end
        ),
    }
}

fn validate_artifact(
    name: &str,
    installed: &InstalledArtifact,
    expected: &crate::artifacts::Artifact,
) -> Result<()> {
    ensure!(
        installed.version == expected.version
            && installed.archive_sha256 == expected.sha256
            && installed.installed_sha256 == expected.installed_sha256,
        "active {name} runtime does not match the frozen artifact catalog"
    );
    Ok(())
}

pub fn initialize_installation_uuid(paths: &RuntimePaths) -> Result<Uuid> {
    let path = paths.installation_id();
    recover_installation_uuid_temp(paths)?;
    let entries = std::fs::read_dir(&paths.data)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    if path.exists() {
        ensure!(
            entries.iter().all(|entry| entry == &path),
            "data exists without a runtime manifest; refusing to assign current runtime metadata"
        );
        return read_installation_uuid(&path);
    }
    ensure!(
        entries.is_empty(),
        "data exists without an installation UUID; refusing to create a replacement identity"
    );
    reject_symlink(&path)?;
    let parent = path.parent().context("installation UUID has no parent")?;
    let value = Uuid::new_v4();
    let temporary_path = parent.join(INSTALLATION_UUID_TEMP_FILE);
    let mut temporary = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&temporary_path)
        .context("create temporary installation UUID")?;
    writeln!(temporary, "{value}")?;
    temporary.sync_all().context("sync installation UUID")?;
    drop(temporary);
    match std::fs::hard_link(&temporary_path, &path) {
        Ok(()) => {
            sync_dir(parent)?;
            std::fs::remove_file(&temporary_path)
                .context("remove published installation UUID temporary")?;
            sync_dir(parent)?;
            Ok(value)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            std::fs::remove_file(&temporary_path)
                .context("remove raced installation UUID temporary")?;
            sync_dir(parent)?;
            read_installation_uuid(&path)
        }
        Err(error) => Err(error).context("publish installation UUID"),
    }
}

fn recover_installation_uuid_temp(paths: &RuntimePaths) -> Result<()> {
    let path = paths.data.join(INSTALLATION_UUID_TEMP_FILE);
    remove_private_file_if_exists(&path).map(|_| ())
}

pub fn read_installation_uuid(path: &Path) -> Result<Uuid> {
    reject_symlink(path)?;
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read installation UUID {}", path.display()))?;
    Uuid::parse_str(raw.trim()).context("parse installation UUID")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn installed_manifest(paths: &RuntimePaths) -> RuntimeManifest {
        let installation_uuid = initialize_installation_uuid(paths).unwrap();
        let artifacts =
            crate::artifacts::for_target(crate::artifacts::SupportedTarget::MacosArm64).unwrap();
        RuntimeManifest::installed(
            &artifacts,
            installation_uuid,
            env!("CARGO_PKG_VERSION"),
            40_001,
            40_002,
        )
    }

    #[test]
    fn restored_data_root_keeps_its_installation_identity() {
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        paths.prepare().unwrap();
        let manifest = installed_manifest(&paths);
        std::fs::write(paths.data.join("database"), b"data").unwrap();

        // A copied-back backup is a new directory inode with the same contents.
        let backup = temporary.path().join("backup");
        std::fs::rename(&paths.data, &backup).unwrap();
        crate::paths::ensure_private_dir(&paths.data).unwrap();
        for name in ["installation.uuid", "database"] {
            std::fs::copy(backup.join(name), paths.data.join(name)).unwrap();
        }
        manifest.validate_identity(&paths).unwrap();

        std::fs::write(paths.installation_id(), format!("{}\n", Uuid::new_v4())).unwrap();
        assert!(manifest.validate_identity(&paths).is_err());
    }

    #[test]
    fn pre_release_protocol_one_manifests_are_refused_with_reinstall_guidance() {
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        paths.prepare().unwrap();
        let artifacts =
            crate::artifacts::for_target(crate::artifacts::SupportedTarget::MacosArm64).unwrap();
        let mut manifest = installed_manifest(&paths);
        manifest.protocol_min = 1;
        manifest.protocol_max = 1;
        let error = manifest
            .validate_compatibility(&artifacts, env!("CARGO_PKG_VERSION"))
            .unwrap_err();
        assert!(error.to_string().contains("reinstall"));
    }

    #[test]
    fn the_running_launcher_version_is_recorded() {
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        paths.prepare().unwrap();
        let mut manifest = installed_manifest(&paths);
        manifest.launcher_version = "0.0.9".to_owned();
        assert!(manifest.record_launcher_version("0.1.0"));
        assert_eq!(manifest.launcher_version, "0.1.0");
        assert!(!manifest.record_launcher_version("0.1.0"));
    }

    #[test]
    fn browser_ports_are_distinct_and_below_the_ephemeral_range() {
        let (dashboard, cdn) = allocate_browser_ports(Uuid::new_v4()).unwrap();
        assert_ne!(dashboard, cdn);
        for port in [dashboard, cdn] {
            assert!(BROWSER_PORT_RANGE.contains(&port));
        }
        validate_browser_ports(dashboard, cdn).unwrap();
        assert!(validate_browser_ports(dashboard, dashboard).is_err());
        assert!(validate_browser_ports(80, cdn).is_err());
    }

    #[test]
    fn installation_identity_is_stable_and_manifest_bound() {
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        paths.prepare().unwrap();
        let first = initialize_installation_uuid(&paths).unwrap();
        assert_eq!(first, initialize_installation_uuid(&paths).unwrap());
        let artifacts =
            crate::artifacts::for_target(crate::artifacts::SupportedTarget::MacosArm64).unwrap();
        let manifest = RuntimeManifest::installed(
            &artifacts,
            first,
            env!("CARGO_PKG_VERSION"),
            40_001,
            40_002,
        );
        manifest.write_atomic(&paths.manifest()).unwrap();
        let loaded = RuntimeManifest::read(&paths.manifest()).unwrap();
        assert_eq!(manifest, loaded);
        assert!(loaded.dashboard_port >= 1024);
        assert!(loaded.cdn_port >= 1024);
        assert_ne!(loaded.dashboard_port, loaded.cdn_port);
        loaded.validate_identity(&paths).unwrap();
        loaded
            .validate_compatibility(&artifacts, env!("CARGO_PKG_VERSION"))
            .unwrap();

        let mut incompatible = loaded;
        incompatible.clickhouse.version = "other".to_owned();
        assert!(
            incompatible
                .validate_compatibility(&artifacts, env!("CARGO_PKG_VERSION"))
                .is_err()
        );
        incompatible.clickhouse.version = artifacts.clickhouse.version.clone();
        incompatible.launcher_version = "999.0.0".to_owned();
        assert!(
            incompatible
                .validate_compatibility(&artifacts, env!("CARGO_PKG_VERSION"))
                .is_err()
        );
    }

    #[test]
    fn existing_data_without_identity_fails_closed() {
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        paths.prepare().unwrap();
        std::fs::write(paths.data.join("unexpected"), b"data").unwrap();
        assert!(initialize_installation_uuid(&paths).is_err());
        assert!(!paths.installation_id().exists());
    }

    #[test]
    fn identity_without_manifest_cannot_wrap_existing_data() {
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        paths.prepare().unwrap();
        initialize_installation_uuid(&paths).unwrap();
        std::fs::write(paths.data.join("database"), b"old data").unwrap();
        assert!(initialize_installation_uuid(&paths).is_err());
    }

    #[test]
    fn stale_identity_temporary_states_converge() {
        for uuid_already_published in [false, true] {
            let temporary = tempfile::tempdir().unwrap();
            let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
            paths.prepare().unwrap();
            let existing =
                uuid_already_published.then(|| initialize_installation_uuid(&paths).unwrap());
            let temporary_path = paths.data.join(INSTALLATION_UUID_TEMP_FILE);
            let mut stale = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(&temporary_path)
                .unwrap();
            writeln!(stale, "interrupted").unwrap();
            stale.sync_all().unwrap();
            drop(stale);

            let recovered = initialize_installation_uuid(&paths).unwrap();
            if let Some(existing) = existing {
                assert_eq!(recovered, existing);
            }
            assert!(!temporary_path.exists());
            assert_eq!(
                std::fs::read_dir(&paths.data).unwrap().count(),
                1,
                "only installation.uuid should remain"
            );
        }
    }

    #[test]
    fn stale_manifest_temporary_is_replaced() {
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        paths.prepare().unwrap();
        let installation_uuid = initialize_installation_uuid(&paths).unwrap();
        let artifacts =
            crate::artifacts::for_target(crate::artifacts::SupportedTarget::MacosArm64).unwrap();
        let manifest =
            RuntimeManifest::installed(&artifacts, installation_uuid, "0.1.0", 40_001, 40_002);
        let stale = paths.state.join(RUNTIME_MANIFEST_TEMP_FILE);
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&stale)
            .unwrap();
        file.write_all(b"interrupted").unwrap();
        file.sync_all().unwrap();
        drop(file);

        manifest.write_atomic(&paths.manifest()).unwrap();
        assert_eq!(RuntimeManifest::read(&paths.manifest()).unwrap(), manifest);
        assert!(!stale.exists());
    }
}
