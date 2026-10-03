use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use flate2::read::GzDecoder;
use fs2::FileExt;
use futures_util::StreamExt;
use kymo_local_runtime_core::artifacts::{
    ArchiveFormat, Artifact, SupportedTarget, TargetArtifacts, for_target,
};
use kymo_local_runtime_core::manifest::{
    RuntimeManifest, allocate_browser_ports, initialize_installation_uuid,
};
use kymo_local_runtime_core::paths::{
    RuntimePaths, ensure_private_dir, reject_symlink, remove_private_file_if_exists, sync_dir,
    validate_confined_regular_file, validate_private_dir, validate_private_file,
};
use sha2::{Digest, Sha256};
use tar::Archive;

// A replacement holds up the start (and `kymo stop` waits for it), so past this budget the start keeps the recorded build; an explicit `kymo install` has no budget.
const REPLACEMENT_BUDGET: Duration = Duration::from_secs(120);
// Bounds each read, the response headers included, so a stalled server fails the install instead of hanging it.
const DOWNLOAD_IDLE_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_secs(1)
} else {
    Duration::from_secs(60)
};

pub async fn install(paths: &RuntimePaths) -> Result<RuntimeManifest> {
    let artifacts = for_target(SupportedTarget::current()?)?;
    paths.prepare()?;
    ensure_private_dir(&paths.downloads())?;
    ensure_private_dir(&paths.artifacts())?;
    let _install_lock = acquire_install_lock(paths)?;
    let _runtime_lock = crate::supervisor::acquire_runtime_lock(paths)?;
    install_locked(paths, &artifacts).await
}

pub(crate) async fn ensure_installed(
    paths: &RuntimePaths,
    artifacts: &TargetArtifacts,
    allow_install: bool,
) -> Result<RuntimeManifest> {
    if !allow_install
        && matches!(
            std::fs::symlink_metadata(paths.manifest()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound
        )
    {
        bail!("local database artifacts are not installed; run `kymo install`");
    }
    paths.prepare()?;
    ensure_private_dir(&paths.downloads())?;
    ensure_private_dir(&paths.artifacts())?;
    let _install_lock = acquire_install_lock(paths)?;
    // Recorded builds that are in place run as they are; a start replaces an earlier PostgreSQL build.
    if let Some(manifest) =
        RuntimeManifest::read_optional_validated(paths, artifacts, env!("CARGO_PKG_VERSION"))?
        && installed_files_present(paths, &manifest)?
    {
        return Ok(manifest);
    }
    ensure!(
        allow_install,
        "local database artifacts are not installed; run `kymo install`"
    );
    let _runtime_lock = crate::supervisor::acquire_runtime_lock(paths)?;
    install_locked(paths, artifacts).await
}

/// Replace a recorded earlier build; the caller holds the runtime lock. On failure or timeout keep the recorded build, so an offline or slow start is not an outage; the start checks that it still runs, and later starts retry.
pub(crate) async fn replace_predecessor(
    paths: &RuntimePaths,
    artifacts: &TargetArtifacts,
    recorded: RuntimeManifest,
) -> RuntimeManifest {
    let error =
        match tokio::time::timeout(REPLACEMENT_BUDGET, install_locked(paths, artifacts)).await {
            Ok(Ok(manifest)) => return manifest,
            Ok(Err(error)) => error,
            Err(_) => anyhow::anyhow!("it took longer than {REPLACEMENT_BUDGET:?}"),
        };
    eprintln!("warning: keeping the installed databases; replacing them failed: {error:#}");
    recorded
}

pub(crate) fn local_install_allowed() -> Result<bool> {
    let value = std::env::var_os("KYMO_LOCAL_NO_INSTALL").unwrap_or_default();
    match value.to_string_lossy().trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(false),
        "0" | "false" | "no" | "off" | "" => Ok(true),
        value => bail!(
            "KYMO_LOCAL_NO_INSTALL must be one of 1/true/yes/on or 0/false/no/off, got {value:?}"
        ),
    }
}

pub(crate) fn acquire_install_lock(paths: &RuntimePaths) -> Result<File> {
    let lock = crate::supervisor::open_lock_file(&paths.install_lock())?;
    lock.lock_exclusive()
        .context("lock local-runtime installation")?;
    Ok(lock)
}

fn installed_files_present(paths: &RuntimePaths, manifest: &RuntimeManifest) -> Result<bool> {
    let binaries = [
        paths.postgresql_binary(&manifest.postgresql.version),
        paths.clickhouse_binary(&manifest.clickhouse.version),
    ];
    if binaries.iter().any(|path| !path.is_file()) {
        return Ok(false);
    }
    for binary in binaries {
        validate_confined_regular_file(&paths.state, &binary)?;
    }
    Ok(true)
}

async fn install_locked(
    paths: &RuntimePaths,
    artifacts: &TargetArtifacts,
) -> Result<RuntimeManifest> {
    let mut existing =
        RuntimeManifest::read_optional_validated(paths, artifacts, env!("CARGO_PKG_VERSION"))?;
    if let Some(manifest) = &mut existing {
        crate::supervisor::clear_stopped_remnants(paths, manifest)?;
    }
    let installation_uuid = existing
        .as_ref()
        .map(|manifest| manifest.installation_uuid)
        .map_or_else(|| initialize_installation_uuid(paths), Ok)?;
    let (dashboard_port, cdn_port) = existing
        .as_ref()
        .map(|manifest| (manifest.dashboard_port, manifest.cdn_port))
        .map_or_else(|| allocate_browser_ports(installation_uuid), Ok)?;
    install_postgresql(paths, &artifacts.postgresql).await?;
    if paths.postgresql_data().join("PG_VERSION").exists() {
        check_postgresql_data(paths, &artifacts.postgresql.version)?;
    }
    install_clickhouse(paths, &artifacts.clickhouse).await?;
    let mut manifest = RuntimeManifest::installed(
        artifacts,
        installation_uuid,
        env!("CARGO_PKG_VERSION"),
        dashboard_port,
        cdn_port,
    );
    if let Some(existing) = existing {
        manifest.degraded_reason = existing.degraded_reason;
    }
    manifest.write_atomic(&paths.manifest())?;
    Ok(manifest)
}

async fn install_postgresql(paths: &RuntimePaths, artifact: &Artifact) -> Result<()> {
    ensure!(
        artifact.format == ArchiveFormat::TarGzTree,
        "unexpected PostgreSQL archive format"
    );
    let destination = paths.postgresql_dir(&artifact.version);
    let binary = paths.postgresql_binary(&artifact.version);
    let parent = destination
        .parent()
        .context("PostgreSQL destination has no parent")?;
    let staging = parent.join(format!(".{}.installing", artifact.version));
    recover_staging_directory(&staging)?;
    if binary.is_file() {
        validate_confined_regular_file(&paths.state, &binary)?;
        validate_postgresql(&binary, &artifact.version)?;
        validate_macos_signatures(&destination)?;
        sync_dir(parent)?;
        return Ok(());
    }
    let archive = download_verified(paths, artifact).await?;
    ensure_private_dir(parent)?;
    ensure_private_dir(&staging).context("create PostgreSQL extraction directory")?;
    let extracted = staging.join("tree");
    extract_tree(&archive, &extracted)?;
    ensure!(
        extracted.join("bin/postgres").is_file(),
        "PostgreSQL archive did not contain bin/postgres"
    );
    validate_postgresql(&extracted.join("bin/postgres"), &artifact.version)?;
    validate_macos_signatures(&extracted)?;
    sync_tree(&extracted)?;
    activate_directory(&extracted, &destination)?;
    finish_staging_directory(&staging)?;
    Ok(())
}

async fn install_clickhouse(paths: &RuntimePaths, artifact: &Artifact) -> Result<()> {
    let destination_dir = paths.clickhouse_dir(&artifact.version);
    let destination = paths.clickhouse_binary(&artifact.version);
    let parent = destination_dir
        .parent()
        .context("ClickHouse destination has no parent")?;
    let staging = parent.join(format!(".{}.installing", artifact.version));
    recover_staging_directory(&staging)?;
    if destination.is_file() {
        validate_confined_regular_file(&paths.state, &destination)?;
        validate_clickhouse(&destination, artifact.installed_sha256.as_deref())?;
        sync_dir(parent)?;
        return Ok(());
    }
    let archive = download_verified(paths, artifact).await?;
    ensure_private_dir(parent)?;
    ensure_private_dir(&staging).context("create ClickHouse extraction directory")?;
    let candidate = staging.join("clickhouse");
    match artifact.format {
        ArchiveFormat::Executable => std::fs::copy(&archive, &candidate)
            .map(|_| ())
            .context("copy ClickHouse executable")?,
        ArchiveFormat::TarGzClickhouse => extract_clickhouse(&archive, &candidate)?,
        ArchiveFormat::TarGzTree => bail!("unexpected ClickHouse archive format"),
    }
    std::fs::set_permissions(&candidate, std::fs::Permissions::from_mode(0o755))?;
    if artifact.format == ArchiveFormat::Executable {
        // The official macOS asset is a self-extracting stub whose first execution rewrites the file in place, so expansion and version validation must precede installed SHA-256.
        eprintln!("preparing ClickHouse executable");
        expand_and_validate_clickhouse(&candidate, &artifact.version)?;
    }
    validate_clickhouse(&candidate, artifact.installed_sha256.as_deref())?;
    File::open(&candidate)?.sync_all()?;
    let staged = staging.join("tree");
    std::fs::create_dir(&staged)?;
    std::fs::rename(&candidate, staged.join("clickhouse"))?;
    sync_dir(&staged)?;
    activate_directory(&staged, &destination_dir)?;
    finish_staging_directory(&staging)?;
    Ok(())
}

async fn download_verified(paths: &RuntimePaths, artifact: &Artifact) -> Result<PathBuf> {
    let destination = paths.downloads().join(&artifact.filename);
    let partial = paths
        .downloads()
        .join(format!(".{}.partial", artifact.filename));
    reject_symlink(&destination)?;
    reject_symlink(&partial)?;
    if destination.is_file()
        && std::fs::metadata(&destination)?.len() == artifact.archive_size
        && sha256(&destination)? == artifact.sha256
    {
        validate_private_file(&destination)?;
        remove_private_file_if_exists(&partial)?;
        return Ok(destination);
    }
    if destination.exists() {
        remove_private_file_if_exists(&destination).context("remove invalid cached archive")?;
    }
    if recover_verified_download(&partial, &destination, artifact)? {
        return Ok(destination);
    }
    eprintln!("downloading {} {}", artifact.product, artifact.version);
    let response = reqwest::Client::builder()
        .user_agent("kymo-local-runtime")
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(DOWNLOAD_IDLE_TIMEOUT)
        .build()?
        .get(&artifact.url)
        .send()
        .await
        .with_context(|| format!("download {}", artifact.url))?
        .error_for_status()
        .with_context(|| format!("download {}", artifact.url))?;
    if let Some(length) = response.content_length() {
        ensure!(
            length == artifact.archive_size,
            "download length mismatch before transfer: expected {}, got {length}",
            artifact.archive_size
        );
    }
    let mut temporary = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&partial)
        .context("create temporary download")?;
    let mut digest = Sha256::new();
    let mut received = 0_u64;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("read artifact response")?;
        received = received
            .checked_add(chunk.len() as u64)
            .context("artifact download size overflow")?;
        ensure!(
            received <= artifact.archive_size,
            "download exceeded expected size {}",
            artifact.archive_size
        );
        digest.update(&chunk);
        temporary.write_all(&chunk)?;
    }
    ensure!(
        received == artifact.archive_size,
        "download length mismatch: expected {}, got {received}",
        artifact.archive_size
    );
    let actual = format!("{:x}", digest.finalize());
    validate_digest(&artifact.filename, &artifact.sha256, &actual)?;
    temporary.sync_all().context("sync verified download")?;
    drop(temporary);
    std::fs::rename(&partial, &destination).context("publish verified download")?;
    sync_dir(&paths.downloads())?;
    eprintln!("verified {} ({received} bytes)", artifact.filename);
    Ok(destination)
}

fn recover_verified_download(
    partial: &Path,
    destination: &Path,
    artifact: &Artifact,
) -> Result<bool> {
    match std::fs::symlink_metadata(partial) {
        Ok(_) => validate_private_file(partial)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error).context("inspect partial artifact download"),
    }
    if std::fs::metadata(partial)?.len() == artifact.archive_size
        && sha256(partial)? == artifact.sha256
    {
        File::open(partial)?
            .sync_all()
            .context("sync recovered artifact download")?;
        std::fs::rename(partial, destination).context("recover verified artifact download")?;
        sync_dir(partial.parent().context("partial download has no parent")?)?;
        return Ok(true);
    }
    remove_private_file_if_exists(partial)?;
    Ok(false)
}

fn recover_staging_directory(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => validate_private_dir(path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("inspect artifact staging directory"),
    }
    std::fs::remove_dir_all(path).context("remove stale artifact staging directory")?;
    sync_dir(
        path.parent()
            .context("artifact staging directory has no parent")?,
    )
}

fn finish_staging_directory(path: &Path) -> Result<()> {
    std::fs::remove_dir(path).context("remove empty artifact staging directory")?;
    sync_dir(
        path.parent()
            .context("artifact staging directory has no parent")?,
    )
}

fn extract_clickhouse(archive: &Path, destination: &Path) -> Result<()> {
    let decoder = GzDecoder::new(BufReader::new(File::open(archive)?));
    let mut bundle = Archive::new(decoder);
    let mut match_count = 0;
    for entry in bundle.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        if entry.header().entry_type().is_file() && path.ends_with("usr/bin/clickhouse") {
            match_count += 1;
            if match_count == 1 {
                let mut output = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .mode(0o700)
                    .open(destination)?;
                std::io::copy(&mut entry, &mut output)?;
                output.sync_all()?;
            }
        }
    }
    ensure!(
        match_count == 1,
        "expected one usr/bin/clickhouse member, found {match_count}"
    );
    Ok(())
}

fn extract_tree(archive: &Path, destination: &Path) -> Result<()> {
    std::fs::create_dir(destination)?;
    let decoder = GzDecoder::new(BufReader::new(File::open(archive)?));
    let mut bundle = Archive::new(decoder);
    let mut archive_root = None;
    for entry in bundle.entries()? {
        let mut entry = entry?;
        let archive_path = entry.path()?.into_owned();
        let mut components = archive_path.components();
        let root = match components.next() {
            Some(Component::Normal(root)) => root.to_owned(),
            _ => bail!("archive contains an invalid root path"),
        };
        match &archive_root {
            Some(expected) => ensure!(
                expected == &root,
                "archive contains multiple root directories"
            ),
            None => archive_root = Some(root),
        }
        let relative: PathBuf = components.collect();
        ensure!(
            !relative
                .components()
                .any(|component| !matches!(component, Component::Normal(_))),
            "archive contains an unsafe path {}",
            archive_path.display()
        );
        if relative.as_os_str().is_empty() {
            continue;
        }
        let output = destination.join(relative);
        let kind = entry.header().entry_type();
        if kind.is_dir() {
            std::fs::create_dir_all(&output)?;
        } else if kind.is_file() {
            let parent = output.parent().context("archive file has no parent")?;
            std::fs::create_dir_all(parent)?;
            let mode = entry.header().mode()? & 0o755;
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(mode)
                .open(&output)?;
            std::io::copy(&mut entry, &mut file)?;
        } else if kind.is_symlink() {
            let target = entry
                .link_name()?
                .context("archive symlink has no target")?;
            ensure!(
                !target.is_absolute()
                    && !target
                        .components()
                        .any(|component| !matches!(component, Component::Normal(_))),
                "archive contains an unsafe symlink {}",
                archive_path.display()
            );
            let parent = output.parent().context("archive symlink has no parent")?;
            std::fs::create_dir_all(parent)?;
            std::os::unix::fs::symlink(target, output)?;
        } else {
            bail!(
                "archive contains unsupported entry {}",
                archive_path.display()
            );
        }
    }
    ensure!(archive_root.is_some(), "archive is empty");
    Ok(())
}

fn walk(root: &Path) -> Result<Vec<PathBuf>> {
    let mut pending = vec![root.to_path_buf()];
    let mut entries = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            let path = entry.path();
            let kind = entry.file_type()?;
            entries.push(path.clone());
            if kind.is_dir() {
                pending.push(path);
            }
        }
    }
    Ok(entries)
}

fn sync_tree(root: &Path) -> Result<()> {
    let mut entries = walk(root)?;
    for path in &entries {
        let metadata = std::fs::symlink_metadata(path)?;
        if metadata.is_file() {
            File::open(path)?.sync_all()?;
        }
    }
    entries.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for path in entries {
        if std::fs::symlink_metadata(&path)?.is_dir() {
            sync_dir(&path)?;
        }
    }
    sync_dir(root)
}

fn activate_directory(staged: &Path, destination: &Path) -> Result<()> {
    reject_symlink(destination)?;
    ensure!(
        !destination.exists(),
        "artifact destination already exists but is incomplete: {}",
        destination.display()
    );
    let parent = destination
        .parent()
        .context("artifact destination has no parent")?;
    std::fs::rename(staged, destination)
        .with_context(|| format!("activate artifact {}", destination.display()))?;
    let source_parent = staged.parent().context("staged artifact has no parent")?;
    sync_dir(source_parent)?;
    if source_parent != parent {
        sync_dir(parent)?;
    }
    Ok(())
}

fn sha256(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn validate_digest(filename: &str, expected: &str, actual: &str) -> Result<()> {
    ensure!(
        actual == expected,
        "{filename} SHA-256 mismatch: expected {expected}, got {actual}"
    );
    Ok(())
}

/// `postgres -C` reads the cluster's control file and configuration as a start does, without starting a server: a build that cannot open the cluster is never recorded. It reads the control file only for a runtime-computed parameter such as `data_checksums`; recheck that when moving PostgreSQL releases.
fn check_postgresql_data(paths: &RuntimePaths, version: &str) -> Result<()> {
    run_postgresql(
        std::process::Command::new(paths.postgresql_binary(version))
            .args(["-C", "data_checksums", "-D"])
            .arg(paths.postgresql_data()),
    )
    .with_context(|| format!("PostgreSQL {version} cannot open the existing data directory"))?;
    Ok(())
}

pub(crate) fn validate_postgresql(binary: &Path, expected_version: &str) -> Result<()> {
    let version = run_postgresql(std::process::Command::new(binary).arg("--version"))
        .context("PostgreSQL version check failed")?;
    // `postgres (PostgreSQL) 17.10` prints major.minor; catalog versions append a build number.
    let release = expected_version
        .rsplit_once('.')
        .map_or(expected_version, |(release, _build)| release);
    ensure!(
        version.split_whitespace().nth(2) == Some(release),
        "unexpected PostgreSQL version: {}",
        version.trim()
    );
    Ok(())
}

/// Run a PostgreSQL program with the stack's environment, so a library path set only in the caller cannot make a build look runnable; returns its standard output.
fn run_postgresql(command: &mut std::process::Command) -> Result<String> {
    let program = Path::new(command.get_program()).display().to_string();
    let output = command
        .env_clear()
        .envs(crate::supervisor::stack_environment())
        .output()
        .with_context(|| format!("execute {program}"))?;
    ensure!(
        output.status.success(),
        "{program} failed ({}): {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

pub(crate) fn validate_clickhouse(binary: &Path, installed_sha256: Option<&str>) -> Result<()> {
    let mode = std::fs::metadata(binary)
        .with_context(|| format!("stat {}", binary.display()))?
        .permissions()
        .mode();
    ensure!(
        mode & 0o100 != 0,
        "installed ClickHouse binary is not owner-executable"
    );
    let expected = installed_sha256.context("ClickHouse installed digest is not frozen")?;
    let actual = sha256(binary)?;
    ensure!(
        actual == expected,
        "installed ClickHouse SHA-256 mismatch: expected {expected}, got {actual}"
    );
    validate_macos_signatures(binary)?;
    Ok(())
}

fn expand_and_validate_clickhouse(binary: &Path, expected_version: &str) -> Result<()> {
    let output = std::process::Command::new(binary)
        .arg("--version")
        .output()
        .with_context(|| format!("execute {}", binary.display()))?;
    ensure!(output.status.success(), "ClickHouse version check failed");
    let version = String::from_utf8_lossy(&output.stdout);
    ensure!(
        version.contains(expected_version),
        "unexpected ClickHouse version: {}",
        version.trim()
    );
    Ok(())
}

#[cfg(target_os = "macos")]
fn validate_macos_signatures(root: &Path) -> Result<()> {
    let paths = if root.is_dir() {
        walk(root)?
    } else {
        vec![root.to_path_buf()]
    };
    for path in paths {
        if !path.is_file() || !is_macho(&path)? {
            continue;
        }
        let status = std::process::Command::new("codesign")
            .args(["--verify", "--strict"])
            .arg(&path)
            .status()
            .with_context(|| format!("verify Mach-O signature {}", path.display()))?;
        ensure!(
            status.success(),
            "invalid Mach-O signature {}",
            path.display()
        );
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn validate_macos_signatures(_root: &Path) -> Result<()> {
    Ok(())
}

#[cfg(target_os = "macos")]
fn is_macho(path: &Path) -> Result<bool> {
    let mut file = File::open(path)?;
    let mut magic = [0_u8; 4];
    if file.read(&mut magic)? != magic.len() {
        return Ok(false);
    }
    Ok(matches!(
        magic,
        [0xfe, 0xed, 0xfa, 0xce]
            | [0xce, 0xfa, 0xed, 0xfe]
            | [0xfe, 0xed, 0xfa, 0xcf]
            | [0xcf, 0xfa, 0xed, 0xfe]
            | [0xca, 0xfe, 0xba, 0xbe]
            | [0xbe, 0xba, 0xfe, 0xca]
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn no_install_mode_does_not_create_a_partial_installation() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("kymo");
        let paths = RuntimePaths::under(root.clone()).unwrap();
        let artifacts = for_target(SupportedTarget::current().unwrap()).unwrap();
        let error = ensure_installed(&paths, &artifacts, false)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("kymo install"));
        assert!(!root.exists());
    }

    #[test]
    fn clickhouse_extraction_requires_exactly_one_binary() {
        for count in [0, 1, 2] {
            let temporary = tempfile::tempdir().unwrap();
            let archive = temporary.path().join("clickhouse.tgz");
            let file = File::create(&archive).unwrap();
            let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
            let mut bundle = tar::Builder::new(encoder);
            for index in 0..count {
                let payload = format!("clickhouse-{index}");
                let mut header = tar::Header::new_gnu();
                header.set_size(payload.len() as u64);
                header.set_mode(0o755);
                header.set_cksum();
                bundle
                    .append_data(
                        &mut header,
                        format!("package-{index}/usr/bin/clickhouse"),
                        payload.as_bytes(),
                    )
                    .unwrap();
            }
            bundle.into_inner().unwrap().finish().unwrap();
            let output = temporary.path().join("clickhouse");
            let result = extract_clickhouse(&archive, &output);
            assert_eq!(result.is_ok(), count == 1);
        }
    }

    #[test]
    fn sha256_streams_file_contents() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("asset");
        std::fs::write(&path, b"mkdb2").unwrap();
        assert_eq!(
            sha256(&path).unwrap(),
            "14c72fb25a63b925622e3b5c21b0f17fd38e9db64148d676af6cc8ba9fc450a7"
        );
    }

    #[test]
    fn checksum_mismatch_is_rejected() {
        let error = validate_digest("artifact", "expected", "tampered").unwrap_err();
        assert!(error.to_string().contains("SHA-256 mismatch"));
    }

    #[test]
    fn clickhouse_validation_requires_owner_execute_permission() {
        let _serial = crate::serialize_process_test();
        let temporary = tempfile::tempdir().unwrap();
        let binary = temporary.path().join("clickhouse");
        std::fs::write(&binary, b"clickhouse").unwrap();
        let digest = sha256(&binary).unwrap();

        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o600)).unwrap();
        let error = validate_clickhouse(&binary, Some(&digest)).unwrap_err();
        assert!(error.to_string().contains("not owner-executable"));

        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
        validate_clickhouse(&binary, Some(&digest)).unwrap();
    }

    #[tokio::test]
    async fn chunked_downloads_are_verified_without_a_content_length() {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nexact\r\n0\r\n\r\n",
                )
                .unwrap();
        });

        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        paths.prepare().unwrap();
        ensure_private_dir(&paths.downloads()).unwrap();
        let artifact = Artifact {
            product: "test",
            version: "1".to_owned(),
            filename: "archive".to_owned(),
            url: format!("http://127.0.0.1:{port}/archive"),
            archive_size: 5,
            sha256: "fa79d4746c21cd960a17b92db8976ddef95a7e20b590721f8e0fa7847a05e486".to_owned(),
            installed_sha256: None,
            format: ArchiveFormat::Executable,
        };

        let downloaded = download_verified(&paths, &artifact).await.unwrap();
        server.join().unwrap();
        assert_eq!(std::fs::read(downloaded).unwrap(), b"exact");
    }

    #[tokio::test]
    async fn a_server_that_never_answers_fails_the_download() {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        // Accept and read the request, then hold the connection open without answering.
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).unwrap();
            std::thread::sleep(DOWNLOAD_IDLE_TIMEOUT * 3);
        });

        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        paths.prepare().unwrap();
        ensure_private_dir(&paths.downloads()).unwrap();
        let mut artifact = installed_artifacts().postgresql;
        artifact.url = format!("http://127.0.0.1:{port}/archive");

        let error = download_verified(&paths, &artifact).await.unwrap_err();
        assert!(format!("{error:#}").contains("timed out"), "{error:#}");
        server.join().unwrap();
    }

    #[test]
    fn deterministic_partial_downloads_recover_or_are_removed() {
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        paths.prepare().unwrap();
        ensure_private_dir(&paths.downloads()).unwrap();
        let artifact = Artifact {
            product: "test",
            version: "1".to_owned(),
            filename: "archive".to_owned(),
            url: "https://example.invalid/archive".to_owned(),
            archive_size: 5,
            sha256: "fa79d4746c21cd960a17b92db8976ddef95a7e20b590721f8e0fa7847a05e486".to_owned(),
            installed_sha256: None,
            format: ArchiveFormat::Executable,
        };
        let destination = paths.downloads().join(&artifact.filename);
        let partial = paths.downloads().join(".archive.partial");

        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&partial)
            .unwrap();
        file.write_all(b"exact").unwrap();
        file.sync_all().unwrap();
        drop(file);
        assert!(recover_verified_download(&partial, &destination, &artifact).unwrap());
        assert_eq!(std::fs::read(&destination).unwrap(), b"exact");

        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&partial)
            .unwrap();
        file.write_all(b"bad").unwrap();
        drop(file);
        assert!(!recover_verified_download(&partial, &destination, &artifact).unwrap());
        assert!(!partial.exists());
    }

    #[test]
    fn deterministic_artifact_staging_is_recovered() {
        let temporary = tempfile::tempdir().unwrap();
        let parent = temporary.path().canonicalize().unwrap().join("artifacts");
        ensure_private_dir(&parent).unwrap();
        let staging = parent.join(".17.10.0.installing");
        ensure_private_dir(&staging).unwrap();
        std::fs::write(staging.join("partial"), b"data").unwrap();

        recover_staging_directory(&staging).unwrap();
        assert!(!staging.exists());
    }

    #[test]
    fn tree_extraction_strips_one_root_and_preserves_confined_links() {
        let temporary = tempfile::tempdir().unwrap();
        let archive = temporary.path().join("postgresql.tgz");
        let file = File::create(&archive).unwrap();
        let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        let mut bundle = tar::Builder::new(encoder);
        let payload = b"postgres";
        let mut file_header = tar::Header::new_gnu();
        file_header.set_size(payload.len() as u64);
        file_header.set_mode(0o755);
        file_header.set_cksum();
        bundle
            .append_data(
                &mut file_header,
                "postgresql-17/bin/postgres",
                payload.as_slice(),
            )
            .unwrap();
        let mut link_header = tar::Header::new_gnu();
        link_header.set_entry_type(tar::EntryType::Symlink);
        link_header.set_size(0);
        link_header.set_mode(0o755);
        link_header.set_link_name("libpq.5.dylib").unwrap();
        link_header.set_cksum();
        bundle
            .append_data(
                &mut link_header,
                "postgresql-17/lib/libpq.dylib",
                std::io::empty(),
            )
            .unwrap();
        bundle.into_inner().unwrap().finish().unwrap();

        let output = temporary.path().join("tree");
        extract_tree(&archive, &output).unwrap();
        assert_eq!(std::fs::read(output.join("bin/postgres")).unwrap(), payload);
        assert_eq!(
            std::fs::read_link(output.join("lib/libpq.dylib")).unwrap(),
            Path::new("libpq.5.dylib")
        );
    }

    const FAKE_CLICKHOUSE: &[u8] = b"#!/bin/sh\n";

    fn write_executable(path: &Path, contents: &[u8]) {
        ensure_private_dir(path.parent().unwrap()).unwrap();
        std::fs::write(path, contents).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    fn fake_postgres(release: &str) -> Vec<u8> {
        format!("#!/bin/sh\necho 'postgres (PostgreSQL) {release}'\n").into_bytes()
    }

    /// The catalog an installation was made from, with stand-in executables.
    fn installed_artifacts() -> TargetArtifacts {
        let artifact = |product, version: &str, filename: &str, format| Artifact {
            product,
            version: version.to_owned(),
            filename: filename.to_owned(),
            url: "https://example.invalid/archive".to_owned(),
            archive_size: 1,
            sha256: "0".repeat(64),
            installed_sha256: None,
            format,
        };
        let mut clickhouse = artifact(
            "clickhouse",
            "25.3.14.14",
            "clickhouse-test",
            ArchiveFormat::Executable,
        );
        clickhouse.installed_sha256 = Some(format!("{:x}", Sha256::digest(FAKE_CLICKHOUSE)));
        TargetArtifacts {
            postgresql: artifact(
                "postgresql",
                "17.10.0",
                "postgresql-17.10.0-test.tar.gz",
                ArchiveFormat::TarGzTree,
            ),
            clickhouse,
        }
    }

    /// A later catalog that replaces the installed PostgreSQL build with `archive`.
    fn replacing_artifacts(installed: &TargetArtifacts, archive: &[u8]) -> TargetArtifacts {
        let mut next = installed.clone();
        next.postgresql.version = "17.11.0".to_owned();
        next.postgresql.filename = "postgresql-17.11.0-test.tar.gz".to_owned();
        next.postgresql.archive_size = archive.len() as u64;
        next.postgresql.sha256 = format!("{:x}", Sha256::digest(archive));
        next
    }

    fn postgresql_archive(postgres: &[u8]) -> Vec<u8> {
        let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        let mut bundle = tar::Builder::new(encoder);
        let mut header = tar::Header::new_gnu();
        header.set_size(postgres.len() as u64);
        header.set_mode(0o755);
        header.set_cksum();
        bundle
            .append_data(&mut header, "postgresql/bin/postgres", postgres)
            .unwrap();
        bundle.into_inner().unwrap().finish().unwrap()
    }

    fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Runtime::new().unwrap().block_on(future)
    }

    /// An installation made from `artifacts` by an older launcher, stopped.
    fn install_stand_in(paths: &RuntimePaths, artifacts: &TargetArtifacts) -> RuntimeManifest {
        paths.prepare().unwrap();
        write_executable(
            &paths.postgresql_binary(&artifacts.postgresql.version),
            &fake_postgres("17.10"),
        );
        write_executable(
            &paths.clickhouse_binary(&artifacts.clickhouse.version),
            FAKE_CLICKHOUSE,
        );
        let installation_uuid = initialize_installation_uuid(paths).unwrap();
        ensure_private_dir(&paths.postgresql_data()).unwrap();
        std::fs::write(paths.postgresql_data().join("PG_VERSION"), b"17\n").unwrap();
        let manifest =
            RuntimeManifest::installed(artifacts, installation_uuid, "0.0.0-older", 40_001, 40_002);
        manifest.write_atomic(&paths.manifest()).unwrap();
        manifest
    }

    #[test]
    fn a_recorded_predecessor_build_is_left_for_the_start_to_replace() {
        let _serial = crate::serialize_process_test();
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        let installed = installed_artifacts();
        let recorded = install_stand_in(&paths, &installed);
        let next = replacing_artifacts(&installed, &postgresql_archive(&fake_postgres("17.11")));

        for allow_install in [true, false] {
            let manifest = block_on(ensure_installed(&paths, &next, allow_install)).unwrap();
            assert_eq!(manifest, recorded);
        }
        assert_eq!(RuntimeManifest::read(&paths.manifest()).unwrap(), recorded);
        assert!(!paths.postgresql_dir("17.11.0").exists());
    }

    /// A later catalog whose PostgreSQL build is already downloaded.
    fn cached_replacement(
        paths: &RuntimePaths,
        installed: &TargetArtifacts,
        postgres: &[u8],
    ) -> TargetArtifacts {
        let archive = postgresql_archive(postgres);
        let next = replacing_artifacts(installed, &archive);
        ensure_private_dir(&paths.downloads()).unwrap();
        let cached = paths.downloads().join(&next.postgresql.filename);
        std::fs::write(&cached, &archive).unwrap();
        std::fs::set_permissions(&cached, std::fs::Permissions::from_mode(0o600)).unwrap();
        next
    }

    #[test]
    fn a_recorded_build_whose_files_are_gone_needs_an_install() {
        let _serial = crate::serialize_process_test();
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        let installed = installed_artifacts();
        install_stand_in(&paths, &installed);
        let next = replacing_artifacts(&installed, &postgresql_archive(&fake_postgres("17.11")));
        std::fs::remove_dir_all(paths.postgresql_dir("17.10.0")).unwrap();

        let error = block_on(ensure_installed(&paths, &next, false)).unwrap_err();
        assert!(error.to_string().contains("kymo install"), "{error:#}");
    }

    #[test]
    fn a_predecessor_build_is_replaced_by_the_catalog_build() {
        let _serial = crate::serialize_process_test();
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        let installed = installed_artifacts();
        let recorded = install_stand_in(&paths, &installed);
        let next = cached_replacement(&paths, &installed, &fake_postgres("17.11"));

        let manifest = block_on(replace_predecessor(&paths, &next, recorded.clone()));
        assert_eq!(manifest.postgresql, next.postgresql.identity());
        assert_eq!(manifest.installation_uuid, recorded.installation_uuid);
        assert_eq!(
            (manifest.dashboard_port, manifest.cdn_port),
            (recorded.dashboard_port, recorded.cdn_port)
        );
        assert_eq!(manifest.launcher_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(RuntimeManifest::read(&paths.manifest()).unwrap(), manifest);
        validate_postgresql(&paths.postgresql_binary("17.11.0"), "17.11.0").unwrap();
        // The replaced build stays on disk.
        assert!(paths.postgresql_binary("17.10.0").is_file());
    }

    #[test]
    fn a_replacement_that_cannot_open_the_data_keeps_the_recorded_build() {
        let _serial = crate::serialize_process_test();
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        let installed = installed_artifacts();
        let recorded = install_stand_in(&paths, &installed);
        let next = cached_replacement(
            &paths,
            &installed,
            b"#!/bin/sh\nif [ \"$1\" = --version ]; then echo 'postgres (PostgreSQL) 17.11'; exit; fi\necho 'FATAL:  database files are incompatible with server' >&2\nexit 1\n",
        );

        let manifest = block_on(replace_predecessor(&paths, &next, recorded.clone()));
        assert_eq!(manifest, recorded);
        assert_eq!(RuntimeManifest::read(&paths.manifest()).unwrap(), recorded);
    }

    #[test]
    fn a_failed_replacement_falls_back_to_the_recorded_build() {
        let _serial = crate::serialize_process_test();
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        let installed = installed_artifacts();
        let recorded = install_stand_in(&paths, &installed);
        let next = replacing_artifacts(&installed, &postgresql_archive(&fake_postgres("17.11")));

        let manifest = block_on(replace_predecessor(&paths, &next, recorded.clone()));
        assert_eq!(manifest, recorded);
        // The fallback writes no manifest.
        assert_eq!(RuntimeManifest::read(&paths.manifest()).unwrap(), recorded);
    }

    #[test]
    fn postgresql_version_check_matches_the_release_exactly_and_reports_loader_errors() {
        let _serial = crate::serialize_process_test();
        let temporary = tempfile::tempdir().unwrap();
        let binary = temporary
            .path()
            .canonicalize()
            .unwrap()
            .join("bin/postgres");
        write_executable(&binary, &fake_postgres("17.10"));
        validate_postgresql(&binary, "17.10.0").unwrap();
        validate_postgresql(&binary, "17.10.3").unwrap();
        for other in ["17.1.0", "17.100.0", "17.11.0", "18.10.0"] {
            assert!(validate_postgresql(&binary, other).is_err(), "{other}");
        }
        write_executable(
            &binary,
            b"#!/bin/sh\necho 'libxml2.so.2: cannot open shared object file' >&2\nexit 127\n",
        );
        let error = validate_postgresql(&binary, "17.10.0").unwrap_err();
        assert!(format!("{error:#}").contains("libxml2.so.2"), "{error:#}");
    }

    #[test]
    fn existing_manifest_without_identity_does_not_mint_a_replacement() {
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        paths.prepare().unwrap();
        let artifacts = for_target(SupportedTarget::current().unwrap()).unwrap();
        let installation_uuid = initialize_installation_uuid(&paths).unwrap();
        RuntimeManifest::installed(
            &artifacts,
            installation_uuid,
            env!("CARGO_PKG_VERSION"),
            40_001,
            40_002,
        )
        .write_atomic(&paths.manifest())
        .unwrap();
        std::fs::remove_file(paths.installation_id()).unwrap();

        assert!(
            RuntimeManifest::read_optional_validated(&paths, &artifacts, env!("CARGO_PKG_VERSION"))
                .is_err()
        );
        assert!(!paths.installation_id().exists());
    }
}
