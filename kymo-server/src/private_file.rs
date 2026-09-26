use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;

use anyhow::{ensure, Context, Result};

pub(crate) fn read(path: &Path, description: &str, max_bytes: u64) -> Result<Vec<u8>> {
    // Keep these trust checks aligned with local-runtime/core/src/paths.rs; the edition-2021 server and edition-2024 launcher cannot safely share a rustfmt-managed source file, so drift between the copies is a security bug.
    let parent = path
        .parent()
        .with_context(|| format!("{description} path has no parent"))?;
    let parent_metadata = std::fs::symlink_metadata(parent)
        .with_context(|| format!("inspect {description} directory {}", parent.display()))?;
    ensure!(
        parent_metadata.is_dir()
            && parent_metadata.uid() == unsafe { libc::geteuid() }
            && parent_metadata.permissions().mode() & 0o077 == 0
            && parent.canonicalize()? == parent,
        "{description} directory is not a private canonical directory"
    );
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("inspect {description} file {}", path.display()))?;
    ensure!(
        metadata.is_file(),
        "{description} path is not a regular file"
    );
    ensure!(
        metadata.uid() == unsafe { libc::geteuid() },
        "{description} file is not owned by the current user"
    );
    ensure!(
        metadata.permissions().mode() & 0o077 == 0,
        "{description} file grants group or other access"
    );
    ensure!(
        metadata.len() <= max_bytes,
        "{description} file is too large"
    );
    ensure!(
        path.canonicalize()? == path,
        "{description} path resolves through a symlink"
    );
    std::fs::read(path).with_context(|| format!("read {description} file"))
}
