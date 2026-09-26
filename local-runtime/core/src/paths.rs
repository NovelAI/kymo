use std::fs::{DirBuilder, File};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use directories::ProjectDirs;

#[derive(Clone, Debug)]
pub struct RuntimePaths {
    pub data: PathBuf,
    pub state: PathBuf,
    pub cache: PathBuf,
}

impl RuntimePaths {
    pub fn discover() -> Result<Self> {
        if std::env::var_os("MKDB2_LOCAL_ROOT").is_some() {
            bail!("MKDB2_LOCAL_ROOT is no longer supported; use KYMO_LOCAL_ROOT");
        }
        if let Some(root) = std::env::var_os("KYMO_LOCAL_ROOT") {
            let root = PathBuf::from(root);
            ensure!(
                root.is_absolute(),
                "KYMO_LOCAL_ROOT must be an absolute path"
            );
            ensure!(
                root.components().all(|component| matches!(
                    component,
                    Component::RootDir | Component::Normal(_)
                )) && root
                    .components()
                    .any(|component| matches!(component, Component::Normal(_))),
                "KYMO_LOCAL_ROOT must be a normalized path below the filesystem root"
            );
            return Self::under(root);
        }
        let project = ProjectDirs::from("ai", "NovelAI", "kymo-local-runtime")
            .context("cannot determine per-user kymo-local-runtime directories")?;
        Self::from_project_paths(
            project.data_dir(),
            project.data_local_dir(),
            project.state_dir(),
            project.cache_dir(),
        )
    }

    pub fn under(root: PathBuf) -> Result<Self> {
        let root = normalize_managed_path(&root)?;
        let paths = Self {
            data: root.join("data"),
            state: root.join("state"),
            cache: root.join("cache"),
        };
        paths.validate_disjoint_roots()?;
        Ok(paths)
    }

    fn from_project_paths(
        data: &Path,
        data_local: &Path,
        state: Option<&Path>,
        cache: &Path,
    ) -> Result<Self> {
        let paths = Self {
            data: lexical_managed_path(&data.join("data"))?,
            state: lexical_managed_path(
                &state
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| data_local.join("state")),
            )?,
            cache: lexical_managed_path(cache)?,
        };
        paths.validate_disjoint_roots()?;
        Ok(paths)
    }

    pub fn prepare(&self) -> Result<()> {
        self.validate_disjoint_roots()?;
        for path in [&self.data, &self.state, &self.cache] {
            ensure_private_dir(path)?;
        }
        Ok(())
    }

    fn validate_disjoint_roots(&self) -> Result<()> {
        let roots = [&self.data, &self.state, &self.cache];
        for (index, root) in roots.iter().enumerate() {
            for other in &roots[index + 1..] {
                ensure!(
                    root != other && !root.starts_with(other) && !other.starts_with(root),
                    "managed roots must not overlap: {} and {}",
                    root.display(),
                    other.display()
                );
            }
        }
        Ok(())
    }

    pub fn installation_id(&self) -> PathBuf {
        self.data.join("installation.uuid")
    }

    pub fn manifest(&self) -> PathBuf {
        self.state.join("runtime.json")
    }

    pub fn install_lock(&self) -> PathBuf {
        self.state.join("install.lock")
    }

    pub fn downloads(&self) -> PathBuf {
        self.cache.join("downloads")
    }

    pub fn artifacts(&self) -> PathBuf {
        self.state.join("artifacts")
    }

    pub fn generations(&self) -> PathBuf {
        self.state.join("generations")
    }

    pub fn postgresql_data(&self) -> PathBuf {
        self.data.join("postgresql")
    }

    pub fn clickhouse_data(&self) -> PathBuf {
        self.data.join("clickhouse")
    }

    pub fn cdn_data(&self) -> PathBuf {
        self.data.join("cdn")
    }

    pub fn postgresql_dir(&self, version: &str) -> PathBuf {
        self.artifacts().join("postgresql").join(version)
    }

    pub fn postgresql_binary(&self, version: &str) -> PathBuf {
        self.postgresql_dir(version).join("bin/postgres")
    }

    pub fn clickhouse_dir(&self, version: &str) -> PathBuf {
        self.artifacts().join("clickhouse").join(version)
    }

    pub fn clickhouse_binary(&self, version: &str) -> PathBuf {
        self.clickhouse_dir(version).join("clickhouse")
    }
}

fn normalize_managed_path(path: &Path) -> Result<PathBuf> {
    ensure!(path.is_absolute(), "managed path must be absolute");
    ensure!(
        path.components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_))),
        "managed path must be normalized"
    );
    let mut ancestor = path.parent().context("managed path has no parent")?;
    let mut suffix = vec![path.file_name().context("managed path has no name")?];
    let canonical_parent = loop {
        match ancestor.canonicalize() {
            Ok(canonical) => break canonical,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                suffix.push(
                    ancestor
                        .file_name()
                        .context("managed path has no existing ancestor")?,
                );
                ancestor = ancestor
                    .parent()
                    .context("managed path has no existing ancestor")?;
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("resolve managed path parent {}", ancestor.display())
                });
            }
        }
    };
    Ok(suffix
        .into_iter()
        .rev()
        .fold(canonical_parent, |resolved, component| {
            resolved.join(component)
        }))
}

fn lexical_managed_path(path: &Path) -> Result<PathBuf> {
    ensure!(path.is_absolute(), "managed path must be absolute");
    ensure!(
        path.components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_))),
        "managed path must be normalized"
    );
    Ok(path.to_path_buf())
}

pub fn ensure_private_dir(path: &Path) -> Result<()> {
    ensure!(
        path.is_absolute(),
        "private directory must be an absolute path"
    );
    if path.exists() {
        validate_private_dir(path)?;
        sync_dir(path.parent().context("private directory has no parent")?)?;
        return Ok(());
    }
    let parent = path.parent().context("private directory has no parent")?;
    if !parent.exists() {
        ensure_private_dir(parent)?;
    }
    validate_unredirected_path(parent)?;
    match DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => sync_dir(parent)?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("create private directory {}", path.display()));
        }
    }
    validate_private_dir(path)
}

pub fn validate_private_dir(path: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("inspect private directory {}", path.display()))?;
    ensure!(metadata.is_dir(), "{} is not a directory", path.display());
    ensure!(
        !metadata.file_type().is_symlink(),
        "{} is a symlink",
        path.display()
    );
    ensure!(
        metadata.uid() == unsafe { libc::geteuid() },
        "{} is not owned by the current user",
        path.display()
    );
    ensure!(
        metadata.permissions().mode() & 0o077 == 0,
        "{} grants group or other access",
        path.display()
    );
    validate_unredirected_path(path)?;
    Ok(())
}

pub fn validate_private_file(path: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("inspect private file {}", path.display()))?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "{} is not a regular file",
        path.display()
    );
    ensure!(
        metadata.uid() == unsafe { libc::geteuid() },
        "{} is not owned by the current user",
        path.display()
    );
    ensure!(
        metadata.permissions().mode() & 0o077 == 0,
        "{} grants group or other access",
        path.display()
    );
    validate_unredirected_path(path)
}

pub fn validate_confined_regular_file(root: &Path, path: &Path) -> Result<()> {
    validate_private_dir(root)?;
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("inspect confined file {}", path.display()))?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "{} is not a regular file",
        path.display()
    );
    let resolved = path
        .canonicalize()
        .with_context(|| format!("resolve confined file {}", path.display()))?;
    ensure!(
        resolved.starts_with(root),
        "{} resolves outside {} to {}",
        path.display(),
        root.display(),
        resolved.display()
    );
    Ok(())
}

pub fn remove_private_file_if_exists(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => validate_private_file(path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error).with_context(|| format!("inspect private file {}", path.display()));
        }
    }
    std::fs::remove_file(path)
        .with_context(|| format!("remove private file {}", path.display()))?;
    sync_dir(path.parent().context("private file has no parent")?)?;
    Ok(true)
}

fn validate_unredirected_path(path: &Path) -> Result<()> {
    let canonical = path
        .canonicalize()
        .with_context(|| format!("resolve private path {}", path.display()))?;
    ensure!(
        canonical == path,
        "{} resolves through a symlink to {}",
        path.display(),
        canonical.display()
    );
    Ok(())
}

pub fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

pub fn reject_symlink(path: &Path) -> Result<()> {
    if let Ok(metadata) = std::fs::symlink_metadata(path)
        && metadata.file_type().is_symlink()
    {
        bail!("refusing symlinked path {}", path.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_roots_are_disjoint() {
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        paths.validate_disjoint_roots().unwrap();

        let nested = RuntimePaths {
            data: temporary.path().join("data"),
            state: temporary.path().join("data/state"),
            cache: temporary.path().join("cache"),
        };
        assert!(nested.validate_disjoint_roots().is_err());
    }

    #[test]
    fn project_layout_keeps_identity_data_separate_from_state() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let support = root.join("Application Support/kymo-local-runtime");
        let cache = root.join("Caches/kymo-local-runtime");
        let paths = RuntimePaths::from_project_paths(&support, &support, None, &cache).unwrap();

        assert_eq!(paths.data, support.join("data"));
        assert_eq!(paths.state, support.join("state"));
        paths.prepare().unwrap();
        crate::manifest::initialize_installation_uuid(&paths).unwrap();
    }

    #[test]
    fn private_directories_reject_symlinked_ancestors() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let real_root = root.join("real");
        ensure_private_dir(&real_root).unwrap();
        let managed = real_root.join("managed");
        ensure_private_dir(&managed).unwrap();
        let alias = root.join("alias");
        std::os::unix::fs::symlink(&real_root, &alias).unwrap();

        assert!(validate_private_dir(&alias.join("managed")).is_err());
        assert!(ensure_private_dir(&alias.join("new-managed")).is_err());
        assert!(!real_root.join("new-managed").exists());
    }

    #[test]
    fn confined_files_cannot_escape_through_nested_symlinks() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let managed = root.join("managed");
        let outside = root.join("outside");
        ensure_private_dir(&managed).unwrap();
        ensure_private_dir(&outside).unwrap();
        std::fs::write(managed.join("inside"), b"inside").unwrap();
        std::fs::write(outside.join("escaped"), b"outside").unwrap();
        std::os::unix::fs::symlink(&outside, managed.join("redirect")).unwrap();

        validate_confined_regular_file(&managed, &managed.join("inside")).unwrap();
        assert!(
            validate_confined_regular_file(&managed, &managed.join("redirect/escaped")).is_err()
        );
    }
}
