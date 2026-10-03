use std::collections::BTreeMap;
use std::path::{Component, Path};

use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;

use crate::manifest::InstalledArtifact;

const CATALOG_JSON: &str = include_str!("../../../shared/local-runtime-artifacts.json");

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SupportedTarget {
    MacosArm64,
    LinuxX86_64,
}

impl SupportedTarget {
    pub const fn key(self) -> &'static str {
        match self {
            Self::MacosArm64 => "macos-arm64",
            Self::LinuxX86_64 => "linux-x86_64",
        }
    }

    pub fn current() -> Result<Self> {
        match (std::env::consts::OS, std::env::consts::ARCH) {
            ("macos", "aarch64") => Ok(Self::MacosArm64),
            ("linux", "x86_64") => Ok(Self::LinuxX86_64),
            (os, arch) => bail!("unsupported local-runtime target {os}-{arch}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ArchiveFormat {
    Executable,
    TarGzClickhouse,
    TarGzTree,
}

#[derive(Deserialize)]
struct Catalog {
    schema_version: u32,
    postgresql: Product,
    clickhouse: Product,
}

#[derive(Deserialize)]
struct Product {
    version: String,
    base_url: String,
    targets: BTreeMap<String, CatalogArtifact>,
}

#[derive(Deserialize)]
struct CatalogArtifact {
    filename: String,
    size: u64,
    sha256: String,
    installed_sha256: Option<String>,
    format: ArchiveFormat,
}

#[derive(Clone, Debug)]
pub struct Artifact {
    pub product: &'static str,
    pub version: String,
    pub filename: String,
    pub url: String,
    pub archive_size: u64,
    pub sha256: String,
    /// Digest after any artifact-defined preparation, such as macOS ClickHouse self-extraction.
    pub installed_sha256: Option<String>,
    pub format: ArchiveFormat,
}

impl Artifact {
    /// The manifest's record of this artifact.
    pub fn identity(&self) -> InstalledArtifact {
        InstalledArtifact {
            version: self.version.clone(),
            archive_sha256: self.sha256.clone(),
            installed_sha256: self.installed_sha256.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TargetArtifacts {
    pub postgresql: Artifact,
    pub clickhouse: Artifact,
}

pub fn for_target(target: SupportedTarget) -> Result<TargetArtifacts> {
    let catalog: Catalog = serde_json::from_str(CATALOG_JSON).context("parse artifact catalog")?;
    ensure!(
        catalog.schema_version == 1,
        "unsupported artifact catalog schema"
    );
    Ok(TargetArtifacts {
        postgresql: artifact("postgresql", catalog.postgresql, target)?,
        clickhouse: artifact("clickhouse", catalog.clickhouse, target)?,
    })
}

fn artifact(
    product_name: &'static str,
    product: Product,
    target: SupportedTarget,
) -> Result<Artifact> {
    validate_path_component("version", &product.version)?;
    ensure!(
        product.base_url.starts_with("https://"),
        "{product_name} base URL must use HTTPS"
    );
    let value = product
        .targets
        .get(target.key())
        .with_context(|| format!("{product_name} has no artifact for {}", target.key()))?;
    validate_path_component("artifact filename", &value.filename)?;
    ensure!(value.size > 0, "{product_name} has an invalid archive size");
    ensure!(
        value.sha256.len() == 64 && value.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "{product_name} has an invalid SHA-256"
    );
    if let Some(digest) = &value.installed_sha256 {
        ensure!(
            digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "{product_name} has an invalid installed SHA-256"
        );
    }
    Ok(Artifact {
        product: product_name,
        version: product.version,
        filename: value.filename.clone(),
        url: format!("{}/{}", product.base_url, value.filename),
        archive_size: value.size,
        sha256: value.sha256.clone(),
        installed_sha256: value.installed_sha256.clone(),
        format: value.format,
    })
}

pub(crate) fn validate_path_component(name: &str, value: &str) -> Result<()> {
    let mut components = Path::new(value).components();
    ensure!(
        matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none(),
        "{name} must be one normal path component"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_freezes_exactly_the_supported_targets() {
        let catalog: Catalog = serde_json::from_str(CATALOG_JSON).unwrap();
        let expected = vec!["linux-x86_64", "macos-arm64"];
        assert_eq!(
            catalog
                .postgresql
                .targets
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            catalog
                .clickhouse
                .targets
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            expected
        );
        for target in [SupportedTarget::MacosArm64, SupportedTarget::LinuxX86_64] {
            let artifacts = for_target(target).unwrap();
            assert_eq!(artifacts.postgresql.version, "17.11.0");
            assert_eq!(artifacts.clickhouse.version, "25.3.14.14");
            assert!(artifacts.postgresql.url.starts_with("https://github.com/"));
            assert!(artifacts.clickhouse.url.starts_with("https://github.com/"));
            assert!(artifacts.postgresql.archive_size > 0);
            assert!(artifacts.clickhouse.archive_size > 0);
        }
    }

    #[test]
    fn catalog_path_components_cannot_escape_managed_roots() {
        for value in ["", ".", "..", "../outside", "/absolute", "nested/name"] {
            assert!(validate_path_component("test", value).is_err(), "{value:?}");
        }
        validate_path_component("test", "postgresql-17.10.0.tar.gz").unwrap();
    }
}
