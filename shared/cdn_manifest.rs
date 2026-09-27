//! The CDN manifest shape, included by path in the frontend, which renders every root through it, and in the server's CDN collector, which keeps every key it links (docs/cdn-gcs-migration.md § Garbage collection).

use serde::Deserialize;

/// Bump on any change that could make a new key reachable — to these structs, to what the frontend links, or to how the collector extracts links — so the collector re-parses every root it cached under an older version.
pub const LINKS_VERSION: u32 = 1;

/// `Data` is the metadata payload's type: the frontend reads it, the collector skips it.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Manifest<Data> {
    #[serde(default)]
    pub v: u32,
    #[serde(default)]
    pub class: String,
    #[serde(default)]
    pub items: Vec<ManifestItem>,
    #[serde(default)]
    pub data: Option<Data>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ManifestItem {
    pub resource: String,
    pub filename: Option<String>,
    #[serde(default)]
    pub caption: Option<String>,
}

impl<Data> Manifest<Data> {
    /// Every key the manifest links, which must cover every field the frontend links. Nothing reads a linked object as a manifest.
    pub fn resources(&self) -> impl Iterator<Item = &str> {
        self.items.iter().map(|item| item.resource.as_str())
    }
}
