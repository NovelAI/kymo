use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=MKDB2_FRONTEND_BUNDLE_DIR");
    if std::env::var_os("MKDB2_FRONTEND_BUNDLE_DIR").is_some() {
        return Err(
            "MKDB2_FRONTEND_BUNDLE_DIR is no longer supported; use KYMO_FRONTEND_BUNDLE_DIR".into(),
        );
    }
    println!("cargo:rerun-if-changed=../proto/kymo.proto");
    println!("cargo:rerun-if-changed=../proto/local_runtime.proto");
    // Use one known-new-enough protoc in local wheels, CI, and container builds.
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["../proto/kymo.proto", "../proto/local_runtime.proto"],
            &["../proto"],
        )?;
    generate_frontend_assets()?;
    Ok(())
}

fn generate_frontend_assets() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=KYMO_FRONTEND_BUNDLE_DIR");
    let enabled = std::env::var_os("CARGO_FEATURE_LOCAL_BUNDLE").is_some();
    let root = if enabled {
        std::env::var_os("KYMO_FRONTEND_BUNDLE_DIR")
            .map(PathBuf::from)
            .map(|path| {
                path.canonicalize().map_err(|error| {
                    format!(
                        "canonicalize frontend bundle directory {}: {error}",
                        path.display()
                    )
                })
            })
            .transpose()?
    } else {
        None
    };
    let mut files = Vec::new();
    if enabled {
        if let Some(root) = &root {
            println!("cargo:rerun-if-changed={}", root.display());
            collect_files(root, root, &mut files)?;
            let index = files
                .iter()
                .find(|(name, _)| name == "index.html")
                .ok_or_else(|| format!("frontend bundle {} has no index.html", root.display()))?;
            let index_text = std::fs::read_to_string(&index.1)
                .map_err(|error| format!("read frontend index {}: {error}", index.1.display()))?;
            if index_text.matches("</head>").count() != 1 {
                return Err("frontend index must contain exactly one </head> marker".into());
            }
        }
    }
    let available = enabled && root.is_some();
    let mut entries = String::new();
    for (name, path) in &files {
        // Content-hash ETags let the browser revalidate the large WASM instead of re-downloading it on every open.
        let etag = format!("\"{:x}\"", Sha256::digest(std::fs::read(path)?));
        entries.push_str(&format!(
            "    ({name:?}, super::Asset {{ bytes: include_bytes!({path:?}), etag: {etag:?} }}),\n",
            path = path.to_string_lossy()
        ));
    }
    let generated = format!(
        "pub const AVAILABLE: bool = {available};\npub static ASSETS: &[(&str, super::Asset)] = &[\n{entries}];\n"
    );
    std::fs::write(
        PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR is set"))
            .join("frontend_assets.rs"),
        generated,
    )?;
    Ok(())
}

fn collect_files(
    root: &Path,
    directory: &Path,
    files: &mut Vec<(String, PathBuf)>,
) -> Result<(), Box<dyn std::error::Error>> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Err(format!("frontend bundle contains symlink {}", path.display()).into());
        }
        if file_type.is_dir() {
            collect_files(root, &path, files)?;
        } else if file_type.is_file() {
            println!("cargo:rerun-if-changed={}", path.display());
            let name = path
                .strip_prefix(root)?
                .to_string_lossy()
                .replace('\\', "/");
            files.push((name, path));
        }
    }
    files.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(())
}
