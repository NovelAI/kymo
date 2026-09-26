use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use kymo_local_runtime_core::paths::{RuntimePaths, ensure_private_dir, sync_dir};
use kymo_local_runtime_core::profile::{clickhouse_config_xml, clickhouse_users_xml};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use serde::Serialize;
use uuid::Uuid;

const AUTH_FORMAT_VERSION: u32 = 1;

pub(crate) struct PreparedGeneration {
    pub(crate) id: Uuid,
    pub(crate) root: PathBuf,
    pub(crate) auth_secret: PathBuf,
    pub(crate) lifecycle_secret: PathBuf,
    pub(crate) supervisor_secret: PathBuf,
    pub(crate) supervisor_bearer: String,
    pub(crate) server_bearer: String,
    pub(crate) lifecycle_bearer: String,
    pub(crate) clickhouse_certificate: PathBuf,
    pub(crate) clickhouse_password: String,
    pub(crate) clickhouse_port: u16,
}

#[derive(Serialize)]
struct ClientSecrets<'a> {
    format_version: u32,
    server_bearer: &'a str,
}

#[derive(Serialize)]
struct LifecycleSecret<'a> {
    format_version: u32,
    lifecycle_bearer: &'a str,
}

#[derive(Serialize)]
struct SupervisorSecret<'a> {
    format_version: u32,
    supervisor_bearer: &'a str,
}

pub(crate) fn prepare(paths: &RuntimePaths, clickhouse_port: u16) -> Result<PreparedGeneration> {
    ensure!(
        clickhouse_port >= 1024,
        "ClickHouse port must be non-system"
    );
    for path in [
        paths.postgresql_data(),
        paths.clickhouse_data(),
        paths.cdn_data(),
        paths.generations(),
    ] {
        ensure_private_dir(&path)?;
    }

    let id = Uuid::new_v4();
    let root = paths.generations().join(id.to_string());
    ensure_private_dir(&root)?;
    for name in ["data", "tmp", "user-files", "format-schemas"] {
        ensure_private_dir(&paths.clickhouse_data().join(name))?;
    }

    let (certificate_pem, private_key_pem) = certificate_material()?;
    let certificate = root.join("clickhouse.pem");
    let private_key = root.join("clickhouse.key");
    write_private(&certificate, certificate_pem.as_bytes())?;
    write_private(&private_key, private_key_pem.as_bytes())?;

    let server_bearer = random_token()?;
    let lifecycle_bearer = random_token()?;
    let supervisor_bearer = random_token()?;
    let clickhouse_password = random_token()?;
    let auth_secret = root.join("auth.json");
    let lifecycle_secret = root.join("lifecycle.json");
    let supervisor_secret = root.join("supervisor.json");
    write_json(
        &auth_secret,
        &ClientSecrets {
            format_version: AUTH_FORMAT_VERSION,
            server_bearer: &server_bearer,
        },
    )?;
    write_json(
        &lifecycle_secret,
        &LifecycleSecret {
            format_version: AUTH_FORMAT_VERSION,
            lifecycle_bearer: &lifecycle_bearer,
        },
    )?;
    write_json(
        &supervisor_secret,
        &SupervisorSecret {
            format_version: AUTH_FORMAT_VERSION,
            supervisor_bearer: &supervisor_bearer,
        },
    )?;
    let users = root.join("users.xml");
    write_private(
        &users,
        clickhouse_users_xml(&clickhouse_password).as_bytes(),
    )?;
    write_private(
        &root.join("config.xml"),
        clickhouse_config_xml(
            &paths.clickhouse_data(),
            clickhouse_port,
            &certificate,
            &private_key,
            &users,
        )?
        .as_bytes(),
    )?;
    sync_dir(&root)?;

    Ok(PreparedGeneration {
        id,
        root,
        auth_secret,
        lifecycle_secret,
        supervisor_secret,
        supervisor_bearer,
        server_bearer,
        lifecycle_bearer,
        clickhouse_certificate: certificate,
        clickhouse_password,
        clickhouse_port,
    })
}

fn random_token() -> Result<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes).context("generate local runtime secret")?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn certificate_material() -> Result<(String, String)> {
    // webpki ignores the common name. DNS:localhost is the deliberate SAN because kymo-server connects to https://localhost while browser Origins use 127.0.0.1.
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["localhost".to_owned()])?;
    Ok((cert.pem(), signing_key.serialize_pem()))
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    write_private(path, &bytes)
}

fn write_private(path: &Path, contents: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("create private generation file {}", path.display()))?;
    file.write_all(contents)?;
    file.sync_all()
        .with_context(|| format!("sync private generation file {}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;

    use rustls::RootCertStore;
    use rustls::client::WebPkiServerVerifier;
    use rustls::client::danger::ServerCertVerifier;
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};

    use super::*;

    #[test]
    fn generation_is_private_distinct_and_san_pinned() {
        let temporary = tempfile::tempdir().unwrap();
        let paths = RuntimePaths::under(temporary.path().join("kymo")).unwrap();
        paths.prepare().unwrap();
        let generation = prepare(&paths, 18123).unwrap();
        assert_eq!(
            std::fs::metadata(&generation.root)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        for path in [
            &generation.auth_secret,
            &generation.lifecycle_secret,
            &generation.supervisor_secret,
            &generation.clickhouse_certificate,
        ] {
            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let auth: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&generation.auth_secret).unwrap()).unwrap();
        let lifecycle: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&generation.lifecycle_secret).unwrap()).unwrap();
        let tokens = [
            auth["server_bearer"].as_str().unwrap(),
            lifecycle["lifecycle_bearer"].as_str().unwrap(),
            &generation.supervisor_bearer,
            &generation.clickhouse_password,
        ];
        assert!(tokens.iter().all(|token| token.len() == 43));
        assert_eq!(tokens.into_iter().collect::<BTreeSet<_>>().len(), 4);

        let certificate = CertificateDer::from_pem_slice(
            &std::fs::read(&generation.clickhouse_certificate).unwrap(),
        )
        .unwrap();
        let mut roots = RootCertStore::empty();
        roots.add(certificate.clone()).unwrap();
        let verifier = WebPkiServerVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::new(rustls::crypto::ring::default_provider()),
        )
        .build()
        .unwrap();
        verifier
            .verify_server_cert(
                &certificate,
                &[],
                &ServerName::try_from("localhost").unwrap(),
                &[],
                UnixTime::now(),
            )
            .unwrap();
        assert!(
            verifier
                .verify_server_cert(
                    &certificate,
                    &[],
                    &ServerName::try_from("127.0.0.1").unwrap(),
                    &[],
                    UnixTime::now(),
                )
                .is_err()
        );
    }
}
