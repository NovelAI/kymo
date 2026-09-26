use std::process::Command;

#[path = "../../shared/retired_env.rs"]
mod retired_env;

#[test]
fn retired_local_names_fail_before_managed_root_creation_even_when_blank_and_masked() {
    for (legacy, canonical) in retired_env::RETIRED_LOCAL_ENV
        .iter()
        .chain(retired_env::RETIRED_HOSTED_ENV)
    {
        let temporary = tempfile::tempdir().unwrap();
        let managed_root = temporary.path().join("managed");
        let mut command = Command::new(env!("CARGO_BIN_EXE_kymo"));
        command
            .env_clear()
            .env(canonical, "masked")
            .env("KYMO_LOCAL_ROOT", &managed_root)
            .env("KYMO_LOCAL_NO_INSTALL", "0")
            .env(legacy, "")
            .args(["ensure", "--json"]);
        let output = command.output().unwrap();
        assert!(!output.status.success(), "{legacy} was accepted");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(&format!("{legacy} is no longer supported; use {canonical}")),
            "unexpected {legacy} error: {stderr}"
        );
        assert!(
            !managed_root.exists(),
            "{legacy} mutated the managed root before rejection"
        );
    }

    let temporary = tempfile::tempdir().unwrap();
    let managed_root = temporary.path().join("managed");
    let output = Command::new(env!("CARGO_BIN_EXE_kymo"))
        .env_clear()
        .env("KYMO_LOCAL_ROOT", &managed_root)
        .env("MKDB2_RUN_REAPER_INTERVAL_SECONDS", "")
        .args(["ensure", "--json"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("MKDB2_RUN_REAPER_INTERVAL_SECONDS is no longer supported")
    );
    assert!(!managed_root.exists());

    let temporary = tempfile::tempdir().unwrap();
    let managed_root = temporary.path().join("managed");
    let output = Command::new(env!("CARGO_BIN_EXE_kymo"))
        .env_clear()
        .env("KYMO_LOCAL_ROOT", &managed_root)
        .env("KYMO_RUN_REAPER_INTERVAL_SECONDS", "")
        .args(["ensure", "--json"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("KYMO_RUN_REAPER_INTERVAL_SECONDS is not supported")
    );
    assert!(!managed_root.exists());
}

#[test]
fn invalid_canonical_no_install_fails_before_managed_root_creation() {
    let temporary = tempfile::tempdir().unwrap();
    let managed_root = temporary.path().join("managed");
    let output = Command::new(env!("CARGO_BIN_EXE_kymo"))
        .env_clear()
        .env("KYMO_LOCAL_ROOT", &managed_root)
        .env("KYMO_LOCAL_NO_INSTALL", "invalid")
        .args(["ensure", "--json"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("KYMO_LOCAL_NO_INSTALL must be one of")
    );
    assert!(!managed_root.exists());
}
