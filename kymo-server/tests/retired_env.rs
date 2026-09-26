use std::process::Command;

use kymo_server::retired_env::RETIRED_HOSTED_ENV;

#[test]
fn retired_server_names_fail_before_startup_even_when_blank_and_masked() {
    for (legacy, canonical) in RETIRED_HOSTED_ENV {
        let output = Command::new(env!("CARGO_BIN_EXE_kymo-server"))
            .env_clear()
            .env(legacy, "")
            .env(canonical, "masked")
            .output()
            .unwrap();
        assert!(!output.status.success(), "{legacy} was accepted");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(&format!("{legacy} is no longer supported; use {canonical}")),
            "unexpected {legacy} error: {stderr}"
        );
    }

    let output = Command::new(env!("CARGO_BIN_EXE_kymo-server"))
        .env_clear()
        .env("CDN_ROOT", "")
        .env("KYMO_CDN_ROOT", "masked")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("CDN_ROOT is no longer supported; use KYMO_CDN_ROOT"));

    let output = Command::new(env!("CARGO_BIN_EXE_kymo-server"))
        .env_clear()
        .env("MKDB2_RUN_REAPER_INTERVAL_SECONDS", "")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("MKDB2_RUN_REAPER_INTERVAL_SECONDS is no longer supported"));

    let output = Command::new(env!("CARGO_BIN_EXE_kymo-server"))
        .env_clear()
        .env("KYMO_RUN_REAPER_INTERVAL_SECONDS", "")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("KYMO_RUN_REAPER_INTERVAL_SECONDS is not supported"));
}
