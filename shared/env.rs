//! Typed environment configuration shared by kymo binaries.

use anyhow::{bail, ensure, Context, Result};

const MEBIBYTE: usize = 1024 * 1024;

fn parse_bool(name: &str, raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "yes" => Ok(true),
        "0" | "false" | "off" | "no" => Ok(false),
        _ => bail!("{name} must be true/false, 1/0, on/off, or yes/no; got {raw:?}"),
    }
}

fn parse_usize(name: &str, raw: &str) -> Result<usize> {
    raw.trim()
        .parse::<usize>()
        .with_context(|| format!("{name} must be a non-negative integer; got {raw:?}"))
}

fn nonempty(raw: String) -> Option<String> {
    (!raw.trim().is_empty()).then_some(raw)
}

fn nonempty_var(name: &str) -> std::result::Result<Option<String>, std::env::VarError> {
    match std::env::var(name) {
        Ok(raw) => Ok(nonempty(raw)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error),
    }
}

fn reject_retired_with(retired: &[(&str, &str)], present: impl Fn(&str) -> bool) -> Result<()> {
    for (legacy_name, canonical_name) in retired {
        ensure!(
            !present(legacy_name),
            "{legacy_name} is no longer supported; use {canonical_name}"
        );
    }
    Ok(())
}

/// Reject retired environment names, including names set to a blank value.
/// Call this at process entry before logging, metrics, network, or storage
/// initialization so stale deployment configuration cannot be masked by a
/// canonical value or a default.
pub fn reject_retired_environment(retired: &[(&str, &str)]) -> Result<()> {
    reject_retired_with(retired, |name| std::env::var_os(name).is_some())
}

fn bounded_value(raw: Option<&str>, default: usize, min: usize, max: usize) -> (usize, bool) {
    debug_assert!(min <= default && default <= max);
    let raw = raw.map(str::trim).filter(|value| !value.is_empty());
    match raw.and_then(|value| value.parse::<usize>().ok()) {
        Some(value) => (value.clamp(min, max), (min..=max).contains(&value)),
        None => (default, raw.is_none()),
    }
}

/// Read an optional string. Missing and blank values are both absent.
pub fn optional_string(name: &str) -> Option<String> {
    match nonempty_var(name) {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(%error, setting = name, "unreadable string environment setting; treating as unset");
            None
        }
    }
}

/// Read an optional string whose unreadable value must fail startup. Missing
/// and blank values are both absent.
pub fn required_optional_string(name: &str) -> Result<Option<String>> {
    nonempty_var(name).with_context(|| format!("reading {name}"))
}

/// Read a string with a default while preserving strict startup failure for an
/// unreadable supplied value.
pub fn required_string_or(name: &str, default: &str) -> Result<String> {
    Ok(required_optional_string(name)?.unwrap_or_else(|| default.to_owned()))
}

/// Read a string with a warning-and-default policy. Missing and blank values
/// both use `default`, matching the Helm convention that an unset value may be
/// rendered as an empty string.
pub fn string_or(name: &str, default: &str) -> String {
    match nonempty_var(name) {
        Ok(Some(value)) => value,
        Ok(None) => default.to_owned(),
        Err(error) => {
            tracing::warn!(%error, setting = name, default, "unreadable string environment setting; using default");
            default.to_owned()
        }
    }
}

/// Read a strict boolean. Missing or blank values use `default`; malformed
/// values fail startup instead of silently selecting a potentially unsafe mode.
pub fn required_bool(name: &str, default: bool) -> Result<bool> {
    match nonempty_var(name) {
        Ok(Some(raw)) => parse_bool(name, &raw),
        Ok(None) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("reading {name}")),
    }
}

/// Read a boolean with a warning-and-default policy for optional tuning knobs.
pub fn bool_or(name: &str, default: bool) -> bool {
    match nonempty_var(name) {
        Ok(Some(raw)) => match parse_bool(name, &raw) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(value = %raw, default, %error, "invalid boolean environment setting; using default");
                default
            }
        },
        Ok(None) => default,
        Err(error) => {
            tracing::warn!(default, %error, setting = name, "unreadable boolean environment setting; using default");
            default
        }
    }
}

/// Read a bounded integer, clamping out-of-range values and falling back to
/// `default` for malformed values. Any supplied value that changes is logged.
pub fn bounded_usize(name: &str, default: usize, min: usize, max: usize) -> usize {
    let raw = match nonempty_var(name) {
        Ok(raw) => raw,
        Err(error) => {
            tracing::warn!(default, %error, setting = name, "unreadable integer environment setting; using default");
            return default;
        }
    };
    let (value, valid) = bounded_value(raw.as_deref(), default, min, max);
    if !valid {
        tracing::warn!(
            setting = name,
            configured = %raw.as_deref().unwrap_or_default(),
            value,
            min,
            max,
            "invalid or out-of-range integer environment setting; using bounded value"
        );
    }
    value
}

/// Read a bounded integer whose invalid values must fail startup.
pub fn required_bounded_usize(name: &str, default: usize, min: usize, max: usize) -> Result<usize> {
    let raw = match nonempty_var(name) {
        Ok(Some(raw)) => raw,
        Ok(None) => return Ok(default),
        Err(error) => return Err(error).with_context(|| format!("reading {name}")),
    };
    let value = parse_usize(name, &raw)?;
    if !(min..=max).contains(&value) {
        bail!("{name} must be between {min} and {max}; got {raw:?}");
    }
    Ok(value)
}

/// Read a strict cache budget expressed in MiB. Zero remains valid; malformed
/// or overflowing values fail startup.
pub fn required_mebibytes(name: &str, default: usize) -> Result<usize> {
    Ok(required_bounded_usize(name, default, 0, usize::MAX / MEBIBYTE)? * MEBIBYTE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn booleans_accept_the_shared_spelling_set() {
        assert!(parse_bool("FLAG", " YES ").unwrap());
        assert!(!parse_bool("FLAG", "off").unwrap());
        assert!(parse_bool("FLAG", "maybe").is_err());
    }

    #[test]
    fn blank_environment_values_are_unset() {
        assert_eq!(nonempty(String::new()), None);
        assert_eq!(nonempty(" \t ".to_string()), None);
        assert_eq!(nonempty(" false ".to_string()), Some(" false ".to_string()));
    }

    #[test]
    fn bounded_values_default_or_clamp_consistently() {
        assert_eq!(bounded_value(None, 4, 1, 16), (4, true));
        assert_eq!(bounded_value(Some("   "), 4, 1, 16), (4, true));
        assert_eq!(bounded_value(Some(" 8 "), 4, 1, 16), (8, true));
        assert_eq!(bounded_value(Some("0"), 4, 1, 16), (1, false));
        assert_eq!(bounded_value(Some("99"), 4, 1, 16), (16, false));
        assert_eq!(bounded_value(Some("invalid"), 4, 1, 16), (4, false));
        assert_eq!(bounded_value(Some("0"), 4, 0, 16), (0, true));
    }

    #[test]
    fn retired_names_are_rejected_even_when_blank_or_masked() {
        let retired = [("MKDB2_LIMIT", "KYMO_LIMIT")];
        let error = reject_retired_with(&retired, |name| name == "MKDB2_LIMIT").unwrap_err();
        assert_eq!(
            error.to_string(),
            "MKDB2_LIMIT is no longer supported; use KYMO_LIMIT"
        );
        assert!(reject_retired_with(&retired, |_| false).is_ok());
    }
}
