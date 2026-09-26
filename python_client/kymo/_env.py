"""Canonical kymo client environment with loud retired-name rejection."""

import os
from typing import Callable, Optional, TypeVar

T = TypeVar("T")


class RetiredEnvError(ValueError):
    """A retired MKDB2_*/PYMKDB2_* name is set.

    Raised while `import kymo` runs, so best-effort callers cannot import this
    class; they re-raise any exception whose `retired_env` attribute is true.
    """

    retired_env = True


LEGACY_CLIENT_ENV = {
    "MKDB2_MODE": "KYMO_MODE",
    "MKDB2_SERVER": "KYMO_SERVER",
    "MKDB2_URL_BASE": "KYMO_URL_BASE",
    "MKDB2_SPOOL_DIR": "KYMO_SPOOL_DIR",
    "MKDB2_FLUSH_TIMEOUT": "KYMO_FLUSH_TIMEOUT",
    "MKDB2_SIGNAL_FLUSH_TIMEOUT": "KYMO_SIGNAL_FLUSH_TIMEOUT",
    "MKDB2_MAX_BUFFER_POINTS": "KYMO_MAX_BUFFER_POINTS",
    "MKDB2_MAX_BUFFER_BYTES": "KYMO_MAX_BUFFER_BYTES",
    "MKDB2_MAX_RICH_BUFFER_BYTES": "KYMO_MAX_RICH_BUFFER_BYTES",
    "MKDB2_ACK_PROGRESS_TIMEOUT": "KYMO_ACK_PROGRESS_TIMEOUT",
    "MKDB2_LOCAL_ENSURE_TIMEOUT": "KYMO_LOCAL_ENSURE_TIMEOUT",
    "PYMKDB2_VERBOSE": "KYMO_VERBOSE",
}


def reject_legacy_client_env() -> None:
    present = [name for name in LEGACY_CLIENT_ENV if name in os.environ]
    if not present:
        return
    replacements = ", ".join(
        f"{name} (use {LEGACY_CLIENT_ENV[name]})" for name in present
    )
    raise RetiredEnvError(
        "legacy kymo client environment variables are no longer supported: "
        + replacements
    )


def resolve(
    canonical: str,
    legacy: str,
    *,
    default: T,
    parse: Callable[[str], T],
) -> T:
    if legacy in os.environ:
        raise RetiredEnvError(f"{legacy} is no longer supported; use {canonical}")
    canonical_raw = os.environ.get(canonical)
    if canonical_raw is not None and not canonical_raw.strip():
        canonical_raw = None
    canonical_value = parse(canonical_raw) if canonical_raw is not None else None
    if canonical_value is not None:
        return canonical_value
    return default


def string(canonical: str, legacy: str, default: str = "") -> str:
    return resolve(canonical, legacy, default=default, parse=lambda value: value)


def optional_string(canonical: str, legacy: str) -> Optional[str]:
    value = string(canonical, legacy)
    return value or None


def integer(canonical: str, legacy: str, default: int) -> int:
    return resolve(canonical, legacy, default=default, parse=int)


def number(canonical: str, legacy: str, default: float) -> float:
    return resolve(canonical, legacy, default=default, parse=float)


def boolean(
    canonical: str, legacy: str, default: bool = False, *, strict: bool = True
) -> bool:
    if not strict:
        if legacy in os.environ:
            raise RetiredEnvError(f"{legacy} is no longer supported; use {canonical}")
        raw = os.environ.get(canonical)
        if raw is not None and raw.strip():
            return raw.strip().lower() in ("1", "true", "yes", "on")
        return default

    def parse(raw: str) -> bool:
        value = raw.strip().lower()
        if value in ("1", "true", "yes", "on"):
            return True
        if value in ("0", "false", "no", "off"):
            return False
        raise ValueError(
            f"{canonical}/{legacy} must be one of 1/true/yes/on or 0/false/no/off"
        )

    return resolve(canonical, legacy, default=default, parse=parse)


def validate_worker_settings() -> None:
    """Fail in the owner before init creates subprocesses or durable state."""
    integer("KYMO_MAX_BUFFER_POINTS", "MKDB2_MAX_BUFFER_POINTS", 0)
    integer("KYMO_MAX_BUFFER_BYTES", "MKDB2_MAX_BUFFER_BYTES", 0)
    integer("KYMO_MAX_RICH_BUFFER_BYTES", "MKDB2_MAX_RICH_BUFFER_BYTES", 0)
    timeout = number("KYMO_ACK_PROGRESS_TIMEOUT", "MKDB2_ACK_PROGRESS_TIMEOUT", 1.0)
    if timeout <= 0:
        raise ValueError("KYMO_ACK_PROGRESS_TIMEOUT must be positive")


def validate_client_settings() -> None:
    """Validate client settings before init changes process or durable state."""
    reject_legacy_client_env()
    string("KYMO_MODE", "MKDB2_MODE", "hosted")
    string("KYMO_SERVER", "MKDB2_SERVER")
    string("KYMO_URL_BASE", "MKDB2_URL_BASE")
    string("KYMO_SPOOL_DIR", "MKDB2_SPOOL_DIR")
    for canonical, legacy, default in (
        ("KYMO_FLUSH_TIMEOUT", "MKDB2_FLUSH_TIMEOUT", 60.0),
        ("KYMO_SIGNAL_FLUSH_TIMEOUT", "MKDB2_SIGNAL_FLUSH_TIMEOUT", 10.0),
        ("KYMO_LOCAL_ENSURE_TIMEOUT", "MKDB2_LOCAL_ENSURE_TIMEOUT", 600.0),
    ):
        if number(canonical, legacy, default) <= 0:
            raise ValueError(f"{canonical} must be positive")
    validate_worker_settings()
