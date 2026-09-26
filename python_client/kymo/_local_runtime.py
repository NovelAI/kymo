"""Validated launcher handoff and private local transports."""

import json
import logging
import math
import os
import shutil
import subprocess
import sysconfig
import uuid
from collections import namedtuple
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit

import grpc

from kymo._env import number as _env_number

_log = logging.getLogger("kymo")
_PROTOCOL_VERSION = 2
_DEFAULT_ENSURE_TIMEOUT = 600.0
_REQUIRED_FIELDS = {
    "protocol_min",
    "protocol_max",
    "installation_uuid",
    "endpoint_generation",
    "native_socket",
    "upload_socket",
    "dashboard_origin",
    "cdn_origin",
    "server_bearer",
}
_FINAL_TOKEN_CHARS = frozenset("AEIMQUYcgkosw048")


class LocalInstallationMismatch(RuntimeError):
    """Queued work belongs to a different durable local installation."""


def _launcher_path() -> str:
    # The launcher installed beside this interpreter comes first, so an unactivated venv (or a notebook kernel started from one) finds it.
    scripts = sysconfig.get_path("scripts")
    launcher = shutil.which("kymo", path=scripts) or shutil.which("kymo")
    if launcher is None:
        raise RuntimeError(
            "the local kymo runtime launcher is not installed; install it with "
            "`pip install \"kymo[local]\"` before using mode='local'"
        )
    # subprocess requires a path component to select posix_spawn.
    return os.path.abspath(launcher)


@dataclass(frozen=True)
class LocalEndpoint:
    protocol_min: int
    protocol_max: int
    installation_uuid: str
    endpoint_generation: str
    native_socket: str
    upload_socket: str
    dashboard_origin: str
    cdn_origin: str
    server_bearer: str

    @property
    def grpc_target(self) -> str:
        return f"unix://{self.native_socket}"

    @property
    def upload_origin(self) -> str:
        # HTTP still needs an authority on a Unix-domain transport. The local
        # upload listener ignores it and admits only the bearer on its 0600 UDS.
        return "http://localhost"

    def worker_config(self) -> dict:
        return asdict(self)


class _ClientCallDetails(
    namedtuple(
        "_ClientCallDetailsBase",
        (
            "method",
            "timeout",
            "metadata",
            "credentials",
            "wait_for_ready",
            "compression",
        ),
    ),
    grpc.ClientCallDetails,
):
    pass


class _BearerInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    def __init__(self, token: str):
        self._authorization = ("authorization", f"Bearer {token}")

    def _details(self, details):
        metadata = list(details.metadata or ())
        if any(str(key).lower() == "authorization" for key, _ in metadata):
            raise RuntimeError(
                "local gRPC call already contains authorization metadata"
            )
        metadata.append(self._authorization)
        return _ClientCallDetails(
            details.method,
            details.timeout,
            metadata,
            details.credentials,
            getattr(details, "wait_for_ready", None),
            getattr(details, "compression", None),
        )

    def intercept_unary_unary(self, continuation, client_call_details, request):
        return continuation(self._details(client_call_details), request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        return continuation(self._details(client_call_details), request)

    def intercept_stream_unary(
        self, continuation, client_call_details, request_iterator
    ):
        return continuation(self._details(client_call_details), request_iterator)

    def intercept_stream_stream(
        self, continuation, client_call_details, request_iterator
    ):
        return continuation(self._details(client_call_details), request_iterator)


def ensure_local_endpoint(
    *,
    expected_installation_uuid: Optional[str] = None,
    timeout: Optional[float] = None,
    init_hold_id: Optional[str] = None,
) -> LocalEndpoint:
    if expected_installation_uuid is not None:
        expected_installation_uuid = _uuid(
            expected_installation_uuid, "expected_installation_uuid"
        )
    if init_hold_id is not None:
        init_hold_id = _uuid(init_hold_id, "init_hold_id")
    launcher = _launcher_path()
    if timeout is None:
        timeout = _ensure_timeout()
    command = [launcher, "ensure", "--json"]
    if init_hold_id is not None:
        command.extend(("--init-hold-id", init_hold_id))
    try:
        completed = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            text=True,
            timeout=timeout,
            check=False,
            # Worker recovery may run after gRPC has started helper threads.
            # Force posix_spawn rather than fork-before-exec on POSIX.
            close_fds=False,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(
            f"local kymo runtime did not become ready within {timeout:g}s"
        ) from error
    if completed.returncode != 0:
        raise RuntimeError(
            "local kymo runtime failed to start; run `kymo doctor` for details"
        )
    try:
        payload = json.loads(completed.stdout)
        endpoint = _validate_endpoint(payload)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise RuntimeError(
            "kymo ensure returned an invalid endpoint contract"
        ) from error
    if (
        expected_installation_uuid is not None
        and endpoint.installation_uuid != expected_installation_uuid
    ):
        raise LocalInstallationMismatch(
            "local kymo installation identity changed; refusing to deliver queued run data"
        )
    return endpoint


def open_local_run(
    project_id: str,
    run_id: str,
    *,
    expected_installation_uuid: str,
    timeout: float = 600.0,
) -> str:
    """Wake the local stack, open its dashboard, and return the stable, non-secret URL.

    Where no browser can be launched (a headless VPN server), the launcher
    still succeeds and the returned URL is the thing to forward and open.
    """
    launcher = _launcher_path()
    command = [
        launcher,
        "open",
        "--expected-installation-uuid",
        _uuid(expected_installation_uuid, "expected_installation_uuid"),
        "--",
        project_id,
        run_id,
    ]
    try:
        completed = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            check=False,
            # init/open may run after gRPC helper threads exist.
            close_fds=False,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(
            f"local kymo dashboard did not open within {timeout:g}s"
        ) from error
    if completed.returncode != 0:
        detail = completed.stderr.strip()
        if len(detail) > 1000:
            detail = detail[-1000:]
        raise RuntimeError(
            f"failed to open the local kymo dashboard: {detail or 'unknown error'}"
        )
    if completed.stderr.strip():
        _log.warning("%s", completed.stderr.strip()[-1000:])
    return completed.stdout.strip()


def endpoint_from_worker_config(config: dict) -> LocalEndpoint:
    try:
        return _validate_endpoint(config)
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("invalid local endpoint passed to upload worker") from error


def grpc_channel(endpoint: LocalEndpoint):
    # grpcio otherwise derives an empty/UDS-shaped :authority on macOS and
    # tonic rejects the HTTP/2 stream. This exact convention is qualified in
    # tools/local-transport-qualification/qualify_python_uds.py.
    base = grpc.insecure_channel(
        endpoint.grpc_target,
        options=(("grpc.default_authority", "localhost"),),
    )
    return grpc.intercept_channel(base, _BearerInterceptor(endpoint.server_bearer))


def http_client(endpoint: LocalEndpoint, *, timeout: float = 60.0):
    import httpx

    return httpx.Client(
        transport=httpx.HTTPTransport(uds=endpoint.upload_socket),
        headers={"Authorization": f"Bearer {endpoint.server_bearer}"},
        timeout=timeout,
    )


def _ensure_timeout() -> float:
    timeout = _env_number(
        "KYMO_LOCAL_ENSURE_TIMEOUT",
        "MKDB2_LOCAL_ENSURE_TIMEOUT",
        _DEFAULT_ENSURE_TIMEOUT,
    )
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("KYMO_LOCAL_ENSURE_TIMEOUT must be a positive number")
    return timeout


def _validate_endpoint(payload) -> LocalEndpoint:
    if not isinstance(payload, dict) or not _REQUIRED_FIELDS.issubset(payload):
        raise TypeError("endpoint must contain every required field")
    protocol_min = _plain_int(payload["protocol_min"], "protocol_min")
    protocol_max = _plain_int(payload["protocol_max"], "protocol_max")
    if not protocol_min <= _PROTOCOL_VERSION <= protocol_max:
        raise ValueError("local runtime protocol is incompatible")
    installation_uuid = _uuid(payload["installation_uuid"], "installation_uuid")
    endpoint_generation = _uuid(payload["endpoint_generation"], "endpoint_generation")
    native_socket = _absolute_path(payload["native_socket"], "native_socket")
    upload_socket = _absolute_path(payload["upload_socket"], "upload_socket")
    if native_socket == upload_socket:
        raise ValueError("local runtime sockets must be distinct")
    dashboard_origin = _loopback_origin(payload["dashboard_origin"], "dashboard_origin")
    cdn_origin = _loopback_origin(payload["cdn_origin"], "cdn_origin")
    if dashboard_origin == cdn_origin:
        raise ValueError("local browser origins must be distinct")
    server_bearer = payload["server_bearer"]
    if not _valid_token(server_bearer):
        raise ValueError("server_bearer is not canonical base64url")
    return LocalEndpoint(
        protocol_min=protocol_min,
        protocol_max=protocol_max,
        installation_uuid=installation_uuid,
        endpoint_generation=endpoint_generation,
        native_socket=native_socket,
        upload_socket=upload_socket,
        dashboard_origin=dashboard_origin,
        cdn_origin=cdn_origin,
        server_bearer=server_bearer,
    )


def _plain_int(value, name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    return value


def _uuid(value, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a UUID string")
    return str(uuid.UUID(value))


def _absolute_path(value, name: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise TypeError(f"{name} must be a non-empty path")
    path = Path(value)
    if not path.is_absolute():
        raise ValueError(f"{name} must be absolute")
    return str(path)


def _loopback_origin(value, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "http"
        or parsed.hostname != "127.0.0.1"
        or parsed.port is None
        or parsed.path
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError(f"{name} must be an exact numeric-loopback HTTP origin")
    return f"http://127.0.0.1:{parsed.port}"


def _valid_token(value) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 43
        and all(
            character.isascii() and (character.isalnum() or character in "-_")
            for character in value
        )
        and value[-1] in _FINAL_TOKEN_CHARS
    )
