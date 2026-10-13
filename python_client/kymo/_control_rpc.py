"""Private clean-process boundary for synchronous lifecycle RPCs."""

import base64
import json
import math
import os
import sys
import time
from pathlib import Path

import grpc

from kymo._generated import kymo_pb2, kymo_pb2_grpc


_METHODS = {
    "InitRun": kymo_pb2.InitRunRequest,
    "TerminateRun": kymo_pb2.TerminateRunRequest,
}

# _run_control_rpc starts `python -S -c BOOTSTRAP <timeout>` with group signals blocked for TerminateRun. Set SIGALRM to its default action, unblock it and arm it for ceil(timeout)+2 s before site.main() and kymo/grpc imports. This bounds site, import and RPC hangs (NFS, .pth files, sitecustomize). -S defers site processing; -m would import kymo/grpc before the alarm. Ignored dispositions and blocked masks survive exec.
BOOTSTRAP = "import math, signal, sys; signal.signal(signal.SIGALRM, signal.SIG_DFL); signal.pthread_sigmask(signal.SIG_UNBLOCK, [signal.SIGALRM]); signal.alarm(math.ceil(float(sys.argv[1])) + 2); import site; site.main(); from kymo._control_rpc import main; sys.exit(main())"


def _ended(pid: int) -> bool:
    """Whether the process pid has ended: it no longer exists, or, on Linux, it is a zombie (state Z in /proc/<pid>/stat), as it is until its parent reaps it, and for good once that parent has exited under a PID 1 that never reaps. Off Linux a zombie reads as running, and anywhere so does a process that has since taken the pid: either holds the wait for it (after, in _dispatch) until the alarm. The client reads the upload worker its forkserver's death orphaned the same way (kymo.client._worker_alive)."""
    try:
        os.kill(pid, 0)
    except OSError:
        return True
    try:
        stat = Path(f"/proc/{pid}/stat").read_bytes()
    except OSError:
        return False
    # The state follows the name, which may itself hold spaces and parentheses.
    return stat.rpartition(b")")[2].split()[:1] == [b"Z"]


def _dispatch(payload: dict) -> bytes:
    if not isinstance(payload, dict):
        raise TypeError("control request must be an object")
    method = payload.get("method")
    if method not in _METHODS:
        raise ValueError("unsupported control RPC")
    server_address = payload.get("server_address")
    if not isinstance(server_address, str) or not server_address:
        raise ValueError("server_address must be non-empty")
    timeout = payload.get("timeout")
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or timeout <= 0
    ):
        raise ValueError("timeout must be a positive number")
    after = payload.get("after")
    if after is not None and (isinstance(after, bool) or not isinstance(after, int)):
        raise ValueError("after must be a process id")
    encoded_request = payload.get("request")
    if not isinstance(encoded_request, str):
        raise ValueError("request must be base64 text")
    try:
        request_bytes = base64.b64decode(encoded_request, validate=True)
        request = _METHODS[method].FromString(request_bytes)
    except Exception as error:
        raise ValueError("request is not a valid protobuf") from error

    local_config = payload.get("local_endpoint")
    if local_config is None:
        channel = grpc.insecure_channel(server_address)
    else:
        from kymo._local_runtime import endpoint_from_worker_config, grpc_channel

        endpoint = endpoint_from_worker_config(local_config)
        channel = grpc_channel(endpoint)

    if after is not None:
        # after is the pid of a control process an earlier end of the run left running: send once it has ended (_ended), within the same deadline, so the server takes this code last (kymo.client._terminate_run). The alarm bounds the wait.
        started = time.monotonic()
        while not _ended(after):
            time.sleep(0.01)
        timeout = max(0.001, timeout - (time.monotonic() - started))
    with channel:
        stub = kymo_pb2_grpc.KymoStub(channel)
        response = getattr(stub, method)(request, timeout=float(timeout))
    return response.SerializeToString()


def main() -> int:
    try:
        payload = json.loads(sys.stdin.buffer.read())
        response = _dispatch(payload)
    except BaseException as error:
        if isinstance(error, grpc.RpcError):
            detail = error.details() or str(error)
            message = f"{error.code().name}: {detail}"
        else:
            message = f"{type(error).__name__}: {error}"
        print(message, file=sys.stderr)
        return 1
    sys.stdout.buffer.write(response)
    return 0
