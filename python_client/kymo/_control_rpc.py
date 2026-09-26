"""Private clean-process boundary for synchronous lifecycle RPCs."""

import base64
import json
import math
import sys

import grpc

from kymo._generated import kymo_pb2, kymo_pb2_grpc


_METHODS = {
    "InitRun": (kymo_pb2.InitRunRequest, "InitRun"),
    "TerminateRun": (kymo_pb2.TerminateRunRequest, "TerminateRun"),
}


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
    encoded_request = payload.get("request")
    if not isinstance(encoded_request, str):
        raise ValueError("request must be base64 text")
    try:
        request_bytes = base64.b64decode(encoded_request, validate=True)
        request_type, stub_method = _METHODS[method]
        request = request_type.FromString(request_bytes)
    except Exception as error:
        raise ValueError("request is not a valid protobuf") from error

    local_config = payload.get("local_endpoint")
    if local_config is None:
        channel = grpc.insecure_channel(server_address)
    else:
        from kymo._local_runtime import endpoint_from_worker_config, grpc_channel

        endpoint = endpoint_from_worker_config(local_config)
        channel = grpc_channel(endpoint)

    with channel:
        stub = kymo_pb2_grpc.KymoStub(channel)
        response = getattr(stub, stub_method)(request, timeout=float(timeout))
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


if __name__ == "__main__":
    raise SystemExit(main())
