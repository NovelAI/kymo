import argparse
import time

import grpc
import httpx


MAX_MESSAGE_BYTES = 64 * 1024


def _varint(value: int) -> bytes:
    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def _payload(data: bytes) -> bytes:
    return b"\x0a" + _varint(len(data)) + data


def _parse_payload(message: bytes) -> bytes:
    if not message or message[0] != 0x0A:
        raise AssertionError("unexpected protobuf payload tag")
    length = 0
    shift = 0
    offset = 1
    while True:
        byte = message[offset]
        offset += 1
        length |= (byte & 0x7F) << shift
        if byte < 0x80:
            break
        shift += 7
    data = message[offset : offset + length]
    if len(data) != length or offset + length != len(message):
        raise AssertionError("invalid protobuf payload length")
    return data


def qualify_grpc(socket: str) -> None:
    channel = grpc.insecure_channel(
        f"unix://{socket}", options=(("grpc.default_authority", "localhost"),)
    )
    grpc.channel_ready_future(channel).result(timeout=5)
    unary = channel.unary_unary(
        "/kymo.transport.v1.Probe/Echo",
        request_serializer=lambda value: value,
        response_deserializer=lambda value: value,
    )
    sent = b"python-grpc-uds" * 1024
    if _parse_payload(unary(_payload(sent), timeout=5)) != sent:
        raise AssertionError("Python gRPC Unix-socket unary echo changed payload")

    bidi = channel.stream_stream(
        "/kymo.transport.v1.Probe/Bidi",
        request_serializer=lambda value: value,
        response_deserializer=lambda value: value,
    )
    expected = [b"one", b"two", b"three"]
    received = [
        _parse_payload(value)
        for value in bidi((_payload(value) for value in expected), timeout=5)
    ]
    if received != expected:
        raise AssertionError("Python gRPC Unix-socket bidi echo changed payloads")

    try:
        unary(_payload(b"x" * (MAX_MESSAGE_BYTES + 1)), timeout=5)
    except grpc.RpcError as error:
        if error.code() not in {
            grpc.StatusCode.OUT_OF_RANGE,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
        }:
            raise
    else:
        raise AssertionError("tonic accepted an oversized Python gRPC message")

    slow = channel.unary_unary(
        "/kymo.transport.v1.Probe/Slow",
        request_serializer=lambda value: value,
        response_deserializer=lambda value: value,
    ).future(_payload(b"cancel"), timeout=10)
    time.sleep(0.2)
    if not slow.cancel():
        raise AssertionError("Python gRPC future could not be cancelled")
    channel.close()


def qualify_http(socket: str) -> None:
    transport = httpx.HTTPTransport(uds=socket)
    with httpx.Client(transport=transport, timeout=5) as client:
        sent = b"python-httpx-uds" * 1024
        response = client.post("http://localhost/echo", content=sent)
        response.raise_for_status()
        if response.content != sent:
            raise AssertionError("httpx Unix-socket echo changed payload")

        with client.stream("GET", "http://localhost/stream") as response:
            response.raise_for_status()
            if b"".join(response.iter_bytes()) != b"stream-works":
                raise AssertionError(
                    "httpx Unix-socket streaming response changed payload"
                )

        response = client.post(
            "http://localhost/echo", content=b"x" * (MAX_MESSAGE_BYTES + 1)
        )
        if response.status_code != 413:
            raise AssertionError(
                f"Axum returned {response.status_code} for an oversized body"
            )

    with httpx.Client(transport=httpx.HTTPTransport(uds=socket), timeout=0.1) as client:
        try:
            client.get("http://localhost/slow")
        except httpx.ReadTimeout:
            pass
        else:
            raise AssertionError("httpx cancellation timeout did not fire")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--grpc-socket", required=True)
    parser.add_argument("--http-socket", required=True)
    args = parser.parse_args()
    qualify_grpc(args.grpc_socket)
    qualify_http(args.http_socket)
    print(
        "PASS Python grpcio/httpx Unix-socket unary, bidi, streaming, limits, and cancellation"
    )


if __name__ == "__main__":
    main()
