"""Golden wire vectors for the deployed WebSocket envelope, independent of protobuf."""

import unittest
import zlib

from fences_common import encode_response, request_frame


class FramingTests(unittest.TestCase):
    def test_request_uses_little_endian_id_and_utf8_byte_length(self):
        self.assertEqual(
            request_frame(b"\x04\x03\x02\x01\x13\x00/kymo.Kymo/ListRuns\x0a\x01p"),
            (0x01020304, "/kymo.Kymo/ListRuns", b"\x0a\x01p"),
        )
        self.assertEqual(
            request_frame(b"\x01\x00\x00\x00\x02\x00\xc3\xa9x"), (1, "é", b"x")
        )

    def test_grpc_ws2_length_bit_marks_a_zlib_body(self):
        body = b"\x0a\x01p" * 400
        stream = zlib.compress(body)
        flagged = b"\x04\x03\x02\x01\x13\x80/kymo.Kymo/ListRuns" + stream
        self.assertEqual(
            request_frame(flagged, "/grpc-ws2"),
            (0x01020304, "/kymo.Kymo/ListRuns", body),
        )
        # Exactly one complete stream, as the server requires.
        for bad in (b"not zlib", stream + b"\0", stream[:-1], b""):
            with self.subTest(body=bad[:16]), self.assertRaises(ValueError):
                request_frame(b"\x01\x00\x00\x00\x01\x80x" + bad, "/grpc-ws2")
        # Elsewhere the bit is part of the path length, as the server reads it: this path would run past the frame.
        for route in ("/grpc-ws", "/trash/_kymo-grpc-ws-local-v1"):
            with self.subTest(route=route), self.assertRaises(ValueError):
                request_frame(flagged, route)
        # So without a route, which reads as /grpc-ws.
        with self.assertRaises(ValueError):
            request_frame(flagged)

    def test_response_error_and_push_keep_their_wire_headers(self):
        vectors = [
            (0x01020304, 0, b"\x10\x00", b"\x04\x03\x02\x01\x00\x10\x00"),
            (7, 5, b"missing", b"\x07\x00\x00\x00\x05missing"),
            (0, 0, b"event", b"\x00\x00\x00\x00\x00event"),
        ]
        for request_id, code, body, wire in vectors:
            with self.subTest(request_id=request_id, code=code):
                self.assertEqual(encode_response(request_id, body, code=code), wire)

    def test_malformed_headers_fail_before_protobuf_decoding(self):
        for wire in (b"", b"\0" * 5, b"\0" * 6, b"\0\0\0\0\x02\0x", "text"):
            with self.subTest(request=wire), self.assertRaises(ValueError):
                request_frame(wire)
        with self.assertRaises(UnicodeDecodeError):
            request_frame(b"\0\0\0\0\x01\0\xff")


if __name__ == "__main__":
    unittest.main()
