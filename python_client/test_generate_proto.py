"""Focused checks for the checked-in protobuf generator and its stubs."""

import os
import re
import shlex
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import generate_proto

CLIENT = Path(__file__).parent.resolve()


class GenerateProtoTests(unittest.TestCase):
    def test_repair_command_is_independent_of_the_invocation_directory(self):
        previous = os.getcwd()
        try:
            with tempfile.TemporaryDirectory() as directory:
                os.chdir(directory)
                argv = shlex.split(generate_proto._repair_command())
        finally:
            os.chdir(previous)

        self.assertEqual(argv[0], sys.executable)
        self.assertEqual(argv[1], os.path.realpath(generate_proto.__file__))

    def test_the_runtime_floors_are_the_stubs_own(self):
        # The stubs refuse an older protobuf or grpc at import, and CI installs the newest, so only this keeps pyproject's floors from falling behind a regeneration.
        pb2 = (generate_proto.OUT_DIR / "kymo_pb2.py").read_text()
        pb2_grpc = (generate_proto.OUT_DIR / "kymo_pb2_grpc.py").read_text()
        gencode = re.search(
            r"ValidateProtobufRuntimeVersion\(\s*[\w.]+,\s*(\d+),\s*(\d+),\s*(\d+),",
            pb2,
        )
        generated = re.search(r"^GRPC_GENERATED_VERSION = '([\d.]+)'$", pb2_grpc, re.M)
        pyproject = (CLIENT / "pyproject.toml").read_text()
        self.assertIn(f'"protobuf>={".".join(gencode.groups())}"', pyproject)
        self.assertIn(f'"grpcio>={generated[1]},<2"', pyproject)

    def test_an_older_runtime_fails_import_kymo_with_its_import_error(self):
        # Each older runtime's refusal surfaces as client.py's ImportError (see its guard); the cwd puts this checkout's kymo first on sys.path.
        for older in (
            "import google.protobuf.runtime_version as v; v.MAJOR -= 1",
            "import grpc; grpc.__version__ = '1.0.0'",
        ):
            with self.subTest(older=older):
                completed = subprocess.run(
                    [sys.executable, "-c", f"{older}\nimport kymo"],
                    cwd=CLIENT,
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertIn(
                    "ImportError: kymo's generated proto stubs are unavailable",
                    completed.stderr,
                )


if __name__ == "__main__":
    unittest.main()
