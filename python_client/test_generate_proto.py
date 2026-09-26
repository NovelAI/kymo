"""Focused checks for the checked-in protobuf generator."""

import os
import shlex
import sys
import tempfile
import unittest

import generate_proto


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


if __name__ == "__main__":
    unittest.main()
