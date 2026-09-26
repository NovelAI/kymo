#!/usr/bin/env python3
"""Regenerate the checked-in Python stubs from the canonical kymo.proto."""

import argparse
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
from importlib import metadata
from pathlib import Path

PROTO_DIR = Path(__file__).parent.parent / "proto"
OUT_DIR = Path(__file__).parent / "kymo" / "_generated"
GENERATOR_VERSION = "1.81.0"
GENERATED_FILES = ("kymo_pb2.py", "kymo_pb2_grpc.py")


def _repair_command() -> str:
    return shlex.join([sys.executable, str(Path(__file__).resolve())])


def _check_toolchain() -> bool:
    try:
        installed = metadata.version("grpcio-tools")
    except metadata.PackageNotFoundError:
        installed = None
    if installed != GENERATOR_VERSION:
        print(
            f"generate_proto.py requires grpcio-tools=={GENERATOR_VERSION}; "
            f"found {installed or 'nothing'}",
            file=sys.stderr,
        )
        return False
    return True


def _generate(out_dir: Path) -> int:
    proto_file = PROTO_DIR / "kymo.proto"
    if not proto_file.exists():
        print(f"Proto file not found: {proto_file}", file=sys.stderr)
        return 1

    out_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"--proto_path={PROTO_DIR}",
        f"--python_out={out_dir}",
        f"--grpc_python_out={out_dir}",
        str(proto_file),
    ]

    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        print("Proto generation failed", file=sys.stderr)
        return 1

    # protoc sees kymo.proto at the proto-path root, so its Python plugin emits
    # a top-level import. The checked stubs live in kymo._generated and need a
    # package-relative import. Fail loudly if a future generator changes the
    # expected line instead of silently producing a wheel that cannot import.
    grpc_file = out_dir / "kymo_pb2_grpc.py"
    absolute_import = "import kymo_pb2 as kymo__pb2"
    relative_import = "from . import kymo_pb2 as kymo__pb2"
    text = grpc_file.read_text()
    text, replacements = re.subn(
        rf"(?m)^{re.escape(absolute_import)}$", relative_import, text
    )
    if replacements != 1:
        print(
            "Generated gRPC stub did not contain exactly one expected "
            f"import line: {absolute_import!r}",
            file=sys.stderr,
        )
        return 1
    grpc_file.write_text(text)

    missing = [name for name in GENERATED_FILES if not (out_dir / name).is_file()]
    if missing:
        print(
            f"Proto generation omitted expected file(s): {', '.join(missing)}",
            file=sys.stderr,
        )
        return 1
    return 0


def _unexpected_checked_stubs() -> list[str]:
    expected = set(GENERATED_FILES)
    actual = {path.name for path in OUT_DIR.glob("*_pb2*.py")}
    return sorted(actual - expected)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify checked-in stubs without modifying the working tree",
    )
    args = parser.parse_args(argv)

    if not _check_toolchain():
        return 2

    with tempfile.TemporaryDirectory(prefix="kymo-proto-") as temp_dir:
        generated_dir = Path(temp_dir)
        result = _generate(generated_dir)
        if result != 0:
            return result

        unexpected = _unexpected_checked_stubs()
        if unexpected:
            print(
                "Unexpected checked-in generated stub(s): " + ", ".join(unexpected),
                file=sys.stderr,
            )
            return 1

        if args.check:
            stale = [
                name
                for name in GENERATED_FILES
                if not (OUT_DIR / name).is_file()
                or (OUT_DIR / name).read_bytes() != (generated_dir / name).read_bytes()
            ]
            if stale:
                print(
                    "Checked-in proto stubs are stale: " + ", ".join(stale),
                    file=sys.stderr,
                )
                print(f"Run: {_repair_command()}", file=sys.stderr)
                return 1
            print("Checked-in proto stubs are up to date")
            return 0

        OUT_DIR.mkdir(parents=True, exist_ok=True)
        (OUT_DIR / "__init__.py").touch()
        for name in GENERATED_FILES:
            shutil.copyfile(generated_dir / name, OUT_DIR / name)

    print("Proto generation complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
