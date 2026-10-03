#!/usr/bin/env python3
"""Check that a PostgreSQL archive from build.sh installs and needs nothing from the host but the C library.

Usage: check.py <archive> <linux-x86_64|macos-arm64>

The archive holds one root directory of files, directories and symlinks with relative targets, as the launcher's extraction requires. Linux: every ELF file links only glibc's libraries or the tree's own, finds the latter through $ORIGIN, and needs no symbol newer than glibc 2.28 (the wheel's manylinux tag). macOS: every Mach-O file links only libSystem or the tree's own libraries through @loader_path, targets macOS 11.0 or older (the wheel's tag), and carries a valid signature.
"""

import re
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

REQUIRED = ["bin/postgres", "bin/initdb", "bin/pg_ctl", "bin/psql", "bin/createdb"]
GLIBC_LIBRARIES = {"libc.so.6", "libm.so.6", "libdl.so.2", "libpthread.so.0", "librt.so.1", "ld-linux-x86-64.so.2"}
# The library search path build.sh sets, which reaches the tree's lib/ from bin/ and from lib/.
RUNPATH = "$ORIGIN/../lib"
GLIBC_FLOOR = (2, 28)
MACOS_FLOOR = (11, 0)


def output(*command: str) -> str:
    return subprocess.run(command, check=True, capture_output=True, text=True).stdout


def binaries(tree: Path, magic: tuple[bytes, ...]) -> list[Path]:
    found = []
    for path in sorted(tree.rglob("*")):
        if path.is_file() and not path.is_symlink():
            with path.open("rb") as file:
                if file.read(4) in magic:
                    found.append(path)
    return found


def check_linux(tree: Path, own: set[str]) -> list[str]:
    failures = []
    for path in binaries(tree, (b"\x7fELF",)):
        dynamic = output("readelf", "--dynamic", "--wide", str(path))
        for needed in re.findall(r"\(NEEDED\).*\[(.+?)\]", dynamic):
            if needed not in GLIBC_LIBRARIES | own:
                failures.append(f"{path}: needs host library {needed}")
        for search in re.findall(r"\((?:RUNPATH|RPATH)\).*\[(.+?)\]", dynamic):
            if search != RUNPATH:
                failures.append(f"{path}: searches {search}")
        symbols = output("readelf", "--dyn-syms", "--wide", str(path))
        for version in {tuple(map(int, match)) for match in re.findall(r"@GLIBC_(\d+)\.(\d+)", symbols)}:
            if version > GLIBC_FLOOR:
                failures.append(f"{path}: needs GLIBC_{version[0]}.{version[1]}")
    return failures


def check_macos(tree: Path, own: set[str]) -> list[str]:
    failures = []
    # Every Mach-O and universal header, as the launcher's signature check recognizes them.
    magic = (
        b"\xfe\xed\xfa\xce",
        b"\xce\xfa\xed\xfe",
        b"\xfe\xed\xfa\xcf",
        b"\xcf\xfa\xed\xfe",
        b"\xca\xfe\xba\xbe",
        b"\xbe\xba\xfe\xca",
    )
    allowed = {"/usr/lib/libSystem.B.dylib"} | {f"@loader_path/../lib/{name}" for name in own}
    for path in binaries(tree, magic):
        for dependency in output("otool", "-L", str(path)).splitlines()[1:]:
            name = dependency.split()[0]
            if name not in allowed:
                failures.append(f"{path}: links {name}")
        for version in re.findall(r"\bminos (\d+)\.(\d+)", output("otool", "-l", str(path))):
            if tuple(map(int, version)) > MACOS_FLOOR:
                failures.append(f"{path}: requires macOS {version[0]}.{version[1]}")
        signature = subprocess.run(["codesign", "--verify", "--strict", str(path)], capture_output=True, text=True)
        if signature.returncode != 0:
            failures.append(f"{path}: invalid signature: {signature.stderr.strip()}")
    return failures


def check_archive(archive: Path, target: str) -> list[str]:
    with tarfile.open(archive) as bundle:
        members = bundle.getmembers()
        roots = {member.name.split("/")[0] for member in members}
        if len(roots) != 1 or roots & {"", ".", ".."}:
            return [f"{archive}: root entries {sorted(roots)}"]
        failures = [
            f"{archive}: {member.name} is not a file, directory or symlink"
            for member in members
            if not (member.isfile() or member.isdir() or member.issym())
        ]
        failures += [
            f"{archive}: {member.name} links to {member.linkname}"
            for member in members
            if member.issym() and any(part in ("", ".", "..") for part in member.linkname.split("/"))
        ]
        with tempfile.TemporaryDirectory() as extracted:
            bundle.extractall(extracted, filter="tar")
            tree = Path(extracted, roots.pop())
            failures += [f"missing {name}" for name in REQUIRED if not tree.joinpath(name).is_file()]
            own = {path.name for path in tree.glob("lib/*")}
            failures += {"linux-x86_64": check_linux, "macos-arm64": check_macos}[target](tree, own)
    return failures


def main() -> None:
    archive, target = Path(sys.argv[1]), sys.argv[2]
    if failures := check_archive(archive, target):
        sys.exit("\n".join(failures))
    print(f"{archive}: installs and needs only the C library")


if __name__ == "__main__":
    main()
