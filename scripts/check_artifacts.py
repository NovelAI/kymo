#!/usr/bin/env python3
"""Gate built distributions before upload: python scripts/check_artifacts.py DIST."""

import argparse
import re
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_TAGS = ("manylinux_2_28_x86_64", "macosx_11_0_arm64")
STRINGS = re.compile(rb"[\x20-\x7e]{6,}")
URL = re.compile(r"\b(?:https?|wss?|grpcs?)://([^\s/?#\"'<>`)\]\\,;]+)")
# A relative Markdown link, which a package description on PyPI cannot resolve.
RELATIVE_LINK = re.compile(r"\]\((?![a-z][a-z0-9+.-]*:|#)[^)\s]+\)")
DOC_PATH = re.compile(r"\bdocs/([\w.-]+\.md)\b")
HOME_PATH = re.compile(r"/(?:Users|home)/[\w.-]+(?:/[\w.-]+)?")
# CI checkouts and Cargo homes on GitHub-hosted runners; panic locations and debug info name them.
ALLOWED_HOME_PATHS = {f"/{home}/runner/{d}" for home in ("home", "Users") for d in ("work", ".cargo", ".rustup")}


def allowlist() -> set[str]:
    lines = (ROOT / "scripts/url-allowlist.txt").read_text().splitlines()
    return {line.split("#", 1)[0].strip().lower() for line in lines} - {""}


def host_allowed(host: str, hosts: set[str], binary: bool) -> bool:
    host = host.lower().rsplit("@", 1)[-1].split(":", 1)[0].strip(".")
    if not re.fullmatch(r"[a-z0-9.-]*[a-z0-9]", host):
        return True
    if host in hosts or any(pattern.startswith("*.") and host.endswith(pattern[1:]) for pattern in hosts):
        return True
    if not binary:
        return False
    # In a binary's string table, dotless "hosts" are byte noise (a scheme followed by "H9"), and a real host can run into the next string ("storage.googleapis.comno").
    return "." not in host or any(
        "." in pattern and host.startswith(pattern) and host[len(pattern)] not in ".-" for pattern in hosts if not pattern.startswith("*.")
    )


def members(path: Path) -> dict[str, bytes]:
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            return {name: archive.read(name) for name in archive.namelist() if not name.endswith("/")}
    with tarfile.open(path) as archive:
        return {m.name: archive.extractfile(m).read() for m in archive.getmembers() if m.isfile()}


def scan(name: str, data: bytes, hosts: set[str], shipped_docs: set[str]) -> list[str]:
    text = "\n".join(s.decode() for s in STRINGS.findall(data))
    # License texts and maturin's SBOM name third-party sites by design.
    urls = [] if "/licenses/" in name or name.endswith(".cyclonedx.json") else URL.finditer(text)
    binary = b"\0" in data[:8192]
    failures = [f"{name}: URL host {m.group(1)!r} is not on scripts/url-allowlist.txt" for m in urls if not host_allowed(m.group(1), hosts, binary)]
    failures += [f"{name}: cites unshipped docs/{m.group(1)}" for m in DOC_PATH.finditer(text) if m.group(1) not in shipped_docs]
    failures += [f"{name}: absolute path {m.group(0)!r}" for m in HOME_PATH.finditer(text) if m.group(0) not in ALLOWED_HOME_PATHS]
    return sorted(set(failures))


def check_distribution(path: Path, files: dict[str, bytes]) -> list[str]:
    failures = []
    name = path.name
    if name.startswith("kymo_local_runtime-"):
        version = name.split("-")[1]
        data = f"kymo_local_runtime-{version}.data/scripts/"
        exact, prefixes = {data + "kymo", data + "kymo-server"}, (f"kymo_local_runtime-{version}.dist-info/",)
        licenses = {"LICENSE", "NOTICE", "THIRD-PARTY-NOTICES"}
        tag = name.removesuffix(".whl").rsplit("-", 1)[1]
        if tag not in RUNTIME_TAGS:
            failures.append(f"{name}: platform tag {tag} is not one of {RUNTIME_TAGS}")
        failures += [f"{name}: missing {data}{binary}" for binary in ("kymo", "kymo-server") if data + binary not in files]
        server = files.get(data + "kymo-server", b"")
        if b"kymo-frontend_bg-" not in server:
            failures.append(f"{name}: kymo-server embeds no dashboard bundle")
        if tag.startswith("macosx"):
            failures += check_macho(name, {m: files[m] for m in (data + "kymo", data + "kymo-server") if m in files})
    elif name.endswith(".whl"):
        version = name.split("-")[1]
        exact, prefixes = set(), ("kymo/", f"kymo-{version}.dist-info/")
        licenses = {"LICENSE", "NOTICE"}
    else:
        prefix = name.removesuffix(".tar.gz") + "/"
        exact, prefixes = set(), (prefix,)
        licenses = set()
        failures += [f"{name}: unexpected member {m}" for m in files if m.removeprefix(prefix).startswith(("test_", "."))]
        failures += [f"{name}: missing {prefix}{f}" for f in ("LICENSE", "NOTICE") if prefix + f not in files]
    failures += [f"{name}: unexpected member {m}" for m in files if m not in exact and not m.startswith(prefixes)]
    present = {m.rsplit("/", 1)[-1] for m in files if ".dist-info/licenses/" in m}
    failures += [f"{name}: missing dist-info/licenses/{f}" for f in sorted(licenses - present)]
    for member in (m for m in files if m.endswith(("/METADATA", "/PKG-INFO"))):
        failures += [f"{name}: {member} description has relative link {m.group(0)!r}" for m in RELATIVE_LINK.finditer(files[member].decode(errors="replace"))]
    return failures


def check_macho(name: str, binaries: dict[str, bytes]) -> list[str]:
    failures = []
    for member, data in binaries.items():
        # 64-bit Mach-O magic, then CPU type arm64 (0x0100000c), both little-endian.
        if data[:8] != bytes.fromhex("cffaedfe0c000001"):
            failures.append(f"{name}: {member} is not an arm64 Mach-O binary")
            continue
        with tempfile.NamedTemporaryFile() as binary:
            binary.write(data)
            binary.flush()
            build = subprocess.run(["otool", "-l", binary.name], capture_output=True, text=True, check=True).stdout
        minos = re.search(r"minos (\d+)\.(\d+)", build)
        if not minos or (int(minos.group(1)), int(minos.group(2))) > (11, 0):
            failures.append(f"{name}: {member} requires macOS {minos.group(0) if minos else '?'} (tag says 11.0)")
    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dist", type=Path)
    args = parser.parse_args()
    paths = sorted(p for p in args.dist.iterdir() if p.suffix == ".whl" or p.name.endswith(".tar.gz"))
    if not paths:
        sys.exit(f"check_artifacts: no distributions in {args.dist}")
    subprocess.run([sys.executable, "-m", "twine", "check", "--strict", *map(str, paths)], check=True)
    hosts = allowlist()
    shipped_docs = {p.name for p in (ROOT / "docs").glob("*.md")}
    failures = []
    for path in paths:
        files = members(path)
        failures += check_distribution(path, files)
        for member, data in files.items():
            failures += scan(f"{path.name}!{member}", data, hosts, shipped_docs)
    if failures:
        print("\n".join(failures), file=sys.stderr)
        sys.exit(f"check_artifacts: {len(failures)} failure(s)")
    print(f"check_artifacts: {len(paths)} distribution(s) passed")


if __name__ == "__main__":
    main()
