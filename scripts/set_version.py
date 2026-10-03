#!/usr/bin/env python3
"""Stamp a release version over both packages' 0.0.0 placeholders: scripts/set_version.py YYYY.M.D."""

import re
import sys
from pathlib import Path

# The kymo tree: the public repository's root, or kymo/ in the monorepo.
ROOT = next(path for path in Path(__file__).resolve().parents if (path / "local-runtime" / "Cargo.toml").is_file())
PLACEHOLDERS = {
    "python_client/pyproject.toml": ['version = "0.0.0"', 'local = ["kymo-local-runtime==0.0.0"]'],
    "local-runtime/Cargo.toml": ['version = "0.0.0"'],
    "local-runtime/Cargo.lock": ['name = "kymo-local-runtime"\nversion = "0.0.0"'],
}


def main() -> None:
    version = sys.argv[1]
    # CalVer with no leading zeros, so Cargo (semver) and PyPI (PEP 440) spell it the same way.
    if not re.fullmatch(r"20\d\d\.[1-9]\d?\.[1-9]\d?", version):
        sys.exit(f"set_version: {version!r} is not a YYYY.M.D release version")
    for rel, placeholders in PLACEHOLDERS.items():
        path = ROOT / rel
        text = path.read_text()
        for placeholder in placeholders:
            if text.count(placeholder) != 1:
                sys.exit(f"set_version: expected one {placeholder!r} in {rel}")
            text = text.replace(placeholder, placeholder.replace("0.0.0", version))
        path.write_text(text)


if __name__ == "__main__":
    main()
