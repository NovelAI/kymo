import argparse
import hashlib
import json
import os
import platform
import shutil
import tarfile
import tempfile
import urllib.request
from pathlib import Path


CATALOG_PATH = Path(__file__).parents[2] / "shared" / "local-runtime-artifacts.json"
CATALOG = json.loads(CATALOG_PATH.read_text())
if CATALOG.get("schema_version") != 1:
    raise RuntimeError("unsupported local-runtime artifact catalog schema")
BASE_URL = CATALOG["clickhouse"]["base_url"]
TARGETS = {
    ("Darwin", "arm64"): "macos-arm64",
    ("Linux", "x86_64"): "linux-x86_64",
}


def _catalog_component(name: str, value: str) -> str:
    if not value or value in {".", ".."} or Path(value).parts != (value,):
        raise RuntimeError(f"{name} must be one normal path component")
    return value


ASSETS = {
    platform_key: (
        _catalog_component(
            "ClickHouse filename",
            CATALOG["clickhouse"]["targets"][catalog_key]["filename"],
        ),
        CATALOG["clickhouse"]["targets"][catalog_key]["sha256"],
        CATALOG["clickhouse"]["targets"][catalog_key]["installed_sha256"],
        CATALOG["clickhouse"]["targets"][catalog_key]["size"],
        CATALOG["clickhouse"]["targets"][catalog_key]["format"] == "tar-gz-clickhouse",
    )
    for platform_key, catalog_key in TARGETS.items()
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _copy_exact(source, output, expected_size: int) -> None:
    received = 0
    while chunk := source.read(1024 * 1024):
        received += len(chunk)
        if received > expected_size:
            raise RuntimeError(f"download exceeded expected size {expected_size} bytes")
        output.write(chunk)
    if received != expected_size:
        raise RuntimeError(
            f"download length mismatch: expected {expected_size}, got {received}"
        )


def _download(url: str, destination: Path, expected_size: int) -> None:
    request = urllib.request.Request(
        url, headers={"User-Agent": "kymo-local-transport-qualification"}
    )
    with (
        urllib.request.urlopen(request, timeout=120) as response,
        destination.open("wb") as output,
    ):
        content_length = response.headers.get("Content-Length")
        if content_length is not None and int(content_length) != expected_size:
            raise RuntimeError(
                "download length mismatch before transfer: "
                f"expected {expected_size}, got {content_length}"
            )
        _copy_exact(response, output, expected_size)


def _extract_clickhouse(archive: Path, destination: Path) -> None:
    with tarfile.open(archive, "r:gz") as bundle:
        matches = [
            member
            for member in bundle.getmembers()
            if member.isfile() and member.name.endswith("/usr/bin/clickhouse")
        ]
        if len(matches) != 1:
            raise RuntimeError(
                f"expected one usr/bin/clickhouse member, found {len(matches)}"
            )
        source = bundle.extractfile(matches[0])
        if source is None:
            raise RuntimeError("ClickHouse archive member has no data")
        with source, destination.open("wb") as output:
            shutil.copyfileobj(source, output, length=1024 * 1024)


def install(destination: Path) -> Path:
    key = (platform.system(), platform.machine())
    try:
        asset, expected, installed, archive_size, archived = ASSETS[key]
    except KeyError as error:
        raise RuntimeError(
            f"unsupported qualification target {key[0]} {key[1]}"
        ) from error
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        valid_digests = {installed} if archived else {expected, installed}
        if _sha256(destination) in valid_digests:
            os.chmod(destination, 0o755)
            return destination
    with tempfile.TemporaryDirectory(
        prefix="kymo-clickhouse-download-", dir=destination.parent
    ) as temporary:
        download = Path(temporary) / asset
        _download(f"{BASE_URL}/{asset}", download, archive_size)
        actual = _sha256(download)
        if actual != expected:
            raise RuntimeError(
                f"ClickHouse {asset} SHA-256 mismatch: expected {expected}, got {actual}"
            )
        candidate = Path(temporary) / "clickhouse"
        if archived:
            _extract_clickhouse(download, candidate)
            actual_installed = _sha256(candidate)
            if actual_installed != installed:
                raise RuntimeError(
                    "ClickHouse extracted binary SHA-256 mismatch: "
                    f"expected {installed}, got {actual_installed}"
                )
        else:
            candidate = download
        os.chmod(candidate, 0o755)
        os.replace(candidate, destination)
    return destination


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(install(args.output))


if __name__ == "__main__":
    main()
