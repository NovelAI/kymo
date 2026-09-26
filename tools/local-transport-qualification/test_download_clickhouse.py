import hashlib
import io
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import download_clickhouse


class DownloadClickHouseTest(unittest.TestCase):
    def test_every_supported_target_has_a_frozen_sha256(self) -> None:
        self.assertEqual(
            {
                ("Darwin", "arm64"),
                ("Linux", "x86_64"),
            },
            set(download_clickhouse.ASSETS),
        )
        self.assertEqual(
            {"macos-arm64", "linux-x86_64"},
            set(download_clickhouse.CATALOG["clickhouse"]["targets"]),
        )
        for (
            _asset,
            archive_digest,
            installed_digest,
            archive_size,
            _archived,
        ) in download_clickhouse.ASSETS.values():
            self.assertGreater(archive_size, 0)
            for digest in (archive_digest, installed_digest):
                self.assertEqual(64, len(digest))
                int(digest, 16)

    def test_extracts_only_the_clickhouse_binary_member(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "clickhouse.tgz"
            payload = b"qualified-clickhouse"
            with tarfile.open(archive, "w:gz") as bundle:
                member = tarfile.TarInfo("package/usr/bin/clickhouse")
                member.size = len(payload)
                bundle.addfile(member, io.BytesIO(payload))
                ignored = tarfile.TarInfo("package/etc/config.xml")
                ignored.size = 7
                bundle.addfile(ignored, io.BytesIO(b"ignored"))
            output = root / "clickhouse"
            download_clickhouse._extract_clickhouse(archive, output)
            self.assertEqual(payload, output.read_bytes())

    def test_catalog_components_cannot_escape_the_download_directory(self) -> None:
        for value in ("", ".", "..", "../outside", "/absolute", "nested/name"):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(RuntimeError, "one normal path component"),
            ):
                download_clickhouse._catalog_component("test", value)
        self.assertEqual(
            "clickhouse.tgz",
            download_clickhouse._catalog_component("test", "clickhouse.tgz"),
        )

    def test_rejects_missing_or_duplicate_binary_members(self) -> None:
        for member_count in (0, 2):
            with self.subTest(member_count=member_count):
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    archive = root / "clickhouse.tgz"
                    with tarfile.open(archive, "w:gz") as bundle:
                        for index in range(member_count):
                            payload = f"clickhouse-{index}".encode()
                            member = tarfile.TarInfo(
                                f"package-{index}/usr/bin/clickhouse"
                            )
                            member.size = len(payload)
                            bundle.addfile(member, io.BytesIO(payload))
                    with self.assertRaisesRegex(
                        RuntimeError,
                        f"expected one usr/bin/clickhouse member, found {member_count}",
                    ):
                        download_clickhouse._extract_clickhouse(
                            archive, root / "clickhouse"
                        )

    def test_rejects_download_with_wrong_sha256(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "clickhouse"

            def write_tampered_download(
                _url: str, output: Path, _expected_size: int
            ) -> None:
                output.write_bytes(b"tampered")

            with (
                mock.patch.object(
                    download_clickhouse.platform, "system", return_value="Darwin"
                ),
                mock.patch.object(
                    download_clickhouse.platform, "machine", return_value="arm64"
                ),
                mock.patch.object(
                    download_clickhouse,
                    "_download",
                    side_effect=write_tampered_download,
                ),
                self.assertRaisesRegex(RuntimeError, "SHA-256 mismatch"),
            ):
                download_clickhouse.install(destination)
            self.assertFalse(destination.exists())

    def test_rejects_extracted_binary_with_wrong_sha256(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.tgz"
            payload = b"qualified-clickhouse"
            with tarfile.open(source, "w:gz") as bundle:
                member = tarfile.TarInfo("package/usr/bin/clickhouse")
                member.size = len(payload)
                bundle.addfile(member, io.BytesIO(payload))
            archive_digest = hashlib.sha256(source.read_bytes()).hexdigest()
            destination = root / "clickhouse"
            target = ("Linux", "x86_64")

            def copy_download(_url: str, output: Path, _expected_size: int) -> None:
                output.write_bytes(source.read_bytes())

            with (
                mock.patch.object(
                    download_clickhouse.platform, "system", return_value=target[0]
                ),
                mock.patch.object(
                    download_clickhouse.platform, "machine", return_value=target[1]
                ),
                mock.patch.dict(
                    download_clickhouse.ASSETS,
                    {
                        target: (
                            "asset.tgz",
                            archive_digest,
                            "0" * 64,
                            len(source.read_bytes()),
                            True,
                        )
                    },
                    clear=True,
                ),
                mock.patch.object(
                    download_clickhouse, "_download", side_effect=copy_download
                ),
                self.assertRaisesRegex(
                    RuntimeError, "extracted binary SHA-256 mismatch"
                ),
            ):
                download_clickhouse.install(destination)
            self.assertFalse(destination.exists())

    def test_reuses_verified_installed_binary(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "clickhouse"
            destination.write_bytes(b"expanded-clickhouse")
            installed_digest = hashlib.sha256(destination.read_bytes()).hexdigest()
            target = ("Linux", "x86_64")
            with (
                mock.patch.object(
                    download_clickhouse.platform, "system", return_value=target[0]
                ),
                mock.patch.object(
                    download_clickhouse.platform, "machine", return_value=target[1]
                ),
                mock.patch.dict(
                    download_clickhouse.ASSETS,
                    {
                        target: (
                            "asset.tgz",
                            "0" * 64,
                            installed_digest,
                            1,
                            True,
                        )
                    },
                    clear=True,
                ),
                mock.patch.object(download_clickhouse, "_download") as download,
            ):
                self.assertEqual(destination, download_clickhouse.install(destination))
                self.assertEqual(0o755, destination.stat().st_mode & 0o777)
                download.assert_not_called()

    def test_sha256_streams_the_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "asset"
            path.write_bytes(b"kymo")
            self.assertEqual(
                hashlib.sha256(b"kymo").hexdigest(), download_clickhouse._sha256(path)
            )

    def test_copy_exact_rejects_short_and_oversized_responses(self) -> None:
        for payload, expected_size in [(b"short", 6), (b"oversized", 8)]:
            with self.subTest(payload=payload, expected_size=expected_size):
                with self.assertRaisesRegex(RuntimeError, "download"):
                    download_clickhouse._copy_exact(
                        io.BytesIO(payload), io.BytesIO(), expected_size
                    )

        output = io.BytesIO()
        download_clickhouse._copy_exact(io.BytesIO(b"exact"), output, 5)
        self.assertEqual(b"exact", output.getvalue())

    def test_download_accepts_exact_chunked_response_without_content_length(
        self,
    ) -> None:
        class Response(io.BytesIO):
            headers = {}

        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "download"
            with mock.patch.object(
                download_clickhouse.urllib.request,
                "urlopen",
                return_value=Response(b"exact"),
            ):
                download_clickhouse._download("https://example.test", destination, 5)

            self.assertEqual(b"exact", destination.read_bytes())


if __name__ == "__main__":
    unittest.main()
