#!/usr/bin/env python3
"""Offline CA bundle regression tests: python3 security/test_trust.py.

The real public root certificates in fixtures/ and blocked-roots/ were extracted
from a macOS /etc/ssl/cert.pem. Tests only create temporary bundles; they never
change OS trust, install packages, or access the network.
"""

import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import ssl
import subprocess
import sys
import tempfile
import unittest


HERE = Path(__file__).resolve().parent
VERIFY = HERE / "verify-ca-bundle.sh"
PEM = re.compile(rb"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----", re.S)
FIXTURES = {
    "emsign-ecc-root-ca-g3.pem": "86a1ecba089c4a8d3bbe2734c612ba341d813e043cf9e8a862cd5c57a36bbe6b",
    "aaa-certificate-services.pem": "d7a7a0fb5d7e2731d771e9484ebcdef71d5f0c3e0a2948782bc83ee0ea699ef4",
    "amazon-root-ca-3.pem": "18ce6cfe7bf14e60b2e347b8dfe868cb31d02ebb3ada271569f50343b46db3a4",
    "globalsign-ecc-root-ca-r4.pem": "bec94911c2955676db6c0a550986d76e3ba005667c442c9762b4fbb773de228c",
}
BLOCKED = ("emsign-ecc-root-ca-g3.pem", "aaa-certificate-services.pem")
CLEAN = tuple(name for name in FIXTURES if name not in BLOCKED)
CA_VARIABLES = (
    "SSL_CERT_FILE",
    "CURL_CA_BUNDLE",
    "CARGO_HTTP_CAINFO",
    "GIT_SSL_CAINFO",
)

# Every executable reachable by the test copies is either a harmless filesystem
# utility or this dispatcher. In particular, no host package/trust manager,
# sudo, curl, Rustup, or apt-get is reachable through the test PATH.
FAKE_COMMAND = r"""
import hashlib, json, os, pathlib, re, ssl, sys
command = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
root = pathlib.Path(os.environ["TEST_ROOT"])
bundle = pathlib.Path(os.environ["TEST_BUNDLE"])
def certs(data):
    return re.findall(rb"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----", data, re.S)
def fingerprint(pem):
    return hashlib.sha256(ssl.PEM_cert_to_DER_cert(pem.decode())).hexdigest()
def clean(path):
    return not (set(fingerprint(pem) for pem in certs(path.read_bytes())) & blocked)
blocked = set(json.loads(os.environ["TEST_BLOCKED"]))
event = {"command": command, "args": args}
if command in ("dnf", "curl", "update-ca-trust"):
    event["fingerprints"] = [fingerprint(pem) for pem in certs(bundle.read_bytes())]
if command == "curl":
    event["github_env"] = (root / "github-env").read_text() if (root / "github-env").exists() else ""
if command in ("curl", "rustup-installer"):
    expected = json.loads(os.environ["TEST_EXPECT_CA_ENV"])
    event["ca_env"] = {key: os.environ.get(key) for key in expected}
    assert event["ca_env"] == expected, event["ca_env"]
with (root / "commands.jsonl").open("a") as log:
    log.write(json.dumps(event) + "\n")
if command == "uname":
    print(os.environ["TEST_PLATFORM"])
elif command == "id":
    print(os.environ.get("TEST_UID", "123"))
elif command == "sudo":
    assert args[0] in ("cp", "mkdir", "update-ca-trust", "dnf"), args
    os.execvpe(args[0], args, os.environ)
elif command == "update-ca-trust":
    # Model p11-kit extraction from the actual copied blocklist certificates.
    for policy in ("blacklist", "blocklist"):
        policy_dir = root / "system/etc/pki/ca-trust/source" / policy
        actual = {fingerprint(path.read_bytes()) for path in policy_dir.glob("*.pem")}
        assert actual == blocked, (policy, actual)
    if not os.environ.get("TEST_STALE_UPDATE"):
        # Regenerate from persistent source roots, not the previous extraction.
        source = (root / "original.pem").read_bytes()
        bundle.write_bytes(b"".join(pem + b"\n" for pem in certs(source) if fingerprint(pem) not in blocked))
elif command == "dnf":
    assert clean(bundle), "package download started before trust repair"
    # A package update may restore source roots; the persistent policy must win.
    bundle.write_bytes((root / "original.pem").read_bytes())
elif command == "curl":
    assert clean(bundle), "download started before trust repair"
    assert clean(pathlib.Path(args[args.index("--cacert") + 1]))
    pathlib.Path(args[args.index("-o") + 1]).write_text("#!/bin/sh\nrustup-installer\n")
elif command == "rustup-installer":
    if os.environ.get("TEST_INSTALL_RUSTUP"):
        cargo_bin = pathlib.Path(os.environ["CARGO_HOME"]) / "bin"
        cargo_bin.mkdir(parents=True)
        (cargo_bin / "rustup").symlink_to(root / "bin/fake-command")
elif command == "rustup":
    print("rustup 1.29.2 (offline fixture)")
else:
    raise AssertionError(command)
"""


class PlatformHarness:
    def __init__(self, parent, platform, certificates):
        self.root = Path(tempfile.mkdtemp(prefix="platform ", dir=parent))
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.scripts = self.root / "scripts"
        self.scripts.mkdir()
        for name in (
            "refresh-trust.sh",
            "verify-ca-bundle.sh",
            "manylinux-entrypoint.sh",
        ):
            source = (HERE / name).read_text()
            source = source.replace("/etc/", str(self.root / "system/etc") + "/")
            source = source.replace(
                "/var/lib/apt/", str(self.root / "system/var/lib/apt") + "/"
            )
            source = source.replace(
                "/opt/_internal/certs.pem",
                str(self.root / "system/opt/_internal/certs.pem"),
            )
            (self.scripts / name).write_text(source)
        (self.scripts / "blocked-roots").mkdir()
        for name in BLOCKED:
            shutil.copyfile(
                HERE / "blocked-roots" / name,
                self.scripts / "blocked-roots" / name,
            )
        for command in (
            "awk",
            "cat",
            "cp",
            "dirname",
            "mkdir",
            "mktemp",
            "mv",
            "openssl",
            "rm",
            "sh",
            "tr",
        ):
            (self.bin / command).symlink_to(shutil.which(command))
        dispatcher = self.bin / "fake-command"
        dispatcher.write_text("#!" + sys.executable + "\n" + FAKE_COMMAND)
        dispatcher.chmod(0o755)
        for command in (
            "uname",
            "id",
            "sudo",
            "dnf",
            "update-ca-trust",
            "curl",
            "rustup",
            "rustup-installer",
        ):
            (self.bin / command).symlink_to(dispatcher)
        relative = (
            "etc/ssl/cert.pem"
            if platform == "Darwin"
            else "etc/pki/tls/certs/ca-bundle.crt"
        )
        self.bundle = self.root / "system" / relative
        self.bundle.parent.mkdir(parents=True)
        original = b"".join(certificates[name] for name in (*CLEAN, *BLOCKED))
        self.bundle.write_bytes(original)
        (self.root / "original.pem").write_bytes(original)
        (self.root / "tmp").mkdir()
        self.env = {
            key: value
            for key, value in os.environ.items()
            if key not in (*CA_VARIABLES, "GITHUB_ENV")
        }
        self.env.update(
            PATH=str(self.bin),
            HOME=str(self.root / "home"),
            CARGO_HOME=str(self.root / "cargo"),
            TMPDIR=str(self.root / "tmp"),
            TEST_ROOT=str(self.root),
            TEST_PLATFORM=platform,
            TEST_BUNDLE=str(self.bundle),
            TEST_BLOCKED=json.dumps([FIXTURES[name] for name in BLOCKED]),
            TEST_EXPECT_CA_ENV=json.dumps(
                {name: str(self.bundle) for name in CA_VARIABLES}
            ),
            GITHUB_ENV=str(self.root / "github-env"),
        )

    def run(self, script="refresh-trust.sh", *arguments):
        return subprocess.run(
            [str(self.bin / "sh"), str(self.scripts / script), *arguments],
            env=self.env,
            capture_output=True,
            text=True,
            timeout=30,
        )

    def events(self):
        log = self.root / "commands.jsonl"
        return (
            [json.loads(line) for line in log.read_text().splitlines()]
            if log.exists()
            else []
        )


def fingerprints(data):
    return [
        hashlib.sha256(ssl.PEM_cert_to_DER_cert(pem.decode("ascii"))).hexdigest()
        for pem in PEM.findall(data)
    ]


class TrustBundleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.certificates = {
            name: (
                HERE / ("blocked-roots" if name in BLOCKED else "fixtures") / name
            ).read_bytes()
            for name in FIXTURES
        }

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="kymo trust test ")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def bundle(self, data, name="input bundle.pem"):
        path = self.root / name
        path.write_bytes(data)
        return path

    def run_script(self, *arguments):
        return subprocess.run(
            ["sh", str(VERIFY), *(str(arg) for arg in arguments)],
            capture_output=True,
            text=True,
            timeout=30,
        )

    def assert_success(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def assert_failure(self, result):
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)

    def assert_no_trust_side_effects(self, platform, original):
        self.assertEqual(platform.bundle.read_bytes(), original)
        self.assertFalse((platform.root / "github-env").exists())
        self.assertFalse((platform.root / "system/etc/pki/ca-trust/source").exists())
        self.assertFalse(
            any(
                event["command"]
                in ("sudo", "update-ca-trust", "dnf", "curl", "rustup")
                for event in platform.events()
            )
        )

    def assert_complete_rpm_distrust(self, platform):
        self.assertFalse((platform.root / "github-env").exists())
        for policy in ("blacklist", "blocklist"):
            paths = (platform.root / "system/etc/pki/ca-trust/source" / policy).glob(
                "*.pem"
            )
            self.assertEqual(
                {fingerprints(path.read_bytes())[0] for path in paths},
                {FIXTURES[name] for name in BLOCKED},
            )
        self.assertEqual(
            fingerprints(platform.bundle.read_bytes()),
            [FIXTURES[name] for name in CLEAN],
        )

    def malformed_bundles(self):
        clean = self.certificates[CLEAN[0]]
        indented_blocked = b"".join(
            b" " + line
            for line in self.certificates[BLOCKED[0]].splitlines(keepends=True)
        )
        return {
            "empty": b"",
            "no certificates": b"this is not a certificate bundle\n",
            "invalid certificate": (
                b"-----BEGIN CERTIFICATE-----\nnot valid base64\n"
                b"-----END CERTIFICATE-----\n"
            ),
            "truncated": clean.replace(b"-----END CERTIFICATE-----\n", b""),
            "nested": clean.replace(
                b"-----BEGIN CERTIFICATE-----\n",
                b"-----BEGIN CERTIFICATE-----\n-----BEGIN CERTIFICATE-----\n",
                1,
            ),
            "unmatched end": b"-----END CERTIFICATE-----\n" + clean,
            "valid then truncated": clean + b"-----BEGIN CERTIFICATE-----\nMIIB\n",
            "valid then invalid": (
                clean + b"-----BEGIN CERTIFICATE-----\nAAAA\n"
                b"-----END CERTIFICATE-----\n"
            ),
            "valid then fully indented blocked root": clean + indented_blocked,
        }

    def test_real_fixture_fingerprints(self):
        for name, expected in FIXTURES.items():
            with self.subTest(certificate=name):
                self.assertEqual(fingerprints(self.certificates[name]), [expected])

    def test_verify_accepts_clean_roots_and_duplicates(self):
        contents = b"".join(self.certificates[name] for name in (*CLEAN, CLEAN[0]))
        self.assert_success(self.run_script(self.bundle(contents)))

    def test_verify_rejects_each_blocked_root_alone_and_in_mixed_bundles(self):
        for blocked in BLOCKED:
            for surrounding in (b"", self.certificates[CLEAN[0]]):
                with self.subTest(blocked=blocked, mixed=bool(surrounding)):
                    bundle = self.bundle(
                        surrounding + self.certificates[blocked] + surrounding
                    )
                    self.assert_failure(self.run_script(bundle))

    def test_verify_checks_every_bundle_argument(self):
        clean = self.bundle(self.certificates[CLEAN[0]], "clean.pem")
        other_clean = self.bundle(self.certificates[CLEAN[1]], "other clean.pem")
        blocked = self.bundle(self.certificates[BLOCKED[0]], "blocked.pem")
        self.assert_success(self.run_script(clean, other_clean))
        self.assert_failure(self.run_script(clean, blocked))
        self.assert_failure(self.run_script(blocked, clean))

    def test_verify_rejects_malformed_bundles(self):
        for name, contents in self.malformed_bundles().items():
            with self.subTest(case=name):
                self.assert_failure(self.run_script(self.bundle(contents)))

    def test_filter_removes_each_blocked_root_and_both_together(self):
        for index, blocked_names in enumerate(((BLOCKED[0],), (BLOCKED[1],), BLOCKED)):
            with self.subTest(blocked=blocked_names):
                # Repeated clean roots exercise preservation rather than deduplication.
                original_names = (CLEAN[0], *blocked_names, CLEAN[1], CLEAN[0])
                source = self.bundle(
                    b"".join(self.certificates[name] for name in original_names)
                )
                output = self.root / f"filtered output {index}.pem"
                self.assert_success(self.run_script("--filter", output, source))
                self.assertEqual(
                    fingerprints(output.read_bytes()),
                    [FIXTURES[name] for name in (CLEAN[0], CLEAN[1], CLEAN[0])],
                )
                self.assert_success(self.run_script(output))

    def test_filter_is_idempotent_and_preserves_clean_der_certificates(self):
        source = self.bundle(
            b"".join(self.certificates[name] for name in (CLEAN[1], CLEAN[0], CLEAN[1]))
        )
        first = self.root / "first filtered.pem"
        second = self.root / "second filtered.pem"
        self.assert_success(self.run_script("--filter", first, source))
        self.assert_success(self.run_script("--filter", second, first))
        self.assertEqual(first.read_bytes(), second.read_bytes())
        self.assertEqual(
            fingerprints(source.read_bytes()),
            fingerprints(second.read_bytes()),
        )

    def test_filter_preserves_certificate_order_past_ten_certificates(self):
        names = (CLEAN[0], CLEAN[1], CLEAN[1]) * 5
        source = self.bundle(b"".join(self.certificates[name] for name in names))
        output = self.root / "long filtered.pem"
        self.assert_success(self.run_script("--filter", output, source))
        self.assertEqual(
            fingerprints(output.read_bytes()),
            [FIXTURES[name] for name in names],
        )

    def test_crlf_bundles_verify_and_filter(self):
        source = self.bundle(
            b"".join(self.certificates[name] for name in (*CLEAN, *BLOCKED)).replace(
                b"\n", b"\r\n"
            )
        )
        self.assert_failure(self.run_script(source))
        output = self.root / "crlf filtered.pem"
        self.assert_success(self.run_script("--filter", output, source))
        self.assertEqual(
            fingerprints(output.read_bytes()),
            [FIXTURES[name] for name in CLEAN],
        )
        self.assert_success(self.run_script(output))

    def test_trusted_certificate_pem_cannot_hide_a_blocked_root(self):
        # OpenSSL accepts this alternate PEM container; an ordinary-certificate
        # parser must reject it rather than silently treating the bundle as clean.
        for index, name in enumerate(BLOCKED):
            with self.subTest(blocked=name):
                trusted = subprocess.run(
                    ["openssl", "x509", "-trustout"],
                    input=self.certificates[name],
                    capture_output=True,
                    check=True,
                ).stdout
                self.assertIn(b"-----BEGIN TRUSTED CERTIFICATE-----", trusted)
                source = self.bundle(self.certificates[CLEAN[0]] + trusted)
                self.assert_failure(self.run_script(source))
                output = self.root / f"trusted filtered {index}.pem"
                self.assert_failure(self.run_script("--filter", output, source))
                self.assertFalse(output.exists())

    def test_filter_rejects_malformed_input_without_usable_bundle(self):
        for index, (name, contents) in enumerate(self.malformed_bundles().items()):
            with self.subTest(case=name):
                output = self.root / f"invalid filtered {index}.pem"
                self.assert_failure(
                    self.run_script("--filter", output, self.bundle(contents))
                )
                self.assertFalse(
                    output.exists() and output.read_bytes(),
                    "failed filter left a usable bundle",
                )

    def test_filter_rejects_all_blocked_input_without_usable_bundle(self):
        for index, blocked_names in enumerate(((BLOCKED[0],), (BLOCKED[1],), BLOCKED)):
            with self.subTest(blocked=blocked_names):
                source = self.bundle(
                    b"".join(self.certificates[name] for name in blocked_names)
                )
                output = self.root / f"all blocked {index}.pem"
                self.assert_failure(self.run_script("--filter", output, source))
                self.assertFalse(
                    output.exists() and output.read_bytes(),
                    "failed filter left a usable bundle",
                )

    def test_filter_rejects_empty_output_argument(self):
        source = self.bundle(self.certificates[CLEAN[0]])
        self.assert_failure(self.run_script("--filter", "", source))

    def test_filter_preserves_existing_files_and_symlinks(self):
        source = self.bundle(self.certificates[CLEAN[0]])
        sentinel = b"keep existing output\n"
        for kind in ("regular", "symlink", "dangling"):
            with self.subTest(kind=kind):
                output = self.root / f"existing {kind}.pem"
                target = self.root / f"target {kind}.pem"
                if kind == "regular":
                    output.write_bytes(sentinel)
                else:
                    if kind == "symlink":
                        target.write_bytes(sentinel)
                    output.symlink_to(target)
                self.assert_failure(self.run_script("--filter", output, source))
                if kind == "regular":
                    self.assertFalse(output.is_symlink())
                    self.assertEqual(output.read_bytes(), sentinel)
                else:
                    self.assertTrue(output.is_symlink())
                    self.assertEqual(output.readlink(), target)
                    if kind == "symlink":
                        self.assertEqual(target.read_bytes(), sentinel)
                    else:
                        self.assertFalse(target.exists())

    def test_each_clean_override_preserves_independent_environment_and_curl_choice(self):
        for operating_system in ("Darwin", "Linux"):
            for variable in CA_VARIABLES:
                with self.subTest(operating_system=operating_system, variable=variable):
                    platform = PlatformHarness(
                        self.root, operating_system, self.certificates
                    )
                    override = platform.root / "clean independent override.pem"
                    contents = self.certificates[CLEAN[0]]
                    override.write_bytes(contents)
                    platform.env[variable] = str(override)
                    selected_ssl = (
                        override if variable == "SSL_CERT_FILE" else platform.bundle
                    )
                    expected = {name: str(selected_ssl) for name in CA_VARIABLES}
                    expected[variable] = str(override)
                    platform.env["TEST_EXPECT_CA_ENV"] = json.dumps(expected)
                    self.assert_success(platform.run("refresh-trust.sh", "--rustup"))
                    download = next(
                        event
                        for event in platform.events()
                        if event["command"] == "curl"
                    )
                    self.assertEqual(download["ca_env"], expected)
                    self.assertEqual(
                        download["args"][download["args"].index("--cacert") + 1],
                        expected["CURL_CA_BUNDLE"],
                    )
                    self.assertEqual(override.read_bytes(), contents)
                    github_env = platform.root / "github-env"
                    if operating_system == "Darwin":
                        self.assertEqual(
                            dict(
                                line.split("=", 1)
                                for line in github_env.read_text().splitlines()
                            ),
                            expected,
                        )
                    else:
                        self.assertFalse(github_env.exists())

    def test_direct_refresh_finds_newly_installed_rustup_outside_initial_path(self):
        for operating_system in ("Darwin", "Linux"):
            with self.subTest(operating_system=operating_system):
                platform = PlatformHarness(
                    self.root, operating_system, self.certificates
                )
                (platform.bin / "rustup").unlink()
                platform.env["TEST_INSTALL_RUSTUP"] = "1"
                installed = Path(platform.env["CARGO_HOME"]) / "bin/rustup"
                self.assertFalse(installed.exists())
                self.assertNotIn(
                    str(installed.parent), platform.env["PATH"].split(os.pathsep)
                )
                self.assert_success(platform.run("refresh-trust.sh", "--rustup"))
                self.assertTrue(installed.is_symlink())
                self.assertTrue(
                    any(event["command"] == "rustup" for event in platform.events())
                )

    def test_darwin_repairs_default_before_download_and_exports_actions_environment(
        self,
    ):
        platform = PlatformHarness(self.root, "Darwin", self.certificates)
        self.assert_success(platform.run("refresh-trust.sh", "--rustup"))
        self.assertEqual(
            fingerprints(platform.bundle.read_bytes()),
            [FIXTURES[name] for name in CLEAN],
        )
        expected = {name: str(platform.bundle) for name in CA_VARIABLES}
        exports = dict(
            line.split("=", 1)
            for line in (platform.root / "github-env").read_text().splitlines()
        )
        self.assertEqual(exports, expected)
        downloads = [event for event in platform.events() if event["command"] == "curl"]
        self.assertEqual(len(downloads), 1)
        self.assertEqual(
            downloads[0]["fingerprints"], [FIXTURES[name] for name in CLEAN]
        )
        self.assertEqual(
            dict(
                line.split("=", 1) for line in downloads[0]["github_env"].splitlines()
            ),
            expected,
        )
        installer = next(
            event
            for event in platform.events()
            if event["command"] == "rustup-installer"
        )
        self.assertEqual(installer["ca_env"], expected)

    def test_darwin_preserves_clean_overrides_and_rejects_each_dirty_override(self):
        platform = PlatformHarness(self.root, "Darwin", self.certificates)
        override = platform.root / "clean override.pem"
        override.write_bytes(self.certificates[CLEAN[0]])
        platform.env.update({name: str(override) for name in CA_VARIABLES})
        platform.env["TEST_EXPECT_CA_ENV"] = json.dumps(
            {name: str(override) for name in CA_VARIABLES}
        )
        self.assert_success(platform.run("refresh-trust.sh", "--rustup"))
        exports = dict(
            line.split("=", 1)
            for line in (platform.root / "github-env").read_text().splitlines()
        )
        self.assertEqual(exports, {name: str(override) for name in CA_VARIABLES})
        download = next(
            event for event in platform.events() if event["command"] == "curl"
        )
        self.assertEqual(
            download["args"][download["args"].index("--cacert") + 1], str(override)
        )
        for variable in CA_VARIABLES:
            with self.subTest(variable=variable):
                platform = PlatformHarness(self.root, "Darwin", self.certificates)
                dirty = platform.root / "dirty override.pem"
                dirty.write_bytes(
                    self.certificates[CLEAN[0]] + self.certificates[BLOCKED[0]]
                )
                original = platform.bundle.read_bytes()
                platform.env[variable] = str(dirty)
                self.assert_failure(platform.run("refresh-trust.sh", "--rustup"))
                self.assert_no_trust_side_effects(platform, original)

    def test_dirty_default_bundle_overrides_reject_before_any_trust_mutation(self):
        for operating_system in ("Darwin", "Linux"):
            for alias in (False, True):
                for variable in CA_VARIABLES:
                    with self.subTest(
                        operating_system=operating_system,
                        symlink=alias,
                        variable=variable,
                    ):
                        platform = PlatformHarness(
                            self.root, operating_system, self.certificates
                        )
                        original = platform.bundle.read_bytes()
                        override = platform.bundle
                        if alias:
                            override = platform.root / "default bundle alias.pem"
                            override.symlink_to(platform.bundle)
                        platform.env[variable] = str(override)
                        self.assert_failure(
                            platform.run("refresh-trust.sh", "--rustup")
                        )
                        self.assert_no_trust_side_effects(platform, original)
                        if alias:
                            self.assertTrue(override.is_symlink())
                            self.assertEqual(override.read_bytes(), original)

    def test_rpm_distrust_precedes_packages_and_survives_package_reintroduction(self):
        platform = PlatformHarness(self.root, "Linux", self.certificates)
        self.assert_success(platform.run("refresh-trust.sh", "--rustup"))
        events = [
            event
            for event in platform.events()
            if event["command"] in ("update-ca-trust", "dnf", "curl", "rustup")
        ]
        self.assertEqual(
            [event["command"] for event in events],
            ["update-ca-trust", "dnf", "update-ca-trust", "curl", "rustup"],
        )
        self.assertEqual(events[1]["fingerprints"], [FIXTURES[name] for name in CLEAN])
        self.assert_complete_rpm_distrust(platform)

    def test_rpm_persists_both_blocked_roots_when_extracted_bundle_is_clean(self):
        platform = PlatformHarness(self.root, "Linux", self.certificates)
        platform.bundle.write_bytes(
            b"".join(self.certificates[name] for name in CLEAN)
        )
        self.assert_success(platform.run("refresh-trust.sh", "--rustup"))
        self.assert_complete_rpm_distrust(platform)
        self.assertTrue(any(event["command"] == "dnf" for event in platform.events()))

    def test_rpm_persists_both_blocked_roots_when_only_one_is_extracted(self):
        for blocked in BLOCKED:
            with self.subTest(blocked=blocked):
                platform = PlatformHarness(self.root, "Linux", self.certificates)
                platform.bundle.write_bytes(
                    b"".join(self.certificates[name] for name in (*CLEAN, blocked))
                )
                self.assert_success(platform.run("refresh-trust.sh", "--rustup"))
                self.assert_complete_rpm_distrust(platform)
                self.assertTrue(
                    any(event["command"] == "dnf" for event in platform.events())
                )

    def test_rpm_refuses_download_if_trust_extraction_does_not_remove_roots(self):
        platform = PlatformHarness(self.root, "Linux", self.certificates)
        platform.env["TEST_STALE_UPDATE"] = "1"
        self.assert_failure(platform.run("refresh-trust.sh", "--rustup"))
        self.assertEqual(
            sum(event["command"] == "update-ca-trust" for event in platform.events()), 1
        )
        self.assertFalse(
            any(
                event["command"] in ("dnf", "curl", "rustup")
                for event in platform.events()
            )
        )

    def test_manylinux_entrypoint_forwards_arguments_environment_and_exit_status(self):
        platform = PlatformHarness(self.root, "Linux", self.certificates)
        child = (
            "import json,os,sys; print('CHILD ' + json.dumps({'args':sys.argv[1:],"
            f"'env':{{key:os.environ[key] for key in {CA_VARIABLES!r}}},"
            "'path':os.environ['PATH']})); sys.exit(37)"
        )
        generated_script = platform.root / "generated action script.sh"
        generated_script.write_text(
            "#!/bin/bash\nset -euo pipefail\n"
            f'exec {shlex.quote(sys.executable)} -c {shlex.quote(child)} "$@"\n'
        )
        generated_script.chmod(0o755)
        result = platform.run(
            "manylinux-entrypoint.sh",
            str(generated_script),
            "two words",
            "literal ; $*",
        )
        self.assertEqual(result.returncode, 37, result.stdout + result.stderr)
        output = json.loads(
            next(
                line.removeprefix("CHILD ")
                for line in result.stdout.splitlines()
                if line.startswith("CHILD ")
            )
        )
        self.assertEqual(output["args"], ["two words", "literal ; $*"])
        self.assertEqual(
            output["env"], {name: str(platform.bundle) for name in CA_VARIABLES}
        )
        self.assertEqual(
            output["path"].split(os.pathsep)[0],
            str(Path(platform.env["CARGO_HOME"]) / "bin"),
        )

    def test_manylinux_factory_certifi_default_selects_os_trust_without_sudo(self):
        platform = PlatformHarness(self.root, "Linux", self.certificates)
        factory_bundle = platform.root / "system/opt/_internal/certs.pem"
        factory_bundle.parent.mkdir(parents=True)
        original = b"".join(
            self.certificates[name] for name in (*CLEAN, *BLOCKED)
        )
        factory_bundle.write_bytes(original)
        platform.env.update(SSL_CERT_FILE=str(factory_bundle), TEST_UID="0")
        generated_script = platform.root / "generated factory action.py"
        generated_script.write_text(
            f"#!{sys.executable}\nimport json, os\n"
            "print('FACTORY ' + json.dumps("
            f"{{key: os.environ.get(key) for key in {CA_VARIABLES!r}}}))\n"
        )
        generated_script.chmod(0o755)
        result = platform.run("manylinux-entrypoint.sh", str(generated_script))
        self.assert_success(result)
        output = json.loads(
            next(
                line.removeprefix("FACTORY ")
                for line in result.stdout.splitlines()
                if line.startswith("FACTORY ")
            )
        )
        expected = {name: str(platform.bundle) for name in CA_VARIABLES}
        self.assertEqual(output, expected)
        self.assertEqual(factory_bundle.read_bytes(), original)
        self.assert_complete_rpm_distrust(platform)
        self.assertFalse(any(event["command"] == "sudo" for event in platform.events()))
        download = next(
            event for event in platform.events() if event["command"] == "curl"
        )
        self.assertEqual(download["ca_env"], expected)
        self.assertEqual(
            download["args"][download["args"].index("--cacert") + 1],
            str(platform.bundle),
        )

    def test_manylinux_entrypoint_rejects_every_dirty_custom_override(self):
        for variable in CA_VARIABLES:
            with self.subTest(variable=variable):
                platform = PlatformHarness(self.root, "Linux", self.certificates)
                original = platform.bundle.read_bytes()
                custom = platform.root / "system/opt/_internal/custom.pem"
                custom.parent.mkdir(parents=True)
                custom.write_bytes(original)
                platform.env[variable] = str(custom)
                generated_script = platform.root / "generated custom action.sh"
                generated_script.write_text("#!/bin/sh\nprintf 'CHILD-EXECUTED\\n'\n")
                generated_script.chmod(0o755)
                result = platform.run("manylinux-entrypoint.sh", str(generated_script))
                self.assert_failure(result)
                self.assertNotIn("CHILD-EXECUTED", result.stdout)
                self.assert_no_trust_side_effects(platform, original)
                self.assertEqual(custom.read_bytes(), original)

    def test_manylinux_entrypoint_stops_before_child_when_trust_repair_fails(self):
        platform = PlatformHarness(self.root, "Linux", self.certificates)
        platform.env["TEST_STALE_UPDATE"] = "1"
        generated_script = platform.root / "generated action script.sh"
        generated_script.write_text("#!/bin/sh\nprintf 'CHILD-EXECUTED\\n'\n")
        generated_script.chmod(0o755)
        result = platform.run("manylinux-entrypoint.sh", str(generated_script))
        self.assert_failure(result)
        self.assertNotIn("CHILD-EXECUTED", result.stdout)
        self.assertEqual(
            sum(event["command"] == "update-ca-trust" for event in platform.events()), 1
        )
        self.assertFalse(
            any(
                event["command"] in ("dnf", "curl", "rustup")
                for event in platform.events()
            )
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
