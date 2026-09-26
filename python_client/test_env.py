import os
import importlib.util
import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

_SPEC = importlib.util.spec_from_file_location(
    "kymo_env_under_test", Path(__file__).parent / "kymo" / "_env.py"
)
assert _SPEC is not None and _SPEC.loader is not None
_env = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_env)


class EnvironmentAliasTests(unittest.TestCase):
    def resolve(self, values):
        with mock.patch.dict(os.environ, values, clear=True):
            return _env.integer("KYMO_LIMIT", "MKDB2_LIMIT", 4)

    def test_unset_uses_default(self):
        self.assertEqual(self.resolve({}), 4)

    def test_canonical_only(self):
        self.assertEqual(self.resolve({"KYMO_LIMIT": "8"}), 8)

    def test_legacy_only(self):
        with self.assertRaisesRegex(ValueError, "MKDB2_LIMIT.*KYMO_LIMIT"):
            self.resolve({"MKDB2_LIMIT": "8"})

    def test_equal_values_still_reject_the_legacy_name(self):
        with self.assertRaisesRegex(ValueError, "MKDB2_LIMIT.*KYMO_LIMIT"):
            self.resolve({"KYMO_LIMIT": "08", "MKDB2_LIMIT": "8"})

    def test_conflicting_values_fail(self):
        with self.assertRaisesRegex(ValueError, "MKDB2_LIMIT.*KYMO_LIMIT"):
            self.resolve({"KYMO_LIMIT": "8", "MKDB2_LIMIT": "9"})

    def test_invalid_value_fails(self):
        with self.assertRaises(ValueError):
            self.resolve({"MKDB2_LIMIT": "invalid"})

    def test_blank_values_are_unset_and_fall_back(self):
        self.assertEqual(self.resolve({"KYMO_LIMIT": " \t "}), 4)
        with self.assertRaisesRegex(ValueError, "MKDB2_LIMIT.*KYMO_LIMIT"):
            self.resolve({"KYMO_LIMIT": "", "MKDB2_LIMIT": ""})

    def test_string_aliases_accept_either_equal_namespace_and_reject_conflicts(self):
        for values, expected in (
            ({}, "default"),
            ({"KYMO_SERVER": "canonical"}, "canonical"),
        ):
            with (
                self.subTest(values=values),
                mock.patch.dict(os.environ, values, clear=True),
            ):
                self.assertEqual(
                    _env.string("KYMO_SERVER", "MKDB2_SERVER", "default"), expected
                )
        with (
            mock.patch.dict(
                os.environ,
                {"KYMO_SERVER": "canonical", "MKDB2_SERVER": "legacy"},
                clear=True,
            ),
            self.assertRaisesRegex(ValueError, "MKDB2_SERVER.*KYMO_SERVER"),
        ):
            _env.string("KYMO_SERVER", "MKDB2_SERVER", "default")

    def test_boolean_aliases_are_typed_and_invalid_values_fail(self):
        for values, expected in (
            ({}, False),
            ({"KYMO_FLAG": "yes"}, True),
        ):
            with (
                self.subTest(values=values),
                mock.patch.dict(os.environ, values, clear=True),
            ):
                self.assertEqual(_env.boolean("KYMO_FLAG", "MKDB2_FLAG"), expected)
        for values in (
            {"KYMO_FLAG": "true", "MKDB2_FLAG": "true"},
            {"MKDB2_FLAG": "invalid"},
        ):
            with (
                self.subTest(values=values),
                mock.patch.dict(os.environ, values, clear=True),
                self.assertRaises(ValueError),
            ):
                _env.boolean("KYMO_FLAG", "MKDB2_FLAG")
        with mock.patch.dict(os.environ, {"KYMO_VERBOSE": "invalid"}, clear=True):
            self.assertFalse(
                _env.boolean("KYMO_VERBOSE", "PYMKDB2_VERBOSE", strict=False)
            )

    def test_import_kymo_rejects_legacy_names_before_package_side_effects(self):
        checkout = Path(__file__).parent.resolve()
        base = {
            key: value
            for key, value in os.environ.items()
            if key not in {"KYMO_VERBOSE", "PYMKDB2_VERBOSE"}
        }
        base["PYTHONPATH"] = os.pathsep.join(
            [str(checkout), base.get("PYTHONPATH", "")]
        ).rstrip(os.pathsep)
        imported = subprocess.run(
            [
                sys.executable,
                "-c",
                "import pathlib, kymo; print(pathlib.Path(kymo.__file__).resolve())",
            ],
            cwd=checkout,
            env=base,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(imported.returncode, 0, imported.stderr)
        self.assertTrue(Path(imported.stdout.strip()).is_relative_to(checkout))
        for values in (
            {"PYMKDB2_VERBOSE": "historically-false"},
            {"KYMO_VERBOSE": "true", "PYMKDB2_VERBOSE": ""},
            {"MKDB2_SERVER": ""},
            {"MKDB2_SPOOL_DIR": "/shared/legacy"},
        ):
            with self.subTest(values=values):
                completed = subprocess.run(
                    [sys.executable, "-c", "import kymo"],
                    cwd=checkout,
                    env={**base, **values},
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertNotEqual(completed.returncode, 0)
                self.assertIn("no longer supported", completed.stderr)

    def test_numeric_legacy_name_is_rejected_even_when_values_are_equal(self):
        with mock.patch.dict(
            os.environ,
            {
                "KYMO_LOCAL_ENSURE_TIMEOUT": "600",
                "MKDB2_LOCAL_ENSURE_TIMEOUT": "600.0",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(
                ValueError, "MKDB2_LOCAL_ENSURE_TIMEOUT.*KYMO_LOCAL_ENSURE_TIMEOUT"
            ):
                _env.number(
                    "KYMO_LOCAL_ENSURE_TIMEOUT",
                    "MKDB2_LOCAL_ENSURE_TIMEOUT",
                    1.0,
                )

    def test_full_legacy_inventory_is_rejected_without_values(self):
        for legacy, canonical in _env.LEGACY_CLIENT_ENV.items():
            with (
                self.subTest(legacy=legacy),
                mock.patch.dict(os.environ, {legacy: ""}, clear=True),
                self.assertRaisesRegex(_env.RetiredEnvError, f"{legacy}.*{canonical}"),
            ):
                _env.reject_legacy_client_env()

    def test_import_failure_carries_the_marker_best_effort_callers_reraise(self):
        # Callers cannot import RetiredEnvError when `import kymo` itself fails,
        # so best-effort trainer loggers key on this attribute instead.
        checkout = Path(__file__).parent.resolve()
        env = {
            key: value
            for key, value in os.environ.items()
            if key not in _env.LEGACY_CLIENT_ENV
        }
        env["PYTHONPATH"] = str(checkout)
        env["MKDB2_SERVER"] = "legacy:1"
        probe = subprocess.run(
            [
                sys.executable,
                "-c",
                "try:\n    import kymo\n"
                "except Exception as e:\n"
                "    print(type(e).__name__, getattr(e, 'retired_env', False))",
            ],
            cwd=checkout,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(probe.stdout.strip(), "RetiredEnvError True")


if __name__ == "__main__":
    unittest.main()
