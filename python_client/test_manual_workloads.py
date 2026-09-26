"""Service-free checks for manual live-workload result reporting."""

import contextlib
import io
import unittest
from unittest import mock

import test_live
import test_system_metrics


class ManualWorkloadCompletionTests(unittest.TestCase):
    def test_live_workload_reports_success_only_after_upload_completion(self):
        for delivered in (False, True):
            with (
                self.subTest(delivered=delivered),
                mock.patch.object(test_live, "TOTAL_STEPS", 0),
                mock.patch.object(test_live, "LOG_INTERVAL", 0),
                mock.patch.object(test_live.kymo, "init"),
                mock.patch.object(
                    test_live.kymo,
                    "wait_for_upload",
                    return_value=delivered,
                ) as wait,
                contextlib.redirect_stdout(io.StringIO()) as output,
            ):
                if delivered:
                    test_live.run_one("manual-run", seed=1)
                    self.assertIn("manual-run: done", output.getvalue())
                else:
                    with self.assertRaisesRegex(
                        RuntimeError, "manual-run: upload did not complete"
                    ):
                        test_live.run_one("manual-run", seed=1)
                    self.assertNotIn("manual-run: done", output.getvalue())
                wait.assert_called_once_with(timeout=120)

    def test_system_metrics_workload_reports_success_only_after_upload_completion(self):
        for delivered in (False, True):
            with (
                self.subTest(delivered=delivered),
                mock.patch.object(test_system_metrics, "STEPS", 0),
                mock.patch.object(test_system_metrics.time, "sleep"),
                mock.patch.object(test_system_metrics.kymo, "init"),
                mock.patch.object(
                    test_system_metrics.kymo,
                    "wait_for_upload",
                    return_value=delivered,
                ) as wait,
                contextlib.redirect_stdout(io.StringIO()) as output,
            ):
                if delivered:
                    test_system_metrics.main()
                    self.assertIn("Done!", output.getvalue())
                else:
                    with self.assertRaisesRegex(
                        RuntimeError, "run-1: upload did not complete"
                    ):
                        test_system_metrics.main()
                    self.assertNotIn("Done!", output.getvalue())
                wait.assert_called_once_with(timeout=30)


if __name__ == "__main__":
    unittest.main()
