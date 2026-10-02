"""Opt-in live test for the packaged local supervisor and server."""

import os
import subprocess
import uuid
import unittest
from unittest import mock

import grpc
import kymo

from kymo._generated import kymo_pb2, kymo_pb2_grpc
from kymo._local_runtime import _launcher_path, ensure_local_endpoint, grpc_channel


@unittest.skipUnless(
    os.environ.get("KYMO_LIVE_LOCAL_TESTS") == "1",
    "set KYMO_LIVE_LOCAL_TESTS=1 with an installed local runtime",
)
class LocalRuntimeIntegrationTests(unittest.TestCase):
    def test_init_rich_and_numeric_delivery_and_finish_use_private_endpoints(self):
        initial_endpoint = ensure_local_endpoint()
        run_id = f"local-client-{uuid.uuid4().hex}"
        try:
            kymo.init(
                mode="local",
                project_id="local-client-integration",
                run_name="local Python client integration",
                run_id=run_id,
                system_metrics=False,
                config={"phase": "initial"},
            )
            self.assertEqual(
                kymo.run_url(),
                f"{initial_endpoint.dashboard_origin}/local-client-integration/{run_id}",
            )
            kymo.log({"loss": 1.25}, step=1)
            print(f"live-marker-{run_id}")
            kymo.update_config({"phase": "updated"})

            channel = grpc_channel(initial_endpoint)
            try:
                stub = kymo_pb2_grpc.KymoStub(channel)
                older = stub.InitRun(
                    kymo_pb2.InitRunRequest(
                        project_id="local-client-integration",
                        run_id=run_id,
                        run_name="local Python client integration",
                    )
                ).writer_epoch
                newer = stub.InitRun(
                    kymo_pb2.InitRunRequest(
                        project_id="local-client-integration",
                        run_id=run_id,
                        run_name="local Python client integration",
                    )
                ).writer_epoch
                self.assertGreater(newer, older)
                key = "ordering/probe"
                newer_version = (newer << 32) | 1
                older_version = (older << 32) | 9
                accepted = stub.PublishRichMutation(
                    kymo_pb2.PublishRichMutationRequest(
                        project_id="local-client-integration",
                        run_id=run_id,
                        metric_name=key,
                        step=0,
                        timestamp_ms=1_700_000_000_001,
                        cdn_key="newer.json",
                        mutation_version=newer_version,
                    )
                )
                self.assertEqual(accepted.disposition, kymo_pb2.RICH_MUTATION_ACCEPTED)
                tagged_version = (newer << 32) | 2
                tagged = stub.PublishRichMutation(
                    kymo_pb2.PublishRichMutationRequest(
                        project_id="local-client-integration",
                        run_id=run_id,
                        metric_name=key,
                        tag="tagged",
                        step=0,
                        timestamp_ms=1_700_000_000_002,
                        cdn_key="tagged.json",
                        mutation_version=tagged_version,
                    )
                )
                self.assertEqual(tagged.disposition, kymo_pb2.RICH_MUTATION_ACCEPTED)
                superseded = stub.PublishRichMutation(
                    kymo_pb2.PublishRichMutationRequest(
                        project_id="local-client-integration",
                        run_id=run_id,
                        metric_name=key,
                        step=0,
                        timestamp_ms=1_700_000_000_000,
                        cdn_key="older.json",
                        mutation_version=older_version,
                    )
                )
                self.assertEqual(
                    superseded.disposition, kymo_pb2.RICH_MUTATION_SUPERSEDED
                )
                response = stub.QueryCdnKeys(
                    kymo_pb2.QueryCdnKeysRequest(
                        refs=[
                            kymo_pb2.SeriesRef(
                                project_id="local-client-integration",
                                run_id=run_id,
                                metric_name=key,
                            )
                        ]
                    )
                )
                self.assertEqual(
                    {entry.cdn_key for entry in response.series[0].entries},
                    {"newer.json", "tagged.json"},
                )
                with self.assertRaises(grpc.RpcError) as conflict:
                    stub.PublishRichMutation(
                        kymo_pb2.PublishRichMutationRequest(
                            project_id="local-client-integration",
                            run_id=run_id,
                            metric_name=key,
                            step=0,
                            timestamp_ms=1_700_000_000_001,
                            cdn_key="different.json",
                            mutation_version=newer_version,
                        )
                    )
                self.assertEqual(conflict.exception.code(), grpc.StatusCode.DATA_LOSS)
            finally:
                channel.close()
            self.assertTrue(kymo.finish(flush_timeout=30))
        finally:
            kymo.finish(flush_timeout=5)

        # Reading through Api covers its channel, bearer, receive limit and loopback CDN. The patch restores the fork-guard flag these reads set, so later init() tests still run, as they already did after grpc_channel above started gRPC here.
        with (
            mock.patch.object(kymo.api, "_channel_opened", False),
            kymo.Api(mode="local") as api,
        ):
            project = "local-client-integration"
            self.assertIn(run_id, [run.run_id for run in api.runs(project)])
            self.assertEqual(api.history(project, run_id, "loss"), {"": [(1, 1.25)]})
            self.assertEqual(
                api.run_info(project, run_id)["config"], {"phase": "updated"}
            )
            # One stream: a merged read splices a partial line of one stream (the runner's unterminated "test ... ") onto the other's next chunk.
            marked = api.logs(
                project,
                run_id,
                streams=["logs/std_out"],
                search=f"live-marker-{run_id}",
            )
            self.assertEqual(
                [ln.text for ln in marked.lines], [f"live-marker-{run_id}"]
            )

            final_endpoint = ensure_local_endpoint(
                expected_installation_uuid=initial_endpoint.installation_uuid
            )
            self.assertEqual(
                final_endpoint.endpoint_generation,
                initial_endpoint.endpoint_generation,
            )

            # A read right after the stack stops starts it again and succeeds, though the reconnected channel shares a connection still backing off from the stop.
            subprocess.run([_launcher_path(), "stop"], check=True, capture_output=True)
            self.assertEqual(api.history(project, run_id, "loss"), {"": [(1, 1.25)]})

            # A restart while idle keeps the socket path but rotates the bearer: the next read reconnects, and its CDN fetch follows the new endpoint.
            for command in (["stop"], ["ensure", "--json"]):
                subprocess.run(
                    [_launcher_path(), *command], check=True, capture_output=True
                )
            self.assertEqual(
                api.run_info(project, run_id)["config"], {"phase": "updated"}
            )


if __name__ == "__main__":
    unittest.main()
