"""Create the real run used by the local browser qualification."""

import base64

import kymo
from kymo._generated import kymo_pb2 as pb, kymo_pb2_grpc
from kymo._local_runtime import ensure_local_endpoint, grpc_channel


def seed_trash_run() -> None:
    with grpc_channel(ensure_local_endpoint()) as channel:
        stub = kymo_pb2_grpc.KymoStub(channel)
        identity = {"project_id": "browser-e2e", "run_id": "browser-e2e-trashed"}
        stub.InitRun(pb.InitRunRequest(**identity, run_name="x" * 200), timeout=20)
        stub.TerminateRun(pb.TerminateRunRequest(**identity, exit_code=0), timeout=20)
        stub.TrashRuns(
            pb.TrashRunsRequest(
                project_id=identity["project_id"], run_ids=[identity["run_id"]]
            ),
            timeout=20,
        )


def main() -> None:
    png = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
    )
    kymo.init(
        mode="local",
        project_id="browser-e2e",
        run_id="browser-e2e",
        run_name="offline browser acceptance",
        system_metrics=False,
    )
    # Captured stdout takes the normal text-ingest path. More than one
    # virtual window lets the Settings fence check deep-scroll alignment.
    for line in range(640):
        print(f"settings log line {line:04d}")
    kymo.log(
        {
            "loss": 1.0,
            "sample": kymo.Image(png, caption="offline image"),
        },
        step=1,
    )
    if not kymo.finish(flush_timeout=30):
        raise RuntimeError("browser fixture did not flush")

    # A second run so the sidebar has a row to paint *onto*. gesture_fences.py needs two rows to
    # prove a drag actually propagates before it can assert that a mode change stops it; with one
    # row, repainting it with the value it already has is indistinguishable from nothing happening.
    kymo.init(
        mode="local",
        project_id="browser-e2e",
        run_id="browser-e2e-second",
        run_name="offline browser acceptance (second row)",
        system_metrics=False,
    )
    kymo.log({"loss": 2.0}, step=1)
    if not kymo.finish(flush_timeout=30):
        raise RuntimeError("second browser fixture run did not flush")
    seed_trash_run()


if __name__ == "__main__":
    main()
