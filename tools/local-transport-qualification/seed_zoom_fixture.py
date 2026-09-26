"""Seed the compact deterministic dataset used by ``zoom_fences.py``.

The three unprefixed metrics land in the dashboard's initially-open catch-all
section, in lexical order and on one grid row:

* ``zoom_a_source`` is a contiguous, downsampled step chart.
* ``zoom_b_peer`` has a narrower, asymmetric domain but the same step sync key.
* ``zoom_c_sparse`` has two dense clusters separated by a large empty gap.

``system/zoom_client_a`` and ``system/zoom_client_b`` land in the system
section. System metrics default to a relative-time X axis, providing a
deterministic client-only synced pair for reset fences.
Those points are spaced by two milliseconds so their timestamp domain cannot
collapse to one slot on a fast runner.

Roughly 2.4k public ``log`` calls are intentional.  They exercise the ordinary
client path while staying far below the 90k-point production reproduction.
"""

import time

import kymo


PROJECT_ID = "zoom-e2e"
RUN_ID = "zoom-fixture"

SOURCE_FIRST = 300
SOURCE_LAST = 1_899
PEER_FIRST = 700
PEER_LAST = 1_299
SPARSE_CLUSTERS = ((0, 319), (2_048, 2_367))


def in_sparse_cluster(step: int) -> bool:
    return any(first <= step <= last for first, last in SPARSE_CLUSTERS)


def main() -> None:
    kymo.init(
        mode="local",
        project_id=PROJECT_ID,
        run_id=RUN_ID,
        run_name="zoom gesture qualification",
        system_metrics=False,
    )

    last_step = max(SOURCE_LAST, PEER_LAST, *(last for _, last in SPARSE_CLUSTERS))
    for step in range(last_step + 1):
        metrics: dict[str, float] = {}
        if SOURCE_FIRST <= step <= SOURCE_LAST:
            # Monotone sub-cent values make y-axis label precision/width change
            # as an x-axis pull narrows the visible window. This exercises
            # uPlot's internal axis-size convergence without a container resize.
            metrics["zoom_a_source"] = 1.0 + step / 100_000
        if PEER_FIRST <= step <= PEER_LAST:
            metrics["zoom_b_peer"] = float(200 + (step * 11) % 67)
        if in_sparse_cluster(step):
            metrics["zoom_c_sparse"] = float(400 + (step * 7) % 43)
        if metrics:
            kymo.log(metrics, step=step)

    for offset in range(96):
        step = last_step + 1 + offset
        kymo.log(
            {
                "system/zoom_client_a": float(500 + (offset * 5) % 71),
                "system/zoom_client_b": float(600 + (offset * 13) % 89),
            },
            step=step,
        )
        time.sleep(0.002)

    if not kymo.finish(flush_timeout=30):
        raise RuntimeError("zoom browser fixture did not flush")


if __name__ == "__main__":
    main()
