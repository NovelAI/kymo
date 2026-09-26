"""
Live logging test: 1M points over ~10 minutes.
Logs 5 metrics every step, 100k steps total = 500k points.
Plus a second run logging independently = 1M total.
"""

import math
import time

import numpy as np
import kymo

PROJECT = "stress-test"
TOTAL_STEPS = 100_000
METRICS_PER_STEP = 5  # 5 metrics × 100k steps × 2 runs = 1M points
LOG_INTERVAL = 0.003  # ~3ms per step → ~5min per run, ~10min total


def run_one(run_name: str, seed: int):
    kymo.init(project_id=PROJECT, run_name=run_name)
    print(f"Starting {run_name}: {TOTAL_STEPS} steps, {METRICS_PER_STEP} metrics")
    rng = np.random.RandomState(seed)
    t0 = time.monotonic()

    for step in range(TOTAL_STEPS):
        progress = step / TOTAL_STEPS
        kymo.log(
            {
                "training/loss": float(
                    2.0 * math.exp(-3 * progress) + 0.05 + rng.normal(0, 0.01)
                ),
                "training/accuracy": float(
                    1 - math.exp(-2 * progress) + rng.normal(0, 0.005)
                ),
                "optimizer/lr": float(1e-3 * (1 - progress) ** 2),
                "optimizer/grad_norm": float(
                    0.3 * math.exp(-progress) + rng.exponential(0.02)
                ),
                "system/throughput": float(5000 + 2000 * progress + rng.normal(0, 100)),
            },
            step=step,
        )

        if step % 10000 == 0 and step > 0:
            elapsed = time.monotonic() - t0
            rate = step / elapsed
            print(
                f"  {run_name} step {step}: {rate:.0f} steps/s, {elapsed:.1f}s elapsed"
            )

        if LOG_INTERVAL > 0:
            time.sleep(LOG_INTERVAL)

    print(f"  {run_name}: waiting for upload...")
    if not kymo.wait_for_upload(timeout=120):
        raise RuntimeError(f"{run_name}: upload did not complete")
    elapsed = time.monotonic() - t0
    print(
        f"  {run_name}: done in {elapsed:.1f}s ({TOTAL_STEPS * METRICS_PER_STEP} points)"
    )


def main():
    t0 = time.monotonic()
    run_one("live-run-a", seed=42)
    run_one("live-run-b", seed=99)
    elapsed = time.monotonic() - t0
    print(
        f"\nAll done in {elapsed:.1f}s, {2 * TOTAL_STEPS * METRICS_PER_STEP} total points"
    )


if __name__ == "__main__":
    main()
