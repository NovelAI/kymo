"""
Test script: 10 runs × 1000 steps × multiple metrics across sections.
No images, just numeric data.
"""

import math
import time

import numpy as np
import kymo

PROJECT = "stress-test"
TOTAL_STEPS = 1000
NUM_RUNS = 10


def run_one(run_name: str, seed: int):
    kymo.init(project_id=PROJECT, run_name=run_name)
    rng = np.random.RandomState(seed)

    for step in range(TOTAL_STEPS):
        progress = step / TOTAL_STEPS

        kymo.log(
            {
                # training section
                "training/loss": float(
                    2.0 * math.exp(-3 * progress) + 0.1 + rng.normal(0, 0.03)
                ),
                "training/accuracy": float(
                    1 - math.exp(-2 * progress) + rng.normal(0, 0.02)
                ),
                "training/perplexity": float(
                    math.exp(2.0 * math.exp(-3 * progress) + 0.1)
                ),
                # optimizer section
                "optimizer/learning_rate": float(1e-4 * (1 - progress)),
                "optimizer/grad_norm": float(
                    0.5 * math.exp(-progress) + rng.exponential(0.05)
                ),
                "optimizer/weight_decay": float(0.01),
                # eval section
                "eval/val_loss": float(
                    2.2 * math.exp(-2.5 * progress) + 0.15 + rng.normal(0, 0.04)
                ),
                "eval/val_accuracy": float(
                    1 - math.exp(-1.8 * progress) + rng.normal(0, 0.03)
                ),
                # system section
                "system/gpu_util": float(85 + rng.normal(0, 5)),
                "system/memory_gb": float(12 + rng.normal(0, 0.5)),
                "system/throughput": float(1000 + 500 * progress + rng.normal(0, 50)),
            },
            step=step,
        )

    if not kymo.wait_for_upload(timeout=60):
        raise RuntimeError(f"{run_name}: upload did not complete")
    print(f"  {run_name} done")


def main():
    t0 = time.monotonic()
    for i in range(NUM_RUNS):
        run_name = f"run-{i:02d}"
        print(f"Starting {run_name}...")
        run_one(run_name, seed=i * 42)
    elapsed = time.monotonic() - t0
    print(f"\nAll done in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
