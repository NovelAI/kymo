"""
Test tagged (bundled) metrics: log GPU-like metrics as lists.
Each list element becomes a separate trace under one metric ID.
"""

import math
import numpy as np
import kymo

PROJECT = "tagged-test"
RUN_ID = "run-1"
STEPS = 200
NUM_GPUS = 4


def run_one(run_name, seed, loss_scale=2.0, gpu_base=0.7):
    kymo.init(project_id=PROJECT, run_name=run_name)

    rng = np.random.RandomState(seed)

    for step in range(STEPS):
        progress = step / STEPS

        loss = loss_scale * math.exp(-3 * progress) + 0.05 + rng.normal(0, 0.01)

        gpu_util = [
            float(gpu_base + 0.2 * progress + rng.normal(0, 0.03) + 0.05 * i)
            for i in range(NUM_GPUS)
        ]

        gpu_mem = [
            float(8.0 + 4.0 * progress + rng.normal(0, 0.1) + 2.0 * i)
            for i in range(NUM_GPUS)
        ]

        kymo.log(
            {
                "training/loss": loss,
                "system/gpu_util": gpu_util,
                "system/gpu_mem_gb": gpu_mem,
            },
            step=step,
        )

        if step % 50 == 0:
            print(
                f"  step {step}: loss={loss:.3f}, gpu_util={[f'{v:.2f}' for v in gpu_util]}"
            )

    print(f"Waiting for {run_name} upload...")
    if not kymo.wait_for_upload(timeout=30):
        raise RuntimeError(f"{run_name}: upload did not complete")
    print(f"{run_name} done!")


def main():
    run_one("run-1", seed=42, loss_scale=2.0, gpu_base=0.7)
    run_one("run-2", seed=99, loss_scale=1.5, gpu_base=0.6)


if __name__ == "__main__":
    main()
