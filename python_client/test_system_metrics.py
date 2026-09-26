"""Test system metrics logging — logs GPU/CPU/memory/disk/network automatically."""

import math
import time

import kymo

PROJECT = "system-metrics-test"
RUN_NAME = "run-1"
STEPS = 100


def main():
    kymo.init(project_id=PROJECT, run_name=RUN_NAME)

    # Give poller a moment to collect first sample
    time.sleep(3)

    for step in range(STEPS):
        progress = step / STEPS
        loss = 2.0 * math.exp(-3 * progress) + 0.05

        # Just log loss — system metrics are merged automatically
        kymo.log({"training/loss": loss}, step=step)

        if step % 25 == 0:
            print(f"  step {step}: loss={loss:.3f}")

        time.sleep(0.05)  # ~50ms per step

    print("Waiting for upload...")
    if not kymo.wait_for_upload(timeout=30):
        raise RuntimeError(f"{RUN_NAME}: upload did not complete")
    print("Done!")


if __name__ == "__main__":
    main()
