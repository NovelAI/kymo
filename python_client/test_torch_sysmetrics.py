"""
Train a silly large linear model on random GPU tensors.
Tests system metrics under actual GPU load.
"""

import sys
import time

import kymo
import torch
import torch.nn as nn

PROJECT = "meta_test"
RUN_NAME = "torch-run-3"
STEPS = 5000000
DIM = 8192  # large enough to actually use the GPU


def main():
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Device: {device}")

    model = nn.Sequential(
        nn.Linear(DIM, DIM),
        nn.ReLU(),
        nn.Linear(DIM, DIM),
        nn.ReLU(),
        nn.Linear(DIM, DIM),
    ).to(device)

    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
    criterion = nn.MSELoss()

    kymo.init(project_id=PROJECT, run_name=RUN_NAME)
    time.sleep(2)  # let poller collect first sample

    t0 = time.monotonic()
    for step in range(STEPS):
        x = torch.randn(256, DIM, device=device)
        target = torch.randn(256, DIM, device=device)

        optimizer.zero_grad()
        y = model(x)
        loss = criterion(y, target)
        loss.backward()
        optimizer.step()

        kymo.log(
            {
                "training/loss": loss.item(),
            },
            step=step,
        )

        if step % 100 == 0:
            elapsed = time.monotonic() - t0
            print(f"  step {step}: loss={loss.item():.4f}, elapsed={elapsed:.1f}s")
            print("error lol", file=sys.stderr)

    elapsed = time.monotonic() - t0
    print(f"\n{STEPS} steps in {elapsed:.1f}s ({STEPS / elapsed:.0f} steps/s)")
    print("Waiting for upload...")
    if not kymo.wait_for_upload(timeout=30):
        raise RuntimeError(f"{RUN_NAME}: upload did not complete")
    print("Done!")


if __name__ == "__main__":
    main()
