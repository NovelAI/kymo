"""
Test script for the kymo CDN image gallery feature.

Two runs with numeric metrics (loss, lr, grad_norm) plus two image galleries
(samples in color, other_samples in grayscale) logged every 1000 steps.
"""

import math
import time

import numpy as np
import kymo

PROJECT = "cdn-test-2"

TOTAL_STEPS = 10_000
IMAGE_INTERVAL = 1_000
IMAGES_PER_SAMPLE = 4


def make_test_image(step: int, idx: int, size: int = 256) -> np.ndarray:
    """Generate a colorful test image that visually changes with step."""
    img = np.zeros((size, size, 3), dtype=np.uint8)
    for y in range(size):
        for x in range(size):
            t = step / TOTAL_STEPS
            r = int(128 + 127 * math.sin(2 * math.pi * (x / size + t + idx * 0.25)))
            g = int(
                128 + 127 * math.sin(2 * math.pi * (y / size + t * 1.3 + idx * 0.15))
            )
            b = int(128 + 127 * math.sin(2 * math.pi * ((x + y) / size / 2 + t * 0.7)))
            img[y, x] = [r, g, b]
    img[:3, :] = img[-3:, :] = img[:, :3] = img[:, -3:] = 255
    freq = max(4, int(64 * (1 - step / TOTAL_STEPS)))
    for y in range(size):
        for x in range(size):
            if x % freq == 0 or y % freq == 0:
                img[y, x] = ((img[y, x].astype(np.int16) + 255) // 2).astype(np.uint8)
    return img


def to_grayscale(img: np.ndarray) -> np.ndarray:
    """Convert RGB image to grayscale (single channel)."""
    gray = (0.299 * img[:, :, 0] + 0.587 * img[:, :, 1] + 0.114 * img[:, :, 2]).astype(
        np.uint8
    )
    return gray


def run_one(run_name: str, seed: int):
    kymo.init(project_id=PROJECT, run_name=run_name)
    print(f"\n=== {run_name} ===")
    rng = np.random.RandomState(seed)

    for step in range(TOTAL_STEPS):
        progress = step / TOTAL_STEPS
        loss = (
            2.0 * math.exp(-3 * progress) + 0.1 + rng.normal(0, 0.02 * (1 - progress))
        )
        lr = 1e-4 * (1 - progress)
        grad_norm = 0.5 * math.exp(-progress) + rng.exponential(0.05)

        metrics: dict = {
            "loss": float(loss),
            "learning_rate": float(lr),
            "grad_norm": float(grad_norm),
        }

        if step % IMAGE_INTERVAL == 0:
            color_imgs = [
                kymo.Image(
                    make_test_image(step, i),
                    caption=f"sample {i} (step {step})",
                )
                for i in range(IMAGES_PER_SAMPLE)
            ]
            gray_imgs = [
                kymo.Image(
                    to_grayscale(make_test_image(step, i)),
                    caption=f"gray {i} (step {step})",
                )
                for i in range(IMAGES_PER_SAMPLE)
            ]
            metrics["samples"] = color_imgs
            metrics["other_samples"] = gray_imgs
            print(
                f"  step {step}: loss={loss:.4f}, + {IMAGES_PER_SAMPLE} color + {IMAGES_PER_SAMPLE} gray"
            )
        elif step % 2000 == 0:
            print(f"  step {step}: loss={loss:.4f}")

        kymo.log(metrics, step=step)

    print(f"  Waiting for {run_name} uploads...")
    if not kymo.wait_for_upload(timeout=300):
        raise RuntimeError(f"{run_name}: upload did not complete")
    print(f"  {run_name} done!")


def main():
    t0 = time.monotonic()
    run_one(f"run-a-{int(time.time()) % 10000}", seed=42)
    run_one(f"run-b-{int(time.time()) % 10000}", seed=123)
    elapsed = time.monotonic() - t0
    print(f"\nAll done in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
