"""
Layout-iteration script for the CDN image gallery.

Logs a few image groups for several runs, with a mix of aspect ratios
(square, wide, tall, panorama, portrait) so we can iterate on the
gallery CSS without needing real training data.

Cheap to run — numpy vectorized, no Python pixel loops.
"""

import math
import time

import numpy as np
import kymo

PROJECT = "gallery-layout"

NUM_RUNS = 4
STEPS_WITH_IMAGES = [0, 100, 200, 300]
IMAGES_PER_STEP = 5

# (w, h) per slot — mixed aspect ratios.
ASPECTS = [
    (256, 256),  # square
    (512, 256),  # 2:1 wide
    (256, 512),  # 1:2 tall
    (640, 192),  # ultra-wide / banner
    (192, 320),  # portrait
]


def make_image(w: int, h: int, step: int, run_idx: int, slot: int) -> np.ndarray:
    """Vectorized gradient + stripes, varies with step/run/slot."""
    xs = np.linspace(0, 1, w, dtype=np.float32)
    ys = np.linspace(0, 1, h, dtype=np.float32)
    xv, yv = np.meshgrid(xs, ys)

    t = step / max(1, max(STEPS_WITH_IMAGES))
    phase = run_idx * 0.7 + slot * 0.3

    r = 0.5 + 0.5 * np.sin(2 * math.pi * (xv + t + phase))
    g = 0.5 + 0.5 * np.sin(2 * math.pi * (yv + t * 1.3 + phase * 0.5))
    b = 0.5 + 0.5 * np.sin(2 * math.pi * ((xv + yv) * 0.5 + t * 0.7))

    img = np.stack([r, g, b], axis=-1)

    # Stripes whose frequency changes with step — gives a visible "moving" pattern.
    freq = 4 + int(20 * (1 - t))
    stripes = (np.arange(w) % freq == 0).astype(np.float32)
    img *= 1 - 0.4 * stripes[None, :, None]

    img = np.clip(img * 255, 0, 255).astype(np.uint8)

    # White border so individual tiles are visible in the grid.
    img[:2, :] = img[-2:, :] = img[:, :2] = img[:, -2:] = 255
    return img


def run_one(run_name: str, run_idx: int):
    kymo.init(project_id=PROJECT, run_name=run_name)
    print(f"\n=== {run_name} ===")

    for step in range(max(STEPS_WITH_IMAGES) + 1):
        metrics: dict = {
            "loss": float(2.0 * math.exp(-step / 200) + 0.1),
        }

        if step in STEPS_WITH_IMAGES:
            imgs = []
            for slot in range(IMAGES_PER_STEP):
                w, h = ASPECTS[slot % len(ASPECTS)]
                imgs.append(
                    kymo.Image(
                        make_image(w, h, step, run_idx, slot),
                        caption=f"{w}x{h} slot{slot}",
                    )
                )
            metrics["samples"] = imgs
            print(f"  step {step}: logged {IMAGES_PER_STEP} images")

        kymo.log(metrics, step=step)

    print(f"  waiting for {run_name} uploads...")
    if not kymo.wait_for_upload(timeout=120):
        raise RuntimeError(f"{run_name}: upload did not complete")
    print(f"  {run_name} done")


def main():
    t0 = time.monotonic()
    suffix = int(time.time()) % 10000
    for i in range(NUM_RUNS):
        run_one(f"run-{chr(ord('a') + i)}-{suffix}", run_idx=i)
    print(f"\nAll {NUM_RUNS} runs done in {time.monotonic() - t0:.1f}s")


if __name__ == "__main__":
    main()
