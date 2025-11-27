#!/usr/bin/env python3
# --------------------------------------------------------------
# Simple empirical CDF plots for burst anchoring results
# Input : test1_burstfix.csv
# Output: test1_burstfix_cdf_panels_simple.png / .svg
#
# Each subplot shows: empirical CDF + mean, median, 95th percentile
# --------------------------------------------------------------

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ------------------- Paths -------------------
BASE_DIR = "/home/fjaved/CE-SI/fresh-iota-sandbox/iota-sandbox/scripts/results"
CSV_PATH = os.path.join(BASE_DIR, "test1_burstfix.csv")
OUT_PATH_PNG = os.path.join(BASE_DIR, "test1_burstfix_cdf_panels_simple.png")
OUT_PATH_SVG = os.path.join(BASE_DIR, "test1_burstfix_cdf_panels_simple.svg")
OUT_PATH_SVG = os.path.join(BASE_DIR, "test1_burstfix_cdf_panels_simple.pdf")

# ------------------- Load & Filter -------------------
df = pd.read_csv(CSV_PATH)
print(f"Loaded {len(df)} rows from {CSV_PATH}")
okdf = df[df["ok"] == True].copy()

# ------------------- Plot -------------------
tiers = [10, 30, 50, 100]
fig, axes = plt.subplots(1, len(tiers), figsize=(14, 4), sharey=True)

for ax, t in zip(axes, tiers):
    vals = okdf[okdf["tier_commits"] == t]["ttc_s"].dropna().values
    if len(vals) == 0:
        ax.set_visible(False)
        continue

    vals = np.sort(vals)
    y = np.arange(1, len(vals) + 1) / len(vals)

    mean = np.mean(vals)
    median = np.median(vals)
    p95 = np.percentile(vals, 95)

    # Empirical CDF
    ax.step(vals, y, where="post", color="royalblue", lw=2, label="Anchoring TTC")

    # Reference lines
    if t == tiers[0]:
        ax.axvline(mean, color="red", linestyle="--", label="Mean")
        ax.axvline(median, color="green", linestyle="dashdot", label="Median")
        ax.axvline(p95, color="royalblue", linestyle=":", label="95th Percentile")
    else:
        ax.axvline(mean, color="red", linestyle="--")
        ax.axvline(median, color="green", linestyle="dashdot")
        ax.axvline(p95, color="royalblue", linestyle=":")

      # Label box
    ax.text(
        0.95, 0.1, f"$L_t = {t}$",
        transform=ax.transAxes,
        fontsize=10,
        ha="right", va="bottom",
        bbox=dict(facecolor="white", edgecolor="gray", boxstyle="round,pad=0.3")
    )


    ax.set_xlabel("Time to Confirmation (TTC) [s]")
    ax.set_xlim(left=max(0, np.min(vals) - 0.2), right=np.max(vals) + 0.5)
    ax.set_ylim(0, 1)

    print(f"Tier {t}: mean={mean:.2f}s, median={median:.2f}s, 95th={p95:.2f}s, n={len(vals)}")

axes[0].set_ylabel("CDF")
axes[0].legend(loc="upper left", frameon=True)

plt.tight_layout(rect=[0, 0, 1, 0.96])
plt.savefig(OUT_PATH_PNG, dpi=300)
plt.savefig(OUT_PATH_SVG)
# plt.show()  # Uncomment if running locally

print(f"✅ Saved empirical CDF panel plots:\n   {OUT_PATH_PNG}\n   {OUT_PATH_SVG}")
