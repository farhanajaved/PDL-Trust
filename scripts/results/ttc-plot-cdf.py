#!/usr/bin/env python3
# --------------------------------------------------------------
# True CDF plots (smoothed) for burst anchoring results
# Input : test1_burstfix.csv
# Output: test1_burstfix_cdf_panels.png
#
# Each subplot shows: CDF + mean, median, 95th percentile
#
# Requirements:
#   pip install pandas matplotlib seaborn numpy scipy
# --------------------------------------------------------------

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import norm

# ------------------- Paths -------------------
BASE_DIR = "/home/fjaved/CE-SI/fresh-iota-sandbox/iota-sandbox/scripts/results"
CSV_PATH = os.path.join(BASE_DIR, "test1_burstfix.csv")
OUT_PATH_PNG = os.path.join(BASE_DIR, "/home/fjaved/CE-SI/fresh-iota-sandbox/iota-sandbox/scripts/results/test1_burstfix_cdf_panels.png")
OUT_PATH_SVG = os.path.join(BASE_DIR, "/home/fjaved/CE-SI/fresh-iota-sandbox/iota-sandbox/scripts/results/test1_burstfix_cdf_panels.svg")


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
        continue

    mean = np.mean(vals)
    median = np.median(vals)
    p95 = np.percentile(vals, 95)
    std = np.std(vals)

    # Define range for smooth CDF curve
    x = np.linspace(min(vals), max(vals), 200)
    y = norm.cdf(x, loc=mean, scale=std)

    # CDF curve
    ax.plot(x, y, color="blue", lw=2, label="Anchoring TTC")

    # Mean, Median, 95th
    ax.axvline(mean, color="red", linestyle="--", label="Mean" if t == tiers[0] else "")
    ax.axvline(median, color="green", linestyle="dashdot", label="Median" if t == tiers[0] else "")
    ax.axvline(p95, color="blue", linestyle=":", label="95th Percentile" if t == tiers[0] else "")

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


    print(f"Tier {t}: mean={mean:.2f}s, median={median:.2f}s, 95th={p95:.2f}s, std={std:.2f}s, n={len(vals)}")

axes[0].set_ylabel("CDF")
axes[0].legend(loc="upper left", frameon=True)
plt.tight_layout(rect=[0, 0, 1, 0.96])
plt.savefig(OUT_PATH_PNG, dpi=300)
plt.savefig(OUT_PATH_SVG)
plt.show()

print(f"✅ Saved CDF panel plot: {OUT_PATH}")
