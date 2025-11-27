#!/usr/bin/env python3
# --------------------------------------------------------------
# Violin + Swarm plot for burst anchoring results
# Input: test1_burstfix.csv (output from your anchoring test)
# Output: test1_burstfix_violin.png
#
# Requirements:
#   pip install pandas matplotlib seaborn numpy
# --------------------------------------------------------------

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# ------------------- Paths -------------------
BASE_DIR = "/home/fjaved/CE-SI/fresh-iota-sandbox/iota-sandbox/scripts/results"
CSV_PATH = os.path.join(BASE_DIR, "test1_burstfix.csv")
OUT_PATH_PNG = os.path.join(BASE_DIR, "/home/fjaved/CE-SI/fresh-iota-sandbox/iota-sandbox/scripts/results/test1_burstfix_violin.png")
OUT_PATH_SVG = os.path.join(BASE_DIR, "/home/fjaved/CE-SI/fresh-iota-sandbox/iota-sandbox/scripts/results/test1_burstfix_violin.svg")
OUT_PATH_SVG = os.path.join(BASE_DIR, "/home/fjaved/CE-SI/fresh-iota-sandbox/iota-sandbox/scripts/results/test1_burstfix_violin.pdf")

# ------------------- Load & Filter -------------------
df = pd.read_csv(CSV_PATH)
print(f"Loaded {len(df)} rows from {CSV_PATH}")

# Keep only successful (confirmed) commits
okdf = df[df["ok"] == True].copy()

# ------------------- Plot -------------------

plt.figure(figsize=(6, 5))
sns.violinplot(
    data=okdf,
    x="tier_commits",
    y="ttc_s",
    inner=None,
    color="royalblue",
    linewidth=1
)

plt.xlabel("Tier (Number of Commits per Run)")
plt.ylabel("Time to Confirmation (TTC) [s]")
plt.title("")
plt.tight_layout()
plt.savefig(OUT_PATH_PNG, dpi=300)
plt.savefig(OUT_PATH_SVG)
plt.show()

print(f"✅ Saved violin plot: {OUT_PATH}")
