#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test3_plots_minimal.py — Generate only 3 key plots for Test 3:
1. TTC by Phase (Box)
2. Recovery TTC ECDF (Smoothed with Mean/Median/P95)
3. Decision Latency by Phase (Box)
"""

import argparse
import os
import sys
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

PHASE_ORDER = ["normal", "impaired", "offline", "recovery"]


# -------------------------------
# CLI
# -------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Generate P1-box, P2, and P3 plots for Test 3")
    p.add_argument("--base-dir",
                   default="/home/fjaved/CE-SI/fresh-iota-sandbox/iota-sandbox/scripts",
                   help="Base directory")
    p.add_argument("--results-dir", default="results",
                   help="Results subdirectory under base-dir")
    p.add_argument("--csv", default=None,
                   help="Path to CSV (default: <base-dir>/<results-dir>/test3_partition.csv)")
    p.add_argument("--dpi", type=int, default=300,
                   help="DPI for saved figures (default: 300)")
    return p.parse_args()


# -------------------------------
# Helpers
# -------------------------------
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def to_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def read_csv_or_die(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        print(f"ERROR: CSV not found: {path}", file=sys.stderr)
        sys.exit(1)
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"ERROR: failed to read CSV: {e}", file=sys.stderr)
        sys.exit(1)
    return df


# -------------------------------
# P1 — TTC by Phase (Box)
# -------------------------------
def plot_ttc_by_phase_box(df: pd.DataFrame, outdir: str, dpi: int):
    d = df[df.get("event", "") == "anchor_confirm"].copy()
    if d.empty:
        print("[P1] No anchor_confirm rows; skipping TTC by phase box.")
        return

    d["ttc_s"] = to_numeric(d.get("ttc_s", pd.Series(dtype=float)))
    d = d.dropna(subset=["ttc_s"])
    if d.empty:
        print("[P1] No numeric ttc_s; skipping.")
        return

    d["phase"] = d.get("phase", pd.Series([""] * len(d))).fillna("")
    d["phase"] = pd.Categorical(d["phase"], categories=PHASE_ORDER, ordered=True)

    fig, ax = plt.subplots(figsize=(6, 5))
    data = [d.loc[d["phase"] == ph, "ttc_s"].values for ph in PHASE_ORDER]
    labels = PHASE_ORDER
    if not any(len(a) for a in data):
        print("[P1] All phase bins empty; skipping box.")
        plt.close(fig)
        return

    bp = ax.boxplot(
        data,
        labels=labels,
        patch_artist=True,
        showfliers=True,
        widths=0.2,
        boxprops=dict(color="black"),
        medianprops=dict(color="black", linewidth=1.5),
        whiskerprops=dict(color="black"),
        capprops=dict(color="black")
    )

    # ✅ Set fill color manually
    for patch in bp['boxes']:
        patch.set_facecolor("royalblue")
        patch.set_alpha(0.85)

    ax.set_xlabel("Phase")
    ax.set_ylabel("TTC (s)")
    ax.grid(False)
    fig.tight_layout()

    for ext in ["png", "svg", "pdf"]:
        path = os.path.join(outdir, f"test3_ttc_by_phase_box.{ext}")
        fig.savefig(path, dpi=dpi)
    plt.close(fig)
    print("[P1] Wrote test3_ttc_by_phase_box.[png,svg]")


# -------------------------------
# P2 — Recovery TTC ECDF (Smoothed)
# -------------------------------
def plot_recovery_ttc_ecdf(df: pd.DataFrame, outdir: str, dpi: int):
    d = df[(df.get("event", "") == "anchor_confirm") & (df.get("phase", "") == "recovery")].copy()
    if d.empty:
        print("[P2] No recovery confirmations; skipping ECDF.")
        return

    d["ttc_s"] = to_numeric(d.get("ttc_s", pd.Series(dtype=float)))
    d = d.dropna(subset=["ttc_s"])
    if d.empty:
        print("[P2] No numeric ttc_s; skipping ECDF.")
        return

    vals = np.sort(d["ttc_s"].values)
    n = len(vals)
    y = np.arange(1, n + 1) / n

    mean_val = np.mean(vals)
    median_val = np.median(vals)
    p95 = np.percentile(vals, 95)

    # Smooth curve
    try:
        from scipy.interpolate import make_interp_spline
        x_smooth = np.linspace(vals.min(), vals.max(), 400)
        spline = make_interp_spline(vals, y, k=3)
        y_smooth = spline(x_smooth)
    except Exception:
        x_smooth, y_smooth = vals, y

    fig, ax = plt.subplots(figsize=(6, 5))
    ax.plot(x_smooth, y_smooth, lw=2.5, color="royalblue", label=r"$T_{\mathrm{TTC}\mid \mathrm{rec}}$ (s)")

    # Vertical lines for Mean / Median / P95
    ax.axvline(mean_val, color="red", linestyle="--", lw=1.5, label="Mean")
    ax.axvline(median_val, color="green", linestyle="-.", lw=1.5, label="Median")
    ax.axvline(p95, color="royalblue", linestyle=":", lw=1.5, label="95th Percentile")

    ax.set_xlabel(r"$T_{\mathrm{TTC}\mid \mathrm{rec}}$ (s)")
    ax.set_ylabel("CDF")
    ax.set_ylim(0, 1)
    ax.grid(False)
    ax.legend(frameon=True, fancybox=True, framealpha=0.9, edgecolor="gray", loc="lower right")

    fig.tight_layout()
    for ext in ["png", "svg", "pdf"]:
        path = os.path.join(outdir, f"test3_ttc_recovery_ecdf.{ext}")
        fig.savefig(path, dpi=dpi)
    plt.close(fig)
    print("[P2] Wrote test3_ttc_recovery_ecdf.[png,svg]")


# -------------------------------
# P3 — Decision Latency by Phase (Box)
# -------------------------------
def plot_decision_latency_box(df: pd.DataFrame, outdir: str, dpi: int):
    d = df[df.get("event", "") == "decision"].copy()
    if d.empty:
        print("[P3] No decision rows; skipping latency box.")
        return

    d["t_decide_ms"] = to_numeric(d.get("t_decide_ms", pd.Series(dtype=float)))
    d = d.dropna(subset=["t_decide_ms"])
    if d.empty:
        print("[P3] No numeric t_decide_ms; skipping.")
        return

    d["phase"] = d.get("phase", pd.Series([""] * len(d))).fillna("")
    d["phase"] = pd.Categorical(d["phase"], categories=PHASE_ORDER, ordered=True)

    fig, ax = plt.subplots(figsize=(6, 5))
    data = [d.loc[d["phase"] == ph, "t_decide_ms"].values for ph in PHASE_ORDER]
    labels = PHASE_ORDER
    if not any(len(a) for a in data):
        print("[P3] All bins empty; skipping.")
        plt.close(fig)
        return

    bp = ax.boxplot(
        data,
        labels=labels,
        patch_artist=True,
        showfliers=True,
        widths=0.2,
        boxprops=dict(color="black"),
        medianprops=dict(color="black", linewidth=1.5),
        whiskerprops=dict(color="black"),
        capprops=dict(color="black")
    )

    # ✅ Set fill color manually
    for patch in bp['boxes']:
        patch.set_facecolor("royalblue")
        patch.set_alpha(0.85)

    ax.set_xlabel("Phase")
    ax.set_ylabel("Decision latency (ms)")
    ax.grid(False)
    fig.tight_layout()

    for ext in ["png", "svg", "pdf"]:
        path = os.path.join(outdir, f"test3_tdecide_box.{ext}")
        fig.savefig(path, dpi=dpi)
    plt.close(fig)
    print("[P3] Wrote test3_tdecide_box.[png,svg, pdf]")


# -------------------------------
# Main
# -------------------------------
def main():
    args = parse_args()
    results_dir = os.path.join(args.base_dir, args.results_dir)
    ensure_dir(results_dir)

    csv_path = args.csv or os.path.join(results_dir, "test3_partition.csv")
    print(f"Reading: {csv_path}")
    df = read_csv_or_die(csv_path)

    plot_ttc_by_phase_box(df, results_dir, args.dpi)
    plot_recovery_ttc_ecdf(df, results_dir, args.dpi)
    plot_decision_latency_box(df, results_dir, args.dpi)

    print("Done (P1-box, P2, P3-only).")


if __name__ == "__main__":
    warnings.simplefilter("ignore", category=UserWarning)
    warnings.simplefilter("ignore", category=FutureWarning)
    main()
