#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test3_plots.py — P1–P5 visuals for Test 3 (Partition Tolerance)

Inputs
------
CSV expected at:
  <base-dir>/<results-dir>/test3_partition.csv
Defaults:
  base-dir   = /home/fjaved/CE-SI/fresh-iota-sandbox/iota-sandbox/scripts
  results-dir= results

Outputs (saved into <base-dir>/<results-dir>/):
  P1  test3_ttc_by_phase_violin.png
      test3_ttc_by_phase_box.png
  P2  test3_ttc_recovery_ecdf.png
  P3  test3_queue_depth_over_time.png
  P4  test3_send_confirm_timeline.png
  P5  test3_tdecide_box.png
  Summary: test3_summary.txt

Columns used if present:
  event, phase, t_rel_s, ttc_s, queued_after, t_decide_ms, batch_id, block_id, note,
  snapshot_ok, cache_age_before_s, cache_age_after_s

Notes
-----
- Handles missing categories gracefully (e.g., no confirmations during 'offline').
- Uses seaborn theme for consistency with earlier figures.
"""

import argparse
import os
import sys
import math
import warnings

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Seaborn is used only for theming; plots are pure Matplotlib.
try:
    import seaborn as sns
    sns.set_theme(context="paper", style="", font_scale=1.0)
except Exception:
    warnings.warn("seaborn not available; continuing with default Matplotlib style")

PHASE_ORDER = ["normal", "impaired", "offline", "recovery"]


# -------------------------------
# CLI
# -------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Generate P1–P5 plots for Test 3")
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


def get_phase_spans(df: pd.DataFrame):
    """
    Returns list of tuples (start_time, end_time, phase_name) derived from
    phase_change events. Uses t_rel_s and 'note' text (contains 'enter <phase>').
    """
    pc = df[df.get("event", "") == "phase_change"].copy()
    if pc.empty or "t_rel_s" not in pc.columns:
        return []

    pc["t_rel_s"] = to_numeric(pc["t_rel_s"])
    pc = pc.dropna(subset=["t_rel_s"]).sort_values("t_rel_s")
    starts = pc["t_rel_s"].tolist()
    notes = pc.get("note", pd.Series([""] * len(pc))).fillna("").tolist()

    labels = []
    for n in notes:
        name = None
        for ph in PHASE_ORDER:
            if f"enter {ph}" in n:
                name = ph
                break
        labels.append(name or "phase")

    spans = []
    for i, t in enumerate(starts):
        name = labels[i]
        end = starts[i + 1] if i + 1 < len(starts) else None
        spans.append((t, end, name))
    return spans


def ecdf_from_array(vals: np.ndarray):
    x = np.sort(vals)
    y = np.arange(1, len(x) + 1) / len(x)
    return x, y


def print_counts(df: pd.DataFrame):
    def cnt(mask):
        return int(mask.sum())
    print("Row counts by view:")
    m_conf = (df.get("event", "") == "anchor_confirm")
    m_send = (df.get("event", "") == "anchor_send")
    m_q    = (df.get("event", "") == "queue_depth")
    m_dec  = (df.get("event", "") == "decision")
    m_pc   = (df.get("event", "") == "phase_change")
    print(f"  anchor_confirm: {cnt(m_conf)}")
    print(f"  anchor_send   : {cnt(m_send)}")
    print(f"  queue_depth   : {cnt(m_q)}")
    print(f"  decision      : {cnt(m_dec)}")
    print(f"  phase_change  : {cnt(m_pc)}")


# -------------------------------
# P1 – TTC by phase (violin + box)
# -------------------------------
def plot_ttc_by_phase(df: pd.DataFrame, outdir: str, dpi: int):
    d = df[df.get("event", "") == "anchor_confirm"].copy()
    if d.empty:
        print("[P1] No anchor_confirm rows; skipping TTC by phase.")
        return

    d["ttc_s"] = to_numeric(d.get("ttc_s", pd.Series(dtype=float)))
    d = d.dropna(subset=["ttc_s"])
    if d.empty:
        print("[P1] No numeric ttc_s; skipping.")
        return

    # Ensure phase order and include missing as empty bins
    d["phase"] = d.get("phase", pd.Series([""] * len(d))).fillna("")
    d["phase"] = pd.Categorical(d["phase"], categories=PHASE_ORDER, ordered=True)

    # Violin
    try:
        import seaborn as sns  # only for this plot
        fig, ax = plt.subplots(figsize=(6, 5))
        sns.violinplot(data=d, x="phase", y="ttc_s", order=PHASE_ORDER, cut=0, inner="quartile", ax=ax)
        ax.set_xlabel("Phase")
        ax.set_ylabel("TTC (s)")
        fig.tight_layout()
        path = os.path.join(outdir, "test3_ttc_by_phase_violin.png")
        fig.savefig(path, dpi=dpi)
        plt.close(fig)
        print(f"[P1] Wrote {path}")
    except Exception as e:
        print(f"[P1] Violin failed ({e}); falling back to box only.")

    # Box
    fig, ax = plt.subplots(figsize=(6, 5))
    data = [d.loc[d["phase"] == ph, "ttc_s"].values for ph in PHASE_ORDER]
    labels = PHASE_ORDER
    # Handle the case where all are empty
    if not any(len(a) for a in data):
        print("[P1] All phase bins empty; skipping box.")
        plt.close(fig)
        return

    bp = ax.boxplot(data, labels=labels, patch_artist=True, showfliers=True)
    for b in bp["boxes"]:
        b.set_alpha(0.9)
    ax.set_xlabel("Phase")
    ax.set_ylabel("TTC (s)")
    fig.tight_layout()
    path = os.path.join(outdir, "test3_ttc_by_phase_box.png")
    fig.savefig(path, dpi=dpi)
    plt.close(fig)
    print(f"[P1] Wrote {path}")


# -------------------------------
# P2 – Recovery TTC ECDF
# -------------------------------
def plot_recovery_ttc_ecdf(df: pd.DataFrame, outdir: str, dpi: int):
    d = df[(df.get("event", "") == "anchor_confirm") & (df.get("phase", "") == "recovery")].copy()
    if d.empty:
        print("[P2] No recovery confirmations; skipping.")
        return
    d["ttc_s"] = to_numeric(d.get("ttc_s", pd.Series(dtype=float)))
    d = d.dropna(subset=["ttc_s"])
    if d.empty:
        print("[P2] No numeric ttc_s; skipping.")
        return

    vals = d["ttc_s"].values
    x, y = ecdf_from_array(vals)
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.plot(x, y, lw=2)
    ax.set_xlabel("Anchor TTC during recovery (s)")
    ax.set_ylabel("ECDF")
    ax.set_ylim(0, 1)
    fig.tight_layout()
    path = os.path.join(outdir, "test3_ttc_recovery_ecdf.png")
    fig.savefig(path, dpi=dpi)
    plt.close(fig)
    print(f"[P2] Wrote {path}")


# -------------------------------
# P3 – Queue depth over time with phase bands
# -------------------------------
def plot_queue_depth_over_time(df: pd.DataFrame, outdir: str, dpi: int):
    q = df[df.get("event", "") == "queue_depth"].copy()
    if q.empty:
        print("[P3] No queue_depth rows; skipping.")
        return
    q["t_rel_s"] = to_numeric(q.get("t_rel_s", pd.Series(dtype=float)))
    q["queued_after"] = to_numeric(q.get("queued_after", pd.Series(dtype=float)))
    q = q.dropna(subset=["t_rel_s", "queued_after"])
    if q.empty:
        print("[P3] No numeric queue_depth rows; skipping.")
        return

    spans = get_phase_spans(df)
    t_end = float(df.get("t_rel_s", pd.Series([0.0])).astype(float).max()) if "t_rel_s" in df.columns else float(q["t_rel_s"].max())

    fig, ax = plt.subplots(figsize=(6, 5))
    # Background bands
    for i, (start, end, name) in enumerate(spans):
        end = end if end is not None else t_end
        if start is None:
            continue
        ax.axvspan(start, end, alpha=0.08, label=name if i == 0 else None)
    ax.plot(q["t_rel_s"], q["queued_after"], lw=1.8)
    ax.set_xlabel("Time since start (s)")
    ax.set_ylabel("Queue size (batches)")
    if spans:
        ax.legend(frameon=False, fontsize=8)
    fig.tight_layout()
    path = os.path.join(outdir, "test3_queue_depth_over_time.png")
    fig.savefig(path, dpi=dpi)
    plt.close(fig)
    print(f"[P3] Wrote {path}")


# -------------------------------
# P4 – Send vs confirm timeline
# -------------------------------
def plot_send_confirm_timeline(df: pd.DataFrame, outdir: str, dpi: int):
    s = df[df.get("event", "") == "anchor_send"].copy()
    c = df[df.get("event", "") == "anchor_confirm"].copy()
    if s.empty and c.empty:
        print("[P4] No send/confirm rows; skipping.")
        return

    for z in (s, c):
        if not z.empty:
            z["t_rel_s"] = to_numeric(z.get("t_rel_s", pd.Series(dtype=float)))
            z.dropna(subset=["t_rel_s"], inplace=True)

    fig, ax = plt.subplots(figsize=(5, 6))
    if not s.empty:
        ax.scatter(s["t_rel_s"], np.zeros(len(s)), s=14, label="send", alpha=0.8)
    if not c.empty:
        ax.scatter(c["t_rel_s"], np.ones(len(c)), s=14, label="confirm", alpha=0.8)

    # Vertical line at recovery start
    spans = get_phase_spans(df)
    for (start, end, name) in spans:
        if name == "recovery":
            ax.axvline(start, linestyle="--", lw=1.2, color="k", alpha=0.8)
            break

    ax.set_yticks([0, 1])
    ax.set_yticklabels(["send", "confirm"])
    ax.set_xlabel("Time since start (s)")
    ax.set_title("Anchors: send vs confirm timeline")
    ax.legend(frameon=False)
    fig.tight_layout()
    path = os.path.join(outdir, "test3_send_confirm_timeline.png")
    fig.savefig(path, dpi=dpi)
    plt.close(fig)
    print(f"[P4] Wrote {path}")


# -------------------------------
# P5 – Local decision latency by phase
# -------------------------------
def plot_decision_latency_box(df: pd.DataFrame, outdir: str, dpi: int):
    d = df[df.get("event", "") == "decision"].copy()
    if d.empty:
        print("[P5] No decision rows; skipping.")
        return
    d["t_decide_ms"] = to_numeric(d.get("t_decide_ms", pd.Series(dtype=float)))
    d = d.dropna(subset=["t_decide_ms"])
    if d.empty:
        print("[P5] No numeric t_decide_ms; skipping.")
        return
    d["phase"] = d.get("phase", pd.Series([""] * len(d))).fillna("")
    d["phase"] = pd.Categorical(d["phase"], categories=PHASE_ORDER, ordered=True)

    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    data = [d.loc[d["phase"] == ph, "t_decide_ms"].values for ph in PHASE_ORDER]
    labels = PHASE_ORDER
    if not any(len(a) for a in data):
        print("[P5] All bins empty; skipping.")
        plt.close(fig)
        return

    bp = ax.boxplot(data, labels=labels, patch_artist=True, showfliers=True)
    for b in bp["boxes"]:
        b.set_alpha(0.9)
    ax.set_xlabel("Phase")
    ax.set_ylabel("Decision latency (ms)")
    fig.tight_layout()
    path = os.path.join(outdir, "test3_tdecide_box.png")
    fig.savefig(path, dpi=dpi)
    plt.close(fig)
    print(f"[P5] Wrote {path}")


# -------------------------------
# Summary text
# -------------------------------
def write_summary(df: pd.DataFrame, outdir: str):
    lines = []
    lines.append("Test 3 — Summary")
    lines.append("-----------------")

    # TTC med & P95 by phase
    d = df[df.get("event", "") == "anchor_confirm"].copy()
    if not d.empty:
        d["ttc_s"] = to_numeric(d.get("ttc_s", pd.Series(dtype=float)))
        d = d.dropna(subset=["ttc_s"])
        lines.append("TTC by phase (median / P95) [s]:")
        for ph in PHASE_ORDER:
            vals = d.loc[d.get("phase", "") == ph, "ttc_s"].values
            if vals.size == 0:
                lines.append(f"  - {ph:8s}: n=0")
            else:
                med = float(np.median(vals))
                p95 = float(np.percentile(vals, 95))
                lines.append(f"  - {ph:8s}: n={vals.size:4d}  median={med:.3f}  P95={p95:.3f}")
    else:
        lines.append("No anchor_confirm rows found.")

    # Recovery TTC median
    rc = d.loc[d.get("phase", "") == "recovery", "ttc_s"].values if not d.empty else np.array([])
    if rc.size > 0:
        lines.append(f"Recovery TTC median: {float(np.median(rc)):.3f} s")
    else:
        lines.append("Recovery TTC median: n/a")

    # Max queue depth
    q = df[df.get("event", "") == "queue_depth"].copy()
    if not q.empty:
        q["queued_after"] = to_numeric(q.get("queued_after", pd.Series(dtype=float)))
        q = q.dropna(subset=["queued_after"])
        if not q.empty:
            lines.append(f"Max observed queue depth: {int(q['queued_after'].max())}")
        else:
            lines.append("Max observed queue depth: n/a")
    else:
        lines.append("Max observed queue depth: n/a")

    # Timeouts per phase
    tmo = df[df.get("event", "") == "anchor_timeout"].copy()
    if not tmo.empty:
        counts = tmo.groupby(tmo.get("phase", pd.Series([""] * len(tmo)))).size().to_dict()
        lines.append("anchor_timeout counts:")
        if counts:
            for k in PHASE_ORDER:
                if k in counts:
                    lines.append(f"  - {k}: {counts[k]}")
        else:
            lines.append("  - none")
    else:
        lines.append("anchor_timeout counts: none")

    # Catch-up time from recovery start: last confirm after recovery – recovery start
    spans = get_phase_spans(df)
    recovery_start = None
    for (start, end, name) in spans:
        if name == "recovery":
            recovery_start = start
            break
    if recovery_start is not None:
        conf = df[(df.get("event", "") == "anchor_confirm")].copy()
        if not conf.empty:
            conf["t_rel_s"] = to_numeric(conf.get("t_rel_s", pd.Series(dtype=float)))
            conf = conf.dropna(subset=["t_rel_s"])
            after = conf.loc[conf["t_rel_s"] >= recovery_start, "t_rel_s"]
            if not after.empty:
                catchup = float(after.max() - recovery_start)
                lines.append(f"Catch-up time from recovery start: {catchup:.3f} s")
            else:
                lines.append("Catch-up time from recovery start: n/a (no confirms after recovery)")
        else:
            lines.append("Catch-up time from recovery start: n/a")
    else:
        lines.append("Catch-up time from recovery start: n/a (no recovery span found)")

    outpath = os.path.join(outdir, "test3_summary.txt")
    with open(outpath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"[Summary] Wrote {outpath}")


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

    # Basic helpful prints
    print_counts(df)

    # Generate plots
    plot_ttc_by_phase(df, results_dir, args.dpi)            # P1
    plot_recovery_ttc_ecdf(df, results_dir, args.dpi)       # P2
    plot_queue_depth_over_time(df, results_dir, args.dpi)   # P3
    plot_send_confirm_timeline(df, results_dir, args.dpi)   # P4
    plot_decision_latency_box(df, results_dir, args.dpi)    # P5

    # Summary
    write_summary(df, results_dir)

    print("Done.")


if __name__ == "__main__":
    # Quiet some pandas warnings for cleaner logs
    warnings.simplefilter("ignore", category=UserWarning)
    warnings.simplefilter("ignore", category=FutureWarning)
    main()
