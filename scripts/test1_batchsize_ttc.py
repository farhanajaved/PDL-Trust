#!/usr/bin/env python3
# --------------------------------------------------------------
# Test 1 — Gateway-driven anchoring under realistic CE load
# Fixed coordinator cadence (default ~10 s); vary commit volume.
#
# Patterns:
#   - staggered: Poisson-like arrivals of device "decisions"; batch 20 → 1 commit
#   - burst: short spikes that quickly form batches of 20 → commits
#
# Outputs:
#   - CSV: scripts/results/test1_batchsize_ttc.csv
#   - Plots:
#       scripts/results/test1_batchsize_boxplot.png
#       scripts/results/test1_batchsize_ecdf_staggered.png
#       scripts/results/test1_batchsize_ecdf_burst.png
#
# Requires: requests, pandas, matplotlib, numpy
#   pip install requests pandas matplotlib numpy
# --------------------------------------------------------------

import os
import time
import json
import math
import random
import hashlib
import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timezone

# ------------------- User settings -------------------

NODE_URL = "http://172.18.211.11:14265"   # Your working Hornet v2 endpoint
CADENCE_SEC = 10                          # We keep the default coordinator cadence
BASE_DIR = "/home/fjaved/CE-SI/fresh-iota-sandbox/iota-sandbox/scripts"
RESULTS_DIR = os.path.join(BASE_DIR, "results")
CSV_PATH = os.path.join(RESULTS_DIR, "test1_batchsize_ttc.csv")

# Load tiers = number of commits (anchoring transactions) per run
TIERS = [10, 30, 50, 100]                 # LCE-10/30/50/100

# Arrival patterns config (tweak if you like)
STAGGERED_MEAN_DECISION_SEC = 0.15        # mean inter-decision (Poisson); 20 decisions → 1 commit
BURST_GROUP_COMMITS = 5                   # commits per burst group
BURST_GROUP_GAP_SEC = 1.0                 # gap between groups
BURST_INTRA_COMMIT_GAP_SEC = 0.02         # small spacing within a burst group

BATCH_SIZE_DECISIONS = 20                 # 20 decisions per LedgerCommit
TAG_PREFIX = "gateway"                    # will be hex-encoded
TIMEOUT_SEC = 60                          # per-commit confirmation timeout
POLL_INTERVAL_SEC = 1.0                   # poll metadata once per second

# Repeats per tier per pattern (you can raise later)
REPEATS = 1

# ------------------- Helpers -------------------

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def api_info():
    r = requests.get(f"{NODE_URL}/api/core/v2/info", timeout=5)
    r.raise_for_status()
    return r.json()

def seconds_to_next_milestone(cadence=CADENCE_SEC):
    """
    Use node's latestMilestone.timestamp (epoch seconds) to estimate time to next tick.
    Assumes stable cadence. Good enough for explanatory covariate in plots.
    """
    info = api_info()
    latest_ts = info["status"]["latestMilestone"]["timestamp"]
    now_s = int(time.time())
    delta = (now_s - latest_ts) % cadence
    to_next = cadence - delta if delta != 0 else 0
    return float(to_next)

def make_commit_payload(batch_size=BATCH_SIZE_DECISIONS) -> bytes:
    """
    Build ~2.5 KB JSON: includes merkleRoot, policyDigest, version, batchSize, and padded 'items'.
    """
    # synthetic digests
    root = hashlib.sha256(f"root-{time.time()}".encode()).hexdigest()
    pol = hashlib.sha256(f"policy-{time.time()}".encode()).hexdigest()

    # build 20 small decision entries
    items = []
    for i in range(batch_size):
        it = {
            "d": hashlib.sha256(f"dev-{i}-{time.time()}".encode()).hexdigest()[:16],
            "act": random.choice(["allow","deny","throttle"]),
            "v": random.randint(1,5)
        }
        items.append(it)

    # assemble json
    obj = {
        "type": "LedgerCommit",
        "version": "1.0",
        "merkleRoot": root,
        "policyDigest": pol,
        "batchSize": batch_size,
        "createdAt": now_iso(),
        "items": items
    }
    b = json.dumps(obj, separators=(",",":")).encode()

    # pad up to ~2.5 KB if smaller (purely to stabilize byte size)
    target = 2500
    if len(b) < target:
        pad = target - len(b)
        b = b + b" " * pad
    return b

def post_tagged_data(data_bytes: bytes, tag_ascii: str) -> str:
    tag_hex = "0x" + tag_ascii.encode().hex()
    data_hex = "0x" + data_bytes.hex()
    payload = {
        "protocolVersion": 2,
        "payload": {
            "type": 5,
            "tag": tag_hex,
            "data": data_hex
        }
    }
    r = requests.post(f"{NODE_URL}/api/core/v2/blocks", json=payload, timeout=10)
    r.raise_for_status()
    j = r.json()
    return j.get("blockId")

def get_block_metadata(block_id: str):
    r = requests.get(f"{NODE_URL}/api/core/v2/blocks/{block_id}/metadata", timeout=5)
    r.raise_for_status()
    return r.json()

def confirm_ttc(block_id: str, timeout_s=TIMEOUT_SEC, poll_interval=POLL_INTERVAL_SEC):
    t0 = time.time()
    ms_index = None
    while time.time() - t0 < timeout_s:
        try:
            meta = get_block_metadata(block_id)
            ms_index = meta.get("referencedByMilestoneIndex")
            if ms_index is not None:
                return (time.time() - t0, ms_index, True)
        except Exception:
            pass
        time.sleep(poll_interval)
    return (None, ms_index, False)

# ------------------- Patterns -------------------

def run_staggered(tier_commits: int, run_id: int, pattern_label: str):
    """
    Poisson-like device decisions: exponential inter-arrivals with mean STAGGERED_MEAN_DECISION_SEC.
    Every 20 decisions → form a commit and anchor.
    """
    rows = []
    decisions = 0
    commits_done = 0
    print(f"\n[staggered] run={run_id} target_commits={tier_commits}")

    while commits_done < tier_commits:
        # simulate arrivals until batch fills
        batch_start = time.time()
        needed = BATCH_SIZE_DECISIONS
        total_sleep = 0.0
        for _ in range(needed):
            dt = random.expovariate(1.0 / STAGGERED_MEAN_DECISION_SEC)  # mean
            time.sleep(dt)
            total_sleep += dt
            decisions += 1

        # batch ready -> anchor one commit
        to_next_ms = seconds_to_next_milestone(CADENCE_SEC)
        data = make_commit_payload(BATCH_SIZE_DECISIONS)
        t_send = time.time()
        try:
            block_id = post_tagged_data(data, TAG_PREFIX)
        except Exception as e:
            print(f"❌ post failed: {e}")
            block_id = None

        ms_idx = None
        ttc = None
        ok = False
        if block_id:
            ttc, ms_idx, ok = confirm_ttc(block_id)

        commits_done += 1
        rows.append({
            "pattern": pattern_label,
            "tier_commits": tier_commits,
            "run_id": run_id,
            "commit_idx": commits_done,
            "blockId": block_id,
            "ok": ok,
            "ttc_s": ttc,
            "msIndex": ms_idx,
            "payload_bytes": len(data),
            "batch_size": BATCH_SIZE_DECISIONS,
            "delta_to_next_ms_s": to_next_ms,
            "batch_wait_s": total_sleep
        })
        if ok:
            print(f"✅ commit {commits_done}/{tier_commits}  TTC={ttc:.2f}s  ms={ms_idx}  Δ→MS={to_next_ms:.1f}s")
        else:
            print(f"⚠️ commit {commits_done}/{tier_commits}  not confirmed within timeout")

        # brief idle to avoid tight coupling to milestone edge
        time.sleep(0.05)

    return rows

def run_burst(tier_commits: int, run_id: int, pattern_label: str):
    """
    Burst groups: quickly create commits in small groups, then a short gap.
    """
    rows = []
    commits_done = 0
    print(f"\n[burst] run={run_id} target_commits={tier_commits}")

    while commits_done < tier_commits:
        # one burst group
        group = min(BURST_GROUP_COMMITS, tier_commits - commits_done)
        for _ in range(group):
            # quick batch of 20 (simulate they just arrived together)
            data = make_commit_payload(BATCH_SIZE_DECISIONS)
            to_next_ms = seconds_to_next_milestone(CADENCE_SEC)
            try:
                block_id = post_tagged_data(data, TAG_PREFIX)
            except Exception as e:
                print(f"❌ post failed: {e}")
                block_id = None

            ms_idx = None
            ttc = None
            ok = False
            if block_id:
                ttc, ms_idx, ok = confirm_ttc(block_id)

            commits_done += 1
            rows.append({
                "pattern": pattern_label,
                "tier_commits": tier_commits,
                "run_id": run_id,
                "commit_idx": commits_done,
                "blockId": block_id,
                "ok": ok,
                "ttc_s": ttc,
                "msIndex": ms_idx,
                "payload_bytes": len(data),
                "batch_size": BATCH_SIZE_DECISIONS,
                "delta_to_next_ms_s": to_next_ms,
                "batch_wait_s": 0.0
            })
            if ok:
                print(f"✅ commit {commits_done}/{tier_commits}  TTC={ttc:.2f}s  ms={ms_idx}  Δ→MS={to_next_ms:.1f}s")
            else:
                print(f"⚠️ commit {commits_done}/{tier_commits}  not confirmed within timeout")

            time.sleep(BURST_INTRA_COMMIT_GAP_SEC)

        # gap between bursts
        time.sleep(BURST_GROUP_GAP_SEC)

    return rows

# ------------------- Plots -------------------

def plot_grouped_boxplot(df: pd.DataFrame, outpath: str):
    okdf = df[df["ok"] == True]
    tiers = sorted(okdf["tier_commits"].unique())
    patterns = sorted(okdf["pattern"].unique())
    plt.figure(figsize=(7.0,4.2))
    # produce grouped boxplot: for each tier, show two boxes (staggered, burst)
    positions = []
    data = []
    labels = []
    gap = 0.8
    width = 0.3
    pos = 1
    for t in tiers:
        for p in patterns:
            vals = okdf[(okdf["tier_commits"]==t) & (okdf["pattern"]==p)]["ttc_s"].dropna().values
            if len(vals) == 0: continue
            positions.append(pos)
            data.append(vals)
            labels.append(f"{t}\n{p[0].upper()}")
            pos += 1
        pos += gap
    plt.boxplot(data, positions=positions, widths=width, patch_artist=True, showfliers=True)
    plt.xticks(positions, labels)
    plt.ylabel("TTC (s)")
    plt.title("Anchoring TTC vs Load (cadence fixed at 10 s)")
    plt.tight_layout()
    plt.savefig(outpath, dpi=220)
    plt.close()
    print(f"📈 Saved grouped boxplot: {outpath}")

def plot_ecdf_by_pattern(df: pd.DataFrame, pattern: str, outpath: str):
    okdf = df[(df["ok"] == True) & (df["pattern"] == pattern)]
    tiers = sorted(okdf["tier_commits"].unique())
    plt.figure(figsize=(6.4,4.2))
    for t in tiers:
        vals = np.sort(okdf[okdf["tier_commits"]==t]["ttc_s"].dropna().values)
        if len(vals) == 0: 
            continue
        y = np.arange(1, len(vals)+1) / len(vals)
        plt.plot(vals, y, label=f"{t} commits")
    plt.axvline(CADENCE_SEC, linestyle="--", alpha=0.7)
    plt.xlabel("TTC (s)")
    plt.ylabel("ECDF")
    plt.title(f"ECDF of TTC — {pattern}")
    plt.legend()
    plt.tight_layout()
    plt.savefig(outpath, dpi=220)
    plt.close()
    print(f"📈 Saved ECDF ({pattern}): {outpath}")

# ------------------- Main -------------------

def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    print(f"Using node: {NODE_URL}")
    # Warm-up: ensure node is healthy and we have a couple of milestones
    info = api_info()
    print(f"Node healthy={info['status']['isHealthy']}  network={info['protocol']['networkName']}  cadence≈{CADENCE_SEC}s")
    print("Warming up for 3 milestones (~30 s)...")
    time.sleep(3 * CADENCE_SEC)

    all_rows = []
    patterns = [("staggered", run_staggered), ("burst", run_burst)]

    for pattern_label, runner in patterns:
        for tier in TIERS:
            for rep in range(1, REPEATS+1):
                print(f"\n=== pattern={pattern_label} tier={tier} rep={rep} ===")
                rows = runner(tier, rep, pattern_label)
                all_rows.extend(rows)
                # brief idle between runs (~1 milestone)
                time.sleep(CADENCE_SEC)

    df = pd.DataFrame(all_rows)
    df.to_csv(CSV_PATH, index=False)
    print(f"\n💾 Wrote {CSV_PATH} (rows={len(df)})")

    # plots
    box_path = os.path.join(RESULTS_DIR, "test1_batchsize_boxplot.png")
    plot_grouped_boxplot(df, box_path)

    plot_ecdf_by_pattern(df, "staggered", os.path.join(RESULTS_DIR, "test1_batchsize_ecdf_staggered.png"))
    plot_ecdf_by_pattern(df, "burst", os.path.join(RESULTS_DIR, "test1_batchsize_ecdf_burst.png"))

    # brief stats per (pattern, tier)
    okdf = df[df["ok"]==True]
    print("\nSummary (per pattern × tier):")
    for (p, t), grp in okdf.groupby(["pattern","tier_commits"]):
        vals = grp["ttc_s"].dropna().values
        if len(vals)==0: 
            continue
        p50 = np.percentile(vals, 50)
        p95 = np.percentile(vals, 95)
        p99 = np.percentile(vals, 99)
        over20 = float(np.mean(vals > 2*CADENCE_SEC))*100.0
        print(f"  [{p:9s} | {t:3d}] n={len(vals)}  P50={p50:.2f}s  P95={p95:.2f}s  P99={p99:.2f}s  >20s={over20:.1f}%")

if __name__ == "__main__":
    main()
