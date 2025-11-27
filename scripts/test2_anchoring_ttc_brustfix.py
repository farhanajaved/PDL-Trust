#!/usr/bin/env python3
# --------------------------------------------------------------
# Test 1 — Anchoring vs Load (fixed 10 s cadence), with true bursts
# - Hornet v2 REST
# - Burst arrivals only (true concurrent anchoring)
# - 20 decisions -> 1 LedgerCommit (~2.5 KB)
# - Background polling for bursts (non-serialized confirmation)
# - Polling resolution 0.25 s
# - Records delta_to_next_ms at *send time* + observed ms_interval
#
# Outputs:
#   CSV  : scripts/results/test1_burstfix.csv
#   Plot : scripts/results/test1_burstfix_ecdf_burst.png
#
# Requirements: pip install requests pandas numpy matplotlib
# --------------------------------------------------------------

import os
import time
import json
import random
import hashlib
import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timezone

# ------------------- Paths & Node -------------------

NODE_URL = "http://172.18.211.11:14265"   # Hornet node endpoint
CADENCE_SEC = 10                          # Default coordinator cadence
BASE_DIR = "/home/fjaved/CE-SI/fresh-iota-sandbox/iota-sandbox/scripts"
RESULTS_DIR = os.path.join(BASE_DIR, "results")
CSV_PATH = os.path.join(RESULTS_DIR, "test1_burstfix.csv")

# ------------------- Experiment knobs -------------------

TIERS = [10, 30, 50, 100]     # number of LedgerCommits to send per tier
REPEATS = 10                  # repeat 10× for statistical significance

BATCH_SIZE_DECISIONS = 20
TARGET_BYTES = 2500

BURST_GROUP_COMMITS = 5
BURST_INTRA_COMMIT_GAP_SEC = 0.02
BURST_GROUP_GAP_SEC = 1.0

POLL_INTERVAL_SEC = 0.25
TIMEOUT_SEC = 120
WARMUP_MILESTONES = 3

TAG_PREFIX = "gateway"

# ------------------- HTTP session -------------------

SESS = requests.Session()

# ------------------- Helpers -------------------

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def api_info():
    r = SESS.get(f"{NODE_URL}/api/core/v2/info", timeout=5)
    r.raise_for_status()
    return r.json()

def get_milestone_ts(ms_index: int):
    r = SESS.get(f"{NODE_URL}/api/core/v2/milestones/{ms_index}", timeout=5)
    r.raise_for_status()
    j = r.json()
    if isinstance(j, dict) and "timestamp" in j:
        return int(j["timestamp"])
    return int(j.get("data", {}).get("timestamp"))

def seconds_to_next_milestone(cadence=CADENCE_SEC):
    info = api_info()
    latest_ts = int(info["status"]["latestMilestone"]["timestamp"])
    now_s = int(time.time())
    delta = (now_s - latest_ts) % cadence
    return float(cadence - delta if delta != 0 else 0.0)

def make_commit_payload(batch_size=BATCH_SIZE_DECISIONS, target_bytes=TARGET_BYTES) -> bytes:
    root = hashlib.sha256(f"root-{time.time()}".encode()).hexdigest()
    pol  = hashlib.sha256(f"policy-{time.time()}".encode()).hexdigest()
    items = [
        {
            "d": hashlib.sha256(f"dev-{i}-{time.time()}".encode()).hexdigest()[:16],
            "act": random.choice(["allow","deny","throttle"]),
            "v": random.randint(1,5)
        } for i in range(batch_size)
    ]
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
    if len(b) < target_bytes:
        b += b" " * (target_bytes - len(b))
    return b

def post_tagged_data(data_bytes: bytes, tag_ascii: str) -> str:
    tag_hex = "0x" + tag_ascii.encode().hex()
    data_hex = "0x" + data_bytes.hex()
    payload = {
        "protocolVersion": 2,
        "payload": {"type": 5, "tag": tag_hex, "data": data_hex}
    }
    r = SESS.post(f"{NODE_URL}/api/core/v2/blocks", json=payload, timeout=10)
    r.raise_for_status()
    j = r.json()
    return j.get("blockId")

def get_block_metadata(block_id: str):
    r = SESS.get(f"{NODE_URL}/api/core/v2/blocks/{block_id}/metadata", timeout=5)
    r.raise_for_status()
    return r.json()

def confirm_ttc_until(block_id: str, start_ts: float,
                      timeout_s=TIMEOUT_SEC, poll_interval=POLL_INTERVAL_SEC, single_check=False):
    ms_idx = None
    if single_check:
        try:
            meta = get_block_metadata(block_id)
            ms_idx = meta.get("referencedByMilestoneIndex")
            if ms_idx is not None:
                ttc = time.time() - start_ts
                try:
                    ts_curr = get_milestone_ts(ms_idx)
                    ts_prev = get_milestone_ts(ms_idx - 1) if ms_idx > 0 else None
                    ms_interval = float(ts_curr - ts_prev) if ts_prev else None
                except Exception:
                    ms_interval = None
                return True, ttc, ms_idx, ms_interval
        except Exception:
            pass
        return False, None, None, None

    deadline = start_ts + timeout_s
    while time.time() < deadline:
        try:
            meta = get_block_metadata(block_id)
            ms_idx = meta.get("referencedByMilestoneIndex")
            if ms_idx is not None:
                ttc = time.time() - start_ts
                try:
                    ts_curr = get_milestone_ts(ms_idx)
                    ts_prev = get_milestone_ts(ms_idx - 1) if ms_idx > 0 else None
                    ms_interval = float(ts_curr - ts_prev) if ts_prev else None
                except Exception:
                    ms_interval = None
                return True, ttc, ms_idx, ms_interval
        except Exception:
            pass
        time.sleep(poll_interval)
    return False, None, ms_idx, None

# ------------------- Burst logic -------------------

def run_burst(tier_commits: int, run_id: int, pattern_label: str):
    rows = []
    commits_done = 0
    test_start = time.time()
    print(f"\n[burst] run={run_id} target_commits={tier_commits}")

    group_id = 0
    while commits_done < tier_commits:
        group_id += 1
        group = min(BURST_GROUP_COMMITS, tier_commits - commits_done)
        pending = []

        for i in range(group):
            data = make_commit_payload()
            t_send = time.time()
            to_next_ms = seconds_to_next_milestone()
            try:
                block_id = post_tagged_data(data, TAG_PREFIX)
            except Exception as e:
                print(f"❌ post failed: {e}")
                block_id = None
            pending.append({
                "blockId": block_id,
                "t_send": t_send,
                "to_next_ms": to_next_ms,
                "payload_len": len(data),
                "commit_in_group": i + 1,
                "group_id": group_id
            })
            time.sleep(BURST_INTRA_COMMIT_GAP_SEC)

        unresolved = {p["blockId"]: p for p in pending if p["blockId"]}
        start_time = time.time()
        while unresolved and (time.time() - start_time) < TIMEOUT_SEC:
            for bid, p in list(unresolved.items()):
                ok, ttc, ms_idx, ms_int = confirm_ttc_until(bid, p["t_send"], single_check=True)
                if ok:
                    commits_done += 1
                    elapsed_since_start = p["t_send"] - test_start
                    rows.append({
                        "pattern": pattern_label,
                        "tier_commits": tier_commits,
                        "run_id": run_id,
                        "group_id": p["group_id"],
                        "commit_in_group": p["commit_in_group"],
                        "commit_idx": commits_done,
                        "blockId": bid,
                        "epoch_send": p["t_send"],
                        "elapsed_since_start_s": elapsed_since_start,
                        "ok": True,
                        "ttc_s": ttc,
                        "msIndex": ms_idx,
                        "ms_interval_s": ms_int,
                        "payload_bytes": p["payload_len"],
                        "batch_size": BATCH_SIZE_DECISIONS,
                        "delta_to_next_ms_s": p["to_next_ms"]
                    })
                    print(f"✅ commit {commits_done}/{tier_commits} TTC={ttc:.2f}s ms={ms_idx} Δ→MS={p['to_next_ms']:.1f}s")
                    unresolved.pop(bid, None)
            if unresolved:
                time.sleep(POLL_INTERVAL_SEC)

        # unresolved (timeouts)
        for bid, p in unresolved.items():
            commits_done += 1
            rows.append({
                "pattern": pattern_label,
                "tier_commits": tier_commits,
                "run_id": run_id,
                "group_id": p["group_id"],
                "commit_in_group": p["commit_in_group"],
                "commit_idx": commits_done,
                "blockId": bid,
                "epoch_send": p["t_send"],
                "elapsed_since_start_s": p["t_send"] - test_start,
                "ok": False,
                "ttc_s": None,
                "msIndex": None,
                "ms_interval_s": None,
                "payload_bytes": p["payload_len"],
                "batch_size": BATCH_SIZE_DECISIONS,
                "delta_to_next_ms_s": p["to_next_ms"]
            })
            print(f"⚠️ commit {commits_done}/{tier_commits} not confirmed within timeout")

        time.sleep(BURST_GROUP_GAP_SEC)
    return rows

# ------------------- Plots -------------------

def plot_ecdf(df: pd.DataFrame, outpath: str):
    okdf = df[df["ok"]]
    if okdf.empty:
        print("No OK rows for ECDF.")
        return
    tiers = sorted(okdf["tier_commits"].unique())
    plt.figure(figsize=(6.6,4.2))
    for t in tiers:
        vals = np.sort(okdf[okdf["tier_commits"]==t]["ttc_s"].dropna().values)
        if len(vals) == 0: continue
        y = np.arange(1, len(vals)+1) / len(vals)
        plt.plot(vals, y, label=f"{t} commits")
    plt.axvline(CADENCE_SEC, linestyle="--", alpha=0.7)
    plt.xlabel("TTC (s)")
    plt.ylabel("ECDF")
    plt.title("ECDF of TTC — burst pattern")
    plt.legend()
    plt.tight_layout()
    plt.savefig(outpath, dpi=220)
    plt.close()
    print(f"📈 Saved ECDF: {outpath}")

# ------------------- Main -------------------

def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)

    info = api_info()
    print(f"Node: {info['name']} v{info['version']}, healthy={info['status']['isHealthy']}, "
          f"network={info['protocol']['networkName']}, cadence≈{CADENCE_SEC}s")
    print(f"Warming up for {WARMUP_MILESTONES} milestones (~{WARMUP_MILESTONES*CADENCE_SEC}s)...")
    time.sleep(WARMUP_MILESTONES * CADENCE_SEC)

    all_rows = []
    for tier in TIERS:
        for rep in range(1, REPEATS+1):
            print(f"\n=== burst tier={tier} rep={rep} ===")
            rows = run_burst(tier, rep, "burst")
            all_rows.extend(rows)
            time.sleep(CADENCE_SEC)

    df = pd.DataFrame(all_rows)
    df.to_csv(CSV_PATH, index=False)
    print(f"\n💾 Wrote {CSV_PATH} (rows={len(df)})")

    plot_ecdf(df, os.path.join(RESULTS_DIR, "test1_burstfix_ecdf_burst.png"))

    okdf = df[df["ok"]]
    if not okdf.empty:
        print("\nSummary (per tier):")
        for t, grp in okdf.groupby("tier_commits"):
            vals = grp["ttc_s"].dropna().values
            p50, p95, p99 = np.percentile(vals, [50, 95, 99])
            print(f"  [tier={t:3d}] n={len(vals)} P50={p50:.2f}s P95={p95:.2f}s P99={p99:.2f}s")

if __name__ == "__main__":
    main()
