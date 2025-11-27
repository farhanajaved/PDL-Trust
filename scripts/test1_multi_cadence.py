#!/usr/bin/env python3
# --------------------------------------------------------------
# test1_multi_cadence_v2.py
# Anchoring TTC vs milestone cadence (5s / 10s / 15s)
# --------------------------------------------------------------

import os, time, json, requests, concurrent.futures, pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

NODE_URL = "http://172.18.211.11:14265"  # Hornet API
COORD_NAME = "inx-coordinator"
CADENCES = [5, 10, 15]  # seconds
TOTAL_TX = 20
CONCURRENCY = 5
RESULTS_DIR = "/home/fjaved/CE-SI/fresh-iota-sandbox/iota-sandbox/scripts/results"

os.makedirs(RESULTS_DIR, exist_ok=True)

def restart_coordinator(cadence: int):
    """Restart the existing inx-coordinator container with a new interval."""
    print(f"🌀 Restarting coordinator with interval={cadence}s ...")
    os.system(f"docker stop {COORD_NAME} >/dev/null 2>&1")
    time.sleep(2)
    os.system(f"docker start {COORD_NAME} >/dev/null 2>&1")
    time.sleep(10)
    print(f"✅ Coordinator restarted. (interval={cadence}s configured manually or fixed)")

def post_block(tag: str, data: str):
    url = f"{NODE_URL}/api/core/v2/blocks"
    payload = {"protocolVersion": 2, "payload": {"type": 5, "tag": tag, "data": data}}
    r = requests.post(url, json=payload, timeout=10)
    r.raise_for_status()
    return r.json().get("blockId")

def wait_confirm(block_id: str, timeout_s: int = 60):
    url = f"{NODE_URL}/api/core/v2/blocks/{block_id}/metadata"
    start = time.time()
    while time.time() - start < timeout_s:
        r = requests.get(url, timeout=5)
        if r.status_code == 200 and r.json().get("referencedByMilestoneIndex"):
            return (True, time.time() - start)
        time.sleep(1)
    return (False, None)

def run_round(cadence):
    tag_hex = "0x" + f"cad{cadence}".encode().hex()
    results = []
    print(f"\n🚀 Running cadence {cadence}s ...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as exe:
        futs = []
        for i in range(TOTAL_TX):
            data_hex = "0x" + f"block-{cadence}-{i}-{datetime.utcnow().isoformat()}".encode().hex()
            futs.append(exe.submit(post_block, tag_hex, data_hex))
        block_ids = []
        for f in concurrent.futures.as_completed(futs):
            try:
                bid = f.result()
                block_ids.append(bid)
            except Exception as e:
                print(f"❌ Error posting block: {e}")
    # Wait for confirmation
    for idx, bid in enumerate(block_ids):
        ok, ttc = wait_confirm(bid, timeout_s=cadence * 3)
        if ok:
            print(f"✅ TX {idx:03d} confirmed in {ttc:.2f}s")
            results.append({"cadence": cadence, "blockId": bid, "ttc_s": ttc, "ok": True})
        else:
            print(f"⚠️ TX {idx:03d} not confirmed")
            results.append({"cadence": cadence, "blockId": bid, "ttc_s": None, "ok": False})
    return results

def plot_grouped_boxplot(df: pd.DataFrame, outpath: str):
    plt.figure(figsize=(6,4))
    data = [df[df["cadence"]==c]["ttc_s"].dropna() for c in sorted(df["cadence"].unique())]
    plt.boxplot(data, labels=[f"{c}s" for c in sorted(df["cadence"].unique())], patch_artist=True)
    plt.ylabel("TTC (s)")
    plt.xlabel("Milestone cadence")
    plt.title("Anchoring TTC vs Coordinator Cadence (Hornet v2)")
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    print(f"📈 Saved grouped boxplot: {outpath}")

def main():
    all_rows = []
    for cadence in CADENCES:
        # Instead of forcing interval, just restart (you can manually change interval between runs)
        restart_coordinator(cadence)
        results = run_round(cadence)
        all_rows.extend(results)
        print(f"Sleeping 15s before next cadence...")
        time.sleep(15)

    df = pd.DataFrame(all_rows)
    csv_path = os.path.join(RESULTS_DIR, "test1_multi_cadence_v2.csv")
    df.to_csv(csv_path, index=False)
    print(f"\n💾 Saved results to {csv_path}")

    plot_path = os.path.join(RESULTS_DIR, "test1_multi_cadence_boxplot_v2.png")
    plot_grouped_boxplot(df, plot_path)

if __name__ == "__main__":
    main()
