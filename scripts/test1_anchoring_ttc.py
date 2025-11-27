#!/usr/bin/env python3
"""
Test 1 – Anchoring vs Milestone Cadence (Hornet v2 compatible)
Measures confirmation latency (TTC) for anchoring payloads on IOTA Hornet 2.x.
"""

import requests
import json
import time
import csv
import os
import concurrent.futures
import hashlib
import matplotlib.pyplot as plt
import numpy as np

# ============================================================
# Configuration
# ============================================================

NODE_URL = "http://172.18.211.11:14265"  # Hornet REST endpoint
BASE_DIR = "/home/fjaved/CE-SI/fresh-iota-sandbox/iota-sandbox/scripts"
OUTPUT_DIR = os.path.join(BASE_DIR, "results")
CSV_FILE = os.path.join(OUTPUT_DIR, "test1_single_v2.csv")

TOTAL_TX = 20             # keep small for first test
CONCURRENCY = 5
POLL_INTERVAL = 1.0       # seconds between metadata checks
TIMEOUT = 120             # seconds per transaction
TAG_PREFIX = "anchor"

# ============================================================
# Helper functions
# ============================================================

def make_payload(i: int) -> dict:
    """Create a tagged data payload compatible with Hornet v2."""
    data = f"anchor-payload-{i}-{time.time()}".encode()
    digest = hashlib.sha256(data).hexdigest()
    tag_hex = "0x" + TAG_PREFIX.encode().hex()
    data_hex = "0x" + digest.encode().hex()
    payload = {
        "protocolVersion": 2,
        "payload": {
            "type": 5,
            "tag": tag_hex,
            "data": data_hex
        }
    }
    return payload

def post_block(payload: dict) -> str:
    """Post a block using v2 API."""
    try:
        r = requests.post(f"{NODE_URL}/api/core/v2/blocks", json=payload, timeout=10)
        r.raise_for_status()
        j = r.json()
        return j.get("blockId")
    except Exception as e:
        print(f"❌ Error posting block: {e}")
        return None

def wait_for_confirmation(block_id: str, t_start: float) -> float:
    """Poll until referenced by milestone."""
    url = f"{NODE_URL}/api/core/v2/blocks/{block_id}/metadata"
    for _ in range(int(TIMEOUT / POLL_INTERVAL)):
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                meta = r.json()
                ms_index = meta.get("referencedByMilestoneIndex")
                if ms_index is not None:
                    return time.time() - t_start
        except Exception:
            pass
        time.sleep(POLL_INTERVAL)
    return None

def send_and_measure(i: int):
    """Send one payload and measure TTC."""
    payload = make_payload(i)
    t_send = time.time()
    block_id = post_block(payload)
    if not block_id:
        return (i, None)
    ttc = wait_for_confirmation(block_id, t_send)
    return (i, ttc)

# ============================================================
# Main experiment
# ============================================================

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Starting anchoring test (Hornet v2) on {NODE_URL}")
    print(f"Total TX = {TOTAL_TX}, concurrency = {CONCURRENCY}")
    print(f"Results will be stored in {OUTPUT_DIR}")

    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = [executor.submit(send_and_measure, i) for i in range(TOTAL_TX)]
        for fut in concurrent.futures.as_completed(futures):
            i, ttc = fut.result()
            if ttc:
                print(f"✅ TX {i:03d} confirmed in {ttc:.2f}s")
                results.append(ttc)
            else:
                print(f"⚠️ TX {i:03d} not confirmed (timeout)")

    # Save results
    with open(CSV_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["tx_index", "ttc_sec"])
        for idx, ttc in enumerate(results):
            writer.writerow([idx, ttc])

    print(f"\nSaved {len(results)} confirmations to {CSV_FILE}")

    if results:
        arr = np.array(results)
        arr.sort()
        y = np.arange(1, len(arr)+1) / len(arr)

        # ECDF
        plt.figure(figsize=(7,5))
        plt.plot(arr, y, marker='.', linestyle='none')
        plt.xlabel("Time-to-Confirmation (s)")
        plt.ylabel("Empirical CDF")
        plt.title("Anchoring Confirmation ECDF (Hornet v2)")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, "ecdf_test1_v2.png"))
        plt.close()

        # Boxplot
        plt.figure(figsize=(5,4))
        plt.boxplot(arr, vert=True, patch_artist=True)
        plt.ylabel("TTC (s)")
        plt.title("Anchoring TTC Boxplot (Hornet v2)")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, "boxplot_test1_v2.png"))
        plt.close()

        print("📈 Plots saved under", OUTPUT_DIR)
        print(f"P50 = {np.percentile(arr,50):.2f}s, "
              f"P95 = {np.percentile(arr,95):.2f}s, "
              f"P99 = {np.percentile(arr,99):.2f}s")

if __name__ == "__main__":
    main()
