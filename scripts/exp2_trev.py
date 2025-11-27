#!/usr/bin/env python3
# exp2_trev.py
# --------------------------------------------------------------
# Experiment 2 — Registry Propagation SLO (T_rev)
#
# Measures end-to-end time until the gateway starts denying
# after a registry change hits the ledger:
#   T_rev = t_deny − t_registry_write
#
# Also logs:
#   TTC_reg_s   := t_confirm − t_registry_write
#   snap_wait_s := t_cache_seen − t_confirm
#   (optional) msInterval_s (observed milestone interval)
#
# Behavior:
# - Emits IOTA tagged data blocks (payload type=5) for event types:
#     RevokeKey, RecallModel, TransferOwnership
# - Confirmation: metadata.referencedByMilestoneIndex
# - Gateway "Trust Cache" is simulated by a snapshot poller every Δ_snap
#   (event becomes visible at the first poll tick AFTER confirmation)
# - Gateway action probes at --probe-hz; denial flips on immediately
#   after cache sees the event; first probe ≥ t_cache_seen is t_deny.
#
# Outputs:
# - CSV with one row per event under --out-dir
# - Optional quick plots (boxplot + ECDF) for T_rev grouped by Δ_snap
#
# Requirements:
#   pip install requests pandas numpy matplotlib
# --------------------------------------------------------------

import argparse
import os
import sys
import time
import json
import math
import random
import string
import hashlib
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple, List

import requests
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ----------------------------- Helpers -----------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def seconds() -> float:
    return time.time()

def ceil_to_tick(t: float, origin: float, step: float) -> float:
    """Ceiling of t to the grid origin + k*step (k integer)."""
    if step <= 0:
        return t
    k = math.ceil((t - origin) / step)
    return origin + k * step

def json_tag(ascii_text: str) -> str:
    return "0x" + ascii_text.encode().hex()

def make_nonce(nbytes: int = 8) -> str:
    return hashlib.sha256(f"{time.time_ns()}-{random.random()}".encode()).hexdigest()[:2*nbytes]

def make_device_id() -> str:
    base = random.choice(["cam", "thermo", "lock", "plug", "hgw"])
    return f"{base}-{random.randint(1, 32)}"

def make_model_digest() -> str:
    return hashlib.sha256(f"model-{random.randint(1,9999)}".encode()).hexdigest()

def build_payload(event_type: str) -> Dict[str, Any]:
    ts = now_iso()
    if event_type == "revoke":
        return {
            "type":"RevokeKey",
            "deviceId": make_device_id(),
            "reason":"compromised",
            "issuer":"Lab",
            "ts": ts,
            "nonce": make_nonce()
        }
    elif event_type == "recall":
        return {
            "type":"RecallModel",
            "modelDigest": make_model_digest(),
            "severity":"high",
            "issuer":"Lab",
            "ts": ts,
            "nonce": make_nonce()
        }
    elif event_type == "transfer":
        return {
            "type":"TransferOwnership",
            "deviceId": make_device_id(),
            "from":"oldOwner",
            "to":"newOwner",
            "ts": ts,
            "nonce": make_nonce()
        }
    else:
        raise ValueError(f"unknown event_type: {event_type}")

# --------------------------- IOTA REST -----------------------------

class IotaClient:
    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base = base_url.rstrip("/")
        self.s = requests.Session()
        self.timeout = timeout

    def info(self) -> Dict[str, Any]:
        r = self.s.get(f"{self.base}/api/core/v2/info", timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def post_tagged(self, tag_ascii: str, data_bytes: bytes) -> str:
        payload = {
            "protocolVersion": 2,
            "payload": {
                "type": 5,
                "tag": json_tag(tag_ascii),
                "data": "0x" + data_bytes.hex()
            }
        }
        r = self.s.post(f"{self.base}/api/core/v2/blocks", json=payload, timeout=self.timeout)
        r.raise_for_status()
        j = r.json()
        return j.get("blockId")

    def block_meta(self, block_id: str) -> Dict[str, Any]:
        r = self.s.get(f"{self.base}/api/core/v2/blocks/{block_id}/metadata", timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def milestone_ts(self, index: int) -> Optional[int]:
        """Return milestone timestamp (epoch seconds) if endpoint available, else None."""
        try:
            r = self.s.get(f"{self.base}/api/core/v2/milestones/{index}", timeout=self.timeout)
            r.raise_for_status()
            j = r.json()
            if isinstance(j, dict) and "timestamp" in j:
                return int(j["timestamp"])
            # fallback schema
            return int(j.get("data", {}).get("timestamp"))
        except Exception:
            return None

    def wait_confirm(self, block_id: str, start_ts: float, poll_interval: float = 0.25,
                     timeout_s: float = 120.0) -> Tuple[bool, Optional[float], Optional[int], Optional[float]]:
        """
        Poll until referenced. Return (ok, ttc_s, msIndex, msInterval_s).
        """
        deadline = start_ts + timeout_s
        last_ms_idx = None
        while seconds() < deadline:
            try:
                meta = self.block_meta(block_id)
                ms_idx = meta.get("referencedByMilestoneIndex")
                if ms_idx is not None:
                    ttc = seconds() - start_ts
                    # Optional observed interval
                    ts_curr = self.milestone_ts(ms_idx)
                    ts_prev = self.milestone_ts(ms_idx - 1) if ms_idx and ms_idx > 0 else None
                    ms_interval = float(ts_curr - ts_prev) if (ts_curr and ts_prev) else None
                    return True, ttc, ms_idx, ms_interval
                last_ms_idx = ms_idx
            except Exception:
                pass
            time.sleep(poll_interval)
        return False, None, last_ms_idx, None


# --------------------------- Experiment ----------------------------

def event_type_from_label(label: str) -> str:
    label = label.strip().lower()
    if label in ("revoke", "revokekey", "revoke_key"):   return "revoke"
    if label in ("recall", "recallmodel", "recall_model"): return "recall"
    if label in ("transfer", "transferownership", "transfer_ownership"): return "transfer"
    raise ValueError(f"Unsupported event label: {label}")

def build_tag_for_payload(payload: Dict[str, Any]) -> str:
    # e.g., "reg:RevokeKey"
    return f"reg:{payload['type']}"

def simulate_gateway(snapshot_origin: float, delta_snap: float,
                     t_confirm: float, probe_hz: float,
                     probe_start: float) -> Tuple[float, float]:
    """
    Returns (t_cache_seen, t_deny).
    - Cache sees the event at the first snapshot tick AFTER t_confirm
      (ticks at snapshot_origin + k*delta_snap).
    - Probes every 1/probe_hz from probe_start; first probe >= t_cache_seen is deny.
    """
    t_cache_seen = ceil_to_tick(t_confirm, snapshot_origin, delta_snap)
    if probe_hz <= 0:
        # Immediate deny at t_cache_seen if no probes configured
        return t_cache_seen, t_cache_seen
    period = 1.0 / probe_hz
    t_deny = ceil_to_tick(t_cache_seen, probe_start, period)
    return t_cache_seen, t_deny


def run_cell(client: IotaClient,
             event_type: str,
             delta_snap: float,
             net_profile: str,
             events_per_cell: int,
             probe_hz: float,
             deny_timeout_extra: float,
             log_rows: List[Dict[str, Any]],
             poll_interval: float = 0.25) -> None:
    """
    Run N events for a single (event_type, delta_snap, net_profile) cell.
    """
    print(f"\n▶ cell: type={event_type}  Δ_snap={delta_snap:.0f}s  net={net_profile}  N={events_per_cell}")
    cell_origin = seconds()   # snapshot poller origin
    for i in range(1, events_per_cell + 1):
        # Build registry event payload
        payload = build_payload(event_type)
        tag_ascii = build_tag_for_payload(payload)
        data_bytes = json.dumps(payload, separators=(",", ":")).encode()

        # Post registry write
        t_write = seconds()
        try:
            block_id = client.post_tagged(tag_ascii, data_bytes)
        except Exception as e:
            err = f"post_failed:{e}"
            print(f"  ❌ {event_type} #{i} post error: {e}")
            log_rows.append({
                "ts_iso": now_iso(),
                "seq": i,
                "eventType": payload["type"],
                "deltaSnap_s": delta_snap,
                "netProfile": net_profile,
                "blockId": None,
                "msIndex": None,
                "msInterval_s": None,
                "t_write": t_write,
                "t_confirm": None,
                "t_cache_seen": None,
                "t_deny": None,
                "T_rev_s": None,
                "TTC_reg_s": None,
                "snap_wait_s": None,
                "ok": 0,
                "error": err,
                "payload_bytes": len(data_bytes),
            })
            continue

        # Wait for confirmation
        ok, ttc, ms_idx, ms_interval = client.wait_confirm(block_id, t_write, poll_interval=poll_interval)
        if not ok:
            print(f"  ⚠️  {event_type} #{i} not confirmed (timeout)")
            log_rows.append({
                "ts_iso": now_iso(),
                "seq": i,
                "eventType": payload["type"],
                "deltaSnap_s": delta_snap,
                "netProfile": net_profile,
                "blockId": block_id,
                "msIndex": ms_idx,
                "msInterval_s": ms_interval,
                "t_write": t_write,
                "t_confirm": None,
                "t_cache_seen": None,
                "t_deny": None,
                "T_rev_s": None,
                "TTC_reg_s": None,
                "snap_wait_s": None,
                "ok": 0,
                "error": "confirm_timeout",
                "payload_bytes": len(data_bytes),
            })
            continue

        t_confirm = t_write + ttc

        # Simulate cache polling + action probes
        # Deny timeout = 2*Δ_snap + extra
        deny_deadline = t_write + (2.0 * delta_snap) + float(deny_timeout_extra)
        t_cache_seen, t_deny = simulate_gateway(snapshot_origin=cell_origin,
                                                delta_snap=delta_snap,
                                                t_confirm=t_confirm,
                                                probe_hz=probe_hz,
                                                probe_start=t_write)

        if t_deny > deny_deadline:
            # Treat as timeout
            print(f"  ⚠️  {event_type} #{i} deny timeout (t_deny − t_write={t_deny - t_write:.2f}s)")
            log_rows.append({
                "ts_iso": now_iso(),
                "seq": i,
                "eventType": payload["type"],
                "deltaSnap_s": delta_snap,
                "netProfile": net_profile,
                "blockId": block_id,
                "msIndex": ms_idx,
                "msInterval_s": ms_interval,
                "t_write": t_write,
                "t_confirm": t_confirm,
                "t_cache_seen": t_cache_seen,
                "t_deny": None,
                "T_rev_s": None,
                "TTC_reg_s": t_confirm - t_write,
                "snap_wait_s": t_cache_seen - t_confirm,
                "ok": 0,
                "error": "deny_timeout",
                "payload_bytes": len(data_bytes),
            })
            continue

        T_rev = t_deny - t_write
        snap_wait_s = t_cache_seen - t_confirm
        print(f"  ✅ {event_type} #{i}  T_rev={T_rev:.2f}s  TTC_reg={ttc:.2f}s  snap_wait={snap_wait_s:.2f}s  ms={ms_idx}")

        log_rows.append({
            "ts_iso": now_iso(),
            "seq": i,
            "eventType": payload["type"],
            "deltaSnap_s": delta_snap,
            "netProfile": net_profile,
            "blockId": block_id,
            "msIndex": ms_idx,
            "msInterval_s": ms_interval,
            "t_write": t_write,
            "t_confirm": t_confirm,
            "t_cache_seen": t_cache_seen,
            "t_deny": t_deny,
            "T_rev_s": T_rev,
            "TTC_reg_s": t_confirm - t_write,
            "snap_wait_s": snap_wait_s,
            "ok": 1,
            "error": "",
            "payload_bytes": len(data_bytes),
        })


# ----------------------------- Plots ------------------------------

def quick_plots(out_dir: str, df: pd.DataFrame) -> None:
    okdf = df[df["ok"] == 1].copy()
    if okdf.empty:
        print("No successful rows for plots.")
        return

    # Boxplot of T_rev by Δ_snap (hue=eventType)
    plt.figure(figsize=(7.2, 4.2))
    # group data
    groups = []
    labels = []
    for d in sorted(okdf["deltaSnap_s"].unique()):
        for et in ["RevokeKey", "RecallModel", "TransferOwnership"]:
            vals = okdf[(okdf["deltaSnap_s"] == d) & (okdf["eventType"] == et)]["T_rev_s"].dropna().values
            if len(vals) == 0: 
                continue
            groups.append(vals)
            labels.append(f"{int(d)}s\n{et.split('R')[0]+'R' if et!='RevokeKey' else 'Rev'}")
    if groups:
        positions = np.arange(1, len(groups)+1)
        plt.boxplot(groups, positions=positions, showfliers=True)
        plt.xticks(positions, labels, rotation=0)
        plt.ylabel("T_rev (s)")
        plt.title("Registry Propagation SLO by Δ_snap and event type")
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, "exp2_trev_boxplot.png"), dpi=220)
        plt.close()
        print(f"📈 Saved plot: {os.path.join(out_dir, 'exp2_trev_boxplot.png')}")

    # ECDF of T_rev per Δ_snap (all event types combined)
    plt.figure(figsize=(6.4, 4.2))
    for d in sorted(okdf["deltaSnap_s"].unique()):
        vals = np.sort(okdf[okdf["deltaSnap_s"] == d]["T_rev_s"].dropna().values)
        if len(vals) == 0: continue
        y = np.arange(1, len(vals)+1) / len(vals)
        plt.plot(vals, y, label=f"Δ={int(d)}s")
    plt.xlabel("T_rev (s)")
    plt.ylabel("ECDF")
    plt.title("ECDF of T_rev by snapshot cadence")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "exp2_trev_ecdf.png"), dpi=220)
    plt.close()
    print(f"📈 Saved plot: {os.path.join(out_dir, 'exp2_trev_ecdf.png')}")


# ------------------------------ Main ------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Experiment 2: Registry Propagation SLO (T_rev)")
    p.add_argument("--node-url", required=True, help="IOTA node base URL, e.g. http://127.0.0.1:14265")
    p.add_argument("--events-per-cell", type=int, default=50, help="N per (event_type × delta_snap × net_profile)")
    p.add_argument("--event-types", default="revoke,recall,transfer",
                   help="csv of event types (revoke, recall, transfer)")
    p.add_argument("--delta-snaps", default="15,60",
                   help="csv of snapshot cadences to test, seconds (e.g., 15,60)")
    p.add_argument("--net-profiles", default="normal,impaired",
                   help="csv of labels; does not alter network, just logs the label")
    p.add_argument("--probe-hz", type=float, default=2.0, help="action probe rate (Hz)")
    p.add_argument("--deny-timeout-extra", type=int, default=30, help="extra seconds beyond 2*Δ_snap")
    p.add_argument(
    "--out-dir",
    default="/home/fjaved/CE-SI/fresh-iota-sandbox/iota-sandbox/scripts/results",
    help="directory for CSV/plots"
    )
    p.add_argument("--seed", type=int, default=42, help="random seed")
    return p.parse_args()

def main():
    args = parse_args()
    random.seed(args.seed)
    np.random.seed(args.seed)

    out_dir = args.out_dir
    os.makedirs(out_dir, exist_ok=True)
    tsstamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    csv_path = os.path.join(out_dir, f"exp2_trev_{tsstamp}.csv")

    client = IotaClient(args.node_url)

    # Warm-up
    try:
        info = client.info()
        print(f"Node: {info.get('name','?')} v{info.get('version','?')}  healthy={info['status']['isHealthy']}  "
              f"network={info['protocol']['networkName']}")
    except Exception as e:
        print(f"Cannot reach node: {e}", file=sys.stderr)
        sys.exit(1)

    print("Warming up across ~3 milestones (assuming ~10 s cadence)...")
    time.sleep(30.0)

    event_types = [event_type_from_label(x) for x in args.event_types.split(",") if x.strip()]
    delta_snaps = [float(x.strip()) for x in args.delta_snaps.split(",") if x.strip()]
    net_profiles = [x.strip() for x in args.net_profiles.split(",") if x.strip()]

    rows: List[Dict[str, Any]] = []

    # Run matrix
    for et in event_types:
        for ds in delta_snaps:
            for np_label in net_profiles:
                run_cell(client=client,
                         event_type=et,
                         delta_snap=ds,
                         net_profile=np_label,
                         events_per_cell=args.events_per_cell,
                         probe_hz=args.probe_hz,
                         deny_timeout_extra=args.deny_timeout_extra,
                         log_rows=rows)

    # Save CSV
    df = pd.DataFrame(rows, columns=[
        "ts_iso","seq","eventType","deltaSnap_s","netProfile",
        "blockId","msIndex","msInterval_s",
        "t_write","t_confirm","t_cache_seen","t_deny",
        "T_rev_s","TTC_reg_s","snap_wait_s",
        "ok","error","payload_bytes"
    ])
    df.to_csv(csv_path, index=False)
    print(f"\n💾 Wrote {csv_path}  rows={len(df)}")

    # Quick summary
    okdf = df[df["ok"] == 1]
    if not okdf.empty:
        print("\nSummary by Δ_snap (all event types, ok==1):")
        for ds, grp in okdf.groupby("deltaSnap_s"):
            vals = grp["T_rev_s"].dropna().values
            p50, p95, p99 = np.percentile(vals, [50, 95, 99])
            print(f"  Δ={int(ds):>3d}s  n={len(vals):4d}  P50={p50:5.2f}s  P95={p95:5.2f}s  P99={p99:5.2f}s")

    # Optional quick plots
    try:
        quick_plots(out_dir, df)
    except Exception as e:
        print(f"[plots] skipped: {e}")

if __name__ == "__main__":
    main()
