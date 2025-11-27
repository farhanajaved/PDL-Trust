#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test3_partition_tolerance.py — Backhaul Impairment / Partition-Tolerance (Test 3)

Purpose
-------
Evaluate a gateway’s Trust-Cache behavior under backhaul impairment/partition and recovery
on an IOTA (Hornet v2) sandbox. The script emulates a household gateway with three concurrent
paths: (1) a fast, local decision loop that only uses Trust-Cache age (T_decide in ms),
(2) an anchoring pipeline that batches local decisions into ~2.5 KB LedgerCommit payloads and
posts to Hornet /api/core/v2/blocks, then confirms via /api/core/v2/blocks/{id}/metadata,
and (3) a snapshotter that represents ingest of a signed registry snapshot on a fixed cadence
(Δ_snap). The system marches through four phases with different connectivity:
normal → impaired → offline → recovery.

All events are written to a single append-safe CSV from t=0 with immediate flush
and periodic fsync/backup so partial runs are salvageable.

Repro Notes
-----------
All payloads are SYNTHETIC (~2.5 KB) shaped to mimic an IoT device mix; no real user data is posted.
We only anchor commitments consistent with CE data-minimization (no PII). This script targets Python 3.9+.

Usage example
-------------
python3 test3_partition_tolerance.py \
  --node http://172.18.211.11:14265 \
  --base-dir /home/fjaved/CE-SI/fresh-iota-sandbox/iota-sandbox/scripts \
  --results-dir results \
  --delta-snap 15 \
  --normal-secs 90 --impaired-secs 120 --offline-secs 90 --recovery-secs 120 \
  --decisions-per-sec 3 --batch-size 20 --payload-bytes 2500 \
  --software-impairment true
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import json
import csv
import shutil
import math
import hashlib
import random
import threading
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple

import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# ===========================
# CLI
# ===========================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Test 3 — Backhaul Impairment / Partition-Tolerance")
    p.add_argument("--node", default="http://172.18.211.11:14265", help="Hornet node base URL")
    p.add_argument("--base-dir", default="/home/fjaved/CE-SI/fresh-iota-sandbox/iota-sandbox/scripts", help="Base directory")
    p.add_argument("--results-dir", default="results", help="Results subdirectory under base-dir")
    p.add_argument("--delta-snap", type=int, default=15, help="Snapshot cadence in seconds")
    p.add_argument("--normal-secs", type=int, default=90)
    p.add_argument("--impaired-secs", type=int, default=120)
    p.add_argument("--offline-secs", type=int, default=90)
    p.add_argument("--recovery-secs", type=int, default=120)
    p.add_argument("--decisions-per-sec", type=float, default=3.0)
    p.add_argument("--batch-size", type=int, default=20)
    p.add_argument("--payload-bytes", type=int, default=2500)
    p.add_argument("--poll-interval", type=float, default=1.0)
    p.add_argument("--confirm-timeout", type=float, default=120.0)
    p.add_argument("--impair-latency-ms", type=int, default=120)
    p.add_argument("--impair-loss-pct", type=float, default=1.0)
    p.add_argument("--software-impairment", type=str, default="true", choices=["true", "false"])
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--dpi", type=int, default=240)
    return p.parse_args()


# ===========================
# Utility helpers
# ===========================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def rel_seconds(t0: float) -> float:
    return time.monotonic() - t0

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def fsync_file(fh) -> None:
    try:
        fh.flush()
        os.fsync(fh.fileno())
    except Exception:
        pass

def rand_hex(nbytes: int) -> str:
    return hashlib.sha256(os.urandom(nbytes)).hexdigest()

def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def percentile(series: pd.Series, q: float) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return float("nan")
    return float(np.percentile(s, q))


# ===========================
# Durable Queue (append-only jsonl)
# ===========================
class DurableQueue:
    """
    Append-only JSONL queue with a memory mirror.
    Each line is a JSON object:
      {"batch_id": int, "payload": <dict>, "created_iso": str, "size_bytes": int}
    """
    def __init__(self, path: str, lock: threading.Lock):
        self.path = path
        self.lock = lock
        self.mem: List[Dict[str, Any]] = []
        ensure_dir(os.path.dirname(path))
        self._load()

    def _load(self) -> None:
        if not os.path.isfile(self.path):
            return
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict) and "batch_id" in obj and "payload" in obj:
                        self.mem.append(obj)
                except Exception:
                    continue

    def _append_line(self, obj: Dict[str, Any]) -> None:
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, separators=(",", ":")) + "\n")
            fsync_file(f)

    def enqueue(self, obj: Dict[str, Any]) -> None:
        with self.lock:
            self.mem.append(obj)
            self._append_line(obj)

    def dequeue(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            if not self.mem:
                return None
            obj = self.mem.pop(0)
            # Re-write file (simple compaction) so disk mirrors memory
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                for it in self.mem:
                    f.write(json.dumps(it, separators=(",", ":")) + "\n")
                fsync_file(f)
            try:
                os.replace(tmp, self.path)
            except Exception:
                pass
            return obj

    def requeue_front(self, obj: Dict[str, Any]) -> None:
        with self.lock:
            self.mem.insert(0, obj)
            # Re-write file to preserve durable order
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                for it in self.mem:
                    f.write(json.dumps(it, separators=(",", ":")) + "\n")
                fsync_file(f)
            try:
                os.replace(tmp, self.path)
            except Exception:
                pass

    def size(self) -> int:
        with self.lock:
            return len(self.mem)


# ===========================
# CSV Logger (append-safe with backup every N rows)
# ===========================
class CsvLogger:
    HEADER = [
        "t_wall_iso", "t_rel_s", "phase", "event",
        "cache_age_s", "delta_snap_s",
        "decision_id", "t_decide_ms", "permit",
        "batch_id", "block_id", "payload_bytes", "queued_before", "queued_after",
        "ttc_s", "msIndex",
        "snapshot_ok", "cache_age_before_s", "cache_age_after_s",
        "req_fail_injected", "extra_rtt_ms", "note"
    ]

    def __init__(self, csv_path: str, bak_path: str, backup_every: int = 200):
        self.csv_path = csv_path
        self.bak_path = bak_path
        self.backup_every = backup_every
        self._fh = open(self.csv_path, "a", newline="", encoding="utf-8")
        self._writer = csv.writer(self._fh)
        self._rows = 0
        # Write header if empty
        if os.path.getsize(self.csv_path) == 0:
            self._writer.writerow(self.HEADER)
            fsync_file(self._fh)

    def write(self, row: Dict[str, Any]) -> None:
        out = [row.get(k, "") for k in self.HEADER]
        self._writer.writerow(out)
        self._rows += 1
        self._fh.flush()
        if self._rows % self.backup_every == 0:
            try:
                os.fsync(self._fh.fileno())
                shutil.copyfile(self.csv_path, self.bak_path)
            except Exception:
                pass

    def close(self) -> None:
        try:
            fsync_file(self._fh)
            self._fh.close()
        except Exception:
            pass


# ===========================
# Phase Controller
# ===========================
PHASES = ("normal", "impaired", "offline", "recovery")

class PhaseController:
    def __init__(self, durations: Dict[str, int]):
        self.durations = durations
        self.current_phase = "normal"
        self.phase_start_monotonic = time.monotonic()
        self.sequence = ["normal", "impaired", "offline", "recovery"]
        self.idx = 0

    def time_in_phase(self) -> float:
        return time.monotonic() - self.phase_start_monotonic

    def maybe_advance(self) -> Optional[str]:
        dur = self.durations[self.current_phase]
        if self.time_in_phase() >= dur:
            self.idx = min(self.idx + 1, len(self.sequence) - 1)
            new_phase = self.sequence[self.idx]
            changed = (new_phase != self.current_phase)
            if changed:
                self.current_phase = new_phase
                self.phase_start_monotonic = time.monotonic()
                return new_phase
        return None

    def set_phase(self, phase: str) -> None:
        self.current_phase = phase
        self.phase_start_monotonic = time.monotonic()


# ===========================
# Gateway Emulator
# ===========================
class GatewayEmu:
    def __init__(self, args: argparse.Namespace):
        random.seed(args.seed)
        np.random.seed(args.seed)

        self.node = args.node.rstrip("/")
        self.delta_snap = float(args.delta_snap)
        self.decisions_per_sec = float(args.decisions_per_sec)
        self.batch_size = int(args.batch_size)
        self.payload_bytes = int(args.payload_bytes)
        self.poll_interval = float(args.poll_interval)
        self.confirm_timeout = float(args.confirm_timeout)
        self.impair_latency_ms = int(args.impair_latency_ms)
        self.impair_loss_pct = float(args.impair_loss_pct)
        self.software_impairment = (args.software_impairment.lower() == "true")

        # Paths
        self.outdir = os.path.join(args.base_dir, args.results_dir)
        ensure_dir(self.outdir)
        self.csv_path = os.path.join(self.outdir, "test3_partition.csv")
        self.bak_path = os.path.join(self.outdir, "test3_partition_bak.csv")
        self.queue_path = os.path.join(self.outdir, "test3_anchor_queue.jsonl")
        self.summary_path = os.path.join(self.outdir, "test3_summary.txt")

        # Plots
        self.fig_box = os.path.join(self.outdir, "test3_tdecide_box.png")
        self.fig_qdepth = os.path.join(self.outdir, "test3_queue_depth_over_time.png")
        self.fig_catchup = os.path.join(self.outdir, "test3_catchup_timeline.png")
        self.fig_ttc_ecdf = os.path.join(self.outdir, "test3_ttc_recovery_ecdf.png")
        self.dpi = int(args.dpi)

        # CSV logger
        self.logger = CsvLogger(self.csv_path, self.bak_path, backup_every=200)

        # Durable queue for anchor payloads
        self.q_lock = threading.Lock()
        self.queue = DurableQueue(self.queue_path, self.q_lock)

        # Shared state
        self.t0 = time.monotonic()
        self.stop_flag = threading.Event()
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self.phase_ctrl = PhaseController({
            "normal": args.normal_secs,
            "impaired": args.impaired_secs,
            "offline": args.offline_secs,
            "recovery": args.recovery_secs
        })
        self.recovery_start_rel_s: Optional[float] = None

        # Trust Cache age state
        self._cache_lock = threading.Lock()
        self._last_snapshot_ok_monotonic = time.monotonic()  # starts as if just refreshed

        # Decision batching
        self._batch_lock = threading.Lock()
        self._current_batch: List[Dict[str, Any]] = []
        self._next_batch_id = 1
        self._next_decision_id = 1

        # Queue-depth ticker timestamp
        self._last_qdepth_emit = 0.0

    # --------------------
    # Console debug helper
    # --------------------
    def dbg(self, msg: str) -> None:
        try:
            th = threading.current_thread().name
        except Exception:
            th = "main"
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [{th}] [{self.phase_ctrl.current_phase}] {msg}", flush=True)

    # --------------------
    # Cache helpers
    # --------------------
    def cache_age_seconds(self) -> float:
        with self._cache_lock:
            return max(0.0, time.monotonic() - self._last_snapshot_ok_monotonic)

    def reset_cache_age(self) -> None:
        with self._cache_lock:
            self._last_snapshot_ok_monotonic = time.monotonic()

    # --------------------
    # CSV rows
    # --------------------
    def log_row(self, **kwargs) -> None:
        row = {
            "t_wall_iso": utc_now_iso(),
            "t_rel_s": f"{rel_seconds(self.t0):.3f}",
            "phase": self.phase_ctrl.current_phase,
            "event": "",
            "cache_age_s": f"{self.cache_age_seconds():.3f}",
            "delta_snap_s": f"{self.delta_snap:.1f}",
            "decision_id": "",
            "t_decide_ms": "",
            "permit": "",
            "batch_id": "",
            "block_id": "",
            "payload_bytes": "",
            "queued_before": "",
            "queued_after": "",
            "ttc_s": "",
            "msIndex": "",
            "snapshot_ok": "",
            "cache_age_before_s": "",
            "cache_age_after_s": "",
            "req_fail_injected": "",
            "extra_rtt_ms": "",
            "note": "",
        }
        row.update({k: ("" if v is None else v) for k, v in kwargs.items()})
        self.logger.write(row)

    # --------------------
    # Decision loop
    # --------------------
    def decision_loop(self):
        try:
            target_period = 1.0 / max(0.1, self.decisions_per_sec)
            self.dbg(f"decision_loop start dps={self.decisions_per_sec} batch_size={self.batch_size}")
            while not self.stop_flag.is_set():
                t_start = time.perf_counter()
                c_age = self.cache_age_seconds()

                # Policy tiers
                if c_age <= self.delta_snap:
                    permit_prob = 0.95
                    extra_delay_ms = 0.0
                elif c_age <= 2 * self.delta_snap:
                    permit_prob = 0.50
                    extra_delay_ms = random.uniform(2.0, 4.0)
                else:
                    permit_prob = 0.10
                    extra_delay_ms = 0.0

                # Simulate local decision time (fast path + optional small delay)
                t0_ms = time.perf_counter()
                if extra_delay_ms > 0:
                    time.sleep(extra_delay_ms / 1000.0)
                t_decide_ms = (time.perf_counter() - t0_ms) * 1000.0

                permit = 1 if random.random() < permit_prob else 0

                # Record decision
                decision_id = self._next_decision_id
                self._next_decision_id += 1

                self.dbg(f"decision #{decision_id} permit={permit} cache_age={c_age:.2f}s t_decide={t_decide_ms:.2f}ms")
                self.log_row(
                    event="decision",
                    decision_id=str(decision_id),
                    t_decide_ms=f"{t_decide_ms:.3f}",
                    permit=str(permit),
                )

                # Batch producer
                self._add_to_batch({"decision_id": decision_id, "permit": permit, "t_rel_s": rel_seconds(self.t0)})

                # Once per second, log queue depth (CSV + console via ticker thread too)
                now = time.monotonic()
                if now - self._last_qdepth_emit >= 1.0:
                    self._last_qdepth_emit = now
                    qsize = self.queue.size()
                    self.log_row(event="queue_depth", queued_after=str(qsize))

                # Sleep to maintain rate
                elapsed = time.perf_counter() - t_start
                remaining = max(0.0, target_period - elapsed)
                time.sleep(remaining)
        except Exception as e:
            self.log_row(event="error", note=f"decision_loop_exception:{type(e).__name__}")
            self.dbg(f"decision loop crashed: {repr(e)}")
            raise

    def queue_ticker_loop(self):
        self.dbg("queue_ticker start")
        while not self.stop_flag.is_set():
            try:
                size = self.queue.size()
                self.log_row(event="queue_depth", queued_after=str(size))
                self.dbg(f"queue_depth={size}")
            except Exception:
                pass
            time.sleep(1.0)

    def _add_to_batch(self, item: Dict[str, Any]) -> None:
        with self._batch_lock:
            self._current_batch.append(item)
            if len(self._current_batch) >= self.batch_size:
                batch_id = self._next_batch_id
                self._next_batch_id += 1
                payload_dict, size_bytes = self._build_ledger_commit(self._current_batch)
                self._current_batch = []
                # Enqueue durable
                self.queue.enqueue({
                    "batch_id": batch_id,
                    "payload": payload_dict,   # store as dict for json= POST
                    "created_iso": utc_now_iso(),
                    "size_bytes": size_bytes
                })
                # Log batch formation / enqueue
                self.log_row(event="batch_enqueued",
                             batch_id=str(batch_id),
                             payload_bytes=str(size_bytes),
                             queued_after=str(self.queue.size()))
                self.dbg(f"batch_enqueued id={batch_id} size={size_bytes}B queue={self.queue.size()}")

    def _build_ledger_commit(self, items: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], int]:
        # Hash a simple concatenation of items as a merkleRoot surrogate
        concat = "".join([f"{it['decision_id']}|{it['permit']}|{it['t_rel_s']}" for it in items]).encode("utf-8")
        merkle_root = sha256_hex(concat)
        policy_digest = rand_hex(16)

        # Synthetic items: tiny device/action/version records
        synth_items = []
        for it in items:
            synth_items.append({
                "device": f"dev-{it['decision_id']%10}",
                "action": "allow" if it["permit"] else "deny",
                "version": f"v{1 + (it['decision_id']%3)}"
            })

        body = {
            "type": "LedgerCommit",
            "version": "1.0",
            "merkleRoot": merkle_root,
            "policyDigest": policy_digest,
            "batchSize": len(items),
            "createdAt": utc_now_iso(),
            "items": synth_items
        }
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")

        # Pad to payload_bytes if needed
        if len(data) < self.payload_bytes:
            pad = self.payload_bytes - len(data)
            data += b" " * pad

        # Wrap in Hornet block API structure with tag "gateway"
        block = {
            "protocolVersion": 2,
            "payload": {
                "type": 5,
                "tag": "0x67617465776179",  # "gateway"
                "data": "0x" + data.hex()
            }
        }
        return block, len(data)

    # --------------------
    # Snapshotter loop
    # --------------------
    def snapshotter_loop(self):
        delta = self.delta_snap
        next_tick = time.monotonic()
        while not self.stop_flag.is_set():
            now = time.monotonic()
            if now < next_tick:
                time.sleep(min(0.1, next_tick - now))
                continue
            next_tick += delta

            # Attempt heartbeat GET /api/core/v2/info
            injected = 0
            extra_rtt = 0
            phase = self.phase_ctrl.current_phase
            ok = 0
            cache_before = f"{self.cache_age_seconds():.3f}"

            try:
                if self.software_impairment:
                    if phase == "impaired":
                        extra_rtt = self.impair_latency_ms
                        time.sleep(self.impair_latency_ms / 1000.0)
                        if random.random() < (self.impair_loss_pct / 100.0):
                            injected = 1
                            raise requests.RequestException("Injected loss (software impairment)")
                    elif phase == "offline":
                        raise requests.RequestException("Forced offline (software impairment)")
                else:
                    # In non-software impairment mode we still force offline during offline phase
                    if phase == "offline":
                        raise requests.RequestException("Forced offline (no software-impairment)")

                url = f"{self.node}/api/core/v2/info"
                r = self.session.get(url, timeout=10)
                r.raise_for_status()
                _ = r.json()
                ok = 1
                self.reset_cache_age()
            except Exception:
                ok = 0

            cache_after = f"{self.cache_age_seconds():.3f}"
            self.log_row(
                event="snapshot_tick",
                snapshot_ok=str(ok),
                cache_age_before_s=cache_before,
                cache_age_after_s=cache_after,
                req_fail_injected=str(injected),
                extra_rtt_ms=str(extra_rtt),
            )
            if ok == 1:
                self.dbg("snapshot OK (cache_age reset)")
            else:
                self.dbg(f"snapshot MISS (offline/impair); cache_age now {cache_after}s")

    # --------------------
    # Sender loop + watcher
    # --------------------
    def sender_loop(self):
        while not self.stop_flag.is_set():
            item = self.queue.dequeue()
            if item is None:
                time.sleep(0.2)
                continue

            with self.q_lock:
                queued_before = self.queue.size() + 1  # including the one we just took
            self.dbg(f"dequeued batch={item.get('batch_id')} q_before={queued_before}")

            try:
                block_id = self._post_block(item)
                with self.q_lock:
                    queued_after = self.queue.size()
                self.log_row(
                    event="anchor_send",
                    batch_id=str(item["batch_id"]),
                    block_id=block_id or "",
                    payload_bytes=str(item.get("size_bytes", "")),
                    queued_before=str(queued_before),
                    queued_after=str(queued_after)
                )
                if block_id:
                    self.dbg(f"watch_confirm start batch={item['batch_id']} blockId={block_id}")
                    threading.Thread(
                        target=self._watch_confirm,
                        args=(item["batch_id"], block_id),
                        name=f"confirm-{item['batch_id']}",
                        daemon=True
                    ).start()
                else:
                    self.dbg(f"no blockId — requeue_front batch={item['batch_id']}")
                    self.queue.requeue_front(item)
            except Exception as e:
                self.queue.requeue_front(item)
                with self.q_lock:
                    queued_after = self.queue.size()
                self.log_row(
                    event="anchor_send",
                    batch_id=str(item["batch_id"]),
                    block_id="",
                    payload_bytes=str(item.get("size_bytes", "")),
                    queued_before=str(queued_before),
                    queued_after=str(queued_after),
                    note=f"send_exception:{type(e).__name__}"
                )
                self.dbg(f"POST failed batch={item['batch_id']} requeued; err={repr(e)}")
                time.sleep(0.5)

    def _post_block(self, item: Dict[str, Any]) -> Optional[str]:
        phase = self.phase_ctrl.current_phase
        # Software impairment may add latency/drop for POST
        if self.software_impairment and phase == "impaired":
            time.sleep(self.impair_latency_ms / 1000.0)
            if random.random() < (self.impair_loss_pct / 100.0):
                raise requests.RequestException("Injected send loss (software impairment)")
        if phase == "offline":
            raise requests.RequestException("Forced offline during send")

        url = f"{self.node}/api/core/v2/blocks"
        payload = item.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("Queue item payload is not a dict")

        self.dbg(f"POST /blocks batch={item.get('batch_id')} bytes={item.get('size_bytes')} ...")
        r = self.session.post(url, json=payload, timeout=10)  # <-- json= (not data=)
        r.raise_for_status()
        obj = {}
        try:
            if "application/json" in r.headers.get("Content-Type", ""):
                obj = r.json()
        except Exception:
            obj = {}
        block_id = obj.get("blockId") or obj.get("blockID") or ""
        if block_id:
            self.dbg(f"POST ok batch={item.get('batch_id')} blockId={block_id}")
        else:
            self.dbg(f"POST ok but no blockId in response for batch={item.get('batch_id')}")
        return block_id or None

    def _watch_confirm(self, batch_id: int, block_id: str):
        send_t = time.monotonic()
        end_t = send_t + self.confirm_timeout
        last_err = None
        while time.monotonic() <= end_t and not self.stop_flag.is_set():
            try:
                url = f"{self.node}/api/core/v2/blocks/{block_id}/metadata"
                r = self.session.get(url, timeout=10)
                r.raise_for_status()
                meta = r.json()
                ms_idx = meta.get("referencedByMilestoneIndex")
                if ms_idx is not None:
                    ttc = time.monotonic() - send_t
                    self.dbg(f"CONFIRMED batch={batch_id} blockId={block_id} TTC={ttc:.2f}s ms={ms_idx}")
                    self.log_row(
                        event="anchor_confirm",
                        batch_id=str(batch_id),
                        block_id=block_id,
                        ttc_s=f"{ttc:.3f}",
                        msIndex=str(ms_idx)
                    )
                    return
            except Exception as e:
                last_err = e
            time.sleep(self.poll_interval)

        self.dbg(f"TIMEOUT batch={batch_id} blockId={block_id} last_err={type(last_err).__name__ if last_err else 'None'}")
        self.log_row(
            event="anchor_timeout",
            batch_id=str(batch_id),
            block_id=block_id,
            note=("timeout" if last_err is None else f"timeout_last_err:{type(last_err).__name__}")
        )

    # --------------------
    # Phase advancement & tc prompts
    # --------------------
    def run_controller(self):
        # Emit initial phase_change
        self.log_row(event="phase_change", note="enter normal")
        self.dbg("enter normal")
        while not self.stop_flag.is_set():
            new_phase = self.phase_ctrl.maybe_advance()
            if new_phase:
                self.log_row(event="phase_change", note=f"enter {new_phase}")
                self.dbg(f"enter {new_phase}")
                if new_phase == "recovery" and self.recovery_start_rel_s is None:
                    self.recovery_start_rel_s = rel_seconds(self.t0)

                if not self.software_impairment and new_phase == "impaired":
                    self._print_tc_start()
                    input("Apply the above tc command and press Enter to continue...")
                    self.log_row(event="note", note="user-continued after tc add")
                if not self.software_impairment and new_phase == "recovery":
                    self._print_tc_clear()
                    input("Clear the tc qdisc and press Enter to continue...")
                    self.log_row(event="note", note="user-continued after tc del")

            time.sleep(0.2)

    def _print_tc_start(self):
        print("\nTo start impairment (fill <IFACE>):")
        print(f"  sudo tc qdisc add dev <IFACE> root netem delay {self.impair_latency_ms}ms loss {self.impair_loss_pcnt_str()}%\n")

    def _print_tc_clear(self):
        print("\nTo clear impairment:")
        print("  sudo tc qdisc del dev <IFACE> root netem\n")

    def impair_loss_pcnt_str(self) -> str:
        try:
            return f"{float(self.impair_loss_pct):g}"
        except Exception:
            return str(self.impair_loss_pct)

    # --------------------
    # Shutdown
    # --------------------
    def stop(self):
        self.stop_flag.set()
        try:
            # Persist any current batch into queue
            with self._batch_lock:
                if self._current_batch:
                    batch_id = self._next_batch_id
                    self._next_batch_id += 1
                    payload_dict, size_bytes = self._build_ledger_commit(self._current_batch)
                    self._current_batch = []
                    self.queue.enqueue({
                        "batch_id": batch_id,
                        "payload": payload_dict,
                        "created_iso": utc_now_iso(),
                        "size_bytes": size_bytes
                    })
                    self.dbg(f"persisted partial batch id={batch_id} at shutdown")
        except Exception:
            pass
        self.logger.close()


# ===========================
# Plotting
# ===========================
def make_plots(csv_path: str, outdir: str, dpi: int, recovery_start_rel_s: Optional[float]) -> Dict[str, Any]:
    try:
        df = pd.read_csv(csv_path)
    except Exception:
        print("[Plots] Could not read CSV for plotting.")
        return {}

    stats: Dict[str, Any] = {}

    # Box plot of t_decide_ms by phase
    try:
        d = df[(df["event"] == "decision") & df["t_decide_ms"].notna()].copy()
        d["t_decide_ms"] = pd.to_numeric(d["t_decide_ms"], errors="coerce")
        d = d.dropna(subset=["t_decide_ms", "phase"])
        phases = ["normal", "impaired", "offline", "recovery"]
        data = [d.loc[d["phase"] == p, "t_decide_ms"].values for p in phases]

        fig, ax = plt.subplots(figsize=(7, 4.2))
        bp = ax.boxplot(data, labels=phases, patch_artist=True, showfliers=True)
        for b in bp["boxes"]:
            b.set_alpha(0.9)
        ax.set_ylabel("Decision latency (ms)")
        ax.set_xlabel("Phase")
        fig.tight_layout()
        path = os.path.join(outdir, "test3_tdecide_box.png")
        fig.savefig(path, dpi=dpi)
        plt.close(fig)
        print(f"[Plots] Wrote {path}")

        # stats: median & P95 per phase
        med_p95 = {}
        for p, arr in zip(phases, data):
            if len(arr) > 0:
                med_p95[p] = {"median": float(np.median(arr)), "p95": float(np.percentile(arr, 95))}
            else:
                med_p95[p] = {"median": float("nan"), "p95": float("nan")}
        stats["t_decide"] = med_p95
    except Exception as e:
        print(f"[Plots] t_decide box plot failed: {e}")

    # Queue depth vs time with phase bands
    try:
        q = df[df["event"] == "queue_depth"].copy()
        q["t_rel_s"] = pd.to_numeric(q["t_rel_s"], errors="coerce")
        q["queued_after"] = pd.to_numeric(q["queued_after"], errors="coerce")
        q = q.dropna(subset=["t_rel_s", "queued_after"])

        # Phase bands from phase_change events
        pc = df[df["event"] == "phase_change"].copy()
        pc["t_rel_s"] = pd.to_numeric(pc["t_rel_s"], errors="coerce")
        pc = pc.dropna(subset=["t_rel_s"]).sort_values("t_rel_s")
        times = pc["t_rel_s"].tolist()
        labels = pc["note"].fillna("").tolist()

        spans: List[Tuple[float, str]] = []
        for i, t in enumerate(times):
            label = labels[i]
            if "impaired" in label:
                name = "impaired"
            elif "offline" in label:
                name = "offline"
            elif "recovery" in label:
                name = "recovery"
            else:
                name = "normal"
            spans.append((t, name))
        t_end = float(df["t_rel_s"].astype(float).max()) if not df.empty else 0.0

        fig, ax = plt.subplots(figsize=(9, 4.2))
        # background bands
        for i, (start, name) in enumerate(spans):
            end = spans[i + 1][0] if i + 1 < len(spans) else t_end
            ax.axvspan(start, end, alpha=0.08, label=name if i == 0 else None)
        ax.plot(q["t_rel_s"], q["queued_after"], lw=1.5)
        ax.set_xlabel("Time (s)")
        ax.set_ylabel("Queue size (batches)")
        if spans:
            ax.legend(frameon=False, fontsize=8)
        fig.tight_layout()
        path = os.path.join(outdir, "test3_queue_depth_over_time.png")
        fig.savefig(path, dpi=dpi)
        plt.close(fig)
        print(f"[Plots] Wrote {path}")
    except Exception as e:
        print(f"[Plots] Queue depth plot failed: {e}")

    # Catch-up timeline: anchor send vs confirm timestamps; vertical line at recovery start
    try:
        s = df[df["event"] == "anchor_send"].copy()
        c = df[df["event"] == "anchor_confirm"].copy()
        for z in (s, c):
            z["t_rel_s"] = pd.to_numeric(z["t_rel_s"], errors="coerce")
        s = s.dropna(subset=["t_rel_s"])
        c = c.dropna(subset=["t_rel_s"])
        fig, ax = plt.subplots(figsize=(9, 4.2))
        ax.scatter(s["t_rel_s"], np.zeros(len(s)), s=12, label="send", alpha=0.7)
        ax.scatter(c["t_rel_s"], np.ones(len(c)), s=12, label="confirm", alpha=0.7)
        if recovery_start_rel_s is not None:
            ax.axvline(recovery_start_rel_s, linestyle="--", lw=1.2)

        ax.set_yticks([0, 1])
        ax.set_yticklabels(["send", "confirm"])
        ax.set_xlabel("Time (s)")
        ax.set_title("Anchors: send vs confirm timeline")
        ax.legend(frameon=False)
        fig.tight_layout()
        path = os.path.join(outdir, "test3_catchup_timeline.png")
        fig.savefig(path, dpi=dpi)
        plt.close(fig)
        print(f"[Plots] Wrote {path}")

        # Catch-up time: from recovery start until last confirm after recovery
        catchup_time_s = float("nan")
        if recovery_start_rel_s is not None and not c.empty:
            after = c.loc[c["t_rel_s"] >= recovery_start_rel_s, "t_rel_s"]
            if not after.empty:
                catchup_time_s = float(after.max() - recovery_start_rel_s)
        stats["catchup_time_s"] = catchup_time_s
    except Exception as e:
        print(f"[Plots] Catch-up timeline failed: {e}")

    # ECDF of anchor TTC during recovery
    try:
        rc = df[(df["event"] == "anchor_confirm") & (df["phase"] == "recovery")].copy()
        rc["ttc_s"] = pd.to_numeric(rc["ttc_s"], errors="coerce")
        rc = rc.dropna(subset=["ttc_s"])
        vals = rc["ttc_s"].values
        if len(vals) > 0:
            x = np.sort(vals)
            y = np.arange(1, len(x) + 1) / len(x)
            fig, ax = plt.subplots(figsize=(6, 4))
            ax.plot(x, y, lw=2)
            ax.set_xlabel("Anchor TTC during recovery (s)")
            ax.set_ylabel("ECDF")
            ax.set_ylim(0, 1)
            fig.tight_layout()
            path = os.path.join(outdir, "test3_ttc_recovery_ecdf.png")
            fig.savefig(path, dpi=dpi)
            plt.close(fig)
            print(f"[Plots] Wrote {path}")
            stats["recovery_ttc_median"] = float(np.median(vals))
        else:
            stats["recovery_ttc_median"] = float("nan")
    except Exception as e:
        print(f"[Plots] TTC ECDF failed: {e}")

    # Max observed queue depth
    try:
        q = df[df["event"] == "queue_depth"].copy()
        q["queued_after"] = pd.to_numeric(q["queued_after"], errors="coerce")
        stats["max_queue_depth"] = int(q["queued_after"].max()) if not q["queued_after"].dropna().empty else 0
    except Exception:
        stats["max_queue_depth"] = 0

    # anchor_timeout counts per phase
    try:
        tmo = df[df["event"] == "anchor_timeout"]
        cnt = tmo.groupby("phase")["event"].count().to_dict()
        stats["timeout_counts"] = cnt
    except Exception:
        stats["timeout_counts"] = {}

    return stats


# ===========================
# Summary writer
# ===========================
def write_summary(stats: Dict[str, Any], outpath: str) -> None:
    lines = []
    lines.append("Test 3 — Partition-Tolerance Summary")
    lines.append("-------------------------------------")
    td = stats.get("t_decide", {})
    if td:
        lines.append("Decision latency (ms) by phase:")
        for phase in ["normal", "impaired", "offline", "recovery"]:
            ph = td.get(phase, {"median": float("nan"), "p95": float("nan")})
            med = ph["median"]
            p95 = ph["p95"]
            med_s = "nan" if not isinstance(med, float) or math.isnan(med) else f"{med:.2f}"
            p95_s = "nan" if not isinstance(p95, float) or math.isnan(p95) else f"{p95:.2f}"
            lines.append(f"  - {phase:8s}: median={med_s}  P95={p95_s}")
    lines.append(f"Max queue depth: {stats.get('max_queue_depth', 'n/a')}")
    ct = stats.get("catchup_time_s", float("nan"))
    if isinstance(ct, float) and math.isfinite(ct):
        lines.append(f"Catch-up time from recovery start: {ct:.2f} s")
    else:
        lines.append("Catch-up time from recovery start: n/a")

    tmo = stats.get("timeout_counts", {})
    lines.append("Anchor timeouts per phase:")
    if tmo:
        for k, v in tmo.items():
            lines.append(f"  - {k}: {v}")
    else:
        lines.append("  - none recorded")

    rmed = stats.get("recovery_ttc_median", float("nan"))
    if isinstance(rmed, float) and math.isfinite(rmed):
        lines.append(f"Median anchor TTC during recovery: {rmed:.2f} s")
    else:
        lines.append("Median anchor TTC during recovery: n/a")

    with open(outpath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"[Summary] Wrote {outpath}")


# ===========================
# Main
# ===========================
def main():
    args = parse_args()
    random.seed(args.seed)
    np.random.seed(args.seed)

    gw = GatewayEmu(args)

    # Threads (named for clear console logs)
    t_decision   = threading.Thread(target=gw.decision_loop,    name="decision",   daemon=True)
    t_snapshot   = threading.Thread(target=gw.snapshotter_loop, name="snapshot",   daemon=True)
    t_sender     = threading.Thread(target=gw.sender_loop,      name="sender",     daemon=True)
    t_controller = threading.Thread(target=gw.run_controller,   name="controller", daemon=True)
    t_qticker    = threading.Thread(target=gw.queue_ticker_loop,name="qticker",    daemon=True)

    try:
        t_controller.start()
        t_decision.start()
        t_snapshot.start()
        t_sender.start()
        t_qticker.start()

        # Run until all phase durations elapse
        total_secs = args.normal_secs + args.impaired_secs + args.offline_secs + args.recovery_secs
        time.sleep(total_secs)

    except KeyboardInterrupt:
        print("\n[Main] KeyboardInterrupt: stopping...")
    finally:
        gw.stop()

    # Plots + summary
    stats = make_plots(gw.csv_path, gw.outdir, gw.dpi, gw.recovery_start_rel_s)
    write_summary(stats, gw.summary_path)


if __name__ == "__main__":
    main()
