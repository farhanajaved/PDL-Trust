#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test3_partition_tolerance_v0_3.py — Gateway Trust Cache partition-tolerance test
Verbose debug build (v0.3, single-file)

What this does
--------------
- Decision loop: emits local "decisions" at a fixed rate and batches them
- Durable queue: every batch becomes a ~2.5 KB LedgerCommit payload (synthetic)
- Sender: dequeues, POSTs to Hornet /api/core/v2/blocks (JSON body), then watches for confirmation
- Snapshotter: heartbeats /api/core/v2/info on cadence; OK resets cache-age; MISS ages cache
- Phases: normal → impaired → offline → recovery (with software impairment by default)
- Logging: every event → CSV (append-safe); periodic queue_depth rows; loud console prints

Outputs (under base-dir/results-dir)
------------------------------------
- test3_partition.csv
- test3_partition_bak.csv
- test3_anchor_queue.jsonl   (durable queue)
"""

import os
import sys
import csv
import json
import time
import random
import shutil
import requests
import threading
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

__VERSION__ = "test3 v0.3 debug-sender (single-file)"
print(__VERSION__, flush=True)

# -------------------------
# Small utilities
# -------------------------
def utc_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")

def safe_sleep(sec: float) -> None:
    try:
        time.sleep(sec)
    except KeyboardInterrupt:
        pass

# -------------------------
# CSV Logger (append-safe)
# -------------------------
class CsvLogger:
    FIELDS = [
        "t_wall_iso","t_rel_s","phase","event",
        "cache_age_s","delta_snap_s",
        "decision_id","t_decide_ms","permit",
        "batch_id","block_id","payload_bytes","queued_before","queued_after",
        "ttc_s","msIndex",
        "snapshot_ok","cache_age_before_s","cache_age_after_s",
        "req_fail_injected","extra_rtt_ms","note"
    ]

    def __init__(self, path: str, bak_path: str):
        self.path = path
        self.bak_path = bak_path
        self._rows = 0
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if not os.path.exists(self.path) or os.path.getsize(self.path) == 0:
            with open(self.path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=self.FIELDS)
                w.writeheader()
                f.flush()
        # no file-handle kept open (append on each write)

    def write(self, row: Dict[str, Any]) -> None:
        # Ensure only known fields, and strings
        out = {}
        for k in self.FIELDS:
            v = row.get(k, "")
            if v is None:
                v = ""
            out[k] = v
        with open(self.path, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=self.FIELDS)
            w.writerow(out)
            f.flush()
        self._rows += 1
        if self._rows % 200 == 0:
            try:
                shutil.copy(self.path, self.bak_path)
            except Exception:
                pass

# -------------------------
# Durable Queue (jsonl)
# -------------------------
class DurableQueue:
    """
    Append-only JSONL file + in-memory list.
    Each item: {"batch_id": int, "payload": dict, "size_bytes": int}
    """
    def __init__(self, jsonl_path: str):
        self.path = jsonl_path
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._lock = threading.Lock()
        self._mem: List[Dict[str, Any]] = []
        self._load()

    def _load(self):
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        if isinstance(obj, dict) and "batch_id" in obj:
                            self._mem.append(obj)
                    except Exception:
                        pass
        except Exception:
            pass

    def _append_line(self, obj: Dict[str, Any]) -> None:
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, separators=(",", ":")) + "\n")
            f.flush()

    def enqueue(self, obj: Dict[str, Any]) -> None:
        with self._lock:
            self._mem.append(obj)
            self._append_line(obj)

    def requeue_front(self, obj: Dict[str, Any]) -> None:
        with self._lock:
            self._mem.insert(0, obj)
            # rewrite whole file to reflect current memory order
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                for it in self._mem:
                    f.write(json.dumps(it, separators=(",", ":")) + "\n")
                f.flush()
            try:
                os.replace(tmp, self.path)
            except Exception:
                pass

    def dequeue(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            if not self._mem:
                return None
            obj = self._mem.pop(0)
            # rewrite file to reflect new order
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                for it in self._mem:
                    f.write(json.dumps(it, separators=(",", ":")) + "\n")
                f.flush()
            try:
                os.replace(tmp, self.path)
            except Exception:
                pass
            return obj

    def size(self) -> int:
        with self._lock:
            return len(self._mem)

# -------------------------
# Phase Controller
# -------------------------
class PhaseController:
    def __init__(self, seq: List[Tuple[str, int]]):
        self.seq = seq[:]  # list of (phase, seconds)
        self.current_phase = "normal"
        self.start_wall = time.time()
        self.phase_start = self.start_wall

    def run(self, on_change):
        for phase, secs in self.seq:
            self.current_phase = phase
            self.phase_start = time.time()
            on_change(phase)
            for _ in range(secs):
                time.sleep(1)
        self.current_phase = "done"
        on_change("done")

    def t_rel_s(self) -> float:
        return max(0.0, time.time() - self.start_wall)

# -------------------------
# Gateway Emulator
# -------------------------
class GatewayEmu:
    def __init__(self, args):
        random.seed(args.seed)

        # Config
        self.node = args.node.rstrip("/")
        self.delta_snap = float(args.delta_snap)
        self.decisions_per_sec = float(args.decisions_per_sec)
        self.batch_size = int(args.batch_size)
        self.payload_bytes = int(args.payload_bytes)
        self.poll_interval = float(args.poll_interval)
        self.confirm_timeout = float(args.confirm_timeout)
        self.impair_latency_ms = int(args.impair_latency_ms)
        self.impair_loss_pct = float(args.impair_loss_pct)
        self.software_impairment = bool(args.software_impairment)

        # Paths
        self.outdir = os.path.join(args.base_dir, args.results_dir)
        os.makedirs(self.outdir, exist_ok=True)
        self.csv_path = os.path.join(self.outdir, "test3_partition.csv")
        self.bak_path = os.path.join(self.outdir, "test3_partition_bak.csv")
        self.queue_path = os.path.join(self.outdir, "test3_anchor_queue.jsonl")

        # IO + state
        self.logger = CsvLogger(self.csv_path, self.bak_path)
        self.queue = DurableQueue(self.queue_path)
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self.stop_flag = threading.Event()

        # Cache age state
        self._cache_lock = threading.Lock()
        self._last_snapshot_ok = time.time()  # pretend just refreshed
        self._phase: Optional[PhaseController] = None

        # IDs
        self._next_decision_id = 1
        self._current_batch: List[Dict[str, Any]] = []
        self._batch_lock = threading.Lock()

    # ---------- debug print ----------
    def dbg(self, tag: str, msg: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        phase = self._phase.current_phase if self._phase else "init"
        print(f"[{ts}] [{tag}] [{phase}] {msg}", flush=True)

    # ---------- csv rows ----------
    def _log(self, **kw) -> None:
        row = {k: "" for k in self.logger.FIELDS}
        row.update(kw)
        row["t_wall_iso"] = utc_iso()
        row["t_rel_s"] = f"{self._phase.t_rel_s():.3f}" if self._phase else "0.000"
        row["phase"] = self._phase.current_phase if self._phase else "init"
        # set cache age and delta_snap if not provided
        if "cache_age_s" not in row or row["cache_age_s"] == "":
            row["cache_age_s"] = f"{self.cache_age_seconds():.3f}"
        if "delta_snap_s" not in row or row["delta_snap_s"] == "":
            row["delta_snap_s"] = f"{self.delta_snap:.1f}"
        self.logger.write(row)

    # ---------- cache helpers ----------
    def cache_age_seconds(self) -> float:
        with self._cache_lock:
            return max(0.0, time.time() - self._last_snapshot_ok)

    def reset_cache(self) -> None:
        with self._cache_lock:
            self._last_snapshot_ok = time.time()

    # ---------- decision loop ----------
    def decision_loop(self):
        self.dbg("decision", f"decision_loop start dps={self.decisions_per_sec} batch_size={self.batch_size}")
        target_period = 1.0 / max(0.1, self.decisions_per_sec)
        last_depth_emit = 0.0
        while not self.stop_flag.is_set():
            t0 = time.perf_counter()
            age = self.cache_age_seconds()
            # policy tiers
            if age <= self.delta_snap:
                permit_p = 0.95
                delay_ms = 0.0
            elif age <= 2 * self.delta_snap:
                permit_p = 0.50
                delay_ms = random.uniform(2.0, 4.0)
            else:
                permit_p = 0.10
                delay_ms = random.uniform(4.0, 6.0)
            if delay_ms > 0:
                safe_sleep(delay_ms / 1000.0)

            permit = 1 if random.random() < permit_p else 0
            t_decide_ms = (time.perf_counter() - t0) * 1000.0

            did = self._next_decision_id
            self._next_decision_id += 1
            self.dbg("decision", f"decision #{did} permit={permit} cache_age={age:.2f}s t_decide={t_decide_ms:.2f}ms")
            self._log(event="decision", decision_id=str(did), t_decide_ms=f"{t_decide_ms:.3f}", permit=str(permit))

            # batching
            self._add_to_batch({"decision_id": did, "permit": permit})

            # periodic queue depth row
            now = time.time()
            if now - last_depth_emit >= 1.0:
                last_depth_emit = now
                self._log(event="queue_depth", queued_after=str(self.queue.size()))

            # pacing
            elapsed = time.perf_counter() - t0
            sleep_left = max(0.0, target_period - elapsed)
            safe_sleep(sleep_left)

    def _add_to_batch(self, item: Dict[str, Any]) -> None:
        with self._batch_lock:
            self._current_batch.append(item)
            if len(self._current_batch) >= self.batch_size:
                batch = self._current_batch
                self._current_batch = []
                batch_id = int(time.time() * 1000) % 1_000_000
                payload_dict, sz = self._make_ledger_commit(batch, batch_id)
                self.queue.enqueue({"batch_id": batch_id, "payload": payload_dict, "size_bytes": sz})
                self.dbg("decision", f"batch_enqueued id={batch_id} size={sz}B queue={self.queue.size()}")
                self._log(event="anchor_enqueue", batch_id=str(batch_id),
                          payload_bytes=str(sz), queued_after=str(self.queue.size()))

    def _make_ledger_commit(self, items: List[Dict[str, Any]], batch_id: int) -> Tuple[Dict[str, Any], int]:
        # synthetic inner body (LedgerCommit)
        inner = {
            "type": "LedgerCommit",
            "version": "1.0",
            "batchSize": len(items),
            "batchId": batch_id,
            "createdAt": utc_iso(),
            "items": [{"device": f"d{i%8}", "action": "allow" if it["permit"] else "deny", "version": i % 3}
                      for i, it in enumerate(items, 1)]
        }
        raw = json.dumps(inner, separators=(",", ":")).encode("utf-8")
        if len(raw) < self.payload_bytes:
            raw += b" " * (self.payload_bytes - len(raw))
        # hornet block payload
        payload = {
            "protocolVersion": 2,
            "payload": {
                "type": 5,
                "tag": "0x67617465776179",           # "gateway"
                "data": "0x" + raw.hex()
            }
        }
        return payload, len(raw)

    # ---------- snapshot loop ----------
    def snapshot_loop(self):
        self.dbg("snapshot", "snapshot_loop start")
        while not self.stop_flag.is_set():
            injected = 0
            extra_rtt = 0
            ok = 0

            try:
                phase = self._phase.current_phase
            except Exception:
                phase = "init"

            if self.software_impairment and phase == "impaired":
                extra_rtt = self.impair_latency_ms
                safe_sleep(self.impair_latency_ms / 1000.0)
                if random.random() < (self.impair_loss_pct / 100.0):
                    injected = 1
                    # fail without request
                else:
                    ok = self._snapshot_once()
            elif phase == "offline":
                ok = 0
            else:
                ok = self._snapshot_once()

            before = f"{self.cache_age_seconds():.3f}"
            if ok:
                self.reset_cache()
                after = "0.000"
                self.dbg("snapshot", "snapshot OK (cache_age reset)")
                self._log(event="snapshot_tick", snapshot_ok="1",
                          cache_age_before_s=before, cache_age_after_s=after,
                          req_fail_injected=str(injected), extra_rtt_ms=str(extra_rtt))
            else:
                after = f"{self.cache_age_seconds():.3f}"
                self.dbg("snapshot", f"snapshot MISS (offline/impair); cache_age now {after}s")
                self._log(event="snapshot_tick", snapshot_ok="0",
                          cache_age_before_s=before, cache_age_after_s=after,
                          req_fail_injected=str(injected), extra_rtt_ms=str(extra_rtt))

            safe_sleep(self.delta_snap)

    def _snapshot_once(self) -> int:
        try:
            r = self.session.get(f"{self.node}/api/core/v2/info", timeout=8)
            if r.status_code == 200:
                return 1
        except Exception:
            pass
        return 0

    # ---------- queue ticker ----------
    def queue_ticker(self):
        self.dbg("qticker", "queue_ticker start")
        while not self.stop_flag.is_set():
            try:
                self._log(event="queue_depth", queued_after=str(self.queue.size()))
                self.dbg("qticker", f"queue_depth={self.queue.size()}")
            except Exception:
                pass
            safe_sleep(1.0)

    # ---------- sender ----------
    def sender_loop(self):
        self.dbg("sender", "sender_loop start")
        idle_ticks = 0
        while not self.stop_flag.is_set():
            item = self.queue.dequeue()
            if item is None:
                idle_ticks += 1
                if idle_ticks % 10 == 0:
                    self.dbg("sender", "idle (queue empty)")
                safe_sleep(0.2)
                continue
            idle_ticks = 0
            q_before = self.queue.size() + 1
            batch_id = item.get("batch_id")
            try:
                self.dbg("sender", f"dequeued batch={batch_id} q_before={q_before}")
                block_id = self._post_block(item)
                q_after = self.queue.size()
                self.dbg("sender", f"POST ok batch={batch_id} blockId={block_id} q_after={q_after}")
                self._log(event="anchor_send", batch_id=str(batch_id), block_id=(block_id or ""),
                          payload_bytes=str(item.get("size_bytes", "")),
                          queued_before=str(q_before), queued_after=str(q_after))
                if block_id:
                    t = threading.Thread(target=self._watch_confirm, args=(batch_id, block_id), daemon=True)
                    t.start()
            except Exception as e:
                self.dbg("sender", f"POST failed batch={batch_id} err={type(e).__name__}: {e}")
                self.queue.requeue_front(item)
                self._log(event="anchor_send", batch_id=str(batch_id), block_id="",
                          payload_bytes=str(item.get("size_bytes", "")),
                          queued_before=str(q_before), queued_after=str(self.queue.size()),
                          note=f"send_exception:{type(e).__name__}")
                safe_sleep(0.5)

    def _post_block(self, item: Dict[str, Any]) -> Optional[str]:
        self.dbg("sender", f"POST /blocks batch={item.get('batch_id')}")
        # impairment on send
        phase = self._phase.current_phase
        if self.software_impairment and phase == "impaired":
            safe_sleep(self.impair_latency_ms / 1000.0)
            if random.random() < (self.impair_loss_pct / 100.0):
                raise requests.RequestException("Injected send loss (software impairment)")
        if phase == "offline":
            raise requests.RequestException("Forced offline during send")

        url = f"{self.node}/api/core/v2/blocks"
        # IMPORTANT: json= (proper JSON body)
        r = self.session.post(url, json=item["payload"], timeout=10)
        self.dbg("sender", f"POST status={r.status_code}")
        r.raise_for_status()
        try:
            obj = r.json()
        except Exception:
            obj = {}
        block_id = obj.get("blockId") or obj.get("blockID") or ""
        self.dbg("sender", f"block_id={block_id!s}")
        return block_id or None

    def _watch_confirm(self, batch_id: int, block_id: str):
        send_t = time.monotonic()
        end_t = send_t + self.confirm_timeout
        self.dbg("confirm", f"watch start batch={batch_id} blockId={block_id}")
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
                    self.dbg("confirm", f"CONFIRMED batch={batch_id} TTC={ttc:.3f}s msIndex={ms_idx}")
                    self._log(event="anchor_confirm", batch_id=str(batch_id), block_id=block_id,
                              ttc_s=f"{ttc:.3f}", msIndex=str(ms_idx))
                    return
            except Exception as e:
                last_err = e
            safe_sleep(self.poll_interval)
        self.dbg("confirm", f"TIMEOUT batch={batch_id} blockId={block_id}")
        self._log(event="anchor_timeout", batch_id=str(batch_id), block_id=block_id,
                  note=("timeout" if last_err is None else f"timeout_last_err:{type(last_err).__name__}"))

    # ---------- controller callback ----------
    def on_phase_change(self, phase: str) -> None:
        if phase == "done":
            self.dbg("controller", "phases complete")
            self._log(event="phase_change", note="done")
            return
        self.dbg("controller", f"enter {phase}")
        self._log(event="phase_change", note=f"enter {phase}")

    # ---------- lifecycle ----------
    def start(self, phase_ctrl: PhaseController):
        self._phase = phase_ctrl
        # threads
        t_decision  = threading.Thread(target=self.decision_loop,  name="decision",  daemon=True)
        t_snapshot  = threading.Thread(target=self.snapshot_loop,  name="snapshot",  daemon=True)
        t_sender    = threading.Thread(target=self.sender_loop,    name="sender",    daemon=True)
        t_qticker   = threading.Thread(target=self.queue_ticker,   name="qticker",   daemon=True)
        t_decision.start()
        t_snapshot.start()
        t_sender.start()
        t_qticker.start()

    def stop(self):
        self.stop_flag.set()

# -------------------------
# Main
# -------------------------
def main():
    import argparse
    p = argparse.ArgumentParser(description="Test 3 — Backhaul Impairment / Partition-Tolerance (v0.3)")
    p.add_argument("--node", default="http://172.18.211.11:14265")
    p.add_argument("--base-dir", default=".")
    p.add_argument("--results-dir", default="results")
    p.add_argument("--delta-snap", type=int, default=15)
    p.add_argument("--normal-secs", type=int, default=20)
    p.add_argument("--impaired-secs", type=int, default=20)
    p.add_argument("--offline-secs", type=int, default=15)
    p.add_argument("--recovery-secs", type=int, default=20)
    p.add_argument("--decisions-per-sec", type=float, default=5.0)
    p.add_argument("--batch-size", type=int, default=5)
    p.add_argument("--payload-bytes", type=int, default=2500)
    p.add_argument("--poll-interval", type=float, default=1.0)
    p.add_argument("--confirm-timeout", type=float, default=120.0)
    p.add_argument("--impair-latency-ms", type=int, default=120)
    p.add_argument("--impair-loss-pct", type=float, default=1.0)
    p.add_argument("--software-impairment", type=lambda s: s.lower() == "true", default=True)
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    # Resolve results directory
    args.results_dir = os.path.join(args.base_dir, args.results_dir)

    # Build phase sequence
    seq = [
        ("normal",   args.normal_secs),
        ("impaired", args.impaired_secs),
        ("offline",  args.offline_secs),
        ("recovery", args.recovery_secs),
    ]
    phase_ctrl = PhaseController(seq)

    gw = GatewayEmu(args)
    gw.start(phase_ctrl)

    try:
        # run phase timeline in main thread
        phase_ctrl.run(gw.on_phase_change)
    except KeyboardInterrupt:
        print("\n[main] KeyboardInterrupt — stopping...", flush=True)
    finally:
        gw.stop()
        # small grace period
        safe_sleep(1.0)
        print("[main] done", flush=True)

if __name__ == "__main__":
    main()
