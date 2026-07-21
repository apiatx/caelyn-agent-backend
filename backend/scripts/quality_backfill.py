"""
One-time Quality backfill — runs through the existing canonical path:
  FmpFundamentalsRefresher.refresh_symbols(..., dev_force=True)

Processes all FMP-eligible Watchlist symbols in batches of 20.
Writes progress to /tmp/quality_backfill_progress.json after each batch.
Safe to interrupt; progress file shows last completed batch.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.chdir(os.path.join(os.path.dirname(__file__), ".."))

from data.pg_storage import watchlist_list
from services.watchlist_quote_cache import is_fmp_symbol_eligible
from services.watchlist_service import load_watchlist
from data.watchlist_fundamentals_store import get_snapshots_bulk
from services.watchlist_fundamentals_refresh import FmpFundamentalsRefresher

QUALITY_FIELDS = [
    "Cash", "Net Cash / Debt", "Current Ratio", "Operating Margin",
    "ROIC", "FCF Conversion", "Diluted Shares Growth YoY",
    "Revenue Acceleration", "Forward Revenue Growth", "Forward P/S",
    "Altman Z-Score", "Altman Z-Risk", "Piotroski Score",
    "Interest Coverage", "FCF Yield", "SBC / Revenue",
    "Gross Margin Change YoY", "Incremental Operating Margin",
]

BATCH_SIZE   = 20
PROGRESS_FILE = "/tmp/quality_backfill_progress.json"


def _load_eligible_symbols() -> list[tuple[str, str]]:
    """Returns list of (symbol, watchlist_id) deduped by symbol."""
    seen: set[str] = set()
    result: list[tuple[str, str]] = []
    for wl in (watchlist_list() or []):
        wl_id = wl.get("id", "")
        wl_data = load_watchlist(wl_id) or {}
        for t in (wl_data.get("tickers") or []):
            sym = (t if isinstance(t, str) else (t or {}).get("symbol", "")).upper()
            if not sym or sym in seen:
                continue
            if not is_fmp_symbol_eligible(sym):
                continue
            seen.add(sym)
            result.append((sym, wl_id))
    return result


def _has_quality(fields: dict) -> bool:
    return any(fields.get(qf) is not None for qf in QUALITY_FIELDS)


def _save_progress(data: dict) -> None:
    try:
        with open(PROGRESS_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"[BACKFILL] progress save error: {e}")


async def run_backfill() -> None:
    fmp_key = os.getenv("FMP_API_KEY", "")
    if not fmp_key:
        print("[BACKFILL] ERROR: FMP_API_KEY not set")
        return

    refresher = FmpFundamentalsRefresher(fmp_key)

    print("[BACKFILL] Loading eligible symbols ...")
    sym_wl_pairs = _load_eligible_symbols()
    total        = len(sym_wl_pairs)
    print(f"[BACKFILL] {total} eligible symbols × 10 = {total * 10} estimated FMP calls")

    # Build batches grouped by watchlist_id
    batches: list[tuple[str, list[str]]] = []
    current_wl: str | None = None
    current_batch: list[str] = []
    for sym, wl_id in sym_wl_pairs:
        if current_wl is None:
            current_wl = wl_id
        if wl_id != current_wl or len(current_batch) >= BATCH_SIZE:
            if current_batch:
                batches.append((current_wl, current_batch))
            current_wl    = wl_id
            current_batch = [sym]
        else:
            current_batch.append(sym)
    if current_batch and current_wl:
        batches.append((current_wl, current_batch))

    totals = {
        "refreshed": 0, "failed": 0, "skipped": 0,
        "empty_preserved": 0, "empty_no_prior": 0,
        "fmp_calls_used": 0,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "batches_done": 0,
        "batches_total": len(batches),
        "batch_log": [],
    }

    backfill_start = time.time()

    for batch_num, (wl_id, syms) in enumerate(batches, 1):
        batch_start = time.time()
        print(
            f"[BACKFILL] batch {batch_num}/{len(batches)} — "
            f"wl={wl_id[:8]} syms={syms}"
        )
        try:
            res = await refresher.refresh_symbols(syms, wl_id, dev_force=True)
        except Exception as e:
            print(f"[BACKFILL] batch {batch_num} ERROR: {e}")
            res = {"refreshed": [], "failed": syms, "skipped": [],
                   "empty_payload_preserved": [], "empty_payload_no_prior": []}

        elapsed = round(time.time() - batch_start, 1)
        n_ref  = len(res.get("refreshed", []))
        n_fail = len(res.get("failed",    []))
        n_skip = len(res.get("skipped",   []))
        n_ep   = len(res.get("empty_payload_preserved", []))
        n_eno  = len(res.get("empty_payload_no_prior",  []))
        calls  = n_ref * 10

        totals["refreshed"]     += n_ref
        totals["failed"]        += n_fail
        totals["skipped"]       += n_skip
        totals["empty_preserved"] += n_ep
        totals["empty_no_prior"]  += n_eno
        totals["fmp_calls_used"]  += calls
        totals["batches_done"]     = batch_num

        batch_entry = {
            "batch": batch_num,
            "wl_id": wl_id[:8],
            "symbols": syms,
            "refreshed": n_ref,
            "failed":    n_fail,
            "skipped":   n_skip,
            "empty_preserved": n_ep,
            "empty_no_prior":  n_eno,
            "fmp_calls": calls,
            "elapsed_s": elapsed,
            "failed_syms":  res.get("failed", []),
        }
        totals["batch_log"].append(batch_entry)
        _save_progress(totals)

        print(
            f"[BACKFILL]   refreshed={n_ref} failed={n_fail} skipped={n_skip} "
            f"empty_preserved={n_ep} calls={calls} elapsed={elapsed}s"
        )

    total_elapsed = round(time.time() - backfill_start, 1)
    totals["finished_at"]     = datetime.now(timezone.utc).isoformat()
    totals["total_elapsed_s"] = total_elapsed
    _save_progress(totals)

    print(f"\n[BACKFILL] COMPLETE — total_elapsed={total_elapsed}s")
    print(f"  refreshed={totals['refreshed']} failed={totals['failed']} "
          f"skipped={totals['skipped']} fmp_calls={totals['fmp_calls_used']}")

    # Final quality coverage check
    all_syms = [s for s, _ in sym_wl_pairs]
    snaps = get_snapshots_bulk(all_syms)
    q_complete = sum(1 for s in all_syms if _has_quality(snaps.get(s, {}).get("fields") or {}))
    print(f"  post-backfill quality-complete: {q_complete}/{len(all_syms)}")


if __name__ == "__main__":
    asyncio.run(run_backfill())
