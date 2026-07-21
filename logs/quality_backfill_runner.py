#!/usr/bin/env python3.11
"""
One-time Quality backfill — detached background operator process.
Runs FmpFundamentalsRefresher.refresh_symbols(..., dev_force=True) for all
FMP-eligible Watchlist symbols in safe sequential batches of 25.

Log:      logs/quality_backfill.log   (appended each batch)
Progress: logs/quality_backfill_progress.json  (rewritten each batch, resumable)

Safe to kill and restart — resumes from last completed symbol.
Never runs batches concurrently.
"""
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone

# ── Absolute paths — required for detached/nohup execution ──────────────────
_ROOT    = "/home/runner/workspace"
_BACKEND = f"{_ROOT}/backend"
_LOG     = f"{_ROOT}/logs/quality_backfill.log"
_PROG    = f"{_ROOT}/logs/quality_backfill_progress.json"
_PAIRS   = f"{_ROOT}/logs/quality_backfill_pairs.json"

sys.path.insert(0, _BACKEND)
os.chdir(_BACKEND)

BATCH_SIZE = 25

# ── Logging ──────────────────────────────────────────────────────────────────
def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(_LOG, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


# ── Symbol universe ───────────────────────────────────────────────────────────
def _build_pairs() -> list[list[str]]:
    from data.pg_storage import watchlist_list
    from services.watchlist_service import load_watchlist
    from services.watchlist_quote_cache import is_fmp_symbol_eligible
    seen: set[str] = set()
    pairs: list[list[str]] = []
    for wl in (watchlist_list() or []):
        wl_id = wl.get("id", "")
        for t in (load_watchlist(wl_id) or {}).get("tickers", []):
            sym = (t if isinstance(t, str) else (t or {}).get("symbol", "")).upper()
            if sym and sym not in seen and is_fmp_symbol_eligible(sym):
                seen.add(sym)
                pairs.append([sym, wl_id])
    return pairs


def _get_pairs() -> list[list[str]]:
    """Load cached pairs or rebuild from live Watchlist."""
    if os.path.exists(_PAIRS):
        try:
            pairs = json.load(open(_PAIRS))
            if pairs:
                return pairs
        except Exception:
            pass
    pairs = _build_pairs()
    with open(_PAIRS, "w") as f:
        json.dump(pairs, f)
    return pairs


# ── Progress (resumable) ──────────────────────────────────────────────────────
def _load_progress() -> dict:
    try:
        return json.load(open(_PROG))
    except Exception:
        return {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "pid": os.getpid(),
            "total_eligible": 0,
            "completed": [],
            "failed": [],
            "empty_preserved": [],
            "refreshed": 0,
            "failed_total": 0,
            "empty_total": 0,
            "fmp_calls": 0,
            "batches_done": 0,
            "last_completed_symbol": None,
            "status": "running",
        }


def _save_progress(prog: dict) -> None:
    prog["updated_at"] = datetime.now(timezone.utc).isoformat()
    with open(_PROG, "w") as f:
        json.dump(prog, f, indent=2)


# ── Main backfill loop ────────────────────────────────────────────────────────
async def run() -> None:
    fmp_key = os.getenv("FMP_API_KEY", "")
    if not fmp_key:
        _log("ERROR: FMP_API_KEY not set — aborting")
        return

    from services.watchlist_fundamentals_refresh import FmpFundamentalsRefresher
    refresher = FmpFundamentalsRefresher(fmp_key)

    pairs = _get_pairs()
    prog  = _load_progress()

    prog["pid"]            = os.getpid()
    prog["total_eligible"] = len(pairs)
    prog["status"]         = "running"
    _save_progress(prog)

    done_set = set(prog["completed"]) | set(prog["failed"]) | set(prog["empty_preserved"])
    pending  = [[s, w] for s, w in pairs if s not in done_set]

    _log(
        f"START pid={os.getpid()} total={len(pairs)} already_done={len(done_set)} "
        f"pending={len(pending)} batch_size={BATCH_SIZE} "
        f"est_fmp_calls={len(pending)*10}"
    )

    batch_num = prog["batches_done"]

    while pending:
        chunk    = pending[:BATCH_SIZE]
        wl_id    = chunk[0][1]
        syms     = [s for s, _ in chunk]
        batch_num += 1
        t0 = time.time()

        _log(
            f"BATCH {batch_num} start — "
            f"wl={wl_id[:8]} size={len(syms)} "
            f"syms={syms[:5]}{'...' if len(syms)>5 else ''} "
            f"remaining_before={len(pending)}"
        )

        try:
            res = await refresher.refresh_symbols(syms, wl_id, dev_force=True)
        except Exception as exc:
            _log(f"BATCH {batch_num} ERROR: {exc}")
            res = {
                "refreshed": [], "failed": syms,
                "skipped": [], "empty_payload_preserved": [],
                "empty_payload_no_prior": [],
            }

        elapsed = round(time.time() - t0, 1)
        n_ref   = len(res.get("refreshed", []))
        n_fail  = len(res.get("failed",    []))
        n_skip  = len(res.get("skipped",   []))
        n_ep    = len(res.get("empty_payload_preserved", []))
        n_eno   = len(res.get("empty_payload_no_prior",  []))

        prog["completed"]       += res.get("refreshed", [])
        prog["failed"]          += res.get("failed",    [])
        prog["empty_preserved"] += res.get("empty_payload_preserved", []) + res.get("empty_payload_no_prior", [])
        prog["refreshed"]        += n_ref
        prog["failed_total"]     += n_fail
        prog["empty_total"]      += n_ep + n_eno
        prog["fmp_calls"]        += n_ref * 10
        prog["batches_done"]      = batch_num
        prog["last_completed_symbol"] = (res.get("refreshed") or [""])[-1] or None

        _save_progress(prog)

        done_set = set(prog["completed"]) | set(prog["failed"]) | set(prog["empty_preserved"])
        pending  = [[s, w] for s, w in pairs if s not in done_set]

        _log(
            f"BATCH {batch_num} done — "
            f"ref={n_ref} fail={n_fail} skip={n_skip} ep={n_ep} eno={n_eno} "
            f"elapsed={elapsed}s "
            f"total_ref={prog['refreshed']} total_fail={prog['failed_total']} "
            f"fmp_calls_so_far={prog['fmp_calls']} remaining={len(pending)}"
        )
        if res.get("failed"):
            _log(f"BATCH {batch_num} failed_syms={res.get('failed')}")

    prog["status"]       = "complete"
    prog["finished_at"]  = datetime.now(timezone.utc).isoformat()
    _save_progress(prog)
    _log(
        f"COMPLETE — total_ref={prog['refreshed']} total_fail={prog['failed_total']} "
        f"fmp_calls={prog['fmp_calls']} batches={prog['batches_done']}"
    )


if __name__ == "__main__":
    asyncio.run(run())
