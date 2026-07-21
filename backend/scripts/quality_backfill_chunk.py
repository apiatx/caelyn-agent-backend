"""
Resumable quality backfill — processes one chunk of N symbols per invocation.
State file: /tmp/qbf_state.json
Usage:  python3.11 quality_backfill_chunk.py [chunk_size]
"""
from __future__ import annotations
import asyncio, json, os, sys, time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
_BACKEND = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
os.chdir(_BACKEND)

from data.pg_storage import watchlist_list
from services.watchlist_quote_cache import is_fmp_symbol_eligible
from services.watchlist_service import load_watchlist
from services.watchlist_fundamentals_refresh import FmpFundamentalsRefresher

STATE_FILE  = "/tmp/qbf_state.json"
CHUNK_SIZE  = int(sys.argv[1]) if len(sys.argv) > 1 else 10


def _get_all_pairs() -> list[tuple[str, str]]:
    seen: set[str] = set()
    result: list[tuple[str, str]] = []
    for wl in (watchlist_list() or []):
        wl_id = wl.get("id", "")
        wl_data = load_watchlist(wl_id) or {}
        for t in (wl_data.get("tickers") or []):
            sym = (t if isinstance(t, str) else (t or {}).get("symbol", "")).upper()
            if not sym or sym in seen:
                continue
            seen.add(sym)
            if is_fmp_symbol_eligible(sym):
                result.append((sym, wl_id))
    return result


def _load_state(all_pairs: list[tuple[str, str]]) -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                st = json.load(f)
            if st.get("universe") == [s for s, _ in all_pairs]:
                return st
        except Exception:
            pass
    return {
        "universe":  [s for s, _ in all_pairs],
        "wl_map":    {s: w for s, w in all_pairs},
        "done":      [],
        "failed":    [],
        "empty":     [],
        "refreshed": 0,
        "fmp_calls": 0,
        "batches":   [],
        "started_at": datetime.now(timezone.utc).isoformat(),
    }


def _save_state(st: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(st, f)


async def run_chunk() -> None:
    fmp_key = os.getenv("FMP_API_KEY", "")
    if not fmp_key:
        print("[QBF] ERROR: FMP_API_KEY not set"); return

    all_pairs = _get_all_pairs()
    st        = _load_state(all_pairs)

    done_set    = set(st["done"]) | set(st["failed"]) | set(st["empty"])
    pending     = [(s, st["wl_map"].get(s, "")) for s in st["universe"] if s not in done_set]
    total       = len(st["universe"])
    n_done      = len(done_set)

    print(f"[QBF] universe={total} done={n_done} pending={len(pending)} chunk={CHUNK_SIZE}")

    if not pending:
        print("[QBF] ALL COMPLETE"); _save_state(st); return

    chunk = pending[:CHUNK_SIZE]

    # Group chunk by wl_id
    by_wl: dict[str, list[str]] = {}
    for sym, wl_id in chunk:
        by_wl.setdefault(wl_id, []).append(sym)

    refresher = FmpFundamentalsRefresher(fmp_key)
    batch_start = time.time()

    for wl_id, syms in by_wl.items():
        print(f"[QBF]   batch wl={wl_id[:8]} syms={syms}")
        try:
            res = await refresher.refresh_symbols(syms, wl_id, dev_force=True)
        except Exception as e:
            print(f"[QBF]   ERROR: {e}")
            res = {"refreshed": [], "failed": syms, "skipped": [],
                   "empty_payload_preserved": [], "empty_payload_no_prior": []}

        n_ref  = len(res.get("refreshed", []))
        n_fail = len(res.get("failed", []))
        n_skip = len(res.get("skipped", []))
        n_ep   = len(res.get("empty_payload_preserved", []))
        n_eno  = len(res.get("empty_payload_no_prior", []))

        st["done"]    += res.get("refreshed", [])
        st["failed"]  += res.get("failed", [])
        st["empty"]   += res.get("empty_payload_preserved", []) + res.get("empty_payload_no_prior", [])
        st["refreshed"] += n_ref
        st["fmp_calls"] += n_ref * 10

        elapsed = round(time.time() - batch_start, 1)
        st["batches"].append({
            "wl": wl_id[:8], "syms": syms,
            "ref": n_ref, "fail": n_fail, "skip": n_skip,
            "ep": n_ep, "eno": n_eno, "elapsed_s": elapsed,
            "failed_syms": res.get("failed", []),
        })

        print(f"[QBF]   ref={n_ref} fail={n_fail} skip={n_skip} ep={n_ep} eno={n_eno} elapsed={elapsed}s")

    _save_state(st)
    remaining = len(pending) - CHUNK_SIZE
    print(f"[QBF] chunk done — remaining={max(0, remaining)} refreshed_total={st['refreshed']} fmp_calls={st['fmp_calls']}")


if __name__ == "__main__":
    asyncio.run(run_chunk())
