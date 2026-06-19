"""
Watchlist Stage 2 Analysis — disk-backed LKG + controlled off-hours warmup.

Architecture
============
  warmup_stage2(tickers)
      Fetches 400 days of Tradier daily bars for each watchlist ticker
      (batched, 4 concurrent, rate-controlled), computes analyze_symbol_stage(),
      and persists results to disk.  Never called on page render.

  get_stage2(sym) -> dict
      Reads from the in-memory LKG dict — zero I/O, safe to call inside
      _build_ticker_row() on every request.

  load_lkg()
      Called once at startup.  Reads the disk file into the in-memory dict so
      values are immediately available after a server restart.

Disk LKG : backend/data/watchlist_stage2_lkg.json
Format    : {updated_at: ISO, results: {SYM: {score, label, reason, computed_at}}}
Freshness : Skip symbols whose computed_at is < _FRESH_HOURS hours old in warmup.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ── Disk LKG path ────────────────────────────────────────────────────────────
_LKG_PATH = Path(__file__).parent.parent / "data" / "watchlist_stage2_lkg.json"

# How old a per-symbol result must be before it is recomputed
_FRESH_HOURS = 20          # hours

# Max Tradier calls in flight simultaneously (conserv. — market data budget)
_CONCURRENCY = 4

# Days of history to fetch (same as theme_rs_service)
_HIST_DAYS = 400

# Cache TTL for bar entries (1h, same as tdier_hist)
_BAR_TTL = 3600

# ── In-memory LKG ────────────────────────────────────────────────────────────
# Keyed by uppercase symbol → {score, label, reason, computed_at}
_STAGE2_LKG: dict[str, dict] = {}
_lkg_loaded_at: float = 0.0


# ── Startup ───────────────────────────────────────────────────────────────────

def load_lkg() -> None:
    """
    Load the disk LKG into _STAGE2_LKG.  Called once at server startup.
    Safe to call multiple times (idempotent).
    """
    global _lkg_loaded_at
    if not _LKG_PATH.exists():
        print("[STAGE2_WL] no disk LKG found — starting cold")
        return
    try:
        data = json.loads(_LKG_PATH.read_text())
        results = data.get("results") or {}
        _STAGE2_LKG.clear()
        _STAGE2_LKG.update({s.upper(): v for s, v in results.items()})
        _lkg_loaded_at = time.time()
        updated_at = data.get("updated_at", "unknown")
        non_null = sum(1 for v in _STAGE2_LKG.values() if v.get("score") is not None)
        print(
            f"[STAGE2_WL] disk LKG loaded: {len(_STAGE2_LKG)} symbols "
            f"({non_null} with stage data) updated_at={updated_at}"
        )
    except Exception as exc:
        print(f"[STAGE2_WL] disk LKG load error (non-fatal): {exc}")


# ── Read path (zero I/O) ──────────────────────────────────────────────────────

def get_stage2(sym: str) -> dict:
    """
    Return the cached stage2_breakout dict for *sym*.
    Returns {"score": None, "label": None, "reason": None} when not found.
    Never issues any I/O.
    """
    entry = _STAGE2_LKG.get(sym.upper())
    if entry is None:
        return {"score": None, "label": None, "reason": None}
    return {
        "score":  entry.get("score"),
        "label":  entry.get("label"),
        "reason": entry.get("reason"),
    }


# ── Warmup helpers ────────────────────────────────────────────────────────────

def _is_fresh(sym: str) -> bool:
    """Return True if the in-memory entry was computed within _FRESH_HOURS."""
    entry = _STAGE2_LKG.get(sym.upper())
    if not entry:
        return False
    computed_at_str = entry.get("computed_at") or ""
    if not computed_at_str:
        return False
    try:
        computed_at = datetime.fromisoformat(computed_at_str.replace("Z", "+00:00"))
        age_h = (datetime.now(timezone.utc) - computed_at).total_seconds() / 3600
        return age_h < _FRESH_HOURS
    except Exception:
        return False


async def _fetch_bars(sym: str) -> list[dict]:
    """
    Return daily price bars for *sym* (oldest → newest, each with date+close).

    Probe order:
      1. In-memory cache: fmp_hist:{sym}  (FMP bars, ~4h TTL, set by theme_rs)
      2. In-memory cache: tdier_hist:{sym}:400  (Tradier bars, 1h TTL)
      3. Live FMP /stable/historical-price-eod via theme_rs_service._fetch_fmp_daily_history
         (uses the shared semaphore, FMP key, and writes back to fmp_hist:{sym} cache)
      4. Tradier daily history via theme_rs_service._fetch_tradier_daily_history
         (writes back to tdier_hist:{sym}:400 cache)

    Reuses theme_rs_service providers so rate-limiting and caching are shared
    across the whole application — no independent httpx calls.
    Never raises — returns [] on any failure.
    """
    s = sym.upper()
    try:
        from data.cache import cache as _cache
        cached = _cache.get(f"fmp_hist:{s}") or _cache.get(f"tdier_hist:{s}:{_HIST_DAYS}")
        if cached:
            return cached
    except Exception:
        pass

    # Reuse theme_rs_service fetch functions (FMP semaphore + Tradier fallback).
    # They cache results back to fmp_hist:{sym} / tdier_hist:{sym}:400, so the
    # _get_stage2_breakout() bar probe also benefits after the first warmup.
    try:
        from services.theme_rs_service import (
            _fetch_fmp_daily_history,
            _fetch_tradier_daily_history,
        )
        bars = await _fetch_fmp_daily_history(s)
        if not bars:
            bars = await _fetch_tradier_daily_history(s, days=_HIST_DAYS)
        return bars
    except Exception as exc:
        print(f"[STAGE2_WL] bar fetch error {sym}: {exc}")
        return []


def _persist_lkg() -> None:
    """Write _STAGE2_LKG atomically to disk."""
    try:
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "symbol_count": len(_STAGE2_LKG),
            "results": dict(_STAGE2_LKG),
        }
        tmp = _LKG_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        tmp.replace(_LKG_PATH)
    except Exception as exc:
        print(f"[STAGE2_WL] disk write error (non-fatal): {exc}")


# ── Public warmup ─────────────────────────────────────────────────────────────

async def warmup_stage2(tickers: list[str]) -> dict:
    """
    Fetch daily bars + compute Weinstein stage for every ticker in *tickers*.

    Skips symbols whose cached result is < _FRESH_HOURS old.
    Runs _CONCURRENCY concurrent Tradier calls with a small sleep between batches.

    Returns a summary dict.
    """
    if not tickers:
        return {"status": "skipped", "reason": "no_tickers"}

    deduped = list(dict.fromkeys(s.strip().upper() for s in tickers if s.strip()))
    skip_count = sum(1 for s in deduped if _is_fresh(s))
    to_process = [s for s in deduped if not _is_fresh(s)]

    print(
        f"[STAGE2_WL] warmup starting: {len(deduped)} unique tickers, "
        f"{skip_count} fresh (skip), {len(to_process)} to compute"
    )

    if not to_process:
        return {"status": "all_fresh", "skipped": skip_count, "computed": 0}

    # SPY bars for RS calculation (optional)
    spy_weekly = None
    try:
        from services.stage_analysis import weekly_bars_from_daily
        spy_bars = await _fetch_bars("SPY")
        if spy_bars:
            spy_weekly = weekly_bars_from_daily(spy_bars)
    except Exception as _spy_err:
        print(f"[STAGE2_WL] SPY bar fetch failed (non-fatal): {_spy_err}")

    from services.stage_analysis import weekly_bars_from_daily, analyze_symbol_stage

    sem = asyncio.Semaphore(_CONCURRENCY)
    now_ts = datetime.now(timezone.utc).isoformat()

    computed  = 0
    no_bars   = 0
    too_short = 0
    errors    = 0

    async def _process_one(sym: str) -> None:
        nonlocal computed, no_bars, too_short, errors
        async with sem:
            try:
                await asyncio.sleep(0.3)   # gentle pacing — 4 concurrent × 0.3s ≈ 25 req/s max
                bars = await _fetch_bars(sym)
                if not bars:
                    no_bars += 1
                    # Store explicit null so we don't retry until next warmup
                    _STAGE2_LKG[sym] = {
                        "score": None, "label": None, "reason": None,
                        "computed_at": now_ts,
                    }
                    return

                weekly = weekly_bars_from_daily(bars)
                if len(weekly) < 35:
                    too_short += 1
                    _STAGE2_LKG[sym] = {
                        "score": None, "label": None, "reason": None,
                        "computed_at": now_ts,
                    }
                    return

                result = analyze_symbol_stage(
                    weekly_bars=weekly,
                    spy_weekly_bars=spy_weekly,
                    source="watchlist_stage2_warmup",
                )
                _STAGE2_LKG[sym] = {
                    "score":       result.get("stage_score"),
                    "label":       result.get("stage_label"),
                    "reason":      result.get("stage_reason"),
                    "computed_at": now_ts,
                }
                computed += 1
            except Exception as exc:
                errors += 1
                print(f"[STAGE2_WL] error processing {sym}: {exc}")

    # Fire all tasks and let the semaphore gate concurrency
    tasks = [_process_one(sym) for sym in to_process]
    await asyncio.gather(*tasks, return_exceptions=True)

    _persist_lkg()

    non_null = sum(1 for v in _STAGE2_LKG.values() if v.get("score") is not None)
    summary = {
        "status":     "done",
        "total":      len(deduped),
        "skipped":    skip_count,
        "computed":   computed,
        "no_bars":    no_bars,
        "too_short":  too_short,
        "errors":     errors,
        "non_null_in_lkg": non_null,
    }
    print(f"[STAGE2_WL] warmup done: {summary}")
    return summary


async def warmup_stage2_all_watchlists(startup_delay_s: float = 0.0) -> dict:
    """
    Load all watchlist tickers from Neon and run warmup_stage2 on the union.
    Called by the scheduler and at startup.

    At startup we wait *startup_delay_s* seconds before fetching any bars so
    the main screener loop (which saturates the Tradier rate-limiter at boot)
    has time to settle before we add more API calls.
    """
    if startup_delay_s > 0:
        print(f"[STAGE2_WL] startup delay {startup_delay_s:.0f}s — waiting for other loops to settle")
        await asyncio.sleep(startup_delay_s)

    tickers: list[str] = []
    try:
        from data.pg_storage import watchlist_list, watchlist_read
        wl_metas = watchlist_list()
        for meta in wl_metas:
            wl_id = meta.get("id")
            if not wl_id:
                continue
            try:
                store = watchlist_read(wl_id)
                if store:
                    tickers.extend(store.get("tickers") or [])
            except Exception as _re:
                print(f"[STAGE2_WL] read error wl={wl_id}: {_re}")
    except Exception as exc:
        print(f"[STAGE2_WL] watchlist_list error: {exc}")

    if not tickers:
        return {"status": "skipped", "reason": "no_tickers_in_any_watchlist"}

    return await warmup_stage2(tickers)
