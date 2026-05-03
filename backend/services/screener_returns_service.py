"""
Screener Returns Service — real 2w/4w/10w historical return calculations.

Fetches FMP stable/historical-price-eod/full for each symbol, computes
trailing returns at 10/20/50 trading-day lookbacks (≈ 2w/4w/10w), and
caches results in screener_returns_cache (Neon).

Design rules:
- Only writes to DB; never returns fake values.
- If no historical data exists for a symbol, stores NULL for all return fields.
- Request path reads from cache only (get_cached_returns). No live API calls
  at request time — warm jobs populate the cache on a weekly cadence.
- Never raises; always returns partial data on partial failure.
"""
from __future__ import annotations

import asyncio
import os
from typing import Iterable, Optional

import httpx

try:
    from data.screener_hub_store import (
        ensure_tables,
        upsert_returns,
        get_returns,
        returns_fresh_symbols,
        start_job_run,
        finish_job_run,
    )
except Exception:
    ensure_tables = lambda: None  # type: ignore
    upsert_returns = lambda *a, **kw: False  # type: ignore
    get_returns = lambda s: {}  # type: ignore
    returns_fresh_symbols = lambda s, **kw: set()  # type: ignore
    start_job_run = lambda *a, **kw: None  # type: ignore
    finish_job_run = lambda *a, **kw: None  # type: ignore


FMP_BASE = "https://financialmodelingprep.com/stable"
_FMP_TIMEOUT = 12.0
_HIST_SEM = asyncio.Semaphore(3)


def _fmp_key() -> str:
    return os.getenv("FMP_API_KEY") or ""


async def _fetch_fmp_bars(symbol: str) -> list[dict]:
    """
    Fetch daily OHLCV bars from FMP stable/historical-price-eod/full.
    Returns sorted list of {date, close} newest-last.
    Empty list on any failure.
    """
    key = _fmp_key()
    if not key:
        return []
    sym = symbol.upper()
    async with _HIST_SEM:
        try:
            async with httpx.AsyncClient(timeout=_FMP_TIMEOUT) as client:
                resp = await client.get(
                    f"{FMP_BASE}/historical-price-eod/full",
                    params={"symbol": sym, "apikey": key},
                )
            if resp.status_code not in (200, 201):
                if resp.status_code not in (402, 403, 404):
                    print(f"[RETURNS] FMP {sym} HTTP {resp.status_code}")
                return []
            raw = resp.json()
            bars_raw = raw if isinstance(raw, list) else (raw.get("historical") or [])
            bars: list[dict] = []
            for b in bars_raw:
                if not isinstance(b, dict):
                    continue
                d = b.get("date") or b.get("formattedDate") or ""
                c = b.get("adjClose") or b.get("close")
                if d and c is not None:
                    try:
                        bars.append({"date": str(d)[:10], "close": float(c)})
                    except (TypeError, ValueError):
                        pass
            bars.sort(key=lambda r: r["date"])
            return bars
        except Exception as e:
            print(f"[RETURNS] FMP bars {sym}: {e}")
            return []


def _compute_returns(bars: list[dict]) -> dict:
    """
    Given sorted (oldest-first) {date, close} bars, compute trailing returns.

    Lookbacks (trading days):
      2w  = 10 bars
      4w  = 20 bars
      10w = 50 bars

    rs_accel = return_2w - return_4w  (positive = accelerating short-term)

    Returns None for any window where we lack sufficient bars.
    """
    if not bars:
        return {"return_2w": None, "return_4w": None, "return_10w": None, "rs_accel": None}

    n = len(bars)
    cur = bars[-1]["close"]

    def pct(lookback: int) -> Optional[float]:
        if n <= lookback:
            return None
        old = bars[-(lookback + 1)]["close"]
        if not old:
            return None
        return round((cur - old) / old * 100, 4)

    r2  = pct(10)
    r4  = pct(20)
    r10 = pct(50)
    accel = round(r2 - r4, 4) if r2 is not None and r4 is not None else None

    return {"return_2w": r2, "return_4w": r4, "return_10w": r10, "rs_accel": accel}


async def fetch_and_cache_returns(
    symbols: Iterable[str],
    *,
    force: bool = False,
    sleep_between_s: float = 2.0,
    max_calls: int = 200,
    job_name: str = "returns_warm",
) -> dict:
    """
    Fetch historical bars + compute trailing returns for each symbol.
    Stores results in screener_returns_cache. Never raises.

    Parameters
    ----------
    symbols          : Iterable of ticker strings.
    force            : If True, re-fetch even when cache is fresh.
    sleep_between_s  : Polite delay between FMP calls.
    max_calls        : Hard cap on FMP API calls this run.
    job_name         : Label for screener_job_runs tracking.

    Returns
    -------
    Summary dict: {status, symbols_count, completed, failed, api_calls_used}
    """
    ensure_tables()

    from services.screener_hub_service import _dedupe_filter
    deduped = _dedupe_filter(symbols)
    if not deduped:
        return {"job_name": job_name, "status": "ok", "symbols_count": 0,
                "completed": 0, "failed": 0, "api_calls_used": 0}

    run_id = start_job_run(job_name, symbols_count=len(deduped),
                           metadata={"force": force})

    if not force:
        fresh = returns_fresh_symbols(deduped, max_age_days=7)
        queue = [s for s in deduped if s not in fresh]
        print(f"[RETURNS] {job_name}: {len(deduped)} total, {len(queue)} stale, {len(fresh)} fresh")
    else:
        queue = list(deduped)
        print(f"[RETURNS] {job_name}: force=True, processing {len(queue)}")

    completed = 0
    failed = 0
    api_calls = 0

    try:
        for idx, sym in enumerate(queue):
            if api_calls >= max_calls:
                print(f"[RETURNS] {job_name}: max_calls={max_calls} reached")
                break
            try:
                bars = await _fetch_fmp_bars(sym)
                api_calls += 1
                rets = _compute_returns(bars)
                ok = upsert_returns(
                    sym,
                    return_2w=rets["return_2w"],
                    return_4w=rets["return_4w"],
                    return_10w=rets["return_10w"],
                    rs_accel=rets["rs_accel"],
                    bars_count=len(bars),
                    ttl_days=7,
                )
                if ok:
                    completed += 1
                    print(f"[RETURNS] {sym}: 2w={rets['return_2w']} 4w={rets['return_4w']} "
                          f"10w={rets['return_10w']} accel={rets['rs_accel']}")
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                print(f"[RETURNS] {sym} error: {e}")

            if idx < len(queue) - 1 and sleep_between_s > 0:
                await asyncio.sleep(sleep_between_s)

        status = "ok" if failed == 0 else ("partial" if completed > 0 else "failed")
        finish_job_run(run_id, status=status,
                       symbols_completed=completed, symbols_failed=failed,
                       api_calls_used=api_calls)
        return {
            "job_name": job_name, "status": status,
            "symbols_count": len(deduped),
            "completed": completed, "failed": failed,
            "api_calls_used": api_calls,
        }
    except Exception as e:
        finish_job_run(run_id, status="failed",
                       symbols_completed=completed, symbols_failed=failed,
                       api_calls_used=api_calls, error=str(e))
        return {
            "job_name": job_name, "status": "failed",
            "symbols_count": len(deduped),
            "completed": completed, "failed": failed,
            "api_calls_used": api_calls, "error": str(e),
        }


def get_cached_returns(symbols: Iterable[str]) -> dict[str, dict]:
    """
    Read cached returns from screener_returns_cache.
    Returns {symbol: {return_2w, return_4w, return_10w, rs_accel, bars_count, fetched_at}}.
    Empty dict on failure or cache miss.
    """
    ensure_tables()
    return get_returns(symbols)
