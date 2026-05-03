"""
Screener Returns Service — real 2w/4w/10w historical return calculations.

Fetches FMP stable/historical-price-eod (date-ranged, ~180 cal days) for each
symbol, computes trailing returns at 10/20/50 trading-day lookbacks (≈ 2w/4w/10w),
and caches results in screener_returns_cache (Neon).

Design rules:
- Only writes to DB; never returns fake values.
- If no historical data exists for a symbol, stores NULL for all return fields.
- Request path reads from cache only (get_cached_returns). No live API calls
  at request time — warm jobs populate the cache on a weekly cadence.
- Never raises; always returns partial data on partial failure.
- Uses /stable/historical-price-eod with from/to params (NOT /full) to limit
  bandwidth to ~180 calendar days per symbol.
- All FMP calls flow through the FMP Governor (opt-in, Screener Hub only).
"""
from __future__ import annotations

import asyncio
import os
from datetime import date, timedelta
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

try:
    from services.fmp_governor import fmp_governor
except Exception:
    fmp_governor = None  # type: ignore


FMP_BASE = "https://financialmodelingprep.com/stable"
_FMP_TIMEOUT = 12.0

# Semaphore caps concurrent in-flight FMP calls regardless of governor
_HIST_SEM = asyncio.Semaphore(2)

# How many calendar days of history to fetch (enough for 10w=50 bars + generous buffer)
_HISTORY_DAYS = 180


def _fmp_key() -> str:
    return os.getenv("FMP_API_KEY") or ""


def _date_range() -> tuple[str, str]:
    """Return (from_date, to_date) as ISO strings covering _HISTORY_DAYS calendar days."""
    today = date.today()
    from_dt = today - timedelta(days=_HISTORY_DAYS)
    return from_dt.isoformat(), today.isoformat()


async def _fetch_fmp_bars(symbol: str, *, job_name: str = "returns_warm") -> list[dict]:
    """
    Fetch daily OHLCV bars from FMP stable/historical-price-eod (date-ranged).

    Uses from/to params to fetch only the last _HISTORY_DAYS calendar days —
    avoids the /full endpoint which returns complete multi-year history and
    generates most of FMP bandwidth consumption.

    Returns sorted list of {date, close} newest-last.
    Empty list on any failure or governor budget exceeded.
    """
    key = _fmp_key()
    if not key:
        return []

    # ── Governor check (Screener Hub opt-in only) ─────────────────────────────
    if fmp_governor is not None:
        ok = await fmp_governor.acquire(job_name=job_name)
        if not ok:
            print(f"[RETURNS] FMP governor budget exceeded — skipping {symbol}")
            return []

    sym = symbol.upper()
    from_date, to_date = _date_range()

    async with _HIST_SEM:
        try:
            async with httpx.AsyncClient(timeout=_FMP_TIMEOUT) as client:
                resp = await client.get(
                    f"{FMP_BASE}/historical-price-eod",
                    params={
                        "symbol": sym,
                        "from": from_date,
                        "to": to_date,
                        "apikey": key,
                    },
                )
            if fmp_governor is not None:
                fmp_governor.record_call()

            if resp.status_code not in (200, 201):
                if resp.status_code not in (402, 403, 404):
                    print(f"[RETURNS] FMP {sym} HTTP {resp.status_code} "
                          f"(endpoint_group=historical_returns)")
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
            if fmp_governor is not None:
                fmp_governor.record_call()  # still counts even on exception
            print(f"[RETURNS] FMP bars {sym} error (endpoint_group=historical_returns): {e}")
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
    sleep_between_s: float = 0.0,   # Governor handles pacing; extra sleep is redundant
    max_calls: int = 200,
    job_name: str = "returns_warm",
) -> dict:
    """
    Fetch historical bars + compute trailing returns for each symbol.
    Stores results in screener_returns_cache. Never raises.

    Parameters
    ----------
    symbols          : Iterable of ticker strings (automatically deduped).
    force            : If True, re-fetch even when cache is fresh.
    sleep_between_s  : Legacy param — governor handles spacing; set 0 here.
    max_calls        : Hard cap on FMP API calls this run (also gated by governor).
    job_name         : Label for screener_job_runs tracking + governor job context.

    Returns
    -------
    Summary dict: {status, symbols_count, completed, failed, api_calls_used, endpoint_group}
    """
    ensure_tables()

    from services.screener_hub_service import _dedupe_filter
    deduped = _dedupe_filter(symbols)
    if not deduped:
        return {
            "job_name": job_name, "status": "ok", "symbols_count": 0,
            "completed": 0, "failed": 0, "api_calls_used": 0,
            "endpoint_group": "historical_returns",
        }

    # ── Register job with governor ─────────────────────────────────────────────
    if fmp_governor is not None:
        fmp_governor.start_job(job_name)

    run_id = start_job_run(
        job_name, symbols_count=len(deduped),
        metadata={"force": force, "endpoint_group": "historical_returns",
                  "history_days": _HISTORY_DAYS},
    )

    if not force:
        fresh = returns_fresh_symbols(deduped, max_age_days=7)
        queue = [s for s in deduped if s not in fresh]
        skipped_fresh = len(fresh)
        print(f"[RETURNS] {job_name}: {len(deduped)} total, "
              f"{len(queue)} stale, {skipped_fresh} fresh — "
              f"endpoint_group=historical_returns from={_date_range()[0]}")
    else:
        queue = list(deduped)
        skipped_fresh = 0
        print(f"[RETURNS] {job_name}: force=True, processing {len(queue)} — "
              f"endpoint_group=historical_returns")

    completed = 0
    failed = 0
    api_calls = 0
    budget_limited = False

    try:
        for idx, sym in enumerate(queue):
            if api_calls >= max_calls:
                print(f"[RETURNS] {job_name}: max_calls={max_calls} reached, stopping")
                break

            try:
                bars = await _fetch_fmp_bars(sym, job_name=job_name)

                # Governor returned empty due to budget → treat as soft stop
                if not bars and fmp_governor is not None and fmp_governor._job_budget_hit:
                    budget_limited = True
                    print(f"[RETURNS] {job_name}: governor budget hit after {api_calls} calls")
                    break

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
                    print(f"[RETURNS] {sym}: bars={len(bars)} "
                          f"2w={rets['return_2w']} 4w={rets['return_4w']} "
                          f"10w={rets['return_10w']} accel={rets['rs_accel']}")
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                print(f"[RETURNS] {sym} error: {e}")

            # Legacy sleep param (ignored when governor is active)
            if sleep_between_s > 0 and fmp_governor is None and idx < len(queue) - 1:
                await asyncio.sleep(sleep_between_s)

        status = (
            "partial_budget_limit" if budget_limited
            else ("ok" if failed == 0 else ("partial" if completed > 0 else "failed"))
        )
        finish_job_run(
            run_id, status=status,
            symbols_completed=completed, symbols_failed=failed,
            api_calls_used=api_calls,
        )
        if fmp_governor is not None:
            fmp_governor.finish_job(job_name, budget_limited=budget_limited)

        return {
            "job_name": job_name, "status": status,
            "symbols_count": len(deduped),
            "completed": completed, "failed": failed,
            "api_calls_used": api_calls,
            "skipped_fresh": skipped_fresh,
            "budget_limited": budget_limited,
            "endpoint_group": "historical_returns",
            "history_days": _HISTORY_DAYS,
        }
    except Exception as e:
        finish_job_run(
            run_id, status="failed",
            symbols_completed=completed, symbols_failed=failed,
            api_calls_used=api_calls, error=str(e),
        )
        if fmp_governor is not None:
            fmp_governor.finish_job(job_name, budget_limited=False)
        return {
            "job_name": job_name, "status": "failed",
            "symbols_count": len(deduped),
            "completed": completed, "failed": failed,
            "api_calls_used": api_calls, "error": str(e),
            "endpoint_group": "historical_returns",
        }


def get_cached_returns(symbols: Iterable[str]) -> dict[str, dict]:
    """
    Read cached returns from screener_returns_cache.
    Returns {symbol: {return_2w, return_4w, return_10w, rs_accel, bars_count, fetched_at}}.
    Empty dict on failure or cache miss.
    """
    ensure_tables()
    return get_returns(symbols)
