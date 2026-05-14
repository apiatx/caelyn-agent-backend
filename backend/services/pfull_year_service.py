"""
pfull_year_service.py — Incremental cache management for
GET /api/catalysts/earnings/portfolio-full-year.

Design
------
- 90-day TTL: the full 52-week FMP fan-out (~52 HTTP calls) is expensive.
  Once built, the result lives in cache for 90 days.
- Incremental on add: when a new ticker enters the portfolio, only *that*
  ticker's upcoming earnings are fetched (52-week fan-out for 1 symbol) and
  merged into the existing cached list — the 18 existing rows are untouched.
- Incremental on remove: dropped tickers are filtered out of the cached list
  immediately — zero FMP calls needed.
- Fixed cache key: because we patch in place, the key is static (not
  symbol-set-hash-based). Any save_holdings call triggers a diff + patch.
- Cold-cache guard: if the cache is empty when a mutation arrives, nothing
  is patched — the next GET /portfolio-full-year builds it from scratch.
"""
from __future__ import annotations

import asyncio
from datetime import date, timedelta


PFULL_CACHE_KEY = "pfull:v1:main"
PFULL_TTL       = 7_776_000   # 90 days in seconds


def _safe_f(v) -> float | None:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _row_to_entry(row: dict, sym_override: str | None = None) -> dict:
    sym = sym_override or (row.get("symbol") or "").upper()
    return {
        "symbol":           sym,
        "date":             row.get("date"),
        "eps_estimate":     _safe_f(row.get("epsEstimated")),
        "revenue_estimate": _safe_f(row.get("revenueEstimated")),
        "company_name":     row.get("name") or row.get("companyName") or None,
    }


async def _fetch_ticker_entry(fmp, sym: str, today_s: str) -> dict | None:
    """52-week fan-out for a single symbol; returns the earliest upcoming row."""
    weeks: list[tuple[str, str]] = []
    cur = date.today()
    for _ in range(52):
        weeks.append((cur.isoformat(), (cur + timedelta(days=6)).isoformat()))
        cur += timedelta(days=7)

    all_rows: list[dict] = []
    for batch_start in range(0, 52, 13):
        batch   = weeks[batch_start:batch_start + 13]
        results = await asyncio.gather(
            *[fmp.earnings_calendar(f, t) for f, t in batch],
            return_exceptions=True,
        )
        for res in results:
            if isinstance(res, list):
                all_rows.extend(res)

    candidates = [
        r for r in all_rows
        if (r.get("symbol") or "").upper() == sym
        and (r.get("date") or "") >= today_s
    ]
    if not candidates:
        return None
    earliest = min(candidates, key=lambda r: r.get("date") or "")
    return _row_to_entry(earliest, sym)


async def patch_pfull_cache(new_syms: set[str], fmp_key: str) -> None:
    """
    Diff new_syms against the currently cached earnings list and apply
    incremental adds/removes.

    Called as a background task from POST /api/portfolio/holdings and
    POST /api/portfolio/sync after every successful save.

    - Removed tickers: dropped from the list instantly.
    - Added tickers: 52-week FMP fan-out per new ticker (parallelised), merged.
    - TTL reset to 90 days on every successful patch.
    - If cache is cold, skips — next GET /portfolio-full-year rebuilds from scratch.
    - Never raises: all errors are caught and logged so the caller is unaffected.
    """
    try:
        from data.cache import cache  # type: ignore

        cached: list[dict] | None = cache.get(PFULL_CACHE_KEY)
        if cached is None:
            print("[pfull_patch] cache cold — skipping patch, next GET will rebuild")
            return

        cached_syms  = {e["symbol"] for e in cached if e.get("symbol")}
        removed_syms = cached_syms - new_syms
        added_syms   = new_syms   - cached_syms

        print(
            f"[pfull_patch] cached={len(cached_syms)}  new={len(new_syms)}  "
            f"removed={removed_syms or 'none'}  added={added_syms or 'none'}"
        )

        if not removed_syms and not added_syms:
            print("[pfull_patch] no symbol changes — cache unchanged")
            return

        # 1. Drop removed tickers
        if removed_syms:
            cached = [e for e in cached if e.get("symbol") not in removed_syms]
            print(f"[pfull_patch] removed {removed_syms} from cache")

        # 2. Fetch earnings for newly added tickers
        if added_syms and fmp_key:
            from services.catalyst_calendar_service import CatalystFMP  # type: ignore
            fmp     = CatalystFMP(fmp_key)
            today_s = date.today().isoformat()

            new_entries = await asyncio.gather(
                *[_fetch_ticker_entry(fmp, sym, today_s) for sym in sorted(added_syms)],
                return_exceptions=True,
            )
            for sym, entry in zip(sorted(added_syms), new_entries):
                if isinstance(entry, dict) and entry.get("date"):
                    cached.append(entry)
                    print(f"[pfull_patch] added {sym} → {entry['date']}")
                else:
                    print(f"[pfull_patch] {sym}: no upcoming earnings found in 52-week window")

        # 3. Re-sort and persist
        cached.sort(key=lambda e: e.get("date") or "")
        cache.set(PFULL_CACHE_KEY, cached, PFULL_TTL)
        print(
            f"[pfull_patch] done: earnings_count={len(cached)}  "
            f"ttl={PFULL_TTL}s (90 days)"
        )

    except Exception as _e:
        print(f"[pfull_patch] patch failed (non-fatal): {_e}")
