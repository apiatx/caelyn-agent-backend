"""
canonical_history_backfill.py — 5-year price history backfill job.
====================================================================
V4.2.5.2 — Admin-triggered only.  NOT called at server startup.
            Startup reads the disk cache (canonical_history_service).

Provider order
--------------
  1. FMP stable/historical-price-eod — direct call (bypasses FMP_BLOCK_FULL_HISTORICAL
     guard for admin backfills; still budget-checked via api_budget)
  2. Tradier /markets/history with extended window (may return < full 5Y)

Budget
------
  Checks api_budget before every FMP call.
  Each FMP history fetch = 1 FMP call.
  If budget hard-stops, remaining symbols fall to Tradier.

Eligible symbols
----------------
  - watchlist_stage2_lkg.json universe
  - symbols without ":" (no foreign/OTC prefixes)
  - max 8-character tickers

Priority ordering (per run)
---------------------------
  T0  not_yet_backfilled, fetch_failed
  T1  recent_only, intermediate_only
  T2  partial_history, available_3y
  T3  available_5y (still refreshed when stale)
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from typing import Optional

_LONG_HIST_DAYS     = 1900   # FMP: 5.2Y calendar days
_TRADIER_LONG_DAYS  = 1825   # Tradier extended attempt (may be truncated at ~400)
_DEFAULT_MAX_SYMS   = 80
_DEFAULT_DELAY_S    = 0.25   # between provider calls

_STATE: dict = {
    "running":    False,
    "started_at": None,
    "completed":  0,
    "skipped":    0,
    "failed":     0,
    "upgraded":   0,
    "last_symbol": None,
    "error":      None,
}


def get_backfill_status() -> dict:
    return dict(_STATE)


def _is_eligible(symbol: str) -> bool:
    return ":" not in symbol and 1 <= len(symbol) <= 8


# ── FMP direct fetch (bypasses FMP_BLOCK guard for admin backfill) ────────────

async def _fetch_fmp_direct(symbol: str) -> list[dict]:
    """
    Direct FMP historical EOD fetch.  Does NOT check FMP_BLOCK_FULL_HISTORICAL
    (admin backfill is an explicit operator action).
    Does spend one FMP budget unit — hard-stops if budget exhausted.
    """
    try:
        from api_budget import budget
        if not budget.spend("fmp", 1):
            print(f"[CANON_BACKFILL] FMP budget hard-stop, skipping {symbol}")
            return []
    except Exception:
        pass   # api_budget unavailable — proceed but log

    try:
        from services.theme_rs_service import _fmp_key
        key = _fmp_key()
        if not key:
            print(f"[CANON_BACKFILL] no FMP key for {symbol}")
            return []

        import httpx
        from_date = (date.today() - timedelta(days=_LONG_HIST_DAYS)).isoformat()
        to_date   = date.today().isoformat()

        async with httpx.AsyncClient(timeout=18.0) as client:
            resp = await client.get(
                "https://financialmodelingprep.com/stable/historical-price-eod",
                params={"symbol": symbol.upper(), "from": from_date,
                        "to": to_date, "apikey": key},
            )
        if resp.status_code not in (200, 201):
            if resp.status_code in (404, 403, 402):
                return []     # ticker not on FMP
            print(f"[CANON_BACKFILL] FMP HTTP {resp.status_code} for {symbol}")
            return []

        raw      = resp.json()
        bars_raw = raw if isinstance(raw, list) else (raw.get("historical") or [])
        bars: list[dict] = []
        for b in bars_raw:
            if not isinstance(b, dict):
                continue
            d = b.get("date") or b.get("formattedDate") or ""
            c = b.get("close") or b.get("adjClose")
            if not d or c is None:
                continue
            try:
                bar: dict = {"date": str(d)[:10], "close": float(c)}
                for fld, keys in (
                    ("open",   ("open",)),
                    ("high",   ("high",)),
                    ("low",    ("low",)),
                    ("volume", ("volume",)),
                ):
                    for k in keys:
                        v = b.get(k)
                        if v is not None:
                            try:
                                bar[fld] = float(v)
                            except (TypeError, ValueError):
                                pass
                            break
                bars.append(bar)
            except (TypeError, ValueError):
                pass

        bars.sort(key=lambda x: x["date"])
        if bars:
            print(f"[CANON_BACKFILL] FMP {symbol}: {len(bars)} bars ✓")
        return bars

    except Exception as exc:
        print(f"[CANON_BACKFILL] FMP exception {symbol}: {exc}")
        return []


# ── Actual-ticker-history-limit detection ────────────────────────────────────

def _detect_actual_limit(bars: list[dict]) -> bool:
    """
    Return True when the ticker genuinely has < 2 years of public trading
    history (new IPO / recent listing) rather than a data-source failure.
    """
    if len(bars) >= 504:
        return False     # 2+ years of data available — not a new ticker
    if not bars:
        return False
    oldest = bars[0].get("date", "")
    if not oldest:
        return False
    try:
        oldest_d = datetime.strptime(oldest[:10], "%Y-%m-%d").date()
        return (date.today() - oldest_d).days < 730   # < ~2 years
    except Exception:
        return False


# ── Single-symbol backfill ────────────────────────────────────────────────────

async def backfill_symbol(
    symbol:                str,
    allow_tradier_fallback: bool = True,
) -> dict:
    """
    Fetch and cache canonical 5-year history for one symbol.
    Returns a result dict describing the outcome.
    """
    from services.canonical_history_service import (
        save_bars, mark_failed, mark_excluded, get_metadata, is_fresh,
    )
    sym = symbol.upper()

    if not _is_eligible(sym):
        mark_excluded(sym)
        return {"symbol": sym, "action": "excluded", "bar_count": 0}

    # Skip symbols already at available_5y and still fresh
    if is_fresh(sym, max_age_h=20.0):
        meta = get_metadata(sym)
        if meta and meta.get("history_status") == "available_5y":
            return {"symbol": sym, "action": "skipped_fresh",
                    "bar_count": meta.get("bar_count", 0)}

    # ── Provider: FMP ─────────────────────────────────────────────────────────
    bars     = await _fetch_fmp_direct(sym)
    provider = "fmp"

    # ── Provider fallback: Tradier extended window ────────────────────────────
    if not bars and allow_tradier_fallback:
        try:
            from services.theme_rs_service import _fetch_tradier_daily_history
            bars     = await _fetch_tradier_daily_history(sym, days=_TRADIER_LONG_DAYS)
            provider = "tradier"
            if bars:
                print(f"[CANON_BACKFILL] Tradier fallback {sym}: {len(bars)} bars")
        except Exception as exc:
            print(f"[CANON_BACKFILL] Tradier exception {sym}: {exc}")

    if not bars:
        mark_failed(sym, "no_bars_from_providers", provider)
        return {"symbol": sym, "action": "fetch_failed", "bar_count": 0}

    is_actual_limit = _detect_actual_limit(bars)
    meta = save_bars(sym, bars, provider, is_actual_limit=is_actual_limit)

    # Warm in-memory FMP cache so immediate warmup_stage2 benefits
    if provider == "fmp":
        try:
            from data.cache import cache
            cache.set(f"fmp_hist:{sym}", bars, 14400)   # 4h
        except Exception:
            pass

    return {
        "symbol":          sym,
        "action":          "saved",
        "bar_count":       meta["bar_count"],
        "history_status":  meta["history_status"],
        "provider":        meta["provider"],
        "years_available": meta["years_available"],
        "is_actual_limit": is_actual_limit,
    }


# ── Priority queue builder ────────────────────────────────────────────────────

async def _build_symbol_list() -> list[str]:
    """
    Build eligible symbol universe from stage2 LKG + watchlist store.
    Falls back gracefully if any source is unavailable.
    """
    syms: set[str] = set()
    try:
        import json
        from pathlib import Path
        lkg_path = Path(__file__).parent.parent / "data" / "watchlist_stage2_lkg.json"
        if lkg_path.exists():
            data = json.loads(lkg_path.read_text())
            for s in data.get("results", {}):
                if _is_eligible(s):
                    syms.add(s.upper())
    except Exception as exc:
        print(f"[CANON_BACKFILL] stage2 LKG read error: {exc}")

    try:
        from data.pg_storage import watchlist_list, watchlist_read
        lists = await watchlist_list()
        for wl in lists:
            try:
                wdata = await watchlist_read(wl.get("id") or wl.get("name") or "")
                for t in (wdata.get("tickers") or []):
                    s = (t if isinstance(t, str) else t.get("symbol") or "").upper()
                    if _is_eligible(s):
                        syms.add(s)
            except Exception:
                pass
    except Exception:
        pass

    return sorted(syms)


def _priority_key(symbol: str) -> int:
    """Lower key = higher priority."""
    from services.canonical_history_service import get_metadata
    meta = get_metadata(symbol)
    if not meta:
        return 0   # not yet attempted → highest priority
    return {
        "not_yet_backfilled":           0,
        "fetch_failed":                 1,
        "recent_only":                  2,
        "intermediate_only":            3,
        "partial_history":              4,
        "available_3y":                 5,
        "actual_ticker_history_limit":  6,
        "available_5y":                 9,
        "excluded_prefixed_symbol":    99,
    }.get(meta.get("history_status", "not_yet_backfilled"), 5)


# ── Batch runner ──────────────────────────────────────────────────────────────

async def run_backfill_batch(
    symbols:                Optional[list[str]] = None,
    max_symbols:            int   = _DEFAULT_MAX_SYMS,
    delay_s:                float = _DEFAULT_DELAY_S,
    allow_tradier_fallback: bool  = True,
    priority_only:          bool  = False,
) -> dict:
    """
    Run one backfill batch.  Returns a summary dict.

    symbols=None → auto-build from watchlist + stage2 LKG.
    symbols=[...] → use explicit list (admin override).
    priority_only=True → skip available_5y symbols (only improve lower tiers).
    """
    global _STATE

    if _STATE["running"]:
        return {"error": "backfill_already_running", **_STATE}

    _STATE.update({
        "running":    True,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "completed":  0,
        "skipped":    0,
        "failed":     0,
        "upgraded":   0,
        "last_symbol": None,
        "error":      None,
    })

    try:
        if symbols is None:
            symbols = await _build_symbol_list()

        # Filter and sort by priority
        symbols = [s.upper() for s in symbols if _is_eligible(s)]
        symbols.sort(key=_priority_key)

        if priority_only:
            symbols = [s for s in symbols if _priority_key(s) <= 4]

        symbols = symbols[:max_symbols]

        results: list[dict] = []
        for sym in symbols:
            _STATE["last_symbol"] = sym
            try:
                r = await backfill_symbol(sym, allow_tradier_fallback=allow_tradier_fallback)
                results.append(r)

                action = r.get("action", "")
                if action == "skipped_fresh":
                    _STATE["skipped"] += 1
                elif action in ("fetch_failed", "excluded"):
                    _STATE["failed"] += 1
                else:
                    _STATE["completed"] += 1
                    if r.get("bar_count", 0) >= 700:
                        _STATE["upgraded"] += 1
            except Exception as exc:
                print(f"[CANON_BACKFILL] error {sym}: {exc}")
                _STATE["failed"] += 1

            if delay_s > 0:
                await asyncio.sleep(delay_s)

        return {
            "batch_size":       len(symbols),
            "completed":        _STATE["completed"],
            "skipped_fresh":    _STATE["skipped"],
            "failed":           _STATE["failed"],
            "upgraded_to_5y":   _STATE["upgraded"],
            "results":          results,
        }

    except Exception as exc:
        _STATE["error"] = str(exc)
        raise
    finally:
        _STATE["running"] = False
