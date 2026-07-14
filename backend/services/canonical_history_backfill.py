"""
canonical_history_backfill.py — 5-year price history backfill job.
===================================================================
V4.2.5.3 — Scheduler-safe: all Tradier calls route through TradierProvider._get()
           which enforces TRADIER_LIMITER + the canonical_history_backfill lane budget.
           No raw httpx calls to Tradier anywhere in this module.

Provider order
--------------
  1. Tradier /markets/history  (PRIMARY — scheduler-safe, canonical_history_backfill lane)
  2. FMP stable/historical-price-eod  (FALLBACK — admin only, budget-checked)

Session-aware throttling
------------------------
  Active session (pre/regular/post-market weekday):
    - canonical_history_backfill lane budget: 5 RPM (configurable via TRADIER_CANON_HIST_RPM_BUDGET)
    - Additional soft gate: if options_flow lane is ≥ PAUSE_IF_OPTIONS_BUSY_PCT (80%) saturated,
      skip the current call cycle and add extra sleep
    - Off-hours RPM cap NOT applied (market gate already keeps calls very low)

  Off-hours / weekend:
    - Lane budget: still 5 RPM (hard cap from tradier_budget)
    - Off-hours sleep delay shortened to allow up to OFFHOURS_MAX_RPM (15) effective RPM
    - Options-flow gate NOT applied (options loops at reduced maintenance cadence)

Refresh modes
-------------
  initial_full_backfill       first-ever 5Y fetch (5-year start/end window)
  incremental_daily_append    fetch only bars since newest_bar_date + merge
  manual_rebuild              force full re-fetch regardless of existing cache
  cache_read_only             diagnostics only, no provider calls
  weekly_health_check         periodic re-verification (treated same as initial)
  monthly_full_refresh        full re-fetch on monthly schedule

Backfill priority tiers
-----------------------
  Tier 0  watchlist symbols with < 756 bars (Stage/Fib using 400-bar fallback),
           symbols flagged not_yet_backfilled or fetch_failed
  Tier 1  portfolio underlyings, symbols with partial_history / available_3y
  Tier 2  remaining eligible watchlist / theme universe (available_5y stale refresh)

Excluded
--------
  - Tickers with ":"
  - > 8 characters
  - confirmed actual_ticker_history_limit (already satisfied)
  - excluded_prefixed_symbol

Tradier historical capability classification
--------------------------------------------
  TRADIER_FULL_HISTORY_OK       bars >= 1100 (≥ 4.4 years)
  TRADIER_PARTIAL_HISTORY       40 <= bars < 1100
  TRADIER_SYMBOL_TOO_NEW        is_actual_limit=True
  TRADIER_PROVIDER_FAILED       bars == 0 after attempts
  TRADIER_UNVERIFIED            not yet tested
"""
from __future__ import annotations

import asyncio
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional

_LONG_HIST_DAYS     = 1825   # Tradier extended 5-year window
_FMP_LONG_HIST_DAYS = 1900   # FMP 5.2-year window
_DEFAULT_MAX_SYMS   = 80
_DEFAULT_DELAY_S    = 0.5    # base inter-symbol delay (seconds) — lengthened for safety

# Off-hours: target at most this many RPM through the scheduler lane.
# Achieved via sleep math, not by raising the lane budget.
_OFFHOURS_MAX_RPM: int = int(os.environ.get("TRADIER_CANON_HIST_OFFHOURS_MAX_RPM", 15))
_OFFHOURS_DELAY_S: float = max(0.1, 60.0 / max(1, _OFFHOURS_MAX_RPM))

# If options_flow lane usage exceeds this fraction of its budget, skip the current
# canonical history call to protect options flow.
_PAUSE_IF_OPTIONS_BUSY_PCT: float = float(
    os.environ.get("CANON_HIST_OPTIONS_BUSY_PAUSE_PCT", 0.80)
)

_STATE: dict = {
    "running":                   False,
    "started_at":                None,
    "completed":                 0,
    "skipped":                   0,
    "failed":                    0,
    "upgraded":                  0,
    "last_symbol":               None,
    "last_success_at":           None,
    "last_error":                None,
    "error":                     None,
    "current_mode":              None,
    "paused_reason":             None,
    "history_lane_calls_used":   0,
    "tradier_calls_used":        0,
    "fmp_calls_used":            0,
    "request_time_calls":        0,   # must always be 0
}


def get_backfill_status() -> dict:
    """Return full diagnostics snapshot including scheduler lane and options-flow context."""
    snap = dict(_STATE)
    try:
        from data.tradier_budget import diagnostics as _bgt_diag, BUDGETS, _timestamps, WINDOW_S
        import time as _t
        cutoff = _t.monotonic() - WINDOW_S
        hist_used = sum(1 for ts in _timestamps["canonical_history_backfill"] if ts > cutoff)
        opts_used = sum(1 for ts in _timestamps["options_flow"] if ts > cutoff)
        quot_used = sum(1 for ts in _timestamps["quotes"] if ts > cutoff)
        opts_budget = BUDGETS["options_flow"]
        snap["history_lane_rpm_config"]    = BUDGETS["canonical_history_backfill"]
        snap["history_lane_current_usage"] = hist_used
        snap["history_lane_backlog_count"] = _STATE.get("_backlog_count", 0)
        snap["history_lane_last_symbol"]   = _STATE["last_symbol"]
        snap["history_lane_last_success_at"] = _STATE["last_success_at"]
        snap["history_lane_last_error"]    = _STATE["last_error"]
        snap["options_flow_queue_depth"]   = opts_used
        snap["options_flow_busy_pct"]      = round(opts_used / max(1, opts_budget), 2)
        snap["quotes_queue_depth"]         = quot_used
    except Exception:
        pass
    try:
        from data.tradier_provider import TRADIER_LIMITER
        st = TRADIER_LIMITER.status()
        snap["tradier_global_available_tokens"] = st.get("headroom", 0)
        snap["tradier_global_calls_last_60s"]   = st.get("calls_last_60s", 0)
    except Exception:
        pass
    return snap


def _is_eligible(symbol: str) -> bool:
    return ":" not in symbol and 1 <= len(symbol) <= 8


# ── Tradier provider singleton (scheduler-safe) ───────────────────────────────

_TRADIER_PROVIDER = None


def _get_tradier_provider():
    global _TRADIER_PROVIDER
    if _TRADIER_PROVIDER is None:
        try:
            from data.tradier_provider import TradierProvider
            key = os.environ.get("TRADIER_API_KEY", "")
            if key:
                _TRADIER_PROVIDER = TradierProvider(api_key=key)
        except Exception as exc:
            print(f"[CANON_BACKFILL] TradierProvider init error: {exc}")
    return _TRADIER_PROVIDER


async def _fetch_tradier_history_managed(
    symbol: str,
    start_date: str,
    end_date: str,
) -> list[dict]:
    """
    Scheduler-safe Tradier /markets/history fetch.

    Routes through TradierProvider._get() which enforces:
      - TRADIER_LIMITER (global 110 RPM sliding window)
      - canonical_history_backfill lane budget (5 RPM default)

    No raw httpx calls. Returns None from _get() if lane is over-budget
    (deferral) — caller treats that as an empty response.
    """
    provider = _get_tradier_provider()
    if not provider:
        return []

    try:
        from data.tradier_budget import lane as _lane
        with _lane("canonical_history_backfill"):
            raw_bars = await provider.get_history(
                symbol=symbol.upper(),
                interval="daily",
                start=start_date,
                end=end_date,
            )
        _STATE["history_lane_calls_used"] = _STATE.get("history_lane_calls_used", 0) + 1
        _STATE["tradier_calls_used"]      = _STATE.get("tradier_calls_used", 0) + 1
    except Exception as exc:
        print(f"[CANON_BACKFILL][TRADIER_SCHED] {symbol}: {exc}")
        return []

    if not raw_bars:
        return []

    result: list[dict] = []
    for b in raw_bars:
        d = str(b.get("date") or "")[:10]
        c = b.get("close")
        if not d or c is None:
            continue
        try:
            bar: dict = {"date": d, "close": float(c)}
            for fld in ("open", "high", "low", "volume"):
                v = b.get(fld)
                if v is not None:
                    try:
                        bar[fld] = float(v)
                    except (TypeError, ValueError):
                        pass
            result.append(bar)
        except (TypeError, ValueError):
            pass
    result.sort(key=lambda x: x["date"])
    return result


# ── Session-aware throttling helpers ─────────────────────────────────────────

def _get_session() -> str:
    try:
        from data.tradier_market_session import get_session
        return get_session()
    except Exception:
        return "unknown"


def _is_active_session() -> bool:
    try:
        from data.tradier_market_session import is_active_session
        return is_active_session()
    except Exception:
        return True  # conservative: treat unknown as active


def _options_flow_busy() -> bool:
    """True when options_flow lane is ≥ PAUSE_IF_OPTIONS_BUSY_PCT saturated."""
    try:
        import time as _t
        from data.tradier_budget import _timestamps, BUDGETS, WINDOW_S as _W
        cutoff  = _t.monotonic() - _W
        used    = sum(1 for ts in _timestamps["options_flow"] if ts > cutoff)
        budget  = BUDGETS["options_flow"]
        return used >= budget * _PAUSE_IF_OPTIONS_BUSY_PCT
    except Exception:
        return False


def _effective_delay_s(session: str, base_delay: float) -> float:
    """
    Return the inter-symbol sleep duration based on the current session.

    Active session  → use base_delay (conservative; lane cap at 5 RPM does the rest)
    Off-hours       → shorten to _OFFHOURS_DELAY_S to approach _OFFHOURS_MAX_RPM
    Weekend         → same as off-hours
    """
    if session in ("off_hours", "weekend"):
        return _OFFHOURS_DELAY_S
    return base_delay


def _pause_for_busy_options(extra_sleep: float = 3.0) -> str:
    """Sleep briefly when options_flow is saturated. Returns reason string."""
    reason = "options_flow_busy_gate"
    _STATE["paused_reason"] = reason
    return reason


# ── FMP direct fetch (fallback only, budget-checked) ─────────────────────────

async def _fetch_fmp_direct(symbol: str) -> list[dict]:
    """
    FMP historical EOD — fallback when Tradier returns insufficient bars.
    Admin-only: bypasses FMP_BLOCK_FULL_HISTORICAL guard.
    Spends one FMP budget unit.
    """
    try:
        from api_budget import budget
        if not budget.spend("fmp", 1):
            print(f"[CANON_BACKFILL] FMP budget hard-stop, skipping {symbol}")
            return []
    except Exception:
        pass

    try:
        from services.theme_rs_service import _fmp_key
        key = _fmp_key()
        if not key:
            return []

        import httpx
        from_date = (date.today() - timedelta(days=_FMP_LONG_HIST_DAYS)).isoformat()
        to_date   = date.today().isoformat()

        async with httpx.AsyncClient(timeout=18.0) as client:
            resp = await client.get(
                "https://financialmodelingprep.com/stable/historical-price-eod",
                params={"symbol": symbol.upper(), "from": from_date,
                        "to": to_date, "apikey": key},
            )
        _STATE["fmp_calls_used"] = _STATE.get("fmp_calls_used", 0) + 1

        if resp.status_code not in (200, 201):
            if resp.status_code in (404, 403, 402):
                return []
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
            print(f"[CANON_BACKFILL] FMP fallback {symbol}: {len(bars)} bars ✓")
        return bars

    except Exception as exc:
        print(f"[CANON_BACKFILL] FMP exception {symbol}: {exc}")
        return []


# ── Actual-ticker-history-limit detection ─────────────────────────────────────

def _detect_actual_limit(bars: list[dict]) -> bool:
    if len(bars) >= 504:
        return False
    if not bars:
        return False
    oldest = bars[0].get("date", "")
    if not oldest:
        return False
    try:
        oldest_d = datetime.strptime(oldest[:10], "%Y-%m-%d").date()
        return (date.today() - oldest_d).days < 730
    except Exception:
        return False


def _classify_tradier_capability(bar_count: int, is_actual_limit: bool) -> str:
    if is_actual_limit:
        return "TRADIER_SYMBOL_TOO_NEW"
    if bar_count >= 1100:
        return "TRADIER_FULL_HISTORY_OK"
    if bar_count >= 40:
        return "TRADIER_PARTIAL_HISTORY"
    return "TRADIER_PROVIDER_FAILED"


# ── Single-symbol backfill ─────────────────────────────────────────────────────

async def backfill_symbol(
    symbol:          str,
    allow_fmp_fallback: bool = True,
    mode:            str     = "initial_full_backfill",
) -> dict:
    """
    Fetch and cache canonical 5-year history for one symbol.

    mode='incremental_daily_append'  — only fetch missing bars since newest_bar_date.
    mode='manual_rebuild'            — force full 5Y re-fetch.
    All other modes                  — full 5Y fetch if symbol not fresh.

    Returns a result dict describing the outcome.
    """
    from services.canonical_history_service import (
        save_bars, append_bars, mark_failed, mark_excluded,
        get_metadata, is_fresh, get_bars as _get_cached_bars,
    )
    sym = symbol.upper()

    if not _is_eligible(sym):
        mark_excluded(sym)
        return {"symbol": sym, "action": "excluded", "bar_count": 0,
                "tradier_capability": "TRADIER_UNVERIFIED"}

    # ── Fresh-cache fast-path ─────────────────────────────────────────────────
    if mode not in ("manual_rebuild", "incremental_daily_append"):
        if is_fresh(sym, max_age_h=20.0):
            meta = get_metadata(sym)
            if meta and meta.get("history_status") == "available_5y":
                return {
                    "symbol":              sym,
                    "action":              "skipped_fresh",
                    "bar_count":           meta.get("bar_count", 0),
                    "provider":            meta.get("provider", "unknown"),
                    "tradier_capability":  meta.get("tradier_capability",
                                                    "TRADIER_FULL_HISTORY_OK"),
                }

    # ── Incremental append path ───────────────────────────────────────────────
    if mode == "incremental_daily_append":
        meta = get_metadata(sym)
        if meta and meta.get("history_status") == "available_5y":
            newest = meta.get("newest_bar_date")
            if newest:
                try:
                    newest_d     = datetime.strptime(newest[:10], "%Y-%m-%d").date()
                    days_behind  = (date.today() - newest_d).days
                    if days_behind <= 0:
                        return {"symbol": sym, "action": "skipped_current",
                                "bar_count": meta.get("bar_count", 0)}
                    start_date = (newest_d - timedelta(days=2)).isoformat()  # small overlap
                    end_date   = date.today().isoformat()
                    new_bars = await _fetch_tradier_history_managed(sym, start_date, end_date)
                    if new_bars:
                        updated = append_bars(sym, new_bars, "tradier")
                        if updated:
                            print(f"[CANON_BACKFILL] incremental {sym}: "
                                  f"+{len(new_bars)} bars → {updated['bar_count']} total")
                            return {
                                "symbol":             sym,
                                "action":             "incremental_append",
                                "bar_count":          updated["bar_count"],
                                "new_bars":           len(new_bars),
                                "provider":           "tradier",
                                "tradier_capability": "TRADIER_FULL_HISTORY_OK",
                            }
                    return {"symbol": sym, "action": "incremental_no_new_bars",
                            "bar_count": meta.get("bar_count", 0)}
                except Exception as exc:
                    print(f"[CANON_BACKFILL] incremental error {sym}: {exc}")

    # ── Full 5Y fetch — Tradier primary (scheduler-safe) ──────────────────────
    start_date = (date.today() - timedelta(days=_LONG_HIST_DAYS)).isoformat()
    end_date   = date.today().isoformat()

    bars     = await _fetch_tradier_history_managed(sym, start_date, end_date)
    provider = "tradier"

    # ── FMP fallback — when Tradier returns < partial_history threshold ────────
    fmp_bars: list[dict] = []
    if allow_fmp_fallback and len(bars) < 504:
        fmp_bars = await _fetch_fmp_direct(sym)
        if fmp_bars and len(fmp_bars) > len(bars):
            print(f"[CANON_BACKFILL] FMP fallback preferred {sym}: "
                  f"tradier={len(bars)} fmp={len(fmp_bars)}")
            bars     = fmp_bars
            provider = "fmp"

    if not bars:
        mark_failed(sym, "no_bars_from_providers", provider)
        _STATE["last_error"] = f"{sym}: no bars"
        return {
            "symbol":             sym,
            "action":             "fetch_failed",
            "bar_count":          0,
            "tradier_capability": "TRADIER_PROVIDER_FAILED",
        }

    is_actual_limit   = _detect_actual_limit(bars)
    tradier_cap       = _classify_tradier_capability(
        len(bars) if provider == "tradier" else 0, is_actual_limit
    )
    meta = save_bars(
        sym, bars, provider,
        is_actual_limit=is_actual_limit,
        refresh_mode=mode,
        tradier_capability=tradier_cap,
    )

    if provider == "tradier":
        try:
            from data.cache import cache
            cache.set(f"tdier_hist:{sym}:{_LONG_HIST_DAYS}", bars, 14400)
        except Exception:
            pass
    elif provider == "fmp":
        try:
            from data.cache import cache
            cache.set(f"fmp_hist:{sym}", bars, 14400)
        except Exception:
            pass

    _STATE["last_success_at"] = datetime.now(timezone.utc).isoformat()
    return {
        "symbol":             sym,
        "action":             "saved",
        "bar_count":          meta["bar_count"],
        "history_status":     meta["history_status"],
        "provider":           meta["provider"],
        "years_available":    meta["years_available"],
        "is_actual_limit":    is_actual_limit,
        "tradier_capability": tradier_cap,
        "canonical_history_quality": meta.get("canonical_history_quality"),
    }


# ── Priority queue builder ────────────────────────────────────────────────────

def _priority_key(symbol: str) -> int:
    """
    Lower = higher priority.

    Tier 0 (0–1): not_yet_backfilled, fetch_failed
                  (or symbols with <756 bars — computed in _build_symbol_list_tiered)
    Tier 1 (2–4): recent_only, intermediate_only, partial_history, available_3y
    Tier 2 (5–8): available_5y (stale refresh)
    Excluded (99): excluded_prefixed_symbol, actual_ticker_history_limit
    """
    from services.canonical_history_service import get_metadata
    meta = get_metadata(symbol)
    if not meta:
        return 0
    return {
        "not_yet_backfilled":           0,
        "fetch_failed":                 1,
        "recent_only":                  2,
        "intermediate_only":            3,
        "partial_history":              4,
        "available_3y":                 5,
        "actual_ticker_history_limit":  9,
        "available_5y":                 8,
        "excluded_prefixed_symbol":    99,
    }.get(meta.get("history_status", "not_yet_backfilled"), 5)


async def _build_symbol_list_tiered(
    tier_0_override: Optional[list[str]] = None,
) -> list[str]:
    """
    Build prioritized eligible symbol list.

    Tier 0 override: caller can supply explicit symbols to promote (e.g. validation set).
    Tier 0 auto: symbols from watchlist + stage2 LKG with bar_count < 756 or not yet attempted.
    Tier 1: portfolio underlyings.
    Tier 2: remaining universe.
    """
    from services.canonical_history_service import get_metadata

    tier0: set[str] = set()
    tier1: set[str] = set()
    tier2: set[str] = set()

    if tier_0_override:
        for s in tier_0_override:
            if _is_eligible(s.upper()):
                tier0.add(s.upper())

    # Stage2 LKG universe
    try:
        import json
        from pathlib import Path
        lkg_path = Path(__file__).parent.parent / "data" / "watchlist_stage2_lkg.json"
        if lkg_path.exists():
            data = json.loads(lkg_path.read_text())
            for s, entry in (data.get("results") or {}).items():
                if not _is_eligible(s):
                    continue
                sym  = s.upper()
                meta = get_metadata(sym)
                bc   = meta.get("bar_count", 0) if meta else 0
                hs   = meta.get("history_status", "not_yet_backfilled") if meta else "not_yet_backfilled"
                if hs in ("not_yet_backfilled", "fetch_failed") or bc < 756:
                    tier0.add(sym)
                elif hs in ("recent_only", "intermediate_only", "partial_history", "available_3y"):
                    tier1.add(sym)
                else:
                    tier2.add(sym)
    except Exception as exc:
        print(f"[CANON_BACKFILL] stage2 LKG read error: {exc}")

    # Watchlist universe
    try:
        from data.pg_storage import watchlist_list, watchlist_read
        lists = await watchlist_list()
        for wl in lists:
            try:
                wdata = await watchlist_read(wl.get("id") or wl.get("name") or "")
                for t in (wdata.get("tickers") or []):
                    s = (t if isinstance(t, str) else t.get("symbol") or "").upper()
                    if not _is_eligible(s) or s in tier0:
                        continue
                    meta = get_metadata(s)
                    bc   = meta.get("bar_count", 0) if meta else 0
                    if bc < 756:
                        tier0.add(s)
                    elif s not in tier1:
                        tier2.add(s)
            except Exception:
                pass
    except Exception:
        pass

    # Portfolio underlyings → Tier 1 if not already in a higher tier
    try:
        from pathlib import Path
        import json
        portfolio_path = Path(__file__).parent.parent / "data" / "portfolio" / "active_holdings.json"
        if portfolio_path.exists():
            ph = json.loads(portfolio_path.read_text())
            for h in (ph.get("holdings") or ph if isinstance(ph, list) else []):
                s = (h.get("symbol") or "").upper()
                if _is_eligible(s) and s not in tier0:
                    tier1.add(s)
    except Exception:
        pass

    # Merge: tier0 → tier1 (non-tier0) → tier2 (non-tier0/1)
    ordered: list[str] = []
    seen: set[str] = set()
    for s in sorted(tier0):
        if s not in seen:
            ordered.append(s)
            seen.add(s)
    for s in sorted(tier1 - tier0):
        if s not in seen:
            ordered.append(s)
            seen.add(s)
    for s in sorted(tier2 - tier0 - tier1):
        if s not in seen:
            ordered.append(s)
            seen.add(s)

    # Within each tier, sort by priority_key (finer ordering)
    t0_syms = ordered[:len(tier0)]
    t1_syms = ordered[len(tier0):len(tier0) + len(tier1 - tier0)]
    t2_syms = ordered[len(tier0) + len(tier1 - tier0):]
    t0_syms.sort(key=_priority_key)
    t1_syms.sort(key=_priority_key)
    t2_syms.sort(key=_priority_key)
    return t0_syms + t1_syms + t2_syms


# ── Batch runner ───────────────────────────────────────────────────────────────

async def run_backfill_batch(
    symbols:             Optional[list[str]] = None,
    max_symbols:         int   = _DEFAULT_MAX_SYMS,
    delay_s:             float = _DEFAULT_DELAY_S,
    allow_fmp_fallback:  bool  = True,
    priority_only:       bool  = False,
    mode:                str   = "initial_full_backfill",
) -> dict:
    """
    Run one backfill batch.  Returns a summary dict.

    symbols=None      → auto-build tiered list from watchlist + stage2 LKG.
    symbols=[...]     → use explicit list (admin override).
    priority_only     → skip symbols already at available_5y.
    mode              → 'initial_full_backfill' | 'incremental_daily_append' | 'manual_rebuild'
    """
    global _STATE

    if _STATE["running"]:
        return {"error": "backfill_already_running", **_STATE}

    _STATE.update({
        "running":                 True,
        "started_at":              datetime.now(timezone.utc).isoformat(),
        "completed":               0,
        "skipped":                 0,
        "failed":                  0,
        "upgraded":                0,
        "last_symbol":             None,
        "last_success_at":         None,
        "last_error":              None,
        "error":                   None,
        "current_mode":            mode,
        "paused_reason":           None,
        "history_lane_calls_used": 0,
        "tradier_calls_used":      0,
        "fmp_calls_used":          0,
        "request_time_calls":      0,
    })

    try:
        if symbols is None:
            explicit_tier0 = None
        else:
            explicit_tier0 = [s.upper() for s in symbols if _is_eligible(s.upper())]

        if symbols is None:
            sym_list = await _build_symbol_list_tiered()
        else:
            sym_list = explicit_tier0 or []

        _STATE["_backlog_count"] = len(sym_list)

        if priority_only:
            from services.canonical_history_service import get_metadata
            sym_list = [
                s for s in sym_list
                if not (get_metadata(s) or {}).get("history_status") == "available_5y"
            ]

        sym_list = sym_list[:max_symbols]

        results: list[dict] = []
        for sym in sym_list:
            _STATE["last_symbol"]    = sym
            _STATE["paused_reason"]  = None

            # ── Session-aware throttle gate ───────────────────────────────
            session   = _get_session()
            effective_delay = _effective_delay_s(session, delay_s)

            if _is_active_session() and _options_flow_busy():
                reason = _pause_for_busy_options()
                print(f"[CANON_BACKFILL] {sym} — {reason}, extra sleep 5s")
                await asyncio.sleep(5.0)
                # Re-check after sleep
                if _options_flow_busy():
                    _STATE["skipped"] += 1
                    results.append({
                        "symbol": sym,
                        "action": "skipped_options_busy",
                        "bar_count": 0,
                    })
                    continue
                _STATE["paused_reason"] = None

            # ── Backfill call ─────────────────────────────────────────────
            try:
                r = await backfill_symbol(
                    sym,
                    allow_fmp_fallback=allow_fmp_fallback,
                    mode=mode,
                )
                results.append(r)
                action = r.get("action", "")
                if action in ("skipped_fresh", "skipped_current"):
                    _STATE["skipped"] += 1
                elif action in ("fetch_failed", "excluded"):
                    _STATE["failed"] += 1
                else:
                    _STATE["completed"] += 1
                    if r.get("bar_count", 0) >= 700:
                        _STATE["upgraded"] += 1
            except Exception as exc:
                print(f"[CANON_BACKFILL] error {sym}: {exc}")
                _STATE["failed"]     += 1
                _STATE["last_error"] = f"{sym}: {exc}"

            if effective_delay > 0:
                await asyncio.sleep(effective_delay)

        return {
            "batch_size":          len(sym_list),
            "completed":           _STATE["completed"],
            "skipped_fresh":       _STATE["skipped"],
            "failed":              _STATE["failed"],
            "upgraded_to_5y":      _STATE["upgraded"],
            "tradier_calls_used":  _STATE["tradier_calls_used"],
            "fmp_calls_used":      _STATE["fmp_calls_used"],
            "request_time_calls":  _STATE["request_time_calls"],  # always 0
            "mode":                mode,
            "results":             results,
        }

    except Exception as exc:
        _STATE["error"] = str(exc)
        raise
    finally:
        _STATE["running"] = False


# ── Incremental daily append runner ───────────────────────────────────────────

async def run_incremental_append(
    symbols:     Optional[list[str]] = None,
    max_symbols: int                 = 200,
    delay_s:     float               = _OFFHOURS_DELAY_S,
) -> dict:
    """
    Append-only refresh: only fetch bars since newest_bar_date for symbols
    that already have available_5y or available_3y history.

    Intended to run nightly off-hours.  Much faster than a full backfill.
    """
    return await run_backfill_batch(
        symbols=symbols,
        max_symbols=max_symbols,
        delay_s=delay_s,
        allow_fmp_fallback=False,
        priority_only=False,
        mode="incremental_daily_append",
    )
