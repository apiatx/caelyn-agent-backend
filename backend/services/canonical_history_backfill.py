"""
canonical_history_backfill.py — 10-year price history backfill job.
====================================================================
V4.2.5.4 — Safe-mode hardening:
  - All Tradier calls through TradierProvider._get() (TRADIER_LIMITER + lane budget)
  - 4 safe-enable flags (all conservative defaults)
  - Market-hours gate: backfill paused unless CANONICAL_HISTORY_ALLOW_MARKET_HOURS=true
  - 10Y target (3650 days)
  - weekly_health_check = metadata-only (no provider calls)
  - monthly_full_refresh = disabled by default, requires confirm=true
  - Incremental append = 1 request per symbol (2-day overlap inside that request)
  - available_10y symbols skip full refetch unconditionally

===========================================================================
SAFE-MODE FLAGS (all default to safe/off)
===========================================================================
  CANONICAL_HISTORY_BACKFILL_ENABLED=false
      Master switch. If false: no provider calls; status + cache reads still work.

  CANONICAL_HISTORY_FULL_BACKFILL_ENABLED=false
      If false: no full 10Y fetch allowed.
      Exception: manual endpoint with mode=manual_rebuild AND confirm=true.

  CANONICAL_HISTORY_INCREMENTAL_APPEND_ENABLED=true
      If false: incremental append jobs also blocked.

  CANONICAL_HISTORY_ALLOW_MARKET_HOURS=false
      If false: all provider calls paused during active market session.
      Returns paused_market_hours instead of calling Tradier.

===========================================================================
PROVIDER ORDER
===========================================================================
  1. Tradier /markets/history (scheduler-safe, canonical_history_backfill lane)
  2. FMP historical-price-eod (fallback — only when Tradier returns < 700 bars
     and symbol is NOT actual_ticker_history_limit)

===========================================================================
BACKFILL MODES
===========================================================================
  initial_full_backfill     First-ever 10Y fetch (3650-day window)
  incremental_daily_append  Fetch only bars since newest_bar_date - 2d; 1 request/symbol
  manual_rebuild            Force full re-fetch (requires confirm=true from endpoint)
  cache_read_only           Diagnostics only; no provider calls
  weekly_health_check       Metadata/file existence check; sample 1 symbol; no full-refetch
  monthly_full_refresh      Full re-fetch (admin only; requires confirm=true; disabled by default)

===========================================================================
PRIORITY TIERS
===========================================================================
  Tier 0  not_yet_backfilled, fetch_failed, bar_count < 756
  Tier 1  available_3y_partial_history, partial_history, intermediate_only;
           portfolio underlyings
  Tier 2  available_5y_partial_long_history (upgrade to 10Y)
  SKIP    available_10y, actual_ticker_history_limit (complete — never full-refetch)
  SKIP    excluded_prefixed_symbol

===========================================================================
TRADIER HISTORICAL CAPABILITY
===========================================================================
  TRADIER_FULL_HISTORY_OK       bars >= 2200 (≥ 8.7 years, 10Y complete)
  TRADIER_PARTIAL_LONG_HISTORY  1100 <= bars < 2200 (5Y range)
  TRADIER_PARTIAL_HISTORY       40 <= bars < 1100
  TRADIER_SYMBOL_TOO_NEW        is_actual_limit=True (genuine new ticker)
  TRADIER_PROVIDER_FAILED       bars == 0 after attempts
  TRADIER_UNVERIFIED            not yet tested
"""
from __future__ import annotations

import asyncio
import os
from datetime import date, datetime, timedelta, timezone
from typing import Optional

# ── Safe-mode flags (conservative defaults) ──────────────────────────────────

def _flag(env: str, default: str) -> bool:
    return os.environ.get(env, default).strip().lower() in ("1", "true", "yes", "on")

_BACKFILL_ENABLED          = lambda: _flag("CANONICAL_HISTORY_BACKFILL_ENABLED", "false")
_FULL_BACKFILL_ENABLED     = lambda: _flag("CANONICAL_HISTORY_FULL_BACKFILL_ENABLED", "false")
_INCREMENTAL_APPEND_ENABLED = lambda: _flag("CANONICAL_HISTORY_INCREMENTAL_APPEND_ENABLED", "true")
_ALLOW_MARKET_HOURS        = lambda: _flag("CANONICAL_HISTORY_ALLOW_MARKET_HOURS", "false")

# ── History window ────────────────────────────────────────────────────────────

_LONG_HIST_DAYS     = 3650   # 10Y window for initial/full backfill
_FMP_LONG_HIST_DAYS = 3750   # FMP 10.3Y window (slight buffer)
_DEFAULT_MAX_SYMS   = 80
_DEFAULT_DELAY_S    = 0.5    # base inter-symbol delay (seconds)

# Off-hours: target at most this many RPM through the scheduler lane.
# Achieved via sleep math, NOT by raising the lane budget.
_OFFHOURS_MAX_RPM:  int   = int(os.environ.get("TRADIER_CANON_HIST_OFFHOURS_MAX_RPM", "10"))
_OFFHOURS_DELAY_S:  float = max(0.5, 60.0 / max(1, _OFFHOURS_MAX_RPM))

# Options-flow busy gate: if options_flow lane >= this fraction, skip current call.
_PAUSE_IF_OPTIONS_BUSY_PCT: float = float(
    os.environ.get("CANON_HIST_OPTIONS_BUSY_PAUSE_PCT", "0.80")
)

_STATE: dict = {
    "running":                    False,
    "started_at":                 None,
    "completed":                  0,
    "skipped":                    0,
    "failed":                     0,
    "upgraded":                   0,
    "last_symbol":                None,
    "last_success_at":            None,
    "last_error":                 None,
    "error":                      None,
    "current_mode":               None,
    "paused_reason":              None,
    "history_lane_calls_used":    0,
    "tradier_calls_used":         0,
    "fmp_calls_used":             0,
    "request_time_calls":         0,   # must always remain 0
    "_backlog_count":             0,
}


def get_backfill_status() -> dict:
    """Return full diagnostics snapshot including scheduler lane and options-flow context."""
    snap = dict(_STATE)
    # Safe flag state
    snap["flags"] = {
        "CANONICAL_HISTORY_BACKFILL_ENABLED":           _BACKFILL_ENABLED(),
        "CANONICAL_HISTORY_FULL_BACKFILL_ENABLED":      _FULL_BACKFILL_ENABLED(),
        "CANONICAL_HISTORY_INCREMENTAL_APPEND_ENABLED": _INCREMENTAL_APPEND_ENABLED(),
        "CANONICAL_HISTORY_ALLOW_MARKET_HOURS":         _ALLOW_MARKET_HOURS(),
    }
    try:
        import time as _t
        from data.tradier_budget import BUDGETS, _timestamps, WINDOW_S
        cutoff   = _t.monotonic() - WINDOW_S
        hist_used = sum(1 for ts in _timestamps["canonical_history_backfill"] if ts > cutoff)
        opts_used = sum(1 for ts in _timestamps["options_flow"]               if ts > cutoff)
        quot_used = sum(1 for ts in _timestamps["quotes"]                     if ts > cutoff)
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
        snap["canonical_history_backfill_budget_used"] = hist_used
        snap["canonical_history_backfill_budget_cap"]  = BUDGETS["canonical_history_backfill"]
        snap["canonical_history_backfill_saturated"]   = (
            hist_used >= BUDGETS["canonical_history_backfill"]
        )
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
    symbol:     str,
    start_date: str,
    end_date:   str,
) -> list[dict]:
    """
    Scheduler-safe Tradier /markets/history fetch.

    Routes through TradierProvider._get() which enforces:
      - TRADIER_LIMITER (global 110 RPM sliding window)
      - canonical_history_backfill lane budget (5 RPM default)

    Zero raw httpx calls.  Returns [] if lane is over-budget (deferred).
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


# ── Session-aware helpers ─────────────────────────────────────────────────────

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
        cutoff = _t.monotonic() - _W
        used   = sum(1 for ts in _timestamps["options_flow"] if ts > cutoff)
        budget = BUDGETS["options_flow"]
        return used >= budget * _PAUSE_IF_OPTIONS_BUSY_PCT
    except Exception:
        return False


def _effective_delay_s(session: str, base_delay: float) -> float:
    """
    Inter-symbol sleep based on session.
    Off-hours / weekend: shorten to approach _OFFHOURS_MAX_RPM.
    Active session: use base_delay (conservative; lane cap enforces the rest).
    """
    if session in ("off_hours", "weekend"):
        return _OFFHOURS_DELAY_S
    return base_delay


# ── Market-hours gate ─────────────────────────────────────────────────────────

def _check_market_hours_gate() -> Optional[str]:
    """
    Returns a pause reason string if the backfill should be blocked,
    or None if it's safe to proceed.
    """
    if not _ALLOW_MARKET_HOURS():
        if _is_active_session():
            return "paused_market_hours"
    return None


# ── Full-backfill allowed check ───────────────────────────────────────────────

def _check_full_backfill_allowed(
    mode: str,
    confirm: bool = False,
) -> Optional[str]:
    """Returns error reason string if full backfill is not allowed, else None."""
    if not _BACKFILL_ENABLED():
        return "backfill_disabled_by_flag"
    if mode in ("manual_rebuild", "monthly_full_refresh"):
        if not confirm:
            return f"{mode}_requires_confirm=true"
        if mode == "monthly_full_refresh" and not _FULL_BACKFILL_ENABLED():
            return "monthly_full_refresh_disabled_by_flag"
    if mode == "initial_full_backfill" and not _FULL_BACKFILL_ENABLED():
        return "full_backfill_disabled_by_flag"
    return None


# ── FMP direct fetch (fallback only) ─────────────────────────────────────────

async def _fetch_fmp_direct(symbol: str) -> list[dict]:
    """
    FMP historical EOD — fallback when Tradier returns insufficient bars
    and the symbol is not a genuine new-ticker (actual_ticker_history_limit).
    Admin/backfill use only; bypasses FMP_BLOCK_FULL_HISTORICAL guard.
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


# ── Actual-ticker-history-limit detection ────────────────────────────────────

def _detect_actual_limit(bars: list[dict]) -> bool:
    """
    Returns True when bars are sparse AND the oldest bar is genuinely recent
    (the symbol is legitimately < 10 years old, not a data gap).
    """
    if not bars or len(bars) >= 2520:
        return False
    oldest = bars[0].get("date", "")
    if not oldest:
        return False
    try:
        oldest_d = datetime.strptime(oldest[:10], "%Y-%m-%d").date()
        return (date.today() - oldest_d).days < 3650
    except Exception:
        return False


def _classify_tradier_capability(bar_count: int, is_actual_limit: bool) -> str:
    if is_actual_limit:
        return "TRADIER_SYMBOL_TOO_NEW"
    if bar_count >= 2200:
        return "TRADIER_FULL_HISTORY_OK"
    if bar_count >= 1100:
        return "TRADIER_PARTIAL_LONG_HISTORY"
    if bar_count >= 40:
        return "TRADIER_PARTIAL_HISTORY"
    return "TRADIER_PROVIDER_FAILED"


# ── Single-symbol backfill ────────────────────────────────────────────────────

async def backfill_symbol(
    symbol:             str,
    allow_fmp_fallback: bool  = True,
    mode:               str   = "initial_full_backfill",
    confirm:            bool  = False,
) -> dict:
    """
    Fetch and cache canonical 10-year history for one symbol.

    Skips symbols already at available_10y or actual_ticker_history_limit
    (those are complete and must not be full-refetched on normal runs).

    mode='incremental_daily_append' → only fetch bars since newest_bar_date.
    mode='manual_rebuild'           → force full 10Y re-fetch (confirm required).
    """
    from services.canonical_history_service import (
        save_bars, append_bars, mark_failed, mark_excluded,
        get_metadata, is_fresh, is_10y_complete, needs_append,
        get_bars as _get_cached_bars,
        _10Y_MIN_BARS,
    )
    sym = symbol.upper()

    if not _is_eligible(sym):
        mark_excluded(sym)
        return {"symbol": sym, "action": "excluded", "bar_count": 0,
                "tradier_capability": "TRADIER_UNVERIFIED"}

    meta = get_metadata(sym)
    status = (meta or {}).get("history_status", "not_yet_backfilled")

    # ── Already complete — skip unless explicit manual_rebuild ────────────────
    if mode not in ("manual_rebuild",) and (
        is_10y_complete(status) or status == "actual_ticker_history_limit"
    ):
        return {
            "symbol":              sym,
            "action":              "skipped_complete",
            "bar_count":           (meta or {}).get("bar_count", 0),
            "history_status":      status,
            "tradier_capability":  (meta or {}).get("tradier_capability",
                                                     "TRADIER_FULL_HISTORY_OK"),
        }

    # ── Fresh-cache fast-path (non-10Y statuses) ──────────────────────────────
    if mode not in ("manual_rebuild", "incremental_daily_append"):
        if is_fresh(sym, max_age_h=20.0):
            return {
                "symbol":        sym,
                "action":        "skipped_fresh",
                "bar_count":     (meta or {}).get("bar_count", 0),
                "provider":      (meta or {}).get("provider", "unknown"),
                "history_status": status,
            }

    # ── Incremental append path ───────────────────────────────────────────────
    if mode == "incremental_daily_append":
        if not _INCREMENTAL_APPEND_ENABLED():
            return {"symbol": sym, "action": "skipped_append_disabled", "bar_count": 0}

        if meta and meta.get("newest_bar_date"):
            newest = meta["newest_bar_date"]
            try:
                newest_d    = datetime.strptime(newest[:10], "%Y-%m-%d").date()
                days_behind = (date.today() - newest_d).days
                if days_behind <= 0:
                    return {"symbol": sym, "action": "skipped_current",
                            "bar_count": meta.get("bar_count", 0)}

                # One provider call — start date has 2-day overlap inside it
                start_date = (newest_d - timedelta(days=2)).isoformat()
                end_date   = date.today().isoformat()

                mh_reason = _check_market_hours_gate()
                if mh_reason:
                    _STATE["paused_reason"] = mh_reason
                    return {"symbol": sym, "action": "skipped",
                            "history_lane_paused_reason": mh_reason}

                new_bars = await _fetch_tradier_history_managed(sym, start_date, end_date)
                if new_bars:
                    updated = append_bars(sym, new_bars, "tradier")
                    if updated:
                        return {
                            "symbol":              sym,
                            "action":              "incremental_append",
                            "bar_count":           updated["bar_count"],
                            "new_bars":            len(new_bars),
                            "provider":            "tradier",
                            "tradier_capability":  "TRADIER_FULL_HISTORY_OK",
                        }
                return {"symbol": sym, "action": "incremental_no_new_bars",
                        "bar_count": meta.get("bar_count", 0)}
            except Exception as exc:
                print(f"[CANON_BACKFILL] incremental error {sym}: {exc}")

    # ── Full 10Y fetch ────────────────────────────────────────────────────────
    start_date = (date.today() - timedelta(days=_LONG_HIST_DAYS)).isoformat()
    end_date   = date.today().isoformat()

    bars     = await _fetch_tradier_history_managed(sym, start_date, end_date)
    provider = "tradier"

    # FMP fallback — only if Tradier returns < 700 bars AND not a genuine new ticker
    fmp_bars: list[dict] = []
    if allow_fmp_fallback and len(bars) < 700:
        is_lim_check = _detect_actual_limit(bars)
        if not is_lim_check:
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

    is_actual_limit  = _detect_actual_limit(bars)
    tradier_cap      = _classify_tradier_capability(
        len(bars) if provider == "tradier" else 0, is_actual_limit
    )
    meta_out = save_bars(
        sym, bars, provider,
        is_actual_limit=is_actual_limit,
        refresh_mode=mode,
        tradier_capability=tradier_cap,
    )

    # Warm in-memory cache (Fib/Stage2 fallback path)
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
        "bar_count":          meta_out["bar_count"],
        "history_status":     meta_out["history_status"],
        "provider":           meta_out["provider"],
        "years_available":    meta_out["years_available"],
        "is_actual_limit":    is_actual_limit,
        "tradier_capability": tradier_cap,
        "canonical_history_quality": meta_out.get("canonical_history_quality"),
    }


# ── Priority queue builder ────────────────────────────────────────────────────

def _priority_key(symbol: str) -> int:
    from services.canonical_history_service import get_metadata, is_10y_complete
    meta = get_metadata(symbol)
    if not meta:
        return 0
    hs = meta.get("history_status", "not_yet_backfilled")
    bc = meta.get("bar_count", 0)
    # Complete symbols should never appear in a priority list (they're filtered)
    if is_10y_complete(hs) or hs == "actual_ticker_history_limit":
        return 99
    return {
        "not_yet_backfilled":           0,
        "fetch_failed":                 1,
        "cache_corrupt_needs_rebuild":  1,
        "recent_only":                  2,
        "intermediate_only":            3,
        "partial_history":              4,
        "available_3y_partial_history": 5,
        "available_5y_partial_long_history": 6,
        "excluded_prefixed_symbol":    99,
    }.get(hs, 5)


async def _build_symbol_list_tiered(
    tier_0_override: Optional[list[str]] = None,
) -> list[str]:
    """
    Build prioritized eligible symbol list.

    Symbols already at available_10y or actual_ticker_history_limit are excluded
    from all tiers — they are complete and must not be re-fetched.
    """
    from services.canonical_history_service import get_metadata, is_10y_complete

    tier0: set[str] = set()
    tier1: set[str] = set()
    tier2: set[str] = set()
    complete: set[str] = set()

    def _classify(s: str) -> None:
        meta = get_metadata(s)
        bc   = meta.get("bar_count", 0) if meta else 0
        hs   = meta.get("history_status", "not_yet_backfilled") if meta else "not_yet_backfilled"
        if is_10y_complete(hs) or hs == "actual_ticker_history_limit":
            complete.add(s)
            return
        if hs in ("not_yet_backfilled", "fetch_failed",
                  "cache_corrupt_needs_rebuild") or bc < 756:
            tier0.add(s)
        elif hs in ("available_3y_partial_history", "partial_history",
                    "intermediate_only", "recent_only"):
            tier1.add(s)
        elif hs == "available_5y_partial_long_history":
            tier2.add(s)
        else:
            tier1.add(s)

    if tier_0_override:
        for s in tier_0_override:
            su = s.upper()
            if _is_eligible(su):
                _classify(su)

    # Stage2 LKG universe
    try:
        import json
        from pathlib import Path
        lkg_path = Path(__file__).parent.parent / "data" / "watchlist_stage2_lkg.json"
        if lkg_path.exists():
            data = json.loads(lkg_path.read_text())
            for s in (data.get("results") or {}):
                if _is_eligible(s.upper()):
                    _classify(s.upper())
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
                    if _is_eligible(s):
                        _classify(s)
            except Exception:
                pass
    except Exception:
        pass

    # Portfolio underlyings → Tier 1 if not already complete
    try:
        from pathlib import Path
        import json
        portfolio_path = Path(__file__).parent.parent / "data" / "portfolio" / "active_holdings.json"
        if portfolio_path.exists():
            ph = json.loads(portfolio_path.read_text())
            for h in (ph.get("holdings") or ph if isinstance(ph, list) else []):
                s = (h.get("symbol") or "").upper()
                if _is_eligible(s) and s not in complete and s not in tier0:
                    _classify(s)
    except Exception:
        pass

    ordered: list[str] = []
    seen: set[str] = set()
    for group in (tier0, tier1 - tier0, tier2 - tier0 - tier1):
        syms = sorted(group)
        syms.sort(key=_priority_key)
        for s in syms:
            if s not in seen:
                ordered.append(s)
                seen.add(s)

    return ordered


# ── Batch runner ───────────────────────────────────────────────────────────────

async def run_backfill_batch(
    symbols:             Optional[list[str]] = None,
    max_symbols:         int                 = _DEFAULT_MAX_SYMS,
    delay_s:             float               = _DEFAULT_DELAY_S,
    allow_fmp_fallback:  bool                = True,
    priority_only:       bool                = False,
    mode:                str                 = "initial_full_backfill",
    confirm:             bool                = False,
) -> dict:
    """
    Run one backfill batch.

    symbols=None  → auto-build tiered list.
    symbols=[...]  → explicit override list (admin).
    priority_only  → skip symbols not yet needing a full fetch.
    mode           → see module docstring for allowed values.
    confirm        → required for manual_rebuild and monthly_full_refresh.

    Safe-mode checks at entry:
      1. CANONICAL_HISTORY_BACKFILL_ENABLED
      2. CANONICAL_HISTORY_FULL_BACKFILL_ENABLED (for full-fetch modes)
      3. CANONICAL_HISTORY_ALLOW_MARKET_HOURS (market-hours gate)

    weekly_health_check mode:
      - Checks metadata / index / file existence only.
      - Optionally samples 1 symbol to verify bar readability.
      - NO provider calls.

    Incremental daily append:
      - Fetches only bars since newest_bar_date - 2 days.
      - Exactly 1 Tradier request per symbol (2-day overlap inside that request).
      - Requires CANONICAL_HISTORY_INCREMENTAL_APPEND_ENABLED=true.
    """
    global _STATE

    if _STATE["running"]:
        return {"error": "backfill_already_running", **_STATE}

    # ── weekly_health_check: metadata-only, no provider calls ─────────────────
    if mode == "weekly_health_check":
        return await _run_weekly_health_check(symbols or [])

    # ── Gate: master backfill flag ────────────────────────────────────────────
    flag_reason = _check_full_backfill_allowed(mode, confirm)
    if flag_reason:
        return {"error": flag_reason, "mode": mode, "flags": get_backfill_status()["flags"]}

    # ── Gate: incremental append flag ─────────────────────────────────────────
    if mode == "incremental_daily_append" and not _INCREMENTAL_APPEND_ENABLED():
        return {"error": "incremental_append_disabled_by_flag", "mode": mode}

    # ── Gate: market hours ────────────────────────────────────────────────────
    mh_reason = _check_market_hours_gate()
    if mh_reason:
        return {
            "error":                      mh_reason,
            "history_lane_paused_reason": mh_reason,
            "mode":                       mode,
        }

    _STATE.update({
        "running":                  True,
        "started_at":               datetime.now(timezone.utc).isoformat(),
        "completed":                0,
        "skipped":                  0,
        "failed":                   0,
        "upgraded":                 0,
        "last_symbol":              None,
        "last_success_at":          None,
        "last_error":               None,
        "error":                    None,
        "current_mode":             mode,
        "paused_reason":            None,
        "history_lane_calls_used":  0,
        "tradier_calls_used":       0,
        "fmp_calls_used":           0,
        "request_time_calls":       0,
    })

    try:
        explicit_tier0 = (
            [s.upper() for s in symbols if _is_eligible(s.upper())]
            if symbols else None
        )
        sym_list = (
            await _build_symbol_list_tiered(explicit_tier0)
            if symbols is None
            else (explicit_tier0 or [])
        )
        _STATE["_backlog_count"] = len(sym_list)

        if priority_only:
            from services.canonical_history_service import get_metadata
            sym_list = [
                s for s in sym_list
                if (get_metadata(s) or {}).get("history_status") not in (
                    "available_5y_partial_long_history",
                )
            ]

        sym_list = sym_list[:max_symbols]
        results: list[dict] = []

        for sym in sym_list:
            _STATE["last_symbol"]   = sym
            _STATE["paused_reason"] = None

            # Re-check market hours gate each symbol (session may change mid-batch)
            mh = _check_market_hours_gate()
            if mh:
                _STATE["paused_reason"] = mh
                print(f"[CANON_BACKFILL] {sym} — {mh}, stopping batch")
                _STATE["skipped"] += (len(sym_list) - sym_list.index(sym))
                break

            session         = _get_session()
            effective_delay = _effective_delay_s(session, delay_s)

            # Options-flow busy gate (active session only)
            if _is_active_session() and _options_flow_busy():
                reason = "options_flow_busy_gate"
                _STATE["paused_reason"] = reason
                print(f"[CANON_BACKFILL] {sym} — {reason}, extra sleep 5s")
                await asyncio.sleep(5.0)
                if _options_flow_busy():
                    _STATE["skipped"] += 1
                    results.append({
                        "symbol": sym, "action": "skipped_options_busy", "bar_count": 0,
                    })
                    continue
                _STATE["paused_reason"] = None

            try:
                r = await backfill_symbol(
                    sym,
                    allow_fmp_fallback=allow_fmp_fallback,
                    mode=mode,
                    confirm=confirm,
                )
                results.append(r)
                action = r.get("action", "")
                if action in ("skipped_fresh", "skipped_current", "skipped_complete",
                              "skipped_append_disabled"):
                    _STATE["skipped"] += 1
                elif action in ("fetch_failed", "excluded"):
                    _STATE["failed"] += 1
                else:
                    _STATE["completed"] += 1
                    if r.get("bar_count", 0) >= 2200:
                        _STATE["upgraded"] += 1
            except Exception as exc:
                print(f"[CANON_BACKFILL] error {sym}: {exc}")
                _STATE["failed"]     += 1
                _STATE["last_error"] = f"{sym}: {exc}"

            if effective_delay > 0:
                await asyncio.sleep(effective_delay)

        return {
            "batch_size":         len(sym_list),
            "completed":          _STATE["completed"],
            "skipped":            _STATE["skipped"],
            "failed":             _STATE["failed"],
            "upgraded_to_10y":    _STATE["upgraded"],
            "tradier_calls_used": _STATE["tradier_calls_used"],
            "fmp_calls_used":     _STATE["fmp_calls_used"],
            "request_time_calls": _STATE["request_time_calls"],   # always 0
            "mode":               mode,
            "results":            results,
        }

    except Exception as exc:
        _STATE["error"] = str(exc)
        raise
    finally:
        _STATE["running"] = False


# ── Weekly health check (metadata-only, no provider calls) ────────────────────

async def _run_weekly_health_check(
    sample_symbols: list[str],
    max_sample:     int = 1,
) -> dict:
    """
    Check metadata/index/file existence for all cached symbols.
    Optionally sample up to max_sample symbols to verify gz readability.
    NO provider calls.
    """
    from pathlib import Path
    from services.canonical_history_service import (
        get_all_status, get_bars as _get_bars, _bar_file,
    )

    all_meta = get_all_status()
    file_missing:  list[str] = []
    file_corrupt:  list[str] = []
    file_ok:       list[str] = []
    needs_append:  list[str] = []

    for sym, meta in all_meta.items():
        f = _bar_file(sym)
        if not f.exists():
            file_missing.append(sym)
            continue
        hs = meta.get("history_status", "")
        # Check append need (newest bar > 3 days old)
        try:
            from services.canonical_history_service import needs_append as _needs_append
            if _needs_append(sym):
                needs_append.append(sym)
        except Exception:
            pass

    # Sample readability check (no provider call — disk read only)
    sample_checked:  list[str] = []
    sample_readable: list[str] = []
    sample_broken:   list[str] = []

    for sym in (sample_symbols or list(all_meta.keys()))[:max_sample]:
        payload = _get_bars(sym, require_fresh=False)
        sample_checked.append(sym)
        if payload and payload.get("bars"):
            sample_readable.append(sym)
        else:
            sample_broken.append(sym)

    return {
        "mode":             "weekly_health_check",
        "total_indexed":    len(all_meta),
        "file_missing":     file_missing,
        "file_corrupt":     file_corrupt,
        "needs_append":     needs_append,
        "sample_checked":   sample_checked,
        "sample_readable":  sample_readable,
        "sample_broken":    sample_broken,
        "provider_calls":   0,
        "request_time_calls": 0,
    }


# ── Incremental daily append runner ───────────────────────────────────────────

async def run_incremental_append(
    symbols:     Optional[list[str]] = None,
    max_symbols: int                 = 200,
    delay_s:     float               = _OFFHOURS_DELAY_S,
) -> dict:
    """
    Append-only refresh: only fetch bars since newest_bar_date - 2d.
    Exactly 1 Tradier request per symbol (2-day overlap is within that single request).
    Intended to run nightly off-hours.

    Requires CANONICAL_HISTORY_BACKFILL_ENABLED=true
         and CANONICAL_HISTORY_INCREMENTAL_APPEND_ENABLED=true.
    """
    return await run_backfill_batch(
        symbols=symbols,
        max_symbols=max_symbols,
        delay_s=delay_s,
        allow_fmp_fallback=False,
        priority_only=False,
        mode="incremental_daily_append",
        confirm=False,
    )
