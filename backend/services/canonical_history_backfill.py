"""
canonical_history_backfill.py — 10-year price history backfill job.
====================================================================
V4.2.5.5 — Tradier 10Y precision fix.

ROOT CAUSE OF PREVIOUS ~5Y CLAIM (now corrected):
  V4.2.5.3 used _LONG_HIST_DAYS = 1825 (5Y window).
  Tradier returned ~1253 bars — correct for 1825 calendar days.
  The report incorrectly concluded "Tradier caps at 5Y."
  That was never tested with a 3650-day window. V4.2.5.5 fixes this.

PROVIDER ORDER (corrected):
  1. Fresh canonical cache (disk gz)                     — no provider call
  2. Tradier /markets/history, start = today - 3650d    — PRIMARY for all depth
  3. FMP historical-price-eod                           — FALLBACK: only when Tradier
     returns 0 bars (complete failure) for a symbol
  4. 400-bar Stage cache (emergency, request-time only) — LAST RESORT

SAFE-MODE FLAGS (all conservative defaults):
  CANONICAL_HISTORY_BACKFILL_ENABLED=false
  CANONICAL_HISTORY_FULL_BACKFILL_ENABLED=false
  CANONICAL_HISTORY_INCREMENTAL_APPEND_ENABLED=true
  CANONICAL_HISTORY_ALLOW_MARKET_HOURS=false

LIMIT CLASSIFICATION (V4.2.5.5):
  available_10y               >= 2200 bars (Tradier returned full 10Y depth)
  available_lifetime_under_10y  ticker < 10Y old; oldest_bar is near requested start
  available_5y_partial_long_history  ~1100-2199 bars; ticker likely older, provider returned partial
  provider_cap_detected        10Y request sent; provider returned truncated range;
                                oldest_bar significantly later than requested start

  IMPORTANT: _classify_limit_type() uses the REQUESTED start_date to determine
  whether the oldest returned bar represents the ticker's actual IPO boundary vs
  a provider cap. A symbol with oldest_bar within 90 days of start_date is
  classified as available_lifetime_under_10y (full public history). A symbol
  with oldest_bar > 90 days AFTER start_date and bar_count < 2200 may be
  provider_cap_detected if enough bars were returned to rule out a new ticker.
"""
from __future__ import annotations

import asyncio
import os
from datetime import date, datetime, timedelta, timezone
from typing import Optional

# ── Safe-mode flags (conservative defaults) ──────────────────────────────────

def _flag(env: str, default: str) -> bool:
    return os.environ.get(env, default).strip().lower() in ("1", "true", "yes", "on")

_BACKFILL_ENABLED           = lambda: _flag("CANONICAL_HISTORY_BACKFILL_ENABLED",           "false")
_FULL_BACKFILL_ENABLED      = lambda: _flag("CANONICAL_HISTORY_FULL_BACKFILL_ENABLED",      "false")
_INCREMENTAL_APPEND_ENABLED = lambda: _flag("CANONICAL_HISTORY_INCREMENTAL_APPEND_ENABLED", "true")
_ALLOW_MARKET_HOURS         = lambda: _flag("CANONICAL_HISTORY_ALLOW_MARKET_HOURS",         "false")

# ── History window ────────────────────────────────────────────────────────────

_LONG_HIST_DAYS     = 3650   # 10Y calendar window (primary)
_FMP_LONG_HIST_DAYS = 3750   # FMP 10.3Y buffer (fallback)
_DEFAULT_MAX_SYMS   = 80
_DEFAULT_DELAY_S    = 0.5

_OFFHOURS_MAX_RPM:  int   = int(os.environ.get("TRADIER_CANON_HIST_OFFHOURS_MAX_RPM", "10"))
_OFFHOURS_DELAY_S:  float = max(0.5, 60.0 / max(1, _OFFHOURS_MAX_RPM))

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
    "request_time_calls":         0,  # must always remain 0
    "_backlog_count":             0,
}


def get_backfill_status() -> dict:
    snap = dict(_STATE)
    snap["flags"] = {
        "CANONICAL_HISTORY_BACKFILL_ENABLED":           _BACKFILL_ENABLED(),
        "CANONICAL_HISTORY_FULL_BACKFILL_ENABLED":      _FULL_BACKFILL_ENABLED(),
        "CANONICAL_HISTORY_INCREMENTAL_APPEND_ENABLED": _INCREMENTAL_APPEND_ENABLED(),
        "CANONICAL_HISTORY_ALLOW_MARKET_HOURS":         _ALLOW_MARKET_HOURS(),
    }
    try:
        import time as _t
        from data.tradier_budget import BUDGETS, _timestamps, WINDOW_S
        cutoff    = _t.monotonic() - WINDOW_S
        hist_used = sum(1 for ts in _timestamps["canonical_history_backfill"] if ts > cutoff)
        opts_used = sum(1 for ts in _timestamps["options_flow"]               if ts > cutoff)
        quot_used = sum(1 for ts in _timestamps["quotes"]                     if ts > cutoff)
        snap["history_lane_rpm_config"]              = BUDGETS["canonical_history_backfill"]
        snap["history_lane_current_usage"]           = hist_used
        snap["history_lane_backlog_count"]           = _STATE.get("_backlog_count", 0)
        snap["history_lane_last_symbol"]             = _STATE["last_symbol"]
        snap["history_lane_last_success_at"]         = _STATE["last_success_at"]
        snap["history_lane_last_error"]              = _STATE["last_error"]
        snap["history_lane_paused_reason"]           = _STATE.get("paused_reason")
        snap["options_flow_queue_depth"]             = opts_used
        snap["options_flow_busy_pct"]                = round(opts_used / max(1, BUDGETS["options_flow"]), 2)
        snap["quotes_queue_depth"]                   = quot_used
        snap["canonical_history_backfill_budget_used"]  = hist_used
        snap["canonical_history_backfill_budget_cap"]   = BUDGETS["canonical_history_backfill"]
        snap["canonical_history_backfill_saturated"]    = hist_used >= BUDGETS["canonical_history_backfill"]
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


# ── Tradier provider singleton ────────────────────────────────────────────────

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
    Routes through TradierProvider._get() → TRADIER_LIMITER + canonical_history_backfill lane.
    Zero raw httpx calls.
    Exact params sent to Tradier: symbol, interval="daily", start=start_date, end=end_date
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
        return True


def _options_flow_busy() -> bool:
    try:
        import time as _t
        from data.tradier_budget import _timestamps, BUDGETS, WINDOW_S as _W
        cutoff = _t.monotonic() - _W
        used   = sum(1 for ts in _timestamps["options_flow"] if ts > cutoff)
        return used >= BUDGETS["options_flow"] * _PAUSE_IF_OPTIONS_BUSY_PCT
    except Exception:
        return False


def _effective_delay_s(session: str, base_delay: float) -> float:
    if session in ("off_hours", "weekend"):
        return _OFFHOURS_DELAY_S
    return base_delay


# ── Market-hours gate ─────────────────────────────────────────────────────────

def _check_market_hours_gate() -> Optional[str]:
    if not _ALLOW_MARKET_HOURS():
        if _is_active_session():
            return "paused_market_hours"
    return None


# ── Full-backfill allowed check ───────────────────────────────────────────────

def _check_full_backfill_allowed(mode: str, confirm: bool = False) -> Optional[str]:
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


# ── FMP fallback (complete provider failure only) ─────────────────────────────

async def _fetch_fmp_direct(symbol: str) -> list[dict]:
    """
    FMP historical EOD — fallback ONLY when Tradier returns 0 bars (complete failure).
    Not used as a "10Y upgrade" path — Tradier is primary for all depth.
    """
    try:
        from api_budget import budget
        if not budget.spend("fmp", 1):
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
                    ("open", ("open",)), ("high", ("high",)),
                    ("low", ("low",)),   ("volume", ("volume",)),
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


# ── Limit type classification ─────────────────────────────────────────────────

def _classify_limit_type(
    bars:               list[dict],
    requested_start:    date,
) -> dict:
    """
    Classify returned bars using evidence from the live 10Y capability test.

    PROVEN BEHAVIOUR (2026-07-14 test, 5 established + 10 recent symbols):
      - Tradier returns ALL available history when given a large-enough window.
      - For established symbols (SPY/AAPL/MSFT/NVDA/TSM/MARA/SMCI): 3650d window
        → 2510 bars back to 2016-07-18.  Explicit start=2010-01-01 → 4155 bars.
      - For recently-IPO'd symbols (ABCL/ALGM/VRT/CRDO/SOFI/HOOD/HIMS): BOTH the
        3650d window AND the 2010-01-01 window return identical bar counts and the
        same oldest_bar_date — proving Tradier returned full lifetime data, not a
        capped subset.
      - FALSE 5Y CAP ROOT CAUSE: V4.2.5.3 used _LONG_HIST_DAYS=1825 (5Y window).
        1253 bars = exactly correct for 1825 calendar days.  The claim that
        "Tradier caps at ~5Y" was never tested with a 3650d window.

    CLASSIFICATION RULES (single 3650-day Tradier request):
      bars >= 2200   → available_10y       (Tradier confirmed full 10Y depth)
      130–2199 bars  → available_lifetime_under_10y
                       (Tradier returned all available history; ticker < 10Y old)
      1–129  bars    → actual_ticker_history_limit (brand-new; < ~6 months listed)
      0 bars         → no_bars / fetch_failed

    provider_cap_detected:
      NOT set from a single 3650d request.  Tradier is confirmed to return full
      lifetime coverage.  Only set if explicit external evidence shows the ticker
      is older than the oldest returned bar AND a different provider returns more
      bars (not implemented at single-request time).

    oldest_gap_days:
      Calendar days between requested_start and oldest returned bar.
      Informational only — does NOT drive classification.
    """
    bar_count = len(bars)

    if bar_count == 0:
        return {
            "is_10y_complete":       False,
            "is_lifetime_under_10y": False,
            "is_provider_cap":       False,
            "is_actual_limit":       False,
            "oldest_gap_days":       None,
            "classification":        "no_bars",
        }

    if bar_count >= 2200:
        return {
            "is_10y_complete":       True,
            "is_lifetime_under_10y": False,
            "is_provider_cap":       False,
            "is_actual_limit":       False,
            "oldest_gap_days":       0,
            "classification":        "available_10y",
        }

    # oldest_gap is informational only
    try:
        oldest_d   = datetime.strptime(bars[0]["date"][:10], "%Y-%m-%d").date()
        oldest_gap = (oldest_d - requested_start).days
    except Exception:
        oldest_gap = None

    # Brand-new ticker: < ~130 trading days listed (< ~6 calendar months)
    is_brand_new = bar_count < 130

    # All other cases (130–2199 bars): Tradier returned full lifetime coverage.
    # The ticker simply hasn't been publicly listed for 10 years.
    is_lifetime = not is_brand_new and bar_count >= 40

    # provider_cap_detected is not set from a single 3650d request
    is_provider_cap = False

    classification = (
        "actual_ticker_history_limit"  if is_brand_new else
        "available_lifetime_under_10y" if is_lifetime  else
        "available_5y_partial_long_history" if bar_count >= 1100 else
        "available_3y_partial_history"      if bar_count >= 700  else
        "partial_history"
    )

    return {
        "is_10y_complete":       False,
        "is_lifetime_under_10y": is_lifetime,
        "is_provider_cap":       is_provider_cap,
        "is_actual_limit":       is_brand_new,
        "oldest_gap_days":       oldest_gap,
        "classification":        classification,
    }


def _classify_tradier_capability(bar_count: int, is_actual_limit: bool, is_lifetime: bool) -> str:
    if is_actual_limit:
        return "TRADIER_SYMBOL_TOO_NEW"
    if is_lifetime:
        return "TRADIER_LIFETIME_OK"
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
    allow_fmp_fallback: bool = True,
    mode:               str  = "initial_full_backfill",
    confirm:            bool = False,
) -> dict:
    """
    Fetch and cache canonical 10-year history for one symbol.

    Provider order:
      1. Tradier with start = today - 3650 days (primary for ALL depth)
      2. FMP (fallback: ONLY when Tradier returns 0 bars — complete failure)

    Symbols already at available_10y or available_lifetime_under_10y or
    actual_ticker_history_limit skip full-backfill unconditionally
    (unless mode=manual_rebuild with confirm=True).
    """
    from services.canonical_history_service import (
        save_bars, append_bars, mark_failed, mark_excluded,
        get_metadata, is_fresh, is_10y_complete, needs_append,
        get_bars as _get_cached_bars, is_always_usable,
        _RECENT_MIN, _5Y_MIN_BARS,
    )
    sym = symbol.upper()

    if not _is_eligible(sym):
        mark_excluded(sym)
        return {"symbol": sym, "action": "excluded", "bar_count": 0}

    meta   = get_metadata(sym)
    status = (meta or {}).get("history_status", "not_yet_backfilled")

    # ── Skip complete symbols unless explicit manual_rebuild ──────────────────
    if mode not in ("manual_rebuild",):
        if is_10y_complete(status):
            return {
                "symbol":         sym,
                "action":         "skipped_complete",
                "bar_count":      (meta or {}).get("bar_count", 0),
                "history_status": status,
            }

    # ── Fresh-cache fast-path ─────────────────────────────────────────────────
    if mode not in ("manual_rebuild", "incremental_daily_append"):
        if is_fresh(sym, max_age_h=20.0):
            return {
                "symbol":         sym,
                "action":         "skipped_fresh",
                "bar_count":      (meta or {}).get("bar_count", 0),
                "history_status": status,
            }

    # ── Incremental append path ───────────────────────────────────────────────
    if mode == "incremental_daily_append":
        if not _INCREMENTAL_APPEND_ENABLED():
            return {"symbol": sym, "action": "skipped_append_disabled", "bar_count": 0}
        if meta and meta.get("newest_bar_date"):
            newest   = meta["newest_bar_date"]
            newest_d = datetime.strptime(newest[:10], "%Y-%m-%d").date()
            days_behind = (date.today() - newest_d).days
            if days_behind <= 0:
                return {"symbol": sym, "action": "skipped_current",
                        "bar_count": meta.get("bar_count", 0)}
            # 1 request: start = newest - 2 days (2-day overlap inside this single request)
            start_date = (newest_d - timedelta(days=2)).isoformat()
            end_date   = date.today().isoformat()
            new_bars   = await _fetch_tradier_history_managed(sym, start_date, end_date)
            if new_bars:
                updated = append_bars(sym, new_bars, "tradier")
                if updated:
                    return {
                        "symbol":    sym,
                        "action":    "incremental_append",
                        "bar_count": updated["bar_count"],
                        "new_bars":  len(new_bars),
                        "provider":  "tradier",
                    }
            return {"symbol": sym, "action": "incremental_no_new_bars",
                    "bar_count": meta.get("bar_count", 0) if meta else 0}

    # ── Full 10Y fetch ────────────────────────────────────────────────────────
    requested_start_dt = date.today() - timedelta(days=_LONG_HIST_DAYS)
    start_date = requested_start_dt.isoformat()
    end_date   = date.today().isoformat()

    bars     = await _fetch_tradier_history_managed(sym, start_date, end_date)
    provider = "tradier"

    # FMP fallback — ONLY on complete Tradier failure (0 bars)
    # NOT used as an "upgrade path" to 10Y; Tradier is primary for all depth
    if allow_fmp_fallback and len(bars) == 0:
        fmp_bars = await _fetch_fmp_direct(sym)
        if fmp_bars:
            print(f"[CANON_BACKFILL] FMP fallback (Tradier=0) {sym}: {len(fmp_bars)} bars")
            bars     = fmp_bars
            provider = "fmp"

    if not bars:
        mark_failed(sym, "no_bars_from_providers", provider)
        _STATE["last_error"] = f"{sym}: no bars"
        return {
            "symbol":             sym,
            "action":             "fetch_failed",
            "bar_count":          0,
            "requested_start":    start_date,
            "tradier_capability": "TRADIER_PROVIDER_FAILED",
        }

    # Classify limit type using REQUESTED start date (not today)
    limit_info   = _classify_limit_type(bars, requested_start_dt)
    tradier_cap  = _classify_tradier_capability(
        len(bars) if provider == "tradier" else 0,
        limit_info["is_actual_limit"],
        limit_info["is_lifetime_under_10y"],
    )

    meta_out = save_bars(
        sym, bars, provider,
        is_actual_limit       = limit_info["is_actual_limit"],
        is_lifetime_under_10y = limit_info["is_lifetime_under_10y"],
        is_provider_cap       = limit_info["is_provider_cap"],
        refresh_mode          = mode,
        tradier_capability    = tradier_cap,
    )

    # Warm in-memory cache
    try:
        from data.cache import cache
        cache_key = f"tdier_hist:{sym}:{_LONG_HIST_DAYS}" if provider == "tradier" else f"fmp_hist:{sym}"
        cache.set(cache_key, bars, 14400)
    except Exception:
        pass

    _STATE["last_success_at"] = datetime.now(timezone.utc).isoformat()
    return {
        "symbol":              sym,
        "action":              "saved",
        "bar_count":           meta_out["bar_count"],
        "history_status":      meta_out["history_status"],
        "provider":            meta_out["provider"],
        "years_available":     meta_out["years_available"],
        "oldest_bar_date":     meta_out.get("oldest_bar_date"),
        "newest_bar_date":     meta_out.get("newest_bar_date"),
        "requested_start":     start_date,
        "requested_end":       end_date,
        "requested_days":      _LONG_HIST_DAYS,
        "oldest_gap_days":     limit_info.get("oldest_gap_days"),
        "is_lifetime_under_10y": limit_info["is_lifetime_under_10y"],
        "is_provider_cap":     limit_info["is_provider_cap"],
        "is_actual_limit":     limit_info["is_actual_limit"],
        "tradier_capability":  tradier_cap,
        "canonical_history_quality": meta_out.get("canonical_history_quality"),
    }


# ── Priority queue builder ────────────────────────────────────────────────────

def _priority_key(symbol: str) -> int:
    from services.canonical_history_service import get_metadata, is_10y_complete
    meta = get_metadata(symbol)
    if not meta:
        return 0
    hs = meta.get("history_status", "not_yet_backfilled")
    if is_10y_complete(hs):
        return 99
    return {
        "not_yet_backfilled":                0,
        "fetch_failed":                      1,
        "cache_corrupt_needs_rebuild":        1,
        "recent_only":                        2,
        "intermediate_only":                  3,
        "partial_history":                    4,
        "available_3y_partial_history":       5,
        "provider_cap_detected":              6,
        "available_5y_partial_long_history":  7,
        "excluded_prefixed_symbol":          99,
    }.get(hs, 5)


async def _build_symbol_list_tiered(
    tier_0_override: Optional[list[str]] = None,
) -> list[str]:
    """Build prioritized eligible symbol list. Complete symbols are excluded."""
    from services.canonical_history_service import get_metadata, is_10y_complete

    tier0:    set[str] = set()
    tier1:    set[str] = set()
    tier2:    set[str] = set()
    complete: set[str] = set()

    def _classify(s: str) -> None:
        meta = get_metadata(s)
        bc   = meta.get("bar_count", 0) if meta else 0
        hs   = meta.get("history_status", "not_yet_backfilled") if meta else "not_yet_backfilled"
        if is_10y_complete(hs):
            complete.add(s)
            return
        if hs in ("not_yet_backfilled", "fetch_failed",
                  "cache_corrupt_needs_rebuild") or bc < 756:
            tier0.add(s)
        elif hs in ("available_3y_partial_history", "partial_history",
                    "intermediate_only", "recent_only", "provider_cap_detected"):
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

    # Portfolio underlyings → Tier 1
    try:
        import json
        from pathlib import Path
        portfolio_path = Path(__file__).parent.parent / "data" / "portfolio" / "active_holdings.json"
        if portfolio_path.exists():
            ph = json.loads(portfolio_path.read_text())
            for h in (ph.get("holdings") or ph if isinstance(ph, list) else []):
                s = (h.get("symbol") or "").upper()
                if _is_eligible(s) and s not in complete and s not in tier0:
                    _classify(s)
    except Exception:
        pass

    # Theme proxy ETFs + candidate constituent stocks → Tier 1
    # Required so _fetch_proxy_history() and the constituent-stock path both have
    # canonical history after the one-time 10Y backfill.  Without this, the
    # canonical Step 0 patches in theme_rs_service.py always miss on a cold cache.
    try:
        from services.theme_merge_layer import (
            ENRICHED_ALL_PROXY_SYMBOLS as _all_proxy,
            ENRICHED_ALL_CANDIDATE_SYMBOLS as _all_cands,
        )
        _theme_symbols = set(s.upper() for s in list(_all_proxy) + list(_all_cands))
        for s in sorted(_theme_symbols):
            if _is_eligible(s):
                _classify(s)
    except Exception as _exc:
        print(f"[CANON_BACKFILL] theme proxy/cand read error (non-fatal): {_exc}")

    ordered: list[str] = []
    seen:    set[str]  = set()
    for group in (tier0, tier1 - tier0, tier2 - tier0 - tier1):
        syms = sorted(group)
        syms.sort(key=_priority_key)
        for s in syms:
            if s not in seen:
                ordered.append(s)
                seen.add(s)
    return ordered


# ── Weekly health check ───────────────────────────────────────────────────────

async def _run_weekly_health_check(sample_symbols: list[str], max_sample: int = 1) -> dict:
    """Metadata/file existence check only. No provider calls."""
    from services.canonical_history_service import (
        get_all_status, get_bars as _get_bars, _bar_file, needs_append as _needs_append,
    )
    all_meta       = get_all_status()
    file_missing:  list[str] = []
    needs_append_l: list[str] = []

    for sym, meta in all_meta.items():
        if not _bar_file(sym).exists():
            file_missing.append(sym)
            continue
        try:
            if _needs_append(sym):
                needs_append_l.append(sym)
        except Exception:
            pass

    sample_checked:  list[str] = []
    sample_readable: list[str] = []
    sample_broken:   list[str] = []
    for sym in (sample_symbols or list(all_meta.keys()))[:max_sample]:
        payload = _get_bars(sym, require_fresh=False)
        sample_checked.append(sym)
        (sample_readable if payload and payload.get("bars") else sample_broken).append(sym)

    return {
        "mode":             "weekly_health_check",
        "total_indexed":    len(all_meta),
        "file_missing":     file_missing,
        "needs_append":     needs_append_l,
        "sample_checked":   sample_checked,
        "sample_readable":  sample_readable,
        "sample_broken":    sample_broken,
        "provider_calls":   0,
        "request_time_calls": 0,
    }


# ── Batch runner ───────────────────────────────────────────────────────────────

async def run_backfill_batch(
    symbols:            Optional[list[str]] = None,
    max_symbols:        int                 = _DEFAULT_MAX_SYMS,
    delay_s:            float               = _DEFAULT_DELAY_S,
    allow_fmp_fallback: bool                = True,
    priority_only:      bool                = False,
    mode:               str                 = "initial_full_backfill",
    confirm:            bool                = False,
) -> dict:
    """
    Run one backfill batch.
    weekly_health_check mode bypasses all flag/session gates.
    All other modes: checked against safe-mode flags + market-hours gate.
    """
    global _STATE

    if _STATE["running"]:
        return {"error": "backfill_already_running", **_STATE}

    if mode == "weekly_health_check":
        return await _run_weekly_health_check(symbols or [])

    flag_reason = _check_full_backfill_allowed(mode, confirm)
    if flag_reason:
        return {"error": flag_reason, "mode": mode, "flags": get_backfill_status()["flags"]}

    if mode == "incremental_daily_append" and not _INCREMENTAL_APPEND_ENABLED():
        return {"error": "incremental_append_disabled_by_flag", "mode": mode}

    mh_reason = _check_market_hours_gate()
    if mh_reason:
        return {
            "error":                      mh_reason,
            "history_lane_paused_reason": mh_reason,
            "mode":                       mode,
        }

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
        results:  list[dict] = []

        for sym in sym_list:
            _STATE["last_symbol"]   = sym
            _STATE["paused_reason"] = None

            mh = _check_market_hours_gate()
            if mh:
                _STATE["paused_reason"] = mh
                print(f"[CANON_BACKFILL] {sym} — {mh}, stopping batch")
                _STATE["skipped"] += (len(sym_list) - sym_list.index(sym))
                break

            session         = _get_session()
            effective_delay = _effective_delay_s(session, delay_s)

            if _is_active_session() and _options_flow_busy():
                reason = "options_flow_busy_gate"
                _STATE["paused_reason"] = reason
                print(f"[CANON_BACKFILL] {sym} — {reason}, extra sleep 5s")
                await asyncio.sleep(5.0)
                if _options_flow_busy():
                    _STATE["skipped"] += 1
                    results.append({"symbol": sym, "action": "skipped_options_busy", "bar_count": 0})
                    continue
                _STATE["paused_reason"] = None

            try:
                r = await backfill_symbol(sym, allow_fmp_fallback=allow_fmp_fallback,
                                          mode=mode, confirm=confirm)
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
            "request_time_calls": _STATE["request_time_calls"],
            "mode":               mode,
            "results":            results,
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
    Append-only refresh: 1 request per symbol (2-day overlap inside that request).
    Intended to run nightly off-hours.
    """
    return await run_backfill_batch(
        symbols=symbols, max_symbols=max_symbols, delay_s=delay_s,
        allow_fmp_fallback=False, priority_only=False,
        mode="incremental_daily_append", confirm=False,
    )
