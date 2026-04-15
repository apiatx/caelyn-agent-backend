"""
Shared Context Broker — reads cached page-level summaries and builds a
compact, token-disciplined overlay dict for /api/query injection.

Public API:
    read_shared_context()  -> dict       — reads all supported cache sources
    build_context_overlay(category, ctx) -> dict | None  — formats slim overlay

Design rules:
  - All values are ≤ 200 chars so data_compressor.py (MAX_STRING_LENGTH=200)
    passes them through untouched.
  - Total overlay is 4-5 keys at most (~150 tokens).
  - Never raises — all errors are caught and logged as non-fatal.
  - Returns None (not an empty dict) when nothing useful was found.

Cache keys read:
  - notifai_weekly_summary_v2   (TTL: until next Saturday 07:00)
  - sr:dashboard:v1             (TTL: 5 minutes, set by sector rotation service)
  - notifai_the_brief_v1        (TTL: 2 hours)
  - fred:quick_macro            (TTL: set by FRED provider)
  Disk fallback:
  - data/sector_rotation_analysis.json  (TTL: 7 days)
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# ──────────────────────────────────────────────────────────────────────────────
# Categories excluded from overlay injection
# ──────────────────────────────────────────────────────────────────────────────

_EXCLUDED_CATEGORIES: frozenset[str] = frozenset({
    "crypto",
    "portfolio_review",
    "prediction_markets",
    "earnings_catalyst",
    "followup",
    "chat",
})

# Disk path mirrors services/sector_rotation/gemini_analysis._CACHE_PATH
_SR_DISK_CACHE = Path(__file__).parent.parent / "data" / "sector_rotation_analysis.json"
_SR_DISK_TTL = 7 * 24 * 3600  # 7 days


# ──────────────────────────────────────────────────────────────────────────────
# Public: read all cache sources
# ──────────────────────────────────────────────────────────────────────────────

def read_shared_context() -> dict:
    """
    Read all supported cache sources.
    Returns a flat dict of raw extracted values keyed with internal _-prefixed
    names. Only populated keys are returned. Never raises.
    """
    ctx: dict = {}
    try:
        from data.cache import cache

        # ── 1. NotifAI weekly summary ─────────────────────────────────────────
        weekly = cache.get("notifai_weekly_summary_v2")
        if weekly and isinstance(weekly, dict):
            headline = (weekly.get("headline") or "")[:180]
            outlook = (weekly.get("outlook_label") or "")[:80]
            summary = (weekly.get("summary") or "")[:150]
            if headline:
                ctx["_notifai_headline"] = headline
            if outlook:
                ctx["_notifai_outlook"] = outlook
            if summary:
                ctx["_notifai_summary"] = summary

        # ── 2. Sector rotation — memory cache first, disk fallback ────────────
        sr_dash = cache.get("sr:dashboard:v1")
        if sr_dash and isinstance(sr_dash, dict):
            regime = sr_dash.get("regime", {})
            if isinstance(regime, dict):
                posture = (regime.get("market_posture") or "")[:80]
                leadership = (regime.get("leadership_style") or "")[:80]
                if posture:
                    ctx["_sr_posture"] = posture
                if leadership:
                    ctx["_sr_leadership"] = leadership
        else:
            _read_sr_disk_cache(ctx)

        # ── 3. NotifAI brief — today + tomorrow earnings tickers only ─────────
        brief = cache.get("notifai_the_brief_v1")
        if brief and isinstance(brief, dict):
            _extract_upcoming_earnings(brief.get("earnings_by_day", {}), ctx)

        # ── 4. FRED quick macro ───────────────────────────────────────────────
        quick_macro = cache.get("fred:quick_macro")
        if quick_macro and isinstance(quick_macro, dict):
            fed = quick_macro.get("fed_funds_rate", {})
            if isinstance(fed, dict) and "current_rate" in fed:
                ctx["_macro_fed_rate"] = str(fed["current_rate"])

    except Exception as e:
        print(f"[CONTEXT_BROKER] read_shared_context error (non-fatal): {e}")

    return ctx


# ──────────────────────────────────────────────────────────────────────────────
# Public: build the overlay dict
# ──────────────────────────────────────────────────────────────────────────────

def build_context_overlay(category: str, ctx: dict) -> Optional[dict]:
    """
    Format a compact overlay dict ready to store in market_data["_shared_context"].

    Returns None when:
      - category is in _EXCLUDED_CATEGORIES
      - ctx is empty
      - no overlay fields could be formed from ctx

    All string values are capped at 200 chars (data_compressor MAX_STRING_LENGTH).
    Overlay keys are namespaced with the shared_ prefix per spec.
    """
    if category in _EXCLUDED_CATEGORIES or not ctx:
        return None

    overlay: dict = {}

    # ── Weekly market headline + outlook ──────────────────────────────────────
    headline = ctx.get("_notifai_headline", "")
    outlook = ctx.get("_notifai_outlook", "")
    if headline:
        overlay["shared_weekly_headline"] = headline[:180]
    if outlook:
        overlay["shared_market_outlook"] = outlook[:80]

    # ── Sector rotation regime ────────────────────────────────────────────────
    posture = ctx.get("_sr_posture", "")
    leadership = ctx.get("_sr_leadership", "")
    sr_regime = ctx.get("_sr_regime", "")
    if posture and leadership:
        overlay["shared_market_regime"] = f"{posture} | {leadership}"[:160]
    elif posture:
        overlay["shared_market_regime"] = posture[:80]
    elif sr_regime:
        overlay["shared_market_regime"] = sr_regime[:80]

    # ── Upcoming earnings (today + tomorrow) ──────────────────────────────────
    upcoming = ctx.get("_earnings_upcoming", "")
    if upcoming:
        overlay["shared_earnings_today"] = upcoming[:120]

    # ── Macro: Fed rate ───────────────────────────────────────────────────────
    fed_rate = ctx.get("_macro_fed_rate", "")
    if fed_rate:
        overlay["shared_macro_fed_rate"] = f"Fed Funds: {fed_rate}%"[:60]

    return overlay if overlay else None


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────

def _read_sr_disk_cache(ctx: dict) -> None:
    """Disk fallback: read sector rotation analysis.json."""
    try:
        if not _SR_DISK_CACHE.exists():
            return
        raw = json.loads(_SR_DISK_CACHE.read_text())
        saved_at = raw.get("_saved_at", 0)
        if time.time() - saved_at > _SR_DISK_TTL:
            return
        summary = (raw.get("summary") or "")[:150]
        regime = (raw.get("market_regime") or "")[:60]
        if summary:
            ctx["_sr_summary"] = summary
        if regime:
            ctx["_sr_regime"] = regime
    except Exception:
        pass


def _extract_upcoming_earnings(earnings_by_day: dict, ctx: dict) -> None:
    """Collect BMO + AMC tickers for today and tomorrow only (max 10 tickers)."""
    today_str = datetime.now().strftime("%Y-%m-%d")
    tomorrow_str = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    tickers: list[str] = []
    for date_str in (today_str, tomorrow_str):
        day = earnings_by_day.get(date_str, {})
        for bucket in ("bmo", "amc"):
            tickers.extend(
                e.get("ticker", "")
                for e in day.get(bucket, [])[:5]
            )
    tickers = [t for t in tickers if t][:10]
    if tickers:
        ctx["_earnings_upcoming"] = ", ".join(tickers)
