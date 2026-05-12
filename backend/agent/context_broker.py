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

# X consensus weekly snapshot (written by social_x_service)
_X_CONSENSUS_DISK = Path(__file__).parent.parent / "data" / "x_consensus_weekly.json"
_X_CONSENSUS_TTL = 7 * 24 * 3600  # 7 days


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

        # ── 5. Macro regime tag (regime_engine write-through) ─────────────────
        # Written by core/regime_engine.detect_market_regime after any /api/query.
        # Safe to read here: no API call, no LLM call.
        regime_snap = cache.get("regime:current_v1")
        if regime_snap and isinstance(regime_snap, dict):
            tag = (regime_snap.get("regime") or "")[:40]
            if tag:
                ctx["_macro_regime_tag"] = tag

        # ── 6b. X consensus weekly snapshot (disk) ────────────────────────────
        _read_x_consensus_disk(ctx)

        # ── 6. Thematic snapshot — active/emerging themes (if already cached) ─
        # Read thematic_context:snapshot:v1 only if it already exists in cache.
        # We do NOT call get_shared_thematic_context() here to avoid circular
        # dependency; the snapshot is built lazily by /api/thematic-context/* or
        # by the options/screener/strategy-screener overlays.
        theme_snap = cache.get("thematic_context:snapshot:v1")
        if theme_snap and isinstance(theme_snap, dict):
            active   = [t.get("name", "") for t in theme_snap.get("active_themes",   [])[:3] if t.get("name")]
            emerging = [t.get("name", "") for t in theme_snap.get("emerging_themes", [])[:2] if t.get("name")]
            dead     = [t.get("name", "") for t in theme_snap.get("dead_zones",      [])[:2] if t.get("name")]
            if active:
                ctx["_active_themes"]   = ", ".join(active)[:180]
            if emerging:
                ctx["_emerging_themes"] = ", ".join(emerging)[:120]
            if dead:
                ctx["_dead_zone_themes"] = ", ".join(dead)[:120]

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

    # ── Macro regime tag (from regime_engine write-through) ───────────────────
    regime_tag = ctx.get("_macro_regime_tag", "")
    if regime_tag and "shared_market_regime" not in overlay:
        # Only inject if sector rotation posture not already set
        overlay["shared_macro_regime"] = regime_tag[:40]
    elif regime_tag:
        # Append tag to existing regime string (e.g. "Defensive | risk_off")
        existing = overlay.get("shared_market_regime", "")
        if regime_tag not in existing:
            overlay["shared_market_regime"] = f"{existing} [{regime_tag}]"[:190]

    # ── Active themes (top 3 from thematic snapshot, if cached) ──────────────
    # Only included when snapshot has already been built by a prior request.
    # Concise: "AI Networking, Semiconductors, Datacenter" (≤180 chars).
    active_themes = ctx.get("_active_themes", "")
    if active_themes:
        overlay["shared_active_themes"] = f"Active: {active_themes}"[:190]

    # ── Emerging themes (top 2) ───────────────────────────────────────────────
    emerging_themes = ctx.get("_emerging_themes", "")
    if emerging_themes:
        overlay["shared_emerging_themes"] = f"Emerging: {emerging_themes}"[:140]

    # ── X consensus top tickers + market sentiment ─────────────────────────
    x_tickers = ctx.get("_x_consensus_tickers", "")
    x_sentiment = ctx.get("_x_consensus_sentiment", "")
    if x_tickers:
        label = f"X consensus leaders: {x_tickers}"
        if x_sentiment:
            label += f" | {x_sentiment}"
        overlay["shared_x_consensus"] = label[:190]

    return overlay if overlay else None


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────

def _read_x_consensus_disk(ctx: dict) -> None:
    """Read x_consensus_weekly.json and extract top tickers + market sentiment."""
    try:
        if not _X_CONSENSUS_DISK.exists():
            return
        raw = json.loads(_X_CONSENSUS_DISK.read_text())
        saved_at = raw.get("_saved_at") or raw.get("generated_at_ts") or 0
        if isinstance(saved_at, str):
            saved_at = 0  # can't easily parse, skip TTL check
        if saved_at and time.time() - float(saved_at) > _X_CONSENSUS_TTL:
            return
        # Top tickers: may be a list of dicts with "ticker" key, or list of strings
        raw_tickers = raw.get("top_tickers") or raw.get("tickers") or []
        tickers: list[str] = []
        for entry in raw_tickers[:5]:
            if isinstance(entry, str):
                tickers.append(entry)
            elif isinstance(entry, dict):
                sym = entry.get("ticker") or entry.get("symbol") or ""
                if sym:
                    tickers.append(str(sym))
        if tickers:
            ctx["_x_consensus_tickers"] = ", ".join(tickers)
        # Market sentiment
        sentiment = (
            raw.get("market_sentiment")
            or raw.get("overall_sentiment")
            or raw.get("market_mood")
            or ""
        )
        if sentiment and isinstance(sentiment, str):
            ctx["_x_consensus_sentiment"] = sentiment[:60]
    except Exception:
        pass


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
