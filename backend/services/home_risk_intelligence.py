"""
Home Risk Intelligence — composer/aggregator for GET /api/home/risk-intelligence.

Design contract:
  - ZERO new upstream API calls. All values read from existing cached service outputs.
  - Sources reused (each retains its own upstream TTL):
      macro:dashboard:v3          MacroProvider.get_dashboard()   15-min TTL
      strategy:vix_regime:v1      build_vix_regime_payload()      15-min TTL  ← same engine as
                                                                                  Macro "Should I Be Trading?"
      Neon calendar_snapshots     get_snapshot("economic_releases") weekly      ← same source as
                                                                                  Calendar page
      HL in-memory state          get_state_optional()            in-process   ← BTC price/change
      SR dashboard cache          sector_rotation.get_dashboard() 5-min TTL   ← breadth score

  - Composer cache: home:risk_intel:v1 (60 s TTL), LKG: home:risk_intel:v1:lkg (4 h)
  - Canonical scoring is delegated to swing_regime_service (pure, testable).
  - trade_decision is a projection from swing_regime — no independent VIX mapping.
  - risk_cluster.severity/score/active MUST match swing_regime.risk_level/risk_score/active.
"""
from __future__ import annotations

import asyncio
import math
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from data.cache import cache

_RISK_INTEL_KEY     = "home:risk_intel:v1"
_RISK_INTEL_LKG_KEY = "home:risk_intel:v1:lkg"
_RISK_INTEL_TTL     = 60          # 1 min — upstream caches do the heavy lifting
_RISK_INTEL_LKG_TTL = 4 * 3600   # 4 h  — survives cold restarts


# ─────────────────────────────────────────────────────────────────────────────
# Primitive helpers
# ─────────────────────────────────────────────────────────────────────────────

def _r(v: Any, n: int = 2) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else round(f, n)
    except Exception:
        return None


def _safe(v: Any, default: Any) -> Any:
    return default if isinstance(v, Exception) or v is None else v


def _ts_age(ts_str: str | None, now_utc: datetime) -> int | None:
    if not ts_str:
        return None
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return max(0, int((now_utc - dt).total_seconds()))
    except Exception:
        return None


def _freshness_status(age_s: int | None, stale_threshold_s: int) -> str:
    if age_s is None:
        return "unknown"
    if age_s <= stale_threshold_s // 2:
        return "live"
    if age_s <= stale_threshold_s:
        return "cached"
    return "stale"


# ─────────────────────────────────────────────────────────────────────────────
# BTC from in-memory Hyperliquid state (zero API call)
# ─────────────────────────────────────────────────────────────────────────────

def _get_btc_from_hl() -> dict | None:
    try:
        from services.hyperliquid.router import get_state_optional
        state = get_state_optional()
        if state is None:
            return None
        btc = next((a for a in (state.perps or []) if a.coin == "BTC"), None)
        if btc is None:
            return None
        return {
            "price":      _r(btc.mark_px, 2),
            "change_pct": _r(btc.pct_change_24h, 2),
        }
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Breadth score from sector rotation dashboard (existing 5-min cache)
# ─────────────────────────────────────────────────────────────────────────────

async def _get_sector_data() -> dict:
    """
    Extract sector breadth (1D, 7D) and regime posture from the SR dashboard cache.
    Reuses SR dashboard cache — no new FMP call.

    Returns dict with keys:
      sector_breadth_1d   — 0-100, % sectors with positive 1-day return
      sector_breadth_7d   — 0-100, % sectors with positive 7-day return (None if unavailable)
      cyclical_vs_defensive_spread — float or None
      market_posture       — "Risk-On" | "Risk-Off" | "Neutral"
      sector_count         — number of sectors in denominator
    """
    try:
        from services.sector_rotation.service import get_dashboard as _sr_get
        sr = await _sr_get()
        if sr is None:
            return {"sector_breadth_1d": None}
        d = sr.model_dump() if hasattr(sr, "model_dump") else (sr if isinstance(sr, dict) else {})
        sectors = [s for s in (d.get("sectors") or []) if isinstance(s, dict)]
        if not sectors:
            return {"sector_breadth_1d": None}

        pos_1d = sum(1 for s in sectors if (s.get("change_1d") or 0) > 0)
        breadth_1d = round(100.0 * pos_1d / len(sectors))

        c7d_vals = [s.get("change_7d") for s in sectors if s.get("change_7d") is not None]
        breadth_7d = round(100.0 * sum(1 for v in c7d_vals if v > 0) / len(c7d_vals)) if c7d_vals else None

        regime = d.get("regime") or {}
        cyc_vs_def = regime.get("cyclical_vs_defensive")
        posture = regime.get("market_posture") or ""

        return {
            "sector_breadth_1d":             breadth_1d,
            "sector_breadth_7d":             breadth_7d,
            "cyclical_vs_defensive_spread":  _r(cyc_vs_def, 2) if cyc_vs_def is not None else None,
            "market_posture":                posture,
            "sector_count":                  len(sectors),
        }
    except Exception:
        return {"sector_breadth_1d": None}


# ─────────────────────────────────────────────────────────────────────────────
# Economic events — normalize and filter upcoming
# ─────────────────────────────────────────────────────────────────────────────

def _filter_upcoming_events(snapshot: dict, days_ahead: int = 7) -> list[dict]:
    """
    From the calendar_snapshots economic_releases envelope, return events
    within `days_ahead` calendar days from today, normalized to spec shape.
    Never calls FMP. Uses the same Neon snapshot as Calendar → Economic Releases.
    """
    today   = datetime.now(timezone.utc).date()
    cutoff  = today + timedelta(days=days_ahead)
    out: list[dict] = []

    for ev in (snapshot.get("current_week") or []):
        if not isinstance(ev, dict):
            continue
        date_str = ev.get("date") or ev.get("event_date") or ""
        try:
            ev_date = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
        except Exception:
            continue
        if ev_date < today or ev_date > cutoff:
            continue
        out.append({
            "date":       date_str[:10],
            "time":       ev.get("time"),
            "title":      ev.get("title") or ev.get("event") or ev.get("name") or "",
            "importance": ev.get("importance") or ev.get("impact") or "medium",
            "actual":     ev.get("actual"),
            "estimate":   ev.get("estimate") or ev.get("estimated"),
            "previous":   ev.get("previous") or ev.get("prev"),
            "country":    ev.get("country") or "US",
            "source":     "calendar_reused",
        })

    out.sort(key=lambda e: e["date"])
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Trade decision — mapped from the existing VIX regime signal
#   MUST match the Macro → "Should I Be Trading?" tab.
# ─────────────────────────────────────────────────────────────────────────────

def _project_trade_decision_from_swing_regime(swing_regime: dict, vix_payload: dict) -> dict:
    """
    Project trade_decision from the canonical swing_regime result.
    No longer maps from VIX alone — this eliminates the old contradiction.

    Mapping:
      swing_regime.trade_bias:
        LONG             → label = "YES"
        SELECTIVE_LONG   → label = "CAUTION"
        NEUTRAL          → label = "CAUTION"
        SELECTIVE_SHORT  → label = "CAUTION"
        SHORT_HEDGE      → label = "NO"

      score: 100 - swing_regime.risk_score  (inverse risk → tradeability)
      mode: always "swing" for this endpoint
      position_size_hint: from swing_regime
      one_line: from swing_regime
      avoid: deterministic from dominant driver and risk level
      vix_zone / risk_regime / signal_summary: passthrough from vix_payload (preserved for compat)
    """
    trade_bias = swing_regime.get("trade_bias", "NEUTRAL")

    bias_label_map = {
        "LONG":             "YES",
        "SELECTIVE_LONG":   "CAUTION",
        "NEUTRAL":          "CAUTION",
        "SELECTIVE_SHORT":  "CAUTION",
        "SHORT_HEDGE":      "NO",
    }
    label = bias_label_map.get(trade_bias, "CAUTION")
    score = max(0, min(100, 100 - (swing_regime.get("risk_score", 50))))
    mode  = "swing"
    pos_hint = swing_regime.get("position_size_hint", "selective")
    one_line = swing_regime.get("one_line", "")

    risk_level = swing_regime.get("risk_level", "MODERATE")
    driver = swing_regime.get("dominant_driver", "")
    avoid: list[str] = []

    if risk_level in ("EXTREME",):
        avoid = ["high-beta", "small-cap speculative", "leveraged positions", "all new entries"]
    elif risk_level == "HIGH":
        avoid = ["high-beta", "small-cap speculative", "leveraged positions"]
    elif risk_level == "ELEVATED":
        avoid = ["low-liquidity names"]
    elif driver == "rate_and_dollar_pressure":
        avoid = ["rate-sensitive growth names"]

    sig = (vix_payload.get("vix_regime_signal") or {})
    zone    = vix_payload.get("vix_zone")   or sig.get("current_zone")   or "unknown"
    warning = vix_payload.get("risk_regime") or sig.get("warning_level") or "unknown"

    return {
        "label":              label,
        "score":              score,
        "mode":               mode,
        "position_size_hint": pos_hint,
        "one_line":           one_line,
        "avoid":              avoid,
        "vix_zone":           zone,
        "risk_regime":        warning,
        "signal_summary":     sig.get("signal_summary") or "",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Risk cluster — deterministic trigger rules from existing values
# ─────────────────────────────────────────────────────────────────────────────

def _assess_risk_cluster(
    *,
    vix: float | None,
    vix_change_pct: float | None,
    spy_change_pct: float | None,
    qqq_change_pct: float | None,
    btc_change_pct: float | None,
    breadth_score: float | None,
    us10y: float | None,
    dxy: float | None,
    dxy_change_pct: float | None,
    hyg_change_pct: float | None,
    has_upcoming_high_impact_event: bool,
) -> dict:
    triggers: list[dict] = []

    # ── VIX ──────────────────────────────────────────────────────────────────
    vix_spike = (vix_change_pct is not None and vix_change_pct >= 20)
    vix_high  = (vix is not None and vix >= 25)
    vix_elev  = (vix is not None and vix >= 20)

    if vix is not None or vix_change_pct is not None:
        if vix_spike or vix_high:
            status = "red"
            val_str = f"{vix:.2f}" if vix else "—"
            chg_str = f" (+{vix_change_pct:.1f}%)" if vix_change_pct is not None else ""
            msg = f"VIX {val_str}{chg_str} — fear/volatility spike"
        elif vix_elev:
            status = "yellow"
            msg = f"VIX {vix:.1f} — elevated above 20 threshold"
        else:
            status = "green"
            msg = f"VIX {vix:.1f} — calm zone" if vix else "VIX: data unavailable"
        triggers.append({
            "key":       "vix_spike",
            "label":     "VIX",
            "status":    status,
            "value":     f"{vix:.2f}" if vix else None,
            "threshold": "≥25 or spike ≥+20% = red | ≥20 = yellow",
            "message":   msg,
        })

    # ── Nasdaq 100 / QQQ ──────────────────────────────────────────────────────
    if qqq_change_pct is not None:
        if qqq_change_pct <= -2.5:
            status, msg = "red",    f"QQQ {qqq_change_pct:+.1f}% — significant tech selling"
        elif qqq_change_pct <= -1.0:
            status, msg = "yellow", f"QQQ {qqq_change_pct:+.1f}% — weakness"
        elif qqq_change_pct >= 1.5:
            status, msg = "green",  f"QQQ {qqq_change_pct:+.1f}% — risk-on momentum"
        else:
            status, msg = "green",  f"QQQ {qqq_change_pct:+.1f}%"
        triggers.append({
            "key":       "nasdaq_selloff",
            "label":     "Nasdaq 100 (QQQ)",
            "status":    status,
            "value":     f"{qqq_change_pct:+.2f}%",
            "threshold": "≤-2.5% = red | ≤-1% = yellow",
            "message":   msg,
        })

    # ── S&P 500 / SPY ─────────────────────────────────────────────────────────
    if spy_change_pct is not None:
        if spy_change_pct <= -1.5:
            status, msg = "red",    f"S&P 500 {spy_change_pct:+.1f}% — broad market selling"
        elif spy_change_pct <= -0.5:
            status, msg = "yellow", f"S&P 500 {spy_change_pct:+.1f}%"
        elif spy_change_pct >= 1.0:
            status, msg = "green",  f"S&P 500 {spy_change_pct:+.1f}% — broad market strength"
        else:
            status, msg = "green",  f"S&P 500 {spy_change_pct:+.1f}%"
        triggers.append({
            "key":       "sp500_selloff",
            "label":     "S&P 500 (SPY)",
            "status":    status,
            "value":     f"{spy_change_pct:+.2f}%",
            "threshold": "≤-1.5% = red | ≤-0.5% = yellow",
            "message":   msg,
        })

    # ── Bitcoin risk-off ──────────────────────────────────────────────────────
    if btc_change_pct is not None:
        if btc_change_pct <= -5.0:
            status, msg = "red",    f"BTC {btc_change_pct:+.1f}% — risk-off, heavy selling"
        elif btc_change_pct <= -2.5:
            status, msg = "orange", f"BTC {btc_change_pct:+.1f}% — caution, weakening"
        elif btc_change_pct >= 3.0:
            status, msg = "green",  f"BTC {btc_change_pct:+.1f}% — risk-on appetite"
        else:
            status, msg = "green",  f"BTC {btc_change_pct:+.1f}%"
        triggers.append({
            "key":       "btc_risk_off",
            "label":     "Bitcoin",
            "status":    status,
            "value":     f"{btc_change_pct:+.2f}%",
            "threshold": "≤-5% = red | ≤-2.5% = orange",
            "message":   msg,
        })

    # ── Market breadth ────────────────────────────────────────────────────────
    if breadth_score is not None:
        if breadth_score < 40:
            status, msg = "red",    f"Breadth {breadth_score:.0f}/100 — majority of sectors declining"
        elif breadth_score < 50:
            status, msg = "yellow", f"Breadth {breadth_score:.0f}/100 — mixed/negative"
        elif breadth_score >= 70:
            status, msg = "green",  f"Breadth {breadth_score:.0f}/100 — broad participation"
        else:
            status, msg = "green",  f"Breadth {breadth_score:.0f}/100 — neutral"
        triggers.append({
            "key":       "market_breadth",
            "label":     "Market breadth",
            "status":    status,
            "value":     f"{breadth_score:.0f}/100",
            "threshold": "<40 = red | <50 = yellow",
            "message":   msg,
        })

    # ── 10Y yield pressure ────────────────────────────────────────────────────
    if us10y is not None:
        if us10y >= 4.75:
            status, msg = "red",    f"10Y yield {us10y:.2f}% — elevated rate pressure on equities"
        elif us10y >= 4.5:
            status, msg = "yellow", f"10Y yield {us10y:.2f}% — rate headwind watch zone"
        else:
            status, msg = "green",  f"10Y yield {us10y:.2f}% — below key 4.5% threshold"
        triggers.append({
            "key":       "ten_y_yield",
            "label":     "10Y Treasury yield",
            "status":    status,
            "value":     f"{us10y:.3f}%",
            "threshold": "≥4.75% = red | ≥4.5% = yellow",
            "message":   msg,
        })

    # ── DXY headwind ──────────────────────────────────────────────────────────
    if dxy is not None and dxy_change_pct is not None:
        if dxy_change_pct >= 0.5:
            status, msg = "orange", f"DXY +{dxy_change_pct:.2f}% — dollar strength headwind for risk assets"
        elif dxy_change_pct >= 0.2:
            status, msg = "yellow", f"DXY +{dxy_change_pct:.2f}% — mild dollar strength"
        elif dxy_change_pct <= -0.3:
            status, msg = "green",  f"DXY {dxy_change_pct:.2f}% — dollar weakness, risk-on tailwind"
        else:
            status, msg = "green",  f"DXY {dxy_change_pct:+.2f}%"
        triggers.append({
            "key":       "dxy_headwind",
            "label":     "US Dollar (DXY)",
            "status":    status,
            "value":     f"{dxy:.2f} ({dxy_change_pct:+.2f}%)",
            "threshold": "≥+0.5%/day = orange | ≥+0.2% = yellow",
            "message":   msg,
        })

    # ── Credit stress (HYG proxy) ─────────────────────────────────────────────
    if hyg_change_pct is not None:
        if hyg_change_pct <= -1.5:
            status, msg = "red",    f"HYG {hyg_change_pct:+.1f}% — high-yield stress, credit spreads widening"
        elif hyg_change_pct <= -0.5:
            status, msg = "yellow", f"HYG {hyg_change_pct:+.1f}% — watch credit conditions"
        else:
            status, msg = "green",  f"HYG {hyg_change_pct:+.1f}% — credit calm"
        triggers.append({
            "key":       "credit_stress",
            "label":     "Credit stress (HYG)",
            "status":    status,
            "value":     f"{hyg_change_pct:+.2f}%",
            "threshold": "≤-1.5% = red | ≤-0.5% = yellow",
            "message":   msg,
        })

    # ── Upcoming high-impact macro event ──────────────────────────────────────
    if has_upcoming_high_impact_event:
        triggers.append({
            "key":       "macro_event_risk",
            "label":     "Upcoming high-impact event",
            "status":    "orange",
            "value":     "within 3 trading days",
            "threshold": "high-importance economic release",
            "message":   "High-impact macro event within 3 trading days — elevated volatility likely",
        })

    # ── Severity score ────────────────────────────────────────────────────────
    hot_count = sum(1 for t in triggers if t["status"] in ("red", "orange"))

    if hot_count >= 4:
        severity = "EXTREME"
    elif hot_count == 3:
        severity = "HIGH"
    elif hot_count == 2:
        severity = "ELEVATED"
    elif hot_count == 1:
        severity = "MODERATE"
    else:
        severity = "LOW"

    active = hot_count >= 2

    n_total = max(len(triggers), 1)
    score   = round(min(100, (hot_count / n_total) * 60 + hot_count * 8))

    hot_triggers = [t for t in triggers if t["status"] in ("red", "orange")]
    if not hot_triggers:
        headline = "Markets appear orderly — no major risk signals active"
        summary  = "All monitored risk indicators are within normal parameters."
    else:
        verb     = "active" if active else "flagged"
        headline = f"{hot_count} risk signal{'s' if hot_count > 1 else ''} {verb} — consider defensive positioning" \
                   if active else f"{hot_count} risk signal flagged — monitor closely"
        summary  = " | ".join(t["message"] for t in hot_triggers[:3])

    return {
        "active":        active,
        "severity":      severity,
        "score":         score,
        "headline":      headline,
        "summary":       summary,
        "trigger_count": hot_count,
        "triggers":      triggers,
    }


# ─────────────────────────────────────────────────────────────────────────────
# "Why market is moving" — deterministic short bullets
# ─────────────────────────────────────────────────────────────────────────────

def _build_why_bullets(
    *,
    vix: float | None,
    vix_change_pct: float | None,
    spy_change_pct: float | None,
    qqq_change_pct: float | None,
    btc_change_pct: float | None,
    us10y: float | None,
    dxy_change_pct: float | None,
    vix_signal_title: str | None,
) -> list[str]:
    bullets: list[str] = []

    # VIX spike
    if vix is not None and vix_change_pct is not None and vix_change_pct >= 10:
        bullets.append(f"VIX spiked +{vix_change_pct:.1f}% to {vix:.1f} — heightened demand for protection")
    elif vix is not None and vix >= 20:
        bullets.append(f"VIX at {vix:.1f} — elevated fear/uncertainty in options market")

    # Equities
    if qqq_change_pct is not None and abs(qqq_change_pct) >= 1.0:
        dir_ = "selling off" if qqq_change_pct < 0 else "rallying"
        bullets.append(f"Tech/Nasdaq (QQQ) {dir_}: {qqq_change_pct:+.1f}%")
    elif spy_change_pct is not None and abs(spy_change_pct) >= 0.8:
        dir_ = "selling off" if spy_change_pct < 0 else "rallying"
        bullets.append(f"S&P 500 {dir_}: {spy_change_pct:+.1f}%")

    # BTC
    if btc_change_pct is not None and abs(btc_change_pct) >= 2.5:
        dir_ = "risk-off rotation" if btc_change_pct < 0 else "risk-on buying"
        bullets.append(f"Bitcoin {btc_change_pct:+.1f}% — {dir_}")

    # Rates
    if us10y is not None and us10y >= 4.5:
        bullets.append(f"10Y yield {us10y:.2f}% — persistent rate pressure on growth valuations")

    # Dollar
    if dxy_change_pct is not None and dxy_change_pct >= 0.3:
        bullets.append(f"Dollar (DXY) +{dxy_change_pct:.2f}% — USD strength weighing on risk assets")

    # Fallback: use VIX regime title from existing engine
    if not bullets and vix_signal_title:
        bullets.append(vix_signal_title)

    # Default if truly nothing fired (pre-market / weekend / no data)
    if not bullets:
        parts = []
        if spy_change_pct is not None:
            parts.append(f"SPY {spy_change_pct:+.1f}%")
        if qqq_change_pct is not None:
            parts.append(f"QQQ {qqq_change_pct:+.1f}%")
        if vix is not None:
            parts.append(f"VIX {vix:.1f}")
        bullets.append("Markets trading within normal range" + (": " + ", ".join(parts) if parts else ""))

    return bullets[:5]


# ─────────────────────────────────────────────────────────────────────────────
# Market open check
# ─────────────────────────────────────────────────────────────────────────────

def _is_us_market_open() -> bool:
    try:
        import zoneinfo
        now_et = datetime.now(zoneinfo.ZoneInfo("America/New_York"))
    except Exception:
        now_et = datetime.now(timezone.utc)
    mins = now_et.hour * 60 + now_et.minute
    return now_et.weekday() < 5 and (9 * 60 + 30) <= mins < (16 * 60)


# ─────────────────────────────────────────────────────────────────────────────
# Main builder
# ─────────────────────────────────────────────────────────────────────────────

async def build_home_risk_intelligence(macro_provider) -> dict:
    """
    Build (or return cached) Home Risk Intelligence payload.

    All data is read from existing cached services. No new FMP/upstream API
    calls are made. The macro_provider argument is the same singleton already
    used by /api/home/dashboard and /api/macro/dashboard.
    """
    t0      = time.monotonic()
    now_utc = datetime.now(timezone.utc)

    # ── 1. Fast cache path ────────────────────────────────────────────────────
    cached = cache.get(_RISK_INTEL_KEY)
    if cached is not None:
        return cached

    # ── 2. Gather from existing caches concurrently ───────────────────────────
    #    Each of these reads its own already-populated cache key (no re-fetch).
    from services.calendar_snapshot_service import get_snapshot
    from services.strategy_macro_service import build_vix_regime_payload

    macro_task   = macro_provider.get_dashboard() if macro_provider else asyncio.sleep(0)
    regime_task  = build_vix_regime_payload(macro_provider) if macro_provider else asyncio.sleep(0)
    cal_task     = asyncio.to_thread(get_snapshot, "economic_releases")
    sector_task  = _get_sector_data()

    macro_raw, vix_payload, econ_snap, sector_data = await asyncio.gather(
        macro_task, regime_task, cal_task, sector_task,
        return_exceptions=True,
    )

    macro_raw   = _safe(macro_raw, {})
    vix_payload = _safe(vix_payload, {})
    econ_snap   = _safe(econ_snap, {})
    sector_data = _safe(sector_data, {})
    if isinstance(sector_data, Exception):
        sector_data = {}
    breadth_score = sector_data.get("sector_breadth_1d")

    print(
        "[RISK_INTEL] sources: macro:dashboard:v3 | strategy:vix_regime:v1 | "
        "calendar_snapshots/economic_releases (Neon) | HL in-memory state | "
        "SR dashboard cache — NO new FMP calls"
    )

    # ── 3. Extract values from macro dashboard ────────────────────────────────
    try:
        from data.macro_transforms import transform_dashboard
        tx = transform_dashboard(macro_raw or {})
    except Exception:
        tx = macro_raw or {}

    bench  = {e.get("ticker"): e for e in (tx.get("benchmark_etfs") or [])}
    vix_d  = tx.get("vix") or {}
    rates  = tx.get("rates_and_yields") or {}
    dollar = tx.get("dollar") or {}

    spy = bench.get("SPY") or {}
    qqq = bench.get("QQQ") or {}
    dia = bench.get("DIA") or {}
    hyg = bench.get("HYG") or {}

    vix         = _r(vix_d.get("current"))
    vix_chg     = _r(vix_d.get("change_pct"))
    spy_price   = _r(spy.get("price"))
    spy_chg     = _r(spy.get("change_pct"))
    qqq_price   = _r(qqq.get("price"))
    qqq_chg     = _r(qqq.get("change_pct"))
    dia_price   = _r(dia.get("price"))
    dia_chg     = _r(dia.get("change_pct"))
    us10y       = _r(rates.get("us_10y"), 3)
    us10y_chg_pp = _r(rates.get("us_10y_chg_1d"), 4)   # percentage-points; ×100 = bps
    dxy         = _r(dollar.get("dxy"), 3)
    # Support both field names that appear in different macro providers
    dxy_chg_pct = _r(dollar.get("dxy_change_pct") or dollar.get("dxy_chg_pct") or dollar.get("dxy_chg_1d"), 3)
    hyg_chg     = _r(hyg.get("change_pct"))

    # BTC — in-memory HL state (synchronous, no I/O)
    btc_data  = _get_btc_from_hl()
    btc_price = (btc_data or {}).get("price")
    btc_chg   = (btc_data or {}).get("change_pct")

    # ── 4. Market snapshot (spec-aligned shape) ───────────────────────────────
    # NOTE: us_10y_chg_1d is not populated by MacroProvider.get_dashboard().
    # change_bps remains null until the upstream source provides a 1D change.
    us10y_chg_bps = _r((us10y_chg_pp or 0) * 100, 1) if us10y_chg_pp is not None else None

    market_snapshot = {
        "sp500":     {"symbol": "SPY",   "price": spy_price, "change_pct": spy_chg},
        "dow":       {"symbol": "DIA",   "price": dia_price, "change_pct": dia_chg},
        "nasdaq100": {"symbol": "QQQ",   "price": qqq_price, "change_pct": qqq_chg},
        "bitcoin":   {"symbol": "BTC",   "price": btc_price, "change_pct": btc_chg},
        "us10y": {
            "symbol":     "US10Y",
            "price":      us10y,
            "change_bps": us10y_chg_bps,
        },
        "vix":  {"symbol": "VIX", "price": vix,  "change_pct": vix_chg},
        "dxy":  {"symbol": "DXY", "price": dxy,  "change_pct": dxy_chg_pct},
    }

    # ── 5. Extract multi-TF data from existing cached service outputs ──────────
    # SPX returns from vix_payload historical_windows
    hist_windows = (vix_payload.get("historical_windows") or {})
    spx_7d  = (hist_windows.get("7d") or {}).get("spx_return_pct")
    spx_63d = (hist_windows.get("quarter") or {}).get("spx_return_pct")
    vix_7d  = (hist_windows.get("7d") or {}).get("vix_min")  # not return, just snapshot

    # ── 6. Economic events ────────────────────────────────────────────────────
    upcoming_events = _filter_upcoming_events(econ_snap, days_ahead=7)

    # High-impact event within ~3 trading days (≈ 5 calendar days)
    three_td_cutoff = (now_utc.date() + timedelta(days=5)).isoformat()
    has_hi_impact = any(
        ev.get("importance") in ("high", "critical", "HIGH", "CRITICAL")
        and (ev.get("date") or "9999") <= three_td_cutoff
        for ev in upcoming_events
    )
    next_hi_event = None
    next_hi_days = None
    if has_hi_impact:
        for ev in sorted(upcoming_events, key=lambda e: e.get("date", "9999")):
            if ev.get("importance") in ("high", "critical", "HIGH", "CRITICAL"):
                next_hi_event = ev.get("title") or ""
                try:
                    ed = datetime.strptime((ev.get("date") or "")[:10], "%Y-%m-%d").date()
                    next_hi_days = (ed - now_utc.date()).days
                except Exception:
                    pass
                break

    # ── 7. Build normalized inputs for canonical swing-regime engine ───────────
    swing_inputs = {
        "spy_change_1d":                  spy_chg,
        "qqq_change_1d":                  qqq_chg,
        "sector_breadth_1d":              sector_data.get("sector_breadth_1d"),
        "sector_breadth_7d":              sector_data.get("sector_breadth_7d"),
        "spx_return_7d":                  spx_7d,
        "spx_return_63d":                 spx_63d,
        "vix_current":                    vix,
        "vix_change_1d":                  vix_chg,
        "vix_return_7d":                  vix_7d,
        "hyg_change_1d":                  hyg_chg,
        "us10y_yield":                    us10y,
        "dxy_price":                      dxy,
        "dxy_change_1d":                  dxy_chg_pct,
        "btc_change_24h":                 btc_chg,
        "cyclical_vs_defensive_spread":   sector_data.get("cyclical_vs_defensive_spread"),
        "market_posture":                 sector_data.get("market_posture"),
        "has_upcoming_high_impact_event": has_hi_impact,
        "days_until_next_event":          next_hi_days,
        "next_event_title":               next_hi_event,
    }

    from services.swing_regime_service import assess_swing_regime
    swing_regime = assess_swing_regime(swing_inputs)

    # ── 8. Project trade_decision from swing_regime ────────────────────────────
    trade_decision = _project_trade_decision_from_swing_regime(swing_regime, vix_payload)

    # ── 9. Risk cluster — preserved triggers + swing-regime-aligned severity ───
    breadth_float = float(breadth_score) if isinstance(breadth_score, (int, float)) else None

    risk_cluster = _assess_risk_cluster(
        vix=vix,
        vix_change_pct=vix_chg,
        spy_change_pct=spy_chg,
        qqq_change_pct=qqq_chg,
        btc_change_pct=btc_chg,
        breadth_score=breadth_float,
        us10y=us10y,
        dxy=dxy,
        dxy_change_pct=dxy_chg_pct,
        hyg_change_pct=hyg_chg,
        has_upcoming_high_impact_event=has_hi_impact,
    )

    # Override severity / score / active from canonical swing_regime
    risk_cluster["severity"] = swing_regime["risk_level"]
    risk_cluster["score"]    = swing_regime["risk_score"]
    risk_cluster["active"]   = swing_regime["risk_level"] in ("ELEVATED", "HIGH", "EXTREME")
    # Preserve trigger_count, triggers, headline, summary from the legacy assessment
    # so the detailed trigger table remains available for inspection.

    # ── 10. Why bullets ───────────────────────────────────────────────────────
    vix_sig_title = (vix_payload.get("vix_regime_signal") or {}).get("signal_title")
    why_bullets = _build_why_bullets(
        vix=vix,
        vix_change_pct=vix_chg,
        spy_change_pct=spy_chg,
        qqq_change_pct=qqq_chg,
        btc_change_pct=btc_chg,
        us10y=us10y,
        dxy_change_pct=dxy_chg_pct,
        vix_signal_title=vix_sig_title,
    )

    # ── 11. Freshness metadata (no new fetches) ────────────────────────────────
    macro_gen_at = vix_payload.get("generated_at")          # ISO string from vix_regime payload
    cal_updated  = econ_snap.get("last_updated")             # ISO string from calendar snapshot

    macro_age = _ts_age(macro_gen_at, now_utc)
    cal_age   = _ts_age(cal_updated,  now_utc)

    data_freshness = {
        "market_snapshot_age_seconds": macro_age,
        "calendar_age_seconds":        cal_age,
        "macro_age_seconds":           macro_age,
        "source_summary":              "reused existing cached Home/Macro/Calendar data",
        "market_snapshot_status":      _freshness_status(macro_age, 900),
        "calendar_status":             _freshness_status(cal_age,   86400 * 7),
        "macro_status":                _freshness_status(macro_age, 900),
        "diagnostic_sources": [
            "macro:dashboard:v3 (MacroProvider.get_dashboard, 15-min TTL) — SPY/QQQ/DIA/VIX/10Y/DXY/HYG",
            "strategy:vix_regime:v1 (build_vix_regime_payload, 15-min TTL) — SAME engine as Macro 'Should I Be Trading?'",
            "Neon calendar_snapshots/economic_releases (weekly refresh) — SAME source as Calendar page",
            "Hyperliquid in-memory state (get_state_optional, no TTL) — BTC price and 24h change",
            "sector_rotation.get_dashboard (SR dashboard cache, 5-min TTL) — sector breadth + multi-TF regime",
            "NO new FMP/upstream API calls made for Home Risk Intelligence",
        ],
    }

    # ── 12. Assemble result ────────────────────────────────────────────────────
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    print(
        f"[RISK_INTEL] built in {elapsed_ms}ms — "
        f"swing_level={swing_regime['risk_level']} swing_score={swing_regime['risk_score']} "
        f"bias={swing_regime['trade_bias']} events={len(upcoming_events)} "
        f"breadth={breadth_float} sr_sectors={sector_data.get('sector_count')}"
    )

    result = {
        "as_of":                    now_utc.isoformat(),
        "market_open":              _is_us_market_open(),
        "data_freshness":           data_freshness,
        "market_snapshot":          market_snapshot,
        "trade_decision":           trade_decision,
        "risk_cluster":             risk_cluster,
        "swing_regime":             swing_regime,
        "upcoming_economic_events": upcoming_events,
        "why_market_is_moving":     why_bullets,
    }

    cache.set(_RISK_INTEL_KEY,     result, _RISK_INTEL_TTL)
    cache.set(_RISK_INTEL_LKG_KEY, result, _RISK_INTEL_LKG_TTL)
    return result


async def build_home_risk_intelligence_safe(macro_provider) -> dict:
    """
    LKG-fallback wrapper. Returns last-good-known payload on any error.
    Suitable for the FastAPI route.
    """
    try:
        return await build_home_risk_intelligence(macro_provider)
    except Exception as exc:
        print(f"[RISK_INTEL] build error (trying LKG): {exc}")
        lkg = cache.get(_RISK_INTEL_LKG_KEY)
        if lkg is not None:
            # Return a shallow copy with the fallback flag — never mutate the cached LKG.
            return {**lkg, "_lkg_fallback": True}
        raise
