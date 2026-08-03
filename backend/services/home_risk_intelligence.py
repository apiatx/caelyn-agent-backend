"""
Home Risk Intelligence — composer/aggregator for GET /api/home/risk-intelligence.

Design contract:
  - ZERO new upstream API calls. All values read from existing cached service outputs.
  - Sources reused (each retains its own upstream TTL):
      macro:dashboard:v3          MacroProvider.get_dashboard()   15-min TTL
      strategy:vix_regime:v1      build_vix_regime_payload()      15-min TTL  - same engine as
                                                                                  Macro "Should I Be Trading?"
      Neon calendar_snapshots     get_snapshot("economic_releases") weekly      - same source as
                                                                                  Calendar page
      HL in-memory state          get_state_optional()            in-process   - BTC price/change
      SR dashboard cache          sector_rotation.get_dashboard() 5-min TTL   - breadth score
      strategy:hist:dgs10:1830    precomputed DGS10 history (6h TTL + Neon)   - 10Y rate direction
      strategy:hist:vixcls:1830   precomputed VIXCLS history (6h TTL + Neon)  - VIX 7-session return

  - Composer cache: home:risk_intel:v1 (60 s TTL), LKG: home:risk_intel:v1:lkg (4 h)
  - Canonical scoring is delegated to swing_regime_service (pure, testable).
  - risk_cluster.triggers are canonical pillar-based display triggers.
  - legacy_triggers / legacy_trigger_count / legacy_headline / legacy_summary
    preserve old diagnostics for inspection only.
  - why_market_is_moving is a canonical swing-regime explanation.
  - legacy_why_market_is_moving preserves old bullets for inspection only.
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
_RISK_INTEL_TTL     = 60          # 1 min - upstream caches do the heavy lifting
_RISK_INTEL_LKG_TTL = 4 * 3600   # 4 h  - survives cold restarts


# -----------------------------------------------------------------------------
# Primitive helpers
# -----------------------------------------------------------------------------

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


# -----------------------------------------------------------------------------
# BTC from in-memory Hyperliquid state (zero API call)
# -----------------------------------------------------------------------------

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


# -----------------------------------------------------------------------------
# Sector data from SR dashboard cache (existing 5-min TTL)
# -----------------------------------------------------------------------------

async def _get_sector_data() -> dict:
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


# -----------------------------------------------------------------------------
# Rate history from precomputed DGS10 series
# -----------------------------------------------------------------------------

_DGS10_CACHE_KEY = "strategy:hist:dgs10:1830"


def _read_rate_history() -> dict:
    history: list[dict] = []
    source = "unavailable"
    status = "unavailable"

    # 1. In-memory cache
    hit = cache.get(_DGS10_CACHE_KEY)
    if hit and isinstance(hit, list) and len(hit) >= 2:
        history = hit
        source = "strategy:hist:dgs10:1830 (in-memory cache)"
        status = "available"
        return {"history": history, "history_source": source, "history_status": status}

    # 2. Neon fresh fallback (max 24h)
    try:
        from data.pg_storage import strategy_hist_read
        neon = strategy_hist_read(_DGS10_CACHE_KEY, 86400)
        if neon and isinstance(neon, list) and len(neon) >= 2:
            return {"history": neon, "history_source": "strategy:hist:dgs10:1830 (Neon fresh fallback)", "history_status": "available"}
    except Exception:
        pass

    # 3. Neon any-age fallback (weekend / holiday / delayed precompute)
    try:
        from data.pg_storage import strategy_hist_read
        neon = strategy_hist_read(_DGS10_CACHE_KEY, None)
        if neon and isinstance(neon, list) and len(neon) >= 2:
            return {"history": neon, "history_source": "strategy:hist:dgs10:1830 (Neon stale fallback)", "history_status": "stale"}
    except Exception:
        pass

    return {"history": [], "history_source": source, "history_status": status}


def _compute_yield_changes(history: list[dict], current_yield: float | None) -> dict:
    if not history or len(history) < 2:
        return {
            "history_as_of":       None,
            "change_1d_bps":       None,
            "change_5d_bps":       None,
            "change_20d_bps":      None,
            "history_source":      "strategy:hist:dgs10:1830",
            "history_status":      "unavailable",
        }

    sorted_hist = sorted(history, key=lambda r: r.get("date", ""))
    latest_obs = sorted_hist[-1]
    latest_date = latest_obs.get("date", "")

    latest_val = current_yield if current_yield is not None else latest_obs.get("value")

    def _lookback_value(n_sessions: int) -> float | None:
        idx = max(0, len(sorted_hist) - 1 - n_sessions)
        if idx >= len(sorted_hist):
            return None
        return sorted_hist[idx].get("value")

    def _bps_change(n_sessions: int, kind_label: str) -> float | None:
        if latest_val is None:
            return None
        back_val = _lookback_value(n_sessions)
        if back_val is None:
            return None
        if back_val == 0:
            return None
        try:
            latest_d = datetime.strptime(latest_date[:10], "%Y-%m-%d").date()
            back_d = datetime.strptime(sorted_hist[max(0, len(sorted_hist) - 1 - n_sessions)].get("date", "")[:10], "%Y-%m-%d").date()
        except Exception:
            return None
        delta_cal = (latest_d - back_d).days
        if delta_cal <= 0 and n_sessions > 0:
            return None
        return round((latest_val - back_val) * 100, 1)

    change_1d  = _bps_change(1, "1d")
    change_5d  = _bps_change(5, "5d")
    change_20d = _bps_change(20, "20d")

    return {
        "history_as_of":  latest_date,
        "change_1d_bps":  change_1d,
        "change_5d_bps":  change_5d,
        "change_20d_bps": change_20d,
        "history_source": "strategy:hist:dgs10:1830",
        "history_status": "available",
    }


# -----------------------------------------------------------------------------
# VIXCLS history helper - for real 7-session return
# -----------------------------------------------------------------------------

_VIXCLS_CACHE_KEY = "strategy:hist:vixcls:1830"


def _read_vixcls_history() -> dict:
    # 1. In-memory cache
    hit = cache.get(_VIXCLS_CACHE_KEY)
    if hit and isinstance(hit, list):
        return {"history": hit, "history_source": "strategy:hist:vixcls:1830 (in-memory cache)", "history_status": "available"}

    # 2. Neon fresh fallback (max 24h)
    try:
        from data.pg_storage import strategy_hist_read
        neon = strategy_hist_read(_VIXCLS_CACHE_KEY, 86400)
        if neon and isinstance(neon, list):
            return {"history": neon, "history_source": "strategy:hist:vixcls:1830 (Neon fresh fallback)", "history_status": "available"}
    except Exception:
        pass

    # 3. Neon any-age fallback (weekend / holiday)
    try:
        from data.pg_storage import strategy_hist_read
        neon = strategy_hist_read(_VIXCLS_CACHE_KEY, None)
        if neon and isinstance(neon, list):
            return {"history": neon, "history_source": "strategy:hist:vixcls:1830 (Neon stale fallback)", "history_status": "stale"}
    except Exception:
        pass

    return {"history": [], "history_source": "strategy:hist:vixcls:1830", "history_status": "unavailable"}


def _compute_vix_7d_return(vixcls: list[dict]) -> float | None:
    if not vixcls or len(vixcls) < 8:
        return None
    sorted_hist = sorted(vixcls, key=lambda r: r.get("date", ""))
    v_ago = sorted_hist[max(0, len(sorted_hist) - 8)].get("value")
    v_now = sorted_hist[-1].get("value")
    if v_ago is None or v_now is None or v_ago == 0:
        return None
    return round(((v_now / v_ago) - 1) * 100, 2)


# -----------------------------------------------------------------------------
# Economic events - normalize and filter upcoming
# -----------------------------------------------------------------------------

def _filter_upcoming_events(snapshot: dict, days_ahead: int = 7) -> list[dict]:
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


# -----------------------------------------------------------------------------
# Canonical display trigger builder
# -----------------------------------------------------------------------------

def _pillar_status_from_score(score: int) -> str:
    if score >= 65:    return "red"
    elif score >= 45:  return "orange"
    elif score >= 25:  return "yellow"
    else:              return "green"


def _build_canonical_trigger_list(swing_regime: dict, yc: dict, sector_data: dict) -> list[dict]:
    pillars = swing_regime.get("pillars", {})
    ev = swing_regime.get("event_overlay", {})
    triggers: list[dict] = []

    # --- Trend & Breadth ---
    tb = pillars.get("trend_and_breadth", {})
    tb_comp = tb.get("components", {})
    tb_risk = tb.get("risk_score") or 50
    tb_dir = tb.get("direction", "UNKNOWN")
    breadth_1d = tb_comp.get("breadth_1d")
    spx_7d = tb_comp.get("spx_return_7d")
    eq_avg = tb_comp.get("equity_1d_avg")

    tb_value_parts = []
    if breadth_1d is not None:
        tb_value_parts.append(f"{breadth_1d:.0f}/100 breadth")
    if eq_avg is not None:
        tb_value_parts.append(f"SPY/QQQ {eq_avg:+.1f}% 1D")
    if spx_7d is not None:
        tb_value_parts.append(f"SPX {spx_7d:+.1f}% 7D")
    tb_value = " · ".join(tb_value_parts) if tb_value_parts else None

    tb_msg = f"Equity trend {tb_dir.lower()}"
    if breadth_1d is not None and breadth_1d < 40:
        tb_msg += " — narrow participation"
    elif breadth_1d is not None and breadth_1d >= 70:
        tb_msg += " — broad participation"

    triggers.append({
        "key": "trend_and_breadth", "label": "Trend & Breadth",
        "status": _pillar_status_from_score(tb_risk),
        "value": tb_value,
        "threshold": "",
        "message": tb_msg,
        "direction": tb_dir, "timeframe": "multi-timeframe",
        "risk_score": tb_risk, "source_pillar": "trend_and_breadth",
    })

    # --- Volatility & Credit ---
    vc = pillars.get("volatility_and_credit", {})
    vc_comp = vc.get("components", {})
    vc_risk = vc.get("risk_score") or 50
    vc_dir = vc.get("direction", "UNKNOWN")
    vix_val = vc_comp.get("vix")
    hyg_val = vc_comp.get("hyg_change_1d")

    vc_value_parts = []
    if vix_val is not None:
        vc_value_parts.append(f"VIX {vix_val:.1f}")
    if hyg_val is not None:
        vc_value_parts.append(f"HYG {hyg_val:+.1f}%")
    vc_value = " · ".join(vc_value_parts) if vc_value_parts else None

    vc_msg = f"Volatility {vc_dir.lower()}"
    if vix_val is not None and vix_val >= 25:
        vc_msg += " — stress zone"
    elif vix_val is not None and vix_val >= 20:
        vc_msg += " — elevated"
    if hyg_val is not None and hyg_val <= -1.0:
        vc_msg += " · credit stress"
    elif hyg_val is not None and hyg_val <= -0.3:
        vc_msg += " · credit watch"

    triggers.append({
        "key": "volatility_and_credit", "label": "Volatility & Credit",
        "status": _pillar_status_from_score(vc_risk),
        "value": vc_value,
        "threshold": "",
        "message": vc_msg,
        "direction": vc_dir, "timeframe": "1D · 7D",
        "risk_score": vc_risk, "source_pillar": "volatility_and_credit",
    })

    # --- Rates & Dollar ---
    rd = pillars.get("rates_and_dollar", {})
    rd_comp = rd.get("components", {})
    rd_risk = rd.get("risk_score") or 50
    rd_dir = rd.get("direction", "UNKNOWN")
    us10y_val = rd_comp.get("us10y")
    chg_5d = rd_comp.get("us10y_change_5d_bps")
    dxy_val = rd_comp.get("dxy")
    dxy_chg = rd_comp.get("dxy_change_1d")

    rd_value_parts = []
    if us10y_val is not None:
        if chg_5d is not None:
            rd_value_parts.append(f"{us10y_val:.2f}% · {chg_5d:+.0f} bps/5D")
        else:
            rd_value_parts.append(f"{us10y_val:.2f}%")
    if dxy_val is not None and dxy_chg is not None:
        rd_value_parts.append(f"DXY {dxy_val:.1f} ({dxy_chg:+.1f}%)")
    rd_value = " · ".join(rd_value_parts) if rd_value_parts else None

    rd_msg = f"Rates/Dollar {rd_dir.lower()}"
    if us10y_val is not None and us10y_val >= 4.75 and chg_5d is not None and chg_5d < -5:
        rd_msg = f"10Y restrictive at {us10y_val:.2f}% but easing ({chg_5d:+.0f} bps/5D)"
    elif us10y_val is not None and chg_5d is not None and chg_5d > 5:
        rd_msg = f"10Y at {us10y_val:.2f}% · rising +{chg_5d:.0f} bps over 5 sessions"
    elif us10y_val is not None and us10y_val >= 4.75:
        rd_msg = f"10Y elevated at {us10y_val:.2f}%"
    if dxy_chg is not None and dxy_chg >= 0.5:
        rd_msg += " · dollar strengthening"

    triggers.append({
        "key": "rates_and_dollar", "label": "Rates & Dollar",
        "status": _pillar_status_from_score(rd_risk),
        "value": rd_value,
        "threshold": "",
        "message": rd_msg,
        "direction": rd_dir, "timeframe": "1D · 5D · 20D",
        "risk_score": rd_risk, "source_pillar": "rates_and_dollar",
    })

    # --- Leadership & Cross-Asset ---
    lc = pillars.get("leadership_and_cross_asset", {})
    lc_comp = lc.get("components", {})
    lc_risk = lc.get("risk_score") or 50
    lc_dir = lc.get("direction", "UNKNOWN")
    btc_val = lc_comp.get("btc_change_24h")
    cvd_val = lc_comp.get("cyclical_vs_defensive_spread")
    posture_val = lc_comp.get("market_posture")

    lc_value_parts = []
    if btc_val is not None:
        lc_value_parts.append(f"BTC {btc_val:+.1f}%")
    if cvd_val is not None:
        lc_value_parts.append(f"Cyc/Def {cvd_val:+.1f}%")
    if posture_val:
        lc_value_parts.append(f"{posture_val}")
    lc_value = " · ".join(lc_value_parts) if lc_value_parts else None

    lc_msg = f"Cross-asset {lc_dir.lower()}"
    if btc_val is not None and btc_val <= -5.0:
        lc_msg += " — BTC risk-off"
    if cvd_val is not None and cvd_val <= -1.0:
        lc_msg += " · defensive rotation"
    elif cvd_val is not None and cvd_val >= 2.0:
        lc_msg += " · risk-on rotation"

    triggers.append({
        "key": "leadership_and_cross_asset", "label": "Leadership & Cross-Asset",
        "status": _pillar_status_from_score(lc_risk),
        "value": lc_value,
        "threshold": "",
        "message": lc_msg,
        "direction": lc_dir, "timeframe": "1D · 30D",
        "risk_score": lc_risk, "source_pillar": "leadership_and_cross_asset",
    })

    # --- Event overlay trigger ---
    if ev.get("active"):
        ev_severity = ev.get("severity", "MODERATE")
        ev_status = "orange" if ev_severity == "HIGH" else "yellow"
        triggers.append({
            "key": "event_risk", "label": "Event Risk",
            "status": ev_status,
            "value": f"{ev.get('next_event', '')} ({ev.get('days_until_event', '?')}d)",
            "threshold": "",
            "message": f"{ev.get('next_event', 'Event')} in {ev.get('days_until_event', '?')} days — event risk reduces position size, not a directional signal",
            "direction": "UNKNOWN", "timeframe": "calendar",
            "risk_score": 0, "source_pillar": "event_overlay",
        })

    return triggers


# -----------------------------------------------------------------------------
# Canonical why_market_is_moving builder
# -----------------------------------------------------------------------------

def _build_canonical_why_bullets(swing_regime: dict, market_open: bool) -> list[str]:
    bullets: list[str] = []

    risk_level = swing_regime.get("risk_level", "UNKNOWN")
    direction = swing_regime.get("regime_direction", "UNKNOWN")
    trade_bias = swing_regime.get("trade_bias", "UNKNOWN")
    assessment = swing_regime.get("assessment_status", "PARTIAL")
    driver = swing_regime.get("dominant_driver", "")
    pillars = swing_regime.get("pillars", {})
    ev = swing_regime.get("event_overlay", {})

    if not market_open:
        bullets.append("US cash market is closed; equity and breadth signals reflect the latest completed session.")

    if assessment == "INSUFFICIENT_DATA":
        bullets.append("Insufficient data available — no directional conclusion should be drawn.")
        return bullets[:3]

    bias_str = trade_bias.replace("_", " ").lower()
    bullets.append(f"Swing risk is {risk_level} and {direction.lower()}; {bias_str} bias.")

    # Dominant driver details
    if driver == "rate_and_dollar_pressure":
        rd = pillars.get("rates_and_dollar", {}).get("components", {})
        us10y = rd.get("us10y")
        chg_5d = rd.get("us10y_change_5d_bps")
        if us10y is not None and chg_5d is not None and chg_5d < -5 and us10y >= 4.5:
            bullets.append(f"10Y is still restrictive at {us10y:.2f}% but has fallen {abs(chg_5d):.0f} bps over five sessions.")
        elif us10y is not None and chg_5d is not None and chg_5d > 5:
            bullets.append(f"10Y is at {us10y:.2f}% and has risen {chg_5d:.0f} bps over five sessions — rate pressure accelerating.")
        elif us10y is not None:
            bullets.append(f"10Y remains at {us10y:.2f}% — rate headwind persisting.")
    elif driver == "broad_market_trend":
        tb = pillars.get("trend_and_breadth", {}).get("components", {})
        breadth_1d = tb.get("breadth_1d")
        eq_avg = tb.get("equity_1d_avg")
        if breadth_1d is not None and breadth_1d < 40:
            bullets.append(f"Breadth remains narrow, with only {breadth_1d:.0f}% of sectors advancing.")
        elif eq_avg is not None and eq_avg <= -1.0:
            bullets.append(f"Equities are under pressure (SPY/QQQ avg {eq_avg:+.1f}% 1D).")
        elif eq_avg is not None and eq_avg >= 1.0:
            bullets.append(f"Equities showing strength (SPY/QQQ avg {eq_avg:+.1f}% 1D).")
    elif driver == "volatility_stress":
        vc = pillars.get("volatility_and_credit", {}).get("components", {})
        vix_val = vc.get("vix")
        if vix_val is not None:
            bullets.append(f"VIX elevated at {vix_val:.1f} — heightened option-market uncertainty.")
    elif driver == "cross_asset_deleveraging":
        lc = pillars.get("leadership_and_cross_asset", {}).get("components", {})
        btc_val = lc.get("btc_change_24h")
        cvd_val = lc.get("cyclical_vs_defensive_spread")
        if btc_val is not None and btc_val <= -5.0:
            bullets.append(f"Bitcoin {btc_val:+.1f}% — risk-off appetite in crypto markets.")
        if cvd_val is not None and cvd_val <= -1.0:
            bullets.append("Defensives are leading cyclicals — sector rotation toward safety.")

    # Event overlay bullet
    if ev.get("active"):
        ev_title = ev.get("next_event") or "Event"
        ev_days = ev.get("days_until_event")
        if ev_days is not None:
            bullets.append(f"{ev_title} is due in {ev_days} day{'s' if ev_days != 1 else ''}; event risk reduces position size but does not create a bearish directional signal.")

    # Fallback
    if len(bullets) < 2:
        bullets.append("Markets trading within monitored parameters — no single dominant risk driver detected.")

    return bullets[:3]


# -----------------------------------------------------------------------------
# Trade decision - projected from swing_regime
# -----------------------------------------------------------------------------

def _project_trade_decision_from_swing_regime(swing_regime: dict, vix_payload: dict) -> dict:
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


# -----------------------------------------------------------------------------
# Risk cluster - legacy trigger rules from existing values
# -----------------------------------------------------------------------------

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

    # VIX
    vix_spike = (vix_change_pct is not None and vix_change_pct >= 20)
    vix_high  = (vix is not None and vix >= 25)
    vix_elev  = (vix is not None and vix >= 20)

    if vix is not None or vix_change_pct is not None:
        if vix_spike or vix_high:
            status = "red"
            val_str = f"{vix:.2f}" if vix else "-"
            chg_str = f" (+{vix_change_pct:.1f}%)" if vix_change_pct is not None else ""
            msg = f"VIX {val_str}{chg_str} - fear/volatility spike"
        elif vix_elev:
            status = "yellow"
            msg = f"VIX {vix:.1f} - elevated above 20 threshold"
        else:
            status = "green"
            msg = f"VIX {vix:.1f} - calm zone" if vix else "VIX: data unavailable"
        triggers.append({
            "key": "vix_spike", "label": "VIX", "status": status,
            "value": f"{vix:.2f}" if vix else None,
            "threshold": ">=25 or spike >=+20% = red | >=20 = yellow",
            "message": msg,
        })

    # QQQ
    if qqq_change_pct is not None:
        if qqq_change_pct <= -2.5:
            status, msg = "red",    f"QQQ {qqq_change_pct:+.1f}% - significant tech selling"
        elif qqq_change_pct <= -1.0:
            status, msg = "yellow", f"QQQ {qqq_change_pct:+.1f}% - weakness"
        elif qqq_change_pct >= 1.5:
            status, msg = "green",  f"QQQ {qqq_change_pct:+.1f}% - risk-on momentum"
        else:
            status, msg = "green",  f"QQQ {qqq_change_pct:+.1f}%"
        triggers.append({
            "key": "nasdaq_selloff", "label": "Nasdaq 100 (QQQ)", "status": status,
            "value": f"{qqq_change_pct:+.2f}%",
            "threshold": "<=-2.5% = red | <=-1% = yellow", "message": msg,
        })

    # SPY
    if spy_change_pct is not None:
        if spy_change_pct <= -1.5:
            status, msg = "red",    f"S&P 500 {spy_change_pct:+.1f}% - broad market selling"
        elif spy_change_pct <= -0.5:
            status, msg = "yellow", f"S&P 500 {spy_change_pct:+.1f}%"
        elif spy_change_pct >= 1.0:
            status, msg = "green",  f"S&P 500 {spy_change_pct:+.1f}% - broad market strength"
        else:
            status, msg = "green",  f"S&P 500 {spy_change_pct:+.1f}%"
        triggers.append({
            "key": "sp500_selloff", "label": "S&P 500 (SPY)", "status": status,
            "value": f"{spy_change_pct:+.2f}%",
            "threshold": "<=-1.5% = red | <=-0.5% = yellow", "message": msg,
        })

    # BTC
    if btc_change_pct is not None:
        if btc_change_pct <= -5.0:
            status, msg = "red",    f"BTC {btc_change_pct:+.1f}% - risk-off, heavy selling"
        elif btc_change_pct <= -2.5:
            status, msg = "orange", f"BTC {btc_change_pct:+.1f}% - caution, weakening"
        elif btc_change_pct >= 3.0:
            status, msg = "green",  f"BTC {btc_change_pct:+.1f}% - risk-on appetite"
        else:
            status, msg = "green",  f"BTC {btc_change_pct:+.1f}%"
        triggers.append({
            "key": "btc_risk_off", "label": "Bitcoin", "status": status,
            "value": f"{btc_change_pct:+.2f}%",
            "threshold": "<=-5% = red | <=-2.5% = orange", "message": msg,
        })

    # Breadth
    if breadth_score is not None:
        if breadth_score < 40:
            status, msg = "red",    f"Breadth {breadth_score:.0f}/100 - majority of sectors declining"
        elif breadth_score < 50:
            status, msg = "yellow", f"Breadth {breadth_score:.0f}/100 - mixed/negative"
        elif breadth_score >= 70:
            status, msg = "green",  f"Breadth {breadth_score:.0f}/100 - broad participation"
        else:
            status, msg = "green",  f"Breadth {breadth_score:.0f}/100 - neutral"
        triggers.append({
            "key": "market_breadth", "label": "Market breadth", "status": status,
            "value": f"{breadth_score:.0f}/100",
            "threshold": "<40 = red | <50 = yellow", "message": msg,
        })

    # 10Y
    if us10y is not None:
        if us10y >= 4.75:
            status, msg = "red",    f"10Y yield {us10y:.2f}% - elevated rate pressure on equities"
        elif us10y >= 4.5:
            status, msg = "yellow", f"10Y yield {us10y:.2f}% - rate headwind watch zone"
        else:
            status, msg = "green",  f"10Y yield {us10y:.2f}% - below key 4.5% threshold"
        triggers.append({
            "key": "ten_y_yield", "label": "10Y Treasury yield", "status": status,
            "value": f"{us10y:.3f}%",
            "threshold": ">=4.75% = red | >=4.5% = yellow", "message": msg,
        })

    # DXY
    if dxy is not None and dxy_change_pct is not None:
        if dxy_change_pct >= 0.5:
            status, msg = "orange", f"DXY +{dxy_change_pct:.2f}% - dollar strength headwind for risk assets"
        elif dxy_change_pct >= 0.2:
            status, msg = "yellow", f"DXY +{dxy_change_pct:.2f}% - mild dollar strength"
        elif dxy_change_pct <= -0.3:
            status, msg = "green",  f"DXY {dxy_change_pct:.2f}% - dollar weakness, risk-on tailwind"
        else:
            status, msg = "green",  f"DXY {dxy_change_pct:+.2f}%"
        triggers.append({
            "key": "dxy_headwind", "label": "US Dollar (DXY)", "status": status,
            "value": f"{dxy:.2f} ({dxy_change_pct:+.2f}%)",
            "threshold": ">=+0.5%/day = orange | >=+0.2% = yellow", "message": msg,
        })

    # HYG
    if hyg_change_pct is not None:
        if hyg_change_pct <= -1.5:
            status, msg = "red",    f"HYG {hyg_change_pct:+.1f}% - high-yield stress, credit spreads widening"
        elif hyg_change_pct <= -0.5:
            status, msg = "yellow", f"HYG {hyg_change_pct:+.1f}% - watch credit conditions"
        else:
            status, msg = "green",  f"HYG {hyg_change_pct:+.1f}% - credit calm"
        triggers.append({
            "key": "credit_stress", "label": "Credit stress (HYG)", "status": status,
            "value": f"{hyg_change_pct:+.2f}%",
            "threshold": "<=-1.5% = red | <=-0.5% = yellow", "message": msg,
        })

    if has_upcoming_high_impact_event:
        triggers.append({
            "key": "macro_event_risk", "label": "Upcoming high-impact event",
            "status": "orange", "value": "within 3 trading days",
            "threshold": "high-importance economic release",
            "message": "High-impact macro event within 3 trading days - elevated volatility likely",
        })

    hot_count = sum(1 for t in triggers if t["status"] in ("red", "orange"))
    hot_triggers = [t for t in triggers if t["status"] in ("red", "orange")]
    if not hot_triggers:
        legacy_headline = "Markets appear orderly - no major risk signals active"
        legacy_summary  = "All monitored risk indicators are within normal parameters."
    else:
        verb = "active" if hot_count >= 2 else "flagged"
        legacy_headline = f"{hot_count} risk signal{'s' if hot_count > 1 else ''} {verb} - consider defensive positioning" \
                          if hot_count >= 2 else f"{hot_count} risk signal flagged - monitor closely"
        legacy_summary  = " | ".join(t["message"] for t in hot_triggers[:3])

    return {
        "triggers":                triggers,
        "legacy_trigger_count":    hot_count,
        "legacy_headline":         legacy_headline,
        "legacy_summary":          legacy_summary,
    }


# -----------------------------------------------------------------------------
# Legacy "Why market is moving" - preserved for backward compat
# -----------------------------------------------------------------------------

def _build_legacy_why_bullets(
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

    if vix is not None and vix_change_pct is not None and vix_change_pct >= 10:
        bullets.append(f"VIX spiked +{vix_change_pct:.1f}% to {vix:.1f} - heightened demand for protection")
    elif vix is not None and vix >= 20:
        bullets.append(f"VIX at {vix:.1f} - elevated fear/uncertainty in options market")

    if qqq_change_pct is not None and abs(qqq_change_pct) >= 1.0:
        dir_ = "selling off" if qqq_change_pct < 0 else "rallying"
        bullets.append(f"Tech/Nasdaq (QQQ) {dir_}: {qqq_change_pct:+.1f}%")
    elif spy_change_pct is not None and abs(spy_change_pct) >= 0.8:
        dir_ = "selling off" if spy_change_pct < 0 else "rallying"
        bullets.append(f"S&P 500 {dir_}: {spy_change_pct:+.1f}%")

    if btc_change_pct is not None and abs(btc_change_pct) >= 2.5:
        dir_ = "risk-off rotation" if btc_change_pct < 0 else "risk-on buying"
        bullets.append(f"Bitcoin {btc_change_pct:+.1f}% - {dir_}")

    if us10y is not None and us10y >= 4.5:
        bullets.append(f"10Y yield {us10y:.2f}% - persistent rate pressure on growth valuations")

    if dxy_change_pct is not None and dxy_change_pct >= 0.3:
        bullets.append(f"Dollar (DXY) +{dxy_change_pct:.2f}% - USD strength weighing on risk assets")

    if not bullets and vix_signal_title:
        bullets.append(vix_signal_title)

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


# -----------------------------------------------------------------------------
# Market open check
# -----------------------------------------------------------------------------

def _is_us_market_open() -> bool:
    try:
        import zoneinfo
        now_et = datetime.now(zoneinfo.ZoneInfo("America/New_York"))
    except Exception:
        now_et = datetime.now(timezone.utc)
    mins = now_et.hour * 60 + now_et.minute
    return now_et.weekday() < 5 and (9 * 60 + 30) <= mins < (16 * 60)


def _compute_market_context(market_open: bool, macro_age: int | None) -> str:
    if market_open and macro_age is not None and macro_age <= 900:
        return "live_session"
    elif not market_open:
        return "closed_last_session"
    else:
        return "stale"


# -----------------------------------------------------------------------------
# Legacy risk-cluster to canonical projection
# -----------------------------------------------------------------------------

def _project_risk_cluster_from_swing_regime(
    swing_regime: dict,
    legacy: dict,
    canonical_triggers: list[dict],
) -> dict:
    """
    Build a fully coherent risk_cluster from the canonical swing_regime.

    risk_cluster.triggers  = canonical pillar-based display triggers
    risk_cluster.severity  = swing_regime.risk_level
    risk_cluster.score     = swing_regime.risk_score
    risk_cluster.active    = true for ELEVATED/HIGH/EXTREME
    risk_cluster.headline  = risk level + regime direction + trade bias
    risk_cluster.summary   = swing_regime.one_line
    risk_cluster.trigger_count = number of at-risk pillars (pillar risk_score >= 45)

    Legacy diagnostics (preserved additively):
      - legacy_triggers
      - legacy_trigger_count
      - legacy_headline
      - legacy_summary
    """
    risk_level = swing_regime.get("risk_level", "LOW")
    direction  = swing_regime.get("regime_direction", "UNKNOWN")
    trade_bias = swing_regime.get("trade_bias", "NEUTRAL")
    assessment = swing_regime.get("assessment_status", "PARTIAL")

    if assessment == "INSUFFICIENT_DATA":
        headline = "INSUFFICIENT DATA - NO DIRECTIONAL CONCLUSION"
        summary  = swing_regime.get("one_line", "")
        canonical_trigger_count = 0
        active = False
    else:
        headline = f"{risk_level} SWING RISK - CONDITIONS {direction} - {trade_bias.replace('_', ' ')} BIAS"
        summary  = swing_regime.get("one_line", "")
        pillars = swing_regime.get("pillars", {})
        canonical_trigger_count = sum(
            1 for p in pillars.values()
            if (p.get("risk_score") or 0) >= 45
        )
        active = risk_level in ("ELEVATED", "HIGH", "EXTREME")

    return {
        "active":                   active,
        "severity":                 risk_level,
        "score":                    swing_regime.get("risk_score", 50),
        "headline":                 headline,
        "summary":                  summary,
        "trigger_count":            canonical_trigger_count,
        "triggers":                 canonical_triggers,
        "legacy_triggers":          legacy.get("triggers", []),
        "legacy_trigger_count":     legacy.get("legacy_trigger_count", 0),
        "legacy_headline":          legacy.get("legacy_headline", ""),
        "legacy_summary":           legacy.get("legacy_summary", ""),
    }


# -----------------------------------------------------------------------------
# Main builder
# -----------------------------------------------------------------------------

async def build_home_risk_intelligence(macro_provider) -> dict:
    t0      = time.monotonic()
    now_utc = datetime.now(timezone.utc)

    # 1. Fast cache path
    cached = cache.get(_RISK_INTEL_KEY)
    if cached is not None:
        return cached

    # 2. Gather from existing caches concurrently
    from services.calendar_snapshot_service import get_snapshot
    from services.strategy_macro_service import build_vix_regime_payload

    macro_task   = macro_provider.get_dashboard() if macro_provider else asyncio.sleep(0)
    regime_task  = build_vix_regime_payload(macro_provider) if macro_provider else asyncio.sleep(0)
    cal_task     = asyncio.to_thread(get_snapshot, "economic_releases")
    sector_task  = _get_sector_data()
    dgs10_task   = asyncio.to_thread(_read_rate_history)
    vixcls_task  = asyncio.to_thread(_read_vixcls_history)

    macro_raw, vix_payload, econ_snap, sector_data, rate_hist, vixcls_hist = await asyncio.gather(
        macro_task, regime_task, cal_task, sector_task, dgs10_task, vixcls_task,
        return_exceptions=True,
    )

    macro_raw    = _safe(macro_raw, {})
    vix_payload  = _safe(vix_payload, {})
    econ_snap    = _safe(econ_snap, {})
    sector_data  = _safe(sector_data, {})
    if isinstance(sector_data, Exception):
        sector_data = {}
    rate_hist    = _safe(rate_hist, {})
    if isinstance(rate_hist, Exception):
        rate_hist = {"history": [], "history_source": "error", "history_status": "unavailable"}
    vixcls_hist  = _safe(vixcls_hist, {})
    if isinstance(vixcls_hist, Exception):
        vixcls_hist = {"history": [], "history_source": "error", "history_status": "unavailable"}

    breadth_score = sector_data.get("sector_breadth_1d")

    print(
        "[RISK_INTEL] sources: macro:dashboard:v3 | strategy:vix_regime:v1 | "
        "calendar_snapshots/economic_releases (Neon) | HL in-memory state | "
        "SR dashboard cache | strategy:hist:dgs10:1830 | strategy:hist:vixcls:1830 "
        "- NO new FMP calls"
    )

    # 3. Extract values from macro dashboard
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

    vix       = _r(vix_d.get("current"))
    vix_chg   = _r(vix_d.get("change_pct"))
    spy_price = _r(spy.get("price"))
    spy_chg   = _r(spy.get("change_pct"))
    qqq_price = _r(qqq.get("price"))
    qqq_chg   = _r(qqq.get("change_pct"))
    dia_price = _r(dia.get("price"))
    dia_chg   = _r(dia.get("change_pct"))
    us10y     = _r(rates.get("us_10y"), 3)
    dxy       = _r(dollar.get("dxy"), 3)
    dxy_chg_pct = _r(dollar.get("dxy_change_pct") or dollar.get("dxy_chg_pct") or dollar.get("dxy_chg_1d"), 3)
    hyg_chg   = _r(hyg.get("change_pct"))

    # BTC
    btc_data  = _get_btc_from_hl()
    btc_price = (btc_data or {}).get("price")
    btc_chg   = (btc_data or {}).get("change_pct")

    # Rate history context
    yc = _compute_yield_changes(rate_hist.get("history", []), us10y)
    us10y_chg_bps = yc.get("change_1d_bps")
    yc_status = rate_hist.get("history_status", "unavailable")

    # VIX 7-session return (real % change, not vix_min)
    vix_7d_ret = _compute_vix_7d_return(vixcls_hist.get("history", []))

    # 4. Market snapshot
    market_snapshot = {
        "sp500":     {"symbol": "SPY",   "price": spy_price, "change_pct": spy_chg},
        "dow":       {"symbol": "DIA",   "price": dia_price, "change_pct": dia_chg},
        "nasdaq100": {"symbol": "QQQ",   "price": qqq_price, "change_pct": qqq_chg},
        "bitcoin":   {"symbol": "BTC",   "price": btc_price, "change_pct": btc_chg},
        "us10y": {
            "symbol":        "US10Y",
            "price":         us10y,
            "change_bps":    us10y_chg_bps,
            "change_source": yc.get("history_source"),
            "change_as_of":  yc.get("history_as_of"),
            "change_status": yc_status,
        },
        "vix":  {"symbol": "VIX", "price": vix,  "change_pct": vix_chg},
        "dxy":  {"symbol": "DXY", "price": dxy,  "change_pct": dxy_chg_pct},
    }

    # 5. Multi-TF from vix_payload
    hist_windows = (vix_payload.get("historical_windows") or {})
    spx_7d  = (hist_windows.get("7d") or {}).get("spx_return_pct")
    spx_63d = (hist_windows.get("quarter") or {}).get("spx_return_pct")

    # 6. Economic events
    upcoming_events = _filter_upcoming_events(econ_snap, days_ahead=7)

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

    # 7. Build normalized inputs for canonical swing-regime engine
    swing_inputs = {
        "spy_change_1d":                  spy_chg,
        "qqq_change_1d":                  qqq_chg,
        "sector_breadth_1d":              sector_data.get("sector_breadth_1d"),
        "sector_breadth_7d":              sector_data.get("sector_breadth_7d"),
        "spx_return_7d":                  spx_7d,
        "spx_return_63d":                 spx_63d,
        "vix_current":                    vix,
        "vix_change_1d":                  vix_chg,
        "vix_return_7d":                  vix_7d_ret,
        "hyg_change_1d":                  hyg_chg,
        "us10y_yield":                    us10y,
        "us10y_change_1d_bps":            yc.get("change_1d_bps"),
        "us10y_change_5d_bps":            yc.get("change_5d_bps"),
        "us10y_change_20d_bps":           yc.get("change_20d_bps"),
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

    # 8. Project trade_decision from swing_regime
    trade_decision = _project_trade_decision_from_swing_regime(swing_regime, vix_payload)

    # 9. Build canonical display triggers
    canonical_triggers = _build_canonical_trigger_list(swing_regime, yc, sector_data)

    # 10. Build legacy risk cluster
    breadth_float = float(breadth_score) if isinstance(breadth_score, (int, float)) else None
    legacy_rc = _assess_risk_cluster(
        vix=vix, vix_change_pct=vix_chg,
        spy_change_pct=spy_chg, qqq_change_pct=qqq_chg,
        btc_change_pct=btc_chg, breadth_score=breadth_float,
        us10y=us10y, dxy=dxy, dxy_change_pct=dxy_chg_pct,
        hyg_change_pct=hyg_chg,
        has_upcoming_high_impact_event=has_hi_impact,
    )
    risk_cluster = _project_risk_cluster_from_swing_regime(swing_regime, legacy_rc, canonical_triggers)

    # 11. Market open
    market_open = _is_us_market_open()

    # 12. Why bullets (canonical)
    why_bullets = _build_canonical_why_bullets(swing_regime, market_open)

    # 13. Legacy why bullets
    vix_sig_title = (vix_payload.get("vix_regime_signal") or {}).get("signal_title")
    legacy_why_bullets = _build_legacy_why_bullets(
        vix=vix, vix_change_pct=vix_chg,
        spy_change_pct=spy_chg, qqq_change_pct=qqq_chg,
        btc_change_pct=btc_chg, us10y=us10y,
        dxy_change_pct=dxy_chg_pct, vix_signal_title=vix_sig_title,
    )

    # 14. Freshness
    macro_gen_at = vix_payload.get("generated_at")
    cal_updated  = econ_snap.get("last_updated")
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
        "market_context":              _compute_market_context(market_open, macro_age),
        "diagnostic_sources": [
            "macro:dashboard:v3 (MacroProvider.get_dashboard, 15-min TTL) - SPY/QQQ/DIA/VIX/10Y/DXY/HYG",
            "strategy:vix_regime:v1 (build_vix_regime_payload, 15-min TTL) - SAME engine as Macro",
            "Neon calendar_snapshots/economic_releases (weekly refresh) - SAME source as Calendar page",
            "Hyperliquid in-memory state (get_state_optional, no TTL) - BTC price and 24h change",
            "sector_rotation.get_dashboard (SR dashboard cache, 5-min TTL) - sector breadth + multi-TF regime",
            "strategy:hist:dgs10:1830 (6-h TTL + Neon) - 10Y rate direction (1D/5D/20D bps changes)",
            "strategy:hist:vixcls:1830 (6-h TTL + Neon) - VIX 7-session return",
            "NO new FMP/upstream API calls made for Home Risk Intelligence",
        ],
    }

    # 15. Assemble result
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    print(
        f"[RISK_INTEL] built in {elapsed_ms}ms - "
        f"swing_level={swing_regime['risk_level']} swing_score={swing_regime['risk_score']} "
        f"bias={swing_regime['trade_bias']} status={swing_regime['assessment_status']} "
        f"events={len(upcoming_events)} breadth={breadth_float} "
        f"10y_chg_1d={us10y_chg_bps} mkt={data_freshness['market_context']}"
    )

    result = {
        "as_of":                     now_utc.isoformat(),
        "market_open":               market_open,
        "data_freshness":            data_freshness,
        "market_snapshot":           market_snapshot,
        "trade_decision":            trade_decision,
        "risk_cluster":              risk_cluster,
        "swing_regime":              swing_regime,
        "upcoming_economic_events":  upcoming_events,
        "why_market_is_moving":      why_bullets,
        "legacy_why_market_is_moving": legacy_why_bullets,
    }

    cache.set(_RISK_INTEL_KEY,     result, _RISK_INTEL_TTL)
    cache.set(_RISK_INTEL_LKG_KEY, result, _RISK_INTEL_LKG_TTL)
    return result


async def build_home_risk_intelligence_safe(macro_provider) -> dict:
    try:
        return await build_home_risk_intelligence(macro_provider)
    except Exception as exc:
        print(f"[RISK_INTEL] build error (trying LKG): {exc}")
        lkg = cache.get(_RISK_INTEL_LKG_KEY)
        if lkg is not None:
            return {**lkg, "_lkg_fallback": True}
        raise
