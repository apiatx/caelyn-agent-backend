"""
Swing Regime Service — canonical deterministic scoring engine.

Pure computation module:
  - No provider requests
  - No database reads or writes
  - No cache ownership
  - No background jobs
  - No FastAPI awareness
  - Accepts normalized input dicts
  - Returns canonical swing-regime result
  - Independently unit-testable

Pillar model (correlation-aware):
  1. Trend & Breadth         40% weight
  2. Volatility & Credit     25% weight
  3. Rates & Dollar          20% weight
  4. Leadership & Cross-Asset 15% weight

Event risk is a separate sizing/volatility overlay — NOT a directional input.
Within each pillar, correlated components are combined via maximum-risk logic
to prevent single events from being triple-counted.

All weights and thresholds are initial estimates labelled
  calibration_status = "deterministic_uncalibrated"
and must not be presented as historically validated.
"""
from __future__ import annotations

import math
from typing import Any, Optional


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def assess_swing_regime(inputs: dict) -> dict:
    """
    Compute the canonical swing-regime result from normalized inputs.

    `inputs` expect fields produced by the composer (home_risk_intelligence.py).
    Every field is optional; missing values degrade gracefully to UNKNOWN.
    """
    pillars = _score_pillars(inputs)
    overall_risk_score = _compute_overall_risk_score(pillars)
    risk_level = _risk_level_from_score(overall_risk_score)
    regime_direction = _compute_regime_direction(pillars, risk_level)
    trade_bias = _compute_trade_bias(risk_level, regime_direction)
    position_size_hint = _compute_position_size_hint(risk_level, inputs)
    dominant_driver = _compute_dominant_driver(pillars, inputs)
    one_line = _build_one_line(risk_level, regime_direction, trade_bias, dominant_driver, pillars)
    flip_conditions = _compute_flip_conditions(pillars, inputs)
    event_overlay = _compute_event_overlay(inputs)

    return {
        "model_version":       "swing_regime_v1",
        "calibration_status":  "deterministic_uncalibrated",
        "risk_level":          risk_level,
        "risk_score":          overall_risk_score,
        "regime_direction":    regime_direction,
        "trade_bias":          trade_bias,
        "position_size_hint":  position_size_hint,
        "dominant_driver":     dominant_driver,
        "one_line":            one_line,
        "conditions_that_would_flip": flip_conditions,
        "pillars":             pillars,
        "event_overlay":       event_overlay,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Pillar scoring
# ─────────────────────────────────────────────────────────────────────────────

def _score_pillars(inputs: dict) -> dict:
    return {
        "trend_and_breadth":          _score_trend_and_breadth(inputs),
        "volatility_and_credit":      _score_volatility_and_credit(inputs),
        "rates_and_dollar":           _score_rates_and_dollar(inputs),
        "leadership_and_cross_asset": _score_leadership_and_cross_asset(inputs),
    }


PILLAR_WEIGHTS = {
    "trend_and_breadth":          0.40,
    "volatility_and_credit":      0.25,
    "rates_and_dollar":           0.20,
    "leadership_and_cross_asset": 0.15,
}


def _compute_overall_risk_score(pillars: dict) -> int:
    score = 0.0
    for name, pillar in pillars.items():
        w = PILLAR_WEIGHTS.get(name, 0.0)
        s = pillar.get("risk_score") or 0
        score += w * s
    return max(0, min(100, round(score)))


# ─────────────────────────────────────────────────────────────────────────────
# 1. Trend & Breadth (40 %)
# ─────────────────────────────────────────────────────────────────────────────

def _score_trend_and_breadth(inputs: dict) -> dict:
    spy_1d    = _f(inputs.get("spy_change_1d"))
    qqq_1d    = _f(inputs.get("qqq_change_1d"))
    breadth_1d = _f(inputs.get("sector_breadth_1d"))
    breadth_7d = _f(inputs.get("sector_breadth_7d"))
    spx_7d     = _f(inputs.get("spx_return_7d"))
    spx_63d    = _f(inputs.get("spx_return_63d"))

    comp = {}
    signals: list[tuple[float, str]] = []

    # ── Equity 1D direction ─────────────────────────────────────────────────
    equity_1d_signal = None
    if spy_1d is not None and qqq_1d is not None:
        equity_1d = (spy_1d + qqq_1d) / 2.0
        comp["spy_change_1d"] = round(spy_1d, 2)
        comp["qqq_change_1d"] = round(qqq_1d, 2)
        comp["equity_1d_avg"] = round(equity_1d, 2)
        if equity_1d <= -2.0:      sig = (90, "major_selloff_1d")
        elif equity_1d <= -1.0:    sig = (70, "moderate_selloff_1d")
        elif equity_1d <= -0.3:    sig = (45, "mild_weakness_1d")
        elif equity_1d >= 1.5:     sig = (10, "strong_1d")
        elif equity_1d >= 0.5:     sig = (20, "positive_1d")
        else:                       sig = (35, "flat_1d")
        equity_1d_signal = sig[0]
        signals.append(sig)
    elif spy_1d is not None:
        comp["spy_change_1d"] = round(spy_1d, 2)
        if spy_1d <= -2.0:        sig = (85, "major_selloff_spy_1d")
        elif spy_1d <= -1.0:      sig = (65, "moderate_selloff_spy_1d")
        elif spy_1d <= -0.3:      sig = (42, "mild_weakness_spy_1d")
        elif spy_1d >= 1.5:       sig = (10, "strong_spy_1d")
        elif spy_1d >= 0.5:       sig = (20, "positive_spy_1d")
        else:                     sig = (35, "flat_spy_1d")
        equity_1d_signal = sig[0]
        signals.append(sig)
    else:
        comp["equity_1d_avg"] = None

    # ── Equity multi-TF trend ───────────────────────────────────────────────
    if spx_7d is not None:
        comp["spx_return_7d"] = round(spx_7d, 2)
        if spx_7d <= -3.0:     sig = (80, "trend_bearish_7d")
        elif spx_7d <= -1.0:   sig = (60, "trend_weak_7d")
        elif spx_7d <= -0.2:   sig = (40, "trend_flat_7d")
        elif spx_7d >= 2.0:    sig = (15, "trend_bullish_7d")
        elif spx_7d >= 0.2:    sig = (25, "trend_positive_7d")
        else:                   sig = (30, "trend_flat_7d")
        signals.append(sig)

    if spx_63d is not None:
        comp["spx_return_63d"] = round(spx_63d, 2)
        if spx_63d <= -8.0:    sig = (85, "trend_deeply_bearish_63d")
        elif spx_63d <= -3.0:  sig = (65, "trend_bearish_63d")
        elif spx_63d <= -0.5:  sig = (40, "trend_flat_63d")
        elif spx_63d >= 5.0:   sig = (15, "trend_bullish_63d")
        else:                   sig = (28, "trend_mild_positive_63d")
        signals.append(sig)

    # ── Breadth ─────────────────────────────────────────────────────────────
    bd_signals: list[tuple[float, str]] = []
    if breadth_1d is not None:
        comp["breadth_1d"] = round(breadth_1d, 0)
        if breadth_1d < 30:       bd_signals.append((90, "breadth_collapse_1d"))
        elif breadth_1d < 40:     bd_signals.append((75, "breadth_weak_1d"))
        elif breadth_1d < 50:     bd_signals.append((55, "breadth_mixed_1d"))
        elif breadth_1d >= 70:    bd_signals.append((15, "breadth_strong_1d"))
        else:                     bd_signals.append((30, "breadth_neutral_1d"))

    if breadth_7d is not None:
        comp["breadth_7d"] = round(breadth_7d, 0)
        if breadth_7d < 30:       bd_signals.append((85, "breadth_collapse_7d"))
        elif breadth_7d < 40:     bd_signals.append((70, "breadth_weak_7d"))
        elif breadth_7d < 50:     bd_signals.append((50, "breadth_mixed_7d"))
        elif breadth_7d >= 70:    bd_signals.append((15, "breadth_strong_7d"))
        else:                     bd_signals.append((30, "breadth_neutral_7d"))

    # ── Correlation-aware scoring within pillar ─────────────────────────────
    # Equity signals (1D, 7D, 63D) are correlated — take maximum risk.
    # Breadth signals are correlated with equity — blend: max of each group averaged.
    equity_max = max((s[0] for s in signals), default=50)
    breadth_max = max((s[0] for s in bd_signals), default=50) if bd_signals else 50

    if signals and not bd_signals:
        risk_score = equity_max
    elif bd_signals and not signals:
        risk_score = breadth_max
    elif signals and bd_signals:
        risk_score = max(equity_max, breadth_max * 0.85)
    else:
        risk_score = 50

    risk_score = round(risk_score)

    # ── Direction ───────────────────────────────────────────────────────────
    equity_dir = _trend_direction_equity(inputs)
    breadth_dir = _breadth_direction(breadth_1d, breadth_7d)
    direction = _resolve_direction([equity_dir, breadth_dir], dominant="equity")

    n_avail = (1 if spy_1d is not None else 0) + (1 if spx_7d is not None else 0) + (1 if breadth_1d is not None else 0)
    confidence = _confidence(n_avail, 5)

    return {
        "risk_score": risk_score,
        "direction":  direction,
        "confidence": confidence,
        "components": comp,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 2. Volatility & Credit (25 %)
# ─────────────────────────────────────────────────────────────────────────────

def _score_volatility_and_credit(inputs: dict) -> dict:
    vix       = _f(inputs.get("vix_current"))
    vix_chg   = _f(inputs.get("vix_change_1d"))
    hyg_chg   = _f(inputs.get("hyg_change_1d"))
    vix_7d    = _f(inputs.get("vix_return_7d"))
    spx_7d    = _f(inputs.get("spx_return_7d"))

    comp = {}

    # ── VIX absolute level ──────────────────────────────────────────────────
    vix_signals: list[tuple[float, str]] = []
    if vix is not None:
        comp["vix"] = round(vix, 2)
        if vix >= 30:        vix_signals.append((90, "vix_stress"))
        elif vix >= 25:      vix_signals.append((70, "vix_elevated_high"))
        elif vix >= 20:      vix_signals.append((50, "vix_elevated"))
        elif vix >= 15:      vix_signals.append((20, "vix_normal"))
        else:                vix_signals.append((10, "vix_complacency"))

    # ── VIX 1D spike ────────────────────────────────────────────────────────
    if vix_chg is not None:
        comp["vix_change_1d"] = round(vix_chg, 2)
        if vix_chg >= 20:       vix_signals.append((95, "vix_spike_1d"))
        elif vix_chg >= 10:     vix_signals.append((75, "vix_surge_1d"))
        elif vix_chg >= 5:      vix_signals.append((55, "vix_uptick_1d"))
        elif vix_chg >= -5:     vix_signals.append((25, "vix_stable_1d"))
        else:                    vix_signals.append((10, "vix_drop_1d"))

    # ── Credit stress ───────────────────────────────────────────────────────
    credit_signals: list[tuple[float, str]] = []
    if hyg_chg is not None:
        comp["hyg_change_1d"] = round(hyg_chg, 2)
        if hyg_chg <= -2.0:       credit_signals.append((90, "credit_shock_1d"))
        elif hyg_chg <= -1.0:     credit_signals.append((70, "credit_weak_1d"))
        elif hyg_chg <= -0.3:     credit_signals.append((45, "credit_mild_weak_1d"))
        elif hyg_chg >= 0.5:      credit_signals.append((10, "credit_strong_1d"))
        else:                      credit_signals.append((25, "credit_stable_1d"))

    # ── VIX regime context ──────────────────────────────────────────────────
    if vix_7d is not None:
        comp["vix_return_7d"] = round(vix_7d, 2)

    # ── Correlation-aware score ─────────────────────────────────────────────
    vix_max    = max((s[0] for s in vix_signals), default=50)
    credit_max = max((s[0] for s in credit_signals), default=50) if credit_signals else 50

    if vix_signals and not credit_signals:
        risk_score = vix_max
    elif credit_signals and not vix_signals:
        risk_score = credit_max
    else:
        risk_score = max(vix_max * 0.75, credit_max * 0.65)

    risk_score = round(risk_score)

    # ── Direction ───────────────────────────────────────────────────────────
    dir_signals: list[str | None] = []
    if vix_chg is not None:
        if vix_chg >= 10:             dir_signals.append("WORSENING")
        elif vix_chg >= 3:            dir_signals.append("WEAKENING")
        elif vix_chg >= -3:           dir_signals.append("STABLE")
        else:                         dir_signals.append("IMPROVING")
    if vix is not None:
        if vix >= 25:                 dir_signals.append("WORSENING" if vix_chg is None or vix_chg > 0 else "STABLE")
        elif vix >= 20:               dir_signals.append("WEAKENING")
    if hyg_chg is not None:
        if hyg_chg <= -1.0:           dir_signals.append("WORSENING")
        elif hyg_chg <= -0.3:         dir_signals.append("WEAKENING")
        elif hyg_chg >= 0.3:          dir_signals.append("IMPROVING")
        else:                         dir_signals.append("STABLE")

    direction = _resolve_direction(dir_signals)

    n_avail = (1 if vix is not None else 0) + (1 if vix_chg is not None else 0) + (1 if hyg_chg is not None else 0)
    confidence = _confidence(n_avail, 3)

    return {
        "risk_score": risk_score,
        "direction":  direction,
        "confidence": confidence,
        "components": comp,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 3. Rates & Dollar (20 %)
# ─────────────────────────────────────────────────────────────────────────────

def _score_rates_and_dollar(inputs: dict) -> dict:
    us10y     = _f(inputs.get("us10y_yield"))
    dxy       = _f(inputs.get("dxy_price"))
    dxy_chg   = _f(inputs.get("dxy_change_1d"))

    comp = {}

    rate_signals: list[tuple[float, str]] = []
    if us10y is not None:
        comp["us10y"] = round(us10y, 3)
        if us10y >= 5.0:         rate_signals.append((90, "10y_extreme"))
        elif us10y >= 4.75:      rate_signals.append((75, "10y_elevated"))
        elif us10y >= 4.5:       rate_signals.append((55, "10y_watch"))
        elif us10y >= 4.0:       rate_signals.append((35, "10y_moderate"))
        else:                    rate_signals.append((15, "10y_low"))

    dxy_signals: list[tuple[float, str]] = []
    if dxy_chg is not None:
        comp["dxy_change_1d"] = round(dxy_chg, 2)
        if dxy_chg >= 1.0:        dxy_signals.append((85, "dxy_spike"))
        elif dxy_chg >= 0.5:      dxy_signals.append((65, "dxy_strength"))
        elif dxy_chg >= 0.2:      dxy_signals.append((45, "dxy_mild_strength"))
        elif dxy_chg >= -0.2:     dxy_signals.append((25, "dxy_flat"))
        elif dxy_chg >= -0.5:     dxy_signals.append((15, "dxy_weakness"))
        else:                     dxy_signals.append((10, "dxy_heavy_weakness"))

    if dxy is not None:
        comp["dxy"] = round(dxy, 3)

    # ── Correlation-aware ───────────────────────────────────────────────────
    rate_max = max((s[0] for s in rate_signals), default=50)
    dxy_max  = max((s[0] for s in dxy_signals), default=50) if dxy_signals else 50

    if rate_signals and not dxy_signals:
        risk_score = rate_max
    elif dxy_signals and not rate_signals:
        risk_score = dxy_max
    else:
        risk_score = max(rate_max * 0.70, dxy_max * 0.55)

    risk_score = round(risk_score)

    # ── Direction ───────────────────────────────────────────────────────────
    dir_signals: list[str | None] = []
    if us10y is not None:
        if us10y >= 5.0:              dir_signals.append("WORSENING")
        elif us10y >= 4.75:           dir_signals.append("WEAKENING")
        elif us10y >= 4.5:            dir_signals.append("WEAKENING")
        else:                         dir_signals.append("STABLE")
    if dxy_chg is not None:
        if dxy_chg >= 0.5:            dir_signals.append("WORSENING")
        elif dxy_chg >= 0.2:          dir_signals.append("WEAKENING")
        elif dxy_chg >= -0.2:         dir_signals.append("STABLE")
        else:                         dir_signals.append("IMPROVING")

    direction = _resolve_direction(dir_signals)

    n_avail = (1 if us10y is not None else 0) + (1 if dxy_chg is not None else 0)
    confidence = _confidence(n_avail, 2)

    return {
        "risk_score": risk_score,
        "direction":  direction,
        "confidence": confidence,
        "components": comp,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 4. Leadership & Cross-Asset (15 %)
# ─────────────────────────────────────────────────────────────────────────────

def _score_leadership_and_cross_asset(inputs: dict) -> dict:
    btc_chg = _f(inputs.get("btc_change_24h"))
    cyc_vs_def = _f(inputs.get("cyclical_vs_defensive_spread"))
    posture = inputs.get("market_posture") or ""

    comp = {}

    btc_signals: list[tuple[float, str]] = []
    if btc_chg is not None:
        comp["btc_change_24h"] = round(btc_chg, 2)
        if btc_chg <= -8.0:       btc_signals.append((90, "btc_crash"))
        elif btc_chg <= -5.0:     btc_signals.append((75, "btc_risk_off"))
        elif btc_chg <= -2.5:     btc_signals.append((55, "btc_weakening"))
        elif btc_chg <= -0.5:     btc_signals.append((35, "btc_flat"))
        elif btc_chg >= 3.0:      btc_signals.append((10, "btc_risk_on"))
        elif btc_chg >= 0.5:      btc_signals.append((20, "btc_positive"))
        else:                     btc_signals.append((30, "btc_quiet"))

    rot_signals: list[tuple[float, str]] = []
    if cyc_vs_def is not None:
        comp["cyclical_vs_defensive_spread"] = round(cyc_vs_def, 2)
        if cyc_vs_def <= -3.0:    rot_signals.append((80, "heavy_defensive_rotation"))
        elif cyc_vs_def <= -1.0:  rot_signals.append((60, "defensive_rotation"))
        elif cyc_vs_def <= 1.0:   rot_signals.append((30, "neutral_rotation"))
        elif cyc_vs_def >= 3.0:   rot_signals.append((10, "risk_on_rotation"))
        else:                     rot_signals.append((20, "cyclical_leadership"))
    if posture:
        comp["market_posture"] = posture
        if posture.lower() == "risk-off":
            rot_signals.append((70, "posture_risk_off"))
        elif posture.lower() == "risk-on":
            rot_signals.append((15, "posture_risk_on"))

    btc_max  = max((s[0] for s in btc_signals), default=50)
    rot_max  = max((s[0] for s in rot_signals), default=50) if rot_signals else 50

    if btc_signals and not rot_signals:
        risk_score = btc_max
    elif rot_signals and not btc_signals:
        risk_score = rot_max
    else:
        risk_score = max(btc_max * 0.60, rot_max * 0.55)

    risk_score = round(risk_score)

    # ── Direction ───────────────────────────────────────────────────────────
    dir_signals: list[str | None] = []
    if btc_chg is not None:
        if btc_chg <= -5.0:           dir_signals.append("WORSENING")
        elif btc_chg <= -2.5:         dir_signals.append("WEAKENING")
        elif btc_chg >= 3.0:          dir_signals.append("IMPROVING")
        elif btc_chg >= 0.5:          dir_signals.append("IMPROVING")
        else:                         dir_signals.append("STABLE")
    if cyc_vs_def is not None:
        if cyc_vs_def <= -3.0:        dir_signals.append("WORSENING")
        elif cyc_vs_def <= -1.0:      dir_signals.append("WEAKENING")
        elif cyc_vs_def >= 2.0:       dir_signals.append("IMPROVING")
        else:                         dir_signals.append("STABLE")

    direction = _resolve_direction(dir_signals)

    n_avail = (1 if btc_chg is not None else 0) + (1 if cyc_vs_def is not None else 0)
    confidence = _confidence(n_avail, 2)

    return {
        "risk_score": risk_score,
        "direction":  direction,
        "confidence": confidence,
        "components": comp,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Overall regime logic
# ─────────────────────────────────────────────────────────────────────────────

def _risk_level_from_score(score: int) -> str:
    if score >= 80:     return "EXTREME"
    elif score >= 65:   return "HIGH"
    elif score >= 45:   return "ELEVATED"
    elif score >= 25:   return "MODERATE"
    else:               return "LOW"


def _compute_regime_direction(pillars: dict, risk_level: str) -> str:
    dirs = [
        p.get("direction")
        for p in pillars.values()
        if p.get("direction") and p["direction"] != "UNKNOWN"
    ]
    if not dirs:
        return "UNKNOWN"

    n_worsening = sum(1 for d in dirs if d == "WORSENING")
    n_weakening = sum(1 for d in dirs if d == "WEAKENING")
    n_improving = sum(1 for d in dirs if d == "IMPROVING")
    n_stable    = sum(1 for d in dirs if d == "STABLE")

    risk_high = risk_level in ("HIGH", "EXTREME")
    risk_elev = risk_level in ("HIGH", "EXTREME", "ELEVATED")

    if n_worsening >= 2:
        return "WORSENING"
    if n_worsening >= 1 and risk_high:
        return "WORSENING"
    if n_weakening >= 2 and not n_improving:
        return "WEAKENING"
    if n_weakening >= 1 and risk_elev and not n_improving:
        return "WEAKENING"
    if n_improving >= 2 and n_worsening == 0:
        return "IMPROVING"
    if n_improving >= 1 and not risk_elev:
        return "IMPROVING"
    if n_stable >= len(dirs) * 0.5:
        return "STABLE"

    return "WEAKENING"


def _compute_trade_bias(risk_level: str, regime_direction: str) -> str:
    if regime_direction == "UNKNOWN":
        return "NEUTRAL"

    if risk_level == "EXTREME":
        if regime_direction in ("IMPROVING",):
            return "NEUTRAL"
        return "SHORT_HEDGE"

    if risk_level == "HIGH":
        if regime_direction == "IMPROVING":
            return "NEUTRAL"
        if regime_direction == "STABLE":
            return "SELECTIVE_SHORT"
        return "SHORT_HEDGE"

    if risk_level == "ELEVATED":
        if regime_direction == "IMPROVING":
            return "SELECTIVE_LONG"
        if regime_direction == "STABLE":
            return "NEUTRAL"
        return "SELECTIVE_SHORT"

    if risk_level == "MODERATE":
        if regime_direction == "IMPROVING":
            return "SELECTIVE_LONG"
        if regime_direction == "STABLE":
            return "SELECTIVE_LONG"
        return "NEUTRAL"

    if risk_level == "LOW":
        if regime_direction == "WORSENING":
            return "NEUTRAL"
        if regime_direction == "WEAKENING":
            return "SELECTIVE_LONG"
        return "LONG"

    return "NEUTRAL"


def _compute_position_size_hint(risk_level: str, inputs: dict) -> str:
    base = {
        "LOW":      "normal",
        "MODERATE": "selective",
        "ELEVATED": "selective",
        "HIGH":     "half-size",
        "EXTREME":  "preserve capital",
    }.get(risk_level, "selective")

    ev = inputs.get("has_upcoming_high_impact_event", False)
    if ev:
        if base == "normal":
            return "selective"
        if base == "selective":
            return "half-size"
        if base == "half-size":
            return "preserve capital"

    return base


def _compute_dominant_driver(pillars: dict, inputs: dict) -> str:
    best = None
    best_score = -1
    for name, p in pillars.items():
        s = p.get("risk_score") or 0
        if s > best_score:
            best_score = s
            best = name

    label_map = {
        "trend_and_breadth":          "broad_market_trend",
        "volatility_and_credit":      "volatility_stress",
        "rates_and_dollar":           "rate_and_dollar_pressure",
        "leadership_and_cross_asset": "cross_asset_deleveraging",
    }
    return label_map.get(best or "", "unknown")


def _build_one_line(risk_level: str, direction: str, trade_bias: str, driver: str, pillars: dict) -> str:
    parts: list[str] = []

    if risk_level in ("EXTREME", "HIGH"):
        parts.append(f"Risk level {risk_level} — conditions {direction.lower()}")
        if growth_weak := _growth_messaging(pillars):
            parts.append(growth_weak)
    elif risk_level == "ELEVATED":
        parts.append(f"Risk elevated, {direction.lower()}")
        if driver == "rate_and_dollar_pressure":
            parts.append("rate headwind persisting")
        elif driver == "volatility_stress":
            parts.append("elevated volatility, monitor credit")
    elif risk_level == "MODERATE":
        parts.append(f"Moderate risk, {direction.lower()}")
    else:
        parts.append(f"Low-risk environment, {direction.lower()}")

    if trade_bias in ("SHORT_HEDGE", "SELECTIVE_SHORT"):
        parts.append("— defensive posture warranted")
    elif trade_bias == "NEUTRAL":
        parts.append("— neutral positioning")
    elif trade_bias == "SELECTIVE_LONG":
        parts.append("— selective long entries")
    elif trade_bias == "LONG":
        parts.append("— risk-on")

    return " ".join(parts)


def _growth_messaging(pillars: dict) -> str | None:
    tb = pillars.get("trend_and_breadth", {})
    comps = tb.get("components", {})
    spx_63d = comps.get("spx_return_63d")
    breadth_1d = comps.get("breadth_1d")
    if spx_63d is not None and spx_63d < -3:
        return "sustained equity weakness"
    if breadth_1d is not None and breadth_1d < 40:
        return "participation collapsing"
    return None


def _compute_flip_conditions(pillars: dict, inputs: dict) -> list[str]:
    conditions: list[str] = []

    tb = pillars.get("trend_and_breadth", {})
    tb_comp = tb.get("components", {})
    if tb_comp.get("breadth_1d", 100) < 50:
        conditions.append("Breadth rises above 50")
    spx_7d = tb_comp.get("spx_return_7d")
    if spx_7d is not None and spx_7d < -1.0:
        conditions.append("SPX 7-day return turns positive")

    vc = pillars.get("volatility_and_credit", {})
    vc_comp = vc.get("components", {})
    if vc_comp.get("vix", 100) >= 25:
        conditions.append("VIX falls below 25")
    if vc_comp.get("hyg_change_1d", 0) is not None and vc_comp.get("hyg_change_1d", 0) <= -0.5:
        conditions.append("HYG stabilizes (1D positive)")

    rd = pillars.get("rates_and_dollar", {})
    rd_comp = rd.get("components", {})
    if rd_comp.get("us10y", 0) >= 4.75:
        conditions.append("10Y yield falls below 4.75%")
    dxy_chg = rd_comp.get("dxy_change_1d")
    if dxy_chg is not None and dxy_chg >= 0.5:
        conditions.append("DXY 1D strength reverses")

    lc = pillars.get("leadership_and_cross_asset", {})
    lc_comp = lc.get("components", {})
    if lc_comp.get("cyclical_vs_defensive_spread", 0) is not None and lc_comp.get("cyclical_vs_defensive_spread", 0) <= -1.0:
        conditions.append("Cyclicals resume leadership over defensives")

    if not conditions:
        conditions.append("No immediate flip conditions — monitor daily")

    return conditions[:5]


def _compute_event_overlay(inputs: dict) -> dict:
    has_event = inputs.get("has_upcoming_high_impact_event", False)
    days = inputs.get("days_until_next_event")
    next_title = inputs.get("next_event_title")

    if not has_event:
        return {
            "active": False,
            "severity": "NONE",
            "next_event": None,
            "days_until_event": None,
            "position_size_impact": None,
            "contributes_to_directional_score": False,
        }

    severity = "MODERATE"
    if days is not None and days <= 2:
        severity = "HIGH"

    impact = None
    risk = inputs.get("_risk_level_for_event") or "MODERATE"
    if severity == "HIGH" and risk in ("HIGH", "EXTREME"):
        impact = "consider delaying entries until after event"

    return {
        "active": True,
        "severity": severity,
        "next_event": next_title,
        "days_until_event": days,
        "position_size_impact": impact,
        "contributes_to_directional_score": False,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _f(v: Any) -> float | None:
    if v is None:
        return None
    try:
        fval = float(v)
        return None if (math.isnan(fval) or math.isinf(fval)) else fval
    except (TypeError, ValueError):
        return None


def _confidence(n_available: int, n_total: int) -> str:
    if n_available == 0:
        return "UNKNOWN"
    ratio = n_available / max(n_total, 1)
    if ratio >= 0.75:   return "HIGH"
    elif ratio >= 0.5:  return "MEDIUM"
    else:               return "LOW"


def _resolve_direction(dirs: list[str | None], dominant: str = "") -> str:
    valid = [d for d in dirs if d and d != "UNKNOWN"]
    if not valid:
        return "UNKNOWN"
    if "WORSENING" in valid:
        return "WORSENING"
    if "WEAKENING" in valid:
        ordering = {"WORSENING": 0, "WEAKENING": 1, "STABLE": 2, "IMPROVING": 3}
        worst = min(valid, key=lambda d: ordering.get(d, 99))
        return worst
    if all(d == "STABLE" for d in valid):
        return "STABLE"
    n_improving = sum(1 for d in valid if d == "IMPROVING")
    if n_improving >= len(valid) * 0.6:
        return "IMPROVING"
    return "STABLE"


def _trend_direction_equity(inputs: dict) -> str | None:
    spy_1d  = _f(inputs.get("spy_change_1d"))
    spx_7d  = _f(inputs.get("spx_return_7d"))
    spx_63d = _f(inputs.get("spx_return_63d"))

    signals: list[str] = []
    if spy_1d is not None:
        if spy_1d <= -1.0:     signals.append("WORSENING")
        elif spy_1d <= -0.3:   signals.append("WEAKENING")
        elif spy_1d >= 0.5:    signals.append("IMPROVING")
        else:                   signals.append("STABLE")
    if spx_7d is not None:
        if spx_7d <= -3.0:     signals.append("WORSENING")
        elif spx_7d <= -1.0:   signals.append("WEAKENING")
        elif spx_7d >= 1.5:    signals.append("IMPROVING")
        else:                   signals.append("STABLE")

    if not signals:
        return None
    return _resolve_direction(signals)


def _breadth_direction(breadth_1d: float | None, breadth_7d: float | None) -> str | None:
    signals: list[str] = []
    if breadth_1d is not None:
        if breadth_1d < 40:      signals.append("WORSENING")
        elif breadth_1d < 50:    signals.append("WEAKENING")
        elif breadth_1d >= 70:   signals.append("IMPROVING")
        else:                    signals.append("STABLE")
    if breadth_7d is not None:
        if breadth_7d < 40:      signals.append("WORSENING")
        elif breadth_7d < 50:    signals.append("WEAKENING")
        elif breadth_7d >= 70:   signals.append("IMPROVING")
        else:                    signals.append("STABLE")
    if not signals:
        return None
    return _resolve_direction(signals)
