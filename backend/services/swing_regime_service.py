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
from typing import Any


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def assess_swing_regime(inputs: dict) -> dict:
    """
    Compute the canonical swing-regime result from normalized inputs.

    `inputs` expect fields produced by the composer (home_risk_intelligence.py).
    Every field is optional; missing values degrade gracefully.
    """
    pillars = _score_pillars(inputs)
    pillar_avail = _compute_pillar_availability(pillars)
    overall_risk_score = _compute_overall_risk_score(pillars, pillar_avail)
    risk_level = _risk_level_from_score(overall_risk_score, pillar_avail)
    regime_direction = _compute_regime_direction(pillars, risk_level, pillar_avail)
    trade_bias = _compute_trade_bias(risk_level, regime_direction, pillar_avail)
    base_pos_size = _compute_position_size_hint(risk_level, inputs)
    final_pos_size = _apply_event_sizing(base_pos_size, inputs)
    dominant_driver = _compute_dominant_driver(pillars, inputs)
    one_line = _build_one_line(risk_level, regime_direction, trade_bias, dominant_driver, pillars, pillar_avail)
    flip_conditions = _compute_flip_conditions(pillars, inputs)
    event_overlay = _compute_event_overlay(inputs, risk_level, base_pos_size, final_pos_size)

    _enrich_pillar_diagnostics(pillars)

    # Pillar counts for sufficiency
    n_avail_pillars = sum(1 for v in pillar_avail.values() if v)
    n_total_pillars = len(pillar_avail)

    if n_avail_pillars < 2:
        assessment_status = "INSUFFICIENT_DATA"
    elif n_avail_pillars < n_total_pillars:
        assessment_status = "PARTIAL"
    else:
        assessment_status = "COMPLETE"

    total_comp_avail = sum(pillars[n]["available_component_count"] for n in pillars)
    total_comp_exp = sum(pillars[n]["expected_component_count"] for n in pillars)
    missing_critical = []
    if not pillar_avail.get("trend_and_breadth"):
        missing_critical.append("trend_and_breadth")
    if not pillar_avail.get("volatility_and_credit"):
        missing_critical.append("volatility_and_credit")

    return {
        "model_version":             "swing_regime_v1",
        "calibration_status":        "deterministic_uncalibrated",
        "assessment_status":         assessment_status,
        "available_pillar_count":    n_avail_pillars,
        "available_component_count": total_comp_avail,
        "missing_critical_inputs":   missing_critical,
        "risk_level":                risk_level,
        "risk_score":                overall_risk_score,
        "regime_direction":          regime_direction,
        "trade_bias":                trade_bias,
        "base_position_size_hint":   base_pos_size,
        "position_size_hint":        final_pos_size,
        "dominant_driver":           dominant_driver,
        "one_line":                  one_line,
        "conditions_that_would_flip": flip_conditions,
        "pillars":                   pillars,
        "event_overlay":             event_overlay,
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


def _compute_pillar_availability(pillars: dict) -> dict[str, bool]:
    avail = {}
    for name, p in pillars.items():
        ec = p.get("expected_component_count", 1)
        ac = p.get("available_component_count", 0)
        avail[name] = ac >= max(1, ec * 0.4)
    return avail


def _compute_overall_risk_score(pillars: dict, pillar_avail: dict[str, bool]) -> int:
    active_weights = {
        name: PILLAR_WEIGHTS[name]
        for name, available in pillar_avail.items()
        if available
    }
    if not active_weights:
        return 50
    total_w = sum(active_weights.values())
    if total_w == 0:
        return 50

    score = 0.0
    for name, w in active_weights.items():
        s = pillars[name].get("risk_score") or 50
        score += (w / total_w) * s
    return max(0, min(100, round(score)))


# ─────────────────────────────────────────────────────────────────────────────
# 1. Trend & Breadth (40 %)
# ─────────────────────────────────────────────────────────────────────────────

_TREND_EXPECTED = 6

def _score_trend_and_breadth(inputs: dict) -> dict:
    spy_1d    = _f(inputs.get("spy_change_1d"))
    qqq_1d    = _f(inputs.get("qqq_change_1d"))
    breadth_1d = _f(inputs.get("sector_breadth_1d"))
    breadth_7d = _f(inputs.get("sector_breadth_7d"))
    spx_7d     = _f(inputs.get("spx_return_7d"))
    spx_63d    = _f(inputs.get("spx_return_63d"))

    comp = {}
    signals: list[tuple[float, str]] = []
    n_avail = 0

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
        signals.append(sig)
        n_avail += 2
    elif spy_1d is not None:
        comp["spy_change_1d"] = round(spy_1d, 2)
        if spy_1d <= -2.0:        sig = (85, "major_selloff_spy_1d")
        elif spy_1d <= -1.0:      sig = (65, "moderate_selloff_spy_1d")
        elif spy_1d <= -0.3:      sig = (42, "mild_weakness_spy_1d")
        elif spy_1d >= 1.5:       sig = (10, "strong_spy_1d")
        elif spy_1d >= 0.5:       sig = (20, "positive_spy_1d")
        else:                     sig = (35, "flat_spy_1d")
        signals.append(sig)
        n_avail += 1

    if spx_7d is not None:
        comp["spx_return_7d"] = round(spx_7d, 2)
        if spx_7d <= -3.0:     sig = (80, "trend_bearish_7d")
        elif spx_7d <= -1.0:   sig = (60, "trend_weak_7d")
        elif spx_7d <= -0.2:   sig = (40, "trend_flat_7d")
        elif spx_7d >= 2.0:    sig = (15, "trend_bullish_7d")
        elif spx_7d >= 0.2:    sig = (25, "trend_positive_7d")
        else:                   sig = (30, "trend_flat_7d")
        signals.append(sig)
        n_avail += 1

    if spx_63d is not None:
        comp["spx_return_63d"] = round(spx_63d, 2)
        if spx_63d <= -8.0:    sig = (85, "trend_deeply_bearish_63d")
        elif spx_63d <= -3.0:  sig = (65, "trend_bearish_63d")
        elif spx_63d <= -0.5:  sig = (40, "trend_flat_63d")
        elif spx_63d >= 5.0:   sig = (15, "trend_bullish_63d")
        else:                   sig = (28, "trend_mild_positive_63d")
        signals.append(sig)
        n_avail += 1

    bd_signals: list[tuple[float, str]] = []
    if breadth_1d is not None:
        comp["breadth_1d"] = round(breadth_1d, 0)
        if breadth_1d < 30:       bd_signals.append((90, "breadth_collapse_1d"))
        elif breadth_1d < 40:     bd_signals.append((75, "breadth_weak_1d"))
        elif breadth_1d < 50:     bd_signals.append((55, "breadth_mixed_1d"))
        elif breadth_1d >= 70:    bd_signals.append((15, "breadth_strong_1d"))
        else:                     bd_signals.append((30, "breadth_neutral_1d"))
        n_avail += 1

    if breadth_7d is not None:
        comp["breadth_7d"] = round(breadth_7d, 0)
        if breadth_7d < 30:       bd_signals.append((85, "breadth_collapse_7d"))
        elif breadth_7d < 40:     bd_signals.append((70, "breadth_weak_7d"))
        elif breadth_7d < 50:     bd_signals.append((50, "breadth_mixed_7d"))
        elif breadth_7d >= 70:    bd_signals.append((15, "breadth_strong_7d"))
        else:                     bd_signals.append((30, "breadth_neutral_7d"))
        n_avail += 1

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

    equity_dir = _trend_direction_equity(inputs)
    breadth_dir = _breadth_direction(breadth_1d, breadth_7d)
    direction = _resolve_direction([equity_dir, breadth_dir])

    return {
        "risk_score":                 risk_score,
        "direction":                  direction,
        "confidence":                 _confidence(n_avail, _TREND_EXPECTED),
        "components":                 comp,
        "available_component_count":  n_avail,
        "expected_component_count":   _TREND_EXPECTED,
        "is_available":               n_avail >= max(1, _TREND_EXPECTED * 0.4),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 2. Volatility & Credit (25 %)
# ─────────────────────────────────────────────────────────────────────────────

_VOL_EXPECTED = 4

def _score_volatility_and_credit(inputs: dict) -> dict:
    vix       = _f(inputs.get("vix_current"))
    vix_chg   = _f(inputs.get("vix_change_1d"))
    hyg_chg   = _f(inputs.get("hyg_change_1d"))
    vix_7d_ret = _f(inputs.get("vix_return_7d"))

    comp = {}
    n_avail = 0

    vix_signals: list[tuple[float, str]] = []
    if vix is not None:
        comp["vix"] = round(vix, 2)
        if vix >= 30:        vix_signals.append((90, "vix_stress"))
        elif vix >= 25:      vix_signals.append((70, "vix_elevated_high"))
        elif vix >= 20:      vix_signals.append((50, "vix_elevated"))
        elif vix >= 15:      vix_signals.append((20, "vix_normal"))
        else:                vix_signals.append((10, "vix_complacency"))
        n_avail += 1

    if vix_chg is not None:
        comp["vix_change_1d"] = round(vix_chg, 2)
        if vix_chg >= 20:       vix_signals.append((95, "vix_spike_1d"))
        elif vix_chg >= 10:     vix_signals.append((75, "vix_surge_1d"))
        elif vix_chg >= 5:      vix_signals.append((55, "vix_uptick_1d"))
        elif vix_chg >= -5:     vix_signals.append((25, "vix_stable_1d"))
        else:                    vix_signals.append((10, "vix_drop_1d"))
        n_avail += 1

    credit_signals: list[tuple[float, str]] = []
    if hyg_chg is not None:
        comp["hyg_change_1d"] = round(hyg_chg, 2)
        if hyg_chg <= -2.0:       credit_signals.append((90, "credit_shock_1d"))
        elif hyg_chg <= -1.0:     credit_signals.append((70, "credit_weak_1d"))
        elif hyg_chg <= -0.3:     credit_signals.append((45, "credit_mild_weak_1d"))
        elif hyg_chg >= 0.5:      credit_signals.append((10, "credit_strong_1d"))
        else:                      credit_signals.append((25, "credit_stable_1d"))
        n_avail += 1

    if vix_7d_ret is not None:
        comp["vix_return_7d"] = round(vix_7d_ret, 2)
        n_avail += 1

    vix_max    = max((s[0] for s in vix_signals), default=50)
    credit_max = max((s[0] for s in credit_signals), default=50) if credit_signals else 50

    if vix_signals and not credit_signals:
        risk_score = vix_max
    elif credit_signals and not vix_signals:
        risk_score = credit_max
    else:
        risk_score = max(vix_max * 0.75, credit_max * 0.65)

    risk_score = round(risk_score)

    dir_signals: list[str | None] = []
    if vix_chg is not None:
        if vix_chg >= 10:          dir_signals.append("WORSENING")
        elif vix_chg >= 3:         dir_signals.append("WEAKENING")
        elif vix_chg >= -3:        dir_signals.append("STABLE")
        else:                      dir_signals.append("IMPROVING")
    if vix is not None:
        if vix >= 25 and (vix_chg is None or vix_chg > 0): dir_signals.append("WORSENING")
        elif vix >= 25:            dir_signals.append("STABLE")
        elif vix >= 20 and (vix_chg is None or vix_chg > 0): dir_signals.append("WEAKENING")
        elif vix >= 20:            dir_signals.append("STABLE")
    if hyg_chg is not None:
        if hyg_chg <= -1.0:        dir_signals.append("WORSENING")
        elif hyg_chg <= -0.3:      dir_signals.append("WEAKENING")
        elif hyg_chg >= 0.3:       dir_signals.append("IMPROVING")
        else:                      dir_signals.append("STABLE")

    direction = _resolve_direction(dir_signals)

    return {
        "risk_score":                 risk_score,
        "direction":                  direction,
        "confidence":                 _confidence(n_avail, _VOL_EXPECTED),
        "components":                 comp,
        "available_component_count":  n_avail,
        "expected_component_count":   _VOL_EXPECTED,
        "is_available":               n_avail >= max(1, _VOL_EXPECTED * 0.4),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 3. Rates & Dollar (20 %)
# ─────────────────────────────────────────────────────────────────────────────

_RATES_EXPECTED = 6

def _score_rates_and_dollar(inputs: dict) -> dict:
    us10y     = _f(inputs.get("us10y_yield"))
    dxy       = _f(inputs.get("dxy_price"))
    dxy_chg   = _f(inputs.get("dxy_change_1d"))
    us10y_chg_1d  = _f(inputs.get("us10y_change_1d_bps"))
    us10y_chg_5d  = _f(inputs.get("us10y_change_5d_bps"))
    us10y_chg_20d = _f(inputs.get("us10y_change_20d_bps"))

    comp = {}
    n_avail = 0

    if us10y is not None:
        comp["us10y"] = round(us10y, 3)
        n_avail += 1

    # ── Rate impulse signals ─────────────────────────────────────────────────
    rate_impulse_signals: list[tuple[float, str]] = []

    if us10y_chg_1d is not None:
        comp["us10y_change_1d_bps"] = round(us10y_chg_1d, 1)
        if us10y_chg_1d >= 10:      rate_impulse_signals.append((85, "10y_spike_1d"))
        elif us10y_chg_1d >= 5:     rate_impulse_signals.append((65, "10y_rising_1d"))
        elif us10y_chg_1d >= -5:    rate_impulse_signals.append((30, "10y_flat_1d"))
        else:                        rate_impulse_signals.append((10, "10y_falling_1d"))
        n_avail += 1

    if us10y_chg_5d is not None:
        comp["us10y_change_5d_bps"] = round(us10y_chg_5d, 1)
        if us10y_chg_5d >= 15:      rate_impulse_signals.append((85, "10y_spike_5d"))
        elif us10y_chg_5d >= 5:     rate_impulse_signals.append((60, "10y_rising_5d"))
        elif us10y_chg_5d >= -5:    rate_impulse_signals.append((28, "10y_flat_5d"))
        elif us10y_chg_5d >= -15:   rate_impulse_signals.append((12, "10y_falling_5d"))
        else:                        rate_impulse_signals.append((10, "10y_dropping_5d"))
        n_avail += 1

    if us10y_chg_20d is not None:
        comp["us10y_change_20d_bps"] = round(us10y_chg_20d, 1)
        n_avail += 1

    # ── Absolute level signal ────────────────────────────────────────────────
    level_signals: list[tuple[float, str]] = []
    if us10y is not None:
        if us10y >= 5.0:         level_signals.append((90, "10y_extreme_level"))
        elif us10y >= 4.75:      level_signals.append((70, "10y_elevated_level"))
        elif us10y >= 4.5:       level_signals.append((50, "10y_watch_level"))
        elif us10y >= 4.0:       level_signals.append((30, "10y_moderate_level"))
        else:                    level_signals.append((15, "10y_low_level"))

    dxy_signals: list[tuple[float, str]] = []
    if dxy_chg is not None:
        comp["dxy_change_1d"] = round(dxy_chg, 2)
        if dxy_chg >= 1.0:        dxy_signals.append((85, "dxy_spike"))
        elif dxy_chg >= 0.5:      dxy_signals.append((65, "dxy_strength"))
        elif dxy_chg >= 0.2:      dxy_signals.append((45, "dxy_mild_strength"))
        elif dxy_chg >= -0.2:     dxy_signals.append((25, "dxy_flat"))
        elif dxy_chg >= -0.5:     dxy_signals.append((15, "dxy_weakness"))
        else:                     dxy_signals.append((10, "dxy_heavy_weakness"))
        n_avail += 1

    if dxy is not None:
        comp["dxy"] = round(dxy, 3)
        n_avail += 1

    # ── Correlation-aware composite ──────────────────────────────────────────
    rate_level_max  = max((s[0] for s in level_signals), default=50)
    impulse_max     = max((s[0] for s in rate_impulse_signals), default=50) if rate_impulse_signals else rate_level_max
    dxy_max         = max((s[0] for s in dxy_signals), default=50) if dxy_signals else 50

    if rate_impulse_signals:
        if impulse_max <= 30:
            rate_combined = max(rate_level_max * 0.85, rate_level_max * 0.50 + impulse_max * 0.50)
        else:
            rate_combined = max(rate_level_max * 0.70, impulse_max * 0.90)
    else:
        rate_combined = rate_level_max

    risk_score = round(max(rate_combined * 0.78, dxy_max * 0.50))

    # ── Direction ────────────────────────────────────────────────────────────
    dir_signals: list[str | None] = []

    if us10y_chg_5d is not None:
        if us10y_chg_5d >= 15:          dir_signals.append("WORSENING")
        elif us10y_chg_5d >= 5:         dir_signals.append("WEAKENING")
        elif us10y_chg_5d >= -5:        dir_signals.append("STABLE")
        else:                           dir_signals.append("IMPROVING")

    if us10y is not None and us10y_chg_5d is None:
        if us10y >= 5.0:                dir_signals.append("WORSENING")
        elif us10y >= 4.75:             dir_signals.append("WEAKENING")
        elif us10y >= 4.5:              dir_signals.append("WEAKENING")
        else:                           dir_signals.append("STABLE")

    if us10y_chg_20d is not None:
        if us10y_chg_20d >= 20 and us10y_chg_5d is not None and us10y_chg_5d > 0:
            if _resolve_direction(dir_signals) in ("WEAKENING",):
                dir_signals.append("WORSENING")
        if us10y_chg_20d <= -10 and us10y_chg_5d is not None and us10y_chg_5d < 0:
            if _resolve_direction(dir_signals) in ("WEAKENING", "WORSENING"):
                dir_signals.append("STABLE")

    if dxy_chg is not None:
        if dxy_chg >= 0.5:              dir_signals.append("WORSENING")
        elif dxy_chg >= 0.2:            dir_signals.append("WEAKENING")
        elif dxy_chg >= -0.2:           dir_signals.append("STABLE")
        else:                           dir_signals.append("IMPROVING")

    direction = _resolve_direction(dir_signals)

    return {
        "risk_score":                 risk_score,
        "direction":                  direction,
        "confidence":                 _confidence(n_avail, _RATES_EXPECTED),
        "components":                 comp,
        "available_component_count":  n_avail,
        "expected_component_count":   _RATES_EXPECTED,
        "is_available":               n_avail >= max(1, _RATES_EXPECTED * 0.4),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 4. Leadership & Cross-Asset (15 %)
# ─────────────────────────────────────────────────────────────────────────────

_LEAD_EXPECTED = 3

def _score_leadership_and_cross_asset(inputs: dict) -> dict:
    btc_chg = _f(inputs.get("btc_change_24h"))
    cyc_vs_def = _f(inputs.get("cyclical_vs_defensive_spread"))
    posture = inputs.get("market_posture") or ""

    comp = {}
    n_avail = 0

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
        n_avail += 1

    rot_signals: list[tuple[float, str]] = []
    if cyc_vs_def is not None:
        comp["cyclical_vs_defensive_spread"] = round(cyc_vs_def, 2)
        if cyc_vs_def <= -3.0:    rot_signals.append((80, "heavy_defensive_rotation"))
        elif cyc_vs_def <= -1.0:  rot_signals.append((60, "defensive_rotation"))
        elif cyc_vs_def <= 1.0:   rot_signals.append((30, "neutral_rotation"))
        elif cyc_vs_def >= 3.0:   rot_signals.append((10, "risk_on_rotation"))
        else:                     rot_signals.append((20, "cyclical_leadership"))
        n_avail += 1
    if posture:
        comp["market_posture"] = posture
        if posture.lower() == "risk-off":
            rot_signals.append((70, "posture_risk_off"))
        elif posture.lower() == "risk-on":
            rot_signals.append((15, "posture_risk_on"))
        n_avail += 1

    btc_max  = max((s[0] for s in btc_signals), default=50)
    rot_max  = max((s[0] for s in rot_signals), default=50) if rot_signals else 50

    if btc_signals and not rot_signals:
        risk_score = btc_max
    elif rot_signals and not btc_signals:
        risk_score = rot_max
    else:
        risk_score = max(btc_max * 0.60, rot_max * 0.55)

    risk_score = round(risk_score)

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

    return {
        "risk_score":                 risk_score,
        "direction":                  direction,
        "confidence":                 _confidence(n_avail, _LEAD_EXPECTED),
        "components":                 comp,
        "available_component_count":  n_avail,
        "expected_component_count":   _LEAD_EXPECTED,
        "is_available":               n_avail >= max(1, _LEAD_EXPECTED * 0.4),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Pillar diagnostics — derived from existing scoring logic
# ─────────────────────────────────────────────────────────────────────────────

_SIGNAL_STRENGTH = {
    (0, 15):   "STRONG_BULLISH",
    (15, 30):  "MODERATE",
    (30, 45):  "MILD",
    (45, 60):  "MILD_NEGATIVE",
    (60, 80):  "MODERATE_NEGATIVE",
    (80, 101): "STRONG_BEARISH",
}

def _signal_strength(risk_contribution: float) -> str:
    for (lo, hi), label in _SIGNAL_STRENGTH.items():
        if lo <= risk_contribution < hi:
            return label
    return "MODERATE"

def _message_strength(risk_contribution: float) -> str:
    if risk_contribution <= 20:           return "STRONG"
    elif risk_contribution <= 35:         return "MODERATE"
    elif risk_contribution <= 50:         return "MILD"
    elif risk_contribution <= 70:         return "MODERATE"
    else:                                  return "STRONG"

def _enrich_pillar_diagnostics(pillars: dict) -> None:

    # ── Trend & Breadth diagnostics ─────────────────────────────────────
    tb_comp = pillars["trend_and_breadth"].get("components", {})
    tb_support: list[dict] = []
    tb_risk: list[dict] = []
    tb_missing: list[str] = []
    tb_improve: list[str] = []
    tb_worsen: list[str] = []

    spy_1d = tb_comp.get("spy_change_1d")
    qqq_1d = tb_comp.get("qqq_change_1d")
    eq_avg = tb_comp.get("equity_1d_avg")
    spx_7d = tb_comp.get("spx_return_7d")
    spx_63d = tb_comp.get("spx_return_63d")
    breadth_1d = tb_comp.get("breadth_1d")
    breadth_7d = tb_comp.get("breadth_7d")

    if eq_avg is not None:
        if eq_avg >= 1.5:
            tb_support.append({"key": "equity_1d_strong", "label": "SPY/QQQ 1D", "value": round(eq_avg, 2), "unit": "%", "message": f"SPY and QQQ averaged {eq_avg:+.1f}% in the latest session.", "strength": "MODERATE"})
        elif eq_avg >= 0.5:
            tb_support.append({"key": "equity_1d_positive", "label": "SPY/QQQ 1D", "value": round(eq_avg, 2), "unit": "%", "message": f"SPY and QQQ averaged {eq_avg:+.1f}% — constructive session.", "strength": "MILD"})
        elif eq_avg <= -2.0:
            tb_risk.append({"key": "equity_1d_major_selloff", "label": "SPY/QQQ 1D", "value": round(eq_avg, 2), "unit": "%", "message": f"Major equity selloff: SPY/QQQ averaged {eq_avg:+.1f}%.", "strength": "STRONG"})
        elif eq_avg <= -1.0:
            tb_risk.append({"key": "equity_1d_selloff", "label": "SPY/QQQ 1D", "value": round(eq_avg, 2), "unit": "%", "message": f"Equities under pressure: SPY/QQQ averaged {eq_avg:+.1f}%.", "strength": "MODERATE"})
        elif eq_avg <= -0.3:
            tb_risk.append({"key": "equity_1d_weak", "label": "SPY/QQQ 1D", "value": round(eq_avg, 2), "unit": "%", "message": f"Mild weakness: SPY/QQQ averaged {eq_avg:+.1f}%.", "strength": "MILD"})

    if breadth_1d is not None:
        if breadth_1d >= 70:
            tb_support.append({"key": "breadth_strong_1d", "label": "Breadth 1D", "value": round(breadth_1d, 0), "unit": "%", "message": f"{breadth_1d:.0f}% of sectors advanced — broad participation.", "strength": "STRONG"})
        elif breadth_1d < 40:
            tb_risk.append({"key": "breadth_weak_1d", "label": "Breadth 1D", "value": round(breadth_1d, 0), "unit": "%", "message": f"Only {breadth_1d:.0f}% of sectors advanced — narrow participation.", "strength": "STRONG" if breadth_1d < 30 else "MODERATE"})
        elif breadth_1d < 50:
            tb_risk.append({"key": "breadth_mixed_1d", "label": "Breadth 1D", "value": round(breadth_1d, 0), "unit": "%", "message": f"Sector breadth is mixed at {breadth_1d:.0f}%.", "strength": "MILD"})

    if breadth_7d is not None:
        if breadth_7d < 40:
            tb_risk.append({"key": "breadth_weak_7d", "label": "Breadth 7D", "value": round(breadth_7d, 0), "unit": "%", "message": f"7-day breadth is narrow at {breadth_7d:.0f}%.", "strength": "MODERATE"})

    if spx_7d is not None:
        if spx_7d >= 2.0:
            tb_support.append({"key": "spx_7d_strong", "label": "SPX 7D", "value": round(spx_7d, 2), "unit": "%", "message": f"SPX gained {spx_7d:+.1f}% over seven sessions.", "strength": "STRONG"})
        elif spx_7d <= -3.0:
            tb_risk.append({"key": "spx_7d_weak", "label": "SPX 7D", "value": round(spx_7d, 2), "unit": "%", "message": f"SPX fell {abs(spx_7d):.1f}% over seven sessions.", "strength": "STRONG"})
        elif spx_7d <= -1.0:
            tb_risk.append({"key": "spx_7d_mild_weak", "label": "SPX 7D", "value": round(spx_7d, 2), "unit": "%", "message": f"SPX 7-day return is negative at {spx_7d:+.1f}%.", "strength": "MODERATE"})

    if spx_63d is not None:
        if spx_63d >= 5.0:
            tb_support.append({"key": "spx_63d_strong", "label": "SPX 3M", "value": round(spx_63d, 2), "unit": "%", "message": f"SPX 3-month return is strong at {spx_63d:+.1f}%.", "strength": "STRONG"})
        elif spx_63d <= -3.0:
            tb_risk.append({"key": "spx_63d_weak", "label": "SPX 3M", "value": round(spx_63d, 2), "unit": "%", "message": f"The three-month trend remains negative at {spx_63d:+.1f}%.", "strength": "STRONG"})
        elif spx_63d <= -0.5:
            tb_risk.append({"key": "spx_63d_flat", "label": "SPX 3M", "value": round(spx_63d, 2), "unit": "%", "message": f"The three-month return is mildly negative at {spx_63d:+.1f}%.", "strength": "MODERATE"})

    # Expected components
    if spy_1d is None:    tb_missing.append("spy_change_1d")
    if qqq_1d is None:    tb_missing.append("qqq_change_1d")
    if spx_7d is None:    tb_missing.append("spx_return_7d")
    if spx_63d is None:   tb_missing.append("spx_return_63d")
    if breadth_1d is None: tb_missing.append("sector_breadth_1d")
    if breadth_7d is None: tb_missing.append("sector_breadth_7d")

    # Improve/worsen from actual thresholds used in scoring
    if breadth_1d is not None and breadth_1d < 55:
        tb_improve.append(f"Breadth rises above 55%.")
    if spx_63d is not None and spx_63d < 0:
        tb_improve.append("SPX 3-month return turns positive.")
    if breadth_1d is not None:
        tb_worsen.append(f"Breadth falls below 45% — the canonical weak-participation threshold.")
    if spx_63d is not None and spx_63d > -3.0:
        tb_worsen.append("SPX 3-month return falls below -3%.")

    # Interpretation
    tb_pos_count = len(tb_support)
    tb_neg_count = len(tb_risk)
    interpretation_parts: list[str] = []

    if tb_support and tb_risk:
        pos_label = tb_support[0].get("label", "") if tb_support else ""
        neg_label = tb_risk[0].get("label", "") if tb_risk else ""
        interpretation_parts.append(f"Latest-session participation is constructive, but the longer-term trend remains weak, leaving Trend & Breadth in a worsening state." if breadth_1d is not None and breadth_1d >= 50 and spx_63d is not None and spx_63d < 0 else f"Positive {pos_label} signals are offset by {neg_label} risk, leaving Trend & Breadth mixed.")
    elif tb_support:
        interpretation_parts.append("Trend and breadth signals are supportive across timeframes.")
    elif tb_risk:
        dominant_risk = tb_risk[0].get("message", "").rstrip(".")
        interpretation_parts.append(f"Risk dominates: {dominant_risk}.")
    else:
        interpretation_parts.append("Trend & Breadth data is insufficient for interpretation.")

    pillars["trend_and_breadth"]["interpretation"] = " ".join(interpretation_parts)
    pillars["trend_and_breadth"]["supportive_signals"] = tb_support
    pillars["trend_and_breadth"]["risk_signals"] = tb_risk
    pillars["trend_and_breadth"]["missing_inputs"] = tb_missing
    pillars["trend_and_breadth"]["conditions_to_improve"] = tb_improve
    pillars["trend_and_breadth"]["conditions_to_worsen"] = tb_worsen

    # ── Volatility & Credit diagnostics ───────────────────────────────────
    vc_comp = pillars["volatility_and_credit"].get("components", {})
    vc_sup: list[dict] = []
    vc_risk: list[dict] = []
    vc_miss: list[str] = []
    vc_imp: list[str] = []
    vc_wrs: list[str] = []

    vix_val = vc_comp.get("vix")
    vix_chg_val = vc_comp.get("vix_change_1d")
    hyg_val = vc_comp.get("hyg_change_1d")
    vix_7d = vc_comp.get("vix_return_7d")

    if vix_val is not None:
        if vix_val >= 30:
            vc_risk.append({"key": "vix_stress", "label": "VIX", "value": vix_val, "unit": "", "message": f"VIX at {vix_val:.1f} — stress zone.", "strength": "STRONG"})
            vc_wrs.append("VIX rises above 30 — the canonical stress threshold.")
        elif vix_val >= 25:
            vc_risk.append({"key": "vix_high", "label": "VIX", "value": vix_val, "unit": "", "message": f"VIX elevated at {vix_val:.1f}.", "strength": "MODERATE"})
        elif vix_val >= 20:
            vc_risk.append({"key": "vix_elevated", "label": "VIX", "value": vix_val, "unit": "", "message": f"VIX is above the 20 threshold at {vix_val:.1f}.", "strength": "MILD"})
        else:
            vc_sup.append({"key": "vix_low", "label": "VIX", "value": vix_val, "unit": "", "message": f"VIX is contained at {vix_val:.1f}.", "strength": "STRONG" if vix_val < 15 else "MODERATE"})

    if vix_chg_val is not None:
        if vix_chg_val >= 5:
            vc_risk.append({"key": "vix_spiking", "label": "VIX 1D", "value": round(vix_chg_val, 2), "unit": "%", "message": f"VIX jumped {vix_chg_val:+.1f}% in the latest session.", "strength": "STRONG"})
            vc_wrs.append("VIX 1D change exceeds +20% — the canonical spike threshold.")
        elif vix_chg_val < -3:
            vc_sup.append({"key": "vix_falling", "label": "VIX 1D", "value": round(vix_chg_val, 2), "unit": "%", "message": f"VIX fell {abs(vix_chg_val):.1f}% in the latest session.", "strength": "MODERATE"})
        elif vix_chg_val > 0:
            vc_risk.append({"key": "vix_rising", "label": "VIX 1D", "value": round(vix_chg_val, 2), "unit": "%", "message": f"VIX rose {vix_chg_val:+.1f}%, indicating slight stress increase.", "strength": "MILD"})

    if hyg_val is not None:
        if hyg_val >= 0.5:
            vc_sup.append({"key": "hyg_strong", "label": "HYG 1D", "value": round(hyg_val, 2), "unit": "%", "message": f"HYG gained {hyg_val:+.2f}%, showing no current credit stress.", "strength": "STRONG"})
        elif hyg_val <= -1.0:
            vc_risk.append({"key": "hyg_weak", "label": "HYG 1D", "value": round(hyg_val, 2), "unit": "%", "message": f"HYG fell {abs(hyg_val):.1f}% — credit stress signal.", "strength": "STRONG"})
            vc_wrs.append("HYG falls below -1.0% — the canonical credit-stress threshold.")
        elif hyg_val <= -0.3:
            vc_risk.append({"key": "hyg_mild_weak", "label": "HYG 1D", "value": round(hyg_val, 2), "unit": "%", "message": f"HYG mildly negative at {hyg_val:+.2f}%.", "strength": "MODERATE"})

    if vix_val is not None and vix_val < 20:
        vc_imp.append(f"VIX remains below 20 — current conditions are already benign.")
    if vix_val is not None and vix_val >= 20:
        vc_imp.append(f"VIX falls below 20.")
    if hyg_val is not None and hyg_val <= 0:
        vc_imp.append("HYG returns above the credit-warning threshold.")

    if vix_val is None:  vc_miss.append("vix_current")
    if vix_chg_val is None: vc_miss.append("vix_change_1d")
    if hyg_val is None:  vc_miss.append("hyg_change_1d")
    if vix_7d is None:   vc_miss.append("vix_return_7d")

    vc_pos = len(vc_sup)
    vc_neg = len(vc_risk)
    if vc_pos > vc_neg:
        pillars["volatility_and_credit"]["interpretation"] = "Volatility is contained and credit is stable, indicating limited stress."
    elif vc_neg > vc_pos:
        pillars["volatility_and_credit"]["interpretation"] = "Volatility and credit signals are indicating elevated stress."
    else:
        pillars["volatility_and_credit"]["interpretation"] = "Volatility and credit signals are mixed."

    pillars["volatility_and_credit"]["supportive_signals"] = vc_sup
    pillars["volatility_and_credit"]["risk_signals"] = vc_risk
    pillars["volatility_and_credit"]["missing_inputs"] = vc_miss
    pillars["volatility_and_credit"]["conditions_to_improve"] = vc_imp
    pillars["volatility_and_credit"]["conditions_to_worsen"] = vc_wrs

    # ── Rates & Dollar diagnostics ────────────────────────────────────────
    rd_comp = pillars["rates_and_dollar"].get("components", {})
    rd_sup: list[dict] = []
    rd_risk: list[dict] = []
    rd_miss: list[str] = []
    rd_imp: list[str] = []
    rd_wrs: list[str] = []

    us10y_val_rd = rd_comp.get("us10y")
    chg_1d = rd_comp.get("us10y_change_1d_bps")
    chg_5d_val = rd_comp.get("us10y_change_5d_bps")
    chg_20d = rd_comp.get("us10y_change_20d_bps")
    dxy_val_rd = rd_comp.get("dxy")
    dxy_chg_val = rd_comp.get("dxy_change_1d")

    if chg_5d_val is not None:
        if chg_5d_val >= 15:
            rd_risk.append({"key": "10y_spike_5d", "label": "10Y 5D", "value": round(chg_5d_val, 1), "unit": "bps", "message": f"10Y has surged {chg_5d_val:+.0f} bps over five sessions — increasing pressure on long-duration assets.", "strength": "STRONG"})
            rd_wrs.append("10Y 5-session increase exceeds +15 bps — the canonical pressure threshold.")
        elif chg_5d_val >= 5:
            rd_risk.append({"key": "10y_rising_5d", "label": "10Y 5D", "value": round(chg_5d_val, 1), "unit": "bps", "message": f"10Y has risen {chg_5d_val:+.0f} bps over five sessions.", "strength": "MODERATE"})
            rd_wrs.append("10Y 5-session increase exceeds +5 bps.")
        elif chg_5d_val <= -10:
            rd_sup.append({"key": "10y_falling_5d", "label": "10Y 5D", "value": round(chg_5d_val, 1), "unit": "bps", "message": f"10Y has fallen {abs(chg_5d_val):.0f} bps over five sessions — rate pressure easing.", "strength": "STRONG"})
            rd_imp.append("10Y 5-session pressure remains below +5 bps.")
        elif chg_5d_val <= -5:
            rd_sup.append({"key": "10y_easing_5d", "label": "10Y 5D", "value": round(chg_5d_val, 1), "unit": "bps", "message": f"10Y has eased {abs(chg_5d_val):.0f} bps over five sessions.", "strength": "MODERATE"})

    if us10y_val_rd is not None:
        if us10y_val_rd >= 4.75:
            rd_risk.append({"key": "10y_elevated", "label": "10Y Level", "value": us10y_val_rd, "unit": "%", "message": f"10Y remains restrictive at {us10y_val_rd:.2f}%.", "strength": "STRONG" if us10y_val_rd >= 5.0 else "MODERATE"})
            if chg_5d_val is not None and chg_5d_val <= 0:
                rd_imp.append("10Y yield falls below 4.75%.")
        elif us10y_val_rd >= 4.5:
            rd_risk.append({"key": "10y_watch", "label": "10Y Level", "value": us10y_val_rd, "unit": "%", "message": f"10Y is in the watch zone at {us10y_val_rd:.2f}%.", "strength": "MILD"})
        else:
            rd_sup.append({"key": "10y_moderate", "label": "10Y Level", "value": us10y_val_rd, "unit": "%", "message": f"10Y is manageable at {us10y_val_rd:.2f}%.", "strength": "STRONG"})

    if dxy_chg_val is not None:
        if dxy_chg_val >= 0.5:
            rd_risk.append({"key": "dxy_strong", "label": "DXY 1D", "value": round(dxy_chg_val, 2), "unit": "%", "message": f"DXY gained {dxy_chg_val:+.2f}% — dollar strength headwind.", "strength": "STRONG"})
            rd_wrs.append("DXY 1D increase exceeds +0.5% — the canonical pressure threshold.")
        elif dxy_chg_val <= -0.3:
            rd_sup.append({"key": "dxy_weak", "label": "DXY 1D", "value": round(dxy_chg_val, 2), "unit": "%", "message": f"DXY fell {abs(dxy_chg_val):.2f}% — dollar weakness is supportive.", "strength": "MODERATE"})

    if dxy_val_rd is not None:
        pass  # level alone is not a strong signal; present in components

    if us10y_val_rd is None:  rd_miss.append("us10y_yield")
    if chg_1d is None:        rd_miss.append("us10y_change_1d_bps")
    if chg_5d_val is None:    rd_miss.append("us10y_change_5d_bps")
    if chg_20d is None:       rd_miss.append("us10y_change_20d_bps")
    if dxy_chg_val is None:   rd_miss.append("dxy_change_1d")
    if dxy_val_rd is None:    rd_miss.append("dxy_price")

    # Interpretation with level + direction
    rd_parts: list[str] = []
    if us10y_val_rd is not None:
        if chg_5d_val is not None and chg_5d_val < -5:
            rd_parts.append(f"10Y remains restrictive at {us10y_val_rd:.2f}%, but five-session pressure is easing.")
        elif chg_5d_val is not None and chg_5d_val > 5:
            rd_parts.append(f"10Y is {us10y_val_rd:.2f}% and has risen {chg_5d_val:+.0f} bps over five sessions, increasing pressure on long-duration assets.")
        elif us10y_val_rd >= 4.75:
            rd_parts.append(f"10Y is elevated at {us10y_val_rd:.2f}% with flat short-term direction.")
        else:
            rd_parts.append(f"10Y is at {us10y_val_rd:.2f}%.")
    if dxy_chg_val is not None and dxy_chg_val >= 0.3:
        rd_parts.append(f"DXY rose {dxy_chg_val:+.2f}%, adding pressure.")

    if not rd_parts:
        rd_parts.append("Rates & Dollar data is insufficient for interpretation.")

    pillars["rates_and_dollar"]["interpretation"] = " ".join(rd_parts)
    pillars["rates_and_dollar"]["supportive_signals"] = rd_sup
    pillars["rates_and_dollar"]["risk_signals"] = rd_risk
    pillars["rates_and_dollar"]["missing_inputs"] = rd_miss
    pillars["rates_and_dollar"]["conditions_to_improve"] = rd_imp
    pillars["rates_and_dollar"]["conditions_to_worsen"] = rd_wrs

    # ── Leadership & Cross-Asset diagnostics ──────────────────────────────
    lc_comp = pillars["leadership_and_cross_asset"].get("components", {})
    lc_sup: list[dict] = []
    lc_risk: list[dict] = []
    lc_miss: list[str] = []
    lc_imp: list[str] = []
    lc_wrs: list[str] = []

    btc_chg_val = lc_comp.get("btc_change_24h")
    cvd_val = lc_comp.get("cyclical_vs_defensive_spread")
    posture_val = lc_comp.get("market_posture")

    if btc_chg_val is not None:
        if btc_chg_val <= -5.0:
            lc_risk.append({"key": "btc_risk_off", "label": "BTC 24H", "value": round(btc_chg_val, 2), "unit": "%", "message": f"BTC fell {abs(btc_chg_val):.1f}% — risk-off signal.", "strength": "STRONG"})
        elif btc_chg_val >= 3.0:
            lc_sup.append({"key": "btc_risk_on", "label": "BTC 24H", "value": round(btc_chg_val, 2), "unit": "%", "message": f"BTC gained {btc_chg_val:+.1f}% — risk-on appetite.", "strength": "STRONG"})
    else:
        lc_miss.append("btc_change_24h")

    if cvd_val is not None:
        if cvd_val <= -3.0:
            lc_risk.append({"key": "defensive_rotation", "label": "Cyclical vs Defensive", "value": round(cvd_val, 2), "unit": "%", "message": f"Heavy defensive rotation: cyclicals-vs-defensive spread is {cvd_val:+.1f}%.", "strength": "STRONG"})
            lc_wrs.append("Cyclicals begin materially underperforming defensives (spread below -3%).")
        elif cvd_val <= -1.0:
            lc_risk.append({"key": "mild_defensive", "label": "Cyclical vs Defensive", "value": round(cvd_val, 2), "unit": "%", "message": f"Defensives are leading: spread is {cvd_val:+.1f}%.", "strength": "MODERATE"})
        elif cvd_val >= 2.0:
            lc_sup.append({"key": "risk_on_rotation", "label": "Cyclical vs Defensive", "value": round(cvd_val, 2), "unit": "%", "message": f"Cyclicals leading: spread is {cvd_val:+.1f}% — risk-on rotation.", "strength": "STRONG"})
        elif cvd_val >= 0.5:
            lc_sup.append({"key": "cyclical_mild", "label": "Cyclical vs Defensive", "value": round(cvd_val, 2), "unit": "%", "message": f"Cyclicals are modestly outperforming defensives at {cvd_val:+.1f}%.", "strength": "MILD"})
    else:
        lc_miss.append("cyclical_vs_defensive_spread")

    if not posture_val:
        lc_miss.append("market_posture")

    # Data status and confirmation
    lc_n_avail = pillars["leadership_and_cross_asset"].get("available_component_count", 0)
    lc_n_exp = pillars["leadership_and_cross_asset"].get("expected_component_count", 3)

    if lc_n_avail == lc_n_exp:
        data_status = "COMPLETE"
    elif lc_n_avail > 0:
        data_status = "PARTIAL"
    else:
        data_status = "UNAVAILABLE"

    pillars["leadership_and_cross_asset"]["data_status"] = data_status

    # Confirmation status
    if lc_risk and not lc_sup:
        confirmation = "UNCONFIRMED"
    elif lc_sup and not lc_risk:
        confirmation = "CONFIRMED"
    elif lc_sup and lc_risk:
        confirmation = "MIXED"
    else:
        confirmation = "UNCONFIRMED"
    pillars["leadership_and_cross_asset"]["confirmation_status"] = confirmation

    # Interpretation
    lc_parts: list[str] = []
    if data_status == "PARTIAL":
        lc_parts.append(f"Only {lc_n_avail} of {lc_n_exp} inputs available")
        if lc_miss:
            lc_parts.append(f"(missing: {', '.join(lc_miss)}).")
    if cvd_val is not None and cvd_val > 0:
        lc_parts.append("Cyclicals are outperforming defensives — mildly supportive.")
    elif cvd_val is not None:
        lc_parts.append("Defensives are leading — cautious posture.")
    if posture_val:
        lc_parts.append(f"Market posture is {posture_val}.")
    if btc_chg_val is None:
        lc_parts.append("BTC confirmation is unavailable.")

    if not lc_parts:
        lc_parts.append("Leadership & Cross-Asset data is insufficient.")

    pillars["leadership_and_cross_asset"]["interpretation"] = " ".join(lc_parts)
    pillars["leadership_and_cross_asset"]["supportive_signals"] = lc_sup
    pillars["leadership_and_cross_asset"]["risk_signals"] = lc_risk
    pillars["leadership_and_cross_asset"]["missing_inputs"] = lc_miss
    pillars["leadership_and_cross_asset"]["conditions_to_improve"] = lc_imp
    pillars["leadership_and_cross_asset"]["conditions_to_worsen"] = lc_wrs


# ─────────────────────────────────────────────────────────────────────────────
# Overall regime logic
# ─────────────────────────────────────────────────────────────────────────────

def _risk_level_from_score(score: int, pillar_avail: dict[str, bool]) -> str:
    n_avail = sum(1 for v in pillar_avail.values() if v)
    if n_avail < 2:
        return "MODERATE"
    if score >= 80:     return "EXTREME"
    elif score >= 65:   return "HIGH"
    elif score >= 45:   return "ELEVATED"
    elif score >= 25:   return "MODERATE"
    else:               return "LOW"


def _compute_regime_direction(pillars: dict, risk_level: str, pillar_avail: dict[str, bool]) -> str:
    dirs = [
        p.get("direction")
        for name, p in pillars.items()
        if p.get("direction") and p["direction"] != "UNKNOWN" and pillar_avail.get(name, False)
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


def _compute_trade_bias(risk_level: str, regime_direction: str, pillar_avail: dict[str, bool]) -> str:
    n_avail = sum(1 for v in pillar_avail.values() if v)
    if n_avail < 2 or regime_direction == "UNKNOWN":
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
    return base


def _apply_event_sizing(base: str, inputs: dict) -> str:
    ev = inputs.get("has_upcoming_high_impact_event", False)
    if not ev:
        return base
    sizing_upgrade = {
        "normal":          "selective",
        "selective":       "half-size",
        "half-size":       "preserve capital",
        "preserve capital": "preserve capital",
    }
    return sizing_upgrade.get(base, base)


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


def _build_one_line(risk_level: str, direction: str, trade_bias: str, driver: str, pillars: dict, pillar_avail: dict[str, bool]) -> str:
    n_avail = sum(1 for v in pillar_avail.values() if v)
    if n_avail < 2:
        return "Insufficient data — no directional conclusion should be drawn"

    parts: list[str] = []

    if risk_level in ("EXTREME", "HIGH"):
        parts.append(f"Risk level {risk_level} — conditions {direction.lower()}")
        if growth_weak := _growth_messaging(pillars):
            parts.append(growth_weak)
    elif risk_level == "ELEVATED":
        parts.append(f"Risk elevated, {direction.lower()}")
        if driver == "rate_and_dollar_pressure":
            rd = pillars.get("rates_and_dollar", {}).get("components", {})
            chg_5d = rd.get("us10y_change_5d_bps")
            level = rd.get("us10y")
            if chg_5d is not None and chg_5d < -5 and level is not None and level >= 4.5:
                parts.append(f"restrictive at {level:.2f}% but rate pressure easing")
            elif chg_5d is not None and chg_5d > 5 and level is not None:
                parts.append(f"rising rapidly (+{chg_5d:.0f} bps/5d)")
            else:
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
        conditions.append("Breadth rises above 50%")
    spx_7d = tb_comp.get("spx_return_7d")
    if spx_7d is not None and spx_7d < -1.0:
        conditions.append("SPX 7-day return turns positive")
    spx_63d = tb_comp.get("spx_return_63d")
    if spx_63d is not None and spx_63d < -0.5:
        conditions.append("SPX 3-month return turns positive")

    vc = pillars.get("volatility_and_credit", {})
    vc_comp = vc.get("components", {})
    if vc_comp.get("vix", 100) >= 25:
        conditions.append("VIX falls below 25")
    elif vc_comp.get("vix", 100) >= 20:
        conditions.append("VIX falls below 20")
    vix_chg = vc_comp.get("vix_change_1d")
    if vix_chg is not None and vix_chg >= 5:
        conditions.append("VIX 1D change falls below 5%")
    if vc_comp.get("hyg_change_1d", 0) is not None and vc_comp.get("hyg_change_1d", 0) <= -0.5:
        conditions.append("HYG returns above the credit-warning threshold")

    rd = pillars.get("rates_and_dollar", {})
    rd_comp = rd.get("components", {})
    us10y = rd_comp.get("us10y")
    chg_5d = rd_comp.get("us10y_change_5d_bps")
    if us10y is not None and us10y >= 4.75 and chg_5d is not None and chg_5d <= 0:
        conditions.append("10Y yield falls below 4.75%")
    elif chg_5d is not None and chg_5d > 5:
        conditions.append("10Y 5-session trend reverses (bps change turns negative)")
    dxy_chg = rd_comp.get("dxy_change_1d")
    if dxy_chg is not None and dxy_chg >= 0.5:
        conditions.append("DXY 1D strength reverses")

    lc = pillars.get("leadership_and_cross_asset", {})
    lc_comp = lc.get("components", {})
    cvd = lc_comp.get("cyclical_vs_defensive_spread")
    if cvd is not None and cvd <= -1.0:
        conditions.append("Cyclicals resume leadership over defensives")

    return conditions[:5]


def _compute_event_overlay(inputs: dict, risk_level: str, base_size: str, final_size: str) -> dict:
    has_event = inputs.get("has_upcoming_high_impact_event", False)
    days = inputs.get("days_until_next_event")
    next_title = inputs.get("next_event_title")

    if not has_event:
        return {
            "active":                             False,
            "severity":                           "NONE",
            "next_event":                         None,
            "days_until_event":                   None,
            "position_size_impact":               None,
            "contributes_to_directional_score":   False,
            "position_size_adjustment_applied":   False,
            "pre_event_size":                     base_size,
            "post_event_size":                    base_size,
        }

    severity = "HIGH" if (days is not None and days <= 2) else "MODERATE"

    adjustment_applied = final_size != base_size

    if adjustment_applied:
        impact = f"{base_size} reduced to {final_size} ahead of {next_title or 'event'}"
    else:
        impact = "No additional sizing reduction"

    return {
        "active":                             True,
        "severity":                           severity,
        "next_event":                         next_title,
        "days_until_event":                   days,
        "position_size_impact":               impact,
        "contributes_to_directional_score":   False,
        "position_size_adjustment_applied":   adjustment_applied,
        "pre_event_size":                     base_size,
        "post_event_size":                    final_size,
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


def _resolve_direction(dirs: list[str | None]) -> str:
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
