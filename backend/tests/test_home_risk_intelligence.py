"""
Unit tests for swing_regime_service — pure deterministic scoring engine.

Mocked-data-only — no network, no DB, no provider calls.
"""
from __future__ import annotations

import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parent.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from services.swing_regime_service import (
    assess_swing_regime,
    _score_trend_and_breadth,
    _score_volatility_and_credit,
    _score_rates_and_dollar,
    _score_leadership_and_cross_asset,
    _compute_overall_risk_score,
    _risk_level_from_score,
    _compute_regime_direction,
    _compute_trade_bias,
    _compute_position_size_hint,
    _compute_dominant_driver,
    _compute_event_overlay,
)


# ── Test helpers ──────────────────────────────────────────────────────────────

def bare_minimal_inputs(**overrides) -> dict:
    defaults = {
        "spy_change_1d":                 0.0,
        "qqq_change_1d":                 0.0,
        "sector_breadth_1d":             50.0,
        "sector_breadth_7d":             50.0,
        "spx_return_7d":                 0.0,
        "spx_return_63d":                0.0,
        "vix_current":                   17.0,
        "vix_change_1d":                 0.0,
        "vix_return_7d":                 None,
        "hyg_change_1d":                 0.0,
        "us10y_yield":                   4.2,
        "dxy_price":                     104.0,
        "dxy_change_1d":                 0.0,
        "btc_change_24h":                0.0,
        "cyclical_vs_defensive_spread":  0.0,
        "market_posture":                "Neutral",
        "has_upcoming_high_impact_event": False,
        "days_until_next_event":         None,
        "next_event_title":              None,
    }
    defaults.update(overrides)
    return defaults


# ── Full regime output tests ──────────────────────────────────────────────────

def test_low_risk_calm_environment() -> None:
    inputs = bare_minimal_inputs(
        spy_change_1d=0.5,
        qqq_change_1d=0.8,
        sector_breadth_1d=65.0,
        sector_breadth_7d=60.0,
        spx_return_7d=2.0,
        spx_return_63d=6.0,
        vix_current=14.0,
        btc_change_24h=2.0,
        cyclical_vs_defensive_spread=2.5,
    )
    result = assess_swing_regime(inputs)
    assert result["risk_level"] == "LOW", f"Expected LOW, got {result['risk_level']}"
    assert result["trade_bias"] == "LONG", f"Expected LONG, got {result['trade_bias']}"
    assert result["position_size_hint"] == "normal"
    assert result["risk_score"] < 25
    assert result["regime_direction"] in ("STABLE", "IMPROVING")
    print("test_low_risk_calm_environment PASSED")


def test_elevated_rate_pressure() -> None:
    inputs = bare_minimal_inputs(
        spy_change_1d=-0.2,
        qqq_change_1d=-0.3,
        sector_breadth_1d=40.0,
        sector_breadth_7d=45.0,
        spx_return_7d=-0.8,
        spx_return_63d=1.0,
        vix_current=19.0,
        us10y_yield=4.80,
        dxy_change_1d=0.3,
        btc_change_24h=-1.0,
        cyclical_vs_defensive_spread=-0.5,
    )
    result = assess_swing_regime(inputs)
    assert result["risk_level"] in ("MODERATE", "ELEVATED"), \
        f"Expected MODERATE/ELEVATED, got {result['risk_level']}"
    assert result["dominant_driver"] == "rate_and_dollar_pressure", \
        f"Expected rate_and_dollar_pressure, got {result['dominant_driver']}"
    assert result["position_size_hint"] in ("selective", "half-size")
    print("test_elevated_rate_pressure PASSED")


def test_high_risk_selloff() -> None:
    inputs = bare_minimal_inputs(
        spy_change_1d=-2.0,
        qqq_change_1d=-3.0,
        sector_breadth_1d=18.0,
        sector_breadth_7d=25.0,
        spx_return_7d=-4.0,
        spx_return_63d=-5.0,
        vix_current=28.0,
        vix_change_1d=25.0,
        hyg_change_1d=-2.0,
        us10y_yield=4.85,
        dxy_change_1d=0.8,
        btc_change_24h=-8.0,
        cyclical_vs_defensive_spread=-4.0,
        market_posture="Risk-Off",
    )
    result = assess_swing_regime(inputs)
    assert result["risk_level"] in ("HIGH", "EXTREME"), \
        f"Expected HIGH/EXTREME, got {result['risk_level']}"
    assert result["trade_bias"] in ("SHORT_HEDGE", "SELECTIVE_SHORT"), \
        f"Expected SHORT_HEDGE/SELECTIVE_SHORT, got {result['trade_bias']}"
    assert result["position_size_hint"] in ("half-size", "preserve capital")
    print("test_high_risk_selloff PASSED")


def test_high_risk_but_improving() -> None:
    inputs = bare_minimal_inputs(
        spy_change_1d=1.5,
        qqq_change_1d=2.0,
        sector_breadth_1d=60.0,
        sector_breadth_7d=45.0,
        spx_return_7d=2.0,
        spx_return_63d=-4.0,
        vix_current=23.0,
        vix_change_1d=-10.0,
        hyg_change_1d=0.5,
        us10y_yield=4.55,
        dxy_change_1d=-0.5,
        btc_change_24h=4.0,
        cyclical_vs_defensive_spread=1.5,
    )
    result = assess_swing_regime(inputs)
    direction = result["regime_direction"]
    bias = result["trade_bias"]
    assert direction != "WORSENING", f"Should not be WORSENING when multi-TF is improving: {direction}"
    assert bias not in ("SHORT_HEDGE",), \
        f"Should not be SHORT_HEDGE when improving, got {bias}"
    print("test_high_risk_but_improving PASSED")


def test_low_risk_deteriorating() -> None:
    inputs = bare_minimal_inputs(
        spy_change_1d=-0.4,
        qqq_change_1d=-0.6,
        sector_breadth_1d=42.0,
        sector_breadth_7d=55.0,
        spx_return_7d=-1.2,
        spx_return_63d=3.0,
        vix_current=17.0,
        vix_change_1d=3.0,
        dxy_change_1d=0.25,
        btc_change_24h=-1.5,
        cyclical_vs_defensive_spread=-0.5,
    )
    result = assess_swing_regime(inputs)
    bias = result["trade_bias"]
    assert bias != "SHORT_HEDGE", \
        f"Low-moderate risk deteriorating should not be SHORT_HEDGE, got {bias}"
    print("test_low_risk_deteriorating PASSED")


def test_correlation_dampening_single_event() -> None:
    """SPY, QQQ, and breadth all down from one risk-off event — not triple-counted."""
    inputs = bare_minimal_inputs(
        spy_change_1d=-1.8,
        qqq_change_1d=-2.8,
        sector_breadth_1d=20.0,
        sector_breadth_7d=30.0,
        spx_return_7d=-3.5,
        spx_return_63d=-2.0,
        vix_current=22.0,
        vix_change_1d=12.0,
    )
    # Same inputs but VIX only moderately elevated, credit normal
    result = assess_swing_regime(inputs)
    # Should NOT be EXTREME just because 3 correlated fields are red
    assert result["risk_level"] != "EXTREME", \
        f"Single correlated event should not produce EXTREME, got {result['risk_level']}"
    print("test_correlation_dampening_single_event PASSED")


def test_event_overlay_preserves_directional_score() -> None:
    inputs = bare_minimal_inputs(
        has_upcoming_high_impact_event=True,
        days_until_next_event=2,
        next_event_title="CPI",
    )
    result = assess_swing_regime(inputs)
    ev = result["event_overlay"]
    assert ev["active"] is True, "Event overlay should be active"
    assert ev["contributes_to_directional_score"] is False, \
        "Event risk should NOT contribute to directional score"
    assert ev["severity"] == "HIGH", f"2-day event should be HIGH, got {ev['severity']}"
    print("test_event_overlay_preserves_directional_score PASSED")


def test_all_fields_null_graceful() -> None:
    inputs = {
        "spy_change_1d": None,
        "qqq_change_1d": None,
        "sector_breadth_1d": None,
        "sector_breadth_7d": None,
        "spx_return_7d": None,
        "spx_return_63d": None,
        "vix_current": None,
        "vix_change_1d": None,
        "vix_return_7d": None,
        "hyg_change_1d": None,
        "us10y_yield": None,
        "dxy_price": None,
        "dxy_change_1d": None,
        "btc_change_24h": None,
        "cyclical_vs_defensive_spread": None,
        "market_posture": None,
        "has_upcoming_high_impact_event": False,
        "days_until_next_event": None,
        "next_event_title": None,
    }
    result = assess_swing_regime(inputs)
    assert result["risk_level"] in ("LOW", "MODERATE", "ELEVATED", "HIGH", "EXTREME")
    assert result["regime_direction"] == "UNKNOWN"
    assert result["trade_bias"] == "NEUTRAL"
    assert result["risk_score"] >= 0 and result["risk_score"] <= 100
    pillars = result["pillars"]
    for name, p in pillars.items():
        assert p["confidence"] in ("UNKNOWN", "LOW", "MEDIUM", "HIGH",)
        assert p["direction"] in ("IMPROVING", "STABLE", "WEAKENING", "WORSENING", "UNKNOWN",)
    print("test_all_fields_null_graceful PASSED")


# ── Trade decision projection tests ──────────────────────────────────────────

def test_trade_decision_no_contradiction() -> None:
    """trade_decision and risk_cluster must never disagree on severity vs posture."""
    from services.home_risk_intelligence import _project_trade_decision_from_swing_regime

    # Elevated risk with high rate pressure — should NOT say "normal"
    sr = {
        "risk_level": "ELEVATED",
        "risk_score": 55,
        "trade_bias": "SELECTIVE_SHORT",
        "position_size_hint": "selective",
        "one_line": "Risk elevated, weakening — rate headwind persisting — neutral positioning",
        "dominant_driver": "rate_and_dollar_pressure",
    }
    td = _project_trade_decision_from_swing_regime(sr, {})
    assert td["position_size_hint"] == "selective", "Position sizing must match swing_regime"
    assert td["label"] != "YES", "ELEVATED risk should not produce YES label"
    assert td["mode"] == "swing"
    assert td["score"] == 45, f"Expected 45 (100-55), got {td['score']}"
    print("test_trade_decision_no_contradiction PASSED")


def test_trade_decision_extreme_to_no() -> None:
    from services.home_risk_intelligence import _project_trade_decision_from_swing_regime

    sr = {
        "risk_level": "EXTREME",
        "risk_score": 88,
        "trade_bias": "SHORT_HEDGE",
        "position_size_hint": "preserve capital",
        "one_line": "Risk level EXTREME — conditions worsening — defensive posture warranted",
        "dominant_driver": "broad_market_trend",
    }
    td = _project_trade_decision_from_swing_regime(sr, {})
    assert td["label"] == "NO"
    assert td["position_size_hint"] == "preserve capital"
    assert "all new entries" in td["avoid"]
    print("test_trade_decision_extreme_to_no PASSED")


# ── Pillar unit tests ─────────────────────────────────────────────────────────

def test_trend_and_breadth_strong() -> None:
    p = _score_trend_and_breadth(bare_minimal_inputs(
        spy_change_1d=1.5, qqq_change_1d=2.0,
        sector_breadth_1d=80.0, sector_breadth_7d=75.0,
        spx_return_7d=3.0, spx_return_63d=8.0,
    ))
    assert p["risk_score"] <= 30, f"Strong trend should be low risk, got {p['risk_score']}"
    assert p["direction"] in ("STABLE", "IMPROVING")
    print("test_trend_and_breadth_strong PASSED")


def test_trend_and_breadth_weak() -> None:
    p = _score_trend_and_breadth(bare_minimal_inputs(
        spy_change_1d=-2.5, qqq_change_1d=-3.5,
        sector_breadth_1d=15.0, sector_breadth_7d=20.0,
        spx_return_7d=-5.0, spx_return_63d=-10.0,
    ))
    assert p["risk_score"] >= 70, f"Weak trend should be high risk, got {p['risk_score']}"
    print("test_trend_and_breadth_weak PASSED")


def test_volatility_and_credit_calm() -> None:
    p = _score_volatility_and_credit(bare_minimal_inputs(
        vix_current=13.0, vix_change_1d=-2.0, hyg_change_1d=0.3,
    ))
    assert p["risk_score"] <= 30
    assert p["direction"] in ("STABLE", "IMPROVING")
    print("test_volatility_and_credit_calm PASSED")


def test_volatility_and_credit_stressed() -> None:
    p = _score_volatility_and_credit(bare_minimal_inputs(
        vix_current=32.0, vix_change_1d=25.0, hyg_change_1d=-2.5,
    ))
    assert p["risk_score"] >= 70
    assert p["direction"] in ("WORSENING", "WEAKENING")
    print("test_volatility_and_credit_stressed PASSED")


def test_rates_and_dollar_benign() -> None:
    p = _score_rates_and_dollar(bare_minimal_inputs(
        us10y_yield=3.5, dxy_change_1d=-0.3,
    ))
    assert p["risk_score"] <= 35
    print("test_rates_and_dollar_benign PASSED")


def test_rates_and_dollar_pressure() -> None:
    p = _score_rates_and_dollar(bare_minimal_inputs(
        us10y_yield=5.1, dxy_change_1d=0.8,
    ))
    assert p["risk_score"] >= 60
    assert p["direction"] in ("WORSENING", "WEAKENING")
    print("test_rates_and_dollar_pressure PASSED")


def test_leadership_cross_asset_risk_on() -> None:
    p = _score_leadership_and_cross_asset(bare_minimal_inputs(
        btc_change_24h=4.0, cyclical_vs_defensive_spread=3.5, market_posture="Risk-On",
    ))
    assert p["risk_score"] <= 25
    assert p["direction"] in ("STABLE", "IMPROVING")
    print("test_leadership_cross_asset_risk_on PASSED")


def test_leadership_cross_asset_risk_off() -> None:
    p = _score_leadership_and_cross_asset(bare_minimal_inputs(
        btc_change_24h=-7.0, cyclical_vs_defensive_spread=-4.0, market_posture="Risk-Off",
    ))
    assert p["risk_score"] >= 45, f"Risk-off cross-asset should be elevated, got {p['risk_score']}"
    assert p["direction"] in ("WORSENING", "WEAKENING")
    print("test_leadership_cross_asset_risk_off PASSED")


# ── Severity boundary tests ───────────────────────────────────────────────────

def test_risk_level_bounds() -> None:
    assert _risk_level_from_score(0) == "LOW"
    assert _risk_level_from_score(24) == "LOW"
    assert _risk_level_from_score(25) == "MODERATE"
    assert _risk_level_from_score(44) == "MODERATE"
    assert _risk_level_from_score(45) == "ELEVATED"
    assert _risk_level_from_score(64) == "ELEVATED"
    assert _risk_level_from_score(65) == "HIGH"
    assert _risk_level_from_score(79) == "HIGH"
    assert _risk_level_from_score(80) == "EXTREME"
    assert _risk_level_from_score(100) == "EXTREME"
    print("test_risk_level_bounds PASSED")


def test_trade_bias_decision_matrix() -> None:
    matrix = [
        ("LOW",      "IMPROVING", "LONG"),
        ("LOW",      "STABLE",    "LONG"),
        ("LOW",      "WEAKENING", "SELECTIVE_LONG"),
        ("LOW",      "WORSENING", "NEUTRAL"),
        ("MODERATE", "IMPROVING", "SELECTIVE_LONG"),
        ("MODERATE", "STABLE",    "SELECTIVE_LONG"),
        ("MODERATE", "WEAKENING", "NEUTRAL"),
        ("ELEVATED", "IMPROVING", "SELECTIVE_LONG"),
        ("ELEVATED", "STABLE",    "NEUTRAL"),
        ("ELEVATED", "WEAKENING", "SELECTIVE_SHORT"),
        ("HIGH",     "IMPROVING", "NEUTRAL"),
        ("HIGH",     "STABLE",    "SELECTIVE_SHORT"),
        ("HIGH",     "WEAKENING", "SHORT_HEDGE"),
        ("HIGH",     "WORSENING", "SHORT_HEDGE"),
        ("EXTREME",  "IMPROVING", "NEUTRAL"),
        ("EXTREME",  "STABLE",    "SHORT_HEDGE"),
        ("EXTREME",  "WEAKENING", "SHORT_HEDGE"),
        ("EXTREME",  "WORSENING", "SHORT_HEDGE"),
    ]
    for risk_level, direction, expected_bias in matrix:
        bias = _compute_trade_bias(risk_level, direction)
        assert bias == expected_bias, \
            f"({risk_level}, {direction}) → expected {expected_bias}, got {bias}"
    print("test_trade_bias_decision_matrix PASSED")


def test_position_size_with_event_upgrade() -> None:
    assert _compute_position_size_hint("LOW", {"has_upcoming_high_impact_event": True}) == "selective"
    assert _compute_position_size_hint("MODERATE", {"has_upcoming_high_impact_event": True}) == "half-size"
    assert _compute_position_size_hint("ELEVATED", {"has_upcoming_high_impact_event": True}) == "half-size"
    assert _compute_position_size_hint("HIGH", {"has_upcoming_high_impact_event": True}) == "preserve capital"
    assert _compute_position_size_hint("EXTREME", {"has_upcoming_high_impact_event": True}) == "preserve capital"
    print("test_position_size_with_event_upgrade PASSED")


def test_trade_decision_avoid_lists() -> None:
    from services.home_risk_intelligence import _project_trade_decision_from_swing_regime

    td = _project_trade_decision_from_swing_regime({"risk_level": "EXTREME", "risk_score": 85, "trade_bias": "SHORT_HEDGE", "position_size_hint": "preserve capital", "one_line": "", "dominant_driver": ""}, {})
    assert "all new entries" in td["avoid"]
    assert "leveraged positions" in td["avoid"]

    td2 = _project_trade_decision_from_swing_regime({"risk_level": "HIGH", "risk_score": 72, "trade_bias": "SELECTIVE_SHORT", "position_size_hint": "half-size", "one_line": "", "dominant_driver": ""}, {})
    assert "all new entries" not in td2["avoid"]
    assert "leveraged positions" in td2["avoid"]

    td3 = _project_trade_decision_from_swing_regime({"risk_level": "MODERATE", "risk_score": 35, "trade_bias": "SELECTIVE_LONG", "position_size_hint": "selective", "one_line": "", "dominant_driver": "rate_and_dollar_pressure"}, {})
    assert "rate-sensitive growth names" in td3["avoid"]
    print("test_trade_decision_avoid_lists PASSED")


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_low_risk_calm_environment()
    test_elevated_rate_pressure()
    test_high_risk_selloff()
    test_high_risk_but_improving()
    test_low_risk_deteriorating()
    test_correlation_dampening_single_event()
    test_event_overlay_preserves_directional_score()
    test_all_fields_null_graceful()
    test_trade_decision_no_contradiction()
    test_trade_decision_extreme_to_no()
    test_trend_and_breadth_strong()
    test_trend_and_breadth_weak()
    test_volatility_and_credit_calm()
    test_volatility_and_credit_stressed()
    test_rates_and_dollar_benign()
    test_rates_and_dollar_pressure()
    test_leadership_cross_asset_risk_on()
    test_leadership_cross_asset_risk_off()
    test_risk_level_bounds()
    test_trade_bias_decision_matrix()
    test_position_size_with_event_upgrade()
    test_trade_decision_avoid_lists()
    print("\nAll 22 tests PASSED")
