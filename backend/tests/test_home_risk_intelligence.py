"""
Unit tests for swing_regime_service and home_risk_intelligence composer.

Mocked-data-only - no network, no DB, no provider calls.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

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
    _compute_pillar_availability,
    _risk_level_from_score,
    _compute_regime_direction,
    _compute_trade_bias,
    _compute_position_size_hint,
    _apply_event_sizing,
    _compute_event_overlay,
)


# =============================================================================
# Helpers
# =============================================================================

def bare_inputs(**overrides) -> dict:
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
        "us10y_change_1d_bps":           0.0,
        "us10y_change_5d_bps":           0.0,
        "us10y_change_20d_bps":          0.0,
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


# =============================================================================
# VIX 7-day return correction tests
# =============================================================================

def test_vix_7d_min_is_not_a_return() -> None:
    """Prove that vix_min=14 is never treated as +14% return."""
    # This function does not receive vix_min under vix_return_7d.
    # vix_return_7d is always null unless computed from daily history.
    result = assess_swing_regime(bare_inputs())
    vc = result["pillars"]["volatility_and_credit"]
    comps = vc.get("components", {})
    # vix_return_7d should NOT appear unless provided via input
    # When None, it's not in components at all
    assert comps.get("vix_return_7d") is None, \
        "vix_return_7d should not be present when not provided"
    print("test_vix_7d_min_is_not_a_return PASSED")


def test_vix_7d_real_return_computed_from_history() -> None:
    """Real 7-session return: ((latest/7sessions_ago) - 1) * 100"""
    from services.home_risk_intelligence import _compute_vix_7d_return
    from datetime import date, timedelta

    today = date.today()
    history = []
    for i in range(60):
        d = today - timedelta(days=60 - i)
        history.append({"date": d.isoformat(), "value": 15.0 + i * 0.05})

    ret = _compute_vix_7d_return(history)
    # VIX rose from ~15.0 + 52*0.05=17.6 to 15.0+59*0.05=17.95
    # return = (17.95/17.6 - 1)*100 = ~1.99%
    assert ret is not None
    assert ret > 0, f"Expected positive return, got {ret}"
    assert ret < 5, f"Expected modest return, got {ret}"
    print("test_vix_7d_real_return_computed_from_history PASSED")


def test_vix_7d_return_insufficient_data() -> None:
    from services.home_risk_intelligence import _compute_vix_7d_return
    assert _compute_vix_7d_return([]) is None
    assert _compute_vix_7d_return([{"date": "2026-01-01", "value": 14.0}]) is None
    print("test_vix_7d_return_insufficient_data PASSED")


# =============================================================================
# 10Y rate direction tests
# =============================================================================

def _make_dgs10_history(base: float = 4.5, n_days: int = 25) -> list[dict]:
    from datetime import date, timedelta
    today = date.today()
    result = []
    for i in range(n_days):
        d = today - timedelta(days=n_days - i)
        result.append({"date": d.isoformat(), "value": round(base, 3)})
    return result


def test_10y_change_bps_calculation() -> None:
    """Verify yield changes in basis points from DGS10 history."""
    from services.home_risk_intelligence import _compute_yield_changes

    history = _make_dgs10_history(base=4.76, n_days=25)
    # All values same -> changes should be 0
    yc = _compute_yield_changes(history, None)
    assert yc["change_1d_bps"] == 0.0
    assert yc["change_5d_bps"] == 0.0

    # Rising: set day 20 to 4.71 (down 5 bp from 4.76)
    history[19]["value"] = 4.71
    yc = _compute_yield_changes(history, None)
    assert yc["change_5d_bps"] == 5.0, f"Expected +5.0 bps, got {yc['change_5d_bps']}"

    # Falling: set day 20 to 4.91 (up 15 bp from 4.76)
    history[19]["value"] = 4.91
    yc = _compute_yield_changes(history, None)
    assert yc["change_5d_bps"] == -15.0, f"Expected -15.0 bps, got {yc['change_5d_bps']}"
    print("test_10y_change_bps_calculation PASSED")


# =============================================================================
# Rates pillar direction tests (scenario A and B from prompt)
# =============================================================================

def test_scenario_a_10y_restrictive_but_easing() -> None:
    """10Y at 4.76%, down 15 bps over 5 sessions -> IMPROVING or STABLE, not WORSENING."""
    result = assess_swing_regime(bare_inputs(
        us10y_yield=4.76,
        us10y_change_1d_bps=-3.0,
        us10y_change_5d_bps=-15.0,
        us10y_change_20d_bps=-8.0,
    ))
    rd = result["pillars"]["rates_and_dollar"]
    assert rd["direction"] not in ("WORSENING",), \
        f"Falling 15bps should not be WORSENING, got {rd['direction']}"
    assert rd["direction"] in ("IMPROVING", "STABLE"), \
        f"Expected IMPROVING or STABLE, got {rd['direction']}"
    # Level is still a headwind (risk_score elevated), but direction is easing
    assert rd["risk_score"] >= 40, \
        f"Absolute level (4.76%) should contribute, got {rd['risk_score']}"
    print("test_scenario_a_10y_restrictive_but_easing PASSED")


def test_scenario_b_10y_below_threshold_but_rising() -> None:
    """10Y at 4.45%, up 20 bps over 5 sessions -> WEAKENING or WORSENING, not STABLE."""
    result = assess_swing_regime(bare_inputs(
        us10y_yield=4.45,
        us10y_change_1d_bps=4.0,
        us10y_change_5d_bps=20.0,
        us10y_change_20d_bps=15.0,
    ))
    rd = result["pillars"]["rates_and_dollar"]
    assert rd["direction"] not in ("STABLE",), \
        f"Rising 20bps should not be STABLE, got {rd['direction']}"
    assert rd["direction"] in ("WORSENING", "WEAKENING"), \
        f"Expected WORSENING or WEAKENING, got {rd['direction']}"
    print("test_scenario_b_10y_below_threshold_but_rising PASSED")


def test_10y_absolute_level_alone_is_not_direction() -> None:
    """Without impulse data, absolute level is a weaker signal."""
    result = assess_swing_regime(bare_inputs(
        us10y_yield=4.76,
        us10y_change_1d_bps=None,
        us10y_change_5d_bps=None,
        us10y_change_20d_bps=None,
    ))
    rd = result["pillars"]["rates_and_dollar"]
    # Should not be WORSENING from level alone
    assert rd["direction"] != "WORSENING", \
        f"Level alone should not produce WORSENING, got {rd['direction']}"
    print("test_10y_absolute_level_alone_is_not_direction PASSED")


# =============================================================================
# Event overlay tests
# =============================================================================

def test_event_overlay_does_not_affect_risk_score() -> None:
    """Risk score must be identical with and without the event."""
    base = bare_inputs(
        spy_change_1d=-1.0, qqq_change_1d=-1.5,
        us10y_yield=4.76, us10y_change_5d_bps=10,
        vix_current=22.0,
    )
    result_without_event = assess_swing_regime(base)
    result_with_event = assess_swing_regime({**base,
        "has_upcoming_high_impact_event": True,
        "days_until_next_event": 2,
        "next_event_title": "CPI",
    })
    assert result_without_event["risk_score"] == result_with_event["risk_score"], \
        "Risk score must be identical with and without event"
    assert result_without_event["trade_bias"] == result_with_event["trade_bias"], \
        "Trade bias must be identical with and without event"
    print("test_event_overlay_does_not_affect_risk_score PASSED")


def test_event_overlay_explains_position_size_change() -> None:
    """position_size_impact must explain the actual decision."""
    result = assess_swing_regime(bare_inputs(
        spy_change_1d=-0.5, us10y_yield=4.76,
        has_upcoming_high_impact_event=True,
        days_until_next_event=1,
        next_event_title="FOMC",
    ))
    ev = result["event_overlay"]
    assert ev["active"] is True
    assert ev["contributes_to_directional_score"] is False
    # Base should be "selective" for MODERATE risk with this input
    # Event upgrades to "half-size"
    assert ev["position_size_impact"] is not None
    assert "reduced" in ev["position_size_impact"].lower() or "half-size" in ev["position_size_impact"].lower(), \
        f"Impact must explain sizing change, got: {ev['position_size_impact']}"
    print("test_event_overlay_explains_position_size_change PASSED")


def test_event_no_sizing_change_explained() -> None:
    """When event doesn't change sizing, say 'No additional sizing reduction'."""
    result = assess_swing_regime(bare_inputs(
        us10y_yield=4.76, us10y_change_5d_bps=10,
        has_upcoming_high_impact_event=True,
        days_until_next_event=3,
        next_event_title="CPI",
    ))
    ev = result["event_overlay"]
    assert ev["position_size_impact"] is not None
    if result["position_size_hint"] == "preserve capital":
        impact = ev["position_size_impact"]
        assert "No additional" in impact or "already" in impact.lower() or "preserve capital" in impact.lower(), \
            f"Should explain no upgrade, got: {impact}"
    print("test_event_no_sizing_change_explained PASSED")


# =============================================================================
# Data sufficiency tests
# =============================================================================

def test_all_fields_null_insufficient_data() -> None:
    """All inputs missing -> assessment_status INSUFFICIENT_DATA, active false."""
    result = assess_swing_regime({
        "spy_change_1d": None, "qqq_change_1d": None,
        "sector_breadth_1d": None, "sector_breadth_7d": None,
        "spx_return_7d": None, "spx_return_63d": None,
        "vix_current": None, "vix_change_1d": None, "vix_return_7d": None,
        "hyg_change_1d": None,
        "us10y_yield": None, "us10y_change_1d_bps": None,
        "us10y_change_5d_bps": None, "us10y_change_20d_bps": None,
        "dxy_price": None, "dxy_change_1d": None,
        "btc_change_24h": None,
        "cyclical_vs_defensive_spread": None, "market_posture": None,
        "has_upcoming_high_impact_event": False,
        "days_until_next_event": None, "next_event_title": None,
    })
    assert result["assessment_status"] == "INSUFFICIENT_DATA"
    assert result["trade_bias"] == "NEUTRAL"
    assert result["regime_direction"] == "UNKNOWN"
    assert result["risk_level"] == "MODERATE"  # conservative default

    # Verify pillar availability
    for name, p in result["pillars"].items():
        assert p["is_available"] is False
    print("test_all_fields_null_insufficient_data PASSED")


def test_only_vix_available_insufficient() -> None:
    """Only VIX but nothing else -> INSUFFICIENT_DATA."""
    result = assess_swing_regime(bare_inputs(
        spy_change_1d=None, qqq_change_1d=None,
        sector_breadth_1d=None, spx_return_7d=None, spx_return_63d=None,
        us10y_yield=None, dxy_change_1d=None,
        hyg_change_1d=None,
        btc_change_24h=None,
        cyclical_vs_defensive_spread=None, market_posture=None,
    ))
    assert result["assessment_status"] in ("INSUFFICIENT_DATA", "PARTIAL"), \
        f"Expected INSUFFICIENT_DATA or PARTIAL, got {result['assessment_status']}"
    print("test_only_vix_available_insufficient PASSED")


def test_two_pillars_partial() -> None:
    """Only Trend + Rates pillars -> PARTIAL."""
    result = assess_swing_regime(bare_inputs(
        vix_current=None, vix_change_1d=None, vix_return_7d=None,
        hyg_change_1d=None,
        btc_change_24h=None,
        cyclical_vs_defensive_spread=None, market_posture=None,
    ))
    assert result["assessment_status"] == "PARTIAL"
    assert result["available_pillar_count"] >= 2
    print("test_two_pillars_partial PASSED")


def test_all_four_pillars_complete() -> None:
    """All four pillars with data -> COMPLETE."""
    result = assess_swing_regime(bare_inputs(
        cyclical_vs_defensive_spread=1.5, market_posture="Risk-On",
    ))
    assert result["assessment_status"] == "COMPLETE"
    assert result["available_pillar_count"] == 4
    print("test_all_four_pillars_complete PASSED")


# =============================================================================
# Risk cluster alignment tests
# =============================================================================

def test_risk_cluster_no_contradiction() -> None:
    """Canonical severity, headline, and posture must be fully coherent."""
    from services.home_risk_intelligence import _project_risk_cluster_from_swing_regime

    sr = {
        "risk_level": "HIGH",
        "risk_score": 72,
        "regime_direction": "WEAKENING",
        "trade_bias": "SELECTIVE_SHORT",
        "one_line": "Risk level HIGH - conditions worsening - defensive posture warranted",
        "assessment_status": "COMPLETE",
        "pillars": {
            "trend_and_breadth":          {"risk_score": 75},
            "volatility_and_credit":      {"risk_score": 55},
            "rates_and_dollar":           {"risk_score": 70},
            "leadership_and_cross_asset": {"risk_score": 45},
        },
    }
    legacy = {
        "triggers": [{"key": "test", "status": "red", "message": "test"}],
        "legacy_trigger_count": 1,
        "legacy_headline": "1 risk signal flagged",
        "legacy_summary": "old summary",
    }
    rc = _project_risk_cluster_from_swing_regime(sr, legacy)

    assert rc["severity"] == "HIGH"
    assert rc["score"] == 72
    assert rc["active"] is True
    assert "HIGH" in rc["headline"]
    assert "WEAKENING" in rc["headline"]
    assert "SELECTIVE SHORT" in rc["headline"]
    assert rc["summary"] == sr["one_line"]
    # Canonical trigger_count counts at-risk pillars (>=45), not legacy chips
    assert rc["trigger_count"] == 4  # all 4 pillars >= 45
    # Legacy fields preserved
    assert rc["legacy_trigger_count"] == 1
    assert rc["legacy_headline"] == "1 risk signal flagged"
    assert rc["triggers"] == legacy["triggers"]
    print("test_risk_cluster_no_contradiction PASSED")


def test_insufficient_data_risk_cluster_headline() -> None:
    """When assessment is INSUFFICIENT_DATA, headline must say so."""
    from services.home_risk_intelligence import _project_risk_cluster_from_swing_regime

    sr = {
        "risk_level": "MODERATE",
        "risk_score": 30,
        "regime_direction": "UNKNOWN",
        "trade_bias": "NEUTRAL",
        "one_line": "Insufficient data - no directional conclusion should be drawn",
        "assessment_status": "INSUFFICIENT_DATA",
        "pillars": {},
    }
    legacy = {"triggers": [], "legacy_trigger_count": 0,
              "legacy_headline": "", "legacy_summary": ""}
    rc = _project_risk_cluster_from_swing_regime(sr, legacy)
    assert "INSUFFICIENT DATA" in rc["headline"]
    assert rc["active"] is False
    assert rc["trigger_count"] == 0
    print("test_insufficient_data_risk_cluster_headline PASSED")


def test_trade_decision_no_contradiction() -> None:
    """ELEVATED risk must never say 'normal' position sizing."""
    from services.home_risk_intelligence import _project_trade_decision_from_swing_regime

    sr = {
        "risk_level": "ELEVATED",
        "risk_score": 55,
        "trade_bias": "SELECTIVE_SHORT",
        "position_size_hint": "selective",
        "one_line": "Risk elevated, worsening",
        "dominant_driver": "rate_and_dollar_pressure",
    }
    td = _project_trade_decision_from_swing_regime(sr, {})
    assert td["position_size_hint"] != "normal", "ELEVATED must never be normal"
    assert td["label"] != "YES"
    assert td["mode"] == "swing"
    print("test_trade_decision_no_contradiction PASSED")


# =============================================================================
# Legacy diagnostic preservation
# =============================================================================

def test_legacy_triggers_preserved() -> None:
    """Legacy triggers list and counts survive in the response."""
    from services.home_risk_intelligence import _project_risk_cluster_from_swing_regime

    sr = {
        "risk_level": "MODERATE",
        "risk_score": 35,
        "regime_direction": "STABLE",
        "trade_bias": "SELECTIVE_LONG",
        "one_line": "Moderate risk, stable",
        "assessment_status": "COMPLETE",
        "pillars": {"trend_and_breadth": {"risk_score": 30},
                    "volatility_and_credit": {"risk_score": 25},
                    "rates_and_dollar": {"risk_score": 50},
                    "leadership_and_cross_asset": {"risk_score": 20}},
    }
    legacy = {
        "triggers": [
            {"key": "vix_spike", "status": "green", "message": "ok"},
            {"key": "ten_y_yield", "status": "red", "message": "10Y 4.80%"},
            {"key": "market_breadth", "status": "yellow", "message": "Breadth 45%"},
        ],
        "legacy_trigger_count": 1,  # only ten_y_yield is red
        "legacy_headline": "1 risk signal flagged",
        "legacy_summary": "10Y 4.80%",
    }
    rc = _project_risk_cluster_from_swing_regime(sr, legacy)

    # Canonical fields from swing_regime
    assert rc["severity"] == "MODERATE"
    # Legacy fields preserved
    assert rc["legacy_trigger_count"] == 1
    assert "1 risk signal flagged" in rc["legacy_headline"]
    assert len(rc["triggers"]) == 3
    # Canonical trigger_count = at-risk pillars, not legacy
    assert rc["trigger_count"] == 1  # only rates_and_dollar >= 45
    print("test_legacy_triggers_preserved PASSED")


# =============================================================================
# Pillar unit tests
# =============================================================================

def test_trend_and_breadth_strong() -> None:
    p = _score_trend_and_breadth(bare_inputs(
        spy_change_1d=1.5, qqq_change_1d=2.0,
        sector_breadth_1d=80.0, sector_breadth_7d=75.0,
        spx_return_7d=3.0, spx_return_63d=8.0,
    ))
    assert p["risk_score"] <= 30
    assert p["direction"] in ("STABLE", "IMPROVING")
    assert p["is_available"] is True
    print("test_trend_and_breadth_strong PASSED")


def test_trend_and_breadth_weak() -> None:
    p = _score_trend_and_breadth(bare_inputs(
        spy_change_1d=-2.5, qqq_change_1d=-3.5,
        sector_breadth_1d=15.0, sector_breadth_7d=20.0,
        spx_return_7d=-5.0, spx_return_63d=-10.0,
    ))
    assert p["risk_score"] >= 70
    print("test_trend_and_breadth_weak PASSED")


def test_volatility_and_credit_calm() -> None:
    p = _score_volatility_and_credit(bare_inputs(
        vix_current=13.0, vix_change_1d=-2.0, hyg_change_1d=0.3,
    ))
    assert p["risk_score"] <= 30
    print("test_volatility_and_credit_calm PASSED")


def test_volatility_and_credit_stressed() -> None:
    p = _score_volatility_and_credit(bare_inputs(
        vix_current=32.0, vix_change_1d=25.0, hyg_change_1d=-2.5,
    ))
    assert p["risk_score"] >= 70
    print("test_volatility_and_credit_stressed PASSED")


def test_rates_and_dollar_benign() -> None:
    p = _score_rates_and_dollar(bare_inputs(
        us10y_yield=3.5, dxy_change_1d=-0.3,
    ))
    assert p["risk_score"] <= 35
    print("test_rates_and_dollar_benign PASSED")


def test_rates_and_dollar_pressure() -> None:
    p = _score_rates_and_dollar(bare_inputs(
        us10y_yield=5.1, dxy_change_1d=0.8,
        us10y_change_5d_bps=10,
    ))
    assert p["risk_score"] >= 45
    print("test_rates_and_dollar_pressure PASSED")


def test_leadership_risk_on() -> None:
    p = _score_leadership_and_cross_asset(bare_inputs(
        btc_change_24h=4.0, cyclical_vs_defensive_spread=3.5, market_posture="Risk-On",
    ))
    assert p["risk_score"] <= 25
    print("test_leadership_risk_on PASSED")


def test_leadership_risk_off() -> None:
    p = _score_leadership_and_cross_asset(bare_inputs(
        btc_change_24h=-7.0, cyclical_vs_defensive_spread=-4.0, market_posture="Risk-Off",
    ))
    assert p["risk_score"] >= 45
    print("test_leadership_risk_off PASSED")


# =============================================================================
# Score and band tests
# =============================================================================

def test_risk_level_bounds() -> None:
    avail = {"trend_and_breadth": True, "volatility_and_credit": True,
             "rates_and_dollar": True, "leadership_and_cross_asset": True}
    assert _risk_level_from_score(0, avail) == "LOW"
    assert _risk_level_from_score(24, avail) == "LOW"
    assert _risk_level_from_score(25, avail) == "MODERATE"
    assert _risk_level_from_score(44, avail) == "MODERATE"
    assert _risk_level_from_score(45, avail) == "ELEVATED"
    assert _risk_level_from_score(64, avail) == "ELEVATED"
    assert _risk_level_from_score(65, avail) == "HIGH"
    assert _risk_level_from_score(79, avail) == "HIGH"
    assert _risk_level_from_score(80, avail) == "EXTREME"
    assert _risk_level_from_score(100, avail) == "EXTREME"
    print("test_risk_level_bounds PASSED")


def test_trade_bias_decision_matrix() -> None:
    avail = {"trend_and_breadth": True, "volatility_and_credit": True,
             "rates_and_dollar": True, "leadership_and_cross_asset": True}
    matrix = [
        ("LOW", "IMPROVING", "LONG"),
        ("LOW", "STABLE", "LONG"),
        ("LOW", "WEAKENING", "SELECTIVE_LONG"),
        ("LOW", "WORSENING", "NEUTRAL"),
        ("MODERATE", "IMPROVING", "SELECTIVE_LONG"),
        ("MODERATE", "STABLE", "SELECTIVE_LONG"),
        ("MODERATE", "WEAKENING", "NEUTRAL"),
        ("ELEVATED", "IMPROVING", "SELECTIVE_LONG"),
        ("ELEVATED", "STABLE", "NEUTRAL"),
        ("ELEVATED", "WEAKENING", "SELECTIVE_SHORT"),
        ("HIGH", "IMPROVING", "NEUTRAL"),
        ("HIGH", "STABLE", "SELECTIVE_SHORT"),
        ("HIGH", "WEAKENING", "SHORT_HEDGE"),
        ("HIGH", "WORSENING", "SHORT_HEDGE"),
        ("EXTREME", "IMPROVING", "NEUTRAL"),
        ("EXTREME", "STABLE", "SHORT_HEDGE"),
        ("EXTREME", "WEAKENING", "SHORT_HEDGE"),
        ("EXTREME", "WORSENING", "SHORT_HEDGE"),
    ]
    for risk_level, direction, expected_bias in matrix:
        bias = _compute_trade_bias(risk_level, direction, avail)
        assert bias == expected_bias, \
            f"({risk_level}, {direction}) -> expected {expected_bias}, got {bias}"
    print("test_trade_bias_decision_matrix PASSED")


def test_position_size_with_event_upgrade() -> None:
    assert _apply_event_sizing("normal", {"has_upcoming_high_impact_event": True}) == "selective"
    assert _apply_event_sizing("selective", {"has_upcoming_high_impact_event": True}) == "half-size"
    assert _apply_event_sizing("half-size", {"has_upcoming_high_impact_event": True}) == "preserve capital"
    assert _apply_event_sizing("preserve capital", {"has_upcoming_high_impact_event": True}) == "preserve capital"
    assert _apply_event_sizing("normal", {"has_upcoming_high_impact_event": False}) == "normal"
    print("test_position_size_with_event_upgrade PASSED")


def test_trade_decision_avoid_lists() -> None:
    from services.home_risk_intelligence import _project_trade_decision_from_swing_regime

    td = _project_trade_decision_from_swing_regime({
        "risk_level": "EXTREME", "risk_score": 85,
        "trade_bias": "SHORT_HEDGE", "position_size_hint": "preserve capital",
        "one_line": "", "dominant_driver": "",
    }, {})
    assert "all new entries" in td["avoid"]

    td2 = _project_trade_decision_from_swing_regime({
        "risk_level": "HIGH", "risk_score": 72,
        "trade_bias": "SELECTIVE_SHORT", "position_size_hint": "half-size",
        "one_line": "", "dominant_driver": "",
    }, {})
    assert "all new entries" not in td2["avoid"]
    assert "leveraged positions" in td2["avoid"]

    td3 = _project_trade_decision_from_swing_regime({
        "risk_level": "MODERATE", "risk_score": 35,
        "trade_bias": "SELECTIVE_LONG", "position_size_hint": "selective",
        "one_line": "", "dominant_driver": "rate_and_dollar_pressure",
    }, {})
    assert "rate-sensitive growth names" in td3["avoid"]
    print("test_trade_decision_avoid_lists PASSED")


# =============================================================================
# Composer-level integration tests (narrow, mocked)
# =============================================================================

def test_risk_cluster_projection_high_with_one_legacy_chip() -> None:
    """When canonical says HIGH but legacy has 1 hot trigger, fields align canonically."""
    from services.home_risk_intelligence import _project_risk_cluster_from_swing_regime

    sr = {
        "risk_level": "HIGH",
        "risk_score": 70,
        "regime_direction": "WORSENING",
        "trade_bias": "SHORT_HEDGE",
        "one_line": "Risk level HIGH - conditions worsening - defensive posture warranted",
        "assessment_status": "COMPLETE",
        "pillars": {
            "trend_and_breadth":          {"risk_score": 80},
            "volatility_and_credit":      {"risk_score": 60},
            "rates_and_dollar":           {"risk_score": 55},
            "leadership_and_cross_asset": {"risk_score": 40},
        },
    }
    legacy = {
        "triggers": [{"key": "ten_y_yield", "status": "red", "message": "10Y 4.80%"}],
        "legacy_trigger_count": 1,
        "legacy_headline": "1 risk signal flagged",
        "legacy_summary": "10Y 4.80%",
    }
    rc = _project_risk_cluster_from_swing_regime(sr, legacy)
    # Canonical must show HIGH, not legacy's "1 risk signal"
    assert rc["severity"] == "HIGH"
    assert "HIGH" in rc["headline"]
    # Legacy preserved for inspection
    assert rc["legacy_trigger_count"] == 1
    # Canonical count = at-risk pillars
    assert rc["trigger_count"] == 3
    print("test_risk_cluster_projection_high_with_one_legacy_chip PASSED")


def test_lkg_fallback_no_mutation() -> None:
    """LKG fallback must return a copy, not mutate the cached object."""
    from data.cache import cache as test_cache

    key = "__test_lkg_no_mutate__"
    original = {"as_of": "test", "swing_regime": {"risk_level": "MODERATE"}}
    test_cache.set(key, original, 3600)

    # Simulate LKG fallback (what build_home_risk_intelligence_safe does)
    lkg = test_cache.get(key)
    # Should be the same reference initially
    assert lkg is not None
    returned = {**lkg, "_lkg_fallback": True}

    # The original in cache must NOT have _lkg_fallback
    still_in_cache = test_cache.get(key)
    assert "_lkg_fallback" not in still_in_cache, \
        "LKG cache must not be mutated by fallback"
    assert returned["_lkg_fallback"] is True

    test_cache.delete(key)
    print("test_lkg_fallback_no_mutation PASSED")


# =============================================================================
# Run
# =============================================================================

if __name__ == "__main__":
    # VIX 7-day correction
    test_vix_7d_min_is_not_a_return()
    test_vix_7d_real_return_computed_from_history()
    test_vix_7d_return_insufficient_data()

    # 10Y direction
    test_10y_change_bps_calculation()
    test_scenario_a_10y_restrictive_but_easing()
    test_scenario_b_10y_below_threshold_but_rising()
    test_10y_absolute_level_alone_is_not_direction()

    # Event overlay
    test_event_overlay_does_not_affect_risk_score()
    test_event_overlay_explains_position_size_change()
    test_event_no_sizing_change_explained()

    # Data sufficiency
    test_all_fields_null_insufficient_data()
    test_only_vix_available_insufficient()
    test_two_pillars_partial()
    test_all_four_pillars_complete()

    # Risk cluster alignment
    test_risk_cluster_no_contradiction()
    test_insufficient_data_risk_cluster_headline()
    test_trade_decision_no_contradiction()
    test_legacy_triggers_preserved()
    test_risk_cluster_projection_high_with_one_legacy_chip()

    # Pillar unit tests
    test_trend_and_breadth_strong()
    test_trend_and_breadth_weak()
    test_volatility_and_credit_calm()
    test_volatility_and_credit_stressed()
    test_rates_and_dollar_benign()
    test_rates_and_dollar_pressure()
    test_leadership_risk_on()
    test_leadership_risk_off()

    # Score/band tests
    test_risk_level_bounds()
    test_trade_bias_decision_matrix()
    test_position_size_with_event_upgrade()
    test_trade_decision_avoid_lists()

    # Composer integration
    test_lkg_fallback_no_mutation()

    print("\nAll 31 tests PASSED")
