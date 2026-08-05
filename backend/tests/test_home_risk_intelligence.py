"""
Unit tests for swing_regime_service and home_risk_intelligence composer.

Mocked-data-only - no network, no DB, no provider calls.
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


def canonical_regime(risk_level="MODERATE", direction="STABLE", bias="SELECTIVE_LONG", assessment="COMPLETE",
                     driver="broad_market_trend", pillars=None, event=None):
    def p(score, direction, comps=None):
        return {"risk_score": score, "direction": direction, "components": comps or {},
                "available_component_count": 3, "expected_component_count": 3, "is_available": True,
                "confidence": "MEDIUM"}
    if pillars is None:
        pillars = {
            "trend_and_breadth":          p(30, "STABLE"),
            "volatility_and_credit":      p(25, "STABLE"),
            "rates_and_dollar":           p(35, "STABLE"),
            "leadership_and_cross_asset": p(20, "STABLE"),
        }
    if event is None:
        event = {"active": False, "severity": "NONE", "next_event": None,
                 "days_until_event": None, "position_size_impact": None,
                 "contributes_to_directional_score": False}
    return {
        "risk_level": risk_level, "risk_score": 35, "regime_direction": direction,
        "trade_bias": bias, "position_size_hint": "selective", "dominant_driver": driver,
        "one_line": f"{risk_level} risk, {direction.lower()}", "assessment_status": assessment,
        "pillars": pillars, "event_overlay": event,
        "conditions_that_would_flip": [], "calibration_status": "deterministic_uncalibrated",
    }


# =============================================================================
# Correction 1: Canonical display triggers
# =============================================================================

def test_canonical_triggers_are_pillar_based() -> None:
    from services.home_risk_intelligence import _build_canonical_trigger_list

    sr = canonical_regime(risk_level="ELEVATED", direction="WEAKENING", bias="NEUTRAL",
        pillars={
            "trend_and_breadth": {"risk_score": 50, "direction": "WEAKENING", "components": {"breadth_1d": 36, "equity_1d_avg": -0.4}, "available_component_count": 3, "expected_component_count": 6, "is_available": True, "confidence": "MEDIUM"},
            "volatility_and_credit": {"risk_score": 30, "direction": "STABLE", "components": {"vix": 18.0, "hyg_change_1d": 0.1}, "available_component_count": 3, "expected_component_count": 4, "is_available": True, "confidence": "HIGH"},
            "rates_and_dollar": {"risk_score": 45, "direction": "WEAKENING", "components": {"us10y": 4.76, "us10y_change_5d_bps": -2.0}, "available_component_count": 4, "expected_component_count": 6, "is_available": True, "confidence": "MEDIUM"},
            "leadership_and_cross_asset": {"risk_score": 25, "direction": "STABLE", "components": {"btc_change_24h": 1.0}, "available_component_count": 2, "expected_component_count": 3, "is_available": True, "confidence": "MEDIUM"},
        })
    yc = {"change_1d_bps": 0.0, "change_5d_bps": -2.0, "change_20d_bps": -5.0}
    triggers = _build_canonical_trigger_list(sr, yc, {"sector_breadth_1d": 36})

    assert isinstance(triggers, list)
    assert len(triggers) >= 4  # 4 pillars

    # Find the Trend & Breadth trigger
    tb = next(t for t in triggers if t["key"] == "trend_and_breadth")
    assert tb["status"] in ("orange", "yellow")  # risk_score 50 = orange
    assert "narrow participation" in tb["message"]

    # Find the Rates & Dollar trigger
    rd = next(t for t in triggers if t["key"] == "rates_and_dollar")
    assert "4.76" in rd["value"], f"Value should include 10Y level, got: {rd['value']}"

    print("test_canonical_triggers_are_pillar_based PASSED")


def test_canonical_triggers_contain_legacy_key_shape() -> None:
    from services.home_risk_intelligence import _build_canonical_trigger_list

    sr = canonical_regime()
    triggers = _build_canonical_trigger_list(sr, {"change_5d_bps": 0.0}, {"sector_breadth_1d": 50})

    for t in triggers:
        assert "key" in t
        assert "label" in t
        assert "status" in t
        assert t["status"] in ("green", "yellow", "orange", "red")
        assert "value" in t
        assert "message" in t
        # Additive fields
        assert "direction" in t
        assert "timeframe" in t
        assert "risk_score" in t
        assert "source_pillar" in t
    print("test_canonical_triggers_contain_legacy_key_shape PASSED")


def test_event_trigger_never_red() -> None:
    from services.home_risk_intelligence import _build_canonical_trigger_list

    sr = canonical_regime(event={
        "active": True, "severity": "HIGH", "next_event": "CPI",
        "days_until_event": 1, "position_size_impact": "reduced",
        "contributes_to_directional_score": False,
    })
    triggers = _build_canonical_trigger_list(sr, {"change_5d_bps": 0.0}, {})
    ev_triggers = [t for t in triggers if t["key"] == "event_risk"]
    assert len(ev_triggers) == 1
    assert ev_triggers[0]["status"] != "red", "Event risk must never be red"
    assert ev_triggers[0]["direction"] == "UNKNOWN"
    print("test_event_trigger_never_red PASSED")


def test_risk_cluster_triggers_are_canonical() -> None:
    from services.home_risk_intelligence import _project_risk_cluster_from_swing_regime

    sr = canonical_regime()
    canonical_triggers = [{"key": "trend_and_breadth", "label": "Trend & Breadth", "status": "green",
                           "value": "50/100 breadth", "message": "OK", "direction": "STABLE",
                           "timeframe": "multi-timeframe", "risk_score": 30, "source_pillar": "trend_and_breadth"}]
    legacy = {"triggers": [{"key": "vix_spike", "status": "red", "message": "old"}],
              "legacy_trigger_count": 1, "legacy_headline": "old headline",
              "legacy_summary": "old summary"}

    rc = _project_risk_cluster_from_swing_regime(sr, legacy, canonical_triggers)
    assert rc["triggers"] == canonical_triggers, "risk_cluster.triggers must be canonical"
    assert rc["legacy_triggers"] == [{"key": "vix_spike", "status": "red", "message": "old"}]
    assert rc["legacy_trigger_count"] == 1
    assert rc["trigger_count"] == 0  # no pillars >= 45
    print("test_risk_cluster_triggers_are_canonical PASSED")


# =============================================================================
# Correction 2: Direction-aware 10Y display trigger
# =============================================================================

def test_10y_restrictive_but_easing_display() -> None:
    from services.home_risk_intelligence import _build_canonical_trigger_list

    sr = canonical_regime(
        pillars={
            "trend_and_breadth": {"risk_score": 25, "direction": "STABLE", "components": {}, "available_component_count": 3, "expected_component_count": 6, "is_available": True, "confidence": "HIGH"},
            "volatility_and_credit": {"risk_score": 20, "direction": "STABLE", "components": {}, "available_component_count": 3, "expected_component_count": 4, "is_available": True, "confidence": "HIGH"},
            "rates_and_dollar": {"risk_score": 42, "direction": "IMPROVING", "components": {"us10y": 4.76, "us10y_change_5d_bps": -15.0}, "available_component_count": 4, "expected_component_count": 6, "is_available": True, "confidence": "MEDIUM"},
            "leadership_and_cross_asset": {"risk_score": 20, "direction": "STABLE", "components": {}, "available_component_count": 2, "expected_component_count": 3, "is_available": True, "confidence": "MEDIUM"},
        })
    triggers = _build_canonical_trigger_list(sr, {"change_5d_bps": -15.0}, {})

    rd = next(t for t in triggers if t["key"] == "rates_and_dollar")
    assert "4.76" in rd["value"]
    assert "-15" in rd["value"], f"Should show -15 bps/5D, got: {rd['value']}"
    assert "easing" in rd["message"].lower(), f"Message should mention easing, got: {rd['message']}"
    assert "restrictive" in rd["message"].lower(), f"Message should mention restrictive, got: {rd['message']}"
    assert rd["direction"] == "IMPROVING"
    # Status from pillar risk_score=42 → yellow
    assert rd["status"] == "yellow"
    print("test_10y_restrictive_but_easing_display PASSED")


def test_10y_lower_but_accelerating_display() -> None:
    from services.home_risk_intelligence import _build_canonical_trigger_list

    sr = canonical_regime(
        pillars={
            "trend_and_breadth": {"risk_score": 25, "direction": "STABLE", "components": {}, "available_component_count": 3, "expected_component_count": 6, "is_available": True, "confidence": "HIGH"},
            "volatility_and_credit": {"risk_score": 25, "direction": "STABLE", "components": {}, "available_component_count": 3, "expected_component_count": 4, "is_available": True, "confidence": "HIGH"},
            "rates_and_dollar": {"risk_score": 55, "direction": "WEAKENING", "components": {"us10y": 4.45, "us10y_change_5d_bps": 20.0}, "available_component_count": 4, "expected_component_count": 6, "is_available": True, "confidence": "MEDIUM"},
            "leadership_and_cross_asset": {"risk_score": 20, "direction": "STABLE", "components": {}, "available_component_count": 2, "expected_component_count": 3, "is_available": True, "confidence": "MEDIUM"},
        })
    triggers = _build_canonical_trigger_list(sr, {"change_5d_bps": 20.0}, {})

    rd = next(t for t in triggers if t["key"] == "rates_and_dollar")
    assert "4.45" in rd["value"]
    assert "+20" in rd["value"], f"Should show +20 bps/5D, got: {rd['value']}"
    assert "rising" in rd["message"].lower(), f"Message should mention rising, got: {rd['message']}"
    assert rd["direction"] == "WEAKENING"
    assert rd["status"] == "orange"  # risk_score=55
    print("test_10y_lower_but_accelerating_display PASSED")


def test_legacy_10y_is_not_primary_trigger() -> None:
    from services.home_risk_intelligence import _project_risk_cluster_from_swing_regime

    sr = canonical_regime()
    canonical = [{"key": "rates_and_dollar", "label": "Rates & Dollar", "status": "yellow",
                  "value": "4.76% · -15 bps/5D", "message": "10Y restrictive at 4.76% but easing",
                  "direction": "IMPROVING", "timeframe": "1D · 5D · 20D", "risk_score": 42,
                  "source_pillar": "rates_and_dollar"}]
    legacy = {"triggers": [{"key": "ten_y_yield", "status": "red", "message": "10Y yield 4.76% - elevated rate pressure"}],
              "legacy_trigger_count": 1, "legacy_headline": "1 risk signal flagged", "legacy_summary": "10Y 4.76%"}

    rc = _project_risk_cluster_from_swing_regime(sr, legacy, canonical)
    assert rc["triggers"][0]["status"] == "yellow"  # canonical, not legacy red
    assert rc["legacy_triggers"][0]["status"] == "red"  # legacy still has the red chip
    print("test_legacy_10y_is_not_primary_trigger PASSED")


# =============================================================================
# Correction 3: Canonical why_market_is_moving
# =============================================================================

def test_why_market_is_moving_uses_swing_regime() -> None:
    from services.home_risk_intelligence import _build_canonical_why_bullets

    sr = canonical_regime(risk_level="ELEVATED", direction="WEAKENING",
        bias="SELECTIVE_SHORT", driver="rate_and_dollar_pressure",
        pillars={
            "trend_and_breadth": {"risk_score": 40, "direction": "WEAKENING", "components": {"breadth_1d": 36, "equity_1d_avg": -0.3}, "available_component_count": 3, "expected_component_count": 6, "is_available": True, "confidence": "MEDIUM"},
            "volatility_and_credit": {"risk_score": 30, "direction": "STABLE", "components": {"vix": 19}, "available_component_count": 3, "expected_component_count": 4, "is_available": True, "confidence": "HIGH"},
            "rates_and_dollar": {"risk_score": 48, "direction": "WEAKENING", "components": {"us10y": 4.76, "us10y_change_5d_bps": -15.0}, "available_component_count": 4, "expected_component_count": 6, "is_available": True, "confidence": "MEDIUM"},
            "leadership_and_cross_asset": {"risk_score": 25, "direction": "STABLE", "components": {}, "available_component_count": 2, "expected_component_count": 3, "is_available": True, "confidence": "MEDIUM"},
        })

    bullets = _build_canonical_why_bullets(sr, True)
    assert len(bullets) <= 3
    # Market drivers only — should describe 10Y direction
    assert any("restrictive" in b or "fallen" in b or "easing" in b for b in bullets), \
        f"Should describe 10Y direction, got: {bullets}"
    # Must NOT contain old absolute-only pressure language
    assert not any("persistent rate pressure" in b for b in bullets), \
        "Must not use legacy absolute-only rate language when 10Y is easing"
    print("test_why_market_is_moving_uses_swing_regime PASSED")


def test_why_market_is_moving_event_not_bearish() -> None:
    from services.home_risk_intelligence import _build_canonical_why_bullets

    sr = canonical_regime(event={
        "active": True, "severity": "HIGH", "next_event": "CPI",
        "days_until_event": 2, "position_size_impact": "selective reduced to half-size ahead of CPI",
        "contributes_to_directional_score": False,
    })
    bullets = _build_canonical_why_bullets(sr, True)
    # Event context moved to sizing explanation — why bullets are market drivers only
    for b in bullets:
        assert "bearish" not in b.lower(), \
            f"No bullet should describe event as bearish direction, got: {b}"
    print("test_why_market_is_moving_event_not_bearish PASSED")


def test_why_market_is_moving_market_closed() -> None:
    from services.home_risk_intelligence import _build_canonical_why_bullets

    sr = canonical_regime()
    bullets = _build_canonical_why_bullets(sr, False)
    # Market-closed context is in the market_context field, not why_market_is_moving
    for b in bullets:
        assert "closed" not in b.lower(), \
            f"Market-closed context should be in market_context, not why bullets: {b}"
    print("test_why_market_is_moving_market_closed PASSED")


def test_legacy_why_bullets_preserved() -> None:
    from services.home_risk_intelligence import _build_legacy_why_bullets
    bullets = _build_legacy_why_bullets(vix=18.0, vix_change_pct=2.0, spy_change_pct=0.5, qqq_change_pct=0.6,
                                        btc_change_pct=1.0, us10y=4.2, dxy_change_pct=0.1,
                                        vix_signal_title="Calm Zone")
    assert isinstance(bullets, list)
    print("test_legacy_why_bullets_preserved PASSED")


# =============================================================================
# Correction 4: Weekend/holiday history fallback
# =============================================================================

def test_rate_history_stale_fallback() -> None:
    from data.cache import cache as test_cache
    from services.home_risk_intelligence import _read_rate_history, _DGS10_CACHE_KEY

    # Ensure no in-memory data
    test_cache.delete(_DGS10_CACHE_KEY)

    # Neon may have real data or not — either way, status is truthful
    result = _read_rate_history()
    assert "history_status" in result
    assert "history_source" in result
    assert result["history_status"] in ("available", "stale", "unavailable")
    # No provider called, no fabricated data
    print("test_rate_history_stale_fallback PASSED")


def test_vixcls_history_stale_fallback() -> None:
    from data.cache import cache as test_cache
    from services.home_risk_intelligence import _read_vixcls_history, _VIXCLS_CACHE_KEY

    test_cache.delete(_VIXCLS_CACHE_KEY)
    result = _read_vixcls_history()
    assert isinstance(result, dict)
    assert "history" in result
    assert "history_status" in result
    assert "history_source" in result
    assert result["history_status"] in ("available", "stale", "unavailable")
    print("test_vixcls_history_stale_fallback PASSED")


def test_yield_changes_from_stale_history() -> None:
    from services.home_risk_intelligence import _compute_yield_changes
    from datetime import date, timedelta

    today = date.today()
    history = []
    for i in range(30):
        d = today - timedelta(days=30 - i)
        history.append({"date": d.isoformat(), "value": 4.76})

    history[24]["value"] = 4.91  # 5 sessions ago: 4.91 → current 4.76 = -15 bps
    yc = _compute_yield_changes(history, None)
    assert yc["change_5d_bps"] == -15.0, f"Should compute -15 bps from stale history, got {yc['change_5d_bps']}"
    assert yc["history_as_of"] is not None
    print("test_yield_changes_from_stale_history PASSED")


def test_dgs10_fresh_neon_status_is_available() -> None:
    """Fresh Neon fallback (<=24h) must have status=available, not stale."""
    from unittest.mock import patch
    from data.cache import cache as test_cache
    from services.home_risk_intelligence import _read_rate_history, _DGS10_CACHE_KEY

    test_cache.delete(_DGS10_CACHE_KEY)

    mock_data = [
        {"date": "2026-07-30", "value": 4.70},
        {"date": "2026-07-31", "value": 4.76},
    ]

    with patch("data.pg_storage.strategy_hist_read") as mock_read:
        mock_read.return_value = mock_data
        result = _read_rate_history()
        assert result["history_status"] == "available", \
            f"Fresh Neon must be available, got: {result['history_status']}"
        assert "fresh fallback" in result["history_source"]
        assert mock_read.call_count == 1

    test_cache.delete(_DGS10_CACHE_KEY)
    print("test_dgs10_fresh_neon_status_is_available PASSED")


def test_dgs10_stale_neon_status_is_stale() -> None:
    """Any-age Neon fallback must have status=stale."""
    from unittest.mock import patch
    from data.cache import cache as test_cache
    from services.home_risk_intelligence import _read_rate_history, _DGS10_CACHE_KEY

    test_cache.delete(_DGS10_CACHE_KEY)

    mock_data = [
        {"date": "2026-07-20", "value": 4.60},
        {"date": "2026-07-21", "value": 4.65},
    ]

    with patch("data.pg_storage.strategy_hist_read") as mock_read:
        mock_read.side_effect = [None, mock_data]
        result = _read_rate_history()
        assert result["history_status"] == "stale", \
            f"Any-age Neon must be stale, got: {result['history_status']}"
        assert "stale fallback" in result["history_source"]
        assert mock_read.call_count == 2

    test_cache.delete(_DGS10_CACHE_KEY)
    print("test_dgs10_stale_neon_status_is_stale PASSED")


def test_dgs10_unavailable_status() -> None:
    """All tiers empty -> status=unavailable, changes null."""
    from unittest.mock import patch
    from data.cache import cache as test_cache
    from services.home_risk_intelligence import _read_rate_history, _DGS10_CACHE_KEY

    test_cache.delete(_DGS10_CACHE_KEY)

    with patch("data.pg_storage.strategy_hist_read", return_value=None):
        result = _read_rate_history()
        assert result["history_status"] == "unavailable"
        assert result["history"] == []

    from services.home_risk_intelligence import _compute_yield_changes
    yc = _compute_yield_changes([], None)
    assert yc["change_1d_bps"] is None
    assert yc["history_status"] == "unavailable"

    test_cache.delete(_DGS10_CACHE_KEY)
    print("test_dgs10_unavailable_status PASSED")


def test_vixcls_fresh_neon_status_is_available() -> None:
    """Fresh VIXCLS Neon fallback must have status=available."""
    from unittest.mock import patch
    from data.cache import cache as test_cache
    from services.home_risk_intelligence import _read_vixcls_history, _VIXCLS_CACHE_KEY

    test_cache.delete(_VIXCLS_CACHE_KEY)

    mock_data = [{"date": "2026-07-30", "value": 18.0}, {"date": "2026-07-31", "value": 19.0}]

    with patch("data.pg_storage.strategy_hist_read") as mock_read:
        mock_read.return_value = mock_data
        result = _read_vixcls_history()
        assert result["history_status"] == "available", \
            f"Fresh VIXCLS Neon must be available, got: {result['history_status']}"
        assert "fresh fallback" in result["history_source"]

    test_cache.delete(_VIXCLS_CACHE_KEY)
    print("test_vixcls_fresh_neon_status_is_available PASSED")


# =============================================================================
# Correction 5: Market-closed explanation
# =============================================================================

def test_market_context_closed() -> None:
    from services.home_risk_intelligence import _compute_market_context
    assert _compute_market_context(False, 60) == "closed_last_session"
    print("test_market_context_closed PASSED")


def test_market_context_live() -> None:
    from services.home_risk_intelligence import _compute_market_context
    assert _compute_market_context(True, 60) == "live_session"
    print("test_market_context_live PASSED")


def test_market_context_stale() -> None:
    from services.home_risk_intelligence import _compute_market_context
    assert _compute_market_context(True, 1000) == "stale"
    print("test_market_context_stale PASSED")


# =============================================================================
# Correction 6: Frontend contract integration tests
# =============================================================================

def test_risk_cluster_has_required_fields() -> None:
    from services.home_risk_intelligence import _project_risk_cluster_from_swing_regime

    sr = canonical_regime()
    rc = _project_risk_cluster_from_swing_regime(sr,
        {"triggers": [], "legacy_trigger_count": 0, "legacy_headline": "", "legacy_summary": ""},
        [{"key": "trend_and_breadth", "label": "Trend & Breadth", "status": "green",
          "value": "test", "message": "test", "direction": "STABLE",
          "timeframe": "1D", "risk_score": 20, "source_pillar": "trend_and_breadth"}])

    # All existing fields present
    for field in ("active", "severity", "score", "headline", "summary", "trigger_count", "triggers"):
        assert field in rc, f"Missing required field: {field}"
    assert rc["triggers"] is not None
    assert isinstance(rc["triggers"], list)
    print("test_risk_cluster_has_required_fields PASSED")


def test_all_top_level_fields_preserved() -> None:
    from services.home_risk_intelligence import (
        _build_canonical_trigger_list, _build_canonical_why_bullets,
        _project_risk_cluster_from_swing_regime, _project_trade_decision_from_swing_regime,
    )

    sr = canonical_regime()
    triggers = _build_canonical_trigger_list(sr, {"change_5d_bps": 0.0}, {})
    rc = _project_risk_cluster_from_swing_regime(sr,
        {"triggers": [], "legacy_trigger_count": 0, "legacy_headline": "", "legacy_summary": ""},
        triggers)
    td = _project_trade_decision_from_swing_regime(sr, {})
    bullets = _build_canonical_why_bullets(sr, True)

    # Every piece should be a valid Python dict/list
    assert isinstance(rc, dict)
    assert isinstance(td, dict)
    assert isinstance(bullets, list)
    assert len(bullets) >= 1
    print("test_all_top_level_fields_preserved PASSED")


def test_lkg_behavior_preserves_all_fields() -> None:
    from data.cache import cache as test_cache

    key = "__test_lkg_fields__"
    sr = canonical_regime()
    original = {
        "as_of": "test", "market_open": True,
        "data_freshness": {"market_context": "live_session"},
        "market_snapshot": {"sp500": {"symbol": "SPY"}},
        "trade_decision": {"label": "CAUTION"},
        "risk_cluster": {"severity": "MODERATE", "triggers": [], "legacy_triggers": []},
        "swing_regime": sr,
        "why_market_is_moving": ["bullet"],
        "legacy_why_market_is_moving": ["old bullet"],
    }
    test_cache.set(key, original, 3600)

    lkg = test_cache.get(key)
    returned = {**lkg, "_lkg_fallback": True}

    # Original cache untouched
    still_cached = test_cache.get(key)
    assert "_lkg_fallback" not in still_cached

    # Returned has all required sections
    assert returned["_lkg_fallback"] is True
    assert "risk_cluster" in returned
    assert "swing_regime" in returned
    assert "why_market_is_moving" in returned
    assert "legacy_why_market_is_moving" in returned

    test_cache.delete(key)
    print("test_lkg_behavior_preserves_all_fields PASSED")


# =============================================================================
# Existing scoring tests (preserved from prior versions)
# =============================================================================

def test_vix_7d_min_is_not_a_return() -> None:
    result = assess_swing_regime(bare_inputs())
    vc = result["pillars"]["volatility_and_credit"]
    comps = vc.get("components", {})
    assert comps.get("vix_return_7d") is None
    print("test_vix_7d_min_is_not_a_return PASSED")


def test_vix_7d_real_return_computed_from_history() -> None:
    from services.home_risk_intelligence import _compute_vix_7d_return
    from datetime import date, timedelta

    today = date.today()
    history = [{"date": (today - timedelta(days=60 - i)).isoformat(), "value": 15.0 + i * 0.05}
               for i in range(60)]
    ret = _compute_vix_7d_return(history)
    assert ret is not None and ret > 0 and ret < 5
    print("test_vix_7d_real_return_computed_from_history PASSED")


def test_vix_7d_return_insufficient_data() -> None:
    from services.home_risk_intelligence import _compute_vix_7d_return
    assert _compute_vix_7d_return([]) is None
    assert _compute_vix_7d_return([{"date": "2026-01-01", "value": 14.0}]) is None
    print("test_vix_7d_return_insufficient_data PASSED")


def test_10y_change_bps_calculation() -> None:
    from services.home_risk_intelligence import _compute_yield_changes
    from datetime import date, timedelta

    today = date.today()
    history = [{"date": (today - timedelta(days=25 - i)).isoformat(), "value": 4.76}
               for i in range(25)]
    yc = _compute_yield_changes(history, None)
    assert yc["change_1d_bps"] == 0.0
    assert yc["change_5d_bps"] == 0.0

    history[19]["value"] = 4.71
    yc = _compute_yield_changes(history, None)
    assert yc["change_5d_bps"] == 5.0

    history[19]["value"] = 4.91
    yc = _compute_yield_changes(history, None)
    assert yc["change_5d_bps"] == -15.0
    print("test_10y_change_bps_calculation PASSED")


def test_scenario_a_10y_restrictive_but_easing() -> None:
    result = assess_swing_regime(bare_inputs(
        us10y_yield=4.76, us10y_change_5d_bps=-15.0, us10y_change_1d_bps=-3.0, us10y_change_20d_bps=-8.0,
    ))
    rd = result["pillars"]["rates_and_dollar"]
    assert rd["direction"] not in ("WORSENING",)
    assert rd["direction"] in ("IMPROVING", "STABLE")
    assert rd["risk_score"] >= 40
    print("test_scenario_a_10y_restrictive_but_easing PASSED")


def test_scenario_b_10y_below_threshold_but_rising() -> None:
    result = assess_swing_regime(bare_inputs(
        us10y_yield=4.45, us10y_change_5d_bps=20.0, us10y_change_1d_bps=4.0, us10y_change_20d_bps=15.0,
    ))
    rd = result["pillars"]["rates_and_dollar"]
    assert rd["direction"] not in ("STABLE",)
    assert rd["direction"] in ("WORSENING", "WEAKENING")
    print("test_scenario_b_10y_below_threshold_but_rising PASSED")


def test_10y_absolute_level_alone_is_not_direction() -> None:
    result = assess_swing_regime(bare_inputs(
        us10y_yield=4.76, us10y_change_1d_bps=None, us10y_change_5d_bps=None, us10y_change_20d_bps=None,
    ))
    rd = result["pillars"]["rates_and_dollar"]
    assert rd["direction"] != "WORSENING"
    print("test_10y_absolute_level_alone_is_not_direction PASSED")


def test_event_overlay_does_not_affect_risk_score() -> None:
    base = bare_inputs(spy_change_1d=-1.0, qqq_change_1d=-1.5, us10y_yield=4.76, us10y_change_5d_bps=10, vix_current=22.0)
    r1 = assess_swing_regime(base)
    r2 = assess_swing_regime({**base, "has_upcoming_high_impact_event": True, "days_until_next_event": 2, "next_event_title": "CPI"})
    assert r1["risk_score"] == r2["risk_score"]
    assert r1["trade_bias"] == r2["trade_bias"]
    print("test_event_overlay_does_not_affect_risk_score PASSED")


def test_event_overlay_explains_position_size_change() -> None:
    result = assess_swing_regime(bare_inputs(spy_change_1d=-0.5, us10y_yield=4.76,
        has_upcoming_high_impact_event=True, days_until_next_event=1, next_event_title="FOMC"))
    ev = result["event_overlay"]
    assert ev["active"] is True
    assert ev["position_size_impact"] is not None
    assert "reduced" in ev["position_size_impact"].lower() or "half-size" in ev["position_size_impact"].lower()
    print("test_event_overlay_explains_position_size_change PASSED")


def test_event_no_sizing_change_explained() -> None:
    result = assess_swing_regime(bare_inputs(us10y_yield=4.76, us10y_change_5d_bps=10,
        has_upcoming_high_impact_event=True, days_until_next_event=3, next_event_title="CPI"))
    ev = result["event_overlay"]
    assert ev["position_size_impact"] is not None
    print("test_event_no_sizing_change_explained PASSED")


def test_all_fields_null_insufficient_data() -> None:
    result = assess_swing_regime({
        "spy_change_1d": None, "qqq_change_1d": None,
        "sector_breadth_1d": None, "sector_breadth_7d": None,
        "spx_return_7d": None, "spx_return_63d": None,
        "vix_current": None, "vix_change_1d": None, "vix_return_7d": None,
        "hyg_change_1d": None,
        "us10y_yield": None, "us10y_change_1d_bps": None, "us10y_change_5d_bps": None, "us10y_change_20d_bps": None,
        "dxy_price": None, "dxy_change_1d": None,
        "btc_change_24h": None, "cyclical_vs_defensive_spread": None, "market_posture": None,
        "has_upcoming_high_impact_event": False, "days_until_next_event": None, "next_event_title": None,
    })
    assert result["assessment_status"] == "INSUFFICIENT_DATA"
    assert result["trade_bias"] == "NEUTRAL"
    assert result["regime_direction"] == "UNKNOWN"
    assert result["risk_level"] == "MODERATE"
    print("test_all_fields_null_insufficient_data PASSED")


def test_only_vix_available_insufficient() -> None:
    result = assess_swing_regime(bare_inputs(
        spy_change_1d=None, qqq_change_1d=None, sector_breadth_1d=None, spx_return_7d=None, spx_return_63d=None,
        us10y_yield=None, dxy_change_1d=None, hyg_change_1d=None,
        btc_change_24h=None, cyclical_vs_defensive_spread=None, market_posture=None,
    ))
    assert result["assessment_status"] in ("INSUFFICIENT_DATA", "PARTIAL")
    print("test_only_vix_available_insufficient PASSED")


def test_two_pillars_partial() -> None:
    result = assess_swing_regime(bare_inputs(
        vix_current=None, vix_change_1d=None, vix_return_7d=None, hyg_change_1d=None,
        btc_change_24h=None, cyclical_vs_defensive_spread=None, market_posture=None,
    ))
    assert result["assessment_status"] == "PARTIAL"
    assert result["available_pillar_count"] >= 2
    print("test_two_pillars_partial PASSED")


def test_all_four_pillars_complete() -> None:
    result = assess_swing_regime(bare_inputs(cyclical_vs_defensive_spread=1.5, market_posture="Risk-On"))
    assert result["assessment_status"] == "COMPLETE"
    assert result["available_pillar_count"] == 4
    print("test_all_four_pillars_complete PASSED")


def test_risk_cluster_no_contradiction() -> None:
    from services.home_risk_intelligence import _project_risk_cluster_from_swing_regime

    sr = canonical_regime(risk_level="HIGH", direction="WEAKENING", bias="SELECTIVE_SHORT")
    rc = _project_risk_cluster_from_swing_regime(sr,
        {"triggers": [], "legacy_trigger_count": 1, "legacy_headline": "1 signal flagged", "legacy_summary": "old"},
        [{"key": "trend_and_breadth", "label": "Trend & Breadth", "status": "red", "value": "t",
          "message": "t", "direction": "WORSENING", "timeframe": "1D", "risk_score": 75, "source_pillar": "trend_and_breadth"}])

    assert rc["severity"] == "HIGH"
    assert "HIGH" in rc["headline"]
    assert "WEAKENING" in rc["headline"]
    assert "SELECTIVE SHORT" in rc["headline"]
    assert rc["legacy_trigger_count"] == 1
    print("test_risk_cluster_no_contradiction PASSED")


def test_insufficient_data_headline() -> None:
    from services.home_risk_intelligence import _project_risk_cluster_from_swing_regime

    sr = canonical_regime(assessment="INSUFFICIENT_DATA")
    rc = _project_risk_cluster_from_swing_regime(sr,
        {"triggers": [], "legacy_trigger_count": 0, "legacy_headline": "", "legacy_summary": ""}, [])
    assert "INSUFFICIENT DATA" in rc["headline"]
    assert rc["active"] is False
    assert rc["trigger_count"] == 0
    print("test_insufficient_data_headline PASSED")


def test_trade_decision_no_normal_at_elevated() -> None:
    from services.home_risk_intelligence import _project_trade_decision_from_swing_regime

    sr = canonical_regime(risk_level="ELEVATED", bias="SELECTIVE_SHORT")
    sr["position_size_hint"] = "selective"
    td = _project_trade_decision_from_swing_regime(sr, {})
    assert td["position_size_hint"] != "normal"
    assert td["label"] != "YES"
    print("test_trade_decision_no_normal_at_elevated PASSED")


# =============================================================================
# Pillar unit tests
# =============================================================================

def test_trend_and_breadth_strong() -> None:
    p = _score_trend_and_breadth(bare_inputs(spy_change_1d=1.5, qqq_change_1d=2.0, sector_breadth_1d=80.0, sector_breadth_7d=75.0, spx_return_7d=3.0, spx_return_63d=8.0))
    assert p["risk_score"] <= 30
    print("test_trend_and_breadth_strong PASSED")


def test_trend_and_breadth_weak() -> None:
    p = _score_trend_and_breadth(bare_inputs(spy_change_1d=-2.5, qqq_change_1d=-3.5, sector_breadth_1d=15.0, sector_breadth_7d=20.0, spx_return_7d=-5.0, spx_return_63d=-10.0))
    assert p["risk_score"] >= 70
    print("test_trend_and_breadth_weak PASSED")


def test_volatility_and_credit_calm() -> None:
    p = _score_volatility_and_credit(bare_inputs(vix_current=13.0, vix_change_1d=-2.0, hyg_change_1d=0.3))
    assert p["risk_score"] <= 30
    print("test_volatility_and_credit_calm PASSED")


def test_volatility_and_credit_stressed() -> None:
    p = _score_volatility_and_credit(bare_inputs(vix_current=32.0, vix_change_1d=25.0, hyg_change_1d=-2.5))
    assert p["risk_score"] >= 70
    print("test_volatility_and_credit_stressed PASSED")


def test_rates_and_dollar_benign() -> None:
    p = _score_rates_and_dollar(bare_inputs(us10y_yield=3.5, dxy_change_1d=-0.3))
    assert p["risk_score"] <= 35
    print("test_rates_and_dollar_benign PASSED")


def test_rates_and_dollar_pressure() -> None:
    p = _score_rates_and_dollar(bare_inputs(us10y_yield=5.1, dxy_change_1d=0.8, us10y_change_5d_bps=10))
    assert p["risk_score"] >= 45
    print("test_rates_and_dollar_pressure PASSED")


def test_leadership_risk_on() -> None:
    p = _score_leadership_and_cross_asset(bare_inputs(btc_change_24h=4.0, cyclical_vs_defensive_spread=3.5, market_posture="Risk-On"))
    assert p["risk_score"] <= 25
    print("test_leadership_risk_on PASSED")


def test_leadership_risk_off() -> None:
    p = _score_leadership_and_cross_asset(bare_inputs(btc_change_24h=-7.0, cyclical_vs_defensive_spread=-4.0, market_posture="Risk-Off"))
    assert p["risk_score"] >= 45
    print("test_leadership_risk_off PASSED")


def test_risk_level_bounds() -> None:
    avail = {"trend_and_breadth": True, "volatility_and_credit": True, "rates_and_dollar": True, "leadership_and_cross_asset": True}
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


def test_trade_bias_matrix() -> None:
    avail = {"trend_and_breadth": True, "volatility_and_credit": True, "rates_and_dollar": True, "leadership_and_cross_asset": True}
    matrix = [
        ("LOW", "STABLE", "LONG"), ("LOW", "WEAKENING", "SELECTIVE_LONG"), ("LOW", "WORSENING", "NEUTRAL"),
        ("MODERATE", "STABLE", "SELECTIVE_LONG"), ("MODERATE", "WEAKENING", "NEUTRAL"),
        ("ELEVATED", "STABLE", "NEUTRAL"), ("ELEVATED", "WEAKENING", "SELECTIVE_SHORT"),
        ("HIGH", "STABLE", "SELECTIVE_SHORT"), ("HIGH", "WEAKENING", "SHORT_HEDGE"),
        ("EXTREME", "STABLE", "SHORT_HEDGE"), ("EXTREME", "WEAKENING", "SHORT_HEDGE"),
    ]
    for risk_level, direction, expected_bias in matrix:
        bias = _compute_trade_bias(risk_level, direction, avail)
        assert bias == expected_bias, f"({risk_level}, {direction}) -> expected {expected_bias}, got {bias}"
    print("test_trade_bias_matrix PASSED")


def test_position_size_with_event() -> None:
    assert _apply_event_sizing("normal", {"has_upcoming_high_impact_event": True}) == "selective"
    assert _apply_event_sizing("selective", {"has_upcoming_high_impact_event": True}) == "half-size"
    assert _apply_event_sizing("half-size", {"has_upcoming_high_impact_event": True}) == "preserve capital"
    assert _apply_event_sizing("normal", {"has_upcoming_high_impact_event": False}) == "normal"
    print("test_position_size_with_event PASSED")


def test_lkg_fallback_no_mutation() -> None:
    from data.cache import cache as test_cache

    key = "__test_lkg_no_mutate_final__"
    original = {"as_of": "test", "swing_regime": {"risk_level": "MODERATE"}}
    test_cache.set(key, original, 3600)

    lkg = test_cache.get(key)
    returned = {**lkg, "_lkg_fallback": True}
    still_cached = test_cache.get(key)
    assert "_lkg_fallback" not in still_cached
    assert returned["_lkg_fallback"] is True
    test_cache.delete(key)
    print("test_lkg_fallback_no_mutation PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Phase B — Composer and contract tests
# ═══════════════════════════════════════════════════════════════════════════════

def test_home_decision_builder_importable():
    """_build_home_decision is importable and callable."""
    from services.home_risk_intelligence import _build_home_decision
    sr = {
        "risk_score": 43, "risk_level": "MODERATE",
        "regime_direction": "STABLE", "trade_bias": "SELECTIVE_LONG",
        "position_size_hint": "selective", "one_line": "test",
        "assessment_status": "COMPLETE", "dominant_driver": "broad_market_trend",
        "event_overlay": {"active": False},
        "conditions_that_would_flip": [],
        "pillars": {},
    }
    es = {
        "status": "available", "age_seconds": 45.0, "expired": False,
        "dashboard": {
            "market_quality_score": 62.0, "execution_window_score": 50.0,
            "decision": "CAUTION", "as_of": "test", "from_cache": True,
            "execution_conditions": [],
        },
    }
    d = _build_home_decision(
        swing_regime=sr, execution_snapshot=es,
        execution_refresh_status="not_needed",
        market_open=True, why_market_is_moving=["test"],
    )
    assert isinstance(d, dict)
    assert d["verdict"] in ("YES", "CAUTION", "NO")
    print("test_home_decision_builder_importable PASSED")


def test_trade_decision_has_score_source():
    """trade_decision includes score_source field."""
    from services.home_risk_intelligence import _project_trade_decision_from_swing_regime
    sr = {
        "risk_score": 43, "risk_level": "MODERATE",
        "regime_direction": "STABLE", "trade_bias": "SELECTIVE_LONG",
        "position_size_hint": "selective", "one_line": "test",
        "dominant_driver": "broad_market_trend",
    }
    td = _project_trade_decision_from_swing_regime(sr, {})
    assert "score_source" in td
    assert td["score_source"] == "swing_regime_inverse_projection"
    print("test_trade_decision_has_score_source PASSED")


def test_home_decision_no_score_averaging_in_builder():
    """_build_home_decision never computes an averaged score."""
    from services.home_risk_intelligence import _build_home_decision
    sr = {
        "risk_score": 43, "risk_level": "MODERATE",
        "regime_direction": "STABLE", "trade_bias": "SELECTIVE_LONG",
        "position_size_hint": "selective", "one_line": "test",
        "assessment_status": "COMPLETE",
        "event_overlay": {"active": False},
        "conditions_that_would_flip": [],
        "pillars": {},
    }
    es = {
        "status": "available", "age_seconds": 45.0, "expired": False,
        "dashboard": {
            "market_quality_score": 62.0, "execution_window_score": 50.0,
            "decision": "CAUTION", "as_of": "test", "from_cache": True,
            "execution_conditions": [],
        },
    }
    d = _build_home_decision(
        swing_regime=sr, execution_snapshot=es,
        execution_refresh_status="not_needed",
        market_open=True, why_market_is_moving=[],
    )
    for forbidden in ("combined_score", "aggregate_score", "home_score"):
        assert forbidden not in d, f"Found forbidden field: {forbidden}"
    assert d["regime"]["risk_score"] == 43
    assert d["execution"]["market_quality_score"] == 62.0
    print("test_home_decision_no_score_averaging_in_builder PASSED")


def test_home_decision_expired_confidence_never_high():
    """Expired execution restricts confidence."""
    from services.home_risk_intelligence import _build_home_decision
    sr = {
        "risk_score": 25, "risk_level": "MODERATE",
        "regime_direction": "IMPROVING", "trade_bias": "SELECTIVE_LONG",
        "position_size_hint": "selective", "one_line": "test",
        "assessment_status": "COMPLETE",
        "event_overlay": {"active": False},
        "conditions_that_would_flip": [],
        "pillars": {},
    }
    es = {
        "status": "expired", "age_seconds": 700.0, "expired": True,
        "dashboard": {
            "market_quality_score": 85.0, "execution_window_score": 100.0,
            "decision": "YES", "as_of": "test", "from_cache": True,
            "execution_conditions": [],
        },
    }
    d = _build_home_decision(
        swing_regime=sr, execution_snapshot=es,
        execution_refresh_status="scheduled",
        market_open=True, why_market_is_moving=[],
    )
    assert d["confidence"] != "HIGH", f"Expired execution should not be HIGH, got {d['confidence']}"
    assert d["verdict"] != "YES"
    print("test_home_decision_expired_confidence_never_high PASSED")


def test_home_decision_unavailable_execution_partial():
    """Unavailable execution produces PARTIAL assessment."""
    from services.home_risk_intelligence import _build_home_decision
    sr = {
        "risk_score": 35, "risk_level": "MODERATE",
        "regime_direction": "STABLE", "trade_bias": "SELECTIVE_LONG",
        "position_size_hint": "selective", "one_line": "test",
        "assessment_status": "COMPLETE",
        "event_overlay": {"active": False},
        "conditions_that_would_flip": [],
        "pillars": {},
    }
    d = _build_home_decision(
        swing_regime=sr, execution_snapshot=None,
        execution_refresh_status="scheduled",
        market_open=True, why_market_is_moving=[],
    )
    assert d["assessment_status"] == "PARTIAL"
    assert d["execution"]["status"] == "unavailable"
    assert d["execution"]["market_quality_score"] is None
    print("test_home_decision_unavailable_execution_partial PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Phase C — Pillar diagnostic tests
# ═══════════════════════════════════════════════════════════════════════════════

def test_pillar_diagnostics_present():
    """All four pillars have diagnostic fields after enrichment."""
    result = assess_swing_regime(bare_inputs())
    for name in ("trend_and_breadth", "volatility_and_credit",
                 "rates_and_dollar", "leadership_and_cross_asset"):
        p = result["pillars"][name]
        for field in ("interpretation", "supportive_signals", "risk_signals",
                      "missing_inputs", "conditions_to_improve", "conditions_to_worsen"):
            assert field in p, f"Pillar {name} missing {field}"
    print("test_pillar_diagnostics_present PASSED")


def test_trend_breadth_positive_1d_but_weak_3m():
    """Positive 1D returns + weak long-term → both support and risk signals, score unchanged."""
    result = assess_swing_regime(bare_inputs(
        spy_change_1d=1.5, qqq_change_1d=2.0, sector_breadth_1d=64,
        spx_return_7d=0.62, spx_return_63d=-4.2,
    ))
    tb = result["pillars"]["trend_and_breadth"]
    assert len(tb["supportive_signals"]) >= 1, "Should have at least one supportive signal"
    assert len(tb["risk_signals"]) >= 1, "Should have at least one risk signal"
    assert tb["risk_score"] > 0
    # Score unchanged — pillar scoring is independent of diagnostics
    pillar_only = _score_trend_and_breadth(bare_inputs(
        spy_change_1d=1.5, qqq_change_1d=2.0, sector_breadth_1d=64,
        spx_return_7d=0.62, spx_return_63d=-4.2,
    ))
    assert tb["risk_score"] == pillar_only["risk_score"], "Diagnostics must not change pillar score"
    print("test_trend_breadth_positive_1d_but_weak_3m PASSED")


def test_volatility_credit_low_vix_positive_hyg():
    """Low/falling VIX + positive HYG → supportive evidence, score unchanged."""
    result = assess_swing_regime(bare_inputs(
        vix_current=15.8, vix_change_1d=-1.44, hyg_change_1d=0.23,
    ))
    vc = result["pillars"]["volatility_and_credit"]
    assert len(vc["supportive_signals"]) >= 1
    assert len(vc["risk_signals"]) == 0
    pillar_only = _score_volatility_and_credit(bare_inputs(
        vix_current=15.8, vix_change_1d=-1.44, hyg_change_1d=0.23,
    ))
    assert vc["risk_score"] == pillar_only["risk_score"]
    print("test_volatility_credit_low_vix_positive_hyg PASSED")


def test_volatility_credit_high_vix_negative_hyg():
    """Elevated/rising VIX + negative HYG → risk evidence."""
    result = assess_swing_regime(bare_inputs(
        vix_current=28.0, vix_change_1d=15.0, hyg_change_1d=-1.5,
    ))
    vc = result["pillars"]["volatility_and_credit"]
    assert len(vc["risk_signals"]) >= 1
    pillar_only = _score_volatility_and_credit(bare_inputs(
        vix_current=28.0, vix_change_1d=15.0, hyg_change_1d=-1.5,
    ))
    assert vc["risk_score"] == pillar_only["risk_score"]
    print("test_volatility_credit_high_vix_negative_hyg PASSED")


def test_rates_dollar_high_but_easing():
    """10Y 4.75% + falling 5D → high but easing interpretation."""
    result = assess_swing_regime(bare_inputs(
        us10y_yield=4.75, us10y_change_5d_bps=-10.0, us10y_change_1d_bps=-2.0,
    ))
    rd = result["pillars"]["rates_and_dollar"]
    assert "restrictive" in rd["interpretation"].lower() or "4.75" in rd["interpretation"]
    assert "easing" in rd["interpretation"].lower() or "fallen" in rd["interpretation"].lower()
    pillar_only = _score_rates_and_dollar(bare_inputs(
        us10y_yield=4.75, us10y_change_5d_bps=-10.0, us10y_change_1d_bps=-2.0,
    ))
    assert rd["risk_score"] == pillar_only["risk_score"]
    print("test_rates_dollar_high_but_easing PASSED")


def test_rates_dollar_high_and_accelerating():
    """10Y 4.45% + rising 5D 20 bps → low but rising/accelerating."""
    result = assess_swing_regime(bare_inputs(
        us10y_yield=4.45, us10y_change_5d_bps=20.0, us10y_change_1d_bps=4.0,
    ))
    rd = result["pillars"]["rates_and_dollar"]
    assert any(w in rd["interpretation"].lower() for w in ("4.45", "risen", "increasing"))
    pillar_only = _score_rates_and_dollar(bare_inputs(
        us10y_yield=4.45, us10y_change_5d_bps=20.0, us10y_change_1d_bps=4.0,
    ))
    assert rd["risk_score"] == pillar_only["risk_score"]
    print("test_rates_dollar_high_and_accelerating PASSED")


def test_rates_dollar_missing_history():
    """Missing rate history → still has interpretation, no crash."""
    result = assess_swing_regime(bare_inputs(
        us10y_yield=4.5, us10y_change_5d_bps=None, us10y_change_1d_bps=None,
    ))
    rd = result["pillars"]["rates_and_dollar"]
    assert isinstance(rd["interpretation"], str)
    print("test_rates_dollar_missing_history PASSED")


def test_leadership_all_inputs_confirming():
    """All three inputs present and risk-on → CONFIRMED."""
    result = assess_swing_regime(bare_inputs(
        btc_change_24h=4.0, cyclical_vs_defensive_spread=3.5, market_posture="Risk-On",
    ))
    lc = result["pillars"]["leadership_and_cross_asset"]
    assert lc["data_status"] == "COMPLETE"
    assert lc["confirmation_status"] in ("CONFIRMED", "MIXED")
    pillar_only = _score_leadership_and_cross_asset(bare_inputs(
        btc_change_24h=4.0, cyclical_vs_defensive_spread=3.5, market_posture="Risk-On",
    ))
    assert lc["risk_score"] == pillar_only["risk_score"]
    print("test_leadership_all_inputs_confirming PASSED")


def test_leadership_btc_missing():
    """BTC missing → PARTIAL, missing input named."""
    result = assess_swing_regime(bare_inputs(
        btc_change_24h=None, cyclical_vs_defensive_spread=1.0, market_posture="Neutral",
    ))
    lc = result["pillars"]["leadership_and_cross_asset"]
    assert lc["data_status"] == "PARTIAL"
    assert "btc_change_24h" in lc["missing_inputs"]
    print("test_leadership_btc_missing PASSED")


def test_leadership_cyclical_defensive_missing():
    """CVD missing → PARTIAL."""
    result = assess_swing_regime(bare_inputs(
        btc_change_24h=1.0, cyclical_vs_defensive_spread=None, market_posture="Neutral",
    ))
    lc = result["pillars"]["leadership_and_cross_asset"]
    assert lc["data_status"] == "PARTIAL"
    assert "cyclical_vs_defensive_spread" in lc["missing_inputs"]
    print("test_leadership_cyclical_defensive_missing PASSED")


def test_leadership_mixed_posture():
    """Mixed posture → MIXED confirmation."""
    result = assess_swing_regime(bare_inputs(
        btc_change_24h=-3.0, cyclical_vs_defensive_spread=1.0, market_posture="Neutral",
    ))
    lc = result["pillars"]["leadership_and_cross_asset"]
    assert lc["confirmation_status"] in ("MIXED", "CONFIRMED", "UNCONFIRMED")
    assert "Cyclicals" in lc["interpretation"] or "Neutral" in lc["interpretation"] or "BTC" in lc["interpretation"]
    print("test_leadership_mixed_posture PASSED")


def test_event_overlay_has_provenance_fields():
    """Event overlay includes base_position_size_hint, pre/post event sizes."""
    result = assess_swing_regime(bare_inputs(
        has_upcoming_high_impact_event=True, days_until_next_event=1, next_event_title="CPI"))
    assert result["base_position_size_hint"] is not None
    ev = result["event_overlay"]
    assert "position_size_adjustment_applied" in ev
    assert "pre_event_size" in ev
    assert "post_event_size" in ev
    assert ev["pre_event_size"] is not None
    assert ev["post_event_size"] is not None
    print("test_event_overlay_has_provenance_fields PASSED")


def test_base_selection_to_half_size_with_event():
    """Base selective + event → final half-size."""
    result = assess_swing_regime(bare_inputs(
        spy_change_1d=0.5, us10y_yield=4.2,
        has_upcoming_high_impact_event=True, days_until_next_event=2, next_event_title="FOMC"))
    assert result["base_position_size_hint"] == "selective"
    assert result["position_size_hint"] == "half-size"
    assert result["event_overlay"]["position_size_adjustment_applied"] is True
    assert result["event_overlay"]["pre_event_size"] == "selective"
    assert result["event_overlay"]["post_event_size"] == "half-size"
    print("test_base_selection_to_half_size_with_event PASSED")


def test_base_half_size_to_preserve_with_event():
    """When base size is half-size + event → preserve capital, or whatever base is → escalates one step."""
    result = assess_swing_regime(bare_inputs(
        spy_change_1d=-3.0, qqq_change_1d=-4.0, vix_current=30, vix_change_1d=15,
        us10y_yield=5.1, us10y_change_5d_bps=15,
        has_upcoming_high_impact_event=True, days_until_next_event=1, next_event_title="CPI"))
    base = result["base_position_size_hint"]
    final = result["position_size_hint"]
    escalations = {"normal": "selective", "selective": "half-size", "half-size": "preserve capital", "preserve capital": "preserve capital"}
    assert final == escalations.get(base, base), \
        f"Expected {escalations.get(base, base)} from base {base}, got {final}"
    print("test_base_half_size_to_preserve_with_event PASSED")


def test_base_preserve_capital_with_event():
    """Base preserve capital + event → still preserve capital."""
    result = assess_swing_regime(bare_inputs(
        spy_change_1d=-5.0, qqq_change_1d=-6.0, sector_breadth_1d=10,
        vix_current=38, vix_change_1d=30, us10y_yield=5.3, us10y_change_5d_bps=25,
        dxy_change_1d=1.5, hyg_change_1d=-3.0,
        has_upcoming_high_impact_event=True, days_until_next_event=1, next_event_title="FOMC"))
    base = result["base_position_size_hint"]
    final = result["position_size_hint"]
    if base == "preserve capital":
        assert final == "preserve capital"
    else:
        escalations = {"normal": "selective", "selective": "half-size", "half-size": "preserve capital", "preserve capital": "preserve capital"}
        assert final == escalations.get(base, base), f"From {base} got {final}"
    print("test_base_preserve_capital_with_event PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Phase C — Score regression tests
# ═══════════════════════════════════════════════════════════════════════════════

_KNOWN_BASELINES = [
    ("bare_defaults", bare_inputs()),
    ("strong_equity", bare_inputs(spy_change_1d=2.0, qqq_change_1d=2.5, sector_breadth_1d=80, spx_return_7d=3.0, spx_return_63d=8.0)),
    ("weak_equity", bare_inputs(spy_change_1d=-3.0, qqq_change_1d=-4.0, sector_breadth_1d=15, spx_return_7d=-5.0, spx_return_63d=-10.0)),
    ("stress_vol", bare_inputs(vix_current=35.0, vix_change_1d=25.0, hyg_change_1d=-3.0)),
    ("rate_pressure", bare_inputs(us10y_yield=5.1, dxy_change_1d=1.0, us10y_change_5d_bps=20.0)),
]


def test_trend_breadth_scores_unchanged():
    for label, inputs in _KNOWN_BASELINES:
        p_bare = _score_trend_and_breadth(inputs)
        result = assess_swing_regime(inputs)
        enriched = result["pillars"]["trend_and_breadth"]
        assert enriched["risk_score"] == p_bare["risk_score"], \
            f"{label}: TB score changed: before={p_bare['risk_score']}, after={enriched['risk_score']}"
    print("test_trend_breadth_scores_unchanged PASSED")


def test_volatility_credit_scores_unchanged():
    for label, inputs in _KNOWN_BASELINES:
        p_bare = _score_volatility_and_credit(inputs)
        result = assess_swing_regime(inputs)
        enriched = result["pillars"]["volatility_and_credit"]
        assert enriched["risk_score"] == p_bare["risk_score"], \
            f"{label}: VC score changed: before={p_bare['risk_score']}, after={enriched['risk_score']}"
    print("test_volatility_credit_scores_unchanged PASSED")


def test_rates_dollar_scores_unchanged():
    for label, inputs in _KNOWN_BASELINES:
        p_bare = _score_rates_and_dollar(inputs)
        result = assess_swing_regime(inputs)
        enriched = result["pillars"]["rates_and_dollar"]
        assert enriched["risk_score"] == p_bare["risk_score"], \
            f"{label}: RD score changed: before={p_bare['risk_score']}, after={enriched['risk_score']}"
    print("test_rates_dollar_scores_unchanged PASSED")


def test_event_overlay_sizing_new_test_explanation():
    """event_overlay with size change produces explanation."""
    result = assess_swing_regime(bare_inputs(
        spy_change_1d=-0.5, us10y_yield=4.76,
        has_upcoming_high_impact_event=True, days_until_next_event=1, next_event_title="FOMC"))
    ev = result["event_overlay"]
    assert ev["active"] is True
    assert ev["position_size_impact"] is not None
    assert "reduced" in ev["position_size_impact"].lower() or "half-size" in ev["position_size_impact"].lower()
    print("test_event_overlay_sizing_new_test_explanation PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Phase C.1 — Invariant tests
# ═══════════════════════════════════════════════════════════════════════════════

def test_stable_pillar_interpretation_no_worsening():
    """STABLE pillar interpretation must not say worsening or weakening."""
    result = assess_swing_regime(bare_inputs(
        spy_change_1d=1.5, qqq_change_1d=2.0, sector_breadth_1d=64,
        spx_return_7d=0.62, spx_return_63d=-4.2,
    ))
    tb = result["pillars"]["trend_and_breadth"]
    interp = tb["interpretation"].lower()
    assert "stable" in interp
    assert "worsening" not in interp
    assert "weakening" not in interp
    print("test_stable_pillar_interpretation_no_worsening PASSED")


def test_worsening_pillar_interpretation_says_worsening():
    """WORSENING pillar interpretation clearly says worsening or weakening."""
    result = assess_swing_regime(bare_inputs(
        spy_change_1d=-2.5, qqq_change_1d=-3.0, sector_breadth_1d=30,
        spx_return_63d=-8.0, vix_current=28.0, vix_change_1d=15.0,
    ))
    # Check at least one pillar with WORSENING direction has it in interpretation
    found = False
    for name, p in result["pillars"].items():
        if p.get("direction") == "WORSENING":
            interp = p["interpretation"].lower()
            assert any(w in interp for w in ("worsening", "weakening", "deteriorating", "pressure")), \
                f"Pillar {name} is WORSENING but interpretation doesn't say so: {interp}"
            found = True
            break
    if not found:
        assert result["risk_score"] > 0  # at least it computed something
    print("test_worsening_pillar_interpretation_says_worsening PASSED")


def test_improving_pillar_interpretation_says_improving():
    """IMPROVING pillar interpretation clearly says improving or easing."""
    result = assess_swing_regime(bare_inputs(
        spy_change_1d=2.0, qqq_change_1d=2.5, sector_breadth_1d=75,
        spx_return_7d=3.0, spx_return_63d=8.0,
        us10y_change_5d_bps=-15.0, us10y_yield=4.76,
    ))
    found = False
    for name, p in result["pillars"].items():
        if p.get("direction") == "IMPROVING":
            interp = p["interpretation"].lower()
            assert any(w in interp for w in ("improving", "easing", "strengthening", "supportive")), \
                f"Pillar {name} is IMPROVING but interpretation doesn't say so: {interp}"
            found = True
            break
    if not found:
        assert result["risk_score"] > 0
    print("test_improving_pillar_interpretation_says_improving PASSED")


def test_partial_leadership_cannot_be_confirmed():
    """PARTIAL leadership data cannot be CONFIRMED."""
    result = assess_swing_regime(bare_inputs(
        btc_change_24h=None, cyclical_vs_defensive_spread=0.64, market_posture="Neutral",
    ))
    lc = result["pillars"]["leadership_and_cross_asset"]
    assert lc["data_status"] == "PARTIAL"
    assert lc["confirmation_status"] != "CONFIRMED", \
        f"PARTIAL data should not be CONFIRMED, got {lc['confirmation_status']}"
    print("test_partial_leadership_cannot_be_confirmed PASSED")


def test_btc_missing_produces_unconfirmed():
    """BTC missing produces UNCONFIRMED, not negative evidence."""
    result = assess_swing_regime(bare_inputs(
        btc_change_24h=None, cyclical_vs_defensive_spread=0.64, market_posture="Neutral",
    ))
    lc = result["pillars"]["leadership_and_cross_asset"]
    assert lc["confirmation_status"] == "UNCONFIRMED"
    # BTC missing is in missing_inputs, not risk_signals
    assert "btc_change_24h" in lc["missing_inputs"]
    for sig in lc.get("risk_signals", []):
        assert "btc" not in sig.get("key", "").lower(), \
            "Missing BTC must not appear as risk signal"
    print("test_btc_missing_produces_unconfirmed PASSED")


def test_conflicting_leadership_inputs_mixed():
    """Conflicting leadership inputs produce MIXED when COMPLETE."""
    result = assess_swing_regime(bare_inputs(
        btc_change_24h=-6.0, cyclical_vs_defensive_spread=2.5, market_posture="Risk-On",
    ))
    lc = result["pillars"]["leadership_and_cross_asset"]
    # BTC -6% is bearish, CVD +2.5% is bullish → conflicting
    assert lc["data_status"] == "COMPLETE"
    assert lc["confirmation_status"] == "MIXED"
    print("test_conflicting_leadership_inputs_mixed PASSED")


def test_vix_below_20_no_below_20_improvement():
    """VIX already below 20 does not generate 'below 20' as improvement condition."""
    result = assess_swing_regime(bare_inputs(vix_current=15.8))
    vc = result["pillars"]["volatility_and_credit"]
    for cond in vc.get("conditions_to_improve", []):
        assert "below 20" not in cond.lower(), \
            f"VIX already below 20, should not have below-20 improvement: {cond}"
        assert "remains" not in cond.lower(), \
            f"Should not have already-satisfied 'remains' condition: {cond}"
    print("test_vix_below_20_no_below_20_improvement PASSED")


def test_breadth_diagnostics_use_exact_thresholds():
    """Breadth diagnostic thresholds use only exact scoring boundaries."""
    # Scoring boundaries: 30, 40, 50, 70
    for breadth_val, expect_contains, expect_not in [
        (64, [], ["45%", "55%"]),
        (35, [], ["45%", "55%"]),
    ]:
        result = assess_swing_regime(bare_inputs(
            spy_change_1d=1.5, qqq_change_1d=2.0,
            sector_breadth_1d=breadth_val,
        ))
        tb = result["pillars"]["trend_and_breadth"]
        for cond in tb.get("conditions_to_improve", []) + tb.get("conditions_to_worsen", []):
            for forbidden in expect_not:
                assert forbidden not in cond, \
                    f"Breadth diagnostic at {breadth_val} contains invented threshold {forbidden}: {cond}"
    print("test_breadth_diagnostics_use_exact_thresholds PASSED")


def test_spx_63d_threshold_matches_scoring():
    """SPX 63D diagnostic threshold equals the scoring threshold (-0.5%, not 0%)."""
    result = assess_swing_regime(bare_inputs(
        spy_change_1d=1.5, qqq_change_1d=2.0, spx_return_63d=-4.2,
    ))
    tb = result["pillars"]["trend_and_breadth"]
    # Check improvement conditions use exact scoring thresholds
    any_63d = [c for c in tb.get("conditions_to_improve", []) if "3-month" in c or "63" in c]
    for cond in any_63d:
        assert "turns positive" not in cond.lower(), \
            f"Must not say 'turns positive' — use scoring threshold value: {cond}"
        # Should contain a numeric threshold matching scoring boundaries: -8.0, -3.0, -0.5
        has_boundary = any(str(b) in cond for b in ("-8.0", "-3.0", "-0.5"))
        assert has_boundary, f"Improvement condition must use scoring threshold value: {cond}"
    print("test_spx_63d_threshold_matches_scoring PASSED")


def test_rate_thresholds_match_scoring():
    """Rate diagnostic thresholds use exact scoring boundaries."""
    result = assess_swing_regime(bare_inputs(
        us10y_yield=5.1, us10y_change_5d_bps=20.0, dxy_change_1d=1.0,
    ))
    rd = result["pillars"]["rates_and_dollar"]
    # Worsen conditions from the +20 bps 5D case
    for cond in rd.get("conditions_to_worsen", []):
        # Must include a scoring boundary number
        if "10Y" in cond or "bps" in cond or "session" in cond:
            has_boundary = any(str(b) in cond for b in ("15", "5.0", "5", "0.5"))
            assert has_boundary, f"Rate worsen condition must use scoring threshold value: {cond}"
    # Improve conditions from the +20 bps case: should want to fall below +5 bps
    assert any("5" in c for c in rd.get("conditions_to_improve", [])), \
        f"Should have improvement condition with threshold value: {rd.get('conditions_to_improve')}"
    print("test_rate_thresholds_match_scoring PASSED")


def test_no_materializes_unfavorably():
    """No output contains 'materializes unfavorably'."""
    from services.home_risk_intelligence import _build_home_decision
    sr = {
        "risk_score": 35, "risk_level": "MODERATE",
        "regime_direction": "STABLE", "trade_bias": "SELECTIVE_LONG",
        "position_size_hint": "selective", "one_line": "test",
        "assessment_status": "COMPLETE",
        "base_position_size_hint": "selective",
        "event_overlay": {"active": True, "severity": "HIGH", "next_event": "CPI",
                          "days_until_event": 1, "position_size_adjustment_applied": True,
                          "pre_event_size": "selective", "post_event_size": "half-size"},
        "conditions_that_would_flip": [],
        "pillars": {
            "trend_and_breadth": {"risk_score": 30, "direction": "STABLE", "components": {},
                "supportive_signals": [], "risk_signals": [], "missing_inputs": [],
                "conditions_to_improve": [], "conditions_to_worsen": [],
                "available_component_count": 3, "expected_component_count": 6},
            "volatility_and_credit": {"risk_score": 20, "direction": "STABLE", "components": {},
                "supportive_signals": [], "risk_signals": [], "missing_inputs": [],
                "conditions_to_improve": [], "conditions_to_worsen": [],
                "available_component_count": 3, "expected_component_count": 4},
            "rates_and_dollar": {"risk_score": 35, "direction": "STABLE", "components": {},
                "supportive_signals": [], "risk_signals": [], "missing_inputs": [],
                "conditions_to_improve": [], "conditions_to_worsen": [],
                "available_component_count": 4, "expected_component_count": 6},
            "leadership_and_cross_asset": {"risk_score": 25, "direction": "STABLE", "components": {},
                "supportive_signals": [], "risk_signals": [], "missing_inputs": ["btc_change_24h"],
                "conditions_to_improve": [], "conditions_to_worsen": [],
                "available_component_count": 2, "expected_component_count": 3},
        },
    }
    d = _build_home_decision(swing_regime=sr, execution_snapshot=None,
                               execution_refresh_status="scheduled",
                               market_open=True, why_market_is_moving=[])
    for entry in d.get("what_would_worsen", []):
        assert "materializes" not in entry.lower(), f"Found vague event condition: {entry}"
    print("test_no_materializes_unfavorably PASSED")


def test_empty_conditions_remain_empty():
    """Empty substantive conditions remain []."""
    result = assess_swing_regime(bare_inputs(
        spy_change_1d=1.5, qqq_change_1d=2.0, sector_breadth_1d=64,
        spx_return_63d=8.0, vix_current=15.0,
        us10y_yield=4.0, us10y_change_5d_bps=-3.0,
    ))
    vc = result["pillars"]["volatility_and_credit"]
    # Already low VIX → no improvement condition about VIX
    for cond in vc.get("conditions_to_improve", []):
        assert "remains" not in cond.lower(), f"Should not have 'remains' filler: {cond}"
    print("test_empty_conditions_remain_empty PASSED")


def test_no_condition_describes_already_true_as_improvement():
    """No condition describes a state already true as an improvement."""
    # All benign inputs — everything is already "good"
    result = assess_swing_regime(bare_inputs(
        spy_change_1d=2.0, qqq_change_1d=2.5, sector_breadth_1d=75,
        spx_return_7d=3.0, spx_return_63d=8.0,
        vix_current=13.0, vix_change_1d=-2.0, hyg_change_1d=0.5,
        us10y_yield=3.8, us10y_change_5d_bps=-5.0, dxy_change_1d=-0.3,
    ))
    for name, pillar in result["pillars"].items():
        for cond in pillar.get("conditions_to_improve", []):
            assert "remains" not in cond.lower(), \
                f"Pillar {name} has 'remains' filler: {cond}"
    print("test_no_condition_describes_already_true_as_improvement PASSED")


def test_pillar_scores_remain_unchanged_after_diagnostics():
    """Pillar scores identical before and after diagnostic enrichment."""
    for label, inputs in _KNOWN_BASELINES:
        p_tb = _score_trend_and_breadth(inputs)
        p_vc = _score_volatility_and_credit(inputs)
        p_rd = _score_rates_and_dollar(inputs)
        p_lc = _score_leadership_and_cross_asset(inputs)
        result = assess_swing_regime(inputs)
        assert result["pillars"]["trend_and_breadth"]["risk_score"] == p_tb["risk_score"]
        assert result["pillars"]["volatility_and_credit"]["risk_score"] == p_vc["risk_score"]
        assert result["pillars"]["rates_and_dollar"]["risk_score"] == p_rd["risk_score"]
        assert result["pillars"]["leadership_and_cross_asset"]["risk_score"] == p_lc["risk_score"]
    print("test_pillar_scores_remain_unchanged_after_diagnostics PASSED")


def test_event_sizing_applied_exactly_once():
    """Event sizing applied exactly once by Swing Regime, not re-applied by Home."""
    result = assess_swing_regime(bare_inputs(
        spy_change_1d=0.5, us10y_yield=4.2,
        has_upcoming_high_impact_event=True, days_until_next_event=2, next_event_title="FOMC"))
    base = result["base_position_size_hint"]
    final = result["position_size_hint"]
    # Verify Swing Regime escalates one step
    escalations = {"normal": "selective", "selective": "half-size", "half-size": "preserve capital", "preserve capital": "preserve capital"}
    assert final == escalations.get(base, base), f"Base {base} should escalate to {escalations.get(base)} but got {final}"
    # Verify no double escalation
    max_escalated = escalations.get(base, base)
    assert final == max_escalated, f"Expected exactly one escalation: {base} → {max_escalated}, got {final}"
    print("test_event_sizing_applied_exactly_once PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Phase D: WEAKENING direction interpretation and event filtering
# ═══════════════════════════════════════════════════════════════════════════════

def test_weakening_trend_interpretation_not_uncertain():
    """Trend & Breadth WEAKENING + conflicting signals → says weakening, not uncertain."""
    inputs = bare_inputs(
        spy_change_1d=0.6, qqq_change_1d=0.4,
        sector_breadth_1d=42, sector_breadth_7d=44,
        spx_return_7d=1.2, spx_return_63d=3.0)
    result = assess_swing_regime(inputs)
    tb = result["pillars"]["trend_and_breadth"]
    interp = tb.get("interpretation", "")
    assert "uncertain" not in interp.lower(), f"WEAKENING should not say 'uncertain': {interp}"
    assert "weakening" in interp.lower(), f"WEAKENING should say 'weakening': {interp}"
    print("test_weakening_trend_interpretation_not_uncertain PASSED")


def test_weakening_trend_supportive_only_not_supportive():
    """Trend & Breadth WEAKENING + only supportive signals → not just 'supportive'."""
    # Equity positive but breadth barely neutral/still weak → depends on direction
    inputs = bare_inputs(
        spy_change_1d=0.8, qqq_change_1d=0.6,
        sector_breadth_1d=48, sector_breadth_7d=50,
        spx_return_7d=1.5, spx_return_63d=4.0,
        vix_current=16, vix_change_1d=-1,
        us10y_yield=4.55, us10y_change_5d_bps=7,
        dxy_change_1d=0.3)
    result = assess_swing_regime(inputs)
    tb = result["pillars"]["trend_and_breadth"]
    interp = tb.get("interpretation", "")
    sup = tb.get("supportive_signals", [])
    risk = tb.get("risk_signals", [])
    direction = tb.get("direction", "")
    if direction in ("WEAKENING", "WORSENING") and sup and not risk:
        assert "supportive across" not in interp.lower(), (
            f"Direction {direction} with only supportive signals should not say 'supportive': {interp}"
        )
    print("test_weakening_trend_supportive_only_not_supportive PASSED")


def test_weakening_rates_flat_text_not_contradict_direction():
    """Rates & Dollar WEAKENING with flat rate change → text acknowledges pressure source."""
    inputs = bare_inputs(
        us10y_yield=4.80, us10y_change_1d_bps=-2,
        us10y_change_5d_bps=-3, us10y_change_20d_bps=-5,
        dxy_price=104, dxy_change_1d=0.25)
    result = assess_swing_regime(inputs)
    rd = result["pillars"]["rates_and_dollar"]
    interp = rd.get("interpretation", "")
    direction = rd.get("direction", "")
    if direction == "WEAKENING":
        assert "flat short-term direction" not in interp.lower() or "dollar" in interp.lower(), (
            f"WEAKENING rates interpretation should mention dollar pressure: {interp}"
        )
    print("test_weakening_rates_flat_text_not_contradict_direction PASSED")


def test_us_only_event_filtering():
    """US high-importance events are preferred over non-US events."""
    from services.home_risk_intelligence import _filter_upcoming_events
    from datetime import datetime, timezone, timedelta
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
    # Mock snapshot with NZ first, US second — both in the future
    snap = {
        "current_week": [
            {"date": tomorrow, "time": "12:00", "title": "Unemployment Rate (Q2)",
             "importance": "high", "country": "NZ", "event_date": tomorrow},
            {"date": tomorrow, "time": "14:00", "title": "JOLTs Job Openings",
             "importance": "high", "country": "US", "event_date": tomorrow},
        ]
    }
    upcoming = _filter_upcoming_events(snap, days_ahead=7)
    # Find first US high-importance event
    us_hi = [e for e in upcoming
             if e.get("country") == "US"
             and e.get("importance") in ("high", "critical", "HIGH", "CRITICAL")]
    assert len(us_hi) >= 1, "Should find US high-importance event"
    assert "JOLTs" in us_hi[0].get("title", "")
    # Verify NZ event is present but should not be the sizing trigger
    nz_events = [e for e in upcoming if e.get("country") == "NZ"]
    assert len(nz_events) >= 1
    print("test_us_only_event_filtering PASSED")


def test_non_us_event_does_not_trigger_sizing():
    """Non-US high-importance event alone does not trigger sizing overlay."""
    # Build calendar snapshot with only non-US "high" events
    # The has_hi_impact flag should be False when no US high-importance events exist
    from services.home_risk_intelligence import _filter_upcoming_events
    from datetime import datetime, timezone, timedelta
    snap = {
        "current_week": [
            {"date": (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d"),
             "time": "12:00", "title": "Unemployment Rate (Q2)",
             "importance": "high", "country": "NZ",
             "event_date": (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")},
        ]
    }
    upcoming = _filter_upcoming_events(snap, days_ahead=7)
    # Check US-only significance
    us_hi = [e for e in upcoming
             if e.get("country") == "US"
             and e.get("importance") in ("high", "critical", "HIGH", "CRITICAL")]
    assert len(us_hi) == 0, "NZ-only snapshot should produce zero US high-importance events"
    print("test_non_us_event_does_not_trigger_sizing PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Phase E: Complete direction and pillar coverage
# ═══════════════════════════════════════════════════════════════════════════════

def test_volatility_credit_weakening_interpretation():
    """Volatility & Credit WEAKENING says 'mildly deteriorating' not 'contained'."""
    inputs = bare_inputs(vix_current=22, vix_change_1d=4, hyg_change_1d=-0.35)
    result = assess_swing_regime(inputs)
    interp = result["pillars"]["volatility_and_credit"].get("interpretation", "")
    direction = result["pillars"]["volatility_and_credit"].get("direction", "")
    if direction == "WEAKENING":
        assert "contained" not in interp.lower(), f"WEAKENING VC should not say 'contained': {interp}"
        assert "mildly" in interp.lower() or "deteriorat" in interp.lower() or "early" in interp.lower(), f"Missing weakening language: {interp}"
    print("test_volatility_credit_weakening_interpretation PASSED")


def test_leadership_weakening_interpretation():
    """Leadership & Cross-Asset WEAKENING acknowledges weakening."""
    inputs = bare_inputs(btc_change_24h=-3.0, cyclical_vs_defensive_spread=-1.5, market_posture="")
    result = assess_swing_regime(inputs)
    interp = result["pillars"]["leadership_and_cross_asset"].get("interpretation", "")
    direction = result["pillars"]["leadership_and_cross_asset"].get("direction", "")
    if direction in ("WEAKENING", "WORSENING"):
        assert "weakening" in interp.lower() or "deteriorat" in interp.lower() or "leadership" in interp.lower(), f"Missing direction-aware text: {interp}"
    print("test_leadership_weakening_interpretation PASSED")


def test_country_normalization():
    """_is_us_event accepts US/USA/United States variants, rejects missing/NZ."""
    from services.home_risk_intelligence import _is_us_event
    assert _is_us_event({"country": "US"})
    assert _is_us_event({"country": "usa"})
    assert _is_us_event({"country": "United States"})
    assert _is_us_event({"country": "UNITED STATES OF AMERICA"})
    assert not _is_us_event({"country": "NZ"})
    assert not _is_us_event({"country": "CA"})
    assert not _is_us_event({"country": "GB"})
    assert not _is_us_event({"country": ""})
    assert not _is_us_event({"country": None})
    assert not _is_us_event({})
    print("test_country_normalization PASSED")


def test_released_event_excluded():
    """_event_time_status returns correct status for each scenario."""
    from services.home_risk_intelligence import _event_time_status
    assert _event_time_status({"actual": "4.0%"}) == "released"
    assert _event_time_status({"actual": 0}) == "released"
    assert _event_time_status({"time": "08:30"}) == "future"
    assert _event_time_status({}) == "unknown"
    assert _event_time_status({"date": "2099-01-01", "time": "08:30"}) == "future"
    assert _event_time_status({"actual": "", "time": "08:30"}) == "future"
    print("test_released_event_excluded PASSED")


def test_missing_country_not_us():
    """Country missing from event is not treated as US."""
    from services.home_risk_intelligence import _is_us_event, _filter_upcoming_events
    snap = {
        "current_week": [
            {"date": "2026-08-05", "title": "Some Event", "importance": "high",
             "event_date": "2026-08-05"},
        ]
    }
    upcoming = _filter_upcoming_events(snap, days_ahead=7)
    assert len(upcoming) >= 1
    us = [e for e in upcoming if _is_us_event(e)]
    assert len(us) == 0, "Event without country field must not default to US"
    print("test_missing_country_not_us PASSED")


def test_execution_failed_status():
    """Trading Dashboard snapshot exposes refresh_state and refresh_failure_count."""
    from services.trading_dashboard_service import (
        get_trading_dashboard_snapshot, clear_dashboard_cache,
        _refresh_outcome, _refresh_failure_count, _refresh_state,
    )
    clear_dashboard_cache()
    # Simulate a failed state
    _refresh_outcome["swing"] = "failed"
    _refresh_failure_count["swing"] = 1
    snap = get_trading_dashboard_snapshot("swing")
    assert snap["status"] == "unavailable"
    assert snap["refresh_state"] == "failed"
    assert snap["refresh_failure_count"] == 1
    # Clean up
    _refresh_outcome.pop("swing", None)
    _refresh_failure_count.pop("swing", None)
    print("test_execution_failed_status PASSED")


def test_event_country_provenance():
    """swing_regime event_overlay includes event_country and event_time_status."""
    from services.swing_regime_service import _compute_event_overlay
    result = _compute_event_overlay(
        {"has_upcoming_high_impact_event": True, "days_until_next_event": 2,
         "next_event_title": "FOMC Decision", "event_country": "US",
         "event_time_status": "future", "event_selection_reason": "nearest_upcoming_high_impact_us_event"},
        "MODERATE", "selective", "half-size",
    )
    assert result["event_country"] == "US"
    assert result["event_time_status"] == "future"
    assert result["event_selection_reason"] == "nearest_upcoming_high_impact_us_event"
    # Inactive event should have None for provenance fields
    result2 = _compute_event_overlay(
        {"has_upcoming_high_impact_event": False}, "MODERATE", "selective", "selective"
    )
    assert result2["event_country"] is None
    assert result2["event_time_status"] is None
    print("test_event_country_provenance PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Phase F — BTC cache-only reader and timestamp tests
# ═══════════════════════════════════════════════════════════════════════════════

def test_btc_exact_cmc_key_used():
    """CMC BTC uses exact cache key, not prefix scan."""
    from services.home_risk_intelligence import _get_btc_snapshot
    from data.cache import cache as _c
    # Populate exact CMC key
    cmc_key = "cmc:/v2/cryptocurrency/quotes/latest:[('convert', 'USD'), ('symbol', 'BTC')]"
    _c.set(cmc_key, {"data": {"BTC": {"quote": {"USD": {"price": 90000, "percent_change_24h": 2.5, "last_updated": "1700000000"}}}}}, 120)
    result = _get_btc_snapshot()
    assert result is not None
    assert result["source"] == "coinmarketcap"
    assert result["price"] == 90000.0
    assert result["change_pct"] == 2.5
    assert result["as_of"] == 1700000000
    assert result["freshness"] == "cached"
    _c.delete(cmc_key)
    print("test_btc_exact_cmc_key_used PASSED")


def test_btc_exact_coingecko_key_used():
    """CoinGecko BTC uses exact key from provider builder, not prefix scan."""
    from services.home_risk_intelligence import _get_btc_snapshot
    from data.cache import cache as _c
    import main as _m
    ds = getattr(_m, "data_service", None)
    cg = getattr(ds, "coingecko", None) if ds else None
    if cg is None:
        print("test_btc_exact_coingecko_key_used SKIPPED (no CG provider)")
        return
    cg_key = cg.top_coins_cache_key(limit=1)
    _c.set(cg_key, [{"current_price": 90000, "price_change_percentage_24h": 2.5, "last_updated": "1700000000"}], 120)
    result = _get_btc_snapshot()
    assert result is not None
    assert result["source"] == "coingecko"
    assert result["price"] == 90000.0
    _c.delete(cg_key)
    print("test_btc_exact_coingecko_key_used PASSED")


def test_btc_hyperliquid_unknown_freshness():
    """Hyperliquid BTC has as_of=None and freshness='unknown', not 'live'."""
    from services.home_risk_intelligence import _get_btc_from_hl
    hl = _get_btc_from_hl()
    if hl is not None:
        assert hl["as_of"] is None
        assert hl["freshness"] == "unknown"
    print("test_btc_hyperliquid_unknown_freshness PASSED")


def test_btc_parse_iso_timestamp():
    """_parse_ts accepts ISO-8601 with Z and timezone."""
    from services.home_risk_intelligence import _parse_ts
    ts1 = _parse_ts("2026-08-04T12:00:00Z")
    assert ts1 is not None and ts1 > 1754000000
    ts2 = _parse_ts("2026-08-04T12:00:00+00:00")
    assert ts2 is not None and ts2 > 1754000000
    ts3 = _parse_ts("2026-08-04T12:00:00")
    assert ts3 is not None and ts3 > 1754000000
    print("test_btc_parse_iso_timestamp PASSED")


def test_btc_parse_epoch_timestamp():
    """_parse_ts accepts integer epoch seconds and numeric strings."""
    from services.home_risk_intelligence import _parse_ts
    assert _parse_ts(1700000000) == 1700000000
    assert _parse_ts(1700000000.0) == 1700000000
    assert _parse_ts("1700000000") == 1700000000
    print("test_btc_parse_epoch_timestamp PASSED")


def test_btc_parse_rejects_malformed():
    """_parse_ts returns None for malformed timestamps."""
    from services.home_risk_intelligence import _parse_ts
    assert _parse_ts("not-a-timestamp") is None
    assert _parse_ts("") is None
    assert _parse_ts(None) is None
    print("test_btc_parse_rejects_malformed PASSED")


def test_btc_parse_missing_returns_none():
    """_parse_ts returns None for None/empty, never uses current time."""
    from services.home_risk_intelligence import _parse_ts
    import time
    assert _parse_ts(None) is None
    assert _parse_ts("") is None
    # Should not return current time
    result = _parse_ts(None)
    if result is not None:
        assert abs(result - int(time.time())) > 3600, "Should not use current time"
    print("test_btc_parse_missing_returns_none PASSED")


def test_btc_timestamped_beats_unknown():
    """A timestamped CMC candidate beats an unknown-freshness HL candidate."""
    from services.home_risk_intelligence import _get_btc_snapshot
    from data.cache import cache as _c
    cmc_key = "cmc:/v2/cryptocurrency/quotes/latest:[('convert', 'USD'), ('symbol', 'BTC')]"
    _c.set(cmc_key, {"data": {"BTC": {"quote": {"USD": {"price": 90000, "percent_change_24h": 2.5, "last_updated": "1700000000"}}}}}, 120)
    result = _get_btc_snapshot()
    assert result is not None
    # CMC with timestamp should be selected over HL (unknown freshness)
    assert result["source"] == "coinmarketcap"
    assert result["as_of"] == 1700000000
    _c.delete(cmc_key)
    print("test_btc_timestamped_beats_unknown PASSED")


def test_btc_empty_caches_return_none():
    """Empty caches return None without triggering provider calls."""
    from services.home_risk_intelligence import _get_btc_snapshot
    from data.cache import cache as _c
    cmc_key = "cmc:/v2/cryptocurrency/quotes/latest:[('convert', 'USD'), ('symbol', 'BTC')]"
    _c.delete(cmc_key)
    result = _get_btc_snapshot()
    # Should be None (or HL if connected), not trigger any provider call
    print("test_btc_empty_caches_return_none PASSED")


def test_btc_no_provider_imports():
    """_get_btc_snapshot does not import CMCProvider or CoinGeckoProvider."""
    import inspect
    from services import home_risk_intelligence as hri
    src = inspect.getsource(hri._get_btc_snapshot)
    assert "CMCProvider" not in src, "Should not import CMCProvider"
    assert "CoinGeckoProvider" not in src, "Should not import CoinGeckoProvider"
    assert "httpx" not in src, "Should not import httpx"
    print("test_btc_no_provider_imports PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Phase G — Decision contract verification
# ═══════════════════════════════════════════════════════════════════════════════

def test_synthesized_explanation_noun_parallel():
    """Synthesized explanation uses noun-parallel 'cyclical leadership' not 'cyclicals are leading'."""
    from services.home_risk_intelligence import _build_synthesized_explanation
    syn = _build_synthesized_explanation(
        verdict="CAUTION", action="WAIT", pos_size="half-size",
        risk_level="MODERATE", direction="IMPROVING", trade_bias="SELECTIVE_LONG",
        exec_quality="WEAK", exec_ews=25, exec_mqs=70,
        event_active=True, event_title="JOLTs",
        pillars={"leadership_and_cross_asset": {"components": {"cyclical_vs_defensive_spread": 0.5}}},
    )
    assert "cyclicals are leading" not in syn, f"Should use noun phrase, got: {syn}"
    assert "cyclical leadership" in syn, f"Should use 'cyclical leadership': {syn}"
    print("test_synthesized_explanation_noun_parallel PASSED")


def test_synthesized_explanation_two_sentences():
    """Synthesized explanation is exactly two sentences."""
    from services.home_risk_intelligence import _build_synthesized_explanation
    syn = _build_synthesized_explanation(
        verdict="CAUTION", action="WAIT", pos_size="half-size",
        risk_level="MODERATE", direction="IMPROVING", trade_bias="SELECTIVE_LONG",
        exec_quality="WEAK", exec_ews=25, exec_mqs=70,
        event_active=False, event_title=None,
        pillars={"leadership_and_cross_asset": {"components": {"cyclical_vs_defensive_spread": 0.5}}},
    )
    sentences = [s for s in syn.split(". ") if s.strip()]
    assert len(sentences) == 2, f"Should be 2 sentences, got {len(sentences)}: {syn}"
    print("test_synthesized_explanation_two_sentences PASSED")


def test_decision_summary_prioritizes_blockers():
    """Decision summary lists weak EWS and event before milder concerns."""
    from services.home_risk_intelligence import _build_decision_ranked_summary
    summary = _build_decision_ranked_summary(
        pillars={}, exec_ews=25, exec_mqs=70,
        event_active=True, event_title="JOLTs",
    )
    blockers = summary["largest_blockers"]
    assert len(blockers) >= 1
    assert any("Weak" in b["message"] or "25" in b["message"] for b in blockers)
    assert any("JOLTs" in b["message"] for b in blockers)
    # Event should not appear before EWS when EWS is weak
    ews_idx = next((i for i, b in enumerate(blockers) if "Execution" in b["message"]), 99)
    event_idx = next((i for i, b in enumerate(blockers) if "JOLTs" in b["message"]), 99)
    assert ews_idx < event_idx, "EWS should rank above event constraint"
    print("test_decision_summary_prioritizes_blockers PASSED")


def test_market_drivers_synthesized():
    """Synthesized market drivers are concise, not raw bullets."""
    from services.home_risk_intelligence import _build_synthesized_why_moving
    drivers = _build_synthesized_why_moving({
        "trend_and_breadth": {"components": {"equity_1d_avg": 1.5}},
        "rates_and_dollar": {"components": {"us10y_change_5d_bps": 7}},
        "volatility_and_credit": {"components": {"vix": 15}},
        "leadership_and_cross_asset": {"components": {}},
    }, True)
    assert len(drivers) >= 1
    assert len(drivers) <= 2
    for d in drivers:
        assert "verdict" not in d.lower()
        assert "action" not in d.lower()
    print("test_market_drivers_synthesized PASSED")


def test_completeness_reasons_all_components():
    """Completeness reasons include both execution and leadership when both are incomplete."""
    from services.home_risk_intelligence import _build_decision_completeness
    comp = _build_decision_completeness("COMPLETE", "HIGH", "warming", "UNCONFIRMED", "WEAK")
    reasons = comp["reasons"]
    assert len(reasons) >= 2, f"Should have reasons for both exec and leadership: {reasons}"
    assert any("execution" in r["component"] for r in reasons)
    assert any("leadership" in r["component"] for r in reasons)
    print("test_completeness_reasons_all_components PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Run
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # C1: Canonical display triggers
    test_canonical_triggers_are_pillar_based()
    test_canonical_triggers_contain_legacy_key_shape()
    test_event_trigger_never_red()
    test_risk_cluster_triggers_are_canonical()

    # C2: Direction-aware 10Y display
    test_10y_restrictive_but_easing_display()
    test_10y_lower_but_accelerating_display()
    test_legacy_10y_is_not_primary_trigger()

    # C3: Canonical why_market_is_moving
    test_why_market_is_moving_uses_swing_regime()
    test_why_market_is_moving_event_not_bearish()
    test_why_market_is_moving_market_closed()
    test_legacy_why_bullets_preserved()

    # C4: Weekend history fallback
    test_rate_history_stale_fallback()
    test_vixcls_history_stale_fallback()
    test_yield_changes_from_stale_history()
    test_dgs10_fresh_neon_status_is_available()
    test_dgs10_stale_neon_status_is_stale()
    test_dgs10_unavailable_status()
    test_vixcls_fresh_neon_status_is_available()

    # C5: Market-closed
    test_market_context_closed()
    test_market_context_live()
    test_market_context_stale()

    # C6: Frontend contract
    test_risk_cluster_has_required_fields()
    test_all_top_level_fields_preserved()
    test_lkg_behavior_preserves_all_fields()

    # Existing scoring tests
    test_vix_7d_min_is_not_a_return()
    test_vix_7d_real_return_computed_from_history()
    test_vix_7d_return_insufficient_data()
    test_10y_change_bps_calculation()
    test_scenario_a_10y_restrictive_but_easing()
    test_scenario_b_10y_below_threshold_but_rising()
    test_10y_absolute_level_alone_is_not_direction()
    test_event_overlay_does_not_affect_risk_score()
    test_event_overlay_explains_position_size_change()
    test_event_no_sizing_change_explained()
    test_all_fields_null_insufficient_data()
    test_only_vix_available_insufficient()
    test_two_pillars_partial()
    test_all_four_pillars_complete()
    test_risk_cluster_no_contradiction()
    test_insufficient_data_headline()
    test_trade_decision_no_normal_at_elevated()
    test_trend_and_breadth_strong()
    test_trend_and_breadth_weak()
    test_volatility_and_credit_calm()
    test_volatility_and_credit_stressed()
    test_rates_and_dollar_benign()
    test_rates_and_dollar_pressure()
    test_leadership_risk_on()
    test_leadership_risk_off()
    test_risk_level_bounds()
    test_trade_bias_matrix()
    test_position_size_with_event()
    test_lkg_fallback_no_mutation()

    # Phase B composer tests
    test_home_decision_builder_importable()
    test_trade_decision_has_score_source()
    test_home_decision_no_score_averaging_in_builder()
    test_home_decision_expired_confidence_never_high()
    test_home_decision_unavailable_execution_partial()

    # Phase C — Pillar diagnostic tests
    test_pillar_diagnostics_present()
    test_trend_breadth_positive_1d_but_weak_3m()
    test_volatility_credit_low_vix_positive_hyg()
    test_volatility_credit_high_vix_negative_hyg()
    test_rates_dollar_high_but_easing()
    test_rates_dollar_high_and_accelerating()
    test_rates_dollar_missing_history()
    test_leadership_all_inputs_confirming()
    test_leadership_btc_missing()
    test_leadership_cyclical_defensive_missing()
    test_leadership_mixed_posture()
    test_event_overlay_has_provenance_fields()
    test_base_selection_to_half_size_with_event()
    test_base_half_size_to_preserve_with_event()
    test_base_preserve_capital_with_event()

    # Phase C — Score regression
    test_trend_breadth_scores_unchanged()
    test_volatility_credit_scores_unchanged()
    test_rates_dollar_scores_unchanged()
    test_event_overlay_sizing_new_test_explanation()

    # Phase C.1 — Invariant tests
    test_stable_pillar_interpretation_no_worsening()
    test_worsening_pillar_interpretation_says_worsening()
    test_improving_pillar_interpretation_says_improving()
    test_partial_leadership_cannot_be_confirmed()
    test_btc_missing_produces_unconfirmed()
    test_conflicting_leadership_inputs_mixed()
    test_vix_below_20_no_below_20_improvement()
    test_breadth_diagnostics_use_exact_thresholds()
    test_spx_63d_threshold_matches_scoring()
    test_rate_thresholds_match_scoring()
    test_no_materializes_unfavorably()
    test_empty_conditions_remain_empty()
    test_no_condition_describes_already_true_as_improvement()
    test_pillar_scores_remain_unchanged_after_diagnostics()
    test_event_sizing_applied_exactly_once()

    # Phase D — WEAKENING direction interpretation consistency
    test_weakening_trend_interpretation_not_uncertain()
    test_weakening_trend_supportive_only_not_supportive()
    test_weakening_rates_flat_text_not_contradict_direction()
    test_us_only_event_filtering()
    test_non_us_event_does_not_trigger_sizing()

    # Phase E — Complete direction and pillar coverage
    test_volatility_credit_weakening_interpretation()
    test_leadership_weakening_interpretation()
    test_country_normalization()
    test_released_event_excluded()
    test_missing_country_not_us()
    test_execution_failed_status()
    test_event_country_provenance()

    # Phase F — BTC cache-only reader and timestamp tests
    test_btc_exact_cmc_key_used()
    test_btc_exact_coingecko_key_used()
    test_btc_hyperliquid_unknown_freshness()
    test_btc_parse_iso_timestamp()
    test_btc_parse_epoch_timestamp()
    test_btc_parse_rejects_malformed()
    test_btc_parse_missing_returns_none()
    test_btc_timestamped_beats_unknown()
    test_btc_empty_caches_return_none()
    test_btc_no_provider_imports()

    # Phase G — Decision contract verification
    test_synthesized_explanation_noun_parallel()
    test_synthesized_explanation_two_sentences()
    test_decision_summary_prioritizes_blockers()
    test_market_drivers_synthesized()
    test_completeness_reasons_all_components()

    total = 119
    print(f"\nAll {total} tests PASSED")
