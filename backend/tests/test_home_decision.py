"""
Unit tests for _build_home_decision — pure decision matrix.

No network, no DB, no provider calls.
"""
from __future__ import annotations

import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parent.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from services.home_risk_intelligence import _build_home_decision, _execution_quality, _most_conservative


def _make_regime(
    risk_level="MODERATE",
    direction="STABLE",
    bias="SELECTIVE_LONG",
    assessment="COMPLETE",
    risk_score=43,
    pos_size="selective",
    event_active=False,
    flip_conditions=None,
    pillars=None,
):
    if pillars is None:
        pillars = {}
    if flip_conditions is None:
        flip_conditions = ["Breadth rises above 50"]
    return {
        "risk_level": risk_level,
        "risk_score": risk_score,
        "regime_direction": direction,
        "trade_bias": bias,
        "assessment_status": assessment,
        "position_size_hint": pos_size,
        "one_line": f"{risk_level} risk, {direction.lower()}",
        "event_overlay": {
            "active": event_active,
            "severity": "HIGH" if event_active else "NONE",
            "next_event": "CPI" if event_active else None,
            "days_until_event": 1 if event_active else None,
        },
        "conditions_that_would_flip": flip_conditions,
        "pillars": pillars,
    }


def _make_exec_snapshot(status="available", mqs=62.0, ews=50.0, decision="CAUTION", age=45.0):
    if status == "unavailable":
        return {"status": "unavailable", "dashboard": None, "age_seconds": None, "expired": None}
    return {
        "status": status,
        "age_seconds": age,
        "expired": status == "expired",
        "dashboard": {
            "market_quality_score": mqs,
            "execution_window_score": ews,
            "decision": decision,
            "as_of": "2026-08-03T12:00:00Z",
            "from_cache": True,
            "pillars": [],
            "execution_conditions": [],
        },
    }


def _hd(sr=None, es=None, refresh="not_needed", market_open=True):
    if sr is None:
        sr = _make_regime()
    if es is None:
        es = _make_exec_snapshot()
    return _build_home_decision(
        swing_regime=sr,
        execution_snapshot=es,
        execution_refresh_status=refresh,
        market_open=market_open,
        why_market_is_moving=["test bullet"],
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Execution quality
# ═══════════════════════════════════════════════════════════════════════════════

def test_exec_quality_strong():
    assert _execution_quality(80.0, 80.0) == "STRONG"
    assert _execution_quality(70.0, 75.0) == "STRONG"
    print("test_exec_quality_strong PASSED")

def test_exec_quality_mixed():
    assert _execution_quality(55.0, 55.0) == "MIXED"
    assert _execution_quality(69.0, 60.0) == "MIXED"
    print("test_exec_quality_mixed PASSED")

def test_exec_quality_weak():
    assert _execution_quality(30.0, 30.0) == "WEAK"
    assert _execution_quality(80.0, 40.0) == "WEAK"
    print("test_exec_quality_weak PASSED")

def test_exec_quality_unavailable():
    assert _execution_quality(None, None) == "UNAVAILABLE"
    assert _execution_quality(None, 80.0) == "UNAVAILABLE"
    print("test_exec_quality_unavailable PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Position size conservatism
# ═══════════════════════════════════════════════════════════════════════════════

def test_most_conservative():
    assert _most_conservative("normal", "selective") == "selective"
    assert _most_conservative("selective", "half-size") == "half-size"
    assert _most_conservative("half-size", "preserve capital") == "preserve capital"
    assert _most_conservative("normal", "preserve capital") == "preserve capital"
    print("test_most_conservative PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Matrix: LOW risk
# ═══════════════════════════════════════════════════════════════════════════════

def test_low_improving_strong():
    """LOW + IMPROVING + STRONG → YES / PRESS / normal"""
    sr = _make_regime("LOW", "IMPROVING", "LONG", pos_size="normal")
    es = _make_exec_snapshot(mqs=75.0, ews=80.0, decision="YES")
    d = _hd(sr, es)
    assert d["verdict"] == "YES"
    assert d["action"] == "PRESS"
    assert d["position_size_hint"] == "normal"
    assert d["execution"]["status"] == "available"
    print("test_low_improving_strong PASSED")

def test_low_stable_strong():
    """LOW + STABLE + STRONG → YES / SELECTIVE / normal"""
    sr = _make_regime("LOW", "STABLE", "SELECTIVE_LONG", pos_size="normal")
    es = _make_exec_snapshot(mqs=75.0, ews=80.0, decision="YES")
    d = _hd(sr, es)
    assert d["verdict"] == "YES"
    assert d["action"] == "SELECTIVE"
    print("test_low_stable_strong PASSED")

def test_low_mixed():
    """LOW + MIXED → CAUTION / SELECTIVE"""
    sr = _make_regime("LOW", "STABLE", "LONG", pos_size="normal")
    es = _make_exec_snapshot(mqs=55.0, ews=55.0, decision="CAUTION")
    d = _hd(sr, es)
    assert d["verdict"] == "CAUTION"
    assert d["action"] == "SELECTIVE"
    print("test_low_mixed PASSED")

def test_low_weak():
    """LOW + WEAK → CAUTION / WAIT / half-size"""
    sr = _make_regime("LOW", "STABLE", "LONG", pos_size="normal")
    es = _make_exec_snapshot(mqs=30.0, ews=30.0, decision="NO")
    d = _hd(sr, es)
    assert d["verdict"] == "CAUTION"
    assert d["action"] == "WAIT"
    assert d["position_size_hint"] == "half-size"
    print("test_low_weak PASSED")

def test_low_worsening():
    """LOW + WORSENING → CAUTION / WAIT / half-size"""
    sr = _make_regime("LOW", "WORSENING", "NEUTRAL", pos_size="normal")
    es = _make_exec_snapshot(mqs=75.0, ews=80.0, decision="YES")
    d = _hd(sr, es)
    assert d["verdict"] == "CAUTION"
    assert d["action"] == "WAIT"
    assert d["position_size_hint"] == "half-size"
    print("test_low_worsening PASSED")

def test_low_unavailable():
    """LOW + UNAVAILABLE → CAUTION / SELECTIVE / selective"""
    sr = _make_regime("LOW", "STABLE", "LONG", pos_size="normal")
    es = _make_exec_snapshot(status="unavailable")
    d = _hd(sr, es)
    assert d["verdict"] == "CAUTION"
    assert d["action"] == "SELECTIVE"
    assert d["confidence"] != "HIGH"
    print("test_low_unavailable PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Matrix: MODERATE risk
# ═══════════════════════════════════════════════════════════════════════════════

def test_moderate_improving_strong():
    """MODERATE + IMPROVING + STRONG → YES / SELECTIVE"""
    sr = _make_regime("MODERATE", "IMPROVING", "SELECTIVE_LONG")
    es = _make_exec_snapshot(mqs=75.0, ews=80.0, decision="YES")
    d = _hd(sr, es)
    assert d["verdict"] == "YES"
    assert d["action"] == "SELECTIVE"
    print("test_moderate_improving_strong PASSED")

def test_moderate_stable_mixed():
    """MODERATE + STABLE + MIXED → CAUTION / SELECTIVE"""
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG")
    es = _make_exec_snapshot(mqs=55.0, ews=55.0, decision="CAUTION")
    d = _hd(sr, es)
    assert d["verdict"] == "CAUTION"
    assert d["action"] == "SELECTIVE"
    print("test_moderate_stable_mixed PASSED")

def test_moderate_worsening():
    """MODERATE + WORSENING → CAUTION / WAIT"""
    sr = _make_regime("MODERATE", "WORSENING", "NEUTRAL")
    es = _make_exec_snapshot(mqs=75.0, ews=80.0, decision="YES")
    d = _hd(sr, es)
    assert d["verdict"] == "CAUTION"
    assert d["action"] == "WAIT"
    print("test_moderate_worsening PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Matrix: ELEVATED risk
# ═══════════════════════════════════════════════════════════════════════════════

def test_elevated_improving_strong():
    """ELEVATED + IMPROVING + STRONG → CAUTION / SELECTIVE"""
    sr = _make_regime("ELEVATED", "IMPROVING", "SELECTIVE_LONG", pos_size="selective")
    es = _make_exec_snapshot(mqs=75.0, ews=80.0, decision="YES")
    d = _hd(sr, es)
    assert d["verdict"] == "CAUTION"
    assert d["action"] == "SELECTIVE"
    print("test_elevated_improving_strong PASSED")

def test_elevated_stable():
    """ELEVATED + STABLE → CAUTION / WAIT"""
    sr = _make_regime("ELEVATED", "STABLE", "NEUTRAL", pos_size="selective")
    es = _make_exec_snapshot(mqs=55.0, ews=55.0, decision="CAUTION")
    d = _hd(sr, es)
    assert d["verdict"] == "CAUTION"
    assert d["action"] == "WAIT"
    print("test_elevated_stable PASSED")

def test_elevated_worsening():
    """ELEVATED + WORSENING → NO / REDUCE"""
    sr = _make_regime("ELEVATED", "WORSENING", "SELECTIVE_SHORT", pos_size="half-size")
    es = _make_exec_snapshot()
    d = _hd(sr, es)
    assert d["verdict"] == "NO"
    assert d["action"] == "REDUCE"
    assert d["position_size_hint"] == "preserve capital"
    print("test_elevated_worsening PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Matrix: HIGH risk
# ═══════════════════════════════════════════════════════════════════════════════

def test_high_improving_strong():
    """HIGH + IMPROVING + STRONG → CAUTION / SELECTIVE / half-size"""
    sr = _make_regime("HIGH", "IMPROVING", "NEUTRAL", pos_size="half-size")
    es = _make_exec_snapshot(mqs=75.0, ews=80.0, decision="YES")
    d = _hd(sr, es)
    assert d["verdict"] == "CAUTION"
    assert d["action"] == "SELECTIVE"
    assert d["position_size_hint"] == "half-size"
    print("test_high_improving_strong PASSED")

def test_high_stable():
    """HIGH + STABLE → NO / REDUCE"""
    sr = _make_regime("HIGH", "STABLE", "SELECTIVE_SHORT", pos_size="half-size")
    es = _make_exec_snapshot()
    d = _hd(sr, es)
    assert d["verdict"] == "NO"
    assert d["action"] == "REDUCE"
    print("test_high_stable PASSED")

def test_high_worsening():
    """HIGH + WORSENING → NO / HEDGE"""
    sr = _make_regime("HIGH", "WORSENING", "SHORT_HEDGE", pos_size="half-size")
    es = _make_exec_snapshot()
    d = _hd(sr, es)
    assert d["verdict"] == "NO"
    assert d["action"] == "HEDGE"
    print("test_high_worsening PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Matrix: EXTREME risk
# ═══════════════════════════════════════════════════════════════════════════════

def test_extreme_improving():
    """EXTREME + IMPROVING → CAUTION / WAIT"""
    sr = _make_regime("EXTREME", "IMPROVING", "NEUTRAL", pos_size="preserve capital")
    es = _make_exec_snapshot(mqs=75.0, ews=80.0, decision="YES")
    d = _hd(sr, es)
    assert d["verdict"] == "CAUTION"
    assert d["action"] == "WAIT"
    assert d["position_size_hint"] == "preserve capital"
    print("test_extreme_improving PASSED")

def test_extreme_stable():
    """EXTREME + STABLE → NO / REDUCE"""
    sr = _make_regime("EXTREME", "STABLE", "SHORT_HEDGE", pos_size="preserve capital")
    es = _make_exec_snapshot()
    d = _hd(sr, es)
    assert d["verdict"] == "NO"
    assert d["action"] == "REDUCE"
    print("test_extreme_stable PASSED")

def test_extreme_worsening():
    """EXTREME + WORSENING → NO / HEDGE"""
    sr = _make_regime("EXTREME", "WORSENING", "SHORT_HEDGE", pos_size="preserve capital")
    es = _make_exec_snapshot()
    d = _hd(sr, es)
    assert d["verdict"] == "NO"
    assert d["action"] == "HEDGE"
    print("test_extreme_worsening PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Hard guardrails
# ═══════════════════════════════════════════════════════════════════════════════

def test_strong_execution_never_overrides_extreme_worsening():
    """Strong execution may NOT create YES during EXTREME + WORSENING."""
    sr = _make_regime("EXTREME", "WORSENING", "SHORT_HEDGE", pos_size="preserve capital")
    es = _make_exec_snapshot(mqs=95.0, ews=100.0, decision="YES")
    d = _hd(sr, es)
    assert d["verdict"] == "NO", f"Got {d['verdict']} — extreme worsening must be NO"
    print("test_strong_execution_never_overrides_extreme_worsening PASSED")

def test_strong_execution_never_overrides_high_worsening():
    """Strong execution may NOT create YES during HIGH + WORSENING."""
    sr = _make_regime("HIGH", "WORSENING", "SHORT_HEDGE", pos_size="half-size")
    es = _make_exec_snapshot(mqs=95.0, ews=100.0, decision="YES")
    d = _hd(sr, es)
    assert d["verdict"] == "NO"
    print("test_strong_execution_never_overrides_high_worsening PASSED")

def test_weak_execution_prevents_press_in_low_risk():
    """Poor execution prevents PRESS even in LOW risk."""
    sr = _make_regime("LOW", "IMPROVING", "LONG", pos_size="normal")
    es = _make_exec_snapshot(mqs=30.0, ews=25.0, decision="NO")
    d = _hd(sr, es)
    assert d["action"] != "PRESS", f"Weak execution should not produce PRESS, got {d['action']}"
    print("test_weak_execution_prevents_press_in_low_risk PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Insufficient data
# ═══════════════════════════════════════════════════════════════════════════════

def test_insufficient_data():
    """INSUFFICIENT_DATA → CAUTION / WAIT / LOW confidence"""
    sr = _make_regime("MODERATE", "UNKNOWN", "NEUTRAL", assessment="INSUFFICIENT_DATA")
    es = _make_exec_snapshot()
    d = _hd(sr, es)
    assert d["verdict"] == "CAUTION"
    assert d["action"] == "WAIT"
    assert d["confidence"] == "LOW"
    assert d["assessment_status"] == "INSUFFICIENT_DATA"
    print("test_insufficient_data PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Event overlay
# ═══════════════════════════════════════════════════════════════════════════════

def test_event_overlay_does_not_change_verdict():
    """Event overlay changes position size but NOT verdict or action."""
    sr = _make_regime("LOW", "IMPROVING", "LONG", pos_size="normal", event_active=True)
    es = _make_exec_snapshot(mqs=75.0, ews=80.0, decision="YES")
    d = _hd(sr, es)
    assert d["verdict"] == "YES"  # same verdict
    assert d["action"] == "PRESS"  # same action
    assert d["position_size_hint"] == "selective"  # downsized from normal
    print("test_event_overlay_does_not_change_verdict PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Market closed
# ═══════════════════════════════════════════════════════════════════════════════

def test_market_closed_one_line_labels():
    """Market-closed one_line mentions latest completed session."""
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG")
    es = _make_exec_snapshot()
    d = _hd(sr, es, market_open=False)
    assert d["market_context"] == "closed_last_session"
    assert "latest completed" in d["one_line"].lower()
    print("test_market_closed_one_line_labels PASSED")

def test_market_closed_same_decision():
    """Market closed does not change verdict."""
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG")
    es = _make_exec_snapshot()
    d_open = _hd(sr, es, market_open=True)
    d_closed = _hd(sr, es, market_open=False)
    assert d_open["verdict"] == d_closed["verdict"]
    assert d_open["action"] == d_closed["action"]
    print("test_market_closed_same_decision PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Expired execution restrictions
# ═══════════════════════════════════════════════════════════════════════════════

def test_expired_execution_never_yes():
    """Expired snapshot may never produce YES or PRESS."""
    sr = _make_regime("LOW", "IMPROVING", "LONG", pos_size="normal")
    es = _make_exec_snapshot(status="expired", mqs=85.0, ews=100.0, decision="YES", age=700.0)
    d = _hd(sr, es, refresh="scheduled")
    assert d["verdict"] != "YES"
    assert d["action"] != "PRESS"
    assert d["execution"]["status"] == "expired"
    print("test_expired_execution_never_yes PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Score independence
# ═══════════════════════════════════════════════════════════════════════════════

def test_no_score_averaging():
    """Home decision does not average risk_score and MQS."""
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG", risk_score=43)
    es = _make_exec_snapshot(mqs=62.0, ews=50.0)
    d = _hd(sr, es)
    assert d["regime"]["risk_score"] == 43
    assert d["execution"]["market_quality_score"] == 62.0
    # No combined score field
    assert "score" not in d
    print("test_no_score_averaging PASSED")


def test_no_dependence_on_trade_decision_score():
    """Home decision does not use trade_decision.score."""
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG")
    es = _make_exec_snapshot()
    # Function doesn't even receive trade_decision
    d = _hd(sr, es)
    assert isinstance(d, dict)
    print("test_no_dependence_on_trade_decision_score PASSED")


def test_valid_numeric_zero():
    """Zero MQS/EWS values are preserved, not replaced."""
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG")
    es = _make_exec_snapshot(mqs=0.0, ews=0.0, decision="NO")
    d = _hd(sr, es)
    assert d["execution"]["market_quality_score"] == 0.0
    assert d["execution"]["execution_window_score"] == 0.0
    assert d["execution"]["quality"] == "WEAK"
    print("test_valid_numeric_zero PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Response contract
# ═══════════════════════════════════════════════════════════════════════════════

def test_home_decision_has_required_sections():
    """All required sections present."""
    d = _hd()
    top = {"version", "calibration_status", "verdict", "action", "one_line",
           "position_size_hint", "confidence", "assessment_status", "market_context",
           "regime", "execution", "why_now", "buy_reasons", "wait_reasons",
           "reduce_reasons", "what_would_improve", "what_would_worsen"}
    assert top <= set(d.keys()), f"Missing: {top - set(d.keys())}"

    reg = {"risk_score", "risk_level", "direction", "trade_bias",
           "position_size_hint", "assessment_status"}
    assert reg <= set(d["regime"].keys()), f"Missing regime: {reg - set(d['regime'].keys())}"

    exe = {"status", "refresh_status", "quality", "market_quality_score",
           "execution_window_score", "decision", "mode", "as_of",
           "age_seconds", "from_cache", "expired"}
    assert exe <= set(d["execution"].keys()), f"Missing exec: {exe - set(d['execution'].keys())}"

    print("test_home_decision_has_required_sections PASSED")


def test_reasons_are_capped():
    """Reason arrays are capped at 3 items."""
    d = _hd()
    assert len(d["buy_reasons"]) <= 3
    assert len(d["wait_reasons"]) <= 3
    assert len(d["reduce_reasons"]) <= 3
    assert len(d["what_would_improve"]) <= 4
    assert len(d["what_would_worsen"]) <= 4
    print("test_reasons_are_capped PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Position-size ceiling
# ═══════════════════════════════════════════════════════════════════════════════

def test_regime_size_ceiling():
    """Decision never exceeds regime position size."""
    sr = _make_regime("LOW", "IMPROVING", "LONG", pos_size="selective")
    es = _make_exec_snapshot(mqs=95.0, ews=100.0, decision="YES")
    d = _hd(sr, es)
    assert d["position_size_hint"] == "selective"  # capped by regime "selective"
    print("test_regime_size_ceiling PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Run
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    test_exec_quality_strong()
    test_exec_quality_mixed()
    test_exec_quality_weak()
    test_exec_quality_unavailable()
    test_most_conservative()

    test_low_improving_strong()
    test_low_stable_strong()
    test_low_mixed()
    test_low_weak()
    test_low_worsening()
    test_low_unavailable()

    test_moderate_improving_strong()
    test_moderate_stable_mixed()
    test_moderate_worsening()

    test_elevated_improving_strong()
    test_elevated_stable()
    test_elevated_worsening()

    test_high_improving_strong()
    test_high_stable()
    test_high_worsening()

    test_extreme_improving()
    test_extreme_stable()
    test_extreme_worsening()

    test_strong_execution_never_overrides_extreme_worsening()
    test_strong_execution_never_overrides_high_worsening()
    test_weak_execution_prevents_press_in_low_risk()

    test_insufficient_data()

    test_event_overlay_does_not_change_verdict()
    test_market_closed_one_line_labels()
    test_market_closed_same_decision()

    test_expired_execution_never_yes()

    test_no_score_averaging()
    test_no_dependence_on_trade_decision_score()
    test_valid_numeric_zero()

    test_home_decision_has_required_sections()
    test_reasons_are_capped()
    test_regime_size_ceiling()

    print("\nAll 34 tests PASSED")
