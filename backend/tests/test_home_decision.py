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
    sr = _make_regime("LOW", "IMPROVING", "LONG", pos_size="selective", event_active=True)
    es = _make_exec_snapshot(mqs=75.0, ews=80.0, decision="YES")
    d = _hd(sr, es)
    assert d["verdict"] == "YES"  # same verdict
    assert d["action"] == "PRESS"  # same action
    assert d["position_size_hint"] == "selective"  # Swing Regime already applied event sizing; Home does not re-apply
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
           "position_size_hint", "sizing", "signal_summary", "confidence",
           "assessment_status", "market_context",
           "regime", "execution", "why_now", "buy_reasons", "wait_reasons",
           "reduce_reasons", "what_would_improve", "what_would_worsen"}
    assert top <= set(d.keys()), f"Missing: {top - set(d.keys())}"

    reg = {"risk_score", "risk_level", "direction", "trade_bias",
           "position_size_hint", "assessment_status"}
    assert reg <= set(d["regime"].keys()), f"Missing regime: {reg - set(d['regime'].keys())}"

    exe = {"status", "refresh_status", "quality", "market_quality_score",
           "execution_window_score", "decision", "mode", "as_of",
           "age_seconds", "from_cache", "expired",
           "recommended_refetch_seconds", "refresh_in_progress"}
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
# Language hardening tests
# ═══════════════════════════════════════════════════════════════════════════════

_RAW_ENUM_LIKE = ("MODERATE", "ELEVATED", "EXTREME", "IMPROVING", "WORSENING",
                  "CAUTION", "SELECTIVE_LONG", "SHORT_HEDGE", "UNKNOWN",
                  "STRONG", "MIXED", "PRESS", "HEDGE")

def _no_raw_enums_in(text: str) -> bool:
    words = set(text.replace(".", " ").replace(",", " ").replace("/", " ").split())
    for enum_val in _RAW_ENUM_LIKE:
        if enum_val in words:
            return False
    return True

def test_one_line_no_repeated_selective():
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG", pos_size="selective")
    es = _make_exec_snapshot(mqs=62.0, ews=50.0, decision="CAUTION")
    d = _hd(sr, es)
    count = d["one_line"].lower().count("selective")
    assert count <= 1, f"Too many 'selective': {d['one_line']}"
    print("test_one_line_no_repeated_selective PASSED")

def test_one_line_no_raw_enum_casing():
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG")
    es = _make_exec_snapshot()
    d = _hd(sr, es)
    assert _no_raw_enums_in(d["one_line"]), f"Raw enum in: {d['one_line']}"
    print("test_one_line_no_raw_enum_casing PASSED")

def test_why_now_no_repeated_words():
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG", pos_size="selective")
    es = _make_exec_snapshot()
    d = _hd(sr, es)
    for bullet in d["why_now"]:
        assert "SELECTIVE selective" not in bullet, f"Duplicate: {bullet}"
        assert "selective entries at selective" not in bullet.lower()
    print("test_why_now_no_repeated_words PASSED")

def test_why_now_no_raw_enum_casing():
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG")
    es = _make_exec_snapshot()
    d = _hd(sr, es)
    for bullet in d["why_now"]:
        assert _no_raw_enums_in(bullet), f"Raw enum in: {bullet}"
    print("test_why_now_no_raw_enum_casing PASSED")

def test_moderate_not_broadly_supportive():
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG")
    es = _make_exec_snapshot(mqs=62.0, ews=50.0, decision="CAUTION")
    d = _hd(sr, es)
    assert "broadly supportive" not in d["one_line"].lower()
    for r in d["buy_reasons"]:
        assert "broadly supportive" not in r.lower()
    print("test_moderate_not_broadly_supportive PASSED")

def test_moderate_stable_mixed_coherent():
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG")
    es = _make_exec_snapshot(mqs=62.0, ews=50.0, decision="CAUTION")
    d = _hd(sr, es)
    assert d["verdict"] == "CAUTION"; assert d["action"] == "SELECTIVE"
    one_line = d["one_line"]
    assert "Moderate" in one_line
    assert "selective entries" in one_line.lower() or "selective " in one_line.lower()
    assert "broad aggressive" in one_line.lower() or "mixed execution" in one_line.lower()
    bullets = d["why_now"]
    assert any("Decision: Caution" in b for b in bullets)
    assert any("Regime: Moderate" in b for b in bullets)
    assert any("Execution:" in b and "Mixed" in b for b in bullets)
    assert len(d["wait_reasons"]) >= 1
    assert all("broadly" not in r.lower() for r in d["buy_reasons"])
    assert d["reduce_reasons"] == []
    print("test_moderate_stable_mixed_coherent PASSED")

def test_caution_selective_reason_invariants():
    for risk in ("LOW", "MODERATE", "ELEVATED", "HIGH", "EXTREME"):
        for direc in ("IMPROVING", "STABLE"):
            sr = _make_regime(risk, direc, "SELECTIVE_LONG", pos_size="selective")
            es = _make_exec_snapshot(mqs=65.0, ews=55.0, decision="CAUTION")
            d = _hd(sr, es)
            if d["verdict"] == "CAUTION" and d["action"] == "SELECTIVE":
                assert len(d["wait_reasons"]) >= 1, f"({risk},{direc}): empty wait_reasons"
    print("test_caution_selective_reason_invariants PASSED")

def test_caution_wait_buy_reasons_restricted():
    sr = _make_regime("MODERATE", "STABLE", "NEUTRAL")
    es = _make_exec_snapshot(mqs=30.0, ews=25.0, decision="NO")
    d = _hd(sr, es)
    assert d["verdict"] == "CAUTION"; assert d["action"] == "WAIT"
    assert d["buy_reasons"] == []
    sr2 = _make_regime("MODERATE", "IMPROVING", "SELECTIVE_LONG")
    d2 = _hd(sr2, es)
    assert len(d2["buy_reasons"]) <= 1
    print("test_caution_wait_buy_reasons_restricted PASSED")

def test_yes_buy_reasons_nonempty():
    sr = _make_regime("LOW", "IMPROVING", "LONG", pos_size="normal")
    es = _make_exec_snapshot(mqs=75.0, ews=80.0, decision="YES")
    d = _hd(sr, es)
    assert d["verdict"] == "YES"; assert d["action"] == "PRESS"
    assert len(d["buy_reasons"]) >= 1
    assert d["reduce_reasons"] == []
    assert len(d["wait_reasons"]) <= 1
    print("test_yes_buy_reasons_nonempty PASSED")

def test_no_buy_reasons_empty():
    for risk_level, direction in [("HIGH", "STABLE"), ("EXTREME", "WORSENING")]:
        sr = _make_regime(risk_level, direction, "SHORT_HEDGE", pos_size="half-size")
        es = _make_exec_snapshot()
        d = _hd(sr, es)
        assert d["verdict"] == "NO"
        assert d["buy_reasons"] == [], f"({risk_level},{direction}): {d['buy_reasons']}"
        assert len(d["reduce_reasons"]) >= 1
    print("test_no_buy_reasons_empty PASSED")

def test_insufficient_data_buy_reasons_empty():
    sr = _make_regime("MODERATE", "UNKNOWN", "NEUTRAL", assessment="INSUFFICIENT_DATA")
    es = _make_exec_snapshot()
    d = _hd(sr, es)
    assert d["buy_reasons"] == []
    assert len(d["wait_reasons"]) >= 1
    print("test_insufficient_data_buy_reasons_empty PASSED")

def test_expired_one_line_cached():
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG")
    es = _make_exec_snapshot(status="expired", mqs=85.0, ews=80.0, decision="YES", age=700.0)
    d = _hd(sr, es, refresh="scheduled")
    assert "cached" in d["one_line"].lower() or "refresh" in d["one_line"].lower()
    print("test_expired_one_line_cached PASSED")

def test_warming_no_mqs_ews_prose():
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG")
    es = _make_exec_snapshot(status="unavailable")
    d = _hd(sr, es, refresh="scheduled")
    assert d["execution"]["market_quality_score"] is None
    assert "/100" not in d["one_line"]
    print("test_warming_no_mqs_ews_prose PASSED")

def test_market_closed_language():
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG")
    es = _make_exec_snapshot()
    d = _hd(sr, es, market_open=False)
    assert "latest completed" in d["one_line"].lower() or "completed" in d["one_line"].lower()
    print("test_market_closed_language PASSED")

def test_improve_worsen_deduplication():
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG",
                      flip_conditions=["Breadth rises above 50", "Breadth rises above 50"])
    es = _make_exec_snapshot(mqs=62.0, ews=50.0)
    d = _hd(sr, es)
    assert len(d["what_would_improve"]) == len(set(d["what_would_improve"]))
    assert len(d["what_would_worsen"]) == len(set(d["what_would_worsen"]))
    for entry in d["what_would_improve"]:
        assert _no_raw_enums_in(entry), f"Raw enum: {entry}"
    for entry in d["what_would_worsen"]:
        assert _no_raw_enums_in(entry), f"Raw enum: {entry}"
    print("test_improve_worsen_deduplication PASSED")

def test_machine_enum_fields_unchanged():
    d = _hd()
    assert d["verdict"] in ("YES", "CAUTION", "NO")
    assert d["action"] in ("PRESS", "SELECTIVE", "WAIT", "REDUCE", "HEDGE")
    assert d["regime"]["risk_level"] in ("LOW", "MODERATE", "ELEVATED", "HIGH", "EXTREME")
    assert d["regime"]["direction"] in ("IMPROVING", "STABLE", "WORSENING", "UNKNOWN")
    assert d["execution"]["quality"] in ("STRONG", "MIXED", "WEAK", "UNAVAILABLE")
    print("test_machine_enum_fields_unchanged PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Phase C — Sizing provenance tests
# ═══════════════════════════════════════════════════════════════════════════════

def test_sizing_object_present():
    d = _hd()
    assert "sizing" in d
    s = d["sizing"]
    for field in ("matrix_size", "regime_base_size", "regime_final_size",
                  "event_overlay_active", "event_adjustment_applied",
                  "event_pre_size", "event_post_size", "final_size", "explanation"):
        assert field in s, f"Missing sizing.{field}"
    print("test_sizing_object_present PASSED")


def test_sizing_no_event_no_change():
    """No event — sizing unchanged."""
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG", pos_size="selective")
    es = _make_exec_snapshot()
    d = _hd(sr, es)
    assert d["sizing"]["event_overlay_active"] is False
    assert d["sizing"]["event_adjustment_applied"] is False
    assert d["sizing"]["final_size"] == "selective"
    assert d["sizing"]["event_pre_size"] == d["sizing"]["event_post_size"]
    print("test_sizing_no_event_no_change PASSED")


def test_sizing_event_applies_once():
    """One event adjustment: base selective → final half-size. Home does NOT re-apply."""
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG", pos_size="half-size", event_active=True)
    sr["base_position_size_hint"] = "selective"
    sr["event_overlay"]["position_size_adjustment_applied"] = True
    sr["event_overlay"]["pre_event_size"] = "selective"
    sr["event_overlay"]["post_event_size"] = "half-size"
    es = _make_exec_snapshot()
    d = _hd(sr, es)
    assert d["position_size_hint"] == "half-size", f"Expected half-size, got {d['position_size_hint']}"
    assert d["sizing"]["event_adjustment_applied"] is True
    assert d["sizing"]["event_pre_size"] == "selective"
    assert d["sizing"]["event_post_size"] == "half-size"
    assert d["sizing"]["final_size"] == "half-size"
    print("test_sizing_event_applies_once PASSED")


def test_sizing_no_double_event_adjustment():
    """Swing Regime final half-size → Home final half-size, never preserve capital solely from same event."""
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG", pos_size="half-size", event_active=True)
    sr["base_position_size_hint"] = "selective"
    sr["event_overlay"]["position_size_adjustment_applied"] = True
    sr["event_overlay"]["pre_event_size"] = "selective"
    sr["event_overlay"]["post_event_size"] = "half-size"
    es = _make_exec_snapshot()
    d = _hd(sr, es)
    assert d["position_size_hint"] == "half-size", \
        f"Should be half-size, not preserve capital. Got {d['position_size_hint']}"
    print("test_sizing_no_double_event_adjustment PASSED")


def test_sizing_preserve_capital_guardrail_only():
    """Preserve capital only when justified by matrix or regime risk, never just from event."""
    sr = _make_regime("HIGH", "STABLE", "SELECTIVE_SHORT", pos_size="preserve capital", event_active=True)
    sr["base_position_size_hint"] = "half-size"
    sr["event_overlay"]["position_size_adjustment_applied"] = True
    sr["event_overlay"]["pre_event_size"] = "half-size"
    sr["event_overlay"]["post_event_size"] = "preserve capital"
    es = _make_exec_snapshot()
    d = _hd(sr, es)
    assert d["position_size_hint"] == "preserve capital"
    assert d["sizing"]["regime_final_size"] == "preserve capital"
    print("test_sizing_preserve_capital_guardrail_only PASSED")


def test_sizing_event_inactive_equal():
    """Event inactive → adjust_applied false, pre and post sizes equal."""
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG", pos_size="selective")
    es = _make_exec_snapshot()
    d = _hd(sr, es)
    assert d["sizing"]["event_adjustment_applied"] is False
    assert d["sizing"]["event_pre_size"] == d["sizing"]["event_post_size"]
    print("test_sizing_event_inactive_equal PASSED")


def test_sizing_provenance_accurate():
    """All provenance fields match expected values."""
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG", pos_size="half-size", event_active=True)
    sr["base_position_size_hint"] = "selective"
    sr["event_overlay"]["position_size_adjustment_applied"] = True
    sr["event_overlay"]["pre_event_size"] = "selective"
    sr["event_overlay"]["post_event_size"] = "half-size"
    es = _make_exec_snapshot()
    d = _hd(sr, es)
    s = d["sizing"]
    assert s["matrix_size"] == "selective"
    assert s["regime_base_size"] == "selective"
    assert s["regime_final_size"] == "half-size"
    assert s["event_overlay_active"] is True
    assert s["event_adjustment_applied"] is True
    assert s["event_pre_size"] == "selective"
    assert s["event_post_size"] == "half-size"
    assert s["final_size"] == "half-size"
    assert "cpi" in s["explanation"].lower() or "imminent" in s["explanation"].lower()
    print("test_sizing_provenance_accurate PASSED")


def test_representative_fixture_case_half_size():
    """Representative fixture: MODERATE + STABLE + SELECTIVE_LONG + CPI event → half-size.

    This is a synthetic fixture for invariant validation, not a captured live response.
    """
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG", pos_size="half-size", event_active=True)
    sr["base_position_size_hint"] = "selective"
    sr["event_overlay"]["position_size_adjustment_applied"] = True
    sr["event_overlay"]["pre_event_size"] = "selective"
    sr["event_overlay"]["post_event_size"] = "half-size"
    es = _make_exec_snapshot(mqs=62.0, ews=50.0, decision="CAUTION")
    d = _hd(sr, es)
    assert d["position_size_hint"] == "half-size", \
        f"Representative fixture should be half-size, got {d['position_size_hint']}"
    print("test_representative_fixture_case_half_size PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Phase C — Execution warmup contract tests
# ═══════════════════════════════════════════════════════════════════════════════

def test_warmup_recommended_refetch():
    """Warming execution → recommended_refetch_seconds=5, refresh_in_progress=true."""
    sr = _make_regime()
    es = _make_exec_snapshot(status="unavailable")
    d = _hd(sr, es, refresh="scheduled")
    assert d["execution"]["status"] in ("warming", "unavailable")
    if d["execution"]["status"] == "warming" or d["execution"]["refresh_status"] == "scheduled":
        assert d["execution"]["recommended_refetch_seconds"] == 5
        assert d["execution"]["refresh_in_progress"] is True
    print("test_warmup_recommended_refetch PASSED")


def test_available_no_refetch():
    """Available execution → recommended_refetch_seconds=None, refresh_in_progress=false."""
    sr = _make_regime()
    es = _make_exec_snapshot(mqs=62.0, ews=50.0, decision="CAUTION", status="available")
    d = _hd(sr, es, refresh="not_needed")
    assert d["execution"]["status"] == "available"
    assert d["execution"]["recommended_refetch_seconds"] is None
    assert d["execution"]["refresh_in_progress"] is False
    print("test_available_no_refetch PASSED")


def test_expired_refetch():
    """Expired with refresh scheduled → recommended_refetch_seconds=5."""
    sr = _make_regime("LOW", "IMPROVING", "LONG", pos_size="normal")
    es = _make_exec_snapshot(status="expired", mqs=85.0, ews=100.0, decision="YES", age=700.0)
    d = _hd(sr, es, refresh="scheduled")
    assert d["execution"]["recommended_refetch_seconds"] == 5
    assert d["execution"]["refresh_in_progress"] is True
    print("test_expired_refetch PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Phase C — Signal summary tests
# ═══════════════════════════════════════════════════════════════════════════════

def test_signal_summary_present():
    d = _hd()
    assert "signal_summary" in d
    ss = d["signal_summary"]
    for key in ("strongest_supports", "largest_risks", "missing_confirmations"):
        assert key in ss, f"Missing signal_summary.{key}"
        assert isinstance(ss[key], list)
    print("test_signal_summary_present PASSED")


def test_generic_reasons_not_present():
    """No production response should contain generic phrases."""
    d = _hd()
    for reason in d["buy_reasons"] + d["wait_reasons"] + d["reduce_reasons"]:
        assert "regime permits selective long exposure" not in reason.lower()
        assert "broadly supportive" not in reason.lower()
        assert "monitor daily" not in reason.lower()
    for entry in d["what_would_improve"]:
        assert "monitor daily" not in entry.lower()
        assert "no immediate flip" not in entry.lower()
    print("test_generic_reasons_not_present PASSED")


def test_improvise_worsen_empty_not_monitor_daily():
    """Empty improve/worsen arrays must be empty, not contain generic fallback."""
    sr = _make_regime("MODERATE", "STABLE", "SELECTIVE_LONG", flip_conditions=[])
    es = _make_exec_snapshot()
    d = _hd(sr, es)
    for entry in d["what_would_improve"]:
        assert "monitor" not in entry.lower(), f"Found 'monitor': {entry}"
    for entry in d["what_would_worsen"]:
        assert "monitor" not in entry.lower(), f"Found 'monitor': {entry}"
    print("test_improvise_worsen_empty_not_monitor_daily PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Phase D: WEAKENING direction handling
# ═══════════════════════════════════════════════════════════════════════════════

def test_weakening_moderate_waits():
    """MODERATE + WEAKENING + STRONG exec → CAUTION / WAIT (not SELECTIVE)"""
    sr = _make_regime("MODERATE", "WEAKENING", "NEUTRAL")
    es = _make_exec_snapshot(mqs=75.0, ews=80.0, decision="YES")
    d = _hd(sr, es)
    assert d["verdict"] == "CAUTION"
    assert d["action"] == "WAIT", f"expected WAIT, got {d['action']}"
    assert d["position_size_hint"] == "half-size"
    print("test_weakening_moderate_waits PASSED")


def test_weakening_elevated_waits():
    """ELEVATED + WEAKENING → CAUTION / WAIT (not SELECTIVE)"""
    sr = _make_regime("ELEVATED", "WEAKENING", "NEUTRAL", pos_size="selective")
    es = _make_exec_snapshot(mqs=75.0, ews=80.0, decision="YES")
    d = _hd(sr, es)
    assert d["verdict"] == "CAUTION"
    assert d["action"] == "WAIT", f"expected WAIT, got {d['action']}"
    assert d["position_size_hint"] == "half-size"
    print("test_weakening_elevated_waits PASSED")


def test_weakening_h_dir_mapping():
    """_h_dir returns 'Weakening' not raw 'WEAKENING'"""
    from services.home_risk_intelligence import _h_dir
    assert _h_dir("WEAKENING") == "Weakening"
    assert _h_dir("WORSENING") == "Worsening"
    assert _h_dir("IMPROVING") == "Improving"
    assert _h_dir("STABLE") == "Stable"
    print("test_weakening_h_dir_mapping PASSED")


def test_weakening_moderate_unavailable_waits():
    """MODERATE + WEAKENING + UNAVAILABLE exec → WAIT"""
    sr = _make_regime("MODERATE", "WEAKENING", "NEUTRAL")
    es = _make_exec_snapshot(status="unavailable")
    d = _hd(sr, es)
    assert d["action"] == "WAIT", f"expected WAIT, got {d['action']}"
    print("test_weakening_moderate_unavailable_waits PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Phase E: Complete direction matrix coverage
# ═══════════════════════════════════════════════════════════════════════════════

def test_low_weakening_waits():
    """LOW + WEAKENING → CAUTION / WAIT (cannot produce PRESS)"""
    sr = _make_regime("LOW", "WEAKENING", "SELECTIVE_LONG", pos_size="normal")
    es = _make_exec_snapshot(mqs=80.0, ews=85.0, decision="YES")
    d = _hd(sr, es)
    assert d["verdict"] == "CAUTION"
    assert d["action"] == "WAIT", f"LOW+WEAKENING should WAIT, got {d['action']}"
    assert d["action"] != "PRESS", "LOW+WEAKENING must not produce PRESS"
    assert d["position_size_hint"] == "half-size"
    print("test_low_weakening_waits PASSED")


def test_high_weakening_not_improving():
    """HIGH + WEAKENING → NO / REDUCE (not IMPROVING branch)"""
    sr = _make_regime("HIGH", "WEAKENING", "SELECTIVE_SHORT", pos_size="half-size")
    es = _make_exec_snapshot(mqs=75.0, ews=80.0, decision="YES")
    d = _hd(sr, es)
    assert d["verdict"] == "NO"
    assert d["action"] == "REDUCE", f"HIGH+WEAKENING should REDUCE, got {d['action']}"
    assert d["action"] not in ("SELECTIVE",), "HIGH+WEAKENING must not fall into IMPROVING branch"
    print("test_high_weakening_not_improving PASSED")


def test_extreme_weakening_reduce():
    """EXTREME + WEAKENING → NO / REDUCE (not IMPROVING branch)"""
    sr = _make_regime("EXTREME", "WEAKENING", "SHORT_HEDGE", pos_size="preserve capital")
    es = _make_exec_snapshot(mqs=75.0, ews=80.0, decision="YES")
    d = _hd(sr, es)
    assert d["verdict"] == "NO"
    assert d["action"] == "REDUCE", f"EXTREME+WEAKENING should REDUCE, got {d['action']}"
    assert d["action"] != "WAIT", "EXTREME+WEAKENING should be REDUCE not WAIT"
    print("test_extreme_weakening_reduce PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# Phase F: Execution contract completeness
# ═══════════════════════════════════════════════════════════════════════════════

def test_execution_has_mqs_label():
    dash = _make_exec_snapshot(mqs=72, ews=55)
    dash["dashboard"]["market_quality_label"] = "HEALTHY"
    d = _hd(es=dash)
    assert d["execution"].get("market_quality_label") == "HEALTHY"
    print("test_execution_has_mqs_label PASSED")


def test_execution_has_ews_label():
    dash = _make_exec_snapshot(mqs=72, ews=55)
    dash["dashboard"]["execution_window_label"] = "MIXED"
    d = _hd(es=dash)
    assert d["execution"].get("execution_window_label") == "MIXED"
    print("test_execution_has_ews_label PASSED")


def test_execution_has_condition_counts():
    dash = _make_exec_snapshot(mqs=72, ews=55)
    dash["dashboard"]["execution_conditions_available_count"] = 3
    dash["dashboard"]["execution_conditions_expected_count"] = 4
    dash["dashboard"]["execution_conditions_status"] = "partial"
    d = _hd(es=dash)
    assert d["execution"].get("available_condition_count") == 3
    assert d["execution"].get("expected_condition_count") == 4
    assert d["execution"].get("condition_status") == "partial"
    print("test_execution_has_condition_counts PASSED")


def test_execution_has_execution_conditions():
    dash = _make_exec_snapshot(mqs=72, ews=55)
    dash["dashboard"]["execution_conditions"] = [
        {"id": "breakouts", "label": "Breakouts working", "state": "pass", "available": True, "ok": True, "evidence": "75/100 breadth"},
        {"id": "leaders", "label": "Leaders holding", "state": "pass", "available": True, "ok": True, "evidence": "Leaders +1.2%"},
        {"id": "pullbacks", "label": "Pullbacks bought", "state": "fail", "available": True, "ok": False, "evidence": "2/5 days"},
        {"id": "follow_through", "label": "Follow-through", "state": "unavailable", "available": False, "ok": False, "evidence": "No data"},
    ]
    d = _hd(es=dash)
    assert len(d["execution"].get("execution_conditions", [])) == 4
    print("test_execution_has_execution_conditions PASSED")


def test_execution_has_primary_blocker():
    d = _hd(es=_make_exec_snapshot(mqs=72, ews=55))
    assert d["execution"].get("primary_blocker") is not None
    assert isinstance(d["execution"]["primary_blocker"], str)
    print("test_execution_has_primary_blocker PASSED")


def test_execution_has_decision_effect():
    d = _hd(es=_make_exec_snapshot(mqs=72, ews=55))
    assert d["execution"].get("decision_effect") is not None
    assert isinstance(d["execution"]["decision_effect"], str)
    print("test_execution_has_decision_effect PASSED")


def test_execution_has_data_status():
    dash = _make_exec_snapshot(mqs=72, ews=55)
    dash["dashboard"]["data_completeness"] = {"data_status": "available"}
    d = _hd(es=dash)
    assert d["execution"].get("data_status") == "available"
    print("test_execution_has_data_status PASSED")


def test_execution_has_refresh_error():
    snap = _make_exec_snapshot(status="failed", mqs=None, ews=None)
    snap["refresh_error"] = "Connection refused"
    snap["dashboard"] = None
    d = _hd(es=snap)
    assert d["execution"].get("refresh_error") == "Connection refused"
    print("test_execution_has_refresh_error PASSED")


def test_execution_has_completeness_pct():
    dash = _make_exec_snapshot(mqs=72, ews=55)
    dash["dashboard"]["execution_conditions_available_count"] = 3
    dash["dashboard"]["execution_conditions_expected_count"] = 4
    d = _hd(es=dash)
    assert d["execution"].get("completeness_pct") == 75
    print("test_execution_has_completeness_pct PASSED")


def test_available_strong_execution_reaches_matrix():
    d = _hd(sr=_make_regime(risk_level="LOW", direction="IMPROVING"),
            es=_make_exec_snapshot(mqs=80, ews=80))
    assert d["execution"]["status"] == "available"
    assert d["execution"]["quality"] == "STRONG"
    assert d["verdict"] in ("YES", "CAUTION")
    print("test_available_strong_execution_reaches_matrix PASSED")


def test_available_mixed_execution_reaches_matrix():
    d = _hd(sr=_make_regime(risk_level="MODERATE", direction="STABLE"),
            es=_make_exec_snapshot(mqs=55, ews=55))
    assert d["execution"]["quality"] == "MIXED"
    assert d["verdict"] in ("CAUTION", "YES")
    print("test_available_mixed_execution_reaches_matrix PASSED")


def test_available_weak_execution_reaches_matrix():
    d = _hd(sr=_make_regime(risk_level="LOW", direction="STABLE"),
            es=_make_exec_snapshot(mqs=30, ews=25))
    assert d["execution"]["quality"] == "WEAK"
    assert d["verdict"] == "CAUTION"
    assert d["action"] == "WAIT"
    print("test_available_weak_execution_reaches_matrix PASSED")


def test_unavailable_execution_produces_regime_only():
    d = _hd(es=_make_exec_snapshot(status="unavailable", mqs=None, ews=None))
    assert d["execution"]["status"] in ("unavailable", "warming", "failed")
    print("test_unavailable_execution_produces_regime_only PASSED")


def test_expired_execution_no_yes_or_press():
    d = _hd(sr=_make_regime(risk_level="LOW", direction="IMPROVING"),
            es=_make_exec_snapshot(status="expired", mqs=80, ews=80, age=4000))
    assert d["verdict"] != "YES"
    assert d["action"] != "PRESS"
    print("test_expired_execution_no_yes_or_press PASSED")


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

    # Language hardening
    test_one_line_no_repeated_selective()
    test_one_line_no_raw_enum_casing()
    test_why_now_no_repeated_words()
    test_why_now_no_raw_enum_casing()
    test_moderate_not_broadly_supportive()
    test_moderate_stable_mixed_coherent()
    test_caution_selective_reason_invariants()
    test_caution_wait_buy_reasons_restricted()
    test_yes_buy_reasons_nonempty()
    test_no_buy_reasons_empty()
    test_insufficient_data_buy_reasons_empty()
    test_expired_one_line_cached()
    test_warming_no_mqs_ews_prose()
    test_market_closed_language()
    test_improve_worsen_deduplication()
    test_machine_enum_fields_unchanged()

    # Phase C — Sizing provenance
    test_sizing_object_present()
    test_sizing_no_event_no_change()
    test_sizing_event_applies_once()
    test_sizing_no_double_event_adjustment()
    test_sizing_preserve_capital_guardrail_only()
    test_sizing_event_inactive_equal()
    test_sizing_provenance_accurate()
    test_representative_fixture_case_half_size()

    # Phase C — Execution warmup contract
    test_warmup_recommended_refetch()
    test_available_no_refetch()
    test_expired_refetch()

    # Phase C — Signal summary
    test_signal_summary_present()
    test_generic_reasons_not_present()
    test_improvise_worsen_empty_not_monitor_daily()

    # Phase D — WEAKENING direction handling
    test_weakening_moderate_waits()
    test_weakening_elevated_waits()
    test_weakening_h_dir_mapping()
    test_weakening_moderate_unavailable_waits()

    # Phase E — Complete direction matrix coverage
    test_low_weakening_waits()
    test_high_weakening_not_improving()
    test_extreme_weakening_reduce()

    # Phase F — Execution contract completeness
    test_execution_has_mqs_label()
    test_execution_has_ews_label()
    test_execution_has_condition_counts()
    test_execution_has_execution_conditions()
    test_execution_has_primary_blocker()
    test_execution_has_decision_effect()
    test_execution_has_data_status()
    test_execution_has_refresh_error()
    test_execution_has_completeness_pct()
    test_available_strong_execution_reaches_matrix()
    test_available_mixed_execution_reaches_matrix()
    test_available_weak_execution_reaches_matrix()
    test_unavailable_execution_produces_regime_only()
    test_expired_execution_no_yes_or_press()

    total = 89
    print(f"\nAll {total} tests PASSED")
