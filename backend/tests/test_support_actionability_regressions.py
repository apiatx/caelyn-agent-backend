"""Targeted regressions for active-support ranking and asymmetric READY gates."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_BACKEND = Path(__file__).resolve().parent.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from services.actionability_service import (  # noqa: E402
    AVOID,
    READY,
    TOO_EXTENDED,
    WAIT_FOR_RETEST,
    _compute_actionability_core,
)
from services.entry_state_service import _classify_active_support_zone  # noqa: E402


def _support_zone(**overrides):
    inputs = {
        "price": 100.0,
        "sma50": None,
        "sma200": None,
        "ma30w": None,
        "base_low": None,
        "breakout_pivot": None,
        "sorted_bars": [],
    }
    inputs.update(overrides)
    return _classify_active_support_zone(**inputs)


def _actionability(
    *,
    entry_state="HIGH_BASE_READY",
    rr_state="ASYMMETRIC_SUPPORT_ENTRY",
    extension_state="HEALTHY",
    ia_score=75.0,
):
    entry = {
        "entry_state": entry_state,
        "entry_score": 70,
        "entry_grade": "A",
        "entry_family": "CONTINUATION",
        "entry_risk_reward_state": rr_state,
        "extension_state": extension_state,
        "structure_v2": {},
    }
    ta = {
        "trade_alignment_available": True,
        "trade_alignment_score": 75.0,
        "theme_alignment_score": 70.0,
        "options_alignment_score": None,
        "options_pressure_state": None,
        "catalyst_alignment_available": False,
        "investment_alignment_score": ia_score,
    }
    return _compute_actionability_core(entry, ta, None)


class TestActiveSupportCandidateRanking:
    def test_downside_support_beats_slightly_overhead_sma(self):
        result = _support_zone(sma50=102.0, sma200=96.0)

        assert result["active_support_label"] == "SMA200"
        assert result["major_support_level"] == 96.0

    def test_downside_support_beats_slightly_overhead_pivot(self):
        result = _support_zone(breakout_pivot=102.0, base_low=95.0)

        assert result["active_support_label"] == "base_low"
        assert result["major_support_level"] == 95.0

    def test_overhead_candidate_is_fallback_when_no_downside_support_exists(self):
        result = _support_zone(sma50=102.0)

        assert result["active_support_label"] == "SMA50"
        assert result["major_support_level"] == 102.0

    def test_overhead_proximity_cannot_outscore_same_priority_downside_support(self):
        result = _support_zone(sma50=102.0, ma30w=99.0)

        assert result["active_support_label"] == "30w_MA"
        assert result["major_support_level"] == 99.0

    def test_confirmed_loss_buffer_does_not_prematurely_confirm(self):
        result = _support_zone(sma200=100.0, price=97.5)

        assert result["critical_break_level"] == 98.0
        assert 97.5 > result["critical_break_level"] * 0.97
        assert result["active_support_status"] == "broken_unconfirmed"


class TestAsymmetricReadyInvestmentGate:
    def test_available_ia_below_threshold_does_not_become_ready(self):
        result = _actionability(ia_score=69.9)

        assert result["actionability_state"] == WAIT_FOR_RETEST
        assert "INVESTMENT_ALIGNMENT_INSUFFICIENT" in result["actionability_reason_codes"]

    def test_available_ia_at_threshold_can_become_ready(self):
        assert _actionability(ia_score=70.0)["actionability_state"] == READY

    def test_unavailable_ia_preserves_ready_behavior(self):
        assert _actionability(ia_score=None)["actionability_state"] == READY

    def test_ordinary_ready_state_is_unaffected_by_low_ia(self):
        result = _actionability(rr_state="SUPPORT_TEST_CONFIRMING", ia_score=20.0)

        assert result["actionability_state"] == READY

    @pytest.mark.parametrize(
        "entry_state",
        [
            "SUPPORT_LOST",
            "DOWNTREND",
            "FAILED_BREAKOUT",
            "LOWER_LOW_CONFIRMED",
        ],
    )
    def test_hard_bearish_precedence_is_unchanged(self, entry_state):
        result = _actionability(entry_state=entry_state, ia_score=75.0)

        assert result["actionability_state"] == AVOID

    @pytest.mark.parametrize(
        ("entry_state", "extension_state"),
        [
            ("EXTREME_EXTENSION", "EXTREME_EXTENSION"),
            ("HIGH_BASE_READY", "EXTREME_EXTENSION"),
        ],
    )
    def test_too_extended_precedence_is_unchanged(
        self, entry_state, extension_state
    ):
        result = _actionability(
            entry_state=entry_state,
            extension_state=extension_state,
            ia_score=75.0,
        )

        assert result["actionability_state"] == TOO_EXTENDED
