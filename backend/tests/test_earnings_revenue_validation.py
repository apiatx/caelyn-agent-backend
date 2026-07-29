"""
Revenue-validation tests for the two-stage corruption-prevention system.

The 10× threshold (revenue_actual > 10× revenue_estimate) is a *suspicious-data
signal*, not a validity rule.  These tests verify that:

  1. BAND-style corruption    — $428B actual vs $217M estimate is rejected and
                                 held for re-polling until FMP self-corrects.
  2. Real extreme beat        — $15M actual vs $1M estimate (1 400% surprise)
                                 is accepted once the income statement confirms it.
  3. Unconfirmed extreme      — same $15M beat without IS data stays pending,
                                 is not cached as authoritative, and remains
                                 eligible for re-polling.
  4. Normal result            — $220M actual vs $217M estimate follows the
                                 existing path without extra checks.
  5. Zero / null / negative   — ratio-based validation is bypassed when the
       estimates                denominator is unsuitable.

Run with:
    cd backend && python -m pytest tests/test_earnings_revenue_validation.py -v
"""
from __future__ import annotations

import sys
import os

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services.earnings_monitor_service import (
    _is_revenue_suspect,
    _has_complete_results_for_target,
    _merge_results_payload,
)
from services.watchlist_fundamentals_refresh import _confirm_extreme_revenue


# ── shared helpers ─────────────────────────────────────────────────────────────

def _payload(
    *,
    date: str = "2026-07-29",
    eps_actual: float | None = 0.37,
    eps_estimate: float | None = 0.364,
    revenue_actual: float | None = None,
    revenue_estimate: float | None = None,
    revenue_suspect: bool = False,
    revenue_verified: bool = False,
) -> dict:
    p: dict = {
        "date":             date,
        "eps_actual":       eps_actual,
        "eps_estimate":     eps_estimate,
        "revenue_actual":   revenue_actual,
        "revenue_estimate": revenue_estimate,
    }
    if revenue_suspect:
        p["_revenue_suspect"] = True
    if revenue_verified:
        p["_revenue_verified"] = True
    return p


def _income(date: str, revenue: float | None) -> dict:
    """Minimal income-statement row keyed by filing date."""
    return {date: {"revenue": revenue, "filingDate": date}}


# ══════════════════════════════════════════════════════════════════════════════
# Scenario 1 — BAND-style corruption
# provider actual $428 B vs estimate $217 M vs filing/quarterly evidence ~$220 M
# ══════════════════════════════════════════════════════════════════════════════

class TestBandStyleCorruption:

    ACTUAL   = 428_472_200_000   # ~$428 B (corrupted — 6-month × 1 000)
    ESTIMATE = 217_040_000       # ~$217 M (correct estimate)
    DATE     = "2026-07-29"

    def test_flagged_as_suspect_by_ratio(self):
        """_is_revenue_suspect fires for the $428B / $217M ratio (~1 975×)."""
        assert _is_revenue_suspect({"revenue_actual": self.ACTUAL,
                                    "revenue_estimate": self.ESTIMATE})

    def test_income_statement_does_not_confirm_corrupted_value(self):
        """IS revenue of $220M cannot confirm a $428B actual (ratio ≈ 1 946×)."""
        IS_REVENUE = 220_000_000
        confirmed = _confirm_extreme_revenue(
            self.ACTUAL,
            _income(self.DATE, IS_REVENUE),
            self.DATE,
        )
        assert not confirmed

    def test_unflagged_suspect_payload_is_not_complete(self):
        """
        First-poll edge-case: payload has no flags yet but fails the raw check.
        _has_complete_results_for_target must return False to force one re-poll
        during which the _revenue_suspect flag will be written.
        """
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE)
        assert not _has_complete_results_for_target(p, self.DATE)

    def test_suspect_flagged_payload_is_not_complete(self):
        """
        After the first poll the _revenue_suspect flag is set.
        The result must still be incomplete so the monitor keeps re-polling.
        """
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                     revenue_suspect=True)
        assert not _has_complete_results_for_target(p, self.DATE)

    def test_suspect_value_not_promoted_without_second_sighting(self):
        """
        _revenue_suspect alone (first detection only) must never become verified.
        """
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                     revenue_suspect=True)
        assert not p.get("_revenue_verified")

    def test_merge_preserves_suspect_flag_when_incoming_is_same(self):
        """
        If FMP returns the same corrupted value a second time _merge_results_payload
        must carry the existing _revenue_suspect flag forward (flag management is
        left to _process_target, not the merge step).
        """
        existing = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                            revenue_suspect=True)
        incoming = {
            "revenue_actual":   self.ACTUAL,
            "revenue_estimate": self.ESTIMATE,
            "eps_actual":       0.37,
            "eps_estimate":     0.364,
            "date":             self.DATE,
        }
        merged = _merge_results_payload(existing, incoming)
        assert merged.get("_revenue_suspect") is True
        assert not merged.get("_revenue_verified")


# ══════════════════════════════════════════════════════════════════════════════
# Scenario 2 — Real extreme beat confirmed by income statement
# estimate $1 M, actual $15 M (1 400% surprise), IS also shows $15 M
# ══════════════════════════════════════════════════════════════════════════════

class TestRealExtremeBeatConfirmed:

    ACTUAL   = 15_000_000   # $15 M actual
    ESTIMATE =  1_000_000   # $1 M estimate (thin analyst coverage)
    DATE     = "2026-07-29"

    def test_flagged_as_suspect_by_ratio(self):
        """15× the estimate triggers the suspicious-signal check."""
        assert _is_revenue_suspect({"revenue_actual": self.ACTUAL,
                                    "revenue_estimate": self.ESTIMATE})

    def test_income_statement_confirms_matching_actual(self):
        """IS revenue of $15 M confirms the $15 M actual (ratio = 1.0, within 2×)."""
        confirmed = _confirm_extreme_revenue(
            self.ACTUAL,
            _income(self.DATE, self.ACTUAL),
            self.DATE,
        )
        assert confirmed

    def test_income_statement_confirms_within_tolerance(self):
        """IS revenue within 2× of actual still confirms (e.g. $10 M IS vs $15 M actual)."""
        IS_REVENUE_LOW  = 10_000_000   # ratio = 1.5 — within [0.5, 2.0]
        IS_REVENUE_HIGH = 20_000_000   # ratio = 0.75 — within [0.5, 2.0]
        assert _confirm_extreme_revenue(self.ACTUAL, _income(self.DATE, IS_REVENUE_LOW),  self.DATE)
        assert _confirm_extreme_revenue(self.ACTUAL, _income(self.DATE, IS_REVENUE_HIGH), self.DATE)

    def test_verified_flag_makes_result_complete(self):
        """
        Once _revenue_verified is set (after second consistent FMP sighting),
        _has_complete_results_for_target returns True — re-polling stops.
        The 1 400% surprise must be preserved; the flag is the acceptance gate.
        """
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                     revenue_verified=True)
        assert _has_complete_results_for_target(p, self.DATE)

    def test_verified_payload_preserves_surprise_magnitude(self):
        """
        The 1 400% revenue_surprise_pct must survive a merge cycle unchanged.
        The system must not null or cap it after verification.
        """
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                     revenue_verified=True)
        merged = _merge_results_payload(
            p,
            {"date": self.DATE, "eps_actual": 1.0, "eps_estimate": 0.8},
        )
        assert merged["revenue_actual"] == self.ACTUAL
        assert merged.get("_revenue_verified") is True

    def test_fuzzy_date_match_confirms_value(self):
        """IS filing date 3 days before event date still confirms (±7 day tolerance)."""
        IS_DATE = "2026-07-26"   # 3 days before ev_date
        confirmed = _confirm_extreme_revenue(
            self.ACTUAL,
            _income(IS_DATE, self.ACTUAL),
            self.DATE,
        )
        assert confirmed


# ══════════════════════════════════════════════════════════════════════════════
# Scenario 3 — Unconfirmed extreme result
# estimate $1 M, actual $15 M, but no income-statement data available
# ══════════════════════════════════════════════════════════════════════════════

class TestUnconfirmedExtremeResult:

    ACTUAL   = 15_000_000
    ESTIMATE =  1_000_000
    DATE     = "2026-07-29"

    def test_no_income_data_does_not_confirm(self):
        """Empty income_by_filing → _confirm_extreme_revenue returns False."""
        assert not _confirm_extreme_revenue(self.ACTUAL, {}, self.DATE)

    def test_no_income_revenue_field_does_not_confirm(self):
        """IS row with no 'revenue' key → not confirmed."""
        no_rev_row = {self.DATE: {"operatingIncome": 3_000_000}}
        assert not _confirm_extreme_revenue(self.ACTUAL, no_rev_row, self.DATE)

    def test_suspect_flag_keeps_result_pending(self):
        """
        Unconfirmed extreme with _revenue_suspect=True must NOT be treated as
        complete — the value is not yet authoritative.
        """
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                     revenue_suspect=True)
        assert not _has_complete_results_for_target(p, self.DATE)

    def test_suspect_flag_keeps_result_eligible_for_repoll(self):
        """
        _has_complete_results_for_target returning False ensures that the
        should_fmp gate in _process_target stays open for re-polling.
        """
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                     revenue_suspect=True)
        is_complete = _has_complete_results_for_target(p, self.DATE)
        assert not is_complete, (
            "Suspect result must be incomplete so the FMP check is not gated out"
        )

    def test_unconfirmed_not_verified(self):
        """A suspect payload must never have _revenue_verified set."""
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                     revenue_suspect=True)
        assert not p.get("_revenue_verified")

    def test_is_row_outside_7_day_window_does_not_confirm(self):
        """IS row 14 days away (outside ±7-day tolerance) → not confirmed."""
        IS_DATE = "2026-07-15"   # 14 days before event date
        assert not _confirm_extreme_revenue(
            self.ACTUAL,
            _income(IS_DATE, self.ACTUAL),
            self.DATE,
        )

    def test_is_row_contradicts_does_not_confirm(self):
        """
        IS revenue of $220 M vs actual $428 B: ratio ≈ 1 946× — far outside
        the 2× tolerance, so the extreme value is rejected.
        (This is the BAND-style case with the EI-cache path.)
        """
        IS_REVENUE   = 220_000_000
        CORRUPT_ACTUAL = 428_472_200_000
        assert not _confirm_extreme_revenue(CORRUPT_ACTUAL, _income(self.DATE, IS_REVENUE), self.DATE)


# ══════════════════════════════════════════════════════════════════════════════
# Scenario 4 — Normal result (existing path unchanged)
# estimate $217 M, actual $220 M (~1.4% beat)
# ══════════════════════════════════════════════════════════════════════════════

class TestNormalResult:

    ACTUAL   = 220_000_000
    ESTIMATE = 217_040_000
    DATE     = "2026-07-29"

    def test_not_flagged_as_suspect(self):
        """A routine 1.4% revenue beat must not trigger the suspicious-signal check."""
        assert not _is_revenue_suspect({"revenue_actual": self.ACTUAL,
                                        "revenue_estimate": self.ESTIMATE})

    def test_normal_payload_is_complete(self):
        """
        A payload with both actuals, no flags, and matching date must be treated
        as complete — the existing polling path continues to work unchanged.
        """
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE)
        assert _has_complete_results_for_target(p, self.DATE)

    def test_merge_does_not_add_flags_for_normal_values(self):
        """
        _merge_results_payload must not inject _revenue_suspect or _revenue_verified
        for plausible values (flag management is _process_target's responsibility).
        """
        merged = _merge_results_payload(
            None,
            {
                "eps_actual":       0.37,
                "eps_estimate":     0.364,
                "revenue_actual":   self.ACTUAL,
                "revenue_estimate": self.ESTIMATE,
                "date":             self.DATE,
            },
        )
        assert "revenue_actual" in merged
        assert merged["revenue_actual"] == self.ACTUAL

    def test_stale_payload_still_blocked_by_date_window(self):
        """
        Normal values outside the 7-day window must still return False (stale
        quarter guard must not be bypassed by removing the plausibility check).
        """
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                     date="2026-04-30")
        assert not _has_complete_results_for_target(p, self.DATE)


# ══════════════════════════════════════════════════════════════════════════════
# Scenario 5 — Zero, null, or negative estimates
# ratio-based validation must be bypassed; _is_revenue_suspect returns False
# ══════════════════════════════════════════════════════════════════════════════

class TestUnsuitableDenominators:

    ACTUAL = 50_000_000

    def test_zero_estimate_not_suspicious(self):
        """Zero estimate: percentage ratio is undefined — must return False."""
        assert not _is_revenue_suspect({"revenue_actual": self.ACTUAL,
                                        "revenue_estimate": 0})

    def test_negative_estimate_not_suspicious(self):
        """Negative estimate (loss-making company): ratio undefined — False."""
        assert not _is_revenue_suspect({"revenue_actual": self.ACTUAL,
                                        "revenue_estimate": -5_000_000})

    def test_none_estimate_not_suspicious(self):
        """Null estimate: no denominator at all — False."""
        assert not _is_revenue_suspect({"revenue_actual": self.ACTUAL,
                                        "revenue_estimate": None})

    def test_none_actual_not_suspicious(self):
        """Null actual: nothing to check — False."""
        assert not _is_revenue_suspect({"revenue_actual": None,
                                        "revenue_estimate": 1_000_000})

    def test_both_none_not_suspicious(self):
        """Both values absent — False."""
        assert not _is_revenue_suspect({"revenue_actual": None,
                                        "revenue_estimate": None})

    def test_zero_estimate_payload_treated_as_complete(self):
        """
        A payload with zero estimate and large actual must not be blocked by
        the plausibility gate — it follows the normal date-window path.
        """
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=0,
                     date="2026-07-29")
        assert _has_complete_results_for_target(p, "2026-07-29")

    def test_negative_estimate_payload_treated_as_complete(self):
        """Negative estimate does not trigger plausibility — normal path."""
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=-5_000_000,
                     date="2026-07-29")
        assert _has_complete_results_for_target(p, "2026-07-29")

    def test_non_numeric_estimate_not_suspicious(self):
        """Non-numeric string estimate: graceful False, no exception."""
        assert not _is_revenue_suspect({"revenue_actual": self.ACTUAL,
                                        "revenue_estimate": "n/a"})

    def test_confirm_extreme_revenue_zero_income_statement_revenue(self):
        """IS revenue of zero → ZeroDivisionError must be caught → False."""
        assert not _confirm_extreme_revenue(
            1_000_000_000,
            {"2026-07-29": {"revenue": 0}},
            "2026-07-29",
        )
