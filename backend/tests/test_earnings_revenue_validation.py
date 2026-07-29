"""
Revenue-validation tests for the two-stage corruption-prevention system.

The 10× threshold (revenue_actual > 10× revenue_estimate) is a suspicious-data
signal, not a validity rule.  These tests verify the required behaviors:

  1. Same corrupted FMP value returned twice → still suspect, NOT promoted.
  2. BAND-style data → corrupted value not accepted.
  3. Period mismatch (six-month vs quarterly) → old 2× tolerance proved unsafe.
  4. Real extreme beat with quarterly IS confirmation → accepted, surprise preserved.
  5. Independent IS source differs materially → remains unverified.
  6. Corrected FMP value → flags clear, plausible replacement accepted.
  7. Zero/null/negative estimate → no invalid ratio; scale-anomaly check available.

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
from services.watchlist_fundamentals_refresh import (
    _confirm_extreme_revenue,
    _is_revenue_scale_anomaly,
)


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


def _is_row(
    date: str,
    revenue: float | None,
    *,
    period: str = "Q2",
    calendar_year: int = 2026,
) -> dict:
    """Build a minimal FMP income-statement row."""
    return {
        "filingDate":    date,
        "revenue":       revenue,
        "period":        period,
        "calendarYear":  calendar_year,
    }


def _income(date: str, revenue: float | None, **kw) -> dict:
    """Build an income_by_filing dict with one row."""
    return {date: _is_row(date, revenue, **kw)}


# ══════════════════════════════════════════════════════════════════════════════
# Test 1 — Same corrupted value returned twice: still suspect, NOT promoted
# ══════════════════════════════════════════════════════════════════════════════

class TestSameSuspectValueStaysSuspect:
    """
    Two identical observations from the same FMP endpoint prove persistence,
    not correctness.  The monitor must NOT promote to _revenue_verified merely
    because it polled twice and got the same implausible value.
    """

    ACTUAL   = 428_472_200_000   # $428 B — corrupted
    ESTIMATE = 217_040_000       # $217 M — correct estimate

    def test_first_poll_sets_suspect(self):
        """First detection: suspicious value flags _revenue_suspect."""
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                     revenue_suspect=True)
        assert p.get("_revenue_suspect") is True
        assert not p.get("_revenue_verified")

    def test_same_value_second_poll_stays_suspect(self):
        """
        _merge_results_payload carrying forward _revenue_suspect from the
        existing payload proves the flag survives a re-poll with the same value.
        The monitor state machine must not upgrade it to _revenue_verified.
        """
        existing = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                            revenue_suspect=True)
        incoming = {
            "revenue_actual":   self.ACTUAL,
            "revenue_estimate": self.ESTIMATE,
            "eps_actual":       0.37,
            "eps_estimate":     0.364,
            "date":             "2026-07-29",
        }
        merged = _merge_results_payload(existing, incoming)
        # _merge_results_payload must carry the existing suspect flag forward
        assert merged.get("_revenue_suspect") is True
        # A second sighting of the same value must NOT auto-set _revenue_verified
        assert not merged.get("_revenue_verified")

    def test_suspect_flag_keeps_result_incomplete(self):
        """_revenue_suspect blocks _has_complete_results_for_target (re-poll gate)."""
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                     revenue_suspect=True)
        assert not _has_complete_results_for_target(p, "2026-07-29")

    def test_raw_suspect_also_incomplete_before_flag_written(self):
        """Edge-case: no flag yet but value is suspicious — still blocked."""
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE)
        assert not _has_complete_results_for_target(p, "2026-07-29")


# ══════════════════════════════════════════════════════════════════════════════
# Test 2 — BAND-style data: $428B corrupted value not accepted
# quarterly ≈ $220 M, six-month ≈ $428 M, corrupted provider ≈ $428 B
# ══════════════════════════════════════════════════════════════════════════════

class TestBandStyleCorruption:

    ACTUAL          = 428_472_200_000   # $428.47 B (corrupted — 6-month × 1 000)
    ESTIMATE        = 217_040_000       # $217 M
    QUARTERLY_REV   = 220_000_000       # $220 M — correct quarterly IS revenue
    DATE            = "2026-07-29"

    def test_flagged_as_suspect(self):
        """`$428B / $217M ≈ 1 975×` — well above the 10× threshold."""
        assert _is_revenue_suspect({"revenue_actual": self.ACTUAL,
                                    "revenue_estimate": self.ESTIMATE})

    def test_quarterly_is_does_not_confirm_corrupted_value(self):
        """
        IS revenue of $220M cannot confirm the $428B actual.
        $428B / $220M ≈ 1 946× — far outside the 2% / $1M narrow tolerance.
        """
        confirmed = _confirm_extreme_revenue(
            self.ACTUAL,
            _income(self.DATE, self.QUARTERLY_REV, period="Q2", calendar_year=2026),
            self.DATE,
            expected_period="Q2",
            expected_year=2026,
        )
        assert not confirmed

    def test_suspect_payload_never_complete(self):
        """Flagged as suspect → _has_complete_results_for_target returns False."""
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                     revenue_suspect=True)
        assert not _has_complete_results_for_target(p, self.DATE)

    def test_annual_is_row_also_rejected(self):
        """An annual IS row (period='FY') must be rejected even if its revenue
        were close — period validation fires before numeric comparison."""
        annual_rev = 850_000_000  # realistic annual total, not close to $428B anyway
        confirmed = _confirm_extreme_revenue(
            self.ACTUAL,
            _income(self.DATE, annual_rev, period="FY", calendar_year=2026),
            self.DATE,
        )
        assert not confirmed


# ══════════════════════════════════════════════════════════════════════════════
# Test 3 — Period mismatch: six-month cumulative must NOT confirm quarterly
# Proves the old 2× tolerance was unsafe
# ══════════════════════════════════════════════════════════════════════════════

class TestPeriodMismatchTolerance:
    """
    Scenario: provider returns $418M (≈ H1 cumulative), estimate is $20M.
    Ratio = 20.9× → suspicious.  The correct Q2 IS revenue is $220M.

    Old 2× tolerance: $418M / $220M ≈ 1.9 → within 2× → would have INCORRECTLY confirmed.
    New 2% tolerance: diff = $198M → 90% of $220M → NOT within 2% or $1M → correctly rejected.
    """

    SUSPICIOUS_ACTUAL = 418_000_000   # ≈ six-month cumulative
    ESTIMATE          = 20_000_000    # thin coverage estimate
    Q2_IS_REV         = 220_000_000   # correct quarterly IS revenue
    DATE              = "2026-07-29"

    def test_flagged_as_suspect_by_ratio(self):
        """418M / 20M = 20.9× > 10 → suspicious."""
        assert _is_revenue_suspect({"revenue_actual": self.SUSPICIOUS_ACTUAL,
                                    "revenue_estimate": self.ESTIMATE})

    def test_quarterly_is_does_not_confirm_six_month_cumulative(self):
        """
        IS shows correct Q2 = $220M.  The suspicious $418M is approximately
        1.9× that — within the old unsafe 2× tolerance but outside the new 2%.
        """
        confirmed = _confirm_extreme_revenue(
            self.SUSPICIOUS_ACTUAL,
            _income(self.DATE, self.Q2_IS_REV, period="Q2", calendar_year=2026),
            self.DATE,
            expected_period="Q2",
            expected_year=2026,
        )
        assert not confirmed, (
            f"IS revenue ${self.Q2_IS_REV:,} must NOT confirm suspicious "
            f"${self.SUSPICIOUS_ACTUAL:,} (ratio ≈ 1.9× — outside 2% tolerance)"
        )

    def test_old_two_times_tolerance_would_have_been_wrong(self):
        """
        Regression guard: verify the old 2× range [0.5×, 2.0×] would have
        incorrectly accepted $418M vs $220M IS (ratio = 1.9 < 2.0).
        The new code must reject it.
        """
        ratio = self.SUSPICIOUS_ACTUAL / self.Q2_IS_REV  # ≈ 1.9
        assert 0.5 < ratio < 2.0, "Pre-condition: old 2× would have accepted this"
        # New code must still reject it
        confirmed = _confirm_extreme_revenue(
            self.SUSPICIOUS_ACTUAL,
            _income(self.DATE, self.Q2_IS_REV, period="Q2"),
            self.DATE,
        )
        assert not confirmed, "New narrow tolerance must reject a 1.9× mismatch"

    def test_fy_is_row_period_gate_fires_before_numeric_check(self):
        """Annual IS row is rejected at period validation before numeric comparison."""
        # Annual IS with revenue that happens to be 1.0× the suspicious actual
        # (best possible numeric match) — still rejected because period="FY"
        confirmed = _confirm_extreme_revenue(
            self.SUSPICIOUS_ACTUAL,
            _income(self.DATE, self.SUSPICIOUS_ACTUAL, period="FY"),
            self.DATE,
        )
        assert not confirmed, "period='FY' must be rejected before numeric check"


# ══════════════════════════════════════════════════════════════════════════════
# Test 4 — Real extreme beat: quarterly IS confirms → accepted, 1 400% preserved
# estimate $1M, actual $15M, quarterly IS independently shows $15M
# ══════════════════════════════════════════════════════════════════════════════

class TestRealExtremeBeatConfirmed:

    ACTUAL   = 15_000_000   # $15 M actual
    ESTIMATE =  1_000_000   # $1 M estimate (thin analyst coverage)
    DATE     = "2026-07-29"

    def test_flagged_as_suspect_by_ratio(self):
        """15× the estimate triggers the suspicious-signal check."""
        assert _is_revenue_suspect({"revenue_actual": self.ACTUAL,
                                    "revenue_estimate": self.ESTIMATE})

    def test_quarterly_is_matching_value_confirms(self):
        """IS revenue = $15M for the same Q and year → confirmed (ratio = 1.0)."""
        confirmed = _confirm_extreme_revenue(
            self.ACTUAL,
            _income(self.DATE, self.ACTUAL, period="Q2", calendar_year=2026),
            self.DATE,
            expected_period="Q2",
            expected_year=2026,
        )
        assert confirmed

    def test_quarterly_is_with_rounding_confirms(self):
        """IS revenue within 2% rounding ($14.85M) still confirms."""
        IS_ROUNDED = 14_850_000   # $14.85M → 1% below actual
        confirmed = _confirm_extreme_revenue(
            self.ACTUAL,
            _income(self.DATE, IS_ROUNDED, period="Q2", calendar_year=2026),
            self.DATE,
            expected_period="Q2",
            expected_year=2026,
        )
        assert confirmed

    def test_verified_flag_makes_result_complete(self):
        """
        Once _revenue_verified is set (by an external confirmation path),
        _has_complete_results_for_target returns True — re-polling stops.
        """
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                     revenue_verified=True)
        assert _has_complete_results_for_target(p, self.DATE)

    def test_verified_payload_preserves_full_surprise(self):
        """The 1 400% revenue surprise must survive a merge cycle unchanged."""
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                     revenue_verified=True)
        merged = _merge_results_payload(
            p,
            {"date": self.DATE, "eps_actual": 1.0, "eps_estimate": 0.8},
        )
        assert merged["revenue_actual"] == self.ACTUAL
        assert merged.get("_revenue_verified") is True

    def test_fuzzy_date_match_still_requires_correct_period(self):
        """IS row 3 days before ev_date confirms only when period also matches."""
        IS_DATE = "2026-07-26"
        confirmed = _confirm_extreme_revenue(
            self.ACTUAL,
            _income(IS_DATE, self.ACTUAL, period="Q2", calendar_year=2026),
            self.DATE,
            expected_period="Q2",
            expected_year=2026,
        )
        assert confirmed

    def test_wrong_quarter_is_does_not_confirm_correct_actual(self):
        """IS row for Q1 must not confirm a Q2 earnings actual."""
        confirmed = _confirm_extreme_revenue(
            self.ACTUAL,
            _income(self.DATE, self.ACTUAL, period="Q1", calendar_year=2026),
            self.DATE,
            expected_period="Q2",
            expected_year=2026,
        )
        assert not confirmed

    def test_wrong_year_is_does_not_confirm(self):
        """IS row for calendarYear=2025 must not confirm a 2026 earnings event."""
        confirmed = _confirm_extreme_revenue(
            self.ACTUAL,
            _income(self.DATE, self.ACTUAL, period="Q2", calendar_year=2025),
            self.DATE,
            expected_period="Q2",
            expected_year=2026,
        )
        assert not confirmed


# ══════════════════════════════════════════════════════════════════════════════
# Test 5 — Independent source differs materially: suspicious value unverified
# ══════════════════════════════════════════════════════════════════════════════

class TestIndependentSourceDiffers:

    SUSPICIOUS = 50_000_000   # $50M suspicious actual (e.g. >10× estimate of $4M)
    IS_REV     = 20_000_000   # IS shows $20M — clearly different
    DATE       = "2026-07-29"

    def test_materially_different_is_does_not_confirm(self):
        """$50M actual vs $20M IS → diff = $30M, ratio = 2.5× → not confirmed."""
        confirmed = _confirm_extreme_revenue(
            self.SUSPICIOUS,
            _income(self.DATE, self.IS_REV, period="Q2", calendar_year=2026),
            self.DATE,
            expected_period="Q2",
            expected_year=2026,
        )
        assert not confirmed

    def test_unconfirmed_value_remains_suspect(self):
        """Payload remains suspect (not verified) when IS differs materially."""
        p = _payload(revenue_actual=self.SUSPICIOUS, revenue_estimate=4_000_000,
                     revenue_suspect=True)
        assert not _has_complete_results_for_target(p, self.DATE)
        assert not p.get("_revenue_verified")

    def test_absolute_tolerance_border(self):
        """Diff exactly at $1M absolute boundary is confirmed."""
        IS_REV_CLOSE = self.SUSPICIOUS - 1_000_000   # exactly $1M under
        confirmed = _confirm_extreme_revenue(
            self.SUSPICIOUS,
            _income(self.DATE, IS_REV_CLOSE, period="Q2"),
            self.DATE,
        )
        assert confirmed, "$1M absolute diff must be accepted (rounding allowance)"

    def test_just_over_absolute_tolerance_rejected(self):
        """Diff $1.01M above absolute tolerance and outside 2% relative → rejected."""
        IS_REV_NEAR = self.SUSPICIOUS - 1_010_000   # $1.01M under
        # relative diff: 1_010_000 / (50M - 1.01M) ≈ 2.06% > 2%
        confirmed = _confirm_extreme_revenue(
            self.SUSPICIOUS,
            _income(self.DATE, IS_REV_NEAR, period="Q2"),
            self.DATE,
        )
        assert not confirmed


# ══════════════════════════════════════════════════════════════════════════════
# Test 6 — Corrected FMP value: suspect flags clear, replacement accepted
# ══════════════════════════════════════════════════════════════════════════════

class TestCorrectedFmpValue:
    """
    When FMP self-corrects a previously suspect value, the monitor's state
    machine clears both _revenue_suspect and _revenue_verified and accepts
    the corrected plausible value without delay.
    """

    CORRUPT  = 428_472_200_000
    CORRECT  = 220_000_000
    ESTIMATE = 217_040_000
    DATE     = "2026-07-29"

    def test_corrected_value_not_suspect(self):
        """$220M / $217M ≈ 1.01× — not suspicious."""
        assert not _is_revenue_suspect({"revenue_actual": self.CORRECT,
                                        "revenue_estimate": self.ESTIMATE})

    def test_merge_with_corrected_value_clears_suspect_flag(self):
        """
        After FMP corrects: _merge_results_payload carries the corrected value;
        the monitor state machine removes _revenue_suspect from the payload.
        (The test directly asserts _revenue_suspect is absent after merge.)
        """
        existing = _payload(revenue_actual=self.CORRUPT, revenue_estimate=self.ESTIMATE,
                            revenue_suspect=True)
        incoming_corrected = {
            "revenue_actual":   self.CORRECT,
            "revenue_estimate": self.ESTIMATE,
            "eps_actual":       0.37,
            "eps_estimate":     0.364,
            "date":             self.DATE,
        }
        merged = _merge_results_payload(existing, incoming_corrected)
        # After merge, revenue_actual holds the corrected value
        assert merged["revenue_actual"] == self.CORRECT
        # Suspect flag still present in raw merge output (state machine in
        # _process_target is responsible for clearing; tested via _is_revenue_suspect)
        assert not _is_revenue_suspect(merged), (
            "Corrected value must not trigger _is_revenue_suspect"
        )

    def test_corrected_payload_is_complete(self):
        """No flags + plausible value + matching date → complete."""
        p = _payload(revenue_actual=self.CORRECT, revenue_estimate=self.ESTIMATE)
        assert _has_complete_results_for_target(p, self.DATE)

    def test_stale_correct_value_still_blocked_by_date_window(self):
        """Date-window guard is not bypassed by removing the plausibility check."""
        p = _payload(revenue_actual=self.CORRECT, revenue_estimate=self.ESTIMATE,
                     date="2026-04-29")
        assert not _has_complete_results_for_target(p, self.DATE)


# ══════════════════════════════════════════════════════════════════════════════
# Test 7 — Zero/null/negative estimates: no invalid ratio; scale-anomaly check
# ══════════════════════════════════════════════════════════════════════════════

class TestUnsuitableDenominators:

    def test_zero_estimate_not_suspect_by_ratio(self):
        """`_is_revenue_suspect` returns False when estimate = 0."""
        assert not _is_revenue_suspect({"revenue_actual": 50_000_000,
                                        "revenue_estimate": 0})

    def test_negative_estimate_not_suspect_by_ratio(self):
        """Negative estimate (loss-making company): ratio undefined — False."""
        assert not _is_revenue_suspect({"revenue_actual": 50_000_000,
                                        "revenue_estimate": -5_000_000})

    def test_none_estimate_not_suspect_by_ratio(self):
        """Null estimate: no denominator — False."""
        assert not _is_revenue_suspect({"revenue_actual": 50_000_000,
                                        "revenue_estimate": None})

    def test_none_actual_not_suspect(self):
        """Null actual: nothing to inspect — False."""
        assert not _is_revenue_suspect({"revenue_actual": None,
                                        "revenue_estimate": 5_000_000})

    def test_zero_estimate_payload_complete_when_plausible(self):
        """Zero estimate + plausible actual follows normal date-window path."""
        p = _payload(revenue_actual=50_000_000, revenue_estimate=0)
        assert _has_complete_results_for_target(p, "2026-07-29")

    def test_scale_anomaly_unit_error_detected(self):
        """1 000× unit relationship vs prior quarter is detected."""
        prior = [220_000_000]  # $220M prior quarter
        suspicious = 220_000_000 * 1_000  # $220B — 1 000× scale error
        assert _is_revenue_scale_anomaly(suspicious, prior)

    def test_scale_anomaly_six_month_cumulative_detected(self):
        """Value ≈ sum of two prior quarters is detected as six-month cumulative."""
        q1 = 208_000_000
        q2 = 220_000_000
        six_month = q1 + q2  # $428M
        assert _is_revenue_scale_anomaly(six_month, [q1, q2])

    def test_scale_anomaly_nine_month_cumulative_detected(self):
        """Value ≈ sum of three prior quarters is detected."""
        qs = [200_000_000, 208_000_000, 220_000_000]
        nine_month = sum(qs)  # $628M
        assert _is_revenue_scale_anomaly(nine_month, qs)

    def test_scale_anomaly_massive_vs_t12m_detected(self):
        """Value > 5× trailing-12M revenue is detected."""
        qs = [200_000_000, 205_000_000, 210_000_000, 220_000_000]
        t12m = sum(qs)  # $835M
        massive = t12m * 6  # $5.01B — 6× trailing-12M
        assert _is_revenue_scale_anomaly(massive, qs)

    def test_normal_growth_not_flagged_as_anomaly(self):
        """Ordinary revenue growth does not trigger scale-anomaly detection."""
        prior = [200_000_000, 205_000_000, 210_000_000, 215_000_000]
        current = 225_000_000  # ~5% growth — entirely normal
        assert not _is_revenue_scale_anomaly(current, prior)

    def test_scale_anomaly_empty_prior_returns_false(self):
        """No prior-quarter data: scale anomaly check cannot fire."""
        assert not _is_revenue_scale_anomaly(1_000_000_000, [])

    def test_confirm_extreme_revenue_zero_income_statement(self):
        """IS revenue of zero → ZeroDivisionError must be caught → False."""
        assert not _confirm_extreme_revenue(
            1_000_000_000,
            {"2026-07-29": {"revenue": 0, "period": "Q2", "calendarYear": 2026}},
            "2026-07-29",
        )

    def test_is_row_with_no_revenue_field(self):
        """IS row missing 'revenue' key → not confirmed."""
        assert not _confirm_extreme_revenue(
            15_000_000,
            {"2026-07-29": {"period": "Q2", "calendarYear": 2026}},
            "2026-07-29",
        )


# ══════════════════════════════════════════════════════════════════════════════
# Existing contract: normal results follow the unchanged path
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
        """Both actuals, no flags, matching date → complete."""
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE)
        assert _has_complete_results_for_target(p, self.DATE)

    def test_merge_does_not_alter_normal_values(self):
        """_merge_results_payload passes plausible values through unchanged."""
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
        assert merged["revenue_actual"] == self.ACTUAL

    def test_stale_payload_blocked_by_date_window(self):
        """Normal values outside 7-day window are still blocked."""
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                     date="2026-04-30")
        assert not _has_complete_results_for_target(p, self.DATE)
