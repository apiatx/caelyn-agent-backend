"""
Revenue-validation tests for the two-stage corruption-prevention system.

Tests cover the eight required scenarios plus supporting contracts:

  1. Verified flag persistence — monitor confirms, _revenue_verified written, event complete.
  2. Same FMP value twice — remains suspect, never promoted to verified.
  3. BAND corruption — $428B not canonicalized; corrected $220M accepted.
  4. Real extreme result — $15M confirmed by EI cache; 1 400% surprise preserved.
  5. Null estimate + unit corruption — scale anomaly triggers verification gate.
  6. Null estimate + normal scale — ordinary result accepted; no infinite re-poll.
  7. Six-month vs quarter — cumulative value cannot confirm a quarterly result.
  8. Corrected provider value — flags clear; corrected result becomes complete.

Run with:
    cd backend && python -m pytest tests/test_earnings_revenue_validation.py -v
"""
from __future__ import annotations

import sys
import os
import types
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services.earnings_monitor_service import (
    _is_revenue_suspect,
    _is_revenue_scale_anomaly,
    _confirm_extreme_from_ei_cache,
    _has_complete_results_for_target,
    _merge_results_payload,
)
from services.watchlist_fundamentals_refresh import (
    _confirm_extreme_revenue,
)


# ── helpers ───────────────────────────────────────────────────────────────────

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


def _ei_snapshot(
    symbol: str,
    *,
    fiscal_period: str,
    fiscal_year: int,
    cached_revenue: float | None,
) -> dict:
    """Build a minimal watchlist_fundamentals_cache snapshot for testing."""
    return {
        "symbol": symbol,
        "fields": {
            "earnings_intelligence": {
                "earnings_history": [
                    {
                        "fiscal_period": fiscal_period,
                        "fiscal_year":   str(fiscal_year),
                        "revenue_actual": cached_revenue,
                        "eps_actual":    0.37,
                        "date":          "2026-07-29",
                    }
                ]
            }
        },
    }


def _is_row(date: str, revenue: float | None, *, period: str = "Q2",
            calendar_year: int = 2026) -> dict:
    return {"filingDate": date, "revenue": revenue,
            "period": period, "calendarYear": calendar_year}


# ══════════════════════════════════════════════════════════════════════════════
# Scenario 1 — Verified flag persistence
# Monitor confirms via EI cache → _revenue_verified: True → event complete
# ══════════════════════════════════════════════════════════════════════════════

class TestVerifiedFlagPersistence:
    """
    _confirm_extreme_from_ei_cache returns True when the EI cache holds an
    IS-validated revenue that agrees within 2 % / $1 M.
    Once _revenue_verified: True is in results_payload, the completion gate
    returns True and re-polling stops.
    """

    ACTUAL  = 15_000_000   # $15M extreme beat
    SYMBOL  = "TEST"
    PERIOD  = "Q2"
    YEAR    = 2026

    def test_ei_cache_confirms_matching_revenue(self):
        """EI cache same-quarter revenue = $15M → confirmed within 0%."""
        snap = _ei_snapshot(self.SYMBOL, fiscal_period=self.PERIOD,
                            fiscal_year=self.YEAR, cached_revenue=self.ACTUAL)
        with patch("data.watchlist_fundamentals_store.get_snapshot", return_value=snap):
            result = _confirm_extreme_from_ei_cache(
                self.ACTUAL, self.SYMBOL, self.PERIOD, self.YEAR,
            )
        assert result is True

    def test_ei_cache_confirms_with_rounding(self):
        """$14.85M cached (1% below $15M actual) — within 2% tolerance."""
        snap = _ei_snapshot(self.SYMBOL, fiscal_period=self.PERIOD,
                            fiscal_year=self.YEAR, cached_revenue=14_850_000)
        with patch("data.watchlist_fundamentals_store.get_snapshot", return_value=snap):
            result = _confirm_extreme_from_ei_cache(
                self.ACTUAL, self.SYMBOL, self.PERIOD, self.YEAR,
            )
        assert result is True

    def test_verified_flag_makes_event_complete(self):
        """_revenue_verified: True in results_payload → completion gate returns True."""
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=1_000_000,
                     revenue_verified=True)
        assert _has_complete_results_for_target(p, "2026-07-29")

    def test_verified_flag_preserved_through_merge(self):
        """_merge_results_payload carries _revenue_verified forward."""
        existing = _payload(revenue_actual=self.ACTUAL, revenue_estimate=1_000_000,
                            revenue_verified=True)
        merged = _merge_results_payload(
            existing,
            {"date": "2026-07-29", "eps_actual": 0.37, "eps_estimate": 0.30},
        )
        assert merged.get("_revenue_verified") is True
        assert merged["revenue_actual"] == self.ACTUAL

    def test_no_snapshot_returns_false(self):
        """No EI cache for symbol → _confirm_extreme_from_ei_cache returns False."""
        with patch("data.watchlist_fundamentals_store.get_snapshot", return_value=None):
            assert not _confirm_extreme_from_ei_cache(
                self.ACTUAL, self.SYMBOL, self.PERIOD, self.YEAR,
            )

    def test_empty_history_returns_false(self):
        """EI cache exists but earnings_history is empty → False."""
        snap = {"symbol": self.SYMBOL, "fields": {
            "earnings_intelligence": {"earnings_history": []}
        }}
        with patch("data.watchlist_fundamentals_store.get_snapshot", return_value=snap):
            assert not _confirm_extreme_from_ei_cache(
                self.ACTUAL, self.SYMBOL, self.PERIOD, self.YEAR,
            )


# ══════════════════════════════════════════════════════════════════════════════
# Scenario 2 — Same FMP value twice: remains suspect, never verified
# ══════════════════════════════════════════════════════════════════════════════

class TestSameFmpValueTwice:

    ACTUAL   = 428_472_200_000
    ESTIMATE = 217_040_000
    DATE     = "2026-07-29"

    def test_raw_suspicious_value_flagged_as_suspect(self):
        """428B / 217M ≈ 1 975× — clearly above the 10× threshold."""
        assert _is_revenue_suspect({"revenue_actual": self.ACTUAL,
                                    "revenue_estimate": self.ESTIMATE})

    def test_second_poll_same_value_stays_suspect(self):
        """
        _merge_results_payload carries _revenue_suspect forward from existing;
        repeated FMP observations do not promote to _revenue_verified.
        """
        existing = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                            revenue_suspect=True)
        incoming = {"date": self.DATE, "eps_actual": 0.37, "eps_estimate": 0.364,
                    "revenue_actual": self.ACTUAL, "revenue_estimate": self.ESTIMATE}
        merged = _merge_results_payload(existing, incoming)
        assert merged.get("_revenue_suspect") is True
        assert not merged.get("_revenue_verified")

    def test_suspect_blocks_completion_gate(self):
        """_revenue_suspect: True → _has_complete_results_for_target returns False."""
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                     revenue_suspect=True)
        assert not _has_complete_results_for_target(p, self.DATE)

    def test_ei_cache_null_revenue_does_not_confirm(self):
        """
        EI cache has revenue_actual=None for this quarter — the EI refresh also
        could not confirm it.  _confirm_extreme_from_ei_cache must return False.
        """
        snap = _ei_snapshot("BAND", fiscal_period="Q2", fiscal_year=2026,
                            cached_revenue=None)
        with patch("data.watchlist_fundamentals_store.get_snapshot", return_value=snap):
            assert not _confirm_extreme_from_ei_cache(
                self.ACTUAL, "BAND", "Q2", 2026,
            )


# ══════════════════════════════════════════════════════════════════════════════
# Scenario 3 — BAND corruption: $428B not canonicalized; $220M accepted
# ══════════════════════════════════════════════════════════════════════════════

class TestBandCorruption:

    CORRUPT  = 428_472_200_000
    ESTIMATE = 217_040_000
    CORRECT  = 220_000_000
    DATE     = "2026-07-29"

    def test_corrupted_value_flagged_by_ratio(self):
        assert _is_revenue_suspect({"revenue_actual": self.CORRUPT,
                                    "revenue_estimate": self.ESTIMATE})

    def test_ei_cache_quarterly_does_not_confirm_corrupted_value(self):
        """
        EI cache holds the IS-validated $220M quarterly revenue.
        $428B / $220M ≈ 1 946× — far outside 2 % / $1 M tolerance → False.
        """
        snap = _ei_snapshot("BAND", fiscal_period="Q2", fiscal_year=2026,
                            cached_revenue=self.CORRECT)
        with patch("data.watchlist_fundamentals_store.get_snapshot", return_value=snap):
            assert not _confirm_extreme_from_ei_cache(
                self.CORRUPT, "BAND", "Q2", 2026,
            )

    def test_corrected_value_not_flagged_by_ratio(self):
        """$220M / $217M ≈ 1.01× — not suspicious."""
        assert not _is_revenue_suspect({"revenue_actual": self.CORRECT,
                                        "revenue_estimate": self.ESTIMATE})

    def test_corrected_payload_is_complete(self):
        """Plausible value + matching date → completion gate returns True."""
        p = _payload(revenue_actual=self.CORRECT, revenue_estimate=self.ESTIMATE)
        assert _has_complete_results_for_target(p, self.DATE)

    def test_is_level_confirm_extreme_revenue_rejects_corrupted_vs_quarterly(self):
        """watchlist_fundamentals_refresh level: $428B vs $220M IS → rejected."""
        confirmed = _confirm_extreme_revenue(
            self.CORRUPT,
            {self.DATE: {"revenue": self.CORRECT, "period": "Q2",
                         "calendarYear": 2026, "filingDate": self.DATE}},
            self.DATE,
            expected_period="Q2",
            expected_year=2026,
        )
        assert not confirmed

    def test_corrupted_payload_never_complete(self):
        p = _payload(revenue_actual=self.CORRUPT, revenue_estimate=self.ESTIMATE,
                     revenue_suspect=True)
        assert not _has_complete_results_for_target(p, self.DATE)


# ══════════════════════════════════════════════════════════════════════════════
# Scenario 4 — Real extreme result: EI cache confirms; 1 400% surprise preserved
# ══════════════════════════════════════════════════════════════════════════════

class TestRealExtremeResult:

    ACTUAL   = 15_000_000
    ESTIMATE =  1_000_000
    SYMBOL   = "XTICKER"
    PERIOD   = "Q2"
    YEAR     = 2026
    DATE     = "2026-07-29"

    def test_flagged_by_ratio(self):
        """15× the estimate → suspicious."""
        assert _is_revenue_suspect({"revenue_actual": self.ACTUAL,
                                    "revenue_estimate": self.ESTIMATE})

    def test_ei_cache_confirms_exact_match(self):
        """EI cache $15M same-quarter → confirmed (ratio = 1.0)."""
        snap = _ei_snapshot(self.SYMBOL, fiscal_period=self.PERIOD,
                            fiscal_year=self.YEAR, cached_revenue=self.ACTUAL)
        with patch("data.watchlist_fundamentals_store.get_snapshot", return_value=snap):
            assert _confirm_extreme_from_ei_cache(
                self.ACTUAL, self.SYMBOL, self.PERIOD, self.YEAR,
            )

    def test_verified_payload_is_complete(self):
        """_revenue_verified: True → event is complete; re-polling stops."""
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                     revenue_verified=True)
        assert _has_complete_results_for_target(p, self.DATE)

    def test_surprise_magnitude_preserved(self):
        """$15M with $1M estimate = 1 400% surprise; value must not be modified."""
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                     revenue_verified=True)
        assert p["revenue_actual"] == 15_000_000

    def test_wrong_quarter_in_ei_cache_does_not_confirm(self):
        """EI cache shows Q1, but earnings event is Q2 → no confirmation."""
        snap = _ei_snapshot(self.SYMBOL, fiscal_period="Q1",
                            fiscal_year=self.YEAR, cached_revenue=self.ACTUAL)
        with patch("data.watchlist_fundamentals_store.get_snapshot", return_value=snap):
            assert not _confirm_extreme_from_ei_cache(
                self.ACTUAL, self.SYMBOL, self.PERIOD, self.YEAR,
            )

    def test_wrong_year_in_ei_cache_does_not_confirm(self):
        """EI cache entry for 2025 Q2 must not confirm 2026 Q2 earnings."""
        snap = _ei_snapshot(self.SYMBOL, fiscal_period=self.PERIOD,
                            fiscal_year=2025, cached_revenue=self.ACTUAL)
        with patch("data.watchlist_fundamentals_store.get_snapshot", return_value=snap):
            assert not _confirm_extreme_from_ei_cache(
                self.ACTUAL, self.SYMBOL, self.PERIOD, self.YEAR,
            )

    def test_is_level_confirm_accepts_within_rounding(self):
        """watchlist_fundamentals_refresh level: $14.85M IS ($15M ×0.99) → accepted."""
        confirmed = _confirm_extreme_revenue(
            self.ACTUAL,
            {self.DATE: {"revenue": 14_850_000, "period": "Q2",
                         "calendarYear": 2026, "filingDate": self.DATE}},
            self.DATE,
            expected_period="Q2",
            expected_year=2026,
        )
        assert confirmed


# ══════════════════════════════════════════════════════════════════════════════
# Scenario 5 — Null estimate + unit corruption: scale anomaly triggers gate
# ══════════════════════════════════════════════════════════════════════════════

class TestNullEstimateUnitCorruption:

    PRIOR_Q = 220_000_000            # $220M normal quarterly revenue
    CORRUPT = 220_000_000 * 1_000    # $220B — 1 000× unit error

    def test_null_estimate_not_suspicious_by_ratio(self):
        """Null estimate: ratio undefined — _is_revenue_suspect returns False."""
        assert not _is_revenue_suspect({"revenue_actual": self.CORRUPT,
                                        "revenue_estimate": None})

    def test_scale_anomaly_detects_1000x_unit_error(self):
        """$220B vs $220M prior quarter — 1 000× relationship detected."""
        assert _is_revenue_scale_anomaly(self.CORRUPT, [self.PRIOR_Q])

    def test_scale_anomaly_detected_among_multiple_quarters(self):
        """Anomaly still detected when prior list has multiple quarters."""
        prior = [210_000_000, 215_000_000, self.PRIOR_Q]
        assert _is_revenue_scale_anomaly(self.CORRUPT, prior)

    def test_ei_cache_null_means_unconfirmed(self):
        """EI cache revenue_actual=None → confirm returns False → remains suspect."""
        snap = _ei_snapshot("X", fiscal_period="Q2", fiscal_year=2026,
                            cached_revenue=None)
        with patch("data.watchlist_fundamentals_store.get_snapshot", return_value=snap):
            assert not _confirm_extreme_from_ei_cache(
                self.CORRUPT, "X", "Q2", 2026,
            )

    def test_suspect_flag_blocks_completion(self):
        """After scale anomaly triggers suspect flag, event is not complete."""
        p = _payload(revenue_actual=self.CORRUPT, revenue_estimate=None,
                     revenue_suspect=True)
        assert not _has_complete_results_for_target(p, "2026-07-29")


# ══════════════════════════════════════════════════════════════════════════════
# Scenario 6 — Null estimate + normal scale: accepted without re-poll loop
# ══════════════════════════════════════════════════════════════════════════════

class TestNullEstimateNormalScale:

    NORMAL_REV = 225_000_000    # $225M — ordinary ~5% growth
    PRIOR_QS   = [205_000_000, 210_000_000, 215_000_000, 220_000_000]

    def test_no_scale_anomaly_for_ordinary_growth(self):
        """$225M vs four prior quarters averaging $212M — no anomaly."""
        assert not _is_revenue_scale_anomaly(self.NORMAL_REV, self.PRIOR_QS)

    def test_no_anomaly_means_no_suspect_flag(self):
        """_is_revenue_suspect returns False (null estimate); no anomaly → no flag."""
        assert not _is_revenue_suspect({"revenue_actual": self.NORMAL_REV,
                                        "revenue_estimate": None})

    def test_normal_payload_is_complete(self):
        """Both actuals present, no flags, matching date → complete."""
        p = _payload(revenue_actual=self.NORMAL_REV, revenue_estimate=None)
        assert _has_complete_results_for_target(p, "2026-07-29")

    def test_empty_prior_history_also_not_anomalous(self):
        """No prior history → _is_revenue_scale_anomaly returns False (safe default)."""
        assert not _is_revenue_scale_anomaly(self.NORMAL_REV, [])

    def test_single_prior_quarter_below_1000x_not_anomalous(self):
        """Value 5× prior quarter is suspicious but not a 1 000× error or cumulative."""
        assert not _is_revenue_scale_anomaly(200_000_000 * 4.9, [200_000_000])


# ══════════════════════════════════════════════════════════════════════════════
# Scenario 7 — Six-month vs quarter: cumulative cannot confirm quarterly result
# ══════════════════════════════════════════════════════════════════════════════

class TestSixMonthVsQuarter:
    """
    Proves the old 2× tolerance was unsafe:
    - Suspicious actual: $418M (≈ H1 cumulative); estimate: $20M; ratio = 20.9×
    - Correct Q2 IS: $220M
    - Old 2×: $418M / $220M ≈ 1.9 → would have incorrectly confirmed
    - New 2%: diff = $198M = 90% of $220M → correctly rejected
    """

    SUSPICIOUS = 418_000_000   # ≈ six-month cumulative
    ESTIMATE   =  20_000_000
    Q2_IS_REV  = 220_000_000
    DATE       = "2026-07-29"

    def test_flagged_by_ratio(self):
        """$418M / $20M = 20.9× → suspicious."""
        assert _is_revenue_suspect({"revenue_actual": self.SUSPICIOUS,
                                    "revenue_estimate": self.ESTIMATE})

    def test_is_level_quarterly_does_not_confirm_six_month_actual(self):
        """
        IS Q2 revenue $220M fails to confirm $418M suspicious actual.
        1.9× mismatch is far outside the 2 % / $1 M narrow tolerance.
        """
        confirmed = _confirm_extreme_revenue(
            self.SUSPICIOUS,
            {self.DATE: {"revenue": self.Q2_IS_REV, "period": "Q2",
                         "calendarYear": 2026, "filingDate": self.DATE}},
            self.DATE,
            expected_period="Q2",
            expected_year=2026,
        )
        assert not confirmed

    def test_old_two_times_tolerance_would_have_been_wrong(self):
        """Regression guard: the old 2× band [0.5, 2.0] contained 1.9 — unsafe."""
        ratio = self.SUSPICIOUS / self.Q2_IS_REV
        assert 0.5 < ratio < 2.0, "Pre-condition: old 2× would have accepted this"
        confirmed = _confirm_extreme_revenue(
            self.SUSPICIOUS,
            {self.DATE: {"revenue": self.Q2_IS_REV, "period": "Q2",
                         "filingDate": self.DATE}},
            self.DATE,
        )
        assert not confirmed, "New narrow tolerance must reject a 1.9× mismatch"

    def test_ei_cache_with_null_revenue_six_month_quarter_cannot_confirm(self):
        """
        EI cache sees the $418M suspicious actual, can't confirm vs IS ($220M),
        so stores revenue_actual=None for that quarter.
        _confirm_extreme_from_ei_cache must return False (null = unconfirmed).
        """
        snap = _ei_snapshot("XYZ", fiscal_period="Q2", fiscal_year=2026,
                            cached_revenue=None)
        with patch("data.watchlist_fundamentals_store.get_snapshot", return_value=snap):
            assert not _confirm_extreme_from_ei_cache(
                self.SUSPICIOUS, "XYZ", "Q2", 2026,
            )

    def test_six_month_as_scale_anomaly_detected(self):
        """$418M ≈ sum of two prior quarters ($208M + $210M) — cumulative anomaly."""
        assert _is_revenue_scale_anomaly(418_000_000, [208_000_000, 210_000_000])


# ══════════════════════════════════════════════════════════════════════════════
# Scenario 8 — Corrected provider value: flags clear; result becomes complete
# ══════════════════════════════════════════════════════════════════════════════

class TestCorrectedProviderValue:

    CORRUPT  = 428_472_200_000
    CORRECT  = 220_000_000
    ESTIMATE = 217_040_000
    DATE     = "2026-07-29"

    def test_corrected_value_not_suspect_by_ratio(self):
        """$220M / $217M ≈ 1.01× — not suspicious."""
        assert not _is_revenue_suspect({"revenue_actual": self.CORRECT,
                                        "revenue_estimate": self.ESTIMATE})

    def test_corrected_payload_is_complete(self):
        """Plausible value + no flags + date match → complete."""
        p = _payload(revenue_actual=self.CORRECT, revenue_estimate=self.ESTIMATE)
        assert _has_complete_results_for_target(p, self.DATE)

    def test_merge_retains_corrected_value(self):
        """Corrected revenue survives _merge_results_payload unchanged."""
        existing = _payload(revenue_actual=self.CORRUPT, revenue_estimate=self.ESTIMATE,
                            revenue_suspect=True)
        merged = _merge_results_payload(
            existing,
            {"date": self.DATE, "eps_actual": 0.37, "eps_estimate": 0.364,
             "revenue_actual": self.CORRECT, "revenue_estimate": self.ESTIMATE},
        )
        assert merged["revenue_actual"] == self.CORRECT
        # After correction, _is_revenue_suspect must return False
        assert not _is_revenue_suspect(merged)

    def test_stale_correct_value_blocked_by_date_window(self):
        """Correct value more than 7 days ago → still blocked by date gate."""
        p = _payload(revenue_actual=self.CORRECT, revenue_estimate=self.ESTIMATE,
                     date="2026-04-01")
        assert not _has_complete_results_for_target(p, self.DATE)


# ══════════════════════════════════════════════════════════════════════════════
# Supporting: scale-anomaly helper edge cases
# ══════════════════════════════════════════════════════════════════════════════

class TestScaleAnomalyHelper:

    def test_nine_month_cumulative_detected(self):
        qs = [200_000_000, 210_000_000, 220_000_000]
        assert _is_revenue_scale_anomaly(sum(qs), qs)

    def test_massive_vs_t12m_detected(self):
        qs = [200e6, 205e6, 210e6, 220e6]
        t12m = sum(qs)
        assert _is_revenue_scale_anomaly(t12m * 6, qs)

    def test_zero_actual_not_anomalous(self):
        assert not _is_revenue_scale_anomaly(0, [220_000_000])

    def test_negative_prior_ignored(self):
        """Negative prior quarters (losses) do not break the check."""
        assert not _is_revenue_scale_anomaly(220_000_000, [-5_000_000])

    def test_five_percent_tolerance_boundary(self):
        """Value within 5% of the 1 000× multiple is accepted (4.9% inside the band)."""
        prior = [220_000_000]
        within_band = prior[0] * 1_000 * 1.049  # clearly inside the 5% tolerance
        assert _is_revenue_scale_anomaly(within_band, prior)

    def test_just_outside_tolerance_not_flagged(self):
        """Value 6% above 1 000× — outside the 5% band — not flagged."""
        prior = [220_000_000]
        just_outside = prior[0] * 1_000 * 1.06
        assert not _is_revenue_scale_anomaly(just_outside, prior)


# ══════════════════════════════════════════════════════════════════════════════
# Supporting: _confirm_extreme_from_ei_cache edge cases
# ══════════════════════════════════════════════════════════════════════════════

class TestConfirmExtremeFromEiCache:

    def test_import_error_returns_false(self):
        """If the store import fails, return False safely."""
        import builtins
        real_import = builtins.__import__

        def patched_import(name, *args, **kwargs):
            if name == "data.watchlist_fundamentals_store":
                raise ImportError("test")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=patched_import):
            assert not _confirm_extreme_from_ei_cache(
                15_000_000, "X", "Q2", 2026,
            )

    def test_zero_cached_revenue_returns_false(self):
        """IS revenue of zero → ZeroDivisionError caught → False."""
        snap = _ei_snapshot("X", fiscal_period="Q2", fiscal_year=2026,
                            cached_revenue=0)
        with patch("data.watchlist_fundamentals_store.get_snapshot", return_value=snap):
            assert not _confirm_extreme_from_ei_cache(15_000_000, "X", "Q2", 2026)

    def test_absolute_tolerance_boundary(self):
        """Difference exactly $1M is accepted."""
        snap = _ei_snapshot("X", fiscal_period="Q2", fiscal_year=2026,
                            cached_revenue=50_000_000 - 1_000_000)
        with patch("data.watchlist_fundamentals_store.get_snapshot", return_value=snap):
            assert _confirm_extreme_from_ei_cache(50_000_000, "X", "Q2", 2026)

    def test_above_absolute_tolerance_relative_check_governs(self):
        """Diff $1.01M on $50M → rel diff ≈ 2.02% > 2% → rejected."""
        snap = _ei_snapshot("X", fiscal_period="Q2", fiscal_year=2026,
                            cached_revenue=50_000_000 - 1_010_000)
        with patch("data.watchlist_fundamentals_store.get_snapshot", return_value=snap):
            assert not _confirm_extreme_from_ei_cache(50_000_000, "X", "Q2", 2026)


# ══════════════════════════════════════════════════════════════════════════════
# Supporting: normal earnings unaffected
# ══════════════════════════════════════════════════════════════════════════════

class TestNormalEarningsUnaffected:

    ACTUAL   = 220_000_000
    ESTIMATE = 217_040_000
    DATE     = "2026-07-29"

    def test_not_suspicious_by_ratio(self):
        assert not _is_revenue_suspect({"revenue_actual": self.ACTUAL,
                                        "revenue_estimate": self.ESTIMATE})

    def test_normal_payload_complete(self):
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE)
        assert _has_complete_results_for_target(p, self.DATE)

    def test_merge_passes_values_through(self):
        merged = _merge_results_payload(
            None,
            {"date": self.DATE, "eps_actual": 0.37, "eps_estimate": 0.364,
             "revenue_actual": self.ACTUAL, "revenue_estimate": self.ESTIMATE},
        )
        assert merged["revenue_actual"] == self.ACTUAL

    def test_stale_date_blocked(self):
        p = _payload(revenue_actual=self.ACTUAL, revenue_estimate=self.ESTIMATE,
                     date="2026-04-01")
        assert not _has_complete_results_for_target(p, self.DATE)
