"""
Focused tests for the FMP result-matching guard (_select_matching_fmp_result)
and the Recent section ET date-filter.

Tests 1–8  : original contract (Q1-rejection, Q2-acceptance, partials, stale)
Tests 9–12 : two-pass algorithm contract (nearest-date, labeled-only, no-fallback)
Tests 13–15: robustness (malformed dates, malformed labels, no-context guard)
Tests 16–19: Recent date-filter + no-dedup contract (DB-cursor mocked)

Run with:
    cd backend && python -m pytest tests/test_earnings_fmp_matching.py -v
"""
from __future__ import annotations

import sys
import os
import pytest
from datetime import date, timedelta
from unittest.mock import MagicMock

# ── path bootstrap ─────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── helper: build a synthetic FMP earnings record ──────────────────────────────

def _rec(
    date_str: str,
    eps_actual=None,
    revenue_actual=None,
    fiscal_year: int | None = None,
    fiscal_period: str | None = None,
    eps_estimate: float | None = None,
    revenue_estimate: float | None = None,
) -> dict:
    return {
        "ticker":           "TEST",
        "date":             date_str,
        "eps_actual":       eps_actual,
        "eps_estimate":     eps_estimate,
        "revenue_actual":   revenue_actual,
        "revenue_estimate": revenue_estimate,
        "surprise_pct":     None,
        "report_available": eps_actual is not None,
        "fiscal_year":      fiscal_year,
        "fiscal_period":    fiscal_period,
        "source":           "fmp_live",
    }


# ── import the pure function under test ───────────────────────────────────────

from services.earnings_monitor_service import (
    _has_complete_results_for_target,
    _merge_results_payload,
    _select_matching_fmp_result,
)


# ══════════════════════════════════════════════════════════════════════════════
# Tests 1–8 : original contract
# ══════════════════════════════════════════════════════════════════════════════

class TestOriginalContract:

    def test_1_q2_target_rejects_q1_labeled_result(self):
        """A Q2 target must reject a Q1-labeled FMP row even when it has actuals."""
        q1_row = _rec("2026-05-05", eps_actual=-0.04, revenue_actual=8_598_000,
                      fiscal_year=2026, fiscal_period="Q1")
        assert _select_matching_fmp_result(
            [q1_row], expected_date="2026-07-27",
            fiscal_year=2026, fiscal_period="Q2",
        ) is None

    def test_2_matching_q2_labeled_row_is_accepted(self):
        """A Q2-labeled row that matches the target must be returned."""
        q2_row = _rec("2026-07-27", eps_actual=0.05, revenue_actual=12_000_000,
                      fiscal_year=2026, fiscal_period="Q2")
        assert _select_matching_fmp_result(
            [q2_row], expected_date="2026-07-27",
            fiscal_year=2026, fiscal_period="Q2",
        ) is q2_row

    def test_3_eps_only_partial_result_accepted(self):
        """Correctly-matched row with EPS actual but no revenue actual is accepted."""
        row = _rec("2026-07-27", eps_actual=-0.02, revenue_actual=None,
                   fiscal_year=2026, fiscal_period="Q2")
        assert _select_matching_fmp_result(
            [row], expected_date="2026-07-27",
            fiscal_year=2026, fiscal_period="Q2",
        ) is row

    def test_4_revenue_only_partial_result_accepted(self):
        """Correctly-matched row with revenue actual but no EPS actual is accepted."""
        row = _rec("2026-07-27", eps_actual=None, revenue_actual=15_000_000,
                   fiscal_year=2026, fiscal_period="Q2")
        assert _select_matching_fmp_result(
            [row], expected_date="2026-07-27",
            fiscal_year=2026, fiscal_period="Q2",
        ) is row

    def test_5_stale_only_returns_none(self):
        """When only stale rows exist the function must return None."""
        q1_row = _rec("2026-05-05", eps_actual=-0.04, fiscal_year=2026, fiscal_period="Q1")
        q4_row = _rec("2026-02-24", eps_actual=-0.05, fiscal_year=2025, fiscal_period="Q4")
        assert _select_matching_fmp_result(
            [q1_row, q4_row], expected_date="2026-07-27",
            fiscal_year=2026, fiscal_period="Q2",
        ) is None

    def test_6_fiscal_labels_take_priority_over_provider_order(self):
        """Q1 row listed first must not block the Q2 exact fiscal match."""
        q1_row = _rec("2026-05-05", eps_actual=-0.04, fiscal_year=2026, fiscal_period="Q1")
        q2_row = _rec("2026-07-27", eps_actual=0.05,  fiscal_year=2026, fiscal_period="Q2")
        assert _select_matching_fmp_result(
            [q1_row, q2_row], expected_date="2026-07-27",
            fiscal_year=2026, fiscal_period="Q2",
        ) is q2_row

    def test_7_date_proximity_1_day_accepted_when_no_fiscal_labels(self):
        """Without fiscal labels, a result 1 day from expected_date is accepted."""
        row = _rec("2026-07-28", eps_actual=0.10, revenue_actual=20_000_000)
        assert _select_matching_fmp_result(
            [row], expected_date="2026-07-27",
            fiscal_year=None, fiscal_period=None,
        ) is row

    def test_8_date_proximity_60_days_rejected_when_no_fiscal_labels(self):
        """Without fiscal labels, a result 60+ days from expected_date is rejected."""
        old_row = _rec("2026-05-05", eps_actual=-0.04, revenue_actual=8_000_000)
        assert _select_matching_fmp_result(
            [old_row], expected_date="2026-07-27",
            fiscal_year=None, fiscal_period=None,
        ) is None


# ══════════════════════════════════════════════════════════════════════════════
# Tests 9–12 : two-pass algorithm new contract
# ══════════════════════════════════════════════════════════════════════════════

class TestTwoPassAlgorithm:

    def test_9_unlabeled_nearby_row_does_not_beat_exact_fiscal_match(self):
        """
        An earlier unlabeled row within seven days must NOT beat a later exact
        fiscal match.  Pass 1 must win over Pass 2 whenever labeled candidates
        exist.
        """
        # Unlabeled row: 3 days before expected_date — would win under date-proximity
        unlabeled = _rec("2026-07-24", eps_actual=0.03)   # no fiscal labels
        # Exact fiscal match (further in time, but labeled correctly)
        exact_q2  = _rec("2026-07-27", eps_actual=0.05,
                         fiscal_year=2026, fiscal_period="Q2")
        result = _select_matching_fmp_result(
            [unlabeled, exact_q2],           # unlabeled listed first
            expected_date="2026-07-27",
            fiscal_year=2026, fiscal_period="Q2",
        )
        assert result is exact_q2, (
            "Exact fiscal match must win over an unlabeled row, "
            "regardless of provider ordering"
        )

    def test_10_nearest_date_selected_when_two_unlabeled_rows_in_wrong_order(self):
        """
        Two unlabeled rows within seven days, presented in reverse / wrong
        provider order: the one with the smaller date-distance must be selected.
        """
        closer_row  = _rec("2026-07-26", eps_actual=0.04)   # 1 day away
        farther_row = _rec("2026-07-22", eps_actual=0.02)   # 5 days away

        # farther listed first — ordering must not determine the result
        result = _select_matching_fmp_result(
            [farther_row, closer_row],
            expected_date="2026-07-27",
            fiscal_year=None, fiscal_period=None,
        )
        assert result is closer_row, (
            "Nearest date-distance candidate must win regardless of provider order"
        )

    def test_11_exact_date_beats_five_day_candidate_regardless_of_order(self):
        """
        Exact-date candidate (distance=0) must be selected over a 5-day-away
        candidate regardless of which row appears first in the list.
        """
        exact_row = _rec("2026-07-27", eps_actual=0.07)   # 0 days away
        near_row  = _rec("2026-07-22", eps_actual=0.04)   # 5 days away

        # Test both orderings
        for records in ([near_row, exact_row], [exact_row, near_row]):
            result = _select_matching_fmp_result(
                records,
                expected_date="2026-07-27",
                fiscal_year=None, fiscal_period=None,
            )
            assert result is exact_row, (
                f"Exact-date row must win; records order was "
                f"{'near-first' if records[0] is near_row else 'exact-first'}"
            )

    def test_12_labeled_candidates_exist_but_none_match_returns_none_no_fallback(self):
        """
        When labeled candidates exist but none match the target quarter, return
        None.  Must NOT fall back to date proximity even if an unlabeled row is
        within seven days.
        """
        wrong_q_labeled = _rec("2026-05-05", eps_actual=-0.04,
                               fiscal_year=2026, fiscal_period="Q1")
        nearby_unlabeled = _rec("2026-07-25", eps_actual=0.06)  # within 7 days

        result = _select_matching_fmp_result(
            [wrong_q_labeled, nearby_unlabeled],
            expected_date="2026-07-27",
            fiscal_year=2026, fiscal_period="Q2",
        )
        assert result is None, (
            "Labeled candidates present but wrong quarter → must return None; "
            "must NOT fall back to the nearby unlabeled row"
        )


# ══════════════════════════════════════════════════════════════════════════════
# Tests 13–15 : robustness
# ══════════════════════════════════════════════════════════════════════════════

class TestRobustness:

    def test_13_malformed_dates_ignored_without_raising(self):
        """Rows with non-parseable date strings are skipped; valid row is returned."""
        bad_date_row  = _rec("not-a-date", eps_actual=0.05)
        good_date_row = _rec("2026-07-27", eps_actual=0.04)

        result = _select_matching_fmp_result(
            [bad_date_row, good_date_row],
            expected_date="2026-07-27",
            fiscal_year=None, fiscal_period=None,
        )
        assert result is good_date_row, (
            "Malformed date row must be skipped; valid row must still be selected"
        )

    def test_14_malformed_fiscal_labels_do_not_crash(self):
        """Records with non-parseable fiscal labels are skipped gracefully."""
        bad_label_row = _rec("2026-07-27", eps_actual=0.05,
                             fiscal_year="not-an-int", fiscal_period="Q2")
        # Should skip bad_label_row (fails int() cast) and find no labeled candidates
        # then fall through to date-proximity and accept the row based on date alone
        result = _select_matching_fmp_result(
            [bad_label_row],
            expected_date="2026-07-27",
            fiscal_year=2026, fiscal_period="Q2",
        )
        # bad label → skipped from labeled set → zero labeled candidates → Pass 2
        # date-proximity: 0 days → accepted
        assert result is bad_label_row, (
            "Malformed fiscal label must be skipped, not crash; "
            "row should still qualify via date-proximity fallback"
        )

    def test_15_no_validation_context_returns_none(self):
        """No fiscal labels AND no expected_date → None; never return first actuals row."""
        row = _rec("2026-07-27", eps_actual=0.10, revenue_actual=20_000_000)
        assert _select_matching_fmp_result(
            [row],
            expected_date=None,
            fiscal_year=None,
            fiscal_period=None,
        ) is None


# ══════════════════════════════════════════════════════════════════════════════
# Same-day freshness regressions
# ══════════════════════════════════════════════════════════════════════════════

class TestSameDayFreshness:

    def test_glw_current_event_replaces_stale_prior_quarter(self):
        """A later GLW poll selects July, not the complete April row."""
        prior_q1 = _rec("2026-04-28", eps_actual=0.70, revenue_actual=4_345_000_000)
        current_q2 = _rec("2026-07-28", eps_actual=0.78, revenue_actual=4_738_000_000)

        result = _select_matching_fmp_result(
            [prior_q1, current_q2], "2026-07-28", 2026, "Q2"
        )
        assert result is current_q2

        refreshed = _merge_results_payload(
            {"date": "2026-04-28", "eps_actual": 0.70, "revenue_actual": 4_345_000_000},
            result,
        )
        assert refreshed["date"] == "2026-07-28"
        assert refreshed["eps_actual"] == 0.78
        assert refreshed["revenue_actual"] == 4_738_000_000

    def test_partial_publication_merges_without_null_erasure(self):
        """Revenue-first then EPS-only publication keeps both actual values."""
        revenue_first = _merge_results_payload(
            None,
            _rec("2026-07-28", revenue_actual=4_738_000_000, revenue_estimate=4_630_209_000),
        )
        eps_later = _merge_results_payload(
            revenue_first,
            _rec("2026-07-28", eps_actual=0.78, eps_estimate=0.755, revenue_actual=None),
        )
        assert eps_later["revenue_actual"] == 4_738_000_000
        assert eps_later["eps_actual"] == 0.78

    def test_stale_complete_row_cannot_win_over_current_actual(self):
        """Provider ordering never permits the complete Q1 row to win."""
        old_complete = _rec("2026-04-28", eps_actual=0.70, revenue_actual=4_345_000_000)
        current = _rec("2026-07-28", revenue_actual=4_738_000_000)
        assert _select_matching_fmp_result(
            [old_complete, current], "2026-07-28", 2026, "Q2"
        ) is current

    def test_completed_target_query_retries_only_invalid_same_day_results(self):
        """get_due_targets includes same-day complete targets via a simple date window.

        The correlated NOT EXISTS (per-row JSON scan) has been removed.  The
        scheduler's _process_target/_has_complete_results_for_target performs the
        integrity check at application level after fetching the candidate list.
        This keeps the DB query fast and avoids index-defeating JSON predicates.
        """
        from data import earnings_monitor_store as store
        cur = MagicMock()
        cur.fetchall.return_value = []
        conn = MagicMock()
        conn.cursor.return_value = cur
        original_get, original_put = store._get_conn, store._put_conn
        store._get_conn, store._put_conn = lambda: conn, lambda _conn: None
        try:
            store.get_due_targets()
        finally:
            store._get_conn, store._put_conn = original_get, original_put
        sql = cur.execute.call_args[0][0]
        # Same-day complete targets are included via a simple date-window filter
        assert "status <> 'complete'" in sql
        assert "expected_date >= CURRENT_DATE - INTERVAL '1 day'" in sql
        assert "expected_date <= CURRENT_DATE" in sql
        # No per-row correlated subquery: integrity check is at application level
        assert "NOT EXISTS" not in sql
        assert "results_payload ->> 'eps_actual'" not in sql

    def test_amkr_complete_payload_serializes_unchanged(self):
        """A complete AMKR-style event retains its values when the next poll is partial."""
        complete = {
            "date": "2026-07-27", "eps_actual": 0.70, "eps_estimate": 0.47,
            "revenue_actual": 1_897_965_000, "revenue_estimate": 1_814_788_000,
        }
        unchanged = _merge_results_payload(complete, {"date": "2026-07-27"})
        for key, value in complete.items():
            assert unchanged[key] == value

    def test_passed_release_time_does_not_invent_actuals(self):
        """An estimate-only current row remains result-free; no prior value is copied."""
        estimate_only = _rec("2026-07-28", eps_estimate=0.755, revenue_estimate=4_630_209_000)
        assert _select_matching_fmp_result(
            [estimate_only], "2026-07-28", 2026, "Q2"
        ) is None

    def test_stale_completed_payload_keeps_existing_polling_path_open(self):
        """A Q2 target cannot finalize just because its stored payload is Q1."""
        assert not _has_complete_results_for_target(
            {"date": "2026-04-28", "eps_actual": 0.70, "revenue_actual": 4_345_000_000},
            "2026-07-28",
        )
        assert _has_complete_results_for_target(
            {"date": "2026-07-28", "eps_actual": 0.78, "revenue_actual": 4_738_000_000},
            "2026-07-28",
        )


# ══════════════════════════════════════════════════════════════════════════════
# Tests 16–19 : Recent date-filter + no-dedup contract
# ══════════════════════════════════════════════════════════════════════════════

class TestRecentDateFilter:

    def _call_store(self, symbols, since_date, today, mock_conn):
        from data import earnings_monitor_store as _store
        orig_get = _store._get_conn
        orig_put = _store._put_conn
        _store._get_conn = lambda: mock_conn
        _store._put_conn = lambda c: None
        try:
            return _store.get_recent_complete_events_for_symbols(
                symbols, since_date, today
            )
        finally:
            _store._get_conn = orig_get
            _store._put_conn = orig_put

    def _mock_conn(self):
        cur = MagicMock()
        cur.fetchall.return_value = []
        conn = MagicMock()
        conn.cursor.return_value = cur
        return conn, cur

    def test_16_future_event_excluded_via_today_upper_bound(self):
        """SQL must contain expected_date <= %s and today must appear in params."""
        today      = date.today().isoformat()
        since_date = (date.today() - timedelta(days=30)).isoformat()
        conn, cur  = self._mock_conn()

        self._call_store(["FUTR"], since_date, today, conn)

        args      = cur.execute.call_args[0]
        sql_text  = args[0]
        sql_params = list(args[1])

        assert "expected_date <= %s" in sql_text, (
            "SQL must contain 'expected_date <= %s' upper bound"
        )
        assert today in sql_params, (
            f"today={today!r} must appear in SQL params; got {sql_params}"
        )
        today_idx = sql_params.index(today)
        since_idx = sql_params.index(since_date)
        assert today_idx > since_idx, "today must be passed after since_date in params"

    def test_17_today_event_passes_upper_bound(self):
        """today must be in params so that expected_date = today satisfies <= today."""
        today      = date.today().isoformat()
        since_date = (date.today() - timedelta(days=30)).isoformat()
        conn, cur  = self._mock_conn()

        self._call_store(["TDYQ"], since_date, today, conn)

        sql_params = list(cur.execute.call_args[0][1])
        assert today in sql_params and since_date in sql_params

    def test_18_today_ticker_can_appear_in_both_upcoming_and_recent(self):
        """
        Router must not remove a ticker from Upcoming because it appears in Recent.
        Forbidden dedup patterns must be absent from watchlist_earnings_endpoint.
        """
        router_path = os.path.join(
            os.path.dirname(__file__), "..", "services", "watchlist_router.py"
        )
        with open(router_path, encoding="utf-8") as fh:
            source = fh.read()

        func_start = source.find("async def watchlist_earnings_endpoint(")
        assert func_start != -1, "Could not locate watchlist_earnings_endpoint"
        func_source = source[func_start:func_start + 6000]

        forbidden = [
            "_recent_syms = {r[",
            "e for e in normalised if e[\"ticker\"] not in _recent_syms",
            "if e[\"ticker\"] not in _recent_syms",
        ]
        for pat in forbidden:
            assert pat not in func_source, (
                f"Forbidden global dedup pattern in watchlist_earnings_endpoint: {pat!r}"
            )

    def test_19_no_global_upcoming_recent_dedup_variable(self):
        """_recent_syms must not exist inside watchlist_earnings_endpoint."""
        router_path = os.path.join(
            os.path.dirname(__file__), "..", "services", "watchlist_router.py"
        )
        with open(router_path, encoding="utf-8") as fh:
            source = fh.read()

        func_start = source.find("async def watchlist_earnings_endpoint(")
        assert func_start != -1
        func_end   = source.find("\nasync def ", func_start + 1)
        func_source = source[func_start:func_end if func_end != -1 else func_start + 10000]

        assert "_recent_syms" not in func_source, (
            "_recent_syms must not exist in watchlist_earnings_endpoint "
            "(marker for the removed global dedup block)"
        )
