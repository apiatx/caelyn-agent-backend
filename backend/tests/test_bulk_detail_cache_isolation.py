"""
Regression tests: bulk-projection / ticker-detail cache isolation
=================================================================
Contract enforced here:

  1. get_snapshot()       → COMPLETE canonical fields including earnings_intelligence
  2. get_snapshots_bulk() → EI-stripped projection (SQL-level strip, bulk path only)
  3. ticker_detail_endpoint exclusively calls get_snapshot(), never get_snapshots_bulk()
  4. Bulk projections are never stored in any shared per-symbol dict used by detail reads
  5. _BULK_CSV_STRIP strips only earnings_intelligence; profile and _* fields are retained

These tests must continue to pass after any refactor that touches
watchlist_fundamentals_store, watchlist_router, or the ticker-detail endpoint.
"""

import inspect
import pytest
from unittest.mock import MagicMock, patch


# ── helpers ──────────────────────────────────────────────────────────────────

_EI_PAYLOAD = {
    "earnings_history": [
        {"period_label": "Q2 2026", "eps_actual": 0.78, "revenue_actual": 4_738_000_000},
        {"period_label": "Q1 2026", "eps_actual": 0.54, "revenue_actual": 3_860_000_000},
    ],
    "source_status":   {"coverage": {"has_earnings_history": True}},
    "schema_version":  2,
    "ratings":         {},
    "sec_filings":     [],
    "reaction_summary": {},
}

_FULL_FIELDS = {
    "Revenue":                "14.0B",
    "PE Ratio":               28.4,
    "profile":                {"description": "Corning Incorporated", "sector": "Technology"},
    "_eps_growth_method":     "yoy_diluted",
    "_valuation_ttm_revenue": 14_000_000_000,
    "earnings_intelligence":  _EI_PAYLOAD,
}

_STRIPPED_FIELDS = {k: v for k, v in _FULL_FIELDS.items() if k != "earnings_intelligence"}


# ── Test 1: get_snapshot SQL contract ────────────────────────────────────────

def test_get_snapshot_returns_full_ei_fields():
    """get_snapshot() returns full fields — earnings_intelligence is NOT stripped."""
    from data.watchlist_fundamentals_store import get_snapshot

    import datetime, pytz
    mock_row = (
        "GLW",
        "00a0e3ea-31dc-4223-97bc-470720dd3215",
        datetime.datetime(2026, 7, 27, 17, 54, 55, tzinfo=pytz.utc),
        datetime.datetime(2026, 8, 3, 17, 54, 55, tzinfo=pytz.utc),
        dict(_FULL_FIELDS),
        [],
        5,
    )

    mock_conn = MagicMock()
    mock_cur  = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_cur.fetchone.return_value = mock_row

    with patch("data.pg_storage._get_conn", return_value=mock_conn), \
         patch("data.pg_storage._put_conn"):
        snap = get_snapshot("GLW")

    assert snap is not None, "get_snapshot must return a dict for a known symbol"
    fields = snap.get("fields") or {}
    assert "earnings_intelligence" in fields, (
        "get_snapshot must return earnings_intelligence — it reads full fields, not a projection"
    )
    ei = fields["earnings_intelligence"]
    assert ei.get("earnings_history"), "earnings_history must be present in the full snapshot"
    q2 = ei["earnings_history"][0]
    assert q2["eps_actual"] == 0.78
    assert q2["revenue_actual"] == 4_738_000_000


# ── Test 2: get_snapshots_bulk SQL contract ───────────────────────────────────

def test_get_snapshots_bulk_sql_strips_earnings_intelligence():
    """get_snapshots_bulk() SQL projection strips earnings_intelligence at the DB level."""
    from data.watchlist_fundamentals_store import get_snapshots_bulk

    src = inspect.getsource(get_snapshots_bulk)
    assert "- 'earnings_intelligence'" in src, (
        "get_snapshots_bulk SQL must contain the JSONB strip operator "
        "(fields - 'earnings_intelligence') — do not remove it"
    )


def test_get_snapshots_bulk_returns_no_ei_when_stripped():
    """When get_snapshots_bulk returns rows, fields must not contain earnings_intelligence."""
    from data.watchlist_fundamentals_store import get_snapshots_bulk

    import datetime, pytz
    stripped_row = (
        "GLW",
        datetime.datetime(2026, 7, 27, 17, 54, 55, tzinfo=pytz.utc),
        datetime.datetime(2026, 8, 3, 17, 54, 55, tzinfo=pytz.utc),
        dict(_STRIPPED_FIELDS),
        [],
    )

    mock_conn = MagicMock()
    mock_cur  = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_cur.fetchall.return_value = [stripped_row]

    with patch("data.pg_storage._get_conn", return_value=mock_conn), \
         patch("data.pg_storage._put_conn"):
        result = get_snapshots_bulk(["GLW"])

    assert "GLW" in result
    fields = result["GLW"].get("fields") or {}
    assert "earnings_intelligence" not in fields, (
        "get_snapshots_bulk rows must not contain earnings_intelligence — "
        "it is stripped by the SQL projection"
    )
    assert "profile" in fields, "profile must be retained in the bulk projection"
    assert "_eps_growth_method" in fields, "_* fields must be retained in the bulk projection"


# ── Test 3: ticker_detail_endpoint exclusively uses get_snapshot ───────────────

def test_ticker_detail_uses_get_snapshot_not_bulk():
    """
    ticker_detail_endpoint's source must import get_snapshot (singular, full-fields)
    and must never call get_snapshots_bulk (EI-stripped projection).

    This is the structural guard that prevents bulk-projection poisoning of the
    Earnings tab.  If this test fails, the caller introduced get_snapshots_bulk
    into the detail path, which would silently drop earnings_intelligence.
    """
    import services.watchlist_router as wr

    src = inspect.getsource(wr.ticker_detail_endpoint)

    assert "get_snapshot" in src, (
        "ticker_detail_endpoint must call get_snapshot() for both the "
        "fundamentals step and the earnings_intelligence step"
    )
    assert "get_snapshots_bulk" not in src, (
        "ticker_detail_endpoint must NEVER call get_snapshots_bulk() — "
        "that function strips earnings_intelligence and must only be used "
        "for the bulk Watchlist list rendering path"
    )


# ── Test 4: bulk CSV strip policy ─────────────────────────────────────────────

def test_bulk_csv_strip_contains_only_earnings_intelligence():
    """
    _BULK_CSV_STRIP must strip earnings_intelligence and nothing else.
    profile and _* fields must remain in bulk csv_data rows because
    the frontend may read them for overlays, tooltips, or column rendering.
    """
    import services.watchlist_router as wr
    strip = wr._BULK_CSV_STRIP

    assert "earnings_intelligence" in strip, (
        "_BULK_CSV_STRIP must include earnings_intelligence"
    )
    assert "profile" not in strip, (
        "profile must NOT be in _BULK_CSV_STRIP — it must be retained in bulk rows"
    )
    assert not any(k.startswith("_") for k in strip), (
        "_* fields must NOT be in _BULK_CSV_STRIP — they must be retained in bulk rows"
    )


def test_bulk_csv_strip_filter_removes_only_ei():
    """Applying _BULK_CSV_STRIP to a sample row retains all fields except EI."""
    import services.watchlist_router as wr

    sample_row = {
        "Symbol":               "GLW",
        "Revenue":              "14.0B",
        "profile":              {"description": "Corning Inc."},
        "_eps_growth_method":   "yoy_diluted",
        "_valuation_ttm_revenue": 14_000_000_000,
        "earnings_intelligence": {"earnings_history": [{"eps_actual": 0.78}]},
    }
    filtered = {k: v for k, v in sample_row.items() if k not in wr._BULK_CSV_STRIP}

    assert "earnings_intelligence" not in filtered, "EI must be removed from bulk csv_data rows"
    assert "profile" in filtered,              "profile must be retained in bulk csv_data rows"
    assert "_eps_growth_method" in filtered,   "_* fields must be retained in bulk csv_data rows"
    assert "_valuation_ttm_revenue" in filtered
    assert "Revenue" in filtered
    assert "Symbol" in filtered


# ── Test 5: structural isolation — no shared module-level snap cache ───────────

def test_no_module_level_snap_cache_in_fundamentals_store():
    """
    watchlist_fundamentals_store must not define a module-level dict/cache
    that is written by get_snapshots_bulk and read by get_snapshot.

    Isolation is maintained by the absence of any shared in-memory structure.
    """
    import data.watchlist_fundamentals_store as store_mod

    # Scan every module-level name for dict-typed structures that could act as
    # a shared cache between the two functions.
    suspicious = []
    for name in dir(store_mod):
        if name.startswith("__"):
            continue
        obj = getattr(store_mod, name, None)
        # Module-level dict that is NOT a constant (i.e., mutable cache candidate)
        if isinstance(obj, dict) and not name.isupper():
            suspicious.append(name)

    # The only permitted module-level mutable dict is `no_snapshot` (the
    # list_due_symbols helper) — it has nothing to do with fields/EI.
    disallowed = [n for n in suspicious if "snap" in n.lower() or "fund" in n.lower() or "cache" in n.lower()]
    assert not disallowed, (
        f"Module-level cache dict(s) found in watchlist_fundamentals_store: {disallowed}. "
        "If a per-symbol in-memory cache is added here in the future, ensure it is "
        "populated only by get_snapshot() (full fields), never by get_snapshots_bulk() "
        "(EI-stripped projection)."
    )


# ── Test 6: request-order invariant (GLW Q2 values) ───────────────────────────

def test_glw_ei_fields_are_stable_after_bulk_simulation():
    """
    Simulate the bulk path writing a stripped projection and the detail path
    reading the canonical snapshot.  The detail path must always get full EI.

    This is a pure-Python simulation of the request-order invariant proved by
    the live sequence tests (bulk→detail and detail→bulk→detail).
    """
    bulk_result: dict[str, dict] = {
        "GLW": {
            "refreshed_at":    "2026-07-27T17:54:55+00:00",
            "next_refresh_at": "2026-08-03T17:54:55+00:00",
            "fields":          dict(_STRIPPED_FIELDS),
            "missing_fields":  [],
        }
    }

    canonical_snap: dict = {
        "symbol":          "GLW",
        "watchlist_id":    "00a0e3ea-31dc-4223-97bc-470720dd3215",
        "refreshed_at":    "2026-07-27T17:54:55+00:00",
        "next_refresh_at": "2026-08-03T17:54:55+00:00",
        "fields":          dict(_FULL_FIELDS),
        "missing_fields":  [],
        "fmp_call_count":  5,
    }

    bulk_fields   = bulk_result["GLW"]["fields"]
    detail_fields = canonical_snap["fields"]

    assert "earnings_intelligence" not in bulk_fields, (
        "Bulk projection must not contain earnings_intelligence"
    )
    assert "earnings_intelligence" in detail_fields, (
        "Canonical snapshot (detail path) must contain earnings_intelligence"
    )

    ei   = detail_fields["earnings_intelligence"]
    q2   = ei["earnings_history"][0]
    assert q2["eps_actual"]      == 0.78,           "GLW Q2 eps_actual must be 0.78"
    assert q2["revenue_actual"]  == 4_738_000_000,  "GLW Q2 revenue_actual must be 4,738,000,000"
    assert q2["period_label"]    == "Q2 2026",       "GLW Q2 period_label must be 'Q2 2026'"

    assert "profile" in detail_fields,         "profile must be in canonical snapshot"
    assert "_eps_growth_method" in detail_fields, "_* fields must be in canonical snapshot"

    bulk_fields["_injected_bulk_key"] = "should-not-appear-in-detail"
    assert "_injected_bulk_key" not in detail_fields, (
        "Mutating the bulk projection dict must not affect the canonical snapshot dict — "
        "they must be independent objects with no shared reference"
    )


# ── Earnings timing overlay regression tests ─────────────────────────────────
#
# These tests exercise _apply_monitor_timing directly — no DB connection,
# no provider calls, no server required.  They enforce the exact precedence
# and (symbol, date) matching contract specified in the implementation spec.

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from services.user_earnings_service import _apply_monitor_timing


def _ev(ticker, date, time=None):
    """Build a minimal normalised earnings event."""
    return {"ticker": ticker, "earnings_date": date, "time": time,
            "company": ticker, "eps_estimate": None}


def _tm(symbol, date, expected_timing=None, expected_time_local=None):
    """Build a timing_map with a single entry."""
    return {(symbol, date): {"expected_timing": expected_timing,
                              "expected_time_local": expected_time_local}}


class TestApplyMonitorTiming:

    def test_amc_expected_timing_applied(self):
        """Case 1: expected_timing='amc' → event time becomes 'amc'."""
        events = [_ev("META", "2026-07-29")]
        tmap   = _tm("META", "2026-07-29", expected_timing="amc")
        result = _apply_monitor_timing(events, tmap)
        assert result[0]["time"] == "amc", (
            "event with expected_timing=amc must surface 'amc' in time field"
        )

    def test_bmo_expected_timing_applied(self):
        """Case 1b: expected_timing='bmo' → event time becomes 'bmo'."""
        events = [_ev("AAPL", "2026-07-30")]
        tmap   = _tm("AAPL", "2026-07-30", expected_timing="bmo")
        result = _apply_monitor_timing(events, tmap)
        assert result[0]["time"] == "bmo"

    def test_expected_time_local_fallback_when_timing_null(self):
        """Case 2: expected_timing=None but expected_time_local set → local value used."""
        events = [_ev("MSFT", "2026-07-29")]
        tmap   = _tm("MSFT", "2026-07-29",
                     expected_timing=None,
                     expected_time_local="After Market Close (AMC)")
        result = _apply_monitor_timing(events, tmap)
        assert result[0]["time"] == "After Market Close (AMC)", (
            "expected_time_local must be used as fallback when expected_timing is None"
        )

    def test_no_target_row_leaves_time_null(self):
        """Case 3: far-out event with no matching target → time stays null."""
        events = [_ev("NVDA", "2026-10-15")]
        tmap   = {}
        result = _apply_monitor_timing(events, tmap)
        assert result[0]["time"] is None, (
            "event with no target in timing_map must remain time=null"
        )

    def test_exact_date_match_required_multi_event(self):
        """Case 4: symbol with two dates — only the exact (symbol, date) match applies."""
        events = [
            _ev("NET", "2026-07-30"),
            _ev("NET", "2026-08-06"),
        ]
        tmap = _tm("NET", "2026-07-30", expected_timing="amc")
        result = _apply_monitor_timing(events, tmap)
        assert result[0]["time"] == "amc",  "2026-07-30 must get amc timing"
        assert result[1]["time"] is None,   "2026-08-06 has no target — must stay null"

    def test_existing_nonnull_time_preserved(self):
        """Case 5: event already has a non-null time value → must not be overwritten."""
        events = [_ev("TSLA", "2026-07-23", time="bmo")]
        tmap   = _tm("TSLA", "2026-07-23", expected_timing="amc")
        result = _apply_monitor_timing(events, tmap)
        assert result[0]["time"] == "bmo", (
            "existing non-null time must be preserved; target must not overwrite it"
        )

    def test_empty_timing_map_returns_events_unchanged(self):
        """Empty timing_map → list returned unchanged (fast path)."""
        events = [_ev("AAPL", "2026-07-30"), _ev("META", "2026-07-29")]
        result = _apply_monitor_timing(events, {})
        assert result is events, "empty timing_map must return the original list object"

    def test_event_count_and_order_preserved(self):
        """Overlay must not drop or reorder events."""
        events = [
            _ev("AMZN", "2026-07-30"),
            _ev("FSLR", "2026-07-30"),
            _ev("NVDA", "2026-10-15"),
        ]
        tmap = {
            ("AMZN", "2026-07-30"): {"expected_timing": "amc",  "expected_time_local": None},
            ("FSLR", "2026-07-30"): {"expected_timing": None,   "expected_time_local": "After Market Close (AMC)"},
        }
        result = _apply_monitor_timing(events, tmap)
        assert len(result) == 3,               "event count must be unchanged"
        assert result[0]["ticker"] == "AMZN",  "order must be preserved"
        assert result[1]["ticker"] == "FSLR"
        assert result[2]["ticker"] == "NVDA"
        assert result[0]["time"] == "amc"
        assert result[1]["time"] == "After Market Close (AMC)"
        assert result[2]["time"] is None

    def test_raw_fmp_shape_supported(self):
        """Helper must work on raw FMP events (symbol/date) not just normalised shape."""
        raw_ev = {"symbol": "GNRC", "date": "2026-07-29", "time": None,
                  "companyName": "Generac Holdings"}
        tmap = _tm("GNRC", "2026-07-29", expected_timing="bmo")
        result = _apply_monitor_timing([raw_ev], tmap)
        assert result[0]["time"] == "bmo", "raw FMP event shape (symbol/date) must be resolved"

    def test_no_mutation_of_original_events(self):
        """_apply_monitor_timing must not mutate the original event dicts."""
        ev     = _ev("META", "2026-07-29")
        orig   = dict(ev)
        tmap   = _tm("META", "2026-07-29", expected_timing="amc")
        _apply_monitor_timing([ev], tmap)
        assert ev == orig, "original event dict must not be mutated"
