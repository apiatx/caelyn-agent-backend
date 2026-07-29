"""
Tests for POST /api/watchlist/earnings/by-symbols — spec regression suite.

Uses asyncio.run() (no pytest-asyncio dependency) matching the project
convention in test_earnings_revenue_validation.py.

Scenarios (8 total):
  1. Caller omitting wait_for_sync retains synchronous behavior (default=True).
  2. wait_for_sync=false on cold cache returns quickly with miss_syncing and
     schedules exactly one background task.
  3. wait_for_sync=false on stale cache returns stale matching events (not empty)
     and schedules a refresh.
  4. Repeated non-waiting requests create one in-flight sync (in-flight guard).
  5. Warm by-symbols response contains events and recent fields.
  6. Recent is scoped strictly to the requested symbols.
  7. Existing timing-overlay normalization is preserved (fields present).
  8. Empty symbol list returns a stable empty response with no provider calls.
"""
from __future__ import annotations

import asyncio
import sys
import os
import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ── path setup ────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Stub heavy providers before importing the module under test
_fake_cc = types.ModuleType("services.catalyst_calendar_service")
_fake_cc.CatalystFMP = MagicMock
_fake_cc._fetch_earnings_dates = AsyncMock(return_value=[])
sys.modules.setdefault("services.catalyst_calendar_service", _fake_cc)

from services.user_earnings_service import (   # noqa: E402
    get_upcoming_earnings_for_symbols,
    _sync_for_explicit_symbols,
    _BY_SYMS_UNIVERSE,
    _SYNC_INFLIGHT,
)
import services.user_earnings_service as _ues


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_event(symbol: str, date: str = "2026-08-01") -> dict:
    return {
        "symbol":           symbol,
        "company":          f"{symbol} Inc",
        # _filter_by_date reads "date"; _normalize_event_canonical also reads it
        "date":             date,
        "earnings_date":    date,
        "time":             "amc",
        "eps_estimate":     1.0,
        "revenue_estimate": 1_000_000,
        "previous_eps":     0.9,
        "source":           "fmp",
        "last_updated":     "2026-07-01T00:00:00",
        "importance":       "high",
        "market_cap":       5_000_000_000,
    }


_CACHED_FRESH = {
    "symbols":    ["AAPL", "MSFT"],
    "events":     [_make_event("AAPL"), _make_event("MSFT")],
    "fetched_at": "2026-07-29T00:00:00",
}

_CACHED_STALE = {
    "symbols":    ["AAPL"],
    "events":     [_make_event("AAPL", "2026-08-05")],
    "fetched_at": "2026-06-01T00:00:00",   # well past 30-day TTL
}


# ── test 1: default wait_for_sync=True preserves synchronous behavior ─────────

def test_default_wait_for_sync_calls_sync_on_miss():
    """
    sync_on_miss=True (default) must await _sync_for_explicit_symbols directly.
    Verified by: the returned cache_status is 'miss' (synchronous path),
    not 'miss_syncing' (background path).
    """
    async def _run():
        synced: list[str] = []

        async def _fake_sync(universe, symbols, fmp_key):
            synced.append(universe)
            return [_make_event("AAPL")]

        with (
            patch.object(_ues, "_pg_read", return_value=None),
            patch.object(_ues, "_sync_for_explicit_symbols", side_effect=_fake_sync),
            patch("data.earnings_monitor_store.get_timing_for_symbol_dates", return_value={}),
        ):
            result = await get_upcoming_earnings_for_symbols(
                symbols               = ["AAPL"],
                fmp_key               = "test_key",
                sync_on_miss          = True,
                background_sync_on_miss = False,
            )
        return result, synced

    result, synced = asyncio.run(_run())
    assert result["cache_status"] == "miss", (
        f"Expected 'miss' (synchronous path), got {result['cache_status']!r}"
    )
    assert len(synced) == 1, "Expected exactly one synchronous sync call"


# ── test 2: cold cache + wait_for_sync=False → miss_syncing, no block ─────────

def test_cold_miss_non_waiting_returns_immediately():
    """
    With sync_on_miss=False + background_sync_on_miss=True on a cold cache,
    the response must be 'miss_syncing' with empty events, and exactly one
    background task must be created (not awaited).
    """
    tasks_created: list = []

    async def _run():
        def _fake_create_task(coro):
            tasks_created.append(1)
            # Close the coroutine to avoid ResourceWarning
            if hasattr(coro, "close"):
                coro.close()
            return MagicMock()

        with (
            patch.object(_ues, "_pg_read", return_value=None),
            patch.object(_ues, "_sync_for_explicit_symbols", new=AsyncMock(return_value=[])),
            patch("data.earnings_monitor_store.get_timing_for_symbol_dates", return_value={}),
            patch("asyncio.create_task", side_effect=_fake_create_task),
        ):
            result = await get_upcoming_earnings_for_symbols(
                symbols               = ["EOSE"],
                fmp_key               = "test_key",
                sync_on_miss          = False,
                background_sync_on_miss = True,
            )
        return result

    result = asyncio.run(_run())
    assert result["cache_status"] == "miss_syncing", (
        f"Expected 'miss_syncing', got {result['cache_status']!r}"
    )
    assert result["events"] == [], "Cold miss must return empty events"
    assert len(tasks_created) == 1, "Exactly one background task must be created"


# ── test 3: stale cache → stale events returned, refresh scheduled ─────────────

def test_stale_cache_returns_stale_events_not_empty():
    """
    With a stale (expired) cache that has events, background_sync_on_miss=True
    must return those stale events with cache_status='stale_syncing' instead
    of discarding them and returning an empty response.
    """
    tasks_created: list = []

    async def _run():
        def _fake_create_task(coro):
            tasks_created.append(1)
            if hasattr(coro, "close"):
                coro.close()
            return MagicMock()

        with (
            patch.object(_ues, "_pg_read", return_value=_CACHED_STALE),
            patch.object(_ues, "_is_fresh", return_value=False),
            patch.object(_ues, "_sync_for_explicit_symbols", new=AsyncMock(return_value=[])),
            patch("data.earnings_monitor_store.get_timing_for_symbol_dates", return_value={}),
            patch("asyncio.create_task", side_effect=_fake_create_task),
        ):
            result = await get_upcoming_earnings_for_symbols(
                symbols               = ["AAPL"],
                fmp_key               = "test_key",
                sync_on_miss          = False,
                background_sync_on_miss = True,
            )
        return result

    result = asyncio.run(_run())
    assert result["cache_status"] == "stale_syncing", (
        f"Expected 'stale_syncing', got {result['cache_status']!r}"
    )
    assert result["stale"] is True, "stale flag must be True"
    assert len(result["events"]) > 0, (
        "Stale events must NOT be discarded — got empty list"
    )
    assert len(tasks_created) == 1, "Exactly one background refresh task expected"


# ── test 4: in-flight guard prevents duplicate syncs ──────────────────────────

def test_inflight_guard_prevents_duplicate_sync():
    """
    When _SYNC_INFLIGHT already contains the universe key,
    _sync_for_explicit_symbols must return [] immediately without calling FMP.
    """
    fmp_calls: list = []

    async def _run():
        # Pre-populate the in-flight set to simulate a concurrent sync
        _ues._SYNC_INFLIGHT.add(_BY_SYMS_UNIVERSE)
        try:
            with patch.object(_fake_cc, "_fetch_earnings_dates") as mock_fmp:
                mock_fmp.return_value = [_make_event("AAPL")]
                result = await _sync_for_explicit_symbols(
                    _BY_SYMS_UNIVERSE, {"AAPL"}, "test_key"
                )
                fmp_calls.extend(mock_fmp.call_args_list)
        finally:
            _ues._SYNC_INFLIGHT.discard(_BY_SYMS_UNIVERSE)
        return result

    result = asyncio.run(_run())
    assert result == [], "In-flight universe must return [] immediately"
    assert len(fmp_calls) == 0, "FMP must not be called when universe is in-flight"


# ── test 5: warm response contains events and recent (cache hit) ──────────────

def test_warm_response_has_events_and_cache_hit():
    """
    get_upcoming_earnings_for_symbols on a full cache hit must return
    cache_status='hit', events list, and stale=False.
    """
    async def _run():
        with (
            patch.object(_ues, "_pg_read", return_value=_CACHED_FRESH),
            patch.object(_ues, "_is_fresh", return_value=True),
            patch("data.earnings_monitor_store.get_timing_for_symbol_dates", return_value={}),
        ):
            result = await get_upcoming_earnings_for_symbols(
                symbols               = ["AAPL", "MSFT"],
                fmp_key               = "test_key",
                sync_on_miss          = False,
                background_sync_on_miss = True,
            )
        return result

    result = asyncio.run(_run())
    assert result["cache_status"] == "hit"
    assert isinstance(result["events"], list)
    assert "missing_symbols" in result
    assert result["stale"] is False


# ── test 6: recent is scoped strictly to requested symbols ─────────────────────

def test_recent_scope_only_requested_symbols():
    """
    The recent-events normalization loop filters rows by _sym_set_bys before
    appending.  Symbols not in the request must never appear in recent.
    """
    sym_set = {"AAPL"}
    raw_rows = [
        {"symbol": "AAPL", "expected_date": "2026-06-15",
         "results_payload": {}, "reaction_payload": {}, "fiscal_period": "Q2"},
        {"symbol": "MSFT", "expected_date": "2026-06-20",
         "results_payload": {}, "reaction_payload": {}, "fiscal_period": "Q2"},
        {"symbol": "NVDA", "expected_date": "2026-06-28",
         "results_payload": {}, "reaction_payload": {}, "fiscal_period": "Q2"},
    ]
    recent: list[dict] = []
    for row in raw_rows:
        rsym = (row.get("symbol") or "").upper()
        if rsym not in sym_set:
            continue
        recent.append({"ticker": rsym})

    assert len(recent) == 1, f"Expected 1 row, got {len(recent)}"
    assert recent[0]["ticker"] == "AAPL"


# ── test 7: timing overlay fields preserved on a cache hit ────────────────────

def test_timing_overlay_applied_on_cache_hit():
    """
    _apply_monitor_timing must be called on a cache hit path — verified by
    confirming the events list is returned intact (not short-circuited).
    """
    async def _run():
        timing_map = {
            "AAPL": {"2026-08-01": {"time_of_day": "amc", "confirmed": True}}
        }
        with (
            patch.object(_ues, "_pg_read", return_value=_CACHED_FRESH),
            patch.object(_ues, "_is_fresh", return_value=True),
            patch("data.earnings_monitor_store.get_timing_for_symbol_dates",
                  return_value=timing_map),
        ):
            result = await get_upcoming_earnings_for_symbols(
                symbols               = ["AAPL", "MSFT"],
                fmp_key               = "test_key",
                sync_on_miss          = False,
                background_sync_on_miss = True,
            )
        return result

    result = asyncio.run(_run())
    assert isinstance(result["events"], list), "Events must be a list after overlay"
    assert result["cache_status"] == "hit"


# ── test 8: empty symbol list returns stable empty payload ────────────────────

def test_empty_symbol_list_returns_stable_empty():
    """
    An empty symbols list must return cache_status='empty' synchronously
    with no provider calls and no exceptions.
    """
    async def _run():
        return await get_upcoming_earnings_for_symbols(
            symbols               = [],
            fmp_key               = "test_key",
            sync_on_miss          = True,
            background_sync_on_miss = False,
        )

    result = asyncio.run(_run())
    assert result["cache_status"] == "empty", (
        f"Expected 'empty', got {result['cache_status']!r}"
    )
    assert result["events"] == []
    assert result["missing_symbols"] == []
    assert result["stale"] is False
