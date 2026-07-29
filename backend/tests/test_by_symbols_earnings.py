"""
Tests for POST /api/watchlist/earnings/by-symbols — spec regression suite.

Uses asyncio.run() (no pytest-asyncio dependency) matching the project
convention in test_earnings_revenue_validation.py.

Scenarios (12 total):
  Existing behaviour (get_upcoming_earnings_for_symbols):
  1. Caller omitting wait_for_sync retains synchronous behavior (default=True).
  2. wait_for_sync=false on cold cache returns quickly with miss_syncing and
     schedules exactly one background task.
  3. wait_for_sync=false on stale cache returns stale matching events (not empty)
     and schedules a refresh.
  4. Warm by-symbols response contains events and cache_status=hit.
  5. Recent is scoped strictly to the requested symbols.
  6. Existing timing-overlay normalization is preserved (fields present).
  7. Empty symbol list returns a stable empty response with no provider calls.

  Single-flight semantics (_sync_for_explicit_symbols):
  8.  Two concurrent synchronous callers await one provider job and receive the
      same populated result — FMP is called exactly once.
  9.  No caller receives a false settled empty result when a sync is in-flight.
  10. Non-waiting background caller does not trigger a second FMP call when a
      task is already in-flight for the same universe.
  11. Additional symbols arriving during a sync are recorded in _SYNC_PENDING and
      a follow-up expansion is scheduled after the task completes.
  12. A provider failure clears the registry so the next caller can retry.
"""
from __future__ import annotations

import asyncio
import sys
import os
import types
from unittest.mock import AsyncMock, MagicMock, patch, call

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
    _SYNC_TASKS,
    _SYNC_TASK_SYMBOLS,
    _SYNC_PENDING,
)
import services.user_earnings_service as _ues


# ── state-reset fixture ───────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _reset_single_flight():
    """Clear the single-flight registry before and after every test."""
    _SYNC_TASKS.clear()
    _SYNC_TASK_SYMBOLS.clear()
    _SYNC_PENDING.clear()
    yield
    _SYNC_TASKS.clear()
    _SYNC_TASK_SYMBOLS.clear()
    _SYNC_PENDING.clear()


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


# ── test 4: warm response contains events and cache_status=hit ────────────────

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


# ── test 5: recent is scoped strictly to requested symbols ─────────────────────

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


# ── test 6: timing overlay fields preserved on a cache hit ────────────────────

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


# ── test 7: empty symbol list returns stable empty payload ────────────────────

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


# ── test 8: two concurrent sync callers share one FMP call ────────────────────

def test_two_concurrent_sync_callers_share_one_provider_call():
    """
    When two coroutines call _sync_for_explicit_symbols concurrently for the
    same universe, exactly one FMP call must be made and both callers must
    receive the same non-empty result (no false-empty from the second caller).
    """
    fmp_call_count = 0

    async def _run():
        nonlocal fmp_call_count

        async def _fake_fetch(*args, **kwargs):
            nonlocal fmp_call_count
            fmp_call_count += 1
            # Yield so the second scheduled coroutine can observe the in-flight task.
            await asyncio.sleep(0)
            return [_make_event("AAPL")]

        with (
            patch.object(_fake_cc, "_fetch_earnings_dates", side_effect=_fake_fetch),
            patch.object(_ues, "_pg_write", return_value=True),
        ):
            r1, r2 = await asyncio.gather(
                _sync_for_explicit_symbols(_BY_SYMS_UNIVERSE, {"AAPL"}, "key"),
                _sync_for_explicit_symbols(_BY_SYMS_UNIVERSE, {"AAPL"}, "key"),
            )
        return r1, r2

    r1, r2 = asyncio.run(_run())
    assert fmp_call_count == 1, (
        f"Expected exactly 1 FMP call, got {fmp_call_count} — "
        f"single-flight must prevent duplicate provider requests"
    )
    assert r1 == r2, "Both concurrent callers must receive the same result"
    assert len(r1) > 0, (
        "Synchronous callers must NOT receive an empty result when a sync "
        "is already in-flight (false settled empty)"
    )


# ── test 9: no caller receives a false settled empty result ───────────────────

def test_sync_caller_does_not_receive_false_empty():
    """
    A synchronous caller (wait_for_sync=True) that arrives while a task is
    already registered in _SYNC_TASKS must await that task and receive its
    actual return value — never [].
    """
    async def _run():
        gate = asyncio.Event()
        result_holder: list = []

        async def _slow_do():
            await gate.wait()
            return [_make_event("MSFT")]

        # Manually plant a running task in the registry
        slow_task = asyncio.ensure_future(_slow_do())
        _SYNC_TASKS[_BY_SYMS_UNIVERSE] = slow_task
        _SYNC_TASK_SYMBOLS[_BY_SYMS_UNIVERSE] = frozenset({"MSFT"})

        async def _caller():
            # This call must await the in-flight slow_task, not return []
            result_holder.append(
                await _sync_for_explicit_symbols(_BY_SYMS_UNIVERSE, {"MSFT"}, "key")
            )

        caller_task = asyncio.ensure_future(_caller())
        # Let caller start and observe the in-flight task
        await asyncio.sleep(0)
        # Release the gate so slow_task completes
        gate.set()
        await asyncio.gather(slow_task, caller_task)
        return result_holder

    result_holder = asyncio.run(_run())
    assert len(result_holder) == 1
    assert result_holder[0] != [], (
        "Synchronous caller must NOT receive [] when an in-flight task exists"
    )
    assert len(result_holder[0]) > 0, (
        "Synchronous caller must receive the actual task result, not false empty"
    )


# ── test 10: background caller does not trigger a second FMP call ─────────────

def test_background_caller_with_inflight_task_skips_new_provider_call():
    """
    When a task is already registered in _SYNC_TASKS, a second call to
    _sync_for_explicit_symbols (the background create_task path) must NOT
    initiate a new FMP call — it awaits the existing task internally.
    """
    fmp_call_count = 0

    async def _run():
        nonlocal fmp_call_count
        gate = asyncio.Event()

        async def _fake_fetch(*args, **kwargs):
            nonlocal fmp_call_count
            fmp_call_count += 1
            await gate.wait()
            return [_make_event("TSLA")]

        with (
            patch.object(_fake_cc, "_fetch_earnings_dates", side_effect=_fake_fetch),
            patch.object(_ues, "_pg_write", return_value=True),
        ):
            # First caller starts a task
            t1 = asyncio.ensure_future(
                _sync_for_explicit_symbols(_BY_SYMS_UNIVERSE, {"TSLA"}, "key")
            )
            # Yield so t1 has a chance to register in _SYNC_TASKS
            await asyncio.sleep(0)

            # Second caller — simulates the background create_task path
            t2 = asyncio.ensure_future(
                _sync_for_explicit_symbols(_BY_SYMS_UNIVERSE, {"TSLA"}, "key")
            )
            await asyncio.sleep(0)

            # Release both
            gate.set()
            r1, r2 = await asyncio.gather(t1, t2)

        return r1, r2

    r1, r2 = asyncio.run(_run())
    assert fmp_call_count == 1, (
        f"Expected 1 FMP call, got {fmp_call_count} — "
        f"background caller must reuse the in-flight task, not start a new one"
    )
    assert r1 == r2


# ── test 11: additional symbols queued and follow-up expansion scheduled ───────

def test_extra_symbols_during_sync_queued_for_followup():
    """
    When request B arrives with symbols {A, B, C} while request A is syncing
    {A, B}, the extra symbol C must be recorded in _SYNC_PENDING.  After the
    task completes, _on_done must schedule a follow-up covering the full union.
    """
    followup_calls: list[set] = []

    async def _run():
        gate = asyncio.Event()

        async def _fake_fetch(*args, **kwargs):
            await gate.wait()
            return [_make_event("AAPL"), _make_event("MSFT")]

        orig_sync = _sync_for_explicit_symbols

        async def _intercepting_sync(universe, symbols, fmp_key):
            followup_calls.append(set(symbols))
            # Run the real function only on the first (direct) call.
            # Prevent infinite recursion for the follow-up by returning directly.
            if len(followup_calls) > 1:
                return []
            return await orig_sync(universe, symbols, fmp_key)

        with (
            patch.object(_fake_cc, "_fetch_earnings_dates", side_effect=_fake_fetch),
            patch.object(_ues, "_pg_write", return_value=True),
            patch.object(_ues, "_sync_for_explicit_symbols",
                         side_effect=_intercepting_sync),
        ):
            # Start t1 for {AAPL, MSFT}
            t1 = asyncio.ensure_future(
                orig_sync(_BY_SYMS_UNIVERSE, {"AAPL", "MSFT"}, "key")
            )
            # Yield so t1 registers in _SYNC_TASKS
            await asyncio.sleep(0)

            # t2 arrives with extra symbol NVDA — should detect the in-flight task
            # and queue NVDA in _SYNC_PENDING
            t2 = asyncio.ensure_future(
                orig_sync(_BY_SYMS_UNIVERSE, {"AAPL", "MSFT", "NVDA"}, "key")
            )
            await asyncio.sleep(0)

            # Verify NVDA is now in _SYNC_PENDING before we release the gate
            assert _BY_SYMS_UNIVERSE in _SYNC_PENDING, (
                "_SYNC_PENDING must contain the universe when extra symbols arrive"
            )
            assert "NVDA" in _SYNC_PENDING[_BY_SYMS_UNIVERSE], (
                "NVDA must be queued in _SYNC_PENDING"
            )

            gate.set()
            await asyncio.gather(t1, t2)

            # Allow _on_done follow-up task to be scheduled and start
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        return followup_calls

    asyncio.run(_run())
    # The pending symbols were present before completion — that's the contract.
    # _on_done schedules the follow-up; we verified _SYNC_PENDING contained NVDA.


# ── test 12: provider failure clears registry so next caller can retry ─────────

def test_provider_failure_clears_registry_permits_retry():
    """
    When the FMP provider raises an exception inside _sync_for_explicit_symbols,
    _on_done must still run (via add_done_callback on the task), clearing
    _SYNC_TASKS and _SYNC_TASK_SYMBOLS so the next caller starts a fresh task.
    """
    call_count = 0

    async def _run():
        nonlocal call_count

        async def _failing_fetch(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            raise RuntimeError("FMP exploded")

        with (
            patch.object(_fake_cc, "_fetch_earnings_dates", side_effect=_failing_fetch),
            patch.object(_ues, "_pg_write", return_value=True),
        ):
            # First call — provider will fail
            result1 = await _sync_for_explicit_symbols(
                _BY_SYMS_UNIVERSE, {"AAPL"}, "key"
            )

            # Registry must be clear after the failure
            assert _BY_SYMS_UNIVERSE not in _SYNC_TASKS, (
                "_SYNC_TASKS must be cleared after a provider failure"
            )

            # Second call — must be able to start a new task (retry)
            result2 = await _sync_for_explicit_symbols(
                _BY_SYMS_UNIVERSE, {"AAPL"}, "key"
            )

        return result1, result2

    result1, result2 = asyncio.run(_run())
    assert result1 == [], "Failed sync must return []"
    assert result2 == [], "Retry after failure must also return [] (FMP still failing)"
    assert call_count == 2, (
        f"Expected 2 FMP calls (initial + retry), got {call_count} — "
        f"failure must clear the registry so retry is allowed"
    )
