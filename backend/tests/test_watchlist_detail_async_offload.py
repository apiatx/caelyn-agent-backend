"""
Regression tests for the Watchlist detail read path (GET /{watchlist_id}).

Contract under test:
  The synchronous `load_watchlist()` Postgres/disk read must be offloaded
  off the asyncio event loop (via `asyncio.to_thread`), matching the
  established pattern already used elsewhere in this module (e.g. the
  `_aio.to_thread(_fetch_company)` / `_aio.to_thread(_fetch_fund)` calls).

  Everything else about the endpoint — response schema, LKG caching
  behaviour, and the "store is None" -> {"empty": True} contract — must be
  byte-for-byte unchanged.

Run:
    cd backend && python -m pytest tests/test_watchlist_detail_async_offload.py -v
"""

import asyncio
import os
import sys
import threading
import time

import pytest

_BACKEND_DIR = os.path.dirname(os.path.dirname(__file__))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

import services.watchlist_router as wlr  # noqa: E402  (real import; env has all deps)


WL_ID = "test-wl-async-offload-001"


@pytest.fixture(autouse=True)
def _reset_state():
    """Isolate LKG state across tests; restore any monkeypatched attrs after."""
    orig_lkg = dict(wlr._BULK_LKG)
    orig_building = set(wlr._BULK_LKG_BUILDING)
    orig_load_watchlist = wlr.load_watchlist
    orig_builder = wlr._build_watchlist_response

    wlr._BULK_LKG.clear()
    wlr._BULK_LKG_BUILDING.clear()
    yield
    wlr._BULK_LKG.clear()
    wlr._BULK_LKG.update(orig_lkg)
    wlr._BULK_LKG_BUILDING.clear()
    wlr._BULK_LKG_BUILDING.update(orig_building)
    wlr.load_watchlist = orig_load_watchlist
    wlr._build_watchlist_response = orig_builder


# ===========================================================================
# 1. The DB read now happens off the event-loop thread
# ===========================================================================

def test_01_load_watchlist_runs_off_event_loop_thread():
    """The synchronous store read must execute on a worker thread, not the
    event-loop thread that is running the coroutine."""
    seen_thread_id = {}

    def _fake_load_watchlist(watchlist_id):
        seen_thread_id["id"] = threading.get_ident()
        return None  # exercise the "not found" branch; asserted separately below

    wlr.load_watchlist = _fake_load_watchlist

    loop_thread_id = {}

    async def _runner():
        loop_thread_id["id"] = threading.get_ident()
        return await wlr.get_by_id_endpoint(WL_ID)

    result = asyncio.run(_runner())

    assert result == {"empty": True}, "Response schema for a missing watchlist must be unchanged"
    assert "id" in seen_thread_id, "load_watchlist must have been invoked"
    assert seen_thread_id["id"] != loop_thread_id["id"], (
        "load_watchlist must run on a worker thread (asyncio.to_thread), "
        "not inline on the event-loop thread"
    )


# ===========================================================================
# 2. The event loop stays responsive while the (slow) DB read is in flight
# ===========================================================================

def test_02_event_loop_not_blocked_during_db_read():
    """A slow load_watchlist() must not freeze other concurrently-scheduled
    coroutines — proving the call is truly offloaded, not just run on the
    same thread with a different label."""
    SLEEP_S = 0.25

    def _slow_load_watchlist(watchlist_id):
        time.sleep(SLEEP_S)
        return None

    wlr.load_watchlist = _slow_load_watchlist

    tick_count = {"n": 0}

    async def _ticker():
        # Increments roughly every 10ms; if the loop were blocked for
        # SLEEP_S, this would tick ~0 times during that window.
        while True:
            tick_count["n"] += 1
            await asyncio.sleep(0.01)

    async def _runner():
        ticker_task = asyncio.create_task(_ticker())
        await wlr.get_by_id_endpoint(WL_ID)
        ticker_task.cancel()

    asyncio.run(_runner())

    # With a 0.25s blocking read fully offloaded, the ticker should get
    # several chances to run concurrently (~20+ ticks at 10ms cadence).
    # A regression back to an inline blocking call would starve this to ~0-1.
    assert tick_count["n"] >= 5, (
        f"Event loop appears blocked during the DB read (only {tick_count['n']} "
        "ticks observed); load_watchlist may no longer be offloaded"
    )


# ===========================================================================
# 3. "Found" path: response schema / LKG write-through are unchanged
# ===========================================================================

def test_03_found_store_response_schema_unchanged():
    """When a watchlist exists, the endpoint must still build the response
    via _build_watchlist_response and populate the LKG exactly as before —
    only the *sourcing* of `store` changed (now via to_thread)."""
    fake_store = {
        "id": WL_ID,
        "tickers": ["AAPL", "NVDA"],
        "updated_at": "2026-01-01T00:00:00+00:00",
        "saved_at": "2026-01-01T00:00:00+00:00",
    }
    fake_response = {"id": WL_ID, "tickers": ["AAPL", "NVDA"], "_meta": {"rows": 2}}

    def _fake_load_watchlist(watchlist_id):
        assert watchlist_id == WL_ID
        return dict(fake_store)

    async def _fake_builder(watchlist_id, store, wl_load_ms=0):
        assert watchlist_id == WL_ID
        assert store["tickers"] == ["AAPL", "NVDA"]
        return dict(fake_response)

    wlr.load_watchlist = _fake_load_watchlist
    wlr._build_watchlist_response = _fake_builder

    result = asyncio.run(wlr.get_by_id_endpoint(WL_ID))

    assert result == fake_response, "Found-watchlist response payload must be unchanged"
    assert WL_ID in wlr._BULK_LKG, "LKG must still be populated after a cold build"
    assert wlr._BULK_LKG[WL_ID]["payload"] == fake_response
    assert WL_ID not in wlr._BULK_LKG_BUILDING


# ===========================================================================
# 4. LKG hit path is untouched: a fresh cache entry still short-circuits
#    the (now-offloaded) store read's downstream build, without skipping
#    the read itself (version check still needs `store`).
# ===========================================================================

def test_04_lkg_hit_still_served_and_load_watchlist_still_called():
    fake_store = {
        "id": WL_ID,
        "tickers": ["AAPL"],
        "updated_at": "2026-01-01T00:00:00+00:00",
        "saved_at": "2026-01-01T00:00:00+00:00",
    }
    version = f"{fake_store['updated_at']}|{len(fake_store['tickers'])}"
    cached_payload = {"id": WL_ID, "tickers": ["AAPL"], "_meta": {"cached": True}}

    wlr._BULK_LKG[WL_ID] = {
        "payload": cached_payload,
        "ts": time.monotonic(),
        "version": version,
    }

    call_count = {"n": 0}

    def _fake_load_watchlist(watchlist_id):
        call_count["n"] += 1
        return dict(fake_store)

    wlr.load_watchlist = _fake_load_watchlist

    result = asyncio.run(wlr.get_by_id_endpoint(WL_ID))

    assert result == cached_payload, "Fresh LKG hit must still be served verbatim"
    assert call_count["n"] == 1, "load_watchlist must still run once (needed for version check)"


# ===========================================================================
# 5. Endpoint is still registered as a coroutine function on the same route
# ===========================================================================

def test_05_endpoint_is_still_async_get_by_id():
    import inspect

    assert inspect.iscoroutinefunction(wlr.get_by_id_endpoint), (
        "get_by_id_endpoint must remain an async def"
    )
