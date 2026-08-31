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


async def _async_identity(_watchlist_id, sections, _saved_symbols):
    return sections


@pytest.fixture(autouse=True)
def _reset_state():
    """Isolate LKG state across tests; restore any monkeypatched attrs after."""
    orig_lkg = dict(wlr._BULK_LKG)
    orig_building = set(wlr._BULK_LKG_BUILDING)
    orig_load_watchlist = wlr.load_watchlist
    orig_builder = wlr._build_watchlist_response
    orig_rv_mem = dict(wlr._rv_mem)
    orig_volmc_mem = dict(wlr._volmc_mem)

    wlr._BULK_LKG.clear()
    wlr._BULK_LKG_BUILDING.clear()
    wlr._rv_mem.clear()
    wlr._volmc_mem.clear()
    yield
    wlr._BULK_LKG.clear()
    wlr._BULK_LKG.update(orig_lkg)
    wlr._BULK_LKG_BUILDING.clear()
    wlr._BULK_LKG_BUILDING.update(orig_building)
    wlr.load_watchlist = orig_load_watchlist
    wlr._build_watchlist_response = orig_builder
    wlr._rv_mem.clear()
    wlr._rv_mem.update(orig_rv_mem)
    wlr._volmc_mem.clear()
    wlr._volmc_mem.update(orig_volmc_mem)


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

    async def _fake_builder(watchlist_id, store, wl_load_ms=0, **kwargs):
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


# ===========================================================================
# 6. Cold quote refresh is not awaited by the detail enrichment path
# ===========================================================================

def test_06_detail_enrichment_requests_cache_first_quotes(monkeypatch):
    """The detail GET must opt into non-waiting quotes without changing the
    existing skeleton response contract."""
    import services.watchlist_quote_cache as quote_cache

    seen = {}

    async def _fake_quotes(symbols, **kwargs):
        seen["symbols"] = list(symbols)
        seen.update(kwargs)
        return {}

    monkeypatch.setattr(quote_cache, "get_watchlist_quotes", _fake_quotes)
    monkeypatch.setattr(wlr, "_apply_rv_rank_fields", _async_identity)
    monkeypatch.setattr(wlr, "_apply_volmc_rank_fields", _async_identity)

    store = {
        "id": WL_ID,
        "tickers": ["AAPL"],
        "csv_data": [{"Symbol": "AAPL", "Stock Price": "100"}],
        "analysis": {},
    }
    result = asyncio.run(wlr._enrich_store_with_quotes(store))

    assert seen == {"symbols": ["AAPL"], "wait_for_refresh": False}
    assert result["analysis"]["_analysis_pending"] is True
    assert result["analysis"]["_skeleton_reason"] == "analysis_not_yet_run"
    section = result["analysis"]["sections"][0]
    assert {
        "name": section["name"],
        "id": section["id"],
        "subtitle": section["subtitle"],
        "_analysis_pending": section["_analysis_pending"],
    } == {
        "name": "All Tickers",
        "id": "all_tickers",
        "subtitle": "Showing saved tickers — AI analysis running in background",
        "_analysis_pending": True,
    }
    row = section["tickers"][0]
    assert row["symbol"] == "AAPL"
    assert row["price"] == 100.0
    for field in (
        "catalyst",
        "sentiment",
        "action_note",
        "conviction",
        "theme",
        "canonical_theme_name",
        "canonical_theme_id",
        "theme_source",
    ):
        assert field in row


# ===========================================================================
# 7. The cache-only cold mode returns before a slow provider refresh completes
# ===========================================================================

def test_07_cold_quote_cache_mode_does_not_wait_for_provider(monkeypatch):
    """The opt-in mode must schedule, not await, the first live refresh."""
    import services.watchlist_quote_cache as quote_cache

    quote_cache._quote_cache.clear()
    original_loader = quote_cache._load_disk_lkg
    original_refresh = quote_cache._locked_refresh
    original_ts = quote_cache._cache_ts
    refresh_started = asyncio.Event()

    async def _slow_refresh(symbols):
        refresh_started.set()
        await asyncio.Event().wait()

    def _empty_disk_lkg():
        return {}

    quote_cache._load_disk_lkg = _empty_disk_lkg
    quote_cache._locked_refresh = _slow_refresh

    async def _run():
        started = time.monotonic()
        result = await quote_cache.get_watchlist_quotes(
            ["AAPL"], wait_for_refresh=False
        )
        elapsed = time.monotonic() - started
        await asyncio.wait_for(refresh_started.wait(), timeout=0.5)
        pending = [
            task for task in asyncio.all_tasks()
            if task is not asyncio.current_task() and not task.done()
        ]
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        return result, elapsed

    try:
        result, elapsed = asyncio.run(_run())
    finally:
        quote_cache._load_disk_lkg = original_loader
        quote_cache._locked_refresh = original_refresh
        quote_cache._cache_ts = original_ts
        quote_cache._quote_cache.clear()

    assert result == {}
    assert elapsed < 1.0, "Cold detail quote path must not await the provider refresh"


# ===========================================================================
# 8. Watchlist metadata listing does not block the event loop
# ===========================================================================

def test_08_list_endpoint_offloads_synchronous_db_read(monkeypatch):
    """A slow list_watchlists DB read must run on a worker thread."""
    loop_thread_id = {}
    db_thread_id = {}
    tick_count = {"n": 0}
    expected = [{"id": WL_ID, "name": "Primary", "ticker_count": 1}]

    def _slow_list_watchlists():
        db_thread_id["id"] = threading.get_ident()
        time.sleep(0.25)
        return expected

    monkeypatch.setattr(wlr, "list_watchlists", _slow_list_watchlists)

    async def _ticker():
        while True:
            tick_count["n"] += 1
            await asyncio.sleep(0.01)

    async def _run():
        loop_thread_id["id"] = threading.get_ident()
        ticker = asyncio.create_task(_ticker())
        try:
            return await wlr.list_endpoint()
        finally:
            ticker.cancel()
            await asyncio.gather(ticker, return_exceptions=True)

    result = asyncio.run(_run())

    assert result == expected, "List response contract must be unchanged"
    assert db_thread_id["id"] != loop_thread_id["id"]
    assert tick_count["n"] >= 5, "Event loop was blocked by list_watchlists"


# ===========================================================================
# 9. An existing saved watchlist with no tickers remains a normal found record
# ===========================================================================

def test_09_saved_empty_watchlist_contract_unchanged(monkeypatch):
    """An existing empty watchlist is not the same as a missing watchlist."""
    empty_store = {
        "id": WL_ID,
        "name": "Empty",
        "tickers": [],
        "csv_data": [],
        "analysis": {"sections": []},
        "updated_at": "2026-01-01T00:00:00+00:00",
        "saved_at": "2026-01-01T00:00:00+00:00",
    }
    expected = {
        **empty_store,
        "_meta": {"rows": 0},
        "upcoming_earnings": {"events": []},
    }
    seen = {}

    monkeypatch.setattr(wlr, "load_watchlist", lambda _wid: dict(empty_store))

    async def _builder(watchlist_id, store, wl_load_ms=0, **kwargs):
        seen["watchlist_id"] = watchlist_id
        seen["store"] = dict(store)
        return dict(expected)

    monkeypatch.setattr(wlr, "_build_watchlist_response", _builder)

    result = asyncio.run(wlr.get_by_id_endpoint(WL_ID))

    assert seen["watchlist_id"] == WL_ID
    assert seen["store"] == empty_store
    assert result == expected
    assert result != {"empty": True}
    assert wlr._BULK_LKG[WL_ID]["payload"] == expected


# ===========================================================================
# 10. A slow store read with no LKG preserves the real cold-path store
# ===========================================================================

def test_10_slow_db_read_without_lkg_does_not_fabricate_watchlist(monkeypatch):
    store = {
        "id": WL_ID,
        "name": "Real",
        "tickers": ["AAPL"],
        "csv_data": [],
        "analysis": {},
    }
    expected = {"id": WL_ID, "tickers": ["AAPL"], "_meta": {"rows": 1}}

    def _slow_load(_watchlist_id):
        time.sleep(0.05)
        return dict(store)

    async def _builder(watchlist_id, loaded, wl_load_ms=0, **kwargs):
        assert watchlist_id == WL_ID
        assert loaded == store
        return dict(expected)

    monkeypatch.setattr(wlr, "load_watchlist", _slow_load)
    monkeypatch.setattr(wlr, "_build_watchlist_response", _builder)
    monkeypatch.setattr(wlr, "_WATCHLIST_DB_READ_TIMEOUT_S", 0.001)

    result = asyncio.run(wlr.get_by_id_endpoint(WL_ID))

    assert result == expected
    assert result["id"] == WL_ID
    assert result != {"empty": True}


# ===========================================================================
# 11. A timed-out shielded read continues safely after serving the LKG
# ===========================================================================

def test_11_db_read_timeout_shields_worker_until_connection_work_finishes(monkeypatch):
    started = threading.Event()
    finished = threading.Event()
    cached_payload = {"id": WL_ID, "tickers": ["AAPL"], "_meta": {"cached": True}}
    wlr._BULK_LKG[WL_ID] = {
        "payload": cached_payload,
        "ts": time.monotonic(),
        "version": "unvalidated",
    }

    def _slow_load(_watchlist_id):
        started.set()
        time.sleep(0.05)
        finished.set()
        return {"id": WL_ID, "tickers": ["AAPL"]}

    async def _run():
        monkeypatch.setattr(wlr, "load_watchlist", _slow_load)
        monkeypatch.setattr(wlr, "_WATCHLIST_DB_READ_TIMEOUT_S", 0.001)
        result = await wlr.get_by_id_endpoint(WL_ID)
        assert started.is_set()
        assert result == cached_payload
        assert not finished.is_set()
        await asyncio.wait_for(asyncio.to_thread(finished.wait), timeout=0.5)
        return result

    result = asyncio.run(_run())
    assert result == cached_payload


# ===========================================================================
# 12. Earnings remains an optional overlay within the available budget
# ===========================================================================

def test_12_earnings_runs_when_response_budget_remains(monkeypatch):
    import services.user_earnings_service as earnings_service

    calls = {}
    async def _fake_earnings(symbols, **kwargs):
        calls["symbols"] = list(symbols)
        calls["kwargs"] = kwargs
        return {
            "symbols_requested": ["AAPL"],
            "events": [{"ticker": "AAPL"}],
            "missing_symbols": [],
            "source": "cached_earnings",
            "last_updated": "2026-01-01T00:00:00+00:00",
            "stale": False,
            "cache_status": "hit",
        }

    async def _identity(store):
        return store

    monkeypatch.setattr(earnings_service, "get_upcoming_earnings_for_symbols", _fake_earnings)
    monkeypatch.setattr(wlr, "_enrich_store_with_quotes", _identity)
    store = {"id": WL_ID, "tickers": ["AAPL"], "csv_data": [], "analysis": {}}

    result = asyncio.run(
        wlr._build_watchlist_response(
            WL_ID,
            store,
            response_deadline=time.monotonic() + 5.0,
        )
    )

    assert calls["symbols"] == ["AAPL"]
    assert calls["kwargs"]["sync_on_miss"] is False
    assert calls["kwargs"]["background_sync_on_miss"] is True
    assert result["upcoming_earnings"]["events"] == [{"ticker": "AAPL"}]


def test_13_earnings_fallback_shape_is_kept_when_budget_is_exhausted(monkeypatch):
    import services.user_earnings_service as earnings_service

    async def _must_not_run(*args, **kwargs):
        raise AssertionError("earnings overlay must not start with no budget")

    async def _identity(store):
        return store

    monkeypatch.setattr(earnings_service, "get_upcoming_earnings_for_symbols", _must_not_run)
    monkeypatch.setattr(wlr, "_enrich_store_with_quotes", _identity)
    store = {"id": WL_ID, "tickers": ["AAPL"], "csv_data": [], "analysis": {}}

    result = asyncio.run(
        wlr._build_watchlist_response(
            WL_ID,
            store,
            response_deadline=time.monotonic() - 1.0,
        )
    )

    assert result["upcoming_earnings"] == {
        "watchlist_id": WL_ID,
        "symbols_requested": [],
        "events": [],
        "missing_symbols": [],
        "source": "cached_earnings",
        "last_updated": None,
        "stale": True,
        "cache_status": "skipped",
    }


def test_14_rank_snapshot_reads_run_concurrently_and_preserve_results(monkeypatch):
    started = {"rv": threading.Event(), "volmc": threading.Event()}
    release = threading.Event()
    expected_rv = ({"AAPL": {"rank": 1}}, {"AAPL": {"rank": 2}})
    expected_volmc = ({"AAPL": {"rank": 3}}, {"AAPL": {"rank": 4}})

    def _load_rv(watchlist_id):
        assert watchlist_id == WL_ID
        started["rv"].set()
        assert started["volmc"].wait(0.5)
        assert release.wait(0.5)
        return expected_rv

    def _load_volmc(watchlist_id):
        assert watchlist_id == WL_ID
        started["volmc"].set()
        assert started["rv"].wait(0.5)
        release.set()
        return expected_volmc

    monkeypatch.setattr(wlr, "_rv_neon_load", _load_rv)
    monkeypatch.setattr(wlr, "_volmc_neon_load", _load_volmc)

    result = asyncio.run(
        wlr._load_rank_snapshots_cache_first(WL_ID, timeout_s=0.5)
    )

    assert result == (expected_rv, expected_volmc)


def test_15_rank_snapshot_memory_hits_avoid_postgres(monkeypatch):
    rv_current = {"AAPL": {"rank": 1, "rel_vol": 2.0}}
    rv_previous = {"AAPL": {"rank": 2, "rel_vol": 1.5}}
    vm_current = {"AAPL": {"rank": 1, "vol_mc_pct": 1.2}}
    vm_previous = {"AAPL": {"rank": 3, "vol_mc_pct": 0.8}}
    wlr._rv_mem[WL_ID] = {"current": rv_current, "previous": rv_previous}
    wlr._volmc_mem[WL_ID] = {"current": vm_current, "previous": vm_previous}

    def _must_not_read(_watchlist_id):
        raise AssertionError("PostgreSQL must not be read on a memory-cache hit")

    monkeypatch.setattr(wlr, "_rv_neon_load", _must_not_read)
    monkeypatch.setattr(wlr, "_volmc_neon_load", _must_not_read)

    result = asyncio.run(
        wlr._load_rank_snapshots_cache_first(WL_ID, timeout_s=0.1)
    )

    assert result == (
        (rv_current, rv_previous),
        (vm_current, vm_previous),
    )


def test_16_fast_rank_snapshot_reads_preserve_postgres_results(monkeypatch):
    expected_rv = (
        {"AAPL": {"rank": 1, "rel_vol": 2.0}},
        {"AAPL": {"rank": 2, "rel_vol": 1.5}},
    )
    expected_volmc = (
        {"AAPL": {"rank": 1, "vol_mc_pct": 1.2}},
        {"AAPL": {"rank": 3, "vol_mc_pct": 0.8}},
    )
    monkeypatch.setattr(wlr, "_rv_neon_load", lambda _wid: expected_rv)
    monkeypatch.setattr(wlr, "_volmc_neon_load", lambda _wid: expected_volmc)

    result = asyncio.run(
        wlr._load_rank_snapshots_cache_first(WL_ID, timeout_s=0.5)
    )

    assert result == (expected_rv, expected_volmc)


def test_17_bounded_rank_snapshot_miss_keeps_current_rank_contract(monkeypatch):
    workers_finished = {"rv": threading.Event(), "volmc": threading.Event()}

    def _slow_load(label):
        def _load(_watchlist_id):
            time.sleep(0.05)
            workers_finished[label].set()
            return ({"AAPL": {"rank": 9}}, {"AAPL": {"rank": 8}})
        return _load

    monkeypatch.setattr(wlr, "_rv_neon_load", _slow_load("rv"))
    monkeypatch.setattr(wlr, "_volmc_neon_load", _slow_load("volmc"))

    async def _run():
        snapshots = await wlr._load_rank_snapshots_cache_first(
            WL_ID, timeout_s=0.001
        )
        sections = [{
            "name": "Other",
            "tickers": [
                {"symbol": "AAPL", "relative_volume": 2.0, "vol_mc_pct": 1.2},
                {"symbol": "MSFT", "relative_volume": 1.0, "vol_mc_pct": 0.6},
            ],
        }]
        sections = await wlr._apply_rv_rank_fields(
            WL_ID, sections, ["AAPL", "MSFT"],
            preloaded_snapshots=snapshots[0],
        )
        sections = await wlr._apply_volmc_rank_fields(
            WL_ID, sections, ["AAPL", "MSFT"],
            preloaded_snapshots=snapshots[1],
        )
        await asyncio.wait_for(
            asyncio.gather(*(
                asyncio.to_thread(event.wait)
                for event in workers_finished.values()
            )),
            timeout=0.5,
        )
        return sections

    rows = asyncio.run(_run())[0]["tickers"]

    assert [row["rel_vol_rank"] for row in rows] == [1, 2]
    assert [row["vol_mc_rank"] for row in rows] == [1, 2]
    for row in rows:
        assert row["rel_vol_prev_rank"] is None
        assert row["rel_vol_rank_delta"] is None
        assert row["rel_vol_trend"] == "unknown"
        assert row["rel_vol_momentum_label"] == "unknown"
        assert row["vol_mc_prev_rank"] is None
        assert row["vol_mc_rank_delta"] is None
        assert row["vol_mc_trend"] == "unknown"
        assert row["vol_mc_momentum_label"] == "unknown"
