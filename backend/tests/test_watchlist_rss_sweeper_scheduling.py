import ast
import asyncio
import sys
import types
from pathlib import Path

from services import watchlist_rss_sweeper as sweeper


RSS_SWEEPER = (
    Path(__file__).resolve().parents[1]
    / "services"
    / "watchlist_rss_sweeper.py"
)


def _source():
    return RSS_SWEEPER.read_text(encoding="utf-8")


def _async_function_source(name):
    source = _source()
    node = next(
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == name
    )
    return ast.get_source_segment(source, node) or ""


def _configure_single_ticker_sweep(monkeypatch, rebuild):
    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    async def _sweep_ticker(ticker, client, sem, db_write_sem, sweep_diag):
        return {"ticker": ticker}

    archive = types.ModuleType("data.rss_article_archive")
    archive.warm_seen_cache = lambda: 0
    archive.query_ticker_activity = lambda tickers: {}
    router = types.ModuleType("services.watchlist_router")
    router._HYP_CACHE = {}
    router._HYP_CACHE_BUILDING = False
    router._rebuild_hyperscaler_cache = rebuild

    monkeypatch.setattr(sweeper, "list_watchlists", lambda: [{"id": "wl"}])
    monkeypatch.setattr(
        sweeper, "load_watchlist", lambda watchlist_id: {"tickers": ["TEST"]}
    )
    monkeypatch.setattr(sweeper, "_sweep_ticker", _sweep_ticker)
    monkeypatch.setattr(sweeper.httpx, "AsyncClient", lambda **kwargs: _Client())
    monkeypatch.setitem(sys.modules, "data.rss_article_archive", archive)
    monkeypatch.setitem(sys.modules, "services.watchlist_router", router)
    monkeypatch.setattr(sys.modules["services"], "watchlist_router", router, raising=False)
    monkeypatch.setitem(sweeper._SWEEPER_DIAG, "sweep_count", 0)


def test_rss_concurrency_bounds_are_explicit():
    source = _source()
    assert "_SWEEP_SEM_SIZE          = 8" in source
    assert "_DB_WRITE_SEM_SIZE       = 4" in source
    sweep = _async_function_source("run_rss_sweep")
    assert "asyncio.Semaphore(_SWEEP_SEM_SIZE)" in sweep
    assert "asyncio.Semaphore(_DB_WRITE_SEM_SIZE)" in sweep


def test_membership_reads_are_offloaded_from_event_loop():
    sweep = _async_function_source("run_rss_sweep")
    assert "await asyncio.to_thread(list_watchlists)" in sweep
    assert "await asyncio.to_thread(load_watchlist, wl_meta.get(\"id\"))" in sweep
    assert "watchlists = list_watchlists()" not in sweep
    assert "wl = load_watchlist(" not in sweep


def test_archive_writes_have_an_independent_concurrency_bound():
    worker = _async_function_source("_sweep_ticker")
    assert "async with db_write_sem:" in worker
    assert worker.index("async with db_write_sem:") < worker.index(
        "upsert_with_cache, ticker, merged"
    )


def test_full_sweep_has_at_least_sixty_seconds_breathing_room():
    source = _source()
    assert "_MIN_POST_SWEEP_IDLE_S   = 60" in source
    loop = _async_function_source("rss_sweeper_loop")
    assert (
        "max(_MIN_POST_SWEEP_IDLE_S, _TARGET_INTERVAL_S - elapsed)"
        in loop
    )


def test_existing_lock_and_dedupe_write_semantics_remain_present():
    source = _source()
    sweep = _async_function_source("run_rss_sweep")
    worker = _async_function_source("_sweep_ticker")
    loop = _async_function_source("rss_sweeper_loop")
    assert "_cluster_key as _ck" in source
    assert "_merge_and_dedupe(yahoo_arts, google_arts)" in worker
    assert "upsert_with_cache, ticker, merged" in worker
    assert "async with _SWEEP_LOCK:" in loop
    assert "await run_rss_sweep()" in loop


def test_skip_initial_defers_first_sweep_without_startup_catchup():
    source = _source()
    loop = _async_function_source("rss_sweeper_loop")
    tree = ast.parse(source)
    loop_node = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "rss_sweeper_loop"
    )

    assert not loop_node.args.args
    assert [arg.arg for arg in loop_node.args.kwonlyargs] == ["skip_initial"]
    assert len(loop_node.args.kw_defaults) == 1
    assert isinstance(loop_node.args.kw_defaults[0], ast.Constant)
    assert loop_node.args.kw_defaults[0].value is False
    assert "if not skip_initial:" in loop
    assert "await asyncio.sleep(_STARTUP_DELAY_S)" in loop
    assert "await asyncio.to_thread(warm_seen_cache)" in loop
    assert "_STARTUP_DELAY_S         = 120" in source

    assert "initial_wait = _TARGET_INTERVAL_S" in loop
    assert "await asyncio.sleep(initial_wait)" in loop


async def test_default_sweep_still_schedules_rebuild_in_background(monkeypatch):
    rebuild_started = asyncio.Event()
    allow_rebuild_to_finish = asyncio.Event()
    rebuild_tasks = []

    async def rebuild(tickers):
        rebuild_tasks.append(asyncio.current_task())
        rebuild_started.set()
        await allow_rebuild_to_finish.wait()

    _configure_single_ticker_sweep(monkeypatch, rebuild)

    await sweeper.run_rss_sweep()
    await rebuild_started.wait()

    assert len(rebuild_tasks) == 1
    assert rebuild_tasks[0] is not asyncio.current_task()
    assert not rebuild_tasks[0].done()

    allow_rebuild_to_finish.set()
    await rebuild_tasks[0]
