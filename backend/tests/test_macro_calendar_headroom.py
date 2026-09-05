import asyncio
import threading
import time


def test_calendar_async_read_runs_off_event_loop(monkeypatch):
    from services import calendar_snapshot_service as service

    event_loop_thread = threading.get_ident()
    observed = {}
    expected = {"status": "ready", "current_week": [], "previous_week": []}

    def fake_get_snapshot(tab):
        observed["thread"] = threading.get_ident()
        return expected

    monkeypatch.setattr(service, "get_snapshot", fake_get_snapshot)
    result = asyncio.run(service.get_snapshot_async("economic_releases"))

    assert result == expected
    assert observed["thread"] != event_loop_thread


def test_calendar_async_reads_are_bounded_and_preserve_results(monkeypatch):
    from services import calendar_snapshot_service as service

    lock = threading.Lock()
    inflight = 0
    max_inflight = 0

    def fake_get_snapshot(tab):
        nonlocal inflight, max_inflight
        with lock:
            inflight += 1
            max_inflight = max(max_inflight, inflight)
        time.sleep(0.02)
        with lock:
            inflight -= 1
        return {"status": "ready", "tab": tab}

    monkeypatch.setattr(service, "get_snapshot", fake_get_snapshot)

    async def run_many():
        return await asyncio.gather(
            *(service.get_snapshot_async(f"tab-{index}") for index in range(12))
        )

    results = asyncio.run(run_many())
    assert max_inflight <= service.CALENDAR_SNAPSHOT_MAX_PG_INFLIGHT == 2
    assert results == [{"status": "ready", "tab": f"tab-{i}"} for i in range(12)]


def test_calendar_sync_read_retains_neon_and_disk_fallback(monkeypatch):
    from services import calendar_snapshot_service as service

    neon_slot = {
        "current_week": [{"title": "Neon"}],
        "previous_week": [],
        "events": [],
        "meta": {"status": "ready"},
    }
    monkeypatch.setattr(service, "_neon_read", lambda tab: neon_slot)
    monkeypatch.setattr(
        service,
        "_read_disk",
        lambda: {"economic_releases": {"current_week": [{"title": "Disk"}]}},
    )
    neon_result = service.get_snapshot("economic_releases")
    assert neon_result["current_week"][0]["title"] == "Neon"
    assert neon_result["diagnostics"]["source"] == "neon"

    monkeypatch.setattr(service, "_neon_read", lambda tab: None)
    disk_result = service.get_snapshot("economic_releases")
    assert disk_result["current_week"][0]["title"] == "Disk"
    assert disk_result["diagnostics"]["source"] == "disk"


def test_pg_pool_size_remains_five():
    from data import pg_storage

    source = open(pg_storage.__file__, encoding="utf-8").read()
    assert "ThreadedConnectionPool(" in source
    assert "1, 5, _DATABASE_URL" in source


def test_macro_history_reads_are_offloaded_and_tabs_are_bounded():
    source = open("backend/data/macro_provider.py", encoding="utf-8").read()
    main_source = open("backend/main.py", encoding="utf-8").read()

    assert source.count("history = await asyncio.to_thread(") >= 2
    assert "history_raw = await asyncio.to_thread(" in source
    assert "MAX_MACRO_TAB_PREWARM_CONCURRENCY = 2" in main_source
    assert "async with tab_semaphore:" in main_source
    assert "max_event_loop_lag_ms=" in main_source