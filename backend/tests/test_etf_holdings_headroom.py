import asyncio
import threading
import time

import pytest

from services.sector_rotation import etf_holdings_service as svc


def _snapshot(age_s=0):
    return {
        "symbol": "ETF",
        "holdings": [{"ticker": "ONE"}],
        "_fetched_at": time.time() - age_s,
    }


@pytest.fixture(autouse=True)
def clear_state(monkeypatch):
    svc._refreshing.clear()
    svc._live_refresh_tasks.clear()
    svc._last_attempt_at.clear()
    monkeypatch.setattr(svc.cache, "get", lambda _key: None)
    monkeypatch.setattr(svc.cache, "set", lambda *_args: None)


@pytest.mark.asyncio
async def test_fresh_memory_avoids_disk_and_provider(monkeypatch):
    monkeypatch.setattr(svc.cache, "get", lambda _key: _snapshot())
    monkeypatch.setattr(svc, "_load_disk", lambda _sym: pytest.fail("disk read"))
    monkeypatch.setattr(svc, "_fetch_holdings", lambda _sym: pytest.fail("provider"))
    assert (await svc.get_etf_holdings("ETF"))["holdings"]


@pytest.mark.asyncio
async def test_fresh_disk_avoids_provider_and_runs_off_loop(monkeypatch):
    calls = []
    main_thread = threading.get_ident()

    def load(sym):
        calls.append((sym, threading.get_ident()))
        return _snapshot()

    monkeypatch.setattr(svc, "_load_disk", load)
    monkeypatch.setattr(svc, "_fetch_holdings", lambda _sym: pytest.fail("provider"))
    result = await svc.get_etf_holdings("ETF")
    assert result["holdings"] and calls[0][0] == "ETF"
    assert calls[0][1] != main_thread


@pytest.mark.asyncio
async def test_stale_disk_returns_without_waiting_for_refresh(monkeypatch):
    stale = _snapshot(svc._FRESH_TTL + 1)
    started = asyncio.Event()
    release = asyncio.Event()

    monkeypatch.setattr(svc, "_load_disk", lambda _sym: stale)

    async def slow_fetch(_sym):
        started.set()
        await release.wait()
        return None

    monkeypatch.setattr(svc, "_fetch_holdings", slow_fetch)
    result = await asyncio.wait_for(svc.get_etf_holdings("ETF"), 0.1)
    assert result is stale
    await asyncio.wait_for(started.wait(), 0.1)
    release.set()
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_many_live_calls_obey_global_bound(monkeypatch):
    inflight = 0
    observed_max = 0

    monkeypatch.setattr(svc, "_load_disk", lambda _sym: None)
    monkeypatch.setattr(svc, "_stamp_and_cache", lambda _sym, data: asyncio.sleep(0, result=data))

    async def fake_fetch(sym):
        nonlocal inflight, observed_max
        inflight += 1
        observed_max = max(observed_max, inflight)
        await asyncio.sleep(0.02)
        inflight -= 1
        return {"symbol": sym, "holdings": [{"ticker": "ONE"}]}

    monkeypatch.setattr(svc, "_fetch_holdings", fake_fetch)
    await asyncio.gather(*(svc.get_etf_holdings(f"E{i}") for i in range(16)))
    assert observed_max <= svc.ETF_HOLDINGS_MAX_LIVE_REFRESH_CONCURRENCY == 4


@pytest.mark.asyncio
async def test_same_symbol_live_calls_are_single_flight(monkeypatch):
    calls = 0
    monkeypatch.setattr(svc, "_load_disk", lambda _sym: None)
    monkeypatch.setattr(svc, "_stamp_and_cache", lambda _sym, data: asyncio.sleep(0, result=data))

    async def fake_fetch(sym):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.02)
        return {"symbol": sym, "holdings": [{"ticker": "ONE"}]}

    monkeypatch.setattr(svc, "_fetch_holdings", fake_fetch)
    results = await asyncio.gather(
        svc.get_etf_holdings("ETF"),
        svc.get_etf_holdings("ETF"),
    )
    assert calls == 1
    assert all(result["holdings"] for result in results)


@pytest.mark.asyncio
async def test_cancelled_waiter_does_not_leave_singleflight_task(monkeypatch):
    release = asyncio.Event()
    monkeypatch.setattr(svc, "_load_disk", lambda _sym: None)

    async def fake_fetch(sym):
        await release.wait()
        return {"symbol": sym, "holdings": [{"ticker": "ONE"}]}

    monkeypatch.setattr(svc, "_fetch_holdings", fake_fetch)
    waiter = asyncio.create_task(svc.get_etf_holdings("ETF"))
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    release.set()
    await asyncio.sleep(0.01)
    assert "ETF" not in svc._live_refresh_tasks


@pytest.mark.asyncio
async def test_disk_write_runs_off_event_loop(monkeypatch):
    main_thread = threading.get_ident()
    write_threads = []
    monkeypatch.setattr(svc, "_save_disk", lambda *_args: write_threads.append(threading.get_ident()))
    await svc._stamp_and_cache("ETF", {"holdings": []})
    assert write_threads and write_threads[0] != main_thread


@pytest.mark.asyncio
async def test_failed_refresh_backoff_and_fmp_guard_are_preserved(monkeypatch):
    calls = 0
    monkeypatch.setattr(svc, "_load_disk", lambda _sym: None)

    async def failed(_sym):
        nonlocal calls
        calls += 1
        return None

    monkeypatch.setattr(svc, "_fetch_holdings", failed)
    await svc.get_etf_holdings("ETF")
    await svc.get_etf_holdings("ETF")
    assert calls == 1
    assert svc._FMP_ETF_HOLDINGS_DISABLED is True