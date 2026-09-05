import asyncio
import threading
import time
from collections import OrderedDict
from pathlib import Path

import pytest

from services import theme_rs_service as service


def _daily_bars(count=50):
    return [
        {"date": f"2026-{((index // 28) % 12) + 1:02d}-{(index % 28) + 1:02d}", "close": 100.0 + index}
        for index in range(count)
    ]


def _row_meta():
    return {
        "proxy_symbols": ["AAA"],
        "proxy_type": "custom",
        "display_name": "Test Theme",
        "representative_symbol": "AAA",
    }


@pytest.mark.asyncio
async def test_canonical_batch_runs_off_event_loop_and_preserves_hits(monkeypatch):
    loop_thread = threading.get_ident()
    worker_threads = []

    def fake_batch(symbols):
        worker_threads.append(threading.get_ident())
        return ({"AAA": [{"date": "2026-01-01", "close": 1.0}] * 40}, ["BBB"])

    monkeypatch.setattr(service, "_canonical_history_batch", fake_batch)
    hits, missing = await service._canonical_history_batch_offloop(["AAA", "BBB"])
    assert worker_threads[0] != loop_thread
    assert list(hits) == ["AAA"]
    assert missing == ["BBB"]


@pytest.mark.asyncio
async def test_proxy_canonical_batch_checks_each_symbol_once_offloop(monkeypatch):
    import services.canonical_history_service as canonical

    loop_thread = threading.get_ident()
    calls = []

    def fake_get_bars(symbol, require_fresh=True):
        calls.append((symbol, require_fresh, threading.get_ident()))
        return {"bars": _daily_bars()} if symbol == "AAA" else None

    monkeypatch.setattr(canonical, "get_bars", fake_get_bars)
    hits, missing = await service._canonical_history_batch_offloop(["AAA", "BBB"])

    assert [call[:2] for call in calls] == [("AAA", False), ("BBB", False)]
    assert all(call[2] != loop_thread for call in calls)
    assert list(hits) == ["AAA"]
    assert missing == ["BBB"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("canonical_bars", "fmp_bars", "tradier_bars", "yf_bars", "expected_source"),
    [
        (_daily_bars(), _daily_bars(), _daily_bars(), _daily_bars(), "canonical"),
        (None, _daily_bars(), _daily_bars(), _daily_bars(), "fmp"),
        (None, [], _daily_bars(), _daily_bars(), "tradier_hist"),
        (None, [], [], _daily_bars(), "yfinance"),
    ],
)
async def test_proxy_provider_priority_is_preserved_without_duplicate_canonical_lookup(
    monkeypatch, canonical_bars, fmp_bars, tradier_bars, yf_bars, expected_source
):
    import services.canonical_history_service as canonical

    canonical_calls = []

    def forbidden_duplicate_get_bars(*args, **kwargs):
        canonical_calls.append((args, kwargs))
        raise AssertionError("canonical lookup must not repeat after batch preparation")

    async def fake_fmp(symbol):
        return fmp_bars

    async def fake_tradier(symbol, days=400):
        return tradier_bars

    async def fake_yf(symbol, days):
        return yf_bars

    monkeypatch.setattr(canonical, "get_bars", forbidden_duplicate_get_bars)
    monkeypatch.setattr(service, "_fetch_fmp_daily_history", fake_fmp)
    monkeypatch.setattr(service, "_fetch_tradier_daily_history", fake_tradier)
    monkeypatch.setattr(service, "_theme_rs_yfinance_history", fake_yf)

    bars, source = await service._fetch_proxy_history(
        "AAA",
        canonical_checked=True,
        canonical_bars=canonical_bars,
    )
    assert source == expected_source
    assert bars
    assert canonical_calls == []


@pytest.mark.asyncio
async def test_stage_analysis_runs_offloop_and_preserves_exact_result(monkeypatch):
    import services.stage_analysis as stage_analysis

    loop_thread = threading.get_ident()
    worker_threads = []
    expected = {
        "stage": 2,
        "stage_label": "S2",
        "stage_score": 88.0,
        "stage_confidence": "high",
        "stage_reason": "test",
        "stage_signals": {"exact": True},
        "stage_updated_at": "2026-09-04T00:00:00+00:00",
        "stage_source": "test",
    }

    def fake_stage(**kwargs):
        worker_threads.append(threading.get_ident())
        return expected

    async def fake_universe(*args, **kwargs):
        return [], {}, "test"

    monkeypatch.setattr(stage_analysis, "analyze_theme_stage", fake_stage)
    monkeypatch.setattr(service, "_build_leader_universe", fake_universe)
    rows = {"AAA": (_daily_bars(), "test"), "SPY": (_daily_bars(), "test")}

    row = await service._build_theme_row(
        "test_theme", _row_meta(), {}, rows, "7D", {}, {}
    )

    assert worker_threads and worker_threads[0] != loop_thread
    assert row["stage"] == expected["stage"]
    assert row["stage_label"] == expected["stage_label"]
    assert row["stage_score"] == expected["stage_score"]
    assert row["stage_confidence"] == expected["stage_confidence"]
    assert row["stage_reason"] == expected["stage_reason"]
    assert row["stage_signals"] is expected["stage_signals"]
    assert row["stage_updated_at"] == expected["stage_updated_at"]
    assert row["stage_source"] == expected["stage_source"]


@pytest.mark.asyncio
async def test_stage_analysis_exception_keeps_existing_fallback(monkeypatch):
    import services.stage_analysis as stage_analysis

    def broken_stage(**kwargs):
        raise RuntimeError("expected stage failure")

    async def fake_universe(*args, **kwargs):
        return [], {}, "test"

    monkeypatch.setattr(stage_analysis, "analyze_theme_stage", broken_stage)
    monkeypatch.setattr(service, "_build_leader_universe", fake_universe)
    rows = {"AAA": (_daily_bars(), "test"), "SPY": (_daily_bars(), "test")}

    row = await service._build_theme_row(
        "test_theme", _row_meta(), {}, rows, "7D", {}, {}
    )

    assert row["stage"] is None
    assert row["stage_label"] == "Unknown"
    assert row["stage_score"] is None
    assert row["stage_confidence"] == "low"
    assert row["stage_reason"] is None
    assert row["stage_signals"] == {}
    assert row["stage_updated_at"] is None
    assert row["stage_source"] == "fallback"


@pytest.mark.asyncio
async def test_theme_rs_yfinance_is_bounded_and_exception_safe(monkeypatch):
    active = 0
    maximum = 0
    completed = []

    async def fake_fetch(symbol, days):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.01)
        active -= 1
        completed.append(symbol)
        if symbol == "BAD":
            raise RuntimeError("expected")
        return [{"date": "2026-01-01", "close": 1.0}]

    monkeypatch.setattr(service, "fetch_etf_history", fake_fetch)
    symbols = [f"S{i}" for i in range(12)] + ["BAD"]
    results = await asyncio.gather(
        *(service._theme_rs_yfinance_history(symbol, 400) for symbol in symbols),
        return_exceptions=True,
    )
    assert maximum <= 4
    assert sorted(completed) == sorted(symbols)
    assert isinstance(results[-1], RuntimeError)


@pytest.mark.asyncio
async def test_global_fmp_block_skips_physical_fmp_and_keeps_fallback(monkeypatch):
    async def forbidden_fmp(symbol):
        raise AssertionError("FMP must not be called")

    async def tradier(symbol, days):
        return [{"date": "2026-01-01", "close": 2.0}]

    monkeypatch.setattr(service, "_fetch_fmp_daily_history", forbidden_fmp)
    monkeypatch.setattr(service, "_fetch_tradier_daily_history", tradier)
    bars, source = await service._fetch_proxy_history(
        "AAA", canonical_checked=True, canonical_bars=None, skip_fmp=True
    )
    assert source == "tradier_hist"
    assert bars[0]["close"] == 2.0


def test_theme_rs_headroom_source_contracts():
    source = (
        Path(__file__).parents[1] / "services" / "theme_rs_service.py"
    ).read_text(encoding="utf-8")
    assert "await _canonical_history_batch_offloop(" in source
    assert "_THEME_RS_YF_SEM = asyncio.Semaphore(4)" in source
    assert "leaders = await asyncio.to_thread(get_theme_leaders_map)" in source
    assert "is_full_historical_blocked()" in source
    assert "asyncio.create_task(_warmup_loop(skip_initial=skip_initial))" in source
    assert source.count("rows = await _run_compute_with_headroom(tf)") == 2


@pytest.mark.asyncio
async def test_row_loop_guarantees_fairness_and_preserves_rows(monkeypatch):
    universe = OrderedDict(
        (f"theme-{index}", {"value": index}) for index in range(40)
    )
    heartbeat_ticks = 0
    stop = False

    async def no_suspend_builder(theme_id, meta, *args, **kwargs):
        deadline = time.perf_counter() + 0.001
        while time.perf_counter() < deadline:
            pass
        return {
            "theme_id": theme_id,
            "value": meta["value"],
            "performance_curve": [meta["value"]],
            "stage": 2,
            "leaders": [theme_id],
            "laggards": [],
        }

    async def heartbeat():
        nonlocal heartbeat_ticks
        while not stop:
            heartbeat_ticks += 1
            await asyncio.sleep(0)

    monkeypatch.setattr(service, "THEME_RS_UNIVERSE", universe)
    monkeypatch.setattr(service, "_build_theme_row", no_suspend_builder)
    service._THEME_RS_DIAG["7D"] = {}
    heartbeat_task = asyncio.create_task(heartbeat())
    rows = await service._build_theme_rows("7D", {}, {}, {}, {}, {}, {})
    stop = True
    await heartbeat_task

    expected = [
        {
            "theme_id": theme_id,
            "value": meta["value"],
            "performance_curve": [meta["value"]],
            "stage": 2,
            "leaders": [theme_id],
            "laggards": [],
        }
        for theme_id, meta in universe.items()
    ]
    assert rows == expected
    assert heartbeat_ticks >= len(universe)
    assert service._THEME_RS_DIAG["7D"]["theme_row_yield_count"] == len(universe)


@pytest.mark.asyncio
async def test_historical_computes_are_globally_serial_and_all_complete(monkeypatch):
    active = 0
    maximum = 0
    completed = []

    async def fake_compute(tf):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.005)
        completed.append(tf)
        active -= 1
        return [{"timeframe": tf}]

    monkeypatch.setattr(service, "_HISTORICAL_COMPUTE_GATE", asyncio.Semaphore(1))
    monkeypatch.setattr(service, "_compute", fake_compute)
    service._HISTORICAL_COMPUTE_INFLIGHT = 0
    service._HISTORICAL_COMPUTE_MAX_INFLIGHT = 0
    timeframes = ["7D", "30D", "YTD", "1Y", "5Y"]
    results = await asyncio.gather(
        *(service._run_compute_with_headroom(tf) for tf in timeframes)
    )
    assert maximum == 1
    assert completed == timeframes
    assert [result[0]["timeframe"] for result in results] == timeframes


@pytest.mark.asyncio
async def test_same_timeframe_refresh_keeps_existing_single_flight(monkeypatch):
    entered = 0

    async def fake_run(tf):
        nonlocal entered
        entered += 1
        await asyncio.sleep(0.02)
        return []

    monkeypatch.setattr(service, "_run_compute_with_headroom", fake_run)
    monkeypatch.setattr(service, "_refresh_locks", {})
    service._last_computed["30D"] = 0
    first = asyncio.create_task(service._locked_refresh("30D"))
    await asyncio.sleep(0)
    await service._locked_refresh("30D")
    await first
    assert entered == 1


@pytest.mark.asyncio
async def test_1d_does_not_wait_for_historical_gate(monkeypatch):
    async def fake_compute(tf):
        return [{"timeframe": tf}]

    gate = asyncio.Semaphore(1)
    monkeypatch.setattr(service, "_HISTORICAL_COMPUTE_GATE", gate)
    monkeypatch.setattr(service, "_compute", fake_compute)
    await gate.acquire()
    try:
        result = await asyncio.wait_for(
            service._run_compute_with_headroom("1D"), timeout=0.05
        )
    finally:
        gate.release()
    assert result == [{"timeframe": "1D"}]


@pytest.mark.asyncio
async def test_stale_lkg_request_returns_without_waiting_for_compute(monkeypatch):
    release = asyncio.Event()

    async def blocked_refresh(tf, force=False):
        await release.wait()

    monkeypatch.setattr(service.cache, "get", lambda key: None)
    monkeypatch.setattr(
        service,
        "_load_lkg",
        lambda: [{"theme_id": "t", "performance": {}, "proxy_source_health": {}}],
    )
    monkeypatch.setattr(service, "_locked_refresh", blocked_refresh)
    monkeypatch.setattr(
        service, "_validate_basket_hashes", lambda payload, **kwargs: (payload, 0)
    )
    result = await asyncio.wait_for(
        service._get_theme_rs_data_raw("30D", force=False), timeout=0.05
    )
    release.set()
    await asyncio.sleep(0)
    assert result["source"] == "lkg_stale_revalidating"