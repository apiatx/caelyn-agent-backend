from __future__ import annotations

import asyncio
import threading
import time
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from data import options_flow_engine as flow
from data import tradier_flow_engine as tradier


class _ExpiryProvider:
    async def get_option_expirations(self, _symbol):
        return [(date.today() + timedelta(days=14)).isoformat()]


def _engine_defaults() -> dict:
    return {
        "inspect_concurrency": 6,
        "inspect_inter_ticker_sleep": 0,
        "min_dte": 7,
        "max_dte": 45,
        "max_expirations_per_ticker": 2,
        "master_stage2_limit": 0,
    }


def _candidates(count: int) -> list[dict]:
    return [{"ticker": f"T{i}", "price": 100.0} for i in range(count)]


@pytest.mark.asyncio
async def test_stage2b_ticker_enrichment_is_bounded_to_two():
    engine = flow.OptionsFlowEngine.__new__(flow.OptionsFlowEngine)
    engine.defaults = _engine_defaults()
    engine._shared_sem = None
    engine._expiry_cache = {}
    engine.data = SimpleNamespace(public_com=_ExpiryProvider())

    async def inspect(candidate, *_args, **_kwargs):
        return {"ticker": candidate["ticker"]}

    active = 0
    maximum = 0

    async def enrich(_result):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.01)
        active -= 1

    engine._inspect_one_ticker = inspect
    engine._enrich_polygon_async = enrich

    results = await engine._inspect_shortlist(_candidates(12), {}, tab="master")

    assert len(results) == 12
    assert maximum == flow.OPTIONS_HISTORY_MAX_DB_CONCURRENCY == 2


@pytest.mark.asyncio
async def test_real_stage2b_path_has_no_nested_db_fanout(monkeypatch):
    engine = tradier.TradierFlowEngine.__new__(tradier.TradierFlowEngine)
    engine.defaults = _engine_defaults()
    engine._shared_sem = None
    engine._expiry_cache = {}
    engine.data = SimpleNamespace(public_com=_ExpiryProvider())

    async def inspect(candidate, *_args, **_kwargs):
        return {
            "ticker": candidate["ticker"],
            "call_volume": 700,
            "put_volume": 300,
            "composite_score": 10.0,
            "top_contracts": [
                {"contract_symbol": f"{candidate['ticker']}-C{i}"}
                for i in range(5)
            ],
        }

    engine._inspect_one_ticker = inspect

    lock = threading.Lock()
    active = 0
    maximum = 0

    def db_call(value):
        nonlocal active, maximum
        with lock:
            active += 1
            maximum = max(maximum, active)
        time.sleep(0.005)
        with lock:
            active -= 1
        return value

    monkeypatch.setattr(
        tradier,
        "get_options_volume_summary",
        lambda *_args: db_call(
            {
                "call_total_volume": 100,
                "call_avg_daily_vol": 10,
                "put_avg_daily_vol": 10,
            }
        ),
    )
    monkeypatch.setattr(
        tradier,
        "get_latest_technicals",
        lambda symbol: db_call({"ticker": symbol, "rsi": {"value": 55}}),
    )
    monkeypatch.setattr(
        tradier,
        "_polygon_iv_context",
        lambda symbol: db_call({"contract_symbol": symbol, "iv_percentile": 60}),
    )

    results = await engine._inspect_shortlist(_candidates(8), {}, tab="master")

    assert maximum <= 2
    assert [result["ticker"] for result in results] == [f"T{i}" for i in range(8)]
    for result in results:
        assert result["historic_volume"]["call_total_volume"] == 100
        assert result["technicals"]["rsi"]["value"] == 55
        assert [c["polygon_history"]["contract_symbol"] for c in result["top_contracts"]] == [
            f"{result['ticker']}-C{i}" for i in range(5)
        ]
        assert result["composite_score"] == 18.0


@pytest.mark.asyncio
async def test_snapshot_store_runs_off_event_loop_and_preserves_rows(monkeypatch):
    engine = flow.OptionsFlowEngine.__new__(flow.OptionsFlowEngine)
    engine.defaults = {
        "options_inspection_limit": 10,
        "top_contracts_per_ticker": 5,
    }
    engine.weights = {}

    snapshot_rows = [{"underlying": "AAA", "contract_symbol": "AAA-C1"}]
    row = {
        "ticker": "AAA",
        "composite_score": 50,
        "call_volume": 10,
        "put_volume": 5,
        "ranked_contracts": [],
        "snapshot_rows": list(snapshot_rows),
    }

    async def inspect(*_args, **_kwargs):
        engine._s2_neutral_syms = set()
        engine._s2_pending_chain_syms = set()
        return [row]

    engine._inspect_shortlist = inspect
    engine._macro_context_summary = lambda _macro: {}
    main_thread = threading.get_ident()
    observed = {}

    def store(rows):
        observed["thread"] = threading.get_ident()
        observed["rows"] = list(rows)
        return 7

    monkeypatch.setattr(flow, "store_options_flow_snapshots", store)

    result = await engine.run_live_scan(
        prefilter_snapshot={"candidates": [{"ticker": "AAA"}]},
        tab="master",
    )

    assert observed["thread"] != main_thread
    assert observed["rows"] == snapshot_rows
    assert result["pipeline_stats"]["history_snapshot_rows"] == 7
    assert result["market_summary"]["history_snapshots_written"] == 7


@pytest.mark.asyncio
async def test_initial_enrichment_failure_keeps_result_unmodified(monkeypatch):
    engine = tradier.TradierFlowEngine.__new__(tradier.TradierFlowEngine)
    calls = []
    original = {
        "ticker": "AAA",
        "call_volume": 700,
        "put_volume": 300,
        "composite_score": 10.0,
        "top_contracts": [],
    }

    def volume(*_args):
        calls.append("volume")
        return {
            "call_total_volume": 100,
            "call_avg_daily_vol": 10,
            "put_avg_daily_vol": 10,
        }

    def technicals(*_args):
        calls.append("technicals")
        raise RuntimeError("DB unavailable")

    monkeypatch.setattr(tradier, "get_options_volume_summary", volume)
    monkeypatch.setattr(tradier, "get_latest_technicals", technicals)

    result = await engine._enrich_polygon_async(dict(original))

    assert calls == ["volume", "technicals"]
    assert result == original


@pytest.mark.asyncio
async def test_snapshot_store_exception_remains_degraded_not_fatal(monkeypatch):
    engine = flow.OptionsFlowEngine.__new__(flow.OptionsFlowEngine)
    engine.defaults = {"options_inspection_limit": 10}
    engine.weights = {}
    row = {
        "ticker": "AAA",
        "composite_score": 50,
        "call_volume": 10,
        "put_volume": 5,
        "ranked_contracts": [],
        "snapshot_rows": [{"underlying": "AAA"}],
    }

    async def inspect(*_args, **_kwargs):
        engine._s2_neutral_syms = set()
        engine._s2_pending_chain_syms = set()
        return [row]

    engine._inspect_shortlist = inspect
    engine._macro_context_summary = lambda _macro: {}

    def failing_store(_rows):
        raise RuntimeError("write failed")

    monkeypatch.setattr(flow, "store_options_flow_snapshots", failing_store)

    result = await engine.run_live_scan(
        prefilter_snapshot={"candidates": [{"ticker": "AAA"}]},
        tab="master",
    )

    assert result["pipeline_stats"]["history_snapshot_rows"] == 0
    assert result["market_summary"]["history_snapshots_written"] == 0
    assert result["market_summary"]["history_metrics_live"] is False
    assert "snapshot_store:RuntimeError" in result["pipeline_stats"]["degraded_sources"]


def test_pg_pool_size_remains_five():
    source = (
        Path(__file__).resolve().parent.parent / "data" / "pg_storage.py"
    ).read_text()
    assert "ThreadedConnectionPool(" in source
    assert "1, 5, _DATABASE_URL" in source