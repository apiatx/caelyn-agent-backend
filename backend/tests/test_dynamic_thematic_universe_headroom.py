import asyncio
from types import SimpleNamespace

import pytest

from services import dynamic_thematic_universe as dtu


class _Response:
    status_code = 200

    def __init__(self, peers):
        self._peers = peers

    def json(self):
        return self._peers


@pytest.mark.asyncio
async def test_duplicate_etf_and_anchor_work_is_coalesced(monkeypatch):
    snap = {
        "snapshot_status": "fresh",
        "active_themes": [
            {"name": "A", "related_etfs": ["DUP"], "related_tickers": ["ANCHOR"]},
            {"name": "B", "related_etfs": ["DUP"], "related_tickers": ["ANCHOR"]},
        ],
        "emerging_themes": [],
    }
    monkeypatch.setattr(
        "services.thematic_context_provider.get_shared_thematic_context",
        lambda: snap,
    )
    monkeypatch.setattr(dtu, "_augment_etfs_from_universe", lambda _name, etfs: etfs)
    async def no_x():
        return [], "ok"

    monkeypatch.setattr(dtu, "_x_consensus_tickers", no_x)
    monkeypatch.setenv("FMP_API_KEY", "test")

    etf_calls = 0

    async def fake_holdings(symbol, **kwargs):
        nonlocal etf_calls
        etf_calls += 1
        diagnostics = kwargs.get("diagnostics")
        if diagnostics is not None:
            diagnostics["cache_hits"] = diagnostics.get("cache_hits", 0) + 1
        return {"holdings": [{"ticker": "SHARED", "weight": 1}]}

    monkeypatch.setattr(
        "services.sector_rotation.etf_holdings_service.get_etf_holdings",
        fake_holdings,
    )

    peer_calls = 0

    async def fake_get(*_args, **_kwargs):
        nonlocal peer_calls
        peer_calls += 1
        return _Response(["PEER"])

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        get = fake_get

    monkeypatch.setattr("httpx.AsyncClient", lambda **_kwargs: FakeClient())

    result = await dtu._build_dynamic_universe(False, True, 150)

    assert etf_calls == 1
    assert peer_calls == 1
    assert result["sources_by_ticker"]["SHARED"] == ["etf_holding:DUP"]
    assert result["sources_by_ticker"]["PEER"] == ["peer:ANCHOR"]
    assert "static_anchor:A" in result["sources_by_ticker"]["ANCHOR"]
    assert "static_anchor:B" in result["sources_by_ticker"]["ANCHOR"]


@pytest.mark.asyncio
async def test_provider_concurrency_is_bounded_and_output_limit_preserved(monkeypatch):
    themes = [
        {
            "name": f"T{i}",
            "related_etfs": [f"E{i}"],
            "related_tickers": [f"A{i}"],
        }
        for i in range(12)
    ]
    monkeypatch.setattr(
        "services.thematic_context_provider.get_shared_thematic_context",
        lambda: {
            "snapshot_status": "fresh",
            "active_themes": themes,
            "emerging_themes": [],
        },
    )
    monkeypatch.setattr(dtu, "_augment_etfs_from_universe", lambda _name, etfs: etfs)
    async def fake_x():
        return ["X1", "X2"], "ok"

    monkeypatch.setattr(dtu, "_x_consensus_tickers", fake_x)
    monkeypatch.setenv("FMP_API_KEY", "test")

    inflight = 0
    observed_max = 0

    async def enter():
        nonlocal inflight, observed_max
        inflight += 1
        observed_max = max(observed_max, inflight)
        await asyncio.sleep(0.01)
        inflight -= 1

    async def fake_holdings(symbol, **kwargs):
        gate = kwargs.get("live_gate")
        observer = kwargs.get("live_observer")
        async with gate:
            if observer:
                observer(1)
            try:
                await enter()
            finally:
                if observer:
                    observer(-1)
        return {"holdings": [{"ticker": f"H{symbol[1:]}", "weight": 1}]}

    monkeypatch.setattr(
        "services.sector_rotation.etf_holdings_service.get_etf_holdings",
        fake_holdings,
    )

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, _url, params):
            await enter()
            return _Response([f"P{params['symbol'][1:]}"])

    monkeypatch.setattr("httpx.AsyncClient", lambda **_kwargs: FakeClient())

    result = await dtu._build_dynamic_universe(False, True, 7)

    assert observed_max <= 4
    assert result["ticker_count"] == 7
    assert result["tickers"] == result["tickers"][:7]
    assert set(result) >= {
        "tickers",
        "sources_by_ticker",
        "theme_map",
        "snapshot_status",
        "source_health",
        "ticker_count",
    }


@pytest.mark.asyncio
async def test_force_refresh_rebuilds_but_coalesces_within_each_build(monkeypatch):
    monkeypatch.setattr(
        "services.thematic_context_provider.get_shared_thematic_context",
        lambda: {
            "snapshot_status": "fresh",
            "active_themes": [
                {"name": "A", "related_etfs": ["DUP"], "related_tickers": []},
                {"name": "B", "related_etfs": ["DUP"], "related_tickers": []},
            ],
            "emerging_themes": [],
        },
    )
    monkeypatch.setattr(dtu, "_augment_etfs_from_universe", lambda _name, etfs: etfs)
    async def no_x():
        return [], "ok"

    monkeypatch.setattr(dtu, "_x_consensus_tickers", no_x)
    calls = 0

    async def fake_holdings(_symbol, **_kwargs):
        nonlocal calls
        calls += 1
        return {"holdings": [{"ticker": "ONE"}]}

    monkeypatch.setattr(
        "services.sector_rotation.etf_holdings_service.get_etf_holdings",
        fake_holdings,
    )
    monkeypatch.setattr(
        "data.cache.cache",
        SimpleNamespace(get=lambda _key: {"tickers": ["cached"]}, set=lambda *_args: None),
    )

    await dtu.get_dynamic_thematic_universe(force_refresh=True)
    await dtu.get_dynamic_thematic_universe(force_refresh=True)
    assert calls == 2


@pytest.mark.asyncio
async def test_attribution_is_theme_ordered_not_completion_order(monkeypatch):
    monkeypatch.setattr(
        "services.thematic_context_provider.get_shared_thematic_context",
        lambda: {
            "snapshot_status": "fresh",
            "active_themes": [
                {"name": "First", "related_etfs": ["SLOW"], "related_tickers": ["A1"]},
                {"name": "Second", "related_etfs": ["FAST"], "related_tickers": ["A2"]},
            ],
            "emerging_themes": [],
        },
    )
    monkeypatch.setattr(dtu, "_augment_etfs_from_universe", lambda _name, etfs: etfs)

    async def no_x():
        return [], "ok"

    monkeypatch.setattr(dtu, "_x_consensus_tickers", no_x)
    monkeypatch.delenv("FMP_API_KEY", raising=False)

    async def fake_holdings(symbol, **_kwargs):
        if symbol == "SLOW":
            await asyncio.sleep(0.02)
        return {"holdings": [{"ticker": "SHARED", "weight": 1}]}

    monkeypatch.setattr(
        "services.sector_rotation.etf_holdings_service.get_etf_holdings",
        fake_holdings,
    )

    result = await dtu._build_dynamic_universe(False, True, 150)

    assert result["tickers"][0] == "SHARED"
    assert result["theme_map"]["SHARED"]["theme_name"] == "First"
    assert result["sources_by_ticker"]["SHARED"] == [
        "etf_holding:SLOW",
        "etf_holding:FAST",
    ]