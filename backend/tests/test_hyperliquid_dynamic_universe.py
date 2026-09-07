from unittest.mock import AsyncMock

import pytest

from services.hyperliquid import router
from services.hyperliquid.categorizer import build_market_matrix, categorize_asset
from services.hyperliquid.models import ScreenerAsset
from services.hyperliquid.state import HyperliquidState
import services.hyperliquid.websocket_manager as websocket_manager


def _asset(
    coin: str,
    *,
    display: str | None = None,
    market_type: str = "perp",
    dex: str = "hyperliquid",
    volume: float | None = None,
    tags: list[str] | None = None,
) -> ScreenerAsset:
    return ScreenerAsset(
        coin=coin,
        display_name=display or coin,
        display_symbol=display or coin,
        canonical_coin_id=coin,
        is_listed_on_hyperliquid=True,
        market_type=market_type,
        dex=dex,
        tags=tags or [market_type],
        day_ntl_vlm=volume,
        market_status="active",
    )


def _perp_response(*names: str) -> list:
    return [
        {"universe": [{"name": name} for name in names]},
        [{} for _ in names],
    ]


@pytest.mark.asyncio
async def test_refresh_reconciles_new_removed_partial_and_failed_sources(monkeypatch):
    state = HyperliquidState()
    state.assets = {
        "OLD": _asset("OLD"),
        "DELISTED": _asset("DELISTED"),
        "@9": _asset("@9", display="OLDSPOT", market_type="spot"),
        "para:OLD": _asset("para:OLD", display="OLD", dex="hl-para"),
        "xyz:KEEP": _asset("xyz:KEEP", display="KEEP", dex="hl-xyz"),
    }
    state.perp_allowlist = {"OLD"}
    state.spot_allowlist = {"@9"}
    state.universe_allowlist = set(state.assets)

    client = AsyncMock()
    client.get_all_perp_metas.return_value = [
        {"universe": [{"name": "BTC"}, {"name": "NEW"}]},
        {"universe": [{"name": "para:CRDO"}]},
        {"universe": [{"name": "xyz:KEEP"}]},
    ]
    client.get_meta_and_asset_ctxs.return_value = [
        {
            "universe": [
                {"name": "BTC"},
                {"name": "NEW"},
                {"name": "DELISTED", "isDelisted": True},
            ]
        },
        [{}, {}, {}],
    ]

    async def dex_result(prefix):
        if prefix == "xyz":
            raise RuntimeError("temporary xyz failure")
        return _perp_response("para:CRDO")

    client.get_dex_meta_and_asset_ctxs.side_effect = dex_result
    client.get_all_mids.return_value = {}
    monkeypatch.setattr(
        websocket_manager,
        "run_full_feature_pass",
        lambda current: setattr(current, "lkg_assets", dict(current.assets)),
    )
    monkeypatch.setattr(websocket_manager, "_save_hip3_cache", lambda current: None)

    await websocket_manager._refresh_discovered_universe(state, client)

    assert set(state.assets) == {"BTC", "NEW", "para:CRDO", "xyz:KEEP"}
    assert "OLD" not in state.assets
    assert "DELISTED" not in state.assets
    assert "@9" not in state.assets
    assert "para:OLD" not in state.assets
    assert state.assets["para:CRDO"].mark_px is None
    assert state.assets["para:CRDO"].market_status == "active"
    assert state.assets["xyz:KEEP"].display_name == "KEEP"
    assert state.universe_allowlist == set(state.assets)
    assert state.spot_allowlist == set()
    client.get_spot_meta_and_asset_ctxs.assert_not_awaited()


@pytest.mark.asyncio
async def test_discovery_failure_preserves_last_known_good(monkeypatch):
    state = HyperliquidState()
    state.assets = {
        "BTC": _asset("BTC"),
        "xyz:KEEP": _asset("xyz:KEEP", display="KEEP", dex="hl-xyz"),
    }
    state.universe_allowlist = set(state.assets)
    client = AsyncMock()
    client.get_all_perp_metas.side_effect = RuntimeError("temporary outage")
    client.get_meta_and_asset_ctxs.return_value = _perp_response("ETH")
    client.get_all_mids.return_value = {}
    monkeypatch.setattr(websocket_manager, "_save_hip3_cache", lambda current: None)

    await websocket_manager._refresh_discovered_universe(state, client)

    assert set(state.assets) == {"ETH", "xyz:KEEP"}
    assert state.universe_allowlist == set(state.assets)
    client.get_spot_meta_and_asset_ctxs.assert_not_awaited()


@pytest.mark.asyncio
async def test_successful_directory_refresh_removes_absent_namespace(monkeypatch):
    state = HyperliquidState()
    state.assets = {
        "BTC": _asset("BTC"),
        "gone:OLD": _asset("gone:OLD", display="OLD", dex="hl-gone"),
    }
    state.universe_allowlist = set(state.assets)
    client = AsyncMock()
    client.get_all_perp_metas.return_value = [{"universe": [{"name": "BTC"}]}]
    client.get_meta_and_asset_ctxs.return_value = _perp_response("BTC")
    client.get_all_mids.return_value = {}
    monkeypatch.setattr(websocket_manager, "run_full_feature_pass", lambda current: None)
    monkeypatch.setattr(websocket_manager, "_save_hip3_cache", lambda current: None)

    await websocket_manager._refresh_discovered_universe(state, client)

    assert set(state.assets) == {"BTC"}
    assert "gone:OLD" not in state.universe_allowlist


@pytest.mark.asyncio
async def test_optional_enrichment_failure_does_not_hide_new_membership(monkeypatch):
    state = HyperliquidState()
    client = AsyncMock()
    client.get_all_perp_metas.return_value = [{"universe": [{"name": "NEW"}]}]
    client.get_meta_and_asset_ctxs.return_value = _perp_response("NEW")
    client.get_all_mids.side_effect = RuntimeError("mids unavailable")
    monkeypatch.setattr(
        websocket_manager,
        "run_full_feature_pass",
        lambda current: (_ for _ in ()).throw(RuntimeError("scores unavailable")),
    )
    monkeypatch.setattr(websocket_manager, "_save_hip3_cache", lambda current: None)

    await websocket_manager._refresh_discovered_universe(state, client)

    assert set(state.assets) == {"NEW"}
    assert set(state.lkg_assets) == {"NEW"}
    assert state.universe_allowlist == {"NEW"}


@pytest.mark.asyncio
async def test_malformed_main_preserves_perp_lkg_but_evicts_stale_spot(monkeypatch):
    state = HyperliquidState()
    state.assets = {
        "BTC": _asset("BTC"),
        "@9": _asset("@9", display="OLDSPOT", market_type="spot"),
    }
    state.perp_allowlist = {"BTC"}
    state.spot_allowlist = {"@9"}
    state.universe_allowlist = set(state.assets)
    client = AsyncMock()
    client.get_all_perp_metas.return_value = [{"universe": [{"name": "BTC"}]}]
    client.get_meta_and_asset_ctxs.return_value = []
    client.get_all_mids.return_value = {}
    monkeypatch.setattr(websocket_manager, "run_full_feature_pass", lambda current: None)
    monkeypatch.setattr(websocket_manager, "_save_hip3_cache", lambda current: None)

    await websocket_manager._refresh_discovered_universe(state, client)

    assert set(state.assets) == {"BTC"}
    assert set(state.lkg_assets) == {"BTC"}
    assert state.universe_allowlist == {"BTC"}
    assert state.spot_allowlist == set()
    client.get_spot_meta_and_asset_ctxs.assert_not_awaited()


@pytest.mark.asyncio
async def test_metadata_only_main_response_adds_nullable_active_listing(monkeypatch):
    state = HyperliquidState()
    client = AsyncMock()
    client.get_all_perp_metas.return_value = [
        {"universe": [{"name": "NEW", "maxLeverage": 3}]}
    ]
    client.get_meta_and_asset_ctxs.return_value = [
        {"universe": [{"name": "NEW", "maxLeverage": 3}]}
    ]
    client.get_all_mids.return_value = {}
    monkeypatch.setattr(websocket_manager, "run_full_feature_pass", lambda current: None)
    monkeypatch.setattr(websocket_manager, "_save_hip3_cache", lambda current: None)

    await websocket_manager._refresh_discovered_universe(state, client)

    assert set(state.assets) == {"NEW"}
    assert state.assets["NEW"].mark_px is None
    assert state.assets["NEW"].market_status == "active"


@pytest.mark.asyncio
async def test_empty_hip3_directory_preserves_namespace_lkg(monkeypatch):
    state = HyperliquidState()
    state.assets = {
        "BTC": _asset("BTC"),
        "xyz:KEEP": _asset("xyz:KEEP", display="KEEP", dex="hl-xyz"),
    }
    state.perp_allowlist = {"BTC"}
    state.universe_allowlist = set(state.assets)
    client = AsyncMock()
    client.get_all_perp_metas.return_value = []
    client.get_meta_and_asset_ctxs.return_value = _perp_response("BTC")
    client.get_all_mids.return_value = {}
    monkeypatch.setattr(websocket_manager, "run_full_feature_pass", lambda current: None)
    monkeypatch.setattr(websocket_manager, "_save_hip3_cache", lambda current: None)

    await websocket_manager._refresh_discovered_universe(state, client)

    assert set(state.assets) == {"BTC", "xyz:KEEP"}
    assert set(state.lkg_assets) == {"BTC", "xyz:KEEP"}
    assert state.universe_allowlist == {"BTC", "xyz:KEEP"}


@pytest.mark.asyncio
async def test_snapshot_contains_only_perps_and_reports_zero_spots(monkeypatch):
    state = HyperliquidState()
    state.assets = {
        "BTC": _asset("BTC", volume=1_000_000),
        "@1": _asset("@1", display="HFUN", market_type="spot", volume=1),
    }
    state.universe_allowlist = set(state.assets)
    state.lkg_assets = dict(state.assets)
    monkeypatch.setattr(router, "_get_state", lambda: state)

    complete = await router.get_snapshot(limit=1000)
    limited = await router.get_snapshot(limit=1)
    filters = await router.get_filters()

    assert {row["canonicalCoinId"] for row in complete["rows"]} == {"BTC"}
    assert all(row["marketType"] == "perp" for row in complete["rows"])
    assert complete["meta"]["totalAssets"] == 1
    assert len(limited["rows"]) == 1
    assert filters["totalAssets"] == 1
    assert filters["perpCount"] == 1
    assert filters["spotCount"] == 0
    assert state.universe_allowlist == {"BTC", "@1"}


def test_matrix_contains_only_perps_and_reports_zero_spots():
    assets = [
        _asset("PURR", display="PURR"),
        _asset("PURR/USDC", display="PURR", market_type="spot"),
    ]
    matrix = build_market_matrix(assets)
    rows = [
        row
        for tab in matrix["tabs"].values()
        for row in tab["assets"]
    ]
    assert matrix["all_assets_count"] == 1
    assert all(row["market_type"] == "perp" for row in rows)
    assert {row["canonical_coin_id"] for row in rows} == {"PURR"}


@pytest.mark.parametrize("symbol", ["CRDO", "AVGO", "IREN"])
def test_para_public_equities_are_not_classified_as_crypto(symbol):
    asset = _asset(f"para:{symbol}", display=symbol, dex="hl-para")
    assert categorize_asset(asset) == ("stocks_etfs", "annotation")


@pytest.mark.parametrize("symbol", ["BTC", "ETH", "SOL", "HYPE"])
def test_crypto_majors_survive_matrix_pipeline(symbol):
    matrix = build_market_matrix([_asset(symbol)])
    rows = matrix["tabs"]["crypto"]["assets"]
    assert [row["canonical_coin_id"] for row in rows] == [symbol]