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


def _spot_response(coin: str = "@1", display: str = "HFUN") -> list:
    return [
        {
            "universe": [{"name": f"{display}/USDC", "tokens": [0, 1]}],
            "tokens": [
                {"name": display, "index": 0},
                {"name": "USDC", "index": 1},
            ],
        },
        [{"coin": coin}],
    ]


@pytest.mark.asyncio
async def test_refresh_reconciles_new_removed_partial_and_failed_sources(monkeypatch):
    state = HyperliquidState()
    state.assets = {
        "OLD": _asset("OLD"),
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
    client.get_meta_and_asset_ctxs.return_value = _perp_response("BTC", "NEW")
    client.get_spot_meta_and_asset_ctxs.return_value = _spot_response()

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

    assert set(state.assets) == {"BTC", "NEW", "@1", "para:CRDO", "xyz:KEEP"}
    assert "OLD" not in state.assets
    assert "@9" not in state.assets
    assert "para:OLD" not in state.assets
    assert state.assets["para:CRDO"].mark_px is None
    assert state.assets["para:CRDO"].market_status == "active"
    assert state.assets["xyz:KEEP"].display_name == "KEEP"
    assert state.universe_allowlist == set(state.assets)


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
    client.get_spot_meta_and_asset_ctxs.return_value = _spot_response()
    client.get_all_mids.return_value = {}
    monkeypatch.setattr(websocket_manager, "_save_hip3_cache", lambda current: None)

    await websocket_manager._refresh_discovered_universe(state, client)

    assert set(state.assets) == {"ETH", "@1", "xyz:KEEP"}
    assert state.universe_allowlist == set(state.assets)


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
    client.get_spot_meta_and_asset_ctxs.return_value = [
        {"universe": [], "tokens": []},
        [],
    ]
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
    client.get_spot_meta_and_asset_ctxs.return_value = _spot_response()
    client.get_all_mids.side_effect = RuntimeError("mids unavailable")
    monkeypatch.setattr(
        websocket_manager,
        "run_full_feature_pass",
        lambda current: (_ for _ in ()).throw(RuntimeError("scores unavailable")),
    )
    monkeypatch.setattr(websocket_manager, "_save_hip3_cache", lambda current: None)

    await websocket_manager._refresh_discovered_universe(state, client)

    assert set(state.assets) == {"NEW", "@1"}
    assert set(state.lkg_assets) == {"NEW", "@1"}
    assert state.universe_allowlist == {"NEW", "@1"}


@pytest.mark.asyncio
async def test_malformed_main_and_spot_responses_preserve_source_lkg(monkeypatch):
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
    client.get_spot_meta_and_asset_ctxs.return_value = [{"not_universe": []}]
    client.get_all_mids.return_value = {}
    monkeypatch.setattr(websocket_manager, "run_full_feature_pass", lambda current: None)
    monkeypatch.setattr(websocket_manager, "_save_hip3_cache", lambda current: None)

    await websocket_manager._refresh_discovered_universe(state, client)

    assert set(state.assets) == {"BTC", "@9"}
    assert state.universe_allowlist == {"BTC", "@9"}


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
    client.get_spot_meta_and_asset_ctxs.return_value = [
        {"universe": [], "tokens": []}
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
    client.get_spot_meta_and_asset_ctxs.return_value = [
        {"universe": [], "tokens": []},
        [],
    ]
    client.get_all_mids.return_value = {}
    monkeypatch.setattr(websocket_manager, "run_full_feature_pass", lambda current: None)
    monkeypatch.setattr(websocket_manager, "_save_hip3_cache", lambda current: None)

    await websocket_manager._refresh_discovered_universe(state, client)

    assert set(state.assets) == {"BTC", "xyz:KEEP"}
    assert set(state.lkg_assets) == {"BTC", "xyz:KEEP"}
    assert state.universe_allowlist == {"BTC", "xyz:KEEP"}


@pytest.mark.asyncio
async def test_snapshot_keeps_low_volume_spot_and_limit_is_presentation_only(monkeypatch):
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

    assert {row["canonicalCoinId"] for row in complete["rows"]} == {"BTC", "@1"}
    assert len(limited["rows"]) == 1
    assert state.universe_allowlist == {"BTC", "@1"}


def test_matrix_retains_spot_and_canonical_symbol_collisions():
    assets = [
        _asset("PURR", display="PURR"),
        _asset("PURR/USDC", display="PURR", market_type="spot"),
    ]
    matrix = build_market_matrix(assets)
    rows = matrix["tabs"]["crypto"]["assets"]
    assert matrix["all_assets_count"] == 2
    assert matrix["tabs"]["crypto"]["count"] == 2
    assert {row["canonical_coin_id"] for row in rows} == {"PURR", "PURR/USDC"}


@pytest.mark.parametrize("symbol", ["CRDO", "AVGO", "IREN"])
def test_para_public_equities_are_not_classified_as_crypto(symbol):
    asset = _asset(f"para:{symbol}", display=symbol, dex="hl-para")
    assert categorize_asset(asset) == ("stocks_etfs", "annotation")


@pytest.mark.parametrize("symbol", ["BTC", "ETH", "SOL", "HYPE"])
def test_crypto_majors_survive_matrix_pipeline(symbol):
    matrix = build_market_matrix([_asset(symbol)])
    rows = matrix["tabs"]["crypto"]["assets"]
    assert [row["canonical_coin_id"] for row in rows] == [symbol]