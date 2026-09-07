import asyncio
import math
import time

import pytest
from fastapi import HTTPException

from services.hyperliquid import router
import services.hyperliquid.tsmom as tsmom
import services.hyperliquid.websocket_manager as websocket_manager
from services.hyperliquid.client import HyperliquidRestClient
from services.hyperliquid.models import ScreenerAsset
from services.hyperliquid.state import HyperliquidState
from services.hyperliquid.tsmom import compute_tsmom_signals


def _asset(
    coin: str,
    *,
    market_type: str = "perp",
    dex: str = "hyperliquid",
    display: str | None = None,
    funding: float | None = 0.0,
    volume: float = 1_000_000,
) -> ScreenerAsset:
    return ScreenerAsset(
        coin=coin,
        canonical_coin_id=coin,
        display_name=display or coin,
        display_symbol=display or coin,
        market_type=market_type,
        market_status="active",
        dex=dex,
        tags=[market_type],
        funding=funding,
        day_ntl_vlm=volume,
        is_listed_on_hyperliquid=True,
    )


def _candles(count: int, *, forming: bool = False) -> list[dict]:
    now_ms = int(time.time() * 1000)
    closes = [100.0]
    for index in range(1, count):
        daily_return = 0.012 if index % 3 else -0.004
        closes.append(closes[-1] * math.exp(daily_return))
    bars = []
    for index, close in enumerate(closes):
        close_ms = now_ms - (count - index) * 86_400_000
        bars.append({
            "t": close_ms - 86_399_999,
            "T": close_ms,
            "c": str(close),
        })
    if forming:
        bars.append({
            "t": now_ms - 1_000,
            "T": now_ms + 86_399_000,
            "c": str(closes[-1] * 1.5),
        })
    return bars


def _state(*assets: ScreenerAsset) -> HyperliquidState:
    state = HyperliquidState()
    state.assets = {asset.coin: asset for asset in assets}
    state.universe_allowlist = set(state.assets)
    state.is_ready = True
    return state


def test_crypto_mode_preserves_default_and_excludes_stocks_and_spot():
    btc = _asset("BTC")
    main_specialty = _asset("GOLD")
    hip3_crypto = _asset("hyna:BTC", dex="hl-hyna", display="BTC")
    crdo = _asset("para:CRDO", dex="hl-para", display="CRDO")
    spot = _asset("@1", market_type="spot", display="HFUN")
    state = _state(btc, main_specialty, hip3_crypto, crdo, spot)
    state.add_candles("BTC", "1d", _candles(100))
    state.add_candles("GOLD", "1d", _candles(100))
    state.add_candles("hyna:BTC", "1d", _candles(100))
    state.add_candles("para:CRDO", "1d", _candles(40))
    state.add_candles("@1", "1d", _candles(100))

    payload = compute_tsmom_signals(state)

    assert {row["coin"] for row in payload["signals"]} == {"BTC", "GOLD"}
    assert payload["meta"]["market"] == "crypto"
    assert payload["meta"]["lookbacks"] == [10, 30, 90]


def test_stock_mode_renormalizes_available_windows_without_zero_poisoning():
    crdo = _asset(
        "para:CRDO",
        dex="hl-para",
        display="CRDO",
        funding=0.00001,
    )
    state = _state(crdo)
    state.add_candles(crdo.coin, "1d", _candles(20))

    payload = compute_tsmom_signals(state, market="stocks")
    row = payload["signals"][0]

    assert row["lookbacks_available"] == [5, 15]
    assert row["lookbacks_unavailable"] == [30]
    assert row["lookback_weights"] == {"5": 0.5, "15": 0.5}
    expected = sum(row["z_scores_by_lookback"].values()) / 2
    assert row["s_raw"] == pytest.approx(expected, abs=0.001)
    assert row["s_adj"] != row["s_raw"]
    assert row["funding_available"] is True


def test_stock_mode_excludes_insufficient_history_and_forming_bar():
    eligible = _asset("para:CRDO", dex="hl-para", display="CRDO")
    insufficient = _asset("para:SOFI", dex="hl-para", display="SOFI")
    state = _state(eligible, insufficient)
    state.add_candles(eligible.coin, "1d", _candles(16, forming=True))
    state.add_candles(insufficient.coin, "1d", _candles(15))

    payload = compute_tsmom_signals(state, market="stocks")

    assert [row["coin"] for row in payload["signals"]] == ["para:CRDO"]
    assert payload["signals"][0]["completed_bars"] == 16
    assert payload["meta"]["minimum_bars"] == 16
    assert payload["meta"]["insufficient_history_count"] == 1


def test_missing_stock_funding_does_not_remove_eligible_asset():
    stock = _asset(
        "para:CRDO",
        dex="hl-para",
        display="CRDO",
        funding=None,
    )
    state = _state(stock)
    state.add_candles(stock.coin, "1d", _candles(40))

    payload = compute_tsmom_signals(state, market="stocks")

    assert [row["coin"] for row in payload["signals"]] == ["para:CRDO"]
    assert payload["signals"][0]["funding_available"] is False


def test_final_daily_snapshot_replaces_forming_bar_before_becoming_eligible(
    monkeypatch,
):
    stock = _asset("para:CRDO", dex="hl-para", display="CRDO")
    state = _state(stock)
    historical = _candles(16)
    now_ms = int(time.time() * 1000)
    forming = {
        "t": now_ms,
        "T": now_ms + 1_000,
        "c": "999",
    }
    finalized = {
        "t": now_ms,
        "T": now_ms + 1_000,
        "c": "112",
    }
    state.add_candles(stock.coin, "1d", historical + [forming])
    state.add_candles(stock.coin, "1d", [finalized])
    monkeypatch.setattr(tsmom.time, "time", lambda: (now_ms + 2_000) / 1000)

    payload = compute_tsmom_signals(state, market="stocks")

    assert state.get_candles(stock.coin, "1d")[-1]["c"] == "112"
    assert payload["signals"][0]["completed_bars"] == 17


def test_category_split_uses_canonical_identity_and_excludes_spot():
    btc = _asset("BTC")
    stock = _asset("xyz:AVGO", dex="hl-xyz", display="AVGO")
    spot = _asset("AVGO/USDC", market_type="spot", display="AVGO")
    state = _state(btc, stock, spot)
    for asset in (btc, stock, spot):
        state.add_candles(asset.coin, "1d", _candles(40))

    crypto = compute_tsmom_signals(state, market="crypto")
    stocks = compute_tsmom_signals(state, market="stocks")

    assert {row["canonical_coin_id"] for row in crypto["signals"]} == {"BTC"}
    assert {row["canonical_coin_id"] for row in stocks["signals"]} == {"xyz:AVGO"}


@pytest.mark.asyncio
async def test_endpoint_market_parameter_and_crypto_default(monkeypatch):
    btc = _asset("BTC")
    stock = _asset("para:CRDO", dex="hl-para", display="CRDO")
    state = _state(btc, stock)
    state.add_candles(btc.coin, "1d", _candles(40))
    state.add_candles(stock.coin, "1d", _candles(40))
    monkeypatch.setattr(router, "_get_state", lambda: state)

    default_payload = await router.get_tsmom_signals()
    stocks_payload = await router.get_tsmom_signals(market="stocks")

    assert default_payload["meta"]["market"] == "crypto"
    assert [row["coin"] for row in stocks_payload["signals"]] == ["para:CRDO"]
    with pytest.raises(HTTPException) as error:
        await router.get_tsmom_signals(market="commodities")
    assert error.value.status_code == 400


@pytest.mark.asyncio
async def test_bulk_candle_fetch_bounds_concurrency_and_retries_empty_results():
    client = HyperliquidRestClient()
    active = 0
    max_active = 0
    attempts: dict[str, int] = {}

    async def transient_fetch(coin, interval, n_bars):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.001)
        active -= 1
        attempts[coin] = attempts.get(coin, 0) + 1
        if attempts[coin] == 1:
            return []
        return [{"t": 1, "T": 2, "c": "100"}]

    client.get_candle_snapshot = transient_fetch
    try:
        result = await client.get_candles_multi(
            [f"coin-{index}" for index in range(24)],
            "1d",
            n_bars=120,
        )
    finally:
        await client.close()

    assert max_active <= 8
    assert all(len(bars) == 1 for bars in result.values())
    assert set(attempts.values()) == {2}


@pytest.mark.asyncio
async def test_post_boot_enrichment_finishes_hip3_before_daily_history(
    monkeypatch,
):
    events = []

    async def no_sleep(_seconds):
        return None

    async def enrich_hip3(_state, _client):
        events.extend(["hip3_start", "hip3_done"])

    async def enrich_candles(_state, _client):
        events.append("candles_start")

    monkeypatch.setattr(websocket_manager.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(websocket_manager, "_enrich_hip3", enrich_hip3)
    monkeypatch.setattr(websocket_manager, "_enrich_1d_candles", enrich_candles)

    await websocket_manager._post_boot_enrich(HyperliquidState(), object())

    assert events == ["hip3_start", "hip3_done", "candles_start"]