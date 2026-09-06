import asyncio
from datetime import datetime, timedelta, timezone
import inspect
from unittest.mock import AsyncMock, patch

import pytest

import services.crypto_screener_service as crypto_service
from data.coingecko_provider import CoinGeckoProvider
from services.crypto_screener_service import (
    CryptoScreenerService,
    _current_row,
    compute_technical_metrics,
    normalize_completed_history,
    resolve_coingecko_identity,
)


def test_page_service_has_no_history_altfins_or_hyperliquid_calls():
    source = inspect.getsource(CryptoScreenerService.get_screener)
    assert "get_market_chart" not in source
    assert "altfins" not in source.casefold()
    assert "hyperliquid" not in source.casefold()


@pytest.mark.asyncio
async def test_coin_list_requests_platform_contract_metadata():
    provider = CoinGeckoProvider("test-key")
    provider._get = AsyncMock(return_value=[])
    await provider.get_coin_list()
    provider._get.assert_awaited_once_with(
        "coins/list", {"include_platform": "true"}, ttl=86400
    )


def _history(count=220, start=100.0, step=1.0, volume=1000.0):
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    return [
        {
            "date": (base + timedelta(days=i)).date().isoformat(),
            "close": start + i * step,
            "volume": volume + i * 10,
        }
        for i in range(count)
    ]


def test_sma_pct_stack_rising_and_volume_math():
    history = _history()
    result = compute_technical_metrics(history, 325.0, 4000.0)
    assert result["sma_50"] == pytest.approx(sum(range(270, 320)) / 50)
    assert result["sma_150"] == pytest.approx(sum(range(170, 320)) / 150)
    assert result["sma_200"] == pytest.approx(sum(range(120, 320)) / 200)
    assert result["pct_vs_sma_50"] == pytest.approx((325 / result["sma_50"] - 1) * 100, abs=1e-5)
    assert result["above_all_3"] is True
    assert result["bullish_ma_stack"] is True
    assert result["sma_50_rising"] is True
    assert result["sma_150_rising"] is True
    assert result["sma_200_rising"] is True
    latest = [1000 + i * 10 for i in range(213, 220)]
    previous = [1000 + i * 10 for i in range(206, 213)]
    assert result["volume_delta_7d_pct"] == pytest.approx(
        ((sum(latest) / 7) / (sum(previous) / 7) - 1) * 100, abs=1e-5
    )
    assert result["vol_x_7d"] == pytest.approx(4000 / (sum(latest) / 7), abs=1e-5)


def test_incomplete_history_returns_null_not_false():
    result = compute_technical_metrics(_history(149), 300, 2000)
    for key in (
        "sma_50", "sma_150", "sma_200", "above_all_3", "bullish_ma_stack",
        "fresh_breakout_50", "holding_above_200", "setup_label",
    ):
        assert result[key] is None


@pytest.mark.parametrize("period", [50, 150, 200])
def test_fresh_breakout(period):
    history = _history(step=0)
    history[-2]["close"] = 99
    result = compute_technical_metrics(history, 101)
    assert result[f"fresh_breakout_{period}"] is True


def test_holding_above_and_falling_sma():
    history = _history(start=100, step=0)
    history[-3:] = [
        {**history[-3], "close": 101},
        {**history[-2], "close": 101},
        {**history[-1], "close": 101},
    ]
    result = compute_technical_metrics(history, 102)
    assert result["holding_above_200"] is True
    assert result["fresh_breakout_200"] is False
    falling = compute_technical_metrics(_history(start=400, step=-1), 500)
    assert falling["sma_200_rising"] is False


def test_ambiguous_identity_is_not_mapped():
    asset = {"symbol": "ABC", "name": "Alpha Beta"}
    coins = [
        {"id": "one", "symbol": "abc", "name": "Alpha Beta"},
        {"id": "two", "symbol": "abc", "name": "Alpha-Beta"},
    ]
    assert resolve_coingecko_identity(asset, coins) == (None, "ambiguous_symbol:2_candidates")


def test_identity_prefers_unique_cmc_slug_match():
    asset = {"symbol": "BTC", "name": "Bitcoin", "slug": "bitcoin"}
    coins = [
        {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"},
        {"id": "bitcoin-bridged", "symbol": "btc", "name": "Bitcoin"},
    ]
    assert resolve_coingecko_identity(asset, coins) == ("bitcoin", None)


def test_identity_matches_unique_contract_address():
    asset = {
        "symbol": "USDT", "name": "Tether USDt", "slug": "not-the-cg-id",
        "platform": {"token_address": "0xABC123"},
    }
    coins = [
        {"id": "bridged-usdt", "symbol": "usdt", "name": "Bridged USDT",
         "platforms": {"chain": "0x999"}},
        {"id": "tether", "symbol": "usdt", "name": "Tether",
         "platforms": {"ethereum": "0xabc123"}},
    ]
    assert resolve_coingecko_identity(asset, coins) == ("tether", None)


def test_identity_uses_symbol_only_when_unique():
    asset = {"symbol": "ATOM", "name": "Cosmos", "slug": "not-cosmos"}
    coins = [{"id": "cosmos", "symbol": "atom", "name": "Cosmos Hub"}]
    assert resolve_coingecko_identity(asset, coins) == ("cosmos", None)


@pytest.mark.parametrize(("asset", "coins", "expected"), [
    (
        {"symbol": "USDT", "name": "Tether USDt", "slug": "tether",
         "platform": {"token_address": "0xdac17f"}},
        [{"id": "tether", "symbol": "usdt", "name": "Tether",
          "platforms": {"ethereum": "0xdac17f"}}],
        "tether",
    ),
    (
        {"symbol": "DASH", "name": "Dash", "slug": "dash"},
        [{"id": "dash", "symbol": "dash", "name": "Dash"},
         {"id": "dash-2", "symbol": "dash", "name": "DASH"}],
        "dash",
    ),
    (
        {"symbol": "ATOM", "name": "Cosmos", "slug": "cosmos"},
        [{"id": "cosmos", "symbol": "atom", "name": "Cosmos Hub"}],
        "cosmos",
    ),
    (
        {"symbol": "CRV", "name": "Curve DAO Token", "slug": "curve-dao-token"},
        [{"id": "curve-dao-token", "symbol": "crv", "name": "Curve DAO"}],
        "curve-dao-token",
    ),
    (
        {"symbol": "POL", "name": "Polygon (prev. MATIC)", "slug": "polygon-ecosystem-token"},
        [{"id": "polygon-ecosystem-token", "symbol": "pol", "name": "POL (ex-MATIC)"}],
        "polygon-ecosystem-token",
    ),
])
def test_reported_major_identity_outcomes(asset, coins, expected):
    assert resolve_coingecko_identity(asset, coins) == (expected, None)


def test_completed_history_excludes_current_utc_day():
    now = datetime(2026, 1, 2, 12, tzinfo=timezone.utc)
    day1 = datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000
    day2 = datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp() * 1000
    rows = normalize_completed_history({
        "prices": [[day1, 10], [day2, 11]],
        "total_volumes": [[day1, 100], [day2, 110]],
    }, now=now)
    assert rows == [{"date": "2026-01-01", "close": 10.0, "volume": 100.0}]


@pytest.mark.asyncio
async def test_warm_endpoint_service_calls_no_provider():
    cmc = AsyncMock()
    cg = AsyncMock()
    now = datetime.now(timezone.utc)
    record = {
        "cmc_id": 1, "current_updated_at": now, "history_updated_at": now,
        "current_snapshot": {"rank": 1, "symbol": "BTC"},
    }
    with patch("services.crypto_screener_service._load_current_records", return_value=[record]):
        result = await CryptoScreenerService(cmc, cg).get_screener()
    assert result["rows"][0]["symbol"] == "BTC"
    cmc.get_listings_latest.assert_not_awaited()
    cg.get_top_coins.assert_not_awaited()
    cg.get_market_chart.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_lkg_returns_before_provider_and_schedules_refresh():
    cmc = AsyncMock()
    cg = AsyncMock()
    gate = asyncio.Event()

    async def blocked_provider(*_args):
        await gate.wait()
        return []

    cmc.get_listings_latest.side_effect = blocked_provider
    cg.get_top_coins.side_effect = blocked_provider
    old = datetime.now(timezone.utc) - timedelta(hours=1)
    record = {
        "cmc_id": 1, "current_updated_at": old, "history_updated_at": old,
        "current_snapshot": {"rank": 1, "symbol": "BTC"},
    }
    with (
        patch("services.crypto_screener_service._load_current_records", return_value=[record]),
        patch("services.crypto_screener_service._load_records", return_value=[record]),
    ):
        result = await asyncio.wait_for(
            CryptoScreenerService(cmc, cg).get_screener(), timeout=0.1
        )
    assert result["source"] == "stale_lkg"
    assert result["refreshing"] is True
    assert len(result["rows"]) == 1
    gate.set()
    await crypto_service._current_refresh_task
    cg.get_market_chart.assert_not_awaited()


@pytest.mark.asyncio
async def test_multiple_stale_requests_trigger_one_current_refresh():
    now = datetime.now(timezone.utc)
    old = now - timedelta(hours=1)
    history = _history()
    record = {
        "cmc_id": 1, "coingecko_id": "bitcoin", "identity_status": "resolved",
        "daily_history": history, "current_updated_at": old, "history_updated_at": old,
        "current_snapshot": {"rank": 1, "symbol": "BTC"},
    }
    cmc = AsyncMock()
    cmc.get_listings_latest.return_value = [{
        "id": 1, "cmc_rank": 1, "symbol": "BTC", "name": "Bitcoin", "slug": "bitcoin",
        "circulating_supply": 19, "total_supply": 21,
        "quote": {"USD": {
            "price": 325, "market_cap": 1000, "fully_diluted_market_cap": 1100,
            "volume_24h": 250, "volume_change_24h": 12,
        }},
    }]
    cg = AsyncMock()
    cg.get_top_coins.return_value = [{"id": "bitcoin", "total_volume": 4000}]
    with (
        patch("services.crypto_screener_service._load_current_records", return_value=[record]),
        patch("services.crypto_screener_service._load_records", return_value=[record]),
        patch("services.crypto_screener_service._upsert_records") as write,
    ):
        results = await asyncio.gather(*[
            CryptoScreenerService(cmc, cg).get_screener() for _ in range(8)
        ])
        await crypto_service._current_refresh_task
    assert all(result["source"] == "stale_lkg" for result in results)
    assert cmc.get_listings_latest.await_count == 1
    assert cg.get_top_coins.await_count == 1
    assert write.call_count == 1
    persisted_row = write.call_args.args[0][0]["current_snapshot"]
    assert persisted_row["volume_to_market_cap_pct"] == 25
    assert persisted_row["volume_change_24h_pct"] == 12
    assert persisted_row["vol_x_7d"] is not None
    cg.get_market_chart.assert_not_awaited()


@pytest.mark.asyncio
async def test_refresh_rechecks_freshness_after_acquiring_lock():
    old = datetime.now(timezone.utc) - timedelta(hours=1)
    fresh = datetime.now(timezone.utc)
    stale_record = {
        "cmc_id": 1, "current_updated_at": old,
        "current_snapshot": {"rank": 1, "symbol": "BTC"},
    }
    fresh_record = {**stale_record, "current_updated_at": fresh}
    cmc, cg = AsyncMock(), AsyncMock()
    with patch(
        "services.crypto_screener_service._load_current_records",
        return_value=[stale_record],
    ), patch("services.crypto_screener_service._load_records", return_value=[fresh_record]):
        result = await CryptoScreenerService(cmc, cg).get_screener()
        await crypto_service._current_refresh_task
    assert result["source"] == "stale_lkg"
    cmc.get_listings_latest.assert_not_awaited()
    cg.get_top_coins.assert_not_awaited()


@pytest.mark.asyncio
async def test_bootstrap_without_lkg_awaits_minimal_current_refresh():
    history = _history()
    cmc = AsyncMock()
    cmc.get_listings_latest.return_value = [{
        "id": 1, "cmc_rank": 1, "symbol": "BTC", "name": "Bitcoin", "slug": "bitcoin",
        "quote": {"USD": {"price": 325, "market_cap": 1000, "volume_24h": 250}},
    }]
    cg = AsyncMock()
    cg.get_top_coins.return_value = [{"id": "bitcoin", "total_volume": 4000}]
    historical_record = {
        "cmc_id": 1, "coingecko_id": "bitcoin", "daily_history": history,
        "history_updated_at": datetime.now(timezone.utc),
    }
    with (
        patch("services.crypto_screener_service._load_current_records", return_value=[]),
        patch("services.crypto_screener_service._load_records", return_value=[historical_record]),
        patch("services.crypto_screener_service._upsert_records"),
    ):
        result = await CryptoScreenerService(cmc, cg).get_screener()
    assert result["source"] == "live"
    assert result["rows"][0]["symbol"] == "BTC"
    cmc.get_listings_latest.assert_awaited_once_with(100)
    cg.get_top_coins.assert_awaited_once_with(100)
    cg.get_market_chart.assert_not_awaited()


def test_ath_drawdown_uses_coingecko_market_and_other_legacy_fields_stay_null():
    asset = {
        "id": 1, "cmc_rank": 1, "symbol": "BTC", "name": "Bitcoin", "slug": "bitcoin",
        "quote": {"USD": {"price": 100, "market_cap": 1000, "volume_24h": 100}},
    }
    row = _current_row(
        asset,
        {"coingecko_id": "bitcoin", "daily_history": _history()},
        datetime.now(timezone.utc).isoformat(),
        {"id": "bitcoin", "total_volume": 4000, "ath_change_percentage": -36.5},
    )
    assert row["ath_drawdown_pct"] == -36.5
    assert row["cycle_low_move_pct"] is None
    assert row["ai_project"] is None
    assert row["sentiment"] is None
    assert row["x_score"] is None