from datetime import datetime, timedelta, timezone
import inspect
from unittest.mock import AsyncMock, patch

import pytest

from services.crypto_screener_service import (
    CryptoScreenerService,
    compute_technical_metrics,
    normalize_completed_history,
    resolve_coingecko_identity,
)


def test_page_service_has_no_history_altfins_or_hyperliquid_calls():
    source = inspect.getsource(CryptoScreenerService.get_screener)
    assert "get_market_chart" not in source
    assert "altfins" not in source.casefold()
    assert "hyperliquid" not in source.casefold()


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
    assert resolve_coingecko_identity(asset, coins) == (None, "ambiguous")


def test_identity_prefers_unique_cmc_slug_match():
    asset = {"symbol": "BTC", "name": "Bitcoin", "slug": "bitcoin"}
    coins = [
        {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"},
        {"id": "bitcoin-bridged", "symbol": "btc", "name": "Bitcoin"},
    ]
    assert resolve_coingecko_identity(asset, coins) == ("bitcoin", None)


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
    with patch("services.crypto_screener_service._load_records", return_value=[record]):
        result = await CryptoScreenerService(cmc, cg).get_screener()
    assert result["rows"][0]["symbol"] == "BTC"
    cmc.get_listings_latest.assert_not_awaited()
    cg.get_market_chart.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_refresh_uses_only_cmc_and_provider_failure_returns_lkg():
    cmc = AsyncMock()
    cmc.get_listings_latest.return_value = []
    cg = AsyncMock()
    old = datetime.now(timezone.utc) - timedelta(hours=1)
    record = {
        "cmc_id": 1, "current_updated_at": old, "history_updated_at": old,
        "current_snapshot": {"rank": 1, "symbol": "BTC"},
    }
    with patch("services.crypto_screener_service._load_records", return_value=[record]):
        result = await CryptoScreenerService(cmc, cg).get_screener()
    assert result["source"] == "stale_lkg"
    assert len(result["rows"]) == 1
    cmc.get_listings_latest.assert_awaited_once_with(100)
    cg.get_top_coins.assert_awaited_once_with(100)
    cg.get_market_chart.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_success_uses_minimal_current_calls_and_computes_liquidity():
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
        patch("services.crypto_screener_service._load_records", return_value=[record]),
        patch("services.crypto_screener_service._upsert_records") as write,
    ):
        result = await CryptoScreenerService(cmc, cg).get_screener()
    row = result["rows"][0]
    assert row["volume_to_market_cap_pct"] == 25
    assert row["volume_change_24h_pct"] == 12
    assert row["vol_x_7d"] is not None
    cmc.get_listings_latest.assert_awaited_once_with(100)
    cg.get_top_coins.assert_awaited_once_with(100)
    cg.get_market_chart.assert_not_awaited()
    assert write.call_count == 1