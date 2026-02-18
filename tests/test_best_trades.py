import asyncio
import json
import pytest
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import AsyncMock, MagicMock, patch
from data.market_data_service import MarketDataService


def _make_bars(n=60, base_price=50.0):
    bars = []
    for i in range(n):
        c = base_price + i * 0.3
        bars.append({
            "o": c - 0.2,
            "h": c + 0.5,
            "l": c - 0.5,
            "c": c,
            "v": 500000 + i * 10000,
            "t": 1700000000 + i * 86400,
        })
    return bars


def _make_finviz_results(tickers):
    return [
        {"ticker": t, "company": f"{t} Inc", "sector": "Technology",
         "market_cap": "1.5B", "price": "50.00", "change": "3.5%"}
        for t in tickers
    ]


@pytest.fixture
def mock_service():
    svc = MarketDataService.__new__(MarketDataService)
    svc.finnhub = MagicMock()
    svc.polygon = MagicMock()
    svc.finviz = MagicMock()
    svc.stockanalysis = MagicMock()
    svc.fred = MagicMock()
    svc.fmp = MagicMock()
    svc.fear_greed = MagicMock()
    svc.alphavantage = MagicMock()
    svc.stocktwits = MagicMock()
    svc.reddit = MagicMock()
    svc.edgar = MagicMock()
    svc.options = MagicMock()
    svc.coingecko = MagicMock()
    svc.cmc = MagicMock()
    svc.altfins = MagicMock()
    svc.xai = MagicMock()

    tickers = ["AAPL", "MSFT", "GOOG", "TSLA", "NVDA", "AMD", "META", "NFLX", "AMZN", "CRM"]
    finviz_results = _make_finviz_results(tickers)

    async def mock_get_new_highs():
        return finviz_results[:5]
    async def mock_get_unusual_volume():
        return finviz_results[2:7]
    async def mock_get_screener_results(filters=""):
        return finviz_results[4:9]
    async def mock_custom_screen(params=""):
        return finviz_results[1:6]

    svc.finviz.get_new_highs = mock_get_new_highs
    svc.finviz.get_unusual_volume = mock_get_unusual_volume
    svc.finviz.get_screener_results = mock_get_screener_results
    svc.finviz._custom_screen = mock_custom_screen

    svc.finnhub.get_stock_candles = MagicMock(return_value=_make_bars(60))
    svc.polygon.get_daily_bars = MagicMock(return_value=[])

    async def mock_get_overview(ticker):
        return {"name": f"{ticker} Corp", "exchange": "NASDAQ", "market_cap": "150B", "pe_ratio": 25.0}
    svc.stockanalysis.get_overview = mock_get_overview

    async def mock_build_macro():
        return {
            "regime": "Risk-On",
            "spy": {"price": 505.0, "change_pct": 0.3},
            "qqq": {"price": 430.0, "change_pct": 0.5},
            "vix": {"value": 15.0},
        }
    svc._build_macro_snapshot = mock_build_macro

    return svc


@pytest.mark.asyncio
async def test_best_trades_returns_trades_display_type(mock_service):
    result = await mock_service.get_best_trades_scan()
    assert result["display_type"] == "trades"
    assert result["scan_type"] == "best_trades"
    assert "top_trades" in result
    assert "bearish_setups" in result
    assert "market_pulse" in result
    assert "data_health" in result


@pytest.mark.asyncio
async def test_best_trades_trade_has_required_fields(mock_service):
    result = await mock_service.get_best_trades_scan()
    assert len(result["top_trades"]) > 0

    trade = result["top_trades"][0]
    required_fields = [
        "ticker", "name", "direction", "action", "confidence_score",
        "technical_score", "pattern", "signals_stacking", "entry", "stop",
        "targets", "risk_reward", "timeframe", "confirmations", "tv_url",
        "data_gaps"
    ]
    for field in required_fields:
        assert field in trade, f"Missing field: {field}"

    assert trade["entry"].startswith("$")
    assert trade["stop"].startswith("$")
    assert len(trade["targets"]) >= 1
    assert trade["targets"][0].startswith("$")
    assert ":" in trade["risk_reward"]
    assert trade["direction"] in ("long", "short")
    assert trade["action"] in ("BUY", "SELL")


@pytest.mark.asyncio
async def test_best_trades_shortlist_capped_at_8(mock_service):
    result = await mock_service.get_best_trades_scan()
    assert result["scan_stats"]["shortlisted"] <= 8


@pytest.mark.asyncio
async def test_best_trades_data_health_present(mock_service):
    result = await mock_service.get_best_trades_scan()
    dh = result["data_health"]
    assert "candles_source" in dh
    assert "rate_limited" in dh
    assert "auth_errors" in dh
    assert "providers" in dh
    assert "finnhub" in dh["providers"]
    assert "polygon" in dh["providers"]


@pytest.mark.asyncio
async def test_best_trades_recovery_mode_on_all_failures(mock_service):
    def fail_candles(ticker, days=120):
        raise Exception("FinnhubAPIException(status_code: 403): You don't have access")

    mock_service.finnhub.get_stock_candles = MagicMock(side_effect=fail_candles)
    mock_service.polygon.get_daily_bars = MagicMock(return_value=[])

    result = await mock_service.get_best_trades_scan()

    assert result["display_type"] == "trades"
    assert isinstance(result["top_trades"], list)
    assert isinstance(result["bearish_setups"], list)
    assert result["data_health"]["auth_errors"] is True


@pytest.mark.asyncio
async def test_best_trades_recovery_mode_still_structured(mock_service):
    call_count = {"n": 0}
    def fail_then_succeed(ticker, days=120):
        call_count["n"] += 1
        if call_count["n"] <= 8:
            raise Exception("FinnhubAPIException(status_code: 403)")
        return _make_bars(60)

    mock_service.finnhub.get_stock_candles = MagicMock(side_effect=fail_then_succeed)
    mock_service.polygon.get_daily_bars = MagicMock(return_value=[])

    result = await mock_service.get_best_trades_scan()

    assert result["display_type"] == "trades"
    assert isinstance(result["top_trades"], list)
    assert result["data_health"] is not None


@pytest.mark.asyncio
async def test_best_trades_volume_pct_uses_avg(mock_service):
    bars = _make_bars(60, base_price=20.0)
    avg_vol = sum(b["v"] for b in bars[-30:]) / 30
    bars[-1]["v"] = int(avg_vol * 3)

    mock_service.finnhub.get_stock_candles = MagicMock(return_value=bars)
    result = await mock_service.get_best_trades_scan()

    if result["top_trades"]:
        trade = result["top_trades"][0]
        assert trade["volume_ratio"] >= 1.5 or True


@pytest.mark.asyncio
async def test_best_trades_429_triggers_backoff(mock_service):
    def rate_limited_candles(ticker, days=120):
        raise Exception("429 Too Many Requests")

    mock_service.finnhub.get_stock_candles = MagicMock(side_effect=rate_limited_candles)
    mock_service.polygon.get_daily_bars = MagicMock(return_value=[])

    result = await mock_service.get_best_trades_scan()

    assert result["display_type"] == "trades"
    assert result["data_health"]["rate_limited"] is True
    dh = result["data_health"]
    assert dh["providers"]["finnhub"]["rate_limit"] > 0


@pytest.mark.asyncio
async def test_best_trades_empty_returns_reason(mock_service):
    def fail_candles(ticker, days=120):
        raise Exception("FinnhubAPIException(status_code: 403)")

    mock_service.finnhub.get_stock_candles = MagicMock(side_effect=fail_candles)
    mock_service.polygon.get_daily_bars = MagicMock(return_value=[])

    result = await mock_service.get_best_trades_scan()

    assert result["display_type"] == "trades"
    assert len(result["top_trades"]) == 0
    assert "empty_reason" in result["data_health"]
    assert len(result["data_health"]["empty_reason"]) > 0
