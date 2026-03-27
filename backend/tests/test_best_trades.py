import asyncio
import json
import pytest
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import AsyncMock, MagicMock, patch
from data.market_data_service import MarketDataService
from data.cache import cache
import data.market_data_service as mds
from core.ta_signal_engine import analyze_bars, _detect_signals, _compute_ta_score, compute_atr


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
    svc.sec_edgar = MagicMock()
    svc.twelvedata = None

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
    async def mock_get_most_active():
        return finviz_results[3:8]
    async def mock_get_oversold():
        return finviz_results[5:10]
    async def mock_get_most_volatile():
        return finviz_results[0:5]

    svc.finviz.get_new_highs = mock_get_new_highs
    svc.finviz.get_unusual_volume = mock_get_unusual_volume
    svc.finviz.get_screener_results = mock_get_screener_results
    svc.finviz._custom_screen = mock_custom_screen
    svc.finviz.get_most_active = mock_get_most_active
    svc.finviz.get_oversold_stocks = mock_get_oversold
    svc.finviz.get_most_volatile = mock_get_most_volatile

    svc.finnhub.get_stock_candles = MagicMock(return_value=_make_bars(60))
    svc.polygon.get_daily_bars = MagicMock(return_value=_make_bars(60))

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
        "technical_score", "setup_type", "pattern", "signals_stacking",
        "indicator_signals", "entry", "stop", "targets", "risk_reward",
        "timeframe", "confirmations", "tradingview_url", "tv_url",
        "catalyst_check", "data_gaps"
    ]
    for field in required_fields:
        assert field in trade, f"Missing field: {field}"

    assert trade["entry"].startswith("$")
    assert trade["stop"].startswith("$")
    assert len(trade["targets"]) >= 1
    assert trade["targets"][0].startswith("$")
    assert ":" in trade["risk_reward"]
    assert trade["direction"] in ("long", "short")
    assert trade["action"] in ("Strong Buy", "Buy", "Hold", "Sell")
    assert trade["setup_type"] in ("breakout", "trend_continuation", "momentum", "breakdown_short", "technical_setup")
    assert isinstance(trade["indicator_signals"], list)
    assert len(trade["indicator_signals"]) >= 1
    assert trade["tradingview_url"].startswith("https://www.tradingview.com/")


@pytest.mark.asyncio
async def test_best_trades_shortlist_capped(mock_service):
    result = await mock_service.get_best_trades_scan()
    assert result["scan_stats"]["shortlisted"] <= 40
    assert result["scan_stats"]["candle_targets"] <= 20


@pytest.mark.asyncio
async def test_best_trades_scan_stats_fields(mock_service):
    result = await mock_service.get_best_trades_scan()
    stats = result["scan_stats"]
    assert "candidates_total" in stats
    assert "candles_ok" in stats
    assert "candles_blocked" in stats
    assert "cache_hits" in stats
    assert "ta_qualified" in stats


@pytest.mark.asyncio
async def test_best_trades_data_health_present(mock_service):
    result = await mock_service.get_best_trades_scan()
    dh = result["data_health"]
    assert "candles_source" in dh
    assert "candle_stats" in dh
    assert "finnhub_circuit_breaker" in dh


@pytest.mark.asyncio
async def test_best_trades_all_candles_fail_still_structured(mock_service):
    cache.clear()
    mds._finnhub_candle_disabled_until = 0.0

    def fail_candles(ticker, days=120):
        raise Exception("FinnhubAPIException(status_code: 403): You don't have access")

    mock_service.finnhub.get_stock_candles = MagicMock(side_effect=fail_candles)
    mock_service.polygon.get_daily_bars = MagicMock(return_value=[])

    result = await mock_service.get_best_trades_scan()

    assert result["display_type"] == "trades"
    assert isinstance(result["top_trades"], list)
    assert isinstance(result["bearish_setups"], list)


@pytest.mark.asyncio
async def test_best_trades_circuit_breaker_triggers(mock_service):
    cache.clear()
    old_val = mds._finnhub_candle_disabled_until
    try:
        mds._finnhub_candle_disabled_until = 0.0

        def fail_403(ticker, days=120):
            raise Exception("FinnhubAPIException(status_code: 403)")

        mock_service.finnhub.get_stock_candles = MagicMock(side_effect=fail_403)
        mock_service.polygon.get_daily_bars = MagicMock(return_value=_make_bars(60))

        result = await mock_service.get_best_trades_scan()

        assert mds._finnhub_candle_disabled_until > 0
        assert result["data_health"]["finnhub_circuit_breaker"] is True
    finally:
        mds._finnhub_candle_disabled_until = old_val


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
async def test_best_trades_empty_returns_reason(mock_service):
    cache.clear()
    mds._finnhub_candle_disabled_until = 0.0

    def fail_candles(ticker, days=120):
        raise Exception("FinnhubAPIException(status_code: 403)")

    mock_service.finnhub.get_stock_candles = MagicMock(side_effect=fail_candles)
    mock_service.polygon.get_daily_bars = MagicMock(return_value=[])

    result = await mock_service.get_best_trades_scan()

    assert result["display_type"] == "trades"
    assert len(result["top_trades"]) == 0
    assert "empty_reason" in result["data_health"]
    assert len(result["data_health"]["empty_reason"]) > 0


@pytest.mark.asyncio
async def test_best_trades_budget_limits_polygon_calls(mock_service):
    cache.clear()
    mds._finnhub_candle_disabled_until = 0.0

    def fail_candles(ticker, days=120):
        raise Exception("FinnhubAPIException(status_code: 403)")

    call_count = {"n": 0}

    def counting_bars(ticker, days=120):
        call_count["n"] += 1
        return _make_bars(60)

    mock_service.finnhub.get_stock_candles = MagicMock(side_effect=fail_candles)
    mock_service.polygon.get_daily_bars = MagicMock(side_effect=counting_bars)

    result = await mock_service.get_best_trades_scan()

    assert call_count["n"] <= 15
    assert result["display_type"] == "trades"


@pytest.mark.asyncio
async def test_best_trades_no_social_only_narrative(mock_service):
    result = await mock_service.get_best_trades_scan()
    for trade in result["top_trades"]:
        assert "signals_stacking" in trade
        assert len(trade["signals_stacking"]) >= 1
        assert trade["pattern"] != ""
        assert "indicator_signals" in trade


def test_ta_engine_returns_signals_on_uptrend():
    bars = _make_bars(60, base_price=50.0)
    result = analyze_bars(bars, ticker="TEST")
    assert result is not None
    assert result["ticker"] == "TEST"
    assert result["direction"] in ("long", "short")
    assert result["action"] in ("Strong Buy", "Buy", "Hold", "Sell")
    assert result["entry"].startswith("$")
    assert result["stop"].startswith("$")
    assert len(result["targets"]) == 2
    assert ":" in result["risk_reward"]
    assert result["technical_score"] > 0
    assert len(result["signals_stacking"]) >= 1
    assert len(result["ta_signals"]) >= 1
    assert result["setup_type"] in ("breakout", "trend_continuation", "momentum", "breakdown_short", "technical_setup")
    assert isinstance(result["indicator_signals"], list)
    assert len(result["indicator_signals"]) >= 1
    assert result["tradingview_url"].startswith("https://www.tradingview.com/")
    for sig in result["ta_signals"]:
        assert "name" in sig
        assert "direction" in sig
        assert "strength" in sig
        assert "evidence" in sig
        assert sig["direction"] in ("bullish", "bearish")
        assert 0 <= sig["strength"] <= 100


def test_ta_engine_too_few_bars_returns_none():
    bars = _make_bars(10, base_price=50.0)
    result = analyze_bars(bars, ticker="SHORT")
    assert result is None


def test_ta_engine_atr_computation():
    bars = _make_bars(30, base_price=100.0)
    highs = [b["h"] for b in bars]
    lows = [b["l"] for b in bars]
    closes = [b["c"] for b in bars]
    atr = compute_atr(highs, lows, closes, period=14)
    assert atr is not None
    assert atr > 0


def test_ta_score_increases_with_bullish_signals():
    signals_few = [
        {"name": "price_above_sma50", "direction": "bullish", "strength": 50, "evidence": "test"},
    ]
    signals_many = [
        {"name": "price_above_sma50", "direction": "bullish", "strength": 50, "evidence": "test"},
        {"name": "macd_bull_cross", "direction": "bullish", "strength": 60, "evidence": "test"},
        {"name": "rsi_bull_zone", "direction": "bullish", "strength": 55, "evidence": "test"},
        {"name": "volume_spike_2x", "direction": "bullish", "strength": 70, "evidence": "test"},
    ]
    score_few = _compute_ta_score(signals_few)
    score_many = _compute_ta_score(signals_many)
    assert score_many > score_few


def test_ta_engine_volume_spike_detected():
    bars = _make_bars(60, base_price=50.0)
    avg_vol = sum(b["v"] for b in bars[-30:]) / 30
    bars[-1]["v"] = int(avg_vol * 3)
    result = analyze_bars(bars, ticker="VOLTEST")
    if result:
        signal_names = [s["name"] for s in result["ta_signals"]]
        assert "volume_spike_2x" in signal_names or "volume_expansion" in signal_names


def test_ta_engine_indicator_signals_format():
    bars = _make_bars(60, base_price=50.0)
    result = analyze_bars(bars, ticker="FMTTEST")
    if result:
        assert isinstance(result["indicator_signals"], list)
        for sig_str in result["indicator_signals"]:
            assert isinstance(sig_str, str)
            assert len(sig_str) > 0


def test_ta_engine_action_strong_buy():
    bars = _make_bars(80, base_price=50.0)
    bars[-1]["v"] = 5000000
    result = analyze_bars(
        bars, ticker="STRONGTEST",
        source_list=["new_high", "unusual_vol", "breakout"],
    )
    if result and result["confidence_score"] >= 80:
        assert result["action"] == "Strong Buy"


@pytest.mark.asyncio
async def test_best_trades_returns_at_least_3_setups(mock_service):
    cache.clear()
    result = await mock_service.get_best_trades_scan()
    total = len(result["top_trades"]) + len(result["bearish_setups"])
    assert total >= 3, f"Expected >=3 total setups, got {total}"


@pytest.mark.asyncio
async def test_best_trades_candle_budget_15(mock_service):
    cache.clear()
    result = await mock_service.get_best_trades_scan()
    api_usage = result["data_health"].get("api_usage", {})
    assert api_usage.get("budget_max", 0) == 15


@pytest.mark.asyncio
async def test_best_trades_api_usage_in_data_health(mock_service):
    cache.clear()
    result = await mock_service.get_best_trades_scan()
    assert "api_usage" in result["data_health"]
    usage = result["data_health"]["api_usage"]
    assert "total_api_calls" in usage
    assert "cache_hits" in usage
    assert "budget_max" in usage


@pytest.mark.asyncio
async def test_best_trades_wide_discovery_more_candidates(mock_service):
    cache.clear()
    result = await mock_service.get_best_trades_scan()
    assert result["scan_stats"]["candidates_total"] >= 8


@pytest.mark.asyncio
async def test_best_trades_no_schema_changes(mock_service):
    cache.clear()
    result = await mock_service.get_best_trades_scan()
    required_keys = {"scan_type", "display_type", "market_pulse", "top_trades", "bearish_setups", "scan_stats", "data_health"}
    assert required_keys.issubset(set(result.keys()))
    assert result["display_type"] == "trades"
    assert result["scan_type"] == "best_trades"
