import asyncio
import pytest
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import AsyncMock, MagicMock, patch
from data.market_data_service import MarketDataService, CandleBudget
from data.cache import cache
from screener_definitions import SCREENER_DEFINITIONS, SCREENER_PRESETS


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


def _make_finviz_results(n=20, base_price=30.0):
    tickers = [
        "AAPL", "MSFT", "GOOG", "TSLA", "NVDA", "AMD", "META", "NFLX",
        "AMZN", "CRM", "SHOP", "SQ", "PLTR", "SNOW", "DDOG", "NET",
        "CRWD", "ZS", "MDB", "COIN",
    ]
    results = []
    for i in range(min(n, len(tickers))):
        results.append({
            "ticker": tickers[i],
            "company": f"{tickers[i]} Corporation",
            "sector": "Technology",
            "market_cap": "1.5B",
            "price": f"{base_price + i * 2:.2f}",
            "change": f"{2.0 + i * 0.3:.1f}%",
        })
    return results


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
    svc.twelvedata = None

    finviz_results = _make_finviz_results(20)

    async def mock_custom_screen(params=""):
        return finviz_results

    svc.finviz._custom_screen = mock_custom_screen

    def mock_get_quote(ticker):
        return {
            "price": 55.0,
            "change_pct": 2.5,
            "high": 56.0,
            "low": 54.0,
        }

    svc.finnhub.get_quote = mock_get_quote

    async def mock_get_overview(ticker):
        return {
            "ticker": ticker,
            "market_cap": "1.5B",
            "pe_ratio": "18.5",
            "forward_pe": "15.0",
            "dividend_yield": "2.8%",
            "revenue_growth": "+22.5%",
            "revenue": "$5.2B",
        }

    svc.stockanalysis.get_overview = mock_get_overview

    svc.finnhub.get_stock_candles = MagicMock(return_value=_make_bars(60))
    svc.polygon.get_daily_bars = MagicMock(return_value=_make_bars(60))

    return svc


def test_screener_definitions_loaded():
    assert len(SCREENER_DEFINITIONS) == 6
    assert set(SCREENER_PRESETS) == {
        "oversold_growing", "value_momentum", "insider_breakout",
        "high_growth_sc", "dividend_value", "short_squeeze",
    }


def test_screener_definitions_have_required_keys():
    required_keys = [
        "screen_label", "finviz_filters", "finviz_sort",
        "enrichment", "ta_rules", "fundamental_rules",
        "ranking_weights", "explain_template",
    ]
    for preset, defn in SCREENER_DEFINITIONS.items():
        for key in required_keys:
            assert key in defn, f"Missing '{key}' in preset '{preset}'"
        weights = defn["ranking_weights"]
        total = weights["technical"] + weights["fundamental"] + weights["liquidity"]
        assert abs(total - 1.0) < 0.01, f"Weights don't sum to 1.0 for '{preset}'"


@pytest.mark.asyncio
async def test_oversold_growing_screener(mock_service):
    result = await mock_service.run_deterministic_screener("oversold_growing")
    assert result["display_type"] == "screener"
    assert result["screen_name"] == "Oversold + Growing"
    assert result["preset"] == "oversold_growing"
    assert len(result["rows"]) >= 10
    _validate_rows(result["rows"])


@pytest.mark.asyncio
async def test_value_momentum_screener(mock_service):
    result = await mock_service.run_deterministic_screener("value_momentum")
    assert result["display_type"] == "screener"
    assert result["screen_name"] == "Value + Momentum"
    assert len(result["rows"]) >= 10
    _validate_rows(result["rows"])


@pytest.mark.asyncio
async def test_insider_breakout_screener(mock_service):
    result = await mock_service.run_deterministic_screener("insider_breakout")
    assert result["display_type"] == "screener"
    assert result["screen_name"] == "Insider + Breakout"
    assert len(result["rows"]) >= 10
    _validate_rows(result["rows"])


@pytest.mark.asyncio
async def test_high_growth_sc_screener(mock_service):
    result = await mock_service.run_deterministic_screener("high_growth_sc")
    assert result["display_type"] == "screener"
    assert result["screen_name"] == "High Growth Small Cap"
    assert len(result["rows"]) >= 10
    _validate_rows(result["rows"])


@pytest.mark.asyncio
async def test_dividend_value_screener(mock_service):
    result = await mock_service.run_deterministic_screener("dividend_value")
    assert result["display_type"] == "screener"
    assert result["screen_name"] == "Dividend Value"
    assert len(result["rows"]) >= 10
    _validate_rows(result["rows"])


@pytest.mark.asyncio
async def test_short_squeeze_screener(mock_service):
    result = await mock_service.run_deterministic_screener("short_squeeze")
    assert result["display_type"] == "screener"
    assert result["screen_name"] == "Short Squeeze"
    assert len(result["rows"]) >= 10
    _validate_rows(result["rows"])


@pytest.mark.asyncio
async def test_unknown_preset_returns_error(mock_service):
    result = await mock_service.run_deterministic_screener("nonexistent_preset")
    assert result["display_type"] == "screener"
    assert "error" in result
    assert result["rows"] == []


@pytest.mark.asyncio
async def test_screener_scan_stats(mock_service):
    result = await mock_service.run_deterministic_screener("value_momentum")
    stats = result["scan_stats"]
    assert "candidates_total" in stats
    assert "enriched" in stats
    assert "candles_ok" in stats
    assert "qualified" in stats
    assert stats["candidates_total"] >= 1


@pytest.mark.asyncio
async def test_screener_top_picks_present(mock_service):
    result = await mock_service.run_deterministic_screener("value_momentum")
    assert len(result["top_picks"]) >= 2
    for pick in result["top_picks"]:
        assert "ticker" in pick
        assert "confidence" in pick
        assert "reason" in pick


@pytest.mark.asyncio
async def test_screener_explain_template(mock_service):
    result = await mock_service.run_deterministic_screener("oversold_growing")
    assert isinstance(result["explain"], list)
    assert len(result["explain"]) >= 3


@pytest.mark.asyncio
async def test_screener_no_empty_finviz_graceful(mock_service):
    async def empty_screen(params=""):
        return []
    mock_service.finviz._custom_screen = empty_screen

    result = await mock_service.run_deterministic_screener("value_momentum")
    assert result["display_type"] == "screener"
    assert isinstance(result["rows"], list)


def _validate_rows(rows):
    price_count = 0
    chg_count = 0
    for row in rows:
        assert isinstance(row.get("ticker"), str)
        assert len(row["ticker"]) >= 1

        company = row.get("company")
        if company is not None:
            assert len(company) > 1, f"Company is single char: '{company}' for {row['ticker']}"

        if row.get("price") is not None:
            price_count += 1
        if row.get("chg_pct") is not None:
            chg_count += 1

        assert isinstance(row.get("signals", []), list)

        for key, val in row.items():
            assert val != "N/A", f"Found N/A in field '{key}' for {row['ticker']}"

    assert price_count >= min(8, len(rows)), f"Only {price_count}/{len(rows)} rows have price"
    assert chg_count >= min(8, len(rows)), f"Only {chg_count}/{len(rows)} rows have chg_pct"
