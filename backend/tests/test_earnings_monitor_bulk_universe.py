import asyncio
import sys
from pathlib import Path

import pytest


BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from services import earnings_monitor_service as service
from services import fmp_cache_service
from data import earnings_monitor_store, portfolio_store


@pytest.mark.asyncio
async def test_universe_uses_one_bulk_profile_read_off_event_loop(monkeypatch):
    monkeypatch.setattr(
        earnings_monitor_store,
        "get_universe_symbols",
        lambda: ["AAPL", "ETF1", "FUND1", "MISS"],
    )
    monkeypatch.setattr(
        portfolio_store,
        "load_active_holdings",
        lambda: [{"symbol": "AAPL"}, {"ticker": "PORT"}],
    )

    calls = []

    def bulk_profiles(symbols):
        with pytest.raises(RuntimeError):
            asyncio.get_running_loop()
        calls.append(symbols)
        return {
            "AAPL": {"name": "Apple Inc."},
            "ETF1": {"is_etf": True, "name": "Example ETF"},
            "FUND1": {"name": "Example Index Fund"},
            "PORT": {"name": "Portfolio Corp."},
        }

    monkeypatch.setattr(
        fmp_cache_service,
        "get_company_profiles_bulk_cached",
        bulk_profiles,
    )
    monkeypatch.setattr(
        fmp_cache_service,
        "get_company_profile_cached",
        lambda _symbol: pytest.fail("per-symbol profile cache must not be used"),
    )

    result = await service._build_universe()

    assert calls == [["AAPL", "ETF1", "FUND1", "MISS", "PORT"]]
    assert result == ["AAPL", "MISS", "PORT"]
