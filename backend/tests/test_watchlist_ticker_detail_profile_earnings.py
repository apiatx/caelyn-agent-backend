from __future__ import annotations

import asyncio
import os
import sys


BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from services import fmp_cache_service
from services import watchlist_router
from services.watchlist_fundamentals_refresh import (
    FmpFundamentalsRefresher,
    ei_ineligible_reason,
)


AMKR_PROFILE = {
    "companyName": "Amkor Technology, Inc.",
    "description": "Amkor provides outsourced semiconductor packaging and test services.",
    "sector": "Technology",
    "industry": "Semiconductors",
    "marketCap": 16_401_724_516,
    "country": "US",
    "beta": 2.214,
    "ceo": "Kevin K. Engel",
    "exchange": "NASDAQ",
    "website": "https://amkor.com",
    "isEtf": False,
    "isFund": False,
}


def test_amkr_is_eligible_company():
    snapshot = {"fields": {"profile": AMKR_PROFILE}}
    assert ei_ineligible_reason("AMKR", snapshot) is None


def test_amkr_about_uses_persisted_profile_when_screener_row_is_partial():
    company = watchlist_router._merge_company_profile_fields(
        "AMKR",
        {"name": "", "sector": "", "industry": "", "market_cap": None},
        {"profile": {"companyName": ""}},
        AMKR_PROFILE,
        {"Market Cap": AMKR_PROFILE["marketCap"]},
    )
    assert company["company_name"] == "Amkor Technology, Inc."
    assert company["description"].startswith("Amkor provides")
    assert company["website"] == "https://amkor.com"
    assert company["ceo"] == "Kevin K. Engel"


def test_earnings_present_without_upcoming_date_sec_or_reactions():
    snapshot = {
        "fields": {
            "profile": AMKR_PROFILE,
            "Earnings Date": None,
            "earnings_intelligence": {
                "earnings_history": [],
                "reaction_summary": {},
                "sec_filings": None,
            },
        }
    }
    ei = watchlist_router._stable_cached_earnings_intelligence("AMKR", snapshot)
    assert ei is not None
    assert ei["earnings_history"] == []
    assert ei["reaction_summary"] == {}
    assert ei["sec_filings"] is None
    assert "ratings" in ei


def test_eligible_company_without_cached_aggregate_gets_stable_earnings_shape():
    ei = watchlist_router._stable_cached_earnings_intelligence(
        "AMKR", {"fields": {"profile": AMKR_PROFILE}}
    )
    assert ei is not None
    assert ei["earnings_history"] == []
    assert ei["ratings"]["recent_actions"] == []


def test_partial_provider_profile_does_not_erase_cached_fields(monkeypatch):
    refresher = FmpFundamentalsRefresher("test")
    old = {"description": "Good cached description", "website": "https://amkor.com"}

    async def fake_normalize(_symbol):
        return {
            "fields": {"Market Cap": 10, "profile": {"description": ""}},
            "missing_fields": [],
            "fmp_call_count": 1,
            "_not_meaningful_active": set(),
            "_bs_outcome": "success_no_data",
            "_est_outcome": "success_no_data",
            "_scores_outcome": "success_no_data",
        }

    monkeypatch.setattr(refresher, "normalize_symbol", fake_normalize)
    monkeypatch.setattr(refresher, "_fetch_earnings_intelligence", lambda _s: None)
    monkeypatch.setattr(
        "data.watchlist_fundamentals_store.get_snapshots_bulk",
        lambda _symbols: {"AMKR": {"fields": {"Market Cap": 9, "profile": old}}},
    )
    written = {}
    monkeypatch.setattr(
        "data.watchlist_fundamentals_store.upsert_snapshot",
        lambda **kwargs: written.update(kwargs) or "success",
    )
    monkeypatch.setattr("data.watchlist_estimate_history_store.ensure_table", lambda: True)
    monkeypatch.setattr("data.watchlist_estimate_history_store.prune_old_observations", lambda: 0)

    asyncio.run(refresher.refresh_symbols(["AMKR"], "wl", dev_force=True))
    assert written["fields"]["profile"]["description"] == "Good cached description"
    assert written["fields"]["profile"]["website"] == "https://amkor.com"


def test_cached_profile_prevents_repeated_upstream_calls(monkeypatch):
    db_calls = 0
    upstream_calls = 0

    def db_read(_symbols):
        nonlocal db_calls
        db_calls += 1
        return {"AMKR": {"profile": AMKR_PROFILE}}

    def upstream(_symbol):
        nonlocal upstream_calls
        upstream_calls += 1
        return {"name": "should not be used"}

    monkeypatch.setattr(fmp_cache_service, "_get_fundamentals", db_read)
    monkeypatch.setattr(fmp_cache_service, "_try_fmp_provider_profile", upstream)
    assert fmp_cache_service.get_company_profile_cached("AMKR")["name"]
    assert fmp_cache_service.get_company_profile_cached("AMKR")["name"]
    assert db_calls == 2
    assert upstream_calls == 0


def test_unsupported_etfs_and_foreign_symbols_remain_excluded():
    etf = {"fields": {"profile": {"companyName": "SPDR S&P 500 ETF Trust", "isEtf": True}}}
    assert ei_ineligible_reason("SPY", etf) == "etf_by_provider_flag"
    assert watchlist_router._stable_cached_earnings_intelligence("SPY", etf) is None
    assert ei_ineligible_reason("LSE:VOD", None) == "foreign_exchange_prefix"


def test_existing_ticker_detail_profile_contract_is_backward_compatible():
    company = watchlist_router._merge_company_profile_fields(
        "NVDA",
        {
            "name": "NVIDIA Corporation",
            "sector": "Technology",
            "industry": "Semiconductors",
            "market_cap": 1,
            "exchange": "NASDAQ",
            "country": "US",
            "beta": 1.5,
        },
        {"profile": {}},
        {},
        {},
    )
    assert set(company) == {
        "symbol", "company_name", "sector", "industry", "market_cap",
        "exchange", "country", "beta", "website", "image", "ceo", "description",
    }
