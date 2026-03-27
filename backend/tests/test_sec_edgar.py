import asyncio
import pytest
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from data.sec_edgar_provider import SecEdgarProvider, EdgarBudget, _refill_tokens
from data.cache import cache


MOCK_TICKERS_JSON = {
    "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
    "1": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft Corporation"},
    "2": {"cik_str": 1318605, "ticker": "TSLA", "title": "Tesla, Inc."},
    "3": {"cik_str": 1652044, "ticker": "GOOG", "title": "Alphabet Inc."},
}

MOCK_SUBMISSIONS_JSON = {
    "cik": "0000320193",
    "entityType": "operating",
    "name": "Apple Inc.",
    "filings": {
        "recent": {
            "form": ["8-K", "4", "10-Q", "4", "S-3", "8-K", "4", "10-K", "4", "424B5"],
            "filingDate": [
                "2026-02-15", "2026-02-10", "2026-01-30", "2026-01-25",
                "2026-01-20", "2026-01-15", "2026-01-10", "2025-12-20",
                "2025-12-15", "2025-12-10",
            ],
            "primaryDocDescription": [
                "Current Report", "Statement of Changes - acquisition",
                "Quarterly Report", "Statement of Changes - disposition",
                "Registration Statement", "Current Report - earnings",
                "Statement of Changes - purchase", "Annual Report",
                "Statement of Changes - sale", "Prospectus Supplement",
            ],
            "accessionNumber": [
                "0001-23-456789", "0001-23-456790", "0001-23-456791",
                "0001-23-456792", "0001-23-456793", "0001-23-456794",
                "0001-23-456795", "0001-23-456796", "0001-23-456797",
                "0001-23-456798",
            ],
            "primaryDocument": [
                "doc1.htm", "doc2.htm", "doc3.htm", "doc4.htm", "doc5.htm",
                "doc6.htm", "doc7.htm", "doc8.htm", "doc9.htm", "doc10.htm",
            ],
        }
    }
}


class MockResponse:
    def __init__(self, json_data, status_code=200):
        self._json = json_data
        self.status_code = status_code

    def json(self):
        return self._json


class MockClient:
    def __init__(self, responses=None):
        self.responses = responses or {}
        self.requests = []
        self.is_closed = False

    async def get(self, url, **kwargs):
        self.requests.append(url)
        for pattern, resp in self.responses.items():
            if pattern in url:
                return resp
        return MockResponse({}, 404)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


@pytest.fixture(autouse=True)
def clear_caches():
    cache.clear()
    import data.sec_edgar_provider as m
    m._cik_map = None
    m._cik_map_loaded_at = 0.0
    m._token_bucket_tokens = 2.0
    m._token_bucket_last = 0.0
    m._last_error = None
    m._circuit_open = False
    m._circuit_opened_at = 0.0
    yield
    cache.clear()


@pytest.fixture
def provider():
    return SecEdgarProvider()


@pytest.fixture
def mock_client():
    return MockClient({
        "company_tickers.json": MockResponse(MOCK_TICKERS_JSON),
        "submissions/CIK0000320193": MockResponse(MOCK_SUBMISSIONS_JSON),
        "submissions/CIK0000789019": MockResponse(MOCK_SUBMISSIONS_JSON),
    })


def test_edgar_budget_basic():
    b = EdgarBudget(max_requests=3)
    assert b.can_spend()
    b.spend()
    b.spend()
    b.spend()
    assert not b.can_spend()
    b.record_blocked()
    s = b.summary()
    assert s["edgar_requests"] == 3
    assert s["edgar_blocked"] == 1


def test_edgar_budget_cache_hit():
    b = EdgarBudget(max_requests=2)
    b.record_cache_hit()
    assert b.can_spend()
    s = b.summary()
    assert s["edgar_cache_hits"] == 1
    assert s["edgar_requests"] == 0


@pytest.mark.asyncio
async def test_resolve_cik(provider, mock_client):
    provider._client = mock_client
    cik = await provider.resolve_cik("AAPL")
    assert cik == "0000320193"


@pytest.mark.asyncio
async def test_resolve_cik_cached(provider, mock_client):
    provider._client = mock_client
    cik1 = await provider.resolve_cik("AAPL")
    assert cik1 == "0000320193"
    req_count_1 = len(mock_client.requests)

    cik2 = await provider.resolve_cik("AAPL")
    assert cik2 == "0000320193"
    assert len(mock_client.requests) == req_count_1


@pytest.mark.asyncio
async def test_resolve_cik_unknown(provider, mock_client):
    provider._client = mock_client
    cik = await provider.resolve_cik("ZZZZ")
    assert cik is None


@pytest.mark.asyncio
async def test_get_recent_filings(provider, mock_client):
    provider._client = mock_client
    filings = await provider.get_recent_filings("0000320193", lookback_days=60, limit=10)
    assert len(filings) >= 5
    for f in filings:
        assert "form" in f
        assert "filed_at" in f
        assert "url" in f


@pytest.mark.asyncio
async def test_get_recent_filings_filter_form_type(provider, mock_client):
    provider._client = mock_client
    filings = await provider.get_recent_filings(
        "0000320193", form_types=["8-K"], lookback_days=60, limit=10,
    )
    for f in filings:
        assert f["form"].startswith("8-K")


@pytest.mark.asyncio
async def test_get_recent_filings_cached(provider, mock_client):
    provider._client = mock_client
    budget = EdgarBudget(max_requests=3)
    f1 = await provider.get_recent_filings("0000320193", lookback_days=60, budget=budget)
    assert budget.used == 1

    f2 = await provider.get_recent_filings("0000320193", lookback_days=60, budget=budget)
    assert budget.used == 1
    assert budget.cache_hits == 1
    assert f1 == f2


@pytest.mark.asyncio
async def test_get_form4_insider_summary(provider, mock_client):
    provider._client = mock_client
    summary = await provider.get_form4_insider_summary("0000320193", lookback_days=60)
    assert "count" in summary
    assert "signal" in summary
    assert "summary" in summary
    assert summary["count"] >= 1
    assert summary["signal"] in ("net_buying", "net_selling", "mixed", "unknown")


@pytest.mark.asyncio
async def test_get_8k_s1_catalysts(provider, mock_client):
    provider._client = mock_client
    catalysts = await provider.get_8k_s1_catalysts("0000320193", lookback_days=60)
    assert len(catalysts) >= 1
    for c in catalysts:
        assert "form" in c
        assert "filed_at" in c
        assert "title" in c


@pytest.mark.asyncio
async def test_budget_blocks_when_exhausted(provider, mock_client):
    provider._client = mock_client
    budget = EdgarBudget(max_requests=1)
    f1 = await provider.get_recent_filings("0000320193", lookback_days=60, budget=budget)
    assert len(f1) >= 1

    f2 = await provider.get_recent_filings("0000789019", lookback_days=60, budget=budget)
    assert f2 == []
    assert budget.blocked == 1


@pytest.mark.asyncio
async def test_circuit_breaker_on_429(provider):
    import data.sec_edgar_provider as m

    mock_429 = MockClient({
        "company_tickers.json": MockResponse(MOCK_TICKERS_JSON),
        "submissions/": MockResponse({}, 429),
    })
    provider._client = mock_429

    result = await provider.get_recent_filings("0000320193", lookback_days=60)
    assert result == []
    assert m._circuit_open is True

    result2 = await provider.get_recent_filings("0000320193", lookback_days=60)
    assert result2 == []


def test_health_status(provider):
    health = provider.get_health()
    assert health["enabled"] is True
    assert health["circuit"] == "closed"
    assert health["last_error"] is None


def test_health_after_error():
    import data.sec_edgar_provider as m
    m._last_error = "Connection timeout"
    m._circuit_open = True

    provider = SecEdgarProvider()
    health = provider.get_health()
    assert health["circuit"] == "open"
    assert health["last_error"] == "Connection timeout"
