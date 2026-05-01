"""
Validation tests for the realtime quotes service.

These tests do NOT hit the network — they substitute fake provider classes
that mimic the real provider response shapes so we can verify:
  - vendor priority order (Tradier -> Public -> FMP -> Twelve -> LKG)
  - per-symbol failure isolation (one vendor failing one symbol doesn't 500)
  - invalid symbols return per-symbol errors without killing valid ones
  - normalized quote shape contains required freshness metadata fields
"""
from __future__ import annotations

import asyncio
import sys
import os

# Ensure backend cwd is on the path
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(THIS_DIR)
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from services.realtime_quotes_service import (  # noqa: E402
    RealtimeQuotesService,
    RealtimeQuote,
    SOURCE_TRADIER,
    SOURCE_PUBLIC,
    SOURCE_FMP,
    SOURCE_LKG,
)


REQUIRED_FIELDS = [
    "symbol", "price", "last", "bid", "ask", "open", "high", "low", "close",
    "prev_close", "change", "change_percent", "volume", "trade_timestamp",
    "quote_timestamp", "source", "is_realtime", "is_live_backup", "is_stale",
    "staleness_seconds", "market_session", "error",
]


class FakeTradier:
    """Returns Tradier-shape quotes for `available` symbols only."""
    def __init__(self, available: set[str]):
        self.available = available
        self.calls = 0

    async def get_quotes(self, symbols):
        self.calls += 1
        out = []
        for s in symbols:
            if s.upper() in self.available:
                out.append({
                    "symbol": s.upper(),
                    "last": 100.0,
                    "bid": 99.95,
                    "ask": 100.05,
                    "change": 1.5,
                    "change_percentage": 1.5,
                    "volume": 12345,
                    "open": 99.0,
                    "high": 101.0,
                    "low": 98.5,
                    "close": 99.5,
                    "prevclose": 99.5,
                })
        return out


class FakePublic:
    """Public.com-shaped get_quotes — only for `available` symbols."""
    def __init__(self, available: set[str]):
        self.available = available
        self.calls = 0

    async def get_quotes(self, symbols, instrument_type="OPTION"):
        self.calls += 1
        out = []
        for s in symbols:
            if s.upper() in self.available:
                out.append({
                    "instrument": {"symbol": s.upper(), "type": instrument_type},
                    "last": 200.0,
                    "bid": 199.9,
                    "ask": 200.1,
                    "volume": 555,
                })
        return out


class FakeFMP:
    """FMP-shaped get_quote — single ticker."""
    def __init__(self, available: set[str]):
        self.available = available
        self.calls = 0

    async def get_quote(self, symbol):
        self.calls += 1
        if symbol.upper() in self.available:
            return {
                "price": 300.0,
                "change": 2.0,
                "changesPercentage": 0.6,
                "previousClose": 298.0,
                "volume": 9999,
                "dayHigh": 301,
                "dayLow": 297,
            }
        return {}


def _run(coro):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


def _reset_cache():
    from data.cache import cache as _c
    _c.clear()


def test_tradier_primary_path():
    _reset_cache()
    svc = RealtimeQuotesService(
        tradier_provider=FakeTradier({"NVDA", "TSLA", "AAPL"}),
        public_provider=None,
        fmp_provider=None,
        twelve_api_key=None,
    )
    quotes = _run(svc.get_realtime_quotes(["NVDA", "TSLA", "AAPL"]))
    assert set(quotes.keys()) == {"NVDA", "TSLA", "AAPL"}
    for sym, q in quotes.items():
        assert q.source == SOURCE_TRADIER, f"{sym}: source={q.source}"
        assert q.is_realtime is True
        assert q.is_live_backup is False
        assert q.is_stale is False
        assert q.price == 100.0
        for f in REQUIRED_FIELDS:
            assert hasattr(q, f), f"missing field {f}"
    print("test_tradier_primary_path PASSED")


def test_tradier_fail_public_fallback():
    _reset_cache()
    svc = RealtimeQuotesService(
        tradier_provider=FakeTradier({"NVDA"}),  # only NVDA succeeds
        public_provider=FakePublic({"TSLA", "AAPL"}),
        fmp_provider=None,
        twelve_api_key=None,
    )
    quotes = _run(svc.get_realtime_quotes(["NVDA", "TSLA", "AAPL"]))
    assert quotes["NVDA"].source == SOURCE_TRADIER
    assert quotes["TSLA"].source == SOURCE_PUBLIC
    assert quotes["TSLA"].is_live_backup is True
    assert quotes["AAPL"].source == SOURCE_PUBLIC
    print("test_tradier_fail_public_fallback PASSED")


def test_public_unavailable_fmp_fallback():
    """When Public.com is not configured, FMP serves as the next fallback."""
    _reset_cache()
    svc = RealtimeQuotesService(
        tradier_provider=FakeTradier(set()),  # all fail
        public_provider=None,
        fmp_provider=FakeFMP({"NVDA", "TSLA", "AAPL"}),
        twelve_api_key=None,
    )
    quotes = _run(svc.get_realtime_quotes(["NVDA", "TSLA", "AAPL"]))
    for sym, q in quotes.items():
        assert q.source == SOURCE_FMP
        assert q.is_realtime is False
        assert q.is_live_backup is False
        assert q.is_stale is True
        assert q.price == 300.0
    print("test_public_unavailable_fmp_fallback PASSED")


def test_all_vendors_fail_returns_per_symbol_errors():
    _reset_cache()
    svc = RealtimeQuotesService(
        tradier_provider=FakeTradier(set()),
        public_provider=FakePublic(set()),
        fmp_provider=FakeFMP(set()),
        twelve_api_key=None,
    )
    quotes = _run(svc.get_realtime_quotes(["NVDA", "TSLA"]))
    for q in quotes.values():
        assert q.error is not None
        assert q.is_stale is True
        assert q.is_realtime is False
    print("test_all_vendors_fail_returns_per_symbol_errors PASSED")


def test_lkg_fallback_after_cache_set():
    _reset_cache()
    svc = RealtimeQuotesService(
        tradier_provider=FakeTradier({"NVDA"}),
        public_provider=None,
        fmp_provider=None,
        twelve_api_key=None,
    )
    # Prime LKG via successful Tradier call
    _run(svc.get_realtime_quotes(["NVDA"]))
    # Now make Tradier fail entirely — LKG should be used
    svc.tradier = FakeTradier(set())
    # Bypass live cache by using a different symbol set tactic — clear live entry
    from data.cache import cache
    cache._store.pop("realtime_quote:live:NVDA", None)
    quotes = _run(svc.get_realtime_quotes(["NVDA"]))
    assert quotes["NVDA"].source == SOURCE_LKG
    assert quotes["NVDA"].is_stale is True
    assert quotes["NVDA"].price == 100.0
    print("test_lkg_fallback_after_cache_set PASSED")


def test_partial_failure_isolation():
    """One vendor failing on one symbol shouldn't affect others."""
    _reset_cache()
    svc = RealtimeQuotesService(
        tradier_provider=FakeTradier({"NVDA"}),
        public_provider=FakePublic({"TSLA"}),
        fmp_provider=FakeFMP({"AAPL"}),
        twelve_api_key=None,
    )
    quotes = _run(svc.get_realtime_quotes(["NVDA", "TSLA", "AAPL", "BOGUS"]))
    assert quotes["NVDA"].source == SOURCE_TRADIER
    assert quotes["TSLA"].source == SOURCE_PUBLIC
    assert quotes["AAPL"].source == SOURCE_FMP
    assert quotes["BOGUS"].error is not None
    print("test_partial_failure_isolation PASSED")


if __name__ == "__main__":
    test_tradier_primary_path()
    test_tradier_fail_public_fallback()
    test_public_unavailable_fmp_fallback()
    test_all_vendors_fail_returns_per_symbol_errors()
    test_lkg_fallback_after_cache_set()
    test_partial_failure_isolation()
    print("\nALL TESTS PASSED")
