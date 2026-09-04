import asyncio
from pathlib import Path

import pytest

from data import caelyn_terminal as terminal


class _HistoryTradier:
    def __init__(self, *, background_result=None):
        self.normal_calls = []
        self.background_calls = []
        self.active = 0
        self.max_active = 0
        self.background_result = background_result or {}

    async def get_history(self, symbol, interval, start):
        self.normal_calls.append((symbol, interval, start))
        return [{"date": "2026-01-01", "close": 1.0}] * 5

    async def get_history_background(
        self, symbol, interval, start, end, *, lane, reserve
    ):
        self.background_calls.append((symbol, interval, start, end, lane, reserve))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0.01)
        self.active -= 1
        return self.background_result.get(symbol, [])


class _MemoryCache:
    def __init__(self):
        self.values = {}
        self.set_calls = []

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, ttl):
        self.values[key] = value
        self.set_calls.append((key, value, ttl))

    def delete(self, key):
        self.values.pop(key, None)


@pytest.mark.asyncio
async def test_interactive_history_uses_normal_provider_semantics():
    tradier = _HistoryTradier()
    provider = terminal.CaelynTerminalProvider(tradier, None, None, None)

    rows = await provider._fetch_tradier_histories(
        ["AAPL", "MSFT"], "2025-01-01"
    )

    assert set(rows) == {"AAPL", "MSFT"}
    assert len(tradier.normal_calls) == 2
    assert tradier.background_calls == []


@pytest.mark.asyncio
async def test_background_history_uses_existing_admission_and_is_bounded(monkeypatch):
    tradier = _HistoryTradier(
        background_result={
            f"T{i}": [{"date": "2026-01-01", "close": 1.0}] * 5
            for i in range(10)
        }
    )
    provider = terminal.CaelynTerminalProvider(tradier, None, None, None)
    monkeypatch.setattr(terminal, "cache", _MemoryCache())

    rows = await provider._fetch_tradier_histories(
        [f"T{i}" for i in range(10)],
        "2025-01-01",
        background_prewarm=True,
    )

    assert len(rows) == 10
    assert tradier.normal_calls == []
    assert len(tradier.background_calls) == 10
    assert tradier.max_active <= 4
    assert provider._background_history_stats["max_concurrency"] <= 4
    assert provider._background_history_stats["admissions"] == 10


@pytest.mark.asyncio
async def test_background_deferral_returns_promptly_without_yahoo_fallback(monkeypatch):
    tradier = _HistoryTradier()
    provider = terminal.CaelynTerminalProvider(tradier, None, None, None)
    monkeypatch.setattr(terminal, "cache", _MemoryCache())

    rows = await asyncio.wait_for(
        provider._fetch_tradier_histories(
            ["AAPL", "MSFT"],
            "2025-01-01",
            background_prewarm=True,
        ),
        timeout=0.2,
    )

    assert rows == {"AAPL": [], "MSFT": []}
    assert provider._background_history_stats["deferred"] == 2
    assert provider._background_history_stats["yahoo_fallbacks_from_deferrals"] == 0
    assert provider._history_fallback_symbols(
        ["AAPL", "MSFT"], rows, background_prewarm=True
    ) == []
    assert provider._history_fallback_symbols(
        ["AAPL", "MSFT"], rows
    ) == ["AAPL", "MSFT"]


@pytest.mark.asyncio
async def test_partial_prewarm_does_not_poison_normal_cache(monkeypatch, tmp_path):
    fake_cache = _MemoryCache()
    monkeypatch.setattr(terminal, "cache", fake_cache)
    provider = terminal.CaelynTerminalProvider(None, None, None, None)
    portfolio_file = tmp_path / "portfolio.json"
    holdings = [{"ticker": "AAPL", "shares": 1, "avg_cost": 100}]
    monkeypatch.setattr(provider, "_load_holdings", lambda _: holdings)
    calls = []

    async def fake_build(_, *, background_prewarm=False):
        calls.append(background_prewarm)
        provider._background_history_stats = {
            "deferred": 1 if background_prewarm else 0
        }
        return {"_holdings_sig": provider._holdings_sig(holdings), "positions": []}

    monkeypatch.setattr(provider, "_build", fake_build)

    await provider.get(portfolio_file, background_prewarm=True)
    assert fake_cache.set_calls == []

    result = await provider.get(portfolio_file)
    assert result["positions"] == []
    assert calls == [True, False]
    assert len(fake_cache.set_calls) == 1
    assert fake_cache.set_calls[0][2] == 300


@pytest.mark.asyncio
async def test_background_history_reuses_existing_cache(monkeypatch):
    fake_cache = _MemoryCache()
    monkeypatch.setattr(terminal, "cache", fake_cache)
    start = "2025-01-01"
    end = terminal.date.today().isoformat()
    bars = [{"date": "2026-01-01", "close": 1.0}] * 5
    fake_cache.values[f"tradier:history:AAPL:daily:{start}:{end}"] = bars
    tradier = _HistoryTradier()
    provider = terminal.CaelynTerminalProvider(tradier, None, None, None)

    rows = await provider._fetch_tradier_histories(
        ["AAPL"], start, background_prewarm=True
    )

    assert rows["AAPL"] == bars
    assert tradier.background_calls == []
    assert provider._background_history_stats["cache_hits"] == 1