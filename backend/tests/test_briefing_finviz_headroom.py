import asyncio
import threading
from pathlib import Path

import pytest

from data import finviz_scraper as module
from data.finviz_scraper import (
    FinvizScraper,
    _parse_custom_screen_html,
    _parse_screener_html,
)


class _Response:
    status_code = 200
    text = "<html></html>"


class _Client:
    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def get(self, *args, **kwargs):
        return _Response()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "helper_name", "argument"),
    [
        ("get_screener_results", "_parse_screener_html", "ta_topgainers"),
        ("_custom_screen", "_parse_custom_screen_html", {"f": "ta_change_u"}),
    ],
)
async def test_finviz_parsing_runs_off_event_loop(
    monkeypatch, method, helper_name, argument
):
    monkeypatch.setattr(module.httpx, "AsyncClient", _Client)
    monkeypatch.setattr(module.cache, "get", lambda key: None)
    monkeypatch.setattr(module.cache, "set", lambda *args: None)
    loop_thread = threading.get_ident()
    parse_threads = []

    def parse(html):
        parse_threads.append(threading.get_ident())
        threading.Event().wait(0.02)
        return [{"ticker": "TEST"}]

    monkeypatch.setattr(module, helper_name, parse)
    result = await getattr(FinvizScraper(), method)(argument)
    assert result == [{"ticker": "TEST"}]
    assert parse_threads and parse_threads[0] != loop_thread


def test_parser_output_equivalence():
    cells = [
        "1", "ACME", "Acme Inc", "Technology", "Software", "USA",
        "1.2B", "10", "42.00", "3.5%", "900K", "55",
    ]
    basic = (
        "<table class='screener_table'><tr class='screener-body-table-nw'>"
        + "".join(f"<td>{cell}</td>" for cell in cells)
        + "</tr></table>"
    )
    assert _parse_screener_html(basic) == [{
        "ticker": "ACME", "company": "Acme Inc", "sector": "Technology",
        "market_cap": "1.2B", "price": "42.00", "change": "3.5%",
    }]

    headers = [
        "No.", "Ticker", "Company", "Sector", "Industry", "Country",
        "Market Cap", "P/E", "Price", "Change", "Volume", "RSI",
    ]
    custom = (
        "<table class='screener_table'><tr>"
        + "".join(f"<td>{header}</td>" for header in headers)
        + "</tr><tr>"
        + "".join(f"<td>{cell}</td>" for cell in cells)
        + "</tr></table>"
    )
    assert _parse_custom_screen_html(custom) == [{
        "ticker": "ACME", "company": "Acme Inc", "sector": "Technology",
        "industry": "Software", "market_cap": "1.2B", "price": "42.00",
        "change": "3.5%", "volume": "900K", "rsi": "55",
    }]


@pytest.mark.asyncio
async def test_briefing_runs_all_ten_in_order_with_max_two_and_releases_on_error():
    import main

    names = [
        "get_stage2_breakouts", "get_volume_breakouts", "get_macd_crossovers",
        "get_unusual_volume", "get_new_highs", "get_high_short_float",
        "get_insider_buying", "get_revenue_growth_leaders",
        "get_rsi_recovery", "get_accumulation_stocks",
    ]
    active = 0
    maximum = 0
    calls = []

    class FakeFinviz:
        pass

    fake = FakeFinviz()
    for index, name in enumerate(names):
        async def method(i=index, n=name):
            nonlocal active, maximum
            calls.append(n)
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0.01)
            active -= 1
            if i == 3:
                raise RuntimeError("expected")
            return [i]

        setattr(fake, name, method)

    diagnostics = {}
    results = await main._run_briefing_finviz_calls(fake, diagnostics)
    assert calls == names
    assert [result if isinstance(result, Exception) else result[0] for result in results][:3] == [0, 1, 2]
    assert isinstance(results[3], RuntimeError)
    assert results[4:] == [[4], [5], [6], [7], [8], [9]]
    assert maximum <= 2
    assert diagnostics["max_inflight"] <= 2
    assert diagnostics["calls"] == 10
    assert diagnostics["active"] == 0


def test_briefing_cadence_and_no_startup_catchup():
    source = (Path(__file__).parents[1] / "main.py").read_text(encoding="utf-8")
    assert "_briefing_precompute_loop(skip_initial=True)" in source
    assert "await asyncio.sleep(1800)" in source