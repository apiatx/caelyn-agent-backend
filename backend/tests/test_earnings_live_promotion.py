from __future__ import annotations

import asyncio
import os
import sys

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from services import earnings_monitor_service as monitor


def _event(**overrides):
    event = {
        "event_id": "earn_amkr_q2",
        "symbol": "AMKR",
        "expected_date": "2026-07-27",
        "fiscal_year": 2026,
        "fiscal_period": "Q2",
        "state": "results_available",
        "classification": "double_beat",
        "results_payload": {
            "eps_actual": 0.33, "eps_estimate": 0.23,
            "eps_surprise_amount": 0.10, "eps_surprise_pct": 43.48,
            "revenue_actual": 1_684_701_000, "revenue_estimate": 1_630_532_000,
            "revenue_surprise_amount": 54_169_000, "revenue_surprise_pct": 3.32,
        },
        "reaction_payload": None,
        "filing_payload": None,
    }
    event.update(overrides)
    return event


def test_valid_results_are_promoted_as_latest_without_reaction_or_materials():
    existing = {
        "earnings_history": [{"date": "2026-04-27", "fiscal_year": "2026", "fiscal_period": "Q1"}],
        "ratings": {"consensus": {"buy": 4}},
    }
    ei = monitor._merge_live_event_into_ei(existing, _event())
    latest = ei["earnings_history"][0]
    assert latest["fiscal_period"] == "Q2"
    assert latest["date"] == "2026-07-27"
    assert latest["results_status"] == "reported"
    assert latest["reaction_status"] == "reaction_pending"
    assert latest["materials_status"] == "materials_pending"
    assert ei["ratings"]["consensus"]["buy"] == 4


def test_later_reaction_and_materials_merge_into_same_fiscal_event_without_duplicate():
    initial = monitor._merge_live_event_into_ei({}, _event())
    enriched = monitor._merge_live_event_into_ei(initial, _event(
        filing_payload={"latest_earnings_packet": {"accession_number": "x"}},
        reaction_payload={"post_1d_pct": 2.5, "reactions_final": True},
    ))
    q2 = [r for r in enriched["earnings_history"] if r.get("fiscal_period") == "Q2"]
    assert len(q2) == 1
    assert q2[0]["price_reaction"]["post_1d_pct"] == 2.5
    assert q2[0]["reaction_status"] == "available"
    assert q2[0]["materials_status"] == "available"


def test_partial_later_update_does_not_erase_valid_results_or_other_ticker_events():
    existing = {
        "earnings_history": [
            {"date": "2026-07-27", "fiscal_year": "2026", "fiscal_period": "Q2", "eps_actual": 0.33, "revenue_actual": 1_684_701_000},
        ]
    }
    partial = _event(results_payload={"eps_actual": 0.34, "revenue_actual": None})
    merged = monitor._merge_live_event_into_ei(existing, partial)
    amkr = next(row for row in merged["earnings_history"] if row.get("event_id") == "earn_amkr_q2")
    assert amkr["eps_actual"] == 0.34
    assert amkr["revenue_actual"] == 1_684_701_000
    intc = monitor._merge_live_event_into_ei(
        {"earnings_history": [{"date": "2026-07-24", "fiscal_year": "2026", "fiscal_period": "Q2", "eps_actual": 2.0}]},
        _event(symbol="INTC", event_id="earn_intc_q2", expected_date="2026-07-24", results_payload={"eps_actual": 2.1}),
    )
    assert intc["earnings_history"][0]["eps_actual"] == 2.1


def test_promotion_writes_existing_ei_cache_immediately(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        "data.watchlist_fundamentals_store.get_snapshot",
        lambda _symbol: {"fields": {"earnings_intelligence": {"earnings_history": []}}},
    )
    monkeypatch.setattr(
        "data.watchlist_fundamentals_store.merge_fields",
        lambda symbol, fields: captured.update(symbol=symbol, fields=fields) or True,
    )
    assert asyncio.run(monitor._promote_live_event_to_ei(_event()))
    assert captured["symbol"] == "AMKR"
    assert captured["fields"]["earnings_intelligence"]["earnings_history"][0]["fiscal_period"] == "Q2"


def test_results_alert_is_deduplicated_and_contains_earnings_route(monkeypatch):
    calls = []
    monkeypatch.setattr("services.alert_signal_bus.has_alert_dedupe_key", lambda *_args: False)
    monkeypatch.setattr("services.alert_signal_bus._write_alert_sync", lambda record: calls.append(record) or (1, "now"))
    monitor._fire_alert_for_event(_event(), "default")
    assert len(calls) == 1
    assert any(t["key"] == "dedupe_key" and t["value"] == "AMKR|2026|Q2|earnings_results" for t in calls[0]["source_tags"])
    assert any(t["key"] == "route" for t in calls[0]["source_tags"])

    monkeypatch.setattr("services.alert_signal_bus.has_alert_dedupe_key", lambda *_args: True)
    monitor._fire_alert_for_event(_event(reaction_payload={"post_1d_pct": 1.0}), "default")
    assert len(calls) == 1
