from __future__ import annotations

import os
import sys
from datetime import date, timedelta


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(THIS_DIR)
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

import asyncio

from services import canonical_history_service as chs  # noqa: E402
from services import watchlist_router as wr  # noqa: E402
from services.watchlist_fundamentals_refresh import FmpFundamentalsRefresher  # noqa: E402


def _bars(start_day: date, volumes: list[float]) -> list[dict]:
    return [
        {"date": (start_day + timedelta(days=idx)).isoformat(), "volume": volume}
        for idx, volume in enumerate(volumes)
    ]


def test_resolve_cached_watchlist_beta_accepts_zero_and_rejects_nonfinite():
    assert wr._resolve_cached_watchlist_beta({"fields": {"profile": {"beta": 0.0}}}) == 0.0
    assert wr._resolve_cached_watchlist_beta({"fields": {"profile": {"beta": "1.25"}}}) == 1.25
    assert wr._resolve_cached_watchlist_beta({"fields": {"profile": {"beta": "nan"}}}) is None
    assert wr._resolve_cached_watchlist_beta({"fields": {"profile": {"beta": None}}}) is None


def test_compute_watchlist_volume_metrics_from_canonical_history():
    volumes = ([100.0] * 30) + ([150.0] * 23) + ([300.0] * 6) + [450.0]
    metrics = chs._compute_volume_metrics_from_bars(_bars(date(2026, 1, 1), volumes))

    assert metrics["volume_change_1d_pct"] == 50.0
    assert metrics["volume_change_7d_pct"] == 114.285714
    assert metrics["volume_change_30d_pct"] == 90.0
    assert metrics["volume_acceleration_pp"] == 24.285714
    assert metrics["volume_metrics_as_of"] == "2026-03-01"
    assert metrics["volume_metrics_status"] == "ok"


def test_compute_watchlist_volume_metrics_marks_insufficient_history():
    metrics = chs._compute_volume_metrics_from_bars(_bars(date(2026, 1, 1), [100.0] * 10))

    assert metrics["volume_change_1d_pct"] == 0.0
    assert metrics["volume_change_7d_pct"] is None
    assert metrics["volume_change_30d_pct"] is None
    assert metrics["volume_acceleration_pp"] is None
    assert metrics["volume_metrics_status"] == "insufficient_history"


def test_compute_watchlist_volume_metrics_excludes_current_ny_date_partial_bar():
    ny_today = chs.datetime.now(chs.ZoneInfo("America/New_York")).date()
    completed_bars = _bars(ny_today - timedelta(days=60), [100.0] * 60)
    partial_today = {"date": ny_today.isoformat(), "volume": 9_999_999.0}

    completed_metrics = chs._compute_volume_metrics_from_bars(completed_bars)
    metrics_with_partial_today = chs._compute_volume_metrics_from_bars(
        completed_bars + [partial_today]
    )

    assert metrics_with_partial_today == completed_metrics
    assert metrics_with_partial_today["volume_metrics_as_of"] == (
        ny_today - timedelta(days=1)
    ).isoformat()


def test_enrich_store_with_quotes_adds_cached_beta_and_volume_metrics(monkeypatch):
    async def _fake_quotes(_symbols):
        return {
            "AAOI": {
                "name": "Applied Optoelectronics",
                "price": 19.5,
                "change_pct_1d": 4.2,
                "volume": 1200,
                "average_volume": 600,
                "relative_volume": 2.0,
                "quote_source": "lkg",
                "quote_updated_at": "2026-07-14T20:00:00Z",
            }
        }

    monkeypatch.setattr("services.watchlist_quote_cache.get_watchlist_quotes", _fake_quotes)
    monkeypatch.setattr("services.name_overrides.get_name_overrides", lambda scope: {})
    monkeypatch.setattr(
        "data.watchlist_fundamentals_store.get_snapshots_bulk",
        lambda symbols: {
            "AAOI": {
                "fields": {"profile": {"beta": 0.0}},
                "refreshed_at": "2026-07-21T18:17:50.009689+00:00",
            }
        },
    )
    monkeypatch.setattr(
        "services.canonical_history_service.get_volume_metrics_bulk",
        lambda tickers: {
            "AAOI": {
                "volume_change_1d_pct": 50.0,
                "volume_change_7d_pct": 100.0,
                "volume_change_30d_pct": 80.0,
                "volume_acceleration_pp": 20.0,
                "volume_metrics_as_of": "2026-07-14",
                "volume_metrics_status": "ok",
            }
        },
    )
    monkeypatch.setattr(wr, "_get_stage2_breakout", lambda sym: {"score": None, "label": None, "reason": None})

    store = {
        "id": "wl-1",
        "tickers": ["AAOI"],
        "csv_data": [],
        "analysis": {
            "sections": [
                {
                    "name": "Test",
                    "tickers": [{"symbol": "AAOI", "name": None}],
                }
            ]
        },
    }

    enriched = asyncio.run(wr._enrich_store_with_quotes(store))
    row = enriched["analysis"]["sections"][0]["tickers"][0]

    assert row["beta"] == 0.0
    assert row["volume_change_1d_pct"] == 50.0
    assert row["volume_change_7d_pct"] == 100.0
    assert row["volume_change_30d_pct"] == 80.0
    assert row["volume_acceleration_pp"] == 20.0
    assert row["volume_metrics_as_of"] == "2026-07-14"
    assert row["volume_metrics_status"] == "ok"


def test_normalize_symbol_preserves_zero_beta(monkeypatch):
    refresher = FmpFundamentalsRefresher("test-key")

    async def _fake_get(endpoint: str, params: dict | None = None):
        if endpoint == "profile":
            return [{
                "marketCap": 1000,
                "price": 10,
                "beta": 0.0,
                "description": "x",
            }]
        return []

    async def _fake_get_quality(endpoint: str, params: dict | None = None):
        return [], "success_no_data"

    monkeypatch.setattr(refresher, "_get", _fake_get)
    monkeypatch.setattr(refresher, "_get_quality", _fake_get_quality)

    result = asyncio.run(refresher.normalize_symbol("ZERO"))

    assert result["fields"]["profile"]["beta"] == 0.0
