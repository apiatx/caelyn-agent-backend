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


def _price_bars(start_day: date, closes: list[float | None]) -> list[dict]:
    return [
        {
            "date": (start_day + timedelta(days=idx)).isoformat(),
            "close": close,
            "volume": 100.0,
        }
        for idx, close in enumerate(closes)
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


def test_compute_watchlist_price_metrics_returns_positive_and_negative_7d_and_30d():
    start = date(2026, 1, 1)
    positive = chs._compute_watchlist_market_metrics_from_bars(
        _price_bars(start, ([100.0] * 30) + [120.0])
    )
    negative = chs._compute_watchlist_market_metrics_from_bars(
        _price_bars(start, ([100.0] * 30) + [80.0])
    )

    assert positive["change_7d"] == 20.0
    assert positive["change_30d"] == 20.0
    assert negative["change_7d"] == -20.0
    assert negative["change_30d"] == -20.0


def test_compute_watchlist_price_metrics_excludes_current_ny_date_partial_bar():
    ny_today = chs.datetime.now(chs.ZoneInfo("America/New_York")).date()
    completed_bars = _price_bars(ny_today - timedelta(days=31), ([100.0] * 30) + [120.0])
    partial_today = {"date": ny_today.isoformat(), "close": 9_999_999.0, "volume": 9_999_999.0}

    completed_metrics = chs._compute_watchlist_market_metrics_from_bars(completed_bars)
    metrics_with_partial_today = chs._compute_watchlist_market_metrics_from_bars(
        completed_bars + [partial_today]
    )

    assert metrics_with_partial_today == completed_metrics
    assert metrics_with_partial_today["change_7d"] == 20.0
    assert metrics_with_partial_today["change_30d"] == 20.0


def test_compute_watchlist_price_metrics_preserves_zero_and_rejects_invalid_comparisons():
    zero_returns = chs._compute_watchlist_market_metrics_from_bars(
        _price_bars(date(2026, 1, 1), [100.0] * 31)
    )
    invalid_comparisons = chs._compute_watchlist_market_metrics_from_bars(
        _price_bars(date(2026, 1, 1), [0.0] + ([100.0] * 22) + [None] + ([100.0] * 7))
    )
    insufficient_history = chs._compute_watchlist_market_metrics_from_bars(
        _price_bars(date(2026, 1, 1), [100.0] * 7)
    )

    assert zero_returns["change_7d"] == 0.0
    assert zero_returns["change_30d"] == 0.0
    assert invalid_comparisons["change_7d"] is None
    assert invalid_comparisons["change_30d"] is None
    assert insufficient_history["change_7d"] is None
    assert insufficient_history["change_30d"] is None


def test_get_volume_metrics_bulk_uses_existing_local_metadata_without_preload(monkeypatch):
    monkeypatch.setattr(
        chs,
        "_INDEX",
        {
            "AAOI": {
                "volume_metrics_status": "ok",
                "change_7d": 0.0,
                "change_30d": -7.8,
            }
        },
    )
    monkeypatch.setattr(
        chs,
        "preload_index",
        lambda: (_ for _ in ()).throw(AssertionError("must not fetch or preload")),
    )
    monkeypatch.setattr(
        chs,
        "get_bars",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not make history calls")),
    )

    metrics = chs.get_volume_metrics_bulk(["AAOI"])

    assert metrics["AAOI"]["change_7d"] == 0.0
    assert metrics["AAOI"]["change_30d"] == -7.8


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
                "change_7d": 12.4,
                "change_30d": -7.8,
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
    assert row["change_7d"] == 12.4
    assert row["change_30d"] == -7.8


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
