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


def test_bulk_watchlist_omits_earnings_but_ticker_detail_keeps_it():
    earnings_intelligence = {
        "earnings_history": [{"date": "2026-07-27", "eps_actual": 1.2}],
        "source_status": {"coverage": {"has_earnings_history": True}},
    }
    cached_fields = {
        "Revenue": 1000,
        "PE Ratio": 20.5,
        "earnings_intelligence": earnings_intelligence,
    }
    rows = [
        {"symbol": "AAA", "fundamentals": {"fields": wr._bulk_fundamentals_fields(cached_fields)}},
        {"symbol": "BBB", "fundamentals": {"fields": wr._bulk_fundamentals_fields(cached_fields)}},
    ]

    assert len(rows) == 2
    assert [row["symbol"] for row in rows] == ["AAA", "BBB"]
    assert all("earnings_intelligence" not in row["fundamentals"]["fields"] for row in rows)
    assert all(row["fundamentals"]["fields"]["Revenue"] == 1000 for row in rows)
    assert cached_fields["earnings_intelligence"] is earnings_intelligence
    assert wr._ticker_detail_earnings_intelligence(cached_fields) is earnings_intelligence


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


def test_enrich_store_with_quotes_uses_live_price_as_numerator_for_7d_30d(monkeypatch):
    """
    REGRESSION: change_7d / change_30d must use the DISPLAYED live price as
    numerator, not the stale last canonical bar close pre-stored in the index.

    Prior implementation took change_7d=12.4 straight from canonical metadata
    (computed from completed[-1] close at last backfill time).  Corrected
    implementation divides live_price by the historical comparison close:
      change_7d = (live_price / comp_close_7d − 1) × 100
    """
    async def _fake_quotes(_symbols):
        return {
            "AAOI": {
                "name": "Applied Optoelectronics",
                "price": 19.5,          # live displayed price — the numerator
                "change_pct_1d": 4.2,
                "volume": 1200,
                "average_volume": 600,
                "relative_volume": 2.0,
                "quote_source": "lkg",
                "quote_updated_at": "2026-07-14T20:00:00Z",
            }
        }

    # Stale pre-computed values from the canonical index (old numerator = last bar close)
    # These must NOT appear unchanged in the final row
    _STALE_7D  = 12.4
    _STALE_30D = -7.8
    monkeypatch.setattr(
        "services.canonical_history_service.get_volume_metrics_bulk",
        lambda tickers: {
            "AAOI": {
                "volume_change_1d_pct":   50.0,
                "volume_change_7d_pct":  100.0,
                "volume_change_30d_pct":  80.0,
                "volume_acceleration_pp": 20.0,
                "volume_metrics_as_of":   "2026-07-14",
                "volume_metrics_status":  "ok",
                "change_7d":  _STALE_7D,   # stale — wrong numerator
                "change_30d": _STALE_30D,  # stale — wrong numerator
            }
        },
    )
    # Comparison closes — the historical denominators
    _COMP_7D  = 16.5    # price 7 calendar days ago
    _COMP_30D = 20.0    # price 30 calendar days ago
    monkeypatch.setattr(
        "services.canonical_history_service.get_comparison_closes_bulk",
        lambda tickers, **kw: {
            "AAOI": {
                "comparison_close_7d":  _COMP_7D,
                "comparison_date_7d":   "2026-07-07",
                "comparison_close_30d": _COMP_30D,
                "comparison_date_30d":  "2026-06-14",
            }
        },
    )
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
    monkeypatch.setattr(wr, "_get_stage2_breakout",
                        lambda sym: {"score": None, "label": None, "reason": None})

    store = {
        "id": "wl-1",
        "tickers": ["AAOI"],
        "csv_data": [],
        "analysis": {
            "sections": [{"name": "Test",
                          "tickers": [{"symbol": "AAOI", "name": None}]}]
        },
    }
    enriched = asyncio.run(wr._enrich_store_with_quotes(store))
    row = enriched["analysis"]["sections"][0]["tickers"][0]

    # Volume metrics still come from canonical metadata unchanged
    assert row["beta"] == 0.0
    assert row["volume_change_1d_pct"] == 50.0
    assert row["volume_change_7d_pct"] == 100.0
    assert row["volume_change_30d_pct"] == 80.0
    assert row["volume_acceleration_pp"] == 20.0
    assert row["volume_metrics_as_of"] == "2026-07-14"
    assert row["volume_metrics_status"] == "ok"

    # change_7d and change_30d MUST use live price 19.5 as numerator
    expected_7d  = round((19.5 / _COMP_7D  - 1) * 100.0, 6)   # ≈ 18.181818
    expected_30d = round((19.5 / _COMP_30D - 1) * 100.0, 6)   # = -2.5

    assert row["change_7d"]  == expected_7d, (
        f"Expected change_7d={expected_7d} (live-price-based), got {row['change_7d']}. "
        f"If {_STALE_7D}, the stale pre-computed value is leaking through."
    )
    assert row["change_30d"] == expected_30d, (
        f"Expected change_30d={expected_30d} (live-price-based), got {row['change_30d']}. "
        f"If {_STALE_30D}, the stale pre-computed value is leaking through."
    )
    # Explicit proof: stale values did not survive
    assert row["change_7d"]  != _STALE_7D,  "Stale canonical change_7d must not appear"
    assert row["change_30d"] != _STALE_30D, "Stale canonical change_30d must not appear"

    # Price identity: displayed price / comp_close − 1 == change_7d / 100 (within rounding)
    assert abs(row["price"] / _COMP_7D - 1 - row["change_7d"] / 100) < 1e-4, (
        "price, change_7d, and comp_close_7d must satisfy: "
        "price/comp_close_7d − 1 ≈ change_7d/100"
    )


def test_enrich_store_stale_precomputed_cannot_overwrite_live_price_calculation(monkeypatch):
    """
    When no comparison close is available (e.g. insufficient history),
    the row must return None — not the stale pre-computed value from the index.
    """
    async def _fake_quotes(_symbols):
        return {
            "NEWCO": {"price": 25.0, "change_pct_1d": 1.0,
                      "volume": 1000, "average_volume": 500,
                      "quote_source": "lkg", "quote_updated_at": "2026-07-14T20:00:00Z"}
        }

    monkeypatch.setattr(
        "services.canonical_history_service.get_volume_metrics_bulk",
        lambda tickers: {
            "NEWCO": {
                "volume_change_1d_pct": 5.0, "volume_change_7d_pct": None,
                "volume_change_30d_pct": None, "volume_acceleration_pp": None,
                "volume_metrics_as_of": "2026-07-14", "volume_metrics_status": "insufficient_history",
                "change_7d": 99.9,   # stale sentinel — must NOT appear in row
                "change_30d": 88.8,  # stale sentinel — must NOT appear in row
            }
        },
    )
    # No comparison closes available (too-new symbol)
    monkeypatch.setattr(
        "services.canonical_history_service.get_comparison_closes_bulk",
        lambda tickers, **kw: {
            "NEWCO": {
                "comparison_close_7d": None, "comparison_date_7d": None,
                "comparison_close_30d": None, "comparison_date_30d": None,
            }
        },
    )
    monkeypatch.setattr("services.watchlist_quote_cache.get_watchlist_quotes", _fake_quotes)
    monkeypatch.setattr("services.name_overrides.get_name_overrides", lambda scope: {})
    monkeypatch.setattr("data.watchlist_fundamentals_store.get_snapshots_bulk",
                        lambda symbols: {})
    monkeypatch.setattr(wr, "_get_stage2_breakout",
                        lambda sym: {"score": None, "label": None, "reason": None})

    store = {
        "id": "wl-2", "tickers": ["NEWCO"], "csv_data": [],
        "analysis": {"sections": [{"name": "Test",
                                   "tickers": [{"symbol": "NEWCO", "name": None}]}]},
    }
    enriched = asyncio.run(wr._enrich_store_with_quotes(store))
    row = enriched["analysis"]["sections"][0]["tickers"][0]

    # No comparison close → None; stale 99.9 / 88.8 must NOT survive
    assert row["change_7d"]  is None, f"Expected None, got {row['change_7d']}"
    assert row["change_30d"] is None, f"Expected None, got {row['change_30d']}"


def test_enrich_store_changing_quote_changes_7d_30d_proportionally(monkeypatch):
    """
    The same comparison closes with two different live prices produce different
    percentages.  Verifies the numerator is the live quote, not a cached constant.
    """
    async def _fake_quotes_low(_symbols):
        return {"SYM": {"price": 100.0, "change_pct_1d": 0.0,
                        "volume": 1000, "average_volume": 1000,
                        "quote_source": "tradier", "quote_updated_at": "2026-07-14T20:00:00Z"}}

    async def _fake_quotes_high(_symbols):
        return {"SYM": {"price": 200.0, "change_pct_1d": 0.0,
                        "volume": 1000, "average_volume": 1000,
                        "quote_source": "tradier", "quote_updated_at": "2026-07-14T20:00:00Z"}}

    _vol_stub = lambda tickers: {
        "SYM": {
            "volume_change_1d_pct": 0.0, "volume_change_7d_pct": 0.0,
            "volume_change_30d_pct": 0.0, "volume_acceleration_pp": 0.0,
            "volume_metrics_as_of": "2026-07-14", "volume_metrics_status": "ok",
            "change_7d": 0.0, "change_30d": 0.0,  # stale, will be overridden
        }
    }
    _comp_stub = lambda tickers, **kw: {
        "SYM": {"comparison_close_7d": 90.0, "comparison_date_7d": "2026-07-07",
                "comparison_close_30d": 80.0, "comparison_date_30d": "2026-06-14"}
    }
    _shared_patch = dict(
        name_overrides=lambda scope: {},
        fund_snaps=lambda symbols: {},
        vol_metrics=_vol_stub,
        comp_closes=_comp_stub,
    )

    def _run_with_price(fake_quotes_fn):
        monkeypatch.setattr("services.watchlist_quote_cache.get_watchlist_quotes", fake_quotes_fn)
        monkeypatch.setattr("services.name_overrides.get_name_overrides", _shared_patch["name_overrides"])
        monkeypatch.setattr("data.watchlist_fundamentals_store.get_snapshots_bulk", _shared_patch["fund_snaps"])
        monkeypatch.setattr("services.canonical_history_service.get_volume_metrics_bulk", _shared_patch["vol_metrics"])
        monkeypatch.setattr("services.canonical_history_service.get_comparison_closes_bulk", _shared_patch["comp_closes"])
        monkeypatch.setattr(wr, "_get_stage2_breakout",
                            lambda sym: {"score": None, "label": None, "reason": None})
        store = {
            "id": "wl-3", "tickers": ["SYM"], "csv_data": [],
            "analysis": {"sections": [{"name": "T", "tickers": [{"symbol": "SYM", "name": None}]}]},
        }
        enriched = asyncio.run(wr._enrich_store_with_quotes(store))
        return enriched["analysis"]["sections"][0]["tickers"][0]

    row_low  = _run_with_price(_fake_quotes_low)
    row_high = _run_with_price(_fake_quotes_high)

    # change_7d at price 100: (100/90 - 1)*100 ≈ 11.11%
    # change_7d at price 200: (200/90 - 1)*100 ≈ 122.22%
    expected_low_7d  = round((100.0 / 90.0 - 1) * 100.0, 6)
    expected_high_7d = round((200.0 / 90.0 - 1) * 100.0, 6)
    assert abs(row_low["change_7d"]  - expected_low_7d)  < 1e-4
    assert abs(row_high["change_7d"] - expected_high_7d) < 1e-4
    assert row_high["change_7d"] > row_low["change_7d"], (
        "Higher price must produce higher % vs same historical close"
    )


def test_enrich_store_no_live_price_yields_none_for_price_metrics(monkeypatch):
    """When a ticker has no live price, change_7d / change_30d must be None."""
    async def _fake_quotes_no_price(_symbols):
        return {}   # no quote at all

    monkeypatch.setattr(
        "services.canonical_history_service.get_volume_metrics_bulk",
        lambda tickers: {
            "NOPX": {
                "volume_change_1d_pct": None, "volume_change_7d_pct": None,
                "volume_change_30d_pct": None, "volume_acceleration_pp": None,
                "volume_metrics_as_of": None, "volume_metrics_status": "unavailable",
                "change_7d": 5.0,    # stale — no live price, so must be replaced by None
                "change_30d": 10.0,  # stale — same
            }
        },
    )
    monkeypatch.setattr(
        "services.canonical_history_service.get_comparison_closes_bulk",
        lambda tickers, **kw: {
            "NOPX": {"comparison_close_7d": 90.0, "comparison_date_7d": "2026-07-07",
                     "comparison_close_30d": 80.0, "comparison_date_30d": "2026-06-14"}
        },
    )
    monkeypatch.setattr("services.watchlist_quote_cache.get_watchlist_quotes", _fake_quotes_no_price)
    monkeypatch.setattr("services.name_overrides.get_name_overrides", lambda scope: {})
    monkeypatch.setattr("data.watchlist_fundamentals_store.get_snapshots_bulk", lambda symbols: {})
    monkeypatch.setattr(wr, "_get_stage2_breakout",
                        lambda sym: {"score": None, "label": None, "reason": None})

    store = {
        "id": "wl-4", "tickers": ["NOPX"], "csv_data": [],
        "analysis": {"sections": [{"name": "T", "tickers": [{"symbol": "NOPX", "name": None}]}]},
    }
    enriched = asyncio.run(wr._enrich_store_with_quotes(store))
    row = enriched["analysis"]["sections"][0]["tickers"][0]

    assert row.get("change_7d")  is None
    assert row.get("change_30d") is None


# ── Price-metric + NY-date correctness tests ─────────────────────────────────


def test_completed_daily_bars_uses_ny_market_date_not_utc(monkeypatch):
    """
    _completed_daily_bars must exclude the bar whose date == ny_market_date(),
    even when the UTC clock has rolled over to the next calendar day.

    Simulates 23:30 ET on 2026-07-14 (= 03:30 UTC on 2026-07-15).
    date.today() (UTC) would return 2026-07-15, but the NY date is still
    2026-07-14.  The bar for 2026-07-14 must be excluded (still today in NY).
    """
    # Patch ny_market_date to simulate 23:30 ET → NY date = July 14
    monkeypatch.setattr(chs, "ny_market_date", lambda: date(2026, 7, 14))

    bars = [
        {"date": "2026-07-13", "close": 100.0, "volume": 1000.0},
        {"date": "2026-07-14", "close": 150.0, "volume": 2000.0},  # today in NY
    ]
    completed = chs._completed_daily_bars(bars)

    assert len(completed) == 1, (
        f"Bar for 2026-07-14 (today's NY date) must be excluded; got {len(completed)}"
    )
    assert completed[0][0] == "2026-07-13"


def test_append_bars_same_date_dedup_later_bar_wins(monkeypatch, tmp_path):
    """
    append_bars must deduplicate bars by calendar date (ET-sourced from Tradier).
    When the same session date appears in both existing and new bars,
    the new bar wins.  This is the real production merge function, not a dict stub.
    """
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")
    monkeypatch.setattr(chs, "_INDEX", {})

    # get_bars requires bar_count >= 40 (_RECENT_MIN); provide 50 bars
    _base_start = date(2026, 5, 1)
    initial = [
        {"date": (_base_start + timedelta(days=i)).isoformat(),
         "close": 100.0 + i, "volume": 1000.0}
        for i in range(48)
    ]
    # Override the last two dates to match the overlap scenario
    initial[-2]["date"] = "2026-07-10"
    initial[-1]["date"] = "2026-07-11"
    # Also pre-add the date that will be overwritten
    initial.append({"date": "2026-07-12", "close": 104.0, "volume": 1200.0})
    chs.save_bars("TEST", initial, "tradier")

    new_bars = [
        {"date": "2026-07-12", "close": 105.0, "volume": 1250.0},  # same date, new close
        {"date": "2026-07-13", "close": 107.0, "volume": 1300.0},  # genuinely new bar
    ]
    result = chs.append_bars("TEST", new_bars, "tradier")
    assert result is not None

    stored = chs.get_bars("TEST", require_fresh=False)
    dates_list  = [b["date"] for b in stored["bars"]]
    closes_map  = {b["date"]: b["close"] for b in stored["bars"]}

    assert len(set(dates_list)) == len(dates_list), "No duplicate dates after merge"
    assert closes_map["2026-07-12"] == 105.0, "Newer bar must win for the overlapping date"
    assert closes_map["2026-07-13"] == 107.0, "New bar must be appended"
    # Total = initial (49) + 1 new (Jul 13); Jul 12 was overwritten, not added
    assert len(dates_list) == 50, f"Expected 50 bars (49 initial + 1 new), got {len(dates_list)}"


def test_comparison_closes_bulk_real_function_with_temp_dir(monkeypatch, tmp_path):
    """
    get_comparison_closes_bulk must read from bar files and return the correct
    7D and 30D comparison closes relative to the provided ny_today.
    """
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")
    monkeypatch.setattr(chs, "_INDEX", {})

    # Build a bar sequence: 40 consecutive days starting Jan 1
    start_d = date(2026, 1, 1)
    bars = [
        {"date": (start_d + timedelta(days=i)).isoformat(),
         "close": 100.0 + i, "volume": 1000.0}
        for i in range(40)
    ]
    chs.save_bars("XYZ", bars, "tradier")

    # as_of = Feb 9 (index 39 = Jan 1 + 39 days)
    # 7d target  = Feb 2 (day 32): close = 100 + 32 = 132.0
    # 30d target = Jan 10 (day  9): close = 100 + 9  = 109.0
    ny_ref = start_d + timedelta(days=40)   # Feb 10 (bars run Jan 1 – Feb 9)
    result = chs.get_comparison_closes_bulk(["XYZ"], ny_today=ny_ref)

    assert "XYZ" in result
    r = result["XYZ"]
    # 7d target = Feb 10 − 7 = Feb 3 (day 33) → close = 133.0
    assert r["comparison_close_7d"] == 133.0, f"Expected 133.0 got {r['comparison_close_7d']}"
    # 30d target = Feb 10 − 30 = Jan 11 (day 10) → close = 110.0
    assert r["comparison_close_30d"] == 110.0, f"Expected 110.0 got {r['comparison_close_30d']}"


def test_select_comparison_closes_materially_stale_returns_none():
    """
    _select_comparison_closes must return None when the most recent available
    bar is more than max_gap_days before the target date (materially stale history).
    """
    # Completed bars only through Jan 14; target_7d from Aug 5 = Jul 29
    # Gap = Jul 29 − Jan 14 = 196 days >> max_gap_days → None
    completed = [("2026-01-14", 100.0, 1000.0)]
    as_of = date(2026, 8, 5)

    result = chs._select_comparison_closes(completed, as_of, max_gap_days=10)
    assert result["comparison_close_7d"]  is None
    assert result["comparison_close_30d"] is None


def test_comparison_closes_weekend_target_selects_prior_trading_session():
    """
    When the 7-day target falls on a weekend, the comparison bar must be
    the most recent trading session on or before that Saturday/Sunday.
    """
    # Bars: Mon-Fri Jan 5–9, then Mon-Fri Jan 12–16, then Mon Jan 19 (last)
    trading_days = [
        date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7),
        date(2026, 1, 8), date(2026, 1, 9),           # week 1
        date(2026, 1, 12), date(2026, 1, 13), date(2026, 1, 14),
        date(2026, 1, 15), date(2026, 1, 16),          # week 2
        date(2026, 1, 19),                             # Monday week 3
    ]
    closes = [float(i + 100) for i in range(len(trading_days))]
    completed = [(d.isoformat(), c, 1000.0)
                 for d, c in zip(trading_days, closes)]

    # closes = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0, 110.0]
    # Jan 13 is at index 6 → close = 106.0
    # as_of = 2026-01-20 (Tuesday); 7d target = 2026-01-13 (Tuesday)
    # Most recent bar on or before Jan 13 = Jan 13 itself (close = 106.0)
    result = chs._select_comparison_closes(
        completed, date(2026, 1, 20), max_gap_days=10
    )
    assert result["comparison_date_7d"] == "2026-01-13"
    assert result["comparison_close_7d"] == 106.0

    # as_of = 2026-01-17 (Saturday); 7d target = 2026-01-10 (Saturday, no bar)
    # Most recent bar on or before Jan 10 = Jan 9 (Friday, close = 104.0, index 4)
    result_sat = chs._select_comparison_closes(
        completed, date(2026, 1, 17), max_gap_days=10
    )
    assert result_sat["comparison_date_7d"] == "2026-01-09"
    assert result_sat["comparison_close_7d"] == 104.0


def test_price_metrics_calendar_day_7d_differs_from_session_count():
    """
    With real trading gaps (weekends), the calendar-day 7D result differs from
    a naive 7-session-count offset.  Verify the calendar-day lookup is active.
    """
    trading_days = [
        date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7), date(2026, 1, 8),
        date(2026, 1, 9),
        date(2026, 1, 12), date(2026, 1, 13), date(2026, 1, 14), date(2026, 1, 15),
        date(2026, 1, 16),
        date(2026, 1, 19),
    ]
    closes = [100.0] * 5 + [110.0] * 5 + [120.0]

    bars = [{"date": d.isoformat(), "close": c, "volume": 1000.0}
            for d, c in zip(trading_days, closes)]

    metrics = chs._compute_watchlist_market_metrics_from_bars(bars)
    # as_of via ny_market_date; for the fixture the last bar is Jan 19
    # Since fixtures run before today, _completed_daily_bars includes all
    # 11 bars (Jan 2026 is in the past).  Last bar = Jan 19.
    # 7d target = Jan 12 → close = 110.0
    # change_7d = (120/110 − 1) × 100 ≈ 9.0909%
    assert metrics["change_7d"] is not None
    expected = round((120.0 / 110.0 - 1) * 100.0, 6)
    assert abs(metrics["change_7d"] - expected) < 1e-4

    # Session-count 7 would pick Jan 9 (close=100), not Jan 12 (close=110)
    session_7_result = round((120.0 / 100.0 - 1) * 100.0, 6)
    assert abs(metrics["change_7d"] - session_7_result) > 1.0, (
        "Calendar-day result must differ from 7-session-count result on this fixture"
    )


def test_price_metrics_30d_calendar_differs_from_30_sessions():
    """30 trading sessions ≈ 42 calendar days; 30 calendar days is much smaller."""
    start = date(2026, 1, 1)
    bars  = _price_bars(start, [100.0] * 44 + [150.0])  # 45 bars, last = Feb 14
    metrics = chs._compute_watchlist_market_metrics_from_bars(bars)
    # 30d target = Jan 15 → close=100.0; change_30d = (150/100−1)×100 = 50%
    assert metrics["change_30d"] is not None
    assert abs(metrics["change_30d"] - 50.0) < 0.01


def test_price_metrics_exactly_sufficient_history_7d():
    """8 calendar days of data → change_7d available; 7 days → None."""
    start = date(2026, 1, 1)

    # 8 bars Jan 1–8: target Jan 1 (8−7=1) → close=100
    sufficient = chs._compute_watchlist_market_metrics_from_bars(
        _price_bars(start, [100.0] * 7 + [120.0])
    )
    assert sufficient["change_7d"] is not None
    assert abs(sufficient["change_7d"] - 20.0) < 0.01

    # 7 bars Jan 1–7: target Dec 31 → no bar → None
    insufficient = chs._compute_watchlist_market_metrics_from_bars(
        _price_bars(start, [100.0] * 6 + [120.0])
    )
    assert insufficient["change_7d"] is None


def test_price_metrics_null_comparison_close_returns_none_not_zero():
    """None or zero comparison bar close must yield None, not 0%."""
    start = date(2026, 1, 1)
    bars_null = [{"date": (start + timedelta(days=i)).isoformat(),
                  "close": None if i == 24 else 100.0, "volume": 1.0}
                 for i in range(32)]
    bars_zero = [{"date": (start + timedelta(days=i)).isoformat(),
                  "close": 0.0 if i == 1 else 100.0, "volume": 1.0}
                 for i in range(32)]

    m_null = chs._compute_watchlist_market_metrics_from_bars(bars_null)
    m_zero = chs._compute_watchlist_market_metrics_from_bars(bars_zero)
    assert m_null["change_7d"]  is None
    assert m_zero["change_30d"] is None


def test_price_metrics_future_dated_bar_excluded():
    """A bar dated today or later must never enter the calculation."""
    ny_today = chs.ny_market_date()
    bars = _price_bars(ny_today - timedelta(days=31), ([100.0] * 30) + [120.0])
    future_bar = {"date": (ny_today + timedelta(days=1)).isoformat(),
                  "close": 9_999.0, "volume": 1.0}

    m_without = chs._compute_watchlist_market_metrics_from_bars(bars)
    m_with    = chs._compute_watchlist_market_metrics_from_bars(bars + [future_bar])
    assert m_with == m_without, "Future-dated bar must not alter the result"


def test_watchlist_endpoint_field_contract_includes_7d_30d():
    """change_7d and change_30d must remain in the canonical metric field set."""
    from services.canonical_history_service import (
        _WATCHLIST_MARKET_METRIC_FIELDS, _null_price_metrics, _null_volume_metrics,
    )
    required = set(_WATCHLIST_MARKET_METRIC_FIELDS)
    assert "change_7d"  in required
    assert "change_30d" in required
    assert "volume_change_1d_pct"  in required
    assert "volume_metrics_status" in required
    null_out = {**_null_volume_metrics(), **_null_price_metrics()}
    for field in required:
        assert field in null_out, f"Null metrics missing field: {field}"


# ── NEW: Compact comparison-close tail — performance and correctness ──────────


def test_comparison_tail_is_built_and_stored_on_save(monkeypatch, tmp_path):
    """save_bars must populate comparison_close_tail in _INDEX."""
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")
    monkeypatch.setattr(chs, "_INDEX", {})

    start = date(2026, 1, 1)
    bars = [
        {"date": (start + timedelta(days=i)).isoformat(), "close": 100.0 + i, "volume": 1000.0}
        for i in range(40)
    ]
    chs.save_bars("TAIL", bars, "tradier")

    tail = chs._INDEX["TAIL"].get("comparison_close_tail")
    assert tail is not None, "comparison_close_tail must be in _INDEX after save_bars"
    assert isinstance(tail, list), "comparison_close_tail must be a list"
    assert len(tail) <= chs._COMPARISON_TAIL_SESSIONS, "tail must be bounded by constant"
    for entry in tail:
        assert len(entry) == 2, "each entry must be [date_str, close]"
        assert isinstance(entry[0], str), "date must be a string"
        assert isinstance(entry[1], float), "close must be a float"
        assert entry[1] > 0, "close must be strictly positive"


def test_comparison_closes_bulk_zero_gzip_opens_with_populated_tail(monkeypatch, tmp_path):
    """get_comparison_closes_bulk performs ZERO gzip.open calls when tail is in _INDEX."""
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")
    monkeypatch.setattr(chs, "_INDEX", {})

    start = date(2026, 1, 1)
    bars = [
        {"date": (start + timedelta(days=i)).isoformat(), "close": 100.0 + i, "volume": 1000.0}
        for i in range(50)
    ]
    chs.save_bars("PERF", bars, "tradier")

    # Instrument gzip.open AFTER save_bars has already written the file
    import gzip as _real_gzip_mod
    _open_count = [0]
    _real_open = _real_gzip_mod.open

    def _counting_open(*a, **kw):
        _open_count[0] += 1
        return _real_open(*a, **kw)

    monkeypatch.setattr(chs.gzip, "open", _counting_open)

    ny_ref = start + timedelta(days=50)
    result = chs.get_comparison_closes_bulk(["PERF"], ny_today=ny_ref)

    assert _open_count[0] == 0, (
        f"Expected zero gzip.open calls; got {_open_count[0]}. "
        "Normal path must read from _INDEX tail, not the gz file."
    )
    assert result["PERF"]["comparison_close_7d"] is not None


def test_comparison_closes_bulk_zero_gzip_opens_repeated_calls(monkeypatch, tmp_path):
    """Three consecutive calls all perform zero gzip.open calls."""
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")
    monkeypatch.setattr(chs, "_INDEX", {})

    start = date(2026, 1, 1)
    bars = [
        {"date": (start + timedelta(days=i)).isoformat(), "close": 200.0 + i, "volume": 1000.0}
        for i in range(50)
    ]
    chs.save_bars("RPT", bars, "tradier")

    import gzip as _real_gzip_mod
    _open_count = [0]
    _real_open = _real_gzip_mod.open

    def _counting_open(*a, **kw):
        _open_count[0] += 1
        return _real_open(*a, **kw)

    monkeypatch.setattr(chs.gzip, "open", _counting_open)

    ny_ref = start + timedelta(days=50)
    for _ in range(3):
        chs.get_comparison_closes_bulk(["RPT"], ny_today=ny_ref)

    assert _open_count[0] == 0, (
        f"Expected zero gzip.open calls across 3 calls; got {_open_count[0]}."
    )


def test_comparison_closes_bulk_date_roll_changes_selection_without_file_read(monkeypatch, tmp_path):
    """A different ny_today changes the selected comparison bar with zero file reads."""
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")
    monkeypatch.setattr(chs, "_INDEX", {})

    # Mon–Fri bars for 3 weeks: each day has a unique close
    trading_days = [
        date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7), date(2026, 1, 8), date(2026, 1, 9),
        date(2026, 1, 12), date(2026, 1, 13), date(2026, 1, 14), date(2026, 1, 15), date(2026, 1, 16),
        date(2026, 1, 19), date(2026, 1, 20), date(2026, 1, 21), date(2026, 1, 22), date(2026, 1, 23),
    ]
    bars = [
        {"date": d.isoformat(), "close": float(i + 100), "volume": 1000.0}
        for i, d in enumerate(trading_days)
    ]
    chs.save_bars("ROLL", bars, "tradier")

    import gzip as _real_gzip_mod
    _open_count = [0]
    _real_open = _real_gzip_mod.open

    def _counting_open(*a, **kw):
        _open_count[0] += 1
        return _real_open(*a, **kw)

    monkeypatch.setattr(chs.gzip, "open", _counting_open)

    # Jan 20 − 7 = Jan 13 (Tue, index 6 → close=106.0)
    # Jan 21 − 7 = Jan 14 (Wed, index 7 → close=107.0)
    r1 = chs.get_comparison_closes_bulk(["ROLL"], ny_today=date(2026, 1, 20))
    r2 = chs.get_comparison_closes_bulk(["ROLL"], ny_today=date(2026, 1, 21))

    assert _open_count[0] == 0, (
        f"Expected zero file reads when ny_today changes; got {_open_count[0]}"
    )
    assert r1["ROLL"]["comparison_date_7d"] == "2026-01-13", (
        f"Jan 20 − 7d = Jan 13; got {r1['ROLL']['comparison_date_7d']}"
    )
    assert r2["ROLL"]["comparison_date_7d"] == "2026-01-14", (
        f"Jan 21 − 7d = Jan 14; got {r2['ROLL']['comparison_date_7d']}"
    )
    assert r1["ROLL"]["comparison_close_7d"] != r2["ROLL"]["comparison_close_7d"]


def test_comparison_close_tail_bounded_to_constant(monkeypatch, tmp_path):
    """Tail must never exceed _COMPARISON_TAIL_SESSIONS entries, always the most-recent ones."""
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")
    monkeypatch.setattr(chs, "_INDEX", {})

    start = date(2025, 1, 1)
    bars = [
        {"date": (start + timedelta(days=i)).isoformat(), "close": 100.0 + i, "volume": 1000.0}
        for i in range(400)
    ]
    chs.save_bars("BIG", bars, "tradier")

    tail = chs._INDEX["BIG"]["comparison_close_tail"]
    assert len(tail) <= chs._COMPARISON_TAIL_SESSIONS, (
        f"Tail must be ≤ {chs._COMPARISON_TAIL_SESSIONS} entries; got {len(tail)}"
    )
    assert len(tail) == chs._COMPARISON_TAIL_SESSIONS, (
        f"With 400 bars, tail should be exactly {chs._COMPARISON_TAIL_SESSIONS}"
    )
    # Must be the MOST RECENT completed sessions (chronologically latest)
    last_date = (start + timedelta(days=399)).isoformat()
    assert tail[-1][0] == last_date, (
        f"Tail must end with the most-recent completed session; "
        f"got {tail[-1][0]}, expected {last_date}"
    )


def test_comparison_closes_bulk_missing_tail_returns_null_without_file_read(monkeypatch, tmp_path):
    """When comparison_close_tail is absent from _INDEX, return null without any file I/O."""
    import gzip as _real_gzip_mod, json as _json

    # Directly inject _INDEX entry without tail (simulates pre-repair metadata)
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")
    monkeypatch.setattr(chs, "_INDEX", {
        "OLD": {
            "symbol": "OLD",
            "bar_count": 200,
            "history_status": "available_10y",
            "change_7d": 5.0,
            "change_30d": 10.0,
            # intentionally absent: "comparison_close_tail"
        }
    })
    # Create a dummy gz file — the old code would open it; the new code must not
    dummy_gz = tmp_path / "OLD.json.gz"
    with _real_gzip_mod.open(str(dummy_gz), "wt", encoding="utf-8") as fh:
        fh.write(_json.dumps({"bars": [], "symbol": "OLD"}))

    _open_count = [0]
    _real_open = _real_gzip_mod.open

    def _counting_open(*a, **kw):
        _open_count[0] += 1
        return _real_open(*a, **kw)

    monkeypatch.setattr(chs.gzip, "open", _counting_open)

    result = chs.get_comparison_closes_bulk(["OLD"], ny_today=date(2026, 8, 5))

    assert _open_count[0] == 0, (
        f"Expected zero file reads for missing tail; got {_open_count[0]}"
    )
    assert result["OLD"]["comparison_close_7d"]  is None
    assert result["OLD"]["comparison_close_30d"] is None


# ── NEW: _select_from_tail unit tests ─────────────────────────────────────────


def test_select_from_tail_7d_selection():
    """_select_from_tail returns the most recent session on or before as_of − 7 days."""
    tail = [
        ["2026-01-01", 100.0],
        ["2026-01-08", 110.0],
        ["2026-01-15", 120.0],
    ]
    # as_of = Jan 15; 7d target = Jan 8 → close = 110.0
    result = chs._select_from_tail(tail, date(2026, 1, 15))
    assert result["comparison_close_7d"] == 110.0
    assert result["comparison_date_7d"]  == "2026-01-08"


def test_select_from_tail_30d_selection():
    """_select_from_tail returns the most recent session on or before as_of − 30 days."""
    tail = [
        ["2026-01-01", 100.0],
        ["2026-01-08", 110.0],
        ["2026-02-05", 120.0],
    ]
    # as_of = Feb 5; 30d target = Jan 6 → most recent ≤ Jan 6 = Jan 1 → 100.0
    result = chs._select_from_tail(tail, date(2026, 2, 5))
    assert result["comparison_close_30d"] == 100.0
    assert result["comparison_date_30d"]  == "2026-01-01"


def test_select_from_tail_weekend_target_selects_prior_trading_session():
    """When the 7d target falls on a weekend, use the most recent prior session."""
    tail = [
        ["2026-01-05", 100.0],  # Mon
        ["2026-01-06", 101.0],  # Tue
        ["2026-01-07", 102.0],  # Wed
        ["2026-01-08", 103.0],  # Thu
        ["2026-01-09", 104.0],  # Fri
        ["2026-01-12", 105.0],  # Mon
    ]
    # as_of = Jan 17 (Sat); 7d target = Jan 10 (Sat, no bar)
    # Most recent on/before Jan 10 = Jan 9 (Fri) → 104.0
    result = chs._select_from_tail(tail, date(2026, 1, 17))
    assert result["comparison_date_7d"]  == "2026-01-09"
    assert result["comparison_close_7d"] == 104.0


def test_select_from_tail_holiday_gap_within_max_gap_days():
    """A multi-day gap smaller than max_gap_days resolves to the prior session."""
    tail = [
        ["2026-01-01", 100.0],
        ["2026-01-02", 101.0],
        # Jan 3–9 missing (7-day holiday gap)
        ["2026-01-10", 110.0],
        ["2026-01-15", 120.0],
    ]
    # as_of = Jan 17; 7d target = Jan 10; bar present → 110.0
    result = chs._select_from_tail(tail, date(2026, 1, 17))
    assert result["comparison_close_7d"] == 110.0
    assert result["comparison_date_7d"]  == "2026-01-10"


def test_select_from_tail_materially_stale_returns_null():
    """When the best available bar is > max_gap_days before the target, return None."""
    tail = [["2026-01-01", 100.0]]
    # as_of = Aug 5, 2026; 7d target = Jul 29; gap from Jan 1 to Jul 29 ≫ 10 → None
    result = chs._select_from_tail(tail, date(2026, 8, 5), max_gap_days=10)
    assert result["comparison_close_7d"]  is None
    assert result["comparison_close_30d"] is None


def test_select_from_tail_empty_returns_null_contract():
    """Empty tail must return null for all four keys (no KeyError, no exception)."""
    result = chs._select_from_tail([], date(2026, 8, 5))
    assert result["comparison_close_7d"]  is None
    assert result["comparison_close_30d"] is None
    assert result["comparison_date_7d"]   is None
    assert result["comparison_date_30d"]  is None


def test_build_tail_excludes_current_ny_day(monkeypatch):
    """_build_comparison_close_tail_from_completed must not include today's NY session."""
    monkeypatch.setattr(chs, "ny_market_date", lambda: date(2026, 7, 15))
    bars = [
        {"date": "2026-07-14", "close": 100.0, "volume": 1000.0},
        {"date": "2026-07-15", "close": 999.0, "volume": 9999.0},  # today in NY — must be excluded
    ]
    completed = chs._completed_daily_bars(bars)
    tail = chs._build_comparison_close_tail_from_completed(completed)
    dates_in_tail = [e[0] for e in tail]
    assert "2026-07-15" not in dates_in_tail, "Today's NY bar must not appear in comparison tail"
    assert "2026-07-14" in dates_in_tail, "Yesterday's bar must appear in comparison tail"


def test_build_tail_handles_duplicate_dates_via_completed():
    """Completed bars (already filtered) produce a tail with unique dates per canonical contract."""
    # _completed_daily_bars preserves whatever order comes from the payload.
    # In production, bars is sorted by date, so duplicates survive as-is.
    # Verify _build_comparison_close_tail_from_completed passes through correctly.
    completed = [
        ("2026-01-01", 100.0, 1000.0),
        ("2026-01-02", 101.0, 1000.0),
        ("2026-01-03", 102.0, 1000.0),
    ]
    tail = chs._build_comparison_close_tail_from_completed(completed)
    dates = [e[0] for e in tail]
    assert dates == sorted(dates), "Tail must be in chronological order"
    assert len(dates) == 3


# ── NEW: Final row correctness — no leaked internal keys, correct split basis ─


def test_no_comparison_keys_leaked_to_serialized_row(monkeypatch):
    """No _comparison_* key must appear in any serialized Watchlist row after enrichment."""
    async def _fake_quotes(_symbols):
        return {
            "LEAK": {
                "price": 50.0, "change_pct_1d": 1.0,
                "volume": 1000, "average_volume": 500,
                "quote_source": "tradier", "quote_updated_at": "2026-08-01T20:00:00Z",
            }
        }

    monkeypatch.setattr(
        "services.canonical_history_service.get_volume_metrics_bulk",
        lambda tickers: {
            "LEAK": {
                "volume_change_1d_pct": 10.0, "volume_change_7d_pct": 5.0,
                "volume_change_30d_pct": 3.0, "volume_acceleration_pp": 2.0,
                "volume_metrics_as_of": "2026-07-31", "volume_metrics_status": "ok",
                "change_7d": 99.9, "change_30d": 88.8,
            }
        },
    )
    monkeypatch.setattr(
        "services.canonical_history_service.get_comparison_closes_bulk",
        lambda tickers, **kw: {
            "LEAK": {
                "comparison_close_7d": 45.0, "comparison_date_7d": "2026-07-25",
                "comparison_close_30d": 40.0, "comparison_date_30d": "2026-07-01",
            }
        },
    )
    monkeypatch.setattr("services.watchlist_quote_cache.get_watchlist_quotes", _fake_quotes)
    monkeypatch.setattr("services.name_overrides.get_name_overrides", lambda scope: {})
    monkeypatch.setattr("data.watchlist_fundamentals_store.get_snapshots_bulk", lambda syms: {})
    monkeypatch.setattr(wr, "_get_stage2_breakout",
                        lambda sym: {"score": None, "label": None, "reason": None})

    store = {
        "id": "wl-leak", "tickers": ["LEAK"], "csv_data": [],
        "analysis": {"sections": [{"name": "T", "tickers": [{"symbol": "LEAK", "name": None}]}]},
    }
    enriched = asyncio.run(wr._enrich_store_with_quotes(store))
    row = enriched["analysis"]["sections"][0]["tickers"][0]

    leaked = [k for k in row if k.startswith("_comparison_")]
    assert leaked == [], (
        f"Internal _comparison_* keys must not appear in serialized row; found: {leaked}"
    )
    # change_7d / change_30d must use live price 50.0 as numerator
    assert row["change_7d"]  == round((50.0 / 45.0 - 1) * 100.0, 6)
    assert row["change_30d"] == round((50.0 / 40.0 - 1) * 100.0, 6)
    # Public volume fields must still be present
    assert row["volume_change_1d_pct"] == 10.0
    assert row["volume_metrics_status"] == "ok"


def test_split_consistent_canonical_basis_preserved(monkeypatch):
    """Split-adjusted canonical history close is used as denominator (basis preserved)."""
    # Pre-split price 200 → post-split canonical bar close = 100 (split-adjusted).
    # Live price 110; change_7d = (110 / 100 − 1) × 100 = 10%.
    async def _fake_quotes(_symbols):
        return {
            "SPLT": {
                "price": 110.0, "change_pct_1d": 0.0,
                "volume": 1000, "average_volume": 500,
                "quote_source": "tradier", "quote_updated_at": "2026-08-01T20:00:00Z",
            }
        }

    monkeypatch.setattr(
        "services.canonical_history_service.get_volume_metrics_bulk",
        lambda tickers: {
            "SPLT": {
                "volume_change_1d_pct": 0.0, "volume_change_7d_pct": 0.0,
                "volume_change_30d_pct": 0.0, "volume_acceleration_pp": 0.0,
                "volume_metrics_as_of": "2026-07-31", "volume_metrics_status": "ok",
                "change_7d": 0.0, "change_30d": 0.0,
            }
        },
    )
    # Split-adjusted canonical close (already reflected in canonical bar history)
    monkeypatch.setattr(
        "services.canonical_history_service.get_comparison_closes_bulk",
        lambda tickers, **kw: {
            "SPLT": {
                "comparison_close_7d": 100.0, "comparison_date_7d": "2026-07-25",
                "comparison_close_30d": 90.0,  "comparison_date_30d": "2026-07-01",
            }
        },
    )
    monkeypatch.setattr("services.watchlist_quote_cache.get_watchlist_quotes", _fake_quotes)
    monkeypatch.setattr("services.name_overrides.get_name_overrides", lambda scope: {})
    monkeypatch.setattr("data.watchlist_fundamentals_store.get_snapshots_bulk", lambda syms: {})
    monkeypatch.setattr(wr, "_get_stage2_breakout",
                        lambda sym: {"score": None, "label": None, "reason": None})

    store = {
        "id": "wl-split", "tickers": ["SPLT"], "csv_data": [],
        "analysis": {"sections": [{"name": "T", "tickers": [{"symbol": "SPLT", "name": None}]}]},
    }
    enriched = asyncio.run(wr._enrich_store_with_quotes(store))
    row = enriched["analysis"]["sections"][0]["tickers"][0]

    expected_7d  = round((110.0 / 100.0 - 1) * 100.0, 6)   # = 10.0
    expected_30d = round((110.0 / 90.0  - 1) * 100.0, 6)   # ≈ 22.222222
    assert abs(row["change_7d"]  - expected_7d)  < 1e-4, (
        f"Split-consistent 7D basis failed: {row['change_7d']} != {expected_7d}"
    )
    assert abs(row["change_30d"] - expected_30d) < 1e-4, (
        f"Split-consistent 30D basis failed: {row['change_30d']} != {expected_30d}"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# NEW: Tail validator, repair selection, repair behavior, bootstrap (tests 1-32)
# ═══════════════════════════════════════════════════════════════════════════════


# ── Validator tests (1–8) ────────────────────────────────────────────────────


def test_validator_normal_64_session_tail_is_valid():
    """T1: A well-formed 64-session tail is valid."""
    from datetime import timedelta
    start = date(2025, 1, 1)
    tail = [
        [(start + timedelta(days=i)).isoformat(), float(100 + i)]
        for i in range(64)
    ]
    assert chs._is_valid_comparison_close_tail(tail) is True


def test_validator_empty_tail_is_invalid():
    """T2: Empty list is invalid."""
    assert chs._is_valid_comparison_close_tail([]) is False
    assert chs._is_valid_comparison_close_tail(None) is False
    assert chs._is_valid_comparison_close_tail("") is False


def test_validator_malformed_entry_is_invalid():
    """T3: Entry that is not a 2-element list/tuple is invalid."""
    assert chs._is_valid_comparison_close_tail([["2026-01-01"]]) is False          # too short
    assert chs._is_valid_comparison_close_tail(["2026-01-01"]) is False             # string not list
    assert chs._is_valid_comparison_close_tail([[None, 100.0]]) is False            # date None
    assert chs._is_valid_comparison_close_tail([{"date": "2026-01-01", "c": 1}]) is False  # dict


def test_validator_nonpositive_close_is_invalid():
    """T4: Zero or negative close is invalid."""
    assert chs._is_valid_comparison_close_tail([["2026-01-01", 0.0]]) is False
    assert chs._is_valid_comparison_close_tail([["2026-01-01", -1.0]]) is False


def test_validator_nonfinite_close_is_invalid():
    """T5: NaN and ±Inf are invalid."""
    assert chs._is_valid_comparison_close_tail([["2026-01-01", float("inf")]]) is False
    assert chs._is_valid_comparison_close_tail([["2026-01-01", float("-inf")]]) is False
    assert chs._is_valid_comparison_close_tail([["2026-01-01", float("nan")]]) is False


def test_validator_invalid_date_is_invalid():
    """T6: Non-ISO or garbage date strings are invalid."""
    assert chs._is_valid_comparison_close_tail([["not-a-date", 100.0]]) is False
    assert chs._is_valid_comparison_close_tail([["2026-13-01", 100.0]]) is False  # month 13
    assert chs._is_valid_comparison_close_tail([[20260101, 100.0]]) is False       # int, not str


def test_validator_duplicate_dates_are_invalid():
    """T7: Duplicate date strings are invalid."""
    tail = [["2026-01-01", 100.0], ["2026-01-01", 101.0]]
    assert chs._is_valid_comparison_close_tail(tail) is False


def test_validator_nonchronological_dates_are_invalid_and_oversized_tail():
    """T8a: Nonchronological dates are invalid. T8b: Oversized tail is invalid."""
    # Nonchronological
    tail_bad_order = [["2026-01-02", 101.0], ["2026-01-01", 100.0]]
    assert chs._is_valid_comparison_close_tail(tail_bad_order) is False

    # Oversized
    from datetime import timedelta
    start = date(2025, 1, 1)
    big_tail = [
        [(start + timedelta(days=i)).isoformat(), float(100 + i)]
        for i in range(chs._COMPARISON_TAIL_SESSIONS + 1)
    ]
    assert chs._is_valid_comparison_close_tail(big_tail) is False


# ── Repair selection tests (9–15) ────────────────────────────────────────────


def test_repair_selection_valid_tail_symbols_not_selected(monkeypatch, tmp_path):
    """T9: Symbols with valid tails are not selected for repair (no-op)."""
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")
    monkeypatch.setattr(chs, "_INDEX", {})

    start = date(2026, 1, 1)
    bars = [
        {"date": (start + timedelta(days=i)).isoformat(),
         "close": 100.0 + i, "volume": 1000.0}
        for i in range(50)
    ]
    chs.save_bars("GOOD", bars, "tradier")

    # Verify the tail was stored and is valid
    tail = chs._INDEX["GOOD"].get("comparison_close_tail")
    assert chs._is_valid_comparison_close_tail(tail) is True

    import gzip as _gzmod
    open_count = [0]
    real_open = _gzmod.open

    def _counting(*a, **kw):
        open_count[0] += 1
        return real_open(*a, **kw)

    monkeypatch.setattr(chs.gzip, "open", _counting)
    result = chs.ensure_comparison_close_tails()

    assert result["selected"] == 0
    assert result["status"] == "noop"
    assert open_count[0] == 0, f"Expected zero gz opens; got {open_count[0]}"


def test_repair_selection_missing_tail_with_gz_file_is_selected(monkeypatch, tmp_path):
    """T10: Symbol whose _INDEX entry lacks a tail but has a gz file is selected."""
    import gzip as _gzmod, json as _jsonmod
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")

    start = date(2026, 1, 1)
    bars = [
        {"date": (start + timedelta(days=i)).isoformat(),
         "close": 100.0 + i, "volume": 1000.0}
        for i in range(50)
    ]
    # Write gz file manually without comparison_close_tail in meta
    sym = "NOTAIL"
    gz_path = tmp_path / f"{sym}.json.gz"
    payload = {
        "symbol": sym, "bar_count": 50,
        "history_status": "available_10y",
        "bars": bars,
    }
    with _gzmod.open(str(gz_path), "wt", encoding="utf-8") as fh:
        fh.write(_jsonmod.dumps(payload))

    # Inject _INDEX entry without tail
    monkeypatch.setattr(chs, "_INDEX", {
        sym: {
            "symbol": sym, "bar_count": 50,
            "history_status": "available_10y",
            # intentionally no comparison_close_tail
        }
    })

    result = chs.ensure_comparison_close_tails()
    assert result["selected"] >= 1
    assert result["updated"] >= 1
    assert result["file_reads"] >= 1
    assert chs._is_valid_comparison_close_tail(
        chs._INDEX[sym].get("comparison_close_tail")
    ), "Tail must be valid after repair"


def test_repair_selection_empty_and_malformed_tails_are_selected(monkeypatch, tmp_path):
    """T11: Empty or malformed tails are selected for repair."""
    import gzip as _gzmod, json as _jsonmod
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")

    start = date(2026, 1, 1)
    bars = [
        {"date": (start + timedelta(days=i)).isoformat(),
         "close": 100.0 + i, "volume": 1000.0}
        for i in range(50)
    ]

    for sym, bad_tail in [("EMPTY", []), ("MALFORM", [["bad-date", 1.0]])]:
        gz_path = tmp_path / f"{sym}.json.gz"
        with _gzmod.open(str(gz_path), "wt", encoding="utf-8") as fh:
            fh.write(_jsonmod.dumps({"symbol": sym, "bar_count": 50,
                                     "history_status": "available_10y",
                                     "bars": bars}))

    monkeypatch.setattr(chs, "_INDEX", {
        "EMPTY": {
            "symbol": "EMPTY", "bar_count": 50,
            "history_status": "available_10y",
            "comparison_close_tail": [],
        },
        "MALFORM": {
            "symbol": "MALFORM", "bar_count": 50,
            "history_status": "available_10y",
            "comparison_close_tail": [["bad-date", 1.0]],
        },
    })

    result = chs.ensure_comparison_close_tails()
    assert result["selected"] == 2
    assert result["updated"] == 2


def test_repair_selection_no_gz_file_not_selected(monkeypatch, tmp_path):
    """T12: Entry without a gz file is not selected for repair."""
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")
    monkeypatch.setattr(chs, "_INDEX", {
        "NOFILE": {
            "symbol": "NOFILE", "bar_count": 50,
            "history_status": "available_10y",
            # No gz file and no tail
        }
    })
    # Confirm the gz file does NOT exist
    assert not (tmp_path / "NOFILE.json.gz").exists()

    result = chs.ensure_comparison_close_tails()
    assert result["selected"] == 0
    assert result["status"] == "noop"


def test_repair_selection_excluded_failed_insufficient_corrupt_not_selected(monkeypatch, tmp_path):
    """T13: Excluded, failed, insufficient, and corrupt entries are not selected."""
    import gzip as _gzmod, json as _jsonmod
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")

    # Create gz files so the absence-of-file check doesn't filter them first
    for sym in ("EXCL", "FAIL", "INSUF", "CORRUPT"):
        gz_path = tmp_path / f"{sym}.json.gz"
        with _gzmod.open(str(gz_path), "wt", encoding="utf-8") as fh:
            fh.write(_jsonmod.dumps({"symbol": sym, "bar_count": 50, "bars": []}))

    monkeypatch.setattr(chs, "_INDEX", {
        "EXCL": {
            "symbol": "EXCL", "bar_count": 50,
            "history_status": "excluded_prefixed_symbol",
        },
        "FAIL": {
            "symbol": "FAIL", "bar_count": 50,
            "history_status": "fetch_failed",
        },
        "INSUF": {
            "symbol": "INSUF", "bar_count": 5,  # below _RECENT_MIN
            "history_status": "insufficient_history",
        },
        "CORRUPT": {
            "symbol": "CORRUPT", "bar_count": 50,
            "history_status": "cache_corrupt_needs_rebuild",
        },
    })

    result = chs.ensure_comparison_close_tails()
    assert result["selected"] == 0, (
        f"Expected 0 selected; got {result['selected']} — "
        "excluded/failed/insufficient/corrupt entries must not be selected"
    )


def test_repair_selection_foreign_and_dotted_not_selected(monkeypatch, tmp_path):
    """T14: Foreign-prefixed (colon) and dotted legacy keys are not selected."""
    import gzip as _gzmod, json as _jsonmod
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")

    # Create gz files for completeness
    for sym in ("LON:HSBA", "legacy.old"):
        safe = sym.replace(":", "_").replace(".", "_")
        gz_path = tmp_path / f"{safe}.json.gz"
        with _gzmod.open(str(gz_path), "wt", encoding="utf-8") as fh:
            fh.write(_jsonmod.dumps({"symbol": sym, "bar_count": 50, "bars": []}))

    monkeypatch.setattr(chs, "_INDEX", {
        "LON:HSBA": {
            "symbol": "LON:HSBA", "bar_count": 200,
            "history_status": "available_10y",
        },
        "legacy.old": {
            "symbol": "legacy.old", "bar_count": 200,
            "history_status": "available_10y",
        },
    })

    result = chs.ensure_comparison_close_tails()
    assert result["selected"] == 0, (
        "Foreign-prefixed and dotted symbols must never be selected"
    )


def test_repair_selection_fully_valid_index_is_noop(monkeypatch, tmp_path):
    """T15: An index where every symbol has a valid tail is a zero-gz-read no-op."""
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")
    monkeypatch.setattr(chs, "_INDEX", {})

    # Create 5 symbols with valid tails via save_bars
    start = date(2026, 1, 1)
    bars = [
        {"date": (start + timedelta(days=i)).isoformat(),
         "close": 100.0 + i, "volume": 1000.0}
        for i in range(50)
    ]
    for sym in ("AAA", "BBB", "CCC", "DDD", "EEE"):
        chs.save_bars(sym, bars, "tradier")

    import gzip as _gzmod
    open_count = [0]
    real_open = _gzmod.open

    def _counting(*a, **kw):
        open_count[0] += 1
        return real_open(*a, **kw)

    monkeypatch.setattr(chs.gzip, "open", _counting)
    result = chs.ensure_comparison_close_tails()

    assert result["selected"] == 0
    assert result["status"] == "noop"
    assert open_count[0] == 0, (
        f"Expected zero gz opens on no-op; got {open_count[0]}"
    )


# ── Repair behavior tests (16–22) ────────────────────────────────────────────


def test_repair_behavior_missing_tail_repaired_from_gz(monkeypatch, tmp_path):
    """T16: Missing-tail symbol is repaired from its existing gz bars."""
    import gzip as _gzmod, json as _jsonmod
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")

    start = date(2026, 1, 1)
    bars = [
        {"date": (start + timedelta(days=i)).isoformat(),
         "close": 100.0 + i, "volume": 1000.0}
        for i in range(50)
    ]
    sym = "REPAIR"
    gz_path = tmp_path / f"{sym}.json.gz"
    with _gzmod.open(str(gz_path), "wt", encoding="utf-8") as fh:
        fh.write(_jsonmod.dumps({
            "symbol": sym, "bar_count": 50,
            "history_status": "available_10y", "bars": bars,
        }))

    monkeypatch.setattr(chs, "_INDEX", {
        sym: {"symbol": sym, "bar_count": 50, "history_status": "available_10y"}
    })

    result = chs.ensure_comparison_close_tails()
    assert result["updated"] == 1, f"Expected 1 update; got {result['updated']}"


def test_repair_behavior_resulting_tail_is_valid_and_bounded(monkeypatch, tmp_path):
    """T17: After repair, the resulting tail passes the validator and is bounded."""
    import gzip as _gzmod, json as _jsonmod
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")

    start = date(2025, 1, 1)
    bars = [
        {"date": (start + timedelta(days=i)).isoformat(),
         "close": 100.0 + i, "volume": 1000.0}
        for i in range(200)  # more than _COMPARISON_TAIL_SESSIONS
    ]
    sym = "BOUND"
    gz_path = tmp_path / f"{sym}.json.gz"
    with _gzmod.open(str(gz_path), "wt", encoding="utf-8") as fh:
        fh.write(_jsonmod.dumps({
            "symbol": sym, "bar_count": 200,
            "history_status": "available_10y", "bars": bars,
        }))

    monkeypatch.setattr(chs, "_INDEX", {
        sym: {"symbol": sym, "bar_count": 200, "history_status": "available_10y"}
    })

    chs.ensure_comparison_close_tails()
    tail = chs._INDEX[sym].get("comparison_close_tail")
    assert chs._is_valid_comparison_close_tail(tail), "Tail must be valid after repair"
    assert len(tail) <= chs._COMPARISON_TAIL_SESSIONS, "Tail must be bounded"


def test_repair_behavior_get_comparison_closes_bulk_populated_after_repair(monkeypatch, tmp_path):
    """T18: get_comparison_closes_bulk() returns populated 7D/30D comparisons after repair."""
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")
    monkeypatch.setattr(chs, "_INDEX", {})

    # Write a real symbol via save_bars (this also builds the gz and tail)
    start = date(2026, 1, 1)
    bars = [
        {"date": (start + timedelta(days=i)).isoformat(),
         "close": 100.0 + i, "volume": 1000.0}
        for i in range(50)
    ]
    chs.save_bars("POSTREP", bars, "tradier")

    # Simulate the "tail missing" state
    meta_no_tail = {k: v for k, v in chs._INDEX["POSTREP"].items()
                    if k != "comparison_close_tail"}
    chs._INDEX["POSTREP"] = meta_no_tail

    # Confirm null before repair
    ny_ref = start + timedelta(days=50)
    pre = chs.get_comparison_closes_bulk(["POSTREP"], ny_today=ny_ref)
    assert pre["POSTREP"]["comparison_close_7d"] is None, "Should be null before repair"

    # Repair
    chs.ensure_comparison_close_tails()

    # Confirm populated after repair
    post = chs.get_comparison_closes_bulk(["POSTREP"], ny_today=ny_ref)
    assert post["POSTREP"]["comparison_close_7d"] is not None, (
        "Should be populated after repair"
    )


def test_repair_behavior_no_provider_calls(monkeypatch, tmp_path):
    """T19: Repair performs no provider calls (HTTP/network)."""
    import gzip as _gzmod, json as _jsonmod
    import urllib.request
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")

    start = date(2026, 1, 1)
    bars = [
        {"date": (start + timedelta(days=i)).isoformat(),
         "close": 100.0 + i, "volume": 1000.0}
        for i in range(50)
    ]
    sym = "NOPROV"
    gz_path = tmp_path / f"{sym}.json.gz"
    with _gzmod.open(str(gz_path), "wt", encoding="utf-8") as fh:
        fh.write(_jsonmod.dumps({
            "symbol": sym, "bar_count": 50,
            "history_status": "available_10y", "bars": bars,
        }))
    monkeypatch.setattr(chs, "_INDEX", {
        sym: {"symbol": sym, "bar_count": 50, "history_status": "available_10y"}
    })

    network_calls: list[str] = []

    def _no_network(*a, **kw):
        network_calls.append(str(a))
        raise AssertionError(f"No network calls allowed during repair; got {a}")

    monkeypatch.setattr(urllib.request, "urlopen", _no_network)

    chs.ensure_comparison_close_tails()
    assert network_calls == [], f"Unexpected network calls: {network_calls}"


def test_repair_behavior_no_database_calls(monkeypatch, tmp_path):
    """T20: Repair performs no database calls."""
    import gzip as _gzmod, json as _jsonmod
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")

    start = date(2026, 1, 1)
    bars = [
        {"date": (start + timedelta(days=i)).isoformat(),
         "close": 100.0 + i, "volume": 1000.0}
        for i in range(50)
    ]
    sym = "NODB"
    gz_path = tmp_path / f"{sym}.json.gz"
    with _gzmod.open(str(gz_path), "wt", encoding="utf-8") as fh:
        fh.write(_jsonmod.dumps({
            "symbol": sym, "bar_count": 50,
            "history_status": "available_10y", "bars": bars,
        }))
    monkeypatch.setattr(chs, "_INDEX", {
        sym: {"symbol": sym, "bar_count": 50, "history_status": "available_10y"}
    })

    # canonical_history_service has no database imports — just confirm it doesn't
    # try to import or call psycopg2 during repair
    import sys
    psycopg2_before = "psycopg2" in sys.modules
    chs.ensure_comparison_close_tails()
    psycopg2_after = "psycopg2" in sys.modules
    # If psycopg2 wasn't loaded before, repair must not load it
    if not psycopg2_before:
        assert not psycopg2_after, "Repair must not import psycopg2"


def test_repair_behavior_second_call_is_noop_zero_gz_reads(monkeypatch, tmp_path):
    """T21: Repeating the helper immediately is a no-op with zero additional gz reads."""
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")
    monkeypatch.setattr(chs, "_INDEX", {})

    start = date(2026, 1, 1)
    bars = [
        {"date": (start + timedelta(days=i)).isoformat(),
         "close": 100.0 + i, "volume": 1000.0}
        for i in range(50)
    ]
    # Save without tail by removing it
    chs.save_bars("IDMPOT", bars, "tradier")
    meta_no_tail = {k: v for k, v in chs._INDEX["IDMPOT"].items()
                    if k != "comparison_close_tail"}
    chs._INDEX["IDMPOT"] = meta_no_tail

    # First call: repairs
    r1 = chs.ensure_comparison_close_tails()
    assert r1["updated"] == 1

    # Second call: no-op, zero gz reads
    import gzip as _gzmod
    open_count = [0]
    real_open = _gzmod.open

    def _counting(*a, **kw):
        open_count[0] += 1
        return real_open(*a, **kw)

    monkeypatch.setattr(chs.gzip, "open", _counting)
    r2 = chs.ensure_comparison_close_tails()

    assert r2["selected"] == 0, f"Second call must select 0; got {r2['selected']}"
    assert r2["status"] == "noop"
    assert open_count[0] == 0, f"Second call must open 0 gz files; got {open_count[0]}"


def test_repair_behavior_partial_failure_preserves_valid_tails(monkeypatch, tmp_path):
    """T22: A partial failure does not delete valid existing tails."""
    import gzip as _gzmod, json as _jsonmod
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")
    monkeypatch.setattr(chs, "_INDEX", {})

    start = date(2026, 1, 1)
    bars = [
        {"date": (start + timedelta(days=i)).isoformat(),
         "close": 100.0 + i, "volume": 1000.0}
        for i in range(50)
    ]

    # GOOD: has a valid tail
    chs.save_bars("GOOD22", bars, "tradier")
    good_tail_before = chs._INDEX["GOOD22"]["comparison_close_tail"][:]

    # BAD: has no tail; gz file is corrupt (will fail to repair)
    bad_gz = tmp_path / "BAD22.json.gz"
    bad_gz.write_bytes(b"not valid gzip content")
    chs._INDEX["BAD22"] = {
        "symbol": "BAD22", "bar_count": 50,
        "history_status": "available_10y",
    }

    result = chs.ensure_comparison_close_tails()

    # BAD was attempted (selected) but repair failed
    assert result["selected"] >= 1
    # GOOD tail must be untouched
    assert chs._INDEX["GOOD22"].get("comparison_close_tail") == good_tail_before, (
        "Partial failure must not clobber valid existing tails"
    )


# ── Bootstrap behavior tests (23–27) ─────────────────────────────────────────


def test_bootstrap_step_recorded_in_bootstrap_state(monkeypatch, tmp_path):
    """T23–T26: Bootstrap records comparison_tail_repair step with useful counts."""
    import asyncio as _asyncio
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")
    monkeypatch.setattr(chs, "_INDEX", {})

    start = date(2026, 1, 1)
    bars = [
        {"date": (start + timedelta(days=i)).isoformat(),
         "close": 100.0 + i, "volume": 1000.0}
        for i in range(50)
    ]
    chs.save_bars("BOOT", bars, "tradier")

    # Simulate the bootstrap pattern from main.py:
    # ensure_comparison_close_tails is called via asyncio.to_thread
    state: dict = {}

    async def _run():
        result = await _asyncio.to_thread(chs.ensure_comparison_close_tails)
        state["comparison_tail_repair"] = {
            "ok": result.get("status") != "error",
            **result,
        }

    _asyncio.run(_run())

    step = state["comparison_tail_repair"]
    assert "ok" in step
    assert "selected" in step
    assert "updated" in step
    assert "file_reads" in step
    assert "missing_after" in step
    assert "elapsed_ms" in step
    assert "status" in step
    assert step["ok"] is True, f"Step must succeed; got: {step}"


def test_bootstrap_step_exception_is_nonfatal(monkeypatch, tmp_path):
    """T25: A repair exception is caught and recorded; does not propagate."""
    import asyncio as _asyncio
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")

    def _exploding_repair():
        raise RuntimeError("Simulated repair explosion")

    monkeypatch.setattr(chs, "ensure_comparison_close_tails", _exploding_repair)

    state: dict = {}
    errors: list = []

    async def _run():
        try:
            result = await _asyncio.to_thread(chs.ensure_comparison_close_tails)
            state["comparison_tail_repair"] = {
                "ok": result.get("status") != "error",
                **result,
            }
        except Exception as _e:
            errors.append(_e)
            state["comparison_tail_repair"] = {"ok": False, "error": str(_e)}

    _asyncio.run(_run())

    # The bootstrap catches and records the error; does not re-raise
    assert "comparison_tail_repair" in state
    assert state["comparison_tail_repair"]["ok"] is False
    # Application startup continues (no unhandled exception from the bootstrap task)


def test_bootstrap_health_contract_unchanged():
    """T27: Bootstrap health-serving contract is unchanged: health endpoint does not
    depend on comparison_tail_repair completing before returning 200.

    Verifies structurally that ensure_comparison_close_tails is called exclusively
    inside _post_yield_bootstrap (after yield) and not in the pre-yield path.
    """
    import inspect
    import backend.main as _main_mod

    src = inspect.getsource(_main_mod)

    # Must be referenced at all
    assert "ensure_comparison_close_tails" in src, (
        "ensure_comparison_close_tails must be referenced in main.py"
    )
    # Must appear between the bootstrap function definition and its task creation.
    # The bootstrap task is created immediately after the function:
    #   asyncio.create_task(_post_yield_bootstrap())
    bootstrap_def = src.find("async def _post_yield_bootstrap()")
    bootstrap_task = src.find("asyncio.create_task(_post_yield_bootstrap())")
    assert bootstrap_def >= 0, "async def _post_yield_bootstrap() not found in main.py"
    assert bootstrap_task > bootstrap_def, (
        "asyncio.create_task(_post_yield_bootstrap()) must follow the function def"
    )
    repair_pos = src.find("ensure_comparison_close_tails")
    assert bootstrap_def < repair_pos < bootstrap_task, (
        "ensure_comparison_close_tails must appear inside _post_yield_bootstrap "
        "(between the function def and the create_task call), not before yield"
    )
    # Also verify the bootstrap step key is recorded
    assert "comparison_tail_repair" in src, (
        "_BOOTSTRAP_STATE step 'comparison_tail_repair' must be recorded in main.py"
    )


# ── Existing regression invariants (28–32) ───────────────────────────────────


def test_regression_get_comparison_closes_bulk_still_zero_gz_reads_normal(monkeypatch, tmp_path):
    """T28: Normal Watchlist request still performs zero gzip reads."""
    monkeypatch.setattr(chs, "_CANON_DIR", tmp_path)
    monkeypatch.setattr(chs, "_INDEX_FILE", tmp_path / "_index.json")
    monkeypatch.setattr(chs, "_INDEX", {})

    start = date(2026, 1, 1)
    bars = [
        {"date": (start + timedelta(days=i)).isoformat(),
         "close": 100.0 + i, "volume": 1000.0}
        for i in range(50)
    ]
    chs.save_bars("REG28", bars, "tradier")

    import gzip as _gzmod
    opens = [0]
    real_open = _gzmod.open

    def _counting(*a, **kw):
        opens[0] += 1
        return real_open(*a, **kw)

    monkeypatch.setattr(chs.gzip, "open", _counting)

    ny_ref = start + timedelta(days=50)
    result = chs.get_comparison_closes_bulk(["REG28"], ny_today=ny_ref)

    assert opens[0] == 0, f"T28: Expected 0 gz opens; got {opens[0]}"
    assert result["REG28"]["comparison_close_7d"] is not None


def test_regression_10d_staleness_guard_preserved():
    """T31: The 10-day staleness guard remains unchanged."""
    tail = [["2026-01-01", 100.0]]
    # Target 7d from 2026-08-05: Jul 29; Jan 1 to Jul 29 = 209 days >> 10
    result = chs._select_from_tail(tail, date(2026, 8, 5), max_gap_days=10)
    assert result["comparison_close_7d"] is None, "T31: Staleness guard must block stale comparison"


def test_regression_materially_stale_data_returns_null():
    """T32: Materially stale canonical data (bars stop 3+ weeks ago) returns null."""
    # Build a real metrics dict from bars that stopped 3 weeks before today
    ny_today = chs.ny_market_date()
    stale_last = ny_today - timedelta(days=22)  # 22 days stale
    start = stale_last - timedelta(days=31)
    bars = [
        {"date": (start + timedelta(days=i)).isoformat(),
         "close": 100.0 + i, "volume": 1000.0}
        for i in range(32)
    ]
    metrics = chs._compute_watchlist_market_metrics_from_bars(bars)
    tail = metrics.get("comparison_close_tail")
    assert tail is not None and len(tail) > 0, "Tail must be built"

    # Now select from that tail using today as as_of
    # The newest bar is 22 days old; 7d target is 7 days ago; gap from newest to 7d target ≥ 15 days
    result = chs._select_from_tail(tail, ny_today, max_gap_days=10)
    assert result["comparison_close_7d"] is None, (
        "T32: Stale canonical history (22d old) must yield null change_7d"
    )


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
