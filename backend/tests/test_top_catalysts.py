"""
Unit tests for services/top_catalysts_service.py.

Mocked-data-only — no FMP, no network, no DB. Snapshot reads are
monkey-patched and the earnings cache is seeded directly.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

_BACKEND = Path(__file__).resolve().parent.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

import services.top_catalysts_service as top_svc
from data.cache import cache
from services.top_catalysts_service import (
    DEFAULT_CAP,
    MAX_CAP,
    MIN_CAP,
    _merge_dedup,
    _normalize_event,
    _passes_garbage_filter,
    _score_event,
    get_top_catalysts,
)


def _seed_snapshots(monkeypatch, mapping: dict):
    """Patch the snapshot reader so each tab returns the supplied envelope."""
    def fake(tab: str):
        return mapping.get(
            tab,
            {"current_week": [], "previous_week": [], "last_updated": None,
             "status": "empty"},
        )
    monkeypatch.setattr(top_svc, "_get_snapshot", fake)


def _seed_earnings_cache(monday: date, friday: date, top_events: list[dict],
                        as_of: str = "2026-04-30T12:00:00Z"):
    ck = f"earnings:curated:week:{monday}:{friday}"
    cache.set(ck, {"asOf": as_of, "topEvents": top_events}, 600)
    return ck


def test_envelope_shape_when_empty(monkeypatch):
    _seed_snapshots(monkeypatch, {})
    env = get_top_catalysts(cap=30)
    assert env["tab"] == "top_catalysts"
    assert env["mode"] == "weekly"
    assert env["current_week"] == []
    assert env["previous_week"] == []
    assert env["status"] == "empty"
    assert env["last_updated"] is None


def test_cap_clamped_to_min_max(monkeypatch):
    _seed_snapshots(monkeypatch, {})
    assert get_top_catalysts(cap=1)["current_week"] == []
    # Too-large cap clamps silently — checked through _score path.
    assert MIN_CAP <= DEFAULT_CAP <= MAX_CAP


def test_garbage_filter_drops_preferred_and_warrants():
    bad = {"symbol": "BAC-PA", "companyName": "BAC Pref", "eventType": "dividends"}
    assert _passes_garbage_filter(bad) is False
    bad2 = {"symbol": "ABCWS", "companyName": "ABC Warrants", "eventType": "earnings"}
    assert _passes_garbage_filter(bad2) is False
    good = {"symbol": "AAPL", "companyName": "Apple", "eventType": "earnings",
            "marketCap": 3e12}
    assert _passes_garbage_filter(good) is True


def test_garbage_filter_drops_microcap_when_known():
    tiny = {"symbol": "TINY", "eventType": "dividends", "marketCap": 5_000_000}
    assert _passes_garbage_filter(tiny) is False
    # Missing market cap is permitted (do not over-filter on missing metadata).
    no_mc = {"symbol": "OKAY", "eventType": "dividends"}
    assert _passes_garbage_filter(no_mc) is True


def test_garbage_filter_drops_delisted_canonical():
    # TWTR is mapped to None in CANONICAL_SYMBOL_MAP → drop.
    twtr = {"symbol": "TWTR", "eventType": "earnings_dates",
            "marketCap": 50_000_000_000}
    assert _passes_garbage_filter(twtr) is False


def test_normalize_preserves_required_fields():
    raw = {"symbol": "FB", "companyName": "Facebook (legacy)",
           "eventType": "earnings_dates", "date": "2026-04-30",
           "time": "amc", "sector": "Technology", "importance": "high"}
    norm = _normalize_event(raw, source_tab="earnings")
    assert norm["symbol"] == "META"  # canonical remap applied
    assert norm["sourceTab"] == "earnings"
    assert norm["eventType"] == "earnings_dates"
    assert norm["time"] == "amc"
    assert norm["importance"] == "high"
    assert "raw" in norm


def test_score_earnings_outranks_dividends():
    earn = _normalize_event(
        {"symbol": "NVDA", "eventType": "earnings_dates", "date": "2026-04-30",
         "marketCap": 3e12, "importance": "high", "importanceScore": 90,
         "themeTags": ["ai_infra"], "sector": "Technology"},
        "earnings",
    )
    div = _normalize_event(
        {"symbol": "AAPL", "eventType": "dividends", "date": "2026-04-30",
         "marketCap": 3e12, "dividend": 1.0, "importance": "low"},
        "dividends",
    )
    e_score, _, _ = _score_event(earn, set(), set())
    d_score, _, _ = _score_event(div, set(), set())
    assert e_score > d_score


def test_merge_dedup_earnings_dominates():
    e1 = {"symbol": "AAPL", "eventType": "earnings_dates", "date": "2026-04-30",
          "score": 18.0, "scoreReasons": ["earnings event"], "sourceTab": "earnings"}
    e2 = {"symbol": "AAPL", "eventType": "dividends", "date": "2026-04-30",
          "score": 22.0, "scoreReasons": ["dividends event"], "sourceTab": "dividends"}
    merged = _merge_dedup([e1, e2])
    assert len(merged) == 1
    out = merged[0]
    # Earnings is the dominant primary even if its score was lower.
    assert out["eventType"] == "earnings_dates"
    # Multi-catalyst boost added.
    assert any("multiple catalysts" in r for r in out["scoreReasons"])
    # Score was bumped above the original earnings score.
    assert out["score"] > 18.0
    # Secondary catalyst preserved in raw.
    assert isinstance(out.get("raw"), dict)
    assert out["raw"].get("secondaryCatalysts")


def test_full_pipeline_ready_status(monkeypatch):
    monday = date(2026, 4, 27)
    friday = date(2026, 5, 1)
    monkeypatch.setattr(top_svc, "_week_bounds", lambda *_: (monday, friday))

    _seed_snapshots(monkeypatch, {
        "ipos": {
            "current_week": [{
                "symbol": "NEWCO", "eventType": "ipo", "date": "2026-04-29",
                "companyName": "New Co", "exchange": "NASDAQ",
                "marketCap": 6_000_000_000,
            }],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
        "dividends": {
            "current_week": [{
                "symbol": "AAPL", "eventType": "dividends", "date": "2026-04-30",
                "companyName": "Apple Inc.", "marketCap": 3e12,
                "dividend": 1.0, "importance": "medium",
            }],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    _seed_earnings_cache(monday, friday, [
        {"symbol": "AAPL", "eventType": "earnings_dates", "date": "2026-04-30",
         "companyName": "Apple Inc.", "marketCap": 3e12, "importance": "high",
         "importanceScore": 80},
        {"symbol": "BAC-PA", "eventType": "earnings_dates", "date": "2026-04-30"},
    ])

    env = get_top_catalysts(cap=DEFAULT_CAP)
    assert env["status"] == "ready"
    assert env["last_updated"] is not None
    syms = [(e.get("symbol"), e["eventType"]) for e in env["current_week"]]
    # AAPL appears once (earnings dominant), with dividend merged.
    aapl_rows = [s for s in syms if s[0] == "AAPL"]
    assert len(aapl_rows) == 1
    assert aapl_rows[0][1] == "earnings_dates"
    # NEWCO IPO present.
    assert any(s[0] == "NEWCO" for s in syms)
    # BAC-PA preferred junk filtered.
    assert not any(s[0] == "BAC-PA" for s in syms)
    # Cap respected.
    assert len(env["current_week"]) <= DEFAULT_CAP


def test_response_cap_bounds(monkeypatch):
    monday = date(2026, 4, 27)
    friday = date(2026, 5, 1)
    monkeypatch.setattr(top_svc, "_week_bounds", lambda *_: (monday, friday))
    # Synthesize 80 distinct earnings rows.
    rows = [
        {"symbol": f"SYM{i:03d}", "eventType": "earnings_dates",
         "date": "2026-04-30", "companyName": f"Co {i}",
         "marketCap": 1e10, "importance": "high"}
        for i in range(80)
    ]
    _seed_earnings_cache(monday, friday, rows)
    _seed_snapshots(monkeypatch, {})
    env = get_top_catalysts(cap=MAX_CAP)
    assert len(env["current_week"]) == MAX_CAP
    env_min = get_top_catalysts(cap=MIN_CAP)
    assert len(env_min["current_week"]) == MIN_CAP
