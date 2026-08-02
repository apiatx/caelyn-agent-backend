"""
Unit tests for services/top_catalysts_service.py.

Mocked-data-only — no FMP, no network, no DB. Snapshot reads, watchlist
loaders, and the options/sector caches are monkey-patched; the earnings
cache is seeded directly via data.cache.cache.
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
    MAX_EARNINGS_PER_DAY,
    MAX_OTHER_PER_DAY,
    MAX_OTHER_PER_WEEK,
    MIN_CAP,
    _classify_macro,
    _options_strength,
    _passes_garbage_filter,
    _score_earnings,
    get_top_catalysts,
)


# ── Helpers ────────────────────────────────────────────────────────────────

_MONDAY = date(2026, 4, 27)
_FRIDAY = date(2026, 5, 1)


def _seed_snapshots(monkeypatch, mapping: dict):
    """Patch the snapshot reader so each tab returns the supplied envelope."""
    def fake(tab: str):
        return mapping.get(
            tab,
            {"current_week": [], "previous_week": [], "last_updated": None,
             "status": "empty"},
        )
    monkeypatch.setattr(top_svc, "_get_snapshot", fake)
    # get_canonical_macro_window imports get_snapshot directly from the
    # snapshot service, so patch that module attribute too.
    from services import calendar_snapshot_service as _snap_svc
    monkeypatch.setattr(_snap_svc, "get_snapshot", fake)


def _seed_watchlist(monkeypatch, syms: set[str], pf: set[str] | None = None):
    monkeypatch.setattr(top_svc, "_load_watchlist_set", lambda: set(syms))
    monkeypatch.setattr(top_svc, "_load_portfolio_set", lambda: set(pf or set()))


def _seed_options(monkeypatch, mapping: dict[str, dict]):
    monkeypatch.setattr(top_svc, "_read_options_master", lambda: dict(mapping))


def _seed_sectors(monkeypatch, mapping: dict[str, dict]):
    monkeypatch.setattr(top_svc, "_read_sector_dashboard", lambda: dict(mapping))


def _seed_week(monkeypatch):
    monkeypatch.setattr(top_svc, "_week_bounds", lambda *_: (_MONDAY, _FRIDAY))


def _seed_earnings_cache(top_events: list[dict], as_of: str = "2026-04-30T12:00:00Z"):
    ck = f"earnings:curated:week:{_MONDAY}:{_FRIDAY}"
    cache.set(ck, {"asOf": as_of, "topEvents": top_events}, 600)
    return ck


def _clear_earnings_cache():
    cache.set(f"earnings:curated:week:{_MONDAY}:{_FRIDAY}", None, 1)


# ── Envelope shape ──────────────────────────────────────────────────────────

def test_envelope_shape_when_empty(monkeypatch):
    _seed_week(monkeypatch)
    _seed_snapshots(monkeypatch, {})
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()

    env = get_top_catalysts(cap=30)
    assert env["tab"] == "top_catalysts"
    assert env["mode"] == "weekly"
    assert env["week"] == "2026-04-27/2026-05-01"
    # days[] must be 5 weekday entries Mon-Fri.
    assert len(env["days"]) == 5
    assert [d["date"] for d in env["days"]] == [
        "2026-04-27", "2026-04-28", "2026-04-29", "2026-04-30", "2026-05-01"
    ]
    for d in env["days"]:
        assert d["earnings"] == []
        assert d["macro"]    == []
        assert d["other"]    == []
    assert env["current_week"] == []
    assert env["previous_week"] == []
    assert env["status"] == "empty"


def test_grouped_days_have_required_keys(monkeypatch):
    _seed_week(monkeypatch)
    _seed_snapshots(monkeypatch, {})
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    env = get_top_catalysts()
    for d in env["days"]:
        assert set(d.keys()) >= {"date", "weekday", "earnings", "macro", "other"}


# ── Macro whitelist ─────────────────────────────────────────────────────────

def test_macro_whitelist_classifies_canonical_events():
    assert _classify_macro({"eventName": "CPI YoY"}) == "CPI"
    assert _classify_macro({"eventName": "Core PPI MoM"}) == "PPI"
    assert _classify_macro({"eventName": "Nonfarm Payrolls"}) == "NFP"
    assert _classify_macro({"eventName": "FOMC Meeting Minutes"}) == "FOMC"
    assert _classify_macro({"indicatorName": "GDP Growth Rate"}) == "GDP"
    assert _classify_macro({"indicatorName": "10-Year Treasury Auction"}) == "Treasury Auctions"


def test_macro_whitelist_drops_low_signal_events():
    # These are NOT in the whitelist and must be excluded.
    for ev in [
        {"eventName": "Retail Sales"},
        {"eventName": "ISM Manufacturing PMI"},
        {"eventName": "Initial Jobless Claims"},
        {"eventName": "Building Permits"},
        {"eventName": "Trade Balance"},
    ]:
        assert _classify_macro(ev) is None


def test_macro_only_whitelist_in_response(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                {"eventName": "CPI YoY", "date": "2026-04-29",
                 "country": "US", "importance": "high"},
                {"eventName": "Retail Sales", "date": "2026-04-29",
                 "country": "US", "importance": "medium"},
                {"eventName": "ISM Manufacturing", "date": "2026-04-30",
                 "country": "US", "importance": "medium"},
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
        "treasury_macro": {
            "current_week": [
                {"indicatorName": "10-Year Treasury Auction",
                 "date": "2026-04-30", "country": "US"},
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })

    env = get_top_catalysts()
    macro_titles = [m["macroType"] for d in env["days"] for m in d["macro"]]
    macro_card_titles = [m["title"] for d in env["days"] for m in d["macro"]]
    assert "CPI" in macro_titles
    assert "Treasury Auctions" in macro_titles
    # ISM now forms a canonical release-package card via the shared pipeline.
    assert "ISM Manufacturing Report" in macro_card_titles
    # Non-whitelist low-signal discrete events must be absent.
    assert all("Retail" not in str(m.get("title", "")) for d in env["days"] for m in d["macro"])


# ── IPO/Dividend/Split exclusion + caps ────────────────────────────────────

def test_ipos_excluded_unless_large_or_hot_theme(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "ipos": {
            "current_week": [
                # Small IPO without theme — excluded.
                {"symbol": "TINY", "eventType": "ipo", "date": "2026-04-29",
                 "companyName": "Tiny Co", "marketCap": 200_000_000},
                # Large IPO — allowed.
                {"symbol": "BIGCO", "eventType": "ipo", "date": "2026-04-29",
                 "companyName": "Big Co", "marketCap": 80_000_000_000},
                # Theme IPO — allowed.
                {"symbol": "AICHIP", "eventType": "ipo", "date": "2026-04-30",
                 "companyName": "AI Chip Co", "marketCap": 1_000_000_000,
                 "themeTags": ["semiconductors"]},
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    other_syms = [o.get("symbol") for d in env["days"] for o in d["other"]]
    assert "TINY" not in other_syms
    assert "BIGCO" in other_syms
    assert "AICHIP" in other_syms


def test_other_capped_per_week(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    rows = [
        {"symbol": f"BIG{i}", "eventType": "ipo",
         "date": "2026-04-29" if i % 2 else "2026-04-30",
         "companyName": f"Big {i}", "marketCap": 80_000_000_000}
        for i in range(10)
    ]
    _seed_snapshots(monkeypatch, {
        "ipos": {"current_week": rows, "previous_week": [],
                 "last_updated": "2026-04-28T10:00:00Z", "status": "ready"},
    })
    env = get_top_catalysts()
    total_other = sum(len(d["other"]) for d in env["days"])
    assert total_other <= MAX_OTHER_PER_WEEK
    for d in env["days"]:
        assert len(d["other"]) <= MAX_OTHER_PER_DAY


# ── Garbage filter ──────────────────────────────────────────────────────────

def test_garbage_filter_drops_preferred_and_warrants():
    bad = {"symbol": "BAC-PA", "companyName": "BAC Pref", "eventType": "dividends"}
    assert _passes_garbage_filter(bad) is False
    bad2 = {"symbol": "ABCWS", "companyName": "ABC Warrants", "eventType": "earnings"}
    assert _passes_garbage_filter(bad2) is False
    good = {"symbol": "AAPL", "companyName": "Apple", "eventType": "earnings",
            "marketCap": 3e12}
    assert _passes_garbage_filter(good) is True


# ── Options strength classification (existing fields only) ──────────────────

def test_options_strength_unusual_high_normal_none():
    assert _options_strength(None)[0] == "none"
    assert _options_strength({})[0] == "none"
    # composite_score >= 75 → unusual
    assert _options_strength({"composite_score": 82})[0] == "unusual"
    # heat_score 70+ → unusual
    assert _options_strength({"heat_score": 71})[0] == "unusual"
    # uvr 3.0+ → unusual
    assert _options_strength({"unusual_volume_ratio": 3.5})[0] == "unusual"
    # mid range → high
    assert _options_strength({"composite_score": 55})[0] == "high"
    # tiny but present → normal
    assert _options_strength({"composite_score": 5})[0] == "normal"


# ── Earnings ranking driven by 3 signals only (no marketCap) ────────────────

def test_score_ranks_unusual_options_above_market_cap(monkeypatch):
    sectors: dict[str, dict] = {}
    # A: mega cap, no options activity, no watchlist.
    a = {"symbol": "A", "eventType": "earnings_dates", "date": "2026-04-30",
         "marketCap": 3_000_000_000_000}
    # B: small cap but UNUSUAL options activity.
    b = {"symbol": "B", "eventType": "earnings_dates", "date": "2026-04-30",
         "marketCap": 1_500_000_000}
    a_score, _ = _score_earnings(a, None, sectors, set(), set())
    b_score, _ = _score_earnings(
        b,
        {"composite_score": 90, "heat_score": 80, "unusual_volume_ratio": 4.5},
        sectors, set(), set(),
    )
    assert b_score > a_score, "Unusual options activity must outrank market cap"


def test_score_watchlist_boost_binary():
    a = {"symbol": "X", "eventType": "earnings_dates", "date": "2026-04-30"}
    s_no, sig_no = _score_earnings(a, None, {}, set(), set())
    s_yes, sig_yes = _score_earnings(a, None, {}, {"X"}, set())
    assert s_yes > s_no
    assert sig_yes["watchlist_boost"] is True
    assert sig_no["watchlist_boost"] is False


def test_score_sector_alignment_uses_sector_dashboard():
    ev = {"symbol": "SOMECO", "eventType": "earnings_dates",
          "date": "2026-04-30", "sector": "Technology"}
    sectors_hot = {"XLK": {"ticker": "XLK", "rotation_score": 88,
                           "regime_tag": "Leadership"}}
    sectors_cold = {"XLK": {"ticker": "XLK", "rotation_score": 10,
                            "regime_tag": "Lagging"}}
    s_hot, sig_hot = _score_earnings(ev, None, sectors_hot, set(), set())
    s_cold, sig_cold = _score_earnings(ev, None, sectors_cold, set(), set())
    assert s_hot > s_cold
    assert sig_hot["sector_alignment_strength"] == "hot"
    assert sig_cold["sector_alignment_strength"] in ("none", "neutral")


def test_no_market_cap_in_score_signals():
    """Score signals dict must not contain marketCap-derived fields."""
    ev = {"symbol": "Z", "eventType": "earnings_dates",
          "date": "2026-04-30", "marketCap": 2e12}
    _, sig = _score_earnings(ev, None, {}, set(), set())
    assert "marketCap" not in sig
    assert "marketCapBucket" not in sig


# ── Per-day caps ────────────────────────────────────────────────────────────

def test_earnings_per_day_capped(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _seed_snapshots(monkeypatch, {})
    rows = [
        {"symbol": f"S{i:03d}", "eventType": "earnings_dates",
         "date": "2026-04-30", "companyName": f"Co {i}",
         "marketCap": 1e10, "importanceScore": 80 - i}
        for i in range(20)
    ]
    _seed_earnings_cache(rows)
    env = get_top_catalysts()
    thursday = next(d for d in env["days"] if d["date"] == "2026-04-30")
    assert len(thursday["earnings"]) <= MAX_EARNINGS_PER_DAY


# ── Earnings dominance over IPO/div/split ───────────────────────────────────

def test_earnings_dominates_other(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _seed_snapshots(monkeypatch, {
        "ipos": {
            "current_week": [
                {"symbol": "BIGCO", "eventType": "ipo", "date": "2026-04-30",
                 "companyName": "Big Co", "marketCap": 80_000_000_000},
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    _seed_earnings_cache([
        {"symbol": "AAPL", "eventType": "earnings_dates", "date": "2026-04-30",
         "companyName": "Apple", "marketCap": 3e12, "importanceScore": 90},
    ])

    env = get_top_catalysts()
    # The flat current_week list should lead with earnings, not IPO.
    assert env["current_week"], "expected at least one entry"
    assert env["current_week"][0]["eventType"] == "earnings"


# ── No request-time external calls (smoke) ──────────────────────────────────

def test_no_request_time_fmp_or_profile_calls(monkeypatch):
    """
    The service must not import or call any request-time fetch helpers.
    We assert by patching httpx/_enrich_profiles to raise if called.
    """
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _seed_snapshots(monkeypatch, {})
    _seed_earnings_cache([
        {"symbol": "AAPL", "eventType": "earnings_dates", "date": "2026-04-30",
         "companyName": "Apple", "importanceScore": 80},
    ])

    import httpx
    def boom(*a, **k):
        raise AssertionError("network call attempted at request time")
    monkeypatch.setattr(httpx, "AsyncClient", boom)
    monkeypatch.setattr(httpx, "Client", boom)

    env = get_top_catalysts()
    assert env["status"] in ("ready", "stale", "empty")


# ── Backward-compat flat current_week cap ──────────────────────────────────

def test_flat_current_week_capped(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _seed_snapshots(monkeypatch, {})
    rows = [
        {"symbol": f"SYM{i:03d}", "eventType": "earnings_dates",
         "date": ["2026-04-27", "2026-04-28", "2026-04-29",
                  "2026-04-30", "2026-05-01"][i % 5],
         "companyName": f"Co {i}", "importanceScore": 50}
        for i in range(80)
    ]
    _seed_earnings_cache(rows)
    env = get_top_catalysts(cap=MAX_CAP)
    assert len(env["current_week"]) <= MAX_CAP
    assert MIN_CAP <= DEFAULT_CAP <= MAX_CAP


# ═══════════════════════════════════════════════════════════════════════════════
# Macro family-grouping tests (Top Catalysts integration)
# ═══════════════════════════════════════════════════════════════════════════════


def _make_macro_ev(**kw) -> dict:
    return {
        "id":              kw.get("id", "ev1"),
        "eventType":       kw.get("eventType", "economic_release"),
        "eventName":       kw.get("eventName", kw.get("title", "CPI MoM")),
        "title":           kw.get("title", kw.get("eventName", "CPI MoM")),
        "date":            kw.get("date", "2026-04-29"),
        "time":            kw.get("time"),
        "country":         kw.get("country", "US"),
        "importance":      kw.get("importance", "high"),
        "actual":          kw.get("actual"),
        "estimate":        kw.get("estimate"),
        "previous":        kw.get("previous"),
        "unit":            kw.get("unit"),
        "event_family":    kw.get("event_family"),
        "signal_tier":     kw.get("signal_tier"),
        "signal_reason":   kw.get("signal_reason"),
        "source":          kw.get("source", "fmp"),
    }


def test_cpi_variants_produce_one_macro_entry(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="c1", title="CPI MoM", event_family="cpi",
                               signal_tier="major", date="2026-04-29", actual=0.2),
                _make_macro_ev(id="c2", title="Core CPI MoM", event_family="cpi",
                               signal_tier="major", date="2026-04-29", actual=0.1),
                _make_macro_ev(id="c3", title="CPI YoY", event_family="cpi",
                               signal_tier="major", date="2026-04-29", actual=3.0),
                _make_macro_ev(id="c4", title="Core CPI YoY", event_family="cpi",
                               signal_tier="major", date="2026-04-29", actual=3.2),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    macro_entries = [m for d in env["days"] for m in d["macro"]]
    cpi_entries = [m for m in macro_entries if m.get("macroType") == "CPI"]
    assert len(cpi_entries) == 1, f"expected 1 CPI entry, got {len(cpi_entries)}"


def test_ppi_variants_produce_one_macro_entry(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="p1", title="PPI MoM", event_family="ppi",
                               signal_tier="major", date="2026-04-29"),
                _make_macro_ev(id="p2", title="Core PPI MoM", event_family="ppi",
                               signal_tier="major", date="2026-04-29"),
                _make_macro_ev(id="p3", title="PPI YoY", event_family="ppi",
                               signal_tier="major", date="2026-04-29"),
                _make_macro_ev(id="p4", title="Core PPI YoY", event_family="ppi",
                               signal_tier="major", date="2026-04-29"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    macro_entries = [m for d in env["days"] for m in d["macro"]]
    ppi_entries = [m for m in macro_entries if m.get("macroType") == "PPI"]
    assert len(ppi_entries) == 1, f"expected 1 PPI entry, got {len(ppi_entries)}"


def test_pce_variants_produce_one_macro_entry(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="x1", title="PCE Price Index MoM",
                               event_family="pce", signal_tier="major",
                               date="2026-04-30", actual=0.2, estimate=0.3,
                               previous=0.1, unit="%"),
                _make_macro_ev(id="x2", title="Core PCE Price Index MoM",
                               event_family="pce", signal_tier="major",
                               date="2026-04-30", actual=0.1, estimate=0.2,
                               previous=0.1, unit="%"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    macro_entries = [m for d in env["days"] for m in d["macro"]]
    pce_entries = [m for m in macro_entries if m.get("macroType") == "PCE"]
    assert len(pce_entries) == 1, f"expected 1 PCE entry, got {len(pce_entries)}"
    entry = pce_entries[0]
    assert entry["macroType"] == "PCE"
    assert entry["type"] == "macro_family"
    assert "children" in entry
    assert entry["event_count"] == 2
    assert entry["actual"] == 0.1  # Core PCE MoM lead
    assert entry["estimate"] == 0.2
    assert entry["previous"] == 0.1
    assert entry.get("unit") == "%"


def test_gdp_variants_produce_one_macro_entry(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="g1", title="GDP Growth Rate QoQ",
                               event_family="gdp", signal_tier="major",
                               date="2026-04-30", actual=2.4, estimate=2.5,
                               previous=3.0, unit="%"),
                _make_macro_ev(id="g2", title="GDP Price Index",
                               event_family="gdp", signal_tier="major",
                               date="2026-04-30", actual=3.1, unit="%"),
                _make_macro_ev(id="g3", title="GDP Sales QoQ",
                               event_family="gdp", signal_tier="major",
                               date="2026-04-30", actual=3.0, unit="%"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    macro_entries = [m for d in env["days"] for m in d["macro"]]
    gdp_entries = [m for m in macro_entries if m.get("macroType") == "GDP"]
    assert len(gdp_entries) == 1, f"expected 1 GDP entry, got {len(gdp_entries)}"


def test_us_cpi_and_eu_cpi_do_not_merge(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="u1", title="CPI YoY", event_family="cpi",
                               signal_tier="major", date="2026-04-29",
                               country="US"),
                _make_macro_ev(id="e1", title="CPI YoY", event_family="cpi",
                               signal_tier="major", date="2026-04-29",
                               country="EU"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    macro_entries = [m for d in env["days"] for m in d["macro"]]
    cpi_entries = [m for m in macro_entries if m.get("macroType") == "CPI"]
    assert len(cpi_entries) == 1, "EU CPI should have been excluded"


def test_eu_cpi_excluded_from_curated_top_catalysts(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="eu1", title="CPI YoY", event_family="cpi",
                               signal_tier="major", date="2026-04-29",
                               country="EU"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    macro_entries = [m for d in env["days"] for m in d["macro"]]
    assert len(macro_entries) == 0


def test_children_preserved_in_family_entry(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="c1", title="CPI MoM", event_family="cpi",
                               signal_tier="major", date="2026-04-29"),
                _make_macro_ev(id="c2", title="Core CPI MoM", event_family="cpi",
                               signal_tier="major", date="2026-04-29"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    cpi_entries = [m for d in env["days"] for m in d["macro"] if m.get("macroType") == "CPI"]
    assert len(cpi_entries) == 1
    entry = cpi_entries[0]
    assert "children" in entry
    assert isinstance(entry["children"], list)
    assert entry.get("event_count") == 2


def test_lead_actual_estimate_previous_surfaces(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="c1", title="CPI MoM", event_family="cpi",
                               signal_tier="major", date="2026-04-29",
                               actual=0.2, estimate=0.3, previous=0.2, unit="%"),
                _make_macro_ev(id="c2", title="Core CPI MoM", event_family="cpi",
                               signal_tier="major", date="2026-04-29",
                               actual=0.1, estimate=0.2, previous=0.1, unit="%"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    cpi_entry = [m for d in env["days"] for m in d["macro"] if m.get("macroType") == "CPI"][0]
    assert cpi_entry["actual"] == 0.1
    assert cpi_entry["estimate"] == 0.2
    assert cpi_entry["previous"] == 0.1
    assert cpi_entry.get("unit") == "%"


def test_rendered_macro_count_reflects_family_cards(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="c1", title="CPI MoM", event_family="cpi",
                               signal_tier="major", date="2026-04-29"),
                _make_macro_ev(id="c2", title="Core CPI MoM", event_family="cpi",
                               signal_tier="major", date="2026-04-29"),
                _make_macro_ev(id="c3", title="CPI YoY", event_family="cpi",
                               signal_tier="major", date="2026-04-29"),
                _make_macro_ev(id="c4", title="Core CPI YoY", event_family="cpi",
                               signal_tier="major", date="2026-04-29"),
                _make_macro_ev(id="p1", title="PPI MoM", event_family="ppi",
                               signal_tier="major", date="2026-04-29"),
                _make_macro_ev(id="p2", title="Core PPI MoM", event_family="ppi",
                               signal_tier="major", date="2026-04-29"),
                _make_macro_ev(id="p3", title="PPI YoY", event_family="ppi",
                               signal_tier="major", date="2026-04-29"),
                _make_macro_ev(id="p4", title="Core PPI YoY", event_family="ppi",
                               signal_tier="major", date="2026-04-29"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    macro_entries = [m for d in env["days"] for m in d["macro"]]
    assert len(macro_entries) == 2, f"expected 2 (CPI+PPI) family entries, got {len(macro_entries)}"


def test_raw_source_count_unchanged(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="s1", title="CPI MoM", event_family="cpi",
                               signal_tier="major", date="2026-04-29"),
                _make_macro_ev(id="s2", title="Core CPI MoM", event_family="cpi",
                               signal_tier="major", date="2026-04-29"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    cpi_entry = [m for d in env["days"] for m in d["macro"] if m.get("macroType") == "CPI"][0]
    assert cpi_entry.get("raw") is None  # family card has no single raw row


def test_earnings_behavior_unchanged(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _seed_earnings_cache([
        {"symbol": "AAPL", "eventType": "earnings_dates", "date": "2026-04-30",
         "companyName": "Apple", "marketCap": 3e12, "importanceScore": 90},
    ])
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="c1", title="CPI MoM", event_family="cpi",
                               signal_tier="major", date="2026-04-30"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    assert env["current_week"]
    assert env["current_week"][0]["eventType"] == "earnings"
    assert env["current_week"][0]["symbol"] == "AAPL"


def test_fomc_remains_individual(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="fm1", title="FOMC Interest Rate Decision",
                               event_family="fomc_decision", signal_tier="critical",
                               date="2026-04-30"),
                _make_macro_ev(id="fm2", title="FOMC Minutes",
                               event_family="fomc_minutes", signal_tier="major",
                               date="2026-04-28"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    macro_entries = [m for d in env["days"] for m in d["macro"]]
    fomc_entries = [m for m in macro_entries if m.get("macroType") == "FOMC"]
    assert len(fomc_entries) >= 1, f"expected at least 1 FOMC entry, got {len(fomc_entries)}"
    for m in fomc_entries:
        assert m.get("type") != "macro_family"
        assert m["eventType"] == "macro"


def test_no_extra_provider_calls(monkeypatch):
    import httpx

    def boom(*a, **k):
        raise AssertionError("network call attempted at request time")

    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="c1", title="CPI MoM", event_family="cpi",
                               signal_tier="major", date="2026-04-29"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    monkeypatch.setattr(httpx, "AsyncClient", boom)
    monkeypatch.setattr(httpx, "Client", boom)
    env = get_top_catalysts()
    assert env["status"] in ("ready", "stale", "empty")


def test_existing_response_envelope_fields_unchanged(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="c1", title="CPI MoM", event_family="cpi",
                               signal_tier="major", date="2026-04-29"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    assert env["tab"] == "top_catalysts"
    assert env["mode"] == "weekly"
    assert "week" in env
    assert "days" in env
    assert "current_week" in env
    assert "previous_week" in env
    assert "last_updated" in env
    assert "status" in env
    assert len(env["days"]) == 5


# ═══════════════════════════════════════════════════════════════════════════════
# PCE / ECI Top Catalysts integration tests
# ═══════════════════════════════════════════════════════════════════════════════

def test_eci_variants_produce_one_entry(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="e1", title="Employment Cost Index QoQ",
                               event_family="eci", signal_tier="major",
                               date="2026-04-29", actual=0.9, estimate=1.0,
                               previous=0.8, unit="%"),
                _make_macro_ev(id="e2", title="Employment Cost Index YoY",
                               event_family="eci", signal_tier="major",
                               date="2026-04-29", actual=3.5, unit="%"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    macro_entries = [m for d in env["days"] for m in d["macro"]]
    eci_entries = [m for m in macro_entries if m.get("macroType") == "ECI"]
    assert len(eci_entries) == 1, f"expected 1 ECI entry, got {len(eci_entries)}"
    entry = eci_entries[0]
    assert entry["macroType"] == "ECI"
    assert entry["type"] == "macro_family"
    assert entry["event_count"] == 2
    assert entry["actual"] == 0.9  # ECI QoQ lead
    assert entry["estimate"] == 1.0
    assert entry["previous"] == 0.8
    assert entry.get("unit") == "%"


def test_eci_children_preserved(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="e1", title="Employment Cost Index QoQ",
                               event_family="eci", signal_tier="major",
                               date="2026-04-29"),
                _make_macro_ev(id="e2", title="Employment Cost Index YoY",
                               event_family="eci", signal_tier="major",
                               date="2026-04-29"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    eci_entries = [m for d in env["days"] for m in d["macro"] if m.get("macroType") == "ECI"]
    assert len(eci_entries) == 1
    entry = eci_entries[0]
    assert "children" in entry
    assert isinstance(entry["children"], list)
    assert entry["event_count"] == 2


def test_foreign_pce_excluded(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="fp1", title="PCE Price Index MoM",
                               event_family="pce", signal_tier="major",
                               date="2026-04-29", country="EU"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    macro_entries = [m for d in env["days"] for m in d["macro"]]
    assert len(macro_entries) == 0


def test_foreign_eci_excluded(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="fe1", title="Employment Cost Index",
                               event_family="eci", signal_tier="major",
                               date="2026-04-29", country="JP"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    macro_entries = [m for d in env["days"] for m in d["macro"]]
    assert len(macro_entries) == 0


def test_pce_cpi_ppi_gdp_eci_counts_with_families(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="c1", title="CPI MoM", event_family="cpi",
                               signal_tier="major", date="2026-04-29"),
                _make_macro_ev(id="c2", title="Core CPI MoM", event_family="cpi",
                               signal_tier="major", date="2026-04-29"),
                _make_macro_ev(id="p1", title="PPI MoM", event_family="ppi",
                               signal_tier="major", date="2026-04-29"),
                _make_macro_ev(id="x1", title="PCE Price Index MoM",
                               event_family="pce", signal_tier="major",
                               date="2026-04-30"),
                _make_macro_ev(id="x2", title="Core PCE MoM",
                               event_family="pce", signal_tier="major",
                               date="2026-04-30"),
                _make_macro_ev(id="e1", title="Employment Cost Index QoQ",
                               event_family="eci", signal_tier="major",
                               date="2026-04-29"),
                _make_macro_ev(id="g1", title="GDP Growth Rate QoQ",
                               event_family="gdp", signal_tier="major",
                               date="2026-04-30"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    macro_entries = [m for d in env["days"] for m in d["macro"]]
    macro_types = [m["macroType"] for m in macro_entries]
    assert "CPI" in macro_types
    assert "PPI" in macro_types
    assert "PCE" in macro_types
    assert "ECI" in macro_types
    assert "GDP" in macro_types
    assert len(macro_entries) == 5, f"expected 5 family entries, got {len(macro_entries)}"


# ═══════════════════════════════════════════════════════════════════════════════
# PCE plural regex correction tests
# ═══════════════════════════════════════════════════════════════════════════════

def test_pce_singular_expenditure_classified():
    assert _classify_macro({"eventName": "Personal Consumption Expenditure"}) == "PCE"


def test_pce_plural_expenditures_classified():
    assert _classify_macro({"eventName": "Personal Consumption Expenditures"}) == "PCE"


def test_core_pce_price_index_remains_classified():
    assert _classify_macro({"eventName": "Core PCE Price Index MoM"}) == "PCE"


def test_plural_pce_provider_label_appears_in_top_catalysts(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="pp1", title="Personal Consumption Expenditures",
                               event_family="pce", signal_tier="major",
                               date="2026-04-29", country="US"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    macro_entries = [m for d in env["days"] for m in d["macro"]]
    pce_entries = [m for m in macro_entries if m.get("macroType") == "PCE"]
    assert len(pce_entries) == 1


def test_plural_pce_foreign_label_excluded(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="fp1", title="Personal Consumption Expenditures",
                               event_family="pce", signal_tier="major",
                               date="2026-04-29", country="EU"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    macro_entries = [m for d in env["days"] for m in d["macro"]]
    assert len(macro_entries) == 0


def test_consumption_alone_does_not_match_pce():
    assert _classify_macro({"eventName": "Energy Consumption Report"}) is None
    assert _classify_macro({"eventName": "Household Consumption Data"}) is None


def test_pce_family_card_still_produced_after_regex_fix(monkeypatch):
    _seed_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "current_week": [
                _make_macro_ev(id="x1", title="PCE Price Index MoM",
                               event_family="pce", signal_tier="major",
                               date="2026-04-30", actual=0.2, unit="%"),
                _make_macro_ev(id="x2", title="Core PCE Price Index MoM",
                               event_family="pce", signal_tier="major",
                               date="2026-04-30", actual=0.1, unit="%"),
            ],
            "previous_week": [], "last_updated": "2026-04-28T10:00:00Z",
            "status": "ready",
        },
    })
    env = get_top_catalysts()
    pce_entries = [m for d in env["days"] for m in d["macro"] if m.get("macroType") == "PCE"]
    assert len(pce_entries) == 1
    entry = pce_entries[0]
    assert entry["type"] == "macro_family"
    assert entry["event_count"] == 2
    assert entry["actual"] == 0.1


# ═══════════════════════════════════════════════════════════════════════════════
# Unified planning-window regression tests (Aug 3–7, 2026)
# ═══════════════════════════════════════════════════════════════════════════════

_AUG3 = date(2026, 8, 3)
_AUG7 = date(2026, 8, 7)


def _seed_aug_week(monkeypatch):
    """Patch the week bounds to the Aug 3–7 planning window."""
    monkeypatch.setattr(top_svc, "_week_bounds", lambda *_: (_AUG3, _AUG7))


def _make_aug_econ(**kw) -> dict:
    """Build a synthetic economic event for the Aug 3–7 window."""
    return {
        "id": kw.get("id", "ev1"),
        "eventType": "economic_release",
        "eventName": kw.get("eventName", kw.get("title", "Event")),
        "title": kw.get("title", kw.get("eventName", "Event")),
        "date": kw.get("date", "2026-08-03"),
        "time": kw.get("time"),
        "country": kw.get("country", "US"),
        "importance": kw.get("importance", "high"),
        "actual": kw.get("actual"),
        "estimate": kw.get("estimate"),
        "previous": kw.get("previous"),
        "event_family": kw.get("event_family"),
        "signal_tier": kw.get("signal_tier"),
        "signal_reason": kw.get("signal_reason"),
    }


def test_sunday_aug_2_resolves_to_aug_3_7(monkeypatch):
    _seed_aug_week(monkeypatch)
    _seed_snapshots(monkeypatch, {})
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    env = get_top_catalysts(today=date(2026, 8, 2))
    assert env["week"] == "2026-08-03/2026-08-07"
    assert env["week_start"] == "2026-08-03"
    assert env["week_end"] == "2026-08-07"
    assert env["window_mode"] == "next_week_planning"
    assert all(d["date"] >= "2026-08-03" for d in env["days"])


def test_saturday_aug_1_resolves_to_aug_3_7(monkeypatch):
    _seed_aug_week(monkeypatch)
    _seed_snapshots(monkeypatch, {})
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    env = get_top_catalysts(today=date(2026, 8, 1))
    assert env["week"] == "2026-08-03/2026-08-07"
    assert env["window_mode"] == "next_week_planning"


def test_weekday_aug_3_resolves_to_current_week(monkeypatch):
    _seed_aug_week(monkeypatch)
    _seed_snapshots(monkeypatch, {})
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    env = get_top_catalysts(today=date(2026, 8, 3))
    assert env["week"] == "2026-08-03/2026-08-07"
    assert env["window_mode"] == "current_week"


def test_ism_rows_form_one_canonical_card(monkeypatch):
    _seed_aug_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "events": [
                _make_aug_econ(id="ism1", title="ISM Manufacturing PMI",
                               eventName="ISM Manufacturing PMI", date="2026-08-03",
                               signal_tier="secondary", signal_reason="ISM business survey"),
                _make_aug_econ(id="ism2", title="ISM Manufacturing New Orders",
                               eventName="ISM Manufacturing New Orders", date="2026-08-03",
                               signal_tier="secondary", signal_reason="ISM business survey"),
                _make_aug_econ(id="ism3", title="ISM Manufacturing Employment",
                               eventName="ISM Manufacturing Employment", date="2026-08-03",
                               signal_tier="secondary", signal_reason="ISM business survey"),
            ],
            "current_week": [], "previous_week": [],
            "last_updated": "2026-08-02T10:00:00Z", "status": "ready",
        },
    })
    env = get_top_catalysts(today=date(2026, 8, 2))
    ism_cards = [m for d in env["days"] for m in d["macro"]
                 if m.get("macroType") == "ISM Manufacturing Report"]
    assert len(ism_cards) == 1
    assert ism_cards[0]["title"] == "ISM Manufacturing Report"
    assert ism_cards[0]["event_count"] == 3
    assert ism_cards[0]["signal_tier"] == "secondary"


def test_employment_rows_form_one_major_card(monkeypatch):
    _seed_aug_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "events": [
                _make_aug_econ(id="nfp", title="Non Farm Payrolls",
                               eventName="Non Farm Payrolls", date="2026-08-07",
                               event_family="payrolls", signal_tier="major",
                               signal_reason="Monthly payroll release"),
                _make_aug_econ(id="ur", title="Unemployment Rate",
                               eventName="Unemployment Rate", date="2026-08-07",
                               event_family="unemployment", signal_tier="secondary",
                               signal_reason="Unemployment rate release"),
            ],
            "current_week": [], "previous_week": [],
            "last_updated": "2026-08-02T10:00:00Z", "status": "ready",
        },
    })
    env = get_top_catalysts(today=date(2026, 8, 2))
    emp_cards = [m for d in env["days"] for m in d["macro"]
                 if m.get("macroType") == "Employment Report"]
    assert len(emp_cards) == 1
    assert emp_cards[0]["signal_tier"] == "major"
    assert emp_cards[0]["signal_reason"] == "Monthly payroll release"
    assert emp_cards[0]["event_count"] == 2


def test_treasury_auction_duplicate_from_both_sources_collapses(monkeypatch):
    _seed_aug_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "events": [
                _make_aug_econ(id="t1", title="10-Year Treasury Auction",
                               eventName="10-Year Treasury Auction", date="2026-08-05",
                               event_family="treasury_auction", signal_tier="major",
                               signal_reason="Treasury auction"),
            ],
            "current_week": [], "previous_week": [],
            "last_updated": "2026-08-02T10:00:00Z", "status": "ready",
        },
        "treasury_macro": {
            "events": [
                {"eventType": "treasury_rate", "indicatorName": "10-Year Treasury Auction",
                 "date": "2026-08-05", "country": "US", "importance": "high"},
            ],
            "current_week": [], "previous_week": [],
            "last_updated": "2026-08-02T10:00:00Z", "status": "ready",
        },
    })
    env = get_top_catalysts(today=date(2026, 8, 2))
    auction_cards = [m for d in env["days"] for m in d["macro"]
                     if m.get("macroType") == "Treasury Auctions"]
    assert len(auction_cards) == 1


def test_no_provider_call_at_request_time_with_aug_window(monkeypatch):
    import httpx

    def boom(*a, **k):
        raise AssertionError("network call attempted at request time")

    _seed_aug_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "events": [
                _make_aug_econ(id="c1", title="CPI MoM", eventName="CPI MoM",
                               event_family="cpi", signal_tier="major", date="2026-08-05"),
            ],
            "current_week": [], "previous_week": [],
            "last_updated": "2026-08-02T10:00:00Z", "status": "ready",
        },
    })
    monkeypatch.setattr(httpx, "AsyncClient", boom)
    monkeypatch.setattr(httpx, "Client", boom)
    env = get_top_catalysts(today=date(2026, 8, 2))
    assert env["week"] == "2026-08-03/2026-08-07"
    assert any(m.get("macroType") == "CPI" for d in env["days"] for m in d["macro"])


def test_response_includes_window_and_macro_counts(monkeypatch):
    _seed_aug_week(monkeypatch)
    _seed_watchlist(monkeypatch, set())
    _seed_options(monkeypatch, {})
    _seed_sectors(monkeypatch, {})
    _clear_earnings_cache()
    _seed_snapshots(monkeypatch, {
        "economic_releases": {
            "events": [
                _make_aug_econ(id="c1", title="CPI MoM", eventName="CPI MoM",
                               event_family="cpi", signal_tier="major", date="2026-08-05"),
                _make_aug_econ(id="c2", title="Core CPI MoM", eventName="Core CPI MoM",
                               event_family="cpi", signal_tier="major", date="2026-08-05"),
            ],
            "current_week": [], "previous_week": [],
            "last_updated": "2026-08-02T10:00:00Z", "status": "ready",
        },
    })
    env = get_top_catalysts(today=date(2026, 8, 2))
    assert env["week_start"] == "2026-08-03"
    assert env["week_end"] == "2026-08-07"
    assert env["window_mode"] == "next_week_planning"
    assert env["macro_source_event_count"] == 2
    assert env["macro_logical_event_count"] == 1


def test_get_top_catalysts_uses_precomputed_macro_window(monkeypatch):
    """Passing macro_window avoids recomputing the canonical pipeline."""
    _seed_snapshots(monkeypatch, {
        "ipos": {"current_week": [], "previous_week": [], "last_updated": None,
                 "status": "empty"},
        "dividends": {"current_week": [], "previous_week": [], "last_updated": None,
                      "status": "empty"},
        "splits": {"current_week": [], "previous_week": [], "last_updated": None,
                   "status": "empty"},
    })
    _seed_watchlist(monkeypatch, set(), set())
    cache_key = "earnings:curated:week:2026-08-03:2026-08-07"
    cache.set(cache_key, {"topEvents": [], "asOf": "2026-08-02T10:00:00Z"}, 60)

    calls: list[tuple[str, str]] = []

    def _fake_get_canonical_macro_window(*args, **kwargs):
        calls.append((args[0], args[1]))
        return {
            "window_start": "2026-08-03",
            "window_end": "2026-08-07",
            "macro_logical_events": [
                {"date": "2026-08-05", "eventName": "CPI MoM", "country": "US",
                 "event_family": "cpi", "signal_tier": "major"},
            ],
            "economic_logical_events": [
                {"date": "2026-08-05", "eventName": "CPI MoM", "country": "US",
                 "event_family": "cpi", "signal_tier": "major"},
            ],
            "treasury_logical_events": [],
            "source_counts": {"economic_source": 1, "treasury_source": 0,
                              "economic_logical": 1, "treasury_logical": 0},
            "last_updated": "2026-08-02T10:00:00Z",
            "coverage_complete": True,
            "horizon_start": "2026-08-01",
            "horizon_end": "2026-08-31",
        }

    monkeypatch.setattr(
        top_svc, "get_canonical_macro_window", _fake_get_canonical_macro_window,
    )

    precomputed = {
        "window_start": "2026-08-03",
        "window_end": "2026-08-07",
        "macro_logical_events": [
            {"date": "2026-08-04", "eventName": "PPI MoM", "country": "US",
             "event_family": "ppi", "signal_tier": "major"},
        ],
        "economic_logical_events": [
            {"date": "2026-08-04", "eventName": "PPI MoM", "country": "US",
             "event_family": "ppi", "signal_tier": "major"},
        ],
        "treasury_logical_events": [],
        "source_counts": {"economic_source": 1, "treasury_source": 0,
                          "economic_logical": 1, "treasury_logical": 0},
        "last_updated": "2026-08-02T09:00:00Z",
        "coverage_complete": True,
        "horizon_start": "2026-08-01",
        "horizon_end": "2026-08-31",
    }

    env = get_top_catalysts(today=date(2026, 8, 2), macro_window=precomputed)
    assert calls == []  # canonical pipeline was not rerun
    assert env["macro_source_event_count"] == 1
    assert env["macro_logical_event_count"] == 1
    # 2026-08-04 is Tuesday -> days[1]
    assert env["days"][1]["macro"][0]["title"] == "PPI MoM"


def test_calendar_top_internal_provider_gap_reports_coverage_gap(monkeypatch):
    """Calendar Top uses authoritative snapshot coverage-range semantics."""
    _seed_snapshots(monkeypatch, {
        "ipos": {"current_week": [], "previous_week": [], "last_updated": None,
                 "status": "empty"},
        "dividends": {"current_week": [], "previous_week": [], "last_updated": None,
                      "status": "empty"},
        "splits": {"current_week": [], "previous_week": [], "last_updated": None,
                   "status": "empty"},
    })
    _seed_watchlist(monkeypatch, set(), set())
    cache_key = "earnings:curated:week:2026-08-03:2026-08-07"
    cache.set(cache_key, {"topEvents": [], "asOf": "2026-08-02T10:00:00Z"}, 60)

    from services import calendar_snapshot_service as _snap_svc

    def _fake_get_snapshot(tab: str):
        if tab == "economic_releases":
            return {
                "events": [],
                "current_week": [],
                "previous_week": [],
                "last_updated": "2026-08-02T10:00:00Z",
                "status": "ready",
                "horizon": {
                    "horizon_start": "2026-07-18",
                    "horizon_end": "2026-10-29",
                    "coverage_ranges": [
                        {"from": "2026-07-18", "to": "2026-08-04", "status": "complete"},
                        {"from": "2026-08-06", "to": "2026-10-29", "status": "complete"},
                    ],
                },
                "coverage": {"complete": False},
            }
        if tab == "treasury_macro":
            return {"events": [], "current_week": [], "previous_week": [],
                    "last_updated": None, "status": "empty"}
        return {"current_week": [], "previous_week": [], "last_updated": None,
                "status": "empty"}

    monkeypatch.setattr(_snap_svc, "get_snapshot", _fake_get_snapshot)

    from services.calendar_curation import get_canonical_macro_window as real_gcmw
    captured: list[dict] = []

    def _capture_gcmw(*args, **kwargs):
        captured.append(kwargs)
        return real_gcmw(*args, **kwargs)

    monkeypatch.setattr(top_svc, "get_canonical_macro_window", _capture_gcmw)

    env = get_top_catalysts(today=date(2026, 8, 2))
    assert len(captured) == 1
    passed_envelope = captured[0].get("economic_envelope") or {}
    assert passed_envelope.get("empty_reason") == "coverage_gap"
    assert passed_envelope.get("coverage_complete") is False
    assert env["macro_source_event_count"] == 0
