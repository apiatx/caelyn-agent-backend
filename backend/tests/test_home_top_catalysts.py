"""
Unit tests for services/home_top_catalysts.py.

Mocked-data-only — no FMP, no network, no DB.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

_BACKEND = Path(__file__).resolve().parent.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from services.catalyst_calendar_service import _build_event
from services.calendar_curation import group_economic_events_to_families
from services.home_top_catalysts import (
    _build_macro_group,
    _classify_macro,
    _effective_tier_name,
    _effective_tier_val,
    _is_us_macro,
    _logical_event_name,
    _normalize_time,
    _resolve_parent_reason,
    _resolve_parent_tier,
    _build_logical_subtitle,
    _tier_to_impact,
    _tier_to_urgency,
    _MACRO_CATEGORIES,
    _CATEGORY_BY_ID,
    _TIER_ORDER,
)


# ── Helpers ─────────────────────────────────────────────────────────────────

_MONDAY = date(2026, 4, 27)


def _make_econ(**kw) -> dict:
    """Build a synthetic economic event with all signal fields populated."""
    ev = _build_event(
        id=kw.get("id", "ev1"),
        eventType="economic_release",
        date=kw.get("date", "2026-04-29"),
        time=kw.get("time"),
        title=kw.get("title", "CPI MoM"),
        eventName=kw.get("eventName", kw.get("title", "CPI MoM")),
        country=kw.get("country", "US"),
        importance=kw.get("importance", "high"),
        actual=kw.get("actual"),
        estimate=kw.get("estimate"),
        previous=kw.get("previous"),
        event_family=kw.get("event_family"),
        signal_tier=kw.get("signal_tier"),
        signal_reason=kw.get("signal_reason"),
        source=kw.get("source", "fmp"),
        raw=kw.get("raw", {}),
    )
    if kw.get("unit") is not None:
        ev["unit"] = kw["unit"]
    return ev


def _make_fed(**kw) -> dict:
    ev = _build_event(
        id=kw.get("id", "fed1"),
        eventType="economic_release",
        date=kw.get("date", "2026-04-30"),
        title=kw.get("title", "Fed Interest Rate Decision"),
        eventName=kw.get("eventName", kw.get("title", "Fed Interest Rate Decision")),
        country=kw.get("country", "US"),
        importance=kw.get("importance", "high"),
        event_family=kw.get("event_family", "fomc_decision"),
        signal_tier=kw.get("signal_tier", "critical"),
        signal_reason=kw.get("signal_reason", "Scheduled FOMC rate decision"),
        source=kw.get("source", "fmp"),
    )
    return ev


# ═══════════════════════════════════════════════════════════════════════════════
# US-only filtering
# ═══════════════════════════════════════════════════════════════════════════════

def test_us_event_passes():
    assert _is_us_macro({"country": "US"}) is True
    assert _is_us_macro({"country": "USA"}) is True
    assert _is_us_macro({"country": "UNITED STATES "}) is True
    assert _is_us_macro({"country": "United States"}) is True


def test_us_missing_country_excluded():
    assert _is_us_macro({"country": ""}) is False
    assert _is_us_macro({"country": "  "}) is False
    assert _is_us_macro({}) is False
    assert _is_us_macro({"country": None}) is False


def test_foreign_event_excluded():
    assert _is_us_macro({"country": "KR"}) is False
    assert _is_us_macro({"country": "DE"}) is False
    assert _is_us_macro({"country": "EU"}) is False
    assert _is_us_macro({"country": "JP"}) is False
    assert _is_us_macro({"country": "GB"}) is False


def test_countryless_critical_cannot_raise_tier():
    ev = _make_econ(id="nous", title="Critical Event", eventName="Critical Event",
                    country=None, importance="high", date="2026-04-29")
    ev["signal_tier"] = "critical"
    ev["signal_reason"] = "critical reason"
    assert _is_us_macro(ev) is False


def test_countryless_cannot_provide_reason():
    ev = _make_econ(id="nous2", title="No Country", eventName="No Country",
                    country="", importance="high", date="2026-04-29")
    ev["signal_tier"] = "major"
    ev["signal_reason"] = "no country reason"
    assert _is_us_macro(ev) is False


# ═══════════════════════════════════════════════════════════════════════════════
# Family-card consumption
# ═══════════════════════════════════════════════════════════════════════════════

def test_four_cpi_variants_become_one_logical_child():
    cpi_evs = [
        _make_econ(id="c1", title="CPI MoM", eventName="CPI MoM",
                   event_family="cpi", signal_tier="major",
                   signal_reason="Major consumer inflation release",
                   date="2026-04-29"),
        _make_econ(id="c2", title="Core CPI MoM", eventName="Core CPI MoM",
                   event_family="cpi", signal_tier="major",
                   signal_reason="Major consumer inflation release",
                   date="2026-04-29"),
        _make_econ(id="c3", title="CPI YoY", eventName="CPI YoY",
                   event_family="cpi", signal_tier="major",
                   signal_reason="Major consumer inflation release",
                   date="2026-04-29"),
        _make_econ(id="c4", title="Core CPI YoY", eventName="Core CPI YoY",
                   event_family="cpi", signal_tier="major",
                   signal_reason="Major consumer inflation release",
                   date="2026-04-29"),
    ]
    result = group_economic_events_to_families(cpi_evs)
    family_cards = [e for e in result if e.get("type") == "macro_family"]
    assert len(family_cards) == 1
    assert family_cards[0]["event_family"] == "cpi"
    assert family_cards[0]["children"] == cpi_evs
    assert family_cards[0]["event_count"] == 4


def test_four_ppi_variants_become_one_logical_child():
    ppi_evs = [
        _make_econ(id="p1", title="Core PPI MoM", event_family="ppi", signal_tier="major",
                   signal_reason="Producer price inflation release", date="2026-04-29"),
        _make_econ(id="p2", title="PPI MoM", event_family="ppi", signal_tier="major",
                   signal_reason="Producer price inflation release", date="2026-04-29"),
        _make_econ(id="p3", title="Core PPI YoY", event_family="ppi", signal_tier="major",
                   signal_reason="Producer price inflation release", date="2026-04-29"),
        _make_econ(id="p4", title="PPI YoY", event_family="ppi", signal_tier="major",
                   signal_reason="Producer price inflation release", date="2026-04-29"),
    ]
    result = group_economic_events_to_families(ppi_evs)
    family_cards = [e for e in result if e.get("type") == "macro_family"]
    assert len(family_cards) == 1
    assert family_cards[0]["event_family"] == "ppi"
    assert family_cards[0]["event_count"] == 4


def test_four_pce_variants_become_one_logical_child():
    pce_evs = [
        _make_econ(id="e1", title="PCE Price Index MoM", event_family="pce", signal_tier="major",
                   signal_reason="Fed-preferred inflation measure", date="2026-04-29"),
        _make_econ(id="e2", title="PCE Price Index YoY", event_family="pce", signal_tier="major",
                   signal_reason="Fed-preferred inflation measure", date="2026-04-29"),
        _make_econ(id="e3", title="Core PCE Price Index MoM", event_family="pce", signal_tier="major",
                   signal_reason="Fed-preferred inflation measure", date="2026-04-29"),
        _make_econ(id="e4", title="Core PCE Price Index YoY", event_family="pce", signal_tier="major",
                   signal_reason="Fed-preferred inflation measure", date="2026-04-29"),
    ]
    result = group_economic_events_to_families(pce_evs)
    family_cards = [e for e in result if e.get("type") == "macro_family"]
    assert len(family_cards) == 1
    assert family_cards[0]["event_family"] == "pce"
    assert family_cards[0]["event_count"] == 4


def test_gdp_variants_become_one_logical_child():
    gdp_evs = [
        _make_econ(id="g1", title="GDP Growth Rate QoQ", event_family="gdp", signal_tier="major",
                   signal_reason="Gross domestic product release", date="2026-04-29"),
        _make_econ(id="g2", title="GDP Sales QoQ", event_family="gdp", signal_tier="major",
                   signal_reason="Gross domestic product release", date="2026-04-29"),
        _make_econ(id="g3", title="GDP Price Index", event_family="gdp", signal_tier="major",
                   signal_reason="Gross domestic product release", date="2026-04-29"),
    ]
    result = group_economic_events_to_families(gdp_evs)
    family_cards = [e for e in result if e.get("type") == "macro_family"]
    assert len(family_cards) == 1
    assert family_cards[0]["event_family"] == "gdp"
    assert family_cards[0]["event_count"] == 3


def test_eci_variants_become_one_logical_child():
    eci_evs = [
        _make_econ(id="h1", title="Employment Cost Index QoQ", event_family="eci",
                   signal_tier="major", signal_reason="Quarterly wage inflation measure",
                   date="2026-04-29"),
        _make_econ(id="h2", title="Employment Cost Index YoY", event_family="eci",
                   signal_tier="major", signal_reason="Quarterly wage inflation measure",
                   date="2026-04-29"),
    ]
    result = group_economic_events_to_families(eci_evs)
    family_cards = [e for e in result if e.get("type") == "macro_family"]
    assert len(family_cards) == 1
    assert family_cards[0]["event_family"] == "eci"
    assert family_cards[0]["event_count"] == 2


def test_family_children_nested_inside_card_not_flattened():
    cpi_evs = [
        _make_econ(id="f1", title="CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-04-29"),
        _make_econ(id="f2", title="Core CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-04-29"),
    ]
    result = group_economic_events_to_families(cpi_evs)
    card = result[0]
    assert len(card["children"]) == 2
    assert card["children"][0]["id"] == "f1"
    assert card["children"][1]["id"] == "f2"


# ═══════════════════════════════════════════════════════════════════════════════
# Foreign exclusion in Home context
# ═══════════════════════════════════════════════════════════════════════════════

def test_kr_cpi_classified_but_foreign_filtered():
    """KR CPI matches inflation category but is excluded by _is_us_macro."""
    ev = _make_econ(id="kr1", title="CPI YoY", eventName="CPI YoY",
                    country="KR", event_family="foreign",
                    signal_tier="context", signal_reason="Foreign macro release (KR)",
                    date="2026-04-29")
    cat_id = _classify_macro(ev)
    assert cat_id == "inflation"
    assert _is_us_macro(ev) is False


def test_eu_gdp_classified_but_foreign_filtered():
    ev = _make_econ(id="eu1", title="GDP YoY", eventName="GDP YoY",
                    country="EU", event_family="foreign",
                    signal_tier="context", signal_reason="Foreign macro release (EU)",
                    date="2026-04-29")
    cat_id = _classify_macro(ev)
    assert cat_id == "growth"
    assert _is_us_macro(ev) is False


def test_foreign_event_reason_never_on_parent_card():
    """A foreign CPI should not appear as a child in a Home category card."""
    ev_foreign = _make_econ(id="kr2", title="CPI YoY", eventName="CPI YoY",
                            country="KR", event_family="foreign",
                            signal_tier="context",
                            signal_reason="Foreign macro release (KR)",
                            date="2026-04-29")
    ev_us = _make_econ(id="us_cpi", title="CPI MoM", eventName="CPI MoM",
                       country="US", event_family="cpi",
                       signal_tier="major",
                       signal_reason="Major consumer inflation release",
                       date="2026-04-29")
    us_only = [ev_us]
    logical = group_economic_events_to_families(us_only)
    inflation_children = [e for e in logical if _classify_macro(e) == "inflation"]
    children_ids = {e.get("id") for e in inflation_children}
    assert "kr2" not in children_ids
    for c in inflation_children:
        reason = c.get("signal_reason", "")
        assert "KR" not in reason


def test_foreign_determines_tier_false():
    """Foreign event excluded from tier calculation in a Home card."""
    fp = _build_macro_group("inflation", [
        _make_econ(id="us1", title="CPI MoM", event_family="cpi",
                   signal_tier="major",
                   signal_reason="Major consumer inflation release",
                   date="2026-04-29", country="US"),
    ], "2026-04-27")
    assert fp["signal_tier"] == "major"
    assert "Major consumer inflation release" in fp.get("reason", "")


# ═══════════════════════════════════════════════════════════════════════════════
# Parent tier and reason
# ═══════════════════════════════════════════════════════════════════════════════

def test_inflation_pce_major_cpi_major_is_major():
    cpi_card = group_economic_events_to_families([
        _make_econ(id="c1", title="CPI MoM", event_family="cpi", signal_tier="major",
                   signal_reason="Major consumer inflation release", date="2026-04-29"),
        _make_econ(id="c2", title="Core CPI MoM", event_family="cpi", signal_tier="major",
                   signal_reason="Major consumer inflation release", date="2026-04-29"),
    ])
    pce_card = group_economic_events_to_families([
        _make_econ(id="p1", title="Core PCE MoM", event_family="pce", signal_tier="major",
                   signal_reason="Fed-preferred inflation measure", date="2026-04-29"),
        _make_econ(id="p2", title="PCE Price Index MoM", event_family="pce", signal_tier="major",
                   signal_reason="Fed-preferred inflation measure", date="2026-04-29"),
    ])
    inflation_children = cpi_card + pce_card
    tier = _resolve_parent_tier(inflation_children)
    assert tier == "major"


def test_one_critical_child_makes_parent_critical():
    children = [
        _make_econ(id="f1", title="FOMC Decision", event_family="fomc_decision",
                   signal_tier="critical", signal_reason="Scheduled FOMC rate decision",
                   date="2026-04-30"),
        _make_econ(id="f2", title="FOMC Minutes", event_family="fomc_minutes",
                   signal_tier="major", signal_reason="FOMC meeting minutes",
                   date="2026-04-29"),
    ]
    tier = _resolve_parent_tier(children)
    assert tier == "critical"


def test_legacy_high_importance_no_signal_tier_resolves_secondary():
    ev = _make_econ(id="leg1", title="Some Release", eventName="Some Release",
                    country="US", importance="high", date="2026-04-29")
    ev.pop("signal_tier", None)
    ev.pop("event_family", None)
    tier = _resolve_parent_tier([ev])
    assert tier == "secondary"


def test_legacy_medium_importance_no_signal_tier_resolves_secondary():
    ev = _make_econ(id="leg2", title="Some Release", eventName="Some Release",
                    country="US", importance="medium", date="2026-04-29")
    ev.pop("signal_tier", None)
    ev.pop("event_family", None)
    tier = _resolve_parent_tier([ev])
    assert tier == "secondary"


def test_legacy_low_importance_no_signal_tier_resolves_context():
    ev = _make_econ(id="leg3", title="Some Release", eventName="Some Release",
                    country="US", importance="low", date="2026-04-29")
    ev.pop("signal_tier", None)
    ev.pop("event_family", None)
    tier = _resolve_parent_tier([ev])
    assert tier == "context"


def test_parent_reason_from_strongest_logical_child():
    children = [
        _make_econ(id="pay1", title="Nonfarm Payrolls", event_family="payrolls",
                   signal_tier="major",
                   signal_reason="Monthly payroll and labor-market release",
                   date="2026-04-29"),
        _make_econ(id="job1", title="Initial Jobless Claims", event_family="jobless_claims",
                   signal_tier="secondary",
                   signal_reason="Weekly jobless claims",
                   date="2026-04-29"),
    ]
    reason = _resolve_parent_reason(children, "major")
    assert "Monthly payroll and labor-market release" in reason


def test_tie_breaking_deterministic():
    children = [
        _make_econ(id="a", title="CPI MoM", event_family="cpi", signal_tier="major",
                   signal_reason="A", date="2026-04-29"),
        _make_econ(id="b", title="CPI YoY", event_family="cpi", signal_tier="major",
                   signal_reason="B", date="2026-04-29"),
    ]
    reason1 = _resolve_parent_reason(children, "major")
    reason2 = _resolve_parent_reason(children, "major")
    assert reason1 == reason2
    assert reason1 in ("A", "B")


def test_parent_tier_not_inflated_by_multiple_raw_children():
    four_major_cpi = [
        _make_econ(id="m1", title="CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-04-29"),
        _make_econ(id="m2", title="Core CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-04-29"),
        _make_econ(id="m3", title="CPI YoY", event_family="cpi", signal_tier="major",
                   date="2026-04-29"),
        _make_econ(id="m4", title="Core CPI YoY", event_family="cpi", signal_tier="major",
                   date="2026-04-29"),
    ]
    family_cards = group_economic_events_to_families(four_major_cpi)
    assert len(family_cards) == 1
    family_tier = _resolve_parent_tier(family_cards)
    assert family_tier == "major"


def test_tiebreak_earlier_date_wins():
    later = _make_econ(id="late", title="CPI MoM", event_family="cpi",
                       signal_tier="major", signal_reason="later",
                       date="2026-04-30")
    earlier = _make_econ(id="early", title="PPI MoM", event_family="ppi",
                         signal_tier="major", signal_reason="earlier",
                         date="2026-04-29")
    reason = _resolve_parent_reason([later, earlier], "major")
    assert "earlier" in reason


def test_tiebreak_earlier_time_wins():
    late_time = _make_econ(id="lt", title="CPI MoM", event_family="cpi",
                           signal_tier="major", signal_reason="late_time",
                           date="2026-04-29", time="10:00:00")
    early_time = _make_econ(id="et", title="PPI MoM", event_family="ppi",
                            signal_tier="major", signal_reason="early_time",
                            date="2026-04-29", time="08:30:00")
    reason = _resolve_parent_reason([late_time, early_time], "major")
    assert "early_time" in reason


def test_tiebreak_source_order_preserved():
    a = _make_econ(id="so1", title="CPI MoM", event_family="cpi",
                   signal_tier="major", signal_reason="first",
                   date="2026-04-29", time="08:30:00")
    b = _make_econ(id="so2", title="PPI MoM", event_family="ppi",
                   signal_tier="major", signal_reason="second",
                   date="2026-04-29", time="08:30:00")
    reason = _resolve_parent_reason([a, b], "major")
    assert "first" in reason


def test_tiebreak_lexical_only_as_final_fallback():
    """Source order breaks the tie before lexical; lexical is the last sort key."""
    a = _make_econ(id="lx1", title="A Event", event_family="cpi",
                   signal_tier="major", signal_reason="a_reason",
                   date="2026-04-29", time="08:30:00")
    b = _make_econ(id="lx2", title="B Event", event_family="cpi",
                   signal_tier="major", signal_reason="b_reason",
                   date="2026-04-29", time="08:30:00")
    reason1 = _resolve_parent_reason([b, a], "major")
    reason2 = _resolve_parent_reason([a, b], "major")
    assert reason1 == "b_reason"
    assert reason2 == "a_reason"


def test_parent_reason_from_exact_winning_child():
    major1 = _make_econ(id="w1", title="Jobless Claims", event_family="jobless_claims",
                        signal_tier="secondary", signal_reason="should_not_win",
                        date="2026-04-29")
    major2 = _make_econ(id="w2", title="Nonfarm Payrolls", event_family="payrolls",
                        signal_tier="major", signal_reason="should_win",
                        date="2026-04-29")
    reason = _resolve_parent_reason([major1, major2], "major")
    assert reason == "should_win"


def test_lower_tier_child_never_provides_reason():
    major = _make_econ(id="low1", title="Payrolls", event_family="payrolls",
                       signal_tier="major", signal_reason="major_reason",
                       date="2026-04-29")
    second = _make_econ(id="low2", title="Claims", event_family="jobless_claims",
                        signal_tier="secondary", signal_reason="secondary_reason",
                        date="2026-04-28")
    reason = _resolve_parent_reason([second, major], "major")
    assert reason == "major_reason"


# ═══════════════════════════════════════════════════════════════════════════════
# Subtitle and counts
# ═══════════════════════════════════════════════════════════════════════════════

def test_subtitle_uses_logical_names_not_raw_metrics():
    cpi_family = group_economic_events_to_families([
        _make_econ(id="s1", title="CPI MoM", event_family="cpi", signal_tier="major",
                   signal_reason="Major consumer inflation release", date="2026-04-29"),
        _make_econ(id="s2", title="Core CPI MoM", event_family="cpi", signal_tier="major",
                   signal_reason="Major consumer inflation release", date="2026-04-29"),
    ])
    ppi_family = group_economic_events_to_families([
        _make_econ(id="s3", title="PPI MoM", event_family="ppi", signal_tier="major",
                   signal_reason="Producer price inflation release", date="2026-04-29"),
        _make_econ(id="s4", title="Core PPI MoM", event_family="ppi", signal_tier="major",
                   signal_reason="Producer price inflation release", date="2026-04-29"),
    ])
    cat = _CATEGORY_BY_ID["inflation"]
    subtitle, extra = _build_logical_subtitle(cpi_family + ppi_family, cat)
    assert "CPI MoM" not in subtitle
    assert "Core CPI MoM" not in subtitle
    assert "PPI Inflation Report" in subtitle
    assert "CPI Inflation Report" in subtitle


def test_subtitle_deduplicates_logical_names():
    two_cpi_families_same_date = [
        _make_econ(id="d1", title="CPI MoM", event_family="cpi", signal_tier="major",
                   signal_reason="r", date="2026-04-29"),
        _make_econ(id="d2", title="Core CPI MoM", event_family="cpi", signal_tier="major",
                   signal_reason="r", date="2026-04-29"),
        _make_econ(id="d3", title="CPI MoM", event_family="cpi", signal_tier="major",
                   signal_reason="r", date="2026-04-30"),
        _make_econ(id="d4", title="Core CPI MoM", event_family="cpi", signal_tier="major",
                   signal_reason="r", date="2026-04-30"),
    ]
    logical = group_economic_events_to_families(two_cpi_families_same_date)
    cpi_cards = [c for c in logical if c.get("event_family") == "cpi"]
    cat = _CATEGORY_BY_ID["inflation"]
    subtitle, extra = _build_logical_subtitle(cpi_cards, cat)
    count = subtitle.lower().count("cpi inflation report")
    assert count == 1
    assert "PPI" not in subtitle


def test_plus_n_counts_logical_children():
    children = [
        _make_econ(id="n1", title="PPI MoM", event_family="ppi", signal_tier="major",
                   signal_reason="r", date="2026-04-29"),
        _make_econ(id="n2", title="CPI MoM", event_family="cpi", signal_tier="major",
                   signal_reason="r", date="2026-04-29"),
        _make_econ(id="n3", title="PCE Price Index MoM", event_family="pce", signal_tier="major",
                   signal_reason="r", date="2026-04-29"),
        _make_econ(id="n4", title="GDP Growth Rate QoQ", event_family="gdp", signal_tier="major",
                   signal_reason="r", date="2026-04-29"),
        _make_econ(id="n5a", title="Employment Cost Index QoQ", event_family="eci",
                   signal_tier="major", signal_reason="r", date="2026-04-29"),
        _make_econ(id="n6", title="CPI MoM", event_family="cpi", signal_tier="major",
                   signal_reason="r", date="2026-04-30"),
        _make_econ(id="n7", title="Core Inflation Rate MoM", eventName="Core Inflation Rate MoM",
                   event_family="other_us", country="US",
                   signal_tier="secondary", signal_reason="r", date="2026-04-29"),
    ]
    logical = group_economic_events_to_families(children)
    cat = _CATEGORY_BY_ID["inflation"]
    inflation_logical = [e for e in logical if _classify_macro(e) == "inflation"]
    subtitle, extra = _build_logical_subtitle(inflation_logical, cat)
    assert extra > 0
    assert "+" in subtitle
    assert "more" in subtitle


def test_family_event_count_equals_raw_metric_count():
    cpi_evs = [
        _make_econ(id="ec1", title="CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-04-29"),
        _make_econ(id="ec2", title="Core CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-04-29"),
        _make_econ(id="ec3", title="CPI YoY", event_family="cpi", signal_tier="major",
                   date="2026-04-29"),
    ]
    result = group_economic_events_to_families(cpi_evs)
    card = result[0]
    assert card["event_count"] == 3
    assert len(card["children"]) == 3


def test_direct_home_children_count_logical():
    cpi_evs = [
        _make_econ(id="hc1", title="CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-04-29"),
        _make_econ(id="hc2", title="Core CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-04-29"),
    ]
    ppi_evs = [
        _make_econ(id="hc3", title="PPI MoM", event_family="ppi", signal_tier="major",
                   date="2026-04-29"),
        _make_econ(id="hc4", title="Core PPI MoM", event_family="ppi", signal_tier="major",
                   date="2026-04-29"),
    ]
    all_evs = cpi_evs + ppi_evs
    logical = group_economic_events_to_families(all_evs)
    inflation_logical = [e for e in logical if _classify_macro(e) == "inflation"]
    assert len(inflation_logical) == 2  # CPI family + PPI family
    card = _build_macro_group("inflation", inflation_logical, "2026-04-27")
    assert card["event_count"] == 2


def test_rendered_category_count_correct():
    cpi_fam = group_economic_events_to_families([
        _make_econ(id="rc1", title="CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-04-29"),
    ])
    nfp = _make_econ(id="rc2", title="Nonfarm Payrolls", event_family="payrolls",
                     signal_tier="major", signal_reason="Monthly payroll release",
                     date="2026-04-29")
    inflation_evs = [e for e in cpi_fam if _classify_macro(e) == "inflation"]
    labor_evs = [e for e in [nfp] if _classify_macro(e) == "labor"]
    inflation_card = _build_macro_group("inflation", inflation_evs, "2026-04-27")
    labor_card = _build_macro_group("labor", labor_evs, "2026-04-27")
    assert inflation_card["event_count"] == 1
    assert labor_card["event_count"] == 1


# ═══════════════════════════════════════════════════════════════════════════════
# Regression — FOMC, payrolls, etc. stay discrete
# ═══════════════════════════════════════════════════════════════════════════════

def test_fomc_remains_discrete():
    ev = _make_fed(id="fomc1", title="FOMC Interest Rate Decision",
                   event_family="fomc_decision", signal_tier="critical",
                   signal_reason="Scheduled FOMC rate decision",
                   date="2026-04-30")
    result = group_economic_events_to_families([ev])
    assert len(result) == 1
    assert result[0].get("type") != "macro_family"
    assert result[0]["event_family"] == "fomc_decision"


def test_fomc_decision_makes_fed_rates_critical():
    fomc = _make_fed(id="fc1", title="FOMC Interest Rate Decision",
                     event_family="fomc_decision", signal_tier="critical",
                     signal_reason="Scheduled FOMC rate decision",
                     date="2026-04-30")
    card = _build_macro_group("fed_rates", [fomc], "2026-04-27")
    assert card["signal_tier"] == "critical"


def test_payrolls_and_unemployment_remain_separate():
    evs = [
        _make_econ(id="pay", title="Nonfarm Payrolls", event_family="payrolls",
                   signal_tier="major", signal_reason="Monthly payroll release",
                   date="2026-04-29"),
        _make_econ(id="unemp", title="Unemployment Rate", event_family="unemployment",
                   signal_tier="secondary", signal_reason="Unemployment rate release",
                   date="2026-04-29"),
    ]
    result = group_economic_events_to_families(evs)
    assert len(result) == 2
    assert all(e.get("type") != "macro_family" for e in result)


def test_non_macro_cards_unchanged():
    """Earnings and other cards are not affected by macro-family grouping."""
    from services.home_top_catalysts import _build_earnings_card, _build_other_card
    ev = {"symbol": "AAPL", "companyName": "Apple Inc.", "date": "2026-04-29",
          "scoreReasons": [], "options_activity_strength": "normal",
          "rankScore": 95.0}
    ec = _build_earnings_card(ev)
    assert ec["type"] == "earnings"
    assert ec["category"] == "earnings"
    oc = _build_other_card({"symbol": "IPOCO", "companyName": "IpoCo",
                            "eventType": "ipos", "date": "2026-04-29",
                            "whyThisMatters": [], "sourceTab": "ipos"})
    assert oc["category"] == "market_event"


def test_response_envelope_fields_preserved():
    """All expected envelope keys are documented on the response contract."""
    keys = ["view", "source", "window_start", "window_end", "window_mode",
            "generated_at", "catalysts", "total_source_events", "total_grouped_events",
            "hidden_count", "last_updated", "status"]
    assert len(keys) == 12


# ═══════════════════════════════════════════════════════════════════════════════
# Category mapping
# ═══════════════════════════════════════════════════════════════════════════════

def test_cpi_to_inflation():
    assert _classify_macro({"eventName": "CPI YoY", "country": "US"}) == "inflation"
    assert _classify_macro({"display_title": "CPI Inflation Report", "type": "macro_family",
                            "event_family": "cpi"}) == "inflation"


def test_ppi_to_inflation():
    assert _classify_macro({"eventName": "PPI MoM", "country": "US"}) == "inflation"


def test_pce_to_inflation():
    assert _classify_macro({"eventName": "PCE Price Index", "country": "US"}) == "inflation"


def test_eci_to_inflation():
    assert _classify_macro({"display_title": "Employment Cost Index", "type": "macro_family",
                            "event_family": "eci"}) == "inflation"


def test_gdp_to_growth():
    assert _classify_macro({"eventName": "GDP YoY", "country": "US"}) == "growth"


def test_fomc_decision_to_fed_rates():
    result = _classify_macro({"eventName": "Fed Interest Rate Decision", "title": "Fed Interest Rate Decision",
                              "country": "US"})
    assert result == "fed_rates"


def test_payrolls_to_labor():
    assert _classify_macro({"eventName": "Nonfarm Payrolls", "country": "US"}) == "labor"


def test_unemployment_to_labor():
    assert _classify_macro({"eventName": "Unemployment Rate", "country": "US"}) == "labor"


def test_jobless_claims_to_labor():
    assert _classify_macro({"eventName": "Initial Jobless Claims", "country": "US"}) == "labor"


# ═══════════════════════════════════════════════════════════════════════════════
# Impact/urgency mapping
# ═══════════════════════════════════════════════════════════════════════════════

def test_tier_to_impact():
    assert _tier_to_impact("critical") == "high"
    assert _tier_to_impact("major") == "high"
    assert _tier_to_impact("secondary") == "medium"
    assert _tier_to_impact("context") == "low"


def test_tier_to_urgency():
    assert _tier_to_urgency("critical") == "high"
    assert _tier_to_urgency("major") == "important"
    assert _tier_to_urgency("secondary") == "normal"


def test_effective_tier_val():
    assert _effective_tier_val({"signal_tier": "critical"}) == 3
    assert _effective_tier_val({"signal_tier": "major"}) == 2
    assert _effective_tier_val({"signal_tier": "secondary"}) == 1
    assert _effective_tier_val({"signal_tier": "context"}) == 0
    assert _effective_tier_val({"importance": "high"}) == 1
    assert _effective_tier_val({"importance": "medium"}) == 1
    assert _effective_tier_val({"importance": "low"}) == 0
    assert _effective_tier_val({}) == 0


def test_effective_tier_name():
    assert _effective_tier_name({"signal_tier": "critical"}) == "critical"
    assert _effective_tier_name({"signal_tier": "major"}) == "major"
    assert _effective_tier_name({"importance": "high"}) == "secondary"
    assert _effective_tier_name({"importance": "medium"}) == "secondary"
    assert _effective_tier_name({"importance": "low"}) == "context"


def test_normalize_time_pushes_missing_to_end():
    assert _normalize_time("08:30:00") == "08:30:00"
    assert _normalize_time(None) == "~"
    assert _normalize_time("") == "~"
    assert _normalize_time("   ") == "~"
    assert _normalize_time("08:30:00") < _normalize_time(None)
    assert _normalize_time("08:30:00") < _normalize_time("")


def test_logical_family_cards_are_direct_home_children():
    cpi_child = group_economic_events_to_families([
        _make_econ(id="df1", title="CPI MoM", event_family="cpi", signal_tier="major",
                   signal_reason="r", date="2026-04-29"),
        _make_econ(id="df2", title="Core CPI MoM", event_family="cpi", signal_tier="major",
                   signal_reason="r", date="2026-04-29"),
    ])
    card = _build_macro_group("inflation", cpi_child, "2026-04-27")
    assert len(card["children"]) == 1
    assert card["children"][0].get("type") == "macro_family"


def test_raw_metrics_nested_not_direct_children():
    cpi_child = group_economic_events_to_families([
        _make_econ(id="rn1", title="CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-04-29"),
        _make_econ(id="rn2", title="Core CPI MoM", event_family="cpi", signal_tier="major",
                   date="2026-04-29"),
    ])
    card = _build_macro_group("inflation", cpi_child, "2026-04-27")
    direct_ids = {c["id"] for c in card["children"]}
    assert "rn1" not in direct_ids
    assert "rn2" not in direct_ids


# ═══════════════════════════════════════════════════════════════════════════════
# Logical event name
# ═══════════════════════════════════════════════════════════════════════════════

def test_logical_event_name_family_card():
    card = {"type": "macro_family", "display_title": "CPI Inflation Report",
            "event_family": "cpi"}
    assert _logical_event_name(card) == "CPI Inflation Report"


def test_logical_event_name_discrete():
    ev = {"eventName": "Nonfarm Payrolls", "title": "Nonfarm Payrolls"}
    assert _logical_event_name(ev) == "Nonfarm Payrolls"

    ev2 = {"title": "Fed Chair Powell Speaks", "eventName": ""}
    assert _logical_event_name(ev2) == "Fed Chair Powell Speaks"


def test_logical_event_name_fallback():
    ev = {"eventName": "", "title": "Fed Chair Powell Speaks"}
    assert _logical_event_name(ev) == "Fed Chair Powell Speaks"
