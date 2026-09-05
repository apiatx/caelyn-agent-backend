"""
Home Compact Top Catalysts feed — GET /api/home/top-catalysts.

Reuses existing cached data sources only (zero external API calls):
  • top_catalysts_service.get_top_catalysts() — earnings + other (IPO/split/div)
  • services.calendar_curation.get_canonical_macro_window() — macro logical events

Weekend planning-window behavior (Sat/Sun):
  On Saturday/Sunday ET the planning window advances to the NEXT Mon–Fri week.
  The shared rolling-horizon snapshot already covers that week, so Home reads
  the same canonical macro window as Calendar Top Catalysts and Economic
  Releases.  No request-time provider refreshes.

The Calendar page's existing /api/catalysts/top endpoint is NOT modified.
This module applies its own grouping/ranking/limiting layer on top.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from services.calendar_curation import get_canonical_macro_window
from services.calendar_snapshot_service import (
    get_snapshot_async as _get_snapshot,
    get_snapshot_window_async as _get_snapshot_window,
)
from services.top_catalysts_service import resolve_top_catalysts_week


# ── Tier ordering (reused from calendar_curation; local copy for independence) ─
_TIER_ORDER: dict[str, int] = {"critical": 3, "major": 2, "secondary": 1, "context": 0}

# ── Macro category definitions (priority: high number = show first) ──────────

_MACRO_CATEGORIES: list[dict] = [
    {
        "id": "fed_rates",
        "title": "Fed / Rates",
        "reason": (
            "Fed policy signals directly drive rate expectations, "
            "credit spreads, and equity valuations."
        ),
        "priority": 10,
        "pat": re.compile(
            r"\b(?:fomc|federal\s+reserve|fed(?:\s+funds)?(?:\s+rate)?\s+decision|"
            r"interest\s+rate\s+decision|fed\s+chair|powell|fed\s+minutes|"
            r"rate\s+decision|central\s+bank)\b",
            re.I,
        ),
    },
    {
        "id": "inflation",
        "title": "Inflation Data",
        "reason": (
            "Inflation releases move yields, rate-cut expectations, "
            "and high-duration growth stocks."
        ),
        "priority": 9,
        "pat": re.compile(
            r"\b(?:cpi|consumer\s+price\s+index|ppi|producer\s+price\s+index|"
            r"pce|personal\s+consumption\s+expenditure|inflation\s+rate|"
            r"core\s+cpi|core\s+ppi|core\s+pce|deflator|price\s+index|"
            r"employment\s+cost\s+index|"
            r"brc\s+retail\s+sales\s+monitor)\b",
            re.I,
        ),
    },
    {
        "id": "labor",
        "title": "Labor Market Data",
        "reason": (
            "Employment data shapes Fed policy outlook and "
            "consumer spending expectations."
        ),
        "priority": 8,
        "pat": re.compile(
            r"(?:\bnfp\b|non[-\s]?farm\s+payroll|nonfarm\s+payroll|"
            r"\bunemployment(?:\s+rate)?\b|\bjobless\s+claims\b|initial\s+jobless|"
            r"continuing\s+claim|\bjolts\b|\badp\s+employment\b|"
            r"employment\s+change|payrolls?\s+report|labor\s+market|"
            r"claimant\s+count)",
            re.I,
        ),
    },
    {
        "id": "growth",
        "title": "Growth / Demand Data",
        "reason": (
            "Growth indicators signal economic expansion pace "
            "and sector rotation opportunities."
        ),
        "priority": 7,
        "pat": re.compile(
            r"\b(?:gdp|gross\s+domestic|gdpnow|atlanta\s+fed|niesr|"
            r"retail\s+sales|ism|pmi|purchasing\s+managers|"
            r"industrial\s+production|durable\s+goods|factory\s+orders|"
            r"trade\s+balance|business\s+inventories|services\s+pmi|"
            r"manufacturing\s+pmi|composite\s+pmi)\b",
            re.I,
        ),
    },
    {
        "id": "treasury",
        "title": "Treasury / Yields",
        "reason": (
            "Treasury auction and yield curve data affect "
            "borrowing costs and equity risk premiums."
        ),
        "priority": 6,
        "pat": re.compile(
            r"\b(?:treasury|yield\s+curve|t[-\s]?bond|t[-\s]?note|t[-\s]?bill|"
            r"bond\s+auction|\d+[ym]\s+(?:treasury|rate)|"
            r"treasury\s+yield|treasury\s+rate|treasury\s+snapshot)\b",
            re.I,
        ),
    },
    {
        "id": "consumer",
        "title": "Consumer Sentiment",
        "reason": (
            "Consumer confidence and inflation expectations "
            "anticipate spending trends and sector rotation."
        ),
        "priority": 5,
        "pat": re.compile(
            r"\b(?:consumer\s+sentiment|consumer\s+confidence|"
            r"inflation\s+expectations|university\s+of\s+michigan|"
            r"michigan\s+sentiment|consumer\s+expectations|"
            r"gfk\s+consumer)\b",
            re.I,
        ),
    },
    {
        "id": "housing",
        "title": "Housing Data",
        "reason": (
            "Housing data affects financials, materials, "
            "and consumer discretionary sectors."
        ),
        "priority": 4,
        "pat": re.compile(
            r"\b(?:building\s+permits|housing\s+starts|existing\s+home\s+sales|"
            r"new\s+home\s+sales|home\s+price|case[-\s]?shiller|nahb|"
            r"pending\s+home\s+sales|mortgage\s+applications)\b",
            re.I,
        ),
    },
]

_CATEGORY_BY_ID: dict[str, dict] = {c["id"]: c for c in _MACRO_CATEGORIES}
_WEEKDAY_ABBR = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

# Home compact limits
_MAX_MACRO_GROUPS  = 3
_MAX_EARNINGS      = 3
_MAX_OTHER         = 2
_MAX_TOTAL         = 8




# ── Planning window helper ────────────────────────────────────────────────────

def _planning_window(
    today_et: Optional[date] = None,
) -> tuple[date, date, str]:
    """
    Return (monday, friday, window_mode) for the Home Top Catalysts feed.

    Thin wrapper around the shared ``resolve_top_catalysts_week`` helper so
    Calendar and Home Top Catalysts can never drift.  Planning rules (ET):
      Mon–Fri  → current week's Mon–Fri   window_mode="current_week"
      Sat–Sun  → *next* week's Mon–Fri    window_mode="next_week_planning"
    """
    return resolve_top_catalysts_week(today_et)


# ── General helpers ───────────────────────────────────────────────────────────

def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _event_name(ev: dict) -> str:
    return (
        ev.get("display_title") or ev.get("title") or
        ev.get("eventName") or ev.get("indicatorName") or ""
    ).strip()


def _highest_impact(events: list[dict]) -> str:
    order = {"high": 3, "medium": 2, "low": 1}
    best = "low"
    for ev in events:
        imp = (ev.get("importance") or ev.get("impact") or "low").lower()
        if order.get(imp, 0) > order.get(best, 0):
            best = imp
    return best


def _weekday_abbr(date_str: str) -> str:
    try:
        return _WEEKDAY_ABBR[datetime.strptime(date_str, "%Y-%m-%d").weekday()]
    except Exception:
        return date_str


def _date_label(start: str, end: str) -> str:
    if not start:
        return ""
    s_abbr = _weekday_abbr(start)
    if not end or start == end:
        return s_abbr
    e_abbr = _weekday_abbr(end)
    return f"{s_abbr}–{e_abbr}" if s_abbr != e_abbr else s_abbr


def _classify_macro(ev: dict) -> Optional[str]:
    """Return the home macro category id, or None if no match."""
    rg = ev.get("release_group")
    if rg:
        if rg in ("employment_report", "jobless_claims_report", "jolts_report"):
            return "labor"
        if rg in ("ism_manufacturing_report", "ism_services_report", "factory_orders_report"):
            return "growth"

    bag = " ".join(
        str(ev.get(k) or "")
        for k in ("eventName", "indicatorName", "title", "display_title", "event_family")
    ).replace("_", " ")
    if not bag.strip():
        return None
    for cat in _MACRO_CATEGORIES:
        if cat["pat"].search(bag):
            return cat["id"]
    return None


def _is_us_macro(ev: dict) -> bool:
    """True when the event is a US macro release — Home only."""
    raw = ev.get("country")
    if raw is not None:
        country = str(raw).strip().upper()
        if country in ("US", "USA", "UNITED STATES"):
            return True
    # US Treasury point-in-time records from treasury_macro often omit country.
    et = (ev.get("eventType") or "").lower()
    if et in ("treasury_rate", "treasury_macro"):
        return True
    if str(ev.get("companyName") or "").strip().upper() == "US TREASURY":
        return True
    return False


# ── Home logical-event helpers ─────────────────────────────────────────────

def _logical_event_name(ev: dict) -> str:
    """Return the display name for a logical event (family card or discrete)."""
    if ev.get("type") == "macro_family":
        return ev.get("display_title") or ev.get("title") or ev.get("event_family", "") or ""
    return _event_name(ev)


def _effective_tier_val(ev: dict) -> int:
    """Unified numeric tier value: 3=critical, 2=major, 1=secondary, 0=context."""
    t = (ev.get("signal_tier") or "").lower()
    if t in _TIER_ORDER:
        return _TIER_ORDER[t]
    imp = (ev.get("importance") or "low").lower()
    if imp in ("high", "medium"):
        return 1
    return 0


def _effective_tier_name(ev: dict) -> str:
    """Unified string tier: critical, major, secondary, context."""
    t = (ev.get("signal_tier") or "").lower()
    if t in _TIER_ORDER:
        return t
    imp = (ev.get("importance") or "low").lower()
    if imp in ("high", "medium"):
        return "secondary"
    return "context"


def _normalize_time(t: object) -> str:
    """Push missing time to end so earliest is selected first."""
    if t is None or str(t).strip() == "":
        return "~"
    return str(t).strip()


def _normalize_date(d: object) -> str:
    """Push missing / malformed dates to end so valid dates sort first."""
    if d is None:
        return "~"
    s = str(d).strip()
    if s == "":
        return "~"
    if len(s) >= 10 and s[:10].startswith(("202", "201", "200", "199")):
        return s[:10]
    return "~"


def _resolve_parent_tier(children: list[dict]) -> str:
    """Return the strongest effective signal_tier across children."""
    best_val = -1
    best_tier = "context"
    for c in children:
        v = _effective_tier_val(c)
        if v > best_val:
            best_val = v
            best_tier = _effective_tier_name(c)
    return best_tier


def _resolve_parent_reason(children: list[dict], best_tier: str) -> str:
    """
    Return the signal_reason from the strongest / earliest child.
    Tie-breaking (deterministic):
      1.  strongest effective signal tier  (critical > major > secondary > context)
      2.  valid event date before missing date; then earliest date
      3.  valid event time before missing time; then earliest time
      4.  original source order (enumerate before sort)
      5.  lexical title (only if source order is unavailable)
    """
    indexed = list(enumerate(children))
    indexed.sort(
        key=lambda pair: (
            -_effective_tier_val(pair[1]),
            _normalize_date(pair[1].get("date")),
            _normalize_time(pair[1].get("time")),
            pair[0],
            _logical_event_name(pair[1]).lower(),
        ),
    )
    for idx, c in indexed:
        if _effective_tier_name(c) != best_tier:
            continue
        r = c.get("signal_reason")
        if r:
            return r
    for idx, c in indexed:
        r = c.get("signal_reason")
        if r:
            return r
    return _CATEGORY_BY_ID.get(
        _classify_macro(children[0]) if children else "", {}
    ).get("reason", "")


def _tier_to_impact(tier: str) -> str:
    if tier == "critical" or tier == "major":
        return "high"
    if tier == "secondary":
        return "medium"
    return "low"


def _tier_to_urgency(tier: str) -> str:
    if tier == "critical":
        return "high"
    if tier == "major":
        return "important"
    return "normal"


def _build_logical_subtitle(events: list[dict], cat: dict) -> tuple[str, int]:
    """Build subtitle from logical event display names. Returns (subtitle, extra_count)."""
    names: list[str] = []
    seen: set[str] = set()
    for ev in events:
        name = _logical_event_name(ev)
        if not name:
            continue
        key = name.lower()
        if key not in seen:
            seen.add(key)
            names.append(name)

    visible = names[:4]
    extra = max(0, len(names) - len(visible))
    if not visible:
        return cat.get("title", ""), 0
    subtitle = "Includes " + ", ".join(visible)
    if extra > 0:
        subtitle += f" +{extra} more"
    return subtitle, extra


# ── Group builder ────────────────────────────────────────────────────────────

def _build_macro_group(cat_id: str, events: list[dict], week_start: str) -> dict:
    cat = _CATEGORY_BY_ID[cat_id]

    events_sorted = sorted(
        events,
        key=lambda e: (
            -_TIER_ORDER.get((e.get("signal_tier") or "").lower(), 0),
            _normalize_date(e.get("date")),
        ),
    )

    seen_keys: set[tuple[str, str]] = set()
    deduped: list[dict] = []
    for ev in events_sorted:
        name = _logical_event_name(ev).lower()[:80]
        date_key = (ev.get("date") or "")[:10]
        key = (name, date_key)
        if key not in seen_keys:
            seen_keys.add(key)
            deduped.append(ev)

    if not deduped:
        return {}

    start_date = min((e.get("date") or "")[:10] or "z" for e in deduped)
    end_date   = max((e.get("date") or "")[:10] or ""  for e in deduped)

    parent_tier    = _resolve_parent_tier(deduped)
    parent_reason  = _resolve_parent_reason(deduped, parent_tier)

    subtitle, extra = _build_logical_subtitle(deduped, cat)
    date_label = _date_label(start_date, end_date)

    return {
        "id":          f"macro_{cat_id}_{week_start}",
        "type":        "macro_group",
        "category":    cat_id,
        "title":       cat["title"],
        "subtitle":    subtitle,
        "date_label":  date_label,
        "start_date":  start_date,
        "end_date":    end_date,
        "impact":      _tier_to_impact(parent_tier),
        "urgency":     _tier_to_urgency(parent_tier),
        "signal_tier": parent_tier,
        "reason":      parent_reason,
        "source":      "calendar_top_catalysts",
        "event_count": len(deduped),
        "children":    deduped,
    }


# ── Earnings card builder ────────────────────────────────────────────────────

def _build_earnings_card(ev: dict) -> dict:
    d     = (ev.get("date") or "")[:10]
    wd    = _weekday_abbr(d)
    sym   = ev.get("symbol") or ""
    name  = ev.get("companyName") or ev.get("title") or sym

    reasons    = ev.get("scoreReasons") or []
    reason_str = "; ".join(str(r) for r in reasons) if reasons else "Top earnings event this week"

    opt    = (ev.get("options_activity_strength") or "none").lower()
    impact = "high" if opt in ("unusual", "high") else "medium"
    urgency = (
        "high"      if opt == "unusual" else
        "important" if opt == "high"    else
        "normal"
    )

    return {
        "id":          f"earnings_{sym}_{d}",
        "type":        "earnings",
        "category":    "earnings",
        "title":       name,
        "subtitle":    f"{wd}: {sym} earnings" if sym else f"{wd}: Earnings",
        "date_label":  wd,
        "start_date":  d,
        "end_date":    d,
        "impact":      impact,
        "urgency":     urgency,
        "reason":      reason_str,
        "source":      "calendar_top_catalysts",
        "symbol":      sym,
        "options_activity_strength": opt,
        "sector_alignment_strength": ev.get("sector_alignment_strength"),
        "watchlist_boost":           ev.get("watchlist_boost"),
        "children":    [ev],
    }


# ── Other (IPO/split/dividend) card builder ──────────────────────────────────

def _build_other_card(ev: dict) -> dict:
    d   = (ev.get("date") or "")[:10]
    wd  = _weekday_abbr(d)
    sym = ev.get("symbol") or ""
    name = ev.get("companyName") or ev.get("title") or sym

    et_raw = (ev.get("eventType") or ev.get("sourceTab") or "other").lower()
    if "ipo" in et_raw:
        label = "IPO"
    elif "split" in et_raw:
        label = "Split"
    elif "dividend" in et_raw:
        label = "Dividend"
    else:
        label = et_raw.title()

    reasons = ev.get("whyThisMatters") or []
    reason_str = "; ".join(str(r) for r in reasons) if reasons else f"{label} event"

    return {
        "id":         f"other_{sym}_{d}",
        "type":       et_raw,
        "category":   "market_event",
        "title":      name,
        "subtitle":   f"{wd}: {sym} {label}" if sym else f"{wd}: {label}",
        "date_label": wd,
        "start_date": d,
        "end_date":   d,
        "impact":     "medium",
        "urgency":    "normal",
        "reason":     reason_str,
        "source":     "calendar_top_catalysts",
        "symbol":     sym,
        "children":   [ev],
    }


# ── Public entry point ────────────────────────────────────────────────────────

async def build_home_top_catalysts(
    today_override: Optional[date] = None,
) -> dict:
    """
    Build the compact Home Top Catalysts feed.

    Zero external API calls — pure reads from existing cached services.
    On Sat/Sun the planning window advances to the next Mon–Fri week, which is
    already covered by the shared rolling-horizon snapshot.

    `today_override` (ET date) is for testing/validation only.

    Weekend planning-window rules (America/New_York):
      Mon–Fri → current week   (window_mode="current_week")
      Sat–Sun → next week      (window_mode="next_week_planning")
    """
    from services.top_catalysts_service import get_top_catalysts

    monday, friday, window_mode = _planning_window(today_override)
    week_start   = monday.isoformat()
    week_end     = friday.isoformat()
    generated_at = datetime.now(tz=timezone.utc).isoformat()

    refresh_attempted = False
    refresh_succeeded = False
    cache_statuses: dict = {}
    empty_reason: Optional[str] = None

    # ── 1. Macro pool (compute once, reuse everywhere) ───────────────────────
    # One canonical reader/transformation handles both sources and the cross-
    # source dedupe so Home Top Catalysts never diverges from Calendar Top or
    # Economic Releases.  Pure snapshot read — no provider calls, no request-
    # time refreshes.
    # Use the snapshot service's authoritative range selector for Economic
    # Releases so internal provider gaps and covered-empty windows are reported
    # exactly as the snapshot sees them.
    econ_envelope = await _get_snapshot_window(
        "economic_releases", view="week", date=week_start,
    )
    tres_envelope = await _get_snapshot("treasury_macro")
    macro_window = get_canonical_macro_window(
        week_start,
        week_end,
        include_treasury_context=True,
        watchlist=set(),
        portfolio=set(),
        economic_envelope=econ_envelope,
        treasury_envelope=tres_envelope,
    )
    macro_logical = macro_window.get("macro_logical_events") or []
    source_windows = macro_window.get("source_windows") or {}
    coverage_complete = bool(macro_window.get("coverage_complete"))
    horizon_start = macro_window.get("horizon_start")
    horizon_end = macro_window.get("horizon_end")

    # ── 2. Base aggregation (earnings + other) from existing service ─────────
    # get_top_catalysts() uses the same shared ET planning window, so on Sat/Sun
    # it already returns the upcoming week's earnings/other.  Pass the resolved
    # Monday so both services agree on the exact window, and pass the already-
    # computed macro window so the canonical pipeline is not executed twice.
    base = get_top_catalysts(today=monday, macro_window=macro_window)
    days = base.get("days") or []

    earnings_flat: list[dict] = []
    other_flat: list[dict]    = []
    for day in days:
        day_date = _parse_date(day.get("date"))
        if day_date is None or not (monday <= day_date <= friday):
            continue
        earnings_flat.extend(day.get("earnings") or [])
        other_flat.extend(day.get("other") or [])

    earnings_flat.sort(key=lambda e: -float(e.get("rankScore") or 0))

    if not macro_logical:
        # Prefer the authoritative empty_reason from the canonical window
        # (e.g. coverage_gap, outside_horizon, no_events_in_window) when available.
        empty_reason = macro_window.get("empty_reason")
        if not empty_reason:
            if window_mode == "next_week_planning":
                empty_reason = (
                    "snapshot_horizon_incomplete"
                    if not coverage_complete else "no_events_in_planning_window"
                )
            elif window_mode == "current_week":
                empty_reason = "current_week_snapshots_empty"

    total_source = (
        len(earnings_flat) + len(other_flat)
        + macro_window.get("source_counts", {}).get("economic_source", 0)
        + macro_window.get("source_counts", {}).get("treasury_source", 0)
    )

    # ── 3. Categorize and group macro events ─────────────────────────────────
    #    Exclude non-US macro events.  US Treasury point-in-time records are
    #    kept even when country is missing because the source tab is US Treasury.
    macro_us = [ev for ev in macro_logical if _classify_macro(ev) and _is_us_macro(ev)]

    #    3f. Classify each canonical logical event into Home categories.
    events_by_cat: dict[str, list[dict]] = {}
    for ev in macro_us:
        cat_id = _classify_macro(ev)
        if cat_id:
            events_by_cat.setdefault(cat_id, []).append(ev)

    all_macro_cards: list[dict] = []
    for cat in sorted(_MACRO_CATEGORIES, key=lambda c: -c["priority"]):
        evs = events_by_cat.get(cat["id"])
        if not evs:
            continue
        card = _build_macro_group(cat["id"], evs, week_start)
        if card:
            all_macro_cards.append(card)

    has_fed = any(c["category"] == "fed_rates" for c in all_macro_cards)
    selected_macro: list[dict] = []
    treasury_used = 0
    for card in all_macro_cards:
        if len(selected_macro) >= _MAX_MACRO_GROUPS:
            break
        if card["category"] == "treasury":
            if treasury_used == 0:
                selected_macro.append(card)
                treasury_used += 1
            elif has_fed and len(selected_macro) < _MAX_MACRO_GROUPS:
                selected_macro.append(card)
                treasury_used += 1
        else:
            selected_macro.append(card)

    # ── 4. Earnings + other cards ─────────────────────────────────────────────
    earnings_cards = [_build_earnings_card(ev) for ev in earnings_flat[:_MAX_EARNINGS]]
    other_cards    = [_build_other_card(ev)    for ev in other_flat[:_MAX_OTHER]]

    # ── 5. Assemble final list ────────────────────────────────────────────────
    final: list[dict] = []
    final.extend(selected_macro)

    remaining = _MAX_TOTAL - len(final)
    final.extend(earnings_cards[:remaining])

    remaining = _MAX_TOTAL - len(final)
    final.extend(other_cards[:remaining])

    # hidden_count counts omitted logical Home category cards / direct children
    # — NOT raw source events.  Grouping many FMP rows into a single package
    # card is noise reduction, not omission.
    omitted_macro = max(0, len(all_macro_cards) - len(selected_macro))
    omitted_earnings = max(0, len(earnings_flat) - _MAX_EARNINGS)
    omitted_other = max(0, len(other_flat) - _MAX_OTHER)
    hidden = omitted_macro + omitted_earnings + omitted_other

    print(
        f"[home_top_catalysts] mode={window_mode} week={week_start}/{week_end} "
        f"macro_groups={len(selected_macro)} earnings={len(earnings_cards)} "
        f"other={len(other_cards)} total_cards={len(final)} "
        f"source_events={total_source} hidden={hidden}"
    )

    return {
        "view":                 "home_compact",
        "source":               "calendar_top_catalysts",
        "window_start":         week_start,
        "window_end":           week_end,
        "window_mode":          window_mode,
        "generated_at":         generated_at,
        "catalysts":            final,
        "total_source_events":  total_source,
        "total_grouped_events": len(selected_macro),
        "hidden_count":         hidden,
        "last_updated":         base.get("last_updated"),
        "status":               base.get("status") or "ready",
        # Diagnostics
        "refresh_attempted":    refresh_attempted,
        "refresh_succeeded":    refresh_succeeded,
        "cache_status":         cache_statuses,
            "source_windows":       source_windows,
        "empty_reason":         empty_reason,
        # Rolling-horizon coverage
        "coverage_complete":    coverage_complete,
        "horizon_start":        horizon_start,
        "horizon_end":          horizon_end,
    }
