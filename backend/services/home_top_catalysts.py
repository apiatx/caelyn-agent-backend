"""
Home Compact Top Catalysts feed — GET /api/home/top-catalysts.

Reuses existing cached data sources only (zero new external API calls):
  • top_catalysts_service.get_top_catalysts() — earnings + other (IPO/split/div)
  • calendar_snapshot_service.get_snapshot()  — full macro pool for grouping

The Calendar page's existing /api/catalysts/top endpoint is NOT modified.
This module applies its own grouping/ranking/limiting layer on top.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any, Optional


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


# ── Helpers ──────────────────────────────────────────────────────────────────

def _planning_window(
    today_et: Optional[date] = None,
) -> tuple[date, date, str]:
    """
    Return (monday, friday, window_mode) for the Home Top Catalysts feed.

    Planning rules (all in America/New_York):
      Mon–Fri  → current week's Mon–Fri   window_mode="current_week"
      Sat–Sun  → *next* week's Mon–Fri    window_mode="next_week_planning"

    `today_et` is accepted for unit-test overrides; defaults to the real ET date.
    """
    if today_et is None:
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from backports.zoneinfo import ZoneInfo  # type: ignore
        today_et = datetime.now(ZoneInfo("America/New_York")).date()

    wd = today_et.weekday()   # 0=Mon … 6=Sun
    if wd >= 5:               # Saturday (5) or Sunday (6)
        # Advance to next Monday
        days_to_monday = 7 - wd
        monday = today_et + timedelta(days=days_to_monday)
        mode = "next_week_planning"
    else:                     # Monday–Friday
        monday = today_et - timedelta(days=wd)
        mode = "current_week"

    friday = monday + timedelta(days=4)
    return monday, friday, mode


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
    bag = " ".join(
        str(ev.get(k) or "")
        for k in ("eventName", "indicatorName", "title", "display_title")
    )
    if not bag.strip():
        return None
    for cat in _MACRO_CATEGORIES:
        if cat["pat"].search(bag):
            return cat["id"]
    return None


# ── Group builder ────────────────────────────────────────────────────────────

def _build_macro_group(cat_id: str, events: list[dict], week_start: str) -> dict:
    cat = _CATEGORY_BY_ID[cat_id]

    # Sort by date, then dedupe by (normalized_name, date)
    events_sorted = sorted(events, key=lambda e: (e.get("date") or ""))
    seen_keys: set[tuple[str, str]] = set()
    deduped: list[dict] = []
    for ev in events_sorted:
        name_key = (_event_name(ev).lower()[:50], (ev.get("date") or "")[:10])
        if name_key not in seen_keys:
            seen_keys.add(name_key)
            deduped.append(ev)

    if not deduped:
        return {}

    start_date = (deduped[0].get("date") or "")[:10]
    end_date   = (deduped[-1].get("date") or "")[:10]
    impact     = _highest_impact(deduped)

    # Build subtitle: "Mon–Thu: CPI, Core CPI, PPI"  (max 4 distinct short names)
    names_seen: set[str] = set()
    short_names: list[str] = []
    for ev in deduped:
        n = _event_name(ev)
        # Strip trailing parenthetical like "(May)" or "(Q1)" for brevity
        n_short = re.sub(r"\s*\([^)]{1,10}\)\s*$", "", n).strip() or n
        if n_short.lower() not in names_seen and len(short_names) < 4:
            names_seen.add(n_short.lower())
            short_names.append(n_short)

    date_label = _date_label(start_date, end_date)
    names_str  = ", ".join(short_names) if short_names else cat["title"]
    subtitle   = f"{date_label}: {names_str}"
    extra      = len(deduped) - len(short_names)
    if extra > 0:
        subtitle += f" +{extra} more"

    return {
        "id":          f"macro_{cat_id}_{week_start}",
        "type":        "macro_group",
        "category":    cat_id,
        "title":       cat["title"],
        "subtitle":    subtitle,
        "date_label":  date_label,
        "start_date":  start_date,
        "end_date":    end_date,
        "impact":      impact,
        "urgency":     "high" if impact == "high" else "important",
        "reason":      cat["reason"],
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

def build_home_top_catalysts(
    today_override: Optional[date] = None,
) -> dict:
    """
    Build the compact Home Top Catalysts feed.

    Pure read across already-cached services. Zero new external API calls.
    Returns a response with at most 6–8 cards, macro events grouped by
    category (inflation/fed-rates/labor/growth/treasury/consumer/housing).

    `today_override` (date in ET) is for testing/validation only.

    Weekend rollover (planning-window rules):
      Mon–Fri → current week   (window_mode="current_week")
      Sat–Sun → next week      (window_mode="next_week_planning")
    """
    from services.top_catalysts_service import get_top_catalysts
    from services.calendar_snapshot_service import get_snapshot as _get_snapshot

    monday, friday, window_mode = _planning_window(today_override)
    week_start   = monday.isoformat()
    week_end     = friday.isoformat()
    generated_at = datetime.now(tz=__import__("datetime").timezone.utc).isoformat()

    # ── 1. Base aggregation (earnings + other) from existing service ─────────
    # get_top_catalysts() always builds against the current Mon–Fri window via
    # its own _week_bounds(). We then RE-FILTER by our planning window so that
    # on Sat/Sun (next-week mode) we do NOT surface last-week earnings.
    base = get_top_catalysts()
    days = base.get("days") or []

    earnings_flat: list[dict] = []
    other_flat: list[dict]    = []
    for day in days:
        day_date = _parse_date(day.get("date"))
        if day_date is None or not (monday <= day_date <= friday):
            # Outside our planning window — skip entirely on weekend rollover
            continue
        earnings_flat.extend(day.get("earnings") or [])
        other_flat.extend(day.get("other") or [])

    # Sort earnings by rankScore (already within-day sorted, but flatten globally)
    earnings_flat.sort(key=lambda e: -float(e.get("rankScore") or 0))

    # ── 2. Full macro pool from snapshots (same source, broader than whitelist)
    # On Sat/Sun the snapshots still hold the prior week's current_week data;
    # the date-filter below ensures only events that fall in monday…friday are
    # kept, so stale prior-week events are automatically excluded.
    macro_raw: list[dict] = []
    for tab in ("economic_releases", "treasury_macro"):
        try:
            env = _get_snapshot(tab) or {}
            for ev in (env.get("current_week") or []):
                if not isinstance(ev, dict):
                    continue
                d = _parse_date(ev.get("date"))
                if d and monday <= d <= friday:
                    macro_raw.append(ev)
        except Exception as exc:
            print(f"[home_top_catalysts] snapshot read failed tab={tab}: {exc}")

    total_source = len(earnings_flat) + len(macro_raw) + len(other_flat)

    # ── 3. Categorize and group macro events ─────────────────────────────────
    events_by_cat: dict[str, list[dict]] = {}
    for ev in macro_raw:
        cat_id = _classify_macro(ev)
        if cat_id:
            events_by_cat.setdefault(cat_id, []).append(ev)

    # Build group cards, sorted by category priority (descending)
    all_macro_cards: list[dict] = []
    for cat in sorted(_MACRO_CATEGORIES, key=lambda c: -c["priority"]):
        evs = events_by_cat.get(cat["id"])
        if not evs:
            continue
        card = _build_macro_group(cat["id"], evs, week_start)
        if card:
            all_macro_cards.append(card)

    # Apply macro limits: max 3 groups, treasury only once unless Fed event present
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
            # Allow a second treasury slot if there's a Fed event and we have room
            elif has_fed and len(selected_macro) < _MAX_MACRO_GROUPS:
                selected_macro.append(card)
                treasury_used += 1
        else:
            selected_macro.append(card)

    # ── 4. Build earnings cards (top 3 by rank score) ────────────────────────
    earnings_cards = [_build_earnings_card(ev) for ev in earnings_flat[:_MAX_EARNINGS]]

    # ── 5. Build other cards (top 2 from existing service output) ────────────
    other_cards = [_build_other_card(ev) for ev in other_flat[:_MAX_OTHER]]

    # ── 6. Assemble final list: macro → earnings → other, cap at 8 ───────────
    final: list[dict] = []
    final.extend(selected_macro)

    remaining = _MAX_TOTAL - len(final)
    final.extend(earnings_cards[:remaining])

    remaining = _MAX_TOTAL - len(final)
    final.extend(other_cards[:remaining])

    hidden = max(0, total_source - len(final))

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
    }
