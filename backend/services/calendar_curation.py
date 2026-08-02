"""
Calendar Curation — display-layer filter applied AFTER reading raw snapshots
from Neon and BEFORE returning to the frontend.

Scope (target tabs only):
    dividends, ipos, splits, economic_releases, treasury_macro

Earnings is intentionally NOT touched.

Hard rules:
- No external calls. No FMP. No profile enrichment. No DB lookups.
- Only uses fields already present on the event dict (see _build_event in
  services/catalyst_calendar_service.py: symbol, companyName, eventType,
  title, sector, industry, marketCap, marketCapBucket, importance, exchange,
  dividend, splitRatio, numerator, denominator, country, eventName, raw, …).
- Raw Neon storage is unchanged. Curation runs on a copy in-memory.

Output preserves the same envelope contract (current_week, previous_week,
last_updated, status). Only the contents of the two list slices are trimmed
and re-ranked.
"""
from __future__ import annotations

import hashlib
import re
from typing import Any, Iterable, Optional

# ── Public constants ────────────────────────────────────────────────────────

# Per-slice cap. Default user-facing top-N. Kept slightly above the
# task's lower bound (40) so empty tabs are unlikely.
DEFAULT_CAP_PER_SLICE = 50

# Tabs we curate. Earnings is excluded.
CURATED_TABS: tuple[str, ...] = (
    "dividends",
    "ipos",
    "splits",
    "economic_releases",
    "treasury_macro",
)

# Market cap floor (USD). Used only when marketCap is present and trustworthy.
# Below 40M is generally untradable noise. Above this floor we keep, even if
# small — we do NOT impose a 500M floor.
MC_FLOOR = 40_000_000.0


# ── Symbol cleanup ──────────────────────────────────────────────────────────

# Canonical symbol remaps for known stale tickers.
CANONICAL_SYMBOL_MAP: dict[str, str] = {
    "FB":   "META",
    "FB.A": "META",
    "TWTR": None,   # delisted
}


# Regex patterns that indicate non-common-stock junk we should drop:
#   - Preferred share suffixes: "-P", "-PA", "-PB", "-PC", "-PD", "-PE", "-PR…"
#   - Slash variants: "/P", "/PA", etc.
#   - Warrants: "-WT", ".WT", "+", " WS"
#   - Units:    "-U", ".U", "-UN", ".UN"
#   - Rights:   "-R", ".R"
# Tickers like "BRK.A" / "BRK-B" (legitimate share classes) are NOT preferred
# and are kept; the patterns below specifically target P*/W*/U*/R* suffixes.
_PREFERRED_RE = re.compile(
    r"""(?ix)
    (?:
        [-./]P[A-Z]?\d*\b      # -P, -PA, -PB, /P, .P, -P1
      | \bPRF\b
      | \bPREF\b
      | -PR[A-Z]?\b            # -PR, -PRA
    )$
    """
)

_WARRANT_RE = re.compile(r"(?i)(?:[-./](?:WT|WS|W)\d*|\+|\bWS\b)$")
_UNIT_RE    = re.compile(r"(?i)(?:[-./]U(?:N)?\d*)$")
_RIGHT_RE   = re.compile(r"(?i)(?:[-./]R(?:T)?\d*)$")

# Title/companyName tokens that strongly indicate a preferred share row even
# when the ticker itself looks plain (FMP sometimes returns name-only rows).
_PREFERRED_NAME_RE = re.compile(
    r"(?i)\b(preferred|pfd|series\s+[a-z]\b|cumulative|depositary|depositary\s+shares)\b"
)
_WARRANT_NAME_RE = re.compile(r"(?i)\b(warrant|warrants|right(s)?\s+to\s+purchase)\b")
_UNIT_NAME_RE    = re.compile(r"(?i)\b(unit|units)\b")


def _is_preferred_or_junk(symbol: str, name: Optional[str]) -> bool:
    """True if the ticker/name is a preferred, warrant, unit, or right."""
    s = (symbol or "").strip().upper()
    n = (name or "")
    if not s:
        # No symbol at all — only flag when the name itself is clearly junk.
        return bool(_PREFERRED_NAME_RE.search(n) or _WARRANT_NAME_RE.search(n))
    if _PREFERRED_RE.search(s):
        return True
    if _WARRANT_RE.search(s):
        return True
    if _UNIT_RE.search(s):
        return True
    if _RIGHT_RE.search(s):
        return True
    if _PREFERRED_NAME_RE.search(n):
        return True
    if _WARRANT_NAME_RE.search(n):
        return True
    return False


def _canonical_symbol(sym: Optional[str]) -> Optional[str]:
    if not sym:
        return sym
    s = sym.strip().upper()
    if s in CANONICAL_SYMBOL_MAP:
        return CANONICAL_SYMBOL_MAP[s]  # may be None → drop
    return s


# ── Theme detection (uses already-present fields only) ──────────────────────

# Each theme maps to a list of lowercase substrings checked against
# symbol/companyName/title/sector/industry. Materials is included per spec.
_THEMES: dict[str, tuple[str, ...]] = {
    "semiconductors": (
        "semiconductor", "semis", "chip", "foundry", "fabless",
        "nvda", "amd", "tsm", "asml", "lrcx", "klac", "amat", "mu", "intc",
        "avgo", "qcom", "mrvl", "mchp", "on ", "stm",
    ),
    "ai_infra": (
        "artificial intelligence", "ai infrastructure", "ai compute",
        "data center", "datacenter", "gpu", "accelerator", "hyperscaler",
        "anthropic", "openai", "nvidia", "smci", "vrt", "aniv", "crwv",
    ),
    "software_cloud": (
        "software", "cloud", "saas", "platform", "developer tools",
        "msft", "now", "crm", "snow", "ddog", "net", "panw", "team",
        "shop", "adbe", "orcl",
    ),
    "cybersecurity": (
        "cybersecurity", "security software", "endpoint security",
        "crwd", "panw", "zs", "s ", "okta", "ftnt", "net ", "cyber",
    ),
    "energy": (
        "oil", "gas", "energy", "petroleum", "lng", "refin",
        "xom", "cvx", "cop", "eog", "slb", "psx", "mpc", "vlo",
        "uranium", "nuclear",
    ),
    "industrials": (
        "industrial", "machinery", "aerospace", "defense", "rail",
        "ge ", "cat ", "etn", "phm", "pwr", "rtx", "lmt", "noc",
    ),
    "materials": (
        "materials", "chemical", "mining", "copper", "lithium",
        "steel", "aluminum", "rare earth", "fcx", "scco", "x ",
        "albemarle", "alb", "mp materials", "mp ",
    ),
    "supply_chain": (
        "supply chain", "logistics", "shipping", "freight",
        "bottleneck", "container", "port", "fdx", "ups", "csx", "unp",
    ),
}


def _theme_score(ev: dict, watchlist: set[str]) -> int:
    """
    Sum theme matches found in symbol/name/title/sector/industry.
    Each theme contributes +1 if any keyword hits. Watchlist hit adds +3.
    """
    sym = (ev.get("symbol") or "").upper()
    bag_parts = [
        sym,
        ev.get("companyName") or "",
        ev.get("title") or "",
        ev.get("sector") or "",
        ev.get("industry") or "",
        ev.get("eventName") or "",
    ]
    bag = " ".join(p for p in bag_parts if p).lower()
    if not bag.strip():
        return 0
    score = 0
    for _theme, kws in _THEMES.items():
        for kw in kws:
            if kw in bag:
                score += 1
                break
    if sym and sym in watchlist:
        score += 3
    return score


# ── Importance / event-quality helpers ──────────────────────────────────────

_IMPORTANCE_BOOST = {"high": 6, "medium": 3, "low": 0}


def _importance_score(ev: dict) -> int:
    return _IMPORTANCE_BOOST.get((ev.get("importance") or "low").lower(), 0)


def _market_cap(ev: dict) -> Optional[float]:
    mc = ev.get("marketCap")
    try:
        if mc is None:
            return None
        return float(mc)
    except (TypeError, ValueError):
        return None


def _passes_mc_floor(ev: dict) -> bool:
    """
    Microcap policy:
      - Unknown marketCap → KEEP (do not over-filter on missing metadata).
      - marketCap >= MC_FLOOR (40M) → KEEP.
      - Below MC_FLOOR → drop (truly untradable noise).
    """
    mc = _market_cap(ev)
    if mc is None:
        return True
    return mc >= MC_FLOOR


def _liquidity_relative_score(ev: dict) -> int:
    """
    Bonus for liquidity-vs-size data when already present on the event.
    Looks at relativeVolume, avgVolume, and volume fields if any were stamped
    upstream. Does NOT fetch anything. Returns 0 if nothing useful is found.
    """
    rel = ev.get("relativeVolume") or (ev.get("raw") or {}).get("relativeVolume")
    try:
        rel_f = float(rel) if rel is not None else None
    except (TypeError, ValueError):
        rel_f = None
    if rel_f is not None:
        if rel_f >= 2.0:
            return 3
        if rel_f >= 1.3:
            return 1
    return 0


# ── Dedup ───────────────────────────────────────────────────────────────────

def _record_completeness(ev: dict) -> int:
    """Higher = more fields populated. Used as tie-break in dedup."""
    fields = [
        "companyName", "sector", "industry", "marketCap",
        "exchange", "title", "importance",
    ]
    return sum(1 for f in fields if ev.get(f) not in (None, "", 0))


def _dedup_key(ev: dict) -> tuple:
    """Canonical symbol + date + eventType."""
    sym = _canonical_symbol(ev.get("symbol")) or ""
    date = ev.get("date") or ""
    et = ev.get("eventType") or ""
    if et in ("economic_releases", "economic_release"):
        # Economic events have no symbol; key on country + name + date.
        return (
            "ECON",
            (ev.get("country") or "").upper(),
            (ev.get("eventName") or ev.get("title") or "").lower(),
            date,
            et,
        )
    if et in ("treasury_rate", "treasury_macro"):
        return (
            "TRES",
            (ev.get("maturity") or ev.get("indicatorName") or "").upper(),
            date,
            et,
        )
    return (sym, date, et)


def _dedup(events: list[dict]) -> list[dict]:
    """Keep the most-complete record per dedup key."""
    best: dict[tuple, dict] = {}
    for ev in events:
        k = _dedup_key(ev)
        prev = best.get(k)
        if prev is None or _record_completeness(ev) > _record_completeness(prev):
            best[k] = ev
    return list(best.values())


# ── Tab-specific scoring ────────────────────────────────────────────────────

def _score_dividend(ev: dict, watchlist: set[str], portfolio: set[str]) -> float:
    score = 0.0
    score += _importance_score(ev)
    score += _theme_score(ev, watchlist)
    score += _liquidity_relative_score(ev)
    sym = (ev.get("symbol") or "").upper()
    if sym in watchlist:
        score += 4
    if sym in portfolio:
        score += 6
    mc = _market_cap(ev)
    if mc is not None:
        # Mild log-bucket bonus — do NOT make market cap dominant.
        if mc >= 200_000_000_000:
            score += 3
        elif mc >= 50_000_000_000:
            score += 2
        elif mc >= 10_000_000_000:
            score += 1
    # Dividend yield-ish hint (raw dividend amount only — we do not have price).
    try:
        d = float(ev.get("dividend") or 0)
        if d > 0:
            score += 0.5
    except (TypeError, ValueError):
        pass
    return score


def _score_ipo(ev: dict, watchlist: set[str], portfolio: set[str]) -> float:
    score = 0.0
    score += _importance_score(ev)
    score += _theme_score(ev, watchlist)
    exch = (ev.get("exchange") or "").upper()
    if "NASDAQ" in exch or "NYSE" in exch or "NYS" in exch:
        score += 3
    elif exch:
        score -= 1  # OTC, foreign minor exchanges
    mc = _market_cap(ev)
    if mc is not None:
        if mc >= 1_000_000_000:
            score += 3
        elif mc >= 250_000_000:
            score += 1
    sym = (ev.get("symbol") or "").upper()
    if sym in watchlist:
        score += 4
    if sym in portfolio:
        score += 5
    return score


def _score_split(ev: dict, watchlist: set[str], portfolio: set[str]) -> float:
    score = 0.0
    score += _importance_score(ev)
    score += _theme_score(ev, watchlist)
    score += _liquidity_relative_score(ev)
    sym = (ev.get("symbol") or "").upper()
    if sym in watchlist:
        score += 4
    if sym in portfolio:
        score += 5
    mc = _market_cap(ev)
    if mc is not None:
        if mc >= 10_000_000_000:
            score += 2
        elif mc >= 1_000_000_000:
            score += 1
    # Forward vs reverse split detection.
    try:
        num = float(ev.get("numerator") or 0)
        den = float(ev.get("denominator") or 0)
        if num and den:
            if num > den:
                score += 2  # forward split
            elif num < den:
                score -= 2  # reverse split (often distressed)
    except (TypeError, ValueError):
        pass
    return score


# Economic-release priority keywords. Higher index = higher importance.
_ECON_HIGH = (
    "cpi", "core cpi", "pce", "core pce", "ppi",
    "nonfarm", "non-farm", "nfp", "unemployment", "jobless", "jolts", "adp",
    "gdp", "gdp price", "fomc", "fed funds", "interest rate decision",
    "ism", "pmi", "manufacturing pmi", "services pmi",
    "retail sales", "treasury auction", "auction",
)
_ECON_MID = (
    "housing", "consumer confidence", "consumer sentiment",
    "industrial production", "trade balance", "durable goods",
    "factory orders", "construction spending", "personal income",
    "personal spending",
)


def _score_economic(ev: dict, watchlist: set[str], _portfolio: set[str]) -> float:
    score = 0.0
    score += _importance_score(ev)
    name = ((ev.get("eventName") or ev.get("title") or "") + " "
            + (ev.get("indicatorName") or "")).lower()
    country = (ev.get("country") or "").upper()
    if country in ("US", "USA", "UNITED STATES", ""):
        score += 4
    elif country in ("EU", "EUR", "EUROPEAN UNION", "DE", "GERMANY", "CN", "CHINA",
                     "JP", "JAPAN", "GB", "UK", "UNITED KINGDOM"):
        score += 1
    else:
        score -= 1
    if any(k in name for k in _ECON_HIGH):
        score += 8
    elif any(k in name for k in _ECON_MID):
        score += 3
    return score


_TREASURY_KEY_MATURITIES = ("3M", "6M", "1Y", "2Y", "5Y", "10Y", "30Y")


def _score_treasury(ev: dict, _watchlist: set[str], _portfolio: set[str]) -> float:
    score = 0.0
    score += _importance_score(ev)
    mat = (ev.get("maturity") or ev.get("indicatorName") or "").upper().replace(" ", "")
    if any(k in mat for k in _TREASURY_KEY_MATURITIES):
        score += 5
    name = ((ev.get("eventName") or ev.get("title") or "") + " "
            + (ev.get("indicatorName") or "")).lower()
    if any(k in name for k in ("auction", "yield", "treasury", "curve", "fed funds")):
        score += 2
    return score


_SCORERS = {
    "dividends":          _score_dividend,
    "ipos":               _score_ipo,
    "splits":             _score_split,
    "economic_releases":  _score_economic,
    "treasury_macro":     _score_treasury,
}


# ── Tab-specific hard filters ───────────────────────────────────────────────

def _hard_filter_securities(ev: dict) -> bool:
    """For tabs whose rows correspond to a tradable security."""
    name = ev.get("companyName") or ev.get("title")
    if _is_preferred_or_junk(ev.get("symbol") or "", name):
        return False
    canon = _canonical_symbol(ev.get("symbol"))
    if canon is None:
        return False  # explicitly delisted/dropped (e.g. TWTR)
    if not _passes_mc_floor(ev):
        return False
    return True


def _hard_filter_economic(ev: dict) -> bool:
    """Only meaningful macro releases."""
    name = ((ev.get("eventName") or ev.get("title") or "") + " "
            + (ev.get("indicatorName") or "")).lower()
    if not name.strip():
        return False
    # If the country is non-major and the release is not in the high list,
    # drop it. This is the main lever for trimming hundreds of ROW rows.
    country = (ev.get("country") or "").upper()
    is_major = country in (
        "US", "USA", "UNITED STATES",
        "EU", "EUR", "EUROPEAN UNION",
        "DE", "GERMANY", "CN", "CHINA",
        "JP", "JAPAN", "GB", "UK", "UNITED KINGDOM",
    )
    if not is_major and not any(k in name for k in _ECON_HIGH):
        return False
    return True


def _hard_filter_treasury(ev: dict) -> bool:
    raw = (ev.get("maturity") or ev.get("indicatorName") or "")
    mat = re.sub(r"[^A-Z0-9]", "", raw.upper())
    if not mat:
        # Allow named curve/auction events through the title path.
        title = " ".join(
            str(ev.get(k) or "") for k in ("title", "eventName", "indicatorName")
        ).lower()
        return any(k in title for k in ("auction", "treasury", "curve", "yield"))
    return any(k in mat for k in _TREASURY_KEY_MATURITIES)


_HARD_FILTERS = {
    "dividends":         _hard_filter_securities,
    "ipos":              _hard_filter_securities,
    "splits":            _hard_filter_securities,
    "economic_releases": _hard_filter_economic,
    "treasury_macro":    _hard_filter_treasury,
}


# ── Macro family grouping ──────────────────────────────────────────────────

# Only these US families are grouped in this phase.
_FAMILIES_TO_GROUP: frozenset[str] = frozenset({"cpi", "ppi", "pce", "gdp", "eci"})

# Display titles for-family cards.
_FAMILY_DISPLAY_TITLES: dict[str, str] = {
    "cpi": "CPI Inflation Report",
    "ppi": "PPI Inflation Report",
    "pce": "PCE Inflation Report",
    "gdp": "GDP Report",
    "eci": "Employment Cost Index",
}

# Lead-metric precedence by family.  Each tuple contains normalised substrings
# matched case-insensitively against the child eventName / title.  First match
# wins; tie-breaking uses earlier source order then lexical order.
_LEAD_PRECEDENCE: dict[str, list[str]] = {
    "cpi": [
        "core cpi mom",
        "core cpi yoy",
        "cpi mom",
        "cpi yoy",
    ],
    "ppi": [
        "core ppi mom",
        "ppi mom",
        "core ppi yoy",
        "ppi yoy",
    ],
    "pce": [
        "core pce price index mom", "core pce mom",
        "core pce price index yoy", "core pce yoy",
        "pce price index mom", "pce mom",
        "pce price index yoy", "pce yoy",
        "core pce prices qoq",
        "pce prices qoq",
    ],
    "gdp": [
        "gdp growth rate qoq",
        "advance gdp",
        "gdp growth rate yoy",
        "gdp price index",
        "gdp sales",
    ],
    "eci": [
        "employment cost index qoq",
        "employment cost index yoy",
    ],
}

_TIER_ORDER: dict[str, int] = {"critical": 3, "major": 2, "secondary": 1, "context": 0}


def _child_name(ev: dict) -> str:
    return (ev.get("eventName") or ev.get("title") or "").strip().lower()


def _resolve_lead(childs: list[dict], precedence: list[str]) -> dict:
    for pattern in precedence:
        for c in childs:
            if pattern in _child_name(c):
                return c
    return childs[0]


def _strongest_tier(childs: list[dict]) -> str:
    best = "context"
    best_val = 0
    for c in childs:
        t = (c.get("signal_tier") or "").lower()
        v = _TIER_ORDER.get(t, 0)
        if v > best_val:
            best, best_val = t, v
    return best


def _resolve_signal_reason(childs: list[dict], lead: dict) -> str:
    lead_reason = lead.get("signal_reason")
    if lead_reason:
        return lead_reason
    sorted_children = sorted(
        childs,
        key=lambda c: _TIER_ORDER.get((c.get("signal_tier") or "").lower(), 0),
        reverse=True,
    )
    for c in sorted_children:
        r = c.get("signal_reason")
        if r:
            return r
    return ""


def _make_family_id(family: str, country: str, date: str, time_val: str) -> str:
    raw = f"macro_family:{family}:{country}:{date}:{time_val or ''}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def _build_family_card(
    family: str, children: list[dict], date: str, time_val: str | None
) -> dict:
    lead = _resolve_lead(children, _LEAD_PRECEDENCE.get(family, []))
    tier = _strongest_tier(children)
    reason = _resolve_signal_reason(children, lead)
    display = _FAMILY_DISPLAY_TITLES.get(family, family.replace("_", " ").title())

    return {
        "id":            _make_family_id(family, "US", date, time_val or ""),
        "type":          "macro_family",
        "eventType":     "economic_release",
        "eventCategory": "macro",
        "symbol":        "Macro",
        "event_family":  family,
        "display_title": display,
        "title":         display,
        "subtitle":      lead.get("subtitle"),
        "keyDetails":    lead.get("keyDetails"),
        "date":          date,
        "time":          time_val,
        "country":       "US",
        "signal_tier":   tier,
        "signal_reason": reason,
        "importance":    lead.get("importance"),
        "lead_metric":   _child_name(lead).title() or (lead.get("eventName") or lead.get("title") or ""),
        "actual":        lead.get("actual"),
        "estimate":      lead.get("estimate"),
        "previous":      lead.get("previous"),
        "unit":          lead.get("unit") or (lead.get("raw") or {}).get("unit"),
        "children":      children,
        "event_count":   len(children),
        "source":        lead.get("source") or "fmp",
        "raw":           None,
    }


def group_economic_events_to_families(events: list[dict]) -> list[dict]:
    """
    Group US individual economic-release events into family-level cards.

    Only approved US families (cpi, ppi, pce, gdp, eci) are grouped.
    All other events pass through unchanged. Source events are never
    mutated — family cards hold the original children by reference.

    Grouping key: (event_family, date, time, country=US).
    """
    if not events:
        return []

    groups: dict[tuple, list[dict]] = {}
    pass_through: list[dict] = []
    group_order: list[tuple] = []

    for ev in events:
        family = (ev.get("event_family") or "").lower()
        country = (ev.get("country") or "").upper()
        date = ev.get("date") or ""
        time_val = ev.get("time") or ""

        if family in _FAMILIES_TO_GROUP and country == "US":
            key = (family, date, time_val)
            if key not in groups:
                groups[key] = []
                group_order.append(key)
            groups[key].append(ev)
        else:
            pass_through.append(ev)

    result: list[dict] = []
    for key in group_order:
        children = groups[key]
        family, date, time_val = key
        result.append(_build_family_card(family, children, date, time_val if time_val else None))

    result.extend(pass_through)
    return result


# ── Release-package grouping (display-layer only) ────────────────────────────

# Approved display release groups.  These are NOT persisted to Neon and
# operate as a display-layer concept only.  Source event_family values are
# never rewritten.

_RELEASE_GROUPS: frozenset[str] = frozenset({
    "employment_report",
    "jobless_claims_report",
    "jolts_report",
    "ism_manufacturing_report",
    "ism_services_report",
    "factory_orders_report",
})

_RELEASE_GROUP_DISPLAY_TITLES: dict[str, str] = {
    "employment_report":        "Employment Report",
    "jobless_claims_report":   "Jobless Claims Report",
    "jolts_report":             "JOLTS Report",
    "ism_manufacturing_report": "ISM Manufacturing Report",
    "ism_services_report":      "ISM Services Report",
    "factory_orders_report":    "Factory Orders Report",
}

# Lead-metric precedence for each release group.  Matched case-insensitively
# against the child eventName/title.  First match wins; tie-breaking uses
# earlier source order then lexical order.
_RELEASE_GROUP_LEAD_PRECEDENCE: dict[str, list[str]] = {
    "employment_report": [
        "non farm payroll", "nonfarm payroll",
        "unemployment rate",
        "average hourly earnings mom",
        "average hourly earnings yoy",
        "labor force participation rate",
    ],
    "jobless_claims_report": [
        "initial jobless claims",
        "continuing jobless claims",
        "jobless claims 4-week",
    ],
    "jolts_report": [
        "jolts job openings",
        "jolts job quits",
        "jolts hires",
    ],
    "ism_manufacturing_report": [
        "ism manufacturing pmi",
        "ism manufacturing new orders",
        "ism manufacturing prices",
        "ism manufacturing employment",
    ],
    "ism_services_report": [
        "ism services pmi",
        "ism non-manufacturing pmi",
        "ism services business activity",
        "ism non-manufacturing business activity",
        "ism services new orders",
        "ism non-manufacturing new orders",
        "ism services prices",
        "ism non-manufacturing prices",
        "ism services employment",
        "ism non-manufacturing employment",
    ],
    "factory_orders_report": [
        "factory orders mom",
        "factory orders ex transportation",
    ],
}

# Regex patterns for matching release groups.
# Order is important: more-specific patterns must be checked first so that
# e.g. ISM Manufacturing subclassifiers win before generic ISM.
_EMPLOYMENT_REPORT_RE = re.compile(
    r"\b(?:non[\s-]?farm\s+payroll|nfp\b|unemployment\s+rate\b|"
    r"average\s+hourly\s+earnings|labor\s+force\s+participation|"
    r"government\s+payroll|u[\s-]?6\s+unemployment)",
    re.I,
)

_JOBLESS_CLAIMS_RE = re.compile(
    r"\b(?:initial\s+jobless\s+claims|continuing\s+jobless\s+claims|"
    r"jobless\s+claims\s+4[\s-]?week)",
    re.I,
)

_JOLTS_RE = re.compile(
    r"\bjolts?\s+(?:job\s+openings|job\s+quits|hires)",
    re.I,
)

_ISM_MANUFACTURING_RE = re.compile(
    r"\bism\s+manufacturing\b",
    re.I,
)

_ISM_SERVICES_RE = re.compile(
    r"\bism\s+(?:services|non[\s-]?manufacturing)\b",
    re.I,
)

_FACTORY_ORDERS_RE = re.compile(
    r"\bfactory\s+orders\b",
    re.I,
)


def _determine_release_group(ev: dict) -> str | None:
    name = ((ev.get("eventName") or ev.get("title") or "") + " " +
            (ev.get("indicatorName") or "")).lower()
    if not name.strip():
        return None

    # Check most-specific patterns first.
    if _ISM_MANUFACTURING_RE.search(name):
        return "ism_manufacturing_report"
    if _ISM_SERVICES_RE.search(name):
        return "ism_services_report"
    if _JOBLESS_CLAIMS_RE.search(name):
        return "jobless_claims_report"
    if _JOLTS_RE.search(name):
        return "jolts_report"
    if _FACTORY_ORDERS_RE.search(name):
        return "factory_orders_report"
    if _EMPLOYMENT_REPORT_RE.search(name):
        return "employment_report"

    return None


def _make_release_package_id(
    release_group: str, country: str, date: str, time_val: str,
) -> str:
    raw = f"release_pkg:{release_group}:{country}:{date}:{time_val or ''}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def _build_release_package_card(
    release_group: str, children: list[dict], date: str, time_val: str | None,
) -> dict:
    lead = _resolve_lead(children, _RELEASE_GROUP_LEAD_PRECEDENCE.get(release_group, []))
    tier = _strongest_tier(children)
    reason = _resolve_signal_reason(children, lead)
    display = _RELEASE_GROUP_DISPLAY_TITLES.get(
        release_group, release_group.replace("_", " ").title(),
    )

    return {
        "id":            _make_release_package_id(release_group, "US", date, time_val or ""),
        "type":          "macro_family",
        "eventType":     "economic_release",
        "eventCategory": "macro",
        "symbol":        "Macro",
        "event_family":  release_group,
        "release_group": release_group,
        "display_title": display,
        "title":         display,
        "subtitle":      lead.get("subtitle"),
        "keyDetails":    lead.get("keyDetails"),
        "date":          date,
        "time":          time_val,
        "country":       "US",
        "signal_tier":   tier,
        "signal_reason": reason,
        "importance":    lead.get("importance"),
        "lead_metric":   _child_name(lead).title() or (lead.get("eventName") or lead.get("title") or ""),
        "actual":        lead.get("actual"),
        "estimate":      lead.get("estimate"),
        "previous":      lead.get("previous"),
        "unit":          lead.get("unit") or (lead.get("raw") or {}).get("unit"),
        "children":      children,
        "event_count":   len(children),
        "source":        lead.get("source") or "fmp",
        "raw":           None,
    }


def group_events_to_release_packages(events: list[dict]) -> list[dict]:
    """
    Group remaining multi-row US economic-release packages (Employment Report,
    Jobless Claims Report, JOLTS Report, ISM Manufacturing Report, ISM Services
    Report, Factory Orders Report) into display-level package cards.

    Operates AFTER ``group_economic_events_to_families()``.  Family cards
    (type == "macro_family") pass through unchanged.  Discrete US events that
    match a release-group pattern are grouped by (release_group, date, time).
    Source events are never mutated.

    Grouping key: (release_group, date, time, country=US).
    """
    if not events:
        return []

    family_cards: list[dict] = []
    discrete: list[dict] = []
    for ev in events:
        if ev.get("type") == "macro_family":
            family_cards.append(ev)
        else:
            discrete.append(ev)

    groups: dict[tuple, list[dict]] = {}
    group_order: list[tuple] = []
    ungrouped: list[dict] = []

    for ev in discrete:
        country = (ev.get("country") or "").upper()
        if country != "US":
            ungrouped.append(ev)
            continue

        rg = _determine_release_group(ev)
        if rg is None:
            ungrouped.append(ev)
            continue

        date = ev.get("date") or ""
        time_val = ev.get("time") or ""
        key = (rg, date, time_val)
        if key not in groups:
            groups[key] = []
            group_order.append(key)
        groups[key].append(ev)

    result: list[dict] = list(family_cards)

    for key in group_order:
        children = groups[key]
        rg, date, time_val = key
        result.append(
            _build_release_package_card(rg, children, date, time_val if time_val else None),
        )

    result.extend(ungrouped)
    return result


# ── Shared canonical economic logical-event transformation ─────────────────

def curate_economic_logical_events(
    events: Iterable[dict],
    *,
    cap: int = DEFAULT_CAP_PER_SLICE,
    watchlist: Optional[set[str]] = None,
    portfolio: Optional[set[str]] = None,
) -> list[dict]:
    """
    Transform raw individual economic-release rows into canonical logical
    events (family cards + release-package cards + discrete events).

    Order of operations:
      1. hard filtering (economic_releases rules)
      2. deterministic dedup
      3. group_economic_events_to_families
      4. group_events_to_release_packages
      5. canonical scoring/ranking
      6. stable output ordering

    The output preserves canonical fields:
      id, date, display_title, event_family, release_group, signal_tier,
      signal_reason, lead_metric, children, actual/estimate/previous, unit.

    This helper is used by:
      • Economic Releases route curation
      • Calendar Top Catalysts macro aggregation
      • Home Top Catalysts macro aggregation
    """
    raw_list = [e for e in events if isinstance(e, dict)]
    if not raw_list:
        return []

    watchlist = watchlist or set()
    portfolio = portfolio or set()

    # 1. Hard filter.
    filtered = [e for e in raw_list if _hard_filter_economic(e)]

    # 2. Dedup.
    deduped = _dedup(filtered)

    # 3. Family grouping.
    grouped = group_economic_events_to_families(deduped)

    # 4. Release-package grouping.
    packaged = group_events_to_release_packages(grouped)

    # 5. Score & rank using the existing economic scorer.
    #    Family/package cards score by their strongest child (signal_tier) and
    #    the lead child's economic importance.  This keeps the same relative
    #    ordering the Economic Releases tab already uses.
    def _score_logical(ev: dict) -> float:
        # Prefer explicit signal_tier strength over the legacy score so a
        # critical FOMC decision outranks a major CPI family.
        tier_val = _TIER_ORDER.get((ev.get("signal_tier") or "").lower(), 0)
        base = _score_economic(ev, watchlist, portfolio)
        return (tier_val * 1000.0) + base

    packaged.sort(
        key=lambda e: (
            _score_logical(e),
            e.get("date") or "",
            e.get("display_title") or e.get("title") or e.get("eventName") or "",
        ),
        reverse=True,
    )

    # 6. Trim.
    return packaged[: max(1, int(cap))]


# ── Canonical macro-window reader/transformation ───────────────────────────

def get_canonical_macro_window(
    start_date: str,
    end_date: str,
    *,
    include_treasury_context: bool = True,
    watchlist: Optional[set[str]] = None,
    portfolio: Optional[set[str]] = None,
    economic_envelope: Optional[dict] = None,
    treasury_envelope: Optional[dict] = None,
) -> dict:
    """
    One shared macro-catalyst window for all consumers.

    Reads the existing Neon snapshots (economic_releases + treasury_macro),
    selects events that fall inside [start_date, end_date], runs the shared
    canonical logical-event transformation, and performs deterministic cross-
    source deduplication.

    Callers that already hold a snapshot envelope (e.g. the Economic Releases
    requested-window route, or Home/Calendar Top planning-week consumers that
    used `get_snapshot_window`) may pass `economic_envelope` /
    `treasury_envelope` to avoid a second snapshot read and to preserve the
    authoritative coverage-range verdict.

    When an envelope is explicitly supplied and contains the `events` key,
    that exact array is authoritative — even if it is empty.  Only legacy
    non-preloaded snapshots fall back to `current_week`.

    Returns a dict with:
      • window_start / window_end
      • economic_logical_events
      • treasury_logical_events
      • macro_logical_events (economic + de-duplicated treasury)
      • source_counts
      • last_updated
      • coverage_complete, empty_reason, coverage, coverage_ranges
      • horizon_start, horizon_end, actual_start, actual_end
      • status, is_stale

    This function makes ZERO provider calls and ZERO snapshot writes.
    """
    from services.calendar_snapshot_service import get_snapshot

    watchlist = watchlist or set()
    portfolio = portfolio or set()

    last_updated_candidates: list[str] = []
    econ_source: list[dict] = []
    tres_source: list[dict] = []
    source_windows: dict[str, str] = {}
    horizon_start: Optional[str] = None
    horizon_end: Optional[str] = None
    coverage_complete = True
    empty_reason: Optional[str] = None

    # Economic releases: prefer the rolling-horizon `events` collection.
    # If the caller already loaded the envelope, reuse it to avoid a second
    # snapshot read and to preserve the caller's coverage-range verdict.
    econ_env = economic_envelope if economic_envelope is not None else get_snapshot("economic_releases") or {}
    if econ_env.get("last_updated"):
        last_updated_candidates.append(str(econ_env["last_updated"]))
    econ_horizon = econ_env.get("horizon") or {}

    # Presence-based source selection.  A preloaded envelope with an explicit
    # (possibly empty) `events` key is authoritative; never fall back to
    # current_week for those callers.  Legacy callers that did not preload an
    # envelope keep the previous fallback behavior.
    if economic_envelope is not None and "events" in economic_envelope:
        econ_pool = list(economic_envelope.get("events") or [])
    else:
        econ_pool = econ_env.get("events") or econ_env.get("current_week") or []

    for ev in econ_pool:
        if not isinstance(ev, dict):
            continue
        d = (ev.get("date") or "")[:10]
        if start_date <= d <= end_date:
            econ_source.append(ev)

    stored = (econ_env.get("window") or {})
    cov = econ_env.get("coverage") or {}
    stored_horizon_end = (econ_horizon.get("horizon_end") or "")
    has_broad_horizon = bool(econ_env.get("events"))
    if econ_horizon.get("horizon_start") and not horizon_start:
        horizon_start = econ_horizon.get("horizon_start")
    if stored_horizon_end and (not horizon_end or stored_horizon_end > horizon_end):
        horizon_end = stored_horizon_end
    source_windows["economic_releases"] = (
        f"{stored.get('stored_from','?')}→{stored.get('stored_to','?')}"
        f" horizon_end={stored_horizon_end or 'N/A'}"
        f" coverage={'ok' if cov.get('complete') else 'incomplete'}"
    )

    # If the caller supplied a preloaded envelope with a coverage-range-based
    # verdict, trust it over the coarse horizon-end check.
    if economic_envelope is not None and "coverage_complete" in economic_envelope:
        coverage_complete = bool(economic_envelope["coverage_complete"])
        empty_reason = economic_envelope.get("empty_reason")
    else:
        h_start = (econ_horizon.get("horizon_start") or "")
        if not has_broad_horizon:
            # Legacy snapshot without a rolling horizon cannot cover a future week.
            coverage_complete = False
        elif (
            (stored_horizon_end and stored_horizon_end < end_date)
            or (h_start and h_start > start_date)
        ):
            coverage_complete = False

    # Treasury: optional point-in-time context.
    if include_treasury_context:
        if treasury_envelope is not None and "events" in treasury_envelope:
            tres_env = treasury_envelope
            tres_pool = list(treasury_envelope.get("events") or [])
        else:
            tres_env = treasury_envelope if treasury_envelope is not None else get_snapshot("treasury_macro") or {}
            tres_pool = tres_env.get("events") or tres_env.get("current_week") or []

        if tres_env.get("last_updated"):
            last_updated_candidates.append(str(tres_env["last_updated"]))
        for ev in tres_pool:
            if not isinstance(ev, dict):
                continue
            d = (ev.get("date") or "")[:10]
            if start_date <= d <= end_date:
                tres_source.append(ev)

        tres_stored = (tres_env.get("window") or {})
        tres_cov = tres_env.get("coverage") or {}
        source_windows["treasury_macro"] = (
            f"{tres_stored.get('stored_from','?')}→{tres_stored.get('stored_to','?')}"
            f" coverage={'ok' if tres_cov.get('complete') else 'incomplete'}"
        )

    # Shared canonical logical-event transformation.
    econ_logical = curate_economic_logical_events(
        econ_source, cap=500, watchlist=watchlist, portfolio=portfolio,
    )
    tres_curated = curate_events(
        "treasury_macro", tres_source, cap=500,
    ) if include_treasury_context else []

    # Deterministic cross-source dedupe.  Scheduled dated auctions that already
    # have a canonical Economic Releases representation are dropped from the
    # treasury stream; unique yield/curve records remain.
    econ_keys: set[tuple[str, str]] = set()
    for ev in econ_logical:
        title = (
            ev.get("display_title") or ev.get("title") or
            ev.get("eventName") or ""
        ).strip().lower()
        econ_keys.add((title, (ev.get("date") or "")[:10]))

    def _treasury_is_duplicate(ev: dict) -> bool:
        title = (
            ev.get("eventName") or ev.get("title") or
            ev.get("indicatorName") or ""
        ).strip().lower()
        return (title, (ev.get("date") or "")[:10]) in econ_keys

    tres_unique = [ev for ev in tres_curated if not _treasury_is_duplicate(ev)]

    # Preserve authoritative coverage metadata from the source envelope when
    # available, enriching the canonical output without re-deriving it.
    coverage_ranges = econ_env.get("coverage_ranges") or (
        (econ_env.get("meta") or {}).get("coverage_ranges") if isinstance(econ_env.get("meta"), dict) else None
    ) or []
    actual_dates = sorted(
        (e.get("date") or "")[:10] for e in econ_source if (e.get("date") or "")[:10]
    )
    actual_start = actual_dates[0] if actual_dates else None
    actual_end = actual_dates[-1] if actual_dates else None

    return {
        "window_start": start_date,
        "window_end": end_date,
        "economic_logical_events": econ_logical,
        "treasury_logical_events": tres_unique,
        "macro_logical_events": econ_logical + tres_unique,
        "source_counts": {
            "economic_source": len(econ_source),
            "treasury_source": len(tres_source),
            "economic_logical": len(econ_logical),
            "treasury_logical": len(tres_unique),
        },
        "last_updated": max(last_updated_candidates) if last_updated_candidates else None,
        "coverage_complete": coverage_complete,
        "empty_reason": empty_reason,
        "coverage": cov or {"complete": coverage_complete},
        "coverage_ranges": coverage_ranges,
        "horizon_start": horizon_start,
        "horizon_end": horizon_end,
        "actual_start": actual_start,
        "actual_end": actual_end,
        "status": econ_env.get("status"),
        "is_stale": econ_env.get("is_stale"),
        "source_windows": source_windows,
    }


# ── Public entry point ─────────────────────────────────────────────────────

def curate_events(
    tab: str,
    events: Iterable[dict],
    *,
    cap: int = DEFAULT_CAP_PER_SLICE,
    watchlist: Optional[set[str]] = None,
    portfolio: Optional[set[str]] = None,
) -> list[dict]:
    """
    Curate a list of raw events for one of the supported tabs.

    Returns a NEW list. Input is not mutated. If `tab` is not a target tab,
    the input is returned unchanged (defensive — the caller already filters).
    """
    if tab not in CURATED_TABS:
        return list(events)

    raw_list = [e for e in events if isinstance(e, dict)]
    if not raw_list:
        return []

    watchlist = watchlist or set()
    portfolio = portfolio or set()

    hard = _HARD_FILTERS.get(tab, lambda _ev: True)
    scorer = _SCORERS.get(tab)

    # 1. Hard-drop garbage and apply canonical symbol rewrite.
    filtered: list[dict] = []
    for ev in raw_list:
        if not hard(ev):
            continue
        sym = ev.get("symbol")
        if sym:
            canon = _canonical_symbol(sym)
            if canon is None:
                continue
            if canon != sym.upper():
                # Make a shallow copy so we never mutate the Neon-cached dict.
                ev = {**ev, "symbol": canon}
        filtered.append(ev)

    # 2. Dedup.
    deduped = _dedup(filtered)

    # 2b. Family + release-package grouping — applies to economic_releases only.
    #     Runs after dedup, before scoring.  Raw Neon storage is unchanged.
    if tab == "economic_releases":
        deduped = curate_economic_logical_events(
            deduped, cap=cap, watchlist=watchlist, portfolio=portfolio,
        )
        return deduped

    # 3. Score & rank.
    if scorer is not None:
        deduped.sort(
            key=lambda e: (
                scorer(e, watchlist, portfolio),
                # Secondary tiebreak: newer / earlier date first.
                e.get("date") or "",
            ),
            reverse=True,
        )

    # 4. Trim.
    return deduped[: max(1, int(cap))]


def curate_envelope(
    tab: str,
    envelope: dict,
    *,
    cap: int = DEFAULT_CAP_PER_SLICE,
    watchlist: Optional[set[str]] = None,
    portfolio: Optional[set[str]] = None,
) -> dict:
    """
    Curate the current_week / previous_week slices of an envelope. Returns a
    new envelope dict; input is not mutated. Other envelope fields
    (last_updated, status, tab, mode, …) are passed through unchanged.
    """
    if tab not in CURATED_TABS or not isinstance(envelope, dict):
        return envelope

    cw_raw = envelope.get("current_week") or []
    pw_raw = envelope.get("previous_week") or []

    cw = curate_events(tab, cw_raw, cap=cap, watchlist=watchlist, portfolio=portfolio)
    pw = curate_events(tab, pw_raw, cap=cap, watchlist=watchlist, portfolio=portfolio)

    # Lightweight raw->curated logging for ops visibility.
    print(
        f"[calendar_curation] tab={tab} "
        f"current_week raw={len(cw_raw)} curated={len(cw)} "
        f"previous_week raw={len(pw_raw)} curated={len(pw)} cap={cap}"
    )

    out = dict(envelope)
    out["current_week"] = cw
    out["previous_week"] = pw
    return out


# Module self-test / smoke check (run as: python -m services.calendar_curation)
def _smoke() -> int:
    sample = [
        # Dividends — preferred junk, common, watchlist hit, microcap
        {"symbol": "BAC-PA", "companyName": "Bank of America Pref A",
         "eventType": "dividends", "date": "2026-05-01"},
        {"symbol": "AAPL", "companyName": "Apple Inc.", "eventType": "dividends",
         "date": "2026-05-02", "marketCap": 3_000_000_000_000, "sector": "Technology"},
        {"symbol": "TINY", "companyName": "Tiny Co", "eventType": "dividends",
         "date": "2026-05-02", "marketCap": 5_000_000},  # below floor
        {"symbol": "MP", "companyName": "MP Materials", "eventType": "dividends",
         "date": "2026-05-03", "marketCap": 4_000_000_000, "sector": "Materials"},
        {"symbol": "FB", "companyName": "Facebook (legacy)",
         "eventType": "dividends", "date": "2026-05-04",
         "marketCap": 1_500_000_000_000},
    ]
    out = curate_events("dividends", sample, cap=10)
    assert all((e.get("symbol") or "") not in ("BAC-PA", "TINY") for e in out), out
    syms = [e.get("symbol") for e in out]
    assert "META" in syms, syms  # FB→META rewrite
    print("smoke OK", syms)
    return 0


if __name__ == "__main__":
    raise SystemExit(_smoke())
