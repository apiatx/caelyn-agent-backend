"""
Top Catalysts This Week — high-signal weekly intelligence layer.

Answers: "What actually matters this week to trade?"

Read-only aggregation. Reuses ONLY existing cached services / snapshots:
  • Earnings — services.earnings_clean_service week-clean cache
        key: ``earnings:curated:week:{from}:{to}``
  • Options Flow — main.py master screener cache
        keys: ``options_master_screener_v1`` / ``options_master_lkg_v1``
  • Watchlist / Portfolio — services.earnings_clean_service loaders
  • Sector Rotation — services.sector_rotation cached dashboard
        key: ``sr:dashboard:v1``
  • Macro — services.calendar_snapshot_service economic_releases
    + treasury_macro snapshots (whitelist filtered).

NEVER does at request time: FMP fetch, Finnhub fetch, profile enrichment,
new external API calls, scheduler mutation, or snapshot writes.

Output (grouped-by-day):
    {
      "tab": "top_catalysts",
      "mode": "weekly",
      "week": "YYYY-MM-DD/YYYY-MM-DD",
      "days": [
        {
          "date": "YYYY-MM-DD",
          "weekday": "Monday",
          "earnings": [...top events scored by options/watchlist/sector...],
          "macro":    [...whitelisted macro events (CPI/PPI/NFP/FOMC/GDP/Treasury)...],
          "other":    [...rare cap-2-3/week IPO/dividend/split entries...]
        },
        ...
      ],
      "current_week": [...flat ranked list (backward compat)...],
      "previous_week": [],
      "last_updated": "...",
      "status": "ready" | "stale" | "empty"
    }
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

from data.cache import cache
from services.calendar_curation import (
    CANONICAL_SYMBOL_MAP,
    MC_FLOOR,
    _canonical_symbol,
    _is_preferred_or_junk,
    curate_economic_logical_events,
    curate_events as _curate_events,
)
from services.calendar_snapshot_service import (
    get_snapshot as _get_snapshot,
)


# ── Cache keys for shared services we read from ─────────────────────────────

_OPTIONS_MASTER_CACHE_KEY = "options_master_screener_v1"
_OPTIONS_MASTER_LKG_KEY   = "options_master_lkg_v1"
_SECTOR_DASHBOARD_KEY     = "sr:dashboard:v1"

# Per-day caps
MAX_EARNINGS_PER_DAY = 6
MAX_OTHER_PER_DAY    = 1
MAX_OTHER_PER_WEEK   = 3

# Backward-compat flat current_week cap (kept for clients on old shape)
DEFAULT_CAP: int = 40
MAX_CAP: int = 50
MIN_CAP: int = 25

# Weekday names
_WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
             "Saturday", "Sunday"]


# ── Macro whitelist ─────────────────────────────────────────────────────────
#
# Only these high-signal macro events are surfaced. Everything else from
# economic_releases / treasury_macro is dropped.
#
# Match is case-insensitive against eventName / indicatorName / title fields.

_MACRO_WHITELIST_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("CPI",
     re.compile(r"\b(?:cpi|consumer\s+price\s+index|inflation\s+rate)\b", re.I)),
    ("PPI",
     re.compile(r"\b(?:ppi|producer\s+price\s+index)\b", re.I)),
    ("PCE",
     re.compile(r"\b(?:pce|personal\s+consumption\s+expenditures?)\b", re.I)),
    ("ECI",
     re.compile(r"\b(?:eci|employment\s+cost\s+index)\b", re.I)),
    ("NFP",
     re.compile(r"(?:\bnfp\b|non[-\s]?farm\s+payroll|nonfarm\s+payroll|"
                 r"employment\s+change|payrolls?\s+report)", re.I)),
    ("FOMC",
     re.compile(r"\b(?:fomc|federal\s+reserve|fed(?:\s+funds)?(?:\s+rate)?\s+"
                 r"decision|interest\s+rate\s+decision|fed\s+chair|"
                 r"fed\s+minutes)\b", re.I)),
    ("GDP",
     re.compile(r"\bgdp\b", re.I)),
    ("Treasury Auctions",
     re.compile(r"\btreasury\s+(?:auction|bill|note|bond|yield)\b", re.I)),
]


def _classify_macro(ev: dict) -> Optional[str]:
    """Return canonical macro tag (CPI/PPI/NFP/FOMC/GDP/Treasury Auctions)
    if event matches whitelist, else None."""
    bag = " ".join(
        str(ev.get(k) or "") for k in
        ("eventName", "indicatorName", "title", "event", "name")
    )
    if not bag.strip():
        return None
    for tag, pat in _MACRO_WHITELIST_PATTERNS:
        if pat.search(bag):
            return tag
    return None


# ── Watchlist / portfolio ───────────────────────────────────────────────────

def _load_watchlist_set() -> set[str]:
    try:
        from services.earnings_clean_service import _load_watchlist
        return _load_watchlist() or set()
    except Exception:
        return set()


def _load_portfolio_set() -> set[str]:
    try:
        from services.earnings_clean_service import _load_portfolio
        return _load_portfolio() or set()
    except Exception:
        return set()


# ── Week bounds ─────────────────────────────────────────────────────────────

def resolve_top_catalysts_week(
    today_et: Optional[date] = None,
) -> tuple[date, date, str]:
    """
    Return (monday, friday, window_mode) for the Top Catalysts planning week.

    Planning rules (America/New_York):
      Mon–Fri  → current week's Mon–Fri   window_mode="current_week"
      Sat–Sun  → next week's Mon–Friday   window_mode="next_week_planning"

    `today_et` accepts a unit-test override; otherwise defaults to the real ET
    date.  Both Calendar Top Catalysts and Home Top Catalysts use this single
    helper so their windows can never drift.
    """
    if today_et is None:
        today_et = datetime.now(ZoneInfo("America/New_York")).date()

    wd = today_et.weekday()  # 0=Mon … 6=Sun
    if wd >= 5:  # Saturday (5) or Sunday (6)
        days_to_monday = 7 - wd
        monday = today_et + timedelta(days=days_to_monday)
        mode = "next_week_planning"
    else:
        monday = today_et - timedelta(days=wd)
        mode = "current_week"

    friday = monday + timedelta(days=4)
    return monday, friday, mode


def _week_bounds(today: Optional[date] = None) -> tuple[date, date]:
    """Legacy thin wrapper kept for existing tests and callers."""
    monday, friday, _ = resolve_top_catalysts_week(today)
    return monday, friday


def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


# ── Earnings cache read ─────────────────────────────────────────────────────

def _read_earnings_week_cache(monday: date, friday: date) -> Optional[dict]:
    ck = f"earnings:curated:week:{monday.strftime('%Y-%m-%d')}:{friday.strftime('%Y-%m-%d')}"
    hit = cache.get(ck)
    if isinstance(hit, dict):
        return hit
    return None


# ── Options flow cache read ─────────────────────────────────────────────────

def _read_options_master() -> dict[str, dict]:
    """
    Return {SYMBOL: row} from the existing master options screener cache.
    No fetch. Empty dict if cache cold.
    """
    snap = cache.get(_OPTIONS_MASTER_CACHE_KEY) or cache.get(_OPTIONS_MASTER_LKG_KEY)
    if not isinstance(snap, dict):
        return {}
    rows = snap.get("tickers") or []
    out: dict[str, dict] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        sym = (r.get("ticker") or "").upper().strip()
        if sym:
            out[sym] = r
    return out


def _options_strength(opt_row: Optional[dict]) -> tuple[str, float]:
    """
    Classify per-ticker options activity from existing cached fields.
    Returns (label, numeric_strength) where label ∈ {none, normal, high, unusual}.

    Inputs (read existing fields only):
      composite_score   — top-level rank (higher = more unusual)
      heat_score        — engine heat metric (0-100ish)
      unusual_volume_ratio — vol / avg vol multiple
      call_put_premium_ratio
    """
    if not opt_row:
        return "none", 0.0

    cs   = float(opt_row.get("composite_score") or 0)
    hs   = float(opt_row.get("heat_score") or 0)
    uvr  = float(opt_row.get("unusual_volume_ratio") or 0)

    # "unusual": top-tier composite OR very high heat OR >3x volume
    if cs >= 75 or hs >= 70 or uvr >= 3.0:
        return "unusual", max(cs, hs, uvr * 25.0)
    # "high": noticeable abnormality
    if cs >= 50 or hs >= 45 or uvr >= 1.5:
        return "high", max(cs, hs, uvr * 25.0)
    # In master cache at all = at least normal activity
    if cs > 0 or hs > 0 or uvr > 0:
        return "normal", max(cs, hs, uvr * 25.0)
    return "none", 0.0


# ── Sector momentum read ────────────────────────────────────────────────────

# Map FMP/profile sector strings → SPDR sector ETF used by sector rotation.
_SECTOR_NAME_TO_ETF: dict[str, str] = {
    "communication services": "XLC",
    "communications":         "XLC",
    "consumer discretionary": "XLY",
    "consumer cyclical":      "XLY",
    "consumer staples":       "XLP",
    "consumer defensive":     "XLP",
    "energy":                 "XLE",
    "financials":             "XLF",
    "financial services":     "XLF",
    "financial":              "XLF",
    "health care":            "XLV",
    "healthcare":             "XLV",
    "industrials":            "XLI",
    "industrial":             "XLI",
    "materials":              "XLB",
    "basic materials":        "XLB",
    "real estate":            "XLRE",
    "technology":             "XLK",
    "information technology": "XLK",
    "utilities":              "XLU",
}


def _read_sector_dashboard() -> dict[str, dict]:
    """
    Return {ETF: snapshot_dict} for sector rotation. Empty dict if cold.
    Reads the same cache the /api/sectors dashboard serves.
    """
    snap = cache.get(_SECTOR_DASHBOARD_KEY)
    if not isinstance(snap, dict):
        return {}
    out: dict[str, dict] = {}
    for s in (snap.get("sectors") or []):
        if not isinstance(s, dict):
            continue
        t = (s.get("ticker") or "").upper()
        if t:
            out[t] = s
    return out


def _sector_alignment(sector: Optional[str], theme_tags: list[str],
                      sectors_by_etf: dict[str, dict]) -> tuple[str, float, Optional[str]]:
    """
    Determine sector/theme momentum strength using existing sector dashboard.
    Returns (label, numeric, etf) — label ∈ {hot, winning, neutral, none}.

    Strategy:
      1. Resolve event sector → SPDR ETF using local map.
      2. Look up cached SectorSnapshot for that ETF.
      3. Map regime_tag / rotation_score to label.

    No new pipeline; pure read of cached fields.
    """
    if not sectors_by_etf:
        return "none", 0.0, None

    etf: Optional[str] = None
    if sector:
        etf = _SECTOR_NAME_TO_ETF.get(sector.strip().lower())
    # Theme-based fallback for AI / semis / energy / industrials / materials
    if not etf and theme_tags:
        for tag in theme_tags:
            t = tag.lower()
            if t in ("semiconductors", "ai_infra", "software_cloud", "cybersecurity"):
                etf = "XLK"
                break
            if t == "energy":
                etf = "XLE"; break
            if t == "industrials":
                etf = "XLI"; break
            if t == "materials":
                etf = "XLB"; break
    if not etf:
        return "none", 0.0, None

    snap = sectors_by_etf.get(etf)
    if not snap:
        return "none", 0.0, etf

    rscore = float(snap.get("rotation_score") or 0)
    tag = (snap.get("regime_tag") or "").lower()

    if "leader" in tag or rscore >= 70:
        return "hot", rscore, etf
    if "improving" in tag or rscore >= 50:
        return "winning", rscore, etf
    if rscore >= 30:
        return "neutral", rscore, etf
    return "none", rscore, etf


# ── Earnings event scoring ──────────────────────────────────────────────────

# Numeric strengths for ranking
_OPTIONS_NUM = {"unusual": 100.0, "high": 60.0, "normal": 20.0, "none": 0.0}
_SECTOR_NUM  = {"hot": 30.0, "winning": 18.0, "neutral": 5.0, "none": 0.0}
_WATCHLIST_BOOST = 25.0


def _score_earnings(ev: dict, opt_row: Optional[dict],
                    sectors_by_etf: dict[str, dict],
                    watchlist: set[str], portfolio: set[str]
                    ) -> tuple[float, dict]:
    """
    Rank an earnings event by:
      1. options_activity_strength (HIGH weight, top priority)
      2. watchlist_boost (binary)
      3. sector_alignment_strength (sector/theme momentum)

    NO market-cap usage. NO importanceScore-of-event-engine reuse for ranking
    (kept only as a tiny tie-breaker so curated importance doesn't dominate).

    Returns (rank_score, signal_dict) where signal_dict carries the per-event
    fields needed in the response payload.
    """
    sym = (ev.get("symbol") or "").upper()

    opt_label, opt_num = _options_strength(opt_row)

    in_wl = bool(sym and sym in watchlist)
    in_pf = bool(sym and sym in portfolio)
    # Watchlist OR portfolio match → boost.
    watchlist_boost = in_wl or in_pf

    sect_label, sect_num, sect_etf = _sector_alignment(
        ev.get("sector"),
        ev.get("themeTags") or [],
        sectors_by_etf,
    )

    score = (
        _OPTIONS_NUM[opt_label]
        + (_WATCHLIST_BOOST if watchlist_boost else 0.0)
        + _SECTOR_NUM[sect_label]
    )
    # Use options numeric as a secondary nudge so two "high" rows stay ordered.
    score += min(opt_num, 100.0) * 0.05

    # Mild tie-breaker only: pre-existing curated importanceScore (capped tiny).
    iscore = ev.get("importanceScore")
    if isinstance(iscore, (int, float)):
        score += min(float(iscore) / 50.0, 1.5)

    reasons: list[str] = []
    if opt_label == "unusual":
        reasons.append("Unusual options activity")
    elif opt_label == "high":
        reasons.append("High options activity")
    if watchlist_boost:
        reasons.append("Watchlist Boost" if in_wl else "Portfolio Boost")
    if sect_label == "hot":
        reasons.append(
            f"{(ev.get('sector') or sect_etf or 'Sector')} sector momentum"
        )
    elif sect_label == "winning":
        reasons.append(
            f"{(ev.get('sector') or sect_etf or 'Sector')} improving"
        )
    # Theme tags as secondary reason (e.g. AI, semis)
    for t in (ev.get("themeTags") or [])[:2]:
        if isinstance(t, str) and t:
            pretty = t.replace("_", " ").title()
            reasons.append(f"{pretty} theme")

    return score, {
        "options_activity_strength": opt_label,
        "watchlist_boost":           bool(watchlist_boost),
        "sector_alignment_strength": sect_label,
        "sector_etf":                sect_etf,
        "scoreReasons":              reasons,
    }


# ── Garbage filter ──────────────────────────────────────────────────────────

def _passes_garbage_filter(ev: dict) -> bool:
    et = (ev.get("eventType") or "").lower()
    sym = ev.get("symbol") or ""
    name = ev.get("companyName") or ev.get("title") or ""
    if et in ("earnings", "earnings_dates", "dividend", "dividends",
              "ipo", "ipos", "stock_split", "splits"):
        if _is_preferred_or_junk(sym, name):
            return False
        if sym:
            up = sym.strip().upper()
            if up in CANONICAL_SYMBOL_MAP and CANONICAL_SYMBOL_MAP[up] is None:
                return False
        mc = ev.get("marketCap")
        try:
            mc_f = float(mc) if mc is not None else None
        except (TypeError, ValueError):
            mc_f = None
        if mc_f is not None and mc_f < MC_FLOOR:
            return False
    return True


# ── Normalization for the response ──────────────────────────────────────────

def _is_large_cap(ev: dict) -> bool:
    try:
        mc = float(ev.get("marketCap") or 0)
    except (TypeError, ValueError):
        return False
    return mc >= 50_000_000_000


_HOT_THEME_TAGS = {
    "semiconductors", "ai_infra", "software_cloud", "cybersecurity",
    "energy", "industrials", "materials",
}


def _has_hot_theme(ev: dict) -> bool:
    tags = ev.get("themeTags") or []
    if not isinstance(tags, list):
        return False
    return any(isinstance(t, str) and t.lower() in _HOT_THEME_TAGS for t in tags)


def _normalize_earnings_event(ev: dict, signals: dict, score: float) -> dict:
    sym_raw = ev.get("symbol")
    canon = _canonical_symbol(sym_raw) if sym_raw else None
    sym = canon or sym_raw
    out: dict[str, Any] = {
        "symbol":                    sym,
        "title":                     ev.get("companyName") or ev.get("title") or sym or "",
        "companyName":               ev.get("companyName") or sym or "",
        "date":                      (ev.get("date") or "")[:10],
        "eventType":                 "earnings",
        "sourceTab":                 "earnings",
        "rankScore":                 round(score, 3),
        "options_activity_strength": signals["options_activity_strength"],
        "watchlist_boost":           signals["watchlist_boost"],
        "sector_alignment_strength": signals["sector_alignment_strength"],
        "whyThisMatters":            signals["scoreReasons"],
        "scoreReasons":              signals["scoreReasons"],
    }
    # Optional pass-through if present
    for k in ("time", "session", "sector", "industry", "themeTags",
              "epsEstimated", "revenueEstimated", "importanceScore"):
        v = ev.get(k)
        if v is not None and v != "":
            out[k] = v
    if signals.get("sector_etf"):
        out["sector_etf"] = signals["sector_etf"]
    out["raw"] = ev
    return out


def _normalize_macro_event(ev: dict, tag: str) -> dict:
    out: dict[str, Any] = {
        "title":         ev.get("eventName") or ev.get("indicatorName") or ev.get("title") or tag,
        "date":          (ev.get("date") or "")[:10],
        "eventType":     "macro",
        "macroType":     tag,
        "sourceTab":     "macro",
        "whyThisMatters": [f"{tag} release"],
    }
    for k in ("time", "country", "importance", "actual", "estimate", "previous",
              "indicatorName", "eventName", "maturity"):
        v = ev.get(k)
        if v is not None and v != "":
            out[k] = v
    out["raw"] = ev
    return out


# Canonical event_family / release_group → Top Catalysts macroType tag.
# Maintains backward-compatible macroType names while covering the full shared
# canonical logical-event taxonomy.
_FAMILY_TO_TOP_TAG: dict[str, str] = {
    "cpi": "CPI",
    "ppi": "PPI",
    "pce": "PCE",
    "eci": "ECI",
    "gdp": "GDP",
    "fomc_decision": "FOMC",
    "fomc_minutes": "FOMC",
    "treasury_auction": "Treasury Auctions",
}

_RELEASE_GROUP_TO_TOP_TAG: dict[str, str] = {
    "employment_report": "Employment Report",
    "jobless_claims_report": "Jobless Claims Report",
    "jolts_report": "JOLTS Report",
    "ism_manufacturing_report": "ISM Manufacturing Report",
    "ism_services_report": "ISM Services Report",
    "factory_orders_report": "Factory Orders Report",
}


def _is_us_macro_event(ev: dict) -> bool:
    """True for US macro releases or US Treasury point-in-time records."""
    country = str(ev.get("country") or "").strip().upper()
    if country in ("US", "USA", "UNITED STATES"):
        return True
    et = (ev.get("eventType") or "").lower()
    if et in ("treasury_rate", "treasury_macro"):
        return True
    return False


def _macro_type_for_logical_event(ev: dict) -> Optional[str]:
    """Return the backward-compatible macroType tag for a canonical logical event."""
    family = (ev.get("event_family") or "").lower()
    rg = (ev.get("release_group") or "").lower()
    if family in _FAMILY_TO_TOP_TAG:
        return _FAMILY_TO_TOP_TAG[family]
    if rg in _RELEASE_GROUP_TO_TOP_TAG:
        return _RELEASE_GROUP_TO_TOP_TAG[rg]
    # Legacy title-based classification for any event not yet in the maps.
    return _classify_macro(ev)


def _normalize_macro_logical_event(ev: dict, tag: str) -> dict:
    """
    Normalize a canonical logical event (family card, package card, or discrete
    macro event) into the Top Catalysts response shape.

    Preserves canonical fields so the frontend can render explicit tiers and
    child metadata consistently across Calendar, Home and Economic Releases.
    """
    out: dict[str, Any] = {
        "title":         ev.get("display_title") or ev.get("title") or ev.get("eventName") or tag,
        "date":          (ev.get("date") or "")[:10],
        "eventType":     "macro",
        "macroType":     tag,
        "sourceTab":     "macro",
        "whyThisMatters": [ev.get("signal_reason") or f"{tag} release"],
        "type":          "macro_family" if ev.get("type") == "macro_family" else "macro",
        "event_family":  ev.get("event_family"),
        "release_group": ev.get("release_group"),
        "signal_tier":   ev.get("signal_tier"),
        "signal_reason": ev.get("signal_reason"),
        "lead_metric":   ev.get("lead_metric"),
        "children":      ev.get("children") or [],
        "event_count":   ev.get("event_count") or len(ev.get("children") or []),
        "canonical_id":  ev.get("id"),
    }
    for k in ("time", "country", "importance", "actual", "estimate", "previous",
              "unit", "eventName", "indicatorName"):
        v = ev.get(k)
        if v is not None and v != "":
            out[k] = v
    out["raw"] = None
    return out


def _normalize_other_event(ev: dict, source_tab: str) -> dict:
    sym_raw = ev.get("symbol")
    canon = _canonical_symbol(sym_raw) if sym_raw else None
    sym = canon or sym_raw
    et = (ev.get("eventType") or source_tab)
    label = "IPO" if source_tab in ("ipo", "ipos") else (
        "Dividend" if source_tab in ("dividend", "dividends") else (
        "Stock Split" if source_tab in ("stock_split", "splits") else et
    ))
    reasons = [f"{label}"]
    if _is_large_cap(ev):
        reasons.append("Large cap")
    if _has_hot_theme(ev):
        reasons.append("Thematic relevance")
    out: dict[str, Any] = {
        "symbol":        sym,
        "title":         ev.get("companyName") or ev.get("title") or sym or label,
        "companyName":   ev.get("companyName") or sym or "",
        "date":          (ev.get("date") or "")[:10],
        "eventType":     et,
        "sourceTab":     source_tab,
        "whyThisMatters": reasons,
    }
    for k in ("time", "exchange", "priceRange", "shares", "marketCap",
              "dividend", "splitRatio", "numerator", "denominator", "sector",
              "industry", "themeTags"):
        v = ev.get(k)
        if v is not None and v != "":
            out[k] = v
    out["raw"] = ev
    return out


# ── Public entry point ──────────────────────────────────────────────────────

def get_top_catalysts(
    *,
    cap: int = DEFAULT_CAP,
    today: Optional[date] = None,
) -> dict:
    """
    Build the high-signal Top Catalysts response, grouped by day.

    Pure read across already-cached services. No request-time external calls.
    """
    cap = max(MIN_CAP, min(int(cap or DEFAULT_CAP), MAX_CAP))
    monday, friday = _week_bounds(today)
    _, _, window_mode = resolve_top_catalysts_week(today)
    week_label = f"{monday.isoformat()}/{friday.isoformat()}"
    week_start_str = monday.isoformat()
    week_end_str = friday.isoformat()

    watchlist = _load_watchlist_set()
    portfolio = _load_portfolio_set()
    options_by_sym = _read_options_master()
    sectors_by_etf = _read_sector_dashboard()

    last_updated_candidates: list[str] = []

    # ── 1. Earnings (dominant signal) ──────────────────────────────────────
    earnings_per_day: dict[str, list[dict]] = {}
    earnings_flat: list[tuple[float, dict]] = []
    earn_envelope = _read_earnings_week_cache(monday, friday)
    if earn_envelope:
        as_of = earn_envelope.get("asOf")
        if as_of:
            last_updated_candidates.append(str(as_of))
        scored: list[tuple[float, dict, dict, str]] = []
        for ev in (earn_envelope.get("topEvents") or []):
            if not isinstance(ev, dict):
                continue
            if not _passes_garbage_filter(ev):
                continue
            d = _parse_date(ev.get("date"))
            if not d or d < monday or d > friday:
                continue
            sym = (ev.get("symbol") or "").upper()
            opt_row = options_by_sym.get(sym)
            score, signals = _score_earnings(
                ev, opt_row, sectors_by_etf, watchlist, portfolio,
            )
            scored.append((score, ev, signals, d.isoformat()))

        # Sort overall: unusual options first (handled by score), then
        # sort_by score desc; per-day cap enforced after grouping.
        scored.sort(key=lambda t: -t[0])
        seen_per_day_sym: dict[tuple[str, str], bool] = {}
        for score, ev, signals, day in scored:
            sym = (ev.get("symbol") or "").upper()
            key = (day, sym)
            if key in seen_per_day_sym:
                continue
            day_list = earnings_per_day.setdefault(day, [])
            if len(day_list) >= MAX_EARNINGS_PER_DAY:
                continue
            normalized = _normalize_earnings_event(ev, signals, score)
            day_list.append(normalized)
            earnings_flat.append((score, normalized))
            seen_per_day_sym[key] = True

    # ── 2. Macro (shared canonical logical-event transformation) ───────────
    # Phase A: read the broad horizon when available, fall back to current_week.
    macro_source_events: list[dict] = []
    for tab in ("economic_releases", "treasury_macro"):
        try:
            env = _get_snapshot(tab) or {}
        except Exception as e:
            print(f"[top_catalysts] snapshot read failed tab={tab}: {e}")
            continue
        if env.get("last_updated"):
            last_updated_candidates.append(str(env["last_updated"]))
        # Prefer the rolling horizon `events` collection; fall back to legacy
        # current_week for older snapshots.
        pool = env.get("events") if (env.get("events") and tab == "economic_releases") else (env.get("current_week") or [])
        for ev in pool:
            if not isinstance(ev, dict):
                continue
            d = _parse_date(ev.get("date"))
            if not d or d < monday or d > friday:
                continue
            macro_source_events.append({**ev, "_source_tab": tab})

    # Phase B: split by source and apply the appropriate canonical curation.
    econ_raw = [ev for ev in macro_source_events if ev.get("_source_tab") == "economic_releases"]
    tres_raw = [ev for ev in macro_source_events if ev.get("_source_tab") == "treasury_macro"]

    # Shared canonical logical-event pipeline for economic releases.
    econ_logical = curate_economic_logical_events(
        econ_raw, cap=500, watchlist=watchlist, portfolio=portfolio,
    )

    # Treasury/macro is curated separately so point-in-time yield/curve records
    # are preserved without being swallowed by the economic family grouper.
    tres_curated = _curate_events("treasury_macro", tres_raw, cap=500)

    # Phase C: deterministic cross-source dedupe.
    # Scheduled dated auctions from treasury_macro collapse to the canonical
    # Economic Releases event; unique yield/curve records remain.
    econ_keys: set[tuple[str, str]] = set()
    for ev in econ_logical:
        title = (ev.get("display_title") or ev.get("title") or ev.get("eventName") or "").strip().lower()
        econ_keys.add((title, (ev.get("date") or "")[:10]))

    def _treasury_is_duplicate(ev: dict) -> bool:
        title = (ev.get("eventName") or ev.get("title") or ev.get("indicatorName") or "").strip().lower()
        return (title, (ev.get("date") or "")[:10]) in econ_keys

    tres_unique = [ev for ev in tres_curated if not _treasury_is_duplicate(ev)]

    macro_source_count = len(econ_raw) + len(tres_raw)
    macro_logical_count = len(econ_logical) + len(tres_unique)

    # Phase D: normalise canonical logical events into the response shape.
    # Top Catalysts surfaces US releases only (plus US Treasury point-in-time
    # records); foreign canonical events are intentionally dropped here so the
    # Economic Releases tab retains its full global view.
    macro_per_day: dict[str, list[dict]] = {}
    seen_macro: set[tuple[str, str]] = set()
    for ev in econ_logical + tres_unique:
        if not _is_us_macro_event(ev):
            continue
        tag = _macro_type_for_logical_event(ev)
        if not tag:
            continue
        day = (ev.get("date") or "")[:10]
        key = (tag, day)
        if key in seen_macro:
            continue
        seen_macro.add(key)
        normalized = _normalize_macro_logical_event(ev, tag)
        macro_per_day.setdefault(day, []).append(normalized)

    # ── 3. Other (IPO/dividend/split) — default exclude, max 2-3 / week ────
    other_pool: list[tuple[float, dict]] = []
    for tab in ("ipos", "dividends", "splits"):
        try:
            env = _get_snapshot(tab) or {}
        except Exception:
            continue
        if env.get("last_updated"):
            last_updated_candidates.append(str(env["last_updated"]))
        for ev in (env.get("current_week") or []):
            if not isinstance(ev, dict):
                continue
            d = _parse_date(ev.get("date"))
            if not d or d < monday or d > friday:
                continue
            if not _passes_garbage_filter(ev):
                continue
            # Only allow if cached fields clearly show large cap OR hot theme.
            large = _is_large_cap(ev)
            hot   = _has_hot_theme(ev)
            if not (large or hot):
                continue
            sym = (ev.get("symbol") or "").upper()
            # Watchlist match also adds value.
            score = 0.0
            if large: score += 50.0
            if hot:   score += 30.0
            if sym and (sym in watchlist or sym in portfolio):
                score += 20.0
            if tab == "ipos": score += 5.0
            normalized = _normalize_other_event(ev, source_tab=tab)
            other_pool.append((score, normalized))

    other_pool.sort(key=lambda t: -t[0])

    other_per_day: dict[str, list[dict]] = {}
    other_used_total = 0
    for score, ev in other_pool:
        if other_used_total >= MAX_OTHER_PER_WEEK:
            break
        day = ev.get("date") or ""
        if not day:
            continue
        day_list = other_per_day.setdefault(day, [])
        if len(day_list) >= MAX_OTHER_PER_DAY:
            continue
        day_list.append(ev)
        other_used_total += 1

    # ── 4. Build days[] ────────────────────────────────────────────────────
    days_out: list[dict] = []
    cur = monday
    while cur <= friday:
        ds = cur.isoformat()
        days_out.append({
            "date":     ds,
            "weekday":  _WEEKDAYS[cur.weekday()],
            "earnings": earnings_per_day.get(ds, []),
            "macro":    macro_per_day.get(ds, []),
            "other":    other_per_day.get(ds, []),
        })
        cur += timedelta(days=1)

    # ── 5. Backward-compat flat current_week (top earnings + macro + other) ─
    earnings_flat.sort(key=lambda t: -t[0])
    flat: list[dict] = [ev for _s, ev in earnings_flat[:cap]]
    # Append macro entries (date-sorted) without exceeding cap.
    macro_flat = [m for d in days_out for m in d["macro"]]
    other_flat = [o for d in days_out for o in d["other"]]
    for m in macro_flat:
        if len(flat) >= cap:
            break
        flat.append(m)
    for o in other_flat:
        if len(flat) >= cap:
            break
        flat.append(o)

    # ── 6. Status / last_updated ───────────────────────────────────────────
    last_updated = max(last_updated_candidates) if last_updated_candidates else None
    has_any = any(d["earnings"] or d["macro"] or d["other"] for d in days_out)
    if has_any:
        status = "ready"
    elif last_updated:
        status = "stale"
    else:
        status = "empty"

    print(
        f"[top_catalysts] week={week_label} "
        f"earnings={sum(len(d['earnings']) for d in days_out)} "
        f"macro={sum(len(d['macro']) for d in days_out)} "
        f"other={sum(len(d['other']) for d in days_out)} "
        f"status={status}"
    )

    return {
        "tab":                    "top_catalysts",
        "mode":                   "weekly",
        "week":                   week_label,
        "week_start":             week_start_str,
        "week_end":               week_end_str,
        "window_mode":            window_mode,
        "days":                   days_out,
        "current_week":           flat,
        "previous_week":          [],
        "last_updated":           last_updated,
        "status":                 status,
        "macro_source_event_count": macro_source_count,
        "macro_logical_event_count": macro_logical_count,
    }
