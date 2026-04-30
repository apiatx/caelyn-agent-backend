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
    mat = (ev.get("maturity") or ev.get("indicatorName") or "").upper().replace(" ", "")
    if not mat:
        # Allow named curve/auction events through the title path.
        title = ((ev.get("title") or "")
                 + " " + (ev.get("eventName") or "")).lower()
        return any(k in title for k in ("auction", "treasury", "curve", "yield"))
    return any(k in mat for k in _TREASURY_KEY_MATURITIES)


_HARD_FILTERS = {
    "dividends":         _hard_filter_securities,
    "ipos":              _hard_filter_securities,
    "splits":            _hard_filter_securities,
    "economic_releases": _hard_filter_economic,
    "treasury_macro":    _hard_filter_treasury,
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
