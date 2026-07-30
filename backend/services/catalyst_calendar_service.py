"""
Catalyst Calendar service — multi-tab market event calendar.

Primary data source: FMP Stable API (Starter plan).
No LLM calls. Deterministic API + normalisation + filtering.

Tabs (upcoming): earnings_dates, dividends, ipos, splits,
                 economic_releases, treasury_macro
Tabs (recent):   recent_earnings, sec_filings, analyst_ratings,
                 insider_transactions

Design
──────
• Self-contained FMP client (same pattern as stock_compare_service).
• Uses shared in-memory TTL cache from data.cache.
• Errors per tab are captured and returned as warnings (status=partial).
• Profile enrichment is batched per symbol and cached 24 h.
• All normalised events share the same schema.
• Importance scoring is deterministic (no LLM).
"""
from __future__ import annotations

import asyncio
import hashlib
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx

from data.cache import cache
from services.api_audit import (
    fmp_force_429, record_call, record_request,
    get_total_calls, get_cache_counts,
)

# ── Endpoint → feature label map (used for audit telemetry) ──────────────────
_ENDPOINT_FEATURE: dict[str, str] = {
    "earnings-calendar":    "earnings",
    "dividends-calendar":   "dividends",
    "ipos-calendar":        "ipos",
    "splits-calendar":      "splits",
    "economic-calendar":    "economic",
    "treasury-rates":       "treasury",
    "economic-indicators":  "economic",
    "sec-filings":          "sec_filings",
    "ratings-snapshot":     "analyst_ratings",
    "insider-trading":      "insider",
    "profile":              "profile_enrichment",
}

# ── Constants ─────────────────────────────────────────────────────────────────

FMP_STABLE = "https://financialmodelingprep.com/stable"

_TTL_EARNINGS    = 6  * 3600   # 6 h
_TTL_DIVIDENDS   = 12 * 3600   # 12 h
_TTL_IPO         = 12 * 3600   # 12 h
_TTL_SPLITS      = 12 * 3600   # 12 h
_TTL_ECONOMIC    = 6  * 3600   # 6 h
_TTL_TREASURY    = 4  * 3600   # 4 h
_TTL_SEC         = 2  * 3600   # 2 h
_TTL_ANALYST     = 6  * 3600   # 6 h
_TTL_INSIDER     = 4  * 3600   # 4 h
_TTL_PROFILE     = 24 * 3600   # 24 h

# Max concurrent FMP profile HTTP calls.  FMP Starter allows ~10 req/s;
# 8 concurrent keeps us well inside the limit while staying fast.
_PROFILE_CONCURRENCY = 8
# Lazy-initialised per event-loop (avoids SemaphoreError on module import)
_profile_sem: asyncio.Semaphore | None = None

def _get_profile_sem() -> asyncio.Semaphore:
    """Return (or create) the module-level profile-fetch semaphore."""
    global _profile_sem
    if _profile_sem is None:
        _profile_sem = asyncio.Semaphore(_PROFILE_CONCURRENCY)
    return _profile_sem

# Default date windows
_UPCOMING_DAYS  = 60
_RECENT_DAYS    = 30
_MACRO_BACK     = 14
_MACRO_AHEAD    = 45

# Recent mode: how many days back to look per tab (spec-defined defaults)
_RECENT_BACK: dict[str, int] = {
    "earnings_dates":    30,
    "dividends":         60,
    "ipos":              90,
    "splits":            90,
    "economic_releases": 30,
    "treasury_macro":    30,
}

# eventType normalisation for recent mode (per spec)
_RECENT_EVENT_TYPES: dict[str, str] = {
    "earnings_dates":    "earnings_report",
    "dividends":         "dividend",
    "ipos":              "ipo",
    "splits":            "stock_split",
    "economic_releases": "economic_release",
    "treasury_macro":    "treasury_rate",
}

# Importance keywords for economic events
_HIGH_IMPACT_ECON = {
    "CPI", "PCE", "FOMC", "Federal Reserve", "Interest Rate", "NFP",
    "Non-Farm Payroll", "Nonfarm", "GDP", "Unemployment", "Initial Jobless",
    "Retail Sales", "PPI", "PMI", "ISM Manufacturing", "ISM Services",
    "Consumer Price", "Producer Price", "Durable Goods", "Treasury",
    "Inflation", "Jobs Report", "Federal Funds",
}

# High-importance SEC form types
_HIGH_SEC_FORMS = {"8-K", "10-K", "10-Q", "4", "SC 13D", "SC 13G", "DEF 14A"}
_MED_SEC_FORMS  = {"S-1", "S-3", "424B", "6-K", "20-F", "11-K"}

# Market cap bucket thresholds (USD)
_MC_MEGA   = 200_000_000_000
_MC_LARGE  =  10_000_000_000
_MC_MID    =   2_000_000_000
_MC_SMALL  =     300_000_000

ALL_TABS = [
    "earnings_dates", "dividends", "ipos", "splits",
    "economic_releases", "treasury_macro",
    "recent_earnings", "sec_filings", "analyst_ratings", "insider_transactions",
]

# ── FMP client ────────────────────────────────────────────────────────────────

class CatalystFMP:
    """Thin FMP Stable API client for Catalyst Calendar endpoints."""

    def __init__(self, api_key: str):
        self._key = api_key

    async def _get(
        self,
        endpoint: str,
        params: dict,
        cache_key: str,
        ttl: int,
        ticker: Optional[str] = None,
    ) -> Any:
        feature = _ENDPOINT_FEATURE.get(endpoint, endpoint)

        hit = cache.get(cache_key)
        if hit is not None:
            record_call(
                provider="fmp", endpoint=endpoint,
                page="calendar", feature=feature,
                cache_status="hit", http_status=None,
                elapsed_ms=0.0, ticker=ticker,
            )
            return hit

        # ── FMP_FORCE_429 simulation ──────────────────────────────────────
        if fmp_force_429():
            record_call(
                provider="fmp", endpoint=endpoint,
                page="calendar", feature=feature,
                cache_status="miss", http_status=429,
                elapsed_ms=0.0, ticker=ticker,
                success=False, error="FMP_FORCE_429 simulation",
            )
            print(f"[catalyst] FMP_FORCE_429 — simulating 429 for {endpoint}")
            return []

        p = dict(params)
        p["apikey"] = self._key
        t0 = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=12.0) as client:
                resp = await client.get(f"{FMP_STABLE}/{endpoint}", params=p)
            ms = int((time.monotonic() - t0) * 1000)
            status = resp.status_code
            if status not in (200, 201):
                print(f"[catalyst] FMP {endpoint} status={status} ms={ms}")
                record_call(
                    provider="fmp", endpoint=endpoint,
                    page="calendar", feature=feature,
                    cache_status="miss", http_status=status,
                    elapsed_ms=ms, ticker=ticker,
                    success=False, error=f"HTTP {status}",
                )
                return []
            result = resp.json()
            rows = len(result) if isinstance(result, list) else (1 if result else 0)
            print(f"[catalyst] FMP {endpoint} status={status} rows={rows} ms={ms}")
            if result:
                cache.set(cache_key, result, ttl)
            record_call(
                provider="fmp", endpoint=endpoint,
                page="calendar", feature=feature,
                cache_status="miss", http_status=status,
                elapsed_ms=ms, ticker=ticker,
            )
            return result
        except Exception as e:
            ms = int((time.monotonic() - t0) * 1000)
            print(f"[catalyst] FMP {endpoint} error={e} ms={ms}")
            record_call(
                provider="fmp", endpoint=endpoint,
                page="calendar", feature=feature,
                cache_status="miss", http_status=None,
                elapsed_ms=ms, ticker=ticker,
                success=False, error=str(e)[:120],
            )
            return []

    # ── Calendar endpoints ─────────────────────────────────────────────────

    async def earnings_calendar(self, from_date: str, to_date: str) -> list:
        """
        Fetch FMP earnings-calendar in 7-day chunks.

        Why chunking?
        FMP's earnings-calendar endpoint caps responses at 4 000 rows and,
        for wide date ranges, does NOT return them in ascending date order.
        A 60-day window can return 4 000 events starting from week 3, silently
        omitting weeks 1–2.  Splitting into ≤7-day slices keeps each call well
        under the cap (≤750 events/week) so every day in the range is covered.

        Cache strategy
        ──────────────
        • Each 7-day slice is cached independently (key: cat:earn:v5:chunk:{s}:{e}).
        • The merged result for the full range is also cached (key: cat:earn:v5:{f}:{t})
          so repeated requests for the same range are instant.
        • Key prefix v5 to invalidate any stale v4 single-call entries.
        """
        master_ck = f"cat:earn:v5:{from_date}:{to_date}"
        hit = cache.get(master_ck)
        if hit is not None:
            return hit if isinstance(hit, list) else []

        # Build weekly date chunks
        try:
            start = datetime.strptime(from_date, "%Y-%m-%d").date()
            end   = datetime.strptime(to_date,   "%Y-%m-%d").date()
        except ValueError:
            return []

        chunks: list[tuple[str, str]] = []
        cur = start
        while cur <= end:
            chunk_end = min(cur + timedelta(days=6), end)
            chunks.append((cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
            cur = chunk_end + timedelta(days=1)

        async def _fetch_chunk(f: str, t: str) -> list:
            ck = f"cat:earn:v5:chunk:{f}:{t}"
            hit = cache.get(ck)
            if hit is not None:
                return hit if isinstance(hit, list) else []
            rows = await self._get(
                "earnings-calendar", {"from": f, "to": t}, ck, _TTL_EARNINGS
            )
            return rows if isinstance(rows, list) else []

        results = await asyncio.gather(*[_fetch_chunk(f, t) for f, t in chunks])

        # Merge and deduplicate (symbol + date is the natural unique key)
        seen: set[tuple[str, str]] = set()
        merged: list[dict] = []
        for chunk_rows in results:
            for row in chunk_rows:
                key = (row.get("symbol", ""), row.get("date", ""))
                if key not in seen:
                    seen.add(key)
                    merged.append(row)

        # Sort merged result by date ASC for consistent downstream processing
        merged.sort(key=lambda r: r.get("date", ""))

        print(
            f"[catalyst] earnings_calendar chunks={len(chunks)} "
            f"total_rows={len(merged)} range={from_date}→{to_date}"
        )
        if merged:
            cache.set(master_ck, merged, _TTL_EARNINGS)
        return merged

    async def dividends_calendar(self, from_date: str, to_date: str) -> list:
        ck = f"cat:div:{from_date}:{to_date}"
        d  = await self._get("dividends-calendar", {"from": from_date, "to": to_date}, ck, _TTL_DIVIDENDS)
        return d if isinstance(d, list) else []

    async def ipo_calendar(self, from_date: str, to_date: str) -> list:
        """
        FMP stable endpoint is /stable/ipos-calendar (note: 'ipos', not 'ipo').
        Date params work for upcoming windows; for broad/recent windows FMP may
        return the full dataset — caller should filter locally by date.
        """
        ck = f"cat:ipos:{from_date}:{to_date}"
        d  = await self._get("ipos-calendar", {"from": from_date, "to": to_date}, ck, _TTL_IPO)
        return d if isinstance(d, list) else []

    async def splits_calendar(self, from_date: str, to_date: str) -> list:
        ck = f"cat:split:{from_date}:{to_date}"
        d  = await self._get("splits-calendar", {"from": from_date, "to": to_date}, ck, _TTL_SPLITS)
        return d if isinstance(d, list) else []

    async def economic_calendar(self, from_date: str, to_date: str) -> list:
        ck = f"cat:econ:{from_date}:{to_date}"
        d  = await self._get("economic-calendar", {"from": from_date, "to": to_date}, ck, _TTL_ECONOMIC)
        return d if isinstance(d, list) else []

    async def treasury_rates(self) -> list:
        ck = "cat:treasury:latest"
        d  = await self._get("treasury-rates", {}, ck, _TTL_TREASURY)
        return d if isinstance(d, list) else []

    async def economics_indicators(self, name: str = "GDP") -> list:
        ck = f"cat:econ_ind:{name}"
        d  = await self._get("economic-indicators", {"name": name}, ck, _TTL_TREASURY)
        return d if isinstance(d, list) else []

    async def sec_filings_by_symbol(self, symbol: str, limit: int = 10) -> list:
        """Per-symbol SEC filings. Returns empty if not on this plan."""
        ck = f"cat:sec:{symbol}:{limit}"
        d  = await self._get("sec-filings", {"symbol": symbol, "limit": limit}, ck, _TTL_SEC)
        return d if isinstance(d, list) else []

    async def ratings_snapshot(self, symbol: str) -> dict:
        """Per-symbol analyst ratings snapshot (Starter-compatible)."""
        ck = f"cat:rating:{symbol}"
        d  = await self._get("ratings-snapshot", {"symbol": symbol}, ck, _TTL_ANALYST)
        if isinstance(d, list) and d:
            cache.set(ck, d[0], _TTL_ANALYST)
            return d[0]
        return {}

    async def insider_trading_by_symbol(self, symbol: str, limit: int = 10) -> list:
        """Per-symbol insider trading. Returns empty if not on this plan."""
        ck = f"cat:insider:{symbol}:{limit}"
        d  = await self._get("insider-trading", {"symbol": symbol, "limit": limit}, ck, _TTL_INSIDER)
        return d if isinstance(d, list) else []

    async def upgrades_downgrades(self, limit: int = 50) -> list:
        """Bulk upgrades/downgrades — not available on Starter; returns empty."""
        return []

    async def insider_trading(self, limit: int = 50) -> list:
        """Bulk insider trading feed — not available on Starter; returns empty."""
        return []

    async def company_profile(self, symbol: str) -> dict:
        ck = f"fmp:company_profile:v1:{symbol}"
        hit = cache.get(ck)
        if hit is not None:
            return hit
        d = await self._get("profile", {"symbol": symbol}, ck, _TTL_PROFILE)
        if isinstance(d, list) and d:
            row = d[0]
            cache.set(ck, row, _TTL_PROFILE)
            return row
        return {}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _date_offset(days: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%d")


_DATE_RE = __import__("re").compile(r"^\d{4}-\d{2}-\d{2}$")

def _parse_date_time(raw: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Split a raw FMP date string into (date_part, time_part).

    FMP returns:
      - YYYY-MM-DD              → ("2026-04-13", None)
      - "YYYY-MM-DD HH:MM:SS"   → ("2026-04-13", "HH:MM:SS")
      - None / ""               → (None, None)  — caller should skip the event

    Always returns date_part in YYYY-MM-DD or None if unparseable.
    """
    if not raw:
        return None, None
    raw = str(raw).strip()
    if " " in raw:
        parts = raw.split(" ", 1)
        d, t = parts[0], parts[1]
    elif "T" in raw:
        parts = raw.split("T", 1)
        d, t = parts[0], parts[1].split(".")[0].split("Z")[0]
    else:
        d, t = raw, None
    if not _DATE_RE.match(d):
        return None, None
    return d, (t if t else None)


def _mc_bucket(mc: Optional[float]) -> str:
    if mc is None:
        return "unknown"
    if mc >= _MC_MEGA:   return "mega"
    if mc >= _MC_LARGE:  return "large"
    if mc >= _MC_MID:    return "mid"
    if mc >= _MC_SMALL:  return "small"
    return "micro"


def _safe(v, t=float):
    try:
        return t(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


def _event_id(event_type: str, symbol: Optional[str], date: str, extra: str = "") -> str:
    raw = f"{event_type}:{symbol or ''}:{date}:{extra}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def _score_importance(
    event_type: str,
    mc_bucket: str,
    symbol: Optional[str],
    title: str,
    watchlist_syms: set,
    portfolio_syms: set,
    form_type: Optional[str] = None,
    transaction_value: Optional[float] = None,
    action: Optional[str] = None,
) -> str:
    """Deterministic importance scoring."""
    # Portfolio/watchlist match is always high
    if symbol and (symbol in portfolio_syms or symbol in watchlist_syms):
        return "high"

    # Mega / large cap
    if mc_bucket in ("mega", "large"):
        if event_type in ("earnings_dates", "recent_earnings", "earnings_report",
                          "dividends", "dividend"):
            return "high"

    # Treasury rates: scored by maturity only — NOT by _HIGH_IMPACT_ECON keywords
    # (title "Treasury Yield Snapshot" contains "Treasury" which would falsely score high)
    if event_type in ("treasury_rate", "treasury_macro"):
        if any(m in title for m in ("10Y", "2Y", "30Y")):
            return "high"
        return "medium"

    # Economic release keywords (both old plural and new singular forms)
    if event_type in ("economic_releases", "economic_release", "macro_indicator"):
        t_upper = title.upper()
        if any(kw.upper() in t_upper for kw in _HIGH_IMPACT_ECON):
            return "high"
        return "medium"

    # SEC filings
    if event_type == "sec_filings":
        ft = (form_type or "").upper()
        if ft in _HIGH_SEC_FORMS:
            return "high"
        if ft in _MED_SEC_FORMS:
            return "medium"
        return "low"

    # Analyst ratings
    if event_type == "analyst_ratings":
        act = (action or "").lower()
        if mc_bucket in ("mega", "large"):
            return "high"
        if "upgrade" in act or "downgrade" in act or "initiated" in act:
            return "medium"
        return "low"

    # Insider transactions
    if event_type == "insider_transactions":
        tv = abs(transaction_value or 0)
        if tv >= 1_000_000:
            return "high"
        if tv >= 250_000:
            return "medium"
        return "low"

    # IPOs of larger companies (both old "ipos" and new "ipo")
    if event_type in ("ipos", "ipo"):
        if mc_bucket in ("mega", "large", "mid"):
            return "high"
        return "medium"

    # Mid-cap earnings
    if event_type in ("earnings_dates", "recent_earnings", "earnings_report"):
        if mc_bucket in ("mega", "large"):
            return "high"
        if mc_bucket == "mid":
            return "medium"
        return "low"

    return "low"


# ── Macro signal classification helpers ──────────────────────────────────────
# These add deterministic event_family, signal_tier, and signal_reason to
# economic and Treasury calendar events so the frontend can distinguish
# genuinely market-moving events from routine noise.

_EVENT_FAMILY_TIERS: dict[str, str] = {
    "fomc_decision":     "critical",
    "cpi":               "major",
    "ppi":               "major",
    "pce":               "major",
    "eci":               "major",
    "payrolls":          "major",
    "fomc_minutes":      "major",
    "gdp":               "major",
    "treasury_auction":  "major",
    "fed_chair_speech":  "secondary",
    "fed_speech":        "secondary",
    "unemployment":      "secondary",
    "jobless_claims":    "secondary",
    "ism":               "secondary",
    "pmi":               "secondary",
    "retail_sales":      "secondary",
    "consumer_sentiment":"secondary",
    "housing":           "secondary",
    "other_us":          "secondary",
    "treasury_rate":     "context",
    "treasury_snapshot": "context",
    "foreign":           "context",
}

_EVENT_FAMILY_REASONS: dict[str, str] = {
    "fomc_decision":     "Scheduled FOMC rate decision",
    "fomc_minutes":      "FOMC meeting minutes",
    "fed_chair_speech":  "Fed Chair policy remarks",
    "fed_speech":        "Federal Reserve speech",
    "cpi":               "Major consumer inflation release",
    "ppi":               "Producer price inflation release",
    "pce":               "Fed-preferred inflation measure",
    "eci":               "Quarterly wage inflation measure",
    "payrolls":          "Monthly payroll and labor-market release",
    "unemployment":      "Unemployment rate release",
    "jobless_claims":    "Weekly jobless claims",
    "gdp":               "Gross domestic product release",
    "ism":               "ISM business survey",
    "pmi":               "Purchasing managers index",
    "retail_sales":      "Retail sales release",
    "consumer_sentiment":"Consumer sentiment survey",
    "housing":           "Housing market data",
    "treasury_auction":  "Treasury auction",
    "treasury_rate":     "Routine Treasury yield observation",
    "treasury_snapshot": "Treasury yield snapshot",
    "other_us":          "US economic release",
    "foreign":           "Foreign macro release",
}


def _classify_event_family(
    event_type: str,
    title: str,
    event_name: str,
    country: str,
    maturity: str | None = None,
    indicator_name: str | None = None,
) -> str:
    """
    Classify a calendar event into a canonical family.

    Precedence (most specific match runs first):
      1. Treasury rate / snapshot (determined by eventType + title)
      2. Treasury auction keyword
      3. Non-US country → foreign
      4. FOMC decision, minutes, Fed chair, Fed speech
      5. Major indicators: CPI, PPI, PCE, ECI, payrolls, GDP
      6. Secondary: unemployment, jobless claims, ISM, PMI, retail sales,
         consumer sentiment, housing
      7. Catch-all: other_us for US, foreign for non-US
    """
    bag = " ".join(filter(None, [title, event_name, indicator_name])).lower()

    # 1. Treasury rate observations (eventType already set by fetcher)
    if event_type == "treasury_rate":
        if "treasury yield snapshot" in bag:
            return "treasury_snapshot"
        return "treasury_rate"

    # 2. Treasury auctions (keyword before country — US-specific concept)
    if re.search(r"\btreasury\s+(?:auction|bill|note|bond)\b", bag):
        return "treasury_auction"

    # 3. Non-US events
    if country and country.upper() != "US":
        return "foreign"

    # 4. FOMC / Fed (most specific first)
    if re.search(r"\binterest\s+rate\s+decision\b", bag):
        return "fomc_decision"

    if re.search(r"\bfomc\b", bag) and re.search(r"\bminutes\b", bag):
        return "fomc_minutes"

    if re.search(r"\bfed\s+chair\b", bag) or re.search(r"\bpowell\b", bag):
        return "fed_chair_speech"

    if re.search(r"\bfed\b", bag) and re.search(r"\b(?:speech|speaks|remarks|speaking)\b", bag):
        return "fed_speech"

    # 5. Major indicators
    if re.search(r"\bcpi\b", bag) or re.search(r"\bconsumer\s+price\s+index\b", bag):
        return "cpi"

    if re.search(r"\bppi\b", bag) or re.search(r"\bproducer\s+price\s+index\b", bag):
        return "ppi"

    if re.search(r"\bpce\b", bag) or re.search(r"\bpersonal\s+consumption\s+expenditure\b", bag):
        return "pce"

    if re.search(r"\bemployment\s+cost\s+index\b", bag):
        return "eci"

    if re.search(r"\bnfp\b", bag) or re.search(r"\bnon-?farm\s+payroll", bag) or re.search(r"\bpayrolls?\s", bag):
        return "payrolls"

    if re.search(r"\bgdp\b", bag):
        return "gdp"

    # 6. Secondary indicators
    if re.search(r"\bunemployment\s+rate\b", bag):
        return "unemployment"

    if re.search(r"\b(?:initial\s+)?jobless\s+claims\b", bag) or re.search(r"\binitial\s+claims\b", bag):
        return "jobless_claims"

    if re.search(r"\bism\b", bag):
        return "ism"

    if re.search(r"\bpmi\b", bag):
        return "pmi"

    if re.search(r"\bretail\s+sales\b", bag):
        return "retail_sales"

    if re.search(r"\bconsumer\s+(?:sentiment|confidence|expectations)\b", bag) or re.search(r"\bmichigan\b", bag):
        return "consumer_sentiment"

    if re.search(r"\b(?:housing|home\s+sales|building\s+permits)", bag):
        return "housing"

    return "other_us"


def _compute_signal_tier(family: str) -> str:
    """Return the signal importance tier for a given event family."""
    return _EVENT_FAMILY_TIERS.get(family, "context")


def _compute_signal_reason(family: str, country: str = "") -> str:
    """Return a concise deterministic explanation for the event family."""
    base = _EVENT_FAMILY_REASONS.get(family, "Economic release")
    if family == "foreign" and country:
        return f"Foreign macro release ({country})"
    return base


# Generic placeholder strings that must never appear as display_title.
# If the resolution chain produces one of these, we escalate to richer fields.
_GENERIC_DISPLAY_TITLES: frozenset[str] = frozenset({
    "macro",
    "m",
    "economic release",
    "economic releases",
    "treasury / macro",
    "treasury/macro",
    "category",
    "indicator",
    "event",
    "release",
    "n/a",
    "",
})


def _build_event(**kw) -> dict:
    """Build a normalized event dict with all schema fields."""
    _title = kw.get("title", "")
    _display_title = (
        kw.get("display_title")
        or _title
        or kw.get("eventName")
        or kw.get("companyName")
        or ""
    )
    # Safety hardening: if the resolved display_title is a generic placeholder,
    # escalate through richer fields before giving up.  The priority is:
    #   indicatorName → indicator_name → event_name → eventName → companyName
    # Only falls back to the original value if nothing better exists.
    if _display_title.strip().lower() in _GENERIC_DISPLAY_TITLES:
        _display_title = (
            kw.get("indicatorName")
            or kw.get("indicator_name")
            or kw.get("event_name")
            or kw.get("eventName")
            or kw.get("companyName")
            or _display_title
        )
    return {
        "id":                 kw.get("id", ""),
        "symbol":             kw.get("symbol"),
        "companyName":        kw.get("companyName"),
        "eventType":          kw.get("eventType", ""),
        "eventLabel":         kw.get("eventLabel"),
        "eventCategory":      kw.get("eventCategory", "upcoming"),
        "display_title":      _display_title,
        "title":              _title,
        "subtitle":           kw.get("subtitle"),
        "keyDetails":         kw.get("keyDetails"),
        "date":               kw.get("date", ""),
        "time":               kw.get("time"),
        "period":             kw.get("period"),
        "source":             "fmp",
        "sector":             kw.get("sector"),
        "industry":           kw.get("industry"),
        "marketCap":          kw.get("marketCap"),
        "marketCapBucket":    kw.get("marketCapBucket", "unknown"),
        "importance":         kw.get("importance", "low"),
        "raw":                kw.get("raw", {}),
        # earnings
        "epsEstimated":       kw.get("epsEstimated"),
        "epsActual":          kw.get("epsActual"),
        "revenueEstimated":   kw.get("revenueEstimated"),
        "revenueActual":      kw.get("revenueActual"),
        "surprise":           kw.get("surprise"),
        "surprisePercent":    kw.get("surprisePercent"),
        # dividend
        "dividend":           kw.get("dividend"),
        "recordDate":         kw.get("recordDate"),
        "paymentDate":        kw.get("paymentDate"),
        "declarationDate":    kw.get("declarationDate"),
        "exDividendDate":     kw.get("exDividendDate"),
        # IPO
        "exchange":           kw.get("exchange"),
        "shares":             kw.get("shares"),
        "priceRange":         kw.get("priceRange"),
        "offerPrice":         kw.get("offerPrice"),
        # split
        "splitRatio":         kw.get("splitRatio"),
        "numerator":          kw.get("numerator"),
        "denominator":        kw.get("denominator"),
        # SEC filing
        "formType":           kw.get("formType"),
        "filingUrl":          kw.get("filingUrl"),
        # analyst
        "ratingFrom":         kw.get("ratingFrom"),
        "ratingTo":           kw.get("ratingTo"),
        "action":             kw.get("action"),
        "analystFirm":        kw.get("analystFirm"),
        # company identity (populated by profile enrichment)
        "logo":               kw.get("logo"),
        "image":              kw.get("image"),
        # insider
        "insiderName":        kw.get("insiderName"),
        "transactionType":    kw.get("transactionType"),
        "sharesTraded":       kw.get("sharesTraded"),
        "transactionValue":   kw.get("transactionValue"),
        # macro / economic
        "actual":             kw.get("actual"),
        "estimate":           kw.get("estimate"),
        "previous":           kw.get("previous"),
        "country":            kw.get("country"),
        "eventName":          kw.get("eventName"),
        # treasury / macro indicators
        "maturity":           kw.get("maturity"),
        "value":              kw.get("value"),
        "previousValue":      kw.get("previousValue"),
        "indicatorName":      kw.get("indicatorName"),
        # macro signal metadata (added February 2026)
        "event_family":       kw.get("event_family"),
        "signal_tier":        kw.get("signal_tier"),
        "signal_reason":      kw.get("signal_reason"),
    }


# ── Profile enrichment ────────────────────────────────────────────────────────

async def _enrich_profiles(
    symbols: list[str],
    fmp: CatalystFMP,
    max_live_fetches: Optional[int] = None,
) -> dict[str, dict]:
    """
    Batch-fetch company profiles for a list of symbols.
    Returns {symbol: {companyName, logo, image, sector, industry, marketCap, …}}.

    Design
    ──────
    • All unique symbols are enriched — no hard cap.
    • Concurrent HTTP calls are throttled by _PROFILE_CONCURRENCY semaphore
      so we stay inside FMP Starter rate limits even for 100+ symbols.
    • Cache-hits skip the semaphore entirely (only uncached symbols go live).
    • Per-symbol cache TTL = 24 h (key: fmp:company_profile:v1:{symbol}).
    • Any per-symbol failure is silently swallowed — caller gets empty dict.

    max_live_fetches
      When set, limits the number of *live* (uncached) HTTP fetches per call.
      Cached symbols are always returned without limit.  Symbols beyond the
      cap return an empty profile (companyName will fall back to ticker); they
      will be enriched on the next cache-warm request.
      Use this for large calendar requests (1000+ symbols) where enriching
      every cold-cache symbol would take minutes.
    """
    unique = list(dict.fromkeys(s for s in symbols if s))
    if not unique:
        return {}

    # Pre-scan: separate already-cached from uncached to respect max_live_fetches
    ck_base = "fmp:company_profile:v1:"
    cached_syms: list[str] = []
    uncached_syms: list[str] = []
    for sym in unique:
        if cache.get(f"{ck_base}{sym}") is not None:
            cached_syms.append(sym)
        else:
            uncached_syms.append(sym)

    # Cap live fetches to avoid overwhelming FMP on cold cache
    if max_live_fetches is not None and len(uncached_syms) > max_live_fetches:
        live_syms   = uncached_syms[:max_live_fetches]
        skipped     = uncached_syms[max_live_fetches:]
        print(
            f"[enrich_profiles] cap={max_live_fetches} cached={len(cached_syms)} "
            f"live={len(live_syms)} skipped={len(skipped)} (will warm on next request)"
        )
    else:
        live_syms = uncached_syms
        skipped   = []

    active_syms = cached_syms + live_syms
    sem = _get_profile_sem()

    async def _fetch_one(sym: str) -> tuple[str, dict]:
        # Fast path: already in cache (no semaphore needed)
        ck = f"{ck_base}{sym}"
        cached = cache.get(ck)
        if cached is not None:
            return sym, cached
        # Slow path: live HTTP fetch under semaphore
        async with sem:
            return sym, await fmp.company_profile(sym)

    pair_results = await asyncio.gather(
        *[_fetch_one(s) for s in active_syms],
        return_exceptions=True,
    )

    _fmp_logo_base = "https://financialmodelingprep.com/image-stock"

    enriched: dict[str, dict] = {}
    for item in pair_results:
        if isinstance(item, Exception):
            continue
        sym, res = item
        if not res:
            # No profile — still provide a predictable logo URL
            enriched[sym] = {"logo": f"{_fmp_logo_base}/{sym}.png",
                             "image": f"{_fmp_logo_base}/{sym}.png"}
            continue
        mc = _safe(res.get("mktCap") or res.get("marketCap"))
        # FMP profile returns the logo URL in the "image" field;
        # fall back to the predictable CDN URL so logos always render.
        logo_url = res.get("image") or res.get("logo") or f"{_fmp_logo_base}/{sym}.png"
        enriched[sym] = {
            "companyName":       res.get("companyName") or res.get("name") or sym,
            "sector":            res.get("sector"),
            "industry":          res.get("industry"),
            "marketCap":         mc,
            "marketCapBucket":   _mc_bucket(mc),
            "logo":              logo_url,
            "image":             logo_url,
            "price":             res.get("price"),
            "changesPercentage": res.get("changePercentage") or res.get("changesPercentage") or res.get("changes"),
        }
    return enriched


def _apply_enrichment(events: list[dict], enriched: dict[str, dict]) -> list[dict]:
    for ev in events:
        sym = ev.get("symbol")
        if sym and sym in enriched:
            info = enriched[sym]
            # Company identity — profile always wins over the raw calendar name
            # because the earnings calendar row's "name" field is often just the ticker.
            enriched_name = info.get("companyName")
            ev["companyName"] = enriched_name or ev.get("companyName")
            ev["sector"]          = ev.get("sector") or info.get("sector")
            ev["industry"]        = ev.get("industry") or info.get("industry")
            # Logo / image — profile wins over CDN fallback already set on the event
            profile_logo = info.get("logo") or info.get("image")
            if profile_logo:
                ev["logo"]  = profile_logo
                ev["image"] = profile_logo
            elif not ev.get("logo"):
                _fmp_logo_base = "https://financialmodelingprep.com/image-stock"
                ev["logo"]  = f"{_fmp_logo_base}/{sym}.png"
                ev["image"] = ev["logo"]
            if ev.get("marketCap") is None:
                ev["marketCap"]       = info.get("marketCap")
                ev["marketCapBucket"] = info.get("marketCapBucket", "unknown")
            if ev.get("price") is None:
                ev["price"] = info.get("price")
            if ev.get("changesPercentage") is None:
                ev["changesPercentage"] = info.get("changesPercentage")
            # For earnings events: refresh title to use enriched company name
            # Preserve any suffix after "Earnings" (e.g. " Report (Beat)", " Report (Miss)")
            if ev.get("eventType") in ("earnings_dates", "earnings_report") and enriched_name:
                display = ev["companyName"] or sym
                current_title = ev.get("title", "")
                suffix = ""
                if " Earnings" in current_title:
                    suffix = current_title.split(" Earnings", 1)[1]
                ev["title"] = f"{display} Earnings{suffix}"
    return events


# ── Tradier live-quote enrichment for calendar events ─────────────────────────

async def _enrich_tradier_quotes(
    symbols: list[str],
    max_syms: int = 30,
) -> dict[str, dict]:
    """Batch-fetch Tradier live quotes via the shared TradierProvider.

    Returns {SYMBOL: {price, changesPercentage, volume, bid, ask,
                      quote_source, quote_is_stale}}.
    Only US equity symbols (no ":") are submitted. Result is also stored in
    per-symbol LKG cache (72 h) so the calendar degrades gracefully when
    Tradier is unavailable.  Falls back to shared provider LKG when live data
    is absent.

    Never raises — returns empty dict on any failure.
    """
    import time as _time_mod
    _now_ts  = _time_mod.time()
    _LKG_TTL = 72 * 3600
    _LKG_PFX = "cal:tradier_lkg:"

    us_syms = [s for s in symbols if s and ":" not in s][:max_syms]
    if not us_syms:
        return {}

    def _sf(v):
        try:
            return float(v) if v not in (None, "", "-") else None
        except (TypeError, ValueError):
            return None

    out: dict[str, dict] = {}
    try:
        import main as _main  # type: ignore
        _ds = getattr(_main, "data_service", None)
        if _ds is not None and getattr(_ds, "tradier", None):
            from data.tradier_budget import lane as _cat_lane
            with _cat_lane("quotes"):
                raw_quotes = await _ds.tradier.get_quotes(us_syms)
            for q in (raw_quotes or []):
                sym = (q.get("symbol") or "").upper()
                if not sym:
                    continue
                last = _sf(q.get("last"))
                if last is None:
                    continue
                row = {
                    "price":             last,
                    "changesPercentage": _sf(q.get("change_percentage")),
                    "volume":            q.get("volume"),
                    "bid":               _sf(q.get("bid")),
                    "ask":               _sf(q.get("ask")),
                    "quote_source":      "tradier",
                    "quote_is_stale":    False,
                    "quote_cached_at":   _now_ts,
                }
                out[sym] = row
                cache.set(f"{_LKG_PFX}{sym}", row, _LKG_TTL)
            print(f"[CALENDAR_TRADIER] {len(out)} live quotes for {len(us_syms)} symbols via shared provider")
    except Exception as exc:
        print(f"[CALENDAR_TRADIER] provider error: {exc}")

    # Fall back to LKG for tickers we couldn't get live
    for sym in us_syms:
        sym_u = sym.upper()
        if sym_u not in out:
            lkg = cache.get(f"{_LKG_PFX}{sym_u}")
            if not (lkg and lkg.get("price")):
                # Also check the shared provider LKG
                lkg = cache.get(f"tradier:lkg:sym:{sym_u}")
            if lkg and lkg.get("price"):
                out[sym_u] = {**lkg, "quote_is_stale": True}

    return out


def _apply_tradier_quotes(events: list[dict], tradier: dict[str, dict]) -> list[dict]:
    """Overwrite price / changesPercentage on events with Tradier live data."""
    for ev in events:
        sym = (ev.get("symbol") or "").upper()
        if not sym or sym not in tradier:
            continue
        tq = tradier[sym]
        # Always overwrite with Tradier — it's fresher than FMP profile price
        if tq.get("price") is not None:
            ev["price"]             = tq["price"]
            ev["changesPercentage"] = tq.get("changesPercentage")
            ev["quote_source"]      = tq.get("quote_source", "tradier")
            ev["quote_is_stale"]    = tq.get("quote_is_stale", False)
    return events


# ── Tab fetchers ──────────────────────────────────────────────────────────────

def _is_polymarket_row(row: dict, title: str, sym: str) -> bool:
    """
    Safety guard: return True if a row looks like a Polymarket prediction market
    rather than a genuine stock earnings event.  The main earnings calendar must
    contain only real company earnings events sourced from FMP.

    Reject conditions (any one is sufficient):
    - row has a Polymarket-style 'source' field set to 'polymarket'
    - row carries prediction-market keys (outcomes / odds / probability)
    - title starts with a question word pattern ("Will ", "Does ", "Did ", "Is ", "Are ")
    - no stock ticker symbol is present
    - row contains a 'question' or 'slug' key (Gamma API shape)
    """
    if row.get("source") == "polymarket":
        return True
    if any(k in row for k in ("outcomes", "odds", "probability", "question", "slug")):
        return True
    title_lower = title.lower()
    question_prefixes = ("will ", "does ", "did ", "is ", "are ", "can ", "has ", "have ")
    if any(title_lower.startswith(p) for p in question_prefixes):
        return True
    if not sym:
        return True
    return False


async def _fetch_earnings_dates(
    fmp: CatalystFMP,
    from_date: str,
    to_date: str,
    watchlist: set,
    portfolio: set,
) -> list[dict]:
    _fmp_logo_base = "https://financialmodelingprep.com/image-stock"
    rows = await fmp.earnings_calendar(from_date, to_date)
    events: list[dict] = []
    poly_skipped = 0
    for row in (rows or []):
        sym   = (row.get("symbol") or "").upper()
        date  = row.get("date") or row.get("reportDate") or ""
        eps_e = _safe(row.get("epsEstimated"))
        eps_a = _safe(row.get("eps") or row.get("epsActual"))
        rev_e = _safe(row.get("revenueEstimated"))
        rev_a = _safe(row.get("revenue") or row.get("revenueActual"))
        name  = row.get("name") or row.get("companyName") or sym
        mc    = _safe(row.get("marketCap"))
        bucket = _mc_bucket(mc)
        title = f"{name} Earnings" if name != sym else f"{sym} Earnings"
        logo  = f"{_fmp_logo_base}/{sym}.png" if sym else None

        # Safety guard: skip anything that resembles a Polymarket market row
        if _is_polymarket_row(row, title, sym):
            poly_skipped += 1
            continue

        imp = _score_importance("earnings_dates", bucket, sym, name, watchlist, portfolio)
        events.append(_build_event(
            id            = _event_id("earnings_dates", sym, date),
            symbol        = sym or None,
            companyName   = name,
            logo          = logo,
            eventType     = "earnings_dates",
            eventCategory = "upcoming",
            title         = title,
            date          = date,
            time          = row.get("time"),
            period        = row.get("fiscalDateEnding") or row.get("period"),
            marketCap     = mc,
            marketCapBucket = bucket,
            importance    = imp,
            epsEstimated  = eps_e,
            epsActual     = eps_a,
            revenueEstimated = rev_e,
            revenueActual = rev_a,
            source        = "fmp",
            raw           = row,
        ))

    # Debug logging — date distribution
    date_counts: dict[str, int] = {}
    for ev in events:
        d = ev.get("date") or "unknown"
        date_counts[d] = date_counts.get(d, 0) + 1
    sorted_dates = sorted(date_counts.items())
    first_5 = sorted_dates[:5]
    last_5  = sorted_dates[-5:]
    print(
        f"[Earnings Upcoming] from={from_date} to={to_date} "
        f"raw={len(rows or [])} normalized={len(events)} poly_skipped={poly_skipped} "
        f"unique_dates={len(date_counts)} "
        f"first_dates={first_5} last_dates={last_5}"
    )
    return events


async def _fetch_dividends(
    fmp: CatalystFMP,
    from_date: str,
    to_date: str,
    watchlist: set,
    portfolio: set,
) -> list[dict]:
    """
    FMP dividends-calendar fields: symbol, date (ex-div), recordDate,
    paymentDate, declarationDate, adjDividend, dividend, yield, frequency.
    No companyName field — symbol used as display name until profile enrichment.
    """
    rows = await fmp.dividends_calendar(from_date, to_date)
    events: list[dict] = []
    missing_date = 0
    for row in (rows or []):
        sym = (row.get("symbol") or "").upper()
        if not sym:
            continue
        raw_date = (
            row.get("date")
            or row.get("exDividendDate")
            or row.get("recordDate")
            or row.get("paymentDate")
            or ""
        )
        date, _ = _parse_date_time(raw_date)
        if not date:
            missing_date += 1
            continue

        div        = _safe(row.get("dividend") or row.get("adjDividend"))
        yld        = _safe(row.get("yield"))
        frequency  = row.get("frequency") or ""
        ex_div, _  = _parse_date_time(row.get("date") or row.get("exDividendDate") or date)
        rec_date, _ = _parse_date_time(row.get("recordDate") or "")
        pay_date, _ = _parse_date_time(row.get("paymentDate") or "")
        decl_date, _ = _parse_date_time(row.get("declarationDate") or "")
        mc         = _safe(row.get("marketCap"))
        bucket     = _mc_bucket(mc)

        # title — always meaningful
        title = f"{sym} Dividend"

        # subtitle — quick descriptor
        subtitle = f"Ex-Date: {ex_div}" if ex_div else "Dividend"

        # keyDetails — pack all useful numbers
        kd_parts: list[str] = []
        if div is not None:
            kd_parts.append(f"${div:.4f}" if div < 1 else f"${div:.2f}")
        if ex_div:
            kd_parts.append(f"Ex: {ex_div}")
        if pay_date:
            kd_parts.append(f"Pay: {pay_date}")
        if yld is not None:
            kd_parts.append(f"Yield: {yld:.2f}%")
        if frequency:
            kd_parts.append(frequency)
        key_details = " · ".join(kd_parts) or None

        imp = _score_importance("dividend", bucket, sym, title, watchlist, portfolio)
        events.append(_build_event(
            id              = _event_id("dividend", sym, date),
            symbol          = sym,
            companyName     = sym,           # enriched to real name after profile fetch
            eventType       = "dividend",
            eventLabel      = "Dividend",
            eventCategory   = "upcoming",
            title           = title,
            subtitle        = subtitle,
            keyDetails      = key_details,
            date            = date,
            marketCap       = mc,
            marketCapBucket = bucket,
            importance      = imp,
            dividend        = div,
            exDividendDate  = ex_div or date,
            recordDate      = rec_date,
            paymentDate     = pay_date,
            declarationDate = decl_date,
            raw             = row,
        ))

    print(f"[catalyst:debug] dividends raw={len(rows or [])} normalized={len(events)} skipped_date={missing_date}")
    if events:
        e = events[0]
        print(f"[catalyst:debug] dividends[0]: date={e['date']} title={e['title']!r} keyDetails={e['keyDetails']!r}")
    return events


async def _fetch_ipos(
    fmp: CatalystFMP,
    from_date: str,
    to_date: str,
    watchlist: set,
    portfolio: set,
) -> list[dict]:
    """
    FMP /stable/ipos-calendar fields (confirmed):
      symbol, date, daa, company, exchange, actions, shares, priceRange, marketCap

    Key notes:
    - Correct FMP endpoint is 'ipos-calendar' (with 's'), NOT 'ipo-calendar'
    - 'company' is the name field (no separate companyName)
    - 'priceRange' is already a formatted string "4.00 - 6.00"
    - 'actions': "Expected" (upcoming), "Priced" (recent/complete), "Withdrawn"
    - FMP may return the full dataset for broad date windows → filter locally
    - Do NOT skip rows with no symbol; companyName + date is enough to emit an event
    """
    rows = await fmp.ipo_calendar(from_date, to_date)
    raw_count = len(rows or [])

    # Log request path without API key for debugging
    print(f"[catalyst:debug] ipos-calendar request: from={from_date} to={to_date} raw={raw_count}")
    if rows:
        print(f"[catalyst:debug] ipos first row keys: {list(rows[0].keys())}")
        print(f"[catalyst:debug] ipos first row: {rows[0]}")

    events: list[dict] = []
    missing_date = 0
    out_of_window = 0

    for row in (rows or []):
        sym  = (row.get("symbol") or "").upper() or None
        name = (
            row.get("company")
            or row.get("companyName")
            or row.get("name")
            or sym
            or "IPO"
        )

        # Date: 'date' field is YYYY-MM-DD in the ipos-calendar endpoint
        raw_date = (
            row.get("date")
            or row.get("ipoDate")
            or row.get("pricedDate")
            or row.get("filingDate")
            or ""
        )
        date, _ = _parse_date_time(raw_date)
        if not date:
            missing_date += 1
            continue

        # Local date window filter — FMP may return the full dataset
        if from_date and date < from_date:
            out_of_window += 1
            continue
        if to_date and date > to_date:
            out_of_window += 1
            continue

        exchange   = (row.get("exchange") or row.get("market") or "").strip()
        actions    = (row.get("actions") or "").strip()          # "Expected", "Priced", etc.
        shares_raw = _safe(row.get("shares") or row.get("totalSharesValue"))
        mc         = _safe(row.get("marketCap"))
        offer      = _safe(row.get("offerPrice") or row.get("priceOffer"))
        bucket     = _mc_bucket(mc)

        # priceRange: FMP returns "4.00 - 6.00"; add $ prefix
        pr_raw = (row.get("priceRange") or "").strip()
        if pr_raw:
            parts_pr = [p.strip() for p in pr_raw.split("-")]
            if len(parts_pr) == 2 and all(p for p in parts_pr):
                price_range = f"${parts_pr[0]}–${parts_pr[1]}"
            else:
                price_range = f"${pr_raw}"
        elif offer:
            price_range = f"${offer:.2f}"
        else:
            price_range = None

        # title
        if name and name != "IPO":
            title = f"{name} IPO"
        elif sym:
            title = f"{sym} IPO"
        else:
            title = "IPO"

        # subtitle
        subtitle = f"Exchange: {exchange}" if exchange else (actions if actions else "IPO")

        # keyDetails
        kd_parts: list[str] = []
        if exchange:
            kd_parts.append(f"Exchange: {exchange}")
        if price_range:
            kd_parts.append(f"Price: {price_range}")
        if shares_raw:
            s_fmt = (f"{shares_raw/1_000_000:.1f}M" if shares_raw >= 1_000_000
                     else f"{int(shares_raw):,}")
            kd_parts.append(f"Shares: {s_fmt}")
        if mc:
            mc_fmt = (f"${mc/1_000_000_000:.1f}B" if mc >= 1e9
                      else f"${mc/1_000_000:.0f}M")
            kd_parts.append(f"Mkt Cap: {mc_fmt}")
        if actions:
            kd_parts.append(actions)
        key_details = " · ".join(kd_parts) or "IPO scheduled"

        imp = _score_importance("ipo", bucket, sym, title, watchlist, portfolio)
        events.append(_build_event(
            id              = _event_id("ipo", sym or name, date),
            symbol          = sym,
            companyName     = name,
            eventType       = "ipo",
            eventLabel      = "IPO",
            eventCategory   = "upcoming",
            title           = title,
            subtitle        = subtitle,
            keyDetails      = key_details,
            date            = date,
            exchange        = exchange or None,
            shares          = shares_raw,
            priceRange      = price_range,
            offerPrice      = offer,
            marketCap       = mc,
            marketCapBucket = bucket,
            importance      = imp,
            raw             = row,
        ))

    print(
        f"[catalyst:debug] ipos: raw={raw_count} "
        f"normalized={len(events)} "
        f"skipped_date={missing_date} "
        f"out_of_window={out_of_window}"
    )
    if events:
        e = events[0]
        print(
            f"[catalyst:debug] ipos[0]: date={e['date']} "
            f"title={e['title']!r} "
            f"keyDetails={e['keyDetails']!r}"
        )
    return events


async def _fetch_splits(
    fmp: CatalystFMP,
    from_date: str,
    to_date: str,
    watchlist: set,
    portfolio: set,
) -> list[dict]:
    """
    FMP splits-calendar fields: symbol, date, numerator, denominator, splitType.
    numerator=3, denominator=1 → "3-for-1" (forward split).
    numerator=1, denominator=30 → "1-for-30" (reverse split).
    No companyName in FMP response — symbol used as display name.
    """
    rows = await fmp.splits_calendar(from_date, to_date)
    events: list[dict] = []
    missing_date = 0
    for row in (rows or []):
        sym = (row.get("symbol") or "").upper()
        if not sym:
            continue
        raw_date = (
            row.get("date")
            or row.get("splitDate")
            or row.get("executionDate")
            or row.get("effectiveDate")
            or ""
        )
        date, _ = _parse_date_time(raw_date)
        if not date:
            missing_date += 1
            continue

        num = row.get("numerator")
        den = row.get("denominator")
        split_type = (row.get("splitType") or "").lower()

        # Build canonical ratio string: "N-for-M"
        if num and den:
            ratio_str = f"{num}-for-{den}"
        elif row.get("ratio") or row.get("splitFactor"):
            ratio_str = str(row.get("ratio") or row.get("splitFactor"))
        else:
            ratio_str = None

        # Detect reverse split
        is_reverse = (
            "reverse" in split_type
            or (num and den and float(num) < float(den))
        )
        split_label = "Reverse Split" if is_reverse else "Stock Split"

        mc     = _safe(row.get("marketCap"))
        bucket = _mc_bucket(mc)

        # title
        title = f"{sym} {ratio_str} {split_label}" if ratio_str else f"{sym} {split_label}"

        # subtitle
        subtitle = ratio_str if ratio_str else split_label

        # keyDetails
        kd_parts: list[str] = []
        if ratio_str:
            kd_parts.append(f"Ratio: {ratio_str}")
        kd_parts.append(split_label)
        if date:
            kd_parts.append(f"Effective: {date}")
        key_details = " · ".join(kd_parts) or None

        imp = _score_importance("stock_split", bucket, sym, title, watchlist, portfolio)
        events.append(_build_event(
            id              = _event_id("stock_split", sym, date),
            symbol          = sym,
            companyName     = sym,           # no companyName from FMP splits-calendar
            eventType       = "stock_split",
            eventLabel      = split_label,
            eventCategory   = "upcoming",
            title           = title,
            subtitle        = subtitle,
            keyDetails      = key_details,
            date            = date,
            splitRatio      = ratio_str,
            numerator       = num,
            denominator     = den,
            marketCap       = mc,
            marketCapBucket = bucket,
            importance      = imp,
            raw             = row,
        ))

    print(f"[catalyst:debug] splits raw={len(rows or [])} normalized={len(events)} skipped_date={missing_date}")
    if events:
        e = events[0]
        print(f"[catalyst:debug] splits[0]: date={e['date']} title={e['title']!r} keyDetails={e['keyDetails']!r}")
    return events


async def _fetch_economic_releases(
    fmp: CatalystFMP,
    from_date: str,
    to_date: str,
) -> list[dict]:
    """
    FMP economic-calendar fields: date (datetime), country, event, currency,
    previous, estimate, actual, change, impact, changePercentage, unit.
    The 'event' field is the human-readable name (e.g. "CPI YoY (Jul)").
    FMP embeds time in the date field: "2026-04-13 01:40:00".
    """
    rows = await fmp.economic_calendar(from_date, to_date)
    events: list[dict] = []
    missing_date = 0

    _COUNTRY_NAMES: dict[str, str] = {
        "US": "United States", "JP": "Japan", "GB": "United Kingdom",
        "EU": "Eurozone", "DE": "Germany", "CN": "China", "CA": "Canada",
        "AU": "Australia", "NZ": "New Zealand", "CH": "Switzerland",
    }

    def _fmt_val(v: object, u: str) -> str:
        if v is None:
            return "—"
        try:
            fv = float(v)  # type: ignore[arg-type]
            s = f"{fv:,.2f}" if abs(fv) < 1000 else f"{fv:,.1f}"
            return f"{s}{u}" if u else s
        except (TypeError, ValueError):
            return str(v)

    for row in (rows or []):
        country   = (row.get("country") or "").upper()
        title     = (row.get("event") or row.get("name") or "").strip()
        if not title:
            continue
        raw_date  = row.get("date") or row.get("releaseDate") or ""
        date, time_val = _parse_date_time(raw_date)
        if not date:
            missing_date += 1
            continue

        impact    = (row.get("impact") or "").lower()
        actual    = row.get("actual")
        estimate  = row.get("estimate")
        previous  = row.get("previous")
        unit      = (row.get("unit") or "").strip()
        currency  = (row.get("currency") or "").upper()

        # importance
        is_high = impact == "high" or any(
            kw.lower() in title.lower() for kw in _HIGH_IMPACT_ECON
        )
        imp = "high" if is_high else ("medium" if impact == "medium" else "low")

        # subtitle
        country_label = _COUNTRY_NAMES.get(country, country) if country else "Global"
        subtitle = country_label if country else "Economic Release"

        kd_parts: list[str] = []
        if actual is not None:
            kd_parts.append(f"Actual: {_fmt_val(actual, unit)}")
        if estimate is not None:
            kd_parts.append(f"Est: {_fmt_val(estimate, unit)}")
        if previous is not None:
            kd_parts.append(f"Prev: {_fmt_val(previous, unit)}")
        if currency:
            kd_parts.append(currency)
        key_details = " · ".join(kd_parts) or None

        family = _classify_event_family("economic_release", title, title, country)
        tier   = _compute_signal_tier(family)
        reason = _compute_signal_reason(family, country)

        events.append(_build_event(
            id            = _event_id("economic_release", None, date, title[:30]),
            symbol        = "Macro",
            companyName   = title,           # event name used as "company" display
            eventType     = "economic_release",
            eventLabel    = "Economic Release",
            eventCategory = "macro",
            title         = title,
            subtitle      = subtitle,
            keyDetails    = key_details,
            date          = date,
            time          = time_val,
            importance    = imp,
            actual        = actual,
            estimate      = estimate,
            previous      = previous,
            country       = country,
            eventName     = title,
            raw           = row,
            event_family  = family,
            signal_tier   = tier,
            signal_reason = reason,
        ))

    print(f"[catalyst:debug] economic_releases raw={len(rows or [])} normalized={len(events)} skipped_date={missing_date}")
    if events:
        e = events[0]
        print(f"[catalyst:debug] econ[0]: date={e['date']} title={e['title']!r} keyDetails={e['keyDetails']!r}")
    return events


# ── Treasury maturity labels (FMP field → human label) ────────────────────────
_TREASURY_MATURITIES: list[tuple[str, str]] = [
    ("month1", "1M"), ("month2", "2M"), ("month3", "3M"), ("month6", "6M"),
    ("year1", "1Y"),  ("year2", "2Y"),  ("year3", "3Y"),  ("year5", "5Y"),
    ("year7", "7Y"),  ("year10", "10Y"), ("year20", "20Y"), ("year30", "30Y"),
]

# Key maturities to emit as individual events for the latest snapshot
_TREASURY_KEY_MATURITIES: list[tuple[str, str]] = [
    ("month1", "1M"), ("month3", "3M"), ("month6", "6M"),
    ("year2", "2Y"),  ("year5", "5Y"),  ("year10", "10Y"),
    ("year20", "20Y"), ("year30", "30Y"),
]
# Maturities that are considered "high" importance
_TREASURY_HIGH_IMP: frozenset = frozenset({"2Y", "10Y", "30Y"})


async def _fetch_treasury_macro(fmp: CatalystFMP) -> list[dict]:
    """
    FMP treasury-rates fields: date, month1-month6, year1-year30 (all yields).
    One row per trading day. Strategy:
      • Latest row → one event PER KEY MATURITY (rich individual events).
      • Historical rows (up to 29) → one SUMMARY event per date with keyDetails.
    """
    rows = await fmp.treasury_rates()
    events: list[dict] = []
    if not rows:
        print("[catalyst:debug] treasury_macro: no rows returned by FMP")
        return events

    # ── Latest row: individual maturity events ─────────────────────────────────
    latest      = rows[0]
    latest_date = latest.get("date") or _today()

    for fmp_field, label in _TREASURY_KEY_MATURITIES:
        val = latest.get(fmp_field)
        if val is None:
            continue
        try:
            val = float(val)
        except (TypeError, ValueError):
            continue

        imp     = "high" if label in _TREASURY_HIGH_IMP else "medium"
        title   = f"{label} Treasury Rate"
        kd      = f"Yield: {val:.2f}%"

        # Treasury rate observations are always context-level signal
        events.append(_build_event(
            id            = _event_id("treasury_rate", None, latest_date, label),
            symbol        = "Macro",
            companyName   = "US Treasury",
            eventType     = "treasury_rate",
            eventLabel    = "Treasury Rate",
            eventCategory = "macro",
            title         = title,
            subtitle      = f"As of {latest_date}",
            keyDetails    = kd,
            date          = latest_date,
            importance    = imp,
            value         = val,
            actual        = val,
            maturity      = label,
            indicatorName = f"{label} Treasury Rate",
            raw           = latest,
            event_family  = "treasury_rate",
            signal_tier   = "context",
            signal_reason = "Routine Treasury yield observation",
        ))

    # ── Historical rows: one summary per date ──────────────────────────────────
    for row in rows[1:30]:
        d = row.get("date") or ""
        if not d:
            continue
        y10 = row.get("year10")
        y2  = row.get("year2")
        y30 = row.get("year30")
        y5  = row.get("year5")
        parts: list[str] = []
        if y10 is not None: parts.append(f"10Y: {float(y10):.2f}%")
        if y2  is not None: parts.append(f"2Y: {float(y2):.2f}%")
        if y5  is not None: parts.append(f"5Y: {float(y5):.2f}%")
        if y30 is not None: parts.append(f"30Y: {float(y30):.2f}%")
        kd = " · ".join(parts) or None
        events.append(_build_event(
            id            = _event_id("treasury_rate", None, d, "summary"),
            symbol        = "Macro",
            companyName   = "US Treasury",
            eventType     = "treasury_rate",
            eventLabel    = "Treasury Rate",
            eventCategory = "macro",
            title         = "Treasury Yield Snapshot",
            subtitle      = d,
            keyDetails    = kd,
            date          = d,
            importance    = "medium",
            value         = y10,
            actual        = y10,
            maturity      = "10Y",
            indicatorName = "Treasury Yield Curve",
            raw           = row,
            event_family  = "treasury_snapshot",
            signal_tier   = "context",
            signal_reason = "Treasury yield snapshot",
        ))

    print(f"[catalyst:debug] treasury_macro rows={len(rows)} emitted={len(events)}")
    if events:
        e = events[0]
        print(f"[catalyst:debug] treasury[0]: date={e['date']} title={e['title']!r} keyDetails={e['keyDetails']!r}")
    return events


async def _fetch_recent_earnings(
    fmp: CatalystFMP,
    from_date: str,
    to_date: str,
    watchlist: set,
    portfolio: set,
) -> list[dict]:
    """
    Recent earnings reports: use the earnings calendar filtered to past dates,
    keeping rows that have actual eps/revenue values.
    """
    rows = await fmp.earnings_calendar(from_date, to_date)
    events: list[dict] = []
    for row in (rows or []):
        eps_a = _safe(row.get("eps") or row.get("epsActual"))
        rev_a = _safe(row.get("revenue") or row.get("revenueActual"))
        # Only include if there's actual reported data
        if eps_a is None and rev_a is None:
            continue
        sym   = (row.get("symbol") or "").upper()
        date  = row.get("date") or ""
        name  = row.get("name") or row.get("companyName") or sym
        eps_e = _safe(row.get("epsEstimated"))
        rev_e = _safe(row.get("revenueEstimated"))
        mc    = _safe(row.get("marketCap"))
        bucket = _mc_bucket(mc)
        imp = _score_importance("recent_earnings", bucket, sym, name, watchlist, portfolio)
        beat_miss = ""
        surprise     = None
        surp_pct     = None
        if eps_a is not None and eps_e is not None:
            beat_miss = " (Beat)" if eps_a >= eps_e else " (Miss)"
            surprise  = round(eps_a - eps_e, 4)
            surp_pct  = round((surprise / abs(eps_e)) * 100, 2) if eps_e else None
        events.append(_build_event(
            id               = _event_id("recent_earnings", sym, date),
            symbol           = sym or None,
            companyName      = name,
            eventType        = "recent_earnings",
            eventCategory    = "recent",
            title            = f"{name} Earnings Report{beat_miss}",
            date             = date,
            time             = row.get("time"),
            period           = row.get("fiscalDateEnding") or row.get("period"),
            marketCap        = mc,
            marketCapBucket  = bucket,
            importance       = imp,
            epsEstimated     = eps_e,
            epsActual        = eps_a,
            revenueEstimated = rev_e,
            revenueActual    = rev_a,
            surprise         = surprise,
            surprisePercent  = surp_pct,
            raw              = row,
        ))
    return events


async def _fetch_sec_filings(
    fmp: CatalystFMP,
    limit: int,
    watchlist: set,
    portfolio: set,
) -> list[dict]:
    """
    Per-symbol SEC filings for watchlist + portfolio symbols.
    Gracefully returns empty if no symbols are loaded or if FMP
    Starter does not support SEC filings for the requested symbols.
    """
    target_syms = list((watchlist | portfolio))[:20]
    if not target_syms:
        return []

    tasks = [fmp.sec_filings_by_symbol(sym, limit=5) for sym in target_syms]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    events: list[dict] = []
    for sym, result in zip(target_syms, results):
        rows = result if isinstance(result, list) else []
        for row in rows:
            form     = row.get("formType") or row.get("type") or ""
            date     = row.get("fillingDate") or row.get("date") or row.get("filedAt") or ""
            title_co = row.get("companyName") or row.get("name") or sym
            url      = row.get("finalLink") or row.get("link") or row.get("url") or ""
            mc       = _safe(row.get("marketCap"))
            bucket   = _mc_bucket(mc)
            imp = _score_importance("sec_filings", bucket, sym, form, watchlist, portfolio,
                                    form_type=form)
            events.append(_build_event(
                id            = _event_id("sec_filings", sym, date, form),
                symbol        = sym,
                companyName   = title_co,
                eventType     = "sec_filings",
                eventCategory = "recent",
                title         = f"{sym}: {form} Filing" if form else f"{sym} SEC Filing",
                date          = date,
                formType      = form,
                filingUrl     = url,
                marketCap     = mc,
                marketCapBucket = bucket,
                importance    = imp,
                raw           = row,
            ))
    return events


async def _fetch_analyst_ratings(
    fmp: CatalystFMP,
    limit: int,
    watchlist: set,
    portfolio: set,
) -> list[dict]:
    """
    Per-symbol analyst ratings snapshot for watchlist + portfolio symbols.
    Uses FMP /stable/ratings-snapshot (Starter-compatible).
    For global scope (no symbols) returns empty — bulk feed not on Starter.
    """
    target_syms = list((watchlist | portfolio))[:30]
    if not target_syms:
        return []

    tasks = [fmp.ratings_snapshot(sym) for sym in target_syms]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    today = _today()
    events: list[dict] = []
    for sym, result in zip(target_syms, results):
        if isinstance(result, Exception) or not result or not isinstance(result, dict):
            continue
        rating  = result.get("rating") or ""
        score   = result.get("overallScore")
        mc      = _safe(result.get("marketCap"))
        bucket  = _mc_bucket(mc)
        imp = _score_importance("analyst_ratings", bucket, sym, rating, watchlist, portfolio,
                                action="rating_snapshot")
        label_map = {"S": "Strong Buy", "A": "Buy", "B": "Hold", "C": "Sell", "D": "Strong Sell"}
        label = label_map.get(rating, rating) if rating else "No Rating"
        events.append(_build_event(
            id            = _event_id("analyst_ratings", sym, today, "snapshot"),
            symbol        = sym,
            companyName   = sym,
            eventType     = "analyst_ratings",
            eventCategory = "recent",
            title         = f"{sym}: Analyst Rating {label}" + (f" (Score {score}/5)" if score is not None else ""),
            date          = today,
            ratingTo      = label or None,
            action        = "rating_snapshot",
            marketCap     = mc,
            marketCapBucket = bucket,
            importance    = imp,
            raw           = result,
        ))
    return events


async def _fetch_insider_transactions(
    fmp: CatalystFMP,
    limit: int,
    watchlist: set,
    portfolio: set,
) -> list[dict]:
    """
    Per-symbol insider trading for watchlist + portfolio symbols.
    Gracefully returns empty if not available on this FMP plan.
    """
    target_syms = list((watchlist | portfolio))[:20]
    if not target_syms:
        return []

    tasks = [fmp.insider_trading_by_symbol(sym, limit=5) for sym in target_syms]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    events: list[dict] = []
    for sym, result in zip(target_syms, results):
        rows = result if isinstance(result, list) else []
        for row in rows:
            date      = row.get("transactionDate") or row.get("filingDate") or row.get("date") or ""
            if "T" in date:
                date = date.split("T")[0]
            insider   = row.get("reportingName") or row.get("insiderName") or row.get("name") or ""
            tx_type   = row.get("transactionType") or row.get("type") or ""
            shares_t  = _safe(row.get("securitiesTransacted") or row.get("sharesTraded"))
            price_t   = _safe(row.get("price") or row.get("transactionPrice"))
            value     = (shares_t * price_t) if (shares_t and price_t) else _safe(row.get("value") or row.get("transactionValue"))
            name      = row.get("companyName") or row.get("name") or sym
            mc        = _safe(row.get("marketCap"))
            bucket    = _mc_bucket(mc)
            is_buy    = "purchase" in tx_type.lower() or tx_type.upper() in ("P", "P-PURCHASE")
            is_sell   = "sale" in tx_type.lower() or tx_type.upper() in ("S", "S-SALE")
            action_str = "Purchase" if is_buy else ("Sale" if is_sell else tx_type)
            imp = _score_importance("insider_transactions", bucket, sym, name, watchlist, portfolio,
                                    transaction_value=value)
            title_parts = [sym, "Insider", action_str]
            if insider:
                title_parts.append(f"({insider})")
            events.append(_build_event(
                id               = _event_id("insider_transactions", sym, date, (insider + tx_type)[:20]),
                symbol           = sym,
                companyName      = name,
                eventType        = "insider_transactions",
                eventCategory    = "recent",
                title            = " ".join(title_parts),
                date             = date,
                insiderName      = insider or None,
                transactionType  = tx_type or None,
                sharesTraded     = shares_t,
                transactionValue = value,
                marketCap        = mc,
                marketCapBucket  = bucket,
                importance       = imp,
                raw              = row,
            ))
    return events


# ── Filtering ─────────────────────────────────────────────────────────────────

def _filter_events(
    events: list[dict],
    symbols: Optional[set],
    sector: Optional[str],
    mc_bucket: Optional[str],
    event_type: Optional[str],
) -> list[dict]:
    out = []
    for ev in events:
        if symbols and ev.get("symbol") not in symbols:
            continue
        if sector and (ev.get("sector") or "").lower() != sector.lower():
            continue
        if mc_bucket and mc_bucket != "all" and ev.get("marketCapBucket") != mc_bucket:
            continue
        if event_type and ev.get("eventType") != event_type:
            continue
        out.append(ev)
    return out


def _sort_by_importance_date(events: list[dict], date_desc: bool = False) -> list[dict]:
    """
    Sort: high > medium > low importance first, then by date within each tier.
    date_desc=True → most-recent dates first (for recent/history views).
    Uses two stable passes so importance tier is always preserved.
    """
    _rank = {"high": 0, "medium": 1, "low": 2}
    by_date = sorted(events, key=lambda e: e.get("date", ""), reverse=date_desc)
    return sorted(by_date, key=lambda e: _rank.get(e.get("importance", "low"), 2))


# ── Watchlist / portfolio loaders ─────────────────────────────────────────────

def _load_watchlist_symbols() -> set[str]:
    """Load all ticker symbols from all available watchlists."""
    syms: set[str] = set()
    try:
        from services.watchlist_service import list_watchlists, load_watchlist
        wls = list_watchlists() or []
        for wl in wls[:5]:
            wl_id = wl.get("id")
            if wl_id:
                store = load_watchlist(wl_id)
                if store and isinstance(store, dict):
                    for t in store.get("tickers", []):
                        if isinstance(t, str):
                            syms.add(t.upper())
                        elif isinstance(t, dict) and t.get("symbol"):
                            syms.add(t["symbol"].upper())
    except Exception as e:
        print(f"[catalyst] watchlist load error: {e}")
    return syms


def _load_portfolio_symbols() -> set[str]:
    """Load ticker symbols from the canonical Neon-backed portfolio store.

    Uses portfolio_store.load_active_holdings() — the exact same source as
    GET /api/portfolio/holdings — so the catalyst calendar always reflects
    what the user saved on the Portfolio page.

    Also includes underlying symbols from open option positions so that
    catalyst/earnings events for option underlyings appear in calendar views.
    OCC contract IDs are never included — only the underlying equity tickers.
    """
    try:
        from data.portfolio_store import load_active_holdings  # type: ignore
        holdings = load_active_holdings()
        syms: set[str] = set()
        for h in holdings:
            ticker = (h.get("ticker") or h.get("symbol") or "").upper().strip()
            if ticker:
                syms.add(ticker)

        # Include option underlyings so option positions appear in calendars
        try:
            from data.option_trades_store import load_open_option_underlyings
            opt_syms = load_open_option_underlyings()
            syms |= opt_syms
            if opt_syms:
                print(f"[catalyst] option underlyings added: {sorted(opt_syms)}")
        except Exception as _opt_e:
            print(f"[catalyst] option underlyings load error (non-fatal): {_opt_e}")

        print(f"[catalyst] _load_portfolio_symbols() → Neon+options: count={len(syms)}")
        return syms
    except Exception as e:
        print(f"[catalyst] portfolio load error: {e}")
        return set()


# ── Main entry points ─────────────────────────────────────────────────────────

def _default_dates(tab: str, mode: str = "upcoming") -> tuple[str, str]:
    """
    Return (from_date, to_date) defaults for a tab.

    mode="upcoming"  →  forward-looking windows (existing behaviour)
    mode="recent"    →  backward-looking windows defined by _RECENT_BACK
    """
    if mode == "recent":
        back = _RECENT_BACK.get(tab, _RECENT_DAYS)
        return _date_offset(-back), _today()

    # --- upcoming / default ---
    upcoming = {"earnings_dates", "dividends", "ipos", "splits"}
    recent   = {"recent_earnings", "sec_filings", "analyst_ratings", "insider_transactions"}
    macro    = {"economic_releases", "treasury_macro"}

    if tab in upcoming:
        return _today(), _date_offset(_UPCOMING_DAYS)
    if tab in recent:
        return _date_offset(-_RECENT_DAYS), _today()
    if tab in macro:
        return _date_offset(-_MACRO_BACK), _date_offset(_MACRO_AHEAD)
    return _date_offset(-_RECENT_DAYS), _date_offset(_UPCOMING_DAYS)


async def _fetch_tab(
    fmp: CatalystFMP,
    tab: str,
    from_date: str,
    to_date: str,
    watchlist: set,
    portfolio: set,
    limit: int = 100,
    mode: str = "upcoming",
) -> tuple[list[dict], Optional[str]]:
    """
    Fetch one tab. Returns (events, error_message).
    Never raises — all exceptions become error messages.

    mode="upcoming" → forward-looking calendar events (default, unchanged).
    mode="recent"   → historical events for list view.
      - earnings_dates  → delegates to _fetch_recent_earnings (has actual EPS/rev data)
      - dividends/ipos/splits/economic_releases/treasury_macro → same fetcher,
        backward date window, then eventType + eventCategory are normalised.
    """
    try:
        if mode == "recent" and tab == "earnings_dates":
            # Use the dedicated recent-earnings fetcher (filters to reported rows)
            evs = await _fetch_recent_earnings(fmp, from_date, to_date, watchlist, portfolio)

        elif tab == "earnings_dates":
            evs = await _fetch_earnings_dates(fmp, from_date, to_date, watchlist, portfolio)
        elif tab == "dividends":
            evs = await _fetch_dividends(fmp, from_date, to_date, watchlist, portfolio)
        elif tab == "ipos":
            evs = await _fetch_ipos(fmp, from_date, to_date, watchlist, portfolio)
        elif tab == "splits":
            evs = await _fetch_splits(fmp, from_date, to_date, watchlist, portfolio)
        elif tab == "economic_releases":
            evs = await _fetch_economic_releases(fmp, from_date, to_date)
        elif tab == "treasury_macro":
            evs = await _fetch_treasury_macro(fmp)
        elif tab == "recent_earnings":
            evs = await _fetch_recent_earnings(fmp, from_date, to_date, watchlist, portfolio)
        elif tab == "sec_filings":
            evs = await _fetch_sec_filings(fmp, min(limit, 100), watchlist, portfolio)
        elif tab == "analyst_ratings":
            evs = await _fetch_analyst_ratings(fmp, min(limit, 100), watchlist, portfolio)
        elif tab == "insider_transactions":
            evs = await _fetch_insider_transactions(fmp, min(limit, 100), watchlist, portfolio)
        else:
            return [], f"Unknown tab: {tab!r}"

        # ── Recent-mode normalisation ─────────────────────────────────────────
        # Override eventType and eventCategory for all events returned in
        # recent mode so the frontend gets consistent, mode-aware field values.
        if mode == "recent" and tab in _RECENT_EVENT_TYPES:
            recent_type = _RECENT_EVENT_TYPES[tab]
            for ev in evs:
                ev["eventType"]     = recent_type
                ev["eventCategory"] = "recent"
        # ─────────────────────────────────────────────────────────────────────

        # For earnings_dates calendar view (upcoming): do NOT apply limit here.
        # A limit=100 applied before date-sorting drops all events beyond the
        # first few high-importance dates.  The outer get_events() applies the
        # final limit after date-sorting the full dataset.
        if tab == "earnings_dates" and mode == "upcoming":
            evs = sorted(evs, key=lambda e: e.get("date", ""))
        else:
            evs = _sort_by_importance_date(evs, date_desc=(mode == "recent"))[:limit]
        return evs, None

    except Exception as e:
        print(f"[catalyst] tab={tab} mode={mode} error: {e}")
        return [], str(e)


async def get_overview(fmp_key: str) -> dict:
    """
    Fetch all tabs in parallel and return the overview response.
    Caps each tab at 20 events for the overview.
    """
    t0 = time.monotonic()
    _fmp_before = get_total_calls("fmp")
    _hits_before, _misses_before = get_cache_counts()
    fmp = CatalystFMP(fmp_key)

    watchlist = _load_watchlist_symbols()
    portfolio = _load_portfolio_symbols()

    # Build date ranges per tab
    tab_dates = {tab: _default_dates(tab) for tab in ALL_TABS}

    # Fetch all tabs in parallel
    tasks = {
        tab: _fetch_tab(fmp, tab, *tab_dates[tab], watchlist, portfolio, limit=20)
        for tab in ALL_TABS
    }
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    tabs_out: dict[str, dict] = {}
    errors: list[dict] = []
    all_events: list[dict] = []

    for tab, result in zip(tasks.keys(), results):
        if isinstance(result, Exception):
            tabs_out[tab] = {"count": 0, "events": []}
            errors.append({"tab": tab, "message": str(result)})
        else:
            evs, err = result
            tabs_out[tab] = {"count": len(evs), "events": evs}
            all_events.extend(evs)
            if err:
                errors.append({"tab": tab, "message": err})

    # Enrich symbols in parallel
    syms = list({ev["symbol"] for ev in all_events if ev.get("symbol")})
    enriched = await _enrich_profiles(syms, fmp)
    for tab in ALL_TABS:
        tabs_out[tab]["events"] = _apply_enrichment(tabs_out[tab]["events"], enriched)

    # Tradier live quotes for visible symbols (cap 30 — one batch call)
    _tradier_syms = list(dict.fromkeys(
        ev["symbol"] for ev in all_events if ev.get("symbol")
    ))[:30]
    _tradier_data = await _enrich_tradier_quotes(_tradier_syms)
    if _tradier_data:
        for tab in ALL_TABS:
            tabs_out[tab]["events"] = _apply_tradier_quotes(tabs_out[tab]["events"], _tradier_data)

    # Rebuild importance after enrichment (mc_bucket may have changed)
    for tab in ALL_TABS:
        for ev in tabs_out[tab]["events"]:
            sym = ev.get("symbol")
            imp = _score_importance(
                ev["eventType"], ev.get("marketCapBucket", "unknown"), sym,
                ev.get("title", ""), watchlist, portfolio,
                form_type=ev.get("formType"), transaction_value=ev.get("transactionValue"),
                action=ev.get("action"),
            )
            ev["importance"] = imp

    # Summary
    high_count = sum(
        1 for ev in all_events if ev.get("importance") == "high"
    )
    wl_count = sum(
        1 for ev in all_events if ev.get("symbol") in watchlist
    )
    port_count = sum(
        1 for ev in all_events if ev.get("symbol") in portfolio
    )
    next_major = [
        ev for ev in all_events
        if ev.get("importance") == "high" and ev.get("date", "") >= _today()
    ][:5]

    ms = int((time.monotonic() - t0) * 1000)
    print(f"[catalyst] overview tabs={len(ALL_TABS)} events={len(all_events)} errors={len(errors)} ms={ms}")

    _fmp_this = get_total_calls("fmp") - _fmp_before
    _hits_now, _misses_now = get_cache_counts()
    record_request(
        route="/api/catalysts/overview",
        page="calendar",
        feature="overview",
        provider_calls={"fmp": _fmp_this, "finnhub": 0, "finviz": 0,
                        "polygon": 0, "alpha_vantage": 0, "tradier": 0},
        cache_hits=_hits_now - _hits_before,
        cache_misses=_misses_now - _misses_before,
        elapsed_ms=ms,
        http_status=200 if not errors else 207,
        extra={"tabs": len(ALL_TABS), "events": len(all_events)},
    )

    status = "ok" if not errors else ("partial" if tabs_out else "error")
    return {
        "asOf":   datetime.now(timezone.utc).isoformat(),
        "tabs":   tabs_out,
        "summary": {
            "highImportanceCount":    high_count,
            "watchlistCatalystsCount": wl_count,
            "portfolioCatalystsCount": port_count,
            "nextMajorCatalysts":     next_major,
        },
        "status": status,
        "errors": errors,
    }


async def get_events(
    fmp_key: str,
    tab: str = "all",
    from_date: Optional[str] = None,
    to_date: Optional[str]   = None,
    symbols_filter: Optional[set] = None,
    scope: str = "all",
    sector: Optional[str] = None,
    mc_bucket: Optional[str] = None,
    event_type_filter: Optional[str] = None,
    limit: int = 100,
    refresh: bool = False,
    mode: str = "upcoming",
) -> dict:
    """
    Fetch events for one tab (or all) with filtering.

    mode="upcoming"  →  future/current catalyst events for calendar view (default).
    mode="recent"    →  historical events for list view; backward date windows
                        per _RECENT_BACK; eventType and eventCategory normalised
                        per _RECENT_EVENT_TYPES.
    """
    if mode not in ("upcoming", "recent"):
        mode = "upcoming"

    t0  = time.monotonic()
    _fmp_before = get_total_calls("fmp")
    _hits_before, _misses_before = get_cache_counts()
    fmp = CatalystFMP(fmp_key)

    watchlist = _load_watchlist_symbols()
    portfolio = _load_portfolio_symbols()

    # Build scope symbol filter
    scope_syms: Optional[set] = None
    if scope == "watchlist":
        scope_syms = watchlist
    elif scope == "portfolio":
        scope_syms = portfolio

    # Merge scope + explicit symbols
    effective_syms: Optional[set] = None
    if symbols_filter:
        effective_syms = symbols_filter
        if scope_syms:
            effective_syms = symbols_filter & scope_syms
    elif scope_syms:
        effective_syms = scope_syms

    # ── Symbol-driven short-circuit for watchlist / portfolio earnings ────────
    #
    # The broad FMP earnings-calendar covers only a narrow date window (30 d by
    # default).  If a watchlist/portfolio symbol has earnings outside that window
    # it disappears — that's the bug.  For scope=watchlist|portfolio we bypass
    # the broad fetch entirely and use a 120-day forward window cached in Neon,
    # keyed by universe.  The All tab is unaffected.
    if (
        scope in ("watchlist", "portfolio")
        and tab == "earnings_dates"
        and mode == "upcoming"
    ):
        syms = watchlist if scope == "watchlist" else portfolio
        print(
            f"[catalyst] branch=user_symbol_earnings "
            f"tab={tab} mode={mode} scope={scope} "
            f"symbols={len(syms)} first_10={sorted(syms)[:10]}"
        )
        _use_symbol_path = True
        try:
            from services.user_earnings_service import get_or_sync_user_earnings  # lazy

            req_from = from_date or _today()
            req_to   = to_date   or _date_offset(120)

            events, meta = await get_or_sync_user_earnings(
                universe  = scope,
                symbols   = syms,
                fmp_key   = fmp_key,
                from_date = req_from,
                to_date   = req_to,
            )
        except Exception as _ue_err:
            # Defensive fallback: user_earnings_service failed (e.g. Neon unavailable).
            # Log the error and fall through to the standard broad-FMP path below.
            print(
                f"[catalyst] user_earnings_service error for scope={scope}: {_ue_err} "
                f"— falling back to broad FMP path"
            )
            _use_symbol_path = False

        if _use_symbol_path:
            # Apply any additional caller filters (sector, mc_bucket, eventType,
            # explicit symbol list).  Never filter to symbols=None here — that would
            # return All events for non-watchlist symbols.
            events = _filter_events(events, symbols_filter, sector, mc_bucket, event_type_filter)
            events = sorted(
                events,
                key=lambda e: (e.get("date", ""), e.get("symbol", "")),
            )[:limit]

            ms = int((time.monotonic() - t0) * 1000)
            _fmp_this = get_total_calls("fmp") - _fmp_before
            print(
                f"[catalyst] symbol-driven earnings done: "
                f"scope={scope} events={len(events)} fmp_calls={_fmp_this} ms={ms}"
            )
            record_request(
                route   = f"/api/catalysts/events?tab={tab}&scope={scope}",
                page    = "calendar",
                feature = "earnings_symbol_driven",
                provider_calls = {"fmp": _fmp_this, "finnhub": 0, "finviz": 0,
                                  "polygon": 0, "alpha_vantage": 0, "tradier": 0},
                cache_hits   = 0,
                cache_misses = 0,
                elapsed_ms   = ms,
                http_status  = 200,
                extra        = {"tab": tab, "scope": scope, "events": len(events)},
            )
            return {
                "asOf":   datetime.now(timezone.utc).isoformat(),
                "tab":    tab,
                "mode":   mode,
                "from":   req_from,
                "to":     req_to,
                "filters": {
                    "scope":     scope,
                    "sector":    sector,
                    "marketCap": mc_bucket,
                    "eventType": event_type_filter,
                    "symbols":   sorted(symbols_filter) if symbols_filter else None,
                },
                "events":  events,
                "count":   len(events),
                "status":  "ok",
                "errors":  [],
                "meta":    meta,
            }
        # _use_symbol_path=False → fall through to broad FMP path below
        print(
            f"[catalyst] branch=all_broad_calendar (fallback) "
            f"tab={tab} mode={mode} scope={scope}"
        )
    else:
        print(
            f"[catalyst] branch={'all_broad_calendar' if scope == 'all' else 'user_symbol_earnings'} "
            f"tab={tab} mode={mode} scope={scope}"
        )
    # ── End symbol-driven short-circuit ──────────────────────────────────────

    tabs_to_fetch = ALL_TABS if tab == "all" else [tab]
    all_events: list[dict] = []
    errors: list[dict] = []

    for t_name in tabs_to_fetch:
        f_date, t_date = (
            (from_date or _default_dates(t_name, mode)[0]),
            (to_date   or _default_dates(t_name, mode)[1]),
        )
        evs, err = await _fetch_tab(
            fmp, t_name, f_date, t_date, watchlist, portfolio,
            limit=limit, mode=mode,
        )
        if err:
            errors.append({"tab": t_name, "message": err})
        all_events.extend(evs)

    # ── Earnings-dates calendar fast path ────────────────────────────────────
    # For upcoming earnings calendar: sort by date ASC and cap BEFORE enrichment.
    # The full FMP dataset for a 60-day window can be 800-2000 events.  Enriching
    # all symbols simultaneously exhausts FMP Starter rate limits (429) even with
    # a semaphore of 8.  By capping to `limit` events (date-first) up front, we
    # enrich only the symbols that will actually appear in the response.
    if tab == "earnings_dates" and mode == "upcoming":
        # Apply scope/search filter first (no enrichment needed for this)
        all_events = _filter_events(all_events, effective_syms, sector, mc_bucket, event_type_filter)
        # Sort by date ASC, then by symbol for stable ordering within a day
        all_events = sorted(
            all_events, key=lambda e: (e.get("date", ""), e.get("symbol", ""))
        )[:limit]
        # Debug: log date distribution so we can confirm all weekdays are covered
        _date_counts: dict[str, int] = {}
        for _ev in all_events:
            _d = _ev.get("date", "?")
            _date_counts[_d] = _date_counts.get(_d, 0) + 1
        print(f"[earnings_dates] upcoming date_dist={sorted(_date_counts.items())} total={len(all_events)}")
        # Enrich only the capped set.
        # max_live_fetches=40: cap cold-cache HTTP calls to ~30s max
        # (40 / 8 concurrent × 6s each ≈ 30s).  Cached profiles are always
        # returned without limit; uncached symbols beyond the cap fall back to
        # ticker-only and warm on the next request.
        syms = list(dict.fromkeys(ev["symbol"] for ev in all_events if ev.get("symbol")))
        enriched = await _enrich_profiles(syms, fmp, max_live_fetches=40)
        all_events = _apply_enrichment(all_events, enriched)
        # Tradier live quotes for visible symbols
        _tq = await _enrich_tradier_quotes(syms[:30])
        if _tq:
            all_events = _apply_tradier_quotes(all_events, _tq)
        # Re-score importance after enrichment (affects display badges, not sort order)
        for ev in all_events:
            ev["importance"] = _score_importance(
                ev["eventType"], ev.get("marketCapBucket", "unknown"), ev.get("symbol"),
                ev.get("title", ""), watchlist, portfolio,
            )
    else:
        # ── Standard path (all other tabs/modes) ─────────────────────────────
        # Enrich ALL events first (needed for importance re-scoring before sort)
        syms = list({ev["symbol"] for ev in all_events if ev.get("symbol")})
        enriched = await _enrich_profiles(syms, fmp)
        all_events = _apply_enrichment(all_events, enriched)

        # Re-score importance after enrichment
        for ev in all_events:
            sym = ev.get("symbol")
            ev["importance"] = _score_importance(
                ev["eventType"], ev.get("marketCapBucket", "unknown"), sym,
                ev.get("title", ""), watchlist, portfolio,
                form_type=ev.get("formType"), transaction_value=ev.get("transactionValue"),
                action=ev.get("action"),
            )

        # Filter
        all_events = _filter_events(all_events, effective_syms, sector, mc_bucket, event_type_filter)

        # Sort and limit
        all_events = _sort_by_importance_date(all_events, date_desc=(mode == "recent"))[:limit]

        # Tradier live quotes for the final visible set (post-filter/sort)
        _tq_syms = list(dict.fromkeys(ev["symbol"] for ev in all_events if ev.get("symbol")))[:30]
        _tq = await _enrich_tradier_quotes(_tq_syms)
        if _tq:
            all_events = _apply_tradier_quotes(all_events, _tq)

    ms = int((time.monotonic() - t0) * 1000)
    # Summary logging for earnings calendar
    if tab == "earnings_dates" and mode == "upcoming":
        date_counts: dict[str, int] = {}
        for ev in all_events:
            d = ev.get("date") or "unknown"
            date_counts[d] = date_counts.get(d, 0) + 1
        sorted_dc = sorted(date_counts.items())
        srcs = set(ev.get("source", "?") for ev in all_events)
        print(
            f"[catalyst] get_events tab={tab} mode={mode} events={len(all_events)} "
            f"unique_dates={len(date_counts)} sources={srcs} "
            f"first_5={sorted_dc[:5]} last_5={sorted_dc[-5:]} "
            f"errors={len(errors)} ms={ms}"
        )
    else:
        print(f"[catalyst] get_events tab={tab} mode={mode} events={len(all_events)} errors={len(errors)} ms={ms}")

    _fmp_this = get_total_calls("fmp") - _fmp_before
    _hits_now, _misses_now = get_cache_counts()
    record_request(
        route=f"/api/catalysts/events?tab={tab}",
        page="calendar",
        feature=tab,
        provider_calls={"fmp": _fmp_this, "finnhub": 0, "finviz": 0,
                        "polygon": 0, "alpha_vantage": 0, "tradier": 0},
        cache_hits=_hits_now - _hits_before,
        cache_misses=_misses_now - _misses_before,
        elapsed_ms=ms,
        http_status=200,
        extra={"tab": tab, "mode": mode, "events": len(all_events)},
    )

    status = "ok" if not errors else "partial"
    return {
        "asOf":    datetime.now(timezone.utc).isoformat(),
        "tab":     tab,
        "mode":    mode,
        "from":    from_date or _default_dates(tab if tab != "all" else "earnings_dates", mode)[0],
        "to":      to_date   or _default_dates(tab if tab != "all" else "earnings_dates", mode)[1],
        "filters": {
            "scope":     scope,
            "sector":    sector,
            "marketCap": mc_bucket,
            "eventType": event_type_filter,
            "symbols":   sorted(symbols_filter) if symbols_filter else None,
        },
        "events":  all_events,
        "count":   len(all_events),
        "status":  status,
        "errors":  errors,
    }


async def get_filters(fmp_key: str) -> dict:
    """Return available filter values."""
    watchlist = _load_watchlist_symbols()
    portfolio = _load_portfolio_symbols()
    return {
        "sectors": [
            "Technology", "Healthcare", "Finance", "Consumer Cyclical",
            "Consumer Defensive", "Energy", "Industrials", "Materials",
            "Real Estate", "Utilities", "Communication Services",
        ],
        "marketCapBuckets": ["mega", "large", "mid", "small", "micro", "unknown"],
        "eventTypes": ALL_TABS,
        "watchlistSymbols": sorted(watchlist),
        "portfolioSymbols": sorted(portfolio),
    }


_TTL_ASK_CONTEXT = 4 * 3600   # 4-hour TTL for ask-context responses

_ASK_CONTEXT_TABS = [
    "earnings_dates",
    "dividends",
    "ipos",
    "splits",
    "economic_releases",
    "treasury_macro",
]


async def get_ask_context(
    fmp_key: str,
    scope: str = "all",
    symbols: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    include_recent: bool = True,
    include_upcoming: bool = True,
    refresh: bool = False,
    per_tab_limit: int = 100,
) -> dict:
    """
    Return a full Catalyst Calendar context package across all 6 tabs for Ask Caelyn.

    Reuses get_events() for every tab × mode combination so all normalisation,
    caching, enrichment, and filter logic is shared.  The ask-context response
    is itself cached at 4-hour TTL (bypassed when refresh=True).

    Cache key:
        catalysts:ask_context:v1:{scope}:{symbols_hash}:{from}:{to}:{recent}:{upcoming}
    """
    syms_hash = hashlib.md5((symbols or "").lower().encode()).hexdigest()[:8]
    ck = (
        f"catalysts:ask_context:v1:{scope}:{syms_hash}:"
        f"{from_date or ''}:{to_date or ''}:"
        f"{int(include_recent)}:{int(include_upcoming)}"
    )

    if not refresh:
        hit = cache.get(ck)
        if hit is not None:
            return hit

    t0 = time.monotonic()

    # Resolve symbols_filter set once
    symbols_filter: Optional[set] = None
    if symbols:
        symbols_filter = {s.strip().upper() for s in symbols.split(",") if s.strip()}

    modes: list[str] = []
    if include_upcoming:
        modes.append("upcoming")
    if include_recent:
        modes.append("recent")

    # Build all parallel tasks: 6 tabs × up-to-2 modes
    task_keys: list[tuple[str, str]] = []
    coros = []
    for tab in _ASK_CONTEXT_TABS:
        for mode in modes:
            task_keys.append((tab, mode))
            coros.append(get_events(
                fmp_key=fmp_key,
                tab=tab,
                mode=mode,
                scope=scope,
                symbols_filter=symbols_filter,
                from_date=from_date,
                to_date=to_date,
                limit=per_tab_limit,
                refresh=refresh,
            ))

    results = await asyncio.gather(*coros, return_exceptions=True)

    # Assemble response
    tab_data: dict[str, dict] = {t: {"upcoming": [], "recent": []} for t in _ASK_CONTEXT_TABS}
    errors: list[dict] = []
    total_events = 0
    high_importance = 0
    actual_from: Optional[str] = from_date
    actual_to: Optional[str]   = to_date

    watchlist    = _load_watchlist_symbols()
    portfolio_s  = _load_portfolio_symbols()
    watchlist_matches  = 0
    portfolio_matches  = 0

    for (tab, mode), res in zip(task_keys, results):
        if isinstance(res, Exception):
            errors.append({"tab": tab, "mode": mode, "message": str(res)})
            continue
        evs = res.get("events", [])
        if res.get("errors"):
            errors.extend(res["errors"])
        tab_data[tab][mode] = evs
        total_events += len(evs)

        # Track range actually used
        if not actual_from and res.get("from"):
            actual_from = res["from"]
        if not actual_to and res.get("to"):
            actual_to = res["to"]

        for ev in evs:
            if ev.get("importance") == "high":
                high_importance += 1
            sym = ev.get("symbol")
            if sym:
                if sym in watchlist:
                    watchlist_matches += 1
                if sym in portfolio_s:
                    portfolio_matches += 1

    # Derive next/recent major catalysts across all tabs
    today_str = _today()
    all_upcoming = [ev for t in _ASK_CONTEXT_TABS for ev in tab_data[t]["upcoming"]]
    all_recent   = [ev for t in _ASK_CONTEXT_TABS for ev in tab_data[t]["recent"]]

    next_major = _sort_by_importance_date(
        [e for e in all_upcoming if e.get("importance") == "high" and (e.get("date") or "") >= today_str],
        date_desc=False,
    )[:10]
    recent_major = _sort_by_importance_date(
        [e for e in all_recent if e.get("importance") == "high"],
        date_desc=True,
    )[:10]

    ms = int((time.monotonic() - t0) * 1000)
    print(
        f"[catalyst] ask-context scope={scope} tabs={len(_ASK_CONTEXT_TABS)} "
        f"modes={modes} total={total_events} high={high_importance} "
        f"errors={len(errors)} ms={ms}"
    )

    status = "ok" if not errors else "partial"
    payload = {
        "asOf":   datetime.now(timezone.utc).isoformat(),
        "source": "fmp",
        "range":  {
            "from": actual_from or _date_offset(-_RECENT_DAYS),
            "to":   actual_to   or _date_offset(_UPCOMING_DAYS),
        },
        "tabs": tab_data,
        "summary": {
            "totalEvents":         total_events,
            "highImportanceEvents": high_importance,
            "watchlistMatches":    watchlist_matches,
            "portfolioMatches":    portfolio_matches,
            "nextMajorCatalysts":  next_major,
            "recentMajorCatalysts": recent_major,
        },
        "status": status,
        "errors": errors,
    }

    cache.set(ck, payload, _TTL_ASK_CONTEXT)
    return payload


async def get_by_symbol(fmp_key: str, symbol: str) -> dict:
    """
    Return all upcoming and recent catalysts for one ticker.
    Powers the symbol popup/detail panel.
    """
    t0  = time.monotonic()
    fmp = CatalystFMP(fmp_key)
    sym = symbol.upper().strip()

    watchlist = _load_watchlist_symbols()
    portfolio = _load_portfolio_symbols()

    from_upcoming = _today()
    to_upcoming   = _date_offset(_UPCOMING_DAYS)
    from_recent   = _date_offset(-_RECENT_DAYS)

    # Fetch all symbol-relevant tabs in parallel
    tasks = [
        fmp.earnings_calendar(from_upcoming, to_upcoming),
        fmp.earnings_calendar(from_recent, _today()),
        fmp.dividends_calendar(from_upcoming, to_upcoming),
        fmp.splits_calendar(from_upcoming, to_upcoming),
        fmp.sec_filings_by_symbol(sym, limit=10),
        fmp.ratings_snapshot(sym),
        fmp.insider_trading_by_symbol(sym, limit=10),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    # sec and insider are lists; analyst is a single dict
    earn_up  = results[0] if not isinstance(results[0], Exception) else []
    earn_rec = results[1] if not isinstance(results[1], Exception) else []
    divs     = results[2] if not isinstance(results[2], Exception) else []
    splits   = results[3] if not isinstance(results[3], Exception) else []
    sec      = results[4] if not isinstance(results[4], Exception) else []
    analyst_snap = results[5] if not isinstance(results[5], Exception) else {}
    insider  = results[6] if not isinstance(results[6], Exception) else []

    # Enrich profile once
    profile = await fmp.company_profile(sym)
    mc  = _safe(profile.get("mktCap") or profile.get("marketCap"))
    enr = {
        sym: {
            "companyName":     profile.get("companyName") or profile.get("name") or sym,
            "sector":          profile.get("sector"),
            "industry":        profile.get("industry"),
            "marketCap":       mc,
            "marketCapBucket": _mc_bucket(mc),
        }
    }

    all_events: list[dict] = []

    # Filter each raw list to this symbol only, then normalise
    def _sym_rows(rows, key="symbol"):
        return [r for r in (rows or []) if (r.get(key) or r.get("ticker") or "").upper() == sym]

    for row in _sym_rows(earn_up):
        all_events.extend(await _fetch_earnings_dates(
            fmp, row.get("date", from_upcoming), row.get("date", to_upcoming), watchlist, portfolio
        ) if False else [])
        # Direct normalise
        all_events.append(_build_event(
            id=_event_id("earnings_dates", sym, row.get("date", "")),
            symbol=sym, companyName=enr[sym]["companyName"], eventType="earnings_dates",
            eventCategory="upcoming", title=f"{sym} Earnings",
            date=row.get("date", ""), time=row.get("time"),
            period=row.get("fiscalDateEnding"),
            marketCap=enr[sym]["marketCap"], marketCapBucket=enr[sym]["marketCapBucket"],
            importance=_score_importance("earnings_dates", enr[sym]["marketCapBucket"],
                                         sym, sym, watchlist, portfolio),
            epsEstimated=_safe(row.get("epsEstimated")),
            revenueEstimated=_safe(row.get("revenueEstimated")), raw=row,
        ))

    for row in _sym_rows(earn_rec):
        if _safe(row.get("eps") or row.get("epsActual")) is not None:
            all_events.append(_build_event(
                id=_event_id("recent_earnings", sym, row.get("date", "")),
                symbol=sym, companyName=enr[sym]["companyName"], eventType="recent_earnings",
                eventCategory="recent", title=f"{sym} Earnings Report",
                date=row.get("date", ""), time=row.get("time"),
                period=row.get("fiscalDateEnding"),
                marketCap=enr[sym]["marketCap"], marketCapBucket=enr[sym]["marketCapBucket"],
                importance="high" if sym in portfolio or sym in watchlist else "medium",
                epsEstimated=_safe(row.get("epsEstimated")),
                epsActual=_safe(row.get("eps") or row.get("epsActual")),
                revenueEstimated=_safe(row.get("revenueEstimated")),
                revenueActual=_safe(row.get("revenue") or row.get("revenueActual")), raw=row,
            ))

    for row in _sym_rows(divs):
        all_events.append(_build_event(
            id=_event_id("dividends", sym, row.get("date", "")),
            symbol=sym, companyName=enr[sym]["companyName"], eventType="dividends",
            eventCategory="upcoming", title=f"{sym} Dividend",
            date=row.get("date", "") or row.get("exDividendDate", ""),
            dividend=_safe(row.get("dividend")),
            exDividendDate=row.get("exDividendDate") or row.get("date"),
            recordDate=row.get("recordDate"), paymentDate=row.get("paymentDate"),
            declarationDate=row.get("declarationDate"),
            marketCap=enr[sym]["marketCap"], marketCapBucket=enr[sym]["marketCapBucket"],
            importance=_score_importance("dividends", enr[sym]["marketCapBucket"],
                                         sym, sym, watchlist, portfolio), raw=row,
        ))

    for row in _sym_rows(splits):
        num, den = row.get("numerator"), row.get("denominator")
        ratio = f"{num}:{den}" if num and den else None
        all_events.append(_build_event(
            id=_event_id("splits", sym, row.get("date", "")),
            symbol=sym, companyName=enr[sym]["companyName"], eventType="splits",
            eventCategory="upcoming",
            title=f"{sym} {ratio} Stock Split" if ratio else f"{sym} Stock Split",
            date=row.get("date", ""), splitRatio=str(ratio) if ratio else None,
            marketCap=enr[sym]["marketCap"], marketCapBucket=enr[sym]["marketCapBucket"],
            importance=_score_importance("splits", enr[sym]["marketCapBucket"],
                                         sym, sym, watchlist, portfolio), raw=row,
        ))

    for row in _sym_rows(sec, key="ticker") + _sym_rows(sec, key="symbol"):
        form = row.get("formType") or row.get("type") or ""
        date = row.get("fillingDate") or row.get("date") or row.get("filedAt") or ""
        all_events.append(_build_event(
            id=_event_id("sec_filings", sym, date, form),
            symbol=sym, companyName=enr[sym]["companyName"], eventType="sec_filings",
            eventCategory="recent", title=f"{sym}: {form} Filing" if form else f"{sym} SEC Filing",
            date=date, formType=form,
            filingUrl=row.get("finalLink") or row.get("link") or "",
            marketCap=enr[sym]["marketCap"], marketCapBucket=enr[sym]["marketCapBucket"],
            importance=_score_importance("sec_filings", enr[sym]["marketCapBucket"],
                                         sym, form, watchlist, portfolio, form_type=form), raw=row,
        ))

    # Analyst ratings snapshot for this symbol
    if analyst_snap and isinstance(analyst_snap, dict):
        rating  = analyst_snap.get("rating") or ""
        score   = analyst_snap.get("overallScore")
        label_map = {"S": "Strong Buy", "A": "Buy", "B": "Hold", "C": "Sell", "D": "Strong Sell"}
        label = label_map.get(rating, rating) if rating else "No Rating"
        all_events.append(_build_event(
            id=_event_id("analyst_ratings", sym, _today(), "snapshot"),
            symbol=sym, companyName=enr[sym]["companyName"], eventType="analyst_ratings",
            eventCategory="recent",
            title=f"{sym}: Analyst Rating {label}" + (f" (Score {score}/5)" if score is not None else ""),
            date=_today(), ratingTo=label or None, action="rating_snapshot",
            marketCap=enr[sym]["marketCap"], marketCapBucket=enr[sym]["marketCapBucket"],
            importance=_score_importance("analyst_ratings", enr[sym]["marketCapBucket"],
                                         sym, rating, watchlist, portfolio,
                                         action="rating_snapshot"), raw=analyst_snap,
        ))

    for row in _sym_rows(insider):
        date = (row.get("transactionDate") or row.get("filingDate") or "").split("T")[0]
        insider_name = row.get("reportingName") or ""
        tx_type = row.get("transactionType") or ""
        shares_t = _safe(row.get("securitiesTransacted"))
        price_t  = _safe(row.get("price"))
        value = (shares_t * price_t) if (shares_t and price_t) else _safe(row.get("value"))
        all_events.append(_build_event(
            id=_event_id("insider_transactions", sym, date, (insider_name + tx_type)[:20]),
            symbol=sym, companyName=enr[sym]["companyName"], eventType="insider_transactions",
            eventCategory="recent",
            title=f"{sym} Insider {tx_type} ({insider_name})" if insider_name else f"{sym} Insider {tx_type}",
            date=date, insiderName=insider_name or None, transactionType=tx_type or None,
            sharesTraded=shares_t, transactionValue=value,
            marketCap=enr[sym]["marketCap"], marketCapBucket=enr[sym]["marketCapBucket"],
            importance=_score_importance("insider_transactions", enr[sym]["marketCapBucket"],
                                         sym, sym, watchlist, portfolio,
                                         transaction_value=value), raw=row,
        ))

    all_events = _sort_by_importance_date(all_events)
    ms = int((time.monotonic() - t0) * 1000)
    print(f"[catalyst] by-symbol {sym} events={len(all_events)} ms={ms}")

    return {
        "symbol":   sym,
        "profile":  enr.get(sym, {}),
        "asOf":     datetime.now(timezone.utc).isoformat(),
        "events":   all_events,
        "count":    len(all_events),
        "status":   "ok",
        "errors":   [],
    }
