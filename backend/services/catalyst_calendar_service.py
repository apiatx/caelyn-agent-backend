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
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx

from data.cache import cache

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

# Default date windows
_UPCOMING_DAYS  = 60
_RECENT_DAYS    = 30
_MACRO_BACK     = 14
_MACRO_AHEAD    = 45

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
    ) -> Any:
        hit = cache.get(cache_key)
        if hit is not None:
            return hit

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
                return []
            result = resp.json()
            rows = len(result) if isinstance(result, list) else (1 if result else 0)
            print(f"[catalyst] FMP {endpoint} status={status} rows={rows} ms={ms}")
            if result:
                cache.set(cache_key, result, ttl)
            return result
        except Exception as e:
            ms = int((time.monotonic() - t0) * 1000)
            print(f"[catalyst] FMP {endpoint} error={e} ms={ms}")
            return []

    # ── Calendar endpoints ─────────────────────────────────────────────────

    async def earnings_calendar(self, from_date: str, to_date: str) -> list:
        ck = f"cat:earn:{from_date}:{to_date}"
        d  = await self._get("earnings-calendar", {"from": from_date, "to": to_date}, ck, _TTL_EARNINGS)
        return d if isinstance(d, list) else []

    async def dividends_calendar(self, from_date: str, to_date: str) -> list:
        ck = f"cat:div:{from_date}:{to_date}"
        d  = await self._get("dividends-calendar", {"from": from_date, "to": to_date}, ck, _TTL_DIVIDENDS)
        return d if isinstance(d, list) else []

    async def ipo_calendar(self, from_date: str, to_date: str) -> list:
        ck = f"cat:ipo:{from_date}:{to_date}"
        d  = await self._get("ipo-calendar", {"from": from_date, "to": to_date}, ck, _TTL_IPO)
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
        ck = f"cat:profile:{symbol}"
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
        if event_type in ("earnings_dates", "recent_earnings", "dividends"):
            return "high"

    # Economic release keywords
    if event_type in ("economic_releases", "treasury_macro"):
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

    # IPOs of larger companies
    if event_type == "ipos":
        if mc_bucket in ("mega", "large", "mid"):
            return "high"
        return "medium"

    # Mid-cap earnings
    if event_type in ("earnings_dates", "recent_earnings"):
        if mc_bucket in ("mega", "large"):
            return "high"
        if mc_bucket == "mid":
            return "medium"
        return "low"

    return "low"


def _build_event(**kw) -> dict:
    """Build a normalized event dict with all schema fields."""
    return {
        "id":                 kw.get("id", ""),
        "symbol":             kw.get("symbol"),
        "companyName":        kw.get("companyName"),
        "eventType":          kw.get("eventType", ""),
        "eventCategory":      kw.get("eventCategory", "upcoming"),
        "title":              kw.get("title", ""),
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
        # split
        "splitRatio":         kw.get("splitRatio"),
        # SEC filing
        "formType":           kw.get("formType"),
        "filingUrl":          kw.get("filingUrl"),
        # analyst
        "ratingFrom":         kw.get("ratingFrom"),
        "ratingTo":           kw.get("ratingTo"),
        "action":             kw.get("action"),
        "analystFirm":        kw.get("analystFirm"),
        # insider
        "insiderName":        kw.get("insiderName"),
        "transactionType":    kw.get("transactionType"),
        "sharesTraded":       kw.get("sharesTraded"),
        "transactionValue":   kw.get("transactionValue"),
        # macro
        "actual":             kw.get("actual"),
        "estimate":           kw.get("estimate"),
        "previous":           kw.get("previous"),
        "country":            kw.get("country"),
    }


# ── Profile enrichment ────────────────────────────────────────────────────────

async def _enrich_profiles(
    symbols: list[str],
    fmp: CatalystFMP,
) -> dict[str, dict]:
    """
    Batch-fetch company profiles for a list of symbols.
    Returns {symbol: {companyName, sector, industry, marketCap, marketCapBucket}}.
    Uses per-symbol cache (TTL 24 h). Silently ignores failures.
    """
    unique = list(dict.fromkeys(s for s in symbols if s))[:80]
    if not unique:
        return {}

    tasks = [fmp.company_profile(s) for s in unique]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    enriched: dict[str, dict] = {}
    for sym, res in zip(unique, results):
        if isinstance(res, Exception) or not res:
            enriched[sym] = {}
            continue
        mc = _safe(res.get("mktCap") or res.get("marketCap"))
        enriched[sym] = {
            "companyName":     res.get("companyName") or res.get("name") or sym,
            "sector":          res.get("sector"),
            "industry":        res.get("industry"),
            "marketCap":       mc,
            "marketCapBucket": _mc_bucket(mc),
        }
    return enriched


def _apply_enrichment(events: list[dict], enriched: dict[str, dict]) -> list[dict]:
    for ev in events:
        sym = ev.get("symbol")
        if sym and sym in enriched:
            info = enriched[sym]
            ev["companyName"]     = ev.get("companyName") or info.get("companyName")
            ev["sector"]          = ev.get("sector") or info.get("sector")
            ev["industry"]        = ev.get("industry") or info.get("industry")
            if ev.get("marketCap") is None:
                ev["marketCap"]       = info.get("marketCap")
                ev["marketCapBucket"] = info.get("marketCapBucket", "unknown")
    return events


# ── Tab fetchers ──────────────────────────────────────────────────────────────

async def _fetch_earnings_dates(
    fmp: CatalystFMP,
    from_date: str,
    to_date: str,
    watchlist: set,
    portfolio: set,
) -> list[dict]:
    rows = await fmp.earnings_calendar(from_date, to_date)
    events: list[dict] = []
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
        imp = _score_importance("earnings_dates", bucket, sym, name, watchlist, portfolio)
        events.append(_build_event(
            id            = _event_id("earnings_dates", sym, date),
            symbol        = sym or None,
            companyName   = name,
            eventType     = "earnings_dates",
            eventCategory = "upcoming",
            title         = f"{name} Earnings" if name != sym else f"{sym} Earnings",
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
            raw           = row,
        ))
    return events


async def _fetch_dividends(
    fmp: CatalystFMP,
    from_date: str,
    to_date: str,
    watchlist: set,
    portfolio: set,
) -> list[dict]:
    rows = await fmp.dividends_calendar(from_date, to_date)
    events: list[dict] = []
    for row in (rows or []):
        sym  = (row.get("symbol") or "").upper()
        date = row.get("date") or row.get("exDividendDate") or ""
        div  = _safe(row.get("dividend") or row.get("adjDividend"))
        name = row.get("companyName") or row.get("name") or sym
        mc   = _safe(row.get("marketCap"))
        bucket = _mc_bucket(mc)
        imp = _score_importance("dividends", bucket, sym, f"{sym} Dividend", watchlist, portfolio)
        events.append(_build_event(
            id             = _event_id("dividends", sym, date),
            symbol         = sym or None,
            companyName    = name,
            eventType      = "dividends",
            eventCategory  = "upcoming",
            title          = f"{sym} Dividend${f' ${div:.4f}' if div else ''}",
            date           = date,
            marketCap      = mc,
            marketCapBucket = bucket,
            importance     = imp,
            dividend       = div,
            exDividendDate = row.get("exDividendDate") or row.get("date"),
            recordDate     = row.get("recordDate"),
            paymentDate    = row.get("paymentDate"),
            declarationDate = row.get("declarationDate"),
            raw            = row,
        ))
    return events


async def _fetch_ipos(
    fmp: CatalystFMP,
    from_date: str,
    to_date: str,
    watchlist: set,
    portfolio: set,
) -> list[dict]:
    rows = await fmp.ipo_calendar(from_date, to_date)
    events: list[dict] = []
    for row in (rows or []):
        sym    = (row.get("symbol") or "").upper()
        name   = row.get("company") or row.get("companyName") or sym
        date   = row.get("date") or row.get("ipoDate") or ""
        shares = _safe(row.get("shares") or row.get("totalSharesValue"))
        low_p  = row.get("priceFrom") or row.get("priceLow")
        high_p = row.get("priceTo") or row.get("priceHigh")
        price_range = (
            f"${low_p}–${high_p}" if low_p and high_p
            else (f"${low_p}" if low_p else (f"${high_p}" if high_p else None))
        )
        offer = row.get("offerPrice")
        mc   = _safe(row.get("marketCap") or (
            shares * offer if shares and offer else None
        ))
        bucket = _mc_bucket(mc)
        imp = _score_importance("ipos", bucket, sym or None, name, watchlist, portfolio)
        events.append(_build_event(
            id             = _event_id("ipos", sym or name, date),
            symbol         = sym or None,
            companyName    = name,
            eventType      = "ipos",
            eventCategory  = "upcoming",
            title          = f"{name} IPO",
            date           = date,
            exchange       = row.get("exchange"),
            shares         = shares,
            priceRange     = price_range,
            marketCap      = mc,
            marketCapBucket = bucket,
            importance     = imp,
            raw            = row,
        ))
    return events


async def _fetch_splits(
    fmp: CatalystFMP,
    from_date: str,
    to_date: str,
    watchlist: set,
    portfolio: set,
) -> list[dict]:
    rows = await fmp.splits_calendar(from_date, to_date)
    events: list[dict] = []
    for row in (rows or []):
        sym  = (row.get("symbol") or "").upper()
        date = row.get("date") or row.get("effectiveDate") or ""
        num  = row.get("numerator")
        den  = row.get("denominator")
        ratio = f"{num}:{den}" if num and den else row.get("ratio") or row.get("splitFactor")
        name = row.get("companyName") or row.get("name") or sym
        mc   = _safe(row.get("marketCap"))
        bucket = _mc_bucket(mc)
        imp = _score_importance("splits", bucket, sym, name, watchlist, portfolio)
        events.append(_build_event(
            id             = _event_id("splits", sym, date),
            symbol         = sym or None,
            companyName    = name,
            eventType      = "splits",
            eventCategory  = "upcoming",
            title          = f"{sym} {ratio} Stock Split" if ratio else f"{sym} Stock Split",
            date           = date,
            splitRatio     = str(ratio) if ratio else None,
            marketCap      = mc,
            marketCapBucket = bucket,
            importance     = imp,
            raw            = row,
        ))
    return events


async def _fetch_economic_releases(
    fmp: CatalystFMP,
    from_date: str,
    to_date: str,
) -> list[dict]:
    rows = await fmp.economic_calendar(from_date, to_date)
    events: list[dict] = []
    for row in (rows or []):
        country = row.get("country", "")
        title   = row.get("event") or row.get("name") or ""
        date    = row.get("date") or ""
        impact  = (row.get("impact") or "").lower()
        # High impact = FMP labels it "High" or matches our keywords
        is_high = impact == "high" or any(
            kw.lower() in title.lower() for kw in _HIGH_IMPACT_ECON
        )
        imp = "high" if is_high else ("medium" if impact == "medium" else "low")
        events.append(_build_event(
            id             = _event_id("economic_releases", None, date, title[:30]),
            symbol         = None,
            companyName    = None,
            eventType      = "economic_releases",
            eventCategory  = "macro",
            title          = title,
            date           = date,
            time           = row.get("time"),
            importance     = imp,
            actual         = row.get("actual"),
            estimate       = row.get("estimate"),
            previous       = row.get("previous"),
            country        = country,
            raw            = row,
        ))
    return events


async def _fetch_treasury_macro(fmp: CatalystFMP) -> list[dict]:
    """Return treasury yield curve as a macro event (latest snapshot)."""
    rows  = await fmp.treasury_rates()
    events: list[dict] = []
    if not rows:
        return events

    latest = rows[0] if rows else {}
    date   = latest.get("date") or _today()

    # Build human-readable summary
    y10 = latest.get("year10")
    y2  = latest.get("year2")
    y30 = latest.get("year30")
    parts = []
    if y10:  parts.append(f"10Y: {y10:.2f}%")
    if y2:   parts.append(f"2Y: {y2:.2f}%")
    if y30:  parts.append(f"30Y: {y30:.2f}%")
    title = "Treasury Yields: " + ", ".join(parts) if parts else "Treasury Yield Snapshot"

    events.append(_build_event(
        id            = _event_id("treasury_macro", None, date, "yield_curve"),
        eventType     = "treasury_macro",
        eventCategory = "macro",
        title         = title,
        date          = date,
        importance    = "high",
        actual        = y10,
        raw           = latest,
    ))

    # Add recent historical rows as data points (last 5)
    for row in rows[1:6]:
        d   = row.get("date") or ""
        y10r = row.get("year10")
        events.append(_build_event(
            id            = _event_id("treasury_macro", None, d, "yield_history"),
            eventType     = "treasury_macro",
            eventCategory = "macro",
            title         = f"Treasury Yield Snapshot ({d})",
            date          = d,
            importance    = "medium",
            actual        = y10r,
            raw           = row,
        ))
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
        if eps_a is not None and eps_e is not None:
            beat_miss = " (Beat)" if eps_a >= eps_e else " (Miss)"
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


def _sort_by_importance_date(events: list[dict]) -> list[dict]:
    """Sort: high > medium > low, then by date ascending."""
    _rank = {"high": 0, "medium": 1, "low": 2}
    return sorted(
        events,
        key=lambda e: (_rank.get(e.get("importance", "low"), 2), e.get("date", "")),
    )


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
    """Load ticker symbols from the portfolio holdings JSON file."""
    syms: set[str] = set()
    try:
        import json
        from pathlib import Path
        for fname in [
            "data/portfolio_holdings.json",
            *[str(p) for p in Path("data").glob("portfolio_holdings_*.json")],
        ]:
            p = Path(fname)
            if p.exists():
                data = json.loads(p.read_text())
                holdings = data.get("holdings", data) if isinstance(data, dict) else data
                if isinstance(holdings, list):
                    for h in holdings:
                        if isinstance(h, dict):
                            sym = h.get("symbol") or h.get("ticker")
                            if sym:
                                syms.add(sym.upper())
    except Exception as e:
        print(f"[catalyst] portfolio load error: {e}")
    return syms


# ── Main entry points ─────────────────────────────────────────────────────────

def _default_dates(tab: str) -> tuple[str, str]:
    """Return (from_date, to_date) defaults for a tab."""
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
) -> tuple[list[dict], Optional[str]]:
    """
    Fetch one tab. Returns (events, error_message).
    Never raises — all exceptions become error messages.
    """
    try:
        if tab == "earnings_dates":
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

        evs = _sort_by_importance_date(evs)[:limit]
        return evs, None

    except Exception as e:
        print(f"[catalyst] tab={tab} error: {e}")
        return [], str(e)


async def get_overview(fmp_key: str) -> dict:
    """
    Fetch all tabs in parallel and return the overview response.
    Caps each tab at 20 events for the overview.
    """
    t0 = time.monotonic()
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
) -> dict:
    """Fetch events for one tab (or all) with filtering."""
    t0  = time.monotonic()
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

    tabs_to_fetch = ALL_TABS if tab == "all" else [tab]
    all_events: list[dict] = []
    errors: list[dict] = []

    for t_name in tabs_to_fetch:
        f_date, t_date = (
            (from_date or _default_dates(t_name)[0]),
            (to_date   or _default_dates(t_name)[1]),
        )
        evs, err = await _fetch_tab(fmp, t_name, f_date, t_date, watchlist, portfolio, limit=limit)
        if err:
            errors.append({"tab": t_name, "message": err})
        all_events.extend(evs)

    # Enrich
    syms = list({ev["symbol"] for ev in all_events if ev.get("symbol")})
    enriched = await _enrich_profiles(syms[:60], fmp)
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
    all_events = _sort_by_importance_date(all_events)[:limit]

    ms = int((time.monotonic() - t0) * 1000)
    status = "ok" if not errors else "partial"
    return {
        "asOf":    datetime.now(timezone.utc).isoformat(),
        "tab":     tab,
        "from":    from_date or "",
        "to":      to_date or "",
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
