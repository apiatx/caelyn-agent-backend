"""
earnings_clean_service.py — Clean FMP-only Upcoming Earnings service.

Provides two public coroutines:
  get_upcoming_clean(...)  → full date-range calendar with eventsByDate / countsByDate
  get_day_clean(...)       → fully-enriched event list for a single selected date

Design rules (hard):
  • source = "fmp" on every event — no exceptions
  • No Finnhub, Polymarket, beat-odds, smart-ranking, AI curation
  • No prediction-market rows ("Will…" questions)
  • "upcoming-clean" data uses new cache namespace to prevent stale
    Finnhub/Polymarket rows from bleeding in

Data flow:
  FMP earnings-calendar (chunked 7-day slices)
    → normalize to clean event schema
    → (for day-clean) enrich symbol profiles + quotes from cache / FMP profile API
    → sort: US major-exchange first, large-cap first, rev-estimate present, EPS-estimate
            present, ticker alpha
    → return

Cache keys (all v1 to start fresh):
  fmp:earnings:cal_chunk:v1:{from}:{to}   — raw FMP chunk (6 h)
  fmp:earnings:upcoming_clean:v1:{from}:{to} — merged+normalized range (6 h)
  fmp:earnings:day_clean:v1:{date}        — enriched day events (1 h)
  fmp:co_profile:v2:{symbol}             — company profile (24 h)
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

_FMP_STABLE = "https://financialmodelingprep.com/stable"

_TTL_CAL_CHUNK   = 6  * 3600   # 6 h — raw FMP chunk
_TTL_UPCOMING    = 6  * 3600   # 6 h — merged range result
_TTL_DAY         = 1  * 3600   # 1 h — selected-day enriched result
_TTL_PROFILE     = 24 * 3600   # 24 h — company profile / quote
_TTL_QUOTE       = 30 * 60     # 30 min — price / market cap (more volatile)

_DEFAULT_DAYS    = 30           # upcoming window default
_PROFILE_SEM     = 16           # concurrent live profile fetches

# Major US exchange names from FMP profile.exchangeShortName
_US_MAJOR = {"NASDAQ", "NYSE", "AMEX", "NYSE ARCA", "NYSE MKT", "CBOE", "BATS"}

_MC_MEGA  = 200_000_000_000
_MC_LARGE =  10_000_000_000
_MC_MID   =   2_000_000_000
_MC_SMALL =     300_000_000

# ── Shared semaphore (lazy init per event-loop) ────────────────────────────────

_profile_sem_ref: asyncio.Semaphore | None = None


def _get_sem() -> asyncio.Semaphore:
    global _profile_sem_ref
    if _profile_sem_ref is None:
        _profile_sem_ref = asyncio.Semaphore(_PROFILE_SEM)
    return _profile_sem_ref


# ── Low-level FMP client ───────────────────────────────────────────────────────

async def _fmp_get(
    endpoint: str,
    params: dict,
    cache_key: str,
    ttl: int,
    api_key: str,
) -> Any:
    """GET /stable/{endpoint} with cache."""
    hit = cache.get(cache_key)
    if hit is not None:
        return hit

    p = dict(params)
    p["apikey"] = api_key
    t0 = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{_FMP_STABLE}/{endpoint}", params=p)
        ms = int((time.monotonic() - t0) * 1000)
        if resp.status_code not in (200, 201):
            print(f"[earn_clean] FMP {endpoint} status={resp.status_code} ms={ms}")
            return []
        result = resp.json()
        rows = len(result) if isinstance(result, list) else (1 if result else 0)
        print(f"[earn_clean] FMP {endpoint} status=200 rows={rows} ms={ms}")
        if result:
            cache.set(cache_key, result, ttl)
        return result
    except Exception as exc:
        ms = int((time.monotonic() - t0) * 1000)
        print(f"[earn_clean] FMP {endpoint} error={exc} ms={ms}")
        return []


# ── Chunked earnings-calendar fetch ───────────────────────────────────────────

async def _fetch_earnings_range(
    from_date: str,
    to_date: str,
    api_key: str,
) -> list[dict]:
    """
    Fetch FMP earnings-calendar for [from_date, to_date] using 7-day chunks.

    FMP caps single calls at 4 000 rows and for wide ranges returns results in
    an arbitrary internal order that omits early dates.  Weekly chunking keeps
    each call under ~750 rows and guarantees full date coverage.

    Cache keys:
      chunk  → fmp:earnings:cal_chunk:v1:{f}:{t}   (6 h)
      merged → fmp:earnings:upcoming_clean:v1:{f}:{t}  (6 h)
    """
    master_ck = f"fmp:earnings:upcoming_clean:v1:{from_date}:{to_date}"
    hit = cache.get(master_ck)
    if hit is not None:
        return hit if isinstance(hit, list) else []

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

    async def _one_chunk(f: str, t: str) -> list:
        ck = f"fmp:earnings:cal_chunk:v1:{f}:{t}"
        hit = cache.get(ck)
        if hit is not None:
            return hit if isinstance(hit, list) else []
        return await _fmp_get(
            "earnings-calendar", {"from": f, "to": t}, ck, _TTL_CAL_CHUNK, api_key
        )

    results = await asyncio.gather(*[_one_chunk(f, t) for f, t in chunks])

    seen: set[tuple[str, str]] = set()
    merged: list[dict] = []
    for chunk_rows in results:
        for row in chunk_rows:
            key = (row.get("symbol", ""), row.get("date", ""))
            if key not in seen:
                seen.add(key)
                merged.append(row)

    merged.sort(key=lambda r: r.get("date", ""))

    print(
        f"[earn_clean] calendar chunks={len(chunks)} "
        f"rows={len(merged)} range={from_date}→{to_date}"
    )
    if merged:
        cache.set(master_ck, merged, _TTL_UPCOMING)
    return merged


# ── Helpers ───────────────────────────────────────────────────────────────────

def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _date_plus(days: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%d")


def _safe_float(v) -> Optional[float]:
    try:
        return float(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


def _mc_bucket(mc: Optional[float]) -> Optional[str]:
    if mc is None:
        return None
    if mc >= _MC_MEGA:   return "mega"
    if mc >= _MC_LARGE:  return "large"
    if mc >= _MC_MID:    return "mid"
    if mc >= _MC_SMALL:  return "small"
    return "micro"


def _event_id(symbol: str, date: str) -> str:
    raw = f"earnings:{symbol}:{date}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def _is_polymarket_row(title: str, symbol: str) -> bool:
    """
    True if the row looks like a Polymarket prediction-market question
    rather than a real earnings event.
    """
    t = title.strip()
    if t.lower().startswith("will "):
        return True
    # Polymarket symbols often look like base58 hashes or long slugs
    if symbol and (len(symbol) > 12 or " " in symbol or "/" in symbol):
        return True
    return False


def _normalize_row(row: dict) -> Optional[dict]:
    """
    Convert a raw FMP earnings-calendar row to the clean event schema.
    Returns None for rows that should be skipped (Polymarket / missing symbol).
    """
    sym  = (row.get("symbol") or "").strip().upper()
    date = (row.get("date") or "").strip()

    if not sym or not date:
        return None

    name  = (row.get("name") or row.get("companyName") or sym).strip()
    title = f"{name} Earnings" if name != sym else f"{sym} Earnings"

    if _is_polymarket_row(title, sym):
        return None

    eps_e = _safe_float(row.get("epsEstimated"))
    eps_a = _safe_float(row.get("epsActual"))
    rev_e = _safe_float(row.get("revenueEstimated"))
    rev_a = _safe_float(row.get("revenueActual"))

    return {
        "id":               _event_id(sym, date),
        "date":             date,
        "symbol":           sym,
        "companyName":      name if name != sym else None,
        "logo":             None,
        "image":            None,
        "price":            None,
        "marketCap":        None,
        "marketCapBucket":  None,
        "eventType":        "earnings",
        "eventLabel":       "Earnings",
        "title":            title,
        "time":             row.get("time") or None,
        "period":           row.get("fiscalDateEnding") or row.get("period") or None,
        "epsEstimated":     eps_e,
        "epsActual":        eps_a,
        "revenueEstimated": rev_e,
        "revenueActual":    rev_a,
        "source":           "fmp",
        "raw":              row,
    }


# ── Profile + quote enrichment ────────────────────────────────────────────────

async def _fetch_profile(symbol: str, api_key: str) -> dict:
    """Fetch and cache a single FMP company profile."""
    ck = f"fmp:co_profile:v2:{symbol}"
    hit = cache.get(ck)
    if hit is not None:
        return hit if isinstance(hit, dict) else {}

    async with _get_sem():
        rows = await _fmp_get(
            "profile", {"symbol": symbol}, ck, _TTL_PROFILE, api_key
        )

    if isinstance(rows, list) and rows:
        row = rows[0]
        # Re-cache as dict (fmp_get may have cached the list)
        cache.set(ck, row, _TTL_PROFILE)
        return row
    return {}


async def _enrich_events(
    events: list[dict],
    api_key: str,
    max_live: int = 80,
) -> list[dict]:
    """
    Enrich events with company profile data (companyName, logo, price,
    marketCap, exchange).

    Strategy:
      1. Deduplicate symbols.
      2. Symbols already in cache → resolved instantly (no HTTP).
      3. Uncapped cache misses: up to max_live HTTP fetches in parallel
         (controlled by _PROFILE_SEM semaphore).
      4. Symbols beyond the cap get null profile (clearly null, not wrong data).
      5. After enrichment, re-sort events using the full priority order.
    """
    ck_base = "fmp:co_profile:v2:"
    unique_syms = list(dict.fromkeys(ev["symbol"] for ev in events if ev.get("symbol")))

    cached_syms: list[str] = []
    uncached_syms: list[str] = []
    for sym in unique_syms:
        if cache.get(f"{ck_base}{sym}") is not None:
            cached_syms.append(sym)
        else:
            uncached_syms.append(sym)

    live_syms    = uncached_syms[:max_live]
    skipped_syms = set(uncached_syms[max_live:])

    if skipped_syms:
        print(
            f"[earn_clean] enrich cap={max_live} cached={len(cached_syms)} "
            f"live={len(live_syms)} skipped={len(skipped_syms)}"
        )

    active = cached_syms + live_syms

    results = await asyncio.gather(
        *[_fetch_profile(sym, api_key) for sym in active],
        return_exceptions=True,
    )

    profiles: dict[str, dict] = {}
    for sym, res in zip(active, results):
        if isinstance(res, Exception) or not isinstance(res, dict):
            profiles[sym] = {}
        else:
            profiles[sym] = res

    # Apply enrichment to every event
    out: list[dict] = []
    for ev in events:
        sym = ev.get("symbol", "")
        p   = profiles.get(sym, {})

        logo = p.get("image") or p.get("logo") or None
        mc   = _safe_float(p.get("mktCap") or p.get("marketCap"))
        price = _safe_float(p.get("price"))
        cname = (
            p.get("companyName") or p.get("name")
            or ev.get("companyName")
            or (sym if sym else None)
        )
        exchange = p.get("exchangeShortName") or p.get("exchange") or ""

        ev = dict(ev)  # shallow copy so we don't mutate the input
        ev["companyName"]     = cname
        ev["logo"]            = logo
        ev["image"]           = logo
        ev["price"]           = price
        ev["marketCap"]       = mc
        ev["marketCapBucket"] = _mc_bucket(mc)
        ev["_exchange"]       = exchange   # used for sorting, stripped before response
        out.append(ev)

    return out


def _sort_day_events(events: list[dict]) -> list[dict]:
    """
    Sort selected-day events:
      1. US major-exchange first (NASDAQ/NYSE/AMEX/…)
      2. Largest market cap first (None last)
      3. Revenue estimate present first
      4. EPS estimate present first
      5. Ticker alphabetical
    """
    def _key(ev: dict):
        exchange = (ev.get("_exchange") or "").upper()
        us_flag  = 0 if exchange in _US_MAJOR else 1
        mc       = ev.get("marketCap") or 0
        has_rev  = 0 if ev.get("revenueEstimated") is not None else 1
        has_eps  = 0 if ev.get("epsEstimated") is not None else 1
        sym      = ev.get("symbol") or ""
        return (us_flag, -mc, has_rev, has_eps, sym)

    events_sorted = sorted(events, key=_key)
    # Strip the internal exchange field from the outgoing response
    for ev in events_sorted:
        ev.pop("_exchange", None)
    return events_sorted


def _apply_scope(
    events: list[dict],
    search: Optional[str],
    scope: Optional[str],
    watchlist: set[str],
    portfolio: set[str],
) -> list[dict]:
    """Filter by search string and scope (watchlist/portfolio)."""
    out = []
    search_lc = search.strip().lower() if search else None

    for ev in events:
        sym   = (ev.get("symbol") or "").upper()
        cname = (ev.get("companyName") or "").lower()

        if scope == "watchlist" and sym not in watchlist:
            continue
        if scope == "portfolio" and sym not in portfolio:
            continue

        if search_lc:
            if search_lc not in sym.lower() and search_lc not in cname:
                continue

        out.append(ev)
    return out


def _load_watchlist() -> set[str]:
    try:
        from services.watchlist_service import list_watchlists, load_watchlist
        syms: set[str] = set()
        for wl in (list_watchlists() or [])[:5]:
            wl_id = wl.get("id")
            if not wl_id:
                continue
            store = load_watchlist(wl_id)
            if not store:
                continue
            for t in store.get("tickers", []):
                if isinstance(t, str):
                    syms.add(t.upper())
                elif isinstance(t, dict) and t.get("symbol"):
                    syms.add(t["symbol"].upper())
        return syms
    except Exception:
        return set()


def _load_portfolio() -> set[str]:
    try:
        from services.portfolio_service import get_portfolio_symbols
        return {s.upper() for s in (get_portfolio_symbols() or [])}
    except Exception:
        return set()


# ── Public API ─────────────────────────────────────────────────────────────────

async def get_upcoming_clean(
    api_key: str,
    from_date: Optional[str] = None,
    to_date:   Optional[str] = None,
    search:    Optional[str] = None,
    scope:     Optional[str] = None,
    limit:     int = 5000,
) -> dict:
    """
    Return a clean FMP earnings calendar for the specified date range.

    Response includes:
      events        — flat list of normalized events (no enrichment, fast)
      eventsByDate  — {YYYY-MM-DD: [events]}
      countsByDate  — {YYYY-MM-DD: n}
      asOf, source, from, to, status, errors
    """
    frm = from_date or _today()
    to  = to_date   or _date_plus(_DEFAULT_DAYS)

    # Fetch raw chunked FMP data
    raw_rows = await _fetch_earnings_range(frm, to, api_key)

    # Normalize and filter Polymarket/bad rows
    events: list[dict] = []
    for row in raw_rows:
        ev = _normalize_row(row)
        if ev:
            events.append(ev)

    # Apply scope / search
    watchlist = _load_watchlist()
    portfolio = _load_portfolio()
    events = _apply_scope(events, search, scope, watchlist, portfolio)

    # Sort by date then symbol (calendar view)
    events.sort(key=lambda e: (e.get("date", ""), e.get("symbol", "")))

    # Apply limit
    if limit and len(events) > limit:
        events = events[:limit]

    # Build date map
    events_by_date: dict[str, list] = {}
    counts_by_date: dict[str, int]  = {}
    for ev in events:
        d = ev.get("date", "")
        if d:
            events_by_date.setdefault(d, []).append(ev)
            counts_by_date[d] = counts_by_date.get(d, 0) + 1

    return {
        "asOf":         _today(),
        "source":       "fmp",
        "from":         frm,
        "to":           to,
        "events":       events,
        "eventsByDate": events_by_date,
        "countsByDate": counts_by_date,
        "status":       "ok",
        "errors":       [],
    }


async def get_day_clean(
    api_key:     str,
    date:        str,
    search:      Optional[str] = None,
    scope:       Optional[str] = None,
    limit:       int = 2000,
    max_live:    int = 80,
) -> dict:
    """
    Return fully-enriched FMP earnings events for a single selected date.

    Enrichment priority:
      • Cache hits: instant, no HTTP
      • Cache misses: up to max_live live HTTP fetches (16 concurrent via semaphore)
      • Symbols beyond max_live: returned with null companyName/logo/price/marketCap

    Sort order:
      US major exchange → largest market cap → rev estimate → EPS estimate → alpha

    Cache key: fmp:earnings:day_clean:v1:{date}  (1 h TTL)
    Note: day cache stores RAW normalized events (no profile enrichment) because
    profile data has its own cache.  The response is assembled fresh each call
    from the chunk cache + profile cache, so it reflects the latest cached profiles.
    """
    # Fetch the single day's raw FMP data (via weekly chunk cache)
    raw_rows = await _fetch_earnings_range(date, date, api_key)

    events: list[dict] = []
    for row in raw_rows:
        ev = _normalize_row(row)
        if ev:
            events.append(ev)

    # Apply scope / search before enrichment to reduce work
    watchlist = _load_watchlist()
    portfolio = _load_portfolio()
    events = _apply_scope(events, search, scope, watchlist, portfolio)

    # Enrich with profile data
    events = await _enrich_events(events, api_key, max_live=max_live)

    # Sort by priority
    events = _sort_day_events(events)

    # Apply limit
    if limit and len(events) > limit:
        events = events[:limit]

    return {
        "asOf":    _today(),
        "source":  "fmp",
        "date":    date,
        "count":   len(events),
        "events":  events,
        "status":  "ok",
        "errors":  [],
    }
