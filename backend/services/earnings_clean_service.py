"""
earnings_clean_service.py — Safe FMP-only Upcoming Earnings service (v2).

Safety architecture (replaces the burst-prone v1):
  • ONE shared httpx.AsyncClient per top-level request — no new client per symbol.
  • SEQUENTIAL 7-day chunk fetches — no asyncio.gather burst on calendar calls.
  • Max 5 FMP calendar calls per request (30-day window / 7-day chunks).
  • Profile enrichment: opt-in only, concurrency=5 semaphore, hard cap 50 live fetches.
  • Circuit breaker: 429 → block all FMP calls from THIS service for 60 s.
    Does NOT touch FMP clients used by Home / Sectors / Macro routes.
  • All FMP errors are contained: return partial data, never propagate exceptions.

Public API:
  get_upcoming_clean(...)  → calendar counts + flat event list, logos via URL pattern
  get_day_clean(...)       → single-day events with full enrichment (name, logo, price, mktcap)

Response always includes: status, source, fmpCallsUsed, rateLimited, errors.
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

# ── Constants ──────────────────────────────────────────────────────────────────

_FMP_STABLE         = "https://financialmodelingprep.com/stable"
_FMP_LOGO_BASE      = "https://financialmodelingprep.com/image-stock"

_TTL_CAL_CHUNK      = 6  * 3600    # 6 h  — raw FMP chunk
_TTL_UPCOMING       = 6  * 3600    # 6 h  — merged calendar result
_TTL_PROFILE        = 24 * 3600    # 24 h — company profile

_DEFAULT_DAYS       = 14            # default upcoming window (2 weeks)
_MAX_RANGE_DAYS     = 30            # hard cap on date range (30 days)
_MAX_CHUNKS         = 5             # max sequential FMP calendar calls (5 × 7d = 35d covers 30d)

_MAX_LIVE_DEFAULT   = 30            # default live profile fetches for day-clean enrichment
_MAX_LIVE_CAP       = 50            # absolute ceiling for live profile fetches
_ENRICH_CONCURRENCY = 5             # max concurrent live profile HTTP calls
_FMP_TIMEOUT        = 8.0           # seconds per FMP HTTP call (generous for profile)
_CB_BLOCK_SECS      = 60            # 429 circuit-breaker block duration

_US_MAJOR = {"NASDAQ", "NYSE", "AMEX", "NYSE ARCA", "NYSE MKT", "CBOE", "BATS"}

_MC_MEGA  = 200_000_000_000
_MC_LARGE =  10_000_000_000
_MC_MID   =   2_000_000_000
_MC_SMALL =     300_000_000


# ── Circuit breaker (private — this service only) ─────────────────────────────

_fmp_block_until: float = 0.0   # monotonic timestamp; 0.0 = not blocked


def _is_blocked() -> bool:
    return time.monotonic() < _fmp_block_until


def _set_blocked() -> None:
    global _fmp_block_until
    _fmp_block_until = time.monotonic() + _CB_BLOCK_SECS
    print(f"[earn_clean] ⚠ 429 — blocking FMP for {_CB_BLOCK_SECS}s (this service only)")


# ── Low-level FMP call (shared client, circuit-breaker aware) ─────────────────

async def _fmp_get(
    endpoint: str,
    params: dict,
    cache_key: str,
    ttl: int,
    api_key: str,
    client: httpx.AsyncClient,
    call_counter: list[int],
) -> tuple[Any, bool]:
    """
    GET /stable/{endpoint} with:
      • cache-first (returns immediately on hit, no HTTP)
      • circuit breaker check (returns [], True when blocked)
      • 429 detection → triggers circuit breaker
      • timeout handling (returns [], False)

    Returns (result, rate_limited).
    """
    hit = cache.get(cache_key)
    if hit is not None:
        return hit, False

    if _is_blocked():
        print(f"[earn_clean] CB active — skipping {endpoint}")
        return [], True

    p = dict(params)
    p["apikey"] = api_key
    call_counter[0] += 1
    t0 = time.monotonic()

    try:
        resp = await client.get(f"{_FMP_STABLE}/{endpoint}", params=p)
        ms = int((time.monotonic() - t0) * 1000)

        if resp.status_code == 429:
            _set_blocked()
            print(f"[earn_clean] FMP {endpoint} 429 ms={ms}")
            return [], True

        if resp.status_code not in (200, 201):
            print(f"[earn_clean] FMP {endpoint} status={resp.status_code} ms={ms}")
            return [], False

        result = resp.json()
        rows = len(result) if isinstance(result, list) else (1 if result else 0)
        print(f"[earn_clean] FMP {endpoint} 200 rows={rows} ms={ms}")
        if result:
            cache.set(cache_key, result, ttl)
        return result, False

    except httpx.TimeoutException:
        ms = int((time.monotonic() - t0) * 1000)
        print(f"[earn_clean] FMP {endpoint} timeout ms={ms}")
        return [], False
    except Exception as exc:
        ms = int((time.monotonic() - t0) * 1000)
        print(f"[earn_clean] FMP {endpoint} error={exc} ms={ms}")
        return [], False


# ── Sequential chunked calendar fetch ─────────────────────────────────────────

async def _fetch_earnings_range(
    from_date: str,
    to_date: str,
    api_key: str,
    client: httpx.AsyncClient,
    call_counter: list[int],
) -> tuple[list[dict], bool]:
    """
    Fetch FMP earnings-calendar in sequential 7-day chunks.

    Hard constraints:
      • Sequential — no asyncio.gather burst.
      • Max _MAX_CHUNKS (5) FMP calls; extra days beyond 35 are silently dropped
        (caller must clamp before calling).
      • On 429, stops immediately and returns partial data.

    Returns (rows, rate_limited).
    """
    master_ck = f"fmp:earncln:cal:v3:{from_date}:{to_date}"
    hit = cache.get(master_ck)
    if hit is not None:
        return (hit if isinstance(hit, list) else []), False

    try:
        start = datetime.strptime(from_date, "%Y-%m-%d").date()
        end   = datetime.strptime(to_date,   "%Y-%m-%d").date()
    except ValueError:
        return [], False

    chunks: list[tuple[str, str]] = []
    cur = start
    while cur <= end and len(chunks) < _MAX_CHUNKS:
        chunk_end = min(cur + timedelta(days=6), end)
        chunks.append((cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        cur = chunk_end + timedelta(days=1)

    seen: set[tuple[str, str]] = set()
    merged: list[dict] = []
    rate_limited = False

    for f, t in chunks:                         # ← sequential, NOT gather
        ck = f"fmp:earncln:chunk:v3:{f}:{t}"
        rows, rl = await _fmp_get(
            "earnings-calendar", {"from": f, "to": t},
            ck, _TTL_CAL_CHUNK, api_key, client, call_counter,
        )
        if rl:
            rate_limited = True
            break                               # stop on 429 — don't burn more quota
        for row in (rows if isinstance(rows, list) else []):
            key = (row.get("symbol", ""), row.get("date", ""))
            if key not in seen:
                seen.add(key)
                merged.append(row)

    merged.sort(key=lambda r: r.get("date", ""))
    print(
        f"[earn_clean] calendar chunks={len(chunks)} rows={len(merged)} "
        f"range={from_date}→{to_date} calls_so_far={call_counter[0]}"
    )
    if merged:
        cache.set(master_ck, merged, _TTL_UPCOMING)
    return merged, rate_limited


# ── Helpers ───────────────────────────────────────────────────────────────────

def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _date_plus(days: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%d")


def _clamp_range(from_date: str, to_date: str) -> tuple[str, str]:
    """Silently clamp to_date so range is at most _MAX_RANGE_DAYS days."""
    try:
        start   = datetime.strptime(from_date, "%Y-%m-%d").date()
        end     = datetime.strptime(to_date,   "%Y-%m-%d").date()
        max_end = start + timedelta(days=_MAX_RANGE_DAYS - 1)
        if end > max_end:
            return from_date, max_end.strftime("%Y-%m-%d")
        return from_date, to_date
    except ValueError:
        return from_date, to_date


def _safe_float(v) -> Optional[float]:
    try:
        return float(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


def _mc_bucket(mc: Optional[float]) -> Optional[str]:
    if mc is None:
        return None
    if mc >= _MC_MEGA:  return "mega"
    if mc >= _MC_LARGE: return "large"
    if mc >= _MC_MID:   return "mid"
    if mc >= _MC_SMALL: return "small"
    return "micro"


def _event_id(symbol: str, date: str) -> str:
    return hashlib.md5(f"earnings:{symbol}:{date}".encode()).hexdigest()[:16]


def _is_polymarket_row(title: str, symbol: str) -> bool:
    if title.strip().lower().startswith("will "):
        return True
    if symbol and (len(symbol) > 12 or " " in symbol or "/" in symbol):
        return True
    return False


def _logo_url(sym: str) -> str:
    """Return the FMP image-stock CDN URL for a ticker — no API call needed."""
    return f"{_FMP_LOGO_BASE}/{sym}.png"


def _normalize_row(row: dict) -> Optional[dict]:
    sym  = (row.get("symbol") or "").strip().upper()
    date = (row.get("date")   or "").strip()
    if not sym or not date:
        return None

    name  = (row.get("name") or row.get("companyName") or sym).strip()
    title = f"{name} Earnings" if name != sym else f"{sym} Earnings"
    if _is_polymarket_row(title, sym):
        return None

    logo = _logo_url(sym)

    return {
        "id":               _event_id(sym, date),
        "date":             date,
        "symbol":           sym,
        "companyName":      name if name != sym else None,
        "logo":             logo,
        "image":            logo,
        "price":            None,
        "changesPercentage": None,
        "marketCap":        None,
        "marketCapBucket":  None,
        "eventType":        "earnings",
        "eventLabel":       "Earnings",
        "title":            title,
        "time":             row.get("time") or None,
        "period":           row.get("fiscalDateEnding") or row.get("period") or None,
        "epsEstimated":     _safe_float(row.get("epsEstimated")),
        "epsActual":        _safe_float(row.get("epsActual")),
        "revenueEstimated": _safe_float(row.get("revenueEstimated")),
        "revenueActual":    _safe_float(row.get("revenueActual")),
        "source":           "fmp",
        "raw":              row,
    }


# ── Safe profile enrichment (concurrency=5, shared client) ───────────────────

async def _enrich_events(
    events: list[dict],
    api_key: str,
    client: httpx.AsyncClient,
    call_counter: list[int],
    max_live: int,
    concurrency: int = _ENRICH_CONCURRENCY,
) -> tuple[list[dict], bool]:
    """
    Enrich events with FMP company profile data (companyName, logo, price, mktcap, etc).

    Rules:
      • Cache hits: resolved instantly — no HTTP, don't count against max_live.
      • Live fetches: capped at max_live (default 30, never > _MAX_LIVE_CAP=50).
      • Concurrency: semaphore(_ENRICH_CONCURRENCY=5) limits simultaneous HTTP calls.
      • Shared client: no new AsyncClient per symbol.
      • 429: circuit breaker triggers, remaining tasks return [] immediately.

    Returns (enriched_events, rate_limited).
    """
    ck_base = "fmp:co_profile:v2:"
    unique_syms = list(dict.fromkeys(ev["symbol"] for ev in events if ev.get("symbol")))

    cached_syms:   list[str] = []
    uncached_syms: list[str] = []
    for sym in unique_syms:
        if cache.get(f"{ck_base}{sym}") is not None:
            cached_syms.append(sym)
        else:
            uncached_syms.append(sym)

    live_syms = uncached_syms[:max_live]
    skipped   = len(uncached_syms) - len(live_syms)
    if skipped:
        print(f"[earn_clean] enrich: cached={len(cached_syms)} live={len(live_syms)} skipped={skipped}")

    all_syms = cached_syms + live_syms
    sem = asyncio.Semaphore(concurrency)

    async def _bounded_profile(sym: str) -> tuple[str, dict]:
        ck = f"{ck_base}{sym}"
        hit = cache.get(ck)
        if hit is not None:
            return sym, (hit if isinstance(hit, dict) else {})
        async with sem:
            rows, _ = await _fmp_get(
                "profile", {"symbol": sym},
                ck, _TTL_PROFILE, api_key, client, call_counter,
            )
        if isinstance(rows, list) and rows:
            row = rows[0]
            cache.set(ck, row, _TTL_PROFILE)
            return sym, row
        return sym, {}

    raw_results = await asyncio.gather(
        *[_bounded_profile(sym) for sym in all_syms],
        return_exceptions=True,
    )

    profiles: dict[str, dict] = {}
    for r in raw_results:
        if isinstance(r, Exception):
            continue
        sym, prof = r
        profiles[sym] = prof

    rate_limited = _is_blocked()

    out: list[dict] = []
    for ev in events:
        sym = ev.get("symbol", "")
        p   = profiles.get(sym, {})
        mc  = _safe_float(p.get("mktCap") or p.get("marketCap"))
        ev  = dict(ev)

        # Company identity — profile wins; fall back to ticker-derived logo
        profile_logo = p.get("image") or p.get("logo") or None
        ev["companyName"]      = (
            p.get("companyName") or p.get("name")
            or ev.get("companyName") or (sym or None)
        )
        ev["logo"]             = profile_logo or ev.get("logo") or _logo_url(sym)
        ev["image"]            = ev["logo"]
        ev["price"]            = _safe_float(p.get("price"))
        ev["changesPercentage"] = _safe_float(p.get("changePercentage") or p.get("changesPercentage") or p.get("changes"))
        ev["marketCap"]        = mc
        ev["marketCapBucket"]  = _mc_bucket(mc)
        ev["sector"]           = p.get("sector") or None
        ev["industry"]         = p.get("industry") or None
        ev["_exchange"]        = p.get("exchangeShortName") or p.get("exchange") or ""
        out.append(ev)

    return out, rate_limited


# ── Sort helpers ──────────────────────────────────────────────────────────────

def _sort_enriched(events: list[dict]) -> list[dict]:
    """US major → marketCap desc → revenueEst → epsEst → symbol."""
    def _key(ev: dict):
        ex      = (ev.get("_exchange") or "").upper()
        us      = 0 if ex in _US_MAJOR else 1
        mc      = ev.get("marketCap") or 0
        has_rev = 0 if ev.get("revenueEstimated") is not None else 1
        has_eps = 0 if ev.get("epsEstimated")      is not None else 1
        return (us, -mc, has_rev, has_eps, ev.get("symbol") or "")
    sorted_evs = sorted(events, key=_key)
    for ev in sorted_evs:
        ev.pop("_exchange", None)
    return sorted_evs


def _sort_basic(events: list[dict]) -> list[dict]:
    """revenueEstimated desc (None last), then symbol alpha."""
    def _key(ev: dict):
        rev = ev.get("revenueEstimated")
        return (-(rev if rev is not None else -1e18), ev.get("symbol") or "")
    return sorted(events, key=_key)


# ── Scope / search ────────────────────────────────────────────────────────────

def _apply_scope(
    events: list[dict],
    search: Optional[str],
    scope: Optional[str],
    watchlist: set[str],
    portfolio: set[str],
) -> list[dict]:
    out = []
    search_lc = search.strip().lower() if search else None
    for ev in events:
        sym   = (ev.get("symbol") or "").upper()
        cname = (ev.get("companyName") or "").lower()
        if scope == "watchlist" and sym not in watchlist:
            continue
        if scope == "portfolio" and sym not in portfolio:
            continue
        if search_lc and search_lc not in sym.lower() and search_lc not in cname:
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
    FMP earnings calendar for a date range — counts + event list with logos.

    Hard limits:
      • Default window: today → today + 14 days.
      • Max window: 30 days (silently clamped — never 400, just truncated).
      • Max 5 sequential FMP calendar calls (5 × 7-day chunks).
      • Logos populated via FMP image-stock CDN URL (no API call needed).
      • No profile enrichment (names are ticker when FMP doesn't include them).
      • One shared AsyncClient for the whole request lifecycle.

    Response: { status, source, fmpCallsUsed, rateLimited, errors,
                from, to, events, eventsByDate, countsByDate }
    """
    frm = from_date or _today()
    to  = to_date   or _date_plus(_DEFAULT_DAYS)
    frm, to = _clamp_range(frm, to)

    call_counter: list[int] = [0]

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(_FMP_TIMEOUT),
        follow_redirects=True,
    ) as client:
        raw_rows, rate_limited = await _fetch_earnings_range(
            frm, to, api_key, client, call_counter,
        )

    events: list[dict] = []
    for row in raw_rows:
        ev = _normalize_row(row)
        if ev:
            events.append(ev)

    watchlist = _load_watchlist()
    portfolio = _load_portfolio()
    events = _apply_scope(events, search, scope, watchlist, portfolio)
    events.sort(key=lambda e: (e.get("date", ""), e.get("symbol", "")))

    # Build date aggregations from the FULL event set BEFORE applying the flat limit.
    # countsByDate and eventsByDate must reflect all FMP events in the range, not
    # just the first `limit` rows — otherwise days beyond the limit appear empty.
    events_by_date: dict[str, list] = {}
    counts_by_date: dict[str, int]  = {}
    for ev in events:
        d = ev.get("date", "")
        if d:
            events_by_date.setdefault(d, []).append(ev)
            counts_by_date[d] = counts_by_date.get(d, 0) + 1

    # Trim flat events list only (calendar tiles use countsByDate; day detail uses eventsByDate)
    if limit and len(events) > limit:
        events = events[:limit]

    return {
        "asOf":         _today(),
        "source":       "fmp",
        "from":         frm,
        "to":           to,
        "count":        len(events),
        "events":       events,
        "eventsByDate": events_by_date,
        "countsByDate": counts_by_date,
        "status":       "partial" if rate_limited else "ok",
        "fmpCallsUsed": call_counter[0],
        "rateLimited":  rate_limited,
        "errors":       (["FMP rate limit hit — partial data"] if rate_limited else []),
    }


async def get_day_clean(
    api_key:  str,
    date:     str,
    search:   Optional[str] = None,
    scope:    Optional[str] = None,
    limit:    int  = 500,
    enrich:   bool = True,
    max_live: int  = _MAX_LIVE_DEFAULT,
) -> dict:
    """
    FMP earnings events for a single selected date with full enrichment.

    enrich=True (default):
      • 1 FMP calendar call + up to max_live profile calls.
      • Returns companyName, logo (URL or CDN), price, changesPercentage, marketCap,
        sector, industry for each event.
      • Profiles are cached 24 h — subsequent requests for the same symbols are instant.
      • Concurrency: 5 simultaneous profile HTTP calls (semaphore).
      • Shared AsyncClient — no new client per symbol.
      • Timeout: 8 s per call.
      • 429: circuit breaker activates, enrichment stops, returns partial data.
      • Sort: US exchange → marketCap desc → revenueEst → epsEst → symbol.

    enrich=False:
      • 1 FMP calendar call only. Logos still populated via CDN URL.
      • Sort: revenueEstimated desc → symbol alpha.

    Response: { status, source, fmpCallsUsed, rateLimited, errors, date, count, events }
    """
    max_live = min(max(0, max_live), _MAX_LIVE_CAP)
    call_counter: list[int] = [0]
    rate_limited = False

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(_FMP_TIMEOUT),
        follow_redirects=True,
    ) as client:
        raw_rows, rate_limited = await _fetch_earnings_range(
            date, date, api_key, client, call_counter,
        )

        events: list[dict] = []
        for row in raw_rows:
            ev = _normalize_row(row)
            if ev:
                events.append(ev)

        watchlist = _load_watchlist()
        portfolio = _load_portfolio()
        events = _apply_scope(events, search, scope, watchlist, portfolio)

        if enrich and not rate_limited and events:
            events, enrich_rl = await _enrich_events(
                events, api_key, client, call_counter, max_live,
            )
            if enrich_rl:
                rate_limited = True
            events = _sort_enriched(events)
        else:
            events = _sort_basic(events)

    if limit and len(events) > limit:
        events = events[:limit]

    return {
        "asOf":         _today(),
        "source":       "fmp",
        "date":         date,
        "count":        len(events),
        "events":       events,
        "status":       "partial" if rate_limited else "ok",
        "fmpCallsUsed": call_counter[0],
        "rateLimited":  rate_limited,
        "errors":       (["FMP rate limit hit — partial data"] if rate_limited else []),
    }


# ══════════════════════════════════════════════════════════════════════════════
# WEEK-CLEAN — This Week curated earnings view
# ══════════════════════════════════════════════════════════════════════════════

# ── Theme baskets (deterministic — no AI) ─────────────────────────────────────

_THEME_BASKETS: dict[str, set[str]] = {
    "AI Buildout": {
        "NVDA", "AMD", "AVGO", "MRVL", "MU", "TSM", "ASML", "ARM",
        "SMCI", "DELL", "HPE", "ANET", "VRT", "ETN", "ORCL",
        "GOOGL", "MSFT", "AMZN", "META",
    },
    "AI Data Center / Power": {
        "VRT", "ETN", "PWR", "CEG", "VST", "NRG", "GE", "GEV",
        "SMR", "OKLO", "DELL", "SMCI",
    },
    "Semiconductor Bottlenecks": {
        "ASML", "AMAT", "LRCX", "KLAC", "TER", "AEHR", "ACLS", "FORM", "COHR",
    },
    "AI Networking": {
        "ANET", "AVGO", "MRVL", "CIEN", "CSCO", "NOK", "ERIC",
    },
    "Cloud / Software Infra": {
        "MSFT", "AMZN", "GOOGL", "META", "ORCL", "SNOW", "DDOG",
        "NET", "CRWD", "PANW", "MDB", "NOW",
    },
    "Aerospace / Defense / Space": {
        "RKLB", "ASTS", "LMT", "RTX", "NOC", "BA", "LHX",
        "TDG", "HEI", "PLTR", "KTOS", "IRDM",
    },
    "Nuclear / Power / Electrification": {
        "CEG", "VST", "NRG", "ETN", "PWR", "GEV", "SMR", "OKLO", "BWXT",
    },
    "Crypto / Fintech": {
        "COIN", "HOOD", "MSTR", "SQ", "PYPL", "IBKR", "CME",
    },
}

_THEME_ANCHORS:     set[str] = {"NVDA", "GOOGL", "AMZN", "MSFT", "META", "AVGO", "TSM", "ASML"}
_THEME_BOTTLENECKS: set[str] = {
    "MU", "MRVL", "VRT", "ETN", "ANET", "LRCX", "KLAC", "AMAT", "RKLB", "ASTS", "PWR", "GEV",
}

_ALL_THEME_SYMS: set[str] = {sym for basket in _THEME_BASKETS.values() for sym in basket}

# ── Quality / liquidity scoring constants ─────────────────────────────────────

# Matches preferred shares, warrants, units by symbol suffix pattern.
# Intentionally conservative — avoids hitting normal tickers like BRK.B, GOOGL.
_QUAL_PREF_RE = re.compile(
    r'[-.]P[A-Z]$'     # -PA, -PB, .PA … preferred class
    r'|^.{2,}-P$'      # TICKER-P  (e.g. JPM-P, WFC-P)
    r'|[/.]WS$'        # /WS, .WS  warrant
    r'|[-.]WT$'        # -WT       warrant alt
    r'|\^'             # ^ in symbol (Bloomberg-style preferred notation)
    r'|\.PFD$'         # .PFD      preferred
    r'|[-.]U$',        # -U, .U    SPAC unit
    re.IGNORECASE,
)

# Company name keywords that indicate preferred/warrant instruments
_QUAL_NAME_PREF_RE = re.compile(r'\bpreferred\b|\bwarrant\b', re.IGNORECASE)

# ADR / foreign ordinary share indication in company name (softer penalty)
_QUAL_NAME_ADR_RE  = re.compile(r'\bADR\b|\bordinary\s+share', re.IGNORECASE)

# Score adjustments
_QL_MAJOR_US_BOOST  =  15   # NYSE / Nasdaq / AMEX listed
_QL_OTC_PENALTY     = -10   # OTC / foreign / unknown (unless theme-exempt or large/mega cap)
_QL_PREF_SYM_HIT    = -25   # high-confidence preferred / warrant from symbol pattern
_QL_PREF_NAME_HIT   = -20   # preferred / warrant detected in company name
_QL_ADR_PENALTY     =  -8   # ADR (softer — often legitimate large-cap instruments)

_WEEK_CAND_MAX      = 150  # max raw candidates forwarded to enrichment
_MAX_WEEK_ENRICH    = 120  # max events passed to _enrich_events (cache-first; live cap = 40)
_WEEK_MAX_LIVE      = 40   # hard cap on live FMP profile/quote calls for week-clean
_WEEK_CONCURRENCY   = 2    # semaphore for week-clean enrichment
_WEEK_LIMIT_DEFAULT = 8    # default per-session cap
_WEEK_TOTAL_DEFAULT = 60   # default topEvents cap
_WEEK_LIMIT_MAX     = 15   # hard ceiling for limit_per_session
_WEEK_TOTAL_MAX     = 100  # hard ceiling for max_total

_WEEKDAY_NAMES = {0: "Monday", 1: "Tuesday", 2: "Wednesday",
                  3: "Thursday", 4: "Friday", 5: "Saturday", 6: "Sunday"}


# ── Week-clean helpers ─────────────────────────────────────────────────────────

def _symbol_themes(sym: str) -> list[str]:
    """Return list of theme basket names the symbol belongs to."""
    return [name for name, syms in _THEME_BASKETS.items() if sym in syms]


def _session(time_str: Optional[str]) -> str:
    """Map FMP time string to session key."""
    if not time_str:
        return "unknown"
    t = time_str.lower().strip()
    if t in {"bmo", "before market open", "pre-market", "pre market", "pre", "pm"}:
        return "preMarket"
    if t in {"amc", "after market close", "after-hours", "after hours", "after", "ah", "postmarket"}:
        return "afterHours"
    if t in {"dmh", "during market", "during market hours", "during", "midday"}:
        return "duringMarket"
    return "unknown"


def _pre_score(ev: dict, watchlist: set[str], portfolio: set[str]) -> dict:
    """
    Deterministic pre-score using only raw FMP calendar fields + theme knowledge.
    No profile data needed — safe to call before enrichment.

    Theme scoring (capped at 40 total):
      in theme basket: +20 | anchor: +25 | bottleneck: +20

    qualityLiquidity (partial — exchange component added after enrichment):
      preferred/warrant symbol pattern: -25
      preferred/warrant company name:   -20
      ADR / ordinary share name:         -8
      Theme-basket membership cancels any pre-enrichment penalty.
    """
    sym = (ev.get("symbol") or "").upper()
    has_eps = ev.get("epsEstimated") is not None
    has_rev = ev.get("revenueEstimated") is not None

    is_anchor     = sym in _THEME_ANCHORS
    is_bottleneck = sym in _THEME_BOTTLENECKS
    in_theme      = sym in _ALL_THEME_SYMS

    raw_theme = 0
    if in_theme:      raw_theme += 20
    if is_anchor:     raw_theme += 25
    if is_bottleneck: raw_theme += 20
    theme_score = min(raw_theme, 40)   # cap at 40

    is_portfolio = sym in portfolio
    is_watchlist = sym in watchlist
    wp_score = 25 if is_portfolio else (20 if is_watchlist else 0)

    est_score = 0
    if has_eps: est_score += 5
    if has_rev: est_score += 5
    if has_eps and has_rev: est_score += 5

    # ── Quality / liquidity pre-score (symbol + name patterns only) ──────────
    cname = ev.get("companyName") or ""
    ql_pre = 0
    if _QUAL_PREF_RE.search(sym):
        ql_pre = _QL_PREF_SYM_HIT                  # -25: high-confidence preferred/warrant
    elif _QUAL_NAME_PREF_RE.search(cname):
        ql_pre = _QL_PREF_NAME_HIT                  # -20: name says "preferred" or "warrant"
    elif _QUAL_NAME_ADR_RE.search(cname):
        ql_pre = _QL_ADR_PENALTY                    # -8:  ADR / ordinary share

    # Theme-basket membership cancels the pre-enrichment penalty:
    # If the ticker is a known hot theme, anchor, or bottleneck let it through.
    if ql_pre < 0 and (in_theme or is_anchor or is_bottleneck):
        ql_pre = 0

    return {
        "score":        theme_score + wp_score + est_score + ql_pre,
        "isAnchor":     is_anchor,
        "isBottleneck": is_bottleneck,
        "themes":       _symbol_themes(sym),
        "breakdown": {
            "marketCap":            0,        # filled after enrichment
            "theme":                theme_score,
            "watchlistPortfolio":   wp_score,
            "estimateAvailability": est_score,
            "priceMomentum":        0,        # filled after enrichment
            "qualityLiquidity":     ql_pre,   # exchange component added in _apply_full_score
        },
    }


def _apply_full_score(ev: dict, pre: dict) -> tuple[int, dict]:
    """
    Compute final importance score after enrichment adds marketCap / price % / exchange.
    Returns (total_score, updated_breakdown).

    qualityLiquidity (exchange component — only for enriched events):
      Major US exchange (NYSE/Nasdaq/AMEX/…): +15
      OTC / foreign / unknown exchange:       -10
        Exception: skipped if large/mega cap OR in any theme basket.
    """
    mc     = ev.get("marketCap")
    pct    = ev.get("changesPercentage")
    bucket = _mc_bucket(mc)
    sym    = (ev.get("symbol") or "").upper()

    mc_score = {"mega": 40, "large": 30, "mid": 15, "small": 5}.get(bucket or "", 0)

    pm_score = 0
    if pct is not None:
        if pct > 7:
            pm_score = 8
        elif pct > 3:
            pm_score = 5
        elif pct < -5 and bucket in ("mega", "large"):
            pm_score = 5

    # ── Exchange quality component ────────────────────────────────────────────
    ql_pre      = pre["breakdown"].get("qualityLiquidity", 0)
    ql_exchange = 0
    exchange    = (ev.get("_exchange") or "").upper()

    if exchange:  # only present on enriched events
        is_major_us   = exchange in _US_MAJOR
        is_large_plus = bucket in ("mega", "large")
        is_theme_sym  = sym in _ALL_THEME_SYMS or pre.get("isAnchor") or pre.get("isBottleneck")

        if is_major_us:
            ql_exchange = _QL_MAJOR_US_BOOST          # +15
        elif not is_large_plus and not is_theme_sym:
            ql_exchange = _QL_OTC_PENALTY             # -10

    ql_total = ql_pre + ql_exchange

    breakdown = {
        "marketCap":            mc_score,
        "theme":                pre["breakdown"].get("theme", 0),
        "watchlistPortfolio":   pre["breakdown"].get("watchlistPortfolio", 0),
        "estimateAvailability": pre["breakdown"].get("estimateAvailability", 0),
        "priceMomentum":        pm_score,
        "qualityLiquidity":     ql_total,
    }
    return pre["score"] + mc_score + pm_score + ql_exchange, breakdown


def _build_empty_days(week_start, week_end) -> list[dict]:
    """Return a days[] with zero-count slots for each date in range."""
    days = []
    cur = week_start
    while cur <= week_end:
        d_str = cur.strftime("%Y-%m-%d")
        label = cur.strftime("%a, %b ") + str(cur.day)
        days.append({
            "date":         d_str,
            "label":        label,
            "weekday":      _WEEKDAY_NAMES.get(cur.weekday(), ""),
            "count":        0,
            "preMarket":    [],
            "afterHours":   [],
            "duringMarket": [],
            "unknown":      [],
            "entries":      [],
        })
        cur += timedelta(days=1)
    return days


def _decorate_event(ev: dict, pre: dict, score: int, breakdown: dict) -> dict:
    """Attach week-clean fields to a (possibly enriched) normalized event."""
    mc     = ev.get("marketCap")
    bucket = _mc_bucket(mc) or "unknown"
    base   = {k: v for k, v in ev.items() if k != "_exchange"}
    return {
        **base,
        "session":         _session(ev.get("time")),
        "priceChangePct":  ev.get("changesPercentage"),
        "marketCapBucket": bucket,
        "themeTags":       pre.get("themes", []),
        "isThemeAnchor":   pre.get("isAnchor", False),
        "isBottleneck":    pre.get("isBottleneck", False),
        "importanceScore": score,
        "scoreBreakdown":  breakdown,
        "source":          "fmp",
    }


# ── Week-clean candidate helpers ──────────────────────────────────────────────

# OTC/foreign ticker pattern: 4-5 uppercase letters ending in F (foreign) or Y (ADR OTC)
_OTC_TICKER_RE = re.compile(r'^[A-Z]{4,5}[FY]$')

# Broad "fund / trust / preferred / rights / note" guard for company names
_JUNK_NAME_RE = re.compile(
    r'\b(fund|trust\s+preferred|rights|notes?\s+due|unit\s+series|'
    r'acquisition\s+corp|blank\s+check)\b',
    re.IGNORECASE,
)


def _is_raw_candidate(ev: dict, watchlist: set[str], portfolio: set[str]) -> bool:
    """
    Fast pre-filter (no HTTP) — decide whether a raw FMP row is worth enriching.

    Returns True only for events that have a realistic chance of being eligible
    for the curated This Week board.
    """
    sym = (ev.get("symbol") or "").upper()

    # Always keep: explicit theme, anchor, bottleneck
    if sym in _ALL_THEME_SYMS or sym in _THEME_ANCHORS or sym in _THEME_BOTTLENECKS:
        return True

    # Always keep: user watchlist / portfolio
    if sym in watchlist or sym in portfolio:
        return True

    # Hard drop: preferred/warrant/unit symbol pattern
    if _QUAL_PREF_RE.search(sym):
        return False

    # Hard drop: preferred/warrant keyword in company name
    cname = ev.get("companyName") or ""
    if _QUAL_NAME_PREF_RE.search(cname) or _JUNK_NAME_RE.search(cname):
        return False

    # Soft drop: obvious OTC foreign ticker (4-5 letters + F or Y) unless has BOTH estimates
    if _OTC_TICKER_RE.match(sym):
        if ev.get("epsEstimated") is None or ev.get("revenueEstimated") is None:
            return False

    # Pass: has at least one estimate signal (most meaningful filter for unknown names)
    if ev.get("epsEstimated") is not None or ev.get("revenueEstimated") is not None:
        return True

    # Pass: short ticker (≤4 chars) — likely US-listed; enrich and decide later
    if len(sym) <= 4:
        return True

    # Default: exclude (5-letter tickers with no estimates and no theme = likely OTC junk)
    return False


def _is_eligible(ev: dict, pre: dict, watchlist: set[str], portfolio: set[str]) -> bool:
    """
    Post-enrichment eligibility gate for the curated This Week board.

    An event must pass at least one eligibility rule (A-D).
    Hard exclusions override everything except explicit theme / watchlist / portfolio.
    """
    sym      = (ev.get("symbol") or "").upper()
    mc       = ev.get("marketCap")
    exchange = (ev.get("_exchange") or "").upper()
    bucket   = _mc_bucket(mc)
    cname    = (ev.get("companyName") or "").strip()
    themes   = pre.get("themes", [])
    is_anchor    = pre.get("isAnchor", False)
    is_bottleneck = pre.get("isBottleneck", False)

    # Rule B: Explicit theme/anchor/bottleneck — always eligible
    if is_anchor or is_bottleneck or themes:
        return True

    # Rule C: User watchlist / portfolio — always eligible
    if sym in watchlist or sym in portfolio:
        return True

    # ── Hard exclusions (for everything not in theme / watchlist / portfolio) ──

    # Missing or trivial company name (enrichment didn't resolve it)
    if not cname or cname.upper() == sym:
        return False

    # Preferred/warrant pattern guard (belt-and-suspenders after pre-filter)
    if _QUAL_PREF_RE.search(sym) or _QUAL_NAME_PREF_RE.search(cname):
        return False

    # Junk name keywords
    if _JUNK_NAME_RE.search(cname):
        return False

    # Unknown market cap AND unknown/missing exchange → not enough signal
    if (bucket is None or bucket == "unknown") and exchange not in _US_MAJOR:
        return False

    # Missing market cap AND not on a major US exchange → exclude
    if mc is None and exchange not in _US_MAJOR:
        return False

    # ── Eligibility rules ─────────────────────────────────────────────────────

    # Rule A: Major/liquid company
    is_major_us  = exchange in _US_MAJOR
    is_large_mc  = mc is not None and mc >= 5_000_000_000   # ≥ $5 B
    is_mid_plus  = bucket in ("mega", "large", "mid")

    if is_major_us or is_large_mc or is_mid_plus:
        return True

    # Rule D: Known company + both estimates + not OTC-looking ticker
    has_rev      = ev.get("revenueEstimated") is not None
    has_eps      = ev.get("epsEstimated") is not None
    is_otc_like  = bool(_OTC_TICKER_RE.match(sym))

    if has_rev and has_eps and not is_otc_like:
        return True

    return False


# ── Public API: get_week_clean ─────────────────────────────────────────────────

async def get_week_clean(
    api_key:           str,
    week_start:        Optional[str] = None,
    week_end:          Optional[str] = None,
    scope:             Optional[str] = None,
    search:            Optional[str] = None,
    limit_per_session: int = _WEEK_LIMIT_DEFAULT,
    max_total:         int = _WEEK_TOTAL_DEFAULT,
) -> dict:
    """
    FMP earnings calendar for a single business week — curated and scored.

    Safety rules:
      • One week only (Mon–Fri by default) — hard clamped to 7 days max.
      • Max 1-2 FMP calendar calls (one 7-day chunk; cache hit after first call).
      • Two-phase enrichment: pre-score all → enrich top 40 candidates only.
      • Enrichment: concurrency=2, circuit breaker, partial on 429.
      • No AI, no Finnhub, no Polymarket.

    Response: { asOf, source, weekStart, weekEnd, days[], topEvents[], status, errors }
    """
    limit_per_session = min(max(1, limit_per_session), _WEEK_LIMIT_MAX)
    max_total         = min(max(1, max_total), _WEEK_TOTAL_MAX)

    # -- Resolve week bounds
    today_d = datetime.now(timezone.utc).date()
    if week_start:
        try:
            ws = datetime.strptime(week_start, "%Y-%m-%d").date()
        except ValueError:
            ws = today_d - timedelta(days=today_d.weekday())
    else:
        ws = today_d - timedelta(days=today_d.weekday())   # Monday

    if week_end:
        try:
            we = datetime.strptime(week_end, "%Y-%m-%d").date()
        except ValueError:
            we = ws + timedelta(days=4)
    else:
        we = ws + timedelta(days=4)   # Friday

    # Hard clamp: never more than 7 days
    if we > ws + timedelta(days=6):
        we = ws + timedelta(days=6)

    from_str = ws.strftime("%Y-%m-%d")
    to_str   = we.strftime("%Y-%m-%d")

    call_counter: list[int] = [0]
    errors:       list[str] = []
    rate_limited             = False

    # -- Phase 1: Fetch calendar (1-2 FMP calls max, heavily cached)
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(_FMP_TIMEOUT),
        follow_redirects=True,
    ) as client:

        raw_rows, rate_limited = await _fetch_earnings_range(
            from_str, to_str, api_key, client, call_counter,
        )

        # -- Phase 2: Normalize, scope, raw candidate pre-filter
        watchlist = _load_watchlist()
        portfolio = _load_portfolio()

        raw_events: list[dict] = []
        for row in raw_rows:
            ev = _normalize_row(row)
            if ev:
                raw_events.append(ev)

        raw_events = _apply_scope(raw_events, search, scope, watchlist, portfolio)

        if not raw_events:
            return {
                "asOf":      _today(),
                "source":    "fmp",
                "weekStart": from_str,
                "weekEnd":   to_str,
                "days":      _build_empty_days(ws, we),
                "topEvents": [],
                "status":    "partial" if rate_limited else "ok",
                "errors":    errors + (["FMP rate limit hit — partial data"] if rate_limited else []),
            }

        # Phase 2.5: Raw pre-filter — only keep events worth enriching
        # Theme/anchor/bottleneck/watchlist/portfolio always pass.
        # Junk OTC tickers, preferred/warrant patterns, no-estimate unknowns are dropped.
        candidates = [ev for ev in raw_events if _is_raw_candidate(ev, watchlist, portfolio)]

        # Sort candidates: theme/watchlist first, then both-estimates, then by symbol length
        def _cand_key(ev: dict):
            sym = (ev.get("symbol") or "").upper()
            in_theme = sym in _ALL_THEME_SYMS or sym in _THEME_ANCHORS or sym in _THEME_BOTTLENECKS
            in_wp    = sym in watchlist or sym in portfolio
            has_rev  = ev.get("revenueEstimated") is not None
            has_eps  = ev.get("epsEstimated") is not None
            return (
                0 if (in_theme or in_wp) else 1,
                0 if (has_rev and has_eps) else 1,
                0 if has_rev else 1,
                len(sym),         # shorter = more likely major US exchange
            )
        candidates.sort(key=_cand_key)
        candidates = candidates[:_WEEK_CAND_MAX]   # cap at 150

        # -- Phase 3: Pre-score candidates only (no HTTP)
        pre_scores: dict[str, dict] = {}
        for ev in candidates:
            sym = ev.get("symbol", "")
            if sym and sym not in pre_scores:
                pre_scores[sym] = _pre_score(ev, watchlist, portfolio)

        # Select top _MAX_WEEK_ENRICH (120) by pre-score for enrichment
        sym_best_score: dict[str, int] = {}
        for ev in candidates:
            sym = ev.get("symbol", "")
            s   = pre_scores.get(sym, {}).get("score", 0)
            if sym not in sym_best_score or s > sym_best_score[sym]:
                sym_best_score[sym] = s

        enrich_syms: set[str] = set(
            sorted(sym_best_score, key=lambda s: sym_best_score[s], reverse=True)[:_MAX_WEEK_ENRICH]
        )

        # -- Phase 4: Enrich (cache-first, max _WEEK_MAX_LIVE live calls, concurrency=2)
        enrich_pool   = [ev for ev in candidates if ev.get("symbol") in enrich_syms]
        not_enriched  = [ev for ev in candidates if ev.get("symbol") not in enrich_syms]

        if enrich_pool and not rate_limited:
            enrich_pool, enrich_rl = await _enrich_events(
                enrich_pool,
                api_key, client, call_counter,
                max_live=_WEEK_MAX_LIVE,
                concurrency=_WEEK_CONCURRENCY,
            )
            if enrich_rl:
                rate_limited = True
                errors.append("FMP rate limit during enrichment — partial enrichment")

    # -- Phase 5: Eligibility gate + score + decorate
    _empty_pre = {
        "score": 0, "isAnchor": False, "isBottleneck": False,
        "themes": [],
        "breakdown": {"marketCap": 0, "theme": 0, "watchlistPortfolio": 0,
                      "estimateAvailability": 0, "priceMomentum": 0,
                      "qualityLiquidity": 0},
    }
    decorated:  list[dict] = []
    dropped_ct: int        = 0

    # Enriched events: apply eligibility gate, then score
    for ev in enrich_pool:
        sym = ev.get("symbol", "")
        pre = pre_scores.get(sym, _empty_pre)
        if not _is_eligible(ev, pre, watchlist, portfolio):
            dropped_ct += 1
            continue
        score, breakdown = _apply_full_score(ev, pre)
        decorated.append(_decorate_event(ev, pre, score, breakdown))

    # Not-enriched candidates: only include if explicitly theme/watchlist/portfolio
    # (they were in the pre-filtered pool but below the enrichment cap)
    for ev in not_enriched:
        sym = ev.get("symbol", "")
        pre = pre_scores.get(sym, _empty_pre)
        # No profile data → only safe to include explicit theme/wp events
        if not (pre.get("isAnchor") or pre.get("isBottleneck") or pre.get("themes")
                or sym in watchlist or sym in portfolio):
            continue
        decorated.append(_decorate_event(ev, pre, pre["score"], pre["breakdown"]))

    # -- Phase 6: Group by date and session, dedup, apply per-session cap
    # Dedup rule: symbol + date + session (keep highest-scored occurrence)
    days_map: dict[str, list[dict]] = {}
    cur = ws
    while cur <= we:
        days_map[cur.strftime("%Y-%m-%d")] = []
        cur += timedelta(days=1)

    seen_key: set[tuple[str, str, str]] = set()
    for ev in sorted(decorated, key=lambda e: -(e.get("importanceScore") or 0)):
        sym     = ev.get("symbol", "")
        d       = ev.get("date", "")
        sess    = ev.get("session", "unknown")
        dedup_k = (sym, d, sess)
        if dedup_k in seen_key:
            continue
        seen_key.add(dedup_k)
        if d in days_map:
            days_map[d].append(ev)

    days: list[dict] = []
    for date_str in sorted(days_map.keys()):
        day_evs = sorted(days_map[date_str], key=lambda e: -(e.get("importanceScore") or 0))

        pre_mkt   = [e for e in day_evs if e.get("session") == "preMarket"][:limit_per_session]
        after_hrs = [e for e in day_evs if e.get("session") == "afterHours"][:limit_per_session]
        during    = [e for e in day_evs if e.get("session") == "duringMarket"][:limit_per_session]
        unknown   = [e for e in day_evs if e.get("session") == "unknown"][:limit_per_session]
        entries   = pre_mkt + during + after_hrs + unknown

        d_obj = datetime.strptime(date_str, "%Y-%m-%d")
        label = d_obj.strftime("%a, %b ") + str(d_obj.day)

        days.append({
            "date":         date_str,
            "label":        label,
            "weekday":      _WEEKDAY_NAMES.get(d_obj.weekday(), ""),
            "count":        len(day_evs),
            "preMarket":    pre_mkt,
            "afterHours":   after_hrs,
            "duringMarket": during,
            "unknown":      unknown,
            "entries":      entries,
        })

    # topEvents: all decorated events (post-dedup), sorted by score, capped
    all_deduped = [ev for evs in days_map.values() for ev in evs]
    top_events  = sorted(all_deduped, key=lambda e: -(e.get("importanceScore") or 0))[:max_total]

    print(
        f"[earn_clean] week-clean {from_str}→{to_str} "
        f"raw={len(raw_rows)} candidates={len(candidates)} "
        f"enriched={len(enrich_pool)} eligible={len(decorated)} "
        f"dropped={dropped_ct} deduped={len(all_deduped)} calls={call_counter[0]}"
    )

    return {
        "asOf":      _today(),
        "source":    "fmp",
        "weekStart": from_str,
        "weekEnd":   to_str,
        "days":      days,
        "topEvents": top_events,
        "status":    "partial" if rate_limited else "ok",
        "errors":    errors + (["FMP rate limit hit — partial data"] if rate_limited else []),
    }
