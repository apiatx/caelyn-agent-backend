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
    sem = asyncio.Semaphore(_ENRICH_CONCURRENCY)

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
