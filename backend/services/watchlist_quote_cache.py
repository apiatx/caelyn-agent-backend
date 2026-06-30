"""
Watchlist quote cache — shared with Home Watchlist Snapshot.

Design:
  - TTL: 10 minutes (module-level in-memory LKG dict) — unchanged.
  - Delegates the actual quote fetch to home_service._batch_quotes(), which
    already powers the Home Watchlist Snapshot. That helper goes through:
        1) data_service.tradier.get_quotes()  (single batch, request-cached)
        2) Tradier LKG cache  (72 h, per-symbol)
        3) FMP /stable/quote  (covers tickers Tradier misses)
    This eliminates duplicate Tradier batch calls between Home Snapshot and
    the Watchlist page and means the Watchlist table inherits the same FMP
    fallback for volume / average_volume.
  - Skips exchange-prefixed foreign tickers (ASX:, TSX:, OTC:, …) — these
    pass through Tradier's batch and silently return blank fields, which the
    frontend renders as "—".
  - Lock prevents duplicate concurrent refresh calls.
  - If main.data_service isn't available yet (very early startup), falls
    back to direct Tradier HTTP — same behaviour as the previous module.
"""
from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone

import httpx

_QUOTE_TTL = 600   # 10 minutes — DO NOT change
_BATCH_SIZE = 50   # legacy fallback batch size
_TIMEOUT = 12.0    # seconds per Tradier request (legacy fallback only)

# Module-level LKG cache: SYMBOL (uppercase) → enriched quote dict
_quote_cache: dict[str, dict] = {}
_cache_ts: float = 0.0
_refresh_lock: asyncio.Lock | None = None

# Prefixes that identify non-US-listed tickers Tradier cannot quote
_FOREIGN_PREFIXES = (
    "ASX:", "TSX:", "TSXV:", "OTC:", "LSE:", "HK:", "SHE:", "SHA:", "NSE:", "BSE:",
    "KRX:", "STO:", "AIM:", "EPA:", "ETR:", "FRA:", "AMS:", "BIT:", "BME:", "JSE:",
    "TYO:", "TPE:", "SET:", "SGX:", "IDX:", "BVMF:", "BVC:", "SZSE:", "SSE:",
    # Additional exchange prefixes observed in watchlists
    "LON:", "CSE:", "TPEX:", "WSE:", "XSAT:", "OSL:", "SWX:", "NZX:",
    "KLSE:", "BKK:", "IST:", "BCBA:", "MCX:", "MOEX:", "JNB:", "CPH:", "OMX:",
)


def _is_tradier_eligible(symbol: str) -> bool:
    upper = symbol.upper()
    return not any(upper.startswith(p) for p in _FOREIGN_PREFIXES)


def _get_lock() -> asyncio.Lock:
    global _refresh_lock
    if _refresh_lock is None:
        _refresh_lock = asyncio.Lock()
    return _refresh_lock


def _safe_float(v) -> float | None:
    try:
        if v in (None, "", "-"):
            return None
        return float(v)
    except Exception:
        return None


def _normalise(symbol: str, q: dict, now_str: str) -> dict:
    """Translate a home_service-style quote row into the watchlist enriched shape."""
    rel_vol = None
    vol = _safe_float(q.get("volume"))
    avg_vol = _safe_float(q.get("average_volume"))
    if vol is not None and avg_vol and avg_vol > 0:
        rel_vol = round(vol / avg_vol, 4)
    return {
        "price":            _safe_float(q.get("last") if q.get("last") is not None else q.get("price")),
        "change_pct_1d":    _safe_float(q.get("change_percentage") if q.get("change_percentage") is not None else q.get("change_pct_1d")),
        "volume":           vol,
        "average_volume":   avg_vol,
        "relative_volume":  rel_vol,
        "name":             q.get("description") or q.get("name") or symbol,
        "quote_source":     q.get("quote_source") or "tradier",
        "quote_updated_at": now_str,
    }


def _overlay_canonical_per_symbol(symbols: list[str]) -> None:
    """
    Overlay the module-level _quote_cache with any fresher data from the shared
    per-symbol Tradier cache (tradier:quote:sym:{SYM}, 60s TTL).

    This is called when get_watchlist_quotes() serves from the 10-minute module
    cache.  If Portfolio, the screener, or the Home dashboard has called Tradier
    more recently, those fresher quotes are surfaced here without waiting for the
    next full module-cache refresh.
    """
    try:
        from data.cache import cache as _c
    except Exception:
        return

    now_str = datetime.now(timezone.utc).isoformat() + "Z"
    updated = 0
    for sym in symbols:
        sym_upper = sym.upper()
        if sym_upper not in _quote_cache:
            continue
        raw = _c.get(f"tradier:quote:sym:{sym_upper}")
        if not raw:
            continue
        price = _safe_float(raw.get("last"))
        if price is None:
            continue
        change_pct = _safe_float(raw.get("change_percentage"))
        vol        = _safe_float(raw.get("volume"))
        avg_vol    = _safe_float(raw.get("average_volume"))
        rel_vol    = None
        if vol is not None and avg_vol and avg_vol > 0:
            rel_vol = round(vol / avg_vol, 4)
        existing = _quote_cache[sym_upper]
        _quote_cache[sym_upper] = {
            **existing,
            "price":           price,
            "change_pct_1d":   change_pct,
            "volume":          vol,
            "average_volume":  avg_vol,
            "relative_volume": rel_vol if rel_vol is not None else existing.get("relative_volume"),
            "quote_source":    raw.get("quote_source", "tradier") or "tradier",
            "quote_updated_at": now_str,
        }
        # Propagate fresh per-symbol data to shared canonical LKG so Home and
        # Portfolio can fall back to the same Tradier quote when rate-limited.
        _c.set(f"quote:lkg:{sym_upper}", {
            **raw,
            "quote_source":    raw.get("quote_source", "tradier") or "tradier",
            "quote_is_stale":  False,
            "quote_fallback_reason": None,
        }, 72 * 3600)
        updated += 1
    if updated:
        print(f"[WQ_CACHE] Overlaid {updated} canonical per-symbol quotes onto module cache")


async def _fetch_via_home_service(symbols: list[str]) -> dict[str, dict]:
    """
    Reuse home_service._batch_quotes (Tradier live → Tradier LKG → FMP fallback).
    Returns {SYMBOL: enriched_row} in the watchlist shape.

    Requires main.data_service to be initialised; raises RuntimeError otherwise
    so the caller can fall back to the legacy direct-Tradier path.
    """
    try:
        import main  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"main module not importable: {exc}")

    data_service = getattr(main, "data_service", None)
    if data_service is None:
        raise RuntimeError("main.data_service not yet initialised")

    from services.home_service import _batch_quotes  # local import — avoid cycle

    eligible = [s for s in symbols if _is_tradier_eligible(s)]
    if not eligible:
        return {}

    raw = await _batch_quotes(eligible, data_service)
    now_str = datetime.now(timezone.utc).isoformat() + "Z"
    return {sym.upper(): _normalise(sym, q, now_str) for sym, q in raw.items()}


# ── Legacy direct-Tradier path (used only if home_service isn't ready) ─────

async def _fetch_batch_direct(symbols: list[str], api_key: str) -> dict[str, dict]:
    symbols_str = ",".join(s.upper() for s in symbols)
    url = "https://api.tradier.com/v1/markets/quotes"
    headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}
    now_str = datetime.now(timezone.utc).isoformat() + "Z"

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.get(
                url,
                headers=headers,
                params={"symbols": symbols_str, "greeks": "false"},
            )
        if resp.status_code != 200:
            print(f"[WQ_CACHE] Tradier batch error {resp.status_code}: {resp.text[:200]}")
            return {}

        data = resp.json()
        quotes_obj = data.get("quotes", {})
        quote_list = quotes_obj.get("quote", []) if isinstance(quotes_obj, dict) else []
        if isinstance(quote_list, dict):
            quote_list = [quote_list]

        result: dict[str, dict] = {}
        for q in quote_list:
            sym = (q.get("symbol") or "").upper()
            if not sym:
                continue
            result[sym] = _normalise(sym, q, now_str)
        return result

    except Exception as e:
        print(f"[WQ_CACHE] Tradier batch exception: {e}")
        return {}


async def _fetch_direct(symbols: list[str]) -> dict[str, dict]:
    api_key = os.getenv("TRADIER_API_KEY", "")
    if not api_key:
        print("[WQ_CACHE] No TRADIER_API_KEY and no data_service — skipping refresh")
        return {}

    eligible = [s for s in symbols if _is_tradier_eligible(s)]
    if not eligible:
        return {}

    batches = [eligible[i : i + _BATCH_SIZE] for i in range(0, len(eligible), _BATCH_SIZE)]
    batch_results = await asyncio.gather(
        *[_fetch_batch_direct(b, api_key) for b in batches],
        return_exceptions=True,
    )

    merged: dict[str, dict] = {}
    for br in batch_results:
        if isinstance(br, dict):
            merged.update(br)
    return merged


# ── Refresh orchestration ──────────────────────────────────────────────────

async def _do_refresh(symbols: list[str]) -> None:
    """Refresh via shared home_service path; direct Tradier as cold-cache fallback.

    Primary path: home_service._batch_quotes → TradierProvider → LKG → FMP.
    This path may return empty when the Tradier rate limiter is saturated (e.g.
    during the post-restart THEME_RS warmup that consumes all 110 req/min slots)
    AND the in-memory per-symbol LKG is also empty (cold restart).

    Cold-cache fallback: when the primary path returns nothing AND _quote_cache is
    still empty, a single direct Tradier batch is fired using TRADIER_API_KEY
    (bypasses the shared rate limiter).  This fires at most once per cold restart —
    subsequent requests hit the 10-minute module cache.
    """
    global _quote_cache, _cache_ts

    merged: dict[str, dict] = {}
    fetch_source = "shared(home_service)"
    try:
        merged = await _fetch_via_home_service(symbols)
    except RuntimeError as warn:
        # data_service not ready — fall back to direct Tradier
        print(f"[WQ_CACHE] {warn}; using direct Tradier path")
        merged = await _fetch_direct(symbols)
        fetch_source = "direct(tradier)"
    except Exception as exc:
        print(f"[WQ_CACHE] shared-path error, falling back: {exc}")
        merged = await _fetch_direct(symbols)
        fetch_source = "direct(tradier)"

    if merged:
        _quote_cache = {**_quote_cache, **merged}
        _cache_ts = time.monotonic()
        print(
            f"[WQ_CACHE] Refreshed {len(merged)} quotes "
            f"via {fetch_source} for {len(symbols)} watchlist tickers"
        )
    else:
        print(f"[WQ_CACHE] No quotes returned ({fetch_source}) — retaining LKG cache")
        # Cold-cache guard: if the shared path returned nothing AND the module cache
        # is still empty (cold restart, rate limiter saturated, in-memory LKG empty),
        # fire one direct Tradier call so the Watchlist page isn't blank.
        # This bypass is intentional — it fires at most once per server lifetime
        # because subsequent requests find _quote_cache populated and skip refresh.
        if not _quote_cache and fetch_source != "direct(tradier)":
            print("[WQ_CACHE] Cold cache + empty shared result — direct Tradier fallback")
            direct = await _fetch_direct(symbols)
            if direct:
                _quote_cache = {**_quote_cache, **direct}
                _cache_ts = time.monotonic()
                print(f"[WQ_CACHE] Direct fallback: {len(direct)} symbols cached")


async def _locked_refresh(symbols: list[str]) -> None:
    """Acquire lock then refresh — prevents duplicate concurrent refreshes."""
    lock = _get_lock()
    async with lock:
        await _do_refresh(symbols)


async def get_watchlist_quotes(
    symbols: list[str],
    force_refresh: bool = False,
) -> dict[str, dict]:
    """
    Return cached quotes dict
      {SYMBOL: {price, change_pct_1d, volume, average_volume,
                relative_volume, name, quote_source, quote_updated_at}}.

    Behaviour:
      - Empty cache (first call after server start): awaits foreground fetch.
      - Stale cache (>10 min) or force_refresh=True: returns LKG immediately
        and kicks off a background refresh.
      - Fresh cache: returns immediately, no network call.

    The underlying batch call is shared with home_service so Home Watchlist
    Snapshot and the Watchlist page reuse the same quote payload (no duplicate
    Tradier calls inside the 10-minute TTL window).
    """
    age = time.monotonic() - _cache_ts
    is_empty = not _quote_cache
    is_stale = age > _QUOTE_TTL

    if is_empty:
        await _locked_refresh(symbols)
    elif is_stale or force_refresh:
        lock = _get_lock()
        if not lock.locked():
            asyncio.create_task(_locked_refresh(symbols))
        # Serve stale module cache overlaid with any per-symbol canonical data
        _overlay_canonical_per_symbol(symbols)
    else:
        print(f"[WQ_CACHE] Cache hit ({len(_quote_cache)} symbols, age={age:.0f}s)")
        # Even on a fresh module-cache hit, overlay with canonical per-symbol data
        # so data from Portfolio or Home (refreshed within the last 60s) propagates
        # to the Watchlist without waiting for the 10-minute module-cache refresh.
        _overlay_canonical_per_symbol(symbols)

    return _quote_cache


async def refresh_watchlist_quotes_now(symbols: list[str]) -> dict[str, dict]:
    """Synchronously refresh and return quotes (awaits completion)."""
    await _locked_refresh(symbols)
    return _quote_cache
