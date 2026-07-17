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
import json
import math
import os
import time
from datetime import datetime, timezone

import httpx

_QUOTE_TTL = 600   # 10 minutes — DO NOT change
_BATCH_SIZE = 50   # legacy fallback batch size
_TIMEOUT = 12.0    # seconds per Tradier request (legacy fallback only)

# Disk LKG — survives restarts, redeploys, and multi-day market closures.
# Written after every successful refresh; read once at cold-cache startup.
# 4-day max age covers 3-day holiday weekends.
_DISK_LKG_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "watchlist_quote_lkg.json")
_DISK_LKG_MAX_AGE = 96 * 3600   # 4 days

# Module-level LKG cache: SYMBOL (uppercase) → enriched quote dict
_quote_cache: dict[str, dict] = {}
_cache_ts: float = 0.0
_refresh_lock: asyncio.Lock | None = None

def is_tradier_quote_eligible(symbol: str) -> bool:
    """Return True if symbol should be sent to Tradier for a live quote.

    Rules (in order):
      1. Empty / blank → False
      2. Contains ":" → False  (exchange-prefixed: AIM:FTC, LSE:VOD, OTC:XYZ, …)
      3. Otherwise → True

    This is intentionally simple so any future exchange prefix with ":"
    is excluded automatically, without maintaining a prefix list.
    """
    if not symbol or not symbol.strip():
        return False
    return ":" not in symbol


# Internal alias (keeps call sites readable)
_is_tradier_eligible = is_tradier_quote_eligible


def is_fmp_symbol_eligible(symbol: str) -> bool:
    """Return True if symbol should be sent to FMP for fundamentals / quotes.

    Same rule as is_tradier_quote_eligible:
      1. Empty / blank → False
      2. Contains ":" → False  (AIM:FTC, LSE:VOD, TSX:XYZ, FRA:APR, …)
      3. Otherwise → True

    Keeping a dedicated name makes call sites self-documenting and allows
    the FMP and Tradier eligibility rules to diverge in the future without
    a broad search-and-replace.
    """
    if not symbol or not symbol.strip():
        return False
    return ":" not in symbol


def _get_lock() -> asyncio.Lock:
    global _refresh_lock
    if _refresh_lock is None:
        _refresh_lock = asyncio.Lock()
    return _refresh_lock


def _safe_float(v) -> float | None:
    try:
        if v in (None, "", "-"):
            return None
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else f
    except Exception:
        return None


def _safe_positive(v) -> float | None:
    """Return float(v) only when it is finite and strictly > 0; else None."""
    f = _safe_float(v)
    return f if (f is not None and f > 0) else None


def _merge_fields(existing: dict, incoming: dict, now_str: str) -> dict:
    """
    Canonical field-level merge: apply incoming quote onto existing without
    erasing valid older fields when the incoming response is sparse.

    Rules (executed in order — earlier rules win):
      price          : use incoming if finite & > 0, else retain existing
      change_pct_1d  : use incoming if finite, else retain existing
      volume         : use incoming if > 0, else retain existing positive value
                       (never overwrite a valid session volume with null/0)
      average_volume : same rule as volume
      relative_volume: recompute from effective vol + avg_vol; fall back to
                       incoming value; otherwise retain existing
      name           : use incoming if non-empty, else existing
      provenance fields (additive — never erase):
        quote_source, quote_updated_at, quote_is_stale, quote_fallback_reason
        price_is_stale, price_source
        volume_is_stale, volume_source, volume_updated_at
    """
    out = dict(existing)

    # ── price ──────────────────────────────────────────────────────────────
    in_price = _safe_positive(incoming.get("price"))
    if in_price is not None:
        out["price"]          = in_price
        out["price_is_stale"] = incoming.get("price_is_stale", False)
        out["price_source"]   = incoming.get("quote_source") or "tradier"
    else:
        if out.get("price") is not None:
            out["price_is_stale"] = True

    # ── change_pct_1d ──────────────────────────────────────────────────────
    in_chg = _safe_float(incoming.get("change_pct_1d"))
    if in_chg is not None:
        out["change_pct_1d"] = in_chg

    # ── volume — never overwrite positive with None/zero ───────────────────
    in_vol = _safe_positive(incoming.get("volume"))
    if in_vol is not None:
        out["volume"]           = in_vol
        out["volume_is_stale"]  = incoming.get("quote_is_stale", False)
        out["volume_source"]    = incoming.get("quote_source") or "tradier"
        out["volume_updated_at"]= now_str
    else:
        ex_vol = _safe_positive(out.get("volume"))
        if ex_vol is not None:
            out["volume_is_stale"] = True
            out.setdefault("volume_source", "lkg")
        else:
            out["volume"]          = None
            out["volume_is_stale"] = True

    # ── average_volume — preserve positive value ───────────────────────────
    in_avg = _safe_positive(incoming.get("average_volume"))
    if in_avg is not None:
        out["average_volume"] = in_avg
    # else: retain whatever is in out already

    # ── relative_volume — always recompute from effective values ───────────
    eff_vol = _safe_positive(out.get("volume"))
    eff_avg = _safe_positive(out.get("average_volume"))
    if eff_vol is not None and eff_avg is not None:
        out["relative_volume"] = round(eff_vol / eff_avg, 4)
    else:
        in_rv = _safe_float(incoming.get("relative_volume"))
        if in_rv is not None:
            out["relative_volume"] = in_rv
        # else retain existing

    # ── name ───────────────────────────────────────────────────────────────
    in_name = (incoming.get("name") or incoming.get("description") or "").strip()
    if in_name:
        out["name"] = in_name

    # ── provenance ─────────────────────────────────────────────────────────
    out["quote_source"]          = incoming.get("quote_source") or out.get("quote_source") or "tradier"
    out["quote_updated_at"]      = incoming.get("quote_updated_at") or now_str
    out["quote_is_stale"]        = incoming.get("quote_is_stale", False)
    out["quote_fallback_reason"] = incoming.get("quote_fallback_reason") or out.get("quote_fallback_reason")

    return out


# ── Disk LKG — persistent across restarts ─────────────────────────────────────

def _load_disk_lkg() -> dict[str, dict]:
    """
    Load the on-disk quote LKG.  Returns {} on any error or if the file is
    older than _DISK_LKG_MAX_AGE (4-day holiday-weekend tolerance).
    Synchronous — call only from cold-start path before any await.
    """
    try:
        path = os.path.realpath(_DISK_LKG_PATH)
        if not os.path.exists(path):
            return {}
        age = time.time() - os.path.getmtime(path)
        if age > _DISK_LKG_MAX_AGE:
            print(f"[WQ_CACHE] Disk LKG is {age/3600:.1f}h old — ignoring (max {_DISK_LKG_MAX_AGE/3600:.0f}h)")
            return {}
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        print(f"[WQ_CACHE] Loaded disk LKG: {len(data)} symbols (age={age/3600:.1f}h)")
        return data
    except Exception as exc:
        print(f"[WQ_CACHE] Disk LKG load failed (non-fatal): {exc}")
        return {}


def _save_disk_lkg(cache: dict[str, dict]) -> None:
    """
    Persist the current in-memory quote cache to disk.
    Only writes symbols that have a valid positive volume (confirmed session data).
    Merges with the existing disk file so symbols not in the current pass are kept.
    Synchronous — call only from background tasks (never from the hot path).
    """
    try:
        path = os.path.realpath(_DISK_LKG_PATH)
        # Load existing disk content to merge with
        existing: dict[str, dict] = {}
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                if not isinstance(existing, dict):
                    existing = {}
            except Exception:
                existing = {}

        to_write = dict(existing)
        written = 0
        for sym, q in cache.items():
            if _safe_positive(q.get("volume")) is not None:
                # Only persist symbols with a valid session volume
                to_write[sym] = {k: v for k, v in q.items() if v is not None}
                written += 1

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(to_write, f, default=str)
        print(f"[WQ_CACHE] Saved disk LKG: {written} symbols with volume (total={len(to_write)})")
    except Exception as exc:
        print(f"[WQ_CACHE] Disk LKG save failed (non-fatal): {exc}")


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
        # Build incoming in normalised shape for field-level merge
        incoming_norm = {
            "price":             price,
            "change_pct_1d":     change_pct,
            "volume":            _safe_float(raw.get("volume")),
            "average_volume":    _safe_float(raw.get("average_volume")),
            "name":              raw.get("description") or raw.get("name") or "",
            "quote_source":      raw.get("quote_source", "tradier") or "tradier",
            "quote_is_stale":    False,
            "quote_fallback_reason": None,
        }
        _quote_cache[sym_upper] = _merge_fields(_quote_cache[sym_upper], incoming_norm, now_str)
        # Propagate merged (volume-preserving) data to shared canonical LKG so
        # Home and Portfolio see the same volume-safe data when rate-limited.
        _c.set(f"quote:lkg:{sym_upper}", {
            **_quote_cache[sym_upper],
            "quote_is_stale":        False,
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
        # Field-level merge: incoming sparse quote never erases valid existing fields
        now_str = datetime.now(timezone.utc).isoformat() + "Z"
        for sym, new_q in merged.items():
            if sym in _quote_cache:
                _quote_cache[sym] = _merge_fields(_quote_cache[sym], new_q, now_str)
            else:
                _quote_cache[sym] = new_q
        _cache_ts = time.monotonic()
        print(
            f"[WQ_CACHE] Refreshed {len(merged)} quotes "
            f"via {fetch_source} for {len(symbols)} watchlist tickers"
        )
        # Persist merged data to disk so volume survives restarts/redeploys.
        # Runs in this background task so it never blocks the request path.
        _save_disk_lkg(_quote_cache)
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
                now_str = datetime.now(timezone.utc).isoformat() + "Z"
                for sym, new_q in direct.items():
                    if sym in _quote_cache:
                        _quote_cache[sym] = _merge_fields(_quote_cache[sym], new_q, now_str)
                    else:
                        _quote_cache[sym] = new_q
                _cache_ts = time.monotonic()
                print(f"[WQ_CACHE] Direct fallback: {len(direct)} symbols cached")
                _save_disk_lkg(_quote_cache)


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
                relative_volume, name, quote_source, quote_updated_at,
                volume_is_stale, volume_source, price_is_stale, …}}.

    Behaviour:
      - Empty cache after server start: hydrate from disk LKG first (fast,
        synchronous), then schedule a background refresh and return immediately.
        Only awaits a live Tradier call when no disk LKG exists at all.
      - Stale cache (>10 min) or force_refresh=True: returns LKG immediately
        and kicks off a background refresh.
      - Fresh cache: returns immediately, no network call.

    The underlying batch call is shared with home_service so Home Watchlist
    Snapshot and the Watchlist page reuse the same quote payload (no duplicate
    Tradier calls inside the 10-minute TTL window).
    """
    global _quote_cache, _cache_ts
    age = time.monotonic() - _cache_ts
    is_empty = not _quote_cache
    is_stale = age > _QUOTE_TTL

    if is_empty:
        # ── Cold start: try disk LKG first (fast) then schedule background refresh
        disk = _load_disk_lkg()
        if disk:
            _quote_cache = disk
            _cache_ts = time.monotonic() - (_QUOTE_TTL + 1)  # mark stale → triggers refresh
            print(f"[WQ_CACHE] Cold-start: hydrated {len(disk)} symbols from disk LKG")
            # Schedule live refresh in background — do not await
            lock = _get_lock()
            if not lock.locked():
                asyncio.create_task(_locked_refresh(symbols))
            _overlay_canonical_per_symbol(symbols)
        else:
            # No disk LKG — must await inline once (first-ever run or file expired)
            print("[WQ_CACHE] Cold-start: no disk LKG — awaiting live Tradier refresh")
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
