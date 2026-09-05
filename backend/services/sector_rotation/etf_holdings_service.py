"""
ETF holdings service — lazy-loaded, aggressively cached.

Caching contract (stale-while-revalidate):
  0–30 days  → fresh (returned immediately, no refresh)
  30–90 days → stale (returned immediately, background refresh triggered)
  >90 days   → force refresh before returning

Holdings change slowly (rebalances are quarterly/monthly).
A 30-day TTL is plenty — we only need updated holdings ~monthly.

Storage:
  Memory cache (data.cache)  — fast in-process lookups
  Disk cache (data/etf_holdings/) — survives restarts

Data sources (priority order):
  1. Finnhub  GET /etf/holdings?symbol=...
  2. FMP      GET /stable/etf/holdings?symbol=...   ← DISABLED: not in FMP Starter plan
  3. Stale disk cache  (if both APIs fail)
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx

from data.cache import cache

_FRESH_TTL        = 30 * 24 * 3600   # 30 days — consider fresh
_STALE_TTL        = 90 * 24 * 3600   # 90 days — max stale acceptable
_MEM_CACHE_TTL    = _FRESH_TTL        # mirror fresh TTL in memory cache
_RETRY_BACKOFF    = 6 * 3600          # 6 hours — min gap between failed refresh attempts
ETF_HOLDINGS_MAX_LIVE_REFRESH_CONCURRENCY = 4

_DISK_DIR = Path(__file__).parent.parent.parent / "data" / "etf_holdings"

# ── FMP plan guard ─────────────────────────────────────────────────────────────
# FMP Starter plan does NOT include /stable/etf/holdings or ETF Asset Exposure.
# This flag short-circuits _fetch_from_fmp() before any HTTP call is made.
# Set to False only if the plan is upgraded to include ETF & Fund Holdings.
_FMP_ETF_HOLDINGS_DISABLED: bool = True
_FMP_BLOCKED_ENDPOINTS: list[str] = [
    "https://financialmodelingprep.com/stable/etf/holdings",
]

# Diagnostic counters (in-process, reset on restart)
_fmp_blocked_count: int = 0
_fmp_blocked_last_ts: Optional[float] = None

# Tracks in-flight refresh tasks so we don't double-fetch the same symbol
_refreshing: set[str] = set()
_live_refresh_tasks: dict[str, asyncio.Task] = {}
_live_refresh_semaphore = asyncio.Semaphore(ETF_HOLDINGS_MAX_LIVE_REFRESH_CONCURRENCY)

# Tracks when a background refresh was last ATTEMPTED (even if it failed)
# so we don't hammer the API on every call when FMP is returning 429s
_last_attempt_at: dict[str, float] = {}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _disk_path(symbol: str) -> Path:
    return _DISK_DIR / f"{symbol.upper()}.json"


def _mem_key(symbol: str) -> str:
    return f"etf_holdings:{symbol.upper()}"


def _load_disk(symbol: str) -> Optional[dict]:
    p = _disk_path(symbol)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception as e:
        print(f"[ETF_HOLDINGS] Disk read error {symbol}: {e}")
        return None


def _save_disk(symbol: str, data: dict) -> None:
    try:
        _DISK_DIR.mkdir(parents=True, exist_ok=True)
        p = _disk_path(symbol)
        p.write_text(json.dumps(data, indent=2))
    except Exception as e:
        print(f"[ETF_HOLDINGS] Disk write error {symbol}: {e}")


def _age_seconds(data: dict) -> float:
    ts = data.get("_fetched_at", 0)
    return time.time() - ts


def _is_fresh(data: dict) -> bool:
    return _age_seconds(data) < _FRESH_TTL


def _is_usable(data: dict) -> bool:
    return _age_seconds(data) < _STALE_TTL


def _finnhub_key() -> str:
    return os.getenv("FINNHUB_API_KEY", "")


def _fmp_key() -> str:
    return os.getenv("FMP_API_KEY", "")


# ── Data fetchers ─────────────────────────────────────────────────────────────

async def _fetch_from_finnhub(symbol: str) -> Optional[dict]:
    key = _finnhub_key()
    if not key:
        return None
    url = "https://finnhub.io/api/v1/etf/holdings"
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, params={"symbol": symbol.upper(), "token": key})
        if resp.status_code == 403:
            print(f"[ETF_HOLDINGS][Finnhub] 403 for {symbol} — plan restriction")
            return None
        if resp.status_code != 200:
            print(f"[ETF_HOLDINGS][Finnhub] HTTP {resp.status_code} for {symbol}")
            return None
        data = resp.json()
        # Finnhub response: { "holdings": [{symbol, name, percent, share}], "symbol": ..., "date": ... }
        raw_holdings = data.get("holdings") or []
        if not raw_holdings:
            print(f"[ETF_HOLDINGS][Finnhub] empty holdings for {symbol}")
            return None

        holdings = []
        for h in raw_holdings:
            ticker = (h.get("symbol") or "").upper()
            name   = h.get("name") or h.get("description") or ""
            # Finnhub uses 'percent' (0-100 scale) for weight
            pct = h.get("percent")
            if pct is None:
                pct = h.get("share")
            if pct is not None:
                try:
                    pct = round(float(pct), 4)
                except (TypeError, ValueError):
                    pct = None
            if ticker:
                holdings.append({"ticker": ticker, "name": name, "weight": pct})

        holdings.sort(key=lambda h: h.get("weight") or 0, reverse=True)
        as_of = data.get("date") or datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

        return {
            "symbol":        symbol.upper(),
            "as_of":         as_of,
            "source":        "finnhub",
            "holding_count": len(holdings),
            "holdings":      holdings,
        }

    except Exception as e:
        print(f"[ETF_HOLDINGS][Finnhub] Error for {symbol}: {e}")
        return None


async def _fetch_from_fmp(symbol: str) -> Optional[dict]:
    """
    Fetch ETF holdings from FMP stable API.
    Endpoint: GET /stable/etf/holdings?symbol={symbol}
    Response fields: asset (ticker), name, weightPercentage, updatedAt

    NOTE: Disabled for FMP Starter plan — /stable/etf/holdings is not included.
    Returns None immediately without making any HTTP call when _FMP_ETF_HOLDINGS_DISABLED=True.
    """
    global _fmp_blocked_count, _fmp_blocked_last_ts
    if _FMP_ETF_HOLDINGS_DISABLED:
        _fmp_blocked_count += 1
        _fmp_blocked_last_ts = time.time()
        print(
            f"[ETF_HOLDINGS][FMP] BLOCKED {symbol} — /stable/etf/holdings not in FMP Starter plan "
            f"(blocked_total={_fmp_blocked_count})"
        )
        return None

    key = _fmp_key()
    if not key:
        return None
    url = "https://financialmodelingprep.com/stable/etf/holdings"
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, params={"symbol": symbol.upper(), "apikey": key})
        if resp.status_code != 200:
            print(f"[ETF_HOLDINGS][FMP] HTTP {resp.status_code} for {symbol}")
            return None
        raw = resp.json()
        if not raw or not isinstance(raw, list):
            print(f"[ETF_HOLDINGS][FMP] empty/non-list response for {symbol}: {str(raw)[:100]}")
            return None

        holdings = []
        as_of = None
        for h in raw:
            ticker = (h.get("asset") or "").upper().strip()
            name   = (h.get("name") or "").strip()
            weight = h.get("weightPercentage")
            if weight is not None:
                try:
                    weight = round(float(weight), 4)
                except (TypeError, ValueError):
                    weight = None
            if not as_of and h.get("updatedAt"):
                # updatedAt: "2026-04-20 09:04:20" → take the date part
                as_of = str(h["updatedAt"])[:10]
            if ticker:
                holdings.append({"ticker": ticker, "name": name, "weight": weight})

        if not holdings:
            return None

        holdings.sort(key=lambda h: h.get("weight") or 0, reverse=True)
        as_of = as_of or datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

        print(f"[ETF_HOLDINGS][FMP] {symbol}: {len(holdings)} holdings as of {as_of}")
        return {
            "symbol":        symbol.upper(),
            "as_of":         as_of,
            "source":        "fmp",
            "holding_count": len(holdings),
            "holdings":      holdings,
        }
    except Exception as e:
        print(f"[ETF_HOLDINGS][FMP] Error for {symbol}: {e}")
        return None


async def _fetch_holdings(symbol: str) -> Optional[dict]:
    """Try FMP (primary, confirmed working) → Finnhub (fallback, plan-dependent)."""
    result = await _fetch_from_fmp(symbol)
    if result:
        return result
    print(f"[ETF_HOLDINGS] FMP failed for {symbol}, trying Finnhub…")
    return await _fetch_from_finnhub(symbol)


async def _stamp_and_cache(symbol: str, data: dict) -> dict:
    """Add metadata, persist to disk and memory cache, return stamped dict."""
    now_iso = datetime.now(tz=timezone.utc).isoformat(timespec="seconds")
    data["updated_at"]  = now_iso
    data["_fetched_at"] = time.time()
    data["top_holdings"] = data["holdings"][:10]

    await asyncio.to_thread(_save_disk, symbol, data)
    cache.set(_mem_key(symbol), data, _MEM_CACHE_TTL)
    return data


async def _fetch_live_bounded(
    symbol: str,
    *,
    live_gate: Optional[asyncio.Semaphore] = None,
    live_observer=None,
) -> Optional[dict]:
    """Globally bound and coalesce live provider work by ETF symbol."""
    existing = _live_refresh_tasks.get(symbol)
    if existing is not None:
        return await asyncio.shield(existing)

    async def _run() -> Optional[dict]:
        async with _live_refresh_semaphore:
            if live_gate is None:
                return await _fetch_holdings(symbol)
            async with live_gate:
                if live_observer is not None:
                    live_observer(1)
                try:
                    return await _fetch_holdings(symbol)
                finally:
                    if live_observer is not None:
                        live_observer(-1)

    task = asyncio.create_task(_run())
    _live_refresh_tasks[symbol] = task
    task.add_done_callback(
        lambda finished, sym=symbol: (
            _live_refresh_tasks.pop(sym, None)
            if _live_refresh_tasks.get(sym) is finished
            else None
        )
    )
    return await asyncio.shield(task)


async def _background_refresh(
    symbol: str,
    *,
    live_gate: Optional[asyncio.Semaphore] = None,
    live_observer=None,
) -> None:
    """Silently refresh holdings in the background — called for stale data."""
    if symbol in _refreshing:
        return
    # Respect backoff — don't retry within 6 hours of a failed attempt
    last = _last_attempt_at.get(symbol, 0)
    if time.time() - last < _RETRY_BACKOFF:
        return
    _refreshing.add(symbol)
    _last_attempt_at[symbol] = time.time()
    try:
        fresh = await _fetch_live_bounded(
            symbol,
            live_gate=live_gate,
            live_observer=live_observer,
        )
        if fresh:
            await _stamp_and_cache(symbol, fresh)
            print(f"[ETF_HOLDINGS] Background refresh complete: {symbol}")
        else:
            print(f"[ETF_HOLDINGS] Background refresh failed (no data): {symbol}")
    except Exception as e:
        print(f"[ETF_HOLDINGS] Background refresh error for {symbol}: {e}")
    finally:
        _refreshing.discard(symbol)


# ── Public API ────────────────────────────────────────────────────────────────

async def get_etf_holdings(
    symbol: str,
    *,
    diagnostics: Optional[dict] = None,
    live_gate: Optional[asyncio.Semaphore] = None,
    live_observer=None,
) -> dict:
    """
    Return ETF holdings for `symbol`.

    Stale-while-revalidate:
      0–30 days  → serve from cache, no refresh
      30–90 days → serve stale from cache, trigger background refresh
      >90 days   → force refresh before returning (blocks until done)

    Failed fetches are rate-limited to one attempt per 6 hours (_RETRY_BACKOFF)
    so a persistent API error (402/403/429) never causes rapid-fire retries.
    """
    sym = symbol.upper()

    # 1. Memory cache (fastest)
    mem = cache.get(_mem_key(sym))
    if mem and _is_fresh(mem):
        if diagnostics is not None:
            diagnostics["cache_hits"] = diagnostics.get("cache_hits", 0) + 1
        return mem

    # 2. Disk cache
    disk = await asyncio.to_thread(_load_disk, sym)

    if disk and _is_fresh(disk):
        # Warm memory cache and return
        cache.set(_mem_key(sym), disk, _MEM_CACHE_TTL)
        if diagnostics is not None:
            diagnostics["cache_hits"] = diagnostics.get("cache_hits", 0) + 1
        return disk

    if disk and _is_usable(disk):
        # Stale — return immediately, refresh in background
        cache.set(_mem_key(sym), disk, _MEM_CACHE_TTL)
        asyncio.create_task(
            _background_refresh(
                sym,
                live_gate=live_gate,
                live_observer=live_observer,
            )
        )
        if diagnostics is not None:
            diagnostics["stale_served"] = diagnostics.get("stale_served", 0) + 1
            diagnostics["live_attempts"] = diagnostics.get("live_attempts", 0) + 1
        print(f"[ETF_HOLDINGS] Serving stale data for {sym}, background refresh queued")
        return disk

    # 3. No usable cache — fetch live, but respect the 6-hour backoff so a
    #    persistent 402/403/429 doesn't retry on every single call.
    existing_live = _live_refresh_tasks.get(sym)
    if existing_live is not None:
        if diagnostics is not None:
            diagnostics["live_attempts"] = diagnostics.get("live_attempts", 0) + 1
        fresh = await _fetch_live_bounded(
            sym,
            live_gate=live_gate,
            live_observer=live_observer,
        )
        if fresh:
            return await _stamp_and_cache(sym, fresh)

    last = _last_attempt_at.get(sym, 0)
    if time.time() - last < _RETRY_BACKOFF:
        # Within backoff window — return empty rather than hammering the API
        return {
            "symbol":        sym,
            "as_of":         None,
            "source":        "none",
            "holding_count": 0,
            "holdings":      [],
            "top_holdings":  [],
            "updated_at":    None,
            "error":         "Holdings unavailable — retry backoff active",
        }

    print(f"[ETF_HOLDINGS] Cache miss for {sym} — fetching live")
    _last_attempt_at[sym] = time.time()
    if diagnostics is not None:
        diagnostics["live_attempts"] = diagnostics.get("live_attempts", 0) + 1
    fresh = await _fetch_live_bounded(
        sym,
        live_gate=live_gate,
        live_observer=live_observer,
    )
    if fresh:
        return await _stamp_and_cache(sym, fresh)

    # 4. Last resort: if we have anything on disk, return it even if >90 days
    if disk:
        print(f"[ETF_HOLDINGS] API failed for {sym} — serving expired cache")
        return disk

    # Nothing at all — return empty
    return {
        "symbol":        sym,
        "as_of":         None,
        "source":        "none",
        "holding_count": 0,
        "holdings":      [],
        "top_holdings":  [],
        "updated_at":    None,
        "error":         "Holdings unavailable — API failed and no cache exists",
    }


# ── Public diagnostics ─────────────────────────────────────────────────────────

def get_etf_holdings_diagnostics() -> dict:
    """
    Return FMP plan-guard diagnostics. Exposed via /api/sector-rotation/etf-holdings/diagnostics.
    Zero external calls — reads only in-process counters.
    """
    last_ts = _fmp_blocked_last_ts
    return {
        "etf_holdings_disabled_by_plan": _FMP_ETF_HOLDINGS_DISABLED,
        "blocked_fmp_unsupported_endpoint_count": _fmp_blocked_count,
        "blocked_endpoint_names": _FMP_BLOCKED_ENDPOINTS if _FMP_ETF_HOLDINGS_DISABLED else [],
        "last_blocked_timestamp": (
            datetime.fromtimestamp(last_ts, tz=timezone.utc).isoformat(timespec="seconds")
            if last_ts else None
        ),
        "reason": (
            "FMP Starter plan does not include ETF & Fund Holdings (/stable/etf/holdings). "
            "Live source falls back to Finnhub only; disk cache used when Finnhub is also unavailable."
            if _FMP_ETF_HOLDINGS_DISABLED else "FMP ETF holdings enabled"
        ),
        "live_source_fallback_order": ["finnhub", "disk_cache"],
        "disk_cache_dir": str(_DISK_DIR),
        "cache_ttl_days": {"fresh": _FRESH_TTL // 86400, "stale": _STALE_TTL // 86400},
    }
