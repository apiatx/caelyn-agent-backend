"""
Watchlist quote cache — batched Tradier quotes for all watchlist tickers.

Design:
  - TTL: 10 minutes (module-level in-memory LKG dict)
  - Batch size: 50 symbols per Tradier request
  - Skips exchange-prefixed foreign tickers (ASX:, TSX:, TSXV:, OTC:, ...)
  - Falls back to last-known-good on any Tradier failure
  - Non-blocking: callers receive cached data immediately;
    background refresh is kicked off only when TTL has expired
  - Lock prevents duplicate concurrent refresh calls
"""
from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone

import httpx

_QUOTE_TTL = 600   # 10 minutes
_BATCH_SIZE = 50   # safe Tradier batch size (supports ~200, keep conservative)
_TIMEOUT = 12.0    # seconds per Tradier request

# Module-level LKG cache: SYMBOL (uppercase) → enriched quote dict
_quote_cache: dict[str, dict] = {}
_cache_ts: float = 0.0
_refresh_lock: asyncio.Lock | None = None

# Prefixes that identify non-US-listed tickers Tradier cannot quote
_FOREIGN_PREFIXES = (
    "ASX:", "TSX:", "TSXV:", "OTC:", "LSE:", "HK:", "SHE:", "SHA:", "NSE:", "BSE:",
    "KRX:", "STO:", "AIM:", "EPA:", "ETR:", "FRA:", "AMS:", "BIT:", "BME:", "JSE:",
    "TYO:", "TPE:", "SET:", "SGX:", "IDX:", "BVMF:", "BVC:", "SZSE:", "SSE:",
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


async def _fetch_batch(symbols: list[str], api_key: str) -> dict[str, dict]:
    """Fetch one batch of Tradier quotes; returns {SYMBOL: enriched_row}."""
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
            result[sym] = {
                "price":            _safe_float(q.get("last")),
                "change_pct_1d":    _safe_float(q.get("change_percentage")),
                "name":             q.get("description") or sym,
                "quote_source":     "tradier",
                "quote_updated_at": now_str,
            }
        return result

    except Exception as e:
        print(f"[WQ_CACHE] Tradier batch exception: {e}")
        return {}


async def _do_refresh(symbols: list[str]) -> None:
    """Inner refresh — calls Tradier in parallel batches and updates cache."""
    global _quote_cache, _cache_ts

    api_key = os.getenv("TRADIER_API_KEY", "")
    if not api_key:
        print("[WQ_CACHE] No TRADIER_API_KEY — skipping quote refresh")
        return

    eligible = [s for s in symbols if _is_tradier_eligible(s)]
    if not eligible:
        return

    batches = [eligible[i : i + _BATCH_SIZE] for i in range(0, len(eligible), _BATCH_SIZE)]
    batch_results = await asyncio.gather(
        *[_fetch_batch(b, api_key) for b in batches],
        return_exceptions=True,
    )

    merged: dict[str, dict] = {}
    for br in batch_results:
        if isinstance(br, dict):
            merged.update(br)

    if merged:
        _quote_cache = {**_quote_cache, **merged}
        _cache_ts = time.monotonic()
        print(
            f"[WQ_CACHE] Refreshed {len(merged)}/{len(eligible)} quotes "
            f"({len(batches)} Tradier batch(es)) for {len(symbols)} watchlist tickers"
        )
    else:
        print("[WQ_CACHE] All Tradier batches failed — retaining LKG cache")


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
    Return cached quotes dict {SYMBOL: {price, change_pct_1d, name, quote_source, quote_updated_at}}.

    Behaviour:
      - Empty cache (first call after server start): awaits a foreground fetch so
        the very first GET response already contains live Tradier data.
      - Stale cache (TTL expired, but LKG data exists): returns LKG immediately
        and fires a background refresh — no blocking.
      - Fresh cache: returns immediately, no network call.
    """
    age = time.monotonic() - _cache_ts
    is_empty = not _quote_cache
    is_stale = age > _QUOTE_TTL

    if is_empty:
        # First call ever — fetch synchronously so the response has live data
        await _locked_refresh(symbols)
    elif is_stale or force_refresh:
        # Have LKG data — refresh in background without blocking the caller
        lock = _get_lock()
        if not lock.locked():
            asyncio.create_task(_locked_refresh(symbols))

    return _quote_cache


async def refresh_watchlist_quotes_now(symbols: list[str]) -> dict[str, dict]:
    """
    Synchronously refresh and return quotes (awaits completion).
    Use for background tasks where latency is acceptable.
    """
    await _locked_refresh(symbols)
    return _quote_cache
