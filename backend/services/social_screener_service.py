"""
Social Screener + Fundamental Screener — additive Social-page derivations.

Both screeners are derived deterministically from data ALREADY produced by the
existing Social/X/Grok pipeline (services.x_consensus_cache + social_x_service).
NO additional Grok/XAI calls are made.  The existing four Social top sections
(x_consensus, freshest_alpha, theme_leadership, sentiment_acceleration) and the
existing Grok prompt are not touched.

FMP is used only for enrichment — market cap, volume, price changes, and
fundamentals.  Per-symbol FMP failures degrade gracefully to nulls and a
data_quality flag rather than 500ing the page.

Caps & TTLs:
  * Social screener   : up to 100 rows (entire ticker universe seen by Social).
  * Fundamental screener : capped at 50 tickers (top by social_acceleration_score
                           then consensus_score), to keep the Social page snappy.
  * Profile / market cap cache : 24h
  * Quote / volume / price-change cache : 30 min
  * Fundamentals (ratios + statements) : 12h

If FMP enrichment is not available (no API key, all calls fail, etc.), the
screeners still emit rows from the social pipeline data — just with
enrichment_status / cache_status set to "unavailable".
"""

from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

try:
    from zoneinfo import ZoneInfo as _ZoneInfo
    _ET = _ZoneInfo("America/New_York")
except Exception:
    _ET = None  # type: ignore[assignment]

from data.cache import cache
from data.fmp_utils import fmp_hist_ttl, fmp_cache_key, fmp_lkg_key
from services.api_audit import (
    fmp_force_429, record_call, record_request,
    get_total_calls, get_cache_counts,
)


# ── Caps & TTLs ──────────────────────────────────────────────────────────────

_MAX_SOCIAL_ROWS:        int = 100
_MAX_FUND_ENRICH_ROWS:   int = 50

_TTL_PROFILE:            int = 24 * 3600     # 24 h
_TTL_QUOTE:              int = 30 * 60       # 30 min
# 7D/30D/YTD/1Y price-change TTL: weekday 60 min, weekend until Mon 09:30 ET
# → use fmp_hist_ttl() from data.fmp_utils (single source of truth)
_TTL_FUNDAMENTALS:       int = 7 * 24 * 3600 # 7 days — FMP fundamental data
_TTL_FUND_CACHE:         int = 7 * 24 * 3600 # 7 days — fs_payload compiled payload
_TTL_SOCIAL_CACHE:       int = 30 * 60       # 30 min — social_payload compiled rows

_FMP_BASE = "https://financialmodelingprep.com/stable"

# FMP cache keys — cross-service (shared, same as Tradier's flat key pattern):
#   hot cache : fmp:{endpoint}:{SYMBOL}       (via fmp_cache_key from data.fmp_utils)
#   last-known-good: fmp:lkg:{endpoint}:{SYMBOL}  (via fmp_lkg_key)


# Guard against concurrent background fundamental warmup tasks
_fund_bg_running: bool = False

# Guard against concurrent background screener warmup tasks fired from the
# x-dashboard route.  Set synchronously by the route before create_task() so
# that all concurrent cold-cache requests see True immediately (asyncio is
# single-threaded; setting a bool before the first await is race-free).
_screener_warmup_running: bool = False


async def _run_screeners_bg(
    snapshot: Optional[dict],
    x_consensus_rows: list,
    sentiment_accel_rows: list,
    freshest_alpha: dict,
    theme_leadership: dict,
    fmp_api_key: str,
) -> None:
    """Single-flight background wrapper for screener cache warmup.

    Called via asyncio.create_task() from the x-dashboard route when
    social_screener:social_payload or fs_payload is cold.  All exceptions
    are caught and logged — failures never silently die.

    The caller sets _screener_warmup_running = True synchronously before
    creating the task.  This function clears it in a finally block.
    """
    global _screener_warmup_running
    try:
        print("[SOCIAL_SCREENER] BG-SCREENER: starting screener warmup …")
        await build_screeners(
            snapshot=snapshot,
            x_consensus_rows=x_consensus_rows,
            sentiment_accel_rows=sentiment_accel_rows,
            freshest_alpha=freshest_alpha,
            theme_leadership=theme_leadership,
            fmp_api_key=fmp_api_key,
            allow_live_fmp=True,
            background_fund=True,
        )
        print("[SOCIAL_SCREENER] BG-SCREENER: warmup complete")
    except Exception as exc:
        print(f"[SOCIAL_SCREENER] BG-SCREENER: warmup failed: {exc}")
    finally:
        _screener_warmup_running = False


# ── Market-hours guard ────────────────────────────────────────────────────────

def _is_us_market_open() -> bool:
    """Return True only during core NYSE trading hours.

    Hours: Monday–Friday, 09:30–16:00 America/New_York (DST-aware).
    Basic weekday guard only; no holiday calendar (documented limitation).
    Uses zoneinfo (stdlib ≥ 3.9) for proper DST handling; falls back to a
    fixed UTC-5 offset when zoneinfo is unavailable.
    """
    try:
        if _ET is not None:
            now_et = datetime.now(_ET)
        else:
            from datetime import timedelta
            now_et = datetime.now(timezone(timedelta(hours=-5)))
    except Exception:
        return False  # safe default: treat as closed

    if now_et.weekday() >= 5:   # Saturday=5, Sunday=6
        return False
    open_hm  = (9, 30)
    close_hm = (16, 0)
    cur_hm   = (now_et.hour, now_et.minute)
    return open_hm <= cur_hm < close_hm


# ── Display formatters ───────────────────────────────────────────────────────

def _fmt_market_cap(v: Optional[float]) -> Optional[str]:
    if v is None:
        return None
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    if v >= 1e12:
        return f"${v / 1e12:.2f}T"
    if v >= 1e9:
        return f"${v / 1e9:.2f}B"
    if v >= 1e6:
        return f"${v / 1e6:.2f}M"
    if v >= 1e3:
        return f"${v / 1e3:.1f}K"
    return f"${v:,.0f}"


def _fmt_volume(v: Optional[float]) -> Optional[str]:
    if v is None:
        return None
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    if v >= 1e9:
        return f"{v / 1e9:.2f}B"
    if v >= 1e6:
        return f"{v / 1e6:.1f}M"
    if v >= 1e3:
        return f"{v / 1e3:.1f}K"
    return f"{v:,.0f}"


# ── Last-known-good helpers ──────────────────────────────────────────────────
# Keys are cross-service (fmp_cache_key / fmp_lkg_key from data.fmp_utils)
# so any service requesting the same endpoint+symbol shares one cache entry.

def _set_lkg(hot_key: str, value: Any) -> None:
    """Write LKG entry.  hot_key must be the fmp_cache_key() result."""
    lkg_key = hot_key.replace("fmp:", "fmp:lkg:", 1)
    cache.set(lkg_key, value, 7 * 24 * 3600)


def _get_lkg(hot_key: str) -> Any:
    lkg_key = hot_key.replace("fmp:", "fmp:lkg:", 1)
    return cache.get(lkg_key)


def _fmp_cache_lookup(endpoint: str, params: dict) -> Any:
    """Read hot-cache → LKG without any HTTP call. Returns None on total miss."""
    sym = params.get("symbol") or params.get("ticker") or ""
    key = fmp_cache_key(endpoint, sym)
    val = cache.get(key)
    if val is not None:
        return val
    return cache.get(fmp_lkg_key(endpoint, sym))


def _social_enrich_from_cache(ticker: str, fmp_api_key: str) -> dict:
    """Cache-only social enrichment (profile + quote + price_change).

    Checks hot cache then LKG for each data type.  Never opens an HTTP
    connection.  Returns a dict with at least empty sub-dicts so callers
    can read .get() without KeyError.
    """
    profile_raw = _fmp_cache_lookup("profile",
                                    {"symbol": ticker, "apikey": fmp_api_key})
    quote_raw   = _fmp_cache_lookup("quote",
                                    {"symbol": ticker, "apikey": fmp_api_key})
    pchg_raw    = _fmp_cache_lookup("stock-price-change",
                                    {"symbol": ticker, "apikey": fmp_api_key})

    # ── Parse profile ──────────────────────────────────────────────────────
    if isinstance(profile_raw, list) and profile_raw:
        item = profile_raw[0] if isinstance(profile_raw[0], dict) else {}
        profile = {
            "company_name": item.get("companyName") or "",
            "sector":       item.get("sector") or "",
            "industry":     item.get("industry") or "",
            "market_cap":   item.get("marketCap"),
        }
    else:
        profile = {}

    # ── Parse quote ────────────────────────────────────────────────────────
    if isinstance(quote_raw, list) and quote_raw:
        item = quote_raw[0] if isinstance(quote_raw[0], dict) else {}
        quote = {
            "price":      item.get("price"),
            "volume":     item.get("volume"),
            "market_cap": item.get("marketCap"),
            "change_1d":  item.get("changePercentage"),
            "quote_source": "fmp_cache",
        }
    else:
        # FMP quote not cached — Tradier is primary, so _fetch_quote was skipped
        # for this symbol.  Fall back to Tradier LKG which enrich_social writes
        # on every live run.  Gives price/volume to the fast cache-only path.
        _trad_lkg = cache.get(f"social:tradier_lkg:{ticker}")
        if _trad_lkg and _trad_lkg.get("price"):
            quote = {
                "price":      _trad_lkg.get("price"),
                "volume":     _trad_lkg.get("volume"),
                "market_cap": _trad_lkg.get("market_cap"),
                "change_1d":  _trad_lkg.get("change_1d"),
                "quote_source": _trad_lkg.get("quote_source", "tradier_lkg"),
                "quote_is_stale": True,
            }
        else:
            quote = {}

    # ── Parse price-change ─────────────────────────────────────────────────
    if isinstance(pchg_raw, list) and pchg_raw:
        item = pchg_raw[0] if isinstance(pchg_raw[0], dict) else {}
    elif isinstance(pchg_raw, dict):
        item = pchg_raw
    else:
        item = {}

    if item:
        def _f(*keys: str) -> Optional[float]:
            for k in keys:
                v = item.get(k)
                if v is not None:
                    try:
                        return round(float(v), 2)
                    except (TypeError, ValueError):
                        continue
            return None
        pchg = {
            "price_change_1d":  _f("1D", "1d", "oneDay"),
            "price_change_7d":  _f("5D", "5d", "1W", "fiveDays"),
            "price_change_30d": _f("1M", "1m", "oneMonth"),
            "price_change_ytd": _f("ytd", "YTD", "ytd1Y"),
            "price_change_1y":  _f("1Y", "1y", "oneYear"),
        }
    else:
        pchg = {}

    return {"profile": profile, "quote": quote, "price_change": pchg}


# ── FMP enrichment fetchers (per-symbol, with caching) ────────────────────────

async def _fmp_get(
    client: httpx.AsyncClient,
    endpoint: str,
    params: dict,
    ttl: int,
    feature: str = "enrichment",
) -> Any:
    """Cached GET wrapper.  Returns [] on any error; never raises.

    Cache key is cross-service: fmp:{endpoint}:{SYMBOL} — identical to the
    Tradier flat-key pattern so any page requesting the same endpoint+ticker
    reuses one cache entry without a second FMP call.
    """
    ticker: Optional[str] = params.get("symbol") or params.get("ticker")
    cache_key = fmp_cache_key(endpoint, ticker or "")

    cached = cache.get(cache_key)
    if cached is not None:
        record_call(
            provider="fmp", endpoint=endpoint,
            page="social", feature=feature,
            cache_status="hit", http_status=None,
            elapsed_ms=0.0, ticker=ticker,
        )
        return cached

    # ── FMP_FORCE_429 simulation ──────────────────────────────────────────────
    if fmp_force_429():
        record_call(
            provider="fmp", endpoint=endpoint,
            page="social", feature=feature,
            cache_status="miss", http_status=429,
            elapsed_ms=0.0, ticker=ticker,
            success=False, error="FMP_FORCE_429 simulation",
        )
        print(f"[SOCIAL_SCREENER] FMP_FORCE_429 — simulating 429 for {endpoint}")
        return []

    t0 = time.monotonic()
    try:
        resp = await client.get(f"{_FMP_BASE}/{endpoint}", params=params, timeout=10.0)
        ms = int((time.monotonic() - t0) * 1000)
        if resp.status_code == 200:
            data = resp.json()
            if data:
                cache.set(cache_key, data, ttl)
                _set_lkg(cache_key, data)
            record_call(
                provider="fmp", endpoint=endpoint,
                page="social", feature=feature,
                cache_status="miss", http_status=200,
                elapsed_ms=ms, ticker=ticker,
            )
            return data
        # 4xx/5xx — fall back to last known good
        lkg = _get_lkg(cache_key)
        record_call(
            provider="fmp", endpoint=endpoint,
            page="social", feature=feature,
            cache_status="miss", http_status=resp.status_code,
            elapsed_ms=ms, ticker=ticker,
            success=False, error=f"HTTP {resp.status_code} (lkg={'yes' if lkg else 'no'})",
        )
        return lkg if lkg is not None else []
    except Exception as exc:
        ms = int((time.monotonic() - t0) * 1000)
        print(f"[SOCIAL_SCREENER] FMP {endpoint} {params}: {exc}")
        lkg = _get_lkg(cache_key)
        record_call(
            provider="fmp", endpoint=endpoint,
            page="social", feature=feature,
            cache_status="miss", http_status=None,
            elapsed_ms=ms, ticker=ticker,
            success=False, error=str(exc)[:120],
        )
        return lkg if lkg is not None else []


async def _fetch_profile(client: httpx.AsyncClient, ticker: str, key: str) -> dict:
    # ── Cache-first: check screener_fundamentals_cache (DB) before FMP ──────
    # Written by Screener Hub warm_fundamentals job and enrich_fund write-through.
    # Avoids duplicate FMP profile calls for tickers already cached by other tabs.
    try:
        from services.fmp_cache_service import get_company_profile_cached as _gcp
        cached = _gcp(ticker)
        if cached and (cached.get("name") or cached.get("sector") or cached.get("industry")):
            return {
                "company_name": cached.get("name") or cached.get("company_name") or "",
                "sector":       cached.get("sector") or "",
                "industry":     cached.get("industry") or "",
                "market_cap":   cached.get("market_cap"),
            }
    except Exception:
        pass

    data = await _fmp_get(
        client, "profile",
        {"symbol": ticker, "apikey": key},
        _TTL_PROFILE, feature="social_profile",
    )
    if isinstance(data, list) and data:
        item = data[0] if isinstance(data[0], dict) else {}
        return {
            "company_name": item.get("companyName") or "",
            "sector":       item.get("sector") or "",
            "industry":     item.get("industry") or "",
            "market_cap":   item.get("marketCap"),
        }
    return {}


async def _fetch_quote(client: httpx.AsyncClient, ticker: str, key: str) -> dict:
    data = await _fmp_get(
        client, "quote",
        {"symbol": ticker, "apikey": key},
        _TTL_QUOTE, feature="social_quote",
    )
    if isinstance(data, list) and data:
        item = data[0] if isinstance(data[0], dict) else {}
        return {
            "price":      item.get("price"),
            "volume":     item.get("volume"),
            "market_cap": item.get("marketCap"),
            "change_1d":  item.get("changePercentage"),
        }
    return {}


async def _fetch_price_change(client: httpx.AsyncClient, ticker: str, key: str) -> dict:
    """FMP stock-price-change endpoint — returns 1D, 5D, 1M, 3M, 6M, YTD, 1Y, etc.

    TTL follows the global FMP 7D+ rule (fmp_hist_ttl from data.fmp_utils):
      Weekday  → 60 min flat
      Weekend  → cache until Monday 09:30 ET
    """
    data = await _fmp_get(
        client, "stock-price-change",
        {"symbol": ticker, "apikey": key},
        fmp_hist_ttl(), feature="social_price_change",
    )
    if isinstance(data, list) and data:
        item = data[0] if isinstance(data[0], dict) else {}
    elif isinstance(data, dict):
        item = data
    else:
        item = {}
    if not item:
        return {}

    def _f(*keys: str) -> Optional[float]:
        for k in keys:
            v = item.get(k)
            if v is not None:
                try:
                    return round(float(v), 2)
                except (TypeError, ValueError):
                    continue
        return None

    return {
        "price_change_1d":  _f("1D", "1d", "oneDay"),
        "price_change_7d":  _f("5D", "5d", "1W", "fiveDays"),
        "price_change_30d": _f("1M", "1m", "oneMonth"),
        "price_change_ytd": _f("ytd", "YTD", "ytd1Y"),
        "price_change_1y":  _f("1Y", "1y", "oneYear"),
    }


async def _fetch_ratios_ttm(client: httpx.AsyncClient, ticker: str, key: str) -> dict:
    data = await _fmp_get(
        client, "ratios-ttm",
        {"symbol": ticker, "apikey": key},
        _TTL_FUNDAMENTALS, feature="fundamental_ratios",
    )
    if isinstance(data, list) and data:
        item = data[0] if isinstance(data[0], dict) else {}
    else:
        item = {}
    return item or {}


async def _fetch_key_metrics_ttm(client: httpx.AsyncClient, ticker: str, key: str) -> dict:
    data = await _fmp_get(
        client, "key-metrics-ttm",
        {"symbol": ticker, "apikey": key},
        _TTL_FUNDAMENTALS, feature="fundamental_key_metrics",
    )
    if isinstance(data, list) and data:
        item = data[0] if isinstance(data[0], dict) else {}
    else:
        item = {}
    return item or {}


async def _fetch_income_annual(client: httpx.AsyncClient, ticker: str, key: str) -> dict:
    data = await _fmp_get(
        client, "income-statement",
        {"symbol": ticker, "limit": 2, "period": "annual", "apikey": key},
        _TTL_FUNDAMENTALS, feature="fundamental_income",
    )
    if isinstance(data, list) and data:
        return {"latest": data[0] if isinstance(data[0], dict) else {},
                "prior":  data[1] if len(data) > 1 and isinstance(data[1], dict) else {}}
    return {}


async def _fetch_balance_annual(client: httpx.AsyncClient, ticker: str, key: str) -> dict:
    data = await _fmp_get(
        client, "balance-sheet-statement",
        {"symbol": ticker, "limit": 1, "period": "annual", "apikey": key},
        _TTL_FUNDAMENTALS, feature="fundamental_balance",
    )
    if isinstance(data, list) and data:
        return data[0] if isinstance(data[0], dict) else {}
    return {}


async def _fetch_cashflow_annual(client: httpx.AsyncClient, ticker: str, key: str) -> dict:
    data = await _fmp_get(
        client, "cash-flow-statement",
        {"symbol": ticker, "limit": 1, "period": "annual", "apikey": key},
        _TTL_FUNDAMENTALS, feature="fundamental_cashflow",
    )
    if isinstance(data, list) and data:
        return data[0] if isinstance(data[0], dict) else {}
    return {}


# ── Social-screener helpers ──────────────────────────────────────────────────

def _normalize_ticker(t: Any) -> str:
    if not isinstance(t, str):
        return ""
    return t.upper().strip().lstrip("$")


def _consensus_score(unique_accts_7d: int, in_x_consensus: bool) -> int:
    """Base on unique account breadth across the last 7 days, normalised 0-100.

    Saturates at 8 unique accounts (rare; the X selection is ~40 accounts).
    Boosts by +20 (capped) if the ticker is also in the existing X Consensus
    output, since that's an explicit Grok-curated conviction signal.
    """
    base = min(unique_accts_7d, 8) / 8.0 * 80.0
    if in_x_consensus:
        base += 20.0
    return int(round(max(0.0, min(100.0, base))))


def _freshness_score(min_recency_days: Optional[int]) -> int:
    """Most recent mention recency in days → 0-100.

    0d → 100, 1d → 90, 3d → 75, 7d → 50, 14d → 25, 30d+ → 5.
    """
    if min_recency_days is None:
        return 0
    d = max(0, int(min_recency_days))
    if d == 0:
        return 100
    if d == 1:
        return 90
    if d <= 3:
        return 75
    if d <= 7:
        return 50
    if d <= 14:
        return 25
    if d <= 30:
        return 10
    return 5


def _social_acceleration_score(
    mentions_1d: int,
    mentions_7d: int,
    in_sentiment_accel: bool,
) -> int:
    """acceleration = mentions_1d / max(mentions_7d / 7, 1) ; clamped 0-100.

    A ratio of 1.0 ≈ "today is matching the average daily rate of the last week".
    A ratio of 3.0 ≈ "3× the weekly daily-rate" → strong intraday acceleration.

    We map ratio → 0..80 (so ratio of 4.0 → 80) and add a +20 boost when the
    ticker is already in Sentiment Acceleration output.
    """
    if mentions_1d <= 0:
        ratio = 0.0
    else:
        denom = max(mentions_7d / 7.0, 1.0)
        ratio = mentions_1d / denom

    base = min(ratio, 4.0) / 4.0 * 80.0
    if in_sentiment_accel:
        base += 20.0
    return int(round(max(0.0, min(100.0, base))))


def _theme_for(
    ticker: str,
    theme_leadership: dict,
    profile: dict,
) -> str:
    """Resolve theme using the existing payload first, FMP profile last."""
    # 1. Theme Leadership → first theme whose key_tickers contains ticker
    themes = theme_leadership.get("themes") or []
    for th in themes:
        if not isinstance(th, dict):
            continue
        kts = th.get("key_tickers") or []
        # key_tickers can be strings or {ticker, ...} dicts
        for kt in kts:
            sym = ""
            if isinstance(kt, str):
                sym = _normalize_ticker(kt)
            elif isinstance(kt, dict):
                sym = _normalize_ticker(kt.get("ticker") or kt.get("symbol") or "")
            if sym and sym == ticker:
                return th.get("theme") or "Unclassified"

    # 2. FMP profile sector/industry
    industry = (profile or {}).get("industry") or ""
    sector   = (profile or {}).get("sector") or ""
    if industry:
        return industry
    if sector:
        return sector
    return "Unclassified"


# ── Social-screener row builder ──────────────────────────────────────────────

def _aggregate_universe(
    snapshot: Optional[dict],
    x_consensus_rows: list[dict],
    sentiment_accel_rows: list[dict],
    freshest_alpha: dict,
    theme_leadership: Optional[dict] = None,
) -> dict[str, dict]:
    """Walk the existing Social pipeline data and produce one bucket per ticker.

    Bucket fields:
        symbol, accounts (set), mentions_1d, mentions_7d, mentions_total,
        min_recency_days, sample_mentions (list of {handle, ticker, sentiment,
        recency_days, thesis}), source_tags (set).

    Fallback order (each level only activates when the previous yields nothing):
      1. _mention_data  — richest signal; per-account mention list
      2. _backend_ranked — scored ticker list written by the Grok refresh loop
      3. built dashboard sections — x_consensus / freshest_alpha /
         sentiment_acceleration / theme_leadership tickers already on the page

    Additionally, after the waterfall, ALL tickers visible in the built sections
    are ensured to be present as at least minimal buckets, so the screener is
    always a superset of what the Social page is already displaying.
    """
    buckets: dict[str, dict] = {}

    # ── helpers ──────────────────────────────────────────────────────────────
    def _empty_bucket(sym: str) -> dict:
        return {
            "symbol":           sym,
            "accounts":         set(),
            "accounts_1d":      set(),
            "accounts_7d":      set(),
            "mentions_total":   0,
            "mentions_1d":      0,
            "mentions_7d":      0,
            "min_recency_days": 9999,
            "sample_mentions":  [],
            "source_tags":      set(),
            "bullish_count":    0,
            "bearish_count":    0,
        }

    # 1. Collect the full set of tickers from the built dashboard sections
    #    (used both for source-tagging and as the final fallback / fill-in).
    in_xc = {_normalize_ticker(r.get("ticker") or r.get("symbol")) for r in x_consensus_rows
             if isinstance(r, dict)}
    in_xc.discard("")

    in_sa = {_normalize_ticker(r.get("ticker") or r.get("symbol")) for r in sentiment_accel_rows
             if isinstance(r, dict)}
    in_sa.discard("")

    fa_trades = (freshest_alpha or {}).get("trades") or []
    in_fa = {_normalize_ticker(r.get("ticker") or r.get("symbol")) for r in fa_trades
             if isinstance(r, dict)}
    in_fa.discard("")

    in_tl: set[str] = set()
    for _th in (theme_leadership or {}).get("themes") or []:
        if not isinstance(_th, dict):
            continue
        for kt in _th.get("key_tickers") or []:
            if isinstance(kt, str):
                sym = _normalize_ticker(kt)
                if sym:
                    in_tl.add(sym)
            elif isinstance(kt, dict):
                sym = _normalize_ticker(kt.get("ticker") or kt.get("symbol") or "")
                if sym:
                    in_tl.add(sym)

    section_tickers: set[str] = in_xc | in_sa | in_fa | in_tl

    # 2. Walk _mention_data (richest signal)
    mention_data = (snapshot or {}).get("_mention_data") or []
    for acct in mention_data:
        if not isinstance(acct, dict):
            continue
        if acct.get("category") == "macro_big_picture":
            continue
        handle = acct.get("handle") or ""
        for m in (acct.get("mentions") or []):
            if not isinstance(m, dict):
                continue
            sym = _normalize_ticker(m.get("ticker"))
            if not sym or len(sym) > 12 or " " in sym:
                continue
            sentiment = (m.get("sentiment") or "neutral").lower()
            rd_raw = m.get("recency_days")
            try:
                rd = int(rd_raw) if rd_raw is not None else 30
            except (TypeError, ValueError):
                rd = 30
            rd = max(0, min(rd, 365))

            b = buckets.setdefault(sym, _empty_bucket(sym))
            b["mentions_total"] += 1
            b["accounts"].add(handle)
            if rd <= 1:
                b["mentions_1d"] += 1
                b["accounts_1d"].add(handle)
            if rd <= 7:
                b["mentions_7d"] += 1
                b["accounts_7d"].add(handle)
            if sentiment == "bullish":
                b["bullish_count"] += 1
            elif sentiment == "bearish":
                b["bearish_count"] += 1
            b["min_recency_days"] = min(b["min_recency_days"], rd)
            if len(b["sample_mentions"]) < 5:
                thesis = (m.get("thesis") or "").strip()
                if thesis or sentiment == "bullish":
                    b["sample_mentions"].append({
                        "handle":       handle,
                        "sentiment":    sentiment,
                        "recency_days": rd,
                        "thesis":       thesis[:240],
                        "catalysts":    [str(c) for c in (m.get("catalysts") or []) if c][:3],
                    })

    # 3. Fall back to _backend_ranked when _mention_data is sparse / absent.
    #    Also tries the prior snapshot when the current one is completely empty
    #    (belt-and-suspenders — LKG merge in x_consensus_cache normally prevents
    #    this, but protects against a cold start or on-disk corruption).
    def _buckets_from_backend_ranked(ranked_list: list) -> None:
        for bs in ranked_list:
            if not isinstance(bs, dict):
                continue
            sym = _normalize_ticker(bs.get("ticker"))
            if not sym:
                continue
            unique_n = int(bs.get("bullish_account_count") or 0)
            min_rec  = bs.get("recency_days_min")
            try:
                min_rec_int = int(min_rec) if min_rec is not None else 9999
            except (TypeError, ValueError):
                min_rec_int = 9999
            handles = [
                (a.get("handle") or "") for a in (bs.get("top_accounts") or [])
                if isinstance(a, dict)
            ]
            buckets[sym] = {
                "symbol":            sym,
                "accounts":          set(h for h in handles if h),
                "accounts_1d":       set(h for h in handles if h) if min_rec_int <= 1 else set(),
                "accounts_7d":       set(h for h in handles if h) if min_rec_int <= 7 else set(),
                "mentions_total":    unique_n,
                "mentions_1d":       unique_n if min_rec_int <= 1 else 0,
                "mentions_7d":       unique_n if min_rec_int <= 7 else 0,
                "min_recency_days":  min_rec_int,
                "sample_mentions":   [],
                "source_tags":       set(),
                "bullish_count":     unique_n,
                "bearish_count":     0,
            }

    if not buckets:
        _buckets_from_backend_ranked((snapshot or {}).get("_backend_ranked") or [])

    if not buckets:
        # Both _mention_data and current _backend_ranked are empty — try prior snapshot
        # as a last resort so the screener always has some data to display.
        try:
            from services.x_consensus_cache import _load_prior_cache as _lpc_fb
            _prior_snap_fb = _lpc_fb()
            if _prior_snap_fb:
                _prior_br = (_prior_snap_fb.get("_backend_ranked") or [])
                if _prior_br:
                    print(
                        f"[SOCIAL_SCREENER] aggregate_universe fallback: "
                        f"using prior snapshot _backend_ranked ({len(_prior_br)} tickers)"
                    )
                    _buckets_from_backend_ranked(_prior_br)
        except Exception as _fb_exc:
            print(f"[SOCIAL_SCREENER] prior-snapshot fallback error: {_fb_exc}")

    # 4. Guarantee every ticker already visible on the Social page appears in the
    #    screener.  Tickers already present from _mention_data / _backend_ranked
    #    keep their rich data; only genuinely absent tickers get a minimal entry.
    #    This also acts as the final fallback when both sources above are empty.
    for sym in section_tickers:
        if not sym or len(sym) > 12 or " " in sym:
            continue
        if sym not in buckets:
            buckets[sym] = _empty_bucket(sym)

    # 5. Apply source-membership tags now that all buckets exist
    for sym, b in buckets.items():
        if sym in in_xc:
            b["source_tags"].add("x_consensus")
        if sym in in_fa:
            b["source_tags"].add("freshest_alpha")
        if sym in in_sa:
            b["source_tags"].add("sentiment_acceleration")
        if sym in in_tl:
            b["source_tags"].add("theme_leadership")

    return buckets


def build_social_screener(
    snapshot: Optional[dict],
    x_consensus_rows: list[dict],
    sentiment_accel_rows: list[dict],
    freshest_alpha: dict,
    theme_leadership: dict,
    enrichment_by_ticker: dict[str, dict],
    enrichment_status: str,
    *,
    market_hours_open: bool = False,
) -> dict:
    """Produce the social_screener payload — additive, no Grok call."""
    buckets = _aggregate_universe(snapshot, x_consensus_rows,
                                  sentiment_accel_rows, freshest_alpha,
                                  theme_leadership)

    # Theme-leadership tickers also count as a source tag
    for th in (theme_leadership or {}).get("themes") or []:
        if not isinstance(th, dict):
            continue
        for kt in th.get("key_tickers") or []:
            sym = ""
            if isinstance(kt, str):
                sym = _normalize_ticker(kt)
            elif isinstance(kt, dict):
                sym = _normalize_ticker(kt.get("ticker") or kt.get("symbol") or "")
            if sym and sym in buckets:
                buckets[sym]["source_tags"].add("theme_leadership")

    in_xc_set = {_normalize_ticker(r.get("ticker") or r.get("symbol"))
                 for r in x_consensus_rows if isinstance(r, dict)}
    in_xc_set.discard("")
    in_sa_set = {_normalize_ticker(r.get("ticker") or r.get("symbol"))
                 for r in sentiment_accel_rows if isinstance(r, dict)}
    in_sa_set.discard("")

    rows: list[dict] = []
    snapshot_generated_at = (snapshot or {}).get("generated_at")

    for sym, b in buckets.items():
        unique_accts_7d = len(b["accounts_7d"])
        consensus = _consensus_score(unique_accts_7d, sym in in_xc_set)
        freshness = _freshness_score(
            b["min_recency_days"] if b["min_recency_days"] < 9999 else None,
        )
        accel = _social_acceleration_score(
            b["mentions_1d"], b["mentions_7d"], sym in in_sa_set,
        )

        enr = enrichment_by_ticker.get(sym, {}) or {}
        profile = enr.get("profile") or {}
        quote   = enr.get("quote") or {}
        pchg    = enr.get("price_change") or {}

        market_cap = profile.get("market_cap") or quote.get("market_cap")
        volume     = quote.get("volume")

        # Last mentioned at — derived from snapshot generated_at minus min_recency_days
        last_mentioned_at: Optional[str] = None
        try:
            if snapshot_generated_at and b["min_recency_days"] < 9999:
                base_dt = datetime.fromisoformat(snapshot_generated_at.replace("Z", "+00:00"))
                from datetime import timedelta
                last_mentioned_at = (
                    base_dt - timedelta(days=int(b["min_recency_days"]))
                ).astimezone(timezone.utc).isoformat()
        except Exception:
            last_mentioned_at = None

        rows.append({
            "symbol":            sym,
            "company_name":      profile.get("company_name") or "",
            "theme":             _theme_for(sym, theme_leadership, profile),
            "market_cap":        market_cap,
            "market_cap_display": _fmt_market_cap(market_cap),
            "volume":            volume,
            "volume_display":    _fmt_volume(volume),
            "price_change_1d":   pchg.get("price_change_1d")  if pchg.get("price_change_1d")  is not None else quote.get("change_1d"),
            "price_change_7d":   pchg.get("price_change_7d"),
            "price_change_30d":  pchg.get("price_change_30d"),
            "price_change_ytd":  pchg.get("price_change_ytd"),
            "price_change_1y":   pchg.get("price_change_1y"),
            "mentions_1d":       int(b["mentions_1d"]),
            "mentions_7d":       int(b["mentions_7d"]),
            "consensus_score":          consensus,
            "freshness_score":          freshness,
            "social_acceleration_score": accel,
            "accounts":          sorted(b["accounts"]),
            "sample_mentions":   b["sample_mentions"],
            "last_mentioned_at": last_mentioned_at,
            "source_tags":       sorted(b["source_tags"]),
        })

    rows.sort(
        key=lambda r: (
            -int(r["social_acceleration_score"]),
            -int(r["consensus_score"]),
            -int(r["mentions_1d"]),
        )
    )
    rows = rows[:_MAX_SOCIAL_ROWS]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source":       "existing_social_payload",
        "rows":         rows,
        "meta": {
            "xai_call_added":    False,
            "ticker_count":      len(rows),
            "enrichment_status": enrichment_status,
            "market_hours_open": market_hours_open,
        },
    }


# ── Fundamental-screener row builder ─────────────────────────────────────────

def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _build_fundamental_row(
    sym: str,
    profile: dict,
    ratios_ttm: dict,
    metrics_ttm: dict,
    income: dict,
    balance: dict,
    cashflow: dict,
) -> dict:
    """Compose one fundamental row from FMP enrichment. Missing fields → null."""
    income_latest = (income or {}).get("latest") or {}
    income_prior  = (income or {}).get("prior")  or {}

    revenue        = _safe_float(income_latest.get("revenue"))
    revenue_prior  = _safe_float(income_prior.get("revenue"))
    gross_profit   = _safe_float(income_latest.get("grossProfit"))
    net_income     = _safe_float(income_latest.get("netIncome"))
    eps_diluted    = _safe_float(income_latest.get("epsDiluted"))
    ebitda         = _safe_float(income_latest.get("ebitda"))

    free_cash_flow = (
        _safe_float((metrics_ttm or {}).get("freeCashFlowTTM"))
        or _safe_float((cashflow or {}).get("freeCashFlow"))
    )

    total_debt = (
        _safe_float((balance or {}).get("totalDebt"))
        or _safe_float((balance or {}).get("longTermDebt"))
    )
    total_equity = _safe_float((balance or {}).get("totalStockholdersEquity"))

    market_cap = _safe_float((profile or {}).get("market_cap"))

    # Derived fields with fallbacks
    revenue_growth: Optional[float] = None
    if revenue is not None and revenue_prior not in (None, 0):
        try:
            revenue_growth = round((revenue - revenue_prior) / abs(revenue_prior) * 100, 2)
        except ZeroDivisionError:
            revenue_growth = None

    gross_margin: Optional[float] = None
    if gross_profit is not None and revenue not in (None, 0):
        try:
            gross_margin = round(gross_profit / revenue * 100, 2)
        except ZeroDivisionError:
            gross_margin = None
    # Fallback: ratios-ttm exposes grossProfitMarginTTM (0-1 decimal) which is
    # always available even when the income-statement call was rate-limited.
    if gross_margin is None:
        _gpm = _safe_float((ratios_ttm or {}).get("grossProfitMarginTTM"))
        if _gpm is not None:
            gross_margin = round(_gpm * 100, 2)

    fcf_margin: Optional[float] = None
    if free_cash_flow is not None and revenue not in (None, 0):
        try:
            fcf_margin = round(free_cash_flow / revenue * 100, 2)
        except ZeroDivisionError:
            fcf_margin = None

    debt_to_equity = (
        _safe_float((ratios_ttm or {}).get("debtToEquityRatioTTM"))
        or _safe_float((ratios_ttm or {}).get("debtEquityRatioTTM"))
    )
    if debt_to_equity is None and total_debt is not None and total_equity is not None and total_equity > 0:
        try:
            debt_to_equity = round(total_debt / total_equity, 3)
        except ZeroDivisionError:
            debt_to_equity = None

    current_ratio = _safe_float((ratios_ttm or {}).get("currentRatioTTM"))
    ps_ratio      = _safe_float((ratios_ttm or {}).get("priceToSalesRatioTTM"))
    pe_ratio      = _safe_float((ratios_ttm or {}).get("priceToEarningsRatioTTM"))

    fiscal_period = "TTM" if (ratios_ttm or metrics_ttm) else (
        income_latest.get("calendarYear") or income_latest.get("date") or "latest"
    )

    fields = {
        "market_cap": market_cap,
        "revenue":    revenue,
        "revenue_growth": revenue_growth,
        "gross_profit":   gross_profit,
        "gross_margin":   gross_margin,
        "net_income":     net_income,
        "eps_diluted":    eps_diluted,
        "ebitda":         ebitda,
        "free_cash_flow": free_cash_flow,
        "fcf_margin":     fcf_margin,
        "total_debt":     total_debt,
        "debt_to_equity": debt_to_equity,
        "current_ratio":  current_ratio,
        "ps_ratio":       ps_ratio,
        "pe_ratio":       pe_ratio,
    }

    populated = sum(1 for v in fields.values() if v is not None)
    if populated == 0:
        data_quality = "missing"
    elif populated >= 10:
        data_quality = "complete"
    else:
        data_quality = "partial"

    return {
        "symbol":         sym,
        "company_name":   (profile or {}).get("company_name") or "",
        **fields,
        "fiscal_period":  str(fiscal_period),
        "data_quality":   data_quality,
    }


# ── Tradier batch quote helper (for social screener) ─────────────────────────

async def _tradier_batch_live(
    tickers: list[str],
    api_key: str,
    sandbox: bool = False,
) -> dict[str, dict]:
    """Batch-fetch Tradier quotes via the shared TradierProvider.

    api_key / sandbox are retained for call-site compatibility but are ignored —
    the shared provider's configured credentials and rate limiter are used instead.
    Result is written to per-symbol social LKG cache (TTL 72 h) for backward
    compatibility with _tradier_lkg_for_symbol fallback.
    Returns empty dict on any failure so the caller falls back to FMP.
    """
    if not tickers:
        return {}
    import time as _time_mod
    _now_ts = _time_mod.time()
    _LKG_TTL = 72 * 3600
    try:
        import main as _main  # type: ignore
        _ds = getattr(_main, "data_service", None)
        if _ds is None or not getattr(_ds, "tradier", None):
            return {}
        raw_quotes = await _ds.tradier.get_quotes(tickers[:200])
        out: dict[str, dict] = {}
        for q in (raw_quotes or []):
            sym = (q.get("symbol") or "").upper()
            if not sym:
                continue
            try:
                last = float(q["last"]) if q.get("last") not in (None, "", "-") else None
            except (TypeError, ValueError):
                last = None
            try:
                chg_pct = float(q["change_percentage"]) if q.get("change_percentage") not in (None, "", "-") else None
            except (TypeError, ValueError):
                chg_pct = None
            try:
                vol = int(float(q["volume"])) if q.get("volume") not in (None, "", "-") else None
            except (TypeError, ValueError):
                vol = None
            row = {
                "price":                 last,
                "volume":                vol,
                "change_1d":             chg_pct,
                "market_cap":            None,
                "bid":                   q.get("bid"),
                "ask":                   q.get("ask"),
                "quote_source":          "tradier",
                "quote_cached_at":       _now_ts,
                "quote_is_stale":        False,
                "quote_fallback_reason": None,
            }
            out[sym] = row
            cache.set(f"social:tradier_lkg:{sym}", row, _LKG_TTL)
        print(f"[SOCIAL_TRADIER] batch returned {len(out)} quotes for {len(tickers)} tickers via shared provider")
        return out
    except Exception as exc:
        print(f"[SOCIAL_TRADIER] provider error: {exc}")
        return {}


def _tradier_lkg_for_symbol(sym: str) -> dict:
    """Return LKG Tradier quote from cache (stale-flagged) or empty dict."""
    lkg = cache.get(f"social:tradier_lkg:{sym}")
    if lkg and lkg.get("price"):
        return {**lkg, "quote_is_stale": True, "quote_fallback_reason": "tradier_lkg"}
    return {}


# ── Top-level enrichment orchestration ───────────────────────────────────────

async def fetch_enrichment_for_symbols(
    symbols: list[str],
    fmp_api_key: Optional[str],
    *,
    fundamental_symbols: Optional[list[str]] = None,
    allow_live_fmp: bool = True,
) -> tuple[dict[str, dict], dict[str, dict], str, str, bool]:
    """Enrich a list of tickers from FMP.

    Returns:
        (social_enrichment, fundamental_enrichment, enrichment_status,
         fund_status, market_hours_open)

      social_enrichment      — per-symbol {profile, quote, price_change}
      fundamental_enrichment — per-symbol full FMP fundamentals snapshot
      enrichment_status      — ok | partial | unavailable
      fund_status            — fresh | partial | stale | unavailable
      market_hours_open      — True if called during core NYSE hours

    Market-hours behaviour
    ──────────────────────
    • During core NYSE hours (Mon-Fri 09:30-16:00 ET):
        Live Tradier + FMP quote/price-change refresh runs as normal.
    • Outside core hours:
        Tradier live call is skipped; FMP quote/price-change live calls are
        skipped.  Cache/LKG values are returned for those fields so the UI
        always shows the last known close price rather than going blank.
        Profile (24 h TTL) and fundamentals (12 h TTL) are unaffected.

    When allow_live_fmp=False (main dashboard route) the market-hours flag
    is computed but no live calls are made regardless.

    Never raises — failures degrade to nulls + downgraded status.
    """
    _fmp_before = get_total_calls("fmp")
    _hits_before, _misses_before = get_cache_counts()
    _t0 = time.monotonic()

    # Determine market status once for this enrichment run.
    _mkt_open: bool = _is_us_market_open()

    social_enrichment: dict[str, dict]      = {s: {} for s in symbols}
    fundamental_enrichment: dict[str, dict] = {}

    if not fmp_api_key:
        return social_enrichment, fundamental_enrichment, "unavailable", "unavailable", _mkt_open

    # ── Cache-only mode (main dashboard path) ─────────────────────────────
    if not allow_live_fmp:
        hits = 0
        for sym in symbols:
            enr = _social_enrich_from_cache(sym, fmp_api_key)
            social_enrichment[sym] = enr
            if enr.get("profile") or enr.get("quote") or enr.get("price_change"):
                hits += 1
        if hits == 0:
            enrich_status = "unavailable"
        elif hits == len(symbols):
            enrich_status = "ok"
        else:
            enrich_status = "partial"
        # fundamental_enrichment stays empty — lazy endpoint handles live fetch
        return social_enrichment, fundamental_enrichment, enrich_status, "unavailable", _mkt_open

    fund_targets = list(fundamental_symbols or [])[:_MAX_FUND_ENRICH_ROWS]

    successes_social = 0
    failures_social  = 0
    served_from_lkg  = False

    successes_fund = 0
    failures_fund  = 0

    # Limit concurrent FMP calls:
    #   Social: 57 symbols × 2 endpoints  = 114 potential simultaneous calls.
    #   Fund:   50 symbols × 6 endpoints  = 300 potential simultaneous calls.
    # Semaphores cap concurrent coroutines to avoid FMP 429s on cold start.
    _social_sem = asyncio.Semaphore(15)  # caps simultaneous profile+pchg fetches
    _fund_sem   = asyncio.Semaphore(12)  # caps simultaneous 6-call fund enrichments

    # ── Tradier batch quote (primary for price/vol/1D%) ───────────────────
    # Tradier returns last-trade prices 24/7 (pre/post-market + weekends),
    # so we always run the batch call regardless of market hours.  This
    # ensures volume/price are never blank on a cold-cache server restart.
    _tradier_key = os.getenv("TRADIER_API_KEY", "")
    _tradier_sandbox = os.getenv("TRADIER_SANDBOX", "false").lower() == "true"
    _tradier_quotes: dict[str, dict] = {}
    if _tradier_key and symbols:
        try:
            _tradier_quotes = await asyncio.wait_for(
                _tradier_batch_live(list(symbols), _tradier_key, _tradier_sandbox),
                timeout=8.0,
            )
        except Exception as _te:
            print(f"[SOCIAL_SCREENER] Tradier batch timeout/error: {_te}")
    # For any tickers Tradier missed, fall back to LKG cache
    for _sym in symbols:
        if _sym not in _tradier_quotes:
            _lkg = _tradier_lkg_for_symbol(_sym)
            if _lkg:
                _tradier_quotes[_sym] = _lkg

    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            # Light social enrichment: profile + quote + price_change per ticker
            # quote: Tradier primary (price/vol/1D%), FMP cached quote as fallback
            # Outside market hours: quote + price_change are served from
            # hot-cache / LKG only (no live FMP or Tradier calls for those fields).
            async def enrich_social(sym: str) -> None:
                nonlocal successes_social, failures_social, served_from_lkg
                async with _social_sem:
                    try:
                        tradier_q = _tradier_quotes.get(sym, {})

                        if not _mkt_open:
                            # ── Market closed ─────────────────────────────────────
                            # Tradier batch already ran above (returns last-trade
                            # prices 24/7), so tradier_q is populated with live data.
                            # FMP stock-price-change is pure EOD data — equally valid
                            # outside hours — so we fetch it live when cache is cold.
                            cached_enr = _social_enrich_from_cache(sym, fmp_api_key)
                            cached_pchg = cached_enr.get("price_change") or {}
                            cached_quote = cached_enr.get("quote") or {}

                            # Tradier live (batch ran above) wins for quote
                            if tradier_q and tradier_q.get("price"):
                                quote = {**tradier_q,
                                         "quote_fallback_reason": "market_closed"}
                            elif cached_quote:
                                quote = {**cached_quote, "quote_is_stale": True,
                                         "quote_fallback_reason": "market_closed_lkg"}
                            else:
                                quote = {}

                            # Profile: hot-cache first (24 h TTL), then live fallback
                            profile = cached_enr.get("profile") or {}
                            if not profile:
                                try:
                                    profile = await asyncio.wait_for(
                                        _fetch_profile(client, sym, fmp_api_key),
                                        timeout=5.0,
                                    )
                                    if not isinstance(profile, dict):
                                        profile = {}
                                except Exception:
                                    profile = {}

                            # price_change: use cache if warm; else fetch live
                            # (EOD data — valid any time, cached 30 min after fetch)
                            if cached_pchg:
                                pchg = cached_pchg
                            else:
                                try:
                                    pchg = await asyncio.wait_for(
                                        _fetch_price_change(client, sym, fmp_api_key),
                                        timeout=5.0,
                                    )
                                    if not isinstance(pchg, dict):
                                        pchg = {}
                                except Exception:
                                    pchg = {}
                            served_from_lkg = True

                        elif tradier_q and tradier_q.get("price"):
                            # ── Market open, Tradier covered this ticker ──────────
                            profile, pchg = await asyncio.gather(
                                _fetch_profile(client, sym, fmp_api_key),
                                _fetch_price_change(client, sym, fmp_api_key),
                                return_exceptions=True,
                            )
                            profile = profile if isinstance(profile, dict) else {}
                            pchg    = pchg    if isinstance(pchg, dict)    else {}
                            quote = tradier_q

                        else:
                            # ── Market open, Tradier missed — fall back to FMP ───
                            profile, quote, pchg = await asyncio.gather(
                                _fetch_profile(client, sym, fmp_api_key),
                                _fetch_quote(client, sym, fmp_api_key),
                                _fetch_price_change(client, sym, fmp_api_key),
                                return_exceptions=True,
                            )
                            profile = profile if isinstance(profile, dict) else {}
                            quote   = quote   if isinstance(quote, dict)   else {}
                            pchg    = pchg    if isinstance(pchg, dict)    else {}
                            if quote:
                                quote["quote_source"] = "fmp"
                                quote["quote_fallback_reason"] = "tradier_miss"

                        social_enrichment[sym] = {
                            "profile":      profile,
                            "quote":        quote,
                            "price_change": pchg,
                        }
                        has_data = bool(profile or quote or pchg)
                        if has_data:
                            successes_social += 1
                        else:
                            failures_social += 1
                    except Exception as exc:
                        failures_social += 1
                        print(f"[SOCIAL_SCREENER] enrich_social {sym}: {exc}")
                        social_enrichment[sym] = {}

            await asyncio.gather(*(enrich_social(s) for s in symbols),
                                 return_exceptions=True)

            # Heavy fundamental enrichment for the capped subset
            async def enrich_fund(sym: str) -> None:
                nonlocal successes_fund, failures_fund
                # Semaphore limits concurrent coroutines to avoid FMP rate limits:
                # 50 symbols × 6 calls = 300 potential simultaneous reqs → capped at 72.
                async with _fund_sem:
                    try:
                        profile, ratios, metrics, income, balance, cashflow = await asyncio.gather(
                            _fetch_profile(client, sym, fmp_api_key),
                            _fetch_ratios_ttm(client, sym, fmp_api_key),
                            _fetch_key_metrics_ttm(client, sym, fmp_api_key),
                            _fetch_income_annual(client, sym, fmp_api_key),
                            _fetch_balance_annual(client, sym, fmp_api_key),
                            _fetch_cashflow_annual(client, sym, fmp_api_key),
                            return_exceptions=True,
                        )
                        profile  = profile  if isinstance(profile, dict)  else {}
                        ratios   = ratios   if isinstance(ratios, dict)   else {}
                        metrics  = metrics  if isinstance(metrics, dict)  else {}
                        income   = income   if isinstance(income, dict)   else {}
                        balance  = balance  if isinstance(balance, dict)  else {}
                        cashflow = cashflow if isinstance(cashflow, dict) else {}

                        row = _build_fundamental_row(
                            sym, profile, ratios, metrics, income, balance, cashflow,
                        )
                        fundamental_enrichment[sym] = row
                        # ── Write-through to screener_fundamentals_cache ────────────
                        # Lets Screener Hub tabs reuse this FMP fetch without calling
                        # FMP again.  Fire-and-forget: never block enrich_fund.
                        if profile or ratios or metrics:
                            try:
                                from data.screener_hub_store import upsert_fundamentals as _upsert_f
                                _raw_profile = {
                                    "companyName": profile.get("company_name", "") if isinstance(profile, dict) else "",
                                    "sector":      profile.get("sector", "")       if isinstance(profile, dict) else "",
                                    "industry":    profile.get("industry", "")     if isinstance(profile, dict) else "",
                                    "marketCap":   profile.get("market_cap")       if isinstance(profile, dict) else None,
                                }
                                _upsert_f(
                                    sym,
                                    profile=_raw_profile,
                                    metrics=metrics if isinstance(metrics, dict) else {},
                                    ratios=ratios   if isinstance(ratios,  dict) else {},
                                    market_cap=profile.get("market_cap") if isinstance(profile, dict) else None,
                                    sector=profile.get("sector")         if isinstance(profile, dict) else None,
                                    industry=profile.get("industry")     if isinstance(profile, dict) else None,
                                    country=None,
                                    exchange=None,
                                    provider="fmp",
                                    ttl_days=7,
                                )
                            except Exception:
                                pass
                        quality = row.get("data_quality", "missing")
                        if quality != "missing":
                            successes_fund += 1
                        else:
                            failures_fund += 1
                        if quality == "missing":
                            print(
                                f"[FUND_DEBUG] enrich_fund sym={sym} quality=missing "
                                f"profile_ok={bool(profile)} ratios_ok={bool(ratios)} "
                                f"metrics_ok={bool(metrics)} income_ok={bool(income)} "
                                f"balance_ok={bool(balance)} cashflow_ok={bool(cashflow)}"
                            )
                        else:
                            print(
                                f"[FUND_DEBUG] enrich_fund sym={sym} quality={quality} "
                                f"mktcap={row.get('market_cap')} pe={row.get('pe_ratio')}"
                            )
                    except Exception as exc:
                        failures_fund += 1
                        print(f"[SOCIAL_SCREENER] enrich_fund {sym}: {exc}")
                        fundamental_enrichment[sym] = _build_fundamental_row(
                            sym, {}, {}, {}, {}, {}, {},
                        )

            await asyncio.gather(*(enrich_fund(s) for s in fund_targets),
                                 return_exceptions=True)

    except Exception as exc:
        print(f"[SOCIAL_SCREENER] FMP enrichment session failed: {exc}")
        return social_enrichment, fundamental_enrichment, "unavailable", "unavailable", _mkt_open

    # Status determination — enrichment_status: ok | partial | unavailable only.
    # "stale" is not used; minority-hit scenarios degrade to "partial" (rows still
    # exist and have partial enrichment) rather than implying they are unusable.
    if successes_social == 0 and len(symbols) > 0:
        enrichment_status = "unavailable"
    elif failures_social == 0:
        enrichment_status = "ok"
    else:
        enrichment_status = "partial"

    if not fund_targets:
        fund_status = "unavailable"
    elif failures_fund == 0:
        fund_status = "fresh"
    elif successes_fund == 0:
        fund_status = "unavailable"
    elif successes_fund >= failures_fund:
        fund_status = "partial"
    else:
        fund_status = "stale"

    _fmp_this = get_total_calls("fmp") - _fmp_before
    _hits_now, _misses_now = get_cache_counts()
    _ms = int((time.monotonic() - _t0) * 1000)
    record_request(
        route="/api/social/fundamental-screener (enrichment)",
        page="social",
        feature="fundamental_enrichment",
        provider_calls={"fmp": _fmp_this, "finnhub": 0, "finviz": 0,
                        "polygon": 0, "alpha_vantage": 0, "tradier": 0},
        cache_hits=_hits_now - _hits_before,
        cache_misses=_misses_now - _misses_before,
        elapsed_ms=_ms,
        http_status=200,
        extra={
            "symbols": len(symbols),
            "fund_targets": len(fundamental_symbols or []),
            "enrichment_status": enrichment_status,
            "fund_status": fund_status,
            "allow_live_fmp": allow_live_fmp,
        },
    )

    return social_enrichment, fundamental_enrichment, enrichment_status, fund_status, _mkt_open


def build_fundamental_screener(
    fundamental_enrichment: dict[str, dict],
    cache_status: str,
    *,
    market_hours_open: bool = False,
) -> dict:
    """Compose the final fundamental_screener payload from per-symbol rows."""
    rows = list(fundamental_enrichment.values())
    rows.sort(key=lambda r: (-(r.get("market_cap") or 0)))

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source":       "fmp_enrichment",
        "rows":         rows,
        "meta": {
            "ticker_count":      len(rows),
            "cache_status":      cache_status,
            "market_hours_open": market_hours_open,
        },
    }


# ── Background fundamental-cache warmer ──────────────────────────────────────

async def _bg_warm_fundamentals(
    all_symbols: list[str],
    fund_targets: list[str],
    fmp_api_key: str,
    snapshot: Optional[dict] = None,
    x_consensus_rows: Optional[list] = None,
    sentiment_accel_rows: Optional[list] = None,
    freshest_alpha: Optional[dict] = None,
    theme_leadership: Optional[dict] = None,
) -> None:
    """Background task: populate both screener caches without blocking.

    Warms social_screener:social_payload (30-min TTL) AND
    social_screener:fs_payload (7-day TTL) so neither deadlocks after restart.
    """
    global _fund_bg_running
    if _fund_bg_running:
        return
    _fund_bg_running = True
    try:
        print("[SOCIAL_SCREENER] BG-FUND: starting screener cache warmup (social + fund) …")
        social_enrichment, fund_enrichment, enrich_status, fund_status, mkt_open = (
            await fetch_enrichment_for_symbols(
                all_symbols, fmp_api_key,
                fundamental_symbols=fund_targets,
                allow_live_fmp=True,
            )
        )

        # Warm social screener cache (prevents the deadlock where social_payload
        # was never written because build_screeners was gated on both caches warm)
        if social_enrichment and snapshot is not None:
            try:
                ss = build_social_screener(
                    snapshot,
                    x_consensus_rows or [],
                    sentiment_accel_rows or [],
                    freshest_alpha or {},
                    theme_leadership or {},
                    social_enrichment,
                    enrich_status,
                    market_hours_open=mkt_open,
                )
                if ss.get("rows"):
                    cache.set("social_screener:social_payload", ss, _TTL_SOCIAL_CACHE)
                    print(
                        f"[SOCIAL_SCREENER] BG-FUND: social_payload warmed "
                        f"rows={len(ss.get('rows', []))} ttl={_TTL_SOCIAL_CACHE}s"
                    )
                else:
                    print("[SOCIAL_SCREENER] BG-FUND: social_screener returned 0 rows — social_payload not cached")
            except Exception as _ss_exc:
                print(f"[SOCIAL_SCREENER] BG-FUND: social screener build error: {_ss_exc}")

        if fund_enrichment:
            fs = build_fundamental_screener(
                fund_enrichment, fund_status, market_hours_open=mkt_open,
            )
            cache.set("social_screener:fs_payload", fs, _TTL_FUND_CACHE)
            print(
                f"[SOCIAL_SCREENER] BG-FUND: fs_payload warmed "
                f"rows={len(fs.get('rows', []))} ttl={_TTL_FUND_CACHE}s"
            )
        else:
            print("[SOCIAL_SCREENER] BG-FUND: got 0 fund enrichment rows — fs_payload not updated")
    except Exception as exc:
        print(f"[SOCIAL_SCREENER] BG-FUND: warmup failed: {exc}")
    finally:
        _fund_bg_running = False


# ── Public top-level builder ─────────────────────────────────────────────────

async def build_screeners(
    snapshot: Optional[dict],
    x_consensus_rows: list[dict],
    sentiment_accel_rows: list[dict],
    freshest_alpha: dict,
    theme_leadership: dict,
    fmp_api_key: Optional[str] = None,
    *,
    allow_live_fmp: bool = True,
    background_fund: bool = False,
) -> tuple[dict, dict]:
    """Build (social_screener, fundamental_screener) from existing Social data.

    allow_live_fmp
        True  — live Tradier + FMP social enrichment (always real-time market data).
        False — cache-only social enrichment (legacy; no HTTP calls).

    background_fund  (used by x-dashboard)
        True  — fundamental cache check first; if cold, fire a background task and
                return empty fund rows for this request.  Next request will serve
                from the populated 7-day cache.
        False — fundamental enrichment runs inline if the cache is expired
                (lazy endpoint behaviour; accepts slower response on first call).

    Fundamental data is cached for 7 days (_TTL_FUND_CACHE).  When the cache is
    valid neither path makes additional FMP calls for fundamentals.

    Never raises.  On total FMP failure both screeners are still returned with
    enrichment_status / cache_status = "unavailable" — the page never 500s.
    """
    try:
        # 1. Walk universe to know which symbols we need to enrich
        universe = _aggregate_universe(
            snapshot, x_consensus_rows, sentiment_accel_rows, freshest_alpha,
            theme_leadership,
        )
        # Pre-rank for fundamental cap: prefer known acceleration / consensus
        in_xc_set = {(_normalize_ticker(r.get("ticker") or r.get("symbol")))
                     for r in x_consensus_rows if isinstance(r, dict)}
        in_xc_set.discard("")
        in_sa_set = {(_normalize_ticker(r.get("ticker") or r.get("symbol")))
                     for r in sentiment_accel_rows if isinstance(r, dict)}
        in_sa_set.discard("")

        ranking_rows = []
        for sym, b in universe.items():
            accel = _social_acceleration_score(
                b["mentions_1d"], b["mentions_7d"], sym in in_sa_set,
            )
            consensus = _consensus_score(
                len(b["accounts_7d"]), sym in in_xc_set,
            )
            ranking_rows.append((sym, accel, consensus))
        ranking_rows.sort(key=lambda x: (-x[1], -x[2]))

        all_symbols = [r[0] for r in ranking_rows][:_MAX_SOCIAL_ROWS]
        fund_targets = [r[0] for r in ranking_rows[:_MAX_FUND_ENRICH_ROWS]]

        key = fmp_api_key or os.getenv("FMP_API_KEY", "")

        # ── Compiled social-screener cache check ─────────────────────────────
        # Social enrichment (83 symbols × Tradier batch + FMP profiles) is
        # expensive.  We cache the compiled social_screener payload for 30 min
        # (_TTL_SOCIAL_CACHE) to avoid re-running Tradier / FMP calls on every
        # x-dashboard request.  Cache is invalidated naturally on TTL expiry.
        _cached_ss = cache.get("social_screener:social_payload")
        _social_cache_valid = bool(
            _cached_ss and isinstance(_cached_ss, dict) and _cached_ss.get("rows")
        )

        # ── Fundamental cache check ───────────────────────────────────────────
        # Fundamentals are expensive (50 tickers × 6 FMP calls).  We cache the
        # compiled payload for 7 days (_TTL_FUND_CACHE) so most calls skip all
        # FMP fundamental requests entirely.
        _cached_fs = cache.get("social_screener:fs_payload")
        _fund_cache_valid = bool(_cached_fs and isinstance(_cached_fs, dict)
                                 and _cached_fs.get("rows"))

        # Fast-path: both compiled caches are warm — no FMP/Tradier calls needed.
        if _social_cache_valid and _fund_cache_valid:
            _mkt = _is_us_market_open()
            _ss_cached = {
                **_cached_ss,
                "meta": {**(_cached_ss.get("meta") or {}), "market_hours_open": _mkt},
            }
            _fs_cached = {
                **_cached_fs,
                "meta": {**(_cached_fs.get("meta") or {}), "market_hours_open": _mkt},
            }
            print(
                f"[SOCIAL_SCREENER] build_screeners: served BOTH screeners from cache "
                f"social_rows={len(_cached_ss.get('rows', []))} "
                f"fund_rows={len(_cached_fs.get('rows', []))}"
            )
            return _ss_cached, _fs_cached

        # Fund symbols to enrich inline:
        #   - skip if cache is still valid (< 7 days)
        #   - skip if background_fund=True (will fire bg task on cold start)
        _fund_symbols_live: list[str] = (
            [] if (_fund_cache_valid or background_fund) else fund_targets
        )

        social_enrichment, fund_enrichment, enrich_status, fund_status, mkt_open = (
            await fetch_enrichment_for_symbols(
                all_symbols, key,
                fundamental_symbols=_fund_symbols_live,
                allow_live_fmp=allow_live_fmp,
            )
        )

        social_screener = build_social_screener(
            snapshot, x_consensus_rows, sentiment_accel_rows,
            freshest_alpha, theme_leadership,
            social_enrichment, enrich_status,
            market_hours_open=mkt_open,
        )

        # Cache the compiled social screener for 30 min
        if social_screener.get("rows"):
            cache.set("social_screener:social_payload", social_screener, _TTL_SOCIAL_CACHE)
            print(
                f"[SOCIAL_SCREENER] build_screeners: cached social_screener "
                f"rows={len(social_screener.get('rows', []))} ttl={_TTL_SOCIAL_CACHE}s"
            )

        # ── Resolve fundamental screener ──────────────────────────────────────
        if fund_enrichment:
            # Fresh live enrichment (lazy endpoint, or x-dashboard on cache expiry)
            fundamental_screener = build_fundamental_screener(
                fund_enrichment, fund_status, market_hours_open=mkt_open,
            )
            cache.set("social_screener:fs_payload", fundamental_screener, _TTL_FUND_CACHE)
            print(
                f"[SOCIAL_SCREENER] build_screeners: cached fundamental_screener "
                f"rows={len(fundamental_screener.get('rows', []))} ttl={_TTL_FUND_CACHE}s"
            )

        elif _fund_cache_valid:
            # Serve from 7-day cache (normal fast path for x-dashboard)
            fundamental_screener = {
                **_cached_fs,
                "meta": {**(_cached_fs.get("meta") or {}), "market_hours_open": mkt_open},
            }
            print(
                f"[SOCIAL_SCREENER] build_screeners: served fundamental_screener "
                f"from 7-day cache rows={len(_cached_fs.get('rows', []))}"
            )

        else:
            # Cold start: cache is empty and no inline enrichment ran
            if background_fund and not _fund_bg_running:
                # Fire background task — next x-dashboard call will have data
                asyncio.create_task(
                    _bg_warm_fundamentals(all_symbols, fund_targets, key)
                )
                print("[SOCIAL_SCREENER] build_screeners: fired BG-FUND warmup task")
            fundamental_screener = build_fundamental_screener(
                {}, "unavailable", market_hours_open=mkt_open,
            )

        return social_screener, fundamental_screener

    except Exception as exc:
        print(f"[SOCIAL_SCREENER] build_screeners fatal: {exc}")
        # Worst-case: return empty screeners so the endpoint never 500s
        now = datetime.now(timezone.utc).isoformat()
        _mkt_fallback = _is_us_market_open()
        return (
            {
                "generated_at": now,
                "source":       "existing_social_payload",
                "rows":         [],
                "meta": {
                    "xai_call_added":    False,
                    "ticker_count":      0,
                    "enrichment_status": "unavailable",
                    "market_hours_open": _mkt_fallback,
                },
            },
            {
                "generated_at": now,
                "source":       "fmp_enrichment",
                "rows":         [],
                "meta": {
                    "ticker_count":      0,
                    "cache_status":      "unavailable",
                    "market_hours_open": _mkt_fallback,
                },
            },
        )
