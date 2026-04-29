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

from data.cache import cache
from services.api_audit import (
    fmp_force_429, record_call, record_request,
    get_total_calls, get_cache_counts,
)


# ── Caps & TTLs ──────────────────────────────────────────────────────────────

_MAX_SOCIAL_ROWS:        int = 100
_MAX_FUND_ENRICH_ROWS:   int = 50

_TTL_PROFILE:            int = 24 * 3600     # 24 h
_TTL_QUOTE:              int = 30 * 60       # 30 min
_TTL_PRICE_CHANGE:       int = 30 * 60       # 30 min
_TTL_FUNDAMENTALS:       int = 12 * 3600     # 12 h

_FMP_BASE = "https://financialmodelingprep.com/stable"

# Cache key prefixes (so we can also serve last-known-good after expiry)
_LKG_PREFIX = "social_screener:lkg:"


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

def _set_lkg(key: str, value: Any) -> None:
    """Set both fresh and LKG entries.  LKG is held for 7 days."""
    cache.set(_LKG_PREFIX + key, value, 7 * 24 * 3600)


def _get_lkg(key: str) -> Any:
    return cache.get(_LKG_PREFIX + key)


def _fmp_cache_lookup(endpoint: str, params: dict) -> Any:
    """Read hot-cache → LKG without any HTTP call. Returns None on total miss."""
    cache_key = f"social_screener:fmp:{endpoint}:{sorted(params.items())}"
    val = cache.get(cache_key)
    if val is not None:
        return val
    return _get_lkg(cache_key)


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
    """Cached GET wrapper.  Returns [] on any error; never raises."""
    ticker: Optional[str] = params.get("symbol") or params.get("ticker")
    cache_key = f"social_screener:fmp:{endpoint}:{sorted(params.items())}"

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
    """FMP stock-price-change endpoint — returns 1D, 5D, 1M, 3M, 6M, YTD, 1Y, etc."""
    data = await _fmp_get(
        client, "stock-price-change",
        {"symbol": ticker, "apikey": key},
        _TTL_PRICE_CHANGE, feature="social_price_change",
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

    # 3. Fall back to _backend_ranked when _mention_data is sparse / absent
    if not buckets:
        for bs in (snapshot or {}).get("_backend_ranked") or []:
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
    pe_ratio      = _safe_float((ratios_ttm or {}).get("priceEarningsRatioTTM"))

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
    """Batch-fetch Tradier quotes, return {SYMBOL: normalised-quote-dict}.

    Returns empty dict on any failure so the caller can fall back to FMP.
    Result is also written to per-symbol LKG cache (TTL 72 h).
    """
    if not api_key or not tickers:
        return {}
    _base = "https://sandbox.tradier.com/v1" if sandbox else "https://api.tradier.com/v1"
    _LKG_TTL = 72 * 3600
    import time as _time_mod
    _now_ts = _time_mod.time()
    try:
        syms_str = ",".join(s.upper() for s in tickers[:200])
        async with httpx.AsyncClient(timeout=8.0) as _c:
            resp = await _c.get(
                f"{_base}/markets/quotes",
                headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
                params={"symbols": syms_str, "greeks": "false"},
            )
        if resp.status_code != 200:
            print(f"[SOCIAL_TRADIER] batch quotes HTTP {resp.status_code}")
            return {}
        raw = resp.json()
        quotes_block = raw.get("quotes", {})
        quote_list = quotes_block.get("quote", []) if isinstance(quotes_block, dict) else []
        if isinstance(quote_list, dict):
            quote_list = [quote_list]
        out: dict[str, dict] = {}
        for q in quote_list:
            sym = (q.get("symbol") or "").upper()
            last = q.get("last")
            if not sym:
                continue
            try:
                last = float(last) if last not in (None, "", "-") else None
            except (TypeError, ValueError):
                last = None
            try:
                chg_pct = float(q.get("change_percentage")) if q.get("change_percentage") not in (None, "", "-") else None
            except (TypeError, ValueError):
                chg_pct = None
            try:
                vol = int(float(q.get("volume"))) if q.get("volume") not in (None, "", "-") else None
            except (TypeError, ValueError):
                vol = None
            row = {
                "price":       last,
                "volume":      vol,
                "change_1d":   chg_pct,
                "market_cap":  None,
                "bid":         q.get("bid"),
                "ask":         q.get("ask"),
                "quote_source":         "tradier",
                "quote_cached_at":      _now_ts,
                "quote_is_stale":       False,
                "quote_fallback_reason": None,
            }
            out[sym] = row
            cache.set(f"social:tradier_lkg:{sym}", row, _LKG_TTL)
        print(f"[SOCIAL_TRADIER] batch returned {len(out)} quotes for {len(tickers)} tickers")
        return out
    except Exception as exc:
        print(f"[SOCIAL_TRADIER] batch live error: {exc}")
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
) -> tuple[dict[str, dict], dict[str, dict], str, str]:
    """Enrich a list of tickers from FMP.

    Returns:
        (social_enrichment, fundamental_enrichment, enrichment_status, fund_status)

      social_enrichment      — per-symbol {profile, quote, price_change}
      fundamental_enrichment — per-symbol full FMP fundamentals snapshot
      enrichment_status      — ok | partial | unavailable
                               (refers to enrichment fields only — rows still
                                exist regardless of this value)
      fund_status            — fresh | partial | stale | unavailable

    When allow_live_fmp=False (used by the main dashboard route), only the
    hot-cache and last-known-good values are used.  No HTTP connections are
    opened, so the dashboard response is never delayed by FMP latency.

    Never raises — failures degrade to nulls + downgraded status.
    """
    _fmp_before = get_total_calls("fmp")
    _hits_before, _misses_before = get_cache_counts()
    _t0 = time.monotonic()

    social_enrichment: dict[str, dict]      = {s: {} for s in symbols}
    fundamental_enrichment: dict[str, dict] = {}

    if not fmp_api_key:
        return social_enrichment, fundamental_enrichment, "unavailable", "unavailable"

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
        return social_enrichment, fundamental_enrichment, enrich_status, "unavailable"

    fund_targets = list(fundamental_symbols or [])[:_MAX_FUND_ENRICH_ROWS]

    successes_social = 0
    failures_social  = 0
    served_from_lkg  = False

    successes_fund = 0
    failures_fund  = 0

    # ── Tradier batch quote (primary for price/vol/1D%) ───────────────────
    # Done outside the FMP httpx session so it gets a fresh connection pool
    # and doesn't block FMP enrichment if Tradier is slow.
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
    # For tickers Tradier missed, fall back to LKG Tradier from a previous call
    for _sym in symbols:
        if _sym not in _tradier_quotes:
            _lkg = _tradier_lkg_for_symbol(_sym)
            if _lkg:
                _tradier_quotes[_sym] = _lkg

    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            # Light social enrichment: profile + quote + price_change per ticker
            # quote: Tradier primary (price/vol/1D%), FMP cached quote as fallback
            async def enrich_social(sym: str) -> None:
                nonlocal successes_social, failures_social, served_from_lkg
                try:
                    tradier_q = _tradier_quotes.get(sym, {})
                    # Always fetch profile + price_change from FMP; only fall back to
                    # FMP quote if Tradier didn't return a usable price.
                    if tradier_q and tradier_q.get("price"):
                        profile, pchg = await asyncio.gather(
                            _fetch_profile(client, sym, fmp_api_key),
                            _fetch_price_change(client, sym, fmp_api_key),
                            return_exceptions=True,
                        )
                        profile = profile if isinstance(profile, dict) else {}
                        pchg    = pchg    if isinstance(pchg, dict)    else {}
                        quote = tradier_q
                    else:
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
                    if profile or quote or pchg:
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
                    if row["data_quality"] != "missing":
                        successes_fund += 1
                    else:
                        failures_fund += 1
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
        return social_enrichment, fundamental_enrichment, "unavailable", "unavailable"

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

    return social_enrichment, fundamental_enrichment, enrichment_status, fund_status


def build_fundamental_screener(
    fundamental_enrichment: dict[str, dict],
    cache_status: str,
) -> dict:
    """Compose the final fundamental_screener payload from per-symbol rows."""
    rows = list(fundamental_enrichment.values())
    rows.sort(key=lambda r: (-(r.get("market_cap") or 0)))

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source":       "fmp_enrichment",
        "rows":         rows,
        "meta": {
            "ticker_count": len(rows),
            "cache_status": cache_status,
        },
    }


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
) -> tuple[dict, dict]:
    """Build (social_screener, fundamental_screener) from existing Social data.

    When allow_live_fmp=False the FMP enrichment step reads from cache only —
    no HTTP connections are opened.  Missing cache entries produce null fields
    with enrichment_status="partial" or "unavailable", but the screener rows
    are always returned.

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
        social_enrichment, fund_enrichment, enrich_status, fund_status = (
            await fetch_enrichment_for_symbols(
                all_symbols, key,
                fundamental_symbols=fund_targets,
                allow_live_fmp=allow_live_fmp,
            )
        )

        social_screener = build_social_screener(
            snapshot, x_consensus_rows, sentiment_accel_rows,
            freshest_alpha, theme_leadership,
            social_enrichment, enrich_status,
        )
        fundamental_screener = build_fundamental_screener(fund_enrichment, fund_status)
        return social_screener, fundamental_screener

    except Exception as exc:
        print(f"[SOCIAL_SCREENER] build_screeners fatal: {exc}")
        # Worst-case: return empty screeners so the endpoint never 500s
        now = datetime.now(timezone.utc).isoformat()
        return (
            {
                "generated_at": now,
                "source":       "existing_social_payload",
                "rows":         [],
                "meta": {
                    "xai_call_added":    False,
                    "ticker_count":      0,
                    "enrichment_status": "unavailable",
                },
            },
            {
                "generated_at": now,
                "source":       "fmp_enrichment",
                "rows":         [],
                "meta": {
                    "ticker_count": 0,
                    "cache_status": "unavailable",
                },
            },
        )
