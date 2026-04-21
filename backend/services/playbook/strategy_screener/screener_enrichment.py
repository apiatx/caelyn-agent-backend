"""
Strategy Screener — market cap enrichment helper.

Fills missing market_cap_usd for screener candidates (especially ADR/foreign names)
using existing provider infrastructure. Completely isolated from /api/query.

Priority order:
  1. FMP stable/profile  — returns USD directly, works for US-listed ADRs
  2. Finnhub profile     — international coverage, needs currency conversion for non-USD
  3. None → mark as unknown (never fake a value)

Triggered:
  - During snapshot generation (after candidate list is built, before save)
  - On-demand via POST /api/strategy-screener/enrich-market-caps
    (backfill existing snapshot without full regeneration)

Rate-limit discipline:
  - FMP: 250 req/day cap — only called for candidates with missing market_cap_usd
  - Finnhub: called sparingly, only as fallback
  - Results are cached (FMP_TTL / FINNHUB_TTL) to avoid redundant calls
"""
from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, List, Optional, Tuple

# ── Currency conversion approximations ───────────────────────────────────────
# These are rough static rates — good enough for bucket classification.
# We deliberately do NOT use live FX rates to avoid extra API calls.
# Updated conservatively. Source: major FX mid-rates ~2024-2025 averages.

_CURRENCY_PER_USD: Dict[str, float] = {
    "JPY": 150.0,
    "KRW": 1350.0,
    "TWD": 32.0,
    "HKD": 7.8,
    "CNY": 7.1,
    "EUR": 0.92,    # 1 EUR ≈ $1.09 → 0.92 EUR per USD
    "GBP": 0.79,    # 1 GBP ≈ $1.27 → 0.79 GBP per USD
    "CHF": 0.88,
    "SGD": 1.35,
    "AUD": 1.55,
    "CAD": 1.36,
    "USD": 1.0,
}

# Country → currency for Finnhub international results
_COUNTRY_CURRENCY: Dict[str, str] = {
    "JP": "JPY",
    "KR": "KRW",
    "TW": "TWD",
    "HK": "HKD",
    "CN": "CNY",
    "DE": "EUR",
    "NL": "EUR",
    "FR": "EUR",
    "IT": "EUR",
    "ES": "EUR",
    "AT": "EUR",
    "BE": "EUR",
    "FI": "EUR",
    "IE": "EUR",
    "LU": "EUR",
    "PT": "EUR",
    "GB": "GBP",
    "CH": "CHF",
    "SG": "SGD",
    "AU": "AUD",
    "CA": "CAD",
    "US": "USD",
}

# Sanity bounds — reject any market cap outside these bounds
_MIN_MARKET_CAP_USD = 1_000_000         # $1M minimum (ignore sub-million results)
_MAX_MARKET_CAP_USD = 5_000_000_000_000  # $5T maximum (reject currency conversion errors)


def _convert_to_usd(value_local: float, currency: str) -> Optional[float]:
    """Convert a local-currency value (in millions of that currency) to USD."""
    rate = _CURRENCY_PER_USD.get(currency.upper(), 1.0)
    usd_millions = value_local / rate
    usd = usd_millions * 1_000_000
    if _MIN_MARKET_CAP_USD <= usd <= _MAX_MARKET_CAP_USD:
        return usd
    return None


async def _try_fmp(ticker: str, fmp_key: str) -> Optional[Tuple[float, str]]:
    """
    Try FMP stable/profile for market cap in USD.
    Returns (market_cap_usd, "fmp") or None.
    FMP returns market cap in absolute USD — no conversion needed.
    """
    if not fmp_key:
        return None
    try:
        from data.cache import cache, FMP_TTL
        cache_key = f"screener:enrich:fmp:{ticker.upper()}"
        cached = cache.get(cache_key)
        if cached is not None:
            return (cached, "fmp") if cached > 0 else None

        import httpx
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://financialmodelingprep.com/stable/profile",
                params={"symbol": ticker.upper(), "apikey": fmp_key},
            )
        if resp.status_code != 200:
            cache.set(cache_key, -1, FMP_TTL)
            return None
        data = resp.json()
        if not isinstance(data, list) or not data:
            cache.set(cache_key, -1, FMP_TTL)
            return None
        mc = data[0].get("mktCap") or data[0].get("marketCap")
        if not mc:
            cache.set(cache_key, -1, FMP_TTL)
            return None
        mc_usd = float(mc)
        if _MIN_MARKET_CAP_USD <= mc_usd <= _MAX_MARKET_CAP_USD:
            cache.set(cache_key, mc_usd, FMP_TTL)
            return (mc_usd, "fmp")
        cache.set(cache_key, -1, FMP_TTL)
        return None
    except Exception as e:
        print(f"[SCREENER_ENRICH] FMP market cap error for {ticker}: {e}")
        return None


async def _try_finnhub(ticker: str, finnhub_key: str) -> Optional[Tuple[float, str]]:
    """
    Try Finnhub company_profile2 for market cap.
    Finnhub returns marketCapitalization in millions — but for non-USD exchanges
    this is in local currency millions. We convert using static FX rates.
    Returns (market_cap_usd, "finnhub") or None.
    """
    if not finnhub_key:
        return None
    try:
        from data.cache import cache, FINNHUB_TTL
        cache_key = f"screener:enrich:finnhub:{ticker.upper()}"
        cached = cache.get(cache_key)
        if cached is not None:
            return (cached, "finnhub") if cached > 0 else None

        import finnhub as _finnhub
        client = _finnhub.Client(api_key=finnhub_key)
        data = client.company_profile2(symbol=ticker.upper())
        if not data or not isinstance(data, dict):
            cache.set(cache_key, -1, FINNHUB_TTL)
            return None

        mc_raw = data.get("marketCapitalization")  # in millions of local/USD
        if not mc_raw:
            cache.set(cache_key, -1, FINNHUB_TTL)
            return None

        country  = (data.get("country") or "US").upper()
        currency = _COUNTRY_CURRENCY.get(country, "USD")
        mc_usd   = _convert_to_usd(float(mc_raw), currency)
        if mc_usd is not None:
            cache.set(cache_key, mc_usd, FINNHUB_TTL)
            return (mc_usd, "finnhub")
        cache.set(cache_key, -1, FINNHUB_TTL)
        return None
    except Exception as e:
        print(f"[SCREENER_ENRICH] Finnhub market cap error for {ticker}: {e}")
        return None


async def enrich_market_cap(
    ticker: str,
    existing_usd: Optional[float],
    fmp_key: str,
    finnhub_key: str,
) -> Tuple[Optional[float], Optional[str]]:
    """
    Return (market_cap_usd, source) for the given ticker.

    - If an existing valid value is already present, return it as-is ("existing").
    - Otherwise: try FMP first, then Finnhub.
    - If both fail, return (None, None) — caller marks as unknown.
    """
    if existing_usd is not None and existing_usd >= _MIN_MARKET_CAP_USD:
        return (existing_usd, "existing")

    # FMP first (USD-native, works for US-listed ADRs)
    result = await _try_fmp(ticker, fmp_key)
    if result:
        return result

    # Finnhub fallback (international coverage, with currency conversion)
    result = await _try_finnhub(ticker, finnhub_key)
    if result:
        return result

    return (None, None)


async def enrich_candidates(
    candidates: List[Dict[str, Any]],
    fmp_key: str,
    finnhub_key: str,
) -> List[Dict[str, Any]]:
    """
    Enrich a list of screener candidate dicts in-place (returns enriched copy).
    Only enriches candidates where market_cap_usd is missing or invalid.
    Candidates that already have a valid market_cap_usd are not touched.

    Adds two fields if enriched:
      market_cap_source      — "existing" | "fmp" | "finnhub" | "unknown"
      market_cap_enriched    — True if we updated the value during this run

    Returns the enriched list.
    """
    # Only candidates missing a valid market cap need enrichment
    missing = [c for c in candidates if not (c.get("market_cap_usd") and c["market_cap_usd"] >= _MIN_MARKET_CAP_USD)]

    if not missing:
        return candidates

    print(f"[SCREENER_ENRICH] Enriching market cap for {len(missing)} candidate(s): "
          f"{[c['ticker'] for c in missing]}")

    # Run enrichments concurrently (rate-limited by provider caches)
    tasks = [
        enrich_market_cap(
            ticker=c["ticker"],
            existing_usd=c.get("market_cap_usd"),
            fmp_key=fmp_key,
            finnhub_key=finnhub_key,
        )
        for c in missing
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    enriched_by_ticker: Dict[str, Tuple[Optional[float], Optional[str]]] = {}
    for c, result in zip(missing, results):
        if isinstance(result, Exception):
            print(f"[SCREENER_ENRICH] Error enriching {c['ticker']}: {result}")
            enriched_by_ticker[c["ticker"]] = (None, None)
        else:
            enriched_by_ticker[c["ticker"]] = result  # type: ignore

    # Rebuild candidate list with enriched values
    enriched_candidates = []
    for c in candidates:
        ticker = c["ticker"]
        if ticker in enriched_by_ticker:
            mc_usd, source = enriched_by_ticker[ticker]
            updated = dict(c)
            updated["market_cap_usd"]    = mc_usd
            updated["market_cap_source"] = source or "unknown"
            updated["market_cap_enriched"] = mc_usd is not None
            if mc_usd:
                print(f"[SCREENER_ENRICH]   {ticker}: ${mc_usd/1e9:.1f}B (source={source})")
            else:
                print(f"[SCREENER_ENRICH]   {ticker}: unknown market cap (both providers failed)")
            enriched_candidates.append(updated)
        else:
            enriched_candidates.append(c)

    return enriched_candidates
