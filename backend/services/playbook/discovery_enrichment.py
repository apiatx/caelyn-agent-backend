"""
Discovery Enrichment — provider enrichment for shortlisted discovery candidates.

Provider usage rules (from spec):
  Finnhub   — primary: company profile, news, international metadata
  Tradier   — primary market data for US-listed / ADR names (quotes, liquidity)
  FMP       — sparing: basic profile / market cap sanity (250/day cap)
  Perplexity — targeted validation ONLY on shortlisted finalists (not full universe)
  Gemini     — optional; only when Perplexity insufficient (not mandatory)
  Grok       — optional; crowding / X sentiment (not mandatory)

Non-negotiable:
  - No Brave or Tavily usage in this flow
  - No broad market crawls — enrichment runs only on shortlisted candidates
  - Cache aggressively (FMP 5min, Finnhub 10min, Tradier 1min quotes)
  - Graceful fallback on all provider errors
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta

from agent.model_policy import MODEL_SONAR
from typing import Any, Dict, List, Optional

from data.cache import cache, FINNHUB_TTL, FMP_TTL

# Enrichment-specific TTLs
_ENRICH_TTL = 600    # 10 min for profile enrichment
_QUOTE_TTL  = 60     # 1 min for quotes
_PERP_TTL   = 3600   # 1 hr for Perplexity validation (expensive)


# ── Finnhub enrichment ─────────────────────────────────────────────────────────

async def finnhub_profile(ticker: str, api_key: str) -> Dict[str, Any]:
    """
    Fetch Finnhub company profile for a single ticker.
    Returns {company_name, market_cap, description, country, exchange, ipo, sector}
    Caches for FINNHUB_TTL seconds.
    """
    if not api_key:
        return {}
    cache_key = f"discovery:finnhub:profile:{ticker.upper()}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        import finnhub
        client = finnhub.Client(api_key=api_key)
        data = client.company_profile2(symbol=ticker.upper())
        if not data:
            return {}
        result = {
            "company_name": data.get("name", ""),
            "market_cap":   data.get("marketCapitalization"),  # in millions
            "country":      data.get("country", "US"),
            "exchange":     data.get("exchange", ""),
            "ipo":          data.get("ipo"),
            "sector":       data.get("finnhubIndustry", ""),
            "description":  data.get("description", ""),
            "ticker":       data.get("ticker", ticker),
        }
        cache.set(cache_key, result, _ENRICH_TTL)
        return result
    except Exception as e:
        print(f"[DISCOVERY_ENRICH] Finnhub profile error for {ticker}: {e}")
        return {}


async def finnhub_news_recent(ticker: str, api_key: str, days: int = 7) -> List[Dict[str, Any]]:
    """
    Fetch recent Finnhub news for a ticker.
    Returns list of {headline, summary, datetime, source} dicts.
    """
    if not api_key:
        return []
    cache_key = f"discovery:finnhub:news:{ticker.upper()}:{days}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        import finnhub
        from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        to_date   = datetime.now().strftime("%Y-%m-%d")
        client = finnhub.Client(api_key=api_key)
        news = client.company_news(ticker.upper(), _from=from_date, to=to_date)
        result = []
        for item in (news or [])[:10]:
            result.append({
                "headline": item.get("headline", ""),
                "summary":  item.get("summary", ""),
                "datetime": item.get("datetime", 0),
                "source":   item.get("source", ""),
            })
        cache.set(cache_key, result, FINNHUB_TTL)
        return result
    except Exception as e:
        print(f"[DISCOVERY_ENRICH] Finnhub news error for {ticker}: {e}")
        return []


async def finnhub_peers(ticker: str, api_key: str) -> List[str]:
    """Return Finnhub peer tickers for a given stock."""
    if not api_key:
        return []
    cache_key = f"discovery:finnhub:peers:{ticker.upper()}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        import finnhub
        client = finnhub.Client(api_key=api_key)
        peers = client.company_peers(ticker.upper()) or []
        cache.set(cache_key, peers, _ENRICH_TTL)
        return peers[:8]
    except Exception as e:
        print(f"[DISCOVERY_ENRICH] Finnhub peers error for {ticker}: {e}")
        return []


async def enrich_batch_finnhub(
    tickers: List[str],
    finnhub_key: str,
) -> Dict[str, Dict[str, Any]]:
    """
    Enrich a batch of tickers with Finnhub profile data.
    Returns {ticker: profile_dict}.
    Rate-limited: sequential with small delay between calls.
    """
    results: Dict[str, Dict[str, Any]] = {}
    for ticker in tickers:
        profile = await finnhub_profile(ticker, finnhub_key)
        if profile:
            results[ticker] = profile
        await asyncio.sleep(0.15)   # ~6-7 req/sec — well within Finnhub free tier
    return results


# ── Tradier enrichment (US / ADR names only) ──────────────────────────────────

async def tradier_quote(ticker: str, api_key: str, sandbox: bool = False) -> Dict[str, Any]:
    """
    Fetch a Tradier quote for a US-listed ticker via the shared TradierProvider.
    Returns {price, change_pct, volume, week52_high, week52_low, bid, ask, description}.
    api_key / sandbox retained for call-site compatibility but ignored — the shared
    provider's configured credentials and rate limiter are used instead.
    """
    if not ticker:
        return {}
    cache_key = f"discovery:tradier:quote:{ticker.upper()}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        import main as _main  # type: ignore
        _ds = getattr(_main, "data_service", None)
        if _ds is None or not getattr(_ds, "tradier", None):
            return {}
        raw_quotes = await _ds.tradier.get_quotes([ticker])
        quote = next(
            (q for q in (raw_quotes or []) if (q.get("symbol") or "").upper() == ticker.upper()),
            {},
        )
        if not quote:
            return {}
        result = {
            "price":       quote.get("last"),
            "change_pct":  quote.get("change_percentage"),
            "volume":      quote.get("volume"),
            "week52_high": quote.get("week_52_high"),
            "week52_low":  quote.get("week_52_low"),
            "bid":         quote.get("bid"),
            "ask":         quote.get("ask"),
            "description": quote.get("description", ""),
        }
        cache.set(cache_key, result, _QUOTE_TTL)
        return result
    except Exception as e:
        print(f"[DISCOVERY_ENRICH] Tradier quote error for {ticker}: {e}")
        return {}


async def enrich_us_quotes_tradier(
    us_tickers: List[str],
    tradier_key: str,
    sandbox: bool = False,
) -> Dict[str, Dict[str, Any]]:
    """
    Batch Tradier quote enrichment for US-listed and ADR tickers via shared TradierProvider.
    tradier_key / sandbox retained for call-site compatibility but ignored.
    All tickers are fetched in a single batch call; results are also cached per-ticker
    under discovery:tradier:quote:{SYM} for individual lookup reuse.
    """
    if not us_tickers:
        return {}
    try:
        import main as _main  # type: ignore
        _ds = getattr(_main, "data_service", None)
        if _ds is not None and getattr(_ds, "tradier", None):
            raw_quotes = await _ds.tradier.get_quotes(us_tickers)
            results: Dict[str, Dict[str, Any]] = {}
            for q in (raw_quotes or []):
                sym = (q.get("symbol") or "").upper()
                if not sym:
                    continue
                result = {
                    "price":       q.get("last"),
                    "change_pct":  q.get("change_percentage"),
                    "volume":      q.get("volume"),
                    "week52_high": q.get("week_52_high"),
                    "week52_low":  q.get("week_52_low"),
                    "bid":         q.get("bid"),
                    "ask":         q.get("ask"),
                    "description": q.get("description", ""),
                }
                results[sym] = result
                cache.set(f"discovery:tradier:quote:{sym}", result, _QUOTE_TTL)
            return results
    except Exception as e:
        print(f"[DISCOVERY_ENRICH] Tradier batch error: {e}")
    # Fall back to per-ticker calls (uses cached results if available)
    results_fb: Dict[str, Dict[str, Any]] = {}
    quotes = await asyncio.gather(
        *[tradier_quote(t, tradier_key, sandbox) for t in us_tickers],
        return_exceptions=True,
    )
    for ticker, q in zip(us_tickers, quotes):
        if isinstance(q, dict) and q:
            results_fb[ticker] = q
    return results_fb


# ── FMP enrichment (sparing — 250/day cap) ────────────────────────────────────

async def fmp_market_cap(ticker: str, fmp_key: str) -> Optional[float]:
    """
    Fetch market cap in USD from FMP stable/profile endpoint.
    Use sparingly — 250 req/day cap.
    Returns market cap in USD, or None on failure.
    """
    if not fmp_key:
        return None
    cache_key = f"discovery:fmp:mktcap:{ticker.upper()}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        import httpx
        url = f"https://financialmodelingprep.com/stable/profile"
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params={"symbol": ticker.upper(), "apikey": fmp_key})
        if resp.status_code != 200:
            return None
        data = resp.json()
        if isinstance(data, list) and data:
            mkt_cap = data[0].get("mktCap") or data[0].get("marketCap")
            if mkt_cap:
                val = float(mkt_cap)
                cache.set(cache_key, val, FMP_TTL)
                return val
        return None
    except Exception as e:
        print(f"[DISCOVERY_ENRICH] FMP market cap error for {ticker}: {e}")
        return None


# ── Perplexity targeted validation ────────────────────────────────────────────

async def perplexity_validate_candidate(
    ticker: str,
    company_name: str,
    role: str,
    perp_key: str,
) -> Dict[str, Any]:
    """
    Run a targeted Perplexity validation query for a shortlisted discovery candidate.
    Only called for finalists (not full universe) — expensive.

    Returns {confirmed: bool, evidence: [str], customer_relationships: [str], notes: str}
    """
    if not perp_key:
        return {"confirmed": False, "evidence": [], "notes": "Perplexity not configured"}
    cache_key = f"discovery:perplexity:validate:{ticker.upper()}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    prompt = (
        f"You are a financial supply chain analyst. For {company_name} ({ticker}), "
        f"confirm or deny the following supply chain role: {role}. "
        f"Provide: 1) Confirmed? (yes/no), 2) Key customers/relationships, "
        f"3) Any constraints or risks in this role. Be concise (3-5 sentences max)."
    )
    try:
        import httpx
        headers = {
            "Authorization": f"Bearer {perp_key}",
            "Content-Type":  "application/json",
        }
        body = {
            "model":    MODEL_SONAR,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 300,
        }
        async with httpx.AsyncClient(timeout=25) as client:
            resp = await client.post(
                "https://api.perplexity.ai/chat/completions",
                json=body,
                headers=headers,
            )
        if resp.status_code != 200:
            return {"confirmed": False, "evidence": [], "notes": f"Perplexity HTTP {resp.status_code}"}
        content = resp.json()
        text = content.get("choices", [{}])[0].get("message", {}).get("content", "")
        confirmed = "yes" in text.lower()[:50]
        result = {
            "confirmed":  confirmed,
            "evidence":   [text[:300]] if text else [],
            "notes":      text[:200] if text else "",
        }
        cache.set(cache_key, result, _PERP_TTL)
        return result
    except Exception as e:
        print(f"[DISCOVERY_ENRICH] Perplexity validation error for {ticker}: {e}")
        return {"confirmed": False, "evidence": [], "notes": str(e)}


async def validate_shortlist_perplexity(
    candidates: List[Dict[str, Any]],
    perp_key: str,
    max_validate: int = 5,
) -> Dict[str, Dict[str, Any]]:
    """
    Run Perplexity validation on top N shortlisted candidates only.
    Returns {ticker: validation_result}.
    """
    results: Dict[str, Dict[str, Any]] = {}
    to_validate = candidates[:max_validate]
    for c in to_validate:
        ticker       = c.get("ticker", "")
        company_name = c.get("company_name", ticker)
        role         = c.get("role", "")
        if ticker:
            val = await perplexity_validate_candidate(ticker, company_name, role, perp_key)
            results[ticker] = val
        await asyncio.sleep(0.5)  # gentle rate-limiting
    return results
