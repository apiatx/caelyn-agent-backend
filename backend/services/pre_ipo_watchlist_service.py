"""
Pre-IPO Watchlist service.

Aggregates pre-IPO intel for a small fixed set of high-profile private
companies (SpaceX, OpenAI, Anthropic, Databricks, Anduril, Stripe) using
three external data sources:

  - Perplexity (sonar)         → IPO rumors, valuation estimates, funding
                                  context, secondary-market signals,
                                  expected timing.  Citations preserved.
  - Polymarket (Gamma API)     → Prediction markets relevant to IPO
                                  timing or valuation thresholds.
  - Finnhub (general news)     → Best-effort news confirmations.  These
                                  are private companies, so symbol-level
                                  data is not expected to exist.

The full response is cached for ~8 hours.  Stale cache is served if any
of the upstream calls fail.  Per-company failures never break the
endpoint — the affected company simply returns nulls / empty arrays /
"Unknown".

This module is additive and shares no code paths with the existing
FMP-based IPO calendar (services.catalyst_calendar_service).
"""
from __future__ import annotations

import asyncio
import json
import re
import time
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

from config import FINNHUB_API_KEY, PERPLEXITY_API_KEY

try:
    from agent.model_policy import MODEL_SONAR
except Exception:  # pragma: no cover — defensive
    MODEL_SONAR = "sonar"

try:
    from langsmith import traceable
except ImportError:  # pragma: no cover
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop


# ── Tracked companies ────────────────────────────────────────────────────────

TRACKED_COMPANIES: list[dict[str, Any]] = [
    {
        "company": "SpaceX",
        "polymarket_keywords": ["spacex", "starlink"],
        "ipo_keywords":       ["spacex ipo", "starlink ipo"],
    },
    {
        "company": "OpenAI",
        "polymarket_keywords": ["openai"],
        "ipo_keywords":       ["openai ipo"],
    },
    {
        "company": "Anthropic",
        "polymarket_keywords": ["anthropic"],
        "ipo_keywords":       ["anthropic ipo"],
    },
    {
        "company": "Databricks",
        "polymarket_keywords": ["databricks"],
        "ipo_keywords":       ["databricks ipo"],
    },
    {
        "company": "Anduril",
        "polymarket_keywords": ["anduril"],
        "ipo_keywords":       ["anduril ipo"],
    },
    {
        "company": "Stripe",
        "polymarket_keywords": ["stripe"],
        "ipo_keywords":       ["stripe ipo"],
    },
]


# ── Cache ────────────────────────────────────────────────────────────────────

# 8 hours fresh window; stale data is still kept for fallback.
_FRESH_TTL_SECONDS = 8 * 60 * 60
_CACHE_KEY = "pre_ipo_watchlist:v1"

# In-process cache — separate from data.cache so we can keep a stale copy
# even after the fresh TTL expires.
_state: dict[str, Any] = {
    "value":      None,   # last successfully built payload
    "fresh_at":   0.0,    # timestamp the cached value was generated
    "lock":       asyncio.Lock(),
    "in_flight":  False,
}


# ── Polymarket ───────────────────────────────────────────────────────────────

_GAMMA_BASE = "https://gamma-api.polymarket.com"
_PM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; CaelynAI-PreIPO/1.0)",
    "Accept":     "application/json",
}


async def _fetch_polymarket_for_company(company: str, keywords: list[str]) -> dict[str, Any]:
    """
    Best-effort search of Polymarket Gamma API for prediction markets that
    mention the company.  Returns a structured summary; never raises.
    """
    empty = {
        "ipo_probability_12m": None,
        "valuation_markets":   [],
        "summary":             "No relevant Polymarket markets found.",
    }
    try:
        markets: list[dict] = []
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            # Gamma /events supports a free-text `q` search param.
            for kw in keywords:
                try:
                    resp = await client.get(
                        f"{_GAMMA_BASE}/events",
                        params={
                            "q":      kw,
                            "active": "true",
                            "closed": "false",
                            "limit":  "30",
                        },
                        headers=_PM_HEADERS,
                    )
                    if resp.status_code != 200:
                        continue
                    events = resp.json()
                    if not isinstance(events, list):
                        continue
                    for ev in events:
                        for m in (ev.get("markets") or []):
                            if isinstance(m, dict):
                                m.setdefault("_event_title", ev.get("title"))
                                m.setdefault("_event_slug",  ev.get("slug"))
                                markets.append(m)
                except Exception:
                    continue

        if not markets:
            return empty

        # Deduplicate by condition_id / question
        seen: set[str] = set()
        deduped: list[dict] = []
        cl = company.lower()
        for m in markets:
            qid = str(m.get("conditionId") or m.get("id") or m.get("question") or "")
            if not qid or qid in seen:
                continue
            question = str(m.get("question") or "")
            if cl not in question.lower() and not any(k in question.lower() for k in keywords):
                continue
            seen.add(qid)
            deduped.append(m)

        if not deduped:
            return empty

        ipo_prob: Optional[float] = None
        valuation_rows: list[dict[str, Any]] = []

        for m in deduped:
            question = str(m.get("question") or "")
            slug = str(m.get("slug") or m.get("_event_slug") or "")
            url = f"https://polymarket.com/event/{slug}" if slug else None

            prob = _coerce_yes_probability(m)

            ql = question.lower()
            is_ipo_market = ("ipo" in ql or "go public" in ql or "publicly listed" in ql) and cl in ql
            is_valuation_market = ("valuation" in ql or "valued" in ql or "worth" in ql) and cl in ql

            if is_ipo_market and prob is not None:
                # Take the highest-probability "IPO within 12 months" style
                # market we find as the canonical 12m signal.
                if "12" in ql or "2026" in ql or "by end of" in ql or "next year" in ql:
                    if ipo_prob is None or prob > ipo_prob:
                        ipo_prob = prob
                # Also surface as a valuation/IPO market entry
                valuation_rows.append({
                    "question":    question,
                    "probability": prob,
                    "url":         url,
                })
            elif is_valuation_market:
                valuation_rows.append({
                    "question":    question,
                    "probability": prob,
                    "url":         url,
                })

        if not valuation_rows and ipo_prob is None:
            return empty

        if ipo_prob is None:
            summary = "Polymarket markets found but no clean 12-month IPO probability."
        elif ipo_prob < 0.20:
            summary = "Market implies low near-term IPO probability."
        elif ipo_prob < 0.45:
            summary = "Market implies low/moderate near-term IPO probability."
        elif ipo_prob < 0.65:
            summary = "Market implies moderate near-term IPO probability."
        else:
            summary = "Market implies elevated near-term IPO probability."

        return {
            "ipo_probability_12m": ipo_prob,
            "valuation_markets":   valuation_rows[:8],
            "summary":             summary,
        }
    except Exception as e:
        print(f"[PRE_IPO] polymarket {company} error: {e}")
        return empty


def _coerce_yes_probability(market: dict) -> Optional[float]:
    """Extract the YES outcome probability from a Gamma market dict."""
    try:
        outcomes = market.get("outcomes")
        prices = market.get("outcomePrices")
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        if isinstance(prices, str):
            prices = json.loads(prices)
        if isinstance(outcomes, list) and isinstance(prices, list) and len(outcomes) == len(prices):
            for o, p in zip(outcomes, prices):
                if str(o).strip().lower() == "yes":
                    return float(p)
            # Fallback: first outcome
            return float(prices[0])
        # Some endpoints expose lastTradePrice directly
        ltp = market.get("lastTradePrice") or market.get("bestBid")
        if ltp is not None:
            return float(ltp)
    except Exception:
        return None
    return None


# ── Perplexity ───────────────────────────────────────────────────────────────

_PPLX_URL = "https://api.perplexity.ai/chat/completions"


async def _fetch_perplexity_for_company(company: str) -> dict[str, Any]:
    """
    Query Perplexity sonar for IPO/valuation/timing intel.  Returns a dict
    with structured fields plus citations.  Never raises — on failure
    returns an empty skeleton so the caller can still emit the company.
    """
    empty = {
        "ipo_status":          "Unknown",
        "estimated_valuation": "Unknown",
        "valuation_notes":     [],
        "catalysts":           [],
        "expected_window":     {"earliest": "Unknown", "likely": "Unknown"},
        "sources":             [],
    }

    if not PERPLEXITY_API_KEY:
        return empty

    system_prompt = (
        "You are a financial research assistant. "
        "Return only valid JSON, no markdown, no explanation, no code blocks. "
        "Use neutral, factual language. Never use hype words like "
        "'massive', 'huge', 'rocket', 'moon', 'explode'. "
        "If a value is unknown or sources are weak, use the literal "
        "string \"Unknown\" or empty arrays."
    )

    user_prompt = (
        f"Search the web for the latest IPO rumors, valuation estimates, "
        f"funding round context, secondary-market signals, investor "
        f"comments, and expected IPO timing for {company}. "
        f"Focus on these queries: "
        f"\"{company} IPO rumors valuation estimate latest\", "
        f"\"{company} funding round valuation IPO timing\", "
        f"\"{company} secondary market valuation public listing expected\". "
        f"Return only a JSON object with this exact schema:\n"
        "{\n"
        "  \"ipo_status\": string  // e.g. \"Not announced\", \"Filed S-1\", \"Rumored 2026\", \"Unknown\"\n"
        "  \"estimated_valuation\": string  // a range like \"$80B–$120B\" or \"Unknown\"\n"
        "  \"valuation_notes\": [string]  // up to 4 short factual bullet points\n"
        "  \"catalysts\": [string]        // up to 5 short catalysts\n"
        "  \"expected_window\": {\n"
        "    \"earliest\": string,         // e.g. \"Late 2026\" or \"Unknown\"\n"
        "    \"likely\":   string          // e.g. \"2027+\" or \"Unknown\"\n"
        "  },\n"
        "  \"sources\": [{\"title\": string, \"url\": string}]  // up to 6 citations\n"
        "}\n"
        "Return only the JSON object."
    )

    try:
        async with httpx.AsyncClient(timeout=45.0) as client:
            resp = await client.post(
                _PPLX_URL,
                headers={
                    "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
                    "Content-Type":  "application/json",
                },
                json={
                    "model":       MODEL_SONAR,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user",   "content": user_prompt},
                    ],
                    "temperature": 0.2,
                },
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        print(f"[PRE_IPO] perplexity {company} error: {e}")
        return empty

    citations = data.get("citations") or []
    if not isinstance(citations, list):
        citations = []

    choices = data.get("choices") or []
    if not choices:
        return empty
    raw = (choices[0].get("message") or {}).get("content") or ""

    content = raw.strip()
    if content.startswith("```"):
        content = re.sub(r"^```[a-z]*\n?", "", content)
        content = re.sub(r"\n?```$", "", content)
    content = content.strip()

    parsed: dict[str, Any] = {}
    try:
        parsed = json.loads(content)
    except Exception:
        # Best-effort: extract first {...} block
        m = re.search(r"\{[\s\S]*\}", content)
        if m:
            try:
                parsed = json.loads(m.group(0))
            except Exception:
                parsed = {}

    if not isinstance(parsed, dict) or not parsed:
        return empty

    sources_out: list[dict[str, str]] = []
    for s in (parsed.get("sources") or []):
        if not isinstance(s, dict):
            continue
        url = str(s.get("url") or "").strip()
        title = str(s.get("title") or url or "Source").strip()
        if not url:
            continue
        sources_out.append({"title": title, "url": url, "source": "Perplexity"})

    # Augment with Perplexity's own citations array if present
    for c in citations:
        try:
            url = str(c).strip() if isinstance(c, str) else str(c.get("url") or "").strip()
        except Exception:
            url = ""
        if url and not any(s["url"] == url for s in sources_out):
            sources_out.append({"title": url, "url": url, "source": "Perplexity"})

    expected = parsed.get("expected_window") or {}
    if not isinstance(expected, dict):
        expected = {}

    return {
        "ipo_status":          str(parsed.get("ipo_status") or "Unknown") or "Unknown",
        "estimated_valuation": str(parsed.get("estimated_valuation") or "Unknown") or "Unknown",
        "valuation_notes":     [str(x) for x in (parsed.get("valuation_notes") or []) if x][:6],
        "catalysts":           [str(x) for x in (parsed.get("catalysts") or []) if x][:6],
        "expected_window": {
            "earliest": str(expected.get("earliest") or "Unknown") or "Unknown",
            "likely":   str(expected.get("likely")   or "Unknown") or "Unknown",
        },
        "sources":             sources_out[:8],
    }


# ── Finnhub ──────────────────────────────────────────────────────────────────

_FINNHUB_NEWS_URL = "https://finnhub.io/api/v1/news"


async def _fetch_finnhub_news_for_company(company: str) -> list[dict[str, Any]]:
    """
    Best-effort: pull recent general/business news from Finnhub and filter
    by company name.  These targets are private companies so symbol-level
    company-news endpoints will not find them — we use the general feed.
    Never raises; returns [] on any failure.
    """
    if not FINNHUB_API_KEY:
        return []
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            results: list[dict] = []
            for category in ("general", "merger"):
                try:
                    resp = await client.get(
                        _FINNHUB_NEWS_URL,
                        params={"category": category, "token": FINNHUB_API_KEY},
                    )
                    if resp.status_code != 200:
                        continue
                    page = resp.json()
                    if isinstance(page, list):
                        results.extend(page)
                except Exception:
                    continue

        if not results:
            return []

        cl = company.lower()
        matched: list[dict[str, Any]] = []
        seen_urls: set[str] = set()
        for n in results:
            if not isinstance(n, dict):
                continue
            head = (n.get("headline") or "").strip()
            summary = (n.get("summary") or "").strip()
            url = (n.get("url") or "").strip()
            if not head or not url:
                continue
            blob = f"{head} {summary}".lower()
            if cl not in blob:
                continue
            if url in seen_urls:
                continue
            seen_urls.add(url)
            ts = n.get("datetime")
            published = None
            try:
                if ts:
                    published = datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
            except Exception:
                published = None
            matched.append({
                "title":        head,
                "source":       n.get("source") or "Finnhub",
                "url":          url,
                "published_at": published,
            })

        # Most recent first, cap at 5
        matched.sort(key=lambda x: x.get("published_at") or "", reverse=True)
        return matched[:5]
    except Exception as e:
        print(f"[PRE_IPO] finnhub {company} error: {e}")
        return []


# ── Synthesis ────────────────────────────────────────────────────────────────

def _confidence_score(
    perplexity_data: dict[str, Any],
    polymarket_data: dict[str, Any],
    finnhub_news:    list[dict[str, Any]],
) -> str:
    """
    High   = recent credible source + multiple confirmations + prediction market
    Medium = credible recent source but limited market data
    Low    = rumor-only / stale / thin data
    """
    has_perplexity = bool(perplexity_data.get("sources"))
    perplexity_strong = len(perplexity_data.get("sources") or []) >= 2 and (
        perplexity_data.get("estimated_valuation") not in (None, "", "Unknown")
        or perplexity_data.get("ipo_status") not in (None, "", "Unknown")
    )
    has_market = (
        polymarket_data.get("ipo_probability_12m") is not None
        or bool(polymarket_data.get("valuation_markets"))
    )
    has_news = bool(finnhub_news)

    confirmations = sum([perplexity_strong, has_market, has_news])

    if perplexity_strong and has_market and confirmations >= 3:
        return "High"
    if has_perplexity and (has_market or has_news):
        return "Medium"
    if has_perplexity:
        return "Medium"
    return "Low"


async def _build_company_payload(spec: dict[str, Any]) -> dict[str, Any]:
    """Build a single company entry; isolated so one failure can't kill others."""
    company = spec["company"]
    try:
        perplexity_task = asyncio.create_task(_fetch_perplexity_for_company(company))
        polymarket_task = asyncio.create_task(
            _fetch_polymarket_for_company(company, spec["polymarket_keywords"])
        )
        finnhub_task = asyncio.create_task(_fetch_finnhub_news_for_company(company))

        perplexity_data, polymarket_data, finnhub_news = await asyncio.gather(
            perplexity_task, polymarket_task, finnhub_task,
            return_exceptions=False,
        )
    except Exception as e:
        print(f"[PRE_IPO] aggregate fetch failed for {company}: {e}")
        perplexity_data = {
            "ipo_status":          "Unknown",
            "estimated_valuation": "Unknown",
            "valuation_notes":     [],
            "catalysts":           [],
            "expected_window":     {"earliest": "Unknown", "likely": "Unknown"},
            "sources":             [],
        }
        polymarket_data = {
            "ipo_probability_12m": None,
            "valuation_markets":   [],
            "summary":             "Polymarket lookup failed.",
        }
        finnhub_news = []

    confidence = _confidence_score(perplexity_data, polymarket_data, finnhub_news)

    return {
        "company":             company,
        "ipo_status":          perplexity_data.get("ipo_status") or "Unknown",
        "estimated_valuation": perplexity_data.get("estimated_valuation") or "Unknown",
        "valuation_notes":     perplexity_data.get("valuation_notes") or [],
        "polymarket": {
            "ipo_probability_12m": polymarket_data.get("ipo_probability_12m"),
            "valuation_markets":   polymarket_data.get("valuation_markets") or [],
            "summary":             polymarket_data.get("summary") or "",
        },
        "catalysts":           perplexity_data.get("catalysts") or [],
        "expected_window":     perplexity_data.get("expected_window") or {
            "earliest": "Unknown",
            "likely":   "Unknown",
        },
        "confidence_score":    confidence,
        "latest_news":         finnhub_news or [],
        "sources":             perplexity_data.get("sources") or [],
    }


async def _build_full_payload() -> dict[str, Any]:
    """Build the full /api/calendar/pre-ipo-watchlist response."""
    tasks = [_build_company_payload(spec) for spec in TRACKED_COMPANIES]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    companies: list[dict[str, Any]] = []
    for spec, res in zip(TRACKED_COMPANIES, results):
        if isinstance(res, Exception):
            print(f"[PRE_IPO] {spec['company']} task error: {res}")
            companies.append({
                "company":             spec["company"],
                "ipo_status":          "Unknown",
                "estimated_valuation": "Unknown",
                "valuation_notes":     [],
                "polymarket": {
                    "ipo_probability_12m": None,
                    "valuation_markets":   [],
                    "summary":             "Data temporarily unavailable.",
                },
                "catalysts":           [],
                "expected_window":     {"earliest": "Unknown", "likely": "Unknown"},
                "confidence_score":    "Low",
                "latest_news":         [],
                "sources":             [],
            })
        else:
            companies.append(res)

    return {
        "status":     "ok",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "companies":  companies,
    }


# ── Public entry point ──────────────────────────────────────────────────────

@traceable(name="pre_ipo_watchlist.get")
async def get_pre_ipo_watchlist(refresh: bool = False) -> dict[str, Any]:
    """
    Return the cached pre-IPO watchlist payload, refreshing if stale.

    On refresh failure, returns the most recent stale payload (with
    `status` flipped to "stale") so consumers always get usable data
    when at least one successful fetch has happened previously.
    """
    now = time.time()
    cached = _state["value"]
    fresh_at = _state["fresh_at"]
    is_fresh = cached is not None and (now - fresh_at) < _FRESH_TTL_SECONDS

    if cached is not None and is_fresh and not refresh:
        return cached

    async with _state["lock"]:
        # Re-check inside the lock — another caller may have refreshed.
        now = time.time()
        cached = _state["value"]
        fresh_at = _state["fresh_at"]
        is_fresh = cached is not None and (now - fresh_at) < _FRESH_TTL_SECONDS
        if cached is not None and is_fresh and not refresh:
            return cached

        try:
            payload = await _build_full_payload()
            _state["value"] = payload
            _state["fresh_at"] = time.time()
            return payload
        except Exception as e:
            print(f"[PRE_IPO] full build failed: {e}")
            if cached is not None:
                stale = dict(cached)
                stale["status"] = "stale"
                stale["stale_reason"] = f"refresh failed: {e}"
                return stale
            # No cache to fall back on
            return {
                "status":     "error",
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "error":      f"upstream fetch failed: {e}",
                "companies":  [],
            }
