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
from pathlib import Path
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


# ── Scoring ─────────────────────────────────────────────────────────────────

# Strong/heating phrases imply formal IPO progress.
_PHRASE_TIERS: list[tuple[tuple[str, ...], tuple[int, int]]] = [
    # (phrases, (low, high)) — score within range based on how decisive the language is.
    (
        (
            "confidential filing", "confidentially filed", "filed s-1", "filed s1",
            "files for ipo", "filed for ipo", "filing for ipo", "registration statement",
            "ipo filed", "draft registration", "drs",
        ),
        (70, 90),
    ),
    (
        (
            "expected to ipo", "preparing for ipo", "preparing to ipo",
            "selected banks", "hired banks", "hired underwriters", "tapped banks",
            "ipo filing likely", "filing likely", "plans to file", "plans to ipo",
            "ipo as soon as", "targeting ipo", "ipo target", "could file",
            "moving toward ipo",
        ),
        (40, 70),
    ),
    (
        (
            "rumored", "considering ipo", "considering an ipo", "weighing ipo",
            "exploring ipo", "exploring an ipo", "watching market", "watching the market",
            "could ipo", "may ipo", "might ipo", "potential ipo",
        ),
        (20, 35),
    ),
    (
        (
            "no ipo plans", "not planning to ipo", "no plans to ipo",
            "no plans for an ipo", "no announcement", "ceo denies",
            "denies ipo", "ruled out", "no immediate plans",
            "remain private", "stay private", "staying private",
        ),
        (5, 15),
    ),
]


def _ipo_probability_score(
    polymarket_data: dict[str, Any],
    perplexity_data: dict[str, Any],
) -> int:
    """
    Returns IPO probability sub-score (0-30).

    Prefers Polymarket implied probability when present; otherwise derives
    from Perplexity language tier mapping.
    """
    try:
        pm_prob = polymarket_data.get("ipo_probability_12m")
        if isinstance(pm_prob, (int, float)) and 0.0 <= pm_prob <= 1.0:
            # Map 0..1 to 0..30 directly.
            return int(round(float(pm_prob) * 30.0))
    except Exception:
        pass

    # Fall back to Perplexity language inspection.
    blob_parts: list[str] = []
    try:
        blob_parts.append(str(perplexity_data.get("ipo_status") or ""))
        blob_parts.extend(str(x) for x in (perplexity_data.get("catalysts") or []))
        blob_parts.extend(str(x) for x in (perplexity_data.get("valuation_notes") or []))
        ew = perplexity_data.get("expected_window") or {}
        if isinstance(ew, dict):
            blob_parts.append(str(ew.get("earliest") or ""))
            blob_parts.append(str(ew.get("likely") or ""))
    except Exception:
        pass
    blob = " ".join(blob_parts).lower()

    if not blob.strip():
        # No usable Perplexity content.
        return 8  # neutral-low, treat as "unknown"

    for phrases, (low, high) in _PHRASE_TIERS:
        for p in phrases:
            if p in blob:
                # Map within tier to 0-30 by scaling tier midpoint.
                pct = (low + high) / 2.0 / 100.0
                return int(round(pct * 30.0))

    # Generic Perplexity content but nothing matched: treat as low/neutral.
    return 8


_VALUATION_RANGE_RE = re.compile(
    r"\$?\s*(\d+(?:\.\d+)?)\s*([bmtBMT])"
)


def _largest_valuation_billions(text: str) -> Optional[float]:
    """Return the largest dollar figure in `text` expressed in billions."""
    if not text:
        return None
    best: Optional[float] = None
    try:
        for num_str, unit in _VALUATION_RANGE_RE.findall(text):
            n = float(num_str)
            u = unit.lower()
            if u == "t":
                v = n * 1000.0
            elif u == "b":
                v = n
            elif u == "m":
                v = n / 1000.0
            else:
                continue
            if best is None or v > best:
                best = v
    except Exception:
        return best
    return best


def _valuation_momentum_score(perplexity_data: dict[str, Any]) -> int:
    """
    Returns valuation momentum sub-score (0-25).

    Heuristic: compare the largest dollar figure in the headline valuation
    against any prior figure mentioned in valuation_notes.
    """
    headline = str(perplexity_data.get("estimated_valuation") or "")
    notes_blob = " ".join(str(x) for x in (perplexity_data.get("valuation_notes") or []))

    if not headline or headline.strip().lower() in {"unknown", ""}:
        # No headline valuation at all — give a small base if notes exist.
        return 5 if notes_blob.strip() else 0

    headline_v = _largest_valuation_billions(headline)
    notes_v = _largest_valuation_billions(notes_blob)

    blob = (headline + " " + notes_blob).lower()
    has_up_language = any(
        p in blob for p in (
            "up from", "increased from", "doubled", "tripled",
            "raise at", "raising at", "secondary at higher", "tender at higher",
            "boosted valuation", "higher valuation",
        )
    )
    has_down_language = any(
        p in blob for p in (
            "down from", "down round", "markdown", "lower valuation",
            "cut valuation", "valuation cut", "reduced valuation",
        )
    )
    has_flat_language = any(
        p in blob for p in ("flat valuation", "unchanged valuation", "no change in valuation")
    )

    if has_down_language:
        return 5
    if has_flat_language:
        return 10

    if headline_v is not None and notes_v is not None and notes_v > 0:
        ratio = headline_v / notes_v
        if ratio >= 1.5:
            return 25
        if ratio >= 1.15:
            return 20
        if ratio >= 1.0:
            return 15
        if ratio >= 0.85:
            return 10
        return 5

    if has_up_language:
        return 20

    if headline_v is not None:
        # Has a concrete headline figure but no historical anchor.
        if headline_v >= 100.0:
            return 18
        if headline_v >= 25.0:
            return 15
        return 12

    # Headline exists but is unparseable.
    return 8


def _parse_published_dt(s: Any) -> Optional[datetime]:
    if not s:
        return None
    try:
        if isinstance(s, datetime):
            return s if s.tzinfo else s.replace(tzinfo=timezone.utc)
        text = str(s).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _news_recency_score(finnhub_news: list[dict[str, Any]]) -> int:
    """
    Returns news recency sub-score (0-20).
    """
    if not finnhub_news:
        return 0
    now = datetime.now(timezone.utc)
    best_age_days: Optional[float] = None
    for n in finnhub_news:
        if not isinstance(n, dict):
            continue
        dt = _parse_published_dt(n.get("published_at"))
        if dt is None:
            continue
        try:
            age = (now - dt).total_seconds() / 86400.0
        except Exception:
            continue
        if age < 0:
            age = 0
        if best_age_days is None or age < best_age_days:
            best_age_days = age

    if best_age_days is None:
        # We have items but no parseable timestamps — count as old.
        return 6 if len(finnhub_news) >= 2 else 4

    count = len(finnhub_news)
    if best_age_days <= 7:
        base = 18
    elif best_age_days <= 30:
        base = 15
    elif best_age_days <= 90:
        base = 10
    elif best_age_days <= 180:
        base = 5
    else:
        base = 2
    bonus = 2 if count >= 3 else (1 if count >= 2 else 0)
    return min(20, base + bonus)


_CREDIBLE_DOMAINS = (
    "bloomberg.com", "reuters.com", "wsj.com", "ft.com", "nytimes.com",
    "cnbc.com", "theinformation.com", "axios.com", "techcrunch.com",
    "businesswire.com", "prnewswire.com", "barrons.com", "economist.com",
    "sec.gov", "investor.gov", "forbes.com",
)
_OFFICIAL_DOMAINS = ("sec.gov", "investor.gov")


def _source_quality_score(
    perplexity_data: dict[str, Any],
    finnhub_news: list[dict[str, Any]],
) -> int:
    """
    Returns source quality sub-score (0-15).
    """
    sources = perplexity_data.get("sources") or []
    news = finnhub_news or []

    credible = 0
    official = 0
    total = 0
    for s in sources:
        if not isinstance(s, dict):
            continue
        url = str(s.get("url") or "").lower()
        if not url:
            continue
        total += 1
        if any(d in url for d in _OFFICIAL_DOMAINS):
            official += 1
        if any(d in url for d in _CREDIBLE_DOMAINS):
            credible += 1
    for n in news:
        if not isinstance(n, dict):
            continue
        url = str(n.get("url") or "").lower()
        if not url:
            continue
        total += 1
        if any(d in url for d in _OFFICIAL_DOMAINS):
            official += 1
        if any(d in url for d in _CREDIBLE_DOMAINS):
            credible += 1

    if total == 0:
        return 0

    score = 0
    score += min(6, credible * 2)
    if official > 0:
        score += min(5, 2 + official)
    # Quantity bonus
    if total >= 5:
        score += 3
    elif total >= 3:
        score += 2
    elif total >= 1:
        score += 1
    return min(15, score)


_CATALYST_KEYWORDS: list[tuple[tuple[str, ...], int]] = [
    (("revenue run rate", "annual recurring revenue", "arr ", "revenue scale",
      "$1b revenue", "billion in revenue", "revenue billion"), 3),
    (("funding round", "series ", "raised $", "raising $", "tender offer",
      "secondary offering", "secondary sale"), 3),
    (("hired banks", "selected banks", "tapped banks", "hired underwriters",
      "goldman", "morgan stanley", "jpmorgan", "advisor"), 3),
    (("regulatory pressure", "antitrust", "ftc", "doj investigation",
      "compliance pressure"), 2),
    (("liquidity", "employee liquidity", "investor liquidity", "cap table pressure",
      "investor pressure", "shareholder pressure"), 2),
    (("strategic partnership", "partnership with", "joint venture",
      "major contract", "government contract"), 2),
]


def _catalyst_strength_score(perplexity_data: dict[str, Any]) -> int:
    """
    Returns catalyst strength sub-score (0-10).
    """
    catalysts = perplexity_data.get("catalysts") or []
    notes = perplexity_data.get("valuation_notes") or []
    if not catalysts and not notes:
        return 0
    blob = " ".join(str(x).lower() for x in catalysts) + " " + \
        " ".join(str(x).lower() for x in notes)

    score = 0
    matched_keys: set[int] = set()
    for idx, (phrases, weight) in enumerate(_CATALYST_KEYWORDS):
        for p in phrases:
            if p.strip() and p in blob:
                if idx not in matched_keys:
                    score += weight
                    matched_keys.add(idx)
                break

    # Modest bonus for raw catalyst count
    n = len([c for c in catalysts if str(c).strip()])
    if n >= 4:
        score += 2
    elif n >= 2:
        score += 1

    return min(10, score)


def _compute_score_breakdown(
    perplexity_data: dict[str, Any],
    polymarket_data: dict[str, Any],
    finnhub_news: list[dict[str, Any]],
) -> dict[str, int]:
    """Compute the 5 sub-scores; never raises."""
    try:
        ipo_prob = _ipo_probability_score(polymarket_data, perplexity_data)
    except Exception:
        ipo_prob = 0
    try:
        val_mom = _valuation_momentum_score(perplexity_data)
    except Exception:
        val_mom = 0
    try:
        recency = _news_recency_score(finnhub_news)
    except Exception:
        recency = 0
    try:
        src_q = _source_quality_score(perplexity_data, finnhub_news)
    except Exception:
        src_q = 0
    try:
        catalyst = _catalyst_strength_score(perplexity_data)
    except Exception:
        catalyst = 0

    return {
        "ipo_probability_score":     max(0, min(30, int(ipo_prob))),
        "valuation_momentum_score":  max(0, min(25, int(val_mom))),
        "news_recency_score":        max(0, min(20, int(recency))),
        "source_quality_score":      max(0, min(15, int(src_q))),
        "catalyst_strength_score":   max(0, min(10, int(catalyst))),
    }


def _opportunity_score_from_breakdown(breakdown: dict[str, int]) -> int:
    total = sum(int(v) for v in breakdown.values())
    return max(0, min(100, total))


def _momentum_badge(score: int, score_change: Optional[int]) -> str:
    if score >= 70 or (score_change is not None and score_change >= 10):
        return "Heating Up"
    if score >= 40:
        return "Watch"
    return "Dormant"


# ── Snapshot persistence ────────────────────────────────────────────────────

_SNAPSHOT_PATH = Path(__file__).parent.parent / "data" / "pre_ipo_snapshots.json"


def _load_snapshots() -> dict[str, Any]:
    """Load prior snapshot map keyed by company name. Never raises."""
    try:
        if not _SNAPSHOT_PATH.exists():
            return {}
        raw = _SNAPSHOT_PATH.read_text()
        if not raw.strip():
            return {}
        data = json.loads(raw)
        if not isinstance(data, dict):
            return {}
        return data
    except Exception as e:
        print(f"[PRE_IPO] snapshot load failed: {e}")
        return {}


def _write_snapshots(snapshots: dict[str, Any]) -> None:
    """Persist snapshot map to disk. Never raises."""
    try:
        _SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _SNAPSHOT_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(snapshots, indent=2))
        tmp.replace(_SNAPSHOT_PATH)
    except Exception as e:
        print(f"[PRE_IPO] snapshot write failed: {e}")


def _diff_change_tracking(
    company: str,
    current: dict[str, Any],
    prior_snapshots: dict[str, Any],
) -> dict[str, Any]:
    """
    Build the change_tracking block for `company` by comparing `current`
    against the prior snapshot, if any. Always returns a safely shaped dict.
    """
    block: dict[str, Any] = {
        "valuation_change":       "Unknown",
        "ipo_probability_change": "Unknown",
        "new_catalysts":          [],
        "previous_score":         None,
        "score_change":           None,
        "last_snapshot_at":       None,
    }
    try:
        prior = prior_snapshots.get(company)
        if not isinstance(prior, dict):
            return block

        block["last_snapshot_at"] = prior.get("snapshot_at")

        prev_score = prior.get("opportunity_score")
        if isinstance(prev_score, (int, float)):
            block["previous_score"] = int(prev_score)
            cur_score = current.get("opportunity_score")
            if isinstance(cur_score, (int, float)):
                block["score_change"] = int(cur_score) - int(prev_score)

        # Valuation comparison
        prev_val = str(prior.get("estimated_valuation") or "").strip()
        cur_val = str(current.get("estimated_valuation") or "").strip()
        if prev_val and cur_val and prev_val.lower() != "unknown" and cur_val.lower() != "unknown":
            if prev_val == cur_val:
                block["valuation_change"] = "Unchanged"
            else:
                pv = _largest_valuation_billions(prev_val)
                cv = _largest_valuation_billions(cur_val)
                if pv is not None and cv is not None and pv > 0:
                    if cv > pv * 1.02:
                        block["valuation_change"] = f"Up: {prev_val} → {cur_val}"
                    elif cv < pv * 0.98:
                        block["valuation_change"] = f"Down: {prev_val} → {cur_val}"
                    else:
                        block["valuation_change"] = "Unchanged"
                else:
                    block["valuation_change"] = f"{prev_val} → {cur_val}"

        # IPO probability comparison
        prev_pm = prior.get("polymarket_ipo_probability_12m")
        cur_pm = current.get("polymarket", {}).get("ipo_probability_12m")
        if isinstance(prev_pm, (int, float)) and isinstance(cur_pm, (int, float)):
            delta = float(cur_pm) - float(prev_pm)
            if abs(delta) < 0.01:
                block["ipo_probability_change"] = "Unchanged"
            else:
                sign = "+" if delta > 0 else ""
                block["ipo_probability_change"] = (
                    f"{sign}{delta * 100:.1f}pp ({prev_pm * 100:.0f}% → {cur_pm * 100:.0f}%)"
                )
        else:
            prev_status = str(prior.get("ipo_status") or "").strip()
            cur_status = str(current.get("ipo_status") or "").strip()
            if prev_status and cur_status:
                if prev_status == cur_status:
                    block["ipo_probability_change"] = "Unchanged"
                else:
                    block["ipo_probability_change"] = f"{prev_status} → {cur_status}"

        # New catalysts
        prev_catalysts = {str(x).strip().lower() for x in (prior.get("catalysts") or []) if str(x).strip()}
        new_catalysts: list[str] = []
        for c in (current.get("catalysts") or []):
            cs = str(c).strip()
            if cs and cs.lower() not in prev_catalysts:
                new_catalysts.append(cs)
        block["new_catalysts"] = new_catalysts[:6]
    except Exception as e:
        print(f"[PRE_IPO] diff change_tracking failed for {company}: {e}")
    return block


def _build_snapshot_record(company: dict[str, Any]) -> dict[str, Any]:
    """Reduce a fully-built company entry to the fields we need to persist."""
    pm = company.get("polymarket") or {}
    return {
        "company":                          company.get("company"),
        "snapshot_at":                      datetime.now(timezone.utc).isoformat(),
        "opportunity_score":                company.get("opportunity_score"),
        "estimated_valuation":              company.get("estimated_valuation"),
        "ipo_status":                       company.get("ipo_status"),
        "polymarket_ipo_probability_12m":   pm.get("ipo_probability_12m"),
        "catalysts":                        list(company.get("catalysts") or []),
    }


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

    score_breakdown = _compute_score_breakdown(perplexity_data, polymarket_data, finnhub_news)
    opportunity_score = _opportunity_score_from_breakdown(score_breakdown)

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
        "opportunity_score":   opportunity_score,
        "score_breakdown":     score_breakdown,
        # rank, momentum_badge, change_tracking are filled in at the
        # full-payload assembly stage where ordering/snapshots are known.
    }


def _empty_company_entry(company_name: str) -> dict[str, Any]:
    breakdown = {
        "ipo_probability_score":     0,
        "valuation_momentum_score":  0,
        "news_recency_score":        0,
        "source_quality_score":      0,
        "catalyst_strength_score":   0,
    }
    return {
        "company":             company_name,
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
        "opportunity_score":   0,
        "score_breakdown":     breakdown,
    }


async def _build_full_payload() -> dict[str, Any]:
    """Build the full /api/calendar/pre-ipo-watchlist response."""
    tasks = [_build_company_payload(spec) for spec in TRACKED_COMPANIES]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    companies: list[dict[str, Any]] = []
    for spec, res in zip(TRACKED_COMPANIES, results):
        if isinstance(res, Exception):
            print(f"[PRE_IPO] {spec['company']} task error: {res}")
            companies.append(_empty_company_entry(spec["company"]))
        else:
            # Defensive: ensure required scoring fields exist even if
            # _build_company_payload unexpectedly returned a partial dict.
            if "opportunity_score" not in res:
                res["opportunity_score"] = 0
            if "score_breakdown" not in res:
                res["score_breakdown"] = {
                    "ipo_probability_score":     0,
                    "valuation_momentum_score":  0,
                    "news_recency_score":        0,
                    "source_quality_score":      0,
                    "catalyst_strength_score":   0,
                }
            companies.append(res)

    # Load prior snapshots for change tracking; never break on failure.
    try:
        prior_snapshots = _load_snapshots()
    except Exception as e:
        print(f"[PRE_IPO] snapshot load wrapper failed: {e}")
        prior_snapshots = {}

    # Compute change_tracking BEFORE sorting so we have stable references.
    for c in companies:
        try:
            c["change_tracking"] = _diff_change_tracking(
                c.get("company") or "", c, prior_snapshots,
            )
        except Exception as e:
            print(f"[PRE_IPO] change_tracking failed for {c.get('company')}: {e}")
            c["change_tracking"] = {
                "valuation_change":       "Unknown",
                "ipo_probability_change": "Unknown",
                "new_catalysts":          [],
                "previous_score":         None,
                "score_change":           None,
                "last_snapshot_at":       None,
            }

    # Compute momentum_badge using current score and score_change.
    for c in companies:
        try:
            sc = int(c.get("opportunity_score") or 0)
            ch = c.get("change_tracking") or {}
            score_change = ch.get("score_change") if isinstance(ch, dict) else None
            c["momentum_badge"] = _momentum_badge(sc, score_change)
        except Exception:
            c["momentum_badge"] = "Dormant"

    # Sort by opportunity_score desc, stable within ties.
    companies.sort(key=lambda x: int(x.get("opportunity_score") or 0), reverse=True)

    # Assign rank.
    for idx, c in enumerate(companies, start=1):
        c["rank"] = idx

    # Persist new snapshot map (best-effort).
    try:
        new_snapshots: dict[str, Any] = {}
        for c in companies:
            name = c.get("company")
            if not name:
                continue
            new_snapshots[name] = _build_snapshot_record(c)
        _write_snapshots(new_snapshots)
    except Exception as e:
        print(f"[PRE_IPO] snapshot persist wrapper failed: {e}")

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
