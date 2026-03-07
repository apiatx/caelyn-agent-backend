"""
Smart Earnings Scanner — Two-tier AI-curated earnings calendar.

Architecture:
  Tier 1 (Polymarket): Handled entirely on the frontend from already-fetched data.
  Tier 2 (Social + News): Background scan via Grok x_search + Perplexity sonar.

run_smart_scan(finnhub_client, xai_key, pplx_key, reference_date)
  accepts any date — computes the Mon-Fri week containing that date,
  fetches Finnhub earnings for that range, runs ONE Grok + ONE Perplexity
  call, scores and ranks Tier 2 tickers (top 5-15 by buzz), caches per day.

Cache key: each date string ("2026-03-10") maps to its own entry in
  data/earnings_smart_cache.json with a 6-hour TTL.

COST PER SCAN:
  - Finnhub: 1 free API call
  - Grok (xAI): 1 x_search call (~$0.01-0.05)
  - Perplexity: 1 sonar chat call (~$0.005-0.02)
  Total: ~$0.02-0.07 per scan
"""

import asyncio
import json
import time
import re
from datetime import datetime, timedelta
from pathlib import Path

import httpx

CACHE_FILE = Path("data/earnings_smart_cache.json")
CACHE_TTL = 6 * 3600  # 6 hours

MAX_TIER2_PER_DAY = 15


def _read_cache() -> dict:
    """Read the file-backed smart earnings cache."""
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _write_cache(data: dict):
    """Write to disk so cache survives restarts."""
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w") as f:
        json.dump(data, f, separators=(",", ":"))


def get_cached_smart_day(date_str: str) -> dict | None:
    """Return cached smart data for a date, or None if stale/missing."""
    data = _read_cache()
    day = data.get(date_str)
    if not day:
        return None
    cached_at = day.get("cached_at", 0)
    if time.time() - cached_at > CACHE_TTL:
        return None
    return day


def get_cache_status() -> dict:
    """Return cache freshness info for UI display."""
    data = _read_cache()
    if not data:
        return {"status": "empty", "last_updated": None}
    latest = max((v.get("cached_at", 0) for v in data.values()), default=0)
    if latest == 0:
        return {"status": "empty", "last_updated": None}
    age_hours = (time.time() - latest) / 3600
    return {
        "status": "stale" if age_hours > 6 else "fresh",
        "last_updated": datetime.fromtimestamp(latest).isoformat(),
        "age_hours": round(age_hours, 1),
    }


async def _fetch_week_tickers(finnhub_client, reference_date: str | None = None) -> dict[str, list[dict]]:
    """Fetch earnings tickers for the week containing reference_date.
    If reference_date is None, uses today. Accepts any date string (YYYY-MM-DD).
    Returns dict keyed by date string, each value is list of ticker dicts."""
    if reference_date:
        ref = datetime.strptime(reference_date, "%Y-%m-%d")
    else:
        ref = datetime.now()

    # Compute Mon-Fri of that week
    monday = ref - timedelta(days=ref.weekday())
    friday = monday + timedelta(days=4)
    from_date = monday.strftime("%Y-%m-%d")
    to_date = friday.strftime("%Y-%m-%d")

    print(f"[SMART_EARNINGS] Fetching Finnhub earnings {from_date} → {to_date}")

    data = await asyncio.wait_for(
        asyncio.to_thread(
            finnhub_client.earnings_calendar,
            _from=from_date,
            to=to_date,
            symbol=None,
        ),
        timeout=10.0,
    )
    earnings = data.get("earningsCalendar", [])
    print(f"[SMART_EARNINGS] Finnhub returned {len(earnings)} raw earnings entries")

    by_date: dict[str, list[dict]] = {}
    for e in earnings:
        sym = e.get("symbol")
        date = e.get("date")
        if not sym or not date:
            continue
        if date not in by_date:
            by_date[date] = []
        by_date[date].append({
            "ticker": sym,
            "date": date,
            "eps_estimate": e.get("epsEstimate"),
            "revenue_estimate": e.get("revenueEstimate"),
            "hour": e.get("hour", ""),
            "quarter": e.get("quarter"),
            "year": e.get("year"),
        })
    return by_date


# ── GROK BATCH SCAN ─────────────────────────────────────────────

async def _grok_batch_scan(xai_key: str, tickers: list[str]) -> list[dict]:
    """Single Grok x_search call for social buzz on all earnings tickers."""
    if not xai_key or not tickers:
        return []

    ticker_list = ", ".join(tickers[:150])
    prompt = (
        f"Of these companies reporting earnings this week: {ticker_list}\n\n"
        "Which are investors and traders on X most actively discussing, "
        "most anticipating, or most concerned about?\n\n"
        "Return a JSON array of objects with ONLY the high-signal tickers "
        "(skip boring/low-discussion ones):\n"
        '[{"ticker": "AAPL", "sentiment": "bullish", "buzz_level": 8, '
        '"one_line": "why this earnings matters"}]\n\n'
        "sentiment must be: bullish, bearish, or mixed\n"
        "buzz_level: 1-10 (10 = most discussed)\n"
        "Focus on companies where the earnings result could be market-moving. "
        "Return 10-30 tickers maximum."
    )

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {xai_key}",
    }
    payload = {
        "model": "grok-4-1-fast-non-reasoning",
        "tools": [{"type": "x_search", "x_search": {}}],
        "input": [{"role": "user", "content": prompt}],
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                "https://api.x.ai/v1/responses",
                headers=headers,
                json=payload,
            )
        if resp.status_code != 200:
            print(f"[SMART_EARNINGS] Grok scan failed: HTTP {resp.status_code}")
            return []

        data = resp.json()
        text = ""
        for item in data.get("output", []):
            if item.get("type") == "message":
                for block in item.get("content", []):
                    if block.get("type") in ("output_text", "text"):
                        text += block.get("text", "")

        return _parse_json_array(text)

    except httpx.TimeoutException:
        print("[SMART_EARNINGS] Grok scan timed out (30s)")
        return []
    except Exception as e:
        print(f"[SMART_EARNINGS] Grok scan error: {e}")
        return []


# ── PERPLEXITY BATCH SCAN ────────────────────────────────────────

async def _perplexity_batch_scan(pplx_key: str, tickers: list[str]) -> list[dict]:
    """Single Perplexity call for news signals on all earnings tickers."""
    if not pplx_key or not tickers:
        return []

    ticker_list = ", ".join(tickers[:150])
    prompt = (
        f"Of these companies reporting earnings this week: {ticker_list}\n\n"
        "Which have the most significant analyst expectations, recent news "
        "catalysts, or potential for surprise?\n\n"
        "Return a JSON array with ONLY the noteworthy tickers:\n"
        '[{"ticker": "AAPL", "news_signal": "high", "analyst_focus": true, '
        '"one_line": "key narrative"}]\n\n'
        "news_signal: high, medium, or low\n"
        "analyst_focus: true if analysts are particularly focused on this report\n"
        "Return 10-30 tickers maximum. Skip tickers with nothing interesting."
    )

    headers = {
        "Authorization": f"Bearer {pplx_key}",
        "Content-Type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(
                "https://api.perplexity.ai/chat/completions",
                headers=headers,
                json={
                    "model": "sonar",
                    "messages": [
                        {"role": "system", "content": "Return only valid JSON arrays. No markdown, no explanation."},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.1,
                    "max_tokens": 2000,
                },
            )
        if resp.status_code != 200:
            print(f"[SMART_EARNINGS] Perplexity scan failed: HTTP {resp.status_code}")
            return []

        data = resp.json()
        text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        return _parse_json_array(text)

    except httpx.TimeoutException:
        print("[SMART_EARNINGS] Perplexity scan timed out (20s)")
        return []
    except Exception as e:
        print(f"[SMART_EARNINGS] Perplexity scan error: {e}")
        return []


def _parse_json_array(text: str) -> list[dict]:
    """Extract a JSON array from LLM output that may contain markdown fencing."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        text = text.strip()
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
    except json.JSONDecodeError:
        pass
    match = re.search(r"\[[\s\S]*\]", text)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return []


def _build_tier2(
    by_date: dict[str, list[dict]],
    grok_results: list[dict],
    pplx_results: list[dict],
) -> dict:
    """Build Tier 2 ranked tickers per day from Grok + Perplexity results.

    Returns dict keyed by date string, each value has 'tickers' (list of
    enriched ticker dicts sorted by buzz score desc, max 15 per day).
    Polymarket tickers are NOT excluded here — the frontend handles that.
    """
    grok_map = {r.get("ticker", "").upper(): r for r in grok_results if isinstance(r, dict)}
    pplx_map = {r.get("ticker", "").upper(): r for r in pplx_results if isinstance(r, dict)}

    print(f"[SMART_EARNINGS] _build_tier2: grok={len(grok_map)} tickers, pplx={len(pplx_map)} tickers")

    result = {}
    now = time.time()

    for date_str, tickers in by_date.items():
        enriched = []
        for t in tickers:
            sym = t["ticker"].upper()
            grok = grok_map.get(sym, {})
            pplx = pplx_map.get(sym, {})

            buzz = grok.get("buzz_level", 0)
            if not isinstance(buzz, (int, float)):
                try:
                    buzz = int(buzz)
                except (ValueError, TypeError):
                    buzz = 0

            news_signal = pplx.get("news_signal", "low")
            analyst_focus = pplx.get("analyst_focus", False)
            sentiment = grok.get("sentiment", "mixed")
            one_line = grok.get("one_line") or pplx.get("one_line") or ""

            score = (
                buzz * 2
                + (5 if news_signal == "high" else 2 if news_signal == "medium" else 0)
                + (3 if analyst_focus else 0)
            )

            # Only include tickers that have SOME signal from Grok or Perplexity
            if sym in grok_map or sym in pplx_map:
                enriched.append({
                    **t,
                    "buzz_level": buzz,
                    "sentiment": sentiment,
                    "news_signal": news_signal,
                    "analyst_focus": analyst_focus,
                    "one_line": one_line,
                    "score": score,
                })

        # Sort by score desc, cap at MAX_TIER2_PER_DAY
        enriched.sort(key=lambda x: x["score"], reverse=True)
        tier2 = enriched[:MAX_TIER2_PER_DAY]

        print(f"[SMART_EARNINGS]   {date_str}: {len(tickers)} raw, {len(enriched)} with signal, {len(tier2)} tier2")

        result[date_str] = {
            "tickers": tier2,
            "count": len(tier2),
            "cached_at": now,
        }

    return result


async def run_smart_scan(
    finnhub_client,
    xai_key: str,
    perplexity_key: str,
    reference_date: str | None = None,
) -> dict:
    """Full smart scan for a given week.

    reference_date: any YYYY-MM-DD string — scans the Mon-Fri week containing it.
    If None, uses the current week.
    """
    print(f"[SMART_EARNINGS] Starting scan (reference_date={reference_date or 'current week'})...")
    start = time.time()

    try:
        by_date = await _fetch_week_tickers(finnhub_client, reference_date)
    except Exception as e:
        print(f"[SMART_EARNINGS] Finnhub fetch failed: {e}")
        return {}

    all_tickers = list({t["ticker"] for tickers in by_date.values() for t in tickers})
    print(f"[SMART_EARNINGS] {len(all_tickers)} unique tickers across {len(by_date)} days")

    if not all_tickers:
        return {}

    # Run Grok + Perplexity in parallel (one call each)
    grok_results, pplx_results = await asyncio.gather(
        _grok_batch_scan(xai_key, all_tickers),
        _perplexity_batch_scan(perplexity_key, all_tickers),
        return_exceptions=True,
    )

    if isinstance(grok_results, Exception):
        print(f"[SMART_EARNINGS] Grok exception: {grok_results}")
        grok_results = []
    if isinstance(pplx_results, Exception):
        print(f"[SMART_EARNINGS] Perplexity exception: {pplx_results}")
        pplx_results = []

    print(f"[SMART_EARNINGS] Grok returned {len(grok_results)} tickers, Perplexity returned {len(pplx_results)} tickers")

    # Build Tier 2 rankings
    scored = _build_tier2(by_date, grok_results, pplx_results)

    # Write to persistent file cache (per-date keys)
    existing = _read_cache()
    existing.update(scored)
    _write_cache(existing)

    elapsed = time.time() - start
    total_curated = sum(d["count"] for d in scored.values())
    days_cached = list(scored.keys())
    print(f"[SMART_EARNINGS] Scan complete: {total_curated} tier2 tickers across {len(days_cached)} days in {elapsed:.1f}s")
    for d, v in scored.items():
        print(f"[SMART_EARNINGS]   {d}: {v['count']} tier2 tickers cached")

    return scored
