"""
Smart Earnings Scanner — AI-curated earnings calendar.

Runs twice daily (8am + 12pm EST via scheduler in main.py):
  1. Fetches all earnings tickers for the week from Finnhub (free, one call)
  2. Single Grok x_search call across ALL tickers — social buzz + sentiment
  3. Single Perplexity chat call across ALL tickers — news signals + analyst focus
  4. Scores, ranks, and caches 10-20 per day (min 10, max 20)

COST PER SCAN:
  - Finnhub: 1 free API call (included in key)
  - Grok (xAI): 1 x_search call (~$0.01-0.05 depending on output)
  - Perplexity: 1 sonar chat call (~$0.005-0.02)
  Total: ~$0.02-0.07 per scan, 2x/day = ~$0.04-0.14/day

Cache persists to disk (data/earnings_smart_cache.json) across restarts.
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
    # Find most recent cached_at
    latest = max((v.get("cached_at", 0) for v in data.values()), default=0)
    if latest == 0:
        return {"status": "empty", "last_updated": None}
    age_hours = (time.time() - latest) / 3600
    return {
        "status": "stale" if age_hours > 6 else "fresh",
        "last_updated": datetime.fromtimestamp(latest).isoformat(),
        "age_hours": round(age_hours, 1),
    }


async def _fetch_week_tickers(finnhub_client) -> dict[str, list[dict]]:
    """Fetch all earnings tickers for the current week from Finnhub.
    Returns dict keyed by date string, each value is list of ticker dicts."""
    today = datetime.now()
    monday = today - timedelta(days=today.weekday())
    friday = monday + timedelta(days=4)
    from_date = monday.strftime("%Y-%m-%d")
    to_date = friday.strftime("%Y-%m-%d")

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
# ONE xAI x_search call across ALL tickers for the week.
# Uses the Responses API with x_search tool to scan X/Twitter.
# Cost: ~$0.01-0.05 per call depending on output length.

async def _grok_batch_scan(xai_key: str, tickers: list[str]) -> list[dict]:
    """Single Grok x_search call for social buzz on all earnings tickers."""
    if not xai_key or not tickers:
        return []

    ticker_list = ", ".join(tickers[:150])  # Cap at 150 to stay within context
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
        # Extract text from Responses API output
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
# ONE Perplexity sonar chat call across ALL tickers for the week.
# Uses chat/completions endpoint with sonar model.
# Cost: ~$0.005-0.02 per call.

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
    # Try direct parse first
    text = text.strip()
    if text.startswith("```"):
        # Strip markdown code fences
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        text = text.strip()
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
    except json.JSONDecodeError:
        pass
    # Try to find array in text
    match = re.search(r"\[[\s\S]*\]", text)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return []


def _score_and_rank(
    by_date: dict[str, list[dict]],
    grok_results: list[dict],
    pplx_results: list[dict],
) -> dict:
    """Merge Grok + Perplexity results, score, rank, return 10-20 per day.

    GUARANTEE: if a day has >= 10 raw tickers, we always return 10-20.
    If a day has < 10, we return all of them.
    """
    # Index by ticker
    grok_map = {r.get("ticker", "").upper(): r for r in grok_results if isinstance(r, dict)}
    pplx_map = {r.get("ticker", "").upper(): r for r in pplx_results if isinstance(r, dict)}

    all_signal_tickers = set(grok_map.keys()) | set(pplx_map.keys())
    print(f"[SMART_EARNINGS] _score_and_rank: grok_map={len(grok_map)} tickers, pplx_map={len(pplx_map)} tickers, union={len(all_signal_tickers)}")

    result = {}
    now = time.time()

    MIN_PER_DAY = 10
    MAX_PER_DAY = 20

    for date_str, tickers in by_date.items():
        total_raw = len(tickers)

        # Build enriched entries for ALL tickers on this day
        all_entries = []
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

            has_signal = sym in all_signal_tickers or buzz >= 6 or news_signal == "high"
            score = buzz * 2 + (5 if news_signal == "high" else 2 if news_signal == "medium" else 0) + (3 if analyst_focus else 0)
            # Give a small boost to tickers with higher revenue estimates so
            # backfill tickers sort by market importance when scores are tied at 0
            rev = t.get("revenue_estimate") or 0
            rev_boost = min(rev / 1e10, 1.0) if rev > 0 else 0  # 0-1 range, won't override real scores

            all_entries.append({
                **t,
                "buzz_level": buzz,
                "sentiment": sentiment,
                "news_signal": news_signal,
                "analyst_focus": analyst_focus,
                "one_line": one_line,
                "score": score + rev_boost if not has_signal else score,
                "_has_signal": has_signal,
            })

        # Separate signal-passing tickers from the rest
        scored = [e for e in all_entries if e["_has_signal"]]
        remaining = [e for e in all_entries if not e["_has_signal"]]

        # Sort scored by score desc
        scored.sort(key=lambda x: x["score"], reverse=True)

        # Backfill: if fewer than MIN_PER_DAY passed the threshold,
        # fill from remaining sorted by revenue estimate desc
        if len(scored) < MIN_PER_DAY:
            remaining.sort(key=lambda x: x.get("revenue_estimate") or 0, reverse=True)
            need = MIN_PER_DAY - len(scored)
            scored.extend(remaining[:need])

        # Cap at MAX_PER_DAY
        signal_count = len(scored)  # count before backfill cap is already applied, but after extend
        scored = scored[:MAX_PER_DAY]

        print(f"[SMART_EARNINGS]   {date_str}: {total_raw} raw, {len([e for e in all_entries if e.get('_has_signal')])} signal, backfilled to {signal_count}, capped to {len(scored)} final")

        # Clean internal flag before caching
        for e in scored:
            e.pop("_has_signal", None)

        result[date_str] = {
            "tickers": scored,
            "count": len(scored),
            "cached_at": now,
        }

    return result


async def run_smart_scan(finnhub_client, xai_key: str, perplexity_key: str) -> dict:
    """Full smart scan: fetch tickers → Grok + Perplexity → score → cache."""
    print("[SMART_EARNINGS] Starting scan...")
    start = time.time()

    try:
        by_date = await _fetch_week_tickers(finnhub_client)
    except Exception as e:
        print(f"[SMART_EARNINGS] Finnhub fetch failed: {e}")
        return {}

    # Flatten all unique tickers for the week
    all_tickers = list({t["ticker"] for tickers in by_date.values() for t in tickers})
    print(f"[SMART_EARNINGS] {len(all_tickers)} unique tickers across {len(by_date)} days")

    if not all_tickers:
        return {}

    # Run Grok + Perplexity in parallel (one call each)
    grok_task = _grok_batch_scan(xai_key, all_tickers)
    pplx_task = _perplexity_batch_scan(perplexity_key, all_tickers)
    grok_results, pplx_results = await asyncio.gather(
        grok_task, pplx_task, return_exceptions=True
    )

    if isinstance(grok_results, Exception):
        print(f"[SMART_EARNINGS] Grok exception: {grok_results}")
        grok_results = []
    if isinstance(pplx_results, Exception):
        print(f"[SMART_EARNINGS] Perplexity exception: {pplx_results}")
        pplx_results = []

    print(f"[SMART_EARNINGS] Grok returned {len(grok_results)} tickers, Perplexity returned {len(pplx_results)} tickers")

    # Score and rank
    scored = _score_and_rank(by_date, grok_results, pplx_results)

    # Write to persistent file cache
    existing = _read_cache()
    existing.update(scored)
    _write_cache(existing)

    elapsed = time.time() - start
    total_curated = sum(d["count"] for d in scored.values())
    days_cached = list(scored.keys())
    print(f"[SMART_EARNINGS] Scan complete: {total_curated} curated tickers across {len(days_cached)} days in {elapsed:.1f}s")
    print(f"[SMART_EARNINGS] Cache written to {CACHE_FILE} — days: {days_cached}")
    for d, v in scored.items():
        print(f"[SMART_EARNINGS]   {d}: {v['count']} tickers cached")

    return scored


def get_fallback_top_tickers(by_date_from_calendar: list[dict], date_str: str, limit: int = 20) -> dict:
    """Fallback: return top tickers by revenue estimate (proxy for market cap) if cache is empty.
    Always returns 10-20 tickers (or all if fewer than 10 exist)."""
    day_tickers = [e for e in by_date_from_calendar if e.get("date") == date_str]
    # Sort by revenue_estimate desc as proxy for market cap
    day_tickers.sort(key=lambda x: x.get("revenue_estimate") or 0, reverse=True)
    day_tickers = day_tickers[:limit]
    print(f"[SMART_EARNINGS] Fallback for {date_str}: {len(day_tickers)} tickers (from {len([e for e in by_date_from_calendar if e.get('date') == date_str])} total)")

    return {
        "tickers": [
            {
                **t,
                "buzz_level": 0,
                "sentiment": "mixed",
                "news_signal": "low",
                "analyst_focus": False,
                "one_line": "",
                "score": 0,
            }
            for t in day_tickers
        ],
        "count": len(day_tickers),
        "cached_at": 0,
        "fallback": True,
    }
