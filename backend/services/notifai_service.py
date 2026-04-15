"""
NotifAI Service — business logic for the NotifAI page endpoints.

Extracted from main.py so the two HTTP handlers can be thin wrappers.
All caching is managed here; callers just await and return.

Claude model used here: claude-haiku-4-5-20251001
(distinct from the TradingAgent's claude-3-haiku-20240307)
"""
from __future__ import annotations

import asyncio
import re as _re
import json as _json
from datetime import datetime, timedelta
from typing import Any


# ──────────────────────────────────────────────────────────────────────────────
# TTL helper
# ──────────────────────────────────────────────────────────────────────────────

def _weekly_cache_ttl() -> int:
    """Seconds until next Saturday 07:00 local time (minimum 60 s)."""
    now = datetime.now()
    days_until_sat = (5 - now.weekday()) % 7
    if days_until_sat == 0 and (now.hour > 7 or (now.hour == 7 and now.minute > 0)):
        days_until_sat = 7
    next_sat = now.replace(hour=7, minute=0, second=0, microsecond=0) + timedelta(days=days_until_sat)
    ttl = int((next_sat - now).total_seconds())
    return max(ttl, 60)


# ──────────────────────────────────────────────────────────────────────────────
# Weekly summary (Claude-written, cached until next Saturday 07:00)
# ──────────────────────────────────────────────────────────────────────────────

async def generate_weekly_summary(data_service: Any, agent: Any, force: bool = False) -> dict:
    """
    Fetch news + earnings + econ calendar, call claude-haiku-4-5-20251001,
    cache until next Saturday 07:00, and return the result dict.

    Arguments:
        data_service  — MarketDataService instance (from main.py global)
        agent         — TradingAgent instance (for agent.data.finnhub)
        force         — bypass cache and regenerate
    """
    from data.cache import cache
    from config import ANTHROPIC_API_KEY
    import anthropic

    CACHE_KEY = "notifai_weekly_summary_v2"

    if not force:
        cached = cache.get(CACHE_KEY)
        if cached is not None:
            return cached

    now = datetime.now()
    week_start = now - timedelta(days=now.weekday())
    week_label = f"Week of {week_start.strftime('%B %-d, %Y')}"
    monday = week_start.strftime("%Y-%m-%d")
    friday = (week_start + timedelta(days=4)).strftime("%Y-%m-%d")

    # ── News context ──────────────────────────────────────────────────────────
    news_text = ""
    try:
        if data_service and data_service.web_search:
            news_raw = await asyncio.wait_for(
                data_service.web_search.get_market_news(
                    topic="US stock market recap this week earnings macro Fed"),
                timeout=12.0,
            )
            articles = (
                news_raw.get("articles", [])
                if isinstance(news_raw, dict)
                else (news_raw if isinstance(news_raw, list) else [])
            )
            snippets = []
            for a in articles[:12]:
                headline = a.get("title", a.get("headline", ""))
                snippet = a.get("snippet", a.get("description", a.get("text", "")))[:220]
                if headline:
                    snippets.append(f"• {headline}: {snippet}")
            news_text = "\n".join(snippets)
    except Exception as e:
        print(f"[NOTIFAI_WEEKLY] News fetch error: {e}")

    # ── Earnings calendar ─────────────────────────────────────────────────────
    earnings_by_day: dict = {}
    try:
        raw = await asyncio.wait_for(
            asyncio.to_thread(
                agent.data.finnhub.client.earnings_calendar,
                _from=monday, to=friday, symbol=None,
            ),
            timeout=10.0,
        )
        for e in (raw.get("earningsCalendar") or []):
            sym = e.get("symbol", "")
            date = e.get("date", "")
            hour = e.get("hour", "")
            eps_est = e.get("epsEstimate")
            if sym and date:
                day_key = date
                earnings_by_day.setdefault(day_key, {"bmo": [], "amc": [], "other": []})
                bucket = "bmo" if hour == "bmo" else ("amc" if hour == "amc" else "other")
                earnings_by_day[day_key][bucket].append({
                    "ticker": sym,
                    "eps_estimate": eps_est,
                    "hour": hour,
                })
    except Exception as e:
        print(f"[NOTIFAI_WEEKLY] Earnings fetch error: {e}")

    # ── Economic calendar ─────────────────────────────────────────────────────
    economic_events: list = []
    try:
        if data_service and data_service.fmp:
            economic_events = await asyncio.wait_for(
                data_service.fmp.get_economic_calendar(from_date=monday, to_date=friday),
                timeout=10.0,
            )
            if not isinstance(economic_events, list):
                economic_events = []
    except Exception as e:
        print(f"[NOTIFAI_WEEKLY] Econ calendar error: {e}")

    # ── Claude call ───────────────────────────────────────────────────────────
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    summary_text = ""
    summary_days = []
    parsed: dict = {}

    if ANTHROPIC_API_KEY:
        prompt_ctx = f"Today is {now.strftime('%A, %B %-d, %Y')}. {week_label}.\n"
        if news_text:
            prompt_ctx += f"\nNews headlines from this week:\n{news_text}\n"
        else:
            prompt_ctx += "\n(No live news available — use your training knowledge for current market conditions.)\n"

        earnings_ctx = ""
        for i, day in enumerate(day_names):
            date_str = (week_start + timedelta(days=i)).strftime("%Y-%m-%d")
            day_earn = earnings_by_day.get(date_str, {})
            bmo = [x["ticker"] for x in day_earn.get("bmo", [])[:6]]
            amc = [x["ticker"] for x in day_earn.get("amc", [])[:6]]
            if bmo or amc:
                line = f"  {day}:"
                if bmo:
                    line += f" Pre-market: {', '.join(bmo)}."
                if amc:
                    line += f" After-hours: {', '.join(amc)}."
                earnings_ctx += line + "\n"

        econ_ctx = ""
        for ev in economic_events[:15]:
            ev_date = ev.get("date", ev.get("event_date", ""))
            ev_name = ev.get("event", ev.get("name", ""))
            ev_time = ev.get("time", ev.get("hour", ""))
            if ev_date and ev_name:
                econ_ctx += f"  {ev_date} {ev_time}: {ev_name}\n"

        system = (
            "You are Caelyn, a sharp AI trading analyst. Write market content in a confident, "
            "Bloomberg-meets-hedge-fund style. No fluff, no filler. Be specific with tickers, "
            "data points, and market dynamics. Write in flowing prose (not bullet lists) unless "
            "doing a day-by-day recap section.\n\n"
            "OUTPUT FORMAT — respond with a JSON object:\n"
            "{\n"
            '  "headline": "<1-sentence punchy market verdict for the week>",\n'
            '  "opening": "<2-3 sentence narrative of the week\'s dominant theme — risk-on/off driver, '
            'macro headline, sector rotation. Use specific tickers/data.>",\n'
            '  "sub_label": "Let\'s recap and prep you for the week ahead.",\n'
            '  "days": [\n'
            '    {"day": "Monday", "text": "<2-3 sentence market summary for that day>"},\n'
            '    ... (Tuesday through Friday)\n'
            "  ],\n"
            '  "closing_note": "<1 sentence on what to watch next week>",\n'
            '  "outlook_label": "<Bullish|Bearish|Mixed|Cautiously Bullish> — <2-5 word reason>"\n'
            "}\n"
            "Output ONLY valid JSON. No markdown wrapping."
        )

        user_msg = (
            f"{prompt_ctx}\n"
            f"Earnings this week:\n{earnings_ctx or '  (none of note)'}\n\n"
            f"Economic events:\n{econ_ctx or '  (none of note)'}\n\n"
            "Write the weekly recap + prep JSON now."
        )

        try:
            ai_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            resp = await asyncio.wait_for(
                asyncio.to_thread(
                    ai_client.messages.create,
                    model="claude-haiku-4-5-20251001",
                    max_tokens=1400,
                    system=system,
                    messages=[{"role": "user", "content": user_msg}],
                ),
                timeout=40.0,
            )
            raw_json = resp.content[0].text.strip() if resp.content else "{}"
            raw_json = _re.sub(r'^```(?:json)?\s*', '', raw_json, flags=_re.MULTILINE)
            raw_json = _re.sub(r'\s*```$', '', raw_json, flags=_re.MULTILINE)
            raw_json = raw_json.strip()
            parsed = _json.loads(raw_json)
            summary_text = parsed.get("opening", "")
            summary_days = parsed.get("days", [])
        except Exception as e:
            print(f"[NOTIFAI_WEEKLY] Claude error: {e}")
            summary_text = "Market summary temporarily unavailable. Please check back shortly."
            summary_days = []
    else:
        summary_text = "AI summary unavailable — API key not configured."

    result = {
        "week_label": week_label,
        "generated_at": now.isoformat(),
        "summary": summary_text,
        "days": summary_days,
        "closing_note": parsed.get("closing_note", ""),
        "outlook_label": parsed.get("outlook_label", ""),
        "headline": parsed.get("headline", ""),
        "sub_label": "Let's recap and prep you for the week ahead.",
        "earnings_by_day": earnings_by_day,
        "economic_events": economic_events[:20],
    }

    ttl = _weekly_cache_ttl()
    cache.set(CACHE_KEY, result, ttl)
    print(f"[NOTIFAI_WEEKLY] Generated — cached for {ttl // 3600}h {(ttl % 3600) // 60}m")
    return result


# ──────────────────────────────────────────────────────────────────────────────
# The Brief (no AI, cached 2 hours)
# ──────────────────────────────────────────────────────────────────────────────

async def fetch_the_brief(data_service: Any, agent: Any) -> dict:
    """
    Fetch earnings + economic calendar for the current week.
    No AI call. Cached 2 hours.

    Arguments:
        data_service  — MarketDataService instance (from main.py global)
        agent         — TradingAgent instance (for agent.data.finnhub)
    """
    from data.cache import cache

    CACHE_KEY = "notifai_the_brief_v1"
    cached = cache.get(CACHE_KEY)
    if cached is not None:
        return cached

    now = datetime.now()
    week_start = now - timedelta(days=now.weekday())
    monday = week_start.strftime("%Y-%m-%d")
    friday = (week_start + timedelta(days=4)).strftime("%Y-%m-%d")

    # ── Earnings ──────────────────────────────────────────────────────────────
    earnings_by_day: dict = {}
    try:
        raw = await asyncio.wait_for(
            asyncio.to_thread(
                agent.data.finnhub.client.earnings_calendar,
                _from=monday, to=friday, symbol=None,
            ),
            timeout=10.0,
        )
        for e in (raw.get("earningsCalendar") or []):
            sym = e.get("symbol", "")
            date = e.get("date", "")
            hour = e.get("hour", "")
            eps_est = e.get("epsEstimate")
            if sym and date:
                earnings_by_day.setdefault(date, {"bmo": [], "amc": [], "other": []})
                bucket = "bmo" if hour == "bmo" else ("amc" if hour == "amc" else "other")
                earnings_by_day[date][bucket].append({
                    "ticker": sym,
                    "eps_estimate": eps_est,
                    "hour": hour,
                })
    except Exception as e:
        print(f"[NOTIFAI_BRIEF] Earnings error: {e}")

    # ── Economic calendar ─────────────────────────────────────────────────────
    economic_events: list = []
    try:
        if data_service and data_service.fmp:
            economic_events = await asyncio.wait_for(
                data_service.fmp.get_economic_calendar(from_date=monday, to_date=friday),
                timeout=10.0,
            )
            if not isinstance(economic_events, list):
                economic_events = []
    except Exception as e:
        print(f"[NOTIFAI_BRIEF] Econ calendar error: {e}")

    result = {
        "week_start": monday,
        "week_end": friday,
        "earnings_by_day": earnings_by_day,
        "economic_events": economic_events[:30],
        "generated_at": now.isoformat(),
    }
    cache.set(CACHE_KEY, result, 7200)
    return result
