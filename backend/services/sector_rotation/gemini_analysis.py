"""
Weekly sector rotation AI analysis using Gemini with Google Search grounding.
Analysis is persisted to disk and regenerated at most once per 7 days.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx

from services.sector_rotation.schemas import (
    AIAnalysis,
    AILeadership,
    AIScenario,
    AISource,
    SectorSnapshot,
    RegimeSummary,
)

_CACHE_PATH = Path(__file__).parent.parent.parent / "data" / "sector_rotation_analysis.json"
_CACHE_TTL_SECONDS = 7 * 24 * 3600
_GENERATION_LOCK = asyncio.Lock()


def _gemini_key() -> str:
    return os.getenv("GEMINI_API_KEY", "")


def _load_disk_cache() -> Optional[dict]:
    if not _CACHE_PATH.exists():
        return None
    try:
        raw = json.loads(_CACHE_PATH.read_text())
        generated_at = raw.get("_saved_at", 0)
        if time.time() - generated_at < _CACHE_TTL_SECONDS:
            return raw
    except Exception as e:
        print(f"[SR][Gemini] Cache read error: {e}")
    return None


def _save_disk_cache(data: dict) -> None:
    try:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        data["_saved_at"] = time.time()
        _CACHE_PATH.write_text(json.dumps(data, indent=2))
    except Exception as e:
        print(f"[SR][Gemini] Cache write error: {e}")


def _build_prompt(
    snapshots: list[SectorSnapshot],
    regime: RegimeSummary,
    macro: dict,
) -> str:
    today = datetime.now().strftime("%B %d, %Y")

    top3 = [s.ticker for s in snapshots[:3]]
    bot3 = [s.ticker for s in snapshots[-3:]]

    sector_lines = "\n".join(
        f"  {s.ticker} ({s.name}): 30D={s.change_30d:+.1f}% YTD={s.change_ytd:+.1f}% "
        f"vs50MA={s.pct_from_50d:+.1f}% vs200MA={s.pct_from_200d:+.1f}% "
        f"RotScore={s.rotation_score:.0f} Tag={s.regime_tag}"
        for s in snapshots
        if s.change_30d is not None and s.change_ytd is not None
        and s.pct_from_50d is not None and s.pct_from_200d is not None
        and s.rotation_score is not None
    )

    fed_rate  = macro.get("fed_rate", "N/A")
    cpi_yoy   = macro.get("cpi_yoy", "N/A")
    yield_2y  = macro.get("yield_2y", "N/A")
    yield_10y = macro.get("yield_10y", "N/A")
    spread    = macro.get("yield_curve_spread", "N/A")

    return f"""You are a senior macro strategist producing a weekly sector rotation briefing for professional investors.
Today is {today}.

SECTOR ROTATION DATA (quantitative signals only — do NOT just describe these numbers, USE them as context):
Market posture: {regime.market_posture} | Leadership: {regime.leadership_style}
Cyclicals vs Defensives 30D spread: {regime.cyclical_vs_defensive:+.2f}%
Breadth (sectors beating SPY 30D): {regime.breadth_pct_above_spy:.0f}%

Sector performance matrix:
{sector_lines}

Current leaders (highest rotation score): {', '.join(top3)}
Current laggards (lowest rotation score): {', '.join(bot3)}

MACRO CONTEXT:
Fed Funds Rate: {fed_rate}%
CPI YoY: {cpi_yoy}%
2Y Treasury: {yield_2y}%
10Y Treasury: {yield_10y}%
Yield curve (10Y-2Y): {spread}%

TASK:
Using Google Search, gather the most current macro, geopolitical, and sector news.
Cross-reference that with the quantitative sector data above.
Then produce a structured sector rotation analysis.

REQUIREMENTS:
1. Ground ALL claims in current events and real macro conditions (not generic commentary)
2. Explain WHY specific sectors are leading or lagging given current macro/policy environment
3. Identify the most actionable rotation trades for the next 1-4 weeks AND 1-3 months
4. Analyze at least 2 distinct macro/policy scenarios with concrete sector implications
5. Be explicit about uncertainty — acknowledge where the picture is mixed
6. Tie sector dynamics back to real current events (Fed decisions, earnings, geopolitical moves, trade policy)

OUTPUT FORMAT — return ONLY valid JSON matching this schema exactly:
{{
  "summary": "<2-3 sentence synthesis of current sector rotation dynamics>",
  "market_regime": "<one of: Risk-On | Risk-Off | Neutral | Transitioning>",
  "macro_regime": "<one of: Inflationary | Disinflationary | Deflationary | Stagflationary | Goldilocks>",
  "leadership_style": "<one of: Cyclicals | Defensives | Mixed | Growth | Value>",
  "current_leadership": {{
    "leaders": ["XLK", "XLI"],
    "laggards": ["XLU", "XLP"],
    "explanation": "<why these sectors are leading/lagging given current macro>"
  }},
  "outlook_1_4_weeks": "<concrete short-term sector view with specific catalysts>",
  "outlook_1_3_months": "<medium-term view with macro inflection points to watch>",
  "scenarios": [
    {{
      "name": "<scenario name>",
      "timeframe": "<e.g., 1-8 weeks>",
      "probability": "<low | medium | high>",
      "sector_winners": ["XLE"],
      "sector_losers": ["XLY", "XLI"],
      "analysis": "<what drives this scenario and its sector implications>"
    }}
  ],
  "watch_items": ["<specific macro event or risk to monitor>"],
  "sources": [
    {{
      "title": "<article or source title>",
      "url": "<url>",
      "publisher": "<publisher name>"
    }}
  ],
  "generated_at": "{today}"
}}

Return ONLY the JSON object — no markdown fences, no explanation outside the JSON."""


def _parse_ai_json(raw: str) -> Optional[AIAnalysis]:
    try:
        cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip())
        cleaned = re.sub(r"\s*```$", "", cleaned)
        data = json.loads(cleaned)

        leadership_raw = data.get("current_leadership", {})
        leadership = AILeadership(
            leaders=leadership_raw.get("leaders", []),
            laggards=leadership_raw.get("laggards", []),
            explanation=leadership_raw.get("explanation", ""),
        )

        scenarios = [
            AIScenario(
                name=s.get("name", ""),
                timeframe=s.get("timeframe", ""),
                probability=s.get("probability", "medium"),
                sector_winners=s.get("sector_winners", []),
                sector_losers=s.get("sector_losers", []),
                analysis=s.get("analysis", ""),
            )
            for s in (data.get("scenarios") or [])
        ]

        sources = [
            AISource(
                title=src.get("title", ""),
                url=src.get("url", ""),
                publisher=src.get("publisher", ""),
            )
            for src in (data.get("sources") or [])
        ]

        return AIAnalysis(
            summary=data.get("summary", ""),
            market_regime=data.get("market_regime", ""),
            macro_regime=data.get("macro_regime", ""),
            leadership_style=data.get("leadership_style", ""),
            current_leadership=leadership,
            outlook_1_4_weeks=data.get("outlook_1_4_weeks", ""),
            outlook_1_3_months=data.get("outlook_1_3_months", ""),
            scenarios=scenarios,
            watch_items=data.get("watch_items", []),
            sources=sources,
            generated_at=data.get("generated_at", datetime.now().strftime("%B %d, %Y")),
        )
    except Exception as e:
        print(f"[SR][Gemini] JSON parse error: {e}\nRaw (first 500 chars): {raw[:500]}")
        return None


async def get_or_generate_analysis(
    snapshots: list[SectorSnapshot],
    regime: RegimeSummary,
    macro: dict,
    force: bool = False,
) -> Optional[AIAnalysis]:
    """
    Return cached weekly AI analysis or generate a new one.
    Thread-safe: only one generation runs at a time.
    """
    if not force:
        cached = _load_disk_cache()
        if cached:
            try:
                return AIAnalysis(**{k: v for k, v in cached.items() if not k.startswith("_")})
            except Exception:
                pass

    key = _gemini_key()
    if not key:
        print("[SR][Gemini] No GEMINI_API_KEY — skipping AI analysis")
        return None

    async with _GENERATION_LOCK:
        if not force:
            cached = _load_disk_cache()
            if cached:
                try:
                    return AIAnalysis(**{k: v for k, v in cached.items() if not k.startswith("_")})
                except Exception:
                    pass

        print("[SR][Gemini] Generating weekly sector rotation analysis...")
        prompt = _build_prompt(snapshots, regime, macro)

        body = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "tools": [{"google_search": {}}],
            "generationConfig": {"maxOutputTokens": 4096, "temperature": 0.3},
        }

        try:
            async with httpx.AsyncClient(timeout=90.0) as client:
                resp = await client.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent?key={key}",
                    headers={"Content-Type": "application/json"},
                    json=body,
                )
                resp.raise_for_status()
                data = resp.json()

            parts = data["candidates"][0]["content"]["parts"]
            raw_text = "".join(p.get("text", "") for p in parts if "text" in p)

            grounding = data.get("candidates", [{}])[0].get("groundingMetadata", {})
            queries = grounding.get("webSearchQueries", [])
            print(f"[SR][Gemini] Analysis generated — {len(raw_text):,} chars, {len(queries)} search queries")

            analysis = _parse_ai_json(raw_text)
            if analysis:
                _save_disk_cache(analysis.model_dump())
                return analysis

        except httpx.HTTPStatusError as e:
            print(f"[SR][Gemini] HTTP error {e.response.status_code}: {e.response.text[:400]}")
        except Exception as e:
            import traceback
            print(f"[SR][Gemini] Generation error: {e}")
            traceback.print_exc()

    return None


def load_cached_analysis() -> Optional[AIAnalysis]:
    """Load analysis from disk cache regardless of TTL (for stale fallback)."""
    if not _CACHE_PATH.exists():
        return None
    try:
        raw = json.loads(_CACHE_PATH.read_text())
        return AIAnalysis(**{k: v for k, v in raw.items() if not k.startswith("_")})
    except Exception:
        return None
