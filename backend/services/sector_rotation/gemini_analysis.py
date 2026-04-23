"""
Sectors AI analysis using Gemini with Google Search grounding.
Analysis is persisted to disk and survives page refresh indefinitely —
it is ONLY replaced when the user explicitly triggers a new generation.

Model routing (static-by-design):
  MODEL_GEMINI (resolved by model_policy.py) is the only viable choice here
  because this surface requires Google Search grounding, which is a Gemini-
  exclusive capability.  Do NOT route this to Claude/GPT/DeepSeek.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from agent.model_policy import MODEL_GEMINI

import httpx

from services.sector_rotation.schemas import (
    AIAnalysis,
    AILeadership,
    AIScenario,
    AISource,
    SectorSnapshot,
    RegimeSummary,
    SectorStock,
)

_CACHE_PATH = Path(__file__).parent.parent.parent / "data" / "sector_rotation_analysis.json"
_GENERATION_LOCK = asyncio.Lock()


def _gemini_key() -> str:
    return os.getenv("GEMINI_API_KEY", "")


def _load_disk_cache() -> Optional[dict]:
    """
    Load analysis from disk.
    NO TTL check — the analysis persists until the user manually regenerates it.
    Returns None only if the file doesn't exist or is unreadable.
    """
    if not _CACHE_PATH.exists():
        return None
    try:
        raw = json.loads(_CACHE_PATH.read_text())
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
    winning_etfs: list[str],
    sector_stocks_context: str,
) -> str:
    today = datetime.now().strftime("%B %d, %Y")

    top3 = [s.ticker for s in snapshots[:3]]
    bot3 = [s.ticker for s in snapshots[-3:]]

    sector_lines = "\n".join(
        f"  {s.ticker} ({s.name}): 1D={s.change_1d:+.2f}% 7D={s.change_7d:+.2f}% "
        f"30D={s.change_30d:+.1f}% YTD={s.change_ytd:+.1f}% "
        f"RotScore={s.rotation_score:.0f} Tag={s.regime_tag}"
        for s in snapshots
        if s.change_30d is not None and s.change_ytd is not None
        and s.rotation_score is not None
    )

    fed_rate  = macro.get("fed_rate", "N/A")
    cpi_yoy   = macro.get("cpi_yoy", "N/A")
    yield_2y  = macro.get("yield_2y", "N/A")
    yield_10y = macro.get("yield_10y", "N/A")
    spread    = macro.get("yield_curve_spread", "N/A")

    winning_str = ", ".join(winning_etfs) if winning_etfs else "undetermined"

    return f"""You are a senior sector analyst producing a live Sectors briefing for active investors.
Today is {today}.

WINNING SECTOR(S) — the focus of this analysis:
{winning_str}

SECTOR PERFORMANCE MATRIX (quantitative signals — cross-reference with live news):
Market posture: {regime.market_posture} | Leadership: {regime.leadership_style}
Cyclicals vs Defensives 30D spread: {regime.cyclical_vs_defensive:+.2f}%
Breadth (sectors beating SPY 30D): {regime.breadth_pct_above_spy:.0f}%

{sector_lines}

Current leaders (highest rotation score): {', '.join(top3)}
Current laggards (lowest rotation score): {', '.join(bot3)}

MACRO CONTEXT:
Fed Funds Rate: {fed_rate}%  |  CPI YoY: {cpi_yoy}%
2Y Treasury: {yield_2y}%  |  10Y Treasury: {yield_10y}%
Yield curve (10Y-2Y): {spread}%

STOCKS IN WINNING SECTOR(S):
{sector_stocks_context}

TASK:
Using Google Search, gather the most current sector news, earnings catalysts, and macro data.
Cross-reference with the quantitative data and stock universe above.

REQUIREMENTS:
1. Ground ALL claims in current events and real macro conditions (not generic commentary)
2. Explain WHY the winning sector(s) are leading given current macro/policy environment
3. Produce a concrete "top 10 stocks to watch right now" list from the stocks above — rank by immediacy of opportunity
4. For each top-10 stock: one sentence on WHY it is actionable NOW (specific catalyst, not generic)
5. Analyze at least 2 distinct macro/policy scenarios with concrete sector + stock implications
6. Be specific: name real catalysts (earnings dates, Fed decisions, policy events, product cycles)

OUTPUT FORMAT — return ONLY valid JSON:
{{
  "summary": "<2-3 sentence synthesis of what is happening in the winning sector(s) right now>",
  "market_regime": "<one of: Risk-On | Risk-Off | Neutral | Transitioning>",
  "macro_regime": "<one of: Inflationary | Disinflationary | Deflationary | Stagflationary | Goldilocks>",
  "leadership_style": "<one of: Cyclicals | Defensives | Mixed | Growth | Value>",
  "current_leadership": {{
    "leaders": ["XLK", "XLI"],
    "laggards": ["XLU", "XLP"],
    "explanation": "<why these sectors are leading/lagging given current conditions>"
  }},
  "outlook_1_4_weeks": "<concrete short-term view with specific catalysts for the winning sector>",
  "outlook_1_3_months": "<medium-term view with inflection points to watch>",
  "top_stocks_to_watch": [
    "<TICKER — one-sentence specific catalyst or reason to watch RIGHT NOW>",
    "<TICKER — ...>",
    "...up to 10 entries..."
  ],
  "winning_sector_etfs": {json.dumps(winning_etfs)},
  "scenarios": [
    {{
      "name": "<scenario name>",
      "timeframe": "<e.g., 1-8 weeks>",
      "probability": "<low | medium | high>",
      "sector_winners": ["XLE"],
      "sector_losers": ["XLY", "XLI"],
      "analysis": "<what drives this scenario and its sector/stock implications>"
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

        # top_stocks_to_watch — list of strings like "NVDA — reason"
        top_stocks = data.get("top_stocks_to_watch") or []
        if not isinstance(top_stocks, list):
            top_stocks = []

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
            top_stocks_to_watch=top_stocks,
            winning_sector_etfs=data.get("winning_sector_etfs", []),
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
    winning_etfs: list[str] | None = None,
) -> Optional[AIAnalysis]:
    """
    Return cached Sectors analysis or generate a new one.
    Thread-safe: only one generation runs at a time.

    When force=False, returns the disk-cached analysis regardless of age
    (it persists until the user manually regenerates).
    When force=True, always generates a new analysis and saves it.
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

    _winning_etfs = winning_etfs or [s.ticker for s in snapshots[:3]]

    # Build sector-stock context string for the prompt
    sector_stocks_context = _build_stock_context(_winning_etfs)

    async with _GENERATION_LOCK:
        if not force:
            cached = _load_disk_cache()
            if cached:
                try:
                    return AIAnalysis(**{k: v for k, v in cached.items() if not k.startswith("_")})
                except Exception:
                    pass

        print(f"[SR][Gemini] Generating Sectors analysis for winning ETFs: {_winning_etfs}")
        prompt = _build_prompt(snapshots, regime, macro, _winning_etfs, sector_stocks_context)

        body = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "tools": [{"google_search": {}}],
            "generationConfig": {"maxOutputTokens": 4096, "temperature": 0.3},
        }

        try:
            async with httpx.AsyncClient(timeout=90.0) as client:
                resp = await client.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL_GEMINI}:generateContent?key={key}",
                    headers={"Content-Type": "application/json"},
                    json=body,
                )
                resp.raise_for_status()
                data = resp.json()

            candidates = data.get("candidates", [])
            if not candidates:
                print("[SR][Gemini] No candidates in response")
                return None
            parts = candidates[0].get("content", {}).get("parts", [])
            raw_text = "".join(p.get("text", "") for p in parts if "text" in p)

            grounding = candidates[0].get("groundingMetadata", {})
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


def _build_stock_context(etfs: list[str]) -> str:
    """
    Build a compact text block describing the stocks in each winning sector.
    Used in the analysis prompt so Gemini can name specific stocks.
    """
    try:
        from services.sector_rotation.sector_stocks import _RAW
        lines = []
        for etf in etfs:
            raw = _RAW.get(etf, {})
            if not raw:
                continue
            sector_name = raw.get("sector_name", etf)
            lines.append(f"\n{etf} ({sector_name}):")
            for label, role_key in [
                ("Momentum leaders",    "momentum_leaders"),
                ("Bottleneck enablers", "bottleneck_enablers"),
                ("Anchor giants",       "anchor_giants"),
            ]:
                entries = raw.get(role_key, [])
                tickers = [f"{t} ({n})" for t, n, _ in entries]
                if tickers:
                    lines.append(f"  {label}: {', '.join(tickers)}")
        return "\n".join(lines) if lines else "No sector stock data available."
    except Exception as e:
        print(f"[SR][Gemini] Stock context error: {e}")
        return "No sector stock data available."


def load_cached_analysis() -> Optional[AIAnalysis]:
    """Load analysis from disk cache regardless of age (for stale fallback / page load)."""
    if not _CACHE_PATH.exists():
        return None
    try:
        raw = json.loads(_CACHE_PATH.read_text())
        return AIAnalysis(**{k: v for k, v in raw.items() if not k.startswith("_")})
    except Exception:
        return None
