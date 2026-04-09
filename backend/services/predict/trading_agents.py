"""
Multi-Agent Predict Service — TauricResearch/TradingAgents architecture.

Implements a full LLM trading firm where each agent has a distinct role,
adapted for Polymarket prediction markets:

  FundamentalsAgent  → macro/fundamental data relevant to the question
  SentimentAgent     → public mood, news sentiment, social signals
  TechnicalAgent     → price/odds history, trend, market microstructure
  BullAgent          → argues FOR YES with highest-quality evidence
  BearAgent          → argues FOR NO with strongest counter-evidence
  RiskManagerAgent   → synthesizes the debate → final recommendation

Agents run in parallel where possible:
  Phase 1: [Fundamentals, Sentiment, Technical] in parallel
  Phase 2: [Bull, Bear] in parallel (receive Phase 1 outputs)
  Phase 3: [RiskManager] (receives all prior outputs) → final decision

All agents use gemini-3-flash-preview with Google Search grounding.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

from data.cache import cache

_GEMINI_MODEL = "gemini-3-flash-preview"
_GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta/models"
_AGENT_TIMEOUT = 45.0
_ANALYSIS_CACHE_TTL = 300


def _key() -> str:
    return os.getenv("GEMINI_API_KEY", "")


def _gemini_url() -> str:
    return f"{_GEMINI_BASE}/{_GEMINI_MODEL}:generateContent?key={_key()}"


async def _call_gemini(
    system_prompt: str,
    user_content: str,
    temperature: float = 0.3,
    use_search: bool = True,
    max_tokens: int = 2048,
) -> str:
    """
    Direct REST call to Gemini — same pattern as sector_rotation/gemini_analysis.py.
    Returns extracted text or empty string on failure.
    """
    if not _key():
        return "[No GEMINI_API_KEY configured]"

    body: dict = {
        "contents": [
            {"role": "user", "parts": [{"text": f"{system_prompt}\n\n{user_content}"}]},
        ],
        "generationConfig": {
            "maxOutputTokens": max_tokens,
            "temperature": temperature,
        },
    }
    if use_search:
        body["tools"] = [{"google_search": {}}]

    try:
        async with httpx.AsyncClient(timeout=_AGENT_TIMEOUT) as client:
            resp = await client.post(
                _gemini_url(),
                headers={"Content-Type": "application/json"},
                json=body,
            )
            resp.raise_for_status()
            data = resp.json()

        candidates = data.get("candidates", [])
        if not candidates:
            print("[TRADING_AGENTS] Gemini returned no candidates")
            return ""
        parts = candidates[0].get("content", {}).get("parts", [])
        return "".join(p.get("text", "") for p in parts if "text" in p)
    except Exception as e:
        print(f"[TRADING_AGENTS] Gemini call error: {type(e).__name__}: {e}")
        return ""


# ── Individual Agents ─────────────────────────────────────────────────────────

async def fundamentals_agent(question: str, market_context: dict) -> dict:
    """
    Fundamentals analyst: pulls macro/economic data relevant to the prediction.
    Equivalent to TauricResearch's FundamentalsAnalyst role.
    """
    system = (
        "You are a senior macroeconomic fundamentals analyst at a quantitative trading firm. "
        "You specialize in identifying the fundamental drivers that determine binary outcomes. "
        "Your analysis is data-driven, concise, and directly actionable. "
        "You do NOT give investment advice — you analyze probabilities."
    )

    relevant_markets = market_context.get("relevant_markets", [])
    mkt_summary = ""
    if relevant_markets:
        top = relevant_markets[0]
        mkt_summary = (
            f"\nMost relevant Polymarket market: '{top.get('question')}'\n"
            f"Current YES probability: {top.get('yes_pct', 'N/A')}% | "
            f"Volume 24h: ${top.get('volume_24h', 0):,.0f} | "
            f"Liquidity: ${top.get('liquidity', 0):,.0f}"
        )

    user = f"""Prediction market question: "{question}"
{mkt_summary}

Task: Provide a FUNDAMENTALS ANALYSIS that informs the probability of this event resolving YES.

Use Google Search to find current data. Analyze:
1. Key macroeconomic indicators relevant to this question (GDP, inflation, employment, Fed policy if applicable)
2. Historical base rates — how often do similar events resolve YES?
3. Structural factors that make YES more or less likely
4. Any recent data releases or announcements that shift the probability

Output as JSON:
{{
  "agent": "fundamentals",
  "base_rate_estimate": <float 0-1, your fundamental probability estimate>,
  "key_factors_bullish": ["<specific fundamental supporting YES>"],
  "key_factors_bearish": ["<specific fundamental against YES>"],
  "macro_regime": "<current macro environment description>",
  "data_points": ["<specific number/stat>"],
  "confidence": "<low|medium|high>",
  "summary": "<2-3 sentence fundamental thesis>"
}}

Return ONLY the JSON object."""

    raw = await _call_gemini(system, user, temperature=0.2)
    return _parse_agent_json(raw, "fundamentals", question)


async def sentiment_agent(question: str, market_context: dict) -> dict:
    """
    Sentiment analyst: scans public mood, news sentiment, social signals.
    Equivalent to TauricResearch's SentimentAnalyst role.
    """
    system = (
        "You are a market sentiment analyst specializing in prediction markets and event-driven trading. "
        "You track narrative momentum, media coverage bias, and crowd wisdom signals. "
        "You identify when markets are overreacting or underreacting to sentiment. "
        "Be specific about what you find — no vague statements."
    )

    signals_summary = ""
    market_signals = market_context.get("market_signals", {})
    if market_signals.get("summary"):
        s = market_signals["summary"]
        signals_summary = (
            f"\nBroader Polymarket context: {s.get('market_count', 0)} active markets, "
            f"${s.get('total_volume_24h', 0):,.0f} 24h volume, "
            f"{s.get('surging_count', 0)} surging markets, "
            f"{s.get('whale_active_count', 0)} whale-active markets"
        )

    user = f"""Prediction market question: "{question}"
{signals_summary}

Task: Provide a SENTIMENT ANALYSIS that informs the probability of this event resolving YES.

Use Google Search to find current sentiment signals. Analyze:
1. Current media/news sentiment (positive/negative/neutral towards YES outcome)
2. Social media narrative — is the crowd expecting YES or NO?
3. Expert consensus (analysts, forecasters, official statements)
4. Any recent sentiment shifts or surprises
5. Polymarket crowd wisdom — is the current market probability too high/low vs sentiment?

Output as JSON:
{{
  "agent": "sentiment",
  "sentiment_probability": <float 0-1, your sentiment-implied probability>,
  "media_sentiment": "<bullish|bearish|neutral> for YES outcome",
  "crowd_wisdom": "<what the crowd believes>",
  "contrarian_signal": "<any contrarian indicator>",
  "recent_sentiment_shift": "<description of any recent change>",
  "key_narratives": ["<dominant narrative 1>", "<dominant narrative 2>"],
  "confidence": "<low|medium|high>",
  "summary": "<2-3 sentence sentiment thesis>"
}}

Return ONLY the JSON object."""

    raw = await _call_gemini(system, user, temperature=0.3)
    return _parse_agent_json(raw, "sentiment", question)


async def technical_agent(question: str, market_context: dict) -> dict:
    """
    Technical/market-microstructure analyst: price trends, market mechanics.
    Equivalent to TauricResearch's TechnicalAnalyst role — adapted for prediction markets.
    """
    system = (
        "You are a prediction market microstructure analyst. "
        "You analyze probability trends, market efficiency, liquidity signals, "
        "and the mechanical indicators of prediction market pricing. "
        "You think in terms of market efficiency, price discovery, and smart-money signals."
    )

    relevant = market_context.get("relevant_markets", [])
    tech_context = ""
    if relevant:
        m = relevant[0]
        tech_context = f"""
Relevant Polymarket data:
  YES price: {m.get('yes_pct', 'N/A')}% | NO price: {m.get('no_pct', 'N/A')}%
  Spread: {m.get('spread_pct', 'N/A')}% | Market efficiency score: {m.get('market_efficiency_score', 'N/A')}/100
  Volume 24h: ${m.get('volume_24h', 0):,.0f} | Liquidity: ${m.get('liquidity', 0):,.0f}
  Volume momentum: {m.get('volume_momentum', 'N/A')}
  Whale activity: {m.get('whale_activity', False)}
  Edge detected: {m.get('edge_detected', False)} ({m.get('edge_pct', 0)}% implied house edge)
  Competitive market: {m.get('is_competitive', False)}
  Price momentum: {m.get('price_momentum_pct', 0):+.2f}%
  Days to expiry: {m.get('days_to_expiry', 'N/A')}
  Kelly fraction: {m.get('kelly_fraction_pct', 0):.2f}%"""

    user = f"""Prediction market question: "{question}"
{tech_context}

Task: Provide a MARKET MICROSTRUCTURE ANALYSIS for this prediction market.

Analyze:
1. Is the current market price (YES probability) technically justified?
2. Volume momentum — is smart money accumulating or distributing?
3. Market efficiency — is there detectable edge or mispricing?
4. Liquidity analysis — can a meaningful position be taken without moving the market?
5. Time-to-expiry dynamics — does the remaining time favor YES or NO?

Output as JSON:
{{
  "agent": "technical",
  "technical_probability": <float 0-1, your microstructure-implied probability>,
  "trend": "<upward|downward|flat> probability trend for YES",
  "smart_money_signal": "<accumulating|distributing|neutral>",
  "edge_assessment": "<description of any detected edge or mispricing>",
  "liquidity_assessment": "<adequate|thin|excellent>",
  "time_dynamics": "<how time-to-expiry affects positioning>",
  "key_levels": {{"support": <float>, "resistance": <float>}},
  "confidence": "<low|medium|high>",
  "summary": "<2-3 sentence technical thesis>"
}}

Return ONLY the JSON object."""

    raw = await _call_gemini(system, user, temperature=0.2, use_search=False)
    return _parse_agent_json(raw, "technical", question)


async def bull_agent(question: str, phase1_outputs: dict) -> dict:
    """
    Bull agent: constructs the strongest possible case for YES resolution.
    Equivalent to TauricResearch's BullResearcher role.
    """
    system = (
        "You are a bull-side prediction market analyst. "
        "Your ONLY job is to build the STRONGEST POSSIBLE case for YES resolution. "
        "You must use ALL available evidence — fundamental, sentiment, technical — "
        "and identify the most compelling arguments. "
        "You are NOT balanced. You are an advocate for YES."
    )

    context = _format_phase1_for_debate(phase1_outputs)

    user = f"""Prediction market question: "{question}"

Phase 1 Research Summary:
{context}

Task: Build the STRONGEST CASE for YES resolution.

Draw on all three research reports above. Find the most compelling evidence for YES.
Use Google Search to find any additional supporting data not in the research above.

Output as JSON:
{{
  "agent": "bull",
  "yes_probability_estimate": <float 0-1, your bull estimate>,
  "conviction": "<low|medium|high>",
  "primary_argument": "<the single strongest reason for YES>",
  "supporting_evidence": [
    "<specific data point or event supporting YES>",
    "<another specific supporting fact>"
  ],
  "key_catalyst": "<the most important near-term catalyst that drives YES>",
  "risk_to_thesis": "<the one thing that could make YES fail>",
  "sizing_recommendation": "<aggressive|moderate|small> position for YES",
  "summary": "<3-4 sentence bull thesis>"
}}

Return ONLY the JSON object."""

    raw = await _call_gemini(system, user, temperature=0.4)
    return _parse_agent_json(raw, "bull", question)


async def bear_agent(question: str, phase1_outputs: dict) -> dict:
    """
    Bear agent: constructs the strongest possible case for NO resolution.
    Equivalent to TauricResearch's BearResearcher role.
    """
    system = (
        "You are a bear-side prediction market analyst. "
        "Your ONLY job is to build the STRONGEST POSSIBLE case for NO resolution. "
        "You must use ALL available evidence — fundamental, sentiment, technical — "
        "and identify every reason the event will NOT happen. "
        "You are NOT balanced. You are an advocate for NO."
    )

    context = _format_phase1_for_debate(phase1_outputs)

    user = f"""Prediction market question: "{question}"

Phase 1 Research Summary:
{context}

Task: Build the STRONGEST CASE for NO resolution (event does NOT happen).

Find every reason to be skeptical of YES. Challenge every bull argument.
Use Google Search to find counter-evidence and reasons for NO.

Output as JSON:
{{
  "agent": "bear",
  "no_probability_estimate": <float 0-1, your bear estimate for NO>,
  "yes_probability_implied": <float 0-1, implied YES = 1 - no_estimate>,
  "conviction": "<low|medium|high>",
  "primary_argument": "<the single strongest reason for NO>",
  "supporting_evidence": [
    "<specific data point or event supporting NO>",
    "<another specific counter-fact>"
  ],
  "key_risk": "<the scenario that would make YES happen despite bear case>",
  "historical_precedent": "<similar events that resolved NO>",
  "sizing_recommendation": "<aggressive|moderate|small> position for NO",
  "summary": "<3-4 sentence bear thesis>"
}}

Return ONLY the JSON object."""

    raw = await _call_gemini(system, user, temperature=0.4)
    return _parse_agent_json(raw, "bear", question)


async def risk_manager_agent(
    question: str,
    market_context: dict,
    phase1_outputs: dict,
    phase2_outputs: dict,
) -> dict:
    """
    Risk manager: the final decision-maker.
    Hears both sides, weighs all evidence, issues the final recommendation.
    Equivalent to TauricResearch's RiskManager role.
    """
    system = (
        "You are the Chief Risk Officer at a quantitative prediction market trading firm. "
        "You have just received research from 5 analysts: "
        "a fundamentals analyst, a sentiment analyst, a technical analyst, "
        "a bull advocate, and a bear advocate. "
        "Your job is to synthesize ALL of their work and issue a FINAL DECISION. "
        "You are decisive. You size positions. You give clear recommendations. "
        "You weight evidence by quality and conviction."
    )

    bull = phase2_outputs.get("bull", {})
    bear = phase2_outputs.get("bear", {})
    fundamentals = phase1_outputs.get("fundamentals", {})
    sentiment = phase1_outputs.get("sentiment", {})
    technical = phase1_outputs.get("technical", {})

    relevant = market_context.get("relevant_markets", [])
    market_price_pct = relevant[0].get("yes_pct", 50) if relevant else 50

    context = f"""FUNDAMENTAL ANALYST:
  Base rate estimate: {fundamentals.get('base_rate_estimate', 'N/A')}
  Summary: {fundamentals.get('summary', 'N/A')}
  Confidence: {fundamentals.get('confidence', 'N/A')}

SENTIMENT ANALYST:
  Sentiment probability: {sentiment.get('sentiment_probability', 'N/A')}
  Media sentiment: {sentiment.get('media_sentiment', 'N/A')}
  Summary: {sentiment.get('summary', 'N/A')}

TECHNICAL ANALYST:
  Technical probability: {technical.get('technical_probability', 'N/A')}
  Smart money signal: {technical.get('smart_money_signal', 'N/A')}
  Summary: {technical.get('summary', 'N/A')}

BULL ADVOCATE:
  YES probability estimate: {bull.get('yes_probability_estimate', 'N/A')}
  Primary argument: {bull.get('primary_argument', 'N/A')}
  Key catalyst: {bull.get('key_catalyst', 'N/A')}
  Summary: {bull.get('summary', 'N/A')}

BEAR ADVOCATE:
  NO probability estimate: {bear.get('no_probability_estimate', 'N/A')}
  Implied YES: {bear.get('yes_probability_implied', 'N/A')}
  Primary argument: {bear.get('primary_argument', 'N/A')}
  Summary: {bear.get('summary', 'N/A')}

CURRENT MARKET:
  Polymarket YES price: {market_price_pct}%"""

    user = f"""Question: "{question}"

{context}

As Risk Manager, issue your FINAL DECISION.

Synthesize all five analyst reports. Weigh the evidence. 
Compare your fair value estimate vs the current market price to find edge.
If the market is at 65% YES but your synthesis says 45%, that's a NO edge.
If your synthesis says 80% and market is at 65%, that's a YES edge.

Output as JSON:
{{
  "agent": "risk_manager",
  "final_yes_probability": <float 0-1, your synthesized fair value>,
  "market_yes_price": {market_price_pct / 100:.2f},
  "edge_pct": <float, your_probability - market_price (positive = YES edge, negative = NO edge)>,
  "recommendation": "<LONG_YES|LONG_NO|PASS>",
  "conviction": "<low|medium|high|very_high>",
  "position_sizing": "<e.g. 2-3% of risk capital, Kelly quarter-fraction>",
  "thesis": "<4-6 sentence final synthesis — the story of why this resolves YES or NO>",
  "bull_points_adopted": ["<bull arguments you found compelling>"],
  "bear_points_adopted": ["<bear arguments you found compelling>"],
  "key_risk": "<the single scenario that invalidates your recommendation>",
  "entry_note": "<when/how to enter the position>",
  "exit_note": "<when to close the position>",
  "debate_winner": "<bull|bear|draw>",
  "consensus_probability": <float, simple average of the 5 agent estimates>
}}

Return ONLY the JSON object."""

    raw = await _call_gemini(system, user, temperature=0.2, use_search=False, max_tokens=3000)
    return _parse_agent_json(raw, "risk_manager", question)


# ── Orchestrator ──────────────────────────────────────────────────────────────

async def run_predict_analysis(question: str, market_context: dict) -> dict:
    """
    Full TradingAgents pipeline for a prediction market question.

    Phase 1 → Fundamentals, Sentiment, Technical (parallel)
    Phase 2 → Bull, Bear (parallel, with Phase 1 context)
    Phase 3 → Risk Manager (sequential, with all prior context)

    Returns a complete structured analysis ready for the Predict page.
    """
    cache_key = f"predict:analysis:{hash(question.lower().strip())}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    start = time.time()

    print(f"[TRADING_AGENTS] Starting analysis: '{question[:80]}'")

    phase1_funds, phase1_sent, phase1_tech = await asyncio.gather(
        fundamentals_agent(question, market_context),
        sentiment_agent(question, market_context),
        technical_agent(question, market_context),
        return_exceptions=True,
    )

    phase1 = {
        "fundamentals": phase1_funds if isinstance(phase1_funds, dict) else {"agent": "fundamentals", "error": str(phase1_funds)},
        "sentiment": phase1_sent if isinstance(phase1_sent, dict) else {"agent": "sentiment", "error": str(phase1_sent)},
        "technical": phase1_tech if isinstance(phase1_tech, dict) else {"agent": "technical", "error": str(phase1_tech)},
    }
    print(f"[TRADING_AGENTS] Phase 1 complete ({time.time()-start:.1f}s)")

    phase2_bull, phase2_bear = await asyncio.gather(
        bull_agent(question, phase1),
        bear_agent(question, phase1),
        return_exceptions=True,
    )

    phase2 = {
        "bull": phase2_bull if isinstance(phase2_bull, dict) else {"agent": "bull", "error": str(phase2_bull)},
        "bear": phase2_bear if isinstance(phase2_bear, dict) else {"agent": "bear", "error": str(phase2_bear)},
    }
    print(f"[TRADING_AGENTS] Phase 2 complete ({time.time()-start:.1f}s)")

    risk_result = await risk_manager_agent(question, market_context, phase1, phase2)
    if isinstance(risk_result, Exception):
        risk_result = {"agent": "risk_manager", "error": str(risk_result)}
    print(f"[TRADING_AGENTS] Phase 3 complete ({time.time()-start:.1f}s)")

    relevant_markets = market_context.get("relevant_markets", [])
    result = {
        "question": question,
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.time() - start, 1),
        "relevant_markets": relevant_markets[:3],
        "agents": {
            "fundamentals": phase1["fundamentals"],
            "sentiment": phase1["sentiment"],
            "technical": phase1["technical"],
            "bull": phase2["bull"],
            "bear": phase2["bear"],
            "risk_manager": risk_result,
        },
        "final": _extract_final(risk_result, phase1, phase2, relevant_markets),
    }

    cache.set(cache_key, result, _ANALYSIS_CACHE_TTL)
    return result


def _extract_final(risk: dict, phase1: dict, phase2: dict, markets: list) -> dict:
    """Convenience summary for the frontend — top-level card data."""
    yes_prob = risk.get("final_yes_probability", 0.5)
    recommendation = risk.get("recommendation", "PASS")
    edge_pct = risk.get("edge_pct", 0)

    estimates = []
    for agent_key in ("fundamentals", "sentiment", "technical"):
        a = phase1.get(agent_key, {})
        for prob_key in ("base_rate_estimate", "sentiment_probability", "technical_probability"):
            v = a.get(prob_key)
            if isinstance(v, (int, float)):
                estimates.append(v)

    bull_est = phase2.get("bull", {}).get("yes_probability_estimate")
    bear_yes = phase2.get("bear", {}).get("yes_probability_implied")
    if isinstance(bull_est, (int, float)):
        estimates.append(bull_est)
    if isinstance(bear_yes, (int, float)):
        estimates.append(bear_yes)

    consensus = sum(estimates) / len(estimates) if estimates else yes_prob

    market_price_pct = markets[0].get("yes_pct", 50) if markets else 50

    return {
        "recommendation": recommendation,
        "final_yes_probability_pct": round(yes_prob * 100, 1),
        "consensus_probability_pct": round(consensus * 100, 1),
        "market_price_pct": market_price_pct,
        "edge_pct": round(edge_pct * 100, 2),
        "edge_direction": "YES" if edge_pct > 0.02 else "NO" if edge_pct < -0.02 else "NEUTRAL",
        "conviction": risk.get("conviction", "medium"),
        "debate_winner": risk.get("debate_winner", "draw"),
        "thesis": risk.get("thesis", ""),
        "key_risk": risk.get("key_risk", ""),
        "position_sizing": risk.get("position_sizing", ""),
        "entry_note": risk.get("entry_note", ""),
        "exit_note": risk.get("exit_note", ""),
    }


def _format_phase1_for_debate(outputs: dict) -> str:
    lines = []
    for key, data in outputs.items():
        if not isinstance(data, dict):
            continue
        summary = data.get("summary", "No summary")
        prob_keys = ["base_rate_estimate", "sentiment_probability", "technical_probability"]
        prob = next((data.get(k) for k in prob_keys if data.get(k) is not None), None)
        confidence = data.get("confidence", "unknown")
        label = key.replace("_", " ").title()
        prob_str = f"{round(prob * 100, 1)}%" if isinstance(prob, (int, float)) else "N/A"
        lines.append(f"{label} (prob={prob_str}, confidence={confidence}): {summary}")
    return "\n".join(lines) if lines else "No Phase 1 research available."


def _parse_agent_json(raw: str, agent_name: str, question: str) -> dict:
    """Extract and parse JSON from agent output, with graceful fallback."""
    if not raw:
        return {"agent": agent_name, "error": "empty response", "question": question}
    try:
        cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip())
        cleaned = re.sub(r"\s*```$", "", cleaned)
        cleaned = cleaned.strip()
        json_match = re.search(r"\{[\s\S]*\}", cleaned)
        if json_match:
            cleaned = json_match.group(0)
        data = json.loads(cleaned)
        data.setdefault("agent", agent_name)
        return data
    except Exception as e:
        print(f"[TRADING_AGENTS] JSON parse error for {agent_name}: {e} | raw: {raw[:200]}")
        return {
            "agent": agent_name,
            "error": f"parse_error: {e}",
            "raw_snippet": raw[:300],
            "question": question,
        }
