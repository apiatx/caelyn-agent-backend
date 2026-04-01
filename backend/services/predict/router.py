"""
Predict page API router — Jon-Becker + TauricResearch integration.

New endpoints:

  GET  /api/predict/markets          → Enhanced Polymarket market list with signals
  GET  /api/predict/market/{id}      → Deep analysis of a single market
  GET  /api/predict/signals          → Dashboard signals (edges, mispricings, whale watch)
  GET  /api/predict/whale-watch      → Markets with anomalous volume spikes
  GET  /api/predict/categories       → Volume/count breakdown by tag
  GET  /api/predict/context          → Relevant markets for a question (pre-analyze)
  POST /api/predict/analyze          → Full 6-agent TradingAgents analysis
  GET  /api/polymarket/intelligence  → Market intelligence overview (alias for signals)
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from services.predict.polymarket_intelligence import polymarket_intel
from services.predict.trading_agents import run_predict_analysis

router = APIRouter(tags=["predict"])


@router.get("/api/predict/markets")
async def predict_markets(
    limit: int = Query(50, ge=1, le=200),
    tag: Optional[str] = Query(None),
    min_volume: float = Query(0, ge=0),
):
    """
    Enhanced Polymarket market list with Jon-Becker analytics:
    edge detection, volume momentum, whale signals, efficiency scores.
    """
    try:
        markets = await polymarket_intel.get_top_markets(
            limit=limit, tag=tag, min_volume_24h=min_volume
        )
        return JSONResponse(content={"markets": markets, "count": len(markets)})
    except Exception as e:
        print(f"[PREDICT/markets] Error: {e}")
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/predict/signals")
async def predict_signals():
    """
    Prediction market signals dashboard:
    top edges, mispricings, surging/fading markets, whale activity.
    Equivalent to running Jon-Becker's make analyze on live data.
    """
    try:
        signals = await polymarket_intel.get_market_signals()
        return JSONResponse(content=signals)
    except Exception as e:
        print(f"[PREDICT/signals] Error: {e}")
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/polymarket/intelligence")
async def polymarket_intelligence():
    """Alias for /api/predict/signals — Polymarket intelligence dashboard."""
    try:
        signals = await polymarket_intel.get_market_signals()
        return JSONResponse(content=signals)
    except Exception as e:
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/predict/whale-watch")
async def predict_whale_watch(limit: int = Query(20, ge=1, le=50)):
    """
    Whale-watch feed: markets with anomalously high volume/liquidity ratios.
    Signals large coordinated positions moving the market.
    """
    try:
        whales = await polymarket_intel.get_whale_watch(limit=limit)
        return JSONResponse(content={"markets": whales, "count": len(whales)})
    except Exception as e:
        print(f"[PREDICT/whale-watch] Error: {e}")
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/predict/categories")
async def predict_categories():
    """Volume and market count breakdown by tag/category for the Predict page pie chart."""
    try:
        cats = await polymarket_intel.get_category_breakdown()
        return JSONResponse(content={"categories": cats})
    except Exception as e:
        print(f"[PREDICT/categories] Error: {e}")
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/predict/market/{condition_id}")
async def predict_market_detail(condition_id: str):
    """
    Deep single-market analysis: price data, microstructure signals,
    order book depth, edge/mispricing assessment, Kelly fraction.
    """
    try:
        detail = await polymarket_intel.get_market_detail(condition_id)
        if not detail:
            return JSONResponse(status_code=404, content={"error": "Market not found"})
        return JSONResponse(content=detail)
    except Exception as e:
        print(f"[PREDICT/market/{condition_id}] Error: {e}")
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/predict/context")
async def predict_market_context(
    question: str = Query(..., min_length=3, max_length=300),
):
    """
    Fast endpoint: returns relevant Polymarket markets + signals for a question.
    Use this to pre-populate the Predict page before running the full analysis.
    """
    try:
        context = await polymarket_intel.get_predict_agent_context(question)
        return JSONResponse(content=context)
    except Exception as e:
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.post("/api/predict/analyze")
async def predict_analyze(body: dict):
    """
    Full 6-agent TradingAgents analysis for a Polymarket question.

    Request body:
        { "question": "Will the Fed cut rates in June?" }

    Pipeline:
        Phase 1: Fundamentals + Sentiment + Technical (parallel)
        Phase 2: Bull + Bear (parallel, with Phase 1 outputs)
        Phase 3: Risk Manager → final recommendation + position sizing

    Returns structured output with agent-by-agent reasoning and a
    final recommendation (LONG_YES | LONG_NO | PASS) with conviction level.

    Typical response time: 30-90 seconds.
    """
    question = (body.get("question") or "").strip()
    if not question:
        return JSONResponse(status_code=422, content={"error": "question is required"})
    if len(question) > 500:
        return JSONResponse(status_code=422, content={"error": "question too long (max 500 chars)"})

    try:
        market_context = await polymarket_intel.get_predict_agent_context(question)
        analysis = await run_predict_analysis(question, market_context)
        return JSONResponse(content=analysis)
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[PREDICT/analyze] Error: {e}")
        return JSONResponse(status_code=502, content={"error": str(e)})
