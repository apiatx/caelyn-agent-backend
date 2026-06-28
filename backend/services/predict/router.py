"""
Predict page API router — Jon-Becker + TauricResearch + Prophetik Signal Engine.

Endpoints:

  GET  /api/predict/markets          → Enhanced Polymarket market list with signals
  GET  /api/predict/market/{id}      → Deep analysis of a single market
  GET  /api/predict/signals          → Dashboard signals (edges, mispricings, whale watch)
  GET  /api/predict/whale-watch      → Markets with anomalous volume spikes
  GET  /api/predict/categories       → Volume/count breakdown by tag
  GET  /api/predict/context          → Relevant markets for a question (pre-analyze)
  POST /api/predict/analyze          → Full 6-agent TradingAgents analysis
  GET  /api/polymarket/intelligence  → Market intelligence overview (alias for signals)

  -- Prophetik Signal Engine (new) --
  GET  /api/predict/scored           → Scored market list (7 dimensions + composite)
  GET  /api/predict/recommendations  → Top decision-layer recommendation buckets
  GET  /api/predict/enriched-signals → Extended signals dashboard with scoring summaries
  GET  /api/predict/scored/{id}      → Single market with full scoring dimensions
  GET  /api/predict/signal-changes   → Recent signal changes detected via snapshot diffing
  GET  /api/predict/diagnostics      → Scoring metadata for debugging
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query, Request, Depends
from fastapi.responses import JSONResponse

from datetime import datetime, timezone

from services.predict.polymarket_intelligence import polymarket_intel
from services.predict.trading_agents import run_predict_analysis
from services.predict.scoring import score_markets as _score_markets, WEIGHTS as _SCORING_WEIGHTS

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from subscription import require_subscription

router = APIRouter(tags=["predict"])


@router.get("/api/predict/markets")
async def predict_markets(
    limit: int = Query(200, ge=1, le=500),
    tag: Optional[str] = Query(None),
    min_volume: float = Query(0, ge=0),
):
    """
    Enhanced Polymarket market list with Jon-Becker analytics:
    edge detection, volume momentum, whale signals, efficiency scores,
    and Prophetik scoring dimensions (composite, conviction, flow, etc.).
    """
    try:
        markets = await polymarket_intel.get_top_markets(
            limit=limit, tag=tag, min_volume_24h=min_volume
        )
        # Run scoring pipeline so all Prophetik signal fields are present.
        # score_markets() is pure Python over already-enriched dicts — no extra API calls.
        scored = _score_markets(markets)
        for m in scored:
            scores = m.get("scores", {})
            m["composite_score"] = m.get("composite_score", 0) or 0
            m["conviction_score"] = scores.get("conviction", 0) or 0
            m["momentum_score"] = scores.get("momentum", 0) or 0
            m["flow_score"] = scores.get("flow", 0) or 0
            m["execution_quality_score"] = scores.get("execution_quality", 0) or 0
            m["participation_quality_score"] = scores.get("participation_quality", 0) or 0
            m["time_quality_score"] = scores.get("time_quality", 0) or 0
            m["trap_risk_score"] = scores.get("trap_risk", 0) or 0
            m["momentum_label"] = m.get("momentum_label", "flat") or "flat"
            m["price_change_24h"] = m.get("price_change_1d", 0)
            if not m.get("slug"):
                question = m.get("question", "")
                slug = question.lower().strip()
                for ch in ["?", "'", '"', ",", ".", "!", "(", ")", "[", "]", "{", "}", "&", "%", "$", "#", "@"]:
                    slug = slug.replace(ch, "")
                slug = slug.replace(" ", "-").replace("--", "-").strip("-")
                m["slug"] = slug[:100]
        return JSONResponse(content={"markets": scored, "count": len(scored)})
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


# ── Prophetik Signal Engine Endpoints ─────────────────────────────────────────


@router.get("/api/predict/scored")
async def predict_scored_markets(
    limit: int = Query(200, ge=1, le=500),
    tag: Optional[str] = Query(None),
    min_volume: float = Query(0, ge=0),
):
    """
    Scored market list — each market enriched with 7 Prophetik scoring dimensions
    (conviction, momentum, flow, execution_quality, participation_quality,
    time_quality, trap_risk) plus composite_score and momentum_label.
    Sorted by composite_score descending.
    """
    try:
        scored = await polymarket_intel.get_scored_markets(
            limit=limit, tag=tag, min_volume_24h=min_volume
        )
        return JSONResponse(content={"markets": scored, "count": len(scored)})
    except Exception as e:
        print(f"[PREDICT/scored] Error: {e}")
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/predict/recommendations")
async def predict_recommendations():
    """
    Top decision-layer payload: 10 recommendation buckets with explainability strings.
    Each bucket contains up to 8 markets with reason arrays explaining why
    the market was selected. Buckets:
      best_bet_now, best_yes_setup, best_no_setup, best_momentum_continuation,
      best_mean_reversion_candidate, best_whale_follow, avoid_or_trap_markets,
      best_execution_quality, strongest_flow_without_confirmation,
      strongest_conviction_with_good_execution
    """
    try:
        recs = await polymarket_intel.get_recommendations()
        return JSONResponse(content=recs)
    except Exception as e:
        print(f"[PREDICT/recommendations] Error: {e}")
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/predict/enriched-signals")
async def predict_enriched_signals():
    """
    Extended signals dashboard: all standard signals PLUS Prophetik scoring summaries
    and top-scored markets. Backward-compatible with /api/predict/signals — includes
    all original fields with additional scoring_summary and top_scored sections.
    """
    try:
        signals = await polymarket_intel.get_enriched_signals()
        return JSONResponse(content=signals)
    except Exception as e:
        print(f"[PREDICT/enriched-signals] Error: {e}")
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/predict/scored/{condition_id}")
async def predict_scored_market_detail(condition_id: str):
    """
    Deep single-market analysis with full Prophetik scoring dimensions.
    Extends the standard market detail with scores, composite_score,
    and momentum_label.
    """
    try:
        detail = await polymarket_intel.get_scored_market_detail(condition_id)
        if not detail:
            return JSONResponse(status_code=404, content={"error": "Market not found"})
        return JSONResponse(content=detail)
    except Exception as e:
        print(f"[PREDICT/scored/{condition_id}] Error: {e}")
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/predict/signal-changes")
async def predict_signal_changes():
    """
    Recent signal changes detected via snapshot diffing.
    Compares consecutive scored-market snapshots and surfaces meaningful
    changes: score jumps/drops, trap risk spikes, momentum shifts,
    repricings, bucket entries/exits, flow spikes, spread changes, etc.

    Returns:
        {
            "changes": [...],
            "change_count": N,
            "last_updated": "ISO datetime",
            "snapshot_age_seconds": float
        }

    Changes are ephemeral (in-memory, last 50 / last 30 min) and reset
    on server restart.
    """
    try:
        result = await polymarket_intel.get_signal_changes()
        return JSONResponse(content=result)
    except Exception as e:
        print(f"[PREDICT/signal-changes] Error: {e}")
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/predict/diagnostics")
async def predict_diagnostics():
    """
    Scoring engine diagnostics and metadata. Useful for debugging why
    a market ranked highly or was flagged as a trap.
    Returns scoring weights, dimension descriptions, bucket definitions,
    and data source information.
    """
    return JSONResponse(content={
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine_version": "1.0.0",
        "scoring_weights": _SCORING_WEIGHTS,
        "dimensions": {
            "conviction": {
                "range": "0-100",
                "description": "Distance from 50/50 — higher means stronger consensus",
            },
            "momentum": {
                "range": "0-100",
                "description": "Price movement strength across 1h/24h/7d with acceleration/persistence bonuses",
            },
            "flow": {
                "range": "0-100",
                "description": "Volume intensity + burstiness vs 7d average",
            },
            "execution_quality": {
                "range": "0-100",
                "description": "Spread tightness + liquidity depth + fill quality",
            },
            "participation_quality": {
                "range": "0-100",
                "description": "Breadth of participation — penalizes concentrated ownership",
            },
            "time_quality": {
                "range": "0-100",
                "description": "Expiry profile suitability — sweet spot is 3-30 days",
            },
            "trap_risk": {
                "range": "0-100",
                "description": "Crowdedness / fake-move danger — HIGHER is MORE dangerous",
            },
        },
        "recommendation_buckets": [
            "best_bet_now",
            "best_yes_setup",
            "best_no_setup",
            "best_momentum_continuation",
            "best_mean_reversion_candidate",
            "best_whale_follow",
            "avoid_or_trap_markets",
            "best_execution_quality",
            "strongest_flow_without_confirmation",
            "strongest_conviction_with_good_execution",
        ],
        "data_sources": {
            "gamma_api": "https://gamma-api.polymarket.com — market metadata, prices, volume, tags",
            "clob_api": "https://clob.polymarket.com — order book depth (where available)",
            "derived": "All scoring dimensions are computed from Gamma + CLOB data",
        },
        "data_limitations": {
            "holder_concentration": "Not available from public Polymarket APIs — proxied via vol/liq ratio and competitive flag",
            "open_interest": "Not directly exposed by Gamma API — volume serves as proxy",
            "trade_tape": "Individual trades not available — burstiness proxied via 24h vs 7d volume ratio",
            "whale_identity": "No wallet-level data — whale activity detected via volume/liquidity anomalies",
        },
    })


@router.get("/api/predict/odds/live")
async def predict_odds_live():
    """
    Live tracked odds for all registry families — Market Dashboard widget source.

    Returns the most recent scanner payload including:
      - yes_probability, yes_pct
      - delta_1h_pp / delta_24h_pp / delta_7d_pp  (from 7-day DB history when available,
        falls back to Polymarket's own price_change fields on cold start)
      - volume_24h, liquidity, candidate_count, driver_markets
      - dashboard_enabled / prophetik_enabled / preferred_outcome / priority flags

    The payload is pre-warmed every 30 minutes by the _odds_scanner_loop background
    task. On the first request after a cold start, the scan runs inline (~2–5 s).
    """
    try:
        from services.predict.odds_scanner import odds_scanner as _os
        payload = await _os.get_live()
        return JSONResponse(content=payload)
    except Exception as e:
        print(f"[PREDICT/odds/live] Error: {e}")
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/predict/odds/history")
async def predict_odds_history(
    family_key: str = Query(..., min_length=2, max_length=80),
    days: int = Query(7, ge=1, le=7),
):
    """
    7-day probability history for a single tracked-odds family.

    Returns time-series snapshots (one per 30-min scan) as an array of:
      { captured_at, yes_probability, yes_pct, volume_24h, liquidity }

    Use the family_key values from /api/predict/odds/live
    (e.g. "fed_rate_decision", "bitcoin_price", "russia_ukraine").

    Suitable for charting a family's probability over time.
    """
    try:
        from services.predict.odds_scanner import odds_scanner as _os
        result = _os.get_history(family_key=family_key, days=days)
        return JSONResponse(content=result)
    except Exception as e:
        print(f"[PREDICT/odds/history] Error: {e}")
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/predict/odds/diagnostics")
async def predict_odds_diagnostics():
    """Diagnostics for the Tracked Odds Registry scanner and Neon snapshot table."""
    try:
        from services.predict.odds_scanner import odds_scanner as _os
        return JSONResponse(content=_os.get_diagnostics())
    except Exception as e:
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.post("/api/predict/analyze")
async def predict_analyze(request: Request, body: dict, _sub: None = Depends(require_subscription)):
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
