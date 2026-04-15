"""
Playbook Router — FastAPI endpoints for the multi-strategy playbook engine.

Routes (all under /api/playbooks):
  GET  /api/playbooks                     — list all enabled playbooks
  GET  /api/playbooks/top                 — top-ranked tickers in a universe
  POST /api/playbooks/score-watchlist     — score a ticker list against a playbook
  POST /api/playbooks/score-portfolio     — score portfolio holdings
  GET  /api/playbooks/ticker/{ticker}     — score one ticker
  GET  /api/playbooks/{playbook_id}       — get one playbook definition

IMPORTANT: Static paths (top, score-watchlist, score-portfolio, ticker/{t})
are registered BEFORE the parameterized {playbook_id} route to avoid shadowing.

Guardrails:
  - This router is completely isolated from /api/query.
  - /api/query has NO knowledge of playbook_id and its default behavior is UNCHANGED.
  - Any new strategy-aware logic lives exclusively in this router.
  - Feature flag ENABLE_PLAYBOOK_ENGINE=false disables all routes gracefully.
"""
from __future__ import annotations

import os
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from services.playbook.playbook_registry import get as get_playbook, list_enabled, list_all
from services.playbook.playbook_service import (
    score_watchlist,
    score_portfolio,
    score_one_ticker,
    get_top_names,
)
from services.playbook.playbook_types import (
    ScoreWatchlistRequest,
    ScorePortfolioRequest,
)

router = APIRouter(prefix="/api/playbooks", tags=["playbooks"])


def _engine_enabled() -> bool:
    return os.getenv("ENABLE_PLAYBOOK_ENGINE", "true").lower() in ("true", "1", "yes")


def _require_engine():
    if not _engine_enabled():
        raise HTTPException(status_code=503, detail="Playbook engine is disabled.")


def _require_playbook(playbook_id: str):
    pb = get_playbook(playbook_id)
    if pb is None:
        known = [p.id for p in list_all()]
        raise HTTPException(
            status_code=404,
            detail=f"Unknown playbook_id: {playbook_id!r}. Known: {known}",
        )
    if not pb.enabled:
        raise HTTPException(
            status_code=404,
            detail=f"Playbook {playbook_id!r} is disabled.",
        )
    return pb


# ── GET /api/playbooks ────────────────────────────────────────────────────────

@router.get("")
async def list_playbooks():
    """
    Return all enabled playbooks with metadata for strategy selector dropdowns.
    Response shape: [{id, name, short_label, description, enabled, ui_color, version}]
    """
    _require_engine()
    playbooks = list_enabled()
    return [
        {
            "id":          pb.id,
            "name":        pb.name,
            "short_label": pb.short_label,
            "description": pb.description,
            "enabled":     pb.enabled,
            "version":     pb.version,
            "ui_color":    pb.ui_color,
            "preferred_sectors": pb.preferred_sectors,
            "preferred_themes":  pb.preferred_themes,
            "entry_style":       pb.entry_style,
            "exit_style":        pb.exit_style,
            "positioning_style": pb.positioning_style,
        }
        for pb in playbooks
    ]


# ── GET /api/playbooks/top ────────────────────────────────────────────────────
# MUST be registered before /{playbook_id}

@router.get("/top")
async def top_names(
    playbook_id: str = Query(..., description="Playbook ID to score against"),
    universe: str = Query("ai_infra", description="Universe: ai_infra | ai_software | defense | energy_transition | biotech"),
    limit: int = Query(20, ge=1, le=50, description="Max results"),
):
    """
    Score all tickers in a predefined universe and return top N by playbook score.
    """
    _require_engine()
    _require_playbook(playbook_id)
    try:
        results = await get_top_names(playbook_id, universe, limit)
        return {
            "playbook_id": playbook_id,
            "universe":    universe,
            "limit":       limit,
            "count":       len(results),
            "results":     [r.model_dump() for r in results],
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Scoring error: {e}")


# ── POST /api/playbooks/score-watchlist ───────────────────────────────────────

@router.post("/score-watchlist")
async def score_watchlist_endpoint(body: ScoreWatchlistRequest):
    """
    Score a flat list of tickers against a playbook.

    Request: {"playbook_id": "serenity", "tickers": ["NVDA", "AMD"], "include_breakdown": true}
    Response: ranked list of PlaybookScoreResult objects.
    """
    _require_engine()
    _require_playbook(body.playbook_id)
    try:
        results = await score_watchlist(body)
        return {
            "playbook_id": body.playbook_id,
            "count":       len(results),
            "results":     [r.model_dump() for r in results],
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Scoring error: {e}")


# ── POST /api/playbooks/score-portfolio ───────────────────────────────────────

@router.post("/score-portfolio")
async def score_portfolio_endpoint(body: ScorePortfolioRequest):
    """
    Score portfolio holdings and compute aggregate alignment.

    Request: {
      "playbook_id": "sjcapital",
      "holdings": [{"ticker": "NVDA", "weight": 0.18}, ...],
      "include_breakdown": true
    }
    Response: PortfolioScoreResult with per-holding scores + aggregate.
    """
    _require_engine()
    _require_playbook(body.playbook_id)
    try:
        result = await score_portfolio(body)
        return result.model_dump()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Scoring error: {e}")


# ── GET /api/playbooks/ticker/{ticker} ────────────────────────────────────────

@router.get("/ticker/{ticker}")
async def score_one_ticker_endpoint(
    ticker: str,
    playbook_id: str = Query(..., description="Playbook ID to score against"),
):
    """
    Score a single ticker against a named playbook.
    Returns full factor breakdown and matched rules.
    """
    _require_engine()
    _require_playbook(playbook_id)
    try:
        result = await score_one_ticker(ticker, playbook_id)
        return result.model_dump()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Scoring error: {e}")


# ── GET /api/playbooks/{playbook_id} ─────────────────────────────────────────
# Parameterized — MUST be last to avoid shadowing static routes above

@router.get("/{playbook_id}")
async def get_one_playbook(playbook_id: str):
    """
    Return a single playbook definition including weights, filters, and metadata.
    """
    _require_engine()
    pb = _require_playbook(playbook_id)
    return {
        "id":                      pb.id,
        "name":                    pb.name,
        "short_label":             pb.short_label,
        "description":             pb.description,
        "enabled":                 pb.enabled,
        "version":                 pb.version,
        "factor_weights":          pb.factor_weights,
        "hard_filters":            [hf.model_dump() for hf in pb.hard_filters],
        "penalty_rules":           [pr.model_dump() for pr in pb.penalty_rules],
        "preferred_themes":        pb.preferred_themes,
        "preferred_sectors":       pb.preferred_sectors,
        "entry_style":             pb.entry_style,
        "exit_style":              pb.exit_style,
        "positioning_style":       pb.positioning_style,
        "ui_color":                pb.ui_color,
        "explanation_template_key":pb.explanation_template_key,
    }
