"""
Playbook Router — FastAPI endpoints for the multi-strategy playbook engine.

Routes (all under /api/playbooks):
  GET  /api/playbooks                       — list all enabled playbooks
  GET  /api/playbooks/top                   — top-ranked tickers in a universe
  POST /api/playbooks/score-watchlist       — score a ticker list against a playbook
  POST /api/playbooks/score-portfolio       — score portfolio holdings
  GET  /api/playbooks/ticker/{ticker}       — score one ticker
  POST /api/playbooks/analyze               — deep analysis with explanations
  POST /api/playbooks/discover              — on-demand Serenity discovery engine
  POST /api/playbooks/supply-chain-map      — structured supply-chain map
  GET  /api/playbooks/themes                — list all supported themes
  GET  /api/playbooks/giants                — list all supported giant anchors
  GET  /api/playbooks/discovery-capabilities — discovery engine capabilities
  GET  /api/playbooks/{playbook_id}         — get one playbook definition

IMPORTANT: All static paths are registered BEFORE the parameterized {playbook_id}
route to avoid shadowing.

Guardrails:
  - This router is completely isolated from /api/query.
  - /api/query has NO knowledge of playbook_id and its default behavior is UNCHANGED.
  - Discovery routes are fully isolated: no coupling to terminal / AI brain.
  - Feature flag ENABLE_PLAYBOOK_ENGINE=false disables all routes gracefully.
  - No Brave or Tavily usage in the discovery flow.
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
from services.playbook.analyzer import AnalyzeRequest, AnalyzeResponse, run_analyze
from services.playbook.discovery_types import DiscoverRequest, SupplyChainMapRequest
from services.playbook.discovery_service import run_discover, run_supply_chain_map

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


# ── POST /api/playbooks/analyze ───────────────────────────────────────────────
# MUST be registered before /{playbook_id}

@router.post("/analyze", response_model=AnalyzeResponse)
async def analyze_endpoint(body: AnalyzeRequest):
    """
    Deep playbook analysis with deterministic explanations.

    Scores tickers against a playbook and returns structured explanations:
    thesis_summary, fit_reasoning, non_fit_reasoning, key_confirming_signals,
    top_risks, what_would_improve_score, what_would_break_thesis, supply_chain_tags.

    Context modes: watchlist | portfolio | custom | universe

    Optional discovery bridge (Serenity only):
    If discovery_mode is provided and playbook_id=serenity, runs the discovery
    engine first, then scores top discovered candidates and returns a combined
    answer. S&J is unaffected.
    """
    _require_engine()
    _require_playbook(body.playbook_id)
    try:
        result = await run_analyze(body)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Analysis error: {e}")


# ── POST /api/playbooks/discover ──────────────────────────────────────────────
# MUST be registered before /{playbook_id}

@router.post("/discover")
async def discover_endpoint(body: DiscoverRequest):
    """
    On-demand Serenity discovery + supply-chain intelligence engine.

    Discovers hidden bottleneck names from giants, themes, and supply-chain layers.
    Supports foreign-market names with explicit confidence metadata.

    Modes: giant_chain | theme_scan | foreign_bottlenecks | ticker_chain | country_theme_scan | custom

    Provider usage:
      - Finnhub: profile + news enrichment (primary)
      - Tradier: quote/liquidity for US-listed and ADR names
      - FMP: market cap sanity (sparing — 250/day cap)
      - Perplexity: shortlist validation only (if use_web_validation=true)

    Guardrails:
      - User-triggered only — no background scans
      - No Brave/Tavily usage
      - Missing data degrades gracefully
      - Foreign data confidence is explicit, not hand-wavy
    """
    _require_engine()
    try:
        result = await run_discover(body)
        return result.model_dump()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Discovery error: {e}")


# ── POST /api/playbooks/supply-chain-map ──────────────────────────────────────

@router.post("/supply-chain-map")
async def supply_chain_map_endpoint(body: SupplyChainMapRequest):
    """
    Return a structured multi-layer supply-chain map for a giant or theme anchor.

    Layers:
      0 = Giant / End-Demand Anchor
      1 = Core Systems / Direct Integrators
      2 = Key Components / Subsystems
      3 = Constrained Subcomponents / Bottlenecks
      4 = Upstream Materials / Tooling / Support

    Each node includes: ticker, company, country, exchange, role, evidence, ADR proxy.
    """
    _require_engine()
    try:
        result = await run_supply_chain_map(body)
        return result.model_dump()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Supply chain map error: {e}")


# ── GET /api/playbooks/themes ─────────────────────────────────────────────────

@router.get("/themes")
async def list_themes_endpoint():
    """
    Return all supported Serenity discovery themes with full metadata.
    Includes: label, description, giant_anchors, chain_layers, example_companies,
    preferred_countries, policy_linkage, serenity_priority.
    """
    _require_engine()
    try:
        from services.playbook.theme_discovery import list_themes
        return {
            "themes": list_themes(),
            "total":  len(list_themes()),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Theme list error: {e}")


# ── GET /api/playbooks/giants ─────────────────────────────────────────────────

@router.get("/giants")
async def list_giants_endpoint():
    """
    Return all supported giant anchor platforms for discovery.
    Includes: id, name, description, themes, capex_scale, anchor_ticker, foreign_exposure.
    """
    _require_engine()
    try:
        from services.playbook.giant_map import list_giants
        giants = list_giants()
        return {
            "giants": giants,
            "total":  len(giants),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Giants list error: {e}")


# ── GET /api/playbooks/discovery-capabilities ─────────────────────────────────

@router.get("/discovery-capabilities")
async def discovery_capabilities_endpoint():
    """
    Return a structured overview of the discovery engine's capabilities.
    Useful for the frontend to know what modes/filters are supported.
    """
    _require_engine()
    try:
        from services.playbook.theme_discovery import list_themes
        from services.playbook.giant_map import list_giants
        from services.playbook.foreign_market_map import list_supported_countries

        themes = list_themes()
        giants = list_giants()
        countries = list_supported_countries()

        return {
            "discovery_engine_version": "1.0.0",
            "supported_modes": [
                {
                    "mode":        "giant_chain",
                    "description": "Traverse supply chain from a major platform giant (NVDA, MSFT, etc.)",
                    "requires":    ["giant"],
                },
                {
                    "mode":        "theme_scan",
                    "description": "Scan companies relevant to one or more Serenity themes",
                    "requires":    ["theme_ids"],
                },
                {
                    "mode":        "foreign_bottlenecks",
                    "description": "Focus on non-US supply chain positions with US access guidance",
                    "requires":    [],
                },
                {
                    "mode":        "ticker_chain",
                    "description": "Find upstream/downstream neighbors of a known ticker",
                    "requires":    ["ticker"],
                },
                {
                    "mode":        "country_theme_scan",
                    "description": "Cross-filter by country + theme for targeted regional discovery",
                    "requires":    ["country_filters"],
                },
                {
                    "mode":        "custom",
                    "description": "Flexible fallback using any combination of filters",
                    "requires":    [],
                },
            ],
            "discovery_scoring_dimensions": [
                "chain_depth_score",
                "bottleneck_criticality_score",
                "hiddenness_score",
                "giant_dependency_score",
                "foreign_uniqueness_score",
                "supply_chain_confidence_score",
                "proxy_accessibility_score",
                "theme_purity_score",
            ],
            "supported_themes":   [{"id": t["id"], "label": t["label"]} for t in themes],
            "supported_giants":   [{"id": g["id"], "name": g["name"]} for g in giants],
            "supported_countries": [
                {"code": c["code"], "name": c["name"], "coverage_status": c["coverage_status"]}
                for c in countries
            ],
            "provider_notes": {
                "finnhub":    "Primary — profile, news, international metadata (company_profile2, company_news)",
                "tradier":    "Primary market data — quote/liquidity for US-listed and ADR names only",
                "fmp":        "Sparing — market cap/profile reference (250/day cap)",
                "perplexity": "Targeted validation only — shortlisted finalists (max 5 per request)",
                "gemini":     "Optional — web-grounded synthesis when Perplexity insufficient",
                "grok":       "Optional — crowding/X sentiment/theme heat",
                "brave":      "NOT USED — unreliable in this environment",
                "tavily":     "NOT USED — unreliable in this environment",
            },
            "guardrails": [
                "User-triggered only — no background scan jobs",
                "No coupling to /api/query or default AI terminal",
                "No Brave or Tavily usage",
                "Foreign data confidence is explicit — never hand-wavy",
                "Missing data degrades gracefully to heuristic/fallback",
                "S&J philosophy is unchanged — discovery is Serenity-focused",
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Capabilities error: {e}")


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
