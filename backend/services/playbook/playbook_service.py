"""
Playbook Service — high-level operations for watchlist, portfolio, and top-names scoring.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from services.playbook.playbook_registry import get as get_playbook, list_enabled
from services.playbook.playbook_scoring import score_tickers_batch, score_ticker
from services.playbook.playbook_types import (
    PlaybookScoreResult,
    PortfolioScoreResult,
    ScorePortfolioRequest,
    ScoreWatchlistRequest,
)


def _fmp_key() -> str:
    return os.getenv("FMP_API_KEY", "")


# ── Watchlist scoring ─────────────────────────────────────────────────────────

async def score_watchlist(req: ScoreWatchlistRequest) -> List[PlaybookScoreResult]:
    """Score a flat ticker list against a playbook. Returns results sorted by score."""
    pb = get_playbook(req.playbook_id)
    if pb is None:
        raise ValueError(f"Unknown playbook_id: {req.playbook_id!r}")
    if not pb.enabled:
        raise ValueError(f"Playbook {req.playbook_id!r} is disabled.")

    tickers = [t.upper().strip() for t in req.tickers if t.strip()][:50]
    if not tickers:
        return []

    print(f"[PLAYBOOK_SERVICE] score_watchlist playbook={req.playbook_id!r} tickers={len(tickers)}")
    return await score_tickers_batch(tickers, pb, _fmp_key())


# ── Portfolio scoring ─────────────────────────────────────────────────────────

async def score_portfolio(req: ScorePortfolioRequest) -> PortfolioScoreResult:
    """
    Score each holding in a portfolio and compute an aggregate playbook alignment score.
    Aggregate = weight-adjusted mean of per-holding final scores.
    """
    pb = get_playbook(req.playbook_id)
    if pb is None:
        raise ValueError(f"Unknown playbook_id: {req.playbook_id!r}")
    if not pb.enabled:
        raise ValueError(f"Playbook {req.playbook_id!r} is disabled.")

    holdings = req.holdings
    if not holdings:
        return PortfolioScoreResult(
            playbook_id=req.playbook_id,
            aggregate_score=0.0,
            holdings=[],
            strongest_aligned=[],
            weakest_aligned=[],
            concentration_notes=["No holdings provided"],
        )

    tickers = [h.get("ticker", "").upper().strip() for h in holdings if h.get("ticker")]
    tickers = [t for t in tickers if t][:50]

    print(f"[PLAYBOOK_SERVICE] score_portfolio playbook={req.playbook_id!r} holdings={len(tickers)}")
    scored_list = await score_tickers_batch(tickers, pb, _fmp_key())

    # Build ticker → result lookup
    result_map = {r.ticker: r for r in scored_list}

    # Weight-adjusted aggregate score
    total_weight = 0.0
    weighted_sum = 0.0
    for h in holdings:
        t = h.get("ticker", "").upper().strip()
        weight = float(h.get("weight", 0.0))
        if t in result_map:
            weighted_sum += result_map[t].final_score * weight
            total_weight += weight

    if total_weight > 0:
        aggregate = round(weighted_sum / total_weight, 1)
    else:
        scores = [r.final_score for r in scored_list]
        aggregate = round(sum(scores) / len(scores), 1) if scores else 0.0

    # Strongest / weakest (top 3 each)
    by_score = sorted(scored_list, key=lambda r: r.final_score, reverse=True)
    strongest = [r.ticker for r in by_score[:3] if r.hard_filter_pass]
    weakest   = [r.ticker for r in reversed(by_score) if r.hard_filter_pass][:3]

    # Concentration notes
    notes: List[str] = []
    hf_failures = [r.ticker for r in scored_list if not r.hard_filter_pass]
    if hf_failures:
        notes.append(f"{len(hf_failures)} holding(s) failed hard filters: {', '.join(hf_failures)}")
    high_score_count = sum(1 for r in scored_list if r.final_score >= 65)
    if high_score_count == 0:
        notes.append("No holdings with strong playbook alignment (≥65). Consider rotating.")
    elif high_score_count / max(len(scored_list), 1) > 0.7:
        notes.append(f"Strong portfolio alignment: {high_score_count}/{len(scored_list)} holdings score ≥65.")

    return PortfolioScoreResult(
        playbook_id=req.playbook_id,
        aggregate_score=aggregate,
        holdings=scored_list,
        strongest_aligned=strongest,
        weakest_aligned=weakest,
        concentration_notes=notes,
    )


# ── Single-ticker scoring ─────────────────────────────────────────────────────

async def score_one_ticker(ticker: str, playbook_id: str) -> PlaybookScoreResult:
    """Score a single ticker against a named playbook."""
    pb = get_playbook(playbook_id)
    if pb is None:
        raise ValueError(f"Unknown playbook_id: {playbook_id!r}")
    if not pb.enabled:
        raise ValueError(f"Playbook {playbook_id!r} is disabled.")
    return await score_ticker(ticker.upper().strip(), pb, _fmp_key())


# ── Top-names discovery ───────────────────────────────────────────────────────

# Predefined universe tickers — v1 static lists; plug in dynamic screener later
_UNIVERSES: Dict[str, List[str]] = {
    "ai_infra": [
        "NVDA", "AMD", "AVGO", "AMAT", "LRCX", "KLAC", "ASML", "TSM",
        "MRVL", "SMCI", "CRDO", "AAOI", "LITE", "COHR", "II",
        "IPGP", "FORM", "ACLS", "ONTO", "ENTG",
    ],
    "ai_software": [
        "MSFT", "GOOGL", "META", "AMZN", "CRM", "NOW", "SNOW", "PLTR",
        "AI", "BBAI", "SOUN", "UPST", "AMBA",
    ],
    "defense": [
        "LMT", "RTX", "NOC", "GD", "LHX", "HII", "KTOS", "AVAV",
        "DRS", "CACI", "SAIC", "PAE", "RCAT",
    ],
    "energy_transition": [
        "ENPH", "SEDG", "FSLR", "ARRY", "RUN", "NOVA", "STEM",
        "BE", "PLUG", "BLDP", "AMRC", "NEE", "BEP",
    ],
    "biotech": [
        "MRNA", "BNTX", "REGN", "VRTX", "BIIB", "ALNY", "BEAM",
        "EDIT", "NTLA", "CRSP", "IONS", "AKRO",
    ],
}


async def get_top_names(
    playbook_id: str,
    universe: str = "ai_infra",
    limit: int = 20,
) -> List[PlaybookScoreResult]:
    """Score all tickers in a universe and return top N by final score."""
    pb = get_playbook(playbook_id)
    if pb is None:
        raise ValueError(f"Unknown playbook_id: {playbook_id!r}")
    if not pb.enabled:
        raise ValueError(f"Playbook {playbook_id!r} is disabled.")

    tickers = _UNIVERSES.get(universe, [])
    if not tickers:
        raise ValueError(f"Unknown universe: {universe!r}. Available: {list(_UNIVERSES.keys())}")

    print(f"[PLAYBOOK_SERVICE] get_top_names playbook={playbook_id!r} universe={universe!r} n={len(tickers)}")
    results = await score_tickers_batch(tickers, pb, _fmp_key())
    return results[:max(1, limit)]
