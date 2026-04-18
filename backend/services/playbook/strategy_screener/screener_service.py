"""
Strategy Screener — generation orchestrator.

Runs the full Serenity pipeline for one snapshot:
  1. compute_serenity_regime()         → current regime
  2. run_discover(mode="auto", ...)    → ranked candidate list
  3. for each shortlisted candidate:
       build_full_report(candidate, ...)   → deterministic text sections
       save_report(...)                    → persist
  4. save_snapshot(...)                → persist snapshot metadata + candidate list

Completely reuses existing Serenity logic — no duplication.
Zero coupling to /api/query.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from services.playbook.strategy_screener.screener_report_builder import (
    assign_grade,
    build_full_report,
    build_one_line_summary,
)
from services.playbook.strategy_screener.screener_storage import (
    save_snapshot,
    save_report,
)

# Cadence: configurable via env, default 14 days
CADENCE_DAYS  = int(os.environ.get("SCREENER_CADENCE_DAYS", "14"))
SHORTLIST_SIZE = int(os.environ.get("SCREENER_SHORTLIST_SIZE", "20"))
PLAYBOOK_ID   = "serenity"
VERSION       = "1.0"

# In-memory flag to prevent concurrent generation
_generation_in_progress: bool = False


def _snapshot_id_from_dt(dt: datetime) -> str:
    return f"serenity_{dt.strftime('%Y_%m_%d_%H%M')}"


def _candidate_to_screener_dict(c: Any) -> Dict[str, Any]:
    """Convert a DiscoveryCandidate (Pydantic or dict) to a plain dict for storage."""
    if hasattr(c, "model_dump"):
        d = c.model_dump()
    elif hasattr(c, "dict"):
        d = c.dict()
    else:
        d = dict(c)

    bbs  = d.get("best_blend_score", 0.0)
    bcs  = d.get("bottleneck_criticality_score", 50.0)
    hs   = d.get("hiddenness_score", 50.0)
    conf = d.get("data_confidence", "high")

    grade   = assign_grade(bbs, conf, hs, bcs)
    summary = build_one_line_summary(d)
    themes  = d.get("themes", [])

    return {
        "ticker":                       d.get("ticker", ""),
        "company_name":                 d.get("company_name", ""),
        "country":                      d.get("country", "US"),
        "exchange":                     d.get("exchange", "NASDAQ"),
        "market_cap_usd":               d.get("market_cap_usd"),
        "theme":                        themes[0] if themes else "",
        "themes":                       themes,
        "chain_role_type":              d.get("chain_role_type", "adjacent_supplier"),
        "layer_depth":                  d.get("layer_depth", 2),
        "grade":                        grade,
        "best_blend_score":             round(bbs, 1),
        "bottleneck_criticality_score": round(bcs, 1),
        "hiddenness_score":             round(hs, 1),
        "chain_depth_score":            round(d.get("chain_depth_score", 50.0), 1),
        "supply_chain_confidence_score": round(d.get("supply_chain_confidence_score", 50.0), 1),
        "coverage_status":              d.get("coverage_status", "full"),
        "data_confidence":              conf,
        "us_access_proxy":              d.get("us_access_proxy"),
        "one_line_summary":             summary,
        "giant_anchors":                d.get("giant_anchors", []),
        "comparable_names":             d.get("comparable_names", []),
        # Carry through fields needed by report builder
        "thesis_summary":               d.get("thesis_summary", ""),
        "fit_reasoning":                d.get("fit_reasoning", []),
        "why_now":                      d.get("why_now", ""),
        "why_hidden":                   d.get("why_hidden", ""),
        "what_to_verify_next":          d.get("what_to_verify_next", ""),
        "what_would_break_thesis":      d.get("what_would_break_thesis", ""),
        "coverage_notes":               d.get("coverage_notes", ""),
        "crowding_flags":               d.get("crowding_flags", []),
        "data_gaps":                    d.get("data_gaps", []),
        "chain_layers":                 d.get("chain_layers", []),
    }


async def generate_snapshot(manual_override: bool = False) -> Dict[str, Any]:
    """
    Run one full screener snapshot generation and persist the result.

    Returns the snapshot dict.
    Raises on fatal errors; individual candidate failures are logged and skipped.
    """
    global _generation_in_progress

    if _generation_in_progress:
        print("[SCREENER] Generation already in progress — skipping duplicate request")
        return {}

    _generation_in_progress = True
    now       = datetime.now(timezone.utc)
    snap_id   = _snapshot_id_from_dt(now)

    print(f"[SCREENER] Starting snapshot generation: {snap_id}")

    # Save a "generating" placeholder so the frontend shows progress state
    _save_placeholder(snap_id, now, manual_override)

    try:
        # Step 1 — Regime detection
        from services.playbook.regime_service import compute_serenity_regime
        regime = compute_serenity_regime()
        regime_dict = regime.model_dump()
        print(f"[SCREENER] Regime: {regime.regime_id} | confidence={regime.confidence}")

        # Step 2 — Discovery in auto mode
        from services.playbook.discovery_types import DiscoverRequest
        from services.playbook.discovery_service import run_discover

        disc_req = DiscoverRequest(
            playbook_id=PLAYBOOK_ID,
            mode="auto",
            include_foreign=True,
            max_depth=4,
            limit=SHORTLIST_SIZE + 5,   # fetch a few extra to account for low-confidence filtering
            only_hidden=False,
            use_web_validation=False,
            include_adr_or_etf_proxies=True,
        )
        disc_result = await run_discover(disc_req)
        print(f"[SCREENER] Discovery: {len(disc_result.top_candidates)} top candidates")

        # Step 3 — Build candidate list (deduplicated, best_blend sorted)
        all_candidates = disc_result.top_candidates[:]
        # Supplement with best_blend bucket if it adds unique tickers
        seen = {c.ticker for c in all_candidates}
        for c in disc_result.best_blend_candidates:
            if c.ticker not in seen:
                all_candidates.append(c)
                seen.add(c.ticker)

        # Sort by best_blend_score descending, take shortlist
        all_candidates.sort(key=lambda c: c.best_blend_score, reverse=True)
        shortlist = all_candidates[:SHORTLIST_SIZE]

        # Step 4 — Convert to screener dicts + build reports
        screener_candidates = [_candidate_to_screener_dict(c) for c in shortlist]

        # Step 5 — Build and persist full reports
        regime_slim = {
            "regime_id":       regime.regime_id,
            "label":           regime.label,
            "top_themes":      regime.top_themes,
            "top_anchors":     regime.top_anchors,
            "confidence":      regime.confidence,
            "why_now":         regime.why_now,
            "recommended_mode": regime.recommended_mode,
            "recommended_depth": regime.recommended_depth,
        }

        print(f"[SCREENER] Building {len(screener_candidates)} reports...")
        for cand_dict in screener_candidates:
            try:
                report = build_full_report(
                    candidate=cand_dict,
                    snapshot_id=snap_id,
                    regime_context=regime_slim,
                    analyze_result=None,
                )
                save_report(snap_id, cand_dict["ticker"], report)
            except Exception as e:
                print(f"[SCREENER] Report build error for {cand_dict.get('ticker', '?')}: {e}")

        # Step 6 — Build summary text
        top3 = [c["company_name"] for c in screener_candidates[:3]]
        top_themes = regime.top_themes[:3]
        themes_str = ", ".join(t.replace("_", " ") for t in top_themes)
        summary = (
            f"Serenity Strategy Screener — {regime.label}. "
            f"Regime confidence: {regime.confidence}. "
            f"Top themes: {themes_str}. "
            f"This issue surfaces {len(screener_candidates)} supply chain bottleneck candidates. "
            f"Lead names: {', '.join(top3)}."
        )

        # Step 7 — Persist final snapshot
        snapshot = {
            "snapshot_id":    snap_id,
            "playbook_id":    PLAYBOOK_ID,
            "generated_at":   now.isoformat(),
            "cadence":        _cadence_label(CADENCE_DAYS),
            "cadence_days":   CADENCE_DAYS,
            "regime_context": regime_slim,
            "summary":        summary,
            "results":        screener_candidates,
            "results_count":  len(screener_candidates),
            "status":         "complete",
            "version":        VERSION,
            "generation_notes": f"Discovery: {len(disc_result.top_candidates)} raw candidates → {len(shortlist)} shortlisted",
            "manual_override": manual_override,
        }
        save_snapshot(snapshot)
        print(f"[SCREENER] Snapshot complete: {snap_id} ({len(screener_candidates)} candidates)")
        return snapshot

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[SCREENER] Generation FAILED: {e}")
        _save_error(snap_id, now, str(e))
        return {}
    finally:
        _generation_in_progress = False


def _cadence_label(days: int) -> str:
    if days == 1:
        return "daily"
    if days == 7:
        return "weekly"
    if days == 14:
        return "biweekly"
    if days == 30:
        return "monthly"
    return f"every_{days}_days"


def _save_placeholder(snap_id: str, now: datetime, manual_override: bool):
    save_snapshot({
        "snapshot_id":    snap_id,
        "playbook_id":    PLAYBOOK_ID,
        "generated_at":   now.isoformat(),
        "cadence":        _cadence_label(CADENCE_DAYS),
        "cadence_days":   CADENCE_DAYS,
        "regime_context": None,
        "summary":        "Generating...",
        "results":        [],
        "results_count":  0,
        "status":         "generating",
        "version":        VERSION,
        "generation_notes": "Generation in progress",
        "manual_override": manual_override,
    })


def _save_error(snap_id: str, now: datetime, error_msg: str):
    save_snapshot({
        "snapshot_id":    snap_id,
        "playbook_id":    PLAYBOOK_ID,
        "generated_at":   now.isoformat(),
        "cadence":        _cadence_label(CADENCE_DAYS),
        "cadence_days":   CADENCE_DAYS,
        "regime_context": None,
        "summary":        "Generation failed",
        "results":        [],
        "results_count":  0,
        "status":         "error",
        "version":        VERSION,
        "generation_notes": f"Error: {error_msg[:300]}",
        "manual_override": False,
    })
