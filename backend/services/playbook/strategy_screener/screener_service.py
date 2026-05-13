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
SHORTLIST_SIZE = int(os.environ.get("SCREENER_SHORTLIST_SIZE", "30"))
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

        # Step 4 — Convert to screener dicts
        screener_candidates = [_candidate_to_screener_dict(c) for c in shortlist]

        # Step 4b — Enrich missing market_cap_usd (ADR/foreign names often arrive with None)
        fmp_key      = os.environ.get("FMP_API_KEY", "")
        finnhub_key  = os.environ.get("FINNHUB_API_KEY", "")
        if fmp_key or finnhub_key:
            from services.playbook.strategy_screener.screener_enrichment import enrich_candidates
            screener_candidates = await enrich_candidates(screener_candidates, fmp_key, finnhub_key)
        else:
            print("[SCREENER] No FMP_API_KEY or FINNHUB_API_KEY — skipping market cap enrichment")

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


# ── CR-to-screener conversion helpers ────────────────────────────────────────

_CR_MCAP_HIDDEN: Dict[str, float] = {
    "micro_small": 78.0,
    "lower_mid":   70.0,
    "unknown":     65.0,
    "upper_mid":   48.0,
    "large_mega":  28.0,
}

_CR_LAYER_DEPTH_SCORE: Dict[int, float] = {
    0: 10.0,
    1: 25.0,
    2: 50.0,
    3: 70.0,
    4: 85.0,
}

_CR_LAYER_ROLE: Dict[int, str] = {
    0: "platform_anchor",
    1: "platform_anchor",
    2: "adjacent_supplier",
    3: "direct_bottleneck",
    4: "direct_bottleneck",
}

_CR_CONF_SCCS: Dict[str, float] = {
    "high":   82.0,
    "medium": 62.0,
    "low":    38.0,
}

_CR_CONF_COVERAGE: Dict[str, str] = {
    "high":   "full",
    "medium": "partial",
    "low":    "thin",
}


def _cr_row_to_candidate_dict(
    cr_row: Dict[str, Any],
    regime_context: Dict[str, Any],
    node_registry: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Convert one chain_reaction_weekly_outputs row into a screener candidate dict
    that is fully compatible with build_full_report() and the ReportPanel response shape.

    All narrative fields (thesis_summary, why_now, why_hidden, fit_reasoning, giant_anchors)
    are synthesised deterministically from the CR row + NODE_REGISTRY data.
    No LLM call — same philosophy as the existing Serenity pipeline.
    """
    ticker       = str(cr_row.get("bottleneck_ticker") or "").upper()
    company_name = cr_row.get("company_name") or ticker
    themes       = cr_row.get("themes") or [cr_row.get("anchor_theme") or "supply_chain"]
    country      = cr_row.get("country") or "US"
    exchange     = cr_row.get("exchange") or ("NASDAQ" if country == "US" else "OTC")
    market_cap_usd = cr_row.get("marketCap") or cr_row.get("market_cap")
    layer_depth  = int(cr_row.get("layer") or 2)
    bottleneck_score = float(cr_row.get("bottleneck_score") or 50.0)
    final_score      = float(cr_row.get("final_score") or 50.0)
    evidence         = cr_row.get("evidence") or []
    supply_chain_role = (
        cr_row.get("supply_chain_role")
        or cr_row.get("bottleneckReason")
        or ""
    )
    mcap_bucket  = cr_row.get("marketCapBucket") or "unknown"
    anchor_ticker = cr_row.get("anchor_ticker") or ""

    # NODE_REGISTRY enrichment — provides giant_anchors, us_access_proxy, confidence
    node: Dict[str, Any] = node_registry.get(ticker) or {}
    raw_anchors: list = list(node.get("giant_anchors") or [])
    if anchor_ticker and anchor_ticker not in raw_anchors:
        raw_anchors = [anchor_ticker] + raw_anchors
    giant_anchors = [str(a) for a in raw_anchors if a]

    us_access_proxy = node.get("us_access_proxy") or node.get("adr_ticker")
    confidence_str  = str(node.get("confidence") or "medium")

    # ── Score mappings ────────────────────────────────────────────────────────
    best_blend_score             = round(float(final_score), 1)
    bottleneck_criticality_score = round(float(bottleneck_score), 1)

    hidden_base   = _CR_MCAP_HIDDEN.get(mcap_bucket, 55.0)
    layer_bonus   = max(0.0, (layer_depth - 2) * 7.0)
    hiddenness_score = round(min(95.0, hidden_base + layer_bonus), 1)

    chain_depth_score            = round(_CR_LAYER_DEPTH_SCORE.get(layer_depth, 50.0), 1)
    supply_chain_confidence_score = round(_CR_CONF_SCCS.get(confidence_str, 62.0), 1)

    chain_role_type = _CR_LAYER_ROLE.get(layer_depth, "adjacent_supplier")
    coverage_status = _CR_CONF_COVERAGE.get(confidence_str, "partial")

    # ── Narrative synthesis ───────────────────────────────────────────────────
    thesis_summary = (supply_chain_role or "")[:240]
    fit_reasoning  = [str(e) for e in (evidence or [])[:4]]

    theme_str  = themes[0].replace("_", " ") if themes else "supply chain"
    regime_id  = (regime_context.get("regime_id") or "") if regime_context else ""
    why_now = (
        f"Structural supply chain constraint in {theme_str}. "
        + (
            f"Operating under {regime_id.replace('_', ' ')} regime context."
            if regime_id
            else "Bottleneck position identified via cross-theme chain reaction scoring."
        )
    )

    if mcap_bucket in ("micro_small", "lower_mid", "unknown") and layer_depth >= 3:
        why_hidden = (
            f"{company_name} operates at Layer {layer_depth} in the supply chain with "
            f"limited analyst coverage — hiddenness score {hiddenness_score:.0f}/100. "
            f"Small-to-mid market cap places it below most institutional screening thresholds."
        )
    elif country != "US":
        why_hidden = (
            f"{company_name} is a foreign-listed supplier (domicile: {country}) — absent from "
            f"major US indices and most retail screeners. "
            f"Hiddenness score {hiddenness_score:.0f}/100."
        )
    else:
        why_hidden = (
            f"Deep supply chain position at Layer {layer_depth} makes {company_name} structurally "
            f"invisible to top-down thematic ETFs and standard sector screens. "
            f"Hiddenness score {hiddenness_score:.0f}/100."
        )

    what_to_verify_next = (
        f"Verify supply chain positioning via earnings transcripts and SEC filings for {ticker}."
        + (f" Check {giant_anchors[0]} supplier disclosures." if giant_anchors else "")
    )

    what_would_break_thesis = (
        f"Thesis breaks if a platform giant qualifies an alternative supplier or if the "
        f"{theme_str} structural demand driver reverses."
        + (f" Monitor {giant_anchors[0]} capex announcements." if giant_anchors else "")
    )

    one_line_summary = build_one_line_summary({
        "company_name":  company_name,
        "ticker":        ticker,
        "themes":        themes,
        "giant_anchors": giant_anchors,
        "chain_role_type": chain_role_type,
        "thesis_summary":  thesis_summary,
    })

    grade = assign_grade(
        best_blend_score, confidence_str, hiddenness_score, bottleneck_criticality_score
    )

    return {
        # ── Core identity (ReportPanel required) ─────────────────────────────
        "ticker":                        ticker,
        "company_name":                  company_name,
        "country":                       country,
        "exchange":                      exchange,
        "market_cap_usd":                market_cap_usd,
        "theme":                         themes[0] if themes else "",
        "themes":                        themes,
        "chain_role_type":               chain_role_type,
        "layer_depth":                   layer_depth,
        # ── Scores ───────────────────────────────────────────────────────────
        "grade":                         grade,
        "best_blend_score":              best_blend_score,
        "bottleneck_criticality_score":  bottleneck_criticality_score,
        "hiddenness_score":              hiddenness_score,
        "chain_depth_score":             chain_depth_score,
        "supply_chain_confidence_score": supply_chain_confidence_score,
        # ── Coverage ─────────────────────────────────────────────────────────
        "coverage_status":               coverage_status,
        "data_confidence":               confidence_str,
        "us_access_proxy":               us_access_proxy,
        # ── Narrative (ReportPanel required) ─────────────────────────────────
        "one_line_summary":              one_line_summary,
        "giant_anchors":                 giant_anchors,
        "comparable_names":              [],
        "thesis_summary":                thesis_summary,
        "fit_reasoning":                 fit_reasoning,
        "why_now":                       why_now,
        "why_hidden":                    why_hidden,
        "what_to_verify_next":           what_to_verify_next,
        "what_would_break_thesis":       what_would_break_thesis,
        "coverage_notes":                (
            f"Source: chain_reaction_weekly + NODE_REGISTRY. "
            f"Confidence: {confidence_str}. "
            f"Discovery: {', '.join(cr_row.get('discovery_sources') or ['node_registry'])}."
        ),
        "crowding_flags":                [],
        "data_gaps":                     [],
        "chain_layers":                  [],
        # ── CR-origin metadata (additive — does not break ReportPanel) ───────
        "marketCapBucket":               mcap_bucket,
        "cr_bottleneck_score":           bottleneck_score,
        "cr_final_score":                final_score,
        "discovery_sources":             cr_row.get("discovery_sources") or ["node_registry"],
        "revenueSignal":                 cr_row.get("revenueSignal"),
        # ── Anchor label (minimal; derived from chain_role_type) ─────────────
        "is_anchor":                     chain_role_type == "platform_anchor",
        "role_type":                     "anchor" if chain_role_type == "platform_anchor" else "bottleneck",
    }


# ── CR-sourced snapshot generator ─────────────────────────────────────────────

async def generate_snapshot_from_cr(manual_override: bool = False) -> Dict[str, Any]:
    """
    Generates a strategy screener snapshot using chain_reaction_weekly_outputs as the
    candidate source instead of regime-locked run_discover().

    Produces the IDENTICAL snapshot/report shape as generate_snapshot() so that:
      - GET /api/strategy-screener/latest   → unchanged response shape
      - GET /api/strategy-screener/report/… → full ReportPanel payload for every row
      - screener_snapshots + screener_reports tables  → identical schema

    Candidate universe: build_cross_theme_top() — diversity-gated, cross-theme,
    hidden-gem-preserved — instead of regime-locked discovery.
    """
    global _generation_in_progress
    if _generation_in_progress:
        print("[SCREENER_CR] Generation already in progress — skipping")
        return {}

    _generation_in_progress = True
    now     = datetime.now(timezone.utc)
    snap_id = _snapshot_id_from_dt(now)
    _save_placeholder(snap_id, now, manual_override)

    try:
        # Step 1 — Regime context (used for report enrichment, NOT for candidate filtering)
        from services.playbook.regime_service import compute_serenity_regime
        regime = compute_serenity_regime()
        regime_slim = {
            "regime_id":         regime.regime_id,
            "label":             regime.label,
            "top_themes":        regime.top_themes,
            "top_anchors":       regime.top_anchors,
            "confidence":        regime.confidence,
            "why_now":           regime.why_now,
            "recommended_mode":  regime.recommended_mode,
            "recommended_depth": regime.recommended_depth,
        }
        print(f"[SCREENER_CR] Regime context: {regime.regime_id} | confidence={regime.confidence}")

        # Step 2 — Pull cross-theme diverse candidates from CR weekly output
        from services.chain_reaction_weekly_service import build_cross_theme_top
        vis = build_cross_theme_top(limit=SHORTLIST_SIZE, max_age_days=10)

        if vis.get("status") != "ok" or not vis.get("rows"):
            err = vis.get("error", "CR output unavailable or empty")
            print(f"[SCREENER_CR] CR data unavailable: {err}")
            _save_error(snap_id, now, f"CR source unavailable: {err}")
            return {}

        cr_rows = vis["rows"]
        dg      = vis.get("diversity_gate_result") or {}
        n_gems  = dg.get("hidden_gems_achieved", 0)
        print(
            f"[SCREENER_CR] CR source: {len(cr_rows)} diverse candidates "
            f"(themes={dg.get('themes_achieved', 0)}, gems={n_gems}, "
            f"small_mid={dg.get('small_mid_achieved', 0)})"
        )

        # Step 3 — Load NODE_REGISTRY once for batch enrichment
        node_registry: Dict[str, Any] = {}
        try:
            from services.playbook.supply_chain_graph import NODE_REGISTRY
            node_registry = NODE_REGISTRY or {}
        except Exception as _nre:
            print(f"[SCREENER_CR] NODE_REGISTRY load error: {_nre}")

        # Step 4 — Convert CR rows → fully-qualified screener candidate dicts
        screener_candidates = [
            _cr_row_to_candidate_dict(row, regime_slim, node_registry)
            for row in cr_rows
        ]

        # Step 5 — Build and persist full reports (identical to generate_snapshot flow)
        print(f"[SCREENER_CR] Building {len(screener_candidates)} reports...")
        for cand_dict in screener_candidates:
            try:
                report = build_full_report(
                    candidate=cand_dict,
                    snapshot_id=snap_id,
                    regime_context=regime_slim,
                    analyze_result=None,
                )
                save_report(snap_id, cand_dict["ticker"], report)
            except Exception as _re:
                print(f"[SCREENER_CR] Report error for {cand_dict.get('ticker', '?')}: {_re}")

        # Step 6 — Build summary text
        top3       = [c["company_name"] for c in screener_candidates[:3]]
        themes_str = ", ".join(
            t.replace("_", " ") for t in (vis.get("themes_in_visible") or [])[:4]
        )
        summary = (
            f"Serenity Chain Reaction — {regime.label}. "
            f"Cross-theme universe: {dg.get('themes_achieved', 0)} themes, "
            f"{n_gems} Phase-6 hidden gems. "
            f"Active: {themes_str}. "
            f"Lead names: {', '.join(top3)}."
        )

        # Step 7 — Persist snapshot (identical schema to generate_snapshot output)
        snapshot: Dict[str, Any] = {
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
            "generation_notes": (
                f"CR-sourced: {len(cr_rows)} diverse candidates from chain_reaction_weekly_outputs. "
                f"Diversity gate: {dg.get('themes_achieved', 0)} themes, "
                f"{n_gems} hidden gems, {dg.get('small_mid_achieved', 0)} small/mid-cap. "
                f"Supersedes regime-locked discovery."
            ),
            "manual_override": manual_override,
        }
        save_snapshot(snapshot)
        print(
            f"[SCREENER_CR] Snapshot complete: {snap_id} "
            f"({len(screener_candidates)} candidates, {n_gems} hidden gems)"
        )
        return snapshot

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[SCREENER_CR] Snapshot FAILED: {e}")
        _save_error(snap_id, now, str(e))
        return {}
    finally:
        _generation_in_progress = False
