"""
Compare Service — Serenity vs S&J Capital consensus analysis.

Entry point:
  run_compare(req: CompareRequest) -> CompareResponse

For each ticker the service computes:
  - Serenity score  (from curated NODE_REGISTRY discovery composite)
  - S&J score       (from playbook scoring engine)
  - score delta
  - classification  (serenity_only | sj_only | consensus | low_fit_both)
  - explanation of divergence

Serenity score is derived from the curated discovery composite (bottleneck_criticality,
chain_depth, hiddenness, supply_chain_confidence) — not LLM inference.
If a ticker is NOT in the curated NODE_REGISTRY it has no Serenity profile; score = None.

S&J score uses the standard playbook scoring engine (existing factors).

Guardrails:
  - Does NOT change S&J philosophy or factor weights
  - Does NOT modify /api/query
  - No LLM inference in classification or explanation
  - Results are deterministic from factor scores
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Set

from services.playbook.discovery_types import (
    CompareRequest,
    CompareResponse,
    CompareTickerResult,
)
from services.playbook.supply_chain_graph import NODE_REGISTRY


# ── Thresholds ────────────────────────────────────────────────────────────────

SERENITY_PASS_THRESHOLD = 62.0   # discovery composite >= this → Serenity favors
SJ_PASS_THRESHOLD       = 60.0   # S&J final_score >= this → SJ favors


# ── Serenity composite from node registry ─────────────────────────────────────

def _serenity_composite_for_ticker(ticker: str) -> Optional[tuple[float, Dict[str, Any]]]:
    """
    Compute Serenity discovery composite score for a ticker.
    Returns (score, breakdown) or None if ticker not in NODE_REGISTRY.

    Score = bottleneck_criticality(35%) + chain_depth(25%) + hiddenness(20%) + confidence(20%)
    Uses Phase 4 best_blend_score weight structure for consistency.
    """
    from services.playbook.discovery_service import (
        _chain_depth_score,
        _supply_chain_confidence_score,
        _hiddenness_score,
    )

    upper = ticker.upper()

    # Try direct match
    node = NODE_REGISTRY.get(upper)
    native_ticker = upper

    # Try by us_access_proxy (ADR)
    if node is None:
        for t, n in NODE_REGISTRY.items():
            proxy = n.get("us_access_proxy") if n else None
            if proxy and proxy.upper() == upper:
                node = n
                native_ticker = t
                break

    if node is None:
        return None

    country  = node.get("country", "US")
    layer    = node.get("layer", 2)
    bc_score = float(node.get("bottleneck_score", 50))

    hidden_score, hidden_reason = _hiddenness_score(
        upper, country, None, node.get("confidence", "medium"), node
    )
    conf_score  = _supply_chain_confidence_score(node)
    depth_score = _chain_depth_score(layer)

    # Phase 4 best_blend weights (consistent with discovery_service._compute_best_blend_score)
    from services.playbook.discovery_service import (
        _theme_purity_score,
        _giant_dependency_score,
        _proxy_accessibility_score,
        _compute_confidence_penalties,
        _compute_best_blend_score,
        _compute_chain_role_type,
        _compute_visibility_bucket,
    )
    from services.playbook.discovery_types import DiscoveryScores

    us_access_proxy = node.get("us_access_proxy")
    adr_ticker      = node.get("adr_ticker")
    themes          = node.get("themes", [])

    from services.playbook.foreign_market_map import FOREIGN_ACCESS_MAP
    if country != "US":
        fm    = FOREIGN_ACCESS_MAP.get(native_ticker, {})
        cov   = fm.get("coverage_status", "partial")
        dconf = fm.get("data_confidence", "medium")
    else:
        cov   = "full"
        dconf = "high"

    conf_penalties = _compute_confidence_penalties(
        upper, country, None, cov, dconf, adr_ticker, us_access_proxy
    )

    scores = DiscoveryScores(
        chain_depth_score=depth_score,
        bottleneck_criticality_score=bc_score,
        hiddenness_score=hidden_score,
        giant_dependency_score=_giant_dependency_score(node),
        supply_chain_confidence_score=conf_score,
        theme_purity_score=_theme_purity_score(themes),
        proxy_accessibility_score=_proxy_accessibility_score(country, bool(us_access_proxy), adr_ticker),
    )

    best_blend = _compute_best_blend_score(scores, dconf, conf_penalties)
    chain_role = _compute_chain_role_type(layer, bc_score)
    vis_bucket = _compute_visibility_bucket(upper, country, None, cov, node)

    breakdown = {
        "bottleneck_criticality_score": bc_score,
        "chain_depth_score":            depth_score,
        "hiddenness_score":             hidden_score,
        "supply_chain_confidence_score": conf_score,
        "best_blend_score":             best_blend,
        "chain_role_type":              chain_role,
        "visibility_bucket":            vis_bucket,
        "layer_depth":                  layer,
        "themes":                       themes,
        "giant_anchors":                node.get("giant_anchors", []),
        "hiddenness_reason":            hidden_reason,
        "in_node_registry":             True,
    }
    return best_blend, breakdown


# ── S&J scoring for a ticker ──────────────────────────────────────────────────

async def _sj_score_for_ticker(ticker: str, fmp_key: str, finnhub_key: str) -> Optional[tuple[float, Dict[str, Any]]]:
    """
    Score a ticker against S&J Capital playbook using the standard scoring engine.
    Returns (final_score_0_100, breakdown) or None on failure.
    """
    try:
        from services.playbook.playbook_registry import get as get_playbook
        from services.playbook.playbook_scoring import score_ticker

        pb = get_playbook("sjcapital")
        if pb is None:
            return None

        result = await score_ticker(ticker.upper(), pb, fmp_key, finnhub_key)
        if result is None:
            return None

        breakdown = {
            "final_score":       result.final_score,
            "hard_filter_pass":  result.hard_filter_pass,
            "factor_scores":     result.factor_scores,
            "penalties_applied": result.penalties_applied,
            "matched_themes":    result.matched_themes,
            "bottleneck_tags":   result.bottleneck_tags,
            "supply_chain_tags": result.supply_chain_tags,
        }
        return result.final_score, breakdown
    except Exception as e:
        print(f"[COMPARE] S&J score error for {ticker}: {e}")
        return None


# ── Classification logic ──────────────────────────────────────────────────────

def _classify(
    serenity_score: Optional[float],
    sj_score: Optional[float],
) -> tuple[str, bool, bool]:
    """
    Classify ticker as serenity_only | sj_only | consensus | low_fit_both.
    Returns (classification, serenity_pass, sj_pass).
    """
    s_pass = serenity_score is not None and serenity_score >= SERENITY_PASS_THRESHOLD
    j_pass = sj_score      is not None and sj_score      >= SJ_PASS_THRESHOLD

    if s_pass and j_pass:
        return "consensus", True, True
    if s_pass and not j_pass:
        return "serenity_only", True, False
    if j_pass and not s_pass:
        return "sj_only", False, True
    return "low_fit_both", False, False


def _explain(
    ticker: str,
    classification: str,
    serenity_score: Optional[float],
    sj_score: Optional[float],
    serenity_bd: Dict[str, Any],
    sj_bd: Dict[str, Any],
) -> str:
    """Generate a deterministic explanation for why two lenses diverge."""
    s = f"{serenity_score:.1f}" if serenity_score is not None else "N/A"
    j = f"{sj_score:.1f}"      if sj_score      is not None else "N/A"
    role  = serenity_bd.get("chain_role_type", "unknown")
    vis   = serenity_bd.get("visibility_bucket", "unknown")
    layer = serenity_bd.get("layer_depth", "?")
    themes = serenity_bd.get("themes", [])
    theme_str = themes[0] if themes else "supply-chain"
    hard = sj_bd.get("hard_filter_pass")
    sj_factors = sj_bd.get("factor_scores", {})

    if classification == "consensus":
        return (
            f"{ticker} scores well on both lenses (Serenity: {s}, S&J: {j}). "
            f"Serenity surfaces it as a {role} in {theme_str} (layer {layer}, visibility: {vis}). "
            f"S&J confirms via fundamental factor alignment."
        )

    if classification == "serenity_only":
        sj_weakness = ""
        if hard is False:
            sj_weakness = "S&J hard filter did not pass (fundamental screen failed). "
        elif sj_score is not None and sj_score < SJ_PASS_THRESHOLD:
            low_factors = [k for k, v in sj_factors.items() if isinstance(v, (int, float)) and v < 40][:2]
            if low_factors:
                sj_weakness = f"S&J weak on: {', '.join(low_factors)}. "
        return (
            f"{ticker} is a Serenity-only discovery (Serenity: {s}, S&J: {j}). "
            f"Chain logic: {role} in {theme_str} at layer {layer}. "
            f"{sj_weakness}"
            f"Serenity favors it for supply-chain structure; S&J fundamental screen is less enthusiastic."
        )

    if classification == "sj_only":
        if serenity_score is None:
            serenity_note = f"{ticker} has no curated Serenity chain profile — not in discovery registry."
        else:
            serenity_note = (
                f"Serenity sees it as {vis} (score {s}) — chain position or hiddenness is lower priority."
            )
        return (
            f"{ticker} is an S&J-only pick (Serenity: {s}, S&J: {j}). "
            f"{serenity_note} "
            f"S&J's fundamental factors (growth, balance sheet, theme alignment) favor it."
        )

    # low_fit_both
    s_note = f"Serenity score {s} (below {SERENITY_PASS_THRESHOLD:.0f} threshold)" if serenity_score is not None else "No Serenity chain profile"
    j_note = f"S&J score {j} (below {SJ_PASS_THRESHOLD:.0f} threshold)"    if sj_score      is not None else "S&J score unavailable"
    return (
        f"{ticker} is a low-fit on both lenses. "
        f"{s_note}. {j_note}. "
        f"Consider passing unless a specific catalyst or data update changes the picture."
    )


# ── Phase 5 — Consensus strength and disagreement reason ──────────────────────

def _compute_consensus_strength(
    result: "CompareTickerResult",
) -> tuple[Optional[str], str]:
    """
    Derive consensus_strength and disagreement_reason deterministically.

    consensus_strength:
      "strong"    — both pass AND delta < 10
      "moderate"  — both pass AND 10 <= delta < 20
      "borderline"— both pass AND 20 <= delta < 25, or one pass within 5 of threshold
      None        — not consensus (different classifications) or both fail

    disagreement_reason:
      - For consensus: empty string (agreement, no divergence)
      - For serenity_only: explain why S&J falls short
      - For sj_only: explain why Serenity misses it
      - For high disagreement (delta >= 25): specific driver
    """
    from typing import Optional  # local re-import for type checking
    s = result.serenity_score
    j = result.sj_score
    delta = result.score_delta  # serenity - sj (can be negative)
    cls   = result.classification

    consensus_strength: Optional[str] = None
    disagreement_reason: str = ""

    if cls == "consensus":
        abs_delta = abs(delta) if delta is not None else 0.0
        if abs_delta < 10:
            consensus_strength = "strong"
        elif abs_delta < 20:
            consensus_strength = "moderate"
        else:
            consensus_strength = "borderline"
        disagreement_reason = ""  # consensus — no disagreement to explain

    elif cls == "serenity_only":
        if j is not None and s is not None:
            gap = s - j
            if gap >= 25:
                disagreement_reason = (
                    f"Large divergence ({gap:.1f} pts). Serenity favors supply-chain structure and hiddenness; "
                    f"S&J fundamental screen (growth/balance sheet) finds this name below its threshold."
                )
            else:
                disagreement_reason = (
                    f"Serenity scores higher ({s:.1f} vs {j:.1f}). S&J fundamental factors are marginal or "
                    f"hard filter did not pass — chain position strong but fundamentals not confirmed."
                )
        else:
            disagreement_reason = (
                "S&J score unavailable or ticker not scored — Serenity chain profile favors this name "
                "but S&J confirmation is missing."
            )

    elif cls == "sj_only":
        if s is None:
            disagreement_reason = (
                "Not in Serenity curated supply chain registry — no chain profile to confirm. "
                "S&J fundamental score qualifies it, but supply-chain positioning is uncharted."
            )
        else:
            gap = j - s if j is not None else 0.0  # type: ignore
            disagreement_reason = (
                f"S&J scores higher ({j:.1f} vs {s:.1f}). Supply-chain hiddenness or bottleneck criticality "
                f"is insufficient for Serenity's chain-depth filter despite strong fundamentals."
            )

    elif cls == "low_fit_both":
        if s is not None and j is not None:
            disagreement_reason = (
                f"Neither lens reaches pass threshold (Serenity: {s:.1f}/{SERENITY_PASS_THRESHOLD:.0f}, "
                f"S&J: {j:.1f}/{SJ_PASS_THRESHOLD:.0f}). Chain position and fundamentals both marginal."
            )
        else:
            disagreement_reason = "Both lenses return below-threshold or unavailable scores."

    return consensus_strength, disagreement_reason


# ── Main compare orchestrator ─────────────────────────────────────────────────

async def run_compare(req: CompareRequest) -> CompareResponse:
    """
    Compare tickers against Serenity and S&J Capital lenses.
    Returns per-ticker scores, classification, and consensus summary.
    """
    fmp_key      = os.getenv("FMP_API_KEY", "")
    finnhub_key  = os.getenv("FINNHUB_API_KEY", "")

    tickers = [t.upper().strip() for t in req.tickers if t.strip()][:30]
    results: List[CompareTickerResult] = []

    for ticker in tickers:
        # Serenity score (synchronous — from curated maps)
        serenity_result = _serenity_composite_for_ticker(ticker)
        if serenity_result:
            serenity_score, serenity_bd = serenity_result
            in_registry = True
        else:
            serenity_score, serenity_bd, in_registry = None, {}, False

        # S&J score (async — calls scoring engine)
        sj_result = await _sj_score_for_ticker(ticker, fmp_key, finnhub_key)
        if sj_result:
            sj_score, sj_bd = sj_result
        else:
            sj_score, sj_bd = None, {}

        delta = None
        if serenity_score is not None and sj_score is not None:
            delta = round(serenity_score - sj_score, 1)

        classification, s_pass, j_pass = _classify(serenity_score, sj_score)
        explanation = _explain(ticker, classification, serenity_score, sj_score, serenity_bd, sj_bd)

        serenity_out = serenity_bd if req.include_breakdown else {}
        sj_out       = sj_bd       if req.include_breakdown else {}

        results.append(CompareTickerResult(
            ticker=ticker,
            serenity_score=round(serenity_score, 1) if serenity_score is not None else None,
            sj_score=round(sj_score, 1)             if sj_score      is not None else None,
            score_delta=delta,
            classification=classification,
            serenity_pass=s_pass,
            sj_pass=j_pass,
            explanation=explanation,
            serenity_breakdown=serenity_out,
            sj_breakdown=sj_out,
            in_node_registry=in_registry,
        ))

    # Phase 5 — annotate each result with consensus_strength + disagreement_reason
    for r in results:
        r.consensus_strength, r.disagreement_reason = _compute_consensus_strength(r)

    consensus             = [r.ticker for r in results if r.classification == "consensus"]
    serenity_only         = [r.ticker for r in results if r.classification == "serenity_only"]
    sj_only               = [r.ticker for r in results if r.classification == "sj_only"]
    low_fit               = [r.ticker for r in results if r.classification == "low_fit_both"]
    high_disagreement     = [r.ticker for r in results
                             if r.score_delta is not None and abs(r.score_delta) >= 25.0]

    playbooks_used = list(set(req.playbooks)) or ["serenity", "sjcapital"]

    return CompareResponse(
        tickers_compared=tickers,
        playbooks=playbooks_used,
        results=results,
        consensus_names=consensus,
        serenity_only_names=serenity_only,
        sj_only_names=sj_only,
        low_fit_both=low_fit,
        high_disagreement_names=high_disagreement,
        meta={
            "total_compared":             len(results),
            "consensus_count":            len(consensus),
            "serenity_only_count":        len(serenity_only),
            "sj_only_count":              len(sj_only),
            "low_fit_count":              len(low_fit),
            "high_disagreement_count":    len(high_disagreement),
            "serenity_pass_threshold":    SERENITY_PASS_THRESHOLD,
            "sj_pass_threshold":          SJ_PASS_THRESHOLD,
            "include_breakdown":          req.include_breakdown,
        },
    )
