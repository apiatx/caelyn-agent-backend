"""
Serenity Regime Service — deterministic theme/anchor scoring for Auto Serenity.

Decides at request time which theme cluster and anchor platform the discovery
engine should prioritize when running in Auto mode.

Design principles:
  - Fully deterministic — same inputs produce same output
  - No external API calls required for base regime computation
  - Uses existing NODE_REGISTRY, THEME_TAXONOMY, and GIANT_MAP data
  - Fast — all computation from in-memory dicts
  - Cheap — safe to call on every Auto Serenity request

Output: SerenityRegime (see regime_types.py)

NOT a generic macro engine. Purely for Serenity discovery path selection.
Zero coupling to /api/query or Default mode.
"""
from __future__ import annotations

import re
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

from services.playbook.regime_types import (
    AnchorRegimeScore,
    SerenityRegime,
    ThemeRegimeScore,
)


# ── Regime label lookup ────────────────────────────────────────────────────────
# Maps frozensets of 2-3 top theme IDs to deterministic (regime_id, label) pairs.

_REGIME_LABEL_MAP: Dict[FrozenSet[str], Tuple[str, str]] = {
    frozenset({"advanced_packaging_test", "semicap_supply_chain"}): (
        "ai_packaging_bottlenecks",
        "AI Packaging & Tooling Bottlenecks",
    ),
    frozenset({"advanced_packaging_test", "photonics_cpo"}): (
        "ai_packaging_photonics",
        "AI Packaging & Photonics Convergence",
    ),
    frozenset({"semicap_supply_chain", "photonics_cpo"}): (
        "semicap_photonics_chokepoints",
        "Semicap & Photonics Chokepoints",
    ),
    frozenset({"advanced_packaging_test", "semicap_supply_chain", "photonics_cpo"}): (
        "ai_hardware_core_bottlenecks",
        "AI Hardware Core Bottlenecks",
    ),
    frozenset({"ai_power_energy", "grid_transformers"}): (
        "ai_power_grid_buildout",
        "AI Power & Grid Infrastructure",
    ),
    frozenset({"ai_power_energy", "semicap_supply_chain"}): (
        "ai_power_semicap",
        "AI Power & Semicap Convergence",
    ),
    frozenset({"defense_optics", "advanced_packaging_test"}): (
        "defense_ai_supply_chain",
        "Defense & AI Supply Chain",
    ),
    frozenset({"defense_optics", "semicap_supply_chain"}): (
        "defense_semicap",
        "Defense & Semicap Bottlenecks",
    ),
    frozenset({"ai_power_energy", "advanced_packaging_test"}): (
        "ai_power_packaging",
        "AI Power & Advanced Packaging",
    ),
    frozenset({"semicap_supply_chain", "defense_optics"}): (
        "semicap_defense_chokepoints",
        "Semicap & Defense Chokepoints",
    ),
}


# ── Scoring helpers ────────────────────────────────────────────────────────────

def _policy_text_score(text: str) -> float:
    """
    Parse policy_linkage text and return 0-100 policy tailwind score.
    Additive: each keyword adds points; capped at 100.
    """
    score = 0.0
    t = text.lower()
    if "chips act" in t:
        score += 30.0
    if "export control" in t:
        score += 20.0
    if "ira" in t or "inflation reduction" in t:
        score += 20.0
    if "infrastructure" in t:
        score += 15.0
    if "defense" in t or "ndaa" in t:
        score += 25.0
    if "nuclear" in t or "clean power" in t:
        score += 10.0
    if "energy efficiency" in t:
        score += 8.0
    return min(100.0, score)


def _capex_score(capex_text: str) -> float:
    """
    Parse a capex description string and return a 0-100 scale score.
    Handles trillions ($1T), billions ($104B), and ranges ($60-65B).
    """
    text = capex_text

    # Trillions: $1T → treat as $1000B
    t_match = re.search(r"\$(\d+(?:\.\d+)?)\s*[Tt]", text)
    if t_match:
        amt_b = float(t_match.group(1)) * 1000.0
        return min(100.0, amt_b * 0.05)

    # Billion range: $60-65B
    range_match = re.search(r"\$(\d+)[-\u2013](\d+)\s*[Bb]", text)
    if range_match:
        amt = (float(range_match.group(1)) + float(range_match.group(2))) / 2.0
        return min(100.0, max(0.0, amt * 0.9))

    # Single value: $104B or $40B+
    b_matches = re.findall(r"\$(\d+(?:\.\d+)?)\s*[Bb]", text)
    if b_matches:
        amt = max(float(m) for m in b_matches)
        return min(100.0, max(0.0, amt * 0.9))

    return 30.0  # unknown but non-zero capex


# ── Theme scoring ──────────────────────────────────────────────────────────────

def _score_themes() -> List[ThemeRegimeScore]:
    """
    Score all themes in THEME_TAXONOMY using existing NODE_REGISTRY data.
    Returns a list sorted descending by regime_score.

    Scoring weights:
      avg_bottleneck_score   30% — raw bottleneck quality signal
      candidate_density      20% — richness (how many nodes in this theme)
      hiddenness_quality     20% — avg layer depth (layer 3+ = hidden opportunity)
      policy_score           15% — external catalyst / policy tailwind
      anchor_density_score   10% — institutional relevance (# giant anchors)
      country_diversity_score 5% — geographic breadth (more = less crowded)

    Adjustments:
      +8  serenity_priority=="high" bonus
       0  serenity_priority=="medium"
      -12 serenity_priority=="low"
      -8  avg_layer < 2.0 crowding penalty (theme dominated by obvious layer-1 names)
      -5  country_diversity < 3 crowding penalty (geographically concentrated)
    """
    from services.playbook.supply_chain_graph import NODE_REGISTRY
    from services.playbook.theme_discovery import THEME_TAXONOMY

    # Aggregate node stats per theme
    theme_stats: Dict[str, Dict[str, Any]] = {
        theme_id: {"nodes": 0, "bs": [], "layers": [], "countries": set()}
        for theme_id in THEME_TAXONOMY
    }
    for node in NODE_REGISTRY.values():
        if node is None:
            continue
        for theme_id in node.get("themes", []):
            if theme_id not in theme_stats:
                theme_stats[theme_id] = {"nodes": 0, "bs": [], "layers": [], "countries": set()}
            s = theme_stats[theme_id]
            s["nodes"] += 1
            s["bs"].append(float(node.get("bottleneck_score", 50)))
            s["layers"].append(float(node.get("layer", 2)))
            s["countries"].add(node.get("country", "US"))

    max_density = max((s["nodes"] for s in theme_stats.values() if s["nodes"] > 0), default=1)

    results: List[ThemeRegimeScore] = []
    for theme_id, meta in THEME_TAXONOMY.items():
        s = theme_stats.get(theme_id, {"nodes": 0, "bs": [], "layers": [], "countries": set()})
        n         = s["nodes"]
        bs_list   = s["bs"]
        lay_list  = s["layers"]
        countries = s["countries"]

        avg_bs    = sum(bs_list)   / len(bs_list)   if bs_list  else 50.0
        avg_layer = sum(lay_list)  / len(lay_list)  if lay_list else 2.0

        density_score     = min(100.0, (n / max(max_density, 1)) * 100.0)
        hiddenness_quality = max(0.0, min(100.0, (avg_layer - 1.0) / 3.0 * 100.0))
        policy_score      = _policy_text_score(meta.get("policy_linkage") or "")
        anchor_count      = len(meta.get("giant_anchors", []))
        anchor_score      = min(100.0, anchor_count / 6.0 * 100.0)
        country_div       = len(countries)
        country_score     = min(100.0, max(0.0, (country_div - 1) / 7.0 * 100.0))

        priority    = meta.get("serenity_priority", "low")
        priority_adj = {"high": 8.0, "medium": 0.0, "low": -12.0}.get(priority, 0.0)

        crowding_penalty = 0.0
        if avg_layer < 2.0:
            crowding_penalty += 8.0
        if country_div < 3:
            crowding_penalty += 5.0

        raw = (
            avg_bs         * 0.30 +
            density_score  * 0.20 +
            hiddenness_quality * 0.20 +
            policy_score   * 0.15 +
            anchor_score   * 0.10 +
            country_score  * 0.05
        )
        regime_score = max(0.0, min(100.0, raw + priority_adj - crowding_penalty))

        results.append(ThemeRegimeScore(
            theme_id=theme_id,
            label=meta.get("label", theme_id),
            regime_score=round(regime_score, 1),
            candidate_density=n,
            avg_bottleneck_score=round(avg_bs, 1),
            hiddenness_quality=round(hiddenness_quality, 1),
            policy_score=round(policy_score, 1),
            anchor_density=anchor_count,
            country_diversity=country_div,
            serenity_priority=priority,
            crowding_penalty=round(crowding_penalty, 1),
        ))

    return sorted(results, key=lambda x: x.regime_score, reverse=True)


# ── Anchor scoring ─────────────────────────────────────────────────────────────

def _score_anchors(top_theme_ids: List[str]) -> List[AnchorRegimeScore]:
    """
    Score giant anchors relative to the current top-regime theme cluster.
    Returns a list sorted descending by regime_score.

    Scoring weights:
      theme_overlap_frac   50% — fraction of top-regime themes this anchor covers
      capex_scale          30% — capex scale score (0-100)
      candidate_quality    20% — avg bottleneck_score of nodes listing this anchor
    """
    from services.playbook.giant_map import GIANT_MAP
    from services.playbook.supply_chain_graph import NODE_REGISTRY

    top_theme_set = set(top_theme_ids)

    # Build per-anchor node bottleneck stats from NODE_REGISTRY
    anchor_node_bs: Dict[str, List[float]] = {}
    for node in NODE_REGISTRY.values():
        if node is None:
            continue
        for anchor_id in node.get("giant_anchors", []):
            if anchor_id not in anchor_node_bs:
                anchor_node_bs[anchor_id] = []
            anchor_node_bs[anchor_id].append(float(node.get("bottleneck_score", 50)))

    results: List[AnchorRegimeScore] = []
    for anchor_id, meta in GIANT_MAP.items():
        anchor_themes    = set(meta.get("themes", []))
        overlap_count    = len(anchor_themes & top_theme_set)
        overlap_frac     = overlap_count / max(len(top_theme_set), 1)

        capex_sc         = _capex_score(meta.get("capex_scale", ""))

        node_scores      = anchor_node_bs.get(anchor_id, [])
        cand_quality     = sum(node_scores) / len(node_scores) if node_scores else 50.0

        foreign_count    = len(meta.get("foreign_exposure", []))

        regime_score = min(100.0, max(0.0,
            overlap_frac          * 50.0 +
            (capex_sc  / 100.0)   * 30.0 +
            (cand_quality / 100.0) * 20.0
        ))

        results.append(AnchorRegimeScore(
            anchor_id=anchor_id,
            name=meta.get("name", anchor_id),
            regime_score=round(regime_score, 1),
            theme_overlap_count=overlap_count,
            overlapping_theme_ids=sorted(anchor_themes & top_theme_set),
            capex_scale_score=round(capex_sc, 1),
            candidate_quality=round(cand_quality, 1),
            foreign_exposure_count=foreign_count,
        ))

    return sorted(results, key=lambda x: x.regime_score, reverse=True)


# ── Regime label ───────────────────────────────────────────────────────────────

def _pick_regime_label(top_themes: List[str]) -> Tuple[str, str]:
    """Return (regime_id, label) from top themes using lookup, then fallback."""
    if not top_themes:
        return ("serenity_auto", "Serenity Auto Discovery")

    top2 = frozenset(top_themes[:2])
    top3 = frozenset(top_themes[:3])

    if top3 in _REGIME_LABEL_MAP:
        return _REGIME_LABEL_MAP[top3]
    if top2 in _REGIME_LABEL_MAP:
        return _REGIME_LABEL_MAP[top2]

    slug = top_themes[0].replace("_", " ").title()
    return (f"serenity_{top_themes[0]}", f"{slug} Focus")


# ── Explanation builders ───────────────────────────────────────────────────────

def _build_why_now(
    top_ts: List[ThemeRegimeScore],
    top_as: List[AnchorRegimeScore],
) -> List[str]:
    """Build human-readable why_now bullets from top theme/anchor scores."""
    reasons: List[str] = []
    if not top_ts:
        return reasons

    t0 = top_ts[0]
    reasons.append(
        f"{t0.label} has the strongest current bottleneck evidence: "
        f"{t0.candidate_density} nodes, avg bottleneck score {t0.avg_bottleneck_score:.0f}/100."
    )
    if t0.policy_score >= 30:
        reasons.append(
            f"Strong policy tailwind active for {t0.label} "
            f"(policy score {t0.policy_score:.0f}/100 — CHIPS Act and/or export controls)."
        )
    if top_as and top_as[0].theme_overlap_count >= 2:
        names = [a.anchor_id for a in top_as[:3] if a.theme_overlap_count >= 1]
        if names:
            reasons.append(
                f"Multiple platform giants ({', '.join(names)}) converge on "
                f"the same constrained supplier layers."
            )
    if t0.hiddenness_quality >= 50:
        reasons.append(
            f"Deep supply chain positioning (hiddenness quality {t0.hiddenness_quality:.0f}/100) "
            f"creates institutional blind spot opportunity."
        )
    if t0.country_diversity >= 4:
        reasons.append(
            f"International supply chain exposure spans {t0.country_diversity} countries — "
            f"includes foreign bottleneck specialists not captured by US-only screens."
        )
    if len(top_ts) >= 2 and top_ts[1].regime_score >= 65:
        t1 = top_ts[1]
        reasons.append(
            f"{t1.label} adds reinforcing signal "
            f"(regime score {t1.regime_score:.0f}/100, {t1.candidate_density} nodes)."
        )
    return reasons[:5]


def _build_evidence_signals(
    theme_scores: List[ThemeRegimeScore],
    anchor_scores: List[AnchorRegimeScore],
) -> List[str]:
    signals: List[str] = []
    for ts in theme_scores[:4]:
        signals.append(
            f"theme_strength: {ts.theme_id} = {ts.regime_score:.0f} "
            f"({ts.candidate_density} nodes, avg_bs={ts.avg_bottleneck_score:.0f}, "
            f"policy={ts.policy_score:.0f})"
        )
    for a in anchor_scores[:3]:
        signals.append(
            f"anchor_relevance: {a.anchor_id} = {a.regime_score:.0f} "
            f"(theme_overlap={a.theme_overlap_count}, capex={a.capex_scale_score:.0f})"
        )
    return signals


def _build_rejected_paths(theme_scores: List[ThemeRegimeScore]) -> List[str]:
    rejected: List[str] = []
    for ts in theme_scores[3:7]:
        factors = {
            "candidate density":  ts.candidate_density < 10,
            "policy signal":      ts.policy_score < 20,
            "hiddenness quality": ts.hiddenness_quality < 40,
            "anchor convergence": ts.anchor_density < 2,
        }
        weak = [k for k, v in factors.items() if v]
        reason = f"lower {weak[0]}" if weak else f"lower overall regime score ({ts.regime_score:.0f}/100)"
        rejected.append(f"{ts.label}: strong but {reason} today")
    return rejected


def _top_regions(top_themes: List[str]) -> List[str]:
    """Return top countries by node count across the top-regime themes."""
    from services.playbook.supply_chain_graph import NODE_REGISTRY
    theme_set = set(top_themes)
    country_count: Dict[str, int] = {}
    for node in NODE_REGISTRY.values():
        if node is None:
            continue
        if not (set(node.get("themes", [])) & theme_set):
            continue
        c = node.get("country", "US")
        country_count[c] = country_count.get(c, 0) + 1
    return [c for c, _ in sorted(country_count.items(), key=lambda x: -x[1])][:5]


# ── Main entry point ───────────────────────────────────────────────────────────

def compute_serenity_regime() -> SerenityRegime:
    """
    Compute the current Serenity regime selection.

    Deterministic — no external API calls. Uses only:
      - NODE_REGISTRY (supply_chain_graph.py)
      - THEME_TAXONOMY (theme_discovery.py)
      - GIANT_MAP (giant_map.py)

    Returns a SerenityRegime describing:
      - Which theme cluster to prioritize
      - Which giant anchors are most relevant
      - Why this path was chosen
      - What lower-priority paths were considered
    """
    from datetime import datetime, timezone

    theme_scores  = _score_themes()
    top_theme_ids = [ts.theme_id for ts in theme_scores[:3]]

    anchor_scores  = _score_anchors(top_theme_ids)
    # Only surface real ticker anchors in top_anchors (filter synthetic anchor IDs)
    ticker_anchors = [
        a for a in anchor_scores
        if a.anchor_id.isupper() and len(a.anchor_id) <= 5
    ]
    top_anchor_ids = [a.anchor_id for a in ticker_anchors[:3]]

    regime_id, label = _pick_regime_label(top_theme_ids)

    top_score  = theme_scores[0].regime_score if theme_scores else 50.0
    confidence = "high" if top_score >= 70 else "medium" if top_score >= 55 else "low"

    top_regions    = _top_regions(top_theme_ids)
    why_now        = _build_why_now(theme_scores[:3], anchor_scores[:3])
    evidence_sigs  = _build_evidence_signals(theme_scores, anchor_scores)
    rejected       = _build_rejected_paths(theme_scores)

    theme_labels_str = ", ".join(ts.label for ts in theme_scores[:3])
    summary = (
        f"Serenity is prioritizing {theme_labels_str} because these chains have "
        f"the strongest current evidence quality and bottleneck density. "
        f"Top anchors: {', '.join(top_anchor_ids) or 'N/A'}. "
        f"Confidence: {confidence}."
    )

    return SerenityRegime(
        regime_id=regime_id,
        label=label,
        summary=summary,
        top_themes=top_theme_ids,
        top_anchors=top_anchor_ids,
        top_regions=top_regions,
        recommended_mode="theme_scan",
        recommended_depth=3,
        confidence=confidence,
        why_now=why_now,
        evidence_signals=evidence_sigs,
        rejected_or_lower_priority_paths=rejected,
        theme_scores=theme_scores,
        anchor_scores=anchor_scores,
        auto_mode_used=True,
        computed_at=datetime.now(timezone.utc).isoformat(),
    )
