"""
Deterministic Playbook Explainer.

Generates structured explanation fields from factor scores + metadata.
No LLM dependency — all logic is deterministic.

Produces:
  thesis_summary           — 1-sentence summary of best and worst signals
  fit_reasoning            — list of reasons this name fits the playbook
  non_fit_reasoning        — list of reasons this name doesn't fit
  key_confirming_signals   — strongest positive signals (scores >= 70)
  top_risks                — most important risk observations
  what_would_improve_score — actionable items that could boost score
  what_would_break_thesis  — events that would invalidate the thesis
  supply_chain_tags        — direct supply chain confirmation tags
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


# ── Threshold constants ───────────────────────────────────────────────────────

_STRONG = 72.0      # factor score at which we call it a "strong" positive
_WEAK   = 40.0      # below this = notable weakness


# ── Factor label lookup ───────────────────────────────────────────────────────

_FACTOR_LABELS: Dict[str, str] = {
    "bottleneck_exposure":          "Physical bottleneck position",
    "supply_chain_confirmation":    "Supply-chain confirmation",
    "theme_alignment":              "Thematic alignment",
    "balance_sheet_strength":       "Balance sheet quality",
    "catalyst_proximity":           "Upcoming catalyst window",
    "small_cap_asymmetry":          "Small/mid-cap asymmetry",
    "technical_confirmation":       "Technical confirmation",
    "sector_strength":              "Sector momentum",
    "dilution_risk":                "Dilution risk",
    "crowding_risk":                "Crowding risk",
    "execution_risk":               "Execution risk",
    "ebitda_inflection_proximity":  "EBITDA inflection proximity",
    "revenue_growth":               "Revenue growth",
    "revenue_acceleration":         "Revenue acceleration",
    "valuation_discount_vs_peers":  "Valuation discount vs peers",
    "backlog_quality":              "Backlog / order quality",
    "evidence_freshness":           "Evidence freshness",
    "insider_buying":               "Insider buying activity",
    "policy_tailwind":              "Policy / macro tailwind",
    "supply_chain_confirmation":    "Supply-chain confirmation",
}

_PLAYBOOK_PERSONA: Dict[str, str] = {
    "serenity":  "Serenity (bottleneck-exposure, structural edge)",
    "sjcapital": "S&J Capital (sector-momentum, EBITDA inflection)",
}

_RISK_PENALTY_FACTORS = {"dilution_risk", "crowding_risk", "execution_risk"}


# ── Main entry point ──────────────────────────────────────────────────────────

def generate_explanation(
    result: Any,        # PlaybookScoreResult
    playbook: Any,      # PlaybookDefinition
) -> Dict[str, Any]:
    """
    Build a complete explanation dict from a PlaybookScoreResult.
    Returns a dict to be merged into the API response.
    """
    scores: Dict[str, float]       = result.factor_scores
    details: Dict[str, Any]        = result.factor_details
    weights: Dict[str, float]      = playbook.factor_weights
    penalty_factors: Dict[str, float] = result.penalties_applied

    # ── Sorted factors by weighted contribution ───────────────────────────────
    weighted_contribs = {
        f: scores.get(f, 50.0) * weights.get(f, 0.0)
        for f in weights
    }
    sorted_weighted = sorted(weighted_contribs.items(), key=lambda x: x[1], reverse=True)

    # ── Fit reasoning: top positive factors ──────────────────────────────────
    fit_reasoning: List[str] = []
    for factor, contrib in sorted_weighted:
        if contrib <= 0:
            continue
        sc = scores.get(factor, 50.0)
        if sc >= _STRONG and factor not in _RISK_PENALTY_FACTORS:
            label  = _FACTOR_LABELS.get(factor, factor.replace("_", " ").title())
            detail = details.get(factor)
            reason_text = detail.reasons[0] if (detail and detail.reasons) else f"Score: {sc:.0f}/100"
            fit_reasoning.append(f"{label}: {reason_text}")
        if len(fit_reasoning) >= 4:
            break

    # ── Non-fit reasoning: weak factors and penalties ─────────────────────────
    non_fit: List[str] = []
    for factor, contrib in reversed(sorted_weighted):
        sc = scores.get(factor, 50.0)
        if sc < _WEAK and factor not in _RISK_PENALTY_FACTORS:
            label = _FACTOR_LABELS.get(factor, factor.replace("_", " ").title())
            detail = details.get(factor)
            reason_text = detail.reasons[0] if (detail and detail.reasons) else f"Score: {sc:.0f}/100 — weakness"
            non_fit.append(f"Weak {label}: {reason_text}")
        if len(non_fit) >= 3:
            break

    for pf, deduction in penalty_factors.items():
        label = _FACTOR_LABELS.get(pf, pf.replace("_", " ").title())
        detail = details.get(pf)
        penalty_text = detail.reasons[0] if (detail and detail.reasons) else f"Score {scores.get(pf, 50):.0f}/100"
        non_fit.append(f"Penalty — {label}: {penalty_text} (−{deduction:.0f}pts applied)")

    # Hard filter failure
    if not result.hard_filter_pass:
        for hff in result.hard_filter_failures[:2]:
            non_fit.append(f"Hard filter failure: {hff}")

    # ── Key confirming signals ────────────────────────────────────────────────
    key_signals: List[str] = []
    for factor, contrib in sorted_weighted:
        sc = scores.get(factor, 50.0)
        detail = details.get(factor)
        if sc >= _STRONG and factor not in _RISK_PENALTY_FACTORS and detail and detail.reasons:
            key_signals.append(f"[{_factor_short(factor)}] {detail.reasons[0]}")
        if len(key_signals) >= 5:
            break

    # Add matched themes / catalyst signals
    for theme_tag in (result.matched_themes or [])[:2]:
        from services.playbook.theme_map import THEME_LABELS
        tl = THEME_LABELS.get(theme_tag, theme_tag)
        if f"[Theme] {tl}" not in key_signals:
            key_signals.append(f"[Theme] {tl}")

    # ── Top risks ─────────────────────────────────────────────────────────────
    top_risks: List[str] = list(result.risks or [])[:3]
    for pf in _RISK_PENALTY_FACTORS:
        sc = scores.get(pf, 50.0)
        if sc > 65:
            label = _FACTOR_LABELS.get(pf, pf.replace("_", " ").title())
            detail = details.get(pf)
            risk_txt = detail.reasons[0] if (detail and detail.reasons) else f"{sc:.0f}/100"
            entry = f"{label} elevated ({sc:.0f}/100): {risk_txt}"
            if entry not in top_risks:
                top_risks.append(entry)

    # ── What would improve score ──────────────────────────────────────────────
    improve: List[str] = []
    for factor, contrib in reversed(sorted_weighted):
        sc = scores.get(factor, 50.0)
        if sc < 55.0 and factor not in _RISK_PENALTY_FACTORS and weights.get(factor, 0) > 0.05:
            label = _FACTOR_LABELS.get(factor, factor.replace("_", " ").title())
            improve.append(_improvement_hint(factor, label, sc))
        if len(improve) >= 4:
            break

    # Also add: resolving penalties
    for pf in penalty_factors:
        sc = scores.get(pf, 50.0)
        label = _FACTOR_LABELS.get(pf, pf.replace("_", " ").title())
        if pf == "dilution_risk":
            improve.append("Reduction in dilution risk — no new equity offerings or shelf registrations")
        elif pf == "crowding_risk":
            improve.append("Consolidation / pullback — reducing crowded positioning risk")
        elif pf == "execution_risk":
            improve.append("Revenue growth reacceleration or leverage reduction")

    # ── What would break thesis ───────────────────────────────────────────────
    break_thesis: List[str] = _generate_break_thesis(result, playbook, scores, details)

    # ── Thesis summary ────────────────────────────────────────────────────────
    thesis_summary = _generate_thesis_summary(
        result, playbook, scores, details, fit_reasoning, non_fit
    )

    return {
        "thesis_summary":         thesis_summary,
        "fit_reasoning":          fit_reasoning[:4],
        "non_fit_reasoning":      non_fit[:4],
        "key_confirming_signals": key_signals[:5],
        "top_risks":              top_risks[:4],
        "what_would_improve_score": improve[:4],
        "what_would_break_thesis":  break_thesis[:4],
        "supply_chain_tags":       result.factor_details.get(
                                       "supply_chain_confirmation", None
                                   ) and result.factor_details["supply_chain_confirmation"].source_tags or [],
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _factor_short(factor: str) -> str:
    shorts = {
        "bottleneck_exposure":          "Bottleneck",
        "supply_chain_confirmation":    "Supply Chain",
        "theme_alignment":              "Theme",
        "balance_sheet_strength":       "Balance Sheet",
        "catalyst_proximity":           "Catalyst",
        "small_cap_asymmetry":          "Small Cap",
        "technical_confirmation":       "Technical",
        "sector_strength":              "Sector",
        "ebitda_inflection_proximity":  "EBITDA",
        "revenue_growth":               "Revenue",
        "valuation_discount_vs_peers":  "Valuation",
        "backlog_quality":              "Backlog",
        "evidence_freshness":           "Freshness",
        "insider_buying":               "Insider",
        "policy_tailwind":              "Policy",
    }
    return shorts.get(factor, factor.replace("_", " ").title()[:12])


def _improvement_hint(factor: str, label: str, score: float) -> str:
    hints = {
        "bottleneck_exposure":         "Deeper supply-chain analysis to confirm physical bottleneck position",
        "supply_chain_confirmation":   "Confirmed design-win or supply agreement with downstream customer",
        "theme_alignment":             "Stronger alignment with playbook's preferred thematic focus areas",
        "balance_sheet_strength":      "Debt reduction or equity raise to clean balance sheet",
        "catalyst_proximity":          "Upcoming earnings or product announcement within next 30 days",
        "small_cap_asymmetry":         "Lower current valuation or smaller market cap structure",
        "technical_confirmation":      "Technical breakout or price recovery into uptrend",
        "sector_strength":             "Sector ETF momentum improvement across 1M/3M windows",
        "ebitda_inflection_proximity": "EBITDA or operating income approaching positive territory",
        "revenue_growth":              "Re-acceleration of revenue growth above 15% YoY",
        "valuation_discount_vs_peers": "Price pullback to discount vs sector PE median",
        "backlog_quality":             "Fresh backlog / contract award announcement",
        "evidence_freshness":          "New confirming news catalyst in the next 1-2 weeks",
        "insider_buying":              "Open-market insider purchase (size $250K+)",
        "policy_tailwind":             "Specific policy funding/award that benefits the sector",
    }
    return hints.get(factor, f"{label}: improvement from current {score:.0f}/100 baseline")


def _generate_break_thesis(
    result: Any,
    playbook: Any,
    scores: Dict[str, float],
    details: Dict[str, Any],
) -> List[str]:
    """Generate what would invalidate the thesis for this playbook."""
    thesis_breaks: List[str] = []
    pb_id = playbook.id

    # Universal risks
    if scores.get("dilution_risk", 50) > 50:
        thesis_breaks.append("Equity offering or ATM program announcement — confirms dilution risk")

    if scores.get("bottleneck_exposure", 50) >= 70:
        thesis_breaks.append("New entrant or substitute technology eliminating bottleneck advantage")

    if scores.get("sector_strength", 50) >= 70:
        thesis_breaks.append("Sector rotation out of current hot sector — ETF momentum reversal")

    if scores.get("catalyst_proximity", 50) >= 70:
        thesis_breaks.append("Earnings miss or guidance cut at upcoming catalyst event")

    # Playbook-specific
    if pb_id == "serenity":
        thesis_breaks.append("Balance sheet deterioration — D/E spike or credit event")
        if scores.get("theme_alignment", 50) >= 65:
            thesis_breaks.append("Structural disruption to supply-chain position (technology substitution)")
    elif pb_id == "sjcapital":
        thesis_breaks.append("Revenue deceleration — growth dropping below sector average")
        thesis_breaks.append("Sector ETF breakdown across 1M and 3M windows simultaneously")

    return thesis_breaks[:4]


def _generate_thesis_summary(
    result: Any,
    playbook: Any,
    scores: Dict[str, float],
    details: Dict[str, Any],
    fit_reasoning: List[str],
    non_fit: List[str],
) -> str:
    """
    Generate a 1-2 sentence thesis summary.
    Strategy: identify the single strongest positive signal and the single
    most notable weakness, then combine them concisely.
    """
    pb_name = playbook.name
    ticker  = result.ticker
    score   = result.final_score
    hf_pass = result.hard_filter_pass

    if not hf_pass:
        failures = "; ".join(result.hard_filter_failures[:2])
        return f"{ticker} fails {pb_name} hard filters ({failures}) — excluded from consideration."

    if score >= 75:
        strength = fit_reasoning[0].split(":")[0] if fit_reasoning else "strong multi-factor alignment"
        return (
            f"{ticker} shows strong {pb_name} fit ({score:.0f}/100), led by "
            f"{strength.lower()}."
        )
    elif score >= 55:
        strength = fit_reasoning[0].split(":")[0] if fit_reasoning else "partial alignment"
        weakness = non_fit[0].split(":")[0] if non_fit else "some factor gaps"
        return (
            f"{ticker} has moderate {pb_name} alignment ({score:.0f}/100); "
            f"{strength.lower()} is supportive but {weakness.lower()} limits conviction."
        )
    elif score >= 40:
        weakness = non_fit[0].split(":")[0] if non_fit else "weak factor profile"
        return (
            f"{ticker} shows below-average {pb_name} fit ({score:.0f}/100) — "
            f"{weakness.lower()} is the primary drag."
        )
    else:
        return (
            f"{ticker} is poorly aligned with the {pb_name} playbook ({score:.0f}/100) — "
            f"key factors do not support the strategy's core thesis."
        )
