"""
Strategy Screener — Deterministic report section builder.

All sections are built from:
  - DiscoveryCandidate data (supply chain graph + scoring)
  - Optional AnalyzeTickerSummary (if analyze bridge ran)
  - SerenityRegime context

Zero LLM dependency. Same brain as the Serenity terminal — just formatted for
a publication/report surface rather than a prompt-driven response.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


# ── Grade assignment ───────────────────────────────────────────────────────────

_GRADE_THRESHOLDS = [
    (82.0, "A+"),
    (72.0, "A"),
    (60.0, "B+"),
    (48.0, "B"),
    (0.0,  "C"),
]


def assign_grade(
    best_blend_score: float,
    data_confidence: str,
    hiddenness_score: float,
    bottleneck_criticality_score: float,
) -> str:
    """
    Deterministic letter grade from Serenity scoring signals.

    Weights:
      best_blend_score         50 %  (already a composite)
      bottleneck_criticality   25 %
      hiddenness               15 %
      data_confidence bonus    10 %  (high=+10, medium=0, low=-10)

    Thresholds: A+=82, A=72, B+=60, B=48, C=<48
    """
    conf_adj = 10.0 if data_confidence == "high" else (-10.0 if data_confidence == "low" else 0.0)

    composite = (
        best_blend_score            * 0.50
        + bottleneck_criticality_score * 0.25
        + hiddenness_score             * 0.15
        + conf_adj
    )

    for threshold, grade in _GRADE_THRESHOLDS:
        if composite >= threshold:
            return grade
    return "C"


# ── One-line summary ───────────────────────────────────────────────────────────

def build_one_line_summary(candidate: Dict[str, Any]) -> str:
    """Build a one-sentence list-page summary from structured candidate data."""
    name      = candidate.get("company_name", candidate.get("ticker", ""))
    themes    = candidate.get("themes", [])
    anchors   = candidate.get("giant_anchors", [])
    role      = candidate.get("chain_role_type", "adjacent_supplier")
    thesis    = candidate.get("thesis_summary", "")

    if thesis:
        return thesis[:160].rstrip(".,;") + "."

    role_label = {
        "direct_bottleneck":   "critical bottleneck supplier",
        "platform_anchor":     "platform anchor",
        "adjacent_supplier":   "specialist supplier",
        "indirect_beneficiary": "indirect beneficiary",
    }.get(role, "specialist supplier")

    theme_str  = themes[0].replace("_", " ") if themes else "supply chain"
    anchor_str = f" for {anchors[0]}" if anchors else ""

    return f"{name} is a {role_label} in {theme_str}{anchor_str}."


# ── Full report section builders ───────────────────────────────────────────────

def _layer_label(depth: int) -> str:
    return {0: "L0 (Platform Giant)", 1: "L1 (Systems Integrator)",
            2: "L2 (Key Component)", 3: "L3 (Constrained Bottleneck)",
            4: "L4 (Upstream Material / Specialty)"}.get(depth, f"L{depth}")


def build_summary(candidate: Dict[str, Any], analyze_result: Optional[Dict[str, Any]] = None) -> str:
    name      = candidate.get("company_name", candidate.get("ticker", ""))
    ticker    = candidate.get("ticker", "")
    country   = candidate.get("country", "US")
    themes    = candidate.get("themes", [])
    depth     = candidate.get("layer_depth", 2)
    anchors   = candidate.get("giant_anchors", [])
    thesis    = candidate.get("thesis_summary", "")
    fit_rsns  = candidate.get("fit_reasoning", [])
    role      = candidate.get("chain_role_type", "adjacent_supplier")
    bbs       = candidate.get("best_blend_score", 0.0)
    bcs       = candidate.get("bottleneck_criticality_score", 50.0)

    role_label = {
        "direct_bottleneck":   "a critical supply chain bottleneck",
        "platform_anchor":     "a platform anchor",
        "adjacent_supplier":   "a specialist supplier",
        "indirect_beneficiary": "an indirect beneficiary",
    }.get(role, "a specialist supplier")

    theme_str  = ", ".join(t.replace("_", " ") for t in themes[:2]) or "supply chain"
    anchor_str = f" supplying {', '.join(anchors[:3])}" if anchors else ""
    layer_str  = _layer_label(depth)

    lines = [
        f"{name} ({ticker}) is {role_label} in {theme_str}{anchor_str}, "
        f"positioned at {layer_str} in the supply chain.",
    ]
    if thesis:
        lines.append(thesis)
    for r in fit_rsns[:2]:
        lines.append(r)
    if bbs >= 70:
        lines.append(
            f"Serenity scores this at {bbs:.0f}/100 (best-blend composite), with bottleneck "
            f"criticality of {bcs:.0f}/100, placing it in the top tier for this regime."
        )

    if analyze_result:
        answer = analyze_result.get("answer", "")
        if answer and len(answer) > 60:
            lines.append(answer[:400])

    return " ".join(lines[:4])


def build_why_it_matters(candidate: Dict[str, Any]) -> str:
    name    = candidate.get("company_name", candidate.get("ticker", ""))
    bcs     = candidate.get("bottleneck_criticality_score", 50.0)
    anchors = candidate.get("giant_anchors", [])
    themes  = candidate.get("themes", [])
    role    = candidate.get("chain_role_type", "adjacent_supplier")
    depth   = candidate.get("layer_depth", 2)
    conf    = candidate.get("data_confidence", "medium")
    why_now = candidate.get("why_now", "")

    parts = []
    if bcs >= 75:
        parts.append(
            f"{name} holds a high-criticality position (bottleneck score {bcs:.0f}/100): "
            f"few substitutes exist and switching costs are structural."
        )
    elif bcs >= 55:
        parts.append(
            f"{name} occupies a moderately constrained position in the supply chain "
            f"(bottleneck score {bcs:.0f}/100), with limited near-term substitution risk."
        )
    else:
        parts.append(
            f"{name} plays a supporting supply chain role — meaningful but with available alternatives."
        )

    if anchors:
        anchor_str = " and ".join(anchors[:3])
        parts.append(
            f"Its customer base includes {anchor_str} — hyperscaler-scale demand platforms "
            f"that cannot easily qualify alternative suppliers."
        )

    if depth >= 3:
        parts.append(
            f"The deep positioning at {_layer_label(depth)} means most institutional screening "
            f"tools miss this name entirely — it does not appear in standard sector ETF holdings."
        )

    if why_now:
        parts.append(why_now)

    theme_str = themes[0].replace("_", " ") if themes else ""
    if theme_str:
        parts.append(
            f"The {theme_str} theme is currently in the high-conviction regime cluster — "
            f"capital expenditure commitments from top-tier platforms are structurally locked in."
        )

    return " ".join(parts[:4])


def build_supply_chain_map_text(candidate: Dict[str, Any]) -> str:
    name        = candidate.get("company_name", candidate.get("ticker", ""))
    anchors     = candidate.get("giant_anchors", [])
    depth       = candidate.get("layer_depth", 2)
    chain_roles = candidate.get("chain_layers", [])
    country     = candidate.get("country", "US")

    layer_str  = _layer_label(depth)
    anchor_str = ", ".join(anchors[:5]) if anchors else "multiple major technology platforms"

    parts = [
        f"{name} sits at {layer_str} of the supply chain, supplying {anchor_str}."
    ]

    if chain_roles:
        roles_str = "; ".join(chain_roles[:3])
        parts.append(f"Reported supply chain roles: {roles_str}.")

    if country != "US":
        parts.append(
            f"Domiciled in {country} — adds foreign specialist exposure not available through "
            f"standard US-listed semiconductor / technology indices."
        )

    parts.append(
        f"Layer position note: L0 = platform giant (NVDA, TSM), L1 = systems integrator, "
        f"L2 = key component, L3 = constrained bottleneck, L4 = upstream specialty. "
        f"Serenity prioritizes L2-L4 names where institutional coverage is thinnest."
    )

    return " ".join(parts)


def build_competitors(candidate: Dict[str, Any]) -> str:
    name      = candidate.get("company_name", candidate.get("ticker", ""))
    comps     = candidate.get("comparable_names", [])
    crowding  = candidate.get("crowding_flags", [])
    role      = candidate.get("chain_role_type", "adjacent_supplier")
    bcs       = candidate.get("bottleneck_criticality_score", 50.0)

    if not comps:
        return (
            f"No direct publicly-traded comparables were identified for {name} at this supply chain layer. "
            f"This absence of peer coverage is itself a hiddenness signal — "
            f"markets rarely price niche L3-L4 suppliers correctly without a visible comp set."
        )

    comp_str = ", ".join(comps[:5])
    parts    = [f"Comparable names at a similar supply chain tier: {comp_str}."]

    if bcs >= 70:
        parts.append(
            f"Despite the comp set, {name} holds differentiated positioning: "
            f"bottleneck score {bcs:.0f}/100 suggests structural switching barriers that "
            f"most comparables lack."
        )

    if crowding:
        flags = "; ".join(crowding[:3])
        parts.append(f"Crowding signals noted: {flags}.")
    else:
        parts.append("No crowding flags detected — this name is not currently in momentum screens.")

    return " ".join(parts)


def build_catalysts(candidate: Dict[str, Any], regime_context: Optional[Dict[str, Any]] = None) -> str:
    themes    = candidate.get("themes", [])
    anchors   = candidate.get("giant_anchors", [])
    depth     = candidate.get("layer_depth", 2)
    why_now   = candidate.get("why_now", "")
    fit_rsns  = candidate.get("fit_reasoning", [])

    parts = []
    if why_now:
        parts.append(why_now)

    if regime_context:
        regime_why = regime_context.get("why_now", [])
        for bullet in regime_why[:2]:
            if bullet not in " ".join(parts):
                parts.append(bullet)

    if anchors:
        anchor_str = ", ".join(anchors[:3])
        parts.append(
            f"Hyperscaler capex commitments from {anchor_str} create structural demand pull "
            f"through the supply chain layers this company occupies."
        )

    if depth >= 3:
        parts.append(
            "Increasing re-shoring policy pressure (CHIPS Act, export controls) creates "
            "structural preference for qualified suppliers in constrained tiers — "
            "L3/L4 names with existing qualification are the hardest to replace."
        )

    for r in fit_rsns[:1]:
        if r not in " ".join(parts):
            parts.append(r)

    return " ".join(parts[:4]) if parts else "No specific near-term catalyst identified; structural tailwind ongoing."


def build_rerating_case(candidate: Dict[str, Any]) -> str:
    name    = candidate.get("company_name", candidate.get("ticker", ""))
    hs      = candidate.get("hiddenness_score", 50.0)
    bbs     = candidate.get("best_blend_score", 0.0)
    cov     = candidate.get("coverage_status", "full")
    mktcap  = candidate.get("market_cap_usd")
    anchors = candidate.get("giant_anchors", [])

    parts = []
    if hs >= 65:
        parts.append(
            f"{name} scores {hs:.0f}/100 on hiddenness — meaning it is genuinely under-covered "
            f"by sell-side analysts and absent from standard institutional screens. "
            f"Rerated when any tier-1 analyst initiates coverage or the company appears in an "
            f"SEC filing as a named supplier."
        )

    if cov in ("partial", "thin"):
        parts.append(
            f"Data coverage is currently {cov}: re-rating catalyst includes any improvement "
            f"in public disclosure (investor day, earnings call, customer announcement)."
        )

    if mktcap and mktcap < 2_000_000_000:
        mc_str = f"${mktcap/1e6:.0f}M"
        parts.append(
            f"At {mc_str} market cap, institutional ownership is likely minimal. "
            f"A single large investor filing a 13G can move the stock materially — "
            f"Serenity identifies these names before that event."
        )

    if anchors:
        parts.append(
            f"Any public confirmation of supply relationship with {anchors[0]} "
            f"(press release, customer disclosure, earnings transcript mention) "
            f"is a quantifiable re-rating trigger."
        )

    if bbs >= 75:
        parts.append(
            f"Best-blend composite {bbs:.0f}/100 means the supply chain evidence is strong enough "
            f"that the re-rating case does not depend on a single catalyst — "
            f"structural demand growth alone may be sufficient."
        )

    return " ".join(parts[:3]) if parts else "Re-rating case: any increase in institutional visibility or named customer disclosure."


def build_key_risk(candidate: Dict[str, Any]) -> str:
    name   = candidate.get("company_name", candidate.get("ticker", ""))
    wtbt   = candidate.get("what_would_break_thesis", "")
    cov    = candidate.get("coverage_status", "full")
    conf   = candidate.get("data_confidence", "high")
    crowd  = candidate.get("crowding_flags", [])
    bcs    = candidate.get("bottleneck_criticality_score", 50.0)

    parts = []
    if wtbt:
        parts.append(wtbt)

    if conf == "low":
        parts.append(
            f"Data confidence is low — supply chain positioning is inferred from thin public data. "
            f"A direct check of SEC filings, earnings transcripts, or investor presentations "
            f"should precede any position."
        )
    elif conf == "medium" and cov in ("partial", "thin"):
        parts.append(
            f"Coverage is {cov}: some supply chain links are inferred, not directly confirmed. "
            f"Material adverse discovery (customer loss, design-out) may not be immediately visible."
        )

    if bcs < 55:
        parts.append(
            f"Bottleneck criticality of {bcs:.0f}/100 means substitution risk is real — "
            f"the key risk is a platform giant qualifying an alternative supplier at lower cost."
        )

    if crowd:
        flags = "; ".join(crowd[:2])
        parts.append(f"Crowding signals detected: {flags} — momentum-driven positioning can reverse sharply.")

    return " ".join(parts[:3]) if parts else "Key risk: substitution of supplier by a lower-cost alternative or design-out."


def build_why_hidden(candidate: Dict[str, Any]) -> str:
    name     = candidate.get("company_name", candidate.get("ticker", ""))
    why_h    = candidate.get("why_hidden", "")
    hs       = candidate.get("hiddenness_score", 50.0)
    country  = candidate.get("country", "US")
    mktcap   = candidate.get("market_cap_usd")
    cov      = candidate.get("coverage_status", "full")
    cov_note = candidate.get("coverage_notes", "")
    depth    = candidate.get("layer_depth", 2)

    parts = []
    if why_h:
        parts.append(why_h)

    if country != "US":
        parts.append(
            f"Foreign domicile in {country} means the company is absent from major US indices, "
            f"excluded from most retail screeners, and requires an ADR or direct foreign account to access. "
            f"This creates a structural information asymmetry that Serenity specifically targets."
        )

    if mktcap and mktcap < 1_000_000_000:
        mc_str = f"${mktcap/1e6:.0f}M"
        parts.append(
            f"Market cap of {mc_str} places this below the coverage threshold for most sell-side "
            f"research desks and institutional mandates — hiddenness score {hs:.0f}/100."
        )
    elif hs >= 55:
        parts.append(
            f"Hiddenness score {hs:.0f}/100 — under-covered relative to its supply chain significance. "
            f"Serenity specifically prioritizes names scoring above 55 on this dimension."
        )

    if depth >= 3:
        parts.append(
            f"Deep supply chain positioning at {_layer_label(depth)} is inherently invisible to "
            f"screens that look only one step downstream from the platform giants."
        )

    if cov_note:
        parts.append(cov_note)

    return " ".join(parts[:3]) if parts else f"Hiddenness score {hs:.0f}/100; below mainstream institutional coverage threshold."


def build_what_to_verify_next(candidate: Dict[str, Any]) -> str:
    wtv   = candidate.get("what_to_verify_next", "")
    conf  = candidate.get("data_confidence", "high")
    cov   = candidate.get("coverage_status", "full")
    gaps  = candidate.get("data_gaps", [])
    anchors = candidate.get("giant_anchors", [])

    parts = []
    if wtv:
        parts.append(wtv)

    if gaps:
        gaps_str = "; ".join(gaps[:3])
        parts.append(f"Data gaps to close: {gaps_str}.")

    if conf in ("medium", "low") or cov in ("partial", "thin"):
        parts.append(
            "Verify supply chain links via: latest annual report / 20-F, "
            "earnings call transcripts (search customer names), "
            "investor day presentations, and supplier qualification filings."
        )

    if anchors:
        parts.append(
            f"Check {anchors[0]} supply chain filings and 10-K supplier disclosures "
            f"for confirmation of this company as a named vendor."
        )

    parts.append(
        "Standard due diligence: confirm market cap, liquidity, and legal structure "
        "before sizing a position — particularly for foreign-listed names."
    )

    return " ".join(parts[:3])


def build_what_would_break_thesis(candidate: Dict[str, Any]) -> str:
    wtbt   = candidate.get("what_would_break_thesis", "")
    bcs    = candidate.get("bottleneck_criticality_score", 50.0)
    anchors = candidate.get("giant_anchors", [])

    parts = []
    if wtbt:
        parts.append(wtbt)
    else:
        parts.append(
            "Thesis breaks if: (1) platform giant qualifies a lower-cost alternative supplier, "
            "(2) technology transition makes current process obsolete, "
            "(3) geopolitical event disrupts the foreign supply chain link."
        )

    if bcs < 65 and not wtbt:
        parts.append(
            f"Bottleneck criticality {bcs:.0f}/100 — substitution is a live risk "
            f"if any comparable supplier achieves full qualification."
        )

    if anchors:
        parts.append(
            f"Any public announcement of {anchors[0]} vertically integrating this capability "
            f"or switching to an in-house solution would be thesis-breaking."
        )

    return " ".join(parts[:2])


# ── Supply chain layers list ───────────────────────────────────────────────────

def build_supply_chain_layers(candidate: Dict[str, Any]) -> List[str]:
    """Build an ordered list of supply chain layer descriptions."""
    anchors    = candidate.get("giant_anchors", [])
    depth      = candidate.get("layer_depth", 2)
    chain_roles = candidate.get("chain_layers", [])
    ticker     = candidate.get("ticker", "")

    layers = []
    if anchors:
        layers.append(f"L0 Giants: {', '.join(anchors[:4])}")
    if depth >= 1:
        layers.append("L1 Systems Integrators: board manufacturers, system assemblers")
    if depth >= 2:
        layers.append("L2 Key Components: PCBs, substrates, advanced packaging")
    if depth >= 3:
        layers.append(f"L3 Constrained Bottlenecks: ← {ticker} is in this zone" if depth == 3 else "L3 Constrained Bottlenecks")
    if depth >= 4:
        layers.append(f"L4 Upstream Specialty: ← {ticker} operates at this depth" if depth >= 4 else "L4 Upstream Specialty")

    if chain_roles:
        layers.append(f"Reported roles: {'; '.join(chain_roles[:3])}")

    return layers


# ── Master build function ──────────────────────────────────────────────────────

def build_full_report(
    candidate: Dict[str, Any],
    snapshot_id: str,
    regime_context: Optional[Dict[str, Any]] = None,
    analyze_result: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a complete ScreenerReport dict from structured candidate data."""
    ticker   = candidate.get("ticker", "")
    name     = candidate.get("company_name", ticker)
    country  = candidate.get("country", "US")
    exchange = candidate.get("exchange", "NASDAQ")
    depth    = candidate.get("layer_depth", 2)
    mktcap   = candidate.get("market_cap_usd")
    themes   = candidate.get("themes", [])
    anchors  = candidate.get("giant_anchors", [])
    cov      = candidate.get("coverage_status", "full")
    conf     = candidate.get("data_confidence", "high")
    proxy    = candidate.get("us_access_proxy")

    bbs  = candidate.get("best_blend_score", 0.0)
    bcs  = candidate.get("bottleneck_criticality_score", 50.0)
    hs   = candidate.get("hiddenness_score", 50.0)

    grade = assign_grade(bbs, conf, hs, bcs)

    mc_str = (f"${mktcap/1e6:.0f}M" if mktcap and mktcap < 1e9
              else f"${mktcap/1e9:.1f}B" if mktcap else "N/A")

    theme_str = themes[0].replace("_", " ").title() if themes else "Supply Chain"
    layer_str = _layer_label(depth)

    headline  = f"{ticker} • {name}"
    meta_line = f"{mc_str} • {country} • {layer_str} • {theme_str} • {grade}"

    now_str = datetime.now(timezone.utc).isoformat()

    return {
        "snapshot_id":           snapshot_id,
        "ticker":                ticker,
        "company_name":          name,
        "headline":              headline,
        "meta_line":             meta_line,
        "summary":               build_summary(candidate, analyze_result),
        "why_it_matters":        build_why_it_matters(candidate),
        "supply_chain_map_text": build_supply_chain_map_text(candidate),
        "supply_chain_layers":   build_supply_chain_layers(candidate),
        "competitors":           build_competitors(candidate),
        "catalysts":             build_catalysts(candidate, regime_context),
        "rerating_case":         build_rerating_case(candidate),
        "key_risk":              build_key_risk(candidate),
        "why_hidden":            build_why_hidden(candidate),
        "what_to_verify_next":   build_what_to_verify_next(candidate),
        "what_would_break_thesis": build_what_would_break_thesis(candidate),
        "themes":                themes,
        "anchors":               anchors,
        "coverage_status":       cov,
        "data_confidence":       conf,
        "us_access_proxy":       proxy,
        "market_cap_usd":        mktcap,
        "country":               country,
        "exchange":              exchange,
        "grade":                 grade,
        "scores": {
            "best_blend_score":             bbs,
            "bottleneck_criticality_score": bcs,
            "hiddenness_score":             hs,
            "chain_depth_score":            candidate.get("chain_depth_score", 50.0),
            "supply_chain_confidence_score": candidate.get("supply_chain_confidence_score", 50.0),
        },
        "generated_at":   now_str,
        "regime_context": regime_context,
    }
