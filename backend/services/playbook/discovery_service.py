"""
Discovery Service — core on-demand Serenity discovery + supply-chain intelligence engine.

Entry points:
  run_discover(req: DiscoverRequest) -> DiscoverResponse
  run_supply_chain_map(req: SupplyChainMapRequest) -> SupplyChainMapResponse

Discovery modes:
  giant_chain        — traverse supply chain from a major platform giant
  theme_scan         — scan companies relevant to one or more themes
  foreign_bottlenecks — focus on non-US supply chain positions
  ticker_chain       — find upstream/downstream neighbors of a known ticker
  country_theme_scan — cross-filter by country + theme
  custom             — query-driven fallback using all maps

Scoring dimensions (all deterministic/heuristic — no LLM inference):
  chain_depth_score             — layer 3-4 = deeper = more hidden
  bottleneck_criticality_score  — from curated NODE_REGISTRY
  hiddenness_score              — market cap + country + crowding heuristics
                                   (thin data → confidence_penalties, NOT hiddenness boost)
  giant_dependency_score        — how many giants anchor this node
  foreign_uniqueness_score      — non-US presence penalty/bonus
  supply_chain_confidence_score — evidence count + confidence label
  proxy_accessibility_score     — US=100, ADR=72, ETF=55, foreign=30
  theme_purity_score            — 1 theme=95, 2=80, 3=65, 4+=50

Phase 4 additions:
  best_blend_score      — weighted composite driving "best surfaced first" ranking
  visibility_bucket     — household / widely_covered / known / specialist / hidden
  chain_role_type       — platform_anchor / direct_bottleneck / adjacent_supplier / indirect_beneficiary
  confidence_penalties  — explicit list of data quality issues (thin data here, NOT in hiddenness)
  data_gaps             — fields missing from live enrichment
  why_now               — deterministic theme-driven context string
  why_hidden            — deterministic visibility explanation
  what_to_verify_next   — deterministic next-step guidance
  comparable_names      — peer tickers from same themes/layer
  Ranking buckets:
    top_hidden_bottlenecks, top_direct_chokepoints, top_foreign_specialists,
    top_us_accessible_foreign_proxies, highest_confidence_candidates, best_blend_candidates

Provider usage summary:
  Finnhub   — profile + news enrichment for enriched candidates
  Tradier   — quote / liquidity for US-listed and ADR names
  FMP       — market cap (sparing; 250/day cap)
  Perplexity — shortlist validation only (max 5 per request)

Guardrails:
  - No /api/query coupling
  - No background scans — user-triggered only
  - No Brave / Tavily usage
  - Cache all provider calls aggressively
  - Thin data lowers confidence, does NOT inflate hiddenness
"""
from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, List, Optional, Set

from services.playbook.discovery_types import (
    DiscoverRequest,
    DiscoverResponse,
    DiscoveryCandidate,
    DiscoveryScores,
    SupplyChainMapRequest,
    SupplyChainMapResponse,
    ChainLayer,
)
from services.playbook.supply_chain_graph import (
    NODE_REGISTRY,
    LAYER_LABELS,
    get_chain_for_theme,
    get_chain_for_giant,
    get_all_tickers_for_themes,
)
from services.playbook.giant_map import GIANT_MAP, get_giant, list_giants
from services.playbook.theme_discovery import THEME_TAXONOMY, get_theme, list_themes
from services.playbook.foreign_market_map import (
    COUNTRY_METADATA,
    FOREIGN_ACCESS_MAP,
    get_etf_proxies_for_theme,
)


# ── Household / widely-known name sets ────────────────────────────────────────
# These are NOT "hidden" by definition. Penalize hiddenness for these names.

HOUSEHOLD_AI_NAMES: Set[str] = {
    "NVDA", "AMD", "ASML", "AMAT", "LRCX", "KLAC", "TSM", "MU",
    "INTC", "AVGO", "QCOM", "GOOGL", "META", "MSFT", "AMZN",
    "AAPL", "TSM",
}

WIDELY_COVERED_NAMES: Set[str] = {
    "SMCI", "NXPI", "CDNS", "SNPS", "MRVL", "MCHP", "TXN",
    "ONNN", "STM", "ASX", "GEV", "ETN", "VRT", "ENTG",
}


# ── Active-theme explanations for why_now ────────────────────────────────────

_THEME_WHY_NOW: Dict[str, str] = {
    "ai_infrastructure":       "AI infrastructure buildout remains active and hyperscaler capex is accelerating",
    "advanced_packaging_test": "Advanced packaging (CoWoS/SoIC/hybrid bonding) remains a confirmed AI chip supply bottleneck",
    "semicap_supply_chain":    "Semiconductor equipment cycle is tightening ahead of new fab expansions through 2026",
    "memory_hbm":              "HBM memory capacity is a confirmed supply bottleneck for the AI GPU ramp",
    "photonics_cpo":           "Co-packaged optics (CPO) transition is gaining design-win momentum at hyperscalers",
    "ai_power_energy":         "AI datacenter power demand is outpacing utility grid capacity through 2027+",
    "grid_transformers":       "Grid transformer lead times remain 2-3 years, constraining datacenter expansion",
    "defense_optics":          "Defense optics and EO/IR spending is accelerating post-Ukraine and INDOPACOM budget increases",
    "soi_substrates_materials": "Substrate and materials supply is a long-lead gating factor for the 2nm/3nm node ramp",
    "cooling_thermal":         "Liquid cooling adoption is accelerating for AI GPU racks (GB200 NVL72 requires DLC)",
    "energy_transition":       "SiC and GaN demand is growing with EV drivetrain ramp and renewable power electronics",
    "neocloud":                "Neocloud GPU cluster spending remains on aggressive expansion trajectory through 2025-2026",
}

_THEME_VERIFY_HINTS: Dict[str, str] = {
    "advanced_packaging_test": "Ask about CoWoS, SoIC, or hybrid bonding qualification status and customer ramps",
    "photonics_cpo":           "Verify CPO design wins and silicon photonics roadmap traction at hyperscalers",
    "memory_hbm":              "Check HBM yield, capacity allocation, and next-gen (HBM4) qualification timeline",
    "ai_power_energy":         "Verify order backlog from datacenter customers and utility grid connection timeline",
    "grid_transformers":       "Check lead time trends and whether backlogs extend into 2026+",
    "semicap_supply_chain":    "Monitor WFE spending outlook and TSMC/Samsung capex alignment vs capacity commitments",
    "soi_substrates_materials": "Check long-lead substrate order book and any 2nm node qualification wins",
    "defense_optics":          "Verify program names, contract type (IDIQ vs firm-fixed-price), and sole-source status",
}

_ROLE_TYPE_VERIFY: Dict[str, str] = {
    "direct_bottleneck":    "Verify customer concentration, sole-source contract status, and lead-time data",
    "adjacent_supplier":    "Check backlog growth, margin trends, and new platform qualification wins",
    "indirect_beneficiary": "Confirm theme exposure is growing vs commoditized; check peer pricing power",
    "platform_anchor":      "Monitor capex guidance and supply chain dependency mapping updates",
}


# ── Discovery scoring engine ───────────────────────────────────────────────────

def _chain_depth_score(layer: int) -> float:
    """
    Layer depth → chain_depth_score.
    Deeper in chain = more hidden supplier = higher score.
    Layer 0 (giant) = 10, Layer 4 (upstream materials) = 95.
    """
    mapping = {0: 10, 1: 40, 2: 65, 3: 85, 4: 95}
    return float(mapping.get(layer, 50))


def _hiddenness_score(
    ticker: str,
    country: str,
    market_cap_usd: Optional[float],
    coverage_status: str,
    node: Dict[str, Any],
) -> tuple[float, str]:
    """
    Estimate how 'hidden' a company is — not widely known, under-covered.
    Returns (score, reason_string).

    CRITICAL: thin/partial data coverage does NOT inflate hiddenness.
    Thin data → confidence_penalties.  Hiddenness = genuine narrative obscurity.
    """
    # Household AI name → not hidden at all
    if ticker in HOUSEHOLD_AI_NAMES:
        return 15.0, "Household AI name — widely known and well-covered"

    score = 35.0 if country == "US" else 48.0
    reason_parts: List[str] = []

    # Widely covered (not household but still very well known) — slight deduction
    if ticker in WIDELY_COVERED_NAMES:
        score -= 8.0
        reason_parts.append("Widely covered by US analysts")

    # Market cap heuristic — ONLY use if data is credible (not thin OTC distortion)
    if market_cap_usd is not None and coverage_status != "thin":
        cap_m = market_cap_usd / 1e6
        if cap_m > 1e6:
            cap_m = cap_m / 1e6  # already in millions from Finnhub
        if cap_m < 500:
            score += 35
            reason_parts.append("Small cap (<$500M) — minimal institutional coverage")
        elif cap_m < 2_000:
            score += 22
            reason_parts.append("Small cap (<$2B) — limited institutional coverage")
        elif cap_m < 10_000:
            score += 12
            reason_parts.append("Mid cap ($2-10B) — moderate analyst coverage")
        elif cap_m > 100_000:
            score -= 15
            reason_parts.append("Mega-cap — widely covered by institutional analysts")
    else:
        # Unknown or unreliable cap — stay neutral, do NOT inflate hiddenness
        reason_parts.append("Market cap data unavailable — neutrally weighted")

    # Country heuristic — foreign gets moderate boost (less US analyst coverage)
    if country != "US":
        score += 12
        reason_parts.append(f"Foreign ({country}) — limited US analyst coverage")

    # Deep chain position — below investor radar
    layer = node.get("layer", 2)
    if layer >= 3:
        score += 10
        reason_parts.append(f"Layer-{layer} supply chain position — below typical investor radar")

    # Specialized bottleneck with known niche = narrative is still niche
    bottleneck_score = float(node.get("bottleneck_score", 50))
    if bottleneck_score >= 85 and coverage_status != "thin":
        score += 8
        reason_parts.append("Highly specialized bottleneck — niche narrative with limited crowding")

    score = max(10.0, min(95.0, score))
    return score, "; ".join(reason_parts) if reason_parts else "Mid-cap US supply-chain company"


def _compute_visibility_bucket(
    ticker: str,
    country: str,
    market_cap_usd: Optional[float],
    coverage_status: str,
    node: Dict[str, Any],
) -> str:
    """
    Classify candidate visibility.
    household | widely_covered | known | specialist | hidden
    """
    bc_score = float(node.get("bottleneck_score", 50))
    layer = node.get("layer", 2)

    if ticker in HOUSEHOLD_AI_NAMES:
        return "household"

    if country == "US" and market_cap_usd is not None:
        cap_m = market_cap_usd / 1e6
        if cap_m > 1e6:
            cap_m /= 1e6
        if cap_m > 30_000 and ticker not in WIDELY_COVERED_NAMES:
            return "widely_covered"

    if ticker in WIDELY_COVERED_NAMES:
        return "known"

    if country != "US":
        if coverage_status == "thin" or layer >= 3:
            return "hidden" if bc_score >= 70 else "specialist"
        return "specialist"

    if layer >= 3 and bc_score >= 75:
        return "specialist"

    return "known"


def _compute_chain_role_type(layer: int, bc_score: float) -> str:
    """
    Classify chain role.
    platform_anchor | direct_bottleneck | adjacent_supplier | indirect_beneficiary
    """
    if layer == 0:
        return "platform_anchor"
    if layer >= 3 and bc_score >= 80:
        return "direct_bottleneck"
    if layer == 2 or (layer >= 3 and bc_score >= 60):
        return "adjacent_supplier"
    return "indirect_beneficiary"


def _compute_confidence_penalties(
    ticker: str,
    country: str,
    market_cap_usd: Optional[float],
    coverage_status: str,
    data_confidence: str,
    adr_ticker: Optional[str],
    us_access_proxy: Optional[str],
) -> List[str]:
    """
    Explicit list of data quality issues.
    Thin data belongs HERE — not in hiddenness.
    """
    penalties: List[str] = []
    if coverage_status == "thin":
        penalties.append("Thin US data coverage — limited price/market cap data available")
    if data_confidence == "low":
        penalties.append("Low data confidence — key provider metrics unavailable")
    if country != "US" and not adr_ticker and not us_access_proxy:
        penalties.append("No US-listed proxy — foreign native only; limited US brokerage access")
    if adr_ticker and coverage_status in ("thin", "partial"):
        penalties.append(f"OTC ADR ({adr_ticker}) — wide bid-ask, limited US institutional liquidity")
    if market_cap_usd is None:
        penalties.append("Market cap data unavailable from current providers")
    return penalties


def _compute_data_gaps(
    market_cap_usd: Optional[float],
    price: Optional[float],
    coverage_status: str,
    node: Dict[str, Any],
) -> List[str]:
    """List of data fields that are missing or unreliable."""
    gaps: List[str] = []
    if market_cap_usd is None:
        gaps.append("market_cap_usd")
    if price is None:
        gaps.append("price")
    if not node.get("evidence"):
        gaps.append("evidence_sources")
    if coverage_status == "thin":
        gaps.extend(["earnings_data", "analyst_coverage"])
    return gaps


def _compute_why_now(
    themes: List[str],
    giant_anchors: List[str],
    scores: DiscoveryScores,
    node: Dict[str, Any],
) -> str:
    """Deterministic why-this-matters-now string from themes and anchors."""
    parts: List[str] = []

    for t in themes[:2]:
        if t in _THEME_WHY_NOW:
            parts.append(_THEME_WHY_NOW[t])
            break

    if giant_anchors:
        anchor = giant_anchors[0]
        giant_info = GIANT_MAP.get(anchor, {})
        if giant_info:
            parts.append(f"Anchored to {giant_info.get('name', anchor)} capex cycle ({giant_info.get('capex_scale', '')})")
        else:
            parts.append(f"Anchored to {anchor} supply chain")

    if scores.supply_chain_confidence_score >= 85:
        parts.append("Supply chain mapping is high-confidence with multiple corroborating evidence sources")

    return "; ".join(parts[:2]) if parts else "Theme activity is ongoing; supply chain position is validated by curated mapping"


def _compute_why_hidden(
    ticker: str,
    country: str,
    layer: int,
    visibility_bucket: str,
    coverage_status: str,
    adr_ticker: Optional[str],
    node: Dict[str, Any],
) -> str:
    """Deterministic why-this-is-hidden string."""
    parts: List[str] = []

    if visibility_bucket == "hidden":
        parts.append(f"Layer-{layer} supply chain position keeps this below mainstream investor radar")
    elif visibility_bucket == "specialist":
        parts.append("Specialist-level name known to sector professionals but not generalist investors")
    elif visibility_bucket in ("household", "widely_covered"):
        return "Widely covered — hiddenness advantage is limited"

    if country != "US":
        if adr_ticker and coverage_status in ("thin", "partial"):
            parts.append(f"Foreign ({country}) with OTC ADR only — limited US brokerage research coverage")
        elif not adr_ticker:
            parts.append(f"Foreign native ({country}) with no direct US-listed proxy — access barriers reduce coverage")
        else:
            parts.append(f"Foreign ({country}) — US coverage dominated by ETF-level analysis, not stock-specific")

    bc = float(node.get("bottleneck_score", 50))
    if bc >= 80 and visibility_bucket not in ("household", "widely_covered"):
        parts.append("Highly specialized role rarely covered outside sector-specialist research")

    return "; ".join(parts[:2]) if parts else "Chain position is less visible than revenue exposure suggests"


def _compute_what_to_verify_next(
    chain_role_type: str,
    themes: List[str],
    country: str,
) -> str:
    """Deterministic next-step verification guidance."""
    base = _ROLE_TYPE_VERIFY.get(chain_role_type, "Review recent earnings for supply chain commentary")

    for t in themes:
        if t in _THEME_VERIFY_HINTS:
            return f"{base}. {_THEME_VERIFY_HINTS[t]}"

    if country != "US":
        return f"{base}. Review FX impact and local market commentary for earnings translation risk"

    return base


def _compute_best_blend_score(
    scores: DiscoveryScores,
    data_confidence: str,
    confidence_penalties: List[str],
) -> float:
    """
    Weighted composite score for 'best surfaced first' ranking (Phase 5 tuned).

    Formula:
      bottleneck_criticality  28%  — primary quality signal
      chain_depth             18%  — supply-chain obscurity
      hiddenness              14%  — narrative visibility
      supply_chain_confidence 14%  — evidence quality
      theme_purity            10%  — pure-play vs conglomerate
      giant_dependency        10%  — anchored to large-cap demand
      proxy_accessibility      6%  — US investor tradability (rewards ADRs over native-only)

    Multipliers:
      data_confidence: low → 0.82, medium → 0.93
      penalty count:   >=3 → additional 0.90x, >=2 → 0.95x
    """
    raw = (
        scores.bottleneck_criticality_score  * 0.28 +
        scores.chain_depth_score             * 0.18 +
        scores.hiddenness_score              * 0.14 +
        scores.supply_chain_confidence_score * 0.14 +
        scores.theme_purity_score            * 0.10 +
        scores.giant_dependency_score        * 0.10 +
        scores.proxy_accessibility_score     * 0.06
    )

    mult = 1.0
    if data_confidence == "low":
        mult = 0.82
    elif data_confidence == "medium":
        mult = 0.93

    if len(confidence_penalties) >= 3:
        mult *= 0.90
    elif len(confidence_penalties) >= 2:
        mult *= 0.95

    return round(raw * mult, 1)


# ── Theme-based thesis-break signals ─────────────────────────────────────────

_THEME_THESIS_BREAK: Dict[str, str] = {
    "memory_hbm":              "Memory capex cycle downturn, HBM pricing collapse, or customer shift to alternative memory architecture",
    "photonics_cpo":           "CPO adoption slower than expected or hyperscalers internalize silicon photonics without specialist suppliers",
    "semicap_supply_chain":    "WFE demand contraction, major customer fab capex cuts, or equipment market share shift to rival",
    "advanced_packaging_test": "Loss of packaging qualification at TSMC/Samsung or new qualified entrant in ABF substrate / bonding market",
    "soi_substrates_materials":"New commercial entrant breaking sole-source substrate position or RF-SOI technology disruption",
    "ai_power_energy":         "AI datacenter power demand flattening or utility grid capacity relief faster than expected",
    "grid_transformers":       "Utility capex cuts or transformer supply chain relief reducing backlog growth",
    "defense_optics":          "Defense budget sequestration or loss of key program contract / incumbent re-qualification",
    "cooling_thermal":         "AI rack cooling commoditization or hyperscaler shift to standardized non-specialist cooling",
    "energy_transition":       "SiC device demand slowdown from EV market softness or SiC commoditization by new entrants",
    "ai_infrastructure":       "AI capex slowdown, hyperscaler spend cuts, or GPU architecture shift reducing demand",
    "neocloud":                "GPU allocation tightening for neocloud operators or hyperscaler in-house compute substitution",
    "space_sensing":           "Program cancellation, budget cuts, or satellite constellation delays",
}

_ROLE_THESIS_BREAK: Dict[str, str] = {
    "direct_bottleneck":    "New qualified alternative supplier breaking sole-source position or technology disruption of this layer",
    "adjacent_supplier":    "Customer vertical integration into this tier or demand shift reducing exposure to this supply chain position",
    "indirect_beneficiary": "Theme growth decelerates or this company loses relevance as the chain matures/commoditizes",
    "platform_anchor":      "Platform capex reduction, competitive loss, or technology disruption at the anchor itself",
}


def _compute_what_would_break_thesis(
    chain_role_type: str,
    themes: List[str],
    country: str,
    bc_score: float,
) -> str:
    """Deterministic thesis-break condition derived from role type, themes, and country."""
    parts: List[str] = []

    # Theme-specific primary break signal (use most relevant theme)
    for t in themes:
        if t in _THEME_THESIS_BREAK:
            parts.append(_THEME_THESIS_BREAK[t])
            break

    # Role-type secondary break signal
    role_break = _ROLE_THESIS_BREAK.get(chain_role_type, "")
    if role_break and (not parts or role_break not in parts[0]):
        parts.append(role_break)

    # Foreign currency / geopolitical risk for non-US
    if country not in ("US", "IL"):
        if country in ("JP", "KR", "TW"):
            parts.append(f"Currency risk ({country} FX) combined with US-imposed trade restrictions or export controls")
        elif country in ("NL", "DE", "FR"):
            parts.append(f"EU regulatory risk or geopolitical export control expansion affecting {country} supply chain")

    return "; ".join(parts[:2]) if parts else "Competitive or macroeconomic deterioration in the primary theme area"


def _compute_crowding_flags(
    ticker: str,
    visibility_bucket: str,
    themes: List[str],
    country: str,
    market_cap_usd: Optional[float],
) -> List[str]:
    """
    Detect potential crowding signals — deterministic, no LLM.
    Crowding here means the name may already be widely owned/followed,
    reducing the alpha opportunity from discovery surfacing.
    """
    flags: List[str] = []

    if visibility_bucket == "household":
        flags.append("Household name — institutional ownership is likely elevated and crowding risk is high")
    elif visibility_bucket == "widely_covered":
        flags.append("Widely covered by sell-side — analyst crowding may have already priced in the theme")

    # High-momentum AI themes with wide retail/institutional awareness
    crowded_themes = {"ai_infrastructure", "memory_hbm", "neocloud"}
    if any(t in crowded_themes for t in themes):
        if ticker in {"NVDA", "AMD", "MU", "SMCI", "MSFT", "GOOGL", "META", "AMZN"}:
            flags.append("Core AI momentum name — retail and institutional positioning likely extended")

    # Large-cap US names — not a penalty per se, but flag it
    if country == "US" and market_cap_usd is not None:
        cap_bn = market_cap_usd / 1e9
        if cap_m := cap_bn:
            if cap_m > 100:
                flags.append("Large-cap ($100B+) — limited surprise discovery upside from coverage angle")

    return flags


def _compute_coverage_notes(
    country: str,
    coverage_status: str,
    data_confidence: str,
    adr_ticker: Optional[str],
    us_access_proxy: Optional[str],
    evidence: List[str],
    node: Dict[str, Any],
) -> str:
    """Brief structured note on data coverage quality for frontend display."""
    if coverage_status == "full" and data_confidence == "high":
        ev_count = len(evidence)
        return f"Full coverage — {ev_count} curated evidence point{'s' if ev_count != 1 else ''}"

    parts: List[str] = []

    if coverage_status == "thin":
        parts.append("Thin US coverage — limited market data from US providers")
    elif coverage_status == "partial":
        parts.append("Partial coverage — main financial data via native exchange")

    if data_confidence == "low":
        parts.append("Low data confidence")
    elif data_confidence == "medium":
        parts.append("Medium confidence")

    if country != "US":
        if adr_ticker:
            parts.append(f"US access via {adr_ticker} OTC ADR")
        elif us_access_proxy:
            parts.append(f"ETF proxy available ({us_access_proxy})")
        else:
            parts.append("No US-listed proxy — native exchange only")

    return "; ".join(parts) if parts else "Coverage not fully determined"


def _compute_comparable_names(
    ticker: str,
    themes: List[str],
    layer: int,
) -> List[str]:
    """Find peer tickers in the same themes and similar chain layer."""
    peers: List[str] = []
    theme_set = set(themes)
    for t, n in NODE_REGISTRY.items():
        if n is None or t == ticker:
            continue
        if not (set(n.get("themes", [])) & theme_set):
            continue
        if abs(n.get("layer", 2) - layer) > 1:
            continue
        canon = n.get("us_access_proxy", t) if n.get("country", "US") != "US" else t
        if canon != ticker and canon not in peers:
            peers.append(canon)
        if len(peers) >= 4:
            break
    return peers


def _giant_dependency_score(node: Dict[str, Any]) -> float:
    """How directly tied to a major platform giant?"""
    anchors = node.get("giant_anchors", [])
    if not anchors:
        return 30.0
    if len(anchors) == 1:
        return 60.0
    if len(anchors) == 2:
        return 72.0
    if len(anchors) >= 3:
        return 82.0
    return 50.0


def _foreign_uniqueness_score(country: str, has_adr: bool, coverage: str) -> float:
    """Unique foreign exposure not easily accessible."""
    if country == "US":
        return 20.0
    if not has_adr:
        return 82.0 if coverage == "thin" else 72.0
    return 58.0


def _supply_chain_confidence_score(node: Dict[str, Any]) -> float:
    """Evidence quality → confidence score."""
    conf_label = node.get("confidence", "medium")
    evidence   = node.get("evidence", [])
    base = {"high": 85.0, "medium": 65.0, "low": 45.0}.get(conf_label, 60.0)
    bonus = min(10.0, len(evidence) * 3.0)
    return min(98.0, base + bonus)


def _proxy_accessibility_score(country: str, has_us_proxy: bool, adr_ticker: Optional[str]) -> float:
    """How easily tradable from a US investor perspective."""
    if country == "US":
        return 100.0
    if adr_ticker:
        return 72.0
    if has_us_proxy:
        return 55.0
    return 30.0


def _theme_purity_score(themes: List[str]) -> float:
    """Pure-play vs conglomerate. Fewer themes = purer = higher score."""
    n = len(themes)
    if n == 0:
        return 40.0
    if n == 1:
        return 95.0
    if n == 2:
        return 80.0
    if n == 3:
        return 65.0
    return 50.0


def _build_candidate(
    ticker: str,
    node: Dict[str, Any],
    market_cap_usd: Optional[float] = None,
    price: Optional[float] = None,
    extra_giant_anchors: Optional[List[str]] = None,
    coverage_override: Optional[str] = None,
) -> DiscoveryCandidate:
    """
    Construct a DiscoveryCandidate from a NODE_REGISTRY entry + optional live data.
    Phase 4: populates all new fields including best_blend_score, visibility_bucket,
    chain_role_type, confidence_penalties, data_gaps, and all why_* fields.
    """
    country         = node.get("country", "US")
    layer           = node.get("layer", 2)
    themes          = node.get("themes", [])
    us_access_proxy = node.get("us_access_proxy")
    adr_ticker      = node.get("adr_ticker")
    evidence        = node.get("evidence", [])
    giant_anchors   = list(set(node.get("giant_anchors", []) + (extra_giant_anchors or [])))

    # Determine coverage status and data confidence
    if country != "US":
        foreign_meta    = FOREIGN_ACCESS_MAP.get(ticker, {})
        coverage_status = coverage_override or foreign_meta.get("coverage_status", "partial")
        data_confidence = foreign_meta.get("data_confidence", "medium")
        direct_tradable = bool(us_access_proxy or adr_ticker)
    else:
        coverage_status = coverage_override or "full"
        data_confidence = "high"
        direct_tradable = True

    # Score all 8 original dimensions
    hidden_score, hidden_reason = _hiddenness_score(
        ticker, country, market_cap_usd, coverage_status, node
    )
    bc_score = float(node.get("bottleneck_score", 50))
    scores = DiscoveryScores(
        chain_depth_score=_chain_depth_score(layer),
        bottleneck_criticality_score=bc_score,
        hiddenness_score=hidden_score,
        giant_dependency_score=_giant_dependency_score(node),
        foreign_uniqueness_score=_foreign_uniqueness_score(country, bool(adr_ticker), coverage_status),
        supply_chain_confidence_score=_supply_chain_confidence_score(node),
        proxy_accessibility_score=_proxy_accessibility_score(country, bool(us_access_proxy), adr_ticker),
        theme_purity_score=_theme_purity_score(themes),
    )

    # Phase 4 — new derived fields
    visibility_bucket  = _compute_visibility_bucket(ticker, country, market_cap_usd, coverage_status, node)
    chain_role_type    = _compute_chain_role_type(layer, bc_score)
    confidence_penalties = _compute_confidence_penalties(
        ticker, country, market_cap_usd, coverage_status, data_confidence, adr_ticker, us_access_proxy
    )
    data_gaps = _compute_data_gaps(market_cap_usd, price, coverage_status, node)
    why_now   = _compute_why_now(themes, giant_anchors, scores, node)
    why_hidden = _compute_why_hidden(ticker, country, layer, visibility_bucket, coverage_status, adr_ticker, node)
    what_to_verify_next = _compute_what_to_verify_next(chain_role_type, themes, country)
    comparable_names = _compute_comparable_names(ticker, themes, layer)
    best_blend_score = _compute_best_blend_score(scores, data_confidence, confidence_penalties)

    # Phase 5 — additional explanation fields
    what_would_break_thesis = _compute_what_would_break_thesis(chain_role_type, themes, country, bc_score)
    coverage_notes = _compute_coverage_notes(
        country, coverage_status, data_confidence, adr_ticker, us_access_proxy, evidence, node
    )
    crowding_flags = _compute_crowding_flags(ticker, visibility_bucket, themes, country, market_cap_usd)

    # Build thesis and fit reasoning
    role   = node.get("role", "")
    thesis = _build_thesis(ticker, node.get("company_name", ticker), layer, scores, role, themes)
    fit_reasoning = _build_fit_reasoning(node, scores, evidence)

    return DiscoveryCandidate(
        ticker=ticker,
        company_name=node.get("company_name", ticker),
        country=country,
        exchange=node.get("exchange", ""),
        themes=themes,
        chain_layers=[role],
        layer_depth=layer,
        scores=scores,
        chain_depth_score=scores.chain_depth_score,
        bottleneck_criticality_score=scores.bottleneck_criticality_score,
        hiddenness_score=scores.hiddenness_score,
        supply_chain_confidence_score=scores.supply_chain_confidence_score,
        best_blend_score=best_blend_score,
        visibility_bucket=visibility_bucket,
        chain_role_type=chain_role_type,
        confidence_penalties=confidence_penalties,
        data_gaps=data_gaps,
        why_now=why_now,
        why_hidden=why_hidden,
        what_to_verify_next=what_to_verify_next,
        # Phase 5 additions
        what_would_break_thesis=what_would_break_thesis,
        coverage_notes=coverage_notes,
        crowding_flags=crowding_flags,
        comparable_names=comparable_names,
        thesis_summary=thesis,
        fit_reasoning=fit_reasoning,
        giant_anchors=giant_anchors,
        us_access_proxy=us_access_proxy,
        adr_ticker=adr_ticker,
        coverage_status=coverage_status,
        data_confidence=data_confidence,
        direct_tradable=direct_tradable,
        market_cap_usd=market_cap_usd,
        price=price,
        hiddenness_reason=hidden_reason,
        enriched=market_cap_usd is not None,
    )


def _build_thesis(
    ticker: str,
    company_name: str,
    layer: int,
    scores: DiscoveryScores,
    role: str,
    themes: List[str],
) -> str:
    """Build a concise thesis string from discovery scores."""
    depth_adj = {0: "platform-level", 1: "tier-1 supplier", 2: "component-level",
                 3: "bottleneck-positioned", 4: "upstream materials"}.get(layer, "supply-chain")
    bc_score  = scores.bottleneck_criticality_score
    conviction = "high" if bc_score >= 80 else "moderate" if bc_score >= 60 else "lower"
    theme_str  = themes[0] if themes else "supply-chain"
    return (
        f"{company_name} is a {depth_adj} name in {theme_str} "
        f"with {conviction} bottleneck criticality ({bc_score:.0f}/100). "
        f"{role[:80] if role else ''}"
    ).strip()


def _build_fit_reasoning(
    node: Dict[str, Any],
    scores: DiscoveryScores,
    evidence: List[str],
) -> List[str]:
    """Build fit reasoning bullets from node data and scores."""
    bullets: List[str] = []
    if evidence:
        bullets.append(evidence[0])
    if scores.bottleneck_criticality_score >= 80:
        bullets.append(f"Bottleneck criticality: {scores.bottleneck_criticality_score:.0f}/100 — hard to substitute")
    if scores.chain_depth_score >= 80:
        bullets.append(f"Deep supply chain position (layer {node.get('layer', 2)}) — low investor visibility")
    if scores.foreign_uniqueness_score >= 70:
        bullets.append(f"Foreign ({node.get('country', '?')}) — limited US analyst coverage creates alpha opportunity")
    if node.get("us_access_proxy"):
        bullets.append(f"US access via proxy: {node['us_access_proxy']}")
    return bullets[:4]


def _rank_candidates(candidates: List[DiscoveryCandidate], only_hidden: bool = False) -> List[DiscoveryCandidate]:
    """
    Rank candidates by best_blend_score (Phase 4 primary ranking key).
    Falls back to legacy composite if best_blend_score is 0.
    """
    filtered = [c for c in candidates if c.scores.hiddenness_score >= 55] if only_hidden else candidates
    return sorted(filtered, key=lambda c: c.best_blend_score, reverse=True)


def _assign_bucket_positions(bucket: List[DiscoveryCandidate]) -> List[DiscoveryCandidate]:
    """Assign 1-based position_in_bucket to each candidate in a bucket list (mutates in place)."""
    for i, c in enumerate(bucket):
        c = c.model_copy(update={"position_in_bucket": i + 1})
        bucket[i] = c
    return bucket


def _build_ranking_buckets(
    candidates: List[DiscoveryCandidate],
    limit: int = 5,
) -> Dict[str, List[DiscoveryCandidate]]:
    """
    Compute named ranking buckets from all scored candidates (Phase 5 enhanced).
    - Buckets use more distinct criteria to minimize overlap.
    - Each candidate in a bucket receives position_in_bucket (1-based rank within that bucket).
    Returns dict with bucket_name → list of top candidates.
    """
    # Bucket 1 — Hidden bottlenecks: deeply hidden AND high bottleneck score (threshold raised to 75)
    top_hidden = sorted(
        [c for c in candidates
         if c.hiddenness_score >= 60 and c.bottleneck_criticality_score >= 75
         and c.visibility_bucket in ("hidden", "specialist")],
        key=lambda c: c.best_blend_score, reverse=True,
    )[:limit]

    # Bucket 2 — Direct chokepoints: sole/dual-source bottleneck roles, ranked by bottleneck score
    top_chokepoints = sorted(
        [c for c in candidates
         if c.chain_role_type == "direct_bottleneck" and c.bottleneck_criticality_score >= 70],
        key=lambda c: c.bottleneck_criticality_score, reverse=True,
    )[:limit]

    # Bucket 3 — Foreign specialists: non-US, specialist/hidden, has ADR or curated foreign coverage
    top_foreign = sorted(
        [c for c in candidates
         if c.country != "US"
         and c.visibility_bucket in ("specialist", "hidden")
         and (c.adr_ticker or c.us_access_proxy)],
        key=lambda c: c.best_blend_score, reverse=True,
    )[:limit]

    # Bucket 4 — US-accessible foreign proxies: foreign node with partial+ US access (ADR, not thin)
    top_us_proxies = sorted(
        [c for c in candidates
         if c.country != "US"
         and c.adr_ticker is not None
         and c.coverage_status in ("partial", "full")],
        key=lambda c: c.best_blend_score, reverse=True,
    )[:limit]

    # Bucket 5 — Highest confidence: high data_confidence + high supply chain confidence
    highest_confidence = sorted(
        [c for c in candidates
         if c.data_confidence == "high" and c.supply_chain_confidence_score >= 82],
        key=lambda c: c.supply_chain_confidence_score, reverse=True,
    )[:limit]

    # Bucket 6 — Best blend: global ranking by composite score (the default catch-all)
    best_blend = sorted(candidates, key=lambda c: c.best_blend_score, reverse=True)[:limit]

    return {
        "top_hidden_bottlenecks":              _assign_bucket_positions(top_hidden),
        "top_direct_chokepoints":              _assign_bucket_positions(top_chokepoints),
        "top_foreign_specialists":             _assign_bucket_positions(top_foreign),
        "top_us_accessible_foreign_proxies":   _assign_bucket_positions(top_us_proxies),
        "highest_confidence_candidates":       _assign_bucket_positions(highest_confidence),
        "best_blend_candidates":               _assign_bucket_positions(best_blend),
    }


def _build_summary(
    mode: str,
    top_candidates: List[DiscoveryCandidate],
    req: DiscoverRequest,
) -> str:
    """Generate a concise natural-language summary of discovery results."""
    if not top_candidates:
        return "No matching supply-chain candidates found with the given filters."

    top_names = [f"{c.ticker} ({c.best_blend_score:.0f})" for c in top_candidates[:3]]
    lead = f"Top surfaced ideas: {', '.join(top_names)}. "

    if top_candidates:
        best = top_candidates[0]
        lead += f"{best.ticker} leads — {best.thesis_summary[:100]}. "

    foreign_count = sum(1 for c in top_candidates if c.country != "US")
    if foreign_count > 0:
        lead += f"{foreign_count} foreign name(s) included with ADR/proxy guidance. "

    if mode == "giant_chain" and req.giant:
        lead += f"Discovery anchored on {req.giant} supply chain."
    elif mode == "theme_scan" and req.theme_ids:
        lead += f"Theme scan: {', '.join(req.theme_ids[:3])}."

    return lead.strip()


# ── Mode handlers ──────────────────────────────────────────────────────────────

def _candidates_from_nodes(
    node_items: List[tuple],
    include_foreign: bool,
    country_filters: List[str],
    max_depth: int,
) -> List[DiscoveryCandidate]:
    """Build candidates from (ticker, node_dict) pairs."""
    candidates: List[DiscoveryCandidate] = []
    seen: Set[str] = set()

    for ticker, node in node_items:
        if node is None:
            continue
        country = node.get("country", "US")
        if not include_foreign and country != "US":
            continue
        if country_filters and country not in country_filters:
            continue
        if node.get("layer", 2) > max_depth:
            continue

        canon = (node.get("us_access_proxy") or ticker) if country != "US" else ticker
        if canon in seen:
            continue
        seen.add(canon)

        c = _build_candidate(canon, node)
        candidates.append(c)
    return candidates


def _mode_giant_chain(req: DiscoverRequest) -> List[DiscoveryCandidate]:
    """Giant chain mode: traverse from a giant anchor."""
    giant_id = (req.giant or "").strip()
    if not giant_id:
        return []

    giant = get_giant(giant_id)
    relevant_themes = set(giant.get("themes", [])) if giant else set()
    if req.theme_ids:
        relevant_themes = (relevant_themes & set(req.theme_ids)) if relevant_themes else set(req.theme_ids)

    items: List[tuple] = []
    for ticker, node in NODE_REGISTRY.items():
        if node is None:
            continue
        node_themes = set(node.get("themes", []))
        if relevant_themes and not (node_themes & relevant_themes):
            continue
        items.append((ticker, node))

    return _candidates_from_nodes(items, req.include_foreign, req.country_filters, req.max_depth)


def _mode_theme_scan(req: DiscoverRequest) -> List[DiscoveryCandidate]:
    """Theme scan mode: find all companies across specified themes."""
    if not req.theme_ids:
        req_themes = [tid for tid, meta in THEME_TAXONOMY.items()
                      if meta.get("serenity_priority") == "high"]
    else:
        req_themes = req.theme_ids
    theme_set = set(req_themes)

    items: List[tuple] = []
    for ticker, node in NODE_REGISTRY.items():
        if node is None:
            continue
        if not (set(node.get("themes", [])) & theme_set):
            continue
        items.append((ticker, node))

    return _candidates_from_nodes(items, req.include_foreign, req.country_filters, req.max_depth)


def _mode_foreign_bottlenecks(req: DiscoverRequest) -> List[DiscoveryCandidate]:
    """Foreign bottleneck mode: only non-US companies."""
    items: List[tuple] = []
    country_filters = req.country_filters or []

    for ticker, node in NODE_REGISTRY.items():
        if node is None:
            continue
        country = node.get("country", "US")
        if country == "US":
            continue
        if country_filters and country not in country_filters:
            continue
        if node.get("layer", 2) > req.max_depth:
            continue
        items.append((ticker, node))

    candidates: List[DiscoveryCandidate] = []
    seen: Set[str] = set()
    for ticker, node in items:
        if node is None:
            continue
        canon = node.get("us_access_proxy") or ticker
        if canon in seen:
            continue
        seen.add(canon)
        c = _build_candidate(canon, node)
        candidates.append(c)
    return candidates


def _mode_ticker_chain(req: DiscoverRequest) -> List[DiscoveryCandidate]:
    """
    Ticker chain mode: given a known ticker, find related upstream/downstream nodes
    that share themes with the given ticker.
    """
    ticker = (req.ticker or "").upper().strip()
    if not ticker:
        return []

    anchor_node = NODE_REGISTRY.get(ticker)
    if anchor_node is None:
        for t, n in NODE_REGISTRY.items():
            if n and n.get("us_access_proxy", "").upper() == ticker:
                anchor_node = n
                break

    if anchor_node is None:
        from services.playbook.theme_map import MANUAL_THEME_MAP
        anchor_themes = set(MANUAL_THEME_MAP.get(ticker, []))
    else:
        anchor_themes = set(anchor_node.get("themes", []))

    if not anchor_themes:
        return []

    if req.theme_ids:
        anchor_themes = anchor_themes & set(req.theme_ids)

    items: List[tuple] = []
    for t, node in NODE_REGISTRY.items():
        if node is None:
            continue
        if t == ticker:
            continue
        if not (set(node.get("themes", [])) & anchor_themes):
            continue
        items.append((t, node))

    return _candidates_from_nodes(items, req.include_foreign, req.country_filters, req.max_depth)


def _mode_country_theme_scan(req: DiscoverRequest) -> List[DiscoveryCandidate]:
    """Country+theme cross-filter."""
    country_set = set(req.country_filters) if req.country_filters else {"US", "JP", "KR", "TW", "NL", "DE", "FR", "GB"}
    theme_set   = set(req.theme_ids) if req.theme_ids else set(THEME_TAXONOMY.keys())

    items: List[tuple] = []
    for ticker, node in NODE_REGISTRY.items():
        if node is None:
            continue
        if node.get("country", "US") not in country_set:
            continue
        if not (set(node.get("themes", [])) & theme_set):
            continue
        items.append((ticker, node))

    return _candidates_from_nodes(items, True, req.country_filters, req.max_depth)


def _mode_custom(req: DiscoverRequest) -> List[DiscoveryCandidate]:
    """Custom mode: run theme_scan + optionally giant_chain as fallback."""
    if req.giant:
        return _mode_giant_chain(req)
    if req.theme_ids:
        return _mode_theme_scan(req)
    return _mode_theme_scan(req)


# ── Main discovery orchestrator ────────────────────────────────────────────────

def _apply_preset_mode(req: DiscoverRequest) -> DiscoverRequest:
    """
    Mutate request (immutably) to match a preset_mode profile.
    Returns a new DiscoverRequest with appropriate mode/filter overrides.
    """
    pm = (req.preset_mode or "").lower().strip()
    if not pm:
        return req

    overrides: Dict[str, Any] = {}

    if pm == "hidden_bottlenecks":
        overrides["mode"]            = "theme_scan"
        overrides["only_hidden"]     = True
        overrides["include_foreign"] = True
        overrides["sort_mode"]       = "hiddenness"

    elif pm == "top_direct_chokepoints":
        overrides["mode"]            = "theme_scan"
        overrides["include_foreign"] = True
        overrides["only_hidden"]     = False
        overrides["sort_mode"]       = "bottleneck"

    elif pm == "foreign_specialists":
        overrides["mode"]            = "foreign_bottlenecks"
        overrides["include_foreign"] = True
        overrides["only_hidden"]     = False
        overrides["sort_mode"]       = "best_blend"

    elif pm == "us_accessible_foreign_proxies":
        overrides["mode"]            = "foreign_bottlenecks"
        overrides["include_foreign"] = True
        overrides["only_hidden"]     = False
        # filter further in mode to only those with ADR proxies
        overrides["sort_mode"]       = "best_blend"

    elif pm == "highest_confidence":
        overrides["mode"]            = "theme_scan"
        overrides["include_foreign"] = True
        overrides["only_hidden"]     = False
        overrides["sort_mode"]       = "confidence"

    elif pm == "best_blend":
        overrides["mode"]            = "theme_scan"
        overrides["include_foreign"] = True
        overrides["only_hidden"]     = False
        overrides["sort_mode"]       = "best_blend"

    return req.model_copy(update=overrides)


def _rank_by_sort_mode(
    candidates: List[DiscoveryCandidate],
    sort_mode: Optional[str],
    only_hidden: bool = False,
) -> List[DiscoveryCandidate]:
    """
    Apply sort_mode override to ranking.
    Supported sort_modes: best_blend | hiddenness | bottleneck | confidence | chain_depth
    Falls back to best_blend_score for unknown modes.
    """
    if only_hidden:
        candidates = [c for c in candidates if c.scores.hiddenness_score >= 55]

    sm = (sort_mode or "best_blend").lower().strip()

    if sm == "hiddenness":
        return sorted(candidates, key=lambda c: c.hiddenness_score, reverse=True)
    elif sm == "bottleneck":
        return sorted(candidates, key=lambda c: c.bottleneck_criticality_score, reverse=True)
    elif sm == "confidence":
        return sorted(candidates, key=lambda c: c.supply_chain_confidence_score, reverse=True)
    elif sm == "chain_depth":
        return sorted(candidates, key=lambda c: (c.scores.chain_depth_score, c.best_blend_score), reverse=True)
    else:
        return sorted(candidates, key=lambda c: c.best_blend_score, reverse=True)


async def run_discover(req: DiscoverRequest) -> DiscoverResponse:
    """
    Run the discovery engine for a given request.
    Orchestrates mode-specific candidate building, scoring, ranking, and enrichment.
    Phase 5: adds preset_mode dispatch, sort_mode override, position_in_bucket on ranking
    buckets, and three new candidate-level explanation fields.
    """
    # Phase 5 — apply preset_mode to override mode/filter defaults
    req = _apply_preset_mode(req)

    finnhub_key     = os.getenv("FINNHUB_API_KEY", "")
    tradier_key     = os.getenv("TRADIER_API_KEY", "")
    fmp_key         = os.getenv("FMP_API_KEY", "")
    perp_key        = os.getenv("PERPLEXITY_API_KEY", "")
    tradier_sandbox = os.getenv("TRADIER_SANDBOX", "false").lower() == "true"

    from services.playbook.discovery_enrichment import (
        enrich_batch_finnhub,
        enrich_us_quotes_tradier,
        fmp_market_cap,
        validate_shortlist_perplexity,
    )

    # ── 1. Run mode-specific candidate extraction ──────────────────────────────
    mode = req.mode.lower().strip()

    if mode == "giant_chain":
        candidates = _mode_giant_chain(req)
    elif mode == "theme_scan":
        candidates = _mode_theme_scan(req)
    elif mode == "foreign_bottlenecks":
        candidates = _mode_foreign_bottlenecks(req)
    elif mode == "ticker_chain":
        candidates = _mode_ticker_chain(req)
    elif mode == "country_theme_scan":
        candidates = _mode_country_theme_scan(req)
    else:
        candidates = _mode_custom(req)

    # ── 2. Rank candidates (sort_mode aware) ──────────────────────────────────
    ranked    = _rank_by_sort_mode(candidates, req.sort_mode, only_hidden=req.only_hidden)
    shortlist = ranked[:min(req.limit * 2, 40)]

    # ── 3. Finnhub enrichment for shortlist ───────────────────────────────────
    us_tickers     = [c.ticker for c in shortlist if c.country == "US"]
    adr_tickers    = [c.adr_ticker for c in shortlist if c.adr_ticker and c.country != "US"]
    enrich_tickers = (us_tickers + adr_tickers)[:20]

    finnhub_profiles: Dict[str, Any] = {}
    if finnhub_key and enrich_tickers:
        try:
            finnhub_profiles = await enrich_batch_finnhub(enrich_tickers, finnhub_key)
        except Exception as e:
            print(f"[DISCOVERY] Finnhub batch enrich error: {e}")

    # ── 4. Tradier quote enrichment for US/ADR names ──────────────────────────
    tradier_quotes: Dict[str, Any] = {}
    tradier_tickers = [t for t in enrich_tickers if t in us_tickers][:10]
    if tradier_key and tradier_tickers:
        try:
            tradier_quotes = await enrich_us_quotes_tradier(tradier_tickers, tradier_key, tradier_sandbox)
        except Exception as e:
            print(f"[DISCOVERY] Tradier batch quote error: {e}")

    # ── 5. Apply enrichment data + re-compute all Phase 4 fields ──────────────
    enriched_candidates: List[DiscoveryCandidate] = []
    for c in shortlist:
        lookup_key = c.ticker if c.ticker in finnhub_profiles else (c.adr_ticker or "")
        fh = finnhub_profiles.get(lookup_key, {})
        tq = tradier_quotes.get(c.ticker, {})

        market_cap = None
        if fh.get("market_cap"):
            market_cap = float(fh["market_cap"]) * 1e6  # Finnhub in millions → USD

        price = tq.get("price") or fh.get("price")

        if market_cap is not None:
            # Find the original node to fully re-score with real cap data
            node = None
            for t, n in NODE_REGISTRY.items():
                if n and (n.get("us_access_proxy", t) == c.ticker or t.upper() == c.ticker.upper()):
                    node = n
                    break
            if node:
                # Full rebuild with live data — all Phase 4 fields re-derived
                c = _build_candidate(
                    c.ticker, node,
                    market_cap_usd=market_cap,
                    price=price,
                )
            else:
                c = c.model_copy(update={"market_cap_usd": market_cap, "price": price, "enriched": True})
        elif price:
            c = c.model_copy(update={"price": price, "enriched": True})

        enriched_candidates.append(c)

    # ── 6. Perplexity validation on finalists (if requested) ──────────────────
    perp_notes: List[str] = []
    if req.use_web_validation and perp_key and enriched_candidates:
        top_for_validation = [
            {"ticker": c.ticker, "company_name": c.company_name, "role": c.chain_layers[0] if c.chain_layers else ""}
            for c in enriched_candidates[:5]
        ]
        try:
            validation = await validate_shortlist_perplexity(top_for_validation, perp_key, max_validate=5)
            for ticker, val in validation.items():
                if val.get("confirmed"):
                    perp_notes.append(f"Perplexity confirmed supply-chain role for {ticker}")
                else:
                    perp_notes.append(f"Perplexity could not confirm role for {ticker}")
        except Exception as e:
            print(f"[DISCOVERY] Perplexity validation error: {e}")
            perp_notes.append(f"Perplexity validation unavailable: {e}")

    # ── 7. Final ranking and split (sort_mode aware) ───────────────────────────
    final_ranked = _rank_by_sort_mode(enriched_candidates, req.sort_mode, only_hidden=req.only_hidden)
    limit        = max(1, min(req.limit, 30))

    top_candidates = final_ranked[:limit]
    low_confidence = [c for c in final_ranked[limit:] if c.scores.supply_chain_confidence_score < 65][:5]

    # ── 8. Phase 4 — compute ranking buckets over ALL enriched candidates ──────
    ranking_buckets = _build_ranking_buckets(final_ranked, limit=5)

    # ── 9. Chain map preview (layer breakdown for top theme) ──────────────────
    chain_map_preview: Dict[str, Any] = {}
    preview_theme = req.theme_ids[0] if req.theme_ids else None
    if preview_theme:
        layers = get_chain_for_theme(preview_theme, req.max_depth, req.include_foreign, req.country_filters)
        chain_map_preview = {
            "theme":  preview_theme,
            "layers": [
                {
                    "layer": cl.layer_index,
                    "label": cl.label,
                    "tickers": [n.ticker for n in cl.nodes[:5]],
                }
                for cl in layers
            ],
        }

    # ── 10. ADR/ETF proxy suggestions for low-access foreign ──────────────────
    proxy_suggestions: Dict[str, str] = {}
    if req.include_adr_or_etf_proxies:
        for c in top_candidates:
            if c.country != "US" and not c.adr_ticker:
                for theme in c.themes[:1]:
                    etfs = get_etf_proxies_for_theme(theme)
                    if etfs:
                        proxy_suggestions[c.ticker] = etfs[0]

    # ── 11. Build summary and response ────────────────────────────────────────
    summary = _build_summary(mode, top_candidates, req)

    countries_scanned = list({c.country for c in top_candidates + low_confidence})
    provider_notes = ["Finnhub primary enrichment for profile/news"]
    if tradier_quotes:
        provider_notes.append("Tradier quote enrichment for US-listed and ADR names")
    if fmp_key:
        provider_notes.append("FMP market cap (sparing — 250/day cap)")
    if req.use_web_validation and perp_notes:
        provider_notes.extend(perp_notes[:3])
    provider_notes.append("No Brave/Tavily used in this flow")

    return DiscoverResponse(
        playbook_id=req.playbook_id,
        mode=mode,
        query=req.query,
        summary=summary,
        top_candidates=top_candidates,
        low_confidence_candidates=low_confidence,
        chain_map_preview=chain_map_preview,
        # Phase 4 ranking buckets
        top_hidden_bottlenecks=ranking_buckets["top_hidden_bottlenecks"],
        top_direct_chokepoints=ranking_buckets["top_direct_chokepoints"],
        top_foreign_specialists=ranking_buckets["top_foreign_specialists"],
        top_us_accessible_foreign_proxies=ranking_buckets["top_us_accessible_foreign_proxies"],
        highest_confidence_candidates=ranking_buckets["highest_confidence_candidates"],
        best_blend_candidates=ranking_buckets["best_blend_candidates"],
        meta={
            "total_candidates_found":  len(candidates),
            "total_after_ranking":     len(final_ranked),
            "returned":               len(top_candidates),
            "low_confidence_returned": len(low_confidence),
            "countries_scanned":      countries_scanned,
            "foreign_enabled":        req.include_foreign,
            "max_depth":              req.max_depth,
            "only_hidden":            req.only_hidden,
            "proxy_suggestions":      proxy_suggestions,
            "provider_notes":         provider_notes,
        },
    )


# ── Supply Chain Map ───────────────────────────────────────────────────────────

async def run_supply_chain_map(req: SupplyChainMapRequest) -> SupplyChainMapResponse:
    """
    Build a structured multi-layer supply chain map for a giant or theme anchor.
    Returns layered nodes with country/exchange tags, evidence, and ADR proxies.
    """
    anchor_type: str
    anchor: str
    layers: List[ChainLayer]

    if req.anchor and req.anchor.upper() in {k.upper() for k in GIANT_MAP}:
        anchor      = req.anchor
        anchor_type = "giant"
        layers      = get_chain_for_giant(
            req.anchor,
            max_depth=req.max_depth,
            themes_filter=([req.theme_id] if req.theme_id else None),
            include_foreign=req.include_foreign,
            country_filters=req.country_filters,
        )
    elif req.theme_id:
        anchor      = req.theme_id
        anchor_type = "theme"
        layers      = get_chain_for_theme(
            req.theme_id,
            max_depth=req.max_depth,
            include_foreign=req.include_foreign,
            country_filters=req.country_filters,
        )
    elif req.anchor:
        anchor      = req.anchor
        anchor_type = "theme"
        layers      = get_chain_for_theme(
            req.anchor,
            max_depth=req.max_depth,
            include_foreign=req.include_foreign,
            country_filters=req.country_filters,
        )
    else:
        return SupplyChainMapResponse(
            anchor="unknown",
            anchor_type="unknown",
            layers=[],
            meta={"error": "No anchor or theme_id provided"},
        )

    companies_by_layer: Dict[str, List[str]] = {}
    all_countries: Set[str] = set()
    adr_etf_proxies: Dict[str, str] = {}

    for cl in layers:
        label = f"layer_{cl.layer_index}"
        companies_by_layer[label] = [n.ticker or n.company_name for n in cl.nodes]
        for node in cl.nodes:
            if node.country:
                all_countries.add(node.country)
            if node.us_access_proxy and node.country != "US":
                adr_etf_proxies[node.ticker or node.company_name] = node.us_access_proxy

    if anchor_type == "theme":
        etfs = get_etf_proxies_for_theme(anchor)
        for etf in etfs[:2]:
            adr_etf_proxies[f"ETF_{anchor}"] = etf

    evidence_sources = ["Curated NODE_REGISTRY (primary)", "GIANT_MAP theme associations"]
    if req.include_foreign:
        evidence_sources.append("FOREIGN_ACCESS_MAP for ADR/proxy guidance")

    return SupplyChainMapResponse(
        anchor=anchor,
        anchor_type=anchor_type,
        theme=req.theme_id,
        layers=layers,
        companies_by_layer=companies_by_layer,
        country_tags=sorted(all_countries),
        confidence="high",
        evidence_sources=evidence_sources,
        adr_etf_proxies=adr_etf_proxies,
        meta={
            "max_depth":       req.max_depth,
            "include_foreign": req.include_foreign,
            "country_filters": req.country_filters,
            "total_nodes":     sum(len(cl.nodes) for cl in layers),
        },
    )
