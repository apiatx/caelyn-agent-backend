"""
Discovery Types — Pydantic models for the Serenity discovery + supply-chain intelligence layer.

All models are isolated from /api/query and default terminal behavior.
Only used by /api/playbooks/discover, /api/playbooks/supply-chain-map,
/api/playbooks/compare, and the optional discovery bridge in /api/playbooks/analyze.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field


# ── Discovery Scoring Dimensions (8 factors) ──────────────────────────────────

class DiscoveryScores(BaseModel):
    model_config = ConfigDict(extra="ignore")
    chain_depth_score:            float = 50.0   # deeper in supply chain = higher (layer 3-4 vs layer 1)
    bottleneck_criticality_score: float = 50.0   # how constrained / hard to substitute
    hiddenness_score:             float = 50.0   # not widely known / under-covered
    giant_dependency_score:       float = 50.0   # directly tied to a major platform giant
    foreign_uniqueness_score:     float = 20.0   # unique foreign exposure not easily accessed (US=20)
    supply_chain_confidence_score: float = 50.0  # quality of evidence behind the chain position
    proxy_accessibility_score:    float = 100.0  # how easily tradable: US=100, ADR=72, ETF=55, foreign=30
    theme_purity_score:           float = 50.0   # pure-play vs conglomerate (1 theme = 95)


# ── Per-candidate result ───────────────────────────────────────────────────────

class DiscoveryCandidate(BaseModel):
    model_config = ConfigDict(extra="ignore")
    ticker:         str
    company_name:   str
    country:        str = "US"
    exchange:       str = "NASDAQ"
    themes:         List[str] = Field(default_factory=list)
    chain_layers:   List[str] = Field(default_factory=list)   # named chain roles
    layer_depth:    int = 2    # 0=giant, 1=systems, 2=components, 3=constrained, 4=upstream
    scores:         DiscoveryScores = Field(default_factory=DiscoveryScores)

    # Flat score accessors (frontend-friendly)
    chain_depth_score:             float = 50.0
    bottleneck_criticality_score:  float = 50.0
    hiddenness_score:              float = 50.0
    supply_chain_confidence_score: float = 50.0

    # Phase 4 — best-blend composite (replaces raw rank as primary sort key)
    best_blend_score:  float = 0.0

    # Phase 4 — visibility and role classification
    visibility_bucket: str = "known"
    # household | widely_covered | known | specialist | hidden
    chain_role_type:   str = "adjacent_supplier"
    # platform_anchor | direct_bottleneck | adjacent_supplier | indirect_beneficiary

    # Phase 4 — confidence transparency
    confidence_penalties: List[str] = Field(default_factory=list)
    data_gaps:            List[str] = Field(default_factory=list)

    # Phase 4 — deterministic explanations (no LLM dependency)
    why_now:              str = ""
    why_hidden:           str = ""
    what_to_verify_next:  str = ""

    # Phase 4 — peer comparison
    comparable_names: List[str] = Field(default_factory=list)

    thesis_summary:  str = ""
    fit_reasoning:   List[str] = Field(default_factory=list)
    giant_anchors:   List[str] = Field(default_factory=list)   # which giants this supplies

    us_access_proxy:  Optional[str] = None    # ADR ticker or ETF proxy symbol
    adr_ticker:       Optional[str] = None
    coverage_status:  str = "full"            # "full" | "partial" | "thin"
    data_confidence:  str = "high"            # "high" | "medium" | "low"
    direct_tradable:  bool = True

    market_cap_usd:  Optional[float] = None
    price:           Optional[float] = None
    hiddenness_reason: str = ""
    enriched:        bool = False             # True if live provider enrichment ran

    # Optional — compare mode only
    consensus_fit:   Optional[str] = None     # serenity_only | sj_only | consensus | low_fit_both


# ── Discover Request ───────────────────────────────────────────────────────────

class DiscoverRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    playbook_id:              str = "serenity"
    query:                    str = ""
    mode:                     str = "giant_chain"
    # giant_chain | theme_scan | foreign_bottlenecks | ticker_chain | country_theme_scan | custom
    giant:                    Optional[str] = None      # e.g. "NVDA"
    ticker:                   Optional[str] = None      # for ticker_chain mode
    theme_ids:                List[str] = Field(default_factory=list)
    country_filters:          List[str] = Field(default_factory=list)  # ["US", "JP", "KR"]
    include_foreign:          bool = False
    max_depth:                int = 3
    limit:                    int = 20
    only_hidden:              bool = False    # only return high hiddenness_score (>=65)
    only_public:              bool = True
    include_adr_or_etf_proxies: bool = True
    use_web_validation:       bool = False    # Perplexity shortlisted validation (rate-limited)


# ── Discover Response ──────────────────────────────────────────────────────────

class DiscoverResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")
    playbook_id:              str
    mode:                     str
    query:                    str
    summary:                  str
    top_candidates:           List[DiscoveryCandidate]
    low_confidence_candidates: List[DiscoveryCandidate]
    chain_map_preview:        Dict[str, Any] = Field(default_factory=dict)

    # Phase 4 — ranked surfacing buckets (all optional for backward compat)
    top_hidden_bottlenecks:         List[DiscoveryCandidate] = Field(default_factory=list)
    top_direct_chokepoints:         List[DiscoveryCandidate] = Field(default_factory=list)
    top_foreign_specialists:        List[DiscoveryCandidate] = Field(default_factory=list)
    top_us_accessible_foreign_proxies: List[DiscoveryCandidate] = Field(default_factory=list)
    highest_confidence_candidates:  List[DiscoveryCandidate] = Field(default_factory=list)
    best_blend_candidates:          List[DiscoveryCandidate] = Field(default_factory=list)

    meta:                     Dict[str, Any] = Field(default_factory=dict)


# ── Supply Chain Map Types ─────────────────────────────────────────────────────

class ChainNode(BaseModel):
    model_config = ConfigDict(extra="ignore")
    ticker:          Optional[str] = None
    company_name:    str
    country:         str = "US"
    exchange:        Optional[str] = None
    layer:           int = 2
    layer_label:     str = ""
    themes:          List[str] = Field(default_factory=list)
    role:            str = ""
    bottleneck_score: float = 50.0
    confidence:      str = "high"
    evidence:        List[str] = Field(default_factory=list)
    us_access_proxy: Optional[str] = None
    adr_ticker:      Optional[str] = None


class ChainLayer(BaseModel):
    model_config = ConfigDict(extra="ignore")
    layer_index: int
    label:       str
    description: str
    nodes:       List[ChainNode] = Field(default_factory=list)


class SupplyChainMapRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    anchor:         Optional[str] = None        # giant ticker (e.g. "NVDA") or theme_id
    theme_id:       Optional[str] = None
    max_depth:      int = 4
    country_filters: List[str] = Field(default_factory=list)
    include_foreign: bool = False


class SupplyChainMapResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")
    anchor:            str
    anchor_type:       str         # "giant" | "theme"
    theme:             Optional[str] = None
    layers:            List[ChainLayer]
    companies_by_layer: Dict[str, List[str]] = Field(default_factory=dict)
    country_tags:      List[str] = Field(default_factory=list)
    confidence:        str = "high"
    evidence_sources:  List[str] = Field(default_factory=list)
    adr_etf_proxies:   Dict[str, str] = Field(default_factory=dict)
    meta:              Dict[str, Any] = Field(default_factory=dict)


# ── Compare Request / Response (Phase 4) ──────────────────────────────────────

class CompareRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    tickers:           List[str]
    playbooks:         List[str] = Field(default=["serenity", "sjcapital"])
    include_breakdown: bool = True


class CompareTickerResult(BaseModel):
    model_config = ConfigDict(extra="ignore")
    ticker:               str
    serenity_score:       Optional[float] = None   # discovery composite (0-100)
    sj_score:             Optional[float] = None   # S&J Capital scoring (0-100)
    score_delta:          Optional[float] = None   # serenity - sj (positive = serenity favors more)
    classification:       str = "low_fit_both"
    # serenity_only | sj_only | consensus | low_fit_both
    serenity_pass:        bool = False
    sj_pass:              bool = False
    explanation:          str = ""
    serenity_breakdown:   Dict[str, Any] = Field(default_factory=dict)
    sj_breakdown:         Dict[str, Any] = Field(default_factory=dict)
    in_node_registry:     bool = False    # has a Serenity curated profile


class CompareResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")
    tickers_compared:    List[str]
    playbooks:           List[str]
    results:             List[CompareTickerResult]
    consensus_names:     List[str] = Field(default_factory=list)
    serenity_only_names: List[str] = Field(default_factory=list)
    sj_only_names:       List[str] = Field(default_factory=list)
    low_fit_both:        List[str] = Field(default_factory=list)
    meta:                Dict[str, Any] = Field(default_factory=dict)
