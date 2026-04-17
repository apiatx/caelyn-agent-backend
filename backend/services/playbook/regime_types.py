"""
Serenity Regime Types — Pydantic models for regime detection output.

These models describe the result of compute_serenity_regime() in regime_service.py.
They are used by:
  - GET /api/playbooks/serenity-regime
  - DiscoverResponse.regime_context (when mode="auto")
  - AnalyzeResponse.regime_context (when Serenity auto bridge runs)
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field


class ThemeRegimeScore(BaseModel):
    """Per-theme scoring breakdown used in regime detection."""
    model_config = ConfigDict(extra="ignore")

    theme_id:             str
    label:                str
    regime_score:         float   # 0-100 composite — basis for ranking
    candidate_density:    int     # # nodes with this theme in NODE_REGISTRY
    avg_bottleneck_score: float   # average node bottleneck_score
    hiddenness_quality:   float   # 0-100, derived from avg layer depth (deeper = more hidden)
    policy_score:         float   # 0-100, derived from policy_linkage keyword parsing
    anchor_density:       int     # # giant anchors referencing this theme
    country_diversity:    int     # # distinct countries represented in nodes
    serenity_priority:    str     # "high" | "medium" | "low" from taxonomy
    crowding_penalty:     float   # 0-20 subtracted for shallow/US-concentrated themes


class AnchorRegimeScore(BaseModel):
    """Per-giant-anchor scoring breakdown used in regime detection."""
    model_config = ConfigDict(extra="ignore")

    anchor_id:               str
    name:                    str
    regime_score:            float       # 0-100 composite
    theme_overlap_count:     int         # # of top-regime themes this anchor covers
    overlapping_theme_ids:   List[str]   # actual top-regime theme IDs this anchor covers
    capex_scale_score:       float       # 0-100 derived from capex description string
    candidate_quality:       float       # avg bottleneck_score of nodes listing this anchor
    foreign_exposure_count:  int         # # foreign countries in anchor's foreign_exposure list


class SerenityRegime(BaseModel):
    """
    The current Serenity regime detection result.

    A regime describes which theme cluster and anchor platform the discovery
    engine should prioritize when no explicit anchor/theme is provided by the user.

    This is deterministic — same inputs produce same output. No web calls required.
    """
    model_config = ConfigDict(extra="ignore")

    regime_id:                        str
    label:                            str
    summary:                          str

    top_themes:                       List[str]
    top_anchors:                      List[str]
    top_regions:                      List[str]

    recommended_mode:                 str     # always "theme_scan" for regime-driven auto
    recommended_depth:                int     # 1–4
    confidence:                       str     # "high" | "medium" | "low"

    why_now:                          List[str]
    evidence_signals:                 List[str]
    rejected_or_lower_priority_paths: List[str]

    theme_scores:                     List[ThemeRegimeScore] = Field(default_factory=list)
    anchor_scores:                    List[AnchorRegimeScore] = Field(default_factory=list)

    auto_mode_used:                   bool = True
    computed_at:                      str  = ""
