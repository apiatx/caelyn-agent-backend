"""
Strategy Screener Pydantic models.

ScreenerCandidate  — one row in the list page
ScreenerSnapshot   — one full screener issue / run
ScreenerReport     — one deep-dive report per candidate
ScreenerConfig     — cadence, grade scale, and frontend dropdown metadata
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class ScreenerCandidate(BaseModel):
    """Compact candidate representation for the list page."""
    model_config = ConfigDict(extra="ignore")

    ticker:                       str
    company_name:                 str
    country:                      str = "US"
    exchange:                     str = "NASDAQ"
    market_cap_usd:               Optional[float] = None
    theme:                        str = ""
    themes:                       List[str] = Field(default_factory=list)
    chain_role_type:              str = "adjacent_supplier"
    layer_depth:                  int = 2

    grade:                        str = "B"
    best_blend_score:             float = 0.0
    bottleneck_criticality_score: float = 50.0
    hiddenness_score:             float = 50.0
    chain_depth_score:            float = 50.0
    supply_chain_confidence_score: float = 50.0

    coverage_status:              str = "full"
    data_confidence:              str = "high"
    us_access_proxy:              Optional[str] = None
    one_line_summary:             str = ""
    giant_anchors:                List[str] = Field(default_factory=list)
    comparable_names:             List[str] = Field(default_factory=list)


class ScreenerSnapshot(BaseModel):
    """
    One complete screener issue/run.
    The list-page payload. Reports are stored separately and fetched on demand.
    """
    model_config = ConfigDict(extra="ignore")

    snapshot_id:       str
    playbook_id:       str  = "serenity"
    generated_at:      str                   # ISO-8601 UTC
    cadence:           str  = "biweekly"
    cadence_days:      int  = 14
    regime_context:    Optional[Dict[str, Any]] = None
    summary:           str  = ""
    results:           List[ScreenerCandidate] = Field(default_factory=list)
    results_count:     int  = 0
    status:            str  = "complete"     # generating | complete | error
    version:           str  = "1.0"
    generation_notes:  str  = ""
    is_stale:          bool = False          # set by scheduler at read time

    # Filter/sort metadata — only present when query params were applied
    active_filters:        Optional[Dict[str, Any]] = None
    active_sort:           Optional[str] = None
    filtered_result_count: Optional[int] = None
    available_result_count: Optional[int] = None


class ScreenerReport(BaseModel):
    """
    Full deep-dive report for one candidate in a snapshot.
    Stored separately; fetched by GET /report/{snapshot_id}/{ticker}.
    """
    model_config = ConfigDict(extra="ignore")

    snapshot_id:           str
    ticker:                str
    company_name:          str
    headline:              str
    meta_line:             str

    summary:               str
    why_it_matters:        str
    supply_chain_map_text: str
    supply_chain_layers:   List[str] = Field(default_factory=list)
    competitors:           str
    catalysts:             str
    rerating_case:         str
    key_risk:              str
    why_hidden:            str
    what_to_verify_next:   str
    what_would_break_thesis: str

    themes:                List[str] = Field(default_factory=list)
    anchors:               List[str] = Field(default_factory=list)
    coverage_status:       str = "full"
    data_confidence:       str = "high"
    us_access_proxy:       Optional[str] = None
    market_cap_usd:        Optional[float] = None
    country:               str = "US"
    exchange:              str = "NASDAQ"

    grade:                 str = "B"
    scores:                Dict[str, float] = Field(default_factory=dict)
    generated_at:          str
    regime_context:        Optional[Dict[str, Any]] = None


class ScreenerConfig(BaseModel):
    """Read-only config returned by GET /api/strategy-screener/config."""
    model_config = ConfigDict(extra="ignore")

    playbook_id:      str = "serenity"
    cadence:          str = "biweekly"
    cadence_days:     int = 14
    shortlist_size:   int = 20
    version:          str = "1.0"

    grade_scale: Dict[str, str] = Field(default_factory=lambda: {
        "A+": "best_blend >= 82 (confidence-adjusted)",
        "A":  "best_blend >= 72",
        "B+": "best_blend >= 60",
        "B":  "best_blend >= 48",
        "C":  "best_blend < 48",
    })

    # Frontend dropdown metadata
    market_cap_buckets: List[Dict[str, Any]] = Field(default_factory=lambda: [
        {"id": "large_cap",  "label": "Large Cap ($100B+)"},
        {"id": "mid_cap",    "label": "Mid Cap ($20B\u2013$99B)"},
        {"id": "small_cap",  "label": "Small Cap ($2.5B\u2013$19B)"},
        {"id": "micro_cap",  "label": "Micro Cap (<$2.5B)"},
        {"id": "unknown",    "label": "Unknown Market Cap"},
    ])

    layer_filters: List[Dict[str, Any]] = Field(default_factory=lambda: [
        {"id": 1, "label": "Layer 1 \u2014 Systems Integrator"},
        {"id": 2, "label": "Layer 2 \u2014 Key Component"},
        {"id": 3, "label": "Layer 3 \u2014 Constrained Bottleneck"},
    ])

    sort_options: List[Dict[str, Any]] = Field(default_factory=lambda: [
        {"id": "best_fit",   "label": "Best Fit"},
        {"id": "market_cap", "label": "Market Cap"},
        {"id": "layer",      "label": "Layer"},
        {"id": "grade",      "label": "Grade"},
    ])

    description: str = (
        "Serenity Strategy Screener \u2014 proactive supply chain bottleneck publication. "
        "Generated from the same Serenity regime/discovery engine as the terminal. "
        "Stored and browsable; refreshed on cadence or manually."
    )
