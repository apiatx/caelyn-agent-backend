"""
Playbook type definitions — Pydantic models shared across the engine.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict


# ── Playbook Definition ───────────────────────────────────────────────────────

class HardFilter(BaseModel):
    model_config = ConfigDict(extra="ignore")
    field: str
    op: str             # "gte" | "lte" | "gt" | "lt" | "eq" | "neq"
    value: Any
    label: str          # human-readable failure reason


class PenaltyRule(BaseModel):
    model_config = ConfigDict(extra="ignore")
    factor: str
    threshold: float    # if factor_score > threshold → apply deduction
    deduction: float    # points subtracted from final score (positive number)
    label: str


class PlaybookDefinition(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str
    name: str
    short_label: str
    description: str
    enabled: bool
    version: str
    factor_weights: Dict[str, float]        # factor_name -> weight, must sum ≤ 1.0
    hard_filters: List[HardFilter]
    penalty_rules: List[PenaltyRule]
    preferred_themes: List[str]
    preferred_sectors: List[str]
    entry_style: str
    exit_style: str
    positioning_style: str
    ui_color: str                           # hex color for frontend badge
    explanation_template_key: str


# ── Raw ticker data fetched for scoring ──────────────────────────────────────

class TickerRawData(BaseModel):
    """Data fetched per-ticker from FMP (or stubs when unavailable)."""
    model_config = ConfigDict(extra="ignore")
    ticker: str
    price: Optional[float] = None
    mkt_cap: Optional[float] = None         # USD
    sector: Optional[str] = None
    industry: Optional[str] = None
    pe_ratio: Optional[float] = None
    debt_to_equity: Optional[float] = None  # D/E ratio
    revenue_growth_yoy: Optional[float] = None  # decimal e.g. 0.22 = 22%
    week52_high: Optional[float] = None
    week52_low: Optional[float] = None
    day_change_pct: Optional[float] = None
    fetch_error: Optional[str] = None       # non-None = partial/failed fetch


# ── Scoring Result ────────────────────────────────────────────────────────────

class PlaybookScoreResult(BaseModel):
    model_config = ConfigDict(extra="ignore")
    ticker: str
    playbook_id: str
    final_score: float                      # 0–100
    hard_filter_pass: bool
    hard_filter_failures: List[str]         # human-readable reasons
    summary_label: str
    factor_scores: Dict[str, float]         # factor_name -> 0–100
    penalties_applied: Dict[str, float]     # factor_name -> deduction applied
    matched_rules: List[str]                # rules with strong signal
    risks: List[str]                        # risk notes
    stub_factors: List[str]                 # factors not yet computed from real data
    raw_data: Dict[str, Any]                # subset of fetched data for transparency


# ── Service request/response schemas ─────────────────────────────────────────

class ScoreWatchlistRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    playbook_id: str
    tickers: List[str]
    include_breakdown: bool = True


class ScorePortfolioRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    playbook_id: str
    holdings: List[Dict[str, Any]]          # [{ticker, weight, ...}]
    include_breakdown: bool = True


class PortfolioScoreResult(BaseModel):
    model_config = ConfigDict(extra="ignore")
    playbook_id: str
    aggregate_score: float
    holdings: List[PlaybookScoreResult]
    strongest_aligned: List[str]
    weakest_aligned: List[str]
    concentration_notes: List[str]
