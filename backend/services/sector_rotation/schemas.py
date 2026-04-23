"""
Pydantic schemas for the Sector Rotation / Sectors dashboard.
"""
from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field


SECTOR_ETF_MAP: dict[str, str] = {
    "XLC":  "Communication Services",
    "XLY":  "Consumer Discretionary",
    "XLP":  "Consumer Staples",
    "XLE":  "Energy",
    "XLF":  "Financials",
    "XLV":  "Health Care",
    "XLI":  "Industrials",
    "XLB":  "Materials",
    "XLRE": "Real Estate",
    "XLK":  "Technology",
    "XLU":  "Utilities",
}

CYCLICALS:  list[str] = ["XLY", "XLF", "XLI", "XLB", "XLE", "XLK", "XLC"]
DEFENSIVES: list[str] = ["XLP", "XLU", "XLV", "XLRE"]


class ETFSeries(BaseModel):
    dates: list[str] = Field(default_factory=list)
    prices: list[float] = Field(default_factory=list)


class SectorSnapshot(BaseModel):
    ticker: str
    name: str
    price: Optional[float] = None
    change_1d: Optional[float] = None
    change_7d: Optional[float] = None
    change_30d: Optional[float] = None
    change_ytd: Optional[float] = None
    change_1y: Optional[float] = None
    ma_50d: Optional[float] = None
    ma_200d: Optional[float] = None
    pct_from_50d: Optional[float] = None
    pct_from_200d: Optional[float] = None
    rotation_score: Optional[float] = None
    relative_strength_rank: Optional[int] = None
    regime_tag: Optional[str] = None
    is_cyclical: bool = False
    series: dict[str, ETFSeries] = Field(default_factory=dict)


class RegimeSummary(BaseModel):
    market_posture: str = "Neutral"
    cyclical_vs_defensive: Optional[float] = None
    breadth_pct_above_spy: Optional[float] = None
    leadership_style: str = "Mixed"
    macro_overlay: dict[str, Any] = Field(default_factory=dict)


class AIScenario(BaseModel):
    name: str
    timeframe: str
    probability: str
    sector_winners: list[str] = Field(default_factory=list)
    sector_losers: list[str] = Field(default_factory=list)
    analysis: str


class AILeadership(BaseModel):
    leaders: list[str] = Field(default_factory=list)
    laggards: list[str] = Field(default_factory=list)
    explanation: str = ""


class AISource(BaseModel):
    title: str = ""
    url: str = ""
    publisher: str = ""


class AIAnalysis(BaseModel):
    summary: str = ""
    market_regime: str = ""
    macro_regime: str = ""
    leadership_style: str = ""
    current_leadership: AILeadership = Field(default_factory=AILeadership)
    outlook_1_4_weeks: str = ""
    outlook_1_3_months: str = ""
    scenarios: list[AIScenario] = Field(default_factory=list)
    watch_items: list[str] = Field(default_factory=list)
    # top_stocks_to_watch: list of structured stock dicts (SectorStock-shaped)
    # or legacy strings — always injected at serve time from curated sector data
    top_stocks_to_watch: List[Union[Dict[str, Any], str]] = Field(default_factory=list)
    sources: list[AISource] = Field(default_factory=list)
    generated_at: str = ""
    # winning_sector_context: which sector(s) drove this analysis
    winning_sector_etfs: list[str] = Field(default_factory=list)


class SectorRotationDashboard(BaseModel):
    updated_at: str
    analysis_updated_at: Optional[str] = None
    regime: RegimeSummary
    leaders: list[SectorSnapshot] = Field(default_factory=list)
    laggards: list[SectorSnapshot] = Field(default_factory=list)
    sectors: list[SectorSnapshot] = Field(default_factory=list)
    analysis: Optional[AIAnalysis] = None


# ── Sectors page — new models ────────────────────────────────────────────────

class SectorStock(BaseModel):
    """One stock entry in the Sectors page stock scan."""
    ticker: str
    company_name: str
    sector_etf: str                       # e.g. "XLK"
    sector_name: str                      # e.g. "Technology"
    role: str                             # "momentum" | "bottleneck" | "anchor"
    reason_for_inclusion: str = ""
    price: Optional[float] = None
    change_1d_pct: Optional[float] = None
    market_cap_label: str = ""            # "Mega" | "Large" | "Mid" | "Small"
    tv_symbol: str = ""                   # TradingView symbol, e.g. "NASDAQ:NVDA"


class SectorStockGroup(BaseModel):
    """Three role-groups of stocks for one sector ETF."""
    etf: str                              # "XLK"
    sector_name: str                      # "Technology"
    momentum_leaders: List[SectorStock] = Field(default_factory=list)
    bottleneck_enablers: List[SectorStock] = Field(default_factory=list)
    anchor_giants: List[SectorStock] = Field(default_factory=list)


class WinningSector(BaseModel):
    """One sector in the ranked winning-sectors output."""
    etf: str
    sector_name: str
    rotation_score: Optional[float] = None
    relative_strength_rank: int = 0
    regime_tag: str = ""
    change_1d: Optional[float] = None
    change_7d: Optional[float] = None
    change_30d: Optional[float] = None
    change_ytd: Optional[float] = None
    is_top: bool = False                  # True for the single strongest sector


class SectorsPageData(BaseModel):
    """
    Full payload for the Sectors page.
    Backward-compatible: existing SectorRotationDashboard fields all present.
    New fields are additive.
    """
    page_title: str = "Sectors"
    updated_at: str
    analysis_updated_at: Optional[str] = None
    regime: RegimeSummary
    # All 11 sector snapshots, sorted by rotation_score desc
    sectors: list[SectorSnapshot] = Field(default_factory=list)
    leaders: list[SectorSnapshot] = Field(default_factory=list)
    laggards: list[SectorSnapshot] = Field(default_factory=list)
    # Winning-sector detection output
    winning_sectors: list[WinningSector] = Field(default_factory=list)
    top_sector_etf: Optional[str] = None
    # Stock scan for top sector(s) — grouped by role
    sector_stocks: list[SectorStockGroup] = Field(default_factory=list)
    # Flat ranked stock list for the winning sector(s) — always computed from
    # live signals, independent of agent analysis. Momentum leaders sorted by
    # 1-day change descending; bottleneck/anchor keep curated structural order.
    top_stocks_in_winning_sectors: list[SectorStock] = Field(default_factory=list)
    # Persisted AI analysis — returned regardless of age; None if never generated
    saved_analysis: Optional[AIAnalysis] = None
