"""
Pydantic schemas for the Sector Rotation dashboard.
"""
from __future__ import annotations
from datetime import datetime
from typing import Any, Optional
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
    sources: list[AISource] = Field(default_factory=list)
    generated_at: str = ""


class SectorRotationDashboard(BaseModel):
    updated_at: str
    analysis_updated_at: Optional[str] = None
    regime: RegimeSummary
    leaders: list[SectorSnapshot] = Field(default_factory=list)
    laggards: list[SectorSnapshot] = Field(default_factory=list)
    sectors: list[SectorSnapshot] = Field(default_factory=list)
    analysis: Optional[AIAnalysis] = None
