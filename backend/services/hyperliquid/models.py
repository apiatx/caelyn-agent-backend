"""
Hyperliquid Screener — Pydantic models and response schemas.

ScreenerAsset is the canonical normalized row representing one perpetual
or spot market in the screener universe. All derived signals are included.
"""
from __future__ import annotations

import time
from typing import Any, Optional

from pydantic import BaseModel, Field


# ─────────────────────────────────────────────────────────────────────────────
# Core asset row
# ─────────────────────────────────────────────────────────────────────────────

class ScreenerAsset(BaseModel):
    # ── Identity ──────────────────────────────────────────────────────────
    coin: str
    display_name: str
    market_type: str = "perp"           # "perp" | "spot"
    dex: str = "hyperliquid"
    tags: list[str] = Field(default_factory=list)

    # ── Price surface ─────────────────────────────────────────────────────
    mark_px: Optional[float] = None
    mid_px: Optional[float] = None
    oracle_px: Optional[float] = None
    bid_px: Optional[float] = None
    ask_px: Optional[float] = None
    spread_abs: Optional[float] = None
    spread_bps: Optional[float] = None
    prev_day_px: Optional[float] = None
    pct_change_24h: Optional[float] = None

    # ── Perp-specific ─────────────────────────────────────────────────────
    # funding is hourly rate; annualized = funding * 8760
    funding: Optional[float] = None
    predicted_funding: Optional[float] = None
    premium: Optional[float] = None     # (mark - oracle) / oracle

    # ── Market size ───────────────────────────────────────────────────────
    open_interest: Optional[float] = None       # base asset
    open_interest_usd: Optional[float] = None
    open_interest_change_pct: Optional[float] = None   # 1h OI % change
    oi_change_5m: Optional[float] = None               # 5-min OI % change (decimal)
    oi_change_15m: Optional[float] = None              # 15-min OI % change (decimal)
    oi_change_1h: Optional[float] = None               # 1-hour OI % change (decimal)
    day_ntl_vlm: Optional[float] = None         # 24h notional USD
    day_base_vlm: Optional[float] = None        # 24h base volume
    volume_impulse: Optional[float] = None      # recent 1h vol vs rolling avg
    volume_impulse_5m: Optional[float] = None   # recent 5m vol vs rolling avg
    volume_impulse_15m: Optional[float] = None  # 15m window vol impulse
    score_change: Optional[float] = None        # composite score delta vs prior cycle

    # ── Microstructure / book ─────────────────────────────────────────────
    impact_bid_px: Optional[float] = None       # fill price for $5k notional short
    impact_ask_px: Optional[float] = None       # fill price for $5k notional long
    orderbook_bid_depth: Optional[float] = None # $ notional in top-10 bid levels
    orderbook_ask_depth: Optional[float] = None
    orderbook_imbalance: Optional[float] = None # -1..+1 (+1 = all bids)

    # ── Recent trade flow (rolling ~5 min window) ─────────────────────────
    recent_trade_count: int = 0
    recent_trade_buy_volume: Optional[float] = None
    recent_trade_sell_volume: Optional[float] = None
    recent_trade_imbalance: Optional[float] = None  # -1..+1 (+1 = all buys)

    # ── Derived volatility ────────────────────────────────────────────────
    realized_volatility_short: Optional[float] = None   # ~1h annualized %
    realized_volatility_medium: Optional[float] = None  # ~24h annualized %

    # ── Momentum (% change over interval) ────────────────────────────────
    momentum_5m: Optional[float] = None
    momentum_1h: Optional[float] = None
    momentum_4h: Optional[float] = None
    momentum_24h: Optional[float] = None

    # ── Dislocation ratios ────────────────────────────────────────────────
    distance_mark_oracle_pct: Optional[float] = None
    distance_mark_mid_pct: Optional[float] = None
    distance_mark_prev_day_pct: Optional[float] = None

    # ── Component scores (0..100) ─────────────────────────────────────────
    # Base components
    liquidity_score: Optional[float] = None
    volatility_score: Optional[float] = None
    momentum_score: Optional[float] = None
    flow_score: Optional[float] = None
    trend_score: Optional[float] = None          # multi-TF alignment
    book_pressure_score: Optional[float] = None  # bid vs ask depth skew
    crowding_score: Optional[float] = None       # extreme funding + OI concentration
    dislocation_score: Optional[float] = None    # mark vs oracle/mid deviation
    tradability_penalty: Optional[float] = None  # wide spread/thin book penalty

    # Setup-specific scores
    mean_reversion_score: Optional[float] = None
    breakout_score: Optional[float] = None
    trend_continuation_score: Optional[float] = None
    crowding_unwind_score: Optional[float] = None
    avoid_score: Optional[float] = None

    # Overall derived fields
    composite_signal_score: Optional[float] = None
    overall_score: Optional[float] = None        # best non-avoid setup score
    setup_type: Optional[str] = None             # breakout|mean_reversion|trend_continuation|crowding_unwind|avoid

    # ── Signal summary ────────────────────────────────────────────────────
    signal_direction: Optional[str] = None      # "long" | "short" | "neutral"
    signal_confidence: Optional[float] = None   # 0..1

    # ── Universe percentile ranks ─────────────────────────────────────────
    volume_percentile: Optional[float] = None   # 0..1
    oi_percentile: Optional[float] = None
    funding_percentile: Optional[float] = None  # 0..1, 1 = most extreme
    volatility_percentile: Optional[float] = None

    # ── Qualitative flags ─────────────────────────────────────────────────
    crowded_long: bool = False              # high funding + high OI + positive momentum
    crowded_short: bool = False             # very negative funding + high OI
    squeeze_candidate: bool = False         # crowded short + price compression
    trend_continuation_candidate: bool = False
    mean_reversion_candidate: bool = False
    illiquid_high_volatility: bool = False
    avoid_due_to_spread: bool = False
    dislocated_vs_oracle: bool = False

    # ── Contract metadata ─────────────────────────────────────────────────
    market_status: str = "active"
    max_leverage: Optional[int] = None
    only_isolated: bool = False
    margin_table_id: Optional[int] = None
    sz_decimals: int = 0
    growth_mode: bool = False
    open_interest_cap_flag: bool = False

    # ── Agent / ranking ───────────────────────────────────────────────────
    agent_rationale: Optional[str] = None
    score_components: dict[str, float] = Field(default_factory=dict)
    rank: Optional[int] = None
    prev_rank: Optional[int] = None
    rank_change: Optional[int] = None

    last_updated_ts: float = Field(default_factory=time.time)


# ─────────────────────────────────────────────────────────────────────────────
# Snapshot / response envelopes
# ─────────────────────────────────────────────────────────────────────────────

class ScreenerSnapshot(BaseModel):
    rows: list[ScreenerAsset]
    total_assets: int
    perp_count: int
    spot_count: int
    data_freshness_seconds: Optional[float] = None
    ws_connected: bool = False
    server_ts: float = Field(default_factory=time.time)
    schema_version: str = "1.0"
    available_filters: dict[str, Any] = Field(default_factory=dict)
    summary_stats: dict[str, Any] = Field(default_factory=dict)


class AssetDetail(BaseModel):
    asset: ScreenerAsset
    candle_1h: list[dict] = Field(default_factory=list)
    candle_5m: list[dict] = Field(default_factory=list)
    recent_trades: list[dict] = Field(default_factory=list)
    l2_levels: dict = Field(default_factory=dict)
    score_history: list[dict] = Field(default_factory=list)
    server_ts: float = Field(default_factory=time.time)


class AgentRankRequest(BaseModel):
    ranking_mode: str = "balanced"  # balanced|momentum|breakout|mean_reversion|crowding_dislocation
    top_n: int = 20
    include_rationales: bool = True
    include_score_components: bool = True
    filters: dict[str, Any] = Field(default_factory=dict)


class AgentRankResponse(BaseModel):
    request_ts: float = Field(default_factory=time.time)
    ranking_mode: str
    total_ranked: int
    ranked_rows: list[ScreenerAsset]
    top_long: list[ScreenerAsset]
    top_short: list[ScreenerAsset]
    top_dislocations: list[ScreenerAsset]
    avoid_list: list[ScreenerAsset]
    summary: dict[str, Any] = Field(default_factory=dict)
    score_version: str = "1.0"
    schema_version: str = "1.0"


# ─────────────────────────────────────────────────────────────────────────────
# Hero signal — a fully-formed trade idea
# ─────────────────────────────────────────────────────────────────────────────

class HeroSignalMetrics(BaseModel):
    mark_px: Optional[float] = None
    pct_change_24h: Optional[float] = None
    funding: Optional[float] = None
    funding_ann_pct: Optional[float] = None        # annualized %
    open_interest: Optional[float] = None
    day_ntl_vlm: Optional[float] = None
    premium: Optional[float] = None
    mark_oracle_gap_pct: Optional[float] = None    # % (positive = mark > oracle)
    mark_mid_gap_pct: Optional[float] = None
    trade_flow_bias: Optional[float] = None        # -1..+1
    book_imbalance: Optional[float] = None         # -1..+1
    volatility_score: Optional[float] = None
    liquidity_score: Optional[float] = None
    oi_change_5m: Optional[float] = None
    oi_change_15m: Optional[float] = None
    oi_change_1h: Optional[float] = None
    volume_impulse: Optional[float] = None


class HeroSignal(BaseModel):
    coin: str
    side: str                    # "long" | "short" | "neutral_watch"
    overall_score: float         # 0-100
    setup_type: str              # "breakout" | "mean_reversion" | "trend_continuation" | "crowding_unwind" | "avoid"
    confidence: float            # 0-1
    thesis_title: str
    thesis_summary: str
    reasons: list[str]
    risk_flags: list[str]
    invalidation_notes: list[str]
    metrics: HeroSignalMetrics
    score_components: dict[str, float] = Field(default_factory=dict)
    generated_at: float = Field(default_factory=time.time)


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket event types pushed to frontend clients
# ─────────────────────────────────────────────────────────────────────────────

class WsEvent(BaseModel):
    event: str          # snapshot_ready|asset_update|rank_update|connection_status|error
    data: Any
    ts: float = Field(default_factory=time.time)
