"""
Hyperliquid Screener — FastAPI router.

Response shapes match the frontend data contract exactly:

GET  /api/hyperliquid/screener/snapshot  → { rows: [ScreenerRow], meta: ScreenerMeta }
POST /api/hyperliquid/screener/agent-rank → { rankedCoins, longs, shorts, breakouts, meanReversions, avoid, summary, generatedAt }
GET  /api/hyperliquid/screener/asset/{coin} → { coin, priceHistory, orderBook, recentTrades, ... }
WS   /api/hyperliquid/screener/ws
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .models import HeroSignal, ScreenerAsset
from .ranking_engine import generate_rationale, rank_assets
from .signal_modules import build_signal_payload
from .signals import build_signal_sections, build_summary_cards, generate_agent_briefing, generate_hero_signals, _compute_market_regime
from .state import HyperliquidState
from .tsmom import compute_tsmom_signals

router = APIRouter(prefix="/api/hyperliquid/screener", tags=["hyperliquid"])

_state: Optional[HyperliquidState] = None


def set_state(state: HyperliquidState):
    global _state
    _state = state


def _get_state() -> HyperliquidState:
    if _state is None:
        raise HTTPException(503, "Hyperliquid screener not yet initialized")
    return _state


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iso_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ─────────────────────────────────────────────────────────────────────────────
# Translation: internal ScreenerAsset → frontend ScreenerRow
# ─────────────────────────────────────────────────────────────────────────────

def _p(v):
    """Percent-stored value → decimal (5.3 → 0.053). None-safe."""
    return round(v / 100, 6) if v is not None else None


def _s01(v):
    """Score 0-100 → 0-1. None-safe."""
    return round(v / 100, 4) if v is not None else None


def _dir(d: Optional[str]) -> str:
    """Internal direction → frontend signalDirection."""
    return {"long": "bullish", "short": "bearish"}.get(d or "", "neutral")


def _category(tags: list[str]) -> Optional[str]:
    """Extract primary category from tags."""
    priority = ["equity", "pre-IPO", "commodity", "index", "macro", "L1", "DeFi", "AI", "meme", "gaming", "RWA"]
    for cat in priority:
        if cat in tags:
            return cat
    return None


def _asset_to_row(asset: ScreenerAsset, rank: int) -> dict:
    """
    Convert internal ScreenerAsset to the exact frontend ScreenerRow contract.

    Key normalizations:
    - pct/momentum fields stored as % → divide by 100 for decimal output
    - score fields stored 0-100 → divide by 100 for 0-1 output
    - signalDirection: "long"→"bullish", "short"→"bearish"
    - updatedAt: ISO 8601 with Z suffix
    - Never return 0 for unavailable metric — always null
    """
    # Spread computed fields
    bid = asset.bid_px
    ask = asset.ask_px
    spread_abs = (ask - bid) if (bid and ask) else asset.spread_abs
    mid = (bid + ask) / 2 if (bid and ask) else asset.mid_px
    spread_pct = (spread_abs / mid) if (spread_abs and mid and mid != 0) else None
    spread_bps = spread_pct * 10_000 if spread_pct is not None else asset.spread_bps

    # For spot assets and HIP-3 perps (prefixed), output the human-readable display_name as `coin`
    # Spot: display_name = "PURR", not "@14" or "PURR/USDC"
    # HIP-3: display_name = "TSLA", not "xyz:TSLA"
    is_hip3 = asset.market_type == "perp" and ":" in asset.coin
    coin_out = asset.display_name if (asset.market_type == "spot" or is_hip3) else asset.coin

    return {
        # ── Identity ──────────────────────────────────────────────────────
        "rank":                   rank,
        "coin":                   coin_out,
        "displayName":            asset.display_name,
        "canonicalCoinId":        asset.canonical_coin_id or asset.coin,
        "displaySymbol":          asset.display_symbol or coin_out,
        "isListedOnHyperliquid":  asset.is_listed_on_hyperliquid,
        "marketType":             asset.market_type,
        "category":               _category(asset.tags),
        "tags":                   asset.tags,

        # ── Price surface ─────────────────────────────────────────────────
        "markPrice":    asset.mark_px,
        "midPrice":     mid,
        "oraclePrice":  asset.oracle_px,
        "bboBid":       bid,
        "bboAsk":       ask,
        "spread":       round(spread_abs, 6) if spread_abs is not None else None,
        "spreadPct":    round(spread_pct, 6) if spread_pct is not None else None,
        "spreadBps":    round(spread_bps, 2) if spread_bps is not None else None,

        # ── Returns / rates ────────────────────────────────────────────────
        # change24hPct: decimal, 0.053 = +5.3%
        "change24hPct":     _p(asset.pct_change_24h),
        # premium: (mark-oracle)/oracle as decimal
        "premium":          asset.premium,   # already decimal from Hyperliquid
        # funding: hourly rate decimal (0.0001 = 0.01%/hr)
        "funding":          asset.funding,
        # funding8hPct: 8-hour funding rate as a percentage (what Hyperliquid displays)
        # e.g. raw=0.000935/hr → funding8hPct=0.748 (meaning 0.748% per 8h)
        "funding8hPct":     round((asset.funding or 0) * 8 * 100, 4),
        "predictedFunding": None,            # not available in public API

        # ── Open interest ─────────────────────────────────────────────────
        "openInterest":  asset.open_interest_usd,
        "oiChangePct":   asset.open_interest_change_pct,  # 1h change (decimal)
        "oiChange5m":    asset.oi_change_5m,
        "oiChange15m":   asset.oi_change_15m,
        "oiChange1h":    asset.oi_change_1h,

        # ── Volume ────────────────────────────────────────────────────────
        "volume24h":        asset.day_ntl_vlm,
        "volume24hBase":    asset.day_base_vlm,
        "volumeImpulse":    asset.volume_impulse,
        "volumeImpulse5m":  asset.volume_impulse_5m,
        "volumeImpulse15m": asset.volume_impulse_15m,

        # ── Trade flow ────────────────────────────────────────────────────
        "tradeCount":      asset.recent_trade_count if asset.recent_trade_count > 0 else None,
        "tradeImbalance":  asset.recent_trade_imbalance,

        # ── Order book ────────────────────────────────────────────────────
        "bidDepth":        asset.orderbook_bid_depth,
        "askDepth":        asset.orderbook_ask_depth,
        "bidAskImbalance": asset.orderbook_imbalance,
        "impactBidPx":     asset.impact_bid_px,
        "impactAskPx":     asset.impact_ask_px,

        # ── Dislocation (decimal, not %) ──────────────────────────────────
        "distMarkOracle":  _p(asset.distance_mark_oracle_pct),
        "distMarkMid":     _p(asset.distance_mark_mid_pct),
        "distMarkPrevDay": _p(asset.distance_mark_prev_day_pct),

        # ── Scores 0-1 (component scores) ─────────────────────────────────
        "volatility":               _s01(asset.volatility_score),
        "momentum":                 _s01(asset.momentum_score),
        "flow":                     _s01(asset.flow_score),
        "trend":                    _s01(asset.trend_score),
        "bookPressure":             _s01(asset.book_pressure_score),
        "crowding":                 _s01(asset.crowding_score),
        "dislocation":              _s01(asset.dislocation_score),
        "liquidityScore":           _s01(asset.liquidity_score),
        "tradabilityPenalty":       _s01(asset.tradability_penalty),

        # ── Setup-specific scores 0-1 ──────────────────────────────────────
        "breakoutScore":            _s01(asset.breakout_score),
        "meanReversionScore":       _s01(asset.mean_reversion_score),
        "trendContinuationScore":   _s01(asset.trend_continuation_score),
        "crowdingUnwindScore":      _s01(asset.crowding_unwind_score),
        "exhaustionScore":          _s01(asset.exhaustion_score),
        "collapseRiskScore":        _s01(asset.collapse_risk_score),
        "avoidScore":               _s01(asset.avoid_score),

        # ── Structural quality scores (v3 hierarchical pipeline) ──────────
        "structuralQualityScore":   _s01(asset.structural_quality_score),
        "assetRegime":              asset.asset_regime,
        "liquidityQualityScore":    _s01(asset.liquidity_quality_score),
        "pullbackQualityScore":     _s01(asset.pullback_quality_score),
        "breakoutReadinessScore":   _s01(asset.breakout_readiness_score),
        "continuationScore":        _s01(asset.continuation_score),
        "speculativeReversalScore": _s01(asset.speculative_reversal_score),

        # ── Overall ────────────────────────────────────────────────────────
        "overallScore":             _s01(asset.overall_score),
        "setupType":                asset.setup_type,
        "compositeSignal":          _s01(asset.composite_signal_score),
        "scoreChange":              asset.score_change,

        # ── Signal ────────────────────────────────────────────────────────
        "signalDirection":  _dir(asset.signal_direction),
        "signalConfidence": asset.signal_confidence,

        # ── Contract metadata ─────────────────────────────────────────────
        "maxLeverage":   asset.max_leverage,
        "szDecimals":    asset.sz_decimals,
        "marketStatus":  "trading" if asset.market_status == "active" else asset.market_status,
        "updatedAt":     _iso_ts(asset.last_updated_ts),

        # ── Agent fields (populated only by /agent-rank) ──────────────────
        "agentRank":      None,
        "agentScore":     None,
        "agentRationale": None,
        "rankDelta":      None,
    }


def _build_meta(rows: list[dict], state: HyperliquidState) -> dict:
    """Build ScreenerMeta from the row list."""
    changes = [r["change24hPct"] for r in rows if r.get("change24hPct") is not None]
    volumes = [(r["coin"], r["volume24h"]) for r in rows if r.get("volume24h")]
    ois     = [(r["coin"], r["openInterest"]) for r in rows if r.get("openInterest")]
    fundings = [(r["coin"], r["funding"]) for r in rows if r.get("funding") is not None]

    top_mover = max(rows, key=lambda r: abs(r.get("change24hPct") or 0), default=None)
    top_vol   = max(volumes, key=lambda x: x[1], default=(None, None)) if volumes else (None, None)
    top_oi    = max(ois, key=lambda x: x[1], default=(None, None)) if ois else (None, None)

    highest_fund = max(fundings, key=lambda x: x[1], default=(None, None)) if fundings else (None, None)
    lowest_fund  = min(fundings, key=lambda x: x[1], default=(None, None)) if fundings else (None, None)

    now_iso = _iso_now()
    return {
        "totalAssets":        len(rows),
        "gainers":            sum(1 for c in changes if c > 0),
        "losers":             sum(1 for c in changes if c < 0),
        "topMover":           top_mover["coin"] if top_mover else None,
        "topMoverPct":        top_mover["change24hPct"] if top_mover else None,
        "largestVolumeCoin":  top_vol[0],
        "largestVolume":      top_vol[1],
        "largestOICoin":      top_oi[0],
        "largestOI":          top_oi[1],
        "highestFunding":     highest_fund[1],
        "highestFundingCoin": highest_fund[0],
        "lowestFunding":      lowest_fund[1],
        "lowestFundingCoin":  lowest_fund[0],
        "lastUpdated":        now_iso,
        "serverTs":           now_iso,
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/hyperliquid/screener/snapshot
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/snapshot")
async def get_snapshot(
    market_type: str = "all",
    limit: int = 200,
    sort_by: str = "overallScore",
    sort_dir: str = "desc",
    min_volume_usd: Optional[float] = None,
    max_spread_bps: Optional[float] = None,
):
    """
    Full screener snapshot.
    Returns { rows: [ScreenerRow], meta: ScreenerMeta }
    """
    state = _get_state()

    assets = state.all_assets()

    # Filter
    if market_type in ("perp", "spot"):
        assets = [a for a in assets if a.market_type == market_type]
    assets = [a for a in assets if a.market_status == "active"]

    # Universe gate (defensive — state should only contain universe assets after boot)
    if state.universe_allowlist:
        assets = [a for a in assets if state.in_universe(a.coin)]

    # Volume gate: explicit parameter overrides; default minimum keeps junk tokens off the board.
    # Spot markets have a lower default since many legit spots are smaller than major perps.
    _default_spot_min   = 50_000     # $50K/day — eliminates user-created junk spot tokens
    _default_perp_min   = 0          # no default perp floor (perp universe is already clean)
    if min_volume_usd is not None:
        assets = [a for a in assets if (a.day_ntl_vlm or 0) >= min_volume_usd]
    else:
        assets = [
            a for a in assets
            if a.market_type != "spot" or (a.day_ntl_vlm or 0) >= _default_spot_min
        ]
    if max_spread_bps is not None:
        assets = [a for a in assets if (a.spread_bps or 0) <= max_spread_bps]

    # Sort by internal field, then convert to rows
    _SORT_MAP = {
        "overallScore":       "overall_score",
        "compositeSignal":    "composite_signal_score",
        "volume24h":          "day_ntl_vlm",
        "openInterest":       "open_interest_usd",
        "change24hPct":       "pct_change_24h",
        "funding":            "funding",
        "spreadBps":          "spread_bps",
        "momentum":           "momentum_score",
        "breakoutScore":      "breakout_score",
        "liquidityScore":     "liquidity_score",
        "structuralQuality":  "structural_quality_score",
    }
    sort_field = _SORT_MAP.get(sort_by, "overall_score")
    reverse = sort_dir.lower() != "asc"
    try:
        assets.sort(
            key=lambda a: (getattr(a, sort_field) or 0) if getattr(a, sort_field) is not None else -1e18,
            reverse=reverse,
        )
    except AttributeError:
        assets.sort(key=lambda a: a.overall_score or 0, reverse=True)

    assets = assets[:limit]

    rows = [_asset_to_row(a, rank=i + 1) for i, a in enumerate(assets)]
    meta = _build_meta(rows, state)

    return {"rows": rows, "meta": meta}


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/hyperliquid/screener/filters
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/filters")
async def get_filters():
    """Available filter options for the screener UI."""
    state = _get_state()
    rows = state.all_assets()
    all_tags = set()
    for r in rows:
        all_tags.update(r.tags)

    return {
        "marketTypes":    ["perp", "spot", "all"],
        "rankingModes":   ["balanced", "momentum", "breakout", "mean_reversion", "crowding_dislocation"],
        "sortFields":     ["compositeSignal", "volume24h", "openInterest", "change24hPct", "funding", "spreadBps", "momentum", "breakoutScore", "liquidityScore"],
        "tags":           sorted(all_tags),
        "flags":          ["crowded_long", "crowded_short", "squeeze_candidate", "dislocated_vs_oracle", "avoid_due_to_spread"],
        "totalAssets":    len(rows),
        "perpCount":      sum(1 for r in rows if r.market_type == "perp"),
        "spotCount":      sum(1 for r in rows if r.market_type == "spot"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/hyperliquid/screener/hero
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/hero")
async def get_hero_signals(max_ideas: int = 20, min_volume_usd: float = 5_000_000):
    """
    Agent Market Brief — full deterministic briefing payload.

    Returns:
      market_regime, regime_description, best_long, best_short,
      best_breakout_watch, best_exhaustion_watch, actionable_ideas[],
      guidance{ trade_now[], watch_breakout[], watch_collapse[], avoid[] },
      selected_thesis

    Each idea includes: coin, side, setup_type, score, confidence,
    thesis_title, thesis_summary, reasons[], what_to_watch[],
    invalidation_notes[], risk_flags[], metrics, scores (all components).

    No LLM calls — 100% deterministic scoring.
    """
    state = _get_state()
    if not state.is_ready and not state.all_assets():
        raise HTTPException(503, "Screener has no data yet. Please retry in ~30 seconds.")

    return generate_agent_briefing(state, max_ideas=max_ideas, min_volume_usd=min_volume_usd)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/hyperliquid/screener/sections
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/sections")
async def get_sections(rows_per_section: int = 6):
    """
    Section-oriented market data payload.

    Returns a structured dict of signal sections for the frontend terminal.
    Always-available sections are populated immediately.
    Conditional sections (OI expansion/unwind, volume impulse) show
    `available: false` until enough history has accumulated.
    """
    state = _get_state()
    if not state.is_ready and not state.all_assets():
        raise HTTPException(503, "Screener has no data yet. Please retry in ~30 seconds.")

    sections     = build_signal_sections(state, rows_per_section=rows_per_section)
    summary_cards = build_summary_cards(state)
    hero_signals  = generate_hero_signals(state, top_n=5)

    perps = [a for a in state.perp_assets() if a.market_status == "active"]
    has_oi_history = sum(1 for c in state.oi_history if len(state.oi_history[c]) >= 5) >= 10

    return {
        "heroAgentSignals": [s.model_dump() for s in hero_signals],
        "summaryCards":     summary_cards,
        "signalSections":   sections,
        "meta": {
            "totalPerps":         len(perps),
            "totalSpots":         len(state.spot_assets()),
            "oiHistoryAvailable": has_oi_history,
            "volImpulseAvailable": sum(1 for a in perps if a.volume_impulse_5m is not None) >= 10,
            "scoreHistoryAvailable": sum(1 for c in state.score_history if len(state.score_history[c]) >= 2) >= 10,
            "generatedAt":        _iso_now(),
            "scoreVersion":       "2.0",
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/hyperliquid/screener/asset/{coin}
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/asset/{coin}")
async def get_asset(coin: str):
    """
    Single asset detail.
    Returns { coin, priceHistory, orderBook, recentTrades, summaries, scoreHistory }
    """
    state = _get_state()
    coin = coin.upper()
    asset = state.get_asset(coin)
    if asset is None:
        raise HTTPException(404, f"Asset '{coin}' not found in screener universe")

    # Price history from 1h candles → simple {t, p} pairs
    candles_1h = state.get_candles(coin, "1h", n=50)
    candles_5m = state.get_candles(coin, "5m", n=50)
    price_history = [{"t": int(c["t"]), "p": float(c["c"])} for c in candles_1h if c.get("t") and c.get("c")]

    # Order book → [[price, size], ...]
    book = state.get_book(coin) or {}
    levels = book.get("levels", [[], []])
    def _fmt_levels(lvl_list, top=10):
        out = []
        for lvl in lvl_list[:top]:
            if isinstance(lvl, dict):
                px, sz = lvl.get("px"), lvl.get("sz")
            elif isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                px, sz = lvl[0], lvl[1]
            else:
                continue
            try:
                out.append([float(px), float(sz)])
            except (TypeError, ValueError):
                pass
        return out

    order_book = {
        "bids": _fmt_levels(levels[0] if len(levels) > 0 else []),
        "asks": _fmt_levels(levels[1] if len(levels) > 1 else []),
    }

    # Recent trades → {t, p, sz, side}
    raw_trades = state.get_recent_trades(coin, max_age_s=600)[-100:]
    recent_trades = [
        {
            "t":    int(t.get("time", 0)),
            "p":    float(t.get("px", 0) or 0),
            "sz":   float(t.get("sz", 0) or 0),
            "side": "B" if t.get("side") in ("B", "buy") else "S",
        }
        for t in raw_trades
        if t.get("time") and t.get("px")
    ]

    # Deterministic text summaries
    momentum_summary  = _momentum_summary(asset)
    liquidity_summary = _liquidity_summary(asset)
    structure_summary = _structure_summary(asset)
    agent_rationale   = generate_rationale(asset, "balanced")

    return {
        "coin":             coin,
        "priceHistory":     price_history,
        "orderBook":        order_book,
        "recentTrades":     recent_trades,
        "momentumSummary":  momentum_summary,
        "liquiditySummary": liquidity_summary,
        "marketStructure":  structure_summary,
        "agentRationale":   agent_rationale,
        "scoreHistory":     [],   # future: persist score snapshots
    }


def _momentum_summary(a: ScreenerAsset) -> Optional[str]:
    parts = []
    if a.momentum_1h is not None:
        arrow = "↑" if a.momentum_1h > 0 else "↓"
        parts.append(f"1h {arrow} {abs(a.momentum_1h):.2f}%")
    if a.momentum_4h is not None:
        arrow = "↑" if a.momentum_4h > 0 else "↓"
        parts.append(f"4h {arrow} {abs(a.momentum_4h):.2f}%")
    if a.pct_change_24h is not None:
        arrow = "↑" if a.pct_change_24h > 0 else "↓"
        parts.append(f"24h {arrow} {abs(a.pct_change_24h):.2f}%")
    if a.realized_volatility_short:
        parts.append(f"RVol {a.realized_volatility_short:.0f}%")
    if not parts:
        return None
    return " | ".join(parts)


def _liquidity_summary(a: ScreenerAsset) -> Optional[str]:
    parts = []
    if a.day_ntl_vlm:
        parts.append(f"24h vol ${a.day_ntl_vlm / 1e6:.0f}M")
    if a.orderbook_bid_depth and a.orderbook_ask_depth:
        ratio = a.orderbook_bid_depth / a.orderbook_ask_depth if a.orderbook_ask_depth > 0 else None
        if ratio:
            parts.append(f"Bid/Ask depth {ratio:.1f}×")
    if a.spread_bps:
        parts.append(f"Spread {a.spread_bps:.1f}bps")
    if not parts:
        return None
    return ". ".join(parts) + "."


def _structure_summary(a: ScreenerAsset) -> Optional[str]:
    parts = []
    ann_fund = (a.funding or 0) * 8760
    if abs(ann_fund) > 0.20:
        side = "longs" if ann_fund > 0 else "shorts"
        parts.append(f"{side.capitalize()} paying {abs(ann_fund):.0%} ann. funding")
    if a.crowded_long:
        parts.append("crowded long setup")
    elif a.crowded_short:
        parts.append("crowded short — squeeze risk")
    if a.squeeze_candidate:
        parts.append("squeeze candidate")
    if a.dislocated_vs_oracle:
        parts.append(f"oracle dislocation {a.distance_mark_oracle_pct:+.2f}%")
    if a.open_interest_usd:
        parts.append(f"OI ${a.open_interest_usd / 1e6:.0f}M")
    if not parts:
        return "No notable structural setups."
    return ". ".join(parts) + "."


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/hyperliquid/screener/agent-rank
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_fear_greed() -> dict:
    """Fetch Crypto Fear & Greed Index from alternative.me (free, no auth)."""
    try:
        import httpx
        async with httpx.AsyncClient(timeout=4.0) as http:
            r = await http.get("https://api.alternative.me/fng/?limit=1")
            data = r.json()
            items = data.get("data", [])
            return items[0] if items else {}
    except Exception as e:
        print(f"[HL][agent] Fear & Greed fetch error: {e}")
        return {}


def _build_asset_line(a: ScreenerAsset) -> str:
    """
    Build a rich single-line summary of an asset for the LLM prompt.
    Packs all computed signals into a compact, readable format.
    """
    fund_8h  = (a.funding or 0.0) * 8 * 100          # 8-hour funding %
    chg      = a.pct_change_24h or 0.0
    oi       = (a.open_interest_usd or 0.0) / 1_000_000
    vol      = (a.day_ntl_vlm or 0.0) / 1_000_000
    score    = a.composite_signal_score or 50
    setup    = a.setup_type or "—"

    parts = [
        f"{a.coin}",
        f"${a.mark_px:.4g}" if a.mark_px else "",
        f"24h:{chg:+.1f}%",
        f"8hFund:{fund_8h:+.4f}%",
        f"OI:${oi:.0f}M",
        f"Vol:${vol:.0f}M",
        f"score:{score:.0f}",
        f"setup:{setup}",
    ]

    extras = []
    if a.momentum_1h is not None:
        extras.append(f"mom1h:{a.momentum_1h:+.2f}%")
    if a.momentum_4h is not None:
        extras.append(f"mom4h:{a.momentum_4h:+.2f}%")
    if a.recent_trade_imbalance is not None:
        flow_pct = a.recent_trade_imbalance * 100
        extras.append(f"flow:{flow_pct:+.0f}%buy")
    if a.orderbook_imbalance is not None:
        extras.append(f"book:{a.orderbook_imbalance:+.2f}")
    if a.oi_change_1h is not None:
        extras.append(f"OIchg1h:{a.oi_change_1h*100:+.1f}%")
    if a.volume_impulse is not None and abs(a.volume_impulse) > 0.2:
        extras.append(f"volImpulse:{a.volume_impulse:+.2f}")
    if a.distance_mark_oracle_pct is not None and abs(a.distance_mark_oracle_pct) > 0.3:
        extras.append(f"oracleDelta:{a.distance_mark_oracle_pct:+.2f}%")
    if a.exhaustion_score is not None and a.exhaustion_score > 55:
        extras.append(f"exhaustion:{a.exhaustion_score:.0f}")
    if a.collapse_risk_score is not None and a.collapse_risk_score > 55:
        extras.append(f"collapseRisk:{a.collapse_risk_score:.0f}")
    if a.squeeze_candidate:
        extras.append("SQUEEZE")
    if a.crowded_long:
        extras.append("CROWDED_LONG")
    if a.crowded_short:
        extras.append("CROWDED_SHORT")

    if extras:
        parts.append("|".join(extras))

    return "  " + " · ".join(p for p in parts if p)


async def _generate_llm_analysis(ranked: list[ScreenerAsset], fear_greed: dict) -> str:
    """
    Call Claude for a full derivatives market analysis using all computed signals.
    Returns empty string if ANTHROPIC_API_KEY not set or on error (non-fatal).
    Retries once on transient failure.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key or not ranked:
        return ""

    perps = [a for a in ranked if a.market_type == "perp"]

    # ── Market regime ───────────────────────────────────────────────────────
    try:
        regime_data = _compute_market_regime(perps)
        regime      = regime_data.get("regime", "unknown")
        regime_desc = regime_data.get("description", "")
    except Exception:
        regime = "unknown"; regime_desc = ""

    # ── Breadth stats ───────────────────────────────────────────────────────
    long_count   = sum(1 for a in perps if a.signal_direction == "long")
    short_count  = sum(1 for a in perps if a.signal_direction == "short")
    neutral_count= len(perps) - long_count - short_count
    crowded_long_ct  = sum(1 for a in perps if a.crowded_long)
    crowded_short_ct = sum(1 for a in perps if a.crowded_short)
    squeeze_ct   = sum(1 for a in perps if a.squeeze_candidate)
    exhaust_ct   = sum(1 for a in perps if (a.exhaustion_score or 0) > 55)
    collapse_ct  = sum(1 for a in perps if (a.collapse_risk_score or 0) > 55)
    disloc_ct    = sum(1 for a in perps if a.dislocated_vs_oracle)
    avg_fund_8h  = (
        sum((a.funding or 0) * 8 * 100 for a in perps[:60]) / max(len(perps[:60]), 1)
    )

    fg_val = fear_greed.get("value", "N/A")
    fg_cls = fear_greed.get("value_classification", "Unknown")

    btc = next((a for a in perps if a.coin == "BTC"), None)
    eth = next((a for a in perps if a.coin == "ETH"), None)
    btc_line = (
        f"BTC ${btc.mark_px:,.0f} | 24h:{btc.pct_change_24h or 0:+.1f}% | "
        f"8hFund:{(btc.funding or 0)*8*100:+.4f}% | OI:${(btc.open_interest_usd or 0)/1e9:.1f}B"
    ) if btc else "BTC: not in universe"
    eth_line = (
        f"ETH ${eth.mark_px:,.0f} | 24h:{eth.pct_change_24h or 0:+.1f}% | "
        f"8hFund:{(eth.funding or 0)*8*100:+.4f}%"
    ) if eth else ""

    # ── Asset buckets ───────────────────────────────────────────────────────
    top_longs   = [a for a in perps if a.signal_direction == "long"][:8]
    top_shorts  = [a for a in perps if a.signal_direction == "short"][:8]
    breakouts   = sorted(perps, key=lambda a: -(a.breakout_score or 0))[:5]
    squeezes    = [a for a in perps if a.squeeze_candidate][:4]
    exhausted   = sorted(
        [a for a in perps if (a.exhaustion_score or 0) > 55],
        key=lambda a: -(a.exhaustion_score or 0)
    )[:4]
    dislocated  = sorted(
        [a for a in perps if a.dislocated_vs_oracle],
        key=lambda a: -abs(a.distance_mark_oracle_pct or 0)
    )[:4]

    def _section(title: str, assets: list[ScreenerAsset]) -> str:
        if not assets:
            return f"{title}: (none)\n"
        lines = "\n".join(_build_asset_line(a) for a in assets)
        return f"{title}:\n{lines}\n"

    prompt = (
        "You are a professional Hyperliquid perpetuals trader with deep expertise in "
        "derivatives microstructure, funding dynamics, and momentum/mean-reversion signals.\n\n"
        "═══ MARKET CONTEXT ═══\n"
        f"Fear & Greed: {fg_val}/100 ({fg_cls})\n"
        f"{btc_line}\n"
        + (f"{eth_line}\n" if eth_line else "")
        + f"Regime: {regime} — {regime_desc}\n"
        f"Breadth: {long_count} bullish / {short_count} bearish / {neutral_count} neutral "
        f"({len(perps)} perps scanned)\n"
        f"Avg 8h funding: {avg_fund_8h:+.4f}% "
        f"(+ve = longs crowded | –ve = shorts crowded)\n"
        f"Crowded longs: {crowded_long_ct} | Crowded shorts: {crowded_short_ct}\n"
        f"Squeeze candidates: {squeeze_ct} | Exhaustion signals: {exhaust_ct} | "
        f"Collapse risk: {collapse_ct} | Oracle dislocations: {disloc_ct}\n\n"
        "═══ SIGNAL DATA ═══\n"
        "Fields per asset: coin · price · 24h% · 8hFund% · OI · Vol · compositeScore · setupType "
        "· mom1h · mom4h · flowBuy% · bookImbalance · OIchg1h · volImpulse · oracleDelta · "
        "exhaustionScore · collapseRisk · SQUEEZE/CROWDED flags\n\n"
        + _section("TOP LONG SETUPS", top_longs)
        + "\n"
        + _section("TOP SHORT SETUPS", top_shorts)
        + "\n"
        + _section("BREAKOUT CANDIDATES", breakouts)
        + "\n"
        + (_section("SQUEEZE CANDIDATES (short-crowded, neg funding)", squeezes) if squeezes else "")
        + (_section("EXHAUSTION / COLLAPSE RISK (overextended, fade setups)", exhausted) if exhausted else "")
        + (_section("ORACLE DISLOCATIONS (mark vs oracle gap)", dislocated) if dislocated else "")
        + "\n═══ YOUR ANALYSIS ═══\n"
        "Write exactly 3 paragraphs, total ≤ 300 words:\n\n"
        "1. REGIME & BIAS — What the breadth, funding, and fear/greed data tell you about "
        "current market positioning and risk. Is there crowding? Are there squeeze risks? "
        "What's the dominant flow narrative?\n\n"
        "2. BEST LONG SETUPS — Pick 2-3 specific coins from the data above. For each: "
        "why it ranks (momentum, flow, setup type, funding tail-wind), and the exact "
        "price/structure condition that makes it a trade right now.\n\n"
        "3. BEST SHORT OR FADE SETUP — Pick 1-2 coins. Could be a crowded-long fade, "
        "an exhaustion collapse, a squeeze play, or an oracle-dislocation mean-reversion. "
        "State the thesis precisely.\n\n"
        "Rules: No disclaimers. No generic crypto commentary. Every claim must reference "
        "a specific data point from the tables above. Trader to trader. Be direct."
    )

    async def _call_claude() -> str:
        import anthropic as _anthropic
        aclient = _anthropic.AsyncAnthropic(api_key=api_key)
        msg = await aclient.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()

    async def _call_gpt() -> str:
        oai_key = os.getenv("OPENAI_API_KEY")
        if not oai_key:
            raise RuntimeError("No OpenAI key")
        import openai as _openai
        aclient = _openai.AsyncOpenAI(api_key=oai_key)
        resp = await aclient.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.choices[0].message.content.strip()

    # Try Claude first, fall back to GPT-4o-mini
    for provider_name, provider_fn in [("Claude", _call_claude), ("GPT-4o-mini", _call_gpt)]:
        for attempt in range(2):
            try:
                result = await provider_fn()
                print(f"[HL][agent] LLM analysis OK via {provider_name} ({len(result)} chars)")
                return result
            except Exception as e:
                print(f"[HL][agent] {provider_name} attempt {attempt+1} error: {type(e).__name__}: {str(e)[:120]}")
                if attempt == 0:
                    await asyncio.sleep(1.0)
    print("[HL][agent] All LLM providers failed — returning empty analysis")
    return ""


class AgentRankIn(BaseModel):
    """
    Frontend sends the current screener rows for re-ranking.
    We use our internal state (richer data) but accept the request format.
    """
    rows: list[dict] = Field(default_factory=list)
    rankingMode: str = "balanced"
    topN: int = 20
    includeRationales: bool = True


@router.post("/agent-rank")
async def agent_rank(req: AgentRankIn):
    """
    Agent ranking + LLM market analysis.
    Returns deterministic ranked coins + Claude (with GPT fallback) analysis.
    """
    state = _get_state()

    # Use any available assets — do NOT block on is_ready.
    # Only hard-fail if we literally have zero data (never connected to Hyperliquid).
    all_assets = [a for a in state.all_assets() if a.market_status == "active" and a.market_type == "perp"]
    if not all_assets:
        raise HTTPException(503, "Screener has no market data yet. Please wait ~30 seconds after startup and retry.")

    mode = req.rankingMode if req.rankingMode in ("balanced", "momentum", "breakout", "mean_reversion", "crowding_dislocation") else "balanced"

    # Start Fear & Greed fetch concurrently while ranking runs (synchronous CPU work)
    fg_task = asyncio.create_task(_fetch_fear_greed())

    # Rank using our internal state (real-time, richer than frontend rows)
    ranked = rank_assets(all_assets, mode=mode, prev_ranks=state.prev_ranks)

    # Update prev ranks for next call
    state.prev_ranks = {a.coin: a.rank for a in ranked if a.rank is not None}

    def _to_ranked_item(a: ScreenerAsset, direction_override: Optional[str] = None) -> dict:
        signal_dir = direction_override or a.signal_direction or "neutral"
        dir_out = {"long": "long", "short": "short", "neutral": "neutral"}.get(signal_dir, "neutral")

        rationale = generate_rationale(a, mode) if req.includeRationales else None
        rank_movement = None
        if a.prev_rank is not None and a.rank is not None:
            rank_movement = a.prev_rank - a.rank   # positive = moved up

        return {
            "coin":        a.coin,
            "agentRank":   a.rank,
            "agentScore":  round((a.composite_signal_score or 50) / 100, 4),
            "direction":   dir_out,
            "confidence":  round(a.signal_confidence or 0.5, 3),
            "rationale":   rationale,
            "rankMovement": rank_movement,
            "featureContributions": {
                "momentum":       _s01(a.momentum_score),
                "flow":           _s01(a.flow_score),
                "breakout":       _s01(a.breakout_score),
                "mean_reversion": _s01(a.mean_reversion_score),
                "liquidity":      _s01(a.liquidity_score),
            } if a.score_components else None,
        }

    ranked_coins = [_to_ranked_item(a) for a in ranked]

    longs     = [_to_ranked_item(a) for a in ranked if a.signal_direction == "long"][:5]
    shorts    = [_to_ranked_item(a) for a in ranked if a.signal_direction == "short"][:5]
    breakouts = [_to_ranked_item(a) for a in sorted(ranked, key=lambda x: -(x.breakout_score or 0))[:5]]
    mean_revs = [_to_ranked_item(a) for a in sorted(ranked, key=lambda x: -(x.mean_reversion_score or 0))[:5]]
    avoid     = [_to_ranked_item(a, direction_override="avoid") for a in ranked if a.avoid_due_to_spread or a.illiquid_high_volatility][:5]

    # Deterministic summary
    long_count  = sum(1 for a in ranked if a.signal_direction == "long")
    short_count = sum(1 for a in ranked if a.signal_direction == "short")
    top_coin    = ranked[0].coin if ranked else "N/A"
    ann_funds   = [(a.coin, (a.funding or 0) * 8760) for a in ranked]
    extreme_fund = max(ann_funds, key=lambda x: abs(x[1]), default=("N/A", 0))
    summary = (
        f"Universe of {len(ranked)} perps ranked by {mode} mode. "
        f"{long_count} bullish vs {short_count} bearish signals. "
        f"Top ranked: {top_coin}. "
        f"Most extreme funding: {extreme_fund[0]} ({extreme_fund[1]:+.0%} ann.)."
    )

    # Await Fear & Greed (should already be done by now) then call LLM (25s timeout)
    fear_greed = await fg_task
    try:
        llm_analysis = await asyncio.wait_for(
            _generate_llm_analysis(ranked, fear_greed),
            timeout=25.0,
        )
    except asyncio.TimeoutError:
        print("[HL][agent] LLM analysis timed out after 25s — returning without analysis")
        llm_analysis = ""

    return {
        "rankedCoins":   ranked_coins,
        "longs":         longs,
        "shorts":        shorts,
        "breakouts":     breakouts,
        "meanReversions": mean_revs,
        "avoid":         avoid,
        "summary":       summary,
        "llmAnalysis":   llm_analysis,
        "fearGreed":     fear_greed,
        "generatedAt":   _iso_now(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# WS /api/hyperliquid/screener/ws  — live push to frontend
# ─────────────────────────────────────────────────────────────────────────────

_ws_clients: set[WebSocket] = set()
_ws_lock = asyncio.Lock()


@router.websocket("/ws")
async def screener_ws(websocket: WebSocket):
    """
    Backend → frontend WebSocket.
    Events: snapshot_ready | asset_update | connection_status | error | ping
    """
    await websocket.accept()
    async with _ws_lock:
        _ws_clients.add(websocket)

    state = _get_state()

    try:
        # Send initial snapshot or initializing status
        if state.is_ready:
            await websocket.send_json(_build_ws_snapshot(state))
        else:
            await websocket.send_json({"event": "connection_status", "data": {"status": "initializing"}, "ts": time.time()})

        # Keep alive — forward client messages
        while True:
            try:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                msg = json.loads(raw)
                msg_type = msg.get("type", "")
                if msg_type == "refresh":
                    await websocket.send_json(_build_ws_snapshot(state))
                elif msg_type == "pong":
                    pass
            except asyncio.TimeoutError:
                await websocket.send_json({"event": "ping", "ts": time.time()})
    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await websocket.send_json({"event": "error", "data": {"message": str(e)}, "ts": time.time()})
        except Exception:
            pass
    finally:
        async with _ws_lock:
            _ws_clients.discard(websocket)


def _build_ws_snapshot(state: HyperliquidState) -> dict:
    assets = [a for a in state.all_assets() if a.market_status == "active"]
    assets.sort(key=lambda a: a.overall_score or 0, reverse=True)
    rows = [_asset_to_row(a, rank=i + 1) for i, a in enumerate(assets[:300])]
    meta = _build_meta(rows, state)
    return {
        "event": "snapshot_ready",
        "data": {"rows": rows, "meta": meta},
        "ts": time.time(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/hyperliquid/screener/tsmom-signals
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/tsmom-signals")
async def get_tsmom_signals(top_n: int = 60):
    """
    Time-Series Momentum (TSMOM) signals for top perps.

    Returns multi-lookback z-score signals, funding-adjusted and vol-targeted.
    1d candle data is loaded in the post-boot enrichment task (~30-60s after boot).
    Returns empty signals (not 503) while data is loading to avoid frontend error state.
    """
    state = _get_state()
    # Return empty response during boot rather than 503 — frontend shows
    # a friendly "loading" message and auto-refreshes every 60s.
    if not state.is_ready:
        return {
            "signals": [],
            "meta": {
                "total_signals": 0,
                "long_count": 0,
                "short_count": 0,
                "flat_count": 0,
                "generated_at": _iso_now(),
                "status": "initializing",
            },
        }

    return compute_tsmom_signals(state, top_n=top_n)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/hyperliquid/screener/signals
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/signals")
async def get_signals(
    benchmark: str = "BTC",
    depth_window_bps: float = 20.0,
    top_n: int = 20,
):
    """
    Advanced signal modules: Relative Strength Leaders, Order Book Pressure,
    OI Regime Shift, and OI Cap Risk.

    Returns:
      {
        "relative_strength_leaders": [...],
        "order_book_pressure": [...],
        "oi_regime_shift": [...],
        "oi_cap_risk": [...],
        "as_of": "ISO timestamp",
        "metadata": { "benchmark", "depth_window_bps", "intervals", "top_n", "oi_cap_thresholds" }
      }
    """
    state = _get_state()
    if not state.is_ready and not state.all_assets():
        raise HTTPException(503, "Screener has no data yet. Please retry in ~30 seconds.")

    top_n = max(1, min(top_n, 100))
    depth_window_bps = max(5, min(depth_window_bps, 100))

    return build_signal_payload(
        state,
        benchmark=benchmark,
        depth_window_bps=depth_window_bps,
        top_n=top_n,
    )


async def broadcast_asset_update(coin: str, state: HyperliquidState):
    """Push a single asset update to all connected frontend WS clients."""
    if not _ws_clients:
        return
    asset = state.get_asset(coin)
    if asset is None:
        return
    # Find approximate rank
    all_sorted = sorted(state.all_assets(), key=lambda a: a.overall_score or 0, reverse=True)
    rank = next((i + 1 for i, a in enumerate(all_sorted) if a.coin == coin), 0)
    row = _asset_to_row(asset, rank=rank)
    payload = {"event": "asset_update", "data": row, "ts": time.time()}
    dead = set()
    async with _ws_lock:
        for ws in list(_ws_clients):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.add(ws)
        _ws_clients -= dead
