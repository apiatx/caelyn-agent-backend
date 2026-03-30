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
import time
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .models import ScreenerAsset
from .ranking_engine import generate_rationale, rank_assets
from .state import HyperliquidState

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
    priority = ["L1", "DeFi", "AI", "meme", "gaming", "RWA"]
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

    return {
        # ── Identity ──────────────────────────────────────────────────────
        "rank":         rank,
        "coin":         asset.coin,
        "displayName":  asset.display_name,
        "marketType":   asset.market_type,
        "category":     _category(asset.tags),
        "tags":         asset.tags,

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
        "predictedFunding": None,            # not available in public API

        # ── Open interest ─────────────────────────────────────────────────
        "openInterest":  asset.open_interest_usd,
        "oiChangePct":   None,   # requires OI history cache — future
        "oiChange5m":    None,
        "oiChange1h":    None,

        # ── Volume ────────────────────────────────────────────────────────
        "volume24h":      asset.day_ntl_vlm,
        "volume24hBase":  asset.day_base_vlm,
        "volumeImpulse":  None,   # future: rolling vol-z-score

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

        # ── Scores 0-1 ────────────────────────────────────────────────────
        "volatility":         _s01(asset.volatility_score),
        "momentum":           _s01(asset.momentum_score),
        "breakoutScore":      _s01(asset.breakout_score),
        "meanReversionScore": _s01(asset.mean_reversion_score),
        "liquidityScore":     _s01(asset.liquidity_score),
        "flowScore":          _s01(asset.flow_score),
        "compositeSignal":    _s01(asset.composite_signal_score),

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
    sort_by: str = "compositeSignal",
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
    if min_volume_usd:
        assets = [a for a in assets if (a.day_ntl_vlm or 0) >= min_volume_usd]
    if max_spread_bps is not None:
        assets = [a for a in assets if (a.spread_bps or 0) <= max_spread_bps]

    # Sort by internal field, then convert to rows
    _SORT_MAP = {
        "compositeSignal":    "composite_signal_score",
        "volume24h":          "day_ntl_vlm",
        "openInterest":       "open_interest_usd",
        "change24hPct":       "pct_change_24h",
        "funding":            "funding",
        "spreadBps":          "spread_bps",
        "momentum":           "momentum_score",
        "breakoutScore":      "breakout_score",
        "liquidityScore":     "liquidity_score",
    }
    sort_field = _SORT_MAP.get(sort_by, "composite_signal_score")
    reverse = sort_dir.lower() != "asc"
    try:
        assets.sort(
            key=lambda a: (getattr(a, sort_field) or 0) if getattr(a, sort_field) is not None else -1e18,
            reverse=reverse,
        )
    except AttributeError:
        assets.sort(key=lambda a: a.composite_signal_score or 0, reverse=True)

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
    Deterministic agent ranking pass.
    Returns ranked coins in the frontend contract shape.
    """
    state = _get_state()
    if not state.is_ready:
        raise HTTPException(503, "Screener is still initializing. Please retry in a moment.")

    mode = req.rankingMode if req.rankingMode in ("balanced", "momentum", "breakout", "mean_reversion", "crowding_dislocation") else "balanced"

    # Rank using our internal state (real-time, richer than frontend rows)
    all_assets = [a for a in state.all_assets() if a.market_status == "active" and a.market_type == "perp"]
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

    return {
        "rankedCoins":   ranked_coins,
        "longs":         longs,
        "shorts":        shorts,
        "breakouts":     breakouts,
        "meanReversions": mean_revs,
        "avoid":         avoid,
        "summary":       summary,
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
    assets.sort(key=lambda a: a.composite_signal_score or 0, reverse=True)
    rows = [_asset_to_row(a, rank=i + 1) for i, a in enumerate(assets[:300])]
    meta = _build_meta(rows, state)
    return {
        "event": "snapshot_ready",
        "data": {"rows": rows, "meta": meta},
        "ts": time.time(),
    }


async def broadcast_asset_update(coin: str, state: HyperliquidState):
    """Push a single asset update to all connected frontend WS clients."""
    if not _ws_clients:
        return
    asset = state.get_asset(coin)
    if asset is None:
        return
    # Find approximate rank
    all_sorted = sorted(state.all_assets(), key=lambda a: a.composite_signal_score or 0, reverse=True)
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
