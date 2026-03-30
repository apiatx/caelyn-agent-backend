"""
Hyperliquid Screener — FastAPI router.

Endpoints:
  GET  /api/hyperliquid/screener/snapshot          — full screener table
  GET  /api/hyperliquid/screener/filters            — available filter options
  GET  /api/hyperliquid/screener/asset/{coin}       — single asset detail
  POST /api/hyperliquid/screener/agent-rank         — deterministic ranking
  WS   /api/hyperliquid/screener/ws                 — live push to frontend
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from .models import (
    AgentRankRequest,
    AgentRankResponse,
    AssetDetail,
    ScreenerAsset,
    ScreenerSnapshot,
    WsEvent,
)
from .ranking_engine import run_agent_rank
from .state import HyperliquidState

router = APIRouter(prefix="/api/hyperliquid/screener", tags=["hyperliquid"])

# Shared singleton state — injected by main.py after boot task starts
_state: Optional[HyperliquidState] = None


def set_state(state: HyperliquidState):
    global _state
    _state = state


def get_state() -> HyperliquidState:
    if _state is None:
        raise HTTPException(503, "Hyperliquid screener not yet initialized")
    return _state


# ─────────────────────────────────────────────────────────────────────────────
# REST endpoints
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/snapshot", response_model=ScreenerSnapshot)
async def get_snapshot(
    market_type: str = "all",
    sort_by: str = "composite_signal_score",
    sort_dir: str = "desc",
    min_volume_usd: Optional[float] = None,
    max_spread_bps: Optional[float] = None,
):
    """
    Full screener snapshot.

    Returns all normalized assets sorted by the requested field.
    Supports query-param filtering for common screener use cases.
    """
    state = get_state()

    rows = state.all_assets()

    # Market type filter
    if market_type in ("perp", "spot"):
        rows = [r for r in rows if r.market_type == market_type]

    if min_volume_usd:
        rows = [r for r in rows if (r.day_ntl_vlm or 0) >= min_volume_usd]

    if max_spread_bps is not None:
        rows = [r for r in rows if (r.spread_bps or 0) <= max_spread_bps]

    # Active only by default
    rows = [r for r in rows if r.market_status == "active"]

    # Sort
    reverse = sort_dir.lower() != "asc"
    try:
        rows = sorted(
            rows,
            key=lambda a: (getattr(a, sort_by) or 0) if getattr(a, sort_by) is not None else -float("inf"),
            reverse=reverse,
        )
    except AttributeError:
        rows = sorted(rows, key=lambda a: a.composite_signal_score or 0, reverse=True)

    perp_count = sum(1 for r in rows if r.market_type == "perp")
    spot_count = sum(1 for r in rows if r.market_type == "spot")

    summary = _build_summary(rows)

    return ScreenerSnapshot(
        rows=rows,
        total_assets=len(rows),
        perp_count=perp_count,
        spot_count=spot_count,
        data_freshness_seconds=state.freshness_seconds(),
        ws_connected=state.ws_connected,
        server_ts=time.time(),
        available_filters=_build_filter_options(state),
        summary_stats=summary,
    )


@router.get("/filters")
async def get_filters():
    """Available filter options for the screener UI."""
    state = get_state()
    return JSONResponse(content=_build_filter_options(state))


@router.get("/asset/{coin}", response_model=AssetDetail)
async def get_asset(coin: str):
    """
    Detailed view of a single asset including candles, trades, and book.
    """
    state = get_state()
    coin = coin.upper()
    asset = state.get_asset(coin)
    if asset is None:
        raise HTTPException(404, f"Asset '{coin}' not found in screener universe")

    candles_1h = state.get_candles(coin, "1h", n=50)
    candles_5m = state.get_candles(coin, "5m", n=50)
    recent_trades = state.get_recent_trades(coin, max_age_s=600)[-100:]
    book = state.get_book(coin) or {}

    return AssetDetail(
        asset=asset,
        candle_1h=list(candles_1h),
        candle_5m=list(candles_5m),
        recent_trades=list(recent_trades),
        l2_levels=book,
        score_history=[],   # future: store score history in state
        server_ts=time.time(),
    )


@router.post("/agent-rank", response_model=AgentRankResponse)
async def agent_rank(req: AgentRankRequest):
    """
    Deterministic agent ranking pass.

    Reranks the full universe under the requested mode and returns
    ranked rows, bucket summaries, and per-asset rationales.
    All computation is local — no LLM calls per row.
    """
    state = get_state()

    if not state.is_ready:
        raise HTTPException(503, "Screener is still initializing. Please retry in a moment.")

    try:
        result = run_agent_rank(state, req)
        return result
    except Exception as e:
        raise HTTPException(500, f"Ranking error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket — push live updates to frontend clients
# ─────────────────────────────────────────────────────────────────────────────

# Active frontend WS connections
_ws_clients: set[WebSocket] = set()
_ws_lock = asyncio.Lock()


@router.websocket("/ws")
async def screener_ws(websocket: WebSocket):
    """
    Backend → frontend WebSocket.

    On connect: sends a full snapshot_ready event.
    Ongoing: pushes asset_update events as prices/signals change.
    The frontend should apply delta updates to its local state.
    """
    await websocket.accept()
    async with _ws_lock:
        _ws_clients.add(websocket)

    state = get_state()

    try:
        # Send initial snapshot
        if state.is_ready:
            snapshot = _build_snapshot_event(state)
            await websocket.send_json(snapshot)
        else:
            await websocket.send_json(WsEvent(
                event="connection_status",
                data={"status": "initializing", "message": "Screener is booting. Snapshot will follow shortly."}
            ).model_dump())

        # Keep connection alive and forward state updates
        while True:
            try:
                # Client heartbeat / filter updates
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                msg = json.loads(raw)
                await _handle_ws_client_message(websocket, state, msg)
            except asyncio.TimeoutError:
                # Send a keepalive ping
                await websocket.send_json({"event": "ping", "ts": time.time()})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await websocket.send_json(WsEvent(
                event="error",
                data={"message": str(e)}
            ).model_dump())
        except Exception:
            pass
    finally:
        async with _ws_lock:
            _ws_clients.discard(websocket)


async def _handle_ws_client_message(ws: WebSocket, state: HyperliquidState, msg: dict):
    """Handle incoming client messages (e.g. refresh requests, filter changes)."""
    msg_type = msg.get("type", "")
    if msg_type == "refresh":
        snapshot = _build_snapshot_event(state)
        await ws.send_json(snapshot)
    elif msg_type == "subscribe_asset":
        coin = msg.get("coin", "").upper()
        asset = state.get_asset(coin)
        if asset:
            await ws.send_json(WsEvent(
                event="asset_update",
                data=asset.model_dump(),
            ).model_dump())
    elif msg_type == "pong":
        pass


def _build_snapshot_event(state: HyperliquidState) -> dict:
    rows = [a for a in state.all_assets() if a.market_status == "active"]
    rows.sort(key=lambda a: a.composite_signal_score or 0, reverse=True)
    return WsEvent(
        event="snapshot_ready",
        data={
            "rows": [r.model_dump() for r in rows[:300]],  # cap at 300 for payload size
            "total_assets": len(rows),
            "ws_connected": state.ws_connected,
            "freshness_seconds": state.freshness_seconds(),
            "server_ts": time.time(),
        }
    ).model_dump()


async def broadcast_asset_update(coin: str, state: HyperliquidState):
    """
    Push a single asset update to all connected frontend clients.
    Called by the WS consumer after processing an update for a coin.
    """
    if not _ws_clients:
        return
    asset = state.get_asset(coin)
    if asset is None:
        return
    payload = WsEvent(event="asset_update", data=asset.model_dump()).model_dump()
    dead = set()
    async with _ws_lock:
        for ws in list(_ws_clients):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.add(ws)
        _ws_clients -= dead


# ─────────────────────────────────────────────────────────────────────────────
# Helper builders
# ─────────────────────────────────────────────────────────────────────────────

def _build_summary(rows: list[ScreenerAsset]) -> dict[str, Any]:
    perps = [r for r in rows if r.market_type == "perp"]
    vlm_list = [r.day_ntl_vlm for r in perps if r.day_ntl_vlm]
    oi_list  = [r.open_interest_usd for r in perps if r.open_interest_usd]
    fund_list = [r.funding for r in perps if r.funding is not None]
    pct_list  = [r.pct_change_24h for r in perps if r.pct_change_24h is not None]

    def _safe_sum(lst): return round(sum(lst), 2) if lst else None
    def _safe_mean(lst): return round(sum(lst) / len(lst), 4) if lst else None

    return {
        "total_24h_volume_usd": _safe_sum(vlm_list),
        "total_open_interest_usd": _safe_sum(oi_list),
        "mean_funding_rate": _safe_mean(fund_list),
        "mean_pct_change_24h": _safe_mean(pct_list),
        "gainers_count":    sum(1 for p in pct_list if p > 0),
        "losers_count":     sum(1 for p in pct_list if p < 0),
        "flat_count":       sum(1 for p in pct_list if p == 0),
        "high_funding_count":  sum(1 for f in fund_list if abs(f) * 8760 > 0.30),
        "crowded_long_count":  sum(1 for r in perps if r.crowded_long),
        "crowded_short_count": sum(1 for r in perps if r.crowded_short),
        "squeeze_count":       sum(1 for r in perps if r.squeeze_candidate),
        "dislocation_count":   sum(1 for r in perps if r.dislocated_vs_oracle),
    }


def _build_filter_options(state: HyperliquidState) -> dict:
    rows = state.all_assets()
    all_tags = set()
    for r in rows:
        all_tags.update(r.tags)

    return {
        "market_types": ["perp", "spot", "all"],
        "ranking_modes": ["balanced", "momentum", "breakout", "mean_reversion", "crowding_dislocation"],
        "sort_fields": [
            "composite_signal_score", "day_ntl_vlm", "open_interest_usd",
            "pct_change_24h", "funding", "spread_bps", "realized_volatility_short",
            "momentum_1h", "momentum_4h", "orderbook_imbalance", "recent_trade_imbalance",
        ],
        "tags": sorted(all_tags),
        "flags": [
            "crowded_long", "crowded_short", "squeeze_candidate",
            "dislocated_vs_oracle", "trend_continuation_candidate",
            "mean_reversion_candidate", "illiquid_high_volatility", "avoid_due_to_spread",
        ],
        "exclude_flags": [
            "avoid_due_to_spread", "illiquid_high_volatility",
        ],
        "total_assets": len(rows),
        "perp_count": sum(1 for r in rows if r.market_type == "perp"),
        "spot_count": sum(1 for r in rows if r.market_type == "spot"),
    }
