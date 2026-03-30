"""
Hyperliquid Screener — WebSocket consumer + boot sequence.

Boot sequence:
  1. REST: fetch metaAndAssetCtxs → initialize all perp assets
  2. REST: fetch spotMetaAndAssetCtxs → extend with spot assets
  3. REST: fetch allMids → patch mid prices
  4. REST: fetch 1h candles for top-40 assets → volatility/momentum
  5. REST: fetch 5m candles for top-20 assets → short-term vol/momentum
  6. REST: fetch L2 books for top-20 assets → book depth features
  7. Run full feature pass → compute all signals
  8. Mark state.is_ready = True
  9. Connect WebSocket → subscribe to allMids + activeAssetCtx + bbo + trades
 10. Background: periodic candle refresh (every 5 min)
 11. Background: periodic feature recompute (every 60s)
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Optional

import websockets
import websockets.exceptions

from .client import HyperliquidRestClient
from .feature_engine import run_full_feature_pass
from .normalizer import (
    build_perp_universe,
    build_spot_universe,
    patch_from_active_asset_ctx,
    patch_from_all_mids,
    patch_from_bbo,
    patch_from_l2,
    patch_trade_flow,
)
from .state import HyperliquidState

_WS_URL = "wss://api.hyperliquid.xyz/ws"

# Subscription thresholds
_CTX_SUBS   = 50   # activeAssetCtx subscriptions (top N by OI)
_BBO_SUBS   = 30   # BBO subscriptions
_TRADE_SUBS = 30   # trades subscriptions

# Reconnect backoff
_RECONNECT_MIN_S = 3.0
_RECONNECT_MAX_S = 60.0

# Heartbeat interval
_PING_INTERVAL_S = 20.0

_shutdown = False


async def boot_and_run(state: HyperliquidState):
    """
    Top-level background task entry point.
    Runs the boot sequence, then starts the WebSocket consumer
    and periodic background tasks concurrently.
    """
    client = HyperliquidRestClient()
    try:
        print("[HL] Starting boot sequence...")
        await _boot_sequence(state, client)
        print(f"[HL] Boot complete — {len(state.assets)} assets ready. Starting WS...")
        state.is_ready = True
        state.boot_ts = time.time()

        # Run all long-lived tasks concurrently
        await asyncio.gather(
            _ws_consumer(state),
            _periodic_candle_refresh(state, client),
            _periodic_feature_recompute(state),
            return_exceptions=True,
        )
    except Exception as e:
        print(f"[HL] boot_and_run fatal error: {e}")
    finally:
        await client.close()


# ─────────────────────────────────────────────────────────────────────────────
# Boot sequence
# ─────────────────────────────────────────────────────────────────────────────

async def _boot_sequence(state: HyperliquidState, client: HyperliquidRestClient):
    # 1. Perp universe
    print("[HL][boot] Fetching perp universe...")
    try:
        meta_ctxs = await client.get_meta_and_asset_ctxs()
        perp_assets = build_perp_universe(meta_ctxs)
        for coin, asset in perp_assets.items():
            state.assets[coin] = asset
            state.meta[coin] = {}
        print(f"[HL][boot] Loaded {len(perp_assets)} perp assets")
    except Exception as e:
        print(f"[HL][boot] Perp universe error: {e}")

    # 2. Spot universe
    print("[HL][boot] Fetching spot universe...")
    try:
        spot_ctxs = await client.get_spot_meta_and_asset_ctxs()
        spot_assets = build_spot_universe(spot_ctxs)
        for coin, asset in spot_assets.items():
            if coin not in state.assets:   # don't overwrite a perp with same name
                state.assets[coin] = asset
        print(f"[HL][boot] Loaded {len(spot_assets)} spot assets")
    except Exception as e:
        print(f"[HL][boot] Spot universe error: {e}")

    # 3. All mids
    try:
        mids = await client.get_all_mids()
        patch_from_all_mids(state, mids)
        print(f"[HL][boot] Patched mids for {len(mids)} coins")
    except Exception as e:
        print(f"[HL][boot] allMids error: {e}")

    # 4. 1h candles for top-40 by volume
    top40 = state.top_coins_by_volume(40)
    print(f"[HL][boot] Fetching 1h candles for {len(top40)} assets...")
    try:
        candles_1h = await client.get_candles_multi(top40, "1h", n_bars=50)
        for coin, bars in candles_1h.items():
            if bars:
                state.add_candles(coin, "1h", bars)
        print(f"[HL][boot] 1h candles loaded for {sum(1 for b in candles_1h.values() if b)} coins")
    except Exception as e:
        print(f"[HL][boot] 1h candle error: {e}")

    # 5. 5m candles for top-20
    top20 = top40[:20]
    print(f"[HL][boot] Fetching 5m candles for {len(top20)} assets...")
    try:
        candles_5m = await client.get_candles_multi(top20, "5m", n_bars=50)
        for coin, bars in candles_5m.items():
            if bars:
                state.add_candles(coin, "5m", bars)
    except Exception as e:
        print(f"[HL][boot] 5m candle error: {e}")

    # 6. L2 books for top-20
    print(f"[HL][boot] Fetching L2 books for {len(top20)} assets...")
    try:
        books = await client.get_l2_books_multi(top20)
        for coin, book in books.items():
            levels = book.get("levels") or []
            if levels:
                patch_from_l2(state, coin, levels)
                state.set_book(coin, book)
    except Exception as e:
        print(f"[HL][boot] L2 books error: {e}")

    # 7. Initial feature pass
    print("[HL][boot] Running feature pass...")
    n = run_full_feature_pass(state)
    print(f"[HL][boot] Features computed for {n} assets")


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket consumer
# ─────────────────────────────────────────────────────────────────────────────

async def _ws_consumer(state: HyperliquidState):
    """
    Connect to Hyperliquid WebSocket, subscribe to live feeds,
    and process incoming messages indefinitely with auto-reconnect.
    """
    backoff = _RECONNECT_MIN_S
    while not _shutdown:
        try:
            print("[HL][ws] Connecting...")
            async with websockets.connect(
                _WS_URL,
                ping_interval=None,     # we handle pings manually
                open_timeout=15,
                close_timeout=10,
            ) as ws:
                state.ws_connected = True
                backoff = _RECONNECT_MIN_S
                print("[HL][ws] Connected. Subscribing...")
                await _subscribe_all(ws, state)
                print("[HL][ws] Subscriptions sent. Consuming messages...")

                ping_task = asyncio.create_task(_ping_loop(ws))
                try:
                    async for raw in ws:
                        await _handle_message(state, raw)
                finally:
                    ping_task.cancel()

        except websockets.exceptions.ConnectionClosed as e:
            print(f"[HL][ws] Connection closed: {e}. Reconnecting in {backoff}s...")
        except Exception as e:
            print(f"[HL][ws] Error: {e}. Reconnecting in {backoff}s...")
        finally:
            state.ws_connected = False

        await asyncio.sleep(backoff)
        backoff = min(backoff * 1.5, _RECONNECT_MAX_S)


async def _subscribe_all(ws, state: HyperliquidState):
    """Send all subscription requests on a fresh connection."""
    # 1. allMids — one sub covers every asset
    await _subscribe(ws, {"type": "allMids"})

    # 2. activeAssetCtx for top-N perps (funding, OI, mark updates)
    top_ctx = state.top_coins_by_oi(_CTX_SUBS)
    for coin in top_ctx:
        await _subscribe(ws, {"type": "activeAssetCtx", "coin": coin})

    # 3. BBO for top-N
    top_bbo = state.top_coins_by_volume(_BBO_SUBS)
    for coin in top_bbo:
        await _subscribe(ws, {"type": "bbo", "coin": coin})

    # 4. Trades for top-N
    top_trades = state.top_coins_by_volume(_TRADE_SUBS)
    for coin in top_trades:
        await _subscribe(ws, {"type": "trades", "coin": coin})


async def _subscribe(ws, subscription: dict):
    await ws.send(json.dumps({"method": "subscribe", "subscription": subscription}))


async def _ping_loop(ws):
    """Send periodic pings to keep the WS alive."""
    while True:
        await asyncio.sleep(_PING_INTERVAL_S)
        try:
            await ws.send(json.dumps({"method": "ping"}))
        except Exception:
            break


# ─────────────────────────────────────────────────────────────────────────────
# Message dispatch
# ─────────────────────────────────────────────────────────────────────────────

async def _handle_message(state: HyperliquidState, raw: str):
    try:
        msg = json.loads(raw)
    except Exception:
        return

    channel = msg.get("channel", "")
    data    = msg.get("data", {})

    if channel == "allMids":
        mids = data.get("mids", {}) if isinstance(data, dict) else data
        if isinstance(mids, dict):
            patch_from_all_mids(state, mids)

    elif channel == "activeAssetCtx":
        coin = data.get("coin", "")
        ctx  = data.get("ctx", {})
        if coin and ctx:
            patch_from_active_asset_ctx(state, coin, ctx)

    elif channel == "bbo":
        # data = {"coin": "BTC", "data": {...}} or {"coin": "BTC", "bid": [...], ...}
        if isinstance(data, dict):
            coin = data.get("coin", "")
            inner = data.get("data", data)
            if coin and inner:
                patch_from_bbo(state, coin, inner)

    elif channel == "l2Book":
        coin   = data.get("coin", "")
        levels = data.get("levels", [])
        if coin and levels:
            patch_from_l2(state, coin, levels)

    elif channel == "trades":
        # data = list of trade dicts
        trades = data if isinstance(data, list) else []
        affected: set[str] = set()
        for trade in trades:
            coin = trade.get("coin", "")
            if coin:
                state.add_trades(coin, [trade])
                affected.add(coin)
        for coin in affected:
            patch_trade_flow(state, coin)

    elif channel == "candle":
        # data = {coin, interval, candle_data}
        if isinstance(data, dict):
            coin     = data.get("coin", "") or data.get("s", "")
            interval = data.get("interval", "") or data.get("i", "")
            candle   = data.get("data", data)
            if coin and interval and candle:
                state.upsert_candle(coin, interval, candle)

    elif channel in ("pong", "subscriptionResponse"):
        pass   # ignore heartbeats and ack messages

    # else: unknown channel — silently ignore


# ─────────────────────────────────────────────────────────────────────────────
# Periodic background tasks
# ─────────────────────────────────────────────────────────────────────────────

async def _periodic_candle_refresh(state: HyperliquidState, client: HyperliquidRestClient):
    """
    Every 5 minutes: refresh 1h and 5m candles for the full top-40 universe.
    This keeps volatility and momentum features fresh even for assets without
    WS candle subscriptions.
    """
    while not _shutdown:
        await asyncio.sleep(300)
        if not state.assets:
            continue
        try:
            top40 = state.top_coins_by_volume(40)
            candles = await client.get_candles_multi(top40, "1h", n_bars=50)
            for coin, bars in candles.items():
                if bars:
                    state.add_candles(coin, "1h", bars)

            top20 = top40[:20]
            candles5 = await client.get_candles_multi(top20, "5m", n_bars=50)
            for coin, bars in candles5.items():
                if bars:
                    state.add_candles(coin, "5m", bars)
        except Exception as e:
            print(f"[HL][candle_refresh] Error: {e}")


async def _periodic_feature_recompute(state: HyperliquidState):
    """
    Every 60 seconds: save OI snapshots, compute OI changes, recompute all features.
    This ensures composite scores, percentile ranks, and flags stay current
    even for assets that haven't received a live WS update recently.
    """
    while not _shutdown:
        await asyncio.sleep(60)
        if not state.is_ready:
            continue
        try:
            _save_oi_snapshots(state)
            _compute_oi_changes(state)
            run_full_feature_pass(state)
        except Exception as e:
            print(f"[HL][feature_recompute] Error: {e}")


def _save_oi_snapshots(state: HyperliquidState):
    """Record current OI for all perp assets for change computation."""
    now = time.time()
    for asset in state.perp_assets():
        if asset.open_interest_usd:
            state.oi_history[asset.coin].append((now, asset.open_interest_usd))


def _compute_oi_changes(state: HyperliquidState):
    """Compute 5m and 1h OI changes from stored history and patch assets."""
    now = time.time()
    for coin, history in state.oi_history.items():
        asset = state.get_asset(coin)
        if asset is None:
            continue
        snaps = list(history)
        if not snaps:
            continue
        current_oi = asset.open_interest_usd
        if not current_oi:
            continue

        # Find snapshot closest to 5 min ago (270–360 s)
        snap_5m = next(
            (s for s in reversed(snaps) if 270 <= (now - s[0]) <= 600),
            None
        )
        # Find snapshot closest to 1 hour ago (3300–4500 s)
        snap_1h = next(
            (s for s in snaps if 3300 <= (now - s[0]) <= 4500),
            None
        )

        def pct(new, old):
            return round((new - old) / old, 6) if old and old != 0 else None

        oi_5m = pct(current_oi, snap_5m[1]) if snap_5m else None
        oi_1h = pct(current_oi, snap_1h[1]) if snap_1h else None

        if oi_5m is not None or oi_1h is not None:
            state.assets[coin] = asset.model_copy(update={
                "oi_change_5m": oi_5m,
                "oi_change_1h": oi_1h,
            })
