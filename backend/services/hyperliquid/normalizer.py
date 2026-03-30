"""
Hyperliquid Screener — data normalizer.

Converts raw Hyperliquid API responses into typed ScreenerAsset objects
and applies incremental patches from WebSocket updates.
"""
from __future__ import annotations

import time
from typing import Any, Optional

from .models import ScreenerAsset
from .state import HyperliquidState


def _f(v: Any) -> Optional[float]:
    """Safe float conversion — returns None for missing/null/empty."""
    try:
        if v is None or v == "" or v == "null":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap normalizers  (called once on boot from REST snapshots)
# ─────────────────────────────────────────────────────────────────────────────

def build_perp_universe(meta_and_ctxs: list) -> dict[str, ScreenerAsset]:
    """
    Convert the full metaAndAssetCtxs response into a dict of ScreenerAsset.

    meta_and_ctxs = [
      {"universe": [{name, szDecimals, maxLeverage, onlyIsolated, ...}, ...]},
      [{funding, openInterest, prevDayPx, dayNtlVlm, premium, oraclePx, markPx, midPx, impactPxs, dayBaseVlm}, ...]
    ]
    """
    if not meta_and_ctxs or len(meta_and_ctxs) < 2:
        return {}

    meta_block = meta_and_ctxs[0]
    ctx_list   = meta_and_ctxs[1]
    universe   = meta_block.get("universe", [])

    assets: dict[str, ScreenerAsset] = {}
    for idx, asset_meta in enumerate(universe):
        coin = asset_meta.get("name", "")
        if not coin:
            continue
        ctx = ctx_list[idx] if idx < len(ctx_list) else {}
        assets[coin] = _build_perp_asset(coin, asset_meta, ctx)

    return assets


def _build_perp_asset(coin: str, meta: dict, ctx: dict) -> ScreenerAsset:
    mark  = _f(ctx.get("markPx"))
    oracle = _f(ctx.get("oraclePx"))
    mid   = _f(ctx.get("midPx"))
    prev  = _f(ctx.get("prevDayPx"))
    fund  = _f(ctx.get("funding"))
    prem  = _f(ctx.get("premium"))
    oi    = _f(ctx.get("openInterest"))
    ntlvlm = _f(ctx.get("dayNtlVlm"))
    basevlm = _f(ctx.get("dayBaseVlm"))

    # Impact prices: [impactBid, impactAsk] (fill price for $5k notional)
    impact = ctx.get("impactPxs") or []
    impact_bid = _f(impact[0]) if len(impact) > 0 else None
    impact_ask = _f(impact[1]) if len(impact) > 1 else None

    # Derived
    pct_24h = None
    if mark is not None and prev and prev != 0:
        pct_24h = (mark - prev) / prev * 100

    oi_usd = None
    if oi is not None and mark is not None:
        oi_usd = oi * mark

    distance_mark_oracle = None
    if mark is not None and oracle and oracle != 0:
        distance_mark_oracle = (mark - oracle) / oracle * 100

    distance_mark_mid = None
    if mark is not None and mid and mid != 0:
        distance_mark_mid = (mark - mid) / mid * 100

    distance_mark_prev = None
    if mark is not None and prev and prev != 0:
        distance_mark_prev = (mark - prev) / prev * 100

    # Tags
    tags = _perp_tags(coin, meta)

    return ScreenerAsset(
        coin=coin,
        display_name=coin,
        market_type="perp",
        dex="hyperliquid",
        tags=tags,

        mark_px=mark,
        mid_px=mid,
        oracle_px=oracle,
        prev_day_px=prev,
        pct_change_24h=round(pct_24h, 4) if pct_24h is not None else None,

        funding=fund,
        premium=prem,

        open_interest=oi,
        open_interest_usd=round(oi_usd, 2) if oi_usd is not None else None,
        day_ntl_vlm=ntlvlm,
        day_base_vlm=basevlm,

        impact_bid_px=impact_bid,
        impact_ask_px=impact_ask,

        distance_mark_oracle_pct=round(distance_mark_oracle, 4) if distance_mark_oracle is not None else None,
        distance_mark_mid_pct=round(distance_mark_mid, 4) if distance_mark_mid is not None else None,
        distance_mark_prev_day_pct=round(distance_mark_prev, 4) if distance_mark_prev is not None else None,

        # Spread will be computed once we have BBO
        bid_px=None,
        ask_px=None,
        spread_abs=None,
        spread_bps=None,

        max_leverage=meta.get("maxLeverage"),
        only_isolated=meta.get("onlyIsolated", False),
        sz_decimals=meta.get("szDecimals", 0),
        market_status="delisted" if meta.get("isDelisted") else "active",

        momentum_24h=round(pct_24h, 4) if pct_24h is not None else None,

        last_updated_ts=time.time(),
    )


def build_spot_universe(spot_meta_and_ctxs: list) -> dict[str, ScreenerAsset]:
    """
    Convert spotMetaAndAssetCtxs into ScreenerAsset objects.
    Spot markets have fewer fields (no funding, no OI, no impact prices).
    """
    if not spot_meta_and_ctxs or len(spot_meta_and_ctxs) < 2:
        return {}

    meta_block = spot_meta_and_ctxs[0]
    ctx_list   = spot_meta_and_ctxs[1]
    universe   = meta_block.get("universe", [])

    assets: dict[str, ScreenerAsset] = {}
    for idx, market in enumerate(universe):
        coin = market.get("name", "")
        if not coin:
            continue
        ctx = ctx_list[idx] if idx < len(ctx_list) else {}
        mark  = _f(ctx.get("markPx"))
        prev  = _f(ctx.get("prevDayPx"))
        ntlvlm = _f(ctx.get("dayNtlVlm"))
        pct_24h = None
        if mark and prev and prev != 0:
            pct_24h = (mark - prev) / prev * 100

        assets[coin] = ScreenerAsset(
            coin=coin,
            display_name=coin,
            market_type="spot",
            dex="hyperliquid",
            tags=["spot"],
            mark_px=mark,
            prev_day_px=prev,
            pct_change_24h=round(pct_24h, 4) if pct_24h is not None else None,
            day_ntl_vlm=ntlvlm,
            momentum_24h=round(pct_24h, 4) if pct_24h is not None else None,
            last_updated_ts=time.time(),
        )
    return assets


# ─────────────────────────────────────────────────────────────────────────────
# Incremental WebSocket update normalizers
# ─────────────────────────────────────────────────────────────────────────────

def patch_from_all_mids(state: HyperliquidState, mids: dict[str, str]):
    """
    allMids WS update → patch mid_px on all assets.
    Also recompute distance_mark_mid_pct when both mark and mid are known.
    """
    now = time.time()
    for coin, px_str in mids.items():
        mid = _f(px_str)
        if mid is None:
            continue
        asset = state.get_asset(coin)
        if asset is None:
            continue
        mark = asset.mark_px
        dist = (mark - mid) / mid * 100 if (mark and mid) else None
        # Direct dict patch (avoids re-parsing the whole Pydantic model for speed)
        state.assets[coin] = asset.model_copy(update={
            "mid_px": mid,
            "distance_mark_mid_pct": round(dist, 4) if dist is not None else None,
            "last_updated_ts": now,
        })
    state.last_mids_ts = now


def patch_from_active_asset_ctx(state: HyperliquidState, coin: str, ctx: dict):
    """
    activeAssetCtx WS update → full ctx refresh for one coin.
    This carries funding, OI, mark, oracle, mid, volume — patch everything.
    """
    asset = state.get_asset(coin)
    if asset is None:
        return

    mark   = _f(ctx.get("markPx"))
    oracle = _f(ctx.get("oraclePx"))
    mid    = _f(ctx.get("midPx"))
    prev   = _f(ctx.get("prevDayPx"))
    fund   = _f(ctx.get("funding"))
    prem   = _f(ctx.get("premium"))
    oi     = _f(ctx.get("openInterest"))
    ntlvlm = _f(ctx.get("dayNtlVlm"))
    basevlm = _f(ctx.get("dayBaseVlm"))

    # If the ctx message only has partial fields, keep existing values
    mark   = mark   if mark   is not None else asset.mark_px
    oracle = oracle if oracle is not None else asset.oracle_px
    mid    = mid    if mid    is not None else asset.mid_px
    prev   = prev   if prev   is not None else asset.prev_day_px
    fund   = fund   if fund   is not None else asset.funding
    prem   = prem   if prem   is not None else asset.premium
    oi     = oi     if oi     is not None else asset.open_interest
    ntlvlm = ntlvlm if ntlvlm is not None else asset.day_ntl_vlm

    pct_24h = (mark - prev) / prev * 100 if (mark and prev and prev != 0) else asset.pct_change_24h
    oi_usd  = oi * mark if (oi and mark) else asset.open_interest_usd
    dist_mo = (mark - oracle) / oracle * 100 if (mark and oracle and oracle != 0) else asset.distance_mark_oracle_pct
    dist_mm = (mark - mid) / mid * 100 if (mark and mid and mid != 0) else asset.distance_mark_mid_pct
    dist_mp = (mark - prev) / prev * 100 if (mark and prev and prev != 0) else asset.distance_mark_prev_day_pct

    state.assets[coin] = asset.model_copy(update={
        "mark_px": mark,
        "oracle_px": oracle,
        "mid_px": mid,
        "prev_day_px": prev,
        "funding": fund,
        "premium": prem,
        "open_interest": oi,
        "open_interest_usd": round(oi_usd, 2) if oi_usd else None,
        "day_ntl_vlm": ntlvlm,
        "day_base_vlm": basevlm if basevlm is not None else asset.day_base_vlm,
        "pct_change_24h": round(pct_24h, 4) if pct_24h is not None else None,
        "momentum_24h": round(pct_24h, 4) if pct_24h is not None else None,
        "distance_mark_oracle_pct": round(dist_mo, 4) if dist_mo is not None else None,
        "distance_mark_mid_pct": round(dist_mm, 4) if dist_mm is not None else None,
        "distance_mark_prev_day_pct": round(dist_mp, 4) if dist_mp is not None else None,
        "last_updated_ts": time.time(),
    })
    state.last_ctx_ts = time.time()


def patch_from_bbo(state: HyperliquidState, coin: str, bbo_data: dict):
    """
    BBO update → bid_px, ask_px, spread.
    bbo_data may be {"bid": [{px, sz}], "ask": [{px, sz}], "ts": ...}
    or just the data sub-field from the WS message.
    """
    asset = state.get_asset(coin)
    if asset is None:
        return

    bids = bbo_data.get("bid") or bbo_data.get("bids") or []
    asks = bbo_data.get("ask") or bbo_data.get("asks") or []

    bid_px = _f(bids[0].get("px") if bids and isinstance(bids[0], dict) else (bids[0][0] if bids else None))
    ask_px = _f(asks[0].get("px") if asks and isinstance(asks[0], dict) else (asks[0][0] if asks else None))

    spread_abs = (ask_px - bid_px) if (bid_px and ask_px) else None
    mid = (bid_px + ask_px) / 2 if (bid_px and ask_px) else None
    spread_bps = spread_abs / mid * 10_000 if (spread_abs and mid and mid != 0) else None

    state.assets[coin] = asset.model_copy(update={
        "bid_px": bid_px,
        "ask_px": ask_px,
        "spread_abs": round(spread_abs, 6) if spread_abs is not None else None,
        "spread_bps": round(spread_bps, 2) if spread_bps is not None else None,
        "mid_px": round(mid, 6) if mid is not None else asset.mid_px,
        "last_updated_ts": time.time(),
    })


def patch_from_l2(state: HyperliquidState, coin: str, levels: list):
    """
    L2 update → update book depth, imbalance, and best bid/ask.
    levels = [[bid_level, ...], [ask_level, ...]]
    Each level = {px: str, sz: str, n: int} or [px, sz, n].
    """
    if not levels or len(levels) < 2:
        return

    def _level_px_sz(lvl):
        if isinstance(lvl, dict):
            return _f(lvl.get("px")), _f(lvl.get("sz"))
        elif isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
            return _f(lvl[0]), _f(lvl[1])
        return None, None

    def _sum_depth(level_list, top_n=10) -> float:
        total = 0.0
        for lvl in level_list[:top_n]:
            px, sz = _level_px_sz(lvl)
            if px and sz:
                total += px * sz
        return total

    bid_depth = _sum_depth(levels[0], top_n=10)
    ask_depth = _sum_depth(levels[1], top_n=10)
    total = bid_depth + ask_depth
    imbalance = (bid_depth - ask_depth) / total if total > 0 else 0.0

    # Extract best bid/ask from top level
    best_bid = _level_px_sz(levels[0][0])[0] if levels[0] else None
    best_ask = _level_px_sz(levels[1][0])[0] if levels[1] else None
    spread_abs = (best_ask - best_bid) if (best_bid and best_ask) else None
    mid = (best_bid + best_ask) / 2 if (best_bid and best_ask) else None
    spread_bps = spread_abs / mid * 10_000 if (spread_abs and mid and mid != 0) else None

    asset = state.get_asset(coin)
    if asset is None:
        return

    state.assets[coin] = asset.model_copy(update={
        "orderbook_bid_depth": round(bid_depth, 2),
        "orderbook_ask_depth": round(ask_depth, 2),
        "orderbook_imbalance": round(imbalance, 4),
        # Populate BBO from L2 if not already set via BBO subscription
        "bid_px": best_bid if (asset.bid_px is None and best_bid) else asset.bid_px,
        "ask_px": best_ask if (asset.ask_px is None and best_ask) else asset.ask_px,
        "spread_abs": round(spread_abs, 6) if (asset.spread_abs is None and spread_abs is not None) else asset.spread_abs,
        "spread_bps": round(spread_bps, 2) if (asset.spread_bps is None and spread_bps is not None) else asset.spread_bps,
        "last_updated_ts": time.time(),
    })

    # Also store raw book
    state.set_book(coin, {"levels": levels})


def patch_trade_flow(state: HyperliquidState, coin: str, max_age_s: float = 300.0):
    """
    Recompute trade flow aggregates from the rolling trade window.
    Called after new trades are added to state.
    """
    asset = state.get_asset(coin)
    if asset is None:
        return

    recent = state.get_recent_trades(coin, max_age_s)
    count = len(recent)
    buy_vol = sum(_f(t.get("sz")) or 0 for t in recent if t.get("side") in ("B", "buy"))
    sell_vol = sum(_f(t.get("sz")) or 0 for t in recent if t.get("side") in ("A", "sell"))
    total = buy_vol + sell_vol
    imbalance = (buy_vol - sell_vol) / total if total > 0 else 0.0

    state.assets[coin] = asset.model_copy(update={
        "recent_trade_count": count,
        "recent_trade_buy_volume": round(buy_vol, 4),
        "recent_trade_sell_volume": round(sell_vol, 4),
        "recent_trade_imbalance": round(imbalance, 4),
        "last_updated_ts": time.time(),
    })


# ─────────────────────────────────────────────────────────────────────────────
# Tag classification
# ─────────────────────────────────────────────────────────────────────────────

_LAYER1 = {"BTC", "ETH", "SOL", "AVAX", "NEAR", "DOT", "ADA", "ATOM", "SUI", "APT", "INJ", "TIA"}
_DEFI   = {"UNI", "AAVE", "CRV", "MKR", "SNX", "COMP", "YFI", "GMX", "DYDX", "JUP", "PENDLE"}
_AI     = {"FET", "AGIX", "RNDR", "WLD", "TAO", "IO", "ARKM", "VIRTUAL"}
_MEME   = {"DOGE", "SHIB", "PEPE", "WIF", "BONK", "FLOKI", "MOG", "BRETT", "TURBO", "NEIRO"}
_GAMING = {"AXS", "SAND", "MANA", "GALA", "IMX", "BEAM", "RON"}
_RWA    = {"ONDO", "CANTO", "CFG", "TRU", "MPL"}

def _perp_tags(coin: str, meta: dict) -> list[str]:
    tags = ["perp"]
    c = coin.upper()
    if meta.get("onlyIsolated"):
        tags.append("isolated-only")
    if meta.get("isDelisted"):
        tags.append("delisted")
    if c in _LAYER1:  tags.append("L1")
    if c in _DEFI:    tags.append("DeFi")
    if c in _AI:      tags.append("AI")
    if c in _MEME:    tags.append("meme")
    if c in _GAMING:  tags.append("gaming")
    if c in _RWA:     tags.append("RWA")
    return tags
