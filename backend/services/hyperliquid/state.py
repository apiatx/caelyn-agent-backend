"""
Hyperliquid Screener — in-memory market state.

HyperliquidState is the single source of truth for all live screener data.
It is updated incrementally by the WebSocket manager and read by the router.
"""
from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from typing import Optional

from .models import ScreenerAsset

# Max trades kept per asset in the rolling window (covers ~5–30 min at avg pace)
_TRADE_WINDOW = 500
# Max candle bars stored per (coin, interval) in memory
_MAX_CANDLES = 200


class HyperliquidState:
    """
    Thread-safe in-memory store for the full Hyperliquid screener universe.

    Layout:
      assets        — canonical ScreenerAsset map keyed by coin name
      meta          — raw universe metadata from Hyperliquid
      candles       — {coin: {interval: deque[candle_dict]}}
      trades        — {coin: deque[trade_dict]}   rolling window
      books         — {coin: {"levels": [[bids], [asks]]}}
      prev_ranks    — previous rank ordering for rank_change computation
      boot_ts       — unix timestamp of last successful boot
      ws_connected  — True while the WS consumer is alive
    """

    def __init__(self):
        self._lock = asyncio.Lock()

        # Core screener rows
        self.assets: dict[str, ScreenerAsset] = {}

        # Raw universe metadata {coin: {szDecimals, maxLeverage, ...}}
        self.meta: dict[str, dict] = {}

        # Candle cache: coin → interval → deque of candle dicts
        self.candles: dict[str, dict[str, deque]] = defaultdict(
            lambda: defaultdict(lambda: deque(maxlen=_MAX_CANDLES))
        )

        # Rolling recent trades per coin
        self.trades: dict[str, deque] = defaultdict(lambda: deque(maxlen=_TRADE_WINDOW))

        # Latest L2 book snapshot per coin
        self.books: dict[str, dict] = {}

        # Previous ranking order {coin: rank_int}
        self.prev_ranks: dict[str, int] = {}

        # OI history for change computation: coin → deque[(ts_unix, oi_usd)]
        # Stored at ~60s intervals; 130 entries ≈ 2h+ of history
        self.oi_history: dict[str, deque] = defaultdict(lambda: deque(maxlen=130))

        # Score history for score_change computation: coin → deque[(ts_unix, composite_score)]
        # 60 entries ≈ 1h of snapshots
        self.score_history: dict[str, deque] = defaultdict(lambda: deque(maxlen=60))

        # Volume impulse history from 5m candles: coin → deque[(ts_unix, volume_5m_bar)]
        # Used to compute volume_impulse_5m and volume_impulse_15m
        self.volume_5m_history: dict[str, deque] = defaultdict(lambda: deque(maxlen=30))

        # Timing
        self.boot_ts: Optional[float] = None
        self.last_mids_ts: Optional[float] = None
        self.last_ctx_ts: Optional[float] = None

        # Connectivity
        self.ws_connected: bool = False
        self.is_ready: bool = False     # True after boot sequence completes

    # ── Thread-safe accessors ─────────────────────────────────────────────

    async def set_asset(self, coin: str, asset: ScreenerAsset):
        async with self._lock:
            self.assets[coin] = asset

    async def update_asset_fields(self, coin: str, **fields):
        """Patch individual fields on an existing asset without replacing it."""
        async with self._lock:
            if coin in self.assets:
                current = self.assets[coin].model_dump()
                current.update(fields)
                current["last_updated_ts"] = time.time()
                self.assets[coin] = ScreenerAsset(**current)

    def get_asset(self, coin: str) -> Optional[ScreenerAsset]:
        return self.assets.get(coin)

    def all_assets(self) -> list[ScreenerAsset]:
        """Return a stable snapshot of all current assets."""
        return list(self.assets.values())

    def perp_assets(self) -> list[ScreenerAsset]:
        return [a for a in self.assets.values() if a.market_type == "perp"]

    def spot_assets(self) -> list[ScreenerAsset]:
        return [a for a in self.assets.values() if a.market_type == "spot"]

    # ── Candle helpers ────────────────────────────────────────────────────

    def add_candles(self, coin: str, interval: str, candles: list[dict]):
        """Bulk-add candles, deduplicating by open timestamp."""
        dq = self.candles[coin][interval]
        existing_ts = {c["t"] for c in dq}
        for c in sorted(candles, key=lambda x: x.get("t", 0)):
            if c.get("t") not in existing_ts:
                dq.append(c)
                existing_ts.add(c["t"])

    def upsert_candle(self, coin: str, interval: str, candle: dict):
        """Insert or update the most recent candle (live update)."""
        dq = self.candles[coin][interval]
        t = candle.get("t")
        if dq and dq[-1].get("t") == t:
            dq[-1] = candle   # update in-place (candle still forming)
        else:
            dq.append(candle)

    def get_candles(self, coin: str, interval: str, n: int = 50) -> list[dict]:
        """Return the most recent n candles for a coin/interval."""
        dq = self.candles[coin][interval]
        bars = list(dq)
        return bars[-n:] if len(bars) > n else bars

    # ── Trade helpers ─────────────────────────────────────────────────────

    def add_trades(self, coin: str, trades: list[dict]):
        dq = self.trades[coin]
        for t in trades:
            dq.append(t)

    def get_recent_trades(self, coin: str, max_age_s: float = 300.0) -> list[dict]:
        """Return trades from the last max_age_s seconds."""
        cutoff = (time.time() - max_age_s) * 1000   # ms
        return [t for t in self.trades[coin] if t.get("time", 0) >= cutoff]

    # ── Book helpers ──────────────────────────────────────────────────────

    def set_book(self, coin: str, book: dict):
        self.books[coin] = book

    def get_book(self, coin: str) -> Optional[dict]:
        return self.books.get(coin)

    # ── Metadata ─────────────────────────────────────────────────────────

    def freshness_seconds(self) -> Optional[float]:
        if self.boot_ts is None:
            return None
        latest = max(
            t for t in [self.last_mids_ts, self.last_ctx_ts, self.boot_ts]
            if t is not None
        )
        return time.time() - latest

    def top_coins_by_volume(self, n: int = 40) -> list[str]:
        """Return top-N coins sorted by 24h notional volume."""
        perps = [(a.coin, a.day_ntl_vlm or 0) for a in self.assets.values() if a.market_type == "perp"]
        perps.sort(key=lambda x: -x[1])
        return [coin for coin, _ in perps[:n]]

    def top_coins_by_oi(self, n: int = 40) -> list[str]:
        perps = [(a.coin, a.open_interest_usd or 0) for a in self.assets.values() if a.market_type == "perp"]
        perps.sort(key=lambda x: -x[1])
        return [coin for coin, _ in perps[:n]]
