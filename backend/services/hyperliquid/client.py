"""
Hyperliquid REST client.

All Hyperliquid market data endpoints are accessed via a single POST to
https://api.hyperliquid.xyz/info with a typed JSON payload.
No authentication is required for public market data.
"""
from __future__ import annotations

import time
from typing import Any

import httpx

_INFO_URL = "https://api.hyperliquid.xyz/info"
_TIMEOUT = 20.0

# Milliseconds per candle interval
_INTERVAL_MS: dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}


class HyperliquidRestClient:
    """
    Async REST client for Hyperliquid's Info API.
    Uses a single shared httpx.AsyncClient for connection pooling.
    """

    def __init__(self):
        self._http = httpx.AsyncClient(
            timeout=_TIMEOUT,
            headers={"Content-Type": "application/json"},
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )

    # ── Core transport ────────────────────────────────────────────────────

    async def _post(self, payload: dict) -> Any:
        resp = await self._http.post(_INFO_URL, json=payload)
        resp.raise_for_status()
        return resp.json()

    # ── Universe / metadata ───────────────────────────────────────────────

    async def get_meta(self) -> dict:
        """Perpetual universe metadata: list of asset definitions."""
        return await self._post({"type": "meta"})

    async def get_meta_and_asset_ctxs(self) -> list:
        """
        Returns [meta, list[AssetCtx]] for all perpetual assets.
        AssetCtx fields: funding, openInterest, prevDayPx, dayNtlVlm,
        premium, oraclePx, markPx, midPx, impactPxs, dayBaseVlm.
        """
        return await self._post({"type": "metaAndAssetCtxs"})

    async def get_all_mids(self) -> dict[str, str]:
        """All current mid prices keyed by coin name."""
        return await self._post({"type": "allMids"})

    async def get_spot_meta_and_asset_ctxs(self) -> list:
        """
        Returns [spotMeta, list[SpotAssetCtx]].
        spotMeta: {universe: [{name, tokens, index, isCanonical}], tokens: [...]}
        SpotAssetCtx: {dayNtlVlm, markPx, prevDayPx, circulatingSupply, coin}
        """
        return await self._post({"type": "spotMetaAndAssetCtxs"})

    # ── Candle data ───────────────────────────────────────────────────────

    async def get_candle_snapshot(
        self,
        coin: str,
        interval: str,
        n_bars: int = 50,
    ) -> list[dict]:
        """
        Fetch the most recent n_bars candles for a coin.

        Each candle dict has: t (open ms), T (close ms), s (symbol),
        i (interval), o, h, l, c (OHLC prices), v (base volume), n (trades).
        """
        bar_ms = _INTERVAL_MS.get(interval, 3_600_000)
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - bar_ms * (n_bars + 2)   # +2 buffer for boundary bars
        try:
            data = await self._post({
                "type": "candleSnapshot",
                "req": {
                    "coin": coin,
                    "interval": interval,
                    "startTime": start_ms,
                    "endTime": end_ms,
                },
            })
            return data if isinstance(data, list) else []
        except Exception:
            return []

    # ── Order book ────────────────────────────────────────────────────────

    async def get_l2_book(self, coin: str) -> dict:
        """
        Current L2 order book snapshot.
        Returns {coin, levels: [[bid_levels], [ask_levels]]}
        where each level is {px, sz, n}.
        """
        try:
            return await self._post({"type": "l2Book", "coin": coin})
        except Exception:
            return {}

    # ── Bulk helpers ─────────────────────────────────────────────────────

    async def get_candles_multi(
        self,
        coins: list[str],
        interval: str,
        n_bars: int = 50,
    ) -> dict[str, list[dict]]:
        """
        Fetch candles for multiple coins concurrently.
        Returns {coin: [candle, ...]} — missing coins return empty list.
        """
        import asyncio
        results = await asyncio.gather(
            *[self.get_candle_snapshot(c, interval, n_bars) for c in coins],
            return_exceptions=True,
        )
        return {
            coin: (res if isinstance(res, list) else [])
            for coin, res in zip(coins, results)
        }

    async def get_l2_books_multi(self, coins: list[str]) -> dict[str, dict]:
        """Fetch L2 books for multiple coins concurrently."""
        import asyncio
        results = await asyncio.gather(
            *[self.get_l2_book(c) for c in coins],
            return_exceptions=True,
        )
        return {
            coin: (res if isinstance(res, dict) else {})
            for coin, res in zip(coins, results)
        }

    async def close(self):
        await self._http.aclose()
