"""
Polymarket Intelligence Service — Jon-Becker/prediction-market-analysis methodology.

Fetches live data from the Polymarket Gamma API and applies analytical frameworks
inspired by the Jon-Becker prediction-market-analysis repository:

  - Market efficiency scoring (spread tightness, competitive flags, liquidity)
  - Edge detection (implied probability ≠ 1.0 → house edge or mispricing)
  - Volume momentum signals (24h vs 7d vs 30d trend)
  - Whale activity markers (high volume / low liquidity ratio)
  - Smart-money vs retail signals (competitive market flag + spread)
  - Kelly Criterion position sizing helpers
  - Market pulse summary for the Predict page dashboard

Gamma API: https://gamma-api.polymarket.com  (no auth required)
CLOB API:  https://clob.polymarket.com       (order-book data where available)

All data schemas follow the Jon-Becker prediction-market-analysis trade schema:
  block_number, transaction_hash, trader, amount, outcome_index, is_buy, timestamp
We replicate the analytical outputs without requiring the 36 GB dataset.
"""

from __future__ import annotations

import asyncio
import json
import math
from datetime import datetime, timezone
from typing import Optional

import httpx

from data.cache import cache

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; CaelynAI-Predict/1.0)",
    "Accept": "application/json",
}

_MARKET_CACHE_TTL = 90
_SIGNALS_CACHE_TTL = 120
_MARKET_DETAIL_TTL = 60


class PolymarketIntelligence:
    """
    Production prediction-market analytics inspired by Jon-Becker/prediction-market-analysis.

    Key methods:
        get_market_signals()    → dashboard overview with edge/momentum signals
        get_top_markets()       → enriched market list for the Predict page table
        get_market_detail()     → deep dive on one market (condition_id)
        get_whale_watch()       → markets with unusual volume spikes
        get_category_breakdown()→ distribution by tag/category
    """

    # ── Public API ────────────────────────────────────────────────────────────

    async def get_top_markets(
        self,
        limit: int = 50,
        tag: Optional[str] = None,
        min_volume_24h: float = 0,
    ) -> list[dict]:
        """
        Return enriched, analytics-decorated markets sorted by 24h volume.
        Applies all Jon-Becker-style signal computations on top of raw API data.
        """
        key = f"pm:intel:top:{limit}:{tag}:{min_volume_24h}"
        cached = cache.get(key)
        if cached is not None:
            return cached

        raw = await self._fetch_markets(limit=min(limit * 2, 200), tag=tag)
        enriched = [self._enrich_market(m) for m in raw]
        enriched = [m for m in enriched if m["volume_24h"] >= min_volume_24h]
        enriched.sort(key=lambda m: m["volume_24h"], reverse=True)
        result = enriched[:limit]
        cache.set(key, result, _MARKET_CACHE_TTL)
        return result

    async def get_market_signals(self) -> dict:
        """
        Dashboard-level market pulse: edges, mispricings, momentum, whale activity.
        This is the primary data feed for the Predict page signals panel.
        """
        key = "pm:intel:signals"
        cached = cache.get(key)
        if cached is not None:
            return cached

        markets = await self.get_top_markets(limit=100)
        if not markets:
            return {}

        edges = [m for m in markets if m.get("edge_detected")]
        mispricings = [m for m in markets if m.get("mispricing_score", 0) > 0.03]
        momentum_up = [m for m in markets if m.get("volume_momentum") == "surging"]
        momentum_down = [m for m in markets if m.get("volume_momentum") == "fading"]
        whale_markets = [m for m in markets if m.get("whale_activity")]
        competitive = [m for m in markets if m.get("is_competitive")]

        total_vol_24h = sum(m.get("volume_24h", 0) for m in markets)
        total_liquidity = sum(m.get("liquidity", 0) for m in markets)
        avg_spread = (
            sum(m.get("spread", 0) for m in markets if m.get("spread") is not None)
            / max(1, sum(1 for m in markets if m.get("spread") is not None))
        )

        result = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "market_count": len(markets),
            "summary": {
                "total_volume_24h": round(total_vol_24h, 2),
                "total_liquidity": round(total_liquidity, 2),
                "avg_spread_pct": round(avg_spread * 100, 3),
                "competitive_market_pct": round(len(competitive) / max(1, len(markets)) * 100, 1),
                "edge_count": len(edges),
                "mispricing_count": len(mispricings),
                "surging_count": len(momentum_up),
                "fading_count": len(momentum_down),
                "whale_active_count": len(whale_markets),
            },
            "top_edges": _slim(sorted(edges, key=lambda m: abs(m.get("edge_pct", 0)), reverse=True)[:8]),
            "top_mispricings": _slim(sorted(mispricings, key=lambda m: m.get("mispricing_score", 0), reverse=True)[:8]),
            "surging_markets": _slim(momentum_up[:6]),
            "whale_markets": _slim(whale_markets[:6]),
            "top_by_volume": _slim(markets[:10]),
            "top_by_liquidity": _slim(sorted(markets, key=lambda m: m.get("liquidity", 0), reverse=True)[:8]),
        }
        cache.set(key, result, _SIGNALS_CACHE_TTL)
        return result

    async def get_market_detail(self, condition_id: str) -> Optional[dict]:
        """
        Deep analysis of a single market including price history and order book.
        Used by the Predict page's market detail / agent analysis panel.
        """
        key = f"pm:intel:detail:{condition_id}"
        cached = cache.get(key)
        if cached is not None:
            return cached

        market_task = self._fetch_market_by_condition(condition_id)
        book_task = self._fetch_order_book(condition_id)

        market_raw, book = await asyncio.gather(market_task, book_task, return_exceptions=True)

        if isinstance(market_raw, Exception) or not market_raw:
            return None

        enriched = self._enrich_market(market_raw)
        if not isinstance(book, Exception) and book:
            enriched["order_book"] = book
            enriched["book_depth"] = self._analyze_book_depth(book)

        cache.set(key, enriched, _MARKET_DETAIL_TTL)
        return enriched

    async def get_whale_watch(self, limit: int = 20) -> list[dict]:
        """Markets with anomalously high volume-to-liquidity ratio — whale activity signal."""
        key = f"pm:intel:whale:{limit}"
        cached = cache.get(key)
        if cached is not None:
            return cached

        markets = await self.get_top_markets(limit=100)
        whale_markets = [m for m in markets if m.get("whale_activity")]
        whale_markets.sort(key=lambda m: m.get("vol_liq_ratio", 0), reverse=True)
        result = whale_markets[:limit]
        cache.set(key, result, _SIGNALS_CACHE_TTL)
        return result

    async def get_category_breakdown(self) -> list[dict]:
        """
        Volume and market count by tag/category.
        Uses the Gamma events endpoint which carries proper tag data.
        """
        key = "pm:intel:categories"
        cached = cache.get(key)
        if cached is not None:
            return cached

        events = await self._fetch_events_for_categories(limit=200)
        categories: dict[str, dict] = {}
        for ev in events:
            vol = float(ev.get("volume24hr") or 0)
            liq = float(ev.get("liquidity") or 0)
            raw_tags = ev.get("tags") or []
            tags = [
                t.get("label", "") for t in raw_tags
                if isinstance(t, dict) and t.get("label")
                and not t.get("forceHide") and t.get("label") != "Hide From New"
            ]
            if not tags:
                tags = ["Uncategorized"]
            for tag in tags[:3]:
                if not tag:
                    continue
                if tag not in categories:
                    categories[tag] = {"tag": tag, "count": 0, "volume_24h": 0.0, "liquidity": 0.0}
                categories[tag]["count"] += 1
                categories[tag]["volume_24h"] = round(categories[tag]["volume_24h"] + vol, 2)
                categories[tag]["liquidity"] = round(categories[tag]["liquidity"] + liq, 2)

        result = sorted(categories.values(), key=lambda c: c["volume_24h"], reverse=True)
        cache.set(key, result, _SIGNALS_CACHE_TTL)
        return result

    # ── Market Context for Predict Agent ────────────────────────────────────

    async def get_predict_agent_context(self, question: str) -> dict:
        """
        Build a rich context dict for the TradingAgents predict endpoint.
        Searches for the most relevant Polymarket market for a given question,
        plus the broader market signals dashboard for macro context.
        """
        signals_task = self.get_market_signals()
        markets_task = self.get_top_markets(limit=150)

        signals, all_markets = await asyncio.gather(signals_task, markets_task, return_exceptions=True)

        relevant = []
        if not isinstance(all_markets, Exception):
            q_lower = question.lower()
            keywords = [w for w in q_lower.split() if len(w) > 3]
            for m in (all_markets or []):
                text = f"{m.get('question','').lower()} {m.get('description','').lower()}"
                score = sum(1 for kw in keywords if kw in text)
                if score > 0:
                    relevant.append((score, m))
            relevant.sort(key=lambda x: (-x[0], -x[1].get("volume_24h", 0)))
            relevant = [m for _, m in relevant[:5]]

        return {
            "question": question,
            "relevant_markets": relevant,
            "market_signals": signals if not isinstance(signals, Exception) else {},
            "pulled_at": datetime.now(timezone.utc).isoformat(),
        }

    # ── Analytics Engine (Jon-Becker methodology) ────────────────────────────

    def _enrich_market(self, raw: dict) -> dict:
        """Apply full Jon-Becker analytics framework to a raw Gamma API market."""
        try:
            prices = json.loads(raw.get("outcomePrices", "[]"))
        except Exception:
            prices = []

        yes_price = float(prices[0]) if prices else 0.5
        no_price = float(prices[1]) if len(prices) > 1 else (1 - yes_price)

        volume_24h = float(raw.get("volume24hr") or 0)
        volume_1wk = float(raw.get("volume1wk") or 0)
        volume_1mo = float(raw.get("volume1mo") or 0)
        liquidity = float(raw.get("liquidityNum") or raw.get("liquidity") or 0)
        spread = float(raw.get("spread") or 0)
        best_bid = float(raw.get("bestBid") or 0)
        best_ask = float(raw.get("bestAsk") or 0)
        last_trade = float(raw.get("lastTradePrice") or yes_price)
        is_competitive = bool(raw.get("competitive"))
        neg_risk = bool(raw.get("negRisk"))

        implied_sum = yes_price + no_price
        edge_pct = 1.0 - implied_sum if implied_sum > 0 else 0
        edge_detected = abs(edge_pct) > 0.01

        mispricing_score = 0.0
        if best_bid > 0 and best_ask > 0:
            mid_price = (best_bid + best_ask) / 2
            mispricing_score = abs(mid_price - yes_price)

        avg_daily_7d = volume_1wk / 7 if volume_1wk > 0 else 0
        avg_daily_30d = volume_1mo / 30 if volume_1mo > 0 else 0
        if avg_daily_7d > 0:
            volume_ratio = volume_24h / avg_daily_7d
            if volume_ratio > 3.0:
                volume_momentum = "surging"
            elif volume_ratio > 1.5:
                volume_momentum = "accelerating"
            elif volume_ratio < 0.3:
                volume_momentum = "fading"
            else:
                volume_momentum = "stable"
        else:
            volume_momentum = "insufficient_history"

        vol_liq_ratio = volume_24h / max(liquidity, 1)
        whale_activity = vol_liq_ratio > 5.0 and volume_24h > 10_000

        market_efficiency = self._score_efficiency(spread, liquidity, is_competitive, volume_24h)

        kelly_fraction = self._kelly_fraction(yes_price, no_price, edge_pct)

        price_momentum = 0.0
        if last_trade > 0 and yes_price > 0:
            price_momentum = round((last_trade - yes_price) / yes_price * 100, 2)

        end_date = raw.get("endDate") or raw.get("endDateIso")
        days_to_expiry = None
        if end_date:
            try:
                exp = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                days_to_expiry = max(0, (exp - datetime.now(timezone.utc)).days)
            except Exception:
                pass

        tokens = raw.get("clobTokenIds", [])
        if isinstance(tokens, str):
            try:
                tokens = json.loads(tokens)
            except Exception:
                tokens = []

        return {
            "condition_id": raw.get("conditionId", raw.get("condition_id", "")),
            "question": raw.get("question", ""),
            "description": (raw.get("description") or "")[:300],
            "yes_price": round(yes_price, 4),
            "no_price": round(no_price, 4),
            "yes_pct": round(yes_price * 100, 1),
            "no_pct": round(no_price * 100, 1),
            "last_trade_price": round(last_trade, 4),
            "best_bid": round(best_bid, 4),
            "best_ask": round(best_ask, 4),
            "spread": round(spread, 4),
            "spread_pct": round(spread * 100, 3),
            "volume_24h": round(volume_24h, 2),
            "volume_1wk": round(volume_1wk, 2),
            "volume_1mo": round(volume_1mo, 2),
            "liquidity": round(liquidity, 2),
            "implied_sum": round(implied_sum, 4),
            "edge_pct": round(edge_pct * 100, 3),
            "edge_detected": edge_detected,
            "mispricing_score": round(mispricing_score, 4),
            "volume_momentum": volume_momentum,
            "vol_liq_ratio": round(vol_liq_ratio, 2),
            "whale_activity": whale_activity,
            "is_competitive": is_competitive,
            "neg_risk": neg_risk,
            "market_efficiency_score": market_efficiency,
            "kelly_fraction_pct": round(kelly_fraction * 100, 2) if kelly_fraction else 0,
            "price_momentum_pct": price_momentum,
            "days_to_expiry": days_to_expiry,
            "end_date": end_date,
            "tags": [t.get("label", t) if isinstance(t, dict) else t for t in (raw.get("tags") or [])],
            "clob_token_ids": tokens,
            "image": raw.get("image") or raw.get("icon"),
            "accepting_orders": bool(raw.get("acceptingOrders") or raw.get("accepting_orders")),
        }

    def _score_efficiency(
        self, spread: float, liquidity: float, competitive: bool, volume_24h: float
    ) -> float:
        """
        Market efficiency score 0-100 (100 = most efficient).
        Tight spread + high liquidity + competitive flag = sharp money market.
        """
        score = 50.0
        if spread < 0.02:
            score += 20
        elif spread < 0.05:
            score += 10
        elif spread > 0.15:
            score -= 15

        if liquidity > 500_000:
            score += 15
        elif liquidity > 100_000:
            score += 8
        elif liquidity < 10_000:
            score -= 10

        if competitive:
            score += 15

        if volume_24h > 100_000:
            score += 10
        elif volume_24h > 10_000:
            score += 5

        return round(max(0.0, min(100.0, score)), 1)

    def _kelly_fraction(self, yes_price: float, no_price: float, edge_pct: float) -> Optional[float]:
        """
        Kelly Criterion fraction for YES position.
        Kelly = (edge) / (odds_against) where odds = 1/yes_price - 1
        Only positive when we have a genuine edge.
        """
        if yes_price <= 0 or yes_price >= 1 or edge_pct <= 0:
            return None
        try:
            p = yes_price
            b = (1 / yes_price) - 1
            kelly = (b * p - (1 - p)) / b
            return max(0.0, min(kelly * 0.25, 0.15))
        except Exception:
            return None

    def _analyze_book_depth(self, book: dict) -> dict:
        """
        Summarize order book depth — bid/ask imbalance, total resting size.
        Used as a signal for smart-money positioning.
        """
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        total_bid_size = sum(float(b.get("size", 0)) for b in bids[:20])
        total_ask_size = sum(float(a.get("size", 0)) for a in asks[:20])
        imbalance = 0.0
        if total_bid_size + total_ask_size > 0:
            imbalance = (total_bid_size - total_ask_size) / (total_bid_size + total_ask_size)
        return {
            "bid_depth": round(total_bid_size, 2),
            "ask_depth": round(total_ask_size, 2),
            "imbalance": round(imbalance, 3),
            "bid_ask_signal": "buy_pressure" if imbalance > 0.15 else "sell_pressure" if imbalance < -0.15 else "neutral",
        }

    # ── HTTP Helpers ──────────────────────────────────────────────────────────

    async def _fetch_markets(self, limit: int = 100, tag: Optional[str] = None) -> list[dict]:
        params = {
            "limit": str(min(limit, 500)),
            "active": "true",
            "closed": "false",
            "order": "volume24hr",
            "ascending": "false",
        }
        if tag:
            params["tag_slug"] = tag
        try:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
                resp = await client.get(f"{GAMMA_BASE}/markets", params=params, headers=_HEADERS)
                resp.raise_for_status()
                data = resp.json()
                return data if isinstance(data, list) else []
        except Exception as e:
            print(f"[PM_INTEL] _fetch_markets error: {e}")
            return []

    async def _fetch_market_by_condition(self, condition_id: str) -> Optional[dict]:
        try:
            async with httpx.AsyncClient(timeout=12.0, follow_redirects=True) as client:
                resp = await client.get(
                    f"{GAMMA_BASE}/markets",
                    params={"condition_id": condition_id},
                    headers=_HEADERS,
                )
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, list) and data:
                    return data[0]
                if isinstance(data, dict) and data:
                    return data
        except Exception as e:
            print(f"[PM_INTEL] _fetch_market_by_condition error: {e}")
        return None

    async def _fetch_order_book(self, condition_id: str) -> Optional[dict]:
        """
        Attempt to fetch CLOB order book for the yes token of this market.
        Returns None silently if no book exists (many markets don't have one).
        """
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                resp = await client.get(
                    f"{CLOB_BASE}/book",
                    params={"market": condition_id},
                    headers=_HEADERS,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if "error" not in data:
                        return data
        except Exception:
            pass
        return None

    async def _fetch_events_for_categories(self, limit: int = 200) -> list[dict]:
        """Fetch events (which carry proper tag arrays) for category breakdown."""
        try:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
                resp = await client.get(
                    f"{GAMMA_BASE}/events",
                    params={
                        "limit": str(min(limit, 500)),
                        "active": "true",
                        "closed": "false",
                        "order": "volume24hr",
                        "ascending": "false",
                    },
                    headers=_HEADERS,
                )
                resp.raise_for_status()
                data = resp.json()
                return data if isinstance(data, list) else []
        except Exception as e:
            print(f"[PM_INTEL] _fetch_events_for_categories error: {e}")
            return []


# ── Utility helpers ────────────────────────────────────────────────────────────

def _slim(markets: list[dict]) -> list[dict]:
    """Return a slimmed version of market dicts for list responses."""
    keep = [
        "condition_id", "question", "yes_pct", "no_pct", "yes_price", "no_price",
        "volume_24h", "liquidity", "spread_pct", "edge_pct", "edge_detected",
        "mispricing_score", "volume_momentum", "whale_activity", "is_competitive",
        "market_efficiency_score", "kelly_fraction_pct", "price_momentum_pct",
        "days_to_expiry", "end_date", "tags", "image", "vol_liq_ratio",
    ]
    return [{k: m[k] for k in keep if k in m} for m in markets]


polymarket_intel = PolymarketIntelligence()
