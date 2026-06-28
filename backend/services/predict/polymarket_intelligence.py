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
from services.predict.scoring import score_markets
from services.predict.recommendations import build_recommendations, generate_reasons
from services.predict.signal_changes import signal_tracker

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; CaelynAI-Predict/1.0)",
    "Accept": "application/json",
}

_MARKET_CACHE_TTL = 90
_SIGNALS_CACHE_TTL = 120
_MARKET_DETAIL_TTL = 60
_SCORED_CACHE_TTL = 90
_RECOMMENDATIONS_CACHE_TTL = 120
_SIGNAL_CHANGES_CACHE_TTL = 60

_SPORTS_KEYWORDS = [
    "nfl", "nba", "mlb", "nhl", "nascar", "ufc", "mma", "boxing",
    "soccer", "football", "basketball", "baseball", "hockey", "tennis",
    "golf", "cricket", "f1", "formula 1", "formula one", "rugby",
    "super bowl", "world series", "stanley cup", "champions league",
    "europa league", "premier league", "la liga", "serie a", "bundesliga",
    "ligue 1", "copa america", "copa del rey", "fa cup",
    "fifa", "olympics", "paralympics",
    "ipl", "indian premier league", "wta", "atp", "grand prix",
    "vs.", " vs ", "game 1", "game 2", "game 3", "game 4", "game 5",
    "game 6", "game 7", "series",
    "yankees", "dodgers", "mets", "cubs", " sox", "astros", "braves",
    "orioles", "twins", "marlins", "diamondbacks", "rangers", "athletics",
    "phillies", "angels", "padres", "giants", "cardinals", "brewers",
    "nationals", "tigers", "royals", "mariners", "pirates", "reds",
    "rockies", "rays", "blue jays", "red sox", "white sox",
]


def _is_sports_market(m: dict) -> bool:
    """Return True if this market appears to be a live sports game/match result."""
    question = m.get("question", "").lower()
    tags = " ".join(str(t).lower() for t in (m.get("tags") or []))
    combined = f"{question} {tags}"
    return any(kw in combined for kw in _SPORTS_KEYWORDS)


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
        limit: int = 200,
        tag: Optional[str] = None,
        min_volume_24h: float = 0,
    ) -> list[dict]:
        """
        Return enriched, analytics-decorated markets sorted by 24h volume.
        Applies all Jon-Becker-style signal computations on top of raw API data.

        When a tag filter is provided we:
          1. Ask the Gamma API for up to 1 000 markets with that tag (best-effort
             — Gamma's tag_slug param is inconsistent across market types).
          2. Apply a reliable client-side tag filter so niche categories always
             return results even if the API-level filter is incomplete.
        When no tag is given we fetch a pool 3× the requested limit (capped at
        600) so sorting by volume picks the best markets after active filtering.
        """
        key = f"pm:intel:top:{limit}:{tag}:{min_volume_24h}"
        cached = cache.get(key)
        if cached is not None:
            return cached

        # Gamma's /markets endpoint does not carry tag data — tags live on /events.
        # When a tag filter is requested we go through the events API which embeds
        # markets AND has reliable tag_slug filtering. Without a tag we pull a
        # large pool from /markets and sort by volume.
        if tag:
            raw = await self._fetch_markets_by_tag(tag, limit=min(limit * 5, 2000))
        else:
            fetch_size = min(limit * 3, 600)
            raw = await self._fetch_markets(limit=fetch_size)

        enriched = [self._enrich_market(m) for m in raw]

        # Drop expired / resolving markets — their volume is settlement noise,
        # not active trading interest. Also apply the accepting_orders guard.
        enriched = [
            m for m in enriched
            if not m.get("is_expired")
            and not m.get("is_resolving")
            and m.get("accepting_orders", True)
            and m["volume_24h"] >= min_volume_24h
        ]

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

        # Separate active from expired/resolving for signal purposes:
        # expired/resolving volume is settlement activity, not smart money.
        active_markets = [m for m in markets if not m.get("is_expired") and not m.get("is_resolving")]
        expired_count = sum(1 for m in markets if m.get("is_expired"))
        resolving_count = sum(1 for m in markets if m.get("is_resolving"))

        # Score all active markets so score fields are available for _slim().
        # score_markets() is pure Python over already-enriched dicts — no extra API calls.
        scored_active = score_markets(active_markets)
        for m in scored_active:
            scores = m.get("scores", {})
            m["conviction_score"] = scores.get("conviction", 0) or 0
            m["momentum_score"] = scores.get("momentum", 0) or 0
            m["flow_score"] = scores.get("flow", 0) or 0
            m["execution_quality_score"] = scores.get("execution_quality", 0) or 0
            m["participation_quality_score"] = scores.get("participation_quality", 0) or 0
            m["time_quality_score"] = scores.get("time_quality", 0) or 0
            m["trap_risk_score"] = scores.get("trap_risk", 0) or 0
            m["composite_score"] = m.get("composite_score", 0) or 0
            m["momentum_label"] = m.get("momentum_label", "flat") or "flat"
            m["price_change_24h"] = m.get("price_change_1d", 0)
            if not m.get("slug"):
                question = m.get("question", "")
                slug = question.lower().strip()
                for ch in ["?", "'", '"', ",", ".", "!", "(", ")", "[", "]", "{", "}", "&", "%", "$", "#", "@"]:
                    slug = slug.replace(ch, "")
                slug = slug.replace(" ", "-").replace("--", "-").strip("-")
                m["slug"] = slug[:100]
        active_markets = scored_active

        # Exclude sports game markets from all signal lists — sports games resolve
        # within hours (loser → 0%, winner → 100%) and dominate movers/edges/whale
        # signals with noise irrelevant to macro/financial prediction markets.
        macro_markets = [m for m in active_markets if not _is_sports_market(m)]

        edges = [m for m in macro_markets if m.get("edge_detected")]
        mispricings = [m for m in macro_markets if m.get("mispricing_score", 0) > 0.03]
        movers = [m for m in macro_markets if abs(m.get("price_change_1d", 0)) > 1.0]
        momentum_up = [m for m in macro_markets if m.get("volume_momentum") == "surging"]
        momentum_down = [m for m in macro_markets if m.get("volume_momentum") == "fading"]
        whale_markets = [m for m in macro_markets if m.get("whale_activity")]
        competitive = [m for m in macro_markets if m.get("is_competitive")]

        total_vol_24h = sum(m.get("volume_24h", 0) for m in markets)
        total_liquidity = sum(m.get("liquidity", 0) for m in active_markets)
        avg_spread = (
            sum(m.get("spread", 0) for m in active_markets if m.get("spread") is not None)
            / max(1, sum(1 for m in active_markets if m.get("spread") is not None))
        )

        result = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "market_count": len(markets),
            "active_market_count": len(active_markets),
            "expired_count": expired_count,
            "resolving_count": resolving_count,
            "summary": {
                "total_volume_24h": round(total_vol_24h, 2),
                "total_liquidity": round(total_liquidity, 2),
                "avg_spread_pct": round(avg_spread * 100, 3),
                "competitive_market_pct": round(len(competitive) / max(1, len(active_markets)) * 100, 1),
                "edge_count": len(edges),
                "mispricing_count": len(mispricings),
                "surging_count": len(momentum_up),
                "fading_count": len(momentum_down),
                "whale_active_count": len(whale_markets),
            },
            "top_edges": _slim(sorted(edges, key=lambda m: m.get("edge_pct", 0), reverse=True)[:8]),
            "top_mispricings": _slim(sorted(mispricings, key=lambda m: m.get("mispricing_score", 0), reverse=True)[:8]),
            "top_movers": _slim(sorted(movers, key=lambda m: abs(m.get("price_change_1d", 0)), reverse=True)[:8]),
            "surging_markets": _slim(momentum_up[:6]),
            "whale_markets": _slim(whale_markets[:6]),
            # top_by_volume only shows active markets so expired markets don't pollute the feed
            "top_by_volume": _slim(sorted(active_markets, key=lambda m: m.get("volume_24h", 0), reverse=True)[:10]),
            "top_by_liquidity": _slim(sorted(active_markets, key=lambda m: m.get("liquidity", 0), reverse=True)[:8]),
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

    # ── Prophetik Signal Engine ────────────────────────────────────────────────

    async def get_scored_markets(
        self,
        limit: int = 200,
        tag: Optional[str] = None,
        min_volume_24h: float = 0,
    ) -> list[dict]:
        """
        Return markets enriched with all 7 Prophetik scoring dimensions
        plus composite_score and momentum_label. Sorted by composite_score desc.
        Score dimensions are also flattened to top-level keys for direct access.
        All score fields guaranteed to be numbers (never None).

        Filters out:
        - Sports game markets (tennis, cricket, soccer, etc.) — these resolve
          within hours and flood rankings with non-actionable noise.
        - Fully resolved markets (0% or 100%) — no remaining trading opportunity.
        """
        key = f"pm:scored:{limit}:{tag}:{min_volume_24h}"
        cached = cache.get(key)
        if cached is not None:
            return cached

        markets = await self.get_top_markets(limit=limit, tag=tag, min_volume_24h=min_volume_24h)

        # Filter out sports game markets — they dominate rankings with noise
        markets = [m for m in markets if not _is_sports_market(m)]

        # Filter out fully resolved markets (0% or 100%) — no tradable edge
        markets = [m for m in markets if 0.5 < m.get("yes_pct", 50) < 99.5]

        scored = score_markets(markets)

        # Post-process: flatten score dimensions to top level, ensure no None
        for m in scored:
            scores = m.get("scores", {})
            m["conviction_score"] = scores.get("conviction", 0) or 0
            m["momentum_score"] = scores.get("momentum", 0) or 0
            m["flow_score"] = scores.get("flow", 0) or 0
            m["execution_quality_score"] = scores.get("execution_quality", 0) or 0
            m["participation_quality_score"] = scores.get("participation_quality", 0) or 0
            m["time_quality_score"] = scores.get("time_quality", 0) or 0
            m["trap_risk_score"] = scores.get("trap_risk", 0) or 0
            m["composite_score"] = m.get("composite_score", 0) or 0
            m["momentum_label"] = m.get("momentum_label", "flat") or "flat"
            # Add price_change_24h alias for frontend compatibility
            m["price_change_24h"] = m.get("price_change_1d", 0)
            # Use slug from Gamma API if available, otherwise generate from question
            if not m.get("slug"):
                question = m.get("question", "")
                slug = question.lower().strip()
                for ch in ["?", "'", '"', ",", ".", "!", "(", ")", "[", "]", "{", "}", "&", "%", "$", "#", "@"]:
                    slug = slug.replace(ch, "")
                slug = slug.replace(" ", "-").replace("--", "-").strip("-")
                m["slug"] = slug[:100]

        # Enrich with reasons and direction for frontend badges
        for m in scored:
            if not m.get("reasons"):
                m["reasons"] = generate_reasons(m, "best_bet_now")
            if not m.get("direction"):
                pc_24h = m.get("price_change_1d", 0)
                yes_pct = m.get("yes_pct", 50)
                if pc_24h > 0 or yes_pct > 55:
                    m["direction"] = "YES"
                elif pc_24h < 0 or yes_pct < 45:
                    m["direction"] = "NO"
                else:
                    m["direction"] = "YES"

        cache.set(key, scored, _SCORED_CACHE_TTL)
        return scored

    async def get_recommendations(self) -> dict:
        """
        Top decision-layer payload: recommendation buckets with explainability.
        Uses scored markets as input. Returns bucket keys at top level:
        {
            "generated_at": "...",
            "market_count": N,
            "best_bet_now": [...],
            "best_yes_setup": [...],
            ...
        }
        """
        key = "pm:recommendations"
        cached = cache.get(key)
        if cached is not None:
            return cached

        scored = await self.get_scored_markets(limit=200)
        buckets = build_recommendations(scored)

        # Feed the signal tracker with fresh scored data + buckets.
        # The tracker runs the stability stabilizer and emits recommendation-level changes.
        try:
            signal_tracker.update(scored, buckets)
        except Exception as e:
            print(f"[PREDICT/signal-tracker] update error (non-fatal): {e}")

        # Retrieve the stability-controlled best bet from the tracker
        try:
            pinned = signal_tracker.get_pinned_best_bet()
        except Exception as e:
            print(f"[PREDICT/signal-tracker] get_pinned_best_bet error (non-fatal): {e}")
            pinned = {"market": None, "stability": {}}

        # Return bucket keys at the top level for direct frontend access
        result = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "market_count": len(scored),
            **buckets,
            # Stability-controlled Best Bet — use this instead of best_bet_now[0]
            # for the prominent "Best Bet Right Now" card in the Signal Engine UI.
            # Includes hysteresis (min score gap), persistence (N consecutive cycles),
            # and cooldown (hold for 5 min after change) to prevent rapid flipping.
            "pinned_best_bet": pinned,
        }
        cache.set(key, result, _RECOMMENDATIONS_CACHE_TTL)
        return result

    async def get_signal_changes(self) -> dict:
        """
        Return recent signal changes detected by snapshot diffing.
        Triggers a recommendations refresh if the cache is stale so the
        tracker stays reasonably up-to-date.
        """
        key = "pm:signal_changes"
        cached = cache.get(key)
        if cached is not None:
            return cached

        # Ensure tracker has fresh data by calling recommendations
        # (which internally calls get_scored_markets and feeds the tracker)
        await self.get_recommendations()

        result = signal_tracker.get_recent_changes()
        cache.set(key, result, _SIGNAL_CHANGES_CACHE_TTL)
        return result

    async def get_enriched_signals(self) -> dict:
        """
        Extended signals dashboard that includes standard signals PLUS
        Prophetik scoring summaries and top recommendations.
        Backward-compatible: includes all fields from get_market_signals().
        """
        key = "pm:enriched_signals"
        cached = cache.get(key)
        if cached is not None:
            return cached

        # Fetch base signals and scored markets in parallel
        signals_task = self.get_market_signals()
        scored_task = self.get_scored_markets(limit=100)

        signals, scored = await asyncio.gather(signals_task, scored_task, return_exceptions=True)

        if isinstance(signals, Exception):
            signals = {}
        if isinstance(scored, Exception):
            scored = []

        # Build scoring summary stats
        if scored:
            avg_composite = sum(m.get("composite_score", 0) for m in scored) / len(scored)
            avg_trap = sum(m.get("scores", {}).get("trap_risk", 0) for m in scored) / len(scored)
            high_conviction = [m for m in scored if m.get("scores", {}).get("conviction", 0) >= 60]
            high_momentum = [m for m in scored if m.get("scores", {}).get("momentum", 0) >= 50]
            high_flow = [m for m in scored if m.get("scores", {}).get("flow", 0) >= 50]

            signals["scoring_summary"] = {
                "avg_composite_score": round(avg_composite, 1),
                "avg_trap_risk": round(avg_trap, 1),
                "high_conviction_count": len(high_conviction),
                "high_momentum_count": len(high_momentum),
                "high_flow_count": len(high_flow),
            }
            signals["top_scored"] = _slim_scored(sorted(
                scored, key=lambda m: m.get("composite_score", 0), reverse=True
            )[:10])

        cache.set(key, signals, _SIGNALS_CACHE_TTL)
        return signals

    async def get_scored_market_detail(self, condition_id: str) -> Optional[dict]:
        """
        Deep analysis of a single market including scoring dimensions.
        Extends get_market_detail with Prophetik scores.
        """
        detail = await self.get_market_detail(condition_id)
        if not detail:
            return None
        # Apply scoring to this single market
        scored_list = score_markets([detail])
        return scored_list[0] if scored_list else detail

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

        # Price changes from Gamma (reliable, returned for all active markets)
        price_change_1d = float(raw.get("oneDayPriceChange") or 0)
        price_change_1h = float(raw.get("oneHourPriceChange") or 0)
        price_change_1wk = float(raw.get("oneWeekPriceChange") or 0)

        implied_sum = yes_price + no_price

        # Edge = spread as a fraction of the YES price.
        # A $0.01 spread on a $0.20 market is a 5% edge for anyone transacting —
        # meaningful for market-making or for estimating transaction cost impact.
        # Threshold: > 3% spread-to-price ratio = real edge signal.
        spread_pct_of_price = (spread / yes_price * 100) if yes_price > 0.01 else 0
        edge_pct = round(spread_pct_of_price, 2)
        edge_detected = spread_pct_of_price > 3.0 or abs(price_change_1d) > 5.0

        # Mispricing: difference between order book mid and displayed price
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

        # Expiry — compute before momentum/whale so we can suppress stale signals.
        # Gamma is inconsistent: some markets have endDate=None but closed=True,
        # others have endDate in the past with closed=False (resolution lag).
        # We combine all three signals to reliably detect non-active markets.
        end_date = raw.get("endDate") or raw.get("endDateIso")
        days_to_expiry = None
        hours_to_expiry = None
        is_expired = False
        is_resolving = False

        # Signal 1: closed=True or acceptingOrders=False with no future endDate
        _closed = raw.get("closed") is True
        _not_accepting = not bool(raw.get("acceptingOrders") or raw.get("accepting_orders"))

        if end_date:
            try:
                exp = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                delta = exp - datetime.now(timezone.utc)
                total_seconds = delta.total_seconds()
                hours_to_expiry = total_seconds / 3600
                days_to_expiry = max(0, int(delta.days))
                is_expired = total_seconds < 0
                is_resolving = 0 <= total_seconds < 72 * 3600
            except Exception:
                pass

        # Signal 2: no endDate but Gamma says closed or not accepting orders
        if not is_expired and not is_resolving and (_closed or _not_accepting):
            is_expired = True

        # Suppress momentum/whale signals for expired or near-expiry markets:
        # High volume on a market about to close (or already closed) is settlement
        # activity, NOT a smart-money signal. Label it "resolving" to prevent
        # misleading "SURGING" badges from showing up on dead markets.
        if is_expired:
            volume_momentum = "expired"
            whale_activity = False
            edge_detected = False
        elif is_resolving:
            volume_momentum = "resolving"
            whale_activity = False
        else:
            whale_activity = vol_liq_ratio > 5.0 and volume_24h > 10_000

        market_efficiency = self._score_efficiency(spread, liquidity, is_competitive, volume_24h)

        price_momentum = 0.0
        if last_trade > 0 and yes_price > 0:
            price_momentum = round((last_trade - yes_price) / yes_price * 100, 2)

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
            "edge_pct": round(edge_pct, 2),
            "edge_detected": edge_detected,
            "mispricing_score": round(mispricing_score, 4),
            "volume_momentum": volume_momentum,
            "vol_liq_ratio": round(vol_liq_ratio, 2),
            "whale_activity": whale_activity,
            "is_competitive": is_competitive,
            "neg_risk": neg_risk,
            "market_efficiency_score": market_efficiency,
            "spread_pct_of_price": round(spread_pct_of_price, 2),
            "price_change_1h": round(price_change_1h * 100, 2),
            "price_change_1d": round(price_change_1d * 100, 2),
            "price_change_1wk": round(price_change_1wk * 100, 2),
            "price_momentum_pct": price_momentum,
            "days_to_expiry": days_to_expiry,
            "hours_to_expiry": round(hours_to_expiry, 1) if hours_to_expiry is not None else None,
            "is_expired": is_expired,
            "is_resolving": is_resolving,
            "end_date": end_date,
            "tags": [t.get("label", t) if isinstance(t, dict) else t for t in (raw.get("tags") or [])],
            "clob_token_ids": tokens,
            "image": raw.get("image") or raw.get("icon"),
            "accepting_orders": bool(raw.get("acceptingOrders") or raw.get("accepting_orders")),
            "slug": raw.get("slug") or raw.get("market_slug") or "",
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

    # ── Full Catalog Crawl (for Tracked Odds Registry) ────────────────────────

    async def fetch_full_active_catalog(self) -> tuple[list[dict], dict]:
        """
        Paginate ALL active/open events from the Gamma API and flatten their
        nested markets.  No tag filter — this crawls the entire active universe.

        Preferred over /markets because /events carries tags AND has nested
        market arrays, reducing total API calls while capturing all categories.

        Returns:
            (flat_market_list, stats_dict)

        stats_dict keys:
            catalog_events_pages_fetched, catalog_events_total,
            catalog_markets_flattened
        """
        # Gamma /events endpoint returns at most 100 events per page regardless
        # of the limit parameter.  Use 100 to get accurate pagination detection
        # (if page < PAGE_SIZE we know we're done).
        PAGE_SIZE   = 100
        MAX_PAGES   = 150       # safety cap: 150 × 100 = 15 000 events maximum
        TIMEOUT_S   = 30.0

        base_params = {
            "active":     "true",
            "closed":     "false",
            "limit":      str(PAGE_SIZE),
            "order":      "volume24hr",
            "ascending":  "false",
        }

        all_markets: list[dict] = []
        pages_fetched  = 0
        events_total   = 0
        offset         = 0

        async with httpx.AsyncClient(timeout=TIMEOUT_S, follow_redirects=True) as client:
            while pages_fetched < MAX_PAGES:
                params = {**base_params, "offset": str(offset)}
                try:
                    resp = await client.get(
                        f"{GAMMA_BASE}/events", params=params, headers=_HEADERS
                    )
                    resp.raise_for_status()
                    events_page = resp.json()
                    if not isinstance(events_page, list) or not events_page:
                        break

                    pages_fetched += 1
                    events_total  += len(events_page)

                    for ev in events_page:
                        ev_tags = [
                            t.get("label", t) if isinstance(t, dict) else t
                            for t in (ev.get("tags") or [])
                        ]
                        ev_slug = ev.get("slug", "")
                        for market in (ev.get("markets") or []):
                            market["tags"]       = ev_tags
                            market["event_slug"] = ev_slug
                            all_markets.append(market)

                    if len(events_page) < PAGE_SIZE:
                        break   # last page — no more data
                    offset += PAGE_SIZE

                except Exception as exc:
                    print(
                        f"[PM_INTEL] fetch_full_active_catalog error "
                        f"(page={pages_fetched}, offset={offset}): {exc}"
                    )
                    break

        stats = {
            "catalog_events_pages_fetched": pages_fetched,
            "catalog_events_total":         events_total,
            "catalog_markets_flattened":    len(all_markets),
        }
        return all_markets, stats

    async def get_clob_midpoint(self, token_id: str) -> Optional[float]:
        """
        Fetch the YES-token midpoint price from the public CLOB read API.
        No authentication required.

        Endpoint: GET https://clob.polymarket.com/midpoint?token_id=<token_id>
        Response:  {"mid": "0.42"}

        Returns the mid as a float in [0, 1], or None on failure.
        Falls back gracefully — callers should use Gamma outcomePrices if None.
        """
        if not token_id:
            return None
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(
                    f"{CLOB_BASE}/midpoint",
                    params={"token_id": token_id},
                    headers=_HEADERS,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    mid = data.get("mid")
                    if mid is not None:
                        val = float(mid)
                        if 0.0 < val < 1.0:
                            return val
        except Exception:
            pass
        return None

    # ── HTTP Helpers ──────────────────────────────────────────────────────────

    async def _fetch_markets(self, limit: int = 200) -> list[dict]:
        """
        Fetch active markets from the Gamma API, ordered by 24-hour volume.
        Paginates automatically when limit > 500 (Gamma's per-request maximum).
        No tag filtering — use _fetch_markets_by_tag() for category queries.
        """
        PAGE_MAX = 500

        base_params = {
            "active": "true",
            "closed": "false",
            "order": "volume24hr",
            "ascending": "false",
        }

        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
            results: list[dict] = []
            offset = 0
            remaining = min(limit, 1000)

            while remaining > 0:
                page_size = min(remaining, PAGE_MAX)
                params = {**base_params, "limit": str(page_size), "offset": str(offset)}
                try:
                    resp = await client.get(f"{GAMMA_BASE}/markets", params=params, headers=_HEADERS)
                    resp.raise_for_status()
                    page = resp.json()
                    if not isinstance(page, list) or not page:
                        break
                    results.extend(page)
                    if len(page) < page_size:
                        break  # Gamma returned fewer than asked — no more pages
                    offset += page_size
                    remaining -= page_size
                except Exception as e:
                    print(f"[PM_INTEL] _fetch_markets error (offset={offset}): {e}")
                    break

            return results

    async def _fetch_markets_by_tag(self, tag: str, limit: int = 1000) -> list[dict]:
        """
        Fetch markets that belong to events matching `tag`.

        Gamma's /markets endpoint does not carry tag data — tags are attached to
        events, and each event embeds its markets. This method:

          1. Queries /events?tag_slug=<slug> (slug = tag.lower().replace(" ", "-"))
          2. Extracts all embedded markets from matching events
          3. Injects the parent event's tag labels onto each market's `tags` field
             so downstream enrichment and _is_sports_market() can read them.

        The `limit` argument caps how many raw markets are returned before
        enrichment/active-filtering in get_top_markets().
        """
        # Explicit label → Gamma slug mapping.
        # Gamma's /events?tag_slug= requires exact slug values which often differ
        # from the human-readable category labels the frontend dropdown sends.
        # Verified against live Gamma API (April 2026).
        _LABEL_TO_SLUG: dict[str, str] = {
            # Sports
            "sports": "sports",
            # Politics / Government
            "politics": "politics",
            "election": "elections",
            "elections": "elections",
            # Crypto / Finance
            "crypto": "crypto",
            "cryptocurrency": "crypto",
            "bitcoin": "bitcoin",
            "finance": "finance",
            "business": "business",
            "economy": "economy",
            "economics": "economy",
            # Geopolitics
            "geopolitics": "geopolitics",
            "global": "world",
            "world": "world",
            # Tech / AI
            "tech": "tech",
            "technology": "tech",
            "ai": "ai",
            "science": "tech",
            # Culture / Entertainment
            "culture": "pop-culture",
            "pop-culture": "pop-culture",
            "entertainment": "entertainment",
            "pop culture": "pop-culture",
            # Weather / Environment
            "weather": "weather",
            "climate": "weather",
            # Special Polymarket categories — no reliable Gamma slug, fall through to None
            "trending": None,
            "breaking": None,
            "new": None,
            "mentions": None,
        }

        raw_key = tag.strip().lower()
        mapped = _LABEL_TO_SLUG.get(raw_key)
        if mapped is None and raw_key not in _LABEL_TO_SLUG:
            # Unknown label: derive slug the standard way (lower + hyphenate)
            mapped = raw_key.replace(" ", "-")
        # mapped == None means "no good Gamma slug exists" — we return empty so
        # get_top_markets() falls back to the caller showing no-tag results.
        if mapped is None:
            return []
        slug = mapped
        # We always fetch a minimum of 200 events per tag regardless of how many
        # markets the caller requested. Each event embeds many markets (often 10-30)
        # but most may be closed date-series entries. Fetching at least 200 events
        # ensures we have a large enough pool to find `limit` OPEN markets after the
        # caller's active-market filter runs. For tags where the caller needs many
        # results (limit > 400), we scale up proportionally.
        EVENT_FETCH_MIN = 200
        PAGE_MAX = 200  # events endpoint is heavier than markets endpoint

        events_to_fetch = max(EVENT_FETCH_MIN, limit // 5)

        base_params = {
            "active": "true",
            "order": "volume24hr",
            "ascending": "false",
            "tag_slug": slug,
        }

        async with httpx.AsyncClient(timeout=25.0, follow_redirects=True) as client:
            all_markets: list[dict] = []
            offset = 0
            remaining = min(events_to_fetch, 1000)

            while remaining > 0:
                page_size = min(remaining, PAGE_MAX)
                params = {**base_params, "limit": str(page_size), "offset": str(offset)}
                try:
                    resp = await client.get(f"{GAMMA_BASE}/events", params=params, headers=_HEADERS)
                    resp.raise_for_status()
                    events_page = resp.json()
                    if not isinstance(events_page, list) or not events_page:
                        break

                    for ev in events_page:
                        # Extract tag labels from the event so markets can use them
                        ev_tags = [
                            t.get("label", t) if isinstance(t, dict) else t
                            for t in (ev.get("tags") or [])
                        ]
                        for market in (ev.get("markets") or []):
                            # Inject event tags onto the market raw dict
                            market["tags"] = ev_tags
                            all_markets.append(market)

                    if len(events_page) < page_size:
                        break  # No more pages
                    offset += page_size
                    remaining -= page_size
                except Exception as e:
                    print(f"[PM_INTEL] _fetch_markets_by_tag({tag!r}) error (offset={offset}): {e}")
                    break

            return all_markets

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
        "market_efficiency_score", "spread_pct_of_price",
        "price_change_1h", "price_change_1d", "price_change_1wk", "price_change_24h",
        "price_momentum_pct",
        "days_to_expiry", "hours_to_expiry", "is_expired", "is_resolving", "end_date",
        "tags", "image", "vol_liq_ratio", "slug",
        # Score fields — populated when get_market_signals() runs the scoring pipeline
        "composite_score", "conviction_score", "momentum_score", "momentum_label",
        "flow_score", "execution_quality_score", "participation_quality_score",
        "trap_risk_score",
    ]
    return [{k: m[k] for k in keep if k in m} for m in markets]


def _slim_scored(markets: list[dict]) -> list[dict]:
    """Return a slimmed version of scored market dicts — includes scoring fields."""
    keep = [
        "condition_id", "question", "yes_pct", "no_pct", "yes_price",
        "volume_24h", "liquidity", "spread", "spread_pct",
        "price_change_1h", "price_change_1d", "price_change_1wk",
        "price_change_24h",
        "volume_momentum", "whale_activity", "is_competitive",
        "days_to_expiry", "hours_to_expiry", "end_date", "tags", "image",
        "vol_liq_ratio", "slug",
        "scores", "composite_score", "momentum_label",
        "conviction_score", "momentum_score", "flow_score",
        "execution_quality_score", "participation_quality_score",
        "time_quality_score", "trap_risk_score",
    ]
    return [{k: m[k] for k in keep if k in m} for m in markets]


polymarket_intel = PolymarketIntelligence()
