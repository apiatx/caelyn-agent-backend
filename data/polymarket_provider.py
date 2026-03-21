"""
Polymarket prediction markets data provider.

Fetches event and market data from the Polymarket Gamma API for use by the
agent when answering prediction / outcome / probability questions.

Endpoints:
  - Events:  GET https://gamma-api.polymarket.com/events
  - Markets: GET https://gamma-api.polymarket.com/markets

Free / unauthenticated — no API key required.
"""
import json
import asyncio
import httpx
from data.cache import cache

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop


POLYMARKET_TTL = 120          # 2 minutes — markets move fast
POLYMARKET_TAG_TTL = 180      # 3 minutes for tag-specific queries
GAMMA_BASE = "https://gamma-api.polymarket.com"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; TradingAgent/1.0)",
    "Accept": "application/json",
}


class PolymarketProvider:
    """Fetch and normalise Polymarket prediction market data."""

    # ── public methods ──────────────────────────────────────────────

    @traceable(name="get_top_events")
    async def get_top_events(self, limit: int = 50) -> list[dict]:
        """Return the top active events sorted by 24h volume."""
        cache_key = f"polymarket:top_events:{limit}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        try:
            raw = await self._fetch_events(
                params={
                    "limit": str(limit),
                    "active": "true",
                    "closed": "false",
                    "order": "volume24hr",
                    "ascending": "false",
                }
            )
            events = self._normalise_events(raw)
            cache.set(cache_key, events, POLYMARKET_TTL)
            return events
        except Exception as e:
            print(f"[Polymarket] get_top_events error: {e}")
            return []

    @traceable(name="get_events_by_tag")
    async def get_events_by_tag(self, tag_slug: str, limit: int = 50) -> list[dict]:
        """Return active events for a specific tag (e.g. 'earnings', 'crypto', 'finance')."""
        cache_key = f"polymarket:tag:{tag_slug}:{limit}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        try:
            raw = await self._fetch_events(
                params={
                    "limit": str(limit),
                    "active": "true",
                    "closed": "false",
                    "order": "volume24hr",
                    "ascending": "false",
                    "tag_slug": tag_slug,
                }
            )
            events = self._normalise_events(raw)
            cache.set(cache_key, events, POLYMARKET_TAG_TTL)
            return events
        except Exception as e:
            print(f"[Polymarket] get_events_by_tag({tag_slug}) error: {e}")
            return []

    @traceable(name="get_macro_prediction_context")
    async def get_macro_prediction_context(self) -> dict:
        """
        Build a rich context dict suitable for the agent's prediction_markets
        category.  Returns top events + category breakdowns.
        """
        cache_key = "polymarket:macro_context"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        # Fetch top events and a few category-specific slices in parallel
        top_task = self.get_top_events(limit=50)
        earnings_task = self.get_events_by_tag("earnings", limit=30)
        crypto_task = self.get_events_by_tag("crypto", limit=20)
        finance_task = self.get_events_by_tag("finance", limit=20)

        top, earnings, crypto, finance = await asyncio.gather(
            top_task, earnings_task, crypto_task, finance_task,
            return_exceptions=True,
        )

        context: dict = {}
        if isinstance(top, list) and top:
            context["top_events"] = top[:40]
        if isinstance(earnings, list) and earnings:
            context["earnings_events"] = earnings[:20]
        if isinstance(crypto, list) and crypto:
            context["crypto_events"] = crypto[:15]
        if isinstance(finance, list) and finance:
            context["finance_events"] = finance[:15]

        # Summary statistics
        all_events = top if isinstance(top, list) else []
        if all_events:
            total_volume_24h = sum(e.get("volume24hr", 0) for e in all_events)
            total_liquidity = sum(e.get("liquidity", 0) for e in all_events)
            context["summary"] = {
                "total_events": len(all_events),
                "total_24h_volume": round(total_volume_24h, 2),
                "total_liquidity": round(total_liquidity, 2),
            }

        cache.set(cache_key, context, POLYMARKET_TTL)
        return context

    @traceable(name="search_events")
    async def search_events(self, query: str, limit: int = 20) -> list[dict]:
        """
        Search for events matching a free-text query.
        Gamma API doesn't have a search endpoint, so we fetch a large batch
        and filter locally.
        """
        all_events = await self.get_top_events(limit=100)
        q_lower = query.lower()
        keywords = q_lower.split()
        matches = []
        for ev in all_events:
            text = f"{ev.get('title', '')} {ev.get('description', '')}".lower()
            tags_text = " ".join(ev.get("tags", [])).lower()
            combined = f"{text} {tags_text}"
            if any(kw in combined for kw in keywords):
                matches.append(ev)
        return matches[:limit]

    # ── internal helpers ────────────────────────────────────────────

    @traceable(name="fetch_events")
    async def _fetch_events(self, params: dict) -> list[dict]:
        """Raw HTTP call to Gamma API /events endpoint."""
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            resp = await client.get(
                f"{GAMMA_BASE}/events",
                params=params,
                headers=_HEADERS,
            )
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else []

    @traceable(name="normalise_events")
    def _normalise_events(self, raw_events: list[dict]) -> list[dict]:
        """
        Slim events down to essential fields to keep context window usage low
        while preserving the data the agent needs.
        """
        results = []
        for ev in raw_events:
            if not ev.get("active") or ev.get("closed"):
                continue
            markets = []
            for m in ev.get("markets") or []:
                if not m.get("active") or m.get("closed"):
                    continue
                try:
                    prices = json.loads(m.get("outcomePrices", "[]"))
                except Exception:
                    prices = []
                yes_price = round(float(prices[0]), 3) if prices else 0
                no_price = round(float(prices[1]), 3) if len(prices) > 1 else 0
                markets.append({
                    "question": m.get("question", ev.get("title", "")),
                    "yes_price": yes_price,
                    "no_price": no_price,
                    "volume24hr": m.get("volume24hr", 0),
                })
            if not markets:
                continue
            results.append({
                "title": ev.get("title", ""),
                "description": (ev.get("description") or "")[:250],
                "volume24hr": ev.get("volume24hr", 0),
                "volume": ev.get("volume", 0),
                "liquidity": ev.get("liquidity", 0),
                "end_date": ev.get("endDate"),
                "tags": [t.get("label", "") for t in (ev.get("tags") or [])],
                "markets": markets,
            })
        return results
