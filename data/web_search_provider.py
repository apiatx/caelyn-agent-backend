"""
Unified web search provider: Brave (primary) → Tavily (fallback).

Budget strategy:
  - Brave: $5/month free credit = ~1,000 requests. Hard-cap at 950 to avoid charges.
  - When Brave monthly budget exhausted, fall back to Tavily (1,000/month free).
  - Monthly counter resets on the 1st of each month.
"""
from datetime import datetime
from data.brave_provider import BraveProvider
from data.tavily_provider import TavilyProvider


BRAVE_MONTHLY_LIMIT = 950  # Leave 50-request buffer under the 1,000 free cap


class MonthlyUsageTracker:
    """Tracks API call counts per calendar month."""

    def __init__(self, limit: int):
        self.limit = limit
        self._count = 0
        self._month = ""
        self._reset_if_new_month()

    def _reset_if_new_month(self):
        current = datetime.now().strftime("%Y-%m")
        if current != self._month:
            self._month = current
            self._count = 0

    def can_spend(self, n: int = 1) -> bool:
        self._reset_if_new_month()
        return (self._count + n) <= self.limit

    def spend(self, n: int = 1):
        self._reset_if_new_month()
        self._count += n

    @property
    def remaining(self) -> int:
        self._reset_if_new_month()
        return max(0, self.limit - self._count)

    @property
    def used(self) -> int:
        self._reset_if_new_month()
        return self._count

    def status(self) -> dict:
        self._reset_if_new_month()
        return {
            "month": self._month,
            "used": self._count,
            "limit": self.limit,
            "remaining": self.remaining,
            "pct": round(self._count / self.limit * 100, 1) if self.limit else 0,
        }


class WebSearchProvider:
    """
    Drop-in replacement for TavilyProvider.
    Routes through Brave first, falls back to Tavily when Brave budget is exhausted.
    Exposes the exact same method interface so callers don't need to change.
    """

    def __init__(self, brave_api_key: str = None, tavily_api_key: str = None):
        self.brave = BraveProvider(brave_api_key) if brave_api_key else None
        self.tavily = TavilyProvider(tavily_api_key) if tavily_api_key else None
        self.brave_usage = MonthlyUsageTracker(BRAVE_MONTHLY_LIMIT)

        if self.brave:
            print(f"[WebSearch] Brave primary (950/month cap), Tavily fallback")
        elif self.tavily:
            print(f"[WebSearch] Tavily only (no BRAVE_API_KEY)")
        else:
            print(f"[WebSearch] WARNING: No search provider configured")

    def _pick(self, cost: int = 1):
        """Return (provider, label) — Brave if budget allows, else Tavily."""
        if self.brave and self.brave_usage.can_spend(cost):
            return self.brave, "brave"
        if self.tavily:
            return self.tavily, "tavily"
        return None, None

    def _record(self, label: str, cost: int = 1):
        """Record usage after a successful call."""
        if label == "brave":
            self.brave_usage.spend(cost)
            remaining = self.brave_usage.remaining
            if remaining <= 100:
                print(f"[WebSearch] Brave budget low: {remaining} calls remaining this month")

    # ── Public interface (mirrors TavilyProvider exactly) ─────────────

    async def search_ticker_batch(self, tickers: list,
                                  focus: str = "analyst_ratings_news") -> dict:
        provider, label = self._pick(1)
        if not provider:
            return {}
        result = await provider.search_ticker_batch(tickers, focus)
        if not result.get("error"):
            self._record(label, 1)
        return result

    async def enrich_tickers_batched(self, tickers: list) -> dict:
        batch_count = min(2, (len(tickers[:12]) + 5) // 6)
        provider, label = self._pick(batch_count)
        if not provider:
            return {}
        result = await provider.enrich_tickers_batched(tickers)
        if not isinstance(result, Exception) and result:
            self._record(label, batch_count)
        return result

    async def get_market_news(self, topic: str = "stock market today") -> dict:
        provider, label = self._pick(1)
        if not provider:
            return {"topic": topic, "article_count": 0, "summary": "", "articles": []}
        result = await provider.get_market_news(topic)
        if not result.get("error"):
            self._record(label, 1)
        return result

    async def get_ticker_news_sentiment(self, ticker: str) -> dict:
        provider, label = self._pick(1)
        if not provider:
            return {"ticker": ticker, "article_count": 0, "summary": "",
                    "sentiment_label": "Neutral", "articles": []}
        result = await provider.get_ticker_news_sentiment(ticker)
        if not result.get("error"):
            self._record(label, 1)
        return result

    def search_budget_status(self) -> dict:
        """Return current budget status for diagnostics."""
        return {
            "brave": self.brave_usage.status() if self.brave else None,
            "tavily_available": self.tavily is not None,
            "active_provider": "brave" if (self.brave and self.brave_usage.can_spend()) else
                               "tavily" if self.tavily else "none",
        }
