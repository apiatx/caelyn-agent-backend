"""
Unified web search provider: Perplexity (primary) → Brave (fallback) → Tavily (fallback).

Budget strategy:
  - Perplexity: Pay-as-you-go ($5/1K requests). No monthly cap — tracked for diagnostics.
  - Brave: $5/month free credit = ~1,000 requests. Hard-cap at 950 to avoid charges.
  - Tavily: 1,000/month free. Last-resort fallback.
  - If Perplexity fails (error/timeout), fall through to Brave, then Tavily.
  - Monthly counters reset on the 1st of each month.
"""
from datetime import datetime
from data.perplexity_provider import PerplexityProvider
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
        if self.limit == 0:
            return True  # Unlimited (pay-as-you-go)
        return (self._count + n) <= self.limit

    def spend(self, n: int = 1):
        self._reset_if_new_month()
        self._count += n

    @property
    def remaining(self) -> int:
        self._reset_if_new_month()
        if self.limit == 0:
            return 999999  # Unlimited
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
            "limit": self.limit if self.limit > 0 else "unlimited",
            "remaining": self.remaining if self.limit > 0 else "unlimited",
            "pct": round(self._count / self.limit * 100, 1) if self.limit else 0,
        }


class WebSearchProvider:
    """
    Unified web search router.
    Routes: Perplexity (primary) → Brave (fallback) → Tavily (last resort).
    Exposes the exact same method interface so callers don't need to change.
    On Perplexity failure, automatically falls through to Brave/Tavily.
    """

    def __init__(self, brave_api_key: str = None, tavily_api_key: str = None,
                 perplexity_api_key: str = None):
        self.perplexity = PerplexityProvider(perplexity_api_key) if perplexity_api_key else None
        self.brave = BraveProvider(brave_api_key) if brave_api_key else None
        self.tavily = TavilyProvider(tavily_api_key) if tavily_api_key else None

        self.perplexity_usage = MonthlyUsageTracker(0)  # 0 = unlimited (pay-as-you-go)
        self.brave_usage = MonthlyUsageTracker(BRAVE_MONTHLY_LIMIT)

        if self.perplexity:
            fallbacks = []
            if self.brave:
                fallbacks.append("Brave")
            if self.tavily:
                fallbacks.append("Tavily")
            fallback_str = f", fallbacks: {' → '.join(fallbacks)}" if fallbacks else ""
            print(f"[WebSearch] Perplexity primary (pay-as-you-go){fallback_str}")
        elif self.brave:
            print(f"[WebSearch] Brave primary (950/month cap), Tavily fallback")
        elif self.tavily:
            print(f"[WebSearch] Tavily only (no PERPLEXITY_API_KEY or BRAVE_API_KEY)")
        else:
            print(f"[WebSearch] WARNING: No search provider configured")

    def _pick(self, cost: int = 1):
        """Return (provider, label) — Perplexity first, then Brave if budget allows, else Tavily."""
        if self.perplexity:
            return self.perplexity, "perplexity"
        if self.brave and self.brave_usage.can_spend(cost):
            return self.brave, "brave"
        if self.tavily:
            return self.tavily, "tavily"
        return None, None

    def _fallback(self, failed_label: str, cost: int = 1):
        """Return next provider after a failure, skipping the one that failed."""
        chain = []
        if self.perplexity:
            chain.append((self.perplexity, "perplexity"))
        if self.brave and self.brave_usage.can_spend(cost):
            chain.append((self.brave, "brave"))
        if self.tavily:
            chain.append((self.tavily, "tavily"))

        found_failed = False
        for provider, label in chain:
            if label == failed_label:
                found_failed = True
                continue
            if found_failed:
                return provider, label
        return None, None

    def _record(self, label: str, cost: int = 1):
        """Record usage after a successful call."""
        if label == "perplexity":
            self.perplexity_usage.spend(cost)
        elif label == "brave":
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
        if result.get("error"):
            fb_provider, fb_label = self._fallback(label, 1)
            if fb_provider:
                print(f"[WebSearch] {label} failed, falling back to {fb_label}")
                result = await fb_provider.search_ticker_batch(tickers, focus)
                if not result.get("error"):
                    self._record(fb_label, 1)
                return result
        else:
            self._record(label, 1)
        return result

    async def enrich_tickers_batched(self, tickers: list) -> dict:
        batch_count = min(2, (len(tickers[:12]) + 5) // 6)
        provider, label = self._pick(batch_count)
        if not provider:
            return {}
        result = await provider.enrich_tickers_batched(tickers)
        if isinstance(result, Exception) or not result:
            fb_provider, fb_label = self._fallback(label, batch_count)
            if fb_provider:
                print(f"[WebSearch] {label} failed, falling back to {fb_label}")
                result = await fb_provider.enrich_tickers_batched(tickers)
                if not isinstance(result, Exception) and result:
                    self._record(fb_label, batch_count)
                return result
        else:
            self._record(label, batch_count)
        return result

    async def get_market_news(self, topic: str = "stock market today") -> dict:
        provider, label = self._pick(1)
        if not provider:
            return {"topic": topic, "article_count": 0, "summary": "", "articles": [], "provider_used": "none"}
        result = await provider.get_market_news(topic)
        if result.get("error"):
            err = str(result.get("error", ""))
            status = "unknown"
            if "HTTP" in err:
                status = err.replace("HTTP", "").strip()
            fb_provider, fb_label = self._fallback(label, 1)
            if fb_provider:
                print(f"[WebNews] {label} failed status={status} -> fallback={fb_label}")
                result = await fb_provider.get_market_news(topic)
                if not result.get("error"):
                    self._record(fb_label, 1)
                    if isinstance(result, dict):
                        result["provider_used"] = fb_label
                return result
        else:
            self._record(label, 1)
            if isinstance(result, dict):
                result["provider_used"] = label
        return result

    async def get_ticker_news_sentiment(self, ticker: str, company_name: str = "") -> dict:
        provider, label = self._pick(1)
        if not provider:
            return {"ticker": ticker, "article_count": 0, "summary": "",
                    "sentiment_label": "Neutral", "articles": []}
        result = await provider.get_ticker_news_sentiment(ticker, company_name=company_name)
        if result.get("error"):
            fb_provider, fb_label = self._fallback(label, 1)
            if fb_provider:
                print(f"[WebSearch] {label} failed, falling back to {fb_label}")
                result = await fb_provider.get_ticker_news_sentiment(ticker, company_name=company_name)
                if not result.get("error"):
                    self._record(fb_label, 1)
                return result
        else:
            self._record(label, 1)
        return result

    def search_budget_status(self) -> dict:
        """Return current budget status for diagnostics."""
        return {
            "perplexity": self.perplexity_usage.status() if self.perplexity else None,
            "brave": self.brave_usage.status() if self.brave else None,
            "tavily_available": self.tavily is not None,
            "active_provider": "perplexity" if self.perplexity else
                               "brave" if (self.brave and self.brave_usage.can_spend()) else
                               "tavily" if self.tavily else "none",
        }
