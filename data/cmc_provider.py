"""
CoinMarketCap API provider.
Free Basic plan: 10,000 credits/month, ~333/day.
Credit system: 1 credit per ~100 data points returned.

CMC complements CoinGecko by providing:
- Trending: most-visited pages (retail FOMO signal)
- New listings (recently added coins)
- Gainers/losers with different ranking methodology
- Richer metadata and category tagging
- Cross-reference trending data for momentum confirmation
"""
import httpx
from data.cache import cache

CMC_CACHE_TTL = 120


class CMCProvider:
    BASE_URL = "https://pro-api.coinmarketcap.com"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {
            "X-CMC_PRO_API_KEY": api_key,
            "Accept": "application/json",
        }

    async def _get(self, endpoint: str, params: dict = None) -> dict:
        if params is None:
            params = {}

        cache_key = f"cmc:{endpoint}:{str(sorted(params.items()))[:80]}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.BASE_URL}{endpoint}",
                    params=params,
                    headers=self.headers,
                    timeout=15,
                )
            if resp.status_code == 429:
                print("CMC rate limit hit")
                return {}
            if resp.status_code != 200:
                print(f"CMC error {resp.status_code}: {endpoint}")
                return {}
            data = resp.json()
            cache.set(cache_key, data, CMC_CACHE_TTL)
            return data
        except Exception as e:
            print(f"CMC request failed ({endpoint}): {e}")
            return {}

    async def get_listings_latest(self, limit: int = 50) -> list:
        """
        Top coins by market cap with latest market data.
        Returns: price, volume_24h, volume_change_24h, % changes (1h, 24h, 7d, 30d, 90d),
        market_cap, market_cap_dominance, fully_diluted_market_cap.
        
        CMC's volume_change_24h is unique — shows volume acceleration.
        ~1 credit per 100 data points.
        """
        resp = await self._get("/v1/cryptocurrency/listings/latest", {
            "limit": limit,
            "convert": "USD",
            "sort": "market_cap",
            "sort_dir": "desc",
        })
        return resp.get("data", [])

    async def get_quotes(self, symbols: list) -> dict:
        """
        Get latest quotes for specific coins by symbol.
        ~1 credit per coin.
        """
        if not symbols:
            return {}
        resp = await self._get("/v2/cryptocurrency/quotes/latest", {
            "symbol": ",".join(symbols[:20]),
            "convert": "USD",
        })
        return resp.get("data", {})

    async def get_coin_info(self, symbols: list) -> dict:
        """
        Metadata for specific coins: description, logo, tags, platform,
        date_launched, urls (website, explorer, source_code, etc.).
        Tags are valuable — shows what narratives/categories a coin belongs to.
        ~1 credit per coin.
        """
        if not symbols:
            return {}
        resp = await self._get("/v2/cryptocurrency/info", {
            "symbol": ",".join(symbols[:20]),
        })
        return resp.get("data", {})

    async def get_trending_latest(self) -> list:
        """
        Most searched/trending coins on CMC right now.
        Different signal than CoinGecko trending — CMC has a larger
        mainstream audience, so trending here = broader retail awareness.
        ~1 credit.
        """
        resp = await self._get("/v1/cryptocurrency/trending/latest", {
            "limit": 20,
            "convert": "USD",
        })
        return resp.get("data", [])

    async def get_trending_gainers_losers(self, time_period: str = "24h") -> dict:
        """
        Top gainers and losers by % change.
        time_period: '1h', '24h', '7d', '30d'
        ~1 credit.
        """
        resp = await self._get("/v1/cryptocurrency/trending/gainers-losers", {
            "limit": 20,
            "time_period": time_period,
            "convert": "USD",
        })
        data = resp.get("data", [])
        gainers = [d for d in data if isinstance(d, dict) and d.get("quote", {}).get("USD", {}).get("percent_change_24h", 0) > 0]
        losers = [d for d in data if isinstance(d, dict) and d.get("quote", {}).get("USD", {}).get("percent_change_24h", 0) < 0]
        return {"gainers": gainers, "losers": losers}

    async def get_most_visited(self) -> list:
        """
        Most visited coin pages on CMC.
        This is a UNIQUE signal — shows where retail eyeballs are going.
        High visits + price up = FOMO building.
        High visits + price down = fear/panic watching.
        ~1 credit.
        """
        resp = await self._get("/v1/cryptocurrency/trending/most-visited", {
            "limit": 20,
            "convert": "USD",
        })
        return resp.get("data", [])

    async def get_new_listings(self) -> list:
        """
        Recently listed coins on CMC.
        New listings often see high volatility and can run significantly
        in first weeks. High-risk, high-reward.
        ~1 credit.
        """
        resp = await self._get("/v1/cryptocurrency/listings/new", {
            "limit": 20,
            "convert": "USD",
            "sort": "date_added",
            "sort_dir": "desc",
        })
        return resp.get("data", [])

    async def get_categories(self) -> list:
        """
        All crypto categories with market cap data.
        Shows narrative rotation: AI, DeFi, memes, L2, gaming, etc.
        ~1 credit.
        """
        resp = await self._get("/v1/cryptocurrency/categories", {
            "limit": 30,
        })
        return resp.get("data", [])

    async def get_global_metrics(self) -> dict:
        """
        Global market overview: total market cap, BTC dominance,
        ETH dominance, total volume, active coins, DeFi stats.
        ~1 credit.
        """
        resp = await self._get("/v1/global-metrics/quotes/latest", {
            "convert": "USD",
        })
        return resp.get("data", {})

    async def get_full_dashboard(self) -> dict:
        """
        Pull all free-tier CMC data in parallel.
        ~8-10 credits total.
        """
        import asyncio

        (global_metrics, listings, trending, gainers_losers,
         most_visited, new_listings, categories) = await asyncio.gather(
            self.get_global_metrics(),
            self.get_listings_latest(50),
            self.get_trending_latest(),
            self.get_trending_gainers_losers("24h"),
            self.get_most_visited(),
            self.get_new_listings(),
            self.get_categories(),
            return_exceptions=True,
        )

        return {
            "global_metrics": global_metrics if not isinstance(global_metrics, Exception) else {},
            "listings": listings if not isinstance(listings, Exception) else [],
            "trending": trending if not isinstance(trending, Exception) else [],
            "gainers_losers": gainers_losers if not isinstance(gainers_losers, Exception) else {},
            "most_visited": most_visited if not isinstance(most_visited, Exception) else [],
            "new_listings": new_listings if not isinstance(new_listings, Exception) else [],
            "categories": categories if not isinstance(categories, Exception) else [],
        }
