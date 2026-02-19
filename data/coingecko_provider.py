import httpx
from data.cache import cache

CRYPTO_CACHE_TTL = 120

CRYPTO_TV_SYMBOLS = {
    "BTC": "BINANCE:BTCUSDT",
    "ETH": "BINANCE:ETHUSDT",
    "SOL": "BINANCE:SOLUSDT",
    "BNB": "BINANCE:BNBUSDT",
    "XRP": "BINANCE:XRPUSDT",
    "ADA": "BINANCE:ADAUSDT",
    "DOGE": "BINANCE:DOGEUSDT",
    "DOT": "BINANCE:DOTUSDT",
    "LINK": "BINANCE:LINKUSDT",
    "AVAX": "BINANCE:AVAXUSDT",
    "MATIC": "BINANCE:MATICUSDT",
    "UNI": "BINANCE:UNIUSDT",
    "ATOM": "BINANCE:ATOMUSDT",
    "LTC": "BINANCE:LTCUSDT",
    "TRX": "BINANCE:TRXUSDT",
    "NEAR": "BINANCE:NEARUSDT",
    "APT": "BINANCE:APTUSDT",
    "OP": "BINANCE:OPUSDT",
    "ARB": "BINANCE:ARBUSDT",
    "SUI": "BINANCE:SUIUSDT",
    "SEI": "BINANCE:SEIUSDT",
    "INJ": "BINANCE:INJUSDT",
    "FET": "BINANCE:FETUSDT",
    "RENDER": "BINANCE:RENDERUSDT",
    "RNDR": "BINANCE:RENDERUSDT",
    "FIL": "BINANCE:FILUSDT",
    "AAVE": "BINANCE:AAVEUSDT",
    "MKR": "BINANCE:MKRUSDT",
    "PEPE": "BINANCE:PEPEUSDT",
    "SHIB": "BINANCE:SHIBUSDT",
    "WIF": "BINANCE:WIFUSDT",
    "BONK": "BINANCE:BONKUSDT",
    "FLOKI": "BINANCE:FLOKIUSDT",
    "STX": "BINANCE:STXUSDT",
    "IMX": "BINANCE:IMXUSDT",
    "GRT": "BINANCE:GRTUSDT",
    "HBAR": "BINANCE:HBARUSDT",
    "VET": "BINANCE:VETUSDT",
    "ALGO": "BINANCE:ALGOUSDT",
    "FTM": "BINANCE:FTMUSDT",
    "SAND": "BINANCE:SANDUSDT",
    "MANA": "BINANCE:MANAUSDT",
    "AXS": "BINANCE:AXSUSDT",
    "CRV": "BINANCE:CRVUSDT",
    "LDO": "BINANCE:LDOUSDT",
    "RUNE": "BINANCE:RUNEUSDT",
    "ENS": "BINANCE:ENSUSDT",
    "COMP": "BINANCE:COMPUSDT",
    "SNX": "BINANCE:SNXUSDT",
    "THETA": "BINANCE:THETAUSDT",
    "EOS": "BINANCE:EOSUSDT",
    "XLM": "BINANCE:XLMUSDT",
    "ICP": "BINANCE:ICPUSDT",
    "TIA": "BINANCE:TIAUSDT",
    "JUP": "BINANCE:JUPUSDT",
    "WLD": "BINANCE:WLDUSDT",
    "TAO": "BINANCE:TAOUSDT",
    "KAS": "KUCOIN:KASUSDT",
    "TON": "OKX:TONUSDT",
}


def get_crypto_tv_symbol(ticker: str) -> str:
    ticker = ticker.upper().strip()
    if ticker in CRYPTO_TV_SYMBOLS:
        return CRYPTO_TV_SYMBOLS[ticker]
    return f"BINANCE:{ticker}USDT"


class CoinGeckoProvider:
    BASE_URL = "https://api.coingecko.com/api/v3"

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def _get(self, endpoint: str, params: dict = None) -> dict | list:
        if params is None:
            params = {}
        params["x_cg_demo_api_key"] = self.api_key

        cache_key = f"coingecko:{endpoint}:{str(sorted(params.items()))[:80]}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.BASE_URL}/{endpoint}",
                    params=params,
                    timeout=10,
                )
            if resp.status_code == 429:
                print("CoinGecko rate limit hit")
                return []
            if resp.status_code != 200:
                print(f"CoinGecko error {resp.status_code}: {endpoint}")
                return []
            data = resp.json()
            cache.set(cache_key, data, CRYPTO_CACHE_TTL)
            return data
        except Exception as e:
            print(f"CoinGecko request failed ({endpoint}): {e}")
            return []

    async def get_top_coins(self, limit: int = 25) -> list:
        return await self._get("coins/markets", {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": limit,
            "page": 1,
            "sparkline": "false",
            "price_change_percentage": "1h,24h,7d,30d",
        })

    async def get_trending(self) -> dict:
        return await self._get("search/trending")

    async def get_coin_detail(self, coin_id: str) -> dict:
        return await self._get(f"coins/{coin_id}", {
            "localization": "false",
            "tickers": "false",
            "market_data": "true",
            "community_data": "true",
            "developer_data": "true",
        })

    async def get_global_market(self) -> dict:
        data = await self._get("global")
        return data.get("data", {}) if isinstance(data, dict) else {}

    async def get_global_defi(self) -> dict:
        data = await self._get("global/decentralized_finance_defi")
        return data.get("data", {}) if isinstance(data, dict) else {}

    async def get_derivatives_tickers(self) -> list:
        return await self._get("derivatives")

    async def get_derivatives_exchange(self, exchange_id: str = "binance_futures") -> dict:
        return await self._get(f"derivatives/exchanges/{exchange_id}", {
            "include_tickers": "all",
        })

    async def get_categories(self) -> list:
        return await self._get("coins/categories", {
            "order": "market_cap_change_24h_desc",
        })

    async def get_top_gainers_losers(self) -> dict:
        coins = await self.get_top_coins(50)
        if not coins:
            return {"gainers": [], "losers": []}

        valid = [c for c in coins if c.get("price_change_percentage_24h") is not None]
        sorted_coins = sorted(valid, key=lambda x: x.get("price_change_percentage_24h", 0), reverse=True)

        gainers = sorted_coins[:10]
        losers = sorted_coins[-10:][::-1]

        return {"gainers": gainers, "losers": losers}

    async def get_crypto_dashboard(self) -> dict:
        import asyncio

        global_data, top_coins, trending, derivatives, categories, gainers_losers = (
            await asyncio.gather(
                self.get_global_market(),
                self.get_top_coins(25),
                self.get_trending(),
                self.get_derivatives_tickers(),
                self.get_categories(),
                self.get_top_gainers_losers(),
                return_exceptions=True,
            )
        )

        return {
            "global_market": global_data if not isinstance(global_data, Exception) else {},
            "top_coins": top_coins if not isinstance(top_coins, Exception) else [],
            "trending": trending if not isinstance(trending, Exception) else {},
            "derivatives": derivatives if not isinstance(derivatives, Exception) else [],
            "categories": categories if not isinstance(categories, Exception) else [],
            "gainers_losers": gainers_losers if not isinstance(gainers_losers, Exception) else {},
        }

    async def get_coin_deep_dive(self, coin_ids: list) -> dict:
        import asyncio
        results = await asyncio.gather(
            *[self.get_coin_detail(cid) for cid in coin_ids[:5]],
            return_exceptions=True,
        )
        enriched = {}
        for coin_id, result in zip(coin_ids[:5], results):
            if not isinstance(result, Exception) and isinstance(result, dict):
                enriched[coin_id] = result
        return enriched
