"""
altFINS API provider for crypto technical analysis data.
Provides: 90+ technical indicators, trend scores, chart patterns,
candlestick patterns, signals, support/resistance per coin.

Real API endpoints (discovered from Swagger):
  Base: https://altfins.com
  Signals: GET /api/v1/public/signals-feed
  Signal keys: GET /api/v1/public/signals-feed/signal-keys
  Screener: GET /api/v1/nonauth/marketData/screener
  Coins list: GET /api/v1/nonauth/marketData/screener/coins
"""
import asyncio
import httpx
from data.cache import cache

ALTFINS_CACHE_TTL = 600
ALTFINS_SIGNALS_CACHE_TTL = 900

SCREENER_VALUE_TYPES = [
    "MARKET_CAP", "DOLLAR_VOLUME",
    "PRICE_CHANGE_1D", "PRICE_CHANGE_1W", "PRICE_CHANGE_1M", "PRICE_CHANGE_3M",
    "SMA10", "SMA20", "SMA50", "SMA200",
    "RSI14", "MACD",
    "HIGH", "LOW",
]

SIGNAL_KEYS_BULLISH = [
    ("golden_cross", "SIGNALS_SUMMARY_SMA_50_200.TXT", "BULLISH"),
    ("macd_bullish", "SIGNALS_SUMMARY_MACD_SL.TXT", "BULLISH"),
    ("ema_12_50_bullish", "SIGNALS_SUMMARY_EMA_12_50.TXT", "BULLISH"),
    ("oversold_momentum", "SIGNALS_SUMMARY_OVERSOLD_OVERBOUGHT_MOMENTUM.TXT", "BULLISH"),
    ("support_approaching", "SUPPORT_RESISTANCE_APPROACHING.TXT", "BULLISH"),
    ("resistance_breakout", "SUPPORT_RESISTANCE_BREAKOUT.TXT", "BULLISH"),
    ("trend_upgrade", "SIGNALS_SUMMARY_SHORT_TERM_TREND_UPGRADE_DOWNGRADE.TXT", "BULLISH"),
    ("momentum_uptrend", "MOMENTUM_UP_DOWN_TREND.TXT", "BULLISH"),
]

SIGNAL_KEYS_BEARISH = [
    ("death_cross", "SIGNALS_SUMMARY_SMA_50_200.TXT", "BEARISH"),
    ("overbought_momentum", "SIGNALS_SUMMARY_OVERSOLD_OVERBOUGHT_MOMENTUM.TXT", "BEARISH"),
    ("support_breakdown", "SUPPORT_RESISTANCE_BREAKOUT.TXT", "BEARISH"),
    ("trend_downgrade", "SIGNALS_SUMMARY_SHORT_TERM_TREND_UPGRADE_DOWNGRADE.TXT", "BEARISH"),
]


class AltFINSProvider:
    BASE_URL = "https://altfins.com"

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def _get(self, endpoint: str, params: dict = None, cache_key: str = None, ttl: int = ALTFINS_CACHE_TTL) -> dict | list:
        if cache_key:
            cached = cache.get(cache_key)
            if cached is not None:
                return cached

        headers = {
            "Accept": "application/json",
            "X-API-KEY": self.api_key,
        }

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(
                    f"{self.BASE_URL}{endpoint}",
                    params=params or {},
                    headers=headers,
                )

            if resp.status_code == 401:
                print(f"[ALTFINS] Authentication failed — check API key")
                return []
            if resp.status_code == 429:
                print(f"[ALTFINS] Rate limited")
                return []
            if resp.status_code != 200:
                print(f"[ALTFINS] Error {resp.status_code}: {endpoint}")
                return []

            data = resp.json()
            if cache_key:
                cache.set(cache_key, data, ttl)
            return data

        except Exception as e:
            print(f"[ALTFINS] Request failed ({endpoint}): {e}")
            return []

    async def get_coin_analytics(self, symbol: str, interval: str = "DAILY") -> dict:
        """
        Get full screener data for a specific coin.
        Returns: price, market cap, SMAs, RSI, MACD, volume, performance.

        Intervals: MINUTES15, HOURLY, HOURS4, HOURS12, DAILY
        """
        interval_map = {
            "1d": "DAILY", "4h": "HOURS4", "1h": "HOURLY",
            "15m": "MINUTES15", "12h": "HOURS12",
        }
        ti = interval_map.get(interval, interval)

        data = await self._get(
            "/api/v1/nonauth/marketData/screener",
            params={
                "symbols": symbol.upper(),
                "valueTypeIds": ",".join(SCREENER_VALUE_TYPES),
                "timeInterval": ti,
                "page": 0,
                "size": 1,
            },
            cache_key=f"altfins:analytics:{symbol}:{ti}",
            ttl=ALTFINS_CACHE_TTL,
        )

        if not data or not isinstance(data, dict):
            return {}

        formatted = data.get("formattedValues", [])
        raw_values = data.get("values", [])
        headers_list = data.get("headerNames", [])
        value_ids = data.get("valueTypeIds", SCREENER_VALUE_TYPES)

        if not formatted and not raw_values:
            return {}

        result = {"symbol": symbol.upper(), "interval": ti}

        if raw_values:
            entry = raw_values[0] if isinstance(raw_values, list) and raw_values else {}
            vals = entry.get("values", [])
            for i, vid in enumerate(value_ids):
                if i < len(vals):
                    result[vid.lower()] = vals[i]

        if formatted:
            entry = formatted[0] if isinstance(formatted, list) and formatted else {}
            vals = entry.get("values", [])
            fmt = {}
            for i, vid in enumerate(value_ids):
                if i < len(vals):
                    fmt[vid.lower()] = vals[i]
            result["formatted"] = fmt
            if entry.get("symbolUrlName"):
                result["url"] = f"https://altfins.com/crypto-screener/{entry['symbolUrlName']}"

        return result

    async def get_signals_feed(self, signal_key: str, trend: str = "BULLISH", limit: int = 15) -> list:
        """
        Get recent signals from the signals feed.
        Returns coins that recently triggered a specific signal.
        """
        data = await self._get(
            "/api/v1/public/signals-feed",
            params={
                "signalKey": signal_key,
                "trend": trend,
                "page": 0,
                "size": limit,
            },
            cache_key=f"altfins:signals:{signal_key}:{trend}:{limit}",
            ttl=ALTFINS_SIGNALS_CACHE_TTL,
        )

        if isinstance(data, dict):
            return data.get("content", [])
        return data if isinstance(data, list) else []

    async def get_crypto_scanner_data(self) -> dict:
        """
        Main method for the crypto scanner.
        Pulls the most actionable bullish + bearish signals.
        Cached for 15 minutes.
        """
        results = {}

        for name, signal_key, trend in SIGNAL_KEYS_BULLISH:
            try:
                results[name] = await asyncio.wait_for(
                    self.get_signals_feed(signal_key, trend, limit=10),
                    timeout=10.0,
                )
            except Exception as e:
                print(f"[ALTFINS] {name} failed: {e}")
                results[name] = []
            await asyncio.sleep(2.5)

        for name, signal_key, trend in SIGNAL_KEYS_BEARISH:
            try:
                results[name] = await asyncio.wait_for(
                    self.get_signals_feed(signal_key, trend, limit=10),
                    timeout=10.0,
                )
            except Exception as e:
                print(f"[ALTFINS] {name} failed: {e}")
                results[name] = []
            await asyncio.sleep(2.5)

        coin_signal_count = {}
        for signal_name, coins in results.items():
            if not isinstance(coins, list):
                continue
            for coin in coins:
                symbol = coin.get("symbol", "")
                if symbol:
                    if symbol not in coin_signal_count:
                        coin_signal_count[symbol] = {
                            "symbol": symbol,
                            "name": coin.get("symbolName", ""),
                            "signals": [],
                            "last_price": coin.get("lastPrice", ""),
                            "market_cap": coin.get("marketCap", ""),
                            "price_change": coin.get("priceChange", ""),
                        }
                    coin_signal_count[symbol]["signals"].append(signal_name)

        multi_signal = sorted(
            coin_signal_count.values(),
            key=lambda x: len(x["signals"]),
            reverse=True,
        )

        return {
            "source": "altFINS",
            "signals": results,
            "multi_signal_coins": multi_signal[:15],
            "signal_summary": {
                name: len(coins) if isinstance(coins, list) else 0
                for name, coins in results.items()
            },
        }

    async def get_coin_deep_dive(self, symbol: str) -> dict:
        """
        Full deep dive on a single coin — daily + 4h screener data + recent signals.
        """
        daily_task = self.get_coin_analytics(symbol, "DAILY")
        four_hour_task = self.get_coin_analytics(symbol, "HOURS4")
        signals_task = self.get_signals_feed(
            "SIGNALS_SUMMARY_SMA_50_200.TXT", "BULLISH", limit=50
        )

        daily, four_hour, all_signals = await asyncio.gather(
            daily_task, four_hour_task, signals_task,
            return_exceptions=True,
        )

        coin_signals = []
        if isinstance(all_signals, list):
            coin_signals = [s for s in all_signals if s.get("symbol", "").upper() == symbol.upper()]

        return {
            "symbol": symbol.upper(),
            "daily": daily if not isinstance(daily, Exception) else {},
            "four_hour": four_hour if not isinstance(four_hour, Exception) else {},
            "recent_signals": coin_signals[:5],
        }
