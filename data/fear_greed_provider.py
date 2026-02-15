import httpx
from data.cache import cache, FEAR_GREED_TTL


class FearGreedProvider:
    """
    Fetches the CNN Fear & Greed Index.
    
    The index ranges from 0 to 100:
    - 0-25: Extreme Fear (contrarian buy signal)
    - 25-45: Fear
    - 45-55: Neutral
    - 55-75: Greed
    - 75-100: Extreme Greed (contrarian sell signal)
    
    The index is calculated from 7 market indicators:
    1. Market Momentum (S&P 500 vs 125-day avg)
    2. Stock Price Strength (52-week highs vs lows)
    3. Stock Price Breadth (advancing vs declining volume)
    4. Put/Call Ratio
    5. Market Volatility (VIX)
    6. Safe Haven Demand (stocks vs bonds performance)
    7. Junk Bond Demand (yield spread)
    """

    API_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
        "Referer": "https://www.cnn.com/markets/fear-and-greed",
    }

    async def get_fear_greed_index(self) -> dict:
        """
        Get the current Fear & Greed Index value and its components.
        """
        cache_key = "fear_greed:index"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    self.API_URL,
                    headers=self.HEADERS,
                    timeout=10,
                )

            if resp.status_code != 200:
                return await self._fallback_fetch()

            data = resp.json()

            fear_greed = data.get("fear_and_greed", {})
            score = fear_greed.get("score")
            rating = fear_greed.get("rating")
            timestamp = fear_greed.get("timestamp")

            previous_close = data.get("fear_and_greed_historical", {}).get(
                "previousClose", {}
            )
            one_week_ago = data.get("fear_and_greed_historical", {}).get(
                "oneWeekAgo", {}
            )
            one_month_ago = data.get("fear_and_greed_historical", {}).get(
                "oneMonthAgo", {}
            )
            one_year_ago = data.get("fear_and_greed_historical", {}).get(
                "oneYearAgo", {}
            )

            components = {}
            component_keys = [
                "market_momentum_sp500",
                "stock_price_strength",
                "stock_price_breadth",
                "put_call_options",
                "market_volatility_vix",
                "safe_haven_demand",
                "junk_bond_demand",
            ]
            for key in component_keys:
                comp_data = data.get(key, {})
                if comp_data:
                    components[key] = {
                        "score": comp_data.get("score"),
                        "rating": comp_data.get("rating"),
                    }

            result = {
                "current_score": round(float(score), 1) if score else None,
                "current_rating": rating,
                "timestamp": timestamp,
                "signal": self._interpret_score(score),
                "historical": {
                    "previous_close": {
                        "score": previous_close.get("score"),
                        "rating": previous_close.get("rating"),
                    }
                    if previous_close
                    else None,
                    "one_week_ago": {
                        "score": one_week_ago.get("score"),
                        "rating": one_week_ago.get("rating"),
                    }
                    if one_week_ago
                    else None,
                    "one_month_ago": {
                        "score": one_month_ago.get("score"),
                        "rating": one_month_ago.get("rating"),
                    }
                    if one_month_ago
                    else None,
                    "one_year_ago": {
                        "score": one_year_ago.get("score"),
                        "rating": one_year_ago.get("rating"),
                    }
                    if one_year_ago
                    else None,
                },
                "components": components,
                "momentum_shift": self._detect_momentum_shift(
                    score,
                    previous_close.get("score") if previous_close else None,
                    one_week_ago.get("score") if one_week_ago else None,
                ),
            }

            cache.set(cache_key, result, FEAR_GREED_TTL)
            return result
        except Exception as e:
            print(f"Fear & Greed Index error: {e}")
            return await self._fallback_fetch()

    async def _fallback_fetch(self) -> dict:
        """
        Fallback method if the primary API endpoint doesn't work.
        Tries an alternate CNN endpoint.
        """
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    "https://production.dataviz.cnn.io/index/fearandgreed/current",
                    headers=self.HEADERS,
                    timeout=10,
                )

            if resp.status_code != 200:
                return {
                    "current_score": None,
                    "error": "Could not fetch Fear & Greed Index",
                }

            data = resp.json()
            score = data.get("fear_and_greed", {}).get("score")
            rating = data.get("fear_and_greed", {}).get("rating")

            return {
                "current_score": round(float(score), 1) if score else None,
                "current_rating": rating,
                "signal": self._interpret_score(score),
                "historical": None,
                "components": None,
                "momentum_shift": None,
            }
        except Exception as e:
            print(f"Fear & Greed fallback error: {e}")
            return {
                "current_score": None,
                "error": str(e),
            }

    def _interpret_score(self, score) -> str:
        """Interpret the Fear & Greed score for trading context."""
        if score is None:
            return "no data available"
        try:
            score = float(score)
        except (ValueError, TypeError):
            return "no data available"

        if score <= 10:
            return (
                "EXTREME FEAR — Market is in panic mode. Historically this is "
                "one of the best times to buy. Warren Buffett: 'Be greedy when "
                "others are fearful.' Look for oversold quality stocks."
            )
        if score <= 25:
            return (
                "EXTREME FEAR — Strong contrarian buy signal. Markets are "
                "deeply pessimistic. Look for high-quality stocks that have "
                "been unfairly sold off."
            )
        if score <= 40:
            return (
                "FEAR — Market sentiment is negative. Good conditions for "
                "accumulating positions in strong stocks at discount prices."
            )
        if score <= 55:
            return (
                "NEUTRAL — Market is balanced between fear and greed. "
                "No strong contrarian signal. Trade based on individual "
                "stock setups rather than broad sentiment."
            )
        if score <= 70:
            return (
                "GREED — Market is optimistic. Be selective with new "
                "positions. Consider tightening stop losses on existing "
                "positions."
            )
        if score <= 85:
            return (
                "EXTREME GREED — Market is euphoric. High risk of pullback. "
                "Take profits on winners, avoid chasing momentum, and "
                "consider hedging with puts or raising cash."
            )
        return (
            "EXTREME GREED — Market is at peak euphoria. Maximum caution. "
            "Historically this level precedes significant corrections. "
            "Strongly consider taking profits and raising cash positions."
        )

    def _detect_momentum_shift(self, current, previous_close, one_week_ago) -> str:
        """Detect if sentiment is shifting direction."""
        if current is None:
            return "no data"

        try:
            current = float(current)
        except (ValueError, TypeError):
            return "no data"

        shifts = []

        if previous_close is not None:
            try:
                prev = float(previous_close)
                daily_change = current - prev
                if abs(daily_change) > 5:
                    direction = "up" if daily_change > 0 else "down"
                    shifts.append(
                        f"Shifted {direction} {abs(round(daily_change, 1))} points since yesterday"
                    )
            except (ValueError, TypeError):
                pass

        if one_week_ago is not None:
            try:
                week = float(one_week_ago)
                weekly_change = current - week
                if abs(weekly_change) > 10:
                    direction = "more greedy" if weekly_change > 0 else "more fearful"
                    shifts.append(
                        f"Market has become significantly {direction} over the past week "
                        f"({round(weekly_change, 1)} point shift)"
                    )
            except (ValueError, TypeError):
                pass

        if not shifts:
            return "Sentiment relatively stable"

        return ". ".join(shifts)
