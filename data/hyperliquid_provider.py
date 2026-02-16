import asyncio
import time

import httpx

from data.cache import cache

HL_CACHE_TTL = 60
HL_FUNDING_HISTORY_CACHE_TTL = 300


class HyperliquidProvider:
    BASE_URL = "https://api.hyperliquid.xyz/info"

    async def _post(self, payload: dict, cache_key: str = None, ttl: int = HL_CACHE_TTL):
        if cache_key:
            cached = cache.get(cache_key)
            if cached is not None:
                return cached

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(
                    self.BASE_URL,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
            if resp.status_code != 200:
                print(f"[HYPERLIQUID] Error {resp.status_code}: {payload.get('type', 'unknown')}")
                return []
            data = resp.json()
            if cache_key:
                cache.set(cache_key, data, ttl)
            return data
        except Exception as e:
            print(f"[HYPERLIQUID] Request failed: {e}")
            return []

    async def get_all_perps(self) -> dict:
        data = await self._post(
            {"type": "metaAndAssetCtxs"},
            cache_key="hl:all_perps",
            ttl=HL_CACHE_TTL,
        )

        if not data or not isinstance(data, list) or len(data) < 2:
            return {"universe": [], "assets": []}

        universe = data[0].get("universe", []) if isinstance(data[0], dict) else []
        asset_contexts = data[1] if isinstance(data[1], list) else []

        assets = []
        for i, ctx in enumerate(asset_contexts):
            if i >= len(universe):
                break

            coin_info = universe[i]
            coin_name = coin_info.get("name", "")

            funding_rate = float(ctx.get("funding", "0") or "0")
            open_interest = float(ctx.get("openInterest", "0") or "0")
            mark_price = float(ctx.get("markPx", "0") or "0")
            oracle_price = float(ctx.get("oraclePx", "0") or "0")
            volume_24h = float(ctx.get("dayNtlVlm", "0") or "0")
            prev_day_price = float(ctx.get("prevDayPx", "0") or "0")
            premium = float(ctx.get("premium", "0") or "0")

            price_change_24h = 0
            if prev_day_price > 0 and mark_price > 0:
                price_change_24h = round((mark_price - prev_day_price) / prev_day_price * 100, 2)

            oi_usd = open_interest * mark_price if mark_price > 0 else 0

            assets.append({
                "coin": coin_name,
                "mark_price": round(mark_price, 6),
                "oracle_price": round(oracle_price, 6),
                "funding_rate": round(funding_rate, 8),
                "funding_rate_annualized": round(funding_rate * 8 * 365 * 100, 2),
                "open_interest": round(open_interest, 2),
                "open_interest_usd": round(oi_usd, 0),
                "volume_24h_usd": round(volume_24h, 0),
                "price_change_24h_pct": price_change_24h,
                "premium": round(premium, 6),
                "max_leverage": coin_info.get("maxLeverage", 0),
            })

        return {"universe": universe, "assets": assets}

    async def get_funding_analysis(self) -> dict:
        perp_data = await self.get_all_perps()
        assets = perp_data.get("assets", [])

        if not assets:
            return {}

        active_assets = [a for a in assets if a["volume_24h_usd"] > 100000]

        if not active_assets:
            return {"error": "No active assets found"}

        sorted_by_funding = sorted(active_assets, key=lambda x: x["funding_rate"], reverse=True)

        crowded_longs = []
        for a in sorted_by_funding[:15]:
            if a["funding_rate"] > 0.0001:
                signal = "EXTREME crowded longs — high liquidation risk" if a["funding_rate"] > 0.0003 else "Elevated long bias"
                crowded_longs.append({
                    "coin": a["coin"],
                    "funding_rate": a["funding_rate"],
                    "funding_annualized": a["funding_rate_annualized"],
                    "open_interest_usd": a["open_interest_usd"],
                    "volume_24h_usd": a["volume_24h_usd"],
                    "price_change_24h": a["price_change_24h_pct"],
                    "signal": signal,
                })

        squeeze_candidates = []
        for a in sorted_by_funding[-15:]:
            if a["funding_rate"] < -0.0001:
                signal = "EXTREME short crowding — HIGH squeeze probability" if a["funding_rate"] < -0.0003 else "Short bias — squeeze potential"

                if a["price_change_24h_pct"] > 2:
                    signal = "ACTIVE SQUEEZE — negative funding + price rising"

                squeeze_candidates.append({
                    "coin": a["coin"],
                    "funding_rate": a["funding_rate"],
                    "funding_annualized": a["funding_rate_annualized"],
                    "open_interest_usd": a["open_interest_usd"],
                    "volume_24h_usd": a["volume_24h_usd"],
                    "price_change_24h": a["price_change_24h_pct"],
                    "signal": signal,
                })

        squeeze_candidates.reverse()

        divergences = []
        for a in active_assets:
            if a["price_change_24h_pct"] > 3 and a["funding_rate"] < -0.00005:
                divergences.append({
                    "coin": a["coin"],
                    "funding_rate": a["funding_rate"],
                    "price_change_24h": a["price_change_24h_pct"],
                    "type": "BULLISH_DIVERGENCE",
                    "signal": f"Price up {a['price_change_24h_pct']}% but shorts still paying — squeeze fuel remains",
                })
            elif a["price_change_24h_pct"] < -3 and a["funding_rate"] > 0.0001:
                divergences.append({
                    "coin": a["coin"],
                    "funding_rate": a["funding_rate"],
                    "price_change_24h": a["price_change_24h_pct"],
                    "type": "BEARISH_DIVERGENCE",
                    "signal": f"Price down {a['price_change_24h_pct']}% but longs still crowded — more liquidations likely",
                })

        avg_funding = sum(a["funding_rate"] for a in active_assets) / len(active_assets)
        total_oi = sum(a["open_interest_usd"] for a in active_assets)
        total_volume = sum(a["volume_24h_usd"] for a in active_assets)

        if avg_funding > 0.00015:
            market_bias = "Strong long bias — market is leveraged bullish"
        elif avg_funding > 0.00005:
            market_bias = "Mild long bias — healthy uptrend positioning"
        elif avg_funding < -0.00015:
            market_bias = "Strong short bias — contrarian bullish signal"
        elif avg_funding < -0.00005:
            market_bias = "Mild short bias — cautious market"
        else:
            market_bias = "Neutral — no crowding, trend likely sustainable"

        top_by_oi = sorted(active_assets, key=lambda x: x["open_interest_usd"], reverse=True)[:10]
        top_oi_summary = [{
            "coin": a["coin"],
            "open_interest_usd": a["open_interest_usd"],
            "volume_24h_usd": a["volume_24h_usd"],
            "funding_rate": a["funding_rate"],
            "price_change_24h": a["price_change_24h_pct"],
        } for a in top_by_oi]

        top_gainers = sorted(active_assets, key=lambda x: x["price_change_24h_pct"], reverse=True)[:10]
        top_losers = sorted(active_assets, key=lambda x: x["price_change_24h_pct"])[:10]

        return {
            "market_summary": {
                "total_assets_tracked": len(active_assets),
                "avg_funding_rate": round(avg_funding, 8),
                "avg_funding_annualized": round(avg_funding * 8 * 365 * 100, 2),
                "market_bias": market_bias,
                "total_open_interest_usd": round(total_oi, 0),
                "total_volume_24h_usd": round(total_volume, 0),
            },
            "crowded_longs": crowded_longs[:10],
            "squeeze_candidates": squeeze_candidates[:10],
            "funding_divergences": divergences[:10],
            "top_by_open_interest": top_oi_summary,
            "top_gainers": [{
                "coin": a["coin"],
                "price_change_24h": a["price_change_24h_pct"],
                "funding_rate": a["funding_rate"],
                "volume_24h_usd": a["volume_24h_usd"],
            } for a in top_gainers],
            "top_losers": [{
                "coin": a["coin"],
                "price_change_24h": a["price_change_24h_pct"],
                "funding_rate": a["funding_rate"],
                "volume_24h_usd": a["volume_24h_usd"],
            } for a in top_losers],
        }

    async def get_funding_history(self, coin: str, hours_back: int = 72) -> dict:
        start_time = int((time.time() - (hours_back * 3600)) * 1000)

        data = await self._post(
            {
                "type": "fundingHistory",
                "coin": coin.upper(),
                "startTime": start_time,
            },
            cache_key=f"hl:funding_history:{coin}:{hours_back}",
            ttl=HL_FUNDING_HISTORY_CACHE_TTL,
        )

        if not isinstance(data, list):
            return {}

        rates = [float(entry.get("fundingRate", "0") or "0") for entry in data]

        if not rates:
            return {}

        avg_recent = sum(rates[-5:]) / 5 if len(rates) >= 5 else sum(rates) / max(len(rates), 1)
        avg_early = sum(rates[:5]) / 5 if len(rates) >= 10 else avg_recent

        if len(rates) > 10 and avg_recent > avg_early:
            trend = "increasing"
        elif len(rates) > 10 and avg_recent < avg_early:
            trend = "decreasing"
        else:
            trend = "stable"

        return {
            "coin": coin.upper(),
            "data_points": len(rates),
            "hours_covered": hours_back,
            "current_rate": rates[-1] if rates else 0,
            "avg_rate": round(sum(rates) / len(rates), 8),
            "max_rate": round(max(rates), 8),
            "min_rate": round(min(rates), 8),
            "trend": trend,
            "recent_rates": [round(r, 8) for r in rates[-12:]],
        }

    async def get_crypto_dashboard(self) -> dict:
        funding_analysis = await self.get_funding_analysis()

        btc_history, eth_history = await asyncio.gather(
            self.get_funding_history("BTC", hours_back=72),
            self.get_funding_history("ETH", hours_back=72),
            return_exceptions=True,
        )

        return {
            "source": "Hyperliquid (largest on-chain perp DEX)",
            "funding_analysis": funding_analysis,
            "btc_funding_trend": btc_history if not isinstance(btc_history, Exception) else {},
            "eth_funding_trend": eth_history if not isinstance(eth_history, Exception) else {},
        }
