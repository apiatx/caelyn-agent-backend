"""
Cross-Asset Ranking Engine for cross-market scans.
Runs BEFORE Claude — pure math, no AI calls.

Solves the "crypto floods everything" problem by:
1. Extracting individual candidates from each asset class
2. Scoring within each class independently (intra-class normalization)
3. Normalizing across classes (0-100 scale)
4. Applying hard filters (market cap floor, liquidity minimum)
5. Enforcing multi-factor confluence (3/5 minimum)
6. Applying macro regime penalty (risk-off = penalize speculative assets)
7. Enforcing asset-class quotas (at least 1 stock + 1 commodity if available)
8. Logging everything for diagnostics
"""

import json
from typing import Optional


STOCK_MCAP_FLOOR = 500_000_000
CRYPTO_MCAP_FLOOR = 100_000_000
STOCK_VOLUME_FLOOR = 1_000_000
CRYPTO_VOLUME_FLOOR = 50_000_000

MAJOR_COMMODITIES = {
    "gold", "silver", "platinum", "palladium", "copper",
    "oil", "crude", "natural gas", "wti", "brent",
    "wheat", "corn", "soybeans", "sugar", "coffee", "cotton",
    "GLD", "SLV", "GDX", "GDXJ", "COPX", "PPLT",
    "XLE", "XOP", "OIH", "UNG", "USO", "URA",
    "DBA", "CORN", "WEAT", "SOYB", "MOO", "COW",
}


def rank_cross_market(stock_data: dict, crypto_data: dict,
                      commodity_data: dict, macro_data: dict) -> dict:
    debug = {
        "asset_classes_pulled": [],
        "candidates_per_class": {},
        "filter_rejections": {"stocks": [], "crypto": [], "commodities": []},
        "macro_regime": "unknown",
        "regime_penalty_applied": False,
        "quota_adjustments": [],
        "selection_reasons": {},
    }

    macro_regime = _detect_macro_regime(macro_data)
    debug["macro_regime"] = macro_regime

    stocks = _extract_stock_candidates(stock_data, debug)
    cryptos = _extract_crypto_candidates(crypto_data, debug)
    commodities = _extract_commodity_candidates(commodity_data, debug)

    if stocks: debug["asset_classes_pulled"].append("stocks")
    if cryptos: debug["asset_classes_pulled"].append("crypto")
    if commodities: debug["asset_classes_pulled"].append("commodities")

    debug["candidates_per_class"] = {
        "stocks_raw": len(stocks),
        "crypto_raw": len(cryptos),
        "commodities_raw": len(commodities),
    }

    stocks = _apply_hard_filters(stocks, "stock", debug)
    cryptos = _apply_hard_filters(cryptos, "crypto", debug)

    _score_candidates(stocks, "stock")
    _score_candidates(cryptos, "crypto")
    _score_candidates(commodities, "commodity")

    _normalize_within_class(stocks)
    _normalize_within_class(cryptos)
    _normalize_within_class(commodities)

    if macro_regime == "risk_off":
        debug["regime_penalty_applied"] = True
        _apply_regime_penalty(stocks, cryptos, commodities, macro_regime)

    stocks = [c for c in stocks if c["factors_met"] >= 3]
    cryptos = [c for c in cryptos if c["factors_met"] >= 3]

    debug["candidates_per_class"]["stocks_after_filter"] = len(stocks)
    debug["candidates_per_class"]["crypto_after_filter"] = len(cryptos)
    debug["candidates_per_class"]["commodities_after_filter"] = len(commodities)

    final = _assemble_final_ranking(stocks, cryptos, commodities, debug)

    for c in final:
        debug["selection_reasons"][c["symbol"]] = {
            "asset_class": c["asset_class"],
            "normalized_score": round(c["normalized_score"], 1),
            "factors_met": c["factors_met"],
            "factor_detail": c.get("factor_detail", {}),
        }

    print(f"[CROSS-RANKER] Regime: {macro_regime} | "
          f"Stocks: {debug['candidates_per_class'].get('stocks_raw', 0)}→{debug['candidates_per_class'].get('stocks_after_filter', 0)} | "
          f"Crypto: {debug['candidates_per_class'].get('crypto_raw', 0)}→{debug['candidates_per_class'].get('crypto_after_filter', 0)} | "
          f"Commodities: {debug['candidates_per_class'].get('commodities_after_filter', 0)} | "
          f"Final picks: {len(final)}")

    for c in final:
        print(f"  → {c['symbol']} ({c['asset_class']}): score={c['normalized_score']:.1f}, "
              f"factors={c['factors_met']}/5, {c.get('factor_detail', {})}")

    return {
        "ranked_candidates": final,
        "ranking_debug": debug,
    }


def _detect_macro_regime(macro_data: dict) -> str:
    if not isinstance(macro_data, dict) or "error" in macro_data:
        return "unknown"

    fg = macro_data.get("fear_greed_index") or {}
    if isinstance(fg, dict):
        value = fg.get("value") or fg.get("score")
        if value is not None:
            try:
                v = int(value)
                if v <= 25:
                    return "risk_off"
                elif v <= 40:
                    return "cautious"
                elif v >= 70:
                    return "risk_on"
                else:
                    return "neutral"
            except (ValueError, TypeError):
                pass

    fred = macro_data.get("fred_economic_data") or {}
    if isinstance(fred, dict):
        vix = fred.get("vix") or fred.get("VIX")
        if vix is not None:
            try:
                vix_val = float(vix)
                if vix_val > 30:
                    return "risk_off"
                elif vix_val > 22:
                    return "cautious"
            except (ValueError, TypeError):
                pass

    return "neutral"


def _extract_stock_candidates(stock_data: dict, debug: dict) -> list:
    if not isinstance(stock_data, dict) or "error" in stock_data:
        return []

    candidates = []
    enriched = stock_data.get("enriched_data") or {}
    top_trending = stock_data.get("top_trending") or []
    source_map = {}
    for item in top_trending:
        if isinstance(item, dict):
            t = item.get("ticker", "")
            source_map[t] = {
                "source_count": item.get("source_count", 1),
                "sources": item.get("sources", []),
            }

    if isinstance(enriched, dict):
        for ticker, info in enriched.items():
            if not isinstance(info, dict):
                continue
            mcap = _parse_num(info.get("market_cap") or info.get("marketCap"))
            volume = _parse_num(info.get("avg_volume") or info.get("volume") or info.get("avgVolume"))
            src = source_map.get(ticker, {})
            candidates.append({
                "symbol": ticker,
                "asset_class": "stock",
                "market_cap": mcap,
                "volume": volume,
                "price_change_pct": _parse_pct(info.get("change") or info.get("changesPercentage")),
                "pe_ratio": _parse_num(info.get("pe_ratio") or info.get("pe")),
                "revenue_growth": _parse_pct(info.get("revenue_growth") or info.get("revenueGrowth")),
                "analyst_rating": info.get("analyst_rating") or info.get("analystRating"),
                "price_target_upside": _parse_pct(info.get("upside_downside") or info.get("upside")),
                "beta": _parse_num(info.get("beta")),
                "source_count": src.get("source_count", 1),
                "trending_sources": src.get("sources", []),
                "raw_data": info,
            })

    for item in top_trending:
        if isinstance(item, dict):
            t = item.get("ticker", "")
            if t and t not in enriched and isinstance(enriched, dict):
                candidates.append({
                    "symbol": t,
                    "asset_class": "stock",
                    "market_cap": None,
                    "volume": None,
                    "price_change_pct": None,
                    "source_count": item.get("source_count", 1),
                    "trending_sources": item.get("sources", []),
                    "raw_data": {},
                })

    return candidates


def _extract_crypto_candidates(crypto_data: dict, debug: dict) -> list:
    if not isinstance(crypto_data, dict) or "error" in crypto_data:
        return []

    seen = {}

    cg = crypto_data.get("cg_dashboard") or {}
    if isinstance(cg, dict):
        top_coins = cg.get("top_coins") or []
        for coin in top_coins:
            if not isinstance(coin, dict):
                continue
            symbol = (coin.get("symbol") or "").upper()
            if not symbol:
                continue
            seen[symbol] = {
                "symbol": symbol,
                "asset_class": "crypto",
                "name": coin.get("name", symbol),
                "market_cap": _parse_num(coin.get("market_cap")),
                "volume": _parse_num(coin.get("total_volume")),
                "price_change_pct": _parse_num(coin.get("price_change_percentage_24h")),
                "price_change_7d": _parse_num(coin.get("price_change_percentage_7d_in_currency")),
                "market_cap_rank": _parse_num(coin.get("market_cap_rank")),
                "source_count": 1,
                "sources": ["CoinGecko"],
                "raw_data": {k: v for k, v in coin.items()
                             if k in ("current_price", "market_cap", "total_volume",
                                      "price_change_percentage_24h", "ath_change_percentage")},
            }

        trending = cg.get("trending") or {}
        trending_coins = trending.get("coins") or []
        for item in trending_coins:
            coin = item.get("item") or item if isinstance(item, dict) else {}
            symbol = (coin.get("symbol") or "").upper()
            if symbol and symbol in seen:
                seen[symbol]["source_count"] += 1
                seen[symbol]["sources"].append("CoinGecko Trending")
            elif symbol:
                seen[symbol] = {
                    "symbol": symbol,
                    "asset_class": "crypto",
                    "name": coin.get("name", symbol),
                    "market_cap": _parse_num(coin.get("market_cap") or coin.get("data", {}).get("market_cap")),
                    "volume": _parse_num(coin.get("total_volume") or coin.get("data", {}).get("total_volume")),
                    "price_change_pct": _parse_num(coin.get("data", {}).get("price_change_percentage_24h", {}).get("usd")) if isinstance(coin.get("data", {}).get("price_change_percentage_24h"), dict) else _parse_num(coin.get("price_change_percentage_24h")),
                    "source_count": 1,
                    "sources": ["CoinGecko Trending"],
                    "raw_data": {},
                }

        gl = cg.get("gainers_losers") or {}
        for gainer in (gl.get("gainers") or [])[:5]:
            if isinstance(gainer, dict):
                sym = (gainer.get("symbol") or "").upper()
                if sym and sym in seen:
                    seen[sym]["source_count"] += 1
                    seen[sym]["sources"].append("CoinGecko Top Gainer")

    cmc = crypto_data.get("cmc_dashboard") or {}
    if isinstance(cmc, dict):
        for coin in (cmc.get("most_visited") or []):
            if not isinstance(coin, dict):
                continue
            symbol = (coin.get("symbol") or "").upper()
            if not symbol:
                continue
            quote = (coin.get("quote") or {}).get("USD") or {}
            if symbol in seen:
                seen[symbol]["source_count"] += 1
                seen[symbol]["sources"].append("CMC Most Visited")
                if not seen[symbol].get("volume") and quote.get("volume_24h"):
                    seen[symbol]["volume"] = _parse_num(quote["volume_24h"])
            else:
                seen[symbol] = {
                    "symbol": symbol,
                    "asset_class": "crypto",
                    "name": coin.get("name", symbol),
                    "market_cap": _parse_num(quote.get("market_cap")),
                    "volume": _parse_num(quote.get("volume_24h")),
                    "price_change_pct": _parse_num(quote.get("percent_change_24h")),
                    "source_count": 1,
                    "sources": ["CMC Most Visited"],
                    "raw_data": {},
                }

        for coin in (cmc.get("trending") or []):
            if isinstance(coin, dict):
                sym = (coin.get("symbol") or "").upper()
                if sym in seen:
                    seen[sym]["source_count"] += 1
                    seen[sym]["sources"].append("CMC Trending")
                elif sym:
                    quote = (coin.get("quote") or {}).get("USD") or {}
                    seen[sym] = {
                        "symbol": sym,
                        "asset_class": "crypto",
                        "name": coin.get("name", sym),
                        "market_cap": _parse_num(quote.get("market_cap")),
                        "volume": _parse_num(quote.get("volume_24h")),
                        "price_change_pct": _parse_num(quote.get("percent_change_24h")),
                        "source_count": 1,
                        "sources": ["CMC Trending"],
                        "raw_data": {},
                    }

    return list(seen.values())


def _extract_commodity_candidates(commodity_data: dict, debug: dict) -> list:
    if not isinstance(commodity_data, dict) or "error" in commodity_data:
        return []

    candidates = []
    prices = commodity_data.get("commodity_prices") or {}

    if isinstance(prices, dict):
        all_commodities = prices.get("all_commodities") or []
        for item in all_commodities:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol") or item.get("ticker") or ""
            name = item.get("name") or symbol
            price = _parse_num(item.get("price"))
            change_pct = _parse_num(item.get("changesPercentage") or item.get("change_pct"))
            candidates.append({
                "symbol": symbol,
                "asset_class": "commodity",
                "name": name,
                "price": price,
                "price_change_pct": change_pct,
                "volume": None,
                "market_cap": None,
                "is_major": symbol.upper() in MAJOR_COMMODITIES or any(m.lower() in name.lower() for m in ["gold", "silver", "oil", "crude", "gas", "copper", "wheat", "corn"]),
                "raw_data": item,
            })

        for etf_key in ["energy_etfs", "metals_etfs", "agriculture_etfs"]:
            etfs = prices.get(etf_key) or {}
            if isinstance(etfs, dict):
                for symbol, data in etfs.items():
                    if not isinstance(data, dict):
                        continue
                    candidates.append({
                        "symbol": symbol,
                        "asset_class": "commodity",
                        "name": data.get("name") or data.get("companyName") or symbol,
                        "price": _parse_num(data.get("price")),
                        "price_change_pct": _parse_num(data.get("changesPercentage") or data.get("change_pct")),
                        "volume": _parse_num(data.get("volume")),
                        "market_cap": _parse_num(data.get("marketCap")),
                        "is_major": symbol.upper() in MAJOR_COMMODITIES,
                        "raw_data": data,
                    })

        key_commodities = prices.get("key_commodities") or {}
        if isinstance(key_commodities, dict):
            for symbol, data in key_commodities.items():
                if not isinstance(data, dict):
                    continue
                existing = [c for c in candidates if c["symbol"] == symbol]
                if existing:
                    continue
                candidates.append({
                    "symbol": symbol,
                    "asset_class": "commodity",
                    "name": data.get("name") or symbol,
                    "price": _parse_num(data.get("price")),
                    "price_change_pct": _parse_num(data.get("changesPercentage") or data.get("change_pct")),
                    "volume": _parse_num(data.get("volume")),
                    "market_cap": None,
                    "is_major": symbol.upper() in MAJOR_COMMODITIES or True,
                    "raw_data": data,
                })

    return candidates


def _apply_hard_filters(candidates: list, asset_type: str, debug: dict) -> list:
    filtered = []
    rejection_key = "stocks" if asset_type == "stock" else "crypto"

    mcap_floor = STOCK_MCAP_FLOOR if asset_type == "stock" else CRYPTO_MCAP_FLOOR
    vol_floor = STOCK_VOLUME_FLOOR if asset_type == "stock" else CRYPTO_VOLUME_FLOOR

    for c in candidates:
        mcap = c.get("market_cap")
        vol = c.get("volume")

        if mcap is not None and mcap < mcap_floor:
            debug["filter_rejections"][rejection_key].append(
                f"{c['symbol']}: mcap ${mcap/1e6:.0f}M < ${mcap_floor/1e6:.0f}M floor"
            )
            continue

        if vol is not None and vol < vol_floor:
            debug["filter_rejections"][rejection_key].append(
                f"{c['symbol']}: volume {vol:,.0f} < {vol_floor:,.0f} floor"
            )
            continue

        if mcap is None and vol is None:
            c["_unknown_fundamentals"] = True
            c["_penalty"] = 0.5
            debug["filter_rejections"][rejection_key].append(
                f"{c['symbol']}: missing mcap+volume, penalized 50%"
            )

        filtered.append(c)

    return filtered


def _score_candidates(candidates: list, asset_type: str):
    for c in candidates:
        factors = {}
        met = 0

        sc = c.get("source_count") or 0
        if asset_type == "stock":
            factors["social_momentum"] = min(sc / 3.0, 1.0) if sc else 0
        elif asset_type == "crypto":
            factors["social_momentum"] = min(sc / 3.0, 1.0) if sc else 0
        else:
            factors["social_momentum"] = 0.3

        pct = c.get("price_change_pct")
        if pct is not None:
            if asset_type == "stock":
                if 2 <= pct <= 25:
                    factors["technical"] = min(pct / 10.0, 1.0)
                elif 0 < pct < 2:
                    factors["technical"] = pct / 5.0
                else:
                    factors["technical"] = 0
            elif asset_type == "crypto":
                if 3 <= abs(pct) <= 30:
                    factors["technical"] = min(abs(pct) / 15.0, 1.0)
                elif 0 < abs(pct) < 3:
                    factors["technical"] = abs(pct) / 8.0
                else:
                    factors["technical"] = 0
            else:
                if abs(pct or 0) > 0.5:
                    factors["technical"] = min(abs(pct) / 3.0, 1.0)
                else:
                    factors["technical"] = 0
        else:
            factors["technical"] = 0

        if asset_type == "stock":
            has_catalyst = bool(c.get("analyst_rating") or c.get("price_target_upside"))
            factors["catalyst"] = 0.8 if has_catalyst else 0
        elif asset_type == "crypto":
            has_catalyst = (sc or 0) >= 2
            factors["catalyst"] = 0.7 if has_catalyst else 0
        else:
            factors["catalyst"] = 0.5

        if asset_type == "commodity":
            factors["sector_alignment"] = 0.7 if c.get("is_major") else 0.3
        else:
            factors["sector_alignment"] = 0.5

        vol = c.get("volume")
        mcap = c.get("market_cap")
        if asset_type == "stock":
            if vol and vol > 5_000_000:
                factors["liquidity"] = 1.0
            elif vol and vol > STOCK_VOLUME_FLOOR:
                factors["liquidity"] = 0.6
            elif mcap and mcap > 2e9:
                factors["liquidity"] = 0.7
            else:
                factors["liquidity"] = 0.3
        elif asset_type == "crypto":
            if vol and vol > 500_000_000:
                factors["liquidity"] = 1.0
            elif vol and vol > CRYPTO_VOLUME_FLOOR:
                factors["liquidity"] = 0.6
            elif mcap and mcap > 1e9:
                factors["liquidity"] = 0.5
            else:
                factors["liquidity"] = 0.2
        else:
            factors["liquidity"] = 0.7 if c.get("is_major") else 0.4

        met = sum(1 for v in factors.values() if v >= 0.4)

        raw_score = (
            factors.get("social_momentum", 0) * 0.20 +
            factors.get("technical", 0) * 0.30 +
            factors.get("catalyst", 0) * 0.20 +
            factors.get("sector_alignment", 0) * 0.10 +
            factors.get("liquidity", 0) * 0.20
        )

        if c.get("_penalty"):
            raw_score *= c["_penalty"]
            factors["unknown_data_penalty"] = c["_penalty"]

        c["raw_score"] = raw_score
        c["factors_met"] = met
        c["factor_detail"] = {k: round(v, 2) for k, v in factors.items()}


def _normalize_within_class(candidates: list):
    if not candidates:
        return

    scores = [c["raw_score"] for c in candidates]
    min_s = min(scores)
    max_s = max(scores)
    spread = max_s - min_s if max_s > min_s else 1.0

    for c in candidates:
        c["normalized_score"] = ((c["raw_score"] - min_s) / spread) * 100


def _apply_regime_penalty(stocks: list, cryptos: list, commodities: list, regime: str):
    if regime == "risk_off":
        for c in cryptos:
            mcap = c.get("market_cap")
            if mcap and mcap < 1e9:
                c["normalized_score"] *= 0.5
                c["factor_detail"]["regime_penalty"] = -0.5
            else:
                c["normalized_score"] *= 0.75
                c["factor_detail"]["regime_penalty"] = -0.25

        for c in stocks:
            mcap = c.get("market_cap")
            if mcap and mcap < 2e9:
                c["normalized_score"] *= 0.7
                c["factor_detail"]["regime_penalty"] = -0.3

        for c in commodities:
            name = (c.get("name") or "").lower()
            sym = (c.get("symbol") or "").upper()
            if any(sh in name for sh in ["gold", "silver", "treasury"]) or sym in ("GLD", "SLV", "TLT"):
                c["normalized_score"] *= 1.3
                c["factor_detail"]["regime_bonus"] = 0.3
    elif regime == "cautious":
        for c in cryptos:
            mcap = c.get("market_cap")
            if mcap and mcap < 500e6:
                c["normalized_score"] *= 0.7
                c["factor_detail"]["regime_penalty"] = -0.3


def _assemble_final_ranking(stocks: list, cryptos: list,
                            commodities: list, debug: dict) -> list:
    stocks_sorted = sorted(stocks, key=lambda x: x.get("normalized_score", 0), reverse=True)
    crypto_sorted = sorted(cryptos, key=lambda x: x.get("normalized_score", 0), reverse=True)
    commodity_sorted = sorted(commodities, key=lambda x: x.get("normalized_score", 0), reverse=True)

    final = []
    used_symbols = set()

    if stocks_sorted:
        best = stocks_sorted[0]
        final.append(best)
        used_symbols.add(best["symbol"])
        debug["quota_adjustments"].append(f"Quota: reserved slot for stock {best['symbol']}")

    if commodity_sorted:
        best = commodity_sorted[0]
        if best["symbol"] not in used_symbols:
            final.append(best)
            used_symbols.add(best["symbol"])
            debug["quota_adjustments"].append(f"Quota: reserved slot for commodity {best['symbol']}")

    all_remaining = []
    for c in stocks_sorted:
        if c["symbol"] not in used_symbols:
            all_remaining.append(c)
    for c in crypto_sorted:
        if c["symbol"] not in used_symbols:
            all_remaining.append(c)
    for c in commodity_sorted:
        if c["symbol"] not in used_symbols:
            all_remaining.append(c)

    all_remaining.sort(key=lambda x: x.get("normalized_score", 0), reverse=True)

    slots_left = 7 - len(final)
    for c in all_remaining[:slots_left]:
        final.append(c)
        used_symbols.add(c["symbol"])

    final.sort(key=lambda x: x.get("normalized_score", 0), reverse=True)

    for c in final:
        clean_keys = {"symbol", "asset_class", "name", "market_cap", "volume",
                      "price_change_pct", "normalized_score", "factors_met",
                      "factor_detail", "source_count", "trending_sources", "sources",
                      "price", "is_major", "analyst_rating", "price_target_upside"}
        to_remove = [k for k in c if k not in clean_keys and k != "raw_data"]
        for k in to_remove:
            del c[k]
        c.pop("raw_data", None)

    return final


def _parse_num(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        pass
    try:
        s = str(value).strip().replace(",", "").replace("$", "")
        if s.endswith("T"):
            return float(s[:-1]) * 1e12
        elif s.endswith("B"):
            return float(s[:-1]) * 1e9
        elif s.endswith("M"):
            return float(s[:-1]) * 1e6
        elif s.endswith("K"):
            return float(s[:-1]) * 1e3
        return float(s)
    except (TypeError, ValueError):
        return None


def _parse_pct(value) -> Optional[float]:
    if value is None:
        return None
    try:
        s = str(value).strip().replace(",", "")
        if s.endswith("%"):
            return float(s[:-1])
        v = float(s)
        if abs(v) < 5:
            return v * 100
        return v
    except (TypeError, ValueError):
        return None
