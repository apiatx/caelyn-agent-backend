import json


MAX_TOTAL_CHARS = 80000
MAX_ARRAY_ITEMS = 15
MAX_STRING_LENGTH = 200
MAX_DESCRIPTION_LENGTH = 100


STRIP_FIELDS = {
    "image", "thumb", "small", "large", "logo", "icon",
    "sparkline_in_7d", "sparkline", "roi",
    "localization", "description",
    "links", "repos_url", "homepage", "blockchain_site",
    "official_forum_url", "chat_url", "announcement_url",
    "subreddit_url", "genesis_date", "ico_data",
    "last_updated", "updated_at",
    "platform", "contract_address",
    "urls", "date_added",
    "notice", "tags",
    "slug",
}

COIN_KEEP_FIELDS = {
    "id", "symbol", "name", "market_cap_rank",
    "market_data", "community_data", "developer_data",
    "sentiment_votes_up_percentage", "sentiment_votes_down_percentage",
    "watchlist_portfolio_users", "categories",
}

MARKET_DATA_KEEP = {
    "current_price", "market_cap", "total_volume",
    "price_change_percentage_24h", "price_change_percentage_7d",
    "price_change_percentage_30d", "price_change_percentage_1y",
    "ath", "ath_change_percentage", "atl", "atl_change_percentage",
    "circulating_supply", "total_supply", "max_supply",
    "fully_diluted_valuation",
}


def compress_data(data: dict, scan_type: str = "general") -> dict:
    if not isinstance(data, dict):
        return data

    compressed = {}
    for key, value in data.items():
        compressed[key] = _compress_value(value, key)

    result_str = json.dumps(compressed, default=str)
    if len(result_str) > MAX_TOTAL_CHARS:
        compressed = _aggressive_truncate(compressed, MAX_TOTAL_CHARS)

    return compressed


def _compress_value(value, key=""):
    if value is None:
        return None

    if isinstance(value, str):
        if len(value) > MAX_STRING_LENGTH:
            return value[:MAX_STRING_LENGTH] + "..."
        return value

    if isinstance(value, (int, float, bool)):
        return value

    if isinstance(value, list):
        truncated = value[:MAX_ARRAY_ITEMS]
        return [_compress_value(item, key) for item in truncated]

    if isinstance(value, dict):
        return _compress_dict(value, key)

    return str(value)[:MAX_STRING_LENGTH]


def _compress_dict(d: dict, parent_key: str = "") -> dict:
    result = {}
    for k, v in d.items():
        if k.lower() in STRIP_FIELDS:
            continue

        if parent_key == "deep_dive" and isinstance(v, dict):
            v = _compress_coin_detail(v)

        if k == "market_data" and isinstance(v, dict):
            v = {mk: mv for mk, mv in v.items() if mk in MARKET_DATA_KEEP}
            for mk in list(v.keys()):
                if isinstance(v[mk], dict) and "usd" in v[mk]:
                    v[mk] = v[mk]["usd"]

        if k in ("community_data", "developer_data") and isinstance(v, dict):
            v = {ck: cv for ck, cv in v.items() if cv is not None and cv != 0 and cv != ""}

        result[k] = _compress_value(v, k)

    return result


def _compress_coin_detail(coin: dict) -> dict:
    return {k: v for k, v in coin.items() if k in COIN_KEEP_FIELDS}


def _truncate_value(val):
    if isinstance(val, list):
        cut_to = max(1, len(val) // 2)
        return [_truncate_value(item) for item in val[:cut_to]]
    elif isinstance(val, dict):
        if len(val) > 3:
            cut_to = max(3, len(val) // 2)
            keys = list(val.keys())[:cut_to]
            return {k: val[k] for k in keys}
        return {k: _truncate_value(v) for k, v in val.items()}
    elif isinstance(val, str) and len(val) > 100:
        return val[:100] + "..."
    return val


PROTECTED_KEYS = {
    "two_tier_analysis", "grok_x_analysis", "scan_type",
    "source_summary", "x_market_mood", "total_unique_tickers",
    "ranked_tickers", "orchestration_metadata",
    "ranked_candidates", "ranking_debug",
    "institutional_scoring", "prior_score",
    "data_completeness", "budget_exhausted_at", "social_discipline_flag",
    "regime_context", "position_sizing", "catalyst_components",
    "adjusted_final_score", "regime", "asset_multiplier",
    "creative_discovery_override", "weight_matrix",
    "data_flags", "conviction_validation", "scoring_debug",
    "labels", "conviction_label", "failed_inputs",
    "completeness_penalty", "position_size_guidance", "liquidity_tier",
    "catalyst_present_components", "market_cap_category",
    "recommendation_tier", "scoring_summary",
    "x_twitter_crypto", "x_sentiment", "hl_additional_coins",
    "perps_overview", "perps_squeezes", "perps_crowded_longs",
    "perps_divergences", "perps_top_volume", "perps_top_oi",
    "perps_gainers", "perps_losers", "funding_lookup",
    "top_coins", "dominance", "derivatives",
}


def _aggressive_truncate(data: dict, max_chars: int) -> dict:
    result = json.loads(json.dumps(data, default=str))

    for pass_num in range(20):
        result_str = json.dumps(result, default=str)
        if len(result_str) <= max_chars:
            break

        avg_budget = max_chars // max(len(result), 1)
        truncated_any = False
        for k, v in list(result.items()):
            if k in PROTECTED_KEYS:
                continue
            size = len(json.dumps(v, default=str))
            if size > avg_budget and size > 20:
                result[k] = _truncate_value(v)
                truncated_any = True

        if not truncated_any:
            break

    return result


def compress_for_claude(market_data: dict, category: str) -> dict:
    if not market_data or not isinstance(market_data, dict):
        return market_data

    raw_size = len(json.dumps(market_data, default=str))

    compressors = {
        "best_trades": _compress_best_trades,
        "briefing": _compress_briefing,
        "cross_asset_trending": _compress_cross_asset_trending,
        "trending": _compress_trending,
        "cross_market": _compress_trending,
        "deterministic_screener": _compress_screener,
        "crypto": _compress_crypto,
        "sector_rotation": _compress_sector,
        "macro_outlook": _compress_macro,
    }

    compressor = compressors.get(category)
    if compressor:
        compressed = compressor(market_data)
    else:
        compressed = _compress_generic(market_data)

    compressed_size = len(json.dumps(compressed, default=str))
    compressed["_compression"] = {
        "raw_size": raw_size,
        "compressed_size": compressed_size,
        "ratio": round(raw_size / max(compressed_size, 1), 1),
        "category": category,
    }

    return compressed


def _compress_best_trades(data: dict) -> dict:
    top_trades = data.get("top_trades", [])
    bearish = data.get("bearish_setups", [])

    trade_digest = []
    for t in top_trades[:10]:
        digest = {
            "ticker": t.get("ticker"),
            "price": t.get("price"),
            "technical_score": t.get("technical_score"),
            "confidence_score": t.get("confidence_score"),
            "pattern": t.get("pattern"),
            "setup_type": t.get("setup_type"),
            "signals_stacking": t.get("signals_stacking", []),
            "indicator_signals": t.get("indicator_signals", []),
            "trade_plan": {
                "entry": t.get("entry"),
                "stop": t.get("stop"),
                "targets": t.get("targets"),
                "risk_reward": t.get("risk_reward"),
                "timeframe": t.get("timeframe"),
            },
            "volume_confirmation": t.get("volume_confirmation"),
            "market_cap": t.get("market_cap"),
            "name": t.get("name"),
            "sector": t.get("sector"),
            "pe_ratio": t.get("pe_ratio"),
            "exchange": t.get("exchange"),
            "tradingview_url": t.get("tradingview_url"),
            "edgar": t.get("edgar"),
            "source_screens": t.get("source_screens", []),
        }
        digest = {k: v for k, v in digest.items() if v is not None}
        trade_digest.append(digest)

    bearish_digest = []
    for t in bearish[:3]:
        digest = {
            "ticker": t.get("ticker"),
            "price": t.get("price"),
            "technical_score": t.get("technical_score"),
            "confidence_score": t.get("confidence_score"),
            "pattern": t.get("pattern"),
            "signals_stacking": t.get("signals_stacking", []),
            "trade_plan": {
                "entry": t.get("entry"),
                "stop": t.get("stop"),
                "targets": t.get("targets"),
                "risk_reward": t.get("risk_reward"),
            },
        }
        digest = {k: v for k, v in digest.items() if v is not None}
        bearish_digest.append(digest)

    pattern_counts = {}
    for t in top_trades:
        p = t.get("pattern", "unknown")
        pattern_counts[p] = pattern_counts.get(p, 0) + 1

    avg_score = round(sum(t.get("technical_score", 0) for t in top_trades) / max(len(top_trades), 1), 1)
    avg_rr = []
    for t in top_trades:
        rr = t.get("risk_reward")
        if rr:
            try:
                avg_rr.append(float(str(rr).replace(":", "").replace("R", "").strip().split()[0]) if ":" not in str(rr) else float(str(rr).split(":")[1]))
            except Exception:
                pass

    return {
        "scan_type": "best_trades",
        "display_type": "trades",
        "digest": {
            "total_candidates_scanned": data.get("scan_stats", {}).get("candidates_found", 0),
            "ta_qualified": data.get("scan_stats", {}).get("ta_qualified", 0),
            "avg_technical_score": avg_score,
            "dominant_patterns": pattern_counts,
            "avg_risk_reward": round(sum(avg_rr) / max(len(avg_rr), 1), 2) if avg_rr else None,
        },
        "market_pulse": data.get("market_pulse", {}),
        "top_trades": trade_digest,
        "bearish_setups": bearish_digest,
        "scan_stats": data.get("scan_stats", {}),
        "data_health": data.get("data_health", {}),
        "market_mood_social": data.get("market_mood_social"),
    }


def _compress_briefing(data: dict) -> dict:
    highlights = data.get("pre_computed_highlights", data.get("highlights", {}))

    ranked = []
    for c in data.get("ranked_candidates", [])[:15]:
        entry = {
            "ticker": c.get("ticker"),
            "trade_score": c.get("trade_score"),
            "invest_score": c.get("invest_score"),
            "signal_count": c.get("signal_count"),
            "signal_sources": c.get("signal_sources", []),
        }
        entry = {k: v for k, v in entry.items() if v is not None}
        ranked.append(entry)

    enriched_compact = {}
    for ticker, d in data.get("enriched_data", {}).items():
        compact = {}
        sentiment = d.get("sentiment", {})
        if sentiment and isinstance(sentiment, dict):
            compact["sentiment"] = sentiment.get("sentiment", "unknown")
            compact["bull_pct"] = sentiment.get("bullish_pct")
        overview = d.get("overview", {})
        if overview and isinstance(overview, dict):
            for key in ("market_cap", "pe_ratio", "revenue_growth", "sector"):
                if overview.get(key):
                    compact[key] = overview[key]
        if compact:
            enriched_compact[ticker] = compact

    return {
        "scan_type": "briefing",
        "display_type": "briefing",
        "pre_computed_highlights": highlights,
        "macro_snapshot": data.get("macro_snapshot", {}),
        "news_context": _trim_news(data.get("news_context", {})),
        "total_tickers_detected": data.get("total_tickers_detected", 0),
        "multi_signal_tickers": data.get("multi_signal_tickers", {}),
        "ranked_candidates": ranked,
        "enriched_compact": enriched_compact,
        "fear_greed": data.get("fear_greed", {}),
        "fred_macro": data.get("fred_macro", {}),
        "highlights": highlights,
        "upcoming_earnings": data.get("upcoming_earnings", []),
    }


def _compress_cross_asset_trending(data: dict) -> dict:
    compressed = {}

    KEEP_FIELDS = {
        "scan_type", "instructions", "ranked_candidates", "ranking_debug",
        "grok_shortlist", "grok_available", "social_signal", "edgar",
        "module_status", "candidate_summary", "orchestration_metadata",
        "social_scan_unavailable", "social_scan_notice",
        "light_enrichment", "market_mood_social",
    }
    SKIP_FIELDS = {
        "cross_asset_debug", "_cross_asset_debug",
    }

    _ENRICHED_KEEP = {
        "market_cap", "pe_ratio", "revenue_growth", "eps_growth",
        "analyst_rating", "price_target", "upside_downside", "sector",
        "avg_volume", "beta", "price", "change", "ticker",
        "dividend_yield", "forward_pe", "week_52_high", "week_52_low",
    }

    for key, value in data.items():
        if key in SKIP_FIELDS or key.startswith("_"):
            continue

        if key in KEEP_FIELDS:
            if key == "grok_shortlist" and isinstance(value, dict):
                trimmed_grok = {}
                for gk, gv in value.items():
                    if gk == "equities" and isinstance(gv, dict):
                        trimmed_eq = {}
                        for bucket_name, bucket_items in gv.items():
                            if isinstance(bucket_items, list):
                                trimmed_eq[bucket_name] = bucket_items[:8]
                            else:
                                trimmed_eq[bucket_name] = bucket_items
                        trimmed_grok[gk] = trimmed_eq
                    elif isinstance(gv, list):
                        trimmed_grok[gk] = gv[:10]
                    else:
                        trimmed_grok[gk] = gv
                compressed[key] = trimmed_grok
            elif key == "ranked_candidates" and isinstance(value, list):
                compressed[key] = value[:18]
            elif key == "ranking_debug" and isinstance(value, dict):
                slim_debug = {}
                for dk, dv in value.items():
                    if dk in ("selection_reasons", "macro_regime", "quota_adjustments",
                              "coverage_backfills", "candidates_per_class", "post_score_counts"):
                        slim_debug[dk] = dv
                compressed[key] = slim_debug
            else:
                compressed[key] = value
            continue

        if key == "stock_trending" and isinstance(value, dict):
            enriched = value.get("enriched_data", {})
            compact_enriched = {}
            if isinstance(enriched, dict):
                for ticker, info in list(enriched.items())[:12]:
                    if isinstance(info, dict):
                        compact_enriched[ticker] = {k: v for k, v in info.items() if k in _ENRICHED_KEEP}
            top_trending = value.get("top_trending", [])
            compressed[key] = {
                "top_trending": top_trending[:15] if isinstance(top_trending, list) else [],
                "enriched_data": compact_enriched,
            }
            continue

        if key == "crypto_scanner" and isinstance(value, dict):
            if "error" in value:
                compressed[key] = {"error": value["error"]}
            else:
                compact_crypto = {}
                for ck in ("coingecko_trending", "cmc_trending", "top_coins"):
                    items = value.get(ck, [])
                    if isinstance(items, list) and items:
                        compact_crypto[ck] = items[:8]
                compressed[key] = compact_crypto or {"summary": "no crypto data"}
            continue

        if key == "commodities" and isinstance(value, dict):
            if "error" in value:
                compressed[key] = {"error": value["error"]}
            else:
                compact_comm = {}
                for ck in ("commodity_proxies", "all_commodity_quotes", "commodities", "data"):
                    items = value.get(ck, [])
                    if isinstance(items, list) and items:
                        compact_comm[ck] = items[:10]
                        break
                compressed[key] = compact_comm or {"summary": "no commodity data"}
            continue

        if key == "macro_context" and isinstance(value, dict):
            slim_macro = {}
            for mk in ("fear_greed", "market_summary", "regime", "macro_regime",
                        "treasury_rates", "key_indicators"):
                if mk in value:
                    slim_macro[mk] = value[mk]
            compressed[key] = slim_macro
            continue

        if key == "news_context" and isinstance(value, dict):
            compressed[key] = _trim_news(value)
            continue

    compressed = {k: v for k, v in compressed.items() if v is not None}

    return compressed


def _compress_trending(data: dict) -> dict:
    if "picks" in data or "trending_tickers" in data:
        compressed = {k: v for k, v in data.items()
                     if not k.startswith("_") and k not in ("raw_stock_data", "raw_crypto_data", "raw_commodity_data")}

        x_data = compressed.get("x_social_scan", {})
        if isinstance(x_data, dict) and "trending_tickers" in x_data:
            trimmed_tickers = []
            for t in x_data.get("trending_tickers", [])[:15]:
                trimmed = {
                    "ticker": t.get("ticker"),
                    "sentiment": t.get("sentiment"),
                    "sentiment_score": t.get("sentiment_score"),
                    "mention_intensity": t.get("mention_intensity"),
                    "why_trending": t.get("why_trending"),
                    "catalyst": t.get("catalyst"),
                    "risk_flag": t.get("risk_flag"),
                }
                trimmed = {k: v for k, v in trimmed.items() if v is not None}
                trimmed_tickers.append(trimmed)
            x_data["trending_tickers"] = trimmed_tickers
            for key in ("sector_heat", "contrarian_signals"):
                x_data.pop(key, None)

        return compressed

    return _compress_generic(data)


def _compress_screener(data: dict) -> dict:
    rows = []
    for row in data.get("rows", []):
        clean = {k: v for k, v in row.items()
                if not k.startswith("_") and v is not None}
        clean.pop("missing_fields", None)
        rows.append(clean)

    return {
        "display_type": "screener",
        "screen_name": data.get("screen_name", ""),
        "preset": data.get("preset", ""),
        "explain": data.get("explain", []),
        "top_picks": data.get("top_picks", []),
        "rows": rows,
        "scan_stats": data.get("scan_stats", {}),
        "meta": data.get("meta", {}),
        "market_mood_social": data.get("market_mood_social"),
    }


def _compress_crypto(data: dict) -> dict:
    compressed = {}

    cg_global = data.get("cg_global", {})
    mcap_pct = {}
    if isinstance(cg_global, dict) and "data" in cg_global:
        gd = cg_global["data"]
        mcap_pct = gd.get("market_cap_percentage", {})
        compressed["cg_global"] = {
            "total_market_cap_usd": gd.get("total_market_cap", {}).get("usd"),
            "total_volume_usd": gd.get("total_volume", {}).get("usd"),
            "btc_dominance": mcap_pct.get("btc"),
            "eth_dominance": mcap_pct.get("eth"),
            "market_cap_change_24h": gd.get("market_cap_change_percentage_24h_usd"),
            "active_cryptos": gd.get("active_cryptocurrencies"),
        }
    else:
        compressed["cg_global"] = cg_global

    cmc_g = data.get("cmc_global", {})
    cmc_gd = {}
    if isinstance(cmc_g, dict) and "data" in cmc_g:
        cmc_gd = cmc_g["data"]
        q = cmc_gd.get("quote", {}).get("USD", {})
        compressed["cmc_global"] = {
            "btc_dominance": cmc_gd.get("btc_dominance"),
            "eth_dominance": cmc_gd.get("eth_dominance"),
            "total_market_cap": q.get("total_market_cap"),
            "total_volume_24h": q.get("total_volume_24h"),
            "total_volume_change_24h": q.get("total_volume_24h_yesterday_percentage_change"),
        }
    else:
        compressed["cmc_global"] = cmc_g

    btc_dom = cmc_gd.get("btc_dominance") or mcap_pct.get("btc")
    eth_dom = cmc_gd.get("eth_dominance") or mcap_pct.get("eth")
    compressed["dominance"] = {
        "btc_dominance": round(btc_dom, 2) if btc_dom else None,
        "eth_dominance": round(eth_dom, 2) if eth_dom else None,
    }

    hl_data = data.get("hyperliquid", {})
    funding_lookup = {}
    if isinstance(hl_data, dict) and not hl_data.get("error"):
        funding_analysis = hl_data.get("funding_analysis", hl_data)
        for source_key in ("top_funding", "open_interest_leaders", "volume_leaders",
                           "top_by_open_interest", "crowded_longs", "squeeze_candidates",
                           "funding_divergences", "top_gainers", "top_losers"):
            items = funding_analysis.get(source_key, hl_data.get(source_key, []))
            if isinstance(items, list):
                for item in items:
                    coin = (item.get("coin") or item.get("symbol") or "").upper()
                    if coin and coin not in funding_lookup:
                        funding_lookup[coin] = {
                            "funding_rate": item.get("funding_rate"),
                            "funding_annualized": item.get("funding_rate_annualized") or item.get("funding_annualized"),
                            "open_interest_usd": item.get("open_interest_usd"),
                        }
        market_summary = hl_data.get("market_summary", funding_analysis.get("market_summary", {})) if isinstance(funding_analysis, dict) else {}
        if isinstance(market_summary, dict):
            funding_lookup["_market"] = {
                "avg_funding_rate": market_summary.get("avg_funding_rate"),
                "avg_funding_annualized": market_summary.get("avg_funding_annualized"),
                "market_bias": market_summary.get("market_bias"),
                "total_oi": market_summary.get("total_open_interest_usd"),
                "total_volume": market_summary.get("total_volume_24h_usd"),
            }
    print(f"[CRYPTO_COMPRESS] Built funding lookup for {len(funding_lookup)} coins: {list(funding_lookup.keys())[:10]}")

    top_coins_raw = data.get("cg_top_coins", [])
    if top_coins_raw:
        sample = top_coins_raw[0]
        print(f"[CRYPTO_COMPRESS] CoinGecko sample: {sample.get('symbol')}, keys: {sorted(sample.keys())}")
        for k, v in sample.items():
            if "7d" in k.lower() or "7" in str(k).lower():
                print(f"[CRYPTO_COMPRESS]   {k} = {v}")

    compressed["top_coins"] = []
    for c in (top_coins_raw or [])[:15]:
        symbol = (c.get("symbol") or "").upper()
        hl_funding = funding_lookup.get(symbol, {})
        compressed["top_coins"].append({
            "symbol": symbol,
            "name": c.get("name"),
            "price": c.get("current_price"),
            "change_1h": c.get("price_change_percentage_1h_in_currency"),
            "change_24h": c.get("price_change_percentage_24h"),
            "change_7d": c.get("price_change_percentage_7d_in_currency") or c.get("price_change_percentage_7d") or c.get("price_change_7d"),
            "change_30d": c.get("price_change_percentage_30d_in_currency") or c.get("price_change_percentage_30d") or c.get("price_change_30d"),
            "market_cap": c.get("market_cap"),
            "volume_24h": c.get("total_volume"),
            "mcap_rank": c.get("market_cap_rank"),
            "funding_rate": hl_funding.get("funding_rate"),
            "funding_annualized": hl_funding.get("funding_annualized"),
            "open_interest_usd": hl_funding.get("open_interest_usd"),
        })
    if compressed["top_coins"]:
        print(f"[CRYPTO_COMPRESS] Compressed sample: {compressed['top_coins'][0]}")

    cg_symbols = {(c.get("symbol") or "").upper() for c in compressed["top_coins"]}
    hl_all_coins = {}
    if isinstance(hl_data, dict) and not hl_data.get("error"):
        hl_fa_coins = hl_data.get("funding_analysis", {})
        if isinstance(hl_fa_coins, dict):
            for source_key in ("top_by_open_interest", "top_gainers", "squeeze_candidates", "crowded_longs", "funding_divergences", "top_losers"):
                for item in (hl_fa_coins.get(source_key) or []):
                    coin = (item.get("coin") or "").upper()
                    if coin and coin not in hl_all_coins:
                        hl_all_coins[coin] = item
    compressed["hl_additional_coins"] = sorted([
        {
            "symbol": coin,
            "source": "hyperliquid",
            "price_change_24h": item.get("price_change_24h"),
            "funding_rate": item.get("funding_rate"),
            "funding_annualized": item.get("funding_annualized") or item.get("funding_rate_annualized"),
            "open_interest_usd": item.get("open_interest_usd"),
            "volume_24h_usd": item.get("volume_24h_usd"),
        }
        for coin, item in hl_all_coins.items() if coin not in cg_symbols
    ], key=lambda x: x.get("volume_24h_usd") or 0, reverse=True)[:20]
    print(f"[CRYPTO_COMPRESS] CoinGecko coins: {len(compressed['top_coins'])}, HL-only coins: {len(compressed['hl_additional_coins'])}")

    cmc_listings = data.get("cmc_listings", [])
    compressed["cmc_top"] = [
        {
            "symbol": c.get("symbol"),
            "name": c.get("name"),
            "price": c.get("quote", {}).get("USD", {}).get("price"),
            "change_24h": c.get("quote", {}).get("USD", {}).get("percent_change_24h"),
            "change_7d": c.get("quote", {}).get("USD", {}).get("percent_change_7d"),
            "change_30d": c.get("quote", {}).get("USD", {}).get("percent_change_30d"),
            "volume_24h": c.get("quote", {}).get("USD", {}).get("volume_24h"),
            "volume_change_24h": c.get("quote", {}).get("USD", {}).get("volume_change_24h"),
            "market_cap": c.get("quote", {}).get("USD", {}).get("market_cap"),
        }
        for c in (cmc_listings or [])[:12]
    ]

    compressed["dual_trending"] = data.get("dual_trending", [])
    compressed["high_attention"] = data.get("high_attention", [])

    cg_trending = data.get("cg_trending", {})
    if isinstance(cg_trending, dict):
        coins = cg_trending.get("coins", [])
        compressed["cg_trending"] = [
            {
                "symbol": c.get("item", {}).get("symbol", ""),
                "name": c.get("item", {}).get("name", ""),
                "mcap_rank": c.get("item", {}).get("market_cap_rank"),
                "price_btc": c.get("item", {}).get("price_btc"),
            }
            for c in coins[:10]
        ]
    else:
        compressed["cg_trending"] = []

    cmc_trending = data.get("cmc_trending", [])
    compressed["cmc_trending"] = [
        {"symbol": c.get("symbol"), "name": c.get("name")}
        for c in (cmc_trending or [])[:10]
    ]

    cmc_most_visited = data.get("cmc_most_visited", [])
    compressed["cmc_most_visited"] = [
        {"symbol": c.get("symbol"), "name": c.get("name")}
        for c in (cmc_most_visited or [])[:10]
    ]

    cg_gl = data.get("cg_gainers_losers", {})
    if isinstance(cg_gl, dict):
        compressed["gainers"] = [
            {"symbol": g.get("symbol", "").upper(), "name": g.get("name"), "change_24h": g.get("price_change_percentage_24h")}
            for g in (cg_gl.get("gainers") or [])[:6]
        ]
        compressed["losers"] = [
            {"symbol": g.get("symbol", "").upper(), "name": g.get("name"), "change_24h": g.get("price_change_percentage_24h")}
            for g in (cg_gl.get("losers") or [])[:5]
        ]

    cmc_gl = data.get("cmc_gainers_losers", {})
    if isinstance(cmc_gl, dict):
        compressed["cmc_gainers"] = [
            {"symbol": g.get("symbol"), "change_24h": g.get("quote", {}).get("USD", {}).get("percent_change_24h")}
            for g in (cmc_gl.get("gainers") or [])[:5]
        ]

    compressed["funding_analysis"] = data.get("funding_analysis", {})

    hl_market = funding_lookup.get("_market", {})
    compressed["derivatives"] = {
        "avg_funding_rate": hl_market.get("avg_funding_rate"),
        "avg_funding_annualized": hl_market.get("avg_funding_annualized"),
        "market_bias": hl_market.get("market_bias"),
        "total_open_interest": hl_market.get("total_oi"),
        "total_volume_24h": hl_market.get("total_volume"),
    }

    hl_fa = hl_data.get("funding_analysis", {}) if isinstance(hl_data, dict) else {}
    if not isinstance(hl_fa, dict):
        hl_fa = {}

    compressed["perps_overview"] = {
        "source": "Hyperliquid",
        "market_summary": hl_fa.get("market_summary", {}),
        "btc_funding_trend": hl_data.get("btc_funding_trend", {}) if isinstance(hl_data, dict) else {},
        "eth_funding_trend": hl_data.get("eth_funding_trend", {}) if isinstance(hl_data, dict) else {},
    }

    compressed["perps_top_oi"] = [
        {
            "coin": o.get("coin"),
            "open_interest_usd": o.get("open_interest_usd"),
            "funding_rate": o.get("funding_rate"),
            "price_change_24h": o.get("price_change_24h"),
            "volume_24h_usd": o.get("volume_24h_usd"),
        }
        for o in (hl_fa.get("top_by_open_interest") or [])[:5]
    ]

    compressed["perps_top_volume"] = [
        {
            "coin": g.get("coin"),
            "price_change_24h": g.get("price_change_24h"),
            "funding_rate": g.get("funding_rate"),
            "volume_24h_usd": g.get("volume_24h_usd"),
            "open_interest_usd": g.get("open_interest_usd"),
        }
        for g in sorted(
            hl_fa.get("top_by_open_interest", []),
            key=lambda x: x.get("volume_24h_usd") or 0, reverse=True
        )[:5]
    ]

    compressed["perps_squeezes"] = [
        {
            "coin": s.get("coin"),
            "funding_rate": s.get("funding_rate"),
            "funding_annualized": s.get("funding_annualized") or s.get("funding_rate_annualized"),
            "open_interest_usd": s.get("open_interest_usd"),
            "price_change_24h": s.get("price_change_24h"),
            "signal": s.get("signal"),
        }
        for s in (hl_fa.get("squeeze_candidates") or [])[:8]
    ]

    compressed["perps_crowded_longs"] = [
        {
            "coin": l.get("coin"),
            "funding_rate": l.get("funding_rate"),
            "funding_annualized": l.get("funding_annualized") or l.get("funding_rate_annualized"),
            "open_interest_usd": l.get("open_interest_usd"),
            "price_change_24h": l.get("price_change_24h"),
            "signal": l.get("signal"),
        }
        for l in (hl_fa.get("crowded_longs") or [])[:8]
    ]

    compressed["perps_divergences"] = [
        {
            "coin": d.get("coin"),
            "type": d.get("type"),
            "funding_rate": d.get("funding_rate"),
            "price_change_24h": d.get("price_change_24h"),
            "signal": d.get("signal"),
        }
        for d in (hl_fa.get("funding_divergences") or [])[:5]
    ]

    compressed["perps_gainers"] = (hl_fa.get("top_gainers") or [])[:5]
    compressed["perps_losers"] = (hl_fa.get("top_losers") or [])[:5]

    compressed["volume_acceleration"] = dict(list(data.get("volume_acceleration", {}).items())[:10])

    cg_cats = data.get("cg_categories", [])
    compressed["hot_categories"] = [
        {"name": c.get("name"), "change_24h": c.get("market_cap_change_24h"), "volume_24h": c.get("volume_24h"), "top_coins": c.get("top_3_coins_id", c.get("top_3_coins", []))[:3]}
        for c in (cg_cats or [])[:8]
    ] if cg_cats else [
        {"name": c.get("name"), "title": c.get("title")}
        for c in (data.get("cmc_categories", []) or [])[:8]
    ]

    new_listings = data.get("new_listings", [])
    compressed["new_listings"] = [
        {"symbol": c.get("symbol"), "name": c.get("name"), "date": c.get("date_added", "")[:10]}
        for c in (new_listings or [])[:5]
    ]

    deep = data.get("deep_dive", {})
    if isinstance(deep, dict):
        compressed["deep_dive"] = {}
        for coin_id, coin_data in list(deep.items())[:5]:
            if isinstance(coin_data, dict):
                md = coin_data.get("market_data", {})
                def _usd(field):
                    val = md.get(field, {})
                    return val.get("usd") if isinstance(val, dict) else val
                compressed["deep_dive"][coin_id] = {
                    "symbol": coin_data.get("symbol", "").upper(),
                    "price": _usd("current_price"),
                    "change_24h": md.get("price_change_percentage_24h"),
                    "change_7d": md.get("price_change_percentage_7d"),
                    "change_30d": md.get("price_change_percentage_30d"),
                    "market_cap": _usd("market_cap"),
                    "volume": _usd("total_volume"),
                    "ath": _usd("ath"),
                    "ath_change_pct": md.get("ath_change_percentage", {}).get("usd") if isinstance(md.get("ath_change_percentage"), dict) else md.get("ath_change_percentage"),
                    "circulating_supply": md.get("circulating_supply"),
                    "max_supply": md.get("max_supply"),
                }

    altfins = data.get("altfins", {})
    if isinstance(altfins, dict) and not altfins.get("error"):
        compressed["altfins"] = {}
        for k in ("bullish_signals", "bearish_signals", "breakouts", "top_picks", "signals"):
            v = altfins.get(k)
            if isinstance(v, list):
                compressed["altfins"][k] = v[:5]
            elif v is not None:
                compressed["altfins"][k] = v
        if "summary" in altfins:
            compressed["altfins"]["summary"] = altfins["summary"]

    compressed["fear_greed"] = data.get("fear_greed", {})

    news = data.get("crypto_news", {})
    if isinstance(news, dict):
        articles = news.get("feed", news.get("articles", []))
        if isinstance(articles, list):
            compressed["news"] = []
            for a in articles[:6]:
                if not isinstance(a, dict):
                    continue
                tickers = []
                ts = a.get("ticker_sentiment")
                if isinstance(ts, list):
                    tickers = [t.get("ticker") for t in ts[:3] if isinstance(t, dict)]
                compressed["news"].append({
                    "title": a.get("title"),
                    "sentiment": a.get("overall_sentiment_label"),
                    "tickers": tickers,
                })

    x_data = data.get("x_twitter_crypto", {})
    if isinstance(x_data, dict) and not x_data.get("error") and (x_data.get("trending_tickers") or x_data.get("btc_sentiment")):
        raw_tickers = (x_data.get("trending_tickers") or [])[:10]
        compressed["x_sentiment"] = {
            "btc_sentiment": x_data.get("btc_sentiment", {}),
            "market_mood": x_data.get("market_mood"),
            "top_social_movers": [
                {
                    "symbol": t.get("ticker", t.get("symbol", "")),
                    "social_velocity": t.get("social_velocity", t.get("mention_intensity", "")),
                    "sentiment": t.get("sentiment", ""),
                    "why_trending": t.get("why_trending", ""),
                    "catalyst": t.get("catalyst", ""),
                }
                for t in raw_tickers
            ],
            "narrative_heat": (x_data.get("narrative_heat") or x_data.get("sector_heat") or [])[:5],
            "contrarian_signals": (x_data.get("contrarian_signals") or [])[:3],
            "summary": x_data.get("summary"),
        }
        print(f"[CRYPTO_COMPRESS] X sentiment: {len(compressed['x_sentiment'].get('top_social_movers', []))} social movers")
    else:
        compressed["x_sentiment"] = {}
        print(f"[CRYPTO_COMPRESS] X sentiment: unavailable or error")

    coin_metadata = data.get("coin_metadata", {})
    if isinstance(coin_metadata, dict):
        compressed["coin_metadata"] = {}
        for k, v in list(coin_metadata.items())[:8]:
            if isinstance(v, dict):
                entry = {mk: mv for mk, mv in v.items() if mk in ("symbol", "name", "category", "tags")}
                desc = v.get("description", "")
                if isinstance(desc, str) and desc:
                    entry["description"] = desc[:200]
                compressed["coin_metadata"][k] = entry

    import json as _json
    size = len(_json.dumps(compressed, default=str))
    print(f"[COMPRESS] crypto: {size:,} chars (from raw data)", flush=True)

    return compressed


def _compress_sector(data: dict) -> dict:
    return _compress_generic(data)


def _compress_macro(data: dict) -> dict:
    compressed = dict(data)
    compressed["news_context"] = _trim_news(data.get("news_context", {}))
    return compressed


def _compress_generic(data: dict) -> dict:
    def _clean(obj, depth=0):
        if depth > 5:
            return obj
        if isinstance(obj, dict):
            return {k: _clean(v, depth+1) for k, v in obj.items()
                    if v is not None and not (isinstance(k, str) and k.startswith("_") and k != "_routing")}
        if isinstance(obj, list):
            items = obj[:30] if len(obj) > 30 else obj
            return [_clean(item, depth+1) for item in items if item is not None]
        return obj

    return _clean(data)


def _trim_news(news_context: dict) -> dict:
    if not isinstance(news_context, dict):
        return news_context

    trimmed = {}
    for key, articles in news_context.items():
        if isinstance(articles, list):
            trimmed[key] = []
            for a in articles[:8]:
                if isinstance(a, dict):
                    trimmed[key].append({
                        "title": a.get("title", a.get("headline", "")),
                        "source": a.get("source", ""),
                        "sentiment": a.get("overall_sentiment_label", a.get("sentiment", "")),
                        "tickers": a.get("ticker_sentiment", a.get("tickers", [])),
                    })
                else:
                    trimmed[key].append(a)
        else:
            trimmed[key] = articles

    return trimmed
