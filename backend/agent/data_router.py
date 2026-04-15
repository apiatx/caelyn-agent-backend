"""
DataRouter — all category arms from TradingAgent._gather_data(), extracted verbatim.

The 30 arms whose logic lives here fully use self.data (= agent.data).
The 6 delegated arms call back into the TradingAgent instance for methods
that depend on TradingAgent instance state or other agent methods:
  - prediction_markets  → agent._gather_polymarket_context()
  - cross_asset_trending → agent._gather_cross_asset_trending_data()
  - custom_screen       → agent._gather_custom_screen_data()
  - ai_screener         → agent._extract_screener_filters() + agent.data.run_ai_screener()
  - portfolio_review    → agent.review_watchlist() / agent._extract_tickers()
  - chat                → agent._gather_chat_context()

Arm ORDER is preserved exactly from the original _gather_data() implementation.
Do not reorder without auditing matching behavior first.
"""

from __future__ import annotations

import asyncio

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop


class DataRouter:
    """
    Wraps all _gather_data() category dispatch arms.

    Usage:
        router = DataRouter(agent)
        data   = await router.gather(query_info)
    """

    def __init__(self, agent) -> None:
        # Keep a reference to the full TradingAgent so delegated arms can
        # call back into methods that remain on the agent (see module docstring).
        self.agent = agent
        self.data = agent.data  # convenience alias — same object

    @traceable(name="data_router_gather")
    async def gather(self, query_info: dict) -> dict:
        """
        Route query_info to the correct data-gathering arm.
        Mirrors TradingAgent._gather_data() exactly — ordering preserved.
        """
        category = query_info.get("category", "general")
        filters = query_info.get("filters", {})

        # Inject modules into filters so get_market_news_context can gate
        # expensive calls (mirrors original lines 4719-4723 exactly).
        plan = query_info.get("orchestration_plan", {})
        if plan.get("modules"):
            filters = dict(filters)
            filters["_modules"] = plan["modules"]

        # ── Arms below are in original source order ──────────────────────

        if category == "ticker_analysis":
            tickers = query_info.get("tickers", [])
            results = {}
            _tickers_to_research = tickers[:5]
            if _tickers_to_research:
                _research_sem = asyncio.Semaphore(2)

                async def _research_one(t):
                    async with _research_sem:
                        try:
                            return t, await asyncio.wait_for(
                                self.data.research_ticker(t), timeout=30.0
                            )
                        except asyncio.TimeoutError:
                            print(f"[TICKER_ANALYSIS] research_ticker({t}) timed out after 30s")
                            return t, {"error": "timeout"}
                        except Exception as e:
                            print(f"[TICKER_ANALYSIS] research_ticker({t}) failed: {e}")
                            return t, {"error": str(e)}

                _gather_results = await asyncio.gather(
                    *[_research_one(t) for t in _tickers_to_research]
                )
                for t, data in _gather_results:
                    results[t] = data
            if tickers:
                try:
                    edgar_data = await asyncio.wait_for(
                        self.data.enrich_with_edgar(tickers[:5], mode="insider_focus"),
                        timeout=8.0,
                    )
                    if edgar_data:
                        results["edgar"] = edgar_data
                except Exception as e:
                    print(f"[EDGAR] Freeform enrichment error: {e}")
            return results

        elif category == "market_scan":
            return await self.data.wide_scan_and_rank("market_scan", filters)

        elif category == "dashboard":
            return await self.data.get_dashboard()

        elif category == "investments":
            from data.cache import cache, XAI_THEMATIC_TTL
            _cached_thematic = cache.get("xai_thematic_investments")

            _invest_model = query_info.get("reasoning_model", "agent_collab")

            async def _safe_grok_thematic():
                if _invest_model != "agent_collab":
                    return {}
                if _cached_thematic:
                    return _cached_thematic
                if not self.data.xai:
                    return {}
                try:
                    return await asyncio.wait_for(
                        self.data.xai.get_thematic_conviction_ideas(),
                        timeout=22.0,
                    )
                except Exception as e:
                    print(f"[INVESTMENTS] Grok thematic failed/timed out: {e}")
                    return {}

            grok_result, invest_data = await asyncio.gather(
                _safe_grok_thematic(),
                self.data.wide_scan_and_rank("investments", filters),
                return_exceptions=True,
            )

            grok_thematic = grok_result if isinstance(grok_result, dict) else {}
            if not isinstance(invest_data, dict):
                invest_data = {}

            leaders = grok_thematic.get("thematic_leaders", [])
            print(f"[INVESTMENTS] Grok thematic: {len(leaders)} leaders | Finviz candidates: {len(invest_data.get('candidates', invest_data.get('picks', [])))}")

            if isinstance(invest_data, dict):
                if grok_thematic and not grok_thematic.get("error"):
                    invest_data["grok_thematic"] = grok_thematic

                finviz_tickers = [
                    c.get("ticker") for c in invest_data.get("candidates", invest_data.get("picks", []))[:6]
                    if c.get("ticker")
                ]
                grok_tickers = [
                    t.get("ticker") for t in grok_thematic.get("thematic_leaders", [])
                    if t.get("ticker") and t.get("conviction_tier", 3) <= 2
                ][:6]
                all_enrich_tickers = list(dict.fromkeys(grok_tickers + finviz_tickers))[:8]

                if all_enrich_tickers:
                    try:
                        edgar_data = await asyncio.wait_for(
                            self.data.enrich_with_edgar(all_enrich_tickers, mode="standard"),
                            timeout=10.0,
                        )
                        if edgar_data:
                            invest_data["edgar"] = edgar_data
                            print(f"[EDGAR] Investments enriched: {list(edgar_data.keys())}")
                    except Exception as e:
                        print(f"[EDGAR] Investments enrichment error: {e}")

            return invest_data

        elif category == "fundamentals_scan":
            return await self.data.wide_scan_and_rank("fundamentals_scan", filters)

        elif category == "unusual_volume":
            return await self.data.get_unusual_volume()

        elif category == "oversold":
            return await self.data.get_oversold()

        elif category == "overbought":
            return await self.data.get_overbought()

        elif category == "options_flow":
            return await self.data.get_options_flow()

        elif category == "earnings":
            return await self.data.get_earnings_scan()

        elif category == "macro":
            return await self.data.get_macro_overview()

        elif category == "prediction_markets":
            # DELEGATED — relies on agent._gather_polymarket_context() which
            # uses self.data.polymarket, self.data.web_search, self.data.fear_greed
            # and the _orch_model scoping that exists only on that method.
            return await self.agent._gather_polymarket_context(query_info)

        elif category == "sec_filings":
            tickers = query_info.get("tickers", [])
            if tickers:
                return await self.data.get_sec_filings(tickers[0])
            return {"error": "No ticker specified for SEC filings lookup"}

        elif category == "squeeze":
            return await self.data.wide_scan_and_rank("squeeze", filters)

        elif category == "social_momentum":
            return await self.data.wide_scan_and_rank("social_momentum", filters)

        elif category == "volume_spikes":
            return await self.data.wide_scan_and_rank("volume_spikes", filters)

        elif category == "earnings_catalyst":
            _cat_model = query_info.get("reasoning_model", "agent_collab")
            earnings_data = await self.data.get_earnings_catalyst_watch()
            if not isinstance(earnings_data, dict):
                earnings_data = {}
            if _cat_model in ("agent_collab", "all_agents"):
                catalyst_tasks = []
                if self.data.web_search and getattr(self.data.web_search, "perplexity", None):
                    async def _fetch_catalyst_news():
                        try:
                            return await asyncio.wait_for(
                                self.data.web_search.perplexity.get_market_news(
                                    "upcoming stock market catalysts FDA approvals product launches conferences analyst days IPOs lockup expirations this week"
                                ),
                                timeout=15.0,
                            )
                        except Exception as e:
                            print(f"[CATALYST] Perplexity catalyst news failed: {e}")
                            return {}
                    catalyst_tasks.append(("catalyst_news", _fetch_catalyst_news()))
                if self.data.xai:
                    async def _fetch_x_catalysts():
                        try:
                            return await asyncio.wait_for(
                                self.data.xai.get_batch_sentiment(
                                    list(earnings_data.get("enriched_data", {}).keys())[:5]
                                ),
                                timeout=15.0,
                            )
                        except Exception as e:
                            print(f"[CATALYST] Grok X sentiment failed: {e}")
                            return {}
                    catalyst_tasks.append(("x_catalyst_sentiment", _fetch_x_catalysts()))
                if catalyst_tasks:
                    results = await asyncio.gather(
                        *[t for _, t in catalyst_tasks],
                        return_exceptions=True,
                    )
                    for (key, _), result in zip(catalyst_tasks, results):
                        if not isinstance(result, Exception) and result:
                            earnings_data[key] = result
                            print(f"[CATALYST] Added {key}: {len(str(result)):,} chars")
            return earnings_data

        elif category == "sector_rotation":
            _rot_model = query_info.get("reasoning_model", "agent_collab")
            if _rot_model in ("agent_collab", "all_agents"):
                rotation_data, news_ctx = await asyncio.gather(
                    self.data.get_sector_rotation_with_stages(),
                    self.data.get_market_news_context(
                        modules={"social_sentiment": False, "macro_context": True}
                    ),
                )
                slim_news: dict = {}
                if news_ctx.get("market_news"):
                    slim_news["market_news"] = news_ctx["market_news"][:8]
                if news_ctx.get("market_news_summary"):
                    slim_news["market_news_summary"] = news_ctx["market_news_summary"]
                if news_ctx.get("economic_calendar"):
                    slim_news["economic_calendar"] = news_ctx["economic_calendar"]
                if slim_news:
                    rotation_data["market_news_context"] = slim_news
            else:
                rotation_data = await self.data.get_sector_rotation_with_stages()
            return rotation_data

        elif category == "asymmetric":
            return await self.data.wide_scan_and_rank("asymmetric", filters)

        elif category == "best_trades":
            return await self.data.get_best_trades_scan()

        elif category == "deterministic_screener":
            preset = query_info.get("_screener_preset", "")
            if not preset:
                plan = query_info.get("orchestration_plan", {})
                preset = plan.get("_screener_preset", "value_momentum")
            return await self.data.run_deterministic_screener(preset)

        elif category == "bearish":
            return await self.data.wide_scan_and_rank("bearish", filters)

        elif category == "thematic":
            theme = filters.get("theme", "ai_compute")
            return await self.data.get_thematic_scan(theme)

        elif category == "small_cap_spec":
            return await self.data.wide_scan_and_rank("small_cap_spec", filters)

        elif category == "commodities":
            return await self.data.get_commodities_dashboard()

        elif category == "crypto":
            result = await self.data.get_crypto_scanner()
            if isinstance(result, dict):
                from data.coingecko_provider import get_crypto_tv_symbol
                for key in ("cg_top_coins", "cg_trending", "cmc_trending", "cmc_most_visited", "cmc_listings"):
                    items = result.get(key)
                    if isinstance(items, list):
                        for item in items:
                            if isinstance(item, dict):
                                sym = (item.get("symbol") or "").upper()
                                if sym:
                                    item["tradingview_symbol"] = get_crypto_tv_symbol(sym)
                    elif isinstance(items, dict):
                        coins = items.get("coins", [])
                        for coin in coins:
                            ci = coin.get("item", coin) if isinstance(coin, dict) else {}
                            sym = (ci.get("symbol") or "").upper()
                            if sym:
                                ci["tradingview_symbol"] = get_crypto_tv_symbol(sym)
            return result

        elif category == "cross_asset_trending":
            # DELEGATED — 600+ line method with nonlocal state, Grok cache logic,
            # and wall-clock deadline management that lives on TradingAgent.
            return await self.agent._gather_cross_asset_trending_data(query_info)

        elif category == "trending":
            return await self.data.get_cross_platform_trending()

        elif category == "cross_market":
            return await self.data.get_cross_market_scan()

        elif category == "custom_screen":
            # DELEGATED — mutates self.data.CATEGORY_FILTERS (instance state on data service)
            # and calls self._extract_screener_filters() which is a TradingAgent method.
            return await self.agent._gather_custom_screen_data(query_info)

        elif category == "ai_screener":
            # DELEGATED — calls agent._extract_screener_filters() (TradingAgent method).
            try:
                original_prompt = query_info.get("original_prompt", "")
                filters = self.agent._extract_screener_filters(original_prompt)
                print(f"[AI Screener] Extracted filters: {filters}")
                result = await self.data.run_ai_screener(filters)
                print(f"[AI Screener] Got {result.get('total_results', 0)} results")
                return result
            except Exception as e:
                import traceback
                print(f"[AI Screener] ERROR: {e}")
                traceback.print_exc()
                return {"error": str(e), "filters_applied": {}, "total_results": 0, "results": []}

        elif category in ("briefing", "daily_briefing"):
            briefing_data = await self.data.get_morning_briefing()
            if isinstance(briefing_data, dict):
                briefing_tickers = []
                for scan_key in ["stage2_breakouts", "volume_breakouts", "revenue_leaders"]:
                    for item in (briefing_data.get(scan_key) or [])[:3]:
                        t = item.get("ticker") if isinstance(item, dict) else None
                        if t and t not in briefing_tickers:
                            briefing_tickers.append(t)
                if briefing_tickers[:5]:
                    try:
                        edgar_briefing = await asyncio.wait_for(
                            self.data.enrich_with_edgar(briefing_tickers[:5], mode="standard"),
                            timeout=8.0,
                        )
                        if edgar_briefing:
                            briefing_data["edgar"] = edgar_briefing
                            print(f"[EDGAR] Briefing enriched: {list(edgar_briefing.keys())}")
                    except Exception as e:
                        print(f"[EDGAR] Briefing enrichment error: {e}")
            return briefing_data

        elif category == "portfolio_review":
            # DELEGATED — calls agent.review_watchlist() and agent._extract_tickers(),
            # both TradingAgent methods, and mutates query_info["tv_context"] in place.
            original = query_info.get("original_prompt", "")
            tickers = query_info.get("tickers", [])
            if not tickers:
                tickers = self.agent._extract_tickers(original)
            tv_categories = {}
            if "###" in original:
                import re as _re
                sections = _re.split(r"###\s*", original)
                for section in sections:
                    if not section.strip():
                        continue
                    parts = section.split(",", 1)
                    cat_name = parts[0].strip()
                    if len(parts) > 1:
                        sec_tickers = _re.findall(
                            r"(?:NYSE|NASDAQ|AMEX|ASX|OTC|CRYPTO|MEXC|BINANCE|COINBASE|ARCA|BATS|TSX|TSXV|CSE|EURONEXT|GETTEX|TSE):([A-Z0-9]{2,10})",
                            parts[1].upper(),
                        )
                        if sec_tickers:
                            tv_categories[cat_name] = sec_tickers
            if tv_categories:
                priority_order = []
                for key in tv_categories:
                    kl = key.lower()
                    if any(w in kl for w in ["holding", "individual", "active", "position"]):
                        priority_order = tv_categories[key] + priority_order
                    elif any(w in kl for w in ["1 highest", "highest conviction", "conviction"]):
                        priority_order.extend(tv_categories[key])
                    elif "sold" not in kl:
                        priority_order.extend(tv_categories[key])
                seen = set()
                unique = []
                for t in priority_order:
                    if t not in seen:
                        seen.add(t)
                        unique.append(t)
                tickers = unique
                cat_summary = []
                for cat, cat_tickers in tv_categories.items():
                    if "sold" in cat.lower():
                        cat_summary.append(f"SOLD/EXITED: {', '.join(cat_tickers[:5])}...")
                    else:
                        scanned = [t for t in cat_tickers if t in tickers]
                        cat_summary.append(
                            f"{cat}: {', '.join(cat_tickers[:8])}{'...' if len(cat_tickers) > 8 else ''} ({len(scanned)} scanning)"
                        )
                query_info["tv_context"] = (
                    "User pasted TradingView watchlist with categories:\n"
                    + "\n".join(cat_summary)
                    + f"\nAnalyzing top {len(tickers)} priority tickers (holdings and highest conviction first). "
                    + f"Total unique tickers in export: {len(unique) + len(seen)}."
                )
            else:
                pass  # Use all tickers from CSV — no artificial limit
            csv_p = query_info.get("csv_parsed")
            if csv_p and csv_p.get("rows"):
                import json as _json
                csv_str = _json.dumps(csv_p["rows"], default=str)
                print(f"[CSV] Skipping API calls — sending {len(csv_p['rows'])} rows directly to Claude ({len(csv_str)} chars)")
                return {
                    "csv_direct": True,
                    "csv_parsed": csv_p,
                    "tickers": csv_p["tickers"],
                    "rows": csv_p["rows"],
                    "columns": csv_p["columns"],
                }
            if hasattr(self.agent, "review_watchlist") and len(tickers) >= 3:
                try:
                    return await self.agent.review_watchlist(
                        tickers,
                        csv_parsed=query_info.get("csv_parsed"),
                        reasoning_model=query_info.get("reasoning_model", "claude"),
                    )
                except Exception as e:
                    print(f"[WATCHLIST] review_watchlist failed: {e}, falling back to analyze_portfolio")
            try:
                return await self.data.analyze_portfolio(tickers)
            except Exception as e:
                print(f"[WATCHLIST] analyze_portfolio failed: {e}")
                return {"error": f"Portfolio analysis failed: {str(e)}", "tickers": tickers}

        elif category == "chat":
            # DELEGATED — _gather_chat_context is a TradingAgent method that
            # maintains per-session web search and XAI state.
            return await self.agent._gather_chat_context(
                query_info.get("original_prompt", ""),
                query_info,
            ) or {}

        elif category == "general":
            # Fast path: lightweight context only — fear/greed + macro snapshot.
            # No heavy scans, no candles, no enrichment.
            # Behavior must remain identical to original lines 5119-5142.
            fast_ctx = {}
            try:
                fg = await asyncio.wait_for(
                    self.data.fear_greed.get_fear_greed_index(),
                    timeout=4.0,
                )
                if fg:
                    fast_ctx["fear_greed"] = fg
            except Exception:
                pass
            try:
                macro = await asyncio.wait_for(
                    self.data._build_macro_snapshot(),
                    timeout=5.0,
                )
                if macro:
                    slim = {
                        k: macro[k]
                        for k in ("vix", "fed_funds_rate", "treasury_10y", "regime", "spy", "qqq")
                        if k in macro
                    }
                    fast_ctx["macro_snapshot"] = slim
            except Exception:
                pass
            return fast_ctx

        else:
            # Final catch-all — return empty dict, identical to original line 5144-5145.
            return {}
