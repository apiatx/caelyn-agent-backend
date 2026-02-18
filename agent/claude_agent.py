import json
import re
import time
import asyncio

import anthropic
import openai

from agent.data_compressor import compress_data
from agent.institutional_scorer import apply_institutional_scoring
from agent.prompts import SYSTEM_PROMPT, QUERY_CLASSIFIER_PROMPT, ORCHESTRATION_PROMPT, TRENDING_VALIDATION_PROMPT
from data.market_data_service import MarketDataService


class TradingAgent:
    def __init__(self, api_key: str, data_service: MarketDataService, openai_api_key: str = None):
        self.client = anthropic.Anthropic(api_key=api_key, timeout=120.0)
        self.openai_client = openai.OpenAI(api_key=openai_api_key) if openai_api_key else None
        self.data = data_service

    def _build_plan_from_preset(self, preset_intent: str) -> dict:
        profile = self.INTENT_PROFILES.get(preset_intent)
        if not profile:
            return None

        plan = {
            "intent": profile["intent"],
            "asset_classes": list(profile["asset_classes"]),
            "modules": dict(profile["modules"]),
            "risk_framework": profile.get("risk_framework", "neutral"),
            "response_style": profile.get("response_style", "institutional_brief"),
            "priority_depth": profile.get("priority_depth", "medium"),
            "filters": dict(profile.get("filters", {})),
            "tickers": [],
        }
        return plan

    def _refine_plan_with_query(self, base_plan: dict, query: str) -> dict:
        q = query.lower().strip()
        plan = {
            "intent": base_plan["intent"],
            "asset_classes": list(base_plan["asset_classes"]),
            "modules": dict(base_plan["modules"]),
            "risk_framework": base_plan.get("risk_framework", "neutral"),
            "response_style": base_plan.get("response_style", "institutional_brief"),
            "priority_depth": base_plan.get("priority_depth", "medium"),
            "filters": dict(base_plan.get("filters", {})),
            "tickers": list(base_plan.get("tickers", [])),
        }

        if any(w in q for w in ["deep", "detailed", "thorough", "in-depth"]):
            plan["priority_depth"] = "deep"
            plan["response_style"] = "deep_thesis"

        if any(w in q for w in ["quick", "brief", "summary", "tldr"]):
            plan["priority_depth"] = "shallow"
            plan["response_style"] = "institutional_brief"

        if any(w in q for w in ["small cap", "micro cap", "microcap", "small-cap", "under $2b"]):
            plan["filters"]["market_cap_max"] = 2000000000
            plan["risk_framework"] = "asymmetric"

        if "crypto" in q and "crypto" not in plan["asset_classes"]:
            plan["asset_classes"].append("crypto")
        if any(w in q for w in ["stocks", "equities"]) and "equities" not in plan["asset_classes"]:
            plan["asset_classes"].append("equities")
        if any(w in q for w in ["commodities", "gold", "oil", "silver"]) and "commodities" not in plan["asset_classes"]:
            plan["asset_classes"].append("commodities")

        if any(w in q for w in ["twitter", "x sentiment", "social"]):
            plan["modules"]["x_sentiment"] = True
            plan["modules"]["social_sentiment"] = True
        if any(w in q for w in ["earnings", "revenue", "eps"]):
            plan["modules"]["earnings_data"] = True
        if any(w in q for w in ["macro", "fed", "rates", "inflation"]):
            plan["modules"]["macro_context"] = True

        import re
        ticker_pattern = re.findall(r'\b([A-Z]{1,5})\b', query)
        common_words = {
            "I", "A", "AM", "AN", "AS", "AT", "BE", "BY", "DO", "GO",
            "IF", "IN", "IS", "IT", "ME", "MY", "NO", "OF", "ON", "OR",
            "SO", "TO", "UP", "US", "WE", "THE", "AND", "FOR", "ARE",
            "BUT", "NOT", "YOU", "ALL", "CAN", "HAD", "HER", "WAS",
            "ONE", "OUR", "OUT", "HAS", "HIS", "HOW", "ITS", "MAY",
            "NEW", "NOW", "OLD", "SEE", "WAY", "WHO", "DID", "GET",
            "BUY", "SELL", "HOLD", "LONG", "SHORT", "PUT", "CALL",
            "ETF", "IPO", "CEO", "CFO", "EPS", "GDP", "CPI", "FED",
            "SEC", "FDA", "RSI", "SMA", "ATH", "ATL", "YOY", "QOQ",
        }
        real_tickers = [t for t in ticker_pattern if t not in common_words]
        if real_tickers:
            plan["tickers"] = real_tickers
            plan["modules"]["ticker_research"] = True

        return plan

    def _heuristic_fallback_plan(self, prompt: str) -> dict:
        q = prompt.lower().strip()

        import re
        ticker_pattern = re.findall(r'\b([A-Z]{1,5})\b', prompt)
        common_words = {
            "I", "A", "AM", "AN", "AS", "AT", "BE", "BY", "DO", "GO",
            "IF", "IN", "IS", "IT", "ME", "MY", "NO", "OF", "ON", "OR",
            "SO", "TO", "UP", "US", "WE", "THE", "AND", "FOR", "ARE",
            "BUT", "NOT", "YOU", "ALL", "CAN", "HAD", "HER", "WAS",
            "ONE", "OUR", "OUT", "HAS", "HIS", "HOW", "ITS", "MAY",
            "NEW", "NOW", "OLD", "SEE", "WAY", "WHO", "DID", "GET",
            "BUY", "SELL", "HOLD", "LONG", "SHORT", "PUT", "CALL",
            "ETF", "IPO", "CEO", "CFO", "EPS", "GDP", "CPI", "FED",
            "SEC", "FDA", "RSI", "SMA", "ATH", "ATL", "YOY", "QOQ",
            "MACD", "VWAP", "EMA", "EBITDA", "DOJI", "OI", "IV",
        }
        real_tickers = [t for t in ticker_pattern if t not in common_words]

        if real_tickers:
            plan = dict(self.DEFAULT_PLAN)
            plan["intent"] = "deep_dive"
            plan["tickers"] = real_tickers
            plan["modules"] = dict(self.DEFAULT_PLAN["modules"])
            plan["modules"]["ticker_research"] = True
            plan["asset_classes"] = ["equities"]
            print(f"[FALLBACK] Ticker detected ({real_tickers}) → deep_dive")
            return plan

        if any(w in q for w in ["earning", "eps", "revenue", "guidance", "report"]):
            plan = dict(self.DEFAULT_PLAN)
            plan["intent"] = "event_driven"
            plan["modules"] = dict(self.DEFAULT_PLAN["modules"])
            plan["modules"]["earnings_data"] = True
            plan["modules"]["fundamental_validation"] = True
            plan["asset_classes"] = ["equities"]
            print(f"[FALLBACK] Earnings keywords → event_driven")
            return plan

        if any(w in q for w in ["macro", "fed", "rate", "inflation", "gdp", "cpi", "treasury", "yield"]):
            plan = dict(self.DEFAULT_PLAN)
            plan["intent"] = "macro_outlook"
            plan["modules"] = dict(self.DEFAULT_PLAN["modules"])
            plan["modules"]["macro_context"] = True
            plan["modules"]["earnings_data"] = True
            plan["asset_classes"] = ["equities", "commodities", "macro"]
            print(f"[FALLBACK] Macro keywords → macro_outlook")
            return plan

        if any(w in q for w in ["crypto", "bitcoin", "btc", "eth", "altcoin", "defi"]):
            plan = dict(self.DEFAULT_PLAN)
            plan["intent"] = "single_asset_scan"
            plan["modules"] = dict(self.DEFAULT_PLAN["modules"])
            plan["modules"]["x_sentiment"] = True
            plan["modules"]["social_sentiment"] = True
            plan["asset_classes"] = ["crypto"]
            print(f"[FALLBACK] Crypto keywords → single_asset_scan (crypto)")
            return plan

        if any(w in q for w in ["sector", "rotation", "industry"]):
            plan = dict(self.DEFAULT_PLAN)
            plan["intent"] = "sector_rotation"
            plan["modules"] = dict(self.DEFAULT_PLAN["modules"])
            plan["modules"]["macro_context"] = True
            plan["asset_classes"] = ["equities"]
            print(f"[FALLBACK] Sector keywords → sector_rotation")
            return plan

        if any(w in q for w in ["portfolio", "holdings", "my positions", "review my"]):
            plan = dict(self.DEFAULT_PLAN)
            plan["intent"] = "portfolio_review"
            plan["modules"] = dict(self.DEFAULT_PLAN["modules"])
            plan["modules"]["fundamental_validation"] = True
            plan["modules"]["macro_context"] = True
            plan["asset_classes"] = ["equities", "crypto"]
            print(f"[FALLBACK] Portfolio keywords → portfolio_review")
            return plan

        if any(w in q for w in ["brief", "morning", "daily", "overview", "update"]):
            plan = dict(self.DEFAULT_PLAN)
            plan["intent"] = "briefing"
            plan["modules"] = dict(self.DEFAULT_PLAN["modules"])
            plan["asset_classes"] = ["equities", "crypto", "commodities", "macro"]
            print(f"[FALLBACK] Briefing keywords → briefing")
            return plan

        if any(w in q for w in ["short", "bearish", "puts", "downside"]):
            plan = dict(self.DEFAULT_PLAN)
            plan["intent"] = "short_setup"
            plan["modules"] = dict(self.DEFAULT_PLAN["modules"])
            plan["modules"]["technical_scan"] = True
            plan["modules"]["social_sentiment"] = True
            plan["asset_classes"] = ["equities"]
            print(f"[FALLBACK] Bearish keywords → short_setup")
            return plan

        print(f"[FALLBACK] No keyword match → chat (lightweight, no heavy scans)")
        return {
            "intent": "chat",
            "asset_classes": [],
            "modules": {
                "x_sentiment": False,
                "social_sentiment": False,
                "technical_scan": False,
                "fundamental_validation": False,
                "macro_context": False,
                "liquidity_filter": False,
                "earnings_data": False,
                "ticker_research": False,
            },
            "risk_framework": "neutral",
            "response_style": "institutional_brief",
            "priority_depth": "shallow",
            "filters": {},
            "tickers": [],
        }

    async def handle_query(self, user_prompt: str, history: list = None, preset_intent: str = None) -> dict:
        start_time = time.time()
        if history is None:
            history = []
        is_followup = len(history) > 0

        print(f"[AGENT] === NEW REQUEST === (followup={is_followup}, history_turns={len(history)}, preset={preset_intent or 'none'})")
        print(f"[AGENT] Query: {user_prompt[:100]}")

        if is_followup and not self._needs_fresh_data(user_prompt):
            category = "followup"
            market_data = None
            routing_source = "followup"
            routing_confidence = "high"
            print(f"[AGENT] Follow-up detected, skipping data gathering ({time.time() - start_time:.1f}s)")
        elif preset_intent:
            plan = self._build_plan_from_preset(preset_intent)
            if plan is None:
                print(f"[ROUTING] Unknown preset_intent '{preset_intent}', falling back to classifier")
                query_info = await self._orchestrate_with_timeout(user_prompt)
                routing_source = query_info.pop("_routing_source", "heuristic")
                routing_confidence = query_info.pop("_routing_confidence", "low")
            else:
                if user_prompt.strip():
                    plan = self._refine_plan_with_query(plan, user_prompt)
                query_info = self._plan_to_query_info(plan)
                routing_source = "preset"
                routing_confidence = "high"

            query_info["original_prompt"] = user_prompt
            category = query_info.get("category", "general")

            orch_plan = query_info.get("orchestration_plan")
            if orch_plan:
                cross_market_override = self._detect_cross_market(user_prompt.lower().strip())
                if cross_market_override and category != "cross_market":
                    print(f"[AGENT] Cross-market override: {category} → cross_market")
                    category = "cross_market"
                    query_info["category"] = "cross_market"

            print(f"[ROUTING] source={routing_source} | confidence={routing_confidence} | "
                  f"preset={preset_intent} | query={user_prompt[:80]} | "
                  f"category={category} | "
                  f"asset_classes={orch_plan.get('asset_classes') if orch_plan else '?'} | "
                  f"modules={[k for k, v in (orch_plan.get('modules', {}) if orch_plan else {}).items() if v]} | "
                  f"response_style={orch_plan.get('response_style') if orch_plan else '?'}")

            if category == "chat":
                market_data = await self._gather_chat_context(user_prompt, query_info)
                data_size = len(json.dumps(market_data, default=str)) if market_data else 0
                print(f"[AGENT] Chat context gathered: {data_size:,} chars ({time.time() - start_time:.1f}s)")
            else:
                market_data = await self._gather_data_safe(query_info)
                print(f"[AGENT] Data gathered: {len(json.dumps(market_data, default=str)):,} chars ({time.time() - start_time:.1f}s)")
        else:
            query_info = await self._orchestrate_with_timeout(user_prompt)
            routing_source = query_info.pop("_routing_source", "heuristic")
            routing_confidence = query_info.pop("_routing_confidence", "low")
            query_info["original_prompt"] = user_prompt
            category = query_info.get("category", "general")

            plan = query_info.get("orchestration_plan")
            if not plan:
                cross_market_override = self._detect_cross_market(user_prompt.lower().strip())
                if cross_market_override and category != "cross_market":
                    print(f"[AGENT] Cross-market override: {category} → cross_market")
                    category = "cross_market"
                    query_info["category"] = "cross_market"

            print(f"[ROUTING] source={routing_source} | confidence={routing_confidence} | "
                  f"preset=none | query={user_prompt[:80]} | "
                  f"category={category} | "
                  f"asset_classes={plan.get('asset_classes') if plan else '?'} | "
                  f"modules={[k for k, v in (plan.get('modules', {}) if plan else {}).items() if v]} | "
                  f"response_style={plan.get('response_style') if plan else '?'}")

            if category == "chat":
                market_data = await self._gather_chat_context(user_prompt, query_info)
                data_size = len(json.dumps(market_data, default=str)) if market_data else 0
                print(f"[AGENT] Chat context gathered: {data_size:,} chars ({time.time() - start_time:.1f}s)")
            else:
                market_data = await self._gather_data_safe(query_info)
                print(f"[AGENT] Data gathered: {len(json.dumps(market_data, default=str)):,} chars ({time.time() - start_time:.1f}s)")

        SCORING_CATEGORIES = {
            "market_scan", "trending", "investments", "fundamentals_scan",
            "squeeze", "social_momentum", "volume_spikes", "earnings_catalyst",
            "sector_rotation", "asymmetric", "bearish", "thematic",
            "small_cap_spec", "briefing", "crypto", "cross_market",
            "commodities", "dashboard",
        }
        if market_data and isinstance(market_data, dict) and category in SCORING_CATEGORIES:
            try:
                from core.regime_engine import detect_market_regime
                regime_data = detect_market_regime(self.data)
                print(f"[REGIME] Detected: {regime_data.get('regime')} (confidence={regime_data.get('confidence', 0)})")
            except Exception as e:
                print(f"[REGIME] Detection failed, defaulting to neutral: {e}")
                regime_data = {"regime": "neutral", "confidence": 0}
            market_data = apply_institutional_scoring(market_data, regime_data=regime_data)

        raw_response = await self._ask_claude_with_timeout(user_prompt, market_data, history, is_followup=is_followup, category=category)
        print(f"[AGENT] Claude responded: {len(raw_response):,} chars ({time.time() - start_time:.1f}s)")

        result = self._parse_response(raw_response)
        print(f"[AGENT] Response parsed, display_type: {result.get('structured', {}).get('display_type', result.get('type', 'unknown'))} ({time.time() - start_time:.1f}s)")
        return result

    def _needs_fresh_data(self, query: str) -> bool:
        q = query.lower().strip()

        new_scan_triggers = [
            "scan", "screen", "what's trending", "best trades", "macro overview",
            "crypto scan", "sector rotation", "daily briefing", "earnings watch",
            "commodities", "volume spikes", "short squeeze", "show me",
            "run a", "pull up", "find me", "search for", "morning briefing",
            "what's hot", "trending now", "stage 2 breakouts", "best investments",
            "improving fundamentals", "asymmetric only", "social momentum",
            "bearish setups", "small cap spec", "ai/compute", "uranium",
            "crypto scanner", "watchlist review",
            "analyze", "check", "look at", "price action", "how is",
            "what about ticker", "deep dive",
        ]

        for trigger in new_scan_triggers:
            if trigger in q:
                return True

        import re
        ticker_pattern = re.findall(r'\b([A-Z]{1,5})\b', query)
        common_words = {
            "I", "A", "AM", "AN", "AS", "AT", "BE", "BY", "DO", "GO",
            "IF", "IN", "IS", "IT", "ME", "MY", "NO", "OF", "ON", "OR",
            "SO", "TO", "UP", "US", "WE", "THE", "AND", "FOR", "ARE",
            "BUT", "NOT", "YOU", "ALL", "CAN", "HAD", "HER", "WAS",
            "ONE", "OUR", "OUT", "HAS", "HIS", "HOW", "ITS", "MAY",
            "NEW", "NOW", "OLD", "SEE", "WAY", "WHO", "DID", "GET",
            "HIM", "LET", "SAY", "SHE", "TOO", "USE", "BUY", "SELL",
            "HOLD", "LONG", "SHORT", "PUT", "CALL", "ETF", "IPO",
            "CEO", "CFO", "COO", "EPS", "GDP", "CPI", "FED", "SEC",
            "FDA", "RSI", "SMA", "ATH", "ATL", "YOY", "QOQ", "EBITDA",
            "NYSE", "WHAT", "WHICH", "RATE", "WHY", "TELL", "MORE",
            "GIVE", "BEST", "HIGH", "LOW", "TOP", "YES", "THAT", "THIS",
            "THEY", "THEM", "WILL", "WITH", "JUST", "ALSO", "BEEN",
            "LIKE", "MUCH", "WHEN", "ONLY", "VERY", "SURE", "YEAH",
        }
        real_tickers = [t for t in ticker_pattern if t not in common_words]
        if real_tickers:
            return True

        return False

    async def _classify_with_timeout(self, prompt: str) -> dict:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self._classify_query, prompt),
                timeout=10.0,
            )
        except (asyncio.TimeoutError, Exception) as e:
            print(f"[AGENT] Classification failed/timed out: {e}, using keyword fallback")
            return self._keyword_classify(prompt)

    def _keyword_classify(self, query: str) -> dict:
        q = query.lower().strip()

        cross_market = self._detect_cross_market(q)
        if cross_market:
            return cross_market

        scan_keywords = [
            "scan", "screen", "trending", "best trades", "briefing", "watchlist",
            "crypto scan", "macro overview", "sector rotation", "find me",
            "show me", "pull up", "run a", "search for", "morning briefing",
            "what's hot", "trending now", "stage 2 breakouts", "best investments",
            "improving fundamentals", "asymmetric only", "social momentum",
            "bearish setups", "small cap spec", "crypto scanner", "best stocks",
            "top movers", "momentum plays", "short squeeze", "volume spike",
            "earnings watch", "commodities dashboard", "full dashboard",
            "best swing", "swing trades", "swing setups", "best setups",
            "trade setups", "breakout", "what's moving", "daily brief",
            "top picks", "top stocks", "movers today", "analyze my",
            "review my", "portfolio review", "dashboard",
        ]

        conversational_signals = [
            "what do you think", "your opinion", "how would you",
            "why is", "why are", "what's the difference", "should i",
            "would you", "tell me about", "how does", "what happens if",
            "compare", "pros and cons", "risk of", "is it worth",
            "help me understand", "what's your take", "do you like",
            "what would you do", "thoughts on",
            "can you explain", "walk me through",
            "how do i", "when should", "is it too late", "is it a good time",
            "bull case", "bear case", "how risky",
            "is the market", "are we in", "what signals", "your read on",
            "how do you feel", "where do you see",
            "opinion on", "view on",
        ]

        is_conversational = any(signal in q for signal in conversational_signals)
        has_scan_keyword = any(kw in q for kw in scan_keywords)

        if is_conversational and not has_scan_keyword:
            tickers = self._extract_tickers(query)
            if tickers:
                return {"category": "chat", "tickers": tickers}
            return {"category": "chat"}

        sector_scans = {
            "energy sector": "energy", "energy scan": "energy",
            "ai sector": "technology", "ai/compute": "technology", "compute sector": "technology",
            "materials sector": "basic materials", "mining sector": "basic materials",
            "quantum": "technology", "quantum computing": "technology",
            "aerospace": "industrials", "defense sector": "industrials",
            "tech sector": "technology", "technology sector": "technology",
            "finance sector": "financial", "financial sector": "financial", "bank sector": "financial",
            "healthcare sector": "healthcare", "pharma": "healthcare", "biotech": "healthcare",
            "real estate sector": "real estate", "reit": "real estate",
        }
        for trigger, sector in sector_scans.items():
            if trigger in q:
                return {"category": "market_scan", "filters": {"sector": sector}}

        ta_scan_triggers = [
            "bullish breakout", "bearish breakdown", "oversold bounce",
            "overbought warning", "crossover signal", "golden cross", "death cross",
            "ema crossover", "macd crossover", "momentum shift", "momentum inflection",
            "trend status", "trend upgrade", "strong uptrend", "strong downtrend",
            "volume & movers", "volume spike", "unusual volume", "top gainers", "top losers",
            "new local high", "new local low", "pattern breakout",
            "bollinger", "oversold near support", "pullback in uptrend",
            "overbought", "oversold",
        ]
        if any(t in q for t in ta_scan_triggers):
            return {"category": "market_scan"}

        if any(w in q for w in ["news headline", "headline leaders", "dominating the news", "breaking developments"]):
            return {"category": "trending"}
        if any(w in q for w in ["upcoming catalyst", "biggest upcoming", "catalyst calendar", "how should i position"]):
            return {"category": "earnings_catalyst"}

        if any(w in q for w in ["twitter", "x sentiment", "what's x saying", "x/twitter", "x says"]):
            return {"category": "trending"}
        if any(w in q for w in ["trending", "what's trending", "trend", "what's hot", "popular", "buzzing", "what's buzzing"]):
            return {"category": "trending"}

        if any(w in q for w in ["crypto", "bitcoin", "btc", "eth", "solana", "altcoin", "defi", "funding rate"]):
            return {"category": "crypto"}
        if any(w in q for w in ["macro", "fed", "interest rate", "inflation", "gdp", "economy", "dollar"]):
            return {"category": "macro"}
        if any(w in q for w in ["briefing", "morning", "daily brief", "intelligence"]):
            return {"category": "briefing"}
        if any(w in q for w in ["commodity", "commodities", "oil", "gold", "uranium", "copper", "natural gas"]):
            return {"category": "commodities"}
        if any(w in q for w in ["sector", "rotation", "stage 2", "weinstein", "breakout"]):
            return {"category": "sector_rotation"}
        if any(w in q for w in ["squeeze", "short squeeze", "short interest", "short float"]):
            return {"category": "squeeze"}
        if any(w in q for w in ["invest", "long term", "best investment", "hold", "dividend"]):
            return {"category": "investments"}
        if any(w in q for w in ["earnings", "earnings watch", "reporting"]):
            return {"category": "earnings"}
        if any(w in q for w in ["portfolio", "watchlist", "review my"]):
            return {"category": "portfolio_review"}
        if any(w in q for w in ["screen", "screener", "filter", "scan for"]):
            return {"category": "ai_screener"}
        if any(w in q for w in ["bearish", "short", "puts", "downside"]):
            return {"category": "bearish"}
        if any(w in q for w in ["social", "stocktwits", "sentiment", "buzz"]):
            return {"category": "social_momentum"}
        if any(w in q for w in ["volume", "unusual volume", "volume spike"]):
            return {"category": "volume_spikes"}
        if any(w in q for w in ["asymmetric", "risk reward", "r/r"]):
            return {"category": "asymmetric"}
        if any(w in q for w in ["fundamental", "revenue growth", "improving"]):
            return {"category": "fundamentals_scan"}
        if any(w in q for w in ["trade", "best trade", "setup", "swing"]):
            return {"category": "market_scan"}
        return {"category": "market_scan"}

    def _detect_cross_market(self, q: str) -> dict | None:
        trending_intent = ["trending", "what's hot", "what's trending", "buzzing",
                           "what's buzzing", "what's moving", "movers", "momentum",
                           "social momentum", "top picks", "best trades", "best setups",
                           "highest-conviction", "highest conviction"]
        if any(t in q for t in trending_intent):
            return None

        stock_signals = ["stock", "stocks", "equit", "equity", "equities", "s&p", "spy", "nasdaq"]
        crypto_signals = ["crypto", "bitcoin", "btc", "altcoin", "defi"]
        commodity_signals = ["commodit", "oil", "gold", "silver", "copper", "uranium",
                             "natural gas", "metals", "precious metal"]
        broad_signals = ["all markets", "across markets", "every market", "cross market",
                         "all asset", "across asset", "every asset class", "cross asset",
                         "stocks, crypto", "crypto, stock", "stocks and crypto",
                         "crypto and stock"]

        has_stock = any(s in q for s in stock_signals)
        has_crypto = any(s in q for s in crypto_signals)
        has_commodity = any(s in q for s in commodity_signals)
        has_broad = any(s in q for s in broad_signals)

        asset_count = sum([has_stock, has_crypto, has_commodity])

        if has_broad or asset_count >= 2:
            return {"category": "cross_market"}
        return None

    def _extract_tickers(self, query: str) -> list:
        ticker_pattern = re.findall(r'\$?([A-Z]{2,5})\b', query)
        common = {
            "THE", "AND", "FOR", "ARE", "BUT", "NOT", "YOU", "ALL", "CAN",
            "WAS", "ONE", "OUR", "OUT", "HAS", "HOW", "ITS", "MAY", "NEW",
            "NOW", "OLD", "WAY", "WHO", "DID", "GET", "LET", "SAY", "SHE",
            "TOO", "USE", "CEO", "IPO", "ETF", "IMO", "FYI", "JUST", "LIKE",
            "THIS", "THAT", "WITH", "HAVE", "FROM", "BEEN", "WILL", "MORE",
            "WHEN", "SOME", "THAN", "VERY", "WHAT", "OVER", "GOOD", "BACK",
            "ALSO", "INTO", "YOUR", "NEXT", "LONG", "BEST", "BUY", "SELL",
            "HOLD", "SHORT", "PUT", "CALL", "GDP", "CPI", "FED", "SEC",
            "FDA", "RSI", "SMA", "ATH", "ATL", "YOY", "QOQ", "NYSE",
            "GIVE", "HIGH", "LOW", "TOP", "YES", "THEY", "THEM", "MUCH",
            "ONLY", "SURE", "YEAH", "RATE", "TELL", "WHY", "ABOUT",
            "THINK", "WOULD", "SHOULD", "COULD", "STILL", "WORTH",
            "RISK", "TAKE", "PROS", "CONS",
            "MACD", "VWAP", "EMA", "EBITDA", "DOJI", "OI", "IV",
        }
        return [t for t in ticker_pattern if t not in common]

    def _classify_query(self, prompt: str) -> dict:
        if self.openai_client:
            return self._classify_query_openai(prompt)
        return self._classify_query_claude(prompt)

    def _classify_query_openai(self, prompt: str) -> dict:
        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-4o",
                max_tokens=200,
                temperature=0.1,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a query classifier. Reply with ONLY a valid JSON object, nothing else.",
                    },
                    {
                        "role": "user",
                        "content": (
                            f"{QUERY_CLASSIFIER_PROMPT}\n\n"
                            f"User query: {prompt}"
                        ),
                    },
                ],
            )
            text = response.choices[0].message.content.strip()
            text = re.sub(r"```json\s*", "", text)
            text = re.sub(r"```\s*", "", text)
            return json.loads(text)
        except Exception as e:
            print(f"[AGENT] OpenAI classification error: {e}, falling back to keyword classifier")
            return self._keyword_classify(prompt)

    def _classify_query_claude(self, prompt: str) -> dict:
        try:
            response = self.client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=200,
                messages=[
                    {
                        "role": "user",
                        "content": (
                            f"{QUERY_CLASSIFIER_PROMPT}\n\n"
                            f"User query: {prompt}"
                        ),
                    }
                ],
            )
            text = response.content[0].text.strip()
            text = re.sub(r"```json\s*", "", text)
            text = re.sub(r"```\s*", "", text)
            return json.loads(text)
        except Exception as e:
            print(f"[AGENT] Classification API error: {e}")
            return self._keyword_classify(prompt)

    INTENT_PROFILES = {
        "daily_briefing": {
            "intent": "briefing",
            "asset_classes": ["equities", "crypto", "commodities", "macro"],
            "modules": {
                "x_sentiment": True,
                "social_sentiment": True,
                "technical_scan": True,
                "fundamental_validation": True,
                "macro_context": True,
                "liquidity_filter": True,
                "earnings_data": False,
                "ticker_research": False,
            },
            "risk_framework": "neutral",
            "response_style": "institutional_brief",
            "priority_depth": "medium",
        },
        "cross_asset_trending": {
            "intent": "cross_asset_trending",
            "asset_classes": ["equities", "crypto", "commodities"],
            "modules": {
                "x_sentiment": True,
                "social_sentiment": True,
                "technical_scan": True,
                "fundamental_validation": True,
                "macro_context": False,
                "liquidity_filter": False,
                "earnings_data": False,
                "ticker_research": False,
            },
            "risk_framework": "neutral",
            "response_style": "high_conviction_ranked",
            "priority_depth": "medium",
        },
        "microcap_asymmetry": {
            "intent": "cross_asset_trending",
            "asset_classes": ["equities", "crypto"],
            "modules": {
                "x_sentiment": True,
                "social_sentiment": True,
                "technical_scan": True,
                "fundamental_validation": True,
                "macro_context": False,
                "liquidity_filter": False,
                "earnings_data": False,
                "ticker_research": False,
            },
            "filters": {"market_cap_max": 2000000000},
            "risk_framework": "asymmetric",
            "response_style": "deep_thesis",
            "priority_depth": "deep",
        },
        "sector_rotation": {
            "intent": "sector_rotation",
            "asset_classes": ["equities"],
            "modules": {
                "x_sentiment": False,
                "social_sentiment": False,
                "technical_scan": True,
                "fundamental_validation": True,
                "macro_context": True,
                "liquidity_filter": True,
                "earnings_data": False,
                "ticker_research": False,
            },
            "risk_framework": "neutral",
            "response_style": "institutional_brief",
            "priority_depth": "medium",
        },
        "macro_outlook": {
            "intent": "macro_outlook",
            "asset_classes": ["equities", "commodities", "macro"],
            "modules": {
                "x_sentiment": False,
                "social_sentiment": False,
                "technical_scan": False,
                "fundamental_validation": False,
                "macro_context": True,
                "liquidity_filter": False,
                "earnings_data": True,
                "ticker_research": False,
            },
            "risk_framework": "neutral",
            "response_style": "institutional_brief",
            "priority_depth": "deep",
        },
        "earnings_catalyst": {
            "intent": "event_driven",
            "asset_classes": ["equities"],
            "modules": {
                "x_sentiment": True,
                "social_sentiment": True,
                "technical_scan": True,
                "fundamental_validation": True,
                "macro_context": False,
                "liquidity_filter": False,
                "earnings_data": True,
                "ticker_research": False,
            },
            "risk_framework": "neutral",
            "response_style": "high_conviction_ranked",
            "priority_depth": "medium",
        },
        "crypto_scanner": {
            "intent": "single_asset_scan",
            "asset_classes": ["crypto"],
            "modules": {
                "x_sentiment": True,
                "social_sentiment": True,
                "technical_scan": True,
                "fundamental_validation": False,
                "macro_context": False,
                "liquidity_filter": True,
                "earnings_data": False,
                "ticker_research": False,
            },
            "risk_framework": "neutral",
            "response_style": "high_conviction_ranked",
            "priority_depth": "medium",
        },
        "commodity_scan": {
            "intent": "single_asset_scan",
            "asset_classes": ["commodities"],
            "modules": {
                "x_sentiment": False,
                "social_sentiment": False,
                "technical_scan": True,
                "fundamental_validation": False,
                "macro_context": True,
                "liquidity_filter": False,
                "earnings_data": False,
                "ticker_research": False,
            },
            "risk_framework": "neutral",
            "response_style": "institutional_brief",
            "priority_depth": "medium",
        },
        "social_momentum": {
            "intent": "cross_asset_trending",
            "asset_classes": ["equities", "crypto"],
            "modules": {
                "x_sentiment": True,
                "social_sentiment": True,
                "technical_scan": True,
                "fundamental_validation": True,
                "macro_context": False,
                "liquidity_filter": False,
                "earnings_data": False,
                "ticker_research": False,
            },
            "risk_framework": "neutral",
            "response_style": "high_conviction_ranked",
            "priority_depth": "medium",
        },
        "investment_ideas": {
            "intent": "investment_ideas",
            "asset_classes": ["equities"],
            "modules": {
                "x_sentiment": False,
                "social_sentiment": False,
                "technical_scan": True,
                "fundamental_validation": True,
                "macro_context": True,
                "liquidity_filter": True,
                "earnings_data": True,
                "ticker_research": False,
            },
            "risk_framework": "conservative",
            "response_style": "deep_thesis",
            "priority_depth": "deep",
        },
        "bearish_setups": {
            "intent": "short_setup",
            "asset_classes": ["equities"],
            "modules": {
                "x_sentiment": True,
                "social_sentiment": True,
                "technical_scan": True,
                "fundamental_validation": True,
                "macro_context": False,
                "liquidity_filter": True,
                "earnings_data": False,
                "ticker_research": False,
            },
            "risk_framework": "bearish",
            "response_style": "high_conviction_ranked",
            "priority_depth": "medium",
        },
        "thematic_scan": {
            "intent": "thematic",
            "asset_classes": ["equities", "crypto"],
            "modules": {
                "x_sentiment": True,
                "social_sentiment": True,
                "technical_scan": True,
                "fundamental_validation": True,
                "macro_context": False,
                "liquidity_filter": False,
                "earnings_data": False,
                "ticker_research": False,
            },
            "risk_framework": "neutral",
            "response_style": "institutional_brief",
            "priority_depth": "medium",
        },
        "portfolio_review": {
            "intent": "portfolio_review",
            "asset_classes": ["equities", "crypto"],
            "modules": {
                "x_sentiment": False,
                "social_sentiment": False,
                "technical_scan": True,
                "fundamental_validation": True,
                "macro_context": True,
                "liquidity_filter": False,
                "earnings_data": False,
                "ticker_research": False,
            },
            "risk_framework": "neutral",
            "response_style": "deep_thesis",
            "priority_depth": "deep",
        },
    }

    INTENT_TO_CATEGORY = {
        "cross_asset_trending": "trending",
        "single_asset_scan": "market_scan",
        "deep_dive": "ticker_analysis",
        "sector_rotation": "sector_rotation",
        "macro_outlook": "macro",
        "portfolio_review": "portfolio_review",
        "event_driven": "earnings_catalyst",
        "thematic": "thematic",
        "investment_ideas": "investments",
        "briefing": "briefing",
        "custom_screen": "ai_screener",
        "short_setup": "bearish",
        "chat": "chat",
    }

    ASSET_CLASS_CATEGORY_MAP = {
        "equities": "market_scan",
        "crypto": "crypto",
        "commodities": "commodities",
        "macro": "macro",
    }

    VALID_INTENTS = set(INTENT_TO_CATEGORY.keys())

    DEFAULT_PLAN = {
        "intent": "cross_asset_trending",
        "asset_classes": ["equities", "crypto", "commodities", "macro"],
        "modules": {
            "x_sentiment": True,
            "social_sentiment": True,
            "technical_scan": True,
            "fundamental_validation": True,
            "macro_context": True,
            "liquidity_filter": True,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
        "filters": {},
        "tickers": [],
    }

    def _orchestrate_query_openai(self, prompt: str) -> dict:
        if not self.openai_client:
            print(f"[ORCHESTRATOR] No OpenAI client, using default plan")
            return dict(self.DEFAULT_PLAN)
        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-4o",
                max_tokens=500,
                temperature=0.1,
                response_format={"type": "json_object"},
                messages=[
                    {
                        "role": "system",
                        "content": "You are a trading system orchestrator. Reply with ONLY a valid JSON object matching the schema described. No narrative text.",
                    },
                    {
                        "role": "user",
                        "content": f"{ORCHESTRATION_PROMPT}\n\nUser query: {prompt}",
                    },
                ],
            )
            text = response.choices[0].message.content.strip()
            plan = json.loads(text)
            return self._validate_plan(plan, prompt)
        except Exception as e:
            print(f"[ORCHESTRATOR] OpenAI orchestration error: {e}, using heuristic fallback")
            plan = self._heuristic_fallback_plan(prompt)
            plan["_from_heuristic"] = True
            return plan

    def _validate_plan(self, plan: dict, prompt: str) -> dict:
        if not isinstance(plan, dict):
            print(f"[ORCHESTRATOR] Invalid plan type: {type(plan)}, using default")
            return dict(self.DEFAULT_PLAN)

        intent = plan.get("intent", "")
        if intent not in self.VALID_INTENTS:
            print(f"[ORCHESTRATOR] Unknown intent '{intent}', using default")
            return dict(self.DEFAULT_PLAN)

        if "modules" not in plan or not isinstance(plan.get("modules"), dict):
            plan["modules"] = dict(self.DEFAULT_PLAN["modules"])

        if "asset_classes" not in plan or not isinstance(plan.get("asset_classes"), list):
            plan["asset_classes"] = ["equities"]

        if "filters" not in plan:
            plan["filters"] = {}
        if "tickers" not in plan:
            plan["tickers"] = []
        if "risk_framework" not in plan:
            plan["risk_framework"] = "neutral"
        if "response_style" not in plan:
            plan["response_style"] = "institutional_brief"
        if "priority_depth" not in plan:
            plan["priority_depth"] = "medium"

        plan = self._apply_priority_overrides(plan, prompt)
        return plan

    def _apply_priority_overrides(self, plan: dict, prompt: str) -> dict:
        q = prompt.lower().strip()

        cross_asset_signals = [
            "across all markets", "cross asset", "cross-asset", "global opportunities",
            "stocks, crypto", "crypto, stock", "stocks and crypto", "crypto and stock",
            "all asset", "every asset class", "every market",
        ]
        if any(s in q for s in cross_asset_signals):
            plan["asset_classes"] = ["equities", "crypto", "commodities", "macro"]
            if plan["intent"] == "single_asset_scan":
                plan["intent"] = "cross_asset_trending"

        institutional_signals = [
            "highest conviction", "institutional", "serious", "not hype",
            "real opportunities", "quality only", "no memes", "no hype",
        ]
        if any(s in q for s in institutional_signals):
            plan["modules"]["liquidity_filter"] = True
            plan["modules"]["fundamental_validation"] = True
            plan["modules"]["macro_context"] = True

        return plan

    def _plan_to_query_info(self, plan: dict) -> dict:
        intent = plan.get("intent", "cross_asset_trending")
        category = self.INTENT_TO_CATEGORY.get(intent, "market_scan")

        asset_classes = plan.get("asset_classes", ["equities"])

        if intent == "single_asset_scan" and len(asset_classes) == 1:
            ac = asset_classes[0]
            category = self.ASSET_CLASS_CATEGORY_MAP.get(ac, "market_scan")

        if intent == "cross_asset_trending":
            if len(asset_classes) >= 2 and set(asset_classes) != {"equities"}:
                trending_intent = plan.get("_is_trending", False)
                modules = plan.get("modules", {})
                has_social = modules.get("x_sentiment") or modules.get("social_sentiment")
                if has_social:
                    category = "trending"
                else:
                    category = "cross_market"
            else:
                category = "trending"

        if intent == "single_asset_scan":
            modules = plan.get("modules", {})
            if modules.get("social_sentiment") or modules.get("x_sentiment"):
                if category == "market_scan":
                    category = "social_momentum"

        filters = plan.get("filters", {})
        tickers = plan.get("tickers", [])

        query_info = {
            "category": category,
            "filters": filters,
            "orchestration_plan": plan,
        }
        if tickers:
            query_info["tickers"] = tickers

        return query_info

    async def _orchestrate_with_timeout(self, prompt: str) -> dict:
        try:
            plan = await asyncio.wait_for(
                asyncio.to_thread(self._orchestrate_query_openai, prompt),
                timeout=10.0,
            )
            from_heuristic = plan.pop("_from_heuristic", False)
            query_info = self._plan_to_query_info(plan)
            if from_heuristic:
                query_info["_routing_source"] = "heuristic"
                is_chat = plan.get("intent") == "chat"
                query_info["_routing_confidence"] = "low" if is_chat else "medium"
            else:
                query_info["_routing_source"] = "classifier"
                query_info["_routing_confidence"] = "high"
            print(f"[ORCHESTRATOR] Intent: {plan.get('intent')} → Category: {query_info['category']} | "
                  f"Assets: {plan.get('asset_classes')} | "
                  f"Modules: {[k for k, v in plan.get('modules', {}).items() if v]} | "
                  f"Depth: {plan.get('priority_depth')}")
            return query_info
        except (asyncio.TimeoutError, Exception) as e:
            print(f"[ORCHESTRATOR] Orchestration failed/timed out: {e}, using keyword fallback")
            fallback = self._keyword_classify(prompt)
            fallback["_routing_source"] = "heuristic"
            fallback["_routing_confidence"] = "medium"
            return fallback

    async def _execute_orchestration_plan(self, query_info: dict) -> dict:
        plan = query_info.get("orchestration_plan")
        if not plan:
            return await self._gather_data(query_info)

        category = query_info.get("category", "general")
        intent = plan.get("intent", "")
        modules = plan.get("modules", {})
        asset_classes = plan.get("asset_classes", ["equities"])

        primary_data = await self._gather_data(query_info)

        overlay_tasks = []

        if modules.get("macro_context") and category not in ("macro", "briefing", "cross_market"):
            async def fetch_macro():
                try:
                    full_macro = await asyncio.wait_for(
                        self.data.get_macro_overview(),
                        timeout=15.0,
                    )
                    if not isinstance(full_macro, dict):
                        return None
                    slim_macro = {}
                    for key in ("fear_greed", "treasury_rates", "market_summary",
                                "key_indicators", "regime", "macro_regime"):
                        if key in full_macro:
                            slim_macro[key] = full_macro[key]
                    econ = full_macro.get("economic_calendar", [])
                    if econ and isinstance(econ, list):
                        slim_macro["upcoming_events"] = econ[:5]
                    return slim_macro or None
                except Exception as e:
                    print(f"[ORCHESTRATOR] Macro overlay failed: {e}")
                    return None
            overlay_tasks.append(("macro_context", fetch_macro()))

        if modules.get("x_sentiment") and category not in ("trending", "social_momentum", "cross_market"):
            tickers = plan.get("tickers", [])
            if tickers and self.data.xai:
                async def fetch_x_sentiment():
                    try:
                        results = {}
                        for ticker in tickers[:3]:
                            sent = await asyncio.wait_for(
                                self.data.xai.get_ticker_sentiment(ticker, "stock"),
                                timeout=15.0,
                            )
                            if sent and "error" not in sent:
                                results[ticker] = sent
                        return results or None
                    except Exception as e:
                        print(f"[ORCHESTRATOR] X sentiment overlay failed: {e}")
                        return None
                overlay_tasks.append(("x_sentiment_overlay", fetch_x_sentiment()))

        if overlay_tasks:
            overlay_results = await asyncio.gather(
                *[task for _, task in overlay_tasks],
                return_exceptions=True,
            )
            for (name, _), result in zip(overlay_tasks, overlay_results):
                if isinstance(result, Exception):
                    print(f"[ORCHESTRATOR] Overlay '{name}' exception: {result}")
                    continue
                if result:
                    if isinstance(primary_data, dict):
                        primary_data[name] = result
                    print(f"[ORCHESTRATOR] Added overlay: {name}")

        if isinstance(primary_data, dict):
            primary_data["orchestration_metadata"] = {
                "intent": intent,
                "asset_classes": asset_classes,
                "active_modules": [k for k, v in modules.items() if v],
                "risk_framework": plan.get("risk_framework", "neutral"),
                "response_style": plan.get("response_style", "institutional_brief"),
                "priority_depth": plan.get("priority_depth", "medium"),
            }

        return primary_data

    async def _gather_data_safe(self, query_info: dict) -> dict:
        category = query_info.get("category", "general")
        has_plan = "orchestration_plan" in query_info
        gather_timeout = 40.0 if category == "cross_market" else 55.0
        if has_plan and query_info.get("orchestration_plan", {}).get("modules", {}).get("macro_context"):
            gather_timeout = min(gather_timeout + 10.0, 65.0)
        try:
            if has_plan:
                return await asyncio.wait_for(
                    self._execute_orchestration_plan(query_info),
                    timeout=gather_timeout,
                )
            return await asyncio.wait_for(
                self._gather_data(query_info),
                timeout=gather_timeout,
            )
        except asyncio.TimeoutError:
            print(f"[AGENT] Data gathering timed out after {gather_timeout}s for {category}, returning partial data")
            return {"error": f"Data gathering timed out after {gather_timeout}s. Some sources may be slow or rate-limited."}
        except Exception as e:
            print(f"[AGENT] Data gathering error: {e}")
            return {"error": f"Data gathering failed: {str(e)}"}

    async def _gather_chat_context(self, query: str, query_info: dict) -> dict:
        context = {}

        try:
            fg = await asyncio.wait_for(
                self.data.fear_greed.get_fear_greed_index(),
                timeout=5.0,
            )
            if fg:
                context["fear_greed"] = fg
        except Exception:
            pass

        tickers = query_info.get("tickers", [])
        if not tickers:
            tickers = self._extract_tickers(query)

        if tickers:
            print(f"[Chat] Fetching quick data for mentioned tickers: {tickers[:3]}")
            for ticker in tickers[:3]:
                ticker_data = {"ticker": ticker}

                try:
                    overview = await asyncio.wait_for(
                        self.data.stockanalysis.get_overview(ticker),
                        timeout=6.0,
                    )
                    if overview:
                        ticker_data.update(overview)
                except Exception:
                    pass

                try:
                    sentiment = await asyncio.wait_for(
                        self.data.stocktwits.get_sentiment(ticker),
                        timeout=5.0,
                    )
                    if sentiment:
                        ticker_data["social_sentiment"] = sentiment
                except Exception:
                    pass

                try:
                    ratings = await asyncio.wait_for(
                        self.data.stockanalysis.get_analyst_ratings(ticker),
                        timeout=6.0,
                    )
                    if ratings:
                        ticker_data["analyst_ratings"] = ratings
                except Exception:
                    pass

                CRYPTO_SYMBOLS = {
                    "BTC", "ETH", "SOL", "DOGE", "XRP", "ADA", "AVAX", "DOT",
                    "MATIC", "LINK", "UNI", "AAVE", "ATOM", "NEAR", "ARB",
                    "OP", "SUI", "APT", "SEI", "TIA", "INJ", "FET", "RENDER",
                    "TAO", "WIF", "PEPE", "BONK", "JUP", "ONDO", "HYPE",
                    "SHIB", "LTC", "BCH", "FIL", "ICP", "STX", "MKR",
                    "RUNE", "PENDLE", "ENA", "W", "STRK", "ZRO", "PYTH",
                }
                if self.data.xai:
                    try:
                        x_sent = await asyncio.wait_for(
                            self.data.xai.get_ticker_sentiment(
                                ticker,
                                "crypto" if ticker.upper() in CRYPTO_SYMBOLS else "stock",
                            ),
                            timeout=15.0,
                        )
                        if x_sent and "error" not in x_sent:
                            ticker_data["x_sentiment"] = x_sent
                    except Exception:
                        pass

                context[f"ticker_{ticker}"] = ticker_data
                if len(tickers) > 1:
                    await asyncio.sleep(0.5)

        if not context:
            return None

        return context

    DEEP_ANALYSIS_CATEGORIES = {
        "ticker_analysis", "investments", "portfolio_review", "followup",
    }

    async def _ask_claude_with_timeout(self, user_prompt: str, market_data: dict, history: list = None, is_followup: bool = False, category: str = "") -> str:
        data_size = len(json.dumps(market_data, default=str)) if market_data else 0
        print(f"[AGENT] Sending to Claude: {data_size:,} chars of market data (category={category})")
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self._ask_claude, user_prompt, market_data, history, is_followup, category),
                timeout=90.0,
            )
        except asyncio.TimeoutError:
            print(f"[AGENT] Claude API timed out after 90s (data was {data_size:,} chars)")
            return json.dumps({"display_type": "chat", "message": "The AI took too long to respond. Please try again — sometimes the model is under heavy load."})
        except Exception as e:
            print(f"[AGENT] Claude API error: {e}")
            return json.dumps({"display_type": "chat", "message": f"Error reaching AI: {str(e)}"})

    async def _gather_data(self, query_info: dict) -> dict:
        """Fetch the appropriate data based on query classification."""
        category = query_info.get("category", "general")
        filters = query_info.get("filters", {})

        if category == "ticker_analysis":
            tickers = query_info.get("tickers", [])
            results = {}
            for ticker in tickers[:5]:  # Limit to 5 tickers
                results[ticker] = await self.data.research_ticker(ticker)
            return results

        elif category == "market_scan":
            return await self.data.wide_scan_and_rank("market_scan", filters)

        elif category == "dashboard":
            return await self.data.get_dashboard()

        elif category == "investments":
            return await self.data.wide_scan_and_rank("investments", filters)

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
            return await self.data.get_earnings_catalyst_watch()

        elif category == "sector_rotation":
            return await self.data.get_sector_rotation_with_stages()

        elif category == "asymmetric":
            return await self.data.wide_scan_and_rank("asymmetric", filters)

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
            return await self.data.get_crypto_scanner()

        elif category == "trending":
            return await self.data.get_cross_platform_trending()

        elif category == "cross_market":
            return await self.data.get_cross_market_scan()

        elif category == "ai_screener":
            try:
                original_prompt = query_info.get("original_prompt", "")
                filters = self._extract_screener_filters(original_prompt)
                print(f"[AI Screener] Extracted filters: {filters}")
                result = await self.data.run_ai_screener(filters)
                print(f"[AI Screener] Got {result.get('total_results', 0)} results")
                return result
            except Exception as e:
                import traceback
                print(f"[AI Screener] ERROR: {e}")
                traceback.print_exc()
                return {"error": str(e), "filters_applied": {}, "total_results": 0, "results": []}

        elif category == "briefing":
            return await self.data.get_morning_briefing()

        elif category == "portfolio_review":
            tickers = query_info.get("tickers", [])
            if not tickers:
                import re
                ticker_pattern = re.findall(r'\b([A-Z]{1,5})\b', query_info.get("original_prompt", ""))
                common_words = {"I", "A", "AM", "AN", "AS", "AT", "BE", "BY", "DO", "GO",
                               "IF", "IN", "IS", "IT", "ME", "MY", "NO", "OF", "ON", "OR",
                               "SO", "TO", "UP", "US", "WE", "THE", "AND", "FOR", "ARE",
                               "BUT", "NOT", "YOU", "ALL", "CAN", "HAD", "HER", "WAS",
                               "ONE", "OUR", "OUT", "HAS", "HIS", "HOW", "ITS", "MAY",
                               "NEW", "NOW", "OLD", "SEE", "WAY", "WHO", "DID", "GET",
                               "HIM", "LET", "SAY", "SHE", "TOO", "USE", "BUY", "SELL",
                               "HOLD", "LONG", "SHORT", "PUT", "CALL", "ETF", "IPO",
                               "CEO", "CFO", "COO", "EPS", "GDP", "CPI", "FED", "SEC",
                               "FDA", "RSI", "SMA", "ATH", "ATL", "YOY", "QOQ", "EBITDA",
                               "NYSE", "SHOW", "GIVE", "BEST", "WHAT", "WHICH", "RATE",
                               "FULL", "HIGH", "LOW", "TOP"}
                tickers = [t for t in ticker_pattern if t not in common_words][:25]
            return await self.data.analyze_portfolio(tickers)

        elif category == "chat":
            return await self._gather_chat_context(
                query_info.get("original_prompt", ""),
                query_info,
            ) or {}

        else:
            return {}

    async def review_watchlist(self, tickers: list) -> dict:
        """Dedicated watchlist review — bypasses the classifier entirely."""
        import time
        start = time.time()

        tickers = [t.strip().upper() for t in tickers if t.strip()][:25]
        print(f"[WATCHLIST] Reviewing {len(tickers)} tickers: {tickers}")

        async def fetch_ticker_data(ticker, index):
            data = {"ticker": ticker}
            use_polygon = (index < 3)

            try:
                overview = await asyncio.wait_for(
                    self.data.stockanalysis.get_overview(ticker), timeout=8.0,
                )
                if overview:
                    data.update(overview)
            except Exception as e:
                print(f"[WATCHLIST] {ticker} overview failed: {e}")

            try:
                ratings = await asyncio.wait_for(
                    self.data.stockanalysis.get_analyst_ratings(ticker), timeout=8.0,
                )
                if ratings:
                    data["analyst_ratings"] = ratings
            except Exception as e:
                print(f"[WATCHLIST] {ticker} ratings failed: {e}")

            if use_polygon:
                try:
                    data["technicals"] = await asyncio.wait_for(
                        asyncio.to_thread(self.data.polygon.get_technicals, ticker),
                        timeout=8.0,
                    )
                except Exception as e:
                    print(f"[WATCHLIST] {ticker} technicals failed: {e}")

                try:
                    data["snapshot"] = await asyncio.wait_for(
                        asyncio.to_thread(self.data.polygon.get_snapshot, ticker),
                        timeout=8.0,
                    )
                except Exception as e:
                    print(f"[WATCHLIST] {ticker} snapshot failed: {e}")

                await asyncio.sleep(1.0)

            try:
                sentiment = await asyncio.wait_for(
                    self.data.stocktwits.get_sentiment(ticker), timeout=6.0,
                )
                if sentiment:
                    data["social_sentiment"] = sentiment
            except Exception as e:
                print(f"[WATCHLIST] {ticker} sentiment failed: {e}")

            return data

        all_ticker_data = []
        flat_index = 0
        for i in range(0, len(tickers), 5):
            batch = tickers[i:i+5]
            batch_results = await asyncio.gather(
                *[fetch_ticker_data(t, flat_index + j) for j, t in enumerate(batch)],
                return_exceptions=True,
            )
            flat_index += len(batch)
            for result in batch_results:
                if isinstance(result, Exception):
                    print(f"[WATCHLIST] Batch item failed: {result}")
                else:
                    all_ticker_data.append(result)

            if i + 5 < len(tickers):
                await asyncio.sleep(0.5)

        print(f"[WATCHLIST] Data fetched for {len(all_ticker_data)} tickers ({time.time()-start:.1f}s)")

        compressed = compress_data({"watchlist": all_ticker_data})
        data_str = json.dumps(compressed, default=str)
        print(f"[WATCHLIST] Compressed data: {len(data_str)} chars")

        messages = [{
            "role": "user",
            "content": f"""[WATCHLIST DATA]
{data_str}

[USER REQUEST]
Review my watchlist: {', '.join(tickers)}

For EACH ticker, give me:
1. TECHNICAL ANALYSIS: Current stage (Weinstein), trend direction, RSI reading, key support/resistance levels, SMA positioning, MACD signal. Is this in a buyable position right now?
2. FUNDAMENTAL ANALYSIS: Revenue growth, margins, valuation (P/E, P/S), debt levels, earnings trajectory. Is the business improving or deteriorating?
3. CATALYSTS & THESIS: What's the bull case? Any upcoming earnings, product launches, regulatory events, or sector tailwinds? What could move this stock in the next 1-3 months?
4. YOUR VERDICT: Buy, hold, trim, or sell — and why. Be specific about entry points if it's a buy, or exit points if it's a sell.
5. POSITION SIZING: Given the risk/reward, what conviction level (high/medium/low) and how would you size this?

After analyzing each ticker individually, give me an OVERALL PORTFOLIO ASSESSMENT:
- What's the portfolio's biggest strength and biggest weakness?
- Any concentration risk (too many correlated positions)?
- What would you add or remove to improve the portfolio?
- What's your #1 action item for me right now?

Be direct and opinionated. Tell me what you actually think."""
        }]

        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    self.client.messages.create,
                    model="claude-sonnet-4-20250514",
                    max_tokens=16384,
                    system=[
                        {
                            "type": "text",
                            "text": SYSTEM_PROMPT,
                            "cache_control": {"type": "ephemeral"},
                        }
                    ],
                    messages=messages,
                ),
                timeout=60.0,
            )

            response_text = response.content[0].text
            print(f"[WATCHLIST] Claude responded: {len(response_text)} chars ({time.time()-start:.1f}s)")

            parsed = self._parse_response(response_text)
            return parsed

        except asyncio.TimeoutError:
            print(f"[WATCHLIST] Claude timed out ({time.time()-start:.1f}s)")
            return {
                "type": "chat",
                "analysis": "",
                "structured": {
                    "display_type": "chat",
                    "message": "Claude timed out analyzing your watchlist. Try fewer tickers.",
                },
            }
        except Exception as e:
            print(f"[WATCHLIST] Claude error: {e}")
            return {
                "type": "chat",
                "analysis": "",
                "structured": {
                    "display_type": "chat",
                    "message": f"Error analyzing watchlist: {str(e)}",
                },
            }

    def _extract_screener_filters(self, prompt: str) -> dict:
        """
        Parse natural language screener request into structured filters.
        Handles both explicit quantitative filters AND conversational descriptions.
        """
        import re
        filters = {}
        p = prompt.lower()

        cap_match = re.search(r'(?:market\s*cap|mcap).*?(?:under|below|<|max)\s*\$?([\d.]+)\s*([bmtBMT])', p)
        if cap_match:
            val = float(cap_match.group(1))
            unit = cap_match.group(2).lower()
            if unit == 'm': val /= 1000
            elif unit == 't': val *= 1000
            filters["market_cap_max"] = val

        cap_match2 = re.search(r'(?:market\s*cap|mcap).*?(?:over|above|>|min|at least)\s*\$?([\d.]+)\s*([bmtBMT])', p)
        if cap_match2:
            val = float(cap_match2.group(1))
            unit = cap_match2.group(2).lower()
            if unit == 'm': val /= 1000
            elif unit == 't': val *= 1000
            filters["market_cap_min"] = val

        if any(w in p for w in ["penny stock", "penny stocks", "nano cap"]) and "market_cap_max" not in filters:
            filters["market_cap_max"] = 0.3
            filters.setdefault("price_max", 5)
        elif any(w in p for w in ["micro cap", "micro-cap"]) and "market_cap_max" not in filters:
            filters["market_cap_max"] = 0.3
        elif any(w in p for w in ["small cap", "small-cap", "smallcap"]) and "market_cap_max" not in filters:
            filters["market_cap_max"] = 2
        elif "mid cap" in p or "mid-cap" in p or "midcap" in p:
            filters.setdefault("market_cap_min", 2)
            filters.setdefault("market_cap_max", 10)
        elif any(w in p for w in ["large cap", "large-cap", "largecap", "blue chip"]):
            filters.setdefault("market_cap_min", 10)
        elif any(w in p for w in ["mega cap", "mega-cap"]):
            filters.setdefault("market_cap_min", 200)

        rev_match = re.search(r'(?:revenue|sales)\s*(?:growth)?\s*(?:>|over|above|at least|min|greater than)?\s*(\d+)\s*%', p)
        if rev_match:
            filters["revenue_growth_min"] = int(rev_match.group(1))
        elif any(w in p for w in ["fast growing", "fast-growing", "rapid growth", "high growth", "growing fast", "revenue growth", "sales growth", "growing revenue"]):
            filters.setdefault("revenue_growth_min", 15)
        elif any(w in p for w in ["hyper growth", "hypergrowth", "explosive growth"]):
            filters.setdefault("revenue_growth_min", 30)

        eps_match = re.search(r'(?:eps|earnings)\s*(?:growth)?\s*(?:>|over|above)?\s*(\d+)\s*%', p)
        if eps_match:
            filters["eps_growth_min"] = int(eps_match.group(1))
        elif any(w in p for w in ["earnings growth", "growing earnings", "eps growth", "profit growth"]):
            filters.setdefault("eps_growth_min", 15)

        pe_match = re.search(r'(?:p/?e|pe ratio|price.to.earnings)\s*(?:<|under|below|max)?\s*(\d+)', p)
        if pe_match:
            filters["pe_max"] = int(pe_match.group(1))

        ps_match = re.search(r'(?:p/?s|price.to.sales)\s*(?:<|under|below)?\s*(\d+)', p)
        if ps_match:
            filters["ps_max"] = int(ps_match.group(1))

        if any(w in p for w in ["cheap", "undervalued", "bargain", "value stock", "value play", "deep value"]):
            filters.setdefault("pe_max", 20)
            filters.setdefault("ps_max", 3)
        elif "fairly valued" in p or "reasonable valuation" in p:
            filters.setdefault("pe_max", 30)

        rsi_low = re.search(r'rsi\s*(?:<|under|below)\s*(\d+)', p)
        if rsi_low:
            filters["rsi_max"] = int(rsi_low.group(1))
        rsi_high = re.search(r'rsi\s*(?:>|over|above)\s*(\d+)', p)
        if rsi_high:
            filters["rsi_min"] = int(rsi_high.group(1))

        if any(w in p for w in ["oversold", "beaten down", "crushed", "hammered"]) and "rsi_max" not in filters:
            filters["rsi_max"] = 30
        if any(w in p for w in ["overbought", "overextended", "stretched"]) and "rsi_min" not in filters:
            filters["rsi_min"] = 70

        if any(w in p for w in ["above 200", "above sma200", "above 200 sma", "above 200-day", "above the 200"]):
            filters["above_sma200"] = True
        if any(w in p for w in ["above 50", "above sma50", "above 50 sma", "above 50-day", "above the 50"]):
            filters["above_sma50"] = True
        if any(w in p for w in ["below 200", "below sma200", "below 200 sma", "below 200-day"]):
            filters["below_sma200"] = True
        if any(w in p for w in ["below 50", "below sma50", "below 50 sma", "below 50-day"]):
            filters["below_sma50"] = True

        if any(w in p for w in ["stage 2", "weinstein stage 2", "confirmed uptrend", "above all moving averages", "above all sma"]):
            filters["above_sma200"] = True
            filters["above_sma50"] = True
        if any(w in p for w in ["breaking out", "breakout", "breaking above"]):
            filters["above_sma50"] = True
            filters.setdefault("unusual_volume", True)
        if any(w in p for w in ["breaking down", "breakdown", "stage 4"]):
            filters["below_sma200"] = True
            filters["below_sma50"] = True

        if any(w in p for w in ["unusual volume", "volume spike", "volume surge", "heavy volume", "big volume"]):
            filters["unusual_volume"] = True
        rv_match = re.search(r'(?:relative|rel)\s*(?:volume|vol)\s*(?:>|over|above)?\s*([\d.]+)', p)
        if rv_match:
            filters["relative_volume_min"] = float(rv_match.group(1))

        avg_vol_match = re.search(r'(?:avg|average)\s*(?:volume|vol)\s*(?:>|over|above|min)?\s*([\d,]+)', p)
        if avg_vol_match:
            val = avg_vol_match.group(1).replace(",", "")
            filters["avg_volume_min"] = int(int(val) / 1000)

        if any(w in p for w in ["profitable", "positive margin", "positive ebitda", "making money", "positive earnings", "actually profitable"]):
            filters["positive_margin"] = True

        de_match = re.search(r'(?:debt.to.equity|d/?e)\s*(?:<|under|below)\s*([\d.]+)', p)
        if de_match:
            filters["debt_equity_max"] = float(de_match.group(1))
        if any(w in p for w in ["low debt", "no debt", "debt free", "clean balance sheet", "healthy balance sheet"]) and "debt_equity_max" not in filters:
            filters["debt_equity_max"] = 0.5

        sf_match = re.search(r'short\s*(?:float|interest)\s*(?:>|over|above)\s*(\d+)', p)
        if sf_match:
            filters["short_float_min"] = int(sf_match.group(1))
        if any(w in p for w in ["high short", "heavily shorted", "most shorted", "squeeze candidate"]) and "short_float_min" not in filters:
            filters["short_float_min"] = 15

        if any(w in p for w in ["insider buy", "insider purchas", "insider buying", "insiders buying", "insider accumulation"]):
            filters["insider_buying"] = True

        div_match = re.search(r'dividend\s*(?:yield)?\s*(?:>|over|above|at least)\s*([\d.]+)', p)
        if div_match:
            filters["dividend_yield_min"] = float(div_match.group(1))
        if any(w in p for w in ["dividend stock", "dividend play", "income stock", "high yield", "dividend payer"]) and "dividend_yield_min" not in filters:
            filters["dividend_yield_min"] = 2

        sector_keywords = {
            "tech": "technology", "technology": "technology", "software": "technology", "saas": "technology",
            "semiconductor": "technology", "chip": "technology",
            "healthcare": "healthcare", "health care": "healthcare", "pharma": "healthcare",
            "biotech": "healthcare", "medical": "healthcare",
            "financial": "financial", "bank": "financial", "insurance": "financial", "fintech": "financial",
            "energy": "energy", "oil": "energy", "solar": "energy", "renewable": "energy",
            "industrial": "industrials", "manufacturing": "industrials", "defense": "industrials",
            "aerospace": "industrials",
            "consumer cyclical": "consumer cyclical", "retail": "consumer cyclical",
            "consumer defensive": "consumer defensive", "staples": "consumer defensive",
            "real estate": "real estate", "reit": "real estate",
            "utilities": "utilities", "utility": "utilities",
            "materials": "basic materials", "mining": "basic materials", "metals": "basic materials",
            "communication": "communication services", "media": "communication services",
            "telecom": "communication services",
        }
        for kw, sec in sector_keywords.items():
            if kw in p:
                filters["sector"] = sec
                break

        perf_match = re.search(r'(?:up|gained|rose)\s*(?:more than\s*)?(\d+)%?\s*(?:this|in the last|past)\s*(week|month|quarter|year)', p)
        if perf_match:
            pct = int(perf_match.group(1))
            period = perf_match.group(2)
            period_map = {"week": "perf_week", "month": "perf_month", "quarter": "perf_quarter", "year": "perf_year"}
            key = period_map.get(period)
            if key:
                filters[key] = pct

        perf_down_match = re.search(r'(?:down|dropped|fell|lost)\s*(?:more than\s*)?(\d+)%?\s*(?:this|in the last|past)\s*(week|month|quarter|year)', p)
        if perf_down_match:
            pct = int(perf_down_match.group(1))
            period = perf_down_match.group(2)
            period_map = {"week": "perf_week_down", "month": "perf_month_down", "quarter": "perf_quarter_down", "year": "perf_year_down"}
            key = period_map.get(period)
            if key:
                filters[key] = pct

        if any(w in p for w in ["earnings this week", "reporting this week", "earnings coming up"]):
            filters["earnings_this_week"] = True
        if any(w in p for w in ["earnings next week", "reporting next week"]):
            filters["earnings_next_week"] = True
        if any(w in p for w in ["earnings today", "reporting today"]):
            filters["earnings_today"] = True

        upside_match = re.search(r'(?:analyst|price)\s*(?:target|upside)\s*(?:>|over|above|at least)\s*(\d+)\s*%', p)
        if upside_match:
            filters["analyst_upside_min"] = int(upside_match.group(1))
        if any(w in p for w in ["analyst upgrade", "upgraded", "buy rating"]):
            filters["analyst_upgrades"] = True

        if any(w in p for w in ["gap up", "gapping up", "gapped up"]):
            filters["gap_up"] = True
        if any(w in p for w in ["gap down", "gapping down", "gapped down"]):
            filters["gap_down"] = True

        if any(w in p for w in ["low float", "small float", "tiny float"]):
            filters["low_float"] = True
        float_match = re.search(r'float\s*(?:<|under|below)\s*(\d+)\s*[mM]', p)
        if float_match:
            filters["float_max_m"] = int(float_match.group(1))

        price_under_match = re.search(r'(?:price|priced|stock(?:s)?)\s*(?:under|below|<)\s*\$?(\d+)', p)
        if price_under_match:
            filters["price_max"] = int(price_under_match.group(1))
        price_over_match = re.search(r'(?:price|priced|stock(?:s)?)\s*(?:over|above|>)\s*\$?(\d+)', p)
        if price_over_match:
            filters["price_min"] = int(price_over_match.group(1))
        if "under $5" in p or "below $5" in p:
            filters["price_max"] = 5
        if "under $10" in p or "below $10" in p:
            filters.setdefault("price_max", 10)

        if any(w in p for w in ["biggest gain", "top gainer", "best performer", "most up"]):
            filters["sort"] = "-change"
        elif any(w in p for w in ["most volume", "highest volume", "most active", "most traded"]):
            filters["sort"] = "-volume"
        elif any(w in p for w in ["cheapest", "lowest p/e", "most undervalued"]):
            filters["sort"] = "pe"
        elif any(w in p for w in ["fastest growing", "highest growth", "best growth"]):
            filters["sort"] = "-fa_salesqoq"
        elif any(w in p for w in ["most shorted", "highest short"]):
            filters["sort"] = "-shortinterestshare"
        elif any(w in p for w in ["biggest loss", "top loser", "worst performer", "most down"]):
            filters["sort"] = "change"

        print(f"[AI Screener] Extracted filters from prompt: {filters}")
        return filters

    def _trim_history(self, messages: list, max_chars: int = 100000) -> list:
        total = sum(len(m.get("content", "")) for m in messages)
        while total > max_chars and len(messages) > 2:
            oldest = messages[0]
            content_len = len(oldest.get("content", ""))
            if content_len > 5000:
                truncated = oldest["content"][:2000] + "\n...[truncated for context window]..."
                saved = content_len - len(truncated)
                oldest["content"] = truncated
                total -= saved
                print(f"[Agent] Truncated oldest message from {content_len:,} to {len(truncated):,} chars")
            else:
                messages.pop(0)
                total -= content_len
                print(f"[Agent] Removed oldest message ({content_len:,} chars) to fit context window")
        return messages

    def _ask_claude(self, user_prompt: str, market_data: dict, history: list = None, is_followup: bool = False, category: str = "") -> str:
        """Send the user's question + market data to Claude with conversation history."""

        data_str = None
        filter_instructions = ""

        if market_data is not None:
            is_cross_market_data = market_data.get("scan_type") == "cross_market"

            if is_cross_market_data:
                market_data = self._slim_cross_market_data(market_data)

            compressed = compress_data(market_data)
            data_str = json.dumps(compressed, default=str)
            raw_size = len(json.dumps(market_data, default=str))
            print(f"[Agent] Data compression: {raw_size:,} → {len(data_str):,} chars ({100 - len(data_str)*100//max(raw_size,1)}% reduction)")

            is_fast_scan = category not in self.DEEP_ANALYSIS_CATEGORIES
            data_cap = 25000 if (is_cross_market_data or is_fast_scan) else 80000
            if len(data_str) > data_cap:
                from agent.data_compressor import _aggressive_truncate
                compressed = _aggressive_truncate(compressed, data_cap - 5000)
                data_str = json.dumps(compressed, default=str)
                print(f"[Agent] Data over {data_cap//1000}K after compression, aggressive truncation → {len(data_str):,}")

            filters = market_data.get("user_filters", {})
            if filters:
                if filters.get("market_cap"):
                    cap = filters["market_cap"]
                    if cap == "small_cap":
                        filter_instructions += "\n⚠️ USER WANTS SMALL CAP STOCKS ONLY (under $2B market cap). Do NOT recommend any stock with a market cap above $2B. Filter out all large caps like RIVN, NVDA, AAPL, etc."
                    elif cap == "mid_cap":
                        filter_instructions += "\n⚠️ USER WANTS MID CAP STOCKS ONLY ($2B-$10B market cap). Filter out small caps and large caps."
                    elif cap == "large_cap":
                        filter_instructions += "\n⚠️ USER WANTS LARGE CAP STOCKS ONLY (over $10B market cap). Filter out small and mid caps."
                    elif cap == "mega_cap":
                        filter_instructions += "\n⚠️ USER WANTS MEGA CAP STOCKS ONLY (over $200B market cap)."
                if filters.get("sector"):
                    filter_instructions += f"\n⚠️ USER WANTS {filters['sector'].upper()} SECTOR ONLY. Only recommend stocks in this sector."
                if filters.get("style"):
                    style = filters["style"]
                    if style == "day_trade":
                        filter_instructions += "\n⚠️ USER WANTS DAY TRADES. Focus on high volume, high volatility stocks with intraday setups. Mention specific entry/exit levels and timeframes."
                    elif style == "swing":
                        filter_instructions += "\n⚠️ USER WANTS SWING TRADES (days to weeks). Focus on stocks with developing technical patterns and upcoming catalysts."
                    elif style == "position":
                        filter_instructions += "\n⚠️ USER WANTS POSITION TRADES (weeks to months). Focus on fundamental value and longer-term technical trends."

        messages = []

        if history:
            recent_history = history[-10:]
            for msg in recent_history:
                messages.append({
                    "role": msg["role"],
                    "content": msg["content"],
                })

        if data_str:
            user_content = (
                f"[MARKET DATA — use this to inform your analysis]\n"
                f"{data_str}\n\n"
                f"{filter_instructions}\n\n"
                f"[USER QUERY]\n"
                f"{user_prompt}"
            )
        else:
            user_content = user_prompt

        messages.append({"role": "user", "content": user_content})

        messages = self._trim_history(messages, max_chars=100000)

        total_prompt_len = len(SYSTEM_PROMPT) + sum(len(m["content"]) for m in messages)
        if data_str and total_prompt_len > 600000:
            allowed = max(10000, 600000 - len(SYSTEM_PROMPT) - len(user_prompt) - 1000)
            from agent.data_compressor import _aggressive_truncate
            compressed = _aggressive_truncate(compressed, allowed)
            data_str = json.dumps(compressed, default=str)
            messages[-1]["content"] = (
                f"[MARKET DATA — use this to inform your analysis]\n"
                f"{data_str}\n\n"
                f"{filter_instructions}\n\n"
                f"[USER QUERY]\n"
                f"{user_prompt}"
            )
            print(f"[Agent] WARNING: Total prompt was {total_prompt_len:,} chars, re-truncated data to {len(data_str):,}")

        system_blocks = [
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ]
        if is_followup:
            system_blocks.append({
                "type": "text",
                "text": """
FOLLOW-UP MODE: The user is continuing a conversation. You have the full conversation history above.
- If the user asks about a specific ticker or pick from your previous response, go deeper on that specific item.
- If the user asks a general question, answer it using your trading expertise and any data from the conversation.
- You can respond conversationally — you don't need to use a structured JSON display_type for follow-ups.
- For follow-up responses, use display_type "chat" with a "message" field containing your analysis.
- BUT if the user asks you to analyze a new ticker or run a new type of scan, use the appropriate display_type.
- Keep your trader personality — be direct, opinionated, and cut through noise.
- You still have access to all the data from the original scan in the conversation history. Reference specific data points when relevant.""",
            })

        is_hybrid_trending = data_str and '"scan_type": "hybrid_trending"' in data_str
        if is_hybrid_trending or category in ("trending", "social_momentum"):
            system_blocks.append({
                "type": "text",
                "text": TRENDING_VALIDATION_PROMPT,
            })

        use_fast_model = category not in self.DEEP_ANALYSIS_CATEGORIES
        if use_fast_model:
            model = "claude-sonnet-4-20250514"
            token_limit = 4096
        else:
            model = "claude-sonnet-4-5-20250929"
            token_limit = 16384
        print(f"[Agent] Sending {len(messages)} messages to Claude (model={model}, category={category}, followup={is_followup}, max_tokens={token_limit})")

        response = self.client.messages.create(
            model=model,
            max_tokens=token_limit,
            system=system_blocks,
            messages=messages,
        )
        if response.stop_reason == "max_tokens":
            print(f"[Agent] WARNING: Response was truncated (hit max_tokens). Length: {len(response.content[0].text)}")
        if not response.content or not response.content[0].text.strip():
            print(f"[Agent] WARNING: Claude returned empty content (stop_reason={response.stop_reason})")
            return json.dumps({"display_type": "chat", "message": "The AI returned an empty response. Please try again."})
        return response.content[0].text

    def _slim_cross_market_data(self, data: dict) -> dict:
        """Pre-compress cross-market data. Now prioritizes pre-ranked candidates over raw dumps."""
        try:
            slim = {
                "scan_type": "cross_market",
                "instructions": data.get("instructions", ""),
            }

            ranked = data.get("ranked_candidates") or []
            ranking_debug = data.get("ranking_debug") or {}

            if ranked:
                slim["ranked_candidates"] = ranked
                slim["ranking_debug"] = {
                    "macro_regime": ranking_debug.get("macro_regime", "unknown"),
                    "candidates_per_class": ranking_debug.get("candidates_per_class", {}),
                    "regime_penalty_applied": ranking_debug.get("regime_penalty_applied", False),
                    "quota_adjustments": ranking_debug.get("quota_adjustments", []),
                    "selection_reasons": ranking_debug.get("selection_reasons", {}),
                }

            ranked_symbols = {c.get("symbol") for c in ranked if isinstance(c, dict)}
            has_ranked = len(ranked) > 0

            stock = data.get("stock_trending") or {}
            if isinstance(stock, dict) and "error" not in stock:
                slim["stocks"] = {
                    "top_trending": (stock.get("top_trending") or [])[:8],
                }
                enriched_data = stock.get("enriched_data")
                if isinstance(enriched_data, dict):
                    if has_ranked:
                        relevant = {k: v for k, v in enriched_data.items() if k in ranked_symbols}
                    else:
                        relevant = dict(list(enriched_data.items())[:6])
                    if relevant:
                        slim["stocks"]["enriched_ranked"] = {
                            ticker: {k: v for k, v in info.items()
                                     if k in {"market_cap", "pe_ratio", "price_target", "revenue_growth",
                                              "analyst_rating", "upside_downside", "beta", "avg_volume"}}
                            for ticker, info in relevant.items() if isinstance(info, dict)
                        }
            else:
                slim["stocks"] = {"error": "unavailable"}

            if not has_ranked:
                crypto = data.get("crypto_scanner") or {}
                if isinstance(crypto, dict) and "error" not in crypto:
                    slim_crypto = {}
                    for key, val in crypto.items():
                        if isinstance(val, dict):
                            val_str = json.dumps(val, default=str)
                            if "trending" in key.lower() or "top" in key.lower() or len(val_str) < 3000:
                                slim_crypto[key] = val
                        elif isinstance(val, list):
                            slim_crypto[key] = val[:6]
                        else:
                            slim_crypto[key] = val
                    slim["crypto"] = slim_crypto
                else:
                    slim["crypto"] = {"error": "unavailable"}

                commodities = data.get("commodities") or {}
                slim["commodities"] = commodities if isinstance(commodities, dict) else {"error": "unavailable"}

            macro = data.get("macro_context") or {}
            if isinstance(macro, dict) and "error" not in macro:
                slim_macro = {}
                fg = macro.get("fear_greed_index")
                if fg:
                    slim_macro["fear_greed"] = fg
                fred = macro.get("fred_economic_data") or {}
                if isinstance(fred, dict):
                    slim_macro["key_rates"] = {k: v for k, v in fred.items()
                                               if k in {"fed_rate", "vix", "cpi", "gdp", "unemployment",
                                                         "yield_curve", "VIX", "fed_funds_rate"}}
                slim["macro"] = slim_macro if slim_macro else {"error": "unavailable"}
            else:
                slim["macro"] = {"error": "unavailable"}

            return slim
        except Exception as e:
            print(f"[Agent] _slim_cross_market_data error: {e}, passing raw data")
            return data

    def _parse_response(self, raw_response: str) -> dict:
        """
        Parse Claude's response into structured JSON.
        Tries multiple strategies:
        1. Raw JSON (entire response is a JSON object)
        2. JSON in ```json``` code block (extract full block content, not regex-matched braces)
        3. Find outermost JSON object by brace-depth counting
        4. Fallback: wrap raw text as chat response
        """
        response_text = raw_response.strip()
        print(f"[Parser] Response length: {len(response_text)}, starts_with_brace: {response_text[:1] == '{'}")

        if response_text.startswith("{"):
            try:
                structured_data = json.loads(response_text)
                print("[Parser] Tier 1 success: raw JSON")
                analysis_text = structured_data.get("summary", "") or structured_data.get("message", "") or ""
                return {
                    "type": structured_data.get("display_type", "chat"),
                    "analysis": analysis_text,
                    "structured": structured_data,
                }
            except json.JSONDecodeError as e:
                print(f"[Parser] Tier 1 failed: {e}")

        json_block_match = re.search(r"```(?:json)?\s*(.*?)\s*```", response_text, re.DOTALL)
        if json_block_match:
            json_str = json_block_match.group(1).strip()
            json_start = json_block_match.start()
            analysis_text = response_text[:json_start].strip()
            print(f"[Parser] Tier 2 found code block, extracted {len(json_str)} chars")
            try:
                structured_data = json.loads(json_str)
                print("[Parser] Tier 2 success: code block JSON")
                return {
                    "type": structured_data.get("display_type", "chat"),
                    "analysis": analysis_text,
                    "structured": structured_data,
                }
            except json.JSONDecodeError as e:
                print(f"[Parser] Tier 2 failed: {e}")
                print(f"[Parser] Tier 2 extracted starts: {json_str[:100]}...")
                print(f"[Parser] Tier 2 extracted ends: ...{json_str[-100:]}")

        first_brace = response_text.find("{")
        if first_brace != -1:
            depth = 0
            in_string = False
            escape_next = False
            end_pos = -1
            for i in range(first_brace, len(response_text)):
                c = response_text[i]
                if escape_next:
                    escape_next = False
                    continue
                if c == '\\' and in_string:
                    escape_next = True
                    continue
                if c == '"' and not escape_next:
                    in_string = not in_string
                    continue
                if in_string:
                    continue
                if c == '{':
                    depth += 1
                elif c == '}':
                    depth -= 1
                    if depth == 0:
                        end_pos = i
                        break
            if end_pos != -1:
                json_str = response_text[first_brace:end_pos + 1]
                pre_json = response_text[:first_brace].strip()
                try:
                    structured_data = json.loads(json_str)
                    return {
                        "type": structured_data.get("display_type", "chat"),
                        "analysis": pre_json,
                        "structured": structured_data,
                    }
                except json.JSONDecodeError:
                    pass

        first_brace2 = response_text.find("{")
        if first_brace2 != -1:
            truncated_json = response_text[first_brace2:]
            truncated_json = re.sub(r',\s*$', '', truncated_json)
            open_braces = truncated_json.count('{') - truncated_json.count('}')
            open_brackets = truncated_json.count('[') - truncated_json.count(']')
            truncated_json += ']' * max(0, open_brackets)
            truncated_json += '}' * max(0, open_braces)
            try:
                structured_data = json.loads(truncated_json)
                print(f"[Parser] Tier 4 success: repaired truncated JSON ({open_braces} braces, {open_brackets} brackets closed)")
                return {
                    "type": structured_data.get("display_type", "chat"),
                    "analysis": "",
                    "structured": structured_data,
                }
            except json.JSONDecodeError:
                last_valid = max(truncated_json.rfind('}'), truncated_json.rfind(']'))
                if last_valid > 0:
                    attempt = truncated_json[:last_valid + 1]
                    open_b = attempt.count('{') - attempt.count('}')
                    open_a = attempt.count('[') - attempt.count(']')
                    attempt += ']' * max(0, open_a)
                    attempt += '}' * max(0, open_b)
                    try:
                        structured_data = json.loads(attempt)
                        print("[Parser] Tier 4 success: repaired by trimming to last valid delimiter")
                        return {
                            "type": structured_data.get("display_type", "chat"),
                            "analysis": "",
                            "structured": structured_data,
                        }
                    except json.JSONDecodeError:
                        pass

        print("[Parser] All tiers failed, returning raw text as chat")
        structured_data = {
            "display_type": "chat",
            "message": response_text,
        }
        return {
            "type": "chat",
            "analysis": response_text,
            "structured": structured_data,
        }