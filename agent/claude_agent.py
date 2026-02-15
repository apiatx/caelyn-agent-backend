import json
import re

import anthropic

from agent.prompts import SYSTEM_PROMPT, QUERY_CLASSIFIER_PROMPT
from data.market_data_service import MarketDataService


class TradingAgent:
    def __init__(self, api_key: str, data_service: MarketDataService):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.data = data_service

    async def handle_query(self, user_prompt: str, history: list = None) -> dict:
        """
        Main entry point. Classify the query, gather data, ask Claude.
        """
        query_info = self._classify_query(user_prompt)
        query_info["original_prompt"] = user_prompt
        market_data = await self._gather_data(query_info)
        raw_response = self._ask_claude(user_prompt, market_data, history)
        return self._parse_response(raw_response)

    def _classify_query(self, prompt: str) -> dict:
        """Ask Claude to classify what kind of query this is."""
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
            # Clean up any markdown formatting
            text = re.sub(r"```json\s*", "", text)
            text = re.sub(r"```\s*", "", text)
            return json.loads(text)
        except Exception as e:
            print(f"Classification error: {e}")
            # Default to market scan if classification fails
            return {"category": "market_scan"}

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

        elif category == "ai_screener":
            original_prompt = query_info.get("original_prompt", "")
            filters = self._extract_screener_filters(original_prompt)
            return await self.data.run_ai_screener(filters)

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

        else:
            # General question — still provide some market context
            return {
                "market_news": self.data.polygon.get_news(limit=10),
            }

    def _extract_screener_filters(self, prompt: str) -> dict:
        """
        Parse natural language screener request into structured filters.
        Uses keyword matching for common criteria.
        """
        import re
        filters = {}
        p = prompt.lower()

        cap_match = re.search(r'(?:market\s*cap|mcap).*?(?:under|below|<|max)\s*\$?(\d+\.?\d*)\s*([bmtBMT])', p)
        if cap_match:
            val = float(cap_match.group(1))
            unit = cap_match.group(2).lower()
            if unit == 'm': val /= 1000
            elif unit == 't': val *= 1000
            filters["market_cap_max"] = val

        cap_match2 = re.search(r'(?:market\s*cap|mcap).*?(?:over|above|>|min|at least)\s*\$?(\d+\.?\d*)\s*([bmtBMT])', p)
        if cap_match2:
            val = float(cap_match2.group(1))
            unit = cap_match2.group(2).lower()
            if unit == 'm': val /= 1000
            elif unit == 't': val *= 1000
            filters["market_cap_min"] = val

        if "small cap" in p and "market_cap_max" not in filters:
            filters["market_cap_max"] = 2
        if "micro cap" in p and "market_cap_max" not in filters:
            filters["market_cap_max"] = 0.3
        if "mid cap" in p:
            filters.setdefault("market_cap_min", 2)
            filters.setdefault("market_cap_max", 10)
        if "large cap" in p:
            filters.setdefault("market_cap_min", 10)

        rev_match = re.search(r'(?:revenue|sales)\s*(?:growth)?\s*(?:>|over|above|at least|min)?\s*(\d+)\s*%', p)
        if rev_match:
            filters["revenue_growth_min"] = int(rev_match.group(1))
        elif "revenue growth" in p or "sales growth" in p:
            filters["revenue_growth_min"] = 10

        eps_match = re.search(r'(?:eps|earnings)\s*(?:growth)?\s*(?:>|over|above)?\s*(\d+)\s*%', p)
        if eps_match:
            filters["eps_growth_min"] = int(eps_match.group(1))

        pe_match = re.search(r'(?:p/?e|pe ratio)\s*(?:<|under|below|max)?\s*(\d+)', p)
        if pe_match:
            filters["pe_max"] = int(pe_match.group(1))

        ps_match = re.search(r'(?:p/?s|price.to.sales)\s*(?:<|under|below)?\s*(\d+)', p)
        if ps_match:
            filters["ps_max"] = int(ps_match.group(1))

        rsi_low = re.search(r'rsi\s*(?:<|under|below)\s*(\d+)', p)
        if rsi_low:
            filters["rsi_max"] = int(rsi_low.group(1))
        rsi_high = re.search(r'rsi\s*(?:>|over|above)\s*(\d+)', p)
        if rsi_high:
            filters["rsi_min"] = int(rsi_high.group(1))
        if "oversold" in p and "rsi_max" not in filters:
            filters["rsi_max"] = 30
        if "overbought" in p and "rsi_min" not in filters:
            filters["rsi_min"] = 70

        if "above 200" in p or "above sma200" in p or "above 200 sma" in p:
            filters["above_sma200"] = True
        if "above 50" in p or "above sma50" in p or "above 50 sma" in p:
            filters["above_sma50"] = True
        if "below 200" in p or "below sma200" in p:
            filters["below_sma200"] = True
        if "below 50" in p or "below sma50" in p:
            filters["below_sma50"] = True
        if "stage 2" in p:
            filters["above_sma200"] = True
            filters["above_sma50"] = True

        if "insider buy" in p or "insider purchas" in p:
            filters["insider_buying"] = True

        if "unusual volume" in p or "volume spike" in p:
            filters["unusual_volume"] = True
        rv_match = re.search(r'(?:relative|rel)\s*(?:volume|vol)\s*(?:>|over|above)?\s*(\d+\.?\d*)', p)
        if rv_match:
            filters["relative_volume_min"] = float(rv_match.group(1))

        if "profitable" in p or "positive margin" in p or "positive ebitda" in p:
            filters["positive_margin"] = True

        de_match = re.search(r'(?:debt.to.equity|d/?e)\s*(?:<|under|below)\s*(\d+\.?\d*)', p)
        if de_match:
            filters["debt_equity_max"] = float(de_match.group(1))
        if "low debt" in p and "debt_equity_max" not in filters:
            filters["debt_equity_max"] = 0.5

        sf_match = re.search(r'short\s*(?:float|interest)\s*(?:>|over|above)\s*(\d+)', p)
        if sf_match:
            filters["short_float_min"] = int(sf_match.group(1))

        sector_keywords = {
            "tech": "technology", "healthcare": "healthcare", "health care": "healthcare",
            "financial": "financial", "bank": "financial", "energy": "energy",
            "industrial": "industrials", "consumer": "consumer cyclical",
            "real estate": "real estate", "utilities": "utilities", "materials": "basic materials",
        }
        for kw, sec in sector_keywords.items():
            if kw in p:
                filters["sector"] = sec
                break

        div_match = re.search(r'dividend\s*(?:yield)?\s*(?:>|over|above|at least)\s*(\d+\.?\d*)', p)
        if div_match:
            filters["dividend_yield_min"] = float(div_match.group(1))

        print(f"[AI Screener] Extracted filters from prompt: {filters}")
        return filters

    def _ask_claude(self, user_prompt: str, market_data: dict, history: list = None) -> str:
        """Send the user's question + market data to Claude with conversation history."""
        data_str = json.dumps(market_data, indent=2, default=str)

        filters = market_data.get("user_filters", {})
        filter_instructions = ""
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
            recent_history = history[-20:]
            for msg in recent_history:
                messages.append({
                    "role": msg["role"],
                    "content": msg["content"],
                })

        messages.append({
            "role": "user",
            "content": (
                f"## Real-Time Market Data\n"
                f"{data_str}\n\n"
                f"{filter_instructions}\n\n"
                f"## User Question\n"
                f"{user_prompt}"
            ),
        })

        response = self.client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            messages=messages,
        )
        return response.content[0].text

    def _parse_response(self, raw_response: str) -> dict:
        """
        Parse Claude's response into structured JSON.
        Tries multiple strategies:
        1. Raw JSON (entire response is a JSON object)
        2. JSON in ```json``` code block
        3. JSON object embedded in text
        4. Fallback: wrap raw text as chat response
        """
        response_text = raw_response.strip()

        if response_text.startswith("{"):
            try:
                structured_data = json.loads(response_text)
                return {
                    "type": structured_data.get("display_type", "chat"),
                    "analysis": "",
                    "structured": structured_data,
                }
            except json.JSONDecodeError:
                pass

        json_match = re.search(
            r"```json\s*(\{.*?\})\s*```", response_text, re.DOTALL
        )
        if json_match:
            json_start = json_match.start()
            analysis_text = response_text[:json_start].strip()
            try:
                structured_data = json.loads(json_match.group(1))
                return {
                    "type": structured_data.get("display_type", "chat"),
                    "analysis": analysis_text,
                    "structured": structured_data,
                }
            except json.JSONDecodeError:
                pass

        json_match = re.search(r'\{[\s\S]*\}', response_text)
        if json_match:
            try:
                structured_data = json.loads(json_match.group(0))
                pre_json = response_text[:json_match.start()].strip()
                return {
                    "type": structured_data.get("display_type", "chat"),
                    "analysis": pre_json,
                    "structured": structured_data,
                }
            except json.JSONDecodeError:
                pass

        structured_data = {
            "display_type": "chat",
            "message": response_text,
        }
        return {
            "type": "chat",
            "analysis": response_text,
            "structured": structured_data,
        }