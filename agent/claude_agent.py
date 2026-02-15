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
            return await self.data.get_sector_rotation()

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
        Parse Claude's response into text analysis + structured JSON.
        Passes the structured data through to the frontend as-is,
        since the frontend knows how to render each display_type.
        """
        json_match = re.search(
            r"```json\s*(\{.*?\})\s*```", raw_response, re.DOTALL
        )

        if json_match:
            json_start = json_match.start()
            analysis_text = raw_response[:json_start].strip()
            try:
                structured_data = json.loads(json_match.group(1))
            except json.JSONDecodeError:
                structured_data = {"display_type": "chat"}
        else:
            analysis_text = raw_response.strip()
            structured_data = {"display_type": "chat"}

        return {
            "type": structured_data.get("display_type", "chat"),
            "analysis": analysis_text,
            "structured": structured_data,
        }