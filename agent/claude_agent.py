import json
import re

import anthropic

from agent.prompts import SYSTEM_PROMPT, QUERY_CLASSIFIER_PROMPT
from data.market_data_service import MarketDataService


class TradingAgent:
    def __init__(self, api_key: str, data_service: MarketDataService):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.data = data_service

    async def query(self, user_prompt: str) -> dict:
        """
        Main entry point. Takes a user's question, gathers relevant
        market data, sends it to Claude, and returns a structured response.
        """

        # Step 1: Figure out what kind of query this is
        query_info = self._classify_query(user_prompt)

        # Step 2: Fetch the right data based on query type
        market_data = await self._gather_data(query_info)

        # Step 3: Send everything to Claude for analysis
        raw_response = self._ask_claude(user_prompt, market_data)

        # Step 4: Parse the response into text + structured data
        parsed = self._parse_response(raw_response)

        return parsed

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

        if category == "ticker_analysis":
            tickers = query_info.get("tickers", [])
            results = {}
            for ticker in tickers[:5]:  # Limit to 5 tickers
                results[ticker] = await self.data.research_ticker(ticker)
            return results

        elif category == "market_scan":
            return await self.data.scan_market()

        elif category == "unusual_volume":
            return await self.data.get_unusual_volume()

        elif category == "oversold":
            return await self.data.get_oversold()

        elif category == "overbought":
            return await self.data.get_overbought()

        else:
            # General question — still provide some market context
            return {
                "market_news": self.data.polygon.get_news(limit=10),
            }

    def _ask_claude(self, user_prompt: str, market_data: dict) -> str:
        """Send the user's question + market data to Claude."""
        # Convert market data to readable string
        data_str = json.dumps(market_data, indent=2, default=str)

        response = self.client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"## Real-Time Market Data\n"
                        f"{data_str}\n\n"
                        f"## User Question\n"
                        f"{user_prompt}"
                    ),
                }
            ],
        )
        return response.content[0].text

    def _parse_response(self, raw_response: str) -> dict:
        """
        Split Claude's response into the text analysis and the
        structured JSON data for the frontend.
        """
        # Find the JSON block at the end of the response
        json_match = re.search(
            r"```json\s*(\{.*?\})\s*```", raw_response, re.DOTALL
        )

        if json_match:
            # Split: everything before JSON is the analysis text
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
            "data": structured_data.get("rows"),
            "tickers": structured_data.get("tickers"),
            "technicals": structured_data.get("technicals"),
        }