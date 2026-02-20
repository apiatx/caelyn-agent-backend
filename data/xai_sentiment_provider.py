"""
xAI Grok X Sentiment Provider

Uses Grok's native x_search tool to scan X/Twitter for real-time
social sentiment on stocks and crypto. Grok searches X autonomously
and returns structured sentiment analysis.

This is an OpenAI-compatible API — xAI uses the same interface.
"""

import json
import httpx
import asyncio
import re


class XAISentimentProvider:
    """Fetch real-time X/Twitter sentiment via Grok's x_search tool."""

    BASE_URL = "https://api.x.ai/v1"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.model = "grok-4-1-fast-non-reasoning"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }

    async def get_ticker_sentiment(self, ticker: str, asset_type: str = "stock") -> dict:
        """
        Get X/Twitter sentiment for a single ticker.
        Returns structured sentiment data that Claude can use.
        """
        if asset_type == "crypto":
            search_context = f"${ticker} OR #{ticker} cryptocurrency crypto"
        else:
            search_context = f"${ticker} stock"

        prompt = f"""Search X for recent posts about {search_context} from the last 24 hours.

Analyze the sentiment and return ONLY a JSON object (no markdown, no backticks):
{{
    "ticker": "{ticker}",
    "asset_type": "{asset_type}",
    "overall_sentiment": "bullish" | "bearish" | "neutral" | "mixed",
    "sentiment_score": -1.0 to 1.0,
    "confidence": 0.0 to 1.0,
    "bullish_pct": 0-100,
    "bearish_pct": 0-100,
    "neutral_pct": 0-100,
    "post_volume": "high" | "medium" | "low",
    "volume_trend": "surging" | "rising" | "stable" | "declining",
    "key_themes": ["theme1", "theme2", "theme3"],
    "notable_signals": [
        {{"signal": "description of notable post or pattern", "sentiment": "bullish/bearish", "influence": "high/medium/low"}}
    ],
    "catalysts_mentioned": ["catalyst1", "catalyst2"],
    "risk_flags": ["flag1", "flag2"],
    "influencer_sentiment": "bullish" | "bearish" | "neutral" | "mixed",
    "retail_vs_smart_money": "Brief note on whether chatter seems retail-driven or informed",
    "summary": "2-3 sentence summary of what X is saying about this ticker right now"
}}

Be direct and opinionated. If sentiment is strongly one-directional, say so.
If posts are mostly noise with no real signal, flag that.
Focus on posts from accounts with real followers, not bots.
Flag any sarcasm you detect."""

        return await self._call_grok_with_x_search(prompt)

    async def get_batch_sentiment(self, tickers: list, asset_type: str = "stock") -> dict:
        """
        Get X sentiment for multiple tickers. Runs concurrently in batches.
        """
        results = {}
        batch_size = 3

        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            tasks = [self.get_ticker_sentiment(t, asset_type) for t in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            for ticker, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    print(f"[XAI] {ticker} sentiment failed: {result}")
                    results[ticker] = {"ticker": ticker, "error": str(result)}
                else:
                    results[ticker] = result

            if i + batch_size < len(tickers):
                await asyncio.sleep(1.0)

        return results

    async def get_trending_tickers(
        self,
        asset_type: str = "stock",
        sectors: list = None,
        max_market_cap: str = None,
    ) -> dict:
        """
        Ask Grok to find the most talked-about tickers on X right now.
        Optionally filter by sectors and market cap.
        """
        sector_filter = ""
        if sectors:
            sector_filter = f"\nFocus specifically on these sectors: {', '.join(sectors)}."

        cap_filter = ""
        if max_market_cap:
            cap_filter = f"\nOnly include tickers with market cap below {max_market_cap}."

        if asset_type == "crypto":
            asset_context = "cryptocurrencies, tokens, and DeFi projects"
            examples = "$BTC, $ETH, $SOL, altcoins, meme coins"

            crypto_prompt = """Search X for the most actively discussed cryptocurrencies and tokens right now (last 12-24 hours).

PRIORITY SIGNALS:
1. SOCIAL VELOCITY: Which crypto tokens have seen the SHARPEST INCREASE in X mentions in the last 24 hours? A coin going from 100 mentions to 1000 mentions is more interesting than one with steady 5000 mentions.
2. SENTIMENT MOMENTUM: Which tokens are flipping from negative/neutral to POSITIVE sentiment?
3. BTC SENTIMENT: What is the overall X sentiment on BTC right now? Is crypto twitter bullish, bearish, or indifferent?
4. ALTCOIN HYPE: Which specific altcoins are getting disproportionate attention? Include ALL cap sizes — micro-caps with sudden buzz are HIGHER signal than established large caps with normal attention.
5. NARRATIVE PLAYS: What crypto narratives are heating up? (AI tokens, gaming, RWA, meme coins, L2s, DePIN, etc.)

DO NOT limit to large caps. A $50M token with exploding social interest is MORE valuable signal than ETH getting normal discussion.

Return ONLY a JSON object (no markdown, no backticks):
{
    "scan_type": "x_crypto_sentiment",
    "timestamp": "now",
    "btc_sentiment": {
        "overall": "bullish" | "bearish" | "neutral" | "mixed",
        "score": -1.0 to 1.0,
        "key_narrative": "What crypto twitter is saying about BTC right now",
        "notable_calls": ["Any notable trader/influencer BTC calls"]
    },
    "market_mood": "risk-on" | "risk-off" | "mixed" | "euphoric" | "fearful",
    "trending_tickers": [
        {
            "ticker": "SYMBOL",
            "mention_intensity": "extreme" | "high" | "medium",
            "social_velocity": "exploding" | "surging" | "rising" | "steady",
            "sentiment": "bullish" | "bearish" | "mixed",
            "sentiment_score": -1.0 to 1.0,
            "why_trending": "2-3 sentence explanation of what sparked the social buzz",
            "key_narratives": ["narrative1", "narrative2"],
            "catalyst": "Specific event driving discussion",
            "risk_flag": "Pump signals, bot activity, etc. or null",
            "estimated_market_cap_tier": "large" | "mid" | "small" | "micro" | "unknown",
            "trade_sentiment": "strong_buy" | "buy" | "hold" | "sell" | "speculative"
        }
    ],
    "narrative_heat": [
        {"narrative": "name", "buzz_level": "hot" | "warm" | "cold", "direction": "bullish" | "bearish", "top_tokens": ["SYM1", "SYM2"]}
    ],
    "contrarian_signals": ["Cases where X sentiment diverges from price action"],
    "summary": "3-4 sentence overview of crypto X sentiment right now"
}

Return 10-15 tickers sorted by social velocity (fastest-growing mentions first).
Flag coordinated pump signals. Include genuine micro-cap momentum if the catalyst is real.
Always include BTC in trending_tickers even if its velocity is lower."""

            return await self._call_grok_with_x_search(crypto_prompt)

        else:
            asset_context = "stocks and ETFs"
            examples = "$NVDA, $TSLA, $AAPL, small caps, penny stocks"

        prompt = f"""Search X for the most actively discussed {asset_context} right now (last 12 hours).
{sector_filter}{cap_filter}

I want tickers that are BUZZING — not just mentioned once, but actively debated, hyped, or feared.
Look for: cashtags like {examples}, earnings reactions, breaking news, momentum plays, squeeze chatter.

Return ONLY a JSON object (no markdown, no backticks):
{{
    "scan_type": "x_trending_{asset_type}",
    "timestamp": "now",
    "market_mood": "risk-on" | "risk-off" | "mixed" | "euphoric" | "fearful",
    "trending_tickers": [
        {{
            "ticker": "SYMBOL",
            "mention_intensity": "extreme" | "high" | "medium",
            "sentiment": "bullish" | "bearish" | "mixed" | "neutral",
            "sentiment_score": -1.0 to 1.0,
            "why_trending": "2-3 sentence explanation of WHY this is being talked about",
            "key_narratives": ["narrative1", "narrative2"],
            "catalyst": "The specific event or news driving discussion",
            "risk_flag": "Any red flag (pump & dump signals, bot activity, etc.) or null",
            "influencer_driven": true/false,
            "estimated_market_cap_tier": "mega" | "large" | "mid" | "small" | "micro" | "unknown",
            "trade_sentiment": "strong_buy" | "buy" | "hold" | "sell" | "strong_sell" | "speculative"
        }}
    ],
    "sector_heat": [
        {{"sector": "name", "buzz_level": "hot/warm/cold", "direction": "bullish/bearish"}}
    ],
    "notable_themes": ["theme1", "theme2"],
    "contrarian_signals": ["Any cases where X sentiment diverges from price action"],
    "summary": "3-4 sentence overview of what X is talking about in markets right now"
}}

Return the top 10-15 most actively discussed tickers, sorted by mention intensity.
Be ruthless about quality — skip bot-driven noise and focus on real human discussion.
If you see signs of coordinated pumping, FLAG IT.
Don't be risk-averse about small caps — if they're hot and the thesis is real, include them.
If a low-cap stock has genuine momentum and a real catalyst, say so directly.
Also flag any tickers seeing surging mainstream retail interest beyond just financial social media — Google search trends, mainstream news coverage, TikTok/YouTube buzz. If a ticker is crossing over from financial Twitter into mainstream public awareness, that's a significant signal worth highlighting. Also note any references to Substack newsletters, research reports, or long-form analysis being widely shared on X — these often contain deeper thesis work that precedes major moves."""

        return await self._call_grok_with_x_search(prompt)

    async def compare_sentiment(self, tickers: list) -> dict:
        """Compare X sentiment head-to-head across multiple tickers."""
        tickers_str = ", ".join([f"${t}" for t in tickers])

        prompt = f"""Search X for recent discussion (last 24 hours) about these tickers: {tickers_str}

Compare the sentiment and buzz level for each. Return ONLY a JSON object:
{{
    "comparison": [
        {{
            "ticker": "SYMBOL",
            "sentiment_score": -1.0 to 1.0,
            "buzz_volume": "high" | "medium" | "low",
            "trend_direction": "improving" | "stable" | "deteriorating",
            "dominant_narrative": "Brief description",
            "x_consensus": "What does X think you should do with this?"
        }}
    ],
    "strongest_sentiment": "Ticker with most bullish X sentiment",
    "most_controversial": "Ticker with most divided opinion",
    "highest_buzz": "Ticker with most discussion volume",
    "insights": ["Key cross-ticker insight 1", "Key insight 2"]
}}

Be direct. Which of these does X like best right now and why?"""

        return await self._call_grok_with_x_search(prompt)

    async def run_x_social_scan(self, mode: str, query: str = "", constraints: dict = None) -> dict:
        """
        Unified entry point for x_social_scan module.

        Modes:
          - trending: discover what's buzzing on X right now
          - sentiment: get sentiment for specific tickers (pass tickers in constraints)
          - compare: head-to-head sentiment comparison across tickers
          - narrative: free-form query — ask Grok anything about X market chatter

        constraints dict may include:
          - tickers: list of ticker symbols
          - asset_type: "stock" | "crypto" (default "stock")
          - sectors: list of sector strings
          - max_market_cap: str like "2B"
          - lookback: str like "24h" or "7d"
        """
        if constraints is None:
            constraints = {}

        tickers = constraints.get("tickers", [])
        asset_type = constraints.get("asset_type", "stock")
        sectors = constraints.get("sectors")
        max_market_cap = constraints.get("max_market_cap")
        lookback = constraints.get("lookback", "24h")

        print(f"[X_SOCIAL_SCAN] mode={mode} query={query[:80]} tickers={tickers[:5]} asset_type={asset_type}")

        if mode == "trending":
            result = await self.get_trending_tickers(
                asset_type=asset_type,
                sectors=sectors,
                max_market_cap=max_market_cap,
            )
            result["_scan_mode"] = "trending"
            return result

        if mode == "sentiment":
            if not tickers:
                return {"error": "sentiment mode requires tickers in constraints", "_scan_mode": "sentiment"}
            if len(tickers) == 1:
                result = await self.get_ticker_sentiment(tickers[0], asset_type)
            else:
                result = await self.get_batch_sentiment(tickers[:10], asset_type)
            if isinstance(result, dict):
                result["_scan_mode"] = "sentiment"
            return result

        if mode == "compare":
            if not tickers or len(tickers) < 2:
                return {"error": "compare mode requires at least 2 tickers", "_scan_mode": "compare"}
            result = await self.compare_sentiment(tickers[:8])
            result["_scan_mode"] = "compare"
            return result

        if mode == "narrative":
            narrative_prompt = f"""Search X for recent posts ({lookback}) related to: {query}

Analyze what people on X are saying about this topic in the context of financial markets and trading.

Return ONLY a JSON object (no markdown, no backticks):
{{
    "query": "{query}",
    "scan_mode": "narrative",
    "market_relevance": "high" | "medium" | "low",
    "sentiment_direction": "bullish" | "bearish" | "neutral" | "mixed",
    "confidence": 0.0 to 1.0,
    "key_narratives": ["narrative1", "narrative2", "narrative3"],
    "mentioned_tickers": [
        {{"ticker": "SYMBOL", "sentiment": "bullish/bearish/neutral", "context": "why mentioned"}}
    ],
    "catalysts_discussed": ["catalyst1", "catalyst2"],
    "risk_flags": ["flag1"],
    "contrarian_signals": ["any divergence between X sentiment and price action"],
    "influencer_consensus": "What are credible accounts saying vs retail noise",
    "summary": "3-4 sentence synthesis of what X thinks about this topic right now"
}}

Be direct and opinionated. Separate signal from noise. Flag bot activity or coordinated pumping."""

            result = await self._call_grok_with_x_search(narrative_prompt)
            result["_scan_mode"] = "narrative"
            return result

        if mode == "cross_asset":
            prompt = """You are a market social intelligence analyst. Scan X for real-time trading chatter and extract high-signal tradable assets with evidence.
Rules:
- Output valid JSON only. No markdown.
- Do not hallucinate tickers, catalysts, or posts.
- Receipts must be short verbatim excerpts from real posts (max 20 words). Do not include usernames. Do not include links.
- Label each receipt with source="x" and stance="bullish" or "bearish".
- Use credibility heuristics (verified/high engagement/consistent trading content). You cannot claim "accredited".
- Avoid spam; if spam is dominant, explicitly flag it via data_quality_flag and your_opinion.

Scan X in real time for what is trending across markets and return a structured cross-asset shortlist.

Equities buckets and counts:
- large_caps: 0-2 tickers with market cap >= $100B — ONLY include a large cap if it has a SPECIFIC catalyst driving unusual social activity RIGHT NOW (earnings surprise, major news, regulatory event, technical breakout to new highs). Do NOT include a large cap just because it gets mentioned a lot — AAPL, MSFT, NVDA are always discussed. Include them ONLY if something NEW and material is happening today.
- mid_caps: 3-6 tickers with market cap $15B-$100B — This is your sweet spot. Mid-caps with momentum and catalysts are where the best trending signals live.
- small_micro_caps: 3-6 tickers with market cap $50M-$15B — High-conviction small caps with real catalysts, not pump-and-dump noise. Flag any that look like coordinated pumps.

Crypto:
- 2-4 tickers
- Include BTC only if meaningfully relevant (breakout/breakdown/major catalyst/dominant velocity)
- If BTC sideways and alts have accelerating velocity, focus on alts
- Emphasize velocity/acceleration, not raw mention count

Commodities:
- 2-4 hot commodities/themes
- Include a related equity proxy where appropriate (miner/producer/ETF), e.g. Silver -> EXK

For EACH item you MUST include:
- symbol/commodity, category, reason
- social_velocity: "low"|"medium"|"high"|"extreme"
- mention_velocity_score: 0-100 (how fast mentions are accelerating vs prior hours; 0=no change, 100=explosive spike)
- mention_velocity_label: "low"|"medium"|"high"|"extreme"
- source_mix: {"x": 0-100} (percentage of signal from X; set stocktwits and reddit to null unless you have cross-platform evidence)
- catalyst_hint: short string if a catalyst is evident from posts/news chatter (e.g. "FDA approval", "earnings beat", "short squeeze"), else null
- 2 receipts (one bullish, one bearish if available; both bullish if no bearish exists)

Also return: sector_focus (3-6), top_traders_view (3-6 summaries, no usernames), market_direction_call (1-3 sentences), your_opinion (2-4 sentences).
IMPORTANT BIAS: Favor tickers where social velocity is ACCELERATING (new catalysts, breaking news, fresh momentum) over tickers that are ALWAYS discussed (mega-caps with no new catalyst). The user wants to discover what's NEWLY hot, not what's always popular. If you can only find 1 genuine large-cap catalyst, return only 1 large cap. Zero large caps is acceptable if nothing meaningful is happening in mega-cap land.
If insufficient high-quality data, return fewer items and set data_quality_flag="low".

Return ONLY a JSON object matching this exact schema:
{
  "as_of_utc": "<ISO timestamp>",
  "market_direction_call": "...",
  "sector_focus": ["..."],
  "top_traders_view": ["..."],
  "your_opinion": "...",
  "data_quality_flag": "high|medium|low",
  "equities": {
    "large_caps": [{"symbol":"...","asset_class":"equities","category":"large_cap","reason":"...","social_velocity":"low|medium|high|extreme","mention_velocity_score":0,"mention_velocity_label":"low|medium|high|extreme","source_mix":{"x":100,"stocktwits":null,"reddit":null},"catalyst_hint":null,"receipts":[{"source":"x","stance":"bullish|bearish","text":"<=20 words"}]}],
    "mid_caps": [<same item format with category="mid_cap">],
    "small_micro_caps": [<same item format with category="small_micro_cap">]
  },
  "crypto": [{"symbol":"...","asset_class":"crypto","category":"major|alt","reason":"...","social_velocity":"...","mention_velocity_score":0,"mention_velocity_label":"...","source_mix":{"x":100,"stocktwits":null,"reddit":null},"catalyst_hint":null,"receipts":[...]}],
  "commodities": [{"commodity":"...","related_equity":"...","reason":"...","social_velocity":"...","mention_velocity_score":0,"mention_velocity_label":"...","source_mix":{"x":100,"stocktwits":null,"reddit":null},"catalyst_hint":null,"receipts":[...]}]
}"""

            result = await self._call_grok_with_x_search(prompt)

            if "error" not in result:
                is_valid, errors = self._validate_cross_asset_schema(result)
                if not is_valid:
                    print(f"[X_CROSS_ASSET] Schema validation failed (attempt 1): {errors}")
                    retry_prompt = prompt + "\n\nIMPORTANT: Return ONLY valid JSON. No markdown, no extra text. Just the JSON object."
                    result = await self._call_grok_with_x_search(retry_prompt)
                    if "error" not in result:
                        is_valid, errors = self._validate_cross_asset_schema(result)
                        if not is_valid:
                            print(f"[X_CROSS_ASSET] Schema validation failed (attempt 2): {errors}")
                            return {"error": "schema_validation_failed", "grok_available": False, "raw": str(result)[:500]}

            if "error" not in result:
                eq = result.get("equities", {})
                eq_count = len(eq.get("large_caps", [])) + len(eq.get("mid_caps", [])) + len(eq.get("small_micro_caps", []))
                crypto = result.get("crypto", [])
                commodities = result.get("commodities", [])
                print(f"[X_CROSS_ASSET] Grok returned: equities={eq_count} crypto={len(crypto)} commodities={len(commodities)}")

            result["_scan_mode"] = "cross_asset"
            return result

        return {"error": f"Unknown x_social_scan mode: {mode}", "_scan_mode": mode}

    def _validate_cross_asset_schema(self, data: dict) -> tuple[bool, list]:
        errors = []
        required_keys = ["as_of_utc", "market_direction_call", "sector_focus",
                         "top_traders_view", "your_opinion", "data_quality_flag",
                         "equities", "crypto", "commodities"]
        for key in required_keys:
            if key not in data:
                errors.append(f"missing top-level key: {key}")

        equities = data.get("equities")
        if isinstance(equities, dict):
            for sub_key in ["large_caps", "mid_caps", "small_micro_caps"]:
                val = equities.get(sub_key)
                if not isinstance(val, list):
                    errors.append(f"equities.{sub_key} must be a list")
        elif "equities" in data:
            errors.append("equities must be a dict with large_caps, mid_caps, small_micro_caps")

        if "crypto" in data and not isinstance(data.get("crypto"), list):
            errors.append("crypto must be a list")

        if "commodities" in data and not isinstance(data.get("commodities"), list):
            errors.append("commodities must be a list")

        dqf = data.get("data_quality_flag")
        if dqf is not None and dqf not in ("high", "medium", "low"):
            errors.append(f"data_quality_flag must be high/medium/low, got: {dqf}")

        return (len(errors) == 0, errors)

    async def get_market_mood_snapshot(self) -> dict:
        from data.cache import cache

        cache_key = "xai:market_mood_snapshot"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        prompt = """Search X for the overall stock market mood RIGHT NOW. What is the dominant sentiment among traders on X today?

Return ONLY a JSON object (no markdown, no backticks):
{
    "mood": "risk-on" | "risk-off" | "mixed" | "euphoric" | "fearful" | "choppy",
    "mood_score": -1.0 to 1.0,
    "dominant_narratives": ["narrative1", "narrative2", "narrative3"],
    "hot_sectors": ["sector1", "sector2"],
    "avoid_sectors": ["sector1"],
    "trader_consensus": "1-2 sentence summary of what traders on X are focused on right now",
    "contrarian_note": "Anything where X consensus looks wrong, or null"
}

Keep it tight. This is a mood check, not a full scan."""

        result = await self._call_grok_with_x_search(prompt)

        if result and isinstance(result, dict) and "error" not in result:
            cache.set(cache_key, result, 180)

        return result

    async def _call_grok_with_x_search(self, prompt: str) -> dict:
        """Call the xAI Responses API with x_search enabled."""
        payload = {
            "model": self.model,
            "tools": [
                {
                    "type": "x_search",
                    "x_search": {}
                }
            ],
            "input": [
                {"role": "user", "content": prompt}
            ],
        }

        try:
            async with httpx.AsyncClient(timeout=45.0) as client:
                response = await client.post(
                    f"{self.BASE_URL}/responses",
                    headers=self.headers,
                    json=payload,
                )

            if response.status_code != 200:
                error_text = response.text[:500]
                print(f"[XAI] API error {response.status_code}: {error_text}")
                return {"error": f"xAI API returned {response.status_code}", "detail": error_text}

            data = response.json()

            text = self._extract_text(data)

            if not text:
                print(f"[XAI] No text in response. Keys: {list(data.keys())}")
                return {"error": "No text in Grok response", "raw": str(data)[:500]}

            return self._parse_json_response(text)

        except httpx.TimeoutException:
            print("[XAI] Request timed out after 45s")
            return {"error": "xAI request timed out"}
        except Exception as e:
            print(f"[XAI] Error: {e}")
            return {"error": str(e)}

    def _extract_text(self, data: dict) -> str:
        """Extract text content from xAI Responses API output."""
        output = data.get("output", [])
        texts = []
        for item in output:
            if item.get("type") == "message":
                for content_block in item.get("content", []):
                    if content_block.get("type") == "output_text":
                        texts.append(content_block.get("text", ""))
                    elif content_block.get("type") == "text":
                        texts.append(content_block.get("text", ""))
        return "\n".join(texts).strip()

    def _parse_json_response(self, text: str) -> dict:
        """Parse JSON from Grok's response, handling various formats."""
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        cleaned = re.sub(r"```json\s*", "", text)
        cleaned = re.sub(r"```\s*", "", cleaned)
        cleaned = cleaned.strip()

        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            pass

        first_brace = cleaned.find("{")
        if first_brace != -1:
            depth = 0
            for i in range(first_brace, len(cleaned)):
                if cleaned[i] == "{":
                    depth += 1
                elif cleaned[i] == "}":
                    depth -= 1
                    if depth == 0:
                        try:
                            return json.loads(cleaned[first_brace:i + 1])
                        except json.JSONDecodeError:
                            break

        print(f"[XAI] Could not parse JSON, returning raw text")
        return {
            "error": "Could not parse structured response",
            "raw_text": text[:2000],
            "overall_sentiment": "unknown",
        }
