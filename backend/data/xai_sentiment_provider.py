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

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop



class XAISentimentProvider:
    """Fetch real-time X/Twitter sentiment via Grok's x_search tool."""

    BASE_URL = "https://api.x.ai/v1"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.model = "grok-4-1-fast-non-reasoning"  # Non-reasoning for data collection
        self.deep_model = "grok-4-1-fast-reasoning"  # Reasoning model for deep scans (matches Grok on x.com)
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
            "User-Agent": "CaelynAgent/1.0",
        }

    @traceable(name="get_ticker_sentiment")
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
Flag any sarcasm you detect.

HIGH-VALUE SIGNAL PRIORITIES (look for these specifically):
- Supply-chain bottleneck positioning: Is this company discussed as critical infrastructure for a larger trend (e.g., power infra for AI, rare earth processing for EVs, optical interconnects for data centers)?
- EBITDA inflection chatter: Any discussion about approaching profitability, first profitable quarter, or cash flow turning positive? This is the highest-conviction catalyst for small/mid-caps.
- Institutional accumulation signals: Posts referencing 13F filings, dark pool activity, unusual block trades, or smart money positioning.
- Earnings drift: Post-earnings momentum discussion — did the company beat and is the stock still re-rating higher?
- Stage transition: Is there technical analysis discussion about the stock breaking out of a long base (months of consolidation followed by volume surge)?"""

        return await self._call_grok_with_x_search(prompt)

    @traceable(name="get_batch_sentiment")
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

    @traceable(name="get_trending_tickers")
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
Always include BTC in trending_tickers even if its velocity is lower.

CRITICAL — SENTIMENT POLARITY FILTER:
A token being TALKED ABOUT is NOT the same as being BULLISH. You MUST distinguish:
- POSITIVE BUZZ: People are excited, accumulating, sharing bullish theses, discussing real catalysts
- NEGATIVE BUZZ: People are mocking it, calling it a bad investment, celebrating its decline, warning others to avoid it, sharing loss porn
- CONTROVERSIAL BUZZ: Genuine debate with both sides making substantive arguments

If a token is trending primarily because people are CRITICIZING it, MOCKING holders, or discussing how BAD it is:
- Set sentiment to "bearish" (NOT "bullish" or "mixed")
- Set trade_sentiment to "sell" or "hold" (NOT "buy")
- In why_trending, clearly state "Trending due to negative sentiment — X users are criticizing/mocking this asset"
- Do NOT recommend buying something just because lots of people are talking about it negatively

The VOLUME of discussion does NOT equal BULLISH sentiment. 10,000 posts saying "$SOL is dead" is BEARISH, not bullish.

ALPHA SIGNAL PRIORITIES (weight these higher when found):
- FUNDING RATE DIVERGENCES: Discussion about tokens with price rising but funding negative (shorts getting squeezed — more upside likely) or price falling but funding positive (longs getting liquidated — more downside). These are the highest-conviction derivatives signals.
- INFRASTRUCTURE BOTTLENECK TOKENS: Tokens tied to critical crypto infrastructure (L1 scaling, cross-chain bridges, oracle networks, decentralized compute) where the token IS the tollbooth for a larger ecosystem. Same logic as equity bottleneck plays.
- NARRATIVE ROTATION TIMING: When X discussion shifts rapidly from one narrative to another (e.g., meme coins → AI tokens → DePIN), the FIRST tokens mentioned in the new narrative have the highest asymmetry. Flag early narrative rotation.
- SMART MONEY ON-CHAIN: Any discussion of whale wallets accumulating, VC unlocks, or protocol treasury movements. On-chain evidence > social hype.
- OI + VOLUME ACCELERATION: Tokens where X discusses surging open interest alongside rising price — this is new money entering, not just spot buying."""

            return await self._call_grok_with_x_search(crypto_prompt, use_deep_model=True)

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

CRITICAL — SENTIMENT POLARITY FILTER:
A stock being TALKED ABOUT is NOT the same as being BULLISH. You MUST distinguish:
- POSITIVE BUZZ: People are excited, sharing bullish theses, discussing catalysts, accumulating
- NEGATIVE BUZZ: People are mocking holders, calling it overvalued, celebrating its decline, warning others
- CONTROVERSIAL: Genuine bull/bear debate with substantive arguments on both sides

If a ticker is trending primarily because people are CRITICIZING it or discussing how BAD it is:
- Set sentiment to "bearish" (NOT "bullish" or "mixed")
- Set trade_sentiment to "sell" or "hold" (NOT "buy")
- In why_trending, clearly state the negative nature of the buzz
- Do NOT recommend buying something just because lots of people are talking about it negatively

High mention volume + negative sentiment = BEARISH signal, not bullish.
Don't be risk-averse about small caps — if they're hot and the thesis is real, include them.
If a low-cap stock has genuine momentum and a real catalyst, say so directly.
Also flag any tickers seeing surging mainstream retail interest beyond just financial social media — Google search trends, mainstream news coverage, TikTok/YouTube buzz. If a ticker is crossing over from financial Twitter into mainstream public awareness, that's a significant signal worth highlighting. Also note any references to Substack newsletters, research reports, or long-form analysis being widely shared on X — these often contain deeper thesis work that precedes major moves.

ALPHA SIGNAL PRIORITIES (weight these higher when found):
- BOTTLENECK COMPANIES: Small/mid-caps ($200M-$5B) discussed as critical supply-chain chokepoints for mega-trends (AI power infrastructure, rare earth processing, optical interconnects, advanced packaging, grid upgrades, battery-grade lithium). The $200M company a $2T trend cannot function without = highest asymmetry.
- EBITDA INFLECTION: Companies approaching first profitable quarter or cash-flow positive transition. This is the single highest-conviction catalyst — when a company flips from cash burn to cash generation, algorithmic models reclassify it and institutional capital unlocks. Flag ANY discussion of approaching profitability.
- LATE STAGE 1 ACCUMULATION: Stocks discussed as building long flat bases with rising volume — the pre-breakout setup. Months of consolidation + increasing institutional buying + sector tailwind = highest probability breakout.
- ASYMMETRIC SETUPS: Tickers where X discusses the "three-legged stool" — undervalued (low P/S vs peers) + rapid revenue ramp + hot sector. All three present = maximum signal.
- SERIAL ACQUIRERS: Companies buying assets at 5x EBITDA and revalued by market at 15x — the M&A arbitrage play. If X is discussing an active acquisition strategy, flag it."""

        return await self._call_grok_with_x_search(prompt, use_deep_model=True)

    @traceable(name="compare_sentiment")
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

    @traceable(name="get_watchlist_social_momentum")
    async def get_watchlist_social_momentum(self, tickers: list, watchlist_context: str = "") -> dict:
        """
        Deep social momentum scan on X for a specific watchlist of tickers.
        Batches tickers into groups of ~17 for parallel Grok calls, then aggregates.
        Uses natural language prompt (not heavy JSON schema) to let Grok search deeply.
        """
        # Split into batches of ~17 for deeper per-batch scanning
        batch_size = 17
        batches = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
        print(f"[XAI_WATCHLIST] Scanning {len(tickers)} tickers in {len(batches)} batches of ~{batch_size}")

        context_block = ""
        if watchlist_context:
            ctx = watchlist_context[:3000]
            context_block = f"\nCONTEXT from user's CSV (market caps, growth, sectors):\n{ctx}\n"

        # Run all batches concurrently
        tasks = [
            self._scan_watchlist_batch(batch, batch_idx + 1, len(batches), context_block)
            for batch_idx, batch in enumerate(batches)
        ]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Aggregate results
        all_analysis = []
        errors = []
        for i, result in enumerate(batch_results):
            if isinstance(result, Exception):
                errors.append(f"Batch {i+1} failed: {result}")
                print(f"[XAI_WATCHLIST] Batch {i+1} exception: {result}")
            elif isinstance(result, dict) and "error" in result:
                errors.append(f"Batch {i+1}: {result['error']}")
                print(f"[XAI_WATCHLIST] Batch {i+1} error: {result['error']}")
            elif isinstance(result, dict):
                # Extract the analysis text — Grok returns natural language
                raw_text = result.get("_raw_analysis", "")
                if raw_text:
                    all_analysis.append(raw_text)
                    print(f"[XAI_WATCHLIST] Batch {i+1}: {len(raw_text)} chars of analysis")
                else:
                    all_analysis.append(json.dumps(result, default=str))
                    print(f"[XAI_WATCHLIST] Batch {i+1}: got JSON result, {len(json.dumps(result))} chars")

        if not all_analysis:
            return {"error": "All watchlist social scan batches failed", "details": errors}

        # Combine all batch analyses into one comprehensive result
        combined = "\n\n---\n\n".join(all_analysis)
        print(f"[XAI_WATCHLIST] Combined analysis: {len(combined)} chars from {len(all_analysis)} batches")

        return {
            "watchlist_social_scan": True,
            "tickers_scanned": len(tickers),
            "batches_completed": len(all_analysis),
            "batches_failed": len(errors),
            "grok_analysis": combined,
            "errors": errors if errors else None,
        }

    @traceable(name="scan_watchlist_batch")
    async def _scan_watchlist_batch(self, tickers: list, batch_num: int, total_batches: int, context_block: str) -> dict:
        """
        Scan a single batch of tickers on X. Uses a natural, less constrained prompt
        so Grok can search deeply and provide rich analysis rather than shallow JSON.
        """
        tickers_str = ", ".join([f"${t}" for t in tickers])

        prompt = f"""Search X/Twitter thoroughly for recent posts (last 7 days, emphasis on last 48 hours) about these specific stocks from a user's watchlist (batch {batch_num}/{total_batches}):

{tickers_str}
{context_block}
CRITICAL RULES:
- ONLY report on the tickers listed above. Do NOT add outside tickers.
- Search for each ticker individually using cashtags (e.g. $IREN, $ONDS, $AAOI).
- For EACH ticker that has ANY meaningful X discussion, provide a detailed report.
- Even small-cap stocks with just a few quality posts matter — a $1B stock with 20 engaged posts has MORE relative momentum than a $100B stock with 200 posts.

For each ticker with social activity, report:
1. BUZZ LEVEL: How many recent posts? How much engagement (likes, retweets, replies)? Is discussion increasing, steady, or declining?
2. SENTIMENT: Is it overwhelmingly bullish, bearish, mixed, or neutral? Is this retail hype or informed thesis-building?
3. WHAT X IS SAYING: Quote or paraphrase the most notable bullish and bearish posts. Reference specific themes (e.g., "multiple threads on AI data center power demand for $IREN", "conviction posts on photonics boom naming $AAOI and $AXTI").
4. CATALYSTS BEING DISCUSSED: What specific catalysts are people talking about? Earnings dates, sector tailwinds, partnerships, technical breakouts, regulatory events.
5. POST QUALITY: Is the buzz from accounts with real followers doing DD/analysis? Or is it spam, pump posts with Discord links, and bots?
6. SECTOR NARRATIVE: If this ticker is part of a broader narrative being discussed on X (AI power infrastructure, photonics/optics, mining cycle, etc.), name that narrative.

For tickers with ZERO or truly negligible X presence, simply list them at the end.

7. ALPHA SIGNALS (look for these specifically — they are the highest-value signals):
   - BOTTLENECK POSITIONING: Is this company discussed as a critical supply-chain chokepoint for a mega-trend? (e.g., the only domestic rare earth processor, the sole provider of optical interconnects for AI data centers, the power infrastructure company that AI buildout depends on)
   - EBITDA INFLECTION: Any discussion of approaching profitability, first profitable quarter, or cash flow turning positive? This is the #1 catalyst for small/mid-caps — when a company flips from cash burn to cash generation, institutional models reclassify it.
   - INSTITUTIONAL ACCUMULATION: 13F filing discussion, dark pool activity, unusual block trades, smart money moves.
   - ASYMMETRIC SETUP: Discussion combining undervalued (low P/S vs peers) + revenue acceleration + sector tailwind = three-legged stool with maximum asymmetry.
   - STAGE TRANSITION: Technical analysis threads about stocks breaking out of long bases with volume confirmation.

Be thorough, direct, and opinionated. Provide specific evidence from what you find on X. Do not be generic — reference actual post themes, engagement levels, and catalysts.

Return your analysis as a clear report. Start with the highest social momentum tickers first, then work down to lower momentum, then list zero-buzz tickers at the end."""

        # Use reasoning model for deep analysis + date filter for last 7 days
        from datetime import datetime, timedelta
        from_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")

        return await self._call_grok_with_x_search(
            prompt,
            timeout=120.0,
            raw_mode=True,
            use_deep_model=True,
            x_search_config={"from_date": from_date},
        )

    @traceable(name="run_x_social_scan")
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

CRITICAL — ETFs vs EQUITIES: ETFs (SPY, QQQ, IWM, XLE, USO, GLD, ARKK, SMH, SOXX, TAN, etc.) are NOT equities. NEVER put an ETF in the equities section. ETFs are funds that track indices, sectors, or commodities — they do not have market caps in the same way stocks do. If an ETF is trending, put it in the separate "etfs" section, NOT in large_caps/mid_caps/small_micro_caps.

Equities buckets and counts (REAL STOCKS ONLY — no ETFs, no index funds):
- large_caps: 2-4 tickers with market cap >= $100B — ALWAYS include at least 2 large caps. Traders need to see what mega-caps are doing. Include any large cap with a catalyst (earnings, news, regulatory, breakout) OR significant price/volume movement today. NVDA counts if AI capex news is moving it. If nothing dramatic, still pick the 2 large caps with the most notable activity today.
- mid_caps: 3-6 tickers with market cap $2B-$100B — This is your sweet spot. Mid-caps with momentum and catalysts are where the best trending signals live. ALWAYS return at least 3.
- small_micro_caps: 3-6 tickers with market cap $50M-$2B — High-conviction small caps with real catalysts, not pump-and-dump noise. Flag any that look like coordinated pumps. ALWAYS return at least 3. There is ALWAYS something popping off in small/micro cap land — find it.

ETFs (separate section):
- 0-3 ETFs that are showing unusual volume, flows, or momentum. Sector ETFs (XLE, XLK, SMH), thematic ETFs (ARKK, TAN, URA), or commodity ETFs (GLD, USO, UNG) — whatever is actually moving.
- Only include ETFs with a clear reason (sector rotation, unusual flows, breakout/breakdown).

Crypto:
- 2-3 tickers MAX — find what is ACTUALLY performing or breaking out in the last 7-30 days. NEVER return more than 3 crypto.
- DO NOT default to ETH or SOL just because they are popular. If ETH is flat or down, do NOT include it.
- Include BTC only if meaningfully relevant (breakout/breakdown/major catalyst/dominant velocity)
- Focus on ACTUAL MOVERS: tokens with 20%+ moves in 7d, new ATHs, protocol upgrades going live, major exchange listings, or unusual volume spikes
- Look for: L2 tokens gaining TVL, DeFi tokens with fee revenue inflections, memecoins with viral catalysts, tokens with upcoming unlocks/burns
- If you cannot find genuinely trending crypto with real catalysts, return fewer items rather than padding with ETH/SOL defaults
- Emphasize velocity/acceleration, not raw mention count

Commodities:
- 2-4 hot commodities/themes
- Include a related equity proxy where appropriate (miner/producer/ETF), e.g. Silver -> EXK

For EACH item you MUST include:
- symbol/commodity, category, reason
- thesis: 1-2 sentence explanation of WHY this ticker is trending right now. Must include: the specific catalyst driving buzz, social velocity context (how fast mentions are accelerating), and sentiment quality (credible accounts vs noise). Example: "3x normal mention velocity driven by H200 order speculation ahead of Jensen keynote tomorrow, overwhelmingly bullish from credible accounts" or "Copper futures at 52-week high on China stimulus data, mining X accounts flagging supply deficit thesis with 2x usual engagement"
- social_velocity: "low"|"medium"|"high"|"extreme"
- mention_velocity_score: 0-100 (how fast mentions are accelerating vs prior hours; 0=no change, 100=explosive spike)
- mention_velocity_label: "low"|"medium"|"high"|"extreme"
- source_mix: {"x": 0-100} (percentage of signal from X; set stocktwits and reddit to null unless you have cross-platform evidence)
- catalyst_hint: short string if a catalyst is evident from posts/news chatter (e.g. "FDA approval", "earnings beat", "short squeeze"), else null
- 2 receipts (one bullish, one bearish if available; both bullish if no bearish exists)

Also return: sector_focus (3-6), top_traders_view (3-6 summaries, no usernames), market_direction_call (1-3 sentences), your_opinion (2-4 sentences).
IMPORTANT BIAS: Favor tickers where social velocity is ACCELERATING (new catalysts, breaking news, fresh momentum) over tickers that are ALWAYS discussed (mega-caps with no new catalyst). The user wants to discover what's NEWLY hot, not what's always popular. But you MUST always return at least 2 large caps, 3 mid caps, and 3 small/micro caps — there is ALWAYS something moving in every tier. A trader's #1 goal is finding opportunities, and empty buckets provide zero value.
If insufficient high-quality data, return fewer items and set data_quality_flag="low".

ALPHA SIGNAL PRIORITIES (weight these higher across ALL asset classes):
- BOTTLENECK COMPANIES: The $200M-$2B company that a $2T mega-trend literally cannot function without (AI power infrastructure, advanced packaging, optical interconnects, rare earth processing, grid upgrades). These have the highest asymmetry — find X discussion identifying supply-chain chokepoints.
- EBITDA INFLECTION: Any company approaching first profitable quarter or cash-flow positive transition. When a company flips from cash burn to cash generation, algorithmic models reclassify it and institutional capital unlocks. Flag this catalyst specifically in catalyst_hint.
- ASYMMETRIC THREE-LEGGED STOOL: Undervalued (low P/S vs peers) + rapid revenue ramp + hot sector tailwind. All three present = maximum setup. If X is discussing this combination for any ticker, it goes to the top.
- LATE STAGE 1 ACCUMULATION (equities): Stocks building long flat bases (months of consolidation) with rising volume and institutional buying signals. This is the pre-breakout setup with highest probability.
- COMMODITIES AS MACRO SIGNALS: Rising commodity prices (uranium, copper, rare earths) signal sector rotation into producers/miners. Map commodity moves to their equity beneficiaries.
- SERIAL ACQUIRER M&A ARBITRAGE: Companies buying assets at 5x EBITDA that the market values at 15x on their platform. If X discusses an active acquisition strategy, flag it.

Return ONLY a JSON object matching this exact schema:
{
  "as_of_utc": "<ISO timestamp>",
  "market_direction_call": "...",
  "sector_focus": ["..."],
  "top_traders_view": ["..."],
  "your_opinion": "...",
  "data_quality_flag": "high|medium|low",
  "equities": {
    "large_caps": [{"symbol":"...","asset_class":"equities","category":"large_cap","reason":"...","thesis":"1-2 sentence WHY trending + catalyst + velocity context","social_velocity":"low|medium|high|extreme","mention_velocity_score":0,"mention_velocity_label":"low|medium|high|extreme","source_mix":{"x":100,"stocktwits":null,"reddit":null},"catalyst_hint":null,"receipts":[{"source":"x","stance":"bullish|bearish","text":"<=20 words"}]}],
    "mid_caps": [<same item format with category="mid_cap">],
    "small_micro_caps": [<same item format with category="small_micro_cap">]
  },
  "etfs": [{"symbol":"...","asset_class":"etf","category":"sector|thematic|commodity|broad_market","name":"Full ETF name","reason":"...","thesis":"...","social_velocity":"...","mention_velocity_score":0,"mention_velocity_label":"...","source_mix":{"x":100,"stocktwits":null,"reddit":null},"catalyst_hint":null,"receipts":[...]}],
  "crypto": [{"symbol":"...","asset_class":"crypto","category":"major|alt","reason":"...","thesis":"...","social_velocity":"...","mention_velocity_score":0,"mention_velocity_label":"...","source_mix":{"x":100,"stocktwits":null,"reddit":null},"catalyst_hint":null,"receipts":[...]}],
  "commodities": [{"commodity":"...","related_equity":"...","reason":"...","thesis":"...","social_velocity":"...","mention_velocity_score":0,"mention_velocity_label":"...","source_mix":{"x":100,"stocktwits":null,"reddit":null},"catalyst_hint":null,"receipts":[...]}]
}"""

            result = await self._call_grok_with_x_search(prompt, use_deep_model=True)

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
                etfs = result.get("etfs", [])
                print(f"[X_CROSS_ASSET] Grok returned: equities={eq_count} etfs={len(etfs)} crypto={len(crypto)} commodities={len(commodities)}")

            result["_scan_mode"] = "cross_asset"
            return result

        return {"error": f"Unknown x_social_scan mode: {mode}", "_scan_mode": mode}

    @traceable(name="validate_cross_asset_schema")
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

        if "etfs" in data and not isinstance(data.get("etfs"), list):
            errors.append("etfs must be a list")

        dqf = data.get("data_quality_flag")
        if dqf is not None and dqf not in ("high", "medium", "low"):
            errors.append(f"data_quality_flag must be high/medium/low, got: {dqf}")

        return (len(errors) == 0, errors)

    @traceable(name="get_market_mood_snapshot")
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

    @traceable(name="get_thematic_conviction_ideas")
    async def get_thematic_conviction_ideas(self) -> dict:
        """
        Ask Grok to identify the highest-conviction long-term investment ideas
        being discussed by serious investors on X right now.
        Returns structured list of tickers with thesis and sector context.
        This is the DISCOVERY layer for Best Investments — thematic first, not Finviz first.
        """
        from data.cache import cache, XAI_THEMATIC_TTL
        cached = cache.get("xai_thematic_investments")
        if cached:
            return cached

        prompt = """Search X/Twitter right now for what serious long-term investors, fund managers, 
and well-followed analysts are discussing as their highest conviction multi-year holdings.

Focus specifically on these sectors where decade-defining companies are being built:
- AI infrastructure: compute, data centers, power, cooling, optical interconnects, semiconductors
- Defense & Aerospace: next-gen weapons, drones, autonomous systems, space
- Quantum computing: hardware, software, cryptography applications
- Energy infrastructure: grid buildout, nuclear revival, LNG, critical power bottlenecks
- Critical materials & mining: rare earths, copper, lithium, uranium — the physical bottlenecks
- Cybersecurity: infrastructure protection, zero-trust, AI-driven security
- Biotech: late-stage clinical breakthroughs, cancer treatments, GLP-1 adjacents
- Financial infrastructure: payments rails, alternative asset managers benefiting from rate environment

Look for tickers being mentioned by respected investors as:
- "the next Microsoft / Google / Lockheed" type compounders
- Bottleneck monopolies: companies that trillion-dollar trends CANNOT function without
- Companies with visionary respected leadership (Jensen Huang, Palantir founders, etc.)
- Names that Congress members are consistently buying (insider confidence signal)
- Companies with genuine competitive moats that will widen over time

Return ONLY a JSON object (no markdown, no backticks):
{
    "thematic_leaders": [
        {
            "ticker": "SYMBOL",
            "company": "Full Company Name",
            "sector": "AI Infrastructure / Defense / Quantum / Energy / Materials / Cybersecurity / Biotech / Finance",
            "thesis_one_liner": "Why this is a decade-defining company in one sentence",
            "bottleneck_factor": "What critical chokepoint does this company control, if any",
            "leadership_signal": "Notable leadership strength or respected founder/CEO if applicable",
            "x_sentiment": "bullish / very_bullish / mixed",
            "mention_quality": "institutional / retail / mixed",
            "why_now": "What specific catalyst or trend is accelerating this company right now",
            "conviction_tier": 1 to 3 (1=highest conviction, 3=watchlist)
        }
    ],
    "dominant_themes": ["theme1", "theme2", "theme3"],
    "sectors_with_most_conviction": ["sector1", "sector2"],
    "summary": "2-3 sentence summary of where serious long-term capital is positioning right now"
}

Return 8-12 tickers. Prioritize Tier 1 conviction. Include at least one from each major sector if genuine conviction exists.
Be specific and opinionated — generic blue chips like AAPL or MSFT only if there is a specific fresh thesis."""

        result = await self._call_grok_with_x_search(prompt, raw_mode=False, timeout=22.0)
        if isinstance(result, dict) and "thematic_leaders" in result:
            cache.set("xai_thematic_investments", result, XAI_THEMATIC_TTL)
            return result
        # If Grok returned raw text, try to extract JSON
        if isinstance(result, dict) and "raw" in result:
            import re as _re
            raw = result["raw"]
            match = _re.search(r'\{.*\}', raw, _re.DOTALL)
            if match:
                import json as _json
                try:
                    parsed = _json.loads(match.group())
                    cache.set("xai_thematic_investments", parsed, XAI_THEMATIC_TTL)
                    return parsed
                except Exception:
                    pass
        return {"thematic_leaders": [], "summary": "Grok thematic scan unavailable", "error": True}

    @traceable(name="call_grok_with_x_search")
    async def _call_grok_with_x_search(
        self,
        prompt: str,
        timeout: float = 60.0,
        raw_mode: bool = False,
        use_deep_model: bool = False,
        x_search_config: dict = None,
        system_text: str = None,
    ) -> dict:
        """
        Call the xAI Responses API with x_search enabled.
        If raw_mode=True, returns the raw text analysis instead of trying to parse JSON.
        If use_deep_model=True, uses the reasoning model for deeper X searching.
        x_search_config can include from_date, to_date, etc.
        system_text, when provided, is prepended as a system message before the user prompt
        so that full trading context (system prompt + market data) is preserved when Grok
        acts as a final or solo reasoning model.
        """
        model = self.deep_model if use_deep_model else self.model
        x_search_opts = x_search_config or {}

        input_messages = []
        if system_text:
            input_messages.append({"role": "system", "content": system_text})
        input_messages.append({"role": "user", "content": prompt})

        payload = {
            "model": model,
            "tools": [
                {
                    "type": "x_search",
                    "x_search": x_search_opts,
                }
            ],
            "input": input_messages,
        }

        ctx_tag = f", system={len(system_text):,}chars" if system_text else ""
        print(f"[XAI] Calling {model} (raw_mode={raw_mode}, x_search_config={x_search_opts}{ctx_tag})")

        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
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

            if raw_mode:
                # Return raw text analysis — don't try to parse as JSON
                print(f"[XAI] Raw mode: returning {len(text)} chars of analysis")
                return {"_raw_analysis": text, "success": True}

            return self._parse_json_response(text)

        except httpx.TimeoutException:
            print(f"[XAI] Request timed out after {timeout}s")
            return {"error": f"xAI request timed out after {timeout}s"}
        except Exception as e:
            print(f"[XAI] Error: {e}")
            return {"error": str(e)}

    @traceable(name="extract_text")
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

    @traceable(name="parse_json_response")
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
