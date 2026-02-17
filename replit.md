# Trading Analysis Platform - FastAPI Backend

## Overview
This project is a sophisticated Python FastAPI backend for a trading analysis platform. Its primary purpose is to integrate real-time market data from over 15 sources with Claude AI to generate actionable trading insights. The system aims to provide a "master trader" perspective, focusing on conviction picks, macro-contextual analysis, and ruthless filtering. It supports both long-term investment strategies (using the SQGLP framework) and short-term trading (Weinstein stage analysis, momentum, and catalyst-driven approaches). The platform's ambition is to empower users with hedge-fund-style intelligence through comprehensive market scanning, sentiment analysis, and AI-driven recommendations.

## User Preferences
- SQGLP framework for investments, Weinstein stage analysis for trades
- Only recommend Stage 2 breakouts
- Focus on small/mid-cap under $2B (power law returns)
- "Best Trades" = finding SETUPS (multiple indicators aligning), not chasing momentum
- Light enrichment batch size: 30/40 candidates (reduced for faster responses)

## System Architecture
The platform is built on FastAPI, offering a robust and scalable backend.
- **Core Functionality**: The `market_data_service.py` acts as an orchestrator, handling wide market scanning, data enrichment, and quantitative pre-scoring. The `claude_agent.py` integrates Claude AI, utilizing various prompts defined in `prompts.py` for different analysis types.
- **Data Pipeline**: Employs a "wide funnel" approach, screening 50-100+ candidates, mathematically ranking them, and then sending the top 12 to Claude for deep analysis.
- **Scan Types**: Supports over 13 diverse scan types, including social momentum, sector rotation, squeeze plays, thematic investing, commodities, SQGLP, asymmetric trades, Weinstein analysis, portfolio review, morning briefings, cross-platform trending, cross-market (multi-asset), and a dedicated crypto scanner.
- **Cross-Market Scan**: When user asks about "all markets" or mentions 2+ asset classes (stocks + crypto + commodities), triggers `cross_market` category that pulls data from ALL asset classes in parallel. Uses `data/cross_asset_ranker.py` for quantitative pre-ranking BEFORE Claude sees the data. Prevents crypto from flooding results by: (1) extracting individual candidates from each asset class, (2) scoring within each class independently then normalizing to 0-100 scale, (3) applying hard filters (market cap floor $500M stocks/$100M crypto, volume minimums), (4) enforcing multi-factor confluence (3/5 minimum: social momentum, technical, catalyst, sector alignment, liquidity), (5) macro regime penalty (risk-off = penalize speculative small caps/altcoins, boost safe havens), (6) asset-class quota enforcement (at least 1 stock + 1 commodity if available). Diagnostic logging tracks candidate counts, filter rejections, and selection reasons.
- **Conversational AI**: The system is fully conversational, supporting multi-turn interactions where Claude receives conversation history and can answer follow-ups with prior context. It intelligently triggers fresh data gathering for new scans or ticker mentions.
- **Caching**: An in-memory TTL caching system (`cache.py`) is implemented across all data providers to optimize API calls and improve response times.
- **Error Handling & Reliability**: Includes comprehensive timeout mechanisms (global request, classifier, data gathering, Claude API) and keyword-based fallback classification for enhanced robustness.
- **UI/UX Considerations**: The API is designed to deliver concise, dense trading terminal-style JSON output. Analysis results consistently include TradingView chart links for easy visualization.
- **Portfolio Management**: Features include portfolio review capabilities for up to 25 tickers with dual scoring (trade + investment metrics) and endpoints for managing holdings and events.
- **Portfolio Quotes**: Asset-type-aware routing — tickers are separated by asset_type (stock/crypto/commodity/index) BEFORE any API calls. Request format: `{tickers: [...], asset_types: {TICKER: "crypto"|"stock"|"commodity"|"index"}}`. Priority overrides ensure major crypto (BTC, ETH, HYPE, etc.) resolve to correct CoinGecko IDs. Every quote includes a guaranteed `sector` field for portfolio charts.
  - **Stock quote pipeline**: Finnhub primary (parallel quote + profile fetch, profile cached 24h) → Yahoo Finance fallback for missing tickers → FMP last resort. Sector/industry/company_name enriched via Finnhub profile first, then FMP profile fallback.
  - **Crypto quote pipeline**: CoinGecko primary (batched /simple/price) → CoinMarketCap fallback ONLY when CoinGecko returns 429 rate limit. USD/USDT suffix handling for pair-style tickers (e.g., BTCUSD → BTC).
  - **Index pipeline**: Yahoo Finance chart API with proper symbol mapping (SPX→^GSPC, DJI→^DJI, VIX→^VIX, DXY→DX-Y.NYB).
  - **Cache strategy**: 60-second cache on full quote responses, 24-hour cache on sector/profile data.

## External Dependencies
The platform integrates with a wide array of third-party services and APIs to gather comprehensive market data and provide AI capabilities:
- **AI**: OpenAI GPT-4o (orchestrator/query classifier, temperature 0.1) + Anthropic Claude Sonnet (reasoning/analysis layer). Dual-LLM architecture: OpenAI handles deterministic query classification, Claude handles deep market analysis. Cross-market scans use faster Claude Sonnet 4 (non-thinking) for speed; all other scans use Claude Sonnet 4.5 (thinking model) for depth.
- **Cross-Market Performance**: Lightweight data gathering methods with per-source timeouts (25s stocks/crypto, 15s commodities/macro), pre-slimming via `_slim_cross_market_data()`, 25K char data cap, 4096 max_tokens. Total budget: 40s data + 90s Claude < 150s request timeout.
- **Market Data & Screening**:
    - Finviz (screener)
    - Polygon.io (technicals from bar data, news - used for single-ticker research)
    - Finnhub (primary stock quotes, company profiles, insider trading, earnings, recommendations, social sentiment)
    - Financial Modeling Prep (FMP) (fallback quotes, commodities, portfolio events, economic calendar, treasury rates, gainers/losers/actives)
    - Alpha Vantage (AI-powered news sentiment ONLY — macro methods removed, FRED handles all macro data)
    - Nasdaq (economic calendar)
- **Social Sentiment & Trending**:
    - Reddit/ApeWisdom (WSB, r/stocks, r/options, r/investing, r/daytrading sentiment)
    - StockTwits (social sentiment)
    - Yahoo Finance (trending)
    - xAI Grok (real-time X/Twitter sentiment via x_search tool, model: grok-4-1-fast-non-reasoning)
- **Financial Analysis**:
    - StockAnalysis (financials, overview, analyst ratings)
- **Economic Data**:
    - FRED (Federal Reserve Economic Data)
    - CNN (Fear & Greed Index)
- **Cryptocurrency Data**:
    - CoinGecko (spot, derivatives, social, categories, deep dives)
    - CoinMarketCap (trending, most-visited, new listings, metadata)
    - Hyperliquid (real-time perpetual futures data, funding rates, OI, volume)
    - altFINS (90+ pre-computed technical indicators, trend scores, chart patterns, screener signals, support/resistance — primary crypto TA source)