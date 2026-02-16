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
- **Scan Types**: Supports over 12 diverse scan types, including social momentum, sector rotation, squeeze plays, thematic investing, commodities, SQGLP, asymmetric trades, Weinstein analysis, portfolio review, morning briefings, cross-platform trending, and a dedicated crypto scanner.
- **Conversational AI**: The system is fully conversational, supporting multi-turn interactions where Claude receives conversation history and can answer follow-ups with prior context. It intelligently triggers fresh data gathering for new scans or ticker mentions.
- **Caching**: An in-memory TTL caching system (`cache.py`) is implemented across all data providers to optimize API calls and improve response times.
- **Error Handling & Reliability**: Includes comprehensive timeout mechanisms (global request, classifier, data gathering, Claude API) and keyword-based fallback classification for enhanced robustness.
- **UI/UX Considerations**: The API is designed to deliver concise, dense trading terminal-style JSON output. Analysis results consistently include TradingView chart links for easy visualization.
- **Portfolio Management**: Features include portfolio review capabilities for up to 25 tickers with dual scoring (trade + investment metrics) and endpoints for managing holdings and events.
- **Portfolio Quotes**: Dynamic multi-source pricing — FMP for stocks/ETFs, CoinGecko for any crypto (dynamic coin list cached 24h, ~14k coins), FMP commodity symbols for metals/energy. Priority overrides ensure major crypto (BTC, ETH, etc.) resolve correctly. Lookup order: FMP batch → commodities → CoinGecko dynamic.

## External Dependencies
The platform integrates with a wide array of third-party services and APIs to gather comprehensive market data and provide AI capabilities:
- **AI**: Anthropic (Claude AI)
- **Market Data & Screening**:
    - Finviz (screener)
    - Polygon.io (market data snapshots, technicals, news - used for single-ticker research)
    - Finnhub (insider trading, earnings, recommendations)
    - Financial Modeling Prep (FMP) (quotes, portfolio events, gainers/losers/actives)
    - Alpha Vantage (news sentiment)
    - Nasdaq (economic calendar)
- **Social Sentiment & Trending**:
    - Reddit/ApeWisdom (WSB, r/stocks, r/options, r/investing, r/daytrading sentiment)
    - StockTwits (social sentiment)
    - Yahoo Finance (trending)
- **Financial Analysis**:
    - StockAnalysis (financials, overview, analyst ratings)
- **Economic Data**:
    - FRED (Federal Reserve Economic Data)
    - CNN (Fear & Greed Index)
- **Cryptocurrency Data**:
    - CoinGecko (spot, derivatives, social, categories, deep dives)
    - CoinMarketCap (trending, most-visited, new listings, metadata)
    - Hyperliquid (real-time perpetual futures data, funding rates, OI, volume)