# Trading Analysis Platform - FastAPI Backend

## Overview
A comprehensive Python FastAPI backend for a trading analysis platform that combines real-time market data from 12+ sources with Claude AI to provide actionable trading insights. Supports both long-term investment analysis (SQGLP framework) and short-term trading strategies (Weinstein stage analysis, momentum/catalyst-driven).

## Project Architecture
```
main.py                      - FastAPI app entry point, routes, middleware
config.py                    - Configuration and API key management
agent/
  claude_agent.py            - Claude AI integration for analysis
  prompts.py                 - System prompts for different scan types
data/
  cache.py                   - In-memory TTL caching system
  market_data_service.py     - Orchestrator: wide scanning, enrichment, scoring
  scoring_engine.py          - Quantitative pre-scoring engine
  finviz_scraper.py          - Finviz screener integration
  polygon_provider.py        - Polygon.io market data (snapshots, technicals, news)
  stocktwits_provider.py     - StockTwits social sentiment
  stockanalysis_scraper.py   - StockAnalysis financials/overview
  finnhub_provider.py        - Finnhub insider/earnings/recommendations
  fmp_provider.py            - Financial Modeling Prep (optional, 250/day limit)
  fred_provider.py           - FRED economic data
  fear_greed_provider.py     - CNN Fear & Greed Index
```

## Running
The server runs on port 5000 using Uvicorn with hot reload enabled.

## Key Features
- **Wide Funnel Approach**: Screens 50-100+ candidates, ranks mathematically, sends top 12 to Claude
- **10 Scan Types**: Social momentum, sector rotation, squeeze plays, thematic investing, commodities, SQGLP, asymmetric, Weinstein, portfolio review, morning briefing
- **Portfolio Review**: Analyze up to 25 tickers with dual scoring (trade + investment metrics)
- **Morning Briefing**: Hedge-fund-style intelligence report with market pulse, key numbers, top moves
- **In-Memory TTL Caching**: All 8 providers cached with appropriate TTLs to reduce API calls

## Cache TTLs
- Finviz screener: 5 min
- Polygon snapshot: 1 min, technicals: 5 min, details: 1 hr
- StockTwits: 2 min
- StockAnalysis: 15 min
- Finnhub: 10 min
- FMP: 5 min
- FRED: 1 hr
- Fear & Greed: 10 min

## API Keys Required
POLYGON_API_KEY, ANTHROPIC_API_KEY, FINNHUB_API_KEY, ALPHA_VANTAGE_API_KEY, FRED_API_KEY, FMP_API_KEY (optional), AGENT_API_KEY (for cache clear auth)

## Endpoints
- `GET /` - Welcome message
- `GET /health` - Health check
- `POST /api/scan` - Run trading scans
- `POST /api/portfolio` - Portfolio review
- `POST /api/briefing` - Morning briefing
- `POST /api/cache/clear` - Clear cache (requires X-API-Key header)

## User Preferences
- SQGLP framework for investments, Weinstein stage analysis for trades
- Only recommend Stage 2 breakouts
- Focus on small/mid-cap under $2B (power law returns)
- "Best Trades" = finding SETUPS (multiple indicators aligning), not chasing momentum
- Light enrichment batch size: 30/40 candidates (reduced for faster responses)

## Recent Changes
- 2026-02-15: Fixed Finnhub insider_sentiment caching bug (early return before cache.set)
- 2026-02-15: Added caching to StockAnalysis get_financials method
- 2026-02-15: Implemented TTL caching across all 8 data providers
- 2026-02-15: Added cache clear endpoint with API key auth and rate limiting
- 2026-02-15: Reduced light enrichment batch size from 40/60 to 30/40
- 2026-02-15: Added portfolio review and morning briefing features
- 2026-02-14: Initial project setup with FastAPI, CORS middleware, and basic routes
