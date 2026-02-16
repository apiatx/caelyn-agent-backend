# Trading Analysis Platform - FastAPI Backend

## Overview
A comprehensive Python FastAPI backend for a trading analysis platform that combines real-time market data from 13+ sources with Claude AI to provide actionable trading insights. Claude operates as a "master trader" persona — filtering ruthlessly, leading with conviction picks, and weaving macro context into every analysis. Supports both long-term investment analysis (SQGLP framework) and short-term trading strategies (Weinstein stage analysis, momentum/catalyst-driven).

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
  coingecko_provider.py      - CoinGecko crypto market data (spot, derivatives, social)
  cmc_provider.py            - CoinMarketCap trending, most-visited, new listings, metadata
```

## Running
The server runs on port 5000 using Uvicorn with hot reload enabled.

## Key Features
- **Wide Funnel Approach**: Screens 50-100+ candidates, ranks mathematically, sends top 12 to Claude
- **12+ Scan Types**: Social momentum, sector rotation (Weinstein stage-based), squeeze plays, thematic investing, commodities, SQGLP, asymmetric, Weinstein, portfolio review, morning briefing, cross-platform trending, crypto
- **Portfolio Review**: Analyze up to 25 tickers with dual scoring (trade + investment metrics)
- **Morning Briefing**: Hedge-fund-style intelligence report with market pulse, key numbers, top moves
- **Crypto Scanner**: Dual-source crypto dashboard (CoinGecko + CoinMarketCap) — funding rates, dual-trending, most-visited FOMO signals, volume acceleration, new listings, deep dives with social/dev metrics
- **In-Memory TTL Caching**: All 10 providers cached with appropriate TTLs to reduce API calls

## Cache TTLs
- Finviz screener: 5 min
- Polygon snapshot: 1 min, technicals: 5 min, details: 1 hr
- StockTwits: 2 min
- StockAnalysis: 15 min
- Finnhub: 10 min
- FMP: 5 min
- FRED: 1 hr
- Fear & Greed: 10 min
- CoinGecko: 2 min
- CoinMarketCap: 2 min

## API Keys Required
POLYGON_API_KEY, ANTHROPIC_API_KEY, FINNHUB_API_KEY, ALPHA_VANTAGE_API_KEY, FRED_API_KEY, FMP_API_KEY (optional), COINGECKO_API_KEY, CMC_API_KEY, AGENT_API_KEY (for cache clear auth)

## Endpoints
- `GET /` - Welcome message
- `GET /api/health` - Health check (verifies Claude API)
- `POST /api/query` - Main agent query (supports conversation_id for auto-save)
- `POST /api/cache/clear` - Clear cache (requires X-API-Key header)
- `GET /api/conversations` - List all recent conversations (metadata, sorted by most recent)
- `GET /api/conversations/{id}` - Get full conversation with messages
- `POST /api/conversations` - Create new conversation (body: {first_query: string})
- `PUT /api/conversations/{id}` - Update conversation messages
- `DELETE /api/conversations/{id}` - Delete a conversation

## User Preferences
- SQGLP framework for investments, Weinstein stage analysis for trades
- Only recommend Stage 2 breakouts
- Focus on small/mid-cap under $2B (power law returns)
- "Best Trades" = finding SETUPS (multiple indicators aligning), not chasing momentum
- Light enrichment batch size: 30/40 candidates (reduced for faster responses)

## Recent Changes
- 2026-02-16: Tightened Finviz screener filters — CATEGORY_FILTERS dict with per-category filter strings, limits, and enrich_top caps. Single Finviz screen per category instead of 4-11 parallel screens. Per-ticker enrichment timeouts (6s light, 8s deep). Categories: trades, investments, fundamentals, squeeze, asymmetric, social_momentum, volume_spikes, bearish, small_cap_spec, market_scan.
- 2026-02-16: Rewrote sector rotation scan — 3 broad Finviz screens (stage2/stage4/total) instead of 33 per-sector calls. Counts by sector from results. Breakout candidates pulled from stage2 stocks in top sectors. Much faster and avoids Finviz rate limiting.
- 2026-02-16: Added "breakout" keyword to sector_rotation classifier.
- 2026-02-16: Added chat history persistence with file-based storage (data/chat_history.py). Conversations auto-delete after 3 days. CRUD endpoints: list, get, create, update, delete. /api/query auto-saves when conversation_id is provided.
- 2026-02-16: Made agent fully conversational with multi-turn support. Claude now receives conversation history and can answer follow-ups using prior context without re-fetching data. New scans mid-conversation (including ticker mentions like "analyze NVDA") correctly trigger fresh data gathering. API accepts both old (prompt/history) and new (query/conversation_history) field names for backward compatibility. History trimmed to 100K chars with smart truncation (truncates large messages before dropping).
- 2026-02-15: Added comprehensive timeout/reliability layer: 90s global request timeout, 10s classifier timeout with keyword fallback, 45s data gathering timeout, 60s Claude API timeout. All failures return valid JSON chat responses.
- 2026-02-15: Removed Polygon retry logic on 429 (was causing 90s+ delays). Now returns empty immediately.
- 2026-02-15: Reduced all provider HTTP timeouts (15s→10s, 20s→12s, Polygon 10s→8s) to prevent cascading delays.
- 2026-02-15: Added /api/health smoke test endpoint that verifies Claude API connectivity.
- 2026-02-15: Added keyword-based fallback classifier covering all 20+ query categories (no API call needed).
- 2026-02-15: Added detailed timing logs at every pipeline step (classification, data gathering, Claude response, parsing).
- 2026-02-15: Rewrote system prompt identity — "master trader" persona with 10 core principles (signal over noise, conviction-based filtering, macro-driven, opinionated, quality over quantity). Kept all 15 display_type format schemas intact.
- 2026-02-15: Fixed AI Screener — root cause was max_tokens=4096 truncating screener JSON responses (~17K chars). Increased to 16384.
- 2026-02-15: Rewrote _parse_response JSON parser — fixed nested JSON regex bug, added brace-depth counting, truncated JSON repair
- 2026-02-15: Added comprehensive debug logging throughout screener flow (API → classifier → filter extraction → Finviz → parser)
- 2026-02-15: Added dividend_yield_min filter support to run_ai_screener
- 2026-02-15: Improved Finviz _custom_screen with additional table detection fallbacks and diagnostic logging
- 2026-02-15: Fixed StockAnalysis trending scraper — replaced JS-rendered HTML scraping with FMP stock_market/gainers + actives API
- 2026-02-15: Fixed Polygon market movers — added FMP fallback when Polygon snapshot endpoint unavailable (paid tier)
- 2026-02-15: Added FMP gainers/losers/actives methods to FMP provider
- 2026-02-15: Fixed Claude response formatting — strict JSON-only output rules in system prompt, no markdown headers/bullets
- 2026-02-15: Improved _parse_response with 4-tier fallback: raw JSON → code block → embedded JSON → chat fallback
- 2026-02-15: Added concise formatting rules for all JSON string values (dense trading terminal style)
- 2026-02-15: Added AI custom stock screener (natural language → Finviz filters → StockAnalysis enrichment)
- 2026-02-15: Added _extract_screener_filters NLP parser for market cap, revenue growth, P/E, RSI, SMA, insider buying, sectors, etc.
- 2026-02-15: Added screener display format and ai_screener classifier category
- 2026-02-15: Added CoinMarketCap provider (trending, most-visited, new listings, gainers/losers, categories, metadata, global metrics)
- 2026-02-15: Combined crypto scanner now cross-references CoinGecko + CMC trending (dual_trending = strongest signal)
- 2026-02-15: Added volume acceleration, most-visited divergence, new listings watch, and attention signals to crypto format
- 2026-02-15: Updated crypto interpretation guide with signal hierarchy, volume change interpretation, most-visited interpretation
- 2026-02-15: Added Weinstein Stage-based sector rotation analysis (stage2_pct/stage4_pct per GICS sector, breakout candidates)
- 2026-02-15: Added cross-platform trending aggregation (StockTwits + Yahoo + StockAnalysis + Finviz + Polygon, multi-source scoring)
- 2026-02-15: Added Yahoo Finance trending scraper and StockAnalysis trending scraper
- 2026-02-15: Fixed StockAnalysis nested dict bug (get_overview/get_analyst_ratings/get_financials now return flat dicts)
- 2026-02-15: Fixed scoring engine percentage parsing (parse_pct handles "18.20%" strings)
- 2026-02-15: Fixed market cap parsing (parse_market_cap_string handles "$3.45B" format)
- 2026-02-15: Added CoinGecko crypto provider with full dashboard (spot, derivatives, funding rates, categories, deep dives)
- 2026-02-15: Added crypto scanner to market_data_service with funding rate analysis
- 2026-02-15: Added crypto category to classifier and crypto display format to prompts
- 2026-02-15: Added crypto market interpretation guide (funding rates, OI analysis, narrative rotation)
- 2026-02-15: Fixed Finnhub insider_sentiment caching bug (early return before cache.set)
- 2026-02-15: Added caching to StockAnalysis get_financials method
- 2026-02-15: Implemented TTL caching across all 8 data providers
- 2026-02-15: Added cache clear endpoint with API key auth and rate limiting
- 2026-02-15: Reduced light enrichment batch size from 40/60 to 30/40
- 2026-02-15: Added portfolio review and morning briefing features
- 2026-02-14: Initial project setup with FastAPI, CORS middleware, and basic routes
