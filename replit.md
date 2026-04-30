# Caelyn AI — FastAPI Trading Analysis Platform

## Overview

Caelyn AI is a sophisticated FastAPI-based platform designed for real-time trading analysis and intelligence. It provides a comprehensive suite of tools for institutional and retail investors, covering diverse asset classes from traditional equities and commodities to cryptocurrencies and prediction markets. The platform aims to deliver actionable insights through advanced screening, AI-driven analysis, and proprietary scoring engines, helping users identify opportunities and manage risk effectively.

Key capabilities include:
- Real-time Hyperliquid perpetuals and spot market screener with multi-factor scoring.
- Comprehensive sector rotation analysis and AI-generated market commentary.
- Deep dive into SEC Form 4 insider activity with conviction scoring and cluster detection.
- Institutional 13F whale tracking, portfolio return analysis, and AI-summarized investment themes.
- Polymarket prediction market intelligence with edge detection, mispricing signals, and a multi-agent AI trading pipeline.
- Deterministic supply chain bottleneck discovery and regime detection engine for strategic investment playbooks.
- Fundamental stock comparison tool with a wide range of financial metrics.
- Catalyst calendar aggregating key market events like earnings, dividends, IPOs, and economic releases.
- Real portfolio analytics for tracking and analyzing specific holdings.

## User Preferences

No explicit user preferences were provided in the original `replit.md` file.

## System Architecture

The Caelyn AI platform is built on a FastAPI Python backend, running on port 5000. It features a modular architecture with distinct services for different analytical functions.

**Core Architectural Patterns:**
- **Microservices-like Structure:** Each major analytical feature (Hyperliquid Screener, Sector Rotation, Insider Activity, Whale Watch, Prediction Markets, Playbook Engine, Stock Compare, Catalyst Calendar, Real Portfolio) is implemented as a self-contained service with its own router, service logic, and data models.
- **In-Memory State Management:** Utilized for high-performance data access, particularly for Hyperliquid market data (assets, candles, trades, books, OI history).
- **Disk-Persistent Caching:** Critical for fast server restarts and reducing initial data load times, especially for frequently accessed or static datasets like HIP-3 DEX assets and AI analysis results.
- **Hierarchical Scoring Engines:** Employed in the Hyperliquid Screener to process candle features, short-term signals, structural quality, regime classification, and hero selection with guardrails.
- **Multi-Agent AI Pipeline:** The Prediction Markets feature leverages a 6-agent Gemini pipeline (Fundamentals, Sentiment, Technical, Bull, Bear, RiskManager) for comprehensive market analysis and trading recommendations.
- **Deterministic Regime Detection:** The Playbook Engine's `serenity-regime` is designed to produce consistent results based on predefined taxonomies and graphs, without external API calls for core detection.
- **Metric-Aware Data Fetching:** For stock comparison, the system intelligently fetches only the necessary data points from external APIs based on the requested metrics.
- **Importance Scoring:** Catalysts are automatically categorized by importance (high, medium, low) using deterministic rules based on event type, market cap, and associated keywords.

**UI/UX Decisions (Implied):**
- Features like "Market Brief," "signal sections," "summary cards," and "guidance buckets" in the Hyperliquid Screener suggest a dashboard-oriented interface presenting categorized and summarized insights.
- The "Sector Rotation Dashboard" and "Insider Activity Dashboard" imply visually rich interfaces for displaying trends, scores, and aggregated statistics.
- The "Playbook" concept indicates a structured approach to investment strategies, potentially guiding users through discovery and analysis workflows.

**Technical Implementations:**
- **FastAPI:** Used for building robust and high-performance APIs.
- **Pydantic:** Extensively used for data validation and serialization, defining models for various market assets, API requests, and responses.
- **WebSockets:** Utilized for real-time data streaming from sources like Hyperliquid.
- **Asynchronous Programming:** `asyncio` is used for managing concurrent operations, particularly for external API calls and background tasks.
- **Database:** Neon PostgreSQL is used for persisting data such as insider transactions, whale holdings, and portfolio returns.
- **Caching:** Redis or an in-memory dictionary-based cache is used for transient data (e.g., Finnhub quotes, FMP API responses) with defined TTLs.
- **Universe Filtering:** Implemented with strict rules for Hyperliquid assets to ensure data quality, including volume gates for spot markets and allowlists.

## Thematic Context Adapter (Apr 2025)

A shared read-only thematic/regime/sector context layer was added to make the same intelligence used by the main agent available to non-chat endpoints — without creating a competing engine and without adding LLM calls to screeners.

**New Files:**
- `backend/services/thematic_context_provider.py` — Normalized snapshot aggregating regime, sector rotation, theme ETF RS scores, and X consensus. Cached as `thematic_context:snapshot:v1` (10 min TTL). Never raises.
- `backend/services/theme_ticker_mapper.py` — Ticker→theme index built from `home_service.THEME_MAP` + `THEME_ETF_UNIVERSE`. O(1) lookups. Provides `get_ticker_theme_alignment()` for per-row annotation.

**Modified Files:**
- `backend/core/regime_engine.py` — Added write-through: `detect_market_regime()` now also writes result to `cache.set("regime:current_v1", ...)` so any endpoint can read it without `data_service`.
- `backend/agent/context_broker.py` — `read_shared_context()` now also reads `regime:current_v1` and `thematic_context:snapshot:v1` (if pre-warmed). `build_context_overlay()` injects `shared_macro_regime`, `shared_active_themes`, `shared_emerging_themes` into LLM context.
- `backend/main.py` — Added `GET /api/thematic-context/snapshot` and `POST /api/thematic-context/refresh`. Options screener adds 9 additive `theme_*` fields per ticker row.
- `backend/services/playbook/strategy_screener/screener_router.py` — Populates `regime_context` (was always `null`); adds `theme_name`, `theme_state`, `regime_alignment_score`, `regime_alignment_label`, `thematic_badges`, `dead_zone_warning`, `base_score`, `final_score` per result row.

**Cache sources reused (no new engines):**
- `regime:current_v1` (new write-through from regime_engine)
- `sr:dashboard:v1` (sector rotation background loop)
- `sr:theme_data:v2` (sector rotation theme service)
- `thematic_context:snapshot:v1` (self-populated, 10 min)
- `data/x_consensus_weekly.json` (X consensus daily snapshot)
- `data/sector_rotation_analysis.json` (Gemini disk fallback)
- `notifai_weekly_summary_v2`, `fred:quick_macro` (existing)

**Earnings: untouched.**

## Dynamic Thematic Universe Builder (Apr 2026)

Replaces hardcoded ticker lists in TA Screener and Options Flow with a dynamically built, multi-source ticker universe driven by ThematicContextProvider active/emerging themes.

**New File:**
- `backend/services/dynamic_thematic_universe.py` — Core builder.
  - `get_dynamic_thematic_universe(active_only, include_emerging, max_tickers, force_refresh)` — async, builds or returns from 15-min in-memory cache.
  - `get_cached_thematic_universe()` — sync, non-blocking read from cache. Used in hot paths.
  - Discovery sources per theme: (1) ETF holdings via `etf_holdings_service` using keyword-augmented proxy ETFs, (2) FMP company peers via `stable/stock-peers` from anchor tickers, (3) X/Grok consensus from `x_consensus_weekly.json`, (4) static `related_tickers` fallback.
  - `_KEYWORD_ETF_MAP` — maps granular sub-theme names (e.g. "AI Networking", "Datacenter / Compute") to ETF proxies so ETF holdings can be fetched even when theme data has empty `related_etfs`.
  - Returns: `{tickers, sources_by_ticker, theme_map, snapshot_status, source_health, built_at, ticker_count}`.

**TA Screener Integration** (`backend/data/market_data_service.py`):
- After Phase A Finviz discovery: injects up to 15 thematic tickers (`_THEMATIC_ENRICH_CAP`) not already in the Finviz pool. These go through Phase B (enrichment) and Phase C (filter/rank) identically to Finviz candidates. Non-blocking (sync cache read only).
- After Phase C scoring: annotates all `final_rows` with `theme_name`, `theme_state`, `regime_alignment_score`, `discovery_sources`. Uses dynamic universe `theme_map` first; falls back to `get_ticker_theme_alignment()` for Finviz-only tickers.

**Options Flow Integration** (`backend/main.py`):
- Cold prefilter path: replaces old `get_thematic_prefilter_universe()` call with `get_cached_thematic_universe()`. Dynamic universe tickers (up to 80) are prepended to `_master_seeds` before `engine.build_prefilter_snapshot()`.
- Static `_master_seeds` remain as liquid options fallback after dynamic tickers.

**Background Loop** (`backend/main.py`):
- `_dynamic_thematic_universe_loop()` — refreshes every 15 minutes. 30-second initial delay for sector-rotation and X-consensus loops to warm first.
- Started via `asyncio.create_task()` in the lifespan startup.

**Guardrails:**
- No LLM calls. No Tradier calls. No Earnings data touched.
- All vendor calls time-bounded with asyncio.wait_for / httpx timeouts.
- Never raises. Degrades gracefully if individual sources (ETF holdings, FMP peers) are unavailable.
- Finviz broad discovery unchanged — thematic candidates are additive, not replacement.
- Theme annotation is additive, not a filter.

## External Dependencies

The Caelyn AI platform integrates with several external services and APIs to gather and process financial data:

-   **Hyperliquid:**
    *   REST API (for market snapshots, meta-data)
    *   WebSocket API (for real-time market data)
-   **Tradier:** Primary live quote source (price, volume, bid/ask, 1D%) for Portfolio, Social Screener, Watchlist, and Calendar pages. Resilient fallback chain: Tradier → LKG Tradier cache (72 h) → FMP cached quote → Finnhub → null. Per-quote metadata fields: quote_source, quote_cached_at, quote_is_stale, quote_fallback_reason.
-   **Yahoo Finance (`yfinance`):** For historical price data (e.g., GOLD, sector rotation ETFs) and fallback for stock price enrichment.
-   **CoinGecko:** For cryptocurrency data (e.g., BTC).
-   **Finnhub:**
    *   Real-time stock quotes.
    *   Company profile and metrics (fallback for insider activity price enrichment).
-   **FRED (Federal Reserve Economic Data):** For macro-economic indicators (e.g., Fed Funds Rate, CPI, 10Y/2Y Treasury yields).
-   **SEC EDGAR (edgartools):**
    *   For fetching SEC Form 4 filings (insider activity).
    *   For fetching SEC Form 13F-HR filings (institutional holdings for Whale Watch).
    *   For company ticker indices and full-text search (CUSIP to Ticker resolution).
-   **Financial Modeling Prep (FMP) Stable API (Starter Plan):**
    *   Primary source for stock comparison metrics.
    *   Primary source for Catalyst Calendar data (earnings, dividends, IPOs, splits, economic releases, treasury rates, SEC filings, analyst ratings, insider transactions).
-   **Polymarket:**
    *   For prediction market data (prices, volume, order book).
    *   Gamma events API (for market tags).
-   **Google Gemini (gemini-3-flash-preview):** Used for AI analysis in Sector Rotation and as the engine for all agents in the Prediction Market TradingAgents pipeline.
-   **Google Search:** Provides real-time information grounding for Gemini AI agents.
-   **Anthropic Claude (claude-haiku-4-5):** Used for generating AI theme summaries in the Whale Watch feature.
-   **Perplexity AI:** Used to discover new top whales for the Whale Watch feature.