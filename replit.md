# Trading Analysis Platform - FastAPI Backend

## Overview
This project is a Python FastAPI backend for a trading analysis platform that integrates real-time market data from over 15 sources with Claude AI to generate actionable trading insights. It functions as an institutional cross-asset portfolio strategist, focusing on capital allocation, asymmetric risk/reward, probability-weighted repricing, and ruthless filtering. The platform supports both long-term investment strategies and short-term trading, aiming to provide users with hedge-fund-style intelligence through market scanning, sentiment analysis, and AI-driven recommendations with quantitative conviction scoring.

## User Preferences
- SQGLP framework for investments, Weinstein stage analysis for trades
- Only recommend Stage 2 breakouts
- Focus on small/mid-cap under $2B (power law returns)
- "Best Trades" = finding SETUPS (multiple indicators aligning), not chasing momentum
- Light enrichment batch size: 30/40 candidates (reduced for faster responses)

## System Architecture
The platform is built on FastAPI, offering a robust and scalable backend.
- **Core Functionality**: Orchestrates market scanning, data enrichment, quantitative pre-scoring, and integrates Claude AI for analysis.
- **Deterministic Scoring Pipeline**: Detects market regimes, computes structured catalyst scores, applies regime-aware cross-asset multipliers, and uses regime-specific weight matrices for scoring, passing structured scorecards to Claude for interpretation only.
- **Position Sizing Guidance**: Provides regime-driven position sizing brackets (e.g., risk_on 5-8%, risk_off 2-3%) as metadata for Claude.
- **Data Completeness Awareness**: Assigns neutral scores for missing data with deterministic penalties, transparently logging data gaps.
- **Confidence-Blended Weights**: Blends regime weights with base weights based on confidence levels to prevent misclassification swings.
- **Bounded Cross-Asset Multipliers**: Multipliers are clamped [0.75, 1.25] with liquidity-aware penalization for nano/micro caps.
- **Microcap Guardrails**: Implements position sizing caps by tier and requires 2-of-3 confirmation for buying (TA, catalyst, liquidity) to avoid "Speculative/Watch" labels.
- **Creative Discovery Exception**: Allows overrides for specific high-conviction setups with strong sentiment, volume expansion, real catalysts, and adequate liquidity, with capped sizing.
- **Data Pipeline**: Employs a "wide funnel" approach, screening numerous candidates, mathematically ranking them, and sending the top for deep AI analysis.
- **Best Trades Scanner**: A three-phase TA-first pipeline: (1) Finviz discovery screens, (2) OHLCV fetch via candle provider chain, (3) core/ta_signal_engine.py computes structured TA signals (name/direction/strength/evidence), ATR-based trade plans (entry/stop/targets/R:R), and ta_score ranking. Social is disabled by default; Claude polishes TA output but cannot alter trade plan numbers.
  - **Candle Budget**: CandleBudget(max_calls=8), shortlist=25, candle_targets=12, with Phase 2b broadening retry when <6 candles have data.
  - **Output Fields**: setup_type, indicator_signals (human-readable list), tradingview_url, action (Strong Buy/Buy/Hold/Sell), catalyst_check, risk, atr preserved. scan_stats includes candidates_total, candles_ok, candles_blocked, cache_hits.
  - **Hard Enforcement**: Backend enforces display_type=trades, non-empty risk field, and indicator_signals even when Claude returns wrong format.
- **Deterministic Screener Presets**: 6 preset screeners (oversold_growing, value_momentum, insider_breakout, high_growth_sc, dividend_value, short_squeeze) with 3-phase pipeline: Finviz discovery → enrichment (Finnhub quotes, StockAnalysis fundamentals, TA from candles with CandleBudget=8) → deterministic filter/rank. Output: display_type="screener" with rows table (ticker, company, price, chg_pct, mkt_cap, signals, rev_growth_yoy, pe, div_yield). Definitions in screener_definitions.py. Backend enforces no N/A strings, company validation (no single chars), and fallback to raw data if Claude misbehaves.
- **Scan Types**: Supports over 14 diverse scan types including best trades, social momentum, sector rotation, squeeze plays, thematic investing, commodities, and crypto scanning.
- **Hybrid Trending Architecture**: Combines Grok (xAI) and Claude for trending/social momentum analysis, utilizing two-tier conviction scoring, prioritizing small/micro-caps.
- **Cross-Market Scan**: Triggers parallel data pulling across all asset classes and uses a quantitative pre-ranker.
- **Resilient Cross-Asset Trending Pipeline**: Parallel execution of Grok and market scan with module-level status tracking, social-first fallback, and minimum output guarantees across asset classes. Commodity coverage uses a 23-entry COMMODITY_UNIVERSE mapping (ETF/equity proxies across energy, metals, agriculture, battery metals, carbon) with bounded quote sampling (MAX_COMMODITY_QUOTES=20), Grok theme force-inclusion, and 3-minute caching keyed by theme suffix.
- **Conversational AI**: Fully conversational with persistent, server-side stored conversation threads supporting multi-turn interactions and intelligent data gathering.
- **Candle Provider Chain**: cache → TwelveData (8/min, 15min circuit breaker on auth) → Finnhub (60min circuit breaker on 403) → Polygon (budget-tracked). CandleBudget tracks per-provider usage (twelvedata_used, polygon_used). Debug endpoint: GET /api/candle_stats.
- **Caching**: An in-memory TTL caching system optimizes API calls across all data providers.
- **Error Handling & Reliability**: Standardized JSON response envelope with error codes, never-empty guarantee, logging of raw Claude output and parse failures, and a wall-clock data-gathering deadline for cross-asset trending.
- **UI/UX Considerations**: Delivers concise, dense trading terminal-style JSON output, including TradingView chart links.
- **Portfolio Management**: Offers portfolio review for up to 25 tickers with dual scoring and endpoints for managing holdings and events.
- **Portfolio Quotes**: Features asset-type-aware routing for efficient data retrieval from primary and fallback sources.
- **Intent-Driven Orchestration**: Uses OpenAI for query classification and structured plan generation, with preset intent routing and heuristic fallbacks.
- **Data Architecture & Performance**: Utilizes local TA computation, tiered data sources with fallbacks, and scan budgeting with `BudgetTracker` for efficient resource management and adaptive per-preset budgets. Enforces "Social→FA Discipline" to prevent hype-only recommendations.

## External Dependencies
- **AI**: OpenAI (GPT-4o for orchestration/classification), Anthropic (Claude Sonnet for reasoning/analysis).
- **Market Data & Screening**: Finviz, TwelveData (primary candles, 8/min), Polygon.io (fallback candles, 4/min), Finnhub (circuit-broken), Financial Modeling Prep (FMP), Alpha Vantage, Nasdaq.
- **Social Sentiment & Trending**: Reddit/ApeWisdom, StockTwits, Yahoo Finance, xAI Grok.
- **Financial Analysis**: StockAnalysis.
- **Economic Data**: FRED (Federal Reserve Economic Data), CNN (Fear & Greed Index).
- **Cryptocurrency Data**: CoinGecko, CoinMarketCap, Hyperliquid, altFINS (crypto scanner only).