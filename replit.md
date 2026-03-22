# Trading Analysis Platform - FastAPI Backend

## Overview
This project is a Python FastAPI backend for a trading analysis platform designed to integrate real-time market data from over 15 sources with Claude AI. Its primary purpose is to generate actionable trading insights, acting as an institutional cross-asset portfolio strategist. The platform focuses on capital allocation, asymmetric risk/reward, probability-weighted repricing, and ruthless filtering to support both long-term investment strategies and short-term trading. It aims to provide users with hedge-fund-style intelligence through market scanning, sentiment analysis, and AI-driven recommendations with quantitative conviction scoring, ultimately delivering alpha-generating opportunities.

## User Preferences
- SQGLP framework for investments, Weinstein stage analysis for trades
- Only recommend Stage 2 breakouts
- "Best Trades" = finding SETUPS (multiple indicators aligning), not chasing momentum
- Light enrichment batch size: 30/40 candidates (reduced for faster responses)
- INVESTMENTS market cap: $300M–$70B range. Soft preference for <$2B (power law returns) but $2B–$70B compounders with accelerating fundamentals are valid. Never recommend turnarounds, regulatory-dependent revenue, or negative operating margin companies as investments.
- INVESTMENTS quality gates (ALL must pass before recommending): (1) Revenue growth sustainable — not one-time events, regulatory windfalls, or accounting changes. (2) Operating margin positive or clearly turning positive. (3) Business model has durable advantage — network effects, monopoly position, switching costs, bottleneck asset, or brand moat. (4) Price action healthy — above SMA50 or SMA200 (not in technical breakdown). (5) Sector must have multi-year tailwind.
- INVESTMENTS output: ALWAYS return 3-5 picks minimum. Never return 1. If fewer than 3 pass all quality gates from Finviz data, use the grok_thematic leaders to fill remaining slots — they exist precisely for this.
- INVESTMENTS thematic priority: When grok_thematic data is present, PRIORITIZE tickers from grok_thematic.thematic_leaders with conviction_tier=1 over random Finviz growers. A Tier 1 Grok thematic leader with reasonable fundamentals beats a Finviz screener result with great numbers but no strategic importance. The question is always: "Is this company part of a decade-defining trend, or just doing well right now?" Debt buyers, specialty finance, commodity processors, and non-strategic businesses should NEVER appear in Best Investments unless they have a unique structural moat in a critical bottleneck.
- INVESTMENTS forbidden sectors (never recommend for Best Investments): consumer debt collection, payday lending, commodity retail, generic healthcare administration, non-critical specialty finance. These pass Finviz filters but have no place in a 6-20 year hold portfolio.
- INVESTMENTS ideal candidates: AI infrastructure bottlenecks, defense/aerospace primes and disruptors, energy grid buildout, critical materials monopolies, cybersecurity platform leaders, late-stage biotech with breakthrough potential, quantum computing leaders, companies with visionary respected leadership building category-defining businesses.

## System Architecture
The platform's backend is built on FastAPI, designed for robustness and scalability.
- **Core Functionality**: Orchestrates market scanning, data enrichment, quantitative pre-scoring, and integrates Claude AI for analysis.
- **Deterministic Scoring Pipeline**: Detects market regimes, computes structured catalyst scores, applies regime-aware cross-asset multipliers, and uses regime-specific weight matrices for scoring, passing structured scorecards to Claude for interpretation.
- **Data Completeness & Confidence**: Assigns neutral scores for missing data with deterministic penalties and blends regime weights with base weights based on confidence levels.
- **Microcap Guardrails**: Implements position sizing caps and requires multi-factor confirmation for buying to mitigate risks.
- **Data Pipeline**: Employs a "wide funnel" approach for candidate screening, mathematical ranking, and deep AI analysis of top candidates.
- **Best Trades Scanner**: A three-phase technical analysis-first pipeline for discovery, shortlisting, and detailed signal computation with ATR-based trade plans.
- **Deterministic Screener Presets**: Provides 6 preset screeners with a three-phase pipeline including Finviz discovery, enrichment, and deterministic filtering/ranking.
- **Scan Types**: Supports over 14 diverse scan types including best trades, social momentum, sector rotation, squeeze plays, thematic investing, commodities, and crypto scanning.
- **Hybrid Trending Architecture**: Combines Grok (xAI) and Claude for trending/social momentum analysis with two-tier conviction scoring, prioritizing small/micro-caps.
- **Cross-Market Scan**: Triggers parallel data pulling across all asset classes with quantitative pre-ranking.
- **Resilient Cross-Asset Trending Pipeline**: Parallel execution with module-level status tracking, social-first fallback, and minimum output guarantees across asset classes, including commodity coverage via ETF/equity proxies.
- **Conversational AI**: Fully conversational with persistent, server-side stored conversation threads supporting multi-turn interactions.
- **Candle Provider Chain**: Utilizes a tiered fallback system for candle data (cache → TwelveData → Finnhub → Polygon) with budget tracking.
- **Global Daily Budget**: Tracks per-provider daily API calls with configurable limits, warnings, and hard-stops.
- **Finviz-First Price Extraction**: Prioritizes Finviz for price/change data in screener enrichment to reduce API calls.
- **Crypto Scanner Routing**: Centralized classifier routes crypto queries to a dedicated pipeline with specific optimizations for speed and crypto-specific analysis.
- **HL Additional Coins**: Scans HyperLiquid funding analysis for additional coins not covered by CoinGecko, integrating funding/OI/volume data for Claude's analysis.
- **X/Twitter Crypto Sentiment**: Dedicated Grok prompt for crypto X scanning to provide social velocity, BTC sentiment, narrative heat, and contrarian signals.
- **Data Compression Layer**: Pre-digests raw market data into structured, category-specific summaries (5-15KB) before sending to Claude, with aggressive compression for cross-asset trending.
- **Enhanced Model Autonomy**: Three-model collaboration where OpenAI (gpt-4o-mini) provides a reasoning brief, Grok offers a market mood snapshot, and Claude retains analytical autonomy with these as advisory inputs.
- **Caching**: An in-memory TTL caching system optimizes API calls.
- **Error Handling & Reliability**: Standardized JSON response envelope, never-empty guarantee, and robust logging.
- **UI/UX Considerations**: Delivers concise, dense trading terminal-style JSON output, including TradingView chart links.
- **Portfolio Management**: Offers portfolio review with dual scoring and endpoints for managing holdings and events.
- **Intent-Driven Orchestration**: Uses OpenAI for query classification and structured plan generation.
- **Data Architecture & Performance**: Utilizes local TA computation, tiered data sources with fallbacks, and scan budgeting. Enforces "Social→FA Discipline."
- **Options Flow Screener**: Background precompute loop (`_options_precompute_loop`) fires every 90 seconds, scanning 17 tickers (7 ETFs: SPY/QQQ/IWM/GLD/TLT/XLF/XLK + 10 Stocks: AAPL/NVDA/TSLA/AMZN/META/MSFT/AMD/GOOGL/NFLX/COIN) via Public.com using `scan_full_screener()`. No Claude in the screener — pure data pipeline. Cache key `options_screener_v2` (TTL 120s). `POST /api/options/dashboard` returns in <100ms. Response: `{ tickers:[...], all_contracts:[...500], market_summary:{...} }`. Per-ticker: call_volume, put_volume, pc_ratio, call_oi, put_oi, avg_call_iv (volume-weighted), avg_put_iv, iv_skew, max_pain, top_calls[:10], top_puts[:10]. Flat all_contracts: every active contract with underlying, category (stock/etf), side (call/put), strike, bid/ask/last, volume, openInterest, vol_oi_ratio, delta, gamma, theta, vega, iv — frontend can sort by any field. Claude is only for the conversational chat bar below the screener. Cold-start fallback for first request before loop runs.

## External Dependencies
- **AI**: OpenAI (GPT-4o for orchestration/classification), Anthropic (Claude Sonnet for reasoning/analysis), xAI Grok.
- **Market Data & Screening**: Finviz, TwelveData, Polygon.io, Finnhub, Financial Modeling Prep (FMP), Alpha Vantage, Nasdaq.
- **Social Sentiment & Trending**: Reddit/ApeWisdom, StockTwits, Yahoo Finance.
- **Financial Analysis**: StockAnalysis.
- **SEC Filings**: SEC EDGAR (data.sec.gov).
- **Economic Data**: FRED (Federal Reserve Economic Data), CNN (Fear & Greed Index).
- **Cryptocurrency Data**: CoinGecko, CoinMarketCap, Hyperliquid, altFINS.
- **Options Data**: Public.com.