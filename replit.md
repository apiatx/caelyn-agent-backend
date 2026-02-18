# Trading Analysis Platform - FastAPI Backend

## Overview
This project is a Python FastAPI backend for a trading analysis platform that integrates real-time market data from over 15 sources with Claude AI to generate actionable trading insights. It functions as an institutional cross-asset portfolio strategist, focusing on capital allocation, asymmetric risk/reward, probability-weighted repricing, and ruthless filtering. The platform supports both long-term investment strategies (using the SQGLP framework) and short-term trading (Weinstein stage analysis, momentum, and catalyst-driven approaches), aiming to provide users with hedge-fund-style intelligence through market scanning, sentiment analysis, and AI-driven recommendations with quantitative conviction scoring.

## User Preferences
- SQGLP framework for investments, Weinstein stage analysis for trades
- Only recommend Stage 2 breakouts
- Focus on small/mid-cap under $2B (power law returns)
- "Best Trades" = finding SETUPS (multiple indicators aligning), not chasing momentum
- Light enrichment batch size: 30/40 candidates (reduced for faster responses)

## System Architecture
The platform is built on FastAPI, offering a robust and scalable backend.
- **Core Functionality**: The `market_data_service.py` orchestrates market scanning, data enrichment, and quantitative pre-scoring. The `claude_agent.py` integrates Claude AI, using prompts from `prompts.py` for various analysis types.
- **Deterministic Scoring Pipeline**: `core/regime_engine.py` detects market regime (risk_on/risk_off/inflationary/neutral) from SPY/VIX/10Y/DXY/BTC signals. `core/catalyst_engine.py` computes structured catalyst scores (0-100) with component breakdown. `core/asset_weight_engine.py` applies regime-aware cross-asset multipliers. `agent/institutional_scorer.py` uses regime-specific weight matrices (e.g., risk_off: 40% fundamentals, 25% catalyst, 20% technical, 15% sentiment) and passes structured scorecards to Claude. Claude interprets — does NOT rescore.
- **Position Sizing Guidance**: Regime-driven bracket: risk_on 5-8%, risk_off 2-3%, inflationary/neutral 3-5%. Passed as metadata for Claude to reference.
- **Data Completeness Awareness**: Missing data gets neutral scores (50) not 0, plus deterministic penalties (up to 25%): missing fundamentals +10%, missing OHLC +8%, missing volume +7%, missing news +5%, missing social +5%. Transparent `data_flags.missing` array per candidate.
- **Confidence-Blended Weights**: Regime weights blend with base weights by confidence level (confidence 0 → base, confidence 1 → full regime weights) preventing misclassification swings.
- **Bounded Cross-Asset Multipliers**: All multipliers clamped [0.75, 1.25]. Liquidity-aware: nano/micro + low liquidity get hardest penalization. `avg_dollar_volume` computed per candidate.
- **Microcap Guardrails**: Position sizing caps by tier (nano_low ≤0.5%, micro_low ≤1%, micro_med ≤2%, etc.). Buy gating requires 2-of-3 confirmation (TA≥65, catalyst present, liquidity OK). Failing = "Speculative/Watch" label.
- **Creative Discovery Exception**: Override requires ALL: sentiment≥85, volume_expansion present with score≥10, at least one real catalyst component (news/earnings/fundamental), AND liquidity tier not "low". Override candidates capped at ≤2% sizing unless fundamentals≥70 and high liquidity.
- **Data Pipeline**: Employs a "wide funnel" approach, screening 50-100+ candidates, mathematically ranking them, and sending the top 12 to Claude for deep analysis.
- **Scan Types**: Supports over 13 diverse scan types including social momentum, sector rotation, squeeze plays, thematic investing, commodities, SQGLP, asymmetric trades, Weinstein analysis, portfolio review, morning briefings, cross-platform trending, cross-market (multi-asset), and crypto scanning.
- **Hybrid Trending Architecture**: Combines Grok (xAI) and Claude for trending/social momentum analysis, utilizing two-tier conviction scoring, and prioritizes asymmetric small/micro-caps.
- **Cross-Market Scan**: Triggers parallel data pulling from all asset classes (stocks, crypto, commodities) when multiple markets are queried, using `data/cross_asset_ranker.py` for quantitative pre-ranking.
- **Resilient Cross-Asset Trending Pipeline**: Grok (25s) and market scan (25s) run in parallel via asyncio.gather. Module-level status tracking (ok/timeout/error/skipped/ok_cached). Social-first fallback: if market scan fails, `_light_enrich_grok_shortlist()` fetches StockAnalysis overviews (12s budget). Minimum output guarantees: equities>=3, crypto>=1, commodities>=1. Broadening fills gaps from cached market data. Logging: `[SOCIAL_REQUIRED]`, `[MODULE_STATUS]`, `[TRENDING_OUTPUT]`.
- **Conversational AI**: Fully conversational with persistent, server-side stored conversation threads (UUID-based JSON files). Supports multi-turn interactions and intelligently triggers fresh data gathering.
- **Caching**: An in-memory TTL caching system (`cache.py`) optimizes API calls across all data providers.
- **Error Handling & Reliability**: Standardized JSON envelope on ALL /api/query responses: `{type:"ok"|"error", analysis, structured, meta:{request_id, preset_intent, conversation_id, routing, timing_ms}, error:{code, message, details}|null}`. Never-empty guarantee — no blank bodies or silent failures. Claude raw output logged (`[CLAUDE_RAW]`) with parse failure detection (`[PARSE_FAIL]`). Response size logged (`[RESP]`). Cross_asset_trending has 45s wall-clock data-gathering deadline with graceful broadening skip. All error codes: AUTH_FAILED, SERVER_STARTING, NO_QUERY, CLAUDE_JSON_PARSE_FAIL, EMPTY_RESPONSE, REQUEST_TIMEOUT, INTERNAL_ERROR.
- **UI/UX Considerations**: Delivers concise, dense trading terminal-style JSON output, consistently including TradingView chart links.
- **Portfolio Management**: Offers portfolio review for up to 25 tickers with dual scoring and endpoints for managing holdings and events.
- **Portfolio Quotes**: Features asset-type-aware routing for efficient data retrieval from primary and fallback sources (Finnhub, Yahoo Finance, FMP for stocks; CoinGecko, CoinMarketCap for crypto; Yahoo Finance for indices).
- **Intent-Driven Orchestration**: Query routing uses OpenAI to output a structured plan `{intent, asset_classes, modules, risk_framework, response_style, priority_depth, filters, tickers}` for dynamic query processing.
- **Preset Intent Routing**: 13 canonical `INTENT_PROFILES` bypass OpenAI classification for common queries, ensuring efficiency.
- **Heuristic Fallback**: In case of OpenAI classification failure, a heuristic fallback routes queries based on keywords to prevent service interruption.
- **Data Architecture & Performance**: Utilizes local TA computation, tiered data sources with fallbacks, and scan budgeting with `BudgetTracker` for efficient resource management. Caching strategies are implemented for various data types.

## External Dependencies
The platform integrates with a wide array of third-party services and APIs:
- **AI**:
    - **OpenAI**: GPT-4o for orchestrator/query classification.
    - **Anthropic**: Claude Sonnet for reasoning/analysis layer (Sonnet 4 for fast scans, Sonnet 4.5 for deep research).
- **Market Data & Screening**:
    - **Finviz**: Screener.
    - **Polygon.io**: Technicals from bar data, news.
    - **Finnhub**: Primary stock quotes, company profiles, insider trading, earnings, recommendations, social sentiment.
    - **Financial Modeling Prep (FMP)**: Fallback quotes, commodities, portfolio events, economic calendar, treasury rates.
    - **Alpha Vantage**: AI-powered news sentiment.
    - **Nasdaq**: Economic calendar.
- **Social Sentiment & Trending**:
    - **Reddit/ApeWisdom**: WSB, r/stocks, r/options, r/investing, r/daytrading sentiment.
    - **StockTwits**: Social sentiment.
    - **Yahoo Finance**: Trending.
    - **xAI Grok**: Real-time X/Twitter sentiment.
- **Financial Analysis**:
    - **StockAnalysis**: Financials, overview, analyst ratings.
- **Economic Data**:
    - **FRED (Federal Reserve Economic Data)**.
    - **CNN**: Fear & Greed Index.
- **Cryptocurrency Data**:
    - **CoinGecko**: Spot, derivatives, social, categories.
    - **CoinMarketCap**: Trending, most-visited, new listings.
    - **Hyperliquid**: Real-time perpetual futures data.
    - **altFINS**: Pre-computed technical indicators, trend scores (crypto scanner only).

## Data Architecture & Performance
- **Local TA Computation**: `data/ta_utils.py` computes RSI, MACD, SMA/EMA locally from OHLCV bars. Used by both Finnhub and Polygon providers for consistency.
- **Tiered Data Sources**: `fetch_with_fallback()` wrapper tries primary source with timeout, falls back to secondary. Config: equity_price (Finnhub), crypto (CoinGecko+CMC parallel), macro (FRED).
- **Scan Budgeting**: `BudgetTracker` uses weighted points (MAX_BUDGET_POINTS=50) with per-call-type weights (macro=1, quote=1, candle=2, fundamentals=3, crypto=4). Adaptive per-preset budgets: macro_outlook (25pts, no deep dive), microcap_asymmetry (60pts, 12s). Graceful degradation: attaches `data_completeness: "partial"` metadata when budget exhausts so Claude can disclose limited data.
- **TA Fallback Chain**: Finnhub stock_candles→local TA (primary) → Polygon bars→local TA (fallback).
- **Cache TTLs**: Macro overview 10min, sector ETF performance 5min, Fear & Greed 5min, FRED 10min. Candles cached 5min. Macro snapshot (key_numbers) cached 90s.
- **Macro Snapshot**: `_build_macro_snapshot()` runs BEFORE heavy scans in daily_briefing and macro_overview. Fetches SPY/QQQ/IWM/GLD/USO via Finnhub quotes, VIX/10Y via FRED, DXY via FMP (fallback: UUP via Finnhub). Not subject to BudgetTracker. Logged as `[MACRO_SNAPSHOT] filled=... missing=...`.
- **Social→FA Discipline**: Backend enforces that high social scores (>=60) without supporting technical (>=45) or catalyst (>=45) scores get a 15% penalty + `SOCIAL_UNCONFIRMED` flag. Prevents hype-only recommendations.
- **altFINS Restriction**: Only used in crypto_scanner pipeline, removed from general chat context.
- **Cross-Asset Ranker v2** (`data/cross_asset_ranker.py`): Penalty-based scoring (no hard deletion). Coverage quotas: L≥1, M≥2, S≥2 equities, ≥2 crypto, ≥2 commodities. Backfill from adjacent tiers when quotas unmet. Max 18 final picks. Candidates carry `confirmation_status` (confirmed/partial/unconfirmed) and `data_gaps` arrays for Claude's confidence adjustment. `cross_asset_debug` metadata in response meta.
- **Cross-Asset Trending Contract**: Claude MUST output all groups (Equities L/M/S, Crypto, Commodities) with minimums (5 equities, 2 crypto, 2 commodities). Single-pick responses banned. Unconfirmed/backfill items labeled "Watchlist" with confidence penalties. EXCLUDED section removed. Mandatory SOCIAL TRADING SIGNAL section at top with highest-velocity pick, confirmation grid (TA/Volume/Catalyst/FA), and TRADE IDEA vs WATCHLIST classification.
- **Cross-Market Structured Output Schema**: Grouped lists: `structured.social_trading_signal` (single object), `structured.equities.large_caps/mid_caps/small_micro_caps` (lists), `structured.crypto` (list), `structured.commodities` (list). NO flat `top_picks` array. Each item has: symbol, classification, rating, confidence, thesis_bullets[], confirmations {ta,volume,catalyst,fa} (booleans), receipts[], position_size, why_could_fail, catalyst, chart, trade_plan. Optional: score, social_velocity_label, mention_velocity_score. social_trading_signal additionally has risks[]. Renderer backward-compatible: falls back to flat top_picks if grouped lists empty.
- **Social Signal Rank**: `_compute_social_signal_rank()` in claude_agent.py computes weighted rank per Grok candidate: velocity(50%) + engagement(20%) + cross-platform(20%) + catalyst(10%). Assigns TRADE IDEA (velocity high/extreme + confirmation) or WATCHLIST classification. Logged as `[SOCIAL_SPIKE]`.
- **Grok Velocity Fields**: cross_asset scan returns `mention_velocity_score` (0-100), `mention_velocity_label`, `source_mix`, `catalyst_hint` per item.
- **Biggest Volume**: Uses volume % increase (not absolute volume). When avg unavailable, labels "volume data limited".