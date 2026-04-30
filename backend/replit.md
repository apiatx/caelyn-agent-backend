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

## Should I Be Trading? Dashboard
- **URL**: `/should-i-be-trading` (Bloomberg Terminal-style HTML dashboard)
- **API**: `GET /api/trading-dashboard?mode=swing|day` — 5-pillar market scoring
- **API**: `POST /api/trading-dashboard/refresh` — force-clear the 90s cache
- **Pillars** (30/25/20/15/10% weight for swing; 25/20/20/15/20% for day):
  1. Volatility/Risk — VIX level + percentile + trend, put/call ratio, HY OAS, F&G
  2. Trend & Structure — SPY/QQQ vs SMA50/SMA200 (computed from Tradier 320d history), market regime
  3. Market Breadth — F&G breadth/strength/safe-haven, sector ETF participation count
  4. Macro/Liquidity — 2s10s spread + DXY (Yahoo Finance) + FOMC/CPI calendar
  5. Momentum/Sentiment — F&G momentum + put/call + sector leader/laggard
- **Data sources for enrichment** (fetched fresh per request, then cached):
  - SPY/QQQ SMA50/SMA200: Tradier `get_history` (320 calendar days → ~220 trading days), cache 1h
  - Sector ETF performance: Tradier `get_quotes` for 11 sector ETFs (XLK, XLV, XLF, XLE, XLI, XLP, XLY, XLB, XLU, XLRE, XLC), cache 30s
  - VIX history: FRED via MacroProvider, 12-month window
  - NOTE: FMP `v3/quote` endpoint is "Legacy" (blocked for new API keys) — all quote/MA data now uses Tradier
- **Scoring**: MQS (Market Quality Score 0-100) → EWS (penalized for FOMC/CPI/Jobs)
- **Decision**: YES ≥ 70, CAUTION 40-69, NO < 40
- Static files served from `backend/static/` via `/static` mount

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

## Prophetik Signal Engine — Investor Mode (NEW)
- **Purpose**: Translates Polymarket prediction market activity into equity/macro implications for traditional stock investing. Fully additive — zero changes to existing Gambler mode endpoints.
- **Module**: `backend/services/predict/investor/`
  - `themes.py` — 9 equity-relevant macro theme definitions with keyword/tag/category matching rules
  - `classifier.py` — per-market theme classification (equity_relevance_score, sector_relevance_score, regime_relevance_score, multi-theme support)
  - `clustering.py` — aggregates markets per theme: weighted_odds_shift_24h/7d, confidence, consistency, contradiction, freshness, regime_signal_strength, summary_direction
  - `impact_engine.py` — deterministic curated mappings: theme+direction → bullish/bearish sectors+stocks, asset baskets, regime implications, narrative text
  - `regime.py` — 7 regime indicators (risk_on_vs_risk_off, inflationary_vs_disinflationary, growth_vs_slowdown, geopolitical_stress_vs_easing, higher_for_longer_vs_easing, commodity_pressure_vs_relief, ai_capex_supportive_vs_restrictive) derived from cluster signals
  - `investor_intel.py` — orchestration: fetch scored markets (reuses existing cache) → classify → cluster → impact → regime → payload
  - `router.py` — 4 new endpoints
- **Endpoints (NEW)**:
  - `GET /api/predict/investor/overview` — full investor payload (top_equity_signals, sector_rotation, watchlists, regime_scoreboard, theme_clusters)
  - `GET /api/predict/investor/themes` — theme clusters with full equity impact data (drill-down)
  - `GET /api/predict/investor/regime` — regime scoreboard only (lightweight)
  - `GET /api/predict/investor/watchlists` — stock watchlists (bullish/bearish/conditional) + sector reference
- **9 Themes**: macro_rates_inflation, geopolitics_war_trade, energy_commodities, us_politics_policy, ai_semis_tech, crypto_risk_appetite, china_taiwan_supply_chain, defense_security, consumer_labor_growth
- **Classification**: deterministic keyword/tag/category matching, multi-theme per market, equity_relevance_score 0-100
- **Sectors tracked**: Energy, Defense/Aerospace, Airlines/Transport, Industrials, Financials, Semiconductors, Software/Growth Tech, Cybersecurity, AI Infra/Data Centers, REITs/Housing, Consumer Discretionary, Utilities/Nuclear Power, Gold/Metals/Commodities, Crypto Proxies, Small Caps, Clean Energy
- **Cache**: 150s TTL, reuses existing scored-market cache (no extra Polymarket API calls)
- **Gambler endpoints**: ALL unchanged — /api/predict/recommendations, /signals, /scored, etc. fully intact

## Hyperliquid Screener Service
- **Module**: `backend/services/hyperliquid/` (dedicated service layer)
- **Boot sequence**: On startup — fetches `metaAndAssetCtxs` (229 perps) + `spotMetaAndAssetCtxs` (286 spot), `allMids` (534 coins), 1h candles for top-40 by volume, 5m candles for top-20, L2 books for top-20 → full feature pass → WS connect
- **WebSocket**: Connects to `wss://api.hyperliquid.xyz/ws` — subscribes to `allMids` (1 sub) + `activeAssetCtx` for top-50 by OI + `bbo` top-30 + `trades` top-30. Auto-reconnects with exponential backoff.
- **Periodic tasks**: Candle refresh every 5 min; full feature recompute every 60s
- **Endpoints**:
  - `GET  /api/hyperliquid/screener/snapshot` — full universe, sort/filter by query params
  - `GET  /api/hyperliquid/screener/filters` — available filter options for UI
  - `GET  /api/hyperliquid/screener/asset/{coin}` — single asset detail with candles/trades/book
  - `POST /api/hyperliquid/screener/agent-rank` — deterministic ranking (modes: balanced/momentum/breakout/mean_reversion/crowding_dislocation)
  - `WS   /api/hyperliquid/screener/ws` — live push to frontend (snapshot_ready / asset_update events)
- **Scores computed**: liquidity, volatility, momentum, flow, mean_reversion, breakout, composite_signal_score
- **Flags**: crowded_long, crowded_short, squeeze_candidate, dislocated_vs_oracle, trend_continuation_candidate, avoid_due_to_spread, illiquid_high_volatility
- **No auth / no private keys required** — 100% public Hyperliquid market data

## Serenity Discovery Engine (Phase 3) — `/api/playbooks/discover`

### Purpose
On-demand supply-chain intelligence layer for Serenity strategy. Identifies hidden bottleneck companies within the supply chains of major platform giants (NVDA, MSFT, GOOGL, META, AMZN, TSM, AVGO, xAI, Hyperscalers, AI_Power). Fully isolated from `/api/query`.

### New Routes
- `POST /api/playbooks/discover` — run the discovery engine
- `POST /api/playbooks/supply-chain-map` — structured multi-layer chain map
- `GET  /api/playbooks/themes` — list all 14 supported discovery themes
- `GET  /api/playbooks/giants` — list all 10 giant anchors
- `GET  /api/playbooks/discovery-capabilities` — engine metadata

### Discovery Modes
- `giant_chain` — traverse supply chain from a giant anchor (requires `giant`)
- `theme_scan` — scan by theme IDs (requires `theme_ids`)
- `foreign_bottlenecks` — non-US positions with US access guidance
- `ticker_chain` — upstream/downstream neighbors of a known ticker
- `country_theme_scan` — cross-filter by country + theme
- `custom` — flexible fallback

### 8 Scoring Dimensions
`chain_depth_score`, `bottleneck_criticality_score`, `hiddenness_score`, `giant_dependency_score`, `foreign_uniqueness_score`, `supply_chain_confidence_score`, `proxy_accessibility_score`, `theme_purity_score`

Composite rank: bottleneck_criticality(35%) + chain_depth(25%) + hiddenness(20%) + confidence(20%)

### Key Files
- `services/playbook/discovery_types.py` — Pydantic models
- `services/playbook/discovery_service.py` — core engine + scoring
- `services/playbook/discovery_enrichment.py` — provider enrichment (Finnhub/Tradier/FMP/Perplexity)
- `services/playbook/giant_map.py` — 10 giant anchors with theme/capex metadata
- `services/playbook/supply_chain_graph.py` — ~40 curated nodes (5 layers, US+foreign)
- `services/playbook/theme_discovery.py` — 14 themes with policy/priority metadata
- `services/playbook/foreign_market_map.py` — JP/KR/TW/NL/DE/FR/UK with ADRs and ETF proxies

### Provider Rules
- Finnhub = primary (profile, news, international)
- Tradier = US/ADR quotes only (NOT foreign natives)
- FMP = sparing market cap reference (250/day cap)
- Perplexity = shortlist validation only (max 5 per request)
- Brave/Tavily = NOT USED

### Discovery Bridge in /api/playbooks/analyze
If `discovery_mode` is set, the analyze endpoint runs discovery first, injects top US-accessible candidates into the ticker list, and returns `discovery_context` alongside playbook scores. S&J philosophy is unchanged.

### Regime Detection (Phase 6)
`GET /api/playbooks/serenity-regime` — returns full `SerenityRegime` including:
- `regime_id`, `label`, `confidence`, `recommended_mode`, `recommended_depth`
- `anchor_scores[].overlapping_theme_ids` — actual overlapping theme IDs (not just count)
- Regime context propagates through analyze bridge for all Serenity discovery modes

### Strategy Screener Subsystem (Phase 7)
`services/playbook/strategy_screener/` — isolated publication engine on top of Serenity.

**Routes (all under `/api/strategy-screener`):**
- `GET /` or `/latest` — latest snapshot (202 + background refresh if none/stale)
- `GET /snapshots?limit=N` — list recent snapshot metadata
- `GET /config` — cadence info + grade scale
- `GET /report/{snapshot_id}/{ticker}` — full deep-dive report for one candidate
- `POST /refresh` — force manual snapshot regeneration (background task)

**Architecture:**
- `screener_types.py` — Pydantic models: `ScreenerCandidate`, `ScreenerSnapshot`, `ScreenerReport`, `ScreenerConfig`
- `screener_storage.py` — Neon PostgreSQL persistence (tables: `screener_snapshots`, `screener_reports`)
- `screener_report_builder.py` — deterministic section builder (zero LLM dependency): summary, why_it_matters, supply_chain_map_text, competitors, catalysts, rerating_case, key_risk, why_hidden, what_to_verify_next, what_would_break_thesis
- `screener_service.py` — orchestrator: regime → run_discover(auto) → build_full_report × N → save
- `screener_scheduler.py` — stale-refresh pattern; `enqueue_background_refresh()` via asyncio.create_task()
- `screener_router.py` — FastAPI router, registered in main.py

**Grade formula:** `best_blend*0.5 + bottleneck_criticality*0.25 + hiddenness*0.15 + conf_adj`
Thresholds: A+≥82, A≥72, B+≥60, B≥48, C<48. Cadence: 14 days (env `SCREENER_CADENCE_DAYS`), shortlist: 20 (env `SCREENER_SHORTLIST_SIZE`).

### Strategy Screener Market Cap Enrichment (Phase 8.1)
`screener_enrichment.py` — market cap enrichment for ADR/foreign candidates.

**Root cause of Bug 1 & Bug 2**: All 7 ADR/foreign names (TOELY, HASEY, IBDNY, BESIY, ATEYY, SEMCY, MRAAY) had `market_cap_usd=None`. Old code silently treated `None → micro_cap` which put large Japanese/EU companies in the wrong bucket.

**Enrichment provider chain**:
1. FMP stable/profile — returns USD directly, works for US-listed ADRs (primary)
2. Finnhub company_profile2 — international coverage, applies static FX rates for non-USD currencies (fallback)
3. Neither → mark as `"unknown"` (never fake a value)

**Results on current snapshot**: 4/7 enriched via FMP (TOELY=$129B large_cap, BESIY=$21.7B mid_cap, ATEYY=$126B large_cap, MRAAY=$55.2B mid_cap). 3 remain unknown (HASEY/Hanmi Semiconductor, IBDNY/Ibiden, SEMCY/Samsung Electro-Mechanics — not in FMP database).

**Fixed classification** (`classify_market_cap`):
```
None / <$1M   → "unknown"     (was: micro_cap — FIXED)
$1M - $2.5B   → "micro_cap"
$2.5B - $20B  → "small_cap"
$20B - $100B  → "mid_cap"
≥$100B        → "large_cap"
```

**New endpoints**:
- `POST /api/strategy-screener/enrich-market-caps` — backfills market_cap_usd for current snapshot without regeneration. Patches only the results JSONB.

**New response fields** (always present):
- `market_cap_bucket` per result — e.g. `"large_cap"` | `"unknown"`
- `unknown_market_cap_count` — how many candidates have unknown market cap in the snapshot

**New bucket filter**: `?market_cap_bucket=unknown` — explicitly selects candidates with no confirmed market cap. Standard buckets (large/mid/small/micro) now exclude unknowns.

**Going forward**: `generate_snapshot()` automatically calls `enrich_candidates()` after building the shortlist. Future snapshots will have correct market caps from day one.

### Strategy Screener Filter/Sort (Phase 8)
`screener_filters.py` — pure view-layer transform on stored snapshot data. Zero DB writes. Zero regeneration.

**GET /api/strategy-screener/latest query params (all optional, backwards compatible):**
- `market_cap_bucket=large_cap|mid_cap|small_cap|micro_cap`
- `layer=1|2|3`
- `sort_by=best_fit|market_cap|layer|grade` (default: best_fit)
- `limit=1-100` (default: 20)

**Market cap thresholds:** large_cap ≥$100B, mid_cap $20B-$99B, small_cap $2.5B-$19B, micro_cap <$2.5B (None→micro_cap)
**Sort semantics:** best_fit = bbs DESC → bcs DESC → scs DESC; market_cap = mc_usd DESC (None last); layer = depth ASC; grade = A+>A>B+>B>C DESC
**Response extras when filters applied:** `active_filters`, `active_sort`, `filtered_result_count`, `available_result_count`, `limit`
**Unfiltered response is identical to before** — no breaking change.

**GET /api/strategy-screener/config** now includes dropdown metadata:
- `market_cap_buckets`, `layer_filters`, `sort_options` — all with `id` + `label` for frontend dropdowns

### Thematic Context Provider (v2 — Upstream Prefilter)
- **File**: `backend/services/thematic_context_provider.py`
- **Role**: Shared read-only adapter upgraded to upstream prefilter source (not just annotation layer).
- **Endpoints**: `GET /api/thematic-context/snapshot`, `POST /api/thematic-context/refresh`
- **Prefilter API**: `get_thematic_prefilter_universe()` — returns prioritized ticker universe (active → emerging → watchlist → megacap fallback, max 150 tickers) with theme_map per ticker.
- **LKG Disk Persistence**: Snapshot persisted to `data/thematic_context_snapshot.json`. Never empty on cold start — loaded into in-memory cache before any live cache warms up.
- **Static Fallback Registry**: Built from `home_service.THEME_MAP` + `THEME_ETF_UNIVERSE` (31 themes, 126 tickers). Activated when live scores AND LKG are unavailable.
- **snapshot_status**: `"fresh"` (live ETF RS scores) | `"stale_lkg"` (disk LKG) | `"fallback_static"` (static registry)
- **Options Flow injection**: Active/emerging theme tickers prepended to `_master_seeds` in `_master_screener_loop()` when prefilter is cold — thematic candidates scanned with priority before non-thematic seeds.
- **Briefing precompute integration**: Active/emerging theme tickers registered as `"thematic_priority"` source in `_briefing_precompute_loop()` — tickers appearing in both Finviz and thematic universe counted as multi-signal.
- **Startup warmup**: `warmup_thematic_context()` coroutine launched in lifespan handler — loads LKG immediately, then force-rebuilds after 5s.
- **Guarantees**: Never raises. Never calls LLM/Tradier/FMP. Never overwrites good LKG with static fallback.

### Tests
851 tests (0 failures) in `services/playbook/factor_tests.py`. Phase 8 adds 77 filter/sort tests covering market cap classification, bucket filtering, layer filtering, combined filters, all 4 sort modes, tiebreak logic, limit, backwards compatibility, config dropdown metadata, and isolation from /api/query.

## External Dependencies
- **AI**: OpenAI (GPT-4o for orchestration/classification), Anthropic (Claude Sonnet for reasoning/analysis), xAI Grok.
- **Market Data & Screening**: Finviz, TwelveData, Polygon.io, Finnhub, Financial Modeling Prep (FMP), Alpha Vantage, Nasdaq.
- **Social Sentiment & Trending**: Reddit/ApeWisdom, StockTwits, Yahoo Finance.
- **Financial Analysis**: StockAnalysis.
- **SEC Filings**: SEC EDGAR (data.sec.gov).
- **Economic Data**: FRED (Federal Reserve Economic Data), CNN (Fear & Greed Index).
- **Cryptocurrency Data**: CoinGecko, CoinMarketCap, Hyperliquid, altFINS.
- **Options Data**: Public.com.

## Stock Compare Section (Fundamentals)
- **Router**: `backend/routes/stock_compare.py` — mounted at `/api/fundamentals/compare`
- **Service**: `backend/services/stock_compare_service.py`
- **Data source**: FMP Stable API (Starter plan, 300 req/min, 5Y max)
- **Endpoints**:
  - `GET  /api/fundamentals/compare/metrics` — canonical registry of all 24 supported metrics
  - `GET  /api/fundamentals/compare/diagnostics?symbols=X,Y&period=annual` — per-symbol metric availability + per-endpoint status
  - `GET  /api/fundamentals/compare/search?q=...` — ticker autocomplete
  - `POST /api/fundamentals/compare` — multi-ticker comparison (series + screener + snapshot + news)
- **24 supported numeric metrics**: price, price_change_percent, market_cap, enterprise_value, revenue, revenue_growth, gross_profit, gross_margin, operating_income, operating_margin, net_income, profit_margin, eps_diluted, ebitda, free_cash_flow, fcf_margin, total_debt, debt_to_equity, current_ratio, ps_ratio, pe_ratio, ev_to_ebitda, roe, roa
- **recent_news**: non-chart section; sends a warning if used as chart metric (no 500)
- **Fallbacks**: ev_to_ebitda from EV/EBITDA, roe/roa from income+balance, debt_to_equity from balance sheet, price_change_percent from change/previousClose
- **Response shape**: `{ metric, range, symbols, series, screener (24-metric bundle), snapshot (compat), news, metricAvailability, missingSymbols, meta }`
- **Cache TTLs**: statements/ratios/key_metrics 24h; quote 15min; news 30min
- **Never 500**: every per-symbol block wrapped in try/except; unsupported tickers return empty series + warning

## Caelyn Terminal API
- **URL**: `GET /api/caelyn-terminal`
- **Auth**: X-API-Key header (auth currently disabled — all requests pass)
- **Data source**: Reads holdings from `data/portfolio_holdings_{user_id}.json` (falls back to legacy `data/portfolio_holdings.json`)
- **Provider**: `backend/data/caelyn_terminal.py` (CaelynTerminalProvider)
- **Cache**: 90-second in-memory cache (key: `caelyn:terminal:v3`)
- **Data flows**:
  - Quotes: Tradier batch `get_quotes` for all holdings + SPY/QQQ/IWM/GLD/DIA (ticker tape)
  - History: Tradier `get_history` daily 400 days, parallel per holding + SPY (cached 1h)
  - Earnings calendar: Finnhub market-wide calendar filtered to holdings (sync via thread)
  - News ticker: Finnhub `get_company_news` for top 4 holdings by allocation (sync via thread)
  - Market status: Eastern time computation (no API call)
- **Computed fields**: portfolio value/change/perf periods, asset allocation, correlation matrix (NxN top-5), risk metrics (vol, max drawdown, beta, sharpe, sortino), per-holding volatility, rule-based risk suggestions, top movers (2 gainers + 2 losers), earnings calendar, ticker tape, news ticker