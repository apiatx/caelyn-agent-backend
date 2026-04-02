# Caelyn AI — FastAPI Trading Analysis Platform

## Architecture

FastAPI Python backend (port 5000) with two main tracks:

1. **`/api/caelyn-terminal`** — Real portfolio analytics (NVDA, OSS, BUZZ via Tradier; GOLD via Yahoo; BTC via CoinGecko)
2. **`/api/hyperliquid/screener`** — Live Hyperliquid perpetuals/spot screener

## Key Files

| File | Role |
|---|---|
| `backend/main.py` | FastAPI app entry point |
| `backend/services/hyperliquid/websocket_manager.py` | Boot sequence + WS consumer + periodic tasks |
| `backend/services/hyperliquid/normalizer.py` | REST snapshot → ScreenerAsset; universe filtering |
| `backend/services/hyperliquid/feature_engine.py` | Signal/score computation (all 14 components) |
| `backend/services/hyperliquid/signals.py` | Agent briefing, hero signals, section builder |
| `backend/services/hyperliquid/router.py` | HTTP endpoints + row serializer |
| `backend/services/hyperliquid/state.py` | In-memory state (assets, candles, trades, books, OI history) |
| `backend/services/hyperliquid/models.py` | ScreenerAsset Pydantic model |

## Universe Filtering (HL_STRICT_UNIVERSE_ONLY)

Default: enabled (`true`).

- **Perps**: All entries from `metaAndAssetCtxs.universe[]` except `isDelisted=true` are admitted. 39 delisted perps dropped on boot, 190 active perps admitted.
- **Spot**: All entries from `spotMetaAndAssetCtxs.universe[]` are admitted to state (288 markets), representing the full official Hyperliquid spot universe. A **$50K/day display-layer volume gate** in the snapshot endpoint eliminates user-created junk tokens (GPT, 2Z, DROP, JPEG, etc.) from all responses. This leaves ~31 actively-traded spot markets visible.
- **Allowlists**: `state.perp_allowlist`, `state.spot_allowlist`, `state.universe_allowlist` (combined) built at boot.
- **Logging**: Every dropped delisted perp logs `[HL][universe] unknown_market_filtered coin=X source=perp reason=delisted`.

## API Identity Fields (every market row)

| Field | Description |
|---|---|
| `canonicalCoinId` | Exact Hyperliquid market identifier (key in state.assets) |
| `displaySymbol` | Clean frontend label (strips /USDC suffix from spot) |
| `isListedOnHyperliquid` | Always `true` — non-universe assets never enter state |
| `marketType` | `"perp"` or `"spot"` |

## Screener Endpoints

| Endpoint | Description |
|---|---|
| `GET /api/hyperliquid/screener/snapshot` | Full market snapshot with filters (market_type, min_volume_usd, max_spread_bps, sort_by) |
| `GET /api/hyperliquid/screener/hero` | Agent Market Brief: market_regime, best_long/short, guidance buckets, selected_thesis |
| `GET /api/hyperliquid/screener/sections` | 21 signal sections + 9 summary cards + 5 hero signals |
| `GET /api/hyperliquid/screener/agent-rank` | POST-style ranking with rationale |
| `GET /api/hyperliquid/screener/asset/{coin}` | Single asset detail with candles, trades, L2, score history |

## Scoring Engine (version 3.0 — Hierarchical Pipeline)

### Pipeline stages
1. **Candle features** — momentum, volatility (5m + 1h candles, up to 120 bars)
2. **Short-term scores** — 14 signal components (unchanged from v2)
3. **Structural quality + regime** — `compute_structural_quality()` uses 1h candle history
4. **Bucket classification** — 5 buckets based on regime + structural quality
5. **Hero selection** — guardrails prevent dead-cat bounces from appearing as top longs

### 7 Asset Regimes (new in v3)
| Regime | Meaning |
|---|---|
| `structural_uptrend_pullback` | Multi-day uptrend intact, buyable pullback |
| `structural_uptrend_breakout_watch` | Coiling/tightening base on top of uptrend |
| `late_extension_exhaustion` | Good uptrend but aging + momentum fading |
| `speculative_reversal` | Weak structure, short-term reversal signals only |
| `downtrend_dead_cat` | Long downtrend + sharp short-term spike = dead cat |
| `chop_low_quality` | No clear trend, mixed signals |
| `collapse_risk` | Active breakdown, OI dropping, book deteriorating |

### 8 Score Families (new in v3)
| Score | Description |
|---|---|
| `structural_quality_score` | 0-100: overall multi-day structural quality (primary filter) |
| `liquidity_quality_score` | Comprehensive liq + tradability quality |
| `pullback_quality_score` | Quality of pullback within an uptrend |
| `breakout_readiness_score` | Range tightening + volume dry-up + base quality |
| `continuation_score` | Likelihood trend continues |
| `speculative_reversal_score` | Quality as a speculative short-term bounce |
| + all 8 v2 setup scores | unchanged |

### structural_quality_score factors (from 1h candles)
- Long-window OLS slope (100 bars ≈ 4 days) → 30%
- Pct of bars above rolling median → 20%
- Higher-high / higher-low persistence in synthetic 4h bars → 20%
- Range tightening (base/consolidation quality) → 15%
- Momentum persistence (green bar ratio) → 15%

### 5 Guidance Buckets (new in v3)
| Bucket | Criteria | Legacy alias |
|---|---|---|
| `buy_now` | regime=uptrend_pullback, SQ≥52, liq≥38, flow≥46 | `trade_now` |
| `high_quality_watchlist` | SQ≥45, uptrend regime, setup not yet triggered | `watch_breakout` |
| `speculative_reversals` | regime=speculative_reversal or dead_cat | (new) |
| `collapse_watch` | regime=collapse_risk or late_extension_exhaustion | `watch_collapse` |
| `avoid` | high tradability_penalty or illiquid | unchanged |

### Guardrails for top longs (hero)
- Must NOT be in `downtrend_dead_cat` or `speculative_reversal` regime
- Must have `structural_quality_score ≥ 42`
- Must come from `buy_now` or `high_quality_watchlist` bucket (never from speculative_reversals)

### 7 Setup types (unchanged from v2)
`breakout`, `mean_reversion`, `trend_continuation`, `crowding_unwind`, `exhaustion`, `collapse_risk`, `avoid`

## Rolling History

| History | Source | Available |
|---|---|---|
| OI changes (5m/15m/1h) | ~60s snapshot intervals | After ~5 min |
| Score changes | Same | After ~1 cycle |
| Volume impulse (5m/15m) | 5m candle data | Available at boot |

## Market Regime (generate_agent_briefing)

5 regimes from breadth (long_pct/short_pct), avg funding, composite score, exhaustion_pct:
`risk_on_bull`, `risk_off_bear`, `crowded_leveraged_bull`, `exhausted_distribution`, `mixed / rotational`

## Sector Rotation Dashboard (`/api/sector-rotation/`)

| Endpoint | Description |
|---|---|
| `GET /api/sector-rotation/dashboard` | 11 SPDR sector ETFs with rotation scores, YTD, 1M, MA %, rel-SPY, regime tags; macro overlay (FRED); market breadth; cyclical-vs-defensive spread. 300s in-memory cache. |
| `GET /api/sector-rotation/history?range=1y` | 1Y daily price series for all 11 ETFs (yfinance); supports 1m/3m/6m/1y |
| `GET /api/sector-rotation/analysis` | Cached Gemini AI analysis (7-day disk cache at `backend/data/sector_rotation_analysis.json`) |
| `POST /api/sector-rotation/refresh-analysis` | Force-regenerate Gemini AI analysis with Google Search grounding |

### Data Sources
- **Quotes**: Finnhub real-time (1m cache)
- **History**: yfinance daily bars
- **Macro**: FRED (Fed Funds, CPI, 10Y, 2Y; YC spread computed)
- **AI Analysis**: `gemini-3-flash-preview` + `google_search` tool grounding

### Rotation Scoring Formula
`rotation_score (0-100)` = 25% 1M rank + 25% YTD rank + 20% pct-above-50MA + 15% pct-above-200MA + 15% rel-vs-SPY-30D

Regime tags: Leading≥70, Improving≥50, Weakening≥30, Lagging<30

### AI Analysis Schema
Fields: `market_regime`, `macro_regime`, `leadership_style`, `summary`, `current_leadership` (leaders/laggards/explanation), `scenarios` (name/probability/sector_winners/sector_losers), `watch_items`, `sources`, `generated_at`

## Insider Activity Dashboard (`/api/insider-activity`)

Fetches SEC Form 4 filings via edgartools, scores each transaction 0-100, stores in Neon PostgreSQL with 30-day retention.

| Endpoint | Description |
|---|---|
| `GET /api/insider-activity` | Paginated feed with filters: `type`, `timeframe`, `min_score`, `sort`, `order`, `search`, `limit`, `offset`, `cluster_type` (coordinated_buy/coordinated_sell/lockup_expiry/mixed), `clustered_only` (bool), `sector` |
| `GET /api/insider-activity/stats` | Aggregate stats: total_transactions, buys, sales, avg_buy_score, top_buy, top_sell, last_refresh, refresh_in_progress |
| `GET /api/insider-activity/{ticker}` | All transactions for ticker + insider_summary (net_direction, buy/sell 30d values) |
| `GET /api/insider-activity/detail/{accession_number}` | Full detail including score_breakdown, price_context, cluster_type, cluster_metadata |
| `POST /api/insider-activity/refresh` | Manual refresh trigger |

### 8-Factor Conviction Scoring (0-100)

| Factor | Weight |
|---|---|
| Transaction size ($) | 15 |
| Insider role (CEO/Director/10%) | 20 |
| Transaction type (buy vs grant vs sale) | 10 |
| Price context (near 52w low, vs MA) | 15 |
| Cluster activity (type-multiplied: coordinated_buy×1.0, coordinated_sell×0.9, mixed×0.5, lockup_expiry×0.2) | 15 |
| Position change % | 10 |
| Track record (filing frequency) | 10 |
| Earnings proximity | 5 |

### Cluster Classification (`cluster_type`)
`_detect_cluster_type()` queries DB for same-ticker transactions within ±14-day window and classifies:
- `coordinated_buy`: >70% buys → multiplier 1.0
- `coordinated_sell`: >70% sells, diverse stagger → multiplier 0.9
- `lockup_expiry`: >70% sells, tight date-spread (≤2 days) or low position-impact stdev → multiplier 0.2
- `mixed`: else → multiplier 0.5
Metadata: cluster_size, date_spread_days, insiders_in_cluster, distinct_role_count, total_cluster_value, avg/stdev position impact

### Data Pipeline
- **Source**: edgartools `get_filings(form="4")` → `filing.obj().market_trades` DataFrame
- **Price enrichment**: Tradier batch (50/call) → Finnhub quote+metric → yfinance fallback
- **DB**: Neon PostgreSQL `insider_transactions` table; `expires_at = NOW() + 30 days`
- **Background loop**: Creates table → cleans expired rows → initial load (300 filings) → refreshes every 2 hours
- **Dedup key**: `{accession_number}:{row_idx}` (max 29 chars) — handles multiple transactions per filing

## Whale Watch — Institutional 13F Tracker (`/api/whales`)

Tracks the top 10 institutional investors via SEC EDGAR 13F-HR filings. Fetches quarterly holdings, maps CUSIPs to tickers via OpenFIGI, calculates weighted portfolio returns (1m/3m/6m/1y vs SPY), and generates AI theme summaries via Anthropic.

### Tracked Institutions

| Name | CIK | Manager |
|---|---|---|
| Berkshire Hathaway | 1067983 | Warren Buffett |
| Pershing Square Capital Management | 1336528 | Bill Ackman |
| Duquesne Family Office | 1536411 | Stanley Druckenmiller |
| Elliott Investment Management | 1791786 | Paul Singer |
| Appaloosa Management | 1006438 | David Tepper |
| Baupost Group | 1061768 | Seth Klarman |
| Third Point | 1040273 | Dan Loeb |
| Soros Fund Management | 1029160 | George Soros |
| Renaissance Technologies | 1037389 | Jim Simons |
| Bridgewater Associates | 1350694 | Ray Dalio |

### Endpoints

| Endpoint | Description |
|---|---|
| `GET /api/whales?category=institution` | All whales sorted by 3m return (best first) |
| `GET /api/whales/{whale_name}/holdings` | Latest quarter holdings sorted by weight_pct |
| `GET /api/whales/{whale_name}/returns` | All quarterly return records vs SPY benchmark |
| `POST /api/whales/refresh` | Trigger background refresh of all 10 whales |
| `POST /api/whales/{whale_name}/refresh` | Trigger refresh for a single whale |

### Data Pipeline

- **Source**: SEC EDGAR `data.sec.gov/submissions/CIK{cik}.json` → find 13F-HR accession → parse infotable XML
- **CUSIP → Ticker**: OpenFIGI batch API (25/request, free tier, US equities preferred); capped at top 500 positions by value per whale (covers 95%+ of portfolio weight; prevents 30+ min waits for mega-filers like RenTech's 3000+ positions)
- **yfinance Fallback**: For CUSIPs OpenFIGI misses, searches by company name — limited to the capped set
- **Returns**: yfinance batch download (2y) → weighted portfolio 1m/3m/6m/1y returns; SPY as benchmark
- **AI Themes**: Claude claude-haiku-4-5 summarizes top-15 holdings into 2-3 sentence investment thesis
- **Background loop**: Runs on startup if stale (> 24h); refreshes all whales every 24h
- **DB Tables**: `whales`, `whale_holdings`, `whale_portfolio_returns` (Neon PostgreSQL)
- **Key file**: `backend/services/whale_watch_service.py`
- **Safety guard**: `_save_holdings_to_db` skips overwrite when 0 holdings resolved (prevents wiping DB on failed refreshes)
- **No concurrent refreshes**: `_cusips_to_tickers` has no semaphore — never trigger multiple whale refreshes simultaneously (OpenFIGI free tier: 5 req/min)

## Predict Page — Polymarket Intelligence + TradingAgents (`/api/predict/`)

Integrates Jon-Becker/prediction-market-analysis methodology and TauricResearch/TradingAgents architecture.

### New Endpoints

| Endpoint | Description |
|---|---|
| `GET /api/predict/markets?limit=50&tag=&min_volume=0` | Enhanced market list: edge detection, momentum, whale signals, Kelly fraction, efficiency score |
| `GET /api/predict/signals` | Dashboard intelligence: top edges, mispricings, surging markets, whale watch, top by volume |
| `GET /api/predict/whale-watch?limit=20` | Markets with vol/liquidity ratio > 5x — large coordinated position signals |
| `GET /api/predict/categories` | Volume + count breakdown by tag (uses Gamma events API for real tag data) |
| `GET /api/predict/market/{condition_id}` | Deep single-market analysis with order book depth, Kelly fraction, book imbalance |
| `GET /api/predict/context?question=...` | Pre-analyze: finds relevant Polymarket markets for a given question |
| `POST /api/predict/analyze` body: `{"question":"..."}` | Full 6-agent TradingAgents pipeline → final recommendation |
| `GET /api/polymarket/intelligence` | Alias for `/api/predict/signals` |

### Per-Market Analytics Fields (Jon-Becker methodology)

| Field | Description |
|---|---|
| `yes_price` / `no_price` | Current market prices (0–1) |
| `yes_pct` / `no_pct` | As percentage (0–100) |
| `spread_pct` | Bid-ask spread as % |
| `volume_24h` / `volume_1wk` / `volume_1mo` | Volume at different timeframes |
| `volume_momentum` | `surging` / `accelerating` / `stable` / `fading` (24h vs 7d avg) |
| `whale_activity` | true if vol/liquidity > 5x (large coordinated positions) |
| `vol_liq_ratio` | Raw volume/liquidity ratio |
| `edge_detected` / `edge_pct` | Whether implied probs ≠ 100% (mispricing signal) |
| `mispricing_score` | Distance between displayed price and best bid/ask mid |
| `market_efficiency_score` | 0-100 score (tight spread + high liquidity + competitive flag) |
| `kelly_fraction_pct` | Kelly Criterion position size recommendation (%) |
| `is_competitive` | Polymarket competitive market flag (sharp money marker) |
| `days_to_expiry` | Calendar days to market close |
| `price_momentum_pct` | Last trade vs displayed price % delta |

### TradingAgents Pipeline (`POST /api/predict/analyze`)

Phase 1 (parallel): `FundamentalsAgent` + `SentimentAgent` + `TechnicalAgent`
Phase 2 (parallel): `BullAgent` + `BearAgent` (receive Phase 1 outputs)
Phase 3 (sequential): `RiskManagerAgent` → final decision

All agents use `gemini-3-flash-preview` with Google Search grounding.

Response includes `agents.{fundamentals,sentiment,technical,bull,bear,risk_manager}` and a top-level `final` object with:
- `recommendation`: `LONG_YES | LONG_NO | PASS`
- `final_yes_probability_pct`: synthesized fair value
- `consensus_probability_pct`: simple average of 5 agent estimates
- `market_price_pct`: current Polymarket price
- `edge_pct`: edge vs market price
- `conviction`: `low | medium | high | very_high`
- `debate_winner`: `bull | bear | draw`
- `thesis`, `key_risk`, `position_sizing`, `entry_note`, `exit_note`

### Caelyn AI Enhancement

When the prediction_markets category fires, the agent now receives `intelligence_signals` in its context:
- `summary`: market-wide stats
- `top_edges` / `top_mispricings`: actionable mispricing signals
- `surging_markets`: smart money accumulation signals
- `whale_markets`: large coordinated positioning

The `PREDICTION_MARKETS_CONTRACT` in prompts.py instructs Caelyn to reference these signals explicitly.

### Key Files
| File | Role |
|---|---|
| `backend/services/predict/polymarket_intelligence.py` | Jon-Becker analytics engine (edge detection, whale watch, Kelly, efficiency scoring) |
| `backend/services/predict/trading_agents.py` | TauricResearch multi-agent pipeline (6 Gemini agents) |
| `backend/services/predict/router.py` | FastAPI router for all `/api/predict/*` and `/api/polymarket/intelligence` |

## Real Portfolio (caelyn-terminal)

- SQGLP framework for investments, Weinstein Stage 2 for trades
- Positions: NVDA, OSS, BUZZ (Tradier), GOLD→GC=F (Yahoo), BTC (CoinGecko)
- Cache key: `caelyn:terminal:v7`
