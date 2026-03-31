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
| `GET /api/insider-activity` | Paginated feed with filters: `type` (buys/sells/all), `timeframe` (1w/1m/3m), `min_score`, `sort`, `order`, `search`, `limit`, `offset` |
| `GET /api/insider-activity/stats` | Aggregate stats: total_transactions, buys, sales, avg_buy_score, top_buy, top_sell, last_refresh, refresh_in_progress |
| `GET /api/insider-activity/{ticker}` | All transactions for a specific ticker |
| `GET /api/insider-activity/detail/{accession_number}` | Full detail including score_breakdown, price_context |
| `POST /api/insider-activity/refresh` | Manual refresh trigger |

### 8-Factor Conviction Scoring (0-100)

| Factor | Weight |
|---|---|
| Transaction size ($) | 20 |
| Insider role (CEO/Director/10%) | 20 |
| Transaction type (buy vs grant vs sale) | 10 |
| Price context (near 52w low, vs MA) | 15 |
| Cluster buying (multiple insiders same window) | 10 |
| Position change % | 10 |
| Track record (filing frequency) | 10 |
| Earnings proximity | 5 |

### Data Pipeline
- **Source**: edgartools `get_filings(form="4")` → `filing.obj().market_trades` DataFrame
- **Price enrichment**: Tradier batch (50/call) → Finnhub quote+metric → yfinance fallback
- **DB**: Neon PostgreSQL `insider_transactions` table; `expires_at = NOW() + 30 days`
- **Background loop**: Creates table → cleans expired rows → initial load (300 filings) → refreshes every 2 hours
- **Dedup key**: `{accession_number}:{row_idx}` (max 29 chars) — handles multiple transactions per filing

## Real Portfolio (caelyn-terminal)

- SQGLP framework for investments, Weinstein Stage 2 for trades
- Positions: NVDA, OSS, BUZZ (Tradier), GOLD→GC=F (Yahoo), BTC (CoinGecko)
- Cache key: `caelyn:terminal:v7`
