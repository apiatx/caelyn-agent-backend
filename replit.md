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

## Scoring Engine (version 2.1)

7 setup types: `breakout`, `mean_reversion`, `trend_continuation`, `crowding_unwind`, `exhaustion`, `collapse_risk`, `avoid`

Override priority: `avoid > collapse_risk (>65) > exhaustion (>65) > best_setup`

14 score components: liquidity, volatility, momentum, flow, trend, book_pressure, crowding, dislocation, tradability_penalty, mean_reversion, breakout, trend_continuation, crowding_unwind, exhaustion, collapse_risk, avoid

## Rolling History

| History | Source | Available |
|---|---|---|
| OI changes (5m/15m/1h) | ~60s snapshot intervals | After ~5 min |
| Score changes | Same | After ~1 cycle |
| Volume impulse (5m/15m) | 5m candle data | Available at boot |

## Market Regime (generate_agent_briefing)

5 regimes from breadth (long_pct/short_pct), avg funding, composite score, exhaustion_pct:
`risk_on_bull`, `risk_off_bear`, `crowded_leveraged_bull`, `exhausted_distribution`, `mixed / rotational`

## Real Portfolio (caelyn-terminal)

- SQGLP framework for investments, Weinstein Stage 2 for trades
- Positions: NVDA, OSS, BUZZ (Tradier), GOLD→GC=F (Yahoo), BTC (CoinGecko)
- Cache key: `caelyn:terminal:v7`
