---
name: Options sectors supplement scan
description: How theme-only symbols are scanned for the /api/options-flow/sectors endpoint
---

## Rule
Theme proxy symbols not in the master screener are scanned by `_theme_options_supplement_loop` (main.py). Combined data is accessed via `get_combined_ticker_data()` in `data/options_theme_supplement.py`. Each ticker node has a `scan_status` field.

## scan_status values
- `"live"` — ticker appeared in master screener cache (unusual-flow threshold met)
- `"supplement"` — data from supplement loop scan (slower cadence, lower activity threshold)
- `"no_options"` — Stage-1 confirmed no tradeable options expirations exist
- `"pending"` — not yet reached by any scan pass

## Supplement loop design
- Cadence: 6 symbols per batch, every 10 minutes (~2.4 calls/min, <2.2% of 110/min budget)
- 150s startup delay (lets master screener warm up first)
- Uses same `_TRADIER_GLOBAL_SEM` and `TradierFlowEngine` — NO second Tradier client
- Results cached in `options_theme_supplement_v1` (30-min TTL)
- Full coverage of 222 theme-only symbols: ~7 hours per cycle

## Seed injection
- On each prefilter cold rebuild (1h TTL), up to 60 curated theme proxy symbols are injected into `_cycle_seeds` (ETF proxies prioritised)
- High-activity theme symbols that reach Stage 2 will appear as `scan_status="live"` directly in the master cache

## No-options tracking
- After every master screener cycle AND every supplement batch, symbols with confirmed empty expirations from Stage-1 expiry cache are persisted to `options_no_options_tracking:v1` (24h TTL)
- Zero extra Tradier calls — reuses existing Stage-1 data

## Key files
- `data/options_theme_supplement.py` — supplement module (new)
- `data/options_flow_sectors.py` — sectors aggregation using combined data
- `main.py` — `_theme_options_supplement_loop()`, seed injection in `_master_screener_loop`

**Why:** Master screener (Stage 2 limit = 30) only captures unusual-activity tickers. Theme baskets need coverage of all proxy symbols including low-activity ones. A slow supplemental scan with a negligible rate budget covers the gap without polluting the master screener or creating a second Tradier client.

**How to apply:** When updating sectors endpoint or theme proxy symbols, check both master cache AND supplement cache via `get_combined_ticker_data()`. Never read only one source.
