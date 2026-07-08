---
name: Net Premium daily history
description: Architecture for Options Flow 1D/7D/30D delta fields on sector/theme/ticker nodes.
---

## Rule
`options_net_premium_daily` stores one row per (entity_type, entity_id, snapshot_date).
Intraday re-runs use ON CONFLICT DO UPDATE — only today's row is ever updated.
Historical dates are never modified.

## Snapshot eligibility
- Tickers: `premium_scope_id == "net_flow_single_expiry_7_60dte_v1"` AND `nf_snapshot_pending != True`
- Unusual-flow-only tickers (`unusual_flow_7_45dte_2exp_5k_v1`) are never persisted
- Theme/sector aggregates: use existing computed `net_premium` from `_rollup_ticker_nodes`, scope="aggregate"
- entity_type: "stock" | "etf" | "theme" | "sub_theme" | "sector"

## Hook points
- `get_sector_flow` in `options_flow_sectors.py`: inject after `result["diagnostics"] = {...}`, before `result["_from_sectors_cache"] = False`
- `get_theme_flow` in `options_flow_sectors.py`: inject after `result = build_theme_tree(...)`, before `result["_from_themes_cache"] = False`
- Both hooks: collect entities → deduplicate → single bulk DB query (35-day window) → inject deltas → upsert snapshots
- Enriched result is cached so cache hits (5-min sectors / 1-min themes) return delta fields with zero DB round-trips

## Delta fields injected
`net_premium_1d_ago`, `net_premium_7d_ago`, `net_premium_30d_ago`,
`net_premium_delta_1d`, `net_premium_delta_7d`, `net_premium_delta_30d`,
`net_premium_trend_1d`, `net_premium_trend_7d`, `net_premium_trend_30d`

## Trend label values
`more_positive`, `less_positive`, `more_negative`, `less_negative`,
`crossed_positive`, `crossed_negative`, `unchanged`, null

## 1D lookback
Finds the latest `snapshot_date < today` (skips weekends/holidays automatically).
7D/30D: finds latest row `<= today - N calendar days`.

**Why:** Using `< today` for 1D avoids today's own row being the "historical" reference,
correctly handles market closures without a trading-calendar dependency.

## Retention
90 calendar days — registered in `data_retention_rules` via `init_tables`.
Index: `idx_onpd_entity_date (entity_type, entity_id, snapshot_date DESC)` for bulk range queries.
