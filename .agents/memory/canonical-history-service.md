---
name: Canonical 5-year price history service
description: Architecture and gotchas for the disk-persistent canonical bar cache (V4.2.5.2).
---

## Rule
`canonical_history_service.py` is the single source of truth for long-form bar data.
`_fetch_bars` probe order: (0) canonical disk → (1) in-memory `fmp_hist:{SYM}` → (2) in-memory `tdier_hist:{SYM}:400` → (3) live FMP → (4) live Tradier 400d.

**Why:** `FMP_BLOCK_FULL_HISTORICAL=true` is set in env; without canonical cache all analysis falls back to 400-bar Tradier window which breaks Fib weekly/monthly and long-term stage detection.

## V4.2.5.3 changes (scheduler-safe Tradier primary)
- `canonical_history_backfill` lane added to `tradier_budget.py` (default 5 RPM, env `TRADIER_CANON_HIST_RPM_BUDGET`)
- All Tradier history calls now route through `TradierProvider.get_history()` → `_get()` → `TRADIER_LIMITER` + lane budget. **Zero raw httpx calls.**
- `_fetch_tradier_daily_history` from `theme_rs_service.py` (tagged `[TRADIER_UNMANAGED]`) is NO LONGER used by backfill.
- Tradier is now PRIMARY; FMP is fallback only (when Tradier returns <504 bars).
- Session-aware throttling: active-session gate (5 RPM hard cap) + options-flow-busy soft gate (skip if >80% saturated); off-hours shortens sleep to approach `TRADIER_CANON_HIST_OFFHOURS_MAX_RPM` (default 15).
- `canonical_history_backfill` lane visible in `/api/rate-status` `budget_by_lane`.

## Key design points

- Storage: `backend/data/canonical_history/{SYM}.json.gz` per symbol; `_index.json` metadata index (no bars inline).
- `preload_index()` is called **synchronously** at startup before the Stage2 warmup task fires — so the index is always populated before any `_fetch_bars` call.
- `get_bars()` has a lazy-load guard: if `_INDEX` is empty it calls `preload_index()` (guards subprocess / deferred startup ordering).
- `_write_index()` always **merges** with existing disk content (`{**disk_index, **_INDEX}`) — in-memory wins on conflict. Prevents concurrent subprocess writes from clobbering live-server backfill entries.
- `preload_index()` prunes orphaned index entries (no matching `.json.gz` on disk) and recovers `.json.gz` files not in the index, then rewrites the cleaned index.
- Backfill bypasses `FMP_BLOCK_FULL_HISTORICAL` (admin-explicit). FMP primary → Tradier 1825-day fallback. Tradier extended window returns ~1254 bars (≈ 5y).

## Depth thresholds (bar count → confidence)
- ≥ 1300 → 1.00  (FMP 1900d window; typically unavailable due to FMP_BLOCK)
- 1100–1299 → 0.85  (Tradier 1825d window, ~1254 bars in practice)
- 756–1099 → 0.70
- 504–755  → 0.50  (Tradier 400d window ≈ 274–400 bars falls here or below)
- < 252    → 0.25

## History status tags
`available_5y` ≥1100 bars; `available_3y` ≥700; `partial_history` ≥504; `intermediate_only` ≥252; `recent_only` ≥40; `actual_ticker_history_limit`; `insufficient_history`; `fetch_failed`; `not_yet_backfilled`.

## New metadata fields (V4.2.5.3)
- `canonical_history_provider` — same as `provider`
- `canonical_history_provider_rank` — 1=Tradier (primary), 2=FMP
- `canonical_history_quality` — `full_history_tradier_verified/unverified`, `full_5y_fmp`, `partial_tradier`, `partial_fmp`, `actual_ticker_history_limit`, `stage_cache_fallback`, `provider_failed`
- `canonical_history_adjusted_status` — always `"unknown"` (Tradier docs don't specify; conservative)
- `canonical_history_refresh_mode` — `initial_full_backfill`, `incremental_daily_append`, `manual_rebuild`
- `append_bars(symbol, new_bars, provider)` — incremental daily merge by date, dedup, saves with `incremental_daily_append` mode

## Tradier capability (confirmed 2026-07-13)
- 10/12 test symbols: TRADIER_FULL_HISTORY_OK (1253 bars, 5.0Y)
- CRDO: 1117 bars (4.4Y) — IPO Jan 2022
- WYFI: 233 bars — ACTUAL_TICKER_HISTORY_LIMIT (IPO Aug 2025)

## New diagnostic fields
- Stage2 LKG: `stage_bar_count`, `stage_years_available`, `stage_history_status`, `stage_history_source`, `stage_long_history_used`, `stage_data_depth_confidence`, `stage_data_limitation_reason`.
- Entry State: `entry_bar_count`, `entry_years_available`, `entry_history_status`, `entry_long_history_used`, `entry_data_depth_confidence`, `entry_data_limitation_reason`.
- Fib: `fib_history_status`, `fib_history_source`, `fib_long_history_used`, `fib_data_depth_confidence`, `fib_data_limitation_reason`, `fib_multi_year_available`, `fib_weekly_available`, `fib_monthly_available`, `fib_timeframe_scope`.

## Admin endpoints
- `POST /api/admin/canonical-history/backfill?max_syms=N&delay_s=0.5&priority_only=false&symbols_csv=SPY,AAPL`
- `GET  /api/admin/canonical-history/status`

## How to apply
Any new analysis engine that needs long-form bars should call `_fetch_bars(sym)` (from `watchlist_stage2_service`) which routes through canonical cache first.  Run the admin backfill endpoint to populate the cache for any symbol set; Tradier extended window is a reliable 5y source even with FMP blocked.
