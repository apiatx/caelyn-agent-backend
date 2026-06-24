---
name: Options cache bridge watchlist→Sectors
description: How the shared per-ticker options cache (portfolio_opts:{sym}) is bridged into the Sectors aggregation layer.
---

## The bridge
`get_combined_ticker_data()` in `options_theme_supplement.py` has a 4th layer after
master→supplement_fresh→supplement_lkg that reads `portfolio_opts:{sym}` (the per-ticker
cache written by the Watchlist and Portfolio Terminal scanners) for any theme universe
symbol not yet covered by the first three layers.

Rows found this way are tagged `_source: "watchlist_cache"` and counted in
`symbols_from_watchlist_cache` in the Sectors diagnostics.

## Steady-state behavior
Once the supplement background loop catches up (covers overlap symbols), they show as
`_source: "supplement"` and the bridge contribution drops to 0.  This is correct —
bridge is the cold-window fallback, supplement is the steady-state path.

## Progressive deferred drain
`_drain_deferred_watchlist()` in `portfolio_options_service.py` runs as a background
`create_task` after the initial max_live_scan=50 batch.  Scans remaining deferred
symbols in _MAX_SYMBOLS=25 batches with 35s inter-batch sleep.  Ensures deferred_symbols_count
eventually reaches 0 without requiring repeat page loads.

**Why:** Without the drain, 102+ symbols were permanently `scan_pending` because the
endpoint only enqueued 50 per call and the user never called again.

## Cache TTL policy for unavailable rows
- `not_in_tradier_coverage`, `no_options`, `no_expirations`, `otc_or_foreign_unsupported`: 24h TTL
- Transient (rate_limited, timeout, network): 30min TTL
- Fresh optionable data: 5min TTL (_CACHE_PER_TICKER_TTL)
