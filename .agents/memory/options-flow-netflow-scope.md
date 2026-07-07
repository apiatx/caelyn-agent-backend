---
name: Options Flow Net Flow scope hardening
description: premium_scope_id field, canonical Net Flow breadth filtering, required-universe classification diagnostics, priority backfill queue
---

## Net Flow breadth contamination fix

`breadth_pcr` in `_rollup_ticker_nodes()` must ONLY aggregate tickers with
`premium_scope_id == "net_flow_single_expiry_7_60dte_v1"` (chain summarizer scope).
Master screener rows have `premium_scope_id == "unusual_flow_7_45dte_2exp_5k_v1"` —
their DTE window, multi-expiry contracts, and min-premium gate make P/C ratios
non-comparable. Mixing them into the same geometric mean produced a contaminated breadth ratio.

The scope_id map is in `_build_ticker_node()`:
```python
_SCOPE_ID_MAP = {
    "single_expiry_7_60dte_preferred": "net_flow_single_expiry_7_60dte_v1",
    "top_unusual_contracts":           "unusual_flow_7_45dte_2exp_5k_v1",
}
```

New rollup fields: `net_flow_breadth_pcr`, `unusual_flow_breadth_pcr`,
`net_flow_scoped_stock_tickers`, `unusual_flow_scoped_stock_tickers`,
`net_flow_missing_snapshot_stock_tickers` (stocks with only unusual-flow scope).

## False-green instrument type diagnostics fix

Old `instrument_type_classification` used `get_stats()["unknown"]` — only counted
symbols EXPLICITLY stored as "unknown" in `_MEM`. Symbols absent from the cache
were not counted even though they return "unknown" from `get_instrument_type()`.
Result: `unresolved_total=0` while sectors showed 18+ unresolved per-theme.

Fix: `get_required_universe_classification_stats(all_theme_syms)` computes against
the actual required universe (247 symbols). Correctly showed 97 unresolved after fix.

## Instrument-type source tracking

`_MEM_SOURCE: dict[str, str]` added alongside `_MEM` in `options_instrument_type_service.py`.
Sources: `fmp_is_etf`, `sector_inference`, `fmp_profile`, `lkg`, `unresolved`.
`sector_inference` = isEtf is None but FMP sector is non-empty → temporary stock classification.
Exposed per-ticker as `instrument_type_source` and `instrument_type_inferred` bool.

## Priority queue for newly required theme symbols

`_HIGH_PRIORITY_SYMBOLS: dict[str, float]` in `options_theme_supplement.py`.
`add_high_priority_symbols([ticker])` called from `watchlist_router.py` at both
`upsert_theme_ticker_override` and `bulk_upsert_theme_ticker_overrides` call sites.
`get_sectors_pending_symbols()` hoists high-priority-AND-pending symbols to front.
`clear_scanned_high_priority(batch)` called by backfill loop after each scan.

Without priority: a newly added symbol lands at alphabetical position in a 247-symbol queue
and waits up to ~30 min (background mode). With priority: reaches the next batch.

## Periodic classification loop

`_itype_classify_startup()` converted to `_itype_classify_loop()` (30s startup,
1800s repeat). Uses `get_unresolved_symbols(all_theme_syms)` to only process
symbols in the required universe that are still unresolved. Handles mid-session
new theme tickers that were absent at startup.

**Why:** Without periodic refresh, new theme tickers added after startup never
get classified (startup pass is one-shot). The loop also self-heals from FMP
API downtime at startup.
