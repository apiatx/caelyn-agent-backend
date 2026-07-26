---
name: Watchlist options multi-source join
description: How options data layers are joined in scan_watchlist_options for the watchlist options-signals endpoint
---

# Watchlist options multi-source join

## The problem (root cause)
Two LKG layers each carried half the data — neither was joined:

| Layer | Has | Missing |
|---|---|---|
| Supplement LKG (794 tickers, `get_combined_ticker_data()`) | call/put/net premium, interval classification, call/put/total volume | score, IV, OI, signal, vol P/C |
| Portfolio disk LKG (238 tickers, `_load_portfolio_lkg()`) | score, signal, IV, OI, vol P/C (p_c field), call/put volumes | call_premium, put_premium, net_premium (all None) |
| Master screener snap (warm only) | everything | absent on cold restart |

`scan_watchlist_options()` only read primary row then normalized — never joined across layers.

## Fix architecture
`_merge_options_sources(sym, primary_row, supplement_row, lkg_row, history_deltas, ...)`:
- Field precedence: master → supplement/LKG per field category
- Score/signal/IV/OI: master_row → lkg_row
- Premium ($): master_row → supplement_row (LKG has None for all premium fields)
- Volume P/C: master.pc_ratio → lkg.p_c (same scan scope) — never derive from premium
- Volume counts: master → LKG → supplement (scope quality order)
- History 1D/7D/30D: exclusively from DB via `get_historical_snapshots_bulk`
- Uses `_first_non_null()` (preserves real 0) not falsy `or` chains

## Supplement row gotcha
`get_combined_ticker_data()` rows do NOT set `data_available=True`. They must be accepted
when `call_premium is not None or net_premium is not None or call_volume is not None`.
Both `_classify_row_family()` and `_merge_options_sources()` guard for this.

## Key helpers added
- `_first_non_null(*values)` — zero-safe first non-None
- `_si_n(v)` — nullable safe-int (None → None, not 0)
- `_classify_row_family(row)` → master_scored / premium_summary / portfolio_lkg / unavailable
- `_build_snapshot_status(row, is_stale, market_hours)` → never "unknown"
- `_merge_options_sources(...)` → canonical merged row for `_normalize_to_watchlist_row`

## History batch query
Added in step 2.6 of `scan_watchlist_options()` — one Neon round-trip:
- `get_historical_snapshots_bulk([("stock", s), ("etf", s) for all US syms], since=31d ago)`
- `compute_delta_fields(curr_net_prem, rows, today)` per ticker
- curr_net_prem sourced from supplement row → disk_lkg row (supplement more reliable)

**Why:** `_find_historical_np` needs a current value to compute deltas; supplement has the real dollar premiums from chain summarizer scope.

## Output fields added to _normalize_to_watchlist_row
- `snapshot_status` (lifecycle, replaces `scan_status="unknown"`)
- `options_score_status`, `options_score_unavailable_reason`, `master_score_row_present`
- `options_volume_scope`, `options_volume_method`
- `net_premium_history_status_{1d,7d,30d}`, `net_premium_trend_*`, `net_premium_*d_ago`
