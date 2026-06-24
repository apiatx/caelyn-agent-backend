---
name: Supplement loop anti-duplication
description: How the supplement loop avoids duplicate Tradier calls for watchlist-overlap symbols that the Watchlist scanner already owns.
---

## The problem
The supplement loop's inflight guard only blocks CONCURRENT duplicate scans. Sequential duplicates (watchlist scan completes, releases claim, then supplement loop picks up the same symbol) were not blocked.

`get_theme_only_symbols_for_supplement()` returns all theme symbols NOT in master screener — this INCLUDES watchlist-overlap symbols. Without a cache check, the supplement loop would re-scan every overlap symbol every 5-minute cycle.

## The fix (main.py _theme_options_supplement_loop)
After the inflight guard claim, before `get_quotes(batch)`:
1. Load all watchlist symbols via `_load_all_watchlist_symbols()`
2. Split batch into `_overlap_in_batch` and `_theme_only_in_batch`
3. For each overlap symbol, check `cache.get(_per_ticker_cache_key(sym))`
4. If `data_available=True`: add to `_overlap_cache_hits` → skip live scan
5. If no fresh cache (gap-fill case): add to `_overlap_needs_scan` → proceed with live scan
6. Rebuild batch = theme_only + overlap_needs_scan

## Policy
- Supplement SKIPS live scan for overlap symbols with a fresh per-ticker cache row (data_available=True)
- Supplement CAN gap-fill overlap symbols when the Watchlist scanner hasn't gotten to them yet
- Supplement always live-scans theme-only symbols (they have no other owner)

## Diagnostics
`_SUPP_DIAG` in `options_theme_supplement.py` tracks:
- `supplement_overlap_cache_hits` — overlap syms skipped (lifetime)
- `supplement_overlap_live_scans` — overlap gap-fills (lifetime)
- `supplement_only_live_scans` — theme-only live scans (lifetime)
- `supplement_duplicate_scans_blocked` — inflight guard blocks (lifetime)
Exposed via `/api/rate-status` as `supplement_loop` block.

## Validated result
supplement_overlap_live_scans = 0 (12 overlap hits all came from cache)
supplement_only_live_scans = 8 (theme-only symbols proceed normally)
