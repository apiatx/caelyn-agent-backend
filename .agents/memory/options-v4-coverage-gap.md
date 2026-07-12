---
name: V4 Options Coverage Gap Architecture
description: How not_scanned symbols are classified in V4, what causes gaps, and the multi-layer fix applied to reduce 86→29 not_scanned.
---

## Root Cause of Coverage Gaps

V4 `_score_options_alignment` reads `options_alignment_score` and `options_alignment_available` from the retained snapshot row, which is built by `compute_options_alignment()` reading from `get_combined_ticker_data()`.

Three sources of not_scanned:
1. **Foreign-exchange tickers** (AIM:, ASX:, LON:, etc.) — 57 symbols, never in supplement universe
2. **confirmed_no_options known from sectors LKG** — 11 tickers (CODA, OCC, SLNH, SVCO, TRT, TYGO + 5 non-watchlist), only in sectors_lkg not supplement_lkg
3. **Missing from supplement universe** — 13 US watchlist tickers that were never added to the theme-universe scanner (ACOG, ADTN, APH, DVLT, INKT, LIDR, NVEC, NXXT, OPTX, PBLS, SILC, XTIA + AMS:BESI)

A 4th category also exists: tickers IN supplement LKG but with `alignment_score=None` (INSUFFICIENT_HISTORY or `optionable_pending_chain`) — these resolve after scan cycles.

## Fixes Applied

### `caelyn_confluence_v4.py` — `_score_options_alignment()`
- Added foreign-exchange prefix check at start of function: any ticker starting with AIM:, ASX:, CSE:, EPA:, ETR:, FRA:, KRX:, LON:, OSL:, SHA:, STO:, SWX:, TPE:, TPEX:, TSX:, TSXV:, TYO:, WSE:, XSAT:, OTC: → returns `confirmed_no_options` immediately
- Added `stale_but_usable` status mapping in `_snap_status` handling
- Added `stale_but_usable` to `_KNOWN_STATE_STATUSES`

### `data/options_theme_supplement.py`
- **`_CONFLUENCE_EXTRA_SYMBOLS`** module-level set + `set_confluence_extra_symbols()` — populated at startup with US watchlist tickers
- **`get_theme_only_symbols_for_supplement()`** — now includes `_CONFLUENCE_EXTRA_SYMBOLS` so supplement scanner covers full watchlist universe
- **`_seed_no_options_from_sectors_lkg()`** — called at startup; injects confirmed_no_options from sectors LKG disk into in-memory no-options tracking cache using 7-day age limit (not 96h)
- **`get_combined_ticker_data()`** — added unconditional Layer 5 that reads sectors LKG confirmed_no_options entries (7-day limit), injecting them into combined so snapshot build sees them

### `main.py` — startup
- Calls `_seed_no_options_from_sectors_lkg()` after sectors LKG load
- Calls `set_confluence_extra_symbols(all_us_watchlist_tickers)` — uses `list_watchlists()` metadata + `load_watchlist(id)` for tickers (list_watchlists returns metadata only, not tickers)

### `watchlist_router.py` — v4-report endpoint
- Added `not_scanned_symbols` and `confirmed_no_options_symbols` lists to response

## Key Architecture Notes

- `list_watchlists()` returns **metadata only** (id, name, ticker_count) — call `load_watchlist(id)` to get tickers
- `_CONFLUENCE_EXTRA_SYMBOLS`, `get_no_options_symbols()` cache are **in-memory server process only** — standalone subprocess tests always see empty sets
- `_SECTORS_LKG_CNO_MAX_AGE = 604800` (7 days) for confirmed_no_options vs `_SECTORS_LKG_DISK_MAX_AGE = 345600` (96h) for general sectors LKG

**Why:** confirmed_no_options is stable; a company rarely gains/loses listed options overnight.

## Result
- Before: `not_scanned: 86, confirmed_no_options: 0, avg_confidence: 79.5`
- After restart: `not_scanned: 29, confirmed_no_options: 57, avg_confidence: 81.7`
- After next snapshot warm: ~23 not_scanned (6 confirmed_no_options from sectors LKG resolve)
- After next market session scan: ~10 not_scanned (13 missing LKG tickers scanned)
