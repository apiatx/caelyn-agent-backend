---
name: Sectors fast backfill loop
description: How the Sectors tab achieves <30 min coverage backfill via _sectors_fast_backfill_loop() in main.py and Sectors LKG persistence in options_theme_supplement.py
---

# Rule
`_sectors_fast_backfill_loop()` runs at startup in main.py as `asyncio.create_task`. It uses `get_sectors_pending_symbols()` to find both `generic_pending` (no cache) and `stale_lkg` (loaded from disk LKG as supplement_lkg) symbols, batches 8 at a time, sleeps 60 s between batches on the **maintenance** lane → ~29 min for 232 symbols.

# Sectors LKG persistence
`save_sectors_universe_lkg_to_disk()` snapshots all theme universe symbols (including live/master rows) as `_source:"supplement"` to `backend/data/options_sectors_universe_lkg_v1.json`. On next restart, `load_sectors_universe_lkg_from_disk()` injects them into the supplement cache tagged as `supplement_lkg` so `_ticker_state()` marks them `stale_lkg` (counted as **represented** in coverage_pct immediately).

# Why stale_lkg counts as represented
`_ticker_state()` priority: source check before explicit-state checks. `supplement_lkg` → `stale_lkg` which IS included in `_represented_syms`. So after first full pass + LKG save, every restart starts at ~100% coverage_pct before any new scans.

# Budget math
Maintenance lane: 20 RPM. Batch of 8 costs: 1 quote + 8 expiry + 8 chain = 17 calls → fits under budget. Sleep 60 s = 1 RPM per slot effectively safe.

# Supplement loop cadence
Reduced from 300 s → 120 s (main.py line ~12157) to keep stale rows refreshed faster between backfill passes.
