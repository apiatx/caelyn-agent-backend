---
name: Odds scanner LKG cold-start
description: 3-tier fallback for /api/predict/odds/live so it never returns "warming" when data exists
---

# Odds Scanner LKG — cold-start fallback

## Rule
`get_live()` uses a 3-tier fallback:
1. `_mem_cache.get(_LIVE_CACHE_KEY)` → `status:"ok"` (normal post-scan path)
2. `_load_lkg()` → `status:"lkg"` — reads `backend/data/predict_odds_live_lkg.json`, max_age=72h
3. `_hydrate_from_db_snapshots()` → `status:"stale_db"` — calls `_db_get_latest_per_family()` from `data/predict_odds_store.py`
4. Warming stub → `status:"warming"` — only when all 3 are unavailable

## Why
After restart the in-memory cache is empty, the 10-min scan cycle hadn't run yet,
and the endpoint was stuck returning "warming" for 10+ minutes — unacceptable UX.

## How to apply
- `_save_lkg(payload)` is called after `_mem_cache.set(...)` in step 11b of `_do_scan()`
- Uses atomic temp-file + os.replace to prevent partial reads
- `_scanner_running` / `_last_scan_error` / `_last_successful_scan_at` tracked in `scan_and_persist()`
- Diagnostics block always includes `odds_live_source`, `lkg_file_exists`, `lkg_loaded`, `db_snapshot_fallback_loaded`
- `get_latest_per_family()` already existed in `predict_odds_store.py` — no schema changes needed
