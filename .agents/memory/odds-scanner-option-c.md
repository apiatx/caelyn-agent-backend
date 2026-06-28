---
name: Odds scanner catalog persistence Option C
description: Why catalog DB persistence was removed and what replaced it.
---

## Rule
The prediction_market_catalog table is NOT populated during scan cycles.
Only prediction_market_odds_snapshots (26 rows/cycle) is persisted to Neon.

## Why
Upserting 28k+ raw Polymarket markets to Neon via execute_values caused the
asyncio.wait_for(90s) to fire every cycle, leaving catalog_rows_upserted=0.
Option C was chosen: keep the full catalog in self._last_raw_markets (in-process
memory between cycles). If the live Gamma crawl fails mid-cycle, the previous
cycle's raw_markets are used as fallback. This is cheaper than a 29k-row DB
roundtrip and more reliable than a Neon read under load.

## How to apply
- _crawl_catalog() fetches from Gamma API and stores in self._last_raw_markets
- _crawl_catalog() returns the in-memory list on failure (no DB read)
- Diagnostics show: catalog_rows_upserted=0, catalog_persist_success=False,
  catalog_persist_error="option_c_memory_only" (this is intentional, not a bug)
- Snapshot persistence uses asyncio.wait_for(run_in_executor(insert_snapshots), 30s)
  with snapshot_persist_success, snapshot_persist_error, snapshot_persist_duration_ms fields
- If you ever need catalog DB persistence, re-add Option D (chunked upserts)
