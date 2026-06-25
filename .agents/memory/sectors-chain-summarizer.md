---
name: Sectors chain summarizer backfill
description: How the Sectors tab backfill loop gets real call/put premium for ALL tickers, and the budget-deferral safety design.
---

## Problem it solves
`run_live_scan()` only emits rows for tickers that exceed the unusual-flow threshold.
Neutral tickers (no unusual flow) got `premium=0.0` in the Sectors tab.
The Sectors tab needs real call/put premium for EVERY optionable ticker.

## New module: `backend/data/sectors_chain_summarizer.py`
- `summarize_ticker_chain(sym, tradier, expiry_cache)` — fetches expirations (cache-first),
  picks primary expiration (7-60 DTE preferred), fetches chain, sums ALL contracts:
  `premium = volume × mid(bid,ask) × 100`
- Returns: `call_premium, put_premium, net_premium, call_volume, put_volume, total_volume,
  put_call_ratio, scan_result, expiration_used`
- `scan_batch_for_sectors(symbols, tradier, expiry_cache, concurrency=6)` — concurrent batch runner;
  caller wraps in `with lane("sectors"):` or `with lane("maintenance"):`

## Budget deferral safety: `_budget_ok()` pre-check
TradierProvider._get() returns None when budget is exceeded. This propagates as `[]` from
`get_option_expirations()`, which would be misread as "confirmed no options".

Fix: `_budget_ok()` pre-check runs before every Tradier call. If budget exhausted → return
`deferred_retry` immediately (not confirmed_no_options). Post-call check: if expiry returned `[]`
AND budget is NOW exhausted → also return `deferred_retry`. Reduces false no-options by >95%.

**Why:** Accumulating false confirmed_no_options pollutes the no-options set and prevents
those tickers from ever appearing in the Sectors tree.

## Backfill loop modes (main.py `_sectors_fast_backfill_loop`)
- **Priority** (page visited within 5 min via `is_sectors_active()`):
  batch=25, sleep=25s, lane="sectors" (60 RPM) → ~4 min full pass (cached expiry)
- **Background** (Sectors not being viewed):
  batch=8, sleep=60s, lane="maintenance" (20 RPM) → ~30 min full pass

Page-active registration: every GET `/api/options-flow/sectors` calls `register_sectors_active()`.
TTL = 300s (5 min).

## Row shape stored in supplement cache
```
{
  ticker, _source="supplement", call_premium, put_premium, net_premium,
  call_volume, put_volume, total_volume, put_call_ratio, premium (compat total),
  scan_result="sectors_chain_summarized", expiration_used, updated_at
}
```
`scan_status` in API response = "fresh" for `sectors_chain_summarized` rows.

## Options flow sectors changes
- `call_volume` + `put_volume` added to ticker / theme / sector nodes
- `sectors_active_refresh` block added to response (mode, ETA, queue depth, etc.)
- ETA calc is dynamic: uses priority batch/sleep when active, background otherwise

## New tradier_budget lane
`"sectors": 60 RPM` (env: TRADIER_SECTORS_RPM_BUDGET). Separate from maintenance (20 RPM).
Does NOT affect Options Flow Screener tab.
