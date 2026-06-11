---
name: Screener Hub thematic FMP refresh
description: Key decisions and constraints for the thematic screener's background refresh pipeline
---

## Rule: with_fmp_screener=True required
`_background_refresh_theme` must use `with_fmp_screener=True`. Without it,
`screener_meta_by_symbol` is empty → blank rows for all themes.

**Why:** The build-universe path has two modes: with FMP screener (enriches symbols with
sector/mcap/industry/exchange) and without (ETF-holdings only, no metadata). The warm_job
was calling without=True, producing 60 hollow snapshots.

## Durable 24h refresh cap
The cap MUST check both in-memory log AND the DB on post-restart cache miss:
- `_THEME_REFRESH_LOG` (in-memory dict) is the fast path
- On miss: `get_theme_last_refresh_ts(theme_key)` queries `MAX(generated_at)` from
  `screener_universe_snapshots` — if < 24h, hydrates the in-memory log and returns False
- Without DB fallback: every restart lets all 60 themes hammer FMP simultaneously

## In-flight dedup + concurrency guard
- `_THEME_REFRESH_INFLIGHT: set[str]` tracks active background tasks
- Before spawning: check `theme in _THEME_REFRESH_INFLIGHT` (dedup) AND
  `len(_THEME_REFRESH_INFLIGHT) >= 3` (concurrency cap)
- `_background_refresh_theme` uses `asyncio.Semaphore(3)` + finally discard
- Same 3 guards apply to both stale check AND weak_cache trigger
- Semaphore is lazily initialized via `_get_fmp_refresh_sem()` (must be in event loop)

## Filter policy for null values
- mcap filter active → null mcap rows excluded
- volume filter active → null volume rows excluded (added Jun 2026)
- exchange filter → always pass-through

## Default theme daily refresh
- Daily default theme (no theme param) goes through the same stale check pipeline
- `default_last_refreshed_at` = `snap_generated_at` (when universe was built)
- `next_default_refresh_at` = `snap_generated_at + STALE_H`
- Restart-safe via DB fallback cap

## Response fields added (Jun 2026)
selected_theme_label, fmp_refresh_used/reason, row_source_breakdown,
unknown_volume_policy, unknown_exchange_policy, rows_excluded_missing_volume,
default_last_refreshed_at, next_default_refresh_at,
theme_refresh_inflight_count, theme_refresh_concurrency_limit,
theme_next_refresh_allowed_at (always populated via post-hydration)
