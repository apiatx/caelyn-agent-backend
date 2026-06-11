---
name: Screener Hub thematic FMP refresh
description: Key decisions and constraints for the thematic screener's background refresh pipeline
---

## Rule
`_background_refresh_theme` must be called with `with_fmp_screener=True`. Without it,
`screener_meta_by_symbol` is empty in every snapshot → blank rows for all themes.

**Why:** The build-universe path has two modes: with FMP screener (enriches symbols with
sector/mcap/industry/exchange from FMP `/stable/stock-screener`) and without (ETF-holdings
only, no metadata). The warm_job scheduler was calling without=True, producing 60 hollow
snapshots. Fixed by always using with_fmp_screener=True in background refresh.

## Weak-cache trigger
When a snapshot is technically "fresh" (age < _THEME_REFRESH_STALE_H) but has
`low_result_quality=True` and the per-theme refresh cap hasn't fired in the last
_THEME_REFRESH_CAP_H, fire `asyncio.create_task(_background_refresh_theme(theme, reason="weak_cache"))`.
This ensures weak themes self-heal on first user click without waiting for the stale timer.

## 24h cap
`_THEME_REFRESH_STALE_H = 24` and `_THEME_REFRESH_CAP_H = 24`. In-memory log resets on
restart, so the first request to any theme post-restart always fires a refresh.
Cap is per-theme, tracked in `_theme_last_refresh_ts: dict[str, datetime]`.

## Filter policy for null values
- mcap filter active → null mcap rows excluded (not "under the limit")
- volume filter active → null volume rows excluded (same logic, added Jun 2026)
- exchange filter → always pass-through (unknown exchange ≠ wrong exchange)

**How to apply:** When adding a new numeric filter, mirror the mcap/volume pattern:
check if filter is active, exclude None values, increment a `rows_excluded_missing_X`
counter, and document the policy in `filter_policy` response dict.
