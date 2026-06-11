---
name: Screener Hub inline refresh for explicit selected themes
description: Explicit user-selected themes use inline (await) refresh; default theme uses background (create_task) refresh
---

## Rule
When `not is_default_theme` (user explicitly selected a theme) and the snapshot is stale OR has empty `screener_meta_by_symbol`, the refresh runs **inline** (`await asyncio.wait_for(_background_refresh_theme(...), timeout=12s)`). The refreshed snapshot is reloaded before rows are built. Default theme keeps background-only refresh.

**Why:** Background refresh (create_task) returns stale/weak shells immediately and schedules the refresh for the next request. For explicit selected themes this is unacceptable — the user expects their selected theme to actually work, not show 0 rows.

**How to apply:** The same three guards apply to inline refresh: in-flight dedup, concurrency cap ≤3, 24h durable cap. If guards block, set `theme_refresh_status = "stale_cap_active"` or `"refreshing"` / `"refresh_queued"`. After inline refresh, reload snapshot and update `symbols` + `thematic_breakdown` before the row-build loop.

**Weak snapshot detection:** `not bool(thematic_breakdown.get("screener_meta_by_symbol"))` → empty screener_meta means ETF/static-only path ran without FMP screener.
