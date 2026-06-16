---
name: Screener Hub query cache after admin rebuild
description: Admin rebuild must expire the query cache after writing the universe snapshot or stale cached responses (built against the old smaller snapshot) continue to be served.
---

## Rule
After `insert_universe_snapshot()` in `rebuild_universe()`, always call `expire_theme_query_cache(theme_key)`. Without this, the query cache will serve old hydrated responses (e.g. 4 rows) even though the snapshot now has 28 symbols.

**Why:** The query cache stores fully-hydrated API responses keyed by theme + filter combination. It is checked BEFORE the snapshot is read. If it hits, the snapshot read is skipped entirely. The background refresh path already called expire_theme_query_cache (line ~497) but rebuild_universe did not — fixed at lines 4571-4574.

**How to apply:**
- Both call sites are now in screener_hub_service.py: background refresh (~497) and rebuild_universe (~4571).
- If a rebuild appears to have no effect on the API response, expire the query cache manually: `expire_theme_query_cache('theme_key')` via a short Python script against the live DB connection.
