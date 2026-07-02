---
name: Watchlist upload stage2 trigger
description: save_watchlist() is sync and cannot call async warmup; the async save_endpoint POST handler is the correct place to fire background stage2 warmup.
---

`save_watchlist()` in watchlist_service.py is a **synchronous** function. `warmup_stage2()` in watchlist_stage2_service.py is **async**. Cannot call async from sync without an event loop.

**Fix (2026-07-02):** `save_endpoint` (POST /save, watchlist_router.py) now fires a background task after calling `save_watchlist`:
1. Loads the just-saved watchlist to get its tickers
2. Filters to eligible symbols (is_fmp_symbol_eligible)
3. Identifies symbols missing from `_STAGE2_LKG` or with null labels
4. Fires `asyncio.create_task(_run_upload_warmup())` — non-blocking
5. State tracked in `_UPLOAD_WARMUP_STATE` module dict, exposed via `/debug/technical/status`

**Why:** New watchlist uploads previously had no auto-queue for stage2 — warmup ran only on server startup (covering all watchlists). If the new watchlist was uploaded after startup, new-only symbols were not computed until the next restart.

**How to apply:** Any future hook that needs async work after a sync save should be placed in the async endpoint handler, not in the sync service function. Use `asyncio.create_task()` for fire-and-forget.
