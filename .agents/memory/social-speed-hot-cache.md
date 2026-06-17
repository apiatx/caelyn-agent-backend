---
name: Social page speed fix — hot cache + stale-while-revalidate
description: Pattern used to reduce GET /api/social/x-dashboard from 8.5s+ to <10ms server-side.
---

## The pattern

Three-layer optimization applied to the Social page load path:

### Layer 1 — Disk snapshot hot cache (x_consensus_cache.py)
`_load_disk_cache()` and `_load_prior_cache()` now check file mtime before parsing.
Module globals: `_hot_disk_cache`, `_hot_disk_cache_mtime` (same for prior).
If `mtime == cached_mtime` → return cached dict, zero JSON parse.
Invalidation is automatic: new snapshot write → new mtime → next call re-parses.

### Layer 2 — Sections cache in build_x_dashboard() (social_x_service.py)
Module-level `_SECTIONS_CACHE: dict = {}` keyed by snapshot `_saved_at` epoch float.
- If `_saved_at` matches cached `saved_at` → return cached sections immediately.
  Only `_public_payload()` and `_build_metadata()` are recomputed (time-sensitive fields).
- On miss (snapshot changed, ~once/day): load `prior_snap` + `ticker_history`,
  run full classifier + 4 section builders, store result in `_SECTIONS_CACHE`.
- `prior_snap` and `ticker_history` are NOT loaded in the fast path.

### Layer 3 — Non-blocking screener in route (main.py)
`await build_screeners(...)` removed from the request path entirely.
- Check `cache.get("social_screener:social_payload")` and `...fs_payload` directly.
- Both warm → serve from cache, zero network calls.
- Either cold → `asyncio.create_task(build_screeners(...))`, return `enrichment_status="warming"`.
- Response metadata fields added: `cache_hit`, `cache_source`, `cache_age_seconds`,
  `xai_called` (always False), `screener_awaited` (always False),
  `background_refresh_started`, `total_ms`.

**Why:** Sections never change between XAI scans (~daily); disk JSON parse was the biggest
overhead; Tradier batch (8.5s) must never block a user-facing GET.

**How to apply:** Any endpoint that reads the same large JSON snapshot repeatedly should
use the mtime hot-cache pattern. Any async enrichment step with its own compiled cache
should be checked directly and fired as a background task if cold.
