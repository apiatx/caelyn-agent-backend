---
name: Watchlist skeleton theme path
description: Skeleton fallback in _enrich_store_with_quotes must apply theme_ticker_mapper — the block before line 940 returns early.
---

The `_enrich_store_with_quotes` function has two paths:
- **Normal path** (has analysis sections): iterates existing LLM section rows, enriches them. Theme reclassification at ~line 940 handles "Other / Uncategorized" rows.
- **Skeleton fallback** (no sections): builds synthetic rows from raw ticker list, returns immediately — NEVER reaches the reclassification block.

**Why this matters:** Pure CSV uploads (no LLM analysis run) always use skeleton mode. Before the fix, every skeleton row had `theme=None` hardcoded.

**Fix applied (2026-07-02):** Skeleton loop now calls `map_ticker_to_primary_theme(sym)` + `map_ticker_to_theme_id(sym)` before building each row, with `map_industry_to_theme(csv_industry)` as fallback. 334/370 tickers covered for the new watchlist.

**How to apply:** Any future change that adds fields to ticker rows in the normal path must check if the skeleton path also needs the same treatment — the two paths are parallel and neither calls the other.
