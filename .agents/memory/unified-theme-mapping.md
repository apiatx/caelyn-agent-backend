---
name: Unified canonical theme mapping architecture
description: Two Neon stores (watchlist_category_overrides and theme_ticker_overrides) now bidirectionally synced; skeleton path priority chain; cross-sync in all write endpoints
---

## The Two Stores

| Store | Table | Written by | Read by |
|---|---|---|---|
| `watchlist_category_overrides` | user_id+ticker → category_name | `PATCH /api/watchlist/category`, `POST /categories/bulk`, `seed_initial_overrides()` | Watchlist normal path (`apply_to_sections` line ~1204), skeleton path (added), Chart Radar |
| `theme_ticker_overrides` | theme_id+symbol → add/remove | `POST /api/themes/admin/memberships`, `POST /api/themes/admin/memberships/bulk` | `theme_merge_layer._build_enriched_universe()` → `ENRICHED_THEME_RS_UNIVERSE` → Options Flow |

## Bidirectional Cross-Sync

**Watchlist → Options Flow** (`patch_category_endpoint` and `bulk_categories_endpoint`):
1. Write to `watchlist_category_overrides` (primary)
2. Call `register_llm_classified_tickers()` → mapper in-memory + `llm_theme_overrides.json`
3. Look up theme_id via `THEME_RS_UNIVERSE` display_name match
4. If valid theme_id found: `upsert_theme_ticker_override(action="add")` → `refresh_enriched_universe()` → `invalidate_sectors_cache()`

**Themes page → Watchlist** (`admin_upsert_membership` and `admin_bulk_memberships`):
1. Write to `theme_ticker_overrides` (primary)
2. On action="add": `upsert_override("default", symbol, display_name, source="themes_page_manual")` → `watchlist_category_overrides`
3. Call `register_llm_classified_tickers()` → mapper in-memory index

## Skeleton Path Priority Chain (new watchlist, no sections yet)

Loaded once before the per-ticker loop:
1. `category_overrides.get_overrides("default")` — manual always wins
2. `ENRICHED_THEME_RS_UNIVERSE` proxy_symbols — Themes-page membership (keyed sym→display_name)
3. `map_ticker_to_primary_theme()` — static canonical map
4. `map_industry_to_theme(csv_ind)` — CSV industry fallback

Resolution per ticker:
- `manual_override` > `themes_page_membership` > `canonical_map` > `industry_fallback` > `no_mapping`

## FMP-Industry Classifier Persistence

`watchlist_theme_classifier.py` now calls `register_llm_classified_tickers()` with `confidence="fmp_industry"` for FMP-industry-matched tickers so they appear in provenance and status with `source=llm_classified`.

## DB Function Names

- Load watchlist by ID: `data.pg_storage.watchlist_read(watchlist_id)` (NOT `load_watchlist_store`)
- Write category override: `data.pg_storage.upsert_category_override(user_id, ticker, category, source, reason)`
- Write theme override: `data.pg_storage.upsert_theme_ticker_override(theme_id, symbol, action, source, note, created_by)`

## Provenance Function

`get_theme_provenance(sym)` in `watchlist_theme_classifier.py`:
- Fixed: was calling `get_override(sym)` (doesn't exist) → now `get_overrides("default").get(sym)`
- Shows: final_theme, source, neon_override, themes_page_theme, llm_override, needs_review

## Why

The original design used two isolated stores: Watchlist reads `watchlist_category_overrides` (by display name), Options Flow reads `theme_ticker_overrides` (by theme_id+symbol). Themes page wrote only to the latter; Watchlist category edits wrote only to the former. The skeleton path for new watchlists never read either. This caused tickers manually assigned on the Themes page to show Unassigned on the Watchlist.

**How to apply**: Any new write endpoint that changes a ticker's theme assignment must cross-sync to BOTH stores. Use `ENRICHED_THEME_RS_UNIVERSE.get(theme_id, {}).get("display_name")` to convert theme_id → display_name for the category_overrides side.
