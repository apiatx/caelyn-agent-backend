---
name: Canonical Theme Taxonomy v2
description: Registry structure, key ID decisions, deprecated nodes, and cross-file integration points for taxonomy v2.
---

## Registry (services/theme_rs_universe.py)

- 112 total nodes: 11 sector / 23 theme / 67 sub_theme / 3 market_lens / 8 deprecated
- 90 assignable (theme + sub_theme only)
- `validate_theme_hierarchy` = alias for `validate_registry` (backward-compat for test imports)
- `normalize_company_sector_to_id(label)` maps FMP/Tradier sector strings to canonical sector IDs

## Key ID decisions

- `dc_connectivity_silicon` — primary replacement for deprecated `ai_networking`; sub_theme under `semiconductors`
- `gold`, `silver`, `copper_miners` — market_lens (not assignable), but carry `parent_theme_id = "metals_mining"` for rollup context
- Deprecated nodes retain `parent_theme_id` for backward-compat rollup: `semicap_equipment → semiconductors`, `substrates_packaging → semiconductors`
- `defense` rollup: `["industrials"]` only — removing "technology" was intentional
- `datacenter_infra` rollup: `["technology", "utilities", "real_estate"]` — removing "industrials" was intentional
- `memory_storage` — no explicit `rollup_sector_ids`; inherits via parent chain

## Dropped v2 fields

`sector_tags` and `macro_sensitivities` were dropped from all nodes. Tests must not require them.

## SOL 5.6 finding

SOL 5.6 model not found anywhere in the codebase (no SOL56_MODEL_ID env var, no sol model in config). Anthropic `claude-haiku-4-5-20251001` is the fallback for theme_taxonomy_classifier.py.

## Integration points

- `theme_merge_layer._SECTION_TO_THEME_ID` — 63 entries, maps section names to theme IDs
- `theme_merge_layer._CATEGORY_TO_THEME_ID` — 73 entries, maps category names to theme IDs
- `theme_rs_service._validate_basket_hashes` — now repairs classification, parent_sector, assignable in all 3 branches via `_taxonomy_patch` dict
- `routes/themes.py` — `PUT /api/themes/admin/ticker-taxonomy/{ticker}` atomic multi-membership endpoint
- `services/theme_taxonomy_classifier.py` — dry-run AI classification engine; `DRY_RUN_APPLY=False` always

**Why:** Phase 4 spec requirement; ensures every LKG row has current taxonomy fields regardless of when the row was written.
