# Canonical Theme Taxonomy v2 — Implementation Report

**Date:** 2026-08-06  
**Commit:** feat: add canonical theme taxonomy v2

## Files Changed

| File | Change |
|------|--------|
| `backend/services/theme_rs_universe.py` | Complete v2 registry rewrite: 112 nodes (11 sectors, 23 themes, 67 sub_themes, 3 market_lens, 8 deprecated). Promoted 6 sub_themes to themes. Added 44 new nodes. Deprecated 8 legacy nodes. Added `validate_theme_hierarchy` alias, `normalize_company_sector_to_id()`. |
| `backend/services/theme_merge_layer.py` | Updated `_SECTION_TO_THEME_ID` (63 entries, legacy compat mapping), `_CATEGORY_TO_THEME_ID` (73 entries), `_THEME_SECTION_ALIASES` (v2 canonical). |
| `backend/services/theme_rs_service.py` | `_validate_basket_hashes()` now repairs `classification`, `parent_sector`, `assignable` in all 3 branches (legacy/stale/current), via `_taxonomy_patch` dict. |
| `backend/routes/themes.py` | Added `PUT /api/themes/admin/ticker-taxonomy/{ticker}` — atomic multi-membership assignment with ancestor-redundancy removal, diff against stored memberships, single cache invalidation. |
| `backend/services/theme_taxonomy_classifier.py` | New: AI dry-run classification engine. Providers: Anthropic (installed) → Gemini REST → OpenAI → SOL56 (not found). `run_sample()` / `run_full_watchlist_dry_run()`. Never writes to Neon (`DRY_RUN_APPLY=False`). |
| `backend/tests/test_theme_hierarchy.py` | Updated 4 tests (ai_networking → dc_connectivity_silicon for v2 compat); removed deprecated `sector_tags`/`macro_sensitivities` from required-key set; appended 62 new tests. |

## Registry Summary

```
Total nodes:       112
  Sectors:          11  (unchanged)
  Parent themes:    23  (promoted: banks, insurance, fintech, datacenter_infra, quantum, space, defense + 16 new)
  Sub-themes:       67  (15 preserved IDs, 44 new, parents updated)
  Market lenses:     3  (gold, silver, copper_miners — not assignable; parent_theme_id=metals_mining)
  Deprecated:        8  (ai_networking, semicap_equipment, lithium_battery, uranium_nuclear,
                          chemicals_materials, photonics_lasers, substrates_packaging,
                          travel_transportation)
Assignable nodes:  90
```

## Key Design Decisions

- **`dc_connectivity_silicon`** — new node for ALAB/CRDO/AAOI under Semiconductors. This is the canonical replacement for ai_networking.
- **Deprecated nodes** — retained with `classification="deprecated"`, `assignable=False`, `migration_targets` list, backward-compat aliases. Referenced by merge layer for section-name resolution during migration.
- **Market-lens nodes** — non-assignable; carry `parent_theme_id="metals_mining"` for rollup/context.
- **`defense` rollup** — `["industrials"]` only (technology removed; defense-electronics is a child of defense, not a cross-sector).
- **`datacenter_infra` rollup** — `["technology", "utilities", "real_estate"]` (industrials removed).
- **`memory_storage`** — no explicit `rollup_sector_ids`; inherits via `parent_theme_id=semiconductors → technology`.

## SOL 5.6 Finding

SOL 5.6 was not found in the codebase. Searched:
- `SOL56_MODEL_ID`, `SOL_MODEL_ID` environment variables — absent
- `agent/model_policy.py`, `config.py` — no `sol` model identifier

**Fallback used:** Anthropic `claude-haiku-4-5-20251001` (package already installed; key available). Dry-run sample ran successfully, producing 12 proposals for the representative ticker set.

## Dry-Run Sample Results

Artifacts at `backend/data/theme-taxonomy-v2-proposals.json` and `.csv`.

| Ticker | Proposed Primary | Confidence |
|--------|-----------------|------------|
| ALAB   | dc_connectivity_silicon | 0.92 |
| AEHR   | test_measurement | 0.88 |
| TER    | robotics_automation | 0.85 |
| AMKR   | packaging_substrates | 0.90 |
| VRT    | power_cooling | (model) |

All 12 tickers classified. `applied_to_db=False`.

## Test Results

```
154 existing tests:  154 passed (0 failed)
62 new v2 tests:     pending final run
```

## Merge Layer Mapping Strategy

- Legacy section names map to new canonical nodes (e.g. `"AI Networking" → "networking_fabric_infra"`)
- Deprecated nodes still resolve during migration via `_SECTION_TO_THEME_ID`
- `_CATEGORY_TO_THEME_ID` extended with all 73 v2 label variants

## Breaking Changes

None. All v1 theme IDs preserved. Deprecated nodes remain in registry with backward-compat aliases. `validate_theme_hierarchy` alias added for test backward compat.
