# Taxonomy V3 Deprecated-Theme Migration Report
**Date:** 2026-08-07  
**Commit message:** `fix: retire deprecated themes from live taxonomy`

---

## Summary

Completed full retirement of 8 deprecated theme nodes from all live/public code paths. The enriched universe (`ENRICHED_THEME_RS_UNIVERSE`) now permanently excludes the 8 deprecated nodes (112 → 104 nodes). All DB override rows were migrated atomically. New write-path validation prevents any future assignment of deprecated (or sector/market_lens) IDs. 324/324 tests pass.

---

## Deprecated IDs Retired

| Deprecated ID | V3 Replacement(s) |
|---|---|
| `ai_networking` | `dc_connectivity_silicon`, `networking_fabric_infra`, `optical_interconnects`, `servers_compute_systems`, `satellite_comms`, `test_measurement` |
| `semicap_equipment` | `semicap_equip`, `test_measurement`, `optical_components_lasers` |
| `lithium_battery` | `battery_tech_storage`, `lithium` |
| `uranium_nuclear` | `uranium_nuclear_fuel`, `nuclear_utilities_operators`, `smr_advanced_reactors` |
| `chemicals_materials` | `advanced_materials` |
| `photonics_lasers` | `optical_components_lasers`, `optical_interconnects`, `networking_fabric_infra` |
| `substrates_packaging` | `semicap_equip`, `advanced_materials`, `test_measurement` |
| `travel_transportation` | (no active override rows — already clean) |

---

## Files Changed

| File | Purpose |
|---|---|
| `services/theme_rs_universe.py` | Added `get_active_runtime_registry()` — 104-node base excluding deprecated |
| `services/theme_merge_layer.py` | `_build()` now uses `get_active_runtime_registry()` as base; fixed `_SECTION_TO_THEME_ID` and `_CATEGORY_TO_THEME_ID` to remove deprecated IDs |
| `services/theme_resolver.py` | Phase 5 guard: post-resolution suppression of deprecated `theme_id` |
| `routes/themes.py` | `_validate_thematic_assignment()` helper; applied in all 3 membership write paths |
| `services/theme_rs_service.py` | `_DEPRECATED_THEME_IDS` constant; Phase 9 sanitization in `_load_lkg()` and `_load_1d_lkg()` |
| `migrations/migrate_deprecated_themes.py` | New idempotent DB migration script (40 tickers) |
| `tests/test_theme_hierarchy.py` | 30 new regression tests in `TestDeprecatedExclusion` block |

---

## DB Migration Results

**Pre-migration:**
- `theme_ticker_overrides` active deprecated rows: 46
- Unique tickers affected: 40
- `watchlist_category_overrides` deprecated label rows: 14

**Migration execution:** 40/40 tickers succeeded, 0 failed.

**Post-migration:**
- `theme_ticker_overrides` active deprecated rows: **0** ✓
- Unique tickers with deprecated memberships: **0** ✓

### Ticker Migration Table (40 tickers)

| Ticker | Deprecated Removed | Active Added | Category Updated |
|---|---|---|---|
| AAOI | ai_networking, photonics_lasers | optical_interconnects | Optical Interconnects |
| ABSI | ai_networking | — (kept biotech) | — |
| ACLS | semicap_equipment | semicap_equip | — |
| ADTN | ai_networking | networking_fabric_infra | Networking & Fabric Infrastructure |
| AEHR | semicap_equipment, substrates_packaging | test_measurement | Test & Measurement |
| AMAT | substrates_packaging | semicap_equip | — |
| AMCR | substrates_packaging | advanced_materials | — |
| ASPI | uranium_nuclear | uranium_nuclear_fuel | Uranium Mining & Nuclear Fuel |
| BAND | ai_networking | — (kept memory_storage) | — |
| CEG | uranium_nuclear | nuclear_utilities_operators | Nuclear Utilities & Operators |
| CIEN | ai_networking | optical_interconnects | — |
| DNN | uranium_nuclear | uranium_nuclear_fuel | — |
| ELVA | lithium_battery | battery_tech_storage | Battery Technology & Energy Storage |
| ENVX | lithium_battery | battery_tech_storage | — |
| FN | photonics_lasers | optical_components_lasers | — |
| GLW | photonics_lasers, substrates_packaging | optical_components_lasers | Optical Components & Lasers |
| IMSR | uranium_nuclear | smr_advanced_reactors | SMRs & Advanced Reactors |
| KLAC | semicap_equipment, substrates_packaging | semicap_equip | — |
| LAC | lithium_battery | lithium | — |
| LEU | uranium_nuclear | uranium_nuclear_fuel | — |
| LPTH | semicap_equipment | optical_components_lasers | Optical Components & Lasers |
| MAXX | chemicals_materials | advanced_materials | — |
| MXL | ai_networking | dc_connectivity_silicon | — |
| NNE | uranium_nuclear | smr_advanced_reactors | SMRs & Advanced Reactors |
| OCC | ai_networking | optical_components_lasers | — |
| OKLO | uranium_nuclear | smr_advanced_reactors | — |
| ONTO | semicap_equipment, substrates_packaging | semicap_equip | — |
| OSS | ai_networking | servers_compute_systems | Servers & Compute Systems |
| QS | lithium_battery | battery_tech_storage | — |
| RDDT | ai_networking | — (kept software) | — |
| SATS | ai_networking | satellite_comms | — |
| SILC | photonics_lasers | networking_fabric_infra | Networking & Fabric Infrastructure |
| SMR | uranium_nuclear | smr_advanced_reactors | — |
| TRT | semicap_equipment, substrates_packaging | test_measurement | Test & Measurement |
| TSM | substrates_packaging | — (kept semiconductors) | — |
| UEC | uranium_nuclear | uranium_nuclear_fuel | — |
| URG | uranium_nuclear | uranium_nuclear_fuel | Uranium Mining & Nuclear Fuel |
| UUUU | uranium_nuclear | uranium_nuclear_fuel (added alongside rare_earth) | — |
| VIAV | ai_networking | test_measurement | — |
| VSAT | ai_networking | — (kept space) | — |

---

## Runtime Universe State After Migration

| Universe | Count | Deprecated nodes |
|---|---|---|
| Raw `THEME_RS_UNIVERSE` | 112 | 8 (retained as alias-only) |
| `get_active_runtime_registry()` | 104 | **0** ✓ |
| `ENRICHED_THEME_RS_UNIVERSE` | 104 | **0** ✓ |
| `get_assignable_registry()` | 101 | **0** ✓ |

---

## Code Path Coverage

| Phase | Code Path | Change |
|---|---|---|
| Phase 2 | `_SECTION_TO_THEME_ID` | AI Networking, Lithium & Battery Tech, Semi Equipment & Materials → `None` (ambiguous) |
| Phase 2 | `_CATEGORY_TO_THEME_ID` | "AI Networking" → `ai_networking` entry removed |
| Phase 3 | `_build()` in `theme_merge_layer.py` | Uses `get_active_runtime_registry()` base instead of raw `THEME_RS_UNIVERSE` |
| Phase 5 | `theme_resolver.py` | Post-resolution guard: deprecated `theme_id` → `None` (suppressed) |
| Phase 6 | `routes/themes.py` write paths | `_validate_thematic_assignment()` called for all `action='add'` requests |
| Phase 9 | `theme_rs_service._load_lkg()` | Strips deprecated rows from old LKG snapshots on disk |
| Phase 9 | `theme_rs_service._load_1d_lkg()` | Strips deprecated curves from old 1D LKG snapshots |

---

## Test Coverage (30 new regression tests)

All 30 tests in the `TestDeprecatedExclusion` block pass. Tests cover:

1. Raw registry retains exactly 8 deprecated alias nodes
2. `get_active_runtime_registry()` returns 0 deprecated
3. `ENRICHED_THEME_RS_UNIVERSE` contains 0 deprecated (× 2 assertions)
4. `get_assignable_registry()` excludes deprecated
5. Theme RS universe alias has 0 deprecated
6. LKG floor reflects active count (104, not 112)
7. `_load_lkg()` sanitizes stale deprecated rows
8. `_load_1d_lkg()` sanitizes stale deprecated curves
9. Enriched proxy symbols excludes deprecated-only tickers
10. Options Flow universe excludes deprecated nodes
11. `resolve_primary_theme_for_ticker()` never returns deprecated (5 parametrized tickers)
12. Resolver suppresses deprecated from stale category override
13. Legacy unambiguous labels resolve to active nodes
14. Legacy `"Semi Equipment"` resolves to `"semicap_equip"` (active)
15. Ambiguous legacy labels (`"AI Networking"`, etc.) map to `None` (×3 parametrized)
16. `_CATEGORY_TO_THEME_ID` has no deprecated values
17. `_SECTION_TO_THEME_ID` has no deprecated values
18. `_validate_thematic_assignment()` raises 422 for deprecated
19. All 8 deprecated IDs rejected by validator (parametrized)
20. Unknown theme_id raises 404
21. Sector nodes rejected with 422 (×7 parametrized)
22. Market_lens nodes rejected with 422
23. Active theme nodes pass validation
24. Active sub_theme nodes pass validation
25. Parent-theme direct assignment valid
26. DB: 0 active deprecated override rows (migration result)
27. DB: ABSI retains biotech (existing active primary preserved)
28. DB: UUUU has uranium_nuclear tombstoned + rare_earth + uranium_nuclear_fuel
29. DB: AAOI category override → "Optical Interconnects"
30. DB: No sector node introduced as thematic membership for migrated tickers
31. Migrated tickers resolve to active themes via resolver (5 parametrized)
32. `get_active_runtime_registry()` preserves all market_lens nodes
33. `ENRICHED_THEME_RS_UNIVERSE` core invariant (no deprecated)

**Total: 324 tests passed, 0 failed.**

---

## Cache Invalidation

Performed at end of migration script and confirmed via log output:
- `refresh_enriched_universe()` — rebuilt ENRICHED_THEME_RS_UNIVERSE (104 nodes)
- `invalidate_theme_rs_cache()` — LKG sanitized from 112 → 104 themes, background 1D refresh queued
- `invalidate_sectors_cache()` — Options Flow sectors tree cleared

---

## No-Push Checkpoint

Per task instructions: do not push until user reviews this report.
