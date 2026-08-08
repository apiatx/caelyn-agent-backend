# Taxonomy V3 Deprecated-Theme Migration
## Complete Deprecated Theme Removal From Every Live Runtime/Public Data Path

**Task file:** `attached_assets/Pasted-REPLIT-AGENT-COMPLETE-TAXONOMY-V2-DEPRECATED-THEME-MIGR_1786148630251.txt`  
**Report date:** 2026-08-07  
**Commit SHA:** `34aafb75`

---

## 1. Starting State

| Field | Value |
|---|---|
| Repo root | `/home/runner/workspace` |
| Branch | `main` |
| HEAD before migration | `8dac834a` ("Published your App") |
| git status before | 1 local commit ahead of `origin/main` (ETF-only perf fix); runtime JSON dirty |
| Migration commit | `34aafb75` fix: retire deprecated themes from live taxonomy |
| HEAD after | `34aafb75` (1 commit ahead of `origin/main`) |

**git diff --check:** Clean — no trailing whitespace warnings.

---

## 2. The Eight Deprecated Nodes

Verified live in `services/theme_rs_universe.py`:

| Deprecated ID | Display Name (raw registry) | V3 Active Replacements |
|---|---|---|
| `ai_networking` | AI Networking | `dc_connectivity_silicon`, `networking_fabric_infra`, `optical_interconnects`, `servers_compute_systems`, `satellite_comms`, `test_measurement` |
| `semicap_equipment` | Semiconductor Equipment & Materials | `semicap_equip`, `test_measurement`, `optical_components_lasers` |
| `lithium_battery` | Lithium & Battery Technology | `battery_tech_storage`, `lithium` |
| `uranium_nuclear` | Uranium & Nuclear Energy | `uranium_nuclear_fuel`, `smr_advanced_reactors`, `nuclear_utilities_operators` |
| `chemicals_materials` | Chemicals & Materials | `advanced_materials` |
| `photonics_lasers` | Photonics & Lasers | `optical_components_lasers`, `optical_interconnects` |
| `substrates_packaging` | Substrates & Packaging | `semicap_equip`, `advanced_materials`, `test_measurement` |
| `travel_transportation` | Travel & Transportation | `travel_leisure`, `freight_logistics` |

All eight have `classification="deprecated"` and `assignable=False` in the raw registry. They are retained as alias records for backward-compatible historical parsing only.

---

## 3. Complete Reference Audit (Phase 1)

### A. ALLOWED historical/alias references (not changed)
- Raw `THEME_RS_UNIVERSE` entries: 8 deprecated nodes retained for alias parsing
- Migration maps in `theme_rs_universe.py`: `migration_targets` metadata on each deprecated node
- `_REPRESENTATIVE_ETF_MAP` entries for deprecated IDs (harmless display hints, deprecated ID not in enriched universe so never reached)
- Unit test fixtures that test deprecated registry metadata

### B. ILLEGAL live references found and fixed

| Location | Reference | Type | Phase |
|---|---|---|---|
| `theme_merge_layer._build()` | Full raw `THEME_RS_UNIVERSE` including deprecated passed to `_build_enriched_universe()` | Runtime universe base | 3 |
| `theme_merge_layer._SECTION_TO_THEME_ID["AI Networking"]` | `"ai_networking"` | Legacy label → deprecated ID | 2 |
| `theme_merge_layer._SECTION_TO_THEME_ID["Lithium & Battery Tech"]` | `"lithium_battery"` | Legacy label → deprecated ID | 2 |
| `theme_merge_layer._SECTION_TO_THEME_ID["Semi Equipment & Materials"]` | `"semicap_equipment"` | Legacy label → deprecated ID | 2 |
| `theme_merge_layer._SECTION_TO_THEME_ID["Semi Equipment"]` | `"semicap_equipment"` | Legacy label → deprecated ID | 2 |
| `theme_merge_layer._CATEGORY_TO_THEME_ID["AI Networking"]` | `"ai_networking"` | Category label → deprecated ID | 2 |
| `theme_resolver.resolve_primary_theme_for_ticker()` | No post-resolution deprecated guard | Resolver output | 5 |
| `routes/themes._perform_membership_write()` | No deprecated check — only `theme_id in universe` | Write gate | 6 |
| `routes/themes._perform_theme_membership_only_write()` | Same gap | Write gate | 6 |
| `routes/themes.admin_bulk_memberships()` | Same gap | Write gate | 6 |
| `theme_rs_service._load_lkg()` | No deprecated filter on old LKG rows | LKG load | 9 |
| `theme_rs_service._load_1d_lkg()` | No deprecated filter on old 1D curves | LKG load | 9 |
| DB `theme_ticker_overrides` | 46 active add rows for deprecated IDs, 40 tickers | Stored data | 7 |
| DB `watchlist_category_overrides` | 14 rows with deprecated category labels | Stored data | 7 |

---

## 4. Pre-Migration Live DB Counts (re-audited)

| Metric | Count |
|---|---|
| Active (`action='add'`) deprecated `theme_ticker_overrides` rows | **46** |
| Unique tickers with at least one deprecated active membership | **40** |
| Tickers with two deprecated active memberships | **6** (AAOI, AEHR, GLW, KLAC, ONTO, TRT) |
| Tickers with existing good active non-deprecated membership | **6** (ABSI→biotech, BAND→memory_storage, RDDT→software, TSM→semiconductors, UUUU→rare_earth, VSAT→space) |
| `watchlist_category_overrides` rows with deprecated labels | **14** |

---

## 5. Every Affected Ticker — Migration Table

| Ticker | Deprecated Removed | Active Added | Category Override → | Notes |
|---|---|---|---|---|
| AAOI | ai_networking, photonics_lasers | optical_interconnects | Optical Interconnects | Makes networking hardware for data-center optics; AAOI optical interconnect focus |
| ABSI | ai_networking | — (kept biotech) | — | Biotech is correct primary; AI Networking was never a valid membership |
| ACLS | semicap_equipment | semicap_equip | — | Ion implant equipment; clean match to active node |
| ADTN | ai_networking | networking_fabric_infra | Networking & Fabric Infrastructure | Network switching/routing; fits fabric/infra not optical |
| AEHR | semicap_equipment, substrates_packaging | test_measurement | Test & Measurement | AEHR makes wafer test equipment; not a substrate/packaging company |
| AMAT | substrates_packaging | semicap_equip | — | Applied Materials is core semicap equipment, not substrates/packaging |
| AMCR | substrates_packaging | advanced_materials | — | Amcor is specialty packaging/materials; advanced_materials is closest active node |
| ASPI | uranium_nuclear | uranium_nuclear_fuel | Uranium Mining & Nuclear Fuel | Alpha-S produces uranium and specialty materials; fuel cycle fits |
| BAND | ai_networking | — (kept memory_storage) | — | Bandwidth.com is cloud comms/CPaaS; memory_storage is also incorrect but not our scope |
| CEG | uranium_nuclear | nuclear_utilities_operators | Nuclear Utilities & Operators | Constellation Energy is a nuclear power operator |
| CIEN | ai_networking | optical_interconnects | — | Ciena makes optical networking hardware; optical_interconnects correct |
| DNN | uranium_nuclear | uranium_nuclear_fuel | — | Denison Mines — uranium exploration/mining |
| ELVA | lithium_battery | battery_tech_storage | Battery Technology & Energy Storage | Electrovaya — solid-state battery manufacturer |
| ENVX | lithium_battery | battery_tech_storage | — | Enovix — silicon-anode battery technology |
| FN | photonics_lasers | optical_components_lasers | — | II-VI/Fabrinet — optical component manufacturing |
| GLW | photonics_lasers, substrates_packaging | optical_components_lasers | Optical Components & Lasers | Corning's core business is optical fiber and specialty glass |
| IMSR | uranium_nuclear | smr_advanced_reactors | SMRs & Advanced Reactors | Integral Molten Salt Reactor — advanced reactor design company |
| KLAC | semicap_equipment, substrates_packaging | semicap_equip | — | KLA is core process control and inspection for semicap |
| LAC | lithium_battery | lithium | — | Lithium Americas — lithium mining/development |
| LEU | uranium_nuclear | uranium_nuclear_fuel | — | Centrus Energy — uranium enrichment services |
| LPTH | semicap_equipment | optical_components_lasers | Optical Components & Lasers | LightPath Technologies — optical components/lenses, not semicap equipment |
| MAXX | chemicals_materials | advanced_materials | — | MAXX Solar — advanced materials |
| MXL | ai_networking | dc_connectivity_silicon | — | MaxLinear makes data-center connectivity silicon/PHY chips |
| NNE | uranium_nuclear | smr_advanced_reactors | SMRs & Advanced Reactors | Nano Nuclear Energy — micro-reactor technology |
| OCC | ai_networking | optical_components_lasers | — | Optical Cable Corp — fiber optic cable manufacturer |
| OKLO | uranium_nuclear | smr_advanced_reactors | — | Oklo — advanced fission reactor startup |
| ONTO | semicap_equipment, substrates_packaging | semicap_equip | — | Onto Innovation — optical metrology and process control |
| OSS | ai_networking | servers_compute_systems | Servers & Compute Systems | One Stop Systems — GPU-accelerated computing platforms |
| QS | lithium_battery | battery_tech_storage | — | QuantumScape — solid-state battery cells |
| RDDT | ai_networking | — (kept software) | — | Reddit is a software/social platform; AI Networking was incorrect |
| SATS | ai_networking | satellite_comms | — | EchoStar — satellite communications operator |
| SILC | photonics_lasers | networking_fabric_infra | Networking & Fabric Infrastructure | Silicom — network interface cards and fabric adapters |
| SMR | uranium_nuclear | smr_advanced_reactors | — | NuScale Power — SMR technology developer |
| TRT | semicap_equipment, substrates_packaging | test_measurement | Test & Measurement | Trio-Tech — semiconductor test and burn-in systems |
| TSM | substrates_packaging | — (kept semiconductors) | — | TSMC is a foundry/semiconductor company; substrates was incorrect |
| UEC | uranium_nuclear | uranium_nuclear_fuel | — | Uranium Energy Corp — uranium mining |
| URG | uranium_nuclear | uranium_nuclear_fuel | Uranium Mining & Nuclear Fuel | Ur-Energy — uranium in-situ recovery |
| UUUU | uranium_nuclear | uranium_nuclear_fuel (added alongside rare_earth) | — | Energy Fuels — uranium + rare earth processing; both are correct |
| VIAV | ai_networking | test_measurement | — | Viavi Solutions — network test and measurement instruments |
| VSAT | ai_networking | — (kept space) | — | ViaSat is a satellite communications company; space is correct |

### Ambiguous decision explanations

**ai_networking → multiple targets:** The old ai_networking bucket lumped together pure optical networking (AAOI, CIEN, OCC → optical_interconnects), data center interconnect silicon (MXL → dc_connectivity_silicon), network fabric/switching (ADTN, SILC → networking_fabric_infra), compute systems (OSS → servers_compute_systems), satellite comms (SATS → satellite_comms), and test equipment (VIAV → test_measurement). Each was classified by actual business focus, not bucket name similarity.

**semicap_equipment + substrates_packaging → multiple targets:** Equipment companies (ACLS, KLAC, ONTO) → semicap_equip. Test/burn-in companies (AEHR, TRT) → test_measurement. Optical component companies that were mis-filed (LPTH) → optical_components_lasers.

**UUUU (rare_earth + uranium_nuclear_fuel):** Energy Fuels genuinely operates in both uranium and rare earth processing. The existing active `rare_earth` membership is preserved; `uranium_nuclear_fuel` is added as an intentional secondary. The deprecated `uranium_nuclear` is removed.

---

## 6. Post-Migration DB Counts

| Metric | Count |
|---|---|
| Active (`action='add'`) deprecated `theme_ticker_overrides` rows | **0** ✓ |
| Unique tickers with deprecated active membership | **0** ✓ |
| `watchlist_category_overrides` rows with deprecated labels | **0** ✓ |

---

## 7. Category Override Cleanup (14 rows)

| Old Label | Tickers | New Label |
|---|---|---|
| AI Networking | ADTN, OSS | Networking & Fabric Infrastructure, Servers & Compute Systems |
| Lithium & Battery Tech | ELVA | Battery Technology & Energy Storage |
| Photonics / Lasers | AAOI, GLW, SILC | Optical Interconnects, Optical Components & Lasers, Networking & Fabric Infrastructure |
| Semi Equipment & Materials | LPTH | Optical Components & Lasers |
| Substrates / Packaging | AEHR, TRT | Test & Measurement |
| Uranium & Nuclear Energy | ASPI, CEG, IMSR, NNE, URG | Uranium Mining & Nuclear Fuel, Nuclear Utilities & Operators, SMRs & Advanced Reactors |

---

## 8. Resolver Changes (Phase 5)

**File:** `services/theme_resolver.py`  
**Change:** Post-resolution guard added at the return boundary of `resolve_primary_theme_for_ticker()`. After any resolution path (mapper, themes-page membership, manual category override), the function now checks the raw registry for `classification == "deprecated"`. If the resolved `theme_id` is deprecated, it is set to `None` with source `"deprecated_suppressed"`.

**Result:** `resolve_primary_theme_for_ticker(ticker).theme_id` is always:
- An active assignable `theme` or `sub_theme` ID
- `None` (legitimately unassigned)
- Never deprecated, never market_lens, never a sector as thematic primary

---

## 9. Runtime/Enriched Universe Boundary (Phase 3)

**New function:** `get_active_runtime_registry()` in `services/theme_rs_universe.py`

```python
def get_active_runtime_registry(registry=None):
    """Excludes deprecated nodes; preserves market_lens."""
    reg = registry or THEME_RS_UNIVERSE
    return {k: v for k, v in reg.items() if v.get("classification") != "deprecated"}
```

**`_build()` fix in `theme_merge_layer.py`:** Now calls `get_active_runtime_registry(THEME_RS_UNIVERSE)` to produce `active_base` (104 nodes) and passes it to `_build_enriched_universe()` instead of the full raw registry (112 nodes). This is the single upstream fix that automatically cleans every downstream consumer.

**Invariant enforced:**
```
any(meta.get("classification") == "deprecated" 
    for meta in ENRICHED_THEME_RS_UNIVERSE.values()) == False
```
Verified live: `ENRICHED_THEME_RS_UNIVERSE` = 104 nodes, 0 deprecated.

**Legacy label maps fixed:**
- `_SECTION_TO_THEME_ID["AI Networking"]` → `None` (ambiguous; defer to per-ticker resolver)
- `_SECTION_TO_THEME_ID["Lithium & Battery Tech"]` → `None` (ambiguous; split into lithium/battery_tech_storage)
- `_SECTION_TO_THEME_ID["Semi Equipment & Materials"]` → `None` (ambiguous split)
- `_SECTION_TO_THEME_ID["Semi Equipment"]` → `"semicap_equip"` (specific enough)
- `_SECTION_TO_THEME_ID["Photonics / Lasers"]` → `"optical_components_lasers"` (pre-existing active redirect)
- `_CATEGORY_TO_THEME_ID["AI Networking"]` → removed entirely

---

## 10. Theme RS Before/After Row Counts

| Metric | Before | After |
|---|---|---|
| `ENRICHED_THEME_RS_UNIVERSE` nodes | 112 (incl. 8 deprecated) | **104** (0 deprecated) |
| RS compute universe (`_EXPECTED_THEME_COUNT`) | 112 | **104** |
| LKG disk snapshot rows | 112 | **104** (sanitized on first load) |
| 1D LKG curves | 112 | **104** (8 deprecated curves stripped on first load) |
| RS `classification=all` API rows | 112 | **104** (verified live) |
| RS `classification=theme` API rows | 23 | 23 (unchanged — no theme-class deprecated nodes) |
| RS `classification=sub_theme` API rows | 67 | 67 (unchanged — no sub_theme-class deprecated nodes) |

The 8 deprecated nodes had classification `"deprecated"`, so they appeared under `classification=all` only. Themes and sub_themes counts are unchanged.

### Confirmation: deprecated rows no longer distort RS rankings

Deprecated nodes had their own proxy symbol baskets (e.g. ai_networking had AAOI, CIEN, etc. — which are also in dc_connectivity_silicon, optical_interconnects etc.). These duplicate baskets inflated the RS percentile peer universe to 112 nodes and caused those symbols' returns to be double-counted in the denominator. After Phase 3 the percentile/rank universe is 104 active nodes only. No duplicate deprecated baskets contribute to rankings.

---

## 11. Options Flow Before/After Universe/Group Counts

Options Flow `build_sector_tree()` accepts `ENRICHED_THEME_RS_UNIVERSE` as its argument. After Phase 3 this is already deprecated-free. No separate Options Flow fix was required.

| Metric | Before | After |
|---|---|---|
| Options Flow sector groups | 11 | **11** (sectors, same structure) |
| Options Flow theme nodes | included deprecated | **0 deprecated** |
| Supplement required universe | included deprecated-only symbols | **clean** (deprecated nodes removed from enriched universe) |
| Net-premium theme entity generation | could generate deprecated rows | **blocked** (deprecated IDs not in enriched universe) |

**Verified live:** `GET /api/options-flow/sectors` HTTP=200, `sectors` array = 11 entries, 0 deprecated theme IDs.

Tradier cadence, scan cadence, freshness, premium/P&C calculations, and scanner lanes untouched.

---

## 12. Watchlist Identity Validation (Phase 12)

The DB migration (Phase 7) immediately cleared all deprecated memberships from `theme_ticker_overrides` and `watchlist_category_overrides`. Cache invalidation was called at end of migration:

- `refresh_enriched_universe()` → rebuilt ENRICHED_THEME_RS_UNIVERSE with 104 nodes
- `invalidate_theme_rs_cache()` → LKG sanitized 112→104, background 1D refresh queued
- `invalidate_sectors_cache()` → Options Flow tree cache cleared

Server startup confirmed sanitization log:
```
[THEME_RS] LKG sanitized: removed 8 deprecated theme row(s) from old snapshot (112 → 104 themes)
[THEME_RS] 1D LKG sanitized: removed 8 deprecated curve(s): ['photonics_lasers',
  'lithium_battery', 'uranium_nuclear', 'semicap_equipment', 'substrates_packaging',
  'ai_networking', 'chemicals_materials', 'travel_transportation']
```

**Sample ex-deprecated tickers resolved by live resolver:**
| Ticker | theme_id | deprecated? |
|---|---|---|
| AAOI | optical_interconnects | No ✓ |
| AEHR | test_measurement | No ✓ |
| KLAC | semicap_equip | No ✓ |
| ELVA | battery_tech_storage | No ✓ |
| MAXX | advanced_materials | No ✓ |
| SMR | smr_advanced_reactors | No ✓ |

LKG disk file on disk after restart: **104 rows, 0 deprecated** ✓

---

## 13. Home Validation

The `/api/themes/relative-strength` endpoint is the canonical RS source consumed by the Home dashboard. It now returns 104 active themes, 0 deprecated. No separate Home endpoint exists that would source deprecated data.

---

## 14. Public API Validation (Phase 13 Live Results)

| Check | Endpoint | HTTP | Result |
|---|---|---|---|
| E | `GET /api/themes/list` | 200 | 104 themes, **0 deprecated** ✓ |
| F1 | `GET /api/themes/relative-strength?classification=all` | 200 | 104 rows, **0 deprecated** ✓ |
| F2 | `GET /api/themes/relative-strength?classification=sub_theme` | 200 | 67 rows, **0 deprecated** ✓ |
| F3 | `GET /api/themes/relative-strength?classification=theme` | 200 | 23 rows, **0 deprecated** ✓ |
| H | `GET /api/options-flow/sectors` | 200 | 11 sector groups, **0 deprecated** ✓ |

---

## 15. Future-Write Rejection Tests (Phase 6 + Phase 13 Live)

New helper `_validate_thematic_assignment(theme_id)` in `routes/themes.py` — called for every `action='add'` across all 3 membership write paths.

### Live rejection tests (with auth):

| Test | Endpoint | Payload | HTTP | Response |
|---|---|---|---|---|
| Deprecated ai_networking | `POST /api/themes/admin/memberships` | `{theme_id: "ai_networking", action: "add"}` | **422** | "retired deprecated node... migration_targets: ['dc_connectivity_silicon', 'networking_fabric_infra']" ✓ |
| Deprecated uranium_nuclear (bulk) | `POST /api/themes/admin/memberships/bulk` | `{edits: [{theme_id: "uranium_nuclear", action: "add"}]}` | **404** | "Unknown theme_id(s): ['uranium_nuclear']" (not in enriched universe = Phase 3 catches it) ✓ |
| Active theme remove | `POST /api/themes/admin/memberships` | `{theme_id: "semiconductors", action: "remove"}` | **200** | Success — removes work fine ✓ |

**Validation rules enforced (422 for add of):**
- `classification == "deprecated"` → 422 "retired deprecated node"
- `classification == "sector"` → 422 "sector node — use theme/sub_theme"
- `classification == "market_lens"` → 422 "market_lens node — cannot be assigned"
- `assignable == False` → 422 "assignable=False"
- Unknown ID (not in enriched universe) → 404

---

## 16. Cache / LKG Sanitization

| Cache | Action | Status |
|---|---|---|
| `ENRICHED_THEME_RS_UNIVERSE` | Rebuilt via `refresh_enriched_universe()` after migration | 104 nodes, 0 deprecated ✓ |
| Theme RS memory cache | Invalidated via `invalidate_theme_rs_cache()` | Background refresh queued ✓ |
| Theme RS 5Y LKG disk | Sanitized by `_load_lkg()` on next load (Phase 9) | 112→104 rows ✓ |
| Theme RS 1D LKG disk | Sanitized by `_load_1d_lkg()` on next load (Phase 9) | 8 deprecated curves removed ✓ |
| Options Flow sectors cache | Invalidated via `invalidate_sectors_cache()` | Rebuilt from 104-node enriched universe ✓ |

Phase 9 sanitization in `_load_lkg()` and `_load_1d_lkg()` is permanent — any future restart with an old snapshot will strip deprecated rows before serving. No good active LKG data was lost.

---

## 17. Provider Schedules / Freshness — Untouched

- Tradier scheduler: unchanged
- FMP calls: unchanged
- Options scan cadence: unchanged
- Quote refresh cadence: unchanged
- Canonical history: unchanged
- Earnings monitor: unchanged
- Watchlist performance/LKG refresh: unchanged

No new provider calls were added. No earnings changes. No sector reclassification. No LLM architecture change.

---

## 18. Tests

```
PASSED  tests/test_theme_hierarchy.py — 324 passed in 3.74s
```

### New regression tests (30) — `TestDeprecatedExclusion` block

| # | Test | Asserts |
|---|---|---|
| 1 | `test_raw_registry_has_8_deprecated` | Raw registry has exactly 8 deprecated nodes |
| 2 | `test_active_runtime_registry_has_zero_deprecated` | `get_active_runtime_registry()` returns 0 deprecated |
| 3 | `test_active_runtime_registry_size` | Active = raw − 8 |
| 4 | `test_enriched_universe_has_zero_deprecated` | ENRICHED_THEME_RS_UNIVERSE has 0 deprecated |
| 5 | `test_enriched_universe_excludes_all_deprecated_ids` | None of the 8 IDs are keys |
| 6 | `test_assignable_registry_excludes_deprecated` | All 8 absent from assignable registry |
| 7 | `test_theme_rs_service_universe_has_zero_deprecated` | theme_rs_service alias has 0 deprecated |
| 8 | `test_lkg_floor_matches_active_count` | `_EXPECTED_THEME_COUNT` = 104 |
| 9 | `test_load_lkg_sanitizes_deprecated_rows` | `_load_lkg()` strips deprecated rows from old file |
| 10 | `test_load_1d_lkg_sanitizes_deprecated_curves` | `_load_1d_lkg()` strips deprecated curves |
| 11 | `test_enriched_proxy_symbols_excludes_deprecated_only_tickers` | All proxy symbols referenced by ≥1 active node |
| 12 | `test_options_flow_uses_active_only_universe` | 8 deprecated IDs absent from enriched universe |
| 13-17 | `test_resolver_never_returns_deprecated` (×5 tickers) | Resolver returns active or None |
| 18 | `test_resolver_with_deprecated_category_override` | Resolver suppresses stale deprecated category |
| 19 | `test_legacy_photonics_lasers_resolves_active` | "Photonics / Lasers" → active optical node |
| 20 | `test_legacy_semi_equipment_resolves_active` | "Semi Equipment" → "semicap_equip" |
| 21-23 | `test_ambiguous_legacy_label_not_deprecated` (×3) | "AI Networking", "Lithium & Battery Tech", "Semi Equipment & Materials" → None |
| 24 | `test_category_map_has_no_deprecated_ids` | _CATEGORY_TO_THEME_ID values ∩ deprecated = {} |
| 25 | `test_section_map_has_no_deprecated_ids` | _SECTION_TO_THEME_ID values ∩ deprecated = {} |
| 26 | `test_validate_rejects_deprecated_primary` | `_validate_thematic_assignment("ai_networking")` → 422 |
| 27 | `test_validate_rejects_deprecated_additional` | `_validate_thematic_assignment("uranium_nuclear")` → 422 |
| 28 | `test_validate_rejects_all_8_deprecated_ids` | All 8 → 422 |
| 29 | `test_validate_rejects_unknown_theme` | Unknown ID → 404 |
| 30 | `test_validate_rejects_sector` (×7 parametrized) | Sector nodes → 422 |
| 31 | `test_validate_rejects_market_lens` | Market lens nodes → 422 |
| 32 | `test_validate_passes_for_active_theme` | `"semiconductors"` → no exception |
| 33 | `test_validate_passes_for_active_sub_theme` | `"semicap_equip"` → no exception |
| 34 | `test_validate_passes_for_parent_theme` | `"nuclear_energy"` → no exception |
| 35-39 | `TestMigrationResults` (DB, 5 tests) | 0 deprecated add rows; ABSI keeps biotech; UUUU correct; AAOI category updated; no sector written |
| 40-44 | `test_migrated_tickers_resolve_to_active_theme` (×5) | AAOI/KLAC/SMR/SILC/MAXX → active themes |
| 45 | `test_active_runtime_keeps_market_lens` | market_lens nodes preserved in active runtime |
| 46 | `test_enriched_theme_rs_universe_invariant` | Core invariant: no deprecated in enriched universe |

Also updated 4 pre-existing tests that asserted the old (pre-Phase 2) deprecated mapping behavior:
- `test_standalone_subtheme_row_has_rollup_without_parent_theme_id` → `test_subtheme_row_with_parent_has_rollup` (ai_networking retired; dc_connectivity_silicon used)
- `test_deprecated_node_assignable_false_repaired` → updated to assert Phase 9 behavior (LKG strips deprecated upstream)
- `test_section_map_ai_networking_points_to_deprecated_node` → `test_section_map_ai_networking_is_none_post_v3` 
- `test_category_map_ai_networking_points_to_deprecated_node` → `test_category_map_ai_networking_is_none_post_v3`

---

## 19. git diff --check

```
(no output — clean)
```

---

## 20. Exact Files Changed/Staged

```
backend/services/theme_rs_universe.py        +31 lines   (get_active_runtime_registry)
backend/services/theme_merge_layer.py        +38 lines   (active_base boundary; label map fixes)
backend/services/theme_resolver.py           +13 lines   (deprecated suppression guard)
backend/routes/themes.py                     +95 lines   (_validate_thematic_assignment + 3 write-path calls)
backend/services/theme_rs_service.py         +39 lines   (_DEPRECATED_THEME_IDS; LKG/1D sanitization)
backend/migrations/migrate_deprecated_themes.py   (NEW)  idempotent migration script
backend/tests/test_theme_hierarchy.py        +671 lines  (30 new tests; 4 pre-existing updated)
backend/.codex-reports/latest.md             (NEW)       this report
```

Runtime/cache files (dirty but not staged):
- `backend/data/themes_rs_lkg.json`, `backend/data/themes_rs_1d_lkg.json`, `backend/data/watchlist_stage2_lkg.json`, etc.

---

## 21. Final Commit SHA

```
34aafb75  fix: retire deprecated themes from live taxonomy
```

**Push status:** Not pushed — awaiting user review per task instructions.

---

## Summary

All 13 phases complete. The `ENRICHED_THEME_RS_UNIVERSE` is permanently deprecated-free (104 nodes). All 40 affected tickers were individually classified and migrated to correct active theme IDs. No future write can assign a deprecated, sector, or market_lens node as a thematic membership. 324/324 tests pass. Zero deprecated nodes appear in any public API response.
