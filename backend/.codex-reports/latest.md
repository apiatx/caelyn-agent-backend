# POST-MIGRATION TAXONOMY QUALITY AUDIT — THEME ASSIGNMENT CORRECTIONS

**Task:** Verify primary vs. additional semantics for the 40 migrated tickers; correct only proven misclassifications.  
**Date:** 2026-08-08  
**Starting HEAD:** `16f6d02f` (main, origin/main)  
**Branch:** main  
**git status -sb:** 1 dirty (attached_assets new file, unstaged — runtime cache diffs ignored)

---

## 1. Primary-Store Semantics

The canonical primary for a ticker is determined by `resolve_primary_theme_for_ticker()` in the following priority order:

| Step | Source | Wins when |
|------|--------|-----------|
| 1 | `theme_ticker_mapper` (LLM → foreign_alias → THEME_RS_UNIVERSE proxy/candidate → home_service → ETF universe) | ticker has any static or LLM-file entry |
| 2 | `map_industry_to_theme` industry fallback | Step 1 empty |
| 3 | `ENRICHED_THEME_RS_UNIVERSE` membership (themes-page) | display_name differs from Step 1/2 result |
| 4 | `watchlist_category_overrides` manual override | **always wins** |
| 5 | Phase 5 deprecated guard | if final `theme_id` is deprecated → suppress to `None` |

`atomic_taxonomy_write_db` with `primary_operation action='set'` is the canonical write path for the primary store (writes `watchlist_category_overrides`). A theme membership addition alone (`action='add'` in `theme_ticker_overrides`) does **not** set the canonical primary.

---

## 2. Resolver-Ordering Audit Finding

`build_theme_resolution_context()` builds `themes_page_map` by iterating `ENRICHED_THEME_RS_UNIVERSE` and assigning the **first** theme that contains a ticker. For tickers in multiple themes (e.g. UUUU in `nuclear_energy`, `rare_earth`, `uranium_nuclear_fuel`), whichever theme comes first in the dict iteration order wins the themes-page slot. This can cause an additional theme to masquerade as primary when:

- No mapper entry exists for the ticker (Step 1 returns None), AND
- The iteration order places the wrong theme before the intended primary.

**Affected tickers identified:** UUUU, AMAT, OKLO, SMR, LEU, VIAV, MXL.

**Resolution:** Set explicit `watchlist_category_overrides` rows (primary_operation) for all affected tickers. No resolver redesign required — the existing architecture supports explicit primary via the cat_override mechanism.

---

## 3. Full 40-Ticker Audit Table

| Ticker | Company / Business | Old Deprecated | DB Active Memberships | Cat Override (before) | Mapper Primary | Resolver Before | Recommended Primary | Recommended Additional | Change? | Reason |
|--------|-------------------|----------------|-----------------------|----------------------|----------------|-----------------|--------------------|-----------------------|---------|--------|
| AAOI | AAOI — optical interconnect ICs | ai_networking, photonics_lasers | optical_interconnects | Optical Interconnects | optical_interconnects | optical_interconnects | optical_interconnects | optical_components_lasers (static) | ✗ | Correct |
| ABSI | AbSci — AI protein design / biotech | ai_networking | biotech | Biotech | — | biotech | biotech | — | ✗ | Correct |
| ACLS | Axcelis — ion implant equipment | semicap_equipment | semicap_equip | — | semicap_equip | semicap_equip | semicap_equip | — | ✗ | Correct |
| ADTN | Adtran — fiber broadband networking | ai_networking | networking_fabric_infra | Networking & Fabric Infrastructure | — | networking_fabric_infra | networking_fabric_infra | — | ✗ | Correct |
| AEHR | Aehr Test — semiconductor burn-in test | semicap_equipment, substrates_packaging | test_measurement | Test & Measurement | test_measurement | test_measurement | test_measurement | — | ✗ | Correct |
| **AMAT** | Applied Materials — semicon equipment | substrates_packaging | semicap_equip, semiconductors | — | semiconductors | **semiconductors** | **semicap_equip** | semiconductors (additional) | ✓ | Resolver returned semantically lower-priority theme; AMAT is equipment-maker not chipmaker |
| AMCR | Amcor — flexible packaging | substrates_packaging | advanced_materials | — | advanced_materials | advanced_materials | advanced_materials | — | ✗ | Correct |
| **ASPI** | ASP Isotopes — laser uranium enrichment | uranium_nuclear | uranium_nuclear_fuel | Uranium & Nuclear Energy (deprecated display name) | uranium_nuclear (deprecated) | name="Uranium & Nuclear Energy" (deprecated) / id=uranium_nuclear_fuel | **Uranium Mining & Nuclear Fuel** | — | ✓ | Category override carried deprecated display name → name/id mismatch; mapper had stale foreign_alias_map entry |
| **BAND** | Bandwidth Inc — CPaaS / cloud comms API | ai_networking | **memory_storage** (pre-existing wrong membership) | Memory & Storage | datacenter_infra (LLM, wrong) | **memory_storage** | **cloud_software** | — | ✓ | Both DB membership and category override were factually wrong; Bandwidth is a CPaaS SaaS, not a storage company |
| CEG | Constellation Energy — nuclear power | uranium_nuclear | nuclear_utilities_operators | Nuclear Utilities & Operators | nuclear_utilities_operators | nuclear_utilities_operators | nuclear_utilities_operators | — | ✗ | Correct |
| CIEN | Ciena — optical networking equipment | ai_networking | optical_interconnects | — | optical_interconnects | optical_interconnects | optical_interconnects | — | ✗ | Correct |
| DNN | Denison Mines — uranium mining | uranium_nuclear | uranium_nuclear_fuel | — | uranium_nuclear (LLM, deprecated) | uranium_nuclear_fuel (themes_page overrides) | uranium_nuclear_fuel | — | ✗ | Themes_page correctly overrides stale LLM; final correct |
| ELVA | Electrovaya — lithium battery cells | lithium_battery | battery_tech_storage | Battery Technology & Energy Storage | power_cooling (LLM, wrong) | battery_tech_storage (themes_page overrides) | battery_tech_storage | — | ✗ | Themes_page + cat_override correctly override stale LLM |
| ENVX | Enovix — silicon anode batteries | lithium_battery | battery_tech_storage | — | battery_tech_storage | battery_tech_storage | battery_tech_storage | — | ✗ | Correct |
| FN | Fabrinet — optical contract manufacturer | photonics_lasers | optical_components_lasers | — | photonics_optical | photonics_optical | photonics_optical | optical_components_lasers (static) | ✗ | photonics_optical is a valid primary for a photonic systems manufacturer |
| GLW | Corning — optical fiber / photonics | photonics_lasers, substrates_packaging | optical_components_lasers | Optical Components & Lasers | semiconductors (LLM, wrong) | optical_components_lasers (cat_override wins) | optical_components_lasers | — | ✗ | Cat_override correctly overrides stale LLM |
| **IMSR** | Terrestrial Energy — integral MSR reactor | uranium_nuclear | smr_advanced_reactors | Uranium & Nuclear Energy (deprecated display name) | uranium_nuclear (LLM, deprecated) | name="Uranium & Nuclear Energy" / id=smr_advanced_reactors | **SMRs & Advanced Reactors** | — | ✓ | Same pattern as ASPI — deprecated display name in cat_override causing name/id mismatch |
| KLAC | KLA Corp — process control equipment | semicap_equipment, substrates_packaging | semicap_equip | — | semicap_equip | semicap_equip | semicap_equip | — | ✗ | Correct; no meaningful packaging_substrates additional (KLAC's packaging inspection is minor relative to core process control) |
| LAC | Lithium Americas — lithium mining | lithium_battery | lithium | — | lithium | lithium | lithium | — | ✗ | Correct |
| **LEU** | Centrus Energy — uranium enrichment | uranium_nuclear | uranium_nuclear_fuel | — | nuclear_energy (static) | **nuclear_energy** | **uranium_nuclear_fuel** | — | ✓ | Resolver returned higher-level nuclear_energy; uranium_nuclear_fuel is more specific and accurate primary |
| LPTH | LightPath — optical components | semicap_equipment | optical_components_lasers | Optical Components & Lasers | photonics_optical | optical_components_lasers (cat_override wins) | optical_components_lasers | — | ✗ | Cat_override correctly overrides |
| MAXX | Ultralife / Materials-Co — advanced materials | chemicals_materials | advanced_materials | — | — | advanced_materials (themes_page) | advanced_materials | — | ✗ | Correct |
| **MXL** | MaxLinear — DC connectivity silicon | ai_networking | dc_connectivity_silicon | — | semiconductors (LLM, wrong) | **semiconductors** | **dc_connectivity_silicon** | — | ✓ | LLM override + iteration order caused wrong primary; dc_connectivity_silicon is MaxLinear's correct identity |
| NNE | Nano Nuclear Energy — SMR developer | uranium_nuclear | smr_advanced_reactors | SMRs & Advanced Reactors | industrials (LLM, wrong) | smr_advanced_reactors (cat_override wins) | smr_advanced_reactors | — | ✗ | Cat_override correctly overrides stale LLM |
| OCC | OCC Corp — optical cable/connectivity | ai_networking | optical_components_lasers | — | ai_networking (LLM, deprecated) | optical_components_lasers (themes_page overrides) | optical_components_lasers | — | ✗ | Themes_page correctly overrides stale deprecated LLM |
| **OKLO** | Oklo — nuclear microreactor | uranium_nuclear | smr_advanced_reactors | — | nuclear_energy (static) | **nuclear_energy** | **SMRs & Advanced Reactors** | nuclear_energy (static additional) | ✓ | Same iteration-order issue; smr_advanced_reactors is Oklo's primary identity |
| ONTO | Onto Innovation — semicon metrology | semicap_equipment, substrates_packaging | semicap_equip | — | test_measurement | semicap_equip (themes_page) | semicap_equip | packaging_substrates (static) | ✗ | packaging_substrates already in static universe; no DB addition needed |
| OSS | One Stop Systems — rugged compute | ai_networking | servers_compute_systems | Servers & Compute Systems | datacenter_infra (LLM) | servers_compute_systems (cat_override wins) | servers_compute_systems | — | ✗ | Cat_override correctly overrides |
| QS | QuantumScape — solid-state battery | lithium_battery | battery_tech_storage | — | battery_tech_storage | battery_tech_storage | battery_tech_storage | — | ✗ | Correct |
| RDDT | Reddit — social software platform | ai_networking | software | Software | ai_networking (LLM, deprecated) | software (cat_override wins) | software | — | ✗ | Cat_override correctly overrides stale deprecated LLM |
| SATS | EchoStar — satellite broadband | ai_networking | satellite_comms | — | — | satellite_comms (themes_page) | satellite_comms | — | ✗ | Correct |
| SILC | SILC Technologies — silicon photonics | photonics_lasers | networking_fabric_infra | Networking & Fabric Infrastructure | ai_networking (LLM, deprecated) | networking_fabric_infra (cat_override wins) | networking_fabric_infra | — | ✗ | Cat_override correctly overrides stale deprecated LLM |
| **SMR** | NuScale Power — small modular reactor | uranium_nuclear | smr_advanced_reactors | — | nuclear_energy (static) | **nuclear_energy** | **SMRs & Advanced Reactors** | nuclear_energy (static) | ✓ | Same iteration-order issue as OKLO |
| TRT | Trio-Tech — burn-in/test services | semicap_equipment, substrates_packaging | test_measurement | Test & Measurement | semicap_equipment (foreign_alias, deprecated) | test_measurement (cat_override wins) | test_measurement | — | ✗* | Resolver correct via cat_override; foreign_alias_map stale entry updated in code |
| **TSM** | TSMC — foundry + advanced packaging | substrates_packaging | semiconductors | — | semiconductors | semiconductors | semiconductors | **packaging_substrates (added)** | ✓ | CoWoS/SoIC advanced packaging is meaningful thematic exposure; added as intentional additional |
| UEC | Uranium Energy Corp — uranium mining | uranium_nuclear | uranium_nuclear_fuel | — | — | uranium_nuclear_fuel (themes_page) | uranium_nuclear_fuel | — | ✗ | Correct |
| URG | Ur-Energy — uranium mining | uranium_nuclear | uranium_nuclear_fuel | Uranium Mining & Nuclear Fuel | uranium_nuclear_fuel | uranium_nuclear_fuel | uranium_nuclear_fuel | — | ✗ | Correct |
| **UUUU** | Energy Fuels — uranium + rare earth | uranium_nuclear | rare_earth, uranium_nuclear_fuel | — | nuclear_energy (static) | **nuclear_energy** | **rare_earth** | uranium_nuclear_fuel (intentional) | ✓ | Migration intent was rare_earth primary; resolver returned nuclear_energy due to iteration order |
| **VIAV** | Viavi Solutions — optical test equipment | ai_networking | test_measurement | — | photonics_optical (static) | **photonics_optical** | **test_measurement** | photonics_optical (static) | ✓ | Migration added test_measurement as replacement; resolver returned static-universe photonics_optical |
| VSAT | ViaSat — satellite broadband | ai_networking | space | Space Economy | ai_networking (LLM, deprecated) | space (cat_override wins) | space | satellite_comms (static) | ✗ | Cat_override correctly overrides; satellite_comms present as static additional |

---

## 4. Corrections Applied

### 4a. DB Corrections (via `atomic_taxonomy_write_db`, `correct_theme_assignments_v1.py`)

| Ticker | Membership ops | Category set |
|--------|---------------|-------------|
| BAND | remove memory_storage, add cloud_software | Cloud Software |
| ASPI | — | Uranium Mining & Nuclear Fuel |
| IMSR | — | SMRs & Advanced Reactors |
| UUUU | — | Rare Earth Elements |
| AMAT | — | Semiconductor Equipment |
| OKLO | — | SMRs & Advanced Reactors |
| SMR | — | SMRs & Advanced Reactors |
| LEU | — | Uranium Mining & Nuclear Fuel |
| VIAV | — | Test & Measurement |
| MXL | — | Data Center Connectivity & Interconnect Silicon |
| TSM | add packaging_substrates | — |

All 11 succeeded. `atomic_taxonomy_write_db` used exclusively. No direct SQL mutations.

### 4b. Source Code Corrections

**`backend/services/category_overrides.py`** — `_SEED_OVERRIDES`:
- ASPI: `"Uranium & Nuclear Energy"` → `"Uranium Mining & Nuclear Fuel"` (**critical**: seed runs on every server restart and would have re-overwritten the DB fix)
- IMSR: `"Uranium & Nuclear Energy"` → `"SMRs & Advanced Reactors"` (**critical**: same reason)

**`backend/services/theme_ticker_mapper.py`** — `_FOREIGN_ALIAS_MAP`:
- ASPI: `("Nuclear / Grid", "uranium_nuclear")` → `("Uranium Mining & Nuclear Fuel", "uranium_nuclear_fuel")` — stale entry pointing at deprecated theme_id
- TRT/AIM:TRT: `("Semi Equipment & Materials", "semicap_equipment")` → `("Test & Measurement", "test_measurement")` — stale entry pointing at deprecated theme_id

**`backend/data/llm_theme_overrides.json`**:
- BAND: updated from `datacenter_infra` to `cloud_software` (wrong theme)
- IMSR: entry removed (carried deprecated `uranium_nuclear` theme_id)

---

## 5. Before/After DB Rows — Changed Tickers

### BAND
| State | theme_ticker_overrides (active) | watchlist_category_overrides |
|-------|--------------------------------|------------------------------|
| Before | memory_storage (add) | "Memory & Storage" |
| After | cloud_software (add) | "Cloud Software" |

### ASPI
| State | cat override |
|-------|-------------|
| Before | "Uranium & Nuclear Energy" (deprecated display name) |
| After | "Uranium Mining & Nuclear Fuel" |

### IMSR
| State | cat override |
|-------|-------------|
| Before | "Uranium & Nuclear Energy" |
| After | "SMRs & Advanced Reactors" |

### UUUU
| State | cat override | active memberships |
|-------|-------------|--------------------|
| Before | — (none) | rare_earth, uranium_nuclear_fuel |
| After | "Rare Earth Elements" | rare_earth, uranium_nuclear_fuel |

### AMAT
| State | cat override | active memberships |
|-------|-------------|--------------------|
| Before | — (none) | semicap_equip (+ semiconductors from static) |
| After | "Semiconductor Equipment" | semicap_equip |

### OKLO / SMR / LEU / VIAV / MXL
| Ticker | Before (cat override) | After |
|--------|----------------------|-------|
| OKLO | — | "SMRs & Advanced Reactors" |
| SMR | — | "SMRs & Advanced Reactors" |
| LEU | — | "Uranium Mining & Nuclear Fuel" |
| VIAV | — | "Test & Measurement" |
| MXL | — | "Data Center Connectivity & Interconnect Silicon" |

### TSM
| State | active memberships |
|-------|-------------------|
| Before | semiconductors |
| After | semiconductors, packaging_substrates |

---

## 6. Post-State Validation

### Resolver — All 40 Tickers Post-Correction

| Ticker | Resolved Name | Theme ID | Source |
|--------|--------------|----------|--------|
| AAOI | Optical Interconnects | optical_interconnects | manual_override |
| ABSI | Biotech | biotech | manual_override |
| ACLS | Semiconductor Equipment | semicap_equip | canonical_map |
| ADTN | Networking & Fabric Infrastructure | networking_fabric_infra | manual_override |
| AEHR | Test & Measurement | test_measurement | manual_override |
| AMAT | Semiconductor Equipment | semicap_equip | manual_override ✓ |
| AMCR | Advanced Materials | advanced_materials | canonical_map |
| ASPI | Uranium Mining & Nuclear Fuel | uranium_nuclear_fuel | manual_override ✓ |
| BAND | Cloud Software | cloud_software | manual_override ✓ |
| CEG | Nuclear Utilities & Operators | nuclear_utilities_operators | manual_override |
| CIEN | Optical Interconnects | optical_interconnects | canonical_map |
| DNN | Uranium Mining & Nuclear Fuel | uranium_nuclear_fuel | themes_page_membership |
| ELVA | Battery Technology & Energy Storage | battery_tech_storage | manual_override |
| ENVX | Battery Technology & Energy Storage | battery_tech_storage | canonical_map |
| FN | Photonics & Optical Systems | photonics_optical | canonical_map |
| GLW | Optical Components & Lasers | optical_components_lasers | manual_override |
| IMSR | SMRs & Advanced Reactors | smr_advanced_reactors | manual_override ✓ |
| KLAC | Semiconductor Equipment | semicap_equip | canonical_map |
| LAC | Lithium | lithium | canonical_map |
| LEU | Uranium Mining & Nuclear Fuel | uranium_nuclear_fuel | manual_override ✓ |
| LPTH | Optical Components & Lasers | optical_components_lasers | manual_override |
| MAXX | Advanced Materials | advanced_materials | themes_page_membership |
| MXL | Data Center Connectivity & Interconnect Silicon | dc_connectivity_silicon | manual_override ✓ |
| NNE | SMRs & Advanced Reactors | smr_advanced_reactors | manual_override |
| OCC | Optical Components & Lasers | optical_components_lasers | themes_page_membership |
| OKLO | SMRs & Advanced Reactors | smr_advanced_reactors | manual_override ✓ |
| ONTO | Semiconductor Equipment | semicap_equip | themes_page_membership |
| OSS | Servers & Compute Systems | servers_compute_systems | manual_override |
| QS | Battery Technology & Energy Storage | battery_tech_storage | canonical_map |
| RDDT | Software | software | manual_override |
| SATS | Satellite Communications | satellite_comms | themes_page_membership |
| SILC | Networking & Fabric Infrastructure | networking_fabric_infra | manual_override |
| SMR | SMRs & Advanced Reactors | smr_advanced_reactors | manual_override ✓ |
| TRT | Test & Measurement | test_measurement | manual_override |
| TSM | Semiconductors | semiconductors | canonical_map |
| UEC | Uranium Mining & Nuclear Fuel | uranium_nuclear_fuel | themes_page_membership |
| URG | Uranium Mining & Nuclear Fuel | uranium_nuclear_fuel | manual_override |
| UUUU | Rare Earth Elements | rare_earth | manual_override ✓ |
| VIAV | Test & Measurement | test_measurement | manual_override ✓ |
| VSAT | Space Economy | space | manual_override |

### DB Checks
- Active deprecated theme_ticker_overrides rows: **0**
- BAND active memberships: `[cloud_software]`
- BAND category: "Cloud Software"
- ASPI category: "Uranium Mining & Nuclear Fuel"
- IMSR category: "SMRs & Advanced Reactors"
- UUUU category: "Rare Earth Elements"
- TSM active memberships: `[semiconductors, packaging_substrates]`

### Theme RS / Options Flow
- ENRICHED_THEME_RS_UNIVERSE: 104 nodes, 0 deprecated (unchanged)
- GET /api/themes/list → 104 themes, 0 deprecated
- GET /api/options-flow/sectors → 11 sector groups, 0 deprecated

### Write-Rejection Gate (unchanged)
- POST /api/themes/admin/memberships with deprecated theme_id → HTTP 422 ✓

---

## 7. Tests

```
324 passed in 5.18s
```

All pre-existing taxonomy/watchlist/theme tests pass.

---

## 8. Files Changed

| File | Change |
|------|--------|
| `backend/migrations/correct_theme_assignments_v1.py` | New idempotent correction script (DB writes via atomic_taxonomy_write_db) |
| `backend/services/category_overrides.py` | Fixed `_SEED_OVERRIDES`: ASPI + IMSR display names (critical — seed overwrites DB on restart) |
| `backend/services/theme_ticker_mapper.py` | Fixed `_FOREIGN_ALIAS_MAP`: ASPI → `uranium_nuclear_fuel`; TRT/AIM:TRT → `test_measurement` |
| `backend/data/llm_theme_overrides.json` | BAND updated to `cloud_software`; IMSR stale deprecated entry removed |

---

## 9. git diff --check

CLEAN (no whitespace errors)

---

## 10. Commit SHA

`9a0ad8cd` — "fix: correct migrated theme assignments"

---

## 11. No-Change Tickers (32 of 40)

AAOI, ABSI, ACLS, ADTN, AEHR, AMCR, CEG, CIEN, DNN, ELVA, ENVX, FN, GLW, KLAC, LAC, LPTH, MAXX, NNE, OCC, ONTO, OSS, QS, RDDT, SATS, SILC, TRT (code cleanup only), UEC, URG, VIAV\*, VSAT  
(\*VIAV had a cat_override added, counted in changed tickers above)
