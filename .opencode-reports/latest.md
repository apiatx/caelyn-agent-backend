# Theme Hierarchy Registry — Task Report
**Commit:** `72960e96`  
**Date:** 2026-08-05  
**Status:** Complete — 79/79 tests passing, git diff --check clean

---

## Proven Current Architecture

### Canonical Registry
`backend/services/theme_rs_universe.py` — `THEME_RS_UNIVERSE: dict[str, dict]`  
Pure Python dict literals. **No database backing.** Static metadata additions require no schema work.

**Fields per node (pre-task):**  
`classification`, `parent_sector`, `display_name`, `proxy_type`, `proxy_symbols`, `candidate_symbols`, `sector_tags`, `keywords`, `macro_sensitivities`, `aliases` (optional)

**Node count:** 51 entries — 11 SPDR sectors + 40 themes/sub-themes.

### Theme List Endpoint
`GET /api/themes/list` (`backend/routes/themes.py` line 169)  
Serializes directly from `THEME_RS_UNIVERSE`. Additive fields only need adding to the serialization dict — no endpoint logic changes required.

**Pre-task response fields:** `theme_id`, `display_name`, `classification`, `parent_sector`, `proxy_type`, `proxy_symbols`, `candidate_symbols`, `sector_tags`, `keywords`, `macro_sensitivities`

**Post-task additions (additive):** `parent_theme_id`, `rollup_sector_ids`

### Watchlist Ticker Row (Skeleton Path)
`backend/services/watchlist_router.py` lines 1427–1462

**Pre-task theme fields per row:**
| Field | Source |
|-------|--------|
| `canonical_theme_id` | `resolve_primary_theme_for_ticker()` — **equivalent to `primary_theme_id`** |
| `canonical_theme_name` | same |
| `theme_source` | same |

**Absent pre-task:** `sector_id`, `primary_theme_id`, `theme_ids`, `subtheme_ids`

**Post-task additions (additive, no new persistence path):**
| Field | Source |
|-------|--------|
| `sector_id` | `THEME_RS_UNIVERSE[canonical_theme_id]["parent_sector"]` — zero DB calls |
| `primary_theme_id` | alias for `canonical_theme_id` — zero cost |
| `theme_ids` | primary + active "add" rows from batch `get_theme_ticker_overrides()` |
| `subtheme_ids` | subset of `theme_ids` where registry node has `parent_theme_id` set |

One batch `get_theme_ticker_overrides()` call per watchlist load. Not per-ticker. Existing infrastructure — no new persistence.

### Additional Memberships (pre-task)
`theme_ticker_overrides` Neon table. Accessible via `get_theme_ticker_overrides()` in `pg_storage.py`. Columns: `theme_id`, `symbol`, `action`, `source`, `note`, `created_by`. The `_get_ticker_theme_memberships()` helper in `routes/themes.py` already batches all rows and returns `additional_theme_memberships` — the `watchlist_router` skeleton was simply not using it.

---

## Taxonomy Metadata Added

### New Registry Fields (additive — all `dict.get()` safe for existing consumers)

```
parent_theme_id   : str | None  — canonical ID of parent theme; null for sectors and parent themes
rollup_sector_ids : list[str]   — canonical sector IDs for inclusive sector filtering
```

---

## Classification Changes

| ID | Before | After | Reason |
|----|--------|-------|--------|
| `metals_mining` | `sub_theme` | `theme` | parent of gold/silver/copper/rare_earth |
| `semiconductors` | `sub_theme` | `theme` | parent of memory/semicap/substrates |
| `oil_gas` | `sub_theme` | `theme` | parent of lng_gas/oil_services |
| `software` | `sub_theme` | `theme` | parent of cloud_software/cybersecurity |
| `defense` | `sub_theme` | `theme` | parent of drones |

**Backward compat:** `theme_ticker_mapper.py` reads `classification` as a stored label only (`cls_val = meta.get("classification", "theme")`). No branching on "sub_theme" vs "theme". Safe to change.

---

## parent_theme_id Assignments

| Child ID | parent_theme_id | Parent display name |
|----------|-----------------|---------------------|
| `gold` | `metals_mining` | Metals & Mining |
| `silver` | `metals_mining` | Metals & Mining |
| `copper_miners` | `metals_mining` | Metals & Mining |
| `rare_earth` | `metals_mining` | Metals & Mining |
| `memory_storage` | `semiconductors` | Semiconductors |
| `semicap_equipment` | `semiconductors` | Semiconductors |
| `substrates_packaging` | `semiconductors` | Semiconductors |
| `lng_gas` | `oil_gas` | Oil & Gas |
| `oil_services` | `oil_gas` | Oil & Gas |
| `cloud_software` | `software` | Software |
| `cybersecurity` | `software` | Software |
| `drones` | `defense` | Defense |

---

## rollup_sector_ids Assignments

| ID | rollup_sector_ids | Notes |
|----|-------------------|-------|
| `metals_mining` | `["materials"]` | single-sector parent |
| `semiconductors` | `["technology"]` | single-sector parent |
| `oil_gas` | `["energy"]` | single-sector parent |
| `software` | `["technology"]` | single-sector parent |
| `defense` | `["industrials"]` | single-sector parent |
| `clean_energy` | `["utilities", "industrials", "energy"]` | cross-sector; derived from existing `sector_tags` |
| `datacenter_infra` | `["technology", "utilities", "real_estate"]` | cross-sector; derived from existing `sector_tags` |

---

## Display-Label Changes + Aliases Preserved

| ID | Old display_name | New display_name | Alias added |
|----|-----------------|------------------|-------------|
| `copper_miners` | "Copper Miners" | "Copper" | `"copper_miners"` (→ "Copper Miners" via `title()` transform in mapper) |
| `rare_earth` | "Rare Earth Metals" | "Rare Earth Elements" | `"rare_earth_metals"` |

Frozen IDs unchanged. `semicap_equipment` aliases (`semicap`, `semiconductor_equipment`, `semi_equipment`, `semi_materials`, `semiconductor_materials`, `semi_equipment_and_materials`, `semicap_equipment`) all preserved.

---

## Endpoint Response Examples

### GET /api/themes/list — semiconductors node

```json
{
  "theme_id": "semiconductors",
  "display_name": "Semiconductors",
  "classification": "theme",
  "parent_sector": "technology",
  "parent_theme_id": null,
  "rollup_sector_ids": ["technology"],
  "proxy_type": "etf",
  "proxy_symbols": ["SMH", "SOXX", "XSD", "PSI"],
  "candidate_symbols": ["NVDA", "AMD", ...],
  "sector_tags": ["Technology"],
  "keywords": ["semiconductors", "chips", ...],
  "macro_sensitivities": ["AI capex", ...]
}
```

### GET /api/themes/list — memory_storage node

```json
{
  "theme_id": "memory_storage",
  "display_name": "Memory & Storage",
  "classification": "sub_theme",
  "parent_sector": "technology",
  "parent_theme_id": "semiconductors",
  "rollup_sector_ids": [],
  ...
}
```

### GET /api/themes/list — clean_energy node

```json
{
  "theme_id": "clean_energy",
  "display_name": "Clean Energy",
  "classification": "theme",
  "parent_sector": "utilities",
  "parent_theme_id": null,
  "rollup_sector_ids": ["utilities", "industrials", "energy"],
  ...
}
```

### Watchlist Skeleton Row (additive fields only)

```json
{
  "canonical_theme_id":   "memory_storage",
  "canonical_theme_name": "Memory & Storage",
  "sector_id":            "technology",
  "primary_theme_id":     "memory_storage",
  "theme_ids":            ["memory_storage", "semiconductors"],
  "subtheme_ids":         ["memory_storage"]
}
```
(`semiconductors` additional membership from `theme_ticker_overrides`; `memory_storage` is a subtheme because it has `parent_theme_id`)

---

## Tests and Results

**File:** `backend/tests/test_theme_hierarchy.py` — 407 lines, 79 tests  
**Result:** 79 passed, 0 failed, 0.10s

**Test categories:**
- `test_validate_hierarchy_clean` — live registry passes all structural checks
- `test_sector_nodes_exist[×11]` — all SPDR sector nodes valid
- `test_parent_theme_classification[×5]` — promoted nodes have classification="theme"
- `test_parent_theme_has_no_parent_theme_id[×5]` — parent themes have no parent
- Metals & Mining hierarchy (4 child assignments + rollup)
- Copper/Rare Earth display-name changes + aliases (4 tests)
- Semiconductors hierarchy (4 tests + frozen IDs + aliases preserved)
- Oil & Gas hierarchy (3 tests)
- Software hierarchy (3 tests)
- Defense hierarchy (2 tests)
- `test_space_is_independent` — space has no parent_theme_id
- Cross-sector rollup tests (clean_energy, datacenter_infra)
- `test_all_rollup_ids_are_sectors` — integrity check across all nodes
- `test_all_parent_theme_ids_exist` — referential integrity
- `test_all_parent_theme_ids_are_theme_class` — parent classification check
- `test_canonical_id_unchanged[×20]` — all 20 involved IDs unchanged
- `test_list_endpoint_fields_present` — backward compat for all pre-existing fields
- Validator negative tests (5): unknown parent, self-parent, sector-as-parent, cycle, non-sector rollup
- `test_direct_parent_member_no_subtheme` — clean_energy pattern
- `test_theme_ids_union_logic` — primary + additional membership union

**Broader existing tests:** 82/83 passing (1 pre-existing failure: `test_bootstrap_health_contract_unchanged` — subprocess cwd issue unrelated to this task, documented in session memory).

---

## Behavior Deliberately Preserved

- All canonical theme IDs unchanged (including `semicap_equipment` frozen ID)
- All existing `aliases` entries preserved and extended
- `parent_sector` and `sector_tags` fields present and unchanged on all nodes
- `ALL_PROXY_SYMBOLS` and `ALL_CANDIDATE_SYMBOLS` computed lists unaffected
- `ENRICHED_THEME_RS_UNIVERSE` in `theme_merge_layer.py` — reads from `THEME_RS_UNIVERSE`; `parent_theme_id` / `rollup_sector_ids` pass through transparently
- `theme_ticker_mapper.py` — reads `classification` as a stored label; no branch on "sub_theme" vs "theme"; safe
- `theme_resolver.py` — not touched; primary-theme resolution precedence unchanged
- Existing `theme_ticker_overrides` Neon table and `watchlist_category_overrides` — not touched
- Existing endpoint URLs — not changed
- Existing Watchlist membership authority (`public.watchlist.tickers`) — not touched
- Relative-strength calculations and baskets — not affected (use `proxy_symbols` / `candidate_symbols` which are unchanged)
- `canonical_theme_id` field still present in skeleton row (backward compat); `primary_theme_id` is an alias added alongside it

---

## Unresolved Taxonomy Ambiguities

### Space Economy (`space`)
**Design decision: intentionally independent.**  
`space` has `parent_sector="industrials"` (same as defense) and `sector_tags=["Technology", "Industrials"]` but keywords include "defense space." Repository evidence is ambiguous — space companies (RKLB, ASTS) are not primarily defense contractors, and the current registry does not group them under defense. Left independent. Report this to the frontend team.

### Photonics / Lasers (`photonics_lasers`)
**Left independent.**  
Keywords include "silicon photonics" but the node also covers industrial fiber lasers (IPGP), medical applications, and LiDAR. It is not exclusively a semiconductor-value-chain node. The spec condition ("if the current registry clearly treats it as part of the semiconductor value chain") is not met. Left independent; may be reassigned if a dedicated "Silicon Photonics" subtheme is created.

### Lithium & Battery Tech (`lithium_battery`)
Not placed under clean_energy. Battery tech spans materials (ALB, SQM — lithium mining) and industrials/EV (ENVX, QS — cell tech). The spec says "do not automatically place every battery, lithium, grid, or nuclear node under Clean Energy." Left independent; ambiguous.

### Uranium & Nuclear (`uranium_nuclear`)
Not placed under clean_energy. Nuclear straddles Utilities and Energy; the proxy (URA, URNM) tracks uranium miners, not clean-energy operators. Left independent. May need its own classification decision if a "Nuclear Energy" parent theme is created.

---

## Proposed Future Nodes (Semiconductor Value Chain)

Per spec instruction to report rather than invent:

| Proposed ID | Display Name | Would be child of |
|-------------|-------------|-------------------|
| `semiconductor_test` | Semiconductor Test | `semiconductors` |
| `foundry` | Semiconductor Foundry | `semiconductors` |
| `analog_semiconductors` | Analog & Mixed-Signal | `semiconductors` |
| `power_semiconductors` | Power Semiconductors | `semiconductors` |
| `silicon_photonics` | Silicon Photonics | `semiconductors` (if narrowed from `photonics_lasers`) |

These would require curating candidate_symbols and proxy_symbols before adding. Not implemented.

---

## Files Changed

| File | Change |
|------|--------|
| `backend/services/theme_rs_universe.py` | +152 lines: 17 node edits (classification, display_name, parent_theme_id, rollup_sector_ids, aliases) + `validate_theme_hierarchy()` (108 lines) |
| `backend/routes/themes.py` | +3 lines: `parent_theme_id` and `rollup_sector_ids` added to `/list` serialization |
| `backend/services/watchlist_router.py` | +45 lines: batch override load + 4 new skeleton row fields |
| `backend/tests/test_theme_hierarchy.py` | New file, 407 lines, 79 tests |

---

## git status -sb

```
## main...origin/main [ahead 3]
```

---

## Commit SHA and Message

```
72960e96  feat: extend canonical theme registry with parent_theme_id / rollup_sector_ids hierarchy
```

---

## Complete Committed Diff Summary

```
backend/services/theme_rs_universe.py
  clean_energy         + rollup_sector_ids: ["utilities", "industrials", "energy"]
  cloud_software       + parent_theme_id: "software"
  copper_miners          display_name: "Copper Miners" → "Copper"
                       + parent_theme_id: "metals_mining"
                       + aliases: ["copper_miners"]
  cybersecurity        + parent_theme_id: "software"
  datacenter_infra     + rollup_sector_ids: ["technology", "utilities", "real_estate"]
  defense                classification: "sub_theme" → "theme"
                       + rollup_sector_ids: ["industrials"]
  drones               + parent_theme_id: "defense"
  gold                 + parent_theme_id: "metals_mining"
  lng_gas              + parent_theme_id: "oil_gas"
  memory_storage       + parent_theme_id: "semiconductors"
  metals_mining          classification: "sub_theme" → "theme"
                       + rollup_sector_ids: ["materials"]
  oil_gas                classification: "sub_theme" → "theme"
                       + rollup_sector_ids: ["energy"]
  oil_services         + parent_theme_id: "oil_gas"
  rare_earth             display_name: "Rare Earth Metals" → "Rare Earth Elements"
                       + parent_theme_id: "metals_mining"
                       + aliases: ["rare_earth_metals"]
  semicap_equipment    + parent_theme_id: "semiconductors"
  semiconductors         classification: "sub_theme" → "theme"
                       + rollup_sector_ids: ["technology"]
  silver               + parent_theme_id: "metals_mining"
  software               classification: "sub_theme" → "theme"
                       + rollup_sector_ids: ["technology"]
  substrates_packaging + parent_theme_id: "semiconductors"
  [bottom]             + validate_theme_hierarchy() function (108 lines)

backend/routes/themes.py
  themes_list()        + "parent_theme_id": meta.get("parent_theme_id"),
                       + "rollup_sector_ids": meta.get("rollup_sector_ids", []),

backend/services/watchlist_router.py
  skeleton builder     + batch load THEME_RS_UNIVERSE + get_theme_ticker_overrides()
                       + "sector_id", "primary_theme_id", "theme_ids", "subtheme_ids"
                         computed per-ticker from existing data (no new persistence)

backend/tests/test_theme_hierarchy.py
  [new file]           79 tests covering all hierarchy invariants + negative cases
```
