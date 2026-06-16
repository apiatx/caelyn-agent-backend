---
name: Screener Hub weak/strong keyword gate
description: For pure_subtheme FMP candidates, weak keywords alone → soft-downgrade (low-confidence), not hard-drop. Seeds/ETF bypass. Specialist themes are intentionally seed-only.
---

## The rule
For `pure_subtheme` themes that define `weak_keywords` in config, an FMP screener
candidate that matches **only** weak keywords (no strong keyword) is **soft-downgraded**:
- `membership_confidence = "low"`
- `theme_role = "emerging"`
- `membership_reason` prefixed with `"weak proof: <keywords>"`

It is **NOT hard-dropped** — the row appears in the response as a low-conviction signal.

Hard-drops still apply to `exclude_tickers` (e.g. VISN) and `manual_exclude` lists.

Seeds and theme-ETF holdings bypass this gate entirely (trusted sources).
LKG leaders do NOT bypass the gate (see lkg-membership-gate.md).

## Why soft-downgrade instead of hard-drop
Hard-drop on weak-only FMP candidates collapsed cloud_software from 66→60 rows and
made all specialist themes seed-only with no FMP additions. Soft-downgrade restored
24 FMP low-confidence rows to cloud_software (60→68) while still marking them as
low-conviction signals. The weak/strong split prevents VISN-style false positives
without silently hiding adjacent candidates.

## How to apply

### Config (`theme_fmp_industry_map.json`)
Each pure_subtheme may define `weak_keywords` (a subset of `required_any_keywords`):
```json
{
  "theme_type": "pure_subtheme",
  "required_any_keywords": ["optical", "laser", "photonic", "fiber optic", ...],
  "weak_keywords": ["optical", "infrared", "coherent"]
}
```
Strong keywords = `required_any_keywords` minus `weak_keywords`.

### Code (`screener_hub_service.py` — semantic proof gate ~line 2153)
```python
# _weak_only=True flag is set on candidate if all matched keywords are weak
if _weak_kws_set and _theme_type == "pure_subtheme":
    _strong_matched = [kw for kw in _all_matched_kws if kw not in _weak_kws_set]
    if not _strong_matched:
        cand["_weak_only"] = True   # soft-downgrade, not continue
    else:
        cand["_kw_proof"] = _strong_matched[0]
```
In the row-build loop: `_is_weak_fmp = scr_meta.get("_weak_only")` → when True,
sets `membership_confidence="low"` and `theme_role="emerging"`.

## Specialist themes are seed-only by design
Themes like `semicap_equipment`, `photonics_lasers`, `substrates_packaging`,
`quantum`, `drones` produce **zero net-new FMP candidates** beyond seeds because:
1. FMP screener for their core industry returns only companies already seeded
2. Smaller legitimate companies are often mis-classified by FMP into the broader
   "Semiconductors" bucket (FMP dual taxonomy) — appear in parent `semiconductors` theme
3. This is **correct** — the seed list IS the complete pure-play universe for these niches

## Row-floor rescue logging
When `snapshot_symbols_count >= 10` but `rows_after_filters < 5`, a structured log fires:
```
[SCREENER_HUB] row_floor_rescue theme=X snap_symbols=N rows_after_filters=M
    rescued_count=R pass_reason='row_floor_rescue'
```
Also sets `screen_quality.row_floor_rescue=True` in the API response.

## Known weak terms per theme
| Theme | Weak keywords |
|-------|--------------|
| ai_networking | network, communications, connectivity, high-speed, packet, dsp, interconnect |
| photonics_lasers | optical, infrared, coherent |
| power_cooling | cooling, thermal, refrigeration |
| datacenter_infra | server, computing infrastructure |
| cybersecurity | security, identity, threat, surveillance, intelligence |
| defense | aerospace, government services, intelligence, surveillance, reconnaissance |
| robotics_automation | automation, autonomous, computer vision |
| semicap_equipment | fab, inspection |
| substrates_packaging | assembly, encapsul |
| drones | airborne, uncrewed |
| quantum | quantum (alone) |
| memory_storage | memory, storage, flash |
| cloud_software | cloud, platform, software, enterprise software, application/infrastructure/business/subscription software |
