---
name: Screener Hub weak/strong keyword gate
description: For pure_subtheme FMP candidates, weak keywords alone are insufficient proof. At least one strong keyword required. Seeds/ETF/LKG bypass entirely.
---

## The rule
For `pure_subtheme` themes that define `weak_keywords` in config, an FMP screener
candidate must match at least one **strong keyword** (= `required_any_keywords` minus
`weak_keywords`) to be admitted.  Matching only weak keywords is rejected at the gate.

Seeds, ETF holdings, and LKG leaders bypass this gate entirely — they are trusted sources.

## Why
Broad terms like "optical", "security", "automation", "server", "cloud", "software"
can match unrelated companies via the FMP screener. The trigger was VISN (Vision
Marine Technologies) entering ai_networking because "network" appeared in its company
profile. The weak/strong split prevents the same pattern across all pure_subthemes
without requiring an explicit exclude_tickers entry for every false positive.

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

### Code (`screener_hub_service.py` — semantic proof gate ~line 2123)
```python
_weak_kws_set = {k.lower() for k in (theme_cfg.get("weak_keywords") or [])}
# ...in the FMP candidate loop:
_all_matched_kws = [kw for kw in _pos_kws_lower if kw in _srch]
if not _all_matched_kws:
    continue
if _weak_kws_set and _theme_type == "pure_subtheme":
    _strong_matched = [kw for kw in _all_matched_kws if kw not in _weak_kws_set]
    if not _strong_matched:
        continue  # weak-only proof — reject
    cand["_kw_proof"] = _strong_matched[0]
else:
    cand["_kw_proof"] = _all_matched_kws[0]
```

### Guidance for adding new weak_keywords
- Add a term to `weak_keywords` when it can plausibly match companies OUTSIDE the
  theme via their FMP company name / sector / industry string alone.
- Keep `required_any_keywords` as the superset (weak terms stay there for backward compat).
- Never add seed tickers to exclude_tickers just because of a weak keyword match —
  seeds bypass the gate; only FMP candidates are gated.

## Known weak terms per theme (as of initial implementation)
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
