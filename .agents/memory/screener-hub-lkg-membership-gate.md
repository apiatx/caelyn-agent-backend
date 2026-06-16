---
name: Screener Hub LKG membership gate
description: LKG leaders are ranking/discovery signals only for pure_subtheme themes; they never grant standalone universe membership.
---

## The rule
For `pure_subtheme` themes, `lkg_leaders` are **ranking/enrichment signals only**.
A ticker that appears in the LKG map but is NOT already a seed, manual_include,
or theme-ETF holding will NOT be admitted to the universe on LKG alone.

Non-pure_subtheme themes (parent_rollup, curated_seed_core) are unchanged —
LKG may still contribute new candidates there.

## Why
LKG leaders reflect momentum/relative-strength ranking, not thematic membership.
Allowing LKG-only admission for pure_subthemes lets momentum leaders from adjacent
sectors slip into tight specialist themes (e.g. a broad semiconductor name appearing
in quantum or photonics because it was recently top-ranked).

## How to apply

### Source B (screener_hub_service.py ~line 2035)
```python
# Always record LKG signal for ranking/enrichment context
sources_by_symbol.setdefault(su, []).append("lkg_leaders")
# pure_subtheme: signal recorded; do NOT grant universe membership
if _theme_type == "pure_subtheme":
    continue
# Non-pure_subtheme: LKG may introduce new candidates
seen_dynamic.add(su)
lkg_syms.append(su)
```

### Allowed standalone membership proof (pure_subtheme)
1. `manual_include` / `seed` (in `_seed_tickers` / config)
2. Theme-specific ETF holding (Source A)
3. FMP screener candidate that passes strong-keyword gate (Source C)

### NOT standalone membership proof
- `lkg_leaders`
- `options_overlap`
- `social_overlap`
- `watchlist_overlap`

These are all recorded in `sources_by_symbol` for enrichment/ranking context
but cannot bring a ticker into the universe on their own.

## Validation check
After any rebuild of pure_subtheme themes, confirm:
```python
assert all(
    row["membership_source"] != "lkg"
    for row in rows
    if theme_type == "pure_subtheme"
)
```
The validated validation script in `/tmp/weak_kw_report_lkg.txt` confirmed
ZERO lkg-only rows across all 13 pure_subtheme themes after this guard was added.
