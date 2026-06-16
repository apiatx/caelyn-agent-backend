---
name: Screener Hub FMP dual industry taxonomy
description: FMP screener API and fundamentals profile API use different industry strings; this causes the industry leakage filter to drop all seed rows for pure sub-themes.
---

## The rule
Seeds and `manual_include` tickers MUST bypass the thematic industry leakage filter.
For FMP-sourced rows, use `scr_meta.get("industry")` (screener taxonomy) as the
primary check — not `row_industry` from the fundamentals cache.

## Why
FMP exposes two different industry taxonomies:
- **Screener API** (`/api/v3/stock-screener`): returns e.g. `"Semiconductor Equipment & Materials"`
- **Profile/fundamentals API** (`/api/v3/profile`, `/api/v3/key-metrics`): returns e.g. `"Semiconductors"`

`_thematic_allowed_industries` is built from `fmp_industries` in `theme_fmp_industry_map.json`,
which uses screener taxonomy strings. `row_industry` in the row-build loop comes from the
fundamentals cache (profile taxonomy). For ASML, AMAT, LRCX, KLAC, TER etc. the cache
stores `"Semiconductors"` but the allowed set contains only `"Semiconductor Equipment & Materials"` →
ALL rows fail the filter → 0 rows returned, even though the snapshot has 18 valid seeds.

The same mismatch hits photonics_lasers and quantum (and would affect any future pure
sub-theme whose FMP profile industry differs from its screener industry).

## How to apply
The fix lives in the thematic industry leakage filter block (~line 4155 of screener_hub_service.py):

```python
_filter_industry = scr_meta.get("industry") or row_industry
if (
    tab == "thematic"
    and _thematic_allowed_industries
    and _filter_industry
    and membership_source not in ("seed", "manual_include")   # ← seeds bypass
):
    if _filter_industry not in _thematic_allowed_industries:
        continue
```

Never remove the `membership_source not in ("seed", "manual_include")` guard.
Never use bare `row_industry` for the FMP-screener path; always prefer `scr_meta.get("industry")`.
