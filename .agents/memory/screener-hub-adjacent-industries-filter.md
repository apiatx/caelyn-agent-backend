---
name: Screener Hub adjacent_industries leakage filter
description: _thematic_allowed_industries must include both fmp_industries and adjacent_industries; only having fmp_industries silently drops all Source C2 and Source P rows in the GET response
---

The row-build leakage filter (screener_hub_service.py ~line 4070) builds `_thematic_allowed_industries` from `fmp_industries` in the theme config. Source C2 (adjacent-industry FMP screener) and Source P (profile-based discovery from Neon) both produce candidates whose `scr_meta["industry"]` is from `adjacent_industries` — a separate config key. Without including `adjacent_industries` in the allowed set, every non-seed row from these two sources is silently `continue`-d out of the response.

**Why:** The FMP screener taxonomy (fmp_industries) and profile taxonomy (adjacent_industries) name industries differently; the leakage filter gate uses `_filter_industry = scr_meta.get("industry") or row_industry` which matches the screener taxonomy. Adjacent-industry candidates store their source industry in scr_meta, so the mismatch causes a drop.

**How to apply:** Whenever building `_thematic_allowed_industries`, always union both lists:
```python
_thematic_allowed_industries = set(
    (_tind_entry.get("fmp_industries") or [])
    + (_tind_entry.get("adjacent_industries") or [])
)
```
Seeds (`membership_source in ("seed", "manual_include")`) bypass this filter and are always safe.

**Symptom:** GET /api/screener-hub returns only seed/core rows despite the Neon snapshot audit showing watch_candidate/adjacent_discovery rows. `used_static_fallback: True` and `query_cache_status: miss_stored` with low `row_count` matching only seed count are the giveaways.
