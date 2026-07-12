---
name: Options Weekend LKG resilience
description: Why disk LKG max age must be 96h, how supplement LKG feeds V4, and how the empty overwrite guard works.
---

## The rule

All three options LKG disk-age constants must be **345600s (96h / 4 days)**, not 86400s (24h):
- `_SUPPLEMENT_LKG_DISK_MAX_AGE` in `options_theme_supplement.py`
- `_SECTORS_LKG_DISK_MAX_AGE` in `options_theme_supplement.py`
- `_LKG_DISK_MAX_AGE_S` in `main.py`

**Why:** Friday-close LKG data is ~41-48h old by Sunday morning restart. The 24h limit silently rejected it, leaving all 379 V4 tickers as `not_scanned`.

## Supplement LKG is the primary V4 options source

The **supplement LKG** (`options_supplement_lkg_v1.json`, 643 tickers) contains the per-ticker `call_premium / put_premium / net_premium / scan_result` fields that `compute_options_alignment()` uses.  The **master LKG** (`options_master_lkg_v1.json`, only ~19 screener tickers) is too sparse to feed V4.

**How to apply:** When V4 shows `not_scanned` for most tickers, check `_SUPPLEMENT_LKG_DISK_MAX_AGE` first. Also verify `_load_supplement_lkg_from_disk()` completed ("Loaded 643 supplement tickers from disk" in startup logs).

## get_combined_ticker_data() disk fallback order

When both in-memory caches are empty (e.g., weekend restart before first scan):
1. **Layer A:** supplement disk LKG (`options_supplement_lkg_v1.json`) → ~643 tickers
2. **Layer B:** master disk LKG (`options_master_lkg_v1.json`) → ~19 tickers (for screener results)

Only fires when `combined` is empty after memory + watchlist-cache layers.

## Age-based status tagging

- `age_s < 86400` → `lkg_market_closed` (same session data)
- `86400 ≤ age_s < 345600` → `stale_but_usable` (Friday-close data served over weekend)
- `age_s ≥ 345600` → rejected

Stale loads use extended in-memory TTL (`_LKG_DISK_STALE_CACHE_TTL = 345600`) so data persists until Monday scan overwrites it.

## Empty overwrite guard

Both `_save_master_lkg_to_disk()` and `_save_supplement_lkg_to_disk()` check before writing:
- If existing LKG has > 5 (master) or > 50 (supplement) tickers
- AND new scan result is < 50% of existing count
- AND `get_session()` is not `regular/pre/post`

→ Skip write, log `PRESERVED_LAST_GOOD_OPTIONS_LKG`

This prevents a handful of Saturday off-hours scan results from erasing a 643-ticker Friday LKG.

## Validated result (weekend restart)

- `not_scanned: 379 → 86` (77% reduction)
- `available_cached: 0 → 293`
- Confidence avg: 67.7 → 79.5
- ACTIONABLE + IQ tier1 signals: 0 → 4
