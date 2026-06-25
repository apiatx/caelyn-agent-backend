---
name: Sectors off-hours false confirmed_no_options
description: Tradier returns empty expiration chains during postmarket/premarket/off-hours/weekend, causing legitimate optionable tickers to be falsely tagged confirmed_no_options.
---

## The rule
`update_no_options_from_expiry_cache` must only write to `_NO_OPTIONS_CACHE_KEY` during `get_session() == "regular"` (09:30-16:00 ET). All other sessions return empty chains for tickers that definitely have options.

**Why:** Options only trade during regular market hours. Postmarket/premarket/off-hours Tradier expiry calls return empty lists for ALL stocks, not just option-less ones. Trusting those results incorrectly adds large-caps like AKAM, DELL, BTDR to the no-options set.

**How to apply:** The guard is in `update_no_options_from_expiry_cache` (options_theme_supplement.py). If you ever touch that function, keep `if _get_session() != "regular": return` at the top.

## Secondary guard
`update_supplement_cache` Guard 1: a row with `scan_result="confirmed_no_options"` must never overwrite ANY existing entry in either fresh supplement or LKG cache. The check reads both `_SUPPLEMENT_CACHE_KEY` and `_SUPPLEMENT_LKG_CACHE_KEY` before writing. This is the defense-in-depth layer if the session gate somehow passes.

## Guard 2 (data poisoning)
Neutral/pending coverage rows (`premium=0.0`) must never overwrite existing rows that have real premium data (`premium > 0` or direct `call_premium`/`put_premium` fields). The master screener writes neutral coverage rows for tickers that showed no unusual flow in that particular pass — if the supplement cache already has real flow data for that ticker, the neutral row must be discarded.
