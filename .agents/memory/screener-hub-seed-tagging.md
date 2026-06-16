---
name: Screener Hub seed ticker tagging
description: Why seed tickers need to appear in BOTH theme_fmp_industry_map.json AND theme_rs_universe.py, and the defensive tagging loop that prevents unknown sources.
---

## Rule
Any ticker in `seed_tickers` (theme_fmp_industry_map.json) that is NOT also in `candidate_symbols` (theme_rs_universe.py) will be added to the final universe symbol list but will have no `sources_by_symbol` entry. At query time this causes `membership_source="unknown"` and `membership_reason="unknown"` in the row.

**Why:** `sources_by_symbol` is only populated inside the `candidate_syms` loop (Source F) at line ~2109. Seed tickers are prepended to `combined` at line ~2132 AFTER that loop, bypassing the tagging.

**How to apply:**
1. When adding a new seed ticker to theme_fmp_industry_map.json, also add it to theme_rs_universe.py `candidate_symbols`.
2. The defensive loop added after the `combined = _seed_tickers + combined` line guarantees any seed not in sources_by_symbol gets `["static_seed"]` as a safety net — do NOT remove it.
3. After a config change, run `POST /api/admin/screener-hub/rebuild?tab=thematic&theme=<key>&force=true` to regenerate the snapshot with the corrected sources_by_symbol, which also auto-expires the query cache.
