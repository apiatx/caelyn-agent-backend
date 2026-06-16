---
name: Screener Hub seed ticker tagging
description: Why seed tickers need to appear in BOTH theme_fmp_industry_map.json AND theme_rs_universe.py, and the priority-ordered membership_source derivation rule.
---

## Rule 1 — Both config files must be in sync
Any ticker in `seed_tickers` (theme_fmp_industry_map.json) that is NOT also in `candidate_symbols` (theme_rs_universe.py) will be added to the final universe symbol list but will have no `sources_by_symbol` entry. At query time this causes `membership_source="unknown"`.

**Why:** `sources_by_symbol` is only populated inside the `candidate_syms` loop (Source F). Seed tickers are prepended to `combined` AFTER that loop, bypassing the tagging.

**How to apply:**
1. When adding a new seed ticker to theme_fmp_industry_map.json, also add it to theme_rs_universe.py `candidate_symbols`.
2. The defensive loop after `combined = _seed_tickers + combined` guarantees any seed not in sources_by_symbol gets `["static_seed"]` — do NOT remove it.
3. After a config change, run `POST /api/admin/screener-hub/rebuild?tab=thematic&theme=<key>&force=true`.

## Rule 2 — membership_source uses priority-ordered scan, not disc_src[0]
`discovery_sources` is populated in discovery order (lkg_leaders is added before static_seed). Using `disc_src[0]` causes LKG to win over seed for RS-leader seed tickers.

**Why:** `membership_source` answers "why this ticker belongs in this theme." `lkg_leaders` is a momentum/ranking signal, not a theme membership signal.

**Priority order (highest → lowest) — implemented as `_MEMBERSHIP_PRIORITY` list:**
1. `manual_include`
2. `static_seed` ← always wins for explicitly configured seed tickers
3. `etf:<*>` ← theme-specific ETF holding
4. `fmp_screener:*` / `fmp_peers` ← FMP industry screener
5. `lkg_leaders` / `lkg:<*>` ← only if no stronger source present
6. `social_consensus`, `chain_reaction`, `watchlist_portfolio`

**How to apply:** Do NOT revert to `disc_src[0]`. The priority scan is intentional and correct. `lkg_leaders` stays in `discovery_sources` for momentum context but must not override seed/ETF membership reasons.
