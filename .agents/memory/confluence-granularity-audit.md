---
name: Confluence Granularity Audit V4.2.6
description: Three scoring bugs found and fixed in caelyn_confluence_v42.py — catalyst ceiling, investment bucketing, catalyst dead zone
---

# Confluence Component Score Granularity Audit — V4.2.6 Fixes

**Why:** Audit found that Catalyst and Investment components were severely bucketed,
limiting their usefulness as differentiators. Catalyst was capped at 11.25/15 by design
flaw; Investment produced only 4 values for a 15-pt component.

## Bugs Fixed (all in `backend/services/caelyn_confluence_v42.py`)

### P0-A — Catalyst ceiling permanently 11.25/15 (fixed)
- `intelligence_score` is hardcoded 0 in snapshot mode (no data source)
- Old formula: `catalyst_raw = direct_score * 0.75 + 0 * 0.25` → max = 75 → pts = 11.25
- Fix: `catalyst_raw = direct_score` → max = 100 → pts = 15.0
- **When intelligence_score is eventually implemented, restore the blended formula.**
- Comment left in code: `# catalyst_raw = direct_score * 0.75 + intelligence_score * 0.25`

### P0-C — Investment 4-bucket formula replaced with continuous IQ score (fixed)
- Old: `inv_pts = round(float(strong_pillar_count) * 5.0, 2)` → only {0, 5, 10, 15}
- New: `inv_pts = round(inv_quality_score / 100.0 * 15.0, 2)` → full continuous range
- `inv_quality_score` was already computed correctly above this line; just moved it earlier
- Caused ordering inversion: ACLS (IQ=88) scored 5 pts; IONQ (IQ=63) scored 15 pts
- **After fix:** ACLS=13.2, IONQ=9.45 — correct ordering restored

### P1-A — Catalyst score-only dead zone threshold 40→20 (fixed)
- Symbols with cat_score 1–39 got near-zero pts despite meaningful signal
- 26/145 LKG symbols had cat_score 8.1–13.5, all below 40 threshold
- Fix: `direct_present = raw_val >= 20.0` (was 40.0)

## How to Apply
- Do NOT use `intelligence_score` in the formula until a real data source populates it
- `inv_quality_score` must be computed BEFORE `inv_pts` — it's now earlier in the function
- Entry/Exit is already continuous (~20 unique values per 26 symbols) — no structural fix needed

## Distribution Data (pre-fix, n=145 catalyst LKG)
- Catalyst: 4 values: {0: 12, 0.91: 3, 1.52: 23, 11.25: 107}
- Investment: 4 values: {0, 5, 10, 15}
- Entry: ~20 distinct values — healthy, no fix needed

## Remaining Proposed Fixes (not implemented — Phase B/C)
- P2-B: Support status boundary smoothing in `_score_entry_exit_v42`
- Phase B: Event tier scoring for catalyst `direct_score` (graduated, not binary)
- Phase C: Implement catalyst intelligence data pipeline
