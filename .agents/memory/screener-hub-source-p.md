---
name: Screener Hub Source P — profile cache discovery
description: Source P design, re-scorer path, and known gotchas for the fmp_profile discovery layer
---

# Source P: Neon Profile Cache Discovery

## Design
Source P runs after Source C/C2 in `_build_thematic_universe`. It has two paths:

**Re-scorer path** (companies already in `seen_dynamic`):
- Only fires when `_existing.get("candidate_tier_override") == "watch_candidate"` AND `_pcand.get("description")` is non-empty
- Upgrades existing watch_candidates → verified_discovery/adjacent_discovery via description keyword matching
- Updates `candidate_tier_override`, `_all_matched_kws`, `_kw_proof`, `_weak_only=False`, `theme_relevance_score`

**New-candidate path** (companies not in `seen_dynamic`):
- Adds companies in theme industries that Source C/ETF/seed didn't find

## Gotchas

### expires_at grace period (fmp_cache_service.py)
`get_profiles_by_industries` uses `expires_at > NOW() - INTERVAL '30 days'` (NOT strict > NOW()).
Company descriptions rarely change — 30-day grace is intentional for discovery use cases.
`get_fundamentals` has no expiry filter (uses stale data freely); this discrepancy is expected.

### `_adj_industries` scope
`_adj_industries` is defined at Source C2 start (line ~2372), BEFORE Source P at ~2432.
Source P's `_prof_inds` = deduped union of `industries` (fmp_industries) + `_adj_industries`.

### Source C blocks re-scorer if candidate_tier_override unset
If a company's screener_meta entry lacks `candidate_tier_override` (e.g., from old snapshots),
the re-scorer condition fails. The re-scorer ONLY upgrades entries explicitly tagged watch_candidate.

**Why:** Source C now always sets `candidate_tier_override` via `score_theme_candidate`, so new
snapshots will always have it. Old snapshots may not — this is acceptable degradation.
