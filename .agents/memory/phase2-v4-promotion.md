---
name: Phase 2 V4 Promotion — canonical field mapping
description: V4 promoted to canonical caelyn_confluence_* fields; sort order changed; component breakdown first-class; actionability promoted
---

## Rule
After Phase 2 promotion, the canonical Caelyn Confluence fields are sourced from V4:
- `caelyn_confluence_score` = `caelyn_confluence_v4_score` (total, up to 115)
- `caelyn_confluence_normalized_score` = V4 normalized (0-115, corrects for missing signals)
- `caelyn_confluence_confidence_score` = V4 confidence (0-100)
- `caelyn_confluence_bucket` = V4 bucket
- `caelyn_confluence_reason_codes` = V4 reason codes
- `actionability_state` = V4 actionability (legacy preserved as `legacy_actionability_state`)
- `trade_alignment_score` preserved for existing consumers; aliased under `legacy_trade_alignment_*`

## Component fields (first-class on every alignment row)
theme_alignment_points, stage_quality_score/points, options_alignment_points,
options_status/snapshot_status/as_of, entry_risk_reward_points,
catalyst_alignment_points/status, investment_alignment_points,
social/theme_policy/prediction_market/whale_insider/bottleneck_bonus_points

## Sort order
Retained snapshot sorted by `caelyn_confluence_score` (V4), not `trade_confluence_score`.
`confluence_rank` is now the V4 rank.

**Why:**
Phase 2 promotion — V4 scores replace the legacy composite as the authoritative signal.
The V4 engine normalizes for missing signals (options not scanned, catalyst unavailable)
so scores are more comparable across the universe.

**How to apply:**
Any new consumer reading `caelyn_confluence_score` gets V4. V4 debug fields still
available under `caelyn_confluence_v4_*` prefix. Do NOT re-introduce sort by
`trade_confluence_score` — it bypasses the V4 normalization.

## Phase 2.1 addendum — Display contract cleanup

### Bucket/actionability consistency rule
ACTIONABLE bucket requires actionability_state in {READY, NEAR_ACTIONABLE, WAIT_FOR_BREAKOUT}.
Applied as post-processing correction in `confluence_v2_service.py` PART 5 block.
Violation: downgrade bucket to NEAR_ACTIONABLE + append BUCKET_DOWNGRADED_ACT_CONSISTENCY reason code.
V4 engine `caelyn_confluence_v4_bucket` field retains original (for debug tracing).

### Boolean filter fields
is_actionable_setup, is_near_actionable, is_watch_for_reset, is_risk_conflict, is_investment_quality
Computed in PART 6 (confluence_v2_service.py), surfaced in watchlist_router.py.
Frontend must use these — do not re-derive from bucket/actionability strings.

### CRWD blank root cause
Phase 2 validation showed CRWD blank because the alignment endpoint was called while
the retained snapshot rebuild was still in progress — served stale row without promoted fields.
No bug in alignment logic; stale snapshot served pending rebuild completion.
