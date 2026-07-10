---
name: Confluence V2 multi-archetype selection pattern
description: How to add a second Trade Alignment archetype to confluence_v2_service.py without disturbing THEME_ALIGNMENT or Actionability
---

When adding a new Trade Alignment archetype (e.g. ELITE_ASSET_REBOUND) alongside the existing THEME_ALIGNMENT archetype in `backend/services/confluence_v2_service.py`:

- Compute each archetype fully independently (own gates, own weighted score). Never average or blend component scores across archetypes.
- Selection: pick the higher-scoring *available* archetype; ties favor the incumbent (THEME_ALIGNMENT), since it was already the production default before the new archetype existed.
- To keep `actionability_service.compute_actionability` (which is archetype-agnostic and reads generic `trade_alignment_*`/`theme_alignment_score`/`options_alignment_score`/`catalyst_alignment_available` keys) working correctly: start from the THEME_ALIGNMENT fields dict (preserves all its component keys), and only overwrite the top-level `trade_alignment_score/available/archetype/grade/reason_codes` keys when the new archetype wins. Do not touch the other keys.
- Preserve the original THEME_ALIGNMENT trade score/availability under new alias keys (e.g. `theme_alignment_trade_score/available`) before overwriting, so it's never lost.
- Add `trade_alignment_archetype_scores` (dict of archetype→score-or-None) and `trade_alignment_selected_reason` for observability.

**Why:** This makes the new archetype purely additive — when it's not selected (the common case), THEME_ALIGNMENT's output and Actionability's behavior are byte-for-byte unchanged. Only symbols where the new archetype legitimately outscores THEME_ALIGNMENT see any behavior change downstream.

**How to apply:** Any future third+ archetype should follow the same pattern — compute independently, compare scores generically via a scores dict, and only ever override the generic top-level `trade_alignment_*` fields on selection.
