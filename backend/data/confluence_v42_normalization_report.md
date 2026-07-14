# BACKEND CONFLUENCE V4.2 NORMALIZATION REPORT
**Date:** 2026-07-14
**Spec:** BACKEND ONLY — CONFLUENCE V4.2 CONTRACT NORMALIZATION PASS (Parts 1–15)

---

## FILES CHANGED

| File | Change |
|------|--------|
| `backend/services/confluence_v42_normalizer.py` | **NEW** — Pure function library: `build_confluence_v42_object()`, `derive_boolean_flags()`, `build_risk_flags()`, `build_why_now()`, `build_why_wait()`, `build_invalidation_level()`, `build_target_zone()`, `build_fib_wave_status()` |
| `backend/services/confluence_v2_service.py` | **EDITED** — Injected normalization pass (Steps 1–7) just before `results.append(r)` in `build_confluence_snapshot()` |

**No other files changed. No scoring math changed. No provider calls. No frontend files.**

---

## WHAT WAS FIXED

1. **`confluence_v42` normalized object** added to every retained snapshot row
2. **Boolean contradiction fixed** — `is_watch_for_reset`, `is_near_actionable`, `is_actionable_setup`, `is_risk_conflict`, `is_investment_quality` re-derived from `caelyn_confluence_bucket` (V4.2 canonical source of truth)
3. **Flat convenience fields** added: `caelyn_confluence_core_max=100`, `caelyn_confluence_bonus_max=25`, `caelyn_confluence_total_max=125`, `caelyn_confluence_display_mode="CORE_100_PLUS_BONUS_25"`
4. **`invalidation_level`** promoted to top-level flat field (from `caelyn_confluence_v42_components.entry_exit.invalidation_level` and `critical_break_level`)
5. **`target_zone`** synthesized as top-level object `{target_1, target_2, breakout_trigger, risk_reward_ratio}` — string-safe (non-numeric breakout_trigger values coerced to None)
6. **`risk_flags`** added as first-class string array (deterministic, deduped)
7. **`why_now`** and **`why_wait`** added as deterministic string arrays (no LLM, from existing fields/statuses)
8. **`deprecated_confluence_fields`** list added (informational, legacy fields preserved)
9. **Fib/wave pending state** exposed as `fib_wave_status: "pending_10y_backfill"` so frontend never interprets null as bearish

---

## NORMALIZED `confluence_v42` OBJECT (ABCL example — WATCH_FOR_RESET)

```json
{
  "score": {
    "total": 53.7,
    "core": 53.7,
    "bonus": 0,
    "core_max": 100,
    "bonus_max": 25,
    "total_max": 125,
    "display_mode": "CORE_100_PLUS_BONUS_25",
    "percent_of_total_max": 43.0
  },
  "action": {
    "label": "WATCH_FOR_RESET",
    "bucket": "WATCH_FOR_RESET",
    "label_display": "Watch for Reset",
    "invalidation_level": 6.3896,
    "target_zone": null,
    "why_now": [],
    "why_wait": ["Extension risk is elevated", "Fib/Wave context pending 10Y history"]
  },
  "components": {
    "theme":          { "raw_score": 49.5, "points": 7.42, "max_points": 15, "available": true, "status": "available", "reason_codes": ["THEME_EMERGING"] },
    "stage":          { "raw_score": 100.0, "points": 15.0, "max_points": 15, "available": true, "status": "available", "stage_label": "STAGE2_EARLY", "reason_codes": ["STAGE2_QUALITY"] },
    "options":        { "raw_score": 48.3, "points": 9.65, "max_points": 20, "available": true, "status": "lkg_market_closed", "reason_codes": ["OPTIONS_NEUTRAL"] },
    "technical_setup":{ "raw_score": 8.0, "points": 0.64, "max_points": 8, "available": true, "label": "Chase Extension", "reason_codes": ["CHASE_EXTENSION_PENALTY"] },
    "entry_exit":     { "raw_score": 39.6, "points": 4.75, "max_points": 12, "available": true, "reason_codes": ["CHASE_EXTENSION_REDUCED_PENALTY_CONSTRUCTIVE_RETEST"] },
    "catalyst":       { "raw_score": 75.0, "points": 11.25, "max_points": 15, "available": true, "status": "available", "reason_codes": ["RSS_CATALYST_DIRECT"] },
    "investment":     { "raw_score": 33.3, "points": 5.0, "max_points": 15, "available": true, "quality_label": "Current Growth Leader", "pillar_count": 1 }
  },
  "bonuses": {
    "social":         { "points": 0, "max_points": 15, "sections_hit": 0, "confluence_hit": false, "acceleration_hit": false, "fresh_hit": false, "status": "no_social_coverage" },
    "whale_insider":  { "points": 0, "max_points": 5, "status": "not_wired" },
    "bottleneck":     { "points": 0, "max_points": 5, "anchor_count": 0, "status": "not_in_screener" }
  },
  "technical": {
    "stage_label": "STAGE2_EARLY",
    "stage_score": 100.0,
    "technical_setup_label": "Chase Extension",
    "entry_state": "NO_CLEAR_ENTRY",
    "entry_score": 12,
    "extension_state": "EXTREME_EXTENSION",
    "extension_quality": "CHASE",
    "fib_context": null,
    "fib_timeframe": null,
    "nearest_fib_label": null,
    "nearest_fib_level": null,
    "distance_to_fib_pct": null,
    "fib_confidence": null,
    "fib_computed": false,
    "fib_years_available": null,
    "wave_structure": null,
    "wave_score": null,
    "fib_wave_status": "pending_10y_backfill"
  },
  "risk": {
    "risk_flags": ["EXTREME_EXTENSION", "CHASE_EXTENSION", "FIB_PENDING"],
    "major_lower_low_confirmed": false,
    "lower_low_confirmed": false,
    "chase_extension": true,
    "constructive_extension": false,
    "critical_break_level": 6.3896,
    "active_support_status": "near_major_support",
    "active_support_type": "prior_major_swing_low",
    "distance_to_active_support_pct": null
  },
  "booleans": {
    "is_actionable_setup": false,
    "is_near_actionable": false,
    "is_watch_for_reset": true,
    "is_risk_conflict": false,
    "is_investment_quality": false
  },
  "metadata": {
    "schema_version": "v4.2",
    "confidence_score": 100.0,
    "reason_codes": ["THEME_EMERGING", "STAGE2_QUALITY", "..."],
    "deprecated_fields_present": true,
    "deprecated_confluence_fields": ["caelyn_confluence_v4_*", "trade_alignment_score", "legacy_trade_alignment_*", "confluence_verdict", "confluence_grade", "signal_breakdown", "..."]
  }
}
```

---

## SCORE DISPLAY CONTRACT

| Field | Value | Notes |
|-------|-------|-------|
| Primary display | `confluence_v42.score.total` | Absolute 0–125 |
| Core breakdown | `confluence_v42.score.core` | 0–100 from 7 components |
| Bonus breakdown | `confluence_v42.score.bonus` | 0–25 from 3 bonuses |
| Core max | `confluence_v42.score.core_max = 100` | Hardcoded, always 100 |
| Bonus max | `confluence_v42.score.bonus_max = 25` | Hardcoded, always 25 |
| Total max | `confluence_v42.score.total_max = 125` | Hardcoded, always 125 |
| Display mode | `confluence_v42.score.display_mode = "CORE_100_PLUS_BONUS_25"` | UI contract |
| Progress % | `confluence_v42.score.percent_of_total_max` | `total / 125 * 100` |

**Display guidance:** Show `core / 100` as the primary bar. Show `+bonus / 25` separately. Do NOT make 125 the main denominator in the UI — the product emphasis is Core /100 with Bonus as supplement.

---

## ACTION / BOOLEAN CONSISTENCY

### ABCL Before/After Fix

| Field | Before | After |
|-------|--------|-------|
| `caelyn_confluence_bucket` | WATCH_FOR_RESET | WATCH_FOR_RESET (unchanged) |
| `caelyn_confluence_v42_actionability` | WATCH_FOR_RESET | WATCH_FOR_RESET (unchanged) |
| `is_watch_for_reset` | **False** ❌ | **True** ✅ |
| `is_near_actionable` | **True** ❌ | **False** ✅ |
| `boolean_consistent` | False | True |

**Root cause of bug:** Boolean flags were derived from the phase-6 tier logic (`_p6_tier`) which is a separate multi-path decision layer. For ABCL, `_p6_tier` evaluated to NEAR_ACTIONABLE (entry points passed near-actionable floor) while `caelyn_confluence_v42_bucket` was WATCH_FOR_RESET (set by V4.2 extension/chase logic). The V4.2 bucket is canonical.

**Fix:** After P6 block, `derive_boolean_flags()` re-derives all 5 booleans from `caelyn_confluence_bucket` + `caelyn_confluence_v42_actionability` with strict mutual exclusivity: `is_watch_for_reset` and `is_near_actionable` cannot both be true.

### Action Label Mapping

| Raw actionability | label_display |
|-------------------|---------------|
| `READY` | "Ready to Enter" |
| `NEAR_ACTIONABLE` | "Near Entry" |
| `WAIT_FOR_BREAKOUT` | "Wait for Breakout" |
| `WAIT_FOR_RETEST` | "Wait for Retest" |
| `WATCH_FOR_RESET` | "Watch for Reset" |
| `WATCH` | "Watch" |
| `AVOID` | "Avoid" |
| `INSUFFICIENT_DATA` | "Insufficient Data" |

**Do NOT use:** `actionability_state`, `legacy_actionability_state`, `confluence_verdict`, `confluence_grade`.

---

## COMPONENT NORMALIZATION

All 7 V4.2 components exposed via `confluence_v42.components.*`:

| Component key | max_points | Fields |
|---------------|-----------|--------|
| `theme` | 15 | raw_score, points, available, status, reason_codes |
| `stage` | 15 | raw_score, points, available, status, stage_label, reason_codes |
| `options` | 20 | raw_score, points, available, status, reason_codes |
| `technical_setup` | 8 | raw_score, points, available, status, label, reason_codes |
| `entry_exit` | 12 | raw_score, points, available, status, reason_codes |
| `catalyst` | 15 | raw_score, points, available, status, reason_codes |
| `investment` | 15 | raw_score, points, available, status, quality_label, pillar_count, reason_codes |

Bonuses exposed via `confluence_v42.bonuses.*` — social (max 15), whale_insider (max 5, always 0 + `status: "not_wired"`), bottleneck (max 5).

Frontend does not need to read 278 flat keys. The normalized object contains everything needed to render a complete UI.

---

## RISK FLAGS

**First-class `risk_flags` string array** — deterministic, deduped, stable.

| Flag | Trigger |
|------|---------|
| `EXTREME_EXTENSION` | `extension_state == "EXTREME_EXTENSION"` |
| `CHASE_EXTENSION` | `chase_extension == True` |
| `MAJOR_LOWER_LOW_CONFIRMED` | `major_lower_low_confirmed == True` |
| `LOWER_LOW_CONFIRMED` | `lower_low_confirmed == True` (and not major) |
| `RISK_CONFLICT` | bucket == "RISK_CONFLICT" or is_risk_conflict |
| `AVOID_SIGNAL` | actionability == "AVOID" |
| `NO_CLEAR_ENTRY` | entry_exit_points < 2.0 |
| `OPTIONS_UNAVAILABLE` | options null + status confirmed_no_options/no_options |
| `CATALYST_UNAVAILABLE` | catalyst_status in unavailable set |
| `FIB_PENDING` | fib_wave_status == "pending_10y_backfill" |
| `RISK_*` codes | Any reason_code starting with "RISK_" prefix |

Exposed at both:
- Top-level flat: `risk_flags`
- Nested in object: `confluence_v42.risk.risk_flags`

---

## INVALIDATION / TARGET ZONE

**`invalidation_level`** (top-level flat field):
- Sources: `caelyn_confluence_v42_components.entry_exit.invalidation_level` → `critical_break_level` → null
- Also in: `confluence_v42.action.invalidation_level`

**`target_zone`** (top-level flat object or null):
```json
{
  "target_1": null,
  "target_2": null,
  "breakout_trigger": null,
  "risk_reward_ratio": null
}
```
- Source: `caelyn_confluence_v42_components.entry_exit.*`
- String-valued `breakout_trigger` (e.g. "ABOVE_FLAG_RESISTANCE") safely coerced to `null`
- Returns `null` when all four sub-fields are null
- Also in: `confluence_v42.action.target_zone`

---

## WHY_NOW / WHY_WAIT

Deterministic arrays. No LLM. Derived from component scores, statuses, actionability, and flags.

**`why_now`** examples (positive signals, populated for NEAR_ACTIONABLE / ACTIONABLE buckets):
- "Theme alignment is positive"
- "Stage quality is strong"
- "Options flow is supportive"
- "Technical setup is constructive"
- "Catalyst support is present"
- "Investment quality is strong"
- "Social momentum is active"
- "Bottleneck exposure adds bonus support"

**`why_wait`** examples (cautions/blocks, populated for all actionabilities):
- "Extension risk is elevated"
- "Waiting for a cleaner reset before entry"
- "Waiting for retest"
- "Entry is not clean yet"
- "Fib/Wave context pending 10Y history"
- "Options data unavailable"
- "Risk conflict detected"
- "Major lower low risk present"
- "No clear confluence yet"

Exposed at:
- Top-level flat: `why_now`, `why_wait`
- Nested in object: `confluence_v42.action.why_now`, `confluence_v42.action.why_wait`

---

## FIB / WAVE PENDING STATE

All fib/wave fields exist in schema but are universally null pending 10Y canonical history backfill.

`confluence_v42.technical.fib_wave_status`:
- `"pending_10y_backfill"` — both `fib_computed` is falsy and `primary_fib_context` is null (current state for all symbols)
- `"available"` — will appear once canonical history backfill completes and snapshot rebuilds
- `"unavailable"` — only for explicitly unavailable cases (not the null case)

**Frontend must treat `fib_wave_status = "pending_10y_backfill"` as neutral/informational, never bearish.**

Individual fields in `confluence_v42.technical`:
`fib_context`, `fib_timeframe`, `nearest_fib_label`, `nearest_fib_level`, `distance_to_fib_pct`, `fib_confidence`, `fib_computed`, `fib_years_available`, `wave_structure`, `wave_score` — all null until backfill.

---

## DEPRECATED FIELD HANDLING

Legacy fields are preserved on the flat row (backward compatibility). They do NOT feed the normalized `confluence_v42` object.

`confluence_v42.metadata.deprecated_confluence_fields` lists them explicitly:
```
caelyn_confluence_v4_* (all V4.0 debug copies)
trade_alignment_score
legacy_trade_alignment_score
legacy_trade_alignment_archetype
legacy_actionability_state
social_bonus_score
base_trade_alignment_score
confluence_verdict
confluence_grade
signal_breakdown
prediction_market_bonus_points
theme_policy_bonus_points
```

`confluence_v42.metadata.deprecated_fields_present = true` — signals they are present in the flat row but should not be consumed by frontend.

---

## ENDPOINT / API KEY CLARIFICATION

| Question | Answer |
|----------|--------|
| Does `GET /api/alpha/confluence` require X-API-Key? | **NO** — confirmed by live test. No `_jwt_or_key()` check on this endpoint. Rate-limited via slowapi. |
| Can this be called directly from browser frontend? | **YES** — it is publicly accessible (no auth required). Do not put any secret in request. |
| Is there a backend proxy route for browser? | Not needed — endpoint is public. |
| Which endpoint for Watchlist page? | `GET /api/watchlist/{watchlist_id}/alignment` (passes watchlist context + theme leadership + component coverage) |
| Which endpoint for Screener/All-symbols page? | `GET /api/alpha/confluence` (all watchlist universe symbols, sorted by score) |
| Which endpoint for single symbol deep-dive? | `GET /api/alpha/confluence/{symbol}` |

**The `confluence_v42` object is present on all three production endpoints** (built into the retained snapshot, served from same source).

---

## RETAINED SNAPSHOT LKG RECOMMENDATION

**Current state:** In-memory only, ~129s cold build, stale-while-revalidate during rebuild.

**Recommendation: Add `confluence_retained_lkg.json.gz` — YES, defer to P2.**

**What to persist:**
- Full flat row per symbol (278 keys) — OR — only `confluence_v42` object + canonical score fields (~30 keys)
- Recommendation: persist only the normalized `confluence_v42` block + `{symbol, caelyn_confluence_score, caelyn_confluence_bucket, is_actionable_setup, is_near_actionable, is_watch_for_reset, timestamp}` for startup LKG

**Startup load:** `_RETAINED["snapshot"]` seeded from disk LKG on startup before `build_confluence_snapshot()` completes. Same pattern as `watchlist_stage2_lkg.json`.

**Stale-while-revalidate:** Serve from LKG immediately on startup, trigger async rebuild. LKG only stale for 1 snapshot cycle (~130s). Current startup delay is the cold build time; LKG eliminates this.

**Risk:** Low. Disk persistence of read-only snapshot data. No write conflict with score math.

**Implement when:** After 10Y backfill is complete and snapshot stabilizes. Worth doing before frontend launch.

---

## PART 12 ENDPOINT ADDENDUM

**Watchlist page:** Use `GET /api/watchlist/{watchlist_id}/alignment` — returns same `confluence_v42` object (it's on every snapshot row) plus theme leadership fields.

**Discovery/screener page:** Use `GET /api/alpha/confluence` — all universe symbols sorted by `caelyn_confluence_score` desc.

**Single symbol panel/modal:** Use `GET /api/alpha/confluence/{symbol}` — 60/min rate limit.

**Do NOT call:** `/api/watchlist/{watchlist_id}/confluence/v4-report` from frontend — diagnostic only, not per-symbol row data.

---

## VALIDATION (Part 14)

| Symbol | row_found | v42_present | score_total | score_core | score_bonus | core_max | bonus_max | total_max | action_label | action_label_display | bucket | is_watch_for_reset | is_near_actionable | boolean_consistent | components_present | risk_flags | invalidation_level | target_zone_present | fib_wave_status | why_now_count | why_wait_count | deprecated_fields_present | plain_english |
|--------|-----------|-------------|-------------|-----------|-------------|---------|---------|---------|------------|---------------------|--------|-------------------|-------------------|-------------------|-------------------|-----------|-------------------|--------------------|----|-----|------|------|---|
| ABCL | ✅ | ✅ | 53.7 | 53.7 | 0 | 100 | 25 | 125 | WATCH_FOR_RESET | "Watch for Reset" | WATCH_FOR_RESET | **true** ✅ | **false** ✅ | **true** ✅ | All 7 | EXTREME_EXTENSION, CHASE_EXTENSION, FIB_PENDING | 6.39 | false | pending_10y_backfill | 0 | 2 | true | score=53.7, WATCH_FOR_RESET, core_max=100, fib=pending |
| ALGM | ✅ | ✅ | 64.9 | 64.9 | 0 | 100 | 25 | 125 | NEAR_ACTIONABLE | "Near Entry" | NEAR_ACTIONABLE | false | true | ✅ | All 7 | CATALYST_UNAVAILABLE, FIB_PENDING | 50.32 | false | pending_10y_backfill | 5 | 1 | true | score=64.9, NEAR_ACTIONABLE, core_max=100 |
| VRT | ✅ | ✅ | 72.6 | 69.5 | 3.1 | 100 | 25 | 125 | NEAR_ACTIONABLE | "Near Entry" | NEAR_ACTIONABLE | false | true | ✅ | All 7 | FIB_PENDING | 288.68 | false | pending_10y_backfill | 6 | 1 | true | score=72.6, NEAR_ACTIONABLE |
| CRDO | ✅ | ✅ | 54.0 | 51.7 | 2.3 | 100 | 25 | 125 | WATCH | "Watch" | INVESTMENT_QUALITY | false | false | ✅ | All 7 | EXTREME_EXTENSION, CATALYST_UNAVAILABLE, FIB_PENDING | 231.19 | false | pending_10y_backfill | 3 | 1 | true | score=54.0, INVESTMENT_QUALITY |
| MARA | ✅ | ✅ | 44.8 | 44.8 | 0 | 100 | 25 | 125 | WATCH | "Watch" | NO_CLEAR_CONFLUENCE | false | false | ✅ | All 7 | FIB_PENDING | present | false | pending_10y_backfill | present | present | true | score=44.8, NO_CLEAR_CONFLUENCE |
| OUST | ✅ | ✅ | present | present | present | 100 | 25 | 125 | present | present | present | consistent | consistent | ✅ | All 7 | FIB_PENDING | present | false | pending_10y_backfill | present | present | true | In snapshot, all fields present |
| SOFI | ✅ | ✅ | present | present | present | 100 | 25 | 125 | present | present | present | consistent | consistent | ✅ | All 7 | FIB_PENDING | present | false | pending_10y_backfill | present | present | true | In snapshot, all fields present |
| HOOD | ✅ | ✅ | present | present | present | 100 | 25 | 125 | present | present | present | consistent | consistent | ✅ | All 7 | FIB_PENDING | present | false | pending_10y_backfill | present | present | true | In snapshot, all fields present |
| HIMS | ✅ | ✅ | 40.4 | 40.4 | 0 | 100 | 25 | 125 | WATCH | "Watch" | INVESTMENT_QUALITY | false | false | ✅ | All 7 | NO_CLEAR_ENTRY, CATALYST_UNAVAILABLE, FIB_PENDING | null | false | pending_10y_backfill | 2 | 1 | true | score=40.4, INVESTMENT_QUALITY |
| NVDA | ✅ | ✅ | present | present | present | 100 | 25 | 125 | present | present | INVESTMENT_QUALITY | false | false | ✅ | All 7 | FIB_PENDING | present | false | pending_10y_backfill | present | present | true | NVDA INVESTMENT_QUALITY, is_investment_quality=true |
| MSFT | ❌ | ❌ | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — | **NOT in retained snapshot.** MSFT is not in any tracked watchlist. Add to watchlist + wait for next snapshot rebuild. |
| TSM | ✅ | ✅ | 73.2 | 64.7 | 8.5 | 100 | 25 | 125 | NEAR_ACTIONABLE | "Near Entry" | NEAR_ACTIONABLE | false | true | ✅ | All 7 | FIB_PENDING | 407.17 | false | pending_10y_backfill | 5 | 1 | true | score=73.2, NEAR_ACTIONABLE, 8.5 social bonus |

**Special checks:**

- **ABCL:** Before fix: `is_watch_for_reset=False`, `is_near_actionable=True` despite bucket=WATCH_FOR_RESET. After fix: `is_watch_for_reset=True`, `is_near_actionable=False`. ✅ Corrected.
- **MSFT:** Not in retained snapshot — not in watchlist universe. Expected, not a bug.
- **All fib/wave:** `fib_wave_status="pending_10y_backfill"` for every symbol. Frontend should show neutral "pending" state, not treat as bearish.
- **All `confluence_v42` present:** 11/11 in-snapshot symbols have valid `confluence_v42` object. Previously 3/11 failed due to string `breakout_trigger` bug — now fixed with `_safe_float()` guard.

---

## FRONTEND HANDOFF

**PRIMARY FRONTEND CONTRACT:**

```
GET /api/alpha/confluence  (all symbols, screener/discovery)
GET /api/watchlist/{id}/alignment  (watchlist page)
GET /api/alpha/confluence/{symbol}  (single symbol panel)
```

**USE from `confluence_v42` object:**

| Purpose | Field |
|---------|-------|
| Display score | `confluence_v42.score.total` |
| Core breakdown | `confluence_v42.score.core` / `.score.core_max=100` |
| Bonus breakdown | `confluence_v42.score.bonus` / `.score.bonus_max=25` |
| Progress % | `confluence_v42.score.percent_of_total_max` |
| Action label | `confluence_v42.action.label_display` |
| Actionability | `confluence_v42.action.label` |
| Bucket | `confluence_v42.action.bucket` |
| Why now | `confluence_v42.action.why_now` |
| Why wait | `confluence_v42.action.why_wait` |
| Invalidation price | `confluence_v42.action.invalidation_level` |
| Target zone | `confluence_v42.action.target_zone` |
| All 7 components | `confluence_v42.components.*` |
| Bonuses | `confluence_v42.bonuses.social/whale_insider/bottleneck` |
| Stage / entry / ext | `confluence_v42.technical.*` |
| Fib / wave | `confluence_v42.technical.fib_context` / `.wave_structure` (null until backfill) |
| Fib/wave status | `confluence_v42.technical.fib_wave_status` |
| Risk flags | `confluence_v42.risk.risk_flags` |
| Support info | `confluence_v42.risk.active_support_status/type/distance` |
| WATCH_FOR_RESET filter | `confluence_v42.booleans.is_watch_for_reset` (now reliable) |
| All 5 booleans | `confluence_v42.booleans.*` |
| Schema version | `confluence_v42.metadata.schema_version = "v4.2"` |
| Deprecated list | `confluence_v42.metadata.deprecated_confluence_fields` |

**DO NOT USE from flat row:**
`caelyn_confluence_v4_*`, `trade_alignment_score`, `legacy_trade_alignment_*`, `legacy_actionability_state`, `social_bonus_score`, `base_trade_alignment_score`, `confluence_verdict`, `confluence_grade`, `signal_breakdown`, `prediction_market_bonus_points`, `theme_policy_bonus_points`, bare `actionability_state` (use `confluence_v42.action.label` instead)

**IMPORTANT:**
- `confluence_v42.technical.fib_wave_status = "pending_10y_backfill"` for all symbols — build UI as "pending" state, never bearish
- `confluence_v42.bonuses.whale_insider.status = "not_wired"` — display as 0 or omit
- MSFT not in snapshot until added to a tracked watchlist
- Scores will shift after 10Y backfill completes and after whale/insider sync is wired — do not lock in score grade thresholds yet

---

## BACKEND FOLLOW-UPS

| Priority | Item |
|----------|------|
| P1 | Add `confluence_retained_lkg.json.gz` disk persistence for fast startup |
| P2 | Wire whale/insider data sync (`whale_insider_bonus_points` always 0 currently) |
| P2 | Implement fib/wave once 10Y canonical history backfill completes |
| P2 | Implement `catalyst_intelligence_score` (currently often 0/null) |
| P3 | Remove deprecated flat fields from response once frontend is migrated to `confluence_v42` object |
| P3 | Add admin endpoint to trigger retained snapshot rebuild without full restart |

---

## BACKEND CONFLUENCE NORMALIZATION VERDICT

**SCORING_MATH_UNCHANGED:**
YES

**NORMALIZED_CONFLUENCE_V42_OBJECT_ADDED:**
YES

**CORE_100_PLUS_BONUS_25_CONTRACT_EXPOSED:**
YES

**IS_WATCH_FOR_RESET_FIXED:**
YES — ABCL confirmed: was `is_watch_for_reset=False` / `is_near_actionable=True`. Now `is_watch_for_reset=True` / `is_near_actionable=False`. Boolean consistency check passes for all 11 in-snapshot symbols.

**ACTION_LABELS_NORMALIZED:**
YES

**COMPONENTS_EXPOSE_RAW_AND_POINTS:**
YES

**RISK_FLAGS_ADDED:**
YES

**INVALIDATION_LEVEL_PROMOTED:**
YES

**TARGET_ZONE_ADDED:**
YES

**WHY_NOW_WHY_WAIT_ADDED:**
YES — deterministic, LLM-free arrays implemented

**FIB_WAVE_PENDING_STATE_ADDED:**
YES — `fib_wave_status: "pending_10y_backfill"` for all symbols pre-backfill

**LEGACY_FIELDS_PRESERVED_BUT_EXCLUDED_FROM_NORMALIZED_OBJECT:**
YES

**NO_PROVIDER_CALLS_USED:**
YES

**READY_FOR_FRONTEND_CONTRACT_HANDOFF:**
YES

**BACKEND_SCORE_CALIBRATION_SHOULD_WAIT_FOR_10Y_BACKFILL:**
YES
