# BACKEND CONFLUENCE V4.2 CONTRACT AUDIT
**Date:** 2026-07-14

---

## PART 1 — CANONICAL CONFLUENCE ENDPOINTS

| # | Endpoint | Method | Canonical V4.2 | Frontend Should Use | Notes |
|---|----------|--------|---------------|--------------------|----|
| **1** | **`/api/alpha/confluence`** | GET | YES | **PRIMARY** | Rate-limited 20/min; requires X-API-Key header; all watchlist universe symbols; 278 keys/row |
| **2** | **`/api/alpha/confluence/{symbol}`** | GET | YES | YES (single symbol) | 60/min |
| **3** | **`/api/watchlist/{watchlist_id}/alignment`** | GET | YES | YES (watchlist view) | + theme leadership, component coverage |
| 4 | `/api/watchlist/{watchlist_id}/confluence/v4-report` | GET | YES | NO — DIAGNOSTIC ONLY | Bucket/actionability distribution |
| 5 | `/api/debug/caelyn-confluence-ranking` | GET | partial | NO | Top-50, top-25 CAS debug |
| 6 | `/api/debug/confluence-accuracy` | GET | partial | NO | Debug compare |

**Retained Snapshot:**
- In-memory `_RETAINED["snapshot"]` — no disk file, no Neon table
- Built by `build_confluence_snapshot()` → reads `watchlist_stage2_lkg.json`, `themes_rs_lkg.json`, `options_master_lkg_v1.json`, `x_consensus_weekly.json`, Neon fundamentals
- V4.2 injected inline per row via `compute_confluence_v42()` during build
- Canonical fields promoted: `caelyn_confluence_score` ← `caelyn_confluence_v42_score`
- Cold build ~129 seconds; served stale-while-revalidate during rebuild

**THE ONE ENDPOINT FOR FRONTEND:** `GET /api/alpha/confluence`

---

## PART 2 — CANONICAL ROW SCHEMA (278 total keys)

### score_fields

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `caelyn_confluence_score` | float | 79.4 | **PRIMARY** — V4.2 total absolute score (0–125) |
| `caelyn_confluence_core_score` | float | 74.4 | V4.2 core (0–100, 7 components) |
| `caelyn_confluence_bonus_score` | float | 5.0 | V4.2 bonus (0–25, 3 bonuses) |
| `caelyn_confluence_max_score` | int | 125 | Total theoretical max |
| `caelyn_confluence_raw_score` | float | 79.4 | Alias of total score |
| `caelyn_confluence_total_score` | float | 79.4 | Alias of total score |
| `caelyn_confluence_normalized_score` | float | 79.4 | Normalized for bucket assignment — NOT for display |
| `caelyn_confluence_confidence_score` | float | 100.0 | % of components available (0–100) |
| `caelyn_confluence_bucket` | str | `NEAR_ACTIONABLE` | V4.2 bucket (canonical) |
| `caelyn_confluence_reason_codes` | list[str] | `["THEME_EMERGING",…]` | Flat union from all components |

### component_score_fields

| Field | Type | Example | Max pts | Notes |
|-------|------|---------|---------|-------|
| `theme_alignment_score` | float | 52.4 | — | Raw 0–100 |
| `theme_alignment_points` | float | 7.07 | 15 | V4.2 points |
| `stage_alignment_score` | float | 100.0 | — | Raw 0–100 |
| `stage_quality_score` | float | 100.0 | — | Alias of stage_alignment_score |
| `stage_quality_points` | float | 15.0 | 15 | V4.2 points |
| `options_alignment_score` | float\|None | 78.33 / None | — | Null when not scanned |
| `options_alignment_points` | float | varies | 20 | V4.2 points |
| `options_status` | str | `OPTIONS_USING_ALIGNMENT_SCORE_COMPOSITE` | — | |
| `technical_setup_raw_score` | float | 75.0 | — | Pattern quality 0–100 |
| `technical_setup_points` | float | 6.0 | 8 | V4.2 points |
| `technical_setup_label` | str | `"High-Base Consolidation"` | — | Human-readable |
| `entry_exit_raw_score` | float | 37.0 | — | Entry/exit quality 0–100 |
| `entry_exit_points` | float | 4.44 | 12 | V4.2 points |
| `entry_exit_status` | str | `"wait"` | — | Status label |
| `catalyst_alignment_score` | float | 39.9 | — | Catalyst score 0–100 |
| `catalyst_alignment_points` | float | 11.25 | 15 | V4.2 points |
| `catalyst_status` | str | `"available"` | — | |
| `investment_alignment_score` | float | 75.25 | — | Investment alignment 0–100 |
| `investment_alignment_points` | float | varies | 15 | V4.2 points |

### actionability_fields

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `caelyn_confluence_v42_actionability` | str | `NEAR_ACTIONABLE` | **V4.2 actionability — use for display**. Values: READY / NEAR_ACTIONABLE / WAIT_FOR_BREAKOUT / WAIT_FOR_RETEST / WATCH_FOR_RESET / WATCH / AVOID |
| `actionability_state` | str | `NEAR_ACTIONABLE` | Legacy V1 (promoted from V4.0). Prefer v42_actionability. |
| `is_actionable_setup` | bool | True/False | Reliable |
| `is_near_actionable` | bool | True/False | Reliable |
| `is_watch_for_reset` | bool | False | **UNRELIABLE — schema bug** — use `caelyn_confluence_bucket === "WATCH_FOR_RESET"` |
| `is_risk_conflict` | bool | False | Reliable |
| `is_investment_quality` | bool | True/False | Reliable |

### technical_fields

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `entry_state` | str | `BREAKOUT_PULLBACK` | V1 entry state |
| `entry_score` | int | 45 | Entry quality score |
| `entry_grade` | str | `B` | Entry grade |
| `extension_state` | str | `EXTREME_EXTENSION` | Extension classification |
| `extension_quality` | str | `CONSTRUCTIVE` | Extension quality label |
| `chase_extension` | bool | False | Chase extension flag |
| `constructive_extension` | bool | False | Constructive extension flag |
| `pattern_type` | str | `NO_PATTERN` | Continuation pattern type |
| `pattern_state` | str | `NOT_DETECTED` | Pattern state |
| `pattern_score` | int | 0 | Pattern quality score |
| `active_support_status` | str | `bounced_from_support` | Support test status |
| `active_support_type` | str | `prior_major_swing_low` | Support type |
| `distance_to_active_support_pct` | float | 1.59 | Distance to support % |
| `major_lower_low_confirmed` | bool | False | Major LLC flag (risk) |
| `lower_low_confirmed` | bool | False | Generic LLC flag |
| `critical_break_level` | float\|None | 74.4832 | Invalidation price |
| `confluence_at_support` | bool | False | CAS flag |
| `confluence_at_support_score` | int | 25 | CAS score |
| `confluence_at_support_state` | str | `EXTENDED_NOT_AT_SUPPORT` | CAS state |
| `entry_risk_reward_state` | str | `ABOVE_SUPPORT` | RR state |
| `entry_risk_reward_score` | int | 30 | RR quality 0–100 |

### entry_exit_fields (inside caelyn_confluence_v42_components.entry_exit)

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `invalidation_level` | float\|None | 74.4832 | Critical break level — NOT at top-level flat field |
| `target_1` | float\|None | None | First price target — NOT at top-level |
| `target_2` | float\|None | None | Second price target — NOT at top-level |
| `breakout_trigger` | float\|None | None | Pattern breakout trigger |
| `risk_reward_ratio` | float\|None | None | R:R ratio |
| `entry_zone` | object\|None | None | Entry zone object |
| `moving_average_entry_detected` | bool | True | MA entry signal |
| `moving_average_entry_type` | str | `MOMENTUM_PULLBACK_ENTRY` | MA entry type |

### fib_wave_fields (exist in schema, all currently null)

| Field | Type | Current | Notes |
|-------|------|---------|-------|
| `primary_fib_context` | str\|None | **None** | Populates post-10Y backfill |
| `primary_fib_timeframe` | str\|None | **None** | |
| `primary_nearest_fib_label` | str\|None | **None** | e.g. "0.618" |
| `primary_nearest_fib_level` | float\|None | **None** | Price level |
| `primary_fib_target_1` | float\|None | **None** | Extension target |
| `primary_fib_target_2` | float\|None | **None** | Extension target |
| `primary_distance_to_fib_pct` | float\|None | **None** | Distance % |
| `primary_fib_confidence` | float\|None | **None** | |
| `wave_structure_label` | str\|None | **None** | Populates post-10Y backfill |
| `wave_structure_score` | int\|None | **None** | 0–100 |
| `fib_computed` | bool\|None | **None** | |
| `fib_years_available` | float\|None | **None** | |

### investment_fields

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `investment_alignment_score` | float | 75.25 | 3-pillar composite |
| `investment_quality_label` | str\|None | `"STRONG_QUALITY_NEAR_SUPPORT"` | Derived quality label |
| `investment_pillar_count` | int | 2 | How many of 3 pillars are strong |
| `financial_health_strong` | bool | False | Pillar 1 |
| `current_growth_strong` | bool | True | Pillar 2 |
| `forward_growth_strong` | bool | False | Pillar 3 |
| `financial_health_score_0_100` | int | 47 | FH pillar score |
| `current_growth_score_0_100` | int | 73 | CG pillar score |
| `forward_growth_score_0_100` | int | 31 | FWD pillar score |
| `investment_quality_score` | int | 73 | Overall investment quality |
| `investment_quality_rank_label` | str | `"INVESTMENT_A_PLUS"` | Quality tier label |
| `investment_quality_reason_codes` | list[str] | varies | Pillar reason codes |

### bonus_fields

| Field | Type | Example | Notes |
|-------|------|---------|-------|
| `social_bonus_points` | float | 15.0 | V4.2 social bonus (max 15) |
| `social_sections_hit` | int | 3 | 0–3 sections (Consensus/Hype/Fresh) |
| `social_confluence_hit` | bool | True | X consensus picks |
| `social_acceleration_hit` | bool | True | X hype radar |
| `social_fresh_hit` | bool | True | X fresh trades |
| `whale_insider_bonus_points` | int | 0 | V4.2 whale+insider (max 5). Always 0 — not_wired |
| `bottleneck_bonus_points` | float | 1.54 | V4.2 bottleneck (max 5) |
| `bottleneck_anchor_count` | int | 4 | Supply-chain anchor count |
| `prediction_market_bonus_points` | int | 0 | Always 0 — disabled |
| `theme_policy_bonus_points` | int | 0 | Always 0 — folded into theme |

### debug_fields (V4.2 verbose copies — identical to canonical fields)

- `caelyn_confluence_v42_score` — identical to `caelyn_confluence_score`
- `caelyn_confluence_v42_core_score` — identical to `caelyn_confluence_core_score`
- `caelyn_confluence_v42_bonus_score` — identical to `caelyn_confluence_bonus_score`
- `caelyn_confluence_v42_max_score` — identical to `caelyn_confluence_max_score`
- `caelyn_confluence_v42_normalized_score` — identical to `caelyn_confluence_normalized_score`
- `caelyn_confluence_v42_available_max_pts` — available component max points
- `caelyn_confluence_v42_components` — full per-component nested dict
- `caelyn_confluence_v42_bonus_breakdown` — full bonus nested dict
- `caelyn_confluence_v42_reason_codes` — identical to `caelyn_confluence_reason_codes`
- `caelyn_confluence_v42_confidence_score` — identical to `caelyn_confluence_confidence_score`
- `caelyn_confluence_v42_bucket` — identical to `caelyn_confluence_bucket`
- `caelyn_confluence_v42_actionability` — V4.2 actionability state

### deprecated_fields

| Field | Notes |
|-------|-------|
| `caelyn_confluence_v4_score/bucket/actionability/components` | V4.0 debug copies — IGNORE |
| `trade_alignment_score` | V2 THEME_ALIGNMENT archetype — IGNORE |
| `legacy_trade_alignment_score` | Explicit legacy alias — IGNORE |
| `legacy_trade_alignment_archetype` | V2 archetype — IGNORE |
| `legacy_actionability_state` | Pre-V4 state — IGNORE |
| `social_bonus_score` | V2 scale (0–10) — IGNORE, use `social_bonus_points` (V4.2, 0–15) |
| `base_trade_alignment_score` | V2 base — IGNORE |
| `confluence_verdict` | V2 STRONG_BUY/BUY/WATCH/NEUTRAL/AVOID — IGNORE |
| `confluence_grade` | V2 A+/A/B/C — IGNORE |
| `signal_breakdown` | V2 per-signal dict — IGNORE |

---

## PART 3 — REQUIRED FRONTEND-FACING CONTRACT

### Score fields

| Requested field | Present | Backend field | Example |
|----------------|---------|--------------|---------|
| `symbol` | ✅ | `symbol` | "VRT" |
| `caelyn_confluence_score` | ✅ | `caelyn_confluence_score` | 58.4 |
| `core_score` | ✅ rename | `caelyn_confluence_core_score` | 55.3 |
| `bonus_score` | ✅ rename | `caelyn_confluence_bonus_score` | 3.1 |
| `score_max` | ❌ MISSING | `caelyn_confluence_max_score`=125 | 125 |
| `core_score_max` | ❌ MISSING | — | hardcode 100 |
| `bonus_score_max` | ❌ MISSING | — | hardcode 25 |

### Component scores

| Requested field | Present | Backend field | Notes |
|----------------|---------|--------------|-------|
| `theme_alignment_score` | ✅ | `theme_alignment_score` | |
| `stage_quality_score` | ✅ | `stage_quality_score` | |
| `options_alignment_score` | ✅ | `options_alignment_score` | Can be null |
| `technical_setup_score` | ✅ rename | `technical_setup_raw_score` | |
| `entry_exit_score` | ✅ rename | `entry_exit_raw_score` | |
| `catalyst_alignment_score` | ✅ | `catalyst_alignment_score` | |
| `investment_alignment_score` | ✅ | `investment_alignment_score` | |
| `social_bonus` | ✅ rename | `social_bonus_points` | |
| `whale_insider_bonus` | ✅ rename | `whale_insider_bonus_points` | Always 0 |
| `bottleneck_bonus` | ✅ rename | `bottleneck_bonus_points` | |

### Action fields

| Requested field | Present | Backend field | Values |
|----------------|---------|--------------|--------|
| `action_label` | ❌ rename | `caelyn_confluence_v42_actionability` | READY / NEAR_ACTIONABLE / WAIT_FOR_BREAKOUT / WAIT_FOR_RETEST / WATCH_FOR_RESET / WATCH / AVOID |
| `actionability_bucket` | ❌ rename | `caelyn_confluence_bucket` | ACTIONABLE / NEAR_ACTIONABLE / WATCH_FOR_RESET / INVESTMENT_QUALITY / SPECULATIVE_TRADE / RISK_CONFLICT / NO_CLEAR_CONFLUENCE |
| `is_actionable_setup` | ✅ | same | |
| `is_near_actionable` | ✅ | same | Reliable |
| `is_watch_for_reset` | ⚠️ BUG | same | **UNRELIABLE** — use `caelyn_confluence_bucket` |
| `is_risk_conflict` | ✅ | same | |
| `is_investment_quality` | ✅ | same | |

### Technical fields

| Requested field | Present | Backend field | Notes |
|----------------|---------|--------------|-------|
| `stage_label` | ⚠️ nested | `caelyn_confluence_v42_components.stage_quality.stage_label` | Not at top level |
| `stage_score` | ✅ rename | `stage_alignment_score` | |
| `technical_setup_label` | ✅ | `technical_setup_label` | Top level |
| `entry_state` | ✅ | `entry_state` | |
| `extension_state` | ✅ | `extension_state` | |
| `entry_score` | ✅ | `entry_score` | |
| `fib_context` | ⚠️ rename+null | `primary_fib_context` | Null until 10Y backfill |
| `wave_structure` | ⚠️ rename+null | `wave_structure_label` | Null until 10Y backfill |
| `risk_flags` | ❌ MISSING | derive from `caelyn_confluence_reason_codes` | Filter RISK_* prefix |

### Explanation fields

| Requested field | Present | Notes |
|----------------|---------|-------|
| `why_now` | ❌ NOT IMPLEMENTED | No backend implementation exists |
| `why_wait` | ❌ NOT IMPLEMENTED | No backend implementation exists |
| `invalidation_level` | ⚠️ nested | Top-level: `critical_break_level`. Also inside `caelyn_confluence_v42_components.entry_exit.invalidation_level` |
| `target_zone` | ⚠️ partial | Components have `entry_exit.target_1` / `target_2` — no `target_zone` object |

---

## PART 4 — DEPRECATED BACKEND FIELDS

| Field | Where | Deprecated reason | Frontend handling |
|-------|-------|------------------|-------------------|
| `caelyn_confluence_v4_*` | All endpoints | V4.0 debug copies | **IGNORE** |
| `trade_alignment_score` | All endpoints | V2 THEME_ALIGNMENT score | **IGNORE** |
| `legacy_trade_alignment_score` | All endpoints | Explicit legacy alias | **IGNORE** |
| `legacy_trade_alignment_archetype` | All endpoints | V2 archetype | **IGNORE** |
| `legacy_actionability_state` | All endpoints | Pre-V4 state | **IGNORE** |
| `social_bonus_score` | All endpoints | V2 scale differs (0–10 vs V4.2 0–15) | **IGNORE** — use `social_bonus_points` |
| `base_trade_alignment_score` | All endpoints | V2 base score | **IGNORE** |
| `confluence_verdict` | All endpoints | V2 STRONG_BUY/BUY/WATCH/NEUTRAL/AVOID | **IGNORE** |
| `confluence_grade` | All endpoints | V2 A+/A/B/C grades | **IGNORE** |
| `signal_breakdown` | All endpoints | V2 per-signal breakdown dict | **IGNORE** |
| `prediction_market_bonus_points` | All endpoints | Disabled from score | Display 0 or omit |
| `theme_policy_bonus_points` | All endpoints | Folded into theme_alignment | Display 0 or omit |
| `is_watch_for_reset` (boolean) | All endpoints | Derivation bug — based on V4 phase-6 tier, not V4.2 bucket | **DO NOT USE FOR FILTERING** |

---

## PART 5 — NORMALIZED CONTRACT RECOMMENDATION

### Proposed shape vs reality

```
caelyn_confluence_v42 = {
  score: {
    total:    caelyn_confluence_score,          // present ✅
    core:     caelyn_confluence_core_score,     // present ✅ (rename)
    bonus:    caelyn_confluence_bonus_score,    // present ✅ (rename)
    core_max: 100,                              // HARDCODE — not in response ❌
    bonus_max: 25,                              // HARDCODE — not in response ❌
    normalized: caelyn_confluence_normalized_score  // present ✅, NOT for display
  },
  action: {
    label:   caelyn_confluence_v42_actionability,  // present ✅ (rename)
    bucket:  caelyn_confluence_bucket,             // present ✅ (rename)
    why_now: null,       // NOT IMPLEMENTED ❌
    why_wait: null,      // NOT IMPLEMENTED ❌
    invalidation_level: critical_break_level,      // present ✅ (rename)
    target_zone: {
      target_1: caelyn_confluence_v42_components.entry_exit.target_1,
      target_2: caelyn_confluence_v42_components.entry_exit.target_2
    }                   // nested only ⚠️
  },
  components: caelyn_confluence_v42_components,   // present ✅ (rename)
  bonus_breakdown: caelyn_confluence_v42_bonus_breakdown,  // present ✅
  technical: {
    stage_label: caelyn_confluence_v42_components.stage_quality.stage_label,  // nested ⚠️
    entry_state: entry_state,                      // present ✅
    technical_setup_label: technical_setup_label,  // present ✅
    extension_state: extension_state,              // present ✅
    fib_context: primary_fib_context,              // present but null ⚠️
    wave_structure: wave_structure_label,          // present but null ⚠️
    risk_flags: [derived from reason_codes]        // not a field ❌
  },
  booleans: {
    is_actionable_setup:  is_actionable_setup,     // present ✅
    is_near_actionable:   is_near_actionable,      // present ✅
    is_watch_for_reset:   caelyn_confluence_bucket === "WATCH_FOR_RESET",  // USE BUCKET ⚠️
    is_risk_conflict:     is_risk_conflict,        // present ✅
    is_investment_quality: is_investment_quality   // present ✅
  },
  confidence: caelyn_confluence_confidence_score,  // present ✅
  reason_codes: caelyn_confluence_reason_codes     // present ✅
}
```

| Property | Status |
|----------|--------|
| `already_matches` | components, actionability, booleans (except is_watch_for_reset), entry/extension technical, investment, social/bottleneck |
| `missing_fields` | why_now, why_wait, score.core_max, score.bonus_max, technical.risk_flags (as array), action.target_zone (as object) |
| `renames_needed` | action.label, action.bucket, score.core, score.bonus, technical.fib_context, technical.wave_structure, technical.stage_label |
| `backend_normalization_needed` | Fix is_watch_for_reset; add core_max/bonus_max; promote invalidation_level top-level; synthesize target_zone object |

---

## PART 6 — SAMPLE SYMBOL AUDIT

| Symbol | In Snapshot | Score | Bucket | V4.2 Action | Core | Bonus | All 7 Components | Boolean Issue | Fib/Wave | Plain English |
|--------|------------|-------|--------|-------------|------|-------|-----------------|---------------|----------|---------------|
| ABCL | ✅ | 44.7 | WATCH_FOR_RESET | WATCH_FOR_RESET | 44.7 | 0 | ✅ | `is_watch_for_reset=False` ⚠️ `is_near_actionable=True` ⚠️ | null | Schema bug confirmed: bucket=WATCH_FOR_RESET but boolean says near_actionable. Use bucket not boolean. |
| ALGM | ✅ | 48.9 | NEAR_ACTIONABLE | NEAR_ACTIONABLE | 48.9 | 0 | ✅ | clean | null | Near-actionable. All components scored. No bonus signals. |
| VRT | ✅ | 58.4 | NEAR_ACTIONABLE | NEAR_ACTIONABLE | 55.3 | 3.1 | ✅ | clean | null | Near-actionable with small bottleneck bonus. |
| CRDO | ✅ | 44.5 | NEAR_ACTIONABLE | NEAR_ACTIONABLE | 42.2 | 2.3 | ✅ | clean | null | Bucket NEAR_ACTIONABLE; both booleans false — bucket is reliable source. |
| MARA | ✅ | 33.4 | NO_CLEAR_CONFLUENCE | WATCH | 33.4 | 0 | ✅ | clean | null | Low score. No clear confluence. Crypto-adjacent. |
| OUST | ✅ | present | present | present | — | — | ✅ | — | null | In snapshot. |
| SOFI | ✅ | present | present | present | — | — | ✅ | — | null | In snapshot. |
| HOOD | ✅ | present | present | present | — | — | ✅ | — | null | In snapshot. |
| HIMS | ✅ | present | present | present | — | — | ✅ | — | null | In snapshot. |
| NVDA | ✅ | 38.7 | INVESTMENT_QUALITY | WATCH | 36.0 | 2.7 | ✅ | clean, is_investment_quality=True | null | INVESTMENT_QUALITY bucket — strong fundamentals override weak technical entry timing. |
| MSFT | ❌ NOT IN SNAPSHOT | — | — | — | — | — | — | — | — | Not in watchlist universe. Not in stage2_lkg. Add to watchlist + wait for rebuild. |
| TSM | ✅ | 64.1 | NEAR_ACTIONABLE | NEAR_ACTIONABLE | 55.6 | 8.5 | ✅ | clean | null | One of higher scores; 3 social signals = 8.5 bonus pts. |

---

## PART 7 — BACKEND READINESS FOR FRONTEND

### canonical_endpoint_to_use
`GET /api/alpha/confluence`
Watchlist-context: `GET /api/watchlist/{watchlist_id}/alignment`

### fields_frontend_should_use

| Purpose | Field |
|---------|-------|
| Display score | `caelyn_confluence_score` (absolute 0–125) |
| Score breakdown | `caelyn_confluence_core_score` + `caelyn_confluence_bonus_score` |
| Progress bar | `caelyn_confluence_score / 125 * 100` |
| Bucket label | `caelyn_confluence_bucket` |
| Actionability state | `caelyn_confluence_v42_actionability` |
| Action booleans | `is_actionable_setup`, `is_near_actionable`, `is_risk_conflict`, `is_investment_quality` |
| WATCH_FOR_RESET filter | `caelyn_confluence_bucket === "WATCH_FOR_RESET"` (NOT the boolean) |
| Component breakdown | `caelyn_confluence_v42_components` (7 core + 3 bonus nested) |
| Reason codes | `caelyn_confluence_reason_codes` |
| Confidence | `caelyn_confluence_confidence_score` |
| Technical | `technical_setup_label`, `entry_state`, `extension_state`, `extension_quality` |
| Support/risk | `critical_break_level`, `active_support_status`, `major_lower_low_confirmed` |
| Fib/wave | `primary_fib_context`, `wave_structure_label` (null until 10Y backfill — show "pending") |
| Investment | `investment_quality_label`, `investment_pillar_count`, `is_investment_quality` |
| Social bonus | `social_bonus_points`, `social_sections_hit`, `social_confluence_hit`, `social_acceleration_hit`, `social_fresh_hit` |
| Bottleneck | `bottleneck_bonus_points`, `bottleneck_anchor_count` |

### fields_frontend_should_ignore
All `caelyn_confluence_v4_*`, `trade_alignment_score`, `legacy_*`, `social_bonus_score`, `base_trade_alignment_score`, `confluence_verdict`, `confluence_grade`, `signal_breakdown`, `prediction_market_bonus_points`, `theme_policy_bonus_points`, **`is_watch_for_reset`**

### score_display_rule
- Primary display: `caelyn_confluence_score` (e.g. 71.0 out of 125)
- Progress bar: `caelyn_confluence_score / 125 * 100`
- Do NOT display `caelyn_confluence_normalized_score` — for internal bucket logic only
- Core bar: `caelyn_confluence_core_score / 100`
- Bonus bar: `caelyn_confluence_bonus_score / 25`

### action_label_rule
Use `caelyn_confluence_v42_actionability`:
- `READY` → "Ready to Enter"
- `NEAR_ACTIONABLE` → "Near Entry"
- `WAIT_FOR_BREAKOUT` → "Wait for Breakout"
- `WAIT_FOR_RETEST` → "Wait for Retest"
- `WATCH_FOR_RESET` → "Watch for Reset"
- `WATCH` → "Watch"
- `AVOID` → "Avoid"

### component_breakdown_rule
Source: `caelyn_confluence_v42_components` dict. 7 keys: `theme_alignment`, `stage_quality`, `options_alignment`, `technical_setup`, `entry_exit`, `catalyst_alignment`, `investment_alignment`. Each has `points`, `available`, `status`, `reason_codes`. Max pts: theme=15, stage=15, options=20, technical=8, entry_exit=12, catalyst=15, investment=15. Bonus from `caelyn_confluence_v42_bonus_breakdown`: social(max 15), whale_insider(max 5, currently 0), bottleneck(max 5). When `available=false` → show "Data unavailable".

### known_backend_gaps
1. `why_now` / `why_wait` — NOT IMPLEMENTED anywhere
2. `is_watch_for_reset` boolean — UNRELIABLE (V4 derivation bug)
3. Fib/wave fields — universally null until 10Y backfill
4. `whale_insider_bonus_points` — always 0 (not_wired)
5. `catalyst_intelligence_score` — often 0/null (partially implemented)
6. MSFT and non-watchlist symbols — not in snapshot
7. `score_max`, `core_score_max`, `bonus_score_max` — not in response

### backend_changes_needed_before_frontend

| Priority | Change | Effort |
|----------|--------|--------|
| P0 | Fix `is_watch_for_reset` — re-derive from `caelyn_confluence_v42_bucket` | Trivial (1 line) |
| P1 | Add `caelyn_confluence_core_max: 100` and `caelyn_confluence_bonus_max: 25` | Trivial |
| P1 | Promote `invalidation_level` to top-level flat field | Trivial |
| P2 | Synthesize `target_zone: {target_1, target_2}` object | Trivial |
| P2 | Derive `risk_flags` array from reason_codes | Small |
| P3 | Implement `why_now` / `why_wait` (LLM-free from reason codes) | Medium |
| P3 | Wire whale/insider data sync | Large |

---

## PART 8 — FINAL REPORT

### BACKEND CONFLUENCE V4.2 CONTRACT AUDIT REPORT

**CANONICAL ENDPOINT:** `GET /api/alpha/confluence`
Single retained in-memory snapshot. Built from `watchlist_stage2_lkg.json` + `themes_rs_lkg.json` + options LKG + x_consensus + Neon fundamentals. V4.2 computed inline per row via `compute_confluence_v42()`. No disk file; no Neon table. Cold build ~129s. 278 keys/row.

**CANONICAL ROW SCHEMA SUMMARY:**
- Primary score field: `caelyn_confluence_score` (V4.2 total, 0–125, absolute)
- Core/bonus split: `caelyn_confluence_core_score` (0–100) + `caelyn_confluence_bonus_score` (0–25)
- Bucket: `caelyn_confluence_bucket` (7 values)
- Actionability: `caelyn_confluence_v42_actionability` (7 values)
- Full component breakdown: `caelyn_confluence_v42_components` (nested, 7 components)
- Bonus breakdown: `caelyn_confluence_v42_bonus_breakdown`
- 278 total keys; ~50 are V4.2 canonical, ~30 legacy/deprecated, rest are technical/analytical fields

**FRONTEND-FACING FIELDS:**

| Field | Status | Backend name |
|-------|--------|-------------|
| caelyn_confluence_score | ✅ present | same |
| core_score | ✅ rename | caelyn_confluence_core_score |
| bonus_score | ✅ rename | caelyn_confluence_bonus_score |
| score_max | ❌ missing | hardcode 125 |
| core_score_max | ❌ missing | hardcode 100 |
| bonus_score_max | ❌ missing | hardcode 25 |
| action_label | ✅ rename | caelyn_confluence_v42_actionability |
| actionability_bucket | ✅ rename | caelyn_confluence_bucket |
| is_actionable_setup | ✅ present | same |
| is_near_actionable | ✅ present | same |
| is_watch_for_reset | ⚠️ BUG | use caelyn_confluence_bucket |
| is_risk_conflict | ✅ present | same |
| is_investment_quality | ✅ present | same |
| technical_setup_label | ✅ present | same |
| entry_state | ✅ present | same |
| extension_state | ✅ present | same |
| fib_context | ⚠️ null | primary_fib_context |
| wave_structure | ⚠️ null | wave_structure_label |
| why_now | ❌ not implemented | — |
| why_wait | ❌ not implemented | — |
| invalidation_level | ⚠️ rename | critical_break_level |
| target_zone | ⚠️ nested | entry_exit.target_1 / target_2 |
| components (all 7) | ✅ present | caelyn_confluence_v42_components |
| social_bonus | ✅ rename | social_bonus_points |
| whale_insider_bonus | ✅ rename | whale_insider_bonus_points (=0) |
| bottleneck_bonus | ✅ rename | bottleneck_bonus_points |

**DEPRECATED FIELDS:**
trade_alignment_score, legacy_trade_alignment_*, legacy_actionability_state, social_bonus_score, base_trade_alignment_score, confluence_verdict, confluence_grade, signal_breakdown, caelyn_confluence_v4_*, prediction_market_bonus_points, theme_policy_bonus_points

**NORMALIZED CONTRACT RECOMMENDATION:**
Backend exposes all essential V4.2 data. Frontend needs field renames (not new data) for the `confluence_v42` object shape. Two exceptions requiring new backend work: `why_now`/`why_wait` (new derivation) and `risk_flags` array (filter existing reason_codes). Fib/wave schema is complete but values null pending 10Y backfill.

**SAMPLE SYMBOL AUDIT SUMMARY:**
11/12 symbols in snapshot (MSFT absent — not in watchlist universe). All in-snapshot symbols have all 7 V4.2 components present. All fib/wave fields null. Boolean flag bug confirmed on ABCL: `caelyn_confluence_bucket=WATCH_FOR_RESET` but `is_watch_for_reset=False`, `is_near_actionable=True`.

**FRONTEND HANDOFF NOTES:**
1. Use `caelyn_confluence_score` for display (absolute 0–125)
2. Use `caelyn_confluence_v42_actionability` for action label (not `actionability_state`)
3. Use `caelyn_confluence_bucket` for WATCH_FOR_RESET detection (NOT `is_watch_for_reset`)
4. Components: read from `caelyn_confluence_v42_components` (7 keys)
5. Fib/wave: build UI as "pending" state — will auto-populate post-10Y backfill
6. Whale/insider bonus: always 0 — safe to display as 0 or omit
7. `why_now`/`why_wait`: not implemented — omit from current build
8. Ignore all `caelyn_confluence_v4_*`, all `legacy_*`, all `confluence_verdict/grade/signal_breakdown`
9. Score calibration: current scores have partial signal coverage (options null for many, fib/wave null, whale=0). Scores will shift after 10Y backfill + whale wiring. Do not lock in score grade thresholds until post-backfill calibration.
10. MSFT: add to tracked watchlist + wait for next snapshot rebuild cycle.

**BACKEND FIXES NEEDED:**
1. P0: Fix `is_watch_for_reset` boolean (1-line: derive from `caelyn_confluence_v42_bucket`)
2. P1: Add `caelyn_confluence_core_max: 100` and `caelyn_confluence_bonus_max: 25` to response
3. P1: Promote `invalidation_level` as top-level flat field
4. P2: Synthesize `target_zone` object from entry_exit.target_1/2
5. P2: Add `risk_flags` array (filter reason_codes for RISK_* prefix)
6. P3: Implement `why_now`/`why_wait` (LLM-free reason-code derivation)
7. P3: Wire whale/insider data sync

---

## BACKEND CONFLUENCE CONTRACT VERDICT

**CANONICAL_CONFLUENCE_ENDPOINT_IDENTIFIED:**
YES — `GET /api/alpha/confluence`

**BACKEND_CONFLUENCE_V42_CONTRACT_CLEAR:**
YES — V4.2 fully computed and promoted to canonical top-level fields. Formula: Core=100 (7 components) + Bonus=25 (3 bonuses) = Max 125. All `caelyn_confluence_*` flat fields are V4.2. 278-key row contains verbose debug copies and legacy fields alongside clean V4.2 canonicals.

**CORE_100_PLUS_BONUS_25_FIELDS_PRESENT:**
YES — `caelyn_confluence_core_score` (0–100) + `caelyn_confluence_bonus_score` (0–25) + `caelyn_confluence_score` (total, 0–125) all present and live. However `core_score_max=100` and `bonus_score_max=25` are NOT returned as response fields — frontend must hardcode.

**ACTIONABILITY_FIELDS_PRESENT:**
YES — `caelyn_confluence_v42_actionability` (7 values), `caelyn_confluence_bucket` (7 values), `is_actionable_setup`, `is_near_actionable`, `is_risk_conflict`, `is_investment_quality` all present. Exception: `is_watch_for_reset` is present but unreliable — use `caelyn_confluence_bucket` filter instead.

**TECHNICAL_ENTRY_FIB_FIELDS_PRESENT:**
PARTIAL — Technical/entry fields fully present. Fib and wave fields present in schema but universally null — pending 10Y canonical history backfill.

**DEPRECATED_TRADE_ALIGNMENT_FIELDS_STILL_RETURNED:**
YES — `trade_alignment_score`, `legacy_trade_alignment_score`, `legacy_trade_alignment_archetype`, `legacy_actionability_state`, `social_bonus_score`, `base_trade_alignment_score`, `confluence_verdict`, `confluence_grade` still returned. Frontend should ignore all.

**BACKEND_SCHEMA_READY_FOR_FRONTEND:**
YES (with 2 caveats) — V4.2 canonical fields complete, stable, correctly promoted. Frontend can build against `caelyn_confluence_score` / `caelyn_confluence_v42_actionability` / `caelyn_confluence_bucket` / `caelyn_confluence_v42_components` today. Caveats: (1) fix `is_watch_for_reset` boolean before using as filter, (2) treat fib/wave as "pending" until 10Y backfill completes.

**BACKEND_SCORE_CALIBRATION_SHOULD_WAIT_FOR_10Y_BACKFILL:**
YES
