---
name: Caelyn Confluence V4 Engine
description: V4 additive scoring engine architecture, normalization design, and bucket calibration lessons
---

## Architecture

`backend/services/caelyn_confluence_v4.py` — pure function, zero provider calls, zero LLM.

Injection: `confluence_v2_service.build_confluence_snapshot()` → after `_compute_confluence(r)`, calls
`compute_confluence_v4(r, social_map)` and does `r.update(v4)` BEFORE `results.append(r)`.

Alignment endpoint: `watchlist_router.py` reads V4 fields from snapshot row via `row.get("caelyn_confluence_v4_*")`.

## Formula

```
v4_core_score = sum(6 components, max=85)
v4_pre_overlay = min(100, core + social_bonus)   # social max 15
v4_total_score = min(115, pre_overlay + overlay)  # overlay max 15
```

Components: theme(15) + stage(15) + options(15) + entry_rr(15) + catalyst(15) + invest(10) = 85

Overlays: theme_policy(2.5) + prediction_markets(2.5) + whale_insider(5, Phase1=0) + bottleneck(5)

## Normalised Score (critical for bucket assignment)

Options/catalyst are missing for most symbols. Raw core would unfairly deflate good setups.

```python
available_max = sum(max_pts for components that are available)
normalized_core = (core_score / available_max) * 85
normalized_total = min(115, min(100, normalized_core + social) + overlay)
```

Use `normalized_total` for bucket thresholds, expose raw `total_score` for API consumers.

## Bucket Thresholds (calibrated for 375-ticker universe)

- RISK_CONFLICT: `major_lower_low_confirmed` ONLY — not AVOID actionability state
- ACTIONABLE: normalized >= 72 AND entry_pts >= 10 AND no pure chase
- NEAR_ACTIONABLE: normalized >= 55 AND entry_pts >= 4 AND no pure chase
- CAS: normalized >= 50 AND asst_intact AND entry_pts >= 2
- INVESTMENT_QUALITY: invest_score >= 80 AND normalized >= 40
- SPECULATIVE_TRADE: normalized >= 62 AND weak invest
- WATCH_FOR_RESET: (chase AND NOT constructive) OR normalized >= 52
- NO_CLEAR_CONFLUENCE: else

## Entry Scoring for Constructive Extension

`entry_risk_reward_score=30` is expected (and correct) for HIGH_TIGHT_FLAG/BULL_FLAG with constructive extension — price is extended from base, so R/R is poor. But the PATTERN quality is high.

Fix: TIER1_CONSTRUCTIVE = {HIGH_TIGHT_FLAG, BULL_FLAG, BREAKOUT_SHELF, VCP} → floor of 9 pts, uses `pattern_score` (0-100) if available (fallback: 70).

## Phase 1 Unwiried Overlays

- `whale_insider`: async Neon service, no sync LKG → status=not_wired_phase1, 0 pts
- `prediction_markets`: LKG has no families yet → status=not_available, 0 pts
- `bottleneck`: sync `get_multi_anchor_screener()` — module-level 1h cache
- `theme_policy`: reads from snapshot row `theme_policy_boost` (0-1 float) → * 2.5

**Why:** These avoidance decisions are critical for correctness (spec: "never fabricate").

## Validated Distribution (375-ticker Primary watchlist)

ACTIONABLE=3, NEAR_ACTIONABLE=55, CAS=8, INVESTMENT_QUALITY=2, RISK_CONFLICT=17, WATCH_FOR_RESET=43, NO_CLEAR=247

## Phase 1.5 Changes (Calibration)

### Constructive Extension Hardening
Three hard gates before TIER1 constructive override fires:
1. `pattern_score >= 55` (or 0/None which means "no score from engine" → allowed via `if pat_score > 0 and pat_score < 55`)
2. `active_support_status not in (support_lost, breakdown, major_breakdown)`
3. `lower_low_confirmed == False` (minor LLC also disqualifies, not just major)

Shelf quality caps max pts earned:
- `current_shelf_support is not None` → `SHELF_CONFIRMED` → 15 pts max
- `dist_active <= 20` → `SHELF_NOT_CONFIRMED_ESTIMATED` → 12 pts max  
- otherwise → `SHELF_ABSENT_WIDE_EXTENSION` → 10 pts max

### Confidence Score Changes
`_compute_v4_confidence` takes `shelf_confirmed: bool` and `used_constructive_tier1: bool`. Penalty: `-6 pts from earned` when tier1 constructive was used but no confirmed shelf. Effect: WYFI/CRWD/FTNT drop 60.9 → 54.3.

### Confidence Guards on Bucket Assignment
- ACTIONABLE: `confidence >= 55`
- NEAR_ACTIONABLE primary: `confidence >= 45`
- NEAR_ACTIONABLE softer: `confidence >= 45`
- READY actionability: `confidence >= 70`

Confidence computed BEFORE bucket assignment so guards can use it.

### Score Field Clarification
- `caelyn_confluence_v4_raw_score` = explicit alias for `total_score` (added as new field)
- Frontend should display `normalized_score`; confidence is a context badge
- `raw_score` = pts earned; `normalized_score` = adjusted for availability; `confidence_score` = completeness

### Current Data Coverage Facts (Phase 1.5 baseline)
- Options: ALL 375 symbols are `not_scanned` (universal gap, no options scoring)
- Catalyst: 97 available (26%), 238 missing_cache (63%), 40 theme_policy_overlay (11%)
- All case study symbols have `current_shelf_support=None` (estimated shelf treatment)

**Why:** TIER1 constructive with no shelf confirmation must be visibly lower quality — confidence penalty and 12pt cap make this traceable without blocking correct setups.

## Phase 1.6 Changes (Component Wiring + Status Fixes)

### no_catalyst sentinel fix (critical)
`catalyst_detail_status` in snapshot rows is a **trade alignment state** ("neutral",
"moderate_tailwind"), NOT the catalyst component's detail_status.  The original
`if not available and detail_status == "no_catalyst"` never fired.

Fix: use `row.get("catalyst_alignment_available") is False` as the sentinel:
```python
_cat_avail_field = row.get("catalyst_alignment_available")
if not available and _cat_avail_field is False and raw_score is None:
    # V2 service ran and found nothing → no_catalyst, not missing_cache
```

Effect: 240 `missing_cache` → `no_catalyst`; `_KNOWN_STATE_STATUSES` includes `no_catalyst`
so confidence rose avg 0.0 → 67.7, >=70 count 0 → 233 rows.

### V4 report endpoint output key names
- Score key: `caelyn_confluence_v4_normalized_score` (NOT `..._normalized_total`)
- Confidence key: `caelyn_confluence_v4_confidence_score` (NOT `..._confidence`)

### Options status: not_scanned is expected after restart
On weekend restart the disk LKG may only have a partial scan (~19 tickers).  All 379 show
`not_scanned` until the options master screener runs a full pass.  The disk LKG fallback
fix is in place; it fires once the LKG has fresh data.

### Phase 1.6 validated distribution (379-ticker watchlist)
Buckets: NO_CLEAR=253, NEAR_ACTIONABLE=54, WATCH_FOR_RESET=44, RISK_CONFLICT=17,
CAS=8, INVESTMENT_QUALITY=3 (Errors: 0)
Confidence: avg 67.7 | <40: 27 | 40-69: 119 | >=70: 233
