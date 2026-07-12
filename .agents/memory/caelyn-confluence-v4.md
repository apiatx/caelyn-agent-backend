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
