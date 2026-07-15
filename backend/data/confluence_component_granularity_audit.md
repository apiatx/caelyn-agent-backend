# Confluence Component Score Granularity Audit
**Date:** 2026-07-15  
**Scope:** Catalyst Alignment (15 pts), Investment Alignment (15 pts), Entry/Exit Quality (12 pts)  
**Secondary scope:** Technical Setup (8 pts), Stage Quality (15 pts), Theme Alignment (15 pts), Options Alignment (20 pts)  
**Data basis:** Catalyst LKG n=145, Entry/Stage LKG n=347, Live API sample n=26 symbols

---

## Executive Summary

Three of the seven Core components of Caelyn Confluence V4.2 suffer from severe or moderate
bucketing, artificially compressing score differentiation across the 0–100 pt range. Two
components (Catalyst and Investment) each have **4 or fewer distinct values** despite a
max contribution of 15 pts each. One structural dead-weight problem (Catalyst Intelligence
permanently 0) reduces the effective Catalyst ceiling from 15 to 11.25 without any disclosure
in the UI.

The Entry, Technical Setup, Stage Quality, Theme Alignment, and Options Alignment components
range from moderately continuous to highly continuous and need only minor improvements.

---

## Part 1 — Catalyst Alignment (15 pts max)

### 1.1 Formula (from `caelyn_confluence_v42.py`, lines 919–1035)

```
catalyst_raw  = direct_score × 0.75  +  intelligence_score × 0.25
catalyst_pts  = catalyst_raw / 100 × 15

direct_score:   0–100 (described below)
intelligence_score: ALWAYS 0 (hard-coded "unavailable in snapshot")
```

### 1.2 Confirmed Distribution (n=145 from catalyst_alignment_lkg.json)

| catalyst_pts | Count | % | Source path |
|---|---|---|---|
| **0.00** | 12 | 8% | Bearish conflict (direct=0) |
| **0.91** | 3 | 2% | Score-only: cat_score ≈ 8.1 (below threshold 40) |
| **1.52** | 23 | 16% | Score-only: cat_score ≈ 13.5 (below threshold 40) |
| **11.25** | 107 | 74% | Any event present (direct=100) |
| **Total** | **145** | **100%** | **4 unique values** |

Statistics: min=0.00, max=11.25, mean=8.56, median=11.25  
74% of symbols receive the identical maximum event score (11.25).  
The theoretical maximum of 15.00 is **unreachable** in production.

### 1.3 Root Cause Analysis

#### Bug P0-A: Catalyst Intelligence (25%) is permanently dead weight
```python
# Lines 1028–1031 — hard-coded zero, always appended
intelligence_score  = 0.0
intelligence_status = "unavailable"
reason_codes.append("CATALYST_INTELLIGENCE_UNAVAILABLE")
```
The formula weights intelligence at 25% but it contributes **zero to every symbol, every time**.
This silently caps `catalyst_pts` at `100 × 0.75 / 100 × 15 = 11.25` regardless of event quality.
The user sees a 15-pt ceiling but no symbol can score above 11.25.

**Impact:** 3.75 pts permanently locked out. Any ordering based on catalyst score among
event-bearing symbols is impossible because they all score identically (11.25).

#### Bug P0-B: Direct score is binary (0 or 100) for 82% of symbols
```python
elif has_scheduled:
    if is_direct or True:   # line 977 — always True
        direct_score = 100.0

elif has_rss:
    if is_direct or True:   # line 986 — always True
        direct_score = 100.0
```
The `or True` override means **any event type, any quality, any confidence** receives
`direct_score = 100`. An FDA Phase 3 data readout and a generic news blurb tagged
`commercial_contract` score identically.

The event's `materiality_score`, `score`, `source` quality, and `event_type` tier are
visible in the event dict but unused for differentiation.

#### Bug P1-A: Score-only path is a near-zero dead zone (scores 8.1–13.5)
26 symbols in the score-only path all have `cat_score` in the range 8.1–13.5 — far
below the 40-threshold that would make `direct_present = True`. Their effective
`catalyst_pts = cat_score × 0.1125`, producing values of 0.91 or 1.52. Any symbol
with `cat_score` between 1 and 39 receives near-zero points even if the underlying
score reflects genuine moderate-confidence catalyst evidence.

### 1.4 Proposed Fixes

#### Fix P0-A: Remove dead intelligence weight or implement it
**Option A (immediate, no new data required):**  
Scale direct score to the full 15-pt ceiling:
```python
catalyst_pts = round(min(15.0, direct_score / 100.0 * 15.0), 2)
```
This eliminates the dead-weight 0.25 factor and restores the 15-pt ceiling.

**Option B (correct, requires news volume data):**  
Compute `intelligence_score` from the `news_volume_market_cap_48h` and  
`news_change_48h_pct` fields that the catalyst engine already tracks but leaves `None`
in snapshot mode. Populate these fields in the catalyst LKG refresh loop.

#### Fix P0-B: Graduate direct score by event tier + materiality
Replace the binary `direct_score = 100` with a tiered model:

```python
_EVENT_TIER_BASE = {
    "earnings":              100,
    "fda_approval":          100,
    "regulatory_decision":    95,
    "clinical_data":          90,
    "partnership":            80,
    "acquisition":            85,
    "commercial_contract":    70,
    "guidance_raised":        75,
    "management_change":      60,
    "rss_v2":                 55,  # generic RSS source
}

def _event_direct_score(evt: dict) -> float:
    base = _EVENT_TIER_BASE.get(evt.get("event_type", ""), 60)
    mat = float(evt.get("materiality_score") or evt.get("score") or 0)
    # Blend: 60% type tier + 40% materiality
    return min(100.0, base * 0.60 + mat * 0.40)
```

#### Fix P1-A: Eliminate the score-only dead zone
Map `cat_score` 1–100 linearly into the direct contribution, removing the 40-threshold cliff:
```python
# Score-only path — use full range, no threshold
direct_score  = raw_val  # already 0–100
direct_present = raw_val >= 30.0  # lower threshold for "detected"
```

---

## Part 2 — Investment Alignment (15 pts max)

### 2.1 Formula (from `caelyn_confluence_v42.py`, lines 1328–1517)

```
investment_alignment_points = strong_pillar_count × 5

Pillars (binary pass/fail):
  1. Financial Health:  passes if ≥4/7 checks pass
  2. Current Growth:    passes if ≥3/5 checks pass
  3. Forward Growth:    passes if ≥2/4 checks or extreme revenue

strong_pillar_count ∈ {0, 1, 2, 3}
investment_alignment_points ∈ {0, 5, 10, 15}
```

### 2.2 Confirmed Distribution (n=26 live symbols)

| Pts | Pillar count | Symbols | Notes |
|---|---|---|---|
| **0** | 0 | MSTR | 0 pillars |
| **5** | 1 | AOSL, AAOI, ACLS, ABCL, AVAV, BAND, AUR, AMBA, ASTS, IONQ (partially) | 1 pillar |
| **10** | 2 | ALGM, ANET, ADEA, AKAM, APLD, BB, BTDR, CAI, CRWD, HIMS, PANW, SMCI, ARM | 2 pillars |
| **15** | 3 | ALAB, AMD | 3 pillars |

**4 distinct values only.** Three symbols can differ by 65 pts on the continuous
`investment_quality_score` (0–100) yet receive identical `investment_alignment_points`:

| Symbol | investment_pts | investment_quality_score | Gap |
|---|---|---|---|
| CAI | 10 | 98 | |
| SMCI | 10 | 85 | 13-pt IQ gap → same pts |
| BTDR | 10 | 57 | 41-pt IQ gap → same pts |
| ACLS | 5 | 88 | high IQ, only 1 pillar → 5 pts |
| IONQ | 15 | 63 | 3 pillars, mixed IQ → 15 pts |

ACLS (IQ=88) receives fewer investment points than IONQ (IQ=63) because ACLS passes
only 1 pillar by the hard threshold test. This is a systematic inversion.

### 2.3 Root Cause Analysis

#### Bug P0-C: Continuous investment_quality_score computed but not used for points
```python
# Lines 1466–1486 — continuous score computed correctly
inv_quality_score = int(min(100, round(_best_pillar + _breadth_bonus)))
```

The `investment_quality_score` (0–100) already performs a principled continuous computation:
- `_best_pillar` = max of the three sub-pillar continuous scores (0–100 each)
- `_breadth_bonus` = 0/0/7/12 based on how many sub-pillars score ≥50

This score is returned in the API response and visible in the UI, but the
`investment_alignment_points` field used by the Confluence score still uses the
4-bucket formula `strong_pillar_count × 5`.

#### Bug P2-A: Pillar thresholds are hard pass/fail with large cliff effects
Financial Health: 3/7 checks (just below 4/7 threshold) → **0 contribution** from this pillar  
This creates a cliff effect: passing 4/7 vs 3/7 changes the pillar score from 5→0.

The continuous sub-pillar scores (`financial_health_score_0_100`, etc.) are already
computed and avoid this cliff but are only used for the unused `investment_quality_score`.

### 2.4 Proposed Fix

**Replace pillar-count bucketing with continuous quality score:**
```python
# Current (4 buckets):
inv_pts = round(float(strong_count) * 5.0, 2)

# Proposed (continuous):
inv_pts = round(inv_quality_score / 100.0 * 15.0, 2)
```

This is a one-line change. The `investment_quality_score` is already computed correctly
above the points assignment. The change:
- Preserves the 0–15 pt range
- Makes A+ assets (IQ=98) score ~14.7 vs average (IQ=57) scoring ~8.6
- Eliminates the 4-bucket cliff
- Keeps full backward compatibility (no new data, no API changes)

**Before/After for sample symbols:**

| Symbol | IQ | Pillars | Current pts | Proposed pts |
|---|---|---|---|---|
| ALAB | 100 | 3 | 15.0 | 15.0 |
| CAI | 98 | 2 | 10.0 | 14.7 |
| ALGM | 94 | 2 | 10.0 | 14.1 |
| CRWD | 92 | 2 | 10.0 | 13.8 |
| SMCI | 85 | 2 | 10.0 | 12.75 |
| ACLS | 88 | 1 | **5.0** | **13.2** ← fixes inversion |
| BTDR | 57 | 2 | 10.0 | 8.55 |
| ASTS | 45 | 1 | 5.0 | 6.75 |
| MSTR | 0 | 0 | 0.0 | 0.0 |

---

## Part 3 — Entry/Exit Quality (12 pts max)

### 3.1 Formula (from `caelyn_confluence_v42.py`, lines 654–870)

```
entry_raw = 0.35 × support_score  +  0.65 × rr_val
entry_pts = entry_raw / 100 × 12

support_score (6 discrete buckets):
  major_breakdown  →  0
  support_lost     → 10
  below_support    → 30
  neutral/unknown  → 50
  near_support     → max(50, 80 − dist_active × 1.5)   ← continuous
  at_support       → 85
  above_support    → 95

rr_val = entry_risk_reward_score (0–100, continuous)
       + fib_retest bonus (+2 to +6, gated by primary_fib_confidence ≥ 0.25)
       + MA proximity bonus (+2 to +4, when within 3–4% of 20/50/200 DMA)
       (capped at 100)
```

### 3.2 Confirmed Distribution (n=26 live symbols)

| entry_pts | Symbols |
|---|---|
| 0.00 | MSTR |
| 0.40 | APLD |
| 2.85–2.88 | BAND, AVAV, IONQ |
| 3.19–3.35 | SMCI, NVDA, AOSL, ACLS |
| 4.67–4.91 | ARM, ABCL, ALAB, AMD |
| 5.24 | AAOI, AKAM, ASTS |
| 6.49–6.80 | PANW, CRWD, BB |
| 7.95 | AMBA |
| 8.57–8.73 | CAI, AUR, BTDR, ALGM, ANET |
| 8.81 | ADEA |
| 10.54 | HIMS |

**~20 distinct values in 26 symbols.** Range 0.0–10.54/12.  
Entry is the **most continuous** of the three audited components.

Statistics: min=0.0, max=10.54, mean≈5.5, stdev≈2.9

From entry_state_lkg (n=347 symbols), mean entry_risk_reward_score by state:

| Entry State | n | Mean RR |
|---|---|---|
| LOWER_LOW_CONFIRMED | 89 | 10 |
| FAILED_BREAKOUT | 13 | 10 |
| NO_CLEAR_ENTRY | 44 | 33 |
| REVERSAL_WATCH | 7 | 13 |
| LOW_BASE_FORMING | 9 | 28 |
| TRENDLINE_SUPPORT_TEST | 49 | 54 |
| BREAKOUT_PULLBACK | 22 | 44 |
| SUPPORT_TEST | 15 | 67 |
| BREAKOUT_CONFIRMED | 8 | 74 |
| HIGH_BASE_READY | 5 | 67 |
| SIGNALS_BUILDING | 2 | 80 |
| LOWER_HIGH_WARNING | 15 | 80 |

### 3.3 Issues Found

#### Issue P2-B: Support score buckets create discontinuities at boundaries
The transition from `below_support` (30) → `near_support` (≥50) is a ≥20-point cliff.
Similarly `at_support` (85) → `above_support` (95) is a 10-point cliff regardless of
how far above support the price is.

These cliffs propagate into `entry_raw` as 0.35 × 20 = 7-pt discontinuities on a
100-pt raw scale, producing ≈0.84 pt jumps in the final 12-pt score.

For `near_support`, the formula `max(50, 80 − dist_active × 1.5)` is already continuous
in the interior — this is good. The same treatment should extend to the boundary zones.

#### Issue P3-A: RR score has 6 discrete values from entry_state (minor)
`entry_risk_reward_score` takes discrete values (10, 30, 50, 60, 67, 80) based on
`entry_state` classification. The fib (+2–6) and MA (+2–4) bonuses soften this into
a more continuous distribution, which explains why the final entry_pts is continuous.
No fix needed — this discreteness is absorbed by the additive bonuses.

### 3.4 Proposed Fix (Minor — P2-B only)

Replace the hard boundary between `below_support` / `near_support` / `at_support` /
`above_support` with a continuous distance-based interpolation:

```python
# Proposed: smooth boundary zone using dist_active
if asst_status == "above_support":
    support_score = 95.0  # no change — top of range
elif asst_status == "at_support":
    # dist_active = 0 at_support → gradually approach above_support boundary
    support_score = min(95.0, 85.0 + max(0, (3.0 - dist_active) * 2.0))
elif asst_status == "near_support":
    support_score = max(50.0, 80.0 - dist_active * 1.5)  # no change (already continuous)
elif asst_status == "below_support":
    # Smooth below_support: distance-based between 10 and 30
    support_score = max(10.0, 30.0 - dist_active * 2.0)
```

---

## Part 4 — Other Components (Reference)

### 4.1 Technical Setup (8 pts max) — Moderately Continuous

From 26-symbol sample: ~11 distinct values, range 0.64–7.64.  
Bimodal pattern: cluster at 0.64–2.8 (lower setups) and 6.64–7.64 (high-base setups).  
**No critical issues.** The bimodality reflects genuine setup quality dispersion.

### 4.2 Stage Quality (15 pts max) — Moderately Continuous

From 26-symbol sample: ~10 distinct values, range 0–15.0.  
Common values: 0 (structural break), 12.0 (stage 2), 15.0 (clear uptrend).  
**No critical issues.** Some clustering at stage boundaries is expected.

### 4.3 Theme Alignment (15 pts max) — Continuous ✓

From 26-symbol sample: ~15 distinct values, range 3.47–11.4.  
Distribution is well-spread. No bucketing concerns.

### 4.4 Options Alignment (20 pts max) — Highly Continuous ✓

From 26-symbol sample: ~22 distinct values, range 0.0–16.63.  
Best-distributed component in the entire scoring system.  
**No issues.**

---

## Part 5 — Consolidated Priority List

| Priority | Component | Bug | Impact | Fix Complexity |
|---|---|---|---|---|
| **P0** | Catalyst | Intelligence (25%) always 0 — max is 11.25/15 | Caps ceiling for all 145 symbols | Low (1-line formula change) |
| **P0** | Investment | 4-bucket `pillar_count × 5` ignores computed IQ score | Ordering inversions (ACLS < IONQ) | Low (1-line formula change) |
| **P1** | Catalyst | Binary direct score (0 or 100) — no event quality gradient | 74% of symbols score identically at 11.25 | Medium (event tier map) |
| **P1** | Catalyst | Score-only dead zone: cat_score 1–39 → near-zero pts | 26 symbols effectively have no catalyst signal | Low (remove threshold) |
| **P2** | Entry | Support status boundary discontinuities (20-pt cliff) | Step artifacts in entry scoring | Low (interpolate boundary) |
| **P3** | Technical | Bimodal distribution (minor, reflects real setup dispersion) | Minor | None needed |

---

## Part 6 — Implementation Plan

### Phase A (immediate, no schema change, no new data)

1. **Investment formula** — `caelyn_confluence_v42.py` line ~1432:
   ```python
   # Replace:
   inv_pts = round(float(strong_count) * 5.0, 2)
   # With:
   inv_pts = round(inv_quality_score / 100.0 * 15.0, 2)
   ```
   Note: `inv_quality_score` must be computed before this line (it already is, at line ~1474).
   Move the `inv_quality_score` computation block above the `inv_pts` assignment.

2. **Catalyst ceiling fix** — `caelyn_confluence_v42.py` line ~1034:
   ```python
   # Replace:
   catalyst_raw = direct_score * 0.75 + intelligence_score * 0.25
   # With (until intelligence_score is implemented):
   catalyst_raw = direct_score  # use full 100-pt direct score → maps to 15 pts
   ```
   This restores the 15-pt ceiling while keeping the formula structure intact.

3. **Catalyst score-only dead zone** — line ~1015:
   ```python
   # Remove threshold gating; allow any positive cat_score to scale proportionally
   direct_score = raw_val  # no threshold change
   direct_present = raw_val >= 20.0  # lower threshold
   ```

### Phase B (medium effort, improves quality signal)

4. **Event tier scoring** — add `_EVENT_TIER_SCORE` dict and use it in
   `_event_is_direct_catalyst` to return a grade (0–100) instead of binary True/False.
   The `direct_score = 100` assignment becomes `direct_score = _event_tier_score(evt)`.

5. **Support boundary smoothing** — `_score_entry_exit_v42` lines ~742–761:
   Replace hard `below_support = 30` with distance-interpolated formula.

### Phase C (requires new data pipeline)

6. **Catalyst Intelligence** — implement `news_volume_market_cap_48h` and
   `news_change_48h_pct` population in the catalyst LKG refresh loop. Re-activate the
   25% intelligence weight once the data is live.

---

## Part 7 — Before / After Score Impact (Phase A only)

Using the 19-symbol sample, estimated confluence score changes from Phase A fixes:

| Symbol | Current CC | Est. Phase A CC | Delta | Driver |
|---|---|---|---|---|
| CAI | 36.3 | 49.2 | +12.9 | Investment: 10→14.7 |
| ACLS | 43.1 | 51.3 | +8.2 | Investment: 5→13.2, Cat: 0→0 |
| SMCI | 30.3 | 37.5 | +7.2 | Investment: 10→12.75 |
| BTDR | 44.7 | 41.0 | -3.7 | Investment: 10→8.55 (correctly lower) |
| IONQ | 36.1 | 30.5 | -5.6 | Investment: 15→9.45 (correctly lower; IQ=63) |
| ALGM | 76.4 | 76.2 | -0.2 | Investment: 10→14.1, Cat ceiling lifted |
| ALAB | 71.3 | 71.3 | 0 | Already 3-pillar, IQ=100, no change |

Phase A fixes correctly raise high-quality symbols (CAI IQ=98, ACLS IQ=88) and reduce
overscored symbols (IONQ IQ=63 with 3 pillars, BTDR IQ=57 with 2 pillars).

---

## Appendix — Component Max Points Reference

```
Core (100 pts):
  technical_setup_points      8   (raw_score/100 × 8, continuous)
  stage_quality_points       15   (raw_score/100 × 15, moderately continuous)
  trade_alignment_points     30   (computed via archetype selection, continuous)
  options_alignment_points   20   (continuous)
  theme_alignment_points     15   (continuous)
  catalyst_alignment_points  15   (EFFECTIVELY 11.25 MAX — BUG P0-A)
  investment_alignment_pts   15   (4 BUCKETS ONLY — BUG P0-C)
  entry_exit_points          12   (continuous, minor boundary issue)

Bonus (25 pts):
  social_bonus_points         5
  whale_insider_bonus_points 15
  prediction_market_bonus     5

Total max: 125 pts
```

---

*Generated by Confluence Component Score Granularity Audit — Caelyn AI internal tooling*  
*Code references: `backend/services/caelyn_confluence_v42.py`, `backend/core/catalyst_engine.py`*
