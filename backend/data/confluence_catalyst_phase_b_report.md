
# CATALYST PHASE B CONTINUOUS SCORING REPORT
Generated: 2026-07-15 | Source: catalyst_alignment_lkg.json (n=145), live API, backend unit validation

---

## INPUT FIELD AUDIT

### Fields available per catalyst LKG row (all 146 rows)

| Field | Presence | Notes |
|---|---|---|
| catalyst_alignment_score | 146/146 | Raw 0–100 score from catalyst service |
| catalyst_alignment_available | 146/146 | Availability flag |
| catalyst_primary_event | 146/146 | **Rich canonical event dict — primary scoring source** |
| catalyst_primary_source | 146/146 | rss_v2 / theme_policy / scheduled |
| catalyst_bearish_conflict | 146/146 | Bool; zeroes all catalyst pts when True |
| catalyst_rss_event | 116/146 | RSS event dict; overlaps with primary_event |
| catalyst_scheduled_event | 146/146 | Scheduled event dict; null for most |
| catalyst_v2_primary_event | 146/146 | V2 scoring dict; mirrors primary_event |
| catalyst_v2_reason_codes | 146/146 | Reason code array |
| theme_policy_* | 146/146 | Theme policy signal fields |

**Rich fields inside `catalyst_primary_event` (available for rss_v2/scheduled rows, 116/146):**

| Field | Coverage | Range | Notes |
|---|---|---|---|
| event_type | 116/146 | see below | COMMERCIAL_CONTRACT, MNA, etc. |
| materiality_score | 116/146 | 0.34–0.87, mean=0.557 | Pre-classified by catalyst service |
| confidence_score | 116/146 | 0.6–0.83 | Pre-classified by catalyst service |
| ticker_relevance_score | 116/146 | 0.9–1.0 | 1.0 = explicit ticker match |
| ticker_relevance_reason | 116/146 | EXPLICIT_SYM_PARENS, SYM_IN_TITLE | Match classification |
| published_at / catalyst_date | 118/146 | Date string | Used for freshness calculation |
| article_count | 116/146 | 1–3 (median=1) | Corroboration signal |
| days_until | 2/146 | int | Only populated for scheduled events |

**Remaining 30 rows (theme_policy source):** no primary_event rich data — use cat_score mapped to Tier D.

### Distribution by source and event type (pre-fix)

| Source | Count |
|---|---|
| rss_v2 | 102 |
| theme_policy | 30 |
| rss_v2_plus_theme_policy | 12 |
| scheduled | 2 |

| Event Type | Count |
|---|---|
| COMMERCIAL_CONTRACT | 41 |
| MNA | 32 |
| unknown | 28 |
| STRATEGIC_PARTNERSHIP | 16 |
| EARNINGS_GUIDANCE | 10 |
| HYPERSCALER_ANCHOR | 8 |
| TECHNICAL_MILESTONE | 5+1 |
| DEFENSE_MILITARY | 2 |
| SPLIT_THIS_WEEK | 1 |
| DIVIDEND_THIS_WEEK | 1 |

---

## SCORING FORMULA

### Phase B `_catalyst_phb_direct_score()` — new helper

**Step 1 — Event Type Tier → base score:**

| Tier | Events | Base |
|---|---|---|
| TIER_A | HYPERSCALER_ANCHOR, DEFENSE_MILITARY, FDA_READOUT, REGULATORY_DECISION, MAJOR_GOVERNMENT_AWARD | 80 |
| TIER_B | MNA, EARNINGS_GUIDANCE, EARNINGS_DATE, ANALYST_UPGRADE, INVESTOR_DAY | 70 |
| TIER_C | STRATEGIC_PARTNERSHIP, TECHNICAL_MILESTONE, PRODUCT_LAUNCH, PRODUCT_UPDATE, FINANCING | 58 |
| TIER_C | COMMERCIAL_CONTRACT | 48 |
| TIER_D | SPLIT_THIS_WEEK, DIVIDEND_THIS_WEEK, GENERIC_RSS, UNKNOWN | 28–30 |
| TIER_E | Bearish suppressed | 0 |

**Step 2 — Modifiers (all from cached `catalyst_primary_event`):**

| Modifier | Formula | Range |
|---|---|---|
| Materiality | `(mat - 0.5) × 20` | −10 to +10 |
| Confidence | `(conf - 0.5) × 10` | −5 to +5 |
| Ticker relevance | ≥0.95 → +6; 0.70–0.95 → +4; 0.30–0.70 → +2; <0.30 → −6 | −6 to +6 |
| Freshness (news) | 0–3d → +6; 4–14d → +4; 15–45d → +2; stale → 0 | 0 to +6 |
| Proximity (scheduled) | days_until ≤7 → +10; ≤30 → +6; ≤90 → +3 | 0 to +10 |
| Article count | ≥3 → +4; ≥2 → +2; 1 → 0 | 0 to +4 |

**Step 3:** `direct_score = clamp(base + all_modifiers, 0, 100)`

**Step 4:** `catalyst_alignment_points = direct_score / 100 × 15`

**Bearish conflict: direct_score forced to 0 before any modifiers.**

**Theme-policy fallback:** `score = 15 + (cat_score / 100) × 25` → maps cat_score to [15–40] range → [2.25–6 pts]

**No provider calls. No LLM calls. All data from cached `catalyst_primary_event`.**

---

## DISTRIBUTION BEFORE / AFTER

| Metric | Before (Phase A) | After (Phase B) | Change |
|---|---|---|---|
| n | 146 | 145 | — |
| Unique values | **4** | **52** | +48 |
| Min | 0.00 | 0.00 | = |
| Max | 15.00 | 15.00 | = |
| Mean | 11.338 | **8.691** | ↓ (correct — removed over-credit) |
| Median | 15.000 | **9.910** | ↓ |
| Std dev | 6.106 | **4.486** | ↓ (more evenly spread) |
| % at 0 | 8.9% | **8.3%** | ≈ |
| % at max (15) | **73.3%** | **3.4%** | ↓↓↓ |
| % in 10–14 | **0%** | **40.7%** | ↑↑↑ |
| % in 5–10 | **0%** | **29.7%** | ↑↑↑ |
| % in 2–5 | 18.5% | 17.9% | ≈ |

### Bucket distribution after Phase B (n=145)

| Bucket | Count | % | Tier |
|---|---|---|---|
| 15 (max) | 5 | 3.4% | All TIER_A (HYPERSCALER/DEFENSE) |
| 12–14.99 | 32 | 22.1% | TIER_A high-mat + TIER_B fresh |
| 10–11.99 | 27 | 18.6% | TIER_B/C moderate signals |
| 7–9.99 | 41 | 28.3% | TIER_C commercial contracts |
| 5–6.99 | 2 | 1.4% | TIER_C low-mat |
| 2–4.99 | 26 | 17.9% | TIER_D (theme_policy + unknown) |
| 0 | 12 | 8.3% | Bearish conflict |

---

## SAMPLE VALIDATION

### 18 known symbols

| Symbol | Old pts | New pts | Tier | Event Type | Bearish | Change | Correct? |
|---|---|---|---|---|---|---|---|
| AAOI | 15.0 | **10.64** | TIER_C | COMMERCIAL_CONTRACT | No | ↓ | ✅ Generic contract no longer max |
| AEHR | 15.0 | **13.79** | TIER_B | MNA | No | ↓ slight | ✅ M&A stays high |
| ALAB | 15.0 | **10.28** | TIER_C | COMMERCIAL_CONTRACT | No | ↓ | ✅ |
| ALGM | 15.0 | **9.91** | TIER_C | COMMERCIAL_CONTRACT | No | ↓ | ✅ |
| AMD | 0.0 | **0.0** | TIER_E | — | Yes | = | ✅ Bearish preserved |
| ASTS | 0.0 | **0.0** | TIER_E | — | Yes | = | ✅ Bearish preserved |
| AVAV | 0.0 | **0.0** | TIER_E | — | Yes | = | ✅ Bearish preserved |
| MU | 0.0 | **0.0** | TIER_E | — | Yes | = | ✅ Bearish preserved |
| ADEA | 2.03 | **2.76** | TIER_D | THEME_POLICY | No | ↑ slight | ✅ Theme-policy stays low |
| AKAM | 1.22 | **2.55** | TIER_D | THEME_POLICY | No | ↑ slight | ✅ |
| BB | 2.03 | **2.76** | TIER_D | THEME_POLICY | No | ↑ slight | ✅ |
| BTDR | 2.03 | **2.76** | TIER_D | THEME_POLICY | No | ↑ slight | ✅ |
| TSM | 15.0 | **14.73** | TIER_A | HYPERSCALER_ANCHOR | No | ↓ slight | ✅ Tier A stays near-max |
| VRT | 15.0 | **12.63** | TIER_B | MNA | No | ↓ | ✅ |
| ABCL | 0.0 | **0.0** | TIER_E | — | No catalyst | = | ✅ |
| CRWD | 15.0 | **10.68** | TIER_C | TECHNICAL_MILESTONE | No | ↓ | ✅ Tech milestone = TIER_C |
| HIMS | 15.0 | **12.08** | TIER_B | MNA | No | ↓ | ✅ Low-mat M&A scores TIER_B |
| PANW | 15.0 | **10.76** | TIER_C | STRATEGIC_PARTNERSHIP | No | ↓ | ✅ Strategic partner = TIER_C |

### Sample explanations (live API)

**AAOI (10.64/15):** "Moderate catalyst (Commercial Contract) via rss_v2. Published within last 3 days. Scored 10.6/15."

**TSM (14.73/15):** "High-conviction catalyst (Hyperscaler Anchor) via rss_v2. Published within last 3 days. Scored 14.7/15."

**ADEA (2.76/15):** "Theme-policy tailwind only; low direct catalyst confidence. Scored conservatively (2.8/15)."

**ASTS (0.0/15):** "Bearish catalyst conflict detected; catalyst points suppressed to zero."

**HIMS (12.08/15):** "Moderate-high catalyst (Mna) via rss_v2. Published within last 3 days. Scored 12.1/15."

---

## RANKING IMPACT

### Top 30 by catalyst points after Phase B

| # | Symbol | Cat pts | Tier | Event Type | Why |
|---|---|---|---|---|---|
| 1 | ANET | 15.00 | TIER_A | HYPERSCALER_ANCHOR | Max mat+conf+fresh |
| 2 | COHR | 15.00 | TIER_A | HYPERSCALER_ANCHOR | Same |
| 3 | ELVA | 15.00 | TIER_A | HYPERSCALER_ANCHOR | Same |
| 4 | RDW | 15.00 | TIER_A | DEFENSE_MILITARY | Major defense contract |
| 5 | ORCL | 15.00 | TIER_A | HYPERSCALER_ANCHOR | Same as ANET |
| 6 | AMAT | 14.82 | TIER_A | HYPERSCALER_ANCHOR | Slightly lower mat |
| 7 | TSM | 14.73 | TIER_A | HYPERSCALER_ANCHOR | Large foundry win |
| 8 | IREN | 13.98 | TIER_A | HYPERSCALER_ANCHOR | Lower corroboration |
| 9 | AEHR | 13.79 | TIER_B | MNA | High-mat M&A, fresh |
| 10 | APH | 13.79 | TIER_B | MNA | Same |
| 11–17 | CEVA/CIEN/ORA/RKLB/SITM/STM/UUUU | 13.43 | TIER_B | MNA | Moderate-mat M&A, fresh |
| 18 | TEL | 13.34 | TIER_B | EARNINGS_GUIDANCE | EG + high mat |
| 19 | TXN | 13.34 | TIER_B | EARNINGS_GUIDANCE | EG + high mat |
| 20 | NOK | 13.28 | TIER_A | HYPERSCALER_ANCHOR | Lower article count |
| 21–25 | WDC/EQT/AREC/DIOD/VICR | 12.79–13.20 | TIER_B | MNA/EG | Moderate |
| 26 | VRT | 12.63 | TIER_B | MNA | Low-mat M&A |
| 27 | DELL | 12.59 | TIER_B | EARNINGS_GUIDANCE | Upcoming EG |
| 28–30 | SYNA/HIMS/PANW area | 10.76–12.41 | TIER_B/C | MNA/Strategic | |

### Ranking changes summary

**Expected drops (correct):**
- AAOI: 15.0 → 10.6 (generic commercial contract no longer max)
- ALAB: 15.0 → 10.3 (same)
- ALGM: 15.0 → 9.9 (same)
- CRWD: 15.0 → 10.7 (technical milestone ≠ M&A)
- PANW: 15.0 → 10.8 (strategic partnership, not Tier A)

**Correctly stays high:**
- TSM: 15.0 → 14.7 (HYPERSCALER_ANCHOR = TIER_A)
- ANET: 15.0 → 15.0 (HYPERSCALER_ANCHOR, max modifiers)
- VRT: 15.0 → 12.6 (MNA = TIER_B, reasonable reduction)

**No suspicious catalyst inflation found** — top-15 are all genuine Tier A/B events with no generic RSS items.

---

## ACTIONABILITY SAFETY

### Safety check results

| Check | Expected | Result | Pass? |
|---|---|---|---|
| VRT remains READY | READY | v42=READY, cc=75.7, rflags=[] | ✅ |
| ALGM remains READY | READY | v42=READY, cc=72.9, rflags=[] | ✅ |
| TSM remains READY | READY | v42=READY, cc=88.6, rflags=[] | ✅ |
| ABCL not READY | NEAR_ACTIONABLE | v42=NEAR_ACTIONABLE, chase=True | ✅ |
| LITE not READY | NEAR_ACTIONABLE | v42=NEAR_ACTIONABLE, rflags=[LLC] | ✅ |
| LASR not READY | NEAR_ACTIONABLE | v42=NEAR_ACTIONABLE, rflags=[LLC] | ✅ |
| VECO not READY | NEAR_ACTIONABLE | v42=NEAR_ACTIONABLE, rflags=[LLC] | ✅ |
| 0 READY with LLC | 0 | LITE/LASR/VECO all blocked | ✅ |
| 0 READY with CHASE | 0 | ABCL has chase → NEAR_ACTIONABLE | ✅ |
| Actionability logic untouched | unchanged | Phase B only modifies `_score_catalyst_alignment_v42` | ✅ |

**No actionability regressions. All safety checks pass.**

---

## REMAINING CATALYST GAPS (Phase C candidates)

1. **Event proximity scoring not yet active for most symbols** — `days_until` is null for all rss_v2 events (only 2 scheduled events have it). Proximity bonus works but has no data to trigger for news events. Fix: surface days-to-event for earnings/FDA dates from calendar cache.

2. **TIER_C commercial contracts still cluster at 8–11 pts** — Because materiality_score from the classifier is narrow (0.34–0.87, not full 0–1 range), the modifier spread is only ±7. A commercial contract with mat=0.68 vs. mat=0.87 produces only ~4 pt difference in final score. Phase C: expose contract size / customer tier as a separate field.

3. **M&A events are mixed quality** — Some MNA events are actual acquisitions (AEHR at 13.8 = correct) while others may be rumors (lower confidence). Currently confidence_score (0.6–0.79) differentiates them partially. Phase C: add MNA status field (confirmed/rumored).

4. **Theme_policy items still produce only 2 distinct values** (2.55, 2.76) because LKG only has two cat_score values (8.1, 13.5) from theme_policy. These are correctly low. No fix needed — the formula is correct, the input data just has low variance.

5. **Intelligence score (news velocity/change) still wired to 0** — When implemented, restore blended formula: `catalyst_raw = direct_score × 0.75 + intelligence_score × 0.25`.

6. **FINANCING event type** — Currently mapped to TIER_C but "financing" is often bearish (equity dilution, downgrade). The bearish_conflict flag handles some cases (AVAV) but a FINANCING event without bearish_conflict still scores 58 base. Phase C: add bearish-proxy check for FINANCING type.

---

## FINAL REPORT — SUMMARY TABLES

### Distribution comparison

| Metric | Phase A (before) | Phase B (after) |
|---|---|---|
| Unique values | 4 | **52** |
| Mean | 11.34 | **8.69** |
| Median | 15.00 | **9.91** |
| % at max (15) | **73.3%** | **3.4%** |
| % in 10–14 | **0%** | **40.7%** |
| % in 5–10 | **0%** | **29.7%** |
| % at 0 | 8.9% | 8.3% |

### Investment validation (unchanged by Phase B)

Investment scoring was fixed in Phase A. Phase B touches only catalyst. Confirmed unchanged: ACLS=13.2, IONQ=9.45, ALAB=15.0, SMCI=12.75 all stable.

### Actionability safety

All 7 safety checks passed (detailed above).

---

## CATALYST PHASE B VERDICT

**PROVIDER_CALLS_USED:** NO
All data sourced from cached `catalyst_alignment_lkg.json` → `catalyst_primary_event` fields.

**LLM_CALLS_USED:** NO
Pure Python scoring logic. No LLM inference at scoring time.

**GENERIC_RSS_AUTO_MAX_FIXED:** YES
Before: any rss_v2 event → direct=100 → 15/15. After: COMMERCIAL_CONTRACT rss_v2 events with typical materiality (0.68) score 9.9–10.7/15. Only TIER_A events (hyperscaler anchor, defense) can reach near-max via legitimate earned modifiers.

**CATALYST_NOW_CONTINUOUS:** YES
52 distinct values across the 0–15 range. Full distribution across all buckets.

**UNIQUE_CATALYST_VALUES_AFTER:** 52

**PERCENT_AT_MAX_AFTER:** 3.4

**RANGE_10_TO_14_POPULATED:** YES
40.7% of symbols (59/145) score in the 10–14 range.

**RANGE_5_TO_10_POPULATED:** YES
29.7% of symbols (43/145) score in the 5–10 range.

**BEARISH_CONFLICT_SUPPRESSION_PRESERVED:** YES
AMD, ASTS, AVAV, MU all remain at 0.0. Bearish conflict zeroing applied before any modifier calculation.

**ACTIONABILITY_UNCHANGED:** YES
Phase B modifies only `_score_catalyst_alignment_v42`. All actionability gates, execution states, and READY/NEAR_ACTIONABLE classification logic are untouched. VRT/ALGM/TSM remain READY; LITE/LASR/VECO remain not READY with LLC flags.

**READY_SAFETY_CHECK_PASSED:** YES
No READY symbol with LOWER_LOW_CONFIRMED, CHASE_EXTENSION, or missing invalidation found.

**READY_FOR_FRONTEND_CATALYST_TOOLTIP:** YES
New fields live in API response:
- `catalyst_event_type` — normalized event type string
- `catalyst_event_tier` — TIER_A / TIER_B / TIER_C / TIER_D / TIER_E
- `catalyst_freshness_score` — freshness modifier applied
- `catalyst_relevance_score` — relevance modifier applied
- `catalyst_materiality_score` — materiality modifier applied
- `catalyst_reason_codes` — array of reason codes
- `catalyst_explanation` — plain-English explanation string (e.g. "Moderate catalyst (Commercial Contract) via rss_v2. Published within last 3 days. Scored 10.6/15.")
