---
name: Catalyst Phase B continuous scoring
description: Phase B replaces binary event→100 with graduated TIER_A–E scoring using catalyst_primary_event rich fields; report at backend/data/confluence_catalyst_phase_b_report.md
---

# Catalyst Phase B — V4.2.7

**Why:** Phase A fixed the ceiling (11.25→15) but scoring was still binary — 73% of symbols scored exactly 15 because any rss_v2 event → direct=100. Generic commercial_contract RSS items were indistinguishable from hyperscaler anchor deals.

## Implementation

Two new helpers in `backend/services/caelyn_confluence_v42.py` (before `_score_catalyst_alignment_v42`):
- `_catalyst_phb_direct_score(primary_event, source, cat_score_fallback, is_scheduled)` → graduated 0–100
- `_catalyst_phb_theme_policy_score(cat_score)` → maps cat_score to [15–40] Tier D range
- `_catalyst_phb_explanation(...)` → plain-English string for frontend tooltip

`_score_catalyst_alignment_v42` now calls `_catalyst_phb_direct_score` from `catalyst_primary_event` (146/146 rows populated) instead of hard-coding `direct_score = 100`.

## Tier system (base scores)

| Tier | Event types | Base |
|---|---|---|
| TIER_A | HYPERSCALER_ANCHOR, DEFENSE_MILITARY, FDA_READOUT, REGULATORY_DECISION | 80 |
| TIER_B | MNA, EARNINGS_GUIDANCE, EARNINGS_DATE, ANALYST_UPGRADE, INVESTOR_DAY | 70 |
| TIER_C | STRATEGIC_PARTNERSHIP, TECHNICAL_MILESTONE, PRODUCT_LAUNCH, FINANCING | 58 |
| TIER_C | COMMERCIAL_CONTRACT | 48 |
| TIER_D | SPLIT_THIS_WEEK, DIVIDEND_THIS_WEEK, UNKNOWN | 28–30 |

## Modifiers (all from catalyst_primary_event, no provider calls)

- `materiality_score`: (mat−0.5)×20 → [−10,+10]
- `confidence_score`: (conf−0.5)×10 → [−5,+5]
- `ticker_relevance_score`: ≥0.95→+6, ≥0.70→+4, ≥0.30→+2, <0.30→−6
- Freshness (published_at/catalyst_date): 0–3d→+6, 4–14d→+4, 15–45d→+2
- Proximity (days_until, scheduled only): ≤7d→+10, ≤30d→+6, ≤90d→+3
- article_count: ≥3→+4, ≥2→+2

## Results (n=145 LKG)

- Before: 4 unique values, 73.3% at max, 10–14 range empty
- After: 52 unique values, 3.4% at max, 40.7% in 10–14, 29.7% in 5–10

## Explainability fields added to snapshot builder

`catalyst_event_type`, `catalyst_event_tier`, `catalyst_freshness_score`, `catalyst_relevance_score`, `catalyst_materiality_score`, `catalyst_reason_codes`, `catalyst_explanation` — all live in API response and wired in snapshot builder at line ~2207.

## How to Apply

- **Never** re-introduce binary `direct_score = 100` for rss/scheduled events
- `catalyst_primary_event` is 146/146 populated — always the primary scoring source
- Intelligence score path still returns 0 (placeholder) — formula comment says restore `direct×0.75 + intel×0.25` when live
- theme_policy rows have no primary_event rich data → use `_catalyst_phb_theme_policy_score(cat_score)` fallback

## Known remaining gaps (Phase C)

- `days_until` null for all rss_v2 events → proximity bonus has no data for news events
- FINANCING event type may need bearish-proxy check (equity dilution scenarios)
- MNA events need confirmed/rumored status field to differentiate quality
