---
name: Caelyn Confluence V4.2 Engine
description: V4.2 is the canonical Confluence scoring engine — Core=100, Bonus=25, Max=125. caelyn_confluence_score is sourced from V4.2 (not V4).
---

## Rule
V4.2 is the live canonical engine. `caelyn_confluence_score` and `caelyn_confluence_bucket` are always sourced from V4.2 via Phase 2 promotion in `build_confluence_snapshot()`. V4 fields are preserved as debug-only.

**Why:** V4.2 fixes semantic compression (un-capped normalization), splits social into 3 sections, folds theme policy into Theme, removes Prediction Markets bonus, and adds 3-pillar investment model.

**How to apply:**
- Any new scoring change → edit `backend/services/caelyn_confluence_v42.py`
- The Phase 2 promotion block in `confluence_v2_service.py` (line ~1906) is the single place that promotes V4.2 → canonical fields; don't add promotion logic elsewhere
- Always validate via the live server's `/confluence/v4-report` endpoint (re-computes V4.2 live); never trust a standalone subprocess for options-dependent data

## Score formula (V4.2)
- **Core = 100 max:**
  - Theme Alignment: 15 pts (policy multiplier folded in, not additive)
  - Stage Quality: 15 pts
  - Options Alignment: 20 pts (60% `options_current_composite_normalized` + 40% `options_direction_score`)
  - Technical Setup: 8 pts (pattern type × extension/support modifiers)
  - Entry/Exit: 12 pts (shelf state, distance, risk/reward)
  - Catalyst Alignment: 15 pts (75% scheduled event + 25% intelligence score)
  - Investment Alignment: 15 pts (3-pillar model)
- **Bonus = 25 max:**
  - Social: 15 pts (3 sections × 5 pts: Confluence, Acceleration, Fresh from `x_consensus_weekly.json`)
  - Whale/Insider: 5 pts (unavailable → 0)
  - Bottleneck: 5 pts (from `curated_anchor_bottlenecks`)
  - Prediction Markets: 0 (disabled)
  - Theme Policy: 0 (folded into Theme)

## 3-Pillar Investment Model
Fields sourced from Neon `watchlist_fundamentals` snapshot:
- **Financial Health:** gross_margin >= 40% OR (fcf_margin >= 10% AND debt_equity < 1.5)
- **Current Growth:** revenue_growth_q > 0 OR eps_growth > 0
- **Forward Growth:** rev_growth_next_q > 0 OR eps_growth_next_q > 0
- 3/3 pillars → 15 pts (`ELITE`); 2/3 → 10 pts (`STRONG`); 1/3 → 5 pts (`DEVELOPING`); 0/3 → 0 pts

## Social Sections Map
`build_social_sections_map()` in `caelyn_confluence_v42.py` reads `data/x_consensus_weekly.json`:
- `raw.consensus_picks[*].ticker` → Confluence section
- `raw.hype_radar[*].key_tickers[*]` → Acceleration section
- `raw.fresh_trades[*].ticker` → Fresh section

## Bucket thresholds (out of 125)
ACTIONABLE ≥ 85 | NEAR_ACTIONABLE ≥ 60 | INVESTMENT_QUALITY (3 pillars, score < 60) | CONFLUENCE_AT_SUPPORT (at/near support) | WATCH_FOR_RESET | RISK_CONFLICT (major_llc) | NO_CLEAR_CONFLUENCE (default)

## Key files
- Engine: `backend/services/caelyn_confluence_v42.py`
- Wiring + promotion: `backend/services/confluence_v2_service.py` (line ~1882)
- Alignment endpoint fields: `backend/services/watchlist_router.py` (line ~4344)
- Diagnostic report: `GET /{watchlist_id}/confluence/v4-report` (re-computes V4.2 live)

## Validated distribution (379-ticker universe, 2026-07-13)
- 379/379 scored, 0 errors
- avg score 31.5/125 (83% of tickers lack options → max attainable ~80-105)
- confidence avg 75.3%; top scorer CRDO 70.2/125 (3 investment pillars)
