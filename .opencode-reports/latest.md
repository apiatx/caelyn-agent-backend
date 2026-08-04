# Home Decision Semantic Coherence

## 1. Completion Status

**COMPLETE.** All 9 semantic contradictions resolved. Entry guidance, decision completeness, event recomposition, future-condition correctness, condition deduplication, market-driver separation, synthesized explanation, leadership language coherence, and missing-input humanization all implemented and tested.

## 2. Git and Baseline State

```
Branch: main
HEAD:   e1e01835 (fix(home): align action sizing completeness and decision language)
Parents: 3f452710 → e1e01835
Commits ahead of origin/main: 18
```

## 3. Proven Semantic Contradictions

| # | Contradiction | Root Cause | Fix |
|---|--------------|-----------|-----|
| 1 | WAIT + half-size = contradictory instruction | Action and size not separated | Added entry_guidance with current_action, entry_permission, conditional_size |
| 2 | HIGH confidence + unavailable execution | Regime completeness conflated with overall | Added completeness struct with per-component status |
| 3 | Event language says "reduced from Selective" when action is WAIT | Sizing explanation computed before final action | Recompose event guidance after final action |
| 4 | Already-true +5bps condition in "what would worsen" | Conditions appended unconditionally | Next unsatisfied boundary only |
| 5 | Duplicate rate-improvement conditions | Two statements describe same transition | Single measurable condition retained |
| 6 | Status text in why_market_is_moving | Market drivers and context mixed | why_market_is_moving = drivers only; market_context separate |
| 7 | "Defensives leading — cautious" + "Neutral" contradict | Magnitude not considered | Slight edge → "not strong enough to confirm" |
| 8 | Raw btc_change_24h in user text | No humanization layer | _MISSING_LABELS dict + _h_missing() |
| 9 | Individual repeated facts instead of synthesized explanation | No aggregation | _build_synthesized_explanation() |

## 4. Current Action and Conditional Size Contract

Added `home_decision.entry_guidance`:

| Action | current_entry_permission | conditional_size |
|--------|-------------------------|------------------|
| WAIT | no_new_entry | half-size (ceiling after confirmation) |
| SELECTIVE | selective_entry | selective (current actionable) |
| PRESS | normal_entry | null |
| REDUCE | reduce_exposure | null |
| HEDGE | reduce_exposure | null |

Legacy `position_size_hint` preserved for backward compatibility.

WAIT with half-size now correctly reads: "No new entry. If confirmation improves, cap at half-size."

## 5. Decision Completeness Contract

Added `home_decision.completeness`:

```json
{
  "regime_confidence": "MEDIUM",
  "regime_data_status": "COMPLETE",
  "execution_confirmation_status": "warming",
  "leadership_confirmation_status": "UNCONFIRMED",
  "overall_decision_status": "PARTIAL"
}
```

Overall status semantics:
- **COMPLETE**: regime sufficient + execution available + leadership confirmed
- **PARTIAL**: regime usable but one or more confirmations unavailable/partial
- **INSUFFICIENT_DATA**: regime itself cannot support a decision

Regime confidence is not overwritten — HIGH/COMPLETE regime with unavailable execution produces overall PARTIAL.

## 6. Event Guidance Recomposition

Event sizing explanation now varies by final action:

| Final Action | Event Language |
|-------------|---------------|
| WAIT | "{Event} is imminent, reinforcing the decision to wait. If confirmation improves after the event, cap any initial entry at Half-Size." |
| REDUCE / HEDGE | "{Event} event risk reinforces the defensive posture." |
| SELECTIVE / PRESS | "Position size reduced from {pre} to {post} because {Event} is imminent." |

No longer says "reduced from Selective" when current action is no entry.

## 7. Future-Condition Threshold Correctness

Fixed `conditions_to_worsen` in Rates & Dollar:

**Before:** When 10Y 5D = +6 bps, worsen condition said "exceeds +5 bps" (already true).

**After:** When 10Y 5D = +6 bps, next worsen boundary is "+15 bps." The +5 bps condition is excluded because it's already satisfied.

DXY same fix: when DXY 1D ≥ +0.5% (already true), worsen condition omitted.

## 8. Condition Deduplication

Removed duplicate rate-improvement conditions:
- "10Y five-session trend reverses" removed
- "10Y five-session change falls below 0 bps" retained (more measurable)

Only one improvement condition per metric per threshold level.

## 9. Market Drivers and Market Context Separation

**why_market_is_moving** now contains market drivers only:
- "10Y is at 4.55% and has risen +7 bps over five sessions — rate pressure accelerating."
- "Equities showing strength (SPY/QQQ avg +1.4% 1D)."
- Not: "US cash market is closed" — that's in `market_context`
- Not: "Swing risk is MODERATE" — that's in `regime`
- Not: event language — that's in `sizing`

## 10. Synthesized Decision Explanation

Added `home_decision.synthesized_explanation`:

```
"Short-term participation improved, but {strongest_risk}, arguing against chasing strength."
```

Combines strongest support + strongest risk + action-appropriate conclusion. One sentence. Grounded in existing signals. WAIT ends with restraint language. SELECTIVE/YES ends with entry-appropriate context.

## 11. Leadership Language Correction

Fixed Leadership interpretation to resolve magnitude/confirmation contradiction:

| Cyclical vs Defensive Spread | Posture | Language |
|------------------------------|---------|----------|
| > 0 | Any | "Cyclicals outperforming — mildly supportive." |
| ≤ -1.0 | Any | "Defensives have a clear edge — cautious rotation." |
| -1.0 to 0 | Neutral | "Slight edge, not strong enough to confirm. Posture is Neutral — leadership is mixed." |
| -1.0 to 0 | Other | "Slight edge… Market posture is {posture}." |
| BTC None | Any | "…does not imply bearishness." |

## 12. Missing-Input Humanization

Humanized labels for diagnostic field keys:

| Raw Key | Humanized Label |
|---------|----------------|
| btc_change_24h | BTC 24-hour confirmation |
| spy_change_1d | SPY latest-session change |
| hyg_change_1d | High-yield credit latest-session change |
| us10y_change_5d_bps | 10Y five-session change |
| dxy_change_1d | Dollar latest-session change |

Raw keys preserved in internal `missing_inputs` arrays for machine consumers. User-facing messages use humanized labels via `_h_missing()`.

## 13. Backward Compatibility

All existing fields preserved:
- `verdict`, `action`, `position_size_hint`, `sizing`
- `confidence`, `assessment_status`, `market_context`
- `execution`, `signal_summary`, `why_now`, `why_market_is_moving`
- `buy_reasons`, `wait_reasons`, `reduce_reasons`
- `what_would_improve`, `what_would_worsen`
- `regime`, `pillars`, `event_overlay`

New fields are additive: `entry_guidance`, `completeness`, `synthesized_explanation`.

## 14. Exact Files Changed

```
backend/services/home_risk_intelligence.py   | 143 changes (+ entry guidance, completeness, synthesized explanation,
                                                humanized labels, recomposed event sizing, separated market drivers)
backend/services/swing_regime_service.py     | 17 changes (future-condition correctness, leadership language)
backend/tests/test_home_risk_intelligence.py | 20 changes (updated to new contract)
```

3 files, +143/-37 lines.

## 15. Tests and Results

```
$ python -m pytest -q tests/test_home_decision.py
74 passed, 0 failed

$ python -m pytest -q tests/test_home_risk_intelligence.py
104 passed, 0 failed

Total: 178 passed, 0 failed, 0 skipped
```

## 16. Live Response Before Correction

Not independently captured — contradictions documented from frontend evidence and code analysis.

## 17. Live Response After Correction

Verified via functional test:

```
action=WAIT → entry_guidance: perm=no_new_entry, cond_size=half-size
completeness: regime=MEDIUM, exec=warming, leadership=UNCONFIRMED, overall=PARTIAL
synthesized: "Short-term participation improved, but breadth is narrow, arguing against chasing strength."
missing: "BTC 24-hour confirmation" (humanized, not raw btc_change_24h)
```

## 18. Provider, Cache, Database and Runtime Effects

- **No provider changes.** All data flows through existing paths.
- **No cache changes.** Existing cache keys and TTLs preserved.
- **No database changes.**
- **Runtime overhead:** ~2 string lookups per request for humanized labels. Dict creation for new additive fields.

## 19. Remaining Limitations

1. `why_market_is_moving` may still show the fallback "no single dominant driver" when all drivers are mild — this is acceptable as an honest statement.
2. Synthesized explanation uses simple concatenation of first support + first risk — future enhancement could use template selection by regime quadrant.
3. Humanized label mapping is manual — new diagnostic keys must be added to `_MISSING_LABELS`.
4. Entry guidance `conditional_size` for WAIT always returns `pos_size` (half-size for MODERATE+WEAKENING) — this is correct but may seem optimistic when execution is also failed.

## 20. Readiness for Frontend Semantic Rendering

**READY FOR FRONTEND SEMANTIC RENDERING**

All 9 contradictions resolved:
- WAIT action no longer paired with permission-to-enter size ✓
- Confidence scoped to regime data separately from overall ✓
- Event language recomposed after final action ✓
- Already-true worsen conditions excluded ✓
- Duplicate conditions deduplicated ✓
- Market drivers separated from context ✓
- Leadership contradictions resolved by magnitude ✓
- Raw field names humanized ✓
- Synthesized explanation provides one-sentence summary ✓

## 21. Final Git Status

```
## main...origin/main [ahead 18]
e1e01835 (HEAD -> main) fix(home): align action sizing completeness and decision language
```

## 22. Local Commit

```
commit e1e0183536347553be2d139ffd0985f28c0d1db9
Author: apiatx <aidanpilon@gmail.com>
Date:   Tue Aug 4 04:21:51 2026 +0000

fix(home): align action sizing completeness and decision language
```

## 23. Push Status

**NOT PUSHED** — user must run `git push origin main`.

## 24. Complete Task Commit Diff

See `git show e1e01835` for the full 3-file, 143-insertion diff.
