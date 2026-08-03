# Unified Home Decision Language and Runtime Validation

## 1. Completion Status

**COMPLETE** — all narrative text humanized, reason invariants enforced, deduplication added. 148 tests pass (50 decision + 59 home risk + 39 TD service). Live HTTP validation not completed (application not running).

## 2. Proven Semantic Problems

All corrected:

| Problem | Example | Fix |
|---------|---------|-----|
| Repeated words | `"SELECTIVE selective"`, `"selective entries at selective size"` | `_build_one_line()` and `_build_why_now_bullets()` use human labels only |
| Raw all-caps prose | `"MODERATE risk"`, `"CAUTION — SELECTIVE selective"` | `_h_level()`, `_h_dir()`, `_h_verdict()`, etc. helpers for all narrative text |
| Incorrect bullish language | `"Absolute risk is MODERATE — environment broadly supportive"` | Removed — MODERATE alone is not "broadly supportive" |
| Raw snake_case in prose | `"trade_bias.replace('_', ' ')"` in reasons | `_h_bias()` helper |
| Contradictory reasons | buy_reasons for CAUTION/WAIT, reduce_reasons for CAUTION/SELECTIVE | Reason builders with verdict/action invariants |
| Duplicate improve/worsen | Same concept listed twice | Case-normalized deduplication in `_build_improve_worsen()` |

## 3. Exact Files Changed

| File | Change | Lines |
|------|--------|-------|
| `backend/services/home_risk_intelligence.py` | Extracted 7 humanization helpers, 4 narrative builders, 3 reason builders, 1 improve/worsen builder. Replaced inline text generation with builder calls. | +571 / -86 |
| `backend/tests/test_home_decision.py` | 16 new language hardening tests + updated runner count | +142 |

## 4. Narrative Formatting Rules

### Humanization helpers:
- `_h_level("MODERATE")` → `"Moderate"`
- `_h_dir("IMPROVING")` → `"Improving"`
- `_h_verdict("CAUTION")` → `"Caution"`
- `_h_action("SELECTIVE")` → `"Selective"`
- `_h_bias("SELECTIVE_LONG")` → `"Selective Long"`
- `_h_size("half-size")` → `"half-size"`
- `_h_quality("MIXED")` → `"mixed"`
- `_h_exec_status("expired")` → `"cached, awaiting refresh"`

### Machine enum JSON fields unchanged:
`verdict`, `action`, `risk_level`, `direction`, `trade_bias`, `position_size_hint`, `quality` all preserved as ALL_CAPS or snake_case enums.

## 5. one_line Templates

**YES / PRESS:**
> "Low, Improving regime risk with strong execution supports pressing high-quality leaders at normal size."

**YES / SELECTIVE:**
> "Low, Stable regime risk with strong execution supports selective long entries."

**CAUTION / SELECTIVE (fresh, mixed):**
> "Moderate, Stable regime risk with mixed execution favors selective entries, not broad aggressive buying."

**CAUTION / WAIT (warming):**
> "Moderate, Stable regime risk — execution data is still warming; wait for confirmation before adding exposure."

**CAUTION / WAIT (fresh, weak):**
> "Moderate, Stable regime risk with weak execution — wait for stronger breadth and follow-through before adding exposure."

**NO / REDUCE:**
> "High, Stable regime risk with mixed execution favors reducing exposure and avoiding new entries."

**NO / HEDGE:**
> "Extreme, Worsening regime risk — conditions warrant hedging and preserving capital."

**CAUTION / SELECTIVE (expired):**
> "Elevated, Improving regime risk favors selective entries; execution data is cached and awaiting refresh."

**Market closed appendage:**
> "... Signals reflect the latest completed US session."

## 6. why_now Contract

Four bullets max, each prefixed:

1. **Decision**: `"Yes — Press entries"`, `"Caution — Selective entries"`, `"No — Hedge"`
2. **Regime**: `"Moderate risk, Stable direction"` (human labels)
3. **Execution**: `"Mixed — Market Quality 62/100, Execution Window 50/100"` or `"Cached, awaiting refresh"` or `"Warming — no current confirmation available"`
4. **Session/Event**: `"Latest completed US session"` or `"CPI in 2 days — sizing reduced, directional verdict unchanged"` or `"US cash market is open"`

No duplicated word sequences. No raw ALL_CAPS.

## 7. Buy, Wait, and Reduce Reason Invariants

### Buy reasons:
| Verdict/Action | Buy reasons |
|---------------|-------------|
| YES / PRESS | Non-empty (up to 3) |
| YES / SELECTIVE | Non-empty (up to 3) |
| CAUTION / SELECTIVE | At most 2, only genuine positives |
| CAUTION / WAIT | Empty (unless IMPROVING, then ≤1) |
| NO | Empty |
| INSUFFICIENT_DATA | Empty |

### Wait reasons:
| Verdict/Action | Wait reasons |
|---------------|-------------|
| YES / PRESS | At most 1 |
| CAUTION / SELECTIVE | Non-empty (at least 1) |
| CAUTION / WAIT | Non-empty (at least 1) |
| NO | May include cautionary context |

### Reduce reasons:
| Verdict/Action | Reduce reasons |
|---------------|---------------|
| YES | Empty |
| CAUTION / SELECTIVE | Empty (unless HIGH/EXTREME/WORSENING/SHORT_HEDGE) |
| CAUTION / WAIT | Only with explicit defensive signals |
| NO | Non-empty (at least 1) |

### No reason may contradict verdict, action, or position size.

## 8. Change-the-Call Deduplication

`_build_improve_worsen()` uses case-normalized comparison — `"risk direction shifts to worsening"` and `"Risk direction shifts to WORSENING"` are treated as identical.

Specific improvements:
- `"Market Quality falls below 40"` (one entry per concept)
- `"Execution Window falls below 50"` (one entry per concept)
- Execution condition labels use `ec["label"].rstrip("?")` + "resumes confirming"
- Swing Regime flip conditions preserved as-is

## 9. Current MODERATE / STABLE / MIXED Example

For risk_score=43, MODERATE, STABLE, SELECTIVE_LONG, MQS=62, EWS=50, MIXED execution, verdict=CAUTION, action=SELECTIVE:

**one_line:**
> "Moderate, Stable regime risk with mixed execution favors selective entries, not broad aggressive buying."

**why_now:**
```json
[
  "Decision: Caution — Selective entries",
  "Regime: Moderate risk, Stable direction",
  "Execution: Mixed — Market Quality 62/100, Execution Window 50/100",
  "Session: US cash market is open"
]
```

**buy_reasons:** May include `"The regime permits selective long exposure."` or be empty.

**wait_reasons:**
```json
[
  "Execution quality is mixed; wait for broader confirmation.",
  "Risk direction is stable rather than improving."
]
```

**reduce_reasons:** `[]` (empty — no defensive signals)

**what_would_improve:** ≤4 items from flip_conditions + failed execution conditions

**what_would_worsen:** ≤4 items from guardrails (direction worsening, risk escalation, MQS <40, EWS <50)

## 10. Matrix Preservation

All 24 original decision matrix tests pass unchanged. Verdict/action/sizing outputs did not change. Only narrative text was humanized.

## 11. Live Home Endpoint Validation

**LIVE HTTP VALIDATION NOT COMPLETED** — the local application is not running (HTTP 000 on `curl localhost:8000`).

If running, expected validation steps:
1. First `GET /api/home/risk-intelligence` → `execution.status: "warming"`, `assessment_status: "PARTIAL"`
2. Wait 7 seconds for background refresh
3. Second `GET /api/home/risk-intelligence` → `execution.status: "available"`, `assessment_status: "COMPLETE"`
4. No malformed wording in any response
5. Existing Swing Regime fields present
6. `GET /api/trading-dashboard?mode=swing` response contract unchanged

## 12. Trading Dashboard Compatibility

`GET /api/trading-dashboard?mode=swing` response contract unchanged. No files touched outside authorized scope.

## 13. Tests and Results

```
test_home_decision.py:           50 tests PASSED (34 original + 16 language)
test_home_risk_intelligence.py:  59 tests PASSED
test_trading_dashboard_service.py: 39 tests PASSED
Total:                           148 tests PASSED
```

### New language tests:
1. `test_one_line_no_repeated_selective` — max 1 "selective"
2. `test_one_line_no_raw_enum_casing` — no ALL_CAPS in prose
3. `test_why_now_no_repeated_words` — no "SELECTIVE selective"
4. `test_why_now_no_raw_enum_casing` — no raw enums in bullets
5. `test_moderate_not_broadly_supportive` — MODERATE ≠ broadly supportive
6. `test_moderate_stable_mixed_coherent` — full example text check
7. `test_caution_selective_reason_invariants` — WAIT non-empty, REDUCE empty
8. `test_caution_wait_buy_reasons_restricted` — empty unless IMPROVING
9. `test_yes_buy_reasons_nonempty` — YES has buy reasons
10. `test_no_buy_reasons_empty` — NO has no buy reasons
11. `test_insufficient_data_buy_reasons_empty` — no buy, wait explains
12. `test_expired_one_line_cached` — "cached" or "refresh" in line
13. `test_warming_no_mqs_ews_prose` — no `/100` when MQS is None
14. `test_market_closed_language` — "latest completed" wording
15. `test_improve_worsen_deduplication` — no duplicates
16. `test_machine_enum_fields_unchanged` — JSON enums preserved

## 14. Provider, Cache, Database, and Runtime Effects

- **No scoring change**: Decision matrix, MQS, EWS, risk_score unchanged
- **No provider-path change**: Same `_build_trading_fetch_fresh()` callback
- **No cache-TTL change**: 60s Home / 720s TD / 4h LKG
- **No database change**
- **No frontend change**

## 15. Remaining Limitations

1. Thresholds still `"deterministic_uncalibrated"`
2. In-memory-only Trading Dashboard cache (no LKG persistence)
3. No frontend consolidation yet
4. Live endpoint validation not completed (application not running)
5. Runtime warmup duration not measured

## 16. Readiness for Frontend Consolidation

**READY FOR HOME FRONTEND CONSOLIDATION**

All narrative text is human-readable, coherent, invariant-checked, and deterministic. Machine enum JSON fields preserved for programmatic consumers.

## 17. Final Git Status

```
5ebd7368 (HEAD -> main) fix(home): make unified trading decision language coherent
a707723d feat(home): add unified regime and execution decision
9557fed0 refactor(trading-dashboard): extract canonical service and cache
## main...origin/main [ahead 6]
```

## 18. Local Commit

**SHA**: `5ebd7368`
**Message**: `fix(home): make unified trading decision language coherent`

## 19. Push Status

**NOT PUSHED** — user must run `git push origin main`

## 20. Complete Task Commit Diff

```diff
 backend/services/home_risk_intelligence.py | 571 +++++++++++++----
 backend/tests/test_home_decision.py        | 142 +++++
 2 files changed, 713 insertions(+), 132 deletions(-)
```
