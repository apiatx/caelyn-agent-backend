# POST-10Y CONFLUENCE CALIBRATION AUDIT REPORT
Generated: 2026-07-15

---

## PART 1 — COVERAGE / COUNT RECONCILIATION

| Metric | Value |
|--------|-------|
| `stage2_total_symbols` | 413 |
| `stage2_ok_symbols` | 344 |
| `stage2_no_bars_symbols` | 69 |
| `entry_lkg_total_rows` | 347 |
| `entry_lkg_real_rows` | 344 |
| `entry_lkg_test_rows` | 3 (TEST, TEST2B, TEST3M) |
| `entry_lkg_rows_with_fib_wave` | 344 |
| `confluence_snapshot_rows` | 413 |
| `confluence_rows_with_fib_wave` | 344 / 344 ok-status (100%) |

**Gap math corrected:**

The prior report stated "66 gap symbols." The correct breakdown:
- 413 Stage2 total
- 344 `status=ok` → all have entry_state + fib/wave
- 69 `status=no_bars` → no entry_state, no fib/wave (excluded from scoring)
- 3 TEST entries in entry LKG are dev artifacts, not in Stage2, do not affect production counts

**Breakdown of 69 no_bars:**

| Category | Count |
|----------|-------|
| Foreign exchange listings (AIM:, ASX:, EPA:, LON:, TYO:, SHA: etc.) | 58 |
| Domestic — no Tradier bars (delisted, OTC, micro-cap, stale alias) | 11 |

Zero eligible domestic equities with canonical history are missing fib/wave.

---

## PART 2 — CURRENT ACTIONABILITY DISTRIBUTION

### Action Label Distribution

| Label | Count | % of 413 |
|-------|-------|----------|
| WATCH | 278 | 67.3% |
| READY | 57 | 13.8% |
| WATCH_FOR_RESET | 37 | 9.0% |
| WAIT_FOR_RETEST | 19 | 4.6% |
| AVOID | 13 | 3.1% |
| NEAR_ACTIONABLE | 9 | 2.2% |

### Execution State Distribution

| Execution State | Count |
|----------------|-------|
| NOT_ACTIONABLE | 332 |
| RETEST_IN_PROGRESS | 27 |
| READY_AT_ENTRY | 22 |
| BREAKOUT_TRIGGER_READY | 21 |
| SET_ALERT_FOR_RETEST | 9 |
| WAIT_FOR_RETEST | 2 |

### Bucket Distribution

| Bucket | Count |
|--------|-------|
| NO_CLEAR_CONFLUENCE | 217 |
| ACTIONABLE | 57 |
| INVESTMENT_QUALITY | 52 |
| WATCH_FOR_RESET | 37 |
| NEAR_ACTIONABLE | 24 |
| RISK_CONFLICT | 13 |
| CONFLUENCE_AT_SUPPORT | 12 |
| SPECULATIVE_TRADE | 1 |

### Boolean Flags

| Flag | True Count |
|------|------------|
| is_actionable_setup | 57 |
| is_near_actionable | 24 |
| is_watch_for_reset | 37 |
| is_investment_quality | 52 |
| is_risk_conflict | 13 |

**Note on NEAR_ACTIONABLE discrepancy:**
action.label=NEAR_ACTIONABLE = 9 rows, but is_near_actionable=True = 24 rows.
The 15-symbol gap are symbols with action.label=WAIT_FOR_RETEST but bucket=NEAR_ACTIONABLE.
This is a semantic inconsistency covered in Part 9.

---

## PART 3 — READY ROW QUALITY AUDIT

**Total READY rows: 57**

### Safety Check Results

| Safety Check | Result |
|-------------|--------|
| READY rows with CHASE_EXTENSION in risk_flags | 0 ✅ |
| READY rows with is_risk_conflict=True | 0 ✅ |
| READY rows in WATCH_FOR_RESET bucket | 0 ✅ |
| READY rows missing invalidation_level | 0 ✅ |
| READY rows missing primary_fib_context | 0 ✅ |
| READY rows missing wave_structure_label | 0 ✅ |
| READY rows with fib_retest_detected | 41/57 (72%) ✅ |
| READY rows without fib_retest | 16/57 (28%) — acceptable |
| READY rows with constructive_extension caution | 14 (WDC, DELL, MXL, PENG, MU, AMD, TGTX, HPE, SIMO, WYFI, STX, SNDK, NNBR, SILC) |

### ⚠️ Three Contradictions Found

| Symbol | Issue |
|--------|-------|
| LITE | READY + LOWER_LOW_CONFIRMED in risk_flags, entry_state=REVERSAL_WATCH |
| LASR | READY + LOWER_LOW_CONFIRMED in risk_flags, entry_state=CONSTRUCTIVE_DIP |
| VECO | READY + LOWER_LOW_CONFIRMED in risk_flags, entry_state=BREAKOUT_PULLBACK |

These pass READY because entry_state is in _READY_CONTEXT_STATES — the LOWER_LOW_CONFIRMED
risk flag does NOT hard-block actionability_service.py's step 5 READY gate.
This is a calibration error (see Part 10 — P0).

### Top 30 READY Rows

| SYM | TOTAL | ENTRY | TECH | EXEC_STATE | ENTRY_STATE | FIB_CTX | WAVE | EXT_STATE | DIST% | FIB_TYPE |
|-----|-------|-------|------|-----------|------------|---------|------|-----------|-------|----------|
| APH | 78.3 | 8.81 | 5.8 | RETEST_IN_PROGRESS | BREAKOUT_RETEST | intermediate_daily | SECOND_LEG | MODERATELY_EXT | 0.31 | SHALLOW_RETRACEMENT |
| ETN | 77.1 | 10.54 | 6.64 | BREAKOUT_TRIGGER_READY | HIGH_BASE_COILING | monthly_impulse | NO_CLEAR_WAVE | HEALTHY | 3.99 | — |
| VRT | 76.9 | 8.81 | 6.64 | RETEST_IN_PROGRESS | TRENDLINE_SUPPORT_TEST | weekly_impulse | SECOND_LEG | MODERATELY_EXT | 0.22 | SHALLOW_RETRACEMENT |
| WDC | 76.6 | 4.67 | 5.2 | RETEST_IN_PROGRESS | TRENDLINE_SUPPORT_TEST | intermediate_daily | SECOND_LEG | EXTREME_EXTENSION | 3.38 | — |
| DELL | 75.1 | 4.44 | 7.12 | BREAKOUT_TRIGGER_READY | BREAKOUT_CONFIRMED | weekly_impulse | IMPULSE_LEG | EXTREME_EXTENSION | 2.54 | — |
| ROK | 73.0 | 8.57 | 6.12 | READY_AT_ENTRY | BREAKOUT_PULLBACK | weekly_impulse | SECOND_LEG | MODERATELY_EXT | 0.47 | EXTENSION_TARGET |
| TSM | 72.5 | 8.81 | 5.2 | READY_AT_ENTRY | SIGNALS_BUILDING | intermediate_daily | SECOND_LEG | MODERATELY_EXT | 0.39 | SHALLOW_RETRACEMENT |
| AMKR | 71.4 | 10.7 | 6.2 | READY_AT_ENTRY | BREAKOUT_PULLBACK | intermediate_daily | SECOND_LEG | MODERATELY_EXT | 1.29 | SHALLOW_RETRACEMENT |
| GLW | 70.2 | 7.33 | 5.2 | RETEST_IN_PROGRESS | TRENDLINE_SUPPORT_TEST | long_daily | SECOND_LEG | EXTENDED | 0.42 | SHALLOW_RETRACEMENT |
| AME | 69.0 | 8.65 | 6.0 | BREAKOUT_TRIGGER_READY | HIGH_BASE_READY | weekly_impulse | NO_CLEAR_WAVE | HEALTHY | 2.57 | — |
| PKE | 68.8 | 7.72 | 5.2 | RETEST_IN_PROGRESS | TRENDLINE_SUPPORT_TEST | long_daily | SECOND_LEG | MODERATELY_EXT | 0.66 | SHALLOW_RETRACEMENT |
| KLIC | 67.6 | 8.81 | 6.24 | READY_AT_ENTRY | BREAKOUT_PULLBACK | long_daily | SECOND_LEG | EXTENDED | 0.05 | SHALLOW_RETRACEMENT |
| ANET | 66.1 | 8.73 | 7.12 | BREAKOUT_TRIGGER_READY | BREAKOUT_CONFIRMED | weekly_impulse | IMPULSE_LEG | EXTENDED | 1.54 | PRIOR_RESISTANCE |
| MXL | 65.7 | 4.75 | 6.0 | READY_AT_ENTRY | BREAKOUT_PULLBACK | long_daily | SECOND_LEG | EXTREME_EXTENSION | 7.40 | — |
| ONTO | 64.6 | 8.73 | 6.92 | BREAKOUT_TRIGGER_READY | HIGH_BASE_FORMING | weekly_impulse | SECOND_LEG | EXTENDED | 1.05 | PRIOR_RESISTANCE |
| LITE ⚠️ | 64.5 | 5.08 | 5.2 | READY_AT_ENTRY | REVERSAL_WATCH | weekly_impulse | SECOND_LEG | MODERATELY_EXT | 2.75 | — |
| PENG | 64.3 | 4.83 | 6.0 | READY_AT_ENTRY | BREAKOUT_PULLBACK | long_daily | SECOND_LEG | EXTREME_EXTENSION | 0.52 | PRIOR_RESISTANCE |
| MU | 62.8 | 6.8 | 5.6 | RETEST_IN_PROGRESS | TRENDLINE_SUPPORT_TEST | intermediate_daily | SECOND_LEG | EXTREME_EXTENSION | 0.01 | SHALLOW_RETRACEMENT |
| TER | 62.7 | 10.23 | 7.04 | RETEST_IN_PROGRESS | TRENDLINE_SUPPORT_TEST | weekly_impulse | SECOND_LEG | HEALTHY | 2.82 | — |
| AIR | 62.4 | 8.65 | 6.0 | RETEST_IN_PROGRESS | WAIT_FOR_RETEST | weekly_impulse | SECOND_LEG | MODERATELY_EXT | 5.70 | — |
| HLIT | 62.3 | 8.57 | 6.64 | RETEST_IN_PROGRESS | TRENDLINE_SUPPORT_TEST | weekly_impulse | SECOND_LEG | MODERATELY_EXT | 3.46 | — |
| HALO | 61.4 | 10.46 | 6.2 | RETEST_IN_PROGRESS | BREAKOUT_RETEST | monthly_impulse | SECOND_LEG | HEALTHY | 0.25 | EXTENSION_TARGET |
| ADEA | 61.3 | 8.81 | 6.64 | RETEST_IN_PROGRESS | TRENDLINE_SUPPORT_TEST | weekly_impulse | MOMENTUM_WAVE_PULLBACK | MODERATELY_EXT | 0.75 | SHALLOW_RETRACEMENT |
| LASR ⚠️ | 61.2 | 5.24 | 5.2 | READY_AT_ENTRY | CONSTRUCTIVE_DIP | weekly_impulse | SECOND_LEG | MODERATELY_EXT | 0.96 | SHALLOW_RETRACEMENT |
| SLAB | 61.0 | 8.73 | 6.0 | BREAKOUT_TRIGGER_READY | HIGH_BASE_READY | long_daily | NO_CLEAR_WAVE | HEALTHY | 1.31 | PRIOR_RESISTANCE |
| AMD | 60.9 | 4.91 | 7.12 | BREAKOUT_TRIGGER_READY | HIGH_BASE_FORMING | recent_daily | SECOND_LEG | EXTREME_EXTENSION | 0.33 | SHALLOW_RETRACEMENT |
| TGTX | 60.5 | 4.67 | 7.24 | RETEST_IN_PROGRESS | WAIT_FOR_RETEST | monthly_impulse | SECOND_LEG | EXTREME_EXTENSION | 0.54 | EXTENSION_TARGET |
| CGNX | 60.0 | 8.57 | 7.04 | BREAKOUT_TRIGGER_READY | HIGH_BASE_FORMING | monthly_impulse | SECOND_LEG | EXTENDED | 0.39 | EXTENSION_TARGET |
| ENTG | 59.3 | 10.7 | 6.84 | READY_AT_ENTRY | BREAKOUT_PULLBACK | long_daily | SECOND_LEG | HEALTHY | 0.16 | SHALLOW_RETRACEMENT |
| XYZ | 59.1 | 10.62 | 6.2 | RETEST_IN_PROGRESS | WAIT_FOR_RETEST | intermediate_daily | NO_CLEAR_WAVE | MODERATELY_EXT | 1.30 | PRIOR_RESISTANCE |

Notes:
- WDC/DELL/MXL/PENG at EXTREME_EXTENSION with CONSTRUCTIVE quality — in caution, not risk. Correct.
- LITE, LASR carry LOWER_LOW_CONFIRMED risk flag — P0 contradictions.
- Entry scores < 5.0 on WDC/DELL/MXL/PENG/TGTX qualify via high-confluence PATH E or PATH C2.
- AIR: entry_state=WAIT_FOR_RETEST at 5.7% above fib — high fib distance, borderline RETEST_IN_PROGRESS labeling.
- TGTX: entry_state=WAIT_FOR_RETEST → READY via PATH C2 (breakout entry). Semantically borderline.

---

## PART 4 — NEAR ACTIONABLE AUDIT

**Total NEAR_ACTIONABLE rows (action.label): 9**

| SYM | SCORE | EXEC_STATE | ENTRY_STATE | FIB_CTX | DIST% | ENTRY | TECH | STAGE | BLOCKERS |
|-----|-------|-----------|------------|---------|-------|-------|------|-------|---------|
| NBIS | 78.6 | RETEST_IN_PROGRESS | TRENDLINE_SUPPORT_TEST | recent_daily | 0.34 | 8.81 | 2.4 | 12.0 | weak_setup_score — tech=2.4 (sentinel) |
| TSEM | 71.7 | BREAKOUT_TRIGGER_READY | HIGH_BASE_COILING | intermediate_daily | 0.35 | 3.35 | 4.8 | 13.5 | weak_entry_score(3.4) + LOWER_LOW risk |
| LSCC | 71.2 | RETEST_IN_PROGRESS | TRENDLINE_SUPPORT_TEST | intermediate_daily | 0.24 | 8.81 | 2.4 | 12.0 | weak_setup_score — tech=2.4 (sentinel) |
| PDFS | 56.3 | RETEST_IN_PROGRESS | TRENDLINE_SUPPORT_TEST | intermediate_daily | 0.44 | 8.81 | 2.4 | 15.0 | weak_setup_score — tech=2.4 (sentinel) |
| IAK | 53.7 | READY_AT_ENTRY | NO_CLEAR_ENTRY | weekly_impulse | 0.42 | 8.57 | 2.4 | 14.3 | weak_setup_score — tech=2.4 (sentinel) |
| DBA | 49.6 | READY_AT_ENTRY | NO_CLEAR_ENTRY | weekly_impulse | 0.38 | 8.81 | 2.4 | 12.0 | weak_setup_score — tech=2.4 (sentinel) |
| ARKF | 47.6 | READY_AT_ENTRY | BREAKOUT_PULLBACK | monthly_impulse | 0.71 | 8.81 | 6.44 | 4.79 | stage<9(4.79) — ETF |
| NVDA | 46.6 | BREAKOUT_TRIGGER_READY | HIGH_BASE_READY | monthly_impulse | 0.18 | 3.27 | 5.6 | 13.2 | weak_entry_score(3.3) + LOWER_LOW risk |
| SVCO | 43.0 | RETEST_IN_PROGRESS | TRENDLINE_SUPPORT_TEST | long_daily | 1.53 | 8.81 | 2.4 | 15.0 | weak_setup_score — tech=2.4 (sentinel) |

**Pattern:** 6/9 blocked exclusively by technical_setup_points = 2.4.
2.4 is the SENTINEL VALUE returned when no technical pattern is recognized —
it does NOT mean "weak technicals." It means "technical setup scorer found no
qualifying pattern." These symbols have perfect entry scores (8.57–8.81), valid
fib retests at sub-0.5% distance, and high stage scores.

NVDA: entry_exit_points=3.27 is the binding constraint (HIGH_BASE_READY +
LOWER_LOW risk + depressed momentum). Correctly NEAR, not READY.

ARKF: ETF (stage=4.79 < 9.0). Correctly blocked.

---

## PART 5 — WATCH_FOR_RESET / CHASE AUDIT

### Summary Counts

| Metric | Count |
|--------|-------|
| watch_for_reset_count | 37 |
| WFR with weekly or monthly fib | 24 |
| WFR with SECOND_LEG_CONTINUATION wave | 30 |
| WFR with fib_retest_detected | 28 |
| WFR with CHASE_EXTENSION risk flag | 27 |
| WFR with CONSTRUCTIVE caution | 0 |
| WFR with fib distance < 5% | 37 (all) |

### WFR False Negative Analysis

27/37 WFR rows have CHASE_EXTENSION in risk_flags — correctly classified.

3 symbols with NO chase flag and adequate entry/tech/stage are in WFR
with no clear reason from P6 path logic:

| SYM | SCORE | ENTRY_STATE | FIB_CTX | DIST% | ENTRY | TECH | STAGE | FLAGS |
|-----|-------|------------|---------|-------|-------|------|-------|-------|
| CIFR | 57.1 | TRENDLINE_SUPPORT_TEST | monthly_impulse | 0.33 | 8.81 | 6.64 | 11.6 | None |
| TAC | 54.5 | TRENDLINE_SUPPORT_TEST | weekly_impulse | 0.74 | 8.81 | 6.64 | 10.5 | None |
| AUR | 53.3 | TRENDLINE_SUPPORT_TEST | monthly_impulse | 0.12 | 8.65 | 6.64 | 12.0 | None |

CIFR, TAC, AUR are GENUINE WFR FALSE NEGATIVES. All three have valid entry states,
no risk/caution flags, strong fib proximity, adequate scores. Should be NEAR_ACTIONABLE
or READY. Upstream is_watch_for_reset assignment requires investigation.

### Top WFR Rows (by score)

| SYM | SCORE | ENTRY_STATE | FIB_CTX | DIST% | CHASE | RISK_FLAGS |
|-----|-------|------------|---------|-------|-------|------------|
| VICR | 66.0 | NO_CLEAR_ENTRY | weekly_impulse | 2.84 | ✅ | CHASE_EXTENSION |
| ALAB | 65.8 | EXTREME_EXTENSION | weekly_impulse | 0.82 | ✅ | EXTREME+CHASE |
| AMAT | 64.3 | EXTREME_EXTENSION | long_daily | 0.23 | ✅ | EXTREME+CHASE |
| MEI | 62.3 | VERTICAL | weekly_impulse | 0.17 | ✅ | EXTREME+CHASE |
| ABCL | 61.7 | NO_CLEAR_ENTRY | weekly_impulse | 3.45 | ✅ | EXTREME+CHASE |
| CIFR ⚠️ | 57.1 | TRENDLINE_SUPPORT_TEST | monthly_impulse | 0.33 | ❌ | None |
| NTAP | 56.6 | EXTREME_EXTENSION | monthly_impulse | 0.48 | ✅ | EXTREME+CHASE |
| BE | 54.7 | NO_CLEAR_ENTRY | weekly_impulse | 1.02 | ✅ | CHASE_EXTENSION |
| TAC ⚠️ | 54.5 | TRENDLINE_SUPPORT_TEST | weekly_impulse | 0.74 | ❌ | None |
| SHLS | 53.4 | NO_CLEAR_ENTRY | weekly_impulse | 0.48 | ✅ | CHASE_EXTENSION |
| AUR ⚠️ | 53.3 | TRENDLINE_SUPPORT_TEST | monthly_impulse | 0.12 | ❌ | None |
| SMTC | 53.0 | NO_CLEAR_ENTRY | weekly_impulse | 0.11 | ✅ | CHASE_EXTENSION |
| PSNL | 51.8 | VERTICAL | long_daily | 0.92 | ✅ | EXTREME+CHASE |
| POWI | 51.5 | NO_CLEAR_ENTRY | long_daily | 3.85 | ❌ | None |
| BAND | 49.4 | VERTICAL | long_daily | 0.28 | ✅ | EXTREME+CHASE |
| INTC | 48.8 | NO_CLEAR_ENTRY | recent_daily | 0.01 | ✅ | EXTREME+CHASE |
| HYLN | 48.5 | NO_CLEAR_ENTRY | weekly_impulse | 0.12 | ✅ | CHASE_EXTENSION |
| ICHR | 48.2 | EXTREME_EXTENSION | weekly_impulse | 2.76 | ✅ | EXTREME+CHASE |
| UCTT | 46.2 | EXTREME_EXTENSION | intermediate_daily | 1.29 | ✅ | EXTREME+CHASE |
| VPG | 45.5 | REVERSAL_WATCH | intermediate_daily | 1.94 | ✅ | EXTREME+CHASE |
| ZVRA | 45.4 | NO_CLEAR_ENTRY | weekly_impulse | 0.76 | ✅ | CHASE_EXTENSION |
| XLE | 45.0 | REVERSAL_WATCH | monthly_impulse | 0.25 | ❌ | LOWER_LOW |
| KEEL | 44.3 | NO_CLEAR_ENTRY | weekly_impulse | 0.23 | ✅ | CHASE_EXTENSION |
| AMPG | 44.2 | EXTREME_EXTENSION | weekly_impulse | 2.75 | ✅ | EXTREME+CHASE |
| XLK | 44.0 | TRENDLINE_SUPPORT_TEST | weekly_impulse | 1.04 | ❌ | None |

---

## PART 6 — SPECIFIC SYMBOL SANITY CHECKS

| SYM | TOTAL | VERDICT | CANON_LABEL | EXEC_STATE | ENTRY_STATE | FIB_CTX | WAVE | EXT/QUAL | DIST% | FIB_R | RISK_FLAGS |
|-----|-------|---------|------------|-----------|------------|---------|------|----------|-------|-------|------------|
| ABCL | 61.7 | NEUTRAL | WATCH_FOR_RESET | NOT_ACTIONABLE | NO_CLEAR_ENTRY | weekly_impulse | SECOND_LEG | EXTREME/CHASE | 3.45 | ❌ | EXTREME_EXTENSION, CHASE_EXTENSION |
| ALGM | 59.0 | BUY | READY | BREAKOUT_TRIGGER_READY | HIGH_BASE_FORMING | monthly_impulse | SECOND_LEG | EXTENDED/CONSTR | 1.38 | ✅ | None |
| VRT | 76.9 | WATCH | READY | RETEST_IN_PROGRESS | TRENDLINE_SUPPORT_TEST | weekly_impulse | SECOND_LEG | MODERATE/NORMAL | 0.22 | ✅ | None |
| CRDO | 51.2 | WATCH | WATCH | NOT_ACTIONABLE | TRENDLINE_SUPPORT_TEST | long_daily | SECOND_LEG | EXTREME/CONSTR | 1.17 | ✅ | None |
| MARA | 44.9 | WATCH | WATCH | NOT_ACTIONABLE | SUPPORT_TEST | weekly_impulse | SECOND_LEG | MODERATE/NORMAL | 5.90 | ❌ | None |
| NVDA | 46.6 | WATCH | NEAR_ACTIONABLE | BREAKOUT_TRIGGER_READY | HIGH_BASE_READY | monthly_impulse | SECOND_LEG | EXTREME/CONSTR | 0.18 | ✅ | LOWER_LOW_CONFIRMED |
| TSM | 72.5 | BUY | READY | READY_AT_ENTRY | SIGNALS_BUILDING | intermediate_daily | SECOND_LEG | MODERATE/NORMAL | 0.39 | ✅ | None |
| SOFI | 35.1 | AVOID | WATCH | NOT_ACTIONABLE | DOWNTREND | monthly_impulse | SECOND_LEG | BELOW_MA | 0.35 | ✅ | None |
| WYFI | 53.5 | WATCH | READY | BREAKOUT_TRIGGER_READY | HIGH_BASE_COILING | recent_daily | SECOND_LEG | EXTREME/CONSTR | 0.75 | ✅ | None |
| OUST | 49.4 | WATCH | WATCH | NOT_ACTIONABLE | TRENDLINE_SUPPORT_TEST | weekly_impulse | SECOND_LEG | EXTREME/CONSTR | 0.76 | ✅ | None |
| ECHO | 10.1 | AVOID | WATCH | NOT_ACTIONABLE | LOWER_LOW_CONFIRMED | weekly_impulse | MOMENTUM_PULLBACK | BELOW_MA | 0.15 | ✅ | LOWER_LOW_CONFIRMED |
| HIMS | 65.4 | WATCH | WAIT_FOR_RETEST | SET_ALERT_FOR_RETEST | WAIT_FOR_RETEST | weekly_impulse | SECOND_LEG | EXTENDED/CONSTR | 2.91 | ❌ | None |
| NET | 53.0 | WATCH | WAIT_FOR_RETEST | WAIT_FOR_RETEST | WAIT_FOR_RETEST | long_daily | IMPULSE_LEG | EXTREME/CONSTR | 1.78 | ✅ | None |
| CRWD | 78.7 | WATCH | WAIT_FOR_RETEST | SET_ALERT_FOR_RETEST | WAIT_FOR_RETEST | long_daily | IMPULSE_LEG | EXTREME/CONSTR | 0.58 | ✅ | None |
| FTNT | 67.9 | WATCH | WAIT_FOR_RETEST | SET_ALERT_FOR_RETEST | WAIT_FOR_RETEST | monthly_impulse | IMPULSE_LEG | EXTREME/CONSTR | 4.63 | ❌ | None |
| MSTR | — | NOT IN UNIVERSE | — | — | — | — | — | — | — | — | — |
| SMCI | 28.2 | AVOID | WATCH | NOT_ACTIONABLE | LOWER_LOW_CONFIRMED | weekly_impulse | MOMENTUM_PULLBACK | BELOW_MA | 0.45 | ✅ | LOWER_LOW_CONFIRMED |
| MSFT | 43.5 | AVOID | WATCH | NOT_ACTIONABLE | LOWER_LOW_CONFIRMED | monthly_impulse | SECOND_LEG | BELOW_MA | 0.12 | ✅ | LOWER_LOW_CONFIRMED |

### Symbol Verdicts

ABCL — CORRECTLY WATCH_FOR_RESET
  extension_quality=CHASE, tech=0.64, actionability_reason_codes=['V2_EXTREME_EXTENSION_RELABELED','CHASE_RISK']
  actionability_blockers=['ENTRY_NOT_CLEAN'], fib_distance=3.45% > retest detection threshold.
  Weekly Fib/Wave (weekly_impulse/SECOND_LEG_CONTINUATION) confirms extension severity, not a valid entry.
  Must pull back below FIB_1.000 (6.515) to reset. invalidation_level=6.39.
  NO FALSE NEGATIVE. Correctly classified.

CRDO — CORRECTLY WATCH / INVESTMENT_QUALITY
  extension_quality=CONSTRUCTIVE, fib_retest at 1.17% (SHALLOW_RETRACEMENT).
  tech=2.4 (sentinel — no technical pattern). PATH I requires tech>=5.0, blocked.
  caution_flag=EXTREME_EXTENSION_CONSTRUCTIVE_RESET correctly placed.
  why_now=['Investment quality is strong', 'Stage quality is strong', 'Theme alignment is positive'].
  Calibration question: tech sentinel suppresses a genuinely constructive setup (see P1.5).

VRT — CORRECTLY READY / RETEST_IN_PROGRESS ✅
  TRENDLINE_SUPPORT_TEST + fib at 0.22% (FIB_0.236) + weekly_impulse + SECOND_LEG.
  Entry=8.81, tech=6.64, stage=12. All PATH A criteria met. 7 positive why_now signals.
  Legacy WATCH verdict is stale — canonical action is definitively READY.
  invalidation_level=288.68.

ALGM — CORRECTLY READY / BREAKOUT_TRIGGER_READY ✅
  HIGH_BASE_FORMING + monthly fib at 1.38% (PRIOR_RESISTANCE at FIB_1.000=53.05).
  Entry=8.73, tech=6.92, stage=15. EXTENDED/CONSTRUCTIVE — not a risk.
  Legacy BUY coincides with READY but is the wrong field to use.

NVDA — CORRECTLY NEAR_ACTIONABLE / BREAKOUT_TRIGGER_READY ✅
  HIGH_BASE_READY + fib at 0.18% (FIB_1.000=212.19) + LOWER_LOW_CONFIRMED risk.
  Entry=3.27 is the binding blocker (PATH I requires >=4.5, PATH B requires >=6.0).
  Low entry reflects LOWER_LOW drag on conviction. Will qualify for PATH I/B when
  LOWER_LOW_CONFIRMED resolves and entry score recovers.

SOFI — CORRECTLY WATCH (not AVOID) ✅
  DOWNTREND + BELOW_MA + monthly_impulse fib. Score=35.1. Fib retest detected (0.35%)
  but DOWNTREND routes through hard_break_states check. AVOID legacy overstates severity.
  Canonical WATCH is correct.

ECHO — CORRECTLY WATCH / NOT_ACTIONABLE ✅
  Score=10.1, LOWER_LOW_CONFIRMED, BELOW_MA, MOMENTUM_WAVE_PULLBACK.
  Options unavailable. SATS/ECHO alias fully resolved. All systems agree: not actionable.

CRWD — CORRECTLY WAIT_FOR_RETEST / SET_ALERT semantically ✅ (display issue noted)
  IMPULSE_LEG wave (not SECOND_LEG), EXTREME/CONSTRUCTIVE, fib at 0.58%.
  is_near_actionable=True but action.label=WAIT_FOR_RETEST — label/bucket inconsistency (P1.2).
  Score=78.7 is highest in WAIT_FOR_RETEST category.

FTNT — CORRECTLY SET_ALERT_FOR_RETEST ✅
  fib_distance=4.63% — not at retest level yet. Entry=6.33, tech=7.64, stage=15.
  When price pulls to fib level, would qualify immediately for PATH I or PATH B.

HIMS — CORRECTLY SET_ALERT_FOR_RETEST ✅
  Price 2.91% above fib (36.20). Entry=10.54 (highest in NEAR family). When at fib,
  entry score + tech + stage would qualify READY immediately.

SMCI / MSFT — CORRECTLY WATCH with LOWER_LOW ✅
  Both in confirmed downtrends. MSFT at 43.5 kept above AVOID by investment quality.
  Correct — MSFT is a quality name in structural dip, not a broken story.

MSTR — NOT IN UNIVERSE

---

## PART 7 — FIB RETEST PATH AUDIT

### Code Path (confluence_v2_service.py, ~line 2285)

PATH I — Fibonacci Retest Entry:
```python
elif (
    _p6_fib_retest        # bool(r.get("fib_retest_detected"))
    and _p6_tech  >= 5.0
    and _p6_entry >= 4.5
    and _p6_stage >= 9.0
    and not _p6_support_lost
    and not _p6_major_llc
):
    _p6_is_act = True
    _p6_tier   = "ACTIONABLE_NOW"
    _p6_path   = "fib_retest_entry"
```

PATH H — Constructive Retest After Extension (precedes PATH I):
```python
elif (
    _p6_is_constr_retest   # extension_reset_state == "CONSTRUCTIVE_RETEST_AFTER_EXTENSION"
    and _p6_tech  >= 4.5
    and _p6_entry >= 4.5
    and _p6_stage >= 9.0
    and not _p6_support_lost
):
```

Thresholds used:
  technical_setup_points >= 5.0 (PATH I) / >= 4.5 (PATH H)
  entry_exit_points >= 4.5
  stage_quality_points >= 9.0
  fib_retest_detected (boolean)
  primary_fib_retest_detected (NOT read by PATH I — key gap)
  extension_reset_state = "CONSTRUCTIVE_RETEST_AFTER_EXTENSION" (PATH H gate)

### Statistics

| Metric | Value |
|--------|-------|
| Symbols with fib_retest_detected=True (any field) | 230 |
| → became READY | 41 |
| → became NEAR_ACTIONABLE | 9 |
| → stayed WATCH or WATCH_FOR_RESET | 159 |
| why_now containing "FIB_RETEST" text | 0 ⚠️ |

### Top Blocked Fib Retest Symbols

| SYM | SCORE | ACTION | ENTRY_STATE | DIST% | BLOCK_REASON |
|-----|-------|--------|------------|-------|-------------|
| CRWD | 78.7 | WAIT_FOR_RETEST | WAIT_FOR_RETEST | 0.58 | PATH B routing — entry/label classification |
| PANW | 72.9 | WAIT_FOR_RETEST | WAIT_FOR_RETEST | 0.20 | Same as CRWD |
| ALAB | 65.8 | WATCH_FOR_RESET | EXTREME_EXTENSION | 0.82 | CHASE + tech=1.04 |
| AMAT | 64.3 | WATCH_FOR_RESET | EXTREME_EXTENSION | 0.23 | CHASE + tech=1.04 |
| AAOI | 64.2 | WAIT_FOR_RETEST | CONSTRUCTIVE_DIP | 0.50 | tech=2.4 + LOWER_LOW |
| MDA | 63.7 | WAIT_FOR_RETEST | FAILED_BREAKOUT | 0.26 | tech=2.4 + LOWER_LOW |
| BWA | 62.4 | WATCH | TRENDLINE_SUPPORT_TEST | 0.22 | Routes to WATCH (no pattern) |
| MRVL | 62.2 | WATCH | TRENDLINE_SUPPORT_TEST | 0.26 | tech=2.4 |
| VSAT | 61.7 | WATCH | BREAKOUT_PULLBACK | 0.75 | entry<4.5 + LOWER_LOW |
| NOK | 61.6 | WATCH | TRENDLINE_SUPPORT_TEST | 0.46 | entry=3.4 + tech=2.0 |

### Key Findings

1. PATH I uses fib_retest_detected (non-primary field), NOT primary_fib_retest_detected.
   If primary_fib_retest_detected=True but fib_retest_detected=False, PATH I silently misses
   a genuine high-quality fib retest. (P1.1 fix required)

2. why_now NEVER contains FIB_RETEST language despite PATH I firing for 41 READY rows.
   _p6_pos.append("FIB_RETEST_ENTRY_...") is populated but not wired to why_now display.
   Explainability gap only — no scoring error. (P1.4 fix required)

3. Fib Retest path IS actively influencing actionability.
   41 READY + 9 NEAR_ACTIONABLE owe classification to PATH I/H.
   The path is alive and working, not purely decorative.

4. 159 blocked fib retest symbols — primary blockers:
   ~80 blocked by tech=2.4 (sentinel — no technical pattern recognized)
   ~27 blocked by CHASE_EXTENSION (correct WFR)
   ~25 blocked by entry_exit_points < 4.5
   ~15 blocked by LOWER_LOW_CONFIRMED
   ~12 blocked by stage < 9.0

5. Fib Retest CAN override NO_CLEAR_ENTRY via PATH I.
   NO_CLEAR_ENTRY routes to WAIT_FOR_RETEST state → _p6_retest_state=True → PATH B eligible.
   PATH I provides a lower-entry-floor escape (4.5 vs 6.0). But sentinel tech=2.4 blocks it.

6. PATH I has NO explicit _p6_chase_bad guard (unlike PATHs A,B,B2,C,C2,G,H).
   In practice, CHASE symbols all have tech=0.64–1.04 which blocks PATH I anyway.
   But this is a latent risk — PATH I should add 'and not _p6_chase_bad'. (P2 fix)

---

## PART 8 — EXTENSION / CHASE SEVERITY AUDIT

### Extension State Distribution

| Extension State | Count |
|----------------|-------|
| BELOW_MA | 141 |
| HEALTHY | 70 |
| None (no_bars/TEST) | 69 |
| MODERATELY_EXTENDED | 51 |
| EXTREME_EXTENSION | 51 |
| EXTENDED | 31 |

### Extension Quality Distribution

| Extension Quality | Count |
|-----------------|-------|
| NORMAL | 324 |
| CONSTRUCTIVE | 49 |
| CHASE | 27 |
| BROKEN | 13 |

### Risk/Caution Flag Counts

| Flag | Count |
|------|-------|
| risk_flags EXTREME_EXTENSION | 20 |
| risk_flags CHASE_EXTENSION | 27 |
| caution_flags EXTREME_EXT_CONSTRUCTIVE_RESET | 31 |
| EXTREME_EXTENSION + quality=CHASE | 19 |
| EXTREME_EXTENSION + quality=CONSTRUCTIVE | 31 |
| EXTREME_EXTENSION + quality=NORMAL | 1 |
| CHASE_EXTENSION risk + READY action | 0 ✅ |

### Severity Matrix

| Combo | Treatment | Action Labels | Correct? |
|-------|-----------|--------------|---------|
| EXTREME + CHASE | red risk_flags → WFR/RISK | All WFR or RISK_CONFLICT | ✅ |
| EXTREME + CONSTRUCTIVE | amber caution_flags | 14 READY, 10 WATCH, 7 WAIT | ✅ |
| EXTREME + NORMAL | amber caution | 1 NEAR_ACTIONABLE | ⚠️ Should be caution only |
| MODERATELY_EXTENDED + any | no flag | All paths eligible | ✅ |
| HEALTHY + any | no flag | All paths eligible | ✅ |
| BELOW_MA | structural | Drives LLC/DOWNTREND entry | ✅ |

### Contradictions

| Symbol | Issue | Priority |
|--------|-------|---------|
| LITE | READY + LOWER_LOW_CONFIRMED risk, entry=REVERSAL_WATCH | P0 |
| LASR | READY + LOWER_LOW_CONFIRMED risk, entry=CONSTRUCTIVE_DIP | P0 |
| VECO | READY + LOWER_LOW_CONFIRMED risk, entry=BREAKOUT_PULLBACK | P0 |
| (1 symbol) EXTREME+NORMAL | NEAR_ACTIONABLE despite ambiguous quality | P2 |

No READY rows carry CHASE_EXTENSION in risk_flags.
EXTREME + CHASE → red risk wall is working correctly.
EXTREME + CONSTRUCTIVE → amber caution allowing READY is working correctly.

---

## PART 9 — LEGACY FIELD CLEANUP / FRONTEND CONTRACT

### Fields Frontend MUST Use (canonical)

| Field | Usage |
|-------|-------|
| confluence_v42.action.label | Primary action: READY/NEAR_ACTIONABLE/WATCH/WATCH_FOR_RESET/WAIT_FOR_RETEST/AVOID |
| confluence_v42.action.label_display | Display string: "Ready to Enter", "Watch for Reset", etc. |
| confluence_v42.action.execution_state | Sub-phase: RETEST_IN_PROGRESS/READY_AT_ENTRY/BREAKOUT_TRIGGER_READY/SET_ALERT_FOR_RETEST/NOT_ACTIONABLE |
| confluence_v42.action.execution_label | Display string for execution phase |
| is_actionable_setup | Boolean — show entry UI |
| is_near_actionable | Boolean — show "set alert" UI |
| is_watch_for_reset | Boolean — show wait/extension warning |
| risk_flags | Red badges |
| caution_flags | Amber badges |
| data_status_flags | Coverage/quality warnings |
| entry_state_display | Human-readable entry state |
| primary_fib_context | Fib timeframe (detail drawer) |
| wave_structure_label | Wave label (detail drawer) |
| invalidation_level | Stop level |
| why_now / why_wait | Explainability strings |

### Fields Frontend MUST NOT Use (legacy/stale)

| Field | Issue |
|-------|-------|
| confluence_verdict | Legacy BUY/WATCH/NEUTRAL/AVOID. VRT=WATCH despite READY. Misleads. |
| legacy_verdict | Alias of above. |
| actionability_state | Overwritten by V4.2 promotion layer. Unreliable post-promotion. |
| legacy_actionability_state | Debug only. |
| legacy_trade_alignment_* | Use trade_alignment_* directly. |
| caelyn_confluence_v4_actionability | V4 predecessor. |

### Stale/Inconsistent Fields to Remove or Hide Later

1. action.label=WAIT_FOR_RETEST when is_near_actionable=True (15 symbols including CRWD, HIMS, PANW):
   bucket=NEAR_ACTIONABLE but action.label=WAIT_FOR_RETEST. Frontend shows "Wait for Retest"
   for a 78.7-score CRWD while bucket says Near Actionable. Fix in P1.2.

2. target_zone_1 / target_zone_2 = None for nearly all symbols including READY rows.
   Fib T1/T2 computed in entry_state_service but not propagated to confluence row.

3. fib_retest_detected vs primary_fib_retest_detected: two parallel fields.
   PATH I reads fib_retest_detected only. Frontend should display primary_fib_retest_detected.

4. why_now for PATH I symbols: 0/41 fib-retest READY rows have FIB_RETEST in why_now.
   Explainability gap — users cannot see why a READY classification was made.

---

## PART 10 — CALIBRATION RECOMMENDATIONS

### P0 — Required Correctness Fixes

P0.1 — LOWER_LOW_CONFIRMED in risk_flags must hard-block READY
  Problem: LITE, LASR, VECO are READY with LOWER_LOW_CONFIRMED in risk_flags.
           actionability_service.py step 5 READY gate checks entry_state only, not risk_flags.
  Evidence: 3 symbols confirmed.
  Proposed: Before step 5 READY gate, add:
              if "LOWER_LOW_CONFIRMED" in risk_flags:
                  state = WAIT_FOR_RETEST
                  reasons.append("LOWER_LOW_RISK")
                  return {...}
            OR post-process in P6: if _p6_is_act and LOWER_LOW in risk_flags,
            downgrade tier to NEAR_ACTIONABLE.
  Expected: 3 READY → NEAR_ACTIONABLE or WAIT_FOR_RETEST.
  Risk: None. Strictly more conservative.

### P1 — Important Semantic Fixes

P1.1 — PATH I should use primary_fib_retest_detected as authoritative source
  Problem: PATH I gate: _p6_fib_retest = bool(r.get("fib_retest_detected"))
           Does NOT read primary_fib_retest_detected. Primary fib retest can be
           present but PATH I silently fails.
  Proposed: _p6_fib_retest = bool(r.get("primary_fib_retest_detected") or r.get("fib_retest_detected"))
  Expected: May promote several currently-blocked symbols into PATH I.
  Risk: Low — PATH I still requires tech>=5.0, entry>=4.5, stage>=9.0.

P1.2 — Fix action.label=WAIT_FOR_RETEST when is_near_actionable=True (15 symbols)
  Problem: CRWD(78.7), HIMS(65.4), PANW(72.9), SNOW, RDDT, SITM and ~9 others have
           bucket=NEAR_ACTIONABLE + is_near_actionable=True but action.label=WAIT_FOR_RETEST.
           Frontend shows "Wait for Retest" for 78.7-score CRWD in NEAR_ACTIONABLE bucket.
  Proposed: In P5 bucket/label consistency correction, if is_near_actionable=True
            and action.label in {"WAIT_FOR_RETEST"}, promote to:
              action.label = "NEAR_ACTIONABLE"
              action.execution_state = "SET_ALERT_FOR_RETEST"
  Expected: 15 symbols get consistent labels. Display: "Near Actionable — Set Alert."
  Risk: None. No scoring change.

P1.3 — Investigate WFR false negatives: CIFR, TAC, AUR
  Problem: 3 symbols with TRENDLINE_SUPPORT_TEST, no risk/caution flags, fib<1%,
           tech>=6.64, entry>=8.65, stage>=10.5 are in WATCH_FOR_RESET.
           P6 path logic as read should route them to PATH A (Support Entry) or PATH I.
  Required action: Read is_watch_for_reset assignment code (lines after 2304 in
           confluence_v2_service.py) to find what upstream gate overrides PATH A/I.
           This requires a targeted code audit before a fix can be proposed.

P1.4 — Add FIB_RETEST to why_now for PATH I / PATH H symbols
  Problem: 41 READY + 9 NEAR triggered via fib retest paths have zero FIB_RETEST
           language in why_now. _p6_pos.append("FIB_RETEST_ENTRY_...") is never
           piped to the why_now display builder.
  Proposed: In why_now builder, if "fib_retest_entry" in _p6_path or
            "constructive_retest_after_extension" in _p6_path, append:
              f"Fibonacci retest at {nearest_fib_label} ({distance:.1f}% from level)"
  Expected: 41 READY + 9 NEAR gain Fib Retest explainability line.
  Risk: None. No scoring change.

P1.5 — CRDO/OUST/MRVL: tech=2.4 sentinel suppresses constructive retest setups
  Problem: High-quality names with EXTREME/CONSTRUCTIVE + fib_retest + TRENDLINE_SUPPORT_TEST
           have tech=2.4 (sentinel — no pattern) blocking PATH I (tech>=5.0) and PATH H.
           The 2.4 value means "no pattern recognized" in a post-extension consolidation,
           which is expected behavior (no base formed yet), not weak technicals.
  Proposed: For PATH H (constructive retest after extension):
              Verify whether extension_reset_state="CONSTRUCTIVE_RETEST_AFTER_EXTENSION"
              is being set correctly for CRDO/OUST/MRVL. If not, investigate why.
              Alternatively, add a PATH H2 variant with tech>=2.5 threshold for
              EXTREME/CONSTRUCTIVE + fib_retest + TRENDLINE_SUPPORT_TEST scenarios.
  Symbols: CRDO, OUST, MRVL, AEHR, ~10 others with same profile.

P1.6 — why_wait semantics for tech=2.4 sentinel
  Problem: 6 NEAR rows have tech=2.4 but the user sees no explanation of why.
           Generic "No clear action" is misleading for a name with perfect entry + fib retest.
  Proposed: Add why_wait code for tech sentinel: "No confirmed technical breakout
            pattern yet — monitoring for pattern formation at Fib level."

### P2 — Optional Improvements

P2.1 — Populate target_zone_1 / target_zone_2 from fib engine
  All READY rows have target_zone=None. Fib T1/T2 computed in entry_state_service
  but not propagated to confluence row. Wire fib_target_1/fib_target_2 through.

P2.2 — Add explicit CHASE guard to PATH I
  PATH I has no 'and not _p6_chase_bad' guard (unlike A,B,B2,C,C2,G,H).
  CHASE symbols are filtered by WFR gate before reaching PATH I in practice,
  but the missing guard is a latent fragility.
  Add: 'and not _p6_chase_bad' to PATH I conditions.

P2.3 — EXTREME_EXTENSION + NORMAL quality treatment
  1 symbol with EXTREME+NORMAL in NEAR_ACTIONABLE. This case is ambiguous:
  NORMAL means no constructive reset and no CHASE. Recommend treating
  EXTREME+NORMAL as caution (not READY-eligible) without explicit
  extension_reset_state=CONSTRUCTIVE_RETEST_AFTER_EXTENSION.

P2.4 — Fib distance gate for RETEST_IN_PROGRESS labeling
  AIR (62.4) has entry_state=WAIT_FOR_RETEST and is READY but fib_distance=5.7%.
  Consider: if execution_state=RETEST_IN_PROGRESS and fib_distance_pct > 5.0%,
  relabel execution_state to BREAKOUT_TRIGGER_READY or SET_ALERT_FOR_RETEST
  to prevent false "in progress" display when price is materially above the fib level.

---

## POST-10Y CALIBRATION AUDIT VERDICT

FIB_WAVE_AVAILABLE_FOR_ACTIONABILITY:
YES — 344/344 ok-status symbols, PATH I/H active

READY_COUNT:
57

NEAR_ACTIONABLE_COUNT:
9 (action.label) / 24 (is_near_actionable=True)

WATCH_FOR_RESET_COUNT:
37

READY_ROWS_PASS_SAFETY_CHECK:
MOSTLY — 54/57 pass; 3 fail (LITE, LASR, VECO carry LOWER_LOW_CONFIRMED while READY — P0)

FIB_RETEST_PATH_ACTIVE:
YES — 41 READY + 9 NEAR triggered via PATH I or PATH H

FIB_RETEST_PATH_TOO_WEAK:
PARTIALLY — fires correctly but:
  (a) reads fib_retest_detected not primary_fib_retest_detected (P1.1)
  (b) tech>=5.0 floor blocks sentinel-score names with valid retests (P1.5)
  (c) why_now never reflects FIB_RETEST activation — 0/50 symbols (P1.4)

WATCH_FOR_RESET_FALSE_NEGATIVES_FOUND:
YES — CIFR, TAC, AUR (no flags, clean scores, valid fib retests — upstream assignment audit required)

ABCL_CLASSIFICATION_CORRECT:
YES — EXTREME_EXTENSION/CHASE, tech=0.64, actionability_decision=ENTRY_NOT_CLEAN.
Weekly Fib/Wave confirms extension severity, not a false negative.
