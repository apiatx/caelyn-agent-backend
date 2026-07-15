---
name: POST-10Y Confluence Calibration Audit findings
description: Key calibration issues, field gaps, and false negatives discovered in the post-Fib/Wave V4.2 audit. Use before implementing any calibration changes.
---

## Confirmed Bugs (P0/P1 — fix before next release)

**P0 — LOWER_LOW_CONFIRMED in risk_flags does not hard-block READY gate**
- LITE, LASR, VECO are READY + LOWER_LOW_CONFIRMED in risk_flags
- Root: actionability_service.py step 5 checks entry_state only, not risk_flags
- Fix: before step 5 READY gate, if "LOWER_LOW_CONFIRMED" in risk_flags → WAIT_FOR_RETEST

**P1.1 — PATH I uses fib_retest_detected not primary_fib_retest_detected**
- confluence_v2_service.py line ~2289: `_p6_fib_retest = bool(r.get("fib_retest_detected"))`
- Primary retest (higher authority) can be True while this field is False → PATH I silently misses
- Fix: `bool(r.get("primary_fib_retest_detected") or r.get("fib_retest_detected"))`

**P1.2 — action.label=WAIT_FOR_RETEST when is_near_actionable=True (15 symbols)**
- CRWD (78.7), HIMS (65.4), PANW (72.9) etc: bucket=NEAR_ACTIONABLE but action.label=WAIT_FOR_RETEST
- P5 bucket correction sets is_near_actionable but doesn't update action.label
- Fix: in P5, if is_near_actionable=True and action.label==WAIT_FOR_RETEST → promote to NEAR_ACTIONABLE

**P1.3 — WFR false negatives: CIFR, TAC, AUR**
- All three: TRENDLINE_SUPPORT_TEST, no risk/caution flags, fib<1%, tech>=6.64, entry>=8.65
- Should reach PATH A or PATH I in P6 logic — something upstream sets is_watch_for_reset=True
- Requires reading is_watch_for_reset assignment code (after line 2304 in confluence_v2_service.py)

**P1.4 — why_now never contains FIB_RETEST language (0/41 PATH I READY rows)**
- _p6_pos.append("FIB_RETEST_ENTRY_...") is never piped to why_now display builder
- Fix: in why_now builder, check if "fib_retest_entry" in _p6_path and append Fib level context

**P1.5 — tech=2.4 sentinel blocks PATH I for constructive retest scenarios**
- CRDO, OUST, MRVL: EXTREME/CONSTRUCTIVE + fib_retest + TRENDLINE_SUPPORT_TEST but tech=2.4
- 2.4 is sentinel meaning "no technical pattern" not "weak technicals"
- PATH H (tech>=4.5) may be the right escape; verify extension_reset_state is set correctly

## Audit Numbers (2026-07-15 snapshot, 413 symbols)

- READY: 57 | NEAR_ACTIONABLE: 9 (action.label) / 24 (is_near_actionable) | WFR: 37 | WAIT_FOR_RETEST: 19 | AVOID: 13 | WATCH: 278
- Fib retest detected: 230 symbols → 41 READY, 9 NEAR, 159 blocked (80% by tech=2.4 sentinel)
- EXTREME+CHASE (risk): 27 | EXTREME+CONSTRUCTIVE (caution): 31 | CHASE+READY: 0 ✅
- 6 NEAR rows blocked solely by tech=2.4 sentinel (NBIS, LSCC, PDFS, IAK, DBA, SVCO)

## Fields

- Canonical action: confluence_v42.action.{label, execution_state, execution_label}
- Legacy (do not use in frontend): confluence_verdict, actionability_state (pre-V4.2)
- target_zone_1/target_zone_2 = None for almost all symbols — fib T1/T2 not propagated
- PATH I has no _p6_chase_bad guard (latent risk — all CHASE symbols blocked by tech floor in practice)

## Full audit report

backend/data/post_10y_confluence_calibration_audit.md (681 lines)
