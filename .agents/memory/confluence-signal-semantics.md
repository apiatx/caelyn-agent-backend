---
name: Confluence signal semantics bug repair
description: Three bug fixes shipped together; key patterns for future work on these services
---

## Bug 1 — Asymmetric RR / Options-entry conflict

**Rule:** options_primary_signal (from options_flow_engine primary_signal) must NEVER
override entry-structure actionability. Actionability already correctly gates
SEVERE_EXTENSION_STATES → TOO_EXTENDED regardless of options. The fix adds
shadow diagnostic fields only (no scoring change).

**How to apply:** When adding new options signals or option UI labels, ensure they
come from options_primary_signal / options_entry_conflict fields, NOT from
actionability_state or entry_state.

**Architecture:** compute_actionability() is a public wrapper around
_compute_actionability_core(); the wrapper injects options_entry_conflict,
options_primary_signal, setup_summary as additive-only shadow fields.

## Bug 2 — SUPPORT_LOST blanket overfiring

**Rule:** ext_risk == "broken" (price < SMA50 AND < SMA200) excludes V2 structural
scope. _classify_broken_support() runs after V2 block when V2 was skipped due to
broken ext_risk. It uses last 40 bars to detect:
- SUPPORT_TEST: marginal break (pct_vs_sma50 >= -5%) + no confirmed lower-low
- LOWER_HIGH_WARNING: lower high (3% buffer) + no lower-low yet
- LOWER_LOW_CONFIRMED: second-half min > 3% below first-half min → structural break

LOWER_LOW_CONFIRMED → _HARD_BREAK_STATES → AVOID
SUPPORT_TEST + LOWER_HIGH_WARNING → _REVERSAL_WATCH_CONTEXT_STATES

## Bug 3 — investment_unavailable_reason

**Rule:** investment_unavailable_reason is in the minimum_evidence_met==False
return path of compute_investment_alignment(). Values:
fundamentals_missing / cache_missing / not_in_investment_universe / insufficient_data / unknown

Field auto-spreads into the Confluence snapshot via **investment_alignment_fields
at line 1445 of confluence_v2_service.py. Compact row in watchlist_router.py
forwards it as investment_alignment.unavailable_reason (None when available).
