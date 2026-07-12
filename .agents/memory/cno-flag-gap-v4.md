---
name: CNO flag gap — options_alignment.py vs V4 engine
description: The V4 engine's confirmed_no_options check reads options_pressure_state; the CNO return path must set it to "confirmed_no_options" not _UNAVAILABLE_BASE's "INSUFFICIENT_HISTORY"
---

## Rule
In `get_options_alignment_for_ticker`, the confirmed-no-options return branch must
explicitly override:
- `options_pressure_state = "confirmed_no_options"`
- `options_snapshot_status = "confirmed_no_options"`

The `_UNAVAILABLE_BASE` dict sets `options_pressure_state="INSUFFICIENT_HISTORY"`.
The V4 engine (`_score_options_alignment`) checks `"confirmed_no" in pressure` or
`"no_options" in pressure` — "insufficient_history" never matches either.
Without the override, every sectors-LKG CNO ticker falls through to `status="not_scanned"`.

**Why:**
Fixed during Phase 1.7 cleanup after seeing CODA/OCC/SLNH/SVCO/TRT/TYGO all
showing `not_scanned` despite having `scan_result="confirmed_no_options"` in
the sectors-LKG. Root cause was the silent `_UNAVAILABLE_BASE` clobber.

**How to apply:**
Any new CNO return path in `options_alignment.py` must explicitly set both
`options_pressure_state` and `options_snapshot_status` to `"confirmed_no_options"`,
not rely on `_UNAVAILABLE_BASE` alone.
