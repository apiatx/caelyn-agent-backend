---
name: V4.2.1 options field + avail gate
description: options_alignment_available is absent from mapper/disk-LKG rows; correct gate uses score presence as definitive signal
---

## The Rule
In `_score_options_alignment_v42()`, the availability gate must be:
```python
opts_avail = np_norm is not None and (
    bool(row.get("options_alignment_available"))
    or _align_score is not None    # score present → definitively available
)
```

**Why:** `options_alignment_available` is only set in the raw retained snapshot rows built by the full `build_confluence_snapshot()` pipeline. Alignment endpoint mapper output, disk LKG rows, and rows assembled from partial sources all have `options_alignment_available=None`. A non-None `options_alignment_score` is the definitive indicator that the options service computed a valid composite — when score is present, signal is available.

**How to apply:** Any time the V4.2.1 options scorer is extended or the availability check is revisited, prefer score-not-None as the primary gate. The explicit `options_alignment_available=True` check should remain as an OR-branch (not AND) to handle future cases where score is missing but flag is set.

## Field mapping (V4.2.1)
- Primary field: `options_alignment_score` — 0-100, already direction-weighted composite from options_alignment.py
- Fallback: `options_current_composite_normalized` → `options_current_score`
- When using alignment score: use directly as `opts_raw` (no 60/40 re-split; weighting already baked in)
- When using fallback: apply `0.60 * np_val + 0.40 * dir_val` if `options_direction_score` present

## V4.2.1 catalyst changes
- Added `_DIRECT_CATALYST_EVENT_TYPES` frozenset; `_event_is_direct_catalyst()` helper
- Detection order: primary_catalyst → catalyst_v2_primary_event → catalyst_events[0-2] → nested catalyst dict
- SCORE_ONLY branch: removed -25 deduction; threshold ≥ 40 (not 65); `cat_score` used directly
- reason codes: DIRECT_CATALYST_FROM_PRIMARY_CATALYST, CATALYST_SCORE_BASED_DIRECT

## Validated results (379-ticker watchlist)
- options available_cached: 0 → 276 (not_scanned: 315 → 39)
- score avg: 31.0 → 38.6 (+7.6 pts)
- scores ≥60: 10 → 41 (+31)
- confidence avg: 75.3 → 84.7
