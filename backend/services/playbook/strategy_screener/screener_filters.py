"""
Strategy Screener — filter and sort logic.

Operates on stored snapshot results only. Zero DB writes. Zero generation calls.
This is a pure view-layer transform: takes a list of candidate dicts and
returns a filtered, sorted, truncated copy.

Market cap buckets
  large_cap  >= $100B
  mid_cap    $20B – $99.99B
  small_cap  $2.5B – $19.99B
  micro_cap  < $2.5B
  unknown    market_cap_usd is None or invalid — NEVER auto-assigned to a standard bucket

Key rule: unknown is NOT included in any of the four standard bucket filters.
If the user selects large_cap, only confirmed large-cap names appear.
If no bucket is selected, all names (including unknowns) are returned.

Layer filter   → exact match on layer_depth (1, 2, or 3)

Sort options
  best_fit    → best_blend_score DESC, bottleneck_criticality_score DESC,
                supply_chain_confidence_score DESC  (default)
  market_cap  → market_cap_usd DESC  (None sorts last)
  layer       → layer_depth ASC
  grade       → grade rank DESC  (A+ > A > B+ > B > C)
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

# ── Constants ─────────────────────────────────────────────────────────────────

MARKET_CAP_BUCKETS: Dict[str, Dict[str, Any]] = {
    "large_cap": {
        "id":    "large_cap",
        "label": "Large Cap ($100B+)",
        "min":   100_000_000_000,
        "max":   None,
    },
    "mid_cap": {
        "id":    "mid_cap",
        "label": "Mid Cap ($20B\u2013$99B)",
        "min":   20_000_000_000,
        "max":   100_000_000_000,
    },
    "small_cap": {
        "id":    "small_cap",
        "label": "Small Cap ($2.5B\u2013$19B)",
        "min":   2_500_000_000,
        "max":   20_000_000_000,
    },
    "micro_cap": {
        "id":    "micro_cap",
        "label": "Micro Cap (<$2.5B)",
        "min":   None,
        "max":   2_500_000_000,
    },
    "unknown": {
        "id":    "unknown",
        "label": "Unknown Market Cap",
        "min":   None,
        "max":   None,
    },
}

VALID_BUCKETS   = set(MARKET_CAP_BUCKETS.keys())
VALID_LAYERS    = {1, 2, 3}
VALID_SORTS     = {"best_fit", "market_cap", "layer", "grade"}

# Standard four buckets — unknown is intentionally excluded from these
_STANDARD_BUCKETS = {"large_cap", "mid_cap", "small_cap", "micro_cap"}

_GRADE_RANK: Dict[str, int] = {
    "A+": 5,
    "A":  4,
    "B+": 3,
    "B":  2,
    "C":  1,
}

LAYER_FILTERS = [
    {"id": 1, "label": "Layer 1 \u2014 Systems Integrator"},
    {"id": 2, "label": "Layer 2 \u2014 Key Component"},
    {"id": 3, "label": "Layer 3 \u2014 Constrained Bottleneck"},
]

SORT_OPTIONS = [
    {"id": "best_fit",    "label": "Best Fit"},
    {"id": "market_cap",  "label": "Market Cap"},
    {"id": "layer",       "label": "Layer"},
    {"id": "grade",       "label": "Grade"},
]

_MIN_VALID_MARKET_CAP = 1_000_000  # $1M minimum to be considered a real value


# ── Market cap helpers ─────────────────────────────────────────────────────────

def classify_market_cap(market_cap_usd: Optional[float]) -> str:
    """
    Return the bucket id for a given market_cap_usd value.

    None or missing → "unknown"  (never silently classified as micro_cap)
    Confirmed value → large_cap | mid_cap | small_cap | micro_cap
    """
    if market_cap_usd is None or market_cap_usd < _MIN_VALID_MARKET_CAP:
        return "unknown"
    if market_cap_usd < 2_500_000_000:
        return "micro_cap"
    if market_cap_usd < 20_000_000_000:
        return "small_cap"
    if market_cap_usd < 100_000_000_000:
        return "mid_cap"
    return "large_cap"


def _matches_bucket(candidate: Dict[str, Any], bucket: str) -> bool:
    """
    Return True if candidate belongs in the requested bucket.

    Key rule: the "unknown" bucket filter explicitly selects unknowns.
    Standard bucket filters (large/mid/small/micro) never include unknowns.
    """
    mc = candidate.get("market_cap_usd")

    if bucket == "unknown":
        return mc is None or mc < _MIN_VALID_MARKET_CAP

    # Standard buckets: unknown market cap is NEVER included
    if mc is None or mc < _MIN_VALID_MARKET_CAP:
        return False

    cfg = MARKET_CAP_BUCKETS[bucket]
    if cfg["min"] is not None and mc < cfg["min"]:
        return False
    if cfg["max"] is not None and mc >= cfg["max"]:
        return False
    return True


# ── Sort key factories ─────────────────────────────────────────────────────────

def _sort_key_best_fit(c: Dict[str, Any]):
    return (
        -(c.get("best_blend_score") or 0.0),
        -(c.get("bottleneck_criticality_score") or 0.0),
        -(c.get("supply_chain_confidence_score") or 0.0),
    )


def _sort_key_market_cap(c: Dict[str, Any]):
    mc = c.get("market_cap_usd")
    # None → sort last (most positive value → sorts after negated large caps)
    return (-(mc if mc is not None else -1),)


def _sort_key_layer(c: Dict[str, Any]):
    return (c.get("layer_depth", 99),)


def _sort_key_grade(c: Dict[str, Any]):
    return (-_GRADE_RANK.get(c.get("grade", "C"), 0),)


_SORT_KEYS = {
    "best_fit":   _sort_key_best_fit,
    "market_cap": _sort_key_market_cap,
    "layer":      _sort_key_layer,
    "grade":      _sort_key_grade,
}


# ── Per-result enrichment ──────────────────────────────────────────────────────

def _add_market_cap_bucket(candidate: Dict[str, Any]) -> Dict[str, Any]:
    """Add market_cap_bucket field to a candidate dict (non-mutating)."""
    bucket = classify_market_cap(candidate.get("market_cap_usd"))
    return {**candidate, "market_cap_bucket": bucket}


# ── Main entry point ───────────────────────────────────────────────────────────

def apply_filters_and_sort(
    candidates: List[Dict[str, Any]],
    market_cap_bucket: Optional[str] = None,
    layer: Optional[int] = None,
    sort_by: str = "best_fit",
    limit: int = 30,
) -> Dict[str, Any]:
    """
    Apply optional filters and sort to a list of stored screener candidate dicts.

    Classification rule: None/missing market_cap_usd → "unknown", NOT micro_cap.
    Standard bucket filters exclude unknowns. "unknown" bucket filter selects unknowns only.
    No filter selected → all candidates returned (including unknowns).

    Returns:
        {
            "results":                List[dict],  # filtered + sorted + truncated
            "filtered_result_count":  int,         # after filtering, before limit
            "available_result_count": int,         # total in snapshot (no filters)
            "unknown_market_cap_count": int,       # how many have unknown market cap
            "active_filters":         dict,        # only the filters that were applied
            "active_sort":            str,
            "limit":                  int,
        }

    Each result dict gets a "market_cap_bucket" field added.

    Raises ValueError for invalid bucket/layer/sort values.
    """
    # Validate params
    if market_cap_bucket is not None and market_cap_bucket not in VALID_BUCKETS:
        raise ValueError(
            f"Invalid market_cap_bucket '{market_cap_bucket}'. "
            f"Must be one of: {sorted(VALID_BUCKETS)}"
        )
    if layer is not None and layer not in VALID_LAYERS:
        raise ValueError(
            f"Invalid layer '{layer}'. Must be one of: {sorted(VALID_LAYERS)}"
        )
    if sort_by not in VALID_SORTS:
        raise ValueError(
            f"Invalid sort_by '{sort_by}'. Must be one of: {sorted(VALID_SORTS)}"
        )

    available = len(candidates)
    unknown_count = sum(
        1 for c in candidates
        if (c.get("market_cap_usd") is None or c.get("market_cap_usd", 0) < _MIN_VALID_MARKET_CAP)
    )
    filtered = list(candidates)

    # ── Filter: market cap bucket ─────────────────────────────────────────────
    if market_cap_bucket is not None:
        filtered = [c for c in filtered if _matches_bucket(c, market_cap_bucket)]

    # ── Filter: layer ─────────────────────────────────────────────────────────
    if layer is not None:
        filtered = [c for c in filtered if c.get("layer_depth") == layer]

    filtered_count = len(filtered)

    # ── Sort ──────────────────────────────────────────────────────────────────
    sort_fn = _SORT_KEYS.get(sort_by, _sort_key_best_fit)
    filtered.sort(key=sort_fn)

    # ── Limit ─────────────────────────────────────────────────────────────────
    page = filtered[:limit]

    # ── Add market_cap_bucket per result ──────────────────────────────────────
    results = [_add_market_cap_bucket(c) for c in page]

    # ── Active filters metadata ───────────────────────────────────────────────
    active_filters: Dict[str, Any] = {}
    if market_cap_bucket is not None:
        active_filters["market_cap_bucket"] = market_cap_bucket
    if layer is not None:
        active_filters["layer"] = layer

    return {
        "results":                  results,
        "filtered_result_count":    filtered_count,
        "available_result_count":   available,
        "unknown_market_cap_count": unknown_count,
        "active_filters":           active_filters,
        "active_sort":              sort_by,
        "limit":                    limit,
    }
