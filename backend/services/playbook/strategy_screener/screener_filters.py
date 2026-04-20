"""
Strategy Screener — filter and sort logic.

Operates on stored snapshot results only. Zero DB writes. Zero generation calls.
This is a pure view-layer transform: takes a list of candidate dicts and
returns a filtered, sorted, truncated copy.

Market cap buckets
  large_cap  >= $100B
  mid_cap    $20B – $99.99B
  small_cap  $2.5B – $19.99B
  micro_cap  < $2.5B  (also catches None/unknown)

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
}

VALID_BUCKETS   = set(MARKET_CAP_BUCKETS.keys())
VALID_LAYERS    = {1, 2, 3}
VALID_SORTS     = {"best_fit", "market_cap", "layer", "grade"}

_GRADE_RANK: Dict[str, int] = {
    "A+": 5,
    "A":  4,
    "B+": 3,
    "B":  2,
    "C":  1,
}

LAYER_FILTERS = [
    {"id": 1, "label": "Layer 1 — Systems Integrator"},
    {"id": 2, "label": "Layer 2 — Key Component"},
    {"id": 3, "label": "Layer 3 — Constrained Bottleneck"},
]

SORT_OPTIONS = [
    {"id": "best_fit",    "label": "Best Fit"},
    {"id": "market_cap",  "label": "Market Cap"},
    {"id": "layer",       "label": "Layer"},
    {"id": "grade",       "label": "Grade"},
]


# ── Market cap helpers ─────────────────────────────────────────────────────────

def classify_market_cap(market_cap_usd: Optional[float]) -> str:
    """Return the bucket id for a given market_cap_usd value (or None)."""
    if market_cap_usd is None or market_cap_usd < 2_500_000_000:
        return "micro_cap"
    if market_cap_usd < 20_000_000_000:
        return "small_cap"
    if market_cap_usd < 100_000_000_000:
        return "mid_cap"
    return "large_cap"


def _matches_bucket(candidate: Dict[str, Any], bucket: str) -> bool:
    mc  = candidate.get("market_cap_usd")
    cfg = MARKET_CAP_BUCKETS[bucket]
    if cfg["min"] is not None and (mc is None or mc < cfg["min"]):
        return False
    if cfg["max"] is not None and mc is not None and mc >= cfg["max"]:
        return False
    # micro_cap: allow None
    if bucket == "micro_cap" and mc is None:
        return True
    return True


# ── Sort key factories ─────────────────────────────────────────────────────────

def _sort_key_best_fit(c: Dict[str, Any]):
    return (
        -c.get("best_blend_score", 0.0),
        -c.get("bottleneck_criticality_score", 0.0),
        -c.get("supply_chain_confidence_score", 0.0),
    )


def _sort_key_market_cap(c: Dict[str, Any]):
    mc = c.get("market_cap_usd")
    # None → sort last (use -0 so we negate: large = most negative = first)
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


# ── Main entry point ───────────────────────────────────────────────────────────

def apply_filters_and_sort(
    candidates: List[Dict[str, Any]],
    market_cap_bucket: Optional[str] = None,
    layer: Optional[int] = None,
    sort_by: str = "best_fit",
    limit: int = 20,
) -> Dict[str, Any]:
    """
    Apply optional filters and sort to a list of stored screener candidate dicts.

    Returns:
        {
            "results":               List[dict],   # filtered + sorted + truncated
            "filtered_result_count": int,          # after filtering, before limit
            "available_result_count": int,         # total in snapshot (no filters)
            "active_filters":        dict,         # only the filters that were applied
            "active_sort":           str,
            "limit":                 int,
        }

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
    filtered  = list(candidates)

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
    results = filtered[:limit]

    # ── Active filters metadata ───────────────────────────────────────────────
    active_filters: Dict[str, Any] = {}
    if market_cap_bucket is not None:
        active_filters["market_cap_bucket"] = market_cap_bucket
    if layer is not None:
        active_filters["layer"] = layer

    return {
        "results":               results,
        "filtered_result_count": filtered_count,
        "available_result_count": available,
        "active_filters":        active_filters,
        "active_sort":           sort_by,
        "limit":                 limit,
    }
