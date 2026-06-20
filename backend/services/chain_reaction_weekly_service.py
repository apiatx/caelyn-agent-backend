"""
Chain Reaction Weekly Service.

Generates a dynamic weekly Screener Hub Bottlenecks universe from the
NODE_REGISTRY in supply_chain_graph.py, enriched with live market data
from screener_fundamentals_cache and screener_quote_cache.

Design:
- Reads NODE_REGISTRY (89 curated nodes, static ground truth).
- Enriches each node with cached FMP fundamentals + Tradier quotes.
- Computes a dynamic composite score (bottleneck × momentum × fundamentals).
- Writes scored rows + symbol list to chain_reaction_weekly_outputs (Neon).
- Screener Hub bottlenecks tab reads from this table first.
- NODE_REGISTRY itself is never modified; it remains the static fallback.

Weekly cadence: Sunday 2:15 AM ET (fired by screener_hub_scheduler).
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any, Optional

try:
    from services.playbook.supply_chain_graph import NODE_REGISTRY
except Exception:
    NODE_REGISTRY = {}  # type: ignore

try:
    from data.screener_hub_store import (
        ensure_tables,
        get_fundamentals,
        get_quotes,
        insert_chain_reaction_weekly_output,
        get_latest_chain_reaction_weekly,
    )
except Exception:
    ensure_tables = lambda: None  # type: ignore
    get_fundamentals = lambda s: {}  # type: ignore
    get_quotes = lambda s: {}  # type: ignore
    insert_chain_reaction_weekly_output = lambda **kw: False  # type: ignore
    get_latest_chain_reaction_weekly = lambda **kw: None  # type: ignore

_GLOBAL_CAP = 400
_SOURCE_VERSION = "v2"

# Minimum market cap (USD) to include a symbol — filters pre-revenue shells
_MIN_MCAP_USD = 50_000_000   # $50M

# Diversity: guarantee at least this many small/mid-cap slots in top results
_DIVERSITY_SLOTS = 10          # positions 5–25 reserved for <$20B when available
_DIVERSITY_MCAP_THRESHOLD = 20_000_000_000  # $20B
_DIVERSITY_BN_SCORE_MIN   = 65  # must have this bottleneck_score to qualify


# ── Scoring helpers ────────────────────────────────────────────────────────────

def _to_float(v: Any) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _momentum_score(quote: dict) -> float:
    """0–10 momentum score from quote data."""
    chg = _to_float(quote.get("change_percentage"))
    vol_ratio = None
    vol = _to_float(quote.get("volume"))
    avg_vol = _to_float(quote.get("average_volume"))
    if vol and avg_vol and avg_vol > 0:
        vol_ratio = vol / avg_vol

    score = 5.0  # neutral starting point
    if chg is not None:
        if chg > 3:
            score += 2.0
        elif chg > 1:
            score += 1.0
        elif chg > 0:
            score += 0.5
        elif chg < -5:
            score -= 2.0
        elif chg < -2:
            score -= 1.0

    if vol_ratio is not None:
        if vol_ratio >= 2.0:
            score += 1.5
        elif vol_ratio >= 1.5:
            score += 0.75

    return round(max(0.0, min(10.0, score)), 2)


def _volume_score(quote: dict) -> float:
    """0–10 volume score."""
    vol = _to_float(quote.get("volume"))
    avg = _to_float(quote.get("average_volume"))
    if not vol or not avg or avg <= 0:
        return 5.0
    ratio = vol / avg
    if ratio >= 3.0:
        return 10.0
    if ratio >= 2.0:
        return 8.0
    if ratio >= 1.5:
        return 6.5
    if ratio >= 1.0:
        return 5.0
    return max(0.0, 5.0 - (1.0 - ratio) * 3)


def _fundamental_score(profile: dict, metrics: dict) -> float:
    """0–10 fundamental score using available FMP data."""
    score = 5.0
    mcap = _to_float(profile.get("marketCap") or profile.get("mktCap"))
    rev_growth = _to_float(
        metrics.get("revenueGrowthTTM")
        or metrics.get("revenueGrowth")
    )
    pe = _to_float(metrics.get("peRatioTTM") or metrics.get("priceEarningsRatioTTM"))
    roe = _to_float(metrics.get("roeTTM") or metrics.get("returnOnEquityTTM"))

    if rev_growth is not None:
        if rev_growth > 0.25:
            score += 2.0
        elif rev_growth > 0.10:
            score += 1.0
        elif rev_growth < -0.10:
            score -= 1.0

    if pe is not None and 0 < pe < 50:
        score += 0.5  # reasonable valuation

    if roe is not None and roe > 0.15:
        score += 0.5

    # Small/mid-cap boost (less crowded, more upside)
    if mcap is not None:
        if mcap < 5e9:
            score += 1.5
        elif mcap < 20e9:
            score += 0.75
        elif mcap > 200e9:
            score -= 0.5

    return round(max(0.0, min(10.0, score)), 2)


def _theme_alignment_score(themes: list) -> float:
    """0–100 score: how many themes the node participates in."""
    return min(100.0, len(themes) * 25.0)


def _market_cap_bucket(mcap: Optional[float]) -> str:
    """Classify market cap into human-readable bucket."""
    if mcap is None:
        return "unknown"
    if mcap < 2_000_000_000:
        return "micro_small"       # $50M–$2B
    if mcap < 10_000_000_000:
        return "lower_mid"         # $2B–$10B
    if mcap < 20_000_000_000:
        return "upper_mid"         # $10B–$20B
    return "large_mega"            # $20B+


def _apply_diversity_pass(rows: list[dict]) -> list[dict]:
    """
    Post-sort diversity pass: ensure small/mid-cap hidden gems appear near
    the top of the output and are not buried behind large-cap names.

    Algorithm:
    - Keep top 4 rows exactly as-is (highest-scoring anchors).
    - From positions 5 onwards, interleave qualifying small/mid names
      into the first _DIVERSITY_SLOTS available positions, then append
      the remaining large-cap rows.
    - A row qualifies for promotion if:
        market_cap < _DIVERSITY_MCAP_THRESHOLD AND
        bottleneck_score >= _DIVERSITY_BN_SCORE_MIN
    - Rows without market_cap data are treated as potential small caps
      only if bottleneck_score >= _DIVERSITY_BN_SCORE_MIN + 5.
    """
    if len(rows) <= 5:
        return rows

    anchors = rows[:4]
    rest = rows[4:]

    small_mid: list[dict] = []
    large_cap: list[dict] = []

    for r in rest:
        mcap = r.get("market_cap")
        bn   = float(r.get("bottleneck_score") or 0)
        if mcap is None:
            if bn >= _DIVERSITY_BN_SCORE_MIN + 5:
                small_mid.append(r)
            else:
                large_cap.append(r)
        elif mcap < _DIVERSITY_MCAP_THRESHOLD and bn >= _DIVERSITY_BN_SCORE_MIN:
            small_mid.append(r)
        else:
            large_cap.append(r)

    # Sort each bucket by final_score descending
    small_mid.sort(key=lambda r: r["final_score"], reverse=True)
    large_cap.sort(key=lambda r: r["final_score"], reverse=True)

    # Interleave: take up to _DIVERSITY_SLOTS from small_mid, then append large_cap
    promoted   = small_mid[:_DIVERSITY_SLOTS]
    remaining_small = small_mid[_DIVERSITY_SLOTS:]

    mixed = []
    si, li = 0, 0
    diversity_inserted = 0
    for i in range(len(rest)):
        if diversity_inserted < _DIVERSITY_SLOTS and si < len(promoted):
            mixed.append(promoted[si]); si += 1; diversity_inserted += 1
        elif li < len(large_cap):
            mixed.append(large_cap[li]); li += 1
        elif si < len(promoted):
            mixed.append(promoted[si]); si += 1
        else:
            break

    # Append any remaining rows in score order
    leftover = remaining_small + large_cap[li:]
    leftover.sort(key=lambda r: r["final_score"], reverse=True)

    return anchors + mixed + leftover


def _get_tradeable_ticker(ticker: str, node: dict) -> str:
    """Return the best US-tradeable ticker for this node."""
    return str(
        node.get("us_access_proxy")
        or node.get("adr_ticker")
        or ticker
    ).upper()


# ── Main generation logic ──────────────────────────────────────────────────────

def generate_chain_reaction_weekly(
    social_symbols: Optional[set] = None,
    options_symbols: Optional[set] = None,
) -> dict:
    """
    Score all NODE_REGISTRY nodes with live market data and write to DB.

    Parameters
    ----------
    social_symbols   : Set of symbols appearing in x_consensus social screener.
    options_symbols  : Set of symbols with notable options flow.

    Returns
    -------
    Summary dict: {status, rows_written, generated_at, ...}
    """
    ensure_tables()
    if not NODE_REGISTRY:
        return {"status": "error", "error": "NODE_REGISTRY empty"}

    social_set   = social_symbols or set()
    options_set  = options_symbols or set()

    # ── Collect tradeable tickers ──────────────────────────────────────────────
    ticker_to_node: dict[str, dict] = {}
    for raw_ticker, node in NODE_REGISTRY.items():
        if not isinstance(node, dict):
            continue
        us_ticker = _get_tradeable_ticker(raw_ticker, node)
        # Track the best node per US ticker (prefer higher bottleneck_score)
        existing = ticker_to_node.get(us_ticker)
        if existing is None or int(node.get("bottleneck_score") or 0) > int(existing.get("_bottleneck_score_raw") or 0):
            ticker_to_node[us_ticker] = {**node, "_raw_ticker": raw_ticker, "_bottleneck_score_raw": node.get("bottleneck_score", 0)}

    all_tickers = list(ticker_to_node.keys())

    # ── Load cached market data ────────────────────────────────────────────────
    fundamentals = get_fundamentals(all_tickers)
    quotes_raw   = get_quotes(all_tickers)

    # ── Score each node ────────────────────────────────────────────────────────
    scored_rows: list[dict] = []
    for us_ticker, node in ticker_to_node.items():
        f_row   = fundamentals.get(us_ticker) or {}
        q_row   = quotes_raw.get(us_ticker) or {}
        profile = f_row.get("profile") or {}
        metrics = f_row.get("metrics") or {}
        quote   = q_row.get("quote") or {}

        bn_score = float(node.get("bottleneck_score") or 50)
        layer    = int(node.get("layer") or 2)
        themes   = node.get("themes") or []
        evidence = node.get("evidence") or []
        anchors  = node.get("giant_anchors") or []

        # Layer bonus: deeper (3/4) nodes are more supply-chain-critical
        layer_bonus = {0: -5, 1: 0, 2: 2, 3: 5, 4: 8}.get(layer, 0)

        mom_score   = _momentum_score(quote)
        vol_score   = _volume_score(quote)
        fund_score  = _fundamental_score(profile, metrics)
        ta_score    = _theme_alignment_score(themes)
        social_sc   = 3.0 if us_ticker in social_set else 0.0
        options_sc  = 3.0 if us_ticker in options_set else 0.0

        # Weighted composite final score (0–100 scale)
        final_score = round(
            bn_score * 0.50
            + mom_score * 10 * 0.15   # scale mom 0-10 → 0-100
            + fund_score * 10 * 0.10
            + ta_score * 0.10
            + social_sc * 2
            + options_sc * 2
            + layer_bonus
        , 2)
        final_score = max(0.0, min(100.0, final_score))

        discovery_sources = ["node_registry"]
        if us_ticker in social_set:
            discovery_sources.append("social_overlap")
        if us_ticker in options_set:
            discovery_sources.append("options_overlap")

        mcap = _to_float(profile.get("marketCap") or profile.get("mktCap"))
        chg  = _to_float(quote.get("change_percentage"))

        # Skip pre-revenue shells below minimum market cap threshold
        if mcap is not None and mcap < _MIN_MCAP_USD:
            continue

        scored_rows.append({
            "bottleneck_ticker":       us_ticker,
            "company_name":            node.get("company_name") or profile.get("companyName") or us_ticker,
            "anchor_ticker":           (anchors[0] if anchors else None),
            "anchor_theme":            (themes[0] if themes else None),
            "supply_chain_role":       node.get("role"),
            "bottleneck_type":         node.get("confidence"),
            "bottleneckReason":        node.get("role"),        # alias for UI
            "layer":                   layer,
            "themes":                  themes,
            "theme":                   (themes[0] if themes else None),
            "theme_alignment_score":   ta_score,
            "bottleneck_score":        bn_score,
            "momentum_score":          mom_score,
            "volume_score":            vol_score,
            "fundamental_score":       fund_score,
            "social_score":            social_sc,
            "options_score":           options_sc,
            "final_score":             final_score,
            "market_cap":              mcap,
            "marketCap":               mcap,                    # alias for UI
            "marketCapBucket":         _market_cap_bucket(mcap),
            "revenueSignal":           (
                "revenue_growth_strong" if _to_float(metrics.get("revenueGrowthTTM") or metrics.get("revenueGrowth") or 0) is not None
                and (_to_float(metrics.get("revenueGrowthTTM") or metrics.get("revenueGrowth") or 0) or 0) > 0.15
                else "revenue_present" if profile.get("revenue") or metrics.get("revenueTTM")
                else None
            ),
            "evidence":                evidence,
            "change_percent_1d":       chg,
            "discovery_sources":       discovery_sources,
            "country":                 node.get("country"),
            "exchange":                node.get("exchange"),
            "lastUpdated":             datetime.now(timezone.utc).isoformat(),
        })

    # ── Validate: reject empty output ────────────────────────────────────────
    if not scored_rows:
        return {"status": "error", "error": "No scored rows produced — NODE_REGISTRY may be empty or all tickers filtered"}

    valid_rows = [r for r in scored_rows if r.get("bottleneck_ticker") and r.get("supply_chain_role")]
    if len(valid_rows) < 5:
        return {
            "status": "error",
            "error": f"Output validation failed: only {len(valid_rows)} rows have required ticker+role fields",
            "raw_count": len(scored_rows),
        }

    # ── Sort by final_score descending ────────────────────────────────────────
    scored_rows.sort(key=lambda r: r["final_score"], reverse=True)

    # ── Diversity pass: promote hidden-gem small/mid caps ─────────────────────
    scored_rows = _apply_diversity_pass(scored_rows)

    symbols = [r["bottleneck_ticker"] for r in scored_rows]

    # ── Write to DB ───────────────────────────────────────────────────────────
    today = date.today()
    # week_start = most recent Sunday
    days_since_sunday = (today.weekday() + 1) % 7
    from datetime import timedelta
    week_start = today - timedelta(days=days_since_sunday)

    # Market cap bucket distribution for diagnostics
    bucket_dist: dict[str, int] = {}
    for r in scored_rows:
        b = r.get("marketCapBucket") or "unknown"
        bucket_dist[b] = bucket_dist.get(b, 0) + 1

    meta: dict = {
        "node_registry_count":  len(NODE_REGISTRY),
        "scored_count":         len(scored_rows),
        "social_overlap_count": sum(1 for r in scored_rows if "social_overlap" in r["discovery_sources"]),
        "options_overlap_count":sum(1 for r in scored_rows if "options_overlap" in r["discovery_sources"]),
        "generated_by":         "chain_reaction_weekly_service",
        "source_version":       _SOURCE_VERSION,
        "market_cap_buckets":   bucket_dist,
    }

    ok = insert_chain_reaction_weekly_output(
        week_start=week_start.isoformat(),
        symbols=symbols[:_GLOBAL_CAP],
        rows=scored_rows[:_GLOBAL_CAP],
        metadata=meta,
        source_version=_SOURCE_VERSION,
        status="ok",
    )

    generated_at = datetime.now(timezone.utc).isoformat()
    result = {
        "status":          "ok" if ok else "db_write_failed",
        "generated_at":    generated_at,
        "week_start":      week_start.isoformat(),
        "rows_written":    len(scored_rows),
        "symbols_count":   len(symbols),
        "metadata":        meta,
        "market_cap_buckets": bucket_dist,
    }
    print(f"[CR_WEEKLY] Generated {len(scored_rows)} scored rows (v2 diversity pass applied), db_ok={ok}, buckets={bucket_dist}")
    return result


# ── Multi-anchor extension ─────────────────────────────────────────────────────
#
# Each config defines ONE anchor's theme filter.  The existing
# build_cross_theme_top() is called once per anchor; the only thing that
# changes is which themes are used to slice the full universe.
#
# Private companies (SPCX, ANTHROPIC, OPENAI) are mapped to the Serenity
# theme IDs that best represent their supply-chain footprint.
# Public companies (NVDA, TSM, GOOG) use the themes from GIANT_MAP.

MULTI_ANCHOR_CONFIGS: list[dict] = [
    {
        "anchor":        "NVDA",
        "anchor_name":   "NVIDIA",
        "anchor_themes": [
            "ai_infrastructure", "advanced_packaging_test", "memory",
            "photonics_cpo", "ai_power_energy", "semicap_supply_chain",
        ],
    },
    {
        "anchor":        "SPCX",
        "anchor_name":   "SpaceX",
        "anchor_themes": [
            "space", "space_sensing", "defense_optics", "defense",
        ],
    },
    {
        "anchor":        "ANTHROPIC",
        "anchor_name":   "Anthropic",
        "anchor_themes": [
            "ai_infrastructure", "neocloud",
        ],
    },
    {
        "anchor":        "OPENAI",
        "anchor_name":   "OpenAI",
        "anchor_themes": [
            "ai_infrastructure", "neocloud",
        ],
    },
    {
        "anchor":        "TSM",
        "anchor_name":   "Taiwan Semiconductor Manufacturing Company",
        "anchor_themes": [
            "semicap_supply_chain", "advanced_packaging_test",
        ],
    },
    {
        "anchor":        "GOOG",
        "anchor_name":   "Google / Alphabet",
        "anchor_themes": [
            "ai_infrastructure", "neocloud", "advanced_packaging_test",
            "photonics_cpo",
        ],
    },
]


def build_anchor_top(
    anchor: str,
    anchor_name: str,
    anchor_themes: list[str],
    limit: int = 20,
    max_age_days: int = 10,
) -> dict:
    """
    Anchor-filtered variant of build_cross_theme_top().

    This is the ONLY new function added for multi-anchor support.  It
    calls the existing build_cross_theme_top() with a large limit to
    retrieve the full scored universe, then keeps only the rows whose
    `themes` field intersects with this anchor's theme set.

    No scoring, ranking, or normalization logic is changed.
    The existing /api/bottlenecks/current endpoint is completely unaffected.

    Parameters
    ----------
    anchor        : Short identifier (e.g. "NVDA", "SPCX").
    anchor_name   : Display name (e.g. "NVIDIA", "SpaceX").
    anchor_themes : Serenity theme IDs that define this anchor's supply-chain
                    footprint.  Rows whose themes intersect this set are kept.
    limit         : Maximum rows to return for this anchor.
    max_age_days  : Reject CR data older than this many days.

    Returns
    -------
    Dict with status + rows shaped identically to build_cross_theme_top().
    Adds top-level `anchor` and `anchor_name` fields.
    """
    # ── Step 1: Get the full scored universe via the existing function ──────────
    # Pass limit=400 (= _GLOBAL_CAP) so the diversity gates operate on the full
    # universe rather than a truncated slice.  With require_* = 0 the gates are
    # no-ops and the function simply returns all rows in final_score order.
    full = build_cross_theme_top(
        limit=_GLOBAL_CAP,
        max_age_days=max_age_days,
        require_themes=0,
        require_small_mid=0,
        require_gems=0,
    )

    if full.get("status") == "error":
        return {
            **full,
            "anchor":      anchor,
            "anchor_name": anchor_name,
        }

    # ── Step 2: Filter to anchor-relevant rows ──────────────────────────────────
    anchor_theme_set = set(anchor_themes)
    anchor_rows = [
        r for r in full.get("rows", [])
        if set(r.get("themes") or []) & anchor_theme_set
    ]

    if not anchor_rows:
        return {
            "status":          "error",
            "error":           (
                f"No rows found for anchor {anchor!r} "
                f"with themes {anchor_themes!r}. "
                "Run POST /api/admin/bottlenecks/refresh to regenerate CR data."
            ),
            "anchor":          anchor,
            "anchor_name":     anchor_name,
            "rows":            [],
            "visible_tickers": [],
        }

    # Rows arrive pre-sorted by final_score from build_cross_theme_top().
    selected = anchor_rows[:limit]

    themes_in_anchor: list[str] = sorted(
        {t for r in selected for t in (r.get("themes") or [])}
    )
    vis_buckets: dict[str, int] = {}
    for r in selected:
        b = r.get("marketCapBucket") or "unknown"
        vis_buckets[b] = vis_buckets.get(b, 0) + 1

    return {
        "status":                  "ok",
        "anchor":                  anchor,
        "anchor_name":             anchor_name,
        "anchor_themes":           anchor_themes,
        "rows":                    selected,
        "visible_count":           len(selected),
        "visible_tickers":         [r["bottleneck_ticker"] for r in selected],
        "universe_count":          full.get("universe_count", 0),
        "themes_in_anchor":        themes_in_anchor,
        "market_cap_buckets":      vis_buckets,
        "week_start":              full.get("week_start"),
        "visible_generated_at":    full.get("visible_generated_at"),
        "source_version":          full.get("source_version"),
    }


def get_latest_cr_weekly_output(max_age_days: int = 10) -> Optional[dict]:
    """
    Return the most recent chain_reaction_weekly_outputs row if it exists
    and is within max_age_days. Returns None otherwise.
    """
    ensure_tables()
    try:
        return get_latest_chain_reaction_weekly(max_age_days=max_age_days)
    except Exception as e:
        print(f"[CR_WEEKLY] get_latest error: {e}")
        return None


# ── Phase 6 hidden-gem ticker set (small/mid-cap cross-theme bottlenecks) ──────
# Used by the cross-theme diversity gate to guarantee representation.

_PHASE6_HIDDEN_GEMS: frozenset[str] = frozenset({
    "CCJ", "LEU", "BWXT", "NXE", "UEC", "SMR",    # nuclear / uranium / SMR
    "MP", "UUUU",                                   # rare earth / critical materials
    "FLNC", "STEM",                                 # battery / grid storage
    "POWL", "GTLS",                                 # specialty grid hardware
    "VICR",                                         # power ICs
    "TDY", "CACI",                                  # defense niche
    "ACMR", "CEVA", "VIAV", "AZTA",                # semiconductor niche
    "TRMB",                                         # robotics / precision
    "RYCEY",                                        # UK nuclear / defense ADR
})

_VISIBLE_TOP_DEFAULT: int = 30
_VIS_THEMES_REQUIRED: int = 4      # min distinct themes in visible set
_VIS_SMALL_MID_MIN:   int = 5      # min <$20B names in visible set
_VIS_GEM_MIN:         int = 2      # min Phase 6 hidden gems in visible set
_VIS_SMALL_MID_MCAP:  float = 20_000_000_000.0   # $20B threshold
_VIS_GEM_BN_MIN:      float = 65.0               # bn_score floor for gem promotion


def _vis_is_gem(r: dict) -> bool:
    return r.get("bottleneck_ticker", "").upper() in _PHASE6_HIDDEN_GEMS


def _vis_is_small_mid(r: dict) -> bool:
    mcap = r.get("market_cap") or r.get("marketCap")
    bn   = float(r.get("bottleneck_score") or 0)
    if mcap is None:
        return bn >= _DIVERSITY_BN_SCORE_MIN        # unknown cap: trust bn_score
    return mcap < _VIS_SMALL_MID_MCAP and bn >= 55.0


def _vis_promote(pool: list, predicate, n_needed: int, selected: list) -> tuple[list, list]:
    """
    Promote up to n_needed rows matching predicate from pool into selected,
    swapping out the lowest-final_score non-protected row in selected.
    Returns (new_selected, new_pool).
    """
    promoted = 0
    pool = list(pool)
    selected = list(selected)
    candidates = sorted((r for r in pool if predicate(r)),
                        key=lambda r: r.get("final_score", 0), reverse=True)
    for cand in candidates:
        if promoted >= n_needed:
            break
        # Find victim: prefer non-gem AND non-small-mid; fallback to non-gem
        victims = [r for r in selected if not _vis_is_gem(r) and not _vis_is_small_mid(r)]
        if not victims:
            victims = [r for r in selected if not _vis_is_gem(r)]
        if not victims:
            break
        victims.sort(key=lambda r: r.get("final_score", 0))
        victim = victims[0]
        selected = [r for r in selected if r is not victim]
        pool = [r for r in pool if r is not cand]
        selected.append(cand)
        pool.append(victim)
        promoted += 1
    return selected, pool


def build_cross_theme_top(
    limit: int = _VISIBLE_TOP_DEFAULT,
    max_age_days: int = 10,
    require_themes: int = _VIS_THEMES_REQUIRED,
    require_small_mid: int = _VIS_SMALL_MID_MIN,
    require_gems: int = _VIS_GEM_MIN,
    prev_visible_tickers: Optional[list] = None,
) -> dict:
    """
    Build a cross-theme diverse top-N from the latest chain_reaction_weekly_outputs.

    Algorithm:
      1. Load the latest CR weekly output (already final_score-sorted + diversity-passed).
      2. Start with the natural top `limit` rows.
      3. Apply 3 diversity criteria — promote from the tail if not met:
           a) ≥ require_themes distinct primary themes
           b) ≥ require_small_mid names with market_cap < $20B
           c) ≥ require_gems Phase 6 hidden-gem tickers
      4. Return rows + full per-gem diagnostics + tickers_changed flag.

    Returns a dict with:
      status, rows, visible_snapshot_id, visible_generated_at,
      visible_count, visible_tickers, universe_count, universe_tickers,
      universe_only_tickers, overlap_count, selected_from_universe_count,
      gem_candidates_with_reasons, diversity_gate_result,
      themes_in_visible, market_cap_buckets_in_visible,
      tickers_changed, metadata_refreshed_only
    """
    cr_row = get_latest_cr_weekly_output(max_age_days=max_age_days)
    if not cr_row:
        return {
            "status": "error",
            "error": (
                f"No chain_reaction_weekly_output found within {max_age_days} days. "
                "Run POST /api/admin/bottlenecks/refresh to generate fresh data."
            ),
            "rows": [], "visible_tickers": [],
        }

    def _us_tradeable(ticker: str) -> bool:
        """Return True for US-listed tickers; reject foreign exchange codes like 3037.TW, 8035.T."""
        if not ticker:
            return False
        if "." in ticker:
            suffix = ticker.rsplit(".", 1)[-1]
            # Allow single-letter class suffixes: BRK.A, BRK.B
            if len(suffix) == 1 and suffix.isalpha():
                return True
            # Reject exchange/country suffixes 2+ chars: .TW .HK .DE .KS .T etc.
            return False
        return True

    all_rows: list[dict] = [
        r for r in (cr_row.get("rows") or [])
        if r.get("bottleneck_ticker") and _us_tradeable(r["bottleneck_ticker"])
    ]
    if not all_rows:
        return {
            "status": "error",
            "error": "chain_reaction_weekly_output exists but rows list is empty.",
            "rows": [], "visible_tickers": [],
        }

    cr_generated_at = cr_row.get("generated_at")
    cr_week_start   = cr_row.get("week_start")
    universe_tickers = [r["bottleneck_ticker"] for r in all_rows]

    # ── Start: natural top-N from pre-sorted rows ─────────────────────────────
    selected: list[dict] = list(all_rows[:limit])
    pool:     list[dict] = list(all_rows[limit:])

    # ── Gate C: ≥ require_gems Phase 6 hidden gems ────────────────────────────
    gems_now = sum(1 for r in selected if _vis_is_gem(r))
    needed   = max(0, require_gems - gems_now)
    if needed:
        gem_pool = [r for r in pool if _vis_is_gem(r)
                    and float(r.get("bottleneck_score") or 0) >= _VIS_GEM_BN_MIN]
        selected, pool = _vis_promote(gem_pool, _vis_is_gem, needed, selected)

    # ── Gate B: ≥ require_small_mid small/mid-cap names ───────────────────────
    sm_now  = sum(1 for r in selected if _vis_is_small_mid(r))
    needed  = max(0, require_small_mid - sm_now)
    if needed:
        sm_pool = [r for r in pool if _vis_is_small_mid(r) and not _vis_is_gem(r)]
        selected, pool = _vis_promote(sm_pool, _vis_is_small_mid, needed, selected)

    # ── Gate A: ≥ require_themes distinct themes (diagnostic only — swap risky) ─
    themes_set: set[str] = set()
    for r in selected:
        for t in (r.get("themes") or [r.get("anchor_theme")] or []):
            if t:
                themes_set.add(str(t))

    # ── Build visible metadata ────────────────────────────────────────────────
    visible_tickers = [r["bottleneck_ticker"] for r in selected]
    vis_set         = set(visible_tickers)
    univ_set        = set(universe_tickers)

    vis_buckets: dict[str, int] = {}
    for r in selected:
        b = r.get("marketCapBucket") or "unknown"
        vis_buckets[b] = vis_buckets.get(b, 0) + 1

    # ── Per-gem diagnostics ───────────────────────────────────────────────────
    all_rows_by_ticker = {r["bottleneck_ticker"]: (i, r) for i, r in enumerate(all_rows)}
    gem_diag: dict[str, dict] = {}
    for ticker in sorted(_PHASE6_HIDDEN_GEMS):
        in_universe = ticker in univ_set
        in_visible  = ticker in vis_set
        if ticker in all_rows_by_ticker:
            rank, row = all_rows_by_ticker[ticker]
            fs   = row.get("final_score")
            bn   = row.get("bottleneck_score")
            buck = row.get("marketCapBucket", "unknown")
            if in_visible:
                reason = f"included: rank={rank+1} final_score={fs} bn={bn} bucket={buck}"
            else:
                reason = (
                    f"excluded_below_diversity_gate: rank={rank+1} final_score={fs} "
                    f"bn={bn} bucket={buck} — scored below top-{limit} diversity threshold"
                )
        elif in_universe:
            row, fs, bn, buck = None, None, None, "unknown"
            reason = "excluded_universe_mismatch: in universe snapshot but missing from rows_json"
        else:
            row, fs, bn, buck = None, None, None, "unknown"
            reason = (
                "not_in_universe: ticker absent from chain_reaction_weekly_output. "
                "Possible causes: (1) no cached fundamentals/quotes for this symbol, "
                "(2) market_cap below $50M threshold, or (3) foreign ticker without US proxy."
            )
        gem_diag[ticker] = {
            "in_universe":      in_universe,
            "in_visible":       in_visible,
            "reason":           reason,
            "final_score":      fs,
            "bottleneck_score": bn,
            "marketCapBucket":  buck,
        }

    diversity_gate_result = {
        "themes_required":       require_themes,
        "themes_achieved":       len(themes_set),
        "themes_present":        sorted(themes_set),
        "themes_gate_met":       len(themes_set) >= require_themes,
        "small_mid_required":    require_small_mid,
        "small_mid_achieved":    sum(1 for r in selected if _vis_is_small_mid(r)),
        "small_mid_gate_met":    sum(1 for r in selected if _vis_is_small_mid(r)) >= require_small_mid,
        "hidden_gems_required":  require_gems,
        "hidden_gems_achieved":  sum(1 for r in selected if _vis_is_gem(r)),
        "hidden_gems_gate_met":  sum(1 for r in selected if _vis_is_gem(r)) >= require_gems,
    }

    # ── tickers_changed vs previous ───────────────────────────────────────────
    prev_set = set(prev_visible_tickers or [])
    tickers_changed = (prev_set != vis_set) if prev_visible_tickers is not None else None

    snap_id = f"cr_top_{cr_week_start or 'unknown'}_{limit}"

    return {
        "status":                       "ok",
        "visible_snapshot_id":          snap_id,
        "visible_generated_at":         cr_generated_at,
        "visible_count":                len(selected),
        "visible_tickers":              visible_tickers,
        "universe_count":               len(all_rows),
        "universe_tickers":             universe_tickers,
        "universe_only_tickers":        [t for t in universe_tickers if t not in vis_set],
        "overlap_count":                len(vis_set & univ_set),
        "selected_from_universe_count": len(selected),
        "gem_candidates_with_reasons":  gem_diag,
        "diversity_gate_result":        diversity_gate_result,
        "themes_in_visible":            sorted(themes_set),
        "market_cap_buckets_in_visible": vis_buckets,
        "tickers_changed":              tickers_changed,
        "metadata_refreshed_only":      False if tickers_changed else (True if tickers_changed is False else None),
        "week_start":                   cr_week_start,
        "source_version":               cr_row.get("source_version"),
        "rows":                         selected,
    }
