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
