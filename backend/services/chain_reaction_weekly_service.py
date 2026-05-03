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
_SOURCE_VERSION = "v1"


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

        scored_rows.append({
            "bottleneck_ticker":       us_ticker,
            "company_name":            node.get("company_name") or profile.get("companyName") or us_ticker,
            "anchor_ticker":           (anchors[0] if anchors else None),
            "anchor_theme":            (themes[0] if themes else None),
            "supply_chain_role":       node.get("role"),
            "bottleneck_type":         node.get("confidence"),
            "layer":                   layer,
            "themes":                  themes,
            "theme_alignment_score":   ta_score,
            "bottleneck_score":        bn_score,
            "momentum_score":          mom_score,
            "volume_score":            vol_score,
            "fundamental_score":       fund_score,
            "social_score":            social_sc,
            "options_score":           options_sc,
            "final_score":             final_score,
            "market_cap":              mcap,
            "change_percent_1d":       chg,
            "evidence":                evidence,
            "discovery_sources":       discovery_sources,
            "country":                 node.get("country"),
            "exchange":                node.get("exchange"),
        })

    # ── Sort by final_score descending ────────────────────────────────────────
    scored_rows.sort(key=lambda r: r["final_score"], reverse=True)
    symbols = [r["bottleneck_ticker"] for r in scored_rows]

    # ── Write to DB ───────────────────────────────────────────────────────────
    today = date.today()
    # week_start = most recent Sunday
    days_since_sunday = (today.weekday() + 1) % 7
    from datetime import timedelta
    week_start = today - timedelta(days=days_since_sunday)

    meta: dict = {
        "node_registry_count": len(NODE_REGISTRY),
        "scored_count": len(scored_rows),
        "social_overlap_count": sum(1 for r in scored_rows if "social_overlap" in r["discovery_sources"]),
        "options_overlap_count": sum(1 for r in scored_rows if "options_overlap" in r["discovery_sources"]),
        "generated_by": "chain_reaction_weekly_service",
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
        "status": "ok" if ok else "db_write_failed",
        "generated_at": generated_at,
        "week_start": week_start.isoformat(),
        "rows_written": len(scored_rows),
        "symbols_count": len(symbols),
        "metadata": meta,
    }
    print(f"[CR_WEEKLY] Generated {len(scored_rows)} scored rows, db_ok={ok}")
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
