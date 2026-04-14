"""
Investor Mode — Orchestration Layer.

Coordinates the full Investor Signal Engine pipeline:
  1. Fetch scored markets from existing Polymarket intelligence (reuses cache)
  2. Classify markets into equity-relevant themes
  3. Build theme clusters (aggregated signals per theme)
  4. Compute sector/equity impacts for each cluster
  5. Derive regime scoreboard
  6. Assemble investor-facing payload

All heavy Polymarket fetching is delegated to the existing PolymarketIntelligence
instance — no new API calls, no duplicate caching.

Caching: investor payloads cached for _INVESTOR_CACHE_TTL seconds.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from data.cache import cache
from services.predict.polymarket_intelligence import polymarket_intel
from services.predict.scoring import score_markets
from services.predict.investor.classifier import classify_markets, filter_equity_relevant
from services.predict.investor.clustering import build_theme_clusters
from services.predict.investor.impact_engine import (
    get_theme_impact, SECTOR_STOCKS, THEME_BASKETS, ThemeImpact,
)
from services.predict.investor.regime import compute_regime_scoreboard
from services.predict.investor.themes import THEME_BY_ID

logger = logging.getLogger("investor_intel")

_INVESTOR_CACHE_TTL = 150   # 2.5 min — aligned with scored cache TTL


class InvestorIntel:
    """Main entry point for Investor mode backend."""

    async def _get_scored(self) -> list[dict]:
        """
        Fetch and score Polymarket markets.
        Reuses the existing scored-markets cache where possible.
        """
        scored = await polymarket_intel.get_scored_markets(limit=200)
        if not scored:
            return []
        # Ensure composite_score key exists for downstream use
        for m in scored:
            if "composite_score" not in m:
                m["composite_score"] = m.get("scores", {}).get("composite", 50)
        return scored

    async def get_overview(self) -> dict:
        """
        Full investor overview: top_equity_signals, sector_rotation, watchlists,
        regime_scoreboard, theme_clusters.

        Cached for _INVESTOR_CACHE_TTL seconds.
        """
        key = "pm:investor:overview"
        cached = cache.get(key)
        if cached is not None:
            return cached

        try:
            scored = await self._get_scored()
        except Exception as e:
            logger.error(f"Failed to fetch scored markets: {e}")
            return {"error": str(e), "generated_at": _now()}

        # ── Classify → Cluster → Impact → Regime ──────────────────────────
        classified = classify_markets(scored)
        equity_relevant = filter_equity_relevant(classified)
        clusters = build_theme_clusters(equity_relevant)
        cluster_impacts = _compute_cluster_impacts(clusters)
        regime = compute_regime_scoreboard(clusters)

        # ── Assemble output sections ───────────────────────────────────────
        top_equity_signals = _build_top_equity_signals(clusters, cluster_impacts)
        sector_rotation = _build_sector_rotation(cluster_impacts, regime)
        watchlists = _build_watchlists(cluster_impacts)

        # Enrich theme_clusters with impact data so frontend can render
        # theme cards directly from overview without a separate /themes call.
        enriched_clusters = _enrich_clusters_with_impact(clusters, cluster_impacts)

        # Array form of regime_scoreboard for .map() in frontend code.
        # Each item: { id, label, score, direction, confidence, supporting_themes, description }
        regime_list = [
            {"id": k, **v}
            for k, v in regime.items()
        ]

        result = {
            "generated_at": _now(),
            "equity_relevant_market_count": len(equity_relevant),
            "total_market_count": len(scored),

            # ── Hero section ──────────────────────────────────────────────
            "top_equity_signals": top_equity_signals,

            # ── Sector rotation ───────────────────────────────────────────
            "sector_rotation": sector_rotation,

            # ── Watchlists (nested + flattened at top level) ──────────────
            # nested: data.watchlists.bullish_watchlist  (backward-compat)
            "watchlists": watchlists,
            # flattened: data.bullish_watchlist  (preferred — avoids double nesting)
            "bullish_watchlist": watchlists["bullish_watchlist"],
            "bearish_watchlist": watchlists["bearish_watchlist"],
            "conditional_watchlist": watchlists["conditional_watchlist"],
            "watchlist_notes": watchlists["watchlist_notes"],

            # ── Regime scoreboard ─────────────────────────────────────────
            # dict form:  data.regime_scoreboard.risk_on_vs_risk_off.label
            "regime_scoreboard": regime,
            # array form: data.regime_scoreboard_list.map(r => r.label)
            "regime_scoreboard_list": regime_list,

            # ── Theme clusters (now includes impact data) ─────────────────
            "theme_clusters": enriched_clusters,
        }

        cache.set(key, result, _INVESTOR_CACHE_TTL)
        return result

    async def get_themes(self) -> dict:
        """Return theme clusters with supporting markets (drill-down)."""
        key = "pm:investor:themes"
        cached = cache.get(key)
        if cached is not None:
            return cached

        try:
            scored = await self._get_scored()
        except Exception as e:
            return {"error": str(e), "generated_at": _now()}

        classified = classify_markets(scored)
        equity_relevant = filter_equity_relevant(classified)
        clusters = build_theme_clusters(equity_relevant)
        cluster_impacts = _compute_cluster_impacts(clusters)

        # Enrich clusters with impact data
        enriched_clusters = []
        for cl in clusters:
            impact = cluster_impacts.get(cl["theme_id"])
            enriched = dict(cl)
            if impact:
                enriched["bullish_sectors"] = impact.bullish_sectors
                enriched["bearish_sectors"] = impact.bearish_sectors
                enriched["bullish_stocks"] = impact.bullish_stocks
                enriched["bearish_stocks"] = impact.bearish_stocks
                enriched["asset_baskets"] = impact.baskets
                enriched["regime_implications"] = impact.regime_implications
                enriched["narrative"] = impact.narrative
            enriched_clusters.append(enriched)

        result = {
            "generated_at": _now(),
            "theme_clusters": enriched_clusters,
            "cluster_count": len(enriched_clusters),
        }
        cache.set(key, result, _INVESTOR_CACHE_TTL)
        return result

    async def get_regime(self) -> dict:
        """Return regime scoreboard only (lightweight endpoint)."""
        key = "pm:investor:regime"
        cached = cache.get(key)
        if cached is not None:
            return cached

        try:
            scored = await self._get_scored()
        except Exception as e:
            return {"error": str(e), "generated_at": _now()}

        classified = classify_markets(scored)
        equity_relevant = filter_equity_relevant(classified)
        clusters = build_theme_clusters(equity_relevant)
        regime = compute_regime_scoreboard(clusters)

        regime_list = [{"id": k, **v} for k, v in regime.items()]
        result = {
            "generated_at": _now(),
            "regime_scoreboard": regime,
            "regime_scoreboard_list": regime_list,
            "supporting_cluster_count": len(clusters),
        }
        cache.set(key, result, _INVESTOR_CACHE_TTL)
        return result

    async def get_watchlists(self) -> dict:
        """Return stock watchlists derived from current prediction market signals."""
        key = "pm:investor:watchlists"
        cached = cache.get(key)
        if cached is not None:
            return cached

        try:
            scored = await self._get_scored()
        except Exception as e:
            return {"error": str(e), "generated_at": _now()}

        classified = classify_markets(scored)
        equity_relevant = filter_equity_relevant(classified)
        clusters = build_theme_clusters(equity_relevant)
        cluster_impacts = _compute_cluster_impacts(clusters)
        watchlists = _build_watchlists(cluster_impacts)

        # Also add the static full sector→stock reference
        result = {
            "generated_at": _now(),
            "watchlists": watchlists,
            "sector_reference": SECTOR_STOCKS,
        }
        cache.set(key, result, _INVESTOR_CACHE_TTL)
        return result


# ── Internal helpers ──────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _enrich_clusters_with_impact(
    clusters: list[dict],
    impacts: dict[str, ThemeImpact],
) -> list[dict]:
    """
    Inject impact data (bullish/bearish sectors+stocks, baskets, regime_implications,
    narrative) into raw cluster dicts so the frontend can render full theme cards
    from the overview response without a separate /themes call.
    """
    enriched = []
    for cl in clusters:
        tid = cl["theme_id"]
        impact = impacts.get(tid)
        ec = dict(cl)
        if impact:
            ec["bullish_sectors"] = impact.bullish_sectors
            ec["bearish_sectors"] = impact.bearish_sectors
            ec["bullish_stocks"] = impact.bullish_stocks
            ec["bearish_stocks"] = impact.bearish_stocks
            ec["asset_baskets"] = impact.baskets
            ec["regime_implications"] = impact.regime_implications
            ec["narrative"] = impact.narrative
        else:
            ec.setdefault("bullish_sectors", [])
            ec.setdefault("bearish_sectors", [])
            ec.setdefault("bullish_stocks", [])
            ec.setdefault("bearish_stocks", [])
            ec.setdefault("asset_baskets", THEME_BASKETS.get(tid, []))
            ec.setdefault("regime_implications", [])
            ec.setdefault("narrative", "")
        enriched.append(ec)
    return enriched


def _compute_cluster_impacts(clusters: list[dict]) -> dict[str, ThemeImpact]:
    """Compute ThemeImpact for each cluster based on its summary_direction."""
    impacts: dict[str, ThemeImpact] = {}
    for cl in clusters:
        tid = cl["theme_id"]
        direction = cl.get("summary_direction", "mixed")
        impact = get_theme_impact(tid, direction)
        if impact:
            impacts[tid] = impact
    return impacts


def _build_top_equity_signals(
    clusters: list[dict],
    impacts: dict[str, ThemeImpact],
    max_signals: int = 5,
) -> list[dict]:
    """
    Build the top N equity signals for the Investor tab hero section.

    Rank clusters by:
      confidence_score * regime_signal_strength * market_count
    """
    scored_clusters = sorted(
        clusters,
        key=lambda c: (
            c.get("confidence_score", 0) * 0.4
            + c.get("regime_signal_strength", 0) * 0.35
            + min(40, c.get("market_count", 0) * 4) * 0.25
        ),
        reverse=True,
    )[:max_signals]

    signals = []
    for cl in scored_clusters:
        tid = cl["theme_id"]
        impact = impacts.get(tid)
        theme_def = THEME_BY_ID.get(tid)

        direction = cl.get("summary_direction", "mixed")
        shift_24h = cl.get("weighted_odds_shift_24h", 0)
        shift_7d = cl.get("weighted_odds_shift_7d", 0)

        odds_summary = _odds_move_summary(shift_24h, shift_7d, direction)

        if impact:
            bullish_stocks = impact.bullish_stocks[:5]
            bearish_stocks = impact.bearish_stocks[:5]
            bullish_sectors = impact.bullish_sectors
            bearish_sectors = impact.bearish_sectors
            regime_impact = impact.regime_implications
            narrative = impact.narrative
        else:
            bullish_stocks = []
            bearish_stocks = []
            bullish_sectors = theme_def.bullish_bias_sectors if theme_def else []
            bearish_sectors = theme_def.bearish_bias_sectors if theme_def else []
            regime_impact = []
            narrative = ""

        conf_score = cl.get("confidence_score", 0)
        confidence_label = (
            "high" if conf_score >= 60 else
            "medium" if conf_score >= 35 else
            "low"
        )

        why_it_matters = _why_it_matters(tid, direction, cl)

        signals.append({
            "theme_id": tid,
            "title": f"{cl.get('theme_emoji', '')} {cl['theme_name']}",
            "summary": cl.get("description", ""),
            "why_it_matters": why_it_matters,
            "supporting_markets": cl.get("supporting_markets", [])[:4],
            "market_count": cl.get("market_count", 0),
            "odds_move_summary": odds_summary,
            "summary_direction": direction,
            "bullish_sectors": bullish_sectors,
            "bearish_sectors": bearish_sectors,
            "bullish_stocks": bullish_stocks,
            "bearish_stocks": bearish_stocks,
            "asset_baskets": THEME_BASKETS.get(tid, []),
            "regime_impact": regime_impact,
            "confidence": confidence_label,
            "confidence_score": conf_score,
            "narrative": _narrative_with_qualifier(narrative, confidence_label, direction),
            "watchlist_priority": "high" if conf_score >= 60 else "medium" if conf_score >= 35 else "watch",
        })

    return signals


def _build_sector_rotation(
    impacts: dict[str, ThemeImpact],
    regime: dict,
) -> dict:
    """
    Derive sector rotation signals from cluster impacts + regime.
    Returns: strongest_positive, strongest_negative, emerging, fading.
    """
    # Count bullish/bearish sector mentions weighted by confidence
    bullish_tally: dict[str, float] = {}
    bearish_tally: dict[str, float] = {}

    for tid, impact in impacts.items():
        # We don't have per-cluster confidence directly here; use 1.0
        for sec in impact.bullish_sectors:
            bullish_tally[sec] = bullish_tally.get(sec, 0) + 1
        for sec in impact.bearish_sectors:
            bearish_tally[sec] = bearish_tally.get(sec, 0) + 1

    strongest_positive = sorted(bullish_tally, key=lambda k: -bullish_tally[k])[:3]
    strongest_negative = sorted(bearish_tally, key=lambda k: -bearish_tally[k])[:3]

    # Emerging: bullish sectors with low bearish exposure
    emerging = [
        s for s in strongest_positive
        if bearish_tally.get(s, 0) == 0
    ][:2]

    # Fading: bearish sectors with low bullish counter
    fading = [
        s for s in strongest_negative
        if bullish_tally.get(s, 0) == 0
    ][:2]

    # Regime overlay
    geo_label = regime.get("geopolitical_stress_vs_easing", {}).get("label", "neutral")
    hfl_label = regime.get("higher_for_longer_vs_easing", {}).get("label", "neutral")
    risk_label = regime.get("risk_on_vs_risk_off", {}).get("label", "neutral")

    context_notes = []
    if geo_label == "geopolitical_stress":
        context_notes.append("Geopolitical stress reinforces defense/energy leadership")
    if hfl_label == "higher_for_longer":
        context_notes.append("Higher-for-longer rates suppress rate-sensitive sectors (REITs, long-duration)")
    if risk_label == "risk_on":
        context_notes.append("Risk-on environment supports cyclicals and growth names")
    elif risk_label == "risk_off":
        context_notes.append("Risk-off environment favors defensives, gold, utilities")

    return {
        "strongest_positive_sectors": [
            {"sector": s, "mentions": bullish_tally[s], "stocks": SECTOR_STOCKS.get(s, [])[:4]}
            for s in strongest_positive
        ],
        "strongest_negative_sectors": [
            {"sector": s, "mentions": bearish_tally[s], "stocks": SECTOR_STOCKS.get(s, [])[:4]}
            for s in strongest_negative
        ],
        "emerging_leadership": [
            {"sector": s, "stocks": SECTOR_STOCKS.get(s, [])[:5]}
            for s in emerging
        ],
        "fading_leadership": [
            {"sector": s, "stocks": SECTOR_STOCKS.get(s, [])[:5]}
            for s in fading
        ],
        "regime_context_notes": context_notes,
    }


def _build_watchlists(impacts: dict[str, ThemeImpact]) -> dict:
    """
    Build bullish / bearish / conditional stock watchlists from all cluster impacts.
    Deduplicates and annotates each stock with the themes driving it.
    """
    bullish: dict[str, dict] = {}   # ticker → {ticker, themes, sectors, priority}
    bearish: dict[str, dict] = {}
    conditional: dict[str, dict] = {}

    for tid, impact in impacts.items():
        theme_name = THEME_BY_ID[tid].theme_name if tid in THEME_BY_ID else tid

        for ticker in impact.bullish_stocks:
            if ticker not in bullish:
                bullish[ticker] = {"ticker": ticker, "themes": [], "sectors": [], "type": "bullish"}
            bullish[ticker]["themes"].append(theme_name)
            for sec in impact.bullish_sectors:
                if sec not in bullish[ticker]["sectors"]:
                    bullish[ticker]["sectors"].append(sec)

        for ticker in impact.bearish_stocks:
            if ticker not in bearish:
                bearish[ticker] = {"ticker": ticker, "themes": [], "sectors": [], "type": "bearish"}
            bearish[ticker]["themes"].append(theme_name)
            for sec in impact.bearish_sectors:
                if sec not in bearish[ticker]["sectors"]:
                    bearish[ticker]["sectors"].append(sec)

    # Stocks that appear in BOTH bullish and bearish lists are conditional
    overlapping = set(bullish) & set(bearish)
    for ticker in overlapping:
        conditional[ticker] = {
            "ticker": ticker,
            "bullish_themes": bullish[ticker]["themes"],
            "bearish_themes": bearish[ticker]["themes"],
            "type": "conditional",
            "note": "Appears in both bullish and bearish signals — direction depends on which theme dominates",
        }
        del bullish[ticker]
        del bearish[ticker]

    # Sort bullish/bearish by number of supporting themes (more = higher conviction)
    def _sort_key(d: dict) -> int:
        return -len(d.get("themes", []))

    return {
        "bullish_watchlist": sorted(bullish.values(), key=_sort_key),
        "bearish_watchlist": sorted(bearish.values(), key=_sort_key),
        "conditional_watchlist": list(conditional.values()),
        "watchlist_notes": [
            "Stocks driven by multiple independent themes carry higher conviction",
            "Conditional watchlist stocks require monitoring which macro theme dominates",
            "These are equity implication signals, not trading recommendations",
        ],
    }


def _odds_move_summary(shift_24h: float, shift_7d: float, direction: str) -> str:
    """Generate a one-liner odds move description."""
    s24 = f"{shift_24h:+.1f}pp"
    s7d = f"{shift_7d:+.1f}pp"
    dir_word = {
        "rising": "rising",
        "falling": "falling",
        "mixed": "mixed",
        "unstable": "conflicted",
    }.get(direction, direction)
    return f"Odds {dir_word} — 24h: {s24}, 7d: {s7d}"


def _why_it_matters(theme_id: str, direction: str, cluster: dict) -> str:
    """One-sentence 'why this matters for equities' for the signal card."""
    market_count = cluster.get("market_count", 0)
    confidence = cluster.get("confidence_score", 0)
    conf_word = "strong" if confidence >= 60 else "moderate" if confidence >= 35 else "tentative"

    templates = {
        "macro_rates_inflation": f"{conf_word.capitalize()} {direction} signal from {market_count} rate/inflation markets — directly reprices duration assets and financial sector multiples.",
        "geopolitics_war_trade": f"{conf_word.capitalize()} geopolitical {direction} signal across {market_count} markets — historically a catalyst for defense/energy positioning.",
        "energy_commodities": f"Energy/commodity odds {direction} across {market_count} markets — direct input cost and margin implications for airlines, industrials, and energy producers.",
        "us_politics_policy": f"Political {direction} signal from {market_count} US policy markets — sector-specific regulatory and fiscal implications.",
        "ai_semis_tech": f"AI/semis signal {direction} from {market_count} markets — semiconductor supply, export restrictions, and data center demand outlook shifting.",
        "crypto_risk_appetite": f"Crypto/risk markets are {direction} ({market_count} markets) — reliable risk-appetite barometer for positioning in high-beta equities.",
        "china_taiwan_supply_chain": f"China/Taiwan tensions {direction} across {market_count} markets — semiconductor supply chain risk premium and defense spending implications.",
        "defense_security": f"Defense/security odds {direction} in {market_count} markets — direct pipeline signal for defense prime contractors and cybersecurity vendors.",
        "consumer_labor_growth": f"Consumer/growth markets {direction} across {market_count} signals — indicates cyclical vs defensive rotation pressure.",
    }
    return templates.get(theme_id, f"{market_count} prediction markets are {direction} on this theme.")


def _narrative_with_qualifier(narrative: str, confidence: str, direction: str) -> str:
    """Prepend a confidence qualifier to the narrative text."""
    if not narrative:
        return ""
    qualifiers = {
        ("high", "rising"):   "Direct implication:",
        ("high", "falling"):  "Direct implication:",
        ("medium", "rising"):  "Conditional implication:",
        ("medium", "falling"): "Conditional implication:",
        ("low", "mixed"):     "Low-confidence implication:",
        ("low", "unstable"):  "Low-confidence implication:",
    }
    key = (confidence, direction)
    qualifier = qualifiers.get(key, "Conditional implication:")
    return f"{qualifier} {narrative}"


# Module-level singleton
investor_intel = InvestorIntel()
