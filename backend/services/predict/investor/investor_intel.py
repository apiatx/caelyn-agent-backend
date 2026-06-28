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
    get_theme_impact, SECTOR_STOCKS, THEME_BASKETS, ThemeImpact, THEME_IMPACTS,
)
from services.predict.investor.regime import compute_regime_scoreboard
from services.predict.investor.themes import THEME_BY_ID

logger = logging.getLogger("investor_intel")

_INVESTOR_CACHE_TTL = 150   # 2.5 min — aligned with scored cache TTL
_INTELLIGENCE_CACHE_TTL = 2100  # 35 min — pre-warmed by 30-min background loop


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

        result = {
            "generated_at": _now(),
            "watchlists": watchlists,
            "sector_reference": SECTOR_STOCKS,
        }
        cache.set(key, result, _INVESTOR_CACHE_TTL)
        return result

    async def get_intelligence(self) -> dict:
        """
        New normalized intelligence payload for the Predict page.

        Shape:
            updated_at          ISO timestamp
            cache_age_seconds   float — 0 on fresh build
            diagnostics         dict — coverage/build stats
            tracked_odds        list — 19 permanent macro/market families with live odds
            equity_signals      list — event-family-centric signals (Hormuz, Russia, Fed, etc.)
            raw_markets         list — reserved; empty in this version

        Backed by _INTELLIGENCE_CACHE_TTL (35 min) and pre-warmed every 30 min by
        _investor_intelligence_loop() in main.py.

        Existing /overview, /themes, /regime, /watchlists are UNCHANGED.
        """
        import time as _time

        key = "pm:investor:intelligence"
        cached = cache.get(key)
        if cached is not None:
            age = _time.time() - cached.get("_cached_at", _time.time())
            return {**cached, "cache_age_seconds": round(age, 1)}

        t0 = _time.time()

        # ── Parallel fetch ─────────────────────────────────────────────────────
        # all_markets (unfiltered, 300 markets) → tracked_odds
        # scored (sports-filtered, 200 markets) → equity_signals
        all_markets_coro = polymarket_intel.get_top_markets(limit=300)
        scored_coro = self._get_scored()
        results = await asyncio.gather(all_markets_coro, scored_coro, return_exceptions=True)

        all_markets: list[dict] = results[0] if not isinstance(results[0], Exception) else []
        scored: list[dict] = results[1] if not isinstance(results[1], Exception) else []

        # ── Watchlist symbols (sync disk read) ─────────────────────────────────
        watchlist_syms = _get_watchlist_symbols()

        # ── Tracked odds (scanner-backed, falls back to pure-function stub) ──
        # odds_scanner.scan_and_persist() is pre-warmed every 30 min by
        # _odds_scanner_loop() in main.py and carries full 7-day DB history.
        # Fall back to build_tracked_odds() on cold start (scanner not yet run).
        try:
            from services.predict.odds_scanner import odds_scanner as _odds_scanner
            _scanner_payload = _odds_scanner.get_live_payload()
            if _scanner_payload is not None:
                tracked_odds = _scanner_payload.get("odds", [])
            else:
                from services.predict.investor.tracked_odds import build_tracked_odds
                tracked_odds = build_tracked_odds(all_markets)
        except Exception as _oe:
            logger.warning("odds_scanner unavailable, falling back: %s", _oe)
            from services.predict.investor.tracked_odds import build_tracked_odds
            tracked_odds = build_tracked_odds(all_markets)

        # ── Equity signals — event-family-centric ──────────────────────────────
        classified = classify_markets(scored)
        equity_relevant = filter_equity_relevant(classified)

        # Build canonical ticker map once here — passed into _build_equity_signals
        # so it is constructed once per intelligence build, not per signal.
        canonical_ticker_map = _build_canonical_ticker_map()

        equity_signals, build_diag = _build_equity_signals(
            equity_relevant, watchlist_syms, canonical_ticker_map
        )

        # ── Diagnostics ────────────────────────────────────────────────────────
        from services.predict.investor.event_grouping import make_event_family_key
        family_keys_seen = {
            make_event_family_key(m.get("question", ""))
            for m in equity_relevant
        }

        theme_universe_theme_count = 0
        theme_universe_info = "unavailable"
        try:
            from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _uni
            theme_universe_theme_count = len(_uni)
            theme_universe_info = f"ENRICHED_THEME_RS_UNIVERSE ({theme_universe_theme_count} themes)"
        except Exception:
            pass

        diagnostics = {
            "ticker_impact_source":          "watchlist+canonical_theme_universe",
            "hardcoded_sector_stocks_used":  False,
            "raw_markets_seen":              len(all_markets),
            "equity_relevant_markets":       len(equity_relevant),
            "grouped_event_families":        len(family_keys_seen),
            "equity_signals_returned":       len(equity_signals),
            "duplicate_markets_collapsed":   max(0, len(equity_relevant) - len(family_keys_seen)),
            "watchlist_symbols_count":       len(watchlist_syms),
            "watchlist_ticker_hits":         build_diag["watchlist_ticker_hits"],
            "canonical_theme_fallback_hits": build_diag["canonical_theme_fallback_hits"],
            "unmapped_theme_impacts":        build_diag["unmapped_theme_impacts"],
            "theme_universe_theme_count":    theme_universe_theme_count,
            "tracked_odds_families":         len(tracked_odds),
            "tracked_odds_with_data":        sum(
                1 for t in tracked_odds if t.get("yes_probability") is not None
            ),
            "theme_universe_source":         theme_universe_info,
            "build_time_ms":                 round((_time.time() - t0) * 1000, 1),
        }

        now_ts = _time.time()
        result = {
            "updated_at":       _now(),
            "_cached_at":       now_ts,
            "cache_age_seconds": 0,
            "diagnostics":      diagnostics,
            "tracked_odds":     tracked_odds,
            "equity_signals":   equity_signals,
            "raw_markets":      [],
        }
        cache.set(key, result, _INTELLIGENCE_CACHE_TTL)
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


def _sector_mapping_for_driver(dm: dict, theme_id: str) -> tuple[list[str], list[str], list[str], list[str]]:
    """
    Return (bullish_sectors, bearish_sectors, bullish_tickers, bearish_tickers)
    for a single driver market, polarity-adjusted.

      polarity "direct"   → theme's "rising"  impact
      polarity "inverted" → theme's "falling" impact (de-escalation, rate-cut, etc.)
    """
    polarity = dm.get("polarity", "direct")
    lookup_dir = "falling" if polarity == "inverted" else "rising"
    imp = THEME_IMPACTS.get((theme_id, lookup_dir))
    if imp is None:
        return [], [], [], []
    return (
        imp.bullish_sectors[:3],
        imp.bearish_sectors[:3],
        imp.bullish_stocks[:5],
        imp.bearish_stocks[:5],
    )


def _signal_quality(conf_score: float, has_conflict: bool, has_mixed: bool) -> tuple[str, str]:
    """
    Return (signal_quality_label, signal_quality_explanation) for a signal card.

    Labels are designed for non-technical users; they do not imply trade certainty.
    """
    if has_conflict or has_mixed:
        return (
            "Mixed",
            "Markets in this group point to different equity regimes, "
            "so this is not one clean sector signal.",
        )
    if conf_score >= 60:
        return (
            "High",
            "Strong data agreement and clear equity-impact direction. "
            "This is not trade certainty.",
        )
    if conf_score >= 35:
        return (
            "Moderate",
            "Some data agreement, but the equity read is less clean.",
        )
    return (
        "Low",
        "Limited market data or weak agreement — treat as an early-stage signal only.",
    )


def _user_warning(
    has_conflict: bool,
    has_mixed: bool,
    primary_driver: dict,
    theme_id: str,
) -> Optional[str]:
    """
    Return a plain-English user_warning string, or None if the signal is clean.

    Avoids backend jargon like "direct/inverted", "semantic labels", etc.
    """
    if not has_conflict and not has_mixed:
        return None

    pdm_event   = (primary_driver or {}).get("semantic_event_type", "general")
    pdm_polarity = (primary_driver or {}).get("polarity", "direct")

    # Specific plain-English read for de-escalation inside geopolitics theme
    if (
        theme_id == "geopolitics_war_trade"
        and pdm_polarity == "inverted"
        and pdm_event in ("peace_deal", "nuclear_deal", "de_escalation")
    ):
        return (
            "De-escalation odds are rising. That usually weakens war-premium trades "
            "like defense/energy and can support risk-on areas like airlines/consumer."
        )

    # Generic polarity-conflict warning
    if has_conflict:
        return (
            "Mixed drivers: some markets imply escalation/risk-off, while others "
            "imply de-escalation/risk-on. Treat the sector call as mixed, "
            "not a clean trade signal."
        )

    # Mixed semantics without polarity conflict
    return (
        "This theme groups markets with different economic event types. "
        "Headline sector impact reflects the strongest signal only."
    )


def _build_top_equity_signals(
    clusters: list[dict],
    impacts: dict[str, ThemeImpact],
    max_signals: int = 7,
) -> list[dict]:
    """
    Build the top N equity signals for the Investor tab hero section.

    Rank clusters by:
      confidence_score * regime_signal_strength * market_count

    Each signal includes both legacy fields (unchanged) and new user-facing fields:
      signal_quality_label/explanation  — plain-English quality tier
      display_impact_mode               — "cluster" | "mixed"
      headline_*                        — safe headline sectors/tickers for frontend
      primary_driver_market             — highest-contribution market with full attribution
      driver_markets                    — all contributing markets with per-market sectors
      confidence_explanation            — plain-English confidence definition
      signal_integrity                  — conflict flags + backend warning + user_warning
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
        theme_def = THEME_BY_ID.get(tid)

        direction = cl.get("summary_direction", "mixed")
        shift_24h = cl.get("weighted_odds_shift_24h", 0)
        shift_7d  = cl.get("weighted_odds_shift_7d", 0)

        # ── Polarity / semantic conflict ───────────────────────────────────
        has_conflict = cl.get("has_polarity_conflict", False)
        has_mixed    = cl.get("has_mixed_semantics",  False)
        is_conflicted = has_conflict or has_mixed

        # When conflicted, fall back to "mixed" impact so we never present a
        # clean directional sector call that may be backwards for half the markets.
        effective_direction = "mixed" if is_conflicted else direction
        impact = get_theme_impact(tid, effective_direction)

        odds_summary = _odds_move_summary(shift_24h, shift_7d, direction)

        if impact:
            bullish_stocks  = impact.bullish_stocks[:5]
            bearish_stocks  = impact.bearish_stocks[:5]
            bullish_sectors = impact.bullish_sectors
            bearish_sectors = impact.bearish_sectors
            regime_impact   = impact.regime_implications
            narrative       = impact.narrative
        else:
            bullish_stocks  = []
            bearish_stocks  = []
            bullish_sectors = theme_def.bullish_bias_sectors if theme_def else []
            bearish_sectors = theme_def.bearish_bias_sectors if theme_def else []
            regime_impact   = []
            narrative       = ""

        conf_score = cl.get("confidence_score", 0)
        confidence_label = (
            "high"   if conf_score >= 60 else
            "medium" if conf_score >= 35 else
            "low"
        )

        why_it_matters = _why_it_matters(tid, direction, cl)

        # ── Primary driver market (polarity-adjusted sectors + tickers) ────
        raw_primary = cl.get("primary_driver_market") or {}
        pdm_bull_s, pdm_bear_s, pdm_bull_t, pdm_bear_t = _sector_mapping_for_driver(raw_primary, tid)
        primary_driver_market = {
            **raw_primary,
            "mapped_bullish_sectors": pdm_bull_s,
            "mapped_bearish_sectors": pdm_bear_s,
            "mapped_bullish_tickers": pdm_bull_t,
            "mapped_bearish_tickers": pdm_bear_t,
        } if raw_primary else None

        # ── All driver markets with per-market sector + ticker mappings ────
        driver_markets: list[dict] = []
        for dm in cl.get("supporting_markets", []):
            b_s, r_s, b_t, r_t = _sector_mapping_for_driver(dm, tid)
            driver_markets.append({
                **dm,
                "mapped_bullish_sectors": b_s,
                "mapped_bearish_sectors": r_s,
                "mapped_bullish_tickers": b_t,
                "mapped_bearish_tickers": r_t,
            })

        # ── User-facing quality fields ─────────────────────────────────────
        quality_label, quality_explanation = _signal_quality(conf_score, has_conflict, has_mixed)

        # ── Display impact mode ────────────────────────────────────────────
        # "mixed"   → frontend should NOT render a clean bullish/bearish sector box
        # "cluster" → cluster-level sectors are safe to headline
        display_impact_mode = "mixed" if is_conflicted else "cluster"

        # ── Headline impact fields ─────────────────────────────────────────
        # For mixed/conflicted signals: headline uses primary driver's polarity-adjusted
        # sectors/tickers so we never show Defense/Energy as bullish for a peace-deal driver.
        # For clean signals: headline uses cluster-level sectors/tickers.
        if is_conflicted and primary_driver_market:
            headline_bullish_sectors = pdm_bull_s
            headline_bearish_sectors = pdm_bear_s
            headline_bullish_tickers = pdm_bull_t
            headline_bearish_tickers = pdm_bear_t
            headline_impact_note = (
                "Headline impact reflects the primary driver only; "
                "cluster contains conflicting markets."
            )
        else:
            headline_bullish_sectors = bullish_sectors
            headline_bearish_sectors = bearish_sectors
            headline_bullish_tickers = bullish_stocks
            headline_bearish_tickers = bearish_stocks
            headline_impact_note = None

        # ── Signal integrity (diagnostic + user-facing warning) ────────────
        signal_integrity = {
            "has_mixed_semantics":   has_mixed,
            "has_polarity_conflict": has_conflict,
            "warning":               cl.get("signal_integrity_warning"),   # backend detail
            "user_warning":          _user_warning(has_conflict, has_mixed, raw_primary, tid),
        }

        signals.append({
            # ── existing fields (unchanged keys) ──────────────────────────
            "theme_id":        tid,
            "title":           f"{cl.get('theme_emoji', '')} {cl['theme_name']}",
            "summary":         cl.get("description", ""),
            "why_it_matters":  why_it_matters,
            "supporting_markets": cl.get("supporting_markets", [])[:4],
            "market_count":    cl.get("market_count", 0),
            "odds_move_summary": odds_summary,
            "summary_direction": direction,
            "bullish_sectors": bullish_sectors,
            "bearish_sectors": bearish_sectors,
            "bullish_stocks":  bullish_stocks,
            "bearish_stocks":  bearish_stocks,
            "asset_baskets":   THEME_BASKETS.get(tid, []),
            "regime_impact":   regime_impact,
            "confidence":      confidence_label,
            "confidence_score": conf_score,
            "narrative":       _narrative_with_qualifier(narrative, confidence_label, effective_direction),
            "watchlist_priority": "high" if conf_score >= 60 else "medium" if conf_score >= 35 else "watch",
            # ── user-facing quality fields (new) ─────────────────────────
            "signal_quality_label":       quality_label,
            "signal_quality_explanation": quality_explanation,
            # ── display mode + headline fields (new) ──────────────────────
            "display_impact_mode":     display_impact_mode,
            "headline_bullish_sectors": headline_bullish_sectors,
            "headline_bearish_sectors": headline_bearish_sectors,
            "headline_bullish_tickers": headline_bullish_tickers,
            "headline_bearish_tickers": headline_bearish_tickers,
            "headline_impact_note":     headline_impact_note,
            # ── diagnostic fields (preserved + extended) ──────────────────
            "primary_driver_market": primary_driver_market,
            "driver_markets":        driver_markets,
            "confidence_explanation": (
                "Confidence reflects how many markets agree directionally, "
                "how equity-relevant they are, and how consistently they move. "
                "It does not reflect trade certainty or signal precision."
            ),
            "signal_integrity": signal_integrity,
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


# ── Canonical ticker resolution for /intelligence endpoint ────────────────────
#
# impact_engine.ThemeImpact uses human-readable sector labels (e.g. "Defense/Aerospace")
# that have no 1:1 match with canonical theme IDs (e.g. "defense").
# This static adapter is the ONLY bridge — no second taxonomy is introduced.
_SECTOR_LABEL_TO_THEME_IDS: dict[str, list[str]] = {
    "Energy":                       ["energy", "oil_gas", "oil_services", "lng_gas"],
    "Defense/Aerospace":            ["defense", "drones"],
    "Airlines/Transport":           ["travel_transportation"],
    "Industrials":                  ["industrials"],
    "Financials":                   ["financials", "banks", "fintech"],
    "Regional Banks":               ["regional_banks"],
    "Small Caps":                   [],
    "Semiconductors":               ["semiconductors", "semicap_equipment", "memory_storage"],
    "Semiconductors (US domestic)": ["semiconductors"],
    "Software/Growth Tech":         ["software", "cloud_software"],
    "Long-Duration Growth Tech":    ["technology", "software", "cloud_software"],
    "Legacy Tech":                  ["technology"],
    "Cybersecurity":                ["cybersecurity"],
    "AI Infra/Data Centers":        ["datacenter_infra", "ai_networking", "power_cooling"],
    "REITs/Housing":                ["real_estate", "homebuilders"],
    "Consumer Discretionary":       ["consumer_discretionary", "consumer_retail"],
    "Consumer Electronics":         ["consumer_discretionary"],
    "Consumer Hardware":            ["consumer_discretionary"],
    "Utilities/Nuclear Power":      ["utilities", "uranium_nuclear"],
    "Gold/Metals/Commodities":      ["gold", "silver", "metals_mining", "copper_miners", "rare_earth"],
    "Crypto Proxies":               ["crypto_equities"],
    "Clean Energy":                 ["clean_energy", "solar"],
}


def _get_watchlist_symbols() -> set[str]:
    """
    Read the user's watchlist ticker symbols from the JSON store (sync, disk).
    Returns empty set on any error so callers degrade to canonical fallback gracefully.
    """
    try:
        from services.watchlist_service import _read_store, extract_tickers
        store = _read_store()
        csv_data = store.get("csv_data", []) or []
        analysis = store.get("analysis")
        tickers = extract_tickers(csv_data, analysis)
        return set(tickers)
    except Exception as exc:
        logger.debug(f"[InvestorIntel] Watchlist load skipped: {exc}")
        return set()


def _build_canonical_ticker_map() -> dict[str, list[str]]:
    """
    Build reverse map: ticker → [theme_ids it appears in].

    Reads ENRICHED_THEME_RS_UNIVERSE at call time (not at import) so it always
    reflects the current canonical universe, including any post-startup enrichments.
    Both proxy_symbols and candidate_symbols are included.
    No SECTOR_STOCKS used at any stage.
    """
    try:
        from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE
        result: dict[str, list[str]] = {}
        for tid, meta in ENRICHED_THEME_RS_UNIVERSE.items():
            for sym in list(meta.get("proxy_symbols", [])) + list(meta.get("candidate_symbols", [])):
                if sym:
                    result.setdefault(sym, []).append(tid)
        return result
    except Exception as exc:
        logger.warning(f"[InvestorIntel] Canonical ticker map build failed: {exc}")
        return {}


def _get_canonical_tickers_for_themes(theme_ids: list[str]) -> list[str]:
    """
    Return deduped tickers (proxy_symbols then candidate_symbols) for the given
    theme IDs from ENRICHED_THEME_RS_UNIVERSE.  No SECTOR_STOCKS fallback.
    """
    try:
        from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE
        seen: set[str] = set()
        out: list[str] = []
        for tid in theme_ids:
            meta = ENRICHED_THEME_RS_UNIVERSE.get(tid, {})
            for sym in list(meta.get("proxy_symbols", [])) + list(meta.get("candidate_symbols", [])):
                if sym and sym not in seen:
                    seen.add(sym)
                    out.append(sym)
        return out
    except Exception:
        return []


def _resolve_ticker_impacts(
    bullish_sectors: list[str],
    bearish_sectors: list[str],
    watchlist_syms: set[str],
    canonical_ticker_map: dict[str, list[str]],
) -> tuple[dict, int, int, int]:
    """
    Resolve ticker lists for a signal — canonical universe only, no SECTOR_STOCKS.

    Resolution order:
    1. Watchlist symbols whose canonical theme memberships intersect impacted themes
    2. Canonical ENRICHED_THEME_RS_UNIVERSE proxy/candidate tickers as fallback
    3. If no match at either stage, empty lists — no hardcoded catalogs used

    Returns:
        (ticker_impacts_dict, watchlist_hits, canonical_fallback_hits, unmapped_label_count)
    """
    bull_theme_ids: list[str] = []
    bear_theme_ids: list[str] = []
    unmapped = 0
    for sec in bullish_sectors:
        tids = _SECTOR_LABEL_TO_THEME_IDS.get(sec, None)
        if tids is None:
            if sec:
                unmapped += 1
        else:
            bull_theme_ids.extend(tids)
    for sec in bearish_sectors:
        tids = _SECTOR_LABEL_TO_THEME_IDS.get(sec, None)
        if tids is None:
            if sec:
                unmapped += 1
        else:
            bear_theme_ids.extend(tids)

    bull_theme_set = set(bull_theme_ids)
    bear_theme_set = set(bear_theme_ids)

    wl_bull = sorted(
        t for t in watchlist_syms
        if any(tid in bull_theme_set for tid in canonical_ticker_map.get(t, []))
    )
    wl_bear = sorted(
        t for t in watchlist_syms
        if any(tid in bear_theme_set for tid in canonical_ticker_map.get(t, []))
    )

    wl_cond = sorted(set(wl_bull) & set(wl_bear))
    cond_set = set(wl_cond)
    wl_bull = [t for t in wl_bull if t not in cond_set]
    wl_bear = [t for t in wl_bear if t not in cond_set]

    seen_wl = set(wl_bull) | set(wl_bear) | cond_set
    watchlist_hits = len(seen_wl)

    fb_bull = [t for t in _get_canonical_tickers_for_themes(bull_theme_ids) if t not in seen_wl][:8]
    fb_bear = [t for t in _get_canonical_tickers_for_themes(bear_theme_ids) if t not in seen_wl][:8]
    canonical_hits = len(fb_bull) + len(fb_bear)

    return {
        "bullish_watchlist":     wl_bull[:6],
        "bearish_watchlist":     wl_bear[:6],
        "conditional_watchlist": wl_cond[:4],
        "bullish_fallback":      fb_bull,
        "bearish_fallback":      fb_bear,
    }, watchlist_hits, canonical_hits, unmapped


# ── Event-family-centric equity signal builder ────────────────────────────────

def _build_equity_signals(
    equity_relevant: list[dict],
    watchlist_syms: set[str],
    canonical_ticker_map: dict[str, list[str]],
    max_signals: int = 14,
) -> tuple[list[dict], dict]:
    """
    Build equity_signals in the new event-family-centric shape.

    Unlike _build_top_equity_signals (theme-cluster-centric), this function
    groups raw markets by event_family_key first, then derives theme impacts.
    Multiple "Hormuz by Jul 15" / "Hormuz by Jul 31" markets collapse into
    ONE grouped signal instead of contributing to a conflated theme cluster.

    Ticker resolution uses ENRICHED_THEME_RS_UNIVERSE exclusively via
    canonical_ticker_map (pre-built by caller).  No SECTOR_STOCKS used.

    Returns:
        (signals list sorted by total_volume_24h desc, diag dict with hit counts)
    """
    from services.predict.investor.event_grouping import (
        make_event_family_key,
        get_family_label,
        get_family_primary_category,
    )

    # ── Group by event family ──────────────────────────────────────────────────
    family_groups: dict[str, list[dict]] = {}
    for m in equity_relevant:
        fk = make_event_family_key(m.get("question", ""))
        family_groups.setdefault(fk, []).append(m)

    signals: list[dict] = []
    _diag_wl_hits  = 0
    _diag_fb_hits  = 0
    _diag_unmapped = 0

    for family_key, markets in family_groups.items():
        if not markets:
            continue

        # Sort by volume within family
        markets.sort(key=lambda m: m.get("volume_24h", 0) or 0, reverse=True)

        # ── Primary theme: most-voted theme_id across markets ───────────────
        theme_votes: dict[str, float] = {}
        for m in markets:
            for te in m.get("themes", []):
                tid = te.get("theme_id", "")
                theme_votes[tid] = theme_votes.get(tid, 0) + float(te.get("match_score", 1))
        primary_theme_id = (
            max(theme_votes, key=theme_votes.get)
            if theme_votes else "geopolitics_war_trade"
        )

        # ── Volume-weighted aggregated stats ────────────────────────────────
        vols = [max(0.0, m.get("volume_24h", 0) or 0) for m in markets]
        total_vol = sum(vols) or 1.0
        weights = [v / total_vol for v in vols]

        def _wavg(field: str) -> float:
            return sum((m.get(field) or 0) * w for m, w in zip(markets, weights))

        agg_yes_pct    = _wavg("yes_pct")
        agg_delta_24h  = _wavg("price_change_1d")
        agg_delta_7d   = _wavg("price_change_1wk")

        # ── Direction ───────────────────────────────────────────────────────
        # Check for intra-family polarity conflict (escalation vs de-escalation)
        strong_up   = sum(1 for m in markets if (m.get("price_change_1d") or 0) >  3)
        strong_down = sum(1 for m in markets if (m.get("price_change_1d") or 0) < -3)
        has_conflict = strong_up > 0 and strong_down > 0 and len(markets) >= 2

        if has_conflict:
            direction = "mixed"
        elif agg_delta_24h > 0.5:
            direction = "rising"
        elif agg_delta_24h < -0.5:
            direction = "falling"
        else:
            direction = "mixed"

        # ── Theme impact ────────────────────────────────────────────────────
        effective_dir = "mixed" if has_conflict else direction
        impact = get_theme_impact(primary_theme_id, effective_dir)

        if impact:
            bullish_sectors = impact.bullish_sectors
            bearish_sectors = impact.bearish_sectors
            narrative = impact.narrative
        else:
            td = THEME_BY_ID.get(primary_theme_id)
            bullish_sectors = td.bullish_bias_sectors if td else []
            bearish_sectors = td.bearish_bias_sectors if td else []
            narrative = ""

        # ── Canonical ticker resolution — no SECTOR_STOCKS ─────────────────
        ticker_impacts, wl_hits, fb_hits, umap = _resolve_ticker_impacts(
            bullish_sectors, bearish_sectors, watchlist_syms, canonical_ticker_map
        )
        _diag_wl_hits    += wl_hits
        _diag_fb_hits    += fb_hits
        _diag_unmapped   += umap

        # ── Theme impacts list ──────────────────────────────────────────────
        theme_impacts_list: list[dict] = []
        for sec in bullish_sectors[:4]:
            theme_impacts_list.append({
                "sector":    sec,
                "theme":     primary_theme_id.replace("_", " ").title(),
                "sub_theme": None,
                "direction": "bullish",
                "confidence": "moderate",
                "rationale": narrative,
            })
        for sec in bearish_sectors[:4]:
            theme_impacts_list.append({
                "sector":    sec,
                "theme":     primary_theme_id.replace("_", " ").title(),
                "sub_theme": None,
                "direction": "bearish",
                "confidence": "moderate",
                "rationale": narrative,
            })

        # ── Conflicts ───────────────────────────────────────────────────────
        conflicts: list[dict] = []
        if has_conflict:
            up_qs   = [m.get("question", "") for m in markets if (m.get("price_change_1d") or 0) >  3][:2]
            down_qs = [m.get("question", "") for m in markets if (m.get("price_change_1d") or 0) < -3][:2]
            conflicts.append({
                "type":        "opposing_24h_moves",
                "description": (
                    "Markets in this group show opposing 24h moves — "
                    "some rising while others are falling. Direction is ambiguous."
                ),
                "rising_markets":  up_qs,
                "falling_markets": down_qs,
            })

        # ── Signal quality ──────────────────────────────────────────────────
        max_relevance = max(
            (m.get("equity_relevance_score") or 0 for m in markets), default=0
        )
        if has_conflict:
            signal_quality = "low"
        elif max_relevance >= 60:
            signal_quality = "high"
        elif max_relevance >= 35:
            signal_quality = "moderate"
        else:
            signal_quality = "low"

        # ── Why it matters ──────────────────────────────────────────────────
        why = _why_it_matters(primary_theme_id, effective_dir, {
            "market_count":   len(markets),
            "confidence_score": max_relevance,
        })

        # ── Driver markets ──────────────────────────────────────────────────
        driver_markets = [
            {
                "question":    m.get("question", ""),
                "yes_pct":     m.get("yes_pct"),
                "delta_24h_pp": m.get("price_change_1d"),
                "delta_7d_pp": m.get("price_change_1wk"),
                "volume_24h":  m.get("volume_24h", 0),
                "condition_id": m.get("condition_id", ""),
                "slug":         m.get("slug", ""),
            }
            for m in markets[:6]
        ]

        signals.append({
            "event_family_key":  family_key,
            "title":             get_family_label(family_key),
            "primary_category":  get_family_primary_category(family_key, primary_theme_id),
            "primary_theme_id":  primary_theme_id,
            "yes_probability":   round(agg_yes_pct / 100.0, 4),
            "delta_24h_pp":      round(agg_delta_24h, 2),
            "delta_7d_pp":       round(agg_delta_7d, 2),
            "direction":         direction,
            "signal_quality":    signal_quality,
            "why_it_matters":    why,
            "driver_markets":    driver_markets,
            "theme_impacts":     theme_impacts_list,
            "ticker_impacts":    ticker_impacts,
            "conflicts":         conflicts,
            "market_count":      len(markets),
            "total_volume_24h":  round(total_vol, 0),
            "source_links":      [],
        })

    # Sort by total volume (most liquid events first)
    signals.sort(key=lambda s: s["total_volume_24h"], reverse=True)
    diag = {
        "watchlist_ticker_hits":       _diag_wl_hits,
        "canonical_theme_fallback_hits": _diag_fb_hits,
        "unmapped_theme_impacts":      _diag_unmapped,
    }
    return signals[:max_signals], diag


# Module-level singleton
investor_intel = InvestorIntel()
