"""
Investor Mode — Regime Scoreboard.

Derives 7 high-level macro regime indicators from the theme cluster layer.
Each indicator is computed deterministically from cluster signals —
no vibes, no LLM, purely the aggregated odds-shift and confidence data.

Indicators:
    1. risk_on_vs_risk_off
    2. inflationary_vs_disinflationary
    3. growth_vs_slowdown
    4. geopolitical_stress_vs_easing
    5. higher_for_longer_vs_easing
    6. commodity_pressure_vs_relief
    7. ai_capex_supportive_vs_restrictive

Score: 0-100 (50 = neutral)
  > 60 → first label (e.g. "risk_on")
  < 40 → second label (e.g. "risk_off")
  40-60 → "neutral"

Direction: "rising" | "falling" | "neutral"
  reflects whether the score is moving (based on 24h vs 7d shift)
"""

from __future__ import annotations

from typing import Optional


def _cluster_by_id(clusters: list[dict]) -> dict[str, dict]:
    return {c["theme_id"]: c for c in clusters}


def _shift_score(weighted_shift_24h: float, scale: float = 5.0) -> float:
    """
    Convert a weighted 24h odds shift (pp) to a 0-100 contribution.
    scale: number of pp that maps to the extreme ends.
    """
    clamped = max(-scale, min(scale, weighted_shift_24h))
    return 50.0 + (clamped / scale) * 50.0


def _combine(*scores: tuple[float, float]) -> float:
    """
    Weighted combine of (score, weight) pairs.
    Returns combined score 0-100.
    """
    total_w = sum(w for _, w in scores)
    if total_w == 0:
        return 50.0
    combined = sum(s * w for s, w in scores) / total_w
    return round(max(0.0, min(100.0, combined)), 1)


def _direction_from_score(score: float, prev_score: Optional[float] = None) -> str:
    if score > 60:
        return "rising"
    elif score < 40:
        return "falling"
    return "neutral"


def _confidence(cluster: Optional[dict], min_markets: int = 2) -> str:
    if not cluster:
        return "none"
    mc = cluster.get("market_count", 0)
    conf = cluster.get("confidence_score", 0)
    if mc < min_markets or conf < 20:
        return "low"
    if conf >= 60:
        return "high"
    return "medium"


def _supporting_themes(cluster_ids: list[str], clusters_by_id: dict[str, dict]) -> list[str]:
    return [
        clusters_by_id[cid]["theme_name"]
        for cid in cluster_ids
        if cid in clusters_by_id
    ]


def compute_regime_scoreboard(clusters: list[dict]) -> dict:
    """
    Compute the regime scoreboard from theme clusters.

    Returns a dict of 7 regime indicators.
    """
    cb = _cluster_by_id(clusters)

    def cl(tid: str) -> dict:
        return cb.get(tid, {})

    def shift(tid: str, field: str = "weighted_odds_shift_24h") -> float:
        return cl(tid).get(field, 0) or 0.0

    def conf(tid: str) -> float:
        return cl(tid).get("confidence_score", 0) or 0.0

    def mkt_w(tid: str) -> float:
        """Weight proportional to market_count, capped."""
        return min(10.0, cl(tid).get("market_count", 0) or 0)

    # ── 1. Risk-On vs Risk-Off ─────────────────────────────────────────────
    # Rising crypto, consumer growth → risk-on
    # Rising geopolitics, energy prices, gold → risk-off
    crypto_s = _shift_score(shift("crypto_risk_appetite"))
    growth_s = _shift_score(shift("consumer_labor_growth"))
    geo_s = 100.0 - _shift_score(shift("geopolitics_war_trade"))   # rising geo = risk-off
    energy_s = 100.0 - _shift_score(shift("energy_commodities"))   # rising energy = risk-off

    risk_on_score = _combine(
        (crypto_s, mkt_w("crypto_risk_appetite")),
        (growth_s, mkt_w("consumer_labor_growth") * 1.5),
        (geo_s, mkt_w("geopolitics_war_trade")),
        (energy_s, mkt_w("energy_commodities") * 0.5),
    )

    # ── 2. Inflationary vs Disinflationary ────────────────────────────────
    # Rising inflation/rates, energy, commodities → inflationary
    macro_s = _shift_score(shift("macro_rates_inflation"))
    energy_inf_s = _shift_score(shift("energy_commodities"))

    inflationary_score = _combine(
        (macro_s, mkt_w("macro_rates_inflation") * 2.0),
        (energy_inf_s, mkt_w("energy_commodities")),
    )

    # ── 3. Growth vs Slowdown ─────────────────────────────────────────────
    # Rising consumer/labor growth → growth regime
    # Rising recession odds → slowdown (inverted)
    consumer_s = _shift_score(shift("consumer_labor_growth"))
    # Macro rising (i.e. more inflation/recession coverage) slightly negative for growth
    macro_drag = 100.0 - _shift_score(shift("macro_rates_inflation")) * 0.3

    growth_score = _combine(
        (consumer_s, mkt_w("consumer_labor_growth") * 2.5),
        (macro_drag, mkt_w("macro_rates_inflation") * 0.5),
    )

    # ── 4. Geopolitical Stress vs Easing ─────────────────────────────────
    # Rising geopolitics, defense, china/taiwan → stress
    geo_stress_s = _shift_score(shift("geopolitics_war_trade"))
    defense_s = _shift_score(shift("defense_security"))
    china_s = _shift_score(shift("china_taiwan_supply_chain"))

    geo_stress_score = _combine(
        (geo_stress_s, mkt_w("geopolitics_war_trade") * 2.0),
        (defense_s, mkt_w("defense_security")),
        (china_s, mkt_w("china_taiwan_supply_chain") * 1.5),
    )

    # ── 5. Higher-for-Longer vs Easing ───────────────────────────────────
    # Rising macro/rates/inflation → higher-for-longer
    rates_s = _shift_score(shift("macro_rates_inflation"))
    hfl_score = _combine(
        (rates_s, mkt_w("macro_rates_inflation") * 3.0),
        (energy_inf_s, mkt_w("energy_commodities") * 0.5),
    )

    # ── 6. Commodity Pressure vs Relief ──────────────────────────────────
    # Rising energy/commodities → pressure
    commodity_s = _shift_score(shift("energy_commodities"))
    geo_commodity_s = _shift_score(shift("geopolitics_war_trade")) * 0.4 + 30

    commodity_score = _combine(
        (commodity_s, mkt_w("energy_commodities") * 2.5),
        (geo_commodity_s, mkt_w("geopolitics_war_trade") * 0.5),
    )

    # ── 7. AI Capex Supportive vs Restrictive ────────────────────────────
    # Rising AI/semis → capex supportive
    # Rising China/Taiwan/export controls → restrictive
    ai_s = _shift_score(shift("ai_semis_tech"))
    china_restrict_s = 100.0 - _shift_score(shift("china_taiwan_supply_chain"))

    ai_score = _combine(
        (ai_s, mkt_w("ai_semis_tech") * 2.0),
        (china_restrict_s, mkt_w("china_taiwan_supply_chain")),
    )

    def _label_pair(score: float, high_label: str, low_label: str) -> str:
        if score > 62:
            return high_label
        elif score < 38:
            return low_label
        return "neutral"

    def _dir(score: float) -> str:
        if score > 62:
            return "rising"
        elif score < 38:
            return "falling"
        return "neutral"

    def _conf_label(score: float, clusters_ids: list[str]) -> str:
        total_mkt = sum(cl(tid).get("market_count", 0) or 0 for tid in clusters_ids)
        if total_mkt < 3:
            return "low"
        if abs(score - 50) > 20:
            return "high"
        if abs(score - 50) > 10:
            return "medium"
        return "low"

    regime = {
        "risk_on_vs_risk_off": {
            "label": _label_pair(risk_on_score, "risk_on", "risk_off"),
            "score": risk_on_score,
            "direction": _dir(risk_on_score),
            "confidence": _conf_label(risk_on_score, ["crypto_risk_appetite", "consumer_labor_growth", "geopolitics_war_trade"]),
            "supporting_themes": _supporting_themes(
                ["crypto_risk_appetite", "consumer_labor_growth", "geopolitics_war_trade", "energy_commodities"],
                cb,
            ),
            "description": "Measures overall market risk appetite derived from crypto, consumer growth, and geopolitical odds.",
        },
        "inflationary_vs_disinflationary": {
            "label": _label_pair(inflationary_score, "inflationary", "disinflationary"),
            "score": inflationary_score,
            "direction": _dir(inflationary_score),
            "confidence": _conf_label(inflationary_score, ["macro_rates_inflation", "energy_commodities"]),
            "supporting_themes": _supporting_themes(
                ["macro_rates_inflation", "energy_commodities"], cb
            ),
            "description": "Macro inflation regime derived from rates markets and energy/commodity odds shifts.",
        },
        "growth_vs_slowdown": {
            "label": _label_pair(growth_score, "growth", "slowdown"),
            "score": growth_score,
            "direction": _dir(growth_score),
            "confidence": _conf_label(growth_score, ["consumer_labor_growth", "macro_rates_inflation"]),
            "supporting_themes": _supporting_themes(
                ["consumer_labor_growth", "macro_rates_inflation"], cb
            ),
            "description": "Economic growth regime from consumer, labor, and GDP-related prediction markets.",
        },
        "geopolitical_stress_vs_easing": {
            "label": _label_pair(geo_stress_score, "geopolitical_stress", "easing"),
            "score": geo_stress_score,
            "direction": _dir(geo_stress_score),
            "confidence": _conf_label(geo_stress_score, ["geopolitics_war_trade", "defense_security", "china_taiwan_supply_chain"]),
            "supporting_themes": _supporting_themes(
                ["geopolitics_war_trade", "defense_security", "china_taiwan_supply_chain"], cb
            ),
            "description": "Geopolitical risk level from conflict, defense, and China/Taiwan market odds.",
        },
        "higher_for_longer_vs_easing": {
            "label": _label_pair(hfl_score, "higher_for_longer", "easing"),
            "score": hfl_score,
            "direction": _dir(hfl_score),
            "confidence": _conf_label(hfl_score, ["macro_rates_inflation"]),
            "supporting_themes": _supporting_themes(
                ["macro_rates_inflation", "energy_commodities"], cb
            ),
            "description": "Fed policy regime derived from inflation and rates prediction market odds.",
        },
        "commodity_pressure_vs_relief": {
            "label": _label_pair(commodity_score, "commodity_pressure", "commodity_relief"),
            "score": commodity_score,
            "direction": _dir(commodity_score),
            "confidence": _conf_label(commodity_score, ["energy_commodities", "geopolitics_war_trade"]),
            "supporting_themes": _supporting_themes(
                ["energy_commodities", "geopolitics_war_trade"], cb
            ),
            "description": "Energy and commodity price pressure derived from Polymarket energy and geopolitics odds.",
        },
        "ai_capex_supportive_vs_restrictive": {
            "label": _label_pair(ai_score, "ai_capex_supportive", "ai_capex_restrictive"),
            "score": ai_score,
            "direction": _dir(ai_score),
            "confidence": _conf_label(ai_score, ["ai_semis_tech", "china_taiwan_supply_chain"]),
            "supporting_themes": _supporting_themes(
                ["ai_semis_tech", "china_taiwan_supply_chain"], cb
            ),
            "description": "AI and semiconductor capex environment from AI, chip export, and China supply chain odds.",
        },
    }

    return regime
