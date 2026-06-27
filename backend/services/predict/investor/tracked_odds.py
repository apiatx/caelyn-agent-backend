"""
Predict / Investor — Tracked Macro & Market Odds Families.

Maintains a permanent list of high-signal macro/market probability families
(Fed decisions, recession, index direction, mega-cap milestones, etc.) and
matches each to the best available live Polymarket market.

Pure function — no I/O, no caching, no HTTP.
Feed in the already-fetched (cached) market list from PolymarketIntelligence.
"""

from __future__ import annotations

from typing import Optional

# ── Tracked family definitions ─────────────────────────────────────────────────
#
# Each entry maps a family_key to search keywords (OR logic — any match qualifies).
# Candidates are ranked by 24h volume; highest-volume market wins per family.

_TRACKED_FAMILIES: list[dict] = [
    {
        "family_key":        "fed_rate_decision",
        "label":             "Next Fed Rate Decision",
        "category":          "Fed / Rates",
        "dashboard_priority": 1,
        "keywords":          ["fed rate", "fomc rate", "rate cut at", "rate hike at",
                              "basis point cut", "basis point hike", "25bp", "50bp",
                              "federal reserve cut", "federal reserve hike",
                              "rate decision", "fomc decision"],
        "description":       "Probability of a Fed rate cut or hike at the next FOMC meeting.",
    },
    {
        "family_key":        "fed_cuts_2026",
        "label":             "Number of Fed Cuts in 2026",
        "category":          "Fed / Rates",
        "dashboard_priority": 2,
        "keywords":          ["cuts in 2026", "rate cuts in 2026", "how many cuts 2026",
                              "fed cuts 2026", "number of cuts in 2026", "cuts by end of 2026"],
        "description":       "How many Fed rate cuts are priced in for the full year 2026.",
    },
    {
        "family_key":        "fed_hikes_2026",
        "label":             "Fed Hikes in 2026",
        "category":          "Fed / Rates",
        "dashboard_priority": 3,
        "keywords":          ["hikes in 2026", "rate hikes in 2026", "fed hikes 2026",
                              "number of hikes 2026"],
        "description":       "Probability of any Fed rate hike in 2026.",
    },
    {
        "family_key":        "recession_probability",
        "label":             "US Recession Probability",
        "category":          "Macro",
        "dashboard_priority": 4,
        "keywords":          ["recession in 2025", "recession in 2026", "us recession",
                              "gdp contraction", "gdp negative", "nber recession",
                              "technical recession"],
        "description":       "Polymarket odds of a US recession this year.",
    },
    {
        "family_key":        "spx_daily_direction",
        "label":             "S&P 500 Up Today",
        "category":          "Equities",
        "dashboard_priority": 5,
        "keywords":          ["s&p 500 close", "spx close", "spy close",
                              "s&p 500 up", "spx up", "market close up",
                              "s&p 500 green", "stock market up today"],
        "description":       "Daily S&P 500 close direction odds.",
    },
    {
        "family_key":        "nvda_price_milestone",
        "label":             "NVDA Price Milestone",
        "category":          "Equities / Tech",
        "dashboard_priority": 6,
        "keywords":          ["nvidia above", "nvda above", "nvda hit",
                              "nvda reach", "nvidia reach", "nvidia price"],
        "description":       "NVDA reaching key price milestones.",
    },
    {
        "family_key":        "tsla_price_milestone",
        "label":             "TSLA Price Milestone",
        "category":          "Equities / Tech",
        "dashboard_priority": 7,
        "keywords":          ["tesla above", "tsla above", "tesla hit", "tsla reach",
                              "tesla price", "tsla price"],
        "description":       "TSLA reaching key price milestones.",
    },
    {
        "family_key":        "aapl_price_milestone",
        "label":             "AAPL Price Milestone",
        "category":          "Equities / Tech",
        "dashboard_priority": 8,
        "keywords":          ["apple above", "aapl above", "apple hit", "aapl reach",
                              "apple price", "aapl price"],
        "description":       "AAPL reaching key price milestones.",
    },
    {
        "family_key":        "msft_price_milestone",
        "label":             "MSFT Price Milestone",
        "category":          "Equities / Tech",
        "dashboard_priority": 9,
        "keywords":          ["microsoft above", "msft above", "microsoft hit", "msft reach"],
        "description":       "MSFT reaching key price milestones.",
    },
    {
        "family_key":        "googl_price_milestone",
        "label":             "GOOGL / Alphabet Price Milestone",
        "category":          "Equities / Tech",
        "dashboard_priority": 10,
        "keywords":          ["google above", "googl above", "alphabet above", "google hit"],
        "description":       "GOOGL reaching key price milestones.",
    },
    {
        "family_key":        "amd_price_milestone",
        "label":             "AMD Price Milestone",
        "category":          "Equities / Tech",
        "dashboard_priority": 11,
        "keywords":          ["amd above", "amd hit", "amd reach", "amd price"],
        "description":       "AMD reaching key price milestones.",
    },
    {
        "family_key":        "ai_export_controls",
        "label":             "AI / Chip Export Controls",
        "category":          "AI / Tech / Regulation",
        "dashboard_priority": 12,
        "keywords":          ["export control", "chip ban", "ai restriction",
                              "nvidia ban", "entity list", "chip restriction",
                              "semiconductor export", "gpu export", "ai export"],
        "description":       "Probability of new AI/chip export restrictions being enacted.",
    },
    {
        "family_key":        "bitcoin_price",
        "label":             "Bitcoin Price Milestone",
        "category":          "Crypto",
        "dashboard_priority": 13,
        "keywords":          ["bitcoin above", "btc above", "bitcoin hit",
                              "bitcoin reach", "btc hit", "btc reach"],
        "description":       "Bitcoin reaching key price levels.",
    },
    {
        "family_key":        "oil_price_milestone",
        "label":             "Oil / Crude Price Milestone",
        "category":          "Commodities",
        "dashboard_priority": 14,
        "keywords":          ["crude above", "oil above", "wti above", "brent above",
                              "crude below", "oil below", "wti below", "brent below"],
        "description":       "Crude oil reaching key price levels.",
    },
    {
        "family_key":        "gold_price_milestone",
        "label":             "Gold Price Milestone",
        "category":          "Commodities / Safe Haven",
        "dashboard_priority": 15,
        "keywords":          ["gold above", "gold hit", "gold reach", "gold price", "xau above"],
        "description":       "Gold reaching key price levels.",
    },
    {
        "family_key":        "us_tariffs",
        "label":             "US Tariff Escalation",
        "category":          "Trade / Geopolitics",
        "dashboard_priority": 16,
        "keywords":          ["tariff rate exceed", "tariff above", "new tariff",
                              "reciprocal tariff", "tariff 2025", "tariff 2026"],
        "description":       "Probability of further US tariff escalation.",
    },
    {
        "family_key":        "cpi_inflation",
        "label":             "CPI / Inflation Reading",
        "category":          "Macro / Inflation",
        "dashboard_priority": 17,
        "keywords":          ["cpi above", "cpi below", "core cpi", "cpi exceed",
                              "inflation above", "inflation below", "pce above",
                              "pce below", "inflation report"],
        "description":       "Probability of a CPI/PCE print surprising in either direction.",
    },
    {
        "family_key":        "hormuz_iran",
        "label":             "Strait of Hormuz / Iran",
        "category":          "Geopolitics / Energy",
        "dashboard_priority": 18,
        "keywords":          ["hormuz", "strait of hormuz", "iran sanctions",
                              "iran nuclear", "persian gulf"],
        "description":       "Strait of Hormuz closure risk and Iran geopolitical tension.",
    },
    {
        "family_key":        "russia_ukraine",
        "label":             "Russia / Ukraine Ceasefire",
        "category":          "Geopolitics",
        "dashboard_priority": 19,
        "keywords":          ["ukraine ceasefire", "russia ceasefire", "ukraine peace",
                              "russia peace deal", "end the war ukraine"],
        "description":       "Probability of a Russia-Ukraine ceasefire or peace deal.",
    },
]


def build_tracked_odds(all_markets: list[dict]) -> list[dict]:
    """
    Match tracked families against a list of enriched Polymarket market dicts.

    For each family:
      - Search all_markets for any market whose question contains any keyword (OR logic).
      - Pick the highest-volume candidate as the primary market.
      - Include up to 5 driver_markets (top by volume).
      - If no match found, return a stub with null probability fields.

    Args:
        all_markets: List of enriched market dicts from PolymarketIntelligence.get_top_markets().
                     Should be the unfiltered list (300+ markets) so SPX/BTC/etc. are included.

    Returns:
        List of tracked-odds entries, sorted by dashboard_priority.
    """
    results: list[dict] = []

    for fdef in _TRACKED_FAMILIES:
        keywords: list[str] = fdef["keywords"]

        # Collect candidates: any market whose question contains at least one keyword
        candidates: list[dict] = []
        for m in all_markets:
            q = m.get("question", "").lower()
            if any(kw in q for kw in keywords):
                candidates.append(m)

        if candidates:
            # Rank by 24h volume; highest volume = most active signal
            candidates_sorted = sorted(candidates, key=lambda m: m.get("volume_24h", 0), reverse=True)
            best = candidates_sorted[0]

            yes_pct = best.get("yes_pct")
            yes_prob = round(yes_pct / 100.0, 4) if yes_pct is not None else None

            p_1d  = best.get("price_change_1d")
            p_1h  = best.get("price_change_1h")
            p_7d  = best.get("price_change_1wk")

            driver_markets = [
                {
                    "question":    m.get("question", ""),
                    "yes_pct":     m.get("yes_pct"),
                    "volume_24h":  m.get("volume_24h", 0),
                    "delta_24h_pp": m.get("price_change_1d"),
                    "condition_id": m.get("condition_id", ""),
                    "slug":         m.get("slug", ""),
                }
                for m in candidates_sorted[:5]
            ]

            results.append({
                "family_key":        fdef["family_key"],
                "label":             fdef["label"],
                "category":          fdef["category"],
                "description":       fdef["description"],
                "yes_probability":   yes_prob,
                "delta_1h_pp":       round(p_1h, 2) if p_1h is not None else None,
                "delta_24h_pp":      round(p_1d, 2) if p_1d is not None else None,
                "delta_7d_pp":       round(p_7d, 2) if p_7d is not None else None,
                "volume_24h":        best.get("volume_24h"),
                "liquidity":         best.get("liquidity"),
                "dashboard_priority": fdef["dashboard_priority"],
                "market_question":   best.get("question", ""),
                "condition_id":      best.get("condition_id", ""),
                "candidate_count":   len(candidates),
                "driver_markets":    driver_markets,
            })
        else:
            # Family not live on Polymarket — return a null stub so the
            # frontend can still render the card in a "not available" state.
            results.append({
                "family_key":        fdef["family_key"],
                "label":             fdef["label"],
                "category":          fdef["category"],
                "description":       fdef["description"],
                "yes_probability":   None,
                "delta_1h_pp":       None,
                "delta_24h_pp":      None,
                "delta_7d_pp":       None,
                "volume_24h":        None,
                "liquidity":         None,
                "dashboard_priority": fdef["dashboard_priority"],
                "market_question":   None,
                "condition_id":      None,
                "candidate_count":   0,
                "driver_markets":    [],
            })

    return sorted(results, key=lambda x: x["dashboard_priority"])
