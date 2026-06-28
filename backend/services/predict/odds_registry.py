"""
Prediction Market Tracked Odds Registry.

Defines the canonical set of macro/market/geopolitical odds families that
Caelyn tracks continuously on Polymarket.  This is the single source of truth
for both:

  - /api/predict/odds/live         (Market Dashboard + Prophetik Investor page)
  - /api/predict/odds/history      (7-day charting endpoint)
  - InvestorIntel.get_intelligence (tracked_odds field in Prophetik payload)

Registry entry fields
---------------------
family_key          str     canonical identifier — stable across restarts
label               str     human-readable name
category            str     display grouping
priority            int     1 = highest; used for sort order in both widgets
dashboard_enabled   bool    show in Market Dashboard live odds widget
prophetik_enabled   bool    include in /api/predict/investor/intelligence
search_queries      list    free-text query strings sent to Polymarket search
keyword_patterns    list    substring match against question.lower() (OR logic)
exclude_patterns    list    disqualify market if ANY pattern matches question.lower()
preferred_outcome   str     "yes" | "no" — which leg is equity-relevant
                            "higher" / "lower" for price milestone families
"""

from __future__ import annotations

from typing import Any

# ── Registry ──────────────────────────────────────────────────────────────────

ODDS_REGISTRY: list[dict[str, Any]] = [

    # ─── Fed / Rates ─────────────────────────────────────────────────────────

    {
        "family_key":       "fed_rate_decision",
        "label":            "Next Fed Rate Decision",
        "category":         "Fed / Rates",
        "priority":         1,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["fed rate cut", "FOMC rate decision", "federal reserve rate"],
        "keyword_patterns": [
            "fed rate", "fomc rate", "federal reserve rate",
            "cut rates at", "hike rates at", "25bp", "50bp", "75bp",
            "rate decision", "fomc meeting", "basis point cut", "basis point hike",
            "rate cut at", "rate hike at",
            # matches actual Polymarket phrasing: "no change in Fed interest rates after the July meeting"
            "fed interest rate", "interest rates after", "no change in fed",
            "change in fed interest", "rate hold", "rate unchanged", "no rate change",
            "after the fomc", "after the july", "after the september", "after the november",
            "after the december", "after the march", "after the may",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "yes",
    },
    {
        "family_key":       "fed_cuts_2026",
        "label":            "Number of Fed Cuts in 2026",
        "category":         "Fed / Rates",
        "priority":         2,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["fed cuts 2026", "rate cuts 2026"],
        "keyword_patterns": [
            "cuts in 2026", "rate cuts in 2026", "how many cuts 2026",
            "fed cuts 2026", "number of cuts in 2026", "cuts by end of 2026",
            # matches "Will no Fed rate cuts happen in 2026?" / "Will 1 Fed rate cut happen in 2026?"
            "fed rate cut", "rate cuts happen", "no rate cuts happen",
            "rate cut happen", "fed rate cuts",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "yes",
    },
    {
        "family_key":       "fed_hikes_2026",
        "label":            "Fed Hikes in 2026",
        "category":         "Fed / Rates",
        "priority":         3,
        "dashboard_enabled": False,
        "prophetik_enabled": True,
        "search_queries":   ["fed hikes 2026"],
        "keyword_patterns": [
            "hikes in 2026", "rate hikes in 2026", "fed hikes 2026",
            "number of hikes 2026",
            # actual Polymarket phrasing: "Fed Rate Hike by July 2026 Meeting?"
            "fed rate hike", "rate hike in 2026", "rate hike by", "rate hike 2026",
            "hike by", "fed hike", "fed rate hike", "hike in 2026",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "yes",
    },

    # ─── Macro / Economy ─────────────────────────────────────────────────────

    {
        "family_key":       "recession_probability",
        "label":            "US Recession Probability",
        "category":         "Macro",
        "priority":         4,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["US recession 2025", "US recession 2026"],
        "keyword_patterns": [
            "recession in 2025", "recession in 2026", "us recession",
            "gdp contraction", "gdp negative", "nber recession",
            "technical recession",
            # broader phrasing: "Will the US enter a recession by Q4 2026?"
            "enter a recession", "recession by", "recession probability",
            "recession 2025", "recession 2026",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "yes",
    },
    {
        "family_key":       "cpi_inflation",
        "label":            "CPI / Inflation Reading",
        "category":         "Macro / Inflation",
        "priority":         5,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["CPI report", "inflation reading"],
        "keyword_patterns": [
            "cpi above", "cpi below", "core cpi", "cpi exceed",
            "inflation above", "inflation below", "pce above",
            "pce below", "inflation report", "cpi surprise",
            # actual Polymarket phrasing: "Will annual inflation be 3.6% or less in June?"
            "annual inflation", "cpi be", "cpi reading", "cpi print",
            "inflation reading", "inflation in june", "inflation in july",
            "inflation data", "consumer price",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "yes",
    },
    {
        "family_key":       "jobs_unemployment",
        "label":            "Jobs / Unemployment Report",
        "category":         "Macro / Labor",
        "priority":         6,
        "dashboard_enabled": False,
        "prophetik_enabled": True,
        "search_queries":   ["unemployment rate", "nonfarm payrolls", "jobs report"],
        "keyword_patterns": [
            "unemployment rate", "nonfarm payroll", "jobs report",
            "payroll", "jobless claims", "unemployment above",
            "unemployment below", "labor market", "employment report",
        ],
        "exclude_patterns": ["football", "soccer"],
        "preferred_outcome": "yes",
    },

    # ─── Index / Market Direction ─────────────────────────────────────────────

    {
        "family_key":       "spx_daily_direction",
        "label":            "S&P 500 Up / Down Today",
        "category":         "Equities / Index",
        "priority":         7,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["S&P 500 close today", "SPX close"],
        "keyword_patterns": [
            "s&p 500 close", "s&p500 close", "spx close", "spy close",
            "s&p 500 up", "spx up", "s&p 500 green", "stock market up today",
            "market close up", "s&p 500 end",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "yes",
    },
    {
        "family_key":       "nasdaq_daily_direction",
        "label":            "Nasdaq Up / Down Today",
        "category":         "Equities / Index",
        "priority":         8,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["Nasdaq close today", "QQQ close"],
        "keyword_patterns": [
            "nasdaq close", "nasdaq up", "qqq close", "nasdaq green",
            "nasdaq end", "nasdaq 100 close", "ndx close",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "yes",
    },
    {
        "family_key":       "dow_daily_direction",
        "label":            "Dow Jones Up / Down Today",
        "category":         "Equities / Index",
        "priority":         9,
        "dashboard_enabled": False,
        "prophetik_enabled": True,
        "search_queries":   ["Dow close today", "DJIA close"],
        "keyword_patterns": [
            "dow close", "djia close", "dow jones close", "dow up today",
            "dow jones end", "dow jones green",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "yes",
    },

    # ─── Mega-cap / Watchlist Price Milestones ────────────────────────────────

    {
        "family_key":       "nvda_price_milestone",
        "label":            "NVDA Price Milestone",
        "category":         "Equities / Tech",
        "priority":         10,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["NVDA price", "Nvidia stock price"],
        "keyword_patterns": [
            "nvidia above", "nvda above", "nvda hit", "nvda reach",
            "nvidia reach", "nvidia price", "nvda price",
            # "Will NVIDIA be the largest company in the world by market cap on June 30?"
            "nvidia largest", "nvidia market cap", "nvidia be the",
            "nvidia stock", "nvda stock",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "higher",
    },
    {
        "family_key":       "tsla_price_milestone",
        "label":            "TSLA Price Milestone",
        "category":         "Equities / Tech",
        "priority":         11,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["TSLA price", "Tesla stock price"],
        "keyword_patterns": [
            "tesla above", "tsla above", "tesla hit", "tsla reach",
            "tesla price", "tsla price",
            # "Will Tesla be the largest company in the world by market cap on June 30?"
            "tesla largest", "tesla market cap", "tesla be the",
            "tesla stock", "tsla stock",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "higher",
    },
    {
        "family_key":       "aapl_price_milestone",
        "label":            "AAPL Price Milestone",
        "category":         "Equities / Tech",
        "priority":         12,
        "dashboard_enabled": False,
        "prophetik_enabled": True,
        "search_queries":   ["AAPL price", "Apple stock price"],
        "keyword_patterns": [
            "apple above", "aapl above", "apple hit", "aapl reach",
            "apple price", "aapl price",
            # "Will Apple be the largest company in the world by market cap on June 30?"
            "apple largest", "apple market cap", "apple be the",
            "apple stock", "aapl stock",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "higher",
    },
    {
        "family_key":       "msft_price_milestone",
        "label":            "MSFT Price Milestone",
        "category":         "Equities / Tech",
        "priority":         13,
        "dashboard_enabled": False,
        "prophetik_enabled": True,
        "search_queries":   ["MSFT price", "Microsoft stock price"],
        "keyword_patterns": [
            "microsoft above", "msft above", "microsoft hit", "msft reach",
            "microsoft largest", "microsoft market cap", "microsoft be the",
            "msft price", "microsoft stock", "msft stock",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "higher",
    },
    {
        "family_key":       "googl_price_milestone",
        "label":            "GOOGL / Alphabet Price Milestone",
        "category":         "Equities / Tech",
        "priority":         14,
        "dashboard_enabled": False,
        "prophetik_enabled": True,
        "search_queries":   ["Google stock price", "Alphabet price"],
        "keyword_patterns": [
            "google above", "googl above", "alphabet above", "google hit", "googl hit",
            "google largest", "google market cap", "google be the",
            "alphabet market cap", "googl stock", "google stock",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "higher",
    },
    {
        "family_key":       "amd_price_milestone",
        "label":            "AMD Price Milestone",
        "category":         "Equities / Tech",
        "priority":         15,
        "dashboard_enabled": False,
        "prophetik_enabled": True,
        "search_queries":   ["AMD price"],
        "keyword_patterns": [
            "amd above", "amd hit", "amd reach", "amd price",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "higher",
    },

    # ─── Tech / Earnings / AI ─────────────────────────────────────────────────

    {
        "family_key":       "earnings_nvda",
        "label":            "NVDA Earnings",
        "category":         "Earnings / Tech",
        "priority":         16,
        "dashboard_enabled": False,
        "prophetik_enabled": True,
        "search_queries":   ["Nvidia earnings", "NVDA earnings"],
        "keyword_patterns": [
            "nvidia earnings", "nvda earnings", "nvidia revenue",
            "nvda revenue", "nvidia eps", "nvda eps",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "yes",
    },
    {
        "family_key":       "earnings_tsla",
        "label":            "TSLA Earnings / Deliveries",
        "category":         "Earnings / Tech",
        "priority":         17,
        "dashboard_enabled": False,
        "prophetik_enabled": True,
        "search_queries":   ["Tesla earnings", "TSLA deliveries"],
        "keyword_patterns": [
            "tesla earnings", "tsla earnings", "tesla revenue",
            "tsla revenue", "tesla deliveries", "tsla deliveries",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "yes",
    },
    {
        "family_key":       "ai_export_controls",
        "label":            "AI / Chip Export Controls",
        "category":         "AI / Tech / Regulation",
        "priority":         18,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["chip export control", "AI export restriction"],
        "keyword_patterns": [
            "export control", "chip ban", "ai restriction",
            "nvidia ban", "entity list", "chip restriction",
            "semiconductor export", "gpu export", "ai export",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "yes",
    },

    # ─── Geopolitics ─────────────────────────────────────────────────────────

    {
        "family_key":       "hormuz_iran",
        "label":            "Strait of Hormuz / Iran",
        "category":         "Geopolitics / Energy",
        "priority":         19,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["Strait of Hormuz", "Iran sanctions"],
        "keyword_patterns": [
            "hormuz", "strait of hormuz", "iran sanctions",
            "iran nuclear", "persian gulf",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "yes",
    },
    {
        "family_key":       "russia_ukraine",
        "label":            "Russia / Ukraine Ceasefire",
        "category":         "Geopolitics / War",
        "priority":         20,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["Ukraine ceasefire", "Russia Ukraine peace"],
        "keyword_patterns": [
            "ukraine ceasefire", "russia ceasefire", "ukraine peace",
            "russia peace deal", "end the war ukraine",
            "putin", "zelenskyy", "zelensky",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "yes",
    },
    {
        "family_key":       "china_taiwan",
        "label":            "China / Taiwan Strait Tension",
        "category":         "Geopolitics / Tech / Supply Chain",
        "priority":         21,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["Taiwan invasion", "China Taiwan"],
        "keyword_patterns": [
            "taiwan strait", "taiwan invasion", "china invades taiwan",
            "pla crossing", "tsmc takeover", "taiwan independence",
            "china blockade taiwan", "strait blockade",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "yes",
    },
    {
        "family_key":       "israel_gaza",
        "label":            "Israel / Gaza / Hamas",
        "category":         "Geopolitics / Middle East",
        "priority":         22,
        "dashboard_enabled": False,
        "prophetik_enabled": True,
        "search_queries":   ["Israel Gaza ceasefire"],
        "keyword_patterns": [
            "israel", "gaza", "hamas", "hezbollah",
            "west bank", "idf", "netanyahu", "palestin",
        ],
        "exclude_patterns": ["sport", "game", "soccer"],
        "preferred_outcome": "yes",
    },
    {
        "family_key":       "us_tariffs",
        "label":            "US Tariff Escalation",
        "category":         "Trade / Geopolitics",
        "priority":         23,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["US tariffs", "reciprocal tariffs trade war"],
        "keyword_patterns": [
            "tariff rate exceed", "tariff above", "new tariff",
            "reciprocal tariff", "tariff 2025", "tariff 2026", "trade war",
            # actual Polymarket phrasing: "Tariff increase on Canada in effect by June 30?"
            "tariff increase", "tariff on canada", "tariff on china",
            "tariff on", "tariff in effect", "tariff escalat",
            "tariff pause", "tariff suspended", "tariff lifted",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "yes",
    },

    # ─── Commodities / Crypto ─────────────────────────────────────────────────

    {
        "family_key":       "oil_price_milestone",
        "label":            "Oil / Crude Price Milestone",
        "category":         "Commodities / Energy",
        "priority":         24,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["crude oil price", "WTI oil"],
        "keyword_patterns": [
            "crude above", "oil above", "wti above", "brent above",
            "crude below", "oil below", "wti below", "brent below",
            # "Will WTI Crude Oil (WTI) hit (LOW) $20 in June?" / "hit (HIGH) $150"
            "wti hit", "oil hit", "crude hit", "crude (wti)", "crude oil (wti)",
            "wti crude", "crude oil price",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "higher",
    },
    {
        "family_key":       "gold_price_milestone",
        "label":            "Gold Price Milestone",
        "category":         "Commodities / Safe Haven",
        "priority":         25,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["gold price milestone", "gold above"],
        "keyword_patterns": [
            "gold above", "gold hit", "gold reach", "gold price", "xau above",
            # "Will Gold (GC) hit (HIGH) $8,000 by end of June?" — ticker is "GC"
            "gold (gc)", "(gc) hit", "gc) hit",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "higher",
    },
    {
        "family_key":       "bitcoin_price",
        "label":            "Bitcoin Price Milestone",
        "category":         "Crypto / Risk Proxy",
        "priority":         26,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["Bitcoin price", "BTC above"],
        "keyword_patterns": [
            "bitcoin above", "btc above", "bitcoin hit",
            "bitcoin reach", "btc hit", "btc reach",
            # "Will the price of Bitcoin be above $64,000 on June 27?"
            "bitcoin price", "price of bitcoin", "btc price",
            "bitcoin be above", "btc be above",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "higher",
    },
]


# ── Fast lookup helpers ────────────────────────────────────────────────────────

REGISTRY_BY_KEY: dict[str, dict] = {f["family_key"]: f for f in ODDS_REGISTRY}

DASHBOARD_FAMILIES: list[dict] = sorted(
    [f for f in ODDS_REGISTRY if f["dashboard_enabled"]],
    key=lambda f: f["priority"],
)

PROPHETIK_FAMILIES: list[dict] = sorted(
    [f for f in ODDS_REGISTRY if f["prophetik_enabled"]],
    key=lambda f: f["priority"],
)


def get_family(family_key: str) -> dict | None:
    """Return the registry entry for a family_key, or None."""
    return REGISTRY_BY_KEY.get(family_key)


def is_sports_excluded(question: str, family_def: dict) -> bool:
    """Return True if this market should be excluded based on exclude_patterns."""
    q = question.lower()
    for pat in family_def.get("exclude_patterns", []):
        if pat in q:
            return True
    return False
