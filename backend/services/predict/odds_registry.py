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
allow_near_expiry   bool    if True, bypass the 72-hour near-expiry exclusion gate
                            (required for daily direction markets that expire same day)
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
    #
    # allow_near_expiry=True: daily direction markets expire same day — the
    # 72-hour near-expiry gate is bypassed; markets are only excluded if
    # closed=True, acceptingOrders=False, or end_date is already in the past.

    {
        "family_key":       "spx_daily_direction",
        "label":            "S&P 500 Up / Down Today",
        "category":         "Equities / Index",
        "priority":         7,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "allow_near_expiry":  True,
        "search_queries":   ["S&P 500 close today", "SPX close"],
        "keyword_patterns": [
            # Close direction
            "s&p 500 close higher", "s&p 500 close lower", "s&p 500 close positive",
            "s&p 500 close negative", "s&p 500 close above", "s&p 500 finish higher",
            "s&p 500 finish lower", "s&p 500 end higher", "s&p 500 end lower",
            "s&p 500 end positive", "s&p 500 end negative",
            "spx close higher", "spx close lower", "spx finish higher", "spx finish lower",
            "spy close higher", "spy close lower", "spy finish higher", "spy finish lower",
            # Up/Down/Green/Red
            "will the s&p 500 be up", "will the s&p 500 be down",
            "will s&p 500 be up", "will s&p 500 be down",
            "s&p 500 up today", "s&p 500 down today",
            "s&p 500 green today", "s&p 500 red today",
            "s&p 500 up or down", "spx up or down",
            "will the s&p 500 close", "will s&p 500 close",
            "will spx close", "will spy close",
            "stock market up today", "market close higher today", "market close lower today",
            "market close up today", "market close down today",
            "will the stock market be up", "will the stock market be down",
            # Weekly
            "s&p 500 up this week", "s&p 500 down this week",
            "s&p 500 green this week", "s&p 500 red this week",
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
        "allow_near_expiry":  True,
        "search_queries":   ["Nasdaq close today", "QQQ close"],
        "keyword_patterns": [
            # Close direction
            "nasdaq close higher", "nasdaq close lower", "nasdaq close positive",
            "nasdaq close negative", "nasdaq finish higher", "nasdaq finish lower",
            "nasdaq end higher", "nasdaq end lower", "nasdaq end positive",
            "qqq close higher", "qqq close lower", "qqq finish higher", "qqq finish lower",
            "nasdaq 100 close higher", "nasdaq 100 close lower",
            "ndx close higher", "ndx close lower", "ndx finish higher",
            # Up/Down/Green/Red
            "will the nasdaq be up", "will the nasdaq be down",
            "will nasdaq be up", "will nasdaq be down",
            "nasdaq up today", "nasdaq down today",
            "nasdaq green today", "nasdaq red today",
            "nasdaq up or down", "qqq up or down",
            "will the nasdaq close", "will nasdaq close",
            "will qqq close", "will ndx close",
            # Weekly
            "nasdaq up this week", "nasdaq down this week",
            "nasdaq green this week", "nasdaq red this week",
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
        "allow_near_expiry":  True,
        "search_queries":   ["Dow close today", "DJIA close"],
        "keyword_patterns": [
            # Close direction
            "dow close higher", "dow close lower", "dow close positive",
            "dow close negative", "dow finish higher", "dow finish lower",
            "dow end higher", "dow end lower", "dow end positive",
            "djia close higher", "djia close lower", "djia finish higher",
            "dow jones close higher", "dow jones close lower",
            "dia close higher", "dia close lower", "dia finish higher",
            # Up/Down/Green/Red
            "will the dow be up", "will the dow be down",
            "will dow be up", "will dow be down",
            "dow up today", "dow down today",
            "dow green today", "dow red today",
            "dow up or down", "djia up or down",
            "will the dow close", "will dow close", "will djia close",
            # Weekly
            "dow up this week", "dow down this week",
            "dow green this week", "dow red this week",
        ],
        "exclude_patterns": [],
        "preferred_outcome": "yes",
    },

    # ─── Mega-cap / Watchlist Price Milestones ────────────────────────────────
    #
    # Keyword patterns must require actual price / market cap / stock price wording.
    # "X be the" patterns tightened to "X be the largest" to avoid matching
    # AI benchmark questions like "Will Google be the first company to have an AI model..."

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
            "nvidia be the largest", "nvidia market cap", "nvidia largest company",
            "nvidia stock", "nvda stock",
        ],
        "exclude_patterns": ["ai model", "chatbot", "arena", "benchmark"],
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
            "tesla be the largest", "tesla market cap", "tesla largest company",
            "tesla stock", "tsla stock",
        ],
        "exclude_patterns": ["ai model", "chatbot", "arena", "benchmark"],
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
            "apple be the largest", "apple market cap", "apple largest company",
            "apple stock", "aapl stock",
        ],
        "exclude_patterns": ["ai model", "chatbot", "arena", "benchmark"],
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
            "microsoft be the largest", "microsoft market cap", "microsoft largest company",
            "msft price", "microsoft stock", "msft stock",
        ],
        "exclude_patterns": ["ai model", "chatbot", "arena", "benchmark"],
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
            # Tightened: was "google be the" (too broad — matched Chatbot Arena question)
            "google be the largest", "google market cap", "google largest company",
            "alphabet market cap", "googl stock", "google stock",
        ],
        # Explicitly block AI benchmark / Chatbot Arena markets from this family
        "exclude_patterns": [
            "chatbot arena", "arena score", "arena", "ai model",
            "chatbot", "coding ai", "first company", "benchmark",
        ],
        "preferred_outcome": "higher",
    },

    # ─── AI / Tech Benchmarks ─────────────────────────────────────────────────
    #
    # Separate from stock price milestones.
    # Covers AI model performance, Chatbot Arena rankings, frontier AI leadership.

    {
        "family_key":       "google_ai_benchmark",
        "label":            "Google AI Model Benchmark",
        "category":         "AI / Tech",
        "priority":         15,
        "dashboard_enabled": False,
        "prophetik_enabled": True,
        "search_queries":   ["Google AI model", "Chatbot Arena", "AI model benchmark"],
        "keyword_patterns": [
            # Chatbot Arena / Arena Score — these are highly specific phrases
            "chatbot arena", "arena score", "1550 overall", "1550 on chatbot",
            "1550 overall arena", "ai model reach 1550", "ai model hit 1550",
            # Google AI leadership specifically (not generic company AI rankings)
            "google have the best", "google best coding",
            "google first company", "first company to have an ai",
            "will google be the first", "google be the first",
        ],
        "exclude_patterns": [
            # Do not match stock price, market cap, or earnings questions
            "stock", "price", "market cap", "earnings", "revenue",
            "above $", "below $", "reach $",
        ],
        "preferred_outcome": "yes",
    },

    {
        "family_key":       "amd_price_milestone",
        "label":            "AMD Price Milestone",
        "category":         "Equities / Tech",
        "priority":         16,
        "dashboard_enabled": False,
        "prophetik_enabled": True,
        "search_queries":   ["AMD price"],
        "keyword_patterns": [
            "amd above", "amd hit", "amd reach", "amd price",
        ],
        "exclude_patterns": ["ai model", "chatbot", "arena", "benchmark"],
        "preferred_outcome": "higher",
    },

    # ─── Tech / Earnings / AI ─────────────────────────────────────────────────

    {
        "family_key":       "earnings_nvda",
        "label":            "NVDA Earnings",
        "category":         "Earnings / Tech",
        "priority":         17,
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
        "priority":         18,
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
        "priority":         19,
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
        "priority":         20,
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
        "priority":         21,
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
        "priority":         22,
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
        "priority":         23,
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
        "priority":         24,
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
        "priority":         25,
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
        "priority":         26,
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
        "priority":         27,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["Bitcoin price", "BTC above"],
        "keyword_patterns": [
            "bitcoin above", "btc above", "bitcoin hit",
            "bitcoin reach", "btc hit", "btc reach",
            # "Will the price of Bitcoin be above $64,000 on June 27?"
            "bitcoin price", "price of bitcoin", "btc price",
            "bitcoin be above", "btc be above",
            # Range / between
            "price of bitcoin be between", "bitcoin be between",
        ],
        # Do not let price milestone steal daily direction markets
        "exclude_patterns": [
            "up or down", "close higher", "close lower",
            "end positive", "end negative", "finish higher", "finish lower",
            "bitcoin green", "bitcoin red", "btc green", "btc red",
            "up today", "down today",
        ],
        "preferred_outcome": "higher",
    },

    # ─── BTC Daily Direction ──────────────────────────────────────────────────
    #
    # Separate from bitcoin_price (milestone / target-price markets).
    # Tracks daily or weekly close direction: up/green vs down/red.
    # allow_near_expiry=True: these markets expire same day or same week.
    # exclude_patterns block 5-minute intraday micromarkets and price milestones.

    {
        "family_key":       "btc_daily_direction",
        "label":            "Bitcoin Daily Direction",
        "category":         "Crypto / Risk Proxy",
        "priority":         28,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "allow_near_expiry":  True,
        "search_queries":   ["Bitcoin close today", "Bitcoin up today"],
        "keyword_patterns": [
            # Close direction
            "will bitcoin close higher", "will bitcoin close lower",
            "will btc close higher", "will btc close lower",
            "bitcoin close higher", "bitcoin close lower",
            "btc close higher", "btc close lower",
            "bitcoin finish higher", "bitcoin finish lower",
            "bitcoin end higher", "bitcoin end lower",
            "bitcoin end positive", "bitcoin end negative",
            "bitcoin close positive", "bitcoin close negative",
            # Up/Down today
            "bitcoin up today", "bitcoin down today",
            "btc up today", "btc down today",
            # Green/Red
            "will bitcoin be green", "will bitcoin be red",
            "will btc be green", "will btc be red",
            "bitcoin green today", "bitcoin red today",
            "btc green today", "btc red today",
            # Up or Down (daily/weekly, not 5-minute micromarkets)
            # also matches date-specific phrasing: "Bitcoin Up or Down on Jun 29?"
            "bitcoin up or down today", "bitcoin up or down this week",
            "bitcoin up or down on",
            "btc up or down today", "btc up or down this week",
            "btc up or down on",
            # Weekly close
            "bitcoin weekly close", "bitcoin close this week",
            "btc weekly close", "btc close this week",
            "bitcoin up this week", "bitcoin down this week",
            "bitcoin green this week", "bitcoin red this week",
        ],
        # Block price milestone markets AND 5-minute intraday micromarkets
        "exclude_patterns": [
            # Price milestone phrasing
            "above $", "below $", "price of bitcoin", "bitcoin reach",
            "btc reach", "bitcoin hit $", "btc hit $", "bitcoin dip",
            "be above", "be between", "bitcoin be",
            # 5-minute intraday micromarket time-range patterns
            "pm et", "am et", "pm-", "am-",
            ":00pm", ":05pm", ":10pm", ":15pm", ":20pm", ":25pm",
            ":30pm", ":35pm", ":40pm", ":45pm", ":50pm", ":55pm",
            ":00am", ":05am", ":10am", ":15am", ":20am", ":25am",
        ],
        "preferred_outcome": "yes",
    },

    # ─── WTI Daily Direction ──────────────────────────────────────────────────
    #
    # Tracks daily close direction for WTI crude oil: up vs down.
    # Polymarket phrasing: "WTI Crude Oil (WTI) Up or Down on [date]"
    # allow_near_expiry=True: these expire same day.

    {
        "family_key":       "wti_daily_direction",
        "label":            "WTI Oil Daily Direction",
        "category":         "Commodities / Energy",
        "priority":         30,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "allow_near_expiry": True,
        "search_queries":   ["WTI oil up today", "WTI crude oil up or down"],
        "keyword_patterns": [
            # Polymarket exact phrasing
            "wti crude oil (wti) up or down",
            "wti up or down", "crude oil up or down",
            # Close direction
            "wti close higher", "wti close lower",
            "wti end higher", "wti end lower",
            "crude oil close higher", "crude oil close lower",
            # Up/down phrasing
            "wti up today", "wti down today",
            "oil up today", "oil down today",
            "wti up or down on", "wti crude up or down on",
            "crude oil up or down on",
            # Weekly/date-specific
            "wti up or down this week",
        ],
        "exclude_patterns": [
            "above $", "below $", "hit $", "reach $",
            "wti hit", "wti reach", "price of wti",
            "oil above", "oil below", "crude above", "crude below",
            "wti above", "wti below",
        ],
        "preferred_outcome": "yes",
    },

    # ─── Gold Daily Direction ─────────────────────────────────────────────────
    #
    # Tracks daily close direction for gold (XAUUSD): up vs down.
    # Polymarket phrasing: "Gold (XAUUSD) Up or Down on [date]"

    {
        "family_key":       "gold_daily_direction",
        "label":            "Gold Daily Direction",
        "category":         "Commodities / Safe Haven",
        "priority":         31,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "allow_near_expiry": True,
        "search_queries":   ["gold close higher today", "Gold XAUUSD up or down"],
        "keyword_patterns": [
            # Polymarket exact phrasing
            "gold (xauusd) up or down",
            "xauusd up or down",
            # General up/down
            "gold up or down", "gold up or down on", "gold up or down this week",
            # Close direction
            "gold close higher", "gold close lower",
            "gold end higher", "gold end lower",
            # Daily phrasing
            "gold up today", "gold down today",
            "will gold close higher", "will gold close lower",
        ],
        "exclude_patterns": [
            "above $", "below $", "gold hit", "gold reach",
            "gold above", "gold below", "gold price",
            "xau above", "xau below",
        ],
        "preferred_outcome": "yes",
    },

    # ─── NVDA Daily Direction ─────────────────────────────────────────────────
    #
    # Tracks daily close direction for NVIDIA stock: up vs down.
    # Polymarket phrasing: "NVIDIA (NVDA) Up or Down on [date]"
    # Note: may have low volume — quality field will reflect.

    {
        "family_key":       "nvda_daily_direction",
        "label":            "NVIDIA Daily Direction",
        "category":         "Equities / Tech",
        "priority":         32,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "allow_near_expiry": True,
        "search_queries":   ["NVDA up or down today", "Nvidia close higher"],
        "keyword_patterns": [
            # Polymarket exact phrasing
            "nvidia (nvda) up or down",
            "nvda up or down", "nvidia up or down",
            "nvda up or down on", "nvidia up or down on",
            # Close direction
            "nvda close higher", "nvda close lower",
            "nvidia close higher", "nvidia close lower",
            # Daily phrasing
            "nvda up today", "nvda down today",
            "nvidia up today", "nvidia down today",
            "will nvidia close higher", "will nvda close higher",
        ],
        "exclude_patterns": [
            "above $", "below $",
            "nvda hit", "nvda reach", "nvidia hit", "nvidia reach",
            "nvda earnings", "nvidia earnings", "nvda revenue", "nvidia revenue",
            "nvda above", "nvda below", "nvidia above", "nvidia below",
        ],
        "preferred_outcome": "yes",
    },

    # ─── S&P 500 Tomorrow Close Ladder ────────────────────────────────────────
    #
    # Kalshi KXINX series: range buckets for S&P 500 close price tomorrow at 4pm EDT.
    # allow_near_expiry=True: these daily markets expire the same day.

    {
        "family_key":       "spx_tomorrow_close_ladder",
        "label":            "S&P 500 Tomorrow Close",
        "category":         "Indices / Macro",
        "priority":         33,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "allow_near_expiry": True,
        "search_queries":   ["S&P 500 close range tomorrow", "SPX price bucket tomorrow"],
        "keyword_patterns": [
            "s&p 500 close between", "s&p close range",
            "spx close between", "spx close range",
            "s&p 500 price range tomorrow", "spx price range tomorrow",
            "s&p 500 tomorrow close", "will s&p close between",
        ],
        "exclude_patterns": [
            "up or down", "close higher", "close lower",
            "up today", "down today", "year-end", "dec 31",
        ],
        "preferred_outcome": "yes",
    },

    # ─── S&P 500 Year-End Close Range ─────────────────────────────────────────
    #
    # Kalshi KXINXY series: range buckets for S&P 500 closing price on Dec 31.

    {
        "family_key":       "spx_year_end_close_range",
        "label":            "S&P 500 Year-End Close Range",
        "category":         "Indices / Macro",
        "priority":         34,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["S&P 500 year end price range 2026", "SPX close range December 2026"],
        "keyword_patterns": [
            "s&p 500 close between", "spx close between",
            "s&p year-end range", "spx year-end range",
            "s&p 500 year-end close between",
        ],
        "exclude_patterns": [
            "up or down", "up today", "down today",
            "daily", "tomorrow", "month",
        ],
        "preferred_outcome": "yes",
    },

    # ─── S&P 500 Year High Ladder ─────────────────────────────────────────────
    #
    # Kalshi KXINXMAXY series: one-touch "X or above" ladder for S&P 500 year high.

    {
        "family_key":       "spx_year_high_ladder",
        "label":            "S&P 500 Year High",
        "category":         "Indices / Macro",
        "priority":         35,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["S&P 500 year high 2026", "SPX high 2026 reach"],
        "keyword_patterns": [
            "s&p 500 max", "spx maximum", "s&p 500 year high",
            "spx year high", "s&p 500 high this year",
            "s&p high in 2026", "s&p 500 peak",
            "s&p 500 annual high", "spx annual high",
        ],
        "exclude_patterns": [
            "up or down", "close higher", "close lower",
            "up today", "down today", "year-end close", "close range",
        ],
        "preferred_outcome": "yes",
    },

    # ─── S&P 500 Month-End High Ladder ────────────────────────────────────────
    #
    # Kalshi KXINXMAXMM series: one-touch "X or above" ladder for S&P 500 month-end high.

    {
        "family_key":       "spx_month_end_high_ladder",
        "label":            "S&P 500 Month-End High",
        "category":         "Indices / Macro",
        "priority":         36,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["S&P 500 month high", "SPX end of month high"],
        "keyword_patterns": [
            "s&p 500 month high", "spx month high",
            "s&p 500 monthly high", "spx monthly high",
            "s&p 500 by end of month", "spx by end of month",
            "s&p 500 end of month high",
        ],
        "exclude_patterns": [
            "up or down", "daily", "tomorrow", "year-end", "year high",
        ],
        "preferred_outcome": "yes",
    },

    # ─── S&P 500 vs Gold Annual Return (Matchup) ──────────────────────────────
    #
    # Kalshi KXINXVSGOLD series: which asset posts a higher total return in 2026?

    {
        "family_key":       "spx_vs_gold_annual_return",
        "label":            "S&P 500 vs. Gold (Annual Return)",
        "category":         "Indices / Macro",
        "priority":         37,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["S&P 500 vs gold 2026", "SPX beat gold annual return"],
        "keyword_patterns": [
            "s&p 500 vs gold", "spx vs gold",
            "s&p vs gold", "equity vs gold",
            "s&p 500 outperform gold",
            "s&p 500 total return vs gold",
        ],
        "exclude_patterns": [
            "gold price", "gold above", "gold below",
            "gold hit", "gold reach", "xauusd",
        ],
        "preferred_outcome": "yes",
    },

    # ─── Nasdaq-100 Year-End Close Range ──────────────────────────────────────
    #
    # Kalshi KXNASDAQ100Y series: range buckets for Nasdaq-100 close price on Dec 31.

    {
        "family_key":       "nasdaq_year_end_close_range",
        "label":            "Nasdaq-100 Year-End Close Range",
        "category":         "Indices / Macro",
        "priority":         38,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["Nasdaq year end price range 2026", "NDX close range December 2026"],
        "keyword_patterns": [
            "nasdaq year-end range", "ndx year-end range",
            "nasdaq 100 close between", "ndx close between",
            "nasdaq close between", "nasdaq close range",
            "nasdaq year-end close between",
        ],
        "exclude_patterns": [
            "up or down", "up today", "down today",
            "daily", "tomorrow", "month",
        ],
        "preferred_outcome": "yes",
    },

    # ─── S&P 500 Year-End Milestone ───────────────────────────────────────────
    #
    # Tracks market consensus on where the S&P 500 will close on Dec 31.
    # Kalshi PRIMARY via KXINXDIRY (multi-strike ladder).
    # Polymarket fallback: above-level milestone markets for Dec 31.
    # preferred_outcome="higher": equity-positive read is S&P finishing higher.

    {
        "family_key":       "spx_dec31_milestone",
        "label":            "S&P 500 Year-End Level",
        "category":         "Indices / Macro",
        "priority":         29,
        "dashboard_enabled": True,
        "prophetik_enabled": True,
        "search_queries":   ["S&P 500 year end 2026", "SPX December 31 2026"],
        "keyword_patterns": [
            # Kalshi phrasing
            "s&p price at year-end", "s&p price on dec 31",
            "s&p 500 on dec 31", "s&p 500 be above",
            # General year-end phrasing
            "spx year end", "spx year-end", "s&p year end", "s&p 500 year-end",
            "s&p 500 close above", "spx close above",
            # Specific Dec 31 Polymarket style
            "s&p above", "spy close above",
        ],
        "exclude_patterns": [
            # Exclude today/this-week direction markets
            "up or down", "close higher", "close lower",
            "up today", "down today", "this week",
            # Exclude daily direction phrasing
            "end higher", "end lower",
        ],
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
