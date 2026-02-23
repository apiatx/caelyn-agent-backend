"""
Deterministic screener definitions for AI Screener preset buttons.

Each preset defines:
  - finviz_filters: Finviz filter string for Phase A discovery
  - finviz_sort: Sort order for Finviz results
  - enrichment: Which fields are required from enrichment
  - ta_rules: Technical analysis filter criteria applied in Phase C
  - fundamental_rules: Fundamental filter criteria applied in Phase C
  - ranking_weights: Weights for deterministic scoring (tech, fundamental, liquidity)
  - screen_label: Human-readable name
  - explain_template: Bullet points explaining the screen logic
"""

SCREENER_DEFINITIONS = {
    "oversold_growing": {
        "screen_label": "Oversold + Growing",
        "finviz_filters": "fa_salesqoq_o15,cap_smallover,ta_rsi_os40,sh_avgvol_o200",
        "finviz_sort": "-change",
        "enrichment": ["quote", "fundamentals", "candles"],
        "ta_rules": {
            "rsi_max": 40,
            "prefer_rsi_rising": True,
            "prefer_above_sma20": True,
            "prefer_rel_vol": 1.5,
        },
        "fundamental_rules": {
            "rev_growth_yoy_min": 15,
            "market_cap_min_m": 50,
        },
        "ranking_weights": {"technical": 0.40, "fundamental": 0.40, "liquidity": 0.20},
        "explain_template": [
            "RSI(14) <= 40 — oversold or approaching oversold territory",
            "Revenue growth >= 15% YoY — company still growing despite price weakness",
            "Market cap >= $50M — excludes nano-caps with erratic moves",
            "Preferring relative volume >= 1.5x — institutional interest signal",
            "Price above SMA20 or RSI turning up = potential bounce setup",
        ],
    },
    "value_momentum": {
        "screen_label": "Value + Momentum",
        "finviz_filters": "fa_pe_u20,fa_salesqoq_o10,ta_sma50_pa,sh_avgvol_o400",
        "finviz_sort": "-change",
        "enrichment": ["quote", "fundamentals", "candles"],
        "ta_rules": {
            "above_sma50": True,
            "sma50_trending_up": True,
            "macd_histogram_positive_or_cross": True,
            "min_avg_dollar_vol_m": 2.0,
        },
        "fundamental_rules": {
            "pe_max": 20,
            "rev_growth_yoy_min": 10,
        },
        "ranking_weights": {"technical": 0.40, "fundamental": 0.40, "liquidity": 0.20},
        "explain_template": [
            "PE ratio <= 20 — not overvalued relative to earnings",
            "Revenue growth >= 10% YoY — real business expansion",
            "Price above SMA50 with SMA50 trending up — confirmed uptrend",
            "MACD histogram positive or recent bullish cross — momentum intact",
            "Average dollar volume >= $2M — sufficient liquidity for entries/exits",
        ],
    },
    "insider_breakout": {
        "screen_label": "Insider + Breakout",
        "finviz_filters": "ta_highlow20d_nh,sh_relvol_o2,sh_avgvol_o200",
        "finviz_sort": "-change",
        "enrichment": ["quote", "fundamentals", "candles"],
        "ta_rules": {
            "breakout_20d_high": True,
            "rel_vol_min": 2.0,
            "prefer_above_upper_bb": True,
        },
        "fundamental_rules": {},
        "ranking_weights": {"technical": 0.50, "fundamental": 0.20, "liquidity": 0.30},
        "explain_template": [
            "20-day high breakout — price clearing recent resistance",
            "Relative volume >= 2.0x — strong institutional participation",
            "Insider activity flagged when available — smart money confirmation",
            "Bollinger Band upper break preferred — volatility expansion",
        ],
    },
    "high_growth_sc": {
        "screen_label": "High Growth Small Cap",
        "finviz_filters": "fa_salesqoq_o25,cap_smallunder,ta_sma20_pa,sh_avgvol_o200",
        "finviz_sort": "-change",
        "enrichment": ["quote", "fundamentals", "candles"],
        "ta_rules": {
            "above_sma20": True,
            "above_sma50": True,
            "sma20_above_sma50": True,
            "prefer_positive_change": True,
        },
        "fundamental_rules": {
            "rev_growth_yoy_min": 30,
        },
        "ranking_weights": {"technical": 0.35, "fundamental": 0.45, "liquidity": 0.20},
        "explain_template": [
            "Revenue growth >= 30% YoY or >= 15% QoQ — hypergrowth territory",
            "Small/mid cap (under $2B) — power law return potential",
            "Price above SMA20 and SMA50 — strong trend structure",
            "SMA20 > SMA50 — intermediate trend confirmed up",
            "Positive recent price action preferred — momentum alignment",
        ],
    },
    "dividend_value": {
        "screen_label": "Dividend Value",
        "finviz_filters": "fa_div_o2,fa_pe_u20,ta_sma200_pa,sh_avgvol_o200",
        "finviz_sort": "-fa_div",
        "enrichment": ["quote", "fundamentals", "candles"],
        "ta_rules": {
            "above_sma200_or_reclaiming": True,
            "not_severe_downtrend": True,
            "prefer_rsi_improving": True,
        },
        "fundamental_rules": {
            "dividend_yield_min": 2.5,
            "pe_max": 20,
        },
        "ranking_weights": {"technical": 0.30, "fundamental": 0.50, "liquidity": 0.20},
        "explain_template": [
            "Dividend yield >= 2.5% — meaningful income component",
            "PE ratio <= 20 — not overpaying for dividends",
            "Price above SMA200 or reclaiming it — long-term trend intact",
            "Excludes severe downtrends (price below SMA200 AND SMA200 falling)",
            "RSI improving = potential upside inflection",
        ],
    },
    "short_squeeze": {
        "screen_label": "Short Squeeze",
        "finviz_filters": "sh_short_o15,sh_relvol_o3,sh_avgvol_o500",
        "finviz_sort": "-change",
        "enrichment": ["quote", "fundamentals", "candles"],
        "ta_rules": {
            "rel_vol_min": 3.0,
            "breakout_or_gap_up": True,
            "min_avg_dollar_vol_m": 5.0,
        },
        "fundamental_rules": {
            "short_float_min": 15,
        },
        "ranking_weights": {"technical": 0.50, "fundamental": 0.20, "liquidity": 0.30},
        "explain_template": [
            "Short float >= 15% — significant short interest to squeeze",
            "Relative volume >= 3.0x — explosive volume confirming squeeze",
            "Breakout or gap-up pattern — shorts being forced to cover",
            "Average dollar volume >= $5M — sufficient liquidity for execution",
            "Multi-day squeeze continuation patterns also qualify",
        ],
    },
    # ============================================================
    # Technical Analysis screener presets
    # ============================================================
    "stage2_breakouts": {
        "screen_label": "Stage 2 Breakouts (Weinstein)",
        "finviz_filters": "ta_sma200_pa,ta_sma50_pa,ta_highlow52w_nh,sh_avgvol_o300,sh_price_o5",
        "finviz_sort": "-change",
        "enrichment": ["quote", "fundamentals", "candles"],
        "ta_rules": {
            "above_sma50": True,
            "above_sma200": True,
            "sma50_trending_up": True,
            "sma200_trending_up": True,
            "breakout_52w_high": True,
            "prefer_rel_vol": 1.5,
        },
        "fundamental_rules": {},
        "ranking_weights": {"technical": 0.60, "fundamental": 0.15, "liquidity": 0.25},
        "explain_template": [
            "Price above rising SMA50 AND rising SMA200 — Weinstein Stage 2 confirmed",
            "52-week high breakout — clearing major resistance with momentum",
            "Volume confirmation preferred (rel vol >= 1.5x) — institutional participation",
            "Only stocks > $5 with 300K+ avg volume — tradable setups only",
            "Stage 2 is the ONLY stage to buy — strongest risk-adjusted returns",
        ],
    },
    "bullish_breakouts": {
        "screen_label": "Bullish Breakouts",
        "finviz_filters": "ta_highlow20d_nh,sh_relvol_o1.5,ta_sma50_pa,sh_avgvol_o300,sh_price_o3",
        "finviz_sort": "-change",
        "enrichment": ["quote", "fundamentals", "candles"],
        "ta_rules": {
            "breakout_20d_high": True,
            "above_sma50": True,
            "rel_vol_min": 1.5,
            "prefer_rsi_rising": True,
        },
        "fundamental_rules": {},
        "ranking_weights": {"technical": 0.55, "fundamental": 0.15, "liquidity": 0.30},
        "explain_template": [
            "New 20-day high — price clearing recent consolidation resistance",
            "Relative volume >= 1.5x — breakout confirmed by volume expansion",
            "Above SMA50 — intermediate trend is up, not a dead-cat bounce",
            "RSI trending up preferred — momentum supporting the breakout",
            "Avg volume 300K+ — sufficient liquidity for clean execution",
        ],
    },
    "bearish_breakdowns": {
        "screen_label": "Bearish Breakdowns",
        "finviz_filters": "ta_sma50_pb,ta_sma200_pb,ta_highlow20d_nl,sh_avgvol_o300,sh_price_o5",
        "finviz_sort": "change",
        "enrichment": ["quote", "fundamentals", "candles"],
        "ta_rules": {
            "below_sma50": True,
            "below_sma200": True,
            "breakdown_20d_low": True,
            "prefer_rsi_declining": True,
        },
        "fundamental_rules": {},
        "ranking_weights": {"technical": 0.60, "fundamental": 0.15, "liquidity": 0.25},
        "explain_template": [
            "New 20-day low — price breaking below recent support",
            "Below SMA50 AND SMA200 — Weinstein Stage 4 decline confirmed",
            "Volume confirmation shows institutional distribution",
            "RSI declining preferred — no oversold bounce imminent",
            "These are short candidates or names to AVOID on the long side",
        ],
    },
    "oversold_bounces": {
        "screen_label": "Oversold Bounce Candidates",
        "finviz_filters": "ta_rsi_ob30,ta_sma200_pa,sh_avgvol_o300,sh_price_o5",
        "finviz_sort": "ta_rsi",
        "enrichment": ["quote", "fundamentals", "candles"],
        "ta_rules": {
            "rsi_max": 35,
            "prefer_rsi_rising": True,
            "above_sma200": True,
            "prefer_above_sma20": True,
        },
        "fundamental_rules": {
            "market_cap_min_m": 100,
        },
        "ranking_weights": {"technical": 0.50, "fundamental": 0.30, "liquidity": 0.20},
        "explain_template": [
            "RSI(14) <= 30 — deeply oversold territory, bounce probability elevated",
            "Price still above SMA200 — long-term uptrend intact despite pullback",
            "Market cap >= $100M — excludes distressed micro-caps",
            "RSI turning up preferred — early reversal signal",
            "Best when SMA20 reclaim coincides with RSI divergence",
        ],
    },
    "overbought_warnings": {
        "screen_label": "Overbought Warnings",
        "finviz_filters": "ta_rsi_os70,sh_avgvol_o300,sh_price_o5",
        "finviz_sort": "-ta_rsi",
        "enrichment": ["quote", "fundamentals", "candles"],
        "ta_rules": {
            "rsi_min": 70,
            "prefer_rsi_declining": True,
            "prefer_above_upper_bb": True,
        },
        "fundamental_rules": {},
        "ranking_weights": {"technical": 0.60, "fundamental": 0.15, "liquidity": 0.25},
        "explain_template": [
            "RSI(14) >= 70 — overbought territory, mean-reversion risk elevated",
            "Extended above upper Bollinger Band preferred — volatility extreme",
            "These are CAUTION signals — not necessarily shorts, but reduce long exposure",
            "If RSI is declining from 80+, reversal may already be in progress",
            "High volume + overbought = potential blowoff top",
        ],
    },
    "crossover_signals": {
        "screen_label": "Crossover Signals",
        "finviz_filters": "ta_sma20_cross50,sh_avgvol_o300,sh_price_o3",
        "finviz_sort": "-change",
        "enrichment": ["quote", "fundamentals", "candles"],
        "ta_rules": {
            "sma20_crossing_sma50": True,
            "prefer_volume_expansion": True,
            "prefer_rsi_above_50": True,
        },
        "fundamental_rules": {},
        "ranking_weights": {"technical": 0.55, "fundamental": 0.20, "liquidity": 0.25},
        "explain_template": [
            "SMA20 crossing above SMA50 — bullish golden cross intermediate signal",
            "Volume expansion preferred — confirms institutional conviction in the cross",
            "RSI above 50 preferred — momentum supporting the directional shift",
            "Avg volume 300K+ — tradable with clean spreads",
            "Best combined with sector tailwind and catalyst",
        ],
    },
    "momentum_shifts": {
        "screen_label": "Momentum Shifts",
        "finviz_filters": "sh_relvol_o2,ta_change_u,sh_avgvol_o300,sh_price_o3",
        "finviz_sort": "-sh_relvol",
        "enrichment": ["quote", "fundamentals", "candles"],
        "ta_rules": {
            "rel_vol_min": 2.0,
            "prefer_rsi_inflection": True,
            "prefer_macd_cross": True,
        },
        "fundamental_rules": {},
        "ranking_weights": {"technical": 0.55, "fundamental": 0.15, "liquidity": 0.30},
        "explain_template": [
            "Relative volume >= 2x — abnormal activity signaling a shift",
            "Positive price change today — momentum turning up",
            "MACD bullish cross preferred — trend reversal confirmation",
            "RSI inflection from oversold preferred — early momentum shift signal",
            "Best when combined with news catalyst or earnings surprise",
        ],
    },
    "trend_status": {
        "screen_label": "Trend Status Overview",
        "finviz_filters": "ta_sma200_pa,ta_sma50_pa,sh_avgvol_o500,sh_price_o5",
        "finviz_sort": "-perf4w",
        "enrichment": ["quote", "fundamentals", "candles"],
        "ta_rules": {
            "above_sma50": True,
            "above_sma200": True,
            "sma50_trending_up": True,
            "prefer_sma20_above_sma50": True,
        },
        "fundamental_rules": {
            "market_cap_min_m": 300,
        },
        "ranking_weights": {"technical": 0.50, "fundamental": 0.25, "liquidity": 0.25},
        "explain_template": [
            "Above SMA50 AND SMA200 — confirmed uptrend across timeframes",
            "SMA50 trending up — intermediate momentum positive",
            "SMA20 > SMA50 preferred — strong trend structure (all MAs aligned)",
            "Market cap >= $300M — institutional-grade names",
            "Sorted by 4-week performance — strongest recent momentum first",
        ],
    },
    "volume_movers": {
        "screen_label": "Volume & Movers",
        "finviz_filters": "sh_relvol_o3,sh_avgvol_o200,sh_price_o2",
        "finviz_sort": "-sh_relvol",
        "enrichment": ["quote", "fundamentals", "candles"],
        "ta_rules": {
            "rel_vol_min": 3.0,
            "prefer_breakout_or_breakdown": True,
        },
        "fundamental_rules": {},
        "ranking_weights": {"technical": 0.45, "fundamental": 0.15, "liquidity": 0.40},
        "explain_template": [
            "Relative volume >= 3x — massive volume spike signals unusual activity",
            "Avg volume 200K+ — not thinly traded penny stocks",
            "Could be bullish (breakout) or bearish (distribution) — context matters",
            "Sorted by relative volume — most abnormal activity first",
            "Check news catalyst — volume without catalyst = insider activity or block trades",
        ],
    },
    # ============================================================
    # Fundamental Analysis screener presets
    # ============================================================
    "fundamental_leaders": {
        "screen_label": "Fundamental Leaders",
        "finviz_filters": "fa_salesqoq_o20,fa_epsqoq_o15,fa_opermargin_pos,ta_sma50_pa,sh_avgvol_o300",
        "finviz_sort": "-fa_salesqoq",
        "enrichment": ["quote", "fundamentals", "candles"],
        "ta_rules": {
            "above_sma50": True,
            "prefer_positive_change": True,
        },
        "fundamental_rules": {
            "rev_growth_yoy_min": 20,
            "eps_growth_yoy_min": 15,
            "oper_margin_positive": True,
        },
        "ranking_weights": {"technical": 0.25, "fundamental": 0.55, "liquidity": 0.20},
        "explain_template": [
            "Revenue growth >= 20% QoQ — accelerating top line",
            "EPS growth >= 15% QoQ — earnings power expanding",
            "Positive operating margin — profitable business model",
            "Price above SMA50 — market confirming the fundamental strength",
            "Sorted by revenue growth — fastest growers first",
        ],
    },
    "fundamental_acceleration": {
        "screen_label": "Rapidly Improving Fundamentals",
        "finviz_filters": "fa_salesqoq_o25,fa_epsqoq_o25,sh_avgvol_o200,sh_price_o3",
        "finviz_sort": "-fa_salesqoq",
        "enrichment": ["quote", "fundamentals", "candles"],
        "ta_rules": {
            "prefer_above_sma20": True,
            "prefer_rsi_rising": True,
        },
        "fundamental_rules": {
            "rev_growth_yoy_min": 25,
            "eps_growth_yoy_min": 25,
        },
        "ranking_weights": {"technical": 0.20, "fundamental": 0.60, "liquidity": 0.20},
        "explain_template": [
            "Revenue AND EPS growth >= 25% QoQ — rapid fundamental acceleration",
            "These are companies where business is inflecting sharply upward",
            "Price above SMA20 preferred — market starting to notice",
            "RSI rising preferred — momentum building on improving numbers",
            "Best candidates often have upcoming earnings catalyst to realize value",
        ],
    },
}

SCREENER_PRESETS = list(SCREENER_DEFINITIONS.keys())
