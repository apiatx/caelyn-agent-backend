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
}

SCREENER_PRESETS = list(SCREENER_DEFINITIONS.keys())
