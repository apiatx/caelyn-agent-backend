"""
Canonical source of truth for all preset / intent / alias / routing data.

Exports
-------
PRESET_ALIASES          dict[str, str]   raw key → INTENT_PROFILES key
INTENT_PROFILES         dict[str, dict]  canonical profile definitions
INTENT_TO_CATEGORY      dict[str, str]   intent → data-gather category
ASSET_CLASS_CATEGORY_MAP dict[str,str]
VALID_INTENTS           set[str]
DEFAULT_PLAN            dict

_CAELYN_ALIAS_MAP       dict[str, str]   routing alias table (used by caelyn_routing)

resolve_preset(raw)         → canonical profile key or None
build_plan_from_preset(raw) → plan dict or None
plan_to_query_info(plan)    → query_info dict
resolve_to_route_key(raw)   → CAELYN_ROUTES key or None
"""

from __future__ import annotations

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop


# ---------------------------------------------------------------------------
# PRESET_ALIASES  (raw input → canonical INTENT_PROFILES key)
# Source: TradingAgent.PRESET_ALIASES (claude_agent.py lines 31-169)
# ---------------------------------------------------------------------------
PRESET_ALIASES: dict[str, str] = {
    "morning_briefing": "daily_briefing",
    "briefing": "daily_briefing",
    "daily": "daily_briefing",
    "trending": "cross_asset_trending",
    "cross_asset": "cross_asset_trending",
    "whats_hot": "cross_asset_trending",
    "microcap": "microcap_asymmetry",
    "asymmetric": "microcap_asymmetry",
    "small_cap": "microcap_asymmetry",
    "sector": "sector_rotation",
    "rotation": "sector_rotation",
    "macro": "macro_outlook",
    "economy": "macro_outlook",
    "earnings": "earnings_catalyst",
    "crypto": "crypto_scanner",
    "crypto_focus": "crypto_scanner",
    "crypto_scan": "crypto_scanner",
    "commodities": "commodity_scan",
    "commodity": "commodity_scan",
    "commodity_focus": "commodity_scan",
    "commodities_focus": "commodity_scan",
    # --- Sector preset direct keys (frontend sends these) ---
    "energy": "thematic_energy",
    "energy_focus": "thematic_energy",
    "materials": "thematic_materials",
    "materials_focus": "thematic_materials",
    "aerospace_defense": "thematic_defense",
    "aerospace_focus": "thematic_defense",
    "tech": "thematic_tech",
    "tech_focus": "thematic_tech",
    "ai_compute": "thematic_ai",
    "quantum": "thematic_quantum",
    "quantum_focus": "thematic_quantum",
    "fintech": "thematic_financials",
    "finance_focus": "thematic_financials",
    "biotech": "thematic_healthcare",
    "healthcare_focus": "thematic_healthcare",
    "real_estate": "thematic_real_estate",
    "real_estate_focus": "thematic_real_estate",
    "social": "social_momentum",
    "wsb": "social_momentum",
    "reddit": "social_momentum",
    # --- Hyperliquid perps ---
    "hyperliquid": "hyperliquid_screener",
    "hype": "hyperliquid_screener",
    "hl": "hyperliquid_screener",
    "perps": "hyperliquid_screener",
    "crypto_perps": "hyperliquid_screener",
    "hl_screener": "hyperliquid_screener",
    # --- Whale watch / 13F ---
    "whale_watch": "whale_watch",
    "whale": "whale_watch",
    "whales": "whale_watch",
    "13f": "whale_watch",
    "institutional": "whale_watch",
    "big_money": "whale_watch",
    "fund_flows": "whale_watch",
    "whale_activity": "whale_watch",
    # --- Insider activity (Form 4) ---
    "insider_activity": "insider_activity",
    "insider": "insider_activity",
    "form4": "insider_activity",
    "insider_form4": "insider_activity",
    "insider_filings": "insider_activity",
    # --- Social X dashboard ---
    "social_x": "social_x_dashboard",
    "x_dashboard": "social_x_dashboard",
    "x_consensus_dashboard": "social_x_dashboard",
    "x_alpha": "social_x_dashboard",
    "long_term_conviction": "investment_ideas",
    "investments": "investment_ideas",
    "sqglp": "investment_ideas",
    "bearish": "bearish_setups",
    "shorts": "bearish_setups",
    "thematic": "thematic_scan",
    "themes": "thematic_scan",
    "portfolio": "portfolio_review",
    "holdings": "portfolio_review",
    "x_scan": "x_social_scan",
    "twitter_scan": "x_social_scan",
    "trades": "best_trades",
    "setups": "best_trades",
    "trade_setups": "best_trades",
    "x_sentiment_scan": "x_social_scan",
    "grok_scan": "x_social_scan",
    "x_social": "x_social_scan",
    "oversold": "oversold_growing",
    "oversold_bounce": "oversold_growing",
    "value": "value_momentum",
    "insider": "insider_breakout",
    "high_growth": "high_growth_sc",
    "growth_small_cap": "high_growth_sc",
    "dividend": "dividend_value",
    "dividends": "dividend_value",
    "income": "dividend_value",
    "squeeze": "short_squeeze",
    "short_squeeze_scan": "short_squeeze",
    # --- Sector buttons ---
    "sector_energy": "thematic_energy",
    "sector_ai": "thematic_ai",
    "sector_materials": "thematic_materials",
    "sector_quantum": "thematic_quantum",
    "sector_defense": "thematic_defense",
    "sector_tech": "thematic_tech",
    "sector_financials": "thematic_financials",
    "sector_healthcare": "thematic_healthcare",
    "sector_real_estate": "thematic_real_estate",
    "sector_uranium": "thematic_uranium",
    # --- Technical Analysis buttons ---
    "technical_stage2": "screener_stage2_breakouts",
    "technical_bullish_breakouts": "screener_bullish_breakouts",
    "technical_bearish_setups": "bearish_setups",
    "technical_breakdowns": "screener_bearish_breakdowns",
    "technical_oversold": "screener_oversold_bounces",
    "technical_overbought": "screener_overbought_warnings",
    "technical_crossovers": "screener_crossover_signals",
    "momentum_shift_scan": "screener_momentum_shifts",
    "trend_status_scan": "screener_trend_status",
    "volume_movers_scan": "screener_volume_movers",
    # --- Fundamental Analysis buttons ---
    "fundamental_leaders": "screener_fundamental_leaders",
    "fundamental_acceleration": "screener_fundamental_acceleration",
    "earnings_watch": "earnings_catalyst",
    "insider_buying": "screener_insider_buying",
    "revenue_reaccelerating": "screener_revenue_reaccelerating",
    "margin_expansion": "screener_margin_expansion",
    "undervalued_growth": "screener_undervalued_growth",
    "institutional_accumulation": "screener_institutional_accumulation",
    "free_cash_flow_leaders": "screener_free_cash_flow_leaders",
    # --- Buzz buttons ---
    "social_momentum_scan": "social_momentum",
    "news_leaders": "news_leaders",
    "catalyst_scan": "catalyst_scan",
    # --- X Trader Consensus (broader / top traders) ---
    "x_trader_consensus": "x_trader_consensus",
    "trader_consensus": "x_trader_consensus",
    "top_traders": "x_trader_consensus",
    "consensus_tickers": "x_trader_consensus",
    "x_consensus": "x_trader_consensus",
    # --- X Select Trader Consensus (curated 25-account list) ---
    "x_select_trader_consensus": "x_select_trader_consensus",
    "select_traders": "x_select_trader_consensus",
    "select_trader_consensus": "x_select_trader_consensus",
    "curated_traders": "x_select_trader_consensus",
    "x_select_consensus": "x_select_trader_consensus",
    # --- Other ---
    "microcap_spec": "microcap_spec",
    # --- Trending Now (frontend Trending button sends "trending_now") ---
    "trending_now": "cross_asset_trending",
    "trending_scan": "cross_asset_trending",
    # --- Earnings Agent (frontend earnings page) ---
    "earnings_agent": "earnings_catalyst",
    # --- Prediction Markets ---
    "prediction_markets": "prediction_markets",
    "polymarket": "prediction_markets",
    "prediction": "prediction_markets",
    "odds": "prediction_markets",
    "probabilities": "prediction_markets",
    # --- News Intelligence ---
    "news_intelligence": "news_intelligence",
    "notifai": "news_intelligence",
    "news": "news_intelligence",
    "news_analysis": "news_intelligence",
    "news_markets": "news_intelligence",
}


# ---------------------------------------------------------------------------
# INTENT_PROFILES  (canonical profile key → profile dict)
# Source: TradingAgent.INTENT_PROFILES (claude_agent.py lines 1907-2586)
# ---------------------------------------------------------------------------
INTENT_PROFILES: dict[str, dict] = {
    "daily_briefing": {
        "intent": "briefing",
        "asset_classes": ["equities", "crypto", "commodities", "macro"],
        "modules": {
            "x_sentiment": True,
            "social_sentiment": True,
            "technical_scan": True,
            "fundamental_validation": True,
            "macro_context": True,
            "liquidity_filter": True,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
    },
    "cross_asset_trending": {
        "intent": "cross_asset_trending",
        "asset_classes": ["equities", "crypto", "commodities"],
        "modules": {
            "x_sentiment": False,
            "x_social_scan": True,
            "social_sentiment": True,
            "technical_scan": True,
            "fundamental_validation": True,
            "macro_context": False,
            "liquidity_filter": False,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "cross_asset_ranked",
        "priority_depth": "medium",
        "x_social_scan_mode": "cross_asset",
    },
    "microcap_asymmetry": {
        "intent": "cross_asset_trending",
        "asset_classes": ["equities", "crypto"],
        "modules": {
            "x_sentiment": True,
            "social_sentiment": True,
            "technical_scan": True,
            "fundamental_validation": True,
            "macro_context": False,
            "liquidity_filter": False,
            "earnings_data": False,
            "ticker_research": False,
        },
        "filters": {"market_cap_max": 2000000000},
        "risk_framework": "asymmetric",
        "response_style": "deep_thesis",
        "priority_depth": "deep",
    },
    "sector_rotation": {
        "intent": "sector_rotation",
        "asset_classes": ["equities"],
        "modules": {
            "x_sentiment": False,
            "social_sentiment": False,
            "technical_scan": True,
            "fundamental_validation": True,
            "macro_context": True,
            "liquidity_filter": True,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
    },
    "macro_outlook": {
        "intent": "macro_outlook",
        "asset_classes": ["equities", "commodities", "macro"],
        "modules": {
            "x_sentiment": False,
            "social_sentiment": False,
            "technical_scan": False,
            "fundamental_validation": False,
            "macro_context": True,
            "liquidity_filter": False,
            "earnings_data": True,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "deep",
    },
    "earnings_catalyst": {
        "intent": "event_driven",
        "asset_classes": ["equities"],
        "modules": {
            "x_sentiment": True,
            "social_sentiment": True,
            "technical_scan": True,
            "fundamental_validation": True,
            "macro_context": False,
            "liquidity_filter": False,
            "earnings_data": True,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "high_conviction_ranked",
        "priority_depth": "medium",
    },
    "crypto_scanner": {
        "intent": "crypto",
        "asset_classes": ["crypto"],
        "modules": {
            "x_sentiment": True,
            "social_sentiment": True,
            "technical_scan": True,
            "fundamental_validation": False,
            "macro_context": False,
            "liquidity_filter": True,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "high_conviction_ranked",
        "priority_depth": "medium",
    },
    "commodity_scan": {
        "intent": "commodities",
        "asset_classes": ["commodities"],
        "modules": {
            "x_sentiment": False,
            "social_sentiment": False,
            "technical_scan": True,
            "fundamental_validation": False,
            "macro_context": True,
            "liquidity_filter": False,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
    },
    "social_momentum": {
        "intent": "cross_asset_trending",
        "asset_classes": ["equities", "crypto"],
        "modules": {
            "x_sentiment": True,
            "social_sentiment": True,
            "technical_scan": True,
            "fundamental_validation": True,
            "macro_context": False,
            "liquidity_filter": False,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "high_conviction_ranked",
        "priority_depth": "medium",
    },
    "x_trader_consensus": {
        "intent": "x_trader_consensus",
        "asset_classes": ["equities"],
        "modules": {
            "x_sentiment": False,
            "social_sentiment": False,
            "technical_scan": False,
            "fundamental_validation": False,
            "macro_context": False,
            "liquidity_filter": False,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "high_conviction_ranked",
        "priority_depth": "medium",
    },
    "x_select_trader_consensus": {
        "intent": "x_select_trader_consensus",
        "asset_classes": ["equities"],
        "modules": {
            "x_sentiment": False,
            "social_sentiment": False,
            "technical_scan": False,
            "fundamental_validation": False,
            "macro_context": False,
            "liquidity_filter": False,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "high_conviction_ranked",
        "priority_depth": "medium",
    },
    "investment_ideas": {
        "intent": "investment_ideas",
        "asset_classes": ["equities"],
        "modules": {
            "x_sentiment": True,
            "x_social_scan": False,
            "social_sentiment": True,
            "technical_scan": True,
            "fundamental_validation": True,
            "macro_context": True,
            "liquidity_filter": True,
            "earnings_data": True,
            "ticker_research": False,
        },
        "risk_framework": "conservative",
        "response_style": "deep_thesis",
        "priority_depth": "deep",
    },
    "bearish_setups": {
        "intent": "short_setup",
        "asset_classes": ["equities"],
        "modules": {
            "x_sentiment": True,
            "social_sentiment": True,
            "technical_scan": True,
            "fundamental_validation": True,
            "macro_context": False,
            "liquidity_filter": True,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "bearish",
        "response_style": "high_conviction_ranked",
        "priority_depth": "medium",
    },
    "thematic_scan": {
        "intent": "thematic",
        "asset_classes": ["equities", "crypto"],
        "modules": {
            "x_sentiment": True,
            "social_sentiment": True,
            "technical_scan": True,
            "fundamental_validation": True,
            "macro_context": False,
            "liquidity_filter": False,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
    },
    "portfolio_review": {
        "intent": "portfolio_review",
        "asset_classes": ["equities", "crypto"],
        "modules": {
            "x_sentiment": False,
            "social_sentiment": False,
            "technical_scan": True,
            "fundamental_validation": True,
            "macro_context": True,
            "liquidity_filter": False,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "deep_thesis",
        "priority_depth": "deep",
    },
    "best_trades": {
        "intent": "best_trades",
        "asset_classes": ["equities"],
        "modules": {
            "x_sentiment": False,
            "x_social_scan": False,
            "social_sentiment": False,
            "technical_scan": True,
            "fundamental_validation": False,
            "macro_context": True,
            "liquidity_filter": True,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "high_conviction_ranked",
        "priority_depth": "medium",
    },
    "x_social_scan": {
        "intent": "x_social_scan",
        "asset_classes": ["equities", "crypto"],
        "modules": {
            "x_sentiment": False,
            "x_social_scan": True,
            "social_sentiment": False,
            "technical_scan": False,
            "fundamental_validation": False,
            "macro_context": False,
            "liquidity_filter": False,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "high_conviction_ranked",
        "priority_depth": "medium",
    },
    "oversold_growing": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": True},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "oversold_growing",
    },
    "value_momentum": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": True},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "value_momentum",
    },
    "insider_breakout": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": False},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "insider_breakout",
    },
    "high_growth_sc": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": True},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "high_growth_sc",
    },
    "dividend_value": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": True},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "dividend_value",
    },
    "short_squeeze": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": False},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "short_squeeze",
    },
    # ---- Sector / Thematic profiles ----
    "thematic_energy": {
        "intent": "thematic",
        "asset_classes": ["equities"],
        "modules": {"x_sentiment": True, "social_sentiment": True, "technical_scan": True, "fundamental_validation": True, "macro_context": True},
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
        "filters": {"theme": "energy"},
    },
    "thematic_ai": {
        "intent": "thematic",
        "asset_classes": ["equities"],
        "modules": {"x_sentiment": True, "social_sentiment": True, "technical_scan": True, "fundamental_validation": True, "macro_context": False},
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
        "filters": {"theme": "ai_compute"},
    },
    "thematic_materials": {
        "intent": "thematic",
        "asset_classes": ["equities", "commodities"],
        "modules": {"x_sentiment": True, "social_sentiment": True, "technical_scan": True, "fundamental_validation": True, "macro_context": True},
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
        "filters": {"theme": "materials"},
    },
    "thematic_quantum": {
        "intent": "thematic",
        "asset_classes": ["equities"],
        "modules": {"x_sentiment": True, "social_sentiment": True, "technical_scan": True, "fundamental_validation": True, "macro_context": False},
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
        "filters": {"theme": "quantum"},
    },
    "thematic_defense": {
        "intent": "thematic",
        "asset_classes": ["equities"],
        "modules": {"x_sentiment": True, "social_sentiment": True, "technical_scan": True, "fundamental_validation": True, "macro_context": True},
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
        "filters": {"theme": "defense"},
    },
    "thematic_tech": {
        "intent": "thematic",
        "asset_classes": ["equities"],
        "modules": {"x_sentiment": True, "social_sentiment": True, "technical_scan": True, "fundamental_validation": True, "macro_context": False},
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
        "filters": {"theme": "tech"},
    },
    "thematic_financials": {
        "intent": "thematic",
        "asset_classes": ["equities"],
        "modules": {"x_sentiment": True, "social_sentiment": True, "technical_scan": True, "fundamental_validation": True, "macro_context": True},
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
        "filters": {"theme": "financials"},
    },
    "thematic_healthcare": {
        "intent": "thematic",
        "asset_classes": ["equities"],
        "modules": {"x_sentiment": True, "social_sentiment": True, "technical_scan": True, "fundamental_validation": True, "macro_context": False},
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
        "filters": {"theme": "healthcare"},
    },
    "thematic_real_estate": {
        "intent": "thematic",
        "asset_classes": ["equities"],
        "modules": {"x_sentiment": True, "social_sentiment": True, "technical_scan": True, "fundamental_validation": True, "macro_context": True},
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
        "filters": {"theme": "real_estate"},
    },
    "thematic_uranium": {
        "intent": "thematic",
        "asset_classes": ["equities"],
        "modules": {"x_sentiment": True, "social_sentiment": True, "technical_scan": True, "fundamental_validation": True, "macro_context": True},
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
        "filters": {"theme": "uranium"},
    },
    # ---- TA Screener profiles ----
    "screener_stage2_breakouts": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": False},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "stage2_breakouts",
    },
    "screener_bullish_breakouts": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": False},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "bullish_breakouts",
    },
    "screener_bearish_breakdowns": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": False},
        "risk_framework": "bearish",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "bearish_breakdowns",
    },
    "screener_oversold_bounces": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": True},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "oversold_bounces",
    },
    "screener_overbought_warnings": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": False},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "overbought_warnings",
    },
    "screener_crossover_signals": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": False},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "crossover_signals",
    },
    "screener_momentum_shifts": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": False},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "momentum_shifts",
    },
    "screener_trend_status": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": True},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "trend_status",
    },
    "screener_volume_movers": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": False},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "volume_movers",
    },
    # ---- Fundamental screener profiles ----
    "screener_fundamental_leaders": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": True},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "fundamental_leaders",
    },
    "screener_fundamental_acceleration": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": True},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "fundamental_acceleration",
    },
    "screener_insider_buying": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": True},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "insider_buying",
    },
    "screener_revenue_reaccelerating": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": True},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "revenue_reaccelerating",
    },
    "screener_margin_expansion": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": True},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "margin_expansion",
    },
    "screener_undervalued_growth": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": True},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "undervalued_growth",
    },
    "screener_institutional_accumulation": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": True},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "institutional_accumulation",
    },
    "screener_free_cash_flow_leaders": {
        "intent": "deterministic_screener",
        "asset_classes": ["equities"],
        "modules": {"technical_scan": True, "fundamental_validation": True},
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
        "_screener_preset": "free_cash_flow_leaders",
    },
    # ---- Buzz / Social profiles ----
    "news_leaders": {
        "intent": "cross_asset_trending",
        "asset_classes": ["equities", "crypto"],
        "modules": {
            "x_sentiment": True,
            "x_social_scan": True,
            "social_sentiment": True,
            "technical_scan": True,
            "fundamental_validation": False,
            "macro_context": False,
        },
        "risk_framework": "neutral",
        "response_style": "high_conviction_ranked",
        "priority_depth": "medium",
        "x_social_scan_mode": "trending",
    },
    "catalyst_scan": {
        "intent": "event_driven",
        "asset_classes": ["equities"],
        "modules": {
            "x_sentiment": True,
            "social_sentiment": True,
            "technical_scan": True,
            "fundamental_validation": True,
            "macro_context": False,
            "earnings_data": True,
        },
        "risk_framework": "neutral",
        "response_style": "high_conviction_ranked",
        "priority_depth": "medium",
    },
    # ---- Other ----
    "microcap_spec": {
        "intent": "cross_asset_trending",
        "asset_classes": ["equities"],
        "modules": {
            "x_sentiment": True,
            "social_sentiment": True,
            "technical_scan": True,
            "fundamental_validation": True,
            "macro_context": False,
        },
        "filters": {"market_cap_max": 500000000},
        "risk_framework": "asymmetric",
        "response_style": "deep_thesis",
        "priority_depth": "deep",
    },
    # ---- Prediction Markets ----
    "prediction_markets": {
        "intent": "prediction_markets",
        "asset_classes": ["equities", "crypto", "commodities", "macro"],
        "modules": {
            "x_sentiment": False,
            "social_sentiment": False,
            "technical_scan": False,
            "fundamental_validation": False,
            "macro_context": True,
            "liquidity_filter": False,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "full_thesis",
        "priority_depth": "deep",
    },
    "news_intelligence": {
        "intent": "news_intelligence",
        "asset_classes": ["equities", "crypto", "commodities", "macro"],
        "modules": {
            "x_sentiment": True,
            "social_sentiment": True,
            "technical_scan": False,
            "fundamental_validation": False,
            "macro_context": True,
            "liquidity_filter": False,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "full_thesis",
        "priority_depth": "deep",
    },
    # ---- Hyperliquid perpetuals/spot screener ----
    "hyperliquid_screener": {
        "intent": "hyperliquid",
        "asset_classes": ["crypto"],
        "modules": {
            "x_sentiment": False,
            "social_sentiment": False,
            "technical_scan": False,
            "fundamental_validation": False,
            "macro_context": False,
            "liquidity_filter": False,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "screener_table",
        "priority_depth": "medium",
    },
    # ---- Whale watch / 13F institutional tracker ----
    "whale_watch": {
        "intent": "whale_watch",
        "asset_classes": ["equities"],
        "modules": {
            "x_sentiment": False,
            "social_sentiment": False,
            "technical_scan": False,
            "fundamental_validation": False,
            "macro_context": False,
            "liquidity_filter": False,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
    },
    # ---- Insider activity (Form 4) ----
    "insider_activity": {
        "intent": "insider_activity",
        "asset_classes": ["equities"],
        "modules": {
            "x_sentiment": False,
            "social_sentiment": False,
            "technical_scan": False,
            "fundamental_validation": False,
            "macro_context": False,
            "liquidity_filter": False,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
    },
    # ---- Social X dashboard (zero Grok calls, cached snapshots only) ----
    "social_x_dashboard": {
        "intent": "social_x",
        "asset_classes": ["equities"],
        "modules": {
            "x_sentiment": False,
            "social_sentiment": False,
            "technical_scan": False,
            "fundamental_validation": False,
            "macro_context": False,
            "liquidity_filter": False,
            "earnings_data": False,
            "ticker_research": False,
        },
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
    },
}


# ---------------------------------------------------------------------------
# Supporting maps and constants
# Source: TradingAgent class-level attrs (claude_agent.py lines 2588-2640)
# ---------------------------------------------------------------------------
INTENT_TO_CATEGORY: dict[str, str] = {
    "cross_asset_trending": "cross_asset_trending",
    "single_asset_scan": "market_scan",
    "deep_dive": "ticker_analysis",
    "sector_rotation": "sector_rotation",
    "macro_outlook": "macro",
    "portfolio_review": "portfolio_review",
    "event_driven": "earnings_catalyst",
    "thematic": "thematic",
    "investment_ideas": "investments",
    "briefing": "briefing",
    "x_social_scan": "social_momentum",
    "x_trader_consensus": "x_trader_consensus",
    "x_select_trader_consensus": "x_select_trader_consensus",
    "custom_screen": "custom_screen",
    "short_setup": "bearish",
    "best_trades": "best_trades",
    "deterministic_screener": "deterministic_screener",
    "chat": "chat",
    "prediction_markets": "prediction_markets",
    "news_intelligence": "news_intelligence",
    "crypto": "crypto",
    "commodities": "commodities",
    # New data-router arms
    "hyperliquid": "hyperliquid",
    "whale_watch": "whale_watch",
    "insider_activity": "insider_activity",
    "social_x": "social_x",
}

ASSET_CLASS_CATEGORY_MAP: dict[str, str] = {
    "equities": "market_scan",
    "crypto": "crypto",
    "commodities": "commodities",
    "macro": "macro",
}

VALID_INTENTS: set[str] = set(INTENT_TO_CATEGORY.keys())

DEFAULT_PLAN: dict = {
    "intent": "cross_asset_trending",
    "asset_classes": ["equities", "crypto", "commodities", "macro"],
    "modules": {
        "x_sentiment": True,
        "social_sentiment": True,
        "technical_scan": True,
        "fundamental_validation": True,
        "macro_context": True,
        "liquidity_filter": True,
        "earnings_data": False,
        "ticker_research": False,
    },
    "risk_framework": "neutral",
    "response_style": "institutional_brief",
    "priority_depth": "medium",
    "filters": {},
    "tickers": [],
}


# ---------------------------------------------------------------------------
# _CAELYN_ALIAS_MAP  (routing alias table for caelyn_routing.py)
# Source: _ALIAS_MAP in caelyn_routing.py (lines 96-294)
# ---------------------------------------------------------------------------
_CAELYN_ALIAS_MAP: dict[str, str] = {
    # Daily Briefing
    "briefing": "daily_briefing",
    "daily_briefing": "daily_briefing",
    "daily briefing": "daily_briefing",
    "daily": "daily_briefing",
    "market_briefing": "daily_briefing",
    "morning_briefing": "daily_briefing",
    "briefing_dashboard": "daily_briefing",

    # Macro Overview
    "macro_overview": "macro_overview",
    "macro overview": "macro_overview",
    "macro": "macro_overview",
    "macroeconomic": "macro_overview",
    "macro_snapshot": "macro_overview",
    "global_macro": "macro_overview",
    "macro_outlook": "macro_overview",
    "economy": "macro_overview",

    # Headlines
    "headlines": "headlines",
    "news": "headlines",
    "market_news": "headlines",
    "newsfeed": "headlines",
    "news_intelligence": "headlines",
    "notifai": "headlines",
    "news_analysis": "headlines",
    "news_markets": "headlines",

    # Upcoming Catalysts
    "upcoming_catalysts": "upcoming_catalysts",
    "catalysts": "upcoming_catalysts",
    "earnings_catalyst": "upcoming_catalysts",
    "earnings_agent": "upcoming_catalysts",
    "earnings": "upcoming_catalysts",
    "catalyst": "upcoming_catalysts",
    "upcoming catalysts": "upcoming_catalysts",

    # Trending Now
    "trending_now": "trending_now",
    "trending": "trending_now",
    "trending now": "trending_now",
    "cross_asset_trending": "trending_now",
    "cross_asset": "trending_now",
    "cross_market": "trending_now",
    "trending_scan": "trending_now",
    "whats_hot": "trending_now",

    # Social Momentum
    "social_momentum": "social_momentum",
    "social momentum": "social_momentum",
    "social": "social_momentum",
    "social_scan": "social_momentum",
    "sentiment": "social_momentum",
    "wsb": "social_momentum",
    "reddit": "social_momentum",

    # X Trader Consensus (broader / top traders)
    "x_trader_consensus": "x_trader_consensus",
    "trader_consensus": "x_trader_consensus",
    "top_traders": "x_trader_consensus",
    "consensus_tickers": "x_trader_consensus",
    "x_consensus": "x_trader_consensus",

    # X Select Trader Consensus (curated 25-account list)
    "x_select_trader_consensus": "x_select_trader_consensus",
    "select_traders": "x_select_trader_consensus",
    "select_trader_consensus": "x_select_trader_consensus",
    "curated_traders": "x_select_trader_consensus",
    "x_select_consensus": "x_select_trader_consensus",

    # Sector Rotation
    "sector_rotation": "sector_rotation",
    "sector rotation": "sector_rotation",
    "rotation": "sector_rotation",
    "sector_scan": "sector_rotation",

    # Best Trades
    "best_trades": "best_trades",
    "best trades": "best_trades",
    "trades": "best_trades",
    "market_scan": "best_trades",
    "breakout": "best_trades",
    "trade_ideas": "best_trades",
    "setups": "best_trades",
    "trade_setups": "best_trades",

    # Best Investments
    "best_investments": "best_investments",
    "best investments": "best_investments",
    "investments": "best_investments",
    "investment_ideas": "best_investments",
    "long_term": "best_investments",
    "long_term_conviction": "best_investments",
    "sqglp": "best_investments",

    # Asymmetric R:R
    "asymmetric_rr": "asymmetric_rr",
    "asymmetric": "asymmetric_rr",
    "asymmetric r:r": "asymmetric_rr",
    "asymmetric rr": "asymmetric_rr",
    "risk_reward": "asymmetric_rr",

    # Small Cap Spec
    "small_cap_spec": "small_cap_spec",
    "small_cap": "small_cap_spec",
    "small cap spec": "small_cap_spec",
    "small cap": "small_cap_spec",
    "small_cap_speculation": "small_cap_spec",
    "microcap": "small_cap_spec",
    "microcap_spec": "small_cap_spec",

    # Short Squeeze
    "short_squeeze": "short_squeeze",
    "squeeze": "short_squeeze",
    "short squeeze": "short_squeeze",
    "squeeze_plays": "short_squeeze",

    # Fundamental
    "fundamental_leaders": "fundamental_leaders",
    "fundamentals_scan": "fundamental_leaders",
    "fundamentals": "fundamental_leaders",
    "fundamental": "fundamental_leaders",
    "rapidly_improving": "rapidly_improving",
    "revenue_reaccelerating": "revenue_reaccelerating",
    "margin_expansion": "margin_expansion",
    "undervalued_growth": "undervalued_growth",
    "institutional_accumulation": "institutional_accumulation",
    "free_cash_flow_leaders": "free_cash_flow_leaders",
    "earnings_watch": "earnings_watch",
    "insider_buying": "insider_buying",
    "insider": "insider_buying",

    # Crypto
    "crypto": "crypto",
    "cryptocurrency": "crypto",
    "crypto_scan": "crypto",
    "crypto_scanner": "crypto",
    "crypto_focus": "crypto",

    # Commodities
    "commodities": "commodities",
    "commodity": "commodities",
    "commodity_scan": "commodities",
    "commodity_focus": "commodities",
    "commodities_focus": "commodities",

    # Energy
    "energy": "energy",
    "sector_energy": "energy",
    "energy_focus": "energy",

    # Materials
    "materials": "materials",
    "sector_materials": "materials",
    "materials_focus": "materials",

    # Aerospace / Defense
    "aerospace_defense": "aerospace_defense",
    "aerospace": "aerospace_defense",
    "defense": "aerospace_defense",
    "sector_defense": "aerospace_defense",
    "aerospace_focus": "aerospace_defense",

    # Tech
    "tech": "tech",
    "technology": "tech",
    "sector_tech": "tech",
    "tech_focus": "tech",

    # AI / Compute
    "ai_compute": "ai_compute",
    "ai": "ai_compute",
    "ai/compute": "ai_compute",
    "sector_ai": "ai_compute",

    # Quantum
    "quantum": "quantum",
    "quantum_focus": "quantum",
    "sector_quantum": "quantum",

    # Fintech
    "fintech": "fintech",
    "finance_focus": "fintech",
    "sector_financials": "fintech",

    # Biotech
    "biotech": "biotech",
    "biopharma": "biotech",
    "sector_healthcare": "biotech",
    "healthcare_focus": "biotech",

    # Real Estate
    "real_estate": "real_estate",
    "reits": "real_estate",
    "sector_real_estate": "real_estate",
    "real_estate_focus": "real_estate",
}


# ---------------------------------------------------------------------------
# Public resolution functions
# ---------------------------------------------------------------------------

@traceable(name="resolve_preset")
def resolve_preset(preset_intent: str) -> str | None:
    """
    Resolve a raw preset_intent string to a canonical INTENT_PROFILES key.

    Resolution order (must match original TradingAgent._resolve_preset exactly):
      1. Direct hit in INTENT_PROFILES
      2. PRESET_ALIASES lookup
      3. Normalized (lowercase / underscored) hit in INTENT_PROFILES
      4. Normalized lookup in PRESET_ALIASES
    """
    if preset_intent in INTENT_PROFILES:
        return preset_intent
    resolved = PRESET_ALIASES.get(preset_intent)
    if resolved:
        print(f"[ROUTING] Resolved preset alias '{preset_intent}' → '{resolved}'")
        return resolved
    normalized = preset_intent.lower().replace("-", "_").replace(" ", "_")
    if normalized in INTENT_PROFILES:
        return normalized
    resolved = PRESET_ALIASES.get(normalized)
    if resolved:
        print(f"[ROUTING] Resolved normalized preset '{normalized}' → '{resolved}'")
        return resolved
    print(f"[ROUTING] Unknown preset_intent: '{preset_intent}' (normalized: '{normalized}') — no alias or profile found")
    return None


@traceable(name="build_plan_from_preset")
def build_plan_from_preset(preset_intent: str) -> dict | None:
    """
    Build a complete plan dict from a raw preset_intent string.
    Returns None if the preset cannot be resolved.
    Mirrors TradingAgent._build_plan_from_preset exactly.
    """
    resolved = resolve_preset(preset_intent)
    if not resolved:
        return None
    profile = INTENT_PROFILES[resolved]

    plan = {
        "intent": profile["intent"],
        "asset_classes": list(profile["asset_classes"]),
        "modules": dict(profile["modules"]),
        "risk_framework": profile.get("risk_framework", "neutral"),
        "response_style": profile.get("response_style", "institutional_brief"),
        "priority_depth": profile.get("priority_depth", "medium"),
        "filters": dict(profile.get("filters", {})),
        "tickers": [],
    }
    if "x_social_scan_mode" in profile:
        plan["x_social_scan_mode"] = profile["x_social_scan_mode"]
    if "_screener_preset" in profile:
        plan["_screener_preset"] = profile["_screener_preset"]
    return plan


@traceable(name="plan_to_query_info")
def plan_to_query_info(plan: dict) -> dict:
    """
    Convert a resolved plan dict into a query_info dict for _gather_data.
    Mirrors TradingAgent._plan_to_query_info exactly.
    Pure function — no I/O except the optional EDGAR universe side-effect.
    """
    intent = plan.get("intent", "cross_asset_trending")
    category = INTENT_TO_CATEGORY.get(intent, "market_scan")

    asset_classes = plan.get("asset_classes", ["equities"])

    if intent == "single_asset_scan" and len(asset_classes) == 1:
        ac = asset_classes[0]
        category = ASSET_CLASS_CATEGORY_MAP.get(ac, "market_scan")

    if intent == "cross_asset_trending":
        if plan.get("x_social_scan_mode") == "cross_asset":
            category = "cross_asset_trending"
        elif len(asset_classes) >= 2 and set(asset_classes) != {"equities"}:
            modules = plan.get("modules", {})
            has_social = modules.get("x_sentiment") or modules.get("social_sentiment")
            if has_social:
                category = "trending"
            else:
                category = "cross_market"
        else:
            category = "trending"

    if intent == "single_asset_scan":
        modules = plan.get("modules", {})
        if modules.get("social_sentiment") or modules.get("x_sentiment"):
            if category == "market_scan":
                category = "social_momentum"

    filters = plan.get("filters", {})
    tickers = plan.get("tickers", [])

    # Track queried tickers for EDGAR background cache universe
    if tickers:
        try:
            from data.edgar_cache import add_to_universe
            add_to_universe(tickers)
        except Exception:
            pass

    query_info: dict = {
        "category": category,
        "filters": filters,
        "orchestration_plan": plan,
    }
    if tickers:
        query_info["tickers"] = tickers
    if plan.get("_screener_preset"):
        query_info["_screener_preset"] = plan["_screener_preset"]

    return query_info


def resolve_to_route_key(raw: str) -> str | None:
    """
    Resolve any raw string to a canonical CAELYN_ROUTES key.
    Used by caelyn_routing.normalize_route_key() as the alias backend.
    Mirrors the _normalize + _ALIAS_MAP logic from caelyn_routing.py exactly.
    """
    n = raw.lower().strip().replace("-", "_").replace(" ", "_").replace("/", "_")
    if n in _CAELYN_ALIAS_MAP:
        return _CAELYN_ALIAS_MAP[n]
    # Strip common suffixes and retry (mirrors caelyn_routing suffix-strip logic)
    for suffix in ("_scan", "_ideas", "_mode", "_preset", "_dashboard"):
        if n.endswith(suffix):
            stripped = n[: -len(suffix)]
            if stripped in _CAELYN_ALIAS_MAP:
                return _CAELYN_ALIAS_MAP[stripped]
    return None
