"""
Caelyn mode — deterministic category-based routing matrix (Phase 1 MVP).

This module is the ONLY place where Caelyn automatic routing logic lives.
It maps a resolved category/preset to:
  - final:         the final reasoning model (writes the response)
  - collaborators: list of models that gather targeted info before final synthesis
  - mode:          "fast" | "standard" | "deep" (advisory depth hint)

ARCHITECTURAL CONTRACT:
  - _gather_data_safe and all proprietary data pipelines run BEFORE this routing fires.
  - Collaborators are ADDITIVE to the assembled proprietary market_data.
  - This module has zero side effects — pure lookup only.
  - When reasoning_model != "agent_collab", this module is never called.
  - When collab_agents is explicitly set by the user (Customize mode), this module
    is skipped — the user's explicit choices override the matrix.
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

# ── Routing matrix ────────────────────────────────────────────────────────────
# Keys are normalized route identifiers.
# final:        internal model id for the final reasoning/synthesis model
# collaborators: list of models that do domain-specific info gathering first
# mode:          advisory depth hint ("fast" | "standard" | "deep")

CAELYN_ROUTES: dict[str, dict] = {
    # OVERVIEW
    "daily_briefing":       {"final": "claude",      "collaborators": ["perplexity", "grok"],              "mode": "standard"},
    "macro_overview":       {"final": "claude",      "collaborators": ["gemini", "perplexity"],            "mode": "deep"},
    "headlines":            {"final": "perplexity",  "collaborators": [],                                  "mode": "fast"},
    "upcoming_catalysts":   {"final": "claude",      "collaborators": ["perplexity"],                      "mode": "fast"},
    "trending_now":         {"final": "grok",        "collaborators": ["perplexity"],                      "mode": "fast"},
    "social_momentum":      {"final": "grok",        "collaborators": [],                                  "mode": "fast"},
    "x_trader_consensus":        {"final": "grok",   "collaborators": [],                                  "mode": "fast"},
    "x_select_trader_consensus": {"final": "grok",   "collaborators": [],                                  "mode": "fast"},
    "sector_rotation":      {"final": "claude",      "collaborators": ["gemini", "perplexity"],            "mode": "deep"},

    # TRADES & IDEAS
    "best_trades":          {"final": "claude",      "collaborators": ["grok", "perplexity"],              "mode": "standard"},
    "best_investments":     {"final": "claude",      "collaborators": ["gemini", "perplexity"],            "mode": "standard"},
    "asymmetric_rr":        {"final": "claude",      "collaborators": ["gemini", "grok"],                  "mode": "standard"},
    "small_cap_spec":       {"final": "claude",      "collaborators": ["grok", "perplexity"],              "mode": "standard"},
    "short_squeeze":        {"final": "claude",      "collaborators": ["grok", "perplexity"],              "mode": "fast"},

    # FUNDAMENTAL
    "fundamental_leaders":       {"final": "claude", "collaborators": ["gemini"],                          "mode": "standard"},
    "rapidly_improving":         {"final": "claude", "collaborators": ["gemini", "perplexity"],            "mode": "standard"},
    "earnings_watch":            {"final": "claude", "collaborators": ["perplexity", "gemini"],            "mode": "standard"},
    "insider_buying":            {"final": "claude", "collaborators": ["perplexity"],                      "mode": "fast"},
    "revenue_reaccelerating":    {"final": "claude", "collaborators": ["gemini"],                          "mode": "standard"},
    "margin_expansion":          {"final": "claude", "collaborators": ["gemini"],                          "mode": "standard"},
    "undervalued_growth":        {"final": "claude", "collaborators": ["gemini", "perplexity"],            "mode": "standard"},
    "institutional_accumulation":{"final": "claude", "collaborators": ["perplexity", "gemini"],            "mode": "standard"},
    "free_cash_flow_leaders":    {"final": "claude", "collaborators": ["gemini"],                          "mode": "standard"},

    # SECTORS
    # crypto: Grok X sentiment + CoinGecko + CMC + Hyperliquid + altFINS + DeFiLlama + Polymarket
    # are ALL already gathered in parallel inside get_crypto_scanner() before Caelyn fires.
    # Adding collaborators on top forces a second Grok call + Perplexity call that each take
    # 12-90s, making total response time 150s+. Claude gets all the data it needs directly.
    "crypto":               {"final": "claude",      "collaborators": [],                                  "mode": "fast"},
    # commodities: commodity_scan pipeline already pulls ETF proxies + Grok themes + macro data.
    "commodities":          {"final": "claude",      "collaborators": [],                                  "mode": "fast"},
    "energy":               {"final": "claude",      "collaborators": ["perplexity", "gemini"],            "mode": "standard"},
    "materials":            {"final": "claude",      "collaborators": ["gemini", "perplexity"],            "mode": "standard"},
    "aerospace_defense":    {"final": "claude",      "collaborators": ["perplexity", "gemini"],            "mode": "standard"},
    "tech":                 {"final": "claude",      "collaborators": ["gemini", "perplexity"],            "mode": "standard"},
    "ai_compute":           {"final": "claude",      "collaborators": ["grok", "gemini", "perplexity"],    "mode": "deep"},
    "quantum":              {"final": "claude",      "collaborators": ["grok", "gemini", "perplexity"],    "mode": "deep"},
    "fintech":              {"final": "claude",      "collaborators": ["gemini", "perplexity"],            "mode": "standard"},
    "biotech":              {"final": "claude",      "collaborators": ["perplexity", "gemini"],            "mode": "standard"},
    "real_estate":          {"final": "claude",      "collaborators": ["gemini", "perplexity"],            "mode": "standard"},
}

DEFAULT_ROUTE: dict = {
    "final": "claude",
    "collaborators": ["grok", "perplexity"],
    "mode": "standard",
}

# ── Category/preset → route key normalization ─────────────────────────────────
# Maps any string variant the system might use to a canonical route key.
# Covers: preset_intent values, category strings from the classifier, UI labels.

_ALIAS_MAP: dict[str, str] = {
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

    # Headlines — all news_intelligence variants must resolve here
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


@traceable(name="caelyn_routing.normalize")
def _normalize(raw: str) -> str:
    """Lowercase, strip, collapse spaces/dashes/slashes to underscores."""
    return raw.lower().strip().replace("-", "_").replace(" ", "_").replace("/", "_")


@traceable(name="caelyn_routing.normalize_route_key")
def normalize_route_key(preset_intent: str | None, category: str | None) -> str | None:
    """
    Try to resolve a canonical route key from preset_intent first,
    then from category as fallback. Returns None if nothing matches
    (caller should use DEFAULT_ROUTE).
    """
    for raw in (preset_intent, category):
        if not raw:
            continue
        n = _normalize(raw)
        # Direct hit in route table
        if n in CAELYN_ROUTES:
            return n
        # Alias table
        if n in _ALIAS_MAP:
            return _ALIAS_MAP[n]
        # Strip common suffixes and retry
        for suffix in ("_scan", "_ideas", "_mode", "_preset", "_dashboard"):
            if n.endswith(suffix):
                stripped = n[: -len(suffix)]
                if stripped in CAELYN_ROUTES:
                    return stripped
                if stripped in _ALIAS_MAP:
                    return _ALIAS_MAP[stripped]
    return None


@traceable(name="routing")
def get_caelyn_route(preset_intent: str | None, category: str | None) -> dict:
    """
    Return the routing config for a given preset/category in Caelyn mode.
    Always returns a dict with keys: final, collaborators, mode.
    Falls back to DEFAULT_ROUTE on no match.
    """
    key = normalize_route_key(preset_intent, category)
    if key and key in CAELYN_ROUTES:
        route = CAELYN_ROUTES[key]
        print(f"[CAELYN_ROUTING] preset='{preset_intent}' category='{category}' → key='{key}' "
              f"final='{route['final']}' collaborators={route['collaborators']} mode='{route['mode']}'")
        return dict(route)

    print(f"[CAELYN_ROUTING] No route match for preset='{preset_intent}' category='{category}' — using DEFAULT_ROUTE")
    return dict(DEFAULT_ROUTE)


# ── Focused domain prompts for each collaborator ──────────────────────────────
# These are PREPENDED to the user prompt when calling each collaborator.
# They keep each model in its domain and ensure concise, targeted findings.
# The collaborator also receives the full proprietary market_data as context.

COLLAB_DOMAIN_PROMPTS: dict[str, str] = {
    "grok": (
        "You are contributing a focused social sentiment and market narrative analysis. "
        "Use X/Twitter search to identify: current retail and institutional sentiment, "
        "trending narratives, buzz/hype/fear signals, influencer consensus, and any "
        "notable shifts in social momentum relevant to this request. "
        "Provide ONLY your social/narrative findings — do NOT write a full market analysis. "
        "Be concise and specific. Stay under 500 words."
    ),
    "perplexity": (
        "You are contributing a focused news and catalyst analysis. "
        "Search the web for: the most recent relevant news headlines, upcoming catalysts "
        "(earnings, product launches, regulatory events, macro releases), breaking "
        "developments, and any time-sensitive information that may affect this request. "
        "Provide ONLY your news/catalyst findings — do NOT write a full market analysis. "
        "Be concise and cite recency. Stay under 500 words."
    ),
    "gemini": (
        "You are contributing broader market research and contextual background. "
        "Use Google Search to identify: thematic trends, sector dynamics, competitive "
        "positioning, macro/micro structural factors, and any foundational context "
        "that adds depth to this request. "
        "Provide ONLY your research findings — do NOT write a full market analysis. "
        "Be concise and specific. Stay under 500 words."
    ),
    "gpt-4o": (
        "You are contributing a focused web research analysis. "
        "Search the web for: the most relevant current information, recent data points, "
        "expert opinions, and any supporting evidence relevant to this request. "
        "Provide ONLY your research findings — do NOT write a full market analysis. "
        "Be concise and specific. Stay under 500 words."
    ),
}
