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
    "crypto":               {"final": "claude",      "collaborators": ["grok", "perplexity"],              "mode": "standard"},
    "commodities":          {"final": "claude",      "collaborators": ["perplexity", "gemini"],            "mode": "standard"},
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

    # Headlines
    "headlines": "headlines",
    "news": "headlines",
    "market_news": "headlines",
    "newsfeed": "headlines",

    # Upcoming Catalysts
    "upcoming_catalysts": "upcoming_catalysts",
    "catalysts": "upcoming_catalysts",
    "earnings_catalyst": "upcoming_catalysts",
    "catalyst": "upcoming_catalysts",
    "upcoming catalysts": "upcoming_catalysts",

    # Trending Now
    "trending_now": "trending_now",
    "trending": "trending_now",
    "trending now": "trending_now",
    "cross_asset_trending": "trending_now",
    "cross_market": "trending_now",
    "trending_scan": "trending_now",

    # Social Momentum
    "social_momentum": "social_momentum",
    "social momentum": "social_momentum",
    "social": "social_momentum",
    "social_scan": "social_momentum",
    "sentiment": "social_momentum",

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

    # Best Investments
    "best_investments": "best_investments",
    "best investments": "best_investments",
    "investments": "best_investments",
    "investment_ideas": "best_investments",
    "long_term": "best_investments",

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

    # Sectors
    "crypto": "crypto",
    "cryptocurrency": "crypto",
    "crypto_scan": "crypto",
    "commodities": "commodities",
    "commodity": "commodities",
    "energy": "energy",
    "materials": "materials",
    "aerospace_defense": "aerospace_defense",
    "aerospace": "aerospace_defense",
    "defense": "aerospace_defense",
    "tech": "tech",
    "technology": "tech",
    "ai_compute": "ai_compute",
    "ai": "ai_compute",
    "ai/compute": "ai_compute",
    "quantum": "quantum",
    "fintech": "fintech",
    "biotech": "biotech",
    "biopharma": "biotech",
    "real_estate": "real_estate",
    "reits": "real_estate",
}


def _normalize(raw: str) -> str:
    """Lowercase, strip, collapse spaces/dashes/slashes to underscores."""
    return raw.lower().strip().replace("-", "_").replace(" ", "_").replace("/", "_")


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
