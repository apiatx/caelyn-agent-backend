"""
Investor Mode — Market Theme Classifier.

For each Polymarket market (enriched dict from PolymarketIntelligence),
computes equity-relevance scores and maps the market to one or more
equity-relevant macro themes.

Pure function — no I/O, no caching, no HTTP.
Feed in an enriched market dict and get back a classified market dict.
"""

from __future__ import annotations

import re
from typing import Optional

from services.predict.investor.themes import THEMES, ThemeDef, THEME_BY_ID

# ── Relevance scoring tuning ──────────────────────────────────────────────────

# Minimum equity_relevance_score to include a market in investor analysis
EQUITY_RELEVANCE_MIN = 20

# Minimum theme match score to include a theme in a market's theme list
THEME_MATCH_MIN = 15

# Scoring weights for keyword matches
_QUESTION_KW_SCORE = 12    # per keyword hit in question
_TAG_KW_SCORE = 18          # per keyword hit in tags (more reliable)
_CATEGORY_SCORE = 20        # category exact match bonus


def _clean(text: str) -> str:
    return text.lower().strip()


def _keyword_hits(text: str, keywords: list[str]) -> int:
    """Count how many keywords appear as word-boundary matches in text."""
    hits = 0
    for kw in keywords:
        # Use simple substring for multi-word keywords, word-boundary for single
        if " " in kw:
            if kw in text:
                hits += 1
        else:
            if re.search(r"\b" + re.escape(kw) + r"\b", text):
                hits += 1
    return hits


def _score_theme(theme: ThemeDef, question_lower: str, tags_lower: str, category_lower: str) -> float:
    """Return a raw match score for this theme against the market's text fields."""
    score = 0.0

    # Question keyword hits
    q_hits = _keyword_hits(question_lower, theme.question_keywords)
    score += q_hits * _QUESTION_KW_SCORE

    # Tag keyword hits
    t_hits = _keyword_hits(tags_lower, theme.tag_keywords)
    score += t_hits * _TAG_KW_SCORE

    # Category exact match
    if any(_clean(cat) == category_lower for cat in theme.categories):
        score += _CATEGORY_SCORE

    # Apply theme-level boost
    if score > 0:
        score += theme.base_relevance_boost

    return min(100.0, score)


def classify_market(market: dict) -> dict:
    """
    Classify a single enriched Polymarket market dict into equity-relevant themes.

    Adds the following keys to a COPY of the market dict:
        equity_relevance_score   (0-100)
        sector_relevance_score   (0-100)
        regime_relevance_score   (0-100)
        themes                   list[dict]:
            {
              theme_id, theme_name, match_score,
              equity_relevance_score, sector_relevance_score, regime_relevance_score,
              category_relevance_tags, impact_direction_hint
            }
        is_equity_relevant       bool
        primary_theme_id         str | None
    """
    m = dict(market)

    question = _clean(m.get("question", ""))
    tags_raw = m.get("tags") or []
    tags = _clean(" ".join(str(t) for t in tags_raw))
    category = _clean(m.get("category") or "")

    matched_themes: list[dict] = []

    for theme in THEMES:
        raw_score = _score_theme(theme, question, tags, category)
        if raw_score < THEME_MATCH_MIN:
            continue

        # Normalize to 0-100
        theme_equity = min(100.0, raw_score)

        # Sector relevance: slightly lower than equity (more specific) unless tag-driven
        sector_rel = min(100.0, raw_score * 0.85)

        # Regime relevance: highest for macro/geopolitics themes
        regime_multiplier = 1.1 if theme.theme_id in (
            "macro_rates_inflation", "geopolitics_war_trade", "china_taiwan_supply_chain",
            "energy_commodities"
        ) else 0.9
        regime_rel = min(100.0, raw_score * regime_multiplier)

        # Impact direction hint: infer from 24h price change direction
        price_24h = m.get("price_change_1d", 0) or 0
        yes_pct = m.get("yes_pct", 50) or 50
        if price_24h > 1.5:
            direction_hint = "rising"
        elif price_24h < -1.5:
            direction_hint = "falling"
        else:
            direction_hint = "neutral"

        # Category relevance tags = theme tags that appear in the market's actual tags
        cat_tags = [
            kw for kw in theme.tag_keywords
            if kw in tags
        ]

        matched_themes.append({
            "theme_id": theme.theme_id,
            "theme_name": theme.theme_name,
            "match_score": round(raw_score, 1),
            "equity_relevance_score": round(theme_equity, 1),
            "sector_relevance_score": round(sector_rel, 1),
            "regime_relevance_score": round(regime_rel, 1),
            "category_relevance_tags": cat_tags,
            "impact_direction_hint": direction_hint,
        })

    # Sort by match score descending
    matched_themes.sort(key=lambda x: x["match_score"], reverse=True)

    # Aggregate equity/sector/regime scores across all matched themes
    if matched_themes:
        # Top theme drives the score, secondary themes contribute less
        scores = [t["equity_relevance_score"] for t in matched_themes]
        equity_score = scores[0] + sum(s * 0.2 for s in scores[1:3])
        equity_score = min(100.0, equity_score)

        sector_scores = [t["sector_relevance_score"] for t in matched_themes]
        sector_score = sector_scores[0] + sum(s * 0.15 for s in sector_scores[1:3])
        sector_score = min(100.0, sector_score)

        regime_scores = [t["regime_relevance_score"] for t in matched_themes]
        regime_score = regime_scores[0] + sum(s * 0.15 for s in regime_scores[1:3])
        regime_score = min(100.0, regime_score)
    else:
        equity_score = 0.0
        sector_score = 0.0
        regime_score = 0.0

    primary_theme_id = matched_themes[0]["theme_id"] if matched_themes else None

    m["equity_relevance_score"] = round(equity_score, 1)
    m["sector_relevance_score"] = round(sector_score, 1)
    m["regime_relevance_score"] = round(regime_score, 1)
    m["themes"] = matched_themes
    m["is_equity_relevant"] = equity_score >= EQUITY_RELEVANCE_MIN
    m["primary_theme_id"] = primary_theme_id

    return m


def classify_markets(markets: list[dict]) -> list[dict]:
    """Classify a list of markets.  Returns a list of classified market dicts."""
    return [classify_market(m) for m in markets]


def filter_equity_relevant(classified: list[dict]) -> list[dict]:
    """Return only markets that cleared the equity relevance threshold."""
    return [m for m in classified if m.get("is_equity_relevant")]
