"""
catalyst_alignment.py — Clean Catalyst Alignment (zero-provider-call)
======================================================================
Deterministic, cache-only Catalyst signal for the THEME_ALIGNMENT Trade
Alignment archetype (see confluence_v2_service.py).

Excludes the OLD catalyst_engine.py components on purpose (per spec):
    volume_expansion, social_acceleration, fundamental_acceleration
  — those are NOT real "catalysts", they are re-derived technical /
    social / fundamental signals that already exist elsewhere in the
    pipeline (Options Alignment, Social Bonus, Stage/Entry signals).

A "catalyst" here means a real, dated, external EVENT:
    • Earnings report (this week, cached)
    • IPO / Dividend / Split calendar event (this week, cached)
    • Material company news (regulatory / M&A / guidance / legal /
      analyst action / corporate action) detected via the existing RSS
      article archive (Neon), using only already-collected data.

Sources consumed (ALL pure reads of already-persisted data — verified
directly by reading each source module; none fetch a provider on a
cache miss):
    • services.top_catalysts_service.get_top_catalysts()
          -> earnings:curated:week:{from}:{to} cache (data.cache)
    • services.calendar_snapshot_service.get_snapshot("ipos"/"dividends"/"splits")
          -> Neon calendar_snapshots table (falls back to disk JSON)
    • backend.data.rss_article_archive.query_ticker_activity /
      query_ticker_activity_articles
          -> Neon watchlist_rss_article_archive table

Zero provider calls. Zero LLM calls. Zero new schedulers.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

# ── Catalyst category base scores (0-100) ──────────────────────────────────
_SCORE_EARNINGS_THIS_WEEK      = 65.0
_SCORE_EARNINGS_IMMINENT       = 80.0   # today / tomorrow
_SCORE_IPO_THIS_WEEK           = 55.0
_SCORE_DIVIDEND_THIS_WEEK      = 40.0
_SCORE_SPLIT_THIS_WEEK         = 50.0
_SCORE_MA_NEWS                 = 80.0
_SCORE_REGULATORY_NEWS         = 75.0
_SCORE_GUIDANCE_NEWS           = 60.0
_SCORE_LEGAL_NEWS              = 55.0
_SCORE_CORPORATE_ACTION        = 50.0
_SCORE_ANALYST_ACTION          = 45.0

_NEWS_LOOKBACK_HOURS = 96

# ── Keyword categories for material-news classification ────────────────────
# Order matters: first match wins (most-specific / highest-signal first).
_NEWS_CATEGORIES: list[tuple[str, float, re.Pattern[str]]] = [
    ("MA_NEWS", _SCORE_MA_NEWS, re.compile(
        r"\b(acqui(?:re|res|sition|ring)|merger|merges?\s+with|takeover|"
        r"to\s+be\s+acquired|buyout|tender\s+offer)\b", re.I)),
    ("REGULATORY_NEWS", _SCORE_REGULATORY_NEWS, re.compile(
        r"\b(fda|sec\s+(?:probe|investigation|filing)|approv(?:al|ed|es)|"
        r"clearance|recall|warning\s+letter|complete\s+response\s+letter|"
        r"\bcrl\b|phase\s+(?:1|2|3|i|ii|iii)\b.*(?:trial|results)|"
        r"antitrust|doj\b)\b", re.I)),
    ("LEGAL_NEWS", _SCORE_LEGAL_NEWS, re.compile(
        r"\b(lawsuit|class\s+action|litigation|settl(?:e|ement|es)|"
        r"sues?|sued|subpoena)\b", re.I)),
    ("GUIDANCE_NEWS", _SCORE_GUIDANCE_NEWS, re.compile(
        r"\b(guidance|raises?\s+(?:its\s+)?(?:full[-\s]?year\s+)?outlook|"
        r"cuts?\s+(?:its\s+)?(?:full[-\s]?year\s+)?outlook|forecast|"
        r"preliminary\s+results|profit\s+warning)\b", re.I)),
    ("CORPORATE_ACTION", _SCORE_CORPORATE_ACTION, re.compile(
        r"\b(buyback|share\s+repurchase|partnership|strategic\s+alliance|"
        r"contract\s+win|awarded\s+a\s+contract|spin[-\s]?off|"
        r"names?\s+new\s+ceo|ceo\s+(?:resign|steps?\s+down|departure)|"
        r"appoints?\s+.*(?:ceo|cfo|president))\b", re.I)),
    ("ANALYST_ACTION", _SCORE_ANALYST_ACTION, re.compile(
        r"\b(upgrades?|downgrades?|price\s+target|initiates?\s+coverage|"
        r"reiterates?)\b", re.I)),
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _et_today() -> date:
    return datetime.now(timezone.utc).date()


# ── Calendar-source loaders (pure cache reads) ──────────────────────────────

def _load_earnings_this_week() -> dict[str, dict]:
    """
    symbol -> {"date": "YYYY-MM-DD", "score_hint": float}

    Reuses services.top_catalysts_service.get_top_catalysts(), which is
    itself a pure read of the earnings:curated:week:* cache. Never fetches
    FMP at request time (see module docstring in top_catalysts_service.py).
    """
    out: dict[str, dict] = {}
    try:
        from services.top_catalysts_service import get_top_catalysts
        env = get_top_catalysts()
        for day in env.get("days") or []:
            d = day.get("date")
            for ev in day.get("earnings") or []:
                sym = (ev.get("symbol") or "").upper()
                if not sym:
                    continue
                out[sym] = {"date": d, "companyName": ev.get("companyName") or sym}
    except Exception:
        pass
    return out


def _load_calendar_events_this_week() -> dict[str, list[dict]]:
    """
    symbol -> [{"kind": "ipo"|"dividend"|"split", "date": "YYYY-MM-DD"}]

    Reuses services.calendar_snapshot_service.get_snapshot(), which is
    documented to "Never trigger FMP" — reads Neon (or disk fallback) only.
    """
    out: dict[str, list[dict]] = {}
    try:
        from services.calendar_snapshot_service import get_snapshot
    except Exception:
        return out

    monday = _et_today() - timedelta(days=_et_today().weekday())
    friday = monday + timedelta(days=4)

    for tab, kind in (("ipos", "ipo"), ("dividends", "dividend"), ("splits", "split")):
        try:
            env = get_snapshot(tab) or {}
        except Exception:
            continue
        for ev in env.get("current_week") or []:
            if not isinstance(ev, dict):
                continue
            sym = (ev.get("symbol") or "").upper()
            d_raw = (ev.get("date") or "")[:10]
            if not sym or not d_raw:
                continue
            try:
                d = datetime.strptime(d_raw, "%Y-%m-%d").date()
            except Exception:
                continue
            if d < monday or d > friday:
                continue
            out.setdefault(sym, []).append({"kind": kind, "date": d_raw})
    return out


# ── News-source loader (pure cache read of already-persisted RSS archive) ──

def _load_news_activity_counts(symbols: list[str]) -> dict[str, dict]:
    """Bulk (single-query) 48h/prev-48h article counts for `symbols`."""
    try:
        from data.rss_article_archive import query_ticker_activity
        return query_ticker_activity(symbols)
    except Exception:
        return {}


def _classify_news_titles(articles: list[dict]) -> Optional[tuple[str, float, dict]]:
    """
    Scan article titles/summaries for the highest-scoring matched category.
    Returns (category, score, matched_article) or None.
    """
    best: Optional[tuple[str, float, dict]] = None
    for art in articles:
        bag = f"{art.get('title') or ''} {art.get('summary') or ''}"
        if not bag.strip():
            continue
        for cat, score, pat in _NEWS_CATEGORIES:
            if pat.search(bag):
                if best is None or score > best[1]:
                    best = (cat, score, art)
                break  # first (highest-priority) category match per article
    return best


def _news_catalyst_for_symbol(sym: str, has_recent_activity: bool) -> Optional[tuple[str, float, dict]]:
    """Per-symbol title fetch — only called for symbols with articles_48h > 0
    (bulk-counted first), to avoid one Neon round-trip per universe symbol."""
    if not has_recent_activity:
        return None
    try:
        from data.rss_article_archive import query_ticker_activity_articles
        articles, _first_seen = query_ticker_activity_articles(sym, window_hours=_NEWS_LOOKBACK_HOURS)
    except Exception:
        return None
    if not articles:
        return None
    return _classify_news_titles(articles)


# ── Public API ───────────────────────────────────────────────────────────────

def get_catalyst_alignment_bulk(symbols: list[str]) -> dict[str, dict]:
    """
    Compute the clean Catalyst Alignment result for every symbol in
    `symbols`. Zero provider calls (all sources are pre-persisted caches).

    Returns {SYMBOL: {
        catalyst_alignment_score:      float | None (0-100),
        catalyst_alignment_available:  bool,
        primary_catalyst:              dict | None,
        catalyst_events:               list[dict],
        catalyst_alignment_reason:     str,
    }}
    """
    syms = [s.upper() for s in symbols]
    earnings_map  = _load_earnings_this_week()
    calendar_map  = _load_calendar_events_this_week()
    news_counts   = _load_news_activity_counts(syms)

    today = _et_today()
    out: dict[str, dict] = {}

    for sym in syms:
        events: list[dict] = []

        # 1) Earnings this week
        earn = earnings_map.get(sym)
        if earn:
            try:
                ed = datetime.strptime(earn["date"], "%Y-%m-%d").date()
                days_out = (ed - today).days
            except Exception:
                days_out = None
            score = _SCORE_EARNINGS_IMMINENT if (days_out is not None and -1 <= days_out <= 1) \
                else _SCORE_EARNINGS_THIS_WEEK
            events.append({
                "type":        "EARNINGS_THIS_WEEK",
                "date":        earn["date"],
                "description": f"Earnings report on {earn['date']}",
                "score":       score,
            })

        # 2) IPO / dividend / split calendar events this week
        for ev in calendar_map.get(sym, []):
            kind = ev["kind"]
            score = {
                "ipo":      _SCORE_IPO_THIS_WEEK,
                "dividend": _SCORE_DIVIDEND_THIS_WEEK,
                "split":    _SCORE_SPLIT_THIS_WEEK,
            }[kind]
            events.append({
                "type":        f"{kind.upper()}_THIS_WEEK",
                "date":        ev["date"],
                "description": f"{kind.capitalize()} calendar event on {ev['date']}",
                "score":       score,
            })

        # 3) Material news (regulatory / M&A / guidance / legal / analyst / corp)
        counts = news_counts.get(sym) or {}
        has_recent = int(counts.get("articles_48h") or 0) > 0
        news_hit = _news_catalyst_for_symbol(sym, has_recent)
        if news_hit:
            cat, score, art = news_hit
            events.append({
                "type":        cat,
                "date":        (art.get("published_at") or "")[:10],
                "description": art.get("title") or cat,
                "score":       score,
                "url":         art.get("url"),
            })

        if not events:
            out[sym] = {
                "catalyst_alignment_score":     None,
                "catalyst_alignment_available": False,
                "primary_catalyst":             None,
                "catalyst_events":              [],
                "catalyst_alignment_reason":    "CATALYST_UNAVAILABLE_NO_EVENTS",
            }
            continue

        events.sort(key=lambda e: -e["score"])
        primary = events[0]
        out[sym] = {
            "catalyst_alignment_score":     round(primary["score"], 1),
            "catalyst_alignment_available": True,
            "primary_catalyst":             primary,
            "catalyst_events":              events[:5],
            "catalyst_alignment_reason":    "CATALYST_EVENT_DETECTED",
        }

    return out


def get_catalyst_alignment_for_ticker(symbol: str) -> dict:
    result = get_catalyst_alignment_bulk([symbol])
    return result.get(symbol.upper()) or {
        "catalyst_alignment_score":     None,
        "catalyst_alignment_available": False,
        "primary_catalyst":             None,
        "catalyst_events":              [],
        "catalyst_alignment_reason":    "CATALYST_UNAVAILABLE_NO_EVENTS",
    }
