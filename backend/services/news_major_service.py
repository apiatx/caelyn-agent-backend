"""
News Major Developments Service

Post-processes a completed news_map (from fetch_news_for_tickers) to:
  1. Deduplicate syndicated near-duplicate articles by normalized title cluster.
  2. Rank all major articles by score → signal_strength priority → recency.
  3. Surface the top-20 unique major developments.
  4. Annotate every article with four additive fields:
       is_top_major_development  bool
       surface_priority          int | None   (1–20 for top-major, else None)
       major_news_rank           int | None   (same as surface_priority)
       duplicate_cluster_key     str | None   (non-None when is_major_development)

Returns (enriched_news_map, major_summary).
enriched_news_map is the same {TICKER: [articles]} shape — only additive fields added.
major_summary is returned separately so the companion endpoint can serve it.
"""

from __future__ import annotations

import re
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any

# ── Stop-word set (for title normalization / cluster key) ─────────────────────

_STOP: frozenset[str] = frozenset({
    "a", "an", "the", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "shall", "can", "ought", "must",
    "to", "of", "in", "on", "at", "for", "with", "by", "from", "up",
    "about", "into", "through", "during", "as", "and", "but", "or",
    "nor", "so", "yet", "its", "it", "this", "that", "these", "those",
    "after", "why", "how", "what", "which", "who", "when", "where",
    "after", "here", "there", "their", "they", "its", "our", "we",
    "new", "one", "two", "first", "last", "s", "inc", "plc", "ltd",
})

_SOURCE_RANK: dict[str, int] = {
    "primary":   4,
    "tier1":     3,
    "syndicated": 2,
    "low":       1,
    "unknown":   0,
}

_SIGNAL_RANK: dict[str, int] = {
    "high":   3,
    "medium": 2,
    "low":    1,
    "none":   0,
}

_RE_PUNCT = re.compile(r"[^a-z0-9\s]")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalize_title(title: str) -> str:
    """
    Lowercase, strip punctuation, drop stop words, keep first 8 significant tokens.
    Used as the primary dedup key (covers 90%+ of syndicated duplicates).
    """
    t = _RE_PUNCT.sub(" ", title.lower())
    tokens = [w for w in t.split() if w not in _STOP and len(w) > 1]
    return "_".join(tokens[:8])


def _cluster_key(title: str, url: str) -> str:
    """
    Return a string cluster key for deduplication.
    Falls back to first 50 chars of url if title is too short after normalizing.
    """
    norm = _normalize_title(title)
    if len(norm) >= 8:
        return norm
    # Fallback: first 50 chars of url (enough to cluster same-url variations)
    return (url or "")[:50].lower().strip()


def _parse_ts(published_at: str) -> float:
    """Parse RFC-2822 or ISO-8601 date to unix timestamp. Returns 0.0 on failure."""
    if not published_at:
        return 0.0
    try:
        return parsedate_to_datetime(published_at).timestamp()
    except Exception:
        pass
    try:
        return datetime.fromisoformat(
            published_at.replace("Z", "+00:00")
        ).timestamp()
    except Exception:
        pass
    return 0.0


def _sort_key(a: dict[str, Any]) -> tuple:
    """Primary sort: score desc → signal_strength desc → recency desc."""
    return (
        -(a.get("major_news_score") or 0),
        -_SIGNAL_RANK.get(a.get("signal_strength") or "none", 0),
        -_parse_ts(a.get("published_at") or ""),
    )


def _is_better(candidate: dict, incumbent: dict) -> bool:
    """
    True when candidate should replace incumbent as cluster representative.
    Prefer higher source_quality, then higher score, then higher signal_strength.
    """
    sq_c = _SOURCE_RANK.get(candidate.get("source_quality") or "unknown", 0)
    sq_i = _SOURCE_RANK.get(incumbent.get("source_quality") or "unknown", 0)
    if sq_c != sq_i:
        return sq_c > sq_i
    sc_c = candidate.get("major_news_score") or 0
    sc_i = incumbent.get("major_news_score") or 0
    if sc_c != sc_i:
        return sc_c > sc_i
    return _parse_ts(candidate.get("published_at") or "") > _parse_ts(
        incumbent.get("published_at") or ""
    )


# ── Main entry point ──────────────────────────────────────────────────────────

def build_major_developments(
    news_map: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    """
    Args:
        news_map  {TICKER: [article_dict, ...]}  already enriched by score_article

    Returns:
        enriched_news_map   same structure, every article gets 4 new additive fields
        major_summary       dict for the /news/major companion endpoint
    """

    # ── 1. Collect all major articles, build cluster map ─────────────────────
    # Use article object id as stable identity within this call.
    major_articles: list[dict[str, Any]] = []
    for articles in news_map.values():
        for a in articles:
            if a.get("is_major_development"):
                major_articles.append(a)

    # Sort globally (best first) so cluster representative = first encountered
    major_articles.sort(key=_sort_key)

    # Deduplicate: cluster_key → best representative article
    cluster_rep: dict[str, dict[str, Any]] = {}   # ck → article
    article_ck:  dict[int, str] = {}              # id(article) → ck

    for a in major_articles:
        ck = _cluster_key(a.get("title") or "", a.get("url") or "")
        article_ck[id(a)] = ck
        if ck not in cluster_rep:
            cluster_rep[ck] = a
        elif _is_better(a, cluster_rep[ck]):
            cluster_rep[ck] = a

    # Top-20 unique representatives, re-sorted
    top_articles = sorted(cluster_rep.values(), key=_sort_key)[:20]
    top_id_set   = {id(a) for a in top_articles}

    # rank_map: id(article) → 1-based rank (only top articles)
    rank_map: dict[int, int] = {id(a): i for i, a in enumerate(top_articles, 1)}

    # Duplicate-removed count
    dups_removed = len(major_articles) - len(cluster_rep)

    # ── 2. Enrich every article with the 4 new fields ─────────────────────────
    enriched_news_map: dict[str, list[dict[str, Any]]] = {}
    total_articles = 0
    total_major    = 0
    high_signal    = 0
    by_catalyst: dict[str, int] = {}

    for ticker, articles in news_map.items():
        enriched: list[dict[str, Any]] = []
        for a in articles:
            total_articles += 1
            is_major = bool(a.get("is_major_development"))
            if is_major:
                total_major += 1
            sig = a.get("signal_strength") or "none"
            if sig == "high":
                high_signal += 1
            ct = a.get("catalyst_type") or "routine"
            by_catalyst[ct] = by_catalyst.get(ct, 0) + 1

            a_id   = id(a)
            ck     = article_ck.get(a_id)           # None for non-major
            rank   = rank_map.get(a_id)             # None unless top-20
            is_top = a_id in top_id_set

            enriched.append({
                **a,
                "is_top_major_development": is_top,
                "surface_priority":         rank,
                "major_news_rank":          rank,
                "duplicate_cluster_key":    ck if is_major else None,
            })
        enriched_news_map[ticker] = enriched

    # ── 3. Build major_summary for companion endpoint ─────────────────────────
    major_dev_list: list[dict[str, Any]] = []
    for rank, a in enumerate(top_articles, 1):
        syms = a.get("related_watchlist_symbols") or []
        major_dev_list.append({
            "rank":               rank,
            "symbol":             syms[0] if syms else None,
            "title":              a.get("title"),
            "major_news_score":   a.get("major_news_score"),
            "major_news_label":   a.get("major_news_label"),
            "catalyst_type":      a.get("catalyst_type"),
            "bull_bear_impact":   a.get("bull_bear_impact"),
            "signal_strength":    a.get("signal_strength"),
            "source_quality":     a.get("source_quality"),
            "source":             a.get("source"),
            "url":                a.get("url"),
            "published_at":       a.get("published_at"),
            "why_it_matters":     a.get("why_it_matters"),
            "matched_entities":   a.get("matched_entities") or [],
            "matched_keywords":   a.get("matched_keywords") or [],
            "summary":            a.get("summary") or "",
        })

    major_summary: dict[str, Any] = {
        "major_developments":       major_dev_list,
        "major_developments_count": len(major_dev_list),
        "high_signal_count":        high_signal,
        "by_catalyst_type":         dict(
            sorted(by_catalyst.items(), key=lambda x: -x[1])
        ),
        "news_signal_meta": {
            "total_articles":             total_articles,
            "total_major_developments":   total_major,
            "total_top_major":            len(top_articles),
            "duplicate_clusters_removed": dups_removed,
            "unique_clusters":            len(cluster_rep),
        },
    }

    return enriched_news_map, major_summary
