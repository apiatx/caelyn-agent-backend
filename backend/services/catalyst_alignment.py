"""
catalyst_alignment.py — Clean Catalyst Alignment (zero-provider-call)
======================================================================
V1: deterministic, cache-only Catalyst signal using 6 regex categories (unchanged).
V2: reuses existing news_signal_scorer.py taxonomy for richer, business-aware,
    directional, materiality-weighted catalyst events (additive shadow fields).

V2.1 additions (no scoring model changes, V1 unchanged):
    • Ticker-subject relevance gate  — articles about other companies are rejected
      or confidence-downweighted before V2 scoring.
    • Primary-subject mismatch check — explicit (TICKER) in title, leading-company
      extraction, and market-index detection all suppress cross-feed contamination.
    • Subsidiary / alias exception   — small hardcoded map for confirmed subsidiary
      relationships (e.g. KTOS→AEVEX).
    • Governance / compensation filter — CFO grants, bylaw amendments, routine
      conference presentations cannot produce COMPELLING/MODERATE V2 scores.
    • Bearish application stricter    — bearish penalties only apply when the article
      is demonstrably about the scored ticker (relevance ≥ 0.75).
    • Performance fix                 — single bulk Neon fetch replaces N per-symbol
      queries; target < 5s for 317 symbols.

Sources consumed (ALL pure reads of already-persisted data — zero provider calls):
    • services.top_catalysts_service.get_top_catalysts()
          -> earnings:curated:week:{from}:{to} cache (data.cache)
    • services.calendar_snapshot_service.get_snapshot("ipos"/"dividends"/"splits")
          -> Neon calendar_snapshots table (falls back to disk JSON)
    • data.rss_article_archive.query_ticker_activity (bulk count, 1 query)
      data.rss_article_archive.query_recent_articles_for_scoring (bulk articles, 1 query)
          -> Neon watchlist_rss_article_archive table
    • services.news_signal_scorer.score_article (V2 only)
          -> pure-Python in-memory scoring (no I/O)

Zero provider calls.  Zero LLM calls.  Zero new schedulers.  Zero new DB tables.
V1 output fields preserved.  THEME_ALIGNMENT primary score unchanged.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

# ── V1 base scores (0-100) ────────────────────────────────────────────────────
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

# ── V1 keyword categories — order matters: first match wins ──────────────────
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

# ── V2 constants ──────────────────────────────────────────────────────────────

_V2_VERSION = "2.1"

# Minimum news_signal_scorer score to be considered a non-routine catalyst
_V2_MIN_SCORE    = 40
# Final V2 score must exceed this to report catalyst_v2_available=True
_V2_AVAIL_THRESHOLD = 25.0

# Corroboration: +2 per additional supporting bullish event, capped at +8
_V2_CORROBORATION_PER = 2.0
_V2_CORROBORATION_CAP = 8.0
# Bearish penalty factor applied to primary bearish event materiality
_V2_BEARISH_FACTOR = 0.70

# Recency decay: (max_age_hours, weight)
_V2_RECENCY: list[tuple[float, float]] = [
    (24.0,  1.00),
    (48.0,  0.82),
    (72.0,  0.65),
    (96.0,  0.50),
]

# Source quality → confidence component
_V2_SOURCE_SCORE: dict[str, float] = {
    "primary":    1.00,
    "tier1":      0.92,
    "syndicated": 0.75,
    "unknown":    0.60,
    "low":        0.50,
}

# Event types with inherent direction when bull_bear_impact is neutral/unknown
_BULLISH_TYPES = frozenset({
    "hyperscaler_anchor", "government_contract", "defense_military",
    "commercial_contract", "strategic_partnership", "technical_milestone",
    "product_launch", "mna",
})
_BEARISH_TYPES = frozenset({"financing", "investigation"})

# Empty V2 result sentinel (avoids repetition)
_V2_EMPTY: dict[str, Any] = {
    "catalyst_v2_available":         False,
    "catalyst_v2_score":             None,
    "catalyst_v2_state":             "UNAVAILABLE",
    "catalyst_v2_primary_event":     None,
    "catalyst_v2_supporting_events": [],
    "catalyst_v2_bearish_event":     None,
    "catalyst_v2_event_count":       0,
    "catalyst_v2_reason_codes":      ["V2_NO_EVENTS"],
    "catalyst_v2_conflicts":         [],
    "catalyst_v2_version":           _V2_VERSION,
}

# ── V2.1 — Ticker Relevance Gate constants ────────────────────────────────────

# Minimum relevance score for a bullish event to contribute to V2.
# 0.5 = ambiguous (no explicit ticker match but no obvious other-company lead).
_V2_REL_BULLISH_MIN  = 0.40

# Minimum relevance score for a bearish penalty to apply.
# Stricter: require near-certain attribution to the scored ticker.
_V2_REL_BEARISH_MIN  = 0.75

# Ticker → known company name fragments (lowercase) for tickers whose articles
# often appear without an explicit "(TICKER)" parens pattern.
# ONLY add confirmed cases from validation — do NOT create a full company database.
# Each entry: ticker -> tuple of lowercase name fragments that identify the company.
_TICKER_NAME_FRAGS: dict[str, tuple[str, ...]] = {
    "MU":    ("micron",),
    "AREC":  ("american resources",),
    "FLY":   ("firefly",),
    "KTOS":  ("kratos", "aevex"),   # AEVEX is a confirmed Kratos subsidiary
    "ABCL":  ("abcellera",),
    "ONDS":  ("ondas",),
    "LUNR":  ("intuitive machines",),
    "ANET":  ("arista",),
    "ENVX":  ("enovix",),
    "ASTS":  ("ast spacemobile", "ast space"),
    "PLUG":  ("plug power",),
    "FSLR":  ("first solar",),
    "AVAV":  ("aerovironment",),
    "PANW":  ("palo alto",),
    "SNOW":  ("snowflake",),
    "CRDO":  ("credo",),
    "RBRK":  ("rubrik",),
    "SOFI":  ("sofi",),
    "RGTI":  ("rigetti",),
}

# Market-index / macro leading tokens that immediately signal a roundup article
# when they appear at the start of a title — not company-specific news.
_MARKET_INDEX_PAT = re.compile(
    r"^\s*(?:nasdaq|s&p\s*500|dow\s+jones|dow|s&p|spx|spy|qqq|russell|vix|"
    r"market|wall\s+street|stock\s+market|futures|index|indices|broad\s+market|"
    r"tech\s+stocks|small.?cap|large.?cap|crypto\s+market|bitcoin|btc\b)",
    re.IGNORECASE,
)

# Action verbs that split a leading company name from the event description.
# Extract text BEFORE first match as the article's primary subject/company.
_LEADING_VERB_PAT = re.compile(
    r"\b(?:announces?|wins?|lands?|signs?|partners?|acquires?|acquiring|"
    r"cuts?|raises?|sinks?|falls?|fell|drops?|plunges?|tumbles?|rises?|"
    r"climbs?|surges?|soars?|slumps?|reports?|launches?|selected|"
    r"appoints?|builds?|expands?|strengthens?|completes?|closes?|gains?|"
    r"unveils?|introduces?|releases?|slides?|down\b|up\b|hits?|reaches?|"
    r"enters?|agrees?|secures?|receives?|awarded|granted)\b",
    re.IGNORECASE,
)

# Governance / compensation patterns — these produce routine/neutral classification
# in V2 regardless of news_signal_scorer output, and must not produce
# COMPELLING or MODERATE bullish scores.
_GOVERNANCE_PAT = re.compile(
    r"\b(?:"
    # Compensation grants
    r"(?:restricted\s+stock\s+(?:units?|award|grant))"
    r"|\brsus?\b"          # abbreviation for Restricted Stock Units
    r"|(?:(?:cfo|ceo|cto|coo|cso|svp|evp|vp|director|officer)\s+(?:granted?|award(?:ed)?\s+\d))"
    r"|(?:equity\s+(?:grant|award|compensation))"
    r"|(?:option\s+grant(?:s|ed)?)"
    r"|(?:multi.?year\s+restricted)"
    # Governance
    r"|(?:bylaw\s+amend)"
    r"|(?:proxy\s+(?:rule|vote|advisory|statement))"
    r"|(?:annual\s+(?:general\s+)?meeting)"
    # Routine conference presentation (not earnings)
    # Flexible: matches "Presents at <anything> Conference/Summit/Day/Forum"
    r"|(?:presents?\s+at\s+(?:the\s+)?.{0,80}?\b(?:conference|investor\s+day|analyst\s+day|tech\s+day|summit|symposium|forum)\b)"
    r")\b",
    re.IGNORECASE,
)

# Explicit ticker in parentheses: "(AAPL)", "(RXT)", "(FRMI)", etc.
# Matches 1–6 uppercase letters only.
_PARENS_TICKER_PAT = re.compile(r"\(([A-Z]{1,6})\)")


# ── Date helpers ──────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _et_today() -> date:
    return datetime.now(timezone.utc).date()


# ── Calendar-source loaders (pure cache reads) ────────────────────────────────

def _load_earnings_this_week() -> dict[str, dict]:
    """
    symbol -> {"date": "YYYY-MM-DD", "companyName": str}

    Pure read of earnings:curated:week:* cache via top_catalysts_service.
    Never fetches FMP at request time.
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

    Pure read of calendar_snapshots Neon table (falls back to disk JSON).
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


# ── Article loaders ───────────────────────────────────────────────────────────

def _load_news_activity_counts(symbols: list[str]) -> dict[str, dict]:
    """Single Neon query: 48h + prev-48h article counts for all symbols."""
    try:
        from data.rss_article_archive import query_ticker_activity
        return query_ticker_activity(symbols)
    except Exception:
        return {}


def _load_articles_bulk(symbols: list[str]) -> dict[str, list[dict]]:
    """
    V2.1 Performance fix: single bulk Neon query for all requested symbols.

    Replaces the previous per-symbol query_ticker_activity_articles() loop
    (which issued one Neon round-trip per active symbol).

    Returns {ticker: [article_dicts]} for the last _NEWS_LOOKBACK_HOURS hours.
    Uses query_recent_articles_for_scoring() which already issues one
    WHERE ticker = ANY(%s) query and returns all articles sorted by ticker + date.

    Zero extra Neon calls beyond this one.
    """
    if not symbols:
        return {}
    try:
        from data.rss_article_archive import query_recent_articles_for_scoring
        return query_recent_articles_for_scoring(symbols, hours=_NEWS_LOOKBACK_HOURS)
    except Exception:
        return {}


# ── V1 news classification (unchanged) ────────────────────────────────────────

def _classify_news_titles(articles: list[dict]) -> Optional[tuple[str, float, dict]]:
    """
    Scan article titles/summaries for the highest-scoring V1 category.
    Returns (category, score, matched_article) or None.
    First match per article wins (most-specific / highest-priority category).

    V1 deliberately does NOT use the ticker relevance gate — V1 behavior is
    preserved exactly as it was before V2.1.
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
                break
    return best


# ── V2.1 — Ticker Relevance Gate ─────────────────────────────────────────────

def _build_company_frags(sym: str) -> frozenset[str]:
    """
    Build the set of lowercase company name fragments that identify this ticker.
    Sources (no provider calls):
        1. The ticker symbol itself.
        2. Hardcoded _TICKER_NAME_FRAGS map (validated cases only).
    Returns frozenset of lowercase strings.
    """
    frags: set[str] = {sym.lower()}
    for frag in _TICKER_NAME_FRAGS.get(sym, ()):
        frags.add(frag.lower())
    return frozenset(frags)


def _ticker_relevance_gate(
    sym: str,
    title: str,
    company_frags: frozenset[str],
) -> dict[str, Any]:
    """
    Deterministic ticker-subject relevance check.

    Returns dict:
        ticker_relevance_score  float   0.0 – 1.0
        ticker_relevance_reason str     one of the reason codes below
        primary_subject         str     extracted primary company/entity

    Score thresholds (applied in _build_v2_events_from_articles):
        >= 0.75  → accept for BOTH bullish AND bearish contribution
        >= 0.40  → accept for bullish only; bearish reduced to low-confidence note
        <  0.40  → reject for both (article is clearly about another ticker)

    Logic (checked in priority order):
    1. Explicit (TICKER) in title — highest-confidence signal
       a. sym in parens           → 1.00  EXPLICIT_SYM_PARENS
       b. other ticker in parens  → 0.20  EXPLICIT_OTHER_TICKER
    2. Sym token appears standalone in title
                                  → 1.00  SYM_IN_TITLE
    3. Known company alias in title
                                  → 0.90  ALIAS_IN_TITLE
    4. Market-index / macro leads title
                                  → 0.20  MARKET_INDEX_LEAD
    5. Leading company phrase ≠ sym or any alias (other-company lead)
                                  → 0.35  OTHER_COMPANY_LEAD
    6. No disqualifying signal    → 0.50  AMBIGUOUS
    """
    if not title:
        return {"ticker_relevance_score": 0.5,
                "ticker_relevance_reason": "EMPTY_TITLE",
                "primary_subject": "unknown"}

    title_strip = title.strip()
    title_lower = title_strip.lower()

    # ── 1a. Sym token in parens — highest confidence ─────────────────────────
    parens_tickers = _PARENS_TICKER_PAT.findall(title_strip)
    if parens_tickers and sym in parens_tickers:
        return {"ticker_relevance_score": 1.0,
                "ticker_relevance_reason": "EXPLICIT_SYM_PARENS",
                "primary_subject": sym}

    # ── 1b. Known company alias in title ────────────────────────────────────
    # Checked early — before market-index and other-parens checks — so that
    # multi-company analyst notes (e.g. "(LUNR), Firefly NASA awards") correctly
    # attribute the article to FLY via "firefly" alias even when another ticker
    # appears in parens, and so aliases also override the market-index heuristic
    # (e.g. if an article starts "Nasdaq: Micron..." it still passes for MU).
    sym_upper = sym.upper()
    for frag in sorted(company_frags, key=len, reverse=True):
        if frag == sym.lower():
            continue   # sym token checked separately below
        if frag in title_lower:
            return {"ticker_relevance_score": 0.90,
                    "ticker_relevance_reason": "ALIAS_IN_TITLE",
                    "primary_subject": frag}

    # ── 2. Market-index / macro leading entity ───────────────────────────────
    # Checked BEFORE the sym-standalone check so that market-roundup articles
    # that list sym as one of many stocks (e.g. "Nasdaq Futures Cheer Iran
    # Breakthrough … RCAT, NVDA") are blocked even though sym appears in the
    # text.  Exception: if sym appears in the first 40 chars of the title it is
    # likely the primary subject (e.g. "Nasdaq-listed RCAT posts strong Q2").
    mi_match = _MARKET_INDEX_PAT.search(title_strip)
    if mi_match:
        sym_pos_match = re.search(r"\b" + re.escape(sym_upper) + r"\b", title_strip.upper())
        sym_pos = sym_pos_match.start() if sym_pos_match else len(title_strip)
        if sym_pos > 40:
            # Sym is mentioned late in a macro/index-led title → roundup article
            return {"ticker_relevance_score": 0.20,
                    "ticker_relevance_reason": "MARKET_INDEX_LEAD",
                    "primary_subject": mi_match.group(0).strip()}

    # ── 3. Sym token as standalone word in title ─────────────────────────────
    if re.search(r"\b" + re.escape(sym_upper) + r"\b", title_strip.upper()):
        return {"ticker_relevance_score": 1.0,
                "ticker_relevance_reason": "SYM_IN_TITLE",
                "primary_subject": sym}

    # ── 4. Another ticker explicitly named in parens ─────────────────────────
    if parens_tickers:
        primary = parens_tickers[0]
        return {"ticker_relevance_score": 0.20,
                "ticker_relevance_reason": "EXPLICIT_OTHER_TICKER",
                "primary_subject": primary}

    # ── 5. Leading company phrase extraction ─────────────────────────────────
    #
    # Extract text before the first action verb.  If that phrase is ≥ 5 chars
    # and clearly does NOT match sym or its aliases, this article's primary
    # subject is a different company.
    lead_match = _LEADING_VERB_PAT.search(title_strip)
    if lead_match and lead_match.start() > 4:
        lead_phrase = title_strip[:lead_match.start()].strip().rstrip(",:-")
        lead_lower  = lead_phrase.lower()
        # Check if lead contains sym or any alias
        has_match = (
            sym.lower() in lead_lower
            or any(frag in lead_lower for frag in company_frags if len(frag) > 1)
        )
        if not has_match and len(lead_phrase) >= 5:
            return {"ticker_relevance_score": 0.35,
                    "ticker_relevance_reason": "OTHER_COMPANY_LEAD",
                    "primary_subject": lead_phrase[:60]}

    # ── 6. Default: ambiguous but not disqualified ───────────────────────────
    return {"ticker_relevance_score": 0.50,
            "ticker_relevance_reason": "AMBIGUOUS",
            "primary_subject": "unknown"}


def _is_governance_article(title: str) -> bool:
    """
    Return True if the article title indicates a governance / compensation /
    routine conference event that must not produce COMPELLING or MODERATE
    V2 bullish scores regardless of news_signal_scorer output.

    Examples rejected:
        "Intrepid Potash (IPI) CFO granted multi-year restricted stock"
        "AnyTicker Director Compensation — Option Grants"
        "XYZ Presents at Goldman Sachs Technology Conference"
        "ABC Inc. Annual Meeting 2026"
    """
    return bool(_GOVERNANCE_PAT.search(title))


# ── V2 helpers ────────────────────────────────────────────────────────────────

def _recency_weight(published_at_iso: str) -> float:
    """
    Recency decay factor.
    0–24 h  → 1.00  (full materiality)
    24–48 h → 0.82
    48–72 h → 0.65
    72–96 h → 0.50
    >96 h   → 0.00  (excluded by archive retention anyway)
    Unknown age → 0.50 (conservative middle weight)
    """
    if not published_at_iso:
        return 0.50
    try:
        pub = datetime.fromisoformat(published_at_iso.replace("Z", "+00:00"))
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)
        age_h = (_now_utc() - pub).total_seconds() / 3600.0
    except Exception:
        return 0.50
    for threshold, weight in _V2_RECENCY:
        if age_h <= threshold:
            return weight
    return 0.0


def _v2_direction(bull_bear: str, catalyst_type: str) -> str:
    """Map news_signal_scorer bull_bear_impact + type to a V2 direction string."""
    if bull_bear == "bullish":
        return "bullish"
    if bull_bear == "bearish":
        return "bearish"
    if bull_bear == "mixed":
        return "conflict"
    # Neutral/unknown: fall back to inherent event-type direction
    if catalyst_type in _BULLISH_TYPES:
        return "bullish"
    if catalyst_type in _BEARISH_TYPES:
        return "bearish"
    return "neutral"


def _dedupe_key(event_type: str, date_str: str, entities: list[str]) -> str:
    """
    Deduplication key: same type + same calendar day are collapsed.
    For hyperscaler_anchor: also differentiate by primary entity so that
    a NVDA deal and an MSFT deal on the same day remain separate events.
    Syndicated copies of the same story → same key → single event record.
    """
    day = date_str[:10] if date_str else "unknown"
    if event_type == "hyperscaler_anchor" and entities:
        return f"{event_type}:{day}:{entities[0]}"
    return f"{event_type}:{day}"


def _build_v2_events_from_articles(
    sym: str,
    articles: list[dict],
    company_frags: frozenset[str],
) -> list[dict]:
    """
    V2.1: Score all articles with news_signal_scorer.score_article(), apply the
    ticker-subject relevance gate, filter governance/routine events, apply
    recency decay, deduplicate syndicated copies, and return a
    materiality-sorted list of V2 event dicts.

    Properties:
    - Zero provider calls (score_article is pure Python)
    - Idempotent (score_article skips re-scoring if already scored)
    - Does NOT count syndicated duplicates as independent catalysts
    - Governance / compensation articles cannot produce COMPELLING/MODERATE scores
    - Articles about other tickers are rejected (bearish) or downweighted (bullish)
    - article_count per deduped event provides a small confidence boost only
    """
    if not articles:
        return []

    try:
        from services.news_signal_scorer import score_article
    except Exception:
        return []

    # Score every article; apply relevance gate; collect eligible events
    scored: list[dict] = []
    for art in articles:
        title = art.get("title") or ""

        # ── Governance filter ──────────────────────────────────────────────
        if _is_governance_article(title):
            # Still score so it appears in debug fields, but mark as rejected
            continue

        try:
            sa = score_article(art, sym)
        except Exception:
            continue

        if sa.get("catalyst_type") == "routine":
            continue
        raw_score = sa.get("major_news_score", 0)
        if raw_score < _V2_MIN_SCORE:
            continue
        rw = _recency_weight(sa.get("published_at") or "")
        if rw == 0.0:
            continue

        # ── Ticker relevance gate ──────────────────────────────────────────
        rel = _ticker_relevance_gate(sym, title, company_frags)
        rel_score  = rel["ticker_relevance_score"]
        rel_reason = rel["ticker_relevance_reason"]
        primary_s  = rel["primary_subject"]

        direction = _v2_direction(sa.get("bull_bear_impact", "neutral"),
                                  sa.get("catalyst_type", "routine"))
        is_bearish = direction in ("bearish", "conflict")

        # Apply thresholds
        if is_bearish and rel_score < _V2_REL_BEARISH_MIN:
            # Bearish event from another company's article — reject the penalty
            continue
        if not is_bearish and rel_score < _V2_REL_BULLISH_MIN:
            # Bullish event from clearly unrelated article — reject
            continue

        scored.append({
            **sa,
            "_rw":                    rw,
            "_direction":             direction,
            "_ticker_relevance_score": rel_score,
            "_ticker_relevance_reason": rel_reason,
            "_primary_subject":       primary_s,
        })

    if not scored:
        return []

    # Deduplicate: group by (event_type, day[, primary_entity])
    # Keep highest-materiality article per group; count all articles per group
    groups: dict[str, dict] = {}
    for sa in scored:
        ct    = sa.get("catalyst_type") or "routine"
        pub   = sa.get("published_at") or ""
        ent   = sa.get("matched_entities") or []
        day   = pub[:10]
        key   = _dedupe_key(ct, day, ent)
        rw    = sa.get("_rw", 0.5)
        mat   = (sa.get("major_news_score", 0) / 100.0) * rw
        direction = sa["_direction"]

        if key not in groups or mat > groups[key]["materiality_score"]:
            sq = sa.get("source_quality", "unknown")
            groups[key] = {
                "event_id":                 f"{sym}:{ct}:{day}",
                "ticker":                   sym,
                "event_type":               ct,
                "event_reason":             ct.upper(),
                "direction":                direction,
                "materiality_score":        round(mat, 4),
                "confidence_score":         round(_V2_SOURCE_SCORE.get(sq, 0.60), 4),
                "status":                   "confirmed",
                "source":                   "rss",
                "title":                    sa.get("title", ""),
                "published_at":             pub,
                "catalyst_date":            day,
                "days_until":               None,
                "entity_mentions":          ent[:5],
                "url":                      sa.get("url", ""),
                "article_count":            1,
                "why_it_matters":           sa.get("why_it_matters", ""),
                # V2.1 debug fields
                "ticker_relevance_score":   round(sa["_ticker_relevance_score"], 3),
                "ticker_relevance_reason":  sa["_ticker_relevance_reason"],
                "primary_subject":          sa["_primary_subject"],
            }
        else:
            groups[key]["article_count"] += 1

    # Apply article_count confidence bonus: +4% per extra article, capped at +8%
    events: list[dict] = []
    for ev in groups.values():
        cnt   = ev["article_count"]
        bonus = min((cnt - 1) * 0.04, 0.08)
        ev["confidence_score"] = round(min(1.0, ev["confidence_score"] + bonus), 4)
        events.append(ev)

    events.sort(key=lambda e: -e["materiality_score"])
    return events


def _calendar_events_to_v2(
    sym:      str,
    earn:     Optional[dict],
    cal_evs:  list[dict],
) -> list[dict]:
    """
    Map V1 calendar events (earnings, IPO, dividend, split) to V2 event shape.
    Keeps them separate from RSS events so the combined scorer can distinguish
    a scheduled catalyst from a post-event RSS article.

    Calendar events are always ticker-specific (sourced from curated earnings/
    calendar snapshots) and are exempt from the relevance gate.
    """
    today = _et_today()
    out:   list[dict] = []

    if earn:
        try:
            ed       = datetime.strptime(earn["date"], "%Y-%m-%d").date()
            days_out = (ed - today).days
        except Exception:
            days_out = None
        mat = 0.80 if (days_out is not None and -1 <= days_out <= 1) else 0.65
        out.append({
            "event_id":                 f"{sym}:earnings:{earn['date']}",
            "ticker":                   sym,
            "event_type":               "earnings_guidance",
            "event_reason":             "EARNINGS_THIS_WEEK",
            "direction":                "neutral",
            "materiality_score":        mat,
            "confidence_score":         1.0,
            "status":                   "scheduled",
            "source":                   "earnings",
            "title":                    f"Earnings report on {earn['date']}",
            "published_at":             earn["date"] + "T00:00:00+00:00",
            "catalyst_date":            earn["date"],
            "days_until":               days_out,
            "entity_mentions":          [],
            "url":                      None,
            "article_count":            1,
            "why_it_matters":           "Scheduled earnings report is an imminent known catalyst.",
            "ticker_relevance_score":   1.0,
            "ticker_relevance_reason":  "CALENDAR_SOURCE",
            "primary_subject":          sym,
        })

    _DIR = {"ipo": "bullish", "split": "bullish", "dividend": "neutral"}
    _MAT = {"ipo": 0.55,      "split": 0.50,      "dividend": 0.40}

    for ev in cal_evs:
        kind = ev["kind"]
        try:
            cd       = datetime.strptime(ev["date"], "%Y-%m-%d").date()
            days_out = (cd - today).days
        except Exception:
            days_out = None
        out.append({
            "event_id":                 f"{sym}:{kind}:{ev['date']}",
            "ticker":                   sym,
            "event_type":               kind,
            "event_reason":             f"{kind.upper()}_THIS_WEEK",
            "direction":                _DIR.get(kind, "neutral"),
            "materiality_score":        _MAT.get(kind, 0.40),
            "confidence_score":         1.0,
            "status":                   "scheduled",
            "source":                   "calendar",
            "title":                    f"{kind.capitalize()} calendar event on {ev['date']}",
            "published_at":             ev["date"] + "T00:00:00+00:00",
            "catalyst_date":            ev["date"],
            "days_until":               days_out,
            "entity_mentions":          [],
            "url":                      None,
            "article_count":            1,
            "why_it_matters":           f"Scheduled {kind} event is a near-term known catalyst.",
            "ticker_relevance_score":   1.0,
            "ticker_relevance_reason":  "CALENDAR_SOURCE",
            "primary_subject":          sym,
        })

    return out


def _compute_v2_from_events(
    rss_events: list[dict],
    cal_events: list[dict],
) -> dict[str, Any]:
    """
    Combine RSS and calendar V2 events into the final V2 catalyst result.

    Scoring concept:
        positive_score  = primary_bullish_materiality * 100
                        + corroboration_bonus (capped at +8)
        negative_penalty = primary_bearish_materiality * 100 * 0.70
        final_v2_score  = clamp(positive_score - negative_penalty, 0, 100)

    Does not affect any V1 fields or THEME_ALIGNMENT primary score.
    """
    all_events = list(rss_events) + list(cal_events)

    if not all_events:
        return dict(_V2_EMPTY)

    # Partition by direction
    pos_events  = [e for e in all_events if e["direction"] in ("bullish", "neutral")]
    bear_events = [e for e in all_events if e["direction"] in ("bearish", "conflict")]

    pos_events.sort(key=lambda e: -e["materiality_score"])
    bear_events.sort(key=lambda e: -e["materiality_score"])

    primary_pos  = pos_events[0]  if pos_events  else None
    primary_bear = bear_events[0] if bear_events else None
    supporting   = pos_events[1:] if len(pos_events) > 1 else []

    # Score
    if primary_pos:
        pos_base      = primary_pos["materiality_score"] * 100.0
        corroboration = min(len(supporting) * _V2_CORROBORATION_PER, _V2_CORROBORATION_CAP)
        pos_score     = pos_base + corroboration
    else:
        pos_score = 0.0

    neg_penalty = (primary_bear["materiality_score"] * 100.0 * _V2_BEARISH_FACTOR
                   ) if primary_bear else 0.0

    final = round(max(0.0, min(100.0, pos_score - neg_penalty)), 1)

    # Available only when there is at least one meaningful event and score is useful
    available = (
        final >= _V2_AVAIL_THRESHOLD
        and (primary_pos is not None or primary_bear is not None)
    )

    # State
    if not available:
        state = "UNAVAILABLE"
    elif final >= 65.0:
        state = "COMPELLING"
    elif final >= 40.0:
        state = "MODERATE"
    else:
        state = "WEAK"

    # Reason codes — up to 5 distinct event types
    reason_codes: list[str] = []
    for e in all_events:
        rc = (e.get("event_reason") or e.get("event_type") or "UNKNOWN").upper()
        if rc not in reason_codes:
            reason_codes.append(rc)
        if len(reason_codes) >= 5:
            break

    # Conflicts
    conflicts: list[str] = []
    if primary_pos and primary_bear:
        conflicts.append(
            f"Bullish {primary_pos['event_type']} conflicts with "
            f"bearish {primary_bear['event_type']}"
        )

    return {
        "catalyst_v2_available":         available,
        "catalyst_v2_score":             final if available else None,
        "catalyst_v2_state":             state,
        "catalyst_v2_primary_event":     primary_pos,
        "catalyst_v2_supporting_events": supporting[:4],
        "catalyst_v2_bearish_event":     primary_bear,
        "catalyst_v2_event_count":       len(all_events),
        "catalyst_v2_reason_codes":      reason_codes,
        "catalyst_v2_conflicts":         conflicts,
        "catalyst_v2_version":           _V2_VERSION,
    }


# ── Public API ────────────────────────────────────────────────────────────────

def get_catalyst_alignment_bulk(symbols: list[str]) -> dict[str, dict]:
    """
    Compute the clean Catalyst Alignment result for every symbol in `symbols`.
    Zero provider calls (all sources are pre-persisted caches).

    V2.1 performance: 2 total Neon queries regardless of universe size:
        1. query_ticker_activity()           — counts for all syms (1 query)
        2. query_recent_articles_for_scoring() — articles for all active syms (1 query)

    Returns {SYMBOL: {
        # ── V1 fields (unchanged) ──────────────────────────────────────────
        catalyst_alignment_score:      float | None (0-100),
        catalyst_alignment_available:  bool,
        primary_catalyst:              dict | None,
        catalyst_events:               list[dict],
        catalyst_alignment_reason:     str,
        # ── V2 additive shadow fields ──────────────────────────────────────
        catalyst_v2_available:         bool,
        catalyst_v2_score:             float | None,
        catalyst_v2_state:             str,
        catalyst_v2_primary_event:     dict | None,
        catalyst_v2_supporting_events: list[dict],
        catalyst_v2_bearish_event:     dict | None,
        catalyst_v2_event_count:       int,
        catalyst_v2_reason_codes:      list[str],
        catalyst_v2_conflicts:         list[str],
        catalyst_v2_version:           str,
    }}
    """
    syms         = [s.upper() for s in symbols]
    earnings_map = _load_earnings_this_week()
    calendar_map = _load_calendar_events_this_week()
    news_counts  = _load_news_activity_counts(syms)

    # V2.1 Performance fix: single bulk article fetch for all active symbols.
    # Only fetch articles for symbols that have ANY RSS activity (48h or prior-48h),
    # so we don't pay Neon query time for symbols with zero archive rows.
    active_syms = [
        s for s in syms
        if int((news_counts.get(s) or {}).get("articles_48h", 0) or 0) > 0
        or int((news_counts.get(s) or {}).get("previous_articles_48h", 0) or 0) > 0
    ]
    bulk_articles: dict[str, list[dict]] = _load_articles_bulk(active_syms) if active_syms else {}

    today = _et_today()
    out: dict[str, dict] = {}

    for sym in syms:
        v1_events: list[dict] = []

        # ── 1) Earnings this week (V1) ──────────────────────────────────────
        earn = earnings_map.get(sym)
        if earn:
            try:
                ed       = datetime.strptime(earn["date"], "%Y-%m-%d").date()
                days_out = (ed - today).days
            except Exception:
                days_out = None
            score = (
                _SCORE_EARNINGS_IMMINENT
                if (days_out is not None and -1 <= days_out <= 1)
                else _SCORE_EARNINGS_THIS_WEEK
            )
            v1_events.append({
                "type":        "EARNINGS_THIS_WEEK",
                "date":        earn["date"],
                "description": f"Earnings report on {earn['date']}",
                "score":       score,
            })

        # ── 2) Calendar events this week (V1) ──────────────────────────────
        cal_evs_for_sym = calendar_map.get(sym, [])
        for ev in cal_evs_for_sym:
            kind = ev["kind"]
            score = {
                "ipo":      _SCORE_IPO_THIS_WEEK,
                "dividend": _SCORE_DIVIDEND_THIS_WEEK,
                "split":    _SCORE_SPLIT_THIS_WEEK,
            }[kind]
            v1_events.append({
                "type":        f"{kind.upper()}_THIS_WEEK",
                "date":        ev["date"],
                "description": f"{kind.capitalize()} calendar event on {ev['date']}",
                "score":       score,
            })

        # ── 3) Articles — from bulk pre-fetch (V2.1 performance fix) ────────
        #
        # raw_articles is retrieved from the bulk dict (no per-symbol query).
        # V1 only triggers on articles_48h > 0 (existing behavior preserved).
        # V2 also considers previous_articles_48h (48–96h window).
        counts        = news_counts.get(sym) or {}
        articles_48h  = int(counts.get("articles_48h")          or 0)
        prev_articles = int(counts.get("previous_articles_48h") or 0)
        has_recent    = articles_48h > 0
        has_any       = has_recent or (prev_articles > 0)

        raw_articles: list[dict] = bulk_articles.get(sym, []) if has_any else []

        # ── 4) V1 material news (unchanged regex-based classifier) ──────────
        if has_recent and raw_articles:
            news_hit = _classify_news_titles(raw_articles)
            if news_hit:
                cat, score, art = news_hit
                v1_events.append({
                    "type":        cat,
                    "date":        (art.get("published_at") or "")[:10],
                    "description": art.get("title") or cat,
                    "score":       score,
                    "url":         art.get("url"),
                })

        # ── 5) V1 result (fields and logic unchanged) ───────────────────────
        if not v1_events:
            v1_result: dict[str, Any] = {
                "catalyst_alignment_score":     None,
                "catalyst_alignment_available": False,
                "primary_catalyst":             None,
                "catalyst_events":              [],
                "catalyst_alignment_reason":    "CATALYST_UNAVAILABLE_NO_EVENTS",
            }
        else:
            v1_events.sort(key=lambda e: -e["score"])
            primary = v1_events[0]
            v1_result = {
                "catalyst_alignment_score":     round(primary["score"], 1),
                "catalyst_alignment_available": True,
                "primary_catalyst":             primary,
                "catalyst_events":              v1_events[:5],
                "catalyst_alignment_reason":    "CATALYST_EVENT_DETECTED",
            }

        # ── 6) V2 pipeline (additive — zero effect on V1 fields) ────────────
        #
        # Build company aliases once per sym (no I/O — pure dict lookup).
        # Calls services.news_signal_scorer.score_article() (pure Python, no I/O).
        # Does not change THEME_ALIGNMENT weights.
        company_frags = _build_company_frags(sym)
        v2_rss_events = _build_v2_events_from_articles(sym, raw_articles, company_frags)
        v2_cal_events = _calendar_events_to_v2(sym, earn, cal_evs_for_sym)
        v2_result     = _compute_v2_from_events(v2_rss_events, v2_cal_events)

        out[sym] = {**v1_result, **v2_result}

    return out


def get_catalyst_alignment_for_ticker(symbol: str) -> dict:
    result = get_catalyst_alignment_bulk([symbol])
    return result.get(symbol.upper()) or {
        "catalyst_alignment_score":      None,
        "catalyst_alignment_available":  False,
        "primary_catalyst":              None,
        "catalyst_events":               [],
        "catalyst_alignment_reason":     "CATALYST_UNAVAILABLE_NO_EVENTS",
        **_V2_EMPTY,
    }
