"""
News Signal Scorer — deterministic keyword/entity scoring for watchlist news articles.

Operates on (title + summary) text only — no LLM calls, no network I/O.
Designed to run at article-normalization time so results are stored in the
15-minute in-memory news cache and never recomputed for the same article.

Fields added to each article dict:
    is_major_development    bool
    major_news_score        int  0–100
    major_news_label        str
    catalyst_type           str | None
    signal_strength         "high" | "medium" | "low" | "none"
    bull_bear_impact        "bullish" | "bearish" | "mixed" | "neutral" | "unknown"
    why_it_matters          str  (≤180 chars)
    matched_entities        list[str]
    matched_keywords        list[str]
    related_watchlist_symbols list[str]
    source_quality          "primary" | "tier1" | "syndicated" | "low" | "unknown"
"""

from __future__ import annotations

import re
from typing import Any

# ── Entity lists ──────────────────────────────────────────────────────────────

_HYPERSCALERS: list[tuple[str, str]] = [
    # (pattern_to_match, display_name)
    (r"\bmicrosoft\b",     "Microsoft"),
    (r"\bazure\b",         "Azure"),
    (r"\bamazon\b",        "Amazon"),
    (r"\baws\b",           "AWS"),
    (r"\bgoogle cloud\b",  "Google Cloud"),
    (r"\bgoogle\b",        "Google"),
    (r"\bmeta\b",          "Meta"),
    (r"\boracle\b",        "Oracle"),
    (r"\bnvidia\b",        "NVIDIA"),
    (r"\bopenai\b",        "OpenAI"),
    (r"\bcoreweave\b",     "CoreWeave"),
    (r"\bxai\b",           "xAI"),
    (r"\bapple\b",         "Apple"),
    (r"\bbroadcom\b",      "Broadcom"),
    (r"\bmarvell\b",       "Marvell"),
    (r"\barista\b",        "Arista"),
    (r"\btsmc\b",          "TSMC"),
    (r"\basml\b",          "ASML"),
]

# ── Entity → public ticker resolution (display helper only, not a classifier) ──
#
# Maps the display_name strings produced by _HYPERSCALERS → canonical public ticker.
# Entities without a listed public ticker (OpenAI, xAI) are intentionally absent
# so resolve_anchor_symbols() never fabricates a symbol for a private company.
# Azure and AWS are mapped to their parent company tickers.
# Source of truth for the single backend authority on this mapping.

_HYPERSCALER_ANCHOR_SYMBOLS: dict[str, str] = {
    "Microsoft":    "MSFT",
    "Azure":        "MSFT",     # Azure is Microsoft's cloud brand
    "Amazon":       "AMZN",
    "AWS":          "AMZN",     # AWS is Amazon's cloud brand
    "Google Cloud": "GOOGL",
    "Google":       "GOOGL",
    "Meta":         "META",
    "Oracle":       "ORCL",
    "NVIDIA":       "NVDA",
    "CoreWeave":    "CRWV",     # public since March 2025
    "Apple":        "AAPL",
    "Broadcom":     "AVGO",
    "Marvell":      "MRVL",
    "Arista":       "ANET",
    "TSMC":         "TSM",
    "ASML":         "ASML",
    # OpenAI → absent (private)
    # xAI    → absent (private)
}


def resolve_anchor_symbols(matched_entities: list[str]) -> list[str]:
    """
    Convert entity display names (already matched by score_article) to canonical
    public ticker symbols for display in hyperscaler_articles.

    Uses _HYPERSCALER_ANCHOR_SYMBOLS — the single backend authority.
    Entities without a public ticker (OpenAI, xAI) are silently omitted.
    Azure/AWS are collapsed to their parent ticker (MSFT/AMZN).
    Deduplicates while preserving first-occurrence order.

    This is NOT a classifier — it does not re-examine article text.
    It only converts already-matched entity names into tickers.
    """
    seen: dict[str, bool] = {}
    result: list[str] = []
    for entity in (matched_entities or []):
        ticker = _HYPERSCALER_ANCHOR_SYMBOLS.get(entity)
        if ticker and ticker not in seen:
            seen[ticker] = True
            result.append(ticker)
    return result

_GOV_ENTITIES: list[tuple[str, str]] = [
    (r"\bdepartment of defense\b",  "DoD"),
    (r"\b(?:the\s+)?dod\b",         "DoD"),
    (r"\bdarpa\b",                   "DARPA"),
    (r"\bu\.s\.\s+army\b|\bthe army\b", "Army"),
    (r"\bu\.s\.\s+navy\b|\bthe navy\b", "Navy"),
    (r"\bair force\b",               "Air Force"),
    (r"\bspace force\b",             "Space Force"),
    (r"\bnasa\b",                    "NASA"),
    (r"\bdoe\b|\bdepartment of energy\b", "DOE"),
    (r"\bchips act\b",               "CHIPS Act"),
    (r"\bfederal contract\b",        "federal contract"),
    (r"\bmilitary contract\b",       "military contract"),
    (r"\bdefense contract\b",        "defense contract"),
    (r"\bgovernment grant\b",        "government grant"),
    (r"\bsbir\b",                    "SBIR"),
    (r"\bsttr\b",                    "STTR"),
    (r"\bnational security\b",       "national security"),
    (r"\bclassified\b",              "classified"),
    (r"\bmission.critical\b",        "mission-critical"),
]

# ── Keyword groups ────────────────────────────────────────────────────────────

_DEAL_PATTERNS: list[tuple[str, str]] = [
    (r"\bcontract\b",               "contract"),
    (r"\bawarded\b",                "awarded"),
    (r"\bselected by\b",            "selected by"),
    (r"\bdesign win\b",             "design win"),
    (r"\bcustomer win\b",           "customer win"),
    (r"\bstrategic partnership\b",  "strategic partnership"),
    (r"\bmulti.year\b",             "multi-year"),
    (r"\bsupply agreement\b",       "supply agreement"),
    (r"\bdeployment\b",             "deployment"),
    (r"\bpurchase order\b",         "purchase order"),
    (r"\bproduction ramp\b",        "production ramp"),
    (r"\bcommercial launch\b",      "commercial launch"),
    (r"\bvolume production\b",      "volume production"),
    (r"\bqualif(?:ied|ication)\b",  "qualification"),
    (r"\bcertif(?:ied|ication)\b",  "certification"),
    (r"\bapproved supplier\b",      "approved supplier"),
    (r"\bsigns.*?agreement\b",      "signs agreement"),
    (r"\bpartnership\b",            "partnership"),
    (r"\bcollaboration\b",          "collaboration"),
    (r"\bexpansion\b",              "expansion"),
]

_TECH_PATTERNS: list[tuple[str, str]] = [
    (r"\bbreakthrough\b",           "breakthrough"),
    (r"\bmilestone\b",              "milestone"),
    (r"\bworld first\b",            "world first"),
    (r"\brecord performance\b",     "record performance"),
    (r"\bnew product launch\b",     "new product launch"),
    (r"\bsampling\b",               "sampling"),
    (r"\btape.out\b",               "tape-out"),
    (r"\byield improvement\b",      "yield improvement"),
    (r"\bproduction.ready\b",       "production-ready"),
    (r"\bvalidates\b",              "validates"),
    (r"\bsuccessful test\b",        "successful test"),
    (r"\bdemonstrates\b",           "demonstrates"),
    (r"\bcommercially available\b", "commercially available"),
]

_BEARISH_PATTERNS: list[tuple[str, str]] = [
    (r"\bstock offering\b",         "stock offering"),
    (r"\batm program\b|\bat.the.market\b", "ATM program"),
    (r"\bdilut(?:ion|ive|ed)\b",    "dilution"),
    (r"\bconvertible note\b",       "convertible notes"),
    (r"\bgoing concern\b",          "going concern"),
    (r"\bdowngrad(?:ed|e)\b",       "downgrade"),
    (r"\bguidance cut\b",           "guidance cut"),
    (r"\bguidance lower\b",         "guidance lowered"),
    (r"\bcustomer loss\b",          "customer loss"),
    (r"\binvestigation\b",          "investigation"),
    (r"\blawsuit\b",                "lawsuit"),
    (r"\brestatement\b",            "restatement"),
    (r"\bshort report\b",           "short report"),
    (r"\bsecondary offering\b",     "secondary offering"),
    (r"\bwarrant\s+exercise\b",     "warrant exercise"),
    (r"\bchapter\s+11\b",           "Chapter 11"),
    (r"\bbankruptcy\b",             "bankruptcy"),
]

_MNA_PATTERNS: list[tuple[str, str]] = [
    (r"\bacquisition\b",            "acquisition"),
    (r"\bmerger\b",                 "merger"),
    (r"\bacquired\b",               "acquired"),
    (r"\btakeover\b",               "takeover"),
    (r"\bbuyout\b",                 "buyout"),
    (r"\bmerges with\b",            "merges with"),
    (r"\bcombines with\b",          "combines with"),
    (r"\bstrategic investment\b",   "strategic investment"),
    (r"\bprivate equity\b",         "private equity"),
    (r"\btake.private\b",           "take-private"),
    (r"\boffer to buy\b",           "offer to buy"),
]

_EARNINGS_PATTERNS: list[tuple[str, str]] = [
    (r"\bearnings beat\b",          "earnings beat"),
    (r"\bearnings miss\b",          "earnings miss"),
    (r"\bguidance raised\b|\braised guidance\b|\braised full.year\b", "guidance raised"),
    (r"\bguidance lowered\b|\blowered guidance\b", "guidance lowered"),
    (r"\babove (?:analyst |wall street )?estimates\b", "above estimates"),
    (r"\bbelow (?:analyst |wall street )?estimates\b", "below estimates"),
    (r"\brevenue beat\b",           "revenue beat"),
    (r"\brevenue miss\b",           "revenue miss"),
    (r"\bearnings shock\b",         "earnings shock"),
    (r"\bsurprise(?:d)? (?:earnings|revenue|profit)\b", "earnings surprise"),
    (r"\bprofit warning\b",         "profit warning"),
]

# Anti-patterns: market roundup / noise articles → cap score ≤ 20
_ROUNDUP_PATTERNS: list[str] = [
    r"\bstocks?(?:\s+move|\s+rise|\s+fall|\s+rally|\s+tumble|\s+slide)\b",
    r"\bmarket (?:update|recap|wrap|movers|rally|sell.off|pullback)\b",
    r"\bweekly recap\b",
    r"\bmorning (?:briefing|wrap|round.?up)\b",
    r"\bdaily briefing\b",
    r"\bstocks to watch\b",
    r"\btop (?:stock |market )?movers\b",
    r"\bbroader market\b",
    r"\bwall street (?:open|close|today)\b",
    r"\bpremarket (?:update|trading|movers)\b",
    r"\bafter.hours (?:update|trading|movers)\b",
    r"\bsector (?:update|rotation|roundup)\b",
    r"\b(?:dow|nasdaq|s&p|spy|qqq)\s+(?:gains?|loses?|rises?|falls?|tumbles?|rallies)\b",
    r"\bhere(?:'s| is) what(?:'s| is) moving\b",
    r"\bwhy (?:\w+ )?(?:stock|shares) (?:is|are) (?:rising|falling|down|up) today\b",
]

# ── Source quality mapping ────────────────────────────────────────────────────

_TIER1_SOURCES: frozenset[str] = frozenset({
    "reuters", "bloomberg", "wsj", "wall street journal", "financial times",
    "ft", "cnbc", "barron's", "barrons", "marketwatch", "the economist",
    "the wall street journal", "associated press", "ap", "ft.com",
})
_PRIMARY_SOURCES: frozenset[str] = frozenset({
    "pr newswire", "business wire", "globe newswire", "businesswire",
    "accesswire", "newswire", "sec", "edgar", "globenewswire",
    "globe newsroom", "prnewswire", "ir wire",
})
_SYNDICATED_SOURCES: frozenset[str] = frozenset({
    "yahoo finance", "seeking alpha", "motley fool", "benzinga", "marketbeat",
    "investopedia", "fool.com", "zacks", "thestreet", "the street",
    "stockanalysis", "finviz", "tipranks", "nasdaq.com", "investing.com",
    "investor's business daily", "ibd", "simply wall st",
})


def _match_patterns(text: str, patterns: list[tuple[str, str]]) -> list[str]:
    """Return display names of all matched patterns in text."""
    matched = []
    for pat, name in patterns:
        if re.search(pat, text, re.IGNORECASE):
            matched.append(name)
    return matched


def _is_roundup(text: str) -> bool:
    for pat in _ROUNDUP_PATTERNS:
        if re.search(pat, text, re.IGNORECASE):
            return True
    return False


def _source_quality(source: str) -> str:
    s = (source or "").lower().strip()
    if any(t in s for t in _TIER1_SOURCES):
        return "tier1"
    if any(p in s for p in _PRIMARY_SOURCES):
        return "primary"
    if any(syn in s for syn in _SYNDICATED_SOURCES):
        return "syndicated"
    return "unknown"


def _truncate(text: str, max_len: int = 180) -> str:
    return text if len(text) <= max_len else text[:max_len - 1].rstrip() + "…"


def _why_it_matters(
    catalyst_type: str,
    matched_entities: list[str],
    matched_keywords: list[str],
    bull_bear: str,
) -> str:
    entity_str = ", ".join(matched_entities[:3]) if matched_entities else ""
    kw_str = matched_keywords[0] if matched_keywords else ""

    templates: dict[str, str] = {
        "hyperscaler_anchor": (
            f"Direct commercial relationship with {entity_str or 'hyperscaler'} validates "
            "technology and signals major revenue opportunity."
        ),
        "government_contract": (
            f"{entity_str or 'Government'} contract signals regulatory validation and "
            "durable non-commercial revenue stream."
        ),
        "defense_military": (
            f"Defense/military selection by {entity_str or 'DoD/DARPA'} indicates "
            "mission-critical technology validation."
        ),
        "mna": (
            f"M&A activity ({kw_str or 'deal'}) signals strategic transformation or "
            "potential acquisition premium."
        ),
        "earnings_guidance": (
            f"Earnings/guidance development ({kw_str or 'beat/miss'}) signals a "
            "fundamental shift in financial outlook."
        ),
        "financing": (
            f"Financing event ({kw_str or 'offering'}) may dilute shareholders or "
            "signal balance-sheet stress."
        ),
        "commercial_contract": (
            f"Major {kw_str or 'contract'} with {entity_str or 'customer'} signals "
            "commercial traction and revenue pipeline."
        ),
        "technical_milestone": (
            f"Technical {kw_str or 'milestone'} indicates product readiness and "
            "potential accelerant for commercialization."
        ),
        "strategic_partnership": (
            f"Strategic partnership with {entity_str or 'key player'} expands "
            "addressable market and commercial reach."
        ),
        "product_launch": (
            f"New product/service launch opens revenue opportunity and signals "
            "execution capability."
        ),
        "routine": "Routine market mention with no specific actionable catalyst identified.",
    }
    raw = templates.get(catalyst_type, templates["routine"])
    return _truncate(raw)


# ── Text pre-processing ───────────────────────────────────────────────────────

_RE_STRIP_HTML = re.compile(r"<[^>]+>")
_RE_STRIP_URL  = re.compile(r"https?://\S+")
_RE_WHITESPACE = re.compile(r"\s{2,}")


def _clean_text(raw: str) -> str:
    """Strip HTML tags and URLs, collapse whitespace, lowercase."""
    t = _RE_STRIP_HTML.sub(" ", raw)
    t = _RE_STRIP_URL.sub(" ", t)
    t = _RE_WHITESPACE.sub(" ", t)
    return t.lower().strip()


# ── Main scoring function ─────────────────────────────────────────────────────

def score_article(
    article: dict[str, Any],
    ticker_symbol: str,
) -> dict[str, Any]:
    """
    Score a single news article dict and return the augmented dict.

    Operates purely on title + summary text. No I/O, no LLM, O(1) per article.
    Idempotent — skips re-scoring if signal fields already present.

    Two-level entity matching:
    - Classification (hyperscaler_anchor / gov/defense): entity must appear in
      the TITLE text after stripping HTML/URLs. This prevents Google News RSS
      summaries (which embed news.google.com URLs and navigation text) from
      triggering false-positive hyperscaler matches on unrelated articles.
    - Display (matched_entities): entity detected anywhere in title+summary
      after stripping, so genuine summary-level mentions surface in the UI.
    """
    if "is_major_development" in article:
        return article

    title   = str(article.get("title",   "") or "")
    summary = str(article.get("summary", "") or "")
    source  = str(article.get("source",  "") or "")

    # Pre-process: strip HTML/URLs to avoid RSS metadata false matches
    title_clean   = _clean_text(title)
    summary_clean = _clean_text(summary)
    text_clean    = (title_clean + " " + summary_clean).strip()

    # ── 1. Anti-pattern check (title is enough) ───────────────────────────────
    roundup = _is_roundup(title_clean)

    # ── 2. Entity detection ───────────────────────────────────────────────────
    # For CLASSIFICATION — entity must appear in title (prevents summary-bleed)
    hyperscaler_class = _match_patterns(title_clean, _HYPERSCALERS)
    gov_class         = _match_patterns(title_clean, _GOV_ENTITIES)

    # For DISPLAY — entity anywhere in cleaned title+summary
    hyperscaler_all   = _match_patterns(text_clean, _HYPERSCALERS)
    gov_all           = _match_patterns(text_clean, _GOV_ENTITIES)

    matched_entities: list[str] = list(dict.fromkeys(hyperscaler_all + gov_all))

    # ── 3. Keyword detection (full text is fine — no URL-injection risk) ──────
    deal_hits     = _match_patterns(text_clean, _DEAL_PATTERNS)
    tech_hits     = _match_patterns(text_clean, _TECH_PATTERNS)
    bearish_hits  = _match_patterns(text_clean, _BEARISH_PATTERNS)
    mna_hits      = _match_patterns(text_clean, _MNA_PATTERNS)
    earnings_hits = _match_patterns(text_clean, _EARNINGS_PATTERNS)

    matched_keywords: list[str] = list(dict.fromkeys(
        deal_hits + tech_hits + bearish_hits + mna_hits + earnings_hits
    ))

    # ── 4. Score computation ──────────────────────────────────────────────────
    score     = 10
    catalyst  = "routine"
    label     = "Routine Mention"
    bull_bear = "neutral"

    if roundup:
        # Market roundup — cap regardless of entity hits
        score    = 12
        catalyst = "routine"
        label    = "Routine Mention"

    elif bearish_hits and not (hyperscaler_class or gov_class or deal_hits):
        # Pure bearish event (dilution, going-concern, lawsuit) — no offsetting signal
        score     = 70
        catalyst  = "financing"
        label     = "Financing / Dilution"
        bull_bear = "bearish"
        legal_kw  = {"investigation", "lawsuit", "restatement", "short report"}
        if any(h in legal_kw for h in bearish_hits):
            catalyst = "investigation"
            label    = "Legal / Investigation"

    elif mna_hits:
        score     = 75
        catalyst  = "mna"
        label     = "M&A"
        bull_bear = "bullish"
        if bearish_hits:
            bull_bear = "mixed"

    elif gov_class and (deal_hits or tech_hits):
        # Gov/defense entity IN TITLE + action language anywhere → high confidence
        score     = 82
        catalyst  = "defense_military"
        label     = "Government / Defense"
        bull_bear = "bullish"
        mil_entities = {"DoD", "DARPA", "Army", "Navy", "Air Force", "Space Force",
                        "defense contract", "military contract", "classified",
                        "mission-critical"}
        if not any(e in mil_entities for e in gov_class):
            catalyst = "government_contract"
        score = min(score + len(gov_class) * 2, 96)

    elif hyperscaler_class and deal_hits:
        # Hyperscaler IN TITLE + deal/partnership language anywhere → high confidence
        score     = 87
        catalyst  = "hyperscaler_anchor"
        label     = "Hyperscaler Deal"
        bull_bear = "bullish"
        score     = min(score + (len(hyperscaler_class) - 1) * 3, 98)
        if bearish_hits:
            bull_bear = "mixed"
            score     = max(score - 10, 65)

    elif earnings_hits:
        score     = 72
        catalyst  = "earnings_guidance"
        label     = "Earnings Shock"
        bearish_earn = {"guidance lowered", "earnings miss", "below estimates",
                        "revenue miss", "profit warning", "guidance cut"}
        bullish_earn = {"guidance raised", "earnings beat", "above estimates",
                        "revenue beat", "earnings surprise"}
        has_bearish = any(h in bearish_earn for h in earnings_hits)
        has_bullish = any(h in bullish_earn for h in earnings_hits)
        if has_bearish and has_bullish:
            bull_bear = "mixed"
        elif has_bearish:
            bull_bear = "bearish"
        elif has_bullish:
            bull_bear = "bullish"

    elif deal_hits:
        # Major deal language without a title-level hyperscaler/gov anchor
        score     = 68
        catalyst  = "commercial_contract"
        label     = "Major Contract"
        bull_bear = "bullish"
        if any(kw in ("strategic partnership", "partnership", "collaboration")
               for kw in deal_hits):
            catalyst = "strategic_partnership"
            label    = "Strategic Partnership"
        if bearish_hits:
            bull_bear = "mixed"
            score     = max(score - 8, 40)

    elif tech_hits:
        score     = 62
        catalyst  = "technical_milestone"
        label     = "Technical Breakthrough"
        bull_bear = "bullish"

    elif bearish_hits:
        score     = 66
        catalyst  = "financing"
        label     = "Financing / Dilution"
        bull_bear = "bearish"

    elif hyperscaler_all:
        # Hyperscaler only in summary, no deal language → routine mention
        score    = 25
        catalyst = "routine"
        label    = "Routine Mention"

    # ── 5. Derive final fields ────────────────────────────────────────────────
    score = max(0, min(100, score))

    if score >= 85:
        signal_strength = "high"
    elif score >= 65:
        signal_strength = "medium"
    elif score >= 40:
        signal_strength = "low"
    else:
        signal_strength = "none"

    is_major = score >= 65

    why = _why_it_matters(catalyst, matched_entities, matched_keywords, bull_bear)
    sq  = _source_quality(source)

    return {
        **article,
        "is_major_development":        is_major,
        "major_news_score":            score,
        "major_news_label":            label,
        "catalyst_type":               catalyst,
        "signal_strength":             signal_strength,
        "bull_bear_impact":            bull_bear,
        "why_it_matters":              why,
        "matched_entities":            matched_entities,
        "matched_keywords":            matched_keywords[:10],
        "related_watchlist_symbols":   [ticker_symbol.upper()] if ticker_symbol else [],
        "source_quality":              sq,
    }
