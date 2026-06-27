"""
Predict / Investor — Event Family Grouping.

Maps raw Polymarket market questions to canonical event_family_keys so that
similar markets ("Hormuz by Jul 15", "Hormuz by Jul 31") collapse into one
grouped event rather than producing separate conflicting equity signal cards.

All classification is rule-based (keyword patterns).  No LLM, no HTTP.
"""

from __future__ import annotations

import hashlib
import re
from typing import Optional

# ── Family rules ──────────────────────────────────────────────────────────────
#
# Each tuple: (family_key, display_label, primary_category, [keyword patterns])
# Patterns are lowercased substrings of the market question.
# First match wins — order by specificity (longer/more-specific patterns first).

_FAMILY_RULES: list[tuple[str, str, str, list[str]]] = [

    # ── Strait of Hormuz / Iran / Persian Gulf ────────────────────────────────
    ("hormuz_iran", "Strait of Hormuz / Iran",
     "Geopolitics / Energy / Shipping",
     ["hormuz", "strait of hormuz", "persian gulf blockade"]),

    # ── Red Sea / Houthi / Shipping lanes ─────────────────────────────────────
    ("red_sea_houthis", "Red Sea / Houthi Shipping",
     "Geopolitics / Energy / Shipping",
     ["red sea", "houthi", "bab el-mandeb", "bab-el-mandeb"]),

    # ── Russia / Ukraine ───────────────────────────────────────────────────────
    ("russia_ukraine", "Russia / Ukraine War",
     "Geopolitics / War",
     ["russia", "ukraine", "zelensky", "zelenskyy", "russian invasion",
      "donbas", "crimea", "kyiv", "kharkiv"]),

    # ── Israel / Gaza / Hamas / Hezbollah ─────────────────────────────────────
    ("israel_gaza", "Israel / Gaza / Hamas",
     "Geopolitics / Middle East",
     ["israel", "gaza", "hamas", "hezbollah", "west bank", "idf",
      "netanyahu", "palestin"]),

    # ── China / Taiwan Strait (must come before generic "china" if any) ────────
    ("china_taiwan", "China / Taiwan Strait Tension",
     "Geopolitics / Tech / Supply Chain",
     ["taiwan strait", "taiwan invasion", "china invades taiwan",
      "pla crossing", "tsmc takeover", "taiwan independence",
      "china blockade taiwan", "strait blockade"]),

    # ── China / US relations (generic, after Taiwan-specific) ─────────────────
    ("china_us_relations", "China / US Relations",
     "Geopolitics / Trade",
     ["china", "chinese", "xi jinping", "beijing", "sino-us",
      "huawei", "tiktok", "bytedance", "hong kong"]),

    # ── North Korea ────────────────────────────────────────────────────────────
    ("north_korea", "North Korea",
     "Geopolitics",
     ["north korea", "kim jong", "dprk", "north korean"]),

    # ── Fed rate decision (specific meeting) ───────────────────────────────────
    ("fed_rate_decision", "Fed Rate Decision",
     "Fed / Rates / Macro",
     ["fed rate", "fomc rate", "federal reserve rate",
      "cut rates at", "hike rates at", "25bp", "50bp", "75bp",
      "rate decision", "fomc meeting"]),

    # ── Fed cuts count 2026 ────────────────────────────────────────────────────
    ("fed_cuts_2026", "Number of Fed Cuts in 2026",
     "Fed / Rates / Macro",
     ["cuts in 2026", "rate cuts in 2026", "how many cuts 2026",
      "fed cuts 2026", "number of cuts in 2026"]),

    # ── Fed hikes count 2026 ───────────────────────────────────────────────────
    ("fed_hikes_2026", "Fed Hikes in 2026",
     "Fed / Rates / Macro",
     ["hikes in 2026", "rate hikes in 2026", "fed hikes 2026"]),

    # ── Recession ──────────────────────────────────────────────────────────────
    ("recession_probability", "US Recession Probability",
     "Macro / Economy",
     ["recession", "gdp contraction", "gdp negative",
      "technical recession", "nber recession"]),

    # ── S&P 500 / broad market daily ───────────────────────────────────────────
    ("spx_daily_direction", "S&P 500 Daily Direction",
     "Equities / Market Direction",
     ["s&p 500", "s&p500", "spx close", "spy close",
      "market close", "dow close", "nasdaq close"]),

    # ── NVDA price milestones ──────────────────────────────────────────────────
    ("nvda_price_milestone", "NVDA Price Milestone",
     "Equities / Tech",
     ["nvidia above", "nvda above", "nvda hit", "nvda reach",
      "nvidia reach", "nvidia price", "nvda price"]),

    # ── NVDA earnings ─────────────────────────────────────────────────────────
    ("earnings_nvda", "NVDA Earnings",
     "Earnings / Tech",
     ["nvidia earnings", "nvda earnings", "nvidia revenue",
      "nvda revenue", "nvidia eps", "nvda eps"]),

    # ── TSLA price milestones ─────────────────────────────────────────────────
    ("tsla_price_milestone", "Tesla Price Milestone",
     "Equities / Tech",
     ["tesla above", "tsla above", "tesla hit", "tsla reach",
      "tesla price", "tsla price"]),

    # ── TSLA earnings ─────────────────────────────────────────────────────────
    ("earnings_tsla", "Tesla Earnings / Deliveries",
     "Earnings / Tech",
     ["tesla earnings", "tsla earnings", "tesla revenue",
      "tsla revenue", "tesla deliveries", "tsla deliveries"]),

    # ── AAPL price milestones ─────────────────────────────────────────────────
    ("aapl_price_milestone", "Apple Price Milestone",
     "Equities / Tech",
     ["apple above", "aapl above", "apple hit", "aapl reach",
      "apple price", "aapl price"]),

    # ── MSFT price milestones ─────────────────────────────────────────────────
    ("msft_price_milestone", "Microsoft Price Milestone",
     "Equities / Tech",
     ["microsoft above", "msft above", "microsoft hit", "msft reach"]),

    # ── GOOGL / Alphabet price milestones ─────────────────────────────────────
    ("googl_price_milestone", "Google / Alphabet Price Milestone",
     "Equities / Tech",
     ["google above", "googl above", "alphabet above", "google hit",
      "googl hit"]),

    # ── AMD price milestones ──────────────────────────────────────────────────
    ("amd_price_milestone", "AMD Price Milestone",
     "Equities / Tech",
     ["amd above", "amd hit", "amd reach", "amd price"]),

    # ── AI / Chip export controls ─────────────────────────────────────────────
    ("ai_export_controls", "AI / Chip Export Controls",
     "AI / Tech / Regulation",
     ["export control", "chip ban", "ai restriction", "nvidia ban",
      "entity list", "chip restriction", "semiconductor export",
      "ai export", "gpu export"]),

    # ── Bitcoin price milestones ──────────────────────────────────────────────
    ("bitcoin_price", "Bitcoin Price Milestone",
     "Crypto",
     ["bitcoin above", "btc above", "bitcoin hit", "bitcoin reach",
      "btc hit", "btc reach"]),

    # ── Crude / oil price milestones ──────────────────────────────────────────
    ("oil_price_milestone", "Oil / Crude Price Milestone",
     "Commodities / Energy",
     ["crude above", "oil above", "wti above", "brent above",
      "crude below", "oil below", "wti below", "brent below"]),

    # ── Gold price milestones ─────────────────────────────────────────────────
    ("gold_price_milestone", "Gold Price Milestone",
     "Commodities / Safe Haven",
     ["gold above", "gold hit", "gold reach", "gold price",
      "xau above"]),

    # ── US tariffs / trade war (generic) ─────────────────────────────────────
    ("us_tariffs", "US Tariffs / Trade War",
     "Trade / Geopolitics",
     ["tariff", "trade war", "import duty", "trade deficit",
      "reciprocal tariff"]),

    # ── CPI / inflation ───────────────────────────────────────────────────────
    ("cpi_inflation", "CPI / Inflation Reading",
     "Macro / Inflation",
     ["cpi above", "cpi below", "core cpi", "inflation above",
      "inflation below", "pce above", "pce below", "inflation report"]),
]


# ── Fast lookups ───────────────────────────────────────────────────────────────

_FAMILY_LABELS: dict[str, str] = {fk: label for fk, label, _, __ in _FAMILY_RULES}
_FAMILY_CATEGORIES: dict[str, str] = {fk: cat for fk, _, cat, __ in _FAMILY_RULES}


def classify_event_family(question: str) -> Optional[str]:
    """
    Return the canonical event_family_key for a market question, or None.
    First match wins (rules ordered by specificity).
    """
    q = question.lower()
    for family_key, _label, _cat, patterns in _FAMILY_RULES:
        if any(p in q for p in patterns):
            return family_key
    return None


# Strip date-ish suffixes so "Hormuz by Jul 15" and "Hormuz by Jul 31"
# produce the same synthetic key.
_DATE_STRIP_RE = re.compile(
    r"\b(by|before|after|on|through|until)\s+[\w]+\s*\d{1,2}[,.]?\s*\d{0,4}\b"
    r"|\d{4}-\d{2}-\d{2}"
    r"|\b(q[1-4]|h[12])\s*\d{4}\b"
    r"|\b(january|february|march|april|may|june|july|august|"
    r"september|october|november|december)\b",
    re.IGNORECASE,
)


def make_event_family_key(question: str) -> str:
    """
    Stable event_family_key for a market question.
    Rule-based first; falls back to a normalized stem hash.
    """
    canonical = classify_event_family(question)
    if canonical:
        return canonical

    # Strip dates and numbers, normalize whitespace
    stem = _DATE_STRIP_RE.sub("", question.lower())
    stem = re.sub(r"\d+", "", stem)
    stem = re.sub(r"[^\w\s]", " ", stem)
    stem = re.sub(r"\s+", " ", stem).strip()

    # Take first 7 meaningful words as stem key
    words = [w for w in stem.split() if len(w) > 2][:7]
    stem_key = "_".join(words)[:80]
    if stem_key:
        return stem_key

    # Last resort: md5 of raw question
    return "evt_" + hashlib.md5(question.encode()).hexdigest()[:10]


def get_family_label(family_key: str) -> str:
    """Human-readable title for the family key."""
    return _FAMILY_LABELS.get(family_key, family_key.replace("_", " ").title())


def get_family_primary_category(family_key: str, fallback_theme_id: str = "") -> str:
    """Primary category string for the family key."""
    if family_key in _FAMILY_CATEGORIES:
        return _FAMILY_CATEGORIES[family_key]
    _theme_to_cat: dict[str, str] = {
        "macro_rates_inflation":    "Macro / Rates / Inflation",
        "geopolitics_war_trade":    "Geopolitics / War / Trade",
        "energy_commodities":       "Energy / Commodities",
        "us_politics_policy":       "US Politics / Policy",
        "ai_semis_tech":            "AI / Semiconductors / Tech",
        "crypto_risk_appetite":     "Crypto / Risk Appetite",
        "china_taiwan_supply_chain": "China / Taiwan / Supply Chain",
        "defense_security":         "Defense / Security",
        "consumer_labor_growth":    "Consumer / Labor / Macro",
    }
    return _theme_to_cat.get(fallback_theme_id, "Macro / Markets")


def group_markets_by_family(markets: list[dict]) -> dict[str, list[dict]]:
    """Group a list of market dicts into {family_key: [market, ...]}."""
    groups: dict[str, list[dict]] = {}
    for m in markets:
        key = make_event_family_key(m.get("question", ""))
        groups.setdefault(key, []).append(m)
    return groups
