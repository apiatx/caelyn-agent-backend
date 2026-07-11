"""
Theme Policy Catalyst V1
========================
Deterministic government / Trump / White House / federal strategic investment
detection layer.  Scans existing cached RSS articles for policy signals that
can lift an ENTIRE THEME, not just one ticker.

Answers the question: "Did something happen that should boost this entire theme?"
(Ticker-specific Catalyst V2 answers: "Did something happen for this company?")

Classification requires ALL THREE gates:
  1. Policy actor    — WHO announced it (Trump/White House/DoD/DOE/CHIPS/…)
  2. Policy mechanism — WHAT they committed (funding/grant/tariff/stockpile/…)
  3. Theme match     — WHICH investable theme is affected

Without all three → rejected, no boost.

Zero provider calls.  Zero LLM.  Zero new DB tables.
Pure read from the bulk_articles dict already fetched inside get_catalyst_alignment_bulk().

Shadow fields added per ticker (additive, no scoring changes):
    theme_policy_available       bool
    theme_policy_score           float | None  (0-100)
    theme_policy_boost           float         (0-15, added to catalyst_alignment_score)
    theme_policy_event           dict | None   (best matching policy event)
    theme_policy_source          str | None    ("rss_archive")
    theme_policy_theme           str | None    (canonical theme_id matched)
    theme_policy_relevance       float         (0.0-1.0)
    theme_policy_reason_codes    list[str]
"""
from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

# ── Relevance weights by theme membership tier ────────────────────────────────
_REL_PRIMARY    = 1.00   # ticker's first/primary theme is the affected theme
_REL_ADDITIONAL = 0.60   # ticker has the theme as a secondary membership
_REL_NONE       = 0.00

# ── Boost cap (hard ceiling, regardless of score × relevance) ────────────────
_BOOST_CAP   = 15.0
_BOOST_FACTOR = 0.15   # theme_policy_boost = min(CAP, score × relevance × FACTOR)

# ── Minimum boost to make catalyst available from zero (no ticker catalyst) ──
_SOLO_BOOST_MIN = 8.0

# ── Policy availability threshold (same as catalyst V2) ──────────────────────
_POLICY_AVAIL_THRESHOLD = 25.0

# ── Corroboration bonus for multiple policy events hitting same theme ─────────
_CORROBORATION_PER = 3.0
_CORROBORATION_CAP = 10.0

# ── Article lookback for policy scanning ─────────────────────────────────────
_DEFAULT_HOURS = 96

# ══════════════════════════════════════════════════════════════════════════════
# GATE 1 — POLICY ACTOR PATTERNS
# At least one must match for article to be considered a policy signal.
# ══════════════════════════════════════════════════════════════════════════════

_ACTOR_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\btrump\b", re.I),                                           "Trump"),
    (re.compile(r"\bwhite\s+house\b", re.I),                                   "White House"),
    (re.compile(r"\badministration\b(?=.*(?:plan|fund|invest|support|push|move|back|announce|order|sign|polic|initiat|strateg|priorit|secur|requir|mandate))", re.I), "Administration"),
    (re.compile(r"\bu\.s\.\s+government\b|\bfederal\s+government\b", re.I),    "U.S. Government"),
    (re.compile(r"\bdepartment\s+of\s+energy\b|\bdoe\b(?=.*(?:fund|grant|award|strateg|invest|polic|target|loan|program))", re.I), "DOE"),
    (re.compile(r"\bdepartment\s+of\s+defense\b|\bpentagon\b", re.I),          "DoD/Pentagon"),
    (re.compile(r"\b(?:the\s+)?dod\b(?=.*(?:fund|award|contract|program|strateg|invest|procur|spend))", re.I), "DoD"),
    (re.compile(r"\bcommerce\s+department\b|\bcommerce\s+secretary\b", re.I),  "Commerce"),
    (re.compile(r"\bu\.s\.\s+treasury\b|\btreasury\s+department\b|\btreasury\s+secretary\b", re.I), "Treasury"),
    (re.compile(r"\bnasa\b", re.I),                                             "NASA"),
    (re.compile(r"\bchips\s+act\b", re.I),                                     "CHIPS Act"),
    (re.compile(r"\bira\b(?=.*(?:fund|grant|clean|energy|solar|battery|manufactur))", re.I), "IRA"),
    (re.compile(r"\bexecutive\s+order\b", re.I),                               "Executive Order"),
    (re.compile(r"\bcongress\b(?=.*(?:pass|approv|fund|bill|allocat|sign))", re.I), "Congress"),
    (re.compile(r"\bsenate\b(?=.*(?:pass|approv|vote|fund|bill))", re.I),      "Senate"),
    (re.compile(r"\bnational\s+security\s+(?:council|advisor|memorandum|directive)\b", re.I), "NSC"),
    (re.compile(r"\bstrategic\s+(?:national\s+)?reserve\b", re.I),             "Strategic Reserve"),
    (re.compile(r"\bgovernment\s+(?:stake|equity|investment|funding|support|backing)\b", re.I), "Government Investment"),
    (re.compile(r"\bstate\s+department\b|\bsecretary\s+of\s+state\b", re.I),   "State Dept"),
    (re.compile(r"\bfederal\s+(?:reserve|agency|program|contract|grant|fund|invest)\b", re.I), "Federal"),
]

# ══════════════════════════════════════════════════════════════════════════════
# GATE 2 — POLICY MECHANISM PATTERNS
# At least one must match.  Each has a "mechanism strength" (0.3–1.0) used
# to determine materiality.  Stronger = harder capital commitment.
# ══════════════════════════════════════════════════════════════════════════════

_MECHANISM_PATTERNS: list[tuple[re.Pattern, str, float]] = [
    # (compiled pattern, display_name, strength)
    (re.compile(r"\bstrategic\s+(?:stockpile|reserve|accumulation)\b", re.I),      "strategic reserve",      1.00),
    (re.compile(r"\bstockpil\w*\b",                                      re.I),     "stockpile",              0.95),
    (re.compile(r"\breserve\s+accumulation\b",                           re.I),     "reserve accumulation",   0.95),
    (re.compile(r"\bloan\s+guarantee\b",                                 re.I),     "loan guarantee",         0.90),
    (re.compile(r"\bgovernment\s+(?:equity\s+)?stake\b",                 re.I),     "government stake",       0.90),
    (re.compile(r"\bexecutive\s+order\b",                                re.I),     "executive order",        0.90),
    (re.compile(r"\bprocurement\b",                                      re.I),     "procurement",            0.85),
    (re.compile(r"\bawards?\s+contracts?\b|\bawarded?\s+(?:a\s+)?contracts?\b|\bcontract\s+award\b",re.I), "contract award", 0.85),
    (re.compile(r"\bstrategic\s+investment\b",                           re.I),     "strategic investment",   0.85),
    (re.compile(r"\bexport\s+(?:control|restriction|ban)\b",             re.I),     "export control",         0.82),
    (re.compile(r"\btariff\b",                                           re.I),     "tariff",                 0.78),
    (re.compile(r"\bgrants?\b",                                          re.I),     "grant",                  0.78),
    (re.compile(r"\bsubsid(?:y|ies|ize)\b",                              re.I),     "subsidy",                0.75),
    (re.compile(r"\bfunding\b",                                          re.I),     "funding",                0.72),
    (re.compile(r"\bnational\s+priority\b",                              re.I),     "national priority",      0.70),
    (re.compile(r"\bstrategic\s+(?:priority|initiative|plan)\b",         re.I),     "strategic priority",     0.68),
    (re.compile(r"\bmandate\b",                                          re.I),     "mandate",                0.68),
    (re.compile(r"\breshoring\b|\bre.shoring\b",                         re.I),     "reshoring",              0.65),
    (re.compile(r"\bdomestic\s+(?:production|manufacturing|supply chain)\b", re.I), "domestic production",    0.63),
    (re.compile(r"\btax\s+credit\b|\btax\s+incentive\b",                 re.I),     "tax incentive",          0.60),
    (re.compile(r"\bmilitary\s+(?:spending|budget|buildup)\b",           re.I),     "defense spending",       0.65),
    (re.compile(r"\bdefense\s+(?:spending|budget|appropriation)\b",      re.I),     "defense appropriation",  0.65),
    (re.compile(r"\binvest(?:ment|ing)\b(?=.*\d+\s*(?:billion|million|trillion|B\b|M\b))", re.I), "investment commitment", 0.80),
]

# ══════════════════════════════════════════════════════════════════════════════
# GATE 3 — THEME MATCH PATTERNS
# Ordered from most to least specific.  First match within a theme wins.
# Maps to canonical theme_ids used in ticker_theme_idx.
# ══════════════════════════════════════════════════════════════════════════════

_THEME_PATTERNS: list[tuple[str, list[re.Pattern]]] = [
    ("rare_earth", [
        re.compile(r"\brare\s+earth\b", re.I),
        re.compile(r"\bcritical\s+mineral", re.I),
        re.compile(r"\bneodymium\b|\bdysprosium\b|\bterbium\b|\bpraseodymium\b", re.I),
        re.compile(r"\bpermanent\s+magnet\b", re.I),
        re.compile(r"\b(?:REE|MREE|HREE)\b"),
        re.compile(r"\bgallium\b|\bgermanium\b|\bindium\b|\bbismuth\b", re.I),
    ]),
    ("quantum", [
        re.compile(r"\bquantum\s+(?:computing|technology|tech|network|sensing|security|communication|internet|encryption|advantage|supremacy|initiative|program)\b", re.I),
        re.compile(r"\bqubit\b", re.I),
        re.compile(r"\bnational\s+quantum\s+initiative\b", re.I),
    ]),
    ("uranium_nuclear", [
        re.compile(r"\bnuclear\s+(?:energy|power|plant|reactor|program|initiative|fuel|renaissance)\b", re.I),
        re.compile(r"\buranium\b", re.I),
        re.compile(r"\bSMR\b|small\s+modular\s+reactor\b", re.I),
        re.compile(r"\badvanced\s+nuclear\b|\bnext.gen\s+nuclear\b", re.I),
        re.compile(r"\bfission\s+(?:energy|power)\b|\bnuclear\s+fission\b", re.I),
        re.compile(r"\bdoe\b.*nuclear|nuclear.*\bdoe\b", re.I),
    ]),
    ("gold", [
        re.compile(r"\bgold\s+(?:reserve|stockpile|holding|purchase|buying|accumulation)\b", re.I),
        re.compile(r"\bgold\s+standard\b", re.I),
        re.compile(r"\bstrategic\s+gold\b|\bgold\s+backed\b", re.I),
        re.compile(r"\bbullion\s+reserve\b", re.I),
        re.compile(r"\bgold\b", re.I),   # fallback — actor+mechanism gates ensure policy context
    ]),
    ("silver", [
        re.compile(r"\bsilver\s+(?:reserve|stockpile|holding|purchase|accumulation)\b", re.I),
        re.compile(r"\bstrategic\s+silver\b", re.I),
        re.compile(r"\bsilver\s+industrial\b", re.I),
        re.compile(r"\bsilver\b", re.I),  # fallback — actor+mechanism gates ensure policy context
    ]),
    ("metals_mining", [
        re.compile(r"\bstrategic\s+metals?\b|\bcritical\s+metals?\b", re.I),
        re.compile(r"\bdomestic\s+mining\b|\bmining\s+(?:reform|policy|investment)\b", re.I),
        re.compile(r"\bcobalt\s+(?:reserve|supply|mining)\b", re.I),
    ]),
    ("copper_miners", [
        re.compile(r"\bcopper\s+(?:reserve|supply|production|mining|tariff)\b", re.I),
        re.compile(r"\bdomestic\s+copper\b", re.I),
    ]),
    ("semiconductors", [
        re.compile(r"\bsemiconductor\b", re.I),
        re.compile(r"\bchips?\s+(?:act|manufacturing|production|fab|supply)\b", re.I),
        re.compile(r"\bdomestic\s+(?:chip|wafer|fab)\b", re.I),
        re.compile(r"\badvanced\s+(?:semiconductor|chip|fab)\b", re.I),
        re.compile(r"\bchips\s+act\b|\bfab\w*\s+(?:fund|grant|invest|subsidi)\b", re.I),
    ]),
    ("datacenter_infra", [
        re.compile(r"\bdata\s*center\b", re.I),
        re.compile(r"\bAI\s+infrastructure\b|\bartificial\s+intelligence\s+infrastructure\b", re.I),
        re.compile(r"\bsovereign\s+AI\b", re.I),
        re.compile(r"\bcompute\s+(?:capacity|investment|buildout)\b", re.I),
        re.compile(r"\bAI\s+(?:compute|server|cluster|campus)\b", re.I),
    ]),
    ("ai_networking", [
        re.compile(r"\bAI\s+(?:strategy|initiative|policy|investment|act|program|roadmap)\b", re.I),
        re.compile(r"\bartificial\s+intelligence\s+(?:act|policy|strategy|initiative|investment)\b", re.I),
        re.compile(r"\bnational\s+AI\b|\bfederal\s+AI\b", re.I),
        re.compile(r"\bAI\s+chip\s+export\b|\bAI\s+export\s+(?:control|rule|ban)\b", re.I),
    ]),
    ("power_cooling", [
        re.compile(r"\bpower\s+grid\b|\belectrical\s+grid\b|\btransmission\s+grid\b", re.I),
        re.compile(r"\bgrid\s+modernization\b|\belectric\s+grid\s+(?:invest|upgrade|expand|fund)\b", re.I),
        re.compile(r"\belectricity\s+(?:infrastructure|grid|transmission|invest)\b", re.I),
        re.compile(r"\benergy\s+grid\b|\bgrid\s+(?:expan|invest|fund|upgrade)\b", re.I),
    ]),
    ("lng_gas", [
        re.compile(r"\bLNG\b", re.I),
        re.compile(r"\bliquefied\s+natural\s+gas\b", re.I),
        re.compile(r"\bnatural\s+gas\s+(?:export|terminal|infrastructure|invest)\b", re.I),
        re.compile(r"\bLNG\s+(?:export|terminal|facility|project)\b", re.I),
    ]),
    ("defense", [
        re.compile(r"\bdefense\s+(?:spending|appropriation|procurement|budget|contractor|manufacturer)\b", re.I),
        re.compile(r"\bmilitary\s+(?:buildup|spending|modernization|procurement)\b", re.I),
        re.compile(r"\bnational\s+security\s+(?:invest|fund|spend|strateg|polic)\b", re.I),
        re.compile(r"\bDoD\b.*(?:fund|invest|contract|award|procur)", re.I),
        re.compile(r"\bpentagon\b.*(?:fund|invest|contract|award|procur)", re.I),
    ]),
    ("drones", [
        re.compile(r"\bdrone\s+(?:program|policy|invest|procur|strateg|fleet|manufactur)\b", re.I),
        re.compile(r"\bUAV\b|\bunmanned\s+aerial\b", re.I),
        re.compile(r"\bautonomous\s+(?:weapons?|vehicle|drone|system)\b", re.I),
        re.compile(r"\bcounterudrone\b|\bcounter-drone\b|\bc-UAS\b", re.I),
    ]),
    ("space", [
        re.compile(r"\bspace\s+(?:force|program|infrastructure|invest|fund|defense|economy)\b", re.I),
        re.compile(r"\bNASA\b.*(?:fund|invest|mission|contract|award)\b", re.I),
        re.compile(r"\bsatellite\s+(?:program|invest|fund|infrastr)\b", re.I),
        re.compile(r"\blaunch\s+vehicle\s+(?:program|invest|fund)\b", re.I),
        re.compile(r"\bcommercial\s+space\b.*(?:invest|fund|strateg|polic)\b", re.I),
    ]),
    ("robotics_automation", [
        re.compile(r"\brobotics?\s+(?:invest|fund|manufactur|polic|strateg|program)\b", re.I),
        re.compile(r"\bmanufacturing\s+(?:reshoring|automation|robot|moderniz)\b", re.I),
        re.compile(r"\bdomestic\s+(?:robot|manufactur|reshoring)\b", re.I),
        re.compile(r"\badvanced\s+manufacturing\b.*(?:fund|invest|strateg|polic|grant)\b", re.I),
    ]),
    ("solar", [
        re.compile(r"\bsolar\s+(?:energy|panel|manufactur|invest|fund|tariff|subsidi)\b", re.I),
        re.compile(r"\bIRA\b.*(?:solar|clean|renewable)\b|\bclean\s+energy\b.*\bIRA\b", re.I),
        re.compile(r"\bphotovoltaic\b", re.I),
        re.compile(r"\bdomestic\s+solar\b|\bsolar\s+domestic\b", re.I),
    ]),
    ("lithium_battery", [
        re.compile(r"\blithium\s+(?:battery|mining|supply|reserve|strategic)\b", re.I),
        re.compile(r"\bbattery\s+(?:manufactur|invest|fund|domestic|supply)\b", re.I),
        re.compile(r"\belectric\s+vehicle\s+battery\b|\bEV\s+battery\b", re.I),
        re.compile(r"\bbattery\s+material\b|\bgigafactor\b", re.I),
    ]),
    ("semicap_equipment", [
        re.compile(r"\bsemiconductor\s+equipment\b|\bchip\s+equipment\b", re.I),
        re.compile(r"\bEUV\b|\bDUV\b|\blithography\b", re.I),
        re.compile(r"\bexport\s+control\b.*(?:chip|semiconductor|equipment|tool)\b", re.I),
        re.compile(r"\bchip\s+tool\s+export\b", re.I),
    ]),
    ("photonics_lasers", [
        re.compile(r"\bphotonics?\b.*(?:invest|fund|strateg|program|polic)\b", re.I),
        re.compile(r"\blaser\s+(?:weapon|defense|invest|fund)\b", re.I),
        re.compile(r"\bdirected\s+energy\b", re.I),
    ]),
    ("energy", [
        re.compile(r"\benergy\s+(?:independence|security|infrastructure|invest|strateg)\b", re.I),
        re.compile(r"\bDOE\b.*(?:invest|fund|award|grant|program|initiative)\b", re.I),
        re.compile(r"\bdomestic\s+energy\b|\bamerican\s+energy\b", re.I),
    ]),
    ("clean_energy", [
        re.compile(r"\bclean\s+energy\s+(?:invest|fund|strateg|policy|initiative|act)\b", re.I),
        re.compile(r"\brenewable\s+energy\s+(?:invest|fund|target|mandate)\b", re.I),
    ]),
    ("crypto_equities", [
        re.compile(r"\bbitcoin\s+(?:reserve|strategic|stockpile|national)\b", re.I),
        re.compile(r"\bcrypto\s+(?:reserve|policy|strategic|regulatory)\b", re.I),
        re.compile(r"\bdigital\s+asset\s+(?:reserve|policy|strategic|stockpile)\b", re.I),
        re.compile(r"\bstrategic\s+(?:bitcoin|crypto|digital\s+asset)\b", re.I),
    ]),
    ("cybersecurity", [
        re.compile(r"\bcybersecurity\s+(?:invest|fund|mandate|order|strateg|polic)\b", re.I),
        re.compile(r"\bnational\s+cybersecurity\b", re.I),
        re.compile(r"\bcyber\s+(?:defense|infrastructure|resilience)\b.*(?:invest|fund|polic|strateg)\b", re.I),
    ]),
]

# ── Bearish policy direction indicators ───────────────────────────────────────
_BEARISH_POLICY_PHRASES: list[re.Pattern] = [
    re.compile(r"\bfunding\s+cut\b|\bcut\s+funding\b", re.I),
    re.compile(r"\bcancel(?:ed|led)?\s+contract\b|\bcontract\s+cancel", re.I),
    re.compile(r"\bregulatory\s+crackdown\b", re.I),
    re.compile(r"\bsanction\b", re.I),
    re.compile(r"\bimport\s+(?:ban|restriction)\b.*(?:hurt|harm|impact)\b", re.I),
    re.compile(r"\btariff\b.*(?:hurt|harm|raise\s+cost|input\s+cost)\b", re.I),
    re.compile(r"\binvestigation\b.*(?:company|sector|industry)\b", re.I),
    re.compile(r"\bwithdraw(?:ing|al)?\s+(?:from\s+)?(?:funding|program|support)\b", re.I),
    re.compile(r"\bend(?:ing|ed)?\s+(?:subsidies|funding|support|program)\b", re.I),
]

# ── False-positive filters: reject generic political / roundup / opinion ──────
_FP_FILTERS: list[re.Pattern] = [
    re.compile(r"\bopinion\b|\bop.?ed\b|\bcolumn\b|\bcommentary\b", re.I),
    re.compile(r"\belection\b|\bcampaign\b|\bprimary\s+election\b|\bvoting\b", re.I),
    re.compile(r"\bpoll(?:ing|s)\b.*(?:trump|election|candidate)\b", re.I),
    re.compile(r"\bmarket\s+(?:rally|sell.off|pullback|recap|update|movers)\b", re.I),
    re.compile(r"\bstocks?\s+(?:rise|fall|move|gain|lose|rally|tumble)\b.*today\b", re.I),
    re.compile(r"\bweekly\s+recap\b|\bdaily\s+(?:brief|wrap|roundup)\b", re.I),
    re.compile(r"\bmorning\s+(?:brief|wrap|roundup)\b", re.I),
    re.compile(r"\bpremarket\s+(?:movers|update)\b|\bafter.hours\s+movers\b", re.I),
    re.compile(r"\bwhat\s+to\s+watch\b|\bstocks\s+to\s+watch\b", re.I),
    re.compile(r"\bwhy\s+(?:\w+\s+)?(?:stock|shares)\s+(?:is|are)\s+(?:rising|falling|up|down)\b", re.I),
    re.compile(r"\bhere(?:'s| is)\s+what('s| is)\s+moving\b", re.I),
]

# ── Empty sentinel ─────────────────────────────────────────────────────────────
THEME_POLICY_EMPTY: dict[str, Any] = {
    "theme_policy_available":    False,
    "theme_policy_score":        None,
    "theme_policy_boost":        0.0,
    "theme_policy_event":        None,
    "theme_policy_source":       None,
    "theme_policy_theme":        None,
    "theme_policy_relevance":    0.0,
    "theme_policy_reason_codes": ["THEME_POLICY_NO_MATCH"],
}


# ══════════════════════════════════════════════════════════════════════════════
# Internal helpers
# ══════════════════════════════════════════════════════════════════════════════

def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _is_false_positive(text: str) -> bool:
    for pat in _FP_FILTERS:
        if pat.search(text):
            return True
    return False


def _find_actors(text: str) -> list[str]:
    found = []
    for pat, name in _ACTOR_PATTERNS:
        if pat.search(text):
            found.append(name)
    return found


def _find_mechanisms(text: str) -> list[tuple[str, float]]:
    """Returns list of (mechanism_name, strength) for all matching mechanisms."""
    found = []
    for pat, name, strength in _MECHANISM_PATTERNS:
        if pat.search(text):
            found.append((name, strength))
    return found


def _find_themes(text: str) -> list[str]:
    """Returns list of canonical theme_ids matched in the text."""
    found = []
    for theme_id, patterns in _THEME_PATTERNS:
        for pat in patterns:
            if pat.search(text):
                found.append(theme_id)
                break   # one match per theme is enough
    return found


def _detect_direction(text: str, mechanisms: list[tuple[str, float]]) -> str:
    for pat in _BEARISH_POLICY_PHRASES:
        if pat.search(text):
            return "bearish"
    # Tariff alone (without explicit harm language) is treated bullish for domestic suppliers
    return "bullish"


def _compute_materiality(mechanisms: list[tuple[str, float]], actors: list[str]) -> float:
    if not mechanisms:
        return 0.0
    best_strength = max(s for _, s in mechanisms)
    # Actor quality bonus: named actor (Trump/exec order) = 1.0, generic = 0.85
    actor_quality = 1.0 if any(a in ("Trump", "White House", "Executive Order") for a in actors) else 0.85
    return round(_clamp(best_strength * actor_quality, 0.0, 1.0), 4)


def _compute_policy_score(materiality: float, theme_count: int) -> float:
    corr = min(_CORROBORATION_CAP, max(0, theme_count - 1) * _CORROBORATION_PER)
    return round(_clamp(materiality * 100.0 + corr, 0.0, 100.0), 1)


def _parse_pub_dt(pub_str: str) -> Optional[datetime]:
    if not pub_str:
        return None
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(pub_str).astimezone(timezone.utc)
    except Exception:
        pass
    try:
        return datetime.fromisoformat(pub_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def _score_article_for_policy(
    article_key: str,
    title: str,
    summary: str,
    published_at: str,
    source: str,
    cutoff_dt: datetime,
) -> Optional[dict]:
    """
    Evaluate a single article for policy signal.

    Returns a policy_event dict if all three gates pass, else None.
    """
    # Time gate
    pub_dt = _parse_pub_dt(published_at)
    if pub_dt is None or pub_dt < cutoff_dt:
        return None

    text = f"{title} {summary}"

    # False-positive filter
    if _is_false_positive(text):
        return None

    # Gate 1 — actor
    actors = _find_actors(text)
    if not actors:
        return None

    # Gate 2 — mechanism
    mechanisms = _find_mechanisms(text)
    if not mechanisms:
        return None

    # Gate 3 — theme
    affected_themes = _find_themes(text)
    if not affected_themes:
        return None

    materiality = _compute_materiality(mechanisms, actors)
    direction   = _detect_direction(text, mechanisms)

    policy_keywords = [m for m, _ in mechanisms[:5]]
    policy_actors   = actors[:5]

    event_type = "government_strategic_investment"
    mech_names = {m for m, _ in mechanisms}
    if "executive order" in mech_names:
        event_type = "federal_procurement_push"
    elif any(m in mech_names for m in ("export control", "tariff")):
        event_type = "tariff_policy" if "tariff" in mech_names else "export_control_policy"
    elif any(m in mech_names for m in ("strategic reserve", "stockpile", "reserve accumulation")):
        event_type = "reserve_accumulation"
    elif "procurement" in mech_names or "contract award" in mech_names:
        event_type = "federal_procurement_push"
    elif any(t in affected_themes for t in ("defense", "drones", "space")):
        event_type = "defense_policy"
    elif any(t in affected_themes for t in ("uranium_nuclear",)):
        event_type = "nuclear_policy"
    elif any(t in affected_themes for t in ("quantum",)):
        event_type = "quantum_policy_tailwind"
    elif any(t in affected_themes for t in ("datacenter_infra", "ai_networking")):
        event_type = "ai_infrastructure_policy"
    elif any(t in affected_themes for t in ("rare_earth", "metals_mining", "copper_miners")):
        event_type = "critical_minerals_policy"
    elif any(t in affected_themes for t in ("solar", "clean_energy", "lithium_battery")):
        event_type = "energy_grid_policy"
    elif any(t in affected_themes for t in ("semiconductors", "semicap_equipment")):
        event_type = "national_security_priority"
    elif any(t in affected_themes for t in ("crypto_equities",)):
        event_type = "reserve_accumulation"

    return {
        "policy_event_id":          f"pol:{article_key[:40]}",
        "policy_event_type":        event_type,
        "policy_direction":         direction,
        "policy_materiality_score": materiality,
        "policy_actors":            policy_actors,
        "policy_mechanisms":        policy_keywords,
        "policy_source":            "rss_archive",
        "policy_title":             title[:300],
        "policy_published_at":      published_at,
        "affected_theme_ids":       affected_themes,
        "policy_keywords":          policy_keywords,
        "article_key":              article_key,
    }


def detect_policy_events(
    bulk_articles: dict[str, list[dict]],
    hours: int = _DEFAULT_HOURS,
) -> list[dict]:
    """
    Scan all cached articles across all tickers for policy/government signals.

    Deduplicates by article_key so the same headline tagged to multiple tickers
    is only counted once.  Returns events sorted by materiality descending.

    Arguments:
        bulk_articles  {ticker: [article_dict, ...]}  — already fetched by caller
        hours          lookback window in hours (default 96)

    Returns:
        list[policy_event_dict] — may be empty if no policy signals found
    """
    if not bulk_articles:
        return []

    cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=hours)
    seen_keys: set[str] = set()
    events: list[dict] = []

    for _ticker, articles in bulk_articles.items():
        for art in (articles or []):
            art_key = art.get("_article_key") or art.get("article_key") or ""
            if not art_key or art_key in seen_keys:
                continue
            seen_keys.add(art_key)

            ev = _score_article_for_policy(
                article_key  = art_key,
                title        = art.get("title") or "",
                summary      = art.get("summary") or "",
                published_at = art.get("published_at") or "",
                source       = art.get("source") or "",
                cutoff_dt    = cutoff_dt,
            )
            if ev is not None:
                events.append(ev)

    events.sort(key=lambda e: e["policy_materiality_score"], reverse=True)
    return events


def compute_theme_policy_fields(
    sym: str,
    sym_theme_ids: list[str],
    policy_events: list[dict],
) -> dict:
    """
    Compute per-ticker theme_policy_* shadow fields.

    Arguments:
        sym             ticker symbol (for logging only)
        sym_theme_ids   canonical theme_ids for this ticker (primary first)
                        from ticker_theme_idx.get(sym, [])
        policy_events   output of detect_policy_events()

    Returns:
        dict with keys: theme_policy_available, theme_policy_score,
                        theme_policy_boost, theme_policy_event,
                        theme_policy_source, theme_policy_theme,
                        theme_policy_relevance, theme_policy_reason_codes
    """
    if not policy_events or not sym_theme_ids:
        rc = []
        if not policy_events:
            rc.append("THEME_POLICY_NO_EVENTS_IN_ARCHIVE")
        if not sym_theme_ids:
            rc.append("THEME_POLICY_NO_THEME_MEMBERSHIP")
        return {**THEME_POLICY_EMPTY, "theme_policy_reason_codes": rc}

    primary_theme_id    = sym_theme_ids[0] if sym_theme_ids else None
    additional_theme_ids = set(sym_theme_ids[1:]) if len(sym_theme_ids) > 1 else set()

    best_event:     Optional[dict] = None
    best_score:     float          = 0.0
    best_relevance: float          = 0.0
    best_theme_id:  Optional[str]  = None

    # Accumulate events matching this ticker's themes
    matching_count = 0

    for ev in policy_events:
        if ev.get("policy_direction") == "bearish":
            continue  # skip bearish — ticker-level catalyst already captures downside
        affected = set(ev.get("affected_theme_ids") or [])
        if not affected:
            continue

        # Determine relevance tier
        relevance = 0.0
        matched_theme_id = None
        if primary_theme_id and primary_theme_id in affected:
            relevance = _REL_PRIMARY
            matched_theme_id = primary_theme_id
        elif additional_theme_ids & affected:
            relevance = _REL_ADDITIONAL
            matched_theme_id = next(iter(additional_theme_ids & affected))
        else:
            continue

        matching_count += 1
        mat  = float(ev.get("policy_materiality_score") or 0.0)
        score = _compute_policy_score(mat, len(affected))

        # Pick best event (highest score × relevance)
        weighted = score * relevance
        best_weighted = best_score * best_relevance
        if weighted > best_weighted or best_event is None:
            best_event     = ev
            best_score     = score
            best_relevance = relevance
            best_theme_id  = matched_theme_id

    if best_event is None:
        return {**THEME_POLICY_EMPTY, "theme_policy_reason_codes": ["THEME_POLICY_NO_THEME_MATCH"]}

    # Corroboration: additional matching events beyond the best one
    corr = min(_CORROBORATION_CAP, max(0, matching_count - 1) * _CORROBORATION_PER)
    adjusted_score = round(_clamp(best_score + corr, 0.0, 100.0), 1)

    boost = round(min(_BOOST_CAP, adjusted_score * best_relevance * _BOOST_FACTOR), 2)

    available = adjusted_score >= _POLICY_AVAIL_THRESHOLD and best_relevance > 0

    reason_codes = ["THEME_POLICY_MATCH"]
    if best_relevance == _REL_PRIMARY:
        reason_codes.append("PRIMARY_THEME_MATCH")
    else:
        reason_codes.append("ADDITIONAL_THEME_MATCH")
    if matching_count > 1:
        reason_codes.append(f"CORROBORATED_BY_{matching_count}_EVENTS")

    return {
        "theme_policy_available":    available,
        "theme_policy_score":        adjusted_score if available else None,
        "theme_policy_boost":        boost,
        "theme_policy_event":        best_event,
        "theme_policy_source":       "rss_archive",
        "theme_policy_theme":        best_theme_id,
        "theme_policy_relevance":    round(best_relevance, 2),
        "theme_policy_reason_codes": reason_codes,
    }
