"""
Predict / Investor — Family Exposure Resolver.

Maps tracked-odds family_keys to semantic equity exposure using only the
canonical ENRICHED_THEME_RS_UNIVERSE.  No SECTOR_STOCKS hardcoded tickers
are used at any stage.

Resolution order:
  1. Watchlist symbols whose canonical theme memberships intersect impacted sectors.
  2. ENRICHED_THEME_RS_UNIVERSE proxy_symbols / candidate_symbols as fallback.
  3. Direct tickers (mega-cap families: NVDA, TSLA, etc.) — watchlist first, fallback second.
  4. Theme label strings only when no tickers exist; no_direct_exposure=True.

Public API
----------
resolve_family_exposure(family_key, market_question, yes_pct, delta_24h,
                        watchlist_syms, canonical_ticker_map)
    → (market_read: str, exposure: dict)

build_exposure_from_sectors(bullish_sectors, bearish_sectors, conditional_sectors,
                            market_read, watchlist_syms, canonical_ticker_map)
    → (market_read, exposure, watchlist_hits, fallback_hits, has_any)

get_market_read_for_theme_direction(theme_id, direction) → str

build_canonical_ticker_map() → dict[str, list[str]]
"""

from __future__ import annotations

import logging
from typing import Optional

log = logging.getLogger("exposure_resolver")


# ── Sector label → canonical theme IDs ────────────────────────────────────────
#
# Maps human-readable sector labels (from impact_engine.ThemeImpact) to
# canonical theme IDs that exist in ENRICHED_THEME_RS_UNIVERSE.
# This is the only bridge between the two taxonomies — no hardcoded tickers.

_SECTOR_LABEL_TO_THEME_IDS: dict[str, list[str]] = {
    "Energy":                        ["energy", "oil_gas", "oil_services", "lng_gas"],
    "Defense/Aerospace":             ["defense", "drones"],
    "Airlines/Transport":            ["travel_transportation"],
    "Industrials":                   ["industrials"],
    "Financials":                    ["financials", "banks", "fintech"],
    "Regional Banks":                ["regional_banks"],
    "Small Caps":                    [],
    "Semiconductors":                ["semiconductors", "semicap_equipment", "memory_storage"],
    "Semiconductors (US domestic)":  ["semiconductors"],
    "Software/Growth Tech":          ["software", "cloud_software"],
    "Long-Duration Growth Tech":     ["technology", "software", "cloud_software"],
    "Legacy Tech":                   ["technology"],
    "Cybersecurity":                 ["cybersecurity"],
    "AI Infra/Data Centers":         ["datacenter_infra", "ai_networking", "power_cooling"],
    "REITs/Housing":                 ["real_estate", "homebuilders"],
    "Consumer Discretionary":        ["consumer_discretionary", "consumer_retail"],
    "Consumer Electronics":          ["consumer_discretionary"],
    "Consumer Hardware":             ["consumer_discretionary"],
    "Utilities/Nuclear Power":       ["utilities", "uranium_nuclear"],
    "Gold/Metals/Commodities":       ["gold", "silver", "metals_mining", "copper_miners", "rare_earth"],
    "Crypto Proxies":                ["crypto_equities"],
    "Clean Energy":                  ["clean_energy", "solar"],
}


# ── Per-family semantic exposure map ──────────────────────────────────────────
#
# Keys: family_key (from ODDS_REGISTRY).
# Per-entry fields:
#   market_read            str   — semantic read label emitted in the response
#   bullish_sectors        list  — sector labels whose canonical themes benefit
#   bearish_sectors        list  — sector labels whose canonical themes are hurt
#   conditional_sectors    list  — direction-dependent sectors
#   direct_tickers_bullish list  — primary ticker(s) (mega-cap families only)
#   invert_on_below        bool  — flip bull/bear when question contains "below"
#   dynamic                bool  — market_read derived from market_question keywords

_FAMILY_EXPOSURE_MAP: dict[str, dict] = {

    # ── Fed / Rates ──────────────────────────────────────────────────────────

    "fed_rate_decision": {
        "dynamic": True,
        "_cut": {
            "market_read":     "rates easing",
            "bullish_sectors": ["Long-Duration Growth Tech", "Software/Growth Tech",
                                "REITs/Housing", "Small Caps"],
            "bearish_sectors": ["Financials", "Regional Banks"],
        },
        "_hike": {
            "market_read":     "rates restrictive",
            "bullish_sectors": ["Financials", "Regional Banks", "Energy",
                                "Gold/Metals/Commodities"],
            "bearish_sectors": ["REITs/Housing", "Long-Duration Growth Tech",
                                "Utilities/Nuclear Power"],
        },
        "_hold": {
            "market_read":     "mixed",
            "bullish_sectors": ["Financials"],
            "bearish_sectors": ["REITs/Housing"],
        },
    },

    "fed_cuts_2026": {
        "market_read":     "rates easing",
        "bullish_sectors": ["Long-Duration Growth Tech", "Software/Growth Tech",
                            "REITs/Housing", "Small Caps"],
        "bearish_sectors": ["Financials", "Regional Banks"],
    },

    "fed_hikes_2026": {
        "market_read":     "rates restrictive",
        "bullish_sectors": ["Financials", "Regional Banks", "Energy",
                            "Gold/Metals/Commodities"],
        "bearish_sectors": ["REITs/Housing", "Long-Duration Growth Tech",
                            "Utilities/Nuclear Power"],
    },

    # ── Macro / Economy ──────────────────────────────────────────────────────

    "recession_probability": {
        "market_read":     "growth negative",
        "bullish_sectors": ["Gold/Metals/Commodities", "Utilities/Nuclear Power"],
        "bearish_sectors": ["Financials", "Consumer Discretionary", "Industrials"],
    },

    "cpi_inflation": {
        "market_read":     "inflationary",
        "bullish_sectors": ["Energy", "Gold/Metals/Commodities", "Financials"],
        "bearish_sectors": ["REITs/Housing", "Long-Duration Growth Tech",
                            "Utilities/Nuclear Power"],
    },

    "jobs_unemployment": {
        "market_read":     "growth positive",
        "bullish_sectors": ["Consumer Discretionary", "Financials", "Industrials"],
        "bearish_sectors": [],
    },

    # ── Index / Market Direction ─────────────────────────────────────────────

    "spx_daily_direction": {
        "market_read":     "risk-on",
        "bullish_sectors": ["Software/Growth Tech", "Semiconductors",
                            "Consumer Discretionary", "Financials"],
        "bearish_sectors": [],
    },

    "nasdaq_daily_direction": {
        "market_read":     "tech bullish",
        "bullish_sectors": ["Semiconductors", "Software/Growth Tech",
                            "AI Infra/Data Centers"],
        "bearish_sectors": [],
    },

    "dow_daily_direction": {
        "market_read":     "growth positive",
        "bullish_sectors": ["Industrials", "Financials", "Consumer Discretionary"],
        "bearish_sectors": [],
    },

    # ── Mega-cap / Price Milestones ──────────────────────────────────────────

    "nvda_price_milestone": {
        "market_read":              "tech bullish",
        "direct_tickers_bullish":   ["NVDA"],
        "bullish_sectors":          ["Semiconductors", "AI Infra/Data Centers"],
        "bearish_sectors":          [],
        "invert_on_below":          True,
    },

    "tsla_price_milestone": {
        "market_read":              "risk-on",
        "direct_tickers_bullish":   ["TSLA"],
        "bullish_sectors":          ["Consumer Discretionary"],
        "bearish_sectors":          [],
        "invert_on_below":          True,
    },

    "aapl_price_milestone": {
        "market_read":              "tech bullish",
        "direct_tickers_bullish":   ["AAPL"],
        "bullish_sectors":          ["Software/Growth Tech", "Consumer Electronics"],
        "bearish_sectors":          [],
        "invert_on_below":          True,
    },

    "msft_price_milestone": {
        "market_read":              "tech bullish",
        "direct_tickers_bullish":   ["MSFT"],
        "bullish_sectors":          ["Software/Growth Tech", "AI Infra/Data Centers"],
        "bearish_sectors":          [],
        "invert_on_below":          True,
    },

    "googl_price_milestone": {
        "market_read":              "tech bullish",
        "direct_tickers_bullish":   ["GOOGL"],
        "bullish_sectors":          ["Software/Growth Tech"],
        "bearish_sectors":          [],
        "invert_on_below":          True,
    },

    "amd_price_milestone": {
        "market_read":              "tech bullish",
        "direct_tickers_bullish":   ["AMD"],
        "bullish_sectors":          ["Semiconductors"],
        "bearish_sectors":          [],
        "invert_on_below":          True,
    },

    # ── Earnings ─────────────────────────────────────────────────────────────

    "earnings_nvda": {
        "market_read":              "tech bullish",
        "direct_tickers_bullish":   ["NVDA"],
        "bullish_sectors":          ["Semiconductors", "AI Infra/Data Centers"],
        "bearish_sectors":          [],
    },

    "earnings_tsla": {
        "market_read":              "risk-on",
        "direct_tickers_bullish":   ["TSLA"],
        "bullish_sectors":          ["Consumer Discretionary"],
        "bearish_sectors":          [],
    },

    # ── Tech / AI / Policy ───────────────────────────────────────────────────

    "ai_export_controls": {
        "market_read":          "conditional",
        "bullish_sectors":      ["Defense/Aerospace", "Cybersecurity"],
        "bearish_sectors":      ["Semiconductors"],
        "conditional_sectors":  ["AI Infra/Data Centers"],
    },

    # ── Geopolitics ──────────────────────────────────────────────────────────

    "hormuz_iran": {
        "market_read":     "geopolitical stress rising",
        "bullish_sectors": ["Energy", "Defense/Aerospace", "Gold/Metals/Commodities"],
        "bearish_sectors": ["Airlines/Transport", "Consumer Discretionary"],
    },

    "russia_ukraine": {
        # preferred_outcome=yes tracks CEASEFIRE probability.
        # High yes_pct → ceasefire likely → geopolitical stress EASING.
        "market_read":     "geopolitical stress easing",
        "bullish_sectors": ["Airlines/Transport", "Consumer Discretionary", "Industrials"],
        "bearish_sectors": ["Defense/Aerospace", "Gold/Metals/Commodities", "Energy"],
    },

    "china_taiwan": {
        "market_read":     "geopolitical stress rising",
        "bullish_sectors": ["Defense/Aerospace", "Cybersecurity"],
        "bearish_sectors": ["Semiconductors", "Consumer Electronics"],
    },

    "israel_gaza": {
        "market_read":     "geopolitical stress rising",
        "bullish_sectors": ["Defense/Aerospace", "Energy", "Gold/Metals/Commodities"],
        "bearish_sectors": ["Airlines/Transport", "Consumer Discretionary"],
    },

    "us_tariffs": {
        "market_read":     "geopolitical stress rising",
        "bullish_sectors": ["Defense/Aerospace", "Energy", "Industrials"],
        "bearish_sectors": ["Consumer Discretionary", "Semiconductors"],
    },

    # ── BTC Daily Direction ───────────────────────────────────────────────────

    "btc_daily_direction": {
        # preferred_outcome=yes → Bitcoin closes UP / green
        "market_read":     "risk-on",
        "bullish_sectors": ["Crypto Proxies", "Software/Growth Tech", "Financials"],
        "bearish_sectors": [],
        "invert_on_below": False,   # "below" not meaningful for direction markets
    },

    # ── AI / Tech Benchmarks ─────────────────────────────────────────────────

    "google_ai_benchmark": {
        "market_read":              "tech bullish",
        "direct_tickers_bullish":   ["GOOGL"],
        "bullish_sectors":          ["Software/Growth Tech", "AI Infra/Data Centers",
                                     "Semiconductors"],
        "bearish_sectors":          [],
    },

    # ── Commodities / Crypto ─────────────────────────────────────────────────

    "oil_price_milestone": {
        "market_read":     "commodity pressure",
        "bullish_sectors": ["Energy"],
        "bearish_sectors": ["Airlines/Transport", "Consumer Discretionary"],
        "invert_on_below": True,
    },

    "gold_price_milestone": {
        "market_read":     "risk-off",
        "bullish_sectors": ["Gold/Metals/Commodities"],
        "bearish_sectors": [],
        "invert_on_below": True,
    },

    "bitcoin_price": {
        "market_read":     "risk-on",
        "bullish_sectors": ["Crypto Proxies", "Software/Growth Tech"],
        "bearish_sectors": [],
        "invert_on_below": True,
    },
}


# ── Theme (macro cluster) direction → market_read ─────────────────────────────
#
# Used by equity_signals (event-family-centric) where primary_theme_id + direction
# come from the Polymarket intelligence classifier, not the ODDS_REGISTRY.

_THEME_DIRECTION_TO_MARKET_READ: dict[tuple[str, str], str] = {
    ("macro_rates_inflation",      "rising"):  "inflationary",
    ("macro_rates_inflation",      "falling"): "disinflationary",
    ("macro_rates_inflation",      "mixed"):   "mixed",
    ("geopolitics_war_trade",      "rising"):  "geopolitical stress rising",
    ("geopolitics_war_trade",      "falling"): "geopolitical stress easing",
    ("geopolitics_war_trade",      "mixed"):   "mixed",
    ("energy_commodities",         "rising"):  "commodity pressure",
    ("energy_commodities",         "falling"): "disinflationary",
    ("energy_commodities",         "mixed"):   "mixed",
    ("us_politics_policy",         "rising"):  "conditional",
    ("us_politics_policy",         "falling"): "mixed",
    ("ai_semis_tech",              "rising"):  "tech bullish",
    ("ai_semis_tech",              "falling"): "conditional",
    ("ai_semis_tech",              "mixed"):   "mixed",
    ("crypto_risk_appetite",       "rising"):  "risk-on",
    ("crypto_risk_appetite",       "falling"): "risk-off",
    ("crypto_risk_appetite",       "mixed"):   "mixed",
    ("china_taiwan_supply_chain",  "rising"):  "geopolitical stress rising",
    ("china_taiwan_supply_chain",  "falling"): "geopolitical stress easing",
    ("china_taiwan_supply_chain",  "mixed"):   "mixed",
    ("defense_security",           "rising"):  "geopolitical stress rising",
    ("defense_security",           "falling"): "geopolitical stress easing",
    ("consumer_labor_growth",      "rising"):  "growth positive",
    ("consumer_labor_growth",      "falling"): "growth negative",
    ("consumer_labor_growth",      "mixed"):   "mixed",
}


# ── Canonical ticker helpers ──────────────────────────────────────────────────

def _get_canonical_tickers_for_themes(theme_ids: list[str]) -> list[str]:
    """
    Return deduped tickers (proxy_symbols then candidate_symbols) for the given
    theme IDs from ENRICHED_THEME_RS_UNIVERSE.  No SECTOR_STOCKS fallback.
    """
    try:
        from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE
        seen: set[str] = set()
        out: list[str] = []
        for tid in theme_ids:
            meta = ENRICHED_THEME_RS_UNIVERSE.get(tid, {})
            for sym in list(meta.get("proxy_symbols", [])) + list(meta.get("candidate_symbols", [])):
                if sym and sym not in seen:
                    seen.add(sym)
                    out.append(sym)
        return out
    except Exception:
        return []


def build_canonical_ticker_map() -> dict[str, list[str]]:
    """
    Build reverse map: ticker → [theme_ids it appears in].

    Reads ENRICHED_THEME_RS_UNIVERSE at call time so it always reflects the
    current enriched state.  Both proxy_symbols and candidate_symbols included.
    No SECTOR_STOCKS used.
    """
    try:
        from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE
        result: dict[str, list[str]] = {}
        for tid, meta in ENRICHED_THEME_RS_UNIVERSE.items():
            for sym in (
                list(meta.get("proxy_symbols", [])) + list(meta.get("candidate_symbols", []))
            ):
                if sym:
                    result.setdefault(sym, []).append(tid)
        return result
    except Exception as exc:
        log.warning("build_canonical_ticker_map failed: %s", exc)
        return {}


def _sectors_to_theme_ids(sector_labels: list[str]) -> list[str]:
    """Map sector labels → canonical theme IDs (deduped, preserving order)."""
    out: list[str] = []
    seen: set[str] = set()
    for sec in sector_labels:
        for tid in _SECTOR_LABEL_TO_THEME_IDS.get(sec, []):
            if tid not in seen:
                seen.add(tid)
                out.append(tid)
    return out


# ── Core ticker resolution ────────────────────────────────────────────────────

def _resolve_tickers(
    bull_theme_ids: list[str],
    bear_theme_ids: list[str],
    cond_theme_ids: list[str],
    direct_bull: list[str],
    watchlist_syms: set[str],
    canonical_ticker_map: dict[str, list[str]],
) -> tuple[dict, int, int, bool]:
    """
    Watchlist-first, canonical-fallback ticker resolution.

    Returns:
        (ticker_buckets_dict, watchlist_hits, fallback_hits, has_any_tickers)
    """
    bull_set = set(bull_theme_ids)
    bear_set = set(bear_theme_ids)

    # ── Watchlist resolution ──────────────────────────────────────────────────
    wl_bull = sorted(
        t for t in watchlist_syms
        if any(tid in bull_set for tid in canonical_ticker_map.get(t, []))
    )
    wl_bear = sorted(
        t for t in watchlist_syms
        if any(tid in bear_set for tid in canonical_ticker_map.get(t, []))
    )
    wl_cond = sorted(set(wl_bull) & set(wl_bear))
    cond_overlap = set(wl_cond)
    wl_bull = [t for t in wl_bull if t not in cond_overlap]
    wl_bear = [t for t in wl_bear if t not in cond_overlap]

    # Direct tickers go to watchlist bucket if the user holds them
    for t in direct_bull:
        if t in watchlist_syms and t not in cond_overlap and t not in wl_bull:
            wl_bull.insert(0, t)

    seen_wl = set(wl_bull) | set(wl_bear) | cond_overlap
    watchlist_hits = len(seen_wl)

    # ── Canonical fallback ────────────────────────────────────────────────────
    fb_bull_raw = _get_canonical_tickers_for_themes(bull_theme_ids)
    fb_bear_raw = _get_canonical_tickers_for_themes(bear_theme_ids)
    fb_cond_raw = _get_canonical_tickers_for_themes(cond_theme_ids)

    fb_bull = [t for t in fb_bull_raw if t not in seen_wl][:8]
    fb_bear = [t for t in fb_bear_raw if t not in seen_wl][:8]
    fb_used = set(fb_bull) | set(fb_bear)
    fb_cond = [t for t in fb_cond_raw if t not in seen_wl and t not in fb_used][:4]

    # Direct tickers → fallback if not already in watchlist bucket
    for t in direct_bull:
        if t not in seen_wl and t not in fb_bull:
            fb_bull.insert(0, t)

    fallback_hits = len(fb_bull) + len(fb_bear) + len(fb_cond)
    has_any = bool(wl_bull or wl_bear or wl_cond or fb_bull or fb_bear or fb_cond)

    return (
        {
            "bullish_watchlist":     wl_bull[:6],
            "bearish_watchlist":     wl_bear[:6],
            "conditional_watchlist": list(cond_overlap)[:4],
            "bullish_fallback":      fb_bull[:8],
            "bearish_fallback":      fb_bear[:8],
            "conditional_fallback":  fb_cond[:4],
        },
        watchlist_hits,
        fallback_hits,
        has_any,
    )


# ── Empty exposure stub ───────────────────────────────────────────────────────

def _empty_exposure(
    bullish_themes: Optional[list[str]] = None,
    bearish_themes: Optional[list[str]] = None,
) -> dict:
    return {
        "bullish_watchlist":     [],
        "bearish_watchlist":     [],
        "conditional_watchlist": [],
        "bullish_fallback":      [],
        "bearish_fallback":      [],
        "conditional_fallback":  [],
        "bullish_themes":        bullish_themes or [],
        "bearish_themes":        bearish_themes or [],
        "conditional_themes":    [],
        "exposure_source":       "watchlist+canonical_theme_universe",
        "no_direct_exposure":    True,
    }


# ── market_read inversion ─────────────────────────────────────────────────────

_MARKET_READ_INVERSIONS: dict[str, str] = {
    "commodity pressure":          "disinflationary",
    "risk-on":                     "risk-off",
    "tech bullish":                "conditional",
    "risk-off":                    "risk-on",
    "geopolitical stress rising":  "geopolitical stress easing",
    "geopolitical stress easing":  "geopolitical stress rising",
    "growth positive":             "growth negative",
    "growth negative":             "growth positive",
}


def _invert_market_read(market_read: str) -> str:
    return _MARKET_READ_INVERSIONS.get(market_read, market_read)


# ── Public API ────────────────────────────────────────────────────────────────

def get_market_read_for_theme_direction(theme_id: str, direction: str) -> str:
    """Return market_read label for an equity_signal (theme-direction pair)."""
    return _THEME_DIRECTION_TO_MARKET_READ.get((theme_id, direction), "mixed")


def resolve_family_exposure(
    family_key: str,
    market_question: Optional[str],
    yes_pct: Optional[float],
    delta_24h: Optional[float],
    watchlist_syms: set[str],
    canonical_ticker_map: dict[str, list[str]],
) -> tuple[str, dict]:
    """
    Resolve market_read + full exposure dict for a tracked-odds registry family.

    Args:
        family_key:           Registry family_key (e.g. "fed_rate_decision").
        market_question:      The matched market question (used for dynamic parsing).
        yes_pct:              Current YES probability as a percentage (0–100).
        delta_24h:            24h probability shift in pp.
        watchlist_syms:       User watchlist symbols (pass empty set for /live endpoint).
        canonical_ticker_map: Pre-built ticker→theme_ids reverse map.

    Returns:
        (market_read, exposure_dict)
        exposure_dict shape:
          bullish_watchlist, bearish_watchlist, conditional_watchlist,
          bullish_fallback, bearish_fallback, conditional_fallback,
          bullish_themes, bearish_themes, conditional_themes,
          exposure_source, no_direct_exposure.
    """
    spec = _FAMILY_EXPOSURE_MAP.get(family_key)
    if not spec:
        return "mixed", _empty_exposure()

    q_lower = (market_question or "").lower()

    # ── Dynamic: fed_rate_decision ───────────────────────────────────────────
    if spec.get("dynamic") and family_key == "fed_rate_decision":
        if any(kw in q_lower for kw in ("decrease", " cut", "lower", "ease", "reduction")):
            sub = spec["_cut"]
        elif any(kw in q_lower for kw in ("increase", " hike", " raise", " higher")):
            sub = spec["_hike"]
        else:
            sub = spec.get("_hold", spec["_cut"])
        market_read    = sub["market_read"]
        bull_sectors   = list(sub["bullish_sectors"])
        bear_sectors   = list(sub["bearish_sectors"])
        cond_sectors:  list[str] = []
        direct_bull:   list[str] = []

    else:
        bull_sectors   = list(spec.get("bullish_sectors",      []))
        bear_sectors   = list(spec.get("bearish_sectors",      []))
        cond_sectors   = list(spec.get("conditional_sectors",  []))
        direct_bull    = list(spec.get("direct_tickers_bullish", []))
        market_read    = spec.get("market_read", "mixed")

        # Invert polarity for "below" questions (oil below $60, gold below $2500…)
        if spec.get("invert_on_below") and "below" in q_lower:
            bull_sectors, bear_sectors = bear_sectors, bull_sectors
            market_read = _invert_market_read(market_read)

    # ── Map sector labels → canonical theme IDs ──────────────────────────────
    bull_theme_ids = _sectors_to_theme_ids(bull_sectors)
    bear_theme_ids = _sectors_to_theme_ids(bear_sectors)
    cond_theme_ids = _sectors_to_theme_ids(cond_sectors)

    # ── Resolve tickers ──────────────────────────────────────────────────────
    tickers, _wl_hits, _fb_hits, has_any = _resolve_tickers(
        bull_theme_ids, bear_theme_ids, cond_theme_ids,
        direct_bull, watchlist_syms, canonical_ticker_map,
    )

    exposure = {
        **tickers,
        "bullish_themes":     bull_sectors,
        "bearish_themes":     bear_sectors,
        "conditional_themes": cond_sectors,
        "exposure_source":    "watchlist+canonical_theme_universe",
        "no_direct_exposure": not has_any,
    }
    return market_read, exposure


def build_exposure_from_sectors(
    bullish_sectors: list[str],
    bearish_sectors: list[str],
    conditional_sectors: list[str],
    market_read: str,
    watchlist_syms: set[str],
    canonical_ticker_map: dict[str, list[str]],
) -> tuple[str, dict, int, int, bool]:
    """
    Build an exposure dict from sector labels — used by the equity_signals upgrade
    in investor_intel._build_equity_signals().

    Returns:
        (market_read, exposure_dict, watchlist_hits, fallback_hits, has_any_tickers)
    """
    bull_theme_ids = _sectors_to_theme_ids(bullish_sectors)
    bear_theme_ids = _sectors_to_theme_ids(bearish_sectors)
    cond_theme_ids = _sectors_to_theme_ids(conditional_sectors)

    tickers, wl_hits, fb_hits, has_any = _resolve_tickers(
        bull_theme_ids, bear_theme_ids, cond_theme_ids,
        [], watchlist_syms, canonical_ticker_map,
    )

    exposure = {
        **tickers,
        "bullish_themes":     bullish_sectors,
        "bearish_themes":     bearish_sectors,
        "conditional_themes": conditional_sectors,
        "exposure_source":    "watchlist+canonical_theme_universe",
        "no_direct_exposure": not has_any,
    }
    return market_read, exposure, wl_hits, fb_hits, has_any
