"""
Theme Universe + Watchlist Theme Merge Layer
============================================
Enriches THEME_RS_UNIVERSE with curated tickers from the dev account's
saved Watchlist theme taxonomy (AI-enhanced category sections + manual overrides).

Rules:
  - Universal: the enriched universe is identical for all users (Themes page is not
    personalised — it draws from the authoritative dev-account watchlist curation only).
  - US-listed only: any ticker containing ":" (e.g. KRX:000660, ASX:EOS) is excluded.
  - Category overrides win over section assignments for the same ticker.
  - For ETF-based themes (proxy_type="etf"|"basket"): watchlist tickers are added to
    candidate_symbols only, preserving the ETF-median performance character.
  - For custom-basket themes (proxy_type="custom"): watchlist tickers are also added to
    proxy_symbols so they participate in the performance median alongside existing stocks.
  - Falls back gracefully to the static THEME_RS_UNIVERSE when Postgres is unavailable.
"""
from __future__ import annotations

import copy
import logging
from typing import Optional

log = logging.getLogger(__name__)

# ── Static alias maps ──────────────────────────────────────────────────────────
# Maps watchlist analysis-section titles → canonical theme_id in THEME_RS_UNIVERSE.
# None  = skip the section entirely (all-foreign or too generic).

_SECTION_TO_THEME_ID: dict[str, Optional[str]] = {
    "AI Networking":               "ai_networking",
    "Clean Energy":                "clean_energy",
    "Crypto Equities / Blockchain": "crypto_equities",
    "Cybersecurity":               "cybersecurity",
    "Data Center Infrastructure":  "datacenter_infra",
    "Defense":                     "defense",
    "Drones":                      "drones",
    "Industrials":                 "industrials",
    "Lithium & Battery Tech":      "lithium_battery",
    "Memory & Storage":            "memory_storage",
    # "Nuclear / Grid" partially overlaps uranium_nuclear — merge tickers in.
    "Nuclear / Grid":              "uranium_nuclear",
    "Photonics / Lasers":          "photonics_lasers",
    "Power / Cooling":             "power_cooling",
    "Quantum Computing":           "quantum",
    "Rare Earth Metals":           "rare_earth",
    "Robotics & Automation":       "robotics_automation",
    # "Semi Materials" — all three tickers are on foreign exchanges; skip entirely.
    "Semi Materials":              None,
    "Semiconductor Equipment":     "semicap_equipment",
    "Semiconductors":              "semiconductors",
    "Solar":                       "solar",
    "Space Economy":               "space",
    "Substrates / Packaging":      "substrates_packaging",
    "Uranium & Nuclear Energy":    "uranium_nuclear",
    # Generic catch-all; no theme mapping.
    "Other / Uncategorized":       None,
}

# Maps watchlist category-override category names → canonical theme_id.
_CATEGORY_TO_THEME_ID: dict[str, Optional[str]] = {
    "Clean Energy":                "clean_energy",
    "Data Center Infrastructure":  "datacenter_infra",
    "Fintech":                     "fintech",
    "Quantum Computing":           "quantum",
    "Robotics & Automation":       "robotics_automation",
    "Semiconductors":              "semiconductors",
    "Space Economy":               "space",
    "Uranium & Nuclear Energy":    "uranium_nuclear",
}

# Dev-account identifiers (read-only; never exposed in API responses).
_DEV_WATCHLIST_ID = "23eec278-074a-4706-a62a-c35d38b384ea"
_DEV_USER_ID      = "default"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _is_us_ticker(sym: str) -> bool:
    """
    Return True for simple US-listed ticker symbols.
    Rejects any symbol containing ":" (e.g. KRX:000660, ASX:EOS, TSX:MAL, OTC:ATEYY).
    """
    return bool(sym) and ":" not in sym


def _load_watchlist_theme_tickers() -> dict[str, list[str]]:
    """
    Build the authoritative ticker → theme assignment from Postgres.

    Returns:  {theme_id: sorted_us_ticker_list}
    Returns {} if Postgres is unavailable or the watchlist is empty.

    Priority: category_overrides win over analysis-section assignments for the same
    ticker (the override is an explicit curator decision).
    """
    try:
        from data.pg_storage import is_available, watchlist_read, get_category_overrides
    except ImportError:
        log.warning("[THEME_MERGE] pg_storage not importable — static universe only")
        return {}

    if not is_available():
        log.warning("[THEME_MERGE] Postgres unavailable — static universe only")
        return {}

    # {theme_id → set of US tickers assigned to it}
    theme_to_tickers: dict[str, set[str]] = {}

    # ── Step 1: analysis sections ────────────────────────────────────────────
    try:
        store = watchlist_read(_DEV_WATCHLIST_ID)
        if store:
            sections: list[dict] = store.get("analysis", {}).get("sections", [])
            for section in sections:
                section_name = (section.get("title") or section.get("name") or "").strip()
                theme_id = _SECTION_TO_THEME_ID.get(section_name)
                if not theme_id:
                    continue
                for row in section.get("tickers", []):
                    sym = (row.get("symbol") or row.get("ticker") or "").upper().strip()
                    if sym and _is_us_ticker(sym):
                        theme_to_tickers.setdefault(theme_id, set()).add(sym)
        else:
            log.warning(f"[THEME_MERGE] Watchlist {_DEV_WATCHLIST_ID!r} not found in Postgres")
    except Exception as exc:
        log.warning(f"[THEME_MERGE] Error reading watchlist sections: {exc}")

    # ── Step 2: category overrides (win over section assignments) ────────────
    try:
        overrides: dict[str, str] = get_category_overrides(_DEV_USER_ID)
        for ticker, category in overrides.items():
            sym = ticker.upper().strip()
            if not sym or not _is_us_ticker(sym):
                continue
            override_tid = _CATEGORY_TO_THEME_ID.get(category)
            if not override_tid:
                continue
            # Remove ticker from any current section assignment
            for tid in list(theme_to_tickers.keys()):
                theme_to_tickers[tid].discard(sym)
            # Add to the override theme
            theme_to_tickers.setdefault(override_tid, set()).add(sym)
    except Exception as exc:
        log.warning(f"[THEME_MERGE] Error reading category overrides: {exc}")

    return {tid: sorted(tickers) for tid, tickers in theme_to_tickers.items() if tickers}


# ── Core enrichment ────────────────────────────────────────────────────────────

def _build_enriched_universe(
    base: dict,
    watchlist_tickers: dict[str, list[str]],
) -> dict:
    """
    Deep-copy base THEME_RS_UNIVERSE and enrich matching themes with watchlist tickers.

    - custom-basket themes (proxy_type="custom"):
        watchlist tickers → proxy_symbols + candidate_symbols
    - ETF / basket themes (proxy_type="etf" | "basket"):
        watchlist tickers → candidate_symbols only (ETF-median performance preserved)

    The 'watchlist_seeds' key records the net-new tickers for debugging.
    """
    enriched = copy.deepcopy(base)

    for theme_id, wl_tickers in watchlist_tickers.items():
        if theme_id not in enriched:
            log.debug(f"[THEME_MERGE] theme_id '{theme_id}' not in base universe — skipped")
            continue

        meta = enriched[theme_id]
        existing_proxy = set(meta.get("proxy_symbols",     []))
        existing_cand  = set(meta.get("candidate_symbols", []))
        all_existing   = existing_proxy | existing_cand
        new_tickers    = [t for t in wl_tickers if t not in all_existing]

        is_custom = meta.get("proxy_type") == "custom"

        # Always enrich candidate_symbols
        meta["candidate_symbols"] = sorted(existing_cand | set(wl_tickers))

        # For custom-basket themes also enrich proxy_symbols (performance basket)
        if is_custom:
            meta["proxy_symbols"] = sorted(existing_proxy | set(wl_tickers))

        # Record the net-new additions for the debug endpoint
        if new_tickers:
            meta["watchlist_seeds"] = sorted(
                set(meta.get("watchlist_seeds", [])) | set(new_tickers)
            )
            log.info(
                f"[THEME_MERGE] {theme_id} ({meta['proxy_type']}): "
                f"+{len(new_tickers)} new ticker(s) → {new_tickers}"
            )

    return enriched


# ── Module-level initialisation (runs once at import time) ─────────────────────

def _build() -> dict:
    """Build the enriched universe. Falls back to base on any failure."""
    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE
    except ImportError as exc:
        log.error(f"[THEME_MERGE] Cannot import THEME_RS_UNIVERSE: {exc}")
        return {}

    watchlist_tickers = _load_watchlist_theme_tickers()
    if not watchlist_tickers:
        log.info("[THEME_MERGE] No watchlist data — returning deep-copy of base universe")
        return copy.deepcopy(THEME_RS_UNIVERSE)

    merged = _build_enriched_universe(THEME_RS_UNIVERSE, watchlist_tickers)
    theme_count   = len(merged)
    ticker_events = sum(len(v) for v in watchlist_tickers.values())
    log.info(
        f"[THEME_MERGE] Built enriched universe: {theme_count} themes, "
        f"{ticker_events} watchlist ticker assignments applied"
    )
    return merged


ENRICHED_THEME_RS_UNIVERSE: dict = _build()

ENRICHED_ALL_PROXY_SYMBOLS: list[str] = sorted(
    set(sym for v in ENRICHED_THEME_RS_UNIVERSE.values() for sym in v.get("proxy_symbols", []))
)

ENRICHED_ALL_CANDIDATE_SYMBOLS: list[str] = sorted(
    set(sym for v in ENRICHED_THEME_RS_UNIVERSE.values() for sym in v.get("candidate_symbols", []))
    - {""}
)


# ── Public helpers ─────────────────────────────────────────────────────────────

def refresh_enriched_universe() -> None:
    """
    Intentional in-place refresh of the module-level enriched universe.
    Safe to call after a watchlist update (e.g. from an admin endpoint).
    """
    global ENRICHED_THEME_RS_UNIVERSE, ENRICHED_ALL_PROXY_SYMBOLS, ENRICHED_ALL_CANDIDATE_SYMBOLS
    ENRICHED_THEME_RS_UNIVERSE = _build()
    ENRICHED_ALL_PROXY_SYMBOLS = sorted(
        set(sym for v in ENRICHED_THEME_RS_UNIVERSE.values() for sym in v.get("proxy_symbols", []))
    )
    ENRICHED_ALL_CANDIDATE_SYMBOLS = sorted(
        set(sym for v in ENRICHED_THEME_RS_UNIVERSE.values() for sym in v.get("candidate_symbols", []))
        - {""}
    )
    log.info("[THEME_MERGE] Enriched universe refreshed")


def get_merge_debug_info() -> dict:
    """
    Diagnostic snapshot of the merge layer — safe to expose in a dev/admin endpoint.
    Shows only symbol counts and ticker lists; no user PII, no credentials.
    """
    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE as _base
    except ImportError:
        _base = {}

    enriched_themes = []
    for theme_id, meta in ENRICHED_THEME_RS_UNIVERSE.items():
        base_meta = _base.get(theme_id, {})
        base_proxy = set(base_meta.get("proxy_symbols",     []))
        base_cand  = set(base_meta.get("candidate_symbols", []))
        new_proxy  = sorted(set(meta.get("proxy_symbols",     [])) - base_proxy)
        new_cand   = sorted(set(meta.get("candidate_symbols", [])) - base_cand)
        seeds      = meta.get("watchlist_seeds", [])
        if new_proxy or new_cand:
            enriched_themes.append({
                "theme_id":              theme_id,
                "display_name":          meta.get("display_name", ""),
                "proxy_type":            meta.get("proxy_type", ""),
                "new_proxy_symbols":     new_proxy,
                "new_candidate_symbols": new_cand,
                "watchlist_seeds":       seeds,
            })

    return {
        "total_themes":            len(ENRICHED_THEME_RS_UNIVERSE),
        "enriched_theme_count":    len(enriched_themes),
        "total_proxy_symbols":     len(ENRICHED_ALL_PROXY_SYMBOLS),
        "total_candidate_symbols": len(ENRICHED_ALL_CANDIDATE_SYMBOLS),
        "base_proxy_count":        len(set(
            sym for v in _base.values() for sym in v.get("proxy_symbols", [])
        )),
        "base_candidate_count":    len(set(
            sym for v in _base.values() for sym in v.get("candidate_symbols", [])
        )),
        "enriched_themes":         enriched_themes,
    }
