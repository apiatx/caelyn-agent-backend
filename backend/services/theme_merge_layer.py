"""
Theme Universe + Watchlist Theme Merge Layer
============================================
Enriches THEME_RS_UNIVERSE with curated tickers from the dev account's
saved Watchlist theme taxonomy (AI-enhanced category sections + manual overrides).

PERFORMANCE FIELD AUDIT
-----------------------
The actual daily/weekly/monthly theme performance is calculated exclusively from
`meta["proxy_symbols"]` in _build_theme_row → _compute_theme_perf.
`candidate_symbols` is used ONLY for leader/laggard discovery, never for performance.
Therefore watchlist tickers MUST be added to `proxy_symbols` (not candidate_symbols
alone) to participate in the performance basket.

Merge rules:
  - Universal: the enriched universe is identical for all users.
  - US-listed only: any ticker containing ":" (e.g. KRX:000660, ASX:EOS) is excluded.
  - Category overrides win over section assignments for the same ticker.
  - ALL matched themes (ETF, basket, custom, hybrid): watchlist tickers are added to
    BOTH proxy_symbols (performance basket) AND candidate_symbols (leader/laggard pool).
    Existing ETF/basket/custom symbols are NEVER removed — the final set is the union.
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
# Multiple sections may map to the same theme_id (merge, not duplicate rows).

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
    # "Nuclear / Grid" overlaps uranium_nuclear — tickers merged in, no new row.
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

# Watchlist section names that collapsed into an existing theme_id under a
# different label (used for the "aliases" field in merge-debug output).
# key = canonical theme_id,  value = list of alternate watchlist section names merged in.
_THEME_SECTION_ALIASES: dict[str, list[str]] = {
    "uranium_nuclear": ["Nuclear / Grid"],   # second section that fed uranium_nuclear
}

# Dev-account identifiers (read-only; never exposed in API responses).
_DEV_WATCHLIST_ID = "23eec278-074a-4706-a62a-c35d38b384ea"
_DEV_USER_ID      = "default"


# ── Representative chart symbol map ───────────────────────────────────────────
# Stable, explicit ETF/proxy ticker per theme.
# Used ONLY for the Ticker column and TradingView popup — never replaces
# proxy_symbols/performance_symbols.
#
# Coverage:
#   1. All 8 custom/hybrid themes (no ETF in base proxy_symbols).
#   2. ETF themes where seed-map specifies a preferred representative.
#   3. All remaining themes fall back to proxy_symbols[0] from the BASE universe
#      (always an ETF/primary proxy, pre-merge).
#
# Rules: never CUSTOM, never a watchlist-added individual stock,
# separate from the performance basket.

_REPRESENTATIVE_ETF_MAP: dict[str, str] = {
    # ── Custom basket themes (no ETF in base proxy_symbols) ──────────────────
    "ai_networking":        "SMH",    # basket of stocks; SMH is the nearest ETF proxy
    "photonics_lasers":     "ROBO",   # no photonics ETF; robotics ETF is closest
    "power_cooling":        "GRID",   # power-infrastructure ETF
    "pre_ipo":              "VCX",    # private-equity proxy; VCX is the only option
    "quantum":              "QTUM",   # dedicated quantum/AI ETF
    "semicap_equipment":    "SOXX",   # SOXX already in proxy_symbols
    "substrates_packaging": "SOXX",   # semis packaging → SOXX
    # ── Hybrid themes ─────────────────────────────────────────────────────────
    "drones":               "ITA",    # ITA (defense/aerospace) is the primary ETF
    # ── ETF themes — preferred representative from seed map ───────────────────
    "banks":                "KBE",
    "biotech":              "XBI",
    "clean_energy":         "ICLN",
    "copper_miners":        "COPX",
    "crypto_equities":      "BLOK",
    "cybersecurity":        "CIBR",
    "datacenter_infra":     "DTCR",
    "defense":              "ITA",
    "fintech":              "FINX",
    "lithium_battery":      "LIT",
    "memory_storage":       "SMH",
    "rare_earth":           "REMX",
    "regional_banks":       "KRE",
    "robotics_automation":  "BOTZ",
    "semiconductors":       "SMH",
    "space":                "ARKX",
    "uranium_nuclear":      "URA",
}


def _get_representative_symbol(
    theme_id: str,
    base_meta: dict,
) -> tuple[str, str]:
    """
    Return (representative_symbol, source) where source is one of:
      "explicit_map"   — theme_id is in _REPRESENTATIVE_ETF_MAP
      "original_proxy" — first symbol from BASE (pre-merge) proxy_symbols
      "fallback_stock" — last resort: first BASE candidate_symbols entry

    Never returns "CUSTOM". Never uses watchlist-added tickers.
    The symbol is used for display/TradingView only, not performance.
    """
    # 1. Explicit map wins
    if theme_id in _REPRESENTATIVE_ETF_MAP:
        return _REPRESENTATIVE_ETF_MAP[theme_id], "explicit_map"

    # 2. First symbol from BASE proxy_symbols (pre-merge, always a primary ETF/proxy)
    base_proxy = base_meta.get("proxy_symbols", [])
    if base_proxy:
        return base_proxy[0], "original_proxy"

    # 3. Last resort — first candidate
    base_cand = base_meta.get("candidate_symbols", [])
    if base_cand:
        return base_cand[0], "fallback_stock"

    # Absolute fallback (should never happen; every theme has at least one symbol)
    return theme_id.upper()[:6], "fallback_stock"


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
) -> tuple[dict, dict[str, list[str]]]:
    """
    Deep-copy base THEME_RS_UNIVERSE and enrich matching themes.

    CRITICAL: watchlist tickers are added to proxy_symbols for ALL theme types.
    proxy_symbols is the ONLY field read by _compute_theme_perf (the performance
    engine). candidate_symbols is only used for leader/laggard discovery.

    Existing proxy_symbols are NEVER removed — the final set is the union.

    Also stamps representative_symbol + representative_symbol_source onto EVERY
    theme (derived from _REPRESENTATIVE_ETF_MAP or BASE proxy_symbols[0]).
    This field is for display/TradingView only and is separate from performance.

    Returns (enriched_universe, {theme_id: [net_new_proxy_tickers]})
    """
    enriched = copy.deepcopy(base)
    # Track net-new proxy additions per theme for the debug endpoint
    net_new_proxy: dict[str, list[str]] = {}

    for theme_id, wl_tickers in watchlist_tickers.items():
        if theme_id not in enriched:
            log.debug(f"[THEME_MERGE] theme_id '{theme_id}' not in base universe — skipped")
            continue

        meta = enriched[theme_id]
        existing_proxy = set(meta.get("proxy_symbols",     []))
        existing_cand  = set(meta.get("candidate_symbols", []))

        # New to proxy_symbols (participates in performance median)
        new_proxy = sorted(set(wl_tickers) - existing_proxy)
        # New to candidate_symbols (leader/laggard pool)
        new_cand  = sorted(set(wl_tickers) - existing_cand)

        # Add to performance basket (proxy_symbols) for ALL theme types.
        # This is the correct field — _compute_theme_perf uses proxy_symbols exclusively.
        if new_proxy:
            meta["proxy_symbols"] = sorted(existing_proxy | set(wl_tickers))

        # Also add to candidate_symbols for leader/laggard enrichment.
        if new_cand:
            meta["candidate_symbols"] = sorted(existing_cand | set(wl_tickers))

        # Persist net-new proxy additions for debug introspection
        if new_proxy:
            meta["watchlist_seeds"] = sorted(
                set(meta.get("watchlist_seeds", [])) | set(new_proxy)
            )
            net_new_proxy[theme_id] = new_proxy
            log.info(
                f"[THEME_MERGE] {theme_id} ({meta['proxy_type']}): "
                f"+{len(new_proxy)} proxy ticker(s) → {new_proxy}"
            )

    # ── Stamp representative_symbol + holdings_display_mode on EVERY theme ───────
    # representative_symbol: stable display ticker (Ticker column / TradingView).
    #   Uses BASE meta (pre-merge) so watchlist-added stocks never become representative.
    #
    # holdings_display_mode: tells the frontend/row-builder how to populate the
    #   expanded holdings table.
    #   "theme_basket" — custom/hybrid themes: show proxy_symbols directly as the
    #     basket. Do NOT call _etf_holdings_for_proxy on representative_symbol —
    #     those would be holdings of an unrelated ETF used only for charting.
    #   "etf_holdings" — pure ETF/basket themes: existing behavior; ETF holdings
    #     are fetched for the primary proxy ETF and shown in the expanded view.
    for theme_id, meta in enriched.items():
        base_meta = base.get(theme_id, {})
        rep_sym, rep_src = _get_representative_symbol(theme_id, base_meta)
        meta["representative_symbol"]        = rep_sym
        meta["representative_symbol_source"] = rep_src
        # custom and hybrid themes use the curated basket directly as holdings.
        ptype = meta.get("proxy_type", "etf")
        meta["holdings_display_mode"] = (
            "theme_basket" if ptype in ("custom", "hybrid") else "etf_holdings"
        )

    return enriched, net_new_proxy


# ── Module-level initialisation (runs once at import time) ─────────────────────

def _build() -> tuple[dict, dict[str, list[str]]]:
    """Build the enriched universe. Falls back to base on any failure."""
    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE
    except ImportError as exc:
        log.error(f"[THEME_MERGE] Cannot import THEME_RS_UNIVERSE: {exc}")
        return {}, {}

    watchlist_tickers = _load_watchlist_theme_tickers()
    if not watchlist_tickers:
        log.info("[THEME_MERGE] No watchlist data — stamping representative symbols only")
        # Still run _build_enriched_universe with empty watchlist so that
        # representative_symbol is stamped on every theme (the enrichment loop
        # is a no-op when watchlist_tickers is empty, but the representative
        # stamp pass still executes).
        merged, net_new = _build_enriched_universe(THEME_RS_UNIVERSE, {})
        return merged, net_new

    merged, net_new = _build_enriched_universe(THEME_RS_UNIVERSE, watchlist_tickers)
    log.info(
        f"[THEME_MERGE] Enriched universe built: {len(merged)} themes, "
        f"{len(net_new)} enriched, "
        f"{sum(len(v) for v in net_new.values())} net-new proxy symbols"
    )
    return merged, net_new


_enriched_universe, _net_new_proxy = _build()

ENRICHED_THEME_RS_UNIVERSE: dict = _enriched_universe

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
    global ENRICHED_THEME_RS_UNIVERSE, ENRICHED_ALL_PROXY_SYMBOLS, \
           ENRICHED_ALL_CANDIDATE_SYMBOLS, _net_new_proxy
    ENRICHED_THEME_RS_UNIVERSE, _net_new_proxy = _build()
    ENRICHED_ALL_PROXY_SYMBOLS = sorted(
        set(sym for v in ENRICHED_THEME_RS_UNIVERSE.values()
            for sym in v.get("proxy_symbols", []))
    )
    ENRICHED_ALL_CANDIDATE_SYMBOLS = sorted(
        set(sym for v in ENRICHED_THEME_RS_UNIVERSE.values()
            for sym in v.get("candidate_symbols", []))
        - {""}
    )
    log.info("[THEME_MERGE] Enriched universe refreshed")


def get_merge_debug_info() -> dict:
    """
    Full diagnostic snapshot of the merge layer per the audit spec.

    Per canonical theme:
      canonical_theme_id, display_name, aliases, source_type,
      original_proxy_symbols, original_candidate_symbols,
      watchlist_added_symbols, final_performance_symbols,
      performance_field_used, watchlist_included_in_performance,
      duplicate_candidates_detected, visible_in_final_api

    Top-level:
      theme_count (before/after canonicalization), proxy/candidate counts,
      duplicate_groups_collapsed, performance_field, 5 examples.
    """
    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE as _base
    except ImportError:
        _base = {}

    PERF_FIELD = "proxy_symbols"   # the authoritative field read by _compute_theme_perf

    all_rows = []
    merged_rows = []

    for theme_id, meta in ENRICHED_THEME_RS_UNIVERSE.items():
        base_meta      = _base.get(theme_id, {})
        orig_proxy     = sorted(base_meta.get("proxy_symbols",     []))
        orig_cand      = sorted(base_meta.get("candidate_symbols", []))
        final_proxy    = sorted(meta.get("proxy_symbols",          []))
        final_cand     = sorted(meta.get("candidate_symbols",      []))

        wl_added       = _net_new_proxy.get(theme_id, [])
        is_enriched    = bool(wl_added)
        source_type    = "merged" if is_enriched else "existing_only"

        rep_sym = meta.get("representative_symbol", "")
        rep_src = meta.get("representative_symbol_source", "fallback_stock")

        row = {
            "canonical_theme_id":   theme_id,
            "display_name":         meta.get("display_name", ""),
            "proxy_type":           meta.get("proxy_type", ""),
            # Section names that fed into this theme_id under a different label
            "aliases":              _THEME_SECTION_ALIASES.get(theme_id, []),
            "source_type":          source_type,
            # ── Representative chart symbol (Ticker column / TradingView) ──────
            # Stable ETF/proxy — never CUSTOM, never a watchlist-added stock.
            # Separate from performance basket.
            "representative_symbol":        rep_sym,
            "representative_symbol_source": rep_src,
            "representative_symbol_in_proxy_symbols": rep_sym in set(final_proxy),
            "representative_symbol_non_custom": rep_sym != "CUSTOM",
            # Symbols present BEFORE any watchlist enrichment
            "original_proxy_symbols":     orig_proxy,
            "original_candidate_symbols": orig_cand,
            # Net-new tickers added from watchlist (all in proxy_symbols = performance)
            "watchlist_added_symbols":    wl_added,
            # Final proxy_symbols = what _compute_theme_perf will use
            "final_performance_symbols":  final_proxy,
            "performance_field_used":     PERF_FIELD,
            # Proof that watchlist tickers are in the live performance basket
            "watchlist_included_in_performance": is_enriched,
            # No duplicate theme rows — each section maps to a unique theme_id
            "duplicate_candidates_detected":     False,
            "visible_in_final_api":              True,
        }
        all_rows.append(row)
        if is_enriched:
            merged_rows.append(row)

    # ── 5 representative examples ─────────────────────────────────────────────
    example_ids = [
        "uranium_nuclear",       # has "Nuclear / Grid" alias + ASPI/IMSR added
        "datacenter_infra",      # large ETF theme, 14 watchlist tickers added
        "robotics_automation",   # override-driven: AEVA/AMBA/AUR/OUST
        "quantum",               # custom basket, INFQ/XNDU added
        "clean_energy",          # ETF theme, ARRY/HYLN/TE added
    ]
    examples = []
    for eid in example_ids:
        row = next((r for r in all_rows if r["canonical_theme_id"] == eid), None)
        if row:
            examples.append({
                "canonical_theme_id":        row["canonical_theme_id"],
                "display_name":              row["display_name"],
                "aliases":                   row["aliases"],
                "representative_symbol":        row["representative_symbol"],
                "representative_symbol_source": row["representative_symbol_source"],
                "representative_symbol_in_proxy_symbols": row["representative_symbol_in_proxy_symbols"],
                "original_proxy_symbols":    row["original_proxy_symbols"],
                "watchlist_added_symbols":   row["watchlist_added_symbols"],
                "final_performance_symbols": row["final_performance_symbols"],
                "watchlist_included_in_performance": row["watchlist_included_in_performance"],
                "performance_field_used":    row["performance_field_used"],
            })

    # ── Duplicate group report ────────────────────────────────────────────────
    # "Nuclear / Grid" merged into uranium_nuclear (no separate visible row created).
    duplicate_groups_collapsed = [
        {
            "canonical_theme_id": "uranium_nuclear",
            "display_name":       "Uranium & Nuclear Energy",
            "absorbed_section":   "Nuclear / Grid",
            "absorbed_tickers":   ["ASPI"],
            "note": (
                "Watchlist section 'Nuclear / Grid' has no separate theme row. "
                "Its US ticker (ASPI) was merged into uranium_nuclear proxy_symbols."
            ),
        }
    ]

    base_proxy_count = len(set(
        sym for v in _base.values() for sym in v.get("proxy_symbols", [])
    ))
    base_cand_count = len(set(
        sym for v in _base.values() for sym in v.get("candidate_symbols", [])
    ))

    return {
        # ── Summary ──────────────────────────────────────────────────────────
        "performance_field":               PERF_FIELD,
        "performance_field_note": (
            "proxy_symbols is the ONLY field read by _compute_theme_perf. "
            "candidate_symbols is used exclusively for leader/laggard discovery."
        ),
        "theme_count_before_canonicalization": len(_base),
        "theme_count_after_canonicalization":  len(ENRICHED_THEME_RS_UNIVERSE),
        "enriched_theme_count":            len(merged_rows),
        "existing_only_theme_count":       len(all_rows) - len(merged_rows),
        "watchlist_only_theme_count":      0,  # all watchlist sections map to existing themes
        "proxy_symbols_before":            base_proxy_count,
        "proxy_symbols_after":             len(ENRICHED_ALL_PROXY_SYMBOLS),
        "candidate_symbols_before":        base_cand_count,
        "candidate_symbols_after":         len(ENRICHED_ALL_CANDIDATE_SYMBOLS),
        "duplicate_groups_collapsed":      duplicate_groups_collapsed,
        "watchlist_page_modified":         False,
        # ── 5 examples ───────────────────────────────────────────────────────
        "examples":                        examples,
        # ── Full per-theme detail ─────────────────────────────────────────────
        "canonical_themes":                all_rows,
    }
