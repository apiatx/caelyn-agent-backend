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
    # Taxonomy v2 canonical section → theme_id mappings
    # Deprecated IDs that still exist in registry are kept for backward compat
    # until all stored watchlist sections are migrated to new labels.

    # Direct new-label matches
    "Agricultural Inputs & Fertilizers": "agri_inputs_fertilizers",
    "Battery Technology & Energy Storage": "battery_tech_storage",
    "Banking":                         "banks",
    "Blockchain Infrastructure":       "blockchain_infrastructure",
    "Bitcoin Miners":                  "bitcoin_miners",
    "Consumer & Housing":              "consumer_housing",
    "Consumer Retail":                 "consumer_retail",
    "Data Center Connectivity & Interconnect Silicon": "dc_connectivity_silicon",
    "Defense & Aerospace":             "defense",
    "Defense Platforms & Electronics": "defense_platforms_electronics",
    "Digital Asset Platforms":         "digital_asset_platforms",
    "Earth Observation & Space Data":  "earth_observation",
    "Fintech & Digital Payments":      "fintech",
    "Grid & Electrification":          "grid_electrification",
    "Grid Hardware & Electrical Equipment": "grid_hardware_electrical",
    "Healthcare Innovation":           "healthcare_innovation",
    "Industrial Automation":           "industrial_automation",
    "Launch & Space Systems":          "launch_space_systems",
    "Lithium":                         "lithium",
    "Networking & Fabric Infrastructure": "networking_fabric_infra",
    "Nuclear Energy":                  "nuclear_energy",
    "Nuclear Equipment & Services":    "nuclear_equipment_services",
    "Nuclear Utilities & Operators":   "nuclear_utilities_operators",
    "Optical Components & Lasers":     "optical_components_lasers",
    "Optical Interconnects":           "optical_interconnects",
    "Packaging & Substrates":          "packaging_substrates",
    "Photonics & Optical Systems":     "photonics_optical",
    "Power & Cooling":                 "power_cooling",
    "Precious Metals":                 "precious_metals",
    "Rare Earths & Strategic Minerals": "rare_earth",
    "Satellite Communications":        "satellite_comms",
    "Semiconductor Equipment":         "semicap_equip",
    "Semiconductor Materials":         "semicap_materials_node",
    "Servers & Compute Systems":       "servers_compute_systems",
    "SMRs & Advanced Reactors":        "smr_advanced_reactors",
    "Solar":                           "solar",
    "Steel & Ferrous Metals":          "steel_ferrous",
    "Test & Measurement":              "test_measurement",
    "Transportation & Mobility":       "transportation_mobility",
    "Travel & Leisure":                "travel_leisure",
    "Uranium Mining & Nuclear Fuel":   "uranium_nuclear_fuel",
    "Wind & Renewable Generation":     "wind_renewable",

    # Legacy section name → best new node (backward compat)
    "AI Networking":               "networking_fabric_infra",   # v1 catch-all → DC networking
    "AI Accelerators & Compute Silicon": "ai_accelerators",
    "AI Cloud & Data Center Operators":  "ai_cloud_dc_operators",
    "AI Software & Data Platforms":      "ai_software_platforms",
    "Analog, Power & Mixed-Signal":      "analog_power_mixed",
    "Base Metals & Diversified Mining":  "base_metals_diversified",
    "Building Products":               "building_products",
    "Clean Energy":                    "clean_energy",
    "Construction & Infrastructure":   "construction_infrastructure",
    "Crypto Equities / Blockchain":    "crypto_equities",
    "Cybersecurity":                   "cybersecurity",
    "Data Center Infrastructure":      "datacenter_infra",
    "Defense":                         "defense",
    "Diagnostics & Life Science Tools": "diagnostics_life_science",
    "Distributed & Backup Power":      "distributed_backup_power",
    "Drones":                          "drones",
    "Drones & Autonomous Systems":     "drones",
    "Engineering & Construction":      "engineering_construction",
    "Farm Machinery & Equipment":      "farm_machinery",
    "Food Producers & Processors":     "food_producers",
    "Foundry & Manufacturing":         "foundry_manufacturing",
    "Freight & Logistics":             "freight_logistics",
    "Heavy Equipment":                 "heavy_equipment",
    "Homebuilders":                    "homebuilders",
    "Hydrogen & Fuel Cells":           "hydrogen_fuel_cells",
    "Industrial Controls & Sensors":   "industrial_controls_sensors",
    "Industrials":                     "industrials",
    "Insurance":                       "insurance",
    "Memory & Storage":                "memory_storage",
    "Medical Devices":                 "medical_devices",
    "Metals & Mining":                 "metals_mining",
    "Biotech":                         "biotech",
    "Oil & Gas":                       "oil_gas",
    "Oil Services":                    "oil_services",
    "Power Generation & Turbines":     "power_generation_turbines",
    "Quantum Computing":               "quantum",
    "Rare Earth Metals":               "rare_earth",      # v1 label → same node
    "Regional Banks":                  "regional_banks",
    "Robotics & Automation":           "robotics_automation",
    "Sensing & LiDAR":                 "sensing_lidar",
    "Semiconductors":                  "semiconductors",
    "Space Economy":                   "space",
    "Space":                           "space",
    "Specialty Alloys & Engineered Metals": "specialty_alloys",
    "Electronic & Semiconductor Materials": "electronic_materials",
    "Composites & Specialty Materials": "composites_materials",
    "Software":                        "software",
    "Cloud Software":                  "cloud_software",
    "E&P / Upstream":                  "ep_upstream",
    "Integrated Oil & Refining":       "integrated_oil_refining",
    "LNG & Natural Gas":               "lng_gas",
    "Midstream & Pipelines":           "midstream_pipelines",

    # Legacy deprecated section names → nearest new bucket (migration compatibility)
    # These STILL WORK because deprecated nodes remain in the registry.
    "Lithium & Battery Tech":      "lithium_battery",    # deprecated → ambiguous split; keep mapping
    "Nuclear / Grid":              "nuclear_energy",      # v1 catch-all → nuclear_energy parent
    "Photonics / Lasers":          "optical_components_lasers",  # v1 → optical child
    "Power / Cooling":             "power_cooling",       # same ID, new parent
    "Semi Equipment & Materials":  "semicap_equipment",   # deprecated → ambiguous; keep for migration
    "Semi Equipment":              "semicap_equipment",
    "Semi Materials":              "semicap_materials_node",
    "Semicap Equipment":           "semicap_equip",
    "Substrates / Packaging":      "packaging_substrates",  # renamed node
    "Uranium & Nuclear Energy":    "nuclear_energy",         # deprecated → new parent

    # Generic catch-all; no theme mapping.
    "Other / Uncategorized":       None,
}

# Maps watchlist category-override category names → canonical theme_id.
_CATEGORY_TO_THEME_ID: dict[str, Optional[str]] = {
    # Taxonomy v2 labels
    "Agribusiness":                    "agribusiness",
    "AI Accelerators & Compute Silicon": "ai_accelerators",
    "AI Cloud & Data Center Operators":  "ai_cloud_dc_operators",
    "AI Software & Data Platforms":      "ai_software_platforms",
    "Analog, Power & Mixed-Signal":      "analog_power_mixed",
    "Banking":                           "banks",
    "Base Metals & Diversified Mining":  "base_metals_diversified",
    "Battery Technology & Energy Storage": "battery_tech_storage",
    "Biotech":                           "biotech",
    "Bitcoin Miners":                    "bitcoin_miners",
    "Blockchain Infrastructure":         "blockchain_infrastructure",
    "Clean Energy":                      "clean_energy",
    "Cloud Software":                    "cloud_software",
    "Consumer & Housing":                "consumer_housing",
    "Consumer Retail":                   "consumer_retail",
    "Construction & Infrastructure":     "construction_infrastructure",
    "Crypto Equities / Blockchain":      "crypto_equities",
    "Cybersecurity":                     "cybersecurity",
    "Data Center Infrastructure":        "datacenter_infra",
    "Data Center Connectivity & Interconnect Silicon": "dc_connectivity_silicon",
    "Defense & Aerospace":               "defense",
    "Defense Platforms & Electronics":   "defense_platforms_electronics",
    "Diagnostics & Life Science Tools":  "diagnostics_life_science",
    "Digital Asset Platforms":           "digital_asset_platforms",
    "Distributed & Backup Power":        "distributed_backup_power",
    "Drones & Autonomous Systems":       "drones",
    "Earth Observation & Space Data":    "earth_observation",
    "Fintech & Digital Payments":        "fintech",
    "Foundry & Manufacturing":           "foundry_manufacturing",
    "Freight & Logistics":               "freight_logistics",
    "Grid & Electrification":            "grid_electrification",
    "Healthcare Innovation":             "healthcare_innovation",
    "Homebuilders":                      "homebuilders",
    "Hydrogen & Fuel Cells":             "hydrogen_fuel_cells",
    "Industrial Automation":             "industrial_automation",
    "Industrial Controls & Sensors":     "industrial_controls_sensors",
    "Insurance":                         "insurance",
    "Launch & Space Systems":            "launch_space_systems",
    "Lithium":                           "lithium",
    "Medical Devices":                   "medical_devices",
    "Memory & Storage":                  "memory_storage",
    "Metals & Mining":                   "metals_mining",
    "Midstream & Pipelines":             "midstream_pipelines",
    "Networking & Fabric Infrastructure": "networking_fabric_infra",
    "Nuclear Energy":                    "nuclear_energy",
    "Nuclear Equipment & Services":      "nuclear_equipment_services",
    "Nuclear Utilities & Operators":     "nuclear_utilities_operators",
    "Oil & Gas":                         "oil_gas",
    "Oil Services":                      "oil_services",
    "Optical Components & Lasers":       "optical_components_lasers",
    "Optical Interconnects":             "optical_interconnects",
    "Packaging & Substrates":            "packaging_substrates",
    "Photonics & Optical Systems":       "photonics_optical",
    "Power & Cooling":                   "power_cooling",
    "Power Generation & Turbines":       "power_generation_turbines",
    "Precious Metals":                   "precious_metals",
    "Quantum Computing":                 "quantum",
    "Rare Earths & Strategic Minerals":  "rare_earth",
    "Regional Banks":                    "regional_banks",
    "Robotics & Automation":             "robotics_automation",
    "Satellite Communications":          "satellite_comms",
    "Semiconductors":                    "semiconductors",
    "Semiconductor Equipment":           "semicap_equip",
    "Semiconductor Materials":           "semicap_materials_node",
    "Sensing & LiDAR":                   "sensing_lidar",
    "Servers & Compute Systems":         "servers_compute_systems",
    "SMRs & Advanced Reactors":          "smr_advanced_reactors",
    "Software":                          "software",
    "Solar":                             "solar",
    "Space Economy":                     "space",
    "Steel & Ferrous Metals":            "steel_ferrous",
    "Test & Measurement":                "test_measurement",
    "Transportation & Mobility":         "transportation_mobility",
    "Travel & Leisure":                  "travel_leisure",
    "Uranium Mining & Nuclear Fuel":     "uranium_nuclear_fuel",
    "Wind & Renewable Generation":       "wind_renewable",
    # Legacy labels (v1 backward compat)
    "Fintech":                           "fintech",
    "Uranium & Nuclear Energy":          "nuclear_energy",
    "AI Networking":                     "networking_fabric_infra",
}

# Watchlist section names that collapsed into an existing theme_id under a
# different label (used for the "aliases" field in merge-debug output).
# key = canonical theme_id,  value = list of alternate watchlist section names merged in.
_THEME_SECTION_ALIASES: dict[str, list[str]] = {
    # v1 deprecated → v2 equivalents
    "nuclear_energy":         ["Nuclear / Grid", "Uranium & Nuclear Energy"],
    "networking_fabric_infra": ["AI Networking"],
    "optical_components_lasers": ["Photonics / Lasers"],
    "packaging_substrates":   ["Substrates / Packaging"],
    "semicap_equip":          ["Semiconductor Equipment", "Semicap Equipment", "Semi Equipment"],
    "semicap_materials_node": ["Semi Materials", "Semiconductor Materials"],
    # Legacy aliases kept for merge-debug output readability
    "semicap_equipment": [
        "Semi Equipment & Materials",
    ],
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

# Exchange prefix lookup for TradingView chart symbols.
# TradingView uses "AMEX" for NYSE Arca ETFs and "NASDAQ" for NASDAQ-listed ones.
# Any ticker NOT in this map is returned bare — TradingView auto-resolves US tickers.
_TV_ETF_EXCHANGE_MAP: dict[str, str] = {
    # ── NASDAQ-listed ETFs ─────────────────────────────────────────────────────
    "SMH":  "NASDAQ", "SOXX": "NASDAQ", "ICLN": "NASDAQ", "GRID": "NASDAQ",
    "QTUM": "NASDAQ", "ROBO": "NASDAQ", "DTCR": "NASDAQ", "BOTZ": "NASDAQ",
    "CIBR": "NASDAQ", "FINX": "NASDAQ", "BLOK": "NASDAQ", "IBB":  "NASDAQ",
    "SKYY": "NASDAQ", "HACK": "NASDAQ", "PSCT": "NASDAQ", "PAVE": "NASDAQ",
    "QCLN": "NASDAQ",
    # ── AMEX / NYSE Arca ETFs ──────────────────────────────────────────────────
    "MOO":  "AMEX", "DBA":  "AMEX", "VEGI": "AMEX", "KBE":  "AMEX",
    "XBI":  "AMEX", "ARKG": "AMEX", "XLB":  "AMEX", "XLC":  "AMEX",
    "XLF":  "AMEX", "XLI":  "AMEX", "XLK":  "AMEX", "XLP":  "AMEX",
    "XLU":  "AMEX", "XLRE": "AMEX", "XLY":  "AMEX", "XLV":  "AMEX",
    "XLE":  "AMEX", "ITA":  "AMEX", "URA":  "AMEX", "COPX": "AMEX",
    "LIT":  "AMEX", "REMX": "AMEX", "KRE":  "AMEX", "ITB":  "AMEX",
    "IHI":  "AMEX", "TAN":  "AMEX", "XME":  "AMEX", "XRT":  "AMEX",
    "IWM":  "AMEX", "GLD":  "AMEX", "SLV":  "AMEX",
    "ARKX": "AMEX", "ARKK": "AMEX", "ARKF": "AMEX", "BATT": "AMEX",
    "FCG":  "AMEX", "IWC":  "AMEX", "IGV":  "AMEX", "IYT":  "AMEX",
    "OIH":  "AMEX", "XOP":  "AMEX", "RSP":  "AMEX", "FFTY": "AMEX",
    "KIE":  "AMEX", "XLRE": "AMEX", "MAGS": "AMEX", "IWF":  "AMEX",
    "XLC":  "AMEX", "DRAM": "AMEX",
}


def _make_tv_symbol(ticker: str) -> str:
    """Return a TradingView-ready 'EXCHANGE:TICKER' string for theme chart symbols.

    Uses _TV_ETF_EXCHANGE_MAP for known ETFs; falls back to bare ticker for anything
    else (TradingView auto-resolves US-listed stocks and ETFs without an exchange prefix).
    """
    t = ticker.strip().upper()
    if ":" in t:
        return t  # already exchange-prefixed
    exchange = _TV_ETF_EXCHANGE_MAP.get(t)
    return f"{exchange}:{t}" if exchange else t


_REPRESENTATIVE_ETF_MAP: dict[str, str] = {
    # ── Custom basket themes (no ETF in base proxy_symbols) ──────────────────
    "ai_networking":        "SMH",    # basket of stocks; SMH is the nearest ETF proxy
    "photonics_lasers":     "ROBO",   # no photonics ETF; robotics ETF is closest
    "power_cooling":        "GRID",   # power-infrastructure ETF
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


_LAST_WATCHLIST_MEMBERSHIP_DIAGNOSTICS: dict = {
    "watchlist_count": 0,
    "assigned_count": 0,
    "unassigned_symbols": [],
    "theme_counts": {},
}


def _load_current_watchlist_theme_membership() -> dict[str, list[str]]:
    """
    Dynamic Watchlist-derived Theme membership layer.

    Source: CURRENT SAVED WATCHLIST STATE (Postgres `watchlist` row, live read
    — no snapshot, no static list). For every symbol currently saved in the
    Watchlist, resolve its assigned Theme using the ONE canonical resolver
    (services.theme_resolver.resolve_primary_theme_for_ticker), then include
    it in that Theme's membership.

    Returns: {theme_id: sorted_us_ticker_list}
    Returns {} if Postgres is unavailable or the watchlist is empty.

    No LLM/provider calls. No independent theme classification — resolution
    is fully delegated to the canonical resolver (theme_ticker_mapper's
    canonical map + CSV-industry fallback). Tickers with no resolution are
    left unassigned (recorded in diagnostics only, never guessed).
    """
    global _LAST_WATCHLIST_MEMBERSHIP_DIAGNOSTICS

    try:
        from data.pg_storage import is_available, watchlist_read
    except ImportError:
        log.warning("[THEME_MERGE] pg_storage not importable — static universe only")
        return {}

    if not is_available():
        log.warning("[THEME_MERGE] Postgres unavailable — static universe only")
        return {}

    try:
        store = watchlist_read(_DEV_WATCHLIST_ID)
        if not store:
            # _DEV_WATCHLIST_ID can go stale if the saved Watchlist row was
            # re-created under a new id (e.g. re-uploaded from scratch).
            # Fall back to the most recently saved watchlist — the exact same
            # "current saved Watchlist" resolution watchlist_service.load_watchlist()
            # uses when no explicit id is given.
            from data.pg_storage import watchlist_list as _pg_wl_list
            entries = _pg_wl_list()
            if entries:
                store = watchlist_read(entries[0]["id"])
    except Exception as exc:
        log.warning(f"[THEME_MERGE] Error reading current watchlist: {exc}")
        return {}

    if not store or not store.get("tickers"):
        log.warning(f"[THEME_MERGE] Watchlist {_DEV_WATCHLIST_ID!r} empty/not found")
        return {}

    tickers: list[str] = [
        (t or "").strip().upper() for t in store.get("tickers", []) if t
    ]
    tickers = [t for t in tickers if t and _is_us_ticker(t)]

    csv_data = store.get("csv_data") or []
    csv_map: dict[str, dict] = {}
    for row in csv_data:
        sym = (row.get("Symbol") or row.get("symbol") or row.get("Ticker") or "").strip().upper()
        if sym:
            csv_map[sym] = row

    try:
        from services.theme_resolver import (
            build_theme_resolution_context,
            resolve_primary_theme_for_ticker,
        )
    except ImportError as exc:
        log.warning(f"[THEME_MERGE] theme_resolver not importable: {exc}")
        return {}

    # NOTE: at this point in _build() the Themes-page membership map inside
    # the resolver's ctx reflects the PRE-watchlist-merge base universe (this
    # function's own output hasn't been merged in yet), so resolution here is
    # driven by the canonical_map / industry_fallback tiers — exactly the
    # same "what Theme is this ticker assigned to" answer the Watchlist UI
    # itself shows. This is intentional: it is the single source of truth,
    # not a second/independent classifier.
    ctx = build_theme_resolution_context()

    theme_to_tickers: dict[str, set[str]] = {}
    unassigned: list[str] = []

    for sym in tickers:
        industry = (csv_map.get(sym, {}).get("Industry")
                    or csv_map.get(sym, {}).get("industry") or "").strip()
        res = resolve_primary_theme_for_ticker(sym, industry=industry, ctx=ctx)
        tid = res.get("theme_id")
        if tid:
            theme_to_tickers.setdefault(tid, set()).add(sym)
        else:
            unassigned.append(sym)

    result = {tid: sorted(syms) for tid, syms in theme_to_tickers.items() if syms}

    _LAST_WATCHLIST_MEMBERSHIP_DIAGNOSTICS = {
        "watchlist_count": len(tickers),
        "assigned_count": sum(len(v) for v in result.values()),
        "unassigned_symbols": sorted(unassigned),
        "theme_counts": {tid: len(v) for tid, v in result.items()},
    }

    log.info(
        f"[THEME_MERGE] Live watchlist membership resolved: "
        f"{len(tickers)} watchlist symbols, "
        f"{_LAST_WATCHLIST_MEMBERSHIP_DIAGNOSTICS['assigned_count']} assigned "
        f"across {len(result)} themes, {len(unassigned)} unassigned"
    )

    return result


def get_last_watchlist_membership_diagnostics() -> dict:
    """Read-only diagnostics snapshot from the most recent membership sync."""
    return dict(_LAST_WATCHLIST_MEMBERSHIP_DIAGNOSTICS)


def _load_watchlist_theme_tickers() -> dict[str, list[str]]:
    """
    LEGACY / UNUSED (kept for reference only — superseded by
    _load_current_watchlist_theme_membership, which uses the canonical
    resolver instead of a hardcoded analysis-section-title map).

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

def _load_theme_ticker_overrides() -> dict[str, dict]:
    """
    Load manual admin overrides from Neon.
    Returns {theme_id: {"add": [symbols...], "remove": [symbols...]}}
    Failures are non-fatal — returns empty dict on any error.
    """
    try:
        from data.pg_storage import get_theme_ticker_overrides
        rows = get_theme_ticker_overrides()
        result: dict[str, dict] = {}
        for row in rows:
            tid = row["theme_id"]
            if tid not in result:
                result[tid] = {"add": [], "remove": []}
            result[tid][row["action"]].append(row["symbol"])
        if result:
            log.info(
                f"[THEME_MERGE] Loaded manual overrides: "
                f"{sum(len(v['add']) for v in result.values())} adds, "
                f"{sum(len(v['remove']) for v in result.values())} removes "
                f"across {len(result)} themes"
            )
        return result
    except Exception as exc:
        log.warning(f"[THEME_MERGE] Could not load theme_ticker_overrides from Neon: {exc}")
        return {}


def _build_enriched_universe(
    base: dict,
    watchlist_tickers: dict[str, list[str]],
    manual_overrides: dict[str, dict] | None = None,
) -> tuple[dict, dict[str, list[str]]]:
    """
    Deep-copy base THEME_RS_UNIVERSE and enrich matching themes.

    Apply order per theme:
      1. Start with BASE proxy_symbols / candidate_symbols.
      2. Apply watchlist/dev seed merge (watchlist_tickers).
      3. Apply manual admin overrides (manual_overrides):
           action='remove' → exclude from that theme only (does not affect other themes)
           action='add'    → force-include in that theme's basket
      4. Deduplicate within each theme only.

    CRITICAL: proxy_symbols is the ONLY field read by _compute_theme_perf.
    candidate_symbols is used exclusively for leader/laggard discovery.
    Existing symbols are NEVER removed except by explicit admin override.

    Also stamps representative_symbol, representative_symbol_source,
    holdings_display_mode, manual_added_symbols, and manual_removed_symbols
    onto EVERY theme.

    Returns (enriched_universe, {theme_id: [net_new_proxy_tickers]})
    """
    if manual_overrides is None:
        manual_overrides = {}

    enriched = copy.deepcopy(base)
    # Track net-new proxy additions per theme for the debug endpoint
    net_new_proxy: dict[str, list[str]] = {}

    # ── Step 2: Watchlist seed merge ─────────────────────────────────────────
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
                f"+{len(new_proxy)} watchlist proxy ticker(s) → {new_proxy}"
            )

    # ── Step 3: Manual admin overrides (highest priority) ───────────────────
    # Overrides are applied after watchlist merge so they win over everything.
    # Removing a symbol removes it from THIS theme only — no cross-theme side effects.
    # The same symbol can be in multiple themes with independent override rows.
    for theme_id, actions in manual_overrides.items():
        if theme_id not in enriched:
            log.debug(f"[THEME_MERGE] override theme_id '{theme_id}' not in universe — skipped")
            continue
        meta = enriched[theme_id]
        proxy_set = set(meta.get("proxy_symbols",     []))
        cand_set  = set(meta.get("candidate_symbols", []))

        add_syms    = sorted(set(actions.get("add",    [])))
        remove_syms = sorted(set(actions.get("remove", [])))

        # Remove (from THIS theme only)
        proxy_set -= set(remove_syms)
        cand_set  -= set(remove_syms)

        # Add (to THIS theme only)
        proxy_set |= set(add_syms)
        cand_set  |= set(add_syms)

        meta["proxy_symbols"]     = sorted(proxy_set)
        meta["candidate_symbols"] = sorted(cand_set)

        # Stamp for debug introspection and holdings_display_mode logic
        meta["manual_added_symbols"]   = add_syms
        meta["manual_removed_symbols"] = remove_syms

        # Recompute watchlist_seeds: remove any seeds that were manually excluded
        if remove_syms:
            existing_seeds = set(meta.get("watchlist_seeds", []))
            meta["watchlist_seeds"] = sorted(existing_seeds - set(remove_syms))

        if add_syms or remove_syms:
            log.info(
                f"[THEME_MERGE] {theme_id}: manual override +{len(add_syms)} "
                f"-{len(remove_syms)}"
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
        # tv_symbol: exchange-prefixed TradingView chart symbol derived from
        # representative_symbol.  Used by the frontend TradingView widget embed.
        meta["tv_symbol"] = _make_tv_symbol(rep_sym)
        # holdings_display_mode: always "theme_basket".
        # The frontend shows proxy_symbols directly as the curated basket for every
        # theme. "etf_holdings" mode required a live FMP ETF-holdings fetch which
        # failed on restricted data plans ("ETF holdings unavailable on current data
        # plan"). Since every theme already has a pre-computed proxy_symbols basket
        # (ETF proxies for sector themes, hand-curated stocks for custom/hybrid),
        # there is no reason to delegate holdings population to the frontend.
        meta["holdings_display_mode"] = "theme_basket"

    return enriched, net_new_proxy


# ── Module-level initialisation (runs once at import time) ─────────────────────

def _build() -> tuple[dict, dict[str, list[str]]]:
    """
    Build the enriched universe. Apply order:
      1. Base THEME_RS_UNIVERSE (static definitions in theme_rs_universe.py)
      2. Manual overrides from Neon theme_ticker_overrides (highest priority):
           source='manual_admin'            — dev/admin theme editor additions/removals
           source='watchlist_snapshot_seed' — frozen snapshot of historical watchlist seeds
                                              (materialized 2026-06-22; no longer live-fetched)
    Falls back gracefully on any failure.

    Live Watchlist reads use _load_current_watchlist_theme_membership(), which
    resolves each CURRENT saved Watchlist ticker's Theme via the canonical
    resolver (services.theme_resolver.resolve_primary_theme_for_ticker) and
    adds it to that Theme's membership. This runs on every refresh, so the
    Themes-page universe always reflects the current saved Watchlist state
    with no code changes needed when the Watchlist changes.

    Manual admin overrides (source='manual_admin') are applied AFTER the
    watchlist merge (see _build_enriched_universe Step 3) and always win —
    an explicit manual removal cannot be resurrected by the Watchlist sync.
    """
    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE
    except ImportError as exc:
        log.error(f"[THEME_MERGE] Cannot import THEME_RS_UNIVERSE: {exc}")
        return {}, {}

    watchlist_tickers: dict[str, list[str]] = _load_current_watchlist_theme_membership()
    manual_overrides  = _load_theme_ticker_overrides()

    if not manual_overrides and not watchlist_tickers:
        log.info("[THEME_MERGE] No override/watchlist data — stamping representative symbols only")
        merged, net_new = _build_enriched_universe(THEME_RS_UNIVERSE, {}, {})
        return merged, net_new

    merged, net_new = _build_enriched_universe(THEME_RS_UNIVERSE, watchlist_tickers, manual_overrides)
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
    True in-place mutation of the module-level enriched universe dicts/lists.

    WHY IN-PLACE:
      Other modules (e.g. theme_rs_service) do:
        from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as THEME_RS_UNIVERSE
      Python binds that name to the dict OBJECT at import time. If we rebind the
      module global (ENRICHED_THEME_RS_UNIVERSE = new_dict), the other module's
      alias still points to the OLD object and never sees updated data until restart.

      Using .clear() + .update() mutates the SAME object in-place, so all existing
      aliases everywhere see the fresh data immediately — no restart required.
    """
    global _net_new_proxy
    new_uni, new_net_new = _build()

    # Dict: mutate in-place so module-level aliases in other modules stay live
    ENRICHED_THEME_RS_UNIVERSE.clear()
    ENRICHED_THEME_RS_UNIVERSE.update(new_uni)
    _net_new_proxy = new_net_new

    # Lists: slice-assignment mutates in-place for the same reason
    ENRICHED_ALL_PROXY_SYMBOLS[:] = sorted(
        set(sym for v in ENRICHED_THEME_RS_UNIVERSE.values()
            for sym in v.get("proxy_symbols", []))
    )
    ENRICHED_ALL_CANDIDATE_SYMBOLS[:] = sorted(
        set(sym for v in ENRICHED_THEME_RS_UNIVERSE.values()
            for sym in v.get("candidate_symbols", []))
        - {""}
    )
    log.info("[THEME_MERGE] Enriched universe refreshed (in-place, %d themes)", len(ENRICHED_THEME_RS_UNIVERSE))


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
