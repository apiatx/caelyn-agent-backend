"""
theme_ticker_mapper.py — ticker → theme mapping helper.

Combines theme maps in priority order:
  0. _FOREIGN_ALIAS_MAP         (explicit foreign/OTC/ADR aliases — HIGHEST PRIORITY)
  1. THEME_RS_UNIVERSE          (canonical 60-entry registry — PRIMARY)
  2. home_service.THEME_MAP     (curated ticker → theme name, FALLBACK)
  3. THEME_ETF_UNIVERSE         (old 20-theme ETF universe, FALLBACK)

All lookups are case-insensitive and O(1) via pre-built index.
Never raises — returns empty dict / "unknown" on missing tickers.

New fields (from THEME_RS_UNIVERSE):
  - theme_id         (machine-readable key, e.g. "semiconductors")
  - classification   ("sector" | "theme" | "sub_theme")
  - parent_sector    (parent sector theme_id, or None for top-level sectors)

Reused by:
  - thematic_context_provider._enrich_from_x_consensus()
  - options screener theme_alignment annotation
  - strategy screener regime_alignment annotation
"""
from __future__ import annotations

from typing import Optional

# ── Source 0: Foreign / OTC / ADR explicit alias map ─────────────────────────
# Maps exchange-prefixed, OTC, and foreign tickers to canonical themes.
# Only high-confidence, company-verified entries — no LLM calls, no heuristics.
# Checked first so exchange-prefixed symbols are never missed by Sources 1-3.
#
# Format: "SYMBOL_UPPER": ("display_name", "theme_id")
# theme_id must match a key in THEME_RS_UNIVERSE or home_service.THEME_MAP.
_FOREIGN_ALIAS_MAP: dict[str, tuple[str, str]] = {
    # Memory / Storage
    "KRX:000660": ("Memory & Storage",              "memory_storage"),       # SK Hynix — #2 DRAM/NAND globally

    # Semiconductor Equipment
    "OTC:ATEYY":  ("Semiconductor Equipment",       "semicap_equipment"),    # Advantest ADR — SoC/memory test systems
    "OTC:KRKNF":  ("Semiconductor Equipment",       "semicap_equipment"),    # Kokusai Electric — CVD/diffusion batch tools
    "ETR:AIXA":   ("Semiconductor Equipment",       "semicap_equipment"),    # Aixtron SE — MOCVD for GaN/SiC/III-V
    "FRA:KLA":    ("Semiconductor Equipment",       "semicap_equipment"),    # KLA Corp Frankfurt listing — process control

    # Semi Materials
    "AIM:IQE":    ("Semi Materials",                "semi_materials"),       # IQE plc — compound semiconductor epiwafer foundry
    "EPA:SOI":    ("Semi Materials",                "semi_materials"),       # Soitec — SOI & compound semiconductor wafers

    # Semiconductors
    "EPA:XFAB":   ("Semiconductors",                "semiconductors"),       # X-FAB — analog/mixed-signal specialty foundry

    # Substrates / Packaging
    "AMS:BESI":   ("Substrates / Packaging",        "substrates_/_packaging"), # BE Semiconductor (BESI) — advanced packaging equipment

    # Photonics / Lasers
    "STO:SIVE":   ("Photonics / Lasers",            "photonics_/_lasers"),   # Sivers Semiconductors — photonics/mmW ICs

    # Defense
    "ASX:EOS":    ("Defense",                       "defense"),              # Electro Optic Systems — laser/EO systems for defense
    "TSX:MAL":    ("Defense",                       "defense"),              # Magellan Aerospace — F-35 fuselage/defense structures

    # Crypto Equities / Blockchain
    "CIFR":       ("Crypto Equities / Blockchain",  "crypto_equities"),      # Cipher Mining — Bitcoin mining
    "GLXY":       ("Crypto Equities / Blockchain",  "crypto_equities"),      # Galaxy Digital — crypto financial services

    # Drones
    "ONDS":       ("Drones",                        "drones"),               # Ondas Holdings — American Robotics / drone automation
    "UMAC":       ("Drones",                        "drones"),               # Unusual Machines — FPV drone hardware

    # Space Economy
    "RDW":        ("Space Economy",                 "space"),                # Redwire Space — in-space manufacturing & solar arrays
    "SIDU":       ("Space Economy",                 "space"),                # Sidus Space — small satellite constellation
    "TSAT":       ("Space Economy",                 "space"),                # Telesat — LEO satellite constellation (Lightspeed)
}

# ── Index dicts (built once at import time, cheap in-memory) ─────────────────

_TICKER_TO_THEMES:        dict[str, list[str]] = {}   # "ANET" → ["AI Networking"]
_TICKER_TO_THEME_ID:      dict[str, str]       = {}   # "SMH"  → "semiconductors"
_TICKER_TO_CLASSIFICATION:dict[str, str]       = {}   # "SMH"  → "sub_theme"
_TICKER_TO_PARENT_SECTOR: dict[str, str]       = {}   # "SMH"  → "technology"

_ETF_TO_THEME_ID:  dict[str, str] = {}   # "SMH"  → "semiconductors"
_ETF_TO_LABEL:     dict[str, str] = {}   # "SMH"  → "Semiconductors"
_THEME_META:       dict[str, dict] = {}  # theme_name → {etfs, reps, parent_sector, classification, theme_id}

_built = False


def _build_index() -> None:
    global _built
    if _built:
        return
    _built = True

    # ── Source 0: Foreign/OTC alias map (highest priority — explicit overrides) ─
    for raw_sym, (display, theme_id) in _FOREIGN_ALIAS_MAP.items():
        s = raw_sym.upper()
        _TICKER_TO_THEMES.setdefault(s, [])
        if display not in _TICKER_TO_THEMES[s]:
            _TICKER_TO_THEMES[s].append(display)
        _TICKER_TO_THEME_ID[s]       = theme_id
        _TICKER_TO_CLASSIFICATION[s] = "sub_theme"

    # ── Source 1: THEME_RS_UNIVERSE (canonical 60-entry universe — PRIMARY) ─────
    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE
        for theme_id, meta in THEME_RS_UNIVERSE.items():
            display      = meta.get("display_name") or theme_id
            cls_val      = meta.get("classification", "theme")
            parent       = meta.get("parent_sector") or ""
            proxies      = [s.upper() for s in (meta.get("proxy_symbols") or [])]
            cands        = [s.upper() for s in (meta.get("candidate_symbols") or [])]
            sector_tags  = meta.get("sector_tags", [])
            aliases      = [a.replace("_", " ").title() for a in (meta.get("aliases") or [])]

            # Build THEME_META (display_name → metadata)
            if display not in _THEME_META:
                _THEME_META[display] = {
                    "theme_id":       theme_id,
                    "etfs":           proxies,
                    "representative_tickers": cands,
                    "parent_sector":  parent,
                    "classification": cls_val,
                    "sector_tags":    sector_tags,
                    "aliases":        aliases,
                    "source":         "theme_rs_universe",
                }
            else:
                for e in proxies:
                    if e not in _THEME_META[display].get("etfs", []):
                        _THEME_META[display].setdefault("etfs", []).append(e)

            # Map ETF proxy symbols → theme
            for sym in proxies:
                if sym not in _ETF_TO_THEME_ID:
                    _ETF_TO_THEME_ID[sym] = theme_id
                if sym not in _ETF_TO_LABEL:
                    _ETF_TO_LABEL[sym] = display

                _TICKER_TO_THEMES.setdefault(sym, [])
                if display not in _TICKER_TO_THEMES[sym]:
                    _TICKER_TO_THEMES[sym].append(display)

                if sym not in _TICKER_TO_THEME_ID:
                    _TICKER_TO_THEME_ID[sym] = theme_id
                if sym not in _TICKER_TO_CLASSIFICATION:
                    _TICKER_TO_CLASSIFICATION[sym] = cls_val
                if sym not in _TICKER_TO_PARENT_SECTOR and parent:
                    _TICKER_TO_PARENT_SECTOR[sym] = parent

            # Map candidate symbols → theme
            for sym in cands:
                _TICKER_TO_THEMES.setdefault(sym, [])
                if display not in _TICKER_TO_THEMES[sym]:
                    _TICKER_TO_THEMES[sym].append(display)

                if sym not in _TICKER_TO_THEME_ID:
                    _TICKER_TO_THEME_ID[sym] = theme_id
                if sym not in _TICKER_TO_CLASSIFICATION:
                    _TICKER_TO_CLASSIFICATION[sym] = cls_val
                if sym not in _TICKER_TO_PARENT_SECTOR and parent:
                    _TICKER_TO_PARENT_SECTOR[sym] = parent

    except Exception as e:
        print(f"[THEME_MAPPER] THEME_RS_UNIVERSE unavailable: {e}")

    # ── Source 2: home_service.THEME_MAP (curated ticker lists — FALLBACK) ────
    # Only adds tickers not already indexed by THEME_RS_UNIVERSE.
    try:
        from services.home_service import THEME_MAP
        for theme_name, tickers in THEME_MAP.items():
            if theme_name not in _THEME_META:
                _THEME_META[theme_name] = {
                    "theme_id":       theme_name.lower().replace(" ", "_").replace("/", "_"),
                    "etfs":           [],
                    "representative_tickers": [],
                    "parent_sector":  "Technology",
                    "classification": "sub_theme",
                    "source":         "home_theme_map",
                }
            for sym in tickers:
                s = sym.upper()
                _TICKER_TO_THEMES.setdefault(s, [])
                if theme_name not in _TICKER_TO_THEMES[s]:
                    _TICKER_TO_THEMES[s].append(theme_name)
                # Only set metadata fields if not already set by Source 1
                _TICKER_TO_THEME_ID.setdefault(s, theme_name.lower().replace(" ", "_"))
                _TICKER_TO_CLASSIFICATION.setdefault(s, "sub_theme")
    except Exception as e:
        print(f"[THEME_MAPPER] home_service.THEME_MAP unavailable: {e}")

    # ── Source 3: THEME_ETF_UNIVERSE (old 20-theme universe — FALLBACK) ──────
    # Only adds symbols not already covered by Source 1 or 2.
    try:
        from services.sector_rotation.theme_universe import THEME_ETF_UNIVERSE
        for theme_id, meta in THEME_ETF_UNIVERSE.items():
            label  = meta.get("label") or theme_id
            etfs   = [s.upper() for s in (meta.get("symbols") or [])]
            reps   = [s.upper() for s in (meta.get("representative_tickers") or [])]
            parent = meta.get("parent_sector", "")

            # ETF → theme mapping (only if not already set by Source 1)
            for etf in etfs:
                if etf not in _ETF_TO_THEME_ID:
                    _ETF_TO_THEME_ID[etf] = theme_id
                if etf not in _ETF_TO_LABEL:
                    _ETF_TO_LABEL[etf] = label
                _TICKER_TO_THEMES.setdefault(etf, [])
                if label not in _TICKER_TO_THEMES[etf]:
                    _TICKER_TO_THEMES[etf].append(label)
                _TICKER_TO_THEME_ID.setdefault(etf, theme_id)
                _TICKER_TO_CLASSIFICATION.setdefault(etf, "theme")
                if parent:
                    _TICKER_TO_PARENT_SECTOR.setdefault(etf, parent)

            # Representative tickers → theme (fallback only)
            for rep in reps:
                _TICKER_TO_THEMES.setdefault(rep, [])
                if label not in _TICKER_TO_THEMES[rep]:
                    _TICKER_TO_THEMES[rep].append(label)
                _TICKER_TO_THEME_ID.setdefault(rep, theme_id)
                _TICKER_TO_CLASSIFICATION.setdefault(rep, "theme")
                if parent:
                    _TICKER_TO_PARENT_SECTOR.setdefault(rep, parent)

            # THEME_META for label (only if not already set by Source 1)
            if label not in _THEME_META:
                _THEME_META[label] = {
                    "theme_id":       theme_id,
                    "etfs":           etfs,
                    "representative_tickers": reps,
                    "parent_sector":  parent,
                    "classification": "theme",
                    "source":         "theme_etf_universe",
                }
            else:
                existing = _THEME_META[label]
                for e in etfs:
                    if e not in existing.get("etfs", []):
                        existing.setdefault("etfs", []).append(e)
    except Exception as e:
        print(f"[THEME_MAPPER] THEME_ETF_UNIVERSE unavailable: {e}")

    print(
        f"[THEME_MAPPER] Index built: {len(_THEME_META)} themes, "
        f"{len(_TICKER_TO_THEMES)} tickers, {len(_ETF_TO_THEME_ID)} ETF proxies"
    )


# ── Public API ────────────────────────────────────────────────────────────────

def map_ticker_to_themes(ticker: str) -> list[str]:
    """Return list of theme names for a ticker (may be empty). Never raises."""
    _build_index()
    return list(_TICKER_TO_THEMES.get(ticker.upper(), []))


def map_ticker_to_primary_theme(ticker: str) -> Optional[str]:
    """Return the first (most specific) theme name for a ticker, or None."""
    themes = map_ticker_to_themes(ticker)
    return themes[0] if themes else None


def map_ticker_to_theme_id(ticker: str) -> Optional[str]:
    """Return the canonical theme_id (machine key) for a ticker, or None."""
    _build_index()
    return _TICKER_TO_THEME_ID.get(ticker.upper())


def map_ticker_to_classification(ticker: str) -> Optional[str]:
    """Return 'sector', 'theme', or 'sub_theme' for a ticker, or None."""
    _build_index()
    return _TICKER_TO_CLASSIFICATION.get(ticker.upper())


def map_ticker_to_parent_sector(ticker: str) -> Optional[str]:
    """Return the parent_sector theme_id for a ticker, or None."""
    _build_index()
    return _TICKER_TO_PARENT_SECTOR.get(ticker.upper())


def get_theme_meta(theme_name: str) -> dict:
    """Return {etfs, representative_tickers, parent_sector, classification, theme_id, source} for a theme name."""
    _build_index()
    return dict(_THEME_META.get(theme_name, {}))


def map_etf_to_theme_label(etf: str) -> Optional[str]:
    """Return the theme label for an ETF proxy ticker (e.g. SMH → 'Semiconductors')."""
    _build_index()
    return _ETF_TO_LABEL.get(etf.upper())


def map_etf_to_theme_id(etf: str) -> Optional[str]:
    """Return the theme_id for an ETF proxy ticker (e.g. SMH → 'semiconductors')."""
    _build_index()
    return _ETF_TO_THEME_ID.get(etf.upper())


def get_all_theme_names() -> list[str]:
    """Return all known theme names from all sources."""
    _build_index()
    return list(_THEME_META.keys())


def get_ticker_theme_alignment(ticker: str, active_themes: list[dict],
                                emerging_themes: list[dict],
                                dead_zones: list[dict]) -> dict:
    """
    Return a theme_alignment dict for a single ticker against the current snapshot.

    {
      "theme_name":             str | None,
      "theme_state":            "active"|"emerging"|"dead_zone"|"neutral"|"unknown",
      "regime_alignment_score": float,   # 0.0–1.0
      "regime_alignment_label": str,
      "thematic_badges":        [str],
      "dead_zone_warning":      bool,
    }
    """
    _build_index()
    ticker_up = ticker.upper()
    themes    = _TICKER_TO_THEMES.get(ticker_up, [])

    if not themes:
        return {
            "theme_name":             None,
            "theme_state":            "unknown",
            "regime_alignment_score": 0.0,
            "regime_alignment_label": "No theme map",
            "thematic_badges":        [],
            "dead_zone_warning":      False,
        }

    # Build lookup sets by theme name
    active_names   = {t["name"] for t in active_themes}
    emerging_names = {t["name"] for t in emerging_themes}
    dead_names     = {t["name"] for t in dead_zones}

    best_state  = "neutral"
    best_theme  = themes[0]
    best_score  = 0.0
    badges: list[str] = []
    dz_warn = False

    for theme in themes:
        if theme in active_names:
            best_state = "active"
            best_theme = theme
            best_score = 0.8
            badges.append(f"Active: {theme}")
            break
        elif theme in emerging_names and best_state != "active":
            best_state = "emerging"
            best_theme = theme
            best_score = 0.5
            badges.append(f"Emerging: {theme}")
        elif theme in dead_names and best_state not in ("active", "emerging"):
            best_state = "dead_zone"
            best_theme = theme
            best_score = -0.2
            badges.append(f"Dead Zone: {theme}")
            dz_warn = True

    label_map = {
        "active":    "Regime-Aligned Active Theme",
        "emerging":  "Regime-Aligned Emerging Theme",
        "dead_zone": "Dead Zone — below-average momentum",
        "neutral":   "Neutral — no strong theme signal",
    }

    return {
        "theme_name":             best_theme,
        "theme_state":            best_state,
        "regime_alignment_score": best_score,
        "regime_alignment_label": label_map.get(best_state, "Unknown"),
        "thematic_badges":        badges[:3],
        "dead_zone_warning":      dz_warn,
    }
