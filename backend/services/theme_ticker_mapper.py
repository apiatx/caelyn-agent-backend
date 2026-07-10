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
    "AIM:IQE":    ("Semi Materials",                "semiconductors"),       # IQE plc — compound semiconductor epiwafer foundry
    "EPA:SOI":    ("Semi Materials",                "semiconductors"),       # Soitec — SOI & compound semiconductor wafers

    # Semiconductors
    "EPA:XFAB":   ("Semiconductors",                "semiconductors"),       # X-FAB — analog/mixed-signal specialty foundry

    # Substrates / Packaging
    "AMS:BESI":   ("Substrates / Packaging",        "substrates_/_packaging"), # BE Semiconductor (BESI) — advanced packaging equipment

    # Photonics / Lasers
    "STO:SIVE":   ("Photonics / Lasers",            "photonics_/_lasers"),   # Sivers Semiconductors — photonics/mmW ICs

    # Defense
    "ASX:EOS":    ("Defense",                       "defense"),              # Electro Optic Systems — laser/EO systems for defense
    "TSX:MAL":    ("Defense",                       "defense"),              # Magellan Aerospace — F-35 fuselage/defense structures
    "CODA":       ("Defense",                       "defense"),              # Coda Octopus — underwater sonar/3D seabed imaging for defense navies
    "SYPR":       ("Defense",                       "defense"),              # Sypris Solutions — defense electronics, secure comms hardware, aerospace forgings
    "PKE":        ("Defense",                       "defense"),              # Park Electrochemical — composite radomes/fairings for F-35, military aircraft, missiles
    "IPX":        ("Defense",                       "defense"),              # IperionX — low-carbon titanium powders for aerospace/defense/space (DoD-linked)
    "AIR":        ("Defense",                       "defense"),              # AAR Corp — aviation MRO for USAF/USN (~50% revenue from US government defense)

    # Nuclear / Grid
    "ASPI":       ("Nuclear / Grid",                "uranium_nuclear"),       # ASP Isotopes — laser isotope enrichment for enriched uranium / nuclear fuel

    # Cybersecurity
    "LAES":       ("Cybersecurity",                 "cybersecurity"),        # SEALSQ Corp — post-quantum secure RISC-V microchips for IoT/automotive (WISeKey spinoff)
    "AKAM":       ("Cybersecurity",                 "cybersecurity"),        # Akamai — CDN / edge security / bot protection (classified as "Software-Infrastructure" in screeners but core business is cybersecurity)

    # Semiconductor Equipment
    "TRT":        ("Semiconductor Equipment",       "semicap_equipment"),    # Trio-Tech International — semiconductor burn-in/test services for memory & logic
    "AIM:TRT":    ("Semiconductor Equipment",       "semicap_equipment"),    # Trio-Tech International (AIM listing) — same company, overrides wrong "Auto Parts" CSV industry

    # Power / Cooling
    "SEI":        ("Power / Cooling",               "power_cooling"),      # Solaris Energy Infrastructure — mobile power generation for AI data centers & industrial
    "AIM:VLX":    ("Power / Cooling",               "power_cooling"),      # Volex (AIM) — power/data cables & interconnects for data centers & industrial

    # Quantum Computing
    "INFQ":       ("Quantum Computing",             "quantum"),              # Infleqtion (formerly ColdQuanta) — quantum computing & sensing, Sqynet quantum network

    # Semi Materials
    "TSX:VNP":    ("Semi Materials",                "semiconductors"),       # 5N Plus Inc — specialty compound semiconductor materials (GaAs, Ge, Bi) for space solar & defense

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

    # AI Networking
    "AEVA":       ("AI Networking",                 "ai_networking"),        # Aeva Technologies — FMCW LiDAR perception chips for autonomous vehicles / robotics (classified as "Software-Infrastructure" in screeners)
    "AIM:FTC":    ("AI Networking",                 "ai_networking"),        # FastForward Innovations (AIM) — AI connectivity & communications portfolio company

    # Semiconductors
    "ADEA":       ("Semiconductors",                "semiconductors"),       # Adeia — semiconductor IP licensing (memory/storage patents); core value is semiconductor IP
    "AIM:ENSI":   ("Semiconductors",                "semiconductors"),       # Ensurge Micropower (AIM) — ultra-thin embedded semiconductor solutions

    # Clean Energy
    "AMSC":       ("Clean Energy",                  "clean_energy"),         # American Superconductor — power electronics & grid-tied systems for wind turbines & grid stability
    "AMRC":       ("Clean Energy",                  "clean_energy"),         # Ameresco — energy efficiency & renewable energy project developer/owner-operator

    # Lithium & Battery Tech
    "AMPX":       ("Lithium & Battery Tech",        "lithium_battery"),      # Amprius Technologies — silicon anode lithium battery cells (EV/aerospace/defense)
}

# ── Index dicts (built once at import time, cheap in-memory) ─────────────────

_TICKER_TO_THEMES:        dict[str, list[str]] = {}   # "ANET" → ["AI Networking"]
_TICKER_TO_THEME_ID:      dict[str, str]       = {}   # "SMH"  → "semiconductors"
_TICKER_TO_CLASSIFICATION:dict[str, str]       = {}   # "SMH"  → "sub_theme"
_TICKER_TO_PARENT_SECTOR: dict[str, str]       = {}   # "SMH"  → "technology"
_TICKER_TO_PRIMARY_SOURCE: dict[str, str]      = {}   # "ACOG" → "llm_file_override" (origin of the FIRST/primary theme entry only)

_ETF_TO_THEME_ID:  dict[str, str] = {}   # "SMH"  → "semiconductors"
_ETF_TO_LABEL:     dict[str, str] = {}   # "SMH"  → "Semiconductors"
_THEME_META:       dict[str, dict] = {}  # theme_name → {etfs, reps, parent_sector, classification, theme_id}

_built = False

# ── LLM-classified overrides persistence ─────────────────────────────────────
from pathlib import Path as _Path

_LLM_OVERRIDES_PATH = _Path(__file__).parent.parent / "data" / "llm_theme_overrides.json"


def _load_llm_overrides() -> dict[str, dict]:
    """Load {TICKER: {display_name, theme_id, confidence}} from disk. Never raises."""
    try:
        if _LLM_OVERRIDES_PATH.exists():
            import json as _j
            return _j.loads(_LLM_OVERRIDES_PATH.read_text()) or {}
    except Exception as _e:
        print(f"[THEME_MAPPER] LLM overrides load error: {_e}")
    return {}


def _save_llm_overrides(overrides: dict[str, dict]) -> None:
    """Persist overrides dict to disk. Never raises."""
    try:
        import json as _j
        _LLM_OVERRIDES_PATH.parent.mkdir(parents=True, exist_ok=True)
        _LLM_OVERRIDES_PATH.write_text(_j.dumps(overrides, indent=2))
    except Exception as _e:
        print(f"[THEME_MAPPER] LLM overrides save error: {_e}")


def register_llm_classified_tickers(classifications: list[dict]) -> int:
    """
    Register LLM-classified {ticker, theme, confidence} entries into the live
    in-memory index AND persist them to data/llm_theme_overrides.json so they
    survive server restarts.

    Returns the number of tickers successfully registered.
    """
    _build_index()   # ensure index is populated first

    overrides = _load_llm_overrides()
    registered = 0

    for item in classifications:
        sym        = (item.get("ticker") or "").upper().strip()
        display    = (item.get("theme") or "").strip()
        confidence = item.get("confidence") or "medium"
        if not sym or not display:
            continue

        # Derive theme_id from display name or look it up from _THEME_META
        meta      = _THEME_META.get(display, {})
        theme_id  = meta.get("theme_id") or display.lower().replace(" ", "_").replace("/", "_")

        # ── Update in-memory index (immediate effect on next terminal load) ──
        _TICKER_TO_THEMES.setdefault(sym, [])
        if display not in _TICKER_TO_THEMES[sym]:
            _TICKER_TO_THEMES[sym].insert(0, display)   # highest priority

        _TICKER_TO_THEME_ID[sym]       = theme_id
        _TICKER_TO_CLASSIFICATION[sym] = "sub_theme"
        _TICKER_TO_PRIMARY_SOURCE[sym] = "llm_file_override"

        # ── Update disk overrides (survives restarts) ─────────────────────────
        overrides[sym] = {
            "display_name": display,
            "theme_id":     theme_id,
            "confidence":   confidence,
        }
        registered += 1
        print(f"[THEME_MAPPER] LLM override registered: {sym} → {display} ({confidence})")

    _save_llm_overrides(overrides)
    print(f"[THEME_MAPPER] {registered} LLM overrides saved to {_LLM_OVERRIDES_PATH}")
    return registered


def remove_llm_theme_override(ticker: str, only_if_theme_id: Optional[str] = None) -> dict:
    """
    Guarded delete of a single ticker's row from data/llm_theme_overrides.json
    (the file backing "Source -1" in _build_index / register_llm_classified_tickers).

    If only_if_theme_id is given, the row is only removed when its stored
    theme_id still matches — this protects a newer reassignment: if the ticker
    was reassigned to a different theme after the caller captured the "old"
    theme_id, the row now belongs to the new theme and must NOT be deleted here.

    Also reverses the in-memory index effect for this ticker/theme so the
    change is visible without a full process restart (only removes the display
    name this override contributed to _TICKER_TO_THEMES; does not touch other
    themes indexed for the same ticker from unrelated sources).

    Never raises. Returns:
        {"deleted": bool, "reason": str, "ticker": str, "prior_row": dict|None}
    """
    sym = (ticker or "").strip().upper()
    if not sym:
        return {"deleted": False, "reason": "empty_ticker", "ticker": ticker, "prior_row": None}

    overrides = _load_llm_overrides()
    row = overrides.get(sym)
    if row is None:
        return {"deleted": False, "reason": "no_row", "ticker": sym, "prior_row": None}

    if only_if_theme_id is not None and row.get("theme_id") != only_if_theme_id:
        return {
            "deleted": False,
            "reason": "theme_id_mismatch_newer_reassignment",
            "ticker": sym,
            "prior_row": row,
        }

    del overrides[sym]
    _save_llm_overrides(overrides)

    # Reverse the in-memory effect (best-effort; index may not be built yet).
    try:
        display = row.get("display_name")
        if sym in _TICKER_TO_THEMES and display in _TICKER_TO_THEMES[sym]:
            _TICKER_TO_THEMES[sym].remove(display)
            if not _TICKER_TO_THEMES[sym]:
                del _TICKER_TO_THEMES[sym]
        if _TICKER_TO_PRIMARY_SOURCE.get(sym) == "llm_file_override":
            del _TICKER_TO_PRIMARY_SOURCE[sym]
        if _TICKER_TO_THEME_ID.get(sym) == row.get("theme_id"):
            del _TICKER_TO_THEME_ID[sym]
        if _TICKER_TO_CLASSIFICATION.get(sym) == "sub_theme" and sym not in _TICKER_TO_THEMES:
            del _TICKER_TO_CLASSIFICATION[sym]
    except Exception as _e:
        print(f"[THEME_MAPPER] remove_llm_theme_override in-memory reversal error (non-fatal): {_e}")

    print(f"[THEME_MAPPER] LLM override removed: {sym} (was {row.get('display_name')})")
    return {"deleted": True, "reason": "removed", "ticker": sym, "prior_row": row}


def map_ticker_to_primary_theme_source(ticker: str) -> Optional[str]:
    """
    Return the origin of ticker's PRIMARY (first) theme entry, one of:
    "llm_file_override", "foreign_alias_map", "canonical_map", or None if
    the ticker has no mapper entry at all.

    Used by theme_resolver.resolve_primary_theme_for_ticker() so provenance
    correctly distinguishes a manual/LLM file override from a genuine static
    canonical-map hit, without changing which theme is returned.
    """
    _build_index()
    return _TICKER_TO_PRIMARY_SOURCE.get((ticker or "").upper())


def _build_index() -> None:
    global _built
    if _built:
        return
    _built = True

    # ── Source -1: LLM-classified overrides (highest priority — user-triggered) ─
    llm_overrides = _load_llm_overrides()
    for raw_sym, meta in llm_overrides.items():
        s          = raw_sym.upper()
        display    = meta.get("display_name", "")
        theme_id   = meta.get("theme_id", display.lower().replace(" ", "_").replace("/", "_"))
        if not display:
            continue
        _TICKER_TO_THEMES.setdefault(s, [])
        if display not in _TICKER_TO_THEMES[s]:
            _TICKER_TO_THEMES[s].append(display)
        _TICKER_TO_THEME_ID[s]       = theme_id
        _TICKER_TO_CLASSIFICATION[s] = "sub_theme"
        _TICKER_TO_PRIMARY_SOURCE.setdefault(s, "llm_file_override")

    # ── Source 0: Foreign/OTC alias map (highest priority — explicit overrides) ─
    for raw_sym, (display, theme_id) in _FOREIGN_ALIAS_MAP.items():
        s = raw_sym.upper()
        _TICKER_TO_THEMES.setdefault(s, [])
        if display not in _TICKER_TO_THEMES[s]:
            _TICKER_TO_THEMES[s].append(display)
        _TICKER_TO_THEME_ID[s]       = theme_id
        _TICKER_TO_CLASSIFICATION[s] = "sub_theme"
        _TICKER_TO_PRIMARY_SOURCE.setdefault(s, "foreign_alias_map")

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
                _TICKER_TO_PRIMARY_SOURCE.setdefault(sym, "canonical_map")

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
                _TICKER_TO_PRIMARY_SOURCE.setdefault(sym, "canonical_map")

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
                _TICKER_TO_PRIMARY_SOURCE.setdefault(s, "canonical_map")
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
                _TICKER_TO_PRIMARY_SOURCE.setdefault(etf, "canonical_map")

            # Representative tickers → theme (fallback only)
            for rep in reps:
                _TICKER_TO_THEMES.setdefault(rep, [])
                if label not in _TICKER_TO_THEMES[rep]:
                    _TICKER_TO_THEMES[rep].append(label)
                _TICKER_TO_THEME_ID.setdefault(rep, theme_id)
                _TICKER_TO_CLASSIFICATION.setdefault(rep, "theme")
                if parent:
                    _TICKER_TO_PARENT_SECTOR.setdefault(rep, parent)
                _TICKER_TO_PRIMARY_SOURCE.setdefault(rep, "canonical_map")

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

# ── Industry → Canonical Theme mapping ───────────────────────────────────────
# Maps CSV "Industry" column values to (display_name, theme_id) pairs.
# Used as a deterministic fallback when a ticker is not in any of the three
# primary sources (THEME_RS_UNIVERSE, THEME_MAP, THEME_ETF_UNIVERSE).
# Keys are exact CSV industry strings (case-sensitive as they appear in FMP/Finviz CSV exports).
# Only high-confidence mappings are included — when in doubt, leave unlisted (returns None).
INDUSTRY_TO_THEME: dict[str, tuple[str, str]] = {
    # ── Semiconductors ──────────────────────────────────────────────────────
    "Semiconductors":                                       ("Semiconductors",             "semiconductors"),
    "Semiconductors and Related Devices":                   ("Semiconductors",             "semiconductors"),
    "Electronic Components":                                ("Semiconductors",             "semiconductors"),
    "Electronic Components, not elsewhere classified":      ("Semiconductors",             "semiconductors"),
    "Semiconductor Memory":                                 ("Memory & Storage",           "memory_storage"),

    # ── Semiconductor Equipment ─────────────────────────────────────────────
    "Semiconductor Equipment & Materials":                  ("Semiconductor Equipment",    "semicap_equipment"),
    "Scientific & Technical Instruments":                   ("Semiconductor Equipment",    "semicap_equipment"),

    # ── AI Networking / Communication ───────────────────────────────────────
    "Communication Equipment":                              ("AI Networking",              "ai_networking"),
    "Telephone and Telegraph Apparatus":                    ("AI Networking",              "ai_networking"),
    "Telecom Services":                                     ("AI Networking",              "ai_networking"),

    # ── Data Center / Cloud ─────────────────────────────────────────────────
    "Computer Hardware":                                    ("Data Center Infrastructure", "datacenter_infra"),
    "Information Technology Services":                      ("Data Center Infrastructure", "datacenter_infra"),
    "Software - Infrastructure":                            ("Data Center Infrastructure", "datacenter_infra"),
    "REIT - Specialty":                                     ("Data Center Infrastructure", "datacenter_infra"),

    # ── Cloud Software ───────────────────────────────────────────────────────
    "Software - Application":                               ("Cloud Software",             "cloud_software"),
    "Specialty Business Services":                          ("Cloud Software",             "cloud_software"),

    # ── Defense / Aerospace ─────────────────────────────────────────────────
    "Aerospace & Defense":                                  ("Defense",                    "defense"),

    # ── Clean Energy ────────────────────────────────────────────────────────
    "Solar":                                                ("Clean Energy",               "clean_energy"),
    "Utilities - Renewable":                                ("Clean Energy",               "clean_energy"),
    "Utilities - Independent Power Producers":              ("Clean Energy",               "clean_energy"),
    "Pollution & Treatment Controls":                       ("Clean Energy",               "clean_energy"),

    # ── Power / Cooling ─────────────────────────────────────────────────────
    "Electrical Equipment & Parts":                         ("Power / Cooling",            "power_cooling"),
    "Utilities - Regulated Electric":                       ("Nuclear / Grid",             "uranium_nuclear"),

    # ── Lithium & Battery Tech ──────────────────────────────────────────────
    # (covered by explicit _FOREIGN_ALIAS_MAP entries for specific tickers)

    # ── Uranium & Nuclear ────────────────────────────────────────────────────
    "Uranium":                                              ("Uranium & Nuclear Energy",   "uranium_nuclear"),

    # ── Rare Earth / Metals ───────────────────────────────────────────────────
    "Other Industrial Metals & Mining":                     ("Rare Earth Metals",          "rare_earth"),
    "Other Precious Metals & Mining":                       ("Rare Earth Metals",          "rare_earth"),

    # ── Semi Materials ────────────────────────────────────────────────────────
    "Specialty Chemicals":                                  ("Semi Materials",             "semiconductors"),

    # ── Industrials ─────────────────────────────────────────────────────────
    "Specialty Industrial Machinery":                       ("Industrials",                "industrials"),
    "Engineering & Construction":                           ("Industrials",                "industrials"),
    "Auto Parts":                                           ("Industrials",                "industrials"),
    "Conglomerates":                                        ("Industrials",                "industrials"),
    "Oil & Gas Exploration & Production":                   ("Industrials",                "industrials"),
    "Oil & Gas Drilling":                                   ("Industrials",                "industrials"),
    "Thermal Coal":                                         ("Industrials",                "industrials"),
    "Industrial and Commercial Fans and Blowers and Air Purification Equipment": (
                                                             "Industrials",                "industrials"),
    # ── Intentionally left unmapped (return None) ────────────────────────────
    # "Capital Markets", "Asset Management", "Agricultural Inputs" — no clear fit
}


def map_industry_to_theme(industry: str) -> tuple[str, str] | None:
    """
    Return (display_name, theme_id) for a CSV Industry string, or None if not mapped.

    Used as a deterministic fallback classifier when a ticker is absent from all
    primary theme registries.  Never raises.
    """
    if not industry:
        return None
    return INDUSTRY_TO_THEME.get(industry.strip())


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
