"""
theme_ticker_mapper.py — ticker → theme mapping helper.

Combines existing theme maps in priority order (no new taxonomy created):
  1. home_service.THEME_MAP           (ticker → theme name, curated)
  2. THEME_ETF_UNIVERSE.symbols       (ETF proxy → theme id/label)
  3. THEME_ETF_UNIVERSE.representative_tickers (key stock → theme id/label)

All lookups are case-insensitive and O(1) via pre-built index.
Never raises — returns empty dict / "unknown" on missing tickers.

Reused by:
  - thematic_context_provider._enrich_from_x_consensus()
  - options screener theme_alignment annotation
  - strategy screener regime_alignment annotation
"""
from __future__ import annotations

from typing import Optional

# ── Build index once at import time (cheap, in-memory) ───────────────────────

_TICKER_TO_THEMES: dict[str, list[str]] = {}   # "ANET" → ["AI Networking"]
_ETF_TO_THEME_ID:  dict[str, str]       = {}   # "SMH"  → "semiconductors"
_ETF_TO_LABEL:     dict[str, str]       = {}   # "SMH"  → "Semiconductors"
_THEME_META:       dict[str, dict]      = {}   # theme_name → {etfs, reps, parent_sector}

_built = False


def _build_index() -> None:
    global _built
    if _built:
        return
    _built = True

    # ── Source 1: home_service.THEME_MAP (ticker → theme name) ───────────────
    try:
        from services.home_service import THEME_MAP
        for theme_name, tickers in THEME_MAP.items():
            etfs_for_theme: list[str] = []
            reps_for_theme: list[str] = []
            for sym in tickers:
                s = sym.upper()
                _TICKER_TO_THEMES.setdefault(s, [])
                if theme_name not in _TICKER_TO_THEMES[s]:
                    _TICKER_TO_THEMES[s].append(theme_name)
            _THEME_META.setdefault(theme_name, {
                "etfs": etfs_for_theme,
                "representative_tickers": reps_for_theme,
                "parent_sector": "Technology",
                "source": "home_theme_map",
            })
    except Exception as e:
        print(f"[THEME_MAPPER] home_service.THEME_MAP unavailable: {e}")

    # ── Source 2: THEME_ETF_UNIVERSE (ETF symbols + representative tickers) ──
    try:
        from services.sector_rotation.theme_universe import THEME_ETF_UNIVERSE
        for theme_id, meta in THEME_ETF_UNIVERSE.items():
            label = meta.get("label") or theme_id
            etfs  = [s.upper() for s in (meta.get("symbols") or [])]
            reps  = [s.upper() for s in (meta.get("representative_tickers") or [])]
            parent = meta.get("parent_sector", "")

            # ETF → theme mapping
            for etf in etfs:
                _ETF_TO_THEME_ID[etf]  = theme_id
                _ETF_TO_LABEL[etf]     = label
                _TICKER_TO_THEMES.setdefault(etf, [])
                if label not in _TICKER_TO_THEMES[etf]:
                    _TICKER_TO_THEMES[etf].append(label)

            # Representative tickers → theme
            for rep in reps:
                _TICKER_TO_THEMES.setdefault(rep, [])
                if label not in _TICKER_TO_THEMES[rep]:
                    _TICKER_TO_THEMES[rep].append(label)

            # Theme meta (merge / create)
            if label not in _THEME_META:
                _THEME_META[label] = {
                    "etfs": etfs,
                    "representative_tickers": reps,
                    "parent_sector": parent,
                    "source": "theme_etf_universe",
                }
            else:
                existing = _THEME_META[label]
                for e in etfs:
                    if e not in existing.get("etfs", []):
                        existing.setdefault("etfs", []).append(e)
    except Exception as e:
        print(f"[THEME_MAPPER] THEME_ETF_UNIVERSE unavailable: {e}")


# ── Public API ────────────────────────────────────────────────────────────────

def map_ticker_to_themes(ticker: str) -> list[str]:
    """Return list of theme names for a ticker (may be empty). Never raises."""
    _build_index()
    return list(_TICKER_TO_THEMES.get(ticker.upper(), []))


def map_ticker_to_primary_theme(ticker: str) -> Optional[str]:
    """Return the first (most specific) theme name for a ticker, or None."""
    themes = map_ticker_to_themes(ticker)
    return themes[0] if themes else None


def get_theme_meta(theme_name: str) -> dict:
    """Return {etfs, representative_tickers, parent_sector, source} for a theme name."""
    _build_index()
    return dict(_THEME_META.get(theme_name, {}))


def map_etf_to_theme_label(etf: str) -> Optional[str]:
    """Return the theme label for an ETF proxy ticker (e.g. SMH → 'Semiconductors')."""
    _build_index()
    return _ETF_TO_LABEL.get(etf.upper())


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
        "active":   "Regime-Aligned Active Theme",
        "emerging": "Regime-Aligned Emerging Theme",
        "dead_zone": "Dead Zone — below-average momentum",
        "neutral":  "Neutral — no strong theme signal",
    }

    return {
        "theme_name":             best_theme,
        "theme_state":            best_state,
        "regime_alignment_score": best_score,
        "regime_alignment_label": label_map.get(best_state, "Unknown"),
        "thematic_badges":        badges[:3],
        "dead_zone_warning":      dz_warn,
    }
