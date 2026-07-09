"""
theme_resolver.py — the ONE canonical "what primary Theme is this ticker
currently assigned to?" resolver.

Extracted verbatim (behavior-preserving) from the Watchlist skeleton-path
Theme resolution logic in watchlist_router.py (_enrich_store_with_quotes,
the `if not sections:` skeleton branch). This module does not introduce any
new classification logic — it is a pure, read-only extraction so the
Watchlist UI and the Confluence Theme bridge consume the exact same
resolution result instead of two independently-maintained copies.

Resolution order (unchanged from the original inline Watchlist logic):
  1. theme_ticker_mapper.map_ticker_to_primary_theme  (+ map_ticker_to_theme_id)
  2. map_industry_to_theme (CSV Industry fallback)      — only if (1) empty
  3. ENRICHED_THEME_RS_UNIVERSE membership (Themes-page) — overrides (1)/(2)
     only when it resolves to a *different* display name (never downgrades
     a specific canonical mapping to a generic sector)
  4. category_overrides manual override                  — always wins

Zero provider calls. Zero LLM calls. Zero writes. Cache/LKG-read only.
"""
from __future__ import annotations

from typing import Optional, TypedDict


class ThemeResolutionContext(TypedDict):
    themes_page_map: dict[str, str]       # symbol -> display_name
    themes_page_id_map: dict[str, str]    # symbol -> theme_id
    cat_overrides: dict[str, str]         # symbol -> manual display_name


def build_theme_resolution_context() -> ThemeResolutionContext:
    """
    Pre-load the shared, expensive-to-rebuild lookups once per batch
    (Themes-page membership index + manual category overrides), so callers
    resolving many tickers (Watchlist skeleton pass, Confluence bridge)
    don't rebuild them per-ticker. Cheap to rebuild (51 themes, dict reads).
    """
    themes_page_map: dict[str, str] = {}
    themes_page_id_map: dict[str, str] = {}
    try:
        from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _etrs
        for _stid, _stmeta in _etrs.items():
            _sdn = _stmeta.get("display_name", "")
            for _ssym in _stmeta.get("proxy_symbols", []):
                if _ssym not in themes_page_map:
                    themes_page_map[_ssym] = _sdn
                    themes_page_id_map[_ssym] = _stid
    except Exception:
        pass

    cat_overrides: dict[str, str] = {}
    try:
        from services.category_overrides import get_overrides as _get_overrides
        cat_overrides = _get_overrides("default")
    except Exception:
        pass

    return {
        "themes_page_map": themes_page_map,
        "themes_page_id_map": themes_page_id_map,
        "cat_overrides": cat_overrides,
    }


class ThemeResolution(TypedDict):
    theme_name: Optional[str]
    theme_id: Optional[str]
    source: Optional[str]   # canonical_map | industry_fallback | themes_page_membership | manual_override | no_mapping


def resolve_primary_theme_for_ticker(
    ticker: str,
    industry: Optional[str] = None,
    ctx: Optional[ThemeResolutionContext] = None,
) -> ThemeResolution:
    """
    Resolve the single canonical primary Theme identity for `ticker`,
    exactly matching the Watchlist UI's skeleton-path resolution.

    `industry` is the CSV "Industry" column value for this ticker, if known
    (used only as a fallback when the ticker has no canonical_map hit).
    `ctx` should be built once via build_theme_resolution_context() and
    reused across a batch; if omitted, it is built fresh for this one call.

    Never raises. Returns {"theme_name": None, "theme_id": None,
    "source": "no_mapping"} when nothing resolves.
    """
    if ctx is None:
        ctx = build_theme_resolution_context()

    s = (ticker or "").strip().upper()

    canon_theme: Optional[str] = None
    canon_theme_id: Optional[str] = None
    theme_src: Optional[str] = None

    try:
        from services.theme_ticker_mapper import (
            map_ticker_to_primary_theme as _theme_fn,
            map_ticker_to_theme_id as _theme_id_fn,
            map_industry_to_theme as _ind_fn,
        )
    except ImportError:
        _theme_fn = _theme_id_fn = _ind_fn = None  # type: ignore

    if _theme_fn:
        canon_theme = _theme_fn(s)
        canon_theme_id = _theme_id_fn(s) if _theme_id_fn else None
        if canon_theme:
            theme_src = "canonical_map"

    if not canon_theme and _ind_fn and industry:
        ind = (industry or "").strip()
        if ind:
            ind_result = _ind_fn(ind)
            if ind_result:
                canon_theme, canon_theme_id = ind_result
                theme_src = "industry_fallback"

    if not canon_theme and _theme_fn:
        theme_src = "no_mapping"

    themes_page_map = ctx.get("themes_page_map", {})
    if s in themes_page_map:
        tp_name = themes_page_map[s]
        tp_id = ctx.get("themes_page_id_map", {}).get(s)
        if tp_name and tp_name != canon_theme:
            canon_theme = tp_name
            canon_theme_id = tp_id
            theme_src = "themes_page_membership"

    manual_cat = ctx.get("cat_overrides", {}).get(s)
    if manual_cat:
        canon_theme = manual_cat
        theme_src = "manual_override"
        if not canon_theme_id or theme_src == "manual_override":
            try:
                from services.theme_rs_universe import THEME_RS_UNIVERSE as _trs
                canon_theme_id = next(
                    (tid for tid, m in _trs.items()
                     if m.get("display_name", "").lower() == manual_cat.lower()),
                    canon_theme_id,
                )
            except Exception:
                pass

    return {
        "theme_name": canon_theme,
        "theme_id": canon_theme_id,
        "source": theme_src,
    }
