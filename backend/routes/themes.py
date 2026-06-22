"""
Themes by Relative Strength — HTTP endpoints.

GET  /api/themes/relative-strength?timeframe=1D|7D|30D|YTD|1Y|5Y[&classification=all|sector|theme|sub_theme]
GET  /api/themes/relative-strength/refresh  (force-refresh cache, same params)
GET  /api/themes/list                        (static theme registry)
GET  /api/themes/merge-debug                 (dev/admin diagnostic)

Admin (dev-only, X-API-Key required):
GET    /api/themes/admin/memberships                        list all overrides
POST   /api/themes/admin/memberships                        add/remove a single ticker
POST   /api/themes/admin/memberships/bulk                   bulk add/remove
DELETE /api/themes/admin/memberships/{theme_id}/{symbol}    clear an override
GET    /api/themes/admin/theme-basket/{theme_id}            basket breakdown
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query, HTTPException, Request, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator

from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as THEME_RS_UNIVERSE

router = APIRouter(prefix="/api/themes", tags=["themes"])

_VALID_TIMEFRAMES      = {"1D", "7D", "30D", "YTD", "1Y", "5Y"}
_VALID_CLASSIFICATIONS = {"all", "sector", "theme", "sub_theme"}
_VALID_ACTIONS         = {"add", "remove"}

_SYM_RE = __import__("re").compile(r"^[A-Z0-9\.\-]{1,12}$")


# ── Admin guard ────────────────────────────────────────────────────────────────

def _check_admin_key(api_key: Optional[str]) -> Optional[JSONResponse]:
    """
    Mirror the existing _check_admin_key pattern from screener_hub.py.
    Uses AGENT_API_KEY from config — same key that protects screener admin routes.
    """
    try:
        from config import AGENT_API_KEY
    except Exception:
        AGENT_API_KEY = None
    if AGENT_API_KEY and api_key != AGENT_API_KEY:
        return JSONResponse(
            status_code=401,
            content={"error": "Invalid or missing API key — X-API-Key header required"},
        )
    return None


def _invalidate_caches() -> None:
    """Rebuild in-memory enriched universe + clear RS caches after an override write."""
    try:
        from services.theme_merge_layer import refresh_enriched_universe
        refresh_enriched_universe()
    except Exception as exc:
        print(f"[THEMES_ADMIN] refresh_enriched_universe error: {exc}")
    try:
        from services.theme_rs_service import invalidate_theme_rs_cache
        invalidate_theme_rs_cache()
    except Exception as exc:
        print(f"[THEMES_ADMIN] invalidate_theme_rs_cache error: {exc}")


# ── Public read endpoints ──────────────────────────────────────────────────────

@router.get("/relative-strength")
async def themes_relative_strength(
    timeframe: str = Query(
        "30D",
        description="Return timeframe: 1D | 7D | 30D | YTD | 1Y | 5Y",
    ),
    classification: str = Query(
        "all",
        description="Filter by classification: all | sector | theme | sub_theme",
    ),
):
    """
    Sorted list of themes/sectors by relative strength score.

    Each item includes:
      theme_id, display_name, classification, parent_sector, sector_tags,
      proxy_type, proxy_symbols, proxy_symbols_used,
      return_pct, performance (1D/7D/30D/YTD/1Y),
      rs_score, rs_vs_spy, rs_vs_qqq,
      state (active/emerging/neutral/weakening/dead_zone), state_reason,
      leaders, laggards, breadth_pct, momentum_rank, last_updated.

    classification filter:
      all       → all 60 rows (11 SPDR sectors + 49 themes/sub-themes) [default]
      sector    → exactly 11 SPDR broad sector rows
      theme     → broad cross-sector/factor theme rows
      sub_theme → narrow industry sub-theme rows

    No LLM calls. 1D cached 60s market hours / 3600s off-hours.
    7D/30D/YTD/1Y cached 900s market hours / 3600s off-hours.
    LKG persisted to disk — page loads work after restart.
    """
    tf  = timeframe.upper()
    clf = classification.lower()

    if tf not in _VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{timeframe}'. Must be one of: {sorted(_VALID_TIMEFRAMES)}",
        )
    if clf not in _VALID_CLASSIFICATIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid classification '{classification}'. Must be one of: {sorted(_VALID_CLASSIFICATIONS)}",
        )
    try:
        from services.theme_rs_service import get_theme_rs_data
        return await get_theme_rs_data(timeframe=tf, force=False, classification=clf)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Theme RS error: {e}")


@router.get("/relative-strength/refresh")
async def themes_relative_strength_refresh(
    timeframe: str = Query("30D", description="Timeframe to recompute: 1D | 7D | 30D | YTD | 1Y | 5Y"),
    classification: str = Query(
        "all",
        description="Filter by classification: all | sector | theme | sub_theme",
    ),
):
    """Force-refresh the theme RS cache (bypasses TTL). Same response shape."""
    tf  = timeframe.upper()
    clf = classification.lower()

    if tf not in _VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{timeframe}'. Must be one of: {sorted(_VALID_TIMEFRAMES)}",
        )
    if clf not in _VALID_CLASSIFICATIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid classification '{classification}'. Must be one of: {sorted(_VALID_CLASSIFICATIONS)}",
        )
    try:
        from services.theme_rs_service import get_theme_rs_data
        return await get_theme_rs_data(timeframe=tf, force=True, classification=clf)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Theme RS refresh error: {e}")


@router.get("/merge-debug")
async def themes_merge_debug():
    """
    Diagnostic: shows which themes were enriched with watchlist tickers and what was added.
    No price data. No user PII. Dev/admin use only.
    """
    from services.theme_merge_layer import get_merge_debug_info
    return get_merge_debug_info()


@router.get("/list")
async def themes_list(
    classification: str = Query(
        "all",
        description="Filter by classification: all | sector | theme | sub_theme",
    ),
):
    """Return the full static theme registry (no price data)."""
    clf = classification.lower()
    themes = []
    for theme_id, meta in THEME_RS_UNIVERSE.items():
        if clf != "all" and meta.get("classification") != clf:
            continue
        themes.append({
            "theme_id":            theme_id,
            "display_name":        meta["display_name"],
            "classification":      meta.get("classification", "theme"),
            "parent_sector":       meta.get("parent_sector"),
            "proxy_type":          meta["proxy_type"],
            "proxy_symbols":       meta["proxy_symbols"],
            "candidate_symbols":   meta.get("candidate_symbols", []),
            "sector_tags":         meta.get("sector_tags", []),
            "keywords":            meta.get("keywords", []),
            "macro_sensitivities": meta.get("macro_sensitivities", []),
        })
    return {
        "themes":                themes,
        "theme_count":           len(themes),
        "classification_filter": clf,
    }


# ── Admin / dev-only endpoints ─────────────────────────────────────────────────
#
# Protected by X-API-Key matching AGENT_API_KEY (same as screener admin routes).
# Many-to-many: PK is (theme_id, symbol). The same symbol can belong to multiple
# themes simultaneously. Removing it from one theme does not touch any other theme.
#
# Apply order in the enriched universe:
#   1. Base universe  →  2. Watchlist seeds  →  3. Manual overrides  (wins)


class MembershipEdit(BaseModel):
    theme_id: str
    symbol:   str
    action:   str          # "add" | "remove"
    note:     Optional[str] = None
    created_by: Optional[str] = "admin"

    @field_validator("theme_id")
    @classmethod
    def validate_theme_id(cls, v: str) -> str:
        v = v.strip().lower()
        if not v:
            raise ValueError("theme_id must not be empty")
        return v

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        v = v.strip().upper()
        if not _SYM_RE.match(v):
            raise ValueError(f"Invalid symbol format: '{v}'")
        return v

    @field_validator("action")
    @classmethod
    def validate_action(cls, v: str) -> str:
        v = v.strip().lower()
        if v not in _VALID_ACTIONS:
            raise ValueError(f"action must be 'add' or 'remove', got '{v}'")
        return v


class BulkMembershipBody(BaseModel):
    edits: list[MembershipEdit]


@router.get("/admin/memberships")
async def admin_list_memberships(
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
    theme_id: Optional[str] = Query(None, description="Filter by theme_id"),
):
    """
    [Admin] List all manual theme-ticker overrides.
    Includes theme_id, display_name, symbol, action, source, note, timestamps.
    Optionally filter by ?theme_id=<id>.
    """
    err = _check_admin_key(x_api_key)
    if err:
        return err

    try:
        from data.pg_storage import get_theme_ticker_overrides
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Storage unavailable: {exc}")

    rows = get_theme_ticker_overrides(theme_id=theme_id)

    # Enrich with display_name from enriched universe
    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _uni
    for row in rows:
        meta = _uni.get(row["theme_id"], {})
        row["display_name"] = meta.get("display_name", row["theme_id"])

    return {
        "overrides":    rows,
        "override_count": len(rows),
        "filter_theme_id": theme_id,
    }


@router.post("/admin/memberships")
async def admin_upsert_membership(
    body: MembershipEdit,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    """
    [Admin] Add or remove a ticker from a theme basket.

    action='add':    Force-include symbol in theme_id basket. Does NOT remove it from
                     any other theme.
    action='remove': Force-exclude symbol from theme_id basket only. Does NOT remove it
                     from any other theme.

    Upserts on (theme_id, symbol) — sending the same pair again updates the action.
    Rebuilds the enriched universe and invalidates RS cache immediately.
    """
    err = _check_admin_key(x_api_key)
    if err:
        return err

    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _uni
    if body.theme_id not in _uni:
        raise HTTPException(
            status_code=404,
            detail=f"theme_id '{body.theme_id}' not found in universe. "
                   f"Valid ids: {sorted(_uni.keys())}",
        )

    try:
        from data.pg_storage import upsert_theme_ticker_override
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Storage unavailable: {exc}")

    ok = upsert_theme_ticker_override(
        theme_id=body.theme_id,
        symbol=body.symbol,
        action=body.action,
        source="manual_admin",
        note=body.note,
        created_by=body.created_by,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to write override to Neon")

    _invalidate_caches()

    return {
        "ok":       True,
        "theme_id": body.theme_id,
        "symbol":   body.symbol,
        "action":   body.action,
        "note":     body.note,
        "message":  f"Override saved. '{body.symbol}' {body.action}ed in '{body.theme_id}' only. "
                    f"Universe rebuilt and RS cache cleared.",
    }


@router.post("/admin/memberships/bulk")
async def admin_bulk_memberships(
    body: BulkMembershipBody,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    """
    [Admin] Bulk add/remove tickers across themes.

    Supports adding the same symbol to multiple themes (many-to-many).
    Each edit is independent — removing from one theme does not affect other themes.

    Example:
      {"edits": [
        {"theme_id": "ai_networking",  "symbol": "ANET", "action": "add"},
        {"theme_id": "semiconductors", "symbol": "ANET", "action": "add"},
        {"theme_id": "clean_energy",   "symbol": "RUN",  "action": "remove"}
      ]}
    """
    err = _check_admin_key(x_api_key)
    if err:
        return err

    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _uni
    unknown_themes = [e.theme_id for e in body.edits if e.theme_id not in _uni]
    if unknown_themes:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown theme_id(s): {unknown_themes}",
        )

    try:
        from data.pg_storage import bulk_upsert_theme_ticker_overrides
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Storage unavailable: {exc}")

    edits_dicts = [
        {
            "theme_id":   e.theme_id,
            "symbol":     e.symbol,
            "action":     e.action,
            "source":     "manual_admin",
            "note":       e.note,
            "created_by": e.created_by,
        }
        for e in body.edits
    ]
    result = bulk_upsert_theme_ticker_overrides(edits_dicts)

    if result["succeeded"] > 0:
        _invalidate_caches()

    return {
        "ok":        result["failed"] == 0,
        "succeeded": result["succeeded"],
        "failed":    result["failed"],
        "errors":    result["errors"],
        "message":   f"{result['succeeded']} override(s) saved. Universe rebuilt and RS cache cleared.",
    }


@router.delete("/admin/memberships/{theme_id}/{symbol}")
async def admin_delete_membership(
    theme_id: str,
    symbol: str,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    """
    [Admin] Clear the manual override for (theme_id, symbol).
    Restores default universe behavior for that pair only.
    Does NOT remove the symbol from any other theme.
    """
    err = _check_admin_key(x_api_key)
    if err:
        return err

    theme_id = theme_id.strip().lower()
    symbol   = symbol.strip().upper()

    try:
        from data.pg_storage import delete_theme_ticker_override
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Storage unavailable: {exc}")

    deleted = delete_theme_ticker_override(theme_id=theme_id, symbol=symbol)

    if deleted:
        _invalidate_caches()

    return {
        "ok":       True,
        "deleted":  deleted,
        "theme_id": theme_id,
        "symbol":   symbol,
        "message": (
            f"Override for '{symbol}' in '{theme_id}' cleared. Default universe behavior restored."
            if deleted
            else f"No override found for '{symbol}' in '{theme_id}' — nothing changed."
        ),
    }


@router.get("/admin/theme-basket/{theme_id}")
async def admin_theme_basket(
    theme_id: str,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    """
    [Admin] Return a full source breakdown of a theme's basket.
    Shows exactly which symbols came from base universe, watchlist seeds,
    manual overrides, and which are manually excluded.
    Also shows final_theme_holdings and final_performance_symbols.
    """
    err = _check_admin_key(x_api_key)
    if err:
        return err

    theme_id = theme_id.strip().lower()

    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _uni
    if theme_id not in _uni:
        raise HTTPException(
            status_code=404,
            detail=f"theme_id '{theme_id}' not found. Valid ids: {sorted(_uni.keys())}",
        )

    try:
        from services.theme_rs_universe import THEME_RS_UNIVERSE as _base
    except Exception:
        _base = {}

    meta      = _uni[theme_id]
    base_meta = _base.get(theme_id, {})

    base_proxy      = sorted(base_meta.get("proxy_symbols", []))
    watchlist_seeds = sorted(meta.get("watchlist_seeds", []))
    manual_added    = sorted(meta.get("manual_added_symbols", []))
    manual_removed  = sorted(meta.get("manual_removed_symbols", []))
    final_proxy     = sorted(meta.get("proxy_symbols", []))
    holdings_mode   = meta.get("holdings_display_mode", "etf_holdings")

    return {
        "theme_id":                theme_id,
        "display_name":            meta.get("display_name", ""),
        "proxy_type":              meta.get("proxy_type", ""),
        "representative_symbol":   meta.get("representative_symbol", ""),
        "holdings_display_mode":   holdings_mode,
        # ── Source breakdown ────────────────────────────────────────────────
        "base_symbols":            base_proxy,
        "watchlist_seed_symbols":  watchlist_seeds,
        "manual_added_symbols":    manual_added,
        "manual_removed_symbols":  manual_removed,
        # ── Final state ─────────────────────────────────────────────────────
        # final_theme_holdings = what the expanded holdings table shows
        "final_theme_holdings": sorted(final_proxy) if holdings_mode == "theme_basket" else [],
        # final_performance_symbols = what _compute_theme_perf uses
        "final_performance_symbols": final_proxy,
    }
