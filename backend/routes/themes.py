"""
Themes by Relative Strength — HTTP endpoints.

GET  /api/themes/relative-strength?timeframe=1D|7D|30D|YTD|1Y|5Y[&classification=all|sector|theme|sub_theme]
GET  /api/themes/relative-strength/refresh  (force-refresh cache, same params)
GET  /api/themes/list                        (static theme registry)
GET  /api/themes/merge-debug                 (dev/admin diagnostic)

Admin (dev-only, X-API-Key OR admin JWT required):
GET    /api/themes/admin/memberships                        list all ticker overrides
POST   /api/themes/admin/memberships                        add/remove a single ticker
POST   /api/themes/admin/memberships/bulk                   bulk add/remove
DELETE /api/themes/admin/memberships/{theme_id}/{symbol}    clear a ticker override
GET    /api/themes/admin/theme-basket/{theme_id}            basket breakdown + leader

GET    /api/themes/admin/leaders                            list all manual leaders
POST   /api/themes/admin/leaders                            set/update a theme leader
DELETE /api/themes/admin/leaders/{theme_id}                 clear a theme leader
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
# Dual-path: X-API-Key for scripts, Bearer JWT for browser frontend.
# Lives in auth.py so other routers can reuse it without circular imports.

def _check_admin(request: Request, api_key: Optional[str]) -> Optional[JSONResponse]:
    """
    Thin wrapper around auth.require_admin_user_or_api_key().
    Accepts EITHER:
      1. X-API-Key: <AGENT_API_KEY>   — scripts / backend validation (preserved)
      2. Authorization: Bearer <JWT>  — browser session where jwt.sub == AUTH_USERNAME
    Returns None if authorised, JSONResponse(401/403) otherwise.
    """
    from auth import require_admin_user_or_api_key
    return require_admin_user_or_api_key(request, api_key)


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
    try:
        from data.options_flow_sectors import invalidate_sectors_cache
        invalidate_sectors_cache()
    except Exception as exc:
        print(f"[THEMES_ADMIN] invalidate_sectors_cache error: {exc}")


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
      leaders, laggards, breadth_pct, momentum_rank, last_updated,
      leader_symbol, leader_source.

    classification filter:
      all       → all 60 rows (11 SPDR sectors + 49 themes/sub-themes) [default]
      sector    → exactly 11 SPDR broad sector rows
      theme     → broad cross-sector/factor theme rows
      sub_theme → narrow industry sub-theme rows
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


# ── Admin / dev-only endpoints — ticker overrides ─────────────────────────────
#
# Protected by X-API-Key matching AGENT_API_KEY OR valid admin Bearer JWT.
# Many-to-many: PK is (theme_id, symbol). Same symbol can belong to N themes.
#
# Apply order: base universe → watchlist seeds → manual overrides (manual wins)


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
    request: Request,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
    theme_id: Optional[str] = Query(None, description="Filter by theme_id"),
):
    """[Admin] List all manual theme-ticker overrides."""
    err = _check_admin(request, x_api_key)
    if err:
        return err

    try:
        from data.pg_storage import get_theme_ticker_overrides
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Storage unavailable: {exc}")

    rows = get_theme_ticker_overrides(theme_id=theme_id)

    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _uni
    for row in rows:
        meta = _uni.get(row["theme_id"], {})
        row["display_name"] = meta.get("display_name", row["theme_id"])

    return {
        "overrides":       rows,
        "override_count":  len(rows),
        "filter_theme_id": theme_id,
    }


@router.post("/admin/memberships")
async def admin_upsert_membership(
    request: Request,
    body: MembershipEdit,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    """
    [Admin] Add or remove a ticker from a theme basket.

    action='add':    Force-include symbol in theme_id basket only.
    action='remove': Force-exclude symbol from theme_id basket only.

    If action='remove' and the symbol is currently the manual leader for this theme,
    the leader override is also cleared automatically (returned in response).

    Upserts on (theme_id, symbol). Rebuilds enriched universe + invalidates RS cache.
    """
    err = _check_admin(request, x_api_key)
    if err:
        return err

    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _uni
    if body.theme_id not in _uni:
        raise HTTPException(
            status_code=404,
            detail=f"theme_id '{body.theme_id}' not found in universe.",
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

    leader_cleared = False
    if body.action == "remove":
        try:
            from data.pg_storage import get_theme_leaders_map, delete_theme_leader
            lmap = get_theme_leaders_map()
            current_leader = lmap.get(body.theme_id, {}).get("leader_symbol")
            if current_leader == body.symbol:
                delete_theme_leader(body.theme_id)
                leader_cleared = True
        except Exception as _le:
            print(f"[THEMES_ADMIN] leader auto-clear check error: {_le}")

    _invalidate_caches()

    # ── Cross-sync to Watchlist (category_overrides + theme_ticker_mapper) ────
    if body.action == "add":
        try:
            from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _eu_now
            _display = _eu_now.get(body.theme_id, {}).get("display_name") or body.theme_id
            from services.category_overrides import upsert_override as _upsert_cat
            _upsert_cat("default", body.symbol, _display, "themes_page_manual",
                        f"themes_page:{body.theme_id}")
            from services.theme_ticker_mapper import register_llm_classified_tickers as _xsync
            _xsync([{"ticker": body.symbol, "theme": _display, "confidence": "manual"}])
        except Exception as _xse:
            print(f"[THEMES_ADMIN] watchlist cross-sync failed (non-fatal): {_xse}")

    # ── Build authoritative basket from freshly-rebuilt enriched universe ─────
    # _invalidate_caches() called refresh_enriched_universe() which did an
    # in-place update of ENRICHED_THEME_RS_UNIVERSE.  Reading from it now
    # guarantees the response reflects the committed Neon row.
    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _eu_resp
    _resp_meta    = _eu_resp.get(body.theme_id, {})
    _final_basket = sorted(_resp_meta.get("proxy_symbols", []))
    _manual_added = sorted(_resp_meta.get("manual_added_symbols", []))
    _manual_removed = sorted(_resp_meta.get("manual_removed_symbols", []))

    return {
        "ok":                   True,
        "persisted":            True,
        "theme_id":             body.theme_id,
        "symbol":               body.symbol,
        "action":               body.action,
        "note":                 body.note,
        "leader_cleared":       leader_cleared,
        "member_count":         len(_final_basket),
        "theme_holdings":       _final_basket,
        "manual_added_symbols": _manual_added,
        "manual_removed_symbols": _manual_removed,
        "message": (
            f"Override saved. '{body.symbol}' {body.action}ed in '{body.theme_id}' only. "
            + ("Manual leader for this theme was also cleared. " if leader_cleared else "")
            + "Universe rebuilt and RS cache cleared."
        ),
    }


@router.post("/admin/memberships/bulk")
async def admin_bulk_memberships(
    request: Request,
    body: BulkMembershipBody,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    """
    [Admin] Bulk add/remove tickers across themes (many-to-many).
    Same symbol can be added to multiple themes independently.
    """
    err = _check_admin(request, x_api_key)
    if err:
        return err

    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _uni
    unknown_themes = [e.theme_id for e in body.edits if e.theme_id not in _uni]
    if unknown_themes:
        raise HTTPException(status_code=404, detail=f"Unknown theme_id(s): {unknown_themes}")

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

    # ── Cross-sync adds to Watchlist (category_overrides + theme_ticker_mapper) ─
    add_edits = [e for e in body.edits if e.action == "add"]
    if add_edits and result["succeeded"] > 0:
        try:
            from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _eu_bulk
            from services.category_overrides import bulk_upsert as _bulk_cat
            from services.theme_ticker_mapper import register_llm_classified_tickers as _xsync_bulk
            cat_updates = []
            mapper_items = []
            for _ae in add_edits:
                _dn = _eu_bulk.get(_ae.theme_id, {}).get("display_name") or _ae.theme_id
                cat_updates.append({
                    "ticker":   _ae.symbol,
                    "category": _dn,
                    "source":   "themes_page_manual",
                    "reason":   f"themes_page:{_ae.theme_id}",
                })
                mapper_items.append({"ticker": _ae.symbol, "theme": _dn, "confidence": "manual"})
            if cat_updates:
                _bulk_cat("default", cat_updates)
            if mapper_items:
                _xsync_bulk(mapper_items)
        except Exception as _bxse:
            print(f"[THEMES_ADMIN] bulk watchlist cross-sync failed (non-fatal): {_bxse}")

    return {
        "ok":        result["failed"] == 0,
        "succeeded": result["succeeded"],
        "failed":    result["failed"],
        "errors":    result["errors"],
        "message":   f"{result['succeeded']} override(s) saved. Universe rebuilt and RS cache cleared.",
    }


@router.delete("/admin/memberships/{theme_id}/{symbol}")
async def admin_delete_membership(
    request: Request,
    theme_id: str,
    symbol: str,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    """
    [Admin] Clear the manual override for (theme_id, symbol). Restores default behavior.
    Does NOT affect any other theme. If this symbol was the manual leader for this theme,
    the leader override is also cleared automatically.
    """
    err = _check_admin(request, x_api_key)
    if err:
        return err

    theme_id = theme_id.strip().lower()
    symbol   = symbol.strip().upper()

    try:
        from data.pg_storage import delete_theme_ticker_override
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Storage unavailable: {exc}")

    deleted = delete_theme_ticker_override(theme_id=theme_id, symbol=symbol)

    leader_cleared = False
    if deleted:
        try:
            from data.pg_storage import get_theme_leaders_map, delete_theme_leader
            lmap = get_theme_leaders_map()
            current_leader = lmap.get(theme_id, {}).get("leader_symbol")
            if current_leader == symbol:
                delete_theme_leader(theme_id)
                leader_cleared = True
        except Exception as _le:
            print(f"[THEMES_ADMIN] leader auto-clear check error: {_le}")
        _invalidate_caches()

    return {
        "ok":            True,
        "deleted":       deleted,
        "theme_id":      theme_id,
        "symbol":        symbol,
        "leader_cleared": leader_cleared,
        "message": (
            (
                f"Override for '{symbol}' in '{theme_id}' cleared."
                + (" Manual leader also cleared." if leader_cleared else "")
                + " Default universe behavior restored."
            )
            if deleted
            else f"No override found for '{symbol}' in '{theme_id}' — nothing changed."
        ),
    }


@router.get("/admin/theme-basket/{theme_id}")
async def admin_theme_basket(
    request: Request,
    theme_id: str,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    """
    [Admin] Full source breakdown of a theme's basket including leader state.

    Returns:
      theme_id, display_name, representative_symbol, holdings_display_mode,
      base_symbols, watchlist_seed_symbols, manual_added_symbols, manual_removed_symbols,
      final_theme_holdings, final_performance_symbols,
      manual_leader_symbol, effective_leader_symbol, leader_source
    """
    err = _check_admin(request, x_api_key)
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

    manual_leader_sym: Optional[str] = None
    leader_src = "none"
    try:
        from data.pg_storage import get_theme_leaders_map
        lmap = get_theme_leaders_map()
        entry = lmap.get(theme_id)
        if entry:
            manual_leader_sym = entry["leader_symbol"]
            leader_src = "manual"
    except Exception:
        pass

    return {
        "theme_id":                theme_id,
        "display_name":            meta.get("display_name", ""),
        "proxy_type":              meta.get("proxy_type", ""),
        "representative_symbol":   meta.get("representative_symbol", ""),
        "holdings_display_mode":   holdings_mode,
        "base_symbols":            base_proxy,
        "watchlist_seed_symbols":  watchlist_seeds,
        "manual_added_symbols":    manual_added,
        "manual_removed_symbols":  manual_removed,
        "final_theme_holdings":    sorted(final_proxy) if holdings_mode == "theme_basket" else [],
        "final_performance_symbols": final_proxy,
        "manual_leader_symbol":    manual_leader_sym,
        "effective_leader_symbol": manual_leader_sym,
        "leader_source":           leader_src,
    }


# ── Admin / dev-only endpoints — theme leaders ────────────────────────────────
#
# One manual leader per theme (PK = theme_id).
# Same symbol can be leader for multiple different themes.
# leader_symbol is a stock ticker inside the theme basket, not the ETF representative_symbol.


class LeaderEdit(BaseModel):
    theme_id:      str
    leader_symbol: str
    note:          Optional[str] = None
    created_by:    Optional[str] = "admin"

    @field_validator("theme_id")
    @classmethod
    def validate_theme_id(cls, v: str) -> str:
        v = v.strip().lower()
        if not v:
            raise ValueError("theme_id must not be empty")
        return v

    @field_validator("leader_symbol")
    @classmethod
    def validate_leader_symbol(cls, v: str) -> str:
        v = v.strip().upper()
        if not _SYM_RE.match(v):
            raise ValueError(f"Invalid symbol format: '{v}'")
        return v


@router.get("/admin/leaders")
async def admin_list_leaders(
    request: Request,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    """[Admin] List all manual theme leaders."""
    err = _check_admin(request, x_api_key)
    if err:
        return err

    try:
        from data.pg_storage import get_all_theme_leaders
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Storage unavailable: {exc}")

    leaders = get_all_theme_leaders()

    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _uni
    for row in leaders:
        meta = _uni.get(row["theme_id"], {})
        row["display_name"] = meta.get("display_name", row["theme_id"])

    return {"leaders": leaders, "leader_count": len(leaders)}


@router.post("/admin/leaders")
async def admin_set_leader(
    request: Request,
    body: LeaderEdit,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    """
    [Admin] Set or update the manual leader for a theme.

    Rules:
      - One leader per theme (upserts on theme_id).
      - Same symbol can be leader for multiple themes (no global uniqueness).
      - leader_symbol MUST already be present in final_performance_symbols for
        this theme. If not, returns 400 — add the ticker to the basket first.
      - representative_symbol is NOT changed (ETF/proxy chart symbol is separate).

    Invalidates theme caches after write.
    """
    err = _check_admin(request, x_api_key)
    if err:
        return err

    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _uni
    if body.theme_id not in _uni:
        raise HTTPException(
            status_code=404,
            detail=f"theme_id '{body.theme_id}' not found in universe.",
        )

    meta        = _uni[body.theme_id]
    final_syms  = set(meta.get("proxy_symbols", []))
    if body.leader_symbol not in final_syms:
        raise HTTPException(
            status_code=400,
            detail=(
                f"'{body.leader_symbol}' is not in the final basket for '{body.theme_id}'. "
                f"Current basket: {sorted(final_syms)}. "
                f"Add it via POST /api/themes/admin/memberships first."
            ),
        )

    try:
        from data.pg_storage import upsert_theme_leader
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Storage unavailable: {exc}")

    ok = upsert_theme_leader(
        theme_id=body.theme_id,
        leader_symbol=body.leader_symbol,
        source="manual_admin",
        note=body.note,
        created_by=body.created_by,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to write leader to Neon")

    _invalidate_caches()

    return {
        "ok":            True,
        "theme_id":      body.theme_id,
        "leader_symbol": body.leader_symbol,
        "note":          body.note,
        "message": (
            f"Leader set. '{body.leader_symbol}' is now the manual leader for '{body.theme_id}'. "
            f"RS cache cleared — leader_symbol will appear in the next Themes API response."
        ),
    }


@router.delete("/admin/leaders/{theme_id}")
async def admin_clear_leader(
    request: Request,
    theme_id: str,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    """
    [Admin] Clear the manual leader for a theme. Restores default behavior (leader_source='none').
    Does NOT remove the symbol from the basket. Does NOT affect any other theme.
    """
    err = _check_admin(request, x_api_key)
    if err:
        return err

    theme_id = theme_id.strip().lower()

    try:
        from data.pg_storage import delete_theme_leader
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Storage unavailable: {exc}")

    deleted = delete_theme_leader(theme_id)

    if deleted:
        _invalidate_caches()

    return {
        "ok":      True,
        "deleted": deleted,
        "theme_id": theme_id,
        "message": (
            f"Manual leader for '{theme_id}' cleared. Default leader behavior restored."
            if deleted
            else f"No manual leader found for '{theme_id}' — nothing changed."
        ),
    }
