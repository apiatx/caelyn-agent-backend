"""
Themes by Relative Strength — HTTP endpoints.

GET  /api/themes/relative-strength?timeframe=1D|7D|30D|YTD|1Y|5Y[&classification=all|sector|theme|sub_theme]
GET  /api/themes/relative-strength/refresh  (force-refresh cache, same params)
GET  /api/themes/list                        (static theme registry)
GET  /api/themes/merge-debug                 (dev/admin diagnostic)

Admin (dev-only, X-API-Key OR admin JWT required):
GET    /api/themes/admin/memberships                        list all ticker overrides
POST   /api/themes/admin/memberships                        add/remove a single ticker
POST   /api/themes/admin/assign-primary-theme                assign/reassign ONE primary theme (thin wrapper)
POST   /api/themes/admin/memberships/bulk                   bulk add/remove
DELETE /api/themes/admin/memberships/{theme_id}/{symbol}    clear a ticker override
GET    /api/themes/admin/theme-basket/{theme_id}            basket breakdown + leader

GET    /api/themes/admin/leaders                            list all manual leaders
POST   /api/themes/admin/leaders                            set/update a theme leader
DELETE /api/themes/admin/leaders/{theme_id}                 clear a theme leader
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Query, HTTPException, Request, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator

_log = logging.getLogger(__name__)

from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as THEME_RS_UNIVERSE
from services.theme_rs_universe import get_effective_rollup_sector_ids as _get_effective_rollup

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
            # Hierarchy v2 — additive fields (None when not applicable)
            # rollup_sector_ids: always the EFFECTIVE set (explicit + inherited
            # from parent_sector / parent_theme_id chain) so callers never need
            # to reconstruct registry semantics client-side.
            "parent_theme_id":     meta.get("parent_theme_id"),
            "rollup_sector_ids":   _get_effective_rollup(theme_id, THEME_RS_UNIVERSE),
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


def _perform_membership_write(
    theme_id: str,
    symbol: str,
    action: str,
    note: Optional[str] = None,
    created_by: Optional[str] = "admin",
) -> dict:
    """
    THE single authoritative write helper for manual theme-ticker membership.

    Both POST /admin/memberships and POST /admin/assign-primary-theme call this
    exact function — there is no separate database write path. Performs:
      1. upsert_theme_ticker_override (Neon theme_ticker_overrides table)
      2. auto-clear manual leader if the removed symbol was the leader
      3. refresh_enriched_universe() + RS/sectors cache invalidation
      4. Options Flow high-priority backfill hint (action='add' only)
      5. Cross-sync into Watchlist category_overrides + theme_ticker_mapper
         (action='add' only) — this is what makes the write "primary" for
         resolve_primary_theme_for_ticker (cat_overrides always wins).

    Raises HTTPException on validation/storage failure. Returns the same
    response shape previously returned inline by admin_upsert_membership.
    """
    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _uni
    if theme_id not in _uni:
        raise HTTPException(
            status_code=404,
            detail=f"theme_id '{theme_id}' not found in universe.",
        )

    try:
        from data.pg_storage import upsert_theme_ticker_override
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Storage unavailable: {exc}")

    ok = upsert_theme_ticker_override(
        theme_id=theme_id,
        symbol=symbol,
        action=action,
        source="manual_admin",
        note=note,
        created_by=created_by,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to write override to Neon")

    leader_cleared = False
    if action == "remove":
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

    # ── Options Flow: mark newly-added symbol as high-priority for backfill ───
    # Fires before the cross-sync so the scanner can start while sync completes.
    if action == "add":
        try:
            from data.options_theme_supplement import add_high_priority_symbols as _add_hi
            _add_hi([symbol])
        except Exception as _hpe:
            print(f"[THEMES_ADMIN] high-priority hook failed (non-fatal): {_hpe}")

    # ── Cross-sync to Watchlist (category_overrides + theme_ticker_mapper) ────
    if action == "add":
        try:
            from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _eu_now
            _display = _eu_now.get(theme_id, {}).get("display_name") or theme_id
            from services.category_overrides import upsert_override as _upsert_cat
            _upsert_cat("default", symbol, _display, "themes_page_manual",
                        f"themes_page:{theme_id}")
            from services.theme_ticker_mapper import register_llm_classified_tickers as _xsync
            _xsync([{"ticker": symbol, "theme": _display, "confidence": "manual"}])
        except Exception as _xse:
            print(f"[THEMES_ADMIN] watchlist cross-sync failed (non-fatal): {_xse}")
    elif action == "remove":
        # Clean up the single-row category_overrides "primary theme" record so a
        # removed membership does not leave a stale manual_override pointing at
        # this theme_id (resolver precedence always prefers manual_override, so
        # a stale row here would keep resolving to the removed theme).
        try:
            from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _eu_now2
            _display2 = _eu_now2.get(theme_id, {}).get("display_name") or theme_id
            from services.category_overrides import delete_override as _delete_cat
            _delete_cat("default", symbol, only_if_category=_display2)
        except Exception as _xde:
            print(f"[THEMES_ADMIN] watchlist cross-sync (remove cleanup) failed (non-fatal): {_xde}")

        # Clean up the corresponding data/llm_theme_overrides.json row when it
        # still points at this theme_id. Guarded: if the ticker was already
        # reassigned to a different theme_id after this row was written, that
        # newer row is left untouched (guard lives in remove_llm_theme_override).
        try:
            from services.theme_ticker_mapper import remove_llm_theme_override as _delete_llm
            _delete_llm(symbol, only_if_theme_id=theme_id)
        except Exception as _xle:
            print(f"[THEMES_ADMIN] watchlist cross-sync (llm file cleanup) failed (non-fatal): {_xle}")

    # ── Build authoritative basket from freshly-rebuilt enriched universe ─────
    # _invalidate_caches() called refresh_enriched_universe() which did an
    # in-place update of ENRICHED_THEME_RS_UNIVERSE.  Reading from it now
    # guarantees the response reflects the committed Neon row.
    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _eu_resp
    _resp_meta    = _eu_resp.get(theme_id, {})
    _final_basket = sorted(_resp_meta.get("proxy_symbols", []))
    _manual_added = sorted(_resp_meta.get("manual_added_symbols", []))
    _manual_removed = sorted(_resp_meta.get("manual_removed_symbols", []))

    return {
        "ok":                   True,
        "persisted":            True,
        "theme_id":             theme_id,
        "symbol":               symbol,
        "action":               action,
        "note":                 note,
        "leader_cleared":       leader_cleared,
        "member_count":         len(_final_basket),
        "theme_holdings":       _final_basket,
        "manual_added_symbols": _manual_added,
        "manual_removed_symbols": _manual_removed,
        "message": (
            f"Override saved. '{symbol}' {action}ed in '{theme_id}' only. "
            + ("Manual leader for this theme was also cleared. " if leader_cleared else "")
            + "Universe rebuilt and RS cache cleared."
        ),
    }


def _perform_theme_membership_only_write(
    theme_id: str,
    symbol: str,
    action: str,
    note: Optional[str] = None,
    created_by: Optional[str] = "admin",
) -> dict:
    """
    Membership-ONLY write helper for Additional Theme memberships (multi-theme v1).

    Unlike `_perform_membership_write()` (which is reserved for PRIMARY theme
    identity via /admin/assign-primary-theme and the legacy /admin/memberships
    endpoint), this helper deliberately does NOT touch:
      - watchlist_category_overrides (primary theme identity store)
      - data/llm_theme_overrides.json (legacy classification/mapper bootstrap)

    It only writes:
      1. theme_ticker_overrides (Neon) — action='add'|'remove'
      2. auto-clears the theme's manual leader if the removed symbol was it
      3. refresh_enriched_universe() + RS/sectors cache invalidation (existing
         refresh path — no new merge logic, no manual ENRICHED_THEME_RS_UNIVERSE
         mutation)
      4. Options Flow high-priority backfill hint (action='add' only) — this is
         a scan-eligibility hint, not a classification/identity write.

    This is what makes it safe for a ticker to gain an ADDITIONAL theme basket
    membership without altering its canonical primary theme, resolver output,
    Watchlist Theme column, Watchlist performance grouping, or Trade Alignment
    theme selection (all of which read primary identity stores only).
    """
    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _uni
    if theme_id not in _uni:
        raise HTTPException(
            status_code=404,
            detail=f"theme_id '{theme_id}' not found in universe.",
        )

    try:
        from data.pg_storage import upsert_theme_ticker_override
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Storage unavailable: {exc}")

    ok = upsert_theme_ticker_override(
        theme_id=theme_id,
        symbol=symbol,
        action=action,
        source="manual_admin_additional",
        note=note,
        created_by=created_by,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to write override to Neon")

    leader_cleared = False
    if action == "remove":
        try:
            from data.pg_storage import get_theme_leaders_map, delete_theme_leader
            lmap = get_theme_leaders_map()
            current_leader = lmap.get(theme_id, {}).get("leader_symbol")
            if current_leader == symbol:
                delete_theme_leader(theme_id)
                leader_cleared = True
        except Exception as _le:
            print(f"[THEMES_ADMIN] additional-membership leader auto-clear check error: {_le}")

    _invalidate_caches()

    if action == "add":
        try:
            from data.options_theme_supplement import add_high_priority_symbols as _add_hi
            _add_hi([symbol])
        except Exception as _hpe:
            print(f"[THEMES_ADMIN] additional-membership high-priority hook failed (non-fatal): {_hpe}")

    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _eu_resp
    _resp_meta    = _eu_resp.get(theme_id, {})
    _final_basket = sorted(_resp_meta.get("proxy_symbols", []))
    _manual_added = sorted(_resp_meta.get("manual_added_symbols", []))
    _manual_removed = sorted(_resp_meta.get("manual_removed_symbols", []))

    return {
        "ok":                   True,
        "persisted":            True,
        "theme_id":             theme_id,
        "symbol":               symbol,
        "action":               action,
        "note":                 note,
        "leader_cleared":       leader_cleared,
        "member_count":         len(_final_basket),
        "theme_holdings":       _final_basket,
        "manual_added_symbols": _manual_added,
        "manual_removed_symbols": _manual_removed,
        "message": (
            f"Additional theme membership saved. '{symbol}' {action}ed in '{theme_id}' basket only. "
            "Canonical primary theme untouched. "
            + ("Manual leader for this theme was also cleared. " if leader_cleared else "")
            + "Universe rebuilt and RS cache cleared."
        ),
    }


def _get_ticker_theme_memberships(ticker: str) -> dict:
    """
    Shared read helper: returns the canonical primary theme plus every active
    theme_ticker_overrides membership row for a ticker, with is_primary computed
    read-only (membership theme_id == canonical resolver theme_id). No new
    stored field — is_primary is always derived, never persisted.
    """
    from data.pg_storage import get_theme_ticker_overrides
    from services.theme_resolver import resolve_primary_theme_for_ticker
    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _uni

    ticker = ticker.strip().upper()
    resolution = resolve_primary_theme_for_ticker(ticker)
    primary_theme_id = resolution.get("theme_id")

    all_rows = get_theme_ticker_overrides()
    active_by_theme: dict[str, dict] = {}
    for row in all_rows:
        if row.get("symbol") != ticker:
            continue
        tid = row.get("theme_id")
        # Rows are chronological per theme_ticker_overrides ordering; last
        # action for a given theme_id wins (mirrors upsert-on-PK semantics).
        active_by_theme[tid] = row

    memberships = []
    for tid, row in sorted(active_by_theme.items()):
        if row.get("action") != "add":
            continue
        meta = _uni.get(tid, {})
        memberships.append({
            "theme_id":           tid,
            "theme_name":         meta.get("display_name", tid),
            "membership_source":  row.get("source"),
            "is_primary":         (tid == primary_theme_id),
        })

    additional = [m for m in memberships if not m["is_primary"]]

    return {
        "ticker": ticker,
        "primary_theme": {
            "theme_id":   primary_theme_id,
            "theme_name": resolution.get("theme_name"),
            "source":     resolution.get("source"),
        },
        "theme_memberships": memberships,
        "additional_theme_memberships": additional,
    }


class AdditionalMembershipEdit(BaseModel):
    ticker:   str
    theme_id: str
    action:   str          # "add" | "remove"
    note:     Optional[str] = None
    created_by: Optional[str] = "admin"

    @field_validator("ticker")
    @classmethod
    def validate_ticker(cls, v: str) -> str:
        v = v.strip().upper()
        if not _SYM_RE.match(v):
            raise ValueError(f"Invalid ticker format: '{v}'")
        return v

    @field_validator("theme_id")
    @classmethod
    def validate_theme_id(cls, v: str) -> str:
        v = v.strip().lower()
        if not v:
            raise ValueError("theme_id must not be empty")
        return v

    @field_validator("action")
    @classmethod
    def validate_action(cls, v: str) -> str:
        v = v.strip().lower()
        if v not in _VALID_ACTIONS:
            raise ValueError(f"action must be one of {_VALID_ACTIONS}")
        return v


@router.post("/admin/additional-memberships")
async def admin_additional_membership(
    request: Request,
    body: AdditionalMembershipEdit,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    """
    [Admin] Add or remove an ADDITIONAL theme basket membership for a ticker,
    without touching the ticker's canonical PRIMARY theme.

    Use POST /admin/assign-primary-theme to change the primary theme instead.

    action='add':    ticker joins theme_id's basket as an additional membership.
    action='remove': ticker leaves theme_id's basket (only that basket).

    Does NOT write watchlist_category_overrides or data/llm_theme_overrides.json.
    Trade Alignment theme selection is unaffected (it reads primary identity only).
    """
    err = _check_admin(request, x_api_key)
    if err:
        return err

    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _uni
    if body.theme_id not in _uni:
        raise HTTPException(
            status_code=404,
            detail=f"theme_id '{body.theme_id}' not found in universe. "
                   f"Valid ids: {sorted(_uni.keys())}",
        )

    write_result = _perform_theme_membership_only_write(
        theme_id=body.theme_id,
        symbol=body.ticker,
        action=body.action,
        note=body.note,
        created_by=body.created_by,
    )

    memberships_view = _get_ticker_theme_memberships(body.ticker)

    return {
        "ok": write_result["ok"],
        "ticker": body.ticker,
        "theme_id": body.theme_id,
        "action": body.action,
        "member_count": write_result.get("member_count"),
        "primary_theme": memberships_view["primary_theme"],
        "theme_memberships": memberships_view["theme_memberships"],
        "message": write_result["message"],
    }


@router.get("/admin/ticker-memberships/{ticker}")
async def admin_get_ticker_memberships(
    request: Request,
    ticker: str,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    """
    [Admin] Read the canonical primary theme plus every active theme basket
    membership for one ticker (for a future frontend Primary/Additional
    Themes display). Read-only; is_primary is always derived from the
    canonical resolver, never a persisted field.
    """
    err = _check_admin(request, x_api_key)
    if err:
        return err

    ticker = ticker.strip().upper()
    if not _SYM_RE.match(ticker):
        raise HTTPException(status_code=400, detail=f"Invalid ticker format: '{ticker}'")

    return _get_ticker_theme_memberships(ticker)


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

    return _perform_membership_write(
        theme_id=body.theme_id,
        symbol=body.symbol,
        action=body.action,
        note=body.note,
        created_by=body.created_by,
    )


class PrimaryThemeAssignment(BaseModel):
    ticker:   str
    theme_id: str
    note:     Optional[str] = None
    created_by: Optional[str] = "admin"

    @field_validator("ticker")
    @classmethod
    def validate_ticker(cls, v: str) -> str:
        v = v.strip().upper()
        if not _SYM_RE.match(v):
            raise ValueError(f"Invalid ticker format: '{v}'")
        return v

    @field_validator("theme_id")
    @classmethod
    def validate_theme_id(cls, v: str) -> str:
        v = v.strip().lower()
        if not v:
            raise ValueError("theme_id must not be empty")
        return v


@router.post("/admin/assign-primary-theme")
async def admin_assign_primary_theme(
    request: Request,
    body: PrimaryThemeAssignment,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    """
    [Admin] Assign/reassign the ONE canonical primary Theme for a Watchlist ticker.

    Thin wrapper over the same authoritative write path as POST /admin/memberships —
    it does not implement a separate database write path. It exists only to give the
    Watchlist Screener "Theme" dropdown a clean single-call contract (ticker + theme_id)
    instead of requiring the caller to reason about the underlying many-to-many
    theme_ticker_overrides table or manual reassignment sequencing.

    Behavior:
      1. Validates ticker format and that theme_id exists in the canonical universe.
      2. Resolves the ticker's CURRENT primary theme via
         theme_resolver.resolve_primary_theme_for_ticker() (same resolver Watchlist/
         Themes-page/Confluence already use).
      3. If the ticker currently has a different manually-assigned primary theme,
         first calls _perform_membership_write(old_theme_id, ticker, "remove") so the
         old theme's basket does not retain a stale membership row (no ambiguous
         competing primary assignments).
      4. Calls _perform_membership_write(new theme_id, ticker, "add") — this is the
         same call POST /admin/memberships makes, including the category_overrides
         cross-sync that resolve_primary_theme_for_ticker's top-precedence
         manual_override always wins on.
      5. Returns the resolved final primary theme for confirmation.

    Only removes the OLD assignment if it was itself a manual override (i.e. the
    resolver's source was "manual_override" or "themes_page_membership"); if the
    ticker had no prior manual assignment (industry_fallback/canonical_map/no_mapping),
    no remove call is made — there is nothing stale to clean up.
    """
    err = _check_admin(request, x_api_key)
    if err:
        return err

    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _uni
    if body.theme_id not in _uni:
        raise HTTPException(
            status_code=404,
            detail=f"theme_id '{body.theme_id}' not found in universe. "
                   f"Valid ids: {sorted(_uni.keys())}",
        )

    from services.theme_resolver import resolve_primary_theme_for_ticker
    prior = resolve_primary_theme_for_ticker(body.ticker)
    prior_theme_id = prior.get("theme_id")
    prior_source   = prior.get("source")

    removed_old = False
    old_theme_id_cleared: Optional[str] = None
    if (
        prior_theme_id
        and prior_theme_id != body.theme_id
        and prior_theme_id in _uni
        and prior_source in ("manual_override", "themes_page_membership", "llm_classified")
    ):
        try:
            _perform_membership_write(
                theme_id=prior_theme_id,
                symbol=body.ticker,
                action="remove",
                note=f"auto-cleared on reassignment to {body.theme_id}",
                created_by=body.created_by,
            )
            removed_old = True
            old_theme_id_cleared = prior_theme_id
        except HTTPException as _rhe:
            print(f"[THEMES_ADMIN] assign-primary-theme old-theme cleanup failed (non-fatal): {_rhe.detail}")

    add_result = _perform_membership_write(
        theme_id=body.theme_id,
        symbol=body.ticker,
        action="add",
        note=body.note,
        created_by=body.created_by,
    )

    final = resolve_primary_theme_for_ticker(body.ticker)

    return {
        "ok":                  True,
        "ticker":              body.ticker,
        "requested_theme_id":  body.theme_id,
        "prior_theme_id":      prior_theme_id,
        "prior_theme_source":  prior_source,
        "removed_old_theme":   removed_old,
        "old_theme_id_cleared": old_theme_id_cleared,
        "resolved_theme_name": final.get("theme_name"),
        "resolved_theme_id":   final.get("theme_id"),
        "resolved_source":     final.get("source"),
        "member_count":        add_result.get("member_count"),
        "message": (
            f"'{body.ticker}' primary theme assigned to '{body.theme_id}'. "
            + (f"Removed stale membership from '{old_theme_id_cleared}'. " if removed_old else "")
            + "Enriched universe rebuilt; Watchlist/Themes-page/downstream consumers will reflect this on next read."
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

    # ── Options Flow: mark newly-added symbols as high-priority for backfill ──
    add_edits = [e for e in body.edits if e.action == "add"]
    if add_edits and result["succeeded"] > 0:
        try:
            from data.options_theme_supplement import add_high_priority_symbols as _add_hi_bulk
            _add_hi_bulk([e.symbol for e in add_edits])
        except Exception as _hpe:
            print(f"[THEMES_ADMIN] bulk high-priority hook failed (non-fatal): {_hpe}")

    # ── Cross-sync adds to Watchlist (category_overrides + theme_ticker_mapper) ─
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


# ── Atomic multi-membership assignment ────────────────────────────────────────

class TickerTaxonomyBody(BaseModel):
    """
    Atomic request to set primary + additional theme memberships for one ticker.

    primary_theme_id  = one assignable canonical theme or sub_theme ID, or null to clear.
    additional_theme_ids = zero or more additional memberships (deduplicated, sorted).

    Validations applied server-side:
    - All IDs must exist in the canonical assignable registry.
    - Sector IDs are rejected as thematic memberships.
    - Deprecated and market_lens IDs are rejected.
    - primary is removed from additional_theme_ids.
    - Redundant ancestor memberships are removed when a child already provides rollup.
    - No duplicate IDs.
    """
    primary_theme_id:     Optional[str]      = None
    additional_theme_ids: list[str]          = []
    note:                 Optional[str]      = None
    created_by:           Optional[str]      = "admin"

    @field_validator("primary_theme_id")
    @classmethod
    def _normalize_primary(cls, v: Optional[str]) -> Optional[str]:
        return v.strip().lower() if v else None

    @field_validator("additional_theme_ids")
    @classmethod
    def _normalize_additional(cls, v: list[str]) -> list[str]:
        return sorted({t.strip().lower() for t in v if t})


@router.put("/admin/ticker-taxonomy/{ticker}")
async def admin_put_ticker_taxonomy(
    ticker:      str,
    request:     Request,
    body:        TickerTaxonomyBody,
    x_api_key:   Optional[str] = Header(None, alias="X-API-Key"),
):
    """
    [Admin] Atomically assign primary + additional theme memberships for one ticker.

    Implements the canonical multi-membership model:
      One primary membership  (theme or sub_theme, or null to clear)
      Zero or more additional memberships  (different theme families)

    All theme_ticker_overrides changes AND the primary watchlist_category_overrides
    change execute in a single database transaction via atomic_taxonomy_write_db().
    No per-membership helpers are called from this route.

    Processing steps:
      1. Authenticate admin.
      2. Normalize ticker to uppercase.
      3. Validate every ID against the assignable canonical registry.
         - Reject sector IDs.
         - Reject deprecated and market_lens IDs.
      4. Remove primary from additional_theme_ids; deduplicate; remove redundant ancestors.
      5. Read current explicit memberships and current canonical primary.
      6. Compute deterministic desired state (to_add / to_remove).
      7. Build one complete transaction payload.
      8. Call atomic_taxonomy_write_db() exactly once.
      9. Raise immediately when the transaction fails (no compensating writes).
      10. Invalidate caches exactly once after successful commit.
      11. Run optional non-authoritative downstream hints after commit.
      12. Reread authoritative state and validate it matches the requested normalized state.
      13. Return the authoritative reread (primary_theme_id from reread, never from request).
    """
    # ── 1. Authenticate ───────────────────────────────────────────────────────
    err = _check_admin(request, x_api_key)
    if err:
        return err

    # ── 2. Normalize ticker ───────────────────────────────────────────────────
    ticker = ticker.strip().upper()
    if not _SYM_RE.match(ticker):
        raise HTTPException(status_code=422, detail=f"Invalid ticker format: '{ticker}'")

    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _uni
    from services.theme_rs_universe import THEME_RS_UNIVERSE as _base_uni

    # ── 3. Validate IDs ───────────────────────────────────────────────────────
    sector_ids = {k for k, v in _base_uni.items() if v.get("classification") == "sector"}
    assignable_ids = {
        k for k, v in _base_uni.items()
        if v.get("assignable", True) and v.get("classification") not in ("market_lens", "deprecated", "sector")
    }

    def _validate_theme_id(tid: str, field: str) -> None:
        if tid in sector_ids:
            raise HTTPException(
                status_code=422,
                detail=f"{field} {tid!r}: sector IDs cannot be used as thematic memberships.",
            )
        meta = _base_uni.get(tid, {})
        if meta.get("classification") in ("market_lens", "deprecated"):
            raise HTTPException(
                status_code=422,
                detail=f"{field} {tid!r}: market-lens and deprecated IDs are not assignable.",
            )
        if tid not in assignable_ids:
            raise HTTPException(
                status_code=404,
                detail=f"{field} {tid!r}: not found in canonical assignable registry.",
            )

    if body.primary_theme_id:
        _validate_theme_id(body.primary_theme_id, "primary_theme_id")
    for atid in body.additional_theme_ids:
        _validate_theme_id(atid, f"additional_theme_ids[{atid!r}]")

    # ── 4. Normalize: remove primary from additional, deduplicate, prune ancestors
    additional: list[str] = sorted(
        {t for t in body.additional_theme_ids if t != body.primary_theme_id}
    )

    def _get_parent(tid: str) -> Optional[str]:
        return _base_uni.get(tid, {}).get("parent_theme_id")

    final_ids = set(additional)
    if body.primary_theme_id:
        final_ids.add(body.primary_theme_id)

    parents_covered = {_get_parent(t) for t in final_ids if _get_parent(t)}
    # Remove redundant ancestors from additional only (never strip the primary)
    additional = [
        t for t in additional
        if not (t in parents_covered and any(
            _get_parent(child) == t for child in final_ids if child != t
        ))
    ]

    # ── 5. Read current authoritative state ───────────────────────────────────
    current_memberships = _get_ticker_theme_memberships(ticker)
    current_all: set[str] = {m["theme_id"] for m in current_memberships["theme_memberships"]}
    current_primary: Optional[str] = current_memberships["primary_theme"]["theme_id"]

    # ── 6. Compute deterministic desired state ────────────────────────────────
    desired_all: set[str] = set(additional)
    if body.primary_theme_id:
        desired_all.add(body.primary_theme_id)

    to_remove = current_all - desired_all
    to_add    = desired_all - current_all

    # ── 7. Build one complete transaction payload ─────────────────────────────
    # All removals first, then upsert every desired membership (handles
    # promotions/demotions: updating source between manual_admin /
    # manual_admin_additional in the same ON CONFLICT DO UPDATE pass).
    membership_edits: list[dict] = []
    for rem_tid in sorted(to_remove):
        membership_edits.append({
            "theme_id":   rem_tid,
            "symbol":     ticker,
            "action":     "remove",
            "source":     "manual_admin",
            "note":       f"cleared by taxonomy PUT (replaced by {body.primary_theme_id or 'none'})",
            "created_by": body.created_by,
        })
    for desired_tid in sorted(desired_all):
        src = "manual_admin" if desired_tid == body.primary_theme_id else "manual_admin_additional"
        membership_edits.append({
            "theme_id":   desired_tid,
            "symbol":     ticker,
            "action":     "add",
            "source":     src,
            "note":       body.note,
            "created_by": body.created_by,
        })

    # Build primary store operation
    primary_op: Optional[dict] = None
    if body.primary_theme_id:
        display_name = _uni.get(body.primary_theme_id, {}).get("display_name") or body.primary_theme_id
        primary_op = {
            "action":   "set",
            "user_id":  "default",
            "ticker":   ticker,
            "category": display_name,
            "source":   "themes_page_manual",
            "reason":   f"themes_page:{body.primary_theme_id}",
        }
    elif current_primary is not None:
        # Clear the category override in the same transaction
        primary_op = {
            "action":  "clear",
            "user_id": "default",
            "ticker":  ticker,
        }

    # ── 8. One atomic transaction — all or nothing ────────────────────────────
    from data.pg_storage import atomic_taxonomy_write_db
    txn_result = atomic_taxonomy_write_db(
        ticker_overrides=membership_edits,
        primary_operation=primary_op,
    )

    # ── 9. Raise immediately on failure; no compensating writes ───────────────
    if not txn_result["ok"]:
        raise HTTPException(
            status_code=500,
            detail=f"Taxonomy write transaction failed: {txn_result.get('error', 'unknown')}",
        )

    # ── 10. Single cache invalidation after successful commit ─────────────────
    _invalidate_caches()

    # ── 11. Post-commit optional downstream hints (non-authoritative) ─────────
    if to_add:
        try:
            from data.options_theme_supplement import add_high_priority_symbols as _add_hi
            _add_hi([ticker])
        except Exception as _hpe:
            _log.warning("[taxonomy PUT] options high-priority hint failed (non-fatal): %s", _hpe)

    if body.primary_theme_id:
        try:
            _display_sync = _uni.get(body.primary_theme_id, {}).get("display_name") or body.primary_theme_id
            from services.theme_ticker_mapper import register_llm_classified_tickers as _xsync
            _xsync([{"ticker": ticker, "theme": _display_sync, "confidence": "manual"}])
        except Exception as _xse:
            _log.warning("[taxonomy PUT] theme mapper sync failed (non-fatal): %s", _xse)
    elif current_primary:
        try:
            from services.theme_ticker_mapper import remove_llm_theme_override as _del_llm
            _del_llm(ticker, only_if_theme_id=current_primary)
        except Exception as _xle:
            _log.warning("[taxonomy PUT] theme mapper clear failed (non-fatal): %s", _xle)

    # ── 12. Reread authoritative state ────────────────────────────────────────
    final = _get_ticker_theme_memberships(ticker)
    reread_primary: Optional[str] = final["primary_theme"]["theme_id"]
    reread_all: set[str] = {m["theme_id"] for m in final["theme_memberships"]}

    # Validate reread matches the requested normalized state
    expected_primary = body.primary_theme_id
    if reread_primary != expected_primary:
        raise HTTPException(
            status_code=500,
            detail=(
                f"Authoritative reread mismatch after commit: "
                f"expected primary={expected_primary!r}, stored primary={reread_primary!r}. "
                f"Transaction committed but state verification failed. "
                f"Diagnostics: desired_all={sorted(desired_all)}, reread_all={sorted(reread_all)}."
            ),
        )
    if not desired_all.issubset(reread_all):
        missing = sorted(desired_all - reread_all)
        raise HTTPException(
            status_code=500,
            detail=(
                f"Authoritative reread mismatch after commit: "
                f"requested memberships {missing} not found in stored state. "
                f"Transaction committed but state verification failed."
            ),
        )

    # ── 13. Build response with primary first in theme_ids ────────────────────
    reread_sorted = sorted(reread_all)
    if reread_primary and reread_primary in reread_all:
        ordered: list[str] = [reread_primary] + [t for t in reread_sorted if t != reread_primary]
    else:
        ordered = reread_sorted

    subtheme_ids = [
        t for t in ordered
        if _base_uni.get(t, {}).get("classification") == "sub_theme"
    ]

    return {
        "ok":                   True,
        "ticker":               ticker,
        "primary_theme_id":     reread_primary,
        "additional_theme_ids": [t for t in ordered if t != reread_primary],
        "theme_ids":            ordered,
        "subtheme_ids":         subtheme_ids,
        "sector_id":            None,
        "memberships_removed":  sorted(to_remove),
        "memberships_added":    sorted(to_add),
    }


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
