"""
Screener Hub HTTP endpoints.

GET  /api/screener-hub/themes
GET  /api/screener-hub
POST /api/admin/screener-hub/rebuild             (X-API-Key: AGENT_API_KEY)
GET  /api/admin/screener-hub/status              (X-API-Key: AGENT_API_KEY)
GET  /api/admin/screener-hub/audit               (X-API-Key: AGENT_API_KEY) — discovery audit for a single theme
GET  /api/admin/screener-hub/thin-themes         (X-API-Key: AGENT_API_KEY) — list themes needing richer discovery
POST /api/admin/screener-hub/rebuild-thin        (X-API-Key: AGENT_API_KEY) — sequential rebuild for thin themes
POST /api/admin/bottlenecks/refresh              (X-API-Key: AGENT_API_KEY) — force full CR regen + universe rebuild
POST /api/admin/bottlenecks/research-anchor      (X-API-Key: AGENT_API_KEY) — run LLM research for one overlay anchor
POST /api/admin/bottlenecks/revalidate-anchor    (X-API-Key: AGENT_API_KEY) — re-run validation gates on existing approved nodes (no LLM)
POST /api/admin/bottlenecks/research-anchors-monthly (X-API-Key: AGENT_API_KEY) — run monthly refresh for all overlay anchors
GET  /api/admin/bottlenecks/research-status      (X-API-Key: AGENT_API_KEY) — overlay anchor research status
GET  /api/debug/bottlenecks-snapshot             (X-API-Key: AGENT_API_KEY) — diagnostics for snapshot state
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Body, Header, Query, Request
from fastapi.responses import JSONResponse

from services.screener_hub_service import (
    _theme_keys,
    _theme_metadata,
    get_admin_status,
    get_screener_hub,
    rebuild_universe,
    warm_tab_fundamentals,
)

router = APIRouter(tags=["screener_hub"])

_AUTH_HEADER = "X-API-Key"
_VALID_TABS = {"thematic", "social", "bottlenecks", "watchlist_portfolio"}
_VALID_REBUILD_TABS = _VALID_TABS | {"all"}


def _check_admin_key(api_key: Optional[str]) -> Optional[JSONResponse]:
    """Mirror existing AGENT_API_KEY pattern (catalyst_calendar.py)."""
    try:
        from config import AGENT_API_KEY
    except Exception:
        AGENT_API_KEY = None
    if AGENT_API_KEY and api_key != AGENT_API_KEY:
        return JSONResponse(
            status_code=401,
            content={"error": "Invalid or missing API key"},
        )
    return None


# ── GET /api/screener-hub/themes ──────────────────────────────────────────────

@router.get("/api/screener-hub/themes")
async def screener_hub_themes(request: Request):
    """Return the catalogue of themes with RS metadata and dynamic default."""
    try:
        result = _theme_metadata()
        return JSONResponse(content={
            "status":               "ok",
            "count":                result["count"],
            "themes":               result["themes"],
            "default_theme":        result["default_theme"],
            "default_theme_reason": result["default_theme_reason"],
            "theme_rs_updated_at":  result["theme_rs_updated_at"],
        })
    except Exception as e:
        print(f"[SCREENER_HUB] /themes error: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e), "themes": []},
        )


# ── GET /api/screener-hub ─────────────────────────────────────────────────────

@router.get("/api/screener-hub")
async def screener_hub(
    request: Request,
    tab: str = Query("thematic", description="thematic|social|bottlenecks|watchlist_portfolio"),
    theme: Optional[str] = Query(None, description="theme key (thematic tab only)"),
    category: Optional[str] = Query(None, description="filter by category: Leading|Improving|Weakening|Lagging"),
    scoreMode: Optional[bool] = Query(None, description="enable score column"),
    cocFilter: Optional[bool] = Query(None, description="enable change-on-change filter"),
    # Cache-only post-build filters — camelCase and snake_case both accepted
    marketCapMin: Optional[float] = Query(None, description="min market cap in USD"),
    marketCapMax: Optional[float] = Query(None, description="max market cap in USD"),
    market_cap_min: Optional[float] = Query(None, description="alias for marketCapMin"),
    market_cap_max: Optional[float] = Query(None, description="alias for marketCapMax"),
    minVolume: Optional[float] = Query(None, description="min daily volume"),
    min_volume: Optional[float] = Query(None, description="alias for minVolume"),
    exchange: Optional[str] = Query(None, description="exchange filter e.g. NASDAQ, NYSE"),
):
    tab_norm = (tab or "").lower()
    if tab_norm not in _VALID_TABS:
        return JSONResponse(
            status_code=400,
            content={"error": f"invalid tab '{tab}'. Valid: {sorted(_VALID_TABS)}"},
        )

    # Merge camelCase + snake_case aliases (camelCase wins if both supplied)
    _mcap_min = marketCapMin if marketCapMin is not None else market_cap_min
    _mcap_max = marketCapMax if marketCapMax is not None else market_cap_max
    _min_vol  = minVolume    if minVolume    is not None else min_volume

    try:
        data = await get_screener_hub(
            tab=tab_norm,
            theme=theme,
            category=category,
            score_mode=bool(scoreMode),
            coc_filter=bool(cocFilter),
            market_cap_min=_mcap_min,
            market_cap_max=_mcap_max,
            min_volume=_min_vol,
            exchange=exchange,
        )
        return JSONResponse(content=data)
    except Exception as e:
        print(f"[SCREENER_HUB] /api/screener-hub error: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error", "tab": tab_norm, "theme": theme,
                "rows": [], "error": str(e),
            },
        )


# ── POST /api/admin/screener-hub/rebuild ──────────────────────────────────────

@router.post("/api/admin/screener-hub/rebuild")
async def screener_hub_rebuild(
    request: Request,
    api_key: Optional[str] = Header(None, alias=_AUTH_HEADER),
    # Support both query-param and JSON-body for tab/theme/force so curl callers
    # that pass ?tab=thematic&theme=semiconductors&force=true also work.
    tab_q:   Optional[str]  = Query(None, alias="tab"),
    theme_q: Optional[str]  = Query(None, alias="theme"),
    force_q: Optional[bool] = Query(None, alias="force"),
    body: dict = Body(default_factory=dict),
):
    err = _check_admin_key(api_key)
    if err:
        return err

    b     = body or {}
    tab   = b.get("tab")   or tab_q   or "all"
    theme = b.get("theme") or theme_q or None
    force = bool(b.get("force", force_q if force_q is not None else False))

    if tab not in _VALID_REBUILD_TABS:
        return JSONResponse(
            status_code=400,
            content={"error": f"invalid tab '{tab}'. Valid: {sorted(_VALID_REBUILD_TABS)}"},
        )

    # Single-theme thematic rebuilds are time-boxed tightly.
    # - Universe rebuild (ETF holdings + LKG + FMP screener) completes in ~15–30s.
    # - warm_fundamentals is SKIPPED for single-theme requests: it can take
    #   minutes and is not required for correct row rendering (the screener uses
    #   screener_meta_by_symbol from the snapshot instead of the fundamentals cache).
    # - Background options-flow pipeline jobs can delay response; the timeout
    #   ensures we always return valid JSON instead of an empty body.
    is_single_theme = (tab == "thematic" and bool(theme))
    _TIMEOUT = 45 if is_single_theme else 90

    try:
        async def _do_rebuild():
            u_summary = await rebuild_universe(tab, theme=theme, force=force)
            if is_single_theme:
                w_summary = {"status": "skipped",
                             "reason": "single_theme_rebuild — warm job runs on schedule"}
            else:
                w_summary = await warm_tab_fundamentals(
                    tab, theme=theme, force=force, max_calls=250,
                )
            return u_summary, w_summary

        u_summary, w_summary = await asyncio.wait_for(_do_rebuild(), timeout=_TIMEOUT)

        if is_single_theme:
            tb = (u_summary.get("themes_built") or [{}])
            t  = tb[0] if tb else {}
            return JSONResponse(content={
                "status":                   "ok",
                "tab":                      tab,
                "theme":                    theme,
                "force":                    force,
                "rebuild_completed":        True,
                "cache_written":            bool(t.get("ok")),
                "rows_built":               t.get("symbols_count", 0),
                "dynamic_count":            t.get("dynamic_count", 0),
                "fmp_screener_calls_used":  t.get("fmp_screener_calls_used", 0),
                "fmp_screener_symbols_added": t.get("fmp_screener_symbols_added", 0),
                "message": (
                    f"Single-theme rebuild for {theme!r} completed. "
                    f"{t.get('symbols_count', 0)} symbols written to universe snapshot."
                ),
                "universe":         u_summary,
                "warm_fundamentals": w_summary,
            })

        return JSONResponse(content={
            "status": "ok",
            "tab": tab, "theme": theme, "force": force,
            "universe":          u_summary,
            "warm_fundamentals": w_summary,
        })

    except asyncio.TimeoutError:
        return JSONResponse(content={
            "status":          "accepted",
            "tab":             tab,
            "theme":           theme,
            "force":           force,
            "rebuild_started": True,
            "cache_written":   None,
            "message": (
                f"Rebuild for tab={tab!r}"
                + (f" theme={theme!r}" if theme else "")
                + f" is still running (timeout {_TIMEOUT}s hit). "
                "The universe snapshot will be available when it completes. "
                "Re-query the GET endpoint in 30–60s."
            ),
        })
    except Exception as e:
        print(f"[SCREENER_HUB] rebuild error: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e)},
        )


# ── GET /api/admin/screener-hub/status ────────────────────────────────────────

@router.get("/api/admin/screener-hub/status")
async def screener_hub_status(
    request: Request,
    api_key: Optional[str] = Header(None, alias=_AUTH_HEADER),
):
    err = _check_admin_key(api_key)
    if err:
        return err
    try:
        return JSONResponse(content={"status": "ok", **get_admin_status()})
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e)},
        )


# ── GET /api/admin/screener-hub/audit ─────────────────────────────────────────

@router.get("/api/admin/screener-hub/audit")
async def screener_hub_audit(
    request: Request,
    theme: Optional[str] = Query(None, description="Theme key (e.g. 'semicap_equipment')"),
    api_key: Optional[str] = Header(None, alias=_AUTH_HEADER),
):
    """
    Discovery-engine audit for a single theme.

    Returns:
      total_rows, seed_count, verified_discovery_count, adjacent_discovery_count,
      watch_candidate_count, tier_breakdown, source_breakdown, top_25 rows
      (sorted by theme_relevance_score desc).

    Reads from the Neon snapshot — no rebuild triggered.
    """
    err = _check_admin_key(api_key)
    if err:
        return err
    if not theme:
        return JSONResponse(
            status_code=400,
            content={"error": "?theme= query param required"},
        )
    try:
        # Read directly from the Neon snapshot — zero rebuild risk, shows ALL
        # discovered symbols regardless of quote/fundamentals cache warmth.
        from data.screener_hub_store import get_latest_universe as _get_snap
        snap = _get_snap("thematic", theme)
        if not snap:
            return JSONResponse(
                status_code=404,
                content={"error": f"No snapshot found for theme={theme!r}. Run a rebuild first."},
            )

        snap_meta       = snap.get("metadata") or {}
        sources_by_sym  = snap_meta.get("sources_by_symbol")  or {}
        scr_meta_by_sym = snap_meta.get("screener_meta_by_symbol") or {}
        all_syms        = snap.get("symbols") or []

        # Priority-scan constants (mirrors row-build logic)
        _BUCKET_PRIORITY = [
            ("seed",          lambda s: s in ("static_seed", "manual_include")),
            ("etf_holding",   lambda s: s.startswith("etf:")),
            ("fmp_screener",  lambda s: s.startswith("fmp_screener:") or s == "fmp_peers" or s.startswith("fmp_profile:")),
            ("lkg",           lambda s: s in ("lkg_leaders",) or s.startswith("lkg:")),
        ]

        audit_rows: list[dict] = []
        for sym in all_syms:
            disc_src  = sources_by_sym.get(sym) or []
            scr_meta  = scr_meta_by_sym.get(sym) or {}

            # Resolve primary source bucket
            mem_src = "unknown"
            for bucket, pred in _BUCKET_PRIORITY:
                if any(pred(s) for s in disc_src):
                    mem_src = bucket
                    break

            # Candidate tier: prefer stored override, then infer
            tier_override    = scr_meta.get("candidate_tier_override")
            is_adjacent      = bool(scr_meta.get("_is_adjacent")) or scr_meta.get("industry_tier") in ("adjacent", "weak_adjacent")
            is_weak          = bool(scr_meta.get("_weak_only"))
            candidate_tier   = (
                "core"               if mem_src == "seed" else
                "adjacent_discovery" if is_adjacent and not is_weak else
                "watch_candidate"    if is_weak else
                tier_override or (
                    "verified_discovery" if mem_src in ("etf_holding", "fmp_screener") else
                    "watch_candidate"
                )
            )

            _matched_kws = scr_meta.get("_all_matched_kws") or []
            _stored_reason = scr_meta.get("membership_reason") or ""
            _derived_reason = (
                _stored_reason          if _stored_reason else
                "seed ticker"           if mem_src == "seed" else
                "ETF-holding member"    if mem_src == "etf_holding" else
                (f"FMP screener: {_matched_kws[0]}" if _matched_kws else
                 f"FMP screener: {scr_meta.get('industry_tier','industry match')}")
                                        if mem_src == "fmp_screener" else
                "LKG leaders"           if mem_src == "lkg" else
                ""
            )
            audit_rows.append({
                "symbol":                sym,
                "company_name":          scr_meta.get("company_name") or "",
                "candidate_tier":        candidate_tier,
                "theme_relevance_score": scr_meta.get("theme_relevance_score"),
                "membership_source":     mem_src,
                "membership_reason":     _derived_reason,
                "membership_confidence": scr_meta.get("membership_confidence_override") or (
                    "high" if mem_src == "seed" else
                    "low"  if is_weak else
                    "medium"
                ),
                "matched_keywords":      _matched_kws,
                "industry_tier":         scr_meta.get("industry_tier") or "unknown",
                "theme_role":            (
                    "core"       if mem_src == "seed" else
                    "adjacent"   if is_adjacent else
                    "emerging"   if is_weak else
                    "supporting" if mem_src in ("etf_holding", "fmp_screener") else
                    "emerging"
                ),
                "discovery_sources":     disc_src,
            })

        # Compute tier / source breakdowns
        tier_counts:    dict[str, int] = {}
        source_counts:  dict[str, int] = {}
        src_tag_counts: dict[str, int] = {}
        watch_by_src:   dict[str, int] = {}

        for row in audit_rows:
            tier = row["candidate_tier"]
            tier_counts[tier] = tier_counts.get(tier, 0) + 1
            msrc = row["membership_source"]
            source_counts[msrc] = source_counts.get(msrc, 0) + 1
            for stag in row["discovery_sources"]:
                _k = stag.split(":")[0] if ":" in stag else stag
                src_tag_counts[_k] = src_tag_counts.get(_k, 0) + 1
            if tier == "watch_candidate":
                watch_by_src[msrc] = watch_by_src.get(msrc, 0) + 1

        top_25 = sorted(
            audit_rows,
            key=lambda r: -(r.get("theme_relevance_score") or 0),
        )[:25]

        profile_match_count = sum(
            1 for r in audit_rows
            if any(s.startswith("fmp_profile:") for s in r["discovery_sources"])
        )

        return JSONResponse(content={
            "theme":                     theme,
            "generated_at":              snap.get("generated_at"),
            "total_symbols":             len(all_syms),
            "seed_count":                tier_counts.get("core", 0),
            "verified_discovery_count":  tier_counts.get("verified_discovery", 0),
            "adjacent_discovery_count":  tier_counts.get("adjacent_discovery", 0),
            "watch_candidate_count":     tier_counts.get("watch_candidate", 0),
            "profile_match_count":       profile_match_count,
            "fmp_screener_count":        source_counts.get("fmp_screener", 0),
            "tier_breakdown":            tier_counts,
            "source_breakdown":          source_counts,
            "source_tag_breakdown":      src_tag_counts,
            "watch_candidate_by_source": watch_by_src,
            "top_25":                    top_25,
        })
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "theme": theme, "error": str(e)},
        )


# ── GET /api/admin/screener-hub/thin-themes ───────────────────────────────────

_THIN_SEED_THRESHOLD    = 5   # fewer seeds than this → potentially thin
_THIN_SCREENER_THRESHOLD = 3   # fewer screener symbols than this → thin

@router.get("/api/admin/screener-hub/thin-themes")
async def screener_hub_thin_themes(
    request: Request,
    api_key: Optional[str] = Header(None, alias=_AUTH_HEADER),
):
    """
    List all thematic themes whose last snapshot has insufficient dynamic discovery.

    A theme is "thin" when it satisfies ANY of:
      - fmp_screener_count == 0  AND  seed_count < _THIN_SEED_THRESHOLD
      - No snapshot exists at all
      - fmp_profile_discovery_count == 0  AND  fmp_industries defined in config

    Returns a ranked list (thinnest first) with per-theme breakdown so the
    operator knows which themes to prioritise for rebuild.
    """
    err = _check_admin_key(api_key)
    if err:
        return err
    try:
        from data.screener_hub_store import get_latest_universe as _get_snap
        from services.screener_hub_service import _load_industry_map_config as _load_cfg

        cfg_themes = (_load_cfg().get("themes") or {})
        all_keys   = _theme_keys()

        thin: list[dict]  = []
        ok:   list[dict]  = []

        for key in all_keys:
            snap      = _get_snap("thematic", key)
            theme_cfg = cfg_themes.get(key) or {}
            has_fmp   = bool(theme_cfg.get("fmp_industries"))

            if not snap:
                thin.append({
                    "theme":                   key,
                    "reason":                  "no_snapshot",
                    "seed_count":              len(theme_cfg.get("seed_tickers") or []),
                    "fmp_screener_count":      0,
                    "fmp_profile_count":       0,
                    "total_symbols":           0,
                    "generated_at":            None,
                    "has_fmp_industries":      has_fmp,
                })
                continue

            meta        = snap.get("metadata") or {}
            seed_cnt    = int(meta.get("membership_seed_count") or 0)
            scr_cnt     = int(meta.get("fmp_screener_count")    or 0)
            prof_cnt    = int(meta.get("fmp_profile_discovery_count") or 0)
            total_syms  = len(snap.get("symbols") or [])
            gen_at      = snap.get("generated_at")

            is_thin = (
                (scr_cnt == 0 and seed_cnt < _THIN_SEED_THRESHOLD)
                or (has_fmp and prof_cnt == 0 and scr_cnt < _THIN_SCREENER_THRESHOLD)
            )
            entry = {
                "theme":              key,
                "seed_count":         seed_cnt,
                "fmp_screener_count": scr_cnt,
                "fmp_profile_count":  prof_cnt,
                "total_symbols":      total_syms,
                "generated_at":       gen_at,
                "has_fmp_industries": has_fmp,
            }
            if is_thin:
                entry["reason"] = (
                    "no_screener_no_seeds" if scr_cnt == 0 and seed_cnt < _THIN_SEED_THRESHOLD
                    else "no_profile_discovery"
                )
                thin.append(entry)
            else:
                ok.append(entry)

        thin.sort(key=lambda r: (r["fmp_screener_count"], r["seed_count"]))

        return JSONResponse(content={
            "thin_theme_count": len(thin),
            "ok_theme_count":   len(ok),
            "thin_themes":      thin,
            "ok_themes":        ok,
            "thresholds": {
                "min_seeds":    _THIN_SEED_THRESHOLD,
                "min_screener": _THIN_SCREENER_THRESHOLD,
            },
        })
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e)},
        )


# ── POST /api/admin/screener-hub/rebuild-thin ─────────────────────────────────

@router.post("/api/admin/screener-hub/rebuild-thin")
async def screener_hub_rebuild_thin(
    request: Request,
    api_key: Optional[str] = Header(None, alias=_AUTH_HEADER),
    max_themes: int = Query(10, description="Max thin themes to rebuild in one call"),
    dry_run: bool = Query(False, description="List themes that would be rebuilt without running"),
):
    """
    Sequential rebuild for all thin thematic themes (Source P + FMP screener).

    Identifies thin themes using the same logic as GET /api/admin/screener-hub/thin-themes
    and triggers a full with_fmp_screener=True rebuild for each (up to `max_themes`).

    Use dry_run=true to preview which themes would be rebuilt without triggering anything.
    """
    err = _check_admin_key(api_key)
    if err:
        return err
    try:
        from data.screener_hub_store import get_latest_universe as _get_snap
        from services.screener_hub_service import _load_industry_map_config as _load_cfg

        cfg_themes = (_load_cfg().get("themes") or {})
        all_keys   = _theme_keys()

        targets: list[str] = []
        for key in all_keys:
            snap      = _get_snap("thematic", key)
            theme_cfg = cfg_themes.get(key) or {}
            has_fmp   = bool(theme_cfg.get("fmp_industries"))

            if not has_fmp:
                continue  # no FMP config → nothing for FMP screener/profile to add

            if not snap:
                targets.append(key)
                continue

            meta     = snap.get("metadata") or {}
            seed_cnt = int(meta.get("membership_seed_count") or 0)
            scr_cnt  = int(meta.get("fmp_screener_count")    or 0)
            prof_cnt = int(meta.get("fmp_profile_discovery_count") or 0)

            is_thin = (
                (scr_cnt == 0 and seed_cnt < _THIN_SEED_THRESHOLD)
                or (prof_cnt == 0 and scr_cnt < _THIN_SCREENER_THRESHOLD)
            )
            if is_thin:
                targets.append(key)

        targets = targets[:max_themes]

        if dry_run:
            return JSONResponse(content={
                "dry_run":      True,
                "would_rebuild": targets,
                "count":        len(targets),
            })

        results: list[dict] = []
        for key in targets:
            try:
                summary = await asyncio.wait_for(
                    rebuild_universe("thematic", theme=key, force=True),
                    timeout=60.0,
                )
                bd = (summary.get("breakdown") or {}).get(key) or {}
                results.append({
                    "theme":              key,
                    "status":             "ok",
                    "fmp_screener_count": bd.get("fmp_screener_count", 0),
                    "fmp_profile_count":  bd.get("fmp_profile_discovery_count", 0),
                    "total_symbols":      len(summary.get("symbols", {}).get(key) or []),
                })
            except asyncio.TimeoutError:
                results.append({"theme": key, "status": "timeout"})
            except Exception as re:
                results.append({"theme": key, "status": "error", "error": str(re)})

        ok_count  = sum(1 for r in results if r.get("status") == "ok")
        err_count = len(results) - ok_count
        return JSONResponse(content={
            "rebuilt":    len(results),
            "ok":         ok_count,
            "errors":     err_count,
            "results":    results,
        })
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e)},
        )


# ── POST /api/admin/bottlenecks/refresh ───────────────────────────────────────

@router.post("/api/admin/bottlenecks/refresh")
async def bottlenecks_force_refresh(
    request: Request,
    api_key: Optional[str] = Header(None, alias=_AUTH_HEADER),
):
    """
    Force a full Bottlenecks pipeline rebuild:
      1. Regenerate chain_reaction_weekly_outputs (fresh CR scoring from NODE_REGISTRY)
      2. Rebuild the bottlenecks universe snapshot from the new CR output
      3. Return detailed diagnostics explaining what changed

    This is the correct endpoint for the UI "Refresh" button — it ensures the
    snapshot is genuinely new data, not just a re-read of stale CR data.
    Requires X-API-Key: AGENT_API_KEY header.
    """
    err = _check_admin_key(api_key)
    if err:
        return err

    started_at = datetime.now(timezone.utc).isoformat()
    diag: dict = {"started_at": started_at}

    # ── Step 1: Capture previous snapshot state ────────────────────────────────
    try:
        from data.screener_hub_store import (
            get_latest_chain_reaction_weekly,
            get_latest_universe,
            chain_reaction_weekly_stats,
        )
        prev_cr = get_latest_chain_reaction_weekly(max_age_days=30)
        prev_snap = get_latest_universe("bottlenecks")
        diag["previous_cr_generated_at"] = (prev_cr or {}).get("generated_at")
        diag["previous_cr_symbols_count"] = len((prev_cr or {}).get("symbols") or [])
        diag["previous_snapshot_generated_at"] = (prev_snap or {}).get("generated_at")
        diag["previous_snapshot_symbols_count"] = len((prev_snap or {}).get("symbols") or [])
    except Exception as _pe:
        diag["prev_state_error"] = str(_pe)

    # ── Step 2: Regenerate chain_reaction_weekly_outputs ──────────────────────
    cr_result: dict = {}
    cr_error: Optional[str] = None
    try:
        import json
        from pathlib import Path
        from services.chain_reaction_weekly_service import generate_chain_reaction_weekly

        social_set: set = set()
        options_set: set = set()
        try:
            sp = Path(__file__).parent.parent / "data" / "x_consensus_weekly.json"
            if sp.exists():
                d = json.loads(sp.read_text())
                for item in (d.get("top_tickers") or []):
                    sym = item.get("symbol") if isinstance(item, dict) else None
                    if sym:
                        social_set.add(str(sym).upper())
        except Exception:
            pass
        try:
            for fname in ["options_master_lkg_v1.json", "options_lkg_v1_large_cap.json", "options_lkg_v1_small_cap.json"]:
                op = Path(__file__).parent.parent / "data" / fname
                if op.exists():
                    d = json.loads(op.read_text())
                    for t in (d.get("tickers") or []):
                        sym = t.get("ticker") if isinstance(t, dict) else None
                        if sym:
                            options_set.add(str(sym).upper())
        except Exception:
            pass

        cr_result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: generate_chain_reaction_weekly(
                social_symbols=social_set,
                options_symbols=options_set,
            ),
        )
        print(f"[BOTTLENECKS_REFRESH] CR generation: {cr_result.get('status')} rows={cr_result.get('rows_written')}")
    except Exception as _cre:
        cr_error = str(_cre)
        print(f"[BOTTLENECKS_REFRESH] CR generation error: {_cre}")

    diag["cr_generation"] = {
        "status":      cr_result.get("status") if cr_result else "error",
        "error":       cr_error or cr_result.get("error"),
        "rows_written": cr_result.get("rows_written"),
        "generated_at": cr_result.get("generated_at"),
        "market_cap_buckets": cr_result.get("market_cap_buckets"),
    }

    # Abort if CR generation returned an error result (don't overwrite good LKG with bad)
    if cr_result.get("status") == "error" or cr_error:
        diag["aborted"] = True
        diag["abort_reason"] = cr_error or cr_result.get("error") or "CR generation returned error status"
        return JSONResponse(
            status_code=500,
            content={
                "status":      "error",
                "message":     "Bottlenecks refresh aborted — CR generation failed. LKG snapshot preserved.",
                "diagnostics": diag,
            },
        )

    # ── Step 3: Rebuild universe snapshot from fresh CR data ──────────────────
    rebuild_result: dict = {}
    try:
        rebuild_result = await asyncio.wait_for(
            rebuild_universe("bottlenecks", force=True),
            timeout=60,
        )
    except asyncio.TimeoutError:
        rebuild_result = {"status": "timeout", "error": "rebuild_universe timed out after 60s"}
    except Exception as _re:
        rebuild_result = {"status": "error", "error": str(_re)}

    diag["universe_rebuild"] = rebuild_result

    # ── Step 4: Compare new vs previous snapshot ──────────────────────────────
    new_symbols:  list = []
    prev_symbols: list = []
    snapshot_changed = True
    try:
        from data.screener_hub_store import get_latest_universe
        new_snap = get_latest_universe("bottlenecks")
        new_symbols  = list((new_snap or {}).get("symbols") or [])
        prev_symbols = list((prev_snap or {}).get("symbols") or [])  # type: ignore[union-attr]
        diag["new_snapshot_generated_at"]  = (new_snap or {}).get("generated_at")
        diag["new_snapshot_symbols_count"] = len(new_symbols)
        added   = [s for s in new_symbols if s not in set(prev_symbols)]
        removed = [s for s in prev_symbols if s not in set(new_symbols)]
        diag["symbols_added"]              = added
        diag["symbols_removed"]            = removed
        diag["symbols_net_change"]         = len(new_symbols) - len(prev_symbols)
        snapshot_changed                   = bool(added or removed or new_symbols != prev_symbols)
        diag["snapshot_genuinely_changed"] = snapshot_changed
    except Exception as _ne:
        diag["new_state_error"] = str(_ne)

    # ── Step 5: Build cross-theme visible top snapshot ─────────────────────────
    # This is the data the Chain Reaction / Bottlenecks page should display.
    # GET /api/bottlenecks/current reads from the same source.
    visible_diag: dict = {}
    try:
        from services.chain_reaction_weekly_service import build_cross_theme_top
        # Capture the previous visible tickers before the refresh so tickers_changed is accurate
        _prev_vis = build_cross_theme_top(limit=30, max_age_days=30)
        prev_visible_tickers = _prev_vis.get("visible_tickers") or []
        vis_result = build_cross_theme_top(limit=30, max_age_days=10,
                                           prev_visible_tickers=prev_visible_tickers)
        if vis_result.get("status") == "ok":
            visible_diag = {
                "visible_snapshot_id":           vis_result["visible_snapshot_id"],
                "visible_generated_at":          vis_result["visible_generated_at"],
                "visible_count":                 vis_result["visible_count"],
                "visible_tickers":               vis_result["visible_tickers"],
                "universe_count":                vis_result["universe_count"],
                "universe_only_tickers":         vis_result["universe_only_tickers"],
                "overlap_count":                 vis_result["overlap_count"],
                "selected_from_universe_count":  vis_result["selected_from_universe_count"],
                "gem_candidates_with_reasons":   vis_result["gem_candidates_with_reasons"],
                "diversity_gate_result":         vis_result["diversity_gate_result"],
                "themes_in_visible":             vis_result["themes_in_visible"],
                "market_cap_buckets_in_visible": vis_result["market_cap_buckets_in_visible"],
                "tickers_changed":               vis_result["tickers_changed"],
                "metadata_refreshed_only":       vis_result["metadata_refreshed_only"],
            }
        else:
            visible_diag = {"error": vis_result.get("error", "build_cross_theme_top returned non-ok status")}
    except Exception as _ve:
        visible_diag = {"error": str(_ve)}

    diag["visible_snapshot"] = visible_diag

    # ── Step 6: Rebuild strategy screener snapshot from new CR data ───────────
    # This writes to screener_snapshots + screener_reports so that
    # GET /api/strategy-screener/latest serves the new diverse candidates
    # with full ReportPanel-compatible payloads for every visible row.
    screener_snap_diag: dict = {}
    try:
        from services.playbook.strategy_screener.screener_service import generate_snapshot_from_cr
        screener_snap = await asyncio.wait_for(
            generate_snapshot_from_cr(manual_override=True),
            timeout=120,
        )
        if screener_snap and screener_snap.get("status") == "complete":
            screener_snap_diag = {
                "status":          "complete",
                "snapshot_id":     screener_snap.get("snapshot_id"),
                "results_count":   screener_snap.get("results_count"),
                "generation_notes": screener_snap.get("generation_notes"),
            }
        else:
            screener_snap_diag = {
                "status":  "empty" if not screener_snap else screener_snap.get("status", "unknown"),
                "message": "generate_snapshot_from_cr returned no/incomplete snapshot",
            }
    except asyncio.TimeoutError:
        screener_snap_diag = {"status": "timeout", "error": "generate_snapshot_from_cr timed out after 120s"}
    except Exception as _sse:
        screener_snap_diag = {"status": "error", "error": str(_sse)}

    diag["screener_snapshot"] = screener_snap_diag

    finished_at = datetime.now(timezone.utc).isoformat()
    diag["finished_at"] = finished_at

    n_visible  = visible_diag.get("visible_count", "?")
    n_gems     = (visible_diag.get("diversity_gate_result") or {}).get("hidden_gems_achieved", 0)
    n_screener = screener_snap_diag.get("results_count", "?")
    return JSONResponse(content={
        "status":  "ok",
        "message": (
            f"Bottlenecks refresh complete. "
            f"Universe: {diag.get('new_snapshot_symbols_count', '?')} symbols. "
            f"Visible top: {n_visible} rows ({n_gems} Phase-6 hidden gems). "
            f"Screener snapshot: {n_screener} candidates with full reports. "
            + (f"Net universe change: {diag.get('symbols_net_change', 0):+d}. " if diag.get('symbols_net_change') is not None else "")
            + ("Universe genuinely updated." if snapshot_changed else "Universe symbols unchanged (new timestamp).")
        ),
        "snapshot_changed":    snapshot_changed,
        "visible_tickers":     visible_diag.get("visible_tickers", []),
        "visible_count":       n_visible,
        "screener_snapshot_id": screener_snap_diag.get("snapshot_id"),
        "screener_results_count": n_screener,
        "visible_snapshot":    visible_diag,
        "diagnostics":         diag,
    })


# ── POST /api/admin/bottlenecks/research-anchor ───────────────────────────────
# Run monthly LLM research for ONE overlay anchor (SPCX, OPENAI, or ANTHROPIC).
# Safe to call on demand; skips anchors that were researched within 30 days
# unless force=true.  Never runs LLM on page load or as part of weekly job.

@router.post("/api/admin/bottlenecks/research-anchor")
async def bottlenecks_research_anchor(
    request: Request,
    api_key: Optional[str] = Header(None, alias=_AUTH_HEADER),
    anchor:  str  = Query(..., description="Anchor key: SPCX | OPENAI | ANTHROPIC"),
    force:   bool = Query(False, description="Skip the 30-day freshness check"),
):
    """
    Trigger web-search research for a single overlay anchor.

    - One OpenAI Responses API call with forced web_search_preview.
    - Old approved rows are quarantined (→ pending_review) before re-running.
    - Results written to anchor_supply_chain_research_nodes (Neon).
    - Weekly job and page-load endpoints do NOT call any LLM.
    - Freshness gate: skips if researched within 30 days (override with force=true).
    - Valid anchors: SPCX, OPENAI, ANTHROPIC.

    Requires X-API-Key header.
    """
    err = _check_admin_key(api_key)
    if err:
        return err

    return JSONResponse(content={
        "status": "disabled_static_curated_mode",
        "message": (
            "LLM/web-search anchor research is disabled. "
            "Anchor maps are now served from the static curated data in "
            "services/playbook/curated_anchor_bottlenecks.py. "
            "Use GET /api/bottlenecks/anchor/{anchor_key} for curated rows."
        ),
    })


# ── POST /api/admin/bottlenecks/revalidate-anchor ─────────────────────────────
# Re-run validation gates on existing approved nodes WITHOUT calling the LLM.
# Use after a research run to tighten gates or when manually reviewing quality.

@router.post("/api/admin/bottlenecks/revalidate-anchor")
async def bottlenecks_revalidate_anchor(
    request: Request,
    api_key: Optional[str] = Header(None, alias=_AUTH_HEADER),
    anchor:  str  = Query(..., description="Anchor key: SPCX | OPENAI | ANTHROPIC"),
):
    """
    Re-run all validation gates on existing approved nodes (no LLM call).

    Checks each approved row:
    - ticker_validated: FMP live API fallback for cache misses (hard gate)
    - evidence_hedged: reject speculative language like 'may supply'
    - evidence_anchor_keyword: evidence must mention the anchor
    - source_url_reachable: at least one URL must return HTTP 2xx/3xx

    Rows failing any gate → research_status = pending_review.
    Returns per-row report with gate results and a recommend_rerun flag
    (True when approved count < 8 after revalidation).

    Requires X-API-Key header.
    """
    err = _check_admin_key(api_key)
    if err:
        return err

    return JSONResponse(content={
        "status": "disabled_static_curated_mode",
        "message": (
            "LLM/web-search anchor revalidation is disabled. "
            "Anchor maps are now served from static curated data. "
            "Use GET /api/bottlenecks/anchor/{anchor_key} for curated rows."
        ),
    })


# ── POST /api/admin/bottlenecks/research-anchors-monthly ──────────────────────
# Run monthly LLM research for ALL configured overlay anchors, sequentially.

@router.post("/api/admin/bottlenecks/research-anchors-monthly")
async def bottlenecks_research_monthly(
    request: Request,
    api_key: Optional[str] = Header(None, alias=_AUTH_HEADER),
    force:   bool = Query(False, description="Skip freshness check for all anchors"),
):
    """
    Run the monthly web-search research pipeline for all overlay anchors
    (SPCX, OPENAI, ANTHROPIC) one at a time, sequentially.

    - Anchors still fresh (researched within 30 days) are skipped unless force=true.
    - Results are written to anchor_supply_chain_research_nodes (Neon).
    - One OpenAI Responses API call per stale anchor; brief pause between calls.
    - Old approved rows are quarantined before each re-run.

    Requires X-API-Key header.
    """
    err = _check_admin_key(api_key)
    if err:
        return err

    return JSONResponse(content={
        "status": "disabled_static_curated_mode",
        "message": (
            "Monthly LLM anchor research is disabled. "
            "Anchor maps are now served from static curated data. "
            "Use GET /api/bottlenecks/anchors for the curated anchor list."
        ),
    })


# ── GET /api/admin/bottlenecks/research-status ────────────────────────────────

@router.get("/api/admin/bottlenecks/research-status")
async def bottlenecks_research_status(
    request: Request,
    api_key: Optional[str] = Header(None, alias=_AUTH_HEADER),
):
    """
    Return the research status for all overlay anchors:
    node count, last researched timestamp, next research due timestamp,
    and whether each anchor needs a refresh.

    Requires X-API-Key header.
    """
    err = _check_admin_key(api_key)
    if err:
        return err

    return JSONResponse(content={
        "status": "disabled_static_curated_mode",
        "message": (
            "LLM anchor research status is disabled. "
            "Use GET /api/bottlenecks/anchors for curated anchor status."
        ),
    })


# ── GET /api/debug/bottlenecks-snapshot ───────────────────────────────────────

@router.get("/api/debug/bottlenecks-snapshot")
async def debug_bottlenecks_snapshot(
    request: Request,
    api_key: Optional[str] = Header(None, alias=_AUTH_HEADER),
):
    """
    Read-only diagnostics for the Bottlenecks page snapshot pipeline.

    Returns:
      - snapshot age and generated_at
      - CR weekly output age, row count, market cap bucket distribution
      - scheduler next expected run time
      - NODE_REGISTRY size
      - last refresh status
    """
    err = _check_admin_key(api_key)
    if err:
        return err

    now_utc = datetime.now(timezone.utc)
    diag: dict = {"as_of": now_utc.isoformat()}

    # ── Universe snapshot ──────────────────────────────────────────────────────
    try:
        from data.screener_hub_store import get_latest_universe, chain_reaction_weekly_stats
        snap = get_latest_universe("bottlenecks")
        if snap:
            gen_at = snap.get("generated_at")
            try:
                gen_dt = datetime.fromisoformat(str(gen_at).replace("Z", "+00:00")) if gen_at else None
                age_hours = round((now_utc - gen_dt).total_seconds() / 3600, 1) if gen_dt else None
            except Exception:
                age_hours = None
            diag["universe_snapshot"] = {
                "generated_at":  gen_at,
                "age_hours":     age_hours,
                "symbols_count": len(snap.get("symbols") or []),
                "source":        snap.get("source"),
                "status":        snap.get("status"),
            }
        else:
            diag["universe_snapshot"] = {"status": "missing"}
    except Exception as _e:
        diag["universe_snapshot"] = {"error": str(_e)}

    # ── Chain Reaction weekly output ───────────────────────────────────────────
    try:
        from data.screener_hub_store import (
            get_latest_chain_reaction_weekly,
            chain_reaction_weekly_stats,
        )
        cr_stats = chain_reaction_weekly_stats()
        cr_row = get_latest_chain_reaction_weekly(max_age_days=30)
        if cr_row:
            cr_gen_at = cr_row.get("generated_at")
            try:
                cr_gen_dt = datetime.fromisoformat(str(cr_gen_at).replace("Z", "+00:00")) if cr_gen_at else None
                cr_age_days = round((now_utc - cr_gen_dt).total_seconds() / 86400, 1) if cr_gen_dt else None
            except Exception:
                cr_age_days = None

            # Compute bucket distribution from CR rows
            cr_buckets: dict[str, int] = {}
            for r in (cr_row.get("rows") or []):
                b = r.get("marketCapBucket") or "unknown"
                cr_buckets[b] = cr_buckets.get(b, 0) + 1
            if not cr_buckets:
                cr_buckets = (cr_row.get("metadata") or {}).get("market_cap_buckets") or {}

            diag["cr_weekly_output"] = {
                "generated_at":      cr_gen_at,
                "age_days":          cr_age_days,
                "week_start":        cr_row.get("week_start"),
                "source_version":    cr_row.get("source_version"),
                "symbols_count":     len(cr_row.get("symbols") or []),
                "within_10d_window": (cr_age_days or 999) <= 10,
                "within_7d_fresh":   (cr_age_days or 999) <= 7,
                "market_cap_buckets": cr_buckets,
                "total_rows_in_db":  cr_stats.get("total_rows"),
            }
        else:
            diag["cr_weekly_output"] = {"status": "no_row_within_30_days", "total_rows_in_db": cr_stats.get("total_rows")}
    except Exception as _e:
        diag["cr_weekly_output"] = {"error": str(_e)}

    # ── NODE_REGISTRY stats ────────────────────────────────────────────────────
    try:
        from services.playbook.supply_chain_graph import NODE_REGISTRY
        themes_seen: dict[str, int] = {}
        for node in NODE_REGISTRY.values():
            for t in (node.get("themes") or []):
                themes_seen[t] = themes_seen.get(t, 0) + 1
        diag["node_registry"] = {
            "total_nodes": len(NODE_REGISTRY),
            "us_nodes":    sum(1 for n in NODE_REGISTRY.values() if n.get("country") == "US"),
            "theme_distribution": dict(sorted(themes_seen.items(), key=lambda x: -x[1])[:15]),
        }
    except Exception as _e:
        diag["node_registry"] = {"error": str(_e)}

    # ── Scheduler next expected run ────────────────────────────────────────────
    try:
        from zoneinfo import ZoneInfo
        _et = ZoneInfo("America/New_York")
        now_et = now_utc.astimezone(_et)
        # Next Sunday 02:15 ET for chain_reaction_dynamic
        days_until_sunday = (6 - now_et.weekday()) % 7 or 7
        from datetime import timedelta
        next_sunday = (now_et + timedelta(days=days_until_sunday)).replace(
            hour=2, minute=15, second=0, microsecond=0
        )
        next_bottlenecks_warm = next_sunday.replace(hour=3, minute=15)
        diag["scheduler"] = {
            "next_chain_reaction_dynamic_ET": next_sunday.isoformat(),
            "next_bottlenecks_warm_ET":       next_bottlenecks_warm.isoformat(),
            "note": "Jobs fire within 5-minute window of target time. Self-healing: bottlenecks_warm generates fresh CR data if chain_reaction_dynamic was missed.",
        }
    except Exception as _e:
        diag["scheduler"] = {"error": str(_e)}

    # ── Refresh endpoints ──────────────────────────────────────────────────────
    diag["refresh_endpoints"] = {
        "force_full_rebuild":     "POST /api/admin/bottlenecks/refresh  (X-API-Key required)",
        "universe_only_rebuild":  "POST /api/admin/screener-hub/rebuild?tab=bottlenecks  (X-API-Key required)",
        "visible_top_read":       "GET  /api/bottlenecks/current  (public; default limit=20)",
    }

    return JSONResponse(content={"status": "ok", **diag})


# ── GET /api/bottlenecks/current ──────────────────────────────────────────────
#
# Public read endpoint — returns the cross-theme diverse top-N from the latest
# chain_reaction_weekly_outputs run.  This is the correct data source for the
# Chain Reaction / Bottlenecks page; it replaces /api/strategy-screener/latest
# which is regime-locked to the current AI-hardware / semicap cohort.
#
# Query params:
#   limit       int  1–110   default 20   — how many rows to return
#   full        bool         default false — if true, returns full universe (all rows)
#   max_age_days int 1–30    default 10   — reject CR data older than this many days
#   require_gems int 0–5     default 2    — min Phase-6 hidden gems in result set
#   require_small_mid int 0–10 default 5 — min <$20B names in result set
#   require_themes int 0–8   default 4   — min distinct themes in result set
#   diagnostics bool         default false — if true, include gem_candidates_with_reasons etc.

@router.get("/api/bottlenecks/current")
async def bottlenecks_current(
    request: Request,
    limit:             int  = Query(default=20,   ge=1,  le=110,  description="Rows to return (1–110)"),
    full:              bool = Query(default=False,                 description="Return all rows in universe"),
    max_age_days:      int  = Query(default=10,   ge=1,  le=30,   description="Reject CR data older than N days"),
    require_gems:      int  = Query(default=2,    ge=0,  le=5,    description="Min Phase-6 hidden gems in result"),
    require_small_mid: int  = Query(default=5,    ge=0,  le=10,   description="Min <$20B names in result"),
    require_themes:    int  = Query(default=4,    ge=0,  le=8,    description="Min distinct themes in result"),
    diagnostics:       bool = Query(default=False,                description="Include per-gem diagnostics"),
):
    """
    Cross-theme diverse top-N from chain_reaction_weekly_outputs.

    Unlike /api/strategy-screener/latest (which is regime-locked to the current
    AI-hardware / semicap discovery cohort), this endpoint reads directly from
    the CR weekly scoring run and applies a 3-criterion diversity gate:

      • ≥ require_themes distinct primary themes
      • ≥ require_small_mid names with market_cap < $20B
      • ≥ require_gems Phase 6 hidden-gem tickers

    Set full=true to bypass the limit and receive the complete scored universe.
    Set diagnostics=true to include per-gem inclusion/exclusion reasoning.

    status values:
      "ok"    — result ready
      "error" — no fresh CR data; run POST /api/admin/bottlenecks/refresh
    """
    try:
        from services.chain_reaction_weekly_service import build_cross_theme_top

        effective_limit = 110 if full else limit
        result = build_cross_theme_top(
            limit=effective_limit,
            max_age_days=max_age_days,
            require_themes=require_themes,
            require_small_mid=require_small_mid,
            require_gems=require_gems,
        )
    except Exception as _e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(_e)},
        )

    if result.get("status") == "error":
        return JSONResponse(status_code=404, content=result)

    # Always include these top-level fields
    response: dict = {
        "status":               result["status"],
        "visible_snapshot_id":  result["visible_snapshot_id"],
        "visible_generated_at": result["visible_generated_at"],
        "visible_count":        result["visible_count"],
        "visible_tickers":      result["visible_tickers"],
        "universe_count":       result["universe_count"],
        "week_start":           result["week_start"],
        "source_version":       result["source_version"],
        "themes_in_visible":    result["themes_in_visible"],
        "market_cap_buckets_in_visible": result["market_cap_buckets_in_visible"],
        "diversity_gate_result": result["diversity_gate_result"],
        "rows":                 result["rows"],
        "full_universe":        full,
        "limit_applied":        effective_limit,
        # Guidance for frontend migration
        "data_source":          "chain_reaction_weekly_outputs",
        "note": (
            "This endpoint supersedes /api/strategy-screener/latest for the "
            "Chain Reaction / Bottlenecks page. It applies a cross-theme "
            "diversity gate to surface names from nuclear, rare earth, battery, "
            "defense, and semiconductor-niche themes alongside the semicap core."
        ),
    }

    # Optional diagnostics (per-gem inclusion reasoning + universe diff)
    if diagnostics:
        response["gem_candidates_with_reasons"]  = result["gem_candidates_with_reasons"]
        response["universe_tickers"]             = result["universe_tickers"]
        response["universe_only_tickers"]        = result["universe_only_tickers"]
        response["overlap_count"]                = result["overlap_count"]
        response["selected_from_universe_count"] = result["selected_from_universe_count"]

    return JSONResponse(content=response)


# ── GET /api/bottlenecks/multi-anchor ─────────────────────────────────────────
#
# Runs the existing build_anchor_top() (which internally calls the existing
# build_cross_theme_top()) for each of the six market-leading anchors,
# sequentially, and returns a grouped response.
#
# The existing GET /api/bottlenecks/current is completely unchanged.
# No new scoring, ranking, or prompt logic was introduced.
# Anchors are processed in this fixed order:
#   NVDA → SPCX → ANTHROPIC → OPENAI → TSM → GOOG

@router.get("/api/bottlenecks/multi-anchor")
async def bottlenecks_multi_anchor(
    request: Request,
    limit:        int = Query(default=20, ge=1, le=110, description="Max rows per anchor"),
    max_age_days: int = Query(default=10, ge=1, le=30,  description="Reject CR data older than N days"),
):
    """
    Multi-anchor Bottlenecks — runs the existing Bottlenecks supply-chain
    scoring for six market-leading anchors one at a time (sequential, not
    parallel) and returns a grouped result.

    Same source data as GET /api/bottlenecks/current.
    Same scoring, ranking, and output shape per anchor group.
    GET /api/bottlenecks/current is unchanged.

    Anchors (processed in order):
      1. NVDA  / NVIDIA
      2. SPCX  / SpaceX
      3. ANTHROPIC / Anthropic
      4. OPENAI / OpenAI
      5. TSM   / Taiwan Semiconductor Manufacturing Company
      6. GOOG  / Google / Alphabet

    Partial failures: if one anchor fails the others still appear;
    the failed anchor is listed under `partial_failures`.

    status values per anchor:
      "success" — rows ready
      "error"   — no matching data (check partial_failures)

    Top-level status:
      "ok"           — all anchors succeeded
      "partial"      — some anchors succeeded, some failed
      "error"        — all anchors failed
    """
    import time as _time
    from datetime import datetime, timezone
    from services.chain_reaction_weekly_service import (
        build_anchor_top,
        MULTI_ANCHOR_CONFIGS,
    )

    start_total = _time.monotonic()
    anchors_result: list[dict] = []
    partial_failures: list[dict] = []

    for cfg in MULTI_ANCHOR_CONFIGS:
        anchor      = cfg["anchor"]
        anchor_name = cfg["anchor_name"]
        t0 = _time.monotonic()
        print(f"[MULTI_ANCHOR] start  anchor={anchor!r}  ({anchor_name})")

        try:
            result = build_anchor_top(
                anchor=anchor,
                anchor_name=anchor_name,
                anchor_themes=cfg.get("anchor_themes", []),
                limit=limit,
                max_age_days=max_age_days,
            )
            elapsed = round(_time.monotonic() - t0, 2)
            result_status = result.get("status", "error")

            if result_status == "needs_research":
                # Expected state for overlay anchors not yet researched — not an error
                row_count = 0
                print(
                    f"[MULTI_ANCHOR] needs_research  anchor={anchor!r}  "
                    f"elapsed={elapsed}s"
                )
                anchors_result.append({
                    "anchor":            anchor,
                    "anchor_name":       anchor_name,
                    "anchor_themes":     cfg.get("anchor_themes", []),
                    "is_overlay_anchor": cfg.get("is_overlay_anchor", False),
                    "status":            "needs_research",
                    "elapsed_s":         elapsed,
                    "data":              result,
                })
            elif result_status == "error":
                raise ValueError(result.get("error", "unknown error from build_anchor_top"))
            else:
                row_count = result.get("visible_count", 0)
                print(
                    f"[MULTI_ANCHOR] ok     anchor={anchor!r}  "
                    f"rows={row_count}  elapsed={elapsed}s"
                )
                anchors_result.append({
                    "anchor":            anchor,
                    "anchor_name":       anchor_name,
                    "anchor_themes":     cfg.get("anchor_themes", []),
                    "is_overlay_anchor": cfg.get("is_overlay_anchor", False),
                    "status":            "success",
                    "elapsed_s":         elapsed,
                    "data":              result,
                })

        except Exception as _exc:
            elapsed = round(_time.monotonic() - t0, 2)
            print(
                f"[MULTI_ANCHOR] fail   anchor={anchor!r}  "
                f"elapsed={elapsed}s  error={_exc}"
            )
            partial_failures.append({
                "anchor":            anchor,
                "anchor_name":       anchor_name,
                "is_overlay_anchor": cfg.get("is_overlay_anchor", False),
                "status":            "error",
                "error":             str(_exc),
                "elapsed_s":         elapsed,
            })

    total_elapsed = round(_time.monotonic() - start_total, 2)
    n_ok   = len(anchors_result)
    n_fail = len(partial_failures)
    print(
        f"[MULTI_ANCHOR] complete  ok={n_ok}  failed={n_fail}  "
        f"total_elapsed={total_elapsed}s"
    )

    if n_ok == 0:
        top_status = "error"
    elif n_fail > 0:
        top_status = "partial"
    else:
        top_status = "ok"

    return JSONResponse(content={
        "status":          top_status,
        "anchors":         anchors_result,
        "generated_at":    datetime.now(timezone.utc).isoformat(),
        "total_elapsed_s": total_elapsed,
        "partial_failures": partial_failures,
        "anchor_order":    [cfg["anchor"] for cfg in MULTI_ANCHOR_CONFIGS],
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Saved Screens — Parts 1–5
# ═══════════════════════════════════════════════════════════════════════════════

from typing import Any, Dict, List
from pydantic import BaseModel, Field


# ── User identity helper (same pattern as chart_radar_router._get_user_id) ───
def _get_user_id(request: Request) -> str:
    """
    Resolve user_id for the request.

    Priority:
      1. request.state.user_id (middleware, if ever re-enabled)
      2. Authorization: Bearer <JWT> → payload["sub"]
      3. "default" — unauthenticated / local-dev fallback

    Matches the pattern in services/chart_radar_router.py.
    """
    uid = getattr(request.state, "user_id", None)
    if uid:
        return str(uid)
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth[7:]
        try:
            from auth import verify_token
            payload = verify_token(token)
            sub = payload.get("sub")
            if sub:
                return str(sub)
        except Exception:
            pass
    return "default"


# ── Pydantic model for POST body ─────────────────────────────────────────────

class SaveScreenRequest(BaseModel):
    model_config = {"extra": "allow"}

    name:         Optional[str]        = None
    tab:          str
    theme_key:    Optional[str]        = None
    theme_label:  Optional[str]        = None
    filters:      Dict[str, Any]       = Field(default_factory=dict)
    query_params: Dict[str, Any]       = Field(default_factory=dict)
    rows:         List[Dict[str, Any]] = Field(default_factory=list)
    metadata:     Dict[str, Any]       = Field(default_factory=dict)


class DailyAutoSaveRequest(BaseModel):
    model_config = {"extra": "allow"}

    tab:           str
    theme_key:     Optional[str]        = None
    theme_label:   Optional[str]        = None
    filters:       Dict[str, Any]       = Field(default_factory=dict)
    query_params:  Dict[str, Any]       = Field(default_factory=dict)
    rows:          List[Dict[str, Any]] = Field(default_factory=list)
    metadata:      Dict[str, Any]       = Field(default_factory=dict)
    snapshot_date: Optional[str]        = None  # YYYY-MM-DD; defaults to today


# ── POST /api/screener-hub/saved-screens ─────────────────────────────────────

@router.post("/api/screener-hub/saved-screens")
async def save_screen(request: Request, body: SaveScreenRequest):
    """
    Save the current visible Screener Hub results as a dated snapshot.
    Does NOT re-run the screener or call any provider.
    """
    from data.screener_hub_store import create_saved_screen
    user_id = _get_user_id(request)
    try:
        result = create_saved_screen(
            user_id=user_id,
            name=body.name or "",
            tab=body.tab,
            theme_key=body.theme_key,
            theme_label=body.theme_label,
            filters_json=body.filters,
            query_params_json=body.query_params,
            metadata_json=body.metadata,
            rows=body.rows,
            save_type="manual",
        )
        if result is None:
            return JSONResponse(
                status_code=500,
                content={"error": "Failed to save screen — check server logs"},
            )
        return JSONResponse(status_code=201, content={"status": "created", **result})
    except Exception as e:
        print(f"[SAVED_SCREENS] POST error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── POST /api/screener-hub/saved-screens/daily-auto ──────────────────────────
# Registered BEFORE /{screen_id} (and before GET list) so FastAPI resolves
# "daily-auto" as a fixed path, not as a screen UUID.

@router.post("/api/screener-hub/saved-screens/daily-auto")
async def daily_auto_save(request: Request, body: DailyAutoSaveRequest):
    """
    Upsert a daily automatic snapshot for user/tab/theme_key/date.

    Rules:
      • save_type = 'daily_auto'
      • snapshot_date defaults to today (server date UTC)
      • expires_at = snapshot_date + 60 days
      • If a daily_auto screen already exists for same user+tab+theme_key+date:
          delete its rows, update header, re-insert rows  (no duplicate)
      • After successful upsert, runs retention cleanup (delete > 60 day old
        daily_auto screens for this user) — no provider calls.
    """
    from data.screener_hub_store import upsert_daily_auto_screen, cleanup_expired_saved_screens
    user_id = _get_user_id(request)
    try:
        result = upsert_daily_auto_screen(
            user_id=user_id,
            tab=body.tab,
            theme_key=body.theme_key,
            theme_label=body.theme_label,
            filters_json=body.filters,
            query_params_json=body.query_params,
            metadata_json=body.metadata,
            rows=body.rows,
            snapshot_date_str=body.snapshot_date,
        )
        if result is None:
            return JSONResponse(
                status_code=500,
                content={"error": "Failed to upsert daily-auto screen — check server logs"},
            )
        # Retention cleanup — fire-and-forget style, don't fail the response
        try:
            cleanup_expired_saved_screens(user_id=user_id)
        except Exception as ce:
            print(f"[SAVED_SCREENS] retention cleanup warning: {ce}")
        return JSONResponse(
            status_code=200 if result.get("action") == "updated" else 201,
            content={"status": result["action"], **result},
        )
    except Exception as e:
        print(f"[SAVED_SCREENS] POST daily-auto error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /api/screener-hub/saved-screens ──────────────────────────────────────

@router.get("/api/screener-hub/saved-screens")
async def list_saved_screens_endpoint(
    request:       Request,
    tab:           Optional[str] = Query(None),
    theme_key:     Optional[str] = Query(None),
    save_type:     Optional[str] = Query(None, description="'manual' | 'daily_auto' | omit for all"),
    lookback_days: Optional[int] = Query(None, ge=1, le=365, description="Restrict to last N days; default 60 when save_type=daily_auto"),
    limit:         int           = Query(100, ge=1, le=500),
):
    """Return saved screens list (newest first) for the authenticated user."""
    from data.screener_hub_store import list_saved_screens
    user_id = _get_user_id(request)
    try:
        screens = list_saved_screens(
            user_id=user_id,
            tab=tab,
            theme_key=theme_key,
            save_type=save_type,
            lookback_days=lookback_days,
            limit=limit,
        )
        return JSONResponse(content={
            "status": "ok",
            "count":   len(screens),
            "screens": screens,
        })
    except Exception as e:
        print(f"[SAVED_SCREENS] GET list error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /api/screener-hub/saved-screens/insights ─────────────────────────────
# IMPORTANT: registered BEFORE /{id} so "insights" is not treated as a UUID.

@router.get("/api/screener-hub/saved-screens/insights")
async def saved_screens_insights(
    request:       Request,
    tab:           Optional[str] = Query(None),
    theme_key:     Optional[str] = Query(None),
    save_type:     Optional[str] = Query(None, description="'manual' | 'daily_auto' | omit for all"),
    lookback_days: Optional[int] = Query(None, ge=1, le=365, description="Default: 60 for daily_auto, 90 otherwise"),
):
    """
    Cross-screen insights: recurring tickers, performance, theme frequency,
    week-over-week persistence.  Read-only — no provider calls.

    When save_type=daily_auto, lookback_days defaults to 60.
    """
    from data.screener_hub_store import get_saved_screen_insights
    user_id = _get_user_id(request)
    # Resolve effective lookback: 60 for daily_auto, 90 for manual/all
    effective_lookback = lookback_days
    if effective_lookback is None:
        effective_lookback = 60 if save_type == "daily_auto" else 90
    try:
        insights = get_saved_screen_insights(
            user_id=user_id,
            tab=tab,
            theme_key=theme_key,
            save_type=save_type,
            lookback_days=effective_lookback,
        )
        return JSONResponse(content={"status": "ok", **insights})
    except Exception as e:
        print(f"[SAVED_SCREENS] insights error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /api/screener-hub/saved-screens/{id} ─────────────────────────────────

@router.get("/api/screener-hub/saved-screens/{screen_id}")
async def get_saved_screen_endpoint(request: Request, screen_id: str):
    """Return a saved screen with all its rows. No provider calls."""
    from data.screener_hub_store import get_saved_screen
    user_id = _get_user_id(request)
    try:
        screen = get_saved_screen(user_id=user_id, screen_id=screen_id)
        if screen is None:
            return JSONResponse(
                status_code=404,
                content={"error": f"Saved screen '{screen_id}' not found"},
            )
        return JSONResponse(content={"status": "ok", **screen})
    except Exception as e:
        print(f"[SAVED_SCREENS] GET detail error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── DELETE /api/screener-hub/saved-screens/{id} ───────────────────────────────

@router.delete("/api/screener-hub/saved-screens/{screen_id}")
async def delete_saved_screen_endpoint(request: Request, screen_id: str):
    """Delete a saved screen and all its rows (CASCADE). User-scoped."""
    from data.screener_hub_store import delete_saved_screen
    user_id = _get_user_id(request)
    try:
        deleted = delete_saved_screen(user_id=user_id, screen_id=screen_id)
        if not deleted:
            return JSONResponse(
                status_code=404,
                content={"error": f"Saved screen '{screen_id}' not found"},
            )
        return JSONResponse(content={"status": "deleted", "id": screen_id})
    except Exception as e:
        print(f"[SAVED_SCREENS] DELETE error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


# ══════════════════════════════════════════════════════════════════════════════
# STATIC CURATED ANCHOR BOTTLENECK ENDPOINTS
# Source: services/playbook/curated_anchor_bottlenecks.py (no LLM, no web)
# ══════════════════════════════════════════════════════════════════════════════

# ── GET /api/bottlenecks/anchors ──────────────────────────────────────────────

@router.get("/api/bottlenecks/anchors")
async def bottlenecks_curated_anchor_list(request: Request):
    """
    Return a summary row per curated anchor: anchor_key, anchor_name,
    row_count, status, last_curated_at.

    No LLM. No web search. Deterministic static data.
    """
    try:
        from services.playbook.curated_anchor_bottlenecks import get_curated_anchor_list
        items = get_curated_anchor_list()
        return JSONResponse(content={"status": "ok", "anchors": items})
    except Exception as e:
        print(f"[CURATED_ANCHORS] list error: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)})


# ── GET /api/bottlenecks/anchor/{anchor_key} ──────────────────────────────────

@router.get("/api/bottlenecks/anchor/{anchor_key}")
async def bottlenecks_curated_anchor_detail(
    request: Request,
    anchor_key: str,
):
    """
    Return all curated + active manual nodes for one anchor in the same row
    shape used by the Chain Reaction / Bottlenecks table and detail drawer.

    No LLM. No web search.

    Valid anchor_key values: SPCX, ANTHROPIC, NVDA, OPENAI, TSM, GOOG
    """
    try:
        from services.playbook.curated_anchor_bottlenecks import (
            get_curated_anchor_bottlenecks,
            get_curated_anchor_list,
        )
        from data.manual_anchor_bottlenecks_store import (
            get_manual_nodes,
            manual_node_to_cr_row,
        )

        key = anchor_key.strip().upper()

        # Validate anchor key
        valid_keys = {a["anchor_key"] for a in get_curated_anchor_list()}
        if key not in valid_keys:
            return JSONResponse(
                status_code=404,
                content={
                    "status": "error",
                    "error": f"Unknown anchor_key {key!r}. Valid: {sorted(valid_keys)}",
                },
            )

        # Curated static rows
        curated_rows = get_curated_anchor_bottlenecks(key)

        # Active manual overlay rows for this anchor
        manual_db_rows = get_manual_nodes(anchor_key=key, active_only=True)
        manual_rows = [manual_node_to_cr_row(r) for r in manual_db_rows]

        # Merge: curated first (sorted by score desc), then manual
        all_rows = curated_rows + manual_rows
        all_rows.sort(key=lambda r: float(r.get("bottleneck_score") or 0), reverse=True)

        return JSONResponse(content={
            "status":          "ok",
            "anchor_key":      key,
            "curated_count":   len(curated_rows),
            "manual_count":    len(manual_rows),
            "total_count":     len(all_rows),
            "source_type":     "curated_static",
            "rows":            all_rows,
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[CURATED_ANCHOR_DETAIL] error: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)})


# ── GET /api/bottlenecks/anchor-overlap ───────────────────────────────────────

@router.get("/api/bottlenecks/anchor-overlap")
async def bottlenecks_curated_anchor_overlap(request: Request):
    """
    Return tickers that appear in more than one anchor map (curated + manual).

    Response shape:
      {
        "items": [
          {
            "ticker": "TSM",
            "company_name": "...",
            "anchors": ["ANTHROPIC", "GOOG", "NVDA", "OPENAI", "SPCX"],
            "count": 5,
            "max_bottleneck_score": 98,
            "roles_by_anchor": { "SPCX": "...", ... }
          }
        ]
      }
    """
    try:
        from services.playbook.curated_anchor_bottlenecks import get_curated_anchor_overlap
        from data.manual_anchor_bottlenecks_store import get_manual_nodes

        manual_rows = get_manual_nodes(active_only=True)
        items = get_curated_anchor_overlap(include_manual=manual_rows)
        return JSONResponse(content={"status": "ok", "items": items, "count": len(items)})
    except Exception as e:
        print(f"[CURATED_ANCHOR_OVERLAP] error: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)})


# ══════════════════════════════════════════════════════════════════════════════
# MANUAL ANCHOR BOTTLENECK ADMIN CRUD
# Table: public.manual_anchor_bottlenecks
# All routes require X-API-Key header.
# ══════════════════════════════════════════════════════════════════════════════

# ── GET /api/admin/bottlenecks/manual-nodes ───────────────────────────────────

@router.get("/api/admin/bottlenecks/manual-nodes")
async def admin_bottlenecks_manual_nodes_list(
    request: Request,
    api_key:    Optional[str] = Header(None, alias=_AUTH_HEADER),
    anchor_key: Optional[str] = Query(None, description="Filter by anchor_key (e.g. SPCX)"),
    active_only: bool         = Query(True,  description="Only return is_active=true rows"),
):
    """
    List manual anchor bottleneck overlay nodes.
    Optionally filter by anchor_key and/or active status.
    Requires X-API-Key header.
    """
    err = _check_admin_key(api_key)
    if err:
        return err

    try:
        from data.manual_anchor_bottlenecks_store import get_manual_nodes
        rows = get_manual_nodes(anchor_key=anchor_key, active_only=active_only)
        return JSONResponse(content={"status": "ok", "count": len(rows), "nodes": rows})
    except Exception as e:
        print(f"[MANUAL_NODES] list error: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)})


# ── POST /api/admin/bottlenecks/manual-node ───────────────────────────────────

@router.post("/api/admin/bottlenecks/manual-node")
async def admin_bottlenecks_manual_node_create(
    request: Request,
    api_key: Optional[str] = Header(None, alias=_AUTH_HEADER),
):
    """
    Create a new manual anchor bottleneck node.

    Required body fields: anchor_key, ticker, company_name, supply_chain_role.
    Optional: tradingview_symbol, bottleneck_score, evidence_grade,
              relationship_specificity, evidence, source_urls, notes,
              deal_signed_date, added_by.

    Ticker validation is attempted via existing quote paths; if it fails,
    ticker_validated=false is recorded and a warning is included in the response.
    No LLM. No web search.

    Requires X-API-Key header.
    """
    err = _check_admin_key(api_key)
    if err:
        return err

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"status": "error", "error": "Invalid JSON body"})

    # Validate required fields
    required = ["anchor_key", "ticker", "company_name", "supply_chain_role"]
    missing = [f for f in required if not body.get(f)]
    if missing:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "error": f"Missing required fields: {missing}"},
        )

    # Attempt ticker validation via existing quote service (non-blocking)
    ticker_validated = False
    validation_warning: Optional[str] = None
    try:
        from data.market_data_service import get_quote_cached
        quote = get_quote_cached(body["ticker"].upper())
        ticker_validated = bool(quote and quote.get("price"))
    except Exception as ve:
        validation_warning = f"Ticker validation skipped: {ve}"

    body["ticker_validated"] = ticker_validated

    try:
        from data.manual_anchor_bottlenecks_store import insert_manual_node
        row = insert_manual_node(body)
        resp: dict = {"status": "ok", "node": row}
        if not ticker_validated:
            resp["warning"] = validation_warning or "Ticker could not be validated; ticker_validated=false"
        return JSONResponse(status_code=201, content=resp)
    except Exception as e:
        print(f"[MANUAL_NODES] create error: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)})


# ── PUT /api/admin/bottlenecks/manual-node/{id} ───────────────────────────────

@router.put("/api/admin/bottlenecks/manual-node/{node_id}")
async def admin_bottlenecks_manual_node_update(
    request: Request,
    node_id: int,
    api_key: Optional[str] = Header(None, alias=_AUTH_HEADER),
):
    """
    Update an existing manual anchor bottleneck node.

    Updatable fields: company_name, tradingview_symbol, supply_chain_role,
    bottleneck_score, evidence_grade, relationship_specificity, evidence,
    source_urls, notes, deal_signed_date, ticker_validated, is_active.

    Requires X-API-Key header.
    """
    err = _check_admin_key(api_key)
    if err:
        return err

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"status": "error", "error": "Invalid JSON body"})

    try:
        from data.manual_anchor_bottlenecks_store import update_manual_node, get_manual_node_by_id
        existing = get_manual_node_by_id(node_id)
        if existing is None:
            return JSONResponse(status_code=404, content={"status": "error", "error": f"Node {node_id} not found"})
        row = update_manual_node(node_id, body)
        return JSONResponse(content={"status": "ok", "node": row})
    except Exception as e:
        print(f"[MANUAL_NODES] update error: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)})


# ── DELETE /api/admin/bottlenecks/manual-node/{id} (soft disable) ─────────────

@router.delete("/api/admin/bottlenecks/manual-node/{node_id}")
async def admin_bottlenecks_manual_node_disable(
    request: Request,
    node_id: int,
    api_key: Optional[str] = Header(None, alias=_AUTH_HEADER),
):
    """
    Soft-disable a manual anchor bottleneck node (sets is_active=False).
    The row is preserved in the database but excluded from public endpoints.
    Use PUT to re-enable.

    Requires X-API-Key header.
    """
    err = _check_admin_key(api_key)
    if err:
        return err

    try:
        from data.manual_anchor_bottlenecks_store import disable_manual_node
        disabled = disable_manual_node(node_id)
        if not disabled:
            return JSONResponse(status_code=404, content={"status": "error", "error": f"Node {node_id} not found"})
        return JSONResponse(content={"status": "disabled", "id": node_id})
    except Exception as e:
        print(f"[MANUAL_NODES] disable error: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)})
