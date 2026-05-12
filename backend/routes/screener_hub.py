"""
Screener Hub HTTP endpoints.

GET  /api/screener-hub/themes
GET  /api/screener-hub
POST /api/admin/screener-hub/rebuild         (X-API-Key: AGENT_API_KEY)
GET  /api/admin/screener-hub/status          (X-API-Key: AGENT_API_KEY)
POST /api/admin/bottlenecks/refresh          (X-API-Key: AGENT_API_KEY) — force full CR regen + universe rebuild
GET  /api/debug/bottlenecks-snapshot         (X-API-Key: AGENT_API_KEY) — diagnostics for snapshot state
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Body, Header, Query, Request
from fastapi.responses import JSONResponse

from services.screener_hub_service import (
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
    """Return the catalogue of themes available for the thematic tab."""
    try:
        themes = _theme_metadata()
        return JSONResponse(content={
            "status": "ok",
            "count": len(themes),
            "themes": themes,
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
):
    tab_norm = (tab or "").lower()
    if tab_norm not in _VALID_TABS:
        return JSONResponse(
            status_code=400,
            content={"error": f"invalid tab '{tab}'. Valid: {sorted(_VALID_TABS)}"},
        )

    try:
        data = await get_screener_hub(
            tab=tab_norm,
            theme=theme,
            category=category,
            score_mode=bool(scoreMode),
            coc_filter=bool(cocFilter),
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
        _prev_vis = build_cross_theme_top(limit=20, max_age_days=30)
        prev_visible_tickers = _prev_vis.get("visible_tickers") or []
        vis_result = build_cross_theme_top(limit=20, max_age_days=10,
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
