"""
Live Earnings Monitor — REST API.

Public (authenticated via Bearer / "default" fallback):
  GET  /api/earnings/live-events
  POST /api/earnings/live-events/{event_id}/read

Admin (ADMIN_PASSWORD header):
  GET  /api/earnings/monitor/status
  POST /api/earnings/monitor/force-check/{symbol}
  POST /api/earnings/monitor/replay
  POST /api/earnings/monitor/replay/clear
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter(prefix="/api/earnings", tags=["earnings_monitor"])


# ── auth helpers ───────────────────────────────────────────────────────────────

def _get_user_id(request: Request) -> str:
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


def _check_admin(request: Request) -> None:
    pw = os.environ.get("ADMIN_PASSWORD", "")
    if not pw:
        return  # no password configured → open in dev
    provided = (
        request.headers.get("X-Admin-Password")
        or request.headers.get("x-admin-password")
        or request.query_params.get("admin_password")
        or ""
    )
    if provided != pw:
        raise HTTPException(status_code=403, detail="admin credentials required")


# ── universe helper ────────────────────────────────────────────────────────────

def _get_user_symbols(user_id: str) -> list[str]:
    """
    Return all symbols in the user's observable universe
    (watchlist + favorites + portfolio).
    Currently single-user — always returns the full universe.
    """
    symbols: set[str] = set()
    try:
        from data.earnings_monitor_store import get_universe_symbols
        for s in get_universe_symbols():
            symbols.add(s.upper())
    except Exception:
        pass
    try:
        from data.portfolio_store import load_active_holdings
        for h in (load_active_holdings() or []):
            sym = (h.get("symbol") or h.get("ticker") or "").upper().strip()
            if sym:
                symbols.add(sym)
    except Exception:
        pass
    return sorted(symbols)


# ── target schedule enrichment (Neon read, zero provider calls) ────────────────

def _get_target_schedules(symbols: list[str]) -> dict[str, dict]:
    """
    Batch-fetch schedule metadata from earnings_monitor_targets for the given
    symbols.  Pure Neon read — no external API calls.
    Returns {symbol: schedule_dict}.

    Uses get_targets_for_symbols (not get_active_targets) so that complete
    targets are included and schedule fields (expected_at, expected_timing,
    report_time_status, etc.) are never NULL just because a target is complete.
    """
    if not symbols:
        return {}
    try:
        from data.earnings_monitor_store import get_targets_for_symbols
        targets = get_targets_for_symbols(symbols)
        result: dict[str, dict] = {}
        for t in targets:
            sym = (t.get("symbol") or "").upper()
            if not sym:
                continue
            ea = t.get("expected_at")
            result[sym] = {
                "expected_at":         str(ea).replace("+00:00", "Z") if ea else None,
                "expected_timing":     t.get("expected_timing"),
                "expected_time_local": t.get("expected_time_local"),
                "expected_timezone":   "America/New_York",
                "report_time_status":  t.get("report_time_status"),
                "report_period":       t.get("report_period"),
                "schedule_source":     t.get("schedule_source"),
            }
        return result
    except Exception:
        return {}


def _get_company_names_batch(symbols: list[str]) -> dict[str, str | None]:
    """
    Best-effort company name lookup from in-process/Neon caches.
    Zero provider calls.  Returns None for unknown symbols.
    """
    result: dict[str, str | None] = {s: None for s in symbols}
    try:
        from services.fmp_cache_service import get_company_profile_cached, get_fundamentals_cached
        for sym in symbols:
            try:
                prof = get_company_profile_cached(sym) or {}
                name = (prof.get("companyName") or prof.get("company_name")
                        or prof.get("name") or prof.get("shortName"))
                if not name:
                    fdb  = get_fundamentals_cached(sym) or {}
                    raw_p = fdb.get("profile") or {}
                    name = raw_p.get("companyName") or raw_p.get("company_name")
                result[sym] = name or None
            except Exception:
                pass
    except Exception:
        pass
    return result


# ── event serialiser ───────────────────────────────────────────────────────────

def _serialize_event(row: dict, read_at=None) -> dict:
    """
    Serialize one live-event DB row to the full frontend contract.

    `row` should be pre-enriched with target schedule fields
    (expected_at, expected_timing, etc.) and company_name via
    _get_target_schedules() / _get_company_names_batch() before calling.
    """
    def _dt(v):
        if v is None:
            return None
        s = str(v)
        return s.replace("+00:00", "Z") if ("+" in s or "Z" in s) else s

    results  = row.get("results_payload") or {}
    filing   = row.get("filing_payload")  or {}
    reaction = row.get("reaction_payload") or {}
    is_read_val = read_at or row.get("read_at")

    return {
        # ── identity ──────────────────────────────────────────────────────
        "event_id":            row.get("event_id"),
        "event_key":           row.get("event_key"),
        "symbol":              row.get("symbol"),
        "company_name":        row.get("company_name"),
        # ── state ─────────────────────────────────────────────────────────
        "state":               row.get("state"),
        "classification":      row.get("classification"),
        "revision":            row.get("revision", 1),
        # ── timestamps ────────────────────────────────────────────────────
        "detected_at":         _dt(row.get("detected_at")),
        "updated_at":          _dt(row.get("updated_at")),
        # ── schedule (enriched from earnings_monitor_targets) ─────────────
        "expected_date":       _dt(row.get("expected_date")),
        "expected_at":         _dt(row.get("expected_at")),
        "expected_time_local": row.get("expected_time_local"),
        "expected_timezone":   row.get("expected_timezone", "America/New_York"),
        "expected_timing":     row.get("expected_timing"),
        "report_time_status":  row.get("report_time_status"),
        "report_period":       row.get("report_period"),
        "schedule_source":     row.get("schedule_source"),
        # ── fiscal ────────────────────────────────────────────────────────
        "fiscal_period":       row.get("fiscal_period"),
        "fiscal_year":         row.get("fiscal_year"),
        # ── full payloads (safe — no credentials/internal errors) ─────────
        "results_payload":     results if results else None,
        "filing_payload":      filing  if filing  else None,
        "reaction_payload":    reaction if reaction else None,
        # ── read / meta ───────────────────────────────────────────────────
        "source_status":       row.get("source_status"),
        "is_read":             _dt(is_read_val),
    }


# ── GET /api/earnings/live-events ─────────────────────────────────────────────

@router.get("/live-events")
async def get_live_events(
    request:      Request,
    since:        str | None = Query(None, description="ISO timestamp — only events updated after this"),
    unread_only:  bool       = Query(False),
    symbols:      str | None = Query(None, description="Comma-separated symbol filter"),
    limit:        int        = Query(50, ge=1, le=200),
):
    """
    Return live earnings events relevant to the authenticated user's universe.
    Polls-friendly: use `since` to fetch incremental updates.
    """
    user_id = _get_user_id(request)
    universe = _get_user_symbols(user_id)

    # optional symbol filter
    if symbols:
        requested = {s.strip().upper() for s in symbols.split(",") if s.strip()}
        universe  = [s for s in universe if s in requested]

    if not universe:
        return {"events": [], "user_id": user_id, "symbol_count": 0}

    from data.earnings_monitor_store import get_user_event_feed, get_event_read_ids

    import asyncio as _aio

    rows = get_user_event_feed(
        user_id      = user_id,
        symbols      = universe,
        since_iso    = since,
        unread_only  = unread_only,
        limit        = limit,
    )

    if not rows:
        return {
            "events":       [],
            "user_id":      user_id,
            "symbol_count": len(universe),
            "since":        since,
            "count":        0,
        }

    # Enrich rows with target schedule data + company names (Neon reads, zero provider calls)
    event_syms    = list({r["symbol"] for r in rows})
    target_sched, company_names = await _aio.gather(
        _aio.to_thread(_get_target_schedules,     event_syms),
        _aio.to_thread(_get_company_names_batch,  event_syms),
    )

    enriched: list[dict] = []
    for r in rows:
        sym    = (r.get("symbol") or "").upper()
        merged = dict(r)
        merged.update(target_sched.get(sym, {}))
        merged["company_name"] = company_names.get(sym)
        enriched.append(merged)

    events = [_serialize_event(r, read_at=r.get("read_at")) for r in enriched]

    return {
        "events":       events,
        "user_id":      user_id,
        "symbol_count": len(universe),
        "since":        since,
        "count":        len(events),
    }


# ── POST /api/earnings/live-events/{event_id}/read ────────────────────────────

@router.post("/live-events/{event_id}/read")
async def mark_event_read(event_id: str, request: Request):
    """Acknowledge / mark a live earnings event as read."""
    user_id = _get_user_id(request)
    from data.earnings_monitor_store import mark_event_read as _mark
    ok = _mark(event_id, user_id)
    return {"ok": ok, "event_id": event_id, "user_id": user_id}


# ── GET /api/earnings/monitor/status ──────────────────────────────────────────

@router.get("/monitor/status")
async def get_monitor_status(request: Request):
    """Admin: return monitor runtime state and counters."""
    _check_admin(request)
    from services.earnings_monitor_service import get_monitor_status
    from data.earnings_monitor_store import get_target_count
    status = get_monitor_status()
    status["target_counts"] = get_target_count()
    return status


# ── POST /api/earnings/monitor/force-check/{symbol} ───────────────────────────

@router.post("/monitor/force-check/{symbol}")
async def force_check_symbol(symbol: str, request: Request):
    """Admin: queue a background monitoring pass for a specific symbol.
    Returns immediately — real SEC/FMP calls happen asynchronously."""
    _check_admin(request)
    import asyncio
    from services.earnings_monitor_service import run_live_earnings_monitor_once
    asyncio.create_task(run_live_earnings_monitor_once(force_symbol=symbol.upper()))
    return {"ok": True, "queued": True, "symbol": symbol.upper()}


# ── POST /api/earnings/monitor/replay ─────────────────────────────────────────

@router.post("/monitor/replay")
async def run_replay(request: Request, symbol: str = Query("COIN")):
    """
    Admin: run a deterministic replay through all event states using synthetic data.
    Events are flagged is_dry_run=True and do NOT appear in user feeds.
    Returns pass/fail verification of deduplication logic.
    """
    _check_admin(request)
    from services.earnings_monitor_service import run_replay as _replay
    result = await _replay(symbol.upper())
    return result


# ── POST /api/earnings/monitor/replay/clear ───────────────────────────────────

@router.post("/monitor/replay/clear")
async def clear_replay_events(request: Request):
    """Admin: delete all dry-run events from Neon."""
    _check_admin(request)
    from data.earnings_monitor_store import delete_dry_run_events
    deleted = delete_dry_run_events()
    return {"deleted": deleted}


# ── GET /api/earnings/monitor/targets ─────────────────────────────────────────

def _target_to_dict(t: dict) -> dict:
    """Serialize a target row to a JSON-safe dict (used by multiple endpoints)."""
    return {
        "id":                       t["id"],
        "symbol":                   t["symbol"],
        "expected_date":            str(t.get("expected_date") or ""),
        "expected_timing":          t.get("expected_timing"),
        "expected_time_local":      t.get("expected_time_local"),
        "expected_at":              str(t.get("expected_at") or ""),
        "report_time_status":       t.get("report_time_status"),
        "report_period":            t.get("report_period"),
        "fiscal_period":            t.get("fiscal_period"),
        "fiscal_year":              t.get("fiscal_year"),
        "schedule_source":          t.get("schedule_source"),
        "fmp_check_stage":          t.get("fmp_check_stage"),
        "results_first_detected_at": str(t.get("results_first_detected_at") or ""),
        "status":                   t.get("status"),
        "next_sec_check":           str(t.get("next_sec_check_at") or ""),
        "next_fmp_check":           str(t.get("next_fmp_check_at") or ""),
        "lease_owner":              t.get("worker_lease_owner"),
        "lease_expires":            str(t.get("worker_lease_expires_at") or ""),
        "updated_at":               str(t.get("updated_at") or ""),
    }


@router.get("/monitor/targets")
async def list_targets(request: Request):
    """Admin: list all active monitoring targets with full scheduling detail."""
    _check_admin(request)
    from data.earnings_monitor_store import get_active_targets, get_due_targets
    all_active = get_active_targets(200)
    due_now    = get_due_targets(200)
    due_ids    = {t["id"] for t in due_now}
    rows = []
    for t in all_active:
        d = _target_to_dict(t)
        d["due_now"] = t["id"] in due_ids
        rows.append(d)
    return {
        "targets":    rows,
        "count":      len(rows),
        "due_count":  len(due_now),
    }


@router.get("/monitor/targets/{symbol}")
async def get_target_by_symbol(symbol: str, request: Request):
    """Admin: return current monitoring target for a specific symbol."""
    _check_admin(request)
    from data.earnings_monitor_store import get_active_targets
    targets = get_active_targets(500)
    sym_upper = symbol.upper()
    matches = [t for t in targets if t["symbol"] == sym_upper]
    if not matches:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"No active target for {sym_upper}")
    return {
        "symbol":  sym_upper,
        "targets": [_target_to_dict(t) for t in matches],
        "count":   len(matches),
    }


@router.post("/monitor/admin/reaction-repair")
async def trigger_reaction_repair(
    request: Request,
    lookback_days: int = 10,
):
    """
    Admin: run reaction_catchup_pass to finalize missing Pre/Post price reactions
    for complete events in the last N days.

    Fetches fresh Tradier bars for any symbol whose canonical history is stale
    and merges computed horizons into earnings_live_events.reaction_payload.
    """
    _check_admin(request)
    import asyncio as _aio
    try:
        from services.earnings_reaction_service import reaction_catchup_pass
        result = await _aio.wait_for(
            reaction_catchup_pass(lookback_days=min(lookback_days, 30)),
            timeout=120.0,
        )
        return {"status": "ok", "result": result}
    except _aio.TimeoutError:
        return {"status": "timeout", "error": "reaction_catchup_pass exceeded 120s"}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@router.post("/monitor/admin/repair-symbol/{symbol}")
async def repair_symbol_event(symbol: str, request: Request):
    """
    Admin: targeted repair for one symbol.

    1. Clears next_fmp_check_at on the active target (stops stale polling).
    2. Runs _earnings_catchup_pass for just this symbol (fills estimates if available).
    3. Runs reaction finalization for the canonical live event.
    """
    _check_admin(request)
    import asyncio as _aio
    sym = symbol.upper()
    result: dict = {"symbol": sym, "steps": []}

    try:
        from data.earnings_monitor_store import get_targets_for_symbols, update_target
        sym_targets = await _aio.to_thread(get_targets_for_symbols, [sym])
        if sym_targets:
            for t in sym_targets:
                ok = await _aio.to_thread(update_target, t["id"], next_fmp_check_at=None)
                result["steps"].append({
                    "step":      "clear_next_fmp_check_at",
                    "target_id": t["id"],
                    "status":    t.get("status"),
                    "ok":        ok,
                })
        else:
            result["steps"].append({"step": "clear_next_fmp_check_at", "skipped": "no targets found"})
    except Exception as exc:
        result["steps"].append({"step": "clear_next_fmp_check_at", "error": str(exc)})

    try:
        from services.earnings_monitor_service import run_live_earnings_monitor_once
        catchup = await _aio.wait_for(
            run_live_earnings_monitor_once(force_symbol=sym),
            timeout=60.0,
        )
        result["steps"].append({"step": "fmp_check", "result": catchup})
    except _aio.TimeoutError:
        result["steps"].append({"step": "fmp_check", "error": "timeout"})
    except Exception as exc:
        result["steps"].append({"step": "fmp_check", "error": str(exc)})

    try:
        from data.earnings_monitor_store import (
            get_live_event_for_symbol, get_targets_for_symbols,
        )
        ev = await _aio.to_thread(get_live_event_for_symbol, sym, False)
        if ev:
            rp      = ev.get("reaction_payload") or {}
            rd      = str(ev.get("expected_date") or "")[:10]
            tgts    = await _aio.to_thread(get_targets_for_symbols, [sym])
            timing  = next(
                (t.get("expected_timing") for t in tgts
                 if str(t.get("expected_date") or "")[:10] == rd),
                None,
            )
            from services.earnings_reaction_service import finalize_reactions_for_event
            rxn = await _aio.wait_for(
                finalize_reactions_for_event(ev["event_id"], sym, rd, timing, rp),
                timeout=40.0,
            )
            result["steps"].append({
                "step":        "reaction_finalization",
                "event_id":    ev["event_id"],
                "report_date": rd,
                "timing":      timing,
                "horizons":    rxn.get("horizons_available") if rxn else [],
            })
        else:
            result["steps"].append({"step": "reaction_finalization", "skipped": "no live event"})
    except _aio.TimeoutError:
        result["steps"].append({"step": "reaction_finalization", "error": "timeout"})
    except Exception as exc:
        result["steps"].append({"step": "reaction_finalization", "error": str(exc)})

    result["status"] = "ok"
    return result
