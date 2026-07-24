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


# ── event serialiser ───────────────────────────────────────────────────────────

def _serialize_event(row: dict, read_at=None) -> dict:
    def _dt(v):
        if v is None:
            return None
        return str(v).replace("+00:00", "Z") if "+" in str(v) or "Z" in str(v) else str(v)

    results  = row.get("results_payload") or {}
    filing   = row.get("filing_payload") or {}
    reaction = row.get("reaction_payload") or {}
    lep      = filing.get("latest_earnings_packet") or {}

    return {
        "event_id":       row.get("event_id"),
        "event_key":      row.get("event_key"),
        "symbol":         row.get("symbol"),
        "state":          row.get("state"),
        "expected_date":  _dt(row.get("expected_date")),
        "fiscal_period":  row.get("fiscal_period"),
        "fiscal_year":    row.get("fiscal_year"),
        "detected_at":    _dt(row.get("detected_at")),
        "updated_at":     _dt(row.get("updated_at")),
        "revision":       row.get("revision", 1),
        "is_dry_run":     row.get("is_dry_run", False),
        "classification": row.get("classification"),
        "read_at":        _dt(read_at) if read_at else None,
        "results_summary": {
            "eps_estimate":         results.get("eps_estimate"),
            "eps_actual":           results.get("eps_actual"),
            "eps_surprise_pct":     results.get("eps_surprise_pct"),
            "revenue_estimate":     results.get("revenue_estimate"),
            "revenue_actual":       results.get("revenue_actual"),
            "revenue_surprise_pct": results.get("revenue_surprise_pct"),
        } if results else None,
        "filing_summary": {
            "accession_number": (lep.get("primary_filing") or {}).get("accession_number") or lep.get("accession_number"),
            "accepted":         lep.get("accepted"),
        } if lep else None,
        "initial_market_reaction": reaction if reaction else None,
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

    rows = get_user_event_feed(
        user_id      = user_id,
        symbols      = universe,
        since_iso    = since,
        unread_only  = unread_only,
        limit        = limit,
    )

    # attach read status
    event_ids = [r["event_id"] for r in rows]
    read_ids  = get_event_read_ids(user_id, event_ids)

    events = [
        _serialize_event(r, read_at=r.get("read_at"))
        for r in rows
    ]

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

@router.get("/monitor/targets")
async def list_targets(request: Request):
    """Admin: list all active monitoring targets."""
    _check_admin(request)
    from data.earnings_monitor_store import get_active_targets
    targets = get_active_targets(200)
    return {
        "targets": [
            {
                "id":              t["id"],
                "symbol":          t["symbol"],
                "expected_date":   str(t.get("expected_date") or ""),
                "expected_timing": t.get("expected_timing"),
                "fiscal_period":   t.get("fiscal_period"),
                "fiscal_year":     t.get("fiscal_year"),
                "status":          t.get("status"),
                "next_sec_check":  str(t.get("next_sec_check_at") or ""),
                "next_fmp_check":  str(t.get("next_fmp_check_at") or ""),
                "lease_owner":     t.get("worker_lease_owner"),
                "lease_expires":   str(t.get("worker_lease_expires_at") or ""),
            }
            for t in targets
        ],
        "count": len(targets),
    }
