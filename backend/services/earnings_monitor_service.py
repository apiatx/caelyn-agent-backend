"""
Live Earnings Monitor Service — core logic.

Entry points:
  run_live_earnings_monitor_once(now_et, dry_run, force_symbol)
  live_earnings_monitor_loop()
  get_monitor_status()
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import socket
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

# ── global state ──────────────────────────────────────────────────────────────
_STATE: dict[str, Any] = {
    "enabled":               False,
    "deployment_mode":       "autoscale",  # or "reserved_vm"
    "last_run":              None,
    "last_success":          None,
    "run_count":             0,
    "check_count":           0,
    "sec_detections":        0,
    "fmp_detections":        0,
    "events_created":        0,
    "duplicates_suppressed": 0,
    "failures":              0,
    "active_target_count":   0,
    "worker_id":             f"w_{socket.gethostname()}_{os.getpid()}",
}

# ── monitoring windows (ET) ───────────────────────────────────────────────────
_BMO_START   = (5,  0)   # 05:00 ET
_BMO_END     = (11, 0)   # 11:00 ET
_AMC_START   = (15, 30)  # 15:30 ET
_AMC_END     = (21, 0)   # 21:00 ET
_BROAD_START = (5,  0)   # unknown timing: same as BMO start
_BROAD_END   = (21, 0)   # unknown timing: same as AMC end

# polling intervals (seconds)
_SEC_INTERVAL_ACTIVE  = 45
_FMP_INTERVAL_ACTIVE  = 60
_FMP_INTERVAL_POST    = 300  # after results received
_SCHEDULE_REFRESH_TTL = 3600  # refresh upcoming schedule hourly

_last_schedule_refresh: float = 0.0


# ── eligibility filter ────────────────────────────────────────────────────────
_FOREIGN_PREFIXES = ("LON:", "TSX:", "HKEX:", "ETR:", "EPA:", "AMS:", "ASX:", "BSE:", "NSE:", "SGX:")
_ETF_KEYWORDS     = ("etf","fund","trust","etn","index","ishares","spdr","vanguard","invesco")


def _is_eligible(symbol: str) -> bool:
    sym = symbol.upper().strip()
    if not sym or ":" in sym:
        return False
    if any(sym.startswith(p) for p in _FOREIGN_PREFIXES):
        return False
    return True


def _is_etf_by_name(name: str | None) -> bool:
    if not name:
        return False
    nl = name.lower()
    return any(k in nl for k in _ETF_KEYWORDS)


# ── window helpers ─────────────────────────────────────────────────────────────

def _now_et() -> datetime:
    return datetime.now(ET)


def _in_window(now: datetime, start_h: int, start_m: int, end_h: int, end_m: int) -> bool:
    t = now.hour * 60 + now.minute
    return (start_h * 60 + start_m) <= t < (end_h * 60 + end_m)


def _is_monitoring_window(now: datetime, timing: str | None) -> bool:
    timing = (timing or "").lower()
    if timing == "bmo":
        return _in_window(now, *_BMO_START, *_BMO_END)
    if timing in ("amc", "after market close"):
        return _in_window(now, *_AMC_START, *_AMC_END)
    return _in_window(now, *_BROAD_START, *_BROAD_END)


def _monitoring_end_et(date_str: str | None, timing: str | None) -> str | None:
    """Return ISO end-of-monitoring timestamp for a target."""
    if not date_str:
        return None
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=ET)
        timing = (timing or "").lower()
        if timing == "bmo":
            end = d.replace(hour=11, minute=30)
        elif timing in ("amc", "after market close"):
            end = d.replace(hour=21, minute=30)
        else:
            end = d.replace(hour=21, minute=30)
        # allow overflow into next day
        end = end + timedelta(days=1)
        return end.isoformat()
    except Exception:
        return None


def _monitoring_start_et(date_str: str | None, timing: str | None) -> str | None:
    if not date_str:
        return None
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=ET)
        timing = (timing or "").lower()
        if timing == "amc":
            start = d.replace(hour=15, minute=0)
        else:
            start = d.replace(hour=5, minute=0)
        return start.isoformat()
    except Exception:
        return None


def _next_check_ts(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


# ── FMP result classification ─────────────────────────────────────────────────

def _compute_checksum(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, default=str)
    return hashlib.sha1(s.encode()).hexdigest()[:16]


def _classify(eps_surprise: float | None, rev_surprise: float | None) -> str:
    eps_ok = eps_surprise is not None
    rev_ok = rev_surprise is not None
    if not eps_ok and not rev_ok:
        return "unclassified"
    if not eps_ok or not rev_ok:
        return "partial"
    if eps_surprise > 0 and rev_surprise > 0:
        return "double_beat"
    if eps_surprise < 0 and rev_surprise < 0:
        return "double_miss"
    return "mixed"


# ── universe builder ──────────────────────────────────────────────────────────

async def _build_universe() -> list[str]:
    """Collect distinct eligible symbols from watchlist + favorites + portfolio."""
    import asyncio as _aio
    from data.earnings_monitor_store import get_universe_symbols

    symbols: set[str] = set()

    # DB-backed (watchlist + favorites)
    db_syms = await _aio.to_thread(get_universe_symbols)
    for s in db_syms:
        s = s.upper().strip()
        if _is_eligible(s):
            symbols.add(s)

    # Portfolio (JSON file)
    try:
        from data.portfolio_store import load_active_holdings
        holdings = await _aio.to_thread(load_active_holdings)
        for h in (holdings or []):
            sym = (h.get("symbol") or h.get("ticker") or "").upper().strip()
            if sym and _is_eligible(sym):
                symbols.add(sym)
    except Exception as exc:
        print(f"[EarnMon] portfolio holdings error: {exc}")

    # Filter ETFs/funds via FMP profile cache (best-effort)
    eligible: list[str] = []
    for sym in sorted(symbols):
        try:
            from services.fmp_cache_service import get_company_profile_cached
            prof = get_company_profile_cached(sym) or {}
            if prof.get("is_etf") or prof.get("is_fund"):
                continue
            if _is_etf_by_name(prof.get("company_name") or prof.get("name")):
                continue
        except Exception:
            pass
        eligible.append(sym)

    return eligible


# ── schedule refresh ──────────────────────────────────────────────────────────

async def _refresh_schedule(symbols: list[str], now_et: datetime) -> None:
    """
    Refresh upcoming earnings targets from FMP for all symbols.
    Uses FMPProvider.get_earnings_history() which already has 300s TTL cache.
    Only refreshes if _SCHEDULE_REFRESH_TTL has elapsed.
    """
    global _last_schedule_refresh
    now_ts = time.time()
    if now_ts - _last_schedule_refresh < _SCHEDULE_REFRESH_TTL:
        return
    _last_schedule_refresh = now_ts

    from data.earnings_monitor_store import upsert_target
    import asyncio as _aio

    today   = now_et.date()
    window_start = today - timedelta(days=1)
    window_end   = today + timedelta(days=8)

    # We want the FMPProvider but import lazily to avoid circular imports
    try:
        from data.fmp_provider import FMPProvider
        from api_budget import daily_budget
        fmp_key = os.environ.get("FMP_API_KEY", "")
        if not fmp_key:
            return
        fmp = FMPProvider(fmp_key)
    except Exception as exc:
        print(f"[EarnMon] FMP provider init error: {exc}")
        return

    processed = 0
    for sym in symbols:
        if not daily_budget.can_spend("fmp", 1):
            print("[EarnMon] FMP budget exhausted during schedule refresh")
            break
        try:
            daily_budget.spend("fmp")
            records = await fmp.get_earnings_history(sym, limit=4)
            for rec in records:
                date_str = rec.get("date")
                if not date_str:
                    continue
                try:
                    rec_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                except ValueError:
                    continue
                if not (window_start <= rec_date <= window_end):
                    continue
                # upcoming = eps_actual is None
                is_upcoming = rec.get("eps_actual") is None
                if not is_upcoming:
                    # Already released — only enqueue if it happened recently
                    if rec_date < window_start:
                        continue
                timing = None  # FMP stable/earnings doesn't expose timing directly
                start_iso = _monitoring_start_et(date_str, timing)
                end_iso   = _monitoring_end_et(date_str, timing)
                next_check = _next_check_ts(60)
                await _aio.to_thread(
                    upsert_target,
                    sym, date_str, timing, None, None,
                    "scheduled", start_iso, end_iso, next_check, next_check,
                )
            processed += 1
            # slight delay to avoid hammering FMP
            await asyncio.sleep(0.1)
        except Exception as exc:
            print(f"[EarnMon] schedule refresh error {sym}: {exc}")

    print(f"[EarnMon] schedule refresh done: {processed}/{len(symbols)} symbols processed")


# ── SEC detection ──────────────────────────────────────────────────────────────

async def _check_sec(
    symbol: str,
    stored_accession: str | None,
) -> tuple[bool, str | None, dict | None]:
    """
    Check EDGAR for a new earnings-related filing.
    Returns (is_new_filing, accession_number, materials_dict).
    Reuses fetch_and_cache_materials which is the canonical SEC path.
    """
    try:
        from services.ei_materials_service import fetch_and_cache_materials
        materials = await asyncio.wait_for(
            fetch_and_cache_materials(symbol),
            timeout=25.0,
        )
        if not materials:
            return False, None, None
        lep = materials.get("latest_earnings_packet") or {}
        pf  = lep.get("primary_filing") or {}
        acc = pf.get("accession_number") or lep.get("accession_number")
        if not acc:
            return False, None, None
        if acc != stored_accession:
            return True, acc, materials
        return False, acc, None
    except asyncio.TimeoutError:
        print(f"[EarnMon] SEC check timeout {symbol}")
        return False, None, None
    except Exception as exc:
        print(f"[EarnMon] SEC check error {symbol}: {exc}")
        return False, None, None


# ── FMP results detection ──────────────────────────────────────────────────────

async def _check_fmp_results(symbol: str) -> dict | None:
    """
    Fetch the most-recent earnings record for a symbol using the live (short-TTL) path.
    Returns the first record with report_available=True, or None.
    """
    try:
        from data.fmp_provider import FMPProvider
        from api_budget import daily_budget
        if not daily_budget.can_spend("fmp", 1):
            return None
        fmp_key = os.environ.get("FMP_API_KEY", "")
        if not fmp_key:
            return None
        fmp = FMPProvider(fmp_key)
        daily_budget.spend("fmp")
        records = await fmp.get_earnings_history_live(symbol, limit=2)
        for rec in records:
            if rec.get("report_available") or rec.get("eps_actual") is not None:
                return rec
        return None
    except Exception as exc:
        print(f"[EarnMon] FMP results check error {symbol}: {exc}")
        return None


# ── reaction price capture ─────────────────────────────────────────────────────

def _get_reaction_snapshot(symbol: str) -> dict | None:
    """
    Pull whatever extended-hours or regular price data already exists in
    in-process caches.  Zero provider calls.
    Returns None when no reliable extended-hours price exists.
    """
    try:
        # Try the Tradier quote cache (ring-fenced in-memory store)
        from services.canonical_history_service import get_cached_quote_lkg
        q = get_cached_quote_lkg(symbol)
        if not q:
            return None
        last      = q.get("last") or q.get("close")
        exhours   = q.get("exch_vol") or q.get("extended_last")
        session   = "regular"
        price     = last
        if exhours:
            session = q.get("session", "after_hours")
            price   = exhours

        if not price:
            return None
        close = q.get("close") or q.get("prevClose") or q.get("last")
        if not close or close == 0:
            return None
        move_pct = round((float(price) - float(close)) / float(close) * 100, 2)
        return {
            "baseline_price":     close,
            "current_price":      price,
            "initial_move_pct":   move_pct,
            "price_session":      session,
            "data_source":        "tradier_cache",
            "preliminary":        True,
            "captured_at":        datetime.now(timezone.utc).isoformat(),
        }
    except Exception:
        return None


# ── alert injection ────────────────────────────────────────────────────────────

def _fire_alert_for_event(
    event: dict,
    user_id: str,
    is_update: bool = False,
) -> None:
    """Write one alert to ticker_alert_events for a single user."""
    try:
        from services.alert_signal_bus import _write_alert_sync
        sym   = event.get("symbol", "")
        state = event.get("state", "")
        cls   = event.get("classification") or ""
        rev   = event.get("revision", 1)

        state_labels = {
            "filing_detected":    "Earnings Filing Detected",
            "results_partial":    "Partial Earnings Results",
            "results_available":  "Earnings Results Available",
            "results_updated":    "Earnings Results Revised",
            "complete":           "Earnings Complete",
        }
        title = state_labels.get(state, f"Earnings: {state}")
        if cls and cls not in ("unclassified", "partial"):
            title += f" — {cls.replace('_', ' ').title()}"

        rp  = event.get("results_payload") or {}
        eps = rp.get("eps_actual")
        rev_actual = rp.get("revenue_actual")

        summary_parts = []
        if eps is not None:
            summary_parts.append(f"EPS: {eps}")
        if rev_actual is not None:
            rev_m = round(float(rev_actual) / 1e6, 1) if abs(float(rev_actual)) >= 1e6 else rev_actual
            summary_parts.append(f"Rev: {rev_m}M")
        fp  = event.get("filing_payload") or {}
        lep = fp.get("latest_earnings_packet") or fp
        if lep.get("accepted"):
            summary_parts.append(f"SEC: {lep['accepted'][:10]}")
        summary = "; ".join(summary_parts) if summary_parts else f"Earnings {state}"

        record = {
            "user_id":        user_id,
            "ticker":         sym,
            "alert_type":     "earnings_live",
            "alert_lane":     "earnings",
            "severity":       "high" if state == "results_available" else "medium",
            "title":          title,
            "short_label":    f"EARN:{state[:4].upper()}",
            "coverage_label": "Earnings Monitor",
            "summary":        summary,
            "score":          90.0 if cls == "double_beat" else 70.0,
            "reasons":        [{"key": state, "label": title}],
            "source_metrics": {
                "eps_actual":       rp.get("eps_actual"),
                "eps_surprise_pct": rp.get("eps_surprise_pct"),
                "rev_actual":       rp.get("revenue_actual"),
                "rev_surprise_pct": rp.get("revenue_surprise_pct"),
            },
            "source_tags":    [
                {"key": "event_id",  "value": event.get("event_id", "")},
                {"key": "event_key", "value": event.get("event_key", "")},
                {"key": "state",     "value": state},
                {"key": "revision",  "value": str(rev)},
                {"key": "is_dry_run","value": str(event.get("is_dry_run", False))},
            ],
        }
        _write_alert_sync(record)
    except Exception as exc:
        print(f"[EarnMon] alert write error: {exc}")


def _fire_alerts_for_event(event: dict, all_users: list[str]) -> None:
    """Write one alert per interested user."""
    is_dry = event.get("is_dry_run", False)
    if is_dry:
        all_users = ["default"]  # dry-run alerts only for default user
    for uid in all_users:
        _fire_alert_for_event(event, uid)


# ── core event key ─────────────────────────────────────────────────────────────

def _make_event_key(
    symbol: str,
    fiscal_year: int | None,
    fiscal_period: str | None,
    expected_date: str | None,
    state: str,
    revision: int = 1,
) -> str:
    if fiscal_year and fiscal_period:
        base = f"{symbol}|{fiscal_year}|{fiscal_period}"
    elif expected_date:
        base = f"{symbol}|{expected_date}"
    else:
        base = f"{symbol}|unknown"
    return f"{base}|{state}|{revision}"


def _base_event_key(
    symbol: str,
    fiscal_year: int | None,
    fiscal_period: str | None,
    expected_date: str | None,
) -> str:
    if fiscal_year and fiscal_period:
        return f"{symbol}|{fiscal_year}|{fiscal_period}"
    elif expected_date:
        return f"{symbol}|{expected_date}"
    return f"{symbol}|unknown"


# ── single target processing ───────────────────────────────────────────────────

async def _process_target(
    target: dict,
    worker_id: str,
    now_et: datetime,
    dry_run: bool = False,
) -> None:
    """
    Process one earnings target: SEC check + FMP check + state transitions.
    """
    import asyncio as _aio
    from data.earnings_monitor_store import (
        claim_target, update_target,
        upsert_live_event, get_live_event_by_key,
    )

    target_id  = target["id"]
    symbol     = target["symbol"]
    date_str   = str(target.get("expected_date") or "")
    timing     = target.get("expected_timing")
    status     = target.get("status", "scheduled")
    fiscal_p   = target.get("fiscal_period")
    fiscal_y   = target.get("fiscal_year")

    # ── lease claim ──────────────────────────────────────────────────────────
    if not dry_run:
        claimed = await _aio.to_thread(claim_target, target_id, worker_id, 90)
        if not claimed:
            return  # another worker owns this

    # ── derive base event key (no state suffix) ───────────────────────────────
    base_key = _base_event_key(symbol, fiscal_y, fiscal_p, date_str or None)

    # ── load existing event ───────────────────────────────────────────────────
    existing_filing_acc  = None
    existing_state       = "scheduled"
    existing_revision    = 1
    existing_checksum    = None
    existing_results     = None

    # look up newest event for this base key
    from data.earnings_monitor_store import get_live_event_for_symbol
    existing = await _aio.to_thread(get_live_event_for_symbol, symbol, dry_run)
    if existing:
        fp = existing.get("filing_payload") or {}
        lep = fp.get("latest_earnings_packet") or fp
        existing_filing_acc = (
            (lep.get("primary_filing") or {}).get("accession_number")
            or lep.get("accession_number")
        )
        existing_state    = existing.get("state", "scheduled")
        existing_revision = existing.get("revision", 1)
        existing_checksum = existing.get("checksum")
        existing_results  = existing.get("results_payload")

    new_state      = existing_state
    new_revision   = existing_revision
    filing_payload = None
    results_payload= existing_results
    reaction_pld   = None
    classification = existing.get("classification") if existing else None
    last_error     = None

    # ── SEC check ─────────────────────────────────────────────────────────────
    now_utc = datetime.now(timezone.utc)
    sec_due = target.get("next_sec_check_at")
    should_sec = True
    if sec_due and not dry_run:
        try:
            sec_dt = datetime.fromisoformat(str(sec_due))
            if sec_dt.tzinfo is None:
                sec_dt = sec_dt.replace(tzinfo=timezone.utc)
            should_sec = now_utc >= sec_dt
        except Exception:
            pass

    if should_sec and existing_state not in ("results_available","complete","results_updated"):
        if _is_monitoring_window(now_et, timing) or dry_run:
            is_new, acc, materials = await _check_sec(symbol, existing_filing_acc)
            _STATE["check_count"] += 1
            if is_new and materials:
                _STATE["sec_detections"] += 1
                filing_payload = {"latest_earnings_packet": materials.get("latest_earnings_packet")}
                if new_state in ("scheduled", "monitoring"):
                    new_state = "filing_detected"
                reaction_pld = _get_reaction_snapshot(symbol)
            next_sec = _next_check_ts(_SEC_INTERVAL_ACTIVE if dry_run else _SEC_INTERVAL_ACTIVE)
            await _aio.to_thread(update_target, target_id, next_sec_check_at=next_sec, status="monitoring")

    # ── FMP check ─────────────────────────────────────────────────────────────
    fmp_due = target.get("next_fmp_check_at")
    should_fmp = True
    if fmp_due and not dry_run:
        try:
            fmp_dt = datetime.fromisoformat(str(fmp_due))
            if fmp_dt.tzinfo is None:
                fmp_dt = fmp_dt.replace(tzinfo=timezone.utc)
            should_fmp = now_utc >= fmp_dt
        except Exception:
            pass

    if should_fmp and existing_state not in ("complete",):
        if _is_monitoring_window(now_et, timing) or dry_run:
            fmp_rec = await _check_fmp_results(symbol)
            _STATE["check_count"] += 1
            if fmp_rec and fmp_rec.get("eps_actual") is not None:
                _STATE["fmp_detections"] += 1
                # Enrich fiscal labels if FMP returned them
                # (FMP stable/earnings doesn't have period; use income-statement)
                rp = {
                    "eps_estimate":       fmp_rec.get("eps_estimate"),
                    "eps_actual":         fmp_rec.get("eps_actual"),
                    "eps_surprise_amount": _safe_diff(fmp_rec.get("eps_actual"), fmp_rec.get("eps_estimate")),
                    "eps_surprise_pct":   _safe_pct(fmp_rec.get("eps_actual"), fmp_rec.get("eps_estimate")),
                    "revenue_estimate":   fmp_rec.get("revenue_estimate"),
                    "revenue_actual":     fmp_rec.get("revenue_actual"),
                    "revenue_surprise_amount": _safe_diff(fmp_rec.get("revenue_actual"), fmp_rec.get("revenue_estimate")),
                    "revenue_surprise_pct":    _safe_pct(fmp_rec.get("revenue_actual"), fmp_rec.get("revenue_estimate")),
                    "date":               fmp_rec.get("date"),
                }
                new_checksum = _compute_checksum(rp)
                has_eps = rp["eps_actual"] is not None
                has_rev = rp["revenue_actual"] is not None

                if new_checksum != existing_checksum:
                    # data changed — decide state
                    if existing_state in ("results_available","complete"):
                        new_state    = "results_updated"
                        new_revision = existing_revision + 1
                    elif has_eps and has_rev:
                        new_state = "results_available"
                    elif has_eps or has_rev:
                        new_state = "results_partial"
                    else:
                        new_state = filing_payload and "filing_detected" or existing_state

                    classification = _classify(rp.get("eps_surprise_pct"), rp.get("revenue_surprise_pct"))
                    results_payload = rp
                    existing_checksum = new_checksum
                    if not reaction_pld:
                        reaction_pld = _get_reaction_snapshot(symbol)
                else:
                    # same checksum — no update needed
                    _STATE["duplicates_suppressed"] += 1

            # check completion: results_available + 30-min correction window
            if new_state == "results_available" and results_payload:
                detected_str = existing.get("detected_at") if existing else None
                if detected_str:
                    try:
                        det = datetime.fromisoformat(str(detected_str))
                        if det.tzinfo is None:
                            det = det.replace(tzinfo=timezone.utc)
                        if (now_utc - det).total_seconds() > 1800:
                            new_state = "complete"
                    except Exception:
                        pass

            interval = _FMP_INTERVAL_POST if new_state in ("results_available","complete","results_updated") else _FMP_INTERVAL_ACTIVE
            next_fmp = _next_check_ts(interval)
            await _aio.to_thread(update_target, target_id, next_fmp_check_at=next_fmp)

    # ── detect meaningful change in Python (reliable vs DB xmax tricks) ──────
    # A meaningful change is:
    #   - No prior event for this symbol (first detection)
    #   - State transitioned from the prior state
    #   - Revision incremented (results_updated correction)
    state_changed = (not existing) or (new_state != existing_state) or (new_revision != existing_revision)

    # ── persist event ─────────────────────────────────────────────────────────
    # Build the definitive event key (base + state + revision)
    event_key = _make_event_key(symbol, fiscal_y, fiscal_p, date_str or None, new_state, new_revision)
    detected_at = None
    if new_state not in ("scheduled","monitoring") and not (existing and existing.get("detected_at")):
        detected_at = datetime.now(timezone.utc).isoformat()
    elif existing:
        detected_at = str(existing.get("detected_at") or "")

    event_id, _upserted, _ = await _aio.to_thread(
        upsert_live_event,
        event_key,
        symbol,
        new_state,
        date_str or None,
        fiscal_p,
        fiscal_y,
        detected_at or None,
        new_revision,
        dry_run,
        filing_payload or (existing.get("filing_payload") if existing else None),
        results_payload,
        reaction_pld,
        {"sec_checked_at": now_utc.isoformat(), "fmp_checked_at": now_utc.isoformat()},
        classification,
        existing_checksum,
        last_error,
    )

    # ── fire alerts ───────────────────────────────────────────────────────────
    alert_worthy = new_state in ("filing_detected","results_partial","results_available","results_updated","complete")
    if alert_worthy and state_changed:
        _STATE["events_created"] += 1
        event_row = {
            "event_id":        event_id,
            "event_key":       event_key,
            "symbol":          symbol,
            "state":           new_state,
            "revision":        new_revision,
            "is_dry_run":      dry_run,
            "filing_payload":  filing_payload,
            "results_payload": results_payload,
            "classification":  classification,
        }
        users = _get_users_for_symbol(symbol)
        _fire_alerts_for_event(event_row, users)
    elif existing and new_state == existing_state:
        _STATE["duplicates_suppressed"] += 1

    # ── mark target complete ──────────────────────────────────────────────────
    if new_state == "complete" and not dry_run:
        await _aio.to_thread(update_target, target_id, status="complete")


def _safe_diff(actual, estimate) -> float | None:
    try:
        if actual is None or estimate is None:
            return None
        return round(float(actual) - float(estimate), 4)
    except Exception:
        return None


def _safe_pct(actual, estimate) -> float | None:
    try:
        if actual is None or estimate is None or float(estimate) == 0:
            return None
        return round((float(actual) - float(estimate)) / abs(float(estimate)) * 100, 2)
    except Exception:
        return None


def _get_users_for_symbol(symbol: str) -> list[str]:
    """
    Return user_ids who have this symbol in watchlist or favorites.
    Currently single-user — returns ["default"] always.
    When auth is fully wired, extend to per-user lookup.
    """
    return ["default"]


# ── replay / dry-run ──────────────────────────────────────────────────────────

async def run_replay(symbol: str = "COIN") -> dict:
    """
    Run a deterministic replay through all states using cached/historical data.
    Events are flagged is_dry_run=True and do NOT appear in production feeds.
    """
    from data.earnings_monitor_store import upsert_live_event, delete_dry_run_events

    import asyncio as _aio

    # Clean up any existing dry-run events for this symbol
    await _aio.to_thread(delete_dry_run_events)

    date_str     = "2026-05-08"
    fiscal_year  = 2026
    fiscal_period= "Q1"
    symbol       = symbol.upper()

    results: list[dict] = []

    async def _step(state: str, rev: int = 1, rp: dict | None = None, fp: dict | None = None, cls: str | None = None):
        key = _make_event_key(symbol, fiscal_year, fiscal_period, date_str, state, rev)
        eid, is_new, ck = await _aio.to_thread(
            upsert_live_event,
            key, symbol, state,
            date_str, fiscal_period, fiscal_year,
            datetime.now(timezone.utc).isoformat(),
            rev, True,
            fp, rp, None,
            {"replay": True}, cls,
            _compute_checksum(rp or {}),
            None,
        )
        results.append({
            "state":      state,
            "event_id":   eid,
            "is_new":     is_new,
            "ck_changed": ck,
            "revision":   rev,
        })
        await asyncio.sleep(0.05)  # small gap to preserve ordering

    # scheduled → monitoring
    await _step("scheduled")
    await _step("monitoring")

    # filing_detected — simulate SEC detection
    fp_sim = {"latest_earnings_packet": {"accession_number": "0001193125-26-111111", "accepted": "2026-05-08T07:00:00Z"}}
    await _step("filing_detected", fp=fp_sim)

    # results_partial — EPS only
    rp_partial = {
        "eps_estimate": 1.85, "eps_actual": 2.10,
        "eps_surprise_amount": 0.25, "eps_surprise_pct": 13.51,
        "revenue_estimate": None, "revenue_actual": None,
    }
    await _step("results_partial", rp=rp_partial, fp=fp_sim)

    # results_available
    rp_full = {
        "eps_estimate": 1.85, "eps_actual": 2.10,
        "eps_surprise_amount": 0.25, "eps_surprise_pct": 13.51,
        "revenue_estimate": 1900000000, "revenue_actual": 2100000000,
        "revenue_surprise_amount": 200000000, "revenue_surprise_pct": 10.53,
    }
    await _step("results_available", rp=rp_full, fp=fp_sim, cls="double_beat")

    # results_updated (correction)
    rp_corr = dict(rp_full)
    rp_corr["eps_actual"] = 2.09  # minor correction
    rp_corr["eps_surprise_amount"] = 0.24
    rp_corr["eps_surprise_pct"] = 12.97
    await _step("results_updated", rev=2, rp=rp_corr, fp=fp_sim, cls="double_beat")

    # complete
    await _step("complete", rev=2, rp=rp_corr, fp=fp_sim, cls="double_beat")

    # Verify no duplicates: replay same step
    await _step("complete", rev=2, rp=rp_corr, fp=fp_sim, cls="double_beat")
    last = results[-1]
    prev = results[-2] if len(results) >= 2 else None
    # dedup_ok: identical event_id means ON CONFLICT hit the same row (no new row created).
    # This is the correct dedup check regardless of xmax/pool behaviour.
    dedup_ok = (
        prev is not None
        and prev["state"] == last["state"]
        and prev["revision"] == last["revision"]
        and prev["event_id"] == last["event_id"]
    )

    return {
        "symbol":    symbol,
        "steps":     results,
        "dedup_ok":  dedup_ok,
        "pass":      dedup_ok and len(results) >= 6,
    }


# ── main entry points ─────────────────────────────────────────────────────────

async def run_live_earnings_monitor_once(
    now_et: datetime | None = None,
    dry_run: bool           = False,
    force_symbol: str | None= None,
) -> dict:
    """
    Idempotent monitoring pass.  Safe to call from CLI or scheduled job.
    """
    if now_et is None:
        now_et = _now_et()

    _STATE["run_count"] += 1
    _STATE["last_run"] = datetime.now(timezone.utc).isoformat()

    try:
        import asyncio as _aio
        from data.earnings_monitor_store import get_active_targets

        # Build universe — when force-checking one symbol, skip full universe build
        # to avoid scanning all 399 symbols in _refresh_schedule
        if force_symbol:
            sym = force_symbol.upper()
            universe = [sym]
        else:
            universe = await _build_universe()

        # Refresh schedule from FMP
        await _refresh_schedule(universe, now_et)

        # Load active targets
        targets = await _aio.to_thread(get_active_targets, 200)
        if force_symbol:
            sym = force_symbol.upper()
            targets = [t for t in targets if t["symbol"] == sym] or targets

        _STATE["active_target_count"] = len(targets)

        worker_id = _STATE["worker_id"]
        errors = 0
        processed = 0

        for target in targets:
            sym = target.get("symbol","")
            if not sym:
                continue
            # skip symbols not in our universe unless force_symbol
            if not force_symbol and sym not in universe:
                continue
            try:
                await _process_target(target, worker_id, now_et, dry_run)
                processed += 1
            except Exception as exc:
                errors += 1
                _STATE["failures"] += 1
                print(f"[EarnMon] target error {sym}: {exc}")

        _STATE["last_success"] = datetime.now(timezone.utc).isoformat()
        return {
            "processed": processed,
            "errors":    errors,
            "universe":  len(universe),
            "targets":   len(targets),
        }
    except Exception as exc:
        _STATE["failures"] += 1
        print(f"[EarnMon] run_once top-level error: {exc}")
        return {"error": str(exc)}


async def live_earnings_monitor_loop(interval_seconds: int = 30) -> None:
    """
    Persistent monitoring loop. Started only after FastAPI lifespan yield.
    Controlled by LIVE_EARNINGS_MONITOR_ENABLED env var.
    """
    _STATE["enabled"] = True
    print(f"[EarnMon] persistent loop started (interval={interval_seconds}s)")
    while True:
        try:
            await run_live_earnings_monitor_once()
        except Exception as exc:
            _STATE["failures"] += 1
            print(f"[EarnMon] loop iteration error: {exc}")
        await asyncio.sleep(interval_seconds)


def get_monitor_status() -> dict:
    return {
        **_STATE,
        "deployment_note": (
            "Autoscale: persistent loop not guaranteed while scaled to zero. "
            "Run-once CLI: python -m backend.scripts.run_live_earnings_monitor_once"
        ),
    }
