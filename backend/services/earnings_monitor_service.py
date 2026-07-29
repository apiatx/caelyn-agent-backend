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
    # ── tick loop (autoscale post-yield scheduler) ────────────────────────────
    "tick_loop_enabled":      False,
    "tick_loop_last_tick":    None,
    "tick_loop_tick_count":   0,
    # ── startup catch-up pass ──────────────────────────────────────────────────
    "catchup_last_run":       None,
    "catchup_symbols_checked": 0,
    "catchup_results_filled":  0,
    "catchup_ei_triggered":    0,
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

# Anchor times (hour, minute ET) used to compute expected_at for FMP timing
# FMP Starter only returns "bmo" / "amc" / None — no exact clock time
_BMO_ANCHOR_H, _BMO_ANCHOR_M = 8, 0    # 08:00 ET: midpoint of BMO window
_AMC_ANCHOR_H, _AMC_ANCHOR_M = 16, 30  # 16:30 ET: shortly after regular close

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


# ── Part 2: timing / scheduling helpers ───────────────────────────────────────

def _expected_time_local(timing: str | None) -> str | None:
    """Human-readable release window string for UI display."""
    t = (timing or "").lower()
    if t == "bmo":
        return "Before Market Open (BMO)"
    if t in ("amc", "after market close"):
        return "After Market Close (AMC)"
    return None


def _compute_expected_at(
    date_str: str | None,
    timing: str | None,
) -> Optional[datetime]:
    """
    Compute an anchor UTC timestamp for pre-release polling.

    FMP Starter only provides "bmo" / "amc" / None — never an exact clock time.
    We use conservative anchors:
      BMO  → 08:00 ET (well before typical pre-market release)
      AMC  → 16:30 ET (30 min after close, most AMC releases land here)
      None → 16:30 ET (default: treat as AMC so we don't miss anything)
    """
    if not date_str:
        return None
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        t = (timing or "").lower()
        if t == "bmo":
            h, m = _BMO_ANCHOR_H, _BMO_ANCHOR_M
        else:
            h, m = _AMC_ANCHOR_H, _AMC_ANCHOR_M
        anchor_et = datetime(d.year, d.month, d.day, h, m, tzinfo=ET)
        return anchor_et.astimezone(timezone.utc)
    except Exception:
        return None


def _compute_next_fmp_check(
    expected_at: Optional[datetime],
    now_utc: datetime,
    results_detected_at: Optional[datetime] = None,
) -> tuple[datetime, str]:
    """
    Compute next FMP check timestamp and stage label.

    Pre-release cadence (relative to expected_at anchor):
      expected_at − 30 min → pre_release_m30
      expected_at − 15 min → pre_release_m15
      expected_at −  5 min → pre_release_m5
      expected_at +  0 min → at_release
      expected_at +  1 min → post_release_m1

    Post-release rolling polls (no results yet):
      every 60 s until expected_at + 30 min
      every 120 s until expected_at + 2 h
      every 300 s until expected_at + 24 h
      every 6 h  thereafter (expired)

    Once results are detected: every 300 s for corrections window.
    """
    if results_detected_at is not None:
        return now_utc + timedelta(seconds=300), "post_results"

    if expected_at is None:
        return now_utc + timedelta(seconds=_FMP_INTERVAL_ACTIVE), "active"

    checkpoints = [
        (expected_at - timedelta(minutes=30), "pre_release_m30"),
        (expected_at - timedelta(minutes=15), "pre_release_m15"),
        (expected_at - timedelta(minutes=5),  "pre_release_m5"),
        (expected_at,                         "at_release"),
        (expected_at + timedelta(minutes=1),  "post_release_m1"),
    ]
    for ts, stage in checkpoints:
        if ts > now_utc:
            return ts, stage

    # Past all pre-release checkpoints — rolling poll
    elapsed = (now_utc - expected_at).total_seconds()
    if elapsed < 1800:    # < 30 min
        return now_utc + timedelta(seconds=60), "polling_60s"
    elif elapsed < 7200:  # 30 min – 2 h
        return now_utc + timedelta(seconds=120), "polling_120s"
    elif elapsed < 86400: # 2 h – 24 h
        return now_utc + timedelta(seconds=300), "polling_300s"
    else:                 # > 24 h
        return now_utc + timedelta(hours=6), "expired"


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

    Part 2 changes:
      • One call to get_earnings_calendar_with_times() for the full date window
        instead of N per-symbol calls — consumes exactly 1 FMP budget unit.
      • FMP Starter ignores the symbol filter on earnings-calendar, so we fetch
        the whole calendar and filter client-side by our universe set.
      • Timing fields (expected_timing, expected_at, report_time_status, …) are
        extracted from the real FMP response and persisted for precise scheduling.
      • Fallback: for symbols with existing active targets that didn't appear in
        the calendar, a single get_earnings_history call confirms the date.
    """
    global _last_schedule_refresh
    now_ts = time.time()
    if now_ts - _last_schedule_refresh < _SCHEDULE_REFRESH_TTL:
        return
    _last_schedule_refresh = now_ts

    from data.earnings_monitor_store import upsert_target
    import asyncio as _aio

    today        = now_et.date()
    from_date    = (today - timedelta(days=2)).strftime("%Y-%m-%d")
    to_date      = (today + timedelta(days=10)).strftime("%Y-%m-%d")
    window_start = today - timedelta(days=2)
    window_end   = today + timedelta(days=10)

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

    # Build universe set for O(1) lookup
    sym_set = {s.upper().strip() for s in symbols if s}
    now_utc = datetime.now(timezone.utc)
    run_ts  = now_utc.isoformat()

    # ── One FMP call: full calendar for window ────────────────────────────────
    cal_rows: list[dict] = []
    if daily_budget.can_spend("fmp", 1):
        daily_budget.spend("fmp")
        try:
            cal_rows = await asyncio.wait_for(
                fmp.get_earnings_calendar_with_times(from_date, to_date),
                timeout=25.0,
            )
        except Exception as exc:
            print(f"[EarnMon] calendar fetch error: {exc}")

    # Build per-symbol lookup from calendar rows (last row wins if dupes)
    cal_by_sym: dict[str, dict] = {}
    for row in cal_rows:
        sym = (row.get("symbol") or "").upper().strip()
        if not sym:
            continue
        date_str = row.get("expected_date") or ""
        # Validate date falls in window
        try:
            rec_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            continue
        if not (window_start <= rec_date <= window_end):
            continue
        if sym in sym_set:
            cal_by_sym[sym] = row

    # ── Secondary source: regular FMP calendar (no includeReportTimes) ───────
    # One additional budget unit.  Catches symbols the with-times endpoint
    # omits entirely (e.g. RR) and symbols FMP confirmed after the with-times
    # cache was last populated (e.g. TEVA/UMC/QURE at the 04:00 ET run).
    # Symbols already in cal_by_sym are not touched — with-times always wins.
    # Timing fields are left null for these rows; no timing is fabricated.
    reg_matches = 0
    if daily_budget.can_spend("fmp", 1):
        daily_budget.spend("fmp")
        try:
            reg_rows = await asyncio.wait_for(
                fmp.get_earnings_calendar(from_date, to_date),
                timeout=25.0,
            )
        except Exception as exc:
            print(f"[EarnMon] regular calendar fetch error: {exc}")
            reg_rows = []

        for row in reg_rows:
            sym = (row.get("symbol") or "").upper().strip()
            if not sym or sym not in sym_set or sym in cal_by_sym:
                continue
            date_str = row.get("date") or ""
            try:
                rec_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except Exception:
                continue
            if not (window_start <= rec_date <= window_end):
                continue
            cal_by_sym[sym] = {
                "expected_date":    date_str,
                "expected_timing":  None,
                "report_time_status": "unknown",
                "fiscal_period":    None,
                "fiscal_year":      None,
                "report_period":    None,
                "schedule_source":  "fmp_earnings_calendar",
            }
            reg_matches += 1

    cal_matches = len(cal_by_sym)
    processed   = 0
    fallback_calls = 0

    for sym in symbols:
        sym = sym.upper().strip()
        if not sym:
            continue

        cal_row = cal_by_sym.get(sym)
        if cal_row:
            # ── Primary path: data from earnings-calendar ─────────────────────
            date_str           = cal_row.get("expected_date")
            timing             = cal_row.get("expected_timing")
            report_time_status = cal_row.get("report_time_status") or "unknown"
            fiscal_period      = cal_row.get("fiscal_period")
            fiscal_year        = cal_row.get("fiscal_year")
            report_period      = cal_row.get("report_period")
            schedule_source    = "fmp_earnings_calendar"
        else:
            # ── Fallback: per-symbol get_earnings_history ─────────────────────
            # Only run for symbols that have or recently had a target in our window.
            # Limit to avoid exhausting budget across 400 symbols.
            if fallback_calls >= 30 or not daily_budget.can_spend("fmp", 1):
                continue
            try:
                daily_budget.spend("fmp")
                fallback_calls += 1
                records = await fmp.get_earnings_history(sym, limit=4)
                await asyncio.sleep(0.05)
            except Exception as exc:
                print(f"[EarnMon] fallback history error {sym}: {exc}")
                continue

            date_str = None
            for rec in records:
                ds = rec.get("date")
                if not ds:
                    continue
                try:
                    rd = datetime.strptime(ds, "%Y-%m-%d").date()
                except ValueError:
                    continue
                if window_start <= rd <= window_end:
                    date_str = ds
                    break
            if not date_str:
                continue  # no upcoming earnings for this symbol in window

            timing             = None
            report_time_status = "unknown"
            fiscal_period      = None
            fiscal_year        = None
            report_period      = None
            schedule_source    = "fmp_earnings"

        if not date_str:
            continue

        # ── Compute scheduling fields ─────────────────────────────────────────
        expected_at_dt  = _compute_expected_at(date_str, timing)
        expected_at_iso = expected_at_dt.isoformat() if expected_at_dt else None
        time_local      = _expected_time_local(timing)
        start_iso       = _monitoring_start_et(date_str, timing)
        end_iso         = _monitoring_end_et(date_str, timing)

        # Initial poll checkpoint: schedule from expected_at
        next_check_dt, _ = _compute_next_fmp_check(expected_at_dt, now_utc)
        next_check = next_check_dt.isoformat()

        await _aio.to_thread(
            upsert_target,
            sym, date_str, timing, fiscal_period, fiscal_year,
            "scheduled", start_iso, end_iso, next_check, next_check,
            expected_at=expected_at_iso,
            expected_time_local=time_local,
            report_time_status=report_time_status,
            report_period=report_period,
            schedule_source=schedule_source,
            schedule_updated_at=run_ts,
        )
        processed += 1

    print(
        f"[EarnMon] schedule refresh done: {processed}/{len(symbols)} symbols upserted, "
        f"{cal_matches} calendar matches (wt={cal_matches - reg_matches}, reg={reg_matches}), "
        f"{fallback_calls} fallback calls"
    )


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

def _select_matching_fmp_result(
    records: list[dict],
    expected_date: str | None,
    fiscal_year: int | None,
    fiscal_period: str | None,
) -> dict | None:
    """
    From a list of raw FMP earnings records, return the one that matches the
    current target quarter, or None if no qualifying record exists.

    A qualifying record must have at least one provider-confirmed actual:
      - eps_actual  (value present, including zero), OR
      - revenue_actual (value present, including zero)

    Two-pass deterministic selection
    ---------------------------------
    PASS 1 — FISCAL MATCH  (global first pass; runs when target has both labels)
      - Collect actual-bearing candidates that also carry usable fiscal_year +
        fiscal_period labels.
      - If ANY such labeled candidates exist, restrict the result set to them:
          * Exact match on both fiscal_year and fiscal_period → return that row.
          * Labeled candidates present but none match the target → return None.
            Do NOT fall back to date proximity when labeled rows are available.
      - If zero labeled candidates exist, fall through to Pass 2.

    PASS 2 — DATE-PROXIMITY  (used only when no usable labeled candidates exist)
      - Requires expected_date; if absent → return None.
      - Evaluate ALL actual-bearing candidates with a parseable date field.
      - Discard candidates whose |date − expected_date| > 7 calendar days.
      - Select the candidate with the smallest distance (nearest date).
      - Tie-breaker: prefer the later result date (more recent filing wins).
      - Provider ordering must NOT determine the selected row.

    Robustness
    ----------
    - Malformed or missing date strings are skipped without raising.
    - Malformed fiscal labels are skipped without raising.
    - EPS-only and revenue-only actuals are both accepted as qualifying.
    - No I/O, no state changes — pure function.
    """
    from datetime import date as _d

    # Filter to actual-bearing candidates.
    candidates = [
        rec for rec in records
        if rec.get("eps_actual") is not None or rec.get("revenue_actual") is not None
    ]

    if not candidates:
        return None

    # ── PASS 1: global fiscal-label match ─────────────────────────────────────
    if fiscal_year and fiscal_period:
        target_fy  = int(fiscal_year)
        target_fp  = str(fiscal_period).upper()

        labeled = []
        for rec in candidates:
            try:
                rec_fy = rec.get("fiscal_year")
                rec_fp = rec.get("fiscal_period")
                if rec_fy is not None and rec_fp:
                    labeled.append((int(rec_fy), str(rec_fp).upper(), rec))
            except (ValueError, TypeError):
                pass  # malformed label — skip

        if labeled:
            # Labeled rows exist → only accept an exact match; never fall back.
            for fy, fp, rec in labeled:
                if fy == target_fy and fp == target_fp:
                    return rec
            # Labeled candidates present but none matched → definitive miss.
            return None
        # No labeled candidates → fall through to Pass 2.

    # ── PASS 2: date-proximity (nearest within 7 days) ────────────────────────
    if not expected_date:
        # No validation context — never return an unverified row.
        return None

    try:
        exp = _d.fromisoformat(str(expected_date))
    except (ValueError, TypeError):
        return None

    best_dist: int | None = None
    best_rec:  dict | None = None
    best_date: _d | None  = None

    for rec in candidates:
        try:
            rec_date_str = rec.get("date")
            if not rec_date_str:
                continue
            rec_date = _d.fromisoformat(str(rec_date_str))
            dist = abs((rec_date - exp).days)
            if dist > 7:
                continue
            # Prefer smaller distance; on a tie, prefer the later result date.
            if (
                best_dist is None
                or dist < best_dist
                or (dist == best_dist and best_date is not None and rec_date > best_date)
            ):
                best_dist = dist
                best_rec  = rec
                best_date = rec_date
        except (ValueError, TypeError):
            pass  # malformed date — skip safely

    return best_rec


async def _check_fmp_results(
    symbol: str,
    expected_date: str | None = None,
    fiscal_year: int | None = None,
    fiscal_period: str | None = None,
) -> dict | None:
    """
    Fetch FMP live earnings records and return the one matching the current
    target quarter.  Returns None when no qualifying current record exists so
    the caller does not advance the event or target state prematurely.

    Parameters
    ----------
    symbol        : ticker symbol
    expected_date : ISO-8601 date the monitor expects results (from target)
    fiscal_year   : target fiscal year  (used for label-based matching)
    fiscal_period : target fiscal period, e.g. ``"Q2"`` (used for label matching)
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
        return _select_matching_fmp_result(records, expected_date, fiscal_year, fiscal_period)
    except Exception as exc:
        print(f"[EarnMon] FMP results check error {symbol}: {exc}")
        return None


# ── EI refresh trigger ─────────────────────────────────────────────────────────

async def _trigger_ei_refresh(symbol: str) -> bool:
    """
    Narrow single-symbol Earnings Intelligence refresh.
    Called as a fire-and-forget background task after new results are detected.
    Uses FmpFundamentalsRefresher._fetch_earnings_intelligence() (up to 8 FMP calls)
    then merges the result into watchlist_fundamentals_cache via merge_fields().
    Zero side effects on failure — catches all exceptions.
    """
    try:
        import asyncio as _aio
        from services.watchlist_fundamentals_refresh import FmpFundamentalsRefresher
        from data.watchlist_fundamentals_store import merge_fields

        fmp_key = os.environ.get("FMP_API_KEY", "")
        if not fmp_key:
            return False

        refresher = FmpFundamentalsRefresher(fmp_api_key=fmp_key)
        ei_data = await asyncio.wait_for(
            refresher._fetch_earnings_intelligence(symbol.upper()),
            timeout=50.0,
        )
        if not ei_data:
            print(f"[EarnMon] EI refresh: no data returned for {symbol}")
            return False

        ei_data.pop("_call_count", None)
        ok = await _aio.to_thread(
            merge_fields, symbol.upper(), {"earnings_intelligence": ei_data}
        )
        print(f"[EarnMon] EI refresh {symbol}: merged={ok}")
        return bool(ok)
    except asyncio.TimeoutError:
        print(f"[EarnMon] EI refresh timeout {symbol}")
        return False
    except Exception as exc:
        print(f"[EarnMon] EI refresh error {symbol}: {exc}")
        return False


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
    catchup_mode: bool = False,
) -> None:
    """
    Process one earnings target: SEC check + FMP check + state transitions.
    """
    import asyncio as _aio
    from data.earnings_monitor_store import (
        claim_target, update_target,
        upsert_live_event, get_live_event_for_target,
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

    # Bind state exclusively to this scheduled event.  A prior quarter's
    # complete event must never suppress polling or overwrite this target.
    existing = await _aio.to_thread(
        get_live_event_for_target,
        symbol, date_str or None, fiscal_p, fiscal_y, dry_run,
    )
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

    # catchup_mode: suppress SEC for old events (filing already days old)
    if catchup_mode:
        should_sec = False

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
    if fmp_due and not dry_run and not catchup_mode:
        # catchup_mode always forces FMP check regardless of scheduled time
        try:
            fmp_dt = datetime.fromisoformat(str(fmp_due))
            if fmp_dt.tzinfo is None:
                fmp_dt = fmp_dt.replace(tzinfo=timezone.utc)
            should_fmp = now_utc >= fmp_dt
        except Exception:
            pass

    if should_fmp and not _has_complete_results_for_target(existing_results, date_str or None):
        # catchup_mode bypasses window gate — fills results any time of day
        if _is_monitoring_window(now_et, timing) or dry_run or catchup_mode:
            fmp_rec = await _check_fmp_results(
                symbol,
                expected_date=date_str or None,
                fiscal_year=fiscal_y,
                fiscal_period=fiscal_p,
            )
            _STATE["check_count"] += 1
            if fmp_rec and (
                fmp_rec.get("eps_actual") is not None
                or fmp_rec.get("revenue_actual") is not None
            ):
                _STATE["fmp_detections"] += 1
                # Enrich fiscal labels if FMP returned them
                # (FMP stable/earnings doesn't have period; use income-statement)
                incoming_rp = {
                    "eps_estimate":       fmp_rec.get("eps_estimate"),
                    "eps_actual":         fmp_rec.get("eps_actual"),
                    "revenue_estimate":   fmp_rec.get("revenue_estimate"),
                    "revenue_actual":     fmp_rec.get("revenue_actual"),
                    "date":               fmp_rec.get("date"),
                }
                rp = _merge_results_payload(existing_results, incoming_rp)
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

            # ── Precise scheduling via expected_at anchor ────────────────────
            _exp_at_raw = target.get("expected_at")
            _exp_at_dt: Optional[datetime] = None
            if _exp_at_raw:
                try:
                    _exp_at_dt = datetime.fromisoformat(str(_exp_at_raw))
                    if _exp_at_dt.tzinfo is None:
                        _exp_at_dt = _exp_at_dt.replace(tzinfo=timezone.utc)
                except Exception:
                    pass

            _rfda_raw = target.get("results_first_detected_at")
            _rfda_dt: Optional[datetime] = None
            if _rfda_raw and new_state in ("results_available","complete","results_updated"):
                try:
                    _rfda_dt = datetime.fromisoformat(str(_rfda_raw))
                    if _rfda_dt.tzinfo is None:
                        _rfda_dt = _rfda_dt.replace(tzinfo=timezone.utc)
                except Exception:
                    pass

            _next_fmp_dt, _next_stage = _compute_next_fmp_check(_exp_at_dt, now_utc, _rfda_dt)
            next_fmp = _next_fmp_dt.isoformat()
            _tgt_updates: dict = {"next_fmp_check_at": next_fmp, "fmp_check_stage": _next_stage}

            # Record first detection timestamp when results first seen
            if (
                new_state in ("results_available","results_updated","results_partial")
                and not target.get("results_first_detected_at")
            ):
                _tgt_updates["results_first_detected_at"] = now_utc.isoformat()

            await _aio.to_thread(update_target, target_id, **_tgt_updates)

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

    # ── EI refresh on first structured results (fire-and-forget) ─────────────
    if state_changed and new_state in ("results_available", "results_updated") and not dry_run:
        asyncio.create_task(_trigger_ei_refresh(symbol))

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


def _merge_results_payload(existing: dict | None, incoming: dict) -> dict:
    """Merge one FMP observation without allowing a later null to erase data."""
    prior = existing or {}
    merged = dict(prior)
    for key in (
        "eps_estimate", "eps_actual", "revenue_estimate", "revenue_actual", "date",
    ):
        if incoming.get(key) is not None:
            merged[key] = incoming[key]

    merged["eps_surprise_amount"] = _safe_diff(
        merged.get("eps_actual"), merged.get("eps_estimate")
    )
    merged["eps_surprise_pct"] = _safe_pct(
        merged.get("eps_actual"), merged.get("eps_estimate")
    )
    merged["revenue_surprise_amount"] = _safe_diff(
        merged.get("revenue_actual"), merged.get("revenue_estimate")
    )
    merged["revenue_surprise_pct"] = _safe_pct(
        merged.get("revenue_actual"), merged.get("revenue_estimate")
    )
    return merged


def _has_complete_results_for_target(
    payload: dict | None,
    expected_date: str | None,
) -> bool:
    """True only for both actuals on the scheduled event, never a stale quarter."""
    if not payload or payload.get("eps_actual") is None or payload.get("revenue_actual") is None:
        return False
    if not expected_date:
        return False
    try:
        from datetime import date as _date
        return abs((_date.fromisoformat(str(payload.get("date"))) -
                    _date.fromisoformat(expected_date)).days) <= 7
    except (TypeError, ValueError):
        return False


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
    now_et: datetime | None  = None,
    dry_run: bool            = False,
    force_symbol: str | None = None,
    tick_mode: bool          = False,
    catchup_mode: bool       = False,
) -> dict:
    """
    Idempotent monitoring pass.  Safe to call from CLI, scheduled job, or tick loop.

    tick_mode=True  — autoscale-native post-yield scheduler path:
        • Skips _build_universe() (expensive watchlist scan).
        • Uses the symbols already registered in earnings_monitor_targets as the
          schedule-refresh universe; no new symbols are added.
        • Processes only targets that are due RIGHT NOW (get_due_targets).
        • No universe membership filter — all DB-registered targets are eligible.
    """
    if now_et is None:
        now_et = _now_et()

    _STATE["run_count"] += 1
    _STATE["last_run"] = datetime.now(timezone.utc).isoformat()

    try:
        import asyncio as _aio
        from data.earnings_monitor_store import get_due_targets, get_active_targets

        if force_symbol:
            # ── force-symbol path: single-symbol targeted check ─────────────
            sym      = force_symbol.upper()
            universe = [sym]
            await _refresh_schedule(universe, now_et)
            all_active = await _aio.to_thread(get_active_targets, 200)
            targets    = [t for t in all_active if t["symbol"] == sym]

        elif tick_mode:
            # ── tick mode: use existing DB targets, no watchlist scan ────────
            # When the schedule TTL is expiring, augment with the full watchlist
            # universe so _refresh_schedule can discover and register new targets
            # (e.g. TEVA/UMC/QURE/RR that were absent from the with-times
            # calendar at the time of the last refresh).  _build_universe() is
            # called only when _refresh_schedule will actually do real work —
            # the TTL check here matches the TTL guard inside _refresh_schedule.
            all_active  = await _aio.to_thread(get_active_targets, 500)
            active_syms = [t["symbol"] for t in all_active]
            if time.time() - _last_schedule_refresh >= _SCHEDULE_REFRESH_TTL:
                full_universe    = await _build_universe()
                refresh_universe = list(set(active_syms) | set(full_universe))
            else:
                refresh_universe = active_syms
            await _refresh_schedule(refresh_universe, now_et)   # TTL-protected
            targets  = await _aio.to_thread(get_due_targets, 200)
            universe = None  # None signals: skip membership filter below

        else:
            # ── normal path: full universe build ────────────────────────────
            universe = await _build_universe()
            await _refresh_schedule(universe, now_et)
            targets  = await _aio.to_thread(get_due_targets, 200)

        _STATE["active_target_count"] = len(targets)

        worker_id = _STATE["worker_id"]
        errors    = 0
        processed = 0

        for target in targets:
            sym = target.get("symbol", "")
            if not sym:
                continue
            # Universe membership filter — skipped in tick_mode and force_symbol
            if universe is not None and not force_symbol and sym not in universe:
                continue
            try:
                await _process_target(target, worker_id, now_et, dry_run,
                                      catchup_mode=catchup_mode)
                processed += 1
            except Exception as exc:
                errors += 1
                _STATE["failures"] += 1
                print(f"[EarnMon] target error {sym}: {exc}")

        _STATE["last_success"] = datetime.now(timezone.utc).isoformat()
        return {
            "processed": processed,
            "errors":    errors,
            "universe":  len(universe) if universe is not None else len(targets),
            "targets":   len(targets),
            "tick_mode": tick_mode,
        }
    except Exception as exc:
        _STATE["failures"] += 1
        print(f"[EarnMon] run_once top-level error: {exc}")
        return {"error": str(exc)}


async def live_earnings_monitor_loop(interval_seconds: int = 30) -> None:
    """
    Persistent monitoring loop. Started only after FastAPI lifespan yield.
    Controlled by LIVE_EARNINGS_MONITOR_ENABLED env var (Reserved VM mode).
    For Autoscale deployments use earnings_monitor_tick_loop instead.
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


# ── autoscale-native tick loop ─────────────────────────────────────────────────

_TICK_INITIAL_DELAY_S = 30   # let bootstrap settle before first tick
_TICK_INTERVAL_S      = 60   # wake every minute

async def earnings_monitor_tick_loop() -> None:
    """
    Post-yield scheduler for Autoscale deployments.  Wakes every 60 seconds,
    processes only earnings_monitor_targets that are due RIGHT NOW, and exits
    immediately when nothing is due.  No full watchlist scan occurs.

    Differences from live_earnings_monitor_loop:
        • Always started (no env-var gate) — Autoscale native.
        • Calls run_live_earnings_monitor_once(tick_mode=True) which skips
          _build_universe() and uses the DB-registered target list.
        • Targets with no due check-time are skipped (get_due_targets).
        • High-frequency cadence (-30/-15/-5/0/+1 min) is driven by the
          expected_at anchor already stored in each target row.
    """
    _STATE["tick_loop_enabled"] = True
    print(f"[EarnMon] tick loop started (initial_delay={_TICK_INITIAL_DELAY_S}s, interval={_TICK_INTERVAL_S}s)")
    await asyncio.sleep(_TICK_INITIAL_DELAY_S)
    while True:
        try:
            _STATE["tick_loop_last_tick"]  = datetime.now(timezone.utc).isoformat()
            _STATE["tick_loop_tick_count"] = _STATE["tick_loop_tick_count"] + 1
            result = await run_live_earnings_monitor_once(tick_mode=True)
            if result.get("processed", 0) > 0:
                print(f"[EarnMon][tick] processed={result['processed']} targets={result['targets']}")
        except Exception as exc:
            _STATE["failures"] += 1
            print(f"[EarnMon] tick loop error: {exc}")
        await asyncio.sleep(_TICK_INTERVAL_S)


# ── startup catch-up pass ──────────────────────────────────────────────────────

_CATCHUP_LOOKBACK_DAYS  = 4    # inspect earnings from up to 4 calendar days ago
_CATCHUP_STARTUP_DELAY  = 25   # seconds to wait after yield before running
_CATCHUP_RETRY_DELAY_S  = 1800 # 30-min bounded retry when FMP has no data yet

async def _earnings_catchup_pass() -> dict:
    """
    Runs once per process start after a short startup delay.

    Selects recent earnings targets (past _CATCHUP_LOOKBACK_DAYS days) whose
    live-event state is not yet 'complete', makes ONE targeted FMP call per
    symbol, and runs the full state-transition pipeline (including alert + EI
    refresh) if results are found.

    If FMP has no data yet, the target's next_fmp_check_at is set to
    now + _CATCHUP_RETRY_DELAY_S (30 min) so the tick loop picks it up without
    hammering FMP on every tick.

    Does NOT replay pre-release (-30/-15/-5 min) intervals — those only fire
    during live windows via the tick loop.
    """
    await asyncio.sleep(_CATCHUP_STARTUP_DELAY)

    try:
        import asyncio as _aio
        from datetime import date, timedelta as _td
        from data.earnings_monitor_store import (
            get_active_targets, get_live_event_for_symbol, update_target,
        )

        now_utc    = datetime.now(timezone.utc)
        now_et     = _now_et()
        cutoff     = (date.today() - _td(days=_CATCHUP_LOOKBACK_DAYS)).isoformat()
        today_str  = date.today().isoformat()
        worker_id  = _STATE["worker_id"]

        all_active = await _aio.to_thread(get_active_targets, 500)

        # Select targets in the recent window that are not yet complete.
        # get_active_targets() includes same-day complete targets in its result
        # set (via a simple date-window clause) so the scheduler can decide
        # whether a re-check is needed.  The Python filter below is the
        # application-level gate that keeps the catchup pass focused on
        # genuinely incomplete targets only; _has_complete_results_for_target()
        # in _process_target handles the same-day integrity check for due targets.
        recent = [
            t for t in all_active
            if (
                t.get("expected_date")
                and str(t["expected_date"]) >= cutoff
                and str(t["expected_date"]) <= today_str
                and t.get("status") not in ("complete",)
            )
        ]

        if not recent:
            print("[EarnMon][catchup] no recent incomplete targets — nothing to do")
            _STATE["catchup_last_run"] = now_utc.isoformat()
            return {"checked": 0, "filled": 0, "ei_triggered": 0}

        print(f"[EarnMon][catchup] inspecting {len(recent)} recent target(s)")

        checked      = 0
        filled       = 0
        ei_triggered = 0
        retry_set    = 0

        for target in recent:
            symbol = (target.get("symbol") or "").upper()
            if not symbol:
                continue

            # Load current live event to see how far state machine has progressed
            existing = await _aio.to_thread(get_live_event_for_symbol, symbol, False)
            existing_state = (existing.get("state") or "scheduled") if existing else "scheduled"

            if existing_state == "complete":
                # Already fully resolved.  If EI was never triggered (edge case
                # where process restarted before the fire-and-forget task ran),
                # re-trigger it now.
                if not target.get("results_first_detected_at"):
                    asyncio.create_task(_trigger_ei_refresh(symbol))
                    ei_triggered += 1
                continue

            checked += 1

            # Force the FMP check to run regardless of next_fmp_check_at and
            # window time by using catchup_mode=True in _process_target.
            # To satisfy the `should_fmp` gate we patch the target dict in-memory.
            target_patched = dict(target)
            target_patched["next_fmp_check_at"] = now_utc.isoformat()

            # First, probe FMP so we can decide between fill vs bounded-retry
            # without burning two FMP calls when data is absent.
            probe = await _check_fmp_results(
                symbol,
                expected_date=str(target.get("expected_date") or "") or None,
                fiscal_year=target.get("fiscal_year"),
                fiscal_period=target.get("fiscal_period"),
            )

            if probe and (
                probe.get("eps_actual") is not None
                or probe.get("revenue_actual") is not None
            ):
                # Results are on FMP — run full state machine to get
                # alert + EI refresh + state transition in one pass.
                try:
                    await _process_target(
                        target_patched, worker_id, now_et,
                        dry_run=False, catchup_mode=True,
                    )
                    filled += 1
                    print(f"[EarnMon][catchup] {symbol}: filled (eps={probe.get('eps_actual')})")
                except Exception as exc:
                    print(f"[EarnMon][catchup] _process_target error {symbol}: {exc}")
            else:
                # FMP doesn't have results yet — set bounded retry so the tick
                # loop re-checks in 30 min rather than on every tick.
                retry_ts = (now_utc + timedelta(seconds=_CATCHUP_RETRY_DELAY_S)).isoformat()
                await _aio.to_thread(update_target, target["id"], next_fmp_check_at=retry_ts)
                retry_set += 1
                print(f"[EarnMon][catchup] {symbol}: no results yet — retry in {_CATCHUP_RETRY_DELAY_S//60}min")

            # Yield to event loop between targets so API requests are not blocked
            await asyncio.sleep(0)

        _STATE["catchup_last_run"]        = now_utc.isoformat()
        _STATE["catchup_symbols_checked"] = checked
        _STATE["catchup_results_filled"]  = filled
        _STATE["catchup_ei_triggered"]    = ei_triggered

        print(
            f"[EarnMon][catchup] complete: "
            f"checked={checked} filled={filled} ei_triggered={ei_triggered} retry_set={retry_set}"
        )

        # ── reaction finalization pass (separate concern, non-blocking) ───────
        # max_symbols=15 bounds the startup pass so it completes quickly and
        # does not starve API request serving.  The tick loop handles the rest.
        reaction_result: dict = {}
        try:
            from services.earnings_reaction_service import reaction_catchup_pass
            reaction_result = await reaction_catchup_pass(lookback_days=10, max_symbols=15)
        except Exception as _rex:
            print(f"[EarnMon][catchup] reaction_catchup_pass error: {_rex}")

        return {
            "checked": checked,
            "filled":  filled,
            "ei_triggered": ei_triggered,
            "retry_set": retry_set,
            "reaction": reaction_result,
        }

    except Exception as exc:
        print(f"[EarnMon][catchup] top-level error: {exc}")
        _STATE["catchup_last_run"] = datetime.now(timezone.utc).isoformat()
        return {"error": str(exc)}


def get_monitor_status() -> dict:
    sched_age = time.time() - _last_schedule_refresh if _last_schedule_refresh > 0 else None
    return {
        **_STATE,
        # ── schedule refresh ──────────────────────────────────────────────────
        "schedule_refresh_ttl_s":    _SCHEDULE_REFRESH_TTL,
        "schedule_refresh_age_s":    round(sched_age, 1) if sched_age is not None else None,
        "last_schedule_refresh_at": (
            datetime.fromtimestamp(_last_schedule_refresh, tz=timezone.utc).isoformat()
            if _last_schedule_refresh > 0 else None
        ),
        "next_schedule_refresh_at": (
            datetime.fromtimestamp(
                _last_schedule_refresh + _SCHEDULE_REFRESH_TTL, tz=timezone.utc
            ).isoformat()
            if _last_schedule_refresh > 0 else None
        ),
        # ── timing anchors ────────────────────────────────────────────────────
        "bmo_anchor_et": f"{_BMO_ANCHOR_H:02d}:{_BMO_ANCHOR_M:02d}",
        "amc_anchor_et": f"{_AMC_ANCHOR_H:02d}:{_AMC_ANCHOR_M:02d}",
        # ── monitoring windows ────────────────────────────────────────────────
        "bmo_window_et":  f"{_BMO_START[0]:02d}:{_BMO_START[1]:02d}–{_BMO_END[0]:02d}:{_BMO_END[1]:02d}",
        "amc_window_et":  f"{_AMC_START[0]:02d}:{_AMC_START[1]:02d}–{_AMC_END[0]:02d}:{_AMC_END[1]:02d}",
        # ── tick loop ─────────────────────────────────────────────────────────
        "tick_interval_s":      _TICK_INTERVAL_S,
        "tick_initial_delay_s": _TICK_INITIAL_DELAY_S,
        # ── catch-up ──────────────────────────────────────────────────────────
        "catchup_lookback_days": _CATCHUP_LOOKBACK_DAYS,
        "catchup_retry_delay_s": _CATCHUP_RETRY_DELAY_S,
        # ── deployment ────────────────────────────────────────────────────────
        "deployment_note": (
            "Autoscale mode: post-yield tick loop runs every 60s while the process "
            "is awake. Startup catch-up fills any results missed while scaled-to-zero. "
            "Optional persistent loop via LIVE_EARNINGS_MONITOR_ENABLED=true (VM mode)."
        ),
        "cli_command": "python -m backend.scripts.run_live_earnings_monitor_once",
    }
