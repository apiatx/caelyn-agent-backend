"""
Post-earnings price reaction finalization service.

Computes Pre-1D, Post-1D, Post-3D, Post-5D price reactions from canonical
price history and persists them into earnings_live_events.reaction_payload.

Definitions (calendar-session offsets, not calendar days):
  AMC event (reported after market close on report_date):
    pre_1d_pct   = close(report_date) / close(prior session)      - 1
    post_1d_pct  = close(+1 session)  / close(report_date)        - 1
    post_3d_pct  = close(+3 sessions) / close(report_date)        - 1
    post_5d_pct  = close(+5 sessions) / close(report_date)        - 1

  BMO event (reported before market open on report_date):
    pre_1d_pct   = close(prior session)    / close(prior-prior session) - 1
    post_1d_pct  = close(report_date)      / close(prior session)       - 1
    post_3d_pct  = close(+2 sessions after report_date) / close(prior session) - 1
    post_5d_pct  = close(+4 sessions after report_date) / close(prior session) - 1

  Horizons only computed when the required sessions exist in the bar data;
  missing values are omitted (None) rather than defaulted to zero.

Public API
──────────
compute_reaction_horizons(bars, report_date, timing) -> dict
    Pure function.  Returns reaction dict with whatever horizons are available.

async finalize_reactions_for_event(event_id, symbol, report_date, timing) -> dict | None
    Fetches/appends bars if needed, computes horizons, persists to Neon.
    Returns the computed horizon dict or None on failure.

async reaction_catchup_pass(lookback_days) -> dict
    Startup catch-up: scans complete events from the past N days, runs
    finalize_reactions_for_event for any with incomplete/missing reactions.
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from typing import Optional

_LOG = "[EarnReact]"


# ── payload normalization ──────────────────────────────────────────────────────

def _coerce_reaction_dict(rp) -> dict:
    """
    Always return a plain dict from whatever Neon hands back as reaction_payload.

    Handles: None, JSON null (already None at Python level), Python dict (normal),
    list (corrupted from old || bug — extract last dict element), str (raw JSON).
    """
    if rp is None:
        return {}
    if isinstance(rp, dict):
        return rp
    if isinstance(rp, list):
        for item in reversed(rp):
            if isinstance(item, dict):
                return item
        return {}
    if isinstance(rp, str):
        try:
            import json as _j
            parsed = _j.loads(rp)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


# ── session helpers ────────────────────────────────────────────────────────────

def _sorted_dates(bars: list[dict]) -> list[str]:
    """Sorted list of unique trading-session dates that have a valid close."""
    return sorted(
        {str(b["date"])[:10] for b in bars if b.get("close") is not None}
    )


def _nth_session_after(dates: list[str], anchor: str, n: int) -> Optional[str]:
    """Return the date n sessions AFTER anchor (anchor excluded)."""
    after = [d for d in dates if d > anchor]
    return after[n - 1] if len(after) >= n else None


def _nth_session_before(dates: list[str], anchor: str, n: int) -> Optional[str]:
    """Return the date n sessions BEFORE anchor (anchor excluded)."""
    before = [d for d in dates if d < anchor]
    return before[-n] if len(before) >= n else None


def _get_close(bars: list[dict], date_str: str) -> Optional[float]:
    """Return closing price for an exact date string."""
    for b in bars:
        if str(b.get("date") or "")[:10] == date_str:
            c = b.get("close")
            if c is not None:
                try:
                    return float(c)
                except (TypeError, ValueError):
                    pass
    return None


def _pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """Percent change: (a / b) - 1, rounded to 4 dp.  None if either input is None/0."""
    if a is None or b is None or b == 0:
        return None
    try:
        return round((float(a) / float(b) - 1) * 100, 4)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


# ── core computation ───────────────────────────────────────────────────────────

def compute_reaction_horizons(
    bars: list[dict],
    report_date: str,
    timing: Optional[str],
) -> dict:
    """
    Pure function: compute available reaction horizons from bar data.

    Returns a dict with all computed horizons.  Absent horizons are omitted.
    Always includes `report_date`, `timing`, and `computed_at`.
    """
    dates = _sorted_dates(bars)
    t = (timing or "amc").lower().strip()
    is_bmo = t in ("bmo", "before market open")

    result: dict = {
        "report_date": report_date,
        "timing":      t,
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }

    if is_bmo:
        prior         = _nth_session_before(dates, report_date, 1)
        prior_prior   = _nth_session_before(dates, report_date, 2)
        report_close  = _get_close(bars, report_date)
        prior_close   = _get_close(bars, prior) if prior else None
        prior2_close  = _get_close(bars, prior_prior) if prior_prior else None

        result["pre_1d_pct"]  = _pct(prior_close,   prior2_close)
        result["post_1d_pct"] = _pct(report_close,  prior_close)

        s2 = _nth_session_after(dates, report_date, 2)
        s4 = _nth_session_after(dates, report_date, 4)
        result["post_3d_pct"] = _pct(_get_close(bars, s2), prior_close) if s2 else None
        result["post_5d_pct"] = _pct(_get_close(bars, s4), prior_close) if s4 else None

        result["_bmo_report_close"]  = report_close
        result["_bmo_prior_session"] = prior
        result["_bmo_prior_close"]   = prior_close

    else:
        prior         = _nth_session_before(dates, report_date, 1)
        report_close  = _get_close(bars, report_date)
        prior_close   = _get_close(bars, prior) if prior else None

        result["pre_1d_pct"]  = _pct(report_close, prior_close)

        s1 = _nth_session_after(dates, report_date, 1)
        s3 = _nth_session_after(dates, report_date, 3)
        s5 = _nth_session_after(dates, report_date, 5)
        s1c = _get_close(bars, s1) if s1 else None
        s3c = _get_close(bars, s3) if s3 else None
        s5c = _get_close(bars, s5) if s5 else None

        result["post_1d_pct"] = _pct(s1c, report_close)
        result["post_3d_pct"] = _pct(s3c, report_close)
        result["post_5d_pct"] = _pct(s5c, report_close)

        result["_amc_report_close"]  = report_close
        result["_amc_prior_session"] = prior
        result["_amc_prior_close"]   = prior_close

    result["horizons_available"] = [
        k for k in ("pre_1d_pct", "post_1d_pct", "post_3d_pct", "post_5d_pct")
        if result.get(k) is not None
    ]

    return result


# ── bar fetch helper ───────────────────────────────────────────────────────────

async def _fetch_bars_window(
    symbol: str,
    report_date: str,
    extra_sessions: int = 8,
) -> list[dict]:
    """
    Try canonical history cache first; if bars are too stale (newest < report_date),
    fetch a targeted window from Tradier via canonical_history_backfill.

    Returns a merged bar list (cache + fresh), sorted by date.
    """
    import asyncio as _aio

    cached_bars: list[dict] = []

    try:
        from services.canonical_history_service import get_bars as _get_bars
        bd = await _aio.to_thread(_get_bars, symbol, False)
        if bd:
            cached_bars = bd.get("bars") or []
    except Exception as exc:
        print(f"{_LOG} cache read error {symbol}: {exc}")

    dates_in_cache = _sorted_dates(cached_bars)
    newest = dates_in_cache[-1] if dates_in_cache else ""

    if newest >= report_date:
        print(f"{_LOG} {symbol}: cache is fresh enough (newest={newest})")
        return cached_bars

    print(f"{_LOG} {symbol}: cache stale (newest={newest} < report_date={report_date}), fetching from Tradier")

    try:
        from services.canonical_history_backfill import _fetch_tradier_history_managed
        rd = date.fromisoformat(report_date)
        start = (rd - timedelta(days=14)).isoformat()
        end   = (rd + timedelta(days=extra_sessions * 2)).isoformat()
        fresh_bars = await asyncio.wait_for(
            _fetch_tradier_history_managed(symbol, start, end),
            timeout=20.0,
        )
    except asyncio.TimeoutError:
        print(f"{_LOG} {symbol}: Tradier fetch timeout")
        fresh_bars = []
    except Exception as exc:
        print(f"{_LOG} {symbol}: Tradier fetch error: {exc}")
        fresh_bars = []

    if not fresh_bars:
        print(f"{_LOG} {symbol}: no fresh bars returned — using stale cache only")
        return cached_bars

    merged: dict[str, dict] = {str(b["date"])[:10]: b for b in cached_bars}
    for b in fresh_bars:
        d = str(b.get("date") or "")[:10]
        if d:
            merged[d] = b

    result = sorted(merged.values(), key=lambda x: str(x.get("date") or ""))
    print(f"{_LOG} {symbol}: merged bars={len(result)}, newest={result[-1]['date'] if result else 'n/a'}")

    try:
        from services.canonical_history_service import append_bars as _append
        _append(symbol, fresh_bars, "tradier_reaction")
    except Exception:
        pass

    return result


# ── per-event finalizer ────────────────────────────────────────────────────────

async def finalize_reactions_for_event(
    event_id: str,
    symbol: str,
    report_date: str,
    timing: Optional[str],
    existing_reaction: Optional[dict] = None,
) -> Optional[dict]:
    """
    Fetch bars, compute available reaction horizons, and persist to Neon.

    Skips horizons that are already present in existing_reaction (merge-safe).
    Returns the newly computed horizon dict (may be partial), or None on failure.
    """
    import asyncio as _aio

    sym = symbol.upper()
    existing = _coerce_reaction_dict(existing_reaction)

    needed_new = [
        h for h in ("pre_1d_pct", "post_1d_pct", "post_3d_pct", "post_5d_pct")
        if existing.get(h) is None
    ]
    if not needed_new:
        print(f"{_LOG} {sym}: all horizons already present — skipping")
        return None

    try:
        bars = await _fetch_bars_window(sym, report_date)
    except Exception as exc:
        print(f"{_LOG} {sym}: bar fetch failed: {exc}")
        return None

    if not bars:
        print(f"{_LOG} {sym}: no bars available")
        return None

    horizons = compute_reaction_horizons(bars, report_date, timing)

    new_data = {k: v for k, v in horizons.items() if k in needed_new and v is not None}
    if not new_data:
        print(f"{_LOG} {sym}: no new horizons computable (report_date={report_date})")
        return horizons

    payload_to_merge = {
        **new_data,
        "computed_at": horizons["computed_at"],
        "timing":      horizons["timing"],
        "report_date": horizons["report_date"],
        "horizons_available": horizons.get("horizons_available", []),
    }

    try:
        from data.earnings_monitor_store import update_reaction_payload
        ok = await _aio.to_thread(update_reaction_payload, event_id, payload_to_merge, True)
        if ok:
            print(f"{_LOG} {sym}: persisted horizons {list(new_data.keys())} → event_id={event_id}")
        else:
            print(f"{_LOG} {sym}: persist failed (update_reaction_payload returned False)")
    except Exception as exc:
        print(f"{_LOG} {sym}: persist error: {exc}")

    return horizons


# ── startup catch-up pass ─────────────────────────────────────────────────────

_REACTION_CATCHUP_LOOKBACK_DAYS = 10

async def reaction_catchup_pass(lookback_days: int = _REACTION_CATCHUP_LOOKBACK_DAYS) -> dict:
    """
    Startup-safe catch-up: finds complete/results_* events from the last
    `lookback_days` days that have incomplete or missing reaction horizons,
    fetches bars, and finalizes them.

    Designed to be called at the end of _earnings_catchup_pass() so it runs
    once per process start without blocking the main lifespan yield.
    """
    import asyncio as _aio

    since_date = (date.today() - timedelta(days=lookback_days)).isoformat()

    try:
        from data.earnings_monitor_store import get_recent_live_events
        all_recent = await _aio.to_thread(
            get_recent_live_events,
            200,
            since_date,
            False,
        )
    except Exception as exc:
        print(f"{_LOG}[catchup] DB query error: {exc}")
        return {"error": str(exc)}

    result_states = {"results_available", "results_updated", "complete"}
    candidates = [
        ev for ev in all_recent
        if ev.get("state") in result_states
    ]

    if not candidates:
        print(f"{_LOG}[catchup] no complete events in last {lookback_days} days — nothing to do")
        return {"checked": 0, "finalized": 0, "already_complete": 0}

    WANTED = {"pre_1d_pct", "post_1d_pct", "post_3d_pct", "post_5d_pct"}

    checked           = 0
    finalized         = 0
    already_complete  = 0

    for ev in candidates:
        sym = (ev.get("symbol") or "").upper()
        if not sym:
            continue

        rp   = _coerce_reaction_dict(ev.get("reaction_payload"))
        have = {k for k in WANTED if rp.get(k) is not None}

        report_date = str(ev.get("expected_date") or "")[:10]
        if not report_date:
            continue

        timing_field = None
        try:
            from data.earnings_monitor_store import get_targets_for_symbols
            targets = await _aio.to_thread(get_targets_for_symbols, [sym])
            for t in targets:
                if str(t.get("expected_date") or "")[:10] == report_date:
                    timing_field = t.get("expected_timing")
                    break
        except Exception:
            pass

        checked += 1

        if have >= {"pre_1d_pct", "post_1d_pct"}:
            now_str = date.today().isoformat()
            rd = date.fromisoformat(report_date)
            sessions_5d_available_after = rd + timedelta(days=8)
            if date.today() >= sessions_5d_available_after and "post_5d_pct" in have:
                already_complete += 1
                continue

        try:
            result = await finalize_reactions_for_event(
                ev["event_id"], sym, report_date, timing_field, rp
            )
            if result and result.get("horizons_available"):
                finalized += 1
        except Exception as exc:
            print(f"{_LOG}[catchup] {sym}: error: {exc}")

    print(
        f"{_LOG}[catchup] complete: "
        f"checked={checked} finalized={finalized} already_complete={already_complete}"
    )
    return {
        "checked":          checked,
        "finalized":        finalized,
        "already_complete": already_complete,
        "lookback_days":    lookback_days,
    }
