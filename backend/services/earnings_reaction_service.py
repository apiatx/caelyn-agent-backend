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
                # Backfill into watchlist_fundamentals_cache so ticker-detail
                # reflects the reaction immediately (no wait for weekly refresh).
                try:
                    await backfill_ei_price_reaction(sym)
                except Exception as _bf_exc:
                    print(f"{_LOG}[catchup] {sym}: backfill_ei warning: {_bf_exc}")
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


# ── EI price_reaction backfill ─────────────────────────────────────────────────


async def backfill_ei_price_reaction(symbol: str) -> bool:
    """
    Patch earnings_intelligence.earnings_history[*].price_reaction and
    reaction_summary in watchlist_fundamentals_cache for the most-recent
    live event whose reaction_payload has been finalized.

    This bridges the gap between:
      - earnings_live_events.reaction_payload  (written by finalize_reactions_for_event)
      - watchlist_fundamentals_cache.fields.earnings_intelligence.earnings_history
        (written by FmpFundamentalsRefresher, weekly cycle, may be stale)

    The ticker-detail endpoint and "Price Reaction Pending" frontend UI both
    read from the latter; without this backfill the reaction values are null
    until the next weekly fundamentals refresh.

    Uses a targeted SQL jsonb_set UPDATE — the rest of the snapshot (ratings,
    SEC filings, etc.) is left completely untouched.
    """
    import asyncio as _aio
    import json as _json
    import math as _math

    sym = symbol.upper()

    # ── 1. Get live_event → reaction_payload + fiscal identity ────────────
    try:
        from data.earnings_monitor_store import get_live_event_for_symbol
        ev = await _aio.to_thread(get_live_event_for_symbol, sym, False)
    except Exception as exc:
        print(f"{_LOG}[backfill_ei] {sym}: live_event fetch error: {exc}")
        return False

    if not ev:
        print(f"{_LOG}[backfill_ei] {sym}: no live_event found")
        return False

    rp = _coerce_reaction_dict(ev.get("reaction_payload"))
    if not rp or rp.get("post_1d_pct") is None:
        print(f"{_LOG}[backfill_ei] {sym}: reaction_payload empty or post_1d_pct absent")
        return False

    timing      = rp.get("timing") or "unknown"
    report_date = rp.get("report_date") or str(ev.get("expected_date") or "")[:10]
    pre_1d_pct  = rp.get("pre_1d_pct")
    post_1d_pct = rp.get("post_1d_pct")
    post_3d_pct = rp.get("post_3d_pct")
    post_5d_pct = rp.get("post_5d_pct")

    fiscal_year   = ev.get("fiscal_year")
    fiscal_period = ev.get("fiscal_period")

    if not report_date:
        print(f"{_LOG}[backfill_ei] {sym}: no report_date")
        return False

    # ── 2. Load bars to resolve session dates + baseline_close ────────────
    bars: list[dict] = []
    try:
        bars = await _fetch_bars_window(sym, report_date, extra_sessions=8)
    except Exception as exc:
        print(f"{_LOG}[backfill_ei] {sym}: bars fetch warning: {exc}")

    def _adj_close_b(bar: dict):
        v = bar.get("adjClose") or bar.get("close")
        if v is None:
            return None
        try:
            f = float(v)
            return None if (_math.isnan(f) or _math.isinf(f)) else f
        except Exception:
            return None

    sorted_bars = sorted(bars, key=lambda b: str(b.get("date", ""))[:10])
    bar_dates   = [str(b.get("date", ""))[:10] for b in sorted_bars]
    date_to_idx = {d: i for i, d in enumerate(bar_dates)}

    baseline_close        = None
    baseline_date         = None
    first_reaction_session = None
    sessions_used         = []
    pre_earnings_session  = None
    post_earnings_session = None

    ev_idx = date_to_idx.get(report_date)
    if ev_idx is not None:
        if timing == "amc":
            b = sorted_bars[ev_idx]
            baseline_close        = _adj_close_b(b)
            baseline_date         = report_date
            pre_earnings_session  = report_date
            fsi = ev_idx + 1
            if fsi < len(sorted_bars):
                first_reaction_session = bar_dates[fsi]
                post_earnings_session  = bar_dates[fsi]
                sessions_used          = bar_dates[fsi: fsi + 5]
        elif timing == "bmo":
            if ev_idx >= 1:
                baseline_close       = _adj_close_b(sorted_bars[ev_idx - 1])
                baseline_date        = bar_dates[ev_idx - 1]
                pre_earnings_session = bar_dates[ev_idx - 1]
            first_reaction_session = report_date
            post_earnings_session  = report_date
            sessions_used          = bar_dates[ev_idx: ev_idx + 5]

    calc_method   = f"{timing}_inferred" if timing in ("amc", "bmo") else "unknown_timing_close_to_close"
    calc_conf     = "inferred_high"
    reactions_final = post_5d_pct is not None and bool(sessions_used and len(sessions_used) >= 5)

    def _rnd(v):
        if v is None:
            return None
        try:
            return round(float(v), 4)
        except Exception:
            return None

    price_reaction = {
        "baseline_date":          baseline_date,
        "baseline_close":         baseline_close,
        "first_reaction_session": first_reaction_session,
        "opening_gap_pct":        None,
        "reaction_1d_pct":        _rnd(post_1d_pct),
        "reaction_3d_pct":        _rnd(post_3d_pct),
        "reaction_5d_pct":        _rnd(post_5d_pct),
        "max_upside_5d_pct":      None,
        "max_drawdown_5d_pct":    None,
        "sessions_used":          sessions_used,
        "calculation_method":     calc_method,
        "calculation_confidence": calc_conf,
        "reactions_final":        reactions_final,
        "bars_source":            "canonical_cache" if bars else "reaction_payload_only",
        # Pre/Post positioning
        "pre_earnings_1d_pct":    _rnd(pre_1d_pct),
        "post_earnings_1d_pct":   _rnd(post_1d_pct),
        "pre_earnings_session":   pre_earnings_session,
        "post_earnings_session":  post_earnings_session,
        "pre_post_method":        calc_method,
        "pre_post_confidence":    calc_conf,
    }

    # ── 3. Load watchlist_fundamentals_cache snapshot ─────────────────────
    try:
        from data.watchlist_fundamentals_store import get_snapshot as _get_snap
        snap = await _aio.to_thread(_get_snap, sym)
    except Exception as exc:
        print(f"{_LOG}[backfill_ei] {sym}: snapshot fetch error: {exc}")
        return False

    if not snap:
        print(f"{_LOG}[backfill_ei] {sym}: no snapshot in watchlist_fundamentals_cache")
        return False

    fields: dict = snap.get("fields") or {}
    ei = fields.get("earnings_intelligence")
    if not ei or not isinstance(ei, dict):
        print(f"{_LOG}[backfill_ei] {sym}: no earnings_intelligence in snapshot")
        return False

    eh: list = list(ei.get("earnings_history") or [])
    if not eh:
        print(f"{_LOG}[backfill_ei] {sym}: empty earnings_history")
        return False

    # ── 4. Match event: fiscal_year+period → date fallback ────────────────
    matched_idx = None
    for i, entry in enumerate(eh):
        if (fiscal_year is not None and fiscal_period is not None and
                str(entry.get("fiscal_year", "")) == str(fiscal_year) and
                str(entry.get("fiscal_period", "")) == str(fiscal_period)):
            matched_idx = i
            break
    if matched_idx is None and report_date:
        for i, entry in enumerate(eh):
            if str(entry.get("date", ""))[:10] == report_date[:10]:
                matched_idx = i
                break

    if matched_idx is None:
        print(
            f"{_LOG}[backfill_ei] {sym}: no match for "
            f"{fiscal_period} {fiscal_year} / date={report_date}"
        )
        return False

    # ── 5. Merge: don't overwrite already-computed good values ────────────
    patched = dict(eh[matched_idx])
    existing_pr = patched.get("price_reaction") or {}
    if isinstance(existing_pr, dict) and existing_pr.get("reaction_1d_pct") is not None:
        # Existing has real data — only fill null gaps
        merged = dict(existing_pr)
        for k, v in price_reaction.items():
            if merged.get(k) is None and v is not None:
                merged[k] = v
        patched["price_reaction"] = merged
    else:
        patched["price_reaction"] = price_reaction

    new_eh = list(eh)
    new_eh[matched_idx] = patched

    # ── 6. Recompute reaction_summary ─────────────────────────────────────
    new_summary = ei.get("reaction_summary")
    try:
        from services.watchlist_fundamentals_refresh import FmpFundamentalsRefresher as _FMR
        new_summary = _FMR._compute_reaction_summary(new_eh)
    except Exception as exc:
        print(f"{_LOG}[backfill_ei] {sym}: summary recompute error: {exc}")

    # ── 7. Targeted SQL jsonb_set UPDATE ───────────────────────────────────
    try:
        from data.pg_storage import _get_conn, _put_conn

        def _do_patch():
            conn = _get_conn()
            if conn is None:
                return False
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE watchlist_fundamentals_cache
                    SET fields = jsonb_set(
                                    jsonb_set(
                                        fields,
                                        '{earnings_intelligence,earnings_history}',
                                        %s::jsonb
                                    ),
                                    '{earnings_intelligence,reaction_summary}',
                                    %s::jsonb
                                ),
                        refreshed_at = NOW()
                    WHERE symbol = %s
                    """,
                    (
                        _json.dumps(new_eh),
                        _json.dumps(new_summary) if new_summary else "{}",
                        sym,
                    ),
                )
                n = cur.rowcount
                conn.commit()
                cur.close()
                return n > 0
            finally:
                _put_conn(conn)

        ok = await _aio.to_thread(_do_patch)
        if ok:
            r1 = price_reaction.get("reaction_1d_pct")
            print(
                f"{_LOG}[backfill_ei] {sym}: OK — "
                f"{fiscal_period} {fiscal_year} "
                f"reaction_1d={r1} post_earnings_1d={price_reaction.get('post_earnings_1d_pct')}"
            )
        else:
            print(f"{_LOG}[backfill_ei] {sym}: UPDATE matched 0 rows")
        return ok
    except Exception as exc:
        print(f"{_LOG}[backfill_ei] {sym}: SQL patch error: {exc}")
        return False


async def bulk_backfill_ei_reactions(lookback_days: int = 30) -> dict:
    """
    Run backfill_ei_price_reaction for every complete live event in the
    last `lookback_days` calendar days that has a finalized reaction_payload.
    """
    import asyncio as _aio

    since = (date.today() - timedelta(days=lookback_days)).isoformat()
    try:
        from data.earnings_monitor_store import get_recent_live_events
        rows = await _aio.to_thread(get_recent_live_events, 200, since, False)
    except Exception as exc:
        return {"error": str(exc)}

    result_states = {"results_available", "results_updated", "complete"}
    candidates = [
        r for r in rows
        if r.get("state") in result_states
           and _coerce_reaction_dict(r.get("reaction_payload")).get("post_1d_pct") is not None
    ]

    seen: set[str] = set()
    ok_count   = 0
    fail_count = 0
    for ev in candidates:
        sym = (ev.get("symbol") or "").upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        try:
            patched = await backfill_ei_price_reaction(sym)
            if patched:
                ok_count += 1
            else:
                fail_count += 1
        except Exception as exc:
            print(f"{_LOG}[bulk_backfill] {sym}: {exc}")
            fail_count += 1

    return {
        "symbols_attempted": len(seen),
        "ok":   ok_count,
        "fail": fail_count,
        "lookback_days": lookback_days,
    }
