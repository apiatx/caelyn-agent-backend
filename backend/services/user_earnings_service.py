"""
Symbol-driven earnings cache for Watchlist and Portfolio universes.

Stores FMP earnings events keyed by universe ('watchlist' / 'portfolio')
in Neon with a 30-day TTL.  Avoids the "filter from All calendar"
anti-pattern by fetching a 120-day window and filtering to user symbols.

Public API
──────────
get_or_sync_user_earnings(universe, symbols, fmp_key, from_date, to_date)
    → (events, meta)  — checks Neon cache, syncs from FMP if stale/missing.

sync_universe_background(universe, symbols, fmp_key)
    → None  — fire-and-forget; safe to wrap in asyncio.create_task().

invalidate_user_earnings(universe)
    → None  — delete Neon cache row so next request triggers a fresh sync.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Optional

CACHE_TTL_DAYS  = 30   # days before cache is considered stale
SYNC_WINDOW_DAYS = 120  # FMP fetch window (today → +120d)


# ── Date helpers ──────────────────────────────────────────────────────────────

def _today_str() -> str:
    return date.today().isoformat()


def _window_end_str() -> str:
    return (date.today() + timedelta(days=SYNC_WINDOW_DAYS)).isoformat()


def _is_fresh(fetched_at_iso: Optional[str]) -> bool:
    if not fetched_at_iso:
        return False
    try:
        raw = fetched_at_iso
        if isinstance(raw, str):
            raw = raw.replace("Z", "+00:00")
            fetched = datetime.fromisoformat(raw)
        elif hasattr(raw, "isoformat"):
            fetched = raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
        else:
            return False
        if fetched.tzinfo is None:
            fetched = fetched.replace(tzinfo=timezone.utc)
        age_days = (datetime.now(timezone.utc) - fetched).days
        return age_days < CACHE_TTL_DAYS
    except Exception:
        return False


def _filter_by_date(
    events: list[dict],
    from_date: Optional[str],
    to_date: Optional[str],
) -> list[dict]:
    if not from_date and not to_date:
        return list(events)
    out = []
    for ev in events:
        d = ev.get("date", "") or ""
        if from_date and d < from_date:
            continue
        if to_date and d > to_date:
            continue
        out.append(ev)
    return out


# ── Neon persistence ──────────────────────────────────────────────────────────

def _pg_read(universe: str) -> Optional[dict]:
    """Read cached earnings record for the given universe from Neon."""
    try:
        from data.pg_storage import _get_conn, _put_conn  # type: ignore
        conn = _get_conn()
        if conn is None:
            return None
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT events, symbols, fetched_at, fmp_window_from, fmp_window_to
                    FROM public.user_earnings_cache
                    WHERE universe = %s
                    """,
                    (universe,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                events_raw, symbols_raw, fetched_at, win_from, win_to = row
                events  = events_raw  if isinstance(events_raw,  list) else json.loads(events_raw  or "[]")
                symbols = symbols_raw if isinstance(symbols_raw, list) else json.loads(symbols_raw or "[]")
                fetched_str = (
                    fetched_at.isoformat()
                    if hasattr(fetched_at, "isoformat")
                    else str(fetched_at)
                )
                return {
                    "events":          events,
                    "symbols":         symbols,
                    "fetched_at":      fetched_str,
                    "fmp_window_from": win_from,
                    "fmp_window_to":   win_to,
                }
        finally:
            _put_conn(conn)
    except Exception as e:
        print(f"[user_earnings] Neon read error (universe={universe}): {e}")
        return None


def _pg_write(
    universe: str,
    events:   list[dict],
    symbols:  list[str],
    win_from: str,
    win_to:   str,
) -> bool:
    """Upsert the earnings cache row for the given universe."""
    try:
        from data.pg_storage import _get_conn, _put_conn  # type: ignore
        conn = _get_conn()
        if conn is None:
            return False
        try:
            events_json  = json.dumps(events,  default=str)
            symbols_json = json.dumps(symbols, default=str)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.user_earnings_cache
                        (universe, events, symbols, fetched_at, fmp_window_from, fmp_window_to)
                    VALUES (%s, %s::jsonb, %s::jsonb, NOW(), %s, %s)
                    ON CONFLICT (universe) DO UPDATE SET
                        events          = EXCLUDED.events,
                        symbols         = EXCLUDED.symbols,
                        fetched_at      = NOW(),
                        fmp_window_from = EXCLUDED.fmp_window_from,
                        fmp_window_to   = EXCLUDED.fmp_window_to
                    """,
                    (universe, events_json, symbols_json, win_from, win_to),
                )
                conn.commit()
            print(
                f"[user_earnings] Neon write OK (universe={universe}): "
                f"{len(events)} events, {len(symbols)} symbols, window={win_from}→{win_to}"
            )
            return True
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[user_earnings] Neon write error (universe={universe}): {e}")
            return False
        finally:
            _put_conn(conn)
    except Exception as e:
        print(f"[user_earnings] Neon write outer error (universe={universe}): {e}")
        return False


def invalidate_user_earnings(universe: str) -> None:
    """Delete the Neon cache row so the next request triggers a fresh sync."""
    try:
        from data.pg_storage import _get_conn, _put_conn  # type: ignore
        conn = _get_conn()
        if conn is None:
            return
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM public.user_earnings_cache WHERE universe = %s",
                    (universe,),
                )
                conn.commit()
            print(f"[user_earnings] Cache invalidated for universe={universe}")
        finally:
            _put_conn(conn)
    except Exception as e:
        print(f"[user_earnings] invalidate error (universe={universe}): {e}")


# ── FMP sync ──────────────────────────────────────────────────────────────────

async def _sync_from_fmp(
    universe: str,
    symbols:  set[str],
    fmp_key:  str,
) -> list[dict]:
    """
    Fetch a 120-day FMP earnings window, filter to user symbols, store in Neon.

    Uses lazy imports to avoid a circular dependency with
    catalyst_calendar_service (which imports this module).
    """
    win_from = _today_str()
    win_to   = _window_end_str()

    print(
        f"[user_earnings] Syncing universe={universe}: "
        f"{len(symbols)} symbols, window={win_from}→{win_to}"
    )
    print(f"[user_earnings] First 10 symbols: {sorted(symbols)[:10]}")

    try:
        from services.catalyst_calendar_service import (  # type: ignore
            CatalystFMP,
            _fetch_earnings_dates,
            _load_watchlist_symbols,
            _load_portfolio_symbols,
        )

        fmp       = CatalystFMP(fmp_key)
        watchlist = _load_watchlist_symbols()
        portfolio = _load_portfolio_symbols()

        all_events = await _fetch_earnings_dates(fmp, win_from, win_to, watchlist, portfolio)
        print(
            f"[user_earnings] FMP returned {len(all_events)} total events "
            f"for window={win_from}→{win_to}"
        )

        # If FMP returned zero total events for a 120-day window, this is a
        # transient API failure (rate-limit, empty response, etc.). Do NOT write
        # a zero-event result to cache — it would poison every future request
        # for the 30-day TTL. Return [] so the next request will retry.
        if not all_events:
            print(
                f"[user_earnings] FMP returned 0 events (transient failure?) "
                f"— skipping cache write for universe={universe}"
            )
            return []

        # Filter to exactly the user's symbol set
        filtered = [
            ev for ev in all_events
            if (ev.get("symbol") or "").upper() in symbols
        ]
        print(
            f"[user_earnings] After symbol filter: {len(filtered)} events "
            f"for universe={universe}"
        )

        _pg_write(universe, filtered, sorted(symbols), win_from, win_to)
        return filtered

    except Exception as e:
        print(f"[user_earnings] Sync error (universe={universe}): {e}")
        return []


# ── Public API ────────────────────────────────────────────────────────────────

async def get_or_sync_user_earnings(
    universe:  str,
    symbols:   set[str],
    fmp_key:   str,
    from_date: Optional[str] = None,
    to_date:   Optional[str] = None,
) -> tuple[list[dict], dict]:
    """
    Return (events, meta) for the given universe.

    1. Checks Neon cache (30-day TTL).
    2. Re-syncs if the symbol set has changed since last cache write.
    3. Falls back to FMP fetch on cache miss/stale.
    4. Always filters returned events to [from_date, to_date].
    """
    prefix = f"[user_earnings] universe={universe}"
    print(
        f"{prefix} requested: {len(symbols)} symbols, "
        f"from={from_date} to={to_date}"
    )
    print(f"{prefix} first 10 symbols: {sorted(symbols)[:10]}")

    # ── Empty symbol set ──────────────────────────────────────────────────────
    if not symbols:
        print(f"{prefix} no symbols — returning empty result")
        return [], {
            "universe":      universe,
            "symbols_count": 0,
            "events_count":  0,
            "cache_status":  "empty",
            "source":        "fmp",
        }

    # ── Try Neon cache ────────────────────────────────────────────────────────
    cached = _pg_read(universe)

    if cached and _is_fresh(cached.get("fetched_at")):
        cached_syms = set(cached.get("symbols", []))

        # If symbol set has changed since last sync, refresh
        if symbols != cached_syms:
            added   = symbols - cached_syms
            removed = cached_syms - symbols
            print(
                f"{prefix} symbol set changed "
                f"(+{len(added)} -{len(removed)}) — re-syncing"
            )
            events       = await _sync_from_fmp(universe, symbols, fmp_key)
            cache_status = "refreshed"
        else:
            events = cached["events"]
            print(
                f"{prefix} cache HIT: {len(events)} events, "
                f"fetched_at={cached.get('fetched_at')}"
            )
            cache_status = "hit"

        in_range = _filter_by_date(events, from_date, to_date)
        # Re-apply symbol guard (defensive: cache row could contain stale symbols)
        in_range = [
            ev for ev in in_range
            if (ev.get("symbol") or "").upper() in symbols
        ]
        in_range = await _overlay_timing_async(in_range, symbols)
        print(f"{prefix} returning {len(in_range)} events in range (cache_status={cache_status})")
        return in_range, {
            "universe":      universe,
            "symbols_count": len(symbols),
            "events_count":  len(in_range),
            "cache_status":  cache_status,
            "source":        "fmp",
        }

    # ── Cache miss / stale — sync from FMP ───────────────────────────────────
    print(f"{prefix} cache MISS — syncing from FMP")
    events   = await _sync_from_fmp(universe, symbols, fmp_key)
    in_range = _filter_by_date(events, from_date, to_date)
    in_range = await _overlay_timing_async(in_range, symbols)
    print(f"{prefix} returning {len(in_range)} events after sync")
    return in_range, {
        "universe":      universe,
        "symbols_count": len(symbols),
        "events_count":  len(in_range),
        "cache_status":  "miss",
        "source":        "fmp",
    }


async def sync_universe_background(
    universe: str,
    symbols:  set[str],
    fmp_key:  str,
) -> None:
    """
    Fire-and-forget sync.  Always invalidates the Neon cache first so the next
    calendar request will see fresh data even if this task is still running.

    Safe to wrap in asyncio.create_task().
    """
    try:
        invalidate_user_earnings(universe)
        await _sync_from_fmp(universe, symbols, fmp_key)
        print(f"[user_earnings] Background sync complete for universe={universe}")
    except Exception as e:
        print(f"[user_earnings] Background sync error (universe={universe}): {e}")


# ── Explicit-symbol API (by-symbols endpoint + canonical watchlist attachment) ──

# Neon universe key used by the by-symbols cache.  Separate from "watchlist"
# (which is always the default/first watchlist) so there is no cross-contamination.
_BY_SYMS_UNIVERSE = "watchlist_by_syms"


def _apply_monitor_timing(events: list[dict], timing_map: dict) -> list[dict]:
    """
    Overlay timing from earnings_monitor_targets onto a list of upcoming events.

    Timing precedence per event:
      1. Preserve the event's existing non-null ``time`` value.
      2. Use ``expected_timing`` from the matched target when non-null (e.g. "bmo", "amc").
      3. Use ``expected_time_local`` from the matched target when non-null
         (e.g. "After Market Close (AMC)" — preserves human-readable fallbacks).
      4. Leave ``time`` null when no matching target exists.

    Matching key: exact (symbol_upper, earnings_date_str) — never crosses event
    dates so a symbol with two upcoming events is never assigned the wrong timing.

    Supports both raw-FMP event shape (symbol / date) and normalised shape
    (ticker / earnings_date) so the helper works at any point in the pipeline.

    No DB calls are made here; ``timing_map`` must be pre-fetched by the caller
    via ``get_timing_for_symbol_dates``.  Returns ``events`` unchanged when
    ``timing_map`` is empty.
    """
    if not timing_map:
        return events
    result: list[dict] = []
    for ev in events:
        if ev.get("time") is not None:
            result.append(ev)
            continue
        sym  = (ev.get("ticker") or ev.get("symbol") or "").upper()
        date = ev.get("earnings_date") or ev.get("date") or ""
        target = timing_map.get((sym, date))
        if target:
            t = target.get("expected_timing") or target.get("expected_time_local")
            if t is not None:
                ev = {**ev, "time": t}
        result.append(ev)
    return result


async def _overlay_timing_async(events: list[dict], symbols: set | list) -> list[dict]:
    """
    Async wrapper: fetch timing map for *symbols* via run_in_executor then
    apply _apply_monitor_timing.  Used by get_or_sync_user_earnings which
    operates on raw FMP events (symbol / date fields).

    Zero provider calls, one bulk Neon read.  Returns events unchanged on any
    error so the caller is never worse off.
    """
    import asyncio as _aio_ot
    if not events:
        return events
    try:
        from data.earnings_monitor_store import (  # type: ignore
            get_timing_for_symbol_dates as _gtfsd_ot,
        )
        sym_list = list(symbols) if not isinstance(symbols, list) else symbols
        timing_map = await _aio_ot.get_event_loop().run_in_executor(
            None, _gtfsd_ot, sym_list
        )
        return _apply_monitor_timing(events, timing_map)
    except Exception as _ot_err:
        print(f"[user_earnings] _overlay_timing_async error: {_ot_err}")
        return events


def _normalize_event_canonical(ev: dict, last_updated: Optional[str]) -> dict:
    """
    Normalize a raw earnings event dict to the canonical watchlist event shape.

    Canonical fields (new names, spec-defined):
        ticker, company, earnings_date, earnings_date_fmt, time,
        eps_estimate, previous_eps, revenue_estimate, source, last_updated

    Legacy aliases (backward compat for existing callers):
        next_date, date_raw, est_eps, revenue_estimated, importance, market_cap
    """
    sym      = (ev.get("symbol") or "").upper()
    raw_date = ev.get("date")
    try:
        from datetime import datetime as _dtt
        fmt_date = _dtt.strptime(raw_date, "%Y-%m-%d").strftime("%b %-d") if raw_date else None
    except Exception:
        fmt_date = raw_date
    return {
        # ── Canonical (spec) ──────────────────────────────────────────────────
        "ticker":            sym,
        "company":           ev.get("companyName") or ev.get("name") or sym,
        "earnings_date":     raw_date,
        "earnings_date_fmt": fmt_date,
        "time":              ev.get("time"),
        "eps_estimate":      ev.get("epsEstimated"),
        "previous_eps":      ev.get("epsActual"),
        "revenue_estimate":  ev.get("revenueEstimated"),
        "source":            "cached_earnings",
        "last_updated":      last_updated,
        # ── Legacy aliases ────────────────────────────────────────────────────
        "next_date":         fmt_date,
        "date_raw":          raw_date,
        "est_eps":           ev.get("epsEstimated"),
        "revenue_estimated": ev.get("revenueEstimated"),
        "importance":        ev.get("importance"),
        "market_cap":        ev.get("marketCap"),
    }


async def _sync_for_explicit_symbols(
    universe: str,
    symbols:  set[str],
    fmp_key:  str,
) -> list[dict]:
    """
    Fetch FMP earnings for an explicit symbol set and cache under *universe*.

    FMP's earnings-calendar endpoint returns ALL companies for the date window
    regardless of which symbols are passed.  The *symbols* param is forwarded
    to _fetch_earnings_dates only for importance scoring; the actual filtering
    to the requested symbol set is done here in Python.

    Writes the filtered result to Neon under the given universe key.
    Does NOT write if FMP returns zero events (transient API failure guard).
    """
    win_from = _today_str()
    win_to   = _window_end_str()
    print(
        f"[user_earnings] _sync_for_explicit_symbols universe={universe}: "
        f"{len(symbols)} symbols, window={win_from}→{win_to}"
    )
    try:
        from services.catalyst_calendar_service import (  # type: ignore
            CatalystFMP,
            _fetch_earnings_dates,
        )
        fmp = CatalystFMP(fmp_key)
        # Pass requested symbols as *watchlist* — used for importance scoring only.
        all_events = await _fetch_earnings_dates(fmp, win_from, win_to, symbols, set())
        if not all_events:
            print(
                f"[user_earnings] FMP returned 0 events (transient?) "
                f"— skipping cache write for universe={universe}"
            )
            return []
        filtered = [
            ev for ev in all_events
            if (ev.get("symbol") or "").upper() in symbols
        ]
        print(
            f"[user_earnings] _sync_for_explicit_symbols: "
            f"{len(filtered)}/{len(all_events)} events match {len(symbols)} symbols"
        )
        _pg_write(universe, filtered, sorted(symbols), win_from, win_to)
        return filtered
    except Exception as e:
        print(f"[user_earnings] _sync_for_explicit_symbols error (universe={universe}): {e}")
        return []


async def get_earnings_for_symbols(
    symbols:   list[str],
    fmp_key:   str,
    from_date: Optional[str] = None,
    to_date:   Optional[str] = None,
) -> tuple[list[dict], list[str], dict]:
    """
    Return (events, missing_symbols, meta) for an explicit symbol list.

    Cache strategy (universe="watchlist_by_syms"):
      HIT          — all requested symbols are in the cached universe (30-day TTL)
      REFRESHED    — cache exists but new symbols were requested; re-syncs with
                     the UNION of old + new so future requests are still fast
      MISS/STALE   — no cache or TTL expired; full FMP sync for requested symbols

    missing_symbols — requested symbols that were not found in any cached event.
                      These may simply have no upcoming earnings (not an error).

    Events are filtered to [from_date, to_date] before returning.
    Events are always scoped to exactly the requested symbols (defensive re-filter).
    """
    req_syms: set[str] = {s.strip().upper() for s in symbols if s.strip()}
    if not req_syms:
        return [], [], {
            "symbols_requested": [],
            "events_count":      0,
            "missing_symbols_count": 0,
            "cache_status":      "empty",
            "source":            "cached_earnings",
            "last_updated":      None,
            "stale":             False,
        }

    cached       = _pg_read(_BY_SYMS_UNIVERSE)
    cache_status = "miss"
    events: list[dict]  = []
    cached_syms:  set[str] = set()
    last_updated: Optional[str] = None

    if cached and _is_fresh(cached.get("fetched_at")):
        cached_syms  = set(cached.get("symbols", []))
        last_updated = cached.get("fetched_at")

        if req_syms.issubset(cached_syms):
            # Cache covers all requested symbols
            events       = cached["events"]
            cache_status = "hit"
            print(
                f"[user_earnings] get_earnings_for_symbols HIT: "
                f"{len(events)} events, {len(req_syms)} symbols requested"
            )
        else:
            # Expand universe to include new symbols, re-sync
            expanded     = cached_syms | req_syms
            print(
                f"[user_earnings] get_earnings_for_symbols EXPAND: "
                f"{len(req_syms - cached_syms)} new symbols → re-syncing with {len(expanded)} total"
            )
            events       = await _sync_for_explicit_symbols(_BY_SYMS_UNIVERSE, expanded, fmp_key)
            cached_syms  = expanded
            cache_status = "refreshed"
            last_updated = _today_str()
    else:
        # Cache miss or stale
        print(
            f"[user_earnings] get_earnings_for_symbols MISS: "
            f"syncing {len(req_syms)} symbols from FMP"
        )
        events       = await _sync_for_explicit_symbols(_BY_SYMS_UNIVERSE, req_syms, fmp_key)
        cached_syms  = req_syms
        cache_status = "miss"
        last_updated = _today_str()

    # ── Filter to requested symbols + date window ─────────────────────────────
    filtered = [
        ev for ev in events
        if (ev.get("symbol") or "").upper() in req_syms
    ]
    in_range = _filter_by_date(filtered, from_date, to_date)

    # Symbols that appeared in none of the cached events (may have no earnings)
    symbols_with_events = {(ev.get("symbol") or "").upper() for ev in filtered}
    missing = sorted(req_syms - symbols_with_events)

    print(
        f"[user_earnings] get_earnings_for_symbols → {len(in_range)} events, "
        f"{len(missing)} missing, cache_status={cache_status}"
    )
    return in_range, missing, {
        "symbols_requested":     sorted(req_syms),
        "events_count":          len(in_range),
        "missing_symbols_count": len(missing),
        "cache_status":          cache_status,
        "source":                "cached_earnings",
        "last_updated":          last_updated,
        "stale":                 cache_status in ("miss",),
    }


# ── Canonical public API — get_upcoming_earnings_for_symbols ──────────────────
#
# All earnings endpoints (GET /{id}, POST /by-symbols, GET /{id}/earnings)
# delegate here.  This is the single implementation for watchlist-scoped earnings.
#
# Modes controlled by kwargs:
#   sync_on_miss=True  — wait for FMP sync if cache miss (explicit POST/GET calls)
#   sync_on_miss=False + background_sync_on_miss=True — fire background sync,
#                         return empty events immediately (non-blocking GET /{id})
#   sync_on_miss=False + background_sync_on_miss=False — cache-only, never block

async def get_upcoming_earnings_for_symbols(
    symbols:   list[str],
    from_date: Optional[str] = None,
    to_date:   Optional[str] = None,
    *,
    fmp_key:                 str  = "",
    sync_on_miss:            bool = False,
    background_sync_on_miss: bool = True,
) -> dict:
    """
    Canonical function: return watchlist-scoped upcoming earnings for an
    explicit symbol list.

    Response shape (stable):
        {
          "symbols_requested": [...],      // ordered, uppercase
          "events":            [...],      // canonical event dicts
          "missing_symbols":   [...],      // requested but no earnings found
          "source":            "cached_earnings",
          "last_updated":      "...",
          "stale":             false,
          "cache_status":      "hit|miss_syncing|miss|partial_syncing|empty|error"
        }

    Each event uses _normalize_event_canonical — canonical fields + legacy aliases.

    Rules:
      - Never returns events for unrequested symbols.
      - Never makes per-symbol provider calls.
      - sync_on_miss=False  → never blocks for FMP (watchlist GET path).
      - sync_on_miss=True   → awaits FMP sync on cache miss (explicit endpoints).
    """
    from datetime import date as _d, timedelta as _td
    import asyncio as _aio_ue

    ordered_syms: list[str] = list(dict.fromkeys(
        s.strip().upper() for s in symbols if s.strip()
    ))
    req_syms: set[str] = set(ordered_syms)

    _today = _d.today().isoformat()
    from_date = from_date or _today
    to_date   = to_date   or (_d.today() + _td(days=90)).isoformat()

    def _empty_response(cache_status: str, stale: bool = False) -> dict:
        return {
            "symbols_requested": ordered_syms,
            "events":            [],
            "missing_symbols":   ordered_syms,
            "source":            "cached_earnings",
            "last_updated":      None,
            "stale":             stale,
            "cache_status":      cache_status,
        }

    if not req_syms:
        return _empty_response("empty", stale=False)

    # ── Pre-fetch timing overlay map (one bulk read, zero provider calls) ─────
    # Closed over by _build_response so all return paths share the same map.
    _timing_map: dict = {}
    try:
        from data.earnings_monitor_store import (  # type: ignore
            get_timing_for_symbol_dates as _gtfsd,
        )
        _timing_map = await _aio_ue.get_event_loop().run_in_executor(
            None, _gtfsd, list(req_syms)
        )
    except Exception as _tm_err:
        print(f"[user_earnings] timing overlay prefetch error: {_tm_err}")

    def _build_response(
        events_raw: list[dict],
        last_upd: Optional[str],
        status: str,
        stale: bool = False,
    ) -> dict:
        filtered  = [ev for ev in events_raw if (ev.get("symbol") or "").upper() in req_syms]
        in_range  = _filter_by_date(filtered, from_date, to_date)
        normalised = [_normalize_event_canonical(ev, last_upd) for ev in in_range]
        normalised = _apply_monitor_timing(normalised, _timing_map)
        normalised.sort(key=lambda x: x.get("earnings_date") or "")
        syms_with_events = {ev["ticker"] for ev in normalised}
        missing = sorted(req_syms - syms_with_events)
        return {
            "symbols_requested": ordered_syms,
            "events":            normalised,
            "missing_symbols":   missing,
            "source":            "cached_earnings",
            "last_updated":      last_upd,
            "stale":             stale,
            "cache_status":      status,
        }

    # ── Try Neon cache (run in executor so event loop is never blocked) ───────
    try:
        _loop_ue = _aio_ue.get_event_loop()
        cached = await _loop_ue.run_in_executor(None, _pg_read, _BY_SYMS_UNIVERSE)
    except Exception as _cache_err:
        print(f"[user_earnings] get_upcoming_earnings_for_symbols cache read error: {_cache_err}")
        return _empty_response("error", stale=True)

    if cached and _is_fresh(cached.get("fetched_at")):
        cached_syms  = set(cached.get("symbols", []))
        last_updated = cached.get("fetched_at")

        if req_syms.issubset(cached_syms):
            # Full cache HIT — all symbols covered
            return _build_response(cached.get("events", []), last_updated, "hit", stale=False)
        else:
            # Partial miss — some new symbols not in cache
            added_syms = req_syms - cached_syms
            if sync_on_miss and fmp_key:
                # Expand + await sync
                expanded = cached_syms | req_syms
                events = await _sync_for_explicit_symbols(_BY_SYMS_UNIVERSE, expanded, fmp_key)
                return _build_response(events, _today_str(), "refreshed", stale=False)
            elif background_sync_on_miss and fmp_key:
                # Fire background expand, return partial result from existing cache
                expanded = cached_syms | req_syms
                _aio_ue.create_task(
                    _sync_for_explicit_symbols(_BY_SYMS_UNIVERSE, expanded, fmp_key)
                )
                print(
                    f"[user_earnings] partial_syncing: {len(added_syms)} new symbols "
                    f"background-queued for universe expand"
                )
            # Return events we have (filtered to requested symbols)
            return _build_response(cached.get("events", []), last_updated, "partial_syncing", stale=False)
    else:
        # Cache miss or stale
        if sync_on_miss and fmp_key:
            # Await full sync
            events = await _sync_for_explicit_symbols(_BY_SYMS_UNIVERSE, req_syms, fmp_key)
            return _build_response(events, _today_str(), "miss", stale=False)
        elif background_sync_on_miss and fmp_key:
            # Fire background sync, return empty immediately
            _aio_ue.create_task(
                _sync_for_explicit_symbols(_BY_SYMS_UNIVERSE, req_syms, fmp_key)
            )
            print(
                f"[user_earnings] miss_syncing: {len(req_syms)} symbols "
                f"background-queued for initial sync"
            )
            return _empty_response("miss_syncing", stale=True)
        else:
            return _empty_response("miss", stale=True)
