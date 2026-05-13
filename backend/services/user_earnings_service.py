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
