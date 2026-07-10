"""
Watchlist Fundamentals Cache — Neon persistence layer.

Schema: watchlist_fundamentals_cache
  symbol TEXT PRIMARY KEY
  watchlist_id TEXT
  refreshed_at TIMESTAMPTZ
  next_refresh_at TIMESTAMPTZ
  fields JSONB          — normalized field values keyed by CSV column name
  missing_fields JSONB  — list of CSV column names FMP could not provide
  fmp_call_count INT
  created_at TIMESTAMPTZ
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

_TABLE = "public.watchlist_fundamentals_cache"
_WEEK_S = 7 * 24 * 3600

# ── Usable-snapshot evidence rule ────────────────────────────────────────────
# A snapshot is "usable" when it contains at least one real, populated
# fundamentals evidence field (not merely an empty dict). Metadata-only
# payloads (e.g. {} with everything in missing_fields) must never be treated
# as a successful refresh. This is intentionally permissive — a legitimate
# company/provider result may have partial data; we only require ONE piece
# of real evidence, not a complete row.
_EVIDENCE_FIELDS = {
    "Revenue Growth (YoY)",
    "Revenue Growth (Q)",
    "EPS Growth",
    "Gross Margin",
    "FCF Margin",
    "Free Cash Flow",
    "Market Cap",
    "Revenue",
    "Operating Income",
    "EBIT",
    "PE Ratio",
    "PS Ratio",
    "EV/EBITDA",
    "Rev Growth Next Quarter",
    "EPS Growth This Quarter",
}

# Failed/empty refresh attempts retry much sooner than the normal 7-day
# freshness window — they must not be treated as "fresh" for a week.
_EMPTY_PAYLOAD_RETRY_DAYS = 1


def is_usable_fundamentals_snapshot(fields: dict[str, Any] | None) -> bool:
    """
    Canonical usability rule for a normalized fundamentals payload.

    Returns True only when `fields` is a dict containing at least one
    populated (non-None, non-empty-string) canonical evidence field.
    An empty dict, None, or a dict with only unrelated/metadata keys is
    NOT usable.
    """
    if not isinstance(fields, dict) or not fields:
        return False
    for key in _EVIDENCE_FIELDS:
        v = fields.get(key)
        if v is not None and v != "":
            return True
    return False


def ensure_table() -> bool:
    """Create the table if it doesn't exist. Safe to call repeatedly."""
    try:
        from data.pg_storage import _get_conn, _put_conn
        conn = _get_conn()
        if conn is None:
            return False
        try:
            cur = conn.cursor()
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {_TABLE} (
                    symbol           TEXT        PRIMARY KEY,
                    watchlist_id     TEXT,
                    refreshed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    next_refresh_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    fields           JSONB       NOT NULL DEFAULT '{{}}',
                    missing_fields   JSONB       NOT NULL DEFAULT '[]',
                    fmp_call_count   INT         NOT NULL DEFAULT 0,
                    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            conn.commit()
            cur.close()
            return True
        finally:
            _put_conn(conn)
    except Exception as exc:
        log.warning("[FUND_STORE] ensure_table error: %s", exc)
        return False


def upsert_snapshot(
    symbol: str,
    watchlist_id: str,
    fields: dict[str, Any],
    missing_fields: list[str],
    fmp_call_count: int,
    next_refresh_days: int = 7,
) -> str:
    """
    Insert or replace a fundamentals snapshot for one symbol.

    Result contract (string outcome, used by refresh_symbols diagnostics):
      "success"                — usable payload persisted; refreshed_at bumped,
                                  next_refresh_at = now + next_refresh_days (7d).
      "empty_payload_preserved_lkg" — provider returned an unusable/empty
                                  payload AND a usable prior snapshot exists.
                                  Existing fields/missing_fields/refreshed_at
                                  are left untouched (LKG protection); only
                                  next_refresh_at is pulled forward to a short
                                  retry window so the symbol is retried soon,
                                  never treated as fresh for 7 days.
      "empty_payload_no_prior"  — provider returned an unusable/empty payload
                                  and there is no usable prior snapshot. No
                                  row is written/kept as "fresh"; any existing
                                  unusable row is removed so the symbol
                                  reverts to no-snapshot due-immediately
                                  status (list_due_symbols already surfaces
                                  no-row symbols without needing a placeholder).
      "error"                   — DB error; caller should treat as failed.
    """
    usable = is_usable_fundamentals_snapshot(fields)
    try:
        from data.pg_storage import _get_conn, _put_conn
        from psycopg2.extras import Json
        from datetime import timedelta
        conn = _get_conn()
        if conn is None:
            return "error"
        try:
            now = datetime.now(timezone.utc)
            cur = conn.cursor()

            if usable:
                nxt = now + timedelta(days=next_refresh_days)
                cur.execute(
                    f"""
                    INSERT INTO {_TABLE}
                        (symbol, watchlist_id, refreshed_at, next_refresh_at, fields, missing_fields, fmp_call_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol) DO UPDATE SET
                        watchlist_id    = EXCLUDED.watchlist_id,
                        refreshed_at    = EXCLUDED.refreshed_at,
                        next_refresh_at = EXCLUDED.next_refresh_at,
                        fields          = EXCLUDED.fields,
                        missing_fields  = EXCLUDED.missing_fields,
                        fmp_call_count  = EXCLUDED.fmp_call_count
                    """,
                    (
                        symbol.upper(),
                        watchlist_id,
                        now,
                        nxt,
                        Json(fields),
                        Json(missing_fields),
                        fmp_call_count,
                    ),
                )
                conn.commit()
                cur.close()
                return "success"

            # ── Unusable/empty payload path ─────────────────────────────────
            cur.execute(
                f"SELECT fields FROM {_TABLE} WHERE symbol = %s",
                (symbol.upper(),),
            )
            row = cur.fetchone()
            existing_fields = row[0] if row else None
            existing_usable = is_usable_fundamentals_snapshot(existing_fields)

            if existing_usable:
                # LKG protection: never overwrite good data with an empty
                # payload. Only pull the retry window forward.
                retry_at = now + timedelta(days=_EMPTY_PAYLOAD_RETRY_DAYS)
                cur.execute(
                    f"""
                    UPDATE {_TABLE}
                    SET next_refresh_at = LEAST(next_refresh_at, %s)
                    WHERE symbol = %s
                    """,
                    (retry_at, symbol.upper()),
                )
                conn.commit()
                cur.close()
                return "empty_payload_preserved_lkg"

            if row is not None:
                # Existing row is itself unusable (e.g. a previously
                # poisoned empty-fields row) — remove it rather than
                # refreshing its timestamp, so it reverts to genuine
                # no-snapshot/due-immediately status.
                cur.execute(
                    f"DELETE FROM {_TABLE} WHERE symbol = %s",
                    (symbol.upper(),),
                )
                conn.commit()
            cur.close()
            return "empty_payload_no_prior"
        finally:
            _put_conn(conn)
    except Exception as exc:
        log.warning("[FUND_STORE] upsert_snapshot(%s) error: %s", symbol, exc)
        return "error"


def get_snapshot(symbol: str) -> dict | None:
    """Return cached snapshot dict or None if not found."""
    try:
        from data.pg_storage import _get_conn, _put_conn
        conn = _get_conn()
        if conn is None:
            return None
        try:
            cur = conn.cursor()
            cur.execute(
                f"""
                SELECT symbol, watchlist_id, refreshed_at, next_refresh_at,
                       fields, missing_fields, fmp_call_count
                FROM {_TABLE}
                WHERE symbol = %s
                """,
                (symbol.upper(),),
            )
            row = cur.fetchone()
            cur.close()
            if row is None:
                return None
            return {
                "symbol": row[0],
                "watchlist_id": row[1],
                "refreshed_at": row[2].isoformat() if hasattr(row[2], "isoformat") else str(row[2]),
                "next_refresh_at": row[3].isoformat() if hasattr(row[3], "isoformat") else str(row[3]),
                "fields": row[4] or {},
                "missing_fields": row[5] or [],
                "fmp_call_count": row[6] or 0,
            }
        finally:
            _put_conn(conn)
    except Exception as exc:
        log.warning("[FUND_STORE] get_snapshot(%s) error: %s", symbol, exc)
        return None


def get_snapshots_bulk(symbols: list[str]) -> dict[str, dict]:
    """Return {SYMBOL: snapshot_dict} for all symbols found in cache."""
    if not symbols:
        return {}
    try:
        from data.pg_storage import _get_conn, _put_conn
        conn = _get_conn()
        if conn is None:
            return {}
        try:
            cur = conn.cursor()
            upper = [s.upper() for s in symbols]
            cur.execute(
                f"""
                SELECT symbol, refreshed_at, next_refresh_at, fields, missing_fields
                FROM {_TABLE}
                WHERE symbol = ANY(%s)
                """,
                (upper,),
            )
            rows = cur.fetchall()
            cur.close()
            out: dict[str, dict] = {}
            for r in rows:
                out[r[0]] = {
                    "refreshed_at": r[1].isoformat() if hasattr(r[1], "isoformat") else str(r[1]),
                    "next_refresh_at": r[2].isoformat() if hasattr(r[2], "isoformat") else str(r[2]),
                    "fields": r[3] or {},
                    "missing_fields": r[4] or [],
                }
            return out
        finally:
            _put_conn(conn)
    except Exception as exc:
        log.warning("[FUND_STORE] get_snapshots_bulk error: %s", exc)
        return {}


def list_due_symbols(symbols: list[str]) -> list[str]:
    """
    Return which of the given CURRENT eligible symbols are due for refresh.

    A symbol is due when:
      A. it has no snapshot row at all (never refreshed), or
      B. its existing row's next_refresh_at <= NOW()

    This is intentionally NOT scoped by any stored `watchlist_id` — the
    caller passes the current live Watchlist membership (already filtered
    for FMP eligibility), and due-ness is computed purely from that set
    against existing rows. This avoids the historical bug where rows were
    keyed to a stale/rotated watchlist_id and became permanently invisible
    to refresh even though they were already past their TTL.

    Order: no-snapshot symbols first (never-refreshed backlog), then
    existing-but-stale rows ordered oldest-due-first.
    """
    if not symbols:
        return []
    try:
        from data.pg_storage import _get_conn, _put_conn
        conn = _get_conn()
        if conn is None:
            return []
        try:
            cur = conn.cursor()
            upper = sorted({s.upper() for s in symbols if s})
            if not upper:
                return []
            cur.execute(
                f"""
                SELECT symbol, next_refresh_at FROM {_TABLE}
                WHERE symbol = ANY(%s)
                """,
                (upper,),
            )
            rows = cur.fetchall()
            cur.close()

            existing = {r[0]: r[1] for r in rows}
            now = datetime.now(timezone.utc)

            no_snapshot = [s for s in upper if s not in existing]

            stale_existing = [
                (s, nxt) for s, nxt in existing.items()
                if nxt is not None and nxt <= now
            ]
            stale_existing.sort(key=lambda x: x[1])
            stale_symbols = [s for s, _ in stale_existing]

            return no_snapshot + stale_symbols
        finally:
            _put_conn(conn)
    except Exception as exc:
        log.warning("[FUND_STORE] list_due_symbols error: %s", exc)
        return []


def schedule_refresh(symbol: str, watchlist_id: str, days: int = 7) -> bool:
    """
    Ensure a symbol row exists with next_refresh_at = now + days.
    Used after CSV upload to register the 7-day TTL.
    Does NOT create the row if it already exists (preserves existing data).
    """
    try:
        from data.pg_storage import _get_conn, _put_conn
        conn = _get_conn()
        if conn is None:
            return False
        try:
            cur = conn.cursor()
            cur.execute(
                f"""
                INSERT INTO {_TABLE}
                    (symbol, watchlist_id, next_refresh_at, fields, missing_fields)
                VALUES (%s, %s, NOW() + INTERVAL '1 day' * %s, '{{}}', '[]')
                ON CONFLICT (symbol) DO UPDATE SET
                    watchlist_id    = EXCLUDED.watchlist_id,
                    next_refresh_at = GREATEST({_TABLE}.next_refresh_at, EXCLUDED.next_refresh_at)
                """,
                (symbol.upper(), watchlist_id, days),
            )
            conn.commit()
            cur.close()
            return True
        finally:
            _put_conn(conn)
    except Exception as exc:
        log.warning("[FUND_STORE] schedule_refresh(%s) error: %s", symbol, exc)
        return False


def get_diagnostics(watchlist_id: str) -> dict:
    """Return summary stats for the fundamentals cache for a given watchlist."""
    try:
        from data.pg_storage import _get_conn, _put_conn
        conn = _get_conn()
        if conn is None:
            return {"error": "db_unavailable"}
        try:
            cur = conn.cursor()
            cur.execute(
                f"""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE next_refresh_at <= NOW()) AS due,
                    COUNT(*) FILTER (WHERE refreshed_at IS NOT NULL AND fields != '{{}}') AS refreshed,
                    MAX(refreshed_at) AS last_refreshed_at,
                    SUM(fmp_call_count) AS total_fmp_calls
                FROM {_TABLE}
                WHERE watchlist_id = %s
                """,
                (watchlist_id,),
            )
            row = cur.fetchone()
            cur.close()
            if not row:
                return {}
            return {
                "total_symbols": row[0],
                "due_symbols": row[1],
                "refreshed_symbols": row[2],
                "last_refreshed_at": row[3].isoformat() if row[3] and hasattr(row[3], "isoformat") else None,
                "total_fmp_calls": row[4] or 0,
            }
        finally:
            _put_conn(conn)
    except Exception as exc:
        log.warning("[FUND_STORE] get_diagnostics error: %s", exc)
        return {"error": str(exc)}
