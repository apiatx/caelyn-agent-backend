"""
Neon storage layer for the Live Earnings Monitor.

Tables (created via init_earnings_monitor_tables()):
  earnings_monitor_targets  — one row per (symbol, expected_date)
  earnings_live_events      — durable event state, keyed by event_id
  earnings_event_reads      — per-user read/ack tracking
"""
from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

from data.pg_storage import _get_conn, _put_conn


# ── helpers ──────────────────────────────────────────────────────────────────

def _pg():
    conn = _get_conn()
    return conn, _put_conn


def _jdump(obj) -> str:
    return json.dumps(obj, default=str) if obj is not None else "null"


# ── schema ────────────────────────────────────────────────────────────────────

def init_earnings_monitor_tables() -> bool:
    """Idempotent schema creation for earnings monitor tables."""
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.earnings_monitor_targets (
                id                      BIGSERIAL PRIMARY KEY,
                symbol                  TEXT NOT NULL,
                expected_date           DATE,
                expected_timing         TEXT,
                fiscal_period           TEXT,
                fiscal_year             INTEGER,
                monitoring_start_at     TIMESTAMPTZ,
                monitoring_end_at       TIMESTAMPTZ,
                next_sec_check_at       TIMESTAMPTZ,
                next_fmp_check_at       TIMESTAMPTZ,
                status                  TEXT NOT NULL DEFAULT 'scheduled',
                worker_lease_owner      TEXT,
                worker_lease_expires_at TIMESTAMPTZ,
                created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (symbol, expected_date)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_emt_status_date
            ON public.earnings_monitor_targets (status, expected_date)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_emt_symbol
            ON public.earnings_monitor_targets (symbol)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_emt_next_checks
            ON public.earnings_monitor_targets (next_fmp_check_at, next_sec_check_at)
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.earnings_live_events (
                id               BIGSERIAL PRIMARY KEY,
                event_id         TEXT UNIQUE NOT NULL,
                event_key        TEXT NOT NULL,
                symbol           TEXT NOT NULL,
                expected_date    DATE,
                fiscal_period    TEXT,
                fiscal_year      INTEGER,
                state            TEXT NOT NULL DEFAULT 'scheduled',
                detected_at      TIMESTAMPTZ,
                updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                revision         INTEGER NOT NULL DEFAULT 1,
                is_dry_run       BOOLEAN NOT NULL DEFAULT FALSE,
                filing_payload   JSONB,
                results_payload  JSONB,
                reaction_payload JSONB,
                source_status    JSONB,
                classification   TEXT,
                checksum         TEXT,
                last_error       TEXT,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_ele_symbol_state
            ON public.earnings_live_events (symbol, state)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_ele_event_key
            ON public.earnings_live_events (event_key)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_ele_updated
            ON public.earnings_live_events (updated_at DESC)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_ele_expected_date
            ON public.earnings_live_events (expected_date)
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.earnings_event_reads (
                id        BIGSERIAL PRIMARY KEY,
                event_id  TEXT NOT NULL,
                user_id   TEXT NOT NULL,
                read_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (event_id, user_id)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_eer_user_event
            ON public.earnings_event_reads (user_id, event_id)
        """)

        # Additive migrations — no-op if column already exists
        _new_cols = [
            "ALTER TABLE public.earnings_monitor_targets ADD COLUMN IF NOT EXISTS expected_time_local TEXT",
            "ALTER TABLE public.earnings_monitor_targets ADD COLUMN IF NOT EXISTS expected_at TIMESTAMPTZ",
            "ALTER TABLE public.earnings_monitor_targets ADD COLUMN IF NOT EXISTS expected_timezone TEXT DEFAULT 'America/New_York'",
            "ALTER TABLE public.earnings_monitor_targets ADD COLUMN IF NOT EXISTS report_time_status TEXT DEFAULT 'unknown'",
            "ALTER TABLE public.earnings_monitor_targets ADD COLUMN IF NOT EXISTS report_period TEXT",
            "ALTER TABLE public.earnings_monitor_targets ADD COLUMN IF NOT EXISTS schedule_source TEXT",
            "ALTER TABLE public.earnings_monitor_targets ADD COLUMN IF NOT EXISTS schedule_updated_at TIMESTAMPTZ",
            "ALTER TABLE public.earnings_monitor_targets ADD COLUMN IF NOT EXISTS fmp_check_stage TEXT DEFAULT 'pre_release'",
            "ALTER TABLE public.earnings_monitor_targets ADD COLUMN IF NOT EXISTS results_first_detected_at TIMESTAMPTZ",
            "ALTER TABLE public.earnings_monitor_targets ADD COLUMN IF NOT EXISTS monitoring_expires_at TIMESTAMPTZ",
        ]
        for _ddl in _new_cols:
            cur.execute(_ddl)

        conn.commit()
        cur.close()
        print("[EarnMonStore] init_earnings_monitor_tables OK")
        return True
    except Exception as exc:
        print(f"[EarnMonStore] schema error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


# ── universe ──────────────────────────────────────────────────────────────────

def get_universe_symbols() -> list[str]:
    """
    Return distinct symbol strings from:
      - public.watchlist (tickers JSONB array)
      - public.watchlist_favorites
    Portfolio holdings (JSON file) are merged by the caller via load_active_holdings().
    """
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT sym FROM (
                SELECT jsonb_array_elements_text(tickers) AS sym
                FROM   public.watchlist
                WHERE  tickers IS NOT NULL
                  AND  jsonb_array_length(tickers) > 0
                UNION
                SELECT ticker AS sym FROM public.watchlist_favorites
            ) t
            WHERE sym IS NOT NULL AND sym != ''
        """)
        rows = cur.fetchall()
        cur.close()
        return [r[0] for r in rows]
    except Exception as exc:
        print(f"[EarnMonStore] get_universe_symbols error: {exc}")
        return []
    finally:
        _put_conn(conn)


# ── targets ───────────────────────────────────────────────────────────────────

def upsert_target(
    symbol: str,
    expected_date,            # str YYYY-MM-DD or None
    expected_timing: str | None      = None,
    fiscal_period: str | None        = None,
    fiscal_year: int | None          = None,
    status: str                      = "scheduled",
    monitoring_start_at: str | None  = None,
    monitoring_end_at: str | None    = None,
    next_sec_check_at: str | None    = None,
    next_fmp_check_at: str | None    = None,
    # ── timing/schedule fields (Part 2) ──
    expected_at: str | None          = None,
    expected_time_local: str | None  = None,
    report_time_status: str | None   = None,
    report_period: str | None        = None,
    schedule_source: str | None      = None,
    schedule_updated_at: str | None  = None,
    fmp_check_stage: str | None      = None,
) -> dict | None:
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO public.earnings_monitor_targets
                (symbol, expected_date, expected_timing, fiscal_period, fiscal_year,
                 status, monitoring_start_at, monitoring_end_at,
                 next_sec_check_at, next_fmp_check_at,
                 expected_at, expected_time_local, report_time_status,
                 report_period, schedule_source, schedule_updated_at, fmp_check_stage,
                 updated_at)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (symbol, expected_date) DO UPDATE SET
                expected_timing      = EXCLUDED.expected_timing,
                fiscal_period        = COALESCE(EXCLUDED.fiscal_period, earnings_monitor_targets.fiscal_period),
                fiscal_year          = COALESCE(EXCLUDED.fiscal_year,   earnings_monitor_targets.fiscal_year),
                status               = CASE
                    WHEN earnings_monitor_targets.status IN ('complete','results_available') THEN earnings_monitor_targets.status
                    ELSE EXCLUDED.status
                END,
                monitoring_start_at  = COALESCE(EXCLUDED.monitoring_start_at, earnings_monitor_targets.monitoring_start_at),
                monitoring_end_at    = COALESCE(EXCLUDED.monitoring_end_at,   earnings_monitor_targets.monitoring_end_at),
                next_sec_check_at    = COALESCE(EXCLUDED.next_sec_check_at,   earnings_monitor_targets.next_sec_check_at),
                next_fmp_check_at    = COALESCE(EXCLUDED.next_fmp_check_at,   earnings_monitor_targets.next_fmp_check_at),
                expected_at          = COALESCE(EXCLUDED.expected_at,          earnings_monitor_targets.expected_at),
                expected_time_local  = COALESCE(EXCLUDED.expected_time_local,  earnings_monitor_targets.expected_time_local),
                report_time_status   = CASE
                    WHEN EXCLUDED.report_time_status = 'confirmed' THEN 'confirmed'
                    WHEN earnings_monitor_targets.report_time_status = 'confirmed' THEN 'confirmed'
                    ELSE COALESCE(EXCLUDED.report_time_status, earnings_monitor_targets.report_time_status)
                END,
                report_period        = COALESCE(EXCLUDED.report_period,        earnings_monitor_targets.report_period),
                schedule_source      = COALESCE(EXCLUDED.schedule_source,      earnings_monitor_targets.schedule_source),
                schedule_updated_at  = COALESCE(EXCLUDED.schedule_updated_at,  earnings_monitor_targets.schedule_updated_at),
                fmp_check_stage      = COALESCE(EXCLUDED.fmp_check_stage,      earnings_monitor_targets.fmp_check_stage),
                updated_at           = NOW()
            RETURNING id, symbol, expected_date, expected_timing, fiscal_period, fiscal_year,
                      status, next_sec_check_at, next_fmp_check_at,
                      worker_lease_owner, worker_lease_expires_at,
                      expected_at, expected_time_local, report_time_status,
                      report_period, schedule_source, fmp_check_stage
        """, (
            symbol, expected_date, expected_timing, fiscal_period, fiscal_year,
            status, monitoring_start_at, monitoring_end_at,
            next_sec_check_at, next_fmp_check_at,
            expected_at, expected_time_local, report_time_status,
            report_period, schedule_source, schedule_updated_at, fmp_check_stage,
        ))
        row = cur.fetchone()
        conn.commit()
        cur.close()
        if not row:
            return None
        cols = ["id","symbol","expected_date","expected_timing","fiscal_period","fiscal_year",
                "status","next_sec_check_at","next_fmp_check_at","worker_lease_owner","worker_lease_expires_at",
                "expected_at","expected_time_local","report_time_status","report_period","schedule_source","fmp_check_stage"]
        return dict(zip(cols, row))
    except Exception as exc:
        print(f"[EarnMonStore] upsert_target error {symbol}: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    finally:
        _put_conn(conn)


_TARGET_COLS = [
    "id","symbol","expected_date","expected_timing","fiscal_period","fiscal_year",
    "status","next_sec_check_at","next_fmp_check_at","worker_lease_owner",
    "worker_lease_expires_at","updated_at",
    "expected_at","expected_time_local","report_time_status",
    "report_period","schedule_source","fmp_check_stage","results_first_detected_at",
]
_TARGET_SELECT = ", ".join(_TARGET_COLS)


def get_targets_for_symbols(symbols: list[str]) -> list[dict]:
    """
    Return the most-recent target row per symbol for ANY status, including
    'complete' and 'unavailable'.  Used by read endpoints that need to join
    schedule fields (expected_at, expected_timing, etc.) even after a target
    has advanced beyond 'scheduled'.
    """
    if not symbols:
        return []
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        placeholders = ",".join(["%s"] * len(symbols))
        cur.execute(f"""
            SELECT DISTINCT ON (symbol) {_TARGET_SELECT}
            FROM   public.earnings_monitor_targets
            WHERE  symbol = ANY(ARRAY[{placeholders}])
            ORDER  BY symbol, expected_date DESC NULLS LAST
        """, [s.upper() for s in symbols])
        rows = cur.fetchall()
        cur.close()
        return [dict(zip(_TARGET_COLS, r)) for r in rows]
    except Exception as exc:
        print(f"[EarnMonStore] get_targets_for_symbols error: {exc}")
        return []
    finally:
        _put_conn(conn)


def get_active_targets(limit: int = 100) -> list[dict]:
    """Return targets not yet complete, ordered by expected_date."""
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT {_TARGET_SELECT}
            FROM   public.earnings_monitor_targets
            WHERE  status NOT IN ('complete','unavailable')
            ORDER  BY expected_date ASC NULLS LAST
            LIMIT  %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        return [dict(zip(_TARGET_COLS, r)) for r in rows]
    except Exception as exc:
        print(f"[EarnMonStore] get_active_targets error: {exc}")
        return []
    finally:
        _put_conn(conn)


def get_due_targets(limit: int = 100) -> list[dict]:
    """
    Return active targets that are DUE for a check right now.
    A target is due when next_fmp_check_at <= NOW() OR next_sec_check_at <= NOW().
    Ordered by the earliest due time ascending (most overdue first).
    """
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT {_TARGET_SELECT}
            FROM   public.earnings_monitor_targets
            WHERE  status NOT IN ('complete','unavailable')
              AND  (
                    (next_fmp_check_at IS NOT NULL AND next_fmp_check_at <= NOW())
                 OR (next_sec_check_at IS NOT NULL AND next_sec_check_at <= NOW())
                 OR (next_fmp_check_at IS NULL AND next_sec_check_at IS NULL)
              )
            ORDER BY LEAST(
                COALESCE(next_fmp_check_at, NOW() + INTERVAL '999 days'),
                COALESCE(next_sec_check_at, NOW() + INTERVAL '999 days')
            ) ASC NULLS LAST
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        return [dict(zip(_TARGET_COLS, r)) for r in rows]
    except Exception as exc:
        print(f"[EarnMonStore] get_due_targets error: {exc}")
        return []
    finally:
        _put_conn(conn)


def claim_target(target_id: int, worker_id: str, lease_seconds: int = 90) -> bool:
    """
    Atomic lease claim.  Only succeeds if no active lease exists.
    Returns True when this worker owns the lease.
    """
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE public.earnings_monitor_targets
            SET    worker_lease_owner      = %s,
                   worker_lease_expires_at = NOW() + (%s || ' seconds')::interval,
                   updated_at             = NOW()
            WHERE  id = %s
              AND  (worker_lease_owner IS NULL
                    OR worker_lease_expires_at < NOW()
                    OR worker_lease_owner = %s)
            RETURNING id
        """, (worker_id, str(lease_seconds), target_id, worker_id))
        row = cur.fetchone()
        conn.commit()
        cur.close()
        return row is not None
    except Exception as exc:
        print(f"[EarnMonStore] claim_target error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def update_target(target_id: int, **fields) -> bool:
    """Update arbitrary fields on a target row."""
    allowed = {
        "status", "next_sec_check_at", "next_fmp_check_at",
        "monitoring_start_at", "monitoring_end_at",
        "fiscal_period", "fiscal_year", "expected_timing",
        "worker_lease_owner", "worker_lease_expires_at",
        # Part 2 timing fields
        "expected_at", "expected_time_local", "expected_timezone",
        "report_time_status", "report_period", "schedule_source",
        "schedule_updated_at", "fmp_check_stage",
        "results_first_detected_at", "monitoring_expires_at",
    }
    update_fields = {k: v for k, v in fields.items() if k in allowed}
    if not update_fields:
        return False
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        set_clause = ", ".join(f"{k} = %s" for k in update_fields)
        values = list(update_fields.values()) + [target_id]
        cur.execute(
            f"UPDATE public.earnings_monitor_targets SET {set_clause}, updated_at = NOW() WHERE id = %s",
            values,
        )
        conn.commit()
        cur.close()
        return True
    except Exception as exc:
        print(f"[EarnMonStore] update_target error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


# ── live events ───────────────────────────────────────────────────────────────

def _make_event_id(event_key: str, is_dry_run: bool = False) -> str:
    prefix = "dry_" if is_dry_run else ""
    h = hashlib.sha1(event_key.encode()).hexdigest()[:12]
    return f"{prefix}earn_{h}"


def upsert_live_event(
    event_key: str,
    symbol: str,
    state: str,
    expected_date: str | None       = None,
    fiscal_period: str | None       = None,
    fiscal_year: int | None         = None,
    detected_at: str | None         = None,
    revision: int                   = 1,
    is_dry_run: bool                = False,
    filing_payload: dict | None     = None,
    results_payload: dict | None    = None,
    reaction_payload: dict | None   = None,
    source_status: dict | None      = None,
    classification: str | None      = None,
    checksum: str | None            = None,
    last_error: str | None          = None,
) -> tuple[str, bool, bool]:
    """
    Upsert a live earnings event.
    Returns (event_id, is_new, checksum_changed).
    """
    event_id = _make_event_id(event_key, is_dry_run)
    conn = _get_conn()
    if conn is None:
        return event_id, False, False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO public.earnings_live_events
                (event_id, event_key, symbol, expected_date, fiscal_period, fiscal_year,
                 state, detected_at, revision, is_dry_run,
                 filing_payload, results_payload, reaction_payload,
                 source_status, classification, checksum, last_error, updated_at)
            VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW())
            ON CONFLICT (event_id) DO UPDATE SET
                state            = EXCLUDED.state,
                detected_at      = COALESCE(EXCLUDED.detected_at, earnings_live_events.detected_at),
                revision         = EXCLUDED.revision,
                filing_payload   = COALESCE(EXCLUDED.filing_payload,   earnings_live_events.filing_payload),
                results_payload  = COALESCE(EXCLUDED.results_payload,  earnings_live_events.results_payload),
                reaction_payload = COALESCE(EXCLUDED.reaction_payload, earnings_live_events.reaction_payload),
                source_status    = COALESCE(EXCLUDED.source_status,    earnings_live_events.source_status),
                classification   = COALESCE(EXCLUDED.classification,   earnings_live_events.classification),
                checksum         = COALESCE(EXCLUDED.checksum,         earnings_live_events.checksum),
                last_error       = EXCLUDED.last_error,
                updated_at       = NOW()
            RETURNING id
        """, (
            event_id, event_key, symbol, expected_date, fiscal_period, fiscal_year,
            state, detected_at, revision, is_dry_run,
            _jdump(filing_payload), _jdump(results_payload), _jdump(reaction_payload),
            _jdump(source_status), classification, checksum, last_error,
        ))
        row = cur.fetchone()
        conn.commit()
        cur.close()
        # is_new and ck_changed are determined by the caller via Python-level comparisons;
        # the upsert always succeeds so return (event_id, True, True) to let the caller decide.
        upserted = row is not None
        return event_id, upserted, upserted
    except Exception as exc:
        print(f"[EarnMonStore] upsert_live_event error {event_key}: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return event_id, False, False
    finally:
        _put_conn(conn)


def get_live_event_by_key(event_key: str, is_dry_run: bool = False) -> dict | None:
    event_id = _make_event_id(event_key, is_dry_run)
    return get_live_event_by_id(event_id)


def get_live_event_by_id(event_id: str) -> dict | None:
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT event_id, event_key, symbol, expected_date, fiscal_period, fiscal_year,
                   state, detected_at, updated_at, revision, is_dry_run,
                   filing_payload, results_payload, reaction_payload,
                   source_status, classification, checksum, last_error, created_at
            FROM   public.earnings_live_events
            WHERE  event_id = %s
        """, (event_id,))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        cols = ["event_id","event_key","symbol","expected_date","fiscal_period","fiscal_year",
                "state","detected_at","updated_at","revision","is_dry_run",
                "filing_payload","results_payload","reaction_payload",
                "source_status","classification","checksum","last_error","created_at"]
        return dict(zip(cols, row))
    except Exception as exc:
        print(f"[EarnMonStore] get_live_event_by_id error: {exc}")
        return None
    finally:
        _put_conn(conn)


def get_live_event_for_symbol(symbol: str, include_dry_run: bool = False) -> dict | None:
    """
    Canonical current live event for a symbol.

    Within the same symbol, selects the most advanced state using explicit
    precedence so a stale 'scheduled' transition never beats 'complete' or
    'results_available' merely because its updated_at was touched more recently.

    Precedence (ascending = wins):
      complete=0, results_updated=1, results_available=2, results_partial=3,
      filing_detected=4, monitoring=5, scheduled=6, other=7
    Ties broken by revision DESC then updated_at DESC.
    """
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        dry_clause = "" if include_dry_run else "AND is_dry_run = FALSE"
        cur.execute(f"""
            SELECT event_id, event_key, symbol, expected_date, fiscal_period, fiscal_year,
                   state, detected_at, updated_at, revision, is_dry_run,
                   filing_payload, results_payload, reaction_payload,
                   source_status, classification, checksum, created_at
            FROM   public.earnings_live_events
            WHERE  symbol = %s {dry_clause}
            ORDER  BY
                CASE state
                    WHEN 'complete'          THEN 0
                    WHEN 'results_updated'   THEN 1
                    WHEN 'results_available' THEN 2
                    WHEN 'results_partial'   THEN 3
                    WHEN 'filing_detected'   THEN 4
                    WHEN 'monitoring'        THEN 5
                    WHEN 'scheduled'         THEN 6
                    ELSE                          7
                END ASC,
                revision DESC,
                updated_at DESC
            LIMIT  1
        """, (symbol.upper(),))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        cols = ["event_id","event_key","symbol","expected_date","fiscal_period","fiscal_year",
                "state","detected_at","updated_at","revision","is_dry_run",
                "filing_payload","results_payload","reaction_payload",
                "source_status","classification","checksum","created_at"]
        return dict(zip(cols, row))
    except Exception as exc:
        print(f"[EarnMonStore] get_live_event_for_symbol error: {exc}")
        return None
    finally:
        _put_conn(conn)


def get_live_events_for_symbols(symbols: list[str], include_dry_run: bool = False) -> dict[str, dict]:
    """
    Return {symbol: canonical_current_event} for a list of symbols.

    Uses the same state-precedence ORDER BY as get_live_event_for_symbol so
    that DISTINCT ON (symbol) selects the most advanced valid state rather than
    the most recently touched row.
    """
    if not symbols:
        return {}
    conn = _get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor()
        dry_clause = "" if include_dry_run else "AND is_dry_run = FALSE"
        placeholders = ",".join(["%s"] * len(symbols))
        cur.execute(f"""
            SELECT DISTINCT ON (symbol)
                   event_id, event_key, symbol, expected_date, fiscal_period, fiscal_year,
                   state, detected_at, updated_at, revision, is_dry_run,
                   filing_payload, results_payload, reaction_payload,
                   source_status, classification, checksum, created_at
            FROM   public.earnings_live_events
            WHERE  symbol = ANY(ARRAY[{placeholders}]) {dry_clause}
            ORDER  BY symbol,
                CASE state
                    WHEN 'complete'          THEN 0
                    WHEN 'results_updated'   THEN 1
                    WHEN 'results_available' THEN 2
                    WHEN 'results_partial'   THEN 3
                    WHEN 'filing_detected'   THEN 4
                    WHEN 'monitoring'        THEN 5
                    WHEN 'scheduled'         THEN 6
                    ELSE                          7
                END ASC,
                revision DESC,
                updated_at DESC
        """, [s.upper() for s in symbols])
        cols = ["event_id","event_key","symbol","expected_date","fiscal_period","fiscal_year",
                "state","detected_at","updated_at","revision","is_dry_run",
                "filing_payload","results_payload","reaction_payload",
                "source_status","classification","checksum","created_at"]
        result = {}
        for row in cur.fetchall():
            d = dict(zip(cols, row))
            result[d["symbol"]] = d
        cur.close()
        return result
    except Exception as exc:
        print(f"[EarnMonStore] get_live_events_for_symbols error: {exc}")
        return {}
    finally:
        _put_conn(conn)


def get_recent_live_events(
    limit: int = 50,
    since_iso: str | None = None,
    include_dry_run: bool = False,
) -> list[dict]:
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        conditions = ["1=1"]
        params: list = []
        if not include_dry_run:
            conditions.append("is_dry_run = FALSE")
        if since_iso:
            conditions.append("updated_at > %s")
            params.append(since_iso)
        where = " AND ".join(conditions)
        params.append(limit)
        cur.execute(f"""
            SELECT event_id, event_key, symbol, expected_date, fiscal_period, fiscal_year,
                   state, detected_at, updated_at, revision, is_dry_run,
                   filing_payload, results_payload, reaction_payload,
                   source_status, classification, checksum, created_at
            FROM   public.earnings_live_events
            WHERE  {where}
            ORDER  BY updated_at DESC
            LIMIT  %s
        """, params)
        cols = ["event_id","event_key","symbol","expected_date","fiscal_period","fiscal_year",
                "state","detected_at","updated_at","revision","is_dry_run",
                "filing_payload","results_payload","reaction_payload",
                "source_status","classification","checksum","created_at"]
        rows = cur.fetchall()
        cur.close()
        return [dict(zip(cols, r)) for r in rows]
    except Exception as exc:
        print(f"[EarnMonStore] get_recent_live_events error: {exc}")
        return []
    finally:
        _put_conn(conn)


# ── reaction payload update ───────────────────────────────────────────────────

def update_reaction_payload(event_id: str, payload: dict, merge: bool = True) -> bool:
    """
    Write (or JSON-merge) reaction_payload for an existing live event.

    merge=True  — JSONB || operator: preserves keys already in the row that are
                  absent from payload (e.g. preliminary snapshot stays when
                  adding post_3d later).
    merge=False — full replacement.
    """
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        payload_json = _jdump(payload)
        if merge:
            # Guard against JSON null (stored as 'null'::jsonb) which makes
            # the || operator produce an array instead of a merged object.
            cur.execute(
                """
                UPDATE public.earnings_live_events
                SET    reaction_payload = CASE
                           WHEN reaction_payload IS NULL
                             OR reaction_payload = 'null'::jsonb
                             OR jsonb_typeof(reaction_payload) <> 'object'
                           THEN %s::jsonb
                           ELSE reaction_payload || %s::jsonb
                       END,
                       updated_at = NOW()
                WHERE  event_id = %s
                """,
                (payload_json, payload_json, event_id),
            )
        else:
            cur.execute(
                """
                UPDATE public.earnings_live_events
                SET    reaction_payload = %s::jsonb,
                       updated_at       = NOW()
                WHERE  event_id = %s
                """,
                (payload_json, event_id),
            )
        conn.commit()
        cur.close()
        return True
    except Exception as exc:
        print(f"[EarnMonStore] update_reaction_payload error {event_id}: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def get_recent_complete_events_for_symbols(
    symbols: list[str],
    since_date: str,
    states: tuple[str, ...] = ("results_available", "results_updated", "complete"),
) -> list[dict]:
    """
    Return the most-advanced recent completed event per symbol (DISTINCT ON symbol).

    Used by watchlist earnings endpoint to build the "recent" section: events
    that already reported (state in states) since since_date.

    Returns a flat list ordered by expected_date DESC.
    """
    if not symbols:
        return []
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        placeholders = ",".join(["%s"] * len(symbols))
        state_ph     = ",".join(["%s"] * len(states))
        cur.execute(
            f"""
            SELECT DISTINCT ON (symbol)
                   event_id, event_key, symbol, expected_date, fiscal_period, fiscal_year,
                   state, detected_at, updated_at, revision, is_dry_run,
                   filing_payload, results_payload, reaction_payload,
                   source_status, classification, checksum, created_at
            FROM   public.earnings_live_events
            WHERE  symbol = ANY(ARRAY[{placeholders}])
              AND  is_dry_run = FALSE
              AND  state IN ({state_ph})
              AND  expected_date >= %s
            ORDER  BY symbol,
                CASE state
                    WHEN 'complete'          THEN 0
                    WHEN 'results_updated'   THEN 1
                    WHEN 'results_available' THEN 2
                    ELSE                          9
                END ASC,
                revision DESC,
                updated_at DESC
            """,
            [s.upper() for s in symbols] + list(states) + [since_date],
        )
        cols = [
            "event_id", "event_key", "symbol", "expected_date", "fiscal_period",
            "fiscal_year", "state", "detected_at", "updated_at", "revision",
            "is_dry_run", "filing_payload", "results_payload", "reaction_payload",
            "source_status", "classification", "checksum", "created_at",
        ]
        rows = cur.fetchall()
        cur.close()
        result = [dict(zip(cols, r)) for r in rows]
        result.sort(key=lambda x: str(x.get("expected_date") or ""), reverse=True)
        return result
    except Exception as exc:
        print(f"[EarnMonStore] get_recent_complete_events_for_symbols error: {exc}")
        return []
    finally:
        _put_conn(conn)


# ── per-user event feed ───────────────────────────────────────────────────────

def mark_event_read(event_id: str, user_id: str) -> bool:
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO public.earnings_event_reads (event_id, user_id)
            VALUES (%s, %s)
            ON CONFLICT (event_id, user_id) DO UPDATE SET read_at = NOW()
        """, (event_id, user_id))
        conn.commit()
        cur.close()
        return True
    except Exception as exc:
        print(f"[EarnMonStore] mark_event_read error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def get_user_event_feed(
    user_id: str,
    symbols: list[str],
    since_iso: str | None  = None,
    unread_only: bool      = False,
    limit: int             = 50,
    include_dry_run: bool  = False,
) -> list[dict]:
    """
    Return live events for the user's symbol universe, with read status.
    """
    if not symbols:
        return []
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        # Build WHERE conditions and their positional params separately.
        # IMPORTANT: the SQL has r.user_id = %s in the LEFT JOIN ON clause,
        # which comes BEFORE the WHERE params in psycopg2 substitution order.
        # Final params order: [user_id, *sym_params, *extra_where_params, limit]
        conditions   = [f"e.symbol = ANY(ARRAY[{','.join(['%s']*len(symbols))}])"]
        sym_params   = [s.upper() for s in symbols]
        extra_params: list = []

        if not include_dry_run:
            conditions.append("e.is_dry_run = FALSE")
        if since_iso:
            conditions.append("e.updated_at > %s")
            extra_params.append(since_iso)
        if unread_only:
            conditions.append("r.read_at IS NULL")

        where  = " AND ".join(conditions)
        params = [user_id] + sym_params + extra_params + [limit]

        cur.execute(f"""
            SELECT e.event_id, e.event_key, e.symbol,
                   e.expected_date, e.fiscal_period, e.fiscal_year,
                   e.state, e.detected_at, e.updated_at, e.revision,
                   e.is_dry_run, e.filing_payload, e.results_payload,
                   e.reaction_payload, e.classification, e.created_at,
                   r.read_at
            FROM   public.earnings_live_events e
            LEFT   JOIN public.earnings_event_reads r
                   ON r.event_id = e.event_id AND r.user_id = %s
            WHERE  {where}
            ORDER  BY e.updated_at DESC
            LIMIT  %s
        """, params)
        cols = ["event_id","event_key","symbol","expected_date","fiscal_period","fiscal_year",
                "state","detected_at","updated_at","revision","is_dry_run",
                "filing_payload","results_payload","reaction_payload",
                "classification","created_at","read_at"]
        rows = cur.fetchall()
        cur.close()
        return [dict(zip(cols, r)) for r in rows]
    except Exception as exc:
        print(f"[EarnMonStore] get_user_event_feed error: {exc}")
        return []
    finally:
        _put_conn(conn)


def get_event_read_ids(user_id: str, event_ids: list[str]) -> set[str]:
    """Return the subset of event_ids that user has read."""
    if not event_ids:
        return set()
    conn = _get_conn()
    if conn is None:
        return set()
    try:
        cur = conn.cursor()
        placeholders = ",".join(["%s"] * len(event_ids))
        cur.execute(
            f"SELECT event_id FROM public.earnings_event_reads WHERE user_id = %s AND event_id IN ({placeholders})",
            [user_id] + list(event_ids),
        )
        rows = cur.fetchall()
        cur.close()
        return {r[0] for r in rows}
    except Exception as exc:
        print(f"[EarnMonStore] get_event_read_ids error: {exc}")
        return set()
    finally:
        _put_conn(conn)


# ── admin helpers ─────────────────────────────────────────────────────────────

def get_target_count() -> dict:
    conn = _get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT status, COUNT(*) FROM public.earnings_monitor_targets
            GROUP BY status ORDER BY status
        """)
        rows = cur.fetchall()
        cur.close()
        return {r[0]: r[1] for r in rows}
    except Exception as exc:
        print(f"[EarnMonStore] get_target_count error: {exc}")
        return {}
    finally:
        _put_conn(conn)


def delete_target(symbol: str, expected_date: str) -> bool:
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM public.earnings_monitor_targets WHERE symbol=%s AND expected_date=%s",
            (symbol, expected_date),
        )
        conn.commit()
        cur.close()
        return True
    except Exception as exc:
        print(f"[EarnMonStore] delete_target error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def delete_dry_run_events() -> int:
    conn = _get_conn()
    if conn is None:
        return 0
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM public.earnings_live_events WHERE is_dry_run = TRUE")
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        return deleted
    except Exception as exc:
        print(f"[EarnMonStore] delete_dry_run_events error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        _put_conn(conn)
