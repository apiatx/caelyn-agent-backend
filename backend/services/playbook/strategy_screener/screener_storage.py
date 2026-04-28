"""
Strategy Screener — PostgreSQL persistence layer.

Two tables:
  screener_snapshots — snapshot metadata + candidate list (JSONB)
  screener_reports   — per-candidate deep reports (JSONB), keyed by (snapshot_id, ticker)

Reuses the same connection pool from data.pg_storage.
All functions are synchronous (psycopg2) — same pattern as the rest of the app.

Disk fallback: when PostgreSQL is unavailable, completed snapshots are persisted to
  SCREENER_DISK_PATH (JSON) and loaded from there so the page stays functional.
  Only "complete" snapshots are written to disk; placeholder/error rows are not.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

_TABLES_CREATED = False

# Disk fallback path — relative to this file's repo root
_HERE = os.path.dirname(os.path.abspath(__file__))
SCREENER_DISK_PATH = os.path.join(_HERE, "..", "..", "..", "data", "strategy_screener_lkg.json")
SCREENER_DISK_PATH = os.path.normpath(SCREENER_DISK_PATH)


def _save_to_disk(snapshot: Dict[str, Any]) -> None:
    """Write a completed snapshot to the disk fallback file."""
    if snapshot.get("status") != "complete":
        return
    if not snapshot.get("results"):
        return
    try:
        with open(SCREENER_DISK_PATH, "w") as f:
            json.dump(snapshot, f, default=str)
        print(f"[SCREENER][DISK] Saved snapshot {snapshot.get('snapshot_id')} ({snapshot.get('results_count', 0)} candidates)")
    except Exception as e:
        print(f"[SCREENER][DISK] save_to_disk error: {e}")


def _load_from_disk() -> Optional[Dict[str, Any]]:
    """Load the last-known-good snapshot from disk. Returns None if absent or incomplete."""
    try:
        if not os.path.exists(SCREENER_DISK_PATH):
            return None
        with open(SCREENER_DISK_PATH) as f:
            snap = json.load(f)
        if snap.get("status") != "complete" or not snap.get("results"):
            return None
        print(f"[SCREENER][DISK] Loaded snapshot {snap.get('snapshot_id')} ({snap.get('results_count', 0)} candidates) from disk fallback")
        return snap
    except Exception as e:
        print(f"[SCREENER][DISK] load_from_disk error: {e}")
        return None


def _get_conn():
    from data.pg_storage import _get_conn as _pg_get
    return _pg_get()


def _put_conn(conn):
    from data.pg_storage import _put_conn as _pg_put
    _pg_put(conn)


# ── Table initialisation ───────────────────────────────────────────────────────

def init_screener_tables() -> bool:
    """Create screener tables if they don't exist. Safe to call multiple times."""
    global _TABLES_CREATED
    if _TABLES_CREATED:
        return True

    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.screener_snapshots (
                snapshot_id     TEXT PRIMARY KEY,
                playbook_id     TEXT NOT NULL DEFAULT 'serenity',
                generated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                cadence         TEXT NOT NULL DEFAULT 'biweekly',
                cadence_days    INTEGER NOT NULL DEFAULT 14,
                regime_context  JSONB,
                summary         TEXT NOT NULL DEFAULT '',
                results         JSONB NOT NULL DEFAULT '[]',
                results_count   INTEGER NOT NULL DEFAULT 0,
                status          TEXT NOT NULL DEFAULT 'complete',
                version         TEXT NOT NULL DEFAULT '1.0',
                generation_notes TEXT NOT NULL DEFAULT '',
                manual_override  BOOLEAN NOT NULL DEFAULT FALSE
            )
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_screener_snapshots_generated
            ON public.screener_snapshots (generated_at DESC)
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.screener_reports (
                id           BIGSERIAL PRIMARY KEY,
                snapshot_id  TEXT NOT NULL,
                ticker       TEXT NOT NULL,
                report       JSONB NOT NULL,
                generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (snapshot_id, ticker)
            )
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_screener_reports_snapshot
            ON public.screener_reports (snapshot_id, ticker)
        """)

        conn.commit()
        cur.close()
        _TABLES_CREATED = True
        print("[SCREENER][DB] Tables initialized")
        return True
    except Exception as e:
        print(f"[SCREENER][DB] init_screener_tables error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


# ── Snapshot CRUD ──────────────────────────────────────────────────────────────

def save_snapshot(snapshot: Dict[str, Any]) -> bool:
    """Upsert a snapshot row. Also writes to disk as fallback when DB is unavailable."""
    # Always attempt disk write for complete snapshots first (DB-independent)
    _save_to_disk(snapshot)

    init_screener_tables()
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO public.screener_snapshots
                (snapshot_id, playbook_id, generated_at, cadence, cadence_days,
                 regime_context, summary, results, results_count,
                 status, version, generation_notes, manual_override)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (snapshot_id) DO UPDATE SET
                generated_at     = EXCLUDED.generated_at,
                regime_context   = EXCLUDED.regime_context,
                summary          = EXCLUDED.summary,
                results          = EXCLUDED.results,
                results_count    = EXCLUDED.results_count,
                status           = EXCLUDED.status,
                version          = EXCLUDED.version,
                generation_notes = EXCLUDED.generation_notes,
                manual_override  = EXCLUDED.manual_override
        """, (
            snapshot["snapshot_id"],
            snapshot.get("playbook_id", "serenity"),
            snapshot.get("generated_at"),
            snapshot.get("cadence", "biweekly"),
            snapshot.get("cadence_days", 14),
            json.dumps(snapshot.get("regime_context")) if snapshot.get("regime_context") else None,
            snapshot.get("summary", ""),
            json.dumps(snapshot.get("results", [])),
            snapshot.get("results_count", 0),
            snapshot.get("status", "complete"),
            snapshot.get("version", "1.0"),
            snapshot.get("generation_notes", ""),
            snapshot.get("manual_override", False),
        ))
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[SCREENER][DB] save_snapshot error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def get_latest_snapshot() -> Optional[Dict[str, Any]]:
    """Return the most recently generated snapshot row, or None.
    Falls back to disk if the database is unavailable or returns nothing."""
    init_screener_tables()
    conn = _get_conn()
    if conn is None:
        return _load_from_disk()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT snapshot_id, playbook_id, generated_at, cadence, cadence_days,
                   regime_context, summary, results, results_count,
                   status, version, generation_notes
            FROM public.screener_snapshots
            ORDER BY generated_at DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        cur.close()
        if not row:
            return _load_from_disk()
        snap = _row_to_snapshot(row)
        # If DB row is a placeholder/error with no results, prefer disk if it has data
        if snap.get("status") in ("generating", "error") and not snap.get("results"):
            disk = _load_from_disk()
            if disk:
                return disk
        return snap
    except Exception as e:
        print(f"[SCREENER][DB] get_latest_snapshot error: {e}")
        return _load_from_disk()
    finally:
        _put_conn(conn)


def list_snapshots(limit: int = 10) -> List[Dict[str, Any]]:
    """Return the N most recent snapshots (metadata only, no results payload)."""
    init_screener_tables()
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT snapshot_id, playbook_id, generated_at, cadence, cadence_days,
                   NULL, summary, '[]'::jsonb, results_count,
                   status, version, generation_notes
            FROM public.screener_snapshots
            ORDER BY generated_at DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        return [_row_to_snapshot(r) for r in rows]
    except Exception as e:
        print(f"[SCREENER][DB] list_snapshots error: {e}")
        return []
    finally:
        _put_conn(conn)


def get_snapshot_by_id(snapshot_id: str) -> Optional[Dict[str, Any]]:
    """Fetch a specific snapshot by ID."""
    init_screener_tables()
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT snapshot_id, playbook_id, generated_at, cadence, cadence_days,
                   regime_context, summary, results, results_count,
                   status, version, generation_notes
            FROM public.screener_snapshots
            WHERE snapshot_id = %s
        """, (snapshot_id,))
        row = cur.fetchone()
        cur.close()
        return _row_to_snapshot(row) if row else None
    except Exception as e:
        print(f"[SCREENER][DB] get_snapshot_by_id error: {e}")
        return None
    finally:
        _put_conn(conn)


def _row_to_snapshot(row) -> Dict[str, Any]:
    (snapshot_id, playbook_id, generated_at, cadence, cadence_days,
     regime_context, summary, results, results_count,
     status, version, generation_notes) = row
    return {
        "snapshot_id":     snapshot_id,
        "playbook_id":     playbook_id,
        "generated_at":    generated_at.isoformat() if hasattr(generated_at, "isoformat") else str(generated_at),
        "cadence":         cadence,
        "cadence_days":    cadence_days,
        "regime_context":  regime_context if isinstance(regime_context, dict) else (
                               json.loads(regime_context) if isinstance(regime_context, str) else None),
        "summary":         summary or "",
        "results":         results if isinstance(results, list) else (
                               json.loads(results) if isinstance(results, str) else []),
        "results_count":   results_count or 0,
        "status":          status or "complete",
        "version":         version or "1.0",
        "generation_notes": generation_notes or "",
    }


def patch_snapshot_market_caps(snapshot_id: str, enriched_results: list) -> bool:
    """
    Update only the results JSONB of a stored snapshot (market cap backfill).
    Does NOT change any other snapshot fields (generated_at, status, summary, etc).
    """
    init_screener_tables()
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE public.screener_snapshots
            SET results = %s::jsonb
            WHERE snapshot_id = %s
        """, (json.dumps(enriched_results, default=str), snapshot_id))
        conn.commit()
        affected = cur.rowcount
        cur.close()
        return affected > 0
    except Exception as e:
        print(f"[SCREENER][DB] patch_snapshot_market_caps error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


# ── Report CRUD ────────────────────────────────────────────────────────────────

def save_report(snapshot_id: str, ticker: str, report: Dict[str, Any]) -> bool:
    """Upsert a full report for one candidate."""
    init_screener_tables()
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO public.screener_reports (snapshot_id, ticker, report, generated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (snapshot_id, ticker) DO UPDATE SET
                report       = EXCLUDED.report,
                generated_at = EXCLUDED.generated_at
        """, (snapshot_id, ticker, json.dumps(report, default=str)))
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[SCREENER][DB] save_report error ({ticker}): {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def get_report(snapshot_id: str, ticker: str) -> Optional[Dict[str, Any]]:
    """Fetch the full report for a single candidate."""
    init_screener_tables()
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT report FROM public.screener_reports
            WHERE snapshot_id = %s AND ticker = %s
        """, (snapshot_id, ticker))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        r = row[0]
        return r if isinstance(r, dict) else json.loads(r)
    except Exception as e:
        print(f"[SCREENER][DB] get_report error ({snapshot_id}/{ticker}): {e}")
        return None
    finally:
        _put_conn(conn)


def get_latest_report(ticker: str) -> Optional[Dict[str, Any]]:
    """Fetch the most recent report for a ticker across all snapshots."""
    init_screener_tables()
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT r.report FROM public.screener_reports r
            JOIN public.screener_snapshots s ON r.snapshot_id = s.snapshot_id
            WHERE r.ticker = %s
            ORDER BY s.generated_at DESC
            LIMIT 1
        """, (ticker,))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        r = row[0]
        return r if isinstance(r, dict) else json.loads(r)
    except Exception as e:
        print(f"[SCREENER][DB] get_latest_report error ({ticker}): {e}")
        return None
    finally:
        _put_conn(conn)
