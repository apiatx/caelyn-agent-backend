"""
Prediction Market Odds Snapshots — Neon/Postgres persistence layer.

Table: public.prediction_market_odds_snapshots

One row per (family_key, scan_time) snapshot.  Retains 7 days by default.
Delta computation reads prior snapshots at ~1h / ~24h / ~7d before the
current snapshot's captured_at timestamp.

Follows the same patterns as screener_hub_store.py:
  - Imports _get_conn / _put_conn / is_available from data.pg_storage
  - Never raises — all helpers catch, log, and return 0 / [] / {} on failure
  - _DDL_APPLIED flag prevents redundant DDL on every call
  - Uses public. schema prefix
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

log = logging.getLogger("predict_odds_store")

try:
    from data.pg_storage import _get_conn, _put_conn, is_available  # type: ignore
except Exception:
    _get_conn = lambda: None   # type: ignore
    _put_conn = lambda c: None  # type: ignore
    def is_available() -> bool:  # type: ignore
        return False

_DDL_APPLIED = False
_TABLE = "public.prediction_market_odds_snapshots"


# ── DDL ───────────────────────────────────────────────────────────────────────

def _ddl_sql() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {_TABLE} (
        id              BIGSERIAL PRIMARY KEY,
        family_key      TEXT        NOT NULL,
        market_id       TEXT        NOT NULL,
        market_slug     TEXT,
        question        TEXT,
        source          TEXT        NOT NULL DEFAULT 'polymarket',
        yes_probability NUMERIC(8,6),
        no_probability  NUMERIC(8,6),
        best_bid        NUMERIC(8,6),
        best_ask        NUMERIC(8,6),
        volume_24h      NUMERIC(20,4),
        liquidity       NUMERIC(20,4),
        end_date        TEXT,
        captured_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        raw_json        JSONB
    );
    CREATE INDEX IF NOT EXISTS idx_pm_odds_fk_cap
        ON {_TABLE} (family_key, captured_at DESC);
    CREATE INDEX IF NOT EXISTS idx_pm_odds_captured
        ON {_TABLE} (captured_at DESC);
    """


def ensure_table() -> bool:
    """Create table + indexes if they don't exist.  Safe to call repeatedly."""
    global _DDL_APPLIED
    if _DDL_APPLIED:
        return True
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(_ddl_sql())
        conn.commit()
        cur.close()
        _DDL_APPLIED = True
        log.info("[predict_odds_store] DDL applied — %s ready", _TABLE)
        return True
    except Exception as exc:
        log.warning("[predict_odds_store] DDL failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


# ── Writes ────────────────────────────────────────────────────────────────────

def insert_snapshots(rows: list[dict]) -> int:
    """
    Bulk-insert snapshot rows.  Each dict must have:
        family_key, market_id
    Optional:
        market_slug, question, source, yes_probability, no_probability,
        best_bid, best_ask, volume_24h, liquidity, end_date, captured_at, raw_json

    Returns number of rows inserted (0 on failure).
    """
    if not rows:
        return 0
    ensure_table()
    conn = _get_conn()
    if conn is None:
        return 0
    count = 0
    try:
        cur = conn.cursor()
        for row in rows:
            fk = row.get("family_key", "")
            mid = row.get("market_id", "")
            if not fk or not mid:
                continue

            yes_prob = row.get("yes_probability")
            no_prob  = row.get("no_probability")
            raw      = row.get("raw_json")
            cap_at   = row.get("captured_at")   # datetime | None

            raw_json_val = None
            if raw is not None:
                try:
                    from psycopg2.extras import Json
                    raw_json_val = Json(raw if isinstance(raw, dict) else {"value": str(raw)})
                except Exception:
                    raw_json_val = json.dumps(raw if isinstance(raw, dict) else {"value": str(raw)}, default=str)

            if cap_at is not None:
                cur.execute(
                    f"""
                    INSERT INTO {_TABLE}
                        (family_key, market_id, market_slug, question, source,
                         yes_probability, no_probability, best_bid, best_ask,
                         volume_24h, liquidity, end_date, captured_at, raw_json)
                    VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s)
                    """,
                    (
                        fk, mid,
                        row.get("market_slug"),
                        row.get("question"),
                        row.get("source", "polymarket"),
                        yes_prob,
                        no_prob,
                        row.get("best_bid"),
                        row.get("best_ask"),
                        row.get("volume_24h"),
                        row.get("liquidity"),
                        row.get("end_date"),
                        cap_at,
                        raw_json_val,
                    ),
                )
            else:
                cur.execute(
                    f"""
                    INSERT INTO {_TABLE}
                        (family_key, market_id, market_slug, question, source,
                         yes_probability, no_probability, best_bid, best_ask,
                         volume_24h, liquidity, end_date, raw_json)
                    VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s)
                    """,
                    (
                        fk, mid,
                        row.get("market_slug"),
                        row.get("question"),
                        row.get("source", "polymarket"),
                        yes_prob,
                        no_prob,
                        row.get("best_bid"),
                        row.get("best_ask"),
                        row.get("volume_24h"),
                        row.get("liquidity"),
                        row.get("end_date"),
                        raw_json_val,
                    ),
                )
            count += 1
        conn.commit()
        cur.close()
        return count
    except Exception as exc:
        log.warning("[predict_odds_store] insert_snapshots error: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        _put_conn(conn)


def delete_old_snapshots(days: int = 7) -> int:
    """
    Delete snapshots older than `days` days.  Returns rows deleted (0 on failure).
    Safe to call every scan cycle — deletes only when rows exist.
    """
    ensure_table()
    conn = _get_conn()
    if conn is None:
        return 0
    try:
        cur = conn.cursor()
        cur.execute(
            f"DELETE FROM {_TABLE} WHERE captured_at < NOW() - INTERVAL %s",
            (f"{days} days",),
        )
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        if deleted:
            log.debug("[predict_odds_store] Retention: deleted %d rows older than %d days", deleted, days)
        return deleted
    except Exception as exc:
        log.warning("[predict_odds_store] delete_old_snapshots error: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        _put_conn(conn)


# ── Reads ─────────────────────────────────────────────────────────────────────

def get_latest_per_family() -> dict[str, dict]:
    """
    Return the most recent snapshot row for each family_key.
    Result: {family_key: {id, family_key, market_id, yes_probability, ...}}
    Returns {} on failure or if table is empty.
    """
    ensure_table()
    conn = _get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT DISTINCT ON (family_key)
                id, family_key, market_id, market_slug, question, source,
                yes_probability, no_probability, best_bid, best_ask,
                volume_24h, liquidity, end_date, captured_at, raw_json
            FROM {_TABLE}
            ORDER BY family_key, captured_at DESC
            """,
        )
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
        result: dict[str, dict] = {}
        for row in rows:
            rec = dict(zip(cols, row))
            cap = rec.get("captured_at")
            if hasattr(cap, "isoformat"):
                rec["captured_at"] = cap.isoformat()
            for field in ("yes_probability", "no_probability", "best_bid",
                          "best_ask", "volume_24h", "liquidity"):
                if rec.get(field) is not None:
                    rec[field] = float(rec[field])
            result[rec["family_key"]] = rec
        return result
    except Exception as exc:
        log.warning("[predict_odds_store] get_latest_per_family error: %s", exc)
        return {}
    finally:
        _put_conn(conn)


def get_snapshots_before(
    family_key: str,
    before_ts: float,
    window_seconds: int = 900,
    limit: int = 3,
) -> list[dict]:
    """
    Return up to `limit` snapshots for `family_key` whose captured_at is
    within [before_ts - window_seconds, before_ts].

    Used for delta computation:
      - 1h delta:  before_ts = now - 3600,  window = 900 (±15 min)
      - 24h delta: before_ts = now - 86400, window = 1800
      - 7d delta:  before_ts = now - 604800, window = 7200
    """
    ensure_table()
    conn = _get_conn()
    if conn is None:
        return []
    try:
        from psycopg2.extras import RealDictCursor
        target = datetime.fromtimestamp(before_ts, tz=timezone.utc)
        low    = datetime.fromtimestamp(before_ts - window_seconds, tz=timezone.utc)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            f"""
            SELECT id, family_key, yes_probability, captured_at
            FROM {_TABLE}
            WHERE family_key = %s
              AND captured_at BETWEEN %s AND %s
            ORDER BY ABS(EXTRACT(EPOCH FROM (captured_at - %s)))
            LIMIT %s
            """,
            (family_key, low, target, target, limit),
        )
        rows = cur.fetchall()
        cur.close()
        result = []
        for row in rows:
            rec = dict(row)
            cap = rec.get("captured_at")
            if hasattr(cap, "isoformat"):
                rec["captured_at"] = cap.isoformat()
            if rec.get("yes_probability") is not None:
                rec["yes_probability"] = float(rec["yes_probability"])
            result.append(rec)
        return result
    except Exception as exc:
        log.warning("[predict_odds_store] get_snapshots_before error: %s", exc)
        return []
    finally:
        _put_conn(conn)


def get_history(family_key: str, days: int = 7) -> list[dict]:
    """
    Return time-series snapshots for a family over the last `days` days.
    Each row: {captured_at, yes_probability, volume_24h, liquidity}
    Ordered oldest → newest.  Empty list on failure or no data.
    """
    ensure_table()
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT captured_at, yes_probability, volume_24h, liquidity
            FROM {_TABLE}
            WHERE family_key = %s
              AND captured_at >= NOW() - INTERVAL %s
            ORDER BY captured_at ASC
            """,
            (family_key, f"{days} days"),
        )
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
        result = []
        for row in rows:
            rec = dict(zip(cols, row))
            cap = rec.get("captured_at")
            if hasattr(cap, "isoformat"):
                rec["captured_at"] = cap.isoformat()
            for field in ("yes_probability", "volume_24h", "liquidity"):
                if rec.get(field) is not None:
                    rec[field] = float(rec[field])
            result.append(rec)
        return result
    except Exception as exc:
        log.warning("[predict_odds_store] get_history error: %s", exc)
        return []
    finally:
        _put_conn(conn)


def get_diagnostics() -> dict:
    """Snapshot table stats for /diagnostics endpoints."""
    ensure_table()
    conn = _get_conn()
    if conn is None:
        return {"available": False}
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT COUNT(*), MAX(captured_at), MIN(captured_at) FROM {_TABLE}",
        )
        row = cur.fetchone()
        cur.execute(
            f"SELECT COUNT(DISTINCT family_key) FROM {_TABLE}",
        )
        fk_count = cur.fetchone()[0]
        cur.close()
        return {
            "available":      True,
            "table":          _TABLE,
            "total_rows":     row[0] if row else 0,
            "newest_row":     row[1].isoformat() if row and row[1] else None,
            "oldest_row":     row[2].isoformat() if row and row[2] else None,
            "distinct_families": fk_count,
        }
    except Exception as exc:
        log.warning("[predict_odds_store] get_diagnostics error: %s", exc)
        return {"available": False, "error": str(exc)}
    finally:
        _put_conn(conn)
