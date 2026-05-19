"""Options position and closed-trade persistence (Neon PostgreSQL).

Tables
------
  portfolio_option_positions
    One row per open/partially-closed option position (keyed by occ_key).
    Upserted on import; deleted on full_replace.

  portfolio_option_closed_trades
    One row per close event (Sell to Close, Expired).
    Appended on import; CSV-imported rows deleted on full_replace.

Public API
----------
  load_option_positions()                    -> list[dict]
  save_option_positions_batch(rows)          -> int   (rows upserted)
  delete_csv_import_option_positions()       -> int   (rows deleted)

  load_option_closed_trades()                -> list[dict]
  save_option_closed_trades_batch(rows)      -> int   (rows inserted)
  delete_csv_import_option_closed_trades()   -> int   (rows deleted)
"""
from __future__ import annotations

import os
import uuid
from datetime import date, datetime
from typing import Any

_DB_URL  = os.environ.get("NEON_DATABASE_URL") or os.environ.get("DATABASE_URL")
_db_pool = None

import time as _time_opt
_opt_pos_cache: dict = {"data": None, "ts": 0.0}
_opt_ct_cache:  dict = {"data": None, "ts": 0.0}
_OPT_CACHE_TTL = 45.0   # seconds — invalidated immediately on any write


def _invalidate_opt_pos_cache() -> None:
    _opt_pos_cache["data"] = None


def _invalidate_opt_ct_cache() -> None:
    _opt_ct_cache["data"] = None


# ── Connection pool ───────────────────────────────────────────────────────────

def _get_conn():
    global _db_pool
    if not _DB_URL:
        return None
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

    def _clean(url: str) -> str:
        try:
            p  = urlparse(url)
            qs = parse_qs(p.query, keep_blank_values=True)
            qs.pop("channel_binding", None)
            return urlunparse(p._replace(query=urlencode(qs, doseq=True)))
        except Exception:
            return url

    for _ in range(2):
        if _db_pool is None:
            try:
                from psycopg2 import pool as _pgpool
                _db_pool = _pgpool.SimpleConnectionPool(1, 3, _clean(_DB_URL))
            except Exception as e:
                print(f"[OPT_STORE] pool init failed: {e}")
                return None
        try:
            conn = _db_pool.getconn()
            cur  = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            conn.commit()
            cur.close()
            return conn
        except Exception:
            try:
                _db_pool.closeall()
            except Exception:
                pass
            _db_pool = None
    return None


def _put_conn(conn) -> None:
    global _db_pool
    if _db_pool and conn:
        try:
            _db_pool.putconn(conn)
        except Exception:
            pass


# ── Schema creation ───────────────────────────────────────────────────────────

def _ensure_tables(conn) -> None:
    cur = conn.cursor()

    # ── Option positions ───────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS portfolio_option_positions (
            id                  TEXT PRIMARY KEY,
            occ_key             TEXT NOT NULL UNIQUE,
            underlying          TEXT NOT NULL,
            display_symbol      TEXT NOT NULL,
            expiration_date     DATE,
            strike              NUMERIC NOT NULL DEFAULT 0,
            option_type         TEXT NOT NULL,
            contracts_open      NUMERIC NOT NULL DEFAULT 0,
            avg_premium         NUMERIC NOT NULL DEFAULT 0,
            cost_basis          NUMERIC NOT NULL DEFAULT 0,
            contracts_bought    NUMERIC NOT NULL DEFAULT 0,
            contracts_sold      NUMERIC NOT NULL DEFAULT 0,
            total_buy_cost      NUMERIC NOT NULL DEFAULT 0,
            realized_pnl        NUMERIC,
            first_entry_date    DATE,
            last_entry_date     DATE,
            last_exit_date      DATE,
            final_status        TEXT,
            source              TEXT DEFAULT 'csv_import',
            import_batch_id     TEXT,
            source_file         TEXT,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    # Safe migrations — ADD COLUMN IF NOT EXISTS is idempotent
    cur.execute("ALTER TABLE portfolio_option_positions ADD COLUMN IF NOT EXISTS notes TEXT")

    # ── Option closed trades ───────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS portfolio_option_closed_trades (
            id                          TEXT PRIMARY KEY,
            occ_key                     TEXT NOT NULL,
            underlying                  TEXT NOT NULL,
            display_symbol              TEXT NOT NULL,
            expiration_date             DATE,
            strike                      NUMERIC NOT NULL DEFAULT 0,
            option_type                 TEXT NOT NULL,
            contracts_closed            NUMERIC NOT NULL DEFAULT 0,
            entry_date                  DATE,
            exit_date                   DATE,
            avg_entry_premium           NUMERIC NOT NULL DEFAULT 0,
            exit_premium                NUMERIC NOT NULL DEFAULT 0,
            cost_basis_sold             NUMERIC NOT NULL DEFAULT 0,
            proceeds                    NUMERIC NOT NULL DEFAULT 0,
            fees                        NUMERIC NOT NULL DEFAULT 0,
            realized_pnl                NUMERIC,
            realized_pnl_pct            NUMERIC,
            contracts_remaining_after   NUMERIC,
            is_full_close               BOOLEAN NOT NULL DEFAULT FALSE,
            close_type                  TEXT,
            final_option_status         TEXT,
            source                      TEXT DEFAULT 'csv_import',
            import_batch_id             TEXT,
            source_file                 TEXT,
            created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    cur.execute("ALTER TABLE portfolio_option_closed_trades ADD COLUMN IF NOT EXISTS notes TEXT")

    conn.commit()
    cur.close()


# ── Row deserializers ─────────────────────────────────────────────────────────

def _pos_row_to_dict(row: tuple, description) -> dict:
    cols = [d[0] for d in description]
    d    = dict(zip(cols, row))
    for k in ("expiration_date", "first_entry_date", "last_entry_date", "last_exit_date"):
        if isinstance(d.get(k), date):
            d[k] = d[k].isoformat()
    for k in ("created_at", "updated_at"):
        if isinstance(d.get(k), datetime):
            d[k] = d[k].isoformat()
    for k in ("contracts_open", "avg_premium", "cost_basis", "contracts_bought",
              "contracts_sold", "total_buy_cost", "realized_pnl", "strike"):
        if d.get(k) is not None:
            try:
                d[k] = float(d[k])
            except Exception:
                pass
    return d


def _ct_row_to_dict(row: tuple, description) -> dict:
    cols = [d[0] for d in description]
    d    = dict(zip(cols, row))
    for k in ("expiration_date", "entry_date", "exit_date"):
        if isinstance(d.get(k), date):
            d[k] = d[k].isoformat()
    for k in ("created_at",):
        if isinstance(d.get(k), datetime):
            d[k] = d[k].isoformat()
    for k in ("contracts_closed", "avg_entry_premium", "exit_premium", "cost_basis_sold",
              "proceeds", "fees", "realized_pnl", "realized_pnl_pct",
              "contracts_remaining_after", "strike"):
        if d.get(k) is not None:
            try:
                d[k] = float(d[k])
            except Exception:
                pass
    if "is_full_close" in d:
        d["is_full_close"] = bool(d["is_full_close"])
    return d


# ── Option positions ──────────────────────────────────────────────────────────

def load_option_positions() -> list[dict]:
    """Return all option positions ordered by underlying, expiration.

    Cached in memory for 45 s; invalidated on any write.
    """
    _now = _time_opt.time()
    if _opt_pos_cache["data"] is not None and _now - _opt_pos_cache["ts"] < _OPT_CACHE_TTL:
        return list(_opt_pos_cache["data"])

    conn = _get_conn()
    if not conn:
        return []
    try:
        _ensure_tables(conn)
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM portfolio_option_positions
            ORDER BY underlying, expiration_date, strike, option_type
        """)
        rows = cur.fetchall()
        desc = cur.description
        cur.close()
        result = [_pos_row_to_dict(r, desc) for r in rows]
        _opt_pos_cache["data"] = result
        _opt_pos_cache["ts"]   = _now
        return result
    except Exception as e:
        print(f"[OPT_STORE] load_option_positions error: {e}")
        return []
    finally:
        _put_conn(conn)


def save_option_positions_batch(positions: list[dict]) -> int:
    """Upsert option positions. Returns count of rows upserted."""
    _invalidate_opt_pos_cache()
    if not positions:
        return 0
    conn = _get_conn()
    if not conn:
        return 0
    try:
        _ensure_tables(conn)
        cur = conn.cursor()
        count = 0
        for pos in positions:
            pos_id = pos.get("id") or str(uuid.uuid4())
            cur.execute("""
                INSERT INTO portfolio_option_positions
                  (id, occ_key, underlying, display_symbol, expiration_date,
                   strike, option_type, contracts_open, avg_premium, cost_basis,
                   contracts_bought, contracts_sold, total_buy_cost, realized_pnl,
                   first_entry_date, last_entry_date, last_exit_date, final_status,
                   source, import_batch_id, source_file)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (occ_key) DO UPDATE SET
                  contracts_open    = EXCLUDED.contracts_open,
                  avg_premium       = EXCLUDED.avg_premium,
                  cost_basis        = EXCLUDED.cost_basis,
                  contracts_bought  = EXCLUDED.contracts_bought,
                  contracts_sold    = EXCLUDED.contracts_sold,
                  total_buy_cost    = EXCLUDED.total_buy_cost,
                  realized_pnl      = EXCLUDED.realized_pnl,
                  last_entry_date   = EXCLUDED.last_entry_date,
                  last_exit_date    = EXCLUDED.last_exit_date,
                  final_status      = EXCLUDED.final_status,
                  import_batch_id   = EXCLUDED.import_batch_id,
                  source_file       = EXCLUDED.source_file,
                  updated_at        = NOW()
            """, (
                pos_id,
                pos.get("occ_key", ""),
                pos.get("underlying", ""),
                pos.get("display_symbol", pos.get("occ_key", "")),
                pos.get("expiration_date") or None,
                pos.get("strike") or 0,
                pos.get("option_type", ""),
                pos.get("contracts_open") or 0,
                pos.get("avg_premium") or 0,
                pos.get("cost_basis") or 0,
                pos.get("contracts_bought") or 0,
                pos.get("contracts_sold") or 0,
                pos.get("total_buy_cost") or 0,
                pos.get("realized_pnl"),
                pos.get("first_entry_date") or None,
                pos.get("last_entry_date") or None,
                pos.get("last_exit_date") or None,
                pos.get("final_status") or "open",
                pos.get("source") or "csv_import",
                pos.get("import_batch_id") or None,
                pos.get("source_file") or None,
            ))
            count += 1
        conn.commit()
        cur.close()
        print(f"[OPT_STORE] upserted {count} option positions")
        return count
    except Exception as e:
        print(f"[OPT_STORE] save_option_positions_batch error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        _put_conn(conn)


def delete_csv_import_option_positions() -> int:
    """Delete option positions with source='csv_import'. Preserves manual records."""
    _invalidate_opt_pos_cache()
    conn = _get_conn()
    if not conn:
        return 0
    try:
        _ensure_tables(conn)
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM portfolio_option_positions
            WHERE source = 'csv_import' OR source IS NULL
            RETURNING id
        """)
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        print(f"[OPT_STORE] deleted {deleted} csv_import option positions")
        return deleted
    except Exception as e:
        print(f"[OPT_STORE] delete_csv_import_option_positions error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        _put_conn(conn)


# ── Option closed trades ──────────────────────────────────────────────────────

def load_option_closed_trades() -> list[dict]:
    """Return all option closed trades ordered by exit_date desc.

    Cached in memory for 45 s; invalidated on any write.
    """
    _now = _time_opt.time()
    if _opt_ct_cache["data"] is not None and _now - _opt_ct_cache["ts"] < _OPT_CACHE_TTL:
        return list(_opt_ct_cache["data"])

    conn = _get_conn()
    if not conn:
        return []
    try:
        _ensure_tables(conn)
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM portfolio_option_closed_trades
            ORDER BY exit_date DESC NULLS LAST, created_at DESC
        """)
        rows = cur.fetchall()
        desc = cur.description
        cur.close()
        result = [_ct_row_to_dict(r, desc) for r in rows]
        _opt_ct_cache["data"] = result
        _opt_ct_cache["ts"]   = _now
        return result
    except Exception as e:
        print(f"[OPT_STORE] load_option_closed_trades error: {e}")
        return []
    finally:
        _put_conn(conn)


def save_option_closed_trades_batch(trades: list[dict]) -> int:
    """Insert option closed trade records. Returns count inserted."""
    _invalidate_opt_ct_cache()
    if not trades:
        return 0
    conn = _get_conn()
    if not conn:
        return 0
    try:
        _ensure_tables(conn)
        cur = conn.cursor()
        count = 0
        for t in trades:
            trade_id = t.get("id") or str(uuid.uuid4())
            cur.execute("""
                INSERT INTO portfolio_option_closed_trades
                  (id, occ_key, underlying, display_symbol, expiration_date,
                   strike, option_type, contracts_closed, entry_date, exit_date,
                   avg_entry_premium, exit_premium, cost_basis_sold, proceeds, fees,
                   realized_pnl, realized_pnl_pct, contracts_remaining_after,
                   is_full_close, close_type, final_option_status,
                   source, import_batch_id, source_file)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO NOTHING
            """, (
                trade_id,
                t.get("occ_key", ""),
                t.get("underlying", ""),
                t.get("display_symbol", t.get("occ_key", "")),
                t.get("expiration_date") or None,
                t.get("strike") or 0,
                t.get("option_type", ""),
                t.get("contracts_closed") or 0,
                t.get("entry_date") or None,
                t.get("exit_date") or None,
                t.get("avg_entry_premium") or 0,
                t.get("exit_premium") or 0,
                t.get("cost_basis_sold") or 0,
                t.get("proceeds") or 0,
                t.get("fees") or 0,
                t.get("realized_pnl"),
                t.get("realized_pnl_pct"),
                t.get("contracts_remaining_after"),
                bool(t.get("is_full_close", False)),
                t.get("close_type") or None,
                t.get("final_option_status") or None,
                t.get("source") or "csv_import",
                t.get("import_batch_id") or None,
                t.get("source_file") or None,
            ))
            count += 1
        conn.commit()
        cur.close()
        print(f"[OPT_STORE] inserted {count} option closed trades")
        return count
    except Exception as e:
        print(f"[OPT_STORE] save_option_closed_trades_batch error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        _put_conn(conn)


def delete_csv_import_option_closed_trades() -> int:
    """Delete option closed trades with source='csv_import'. Preserves manual records."""
    _invalidate_opt_ct_cache()
    conn = _get_conn()
    if not conn:
        return 0
    try:
        _ensure_tables(conn)
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM portfolio_option_closed_trades
            WHERE source = 'csv_import' OR source IS NULL
            RETURNING id
        """)
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        print(f"[OPT_STORE] deleted {deleted} csv_import option closed trades")
        return deleted
    except Exception as e:
        print(f"[OPT_STORE] delete_csv_import_option_closed_trades error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        _put_conn(conn)


# ── Single-record helpers (edit / sell / full-close) ──────────────────────────

def load_option_position_by_occ_key(occ_key: str) -> dict | None:
    """Load a single option position row by occ_key. Returns None if not found."""
    conn = _get_conn()
    if not conn:
        return None
    try:
        _ensure_tables(conn)
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM portfolio_option_positions WHERE occ_key = %s",
            (occ_key,),
        )
        row  = cur.fetchone()
        desc = cur.description
        cur.close()
        return _pos_row_to_dict(row, desc) if row else None
    except Exception as e:
        print(f"[OPT_STORE] load_option_position_by_occ_key error: {e}")
        return None
    finally:
        _put_conn(conn)


_POS_UPDATABLE_COLS = frozenset({
    "contracts_open", "avg_premium", "cost_basis",
    "expiration_date", "strike", "option_type", "final_status",
    "first_entry_date", "last_entry_date", "last_exit_date",
    "contracts_sold", "realized_pnl", "notes",
})


def update_option_position(occ_key: str, updates: dict) -> dict | None:
    """Update a subset of allowed fields on a single option position row.

    Only keys present in _POS_UPDATABLE_COLS are applied.
    Returns the updated row dict, or None if the row was not found / error occurred.
    """
    _invalidate_opt_pos_cache()

    set_clauses: list[str] = []
    values:      list      = []
    for col, val in updates.items():
        if col in _POS_UPDATABLE_COLS:
            set_clauses.append(f"{col} = %s")
            values.append(val)

    if not set_clauses:
        return load_option_position_by_occ_key(occ_key)

    set_clauses.append("updated_at = NOW()")
    values.append(occ_key)
    sql = (
        f"UPDATE portfolio_option_positions "
        f"SET {', '.join(set_clauses)} "
        f"WHERE occ_key = %s RETURNING *"
    )

    conn = _get_conn()
    if not conn:
        return None
    try:
        _ensure_tables(conn)
        cur = conn.cursor()
        cur.execute(sql, values)
        row  = cur.fetchone()
        desc = cur.description
        conn.commit()
        cur.close()
        if row is None:
            return None
        result = _pos_row_to_dict(row, desc)
        print(f"[OPT_STORE] updated option position occ_key={occ_key} fields={list(updates.keys())}")
        return result
    except Exception as e:
        print(f"[OPT_STORE] update_option_position error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    finally:
        _put_conn(conn)


def delete_option_position(occ_key: str) -> bool:
    """Delete a single option position row by occ_key. Returns True if a row was deleted."""
    _invalidate_opt_pos_cache()
    conn = _get_conn()
    if not conn:
        return False
    try:
        _ensure_tables(conn)
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM portfolio_option_positions WHERE occ_key = %s",
            (occ_key,),
        )
        deleted = cur.rowcount > 0
        conn.commit()
        cur.close()
        if deleted:
            print(f"[OPT_STORE] deleted option position occ_key={occ_key}")
        return deleted
    except Exception as e:
        print(f"[OPT_STORE] delete_option_position error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def save_option_closed_trade(trade: dict) -> dict:
    """Insert a single option closed trade record.

    Returns the persisted row dict (with 'id' set), or the input dict with
    an '_error' key on failure.
    """
    _invalidate_opt_ct_cache()
    conn = _get_conn()
    if not conn:
        return {**trade, "_error": "no_db_connection"}
    try:
        _ensure_tables(conn)
        cur      = conn.cursor()
        trade_id = trade.get("id") or str(uuid.uuid4())
        cur.execute("""
            INSERT INTO portfolio_option_closed_trades
              (id, occ_key, underlying, display_symbol, expiration_date,
               strike, option_type, contracts_closed, entry_date, exit_date,
               avg_entry_premium, exit_premium, cost_basis_sold, proceeds, fees,
               realized_pnl, realized_pnl_pct, contracts_remaining_after,
               is_full_close, close_type, final_option_status,
               source, import_batch_id, source_file, notes)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING *
        """, (
            trade_id,
            trade.get("occ_key", ""),
            trade.get("underlying", ""),
            trade.get("display_symbol", trade.get("occ_key", "")),
            trade.get("expiration_date") or None,
            trade.get("strike") or 0,
            trade.get("option_type", ""),
            trade.get("contracts_closed") or 0,
            trade.get("entry_date") or None,
            trade.get("exit_date") or None,
            trade.get("avg_entry_premium") or 0,
            trade.get("exit_premium") or 0,
            trade.get("cost_basis_sold") or 0,
            trade.get("proceeds") or 0,
            trade.get("fees") or 0,
            trade.get("realized_pnl"),
            trade.get("realized_pnl_pct"),
            trade.get("contracts_remaining_after"),
            bool(trade.get("is_full_close", False)),
            trade.get("close_type") or trade.get("sell_type") or None,
            trade.get("final_option_status") or None,
            trade.get("source") or "dashboard_manual",
            trade.get("import_batch_id") or None,
            trade.get("source_file") or None,
            trade.get("notes") or trade.get("close_reason") or None,
        ))
        row  = cur.fetchone()
        desc = cur.description
        conn.commit()
        cur.close()
        result = _ct_row_to_dict(row, desc) if row else {**trade, "id": trade_id}
        print(
            f"[OPT_STORE] saved option closed trade id={trade_id} "
            f"occ_key={trade.get('occ_key')} is_full={trade.get('is_full_close')}"
        )
        return result
    except Exception as e:
        print(f"[OPT_STORE] save_option_closed_trade error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return {**trade, "_error": str(e)}
    finally:
        _put_conn(conn)
