"""Closed trades store — Neon DB persistence for portfolio closed positions.

Table: portfolio_closed_trades
  Each row is one completed trade with entry/exit metadata and realized P&L.

Public API
----------
  load_closed_trades()                  -> list[dict]
  save_closed_trade(trade)              -> dict          (with generated id)
  update_closed_trade(id, updates)      -> dict | None
  delete_closed_trade(id)               -> bool
"""
from __future__ import annotations

import json
import os
import uuid
from datetime import date, datetime, timezone
from typing import Any

_DB_URL = os.environ.get("NEON_DATABASE_URL") or os.environ.get("DATABASE_URL")
_db_pool = None


def _get_conn():
    global _db_pool
    if not _DB_URL:
        return None
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

    def _clean(url: str) -> str:
        try:
            p = urlparse(url)
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
                print(f"[CLOSED_TRADES_STORE] pool init failed: {e}")
                return None
        try:
            conn = _db_pool.getconn()
            cur = conn.cursor()
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


def _ensure_table(conn) -> None:
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS portfolio_closed_trades (
            id                  TEXT PRIMARY KEY,
            ticker              TEXT NOT NULL,
            shares              NUMERIC NOT NULL DEFAULT 0,
            entry_date          DATE,
            exit_date           DATE,
            entry_price         NUMERIC NOT NULL DEFAULT 0,
            exit_price          NUMERIC NOT NULL DEFAULT 0,
            realized_pnl        NUMERIC,
            realized_pnl_pct    NUMERIC,
            holding_period_days INTEGER,
            notes               TEXT,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()


def _row_to_dict(row: tuple, description) -> dict:
    cols = [d[0] for d in description]
    d = dict(zip(cols, row))
    for k in ("entry_date", "exit_date"):
        if isinstance(d.get(k), date):
            d[k] = d[k].isoformat()
    for k in ("created_at", "updated_at"):
        if isinstance(d.get(k), datetime):
            d[k] = d[k].isoformat()
    for k in ("shares", "entry_price", "exit_price", "realized_pnl", "realized_pnl_pct"):
        if d.get(k) is not None:
            try:
                d[k] = float(d[k])
            except Exception:
                pass
    return d


def _derive_fields(trade: dict) -> dict:
    """Auto-compute realized_pnl, realized_pnl_pct, holding_period_days if not supplied."""
    out = dict(trade)
    shares = float(out.get("shares") or 0)
    entry_price = float(out.get("entry_price") or 0)
    exit_price = float(out.get("exit_price") or 0)
    entry_date = out.get("entry_date")
    exit_date = out.get("exit_date")

    if out.get("realized_pnl") is None and shares and entry_price and exit_price:
        out["realized_pnl"] = round((exit_price - entry_price) * shares, 4)

    if out.get("realized_pnl_pct") is None and entry_price:
        pnl = float(out.get("realized_pnl") or 0)
        cost = shares * entry_price
        if cost:
            out["realized_pnl_pct"] = round(pnl / cost * 100, 4)

    if out.get("holding_period_days") is None and entry_date and exit_date:
        try:
            d1 = date.fromisoformat(str(entry_date))
            d2 = date.fromisoformat(str(exit_date))
            out["holding_period_days"] = (d2 - d1).days
        except Exception:
            pass

    return out


def load_closed_trades() -> list[dict]:
    """Return all closed trades ordered by exit_date desc."""
    conn = _get_conn()
    if not conn:
        return []
    try:
        _ensure_table(conn)
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM portfolio_closed_trades
            ORDER BY exit_date DESC NULLS LAST, created_at DESC
        """)
        rows = cur.fetchall()
        desc = cur.description
        cur.close()
        return [_row_to_dict(r, desc) for r in rows]
    except Exception as e:
        print(f"[CLOSED_TRADES_STORE] load error: {e}")
        return []
    finally:
        _put_conn(conn)


def save_closed_trade(trade: dict) -> dict:
    """Insert a new closed trade. Auto-generates id and derived fields."""
    trade = _derive_fields(trade)
    trade_id = trade.get("id") or str(uuid.uuid4())
    ticker = (trade.get("ticker") or "").upper().strip()
    conn = _get_conn()
    if not conn:
        return {**trade, "id": trade_id, "_error": "DB unavailable"}
    try:
        _ensure_table(conn)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO portfolio_closed_trades
              (id, ticker, shares, entry_date, exit_date, entry_price, exit_price,
               realized_pnl, realized_pnl_pct, holding_period_days, notes)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING *
        """, (
            trade_id,
            ticker,
            trade.get("shares") or 0,
            trade.get("entry_date") or None,
            trade.get("exit_date") or None,
            trade.get("entry_price") or 0,
            trade.get("exit_price") or 0,
            trade.get("realized_pnl"),
            trade.get("realized_pnl_pct"),
            trade.get("holding_period_days"),
            trade.get("notes"),
        ))
        row = cur.fetchone()
        desc = cur.description
        conn.commit()
        cur.close()
        print(f"[CLOSED_TRADES_STORE] saved id={trade_id} ticker={ticker}")
        return _row_to_dict(row, desc)
    except Exception as e:
        print(f"[CLOSED_TRADES_STORE] save error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return {**trade, "id": trade_id, "_error": str(e)}
    finally:
        _put_conn(conn)


def update_closed_trade(trade_id: str, updates: dict) -> dict | None:
    """Patch a closed trade. Auto-recomputes derived fields if price/shares change."""
    existing = None
    all_trades = load_closed_trades()
    for t in all_trades:
        if t.get("id") == trade_id:
            existing = t
            break
    if existing is None:
        return None

    merged = {**existing, **updates, "id": trade_id}
    merged = _derive_fields(merged)

    conn = _get_conn()
    if not conn:
        return merged
    try:
        _ensure_table(conn)
        cur = conn.cursor()
        cur.execute("""
            UPDATE portfolio_closed_trades SET
              ticker              = %s,
              shares              = %s,
              entry_date          = %s,
              exit_date           = %s,
              entry_price         = %s,
              exit_price          = %s,
              realized_pnl        = %s,
              realized_pnl_pct    = %s,
              holding_period_days = %s,
              notes               = %s,
              updated_at          = NOW()
            WHERE id = %s
            RETURNING *
        """, (
            (merged.get("ticker") or "").upper().strip(),
            merged.get("shares") or 0,
            merged.get("entry_date") or None,
            merged.get("exit_date") or None,
            merged.get("entry_price") or 0,
            merged.get("exit_price") or 0,
            merged.get("realized_pnl"),
            merged.get("realized_pnl_pct"),
            merged.get("holding_period_days"),
            merged.get("notes"),
            trade_id,
        ))
        row = cur.fetchone()
        desc = cur.description
        conn.commit()
        cur.close()
        if row is None:
            return None
        print(f"[CLOSED_TRADES_STORE] updated id={trade_id}")
        return _row_to_dict(row, desc)
    except Exception as e:
        print(f"[CLOSED_TRADES_STORE] update error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    finally:
        _put_conn(conn)


def delete_closed_trade(trade_id: str) -> bool:
    """Delete a closed trade by id. Returns True if deleted."""
    conn = _get_conn()
    if not conn:
        return False
    try:
        _ensure_table(conn)
        cur = conn.cursor()
        cur.execute("DELETE FROM portfolio_closed_trades WHERE id = %s", (trade_id,))
        deleted = cur.rowcount > 0
        conn.commit()
        cur.close()
        print(f"[CLOSED_TRADES_STORE] delete id={trade_id} deleted={deleted}")
        return deleted
    except Exception as e:
        print(f"[CLOSED_TRADES_STORE] delete error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)
