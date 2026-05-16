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
    for _col_sql in (
        "ALTER TABLE portfolio_closed_trades ADD COLUMN IF NOT EXISTS sell_type TEXT",
        "ALTER TABLE portfolio_closed_trades ADD COLUMN IF NOT EXISTS remaining_shares_after NUMERIC",
        "ALTER TABLE portfolio_closed_trades ADD COLUMN IF NOT EXISTS cost_method TEXT DEFAULT 'average_cost'",
        # Trade-group columns (allow grouping partial sells of the same trade lifecycle)
        "ALTER TABLE portfolio_closed_trades ADD COLUMN IF NOT EXISTS trade_group_id TEXT",
        "ALTER TABLE portfolio_closed_trades ADD COLUMN IF NOT EXISTS is_full_close BOOLEAN NOT NULL DEFAULT FALSE",
    ):
        try:
            cur.execute(_col_sql)
        except Exception:
            pass
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
    for k in ("shares", "entry_price", "exit_price", "realized_pnl", "realized_pnl_pct",
              "remaining_shares_after"):
        if d.get(k) is not None:
            try:
                d[k] = float(d[k])
            except Exception:
                pass
    # Booleans
    if "is_full_close" in d and d["is_full_close"] is not None:
        d["is_full_close"] = bool(d["is_full_close"])
    else:
        d.setdefault("is_full_close", False)
    # Frontend aliases — keep both so either naming convention works
    d["symbol"] = d.get("ticker")
    d["avg_entry_price"] = d.get("entry_price")
    return d


def _derive_fields(trade: dict, force_recompute: bool = False) -> dict:
    """Auto-compute realized_pnl, realized_pnl_pct, holding_period_days.

    When force_recompute=True (used on UPDATE), derived fields are always
    recalculated from the current prices/dates rather than preserved from the
    existing record.  On INSERT (force_recompute=False), fields are only
    computed when absent so that explicit user-supplied values are honoured.
    """
    out = dict(trade)
    shares = float(out.get("shares") or 0)
    entry_price = float(out.get("entry_price") or 0)
    exit_price = float(out.get("exit_price") or 0)
    entry_date = out.get("entry_date")
    exit_date = out.get("exit_date")

    if (force_recompute or out.get("realized_pnl") is None) and shares and entry_price and exit_price:
        out["realized_pnl"] = round((exit_price - entry_price) * shares, 4)

    if (force_recompute or out.get("realized_pnl_pct") is None) and entry_price:
        pnl = float(out.get("realized_pnl") or 0)
        cost = shares * entry_price
        if cost:
            out["realized_pnl_pct"] = round(pnl / cost * 100, 4)

    if (force_recompute or out.get("holding_period_days") is None) and entry_date and exit_date:
        try:
            # Strip time/timezone component if present (e.g. "2026-05-12T05:27:37.636Z")
            d1 = date.fromisoformat(str(entry_date).split("T")[0])
            d2 = date.fromisoformat(str(exit_date).split("T")[0])
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
               realized_pnl, realized_pnl_pct, holding_period_days, notes,
               sell_type, remaining_shares_after, cost_method,
               trade_group_id, is_full_close)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
            trade.get("sell_type") or None,
            trade.get("remaining_shares_after") if trade.get("remaining_shares_after") is not None else None,
            trade.get("cost_method") or "average_cost",
            trade.get("trade_group_id") or None,
            bool(trade.get("is_full_close", False)),
        ))
        row = cur.fetchone()
        desc = cur.description
        conn.commit()
        cur.close()
        print(f"[CLOSED_TRADES_STORE] saved id={trade_id} ticker={ticker} "
              f"group={trade.get('trade_group_id')} full={trade.get('is_full_close')}")
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

    # Normalise date fields — strip time/timezone component if present
    for _dk in ("entry_date", "exit_date"):
        if updates.get(_dk):
            _raw = str(updates[_dk]).strip()
            updates[_dk] = _raw.split("T")[0] if "T" in _raw else _raw

    merged = {**existing, **updates, "id": trade_id}
    merged = _derive_fields(merged, force_recompute=True)

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


def load_closed_trades_grouped(trades: list[dict] | None = None) -> list[dict]:
    """Return closed trades grouped by trade_group_id for the frontend card view.

    Groups all partial-sell events for the same trade lifecycle into a single
    object so the frontend can render one glasscard per trade (not one per sell).

    Shape of each group:
      {
        "trade_group_id": str | None,   # None for legacy ungrouped records
        "ticker": str,
        "entry_date": str,              # earliest across all sell events
        "final_exit_date": str | None,  # latest exit_date; null if not yet fully closed
        "total_shares_sold": float,
        "avg_entry_price": float,
        "avg_exit_price": float,
        "total_cost_basis": float,
        "total_exit_value": float,
        "total_realized_pnl": float,
        "total_realized_pnl_pct": float,
        "holding_period_days": int | None,
        "is_fully_closed": bool,        # True when a sell event has is_full_close=True
        "sell_events": [ ...individual sell rows... ],  # sorted by exit_date asc
        "current_price": float | None,  # enriched externally (not set here)
      }

    Trades without a trade_group_id are treated as their own single-event group
    (backward-compatible with existing records created before this feature).
    """
    if trades is None:
        trades = load_closed_trades()

    # ── Bucket by trade_group_id ───────────────────────────────────────────────
    from collections import defaultdict
    groups: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        gid = t.get("trade_group_id") or t.get("id")   # ungrouped → own bucket
        groups[gid].append(t)

    # ── Build summary per group, sorted newest-first by final_exit_date ────────
    result: list[dict] = []
    for gid, events in groups.items():
        # Sort events by exit_date ascending (earliest sell first)
        events_sorted = sorted(
            events,
            key=lambda x: x.get("exit_date") or "",
        )
        ticker = (events_sorted[0].get("ticker") or "").upper()

        total_shares = sum(e.get("shares") or 0 for e in events_sorted)
        total_cost   = sum((e.get("shares") or 0) * (e.get("entry_price") or 0)
                           for e in events_sorted)
        total_exit_v = sum((e.get("shares") or 0) * (e.get("exit_price") or 0)
                           for e in events_sorted)
        total_pnl    = sum(e.get("realized_pnl") or 0 for e in events_sorted)

        entry_dates  = [e.get("entry_date") for e in events_sorted if e.get("entry_date")]
        exit_dates   = [e.get("exit_date")  for e in events_sorted if e.get("exit_date")]
        entry_date   = min(entry_dates) if entry_dates else None
        final_exit   = max(exit_dates)  if exit_dates  else None

        is_fully_closed = any(bool(e.get("is_full_close")) for e in events_sorted)
        # Fallback: last event has remaining_shares_after == 0
        if not is_fully_closed and events_sorted:
            last = events_sorted[-1]
            rsaf = last.get("remaining_shares_after")
            if rsaf is not None and float(rsaf) <= 0:
                is_fully_closed = True

        avg_entry = round(total_cost / total_shares, 6) if total_shares else 0
        avg_exit  = round(total_exit_v / total_shares, 6) if total_shares else 0
        pnl_pct   = round(total_pnl / total_cost * 100, 4) if total_cost else None

        hpd: int | None = None
        if entry_date and final_exit:
            try:
                from datetime import date as _d
                hpd = (_d.fromisoformat(final_exit) - _d.fromisoformat(entry_date)).days
            except Exception:
                pass

        result.append({
            # ── Grouped / primary fields ───────────────────────────────────────
            "trade_group_id":         gid,
            "ticker":                 ticker,
            "entry_date":             entry_date,
            "final_exit_date":        final_exit if is_fully_closed else None,
            "total_shares_sold":      round(total_shares, 8),
            "avg_entry_price":        avg_entry,
            "avg_exit_price":         avg_exit,
            "total_cost_basis":       round(total_cost,   4),
            "total_exit_value":       round(total_exit_v, 4),
            "total_realized_pnl":     round(total_pnl,    4),
            "total_realized_pnl_pct": pnl_pct,
            "holding_period_days":    hpd,
            "is_fully_closed":        is_fully_closed,
            "sell_events":            events_sorted,
            "current_price":          None,   # enriched by the API endpoint
            # ── Backward-compat aliases (same names as flat closed_trades list) ─
            "realized_pnl":           round(total_pnl, 4),
            "realized_pnl_pct":       pnl_pct,
            "shares":                 round(total_shares, 8),
            "entry_price":            avg_entry,
            "exit_price":             avg_exit,
            "exit_date":              final_exit if is_fully_closed else (
                                          events_sorted[-1].get("exit_date")
                                          if events_sorted else None
                                      ),
            "is_full_close":          is_fully_closed,
            "symbol":                 ticker,          # alias used by some callers
            "avg_entry_price_compat": avg_entry,       # already present as avg_entry_price
        })

    # Sort: open partial trades first, then fully-closed newest-first
    def _last_exit(g: dict) -> str:
        return (
            g.get("final_exit_date")
            or (g["sell_events"][-1].get("exit_date") if g["sell_events"] else "")
            or ""
        )

    result.sort(key=lambda g: (1 if g["is_fully_closed"] else 0, _last_exit(g)))
    result.reverse()    # newest exit at the top within each bucket
    # Re-stable-sort so open partials always come before closed
    result.sort(key=lambda g: 1 if g["is_fully_closed"] else 0)
    return result


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
