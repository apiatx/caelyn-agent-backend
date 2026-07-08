"""
Daily Net Premium snapshot store for Options Flow 1D/7D/30D history.

Table: public.options_net_premium_daily
  Unique per (entity_type, entity_id, snapshot_date).
  Intraday re-runs UPDATE today's row — prior dates are never modified.
  Retention: 90 calendar days (registered in data_retention_rules).

entity_type values:
  "stock"     — individual stock ticker (canonical scope only)
  "etf"       — ETF ticker (canonical scope only)
  "theme"     — theme-level aggregate
  "sub_theme" — sub-theme-level aggregate
  "sector"    — sector-level aggregate

Only tickers with premium_scope_id = "net_flow_single_expiry_7_60dte_v1"
and nf_snapshot_pending != True are eligible for stock/ETF snapshots.
Unusual-flow-only tickers are never persisted as Net Flow history.
Theme/sector aggregates use their existing canonical rollup net_premium.
"""

from __future__ import annotations

from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo

_ET = ZoneInfo("America/New_York")


# ── ET date helper ─────────────────────────────────────────────────────────────

def _et_today() -> date:
    return datetime.now(_ET).date()


# ── Bulk upsert ────────────────────────────────────────────────────────────────

def upsert_daily_snapshots(rows: list[dict]) -> int:
    """
    Bulk-upsert snapshot rows to options_net_premium_daily.

    Each row dict must contain:
      entity_type, entity_id, snapshot_date (date object),
      net_premium, call_premium, put_premium, premium_scope_id

    Rows are written in a single transaction.
    ON CONFLICT updates today's row — historical rows are never overwritten
    because snapshot_date only equals today when this is called.

    Returns count of rows attempted.
    """
    if not rows:
        return 0
    conn = None
    try:
        from data.pg_storage import _get_conn, _put_conn
        conn = _get_conn()
        cur = conn.cursor()
        for r in rows:
            cur.execute(
                """
                INSERT INTO public.options_net_premium_daily
                    (entity_type, entity_id, snapshot_date,
                     net_premium, call_premium, put_premium, premium_scope_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (entity_type, entity_id, snapshot_date)
                DO UPDATE SET
                    net_premium      = EXCLUDED.net_premium,
                    call_premium     = EXCLUDED.call_premium,
                    put_premium      = EXCLUDED.put_premium,
                    premium_scope_id = EXCLUDED.premium_scope_id,
                    updated_at       = NOW()
                """,
                (
                    r["entity_type"],
                    r["entity_id"],
                    r["snapshot_date"],
                    r.get("net_premium"),
                    r.get("call_premium"),
                    r.get("put_premium"),
                    r.get("premium_scope_id"),
                ),
            )
        conn.commit()
        cur.close()
        _put_conn(conn)
        print(
            f"[NET_PREMIUM_HISTORY] upserted {len(rows)} snapshot rows "
            f"for {_et_today()}"
        )
        return len(rows)
    except Exception as e:
        print(f"[NET_PREMIUM_HISTORY] upsert_daily_snapshots error: {e}")
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        try:
            from data.pg_storage import _put_conn
            if conn:
                _put_conn(conn)
        except Exception:
            pass
        return 0


# ── Bulk historical query ──────────────────────────────────────────────────────

def get_historical_snapshots_bulk(
    entities: list[tuple[str, str]],
    since_date: date,
) -> dict[tuple[str, str], list[dict]]:
    """
    Fetch all snapshot rows for the given (entity_type, entity_id) pairs
    with snapshot_date >= since_date, newest first.

    Returns {(entity_type, entity_id): [row_dict, ...]} — empty list when
    no history exists for a given entity.
    """
    if not entities:
        return {}
    conn = None
    try:
        from data.pg_storage import _get_conn, _put_conn
        conn = _get_conn()
        cur = conn.cursor()
        # Build IN clause for entity pairs
        placeholders = ",".join(["(%s, %s)"] * len(entities))
        params: list = []
        for et, ei in entities:
            params.extend([et, ei])
        params.append(str(since_date))
        cur.execute(
            f"""
            SELECT entity_type, entity_id, snapshot_date,
                   net_premium, call_premium, put_premium
            FROM public.options_net_premium_daily
            WHERE (entity_type, entity_id) IN ({placeholders})
              AND snapshot_date >= %s
            ORDER BY entity_type, entity_id, snapshot_date DESC
            """,
            params,
        )
        rows = cur.fetchall()
        cur.close()
        _put_conn(conn)

        result: dict[tuple[str, str], list[dict]] = {}
        for row in rows:
            key = (row[0], row[1])
            result.setdefault(key, []).append(
                {
                    "snapshot_date": row[2],
                    "net_premium":   float(row[3]) if row[3] is not None else None,
                    "call_premium":  float(row[4]) if row[4] is not None else None,
                    "put_premium":   float(row[5]) if row[5] is not None else None,
                }
            )
        return result
    except Exception as e:
        print(f"[NET_PREMIUM_HISTORY] get_historical_snapshots_bulk error: {e}")
        try:
            from data.pg_storage import _put_conn
            if conn:
                _put_conn(conn)
        except Exception:
            pass
        return {}


# ── Delta computation ──────────────────────────────────────────────────────────

def _find_historical_np(
    history: list[dict],
    today: date,
    target_days: int,
) -> float | None:
    """
    Find the closest available Net Premium value for the requested lookback.

    target_days == 1:
        Latest snapshot with snapshot_date strictly before today (previous
        market day — correctly skips weekends and holidays).
    target_days == 7 / 30:
        Latest snapshot with snapshot_date <= (today - target_days calendar days).
        This picks the nearest available trading day at or before the target
        without assuming every calendar day has a row.

    history must be sorted newest-first (as returned by get_historical_snapshots_bulk).
    """
    if not history:
        return None
    if target_days == 1:
        for row in history:
            if row["snapshot_date"] < today:
                return row["net_premium"]
        return None
    target_date = today - timedelta(days=target_days)
    for row in history:
        if row["snapshot_date"] <= target_date:
            return row["net_premium"]
    return None


def compute_trend_label(
    prev: float | None,
    curr: float | None,
) -> str | None:
    """
    Deterministic direction label for the Net Premium movement from prev → curr.

    Values:
      more_positive    — both sides positive, curr > prev
      less_positive    — both sides positive, curr < prev
      more_negative    — both sides negative, curr < prev (more negative)
      less_negative    — both sides negative, curr > prev (less negative)
      crossed_positive — prev negative, curr zero-or-positive
      crossed_negative — prev zero-or-positive, curr negative
      unchanged        — difference < $0.01 (rounding noise)
      null             — either value is null
    """
    if prev is None or curr is None:
        return None
    if abs(curr - prev) < 0.01:
        return "unchanged"
    if prev < 0 and curr >= 0:
        return "crossed_positive"
    if prev >= 0 and curr < 0:
        return "crossed_negative"
    if prev >= 0 and curr >= 0:
        return "more_positive" if curr > prev else "less_positive"
    return "more_negative" if curr < prev else "less_negative"


def compute_delta_fields(
    current_np: float | None,
    history: list[dict],
    today: date,
) -> dict:
    """
    Compute 1D/7D/30D delta, historical reference, and trend label fields
    for one entity.  Always returns all nine keys (some may be null).

    Intended to be spread into a ticker/theme/sector node:
        node.update(compute_delta_fields(node.get("net_premium"), hist, today))
    """
    np_1d  = _find_historical_np(history, today, 1)
    np_7d  = _find_historical_np(history, today, 7)
    np_30d = _find_historical_np(history, today, 30)

    def _delta(hist_val: float | None) -> float | None:
        if current_np is None or hist_val is None:
            return None
        return round(current_np - hist_val, 2)

    return {
        "net_premium_1d_ago":    round(np_1d,  2) if np_1d  is not None else None,
        "net_premium_7d_ago":    round(np_7d,  2) if np_7d  is not None else None,
        "net_premium_30d_ago":   round(np_30d, 2) if np_30d is not None else None,
        "net_premium_delta_1d":  _delta(np_1d),
        "net_premium_delta_7d":  _delta(np_7d),
        "net_premium_delta_30d": _delta(np_30d),
        "net_premium_trend_1d":  compute_trend_label(np_1d,  current_np),
        "net_premium_trend_7d":  compute_trend_label(np_7d,  current_np),
        "net_premium_trend_30d": compute_trend_label(np_30d, current_np),
    }


# ── Diagnostics ────────────────────────────────────────────────────────────────

def get_history_diagnostics() -> dict:
    """
    Lightweight snapshot-table statistics for the diagnostics block.
    Runs one aggregation query — non-fatal on error.
    """
    conn = None
    try:
        from data.pg_storage import _get_conn, _put_conn
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                COUNT(*)                                 AS total_rows,
                COUNT(DISTINCT (entity_type, entity_id)) AS total_entities,
                MIN(snapshot_date)                       AS oldest_date,
                MAX(snapshot_date)                       AS newest_date,
                COUNT(DISTINCT snapshot_date)            AS distinct_dates
            FROM public.options_net_premium_daily
            """
        )
        row = cur.fetchone()
        cur.close()
        _put_conn(conn)
        if row:
            today    = _et_today()
            newest   = row[3]   # date or None
            oldest   = row[2]   # date or None
            has_1d   = (newest is not None and (today - newest).days <= 4)
            has_7d   = (oldest is not None and (today - oldest).days >= 7)
            has_30d  = (oldest is not None and (today - oldest).days >= 30)
            return {
                "net_premium_history_rows":         int(row[0]),
                "net_premium_history_entities":     int(row[1]),
                "net_premium_snapshot_date":        str(newest) if newest else None,
                "net_premium_oldest_snapshot_date": str(oldest) if oldest else None,
                "net_premium_distinct_dates":       int(row[4]),
                "net_premium_delta_1d_available":   has_1d,
                "net_premium_delta_7d_available":   has_7d,
                "net_premium_delta_30d_available":  has_30d,
            }
    except Exception as e:
        print(f"[NET_PREMIUM_HISTORY] diagnostics error: {e}")
        try:
            from data.pg_storage import _put_conn
            if conn:
                _put_conn(conn)
        except Exception:
            pass
    return {
        "net_premium_history_rows":        0,
        "net_premium_history_entities":    0,
        "net_premium_snapshot_date":       None,
        "net_premium_oldest_snapshot_date": None,
        "net_premium_distinct_dates":      0,
        "net_premium_delta_1d_available":  False,
        "net_premium_delta_7d_available":  False,
        "net_premium_delta_30d_available": False,
    }
