"""
Watchlist Estimate History — append-only Neon persistence for analyst consensus estimates.

Schema: public.watchlist_estimate_history
  id              BIGSERIAL PRIMARY KEY
  symbol          TEXT NOT NULL
  metric          TEXT NOT NULL          -- 'revenue_annual', 'eps_annual', 'ebitda_annual'
  period_type     TEXT NOT NULL          -- 'annual'
  fiscal_period   TEXT NOT NULL          -- FMP date string, e.g. '2027-09-27'
  consensus_value DOUBLE PRECISION       -- revenueAvg / epsAvg / ebitdaAvg
  num_analysts    INT                    -- numAnalystsRevenue or numAnalystsEps
  observed_date   DATE NOT NULL          -- date of this observation (daily grain)
  source          TEXT NOT NULL          -- 'fmp_analyst_estimates'
  fmp_date        TEXT                   -- raw FMP date field (fiscal year end)
  UNIQUE (symbol, metric, fiscal_period, observed_date)

Retention: rows older than 548 days (18 months) are pruned during table maintenance.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

log = logging.getLogger(__name__)

_TABLE = "public.watchlist_estimate_history"

# Tolerance for 90-day lookback: accept a record within ±15 days of the target.
_REVISION_DAYS      = 90
_REVISION_TOLERANCE = 15
_RETAIN_DAYS        = 548   # 18 months


def ensure_table() -> bool:
    """Create the estimate history table if it does not exist. Safe to call repeatedly."""
    try:
        from data.pg_storage import _get_conn, _put_conn
        conn = _get_conn()
        if conn is None:
            return False
        try:
            cur = conn.cursor()
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {_TABLE} (
                    id             BIGSERIAL        PRIMARY KEY,
                    symbol         TEXT             NOT NULL,
                    metric         TEXT             NOT NULL,
                    period_type    TEXT             NOT NULL,
                    fiscal_period  TEXT             NOT NULL,
                    consensus_value DOUBLE PRECISION,
                    num_analysts   INT,
                    observed_date  DATE             NOT NULL,
                    source         TEXT             NOT NULL,
                    fmp_date       TEXT,
                    UNIQUE (symbol, metric, fiscal_period, observed_date)
                )
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_weh_symbol_metric
                ON {_TABLE} (symbol, metric, fiscal_period, observed_date)
            """)
            conn.commit()
            cur.close()
            return True
        finally:
            _put_conn(conn)
    except Exception as exc:
        log.warning("[EST_HIST] ensure_table error: %s", exc)
        return False


def upsert_estimate_observation(
    symbol: str,
    metric: str,
    period_type: str,
    fiscal_period: str,
    consensus_value: float | None,
    num_analysts: int | None,
    source: str,
    fmp_date: str | None = None,
    observed_date: date | None = None,
) -> bool:
    """
    Insert today's estimate observation. Ignores conflicts (prevents duplicates
    for the same symbol+metric+fiscal_period+observed_date).
    Returns True on success.
    """
    if not symbol or not metric or not fiscal_period:
        return False
    obs_date = observed_date or date.today()
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
                    (symbol, metric, period_type, fiscal_period,
                     consensus_value, num_analysts, observed_date, source, fmp_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, metric, fiscal_period, observed_date) DO NOTHING
                """,
                (
                    symbol.upper(),
                    metric,
                    period_type,
                    fiscal_period,
                    consensus_value,
                    num_analysts,
                    obs_date,
                    source,
                    fmp_date,
                ),
            )
            conn.commit()
            cur.close()
            return True
        finally:
            _put_conn(conn)
    except Exception as exc:
        log.warning("[EST_HIST] upsert_observation(%s/%s/%s) error: %s",
                    symbol, metric, fiscal_period, exc)
        return False


def get_revision_90d(
    symbol: str,
    metric: str,
    fiscal_period: str,
) -> dict[str, Any]:
    """
    Compute the ~90-day consensus revision for a given symbol/metric/fiscal_period.

    Returns dict with keys:
      revision_pct      float | None  — (current - prior) / |prior| * 100
      current_value     float | None  — most recent observation
      prior_value       float | None  — observation ~90 days ago
      prior_date        str | None    — actual date of prior record used
      days_gap          int | None    — actual days between current and prior
      reason            str | None    — 'history_building' if insufficient data

    Never treats missing data as 0% revision.
    """
    if not symbol or not metric or not fiscal_period:
        return {"revision_pct": None, "reason": "missing_inputs"}

    try:
        from data.pg_storage import _get_conn, _put_conn
        conn = _get_conn()
        if conn is None:
            return {"revision_pct": None, "reason": "db_unavailable"}
        try:
            cur = conn.cursor()
            # Latest observation for this symbol/metric/fiscal_period
            cur.execute(
                f"""
                SELECT consensus_value, observed_date
                FROM {_TABLE}
                WHERE symbol = %s AND metric = %s AND fiscal_period = %s
                ORDER BY observed_date DESC
                LIMIT 1
                """,
                (symbol.upper(), metric, fiscal_period),
            )
            latest = cur.fetchone()
            if not latest or latest[0] is None:
                cur.close()
                return {"revision_pct": None, "reason": "history_building"}

            current_val, current_date = float(latest[0]), latest[1]

            # Find record closest to 90 days ago within ±15-day tolerance
            target_date  = current_date - timedelta(days=_REVISION_DAYS)
            low_date     = target_date - timedelta(days=_REVISION_TOLERANCE)
            high_date    = target_date + timedelta(days=_REVISION_TOLERANCE)

            cur.execute(
                f"""
                SELECT consensus_value, observed_date,
                       ABS(observed_date - %s::date) AS gap_days
                FROM {_TABLE}
                WHERE symbol = %s
                  AND metric = %s
                  AND fiscal_period = %s
                  AND observed_date BETWEEN %s AND %s
                  AND observed_date < %s
                ORDER BY gap_days ASC
                LIMIT 1
                """,
                (
                    target_date.isoformat(),
                    symbol.upper(),
                    metric,
                    fiscal_period,
                    low_date,
                    high_date,
                    current_date,
                ),
            )
            prior = cur.fetchone()
            cur.close()

            if not prior or prior[0] is None:
                return {
                    "revision_pct": None,
                    "current_value": current_val,
                    "reason": "history_building",
                }

            prior_val, prior_date, gap_days = float(prior[0]), prior[1], int(prior[2])
            if abs(prior_val) < 1e-9:
                return {
                    "revision_pct": None,
                    "current_value": current_val,
                    "prior_value": prior_val,
                    "prior_date": prior_date.isoformat() if prior_date else None,
                    "reason": "prior_value_zero",
                }

            rev_pct = round((current_val - prior_val) / abs(prior_val) * 100, 4)
            return {
                "revision_pct":   rev_pct,
                "current_value":  current_val,
                "prior_value":    prior_val,
                "prior_date":     prior_date.isoformat() if prior_date else None,
                "days_gap":       gap_days,
                "reason":         None,
            }
        finally:
            _put_conn(conn)
    except Exception as exc:
        log.warning("[EST_HIST] get_revision_90d(%s/%s/%s) error: %s",
                    symbol, metric, fiscal_period, exc)
        return {"revision_pct": None, "reason": "error"}


def prune_old_observations() -> int:
    """Delete rows older than _RETAIN_DAYS. Returns count of deleted rows."""
    try:
        from data.pg_storage import _get_conn, _put_conn
        conn = _get_conn()
        if conn is None:
            return 0
        try:
            cutoff = date.today() - timedelta(days=_RETAIN_DAYS)
            cur = conn.cursor()
            cur.execute(
                f"DELETE FROM {_TABLE} WHERE observed_date < %s",
                (cutoff,),
            )
            deleted = cur.rowcount
            conn.commit()
            cur.close()
            return deleted
        finally:
            _put_conn(conn)
    except Exception as exc:
        log.warning("[EST_HIST] prune_old_observations error: %s", exc)
        return 0
