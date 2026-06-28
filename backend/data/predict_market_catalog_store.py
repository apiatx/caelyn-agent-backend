"""
Prediction Market Catalog — Neon/Postgres persistence layer.

Table: public.prediction_market_catalog
Stores the full active market universe discovered during each catalog crawl.
Rows are upserted every scan cycle. Markets no longer returned by the crawl
are marked is_currently_discovered=false (stale) rather than deleted.

Used as a fallback candidate pool when the live crawl fails, and as the
source of catalog_rows_total / catalog_rows_current_active diagnostics.

Follows the same patterns as predict_odds_store.py:
  - Imports _get_conn / _put_conn / is_available from data.pg_storage
  - Never raises — all helpers catch, log, return 0 / [] / {} on failure
  - _DDL_APPLIED flag prevents redundant DDL
  - Uses public. schema prefix
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

log = logging.getLogger("predict_market_catalog_store")

try:
    from data.pg_storage import _get_conn, _put_conn, is_available  # type: ignore
except Exception:
    _get_conn = lambda: None   # type: ignore
    _put_conn = lambda c: None  # type: ignore
    def is_available() -> bool:  # type: ignore
        return False

_DDL_APPLIED = False
_TABLE = "public.prediction_market_catalog"


# ── DDL ───────────────────────────────────────────────────────────────────────

def _ddl_sql() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {_TABLE} (
        condition_id            TEXT        PRIMARY KEY,
        market_slug             TEXT,
        event_slug              TEXT,
        question                TEXT,
        description             TEXT,
        tags                    JSONB,
        active                  BOOLEAN     NOT NULL DEFAULT TRUE,
        closed                  BOOLEAN     NOT NULL DEFAULT FALSE,
        accepting_orders        BOOLEAN     NOT NULL DEFAULT TRUE,
        end_date                TEXT,
        volume_24h              NUMERIC(20,4),
        liquidity               NUMERIC(20,4),
        clob_token_ids          JSONB,
        outcome_prices          JSONB,
        raw_json                JSONB,
        discovered_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_seen_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        is_currently_discovered BOOLEAN     NOT NULL DEFAULT TRUE
    );
    CREATE INDEX IF NOT EXISTS idx_pm_catalog_discovered
        ON {_TABLE} (is_currently_discovered, active, closed);
    CREATE INDEX IF NOT EXISTS idx_pm_catalog_last_seen
        ON {_TABLE} (last_seen_at DESC);
    CREATE INDEX IF NOT EXISTS idx_pm_catalog_vol
        ON {_TABLE} (volume_24h DESC NULLS LAST)
        WHERE is_currently_discovered = TRUE;
    """


def ensure_catalog_table() -> bool:
    """Create table + indexes if they don't exist. Safe to call repeatedly."""
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
        log.info("[predict_market_catalog_store] DDL applied — %s ready", _TABLE)
        return True
    except Exception as exc:
        log.warning("[predict_market_catalog_store] DDL failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_json(val: Any):
    """Wrap a dict/list as psycopg2 Json, fall back to json.dumps string."""
    if val is None:
        return None
    try:
        from psycopg2.extras import Json
        return Json(val if isinstance(val, (dict, list)) else {"_value": str(val)})
    except Exception:
        return json.dumps(val, default=str)


def _raw_to_row(raw: dict) -> tuple:
    """
    Convert a raw Gamma market dict to a flat tuple for bulk upsert.
    Returns None if condition_id is missing.
    """
    cid = raw.get("conditionId") or raw.get("condition_id") or ""
    if not cid:
        return None  # type: ignore

    # Volume/liquidity — Gamma uses camelCase
    vol_24h = raw.get("volume24hr") or raw.get("volume_24h")
    liq     = raw.get("liquidityNum") or raw.get("liquidity") or raw.get("liquidityAmm")

    # Prices — outcomePrices is a list of strings ["0.42", "0.58"]
    outcome_prices = raw.get("outcomePrices")
    if isinstance(outcome_prices, str):
        try:
            outcome_prices = json.loads(outcome_prices)
        except Exception:
            outcome_prices = None

    # CLOB token ids — may be a JSON string
    clob_tokens = raw.get("clobTokenIds") or raw.get("clob_token_ids")
    if isinstance(clob_tokens, str):
        try:
            clob_tokens = json.loads(clob_tokens)
        except Exception:
            clob_tokens = None

    # Tags — may be list of strings or list of dicts
    tags = raw.get("tags") or []
    if tags and isinstance(tags[0], dict):
        tags = [t.get("label", "") for t in tags]

    # End date — try multiple fields
    end_date = raw.get("endDate") or raw.get("endDateIso") or raw.get("end_date")
    if end_date and hasattr(end_date, "isoformat"):
        end_date = end_date.isoformat()

    # Slim raw_json — store only the fields needed for _enrich_market()
    slim_raw = {
        "conditionId":        cid,
        "question":           raw.get("question", ""),
        "description":        (raw.get("description") or "")[:500],
        "outcomePrices":      outcome_prices,
        "volume24hr":         vol_24h,
        "volume1wk":          raw.get("volume1wk") or raw.get("volumeWk"),
        "volume1mo":          raw.get("volume1mo") or raw.get("volumeNum"),
        "liquidityNum":       liq,
        "spread":             raw.get("spread"),
        "bestBid":            raw.get("bestBid"),
        "bestAsk":            raw.get("bestAsk"),
        "lastTradePrice":     raw.get("lastTradePrice"),
        "competitive":        raw.get("competitive"),
        "negRisk":            raw.get("negRisk"),
        "oneDayPriceChange":  raw.get("oneDayPriceChange"),
        "oneHourPriceChange": raw.get("oneHourPriceChange"),
        "oneWeekPriceChange": raw.get("oneWeekPriceChange"),
        "endDate":            end_date,
        "closed":             raw.get("closed"),
        "acceptingOrders":    raw.get("acceptingOrders"),
        "clobTokenIds":       clob_tokens,
        "slug":               raw.get("slug") or raw.get("market_slug"),
        "tags":               tags,
        "event_slug":         raw.get("event_slug", ""),
    }

    return (
        cid,                                         # condition_id
        raw.get("slug") or raw.get("market_slug"),   # market_slug
        raw.get("event_slug", ""),                   # event_slug
        (raw.get("question") or "")[:1000],          # question
        (raw.get("description") or "")[:500],        # description
        _to_json(tags),                              # tags JSONB
        not bool(raw.get("closed")),                 # active
        bool(raw.get("closed")),                     # closed
        bool(raw.get("acceptingOrders") or raw.get("accepting_orders")),  # accepting_orders
        str(end_date) if end_date else None,         # end_date
        float(vol_24h) if vol_24h is not None else None,   # volume_24h
        float(liq) if liq is not None else None,           # liquidity
        _to_json(clob_tokens),                       # clob_token_ids JSONB
        _to_json(outcome_prices),                    # outcome_prices JSONB
        _to_json(slim_raw),                          # raw_json JSONB
    )


# ── Writes ────────────────────────────────────────────────────────────────────

# execute_values template: 15 data %s placeholders + NOW()/NOW()/NOW()/TRUE for
# discovered_at, updated_at, last_seen_at, is_currently_discovered.
# This generates batched multi-row VALUES clauses (~100x faster than executemany).
_UPSERT_EV_SQL = f"""
    INSERT INTO {_TABLE}
        (condition_id, market_slug, event_slug, question, description,
         tags, active, closed, accepting_orders, end_date,
         volume_24h, liquidity, clob_token_ids, outcome_prices, raw_json,
         discovered_at, updated_at, last_seen_at, is_currently_discovered)
    VALUES %s
    ON CONFLICT (condition_id) DO UPDATE SET
        market_slug             = EXCLUDED.market_slug,
        event_slug              = EXCLUDED.event_slug,
        question                = EXCLUDED.question,
        description             = EXCLUDED.description,
        tags                    = EXCLUDED.tags,
        active                  = EXCLUDED.active,
        closed                  = EXCLUDED.closed,
        accepting_orders        = EXCLUDED.accepting_orders,
        end_date                = EXCLUDED.end_date,
        volume_24h              = EXCLUDED.volume_24h,
        liquidity               = EXCLUDED.liquidity,
        clob_token_ids          = EXCLUDED.clob_token_ids,
        outcome_prices          = EXCLUDED.outcome_prices,
        raw_json                = EXCLUDED.raw_json,
        updated_at              = NOW(),
        last_seen_at            = NOW(),
        is_currently_discovered = TRUE
"""
# Row template: 15 %s for data fields; NOW()/TRUE resolved server-side for timestamps/flag.
_UPSERT_EV_TEMPLATE = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW(),NOW(),TRUE)"
_PAGE_SIZE = 500


def upsert_catalog_rows(raw_markets: list[dict]) -> int:
    """
    Bulk-upsert raw Gamma market dicts into the catalog table.
    Uses psycopg2 execute_values() with batched multi-row VALUES clauses
    (~100x faster than executemany for large crawls).
    Returns number of rows successfully upserted (0 on failure).
    """
    if not raw_markets:
        return 0
    ensure_catalog_table()
    conn = _get_conn()
    if conn is None:
        return 0

    # Build rows and deduplicate by condition_id (index 0).
    # The same market can appear in multiple Gamma events; execute_values sends
    # all rows in one batched VALUES clause and PostgreSQL will throw
    # "ON CONFLICT DO UPDATE command cannot affect row a second time" if the
    # same condition_id appears twice in one batch.
    seen_cids: set[str] = set()
    rows: list[tuple] = []
    for raw in raw_markets:
        try:
            row = _raw_to_row(raw)
            if row is None:
                continue
            cid = row[0]
            if cid in seen_cids:
                continue
            seen_cids.add(cid)
            rows.append(row)
        except Exception as exc:
            log.debug("[predict_market_catalog_store] row build error: %s", exc)
            continue

    if not rows:
        return 0

    count = 0
    try:
        from psycopg2.extras import execute_values
        cur = conn.cursor()
        execute_values(
            cur,
            _UPSERT_EV_SQL,
            rows,
            template=_UPSERT_EV_TEMPLATE,
            page_size=_PAGE_SIZE,
        )
        count = len(rows)
        conn.commit()
        cur.close()
        log.info(
            "[predict_market_catalog_store] upserted %d/%d catalog rows",
            count, len(raw_markets),
        )
    except Exception as exc:
        log.warning("[predict_market_catalog_store] upsert_catalog_rows error: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        count = 0
    finally:
        _put_conn(conn)
    return count


def mark_stale_before(crawl_started_at: datetime) -> int:
    """
    Mark rows as is_currently_discovered=false if last_seen_at < crawl_started_at.
    Call after upsert_catalog_rows() to retire markets no longer in the active crawl.
    Returns rows marked stale (0 on failure).
    """
    ensure_catalog_table()
    conn = _get_conn()
    if conn is None:
        return 0
    try:
        # Give a 60s grace period so clock skew / slow upserts don't over-expire
        cutoff = crawl_started_at - timedelta(seconds=60)
        cur = conn.cursor()
        cur.execute(
            f"""
            UPDATE {_TABLE}
            SET is_currently_discovered = FALSE
            WHERE is_currently_discovered = TRUE
              AND last_seen_at < %s
            """,
            (cutoff,),
        )
        stale_count = cur.rowcount
        conn.commit()
        cur.close()
        if stale_count:
            log.info(
                "[predict_market_catalog_store] marked %d rows stale (last_seen < %s)",
                stale_count, cutoff.isoformat(),
            )
        return stale_count
    except Exception as exc:
        log.warning("[predict_market_catalog_store] mark_stale_before error: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        _put_conn(conn)


# ── Reads ─────────────────────────────────────────────────────────────────────

def get_active_catalog_rows() -> list[dict]:
    """
    Return all currently-discovered, active, non-closed catalog rows.
    Each row dict has: condition_id, question, description, tags, active, closed,
    accepting_orders, end_date, volume_24h, liquidity, clob_token_ids,
    outcome_prices, raw_json, market_slug, event_slug.

    Used as fallback candidate pool when live catalog crawl fails.
    Returns [] on failure or empty catalog.
    """
    ensure_catalog_table()
    conn = _get_conn()
    if conn is None:
        return []
    try:
        from psycopg2.extras import RealDictCursor
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            f"""
            SELECT condition_id, market_slug, event_slug,
                   question, description, tags,
                   active, closed, accepting_orders, end_date,
                   volume_24h, liquidity, clob_token_ids, outcome_prices, raw_json
            FROM {_TABLE}
            WHERE is_currently_discovered = TRUE
              AND active = TRUE
              AND closed = FALSE
            ORDER BY volume_24h DESC NULLS LAST
            LIMIT 20000
            """,
        )
        rows = cur.fetchall()
        cur.close()
        result = []
        for row in rows:
            rec = dict(row)
            for f in ("volume_24h", "liquidity"):
                if rec.get(f) is not None:
                    rec[f] = float(rec[f])
            result.append(rec)
        log.info(
            "[predict_market_catalog_store] loaded %d active catalog rows from Neon",
            len(result),
        )
        return result
    except Exception as exc:
        log.warning("[predict_market_catalog_store] get_active_catalog_rows error: %s", exc)
        return []
    finally:
        _put_conn(conn)


def get_catalog_diagnostics() -> dict:
    """Return catalog table row counts for diagnostics endpoint."""
    ensure_catalog_table()
    conn = _get_conn()
    if conn is None:
        return {"catalog_available": False}
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN is_currently_discovered THEN 1 ELSE 0 END) AS active_rows,
                MAX(last_seen_at) AS last_seen
            FROM {_TABLE}
            """,
        )
        row = cur.fetchone()
        cur.close()
        total    = int(row[0]) if row and row[0] else 0
        active   = int(row[1]) if row and row[1] else 0
        last_at  = row[2].isoformat() if row and row[2] else None
        return {
            "catalog_available":           True,
            "catalog_rows_total":          total,
            "catalog_rows_current_active": active,
            "catalog_last_seen_at":        last_at,
        }
    except Exception as exc:
        log.warning("[predict_market_catalog_store] get_catalog_diagnostics error: %s", exc)
        return {"catalog_available": False, "error": str(exc)}
    finally:
        _put_conn(conn)
