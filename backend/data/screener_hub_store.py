"""
Screener Hub — Neon storage helpers.

Thin layer over data/pg_storage._get_conn() that provides typed CRUD access to
the four Screener Hub tables created by backend/migrations/0001_screener_hub.sql:

    screener_fundamentals_cache
    screener_universe_snapshots
    screener_quote_cache
    screener_job_runs

Design rules (mirrors guardrails in CLAUDE.md):
- Never raises — every helper catches and logs on its own. Reads return empty
  collections on failure; writes return False/None on failure.
- Never blanks valid cached data because of a single failed enrichment row.
- Always tries to auto-create the tables on first call, so the service module
  works even if the migration has not been applied yet (Replit-friendly).
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

try:
    from data.pg_storage import _get_conn, _put_conn, is_available  # type: ignore
except Exception:  # pragma: no cover — import-time safety
    _get_conn = lambda: None  # type: ignore
    _put_conn = lambda c: None  # type: ignore
    def is_available() -> bool:  # type: ignore
        return False


# Track whether we've run the inline DDL on this process, so repeated reads
# don't re-issue CREATE TABLE statements.
_DDL_APPLIED = False


def _ddl_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS public.screener_fundamentals_cache (
        symbol         TEXT PRIMARY KEY,
        provider       TEXT NOT NULL DEFAULT 'fmp',
        profile_json   JSONB NOT NULL DEFAULT '{}'::jsonb,
        metrics_json   JSONB NOT NULL DEFAULT '{}'::jsonb,
        ratios_json    JSONB NOT NULL DEFAULT '{}'::jsonb,
        market_cap     NUMERIC(24, 2) NULL,
        sector         TEXT NULL,
        industry       TEXT NULL,
        country        TEXT NULL,
        exchange       TEXT NULL,
        fetched_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at     TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '7 days')
    );
    CREATE INDEX IF NOT EXISTS idx_screener_fund_sector
        ON public.screener_fundamentals_cache (sector);
    CREATE INDEX IF NOT EXISTS idx_screener_fund_industry
        ON public.screener_fundamentals_cache (industry);
    CREATE INDEX IF NOT EXISTS idx_screener_fund_expires
        ON public.screener_fundamentals_cache (expires_at);

    CREATE TABLE IF NOT EXISTS public.screener_universe_snapshots (
        id             BIGSERIAL PRIMARY KEY,
        universe_type  TEXT NOT NULL,
        theme_key      TEXT NULL,
        source         TEXT NOT NULL DEFAULT 'auto',
        symbols_json   JSONB NOT NULL DEFAULT '[]'::jsonb,
        generated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at     TIMESTAMPTZ NULL,
        status         TEXT NOT NULL DEFAULT 'ok'
    );
    CREATE INDEX IF NOT EXISTS idx_screener_universe_type_theme
        ON public.screener_universe_snapshots (universe_type, theme_key, generated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_screener_universe_generated
        ON public.screener_universe_snapshots (generated_at DESC);

    CREATE TABLE IF NOT EXISTS public.screener_quote_cache (
        symbol            TEXT PRIMARY KEY,
        quote_json        JSONB NOT NULL DEFAULT '{}'::jsonb,
        price             NUMERIC(20, 6) NULL,
        change_percent_1d NUMERIC(12, 4) NULL,
        fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        provider          TEXT NOT NULL DEFAULT 'tradier'
    );
    CREATE INDEX IF NOT EXISTS idx_screener_quote_fetched
        ON public.screener_quote_cache (fetched_at DESC);

    CREATE TABLE IF NOT EXISTS public.screener_job_runs (
        id                 BIGSERIAL PRIMARY KEY,
        job_name           TEXT NOT NULL,
        started_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        finished_at        TIMESTAMPTZ NULL,
        status             TEXT NOT NULL DEFAULT 'running',
        symbols_count      INTEGER NOT NULL DEFAULT 0,
        symbols_completed  INTEGER NOT NULL DEFAULT 0,
        symbols_failed     INTEGER NOT NULL DEFAULT 0,
        api_calls_used     INTEGER NOT NULL DEFAULT 0,
        error              TEXT NULL,
        metadata_json      JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    CREATE INDEX IF NOT EXISTS idx_screener_job_runs_name_started
        ON public.screener_job_runs (job_name, started_at DESC);
    CREATE INDEX IF NOT EXISTS idx_screener_job_runs_started
        ON public.screener_job_runs (started_at DESC);
    """


def ensure_tables() -> bool:
    """Idempotently create the Screener Hub tables. Safe to call repeatedly."""
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
        return True
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] ensure_tables error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def _jsonb(value: Any) -> str:
    """Serialize a value for a ::jsonb cast."""
    try:
        return json.dumps(value if value is not None else {}, default=str)
    except Exception:
        return "{}"


# ── screener_fundamentals_cache ───────────────────────────────────────────────

def upsert_fundamentals(
    symbol: str,
    *,
    profile: dict,
    metrics: dict,
    ratios: dict,
    market_cap: Optional[float],
    sector: Optional[str],
    industry: Optional[str],
    country: Optional[str],
    exchange: Optional[str],
    provider: str = "fmp",
    ttl_days: int = 7,
) -> bool:
    """Upsert a single ticker's fundamentals snapshot. Returns True on success."""
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.screener_fundamentals_cache
                (symbol, provider, profile_json, metrics_json, ratios_json,
                 market_cap, sector, industry, country, exchange,
                 fetched_at, expires_at)
            VALUES (%s, %s, %s::jsonb, %s::jsonb, %s::jsonb,
                    %s, %s, %s, %s, %s,
                    NOW(), NOW() + (%s || ' days')::interval)
            ON CONFLICT (symbol) DO UPDATE SET
                provider     = EXCLUDED.provider,
                profile_json = EXCLUDED.profile_json,
                metrics_json = EXCLUDED.metrics_json,
                ratios_json  = EXCLUDED.ratios_json,
                market_cap   = EXCLUDED.market_cap,
                sector       = EXCLUDED.sector,
                industry     = EXCLUDED.industry,
                country      = EXCLUDED.country,
                exchange     = EXCLUDED.exchange,
                fetched_at   = EXCLUDED.fetched_at,
                expires_at   = EXCLUDED.expires_at
            """,
            (
                symbol.upper(), provider,
                _jsonb(profile), _jsonb(metrics), _jsonb(ratios),
                market_cap, sector, industry, country, exchange,
                str(int(ttl_days)),
            ),
        )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] upsert_fundamentals {symbol} error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def get_fundamentals(symbols: Iterable[str]) -> dict[str, dict]:
    """Bulk fetch fundamentals rows. Returns {symbol: row_dict} (no expiry filter)."""
    ensure_tables()
    syms = sorted({s.upper() for s in symbols if s})
    if not syms:
        return {}
    conn = _get_conn()
    if conn is None:
        return {}
    out: dict[str, dict] = {}
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT symbol, provider, profile_json, metrics_json, ratios_json,
                   market_cap, sector, industry, country, exchange,
                   fetched_at, expires_at
            FROM public.screener_fundamentals_cache
            WHERE symbol = ANY(%s)
            """,
            (syms,),
        )
        for row in cur.fetchall():
            (sym, provider, profile, metrics, ratios,
             mcap, sector, industry, country, exchange,
             fetched_at, expires_at) = row
            out[sym] = {
                "symbol": sym,
                "provider": provider,
                "profile": profile if isinstance(profile, dict) else (json.loads(profile) if profile else {}),
                "metrics": metrics if isinstance(metrics, dict) else (json.loads(metrics) if metrics else {}),
                "ratios":  ratios  if isinstance(ratios, dict)  else (json.loads(ratios)  if ratios  else {}),
                "market_cap": float(mcap) if mcap is not None else None,
                "sector":   sector,
                "industry": industry,
                "country":  country,
                "exchange": exchange,
                "fetched_at": fetched_at.isoformat() if fetched_at else None,
                "expires_at": expires_at.isoformat() if expires_at else None,
            }
        cur.close()
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] get_fundamentals error: {e}")
    finally:
        _put_conn(conn)
    return out


def fundamentals_fresh_symbols(symbols: Iterable[str], max_age_days: int = 7) -> set[str]:
    """Return the subset of symbols whose fundamentals rows are still fresh (< max_age)."""
    ensure_tables()
    syms = sorted({s.upper() for s in symbols if s})
    if not syms:
        return set()
    conn = _get_conn()
    if conn is None:
        return set()
    fresh: set[str] = set()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT symbol
            FROM public.screener_fundamentals_cache
            WHERE symbol = ANY(%s)
              AND fetched_at > NOW() - (%s || ' days')::interval
            """,
            (syms, str(int(max_age_days))),
        )
        fresh = {r[0] for r in cur.fetchall()}
        cur.close()
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] fundamentals_fresh_symbols error: {e}")
    finally:
        _put_conn(conn)
    return fresh


def fundamentals_table_stats() -> dict:
    """Diagnostic counts: total rows, fresh-7d rows, latest fetch."""
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return {"available": False}
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*), MAX(fetched_at) FROM public.screener_fundamentals_cache")
        r = cur.fetchone()
        total = int(r[0] or 0)
        latest = r[1].isoformat() if r and r[1] else None
        cur.execute(
            "SELECT COUNT(*) FROM public.screener_fundamentals_cache "
            "WHERE fetched_at > NOW() - INTERVAL '7 days'"
        )
        fresh = int((cur.fetchone() or [0])[0] or 0)
        cur.close()
        return {
            "available": True,
            "total_rows": total,
            "fresh_7d_rows": fresh,
            "latest_fetched_at": latest,
        }
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] fundamentals_table_stats error: {e}")
        return {"available": False, "error": str(e)}
    finally:
        _put_conn(conn)


# ── screener_universe_snapshots ───────────────────────────────────────────────

def insert_universe_snapshot(
    *,
    universe_type: str,
    theme_key: Optional[str],
    symbols: list,
    source: str = "auto",
    status: str = "ok",
    ttl_days: Optional[int] = None,
) -> bool:
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        if ttl_days is not None:
            cur.execute(
                """
                INSERT INTO public.screener_universe_snapshots
                    (universe_type, theme_key, source, symbols_json,
                     generated_at, expires_at, status)
                VALUES (%s, %s, %s, %s::jsonb,
                        NOW(), NOW() + (%s || ' days')::interval, %s)
                """,
                (universe_type, theme_key, source, _jsonb(symbols),
                 str(int(ttl_days)), status),
            )
        else:
            cur.execute(
                """
                INSERT INTO public.screener_universe_snapshots
                    (universe_type, theme_key, source, symbols_json,
                     generated_at, status)
                VALUES (%s, %s, %s, %s::jsonb, NOW(), %s)
                """,
                (universe_type, theme_key, source, _jsonb(symbols), status),
            )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] insert_universe_snapshot {universe_type}/{theme_key} error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def get_latest_universe(
    universe_type: str,
    theme_key: Optional[str] = None,
) -> Optional[dict]:
    """Latest snapshot for (universe_type, theme_key). Returns dict or None."""
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        if theme_key is None:
            cur.execute(
                """
                SELECT id, universe_type, theme_key, source, symbols_json,
                       generated_at, expires_at, status
                FROM public.screener_universe_snapshots
                WHERE universe_type = %s AND theme_key IS NULL
                ORDER BY generated_at DESC
                LIMIT 1
                """,
                (universe_type,),
            )
        else:
            cur.execute(
                """
                SELECT id, universe_type, theme_key, source, symbols_json,
                       generated_at, expires_at, status
                FROM public.screener_universe_snapshots
                WHERE universe_type = %s AND theme_key = %s
                ORDER BY generated_at DESC
                LIMIT 1
                """,
                (universe_type, theme_key),
            )
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        (rid, utype, tkey, source, symbols, gen, expires, status) = row
        return {
            "id": rid,
            "universe_type": utype,
            "theme_key": tkey,
            "source": source,
            "symbols": symbols if isinstance(symbols, list) else (json.loads(symbols) if symbols else []),
            "generated_at": gen.isoformat() if gen else None,
            "expires_at":   expires.isoformat() if expires else None,
            "status": status,
        }
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] get_latest_universe {universe_type}/{theme_key} error: {e}")
        return None
    finally:
        _put_conn(conn)


def universe_table_stats() -> dict:
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return {"available": False}
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*), MAX(generated_at) FROM public.screener_universe_snapshots")
        r = cur.fetchone()
        total = int(r[0] or 0)
        latest = r[1].isoformat() if r and r[1] else None
        cur.execute(
            """
            SELECT universe_type, COUNT(*) AS n, MAX(generated_at) AS latest
            FROM public.screener_universe_snapshots
            GROUP BY universe_type
            ORDER BY universe_type
            """
        )
        per_type = [
            {"universe_type": r[0], "rows": int(r[1] or 0),
             "latest_generated_at": r[2].isoformat() if r[2] else None}
            for r in cur.fetchall()
        ]
        cur.close()
        return {
            "available": True,
            "total_rows": total,
            "latest_generated_at": latest,
            "per_type": per_type,
        }
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] universe_table_stats error: {e}")
        return {"available": False, "error": str(e)}
    finally:
        _put_conn(conn)


# ── screener_quote_cache ──────────────────────────────────────────────────────

def upsert_quote(
    symbol: str,
    *,
    quote: dict,
    price: Optional[float],
    change_percent_1d: Optional[float],
    provider: str = "tradier",
) -> bool:
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.screener_quote_cache
                (symbol, quote_json, price, change_percent_1d, fetched_at, provider)
            VALUES (%s, %s::jsonb, %s, %s, NOW(), %s)
            ON CONFLICT (symbol) DO UPDATE SET
                quote_json        = EXCLUDED.quote_json,
                price             = EXCLUDED.price,
                change_percent_1d = EXCLUDED.change_percent_1d,
                fetched_at        = EXCLUDED.fetched_at,
                provider          = EXCLUDED.provider
            """,
            (symbol.upper(), _jsonb(quote), price, change_percent_1d, provider),
        )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] upsert_quote {symbol} error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def get_quotes(symbols: Iterable[str]) -> dict[str, dict]:
    ensure_tables()
    syms = sorted({s.upper() for s in symbols if s})
    if not syms:
        return {}
    conn = _get_conn()
    if conn is None:
        return {}
    out: dict[str, dict] = {}
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT symbol, quote_json, price, change_percent_1d, fetched_at, provider
            FROM public.screener_quote_cache
            WHERE symbol = ANY(%s)
            """,
            (syms,),
        )
        for sym, qjson, price, chg, fetched, provider in cur.fetchall():
            out[sym] = {
                "symbol": sym,
                "quote": qjson if isinstance(qjson, dict) else (json.loads(qjson) if qjson else {}),
                "price": float(price) if price is not None else None,
                "change_percent_1d": float(chg) if chg is not None else None,
                "fetched_at": fetched.isoformat() if fetched else None,
                "provider": provider,
            }
        cur.close()
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] get_quotes error: {e}")
    finally:
        _put_conn(conn)
    return out


def quote_table_stats() -> dict:
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return {"available": False}
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*), MAX(fetched_at) FROM public.screener_quote_cache")
        r = cur.fetchone()
        total = int(r[0] or 0)
        latest = r[1].isoformat() if r and r[1] else None
        cur.execute(
            "SELECT COUNT(*) FROM public.screener_quote_cache "
            "WHERE fetched_at > NOW() - INTERVAL '15 minutes'"
        )
        recent = int((cur.fetchone() or [0])[0] or 0)
        cur.close()
        return {
            "available": True,
            "total_rows": total,
            "latest_fetched_at": latest,
            "fresh_15m_rows": recent,
        }
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] quote_table_stats error: {e}")
        return {"available": False, "error": str(e)}
    finally:
        _put_conn(conn)


# ── screener_job_runs ─────────────────────────────────────────────────────────

def start_job_run(job_name: str, *, symbols_count: int = 0,
                  metadata: Optional[dict] = None) -> Optional[int]:
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.screener_job_runs
                (job_name, started_at, status, symbols_count, metadata_json)
            VALUES (%s, NOW(), 'running', %s, %s::jsonb)
            RETURNING id
            """,
            (job_name, int(symbols_count), _jsonb(metadata or {})),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        return int(row[0]) if row else None
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] start_job_run {job_name} error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    finally:
        _put_conn(conn)


def finish_job_run(
    run_id: Optional[int],
    *,
    status: str,
    symbols_completed: int = 0,
    symbols_failed: int = 0,
    api_calls_used: int = 0,
    error: Optional[str] = None,
    metadata: Optional[dict] = None,
) -> bool:
    if run_id is None:
        return False
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE public.screener_job_runs
            SET finished_at       = NOW(),
                status            = %s,
                symbols_completed = %s,
                symbols_failed    = %s,
                api_calls_used    = %s,
                error             = %s,
                metadata_json     = COALESCE(metadata_json, '{}'::jsonb) || %s::jsonb
            WHERE id = %s
            """,
            (status, int(symbols_completed), int(symbols_failed),
             int(api_calls_used), error, _jsonb(metadata or {}), run_id),
        )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] finish_job_run {run_id} error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def latest_job_runs(limit: int = 20) -> list[dict]:
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return []
    out: list[dict] = []
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, job_name, started_at, finished_at, status,
                   symbols_count, symbols_completed, symbols_failed,
                   api_calls_used, error, metadata_json
            FROM public.screener_job_runs
            ORDER BY started_at DESC
            LIMIT %s
            """,
            (int(limit),),
        )
        for row in cur.fetchall():
            (rid, name, started, finished, status, n, comp, fail,
             calls, err, meta) = row
            out.append({
                "id": rid,
                "job_name": name,
                "started_at":  started.isoformat()  if started  else None,
                "finished_at": finished.isoformat() if finished else None,
                "status": status,
                "symbols_count":     int(n    or 0),
                "symbols_completed": int(comp or 0),
                "symbols_failed":    int(fail or 0),
                "api_calls_used":    int(calls or 0),
                "error": err,
                "metadata": meta if isinstance(meta, dict) else (json.loads(meta) if meta else {}),
            })
        cur.close()
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] latest_job_runs error: {e}")
    finally:
        _put_conn(conn)
    return out


def job_runs_stats() -> dict:
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return {"available": False}
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM public.screener_job_runs")
        total = int((cur.fetchone() or [0])[0] or 0)
        cur.close()
        return {"available": True, "total_rows": total}
    except Exception as e:
        return {"available": False, "error": str(e)}
    finally:
        _put_conn(conn)
