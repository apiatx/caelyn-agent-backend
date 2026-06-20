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
        metadata_json  JSONB NULL,
        generated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at     TIMESTAMPTZ NULL,
        status         TEXT NOT NULL DEFAULT 'ok'
    );
    CREATE INDEX IF NOT EXISTS idx_screener_universe_type_theme
        ON public.screener_universe_snapshots (universe_type, theme_key, generated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_screener_universe_generated
        ON public.screener_universe_snapshots (generated_at DESC);
    ALTER TABLE public.screener_universe_snapshots
        ADD COLUMN IF NOT EXISTS metadata_json JSONB NULL;

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

    CREATE TABLE IF NOT EXISTS public.chain_reaction_weekly_outputs (
        id              BIGSERIAL PRIMARY KEY,
        generated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        week_start      DATE NOT NULL,
        source_version  TEXT NOT NULL DEFAULT 'v1',
        status          TEXT NOT NULL DEFAULT 'ok',
        symbols_json    JSONB NOT NULL DEFAULT '[]'::jsonb,
        rows_json       JSONB NOT NULL DEFAULT '[]'::jsonb,
        metadata_json   JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    CREATE INDEX IF NOT EXISTS idx_cr_weekly_generated
        ON public.chain_reaction_weekly_outputs (generated_at DESC);

    CREATE TABLE IF NOT EXISTS public.screener_returns_cache (
        symbol      TEXT PRIMARY KEY,
        return_2w   NUMERIC(12,4) NULL,
        return_4w   NUMERIC(12,4) NULL,
        return_10w  NUMERIC(12,4) NULL,
        rs_accel    NUMERIC(12,4) NULL,
        bars_count  INTEGER NOT NULL DEFAULT 0,
        fetched_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at  TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '7 days')
    );
    CREATE INDEX IF NOT EXISTS idx_screener_returns_fetched
        ON public.screener_returns_cache (fetched_at DESC);
    CREATE INDEX IF NOT EXISTS idx_screener_returns_expires
        ON public.screener_returns_cache (expires_at);

    CREATE TABLE IF NOT EXISTS public.screener_options_oi_cache (
        symbol                 TEXT PRIMARY KEY,
        options_oi             BIGINT NULL,
        previous_options_oi    BIGINT NULL,
        options_oi_change      BIGINT NULL,
        options_oi_change_pct  NUMERIC(12,4) NULL,
        options_activity_score NUMERIC(8,4) NULL,
        total_volume           BIGINT NULL,
        updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        provider               TEXT NOT NULL DEFAULT 'tradier'
    );
    CREATE INDEX IF NOT EXISTS idx_screener_options_oi_updated
        ON public.screener_options_oi_cache (updated_at DESC);

    CREATE TABLE IF NOT EXISTS public.screener_saved_screens (
        id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id           TEXT NOT NULL DEFAULT 'default',
        name              TEXT NOT NULL,
        tab               TEXT NOT NULL,
        theme_key         TEXT NULL,
        theme_label       TEXT NULL,
        filters_json      JSONB NOT NULL DEFAULT '{}'::jsonb,
        query_params_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        metadata_json     JSONB NOT NULL DEFAULT '{}'::jsonb,
        row_count         INTEGER NOT NULL DEFAULT 0,
        created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_saved_screens_user_created
        ON public.screener_saved_screens (user_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_saved_screens_user_tab_created
        ON public.screener_saved_screens (user_id, tab, created_at DESC);

    CREATE TABLE IF NOT EXISTS public.screener_saved_screen_rows (
        id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        saved_screen_id        UUID NOT NULL
            REFERENCES public.screener_saved_screens(id) ON DELETE CASCADE,
        user_id                TEXT NOT NULL DEFAULT 'default',
        symbol                 TEXT NOT NULL,
        company_name           TEXT NULL,
        market_cap             NUMERIC(24,2) NULL,
        sector                 TEXT NULL,
        industry               TEXT NULL,
        beta                   NUMERIC(12,6) NULL,
        price_at_save          NUMERIC(20,6) NULL,
        change_percent_1d      NUMERIC(12,4) NULL,
        volume                 NUMERIC(20,2) NULL,
        dollar_volume          NUMERIC(20,2) NULL,
        volume_to_market_cap   NUMERIC(16,8) NULL,
        exchange               TEXT NULL,
        volume_surge           NUMERIC(12,4) NULL,
        accumulation           JSONB NULL,
        options_oi             NUMERIC(20,2) NULL,
        options_oi_change_pct  NUMERIC(12,4) NULL,
        options_activity_score NUMERIC(8,4) NULL,
        role                   TEXT NULL,
        score                  NUMERIC(12,4) NULL,
        hidden_gem_score       NUMERIC(12,4) NULL,
        row_json               JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_saved_screen_rows_screen_id
        ON public.screener_saved_screen_rows (saved_screen_id);
    CREATE INDEX IF NOT EXISTS idx_saved_screen_rows_user_symbol
        ON public.screener_saved_screen_rows (user_id, symbol);
    CREATE INDEX IF NOT EXISTS idx_saved_screen_rows_symbol_created
        ON public.screener_saved_screen_rows (symbol, created_at DESC);

    CREATE TABLE IF NOT EXISTS public.screener_query_cache (
        id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        cache_key              TEXT UNIQUE NOT NULL,
        tab                    TEXT NOT NULL,
        theme_key              TEXT NULL,
        filters_json           JSONB NOT NULL DEFAULT '{}'::jsonb,
        query_params_json      JSONB NOT NULL DEFAULT '{}'::jsonb,
        response_json          JSONB NOT NULL DEFAULT '{}'::jsonb,
        row_count              INTEGER NOT NULL DEFAULT 0,
        refresh_schema_version TEXT NULL,
        created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at             TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '7 days'),
        metadata_json          JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    CREATE INDEX IF NOT EXISTS idx_screener_query_cache_key
        ON public.screener_query_cache (cache_key);
    CREATE INDEX IF NOT EXISTS idx_screener_query_cache_tab_theme
        ON public.screener_query_cache (tab, theme_key, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_screener_query_cache_expires
        ON public.screener_query_cache (expires_at);
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


# ── Schema V2 upgrade: save_type / snapshot_date / expires_at ────────────────

_DDL_V2_APPLIED = False


def _ensure_saved_screens_v2() -> bool:
    """
    Idempotently add new columns and indexes to screener_saved_screens.

    Columns added (all safe to call repeatedly via IF NOT EXISTS):
      save_type     TEXT DEFAULT 'manual'  — 'manual' | 'daily_auto'
      snapshot_date DATE NULL              — calendar date of the snapshot
      expires_at    TIMESTAMPTZ NULL       — when daily_auto rows may be purged

    Also creates three new indexes and a partial unique index that prevents
    duplicate daily_auto screens per user/tab/theme_key/date.
    """
    global _DDL_V2_APPLIED
    if _DDL_V2_APPLIED:
        return True
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            ALTER TABLE public.screener_saved_screens
                ADD COLUMN IF NOT EXISTS save_type
                    TEXT NOT NULL DEFAULT 'manual',
                ADD COLUMN IF NOT EXISTS snapshot_date
                    DATE NULL,
                ADD COLUMN IF NOT EXISTS expires_at
                    TIMESTAMPTZ NULL;

            CREATE INDEX IF NOT EXISTS idx_saved_screens_user_savetype_date
                ON public.screener_saved_screens
                (user_id, save_type, snapshot_date DESC);

            CREATE INDEX IF NOT EXISTS idx_saved_screens_user_savetype_tab_theme
                ON public.screener_saved_screens
                (user_id, save_type, tab, theme_key, snapshot_date DESC);

            CREATE UNIQUE INDEX IF NOT EXISTS idx_saved_screens_daily_auto_unique
                ON public.screener_saved_screens
                (user_id, tab, COALESCE(theme_key, ''), snapshot_date)
                WHERE save_type = 'daily_auto' AND snapshot_date IS NOT NULL;
        """)
        conn.commit()
        cur.close()
        _DDL_V2_APPLIED = True
        return True
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] _ensure_saved_screens_v2 error: {e}")
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
    """Return the subset of symbols whose fundamentals rows are still fresh (< max_age).

    Used by Social Screener and other TTL-aware callers.  For portfolio fundamentals
    use fundamentals_cached_symbols() instead — portfolio rows are treated as fresh
    indefinitely (no auto-expiry; only force_refresh or new-symbol logic refetches).
    """
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


def fundamentals_cached_symbols(symbols: Iterable[str]) -> set[str]:
    """Return the subset of symbols that have a usable cached fundamentals row.

    Unlike fundamentals_fresh_symbols() this applies NO TTL filter — a row is
    considered valid as long as it exists and contains at least one piece of
    usable data (market_cap or a non-empty profile).  Used by the portfolio
    fundamentals endpoint so existing holdings are never auto-refetched after a
    TTL boundary; FMP is only called for genuinely new / uncached symbols or
    when the user explicitly triggers force_refresh.
    """
    ensure_tables()
    syms = sorted({s.upper() for s in symbols if s})
    if not syms:
        return set()
    conn = _get_conn()
    if conn is None:
        return set()
    cached: set[str] = set()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT symbol
            FROM public.screener_fundamentals_cache
            WHERE symbol = ANY(%s)
              AND (
                market_cap IS NOT NULL
                OR (profile_json IS NOT NULL AND profile_json::text <> '{}' AND profile_json::text <> 'null')
                OR (ratios_json  IS NOT NULL AND ratios_json::text  <> '{}' AND ratios_json::text  <> 'null')
              )
            """,
            (syms,),
        )
        cached = {r[0] for r in cur.fetchall()}
        cur.close()
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] fundamentals_cached_symbols error: {e}")
    finally:
        _put_conn(conn)
    return cached


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
    metadata: Optional[dict] = None,
) -> bool:
    """
    Insert a universe snapshot row.

    metadata: optional dict of discovery provenance (sources_by_symbol,
    etf_holdings_count, lkg_leaders_count, …). Stored in metadata_json so
    snapshot-hit reads can hydrate discovery_sources without a live rebuild.
    """
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        meta_jsonb = _jsonb(metadata) if metadata else None
        if ttl_days is not None:
            cur.execute(
                """
                INSERT INTO public.screener_universe_snapshots
                    (universe_type, theme_key, source, symbols_json, metadata_json,
                     generated_at, expires_at, status)
                VALUES (%s, %s, %s, %s::jsonb, %s::jsonb,
                        NOW(), NOW() + (%s || ' days')::interval, %s)
                """,
                (universe_type, theme_key, source, _jsonb(symbols), meta_jsonb,
                 str(int(ttl_days)), status),
            )
        else:
            cur.execute(
                """
                INSERT INTO public.screener_universe_snapshots
                    (universe_type, theme_key, source, symbols_json, metadata_json,
                     generated_at, status)
                VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, NOW(), %s)
                """,
                (universe_type, theme_key, source, _jsonb(symbols), meta_jsonb, status),
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
    """
    Latest snapshot for (universe_type, theme_key). Returns dict or None.

    Returned dict always includes a 'metadata' key (may be None for old rows
    that predate the metadata_json column).
    """
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
                       generated_at, expires_at, status, metadata_json
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
                       generated_at, expires_at, status, metadata_json
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
        (rid, utype, tkey, source, symbols, gen, expires, status, meta) = row
        meta_parsed: Optional[dict] = None
        if meta is not None:
            meta_parsed = meta if isinstance(meta, dict) else (json.loads(meta) if meta else None)
        return {
            "id": rid,
            "universe_type": utype,
            "theme_key": tkey,
            "source": source,
            "symbols": symbols if isinstance(symbols, list) else (json.loads(symbols) if symbols else []),
            "generated_at": gen.isoformat() if gen else None,
            "expires_at":   expires.isoformat() if expires else None,
            "status": status,
            "metadata": meta_parsed,
        }
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] get_latest_universe {universe_type}/{theme_key} error: {e}")
        return None
    finally:
        _put_conn(conn)


def get_theme_last_refresh_ts(theme_key: str):
    """Return the generated_at timestamp of the most recent snapshot for *theme_key*.

    Used by _theme_refresh_allowed() as a durable fallback after a backend
    restart empties the in-memory _THEME_REFRESH_LOG.  Returns a timezone-aware
    datetime or None when no snapshot exists.
    """
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT MAX(generated_at)
            FROM public.screener_universe_snapshots
            WHERE universe_type = 'thematic'
              AND theme_key = %s
            """,
            (theme_key,),
        )
        row = cur.fetchone()
        cur.close()
        return row[0] if row and row[0] else None
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] get_theme_last_refresh_ts error: {e}")
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


def get_all_thematic_screener_meta() -> dict[str, dict]:
    """
    Merge screener_meta_by_symbol from every stored thematic universe snapshot.

    Returns {symbol: scr_meta_dict} covering all themes in a single SQL query.
    Called by _load_global_screener_meta() in screener_hub_service, which adds a
    30-min in-memory TTL on top.  Pure DB read — no FMP calls.

    Symbol precedence: first snapshot (by theme_key alphabetically after
    DISTINCT ON) wins; subsequent themes do not overwrite existing entries.
    This is intentional — any enriched entry is as good as another.
    """
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT ON (theme_key)
                metadata_json
            FROM public.screener_universe_snapshots
            WHERE universe_type = 'thematic'
              AND theme_key IS NOT NULL
              AND status = 'ok'
              AND metadata_json IS NOT NULL
              AND metadata_json::jsonb ? 'screener_meta_by_symbol'
              AND metadata_json::jsonb->'screener_meta_by_symbol' IS DISTINCT FROM 'null'::jsonb
              AND metadata_json::jsonb->'screener_meta_by_symbol' IS DISTINCT FROM '{}'::jsonb
            ORDER BY theme_key, generated_at DESC
            """
        )
        rows = cur.fetchall()
        cur.close()
        merged: dict[str, dict] = {}
        for (meta_json,) in rows:
            try:
                if meta_json is None:
                    continue
                meta = meta_json if isinstance(meta_json, dict) else json.loads(meta_json)
                sym_map = meta.get("screener_meta_by_symbol") or {}
                for sym, sm in sym_map.items():
                    if sym and sym not in merged:
                        merged[sym] = sm
            except Exception:
                continue
        return merged
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] get_all_thematic_screener_meta error: {e}")
        return {}
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


# ── chain_reaction_weekly_outputs ─────────────────────────────────────────────

def insert_chain_reaction_weekly_output(
    *,
    week_start: str,
    symbols: list,
    rows: list,
    metadata: Optional[dict] = None,
    source_version: str = "v1",
    status: str = "ok",
) -> bool:
    """Insert a new weekly Chain Reaction output row. Returns True on success."""
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.chain_reaction_weekly_outputs
                (generated_at, week_start, source_version, status,
                 symbols_json, rows_json, metadata_json)
            VALUES (NOW(), %s::date, %s, %s,
                    %s::jsonb, %s::jsonb, %s::jsonb)
            """,
            (week_start, source_version, status,
             _jsonb(symbols), _jsonb(rows), _jsonb(metadata or {})),
        )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] insert_chain_reaction_weekly_output error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def get_latest_chain_reaction_weekly(max_age_days: int = 10) -> Optional[dict]:
    """
    Return the most recent chain_reaction_weekly_outputs row if it exists
    and is within max_age_days. Returns None otherwise.
    """
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, generated_at, week_start, source_version, status,
                   symbols_json, rows_json, metadata_json
            FROM public.chain_reaction_weekly_outputs
            WHERE status = 'ok'
              AND generated_at > NOW() - (%s || ' days')::interval
            ORDER BY generated_at DESC
            LIMIT 1
            """,
            (str(int(max_age_days)),),
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        (rid, gen_at, week_start, src_ver, status,
         symbols, rows, meta) = row
        return {
            "id":             rid,
            "generated_at":   gen_at.isoformat() if gen_at else None,
            "week_start":     str(week_start) if week_start else None,
            "source_version": src_ver,
            "status":         status,
            "symbols":        symbols if isinstance(symbols, list) else (json.loads(symbols) if symbols else []),
            "rows":           rows    if isinstance(rows, list)    else (json.loads(rows)    if rows    else []),
            "metadata":       meta    if isinstance(meta, dict)    else (json.loads(meta)    if meta    else {}),
        }
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] get_latest_chain_reaction_weekly error: {e}")
        return None
    finally:
        _put_conn(conn)


def chain_reaction_weekly_stats() -> dict:
    """Diagnostic: total rows, latest generated_at for chain_reaction_weekly_outputs."""
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return {"available": False}
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*), MAX(generated_at) FROM public.chain_reaction_weekly_outputs"
        )
        r = cur.fetchone()
        total = int(r[0] or 0)
        latest = r[1].isoformat() if r and r[1] else None
        cur.close()
        return {"available": True, "total_rows": total, "latest_generated_at": latest}
    except Exception as e:
        return {"available": False, "error": str(e)}
    finally:
        _put_conn(conn)


# ── screener_returns_cache ────────────────────────────────────────────────────

def upsert_returns(
    symbol: str,
    *,
    return_2w: Optional[float],
    return_4w: Optional[float],
    return_10w: Optional[float],
    rs_accel: Optional[float],
    bars_count: int = 0,
    ttl_days: int = 7,
) -> bool:
    """Upsert a symbol's computed trailing returns. Returns True on success."""
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.screener_returns_cache
                (symbol, return_2w, return_4w, return_10w, rs_accel,
                 bars_count, fetched_at, expires_at)
            VALUES (%s, %s, %s, %s, %s,
                    %s, NOW(), NOW() + (%s || ' days')::interval)
            ON CONFLICT (symbol) DO UPDATE SET
                return_2w  = EXCLUDED.return_2w,
                return_4w  = EXCLUDED.return_4w,
                return_10w = EXCLUDED.return_10w,
                rs_accel   = EXCLUDED.rs_accel,
                bars_count = EXCLUDED.bars_count,
                fetched_at = EXCLUDED.fetched_at,
                expires_at = EXCLUDED.expires_at
            """,
            (symbol.upper(), return_2w, return_4w, return_10w, rs_accel,
             int(bars_count), str(int(ttl_days))),
        )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] upsert_returns {symbol} error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def get_returns(symbols: Iterable[str]) -> dict[str, dict]:
    """
    Bulk fetch returns rows. Returns {symbol: {return_2w, return_4w, return_10w,
    rs_accel, bars_count, fetched_at}} with no expiry filter.
    """
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
            SELECT symbol, return_2w, return_4w, return_10w, rs_accel,
                   bars_count, fetched_at
            FROM public.screener_returns_cache
            WHERE symbol = ANY(%s)
            """,
            (syms,),
        )
        for row in cur.fetchall():
            (sym, r2, r4, r10, accel, bars, fetched) = row
            out[sym] = {
                "symbol":     sym,
                "return_2w":  float(r2)    if r2    is not None else None,
                "return_4w":  float(r4)    if r4    is not None else None,
                "return_10w": float(r10)   if r10   is not None else None,
                "rs_accel":   float(accel) if accel is not None else None,
                "bars_count": int(bars or 0),
                "fetched_at": fetched.isoformat() if fetched else None,
            }
        cur.close()
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] get_returns error: {e}")
    finally:
        _put_conn(conn)
    return out


def returns_fresh_symbols(symbols: Iterable[str], max_age_days: int = 7) -> set[str]:
    """Return subset of symbols whose returns rows are still fresh."""
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
            SELECT symbol FROM public.screener_returns_cache
            WHERE symbol = ANY(%s)
              AND fetched_at > NOW() - (%s || ' days')::interval
            """,
            (syms, str(int(max_age_days))),
        )
        fresh = {r[0] for r in cur.fetchall()}
        cur.close()
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] returns_fresh_symbols error: {e}")
    finally:
        _put_conn(conn)
    return fresh


# ── screener_options_oi_cache ─────────────────────────────────────────────────

def upsert_screener_options_oi(rows: list[dict]) -> int:
    """
    Upsert options OI summary records for screener symbols.

    Each row dict must have: {symbol, options_oi, previous_options_oi,
    options_oi_change, options_oi_change_pct, options_activity_score,
    total_volume, provider}.

    ON CONFLICT: overwrites all columns with the caller-supplied values.
    The caller is responsible for computing previous_options_oi from the
    old DB value before calling this function.

    Returns number of rows upserted (0 on failure).
    """
    if not rows:
        return 0
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return 0
    count = 0
    try:
        cur = conn.cursor()
        for row in rows:
            sym = (row.get("symbol") or "").upper()
            if not sym:
                continue
            cur.execute(
                """
                INSERT INTO public.screener_options_oi_cache
                    (symbol, options_oi, previous_options_oi, options_oi_change,
                     options_oi_change_pct, options_activity_score, total_volume,
                     updated_at, provider)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s)
                ON CONFLICT (symbol) DO UPDATE SET
                    options_oi             = EXCLUDED.options_oi,
                    previous_options_oi    = EXCLUDED.previous_options_oi,
                    options_oi_change      = EXCLUDED.options_oi_change,
                    options_oi_change_pct  = EXCLUDED.options_oi_change_pct,
                    options_activity_score = EXCLUDED.options_activity_score,
                    total_volume           = EXCLUDED.total_volume,
                    updated_at             = NOW(),
                    provider               = EXCLUDED.provider
                """,
                (
                    sym,
                    row.get("options_oi"),
                    row.get("previous_options_oi"),
                    row.get("options_oi_change"),
                    row.get("options_oi_change_pct"),
                    row.get("options_activity_score"),
                    row.get("total_volume"),
                    row.get("provider", "tradier"),
                ),
            )
            count += 1
        conn.commit()
        cur.close()
        return count
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] upsert_screener_options_oi error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        _put_conn(conn)


def get_screener_options_oi(symbols: list[str]) -> dict[str, dict]:
    """
    Return {symbol: {options_oi, previous_options_oi, options_oi_change,
    options_oi_change_pct, options_activity_score, total_volume,
    updated_at_ts, updated_at_iso, provider}} for the given symbols.

    updated_at_ts is a Unix float for TTL comparisons.
    Returns {} if DB is unavailable.
    """
    syms = sorted({s.upper() for s in symbols if s})
    if not syms:
        return {}
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return {}
    out: dict[str, dict] = {}
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT symbol, options_oi, previous_options_oi, options_oi_change,
                   options_oi_change_pct, options_activity_score, total_volume,
                   updated_at, provider
            FROM public.screener_options_oi_cache
            WHERE symbol = ANY(%s)
            """,
            (syms,),
        )
        for row in cur.fetchall():
            (sym, oi, prev_oi, oi_chg, oi_chg_pct, act_score,
             total_vol, updated_at, provider) = row
            out[sym] = {
                "symbol":                sym,
                "options_oi":            int(oi)           if oi           is not None else None,
                "previous_options_oi":   int(prev_oi)      if prev_oi      is not None else None,
                "options_oi_change":     int(oi_chg)       if oi_chg       is not None else None,
                "options_oi_change_pct": float(oi_chg_pct) if oi_chg_pct   is not None else None,
                "options_activity_score": float(act_score) if act_score     is not None else None,
                "total_volume":          int(total_vol)    if total_vol     is not None else None,
                "updated_at_ts":         updated_at.timestamp() if updated_at else 0.0,
                "updated_at_iso":        updated_at.isoformat() if updated_at else None,
                "provider":              provider or "tradier",
            }
        cur.close()
    except Exception as e:
        print(f"[SCREENER_HUB_STORE] get_screener_options_oi error: {e}")
    finally:
        _put_conn(conn)
    return out


# ── screener_saved_screens / screener_saved_screen_rows ──────────────────────

import uuid as _uuid


def _uuid4() -> str:
    return str(_uuid.uuid4())


def _safe_float(v) -> Optional[float]:
    """Safely coerce a value to float, returning None on failure."""
    if v is None:
        return None
    try:
        f = float(v)
        return None if (f != f) else f  # reject NaN
    except (TypeError, ValueError):
        return None


def create_saved_screen(
    user_id: str,
    name: str,
    tab: str,
    theme_key: Optional[str],
    theme_label: Optional[str],
    filters_json: dict,
    query_params_json: dict,
    metadata_json: dict,
    rows: list,
    save_type: str = "manual",
    snapshot_date=None,
    expires_at=None,
) -> Optional[dict]:
    """
    Persist a Screener Hub result snapshot.

    Normalises key fields into columns on screener_saved_screen_rows and
    preserves the full original row dict in row_json.  No provider calls.
    Returns the created screen header dict or None on failure.
    """
    ensure_tables()
    _ensure_saved_screens_v2()
    screen_id = _uuid4()
    row_count = len(rows)

    if not name or not name.strip():
        from datetime import datetime, timezone as _tz
        now = datetime.now(_tz.utc)
        label = (theme_label or tab.replace("_", " ").title()).strip()
        name = f"{label} \u2014 {now.strftime('%b %-d, %Y')}"

    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.screener_saved_screens
                (id, user_id, name, tab, theme_key, theme_label,
                 filters_json, query_params_json, metadata_json, row_count,
                 save_type, snapshot_date, expires_at)
            VALUES (%s, %s, %s, %s, %s, %s,
                    %s::jsonb, %s::jsonb, %s::jsonb, %s,
                    %s, %s, %s)
            RETURNING id, name, tab, theme_key, theme_label, row_count,
                      created_at, updated_at, save_type, snapshot_date, expires_at
            """,
            (
                screen_id, user_id, name.strip(), tab,
                theme_key or None, theme_label or None,
                json.dumps(filters_json or {}),
                json.dumps(query_params_json or {}),
                json.dumps(metadata_json or {}),
                row_count,
                save_type,
                snapshot_date,
                expires_at,
            ),
        )
        rec = cur.fetchone()

        if rows:
            for r in rows:
                sym = (r.get("symbol") or r.get("ticker") or "").upper()
                if not sym:
                    continue
                acc = r.get("accumulation")
                cur.execute(
                    """
                    INSERT INTO public.screener_saved_screen_rows
                        (id, saved_screen_id, user_id, symbol, company_name,
                         market_cap, sector, industry, beta, price_at_save,
                         change_percent_1d, volume, dollar_volume,
                         volume_to_market_cap, exchange, volume_surge,
                         accumulation, options_oi, options_oi_change_pct,
                         options_activity_score, role, score,
                         hidden_gem_score, row_json)
                    VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s, %s,%s,%s,%s,%s,
                            %s,%s,%s,%s::jsonb)
                    """,
                    (
                        _uuid4(), screen_id, user_id, sym,
                        r.get("company_name") or r.get("name"),
                        _safe_float(r.get("market_cap")),
                        r.get("sector"),
                        r.get("industry"),
                        _safe_float(r.get("beta")),
                        _safe_float(r.get("price")),
                        _safe_float(r.get("change_percent_1d")),
                        _safe_float(r.get("volume")),
                        _safe_float(r.get("dollar_volume")),
                        _safe_float(r.get("volume_to_market_cap")),
                        r.get("exchange"),
                        _safe_float(r.get("volume_surge")),
                        json.dumps(acc) if acc is not None else None,
                        _safe_float(r.get("options_oi")),
                        _safe_float(r.get("options_oi_change_pct")),
                        _safe_float(r.get("options_activity_score")),
                        r.get("role"),
                        _safe_float(r.get("score")),
                        _safe_float(r.get("hidden_gem_score")),
                        json.dumps(r),
                    ),
                )

        conn.commit()
        cur.close()

        if rec:
            return {
                "id":            str(rec[0]),
                "name":          rec[1],
                "tab":           rec[2],
                "theme_key":     rec[3],
                "theme_label":   rec[4],
                "row_count":     rec[5],
                "created_at":    rec[6].isoformat() if rec[6] else None,
                "updated_at":    rec[7].isoformat() if rec[7] else None,
                "save_type":     rec[8],
                "snapshot_date": rec[9].isoformat() if rec[9] else None,
                "expires_at":    rec[10].isoformat() if rec[10] else None,
            }
        return None
    except Exception as e:
        print(f"[SAVED_SCREENS] create error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    finally:
        _put_conn(conn)


def list_saved_screens(
    user_id: str,
    tab: Optional[str] = None,
    theme_key: Optional[str] = None,
    save_type: Optional[str] = None,
    lookback_days: Optional[int] = None,
    limit: int = 100,
) -> list:
    """
    Return saved screens for user_id, newest first.
    Each entry includes a top_symbols_preview (first 8 symbols).

    save_type   — filter to 'manual' or 'daily_auto'; None = all
    lookback_days — restrict to screens created in the last N days;
                    when save_type='daily_auto', defaults to 60 if not set
    """
    ensure_tables()
    _ensure_saved_screens_v2()
    conn = _get_conn()
    if conn is None:
        return []
    out: list = []
    try:
        cur = conn.cursor()
        where_clauses = ["s.user_id = %s"]
        params: list = [user_id]
        if tab:
            where_clauses.append("s.tab = %s")
            params.append(tab)
        if theme_key:
            where_clauses.append("s.theme_key = %s")
            params.append(theme_key)
        if save_type:
            where_clauses.append("s.save_type = %s")
            params.append(save_type)
        # Resolve effective lookback
        effective_lookback = lookback_days
        if effective_lookback is None and save_type == "daily_auto":
            effective_lookback = 60
        if effective_lookback is not None:
            where_clauses.append(
                "s.created_at >= NOW() - INTERVAL '%s days'" % int(effective_lookback)
            )
        where_sql = " AND ".join(where_clauses)

        cur.execute(
            f"""
            SELECT s.id, s.name, s.tab, s.theme_key, s.theme_label,
                   s.row_count, s.created_at, s.updated_at,
                   s.filters_json, s.metadata_json,
                   s.save_type, s.snapshot_date, s.expires_at
            FROM public.screener_saved_screens s
            WHERE {where_sql}
            ORDER BY s.created_at DESC
            LIMIT %s
            """,
            params + [limit],
        )
        rows = cur.fetchall()

        screen_ids = [str(r[0]) for r in rows]
        previews: dict[str, list] = {}
        if screen_ids:
            cur.execute(
                """
                SELECT DISTINCT ON (saved_screen_id, symbol)
                    saved_screen_id, symbol
                FROM public.screener_saved_screen_rows
                WHERE saved_screen_id = ANY(%s::uuid[])
                ORDER BY saved_screen_id, symbol
                """,
                (screen_ids,),
            )
            for sid, sym in cur.fetchall():
                previews.setdefault(str(sid), []).append(sym)

        for r in rows:
            sid = str(r[0])
            all_syms = previews.get(sid, [])
            out.append({
                "id":                  sid,
                "name":                r[1],
                "tab":                 r[2],
                "theme_key":           r[3],
                "theme_label":         r[4],
                "row_count":           r[5],
                "created_at":          r[6].isoformat() if r[6] else None,
                "updated_at":          r[7].isoformat() if r[7] else None,
                "filters_summary":     r[8] if r[8] else {},
                "metadata_summary":    _saved_screen_meta_summary(r[9]),
                "top_symbols_preview": all_syms[:8],
                "save_type":           r[10] if r[10] else "manual",
                "snapshot_date":       r[11].isoformat() if r[11] else None,
                "expires_at":          r[12].isoformat() if r[12] else None,
            })
        cur.close()
    except Exception as e:
        print(f"[SAVED_SCREENS] list error: {e}")
    finally:
        _put_conn(conn)
    return out


def _saved_screen_meta_summary(meta_json) -> dict:
    """Extract lightweight display fields from metadata_json."""
    if not meta_json:
        return {}
    try:
        m = meta_json if isinstance(meta_json, dict) else json.loads(meta_json)
        return {k: m[k] for k in (
            "universe_built_at", "served_at", "universe_age_hours",
            "universe_db_source", "quote_cache_status",
            "low_metadata_coverage", "fund_coverage_pct",
            "eligible_fund_coverage_pct", "theme_rs",
        ) if k in m}
    except Exception:
        return {}


def get_saved_screen(user_id: str, screen_id: str) -> Optional[dict]:
    """
    Return saved screen header + all rows for screen_id.
    Returns None if not found or not owned by user_id.
    """
    ensure_tables()
    _ensure_saved_screens_v2()
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, user_id, name, tab, theme_key, theme_label,
                   filters_json, query_params_json, metadata_json,
                   row_count, created_at, updated_at,
                   save_type, snapshot_date, expires_at
            FROM public.screener_saved_screens
            WHERE id = %s::uuid AND user_id = %s
            """,
            (screen_id, user_id),
        )
        rec = cur.fetchone()
        if not rec:
            cur.close()
            return None

        cur.execute(
            """
            SELECT symbol, company_name, market_cap, sector, industry,
                   beta, price_at_save, change_percent_1d, volume,
                   dollar_volume, volume_to_market_cap, exchange,
                   volume_surge, accumulation, options_oi,
                   options_oi_change_pct, options_activity_score,
                   role, score, hidden_gem_score, row_json, created_at
            FROM public.screener_saved_screen_rows
            WHERE saved_screen_id = %s::uuid
            ORDER BY symbol
            """,
            (screen_id,),
        )
        rows = []
        for row in cur.fetchall():
            rows.append({
                "symbol":                str(row[0]),
                "company_name":          row[1],
                "market_cap":            float(row[2])  if row[2]  is not None else None,
                "sector":                row[3],
                "industry":              row[4],
                "beta":                  float(row[5])  if row[5]  is not None else None,
                "price_at_save":         float(row[6])  if row[6]  is not None else None,
                "change_percent_1d":     float(row[7])  if row[7]  is not None else None,
                "volume":                float(row[8])  if row[8]  is not None else None,
                "dollar_volume":         float(row[9])  if row[9]  is not None else None,
                "volume_to_market_cap":  float(row[10]) if row[10] is not None else None,
                "exchange":              row[11],
                "volume_surge":          float(row[12]) if row[12] is not None else None,
                "accumulation":          row[13],
                "options_oi":            float(row[14]) if row[14] is not None else None,
                "options_oi_change_pct": float(row[15]) if row[15] is not None else None,
                "options_activity_score":float(row[16]) if row[16] is not None else None,
                "role":                  row[17],
                "score":                 float(row[18]) if row[18] is not None else None,
                "hidden_gem_score":      float(row[19]) if row[19] is not None else None,
                "row_json":              row[20] if isinstance(row[20], dict) else {},
                "saved_at":              row[21].isoformat() if row[21] else None,
            })

        cur.close()
        return {
            "id":                str(rec[0]),
            "user_id":           rec[1],
            "name":              rec[2],
            "tab":               rec[3],
            "theme_key":         rec[4],
            "theme_label":       rec[5],
            "filters_json":      rec[6] if isinstance(rec[6], dict) else {},
            "query_params_json": rec[7] if isinstance(rec[7], dict) else {},
            "metadata_json":     rec[8] if isinstance(rec[8], dict) else {},
            "row_count":         rec[9],
            "created_at":        rec[10].isoformat() if rec[10] else None,
            "updated_at":        rec[11].isoformat() if rec[11] else None,
            "save_type":         rec[12] if rec[12] else "manual",
            "snapshot_date":     rec[13].isoformat() if rec[13] else None,
            "expires_at":        rec[14].isoformat() if rec[14] else None,
            "rows":              rows,
        }
    except Exception as e:
        print(f"[SAVED_SCREENS] get error: {e}")
        return None
    finally:
        _put_conn(conn)


def delete_saved_screen(user_id: str, screen_id: str) -> bool:
    """
    Delete a saved screen (and its rows via CASCADE).
    Returns True if a row was deleted, False otherwise.
    """
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            DELETE FROM public.screener_saved_screens
            WHERE id = %s::uuid AND user_id = %s
            """,
            (screen_id, user_id),
        )
        deleted = cur.rowcount > 0
        conn.commit()
        cur.close()
        return deleted
    except Exception as e:
        print(f"[SAVED_SCREENS] delete error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def _get_current_prices_from_cache(symbols: list) -> dict:
    """
    Read current prices from screener_quote_cache (no live fetch).
    Returns {symbol: price_float}.
    """
    if not symbols:
        return {}
    syms = sorted({s.upper() for s in symbols if s})
    conn = _get_conn()
    if conn is None:
        return {}
    out: dict = {}
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT symbol, price FROM public.screener_quote_cache WHERE symbol = ANY(%s)",
            (syms,),
        )
        for sym, price in cur.fetchall():
            if price is not None:
                out[str(sym)] = float(price)
        cur.close()
    except Exception as e:
        print(f"[SAVED_SCREENS] price cache read error: {e}")
    finally:
        _put_conn(conn)
    return out


def get_saved_screen_insights(
    user_id: str,
    tab: Optional[str] = None,
    theme_key: Optional[str] = None,
    save_type: Optional[str] = None,
    lookback_days: int = 90,
) -> dict:
    """
    Compute cross-screen insights over the user's saved screens.

    No provider calls — reads only screener_saved_screens,
    screener_saved_screen_rows, and screener_quote_cache.

    save_type — filter to 'manual' or 'daily_auto'; None = all screens.
    """
    ensure_tables()
    _ensure_saved_screens_v2()
    conn = _get_conn()
    if conn is None:
        return {"error": "db_unavailable"}
    try:
        from datetime import datetime, timedelta, timezone as _tz
        cur = conn.cursor()

        # ── 1. Load all screens + rows in window ─────────────────────────────
        where = ["s.user_id = %s", "s.created_at >= NOW() - INTERVAL '%s days'" % int(lookback_days)]
        params: list = [user_id]
        if tab:
            where.append("s.tab = %s")
            params.append(tab)
        if theme_key:
            where.append("s.theme_key = %s")
            params.append(theme_key)
        if save_type:
            where.append("s.save_type = %s")
            params.append(save_type)
        where_sql = " AND ".join(where)

        cur.execute(
            f"""
            SELECT s.id, s.tab, s.theme_key, s.theme_label, s.created_at,
                   r.symbol, r.price_at_save
            FROM public.screener_saved_screens s
            JOIN public.screener_saved_screen_rows r ON r.saved_screen_id = s.id
            WHERE {where_sql}
            ORDER BY s.created_at ASC
            """,
            params,
        )
        raw = cur.fetchall()
        cur.close()
    except Exception as e:
        print(f"[SAVED_SCREENS] insights load error: {e}")
        _put_conn(conn)
        return {"error": str(e)}
    finally:
        _put_conn(conn)

    if not raw:
        return {
            "saved_screen_count": 0,
            "date_range": None,
            "recurring_tickers": [],
            "newly_appearing_tickers": [],
            "disappeared_tickers": [],
            "tickers_by_appearance_count": {},
            "tickers_week_over_week": [],
            "themes_by_frequency": {},
            "emerging_themes": [],
            "best_performers_since_first_saved": [],
            "worst_performers_since_first_saved": [],
        }

    # ── 2. Organise data ──────────────────────────────────────────────────────
    screens: dict = {}   # screen_id -> {tab, theme_key, theme_label, created_at, symbols}
    # symbol -> list of {screen_id, created_at, price_at_save}
    sym_appearances: dict = {}

    for sid, s_tab, s_tk, s_tl, s_created, symbol, price in raw:
        sid = str(sid)
        if sid not in screens:
            screens[sid] = {
                "tab": s_tab, "theme_key": s_tk, "theme_label": s_tl,
                "created_at": s_created, "symbols": set(),
            }
        screens[sid]["symbols"].add(symbol)
        sym_appearances.setdefault(symbol, []).append({
            "screen_id": sid,
            "created_at": s_created,
            "price_at_save": float(price) if price is not None else None,
        })

    sorted_screens = sorted(screens.values(), key=lambda s: s["created_at"])
    saved_screen_count = len(screens)

    if sorted_screens:
        date_range = {
            "earliest": sorted_screens[0]["created_at"].isoformat(),
            "latest":   sorted_screens[-1]["created_at"].isoformat(),
        }
    else:
        date_range = None

    # ── 3. Tickers by appearance count ───────────────────────────────────────
    tickers_by_count = {sym: len(apps) for sym, apps in sym_appearances.items()}
    recurring = sorted(
        [sym for sym, cnt in tickers_by_count.items() if cnt >= 2],
        key=lambda s: -tickers_by_count[s],
    )

    # ── 4. Newly appearing / disappeared ─────────────────────────────────────
    newly_appearing: list = []
    disappeared: list = []
    if len(sorted_screens) >= 2:
        latest_syms  = sorted_screens[-1]["symbols"]
        prior_syms   = set().union(*[s["symbols"] for s in sorted_screens[:-1]])
        previous_syms = sorted_screens[-2]["symbols"]
        newly_appearing = sorted(latest_syms - prior_syms)
        disappeared     = sorted(previous_syms - latest_syms)

    # ── 5. Week-over-week tickers ─────────────────────────────────────────────
    sym_weeks: dict = {}
    for sym, apps in sym_appearances.items():
        weeks = set()
        for app in apps:
            dt = app["created_at"]
            iso = dt.isocalendar()
            weeks.add((iso[0], iso[1]))
        sym_weeks[sym] = weeks
    tickers_wow = sorted(
        [sym for sym, wks in sym_weeks.items() if len(wks) >= 2],
        key=lambda s: -len(sym_weeks[s]),
    )

    # ── 6. Themes by frequency ────────────────────────────────────────────────
    themes_by_freq: dict = {}
    for s in sorted_screens:
        key = s.get("theme_key") or s.get("tab") or "unknown"
        themes_by_freq[key] = themes_by_freq.get(key, 0) + 1

    # ── 7. Emerging themes ────────────────────────────────────────────────────
    emerging: list = []
    if len(sorted_screens) >= 2:
        mid = len(sorted_screens) // 2
        recent_screens = sorted_screens[mid:]
        older_screens  = sorted_screens[:mid]
        recent_tf: dict = {}
        older_tf: dict = {}
        for s in recent_screens:
            k = s.get("theme_key") or s.get("tab") or "unknown"
            recent_tf[k] = recent_tf.get(k, 0) + 1
        for s in older_screens:
            k = s.get("theme_key") or s.get("tab") or "unknown"
            older_tf[k] = older_tf.get(k, 0) + 1
        emerging = [k for k in recent_tf if recent_tf[k] > older_tf.get(k, 0)]

    # ── 8. Performance since first saved ─────────────────────────────────────
    # first_price per symbol = price_at_save in their earliest appearance
    first_prices: dict = {}
    latest_saved_prices: dict = {}
    for sym, apps in sym_appearances.items():
        sorted_apps = sorted(apps, key=lambda a: a["created_at"])
        first_prices[sym]         = sorted_apps[0]["price_at_save"]
        latest_saved_prices[sym]  = sorted_apps[-1]["price_at_save"]

    syms_with_price = [s for s, p in first_prices.items() if p and p > 0]
    current_prices  = _get_current_prices_from_cache(syms_with_price)

    performances: list = []
    for sym in syms_with_price:
        fp = first_prices[sym]
        cp = current_prices.get(sym) or latest_saved_prices.get(sym)
        if cp is None or cp <= 0:
            continue
        pct = round((cp - fp) / fp * 100, 2)
        performances.append({
            "symbol":          sym,
            "first_price":     fp,
            "latest_price":    cp,
            "price_source":    "quote_cache" if sym in current_prices else "last_saved",
            "performance_pct": pct,
        })

    performances.sort(key=lambda p: p["performance_pct"], reverse=True)
    best_performers  = performances[:10]
    worst_performers = list(reversed(performances[-10:]))

    return {
        "saved_screen_count":            saved_screen_count,
        "date_range":                    date_range,
        "recurring_tickers":             recurring,
        "newly_appearing_tickers":       newly_appearing,
        "disappeared_tickers":           disappeared,
        "tickers_by_appearance_count":   dict(sorted(
            tickers_by_count.items(), key=lambda x: -x[1]
        )),
        "tickers_week_over_week":        tickers_wow,
        "themes_by_frequency":           dict(sorted(
            themes_by_freq.items(), key=lambda x: -x[1]
        )),
        "emerging_themes":               emerging,
        "best_performers_since_first_saved":  best_performers,
        "worst_performers_since_first_saved": worst_performers,
    }


# ── Daily Auto-Save helpers ───────────────────────────────────────────────────

def _insert_rows_for_screen(cur, screen_id: str, user_id: str, rows: list) -> None:
    """Insert screener_saved_screen_rows for screen_id (shared by create + upsert)."""
    for r in rows:
        sym = (r.get("symbol") or r.get("ticker") or "").upper()
        if not sym:
            continue
        acc = r.get("accumulation")
        cur.execute(
            """
            INSERT INTO public.screener_saved_screen_rows
                (id, saved_screen_id, user_id, symbol, company_name,
                 market_cap, sector, industry, beta, price_at_save,
                 change_percent_1d, volume, dollar_volume,
                 volume_to_market_cap, exchange, volume_surge,
                 accumulation, options_oi, options_oi_change_pct,
                 options_activity_score, role, score,
                 hidden_gem_score, row_json)
            VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s, %s,%s,%s,%s,%s,
                    %s,%s,%s,%s::jsonb)
            """,
            (
                _uuid4(), screen_id, user_id, sym,
                r.get("company_name") or r.get("name"),
                _safe_float(r.get("market_cap")),
                r.get("sector"),
                r.get("industry"),
                _safe_float(r.get("beta")),
                _safe_float(r.get("price")),
                _safe_float(r.get("change_percent_1d")),
                _safe_float(r.get("volume")),
                _safe_float(r.get("dollar_volume")),
                _safe_float(r.get("volume_to_market_cap")),
                r.get("exchange"),
                _safe_float(r.get("volume_surge")),
                json.dumps(acc) if acc is not None else None,
                _safe_float(r.get("options_oi")),
                _safe_float(r.get("options_oi_change_pct")),
                _safe_float(r.get("options_activity_score")),
                r.get("role"),
                _safe_float(r.get("score")),
                _safe_float(r.get("hidden_gem_score")),
                json.dumps(r),
            ),
        )


def upsert_daily_auto_screen(
    user_id: str,
    tab: str,
    theme_key: Optional[str],
    theme_label: Optional[str],
    filters_json: dict,
    query_params_json: dict,
    metadata_json: dict,
    rows: list,
    snapshot_date_str: Optional[str] = None,
) -> Optional[dict]:
    """
    Upsert a daily_auto saved screen.

    Upsert key: (user_id, tab, COALESCE(theme_key,''), snapshot_date).

    If an existing daily_auto screen matches:
      • delete all its current rows
      • update the header (name, row_count, updated_at, etc.)
      • insert the new rows
      • action = 'updated'
    Else:
      • insert new screen + rows
      • action = 'created'

    expires_at = snapshot_date + 60 days.
    No provider calls.  Returns the header dict with 'action' key.
    """
    ensure_tables()
    _ensure_saved_screens_v2()

    from datetime import date, timedelta, timezone as _tz, datetime as _dt

    if snapshot_date_str:
        try:
            snap_date = date.fromisoformat(snapshot_date_str)
        except ValueError:
            snap_date = date.today()
    else:
        snap_date = date.today()

    expires_at = _dt.combine(
        snap_date + timedelta(days=60),
        _dt.min.time(),
        tzinfo=_tz.utc,
    )

    label = (theme_label or tab.replace("_", " ").title()).strip()
    name = f"Daily {label} \u2014 {snap_date.strftime('%b %-d, %Y')}"
    row_count = len(rows)
    tk_norm = theme_key or None  # keep None for DB NULL

    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()

        # 1. Check for existing daily_auto screen matching the upsert key
        cur.execute(
            """
            SELECT id FROM public.screener_saved_screens
            WHERE user_id = %s
              AND tab = %s
              AND COALESCE(theme_key, '') = COALESCE(%s, '')
              AND snapshot_date = %s
              AND save_type = 'daily_auto'
            LIMIT 1
            """,
            (user_id, tab, tk_norm, snap_date),
        )
        existing = cur.fetchone()

        if existing:
            # UPDATE path: replace rows, update header
            existing_id = str(existing[0])

            # Delete old rows (cascade would also work, but explicit is safer)
            cur.execute(
                "DELETE FROM public.screener_saved_screen_rows WHERE saved_screen_id = %s::uuid",
                (existing_id,),
            )

            # Update header
            cur.execute(
                """
                UPDATE public.screener_saved_screens SET
                    name              = %s,
                    theme_label       = %s,
                    filters_json      = %s::jsonb,
                    query_params_json = %s::jsonb,
                    metadata_json     = %s::jsonb,
                    row_count         = %s,
                    expires_at        = %s,
                    updated_at        = NOW()
                WHERE id = %s::uuid
                RETURNING id, name, tab, theme_key, theme_label, row_count,
                          created_at, updated_at, save_type, snapshot_date, expires_at
                """,
                (
                    name, theme_label,
                    json.dumps(filters_json or {}),
                    json.dumps(query_params_json or {}),
                    json.dumps(metadata_json or {}),
                    row_count,
                    expires_at,
                    existing_id,
                ),
            )
            rec = cur.fetchone()
            _insert_rows_for_screen(cur, existing_id, user_id, rows)
            conn.commit()
            cur.close()
            if rec:
                return {
                    "action":        "updated",
                    "id":            str(rec[0]),
                    "name":          rec[1],
                    "tab":           rec[2],
                    "theme_key":     rec[3],
                    "theme_label":   rec[4],
                    "row_count":     rec[5],
                    "created_at":    rec[6].isoformat() if rec[6] else None,
                    "updated_at":    rec[7].isoformat() if rec[7] else None,
                    "save_type":     rec[8],
                    "snapshot_date": rec[9].isoformat() if rec[9] else None,
                    "expires_at":    rec[10].isoformat() if rec[10] else None,
                }
            return None

        else:
            # INSERT path: new screen + rows
            new_id = _uuid4()
            cur.execute(
                """
                INSERT INTO public.screener_saved_screens
                    (id, user_id, name, tab, theme_key, theme_label,
                     filters_json, query_params_json, metadata_json,
                     row_count, save_type, snapshot_date, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s,
                        %s::jsonb, %s::jsonb, %s::jsonb,
                        %s, 'daily_auto', %s, %s)
                RETURNING id, name, tab, theme_key, theme_label, row_count,
                          created_at, updated_at, save_type, snapshot_date, expires_at
                """,
                (
                    new_id, user_id, name, tab, tk_norm, theme_label,
                    json.dumps(filters_json or {}),
                    json.dumps(query_params_json or {}),
                    json.dumps(metadata_json or {}),
                    row_count,
                    snap_date,
                    expires_at,
                ),
            )
            rec = cur.fetchone()
            _insert_rows_for_screen(cur, new_id, user_id, rows)
            conn.commit()
            cur.close()
            if rec:
                return {
                    "action":        "created",
                    "id":            str(rec[0]),
                    "name":          rec[1],
                    "tab":           rec[2],
                    "theme_key":     rec[3],
                    "theme_label":   rec[4],
                    "row_count":     rec[5],
                    "created_at":    rec[6].isoformat() if rec[6] else None,
                    "updated_at":    rec[7].isoformat() if rec[7] else None,
                    "save_type":     rec[8],
                    "snapshot_date": rec[9].isoformat() if rec[9] else None,
                    "expires_at":    rec[10].isoformat() if rec[10] else None,
                }
            return None

    except Exception as e:
        print(f"[SAVED_SCREENS] upsert_daily_auto error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    finally:
        _put_conn(conn)


def cleanup_expired_saved_screens(user_id: Optional[str] = None) -> int:
    """
    Delete daily_auto saved screens whose snapshot_date is older than 60 days.
    Rows cascade automatically.

    Manual saved screens (save_type != 'daily_auto') are NEVER deleted.

    user_id — when provided, only clean up that user's screens.
               When None, clean up all users (suitable for a global background job).

    Returns the number of screens deleted.
    """
    ensure_tables()
    _ensure_saved_screens_v2()
    conn = _get_conn()
    if conn is None:
        return 0
    try:
        cur = conn.cursor()
        if user_id:
            cur.execute(
                """
                DELETE FROM public.screener_saved_screens
                WHERE save_type = 'daily_auto'
                  AND snapshot_date < CURRENT_DATE - INTERVAL '60 days'
                  AND user_id = %s
                """,
                (user_id,),
            )
        else:
            cur.execute(
                """
                DELETE FROM public.screener_saved_screens
                WHERE save_type = 'daily_auto'
                  AND snapshot_date < CURRENT_DATE - INTERVAL '60 days'
                """
            )
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        if deleted:
            print(f"[SAVED_SCREENS] retention cleanup: deleted {deleted} expired daily_auto screen(s)")
        return deleted
    except Exception as e:
        print(f"[SAVED_SCREENS] cleanup error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        _put_conn(conn)


# ── screener_query_cache CRUD ─────────────────────────────────────────────────

def get_query_cache(
    cache_key: str,
    schema_version: Optional[str] = None,
) -> Optional[dict]:
    """
    Look up a cached screener query response.

    Returns the cache row dict if:
      - a row with cache_key exists
      - expires_at > NOW()
      - schema_version matches (when provided and stored version is not None)

    Returns None on miss, expiry, version mismatch, or any DB error.
    """
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT cache_key, tab, theme_key,
                   filters_json, query_params_json,
                   response_json, row_count,
                   refresh_schema_version,
                   created_at, updated_at, expires_at,
                   metadata_json
            FROM public.screener_query_cache
            WHERE cache_key = %s
              AND expires_at > NOW()
            LIMIT 1
            """,
            (cache_key,),
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            _put_conn(conn)
            return None
        (
            _key, _tab, _theme,
            _filters, _qp,
            _resp, _rc,
            _sv,
            _created, _updated, _expires,
            _meta,
        ) = row
        _put_conn(conn)
        # Version mismatch — treat as miss (schema upgrade invalidates cache)
        if schema_version and _sv and _sv != schema_version:
            return None
        def _parse(v: Any) -> Any:
            if isinstance(v, dict):
                return v
            if isinstance(v, str):
                try:
                    return json.loads(v)
                except Exception:
                    return {}
            return v or {}
        return {
            "cache_key":              _key,
            "tab":                    _tab,
            "theme_key":              _theme,
            "filters_json":           _parse(_filters),
            "query_params_json":      _parse(_qp),
            "response_json":          _parse(_resp),
            "row_count":              _rc,
            "refresh_schema_version": _sv,
            "created_at":             _created.isoformat() if _created else None,
            "updated_at":             _updated.isoformat() if _updated else None,
            "expires_at":             _expires.isoformat() if _expires else None,
            "metadata_json":          _parse(_meta),
        }
    except Exception as e:
        print(f"[SCREENER_QUERY_CACHE] get error key={cache_key!r}: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        _put_conn(conn)
        return None


def set_query_cache(
    *,
    cache_key: str,
    tab: str,
    theme_key: Optional[str],
    filters_json: dict,
    query_params_json: dict,
    response_json: dict,
    row_count: int,
    refresh_schema_version: Optional[str] = None,
    metadata_json: Optional[dict] = None,
    ttl_days: int = 7,
) -> tuple:
    """
    Upsert a cached screener query response.

    Returns (ok: bool, expires_at_iso: str).
    Runs opportunistic expired-row cleanup on every upsert (single DELETE,
    indexed on expires_at — cheap).
    """
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return (False, "")
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.screener_query_cache
                (cache_key, tab, theme_key,
                 filters_json, query_params_json,
                 response_json, row_count,
                 refresh_schema_version,
                 created_at, updated_at, expires_at,
                 metadata_json)
            VALUES
                (%s, %s, %s,
                 %s::jsonb, %s::jsonb,
                 %s::jsonb, %s,
                 %s,
                 NOW(), NOW(), NOW() + (%s || ' days')::interval,
                 %s::jsonb)
            ON CONFLICT (cache_key)
            DO UPDATE SET
                response_json          = EXCLUDED.response_json,
                row_count              = EXCLUDED.row_count,
                refresh_schema_version = EXCLUDED.refresh_schema_version,
                filters_json           = EXCLUDED.filters_json,
                query_params_json      = EXCLUDED.query_params_json,
                metadata_json          = EXCLUDED.metadata_json,
                updated_at             = NOW(),
                expires_at             = NOW() + (%s || ' days')::interval
            RETURNING expires_at
            """,
            (
                cache_key, tab, theme_key,
                _jsonb(filters_json), _jsonb(query_params_json),
                _jsonb(response_json), int(row_count),
                refresh_schema_version,
                str(int(ttl_days)),
                _jsonb(metadata_json or {}),
                str(int(ttl_days)),
            ),
        )
        result_row = cur.fetchone()
        expires_at_iso = result_row[0].isoformat() if (result_row and result_row[0]) else ""
        # Opportunistic cleanup: one indexed DELETE, no scan
        cur.execute(
            "DELETE FROM public.screener_query_cache WHERE expires_at < NOW()"
        )
        conn.commit()
        cur.close()
        return (True, expires_at_iso)
    except Exception as e:
        print(f"[SCREENER_QUERY_CACHE] set error key={cache_key!r}: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return (False, "")
    finally:
        _put_conn(conn)


def expire_theme_query_cache(theme_key: str) -> int:
    """
    Immediately expire all query-cache entries for a theme.

    Called after a background snapshot rebuild so that stale cached responses
    (e.g. an "All" result written against a 5-symbol snapshot) are not served
    after the snapshot has been upgraded to a broader universe.

    Returns the number of rows expired (0 on error or nothing to expire).
    """
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return 0
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE public.screener_query_cache "
            "SET expires_at = NOW() - INTERVAL '1 second' "
            "WHERE theme_key = %s AND expires_at > NOW()",
            (theme_key,),
        )
        expired = cur.rowcount
        conn.commit()
        cur.close()
        if expired:
            print(f"[SCREENER_QUERY_CACHE] expired {expired} stale entry(ies) for theme={theme_key!r} after snapshot upgrade")
        return expired
    except Exception as e:
        print(f"[SCREENER_QUERY_CACHE] expire_theme error theme={theme_key!r}: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        _put_conn(conn)


def cleanup_expired_query_cache() -> int:
    """Delete all expired rows from screener_query_cache. Returns count deleted."""
    ensure_tables()
    conn = _get_conn()
    if conn is None:
        return 0
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM public.screener_query_cache WHERE expires_at < NOW()")
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        if deleted:
            print(f"[SCREENER_QUERY_CACHE] cleanup: deleted {deleted} expired row(s)")
        return deleted
    except Exception as e:
        print(f"[SCREENER_QUERY_CACHE] cleanup error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        _put_conn(conn)


# ═══════════════════════════════════════════════════════════════════════════════
# Anchor Research — durable overlay tables
# anchor_supply_chain_research_nodes  — researched nodes per private anchor
# anchor_research_runs                — audit log of each LLM research run
# ═══════════════════════════════════════════════════════════════════════════════

_ARCN_DDL_APPLIED: bool = False


def _arcn_ddl_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS public.anchor_supply_chain_research_nodes (
        id                          BIGSERIAL PRIMARY KEY,
        anchor_key                  TEXT NOT NULL,
        anchor_name                 TEXT NOT NULL,
        ticker                      TEXT NOT NULL DEFAULT '',
        company_name                TEXT NOT NULL,
        is_public                   BOOLEAN NOT NULL DEFAULT TRUE,
        exchange                    TEXT NULL,
        supply_chain_role           TEXT NULL,
        relationship_type           TEXT NOT NULL DEFAULT 'direct',
        themes                      JSONB NOT NULL DEFAULT '[]'::jsonb,
        layer                       INTEGER NOT NULL DEFAULT 2,
        bottleneck_score            NUMERIC(6,2) NOT NULL DEFAULT 60,
        confidence                  TEXT NOT NULL DEFAULT 'medium',
        evidence                    JSONB NOT NULL DEFAULT '[]'::jsonb,
        source_urls                 JSONB NOT NULL DEFAULT '[]'::jsonb,
        giant_anchors               JSONB NOT NULL DEFAULT '[]'::jsonb,
        why_it_matters              TEXT NULL,
        why_hidden                  TEXT NULL,
        why_now                     TEXT NULL,
        what_would_break_thesis     TEXT NULL,
        public_market_proxy_reason  TEXT NULL,
        overlap_existing_node_registry BOOLEAN NOT NULL DEFAULT FALSE,
        last_researched_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        next_research_due_at        TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '30 days'),
        research_model              TEXT NULL,
        prompt_version              TEXT NULL,
        prompt_hash                 TEXT NULL,
        research_status             TEXT NOT NULL DEFAULT 'approved',
        error                       TEXT NULL,
        created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_arcn_anchor_key
        ON public.anchor_supply_chain_research_nodes (anchor_key);
    CREATE INDEX IF NOT EXISTS idx_arcn_research_due
        ON public.anchor_supply_chain_research_nodes (next_research_due_at);
    CREATE INDEX IF NOT EXISTS idx_arcn_status
        ON public.anchor_supply_chain_research_nodes (research_status);

    CREATE TABLE IF NOT EXISTS public.anchor_research_runs (
        id              BIGSERIAL PRIMARY KEY,
        anchor_key      TEXT NOT NULL,
        anchor_name     TEXT NOT NULL,
        started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        finished_at     TIMESTAMPTZ NULL,
        status          TEXT NOT NULL DEFAULT 'running',
        nodes_written   INTEGER NOT NULL DEFAULT 0,
        model           TEXT NULL,
        prompt_version  TEXT NULL,
        prompt_hash     TEXT NULL,
        error           TEXT NULL,
        metadata_json   JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    CREATE INDEX IF NOT EXISTS idx_arr_anchor_started
        ON public.anchor_research_runs (anchor_key, started_at DESC);
    CREATE INDEX IF NOT EXISTS idx_arr_started
        ON public.anchor_research_runs (started_at DESC);
    """


def _arcn_migrate_sql() -> str:
    """
    Idempotent migrations run after CREATE TABLE IF NOT EXISTS.
    Safe to call on both fresh and pre-existing tables.
    """
    return """
    ALTER TABLE public.anchor_supply_chain_research_nodes
        ADD COLUMN IF NOT EXISTS tradingview_symbol TEXT NULL;
    ALTER TABLE public.anchor_supply_chain_research_nodes
        ADD COLUMN IF NOT EXISTS source_titles JSONB NOT NULL DEFAULT '[]'::jsonb;
    ALTER TABLE public.anchor_supply_chain_research_nodes
        ADD COLUMN IF NOT EXISTS web_search_sources JSONB NOT NULL DEFAULT '[]'::jsonb;
    ALTER TABLE public.anchor_supply_chain_research_nodes
        ADD COLUMN IF NOT EXISTS ticker_validated BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE public.anchor_supply_chain_research_nodes
        ADD COLUMN IF NOT EXISTS validation_notes TEXT NULL;

    -- Replace the full unique index with a partial index restricted to 'approved' rows.
    -- This allows quarantined (pending_review) rows to coexist with new approved rows
    -- without a unique constraint violation during re-research cycles.
    DROP INDEX IF EXISTS public.idx_arcn_anchor_ticker;
    CREATE UNIQUE INDEX IF NOT EXISTS idx_arcn_anchor_ticker_approved
        ON public.anchor_supply_chain_research_nodes (anchor_key, ticker, company_name)
        WHERE research_status = 'approved';
    """


def ensure_anchor_research_tables() -> bool:
    """Idempotently create the anchor research overlay tables. Safe to call repeatedly."""
    global _ARCN_DDL_APPLIED
    if _ARCN_DDL_APPLIED:
        return True
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(_arcn_ddl_sql())
        conn.commit()
        # Idempotent column additions (safe on both fresh and existing tables)
        try:
            cur.execute(_arcn_migrate_sql())
            conn.commit()
        except Exception as me:
            print(f"[ARCN_DDL] migration warning (non-fatal): {me}")
            try:
                conn.rollback()
            except Exception:
                pass
        cur.close()
        _ARCN_DDL_APPLIED = True
        return True
    except Exception as e:
        print(f"[ARCN_DDL] ensure_anchor_research_tables error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


# ── CRUD: anchor_supply_chain_research_nodes ───────────────────────────────────

def upsert_anchor_research_nodes(
    anchor_key: str,
    anchor_name: str,
    nodes: list,
    model: str,
    prompt_version: str,
    prompt_hash: str,
    last_researched_at: str,
    next_research_due_at: str,
) -> bool:
    """
    Replace all approved nodes for the given anchor with the new researched set.

    Strategy: delete existing approved nodes for anchor_key, then bulk-insert new ones.
    Nodes with research_status='error' are left untouched (they are diagnostic records).

    Returns True on success.
    """
    ensure_anchor_research_tables()
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        # Delete current approved nodes for this anchor
        cur.execute(
            "DELETE FROM public.anchor_supply_chain_research_nodes "
            "WHERE anchor_key = %s AND research_status = 'approved'",
            (anchor_key.upper(),),
        )
        # Bulk insert new nodes
        for node in nodes:
            cur.execute(
                """
                INSERT INTO public.anchor_supply_chain_research_nodes (
                    anchor_key, anchor_name, ticker, company_name,
                    is_public, exchange, tradingview_symbol, supply_chain_role, relationship_type,
                    themes, layer, bottleneck_score, confidence,
                    evidence, source_urls, source_titles, web_search_sources, giant_anchors,
                    why_it_matters, why_hidden, why_now, what_would_break_thesis,
                    public_market_proxy_reason, overlap_existing_node_registry,
                    ticker_validated, validation_notes,
                    last_researched_at, next_research_due_at,
                    research_model, prompt_version, prompt_hash,
                    research_status, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s::jsonb, %s, %s, %s,
                    %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, NOW(), NOW()
                )
                """,
                (
                    anchor_key.upper(),
                    anchor_name,
                    str(node.get("ticker") or ""),
                    str(node.get("company_name") or ""),
                    bool(node.get("is_public", True)),
                    node.get("exchange"),
                    str(node.get("tradingview_symbol") or node.get("ticker") or ""),
                    node.get("supply_chain_role"),
                    str(node.get("relationship_type") or "direct"),
                    json.dumps(node.get("themes") or []),
                    int(node.get("layer") or 2),
                    float(node.get("bottleneck_score") or 60),
                    str(node.get("confidence") or "medium"),
                    json.dumps(node.get("evidence") or []),
                    json.dumps(node.get("source_urls") or []),
                    json.dumps(node.get("source_titles") or []),
                    json.dumps(node.get("web_search_sources") or []),
                    json.dumps(node.get("giant_anchors") or [anchor_key.upper()]),
                    node.get("why_it_matters"),
                    node.get("why_hidden"),
                    node.get("why_now"),
                    node.get("what_would_break_thesis"),
                    node.get("public_market_proxy_reason"),
                    bool(node.get("overlap_existing_node_registry", False)),
                    bool(node.get("ticker_validated", False)),
                    node.get("validation_notes"),
                    last_researched_at,
                    next_research_due_at,
                    model,
                    prompt_version,
                    prompt_hash,
                    str(node.get("research_status") or "approved"),
                ),
            )
        conn.commit()
        cur.close()
        print(f"[ARCN] upsert_anchor_research_nodes: anchor={anchor_key} wrote {len(nodes)} nodes")
        return True
    except Exception as e:
        print(f"[ARCN] upsert_anchor_research_nodes error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def quarantine_anchor_research_nodes(anchor_key: str) -> int:
    """
    Move all 'approved' nodes for anchor_key to 'pending_review'.
    Called before re-running research so old Claude/ungrounded rows
    are preserved for audit but excluded from scoring.
    Returns the number of rows quarantined (0 is fine if none existed).
    """
    ensure_anchor_research_tables()
    conn = _get_conn()
    if conn is None:
        return 0
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE public.anchor_supply_chain_research_nodes
            SET research_status = 'pending_review', updated_at = NOW()
            WHERE anchor_key = %s AND research_status = 'approved'
            """,
            (anchor_key.upper(),),
        )
        n = cur.rowcount
        conn.commit()
        cur.close()
        print(f"[ARCN] quarantine_anchor_research_nodes: anchor={anchor_key} quarantined {n} rows")
        return n
    except Exception as e:
        print(f"[ARCN] quarantine_anchor_research_nodes error (anchor={anchor_key}): {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        _put_conn(conn)


def update_node_statuses(updates: list[dict]) -> int:
    """
    Update research_status, ticker_validated, and validation_notes for specific nodes.
    Each entry in `updates` must have: {id, research_status, ticker_validated, validation_notes}.
    Returns the total number of rows updated.
    """
    if not updates:
        return 0
    ensure_anchor_research_tables()
    conn = _get_conn()
    if conn is None:
        return 0
    try:
        cur = conn.cursor()
        count = 0
        for u in updates:
            cur.execute(
                """
                UPDATE public.anchor_supply_chain_research_nodes
                SET research_status  = %s,
                    ticker_validated = %s,
                    validation_notes = %s,
                    updated_at       = NOW()
                WHERE id = %s
                """,
                (
                    str(u.get("research_status", "pending_review")),
                    bool(u.get("ticker_validated", False)),
                    u.get("validation_notes"),
                    int(u["id"]),
                ),
            )
            count += cur.rowcount
        conn.commit()
        cur.close()
        print(f"[ARCN] update_node_statuses: updated {count} row(s)")
        return count
    except Exception as e:
        print(f"[ARCN] update_node_statuses error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        _put_conn(conn)


def get_anchor_research_nodes(
    anchor_key: str,
    status: str = "approved",
) -> list:
    """
    Return all overlay nodes for an anchor.  Default status='approved'.
    Each row is returned as a plain dict.
    """
    ensure_anchor_research_tables()
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id,
                anchor_key, anchor_name, ticker, company_name,
                is_public, exchange, tradingview_symbol, supply_chain_role, relationship_type,
                themes, layer, bottleneck_score, confidence,
                evidence, source_urls, source_titles, web_search_sources, giant_anchors,
                why_it_matters, why_hidden, why_now, what_would_break_thesis,
                public_market_proxy_reason, overlap_existing_node_registry,
                ticker_validated, validation_notes,
                last_researched_at, next_research_due_at,
                research_model, prompt_version, prompt_hash, research_status
            FROM public.anchor_supply_chain_research_nodes
            WHERE anchor_key = %s AND research_status = %s
            ORDER BY bottleneck_score DESC, id ASC
            """,
            (anchor_key.upper(), status),
        )
        cols = [d[0] for d in cur.description]
        rows = []
        for row in cur.fetchall():
            d = dict(zip(cols, row))
            # Deserialise JSONB columns that psycopg2 returns as dicts/lists
            for col in ("themes", "evidence", "source_urls", "source_titles", "web_search_sources", "giant_anchors"):
                v = d.get(col)
                if isinstance(v, str):
                    try:
                        d[col] = json.loads(v)
                    except Exception:
                        d[col] = []
                elif v is None:
                    d[col] = []
            # Timestamps → str
            for col in ("last_researched_at", "next_research_due_at"):
                v = d.get(col)
                if v is not None and not isinstance(v, str):
                    d[col] = v.isoformat() if hasattr(v, "isoformat") else str(v)
            rows.append(d)
        cur.close()
        return rows
    except Exception as e:
        print(f"[ARCN] get_anchor_research_nodes error (anchor={anchor_key}): {e}")
        return []
    finally:
        _put_conn(conn)


def get_all_approved_research_nodes() -> list:
    """Return all approved overlay nodes across all anchors."""
    ensure_anchor_research_tables()
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                anchor_key, anchor_name, ticker, company_name,
                is_public, exchange, tradingview_symbol, supply_chain_role, relationship_type,
                themes, layer, bottleneck_score, confidence,
                evidence, source_urls, source_titles, web_search_sources, giant_anchors,
                why_it_matters, why_hidden, why_now, what_would_break_thesis,
                public_market_proxy_reason, overlap_existing_node_registry,
                ticker_validated,
                last_researched_at, next_research_due_at,
                research_model, prompt_version, prompt_hash
            FROM public.anchor_supply_chain_research_nodes
            WHERE research_status = 'approved'
            ORDER BY anchor_key, bottleneck_score DESC
            """,
        )
        cols = [d[0] for d in cur.description]
        rows = []
        for row in cur.fetchall():
            d = dict(zip(cols, row))
            for col in ("themes", "evidence", "source_urls", "source_titles", "web_search_sources", "giant_anchors"):
                v = d.get(col)
                if isinstance(v, str):
                    try:
                        d[col] = json.loads(v)
                    except Exception:
                        d[col] = []
                elif v is None:
                    d[col] = []
            for col in ("last_researched_at", "next_research_due_at"):
                v = d.get(col)
                if v is not None and not isinstance(v, str):
                    d[col] = v.isoformat() if hasattr(v, "isoformat") else str(v)
            rows.append(d)
        cur.close()
        return rows
    except Exception as e:
        print(f"[ARCN] get_all_approved_research_nodes error: {e}")
        return []
    finally:
        _put_conn(conn)


def get_anchor_research_status(anchor_key: str) -> Optional[dict]:
    """
    Return summary status for an anchor: node count, last/next research timestamps.
    Returns None if no data exists.
    """
    ensure_anchor_research_tables()
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                COUNT(*) as node_count,
                MAX(last_researched_at) as last_researched_at,
                MAX(next_research_due_at) as next_research_due_at,
                MAX(research_model) as research_model,
                MAX(prompt_version) as prompt_version,
                MAX(prompt_hash) as prompt_hash
            FROM public.anchor_supply_chain_research_nodes
            WHERE anchor_key = %s AND research_status = 'approved'
            """,
            (anchor_key.upper(),),
        )
        row = cur.fetchone()
        cur.close()
        if not row or row[0] == 0:
            return None
        return {
            "anchor_key":          anchor_key.upper(),
            "node_count":          int(row[0]),
            "last_researched_at":  row[1].isoformat() if row[1] and hasattr(row[1], "isoformat") else str(row[1] or ""),
            "next_research_due_at": row[2].isoformat() if row[2] and hasattr(row[2], "isoformat") else str(row[2] or ""),
            "research_model":      row[3],
            "prompt_version":      row[4],
            "prompt_hash":         row[5],
        }
    except Exception as e:
        print(f"[ARCN] get_anchor_research_status error (anchor={anchor_key}): {e}")
        return None
    finally:
        _put_conn(conn)


def get_all_anchor_research_status() -> list:
    """Return status for every anchor that has approved nodes."""
    ensure_anchor_research_tables()
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                anchor_key,
                COUNT(*) as node_count,
                MAX(last_researched_at) as last_researched_at,
                MAX(next_research_due_at) as next_research_due_at,
                MAX(research_model) as research_model,
                MAX(prompt_version) as prompt_version
            FROM public.anchor_supply_chain_research_nodes
            WHERE research_status = 'approved'
            GROUP BY anchor_key
            ORDER BY anchor_key
            """
        )
        rows = []
        for row in cur.fetchall():
            rows.append({
                "anchor_key":          row[0],
                "node_count":          int(row[1]),
                "last_researched_at":  row[2].isoformat() if row[2] and hasattr(row[2], "isoformat") else str(row[2] or ""),
                "next_research_due_at": row[3].isoformat() if row[3] and hasattr(row[3], "isoformat") else str(row[3] or ""),
                "research_model":      row[4],
                "prompt_version":      row[5],
            })
        cur.close()
        return rows
    except Exception as e:
        print(f"[ARCN] get_all_anchor_research_status error: {e}")
        return []
    finally:
        _put_conn(conn)


# ── CRUD: anchor_research_runs ─────────────────────────────────────────────────

def log_research_run(
    anchor_key: str,
    anchor_name: str,
    model: str,
    prompt_version: str,
    prompt_hash: str,
) -> int:
    """
    Insert a new research run record (status='running').
    Returns the new run id, or -1 on failure.
    """
    ensure_anchor_research_tables()
    conn = _get_conn()
    if conn is None:
        return -1
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.anchor_research_runs
                (anchor_key, anchor_name, status, model, prompt_version, prompt_hash, started_at)
            VALUES (%s, %s, 'running', %s, %s, %s, NOW())
            RETURNING id
            """,
            (anchor_key.upper(), anchor_name, model, prompt_version, prompt_hash),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        return int(row[0]) if row else -1
    except Exception as e:
        print(f"[ARCN] log_research_run error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return -1
    finally:
        _put_conn(conn)


def finish_research_run(
    run_id: int,
    status: str,
    nodes_written: int,
    error: Optional[str] = None,
) -> bool:
    """Update a research run record with final status."""
    if run_id < 0:
        return False
    ensure_anchor_research_tables()
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE public.anchor_research_runs
            SET status = %s, nodes_written = %s, error = %s, finished_at = NOW()
            WHERE id = %s
            """,
            (status, nodes_written, error, run_id),
        )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[ARCN] finish_research_run error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)
