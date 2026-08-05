"""
PostgreSQL storage backend for prompt history and chat history.
Prefers NEON_DATABASE_URL (external cloud DB, works in dev + production).
Falls back to DATABASE_URL (Replit internal Helium DB, dev-only).
Auto-creates tables on first use. Survives all deploys and autoscale events.
"""

import json
import os
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse



def _sanitize_database_url(url: str | None) -> str | None:
    """Strip channel_binding from Neon pooler URLs and inject connect_timeout.

    psycopg2-binary doesn't always handle SCRAM channel binding correctly with
    connection poolers. connect_timeout=10 ensures connections never hang
    indefinitely when the Neon pooler is slow or the pool is being rebuilt.
    """
    if not url:
        return url
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query, keep_blank_values=True)
        if "channel_binding" in qs:
            del qs["channel_binding"]
        if "connect_timeout" not in qs:
            qs["connect_timeout"] = ["10"]
        new_query = urlencode(qs, doseq=True)
        url = urlunparse(parsed._replace(query=new_query))
    except Exception:
        pass
    return url


# NEON_DATABASE_URL is the externally-accessible cloud DB (works in both dev and
# production deployments). DATABASE_URL points to Replit's internal Helium host
# which is only reachable from the dev workspace, not from deployed containers.
_RAW_DATABASE_URL = os.environ.get("NEON_DATABASE_URL") or os.environ.get("DATABASE_URL")
_DATABASE_URL = _sanitize_database_url(_RAW_DATABASE_URL)
_pool = None
_available = False
# Track last connection error for diagnostics
_last_conn_error: str | None = None
def _to_jsonb(value):
    if not isinstance(value, dict):
        return None
    try:
        from psycopg2.extras import Json
        return Json(value)
    except Exception:
        return json.dumps(value, default=str)


def _destroy_pool():
    """Tear down the connection pool so the next _get_conn() rebuilds it."""
    global _pool, _available
    if _pool is not None:
        try:
            _pool.closeall()
        except Exception:
            pass
    _pool = None
    _available = False
def _get_conn():
    """Get a healthy connection from the pool (lazy-initialized).

    If a pooled connection is stale (Neon kills idle connections aggressively),
    discard it, destroy the pool, and rebuild once.  This guarantees callers
    always receive a usable connection or an explicit None.
    """
    global _pool, _available, _last_conn_error
    if not _DATABASE_URL:
        _last_conn_error = "No NEON_DATABASE_URL or DATABASE_URL set"
        return None

    for attempt in range(2):  # at most one retry after pool rebuild
        if _pool is None:
            try:
                import psycopg2
                from psycopg2 import pool as _pg_pool
                _pool = _pg_pool.SimpleConnectionPool(
                    1, 5, _DATABASE_URL, connect_timeout=10
                )
                _available = True
                _last_conn_error = None
            except Exception as e:
                _last_conn_error = f"Pool creation failed: {e}"
                print(f"[PG_STORAGE] {_last_conn_error}")
                _available = False
                return None

        conn = None
        try:
            conn = _pool.getconn()
        except Exception as e:
            _last_conn_error = f"getconn failed: {e}"
            print(f"[PG_STORAGE] {_last_conn_error}")
            _destroy_pool()
            continue

        # Health check: verify the connection is alive
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.execute("SET search_path TO public")
            conn.commit()
            cur.close()
            _last_conn_error = None
            return conn
        except Exception as e:
            _last_conn_error = f"Health check failed (attempt {attempt+1}): {e}"
            print(f"[PG_STORAGE] {_last_conn_error}")
            # Connection is dead — drop it and rebuild pool
            try:
                _pool.putconn(conn, close=True)
            except Exception:
                pass
            _destroy_pool()
            continue

    return None
def _put_conn(conn):
    """Return a connection to the pool."""
    if _pool and conn:
        try:
            _pool.putconn(conn)
        except Exception:
            pass
def is_available() -> bool:
    """Check if PostgreSQL is available."""
    if not _DATABASE_URL:
        return False
    conn = _get_conn()
    if conn is None:
        return False
    _put_conn(conn)
    return True


def get_last_conn_error() -> str | None:
    """Return the last connection error message (for diagnostics)."""
    return _last_conn_error
def startup_probe() -> dict:
    """Startup diagnostic for PostgreSQL connectivity/schema visibility."""
    info = {"database_url_detected": bool(_DATABASE_URL), "connected": False, "database": None, "schema": None, "tables": []}
    if not _DATABASE_URL:
        return info
    conn = _get_conn()
    if conn is None:
        return info
    try:
        cur = conn.cursor()
        cur.execute("SELECT current_database(), current_schema()")
        db_row = cur.fetchone()
        if db_row:
            info["database"] = db_row[0]
            info["schema"] = db_row[1]
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name ASC
        """)
        info["tables"] = [r[0] for r in cur.fetchall()]
        info["connected"] = True
        cur.close()
    except Exception as e:
        info["error"] = str(e)
    finally:
        _put_conn(conn)
    return info
def init_tables():
    """Create tables if they don't exist. Safe to call multiple times."""
    print("[PG_STORAGE] init_tables starting (target schema=public)")
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.prompt_history (
                user_id TEXT NOT NULL,
                bucket_key TEXT NOT NULL,
                data JSONB NOT NULL DEFAULT '{}',
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (user_id, bucket_key)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.chat_conversations (
                conv_id TEXT PRIMARY KEY,
                data JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_chat_conversations_updated
            ON public.chat_conversations (updated_at DESC)
        """)

        # New normalized chat schema (source of truth going forward)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.conversations (
                id TEXT PRIMARY KEY,
                session_id TEXT NULL,
                title TEXT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.messages (
                id BIGSERIAL PRIMARY KEY,
                conversation_id TEXT NOT NULL REFERENCES public.conversations(id) ON DELETE CASCADE,
                role TEXT NOT NULL,
                message_type TEXT NOT NULL DEFAULT 'chat',
                content TEXT NOT NULL DEFAULT '',
                structured_payload JSONB NULL,
                preset_key TEXT NULL,
                model_used TEXT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_conversation_created
            ON public.messages (conversation_id, created_at ASC, id ASC)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_conversations_updated_at
            ON public.conversations (updated_at DESC)
        """)

        # Ticker mention snapshots — one row per ticker per assistant message
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.ticker_mentions (
                id BIGSERIAL PRIMARY KEY,
                conversation_id TEXT NOT NULL,
                message_id BIGINT NULL,
                ticker TEXT NOT NULL,
                mentioned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                mention_price NUMERIC(20, 6) NULL,
                asset_type TEXT NULL,
                source TEXT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_ticker_mentions_conv
            ON public.ticker_mentions (conversation_id, mentioned_at DESC)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_ticker_mentions_message
            ON public.ticker_mentions (message_id)
        """)

        # ── Historic options data (Polygon EOD) ─────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.options_history (
                id BIGSERIAL PRIMARY KEY,
                underlying TEXT NOT NULL,
                option_ticker TEXT NOT NULL,
                expiration DATE NOT NULL,
                strike NUMERIC(12, 4) NOT NULL,
                option_type TEXT NOT NULL,
                trade_date DATE NOT NULL,
                open NUMERIC(12, 4),
                high NUMERIC(12, 4),
                low NUMERIC(12, 4),
                close NUMERIC(12, 4),
                volume BIGINT,
                vwap NUMERIC(12, 4),
                num_trades INT,
                fetched_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (option_ticker, trade_date)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_options_history_underlying
            ON public.options_history (underlying, trade_date)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_options_history_expiration
            ON public.options_history (underlying, expiration, trade_date)
        """)

        # ── Technical indicators for underlying stocks (Polygon) ────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.stock_technicals (
                id BIGSERIAL PRIMARY KEY,
                ticker TEXT NOT NULL,
                indicator TEXT NOT NULL,
                trade_date DATE NOT NULL,
                value NUMERIC(20, 6),
                signal_value NUMERIC(20, 6),
                histogram NUMERIC(20, 6),
                fetched_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (ticker, indicator, trade_date)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_technicals_ticker
            ON public.stock_technicals (ticker, trade_date)
        """)

        # ── Fetch progress tracking for background ingestion ────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.options_fetch_progress (
                ticker TEXT PRIMARY KEY,
                last_fetched_date DATE,
                contracts_fetched INT DEFAULT 0,
                status TEXT DEFAULT 'pending',
                error_message TEXT,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        # ── Watchlist persistence (survives deploys) ──────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.watchlist (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL DEFAULT 'Watchlist',
                csv_data JSONB,
                analysis JSONB,
                tickers JSONB,
                saved_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # Add name column if table already exists (preserves existing data)
        cur.execute("""
            ALTER TABLE public.watchlist ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT 'Watchlist'
        """)

        # ── Watchlist Fundamentals Cache (weekly FMP refresh) ─────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.watchlist_fundamentals_cache (
                symbol           TEXT        PRIMARY KEY,
                watchlist_id     TEXT,
                refreshed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                next_refresh_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                fields           JSONB       NOT NULL DEFAULT '{}',
                missing_fields   JSONB       NOT NULL DEFAULT '[]',
                fmp_call_count   INT         NOT NULL DEFAULT 0,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_wl_fund_cache_watchlist_due
            ON public.watchlist_fundamentals_cache (watchlist_id, next_refresh_at ASC)
        """)

        # ── Live options flow snapshots for intraday signal history ───────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.options_flow_snapshots (
                id BIGSERIAL PRIMARY KEY,
                underlying TEXT NOT NULL,
                contract_symbol TEXT NOT NULL,
                expiration DATE NULL,
                option_type TEXT NULL,
                strike NUMERIC(12, 4) NULL,
                underlying_price NUMERIC(12, 4) NULL,
                bid NUMERIC(12, 4) NULL,
                ask NUMERIC(12, 4) NULL,
                last NUMERIC(12, 4) NULL,
                midpoint NUMERIC(12, 4) NULL,
                volume BIGINT NULL,
                open_interest BIGINT NULL,
                implied_volatility NUMERIC(12, 6) NULL,
                delta NUMERIC(12, 6) NULL,
                gamma NUMERIC(12, 6) NULL,
                theta NUMERIC(12, 6) NULL,
                vega NUMERIC(12, 6) NULL,
                spread_pct NUMERIC(12, 4) NULL,
                premium_traded_estimate NUMERIC(16, 2) NULL,
                expected_move_pct NUMERIC(12, 4) NULL,
                captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_options_flow_snapshots_contract
            ON public.options_flow_snapshots (contract_symbol, captured_at DESC)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_options_flow_snapshots_underlying
            ON public.options_flow_snapshots (underlying, captured_at DESC)
        """)

        # ── Data retention rules registry ──────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.data_retention_rules (
                id               BIGSERIAL PRIMARY KEY,
                table_name       TEXT NOT NULL UNIQUE,
                timestamp_column TEXT NOT NULL,
                retention_days   INTEGER NOT NULL,
                enabled          BOOLEAN NOT NULL DEFAULT TRUE,
                description      TEXT NOT NULL DEFAULT '',
                created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        # Seed default retention rules — only insert rows that don't yet exist
        _retention_rules = [
            # ── AI chat / agent responses (45 days) ──────────────────────────
            ("public.conversations",       "updated_at",   45, True,
             "AI chat conversation records — 45-day rolling window"),
            ("public.messages",            "created_at",   45, True,
             "AI chat messages — 45-day rolling window"),
            ("public.chat_conversations",  "updated_at",   45, True,
             "Legacy chat conversation store — 45-day rolling window"),
            ("public.prompt_history",      "updated_at",   45, True,
             "Per-user AI prompt history/cache — 45-day rolling window"),
            ("public.ticker_mentions",     "created_at",   45, True,
             "Ticker mentions extracted from chat messages — 45-day rolling window"),
            ("insider_ai_cache",           "created_at",   45, True,
             "AI-generated insider-activity summaries cache — 45-day rolling window"),
            # ── Options flow snapshots (14 days) ─────────────────────────────
            ("public.options_flow_snapshots", "captured_at", 14, True,
             "Intraday live options flow snapshot rows — 14-day rolling window"),
            ("public.options_history",     "fetched_at",   14, True,
             "Historical options OHLCV data fetched from broker — 14-day rolling window"),
            # ── Sector / theme / market cache (21 days) ───────────────────────
            ("public.stock_technicals",    "fetched_at",   21, True,
             "Technical indicator cache for underlying stocks — 21-day rolling window"),
            # ── AI screener snapshots / reports (45 days) ────────────────────
            ("public.screener_snapshots",  "generated_at", 45, True,
             "Serenity strategy screener snapshots (AI-generated) — 45-day rolling window"),
            ("public.screener_reports",    "generated_at", 45, True,
             "Per-ticker Serenity screener reports (AI-generated) — 45-day rolling window"),
        ]
        for (tbl, ts_col, days, enabled, desc) in _retention_rules:
            cur.execute("""
                INSERT INTO public.data_retention_rules
                    (table_name, timestamp_column, retention_days, enabled, description)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (table_name) DO NOTHING
            """, (tbl, ts_col, days, enabled, desc))

        # ── Calendar snapshot persistence (non-Earnings catalyst tabs) ──
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.calendar_snapshots (
                tab            TEXT PRIMARY KEY,
                current_week   JSONB NOT NULL DEFAULT '[]'::jsonb,
                previous_week  JSONB NOT NULL DEFAULT '[]'::jsonb,
                events         JSONB NOT NULL DEFAULT '[]'::jsonb,
                last_updated   TIMESTAMPTZ NULL,
                status         TEXT NOT NULL DEFAULT 'empty',
                meta           JSONB NOT NULL DEFAULT '{}'::jsonb,
                updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("""
            ALTER TABLE public.calendar_snapshots
            ADD COLUMN IF NOT EXISTS events JSONB NOT NULL DEFAULT '[]'::jsonb
        """)

        # ── Symbol-driven earnings cache (Watchlist / Portfolio universes) ───
        # Keyed by universe ('watchlist' | 'portfolio').  Each row stores the
        # full list of FMP earnings events for that universe's symbol set,
        # fetched once over a 120-day forward window and refreshed when the
        # symbol set changes or the 30-day TTL expires.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.user_earnings_cache (
                universe        TEXT PRIMARY KEY,
                events          JSONB NOT NULL DEFAULT '[]',
                symbols         JSONB NOT NULL DEFAULT '[]',
                fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                fmp_window_from TEXT,
                fmp_window_to   TEXT
            )
        """)

        # ── Strategy macro history snapshots (durable restart-survival) ────────
        # Stores the three precomputed historical series for the Strategy page tabs.
        # key: cache key string (e.g. "strategy:spx_hist:1830")
        # payload: JSONB array of {date, close|value} dicts — compact daily series
        # expires_at: created_at + 24 h (set by writer)
        # max_age rule: use if (NOW() - created_at) < 24 h; refresh every 3 h
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.strategy_hist_snapshots (
                key         TEXT PRIMARY KEY,
                source      TEXT NOT NULL DEFAULT '',
                as_of       TIMESTAMPTZ NULL,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                expires_at  TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '24 hours'),
                row_count   INTEGER NOT NULL DEFAULT 0,
                payload     JSONB NOT NULL DEFAULT '[]'::jsonb
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_strategy_hist_snapshots_expires
            ON public.strategy_hist_snapshots (expires_at DESC)
        """)

        # ── Watchlist relative-volume rank snapshots ───────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.watchlist_rv_snapshots (
                watchlist_id         TEXT PRIMARY KEY,
                payload              JSONB NOT NULL DEFAULT '{}'::jsonb,
                previous_payload     JSONB,
                captured_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                previous_captured_at TIMESTAMPTZ
            )
        """)

        # ── Watchlist Vol/MC snapshots ─────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.watchlist_volmc_snapshots (
                watchlist_id         TEXT PRIMARY KEY,
                payload              JSONB NOT NULL DEFAULT '{}'::jsonb,
                previous_payload     JSONB,
                captured_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                previous_captured_at TIMESTAMPTZ
            )
        """)

        # ── Dev-admin theme ticker overrides (many-to-many) ────────────────
        # PK is (theme_id, symbol) — same symbol can exist in multiple themes.
        # action='add'    → force-include symbol in that theme's basket
        # action='remove' → force-exclude symbol from that theme's basket
        # These overrides win over base universe + watchlist seeds.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.theme_ticker_overrides (
                theme_id    TEXT        NOT NULL,
                symbol      TEXT        NOT NULL,
                action      TEXT        NOT NULL CHECK (action IN ('add', 'remove')),
                source      TEXT        NOT NULL DEFAULT 'manual_admin',
                note        TEXT,
                created_by  TEXT,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (theme_id, symbol)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_theme_ticker_overrides_theme
            ON public.theme_ticker_overrides (theme_id)
        """)

        # ── Dev-admin theme leader overrides (one leader per theme) ────────────
        # PK is theme_id — each theme has at most one manual leader symbol.
        # Same symbol can be leader for multiple different themes.
        # Clearing this row restores default (no manual leader) behavior.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.theme_leader_overrides (
                theme_id      TEXT        PRIMARY KEY,
                leader_symbol TEXT        NOT NULL,
                source        TEXT        NOT NULL DEFAULT 'manual_admin',
                note          TEXT,
                created_by    TEXT,
                created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        # ── Chart Radar saved views ────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.chart_radar_views (
                id           TEXT PRIMARY KEY,
                user_id      TEXT        NOT NULL DEFAULT 'default',
                name         TEXT        NOT NULL DEFAULT 'My Chart View',
                source       TEXT        NOT NULL DEFAULT 'watchlist',
                group_by     TEXT        NOT NULL DEFAULT 'theme',
                watchlist_id TEXT        NULL,
                filters      JSONB       NOT NULL DEFAULT '{}'::jsonb,
                layout       JSONB       NOT NULL DEFAULT '{}'::jsonb,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_chart_radar_views_user
            ON public.chart_radar_views (user_id, updated_at DESC)
        """)

        # ── Manual category overrides ───────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.watchlist_category_overrides (
                id          BIGSERIAL   PRIMARY KEY,
                user_id     TEXT        NOT NULL DEFAULT 'default',
                ticker      TEXT        NOT NULL,
                category    TEXT        NOT NULL,
                source      TEXT        NOT NULL DEFAULT 'manual',
                reason      TEXT,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (user_id, ticker)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_wl_cat_overrides_user
            ON public.watchlist_category_overrides (user_id)
        """)

        # ── Manual ticker name overrides ────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.ticker_name_overrides (
                id          BIGSERIAL   PRIMARY KEY,
                user_id     TEXT        NOT NULL DEFAULT 'default',
                ticker      TEXT        NOT NULL,
                name        TEXT        NOT NULL,
                source      TEXT        NOT NULL DEFAULT 'manual',
                reason      TEXT,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (user_id, ticker)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_ticker_name_overrides_user
            ON public.ticker_name_overrides (user_id)
        """)

        # ── Alert Signal Bus: ticker_signal_snapshots ─────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.ticker_signal_snapshots (
                id                     BIGSERIAL PRIMARY KEY,
                user_id                TEXT,
                ticker                 TEXT NOT NULL,
                source                 TEXT NOT NULL,
                ts                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                price                  NUMERIC,
                price_change_pct       NUMERIC,
                volume                 BIGINT,
                rel_volume             NUMERIC,
                volx                   NUMERIC,
                market_cap             NUMERIC,
                vol_marketcap          NUMERIC,
                options_score          NUMERIC,
                options_rank           INTEGER,
                previous_options_rank  INTEGER,
                rank_delta             INTEGER,
                call_put_bias          TEXT,
                call_volume            NUMERIC,
                put_volume             NUMERIC,
                call_put_ratio         NUMERIC,
                iv                     NUMERIC,
                expected_move          NUMERIC,
                hyperliquid_trade_usd  NUMERIC,
                hyperliquid_liq_usd    NUMERIC,
                hyperliquid_funding    NUMERIC,
                hyperliquid_oi         NUMERIC,
                metric_coverage        JSONB,
                raw                    JSONB
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tss_ticker_ts
            ON public.ticker_signal_snapshots (ticker, ts DESC)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tss_user_ts
            ON public.ticker_signal_snapshots (user_id, ts DESC)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tss_source_ts
            ON public.ticker_signal_snapshots (source, ts DESC)
        """)

        # ── Alert Signal Bus: ticker_alert_events ─────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.ticker_alert_events (
                id               BIGSERIAL PRIMARY KEY,
                user_id          TEXT,
                ticker           TEXT NOT NULL,
                alert_type       TEXT NOT NULL,
                alert_lane       TEXT NOT NULL,
                severity         TEXT NOT NULL,
                title            TEXT NOT NULL,
                short_label      TEXT NOT NULL,
                coverage_label   TEXT NOT NULL,
                summary          TEXT,
                score            NUMERIC,
                reasons          JSONB,
                source_metrics   JSONB,
                source_tags      JSONB,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                acknowledged_at  TIMESTAMPTZ,
                dismissed_at     TIMESTAMPTZ
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tae_user_created
            ON public.ticker_alert_events (user_id, created_at DESC)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tae_ticker_created
            ON public.ticker_alert_events (ticker, created_at DESC)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tae_lane_created
            ON public.ticker_alert_events (alert_lane, created_at DESC)
        """)

        # Seed retention rules for alert tables (idempotent)
        _alert_retention = [
            ("public.ticker_signal_snapshots", "ts",         7,  True,
             "Alert signal snapshots — 7-day rolling window"),
            ("public.ticker_alert_events",     "created_at", 90, True,
             "Alert event records — 90-day rolling window"),
        ]
        for (tbl, ts_col, days, enabled, desc) in _alert_retention:
            cur.execute("""
                INSERT INTO public.data_retention_rules
                    (table_name, timestamp_column, retention_days, enabled, description)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (table_name) DO NOTHING
            """, (tbl, ts_col, days, enabled, desc))

        # ── Watchlist favorites ───────────────────────────────────────────────
        # Pure user metadata — no market-data columns, no provider calls.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.watchlist_favorites (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id     TEXT NOT NULL,
                ticker      TEXT NOT NULL,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (user_id, ticker)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_wf_user_id
            ON public.watchlist_favorites (user_id)
        """)

        # ── Options Flow daily Net Premium history ─────────────────────────────
        # One row per entity per ET market day.  Intraday upserts update today's
        # row; historical dates are never modified.  Retention: 90 calendar days.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.options_net_premium_daily (
                id               BIGSERIAL PRIMARY KEY,
                entity_type      TEXT        NOT NULL,
                entity_id        TEXT        NOT NULL,
                snapshot_date    DATE        NOT NULL,
                net_premium      NUMERIC(18,2),
                call_premium     NUMERIC(18,2),
                put_premium      NUMERIC(18,2),
                premium_scope_id TEXT,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (entity_type, entity_id, snapshot_date)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_onpd_entity_date
            ON public.options_net_premium_daily (entity_type, entity_id, snapshot_date DESC)
        """)
        cur.execute("""
            INSERT INTO public.data_retention_rules
                (table_name, timestamp_column, retention_days, enabled, description)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (table_name) DO NOTHING
        """, (
            "public.options_net_premium_daily",
            "created_at",
            90,
            True,
            "Daily Net Premium snapshots for Options Flow 1D/7D/30D history — 90-day rolling window",
        ))

        # ── Live Earnings Monitor ─────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.earnings_monitor_targets (
                id                      BIGSERIAL PRIMARY KEY,
                symbol                  TEXT NOT NULL,
                expected_date           DATE,
                expected_timing         TEXT,
                fiscal_period           TEXT,
                fiscal_year             INTEGER,
                monitoring_start_at     TIMESTAMPTZ,
                monitoring_end_at       TIMESTAMPTZ,
                next_sec_check_at       TIMESTAMPTZ,
                next_fmp_check_at       TIMESTAMPTZ,
                status                  TEXT NOT NULL DEFAULT 'scheduled',
                worker_lease_owner      TEXT,
                worker_lease_expires_at TIMESTAMPTZ,
                created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (symbol, expected_date)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_emt_status_date
            ON public.earnings_monitor_targets (status, expected_date)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_emt_symbol
            ON public.earnings_monitor_targets (symbol)
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.earnings_live_events (
                id               BIGSERIAL PRIMARY KEY,
                event_id         TEXT UNIQUE NOT NULL,
                event_key        TEXT NOT NULL,
                symbol           TEXT NOT NULL,
                expected_date    DATE,
                fiscal_period    TEXT,
                fiscal_year      INTEGER,
                state            TEXT NOT NULL DEFAULT 'scheduled',
                detected_at      TIMESTAMPTZ,
                updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                revision         INTEGER NOT NULL DEFAULT 1,
                is_dry_run       BOOLEAN NOT NULL DEFAULT FALSE,
                filing_payload   JSONB,
                results_payload  JSONB,
                reaction_payload JSONB,
                source_status    JSONB,
                classification   TEXT,
                checksum         TEXT,
                last_error       TEXT,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_ele_symbol_state
            ON public.earnings_live_events (symbol, state)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_ele_event_key
            ON public.earnings_live_events (event_key)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_ele_updated
            ON public.earnings_live_events (updated_at DESC)
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.earnings_event_reads (
                id        BIGSERIAL PRIMARY KEY,
                event_id  TEXT NOT NULL,
                user_id   TEXT NOT NULL,
                read_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (event_id, user_id)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_eer_user_event
            ON public.earnings_event_reads (user_id, event_id)
        """)

        conn.commit()
        cur.close()
        print("[PG_STORAGE] init_tables completed (CREATE TABLE IF NOT EXISTS executed)")
        return True
    except Exception as e:
        print(f"[PG_STORAGE] Table creation error: {e}")
        conn.rollback()
        return False
    finally:
        _put_conn(conn)


# ── Watchlist favorites CRUD ──────────────────────────────────────────────────

def _ensure_wf_table() -> None:
    """
    Idempotent self-heal: create watchlist_favorites table if init_tables
    missed it (e.g. the app was already running when the column was added).
    Safe to call on every request — short-circuits via an in-process flag.
    """
    global _wf_table_ensured
    if _wf_table_ensured:
        return
    conn = _get_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.watchlist_favorites (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id     TEXT NOT NULL,
                ticker      TEXT NOT NULL,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (user_id, ticker)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_wf_user_id
            ON public.watchlist_favorites (user_id)
        """)
        conn.commit()
        cur.close()
        _wf_table_ensured = True
    except Exception as e:
        print(f"[WF] _ensure_wf_table error: {e}")
        conn.rollback()
    finally:
        _put_conn(conn)

_wf_table_ensured: bool = False


def list_watchlist_favorites(user_id: str) -> list[str]:
    """Return list of uppercase ticker strings favorited by user. [] on DB error."""
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT ticker FROM public.watchlist_favorites WHERE user_id = %s ORDER BY created_at ASC",
            (user_id,),
        )
        rows = cur.fetchall()
        cur.close()
        return [r[0] for r in rows]
    except Exception as e:
        print(f"[WF] list_watchlist_favorites error: {e}")
        return []
    finally:
        _put_conn(conn)


def add_watchlist_favorite(user_id: str, ticker: str) -> bool:
    """
    Idempotently add ticker to favorites for user_id.
    Ticker is normalised to uppercase before insert.
    Returns True on success, False on DB error.
    """
    ticker = ticker.strip().upper()
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.watchlist_favorites (user_id, ticker)
            VALUES (%s, %s)
            ON CONFLICT (user_id, ticker) DO UPDATE SET updated_at = NOW()
            """,
            (user_id, ticker),
        )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[WF] add_watchlist_favorite error: {e}")
        conn.rollback()
        return False
    finally:
        _put_conn(conn)


def remove_watchlist_favorite(user_id: str, ticker: str) -> bool:
    """
    Idempotently remove ticker from favorites for user_id.
    Ticker is normalised to uppercase.
    Returns True on success (including when ticker was not favorited), False on DB error.
    """
    ticker = ticker.strip().upper()
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM public.watchlist_favorites WHERE user_id = %s AND ticker = %s",
            (user_id, ticker),
        )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[WF] remove_watchlist_favorite error: {e}")
        conn.rollback()
        return False
    finally:
        _put_conn(conn)


# ── Category overrides CRUD ───────────────────────────────────────────────────

def get_category_overrides(user_id: str = "default") -> dict[str, str]:
    """Return {ticker: category} map for user. Returns {} on DB unavailability."""
    conn = _get_conn()
    if conn is None:
        return {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ticker, category FROM public.watchlist_category_overrides "
                "WHERE user_id = %s",
                (user_id,),
            )
            return {row[0]: row[1] for row in cur.fetchall()}
    except Exception as e:
        print(f"[PG_STORAGE] get_category_overrides failed: {e}")
        return {}
    finally:
        _put_conn(conn)


def upsert_category_override(
    user_id: str,
    ticker: str,
    category: str,
    source: str = "manual",
    reason: str | None = None,
) -> bool:
    """Insert or update a single ticker→category override. Returns True on success."""
    conn = _get_conn()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.watchlist_category_overrides
                    (user_id, ticker, category, source, reason, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (user_id, ticker)
                DO UPDATE SET
                    category   = EXCLUDED.category,
                    source     = EXCLUDED.source,
                    reason     = EXCLUDED.reason,
                    updated_at = NOW()
                """,
                (user_id, ticker, category, source, reason),
            )
        conn.commit()
        return True
    except Exception as e:
        print(f"[PG_STORAGE] upsert_category_override failed: {e}")
        conn.rollback()
        return False
    finally:
        _put_conn(conn)


def delete_category_override(
    user_id: str,
    ticker: str,
    only_if_category: str | None = None,
) -> bool:
    """
    Delete a single ticker's category override.
    If only_if_category is given, only deletes when the current stored category
    matches it (prevents clobbering an override that was already reassigned to a
    different category by a later write). Returns True if a row was deleted.
    """
    conn = _get_conn()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            if only_if_category is not None:
                cur.execute(
                    "DELETE FROM public.watchlist_category_overrides "
                    "WHERE user_id = %s AND ticker = %s AND category = %s",
                    (user_id, ticker, only_if_category),
                )
            else:
                cur.execute(
                    "DELETE FROM public.watchlist_category_overrides "
                    "WHERE user_id = %s AND ticker = %s",
                    (user_id, ticker),
                )
            deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    except Exception as e:
        print(f"[PG_STORAGE] delete_category_override failed: {e}")
        conn.rollback()
        return False
    finally:
        _put_conn(conn)


def bulk_upsert_category_overrides(
    user_id: str,
    updates: list[dict],
) -> int:
    """
    Upsert multiple overrides in one transaction.
    Each dict: {ticker, category, source='manual', reason=None}
    Returns count of rows affected.
    """
    if not updates:
        return 0
    conn = _get_conn()
    if conn is None:
        return 0
    try:
        count = 0
        with conn.cursor() as cur:
            for u in updates:
                ticker   = str(u.get("ticker", "")).strip().upper()
                category = str(u.get("category", "")).strip()
                source   = str(u.get("source", "manual"))
                reason   = u.get("reason")
                if not ticker or not category:
                    continue
                cur.execute(
                    """
                    INSERT INTO public.watchlist_category_overrides
                        (user_id, ticker, category, source, reason, updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (user_id, ticker)
                    DO UPDATE SET
                        category   = EXCLUDED.category,
                        source     = EXCLUDED.source,
                        reason     = EXCLUDED.reason,
                        updated_at = NOW()
                    """,
                    (user_id, ticker, category, source, reason),
                )
                count += 1
        conn.commit()
        return count
    except Exception as e:
        print(f"[PG_STORAGE] bulk_upsert_category_overrides failed: {e}")
        conn.rollback()
        return 0
    finally:
        _put_conn(conn)


# ── Ticker name overrides CRUD ───────────────────────────────────────────────

def get_name_overrides(user_id: str = "default") -> dict[str, str]:
    """Return {ticker: name} map for user. Returns {} on DB unavailability."""
    conn = _get_conn()
    if conn is None:
        return {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ticker, name FROM public.ticker_name_overrides "
                "WHERE user_id = %s",
                (user_id,),
            )
            return {row[0]: row[1] for row in cur.fetchall()}
    except Exception as e:
        print(f"[PG_STORAGE] get_name_overrides failed: {e}")
        return {}
    finally:
        _put_conn(conn)


def upsert_name_override(
    user_id: str,
    ticker: str,
    name: str,
    source: str = "manual",
    reason: str | None = None,
) -> bool:
    """Insert or update a single ticker→name override. Returns True on success."""
    conn = _get_conn()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.ticker_name_overrides
                    (user_id, ticker, name, source, reason, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (user_id, ticker)
                DO UPDATE SET
                    name       = EXCLUDED.name,
                    source     = EXCLUDED.source,
                    reason     = EXCLUDED.reason,
                    updated_at = NOW()
                """,
                (user_id, ticker, name, source, reason),
            )
        conn.commit()
        return True
    except Exception as e:
        print(f"[PG_STORAGE] upsert_name_override failed: {e}")
        conn.rollback()
        return False
    finally:
        _put_conn(conn)


def bulk_upsert_name_overrides(
    user_id: str,
    updates: list[dict],
) -> int:
    """
    Upsert multiple name overrides in one transaction.
    Each dict: {ticker, name, source='manual', reason=None}
    Returns count of rows affected.
    """
    if not updates:
        return 0
    conn = _get_conn()
    if conn is None:
        return 0
    try:
        count = 0
        with conn.cursor() as cur:
            for u in updates:
                ticker = str(u.get("ticker", "")).strip().upper()
                name   = str(u.get("name", "")).strip()
                source = str(u.get("source", "manual"))
                reason = u.get("reason")
                if not ticker or not name:
                    continue
                cur.execute(
                    """
                    INSERT INTO public.ticker_name_overrides
                        (user_id, ticker, name, source, reason, updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (user_id, ticker)
                    DO UPDATE SET
                        name       = EXCLUDED.name,
                        source     = EXCLUDED.source,
                        reason     = EXCLUDED.reason,
                        updated_at = NOW()
                    """,
                    (user_id, ticker, name, source, reason),
                )
                count += 1
        conn.commit()
        return count
    except Exception as e:
        print(f"[PG_STORAGE] bulk_upsert_name_overrides failed: {e}")
        conn.rollback()
        return 0
    finally:
        _put_conn(conn)


# ── Watchlist CRUD ───────────────────────────────────────────
# (full CRUD functions defined near bottom of file; rename lives here)

def watchlist_rename(watchlist_id: str, new_name: str) -> bool:
    """Rename a specific watchlist."""
    conn = _get_conn()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE public.watchlist SET name = %s, updated_at = NOW() WHERE id = %s",
                (new_name, watchlist_id),
            )
            conn.commit()
        return True
    except Exception as e:
        print(f"[PG_STORAGE] watchlist_rename error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


# ── Prompt History ───────────────────────────────────────────
def ph_read(user_id: str) -> dict:
    """Read all prompt history for a user."""
    conn = _get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT bucket_key, data FROM public.prompt_history WHERE user_id = %s",
            (user_id,),
        )
        result = {}
        for row in cur.fetchall():
            bucket_key, data = row
            if isinstance(data, str):
                data = json.loads(data)
            result[bucket_key] = data
        cur.close()
        return result
    except Exception as e:
        print(f"[PG_STORAGE] ph_read error for {user_id}: {e}")
        return {}
    finally:
        _put_conn(conn)
def ph_write(user_id: str, data: dict):
    """Write all prompt history for a user (full replace by bucket)."""
    conn = _get_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        for bucket_key, bucket_data in data.items():
            json_data = json.dumps(bucket_data, default=str)
            cur.execute("""
                INSERT INTO public.prompt_history (user_id, bucket_key, data, updated_at)
                VALUES (%s, %s, %s::jsonb, NOW())
                ON CONFLICT (user_id, bucket_key)
                DO UPDATE SET data = %s::jsonb, updated_at = NOW()
            """, (user_id, bucket_key, json_data, json_data))
        # Remove buckets that are no longer in data
        if data:
            cur.execute(
                "DELETE FROM public.prompt_history WHERE user_id = %s AND bucket_key != ALL(%s)",
                (user_id, list(data.keys())),
            )
        else:
            cur.execute(
                "DELETE FROM public.prompt_history WHERE user_id = %s",
                (user_id,),
            )
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"[PG_STORAGE] ph_write error for {user_id}: {e}")
        conn.rollback()
    finally:
        _put_conn(conn)
def ph_read_bucket(user_id: str, bucket_key: str) -> dict:
    """Read a single bucket for a user."""
    conn = _get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT data FROM public.prompt_history WHERE user_id = %s AND bucket_key = %s",
            (user_id, bucket_key),
        )
        row = cur.fetchone()
        cur.close()
        if row is None:
            return {}
        data = row[0]
        if isinstance(data, str):
            data = json.loads(data)
        return data
    except Exception as e:
        print(f"[PG_STORAGE] ph_read_bucket error for {user_id}/{bucket_key}: {e}")
        return {}
    finally:
        _put_conn(conn)
def ph_write_bucket(user_id: str, bucket_key: str, bucket_data: dict):
    """Write a single bucket (more efficient for single-intent updates)."""
    conn = _get_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        json_data = json.dumps(bucket_data, default=str)
        cur.execute("""
            INSERT INTO public.prompt_history (user_id, bucket_key, data, updated_at)
            VALUES (%s, %s, %s::jsonb, NOW())
            ON CONFLICT (user_id, bucket_key)
            DO UPDATE SET data = %s::jsonb, updated_at = NOW()
        """, (user_id, bucket_key, json_data, json_data))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"[PG_STORAGE] ph_write_bucket error: {e}")
        conn.rollback()
    finally:
        _put_conn(conn)


# ── Chat History ─────────────────────────────────────────────
def chat_read(conv_id: str) -> dict | None:
    """Read a single conversation."""
    return chat_get_conversation(conv_id)
def chat_write(conv_id: str, data: dict):
    """Write/update a conversation."""
    # Keep compatibility with existing callers while storing in normalized schema.
    title = data.get("title") if isinstance(data, dict) else None
    messages = data.get("messages", []) if isinstance(data, dict) else []
    chat_replace_messages(conv_id, messages=messages, title=title)
def chat_delete(conv_id: str) -> bool:
    """Delete a conversation."""
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM public.conversations WHERE id = %s", (conv_id,))
        deleted_new = cur.rowcount > 0
        # Also clean legacy row if present
        cur.execute("DELETE FROM public.chat_conversations WHERE conv_id = %s", (conv_id,))
        deleted_legacy = cur.rowcount > 0
        deleted = deleted_new or deleted_legacy
        conn.commit()
        cur.close()
        return deleted
    except Exception as e:
        print(f"[PG_STORAGE] chat_delete error for {conv_id}: {e}")
        conn.rollback()
        return False
    finally:
        _put_conn(conn)
def chat_list() -> list:
    """List all conversations (summary only), sorted by updated_at desc."""
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                c.id,
                c.title,
                c.created_at,
                c.updated_at,
                COUNT(m.id) AS message_count
            FROM public.conversations c
            LEFT JOIN public.messages m ON m.conversation_id = c.id
            GROUP BY c.id, c.title, c.created_at, c.updated_at
            ORDER BY c.updated_at DESC, c.created_at DESC
        """)
        results = [
            {
                "id": row[0],
                "title": row[1] or "",
                "created_at": row[2].isoformat() if row[2] else "",
                "updated_at": row[3].isoformat() if row[3] else "",
                "message_count": int(row[4] or 0),
            }
            for row in cur.fetchall()
        ]
        cur.close()
        return results
    except Exception as e:
        print(f"[PG_STORAGE] chat_list error: {e}")
        return []
    finally:
        _put_conn(conn)
def chat_create_conversation(conv_id: str, title: str | None = None, session_id: str | None = None) -> bool:
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.conversations (id, session_id, title, created_at, updated_at)
            VALUES (%s, %s, %s, NOW(), NOW())
            ON CONFLICT (id)
            DO UPDATE SET title = COALESCE(EXCLUDED.title, public.conversations.title), updated_at = NOW()
            """,
            (conv_id, session_id, title),
        )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[PG_STORAGE] chat_create_conversation error for {conv_id}: {e}")
        conn.rollback()
        return False
    finally:
        _put_conn(conn)
def chat_append_message(
    conv_id: str,
    role: str,
    content: str,
    message_type: str = "chat",
    structured_payload: dict | None = None,
    preset_key: str | None = None,
    model_used: str | None = None,
) -> int | None:
    """Append a message and return its BIGINT message_id, or None on failure."""
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.conversations (id, created_at, updated_at)
            VALUES (%s, NOW(), NOW())
            ON CONFLICT (id) DO NOTHING
            """,
            (conv_id,),
        )
        cur.execute(
            """
            INSERT INTO public.messages (
                conversation_id, role, message_type, content,
                structured_payload, preset_key, model_used, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            RETURNING id
            """,
            (conv_id, role, message_type or "chat", content or "", _to_jsonb(structured_payload), preset_key, model_used),
        )
        row = cur.fetchone()
        message_id = row[0] if row else None
        if role == "user":
            trimmed = (content or "").strip()
            title = (trimmed[:60] + "...") if len(trimmed) > 60 else trimmed
            if title:
                cur.execute("UPDATE public.conversations SET title = COALESCE(NULLIF(title, ''), %s), updated_at = NOW() WHERE id = %s", (title, conv_id))
            else:
                cur.execute("UPDATE public.conversations SET updated_at = NOW() WHERE id = %s", (conv_id,))
        else:
            cur.execute("UPDATE public.conversations SET updated_at = NOW() WHERE id = %s", (conv_id,))
        conn.commit()
        cur.close()
        return message_id
    except Exception as e:
        print(f"[PG_STORAGE] chat_append_message error for {conv_id}: {e}")
        conn.rollback()
        return None
    finally:
        _put_conn(conn)


def add_ticker_mentions(
    conversation_id: str,
    message_id: int | None,
    mentions: list[dict],
) -> bool:
    """
    Persist ticker mention snapshots for a given assistant message.
    Each mention dict: {ticker, mention_price, asset_type, source}
    """
    if not mentions:
        return True
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        for m in mentions:
            ticker = (m.get("ticker") or "").upper().strip()
            if not ticker:
                continue
            price = m.get("mention_price") or m.get("price")
            asset_type = m.get("asset_type")
            source = m.get("source")
            cur.execute(
                """
                INSERT INTO public.ticker_mentions
                    (conversation_id, message_id, ticker, mention_price, asset_type, source, mentioned_at, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                """,
                (conversation_id, message_id, ticker, price, asset_type, source),
            )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[PG_STORAGE] add_ticker_mentions error: {e}")
        conn.rollback()
        return False
    finally:
        _put_conn(conn)


def get_ticker_mentions_by_conv(conversation_id: str) -> list[dict]:
    """Return all ticker mention snapshots for a conversation, newest first."""
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, message_id, ticker, mentioned_at, mention_price, asset_type, source
            FROM public.ticker_mentions
            WHERE conversation_id = %s
            ORDER BY mentioned_at DESC
            """,
            (conversation_id,),
        )
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "id": r[0],
                "message_id": r[1],
                "ticker": r[2],
                "mentioned_at": r[3].isoformat() if r[3] else None,
                "mention_price": float(r[4]) if r[4] is not None else None,
                "asset_type": r[5],
                "source": r[6],
            }
            for r in rows
        ]
    except Exception as e:
        print(f"[PG_STORAGE] get_ticker_mentions_by_conv error: {e}")
        return []
    finally:
        _put_conn(conn)


def chat_list_recent(limit: int = 5) -> list:
    """Return the N most recently updated conversations (lightweight, for sidebar)."""
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                c.id,
                c.title,
                c.created_at,
                c.updated_at,
                (
                    SELECT m.model_used
                    FROM public.messages m
                    WHERE m.conversation_id = c.id
                    ORDER BY m.created_at DESC
                    LIMIT 1
                ) AS last_model_used
            FROM public.conversations c
            ORDER BY c.updated_at DESC
            LIMIT %s
            """,
            (max(1, min(limit, 50)),),
        )
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "id": r[0],
                "title": r[1] or "",
                "created_at": r[2].isoformat() if r[2] else "",
                "updated_at": r[3].isoformat() if r[3] else "",
                "last_model_used": r[4],
            }
            for r in rows
        ]
    except Exception as e:
        print(f"[PG_STORAGE] chat_list_recent error: {e}")
        return []
    finally:
        _put_conn(conn)
def chat_replace_messages(conv_id: str, messages: list, title: str | None = None) -> bool:
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.conversations (id, title, created_at, updated_at)
            VALUES (%s, %s, NOW(), NOW())
            ON CONFLICT (id) DO UPDATE SET title = COALESCE(EXCLUDED.title, public.conversations.title), updated_at = NOW()
            """,
            (conv_id, title),
        )
        cur.execute("DELETE FROM public.messages WHERE conversation_id = %s", (conv_id,))
        for msg in messages or []:
            cur.execute(
                """
                INSERT INTO public.messages (conversation_id, role, message_type, content, structured_payload, preset_key, model_used, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (
                    conv_id,
                    msg.get("role", "assistant"),
                    msg.get("message_type", "chat"),
                    msg.get("content", ""),
                    _to_jsonb(msg.get("structured_payload")),
                    msg.get("preset_key"),
                    msg.get("model_used"),
                ),
            )
        cur.execute("UPDATE public.conversations SET updated_at = NOW() WHERE id = %s", (conv_id,))
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[PG_STORAGE] chat_replace_messages error for {conv_id}: {e}")
        conn.rollback()
        return False
    finally:
        _put_conn(conn)
def chat_get_conversation(conv_id: str) -> dict | None:
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, title, created_at, updated_at FROM public.conversations WHERE id = %s", (conv_id,))
        conv_row = cur.fetchone()
        if not conv_row:
            cur.close()
            return None
        cur.execute(
            """
            SELECT id, role, message_type, content, structured_payload, preset_key, model_used, created_at
            FROM public.messages
            WHERE conversation_id = %s
            ORDER BY created_at ASC, id ASC
            """,
            (conv_id,),
        )
        messages = []
        msg_id_list = []
        for row in cur.fetchall():
            msg_id_list.append(row[0])
            messages.append({
                "id": row[0],
                "role": row[1],
                "message_type": row[2],
                "content": row[3] or "",
                "structured_payload": row[4],
                "preset_key": row[5],
                "model_used": row[6],
                "created_at": row[7].isoformat() if row[7] else None,
                "ticker_mentions": [],
            })

        # Attach ticker mentions per message (single batch query)
        if msg_id_list:
            cur.execute(
                """
                SELECT message_id, ticker, mentioned_at, mention_price, asset_type, source
                FROM public.ticker_mentions
                WHERE message_id = ANY(%s)
                ORDER BY mentioned_at ASC
                """,
                (msg_id_list,),
            )
            mentions_by_msg: dict[int, list] = {}
            for mr in cur.fetchall():
                mid = mr[0]
                mentions_by_msg.setdefault(mid, []).append({
                    "ticker": mr[1],
                    "mentioned_at": mr[2].isoformat() if mr[2] else None,
                    "mention_price": float(mr[3]) if mr[3] is not None else None,
                    "asset_type": mr[4],
                    "source": mr[5],
                })
            for msg in messages:
                msg["ticker_mentions"] = mentions_by_msg.get(msg["id"], [])

        cur.close()
        return {
            "id": conv_row[0],
            "title": conv_row[1] or "",
            "created_at": conv_row[2].isoformat() if conv_row[2] else "",
            "updated_at": conv_row[3].isoformat() if conv_row[3] else "",
            "messages": messages,
        }
    except Exception as e:
        print(f"[PG_STORAGE] chat_get_conversation error for {conv_id}: {e}")
        return None
    finally:
        _put_conn(conn)
def storage_info() -> dict:
    """Return diagnostic info about PostgreSQL storage."""
    conn = _get_conn()
    if conn is None:
        return {"available": False, "reason": "No DATABASE_URL or connection failed"}
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM public.prompt_history")
        ph_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM public.conversations")
        conv_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM public.messages")
        msg_count = cur.fetchone()[0]
        cur.close()
        return {
            "available": True,
            "prompt_history_rows": ph_count,
            "conversations": conv_count,
            "messages": msg_count,
        }
    except Exception as e:
        return {"available": False, "reason": str(e)}
    finally:
        _put_conn(conn)


# ── Watchlist Persistence ──────────────────────────────────────────

def watchlist_read(watchlist_id: str = 'default') -> dict | None:
    """Read a saved watchlist by id from PostgreSQL. Returns None if not found."""
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, name, csv_data, analysis, tickers, saved_at, updated_at FROM public.watchlist WHERE id = %s", (watchlist_id,))
        row = cur.fetchone()
        cur.close()
        if row is None:
            return None
        return {
            "id": row[0],
            "name": row[1],
            "csv_data": row[2] or [],
            "analysis": row[3] or {},
            "tickers": row[4] or [],
            "saved_at": row[5].isoformat() if hasattr(row[5], 'isoformat') else str(row[5]),
            "updated_at": row[6].isoformat() if hasattr(row[6], 'isoformat') else str(row[6]),
        }
    except Exception as e:
        print(f"[PG_STORAGE] watchlist_read error: {e}")
        return None
    finally:
        _put_conn(conn)


def watchlist_write(watchlist_id: str, name: str, csv_data: list, analysis: dict, tickers: list) -> bool:
    """Upsert a watchlist into PostgreSQL. Returns True on success."""
    conn = _get_conn()
    if conn is None:
        return False
    try:
        from psycopg2.extras import Json
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.watchlist (id, name, csv_data, analysis, tickers, saved_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                csv_data = EXCLUDED.csv_data,
                analysis = EXCLUDED.analysis,
                tickers = EXCLUDED.tickers,
                updated_at = NOW()
            """,
            (watchlist_id, name, Json(csv_data), Json(analysis), Json(tickers))
        )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[PG_STORAGE] watchlist_write error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def watchlist_delete(watchlist_id: str) -> bool:
    """Delete a specific watchlist from PostgreSQL."""
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM public.watchlist WHERE id = %s", (watchlist_id,))
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[PG_STORAGE] watchlist_delete error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


# ── Atomic single-ticker membership mutations ────────────────────────────────
# Uses pg_advisory_xact_lock to serialise concurrent add/remove on the same
# watchlist_id, preventing the lost-update race inherent in JSONB blob storage.

def _watchlist_lock_key(watchlist_id: str) -> int:
    """Stable signed-int64 advisory lock key derived from watchlist_id."""
    import hashlib
    h = int(hashlib.sha256(watchlist_id.encode()).hexdigest()[:16], 16)
    return h % (2 ** 63)


_TICKER_ROW_KEYS: tuple = ("ticker", "Ticker", "TICKER", "symbol", "Symbol", "SYMBOL")


def _sanitize_analysis(analysis: dict, ticker: str) -> dict:
    """
    Remove ticker from ALL known analysis membership arrays.
    Exact canonical match only — AIM:TRT does not match TRT.
    Handles: sections[*].tickers, top_buys, most_undervalued, best_catalysts,
    hidden_gems, most_revolutionary, right_sector, avoid_list,
    legacy categories dict.
    """
    if not analysis or not isinstance(analysis, dict):
        return analysis or {}

    def _keep(item: object) -> bool:
        if not isinstance(item, dict):
            return True
        return not any(
            str(item.get(k, "")).strip().upper() == ticker
            for k in _TICKER_ROW_KEYS
        )

    def _filter(lst: object) -> list:
        if not isinstance(lst, list):
            return lst
        return [item for item in lst if _keep(item)]

    result = dict(analysis)

    sections = result.get("sections")
    if isinstance(sections, list):
        cleaned = []
        for sec in sections:
            if isinstance(sec, dict):
                s = dict(sec)
                s["tickers"] = _filter(s.get("tickers", []))
                cleaned.append(s)
            else:
                cleaned.append(sec)
        result["sections"] = cleaned

    for key in (
        "top_buys", "most_undervalued", "best_catalysts",
        "hidden_gems", "most_revolutionary", "right_sector", "avoid_list",
    ):
        if key in result:
            result[key] = _filter(result[key])

    cats = result.get("categories")
    if isinstance(cats, dict):
        result["categories"] = {k: _filter(v) for k, v in cats.items()}

    return result


def watchlist_add_ticker(
    watchlist_id: str,
    canonical_ticker: str,
    family_aliases: list | None = None,
) -> dict:
    """
    Atomically add canonical_ticker to watchlist_id.
    Uses advisory lock to prevent concurrent lost-update.

    family_aliases: optional list of alternative canonical forms in the same
        exchange family (e.g. ['AIM:IQE'] when adding 'LON:IQE').  If any
        alias is already a Watchlist member, returns a conflict response
        (Part H: exchange-family duplicate hardening).

    Returns:
        {"added": True,  "duplicate": False, "ticker_count": N}
        {"added": False, "duplicate": True,  "ticker_count": N}
        {"added": False, "duplicate": True,  "ticker_count": N,
         "existing_ticker": "AIM:IQE", "conflict_type": "exchange_family_alias"}
        {"added": False, "error": "<reason>"}
    """
    t = canonical_ticker.strip().upper()
    if not t:
        return {"added": False, "error": "empty_ticker"}
    aliases = [a.strip().upper() for a in (family_aliases or []) if a.strip()]

    conn = _get_conn()
    if conn is None:
        return {"added": False, "error": "db_unavailable"}
    try:
        from psycopg2.extras import Json
        lock_key = _watchlist_lock_key(watchlist_id)
        cur = conn.cursor()
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (lock_key,))
        cur.execute(
            "SELECT tickers, csv_data FROM public.watchlist WHERE id = %s FOR UPDATE",
            (watchlist_id,),
        )
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            cur.close()
            return {"added": False, "error": "not_found"}

        tickers: list = list(row[0] or [])
        csv_data: list = list(row[1] or [])

        if t in tickers:
            conn.rollback()
            cur.close()
            return {"added": False, "duplicate": True, "ticker_count": len(tickers)}

        for alias in aliases:
            if alias in tickers:
                conn.rollback()
                cur.close()
                print(
                    f"[PG_STORAGE] watchlist_add_ticker: exchange-family conflict "
                    f"wl={watchlist_id} requested={t} existing_alias={alias}"
                )
                return {
                    "added":           False,
                    "duplicate":       True,
                    "ticker_count":    len(tickers),
                    "existing_ticker": alias,
                    "conflict_type":   "exchange_family_alias",
                }

        tickers.append(t)
        csv_data.append({"Symbol": t})

        cur.execute(
            """
            UPDATE public.watchlist
               SET tickers   = %s,
                   csv_data  = %s,
                   updated_at = NOW()
             WHERE id = %s
            """,
            (Json(tickers), Json(csv_data), watchlist_id),
        )
        conn.commit()
        cur.close()
        print(f"[PG_STORAGE] watchlist_add_ticker: wl={watchlist_id} ticker={t} new_count={len(tickers)}")
        return {"added": True, "duplicate": False, "ticker_count": len(tickers)}
    except Exception as e:
        print(f"[PG_STORAGE] watchlist_add_ticker error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return {"added": False, "error": str(e)}
    finally:
        _put_conn(conn)


def watchlist_remove_ticker(watchlist_id: str, canonical_ticker: str) -> dict:
    """
    Atomically remove canonical_ticker from watchlist_id.
    Sanitizes ALL persisted membership arrays so save_watchlist() cannot
    resurrect the ticker from stale csv_data or analysis.
    Exact canonical match only — AIM:TRT != TRT.
    Uses advisory lock to prevent concurrent lost-update.
    Returns:
        {"removed": True,  "ticker_count": N}
        {"removed": False, "ticker_count": N}   # already absent
        {"removed": False, "error": "<reason>"}
    """
    t = canonical_ticker.strip().upper()
    if not t:
        return {"removed": False, "error": "empty_ticker"}

    conn = _get_conn()
    if conn is None:
        return {"removed": False, "error": "db_unavailable"}
    try:
        from psycopg2.extras import Json
        lock_key = _watchlist_lock_key(watchlist_id)
        cur = conn.cursor()
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (lock_key,))
        cur.execute(
            "SELECT tickers, csv_data, analysis FROM public.watchlist WHERE id = %s FOR UPDATE",
            (watchlist_id,),
        )
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            cur.close()
            return {"removed": False, "error": "not_found"}

        tickers: list  = list(row[0] or [])
        csv_data: list = list(row[1] or [])
        analysis: dict = dict(row[2] or {})

        was_present = t in tickers

        tickers = [x for x in tickers if str(x).strip().upper() != t]

        csv_data = [
            r for r in csv_data
            if not any(
                str(r.get(k, "")).strip().upper() == t
                for k in _TICKER_ROW_KEYS
            )
        ]

        analysis = _sanitize_analysis(analysis, t)

        cur.execute(
            """
            UPDATE public.watchlist
               SET tickers    = %s,
                   csv_data   = %s,
                   analysis   = %s,
                   updated_at  = NOW()
             WHERE id = %s
            """,
            (Json(tickers), Json(csv_data), Json(analysis), watchlist_id),
        )
        conn.commit()
        cur.close()
        print(
            f"[PG_STORAGE] watchlist_remove_ticker: wl={watchlist_id} "
            f"ticker={t} was_present={was_present} new_count={len(tickers)}"
        )
        return {"removed": was_present, "ticker_count": len(tickers)}
    except Exception as e:
        print(f"[PG_STORAGE] watchlist_remove_ticker error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return {"removed": False, "error": str(e)}
    finally:
        _put_conn(conn)


def watchlist_list() -> list:
    """List all saved watchlists (metadata only, no full data)."""
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, name,
                   jsonb_array_length(COALESCE(tickers, '[]'::jsonb)) as ticker_count,
                   saved_at, updated_at
            FROM public.watchlist
            ORDER BY updated_at DESC
        """)
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "id": r[0],
                "name": r[1],
                "ticker_count": r[2],
                "saved_at": r[3].isoformat() if hasattr(r[3], 'isoformat') else str(r[3]),
                "updated_at": r[4].isoformat() if hasattr(r[4], 'isoformat') else str(r[4]),
            }
            for r in rows
        ]
    except Exception as e:
        print(f"[PG_STORAGE] watchlist_list error: {e}")
        return []
    finally:
        _put_conn(conn)


# ── Calendar Snapshots (non-Earnings catalyst tabs) ─────────────────

def _ensure_calendar_snapshots_table(cur) -> None:
    """Idempotent table creation for emergency self-heal if init_tables didn't run."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.calendar_snapshots (
            tab            TEXT PRIMARY KEY,
            current_week   JSONB NOT NULL DEFAULT '[]'::jsonb,
            previous_week  JSONB NOT NULL DEFAULT '[]'::jsonb,
            events         JSONB NOT NULL DEFAULT '[]'::jsonb,
            last_updated   TIMESTAMPTZ NULL,
            status         TEXT NOT NULL DEFAULT 'empty',
            meta           JSONB NOT NULL DEFAULT '{}'::jsonb,
            updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    cur.execute("""
        ALTER TABLE public.calendar_snapshots
        ADD COLUMN IF NOT EXISTS events JSONB NOT NULL DEFAULT '[]'::jsonb
    """)
def calendar_snapshot_read(tab: str) -> dict | None:
    """Read a single tab's snapshot row. Returns None if Neon unavailable or row missing."""
    conn = _get_conn()
    if conn is None:
        print(f"[PG_STORAGE] calendar_snapshot_read({tab}) skipped: no DB connection")
        return None
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT tab, current_week, previous_week, events, last_updated, status, meta
                FROM public.calendar_snapshots
                WHERE tab = %s
                """,
                (tab,),
            )
            row = cur.fetchone()
        except Exception as inner_e:
            # Likely undefined table — self-heal once and retry.
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[PG_STORAGE] calendar_snapshot_read self-heal triggered: {inner_e}")
            _ensure_calendar_snapshots_table(cur)
            conn.commit()
            cur.execute(
                """
                SELECT tab, current_week, previous_week, events, last_updated, status, meta
                FROM public.calendar_snapshots
                WHERE tab = %s
                """,
                (tab,),
            )
            row = cur.fetchone()
        cur.close()
        if row is None:
            return None
        cw = row[1] or []
        pw = row[2] or []
        evts = row[3] or []
        last_updated = row[4].isoformat() if row[4] else None
        return {
            "tab": row[0],
            "current_week": cw if isinstance(cw, list) else (json.loads(cw) if isinstance(cw, str) else []),
            "previous_week": pw if isinstance(pw, list) else (json.loads(pw) if isinstance(pw, str) else []),
            "events": evts if isinstance(evts, list) else (json.loads(evts) if isinstance(evts, str) else []),
            "last_updated": last_updated,
            "status": row[5] or "empty",
            "meta": row[6] or {},
        }
    except Exception as e:
        print(f"[PG_STORAGE] calendar_snapshot_read error tab={tab}: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    finally:
        _put_conn(conn)
def calendar_snapshot_write(
    tab: str,
    current_week: list,
    previous_week: list,
    last_updated: str | None,
    status: str,
    meta: dict | None = None,
    events: list | None = None,
) -> bool:
    """Upsert a tab's snapshot row. Returns True on success."""
    conn = _get_conn()
    if conn is None:
        print(f"[PG_STORAGE] calendar_snapshot_write({tab}) skipped: no DB connection")
        return False
    try:
        from psycopg2.extras import Json
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO public.calendar_snapshots
                    (tab, current_week, previous_week, events, last_updated, status, meta, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (tab) DO UPDATE SET
                    current_week  = EXCLUDED.current_week,
                    previous_week = EXCLUDED.previous_week,
                    events        = EXCLUDED.events,
                    last_updated  = EXCLUDED.last_updated,
                    status        = EXCLUDED.status,
                    meta          = EXCLUDED.meta,
                    updated_at    = NOW()
                """,
                (tab, Json(current_week or []), Json(previous_week or []),
                 Json(events or []), last_updated, status, Json(meta or {})),
            )
        except Exception as inner_e:
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[PG_STORAGE] calendar_snapshot_write self-heal triggered: {inner_e}")
            _ensure_calendar_snapshots_table(cur)
            conn.commit()
            cur.execute(
                """
                INSERT INTO public.calendar_snapshots
                    (tab, current_week, previous_week, events, last_updated, status, meta, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (tab) DO UPDATE SET
                    current_week  = EXCLUDED.current_week,
                    previous_week = EXCLUDED.previous_week,
                    events        = EXCLUDED.events,
                    last_updated  = EXCLUDED.last_updated,
                    status        = EXCLUDED.status,
                    meta          = EXCLUDED.meta,
                    updated_at    = NOW()
                """,
                (tab, Json(current_week or []), Json(previous_week or []),
                 Json(events or []), last_updated, status, Json(meta or {})),
            )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[PG_STORAGE] calendar_snapshot_write error tab={tab}: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)
# ── Strategy History Snapshots (durable restart-survival) ────────────────────

def _ensure_strategy_hist_table(cur) -> None:
    """Idempotent table creation — self-heal if init_tables didn't run."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.strategy_hist_snapshots (
            key         TEXT PRIMARY KEY,
            source      TEXT NOT NULL DEFAULT '',
            as_of       TIMESTAMPTZ NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            expires_at  TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '24 hours'),
            row_count   INTEGER NOT NULL DEFAULT 0,
            payload     JSONB NOT NULL DEFAULT '[]'::jsonb
        )
    """)


def strategy_hist_read(key: str, max_age_seconds: int | None = 86400) -> list | None:
    """Read a strategy history snapshot from Neon.

    Returns the payload list if:
      - Neon is reachable
      - a row exists for `key`
      - the row is within `max_age_seconds` of NOW()  (pass None to skip age check)

    Returns None on any failure, missing row, or age exceeded.
    """
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        try:
            if max_age_seconds is not None:
                cur.execute(
                    """
                    SELECT payload, created_at, source, row_count
                    FROM public.strategy_hist_snapshots
                    WHERE key = %s
                      AND created_at >= NOW() - (%s * INTERVAL '1 second')
                    """,
                    (key, max_age_seconds),
                )
            else:
                cur.execute(
                    """
                    SELECT payload, created_at, source, row_count
                    FROM public.strategy_hist_snapshots
                    WHERE key = %s
                    """,
                    (key,),
                )
            row = cur.fetchone()
        except Exception as inner_e:
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[PG_STORAGE] strategy_hist_read self-heal: {inner_e}")
            _ensure_strategy_hist_table(cur)
            conn.commit()
            return None
        cur.close()
        if row is None:
            return None
        payload = row[0]
        if isinstance(payload, str):
            payload = json.loads(payload)
        if not isinstance(payload, list):
            return None
        created_at_str = row[1].isoformat() if hasattr(row[1], "isoformat") else str(row[1])
        print(f"[STRATEGY_HIST_NEON] read key={key} rows={row[3]} created_at={created_at_str} source={row[2]}")
        return payload
    except Exception as e:
        print(f"[PG_STORAGE] strategy_hist_read error key={key}: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    finally:
        _put_conn(conn)


def strategy_hist_write(key: str, payload: list, source: str, row_count: int) -> bool:
    """Upsert a strategy history snapshot to Neon. Returns True on success.

    as_of is set to the date of the last element in the payload (if available).
    expires_at is set to NOW() + 24 hours.
    """
    conn = _get_conn()
    if conn is None:
        return False
    try:
        from psycopg2.extras import Json
        as_of = None
        if payload:
            last = payload[-1]
            date_str = last.get("date")
            if date_str:
                try:
                    from datetime import datetime as _dt, timezone as _tz
                    as_of = _dt.fromisoformat(date_str).replace(tzinfo=_tz.utc)
                except Exception:
                    pass
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO public.strategy_hist_snapshots
                    (key, source, as_of, created_at, expires_at, row_count, payload)
                VALUES (%s, %s, %s, NOW(), NOW() + INTERVAL '24 hours', %s, %s)
                ON CONFLICT (key) DO UPDATE SET
                    source     = EXCLUDED.source,
                    as_of      = EXCLUDED.as_of,
                    created_at = NOW(),
                    expires_at = NOW() + INTERVAL '24 hours',
                    row_count  = EXCLUDED.row_count,
                    payload    = EXCLUDED.payload
                """,
                (key, source, as_of, row_count, Json(payload)),
            )
        except Exception as inner_e:
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[PG_STORAGE] strategy_hist_write self-heal: {inner_e}")
            _ensure_strategy_hist_table(cur)
            conn.commit()
            cur.execute(
                """
                INSERT INTO public.strategy_hist_snapshots
                    (key, source, as_of, created_at, expires_at, row_count, payload)
                VALUES (%s, %s, %s, NOW(), NOW() + INTERVAL '24 hours', %s, %s)
                ON CONFLICT (key) DO UPDATE SET
                    source     = EXCLUDED.source,
                    as_of      = EXCLUDED.as_of,
                    created_at = NOW(),
                    expires_at = NOW() + INTERVAL '24 hours',
                    row_count  = EXCLUDED.row_count,
                    payload    = EXCLUDED.payload
                """,
                (key, source, as_of, row_count, Json(payload)),
            )
        conn.commit()
        cur.close()
        print(f"[STRATEGY_HIST_NEON] wrote key={key} rows={row_count} source={source}")
        return True
    except Exception as e:
        print(f"[PG_STORAGE] strategy_hist_write error key={key}: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def calendar_snapshot_get_meta_field(tab: str, field: str) -> str | None:
    """Read a single field from the meta JSON (e.g. 'last_run_week'). None if missing."""
    snap = calendar_snapshot_read(tab)
    if not snap:
        return None
    meta = snap.get("meta") or {}
    val = meta.get(field)
    return val if isinstance(val, str) or val is None else str(val)
def calendar_snapshot_set_meta_field(tab: str, field: str, value) -> bool:
    """Merge a single field into meta without rewriting current/previous_week."""
    conn = _get_conn()
    if conn is None:
        return False
    try:
        from psycopg2.extras import Json
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO public.calendar_snapshots (tab, meta, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (tab) DO UPDATE SET
                    meta = COALESCE(public.calendar_snapshots.meta, '{}'::jsonb) || EXCLUDED.meta,
                    updated_at = NOW()
                """,
                (tab, Json({field: value})),
            )
        except Exception as inner_e:
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[PG_STORAGE] calendar_snapshot_set_meta_field self-heal: {inner_e}")
            _ensure_calendar_snapshots_table(cur)
            conn.commit()
            cur.execute(
                """
                INSERT INTO public.calendar_snapshots (tab, meta, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (tab) DO UPDATE SET
                    meta = COALESCE(public.calendar_snapshots.meta, '{}'::jsonb) || EXCLUDED.meta,
                    updated_at = NOW()
                """,
                (tab, Json({field: value})),
            )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[PG_STORAGE] calendar_snapshot_set_meta_field error tab={tab}: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


# ── Watchlist Rel-Vol Rank Snapshots ─────────────────────────────────────────

def rv_snapshot_save(watchlist_id: str, current_payload: dict) -> bool:
    """
    Upsert a relative-volume rank snapshot for watchlist_id.

    On each call the existing payload column is shifted into previous_payload
    and current_payload becomes the new payload.  This gives a one-step
    previous snapshot without extra rows.

    current_payload shape: {SYMBOL_UPPER: {"rank": int, "rel_vol": float}}
    """
    import json as _json_rv
    conn = _get_conn()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.watchlist_rv_snapshots
                    (watchlist_id, payload, previous_payload,
                     captured_at, previous_captured_at)
                VALUES (%s, %s::jsonb, NULL, NOW(), NULL)
                ON CONFLICT (watchlist_id) DO UPDATE SET
                    previous_payload     = watchlist_rv_snapshots.payload,
                    previous_captured_at = watchlist_rv_snapshots.captured_at,
                    payload              = EXCLUDED.payload,
                    captured_at          = NOW()
                """,
                (watchlist_id, _json_rv.dumps(current_payload)),
            )
        conn.commit()
        return True
    except Exception as exc:
        print(f"[PG] rv_snapshot_save error wl={watchlist_id}: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def rv_snapshot_load(watchlist_id: str) -> tuple:
    """
    Load (current_payload, previous_payload) for watchlist_id.
    Either element is None when no data exists yet.
    Returns a plain tuple so callers avoid Union import.
    """
    conn = _get_conn()
    if conn is None:
        return (None, None)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload, previous_payload "
                "FROM public.watchlist_rv_snapshots "
                "WHERE watchlist_id = %s",
                (watchlist_id,),
            )
            row = cur.fetchone()
        if not row:
            return (None, None)
        current  = row[0] or {}
        previous = row[1] or {}
        return (current or None, previous or None)
    except Exception as exc:
        print(f"[PG] rv_snapshot_load error wl={watchlist_id}: {exc}")
        return (None, None)
    finally:
        _put_conn(conn)


# ── Watchlist Vol/MC Snapshots ────────────────────────────────────────────────

def volmc_snapshot_save(watchlist_id: str, current_payload: dict) -> bool:
    """
    Upsert a Vol/MC snapshot for watchlist_id.

    On each call the existing payload is shifted into previous_payload and
    current_payload becomes the new payload (one-step history, no extra rows).

    current_payload shape: {SYMBOL_UPPER: {"vol_mc_pct": float, "rank": int}}
    """
    import json as _json_vm
    conn = _get_conn()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.watchlist_volmc_snapshots
                    (watchlist_id, payload, previous_payload,
                     captured_at, previous_captured_at)
                VALUES (%s, %s::jsonb, NULL, NOW(), NULL)
                ON CONFLICT (watchlist_id) DO UPDATE SET
                    previous_payload     = watchlist_volmc_snapshots.payload,
                    previous_captured_at = watchlist_volmc_snapshots.captured_at,
                    payload              = EXCLUDED.payload,
                    captured_at          = NOW()
                """,
                (watchlist_id, _json_vm.dumps(current_payload)),
            )
        conn.commit()
        return True
    except Exception as exc:
        print(f"[PG] volmc_snapshot_save error wl={watchlist_id}: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


# ── Theme ticker overrides CRUD ────────────────────────────────────────────────

def get_theme_ticker_overrides(theme_id: str | None = None) -> list[dict]:
    """
    Return all manual theme-ticker overrides, optionally filtered by theme_id.
    Each row: {theme_id, symbol, action, source, note, created_by, created_at, updated_at}
    """
    conn = _get_conn()
    if conn is None:
        return []
    try:
        with conn.cursor() as cur:
            if theme_id:
                cur.execute(
                    "SELECT theme_id, symbol, action, source, note, created_by, "
                    "created_at, updated_at "
                    "FROM public.theme_ticker_overrides "
                    "WHERE theme_id = %s ORDER BY symbol ASC",
                    (theme_id,),
                )
            else:
                cur.execute(
                    "SELECT theme_id, symbol, action, source, note, created_by, "
                    "created_at, updated_at "
                    "FROM public.theme_ticker_overrides "
                    "ORDER BY theme_id ASC, symbol ASC"
                )
            rows = cur.fetchall()
        return [
            {
                "theme_id":   r[0],
                "symbol":     r[1],
                "action":     r[2],
                "source":     r[3],
                "note":       r[4],
                "created_by": r[5],
                "created_at": r[6].isoformat() if r[6] else None,
                "updated_at": r[7].isoformat() if r[7] else None,
            }
            for r in rows
        ]
    except Exception as exc:
        print(f"[PG] get_theme_ticker_overrides error: {exc}")
        return []
    finally:
        _put_conn(conn)


def upsert_theme_ticker_override(
    theme_id: str,
    symbol: str,
    action: str,
    source: str = "manual_admin",
    note: str | None = None,
    created_by: str | None = None,
) -> bool:
    """
    Insert or update a (theme_id, symbol) override row.
    action must be 'add' or 'remove'.
    Returns True on success.
    """
    conn = _get_conn()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.theme_ticker_overrides
                    (theme_id, symbol, action, source, note, created_by, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (theme_id, symbol) DO UPDATE SET
                    action     = EXCLUDED.action,
                    source     = EXCLUDED.source,
                    note       = EXCLUDED.note,
                    created_by = EXCLUDED.created_by,
                    updated_at = NOW()
                """,
                (theme_id, symbol, action, source, note, created_by),
            )
        conn.commit()
        return True
    except Exception as exc:
        print(f"[PG] upsert_theme_ticker_override error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def bulk_upsert_theme_ticker_overrides(edits: list[dict]) -> dict:
    """
    Bulk upsert a list of override edits.
    Each edit: {theme_id, symbol, action, source?, note?, created_by?}
    Returns {succeeded: int, failed: int, errors: list[str]}
    """
    succeeded = 0
    failed = 0
    errors: list[str] = []
    for edit in edits:
        ok = upsert_theme_ticker_override(
            theme_id=edit["theme_id"],
            symbol=edit["symbol"],
            action=edit["action"],
            source=edit.get("source", "manual_admin"),
            note=edit.get("note"),
            created_by=edit.get("created_by"),
        )
        if ok:
            succeeded += 1
        else:
            failed += 1
            errors.append(f"{edit['theme_id']}:{edit['symbol']}")
    return {"succeeded": succeeded, "failed": failed, "errors": errors}


def delete_theme_ticker_override(theme_id: str, symbol: str) -> bool:
    """
    Delete a (theme_id, symbol) override row, restoring default universe behavior
    for that pair only. Does not affect any other theme's membership for the symbol.
    Returns True if a row was deleted, False if not found or on error.
    """
    conn = _get_conn()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM public.theme_ticker_overrides "
                "WHERE theme_id = %s AND symbol = %s",
                (theme_id, symbol),
            )
            deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    except Exception as exc:
        print(f"[PG] delete_theme_ticker_override error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


# ── Theme leader overrides CRUD ────────────────────────────────────────────────

def get_all_theme_leaders() -> list[dict]:
    """
    Return all manual theme-leader overrides.
    Each row: {theme_id, leader_symbol, source, note, created_by, created_at, updated_at}
    """
    conn = _get_conn()
    if conn is None:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT theme_id, leader_symbol, source, note, created_by, "
                "created_at, updated_at "
                "FROM public.theme_leader_overrides "
                "ORDER BY theme_id ASC"
            )
            rows = cur.fetchall()
        return [
            {
                "theme_id":      r[0],
                "leader_symbol": r[1],
                "source":        r[2],
                "note":          r[3],
                "created_by":    r[4],
                "created_at":    r[5].isoformat() if r[5] else None,
                "updated_at":    r[6].isoformat() if r[6] else None,
            }
            for r in rows
        ]
    except Exception as exc:
        print(f"[PG] get_all_theme_leaders error: {exc}")
        return []
    finally:
        _put_conn(conn)


def get_theme_leaders_map() -> dict[str, dict]:
    """
    Return {theme_id: {leader_symbol, source, note}} for all themes.
    Efficient bulk read — use in _compute() to avoid per-theme queries.
    """
    rows = get_all_theme_leaders()
    return {r["theme_id"]: r for r in rows}


def upsert_theme_leader(
    theme_id: str,
    leader_symbol: str,
    source: str = "manual_admin",
    note: str | None = None,
    created_by: str | None = None,
) -> bool:
    """
    Insert or update the manual leader for a theme (PK = theme_id, one per theme).
    Returns True on success.
    """
    conn = _get_conn()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.theme_leader_overrides
                    (theme_id, leader_symbol, source, note, created_by, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (theme_id) DO UPDATE SET
                    leader_symbol = EXCLUDED.leader_symbol,
                    source        = EXCLUDED.source,
                    note          = EXCLUDED.note,
                    created_by    = EXCLUDED.created_by,
                    updated_at    = NOW()
                """,
                (theme_id, leader_symbol, source, note, created_by),
            )
        conn.commit()
        return True
    except Exception as exc:
        print(f"[PG] upsert_theme_leader error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def delete_theme_leader(theme_id: str) -> bool:
    """
    Remove the manual leader override for a theme.
    Returns True if a row was deleted, False if none existed.
    """
    conn = _get_conn()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM public.theme_leader_overrides WHERE theme_id = %s",
                (theme_id,),
            )
            deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    except Exception as exc:
        print(f"[PG] delete_theme_leader error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def volmc_snapshot_load(watchlist_id: str) -> tuple:
    """
    Load (current_payload, previous_payload) from watchlist_volmc_snapshots.
    Either element is None when no data exists yet.
    """
    conn = _get_conn()
    if conn is None:
        return (None, None)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload, previous_payload "
                "FROM public.watchlist_volmc_snapshots "
                "WHERE watchlist_id = %s",
                (watchlist_id,),
            )
            row = cur.fetchone()
        if not row:
            return (None, None)
        current  = row[0] or {}
        previous = row[1] or {}
        return (current or None, previous or None)
    except Exception as exc:
        print(f"[PG] volmc_snapshot_load error wl={watchlist_id}: {exc}")
        return (None, None)
    finally:
        _put_conn(conn)


# ── Trading Dashboard LKG (Neon backing for in-memory canonical cache) ─────────

_TRADING_DASHBOARD_LKG_KEY = "trading_dashboard_lkg"


def _ensure_trading_dashboard_lkg_table(cur) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.trading_dashboard_lkg (
            key          TEXT PRIMARY KEY,
            mode         TEXT NOT NULL DEFAULT 'swing',
            as_of        TIMESTAMPTZ NULL,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            expires_at   TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '24 hours'),
            schema_version INTEGER NOT NULL DEFAULT 1,
            payload      JSONB NOT NULL DEFAULT '{}'::jsonb
        )
    """)


def trading_dashboard_lkg_write(mode: str, payload: dict) -> bool:
    conn = _get_conn()
    if conn is None:
        return False
    try:
        from psycopg2.extras import Json
        as_of_str = payload.get("as_of")
        as_of_val = None
        if as_of_str:
            try:
                from datetime import datetime as _dt, timezone as _tz
                as_of_val = _dt.fromisoformat(as_of_str)
            except Exception:
                pass

        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO public.trading_dashboard_lkg
                    (key, mode, as_of, created_at, expires_at, schema_version, payload)
                VALUES (%s, %s, %s, NOW(), NOW() + INTERVAL '24 hours', %s, %s)
                ON CONFLICT (key) DO UPDATE SET
                    mode           = EXCLUDED.mode,
                    as_of          = EXCLUDED.as_of,
                    created_at     = NOW(),
                    expires_at     = NOW() + INTERVAL '24 hours',
                    schema_version = EXCLUDED.schema_version,
                    payload        = EXCLUDED.payload
                """,
                (_TRADING_DASHBOARD_LKG_KEY, mode, as_of_val, 1, Json(payload)),
            )
            conn.commit()
            return True
        except Exception as inner_e:
            try:
                conn.rollback()
            except Exception:
                pass
            _ensure_trading_dashboard_lkg_table(cur)
            conn.commit()
            try:
                cur.execute(
                    """
                    INSERT INTO public.trading_dashboard_lkg
                        (key, mode, as_of, created_at, expires_at, schema_version, payload)
                    VALUES (%s, %s, %s, NOW(), NOW() + INTERVAL '24 hours', %s, %s)
                    """,
                    (_TRADING_DASHBOARD_LKG_KEY, mode, as_of_val, 1, Json(payload)),
                )
                conn.commit()
                return True
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                return False
        cur.close()
    except Exception as e:
        print(f"[PG_STORAGE] trading_dashboard_lkg_write error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


def trading_dashboard_lkg_read(max_age_seconds: int | None = 86400) -> dict | None:
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        try:
            if max_age_seconds is not None:
                cur.execute(
                    """
                    SELECT payload, mode, as_of, created_at, schema_version
                    FROM public.trading_dashboard_lkg
                    WHERE key = %s
                      AND created_at >= NOW() - (%s * INTERVAL '1 second')
                    """,
                    (_TRADING_DASHBOARD_LKG_KEY, max_age_seconds),
                )
            else:
                cur.execute(
                    """
                    SELECT payload, mode, as_of, created_at, schema_version
                    FROM public.trading_dashboard_lkg
                    WHERE key = %s
                    """,
                    (_TRADING_DASHBOARD_LKG_KEY,),
                )
            row = cur.fetchone()
        except Exception as inner_e:
            try:
                conn.rollback()
            except Exception:
                pass
            _ensure_trading_dashboard_lkg_table(cur)
            conn.commit()
            return None
        cur.close()
        if row is None:
            return None
        payload = row[0]
        if isinstance(payload, str):
            import json as _json
            payload = _json.loads(payload)
        if not isinstance(payload, dict):
            return None
        created_at_str = row[3].isoformat() if hasattr(row[3], "isoformat") else str(row[3])
        print(
            f"[TRADING_DASHBOARD_LKG] read key={_TRADING_DASHBOARD_LKG_KEY} "
            f"mode={row[1]} as_of={row[2]} created_at={created_at_str}"
        )
        return {
            "payload": payload,
            "mode": row[1],
            "as_of": row[2],
            "created_at": row[3],
        }
    except Exception as e:
        print(f"[PG_STORAGE] trading_dashboard_lkg_read error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    finally:
        _put_conn(conn)
