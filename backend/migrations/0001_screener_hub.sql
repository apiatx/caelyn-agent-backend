-- ============================================================================
-- Screener Hub — initial schema
-- ============================================================================
-- 4 tables backing the Caelyn Screener Hub feature.
-- Idempotent: uses CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS so
-- it is safe to re-run on existing Neon databases.
--
-- Apply on Replit / Neon:
--   psql "$NEON_DATABASE_URL" -f backend/migrations/0001_screener_hub.sql
-- ============================================================================

-- 1) Fundamentals cache (weekly TTL, FMP-sourced) ---------------------------
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

-- 2) Universe snapshots (per-tab + per-theme) -------------------------------
CREATE TABLE IF NOT EXISTS public.screener_universe_snapshots (
    id             BIGSERIAL PRIMARY KEY,
    universe_type  TEXT NOT NULL,            -- thematic|social|bottlenecks|watchlist_portfolio
    theme_key      TEXT NULL,                -- nullable; populated for thematic rows
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

-- 3) Page-aware quote cache (Tradier) ---------------------------------------
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

-- 4) Job runs (audit trail for warm jobs / rebuilds) ------------------------
CREATE TABLE IF NOT EXISTS public.screener_job_runs (
    id                 BIGSERIAL PRIMARY KEY,
    job_name           TEXT NOT NULL,
    started_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at        TIMESTAMPTZ NULL,
    status             TEXT NOT NULL DEFAULT 'running',  -- running|ok|partial|failed
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
