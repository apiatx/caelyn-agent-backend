"""
RSS Article Archive — Neon/PostgreSQL persistence for the Watchlist RSS sweeper.

Stores unique (ticker, article_key) associations with 72-hour rolling retention.
Only RSS articles (rss_providers non-empty) enter the archive. FMP fallback
articles are excluded so they never contribute to activity counts.

DB pattern follows whale_watch_service.py:
  - Own _get_conn / _put_conn / SimpleConnectionPool(1, 5)
  - CREATE TABLE IF NOT EXISTS + idempotent ALTER TABLE migrations in ensure_table()
  - commit/rollback/finally in every write function

Write-amplification guard:
  - _SEEN_CACHE: in-memory dict {ticker -> {article_key -> frozenset(providers)}}
  - Warmed from Neon at sweeper startup and after every prune
  - Each sweep: new keys → INSERT, changed providers → UPDATE, unchanged → skip entirely
"""

from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

try:
    import psycopg2
    from psycopg2 import pool as _pg_pool
    from psycopg2.extras import execute_values
    _PSYCOPG2_OK = True
except ImportError:
    _PSYCOPG2_OK = False


# ── Connection pool ───────────────────────────────────────────────────────────

def _sanitize_url(url: str | None) -> str | None:
    if not url:
        return url
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query, keep_blank_values=True)
        qs.pop("channel_binding", None)
        if "connect_timeout" not in qs:
            qs["connect_timeout"] = ["10"]
        url = urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))
    except Exception:
        pass
    return url


_DB_URL = _sanitize_url(os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL"))
_pool: Any = None


def _get_conn():
    global _pool
    if not _DB_URL or not _PSYCOPG2_OK:
        return None
    for _ in range(2):
        if _pool is None:
            try:
                _pool = _pg_pool.SimpleConnectionPool(1, 5, _DB_URL)
            except Exception as e:
                print(f"[RSS_ARCHIVE] pool creation failed: {e}")
                return None
        try:
            conn = _pool.getconn()
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            conn.commit()
            return conn
        except Exception:
            try:
                _pool.closeall()
            except Exception:
                pass
            _pool = None
    return None


def _put_conn(conn):
    if _pool and conn:
        try:
            _pool.putconn(conn)
        except Exception:
            pass


# ── In-memory seen cache (write-amplification guard) ─────────────────────────
#
# {ticker: {article_key: frozenset(rss_providers)}}
#
# Warmed from Neon on sweeper startup and after every prune.
# During each sweep, only rows absent from or changed in this cache reach Neon.

_SEEN_CACHE: dict[str, dict[str, frozenset]] = {}
_CACHE_WARMED: bool = False


# ── Schema ────────────────────────────────────────────────────────────────────

def ensure_table() -> bool:
    """
    Create watchlist_rss_article_archive table and indexes if not present.
    Also runs idempotent column migrations (summary column added post-initial).
    """
    conn = _get_conn()
    if conn is None:
        print("[RSS_ARCHIVE] ensure_table: no DB connection")
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.watchlist_rss_article_archive (
                ticker          TEXT NOT NULL,
                article_key     TEXT NOT NULL,
                title           TEXT NOT NULL,
                source          TEXT,
                url             TEXT,
                published_at    TIMESTAMPTZ NOT NULL,
                rss_providers   TEXT[] NOT NULL DEFAULT '{}',
                first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (ticker, article_key)
            )
        """)
        # Idempotent migration: add summary column for full-text hyperscaler scoring
        cur.execute("""
            ALTER TABLE public.watchlist_rss_article_archive
            ADD COLUMN IF NOT EXISTS summary TEXT
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_wl_rss_archive_ticker_pub
            ON public.watchlist_rss_article_archive (ticker, published_at DESC)
        """)
        conn.commit()
        cur.close()
        print("[RSS_ARCHIVE] ensure_table: OK")
        return True
    except Exception as e:
        conn.rollback()
        print(f"[RSS_ARCHIVE] ensure_table error: {e}")
        return False
    finally:
        _put_conn(conn)


# ── Timestamp parsing ─────────────────────────────────────────────────────────

def _parse_pub_dt(published_at: str) -> datetime | None:
    """Parse RFC-2822 or ISO-8601 pubDate string to timezone-aware UTC datetime."""
    if not published_at:
        return None
    try:
        return parsedate_to_datetime(published_at).astimezone(timezone.utc)
    except Exception:
        pass
    try:
        return datetime.fromisoformat(
            published_at.replace("Z", "+00:00")
        ).astimezone(timezone.utc)
    except Exception:
        pass
    return None


# ── In-memory cache management ────────────────────────────────────────────────

def warm_seen_cache() -> int:
    """
    Load all (ticker, article_key, rss_providers) rows from Neon into
    the in-memory seen cache. Safe to call multiple times — replaces cache
    entirely on each call.

    Called:
      • once at sweeper startup (before first sweep)
      • once after each prune run (cache reconciliation)

    Returns number of rows loaded.
    """
    global _SEEN_CACHE, _CACHE_WARMED
    conn = _get_conn()
    if conn is None:
        return 0
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT ticker, article_key, rss_providers "
            "FROM public.watchlist_rss_article_archive"
        )
        rows = cur.fetchall()
        cur.close()
        new_cache: dict[str, dict[str, frozenset]] = {}
        for ticker, art_key, providers in rows:
            if ticker not in new_cache:
                new_cache[ticker] = {}
            new_cache[ticker][art_key] = frozenset(providers or [])
        _SEEN_CACHE  = new_cache
        _CACHE_WARMED = True
        return len(rows)
    except Exception as e:
        print(f"[RSS_ARCHIVE] warm_seen_cache error: {e}")
        return 0
    finally:
        _put_conn(conn)


# ── Write path (cache-aware, no write amplification) ─────────────────────────

def upsert_with_cache(ticker: str, articles: list[dict]) -> dict:
    """
    Diff-aware archive writer.

    For each valid article (rss_providers non-empty, article_key set, parseable pubdate):
      • NEW article key → INSERT into Neon, add to seen cache
      • Existing key, UNCHANGED providers → skip entirely (no DB write)
      • Existing key, NEW provider surfaced → UPDATE rss_providers only, update cache

    Returns a diagnostic dict with granular counts.
    FMP articles (rss_providers=[]) are silently excluded.
    """
    ticker_cache = _SEEN_CACHE.setdefault(ticker, {})

    to_insert: list[tuple] = []   # full row tuples for new articles
    to_update: list[tuple] = []   # (ticker, article_key, new_providers) for provider-changed rows
    skipped   = 0
    observed  = 0

    for a in articles:
        providers = a.get("rss_providers") or []
        if not providers:
            continue
        art_key = a.get("_article_key") or ""
        if not art_key:
            continue
        pub_dt = _parse_pub_dt(a.get("published_at") or "")
        if pub_dt is None:
            continue

        observed     += 1
        provider_set  = frozenset(providers)
        cached        = ticker_cache.get(art_key)

        if cached is None:
            # Brand-new article — INSERT
            to_insert.append((
                ticker,
                art_key,
                (a.get("title")   or "")[:500],
                (a.get("source")  or "")[:200],
                (a.get("url")     or "")[:1000],
                pub_dt,
                list(provider_set),
                (a.get("summary") or "")[:500],
            ))
            ticker_cache[art_key] = provider_set

        elif provider_set == cached:
            # Identical provider set — skip entirely
            skipped += 1

        else:
            # Provider set grew (same article now seen in both feeds)
            merged_set = cached | provider_set
            to_update.append((ticker, art_key, list(merged_set)))
            ticker_cache[art_key] = merged_set

    inserted  = 0
    updated   = 0

    if not to_insert and not to_update:
        return {
            "articles_observed":                 observed,
            "db_insert_attempts":                0,
            "db_update_attempts":                0,
            "new_rows_inserted":                 0,
            "provider_sets_updated":             0,
            "unchanged_existing_articles_skipped": skipped,
        }

    conn = _get_conn()
    if conn is None:
        return {
            "articles_observed":                 observed,
            "db_insert_attempts":                len(to_insert),
            "db_update_attempts":                len(to_update),
            "new_rows_inserted":                 0,
            "provider_sets_updated":             0,
            "unchanged_existing_articles_skipped": skipped,
            "error": "no_db_connection",
        }

    try:
        cur = conn.cursor()

        if to_insert:
            execute_values(
                cur,
                """
                INSERT INTO public.watchlist_rss_article_archive
                    (ticker, article_key, title, source, url, published_at,
                     rss_providers, first_seen_at, last_seen_at, summary)
                VALUES %s
                ON CONFLICT (ticker, article_key) DO NOTHING
                """,
                to_insert,
                template="(%s, %s, %s, %s, %s, %s, %s::TEXT[], NOW(), NOW(), %s)",
            )
            inserted = cur.rowcount if cur.rowcount >= 0 else len(to_insert)

        if to_update:
            for (t, key, merged_list) in to_update:
                cur.execute(
                    """
                    UPDATE public.watchlist_rss_article_archive
                    SET rss_providers = %s::TEXT[], last_seen_at = NOW()
                    WHERE ticker = %s AND article_key = %s
                    """,
                    (merged_list, t, key),
                )
            updated = len(to_update)

        conn.commit()
        cur.close()

    except Exception as e:
        conn.rollback()
        print(f"[RSS_ARCHIVE] upsert_with_cache error ticker={ticker}: {e}")
        # Evict in-flight cache updates on error to avoid stale skips
        for a in articles:
            art_key = a.get("_article_key") or ""
            if art_key in ticker_cache:
                ticker_cache.pop(art_key, None)
    finally:
        _put_conn(conn)

    return {
        "articles_observed":                 observed,
        "db_insert_attempts":                len(to_insert),
        "db_update_attempts":                len(to_update),
        "new_rows_inserted":                 inserted,
        "provider_sets_updated":             updated,
        "unchanged_existing_articles_skipped": skipped,
    }


# ── Read path ─────────────────────────────────────────────────────────────────

def query_ticker_activity(tickers: list[str]) -> dict[str, dict]:
    """
    For each ticker return:
        articles_24h          int    — articles published in [now-24h, now]
        previous_articles_24h int    — articles published in [now-48h, now-24h)
        first_seen_ts         float  — unix ts of MIN(first_seen_at): when the
                                       collector FIRST observed this ticker.
                                       Used by coverage_status to distinguish
                                       "collector has 48h history" from
                                       "RSS feed has old articles".

    Runs a single query across all tickers.
    """
    if not tickers:
        return {}

    conn = _get_conn()
    if conn is None:
        return {}

    now   = datetime.now(timezone.utc)
    t_24h = now - timedelta(hours=24)
    t_48h = now - timedelta(hours=48)

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                ticker,
                COUNT(*) FILTER (WHERE published_at >= %s AND published_at <= %s) AS articles_24h,
                COUNT(*) FILTER (WHERE published_at >= %s AND published_at < %s)  AS previous_24h,
                MIN(first_seen_at) AS first_seen
            FROM public.watchlist_rss_article_archive
            WHERE ticker = ANY(%s)
            GROUP BY ticker
            """,
            (t_24h, now, t_48h, t_24h, list(tickers)),
        )
        result: dict[str, dict] = {}
        for row in cur.fetchall():
            t, cur_24, prev_24, first_seen = row
            result[t] = {
                "articles_24h":          int(cur_24 or 0),
                "previous_articles_24h": int(prev_24 or 0),
                # first_seen_at = time the collector FIRST wrote data for this ticker
                # (not the article's publication date).
                "first_seen_ts": first_seen.timestamp() if first_seen else None,
            }
        cur.close()
        return result
    except Exception as e:
        print(f"[RSS_ARCHIVE] query_ticker_activity error: {e}")
        return {}
    finally:
        _put_conn(conn)


def query_recent_articles_for_scoring(
    tickers: list[str],
    hours: int = 24,
) -> dict[str, list[dict]]:
    """
    Return full article records for the last `hours` hours for hyperscaler scoring.

    Returns {ticker: [article_dict, ...]} where each article_dict has the fields
    expected by score_article():  title, summary, url, published_at, source,
    rss_providers, _article_key.

    NOT limited to 15 articles per ticker — returns the full untruncated surface.
    Used by _build_hyperscaler_articles to score articles that may lie beyond
    the [:15] truncation in the normal display path.
    """
    if not tickers:
        return {}

    conn = _get_conn()
    if conn is None:
        return {}

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                ticker, article_key, title, COALESCE(summary, '') AS summary,
                COALESCE(source, '') AS source, COALESCE(url, '') AS url,
                published_at, rss_providers
            FROM public.watchlist_rss_article_archive
            WHERE ticker = ANY(%s)
              AND published_at >= %s
            ORDER BY ticker, published_at DESC
            """,
            (list(tickers), cutoff),
        )
        result: dict[str, list[dict]] = {}
        for row in cur.fetchall():
            t, art_key, title, summary, source, url, pub_at, providers = row
            pub_str = pub_at.isoformat() if pub_at else ""
            if t not in result:
                result[t] = []
            result[t].append({
                "title":         title or "",
                "summary":       summary or "",
                "source":        source or "",
                "url":           url or "",
                "published_at":  pub_str,
                "rss_providers": list(providers or []),
                "_article_key":  art_key,
            })
        cur.close()
        return result
    except Exception as e:
        print(f"[RSS_ARCHIVE] query_recent_articles_for_scoring error: {e}")
        return {}
    finally:
        _put_conn(conn)


def count_all_rows() -> int:
    """Return total row count (used for prune diagnostics)."""
    conn = _get_conn()
    if conn is None:
        return 0
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM public.watchlist_rss_article_archive")
        row = cur.fetchone()
        cur.close()
        return int(row[0]) if row else 0
    except Exception:
        return 0
    finally:
        _put_conn(conn)


def prune_old_rows(retention_hours: int = 72) -> int:
    """
    Delete rows where published_at < NOW() - retention_hours.
    Re-warms the in-memory seen cache after deletion so stale keys
    are removed and won't prevent re-insertion of new articles.
    Returns number of rows deleted.
    """
    conn = _get_conn()
    if conn is None:
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(hours=retention_hours)
    deleted = 0
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM public.watchlist_rss_article_archive WHERE published_at < %s",
            (cutoff,),
        )
        deleted = cur.rowcount
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
        print(f"[RSS_ARCHIVE] prune error: {e}")
    finally:
        _put_conn(conn)

    if deleted > 0:
        # Reconcile in-memory cache — remove pruned keys
        warm_seen_cache()

    return deleted
