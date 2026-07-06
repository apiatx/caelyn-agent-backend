"""
RSS Article Archive — Neon/PostgreSQL persistence for the Watchlist RSS sweeper.

Stores unique (ticker, article_key) associations with 72-hour rolling retention.
Only RSS articles (rss_providers non-empty) enter the archive. FMP fallback
articles are excluded so they never contribute to activity counts.

DB pattern follows whale_watch_service.py:
  - Own _get_conn / _put_conn / SimpleConnectionPool(1, 5)
  - CREATE TABLE IF NOT EXISTS in ensure_table()
  - commit/rollback/finally in every write function
"""

from __future__ import annotations

import json
import os
import time
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


# ── Schema ────────────────────────────────────────────────────────────────────

def ensure_table() -> bool:
    """Create watchlist_rss_article_archive table if it does not exist."""
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
    # RFC-2822 (Yahoo/Google RSS format)
    try:
        return parsedate_to_datetime(published_at).astimezone(timezone.utc)
    except Exception:
        pass
    # ISO-8601
    try:
        return datetime.fromisoformat(
            published_at.replace("Z", "+00:00")
        ).astimezone(timezone.utc)
    except Exception:
        pass
    return None


# ── Write path ────────────────────────────────────────────────────────────────

def upsert_article_associations(ticker: str, articles: list[dict]) -> int:
    """
    UPSERT (ticker, article_key) rows.

    Only articles with rss_providers non-empty are inserted (FMP fallback
    articles — rss_providers=[] or absent — are excluded).

    On conflict: update last_seen_at and union the rss_providers array.

    Returns number of rows upserted.
    """
    conn = _get_conn()
    if conn is None:
        return 0

    rows = []
    for a in articles:
        providers = a.get("rss_providers") or []
        if not providers:
            continue  # exclude FMP / Perplexity fallback articles

        art_key = a.get("_article_key") or ""
        if not art_key:
            continue

        pub_dt = _parse_pub_dt(a.get("published_at") or "")
        if pub_dt is None:
            continue

        rows.append((
            ticker,
            art_key,
            (a.get("title") or "")[:500],
            (a.get("source") or "")[:200],
            (a.get("url") or "")[:1000],
            pub_dt,
            list(providers),
        ))

    if not rows:
        _put_conn(conn)
        return 0

    try:
        cur = conn.cursor()
        # Build VALUES list for execute_values
        execute_values(
            cur,
            """
            INSERT INTO public.watchlist_rss_article_archive
                (ticker, article_key, title, source, url, published_at, rss_providers,
                 first_seen_at, last_seen_at)
            VALUES %s
            ON CONFLICT (ticker, article_key) DO UPDATE SET
                last_seen_at  = NOW(),
                rss_providers = (
                    SELECT array_agg(DISTINCT elem)
                    FROM unnest(
                        watchlist_rss_article_archive.rss_providers || EXCLUDED.rss_providers
                    ) AS elem
                )
            """,
            rows,
            template="(%s, %s, %s, %s, %s, %s, %s::TEXT[], NOW(), NOW())",
        )
        n = cur.rowcount
        conn.commit()
        cur.close()
        return max(n, len(rows))  # rowcount may be 0 for ON CONFLICT DO UPDATE in some psycopg2 versions
    except Exception as e:
        conn.rollback()
        print(f"[RSS_ARCHIVE] upsert error ticker={ticker}: {e}")
        return 0
    finally:
        _put_conn(conn)


# ── Read path ─────────────────────────────────────────────────────────────────

def query_ticker_activity(tickers: list[str]) -> dict[str, dict]:
    """
    For each ticker return:
        articles_24h          int  (published in [now-24h, now])
        previous_articles_24h int  (published in [now-48h, now-24h))
        oldest_pub_ts         float | None  (oldest published_at unix ts seen)

    Runs a single query across all tickers.
    """
    if not tickers:
        return {}

    conn = _get_conn()
    if conn is None:
        return {}

    now = datetime.now(timezone.utc)
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
                MIN(published_at) AS oldest_pub
            FROM public.watchlist_rss_article_archive
            WHERE ticker = ANY(%s)
            GROUP BY ticker
            """,
            (t_24h, now, t_48h, t_24h, list(tickers)),
        )
        result: dict[str, dict] = {}
        for row in cur.fetchall():
            t, cur_24, prev_24, oldest = row
            result[t] = {
                "articles_24h":          int(cur_24 or 0),
                "previous_articles_24h": int(prev_24 or 0),
                "oldest_pub_ts": oldest.timestamp() if oldest else None,
            }
        cur.close()
        return result
    except Exception as e:
        print(f"[RSS_ARCHIVE] query_ticker_activity error: {e}")
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
    Returns number of rows deleted.
    """
    conn = _get_conn()
    if conn is None:
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(hours=retention_hours)
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM public.watchlist_rss_article_archive WHERE published_at < %s",
            (cutoff,),
        )
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        return deleted
    except Exception as e:
        conn.rollback()
        print(f"[RSS_ARCHIVE] prune error: {e}")
        return 0
    finally:
        _put_conn(conn)
