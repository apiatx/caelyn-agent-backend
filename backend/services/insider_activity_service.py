"""
Insider Activity Dashboard — Backend Service
Fetches SEC Form 4 filings via edgartools, scores each transaction (0-100),
stores results in Neon PostgreSQL with 30-day retention, and exposes REST API.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import httpx
import psycopg2
from psycopg2 import pool as _pg_pool
from psycopg2.extras import Json, execute_batch
from fastapi import APIRouter, Query, HTTPException, BackgroundTasks, Request, Depends
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(__file__)))
from subscription import require_subscription
from pydantic import BaseModel

try:
    from edgar import get_filings, set_identity
    set_identity("CaelynAI Trading Dashboard admin@caelynai.com")
    _EDGAR_AVAILABLE = True
except ImportError:
    _EDGAR_AVAILABLE = False
    logging.warning("[INSIDER] edgartools not installed — fetch will be disabled")

logger = logging.getLogger("insider_activity")

# ── Constants ────────────────────────────────────────────────────────────────

_TRADIER_KEY = os.getenv("TRADIER_API_KEY", "")
_FINNHUB_KEY = os.getenv("FINNHUB_API_KEY", "")
_PPLX_KEY = os.getenv("PERPLEXITY_API_KEY", "")
_PPLX_MODEL = "sonar-pro"
_PPLX_BASE_URL = "https://api.perplexity.ai"
_SEC_DELAY = 0.15          # seconds between SEC requests (max 10 req/s)
_FETCH_INTERVAL = 7200     # 2 hours in seconds
_AI_INTERVAL = 86400       # 24 hours — Perplexity daily analysis
_RETENTION_DAYS = 30
_BATCH_SIZE = 50           # filings processed per batch
_KEEP_CODES = {"P", "S", "M", "A", "D", "G"}  # transaction codes to keep

# Scoring weights (adjustable)
_W_SIZE = 15
_W_ROLE = 20
_W_TYPE = 10
_W_CONTEXT = 15
_W_CLUSTER = 15
_W_POSITION = 10
_W_TRACK = 10
_W_EVENT = 5

# ── Thread pool for sync DB / blocking work ──────────────────────────────────
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="insider")

# ── DB Pool ──────────────────────────────────────────────────────────────────

def _sanitize_db_url(url: str | None) -> str | None:
    if not url:
        return url
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query, keep_blank_values=True)
        qs.pop("channel_binding", None)
        url = urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))
    except Exception:
        pass
    return url


_DB_URL = _sanitize_db_url(os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL"))
_pool: _pg_pool.SimpleConnectionPool | None = None


def _get_conn():
    global _pool
    if not _DB_URL:
        return None
    for _ in range(2):
        if _pool is None:
            try:
                _pool = _pg_pool.SimpleConnectionPool(1, 5, _DB_URL)
            except Exception as e:
                logger.error("[INSIDER_DB] Pool creation failed: %s", e)
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


# ── DB Setup ─────────────────────────────────────────────────────────────────

def _create_table():
    conn = _get_conn()
    if not conn:
        logger.warning("[INSIDER_DB] No DB connection — skipping table creation")
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS insider_transactions (
                id                 SERIAL PRIMARY KEY,
                accession_number   VARCHAR(30) UNIQUE NOT NULL,
                ticker             VARCHAR(10),
                company_name       VARCHAR(255),
                insider_name       VARCHAR(255),
                insider_title      VARCHAR(100),
                transaction_type   VARCHAR(20),
                transaction_code   VARCHAR(5),
                transaction_date   DATE,
                filing_date        TIMESTAMP,
                shares             BIGINT,
                price_per_share    DECIMAL(12,4),
                total_value        DECIMAL(16,2),
                shares_owned_after BIGINT,
                ownership_type     VARCHAR(10),
                conviction_score   INTEGER,
                score_breakdown    JSONB,
                price_context      JSONB,
                cluster_id         INTEGER,
                context_tags       TEXT[],
                cluster_type       VARCHAR(30),
                cluster_metadata   JSONB,
                sector             VARCHAR(100),
                created_at         TIMESTAMP DEFAULT NOW(),
                expires_at         TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_it_ticker_date
            ON insider_transactions (ticker, transaction_date DESC)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_it_score
            ON insider_transactions (conviction_score DESC)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_it_expires
            ON insider_transactions (expires_at)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_it_cluster
            ON insider_transactions (cluster_id)
        """)
        for col, defn in [
            ("cluster_type", "VARCHAR(30)"),
            ("cluster_metadata", "JSONB"),
        ]:
            cur.execute(
                f"ALTER TABLE insider_transactions ADD COLUMN IF NOT EXISTS {col} {defn}"
            )
        conn.commit()
        cur.close()
        logger.info("[INSIDER_DB] Table ready")
    except Exception as e:
        logger.error("[INSIDER_DB] Table creation error: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        _put_conn(conn)


def _cleanup_expired():
    conn = _get_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM insider_transactions WHERE expires_at < NOW()")
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        if deleted:
            logger.info("[INSIDER_DB] Cleaned up %d expired rows", deleted)
    except Exception as e:
        logger.error("[INSIDER_DB] Cleanup error: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        _put_conn(conn)


def _create_ai_cache_table():
    conn = _get_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS insider_ai_cache (
                id           SERIAL PRIMARY KEY,
                cache_date   DATE NOT NULL UNIQUE,
                result       JSONB NOT NULL,
                model        VARCHAR(50) DEFAULT 'sonar-pro',
                created_at   TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_aic_date ON insider_ai_cache (cache_date DESC)
        """)
        cur.execute("""
            DELETE FROM insider_ai_cache WHERE cache_date < NOW() - INTERVAL '7 days'
        """)
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error("[INSIDER_AI] Cache table error: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        _put_conn(conn)


def _load_ai_cache_from_db() -> dict | None:
    conn = _get_conn()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT result, created_at FROM insider_ai_cache
            WHERE cache_date = CURRENT_DATE
            ORDER BY created_at DESC LIMIT 1
        """)
        row = cur.fetchone()
        cur.close()
        if row:
            result = row[0] if isinstance(row[0], dict) else json.loads(row[0])
            result["cached_at"] = row[1].isoformat() if hasattr(row[1], "isoformat") else str(row[1])
            return result
        return None
    except Exception as e:
        logger.debug("[INSIDER_AI] Cache load error: %s", e)
        return None
    finally:
        _put_conn(conn)


def _save_ai_cache_to_db(result: dict):
    conn = _get_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO insider_ai_cache (cache_date, result, model)
            VALUES (CURRENT_DATE, %s, %s)
            ON CONFLICT (cache_date) DO UPDATE SET result = EXCLUDED.result, created_at = NOW()
        """, (Json(result), _PPLX_MODEL))
        conn.commit()
        cur.close()
        logger.info("[INSIDER_AI] Saved analysis to DB cache for today")
    except Exception as e:
        logger.error("[INSIDER_AI] Cache save error: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        _put_conn(conn)


def _get_existing_accessions(accessions: list[str]) -> set[str]:
    if not accessions:
        return set()
    conn = _get_conn()
    if not conn:
        return set()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT accession_number FROM insider_transactions WHERE accession_number = ANY(%s)",
            (accessions,)
        )
        result = {row[0] for row in cur.fetchall()}
        cur.close()
        return result
    except Exception:
        return set()
    finally:
        _put_conn(conn)


def _count_cluster(ticker: str, tx_date: date) -> int:
    conn = _get_conn()
    if not conn:
        return 1
    try:
        cur = conn.cursor()
        window_start = tx_date - timedelta(days=14)
        window_end = tx_date + timedelta(days=14)
        cur.execute("""
            SELECT COUNT(DISTINCT insider_name)
            FROM insider_transactions
            WHERE ticker = %s
              AND transaction_date BETWEEN %s AND %s
        """, (ticker, window_start, window_end))
        row = cur.fetchone()
        cur.close()
        return int(row[0]) if row else 1
    except Exception:
        return 1
    finally:
        _put_conn(conn)


def _count_insider_filings(insider_name: str) -> int:
    conn = _get_conn()
    if not conn:
        return 1
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*)
            FROM insider_transactions
            WHERE insider_name = %s
              AND created_at > NOW() - INTERVAL '30 days'
        """, (insider_name,))
        row = cur.fetchone()
        cur.close()
        return int(row[0]) if row else 1
    except Exception:
        return 1
    finally:
        _put_conn(conn)


def _get_or_assign_cluster_id(ticker: str, tx_date: date) -> int | None:
    conn = _get_conn()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        window_start = tx_date - timedelta(days=14)
        window_end = tx_date + timedelta(days=14)
        cur.execute("""
            SELECT cluster_id FROM insider_transactions
            WHERE ticker = %s
              AND transaction_date BETWEEN %s AND %s
              AND cluster_id IS NOT NULL
            LIMIT 1
        """, (ticker, window_start, window_end))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute("SELECT COALESCE(MAX(cluster_id), 0) + 1 FROM insider_transactions")
        row = cur.fetchone()
        cur.close()
        return int(row[0]) if row else 1
    except Exception:
        return None
    finally:
        _put_conn(conn)


def _insert_transactions(records: list[dict]) -> int:
    if not records:
        return 0
    conn = _get_conn()
    if not conn:
        return 0
    inserted = 0
    try:
        cur = conn.cursor()
        expires_at = datetime.utcnow() + timedelta(days=_RETENTION_DAYS)
        for r in records:
            try:
                cur.execute("""
                    INSERT INTO insider_transactions (
                        accession_number, ticker, company_name, insider_name,
                        insider_title, transaction_type, transaction_code,
                        transaction_date, filing_date, shares, price_per_share,
                        total_value, shares_owned_after, ownership_type,
                        conviction_score, score_breakdown, price_context,
                        cluster_id, context_tags, cluster_type, cluster_metadata,
                        sector, expires_at
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s
                    ) ON CONFLICT (accession_number) DO NOTHING
                """, (
                    r["accession_number"],
                    r.get("ticker"),
                    r.get("company_name"),
                    r.get("insider_name"),
                    r.get("insider_title"),
                    r.get("transaction_type"),
                    r.get("transaction_code"),
                    r.get("transaction_date"),
                    r.get("filing_date"),
                    r.get("shares"),
                    r.get("price_per_share"),
                    r.get("total_value"),
                    r.get("shares_owned_after"),
                    r.get("ownership_type"),
                    r.get("conviction_score"),
                    Json(r["score_breakdown"]) if r.get("score_breakdown") else None,
                    Json(r["price_context"]) if r.get("price_context") else None,
                    r.get("cluster_id"),
                    r.get("context_tags"),
                    r.get("cluster_type"),
                    Json(r["cluster_metadata"]) if r.get("cluster_metadata") else None,
                    r.get("sector"),
                    expires_at,
                ))
                inserted += 1
            except Exception as e:
                logger.debug("[INSIDER_DB] Row insert error: %s", e)
                conn.rollback()
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error("[INSIDER_DB] Bulk insert error: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        _put_conn(conn)
    return inserted


# ── Scoring Engine ────────────────────────────────────────────────────────────

def _score_size(total_value: float) -> tuple[int, str]:
    if total_value >= 5_000_000:
        return 15, f"${total_value/1e6:.1f}M total value"
    elif total_value >= 1_000_000:
        return 12, f"${total_value/1e6:.1f}M total value"
    elif total_value >= 250_000:
        return 8, f"${total_value/1e3:.0f}K total value"
    elif total_value >= 50_000:
        return 5, f"${total_value/1e3:.0f}K total value"
    else:
        return 2, f"${total_value:.0f} total value"


def _score_role(title: str, is_director: bool, is_officer: bool, is_ten_pct: bool) -> tuple[int, str]:
    t = (title or "").upper()
    if is_ten_pct:
        return 18, "10% Owner"
    for kw, score, label in [
        (["CEO", "CHIEF EXECUTIVE", "CHAIRMAN", "EXEC CHAIR"], 20, title or "CEO/Chairman"),
        (["PRESIDENT", "COO", "CHIEF OPERATING"], 18, title or "President/COO"),
        (["CFO", "CTO", "CMO", "CHIEF FINANCIAL", "CHIEF TECH", "CHIEF MARKET"], 16, title or "C-Suite"),
        (["SVP", "EVP", "SENIOR VP", "EXECUTIVE VP"], 12, title or "SVP/EVP"),
        (["VP ", "VICE PRESIDENT"], 10, title or "VP"),
    ]:
        if any(k in t for k in kw):
            return score, label
    if is_director:
        return 8, "Director"
    if is_officer:
        return 6, title or "Officer"
    return 4, title or "Other"


def _score_type(code: str) -> tuple[int, str]:
    mapping = {
        "P": (10, "Open market purchase"),
        "S": (7, "Open market sale"),
        "M": (5, "Option exercise"),
        "G": (1, "Gift"),
        "A": (0, "Grant/Award"),
        "D": (3, "Disposition"),
        "V": (3, "Voluntary disposition"),
    }
    score, label = mapping.get(code, (0, code or "Unknown"))
    return score, label


def _score_context(code: str, price_context: dict | None) -> tuple[int, str]:
    if not price_context:
        return 0, "No price data"
    pct_from_high = price_context.get("pct_from_52w_high")
    if pct_from_high is None:
        return 0, "No 52w data"
    is_buy = code == "P"
    is_sell = code == "S"
    if is_buy:
        if pct_from_high <= -30:
            return 15, f"Buying {abs(pct_from_high):.0f}% below 52w high"
        elif pct_from_high <= -20:
            return 12, f"Buying {abs(pct_from_high):.0f}% below 52w high"
        elif pct_from_high <= -10:
            return 8, f"Buying {abs(pct_from_high):.0f}% below 52w high"
        elif pct_from_high >= -5:
            return 3, "Buying near 52w high"
        else:
            return 5, f"Buying {abs(pct_from_high):.0f}% below 52w high"
    elif is_sell:
        if pct_from_high >= -5:
            return 5, "Selling near 52w high (expected)"
        elif pct_from_high <= -30:
            return 12, f"Selling {abs(pct_from_high):.0f}% below 52w high (unusual)"
        else:
            return 3, f"Selling {abs(pct_from_high):.0f}% below 52w high"
    return 3, f"{abs(pct_from_high):.0f}% from 52w high"


def _score_cluster(cluster_count: int, cluster_type: str | None = None) -> tuple[int, str]:
    if cluster_count <= 1:
        return 0, "Single insider transaction"
    if cluster_count >= 8:
        raw = 15
    elif cluster_count >= 5:
        raw = 12
    elif cluster_count >= 3:
        raw = 12
    else:
        raw = 8
    multipliers = {
        "coordinated_buy": 1.0,
        "coordinated_sell": 0.9,
        "mixed": 0.5,
        "lockup_expiry": 0.2,
    }
    mult = multipliers.get(cluster_type or "", 1.0)
    score = min(int(round(raw * mult)), 15)
    label = (cluster_type or "cluster").replace("_", " ")
    return score, f"{cluster_count} insiders ({label})"


def _score_position(code: str, shares: int, shares_after: int) -> tuple[int, str]:
    if shares_after <= 0 or shares <= 0:
        return 2, "Position data unavailable"
    pct = abs(shares) / max(abs(shares_after), 1) * 100
    direction = "increase" if code == "P" else "reduction"
    if pct >= 50:
        return 10, f"{pct:.0f}% position {direction}"
    elif pct >= 20:
        return 7, f"{pct:.0f}% position {direction}"
    elif pct >= 10:
        return 4, f"{pct:.0f}% position {direction}"
    return 2, f"{pct:.0f}% position {direction}"


def _score_track_record(filing_count: int) -> tuple[int, str]:
    if filing_count >= 4:
        return 10, f"{filing_count} filings in 30 days"
    elif filing_count >= 2:
        return 7, f"{filing_count} filings in 30 days"
    return 5, "First filing in our database"


def _score_event_proximity(code: str, has_earnings_nearby: bool) -> tuple[int, str]:
    if has_earnings_nearby:
        if code == "P":
            return 5, "Within 14 days of earnings (buy)"
        return 4, "Within 14 days of earnings (sell)"
    return 1, "No nearby earnings event"


def calculate_conviction_score(
    code: str,
    total_value: float,
    title: str,
    is_director: bool,
    is_officer: bool,
    is_ten_pct: bool,
    shares: int,
    shares_after: int,
    cluster_count: int,
    filing_count: int,
    has_earnings_nearby: bool,
    price_context: dict | None,
    cluster_type: str | None = None,
) -> tuple[int, dict]:
    s_size, d_size = _score_size(total_value)
    s_role, d_role = _score_role(title, is_director, is_officer, is_ten_pct)
    s_type, d_type = _score_type(code)
    s_ctx, d_ctx = _score_context(code, price_context)
    s_cluster, d_cluster = _score_cluster(cluster_count, cluster_type)
    s_pos, d_pos = _score_position(code, shares, shares_after)
    s_track, d_track = _score_track_record(filing_count)
    s_event, d_event = _score_event_proximity(code, has_earnings_nearby)

    total = s_size + s_role + s_type + s_ctx + s_cluster + s_pos + s_track + s_event
    breakdown = {
        "size":           {"score": s_size,    "max": _W_SIZE,    "detail": d_size},
        "role":           {"score": s_role,    "max": _W_ROLE,    "detail": d_role},
        "type":           {"score": s_type,    "max": _W_TYPE,    "detail": d_type},
        "context":        {"score": s_ctx,     "max": _W_CONTEXT, "detail": d_ctx},
        "cluster":        {"score": s_cluster, "max": _W_CLUSTER, "detail": d_cluster,
                           "cluster_type": cluster_type, "cluster_size": cluster_count},
        "position_impact":{"score": s_pos,     "max": _W_POSITION,"detail": d_pos},
        "track_record":   {"score": s_track,   "max": _W_TRACK,   "detail": d_track},
        "event_proximity":{"score": s_event,   "max": _W_EVENT,   "detail": d_event},
    }
    return min(total, 100), breakdown


# ── Context Tags ──────────────────────────────────────────────────────────────

def generate_context_tags(
    code: str,
    title: str,
    is_director: bool,
    is_ten_pct: bool,
    total_value: float,
    cluster_count: int,
    shares: int,
    shares_after: int,
    cluster_type: str | None = None,
    date_spread_days: int = 0,
    distinct_role_count: int = 1,
) -> list[str]:
    tags = []
    code_map = {"P": "open-market buy", "S": "open-market sell", "M": "option exercise",
                "A": "grant/award", "G": "gift", "D": "disposition", "V": "disposition"}
    if code in code_map:
        tags.append(code_map[code])
    t = (title or "").upper()
    if any(k in t for k in ["CEO", "PRESIDENT", "CHAIRMAN", "COO"]):
        tags.append("C-suite")
    elif is_director:
        tags.append("board member")
    if is_ten_pct:
        tags.append("major shareholder")
    if total_value >= 10_000_000:
        tags.append("mega trade")
    elif total_value >= 1_000_000:
        tags.append("large trade")
    if cluster_count >= 2:
        tags.append(f"{cluster_count} insiders")
        if cluster_type == "coordinated_buy":
            tags.append("coordinated buying")
        elif cluster_type == "coordinated_sell":
            tags.append("coordinated selling")
            if date_spread_days > 5:
                tags.append("staggered sells")
            if distinct_role_count >= 3:
                tags.append("cross-level selling")
        elif cluster_type == "lockup_expiry":
            tags.append("lockup expiry")
        elif cluster_type == "mixed":
            tags.append("mixed insider activity")
    if shares_after > 0 and abs(shares) / max(abs(shares_after), 1) > 0.5:
        tags.append("major stake change")
    return tags


# ── Price Enrichment ─────────────────────────────────────────────────────────

async def _get_price_context_batch(tickers: list[str]) -> dict[str, dict]:
    results: dict[str, dict] = {}
    remaining = list(set(tickers))

    # PRIMARY: Tradier batch
    if _TRADIER_KEY and remaining:
        try:
            symbols_str = ",".join(remaining[:50])
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(
                    "https://sandbox.tradier.com/v1/markets/quotes",
                    params={"symbols": symbols_str},
                    headers={
                        "Authorization": f"Bearer {_TRADIER_KEY}",
                        "Accept": "application/json",
                    },
                )
            if resp.status_code == 200:
                data = resp.json()
                quotes = data.get("quotes", {}).get("quote", [])
                if isinstance(quotes, dict):
                    quotes = [quotes]
                for q in quotes:
                    sym = q.get("symbol", "")
                    price = q.get("last") or q.get("prevclose")
                    high = q.get("week_52_high")
                    low = q.get("week_52_low")
                    if price and high and low:
                        pct_from_high = (price - high) / high * 100 if high else None
                        results[sym] = {
                            "current_price": price,
                            "high_52w": high,
                            "low_52w": low,
                            "pct_from_52w_high": round(pct_from_high, 2) if pct_from_high else None,
                            "vs_52w_high": f"{pct_from_high:.1f}%" if pct_from_high else None,
                            "price_at_filing": price,
                            "change_since_filing_pct": None,
                            "return_30d": None,
                            "return_90d": None,
                        }
                        if sym in remaining:
                            remaining.remove(sym)
        except Exception as e:
            logger.warning("[INSIDER_PRICE] Tradier batch failed: %s", e)

    # FALLBACK 1: Finnhub per-ticker
    if _FINNHUB_KEY and remaining:
        for sym in remaining[:]:
            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    q_resp = await client.get(
                        "https://finnhub.io/api/v1/quote",
                        params={"symbol": sym, "token": _FINNHUB_KEY},
                    )
                    m_resp = await client.get(
                        "https://finnhub.io/api/v1/stock/metric",
                        params={"symbol": sym, "metric": "all", "token": _FINNHUB_KEY},
                    )
                q = q_resp.json() if q_resp.status_code == 200 else {}
                m = m_resp.json().get("metric", {}) if m_resp.status_code == 200 else {}
                price = q.get("c") or q.get("pc")
                high = m.get("52WeekHigh")
                low = m.get("52WeekLow")
                if price:
                    pct_from_high = (price - high) / high * 100 if high else None
                    results[sym] = {
                        "current_price": price,
                        "high_52w": high,
                        "low_52w": low,
                        "pct_from_52w_high": round(pct_from_high, 2) if pct_from_high else None,
                        "vs_52w_high": f"{pct_from_high:.1f}%" if pct_from_high else None,
                        "price_at_filing": price,
                        "change_since_filing_pct": None,
                        "return_30d": None,
                        "return_90d": None,
                    }
                    remaining.remove(sym)
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.debug("[INSIDER_PRICE] Finnhub failed for %s: %s", sym, e)

    # FALLBACK 2: yfinance
    if remaining:
        def _yf_fetch(syms):
            out = {}
            try:
                import yfinance as yf
                for s in syms:
                    try:
                        info = yf.Ticker(s).info
                        price = info.get("currentPrice") or info.get("regularMarketPrice")
                        high = info.get("fiftyTwoWeekHigh")
                        low = info.get("fiftyTwoWeekLow")
                        if price:
                            pct_from_high = (price - high) / high * 100 if high else None
                            out[s] = {
                                "current_price": price,
                                "high_52w": high,
                                "low_52w": low,
                                "pct_from_52w_high": round(pct_from_high, 2) if pct_from_high else None,
                                "vs_52w_high": f"{pct_from_high:.1f}%" if pct_from_high else None,
                                "price_at_filing": price,
                                "change_since_filing_pct": None,
                                "return_30d": None,
                                "return_90d": None,
                                "sector": info.get("sector"),
                            }
                    except Exception:
                        pass
            except Exception:
                pass
            return out
        loop = asyncio.get_event_loop()
        yf_results = await loop.run_in_executor(_executor, _yf_fetch, remaining)
        results.update(yf_results)

    # ── yfinance historical 30D/90D returns ───────────────────────────────────
    if results:
        def _yf_hist(syms: list[str]) -> dict[str, tuple]:
            out = {}
            try:
                import yfinance as yf
                for s in syms:
                    try:
                        hist = yf.Ticker(s).history(period="3mo", interval="1d")
                        if hist.empty or len(hist) < 5:
                            continue
                        cur = float(hist["Close"].iloc[-1])
                        p30 = float(hist["Close"].iloc[-30]) if len(hist) >= 30 else float(hist["Close"].iloc[0])
                        p90 = float(hist["Close"].iloc[0])
                        r30 = round((cur - p30) / p30 * 100, 2) if p30 else None
                        r90 = round((cur - p90) / p90 * 100, 2) if p90 else None
                        out[s] = (r30, r90)
                    except Exception:
                        pass
            except Exception:
                pass
            return out

        loop = asyncio.get_event_loop()
        try:
            hist_map = await loop.run_in_executor(_executor, _yf_hist, list(results.keys()))
            for sym, (r30, r90) in hist_map.items():
                if sym in results:
                    results[sym]["return_30d"] = r30
                    results[sym]["return_90d"] = r90
        except Exception as e:
            logger.warning("[INSIDER_PRICE] yfinance history batch failed: %s", e)

    return results


# ── Price Returns Backfill ─────────────────────────────────────────────────────

def _sync_backfill_price_returns() -> dict:
    """Update return_30d / return_90d for transactions where they are still null."""
    conn = _get_conn()
    if not conn:
        return {"updated": 0, "error": "no db connection"}
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT ticker
            FROM insider_transactions
            WHERE ticker IS NOT NULL AND ticker NOT IN ('NONE','')
              AND (
                price_context IS NULL
                OR price_context->>'return_30d' IS NULL
                OR price_context->>'return_90d' IS NULL
              )
        """)
        tickers = [r[0] for r in cur.fetchall()]
        cur.close()
    except Exception as e:
        logger.error("[INSIDER_BACKFILL_PRICE] DB read error: %s", e)
        _put_conn(conn)
        return {"updated": 0, "error": str(e)}

    if not tickers:
        _put_conn(conn)
        return {"updated": 0, "message": "all price_context already populated"}

    logger.info("[INSIDER_BACKFILL_PRICE] Backfilling 30D/90D returns for %d tickers", len(tickers))

    # Use yfinance to get 30D/90D returns for each unique ticker
    price_map: dict[str, dict] = {}
    try:
        import yfinance as yf
        for sym in tickers:
            try:
                hist = yf.Ticker(sym).history(period="3mo", interval="1d")
                if hist.empty or len(hist) < 5:
                    continue
                cur = float(hist["Close"].iloc[-1])
                p30 = float(hist["Close"].iloc[-30]) if len(hist) >= 30 else float(hist["Close"].iloc[0])
                p90 = float(hist["Close"].iloc[0])
                price_map[sym] = {
                    "return_30d": round((cur - p30) / p30 * 100, 2) if p30 else None,
                    "return_90d": round((cur - p90) / p90 * 100, 2) if p90 else None,
                    "current_price": cur,
                }
            except Exception as e:
                logger.debug("[INSIDER_BACKFILL_PRICE] yfinance failed for %s: %s", sym, e)
    except Exception as e:
        _put_conn(conn)
        logger.error("[INSIDER_BACKFILL_PRICE] yfinance import error: %s", e)
        return {"updated": 0, "error": str(e)}

    def _safe_float(v):
        """Return None instead of NaN/Inf for JSON safety."""
        import math
        if v is None:
            return None
        try:
            f = float(v)
            return None if (math.isnan(f) or math.isinf(f)) else f
        except (TypeError, ValueError):
            return None

    updated = 0
    try:
        cur2 = conn.cursor()
        for sym, pc in price_map.items():
            r30 = _safe_float(pc.get("return_30d"))
            r90 = _safe_float(pc.get("return_90d"))
            cur_p = _safe_float(pc.get("current_price"))
            if r30 is None and r90 is None:
                continue
            patch = json.dumps({k: v for k, v in {
                "return_30d": r30,
                "return_90d": r90,
                "current_price": cur_p,
            }.items() if v is not None})
            cur2.execute("""
                UPDATE insider_transactions
                SET price_context = COALESCE(price_context, '{}'::jsonb) || %s::jsonb
                WHERE ticker = %s
                  AND (price_context->>'return_30d' IS NULL OR price_context->>'return_90d' IS NULL)
            """, (patch, sym,))
            updated += cur2.rowcount
        conn.commit()
        cur2.close()
        logger.info("[INSIDER_BACKFILL_PRICE] Updated price_context for %d rows", updated)
    except Exception as e:
        logger.error("[INSIDER_BACKFILL_PRICE] Update error: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        _put_conn(conn)

    return {"updated": updated, "tickers_processed": len(price_map)}


# ── Earnings Proximity ────────────────────────────────────────────────────────

async def _check_earnings_nearby(ticker: str, tx_date: date) -> bool:
    if not _FINNHUB_KEY:
        return False
    try:
        from_dt = (tx_date - timedelta(days=14)).isoformat()
        to_dt = (tx_date + timedelta(days=14)).isoformat()
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.get(
                "https://finnhub.io/api/v1/calendar/earnings",
                params={"symbol": ticker, "from": from_dt, "to": to_dt, "token": _FINNHUB_KEY},
            )
        if resp.status_code == 200:
            data = resp.json()
            earnings = data.get("earningsCalendar", [])
            return len(earnings) > 0
    except Exception:
        pass
    return False


# ── Cluster Type Detection ────────────────────────────────────────────────────

def _detect_cluster_type(ticker: str, tx_date: date, filing_acc_prefix: str) -> dict:
    """
    Query existing DB rows for same ticker within 14-day rolling window and
    classify the cluster as coordinated_buy / coordinated_sell / lockup_expiry / mixed.
    Returns {"type": str|None, "count": int, "metadata": dict}.
    """
    conn = _get_conn()
    if not conn:
        return {"type": None, "count": 1, "metadata": {}}
    try:
        cur = conn.cursor()
        window_start = tx_date - timedelta(days=14)
        window_end = tx_date + timedelta(days=14)
        cur.execute("""
            SELECT insider_name, transaction_code, transaction_date,
                   total_value, shares, shares_owned_after, insider_title
            FROM insider_transactions
            WHERE ticker = %s
              AND transaction_date BETWEEN %s AND %s
              AND accession_number NOT LIKE %s
        """, (ticker, window_start, window_end, f"{filing_acc_prefix}%"))
        rows = cur.fetchall()
        cur.close()
    except Exception:
        return {"type": None, "count": 1, "metadata": {}}
    finally:
        _put_conn(conn)

    if not rows:
        return {"type": None, "count": 1, "metadata": {}}

    cluster_insiders = list({r[0] for r in rows if r[0]})
    cluster_count = len(cluster_insiders) + 1  # +1 for current transaction

    codes = [r[1] for r in rows]
    buy_count = sum(1 for c in codes if c == "P")
    sell_count = sum(1 for c in codes if c == "S")

    dates = [r[2] for r in rows if r[2]]
    if dates:
        date_min = min(dates) if not isinstance(min(dates), datetime) else min(dates).date()
        date_max = max(dates) if not isinstance(max(dates), datetime) else max(dates).date()
        if isinstance(date_min, date) and isinstance(date_max, date):
            date_spread = (date_max - date_min).days
        else:
            date_spread = 0
    else:
        date_spread = 0

    total_cluster_val = sum(float(r[3] or 0) for r in rows)

    roles = [r[6] or "" for r in rows]
    role_categories: set[str] = set()
    for role in roles:
        r_up = role.upper()
        if any(k in r_up for k in ["CEO", "CHAIRMAN", "PRESIDENT"]):
            role_categories.add("CEO/Chair")
        elif any(k in r_up for k in ["CFO", "CTO", "CMO"]):
            role_categories.add("C-Suite")
        elif "DIRECTOR" in r_up:
            role_categories.add("Director")
        elif any(k in r_up for k in ["SVP", "EVP", "VP"]):
            role_categories.add("VP/SVP")
        else:
            role_categories.add("Other")

    impacts = []
    for r in rows:
        shares_t = abs(int(r[4] or 0))
        shares_after = abs(int(r[5] or 0))
        if shares_after > 0:
            impacts.append(shares_t / shares_after * 100)

    impact_stdev = 0.0
    if len(impacts) >= 2:
        mean = sum(impacts) / len(impacts)
        impact_stdev = (sum((x - mean) ** 2 for x in impacts) / len(impacts)) ** 0.5

    total_txns = max(len(rows), 1)
    buy_ratio = buy_count / total_txns
    sell_ratio = sell_count / total_txns

    is_lockup = False
    if sell_ratio > 0.7:
        if date_spread <= 2 or impact_stdev < 15:
            is_lockup = True

    if buy_ratio > 0.7:
        cluster_type = "coordinated_buy"
    elif sell_ratio > 0.7 and not is_lockup:
        cluster_type = "coordinated_sell"
    elif sell_ratio > 0.7 and is_lockup:
        cluster_type = "lockup_expiry"
    else:
        cluster_type = "mixed"

    _type_multipliers = {
        "coordinated_buy": 1.0, "coordinated_sell": 0.9,
        "mixed": 0.5, "lockup_expiry": 0.2,
    }
    avg_impact = round(sum(impacts) / max(len(impacts), 1), 2) if impacts else None
    metadata = {
        "cluster_size": cluster_count,
        "cluster_type": cluster_type,
        "cluster_date_range": {
            "start": str(dates[0]) if dates else None,
            "end": str(dates[-1]) if dates else None,
        },
        "date_spread_days": date_spread,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "insiders_in_cluster": cluster_insiders,
        "roles_in_cluster": roles,
        "total_cluster_value": total_cluster_val,
        "avg_position_impact_pct": avg_impact,
        "position_impact_stdev": round(impact_stdev, 2),
        "is_lockup_likely": is_lockup,
        "type_multiplier": _type_multipliers.get(cluster_type, 1.0),
        "distinct_role_count": len(role_categories),
    }
    return {"type": cluster_type, "count": cluster_count, "metadata": metadata}


# ── SEC EDGAR Fetch ───────────────────────────────────────────────────────────

# Global last_refresh tracking
_last_refresh: datetime | None = None
_refresh_in_progress = False
_total_inserted = 0

# Perplexity AI daily analysis cache
_last_ai_run: datetime | None = None
_ai_cache: dict | None = None


def _parse_filing_sync(filing) -> list[dict]:
    import pandas as pd

    records = []
    try:
        form4 = filing.obj()
    except Exception as e:
        logger.debug("[INSIDER] Failed to parse filing %s: %s", getattr(filing, 'accession_number', '?'), e)
        return []

    try:
        trades_df = form4.market_trades
        if trades_df is None or not isinstance(trades_df, pd.DataFrame) or trades_df.empty:
            return []
    except Exception:
        return []

    # Issuer info
    try:
        issuer = form4.issuer
        ticker = getattr(issuer, "ticker", None) or ""
        company_name = getattr(issuer, "name", None) or filing.company or ""
    except Exception:
        ticker = ""
        company_name = filing.company or ""

    # Owner info (use first reporting owner)
    insider_name = ""
    insider_title = ""
    is_director = False
    is_officer = False
    is_ten_pct = False
    try:
        owners = form4.reporting_owners
        if owners:
            owner = owners[0]
            insider_name = getattr(owner, "name_unreversed", None) or getattr(owner, "name", None) or ""
            insider_title = getattr(owner, "officer_title", None) or getattr(owner, "position", None) or ""
            is_director = bool(getattr(owner, "is_director", False))
            is_officer = bool(getattr(owner, "is_officer", False))
            is_ten_pct = bool(getattr(owner, "is_ten_pct_owner", False))
    except Exception:
        pass

    filing_accession = str(getattr(filing, "accession_number", "") or "")
    filing_date_raw = getattr(filing, "filing_date", None)
    if isinstance(filing_date_raw, date) and not isinstance(filing_date_raw, datetime):
        filing_date = datetime.combine(filing_date_raw, datetime.min.time())
    elif isinstance(filing_date_raw, datetime):
        filing_date = filing_date_raw
    else:
        filing_date = datetime.utcnow()

    for idx, (_, row) in enumerate(trades_df.iterrows()):
        code = str(row.get("Code", "")).strip()
        if code not in _KEEP_CODES:
            continue

        accession = f"{filing_accession}:{idx}"
        if len(accession) > 29:
            accession = accession[:29]

        raw_date = row.get("Date")
        if isinstance(raw_date, str):
            try:
                tx_date = date.fromisoformat(raw_date)
            except Exception:
                tx_date = date.today()
        elif isinstance(raw_date, date):
            tx_date = raw_date
        else:
            tx_date = date.today()

        shares = int(row.get("Shares") or 0)
        price = float(row.get("Price") or 0.0)
        shares_after = int(row.get("Remaining") or 0)
        acquired_disposed = str(row.get("AcquiredDisposed", "")).strip()
        ownership_type = "Direct" if str(row.get("DirectIndirect", "D")).strip() == "D" else "Indirect"
        total_value = abs(shares * price)

        tx_type_map = {"P": "Buy", "S": "Sale", "M": "Exercise", "A": "Grant",
                       "D": "Disposition", "G": "Gift", "V": "Disposition"}
        tx_type = tx_type_map.get(code, code)

        records.append({
            "accession_number": accession,
            "ticker": ticker.upper() if ticker else None,
            "company_name": company_name,
            "insider_name": insider_name,
            "insider_title": insider_title,
            "transaction_type": tx_type,
            "transaction_code": code,
            "transaction_date": tx_date,
            "filing_date": filing_date,
            "shares": shares,
            "price_per_share": price if price > 0 else None,
            "total_value": total_value if total_value > 0 else None,
            "shares_owned_after": shares_after if shares_after >= 0 else None,
            "ownership_type": ownership_type,
            "is_director": is_director,
            "is_officer": is_officer,
            "is_ten_pct": is_ten_pct,
        })

    return records


async def fetch_recent_form4_filings(max_filings: int = 200) -> int:
    global _last_refresh, _refresh_in_progress, _total_inserted

    if _refresh_in_progress:
        logger.info("[INSIDER] Fetch already in progress — skipping")
        return 0
    if not _EDGAR_AVAILABLE:
        logger.warning("[INSIDER] edgartools not available")
        return 0

    _refresh_in_progress = True
    inserted = 0
    try:
        today = date.today()
        yesterday = today - timedelta(days=1)
        date_range = f"{yesterday}:{today}"
        logger.info("[INSIDER] Fetching Form 4 filings from SEC EDGAR for %s ...", date_range)
        loop = asyncio.get_event_loop()

        def _get_filings_sync():
            return get_filings(form="4", filing_date=date_range)

        filings = await loop.run_in_executor(_executor, _get_filings_sync)

        # Parse filings in batches
        raw_records: list[dict] = []
        count = 0
        for filing in filings:
            if count >= max_filings:
                break
            count += 1
            try:
                recs = await loop.run_in_executor(_executor, _parse_filing_sync, filing)
                raw_records.extend(recs)
            except Exception as e:
                logger.debug("[INSIDER] Parse error for filing: %s", e)
            await asyncio.sleep(_SEC_DELAY)

        logger.info("[INSIDER] Parsed %d raw transactions from %d filings", len(raw_records), count)
        if not raw_records:
            _last_refresh = datetime.utcnow()
            return 0

        # Check existing accessions (dedup)
        all_accessions = [r["accession_number"] for r in raw_records]
        existing = _get_existing_accessions(all_accessions)
        new_records = [r for r in raw_records if r["accession_number"] not in existing]
        logger.info("[INSIDER] %d new transactions to process (%d duplicates skipped)",
                    len(new_records), len(raw_records) - len(new_records))

        if not new_records:
            _last_refresh = datetime.utcnow()
            return 0

        # Batch price enrichment
        tickers = list({r["ticker"] for r in new_records if r.get("ticker")})
        price_map: dict[str, dict] = {}
        if tickers:
            price_map = await _get_price_context_batch(tickers)

        # Score and tag each transaction
        final_records = []
        for r in new_records:
            ticker = r.get("ticker") or ""
            code = r.get("transaction_code") or "S"
            price_ctx = price_map.get(ticker)
            total_value = float(r.get("total_value") or 0)
            shares = int(r.get("shares") or 0)
            shares_after = int(r.get("shares_owned_after") or 0)

            # Cluster detection (queries DB for nearby same-ticker transactions)
            acc_prefix = r["accession_number"].split(":")[0]
            cluster_info = await loop.run_in_executor(
                _executor, _detect_cluster_type, ticker, r["transaction_date"], acc_prefix
            )
            cluster_count = cluster_info["count"]
            cluster_type = cluster_info["type"]
            cluster_metadata = cluster_info["metadata"] if cluster_info["metadata"] else None

            filing_count = await loop.run_in_executor(
                _executor, _count_insider_filings, r.get("insider_name") or ""
            )
            has_earnings = await _check_earnings_nearby(ticker, r["transaction_date"])

            score, breakdown = calculate_conviction_score(
                code=code,
                total_value=total_value,
                title=r.get("insider_title") or "",
                is_director=r.get("is_director", False),
                is_officer=r.get("is_officer", False),
                is_ten_pct=r.get("is_ten_pct", False),
                shares=shares,
                shares_after=shares_after,
                cluster_count=cluster_count,
                filing_count=filing_count,
                has_earnings_nearby=has_earnings,
                price_context=price_ctx,
                cluster_type=cluster_type,
            )

            date_spread = (cluster_metadata or {}).get("date_spread_days", 0)
            distinct_roles = (cluster_metadata or {}).get("distinct_role_count", 1)
            tags = generate_context_tags(
                code=code,
                title=r.get("insider_title") or "",
                is_director=r.get("is_director", False),
                is_ten_pct=r.get("is_ten_pct", False),
                total_value=total_value,
                cluster_count=cluster_count,
                shares=shares,
                shares_after=shares_after,
                cluster_type=cluster_type,
                date_spread_days=date_spread,
                distinct_role_count=distinct_roles,
            )

            cluster_id = None
            if cluster_count >= 2:
                cluster_id = await loop.run_in_executor(
                    _executor, _get_or_assign_cluster_id, ticker, r["transaction_date"]
                )

            sector = (price_ctx or {}).get("sector")
            record = {**r, "conviction_score": score, "score_breakdown": breakdown,
                      "price_context": price_ctx, "cluster_id": cluster_id,
                      "context_tags": tags, "cluster_type": cluster_type,
                      "cluster_metadata": cluster_metadata, "sector": sector}
            record.pop("is_director", None)
            record.pop("is_officer", None)
            record.pop("is_ten_pct", None)
            final_records.append(record)

        inserted = await loop.run_in_executor(_executor, _insert_transactions, final_records)
        _total_inserted += inserted
        _last_refresh = datetime.utcnow()
        logger.info("[INSIDER] Inserted %d new transactions", inserted)

    except Exception as e:
        logger.error("[INSIDER] Fetch error: %s", e, exc_info=True)
    finally:
        _refresh_in_progress = False

    return inserted


# ── Historical 30-Day Backfill ────────────────────────────────────────────────

async def fetch_historical_30d(days_back: int = 30, filings_per_week: int = 60) -> dict:
    """
    Sweeps through the past `days_back` days in 7-day windows, fetching up to
    `filings_per_week` Form 4 filings per window. Deduplication is handled by
    accession number so this is safe to call repeatedly.
    Returns {"inserted": N, "windows": W, "errors": E}
    """
    global _refresh_in_progress

    if _refresh_in_progress:
        logger.info("[INSIDER_HIST] Fetch in progress — skipping historical load")
        return {"inserted": 0, "windows": 0, "errors": 0, "message": "Fetch already in progress"}

    if not _EDGAR_AVAILABLE:
        return {"inserted": 0, "windows": 0, "errors": 0, "message": "edgartools not available"}

    _refresh_in_progress = True
    total_inserted = 0
    total_errors = 0
    windows_done = 0

    try:
        loop = asyncio.get_event_loop()
        today = date.today()

        # Build weekly windows from oldest → newest
        windows: list[tuple[date, date]] = []
        cursor = today - timedelta(days=days_back)
        while cursor < today:
            end = min(cursor + timedelta(days=6), today)
            windows.append((cursor, end))
            cursor = end + timedelta(days=1)

        logger.info("[INSIDER_HIST] Fetching %d weekly windows over past %d days", len(windows), days_back)

        for win_start, win_end in windows:
            date_range_str = f"{win_start.strftime('%Y-%m-%d')}:{win_end.strftime('%Y-%m-%d')}"
            logger.info("[INSIDER_HIST] Window %s", date_range_str)

            try:
                def _get_window_filings(dr=date_range_str):
                    return get_filings(form="4", filing_date=dr)

                filings = await loop.run_in_executor(_executor, _get_window_filings)
                if not filings:
                    windows_done += 1
                    continue

                raw_records: list[dict] = []
                count = 0
                for filing in filings:
                    if count >= filings_per_week:
                        break
                    count += 1
                    try:
                        recs = await loop.run_in_executor(_executor, _parse_filing_sync, filing)
                        raw_records.extend(recs)
                    except Exception as pe:
                        logger.debug("[INSIDER_HIST] Parse error: %s", pe)
                    await asyncio.sleep(_SEC_DELAY)

                if not raw_records:
                    windows_done += 1
                    continue

                # Dedup
                all_acc = [r["accession_number"] for r in raw_records]
                existing = _get_existing_accessions(all_acc)
                new_records = [r for r in raw_records if r["accession_number"] not in existing]
                logger.info("[INSIDER_HIST] Window %s: %d new / %d total", date_range_str, len(new_records), len(raw_records))

                if not new_records:
                    windows_done += 1
                    continue

                # Price enrichment
                tickers = list({r["ticker"] for r in new_records if r.get("ticker")})
                price_map: dict[str, dict] = {}
                if tickers:
                    price_map = await _get_price_context_batch(tickers)

                # Score and tag
                final_records = []
                for r in new_records:
                    ticker = r.get("ticker") or ""
                    code = r.get("transaction_code") or "S"
                    price_ctx = price_map.get(ticker)
                    total_value = float(r.get("total_value") or 0)
                    shares = int(r.get("shares") or 0)
                    shares_after = int(r.get("shares_owned_after") or 0)

                    acc_prefix = r["accession_number"].split(":")[0]
                    cluster_info = await loop.run_in_executor(
                        _executor, _detect_cluster_type, ticker, r["transaction_date"], acc_prefix
                    )
                    filing_count = await loop.run_in_executor(
                        _executor, _count_insider_filings, r.get("insider_name") or ""
                    )
                    has_earnings = await _check_earnings_nearby(ticker, r["transaction_date"])
                    is_director = r.get("is_director", False)
                    is_officer = r.get("is_officer", False)
                    is_ten_pct = r.get("is_ten_pct_owner", False)

                    cluster_count = cluster_info["count"]
                    cluster_type = cluster_info["type"]
                    cluster_metadata = cluster_info["metadata"] if cluster_info["metadata"] else None
                    cluster_id = None
                    if cluster_count >= 2:
                        cluster_id = _get_or_assign_cluster_id(ticker, r["transaction_date"])

                    s_size, d_size = _score_size(total_value)
                    s_role, d_role = _score_role(r.get("insider_title") or "", is_director, is_officer, is_ten_pct)
                    s_type, d_type = _score_type(code)
                    s_ctx, d_ctx = _score_context(code, price_ctx)
                    s_cluster, d_cluster = _score_cluster(cluster_count, cluster_type)
                    s_pos, d_pos = _score_position(code, shares, shares_after)
                    s_track, d_track = _score_track_record(filing_count)
                    s_event, d_event = _score_event_proximity(has_earnings, code)

                    conviction_score = min(
                        s_size + s_role + s_type + s_ctx + s_cluster + s_pos + s_track + s_event, 100
                    )
                    score_breakdown = {
                        "size":            {"score": s_size,    "max": _W_SIZE,    "detail": d_size},
                        "role":            {"score": s_role,    "max": _W_ROLE,    "detail": d_role},
                        "type":            {"score": s_type,    "max": _W_TYPE,    "detail": d_type},
                        "context":         {"score": s_ctx,     "max": _W_CONTEXT, "detail": d_ctx},
                        "cluster":         {"score": s_cluster, "max": _W_CLUSTER, "detail": d_cluster,
                                            "cluster_type": cluster_type, "cluster_size": cluster_count},
                        "position_impact": {"score": s_pos,     "max": _W_POSITION,"detail": d_pos},
                        "track_record":    {"score": s_track,   "max": _W_TRACK,   "detail": d_track},
                        "event_proximity": {"score": s_event,   "max": _W_EVENT,   "detail": d_event},
                    }
                    context_tags = generate_context_tags(
                        code=code, title=r.get("insider_title") or "",
                        is_director=is_director, is_ten_pct=is_ten_pct,
                        total_value=total_value, cluster_count=cluster_count,
                        shares=shares, shares_after=shares_after,
                        cluster_type=cluster_type, date_spread_days=0, distinct_role_count=1,
                    )
                    record = {**r, "conviction_score": conviction_score, "score_breakdown": score_breakdown,
                              "context_tags": context_tags, "price_context": price_ctx,
                              "cluster_id": cluster_id, "cluster_type": cluster_type,
                              "cluster_metadata": cluster_metadata}
                    final_records.append(record)

                win_inserted = await loop.run_in_executor(_executor, _insert_transactions, final_records)
                total_inserted += win_inserted
                windows_done += 1
                logger.info("[INSIDER_HIST] Window %s → %d inserted", date_range_str, win_inserted)

            except Exception as we:
                logger.error("[INSIDER_HIST] Window %s error: %s", date_range_str, we)
                total_errors += 1
                windows_done += 1

        logger.info("[INSIDER_HIST] Done: %d inserted across %d windows, %d errors",
                    total_inserted, windows_done, total_errors)

    except Exception as e:
        logger.error("[INSIDER_HIST] Fatal error: %s", e, exc_info=True)
        total_errors += 1
    finally:
        _refresh_in_progress = False

    return {"inserted": total_inserted, "windows": windows_done, "errors": total_errors,
            "message": f"Historical fetch complete: {total_inserted} new transactions across {windows_done} weekly windows"}


# ── Initial Load ──────────────────────────────────────────────────────────────

async def maybe_initial_load():
    """On cold start, if table is sparse (<200 rows), run 30-day historical load."""
    conn = _get_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM insider_transactions")
        count = cur.fetchone()[0]
        cur.close()
    except Exception:
        count = 0
    finally:
        _put_conn(conn)

    if count < 200:
        logger.info("[INSIDER] Table has %d rows — running 30-day historical load", count)
        await fetch_historical_30d(days_back=30, filings_per_week=60)
    else:
        logger.info("[INSIDER] Table has %d rows — skipping initial load", count)


# ── Perplexity AI Daily Analysis ─────────────────────────────────────────────

def _build_perplexity_prompt(top_buys: list[dict], top_sells: list[dict]) -> str:
    def fmt_tx(t: dict) -> str:
        return (
            f"  • {t.get('ticker','?')} | {t.get('company_name','?')} | "
            f"{t.get('insider_name','?')} ({t.get('insider_title','?')}) | "
            f"Code: {t.get('transaction_code','?')} | "
            f"Shares: {t.get('shares',0):,} | "
            f"Value: ${float(t.get('total_value') or 0):,.0f} | "
            f"Date: {t.get('transaction_date','?')} | "
            f"Rule-based score: {t.get('conviction_score',0)}"
        )

    buys_block = "\n".join(fmt_tx(t) for t in top_buys[:15]) or "  (none)"
    sells_block = "\n".join(fmt_tx(t) for t in top_sells[:15]) or "  (none)"

    return f"""You are an expert SEC Form 4 insider trading analyst. I will give you a list of recent insider purchases and sales from our database. Your job is to research these on SEC EDGAR and other sources, then produce a structured JSON analysis.

TOP INSIDER PURCHASES (last 30 days, rule-based scores):
{buys_block}

TOP INSIDER SALES (last 30 days, rule-based scores):
{sells_block}

For each transaction, research the company, the insider's history, recent news, and any SEC filings context. Then return ONLY valid JSON in this exact structure (no markdown, no extra text):

{{
  "generated_at": "ISO timestamp",
  "top_buys": [
    {{
      "ticker": "XXXX",
      "company_name": "Full Company Name",
      "insider_name": "Name",
      "insider_title": "Title",
      "ai_score": 0-100,
      "conviction": "high|medium|low",
      "rationale": "2-3 sentence analysis of why this buy is significant",
      "risk_factors": "Key risks to watch",
      "catalysts": "Upcoming events or catalysts",
      "verified_on_sec": true/false
    }}
  ],
  "top_sells": [
    {{
      "ticker": "XXXX",
      "company_name": "Full Company Name",
      "insider_name": "Name",
      "insider_title": "Title",
      "ai_score": 0-100,
      "sell_signal": "bearish|neutral|routine",
      "rationale": "2-3 sentence analysis of why this sell matters",
      "context": "Is this a planned 10b5-1 sale, tax-related, or discretionary?",
      "verified_on_sec": true/false
    }}
  ],
  "market_summary": "2-3 sentence overall assessment of insider sentiment across these transactions",
  "standout_buy": {{"ticker": "XXXX", "summary": "One sentence reason this is the most compelling buy"}},
  "standout_sell": {{"ticker": "XXXX", "summary": "One sentence reason this is the most concerning sell"}},
  "data_date": "YYYY-MM-DD"
}}

Include the 5 most significant buys and 5 most significant sells. Prioritize cluster buys (multiple insiders), large 10% owner purchases, and C-suite activity over routine VP awards."""


async def run_perplexity_analysis() -> dict:
    """Call Perplexity Sonar Pro to analyze top insider transactions. Runs once per day."""
    global _last_ai_run, _ai_cache

    if not _PPLX_KEY:
        logger.warning("[INSIDER_AI] No PERPLEXITY_API_KEY set — skipping AI analysis")
        return {"error": "Perplexity API key not configured"}

    loop = asyncio.get_event_loop()

    # Pull top candidates from DB
    top_buys, _ = await loop.run_in_executor(
        _executor, lambda: _query_transactions(tx_type_filter="P", min_score=50, limit=20, timeframe="1m")
    )
    top_sells, _ = await loop.run_in_executor(
        _executor, lambda: _query_transactions(tx_type_filter="S", min_score=55, limit=20, timeframe="1m")
    )

    prompt = _build_perplexity_prompt(top_buys, top_sells)

    logger.info("[INSIDER_AI] Calling Perplexity %s for daily analysis (%d buys, %d sells)...",
                _PPLX_MODEL, len(top_buys), len(top_sells))

    try:
        async with httpx.AsyncClient(timeout=90) as client:
            resp = await client.post(
                f"{_PPLX_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {_PPLX_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": _PPLX_MODEL,
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "You are an expert SEC insider trading analyst. "
                                "You have access to SEC EDGAR via search. "
                                "Always verify transactions on EDGAR before scoring. "
                                "Return only valid JSON, no markdown code blocks."
                            ),
                        },
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.1,
                    "max_tokens": 3000,
                    "return_citations": True,
                },
            )
        resp.raise_for_status()
        data = resp.json()
        choices = data.get("choices", [])
        if not choices:
            logger.error("[INSIDER] Perplexity returned no choices")
            return {}
        raw_content = choices[0].get("message", {}).get("content", "").strip()

        # Strip markdown code fences if Perplexity adds them
        if raw_content.startswith("```"):
            raw_content = raw_content.split("```")[1]
            if raw_content.startswith("json"):
                raw_content = raw_content[4:]
            raw_content = raw_content.strip()

        result = json.loads(raw_content)
        result["model"] = _PPLX_MODEL
        result["generated_at"] = datetime.utcnow().isoformat()

        # Persist to DB + in-memory cache
        await loop.run_in_executor(_executor, _save_ai_cache_to_db, result)
        _ai_cache = result
        _last_ai_run = datetime.utcnow()
        logger.info("[INSIDER_AI] Analysis complete — %d top buys, %d top sells",
                    len(result.get("top_buys", [])), len(result.get("top_sells", [])))
        return result

    except json.JSONDecodeError as e:
        logger.error("[INSIDER_AI] JSON parse error: %s | raw: %.500s", e, raw_content)
        return {"error": f"JSON parse error: {e}", "raw": raw_content[:500]}
    except Exception as e:
        logger.error("[INSIDER_AI] Perplexity error: %s", e, exc_info=True)
        return {"error": str(e)}


async def get_ai_analysis_cached() -> dict:
    """Return cached AI analysis (in-memory → DB → fresh call)."""
    global _ai_cache, _last_ai_run
    loop = asyncio.get_event_loop()

    # In-memory cache valid for today
    if _ai_cache and _last_ai_run and _last_ai_run.date() == datetime.utcnow().date():
        logger.debug("[INSIDER_AI] Serving in-memory AI cache")
        return _ai_cache

    # Try DB cache for today
    cached = await loop.run_in_executor(_executor, _load_ai_cache_from_db)
    if cached:
        _ai_cache = cached
        _last_ai_run = datetime.utcnow()
        logger.info("[INSIDER_AI] Loaded today's analysis from DB cache")
        return cached

    # Nothing cached — run fresh
    logger.info("[INSIDER_AI] No cache found — running fresh Perplexity analysis")
    return await run_perplexity_analysis()


# ── Background Loop ───────────────────────────────────────────────────────────

async def _ai_daily_loop():
    """Runs Perplexity analysis once per day. Waits for initial data to exist first."""
    global _ai_cache, _last_ai_run
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_executor, _create_ai_cache_table)

    # Load existing cached analysis from DB immediately on startup
    try:
        cached = await loop.run_in_executor(_executor, _load_ai_cache_from_db)
        if cached:
            _ai_cache = cached
            _last_ai_run = datetime.utcnow()
            logger.info("[INSIDER_AI] Loaded AI cache from DB on startup (%d buys, %d sells)",
                        len(cached.get("top_buys", [])), len(cached.get("top_sells", [])))
    except Exception as e:
        logger.warning("[INSIDER_AI] Startup cache load failed: %s", e)

    # Short initial delay before running a fresh analysis
    await asyncio.sleep(300)

    while True:
        try:
            await run_perplexity_analysis()
        except Exception as e:
            logger.error("[INSIDER_AI] Daily loop error: %s", e)
        await asyncio.sleep(_AI_INTERVAL)


async def insider_activity_background_loop():
    """
    Two concurrent tasks:
      1. SEC EDGAR fetch every 2 hours (today/yesterday filings only, dedup by accession)
      2. Perplexity AI analysis once per day (stored in DB cache)
    30-day retention: rows expire via expires_at column, cleaned on each cycle.
    """
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_executor, _create_table)
    await loop.run_in_executor(_executor, _create_ai_cache_table)
    await loop.run_in_executor(_executor, _cleanup_expired)
    await maybe_initial_load()

    # Start daily AI analysis as a concurrent background task
    asyncio.ensure_future(_ai_daily_loop())

    # Backfill any null price returns for existing transactions (runs once on startup)
    async def _startup_price_backfill():
        await asyncio.sleep(60)  # short delay to let connections settle
        try:
            result = await loop.run_in_executor(_executor, _sync_backfill_price_returns)
            logger.info("[INSIDER] Startup price backfill: %s", result)
        except Exception as e:
            logger.warning("[INSIDER] Startup price backfill error: %s", e)
    asyncio.ensure_future(_startup_price_backfill())

    while True:
        await asyncio.sleep(_FETCH_INTERVAL)
        try:
            await loop.run_in_executor(_executor, _cleanup_expired)
            await fetch_recent_form4_filings(max_filings=200)
        except Exception as e:
            logger.error("[INSIDER] Background loop error: %s", e)


# ── REST API ─────────────────────────────────────────────────────────────────

router = APIRouter(tags=["insider-activity"])


def _row_to_dict(row, cols: list[str]) -> dict:
    d = dict(zip(cols, row))
    for k in ("score_breakdown", "price_context", "cluster_metadata"):
        if isinstance(d.get(k), str):
            try:
                d[k] = json.loads(d[k])
            except Exception:
                pass
    for k in ("total_value", "price_per_share"):
        if isinstance(d.get(k), Decimal):
            d[k] = float(d[k])
    if d.get("transaction_date"):
        d["transaction_date"] = str(d["transaction_date"])
    if d.get("filing_date"):
        d["filing_date"] = d["filing_date"].isoformat() if hasattr(d["filing_date"], "isoformat") else str(d["filing_date"])
    if d.get("created_at"):
        d["created_at"] = d["created_at"].isoformat() if hasattr(d["created_at"], "isoformat") else str(d["created_at"])
    return d


def _query_transactions(
    tx_type_filter: str = "all",
    timeframe: str = "1m",
    min_score: int | None = None,
    sort: str = "score",
    order: str = "desc",
    search: str | None = None,
    limit: int = 50,
    offset: int = 0,
    cluster_type_filter: str | None = None,
    clustered_only: bool = False,
    sector_filter: str | None = None,
) -> tuple[list[dict], dict]:
    conn = _get_conn()
    if not conn:
        return [], {"total": 0, "total_buys": 0, "total_sales": 0,
                    "avg_buy_score": 0.0, "top_buy_ticker": None,
                    "top_sell_ticker": None, "last_refresh": None}

    timeframe_days = {"1w": 7, "1m": 30, "3m": 90, "6m": 180}.get(timeframe, 30)
    _code_map = {
        "buys": ("P",), "sales": ("S",), "exercises": ("M",), "gifts": ("G",),
        "P": ("P",), "S": ("S",), "M": ("M",), "A": ("A",), "D": ("D",), "G": ("G",),
        "all": None,
    }
    code_filter = _code_map.get(tx_type_filter) if tx_type_filter not in ("all", "") else None

    sort_col = {"score": "conviction_score", "date": "transaction_date",
                "value": "total_value", "ticker": "ticker"}.get(sort, "conviction_score")
    order_dir = "DESC" if order.lower() == "desc" else "ASC"

    conditions = [
        f"transaction_date >= NOW() - INTERVAL '{timeframe_days} days'",
        "ticker IS NOT NULL",
        "ticker != 'NONE'",
        "ticker != ''",
    ]
    params: list = []
    if code_filter:
        conditions.append(f"transaction_code = ANY(%s)")
        params.append(list(code_filter))
    if min_score is not None:
        conditions.append(f"conviction_score >= %s")
        params.append(min_score)
    if search:
        conditions.append("(ticker ILIKE %s OR company_name ILIKE %s OR insider_name ILIKE %s)")
        like = f"%{search}%"
        params.extend([like, like, like])
    if cluster_type_filter:
        conditions.append("cluster_type = %s")
        params.append(cluster_type_filter)
    if clustered_only:
        conditions.append("cluster_id IS NOT NULL")
    if sector_filter:
        conditions.append("sector ILIKE %s")
        params.append(f"%{sector_filter}%")

    where = " AND ".join(conditions)

    cols = ["id", "accession_number", "ticker", "company_name", "insider_name",
            "insider_title", "transaction_type", "transaction_code", "transaction_date",
            "filing_date", "shares", "price_per_share", "total_value", "shares_owned_after",
            "ownership_type", "conviction_score", "score_breakdown", "price_context",
            "cluster_id", "context_tags", "cluster_type", "cluster_metadata",
            "sector", "created_at"]
    cols_str = ", ".join(cols)

    results = []
    summary = {"total": 0, "total_buys": 0, "total_sales": 0, "avg_buy_score": 0.0,
                "top_buy_ticker": None, "top_sell_ticker": None, "last_refresh": _last_refresh.isoformat() if _last_refresh else None}

    try:
        cur = conn.cursor()

        # Main query
        q = (f"SELECT {cols_str} FROM insider_transactions WHERE {where} "
             f"ORDER BY {sort_col} {order_dir} LIMIT %s OFFSET %s")
        cur.execute(q, params + [limit, offset])
        rows = cur.fetchall()
        results = [_row_to_dict(r, cols) for r in rows]

        # Count total
        cur.execute(f"SELECT COUNT(*) FROM insider_transactions WHERE {where}", params)
        summary["total"] = cur.fetchone()[0]

        # Stats (no extra filters)
        cur.execute("""
            SELECT
                SUM(CASE WHEN transaction_code='P' THEN 1 ELSE 0 END),
                SUM(CASE WHEN transaction_code='S' THEN 1 ELSE 0 END),
                AVG(CASE WHEN transaction_code='P' THEN conviction_score END),
                (SELECT ticker FROM insider_transactions WHERE transaction_code='P' ORDER BY conviction_score DESC LIMIT 1),
                (SELECT ticker FROM insider_transactions WHERE transaction_code='S' ORDER BY conviction_score DESC LIMIT 1)
            FROM insider_transactions
        """)
        st = cur.fetchone()
        if st:
            summary["total_buys"] = int(st[0] or 0)
            summary["total_sales"] = int(st[1] or 0)
            summary["avg_buy_score"] = round(float(st[2] or 0), 1)
            summary["top_buy_ticker"] = st[3]
            summary["top_sell_ticker"] = st[4]

        cur.close()
    except Exception as e:
        logger.error("[INSIDER_API] Query error: %s", e)
    finally:
        _put_conn(conn)

    return results, summary


@router.get("/insider-activity")
async def get_insider_activity(
    type: str = Query("all", description="all|buys|sales|exercises|gifts|P|S|M|A|D|G"),
    timeframe: str = Query("1m", description="1w|1m|3m|6m"),
    min_score: Optional[int] = Query(None),
    sort: str = Query("score", description="score|date|value|ticker"),
    order: str = Query("desc"),
    search: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    cluster_type: Optional[str] = Query(None, description="coordinated_buy|coordinated_sell|lockup_expiry|mixed"),
    clustered_only: bool = Query(False, description="Only return transactions that belong to a cluster"),
    sector: Optional[str] = Query(None, description="Filter by sector (partial match)"),
):
    loop = asyncio.get_event_loop()
    transactions, summary = await loop.run_in_executor(
        _executor,
        lambda: _query_transactions(
            type, timeframe, min_score, sort, order, search, limit, offset,
            cluster_type, clustered_only, sector,
        ),
    )
    # Load AI analysis cache to merge ai_score into transactions
    ai_ticker_map: dict[str, int] = {}
    try:
        cached_ai = _ai_cache or {}
        for item in cached_ai.get("top_buys", []):
            tk = item.get("ticker", "")
            sc = item.get("ai_score")
            if tk and sc is not None:
                ai_ticker_map[tk] = int(sc)
        for item in cached_ai.get("top_sells", []):
            tk = item.get("ticker", "")
            sc = item.get("ai_score")
            if tk and sc is not None and tk not in ai_ticker_map:
                ai_ticker_map[tk] = int(sc)
    except Exception:
        pass

    # Build enriched response with all aliases the frontend needs
    enriched = []
    for t in transactions:
        pc = t.get("price_context") or {}
        cm = t.get("cluster_metadata") or {}

        t["pct_change_since"] = pc.get("change_since_filing_pct")

        # cluster_size
        if cm.get("cluster_size"):
            t["cluster_size"] = cm["cluster_size"]
        else:
            tags = t.get("context_tags") or []
            cluster_tag = next((tg for tg in tags if "insiders" in tg), None)
            t["cluster_size"] = int(cluster_tag.split()[0]) if cluster_tag else 1

        # Merge Perplexity ai_score
        ticker = t.get("ticker", "")
        ai_sc = ai_ticker_map.get(ticker)
        t["ai_score"] = ai_sc

        # score = Perplexity ai_score when available, fallback to rule-based conviction_score
        t["score"] = ai_sc if ai_sc is not None else t.get("conviction_score")

        # Price aliases
        raw_price = t.get("price_per_share")
        t["price"] = float(raw_price) if raw_price is not None else pc.get("current_price")

        # Holdings aliases
        shares_after = t.get("shares_owned_after")
        t["post_tx_holdings"] = shares_after

        # % of holdings = shares traded / (shares traded + shares held after) * 100
        shares_traded = t.get("shares") or 0
        if shares_after and shares_after > 0 and shares_traded:
            total_before = shares_traded + shares_after
            t["pct_of_holdings"] = round(shares_traded / total_before * 100, 1)
        else:
            t["pct_of_holdings"] = None

        # Price context aliases for frontend
        t["return_30d"] = pc.get("return_30d")
        t["return_90d"] = pc.get("return_90d")
        t["vs_52w_high"] = pc.get("vs_52w_high")
        t["current_price"] = pc.get("current_price")

        enriched.append(t)

    return {
        "summary": {
            "total_transactions": summary["total"],
            "total_buys": summary["total_buys"],
            "total_sales": summary["total_sales"],
            "avg_buy_score": summary["avg_buy_score"],
            "top_buy_ticker": summary["top_buy_ticker"],
            "top_sell_ticker": summary["top_sell_ticker"],
            "last_refresh": summary["last_refresh"],
        },
        "transactions": enriched,
        "pagination": {
            "total": summary["total"],
            "limit": limit,
            "offset": offset,
            "has_more": (offset + limit) < summary["total"],
        },
    }


@router.get("/insider-activity/stats")
async def get_insider_stats():
    loop = asyncio.get_event_loop()

    def _stats():
        conn = _get_conn()
        if not conn:
            return {}
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN transaction_code='P' THEN 1 ELSE 0 END) as buys,
                    SUM(CASE WHEN transaction_code='S' THEN 1 ELSE 0 END) as sales,
                    AVG(CASE WHEN transaction_code='P' THEN conviction_score END) as avg_buy_score
                FROM insider_transactions
            """)
            row = cur.fetchone()
            cur.execute("""
                SELECT ticker, conviction_score FROM insider_transactions
                WHERE transaction_code='P' ORDER BY conviction_score DESC LIMIT 1
            """)
            top_buy = cur.fetchone()
            cur.execute("""
                SELECT ticker, conviction_score FROM insider_transactions
                WHERE transaction_code='S' ORDER BY conviction_score DESC LIMIT 1
            """)
            top_sell = cur.fetchone()
            cur.close()
            return {
                "total_transactions": int(row[0] or 0),
                "buys": int(row[1] or 0),
                "sales": int(row[2] or 0),
                "avg_buy_score": round(float(row[3] or 0), 1),
                "top_buy": {"ticker": top_buy[0], "score": top_buy[1]} if top_buy else None,
                "top_sell": {"ticker": top_sell[0], "score": top_sell[1]} if top_sell else None,
                "last_refresh": _last_refresh.isoformat() if _last_refresh else None,
                "refresh_in_progress": _refresh_in_progress,
            }
        except Exception as e:
            logger.error("[INSIDER_API] Stats error: %s", e)
            return {}
        finally:
            _put_conn(conn)

    return await loop.run_in_executor(_executor, _stats)


def _normalize_standout(val: Any, label: str) -> dict | None:
    """Convert old plain-string standout to {ticker, summary} object."""
    if val is None:
        return None
    if isinstance(val, dict):
        return val
    if isinstance(val, str) and val.strip():
        s = val.strip()
        ticker = s.split(" ")[0].split("-")[0].strip().upper()
        summary = s[len(ticker):].lstrip(" -–:").strip() or s
        return {"ticker": ticker, "summary": summary}
    return None


@router.get("/insider-activity/ai-analysis")
async def get_ai_analysis(request: Request, _sub: None = Depends(require_subscription)):
    """
    Returns today's Perplexity Sonar Pro analysis of insider transactions.
    Checks in-memory cache → DB cache → fresh API call.
    Refreshes automatically once per day via the background loop.
    """
    if not _PPLX_KEY:
        raise HTTPException(status_code=503, detail="Perplexity API key not configured")
    result = await get_ai_analysis_cached()
    if "error" in result and len(result) == 1:
        raise HTTPException(status_code=502, detail=result["error"])

    result = dict(result)
    result["standout_buy"] = _normalize_standout(result.get("standout_buy"), "buy")
    result["standout_sell"] = _normalize_standout(result.get("standout_sell"), "sell")
    return result


@router.get("/insider-activity/detail/{accession_number}")
async def get_insider_detail(accession_number: str):
    loop = asyncio.get_event_loop()

    def _fetch():
        conn = _get_conn()
        if not conn:
            return None
        try:
            cols = ["id", "accession_number", "ticker", "company_name", "insider_name",
                    "insider_title", "transaction_type", "transaction_code", "transaction_date",
                    "filing_date", "shares", "price_per_share", "total_value", "shares_owned_after",
                    "ownership_type", "conviction_score", "score_breakdown", "price_context",
                    "cluster_id", "context_tags", "sector", "created_at"]
            cur = conn.cursor()
            cur.execute(
                f"SELECT {', '.join(cols)} FROM insider_transactions WHERE accession_number = %s",
                (accession_number,)
            )
            row = cur.fetchone()
            cur.close()
            return _row_to_dict(row, cols) if row else None
        except Exception as e:
            logger.error("[INSIDER_API] Detail error: %s", e)
            return None
        finally:
            _put_conn(conn)

    result = await loop.run_in_executor(_executor, _fetch)
    if not result:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return result


@router.get("/insider-activity/{ticker}")
async def get_insider_by_ticker(ticker: str):
    loop = asyncio.get_event_loop()
    ticker = ticker.upper()

    def _fetch():
        conn = _get_conn()
        if not conn:
            return [], {}
        try:
            cols = ["id", "accession_number", "ticker", "company_name", "insider_name",
                    "insider_title", "transaction_type", "transaction_code", "transaction_date",
                    "filing_date", "shares", "price_per_share", "total_value", "shares_owned_after",
                    "ownership_type", "conviction_score", "score_breakdown", "price_context",
                    "cluster_id", "context_tags", "cluster_type", "cluster_metadata",
                    "sector", "created_at"]
            cur = conn.cursor()
            cur.execute(
                f"SELECT {', '.join(cols)} FROM insider_transactions WHERE ticker = %s "
                f"ORDER BY conviction_score DESC, transaction_date DESC LIMIT 100",
                (ticker,)
            )
            rows = [_row_to_dict(r, cols) for r in cur.fetchall()]
            company_name = rows[0]["company_name"] if rows else ticker

            # Insider summary (30 days)
            cur.execute("""
                SELECT
                    COUNT(DISTINCT insider_name),
                    COALESCE(SUM(CASE WHEN transaction_code='P' THEN total_value ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN transaction_code='S' THEN total_value ELSE 0 END), 0)
                FROM insider_transactions
                WHERE ticker = %s AND transaction_date >= NOW() - INTERVAL '30 days'
            """, (ticker,))
            st = cur.fetchone()
            buy_val = float(st[1] or 0)
            sell_val = float(st[2] or 0)
            if buy_val > sell_val * 1.5:
                direction = "buying"
            elif sell_val > buy_val * 1.5:
                direction = "selling"
            else:
                direction = "mixed"

            cur.close()
            summary = {
                "total_insiders_active": int(st[0] or 0),
                "net_direction": direction,
                "total_buy_value_30d": buy_val,
                "total_sell_value_30d": sell_val,
            }
            return rows, summary
        except Exception as e:
            logger.error("[INSIDER_API] Ticker fetch error: %s", e)
            return [], {}
        finally:
            _put_conn(conn)

    transactions, insider_summary = await loop.run_in_executor(_executor, _fetch)
    company_name = transactions[0]["company_name"] if transactions else ticker
    return {
        "ticker": ticker,
        "company_name": company_name,
        "transactions": transactions,
        "insider_summary": insider_summary,
    }


def _sync_backfill_scores() -> dict:
    """
    Backfill existing rows with cluster detection + recalculated conviction scores.
    - Detects multi-insider clusters (CSTL=7, AIR=6, SE=6, etc.) and sets cluster_id/type
    - Recalculates conviction_score using current weights + cluster bonus
    - Updates context_tags with cluster-aware labels
    Returns {"updated": N, "clusters_found": M, "errors": K}
    """
    conn = _get_conn()
    if not conn:
        return {"updated": 0, "clusters_found": 0, "errors": 1, "message": "No DB connection"}
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, accession_number, ticker, transaction_code, transaction_date,
                   total_value, shares, shares_owned_after, insider_title, insider_name,
                   score_breakdown, price_context
            FROM insider_transactions
            ORDER BY ticker, transaction_date
        """)
        rows = cur.fetchall()
        cur.close()
    except Exception as e:
        logger.error("[INSIDER_BACKFILL] Fetch error: %s", e)
        return {"updated": 0, "clusters_found": 0, "errors": 1, "message": str(e)}
    finally:
        _put_conn(conn)

    updates = []
    errors = 0
    clusters_found = 0

    for row in rows:
        (row_id, accession, ticker, code, tx_date, total_value, shares, shares_after,
         title, insider_name, score_bd, price_ctx) = row

        try:
            # Detect cluster for this row (queries other rows for same ticker ±14 days)
            acc_prefix = (accession or "").split(":")[0]
            cluster_info = _detect_cluster_type(ticker or "", tx_date, acc_prefix)
            cluster_count = cluster_info["count"]
            cluster_type = cluster_info["type"]
            cluster_metadata = cluster_info["metadata"] if cluster_info.get("metadata") else None

            # Get or assign cluster_id
            cluster_id = None
            if cluster_count >= 2:
                cluster_id = _get_or_assign_cluster_id(ticker or "", tx_date)
                clusters_found += 1

            # Get filing count for this insider
            filing_count = _count_insider_filings(insider_name or "")

            # Extract stored component scores (for non-changing factors)
            bd = score_bd or {}
            role_detail = bd.get("role", {}).get("detail", "")
            # Re-derive role score using current weights (handles 10% Owner 15→18, etc.)
            if "10%" in role_detail or "10 %" in role_detail:
                s_role, _ = _score_role("", False, False, True)
            elif any(k in (title or "").upper() for k in ["CEO", "CHIEF EXECUTIVE", "CHAIRMAN", "EXEC CHAIR"]):
                s_role = 20
            elif any(k in (title or "").upper() for k in ["PRESIDENT", "COO", "CHIEF OPERATING"]):
                s_role = 18
            elif any(k in (title or "").upper() for k in ["CFO", "CTO", "CMO", "CHIEF FINANCIAL", "CHIEF TECH", "CHIEF MARKET"]):
                s_role = 16
            elif any(k in (title or "").upper() for k in ["SVP", "EVP", "SENIOR VP", "EXECUTIVE VP"]):
                s_role = 12
            elif any(k in (title or "").upper() for k in ["VP ", "VICE PRESIDENT"]):
                s_role = 10
            else:
                # Fall back to stored score (Director, Officer, other)
                s_role = bd.get("role", {}).get("score", 0)
            s_type = bd.get("type", {}).get("score", 0)
            s_ctx = bd.get("context", {}).get("score", 0)
            s_pos = bd.get("position_impact", {}).get("score", 0)
            s_event = bd.get("event_proximity", {}).get("score", 0)

            # Recalculate size with current thresholds (max=15 instead of old 20)
            tv = float(total_value or 0)
            new_size, d_size = _score_size(tv)

            # Recalculate track_record with current filing count
            new_track, d_track = _score_track_record(filing_count)

            # Recalculate cluster score with detected cluster info
            new_cluster, d_cluster = _score_cluster(cluster_count, cluster_type)

            # New conviction score (cap at 100)
            new_total = min(s_role + s_type + s_ctx + s_pos + s_event + new_size + new_track + new_cluster, 100)

            # Build updated score_breakdown (preserving stored detail strings for unchanged components)
            new_bd = {
                "size":            {"score": new_size,    "max": _W_SIZE,    "detail": d_size},
                "role":            {"score": s_role,      "max": _W_ROLE,    "detail": bd.get("role", {}).get("detail", "")},
                "type":            {"score": s_type,      "max": _W_TYPE,    "detail": bd.get("type", {}).get("detail", "")},
                "context":         {"score": s_ctx,       "max": _W_CONTEXT, "detail": bd.get("context", {}).get("detail", "")},
                "cluster":         {"score": new_cluster, "max": _W_CLUSTER, "detail": d_cluster,
                                    "cluster_type": cluster_type, "cluster_size": cluster_count},
                "position_impact": {"score": s_pos,       "max": _W_POSITION,"detail": bd.get("position_impact", {}).get("detail", "")},
                "track_record":    {"score": new_track,   "max": _W_TRACK,   "detail": d_track},
                "event_proximity": {"score": s_event,     "max": _W_EVENT,   "detail": bd.get("event_proximity", {}).get("detail", "")},
            }

            # Regenerate context_tags with cluster awareness
            role_score = s_role
            is_ten_pct = "10%" in role_detail or role_score >= 18
            is_director = (role_score == 8) and not any(
                k in (title or "").upper() for k in ["CEO", "CFO", "CTO", "CMO", "PRESIDENT", "COO", "CHAIRMAN", "SVP", "EVP", "VP"]
            )
            date_spread = (cluster_metadata or {}).get("date_spread_days", 0)
            distinct_roles = (cluster_metadata or {}).get("distinct_role_count", 1)
            new_tags = generate_context_tags(
                code=code or "S",
                title=title or "",
                is_director=is_director,
                is_ten_pct=is_ten_pct,
                total_value=tv,
                cluster_count=cluster_count,
                shares=int(shares or 0),
                shares_after=int(shares_after or 0),
                cluster_type=cluster_type,
                date_spread_days=date_spread,
                distinct_role_count=distinct_roles,
            )

            updates.append({
                "id": row_id,
                "conviction_score": new_total,
                "score_breakdown": json.dumps(new_bd),
                "cluster_id": cluster_id,
                "cluster_type": cluster_type,
                "cluster_metadata": json.dumps(cluster_metadata) if cluster_metadata else None,
                "context_tags": new_tags,
            })

        except Exception as e:
            logger.warning("[INSIDER_BACKFILL] Row %s error: %s", row_id, e)
            errors += 1

    if not updates:
        return {"updated": 0, "clusters_found": clusters_found, "errors": errors,
                "message": "No rows to update"}

    # Apply batch updates
    conn2 = _get_conn()
    if not conn2:
        return {"updated": 0, "clusters_found": clusters_found, "errors": errors + 1,
                "message": "DB connection lost before update"}
    try:
        cur2 = conn2.cursor()
        execute_batch(cur2, """
            UPDATE insider_transactions SET
                conviction_score = %(conviction_score)s,
                score_breakdown  = %(score_breakdown)s::jsonb,
                cluster_id       = %(cluster_id)s,
                cluster_type     = %(cluster_type)s,
                cluster_metadata = CASE WHEN %(cluster_metadata)s IS NULL THEN NULL
                                        ELSE %(cluster_metadata)s::jsonb END,
                context_tags     = %(context_tags)s
            WHERE id = %(id)s
        """, updates, page_size=50)
        conn2.commit()
        cur2.close()
        logger.info("[INSIDER_BACKFILL] Updated %d rows, clusters_found=%d, errors=%d",
                    len(updates), clusters_found, errors)
        return {"updated": len(updates), "clusters_found": clusters_found,
                "errors": errors, "message": "Backfill complete"}
    except Exception as e:
        logger.error("[INSIDER_BACKFILL] Update error: %s", e)
        try:
            conn2.rollback()
        except Exception:
            pass
        return {"updated": 0, "clusters_found": clusters_found, "errors": errors + 1,
                "message": str(e)}
    finally:
        _put_conn(conn2)


@router.post("/insider-activity/backfill-scores")
async def backfill_conviction_scores():
    """
    Re-detect clusters and recalculate conviction scores for all existing rows.
    Fixes: (1) cluster_id=NULL for rows inserted before cluster detection was added,
           (2) conviction scores calculated with old weight system.
    This is idempotent — safe to run multiple times.
    """
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(_executor, _sync_backfill_scores)
    return result


@router.post("/insider-activity/backfill-prices")
async def backfill_price_returns(background_tasks: BackgroundTasks):
    """
    Fetch Tradier historical prices to populate return_30d / return_90d for
    all transactions where those fields are currently null.
    Runs in the background — typically completes in 30-60s.
    """
    loop = asyncio.get_event_loop()

    async def _run():
        result = await loop.run_in_executor(_executor, _sync_backfill_price_returns)
        logger.info("[INSIDER] Manual price backfill result: %s", result)

    background_tasks.add_task(_run)
    return {"status": "started", "message": "Price return backfill running in background. Poll /api/insider-activity/stats for progress."}


@router.post("/insider-activity/fetch-historical")
async def trigger_historical_fetch(
    background_tasks: BackgroundTasks,
    days_back: int = Query(30, ge=7, le=90, description="Days of history to fetch (7-90)"),
    filings_per_week: int = Query(60, ge=10, le=150, description="Max filings per weekly window"),
):
    """
    Kicks off a background job that sweeps the past `days_back` days in 7-day
    windows, fetching up to `filings_per_week` Form 4 filings per window.
    Safe to call repeatedly — duplicates are skipped by accession number.
    Automatically runs a backfill after insertion to recalculate cluster scores.
    """
    if _refresh_in_progress:
        return {"status": "already_running", "message": "A fetch is already in progress"}

    async def _run():
        result = await fetch_historical_30d(days_back=days_back, filings_per_week=filings_per_week)
        if result.get("inserted", 0) > 0:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(_executor, _sync_backfill_scores)
            logger.info("[INSIDER_HIST] Post-insert backfill complete")

    background_tasks.add_task(_run)
    windows = (days_back // 7) + (1 if days_back % 7 else 0)
    est_secs = windows * filings_per_week * 0.6
    return {
        "status": "started",
        "days_back": days_back,
        "weekly_windows": windows,
        "filings_per_week": filings_per_week,
        "estimated_seconds": int(est_secs),
        "message": f"Fetching {days_back} days of Form 4 history across {windows} weekly windows (~{int(est_secs)}s). Poll /api/insider-activity/stats to track progress.",
    }


@router.post("/insider-activity/refresh")
async def trigger_refresh(background_tasks: BackgroundTasks):
    if _refresh_in_progress:
        return {"status": "already_running", "message": "Refresh already in progress"}
    background_tasks.add_task(fetch_recent_form4_filings, 200)
    return {"status": "refresh_started", "estimated_time_seconds": 120}


@router.post("/insider-activity/refresh-ai")
async def trigger_ai_refresh(request: Request, background_tasks: BackgroundTasks, _sub: None = Depends(require_subscription)):
    """
    Manually trigger a fresh Perplexity AI analysis (bypasses the 24-hour cache).
    Runs in the background — poll GET /ai-analysis to see when results appear.
    """
    if not _PPLX_KEY:
        raise HTTPException(status_code=503, detail="Perplexity API key not configured")

    async def _run():
        global _ai_cache, _last_ai_run
        _ai_cache = None
        _last_ai_run = None
        await run_perplexity_analysis()

    background_tasks.add_task(_run)
    return {
        "status": "started",
        "model": _PPLX_MODEL,
        "message": "Perplexity AI analysis running in background (~30-60s). "
                   "Poll GET /api/insider-activity/ai-analysis to see results.",
    }
