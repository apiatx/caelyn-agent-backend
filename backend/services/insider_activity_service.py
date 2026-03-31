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
from fastapi import APIRouter, Query, HTTPException, BackgroundTasks
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
_SEC_DELAY = 0.15          # seconds between SEC requests (max 10 req/s)
_FETCH_INTERVAL = 7200     # 2 hours in seconds
_RETENTION_DAYS = 30
_BATCH_SIZE = 50           # filings processed per batch
_KEEP_CODES = {"P", "S", "M", "A", "D", "G"}  # transaction codes to keep

# Scoring weights (adjustable)
_W_SIZE = 20
_W_ROLE = 20
_W_TYPE = 10
_W_CONTEXT = 15
_W_CLUSTER = 10
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
                        cluster_id, context_tags, sector, expires_at
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s
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
        return 20, f"${total_value/1e6:.1f}M total value"
    elif total_value >= 1_000_000:
        return 15, f"${total_value/1e6:.1f}M total value"
    elif total_value >= 250_000:
        return 10, f"${total_value/1e3:.0f}K total value"
    elif total_value >= 50_000:
        return 6, f"${total_value/1e3:.0f}K total value"
    else:
        return 2, f"${total_value:.0f} total value"


def _score_role(title: str, is_director: bool, is_officer: bool, is_ten_pct: bool) -> tuple[int, str]:
    t = (title or "").upper()
    if is_ten_pct:
        return 15, "10% Owner"
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


def _score_cluster(cluster_count: int) -> tuple[int, str]:
    if cluster_count >= 5:
        return 10, f"{cluster_count} insiders in 14-day window"
    elif cluster_count >= 3:
        return 7, f"{cluster_count} insiders in 14-day window"
    elif cluster_count >= 2:
        return 4, f"{cluster_count} insiders in 14-day window"
    return 0, "Single insider transaction"


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
) -> tuple[int, dict]:
    s_size, d_size = _score_size(total_value)
    s_role, d_role = _score_role(title, is_director, is_officer, is_ten_pct)
    s_type, d_type = _score_type(code)
    s_ctx, d_ctx = _score_context(code, price_context)
    s_cluster, d_cluster = _score_cluster(cluster_count)
    s_pos, d_pos = _score_position(code, shares, shares_after)
    s_track, d_track = _score_track_record(filing_count)
    s_event, d_event = _score_event_proximity(code, has_earnings_nearby)

    total = s_size + s_role + s_type + s_ctx + s_cluster + s_pos + s_track + s_event
    breakdown = {
        "size":           {"score": s_size,    "max": _W_SIZE,    "detail": d_size},
        "role":           {"score": s_role,    "max": _W_ROLE,    "detail": d_role},
        "type":           {"score": s_type,    "max": _W_TYPE,    "detail": d_type},
        "context":        {"score": s_ctx,     "max": _W_CONTEXT, "detail": d_ctx},
        "cluster":        {"score": s_cluster, "max": _W_CLUSTER, "detail": d_cluster},
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

    return results


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


# ── SEC EDGAR Fetch ───────────────────────────────────────────────────────────

# Global last_refresh tracking
_last_refresh: datetime | None = None
_refresh_in_progress = False
_total_inserted = 0


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
        logger.info("[INSIDER] Fetching Form 4 filings from SEC EDGAR...")
        loop = asyncio.get_event_loop()

        def _get_filings_sync():
            return get_filings(form="4")

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

            cluster_count = await loop.run_in_executor(
                _executor, _count_cluster, ticker, r["transaction_date"]
            )
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
            )

            tags = generate_context_tags(
                code=code,
                title=r.get("insider_title") or "",
                is_director=r.get("is_director", False),
                is_ten_pct=r.get("is_ten_pct", False),
                total_value=total_value,
                cluster_count=cluster_count,
                shares=shares,
                shares_after=shares_after,
            )

            cluster_id = None
            if cluster_count >= 2:
                cluster_id = await loop.run_in_executor(
                    _executor, _get_or_assign_cluster_id, ticker, r["transaction_date"]
                )

            sector = (price_ctx or {}).get("sector")
            record = {**r, "conviction_score": score, "score_breakdown": breakdown,
                      "price_context": price_ctx, "cluster_id": cluster_id,
                      "context_tags": tags, "sector": sector}
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


# ── Initial Load ──────────────────────────────────────────────────────────────

async def maybe_initial_load():
    """On cold start, if table is empty, do a bulk historical load."""
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

    if count == 0:
        logger.info("[INSIDER] Table empty — running initial load (max 300 filings)")
        await fetch_recent_form4_filings(max_filings=300)
    else:
        logger.info("[INSIDER] Table has %d rows — skipping initial load", count)


# ── Background Loop ───────────────────────────────────────────────────────────

async def insider_activity_background_loop():
    """Runs every 2 hours. Creates table, cleans up, fetches new filings."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_executor, _create_table)
    await loop.run_in_executor(_executor, _cleanup_expired)
    await maybe_initial_load()

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
    for k in ("score_breakdown", "price_context"):
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
) -> tuple[list[dict], dict]:
    conn = _get_conn()
    if not conn:
        return [], {"total": 0, "total_buys": 0, "total_sales": 0,
                    "avg_buy_score": 0.0, "top_buy_ticker": None,
                    "top_sell_ticker": None, "last_refresh": None}

    timeframe_days = {"1w": 7, "1m": 30, "3m": 90, "6m": 180}.get(timeframe, 30)
    code_filter = {"buys": ("P",), "sales": ("S",), "exercises": ("M",),
                   "gifts": ("G",)}.get(tx_type_filter)

    sort_col = {"score": "conviction_score", "date": "transaction_date",
                "value": "total_value", "ticker": "ticker"}.get(sort, "conviction_score")
    order_dir = "DESC" if order.lower() == "desc" else "ASC"

    conditions = [f"transaction_date >= NOW() - INTERVAL '{timeframe_days} days'"]
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

    where = " AND ".join(conditions)

    cols = ["id", "accession_number", "ticker", "company_name", "insider_name",
            "insider_title", "transaction_type", "transaction_code", "transaction_date",
            "filing_date", "shares", "price_per_share", "total_value", "shares_owned_after",
            "ownership_type", "conviction_score", "score_breakdown", "price_context",
            "cluster_id", "context_tags", "sector", "created_at"]
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
    type: str = Query("all", description="all|buys|sales|exercises|gifts"),
    timeframe: str = Query("1m", description="1w|1m|3m|6m"),
    min_score: Optional[int] = Query(None),
    sort: str = Query("score", description="score|date|value|ticker"),
    order: str = Query("desc"),
    search: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    loop = asyncio.get_event_loop()
    transactions, summary = await loop.run_in_executor(
        _executor,
        lambda: _query_transactions(type, timeframe, min_score, sort, order, search, limit, offset),
    )
    # Build response with pct_change_since and cluster_size
    enriched = []
    for t in transactions:
        pc = t.get("price_context") or {}
        t["pct_change_since"] = pc.get("change_since_filing_pct")
        # Count cluster
        tags = t.get("context_tags") or []
        cluster_tag = next((tg for tg in tags if "insiders" in tg), None)
        t["cluster_size"] = int(cluster_tag.split()[0]) if cluster_tag else 1
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
                    "cluster_id", "context_tags", "sector", "created_at"]
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


@router.post("/insider-activity/refresh")
async def trigger_refresh(background_tasks: BackgroundTasks):
    if _refresh_in_progress:
        return {"status": "already_running", "message": "Refresh already in progress"}
    background_tasks.add_task(fetch_recent_form4_filings, 200)
    return {"status": "refresh_started", "estimated_time_seconds": 120}
