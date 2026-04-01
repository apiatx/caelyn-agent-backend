"""
Congressional Trading Service — STOCK Act Disclosure Tracker
Fetches congressional stock trades from FMP (senate-latest, house-latest),
stores in Neon PostgreSQL with 30-day retention, exposes REST API endpoints.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Optional

import httpx
import psycopg2
from psycopg2 import pool as _pg_pool
from psycopg2.extras import execute_batch
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query

logger = logging.getLogger("congressional_trading")

# ── Constants ────────────────────────────────────────────────────────────────

_FMP_KEY = os.getenv("FMP_API_KEY", "")
_FMP_BASE = "https://financialmodelingprep.com/stable"
_TRADIER_KEY = os.getenv("TRADIER_API_KEY", "")
_FINNHUB_KEY = os.getenv("FINNHUB_API_KEY", "")

_FETCH_INTERVAL = 14400   # 4 hours
_RETENTION_DAYS = 30
_REFRESH_LIMIT = 100      # records per FMP endpoint per call

_last_refresh: datetime | None = None
_refresh_in_progress = False

_DB_URL = os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL")
_pool: _pg_pool.SimpleConnectionPool | None = None
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="cong_trade")

# ── Known politician party/state/chamber lookup ───────────────────────────────
# Covers the most active congressional traders. Lookup by lowercase full name.

_POLITICIAN_INFO: dict[str, dict] = {
    # Senate Democrats
    "jon ossoff": {"party": "Democrat", "chamber": "Senate", "state": "GA"},
    "mark warner": {"party": "Democrat", "chamber": "Senate", "state": "VA"},
    "jacky rosen": {"party": "Democrat", "chamber": "Senate", "state": "NV"},
    "angus king": {"party": "Independent", "chamber": "Senate", "state": "ME"},
    "sheldon whitehouse": {"party": "Democrat", "chamber": "Senate", "state": "RI"},
    "tim kaine": {"party": "Democrat", "chamber": "Senate", "state": "VA"},
    "chris van hollen": {"party": "Democrat", "chamber": "Senate", "state": "MD"},
    "john hickenlooper": {"party": "Democrat", "chamber": "Senate", "state": "CO"},
    "gary peters": {"party": "Democrat", "chamber": "Senate", "state": "MI"},
    "michael bennet": {"party": "Democrat", "chamber": "Senate", "state": "CO"},
    "elizabeth warren": {"party": "Democrat", "chamber": "Senate", "state": "MA"},
    "bernie sanders": {"party": "Independent", "chamber": "Senate", "state": "VT"},
    "amy klobuchar": {"party": "Democrat", "chamber": "Senate", "state": "MN"},
    "jeanne shaheen": {"party": "Democrat", "chamber": "Senate", "state": "NH"},
    "maggie hassan": {"party": "Democrat", "chamber": "Senate", "state": "NH"},
    "chris murphy": {"party": "Democrat", "chamber": "Senate", "state": "CT"},
    "bob casey": {"party": "Democrat", "chamber": "Senate", "state": "PA"},
    "sherrod brown": {"party": "Democrat", "chamber": "Senate", "state": "OH"},
    "joe manchin": {"party": "Democrat", "chamber": "Senate", "state": "WV"},
    "kyrsten sinema": {"party": "Independent", "chamber": "Senate", "state": "AZ"},
    "mark kelly": {"party": "Democrat", "chamber": "Senate", "state": "AZ"},
    "raphael warnock": {"party": "Democrat", "chamber": "Senate", "state": "GA"},
    "alex padilla": {"party": "Democrat", "chamber": "Senate", "state": "CA"},
    "john fetterman": {"party": "Democrat", "chamber": "Senate", "state": "PA"},
    "peter welch": {"party": "Democrat", "chamber": "Senate", "state": "VT"},
    # Senate Republicans
    "tommy tuberville": {"party": "Republican", "chamber": "Senate", "state": "AL"},
    "dave mccormick": {"party": "Republican", "chamber": "Senate", "state": "PA"},
    "dan sullivan": {"party": "Republican", "chamber": "Senate", "state": "AK"},
    "rand paul": {"party": "Republican", "chamber": "Senate", "state": "KY"},
    "mitch mcconnell": {"party": "Republican", "chamber": "Senate", "state": "KY"},
    "lindsey graham": {"party": "Republican", "chamber": "Senate", "state": "SC"},
    "marco rubio": {"party": "Republican", "chamber": "Senate", "state": "FL"},
    "ted cruz": {"party": "Republican", "chamber": "Senate", "state": "TX"},
    "john cornyn": {"party": "Republican", "chamber": "Senate", "state": "TX"},
    "thom tillis": {"party": "Republican", "chamber": "Senate", "state": "NC"},
    "richard burr": {"party": "Republican", "chamber": "Senate", "state": "NC"},
    "jim inhofe": {"party": "Republican", "chamber": "Senate", "state": "OK"},
    "pat toomey": {"party": "Republican", "chamber": "Senate", "state": "PA"},
    "kelly loeffler": {"party": "Republican", "chamber": "Senate", "state": "GA"},
    "david perdue": {"party": "Republican", "chamber": "Senate", "state": "GA"},
    "john hoeven": {"party": "Republican", "chamber": "Senate", "state": "ND"},
    "shelley moore capito": {"party": "Republican", "chamber": "Senate", "state": "WV"},
    "bill hagerty": {"party": "Republican", "chamber": "Senate", "state": "TN"},
    "marsha blackburn": {"party": "Republican", "chamber": "Senate", "state": "TN"},
    "mike braun": {"party": "Republican", "chamber": "Senate", "state": "IN"},
    "ron johnson": {"party": "Republican", "chamber": "Senate", "state": "WI"},
    "rick scott": {"party": "Republican", "chamber": "Senate", "state": "FL"},
    "chuck grassley": {"party": "Republican", "chamber": "Senate", "state": "IA"},
    "roger wicker": {"party": "Republican", "chamber": "Senate", "state": "MS"},
    "john barrasso": {"party": "Republican", "chamber": "Senate", "state": "WY"},
    "mike rounds": {"party": "Republican", "chamber": "Senate", "state": "SD"},
    "kevin cramer": {"party": "Republican", "chamber": "Senate", "state": "ND"},
    "james lankford": {"party": "Republican", "chamber": "Senate", "state": "OK"},
    "steve daines": {"party": "Republican", "chamber": "Senate", "state": "MT"},
    "lisa murkowski": {"party": "Republican", "chamber": "Senate", "state": "AK"},
    "susan collins": {"party": "Republican", "chamber": "Senate", "state": "ME"},
    "mitt romney": {"party": "Republican", "chamber": "Senate", "state": "UT"},
    "j.d. vance": {"party": "Republican", "chamber": "Senate", "state": "OH"},
    "jd vance": {"party": "Republican", "chamber": "Senate", "state": "OH"},
    "katie boyd britt": {"party": "Republican", "chamber": "Senate", "state": "AL"},
    "pete ricketts": {"party": "Republican", "chamber": "Senate", "state": "NE"},
    "eric schmitt": {"party": "Republican", "chamber": "Senate", "state": "MO"},
    # House Democrats
    "nancy pelosi": {"party": "Democrat", "chamber": "House", "state": "CA"},
    "jared moskowitz": {"party": "Democrat", "chamber": "House", "state": "FL"},
    "josh gottheimer": {"party": "Democrat", "chamber": "House", "state": "NJ"},
    "ro khanna": {"party": "Democrat", "chamber": "House", "state": "CA"},
    "seth moulton": {"party": "Democrat", "chamber": "House", "state": "MA"},
    "suzan delbene": {"party": "Democrat", "chamber": "House", "state": "WA"},
    "raja krishnamoorthi": {"party": "Democrat", "chamber": "House", "state": "IL"},
    "adam schiff": {"party": "Democrat", "chamber": "House", "state": "CA"},
    "hakeem jeffries": {"party": "Democrat", "chamber": "House", "state": "NY"},
    "steny hoyer": {"party": "Democrat", "chamber": "House", "state": "MD"},
    "jim himes": {"party": "Democrat", "chamber": "House", "state": "CT"},
    "dan kildee": {"party": "Democrat", "chamber": "House", "state": "MI"},
    "alan lowenthal": {"party": "Democrat", "chamber": "House", "state": "CA"},
    "lois frankel": {"party": "Democrat", "chamber": "House", "state": "FL"},
    "brad sherman": {"party": "Democrat", "chamber": "House", "state": "CA"},
    "maxine waters": {"party": "Democrat", "chamber": "House", "state": "CA"},
    "bill foster": {"party": "Democrat", "chamber": "House", "state": "IL"},
    "john sarbanes": {"party": "Democrat", "chamber": "House", "state": "MD"},
    "mike quigley": {"party": "Democrat", "chamber": "House", "state": "IL"},
    "kurt schrader": {"party": "Democrat", "chamber": "House", "state": "OR"},
    # House Republicans
    "michael mccaul": {"party": "Republican", "chamber": "House", "state": "TX"},
    "virginia foxx": {"party": "Republican", "chamber": "House", "state": "NC"},
    "kevin brady": {"party": "Republican", "chamber": "House", "state": "TX"},
    "greg gianforte": {"party": "Republican", "chamber": "House", "state": "MT"},
    "david rouzer": {"party": "Republican", "chamber": "House", "state": "NC"},
    "chip roy": {"party": "Republican", "chamber": "House", "state": "TX"},
    "jim jordan": {"party": "Republican", "chamber": "House", "state": "OH"},
    "kevin mccarthy": {"party": "Republican", "chamber": "House", "state": "CA"},
    "mark green": {"party": "Republican", "chamber": "House", "state": "TN"},
    "pat fallon": {"party": "Republican", "chamber": "House", "state": "TX"},
    "tom cole": {"party": "Republican", "chamber": "House", "state": "OK"},
    "darrell issa": {"party": "Republican", "chamber": "House", "state": "CA"},
    "michael waltz": {"party": "Republican", "chamber": "House", "state": "FL"},
    "brian mast": {"party": "Republican", "chamber": "House", "state": "FL"},
    "ann wagner": {"party": "Republican", "chamber": "House", "state": "MO"},
    "french hill": {"party": "Republican", "chamber": "House", "state": "AR"},
    "bill huizenga": {"party": "Republican", "chamber": "House", "state": "MI"},
    "warren davidson": {"party": "Republican", "chamber": "House", "state": "OH"},
    "jim banks": {"party": "Republican", "chamber": "House", "state": "IN"},
    "mike gallagher": {"party": "Republican", "chamber": "House", "state": "WI"},
    "andrew clyde": {"party": "Republican", "chamber": "House", "state": "GA"},
    "barry loudermilk": {"party": "Republican", "chamber": "House", "state": "GA"},
    "ron estes": {"party": "Republican", "chamber": "House", "state": "KS"},
}

# ── Amount Range Parser ────────────────────────────────────────────────────────

_AMOUNT_PATTERNS: list[tuple[re.Pattern, int, int]] = [
    (re.compile(r"over.*50,000,000", re.I),       50_000_001, 100_000_000),
    (re.compile(r"25,000,001.*50,000,000", re.I), 25_000_001,  50_000_000),
    (re.compile(r"5,000,001.*25,000,000", re.I),   5_000_001,  25_000_000),
    (re.compile(r"1,000,001.*5,000,000", re.I),    1_000_001,   5_000_000),
    (re.compile(r"500,001.*1,000,000", re.I),        500_001,   1_000_000),
    (re.compile(r"250,001.*500,000", re.I),          250_001,     500_000),
    (re.compile(r"100,001.*250,000", re.I),          100_001,     250_000),
    (re.compile(r"50,001.*100,000", re.I),            50_001,     100_000),
    (re.compile(r"15,001.*50,000", re.I),             15_001,      50_000),
    (re.compile(r"1,001.*15,000", re.I),               1_001,      15_000),
    (re.compile(r"0.*1,000", re.I),                        0,       1_000),
]


def _parse_amount(raw: str | None) -> tuple[int | None, int | None, int | None]:
    if not raw:
        return None, None, None
    for pattern, lo, hi in _AMOUNT_PATTERNS:
        if pattern.search(raw):
            return lo, hi, (lo + hi) // 2
    # Try to extract numbers directly
    nums = re.findall(r"[\d,]+", raw.replace("$", ""))
    cleaned = [int(n.replace(",", "")) for n in nums if n.replace(",", "").isdigit()]
    if len(cleaned) >= 2:
        lo, hi = cleaned[0], cleaned[1]
        return lo, hi, (lo + hi) // 2
    if len(cleaned) == 1:
        v = cleaned[0]
        return v, v, v
    return None, None, None


# ── DB Connection Pool ─────────────────────────────────────────────────────────

def _get_conn():
    global _pool
    if not _DB_URL:
        return None
    try:
        if _pool is None:
            _pool = _pg_pool.SimpleConnectionPool(1, 5, _DB_URL)
        return _pool.getconn()
    except Exception as e:
        logger.error("[CONG_TRADE] DB connection error: %s", e)
        return None


def _put_conn(conn):
    global _pool
    if _pool and conn:
        try:
            _pool.putconn(conn)
        except Exception:
            pass


# ── DB Schema ─────────────────────────────────────────────────────────────────

def _create_table() -> None:
    conn = _get_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS congressional_trades (
                id                    SERIAL PRIMARY KEY,
                politician_name       VARCHAR(255),
                politician_party      VARCHAR(20),
                politician_chamber    VARCHAR(10),
                politician_state      VARCHAR(5),
                ticker                VARCHAR(10),
                asset_description     VARCHAR(500),
                asset_type            VARCHAR(50),
                transaction_type      VARCHAR(20),
                transaction_date      DATE,
                disclosure_date       DATE,
                amount_range          VARCHAR(50),
                amount_low            INTEGER,
                amount_high           INTEGER,
                amount_midpoint       INTEGER,
                owner                 VARCHAR(50),
                comment               TEXT,
                filing_url            TEXT,
                price_at_trade        DECIMAL(12,4),
                price_current         DECIMAL(12,4),
                return_since_trade_pct DECIMAL(8,4),
                days_to_disclose      INTEGER,
                is_late_filing        BOOLEAN,
                source                VARCHAR(20),
                fmp_unique_id         VARCHAR(100) UNIQUE,
                created_at            TIMESTAMP DEFAULT NOW(),
                expires_at            TIMESTAMP
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_cong_ticker ON congressional_trades(ticker)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_cong_politician ON congressional_trades(politician_name)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_cong_tx_date ON congressional_trades(transaction_date DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_cong_disc_date ON congressional_trades(disclosure_date DESC)")
        conn.commit()
        cur.close()
        logger.info("[CONG_TRADE] Table created/verified")
    except Exception as e:
        logger.error("[CONG_TRADE] Table creation error: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        _put_conn(conn)


def _cleanup_expired() -> None:
    conn = _get_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM congressional_trades WHERE expires_at < NOW()")
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        if deleted:
            logger.info("[CONG_TRADE] Cleaned up %d expired rows", deleted)
    except Exception as e:
        logger.error("[CONG_TRADE] Cleanup error: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        _put_conn(conn)


# ── Price Enrichment ───────────────────────────────────────────────────────────

async def _get_current_prices_batch(tickers: list[str]) -> dict[str, float]:
    """Get current prices via Tradier batch → Finnhub → yfinance fallback."""
    if not tickers:
        return {}
    results: dict[str, float] = {}
    remaining = list(set(tickers))

    if _TRADIER_KEY and remaining:
        try:
            symbols_str = ",".join(remaining[:50])
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(
                    "https://sandbox.tradier.com/v1/markets/quotes",
                    params={"symbols": symbols_str},
                    headers={"Authorization": f"Bearer {_TRADIER_KEY}", "Accept": "application/json"},
                )
            if resp.status_code == 200:
                quotes = resp.json().get("quotes", {}).get("quote", [])
                if isinstance(quotes, dict):
                    quotes = [quotes]
                for q in quotes:
                    sym = q.get("symbol", "")
                    price = q.get("last") or q.get("prevclose")
                    if sym and price:
                        results[sym] = float(price)
                        if sym in remaining:
                            remaining.remove(sym)
        except Exception as e:
            logger.warning("[CONG_PRICE] Tradier batch failed: %s", e)

    if remaining:
        def _yf_prices(syms):
            out = {}
            try:
                import yfinance as yf
                for s in syms:
                    try:
                        info = yf.Ticker(s).fast_info
                        p = getattr(info, "last_price", None) or getattr(info, "regular_market_price", None)
                        if p:
                            out[s] = float(p)
                    except Exception:
                        pass
            except Exception:
                pass
            return out

        loop = asyncio.get_event_loop()
        yf_res = await loop.run_in_executor(_executor, _yf_prices, remaining)
        results.update(yf_res)

    return results


def _get_historical_price(ticker: str, tx_date: str) -> float | None:
    """Get stock price on transaction_date via yfinance."""
    try:
        import yfinance as yf
        from datetime import timedelta
        start = tx_date
        end_dt = datetime.strptime(tx_date, "%Y-%m-%d") + timedelta(days=5)
        end = end_dt.strftime("%Y-%m-%d")
        hist = yf.Ticker(ticker).history(start=start, end=end, interval="1d")
        if not hist.empty:
            return float(hist["Close"].iloc[0])
    except Exception:
        pass
    return None


# ── FMP Fetch ─────────────────────────────────────────────────────────────────

def _politician_info(name: str, district: str, source: str) -> dict:
    """Look up party/chamber/state for a politician by name."""
    key = name.lower().strip()
    if key in _POLITICIAN_INFO:
        return _POLITICIAN_INFO[key]

    # Derive chamber from source
    chamber = "Senate" if "senate" in source else "House"

    # Derive state from district
    state = ""
    if district:
        # Senate district is typically just state abbreviation ("PA", "TX")
        # House district is state + number ("FL23", "CA15")
        m = re.match(r"([A-Z]{2})", district.upper())
        if m:
            state = m.group(1)

    return {"party": "Unknown", "chamber": chamber, "state": state}


def _build_unique_id(name: str, ticker: str, tx_date: str, tx_type: str) -> str:
    raw = f"{name}_{ticker}_{tx_date}_{tx_type}"
    return re.sub(r"[^a-z0-9_]", "_", raw.lower())[:100]


async def _fetch_from_fmp(endpoint: str, source_label: str) -> list[dict]:
    """Fetch records from one FMP endpoint with retry on 402/429."""
    if not _FMP_KEY:
        logger.warning("[CONG_TRADE] FMP_API_KEY not set")
        return []
    retries = 3
    delays = [45, 90, 180]
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.get(
                    f"{_FMP_BASE}/{endpoint}",
                    params={"apikey": _FMP_KEY, "limit": _REFRESH_LIMIT},
                )
            if resp.status_code in (402, 429):
                wait = delays[attempt]
                logger.warning("[CONG_TRADE] FMP %s returned %d (rate limited), retry %d/%d in %ds",
                               endpoint, resp.status_code, attempt + 1, retries, wait)
                await asyncio.sleep(wait)
                continue
            if resp.status_code != 200:
                logger.warning("[CONG_TRADE] FMP %s returned %d", endpoint, resp.status_code)
                return []
            data = resp.json()
            if not isinstance(data, list):
                logger.warning("[CONG_TRADE] FMP %s unexpected response type: %s", endpoint, type(data))
                return []
            logger.info("[CONG_TRADE] FMP %s: %d records (attempt %d)", endpoint, len(data), attempt + 1)
            return data
        except Exception as e:
            logger.error("[CONG_TRADE] FMP fetch error (%s) attempt %d: %s", endpoint, attempt + 1, e)
            if attempt < retries - 1:
                await asyncio.sleep(delays[attempt])
    logger.warning("[CONG_TRADE] FMP %s could not fetch (rate limited) — will retry at next scheduled interval", endpoint)
    return []


async def fetch_congressional_trades() -> dict:
    """Main fetch function: pulls from all FMP endpoints, enriches with price data, stores in DB."""
    global _last_refresh, _refresh_in_progress
    if _refresh_in_progress:
        return {"status": "already_running"}

    _refresh_in_progress = True
    inserted = 0
    skipped = 0
    errors = 0

    try:
        # Fetch from both working endpoints
        senate_raw = await _fetch_from_fmp("senate-latest", "senate")
        house_raw = await _fetch_from_fmp("house-latest", "house")

        all_raw = [(r, "senate_latest") for r in senate_raw] + \
                  [(r, "house_latest") for r in house_raw]

        if not all_raw:
            logger.warning("[CONG_TRADE] No records from FMP")
            return {"status": "ok", "inserted": 0, "skipped": 0}

        # Normalize records
        normalized: list[dict] = []
        for raw, source in all_raw:
            try:
                first = raw.get("firstName", "").strip()
                last = raw.get("lastName", "").strip()
                name = f"{first} {last}".strip()
                if not name:
                    continue

                ticker = (raw.get("symbol") or "").strip().upper()
                tx_date = raw.get("transactionDate") or ""
                disc_date = raw.get("disclosureDate") or ""
                tx_type = (raw.get("type") or "").strip()
                amount_str = raw.get("amount") or ""
                district = raw.get("district") or ""

                fmp_id = _build_unique_id(name, ticker, tx_date, tx_type)

                lo, hi, mid = _parse_amount(amount_str)

                # Days to disclose
                days_disc = None
                is_late = None
                if tx_date and disc_date:
                    try:
                        d1 = datetime.strptime(tx_date, "%Y-%m-%d").date()
                        d2 = datetime.strptime(disc_date, "%Y-%m-%d").date()
                        days_disc = (d2 - d1).days
                        is_late = days_disc > 45
                    except ValueError:
                        pass

                pinfo = _politician_info(name, district, source)

                normalized.append({
                    "politician_name": name,
                    "politician_party": pinfo["party"],
                    "politician_chamber": pinfo["chamber"],
                    "politician_state": pinfo.get("state", ""),
                    "ticker": ticker,
                    "asset_description": (raw.get("assetDescription") or "")[:500],
                    "asset_type": (raw.get("assetType") or "")[:50],
                    "transaction_type": tx_type[:20],
                    "transaction_date": tx_date or None,
                    "disclosure_date": disc_date or None,
                    "amount_range": amount_str[:50],
                    "amount_low": lo,
                    "amount_high": hi,
                    "amount_midpoint": mid,
                    "owner": (raw.get("owner") or "")[:50],
                    "comment": raw.get("comment") or "",
                    "filing_url": raw.get("link") or "",
                    "days_to_disclose": days_disc,
                    "is_late_filing": is_late,
                    "source": source,
                    "fmp_unique_id": fmp_id,
                    "_tx_date_raw": tx_date,
                    "price_at_trade": None,
                    "price_current": None,
                    "return_since_trade_pct": None,
                })
            except Exception as e:
                logger.debug("[CONG_TRADE] Normalize error: %s", e)
                errors += 1

        # Enrich: current prices via Tradier batch
        stock_tickers = list({r["ticker"] for r in normalized if r["ticker"]})
        current_prices = await _get_current_prices_batch(stock_tickers)
        logger.info("[CONG_TRADE] Got current prices for %d/%d tickers",
                    len(current_prices), len(stock_tickers))

        # Historical price + return calculation via yfinance (sync, in executor)
        loop = asyncio.get_event_loop()

        def _enrich_historical(records: list[dict]) -> list[dict]:
            try:
                import yfinance as yf
            except ImportError:
                return records
            for rec in records:
                sym = rec.get("ticker", "")
                tx_d = rec.get("_tx_date_raw", "")
                cur_p = current_prices.get(sym)
                rec["price_current"] = cur_p
                if sym and tx_d and cur_p:
                    try:
                        from datetime import timedelta
                        end_dt = datetime.strptime(tx_d, "%Y-%m-%d") + timedelta(days=5)
                        hist = yf.Ticker(sym).history(
                            start=tx_d,
                            end=end_dt.strftime("%Y-%m-%d"),
                            interval="1d",
                        )
                        if not hist.empty:
                            p_at = float(hist["Close"].iloc[0])
                            rec["price_at_trade"] = p_at
                            if cur_p and p_at:
                                rec["return_since_trade_pct"] = round(
                                    (cur_p - p_at) / p_at * 100, 4
                                )
                    except Exception:
                        pass
            return records

        normalized = await loop.run_in_executor(_executor, _enrich_historical, normalized)

        # Insert into DB (skip duplicates via fmp_unique_id UNIQUE constraint)
        conn = _get_conn()
        if not conn:
            return {"status": "error", "error": "no db connection"}

        expires_at = datetime.utcnow() + timedelta(days=_RETENTION_DAYS)

        try:
            cur = conn.cursor()
            for rec in normalized:
                try:
                    cur.execute("""
                        INSERT INTO congressional_trades (
                            politician_name, politician_party, politician_chamber,
                            politician_state, ticker, asset_description, asset_type,
                            transaction_type, transaction_date, disclosure_date,
                            amount_range, amount_low, amount_high, amount_midpoint,
                            owner, comment, filing_url, price_at_trade, price_current,
                            return_since_trade_pct, days_to_disclose, is_late_filing,
                            source, fmp_unique_id, expires_at
                        ) VALUES (
                            %(politician_name)s, %(politician_party)s, %(politician_chamber)s,
                            %(politician_state)s, %(ticker)s, %(asset_description)s, %(asset_type)s,
                            %(transaction_type)s, %(transaction_date)s, %(disclosure_date)s,
                            %(amount_range)s, %(amount_low)s, %(amount_high)s, %(amount_midpoint)s,
                            %(owner)s, %(comment)s, %(filing_url)s, %(price_at_trade)s, %(price_current)s,
                            %(return_since_trade_pct)s, %(days_to_disclose)s, %(is_late_filing)s,
                            %(source)s, %(fmp_unique_id)s, %(expires_at)s
                        )
                        ON CONFLICT (fmp_unique_id) DO UPDATE SET
                            price_current = EXCLUDED.price_current,
                            return_since_trade_pct = EXCLUDED.return_since_trade_pct,
                            expires_at = EXCLUDED.expires_at
                    """, {**rec, "expires_at": expires_at})
                    inserted += 1
                except Exception as e:
                    logger.debug("[CONG_TRADE] Insert error (%s): %s", rec.get("fmp_unique_id"), e)
                    skipped += 1
            conn.commit()
            cur.close()
        except Exception as e:
            logger.error("[CONG_TRADE] Batch insert error: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            _put_conn(conn)

        _last_refresh = datetime.utcnow()
        logger.info("[CONG_TRADE] Fetch complete: %d inserted/updated, %d skipped, %d errors",
                    inserted, skipped, errors)
        return {"status": "ok", "inserted": inserted, "skipped": skipped, "errors": errors}

    finally:
        _refresh_in_progress = False


# ── API Helpers ────────────────────────────────────────────────────────────────

_COLS = [
    "id", "politician_name", "politician_party", "politician_chamber", "politician_state",
    "ticker", "asset_description", "asset_type", "transaction_type", "transaction_date",
    "disclosure_date", "amount_range", "amount_low", "amount_high", "amount_midpoint",
    "owner", "comment", "filing_url", "price_at_trade", "price_current",
    "return_since_trade_pct", "days_to_disclose", "is_late_filing", "source", "fmp_unique_id",
    "created_at",
]


def _row_to_dict(row, cols: list[str]) -> dict:
    d = dict(zip(cols, row))
    for k in ("price_at_trade", "price_current", "return_since_trade_pct"):
        if isinstance(d.get(k), Decimal):
            d[k] = float(d[k])
    for k in ("transaction_date", "disclosure_date"):
        if d.get(k) and hasattr(d[k], "isoformat"):
            d[k] = d[k].isoformat()
    if d.get("created_at") and hasattr(d["created_at"], "isoformat"):
        d["created_at"] = d["created_at"].isoformat()
    return d


def _query_trades(
    chamber: str = "all",
    party: str = "all",
    tx_type: str = "all",
    ticker_filter: str | None = None,
    politician_filter: str | None = None,
    timeframe: str = "3m",
    min_amount: int | None = None,
    sort: str = "date",
    order: str = "desc",
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], dict]:
    conn = _get_conn()
    if not conn:
        return [], {"total": 0, "total_purchases": 0, "total_sales": 0}

    days_map = {"1w": 7, "1m": 30, "3m": 90, "6m": 180}
    days_back = days_map.get(timeframe, 90)

    sort_col = {"date": "transaction_date", "amount": "amount_midpoint",
                "politician": "politician_name", "ticker": "ticker",
                "return": "return_since_trade_pct"}.get(sort, "transaction_date")
    order_dir = "DESC" if order.lower() == "desc" else "ASC"

    conditions = [
        f"transaction_date >= NOW() - INTERVAL '{days_back} days'",
        "ticker IS NOT NULL", "ticker != ''",
    ]
    params: list = []

    if chamber != "all":
        conditions.append("LOWER(politician_chamber) = LOWER(%s)")
        params.append(chamber)
    if party != "all":
        conditions.append("LOWER(politician_party) = LOWER(%s)")
        params.append(party)
    if tx_type != "all":
        conditions.append("LOWER(transaction_type) ILIKE %s")
        params.append(f"%{tx_type}%")
    if ticker_filter:
        conditions.append("ticker ILIKE %s")
        params.append(ticker_filter.upper())
    if politician_filter:
        conditions.append("politician_name ILIKE %s")
        params.append(f"%{politician_filter}%")
    if min_amount is not None:
        conditions.append("amount_midpoint >= %s")
        params.append(min_amount)

    where = " AND ".join(conditions)
    cols_str = ", ".join(_COLS)
    results = []
    summary = {
        "total": 0, "total_purchases": 0, "total_sales": 0,
        "late_filings_count": 0, "avg_days_to_disclose": 0,
        "last_refresh": _last_refresh.isoformat() if _last_refresh else None,
    }

    try:
        cur = conn.cursor()

        # Main query
        cur.execute(
            f"SELECT {cols_str} FROM congressional_trades WHERE {where} "
            f"ORDER BY {sort_col} {order_dir} NULLS LAST LIMIT %s OFFSET %s",
            params + [limit, offset]
        )
        results = [_row_to_dict(r, _COLS) for r in cur.fetchall()]

        # Counts
        cur.execute(f"SELECT COUNT(*) FROM congressional_trades WHERE {where}", params)
        summary["total"] = cur.fetchone()[0]

        # Stats (all records, not filtered)
        cur.execute("""
            SELECT
                SUM(CASE WHEN LOWER(transaction_type) LIKE '%purchase%' OR LOWER(transaction_type) LIKE '%buy%' THEN 1 ELSE 0 END),
                SUM(CASE WHEN LOWER(transaction_type) LIKE '%sale%' OR LOWER(transaction_type) LIKE '%sell%' THEN 1 ELSE 0 END),
                SUM(CASE WHEN is_late_filing = TRUE THEN 1 ELSE 0 END),
                AVG(days_to_disclose)
            FROM congressional_trades
            WHERE transaction_date >= NOW() - INTERVAL '90 days'
        """)
        st = cur.fetchone()
        if st:
            summary["total_purchases"] = int(st[0] or 0)
            summary["total_sales"] = int(st[1] or 0)
            summary["late_filings_count"] = int(st[2] or 0)
            summary["avg_days_to_disclose"] = round(float(st[3] or 0), 1)

        # Most active politician (last 90 days)
        cur.execute("""
            SELECT politician_name, politician_party, politician_chamber, COUNT(*) AS cnt
            FROM congressional_trades
            WHERE transaction_date >= NOW() - INTERVAL '90 days'
            GROUP BY politician_name, politician_party, politician_chamber
            ORDER BY cnt DESC LIMIT 1
        """)
        row = cur.fetchone()
        summary["most_active_politician"] = {
            "name": row[0], "party": row[1], "chamber": row[2], "trade_count": row[3]
        } if row else None

        # Most traded ticker
        cur.execute("""
            SELECT ticker, COUNT(*) AS cnt
            FROM congressional_trades
            WHERE transaction_date >= NOW() - INTERVAL '90 days'
              AND ticker IS NOT NULL AND ticker != ''
            GROUP BY ticker ORDER BY cnt DESC LIMIT 1
        """)
        row2 = cur.fetchone()
        summary["most_traded_ticker"] = {"ticker": row2[0], "trade_count": row2[1]} if row2 else None

        # Volume estimate
        cur.execute("""
            SELECT SUM(amount_low), SUM(amount_high)
            FROM congressional_trades
            WHERE transaction_date >= NOW() - INTERVAL '90 days'
        """)
        vol = cur.fetchone()
        if vol and vol[0]:
            def _fmt(v):
                if v >= 1_000_000_000:
                    return f"${v/1_000_000_000:.1f}B"
                if v >= 1_000_000:
                    return f"${v/1_000_000:.0f}M"
                return f"${v:,}"
            summary["total_volume_estimate"] = f"{_fmt(vol[0])} - {_fmt(vol[1])}"
        else:
            summary["total_volume_estimate"] = "N/A"

        cur.close()
    except Exception as e:
        logger.error("[CONG_TRADE] Query error: %s", e)
    finally:
        _put_conn(conn)

    return results, summary


# ── Background Loop ────────────────────────────────────────────────────────────

async def congressional_trading_background_loop():
    """Fetches congressional trades every 4 hours."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_executor, _create_table)
    await loop.run_in_executor(_executor, _cleanup_expired)

    # Delay initial fetch to avoid FMP rate-limit collision with other services at startup
    logger.info("[CONG_TRADE] Waiting 120s before initial fetch to avoid startup rate limits")
    await asyncio.sleep(120)

    # Initial fetch on startup
    try:
        await fetch_congressional_trades()
    except Exception as e:
        logger.error("[CONG_TRADE] Initial fetch error: %s", e)

    while True:
        # If last refresh failed (no data yet), retry sooner (1 hour), else full interval
        wait = 3600 if _last_refresh is None else _FETCH_INTERVAL
        logger.info("[CONG_TRADE] Next fetch in %ds (last_refresh=%s)", wait, _last_refresh)
        await asyncio.sleep(wait)
        try:
            await loop.run_in_executor(_executor, _cleanup_expired)
            await fetch_congressional_trades()
        except Exception as e:
            logger.error("[CONG_TRADE] Background loop error: %s", e)


# ── REST API ──────────────────────────────────────────────────────────────────

router = APIRouter(tags=["congressional-trades"])


@router.get("/congressional-trades")
async def get_congressional_trades(
    chamber: str = Query("all", description="all|senate|house"),
    party: str = Query("all", description="all|democrat|republican|independent"),
    type: str = Query("all", description="all|purchase|sale|exchange"),
    ticker: Optional[str] = Query(None),
    politician: Optional[str] = Query(None),
    timeframe: str = Query("3m", description="1w|1m|3m|6m"),
    min_amount: Optional[int] = Query(None),
    sort: str = Query("date", description="date|amount|politician|ticker|return"),
    order: str = Query("desc"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    loop = asyncio.get_event_loop()
    trades, summary = await loop.run_in_executor(
        _executor,
        lambda: _query_trades(chamber, party, type, ticker, politician,
                               timeframe, min_amount, sort, order, limit, offset),
    )
    return {
        "summary": summary,
        "trades": trades,
        "pagination": {
            "total": summary["total"],
            "limit": limit,
            "offset": offset,
            "has_more": (offset + limit) < summary["total"],
        },
    }


@router.get("/congressional-trades/stats")
async def get_congressional_stats():
    loop = asyncio.get_event_loop()

    def _stats():
        conn = _get_conn()
        if not conn:
            return {}
        try:
            cur = conn.cursor()

            cur.execute("""
                SELECT
                    COUNT(*),
                    SUM(CASE WHEN LOWER(transaction_type) LIKE '%purchase%' OR LOWER(transaction_type) LIKE '%buy%' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN LOWER(transaction_type) LIKE '%sale%' OR LOWER(transaction_type) LIKE '%sell%' THEN 1 ELSE 0 END),
                    SUM(amount_low), SUM(amount_high),
                    AVG(days_to_disclose),
                    SUM(CASE WHEN is_late_filing = TRUE THEN 1 ELSE 0 END)
                FROM congressional_trades
                WHERE transaction_date >= NOW() - INTERVAL '90 days'
            """)
            row = cur.fetchone() or (0,) * 7
            total, purchases, sales, vol_lo, vol_hi, avg_disc, late = row

            # Most active politicians
            cur.execute("""
                SELECT politician_name, politician_party, politician_chamber, COUNT(*) AS cnt
                FROM congressional_trades
                WHERE transaction_date >= NOW() - INTERVAL '90 days'
                GROUP BY politician_name, politician_party, politician_chamber
                ORDER BY cnt DESC LIMIT 10
            """)
            most_active = [
                {"name": r[0], "party": (r[1] or "Unknown")[:1],
                 "chamber": r[2], "count": r[3]}
                for r in cur.fetchall()
            ]

            # Most traded tickers
            cur.execute("""
                SELECT ticker,
                    COUNT(*) AS cnt,
                    SUM(CASE WHEN LOWER(transaction_type) LIKE '%purchase%' THEN 1 ELSE 0 END) AS buys,
                    SUM(CASE WHEN LOWER(transaction_type) LIKE '%sale%' THEN 1 ELSE 0 END) AS sells
                FROM congressional_trades
                WHERE transaction_date >= NOW() - INTERVAL '90 days'
                  AND ticker IS NOT NULL AND ticker != ''
                GROUP BY ticker ORDER BY cnt DESC LIMIT 10
            """)
            most_traded = [
                {"ticker": r[0], "count": r[1],
                 "net_direction": "buy" if (r[2] or 0) >= (r[3] or 0) else "sell"}
                for r in cur.fetchall()
            ]

            # Party breakdown
            cur.execute("""
                SELECT LOWER(politician_party),
                    SUM(CASE WHEN LOWER(transaction_type) LIKE '%purchase%' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN LOWER(transaction_type) LIKE '%sale%' THEN 1 ELSE 0 END)
                FROM congressional_trades
                WHERE transaction_date >= NOW() - INTERVAL '90 days'
                GROUP BY LOWER(politician_party)
            """)
            party_breakdown = {}
            for r in cur.fetchall():
                party_breakdown[r[0] or "unknown"] = {"purchases": int(r[1] or 0), "sales": int(r[2] or 0)}

            # Chamber breakdown
            cur.execute("""
                SELECT LOWER(politician_chamber), COUNT(*)
                FROM congressional_trades
                WHERE transaction_date >= NOW() - INTERVAL '90 days'
                GROUP BY LOWER(politician_chamber)
            """)
            chamber_breakdown = {r[0]: {"total": r[1]} for r in cur.fetchall()}

            cur.close()
            return {
                "total_trades": int(total or 0),
                "purchases": int(purchases or 0),
                "sales": int(sales or 0),
                "volume_estimate_low": int(vol_lo or 0),
                "volume_estimate_high": int(vol_hi or 0),
                "most_active": most_active,
                "most_traded_tickers": most_traded,
                "party_breakdown": party_breakdown,
                "chamber_breakdown": chamber_breakdown,
                "filing_compliance": {
                    "on_time": int(total or 0) - int(late or 0),
                    "late": int(late or 0),
                    "avg_days_to_disclose": round(float(avg_disc or 0), 1),
                },
                "last_refresh": _last_refresh.isoformat() if _last_refresh else None,
            }
        except Exception as e:
            logger.error("[CONG_TRADE] Stats error: %s", e)
            return {}
        finally:
            _put_conn(conn)

    return await loop.run_in_executor(_executor, _stats)


@router.get("/congressional-trades/politician/{name}")
async def get_politician_trades(name: str):
    loop = asyncio.get_event_loop()

    def _fetch():
        conn = _get_conn()
        if not conn:
            return None
        try:
            cur = conn.cursor()
            cols_str = ", ".join(_COLS)
            cur.execute(
                f"SELECT {cols_str} FROM congressional_trades "
                "WHERE politician_name ILIKE %s ORDER BY transaction_date DESC",
                (f"%{name}%",)
            )
            trades = [_row_to_dict(r, _COLS) for r in cur.fetchall()]
            if not trades:
                return None

            pname = trades[0]["politician_name"]
            total = len(trades)
            purchases = sum(1 for t in trades if "purchase" in (t.get("transaction_type") or "").lower())
            sales = sum(1 for t in trades if "sale" in (t.get("transaction_type") or "").lower())
            returns = [t["return_since_trade_pct"] for t in trades if t.get("return_since_trade_pct") is not None]
            avg_return = round(sum(returns) / len(returns), 2) if returns else None
            disclose_days = [t["days_to_disclose"] for t in trades if t.get("days_to_disclose") is not None]
            avg_disc = round(sum(disclose_days) / len(disclose_days), 1) if disclose_days else None
            late = sum(1 for t in trades if t.get("is_late_filing"))
            lo_vals = [t["amount_low"] for t in trades if t.get("amount_low")]
            hi_vals = [t["amount_high"] for t in trades if t.get("amount_high")]

            def _fmt(v):
                if v >= 1_000_000:
                    return f"${v/1_000_000:.1f}M"
                return f"${v:,}"

            vol_range = f"{_fmt(sum(lo_vals))} - {_fmt(sum(hi_vals))}" if lo_vals else "N/A"

            cur.close()
            return {
                "politician": {
                    "name": pname,
                    "party": trades[0].get("politician_party"),
                    "chamber": trades[0].get("politician_chamber"),
                    "state": trades[0].get("politician_state"),
                    "total_trades": total,
                    "total_purchases": purchases,
                    "total_sales": sales,
                    "estimated_volume_range": vol_range,
                    "avg_return_pct": avg_return,
                    "avg_days_to_disclose": avg_disc,
                    "late_filings": late,
                },
                "trades": trades,
            }
        except Exception as e:
            logger.error("[CONG_TRADE] Politician query error: %s", e)
            return None
        finally:
            _put_conn(conn)

    result = await loop.run_in_executor(_executor, _fetch)
    if result is None:
        raise HTTPException(status_code=404, detail=f"No trades found for politician: {name}")
    return result


@router.post("/congressional-trades/refresh")
async def trigger_refresh(background_tasks: BackgroundTasks):
    if _refresh_in_progress:
        return {"status": "already_running", "message": "A refresh is already in progress"}
    background_tasks.add_task(fetch_congressional_trades)
    return {"status": "refresh_started", "message": "Fetching congressional trades in background (~30-60s)"}
