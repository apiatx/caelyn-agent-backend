"""
Whale Watch Service — tracks top institutional investors via SEC 13F filings.

Fetches quarterly 13F-HR holdings from SEC EDGAR, calculates weighted portfolio
returns (1m/3m/6m/1y), and generates AI investment-theme summaries via Anthropic.

DB tables: whales, whale_holdings, whale_portfolio_returns
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from typing import Any, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import httpx
import psycopg2
from psycopg2 import pool as _pg_pool
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query

logger = logging.getLogger("whale_watch")

# ── Constants ────────────────────────────────────────────────────────────────

_ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
_TRADIER_KEY   = os.getenv("TRADIER_API_KEY", "")
_SEC_HEADERS   = {
    "User-Agent": "CaelynAI contact@caelyn.ai",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json",
}
_SEC_BASE      = "https://data.sec.gov"
_EDGAR_ARCHIVE = "https://www.sec.gov/Archives/edgar/data"
_SEC_DELAY     = 0.3    # seconds between SEC requests
_REFRESH_INTERVAL = 86400  # 24 hours

_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="whale")

# ── Default whale roster (CIKs verified against SEC EDGAR) ───────────────────

DEFAULT_WHALES = [
    {
        "name": "Berkshire Hathaway",
        "cik": "1067983",
        "category": "institution",
        "description": "Warren Buffett's conglomerate, the world's largest value-investing portfolio.",
    },
    {
        "name": "Pershing Square Capital Management",
        "cik": "1336528",
        "category": "institution",
        "description": "Bill Ackman's concentrated activist hedge fund known for high-conviction bets.",
    },
    {
        "name": "Duquesne Family Office",
        "cik": "1536411",
        "category": "institution",
        "description": "Stanley Druckenmiller's family office, a macro-driven equity and global-asset fund.",
    },
    {
        "name": "Elliott Investment Management",
        "cik": "1791786",
        "category": "institution",
        "description": "Paul Singer's activist multi-strategy fund with a focus on event-driven opportunities.",
    },
    {
        "name": "Appaloosa Management",
        "cik": "1006438",
        "category": "institution",
        "description": "David Tepper's fund specialising in distressed debt, equities, and macro plays.",
    },
    {
        "name": "Baupost Group",
        "cik": "1061768",
        "category": "institution",
        "description": "Seth Klarman's value-oriented hedge fund with a deep margin-of-safety discipline.",
    },
    {
        "name": "Third Point",
        "cik": "1040273",
        "category": "institution",
        "description": "Dan Loeb's activist and event-driven fund covering equities and credit.",
    },
    {
        "name": "Soros Fund Management",
        "cik": "1029160",
        "category": "institution",
        "description": "George Soros's family office, famed for macro currency and equity strategies.",
    },
    {
        "name": "Renaissance Technologies",
        "cik": "1037389",
        "category": "institution",
        "description": "Jim Simons's quant fund; the Medallion Fund is the most successful in history.",
    },
    {
        "name": "Bridgewater Associates",
        "cik": "1350694",
        "category": "institution",
        "description": "Ray Dalio's macro fund and the world's largest hedge fund by AUM.",
    },
]

# ── DB helpers ────────────────────────────────────────────────────────────────

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
                logger.error("[WHALE_DB] Pool creation failed: %s", e)
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


# ── DB Schema ─────────────────────────────────────────────────────────────────

def _create_tables() -> None:
    conn = _get_conn()
    if not conn:
        logger.warning("[WHALE_DB] No DB connection — skipping table creation")
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS whales (
                id           SERIAL PRIMARY KEY,
                name         TEXT UNIQUE NOT NULL,
                category     TEXT NOT NULL DEFAULT 'institution',
                cik          TEXT,
                description  TEXT,
                ai_theme     TEXT,
                return_1m    FLOAT,
                return_3m    FLOAT,
                return_6m    FLOAT,
                return_1y    FLOAT,
                return_3y    FLOAT,
                last_updated TIMESTAMP DEFAULT NOW(),
                created_at   TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS whale_holdings (
                id           SERIAL PRIMARY KEY,
                whale_name   TEXT NOT NULL,
                ticker       TEXT NOT NULL,
                company_name TEXT,
                shares       BIGINT,
                value_usd    BIGINT,
                weight_pct   FLOAT,
                quarter      TEXT,
                filed_date   TIMESTAMP,
                created_at   TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_wh_whale_quarter
            ON whale_holdings (whale_name, quarter)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_wh_ticker
            ON whale_holdings (ticker)
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS whale_portfolio_returns (
                id                    SERIAL PRIMARY KEY,
                whale_name            TEXT NOT NULL,
                quarter               TEXT NOT NULL,
                portfolio_value_usd   BIGINT,
                calculated_return_pct FLOAT,
                benchmark_spy_return_pct FLOAT,
                created_at            TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_wpr_whale
            ON whale_portfolio_returns (whale_name, quarter)
        """)
        conn.commit()
        cur.close()
        logger.info("[WHALE_DB] Tables ready")
    except Exception as e:
        logger.error("[WHALE_DB] Table creation error: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        _put_conn(conn)


# ── Seed whales ───────────────────────────────────────────────────────────────

def seed_whales() -> None:
    conn = _get_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        for w in DEFAULT_WHALES:
            cur.execute("""
                INSERT INTO whales (name, category, cik, description)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (name) DO UPDATE SET
                    cik         = EXCLUDED.cik,
                    description = EXCLUDED.description,
                    category    = EXCLUDED.category
            """, (w["name"], w["category"], w["cik"], w["description"]))
        conn.commit()
        cur.close()
        logger.info("[WHALE] Seeded %d whales", len(DEFAULT_WHALES))
    except Exception as e:
        logger.error("[WHALE] Seed error: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        _put_conn(conn)


# ── CUSIP → Ticker mapping via OpenFIGI ──────────────────────────────────────

async def _cusips_to_tickers(cusips: list[str]) -> dict[str, str]:
    """
    Map CUSIPs to tickers using the free OpenFIGI batch API.
    Processes in chunks of 25 (free-tier limit per request).
    """
    if not cusips:
        return {}
    mapping: dict[str, str] = {}
    chunk_size = 25
    for i in range(0, len(cusips), chunk_size):
        chunk = cusips[i:i + chunk_size]
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.post(
                    "https://api.openfigi.com/v3/mapping",
                    json=[{"idType": "ID_CUSIP", "idValue": c, "exchCode": "US"} for c in chunk],
                    headers={"Content-Type": "application/json"},
                )
            if resp.status_code == 200:
                results = resp.json()
                for cusip, item in zip(chunk, results):
                    data = item.get("data") or []
                    for d in data:
                        ticker = d.get("ticker")
                        security_type = d.get("securityType", "")
                        # Prefer common shares over options/warrants
                        if ticker and "Option" not in security_type and "Warrant" not in security_type:
                            mapping[cusip] = ticker
                            break
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.warning("[WHALE] OpenFIGI chunk error: %s", e)
    return mapping


# ── SEC EDGAR 13F fetcher ─────────────────────────────────────────────────────

async def fetch_13f_filings(cik: str, whale_name: str) -> list[dict]:
    """
    Fetch the most recent 13F-HR filing for a whale and save holdings to DB.
    Returns list of holding dicts: {ticker, company_name, shares, value_usd, weight_pct, quarter}
    """
    cik_padded = cik.zfill(10)
    logger.info("[WHALE] Fetching 13F for %s (CIK %s)", whale_name, cik_padded)

    # Step 1: Get submissions index from EDGAR
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            resp = await client.get(
                f"{_SEC_BASE}/submissions/CIK{cik_padded}.json",
                headers=_SEC_HEADERS,
            )
            resp.raise_for_status()
            submissions = resp.json()
    except Exception as e:
        logger.error("[WHALE] Submissions fetch error for %s: %s", whale_name, e)
        return []

    # Step 2: Find most recent 13F-HR accession
    recent = submissions.get("filings", {}).get("recent", {})
    forms       = recent.get("form", [])
    accessions  = recent.get("accessionNumber", [])
    filing_dates = recent.get("filingDate", [])
    report_dates = recent.get("reportDate", [])

    accession = None
    filed_date_str = None
    report_date_str = None
    for form, acc, filed, reported in zip(forms, accessions, filing_dates, report_dates):
        if form in ("13F-HR", "13F-HR/A"):
            accession = acc
            filed_date_str = filed
            report_date_str = reported
            break

    if not accession:
        logger.warning("[WHALE] No 13F-HR found for %s", whale_name)
        return []

    # Determine quarter label from report date (e.g. 2024-12-31 → 2024Q4)
    quarter = ""
    if report_date_str:
        try:
            rd = date.fromisoformat(report_date_str)
            q = (rd.month - 1) // 3 + 1
            quarter = f"{rd.year}Q{q}"
        except Exception:
            pass

    filed_date = None
    if filed_date_str:
        try:
            filed_date = datetime.fromisoformat(filed_date_str)
        except Exception:
            pass

    await asyncio.sleep(_SEC_DELAY)

    # Step 3: Fetch filing index HTML to find the infotable XML
    cik_int = str(int(cik))
    accession_nodash = accession.replace("-", "")
    index_url = f"{_EDGAR_ARCHIVE}/{cik_int}/{accession_nodash}/"

    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            resp = await client.get(index_url, headers=_SEC_HEADERS)
            resp.raise_for_status()
            index_html = resp.text
    except Exception as e:
        logger.error("[WHALE] Index fetch error for %s: %s", whale_name, e)
        return []

    # Find XML file links (exclude primary_doc.xml — that is the cover page)
    xml_links = re.findall(r'href="(/Archives/edgar/data/[^"]+\.xml)"', index_html, re.I)
    infotable_url = None
    for link in xml_links:
        if "primary_doc" not in link.lower():
            infotable_url = f"https://www.sec.gov{link}"
            break

    if not infotable_url:
        logger.warning("[WHALE] No infotable XML found for %s (%s)", whale_name, accession)
        return []

    await asyncio.sleep(_SEC_DELAY)

    # Step 4: Download and parse the infotable XML
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(infotable_url, headers=_SEC_HEADERS)
            resp.raise_for_status()
            xml_content = resp.text
    except Exception as e:
        logger.error("[WHALE] Infotable fetch error for %s: %s", whale_name, e)
        return []

    holdings_raw = _parse_infotable_xml(xml_content)
    if not holdings_raw:
        logger.warning("[WHALE] No holdings parsed for %s", whale_name)
        return []

    # Step 5: Map CUSIPs → tickers via OpenFIGI
    cusips = [h["cusip"] for h in holdings_raw if h.get("cusip")]
    cusip_map = await _cusips_to_tickers(list(set(cusips)))

    # Step 6: Calculate weight percentages
    total_value = sum(h["value_usd"] for h in holdings_raw)
    if total_value <= 0:
        total_value = 1

    final_holdings: list[dict] = []
    for h in holdings_raw:
        ticker = cusip_map.get(h.get("cusip", ""), "")
        # Fallback: try to derive ticker from company name (basic sanitize)
        if not ticker:
            ticker = _guess_ticker_from_name(h.get("company_name", ""))
        if not ticker:
            continue

        weight_pct = h["value_usd"] / total_value * 100
        final_holdings.append({
            "ticker":       ticker.upper(),
            "company_name": h.get("company_name", ""),
            "shares":       h.get("shares", 0),
            "value_usd":    h["value_usd"],
            "weight_pct":   round(weight_pct, 4),
            "quarter":      quarter,
            "filed_date":   filed_date,
        })

    # Sort by weight descending
    final_holdings.sort(key=lambda x: x["value_usd"], reverse=True)

    # Step 7: Delete old holdings for this whale+quarter and insert new
    _save_holdings_to_db(whale_name, quarter, final_holdings)

    logger.info("[WHALE] Saved %d holdings for %s (%s)", len(final_holdings), whale_name, quarter)
    return final_holdings


def _parse_infotable_xml(xml_text: str) -> list[dict]:
    """Parse 13F infotable XML; handles both old and new EDGAR namespace variants."""
    records = []
    try:
        # Strip namespace for simpler parsing
        clean = re.sub(r' xmlns[^=]*="[^"]*"', "", xml_text)
        clean = re.sub(r'</?[a-zA-Z]+:', "<", clean).replace("</", "</")
        # Some filings use camelCase tags
        root = ET.fromstring(clean)
    except Exception as e:
        logger.warning("[WHALE] XML parse error: %s", e)
        return []

    for entry in root.iter("infoTable"):
        company_name = _xml_text(entry, ["nameOfIssuer", "issuerName", "name"])
        cusip        = _xml_text(entry, ["cusip", "CUSIP"])
        value_str    = _xml_text(entry, ["value"])
        shares_str   = _xml_text(entry, ["sshPrnamt", "shares", "noShares"])

        try:
            # SEC 13F value field convention varies: most large filers report in full
            # US dollars (despite SEC instructions specifying thousands). We store
            # the raw value without multiplying — weight_pct ratios remain correct
            # regardless of which convention the filer uses.
            value_usd = int(float(value_str or "0"))
        except Exception:
            value_usd = 0

        try:
            shares = int(float(shares_str or "0"))
        except Exception:
            shares = 0

        if value_usd <= 0:
            continue

        records.append({
            "company_name": (company_name or "").strip(),
            "cusip":        (cusip or "").strip().replace("-", ""),
            "value_usd":    value_usd,
            "shares":       shares,
        })

    return records


def _xml_text(element, tag_names: list[str]) -> str:
    """Try multiple tag name variants and return text of first found child."""
    for tag in tag_names:
        child = element.find(tag)
        if child is not None and child.text:
            return child.text.strip()
        # Try case-insensitive iteration
        for child in element:
            if child.tag.lower() == tag.lower() and child.text:
                return child.text.strip()
    return ""


def _guess_ticker_from_name(name: str) -> str:
    """Very rough heuristic: strip common words and return first word as ticker guess."""
    stop = {"INC", "CORP", "LTD", "LLC", "CO", "PLC", "THE", "GROUP", "HOLDINGS",
            "INTERNATIONAL", "INTL", "GLOBAL", "TECHNOLOGIES", "TECHNOLOGY", "TECH",
            "INDUSTRIES", "INDUSTRY", "FINANCIAL", "FINANCE", "SERVICES", "SERVICE"}
    parts = re.sub(r"[^A-Za-z0-9 ]", " ", name).split()
    parts = [p.upper() for p in parts if p.upper() not in stop and len(p) >= 2]
    return parts[0] if parts else ""


def _save_holdings_to_db(whale_name: str, quarter: str, holdings: list[dict]) -> None:
    conn = _get_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        # Delete existing holdings for this whale + quarter
        cur.execute(
            "DELETE FROM whale_holdings WHERE whale_name = %s AND quarter = %s",
            (whale_name, quarter),
        )
        for h in holdings:
            cur.execute("""
                INSERT INTO whale_holdings
                    (whale_name, ticker, company_name, shares, value_usd, weight_pct, quarter, filed_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                whale_name,
                h["ticker"],
                h.get("company_name"),
                h.get("shares"),
                h.get("value_usd"),
                h.get("weight_pct"),
                quarter,
                h.get("filed_date"),
            ))
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error("[WHALE_DB] Holdings save error for %s: %s", whale_name, e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        _put_conn(conn)


# ── Price fetching helpers ────────────────────────────────────────────────────

async def _get_current_prices(tickers: list[str]) -> dict[str, float]:
    """Fetch current prices: Tradier batch → yfinance fallback."""
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
            logger.warning("[WHALE_PRICE] Tradier batch failed: %s", e)

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


def _get_historical_returns_sync(tickers: list[str]) -> dict[str, dict]:
    """
    For each ticker return a dict with keys: ret_1m, ret_3m, ret_6m, ret_1y
    Values are percentage returns (float). Uses yfinance download.
    """
    import yfinance as yf
    import pandas as pd

    result: dict[str, dict] = {}
    if not tickers:
        return result

    try:
        all_tickers = list(set(tickers + ["SPY"]))
        data = yf.download(
            all_tickers,
            period="2y",
            interval="1d",
            progress=False,
            auto_adjust=True,
        )

        # yfinance returns multi-level columns when >1 ticker
        if isinstance(data.columns, pd.MultiIndex):
            close = data["Close"] if "Close" in data.columns.get_level_values(0) else data.xs("Close", axis=1, level=0)
        else:
            close = data[["Close"]] if "Close" in data.columns else data

        today = pd.Timestamp.now(tz="UTC").normalize()

        def _period_return(series: pd.Series, days: int) -> float | None:
            target = today - pd.Timedelta(days=days)
            past = series[series.index <= target]
            if past.empty or pd.isna(past.iloc[-1]):
                return None
            curr = series.dropna()
            if curr.empty:
                return None
            current_price = float(curr.iloc[-1])
            past_price    = float(past.iloc[-1])
            if past_price == 0:
                return None
            return round((current_price / past_price - 1) * 100, 2)

        for ticker in all_tickers:
            try:
                if ticker in close.columns:
                    series = close[ticker].dropna()
                elif len(close.columns) == 1:
                    series = close.iloc[:, 0].dropna()
                else:
                    continue
                # Normalize index to UTC if tz-aware
                if series.index.tz is None:
                    series.index = series.index.tz_localize("UTC")
                result[ticker] = {
                    "ret_1m":  _period_return(series, 30),
                    "ret_3m":  _period_return(series, 90),
                    "ret_6m":  _period_return(series, 180),
                    "ret_1y":  _period_return(series, 365),
                }
            except Exception as e:
                logger.debug("[WHALE_PRICE] yf history error for %s: %s", ticker, e)

    except Exception as e:
        logger.warning("[WHALE_PRICE] yf batch download error: %s", e)

    return result


# ── Portfolio return calculator ───────────────────────────────────────────────

async def calculate_whale_returns(whale_name: str) -> dict:
    """
    Load the whale's most recent holdings, calculate weighted portfolio returns
    for 1m/3m/6m/1y vs SPY benchmark, save to DB.
    """
    holdings = _load_latest_holdings(whale_name)
    if not holdings:
        logger.warning("[WHALE_RET] No holdings for %s", whale_name)
        return {}

    quarter  = holdings[0].get("quarter", "")
    total_val = sum(h.get("value_usd", 0) or 0 for h in holdings)
    tickers  = [h["ticker"] for h in holdings if h.get("ticker")]

    loop = asyncio.get_event_loop()
    price_history = await loop.run_in_executor(_executor, _get_historical_returns_sync, tickers)

    spy_returns = price_history.get("SPY", {})

    # Weighted portfolio returns
    ret_1m = ret_3m = ret_6m = ret_1y = 0.0
    covered_weight = 0.0

    for h in holdings:
        ticker = h.get("ticker", "")
        weight = (h.get("weight_pct") or 0) / 100.0
        hist   = price_history.get(ticker, {})
        if not hist:
            continue
        if hist.get("ret_1m") is not None:
            ret_1m += weight * hist["ret_1m"]
        if hist.get("ret_3m") is not None:
            ret_3m += weight * hist["ret_3m"]
        if hist.get("ret_6m") is not None:
            ret_6m += weight * hist["ret_6m"]
        if hist.get("ret_1y") is not None:
            ret_1y += weight * hist["ret_1y"]
        covered_weight += weight

    if covered_weight > 0 and covered_weight < 1.0:
        scale = 1.0 / covered_weight
        ret_1m *= scale
        ret_3m *= scale
        ret_6m *= scale
        ret_1y *= scale

    ret_1m = round(ret_1m, 2)
    ret_3m = round(ret_3m, 2)
    ret_6m = round(ret_6m, 2)
    ret_1y = round(ret_1y, 2)
    spy_3m = round(spy_returns.get("ret_3m") or 0.0, 2)

    # Save to whale_portfolio_returns
    _save_portfolio_returns(whale_name, quarter, total_val, ret_3m, spy_3m)

    # Update whales table with latest returns
    _update_whale_returns(whale_name, ret_1m, ret_3m, ret_6m, ret_1y)

    logger.info(
        "[WHALE_RET] %s — 1m=%.1f%% 3m=%.1f%% 6m=%.1f%% 1y=%.1f%% (SPY 3m=%.1f%%)",
        whale_name, ret_1m, ret_3m, ret_6m, ret_1y, spy_3m,
    )
    return {"ret_1m": ret_1m, "ret_3m": ret_3m, "ret_6m": ret_6m, "ret_1y": ret_1y, "spy_3m": spy_3m}


def _load_latest_holdings(whale_name: str) -> list[dict]:
    conn = _get_conn()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT ticker, company_name, shares, value_usd, weight_pct, quarter
            FROM whale_holdings
            WHERE whale_name = %s
              AND quarter = (
                  SELECT quarter FROM whale_holdings WHERE whale_name = %s
                  ORDER BY quarter DESC LIMIT 1
              )
            ORDER BY weight_pct DESC
        """, (whale_name, whale_name))
        cols = ["ticker", "company_name", "shares", "value_usd", "weight_pct", "quarter"]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as e:
        logger.error("[WHALE_DB] Load holdings error for %s: %s", whale_name, e)
        return []
    finally:
        _put_conn(conn)


def _save_portfolio_returns(
    whale_name: str, quarter: str, portfolio_value: int,
    calculated_return: float, spy_return: float,
) -> None:
    conn = _get_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO whale_portfolio_returns
                (whale_name, quarter, portfolio_value_usd, calculated_return_pct, benchmark_spy_return_pct)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (whale_name, quarter, int(portfolio_value), calculated_return, spy_return))
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error("[WHALE_DB] Portfolio returns save error: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        _put_conn(conn)


def _update_whale_returns(
    whale_name: str,
    ret_1m: float, ret_3m: float, ret_6m: float, ret_1y: float,
) -> None:
    conn = _get_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE whales
            SET return_1m = %s, return_3m = %s, return_6m = %s,
                return_1y = %s, last_updated = NOW()
            WHERE name = %s
        """, (ret_1m, ret_3m, ret_6m, ret_1y, whale_name))
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error("[WHALE_DB] Update returns error for %s: %s", whale_name, e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        _put_conn(conn)


# ── AI theme generator ────────────────────────────────────────────────────────

async def generate_whale_theme(whale_name: str) -> str | None:
    """
    Calls Anthropic claude-3-haiku to generate a 2-3 sentence investment-theme
    summary based on the whale's top 15 holdings.
    """
    if not _ANTHROPIC_KEY:
        logger.warning("[WHALE_AI] ANTHROPIC_API_KEY not set")
        return None

    holdings = _load_latest_holdings(whale_name)[:15]
    if not holdings:
        return None

    holdings_text = "\n".join(
        f"  {i+1}. {h['ticker']} ({h.get('company_name','')}) — {h.get('weight_pct',0):.1f}% of portfolio"
        for i, h in enumerate(holdings)
    )
    prompt = (
        f"Based on these top holdings for {whale_name}:\n{holdings_text}\n\n"
        "Describe this investor's strategy and themes in 2-3 sentences. "
        "Be specific about sectors, investment styles, and macro themes. Be concise."
    )

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key":         _ANTHROPIC_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type":      "application/json",
                },
                json={
                    "model":      "claude-haiku-4-5",
                    "max_tokens": 200,
                    "messages":   [{"role": "user", "content": prompt}],
                },
            )
            resp.raise_for_status()
            data = resp.json()
            theme = data["content"][0]["text"].strip()
    except Exception as e:
        logger.error("[WHALE_AI] Anthropic error for %s: %s", whale_name, e)
        return None

    # Persist to DB
    conn = _get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE whales SET ai_theme = %s WHERE name = %s",
                (theme, whale_name),
            )
            conn.commit()
            cur.close()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            _put_conn(conn)

    logger.info("[WHALE_AI] Theme generated for %s: %s…", whale_name, theme[:60])
    return theme


# ── Single-whale refresh ──────────────────────────────────────────────────────

async def refresh_whale(whale_name: str) -> dict:
    """Refresh a single whale: fetch 13F → calculate returns → generate theme."""
    # Look up CIK from DB
    cik = _get_whale_cik(whale_name)
    if not cik:
        return {"error": f"Unknown whale: {whale_name}"}

    logger.info("[WHALE] Refreshing %s (CIK %s)", whale_name, cik)
    result: dict[str, Any] = {"whale": whale_name, "status": "ok"}

    try:
        holdings = await fetch_13f_filings(cik, whale_name)
        result["holdings_count"] = len(holdings)
    except Exception as e:
        logger.error("[WHALE] 13F fetch error for %s: %s", whale_name, e)
        result["holdings_error"] = str(e)
        holdings = []

    if holdings:
        try:
            returns = await calculate_whale_returns(whale_name)
            result["returns"] = returns
        except Exception as e:
            logger.error("[WHALE] Returns error for %s: %s", whale_name, e)
            result["returns_error"] = str(e)

        try:
            theme = await generate_whale_theme(whale_name)
            result["theme_generated"] = theme is not None
        except Exception as e:
            logger.error("[WHALE] Theme error for %s: %s", whale_name, e)
            result["theme_error"] = str(e)

    # Update last_updated regardless
    _touch_whale_timestamp(whale_name)
    return result


def _get_whale_cik(whale_name: str) -> str | None:
    conn = _get_conn()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute("SELECT cik FROM whales WHERE name = %s", (whale_name,))
        row = cur.fetchone()
        cur.close()
        return row[0] if row else None
    except Exception:
        return None
    finally:
        _put_conn(conn)


def _touch_whale_timestamp(whale_name: str) -> None:
    conn = _get_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("UPDATE whales SET last_updated = NOW() WHERE name = %s", (whale_name,))
        conn.commit()
        cur.close()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        _put_conn(conn)


# ── Master refresh ────────────────────────────────────────────────────────────

_refresh_in_progress = False


async def refresh_all_whales() -> dict:
    """Seed → loop through every whale → fetch 13F → returns → theme."""
    global _refresh_in_progress
    if _refresh_in_progress:
        return {"status": "already_running"}
    _refresh_in_progress = True

    results = []
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(_executor, seed_whales)
        whales = _load_all_whales()

        for w in whales:
            whale_name = w["name"]
            try:
                r = await refresh_whale(whale_name)
                results.append(r)
                await asyncio.sleep(1.0)   # be polite to SEC and external APIs
            except Exception as e:
                logger.error("[WHALE] Uncaught error for %s: %s", whale_name, e)
                results.append({"whale": whale_name, "error": str(e)})
    finally:
        _refresh_in_progress = False

    logger.info("[WHALE] refresh_all_whales complete: %d whales processed", len(results))
    return {"status": "ok", "whales_processed": len(results), "results": results}


def _load_all_whales() -> list[dict]:
    conn = _get_conn()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute("SELECT name, cik, category, description, last_updated FROM whales ORDER BY name")
        cols = ["name", "cik", "category", "description", "last_updated"]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as e:
        logger.error("[WHALE_DB] Load whales error: %s", e)
        return []
    finally:
        _put_conn(conn)


# ── Background loop ───────────────────────────────────────────────────────────

async def whale_watch_background_loop() -> None:
    """
    On cold start, checks if any whale's last_updated is older than 24 hours.
    If so, runs refresh_all_whales(). Then sleeps 24 hours and repeats.
    """
    await asyncio.sleep(30)   # let the app fully start first

    while True:
        try:
            needs_refresh = _whales_need_refresh()
            if needs_refresh:
                logger.info("[WHALE_LOOP] Starting scheduled refresh of all whales")
                await refresh_all_whales()
            else:
                logger.info("[WHALE_LOOP] All whales are up to date, skipping refresh")
        except Exception as e:
            logger.error("[WHALE_LOOP] Unexpected error: %s", e, exc_info=True)

        await asyncio.sleep(_REFRESH_INTERVAL)


def _whales_need_refresh() -> bool:
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        # True if table is empty OR any row is older than 24h
        cur.execute("SELECT COUNT(*) FROM whales")
        count = cur.fetchone()[0]
        if count == 0:
            return True
        cur.execute("""
            SELECT COUNT(*) FROM whales
            WHERE last_updated IS NULL
               OR last_updated < NOW() - INTERVAL '24 hours'
        """)
        stale = cur.fetchone()[0]
        cur.close()
        return stale > 0
    except Exception:
        return False
    finally:
        _put_conn(conn)


# ── FastAPI Router ────────────────────────────────────────────────────────────

router = APIRouter(tags=["whale-watch"])


@router.get("/whales")
async def get_whales(
    category: Optional[str] = Query(None, description="Filter by category: institution|individual|congress"),
):
    """
    Returns all tracked whales ordered by 3-month return (best performers first).
    """
    conn = _get_conn()
    if not conn:
        return []
    try:
        loop = asyncio.get_event_loop()

        def _fetch():
            _conn = _get_conn()
            if not _conn:
                return []
            try:
                cur = _conn.cursor()
                where = "WHERE category = %s" if category else ""
                params = (category,) if category else ()
                cur.execute(f"""
                    SELECT name, category, cik, description, ai_theme,
                           return_1m, return_3m, return_6m, return_1y, return_3y,
                           last_updated
                    FROM whales
                    {where}
                    ORDER BY return_3m DESC NULLS LAST
                """, params)
                cols = ["name", "category", "cik", "description", "ai_theme",
                        "return_1m", "return_3m", "return_6m", "return_1y", "return_3y",
                        "last_updated"]
                rows = cur.fetchall()
                cur.close()
                result = []
                for row in rows:
                    d = dict(zip(cols, row))
                    if d.get("last_updated"):
                        d["last_updated"] = d["last_updated"].isoformat()
                    result.append(d)
                return result
            except Exception as e:
                logger.error("[WHALE_API] GET /whales error: %s", e)
                return []
            finally:
                _put_conn(_conn)

        return await loop.run_in_executor(_executor, _fetch)
    finally:
        _put_conn(conn)


@router.get("/whales/{whale_name}/holdings")
async def get_whale_holdings(whale_name: str):
    """
    Returns all holdings for the specified whale from the most recent quarter,
    ordered by weight_pct descending.
    """
    loop = asyncio.get_event_loop()

    def _fetch():
        conn = _get_conn()
        if not conn:
            return []
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT ticker, company_name, shares, value_usd, weight_pct, quarter, filed_date
                FROM whale_holdings
                WHERE whale_name = %s
                  AND quarter = (
                      SELECT quarter FROM whale_holdings WHERE whale_name = %s
                      ORDER BY quarter DESC LIMIT 1
                  )
                ORDER BY weight_pct DESC
            """, (whale_name, whale_name))
            cols = ["ticker", "company_name", "shares", "value_usd", "weight_pct", "quarter", "filed_date"]
            rows = cur.fetchall()
            cur.close()
            result = []
            for row in rows:
                d = dict(zip(cols, row))
                if d.get("filed_date"):
                    d["filed_date"] = d["filed_date"].isoformat()
                result.append(d)
            return result
        except Exception as e:
            logger.error("[WHALE_API] Holdings error for %s: %s", whale_name, e)
            return []
        finally:
            _put_conn(conn)

    holdings = await loop.run_in_executor(_executor, _fetch)
    if not holdings:
        raise HTTPException(status_code=404, detail=f"No holdings found for: {whale_name}")
    return {"whale_name": whale_name, "holdings": holdings, "count": len(holdings)}


@router.get("/whales/{whale_name}/returns")
async def get_whale_returns(whale_name: str):
    """
    Returns all quarterly return records for the specified whale.
    """
    loop = asyncio.get_event_loop()

    def _fetch():
        conn = _get_conn()
        if not conn:
            return []
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT quarter, portfolio_value_usd, calculated_return_pct,
                       benchmark_spy_return_pct, created_at
                FROM whale_portfolio_returns
                WHERE whale_name = %s
                ORDER BY quarter DESC
            """, (whale_name,))
            cols = ["quarter", "portfolio_value_usd", "calculated_return_pct",
                    "benchmark_spy_return_pct", "created_at"]
            rows = cur.fetchall()
            cur.close()
            result = []
            for row in rows:
                d = dict(zip(cols, row))
                if d.get("created_at"):
                    d["created_at"] = d["created_at"].isoformat()
                result.append(d)
            return result
        except Exception as e:
            logger.error("[WHALE_API] Returns error for %s: %s", whale_name, e)
            return []
        finally:
            _put_conn(conn)

    records = await loop.run_in_executor(_executor, _fetch)
    return {"whale_name": whale_name, "quarterly_returns": records}


@router.post("/whales/refresh")
async def trigger_refresh_all(background_tasks: BackgroundTasks):
    """Manually trigger a full refresh of all whales (runs in background)."""
    if _refresh_in_progress:
        return {"status": "already_running", "message": "A refresh is already in progress"}
    background_tasks.add_task(refresh_all_whales)
    return {
        "status": "started",
        "message": f"Refreshing all {len(DEFAULT_WHALES)} whales in the background. "
                   "This may take several minutes. Poll GET /api/whales for progress.",
    }


@router.post("/whales/{whale_name}/refresh")
async def trigger_refresh_single(whale_name: str, background_tasks: BackgroundTasks):
    """Manually trigger a refresh for a single whale."""
    if _refresh_in_progress:
        return {"status": "already_running", "message": "A global refresh is already in progress"}
    background_tasks.add_task(refresh_whale, whale_name)
    return {
        "status": "started",
        "whale_name": whale_name,
        "message": "Refresh started in background. Poll GET /api/whales for results.",
    }
