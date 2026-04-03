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

_ANTHROPIC_KEY  = os.getenv("ANTHROPIC_API_KEY", "")
_TRADIER_KEY    = os.getenv("TRADIER_API_KEY", "")
_PERPLEXITY_KEY = os.getenv("PERPLEXITY_API_KEY", "")
_FMP_KEY        = os.getenv("FMP_API_KEY", "")
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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS whale_transactions (
                id               SERIAL PRIMARY KEY,
                whale_name       TEXT NOT NULL,
                ticker           TEXT NOT NULL,
                company_name     TEXT,
                transaction_type TEXT NOT NULL,
                shares           BIGINT,
                value_usd        BIGINT,
                quarter          TEXT,
                filed_date       TIMESTAMP,
                created_at       TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_wt_whale_quarter
            ON whale_transactions (whale_name, quarter)
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


# ── Perplexity whale discovery ────────────────────────────────────────────────

async def lookup_cik_on_edgar(name: str) -> str | None:
    """
    Query SEC EDGAR full-text search to find the CIK of a fund by name,
    restricting to 13F-HR filings filed since 2020.
    Returns a zero-padded 10-digit CIK string, or None if not found.
    """
    try:
        url = (
            f'https://efts.sec.gov/LATEST/search-index?q="{name}"'
            "&dateRange=custom&startdt=2020-01-01&forms=13F-HR"
        )
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, headers=_SEC_HEADERS)
            resp.raise_for_status()
            data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        if hits:
            cik = str(hits[0].get("_source", {}).get("entity_id", "")).strip()
            if cik and cik.isdigit():
                return cik.zfill(10)
    except Exception as e:
        logger.warning("[WHALE_CIK] EDGAR lookup failed for '%s': %s", name, e)
    return None


async def discover_top_whales_via_perplexity() -> list[dict]:
    """
    Call Perplexity sonar to discover the top-performing hedge funds/investors
    right now, verify each has a CIK (must be a 13F filer), and return the list.
    Returns [] if Perplexity fails so callers can fall back gracefully.
    """
    if not _PERPLEXITY_KEY:
        logger.warning("[WHALE_DISCOVER] PERPLEXITY_API_KEY not set — skipping discovery")
        return []

    system_prompt = (
        "You are a financial research assistant. "
        "Return only valid JSON, no markdown, no explanation, no code blocks."
    )
    user_prompt = (
        "Search the web right now for institutional investors and hedge funds that have had the highest "
        "verified returns in the past 1-3 years. I want breakout performers — funds that went concentrated "
        "into AI, Nvidia, GLP-1 drugs, uranium, biotech, or energy and returned 100% to 1000%. Include "
        "funds like Duquesne Family Office, Whale Rock Capital, Coatue Management, Tiger Global, "
        "Andreessen Horowitz public equities, Senvest Management, Voss Capital, Greenlight Capital, "
        "Kerrisdale Capital, and any other fund with verified 100%+ returns. For each fund return: name, "
        "SEC CIK number, approximate return percentage, time period, and one sentence description. "
        "Return only a valid JSON array, no markdown, no explanation."
    )

    logger.info("[WHALE_DISCOVER] Querying Perplexity for top-performing funds…")
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                "https://api.perplexity.ai/chat/completions",
                headers={
                    "Authorization": f"Bearer {_PERPLEXITY_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "sonar",
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user",   "content": user_prompt},
                    ],
                    "temperature": 0.2,
                },
            )
            resp.raise_for_status()
            raw_content = resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error("[WHALE_DISCOVER] Perplexity API error: %s", e)
        return []

    # Strip any markdown code fences Perplexity may wrap around the JSON
    content = raw_content.strip()
    if content.startswith("```"):
        content = re.sub(r"^```[a-z]*\n?", "", content)
        content = re.sub(r"\n?```$", "", content)
    content = content.strip()

    try:
        candidates = json.loads(content)
        if not isinstance(candidates, list):
            raise ValueError("Expected a JSON array")
    except Exception as e:
        logger.error("[WHALE_DISCOVER] JSON parse error: %s | raw: %s", e, content[:500])
        return []

    logger.info("[WHALE_DISCOVER] Perplexity returned %d candidates", len(candidates))

    verified: list[dict] = []
    for c in candidates:
        name = (c.get("name") or "").strip()
        if not name:
            continue

        # Normalise CIK: strip non-digits; treat 'null'/'unknown' as missing
        raw_cik = str(c.get("cik") or "").strip()
        cik = re.sub(r"[^0-9]", "", raw_cik) if raw_cik.lower() not in ("null", "unknown", "none", "") else ""

        if not cik:
            logger.info("[WHALE_DISCOVER] No CIK for '%s' — querying EDGAR…", name)
            cik = await lookup_cik_on_edgar(name) or ""
            await asyncio.sleep(0.3)   # be polite to EDGAR

        if not cik:
            logger.info("[WHALE_DISCOVER] Skipping '%s' — could not find SEC CIK", name)
            continue

        est_return = c.get("estimated_return_pct", 0)
        verified.append({
            "name":                 name,
            "cik":                  cik.zfill(10),
            "category":             c.get("category", "institution"),
            "description":          c.get("description", ""),
            "estimated_return_pct": est_return,
        })
        logger.info("[WHALE_DISCOVER] ✓ %s | CIK %s | est. return ~%.0f%%",
                    name, cik, est_return)

    logger.info("[WHALE_DISCOVER] %d/%d candidates verified with SEC CIKs",
                len(verified), len(candidates))
    return verified


# ── Seed whales ───────────────────────────────────────────────────────────────

async def seed_whales() -> None:
    """
    Upsert whales into DB.
    Attempts Perplexity discovery first; falls back to DEFAULT_WHALES if discovery
    fails or returns an empty list so the app never breaks.
    """
    whales_to_seed: list[dict] = []
    try:
        discovered = await discover_top_whales_via_perplexity()
        if discovered:
            whales_to_seed = discovered
        else:
            logger.info("[WHALE] Discovery returned nothing — using default whale list")
            whales_to_seed = DEFAULT_WHALES
    except Exception as e:
        logger.error("[WHALE] Discovery error (%s) — falling back to default list", e)
        whales_to_seed = DEFAULT_WHALES

    conn = _get_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        for w in whales_to_seed:
            cur.execute("""
                INSERT INTO whales (name, category, cik, description)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (name) DO UPDATE SET
                    cik         = EXCLUDED.cik,
                    description = EXCLUDED.description,
                    category    = EXCLUDED.category
            """, (w["name"], w["category"], w["cik"], w.get("description", "")))
        conn.commit()
        cur.close()
        logger.info("[WHALE] Seeded %d whales into DB", len(whales_to_seed))
    except Exception as e:
        logger.error("[WHALE] Seed DB error: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        _put_conn(conn)


# ── Famous Investor Discovery ─────────────────────────────────────────────────

async def discover_famous_investors_via_perplexity() -> list[dict]:
    """
    Call Perplexity to find known public stock positions for a fixed list of famous investors.
    Upserts each person into whales with category='famous_investor' and cik=NULL.
    Saves their known positions into whale_holdings with null weights/values.
    Stores estimated 1-year return directly into return_1y on the whales table.
    """
    if not _PERPLEXITY_KEY:
        logger.warning("[FAMOUS] PERPLEXITY_API_KEY not set — skipping famous investor discovery")
        return []

    from datetime import date as _date
    import json as _json, re as _re

    today = _date.today()
    q = (today.month - 1) // 3 + 1
    current_quarter = f"{today.year}Q{q}"

    system_prompt = (
        "You are a financial research assistant. "
        "Return only valid JSON, no markdown, no explanation, no code blocks."
    )
    user_prompt = (
        "Search the web right now for these specific investors and their known public stock positions, "
        "recent portfolio disclosures, and verified returns over the past 12 months: "
        "Stanley Druckenmiller, Eric Sprott, Robert Friedland, Mike Novogratz, "
        "Chamath Palihapitiya, Peter Thiel, Christian Arquette, Eric Jackson, "
        "Mike Alfred, Matthew Augustin. "
        "For each person find: their known public stock positions disclosed in interviews, filings, or news, "
        "their approximate 1-year return if mentioned anywhere. "
        "Return a JSON array where each object has: "
        "name (string), description (one sentence about their strategy), "
        "estimated_return_1y_pct (number or null), "
        "known_positions (array of ticker strings, e.g. [\"NVDA\", \"GOLD\"]). "
        "Return only a valid JSON array, no markdown, no explanation."
    )

    logger.info("[FAMOUS] Querying Perplexity for famous investors…")
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                "https://api.perplexity.ai/chat/completions",
                headers={
                    "Authorization": f"Bearer {_PERPLEXITY_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "sonar",
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user",   "content": user_prompt},
                    ],
                },
            )
            resp.raise_for_status()
            raw = resp.json()
    except Exception as e:
        logger.error("[FAMOUS] Perplexity request failed: %s", e)
        return []

    content = raw.get("choices", [{}])[0].get("message", {}).get("content", "")
    try:
        cleaned = _re.sub(r"```(?:json)?", "", content).strip().rstrip("`").strip()
        investors = _json.loads(cleaned)
        if not isinstance(investors, list):
            raise ValueError("Expected a JSON array")
    except Exception as e:
        logger.error("[FAMOUS] Failed to parse Perplexity response: %s | raw: %s", e, content[:300])
        return []

    conn = _get_conn()
    if not conn:
        return []

    saved = []
    try:
        cur = conn.cursor()
        for person in investors:
            name = (person.get("name") or "").strip()
            if not name:
                continue
            description = (person.get("description") or "").strip()
            est_return = person.get("estimated_return_1y_pct")
            positions = person.get("known_positions") or []

            cur.execute("""
                INSERT INTO whales (name, category, cik, description)
                VALUES (%s, 'famous_investor', NULL, %s)
                ON CONFLICT (name) DO UPDATE SET
                    description = EXCLUDED.description,
                    category    = 'famous_investor',
                    cik         = NULL
            """, (name, description))

            if est_return is not None:
                try:
                    cur.execute(
                        "UPDATE whales SET return_1y = %s WHERE name = %s",
                        (float(est_return), name),
                    )
                except Exception:
                    pass

            if positions:
                cur.execute(
                    "DELETE FROM whale_holdings WHERE whale_name = %s AND quarter = %s",
                    (name, current_quarter),
                )
                for ticker in positions:
                    ticker = str(ticker).strip().upper()
                    if not ticker or "." in ticker or len(ticker) > 6:
                        continue
                    try:
                        cur.execute("""
                            INSERT INTO whale_holdings
                                (whale_name, ticker, company_name, shares, value_usd, weight_pct, quarter)
                            VALUES (%s, %s, %s, NULL, NULL, NULL, %s)
                            ON CONFLICT DO NOTHING
                        """, (name, ticker, ticker, current_quarter))
                    except Exception:
                        pass

            saved.append({
                "name": name,
                "description": description,
                "estimated_return_1y_pct": est_return,
                "positions": positions,
            })
            logger.info("[FAMOUS] Upserted %s | return_1y=%s | %d positions",
                        name, est_return, len(positions))

        conn.commit()
        cur.close()
    except Exception as e:
        logger.error("[FAMOUS] DB upsert error: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        _put_conn(conn)

    logger.info("[FAMOUS] Saved %d famous investors", len(saved))
    return saved


# ── CUSIP → Ticker mapping via FMP + SEC EDGAR ───────────────────────────────

def _is_valid_us_ticker(ticker: str) -> bool:
    """
    Accept US equity tickers: 1-5 uppercase letters, optional hyphen+1-2 chars (e.g. BRK-A).
    Reject dot-separated ADR/preferred formats (e.g. BRK.B, PRF.PRA).
    """
    if not ticker or "." in ticker:
        return False
    # Standard plain ticker
    if re.match(r"^[A-Z]{1,5}$", ticker):
        return True
    # Hyphenated class (BRK-A, BRK-B, BF-B) — max 7 chars total
    if re.match(r"^[A-Z]{1,5}-[A-Z]{1,2}$", ticker) and len(ticker) <= 7:
        return True
    return False


# ── EDGAR company tickers index (loaded once, cached in memory) ───────────────

_ticker_name_index: dict[str, str] | None = None   # normalised_name → ticker
_EDGAR_TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"

_NAME_SUFFIXES = (
    " INC", " CORP", " CORPORATION", " LTD", " LIMITED", " CO", " COMPANY",
    " LLC", " LP", " PLC", " NV", " N V", " SA", " AG", " SE", " BV",
    " GROUP", " HOLDINGS", " HOLDING", " INTERNATIONAL", " INTL",
    " FUND", " ETF", " TRUST", " REIT",
    " CLASS A", " CLASS B", " CLASS C", " CL A", " CL B", " CL C",
    " SHS", " SHARE", " SHARES", " COMMON", " COM", " ADR", " ADS",
    " NEW", " THE",
)


def _normalize_company_name(name: str) -> str:
    """
    Normalise a company name for fuzzy matching.
    Order matters: strip punctuation FIRST (so "Apple Inc." → "APPLE INC"),
    then iteratively strip trailing legal/share-class suffixes.
    """
    name = name.upper().strip()
    # 0. Remove jurisdictional designators like "/CA", "/DE", "/NV" before anything else
    name = re.sub(r"/[A-Z]{2,3}$", "", name).strip()
    # 1. Strip all punctuation first so suffix patterns match cleanly
    name = re.sub(r"[^A-Z0-9 ]", " ", name)
    name = " ".join(name.split())
    # 2. Iteratively strip trailing suffixes (handles stacked ones like "INC CL A")
    changed = True
    while changed:
        changed = False
        for sfx in _NAME_SUFFIXES:
            if name.endswith(sfx):
                name = name[: -len(sfx)].strip()
                changed = True
    return name


async def _load_edgar_tickers_index() -> dict[str, str]:
    """
    Download SEC EDGAR's company_tickers_exchange.json (all listed US companies)
    and build a normalised-name → ticker lookup index.
    Result is cached in _ticker_name_index for the lifetime of the process.
    """
    global _ticker_name_index
    if _ticker_name_index is not None:
        return _ticker_name_index

    logger.info("[WHALE_EDGAR] Downloading company tickers index from SEC…")
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(_EDGAR_TICKERS_URL, headers=_SEC_HEADERS)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        logger.error("[WHALE_EDGAR] Failed to download tickers index: %s", e)
        _ticker_name_index = {}
        return {}

    # JSON is column-oriented: {"fields": ["cik","name","ticker","exchange"], "data": [[...],...]}
    fields = data.get("fields", [])
    rows   = data.get("data", [])
    try:
        i_name = fields.index("name")
        i_tick = fields.index("ticker")
        i_exch = fields.index("exchange")
    except ValueError as e:
        logger.error("[WHALE_EDGAR] Unexpected tickers JSON schema: %s | fields=%s", e, fields)
        _ticker_name_index = {}
        return {}

    # Prefer primary US exchanges; allow OTC as last resort
    PREF_EXCHANGES = {"Nasdaq", "NYSE", "NYSE ARCA", "NYSE MKT", "AMEX"}
    index: dict[str, str] = {}
    index_ot: dict[str, str] = {}  # OTC-only bucket (lower priority)

    for row in rows:
        try:
            ticker = str(row[i_tick]).strip()
            name   = str(row[i_name]).strip()
            exch   = str(row[i_exch])
        except (IndexError, TypeError):
            continue
        if not (ticker and name and _is_valid_us_ticker(ticker)):
            continue
        key = _normalize_company_name(name)
        if not key:
            continue
        if exch in PREF_EXCHANGES:
            index[key] = ticker.upper()
        else:
            if key not in index_ot:
                index_ot[key] = ticker.upper()

    # Merge OTC into index only for keys not already covered by a primary exchange
    for k, v in index_ot.items():
        if k not in index:
            index[k] = v

    _ticker_name_index = index
    logger.info("[WHALE_EDGAR] Ticker index loaded: %d companies", len(index))
    return index


async def _edgar_name_to_ticker(company_name: str, client: httpx.AsyncClient) -> str:
    """
    EDGAR full-text search fallback: search the EDGAR search API for the company
    name and extract the ticker from the first 10-K hit.
    Returns ticker string or '' on failure.
    """
    if not company_name:
        return ""
    try:
        resp = await client.get(
            "https://efts.sec.gov/LATEST/search-index",
            params={
                "q":         f'"{company_name}"',
                "forms":     "10-K",
                "dateRange": "custom",
                "startdt":   "2018-01-01",
            },
            headers=_SEC_HEADERS,
            timeout=12,
        )
        if resp.status_code == 200:
            hits = resp.json().get("hits", {}).get("hits", [])
            for hit in hits:
                ticker = hit.get("_source", {}).get("ticker", "")
                if _is_valid_us_ticker(ticker):
                    return ticker.upper()
    except Exception as e:
        logger.debug("[WHALE_EDGAR] Name FTS lookup error for '%s': %s", company_name, e)
    return ""


async def _cusips_to_tickers(
    cusips: list[str],
    names: Optional[dict[str, str]] = None,
) -> dict[str, str]:
    """
    Map CUSIPs → tickers using SEC EDGAR data only. No OpenFIGI, no FMP.

    Pipeline:
      1. EDGAR company_tickers_exchange.json — in-memory normalised-name lookup
         (one HTTP download per process lifetime; thereafter pure dict lookups)
      2. EDGAR full-text search by nameOfIssuer — for names that didn't match
         the index (concurrent, semaphore-limited to respect the 10 req/s guideline)

    Args:
        cusips: list of CUSIP strings to resolve
        names:  dict mapping cusip → nameOfIssuer from the 13F XML
                (used for both name-index lookup and FTS fallback)
    """
    if not cusips:
        return {}

    names = names or {}

    # ── Pass 1: EDGAR tickers index (fast in-memory lookup) ─────────────────
    ticker_index = await _load_edgar_tickers_index()
    mapping:  dict[str, str] = {}
    need_fts: list[str]      = []

    for cusip in cusips:
        raw_name = names.get(cusip, "")
        if not raw_name:
            need_fts.append(cusip)   # no name → try FTS anyway
            continue

        norm = _normalize_company_name(raw_name)
        if norm in ticker_index:
            mapping[cusip] = ticker_index[norm]
            continue

        # Try progressively shorter prefixes (handles " INC A" suffix noise)
        parts = norm.split()
        matched = False
        for length in range(len(parts) - 1, max(1, len(parts) - 3), -1):
            shorter = " ".join(parts[:length])
            if shorter in ticker_index:
                mapping[cusip] = ticker_index[shorter]
                matched = True
                break
        if not matched:
            need_fts.append(cusip)

    logger.info("[WHALE_EDGAR] Index match: %d/%d CUSIPs; %d need FTS fallback",
                len(mapping), len(cusips), len(need_fts))

    # ── Pass 2: EDGAR full-text search for names that didn't match ───────────
    if need_fts:
        sem = asyncio.Semaphore(5)   # respect EDGAR's ~10 req/s guideline

        async def _fts_one(cusip: str) -> tuple[str, str]:
            async with sem:
                company_name = names.get(cusip, "")
                async with httpx.AsyncClient(timeout=12) as client:
                    ticker = await _edgar_name_to_ticker(company_name, client)
                await asyncio.sleep(0.1)
            return cusip, ticker

        fts_results = await asyncio.gather(
            *[_fts_one(c) for c in need_fts],
            return_exceptions=True,
        )
        fts_hits = 0
        for r in fts_results:
            if isinstance(r, tuple):
                cusip, ticker = r
                if ticker:
                    mapping[cusip] = ticker
                    fts_hits += 1

        logger.info("[WHALE_EDGAR] FTS recovered %d more tickers (%d total mapped / %d)",
                    fts_hits, len(mapping), len(cusips))

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

    # Sort by value descending so we map the highest-value positions first
    holdings_raw.sort(key=lambda h: h.get("value_usd", 0), reverse=True)

    # Step 5: Map CUSIPs → tickers via FMP + EDGAR
    # Cap at 500 unique CUSIPs by value — top positions = 95%+ of portfolio weight.
    # Collect company names (nameOfIssuer from 13F XML) alongside CUSIPs so the
    # EDGAR name-search fallback inside _cusips_to_tickers can use them.
    _MAX_CUSIPS = 500
    seen_cusips:  set[str]        = set()
    cusips_to_map: list[str]      = []
    cusip_to_name: dict[str, str] = {}
    for h in holdings_raw:
        c = h.get("cusip", "")
        if c and c not in seen_cusips:
            seen_cusips.add(c)
            cusips_to_map.append(c)
            cusip_to_name[c] = h.get("company_name", "")
        if len(cusips_to_map) >= _MAX_CUSIPS:
            break
    total_unique = len({h.get("cusip", "") for h in holdings_raw if h.get("cusip")})
    if len(cusips_to_map) < total_unique:
        logger.info("[WHALE] Capping %s CUSIP lookups at top %d (total raw: %d)",
                    whale_name, len(cusips_to_map), len(holdings_raw))
    cusip_map = await _cusips_to_tickers(cusips_to_map, names=cusip_to_name)

    # Step 6: Calculate weight percentages
    total_value = sum(h["value_usd"] for h in holdings_raw)
    if total_value <= 0:
        total_value = 1

    final_holdings: list[dict] = []
    for h in holdings_raw:
        ticker = cusip_map.get(h.get("cusip", ""), "")
        if not ticker:
            continue  # skip positions we could not resolve to a real ticker

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

    # Deduplicate by ticker: some filers (e.g. Berkshire) report the same stock
    # across multiple subsidiary accounts; merge shares + value, keep best name
    ticker_agg: dict[str, dict] = {}
    for h in final_holdings:
        t = h["ticker"]
        if t in ticker_agg:
            ticker_agg[t]["shares"]    += h["shares"]
            ticker_agg[t]["value_usd"] += h["value_usd"]
        else:
            ticker_agg[t] = dict(h)

    # Recalculate weight_pct on merged totals
    merged_total = sum(h["value_usd"] for h in ticker_agg.values()) or 1
    final_holdings = []
    for h in ticker_agg.values():
        h["weight_pct"] = round(h["value_usd"] / merged_total * 100, 4)
        final_holdings.append(h)

    # Sort by weight descending
    final_holdings.sort(key=lambda x: x["value_usd"], reverse=True)

    # Step 7: Delete old holdings for this whale+quarter and insert new
    _save_holdings_to_db(whale_name, quarter, final_holdings)

    # Step 8: Detect new buys vs previous quarter and save to whale_transactions
    if quarter and final_holdings:
        _detect_and_save_new_buys(whale_name, quarter, final_holdings, filed_date)

    logger.info("[WHALE] Saved %d holdings for %s (%s)", len(final_holdings), whale_name, quarter)
    return final_holdings


def _parse_infotable_xml(xml_text: str) -> list[dict]:
    """Parse 13F infotable XML; handles both old and new EDGAR namespace variants."""
    records = []
    try:
        # Strip namespaces for simpler parsing
        # Step 1: Remove all xmlns declarations (xmlns="..." and xmlns:prefix="...")
        clean = re.sub(r'\s+xmlns(?::[a-zA-Z0-9_-]+)?="[^"]*"', '', xml_text)
        # Step 2: Remove namespace prefixes from element names, preserving < vs </
        #         e.g. <ns1:infoTable> → <infoTable>, </ns1:infoTable> → </infoTable>
        clean = re.sub(r'(</?)[a-zA-Z_][\w]*:', r'\1', clean)
        # Step 3: Remove namespace-prefixed attributes (e.g. xsi:type="...")
        clean = re.sub(r'\s+[a-zA-Z_][\w]*:[a-zA-Z_][\w]*="[^"]*"', '', clean)
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
    if not holdings:
        # Don't wipe existing data if we failed to resolve any tickers
        logger.warning("[WHALE_DB] Skipping save for %s (%s): 0 holdings resolved", whale_name, quarter)
        return
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


# ── New buys detection ────────────────────────────────────────────────────────

def _detect_and_save_new_buys(
    whale_name: str,
    current_quarter: str,
    current_holdings: list[dict],
    filed_date,
) -> int:
    """
    Compare current quarter's holdings against the previous quarter.
    - Tickers present now but not before → transaction_type = 'NEW'
    - Tickers where shares grew by >20% → transaction_type = 'ADDED'
    Saves results to whale_transactions and returns the count of transactions saved.
    """
    conn = _get_conn()
    if not conn:
        return 0
    try:
        cur = conn.cursor()

        # Load previous quarter holdings for this whale (any quarter != current, most recent)
        cur.execute("""
            SELECT ticker, shares
            FROM whale_holdings
            WHERE whale_name = %s
              AND quarter != %s
              AND quarter = (
                  SELECT quarter FROM whale_holdings
                  WHERE whale_name = %s AND quarter != %s
                  ORDER BY quarter DESC LIMIT 1
              )
        """, (whale_name, current_quarter, whale_name, current_quarter))
        prev_rows = cur.fetchall()
        prev_holdings: dict[str, int] = {row[0]: (row[1] or 0) for row in prev_rows}

        if not prev_holdings:
            logger.info("[WHALE_NB] No previous quarter to compare for %s — skipping new buys", whale_name)
            cur.close()
            return 0

        # Delete any existing transactions for this whale + quarter so we can reinsert fresh
        cur.execute(
            "DELETE FROM whale_transactions WHERE whale_name = %s AND quarter = %s",
            (whale_name, current_quarter),
        )

        count = 0
        for h in current_holdings:
            ticker = h.get("ticker", "")
            if not ticker:
                continue
            curr_shares = h.get("shares") or 0

            if ticker not in prev_holdings:
                tx_type = "NEW"
            else:
                prev_shares = prev_holdings[ticker]
                if prev_shares > 0 and curr_shares > prev_shares * 1.20:
                    tx_type = "ADDED"
                else:
                    continue

            cur.execute("""
                INSERT INTO whale_transactions
                    (whale_name, ticker, company_name, transaction_type, shares, value_usd, quarter, filed_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                whale_name,
                ticker,
                h.get("company_name"),
                tx_type,
                curr_shares,
                h.get("value_usd"),
                current_quarter,
                filed_date,
            ))
            count += 1

        conn.commit()
        cur.close()
        logger.info("[WHALE_NB] Saved %d new-buy transactions for %s (%s)", count, whale_name, current_quarter)
        return count
    except Exception as e:
        logger.error("[WHALE_NB] New buys detection error for %s: %s", whale_name, e)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
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
    Values are percentage returns (float).
    Primary source: Tradier history API.
    Fallback: yfinance (per-ticker, only when Tradier fails or returns empty data).
    """
    import time
    import requests
    import yfinance as yf
    import pandas as pd
    from datetime import date, timedelta

    result: dict[str, dict] = {}
    if not tickers:
        return result

    all_tickers = list(set(tickers + ["SPY"]))
    tradier_key = os.environ.get("TRADIER_API_KEY", "")

    today = date.today()
    start = today - timedelta(days=730)
    today_str = today.strftime("%Y-%m-%d")
    start_str = start.strftime("%Y-%m-%d")

    def _period_return_from_days(day_list: list[dict], days: int) -> float | None:
        """
        day_list: list of {"date": "YYYY-MM-DD", "close": float} sorted ascending.
        Returns (latest_close / close_N_days_ago - 1) * 100 or None.
        """
        if not day_list:
            return None
        target = today - timedelta(days=days)
        target_str = target.strftime("%Y-%m-%d")
        past_candidates = [d for d in day_list if d["date"] <= target_str]
        if not past_candidates:
            return None
        past_price = float(past_candidates[-1]["close"])
        current_price = float(day_list[-1]["close"])
        if past_price == 0:
            return None
        return round((current_price / past_price - 1) * 100, 2)

    def _yf_fallback(ticker: str) -> dict | None:
        """yfinance fallback for a single ticker."""
        try:
            data = yf.download(
                [ticker],
                period="2y",
                interval="1d",
                progress=False,
                auto_adjust=True,
            )
            if data.empty:
                return None
            if isinstance(data.columns, pd.MultiIndex):
                series = data["Close"][ticker].dropna()
            else:
                series = data["Close"].dropna() if "Close" in data.columns else data.iloc[:, 0].dropna()
            if series.empty:
                return None
            ts_today = pd.Timestamp.now(tz="UTC").normalize()
            if series.index.tz is None:
                series.index = series.index.tz_localize("UTC")

            def _pr(days: int) -> float | None:
                target = ts_today - pd.Timedelta(days=days)
                past = series[series.index <= target]
                if past.empty or pd.isna(past.iloc[-1]):
                    return None
                curr_price = float(series.dropna().iloc[-1])
                past_price = float(past.iloc[-1])
                if past_price == 0:
                    return None
                return round((curr_price / past_price - 1) * 100, 2)

            return {
                "ret_1m": _pr(30),
                "ret_3m": _pr(90),
                "ret_6m": _pr(180),
                "ret_1y": _pr(365),
            }
        except Exception as e:
            logger.debug("[WHALE_PRICE] yf fallback error for %s: %s", ticker, e)
            return None

    tickers_needing_fallback: list[str] = []

    for i, ticker in enumerate(all_tickers):
        if i > 0:
            time.sleep(0.2)

        fetched = False
        if tradier_key:
            try:
                resp = requests.get(
                    "https://api.tradier.com/v1/markets/history",
                    headers={
                        "Authorization": f"Bearer {tradier_key}",
                        "Accept": "application/json",
                    },
                    params={
                        "symbol":   ticker,
                        "interval": "daily",
                        "start":    start_str,
                        "end":      today_str,
                    },
                    timeout=10,
                )
                if resp.status_code == 200:
                    payload = resp.json()
                    history = (payload.get("history") or {})
                    day_list = history.get("day") if history else None
                    if day_list:
                        if isinstance(day_list, dict):
                            day_list = [day_list]
                        day_list = sorted(day_list, key=lambda d: d["date"])
                        result[ticker] = {
                            "ret_1m": _period_return_from_days(day_list, 30),
                            "ret_3m": _period_return_from_days(day_list, 90),
                            "ret_6m": _period_return_from_days(day_list, 180),
                            "ret_1y": _period_return_from_days(day_list, 365),
                        }
                        fetched = True
                    else:
                        logger.debug("[WHALE_PRICE] Tradier empty data for %s", ticker)
                else:
                    logger.debug("[WHALE_PRICE] Tradier %s status %s", ticker, resp.status_code)
            except Exception as e:
                logger.debug("[WHALE_PRICE] Tradier error for %s: %s", ticker, e)

        if not fetched:
            tickers_needing_fallback.append(ticker)

    for ticker in tickers_needing_fallback:
        logger.debug("[WHALE_PRICE] yfinance fallback for %s", ticker)
        ret = _yf_fallback(ticker)
        if ret:
            result[ticker] = ret

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
        "Be specific about sectors, investment styles, and macro themes. Be concise. "
        "Respond in plain text only. No markdown, no headers, no bullet points, no bold text."
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

    import re
    theme = re.sub(r'[#*`]+', '', theme).strip()
    theme = re.sub(r'\s+', ' ', theme).strip()

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
    whale_info = _get_whale_info(whale_name)
    if not whale_info:
        return {"error": f"Unknown whale: {whale_name}"}

    category = whale_info.get("category", "institution")
    cik = whale_info.get("cik")

    # Famous investors: no 13F, no returns calculation — just (re)generate theme
    if category == "famous_investor":
        logger.info("[WHALE] Refreshing famous investor %s (no CIK)", whale_name)
        result: dict[str, Any] = {"whale": whale_name, "status": "ok", "category": "famous_investor"}
        holdings = _load_latest_holdings(whale_name)
        if holdings:
            try:
                theme = await generate_whale_theme(whale_name)
                result["theme_generated"] = theme is not None
            except Exception as e:
                logger.error("[WHALE] Theme error for %s: %s", whale_name, e)
                result["theme_error"] = str(e)
        _touch_whale_timestamp(whale_name)
        return result

    if not cik:
        return {"error": f"Unknown whale: {whale_name}"}

    logger.info("[WHALE] Refreshing %s (CIK %s)", whale_name, cik)
    result = {"whale": whale_name, "status": "ok"}

    try:
        holdings = await fetch_13f_filings(cik, whale_name)
        result["holdings_count"] = len(holdings)
    except Exception as e:
        logger.error("[WHALE] 13F fetch error for %s: %s", whale_name, e)
        result["holdings_error"] = str(e)
        holdings = []

    # Always calculate returns (even if holdings came from DB cache, not fresh fetch)
    try:
        returns = await calculate_whale_returns(whale_name)
        result["returns"] = returns
        if returns:
            logger.info(
                "[WHALE_RET] %s — 1m=%+.1f%% 3m=%+.1f%% 6m=%+.1f%% 1y=%+.1f%% (SPY 3m=%+.1f%%)",
                whale_name,
                returns.get("ret_1m") or 0,
                returns.get("ret_3m") or 0,
                returns.get("ret_6m") or 0,
                returns.get("ret_1y") or 0,
                returns.get("spy_3m") or 0,
            )
    except Exception as e:
        logger.error("[WHALE] Returns error for %s: %s", whale_name, e)
        result["returns_error"] = str(e)

    if holdings:
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


def _get_whale_info(whale_name: str) -> dict:
    """Returns {'cik': ..., 'category': ...} or {} if not found."""
    conn = _get_conn()
    if not conn:
        return {}
    try:
        cur = conn.cursor()
        cur.execute("SELECT cik, category FROM whales WHERE name = %s", (whale_name,))
        row = cur.fetchone()
        cur.close()
        if row:
            return {"cik": row[0], "category": row[1] or "institution"}
        return {}
    except Exception:
        return {}
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
    """Seed → discover famous investors → loop through every whale → fetch 13F → returns → theme."""
    global _refresh_in_progress
    if _refresh_in_progress:
        return {"status": "already_running"}
    _refresh_in_progress = True

    results = []
    try:
        await seed_whales()
        try:
            await discover_famous_investors_via_perplexity()
        except Exception as e:
            logger.error("[WHALE] Famous investor discovery error: %s", e)
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
               OR (return_1m IS NULL AND return_3m IS NULL)
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


@router.get("/whales/{whale_name}/new-buys")
async def get_whale_new_buys(whale_name: str):
    """
    Returns all NEW and ADDED transactions for the whale from its most recent quarter,
    ordered by value_usd descending. Useful for highlighting fresh positions.
    """
    loop = asyncio.get_event_loop()

    def _fetch():
        conn = _get_conn()
        if not conn:
            return []
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT ticker, company_name, transaction_type, shares, value_usd, quarter, filed_date
                FROM whale_transactions
                WHERE whale_name = %s
                  AND transaction_type IN ('NEW', 'ADDED')
                  AND quarter = (
                      SELECT quarter FROM whale_transactions WHERE whale_name = %s
                      ORDER BY quarter DESC LIMIT 1
                  )
                ORDER BY value_usd DESC NULLS LAST
            """, (whale_name, whale_name))
            cols = ["ticker", "company_name", "transaction_type", "shares", "value_usd", "quarter", "filed_date"]
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
            logger.error("[WHALE_API] GET new-buys error for %s: %s", whale_name, e)
            return []
        finally:
            _put_conn(conn)

    transactions = await loop.run_in_executor(_executor, _fetch)
    return {"whale_name": whale_name, "transactions": transactions, "count": len(transactions)}


@router.post("/whales/discover")
async def trigger_discover_whales():
    """
    Immediately run Perplexity discovery, upsert the results into the DB,
    and return the list of whales found with their names and estimated returns.
    Falls back to DEFAULT_WHALES if discovery fails.
    This lets the frontend trigger a fresh discovery without waiting 24 hours.
    """
    discovered = await discover_top_whales_via_perplexity()
    whales_to_seed = discovered if discovered else DEFAULT_WHALES
    source = "perplexity" if discovered else "default_fallback"

    conn = _get_conn()
    if conn:
        try:
            cur = conn.cursor()
            for w in whales_to_seed:
                cur.execute("""
                    INSERT INTO whales (name, category, cik, description)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (name) DO UPDATE SET
                        cik         = EXCLUDED.cik,
                        description = EXCLUDED.description,
                        category    = EXCLUDED.category
                """, (w["name"], w["category"], w["cik"], w.get("description", "")))
            conn.commit()
            cur.close()
            logger.info("[WHALE] /discover upserted %d whales (%s)", len(whales_to_seed), source)
        except Exception as e:
            logger.error("[WHALE] /discover DB upsert error: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            _put_conn(conn)

    return {
        "status": "ok",
        "source": source,
        "discovered_count": len(whales_to_seed),
        "whales": [
            {
                "name":                 w["name"],
                "cik":                  w.get("cik", ""),
                "category":             w.get("category", "institution"),
                "estimated_return_pct": w.get("estimated_return_pct"),
                "description":          w.get("description", ""),
            }
            for w in whales_to_seed
        ],
    }


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
