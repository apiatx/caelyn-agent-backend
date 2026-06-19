"""
Defiance 2X Long Single-Stock ETF catalog service.

Source of truth: https://www.defianceetfs.com/wp-json/defiance/v1/etfs-explore
  - Returns all Defiance ETFs with a `category` field.
  - We filter `category == "leveraged-long"` then parse the underlying ticker
    from the ETF name using a regex on the "2X Long {TICKER} ETF" pattern.
  - Broad-basket names (Copper Miners, Drone, Space, Quantum) naturally fail
    the regex and are excluded without special-casing.

Refresh cadence: daily (off-hours, via asyncio loop in main.py).
Storage: disk-backed LKG JSON at data/defiance_2x_lkg.json (atomic write).
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx

_API_URL = "https://www.defianceetfs.com/wp-json/defiance/v1/etfs-explore"
_LKG_PATH = Path(__file__).parent.parent / "data" / "defiance_2x_lkg.json"
_REFRESH_INTERVAL_H = 20

_CATALOG: list[dict] = []
_LAST_REFRESH_TS: float = 0.0
_REFRESH_LOCK: Optional[asyncio.Lock] = None

_EXCLUDE_RE = re.compile(
    r"\b(short|inverse|income|option\s+income|covered\s+call|thematic)\b",
    re.IGNORECASE,
)
_BROAD_BASKET_RE = re.compile(
    r"\b(copper\s+miners?|drone|quantum|space|sector|index|basket|"
    r"commodity|commodities|blockchain|gold\s+miners?|silver\s+miners?|"
    r"pure\s+drone|pure\s+quantum|aerial\s+automation)\b",
    re.IGNORECASE,
)
_UNDERLYING_RE = re.compile(
    r"2[Xx]\s+Long\s+([A-Z][A-Z0-9]{0,5})\s+ETF\b"
)


def _get_lock() -> asyncio.Lock:
    global _REFRESH_LOCK
    if _REFRESH_LOCK is None:
        _REFRESH_LOCK = asyncio.Lock()
    return _REFRESH_LOCK


def _parse_underlying(name: str) -> str | None:
    m = _UNDERLYING_RE.search(name)
    return m.group(1).upper() if m else None


def _is_single_stock_2x_long(etf: dict) -> bool:
    if etf.get("category") != "leveraged-long":
        return False
    name = etf.get("name", "")
    if _EXCLUDE_RE.search(name):
        return False
    if _BROAD_BASKET_RE.search(name):
        return False
    return _parse_underlying(name) is not None


def _build_row(etf: dict, existing_rows: dict[str, dict]) -> dict:
    name = etf.get("name", "")
    ticker = etf["ticker"]
    underlying = _parse_underlying(name) or ""
    now_iso = datetime.now(timezone.utc).isoformat()
    prev = existing_rows.get(ticker, {})
    return {
        "defiance_etf_ticker": ticker,
        "defiance_etf_name":   name,
        "underlying_symbol":   underlying,
        "direction":           "long",
        "leverage":            2,
        "source_url":          etf.get("url", ""),
        "discovered_at":       prev.get("discovered_at", now_iso),
        "last_seen_at":        now_iso,
        "active":              True,
        "confidence":          "high",
        "parse_method":        "defiance_wp_api",
    }


def load_catalog_lkg() -> None:
    """Load catalog from disk LKG on startup. Safe to call synchronously."""
    global _CATALOG, _LAST_REFRESH_TS
    try:
        if not _LKG_PATH.exists():
            print("[DEFIANCE_2X] No disk LKG — catalog empty until first refresh")
            return
        payload = json.loads(_LKG_PATH.read_text())
        rows = payload.get("catalog", [])
        ts = payload.get("refreshed_at_ts", 0.0)
        _CATALOG = rows
        _LAST_REFRESH_TS = float(ts)
        age_h = (time.time() - _LAST_REFRESH_TS) / 3600
        print(f"[DEFIANCE_2X] Disk LKG loaded: {len(rows)} ETFs, age={age_h:.1f}h")
    except Exception as e:
        print(f"[DEFIANCE_2X] LKG load error (non-fatal): {e}")


def get_catalog() -> list[dict]:
    """Return current in-memory catalog rows."""
    return list(_CATALOG)


def get_last_refresh_ts() -> float:
    return _LAST_REFRESH_TS


async def refresh_catalog(force: bool = False) -> dict:
    """
    Fetch the Defiance WP REST API, filter single-stock 2X long ETFs,
    update in-memory catalog, and persist to disk LKG.

    Returns a status dict.
    Skips when fresh (< 20h) unless force=True.
    """
    global _CATALOG, _LAST_REFRESH_TS

    age_h = (time.time() - _LAST_REFRESH_TS) / 3600
    if not force and age_h < _REFRESH_INTERVAL_H:
        print(f"[DEFIANCE_2X] Catalog fresh ({age_h:.1f}h) — skipping refresh")
        return {"skipped": True, "reason": "fresh", "age_h": round(age_h, 2)}

    lock = _get_lock()
    if lock.locked() and not force:
        print("[DEFIANCE_2X] Refresh already running — skipping")
        return {"skipped": True, "reason": "locked"}

    async with lock:
        try:
            print("[DEFIANCE_2X] Fetching catalog from Defiance WP API…")
            async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
                resp = await client.get(
                    _API_URL,
                    headers={
                        "Accept": "application/json",
                        "User-Agent": (
                            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/124.0.0.0 Safari/537.36"
                        ),
                    },
                )
            resp.raise_for_status()
            raw: list[dict] = resp.json()
        except Exception as e:
            print(f"[DEFIANCE_2X] API fetch error: {e} — retaining existing catalog")
            return {"error": str(e)}

        existing: dict[str, dict] = {r["defiance_etf_ticker"]: r for r in _CATALOG}
        seen_underlyings: set[str] = set()
        catalog: list[dict] = []

        for etf in raw:
            if not _is_single_stock_2x_long(etf):
                continue
            row = _build_row(etf, existing)
            underlying = row["underlying_symbol"]
            if underlying in seen_underlyings:
                continue
            seen_underlyings.add(underlying)
            catalog.append(row)

        if not catalog:
            print("[DEFIANCE_2X] No valid ETFs parsed — retaining existing catalog")
            return {"error": "empty_parse"}

        now_ts = time.time()
        now_iso = datetime.now(timezone.utc).isoformat()

        _CATALOG = catalog
        _LAST_REFRESH_TS = now_ts

        payload = {
            "refreshed_at":    now_iso,
            "refreshed_at_ts": now_ts,
            "count":           len(catalog),
            "catalog":         catalog,
        }
        tmp = _LKG_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        tmp.replace(_LKG_PATH)

        print(f"[DEFIANCE_2X] Catalog refreshed: {len(catalog)} single-stock 2X long ETFs")
        return {"ok": True, "count": len(catalog), "refreshed_at": now_iso}
