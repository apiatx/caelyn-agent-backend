"""
Defiance 2X Long Single-Stock ETF catalog service.

Source of truth: https://www.defianceetfs.com/wp-json/defiance/v1/etfs-explore
  - The API returns all Defiance ETFs; `ticker` is the *Defiance ETF ticker*
    (e.g. AMA, AMKL), NOT the underlying stock.
  - `category == "leveraged-long"` selects the leveraged-long bucket.
  - The underlying ticker is parsed from the ETF name via regex:
      "Defiance Daily Target 2X Long AMAT ETF" → underlying = AMAT
  - Broad-basket names (Copper Miners, Drone, Space, Quantum) fail the
    regex naturally and are quarantined without special-casing.
  - Validation rule: underlying_symbol must be non-empty AND must differ
    from the Defiance ETF ticker. Any row failing this is quarantined.

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

# In-memory state
_CATALOG: list[dict] = []           # valid single-stock 2X long ETFs
_QUARANTINED: list[dict] = []       # leveraged-long ETFs that could not be mapped
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
# Matches "2X Long TICKER ETF" — TICKER must start with a letter, 1-6 chars
_UNDERLYING_RE = re.compile(
    r"2[Xx]\s+Long\s+([A-Z][A-Z0-9]{0,5})\s+ETF\b"
)


def _get_lock() -> asyncio.Lock:
    global _REFRESH_LOCK
    if _REFRESH_LOCK is None:
        _REFRESH_LOCK = asyncio.Lock()
    return _REFRESH_LOCK


def _parse_underlying(name: str) -> str | None:
    """
    Extract underlying stock ticker from ETF name.
      "Defiance Daily Target 2X Long AMAT ETF" → "AMAT"
      "Defiance Daily 2X Space ETF"            → None (broad basket)
    """
    m = _UNDERLYING_RE.search(name)
    return m.group(1).upper() if m else None


def _classify(etf: dict) -> tuple[str, str | None]:
    """
    Returns (verdict, underlying):
      verdict: "valid" | "not_leveraged_long" | "excluded" | "broad_basket" | "no_underlying" | "same_as_etf"
    """
    if etf.get("category") != "leveraged-long":
        return "not_leveraged_long", None

    name = etf.get("name", "")

    if _EXCLUDE_RE.search(name):
        return "excluded", None

    if _BROAD_BASKET_RE.search(name):
        return "broad_basket", None

    underlying = _parse_underlying(name)
    if not underlying:
        return "no_underlying", None

    # Critical: the Defiance ETF ticker must differ from the underlying ticker
    etf_ticker = (etf.get("ticker") or "").upper()
    if underlying == etf_ticker:
        return "same_as_etf", underlying

    return "valid", underlying


def _build_row(etf: dict, underlying: str, existing_rows: dict[str, dict]) -> dict:
    ticker = etf["ticker"]
    now_iso = datetime.now(timezone.utc).isoformat()
    prev = existing_rows.get(ticker, {})
    return {
        "defiance_etf_ticker": ticker,
        "defiance_etf_name":   etf.get("name", ""),
        "underlying_symbol":   underlying,
        "direction":           "long",
        "leverage":            2,
        "source_url":          etf.get("url", ""),
        "discovered_at":       prev.get("discovered_at", now_iso),
        "last_seen_at":        now_iso,
        "active":              True,
        "confidence":          "high",
        "parse_method":        "defiance_wp_api_name_regex",
    }


def _build_quarantine_row(etf: dict, verdict: str) -> dict:
    return {
        "defiance_etf_ticker": etf.get("ticker", ""),
        "defiance_etf_name":   etf.get("name", ""),
        "verdict":             verdict,
        "quarantined_at":      datetime.now(timezone.utc).isoformat(),
    }


def load_catalog_lkg() -> None:
    """Load catalog from disk LKG on startup. Safe to call synchronously."""
    global _CATALOG, _QUARANTINED, _LAST_REFRESH_TS
    try:
        if not _LKG_PATH.exists():
            print("[DEFIANCE_2X] No disk LKG — catalog empty until first refresh")
            return
        payload = json.loads(_LKG_PATH.read_text())
        rows = payload.get("catalog", [])
        quarantined = payload.get("quarantined", [])
        ts = payload.get("refreshed_at_ts", 0.0)
        _CATALOG = rows
        _QUARANTINED = quarantined
        _LAST_REFRESH_TS = float(ts)
        age_h = (time.time() - _LAST_REFRESH_TS) / 3600
        print(
            f"[DEFIANCE_2X] Disk LKG loaded: {len(rows)} valid ETFs, "
            f"{len(quarantined)} quarantined, age={age_h:.1f}h"
        )
    except Exception as e:
        print(f"[DEFIANCE_2X] LKG load error (non-fatal): {e}")


def get_catalog() -> list[dict]:
    """Return current in-memory catalog of valid single-stock 2X long ETFs."""
    return list(_CATALOG)


def get_quarantined() -> list[dict]:
    """Return ETFs that were excluded due to mapping failure."""
    return list(_QUARANTINED)


def get_last_refresh_ts() -> float:
    return _LAST_REFRESH_TS


async def refresh_catalog(force: bool = False) -> dict:
    """
    Fetch the Defiance WP REST API, classify each ETF, persist results.

    Validation per row:
      - category == "leveraged-long"
      - no exclusion keywords (short, inverse, income, etc.)
      - underlying_symbol parseable from name via regex
      - underlying_symbol != defiance_etf_ticker   ← critical
    Any ETF failing these is quarantined and logged.

    Returns a status dict.
    Skips when fresh (< 20h) unless force=True.
    """
    global _CATALOG, _QUARANTINED, _LAST_REFRESH_TS

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
        quarantined: list[dict] = []
        verdict_counts: dict[str, int] = {}

        for etf in raw:
            verdict, underlying = _classify(etf)
            verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1

            if verdict != "valid":
                if verdict != "not_leveraged_long":
                    # Quarantine leveraged-long ETFs that couldn't be mapped
                    q = _build_quarantine_row(etf, verdict)
                    quarantined.append(q)
                    print(
                        f"[DEFIANCE_2X] quarantined  etf={etf.get('ticker')}  "
                        f"name={etf.get('name')!r}  verdict={verdict}"
                    )
                continue

            if underlying in seen_underlyings:
                continue
            seen_underlyings.add(underlying)
            catalog.append(_build_row(etf, underlying, existing))

        if not catalog:
            print("[DEFIANCE_2X] No valid ETFs parsed — retaining existing catalog")
            return {"error": "empty_parse"}

        now_ts = time.time()
        now_iso = datetime.now(timezone.utc).isoformat()
        _CATALOG = catalog
        _QUARANTINED = quarantined
        _LAST_REFRESH_TS = now_ts

        payload = {
            "refreshed_at":    now_iso,
            "refreshed_at_ts": now_ts,
            "count":           len(catalog),
            "quarantined_count": len(quarantined),
            "catalog":         catalog,
            "quarantined":     quarantined,
        }
        tmp = _LKG_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        tmp.replace(_LKG_PATH)

        print(
            f"[DEFIANCE_2X] Catalog refreshed: {len(catalog)} valid, "
            f"{len(quarantined)} quarantined | verdicts={verdict_counts}"
        )
        return {
            "ok":              True,
            "count":           len(catalog),
            "quarantined":     len(quarantined),
            "verdict_counts":  verdict_counts,
            "refreshed_at":    now_iso,
        }
