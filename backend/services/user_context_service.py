"""
User Context Service — compact, privacy-safe user-specific context for /api/query injection.

Phase 3 scope: portfolio exposure only.
Watchlist injection is deferred (ambiguous active-watchlist identity, no user_id on schema).

Public API:
    get_portfolio_slice(user_id) -> dict | None

Privacy rules (enforced here, not upstream):
  - NEVER expose shares, avg_cost, cost_basis, or market value.
  - Expose only: ticker symbols and asset-class groupings.

Output format:
  {"user_portfolio_exposure": "NVDA, AMD (stocks); BTC (crypto); GLD (ETF)"}

  All values ≤ 180 chars — within data_compressor.MAX_STRING_LENGTH (200).

Failure contract:
  - All errors are caught internally and return None.
  - No exception from this module ever propagates to the caller.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"

_ASSET_LABEL: dict[str, str] = {
    "stock":       "stocks",
    "stocks":      "stocks",
    "equity":      "stocks",
    "equities":    "stocks",
    "crypto":      "crypto",
    "cryptocurrency": "crypto",
    "etf":         "ETF",
    "fund":        "ETF",
    "commodity":   "commodity",
    "commodities": "commodity",
    "index":       "index",
    "bond":        "bonds",
    "bonds":       "bonds",
    "option":      "options",
    "options":     "options",
}


def get_portfolio_slice(user_id: str = "default") -> Optional[dict]:
    """
    Read the user's portfolio holdings file and return a compact exposure dict.

    Returns:
        {"user_portfolio_exposure": "NVDA, AMD (stocks); BTC (crypto); GLD (ETF)"}
        or None if no holdings or any error.

    Privacy: shares, avg_cost, and cost_basis are never included.
    """
    try:
        portfolio_file = _DATA_DIR / f"portfolio_holdings_{user_id}.json"
        if not portfolio_file.exists():
            # Fall back to the legacy single-user file (pre-migration state)
            portfolio_file = _DATA_DIR / "portfolio_holdings.json"
        if not portfolio_file.exists():
            return None

        raw = json.loads(portfolio_file.read_text())
        holdings = raw.get("holdings", [])
        if not isinstance(holdings, list) or not holdings:
            return None

        groups: dict[str, list[str]] = {}
        for h in holdings:
            if not isinstance(h, dict):
                continue
            ticker = (h.get("ticker") or "").upper().strip()
            if not ticker:
                continue
            raw_type = (
                h.get("asset_type") or h.get("type") or "stock"
            ).lower().strip()
            label = _ASSET_LABEL.get(raw_type, raw_type)
            groups.setdefault(label, []).append(ticker)

        if not groups:
            return None

        parts = [
            f"{', '.join(tickers)} ({label})"
            for label, tickers in groups.items()
        ]
        value = "; ".join(parts)[:180]
        return {"user_portfolio_exposure": value}

    except Exception as e:
        print(f"[USER_CONTEXT] get_portfolio_slice error (non-fatal): {e}")
        return None


def get_watchlist_slice(user_id: str = "default") -> Optional[dict]:
    """
    Return the active watchlist as a compact ambient context string.

    Returns:
        {"user_watchlist_tickers": "NVDA, AMD, TSLA, AAPL, BTC"}
        or None if no watchlist or any error.

    Privacy: only ticker symbols are returned — no quantities or cost data.
    """
    try:
        from services.watchlist_service import load_watchlist
        wl = load_watchlist()
        if not wl:
            return None
        tickers = wl.get("tickers") or []
        if not isinstance(tickers, list) or not tickers:
            return None
        # Normalise — each element may be a str or a dict with a "ticker" key
        symbols: list[str] = []
        for entry in tickers:
            if isinstance(entry, str):
                sym = entry.upper().strip()
            elif isinstance(entry, dict):
                sym = (entry.get("ticker") or entry.get("symbol") or "").upper().strip()
            else:
                sym = ""
            if sym:
                symbols.append(sym)
        symbols = symbols[:20]
        if not symbols:
            return None
        return {"user_watchlist_tickers": ", ".join(symbols)}
    except Exception as e:
        print(f"[USER_CONTEXT] get_watchlist_slice error (non-fatal): {e}")
        return None
