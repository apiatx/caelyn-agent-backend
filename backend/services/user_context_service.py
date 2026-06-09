"""
User Context Service — compact, privacy-safe user-specific context for /api/query injection.

Public API:
    get_portfolio_slice(user_id) -> dict | None
    get_watchlist_slice(user_id)  -> dict | None

Privacy rules (enforced here, not upstream):
  - NEVER expose shares, avg_cost, cost_basis, or market value.
  - Expose only: ticker symbols, counts, and asset-class groupings.

Watchlist output format (chunked to stay within data_compressor MAX_STRING_LENGTH=200):
  {
    "user_watchlist_count": 302,
    "user_watchlist_tickers_1": "AAOI, NVDA, MU, ...",   # ~30 tickers per chunk
    "user_watchlist_tickers_2": "WULF, IREN, ...",
    ...
  }

Portfolio output format:
  {
    "user_portfolio_count": 12,
    "user_portfolio_tickers": "NVDA, AMD, TSLA",
    "user_portfolio_exposure": "NVDA, AMD (stocks); BTC (crypto); GLD (ETF)"
  }

Failure contract:
  - All errors are caught internally and return None.
  - No exception from this module ever propagates to the caller.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Optional

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"

_ASSET_LABEL: dict[str, str] = {
    "stock":          "stocks",
    "stocks":         "stocks",
    "equity":         "stocks",
    "equities":       "stocks",
    "crypto":         "crypto",
    "cryptocurrency": "crypto",
    "etf":            "ETF",
    "fund":           "ETF",
    "commodity":      "commodity",
    "commodities":    "commodity",
    "index":          "index",
    "bond":           "bonds",
    "bonds":          "bonds",
    "option":         "options",
    "options":        "options",
}

# Number of tickers to pack into each chunked key.
# 30 tickers × ~5 chars each ≈ 150 chars → safely under data_compressor MAX_STRING_LENGTH=200.
_TICKERS_PER_CHUNK = 30


def get_portfolio_slice(user_id: str = "default") -> Optional[dict]:
    """
    Read the user's portfolio holdings file and return a compact exposure dict.

    Returns:
        {
          "user_portfolio_count": 12,
          "user_portfolio_tickers": "NVDA, AMD, TSLA",
          "user_portfolio_exposure": "NVDA, AMD (stocks); BTC (crypto); GLD (ETF)"
        }
        or None if no holdings or any error.

    Privacy: shares, avg_cost, and cost_basis are never included.
    """
    try:
        portfolio_file = _DATA_DIR / f"portfolio_holdings_{user_id}.json"
        if not portfolio_file.exists():
            portfolio_file = _DATA_DIR / "portfolio_holdings.json"
        if not portfolio_file.exists():
            return None

        raw = json.loads(portfolio_file.read_text())
        holdings = raw.get("holdings", [])
        if not isinstance(holdings, list) or not holdings:
            return None

        groups: dict[str, list[str]] = {}
        all_tickers: list[str] = []
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
            all_tickers.append(ticker)

        if not groups:
            return None

        parts = [
            f"{', '.join(tickers)} ({label})"
            for label, tickers in groups.items()
        ]
        exposure_str = "; ".join(parts)[:180]
        flat_tickers  = ", ".join(all_tickers)[:180]

        return {
            "user_portfolio_count":    len(all_tickers),
            "user_portfolio_tickers":  flat_tickers,
            "user_portfolio_exposure": exposure_str,
        }

    except Exception as e:
        print(f"[USER_CONTEXT] get_portfolio_slice error (non-fatal): {e}")
        return None


def get_watchlist_slice(user_id: str = "default") -> Optional[dict]:
    """
    Return the active watchlist as chunked ticker context — no truncation at 20.

    Returns a dict with:
      user_watchlist_count          — total ticker count
      user_watchlist_tickers_1      — first  ~30 tickers, comma-separated
      user_watchlist_tickers_2      — next   ~30 tickers  (if needed)
      ...
      user_watchlist_tickers_N      — remaining tickers

    Each chunk is ≤ ~180 chars, within data_compressor MAX_STRING_LENGTH=200.
    Privacy: only ticker symbols — no quantities, scores, or cost data.
    """
    try:
        from services.watchlist_service import load_watchlist
        wl = load_watchlist()
        if not wl:
            return None
        raw_tickers = wl.get("tickers") or []
        if not isinstance(raw_tickers, list) or not raw_tickers:
            return None

        # Normalise — each element may be a str or a dict with a "ticker" key
        symbols: list[str] = []
        for entry in raw_tickers:
            if isinstance(entry, str):
                sym = entry.upper().strip()
            elif isinstance(entry, dict):
                sym = (entry.get("ticker") or entry.get("symbol") or "").upper().strip()
            else:
                sym = ""
            if sym:
                symbols.append(sym)

        if not symbols:
            return None

        result: dict = {"user_watchlist_count": len(symbols)}

        # Chunk into groups of _TICKERS_PER_CHUNK
        n_chunks = math.ceil(len(symbols) / _TICKERS_PER_CHUNK)
        for i in range(n_chunks):
            chunk = symbols[i * _TICKERS_PER_CHUNK : (i + 1) * _TICKERS_PER_CHUNK]
            result[f"user_watchlist_tickers_{i + 1}"] = ", ".join(chunk)

        return result

    except Exception as e:
        print(f"[USER_CONTEXT] get_watchlist_slice error (non-fatal): {e}")
        return None
