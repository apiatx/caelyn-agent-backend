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
    Return a compact, privacy-safe portfolio exposure dict for agent context.

    Primary source: portfolio_store.load_active_holdings() (Neon-backed, same
    canonical store as Terminal/Fundamentals/Dashboard).  Falls back to the local
    holdings file for offline/dev environments where Neon is unavailable.

    Also merges open option underlying symbols so the agent is aware of the full
    portfolio — equity positions AND option underlyings.  OCC contract IDs are
    never included; only the underlying stock tickers.

    Returns:
        {
          "user_portfolio_count": 18,
          "user_portfolio_tickers": "NVDA, AMD, TSLA, CRWV, ...",
          "user_portfolio_exposure": "NVDA, AMD (stocks); CRWV (options)"
        }
        or None if no holdings or any error.

    Privacy: shares, avg_cost, cost_basis are never included.
    """
    try:
        holdings: list[dict] = []

        # 1. Canonical Neon-backed store (same source as Terminal / Fundamentals)
        try:
            from data.portfolio_store import load_active_holdings as _canon_lah
            _neon = _canon_lah()
            if _neon:
                holdings = _neon
        except Exception as _neon_err:
            print(f"[USER_CONTEXT] Neon load error (falling back to file): {_neon_err}")

        # 2. File fallback for local dev / Neon-unavailable environments
        if not holdings:
            portfolio_file = _DATA_DIR / f"portfolio_holdings_{user_id}.json"
            if not portfolio_file.exists():
                portfolio_file = _DATA_DIR / "portfolio_holdings.json"
            if portfolio_file.exists():
                try:
                    raw = json.loads(portfolio_file.read_text())
                    file_h = raw.get("holdings", []) if isinstance(raw, dict) else raw
                    if isinstance(file_h, list):
                        holdings = file_h
                except Exception:
                    pass

        if not isinstance(holdings, list) or not holdings:
            return None

        groups: dict[str, list[str]] = {}
        all_tickers: list[str] = []
        for h in holdings:
            if not isinstance(h, dict):
                continue
            ticker = (h.get("ticker") or h.get("symbol") or "").upper().strip()
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

        # Include open option underlyings so agent context reflects full portfolio
        try:
            from data.option_trades_store import load_open_option_underlyings as _opt_unds
            _existing = set(all_tickers)
            for _opt_sym in sorted(_opt_unds()):
                if _opt_sym and _opt_sym not in _existing:
                    groups.setdefault("options", []).append(_opt_sym)
                    all_tickers.append(_opt_sym)
                    _existing.add(_opt_sym)
        except Exception as _opt_e:
            print(f"[USER_CONTEXT] option underlyings load error (non-fatal): {_opt_e}")

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
