"""
Real-time quotes router — /api/market/realtime-quotes

GET  /api/market/realtime-quotes?symbols=NVDA,TSLA,AAPL
POST /api/market/realtime-quotes  body: {"symbols": [...]}

Returns vendor-prioritized normalized quotes:
    Tradier → Public.com → FMP → Twelve Data → LKG
"""
from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from services.realtime_quotes_service import (
    get_realtime_quotes,
    RealtimeQuote,
    SOURCE_TRADIER,
    SOURCE_PUBLIC,
    SOURCE_FMP,
    SOURCE_TWELVE,
    SOURCE_LKG,
)


router = APIRouter(
    prefix="/api/market",
    tags=["realtime_quotes"],
)

_SYMBOL_RE = re.compile(r"^[A-Z0-9.\-^]{1,12}$")


def _validate_symbols(raw_symbols: list[str]) -> tuple[list[str], dict[str, str]]:
    """Returns (valid_symbols, per_symbol_errors)."""
    seen = set()
    valid: list[str] = []
    errors: dict[str, str] = {}
    for raw in raw_symbols:
        if not isinstance(raw, str):
            continue
        cleaned = raw.strip().upper()
        if not cleaned:
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        if not _SYMBOL_RE.match(cleaned):
            errors[cleaned] = "invalid_symbol"
            continue
        valid.append(cleaned)
    return valid, errors


def _build_response(
    requested: list[str],
    valid: list[str],
    invalid_errors: dict[str, str],
    quotes: dict[str, RealtimeQuote],
) -> dict:
    quote_map: dict[str, dict] = {}
    for sym in valid:
        q = quotes.get(sym)
        if q is None:
            quote_map[sym] = {
                "symbol": sym,
                "source": "none",
                "error": "not_returned",
                "is_realtime": False,
                "is_live_backup": False,
                "is_stale": True,
            }
        else:
            quote_map[sym] = q.to_dict()
    for sym, err in invalid_errors.items():
        quote_map[sym] = {
            "symbol": sym,
            "source": "none",
            "error": err,
            "is_realtime": False,
            "is_live_backup": False,
            "is_stale": True,
        }
    return {
        "status": "ok",
        "requested": len(requested),
        "returned": len(quote_map),
        "quotes": quote_map,
        "source_priority": [
            SOURCE_TRADIER,
            SOURCE_PUBLIC,
            SOURCE_FMP,
            SOURCE_TWELVE,
            SOURCE_LKG,
        ],
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cache_policy": {
            "regular_session_ttl_s": 15,
            "extended_session_ttl_s": 30,
            "closed_session_ttl_s": 300,
            "lkg_retention_s": 86400,
        },
    }


@router.get("/realtime-quotes")
async def realtime_quotes_get(
    symbols: str = Query(..., description="Comma-separated tickers"),
    scope: str = Query(
        "page",
        description=(
            "Requesting scope for demand-priority tracking: "
            "watchlist|portfolio|options_flow|screener|social|themes|sectors|strategy|popup|page"
        ),
    ),
):
    raw_symbols = [s for s in symbols.split(",") if s.strip()]
    if not raw_symbols:
        raise HTTPException(status_code=400, detail="symbols query param required")
    if len(raw_symbols) > 200:
        raise HTTPException(status_code=400, detail="too many symbols (max 200)")

    valid, errors = _validate_symbols(raw_symbols)

    # ── Phase 4A: register active demand before fetching ─────────────────────
    if valid:
        try:
            import data.quote_demand_registry as _qdr
            _ttl = 30 if scope == "popup" else 90
            _qdr.register(valid, scope, ttl=_ttl)
        except Exception:
            pass

    quotes: dict[str, RealtimeQuote] = {}
    if valid:
        try:
            quotes = await get_realtime_quotes(valid, allow_fallback=True)
        except Exception as e:
            print(f"[REALTIME route] error: {e}")
            # Never 500 because of vendor failure — return per-symbol errors
            quotes = {}
            for sym in valid:
                errors.setdefault(sym, f"service_error")
    return _build_response(raw_symbols, valid, errors, quotes)


class QuotesPostBody(BaseModel):
    symbols: List[str]
    allow_fallback: Optional[bool] = True
    scope: Optional[str] = "page"


@router.post("/realtime-quotes")
async def realtime_quotes_post(body: QuotesPostBody):
    raw = body.symbols or []
    if not raw:
        raise HTTPException(status_code=400, detail="symbols list required")
    if len(raw) > 200:
        raise HTTPException(status_code=400, detail="too many symbols (max 200)")

    valid, errors = _validate_symbols(raw)

    # ── Phase 4A: register active demand before fetching ─────────────────────
    if valid:
        try:
            import data.quote_demand_registry as _qdr
            _scope = body.scope or "page"
            _ttl   = 30 if _scope == "popup" else 90
            _qdr.register(valid, _scope, ttl=_ttl)
        except Exception:
            pass

    quotes: dict[str, RealtimeQuote] = {}
    if valid:
        try:
            quotes = await get_realtime_quotes(
                valid, allow_fallback=bool(body.allow_fallback)
            )
        except Exception as e:
            print(f"[REALTIME route] error: {e}")
            quotes = {}
            for sym in valid:
                errors.setdefault(sym, "service_error")
    return _build_response(raw, valid, errors, quotes)
