"""
Stock Compare router — /api/fundamentals/compare

GET  /api/fundamentals/compare/search   ticker autocomplete
POST /api/fundamentals/compare          multi-ticker metric comparison

Validation is done inside the endpoint functions (not via Pydantic field_validators)
to avoid the Pydantic v2 ctx.error serialisation issue that crashes the existing
app-level validation_exception_handler.
"""
from __future__ import annotations

import re
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from services.stock_compare_service import (
    compare_metrics,
    search_symbols,
    VALID_METRICS,
    VALID_PERIODS,
    VALID_RANGES,
    METRIC_ALIASES,
    METRIC_MAP,
)

try:
    from config import FMP_API_KEY
except ImportError:
    import os
    FMP_API_KEY = os.getenv("FMP_API_KEY", "")

router = APIRouter(
    prefix="/api/fundamentals/compare",
    tags=["stock_compare"],
)

_SYMBOL_RE = re.compile(r"^[A-Z0-9.\-^]{1,12}$")


# ── Request model (bare — validation happens in endpoint) ─────────────────────

class CompareRequest(BaseModel):
    symbols: List[str]
    metric:  str
    period:  str = "annual"
    range:   str = "5Y"


# ── Validation helpers ─────────────────────────────────────────────────────────

def _validate_compare(body: CompareRequest) -> tuple[list[str], str]:
    """Validate and normalise CompareRequest. Raises HTTPException(400) on error."""
    symbols = body.symbols
    if not symbols:
        raise HTTPException(status_code=400, detail="At least one symbol is required")
    if len(symbols) > 15:
        raise HTTPException(status_code=400, detail="Maximum 15 symbols allowed")

    cleaned: list[str] = []
    for s in symbols:
        sym = s.upper().strip()
        if not _SYMBOL_RE.match(sym):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid symbol format: {s!r}. Allowed: A-Z 0-9 . - ^ (max 12 chars)",
            )
        if sym not in cleaned:
            cleaned.append(sym)

    metric = body.metric.lower().strip()
    resolved = METRIC_ALIASES.get(metric, metric)
    if resolved not in METRIC_MAP:
        valid = sorted(VALID_METRICS)
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported metric: {body.metric!r}. Valid values: {valid}",
        )

    period = body.period.lower().strip()
    if period not in VALID_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"period must be one of {sorted(VALID_PERIODS)}",
        )

    range_val = body.range.upper().strip()
    if range_val not in VALID_RANGES:
        raise HTTPException(
            status_code=400,
            detail=f"range must be one of {sorted(VALID_RANGES)}",
        )

    return cleaned, metric


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("/search")
async def search_endpoint(
    q:     str = Query(..., min_length=1, max_length=100, description="Ticker or company name"),
    limit: int = Query(10, ge=1, le=50, description="Max results to return"),
):
    """
    Autocomplete / ticker search.

    Returns symbol, name, exchange, type, currency, sector, industry, marketCap.
    Results are cached for 24 hours.
    """
    if not FMP_API_KEY:
        raise HTTPException(status_code=503, detail="FMP API key not configured")

    try:
        return await search_symbols(q.strip(), limit, FMP_API_KEY)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {e}") from e


@router.post("")
async def compare_endpoint(body: CompareRequest):
    """
    Multi-ticker financial metric comparison.

    Returns chart-ready time series, snapshot table, news, and metadata.
    Partial data is returned on individual symbol failures — check meta.warnings
    and meta.invalidSymbols for issues.
    """
    if not FMP_API_KEY:
        raise HTTPException(status_code=503, detail="FMP API key not configured")

    symbols, metric = _validate_compare(body)

    period    = body.period.lower().strip()
    range_val = body.range.upper().strip()

    try:
        result = await compare_metrics(
            symbols=symbols,
            metric=metric,
            period=period,
            range_val=range_val,
            fmp_key=FMP_API_KEY,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Compare failed: {e}") from e

    # If every symbol came back invalid, surface that as 400
    if result["meta"]["invalidSymbols"] and not result["meta"]["validSymbols"]:
        raise HTTPException(
            status_code=400,
            detail={
                "error":          "All symbols are invalid or have no FMP data",
                "invalidSymbols": result["meta"]["invalidSymbols"],
            },
        )

    return result
