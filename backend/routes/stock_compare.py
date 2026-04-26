"""
Stock Compare router — /api/fundamentals/compare

GET  /api/fundamentals/compare/search       ticker autocomplete
GET  /api/fundamentals/compare/metrics      canonical metric registry
GET  /api/fundamentals/compare/diagnostics  per-symbol data availability
POST /api/fundamentals/compare              multi-ticker metric comparison

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
    get_metric_definitions,
    get_diagnostics,
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


# ── Request model ─────────────────────────────────────────────────────────────

class CompareRequest(BaseModel):
    symbols: List[str]
    metric:  str
    period:  str = "annual"
    range:   str = "5Y"


# ── Validation helpers ────────────────────────────────────────────────────────

def _resolve_metric(raw: str) -> str:
    """Normalise metric string: lowercase, then alias-resolve."""
    m = raw.lower().strip()
    # Try lowercase alias first
    resolved = METRIC_ALIASES.get(m, m)
    if resolved in METRIC_MAP:
        return resolved
    # Try original case alias (camelCase aliases like marketCap)
    resolved = METRIC_ALIASES.get(raw.strip(), raw.strip().lower())
    if resolved in METRIC_MAP:
        return resolved
    return m


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

    metric   = body.metric.strip()
    resolved = _resolve_metric(metric)
    if resolved not in METRIC_MAP:
        valid = sorted(set(METRIC_MAP.keys()) | set(METRIC_ALIASES.keys()))
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

    return cleaned, resolved


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/search")
async def search_endpoint(
    q:     str = Query(..., min_length=1, max_length=100, description="Ticker or company name"),
    limit: int = Query(10, ge=1, le=50, description="Max results to return"),
):
    """
    Autocomplete / ticker search.
    Returns symbol, name, exchange, type, currency, sector, industry, marketCap.
    """
    if not FMP_API_KEY:
        raise HTTPException(status_code=503, detail="FMP API key not configured")
    try:
        return await search_symbols(q.strip(), limit, FMP_API_KEY)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {e}") from e


@router.get("/metrics")
async def metrics_endpoint():
    """
    Return the canonical metric registry.

    Lists all 24 supported numeric chart/screener metrics with label, unit,
    source endpoint, and fallback description.  Also lists non-chart sections
    (e.g. recent_news) separately so the frontend can handle them differently.
    """
    return get_metric_definitions()


@router.get("/diagnostics")
async def diagnostics_endpoint(
    symbols: str = Query(..., description="Comma-separated list of ticker symbols"),
    period:  str = Query("annual", description="annual or quarterly"),
):
    """
    Per-symbol data availability diagnostics.

    Returns, for each symbol:
      - availableMetrics / missingMetrics
      - per-endpoint status (ok / error / empty), row count, cache hit
      - dataQuality: complete | mostly_complete | partial | sparse | error

    This endpoint is for debugging/admin visibility and never throws 500.
    """
    if not FMP_API_KEY:
        raise HTTPException(status_code=503, detail="FMP API key not configured")

    period = period.lower().strip()
    if period not in VALID_PERIODS:
        raise HTTPException(status_code=400, detail=f"period must be one of {sorted(VALID_PERIODS)}")

    raw_syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not raw_syms:
        raise HTTPException(status_code=400, detail="At least one symbol is required")

    cleaned: list[str] = []
    for sym in raw_syms[:10]:
        if _SYMBOL_RE.match(sym):
            cleaned.append(sym)

    if not cleaned:
        raise HTTPException(status_code=400, detail="No valid symbols provided")

    try:
        return await get_diagnostics(cleaned, period, FMP_API_KEY)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Diagnostics failed: {e}") from e


@router.post("")
async def compare_endpoint(body: CompareRequest):
    """
    Multi-ticker financial metric comparison.

    Returns chart-ready time series, screener table (all 24 metrics),
    snapshot table (backward compat), news, metric availability, and metadata.

    Partial data is returned on individual symbol failures — check
    meta.warnings and missingSymbols for issues.  Never returns 500 for
    individual symbol data problems.
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

    if result["meta"]["invalidSymbols"] and not result["meta"]["validSymbols"]:
        raise HTTPException(
            status_code=400,
            detail={
                "error":          "All symbols are invalid or have no FMP data",
                "invalidSymbols": result["meta"]["invalidSymbols"],
            },
        )

    return result
