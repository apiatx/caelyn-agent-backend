"""
Pre-IPO Watchlist router — GET /api/calendar/pre-ipo-watchlist

Additive endpoint that aggregates pre-IPO intel for a fixed set of
high-profile private companies using Perplexity, Polymarket, and
Finnhub.  Independent of the existing FMP IPO calendar surface.

Always returns HTTP 200 with at minimum a safe-shape JSON payload so
the frontend never sees a hard failure even when external API keys are
missing or upstream sources are unavailable.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Header, Query, Request
from fastapi.responses import JSONResponse

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop

router = APIRouter(tags=["pre_ipo_watchlist"])

_AUTH_HEADER = "X-API-Key"

# Names kept in sync with services.pre_ipo_watchlist_service.TRACKED_COMPANIES
# so we can emit a safe-shape fallback even if the service module fails to
# import (e.g. transient dependency issue).
_FALLBACK_COMPANY_NAMES = (
    "SpaceX", "OpenAI", "Anthropic", "Databricks", "Anduril", "Stripe",
)


def _check_key(api_key: Optional[str]) -> Optional[JSONResponse]:
    """Return a 401 response if the API key is invalid, else None."""
    try:
        from config import AGENT_API_KEY
    except Exception:
        AGENT_API_KEY = None
    if AGENT_API_KEY and api_key != AGENT_API_KEY:
        return JSONResponse(status_code=401, content={"error": "Invalid or missing API key"})
    return None


def _safe_empty_company(name: str, rank: int) -> dict[str, Any]:
    return {
        "company":             name,
        "ipo_status":          "Unknown",
        "estimated_valuation": "Unknown",
        "valuation_notes":     [],
        "polymarket": {
            "ipo_probability_12m": None,
            "valuation_markets":   [],
            "summary":             "Data temporarily unavailable.",
        },
        "catalysts":           [],
        "expected_window":     {"earliest": "Unknown", "likely": "Unknown"},
        "confidence_score":    "Low",
        "latest_news":         [],
        "sources":             [],
        "opportunity_score":   0,
        "score_breakdown": {
            "ipo_probability_score":     0,
            "valuation_momentum_score":  0,
            "news_recency_score":        0,
            "source_quality_score":      0,
            "catalyst_strength_score":   0,
        },
        "change_tracking": {
            "valuation_change":       "Unknown",
            "ipo_probability_change": "Unknown",
            "new_catalysts":          [],
            "previous_score":         None,
            "score_change":           None,
            "last_snapshot_at":       None,
        },
        "momentum_badge":      "Dormant",
        "rank":                rank,
    }


def _safe_fallback_payload(reason: Optional[str] = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status":     "ok",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "companies": [
            _safe_empty_company(name, idx)
            for idx, name in enumerate(_FALLBACK_COMPANY_NAMES, start=1)
        ],
    }
    if reason:
        payload["fallback_reason"] = reason
    return payload


_CONFIDENCE_LABELS = {
    "live":    "Live intelligence",
    "cached":  "Using cached intelligence",
    "limited": "Limited data available",
}


def _derive_data_confidence(data: dict[str, Any]) -> dict[str, Any]:
    """
    Map the existing service-level status / metadata onto a machine-readable
    top-level signal for the frontend.

      - live    → fresh successful fetch with normal data
      - cached  → stale cached data served because refresh failed
      - limited → safe fallback / hard error / empty companies list

    Reuses existing fields on the payload (`status`, `stale_reason`,
    `fallback_reason`, `error`) — does not require service changes.
    """
    status_str = str(data.get("status") or "").lower()
    fallback_reason = data.get("fallback_reason")
    stale_reason = data.get("stale_reason")
    error_msg = data.get("error")
    companies = data.get("companies")
    has_companies = isinstance(companies, list) and len(companies) > 0

    if status_str == "stale":
        confidence = "cached"
        reason = stale_reason or "Serving cached intelligence; live refresh failed."
    elif status_str == "error" or fallback_reason or not has_companies:
        confidence = "limited"
        reason = (
            error_msg
            or fallback_reason
            or "Live sources unavailable; serving safe fallback data."
        )
    else:
        confidence = "live"
        reason = None

    block: dict[str, Any] = {
        "status": confidence,
        "label":  _CONFIDENCE_LABELS[confidence],
    }
    if reason:
        block["reason"] = str(reason)
    return block


@router.get("/api/calendar/pre-ipo-watchlist")
@traceable(name="pre_ipo_watchlist.endpoint")
async def pre_ipo_watchlist(
    request: Request,
    api_key: str = Header(None, alias=_AUTH_HEADER),
    refresh: bool = Query(
        default=False,
        description="Force-bypass the cache and refetch from upstream sources.",
    ),
):
    """
    Return pre-IPO watchlist data for a fixed set of high-profile private
    companies (SpaceX, OpenAI, Anthropic, Databricks, Anduril, Stripe).

    Data is cached for ~8 hours.  Stale data is returned if a refresh
    fails so callers always receive a usable response when at least one
    successful fetch has happened previously.

    The endpoint always responds with HTTP 200 and a safe-shape JSON
    body — even when the underlying service raises or external API
    keys are missing — so the frontend can render a usable empty state
    instead of a hard error.
    """
    err = _check_key(api_key)
    if err:
        return err

    try:
        from services.pre_ipo_watchlist_service import get_pre_ipo_watchlist
    except Exception as e:
        print(f"[pre_ipo_watchlist] service import failed: {e}")
        payload = _safe_fallback_payload(f"service import failed: {e}")
        payload["data_confidence"] = _derive_data_confidence(payload)
        return JSONResponse(content=payload)

    try:
        data = await get_pre_ipo_watchlist(refresh=refresh)
        if not isinstance(data, dict):
            payload = _safe_fallback_payload("upstream returned non-dict")
            payload["data_confidence"] = _derive_data_confidence(payload)
            return JSONResponse(content=payload)
        # Defensive: ensure required top-level shape exists.
        data.setdefault("status", "ok")
        data.setdefault("updated_at", datetime.now(timezone.utc).isoformat())
        if not isinstance(data.get("companies"), list):
            data["companies"] = []
        data["data_confidence"] = _derive_data_confidence(data)
        return JSONResponse(content=data)
    except Exception as e:
        print(f"[pre_ipo_watchlist] unhandled: {e}")
        payload = _safe_fallback_payload(f"unhandled: {e}")
        payload["data_confidence"] = _derive_data_confidence(payload)
        return JSONResponse(content=payload)
