"""
Pre-IPO Watchlist router — GET /api/calendar/pre-ipo-watchlist

Additive endpoint that aggregates pre-IPO intel for a fixed set of
high-profile private companies using Perplexity, Polymarket, and
lightweight RSS news filtering.  Independent of the existing FMP IPO
calendar surface.

Always returns HTTP 200 with the six tracked companies.  If any path
would produce an empty / missing / non-list `companies`, the response
is normalized at the very end to the six fallback companies so the
frontend never sees `companies: []`.
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

_FALLBACK_DATA_CONFIDENCE = {
    "status": "limited",
    "label":  "Limited data available",
    "reason": "Live intelligence unavailable; showing tracked pre-IPO companies with safe fallback fields.",
}


# Try to share the static baselines with the service module.  If the import
# fails (the same failure mode this file's last-resort fallback exists for),
# fall back to a small inline copy so the six fallback companies still get
# meaningful values.
try:
    from services.pre_ipo_watchlist_service import FALLBACK_BASELINES as _ROUTE_BASELINES  # type: ignore
except Exception:
    _ROUTE_BASELINES = {
        "OpenAI": {
            "ipo_status":          "Not announced",
            "estimated_valuation": "Widely reported private-market range around $80B–$150B+",
            "valuation_notes": [
                "Fallback estimate based on widely reported recent private funding and secondary-market context.",
                "Live valuation intelligence unavailable; refresh when Perplexity/RSS sources are available.",
            ],
            "catalysts": [
                "Enterprise AI revenue scale",
                "Microsoft partnership and infrastructure needs",
                "Investor liquidity pressure",
                "Regulatory scrutiny",
            ],
            "expected_window": {"earliest": "Late 2026", "likely": "2027+"},
            "opportunity_score": 45,
            "momentum_badge":    "Watch",
            "score_breakdown": {
                "ipo_probability_score":    12,
                "valuation_momentum_score": 13,
                "news_recency_score":       4,
                "source_quality_score":     8,
                "catalyst_strength_score":  8,
            },
        },
        "SpaceX": {
            "ipo_status":          "Not announced",
            "estimated_valuation": "Widely reported private-market range around $180B–$350B+",
            "valuation_notes": [
                "Fallback estimate based on widely reported private valuation and secondary-market context.",
                "Starlink spinout remains the most realistic public-market path if leadership chooses to list.",
            ],
            "catalysts": [
                "Starlink revenue scale",
                "Secondary-market liquidity demand",
                "Satellite broadband expansion",
                "Defense and launch cadence",
            ],
            "expected_window": {
                "earliest": "2026",
                "likely":   "Starlink spinout more likely than full SpaceX IPO",
            },
            "opportunity_score": 50,
            "momentum_badge":    "Watch",
            "score_breakdown": {
                "ipo_probability_score":    13,
                "valuation_momentum_score": 15,
                "news_recency_score":       5,
                "source_quality_score":     8,
                "catalyst_strength_score":  9,
            },
        },
        "Anthropic": {
            "ipo_status":          "Not announced",
            "estimated_valuation": "Widely reported private-market range around $60B–$100B+",
            "valuation_notes": [
                "Fallback estimate based on recent AI funding-round reports and strategic investor demand.",
                "Live valuation intelligence unavailable; treat this as a baseline only.",
            ],
            "catalysts": [
                "Enterprise Claude adoption",
                "Strategic cloud partnerships",
                "AI infrastructure funding needs",
                "Competitive pressure from OpenAI and Google",
            ],
            "expected_window": {"earliest": "Late 2026", "likely": "2027+"},
            "opportunity_score": 42,
            "momentum_badge":    "Watch",
            "score_breakdown": {
                "ipo_probability_score":    11,
                "valuation_momentum_score": 12,
                "news_recency_score":       4,
                "source_quality_score":     7,
                "catalyst_strength_score":  8,
            },
        },
        "Databricks": {
            "ipo_status":          "Not announced",
            "estimated_valuation": "Widely reported private-market range around $40B–$60B+",
            "valuation_notes": [
                "Fallback estimate based on mature late-stage private-company valuation context.",
                "Databricks is one of the more IPO-ready private software companies, but timing remains unconfirmed.",
            ],
            "catalysts": [
                "AI data platform demand",
                "Lakehouse adoption",
                "Enterprise revenue maturity",
                "Public software market reopening",
            ],
            "expected_window": {
                "earliest": "2026",
                "likely":   "2026–2027 if IPO window improves",
            },
            "opportunity_score": 55,
            "momentum_badge":    "Watch",
            "score_breakdown": {
                "ipo_probability_score":    15,
                "valuation_momentum_score": 15,
                "news_recency_score":       6,
                "source_quality_score":     9,
                "catalyst_strength_score":  10,
            },
        },
        "Anduril": {
            "ipo_status":          "Not announced",
            "estimated_valuation": "Widely reported private-market range around $25B–$35B+",
            "valuation_notes": [
                "Fallback estimate based on defense-tech funding and private-market momentum.",
                "IPO timing depends heavily on defense spending backdrop and company readiness.",
            ],
            "catalysts": [
                "Defense-tech demand",
                "Autonomous systems adoption",
                "Government contract momentum",
                "Investor demand for defense AI exposure",
            ],
            "expected_window": {"earliest": "2026", "likely": "2027+"},
            "opportunity_score": 48,
            "momentum_badge":    "Watch",
            "score_breakdown": {
                "ipo_probability_score":    12,
                "valuation_momentum_score": 14,
                "news_recency_score":       5,
                "source_quality_score":     8,
                "catalyst_strength_score":  9,
            },
        },
        "Stripe": {
            "ipo_status":          "Not announced",
            "estimated_valuation": "Widely reported private-market range around $50B–$70B+",
            "valuation_notes": [
                "Fallback estimate based on late-stage fintech valuation and secondary-market context.",
                "Stripe has long been IPO-ready, but timing remains a management decision.",
            ],
            "catalysts": [
                "Payments volume growth",
                "Fintech IPO window reopening",
                "Employee/investor liquidity demand",
                "Profitability and public-market readiness",
            ],
            "expected_window": {
                "earliest": "2026",
                "likely":   "2026–2027 if fintech IPO market improves",
            },
            "opportunity_score": 52,
            "momentum_badge":    "Watch",
            "score_breakdown": {
                "ipo_probability_score":    14,
                "valuation_momentum_score": 14,
                "news_recency_score":       5,
                "source_quality_score":     9,
                "catalyst_strength_score":  10,
            },
        },
    }


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
    """
    Last-resort per-company fallback used only when the service module
    itself fails to import.  Mirrors the baselines defined in
    services.pre_ipo_watchlist_service so this layer also avoids the
    all-Unknown/zero shape.
    """
    baseline = _ROUTE_BASELINES.get(name) or {}
    return {
        "company":             name,
        "ipo_status":          baseline.get("ipo_status") or "Unknown",
        "estimated_valuation": baseline.get("estimated_valuation") or "Unknown",
        "valuation_notes":     list(baseline.get("valuation_notes") or []),
        "polymarket": {
            "ipo_probability_12m": None,
            "valuation_markets":   [],
            "summary":             "No direct prediction market found.",
        },
        "catalysts":           list(baseline.get("catalysts") or []),
        "expected_window":     dict(baseline.get("expected_window") or {
            "earliest": "Unknown", "likely": "Unknown",
        }),
        "confidence_score":    "Low",
        "latest_news":         [],
        "sources":             [],
        "opportunity_score":   int(baseline.get("opportunity_score") or 0),
        "rank":                rank,
        "momentum_badge":      baseline.get("momentum_badge") or "Dormant",
        "score_breakdown":     dict(baseline.get("score_breakdown") or {
            "ipo_probability_score":     0,
            "valuation_momentum_score":  0,
            "news_recency_score":        0,
            "source_quality_score":      0,
            "catalyst_strength_score":   0,
        }),
        "change_tracking": {
            "valuation_change":       "Unknown",
            "ipo_probability_change": "Unknown",
            "new_catalysts":          [],
            "previous_score":         None,
            "score_change":           None,
            "last_snapshot_at":       None,
        },
    }


def _fallback_companies() -> list[dict[str, Any]]:
    """
    Build the fallback six.  Tries to mirror the service-module ranking
    (by opportunity_score desc) so the response shape matches what the
    service layer would have produced.
    """
    entries = [
        _safe_empty_company(name, idx)
        for idx, name in enumerate(_FALLBACK_COMPANY_NAMES, start=1)
    ]
    entries.sort(key=lambda x: int(x.get("opportunity_score") or 0), reverse=True)
    for idx, entry in enumerate(entries, start=1):
        entry["rank"] = idx
    return entries


def _safe_fallback_payload(reason: Optional[str] = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status":     "ok",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "companies":  _fallback_companies(),
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
    Map service-level status / metadata onto a top-level signal for the
    frontend.  Reuses existing fields on the payload.
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


def _normalize_response(payload: Any) -> dict[str, Any]:
    """
    Final guard applied immediately before returning the response.

    Rules:
      - If `payload` is not a dict, replace with a fresh fallback payload.
      - If `companies` is missing, null, not a list, or empty, replace with
        the six tracked fallback companies and force `data_confidence` to
        the "limited" block.
      - Otherwise, leave companies untouched and (re)derive data_confidence
        from the service-level fields.
    """
    if not isinstance(payload, dict):
        print("[pre_ipo_watchlist] normalization: payload not a dict, using fallback six")
        out = _safe_fallback_payload("payload not a dict")
        out["data_confidence"] = dict(_FALLBACK_DATA_CONFIDENCE)
        return out

    out: dict[str, Any] = dict(payload)
    out.setdefault("status", "ok")
    out.setdefault("updated_at", datetime.now(timezone.utc).isoformat())

    companies = out.get("companies")
    if not isinstance(companies, list) or len(companies) == 0:
        print(
            "[pre_ipo_watchlist] normalization: empty/missing companies "
            f"(status={out.get('status')!r}, "
            f"fallback_reason={out.get('fallback_reason')!r}); "
            "replacing with tracked fallback six"
        )
        out["companies"] = _fallback_companies()
        out.setdefault("fallback_reason", "empty companies list normalized to tracked six")
        out["data_confidence"] = dict(_FALLBACK_DATA_CONFIDENCE)
        return out

    # If the service signalled a fallback (e.g. live build returned empty
    # and the service substituted the safe six), force the canonical
    # "limited" data_confidence block — we are serving fallback data.
    if out.get("fallback_reason"):
        out["data_confidence"] = dict(_FALLBACK_DATA_CONFIDENCE)
        return out

    out["data_confidence"] = _derive_data_confidence(out)
    return out


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

    Always returns HTTP 200 with exactly the six tracked companies.
    """
    err = _check_key(api_key)
    if err:
        return err

    payload: Any
    try:
        from services.pre_ipo_watchlist_service import get_pre_ipo_watchlist
    except Exception as e:
        print(f"[pre_ipo_watchlist] service import failed: {e}")
        payload = _safe_fallback_payload(f"service import failed: {e}")
        return JSONResponse(content=_normalize_response(payload))

    try:
        payload = await get_pre_ipo_watchlist(refresh=refresh)
    except Exception as e:
        print(f"[pre_ipo_watchlist] unhandled: {e}")
        payload = _safe_fallback_payload(f"unhandled: {e}")

    return JSONResponse(content=_normalize_response(payload))
