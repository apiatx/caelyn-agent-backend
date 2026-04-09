"""
FastAPI router for the Bittensor subnet dashboard.

Proxies/aggregates data from the TaoStats API with in-memory caching.

Endpoints:
  GET /api/bittensor/dashboard              — aggregated dashboard data
  GET /api/bittensor/subnet/{netuid}/metagraph — metagraph for a subnet
  GET /api/bittensor/price/history           — 30-day TAO OHLC price history
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from config import TAOSTATS_API_KEY

router = APIRouter(prefix="/api/bittensor", tags=["bittensor"])

TAOSTATS_BASE = "https://api.taostats.io"
REQUEST_TIMEOUT = 10.0

# ── Simple in-memory cache ───────────────────────────────────────────────────
_cache: dict[str, dict[str, Any]] = {}


def _cache_get(key: str, ttl: float) -> Any | None:
    entry = _cache.get(key)
    if entry and (time.time() - entry["ts"]) < ttl:
        return entry["data"]
    return None


def _cache_set(key: str, data: Any) -> None:
    _cache[key] = {"data": data, "ts": time.time()}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _safe_float(val: Any, default: float = 0.0) -> float:
    """Convert a value that may be string or number to float safely."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _headers() -> dict[str, str]:
    return {"Authorization": TAOSTATS_API_KEY or ""}


async def _taostats_get(client: httpx.AsyncClient, path: str, params: dict | None = None) -> Any:
    """Make a GET request to TaoStats. Returns parsed JSON or None on failure."""
    try:
        resp = await client.get(
            f"{TAOSTATS_BASE}{path}",
            params=params,
            headers=_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        print(f"[bittensor] TaoStats request failed: {path} — {exc}")
        return None


# ── Dashboard endpoint ───────────────────────────────────────────────────────

@router.get("/dashboard")
async def dashboard_endpoint():
    """
    Aggregated Bittensor dashboard: TAO price, network stats,
    total market data, and all subnet pools merged with identities.
    Cached for 60 seconds.
    """
    if not TAOSTATS_API_KEY:
        return JSONResponse(
            status_code=503,
            content={"error": "TAOSTATS_API_KEY not configured"},
        )

    cached = _cache_get("dashboard", ttl=60)
    if cached is not None:
        return cached

    async with httpx.AsyncClient() as client:
        # Fire all requests concurrently
        import asyncio
        tao_price_task = _taostats_get(client, "/api/price/latest/v1", {"asset": "tao"})
        pools_task = _taostats_get(client, "/api/dtao/pool/latest/v1", {
            "page": 1, "limit": 100, "order_by": "market_cap", "order": "desc",
        })
        stats_task = _taostats_get(client, "/api/stats/latest/v1")
        total_price_task = _taostats_get(client, "/api/dtao/pool/total_price/latest/v1")
        identities_task = _taostats_get(client, "/api/subnet/identity/v1", {
            "page": 1, "limit": 100,
        })

        tao_price_raw, pools_raw, stats_raw, total_price_raw, identities_raw = (
            await asyncio.gather(
                tao_price_task, pools_task, stats_task,
                total_price_task, identities_task,
            )
        )

    # ── Parse TAO price ──────────────────────────────────────────────────
    tao_price = {"price": "0", "change_24h": "0"}
    if tao_price_raw:
        tp_data = tao_price_raw.get("data") if isinstance(tao_price_raw, dict) else tao_price_raw
        if isinstance(tp_data, list) and len(tp_data) > 0:
            tp_data = tp_data[0]
        if isinstance(tp_data, dict):
            tao_price = {
                "price": str(tp_data.get("price", "0")),
                "change_24h": str(tp_data.get("price_change_24h", tp_data.get("change_24h", "0"))),
            }

    # ── Parse network stats ──────────────────────────────────────────────
    network_stats: dict[str, Any] = {}
    if stats_raw:
        st_data = stats_raw.get("data") if isinstance(stats_raw, dict) else stats_raw
        if isinstance(st_data, list) and len(st_data) > 0:
            st_data = st_data[0]
        if isinstance(st_data, dict):
            network_stats = st_data

    # ── Parse total market ───────────────────────────────────────────────
    total_market: dict[str, Any] = {
        "total_price_tao": "0",
        "fear_greed_score": 0,
        "fear_greed_label": "N/A",
    }
    if total_price_raw:
        tm_data = total_price_raw.get("data") if isinstance(total_price_raw, dict) else total_price_raw
        if isinstance(tm_data, list) and len(tm_data) > 0:
            tm_data = tm_data[0]
        if isinstance(tm_data, dict):
            total_market = {
                "total_price_tao": str(tm_data.get("total_price_tao", tm_data.get("total_price", "0"))),
                "fear_greed_score": _safe_float(tm_data.get("fear_greed_score", tm_data.get("fear_greed", 0))),
                "fear_greed_label": str(tm_data.get("fear_greed_label", tm_data.get("label", "N/A"))),
            }

    # ── Build identity lookup by netuid ──────────────────────────────────
    identity_map: dict[int, dict] = {}
    if identities_raw:
        id_data = identities_raw.get("data") if isinstance(identities_raw, dict) else identities_raw
        if isinstance(id_data, list):
            for item in id_data:
                if isinstance(item, dict):
                    nid = item.get("netuid")
                    if nid is not None:
                        identity_map[int(nid)] = item

    # ── Parse pools and merge with identities ────────────────────────────
    subnets: list[dict[str, Any]] = []
    if pools_raw:
        pool_data = pools_raw.get("data") if isinstance(pools_raw, dict) else pools_raw
        if isinstance(pool_data, list):
            for pool in pool_data:
                if not isinstance(pool, dict):
                    continue
                netuid = int(_safe_float(pool.get("netuid", 0)))
                identity = identity_map.get(netuid, {})
                subnets.append({
                    "netuid": netuid,
                    "name": identity.get("subnet_name", identity.get("name", f"Subnet {netuid}")),
                    "description": identity.get("description", ""),
                    "price": str(pool.get("price", "0")),
                    "market_cap": str(pool.get("market_cap", "0")),
                    "price_change_24h": str(pool.get("price_change_24h", "0")),
                    "price_change_7d": str(pool.get("price_change_7d", "0")),
                    "emission": str(pool.get("emission", "0")),
                    "tao_in": str(pool.get("tao_in", "0")),
                    "alpha_in": str(pool.get("alpha_in", "0")),
                    "volume_24h": str(pool.get("volume_24h", "0")),
                    "seven_day_price_history": pool.get("seven_day_price_history", []),
                    "is_active": pool.get("is_active", True),
                    "discord": identity.get("discord", ""),
                    "twitter": identity.get("twitter", ""),
                    "github": identity.get("github", ""),
                })

    result = {
        "tao_price": tao_price,
        "network_stats": network_stats,
        "total_market": total_market,
        "subnets": subnets,
        "as_of": datetime.now(timezone.utc).isoformat(),
    }

    _cache_set("dashboard", result)
    return result


# ── Metagraph endpoint ───────────────────────────────────────────────────────

@router.get("/subnet/{netuid}/metagraph")
async def metagraph_endpoint(netuid: int):
    """
    Full metagraph for a specific subnet.
    Cached per netuid for 30 seconds.
    """
    if not TAOSTATS_API_KEY:
        return JSONResponse(
            status_code=503,
            content={"error": "TAOSTATS_API_KEY not configured"},
        )

    cache_key = f"metagraph_{netuid}"
    cached = _cache_get(cache_key, ttl=30)
    if cached is not None:
        return cached

    async with httpx.AsyncClient() as client:
        raw = await _taostats_get(client, "/api/metagraph/latest/v1", {
            "netuid": netuid, "limit": 256,
        })

    if raw is None:
        raise HTTPException(status_code=502, detail="Failed to fetch metagraph from TaoStats")

    data = raw.get("data") if isinstance(raw, dict) else raw
    result = {"netuid": netuid, "data": data}

    _cache_set(cache_key, result)
    return result


# ── Price history endpoint ───────────────────────────────────────────────────

@router.get("/price/history")
async def price_history_endpoint():
    """
    30-day TAO OHLC price history for charting.
    Cached for 5 minutes.
    """
    if not TAOSTATS_API_KEY:
        return JSONResponse(
            status_code=503,
            content={"error": "TAOSTATS_API_KEY not configured"},
        )

    cached = _cache_get("price_history", ttl=300)
    if cached is not None:
        return cached

    async with httpx.AsyncClient() as client:
        raw = await _taostats_get(client, "/api/price/ohlc/v1", {
            "asset": "tao", "period": "1d", "limit": 30,
        })

    if raw is None:
        raise HTTPException(status_code=502, detail="Failed to fetch price history from TaoStats")

    data = raw.get("data") if isinstance(raw, dict) else raw
    result = {"asset": "tao", "period": "1d", "data": data}

    _cache_set("price_history", result)
    return result
