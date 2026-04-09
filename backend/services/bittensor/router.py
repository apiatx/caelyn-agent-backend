"""
FastAPI router for the Bittensor subnet dashboard.

Proxies/aggregates data from the TaoStats API with in-memory caching.

Endpoints:
  GET /api/bittensor/debug                  — diagnostic info (always works)
  GET /api/bittensor/dashboard              — aggregated dashboard data
  GET /api/bittensor/blocks/history         — blocks emitted over time
  GET /api/bittensor/subnet/{netuid}/metagraph — metagraph for a subnet
  GET /api/bittensor/price/history          — 30-day TAO OHLC price history
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from config import TAOSTATS_API_KEY, TAOAPP_API_KEY

router = APIRouter(prefix="/api/bittensor", tags=["bittensor"])

TAOSTATS_BASE = "https://api.taostats.io"
REQUEST_TIMEOUT = 15.0

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


def _safe_str(val: Any, default: str = "0") -> str:
    """Safely convert any value to string."""
    if val is None:
        return default
    return str(val)


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
        print(f"[bittensor] TaoStats request failed: {path} params={params} — {exc}")
        return None


def _extract_data(raw: Any) -> list | dict | None:
    """Extract the 'data' field from a TaoStats response, handling various shapes."""
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw.get("data", raw)
    return raw


def _extract_first(raw: Any) -> dict | None:
    """Extract the first item from a TaoStats paginated response."""
    data = _extract_data(raw)
    if isinstance(data, list) and len(data) > 0:
        return data[0] if isinstance(data[0], dict) else None
    if isinstance(data, dict):
        return data
    return None


def _parse_hotkey(hotkey_val: Any) -> str:
    """Parse hotkey which may be a dict with .ss58 or just a string."""
    if hotkey_val is None:
        return ""
    if isinstance(hotkey_val, dict):
        return str(hotkey_val.get("ss58", hotkey_val.get("address", "")))
    return str(hotkey_val)


# ── Debug endpoint ──────────────────────────────────────────────────────────

@router.get("/debug")
async def debug_endpoint():
    """
    Diagnostic endpoint — always works, no auth required.
    Shows API key status and runs a live test against TaoStats.
    """
    result: dict[str, Any] = {
        "api_key_configured": bool(TAOSTATS_API_KEY),
        "api_key_prefix": (TAOSTATS_API_KEY[:4] + "...") if TAOSTATS_API_KEY and len(TAOSTATS_API_KEY) >= 4 else "(not set)",
        "taoapp_key_configured": bool(TAOAPP_API_KEY),
        "taoapp_key_prefix": (TAOAPP_API_KEY[:4] + "...") if TAOAPP_API_KEY and len(TAOAPP_API_KEY) >= 4 else "(not set)",
        "test_endpoint": "GET /api/price/latest/v1?asset=tao",
        "test_result": "SKIPPED (no API key)",
        "raw_sample": None,
    }

    if TAOSTATS_API_KEY:
        try:
            async with httpx.AsyncClient() as client:
                raw = await _taostats_get(client, "/api/price/latest/v1", {"asset": "tao"})
            if raw is not None:
                first = _extract_first(raw)
                result["test_result"] = "OK"
                result["raw_sample"] = first
            else:
                result["test_result"] = "ERROR: request returned None (check server logs)"
        except Exception as exc:
            result["test_result"] = f"ERROR: {exc}"

    return result


# ── Blocks history endpoint ─────────────────────────────────────────────────

@router.get("/blocks/history")
async def blocks_history_endpoint(
    scale: str = Query("days", regex="^(days|hours)$"),
    points: int = Query(30, ge=1, le=100),
):
    """
    Blocks emitted over time for charting.
    Cached for 5 minutes.
    """
    if not TAOSTATS_API_KEY:
        return JSONResponse(
            status_code=503,
            content={"error": "TAOSTATS_API_KEY not configured"},
        )

    cache_key = f"blocks_history_{scale}_{points}"
    cached = _cache_get(cache_key, ttl=300)
    if cached is not None:
        return cached

    expected_per_interval = 7200 if scale == "days" else 300
    frequency = "by_day" if scale == "days" else "by_hour"

    async with httpx.AsyncClient() as client:
        # Primary approach: use block/interval endpoint
        print(f"[bittensor] blocks/history: trying /api/block/interval/v1 frequency={frequency} limit={points + 1}")
        interval_raw = await _taostats_get(client, "/api/block/interval/v1", {
            "frequency": frequency,
            "limit": points + 1,
        })

        interval_data = _extract_data(interval_raw)
        data_points: list[dict[str, Any]] = []

        if isinstance(interval_data, list) and len(interval_data) >= 2:
            print(f"[bittensor] blocks/history: interval endpoint returned {len(interval_data)} items")
            # Sort by timestamp ascending
            sorted_data = sorted(interval_data, key=lambda x: x.get("timestamp", ""))
            for i in range(1, len(sorted_data)):
                prev = sorted_data[i - 1]
                curr = sorted_data[i]
                prev_block = int(_safe_float(prev.get("block_number", 0)))
                curr_block = int(_safe_float(curr.get("block_number", 0)))
                delta = curr_block - prev_block
                ts = curr.get("timestamp", "")
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    label = dt.strftime("%b %d") if scale == "days" else dt.strftime("%H:%M")
                except Exception:
                    label = ts[:10] if len(ts) >= 10 else ts
                data_points.append({
                    "label": label,
                    "blocks": delta,
                    "expected": expected_per_interval,
                    "timestamp": ts,
                })
        else:
            # Fallback: get current block, then query per-day blocks
            print(f"[bittensor] blocks/history: interval endpoint failed or insufficient data, using fallback")
            current_raw = await _taostats_get(client, "/api/block/v1", {"limit": 1})
            current_item = _extract_first(current_raw)
            if current_item:
                current_block = int(_safe_float(current_item.get("block_number", 0)))
                current_ts = current_item.get("timestamp", datetime.now(timezone.utc).isoformat())
                print(f"[bittensor] blocks/history fallback: current block={current_block}")

                # Query block at each past day/hour boundary
                block_snapshots: list[tuple[str, int, str]] = []
                now = datetime.now(timezone.utc)
                interval_delta = timedelta(days=1) if scale == "days" else timedelta(hours=1)

                for i in range(points + 1):
                    t = now - (interval_delta * i)
                    t_str = t.isoformat()
                    snap_raw = await _taostats_get(client, "/api/block/v1", {
                        "timestamp_end": t_str,
                        "limit": 1,
                    })
                    snap_item = _extract_first(snap_raw)
                    if snap_item:
                        bn = int(_safe_float(snap_item.get("block_number", 0)))
                        block_snapshots.append((t_str, bn, t.strftime("%b %d") if scale == "days" else t.strftime("%H:%M")))

                # Reverse to chronological order and compute deltas
                block_snapshots.reverse()
                for i in range(1, len(block_snapshots)):
                    prev_ts, prev_bn, _ = block_snapshots[i - 1]
                    curr_ts, curr_bn, curr_label = block_snapshots[i]
                    delta = curr_bn - prev_bn
                    data_points.append({
                        "label": curr_label,
                        "blocks": delta,
                        "expected": expected_per_interval,
                        "timestamp": curr_ts,
                    })

    result = {
        "scale": scale,
        "expected_per_interval": expected_per_interval,
        "data": data_points,
    }
    _cache_set(cache_key, result)
    return result


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
        tao_price_task = _taostats_get(client, "/api/price/latest/v1", {"asset": "tao"})
        pools_task = _taostats_get(client, "/api/dtao/pool/latest/v1", {
            "page": 1, "limit": 100, "order_by": "market_cap", "order": "desc",
        })
        stats_task = _taostats_get(client, "/api/stats/latest/v1")
        total_price_task = _taostats_get(client, "/api/dtao/pool/total_price/latest/v1")
        identities_task = _taostats_get(client, "/api/subnet/identity/v1", {
            "page": 1, "limit": 200,
        })
        block_task = _taostats_get(client, "/api/block/v1", {"limit": 1})

        tao_price_raw, pools_raw, stats_raw, total_price_raw, identities_raw, block_raw = (
            await asyncio.gather(
                tao_price_task, pools_task, stats_task,
                total_price_task, identities_task, block_task,
            )
        )

    # ── Log what each subcall returned ──────────────────────────────────
    print(f"[bittensor] dashboard: tao_price_raw type={type(tao_price_raw).__name__}, "
          f"keys={list(tao_price_raw.keys()) if isinstance(tao_price_raw, dict) else 'N/A'}")
    print(f"[bittensor] dashboard: pools_raw type={type(pools_raw).__name__}, "
          f"data_len={len(pools_raw.get('data', [])) if isinstance(pools_raw, dict) else 'N/A'}")
    print(f"[bittensor] dashboard: stats_raw type={type(stats_raw).__name__}, "
          f"keys={list(stats_raw.keys()) if isinstance(stats_raw, dict) else 'N/A'}")
    print(f"[bittensor] dashboard: total_price_raw type={type(total_price_raw).__name__}, "
          f"keys={list(total_price_raw.keys()) if isinstance(total_price_raw, dict) else 'N/A'}")
    print(f"[bittensor] dashboard: identities_raw type={type(identities_raw).__name__}, "
          f"data_len={len(identities_raw.get('data', [])) if isinstance(identities_raw, dict) else 'N/A'}")
    print(f"[bittensor] dashboard: block_raw type={type(block_raw).__name__}, "
          f"keys={list(block_raw.keys()) if isinstance(block_raw, dict) else 'N/A'}")

    # ── Parse TAO price ──────────────────────────────────────────────────
    tao_price = {"price": "0", "change_24h": "0"}
    try:
        tp_item = _extract_first(tao_price_raw)
        if tp_item:
            tao_price = {
                "price": _safe_str(tp_item.get("price", tp_item.get("close", "0"))),
                "change_24h": _safe_str(
                    tp_item.get("price_change_24h",
                    tp_item.get("change_24h",
                    tp_item.get("percent_change_24h", "0")))
                ),
            }
            print(f"[bittensor] dashboard: parsed tao_price={tao_price}")
        else:
            print(f"[bittensor] dashboard: WARNING — could not extract tao price item")
    except Exception as exc:
        print(f"[bittensor] dashboard: ERROR parsing tao_price — {exc}")

    # ── Parse latest block ───────────────────────────────────────────────
    block_number = 0
    try:
        block_item = _extract_first(block_raw)
        if block_item:
            block_number = int(_safe_float(block_item.get("block_number", 0)))
            print(f"[bittensor] dashboard: block_number={block_number}")
        else:
            print(f"[bittensor] dashboard: WARNING — could not extract block number")
    except Exception as exc:
        print(f"[bittensor] dashboard: ERROR parsing block — {exc}")

    # ── Parse network stats ──────────────────────────────────────────────
    network_stats: dict[str, Any] = {}
    try:
        st_item = _extract_first(stats_raw)
        if st_item:
            network_stats = st_item
            print(f"[bittensor] dashboard: network_stats keys={list(st_item.keys())}")
        else:
            print(f"[bittensor] dashboard: WARNING — could not extract network stats")
    except Exception as exc:
        print(f"[bittensor] dashboard: ERROR parsing network_stats — {exc}")

    # ── Parse total market ───────────────────────────────────────────────
    total_market: dict[str, Any] = {
        "total_price_tao": "0",
        "fear_greed_score": 0,
        "fear_greed_label": "N/A",
    }
    try:
        tm_item = _extract_first(total_price_raw)
        if tm_item:
            total_market = {
                "total_price_tao": _safe_str(
                    tm_item.get("total_price_tao",
                    tm_item.get("total_price",
                    tm_item.get("price", "0")))
                ),
                "fear_greed_score": _safe_float(
                    tm_item.get("fear_greed_score",
                    tm_item.get("fear_greed",
                    tm_item.get("score", 0)))
                ),
                "fear_greed_label": _safe_str(
                    tm_item.get("fear_greed_label",
                    tm_item.get("label",
                    tm_item.get("sentiment", "N/A"))),
                    default="N/A"
                ),
            }
            print(f"[bittensor] dashboard: total_market={total_market}")
        else:
            print(f"[bittensor] dashboard: WARNING — could not extract total market data")
    except Exception as exc:
        print(f"[bittensor] dashboard: ERROR parsing total_market — {exc}")

    # ── Build identity lookup by netuid ──────────────────────────────────
    identity_map: dict[int, dict] = {}
    try:
        id_data = _extract_data(identities_raw)
        if isinstance(id_data, list):
            for item in id_data:
                if isinstance(item, dict):
                    nid = item.get("netuid")
                    if nid is not None:
                        identity_map[int(nid)] = item
            print(f"[bittensor] dashboard: loaded {len(identity_map)} subnet identities")
        else:
            print(f"[bittensor] dashboard: WARNING — identities data is not a list: {type(id_data).__name__}")
    except Exception as exc:
        print(f"[bittensor] dashboard: ERROR parsing identities — {exc}")

    # ── Parse pools and merge with identities ────────────────────────────
    subnets: list[dict[str, Any]] = []
    try:
        pool_data = _extract_data(pools_raw)
        if isinstance(pool_data, list):
            print(f"[bittensor] dashboard: processing {len(pool_data)} pools")
            for idx, pool in enumerate(pool_data):
                if not isinstance(pool, dict):
                    print(f"[bittensor] dashboard: WARNING — pool[{idx}] is not a dict: {type(pool).__name__}")
                    continue
                netuid = int(_safe_float(pool.get("netuid", 0)))
                identity = identity_map.get(netuid, {})

                # Defensive name extraction: try multiple field variants
                name = (
                    identity.get("subnet_name")
                    or identity.get("name")
                    or identity.get("subnet_label")
                    or f"SN{netuid}"
                )

                # Defensive tao_in extraction: try multiple field variants
                tao_in_val = pool.get("tao_in")
                if tao_in_val is None:
                    tao_in_val = pool.get("tao_in_pool")
                if tao_in_val is None:
                    tao_in_val = pool.get("pool_tao")
                if tao_in_val is None:
                    tao_in_val = 0

                if idx < 3:
                    print(f"[bittensor] dashboard: pool sample [{idx}] netuid={netuid} "
                          f"name={name} price={pool.get('price')} tao_in={tao_in_val} "
                          f"pool_keys={list(pool.keys())}")

                subnets.append({
                    "netuid": netuid,
                    "name": name,
                    "description": identity.get("description", ""),
                    "price": _safe_str(pool.get("price", "0")),
                    "market_cap": _safe_str(pool.get("market_cap", "0")),
                    "price_change_24h": _safe_str(pool.get("price_change_24h", pool.get("change_24h", "0"))),
                    "price_change_7d": _safe_str(pool.get("price_change_7d", pool.get("change_7d", "0"))),
                    "emission": _safe_str(pool.get("emission", pool.get("daily_emission", "0"))),
                    "tao_in": _safe_str(tao_in_val),
                    "alpha_in": _safe_str(pool.get("alpha_in", pool.get("alpha_in_pool", "0"))),
                    "volume_24h": _safe_str(pool.get("volume_24h", pool.get("volume", "0"))),
                    "seven_day_price_history": pool.get("seven_day_price_history", pool.get("price_history_7d", [])),
                    "is_active": pool.get("is_active", pool.get("active", True)),
                    "discord": identity.get("discord", ""),
                    "twitter": identity.get("twitter", ""),
                    "github": identity.get("github", ""),
                })
        else:
            print(f"[bittensor] dashboard: WARNING — pool data is not a list: {type(pool_data).__name__}")
    except Exception as exc:
        print(f"[bittensor] dashboard: ERROR parsing pools — {exc}")

    result = {
        "tao_price": tao_price,
        "network_stats": network_stats,
        "total_market": total_market,
        "block_number": block_number,
        "subnets": subnets,
        "subnet_count": len(subnets),
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

    data = _extract_data(raw)
    # Normalize metagraph items — hotkey may be dict or string
    normalized: list[dict[str, Any]] = []
    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            normalized.append({
                "uid": item.get("uid", 0),
                "hotkey": _parse_hotkey(item.get("hotkey")),
                "coldkey": _parse_hotkey(item.get("coldkey")),
                "vtrust": _safe_float(item.get("vtrust", 0)),
                "stake": _safe_float(item.get("stake", 0)),
                "emission": _safe_float(item.get("emission", 0)),
                "stake_weight": _safe_float(item.get("stake_weight", 0)),
                "active": item.get("active", item.get("is_active", True)),
                "trust": _safe_float(item.get("trust", 0)),
                "incentive": _safe_float(item.get("incentive", 0)),
                "dividends": _safe_float(item.get("dividends", 0)),
                "consensus": _safe_float(item.get("consensus", 0)),
            })

    result = {"netuid": netuid, "data": normalized}

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

    data = _extract_data(raw)
    result = {"asset": "tao", "period": "1d", "data": data}

    _cache_set("price_history", result)
    return result
