"""
FastAPI router for the Bittensor subnet dashboard.

Proxies/aggregates data from the TaoStats API with in-memory caching.

Rate-limit strategy
-------------------
TaoStats allows 5 requests / minute.  To stay safe the dashboard data is
pre-fetched by a background task that spaces each of its 6 sequential
TaoStats calls by _INTER_CALL_DELAY_BG seconds (≥ 13 s → < 5 calls/min).
The /dashboard endpoint always returns from cache instantly.

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
TAOAPP_BASE = "https://api.tao.app"
REQUEST_TIMEOUT = 20.0

# Refresh interval for the background pre-fetcher (5 minutes)
_DASHBOARD_REFRESH_INTERVAL = 300
# How long the dashboard cache is considered valid before a background refresh
_DASHBOARD_CACHE_TTL = 280  # slightly less than refresh interval

# ── Simple in-memory cache ───────────────────────────────────────────────────
_cache: dict[str, dict[str, Any]] = {}
_dashboard_refresh_lock = asyncio.Lock()
_dashboard_bg_task: asyncio.Task | None = None


def _cache_get(key: str, ttl: float) -> Any | None:
    entry = _cache.get(key)
    if entry and (time.time() - entry["ts"]) < ttl:
        return entry["data"]
    return None


def _cache_set(key: str, data: Any) -> None:
    _cache[key] = {"data": data, "ts": time.time()}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _safe_float(val: Any, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _safe_str(val: Any, default: str = "0") -> str:
    if val is None:
        return default
    return str(val)


def _taostats_headers() -> dict[str, str]:
    return {"Authorization": TAOSTATS_API_KEY or ""}


def _taoapp_headers() -> dict[str, str]:
    return {"X-API-Key": TAOAPP_API_KEY or ""}


async def _taostats_get(
    client: httpx.AsyncClient,
    path: str,
    params: dict | None = None,
    retry_on_429: bool = True,
) -> Any:
    """GET from TaoStats.  On 429 waits 65 s and retries once (if retry_on_429)."""
    url = f"{TAOSTATS_BASE}{path}"
    for attempt in range(2 if retry_on_429 else 1):
        try:
            resp = await client.get(
                url, params=params, headers=_taostats_headers(), timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 429:
                print(f"[bittensor] TaoStats 429 {path} attempt={attempt+1}")
                if attempt == 0 and retry_on_429:
                    await asyncio.sleep(65.0)
                    continue
                return None
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 429:
                print(f"[bittensor] TaoStats 429(HTTPStatusError) {path} attempt={attempt+1}")
                if attempt == 0 and retry_on_429:
                    await asyncio.sleep(65.0)
                    continue
                return None
            print(f"[bittensor] TaoStats error {path}: {exc}")
            return None
        except Exception as exc:
            print(f"[bittensor] TaoStats error {path}: {exc}")
            return None
    return None


async def _taoapp_get(
    client: httpx.AsyncClient,
    path: str,
    params: dict | None = None,
) -> Any:
    """GET from TaoApp API (10 req/min limit, X-API-Key auth)."""
    url = f"{TAOAPP_BASE}{path}"
    try:
        resp = await client.get(
            url, params=params, headers=_taoapp_headers(), timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code == 429:
            print(f"[bittensor] TaoApp 429 {path}")
            return None
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        print(f"[bittensor] TaoApp error {path}: {exc}")
        return None


def _extract_data(raw: Any) -> list | dict | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw.get("data", raw)
    return raw


def _extract_first(raw: Any) -> dict | None:
    data = _extract_data(raw)
    if isinstance(data, list) and len(data) > 0:
        return data[0] if isinstance(data[0], dict) else None
    if isinstance(data, dict):
        return data
    return None


def _parse_hotkey(hotkey_val: Any) -> str:
    if hotkey_val is None:
        return ""
    if isinstance(hotkey_val, dict):
        return str(hotkey_val.get("ss58", hotkey_val.get("address", "")))
    return str(hotkey_val)


# ── Background dashboard pre-fetcher ────────────────────────────────────────

async def _fetch_dashboard_data() -> dict:
    """
    Fetches dashboard data using TaoApp as the primary source (10 req/min)
    and TaoStats for network stats (5 req/min).

    TaoApp calls (4, run in parallel):
      - /api/beta/current          → TAO price, block_number, market_cap
      - /api/beta/subnet_screener  → all 128 subnets with price/volume/social
      - /api/beta/analytics/macro/fear_greed/current → fear/greed index
      - /api/beta/subnets/about/summaries → subnet descriptions

    TaoStats calls (1, sequential to stay under 5 req/min):
      - /api/stats/latest/v1       → network staking/issuance stats

    Stale-on-error: if individual endpoints fail, the previous cached values
    are preserved rather than being overwritten with zeros.
    """
    print("[bittensor] background fetch: starting dashboard data pull (TaoApp primary)")

    # Load previous cached data for stale-on-error fallback
    prev_entry = _cache.get("dashboard")
    prev: dict[str, Any] = prev_entry["data"] if prev_entry else {}

    async with httpx.AsyncClient() as client:
        # Fire all TaoApp calls in parallel — 10 req/min limit is comfortable
        (
            current_raw,
            screener_raw,
            fear_greed_raw,
            summaries_raw,
            stats_raw,
        ) = await asyncio.gather(
            _taoapp_get(client, "/api/beta/current"),
            _taoapp_get(client, "/api/beta/subnet_screener"),
            _taoapp_get(client, "/api/beta/analytics/macro/fear_greed/current"),
            _taoapp_get(client, "/api/beta/subnets/about/summaries"),
            _taostats_get(client, "/api/stats/latest/v1", retry_on_429=False),
        )

    print(f"[bittensor] bg: current={'OK' if current_raw else 'FAIL'} "
          f"screener={'OK,' + str(len(screener_raw)) + ' subnets' if isinstance(screener_raw, list) else 'FAIL'} "
          f"fear_greed={'OK' if fear_greed_raw else 'FAIL'} "
          f"summaries={'OK,' + str(len(summaries_raw)) + ' items' if isinstance(summaries_raw, list) else 'FAIL'} "
          f"stats={'OK' if stats_raw else 'FAIL (non-fatal)'}")

    # ── TAO price & block (from TaoApp /current) ──────────────────────────
    # On failure: preserve previous cached values (stale-on-error)
    prev_tao_price = prev.get("tao_price", {"price": "0", "change_24h": "0"})
    prev_block = prev.get("block_number", 0)
    if isinstance(current_raw, dict):
        tao_price = {
            "price": _safe_str(current_raw.get("price", "0")),
            "change_24h": _safe_str(current_raw.get("percent_change_24h", "0")),
        }
        block_number = int(_safe_float(current_raw.get("max_block_number", 0)))
    else:
        tao_price = prev_tao_price
        block_number = prev_block

    # ── Fear/greed (from TaoApp) ───────────────────────────────────────────
    # On failure: preserve previous cached values (stale-on-error)
    prev_total_market = prev.get("total_market", {
        "total_price_tao": "0", "fear_greed_score": 0, "fear_greed_label": "N/A",
    })
    if isinstance(fear_greed_raw, dict):
        total_market: dict[str, Any] = {
            "total_price_tao": _safe_str(current_raw.get("market_cap", "0") if isinstance(current_raw, dict) else prev_total_market.get("total_price_tao", "0")),
            "fear_greed_score": _safe_float(fear_greed_raw.get("fear_greed_index", 0)),
            "fear_greed_label": _safe_str(fear_greed_raw.get("sentiment", "N/A"), default="N/A"),
        }
    else:
        total_market = prev_total_market

    # ── Network stats (from TaoStats, best-effort) ─────────────────────────
    # On failure: preserve previous cached values (stale-on-error)
    network_stats: dict[str, Any] = prev.get("network_stats", {})
    st_item = _extract_first(stats_raw)
    if st_item:
        network_stats = st_item

    # ── Descriptions map (from TaoApp /subnets/about/summaries) ───────────
    desc_map: dict[int, str] = {}
    if isinstance(summaries_raw, list):
        for item in summaries_raw:
            if isinstance(item, dict) and item.get("netuid") is not None:
                nid = int(_safe_float(item["netuid"]))
                title = item.get("title", "")
                subtitle = item.get("subtitle", "")
                desc_map[nid] = f"{title} — {subtitle}" if subtitle else title

    # ── Subnets (from TaoApp /subnet_screener) ────────────────────────────
    # Field reference for TaoApp subnet_screener:
    #   netuid, subnet_name, price, tao_in, alpha_in, alpha_out, alpha_circ,
    #   market_cap_tao, fdv_tao, emission_pct, alpha_emitted_pct,
    #   buy_volume_tao_1d, sell_volume_tao_1d, total_volume_tao_1d,
    #   price_1h_pct_change, price_1d_pct_change, price_7d_pct_change, price_1m_pct_change,
    #   buy_volume_pct_change, sell_volume_pct_change, total_volume_pct_change,
    #   root_prop, alpha_prop, net_volume_tao_1h, net_volume_tao_24h, net_volume_tao_7d,
    #   realized_pnl_tao, unrealized_pnl_tao, ath_60d, atl_60d,
    #   gini_coeff_top_100, hhi, github_repo, subnet_contact, subnet_url,
    #   subnet_website, discord, additional, owner_coldkey, owner_hotkey, symbol
    # On failure: preserve previous cached subnets (stale-on-error)
    prev_subnets: list[dict[str, Any]] = prev.get("subnets", [])
    subnets: list[dict[str, Any]] = []
    if isinstance(screener_raw, list):
        # Sort by market_cap_tao descending (same order as TaoStats pools used)
        screener_sorted = sorted(
            screener_raw,
            key=lambda s: _safe_float(s.get("market_cap_tao", 0)),
            reverse=True,
        )
        print(f"[bittensor] bg: assembling {len(screener_sorted)} subnets from TaoApp screener")
        for subnet in screener_sorted:
            if not isinstance(subnet, dict):
                continue
            netuid = int(_safe_float(subnet.get("netuid", 0)))
            subnets.append({
                "netuid": netuid,
                "name": subnet.get("subnet_name") or f"SN{netuid}",
                "symbol": subnet.get("symbol", ""),
                "description": desc_map.get(netuid, ""),
                "price": _safe_str(subnet.get("price", "0")),
                "market_cap": _safe_str(subnet.get("market_cap_tao", "0")),
                "fdv": _safe_str(subnet.get("fdv_tao", "0")),
                "price_change_1h": _safe_str(subnet.get("price_1h_pct_change", "0")),
                "price_change_24h": _safe_str(subnet.get("price_1d_pct_change", "0")),
                "price_change_7d": _safe_str(subnet.get("price_7d_pct_change", "0")),
                "price_change_30d": _safe_str(subnet.get("price_1m_pct_change", "0")),
                "emission_pct": _safe_str(subnet.get("emission_pct", "0")),
                "tao_in": _safe_str(subnet.get("tao_in", "0")),
                "alpha_in": _safe_str(subnet.get("alpha_in", "0")),
                "alpha_circ": _safe_str(subnet.get("alpha_circ", "0")),
                "volume_24h": _safe_str(subnet.get("total_volume_tao_1d", "0")),
                "buy_volume_24h": _safe_str(subnet.get("buy_volume_tao_1d", "0")),
                "sell_volume_24h": _safe_str(subnet.get("sell_volume_tao_1d", "0")),
                "net_volume_24h": _safe_str(subnet.get("net_volume_tao_24h", "0")),
                "ath_60d": _safe_str(subnet.get("ath_60d", "0")),
                "atl_60d": _safe_str(subnet.get("atl_60d", "0")),
                "root_prop": _safe_str(subnet.get("root_prop", "0")),
                "discord": subnet.get("discord", "") or "",
                "github": subnet.get("github_repo", "") or "",
                "website": subnet.get("subnet_website", subnet.get("subnet_url", "")) or "",
                "twitter": "",
                "seven_day_price_history": [],
            })
    else:
        subnets = prev_subnets
        if prev_subnets:
            print(f"[bittensor] bg: TaoApp screener unavailable — using {len(prev_subnets)} stale subnets from cache")
        else:
            print(f"[bittensor] bg: TaoApp screener unavailable — subnets will be empty")

    result = {
        "tao_price": tao_price,
        "network_stats": network_stats,
        "total_market": total_market,
        "block_number": block_number,
        "subnets": subnets,
        "subnet_count": len(subnets),
        "as_of": datetime.now(timezone.utc).isoformat(),
    }
    print(f"[bittensor] bg: fetch complete — {len(subnets)} subnets, block={block_number}, "
          f"price={tao_price['price']}, fear_greed={total_market['fear_greed_score']}")
    return result


async def _dashboard_refresh_loop() -> None:
    """
    Background coroutine that refreshes the dashboard cache every 5 minutes.
    First run happens after a 5-second startup delay.
    """
    await asyncio.sleep(5)   # let server finish startup
    while True:
        try:
            async with _dashboard_refresh_lock:
                data = await _fetch_dashboard_data()
                _cache_set("dashboard", data)
        except Exception as exc:
            print(f"[bittensor] bg refresh error: {exc}")
        await asyncio.sleep(_DASHBOARD_REFRESH_INTERVAL)


def start_dashboard_refresh_task() -> None:
    """Schedule the background dashboard pre-fetcher (call once at startup)."""
    global _dashboard_bg_task
    try:
        loop = asyncio.get_event_loop()
        _dashboard_bg_task = loop.create_task(_dashboard_refresh_loop())
        print("[bittensor] background dashboard refresh task scheduled")
    except RuntimeError:
        pass  # no event loop yet; task will be created lazily on first request


# ── Debug endpoint ──────────────────────────────────────────────────────────

@router.get("/debug")
async def debug_endpoint():
    result: dict[str, Any] = {
        "api_key_configured": bool(TAOSTATS_API_KEY),
        "api_key_prefix": (TAOSTATS_API_KEY[:4] + "...") if TAOSTATS_API_KEY and len(TAOSTATS_API_KEY) >= 4 else "(not set)",
        "taoapp_key_configured": bool(TAOAPP_API_KEY),
        "taoapp_key_prefix": (TAOAPP_API_KEY[:4] + "...") if TAOAPP_API_KEY and len(TAOAPP_API_KEY) >= 4 else "(not set)",
        "dashboard_cache_age_seconds": None,
        "dashboard_subnet_count": None,
        "test_result": "SKIPPED (no API key)",
        "raw_sample": None,
    }

    entry = _cache.get("dashboard")
    if entry:
        result["dashboard_cache_age_seconds"] = round(time.time() - entry["ts"], 1)
        result["dashboard_subnet_count"] = entry["data"].get("subnet_count", 0)

    if TAOAPP_API_KEY:
        try:
            async with httpx.AsyncClient() as client:
                taoapp_raw = await _taoapp_get(client, "/api/beta/current")
            if taoapp_raw is not None:
                result["taoapp_test_result"] = "OK"
                result["taoapp_price"] = taoapp_raw.get("price")
                result["taoapp_block"] = taoapp_raw.get("max_block_number")
            else:
                result["taoapp_test_result"] = "ERROR: request returned None"
        except Exception as exc:
            result["taoapp_test_result"] = f"ERROR: {exc}"

    if TAOSTATS_API_KEY:
        try:
            async with httpx.AsyncClient() as client:
                raw = await _taostats_get(client, "/api/price/latest/v1", {"asset": "tao"}, retry_on_429=False)
            if raw is not None:
                result["test_result"] = "OK"
                result["raw_sample"] = _extract_first(raw)
            else:
                result["test_result"] = "ERROR: request returned None"
        except Exception as exc:
            result["test_result"] = f"ERROR: {exc}"

    return result


# ── Blocks history endpoint ─────────────────────────────────────────────────

@router.get("/blocks/history")
async def blocks_history_endpoint(
    scale: str = Query("days", pattern="^(days|hours)$"),
    points: int = Query(30, ge=1, le=100),
):
    """Cached for 15 minutes.  Uses only 1-2 TaoStats calls."""
    if not TAOSTATS_API_KEY:
        return JSONResponse(status_code=503, content={"error": "TAOSTATS_API_KEY not configured"})

    cache_key = f"blocks_history_{scale}_{points}"
    cached = _cache_get(cache_key, ttl=900)
    if cached is not None:
        return cached

    expected_per_interval = 7200 if scale == "days" else 300
    frequency = "by_day" if scale == "days" else "by_hour"
    data_points: list[dict[str, Any]] = []

    async with httpx.AsyncClient() as client:
        interval_raw = await _taostats_get(client, "/api/block/interval/v1", {
            "frequency": frequency, "limit": points + 1,
        }, retry_on_429=False)

        interval_data = _extract_data(interval_raw)

        if isinstance(interval_data, list) and len(interval_data) >= 2:
            sorted_data = sorted(interval_data, key=lambda x: x.get("timestamp", ""))
            for i in range(1, len(sorted_data)):
                prev = sorted_data[i - 1]
                curr = sorted_data[i]
                delta = int(_safe_float(curr.get("block_number", 0))) - int(_safe_float(prev.get("block_number", 0)))
                ts = curr.get("timestamp", "")
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    label = dt.strftime("%b %d") if scale == "days" else dt.strftime("%H:%M")
                except Exception:
                    label = ts[:10] if len(ts) >= 10 else ts
                data_points.append({
                    "label": label, "blocks": delta,
                    "expected": expected_per_interval, "timestamp": ts,
                })
        else:
            # Minimal fallback — just 1 extra call for a reference snapshot
            current_raw = await _taostats_get(client, "/api/block/v1", {"limit": 1}, retry_on_429=False)
            current_item = _extract_first(current_raw)
            if current_item:
                current_block = int(_safe_float(current_item.get("block_number", 0)))
                now = datetime.now(timezone.utc)
                interval_delta = timedelta(days=1) if scale == "days" else timedelta(hours=1)
                # Estimate using the expected blocks-per-interval constant
                for i in range(1, min(points, 7) + 1):
                    t = now - (interval_delta * i)
                    est_block = current_block - (expected_per_interval * i)
                    label = t.strftime("%b %d") if scale == "days" else t.strftime("%H:%M")
                    data_points.insert(0, {
                        "label": label,
                        "blocks": expected_per_interval,
                        "expected": expected_per_interval,
                        "timestamp": t.isoformat(),
                    })

    result = {"scale": scale, "expected_per_interval": expected_per_interval, "data": data_points}
    _cache_set(cache_key, result)
    return result


# ── Dashboard endpoint ───────────────────────────────────────────────────────

@router.get("/dashboard")
async def dashboard_endpoint():
    """
    Returns pre-fetched, cached Bittensor dashboard data.

    Data is populated by a background task that respects the TaoStats 5 req/min
    rate limit by spacing its 6 calls 13 s apart (~78 s total per refresh).
    Cache TTL is 5 minutes; the background task refreshes every 5 minutes.
    First data is available ~78 s after server startup.
    """
    if not TAOSTATS_API_KEY:
        return JSONResponse(status_code=503, content={"error": "TAOSTATS_API_KEY not configured"})

    # Ensure the background task is running
    global _dashboard_bg_task
    if _dashboard_bg_task is None or _dashboard_bg_task.done():
        try:
            loop = asyncio.get_event_loop()
            _dashboard_bg_task = loop.create_task(_dashboard_refresh_loop())
        except Exception:
            pass

    cached = _cache_get("dashboard", ttl=_DASHBOARD_CACHE_TTL)
    if cached is not None:
        return cached

    # Cache miss — background task is still populating data (takes ~90s on cold start).
    # Block and poll rather than returning a useless loading placeholder, so the
    # frontend works without any special retry logic.
    poll_deadline = time.time() + 120  # wait up to 2 minutes
    while time.time() < poll_deadline:
        await asyncio.sleep(5)
        cached = _cache_get("dashboard", ttl=_DASHBOARD_CACHE_TTL)
        if cached is not None:
            return cached

    # Still no data after 2 minutes — return a graceful error so the frontend
    # can show something meaningful.
    return JSONResponse(status_code=503, content={
        "error": "Dashboard data unavailable",
        "loading": True,
        "message": "Dashboard data could not be loaded. The server may be rate-limited. Please try again in a minute.",
        "subnets": [],
        "subnet_count": 0,
        "tao_price": {"price": "0", "change_24h": "0"},
        "network_stats": {},
        "block_number": 0,
    })


# ── Metagraph endpoint ───────────────────────────────────────────────────────

@router.get("/subnet/{netuid}/metagraph")
async def metagraph_endpoint(netuid: int):
    """Cached per netuid for 5 minutes."""
    if not TAOSTATS_API_KEY:
        return JSONResponse(status_code=503, content={"error": "TAOSTATS_API_KEY not configured"})

    cache_key = f"metagraph_{netuid}"
    cached = _cache_get(cache_key, ttl=300)
    if cached is not None:
        return cached

    async with httpx.AsyncClient() as client:
        raw = await _taostats_get(client, "/api/metagraph/latest/v1", {
            "netuid": netuid, "limit": 256,
        }, retry_on_429=False)

    if raw is None:
        raise HTTPException(status_code=502, detail="Failed to fetch metagraph from TaoStats")

    data = _extract_data(raw)
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
    """30-day TAO OHLC price history. Cached for 15 minutes."""
    if not TAOSTATS_API_KEY:
        return JSONResponse(status_code=503, content={"error": "TAOSTATS_API_KEY not configured"})

    cached = _cache_get("price_history", ttl=900)
    if cached is not None:
        return cached

    async with httpx.AsyncClient() as client:
        raw = await _taostats_get(client, "/api/price/ohlc/v1", {
            "asset": "tao", "period": "1d", "limit": 30,
        }, retry_on_429=False)

    if raw is None:
        raise HTTPException(status_code=502, detail="Failed to fetch price history from TaoStats")

    data = _extract_data(raw)
    result = {"asset": "tao", "period": "1d", "data": data}
    _cache_set("price_history", result)
    return result
