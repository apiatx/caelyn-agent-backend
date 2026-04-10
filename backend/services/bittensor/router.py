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

STAGE_LABELS = {
    1: "Accumulating", 2: "Early Breakout", 3: "Momentum",
    4: "Distributing", 5: "Declining", 6: "Recovery",
}

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


def _compute_rsi_from_sparkline(prices: list, period: int = 14) -> float:
    if len(prices) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        d = float(prices[i]) - float(prices[i-1])
        gains.append(max(d, 0)); losses.append(max(-d, 0))
    recent_gains = gains[-period:]
    recent_losses = losses[-period:]
    avg_gain = sum(recent_gains) / period
    avg_loss = sum(recent_losses) / period
    if avg_loss < 1e-12:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)


def _compute_sparkline_slope(prices: list) -> float:
    """Returns the normalized slope of the price series as % change per point."""
    pts = [float(p) for p in prices if p is not None]
    n = len(pts)
    if n < 3:
        return 0.0
    xs = list(range(n))
    mean_x = sum(xs) / n
    mean_y = sum(pts) / n
    num = sum((xs[i] - mean_x) * (pts[i] - mean_y) for i in range(n))
    den = sum((xs[i] - mean_x) ** 2 for i in range(n))
    if den < 1e-12 or mean_y < 1e-12:
        return 0.0
    return round((num / den) / mean_y * 100, 3)


def _detect_rotation_stage(subnet: dict) -> tuple[int, str, float]:
    price_1h   = _safe_float(subnet.get("price_change_1h", 0))
    price_24h  = _safe_float(subnet.get("price_change_24h", 0))
    price_7d   = _safe_float(subnet.get("price_change_7d", 0))
    price_30d  = _safe_float(subnet.get("price_change_30d", 0))
    price_pos  = _safe_float(subnet.get("price_vs_ath_60d_pct", 50))
    rsi        = _safe_float(subnet.get("rsi_7d", 50))
    slope      = _safe_float(subnet.get("sparkline_slope", 0))
    buy_ratio  = _safe_float(subnet.get("_buy_ratio", 0.5))
    net_vol    = _safe_float(subnet.get("net_volume_24h", 0))
    realized   = _safe_float(subnet.get("realized_pnl_tao", 0))
    unrealized = _safe_float(subnet.get("unrealized_pnl_tao", 0))

    scores: dict[int, float] = {}

    # Stage 1 — Accumulation
    s1 = 0.0
    if buy_ratio > 0.57: s1 += 30
    elif buy_ratio > 0.52: s1 += 15
    if -6 <= price_24h <= 6: s1 += 25
    if price_pos < 45: s1 += 20
    if net_vol > 0: s1 += 15
    if abs(price_7d) < 12: s1 += 10
    scores[1] = s1

    # Stage 2 — Early Breakout
    s2 = 0.0
    if 4 <= price_24h <= 30: s2 += 35
    if buy_ratio > 0.60: s2 += 25
    if 3 <= price_7d <= 35: s2 += 20
    if price_pos < 68: s2 += 10
    if rsi < 65: s2 += 10
    scores[2] = s2

    # Stage 3 — Markup / Momentum
    s3 = 0.0
    if price_7d > 20: s3 += 30
    if price_24h > 5: s3 += 20
    if buy_ratio > 0.62: s3 += 20
    if rsi > 55: s3 += 15
    if slope > 0.1: s3 += 10
    if price_pos > 45: s3 += 5
    scores[3] = s3

    # Stage 4 — Distribution
    s4 = 0.0
    if price_pos > 70: s4 += 35
    if buy_ratio < 0.50: s4 += 25
    if realized > 30: s4 += 20
    if -5 <= price_24h <= 12: s4 += 10
    if price_7d > 0: s4 += 10
    scores[4] = s4

    # Stage 5 — Decline
    s5 = 0.0
    if price_24h < -5: s5 += 30
    if price_7d < -10: s5 += 25
    if buy_ratio < 0.47: s5 += 20
    if net_vol < 0: s5 += 15
    if slope < -0.1: s5 += 10
    scores[5] = s5

    # Stage 6 — Capitulation / Recovery
    s6 = 0.0
    if price_pos < 20: s6 += 30
    if rsi < 35: s6 += 25
    if price_30d < -20: s6 += 20
    if buy_ratio > 0.50: s6 += 15
    if -5 <= price_24h <= 5: s6 += 10
    scores[6] = s6

    best = max(scores, key=lambda k: scores[k])
    confidence = min(scores[best], 100.0)
    return best, STAGE_LABELS[best], round(confidence, 1)


def _compute_accumulation_score(subnet: dict) -> float:
    price_24h = _safe_float(subnet.get("price_change_24h", 0))
    buy_ratio = _safe_float(subnet.get("_buy_ratio", 0.5))
    price_pos = _safe_float(subnet.get("price_vs_ath_60d_pct", 50))
    net_vol   = _safe_float(subnet.get("net_volume_24h", 0))
    realized  = _safe_float(subnet.get("realized_pnl_tao", 0))
    rsi       = _safe_float(subnet.get("rsi_7d", 50))
    vol_24h   = _safe_float(subnet.get("volume_24h", 0))
    mcap      = _safe_float(subnet.get("market_cap", 1))

    score = 0.0
    if buy_ratio > 0.63: score += 30
    elif buy_ratio > 0.57: score += 18
    elif buy_ratio > 0.52: score += 8
    if abs(price_24h) < 3: score += 25
    elif abs(price_24h) < 7: score += 14
    if price_pos < 30: score += 20
    elif price_pos < 48: score += 11
    if net_vol > 0: score += 12
    if realized < 15: score += 8
    if rsi < 48: score += 5
    return min(round(score, 1), 100.0)


def _compute_network_level_metrics(subnets: list[dict]) -> None:
    """Compute cross-subnet relative value metrics. Mutates subnets in place."""
    emissions = [_safe_float(s.get("emission_pct", 0)) for s in subnets]
    mcaps     = [_safe_float(s.get("market_cap", 0)) for s in subnets]
    tao_ins   = [_safe_float(s.get("tao_in", 0)) for s in subnets]

    avg_em   = (sum(emissions) / len(emissions)) if emissions else 1
    avg_mcap = (sum(m for m in mcaps if m > 0) / max(1, sum(1 for m in mcaps if m > 0)))

    for s, em, mc, ti in zip(subnets, emissions, mcaps, tao_ins):
        if avg_em > 0 and avg_mcap > 0 and mc > 0 and em > 0:
            em_rel = em / avg_em
            mc_rel = mc / avg_mcap
            opportunity = em_rel / mc_rel
        else:
            opportunity = 1.0
        s["emission_opportunity"] = round(opportunity, 3)

        s["pool_depth_ratio"] = round((ti / mc) if mc > 1e-9 else 0, 4)

        alpha_circ = _safe_float(s.get("alpha_circ", 0))
        alpha_in   = _safe_float(s.get("alpha_in", 0))
        s["alpha_circulation_ratio"] = round((alpha_circ / alpha_in) if alpha_in > 0 else 0, 3)


def _parse_hotkey(hotkey_val: Any) -> str:
    if hotkey_val is None:
        return ""
    if isinstance(hotkey_val, dict):
        return str(hotkey_val.get("ss58", hotkey_val.get("address", "")))
    return str(hotkey_val)


# ── Background dashboard pre-fetcher ────────────────────────────────────────

def _compute_signal_scores(subnets: list[dict[str, Any]]) -> None:
    """
    Compute composite signal_score (0-100) for each subnet using min-max
    normalization across all subnets. Mutates each subnet dict in place.

    Components (weights):
      - momentum_score (30%): weighted price changes across timeframes
      - flow_score (25%): buy/(buy+sell) ratio
      - emission_score (20%): emission_pct normalized
      - social_score_component (15%): social_score from TaoApp
      - health_score (10%): 1 - gini_coeff_top_100
    """
    if not subnets:
        return

    # ── Collect raw values ────────────────────────────────────────────────
    momentums: list[float] = []
    flows: list[float] = []
    emissions: list[float] = []
    socials: list[float] = []
    healths: list[float] = []

    for s in subnets:
        # Momentum: weighted combo, positive momentum weighted higher
        ch1h = _safe_float(s.get("price_change_1h", 0))
        ch24h = _safe_float(s.get("price_change_24h", 0))
        ch7d = _safe_float(s.get("price_change_7d", 0))
        raw_mom = 0.1 * ch1h + 0.4 * ch24h + 0.5 * ch7d
        # Weight positive momentum 1.5x
        if raw_mom > 0:
            raw_mom *= 1.5
        momentums.append(raw_mom)

        # Flow: use pre-computed buy_pct (or recompute if not available)
        flow = _safe_float(s.get("buy_pct", 50))
        flows.append(flow)

        # Emission
        emissions.append(_safe_float(s.get("emission_pct", 0)))

        # Social score (derived from latest_total_messages)
        socials.append(_safe_float(s.get("latest_total_messages", 0)))

        # Health: 1 - gini (lower gini = healthier)
        gini = _safe_float(s.get("gini_coeff_top_100", 0.5))
        healths.append((1.0 - min(gini, 1.0)) * 100)

    # ── Min-max normalization helper ──────────────────────────────────────
    def _minmax(vals: list[float]) -> list[float]:
        lo, hi = min(vals), max(vals)
        rng = hi - lo
        if rng < 1e-9:
            return [50.0] * len(vals)
        return [max(0.0, min(100.0, (v - lo) / rng * 100)) for v in vals]

    norm_mom = _minmax(momentums)
    # flows are already 0-100 range (buy percentage)
    norm_flow = flows
    norm_em = _minmax(emissions)
    norm_soc = _minmax(socials)
    norm_health = healths  # already 0-100

    # ── Assign scores ─────────────────────────────────────────────────────
    for i, s in enumerate(subnets):
        m = norm_mom[i]
        f = norm_flow[i]
        e = norm_em[i]
        sc = norm_soc[i]
        h = norm_health[i]

        signal = 0.30 * m + 0.25 * f + 0.20 * e + 0.15 * sc + 0.10 * h
        signal = max(0.0, min(100.0, signal))

        s["signal_score"] = round(signal, 1)
        s["signal_breakdown"] = {
            "momentum_score": round(m, 1),
            "flow_score": round(f, 1),
            "emission_score": round(e, 1),
            "social_score": round(sc, 1),
            "health_score": round(h, 1),
        }

        # ── price_vs_ath_60d_pct ──────────────────────────────────────────
        price = _safe_float(s.get("price", 0))
        ath = _safe_float(s.get("ath_60d", 0))
        atl = _safe_float(s.get("atl_60d", 0))
        rng = ath - atl
        if rng > 0.000001:
            pct = (price - atl) / rng * 100
        else:
            pct = 50.0
        s["price_vs_ath_60d_pct"] = round(max(0.0, min(100.0, pct)), 1)


async def _fetch_dashboard_data() -> dict:
    """
    Fetches dashboard data using TaoApp as the primary source (10 req/min)
    and TaoStats for network stats (5 req/min).

    TaoApp calls (9, run in parallel):
      - /api/beta/current          → TAO price, block_number, market_cap
      - /api/beta/subnet_screener  → all subnets with price/volume/social
      - /api/beta/analytics/macro/fear_greed/current → fear/greed index
      - /api/beta/subnets/about/summaries → subnet descriptions
      - /api/beta/subnets/sparklines?hours=168&points=24 → 7d sparklines
      - /api/beta/analytics/subnets/social/summary → social analytics
      - /api/beta/price-sustainability → tao_needed_to_sustain
      - /api/beta/subnet_tags → tags per subnet
      - /api/beta/analytics/macro/root_claim_stats/current → root claim stats

    TaoStats calls (1, in parallel):
      - /api/stats/latest/v1       → network staking/issuance stats

    Stale-on-error: if individual endpoints fail, the previous cached values
    are preserved rather than being overwritten with zeros.
    """
    print("[bittensor] background fetch: starting dashboard data pull (TaoApp primary)")

    # Load previous cached data for stale-on-error fallback
    prev_entry = _cache.get("dashboard")
    prev: dict[str, Any] = prev_entry["data"] if prev_entry else {}

    async with httpx.AsyncClient() as client:
        # Fire all calls in parallel — TaoApp 10 req/min limit is comfortable
        (
            current_raw,
            screener_raw,
            fear_greed_raw,
            summaries_raw,
            sparklines_raw,
            social_raw,
            sustain_raw,
            tags_raw,
            root_claim_raw,
            stats_raw,
        ) = await asyncio.gather(
            _taoapp_get(client, "/api/beta/current"),
            _taoapp_get(client, "/api/beta/subnet_screener"),
            _taoapp_get(client, "/api/beta/analytics/macro/fear_greed/current"),
            _taoapp_get(client, "/api/beta/subnets/about/summaries"),
            _taoapp_get(client, "/api/beta/subnets/sparklines", {"hours": 168, "points": 24}),
            _taoapp_get(client, "/api/beta/analytics/subnets/social/summary"),
            _taoapp_get(client, "/api/beta/price-sustainability"),
            _taoapp_get(client, "/api/beta/subnet_tags"),
            _taoapp_get(client, "/api/beta/analytics/macro/root_claim_stats/current"),
            _taostats_get(client, "/api/stats/latest/v1", retry_on_429=False),
        )

    print(f"[bittensor] bg: current={'OK' if current_raw else 'FAIL'} "
          f"screener={'OK,' + str(len(screener_raw)) + ' subnets' if isinstance(screener_raw, list) else 'FAIL'} "
          f"fear_greed={'OK' if fear_greed_raw else 'FAIL'} "
          f"summaries={'OK,' + str(len(summaries_raw)) + ' items' if isinstance(summaries_raw, list) else 'FAIL'} "
          f"sparklines={'OK' if sparklines_raw else 'FAIL'} "
          f"social={'OK' if social_raw else 'FAIL'} "
          f"sustain={'OK' if sustain_raw else 'FAIL'} "
          f"tags={'OK' if tags_raw else 'FAIL'} "
          f"root_claim={'OK' if root_claim_raw else 'FAIL'} "
          f"stats={'OK' if stats_raw else 'FAIL (non-fatal)'}")

    # ── TAO price & block (from TaoApp /current) ──────────────────────────
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
    network_stats: dict[str, Any] = prev.get("network_stats", {})
    st_item = _extract_first(stats_raw)
    if st_item:
        network_stats = st_item

    # ── Root claim stats ──────────────────────────────────────────────────
    root_claim_stats: dict[str, Any] = prev.get("root_claim_stats", {})
    if isinstance(root_claim_raw, dict):
        root_claim_stats = root_claim_raw
    elif isinstance(root_claim_raw, list) and root_claim_raw:
        root_claim_stats = root_claim_raw[0] if isinstance(root_claim_raw[0], dict) else {}

    # ── Build lookup maps for enrichment ──────────────────────────────────

    # Descriptions
    desc_map: dict[int, str] = {}
    if isinstance(summaries_raw, list):
        for item in summaries_raw:
            if isinstance(item, dict) and item.get("netuid") is not None:
                nid = int(_safe_float(item["netuid"]))
                title = item.get("title", "")
                subtitle = item.get("subtitle", "")
                desc_map[nid] = f"{title} — {subtitle}" if subtitle else title

    # Sparklines map: netuid → sparkline array
    sparkline_map: dict[int, list] = {}
    if isinstance(sparklines_raw, list):
        for item in sparklines_raw:
            if isinstance(item, dict) and item.get("netuid") is not None:
                nid = int(_safe_float(item["netuid"]))
                sparkline_map[nid] = item.get("sparkline", [])

    # Social summary map: netuid → social dict
    social_map: dict[int, dict] = {}
    if isinstance(social_raw, list):
        for item in social_raw:
            if isinstance(item, dict) and item.get("netuid") is not None:
                nid = int(_safe_float(item["netuid"]))
                social_map[nid] = item

    # Sustainability map: netuid → sustain dict
    # sustain_raw is {"data": [...]} or possibly a flat list
    sustain_map: dict[int, dict] = {}
    sustain_list: list = []
    if isinstance(sustain_raw, dict):
        sustain_list = sustain_raw.get("data", [])
    elif isinstance(sustain_raw, list):
        sustain_list = sustain_raw  # fallback
    for item in sustain_list:
        if isinstance(item, dict):
            nid = item.get("netuid")
            if nid is not None:
                sustain_map[int(nid)] = item

    # Tags map: netuid → tags list
    tags_map: dict[int, list] = {}
    if isinstance(tags_raw, list):
        for item in tags_raw:
            if isinstance(item, dict) and item.get("netuid") is not None:
                nid = int(_safe_float(item["netuid"]))
                raw_tags = item.get("tags", [])
                if isinstance(raw_tags, list):
                    tags_map[nid] = raw_tags
                elif isinstance(raw_tags, str):
                    tags_map[nid] = [t.strip() for t in raw_tags.split(",") if t.strip()]

    # ── Subnets (from TaoApp /subnet_screener) ────────────────────────────
    prev_subnets: list[dict[str, Any]] = prev.get("subnets", [])
    subnets: list[dict[str, Any]] = []
    if isinstance(screener_raw, list):
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

            # Social enrichment
            soc = social_map.get(netuid, {})
            # Sustainability enrichment
            sus = sustain_map.get(netuid, {})

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
                # Pass-through fields from screener
                "gini_coeff_top_100": _safe_float(subnet.get("gini_coeff_top_100", 0)),
                "hhi": _safe_float(subnet.get("hhi", 0)),
                "realized_pnl_tao": _safe_str(subnet.get("realized_pnl_tao", "0")),
                "unrealized_pnl_tao": _safe_str(subnet.get("unrealized_pnl_tao", "0")),
                # Links
                "discord": subnet.get("discord", "") or "",
                "github": subnet.get("github_repo", "") or "",
                "website": subnet.get("subnet_website", subnet.get("subnet_url", "")) or "",
                "twitter": "",
                # Sparklines
                "seven_day_price_history": sparkline_map.get(netuid, []),
                # Social enrichment
                "latest_unique_authors": int(_safe_float(soc.get("latest_unique_authors", 0))),
                "latest_total_messages": int(_safe_float(soc.get("latest_total_messages", 0))),
                "total_analyses_24h": int(_safe_float(soc.get("total_analyses_24h", 0))),
                "last_analysis_timestamp": soc.get("last_analysis_timestamp", None),
                # Keep social_score as a derived value for signal scoring
                "social_score": _safe_float(soc.get("latest_total_messages", 0)),
                # Sustainability
                "tao_needed_to_sustain": _safe_float(sus.get("tao_needed", 0)),
                # Tags
                "tags": tags_map.get(netuid, []),
            })
    else:
        subnets = prev_subnets
        if prev_subnets:
            print(f"[bittensor] bg: TaoApp screener unavailable — using {len(prev_subnets)} stale subnets from cache")
        else:
            print(f"[bittensor] bg: TaoApp screener unavailable — subnets will be empty")

    # ── Pre-compute rotation intelligence fields ─────────────────────────
    for s in subnets:
        buy = _safe_float(s.get("buy_volume_24h", 0))
        sell = _safe_float(s.get("sell_volume_24h", 0))
        total = buy + sell
        s["_buy_ratio"] = (buy / total) if total > 0 else 0.5
        s["buy_pct"] = round(s["_buy_ratio"] * 100, 1)
        sparkline = s.get("seven_day_price_history", [])
        s["rsi_7d"] = _compute_rsi_from_sparkline(sparkline)
        s["sparkline_slope"] = _compute_sparkline_slope(sparkline)

    _compute_network_level_metrics(subnets)

    for s in subnets:
        stage, label, conf = _detect_rotation_stage(s)
        s["rotation_stage"] = stage
        s["rotation_stage_label"] = label
        s["rotation_stage_confidence"] = conf

    for s in subnets:
        s["accumulation_score"] = _compute_accumulation_score(s)

    for s in subnets:
        s.pop("_buy_ratio", None)

    # ── Compute signal scores ─────────────────────────────────────────────
    _compute_signal_scores(subnets)

    # Sort by signal_score descending
    subnets.sort(key=lambda s: s.get("signal_score", 0), reverse=True)

    result = {
        "tao_price": tao_price,
        "network_stats": network_stats,
        "total_market": total_market,
        "block_number": block_number,
        "root_claim_stats": root_claim_stats,
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


# ── Signal endpoint ─────────────────────────────────────────────────────────

@router.get("/subnets/signal")
async def signal_endpoint(limit: int = Query(30, ge=5, le=100)):
    """Top N subnets by signal_score from dashboard cache."""
    cached = _cache_get("dashboard", ttl=_DASHBOARD_CACHE_TTL)
    if cached is None:
        raise HTTPException(status_code=503, detail="Data loading")
    subnets = sorted(
        cached.get("subnets", []),
        key=lambda s: s.get("signal_score", 0),
        reverse=True,
    )
    return {"subnets": subnets[:limit], "as_of": cached.get("as_of")}


# ── Per-subnet social endpoint ──────────────────────────────────────────────

@router.get("/subnet/{netuid}/social")
async def subnet_social_endpoint(netuid: int):
    """Fresh social analytics for a specific subnet. Cached 10 min per netuid."""
    cache_key = f"social_{netuid}"
    cached = _cache_get(cache_key, ttl=600)
    if cached is not None:
        return cached

    async with httpx.AsyncClient() as client:
        raw = await _taoapp_get(
            client,
            f"/api/beta/analytics/subnets/social/{netuid}/latest",
        )

    if raw is None:
        raise HTTPException(status_code=502, detail="Failed to fetch social data")

    result = raw if isinstance(raw, dict) else {"data": raw}
    _cache_set(cache_key, result)
    return result


# ── Sparklines endpoint ─────────────────────────────────────────────────────

@router.get("/subnets/sparklines")
async def sparklines_endpoint():
    """7-day price sparklines for all subnets. Cached 15 min."""
    cached = _cache_get("sparklines_all", ttl=900)
    if cached is not None:
        return cached

    async with httpx.AsyncClient() as client:
        raw = await _taoapp_get(
            client,
            "/api/beta/subnets/sparklines",
            {"hours": 168, "points": 24},
        )

    if raw is None:
        raise HTTPException(status_code=502, detail="Failed to fetch sparklines")

    result = {"data": raw}
    _cache_set("sparklines_all", result)
    return result


@router.get("/rotation/intel")
async def rotation_intel_endpoint():
    cached = _cache_get("dashboard", ttl=_DASHBOARD_CACHE_TTL)
    if not cached:
        raise HTTPException(status_code=503, detail="Data loading, try again shortly")

    subnets = cached.get("subnets", [])

    by_stage: dict[int, list] = {1: [], 2: [], 3: [], 4: [], 5: [], 6: []}
    for s in subnets:
        stage = s.get("rotation_stage", 0)
        if stage in by_stage:
            by_stage[stage].append(s)

    for stage_list in by_stage.values():
        stage_list.sort(key=lambda x: x.get("rotation_stage_confidence", 0), reverse=True)

    opportunities = [
        s for s in subnets
        if s.get("rotation_stage", 0) in (1, 2)
    ]
    opportunities.sort(key=lambda x: x.get("accumulation_score", 0), reverse=True)

    emission_plays = sorted(subnets, key=lambda x: x.get("emission_opportunity", 1.0), reverse=True)[:20]

    oversold  = sorted([s for s in subnets if s.get("rsi_7d", 50) < 35], key=lambda x: x.get("rsi_7d", 50))[:10]
    overbought = sorted([s for s in subnets if s.get("rsi_7d", 50) > 70], key=lambda x: x.get("rsi_7d", 50), reverse=True)[:10]

    stage_summary = {
        stage: {
            "count": len(lst),
            "label": STAGE_LABELS.get(stage, "Unknown"),
            "avg_signal": round(sum(s.get("signal_score", 0) for s in lst) / max(len(lst), 1), 1)
        }
        for stage, lst in by_stage.items()
    }

    return {
        "by_stage": by_stage,
        "opportunities": opportunities,
        "emission_plays": emission_plays,
        "oversold": oversold,
        "overbought": overbought,
        "stage_summary": stage_summary,
        "as_of": cached.get("as_of"),
    }


@router.get("/subnets/dynamic-history")
async def dynamic_history_endpoint(netuids: str = Query("1,2,3,5,9,18,19,64")):
    """
    Returns 24h of hourly dynamic info for requested subnets.
    Uses free TaoApp /api/beta/analytics/dynamic-info/aggregated endpoint.
    Cached 15 minutes.
    """
    cache_key = f"dynamic_history_{netuids}"
    cached = _cache_get(cache_key, ttl=900)
    if cached is not None:
        return cached

    from datetime import datetime, timedelta, timezone
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=24)

    async with httpx.AsyncClient() as client:
        raw = await _taoapp_get(client, "/api/beta/analytics/dynamic-info/aggregated", {
            "interval": "1hour",
            "netuid": netuids,
            "start": start_dt.isoformat(),
            "end": end_dt.isoformat(),
            "page_size": 500,
        })

    data = raw.get("data", []) if isinstance(raw, dict) else (raw or [])
    result = {"data": data, "netuids": netuids, "as_of": end_dt.isoformat()}
    _cache_set(cache_key, result)
    return result
