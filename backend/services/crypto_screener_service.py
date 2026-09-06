"""Canonical backend-owned crypto screener rows and persisted daily history."""
from __future__ import annotations

import asyncio
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from data.pg_storage import _get_conn, _put_conn

CURRENT_TTL_SECONDS = 300
HISTORY_POINTS = 365
HISTORY_RATE_SECONDS = 3.05

_DDL_APPLIED = False
_refresh_lock = asyncio.Lock()
_current_refresh_task: asyncio.Task | None = None


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _round(value: float | None, digits: int = 6) -> float | None:
    return round(value, digits) if value is not None and math.isfinite(value) else None


def _normalized_identity(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]", "", (value or "").casefold())


def resolve_coingecko_identity(cmc_asset: dict, coin_list: list[dict]) -> tuple[str | None, str | None]:
    """Resolve identity from deterministic provider-native evidence only."""
    symbol = _normalized_identity(cmc_asset.get("symbol"))
    name = _normalized_identity(cmc_asset.get("name"))
    slug = _normalized_identity(cmc_asset.get("slug"))
    all_ids = sorted({str(coin["id"]) for coin in coin_list if coin.get("id")})

    slug_matches = [coin_id for coin_id in all_ids if _normalized_identity(coin_id) == slug]
    if slug and len(slug_matches) == 1:
        return slug_matches[0], None

    cmc_address = _normalized_contract(
        (cmc_asset.get("platform") or {}).get("token_address")
    )
    if cmc_address:
        contract_matches = sorted({
            str(coin["id"])
            for coin in coin_list
            if coin.get("id") and cmc_address in {
                _normalized_contract(address)
                for address in (coin.get("platforms") or {}).values()
            }
        })
        if len(contract_matches) == 1:
            return contract_matches[0], None

    symbol_matches = [
        coin for coin in coin_list
        if _normalized_identity(coin.get("symbol")) == symbol
        and coin.get("id")
    ]
    exact_name_ids = sorted({
        str(coin["id"]) for coin in symbol_matches
        if _normalized_identity(coin.get("name")) == name
    })
    if len(exact_name_ids) == 1:
        return exact_name_ids[0], None

    symbol_ids = sorted({str(coin["id"]) for coin in symbol_matches})
    if len(symbol_ids) == 1:
        return symbol_ids[0], None
    if symbol_ids:
        return None, f"ambiguous_symbol:{len(symbol_ids)}_candidates"
    return None, "no_symbol_candidate"


def _normalized_contract(value: Any) -> str:
    return str(value or "").strip().casefold()


def normalize_completed_history(raw: dict, now: datetime | None = None) -> list[dict]:
    """Join CoinGecko price/volume timestamps and exclude the current UTC day."""
    today = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).date()
    prices: dict[str, float] = {}
    volumes: dict[str, float] = {}
    for timestamp, value in raw.get("prices", []) if isinstance(raw, dict) else []:
        date = datetime.fromtimestamp(timestamp / 1000, timezone.utc).date()
        number = _finite(value)
        if date < today and number is not None:
            prices[date.isoformat()] = number
    for timestamp, value in raw.get("total_volumes", []) if isinstance(raw, dict) else []:
        date = datetime.fromtimestamp(timestamp / 1000, timezone.utc).date()
        number = _finite(value)
        if date < today and number is not None:
            volumes[date.isoformat()] = number
    return [
        {"date": date, "close": prices[date], "volume": volumes.get(date)}
        for date in sorted(prices)[-HISTORY_POINTS:]
    ]


def merge_history(existing: list[dict], incoming: list[dict]) -> list[dict]:
    merged = {row["date"]: row for row in existing if row.get("date")}
    merged.update({row["date"]: row for row in incoming if row.get("date")})
    return [merged[key] for key in sorted(merged)[-HISTORY_POINTS:]]


def _sma_at(closes: list[float], period: int, end: int | None = None) -> float | None:
    end = len(closes) if end is None else end
    if end < period:
        return None
    values = closes[end - period:end]
    return sum(values) / period if len(values) == period else None


def compute_technical_metrics(
    history: list[dict],
    current_price: Any,
    current_cg_volume: Any = None,
) -> dict:
    """Compute deterministic trend, breakout, hold, and 7-day volume metrics."""
    closes = [_finite(row.get("close")) for row in history]
    volumes = [_finite(row.get("volume")) for row in history]
    if any(value is None for value in closes):
        closes = []
    price = _finite(current_price)
    out: dict[str, Any] = {}
    complete = len(closes) >= 205 and price is not None

    for period in (50, 150, 200):
        sma = _sma_at(closes, period) if complete else None
        prior_sma = _sma_at(closes, period, len(closes) - 5) if complete else None
        above = price > sma if sma is not None else None
        out[f"sma_{period}"] = _round(sma)
        out[f"pct_vs_sma_{period}"] = _round((price / sma - 1) * 100) if sma else None
        out[f"above_sma_{period}"] = above
        out[f"sma_{period}_rising"] = sma > prior_sma if sma is not None and prior_sma is not None else None

        prior_above: list[bool] = []
        if complete:
            for index in range(len(closes) - 3, len(closes)):
                daily_sma = _sma_at(closes, period, index + 1)
                if daily_sma is None:
                    prior_above = []
                    break
                prior_above.append(closes[index] > daily_sma)
        fresh = bool(above and prior_above and not all(prior_above)) if above is not None else None
        holding = bool(above and len(prior_above) == 3 and all(prior_above) and not fresh) if above is not None else None
        out[f"fresh_breakout_{period}"] = fresh
        out[f"holding_above_{period}"] = holding

    smas = [out.get(f"sma_{period}") for period in (50, 150, 200)]
    above_values = [out.get(f"above_sma_{period}") for period in (50, 150, 200)]
    out["above_all_3"] = all(above_values) if complete else None
    out["bullish_ma_stack"] = bool(smas[0] > smas[1] > smas[2]) if complete and all(v is not None for v in smas) else None

    if not complete:
        out["setup_label"] = None
    elif out["fresh_breakout_200"]:
        out["setup_label"] = "200D BREAKOUT"
    elif out["fresh_breakout_150"]:
        out["setup_label"] = "150D BREAKOUT"
    elif out["fresh_breakout_50"]:
        out["setup_label"] = "50D BREAKOUT"
    elif out["above_all_3"] and out["bullish_ma_stack"]:
        out["setup_label"] = "ABOVE ALL · BULLISH STACK"
    elif out["above_all_3"]:
        out["setup_label"] = "ABOVE ALL · HOLD"
    elif out["above_sma_200"]:
        out["setup_label"] = "ABOVE 200D"
    else:
        out["setup_label"] = "BELOW 200D"

    valid_volumes = [value for value in volumes if value is not None]
    if len(valid_volumes) >= 14:
        latest = valid_volumes[-7:]
        previous = valid_volumes[-14:-7]
        latest_avg = sum(latest) / 7
        previous_avg = sum(previous) / 7
        out["volume_delta_7d_pct"] = _round((latest_avg / previous_avg - 1) * 100) if previous_avg else None
        cg_volume = _finite(current_cg_volume)
        out["vol_x_7d"] = _round(cg_volume / latest_avg) if cg_volume is not None and latest_avg else None
    else:
        out["volume_delta_7d_pct"] = None
        out["vol_x_7d"] = None
    return out


def _ensure_table() -> bool:
    global _DDL_APPLIED
    if _DDL_APPLIED:
        return True
    conn = _get_conn("crypto_screener:ensure")
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.crypto_screener_cache (
                    cmc_id BIGINT PRIMARY KEY,
                    coingecko_id TEXT,
                    symbol TEXT NOT NULL,
                    name TEXT NOT NULL,
                    slug TEXT,
                    identity_status TEXT NOT NULL DEFAULT 'unresolved',
                    daily_history JSONB NOT NULL DEFAULT '[]'::jsonb,
                    derived_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
                    current_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
                    history_updated_at TIMESTAMPTZ,
                    current_updated_at TIMESTAMPTZ
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_crypto_screener_current ON public.crypto_screener_cache (current_updated_at DESC)")
        conn.commit()
        _DDL_APPLIED = True
        return True
    except Exception as exc:
        conn.rollback()
        print(f"[CRYPTO_SCREENER] table initialization failed: {exc}")
        return False
    finally:
        _put_conn(conn)


def _load_records() -> list[dict]:
    if not _ensure_table():
        return []
    conn = _get_conn("crypto_screener:load")
    if conn is None:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT cmc_id, coingecko_id, symbol, name, slug, identity_status,
                       daily_history, derived_metrics, current_snapshot,
                       history_updated_at, current_updated_at
                FROM public.crypto_screener_cache ORDER BY cmc_id
            """)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        _put_conn(conn)


def _load_current_records() -> list[dict]:
    """Load only request-serving columns; daily histories stay off the page path."""
    if not _ensure_table():
        return []
    conn = _get_conn("crypto_screener:load_current")
    if conn is None:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT cmc_id, coingecko_id, symbol, name, slug, identity_status,
                       current_snapshot, history_updated_at, current_updated_at
                FROM public.crypto_screener_cache ORDER BY cmc_id
            """)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        _put_conn(conn)


def _upsert_record(record: dict) -> None:
    from psycopg2.extras import Json
    if not _ensure_table():
        raise RuntimeError("crypto screener persistence unavailable")
    conn = _get_conn("crypto_screener:upsert")
    if conn is None:
        raise RuntimeError("crypto screener persistence unavailable")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO public.crypto_screener_cache
                    (cmc_id, coingecko_id, symbol, name, slug, identity_status,
                     daily_history, derived_metrics, history_updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (cmc_id) DO UPDATE SET
                    coingecko_id=EXCLUDED.coingecko_id, symbol=EXCLUDED.symbol,
                    name=EXCLUDED.name, slug=EXCLUDED.slug,
                    identity_status=EXCLUDED.identity_status,
                    daily_history=EXCLUDED.daily_history,
                    derived_metrics=EXCLUDED.derived_metrics,
                    history_updated_at=COALESCE(EXCLUDED.history_updated_at, crypto_screener_cache.history_updated_at)
            """, (
                record["cmc_id"], record.get("coingecko_id"), record["symbol"],
                record["name"], record.get("slug"), record.get("identity_status", "unresolved"),
                Json(record.get("daily_history", [])), Json(record.get("derived_metrics", {})),
                record.get("history_updated_at"),
            ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_conn(conn)


def _upsert_records(records: list[dict]) -> None:
    from psycopg2.extras import Json, execute_batch
    if not records:
        return
    if not _ensure_table():
        raise RuntimeError("crypto screener persistence unavailable")
    conn = _get_conn("crypto_screener:upsert_batch")
    if conn is None:
        raise RuntimeError("crypto screener persistence unavailable")
    sql = """
        INSERT INTO public.crypto_screener_cache
            (cmc_id, symbol, name, slug, current_snapshot, current_updated_at)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (cmc_id) DO UPDATE SET
            symbol=EXCLUDED.symbol, name=EXCLUDED.name, slug=EXCLUDED.slug,
            current_snapshot=EXCLUDED.current_snapshot,
            current_updated_at=COALESCE(EXCLUDED.current_updated_at, crypto_screener_cache.current_updated_at)
    """
    values = [(
        record["cmc_id"], record["symbol"], record["name"], record.get("slug"),
        Json(record.get("current_snapshot", {})), record.get("current_updated_at"),
    ) for record in records]
    try:
        with conn.cursor() as cur:
            execute_batch(cur, sql, values, page_size=100)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_conn(conn)


def _current_row(asset: dict, record: dict | None, as_of: str, current_cg_market: dict | None = None) -> dict:
    quote = (asset.get("quote") or {}).get("USD") or {}
    price = _finite(quote.get("price"))
    market_cap = _finite(quote.get("market_cap"))
    volume = _finite(quote.get("volume_24h"))
    history = (record or {}).get("daily_history") or []
    cg_market = current_cg_market or {}
    technical = compute_technical_metrics(history, price, cg_market.get("total_volume"))
    row = {
        "rank": asset.get("cmc_rank"),
        "cmc_id": asset.get("id"),
        "coingecko_id": (record or {}).get("coingecko_id"),
        "symbol": asset.get("symbol"),
        "name": asset.get("name"),
        "slug": asset.get("slug"),
        "price": price,
        "change_1h_pct": _finite(quote.get("percent_change_1h")),
        "change_24h_pct": _finite(quote.get("percent_change_24h")),
        "change_7d_pct": _finite(quote.get("percent_change_7d")),
        "change_30d_pct": _finite(quote.get("percent_change_30d")),
        "market_cap": market_cap,
        "fdv": _finite(quote.get("fully_diluted_market_cap")),
        "volume_24h": volume,
        "circulating_supply": _finite(asset.get("circulating_supply")),
        "total_supply": _finite(asset.get("total_supply")),
        "ath_drawdown_pct": _finite(cg_market.get("ath_change_percentage")),
        "cycle_low_move_pct": None,
        "ai_project": None,
        "sentiment": None,
        "x_score": None,
        **technical,
        "volume_change_24h_pct": _finite(quote.get("volume_change_24h")),
        "volume_to_market_cap_pct": _round(volume / market_cap * 100) if volume is not None and market_cap else None,
        "current_data_as_of": as_of,
        "technical_data_as_of": (
            (record or {}).get("history_updated_at").isoformat()
            if getattr((record or {}).get("history_updated_at"), "isoformat", None) else None
        ),
    }
    return row


class CryptoScreenerService:
    def __init__(self, cmc_provider, coingecko_provider):
        self.cmc = cmc_provider
        self.coingecko = coingecko_provider

    async def get_screener(self) -> dict:
        records = await asyncio.to_thread(_load_current_records)
        newest = max((r.get("current_updated_at") for r in records if r.get("current_updated_at")), default=None)
        now = datetime.now(timezone.utc)
        if newest and newest >= now - timedelta(seconds=CURRENT_TTL_SECONDS):
            return _snapshot_response(records, "lkg", refreshing=False)

        persisted_rows = [r for r in records if r.get("current_snapshot")]
        if persisted_rows:
            refreshing = _schedule_current_refresh(self)
            return _snapshot_response(records, "stale_lkg", refreshing=refreshing)

        return await self._refresh_current()

    async def _refresh_current(self) -> dict:
        """Refresh current rows once, re-checking freshness under ownership."""
        async with _refresh_lock:
            records = await asyncio.to_thread(_load_records)
            newest = max(
                (r.get("current_updated_at") for r in records if r.get("current_updated_at")),
                default=None,
            )
            now = datetime.now(timezone.utc)
            if newest and newest >= now - timedelta(seconds=CURRENT_TTL_SECONDS):
                return _snapshot_response(records, "lkg", refreshing=False)

            listings, cg_markets = await asyncio.gather(
                self.cmc.get_listings_latest(100) if self.cmc else asyncio.sleep(0, result=[]),
                self.coingecko.get_top_coins(100) if self.coingecko else asyncio.sleep(0, result=[]),
            )
            if not listings:
                return _snapshot_response(records, "stale_lkg", refreshing=False)

            persisted = {record["cmc_id"]: record for record in records}
            cg_market_by_id = {
                row.get("id"): row
                for row in cg_markets if isinstance(row, dict) and row.get("id")
            }
            refreshed_at = datetime.now(timezone.utc)
            as_of = refreshed_at.isoformat()
            rows = []
            writes = []
            for asset in listings[:100]:
                cmc_id = asset.get("id")
                record = persisted.get(cmc_id)
                row = _current_row(
                    asset,
                    record,
                    as_of,
                    cg_market_by_id.get((record or {}).get("coingecko_id")),
                )
                rows.append(row)
                writes.append({
                    "cmc_id": cmc_id,
                    "symbol": asset.get("symbol") or "",
                    "name": asset.get("name") or "",
                    "slug": asset.get("slug"),
                    "current_snapshot": row,
                    "current_updated_at": refreshed_at,
                })
            await asyncio.to_thread(_upsert_records, writes)
            return {
                "as_of": as_of,
                "history_as_of": _history_as_of(records),
                "rows": rows,
                "source": "live",
                "refreshing": False,
            }

    async def hydrate_history(self, full: bool = False) -> dict:
        if not self.cmc or not self.coingecko:
            return {"ok": False, "error": "CMC and CoinGecko providers are required"}
        listings, coin_list, cg_markets = await asyncio.gather(
            self.cmc.get_listings_latest(100),
            self.coingecko.get_coin_list(),
            self.coingecko.get_top_coins(100),
        )
        cg_volume_by_id = {
            row.get("id"): row.get("total_volume")
            for row in cg_markets if isinstance(row, dict) and row.get("id")
        }
        records = {r["cmc_id"]: r for r in await asyncio.to_thread(_load_records)}
        result = {"ok": True, "requested": 0, "updated": 0, "unresolved": []}
        for index, asset in enumerate(listings[:100]):
            cmc_id = asset.get("id")
            record = records.get(cmc_id) or {}
            cg_id = record.get("coingecko_id")
            status = record.get("identity_status")
            if not cg_id:
                cg_id, status = resolve_coingecko_identity(asset, coin_list)
            if not cg_id:
                result["unresolved"].append({"cmc_id": cmc_id, "symbol": asset.get("symbol"), "reason": status})
                await asyncio.to_thread(_upsert_record, {
                    "cmc_id": cmc_id, "symbol": asset.get("symbol") or "", "name": asset.get("name") or "",
                    "slug": asset.get("slug"), "identity_status": status or "unresolved",
                    "daily_history": record.get("daily_history", []),
                    "derived_metrics": record.get("derived_metrics", {}),
                })
                continue
            days = 365 if full or len(record.get("daily_history", [])) < 205 else 10
            raw = await self.coingecko.get_market_chart(cg_id, days=days)
            result["requested"] += 1
            incoming = normalize_completed_history(raw)
            history = merge_history(record.get("daily_history", []), incoming)
            quote = (asset.get("quote") or {}).get("USD") or {}
            derived = compute_technical_metrics(
                history, quote.get("price"), cg_volume_by_id.get(cg_id)
            )
            await asyncio.to_thread(_upsert_record, {
                "cmc_id": cmc_id, "coingecko_id": cg_id, "symbol": asset.get("symbol") or "",
                "name": asset.get("name") or "", "slug": asset.get("slug"), "identity_status": "resolved",
                "daily_history": history, "derived_metrics": derived,
                "history_updated_at": datetime.now(timezone.utc),
            })
            result["updated"] += 1
            if index < len(listings[:100]) - 1:
                await asyncio.sleep(HISTORY_RATE_SECONDS)
        return result


def _history_as_of(records: list[dict]) -> str | None:
    newest = max((r.get("history_updated_at") for r in records if r.get("history_updated_at")), default=None)
    return newest.isoformat() if newest else None


def _snapshot_response(records: list[dict], source: str, refreshing: bool) -> dict:
    rows = [r.get("current_snapshot") for r in records if r.get("current_snapshot")]
    rows.sort(key=lambda row: row.get("rank") or 10**9)
    newest = max(
        (r.get("current_updated_at") for r in records if r.get("current_updated_at")),
        default=None,
    )
    return {
        "as_of": newest.isoformat() if newest else None,
        "history_as_of": _history_as_of(records),
        "rows": rows[:100],
        "source": source,
        "refreshing": refreshing,
    }


def _schedule_current_refresh(service: CryptoScreenerService) -> bool:
    global _current_refresh_task
    if _current_refresh_task is not None and not _current_refresh_task.done():
        return True
    _current_refresh_task = asyncio.create_task(service._refresh_current())

    def _report_failure(task: asyncio.Task) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            print(f"[CRYPTO_SCREENER] background current refresh failed: {exc}")

    _current_refresh_task.add_done_callback(_report_failure)
    return True


async def daily_crypto_history_loop(service: CryptoScreenerService) -> None:
    """Run at 00:30 UTC; startup only schedules the next normal cadence."""
    while True:
        now = datetime.now(timezone.utc)
        target = now.replace(hour=0, minute=30, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        try:
            result = await service.hydrate_history(full=False)
            print(f"[CRYPTO_SCREENER] daily history refresh: {result}")
        except Exception as exc:
            print(f"[CRYPTO_SCREENER] daily history refresh failed: {exc}")


async def _run_hydration() -> None:
    from config import CMC_API_KEY, COINGECKO_API_KEY
    from data.cmc_provider import CMCProvider
    from data.coingecko_provider import CoinGeckoProvider
    result = await CryptoScreenerService(
        CMCProvider(CMC_API_KEY), CoinGeckoProvider(COINGECKO_API_KEY)
    ).hydrate_history(full=True)
    print(result)


if __name__ == "__main__":
    asyncio.run(_run_hydration())
