"""
Smart Options Strategy Service
================================
Finds arbitrage opportunities between Hyperliquid 24/7 perpetual prices
and actual market prices (Tradier).

Logic:
  - HL stock data is sourced from the market-matrix cache (stocks_etfs tab)
    so the Hyperliquid page and the Smart Options tab share ONE data fetch path.
    Falls back to all_assets() + disk cache only on a cold start before the
    first market-matrix call.
  - Multiple DEXes may list the same equity; we keep the most liquid instance
    and aggregate OI across all DEX rows.
  - Fetch matching equity quotes from Tradier (last/close/prevclose)
  - Compute the price gap (HL vs actual)
  - Signal: large HL premium → CALL opportunity; large HL discount → PUT opportunity
  - Most actionable when US markets are closed (weekends, after-hours, pre-market)

This service is FULLY ISOLATED from the Bottleneck/Chain-Reaction/Serenity logic.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional

# Gap thresholds (%) for signal classification
_GAP_WEAK_PCT     = 0.5
_GAP_MODERATE_PCT = 1.5
_GAP_STRONG_PCT   = 3.0

# Minimum 24h volume in USD to include an equity perp row
_MIN_VOL_USD = 500.0

# Maximum price ratio between HL and actual before we treat it as a ticker collision
# (e.g. HL "GOLD" ≈ $4500/oz while Tradier "GOLD" = Barrick stock ≈ $38)
_MAX_PRICE_RATIO = 12.0

# Tickers that exist on HL as equity-tagged perps but are NOT US stocks:
# commodities, FX pairs, foreign indices, sector ETF proxies, etc.
_NON_US_EQUITY_BLOCKLIST: set[str] = {
    # Commodities
    "GOLD", "SILVER", "COPPER", "WTI", "BRENTOIL", "CL", "GAS", "NATGAS",
    "PALLADIUM", "PLATINUM",
    # FX pairs
    "JPY", "EUR", "GBP", "CHF", "AUD", "CAD",
    # Foreign indices / non-US ETFs
    "KR200", "EWJ", "EWY", "EWZ", "SMSN",
    # Sector / thematic baskets (not actual US equity tickers)
    "GLDMINE", "BIOTECH", "ENERGY", "DEFENSE",
    # Unknown/index synthetic tokens
    "XYZ100", "SKHX",
    # Crypto assets that share tickers with US stocks on Tradier
    # DASH = Dash (crypto) on HL,  but DoorDash (DASH) on Tradier
    "DASH",
}


# ── Market-status detection ───────────────────────────────────────────────────

def _market_status() -> dict:
    """Return US market status and whether the HL gap is meaningful right now."""
    try:
        import zoneinfo
        et_zone = zoneinfo.ZoneInfo("America/New_York")
        now_et = datetime.now(et_zone)
    except Exception:
        now_et = datetime.now(timezone(timedelta(hours=-4)))

    wd = now_et.weekday()          # 0 = Monday … 6 = Sunday
    t  = now_et.hour + now_et.minute / 60.0

    if wd >= 5:
        days_until_monday = 7 - wd
        label = "weekend"
        context = (
            f"Weekend — US markets closed. "
            f"Next open: Monday 9:30 AM ET "
            f"(~{days_until_monday} day{'s' if days_until_monday != 1 else ''} away)"
        )
        meaningful = True
    elif t < 4.0:
        label = "overnight"
        context = "Overnight — markets closed. Pre-market opens 4:00 AM ET."
        meaningful = True
    elif t < 9.5:
        label = "pre_market"
        context = "Pre-market (4:00–9:30 AM ET) — thin liquidity, gap may close at open."
        meaningful = True
    elif t < 16.0:
        label = "open"
        context = (
            "Market hours (9:30 AM–4:00 PM ET) — real prices updating in real time. "
            "Gap typically narrows quickly while markets are open."
        )
        meaningful = False
    elif t < 20.0:
        label = "after_hours"
        context = "After-hours (4:00–8:00 PM ET) — markets closed. Gap may widen overnight."
        meaningful = True
    else:
        label = "overnight"
        context = "Overnight — markets closed. Pre-market opens 4:00 AM ET."
        meaningful = True

    return {
        "status":         label,
        "context":        context,
        "gap_meaningful": meaningful,
        "et_time":        now_et.strftime("%Y-%m-%d %H:%M ET"),
    }


# ── Main service function ─────────────────────────────────────────────────────

async def build_smart_options_data(
    hl_state,
    tradier,
    options_lkg: Optional[dict] = None,
) -> dict:
    """
    Build the Smart Options comparison table.

    Parameters
    ----------
    hl_state     : HyperliquidState  — live perp data
    tradier      : TradierProvider   — for real equity quotes
    options_lkg  : options master LKG dict or None — for actual options OI per ticker
    """
    market = _market_status()

    # ── 1. Get HL equity stock data ───────────────────────────────────────────
    # PRIMARY source: the market-matrix stocks_etfs cache (same data the
    # Hyperliquid page already fetched). Both pages share one HL data path.
    # FALLBACK: all_assets() + disk cache on cold start (matrix not yet built).
    #
    # Uniform dict stored per ticker:
    #   price           — mark price
    #   oracle_px       — oracle price
    #   chg_24h_pct     — 24h change %
    #   funding_hourly  — raw hourly funding rate (decimal, e.g. 0.0001)
    #   volume_24h_usd  — 24h notional volume USD
    # OI aggregated separately in oi_usd_by_ticker / oi_contracts_by_ticker.

    best_by_ticker:        dict[str, dict]  = {}
    oi_usd_by_ticker:      dict[str, float] = {}
    oi_contracts_by_ticker: dict[str, float] = {}

    # Try matrix cache first (avoids a redundant all_assets() read)
    try:
        from services.hyperliquid.router import get_matrix_stocks_snapshot
        matrix_rows = get_matrix_stocks_snapshot()
    except Exception:
        matrix_rows = []

    if matrix_rows:
        # Matrix rows: coin=display_name, mark, oracle, change_24h_pct,
        # funding (hourly raw), open_interest_usd, volume_24h_usd.
        # Multiple DEX rows may share the same display_name ticker.
        for row in matrix_rows:
            ticker = (row.get("coin") or row.get("display_name") or "").upper().strip()
            if not ticker:
                continue
            price = row.get("mark")
            try:
                price = float(price)
            except (TypeError, ValueError):
                continue
            if price <= 0:
                continue
            vol = float(row.get("volume_24h_usd") or 0)
            if vol < _MIN_VOL_USD:
                continue
            oi = float(row.get("open_interest_usd") or 0)

            # Keep the highest-volume DEX row per ticker
            if ticker not in best_by_ticker or vol > float(best_by_ticker[ticker]["volume_24h_usd"] or 0):
                best_by_ticker[ticker] = {
                    "price":          price,
                    "oracle_px":      row.get("oracle"),
                    "chg_24h_pct":    row.get("change_24h_pct"),
                    "funding_hourly": float(row.get("funding") or 0.0),
                    "volume_24h_usd": vol,
                }

            # Aggregate OI across all DEX rows for the same ticker
            oi_usd_by_ticker[ticker] = oi_usd_by_ticker.get(ticker, 0.0) + oi
            # oi_contracts not available in matrix rows
        print(f"[SmartOptions] HL data from market-matrix cache: {len(best_by_ticker)} stocks")

    else:
        # Cold-start fallback: matrix not yet populated
        print("[SmartOptions] Market-matrix cache empty — falling back to all_assets()")
        raw_assets = []
        try:
            raw_assets = hl_state.all_assets()
        except Exception:
            pass

        equity_assets = [
            a for a in raw_assets
            if a.market_type == "perp"
            and "equity" in (getattr(a, "tags", None) or [])
            and a.mark_px is not None
            and a.mark_px > 0
            and (a.day_ntl_vlm or 0) >= _MIN_VOL_USD
        ]
        if not equity_assets:
            equity_assets = _load_equity_assets_from_disk_cache()

        lkg_best: dict[str, object] = {}
        for a in equity_assets:
            ticker = (getattr(a, "display_name", None) or a.coin).upper()
            if not ticker:
                continue
            existing = lkg_best.get(ticker)
            if existing is None or (a.day_ntl_vlm or 0) > (existing.day_ntl_vlm or 0):
                lkg_best[ticker] = a
            oi_usd_by_ticker[ticker]       = oi_usd_by_ticker.get(ticker, 0.0) + (a.open_interest_usd or 0)
            oi_contracts_by_ticker[ticker] = oi_contracts_by_ticker.get(ticker, 0.0) + (a.open_interest or 0)

        for ticker, a in lkg_best.items():
            best_by_ticker[ticker] = {
                "price":          a.mark_px,
                "oracle_px":      getattr(a, "oracle_px", None),
                "chg_24h_pct":    getattr(a, "pct_change_24h", None),
                "funding_hourly": float(getattr(a, "funding", None) or 0.0),
                "volume_24h_usd": getattr(a, "day_ntl_vlm", None),
            }
        print(f"[SmartOptions] HL data from all_assets() fallback: {len(best_by_ticker)} stocks")

    if not best_by_ticker:
        return {
            "market":            market,
            "rows":              [],
            "total_hl_equities": 0,
            "with_gap":          0,
            "warning": (
                "No equity perps currently in the Hyperliquid state. "
                "The screener may still be initialising — try again in ~30 seconds."
            ),
        }

    tickers = sorted(best_by_ticker.keys())

    # ── 2. Fetch Tradier quotes for all equity tickers ────────────────────────
    # The Tradier rate limiter may be busy (100/100) when the request arrives.
    # Use a generous timeout (35s) so we always wait through any throttle window.
    tradier_quotes: list[dict] = []
    if tradier:
        try:
            from data.tradier_budget import lane as _so_lane
            with _so_lane("saved_options"):
                tradier_quotes = await asyncio.wait_for(
                    tradier.get_quotes(tickers), timeout=35.0
                )
        except asyncio.TimeoutError:
            print("[SmartOptions] Tradier batch-quote timed out — serving HL-only data")
        except Exception as exc:
            import traceback as _tb
            print(f"[SmartOptions] Tradier batch-quote error: {type(exc).__name__}: {exc}")
            _tb.print_exc()

    quote_map: dict[str, dict] = {
        (q.get("symbol") or "").upper(): q
        for q in (tradier_quotes or [])
        if q.get("symbol")
    }

    # ── 3. Build actual-OI lookup from options master LKG ────────────────────
    oi_map: dict[str, int] = {}
    if options_lkg and isinstance(options_lkg, dict):
        for entry in options_lkg.get("tickers", []):
            sym = (entry.get("ticker") or "").upper()
            if sym:
                total = (entry.get("call_oi") or 0) + (entry.get("put_oi") or 0)
                if total:
                    oi_map[sym] = total

    # ── 4. Build comparison rows ──────────────────────────────────────────────
    rows = []
    for ticker, asset in best_by_ticker.items():
        hl_price = float(asset["price"])
        q = quote_map.get(ticker)
        actual_price = _best_price(q)

        # Aggregate OI values across all DEXes for this ticker
        total_hl_oi_usd       = oi_usd_by_ticker.get(ticker)
        total_hl_oi_contracts = oi_contracts_by_ticker.get(ticker)

        hl_block     = _hl_fields(asset, total_hl_oi_usd, total_hl_oi_contracts)
        actual_block = _actual_fields(q, oi_map.get(ticker))

        # Skip non-US-equity tickers (commodities, FX, foreign indices)
        if ticker in _NON_US_EQUITY_BLOCKLIST:
            continue

        if not actual_price or actual_price <= 0:
            rows.append({
                "ticker":          ticker,
                "signal":          "no_data",
                "signal_strength": None,
                "hl":              hl_block,
                "actual":          actual_block,
                "gap":             None,
            })
            continue

        # Price-ratio sanity check: if prices differ by more than _MAX_PRICE_RATIO,
        # it's a ticker collision (e.g. HL "GOLD" = spot gold, Tradier "GOLD" = Barrick).
        price_ratio = (hl_price / actual_price) if hl_price >= actual_price else (actual_price / hl_price)
        if price_ratio > _MAX_PRICE_RATIO:
            continue   # Different instruments with same ticker — exclude silently

        gap_abs = round(hl_price - actual_price, 4)
        gap_pct = round((hl_price - actual_price) / actual_price * 100, 3)
        abs_pct = abs(gap_pct)

        if gap_pct > _GAP_WEAK_PCT:
            signal    = "call"
            direction = "hl_premium"
        elif gap_pct < -_GAP_WEAK_PCT:
            signal    = "put"
            direction = "hl_discount"
        else:
            signal    = "neutral"
            direction = "aligned"

        if abs_pct >= _GAP_STRONG_PCT:
            strength = "strong"
        elif abs_pct >= _GAP_MODERATE_PCT:
            strength = "moderate"
        elif abs_pct >= _GAP_WEAK_PCT:
            strength = "weak"
        else:
            strength = "neutral"

        rows.append({
            "ticker":          ticker,
            "signal":          signal,
            "signal_strength": strength,
            "hl":              hl_block,
            "actual":          actual_block,
            "gap": {
                "abs":       gap_abs,
                "pct":       gap_pct,
                "direction": direction,
            },
        })

    # Sort: no-data rows last, then biggest absolute gap first
    rows.sort(key=lambda r: (
        r["signal"] == "no_data",
        -(abs(r["gap"]["pct"]) if r["gap"] else 0),
    ))

    with_gap = sum(
        1 for r in rows
        if r["gap"] and abs(r["gap"]["pct"]) >= _GAP_WEAK_PCT
    )

    return {
        "market":            market,
        "data_source":       "Hyperliquid perpetuals (live) + Tradier equity quotes",
        "total_hl_equities": len(best_by_ticker),
        "with_gap":          with_gap,
        "rows":              rows,
    }


# ── Field helpers ─────────────────────────────────────────────────────────────

def _best_price(q: Optional[dict]) -> Optional[float]:
    if not q:
        return None
    for field in ("last", "close", "prevclose"):
        v = q.get(field)
        if v is not None:
            try:
                f = float(v)
                if f > 0:
                    return f
            except (TypeError, ValueError):
                pass
    return None


def _hl_fields(asset: dict, total_oi_usd: Optional[float], total_oi_contracts: Optional[float]) -> dict:
    """asset is the uniform dict from best_by_ticker (works for both matrix and fallback sources)."""
    funding_hr = float(asset.get("funding_hourly") or 0.0)
    return {
        "price":               _r(asset.get("price"), 4),
        "oracle_px":           _r(asset.get("oracle_px"), 4),
        "chg_24h_pct":         _r(asset.get("chg_24h_pct"), 3),
        "funding_rate_hourly": _r(funding_hr * 100, 6),
        "funding_rate_ann":    _r(funding_hr * 8760 * 100, 2),
        "oi_usd":              _r(total_oi_usd, 0),
        "oi_contracts":        _r(total_oi_contracts, 2),
        "volume_24h_usd":      _r(asset.get("volume_24h_usd"), 0),
    }


def _actual_fields(q: Optional[dict], options_oi: Optional[int]) -> dict:
    if not q:
        return {
            "price": None, "close": None, "prevclose": None,
            "bid": None, "ask": None, "volume": None,
            "change_pct": None, "options_oi": options_oi,
        }
    return {
        "price":      _best_price(q),
        "close":      q.get("close"),
        "prevclose":  q.get("prevclose"),
        "bid":        q.get("bid"),
        "ask":        q.get("ask"),
        "volume":     q.get("volume"),
        "change_pct": q.get("change_percentage"),
        "options_oi": options_oi,
    }


def _r(val, decimals: int) -> Optional[float]:
    if val is None:
        return None
    try:
        return round(float(val), decimals)
    except (TypeError, ValueError):
        return None


# ── Disk-cache fallback ───────────────────────────────────────────────────────

_HIP3_CACHE_PATH = (
    __import__("pathlib").Path(__file__).parent.parent
    / "data" / "hyperliquid_hip3_cache.json"
)


class _DiskAsset:
    """Lightweight stand-in for ScreenerAsset when loading from disk cache."""
    __slots__ = (
        "coin", "display_name", "market_type", "tags",
        "mark_px", "oracle_px", "pct_change_24h", "funding",
        "open_interest_usd", "open_interest", "day_ntl_vlm",
    )

    def __init__(self, data: dict):
        self.coin           = data.get("coin", "")
        self.display_name   = data.get("display_name", self.coin)
        self.market_type    = data.get("market_type", "perp")
        self.tags           = data.get("tags") or []
        self.mark_px        = _r(data.get("mark_px"), 6)
        self.oracle_px      = _r(data.get("oracle_px"), 6)
        self.pct_change_24h = _r(data.get("pct_change_24h"), 3)
        self.funding        = data.get("funding")
        self.open_interest_usd = data.get("open_interest_usd")
        self.open_interest  = data.get("open_interest")
        self.day_ntl_vlm    = data.get("day_ntl_vlm")


def _load_equity_assets_from_disk_cache() -> list:
    """
    Load equity-tagged HIP-3 assets from the disk cache.
    Used as a fallback when the live HL state hasn't merged HIP-3 assets yet.
    Returns a list of _DiskAsset objects with the same interface as ScreenerAsset.
    """
    import json
    import time

    try:
        if not _HIP3_CACHE_PATH.exists():
            return []
        with open(_HIP3_CACHE_PATH) as f:
            payload = json.load(f)
        age_s = time.time() - payload.get("saved_at", 0)
        if age_s > 86400:          # stale > 24 h
            return []
        assets_raw = payload.get("assets", {})
        result = []
        for coin, data in assets_raw.items():
            if ":" not in coin:
                continue
            a = _DiskAsset(data)
            if (
                a.market_type == "perp"
                and "equity" in a.tags
                and a.mark_px is not None
                and a.mark_px > 0
                and (a.day_ntl_vlm or 0) >= _MIN_VOL_USD
            ):
                result.append(a)
        print(f"[SmartOptions] Loaded {len(result)} equity assets from HIP-3 disk cache (age={age_s/60:.1f} min)")
        return result
    except Exception as exc:
        print(f"[SmartOptions] Disk cache fallback error: {exc}")
        return []
