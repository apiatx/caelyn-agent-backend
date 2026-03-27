"""
Polygon.io Options Provider — fetches historic EOD options data and technical indicators.

Free Massive tier: 5 API calls/minute, 2 years historical, EOD data.
Uses a strict rate limiter to stay within budget.

Endpoints used:
  - GET /v3/reference/options/contracts?underlying_ticker={ticker}  (contracts reference)
  - GET /v2/aggs/ticker/O:{optionsTicker}/range/1/day/{from}/{to}   (daily OHLCV bars)
  - GET /v1/indicators/sma/{ticker}   (Simple Moving Average)
  - GET /v1/indicators/ema/{ticker}   (Exponential Moving Average)
  - GET /v1/indicators/rsi/{ticker}   (Relative Strength Index)
  - GET /v1/indicators/macd/{ticker}  (MACD)
"""

import time
import threading
import requests
from datetime import datetime, timedelta

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop


class PolygonOptionsProvider:
    """
    Polygon.io provider for historic options data and technical indicators.
    Enforces 5 calls/minute rate limit (Massive free tier).
    """

    BASE_URL = "https://api.polygon.io"

    def __init__(self, api_key: str, max_per_minute: int = 5):
        self.api_key = api_key
        self._rate_lock = threading.Lock()
        self._call_times: list[float] = []
        self._max_per_minute = max_per_minute

    def _wait_for_rate_slot(self) -> bool:
        """Block until a rate slot is available. Returns True if slot acquired."""
        for _ in range(120):  # wait up to 2 minutes
            with self._rate_lock:
                now = time.time()
                self._call_times = [t for t in self._call_times if now - t < 60]
                if len(self._call_times) < self._max_per_minute:
                    self._call_times.append(now)
                    return True
            time.sleep(1)
        return False

    def _request(self, path: str, params: dict = None, timeout: int = 15) -> dict:
        """Make a rate-limited request to Polygon API."""
        if params is None:
            params = {}
        params["apiKey"] = self.api_key

        if not self._wait_for_rate_slot():
            print("[POLYGON_OPTIONS] Rate limit wait timed out")
            return {"error": "rate_limit_timeout"}

        try:
            resp = requests.get(f"{self.BASE_URL}{path}", params=params, timeout=timeout)
            if resp.status_code == 429:
                print("[POLYGON_OPTIONS] 429 rate limited")
                return {"error": "rate_limited", "status": 429}
            if resp.status_code == 403:
                return {"error": "not_authorized", "status": 403}
            if resp.status_code != 200:
                return {"error": f"HTTP {resp.status_code}", "status": resp.status_code}
            return resp.json()
        except requests.exceptions.Timeout:
            print(f"[POLYGON_OPTIONS] Request timed out: {path}")
            return {"error": "timeout"}
        except Exception as e:
            print(f"[POLYGON_OPTIONS] Request error: {e}")
            return {"error": str(e)}

    # ── Options Contracts Reference ──────────────────────────────────

    @traceable(name="polygon_options.get_contracts")
    def get_contracts(
        self,
        underlying_ticker: str,
        expired: bool = False,
        limit: int = 250,
        contract_type: str = None,
        expiration_date_gte: str = None,
        expiration_date_lte: str = None,
        strike_price_gte: float = None,
        strike_price_lte: float = None,
        order: str = "asc",
        sort: str = "expiration_date",
    ) -> list[dict]:
        """
        Get options contracts for a ticker from Polygon reference data.
        Returns list of contract objects with: ticker, underlying_ticker,
        contract_type, strike_price, expiration_date, etc.
        Handles pagination automatically.
        """
        params = {
            "underlying_ticker": underlying_ticker.upper(),
            "limit": limit,
            "order": order,
            "sort": sort,
        }
        if expired:
            params["expired"] = "true"
        if contract_type:
            params["contract_type"] = contract_type
        if expiration_date_gte:
            params["expiration_date.gte"] = expiration_date_gte
        if expiration_date_lte:
            params["expiration_date.lte"] = expiration_date_lte
        if strike_price_gte is not None:
            params["strike_price.gte"] = strike_price_gte
        if strike_price_lte is not None:
            params["strike_price.lte"] = strike_price_lte

        all_contracts = []
        next_url = None
        page = 0

        while True:
            if next_url:
                # Polygon pagination uses full URLs
                try:
                    resp = requests.get(
                        next_url,
                        params={"apiKey": self.api_key},
                        timeout=15,
                    )
                    if resp.status_code != 200:
                        break
                    data = resp.json()
                except Exception as e:
                    print(f"[POLYGON_OPTIONS] Pagination error: {e}")
                    break
                # Count this as an API call for rate limiting
                with self._rate_lock:
                    self._call_times.append(time.time())
            else:
                data = self._request("/v3/reference/options/contracts", params=params)
                if "error" in data:
                    print(f"[POLYGON_OPTIONS] Contracts error for {underlying_ticker}: {data['error']}")
                    break

            results = data.get("results", [])
            all_contracts.extend(results)
            page += 1

            # Check for next page
            next_url = data.get("next_url")
            if not next_url or page >= 10:  # cap at 10 pages (2500 contracts)
                break

            # Wait for rate slot before next page
            if not self._wait_for_rate_slot():
                break

        return all_contracts

    @traceable(name="polygon_options.get_key_contracts")
    def get_key_contracts(self, underlying_ticker: str, current_price: float = None) -> list[dict]:
        """
        Get the most relevant options contracts for analysis:
        - Nearest 3 monthly expirations
        - Near-the-money strikes (±20% from current price)
        Returns a filtered, manageable set of contracts.
        """
        ticker = underlying_ticker.upper()
        today = datetime.now().strftime("%Y-%m-%d")
        six_months = (datetime.now() + timedelta(days=180)).strftime("%Y-%m-%d")

        params = {
            "expiration_date_gte": today,
            "expiration_date_lte": six_months,
        }

        if current_price and current_price > 0:
            params["strike_price_gte"] = round(current_price * 0.80, 2)
            params["strike_price_lte"] = round(current_price * 1.20, 2)

        contracts = self.get_contracts(ticker, **params)

        # Filter to monthly expirations (3rd Friday pattern) — keep all if <50
        if len(contracts) > 50:
            # Group by expiration and keep top 3 expirations by contract count
            by_exp: dict[str, list] = {}
            for c in contracts:
                exp = c.get("expiration_date", "")
                by_exp.setdefault(exp, []).append(c)

            # Sort expirations by date, take nearest 3
            sorted_exps = sorted(by_exp.keys())[:3]
            contracts = []
            for exp in sorted_exps:
                contracts.extend(by_exp[exp])

        return contracts

    # ── Options Daily Bars (OHLCV) ───────────────────────────────────

    @traceable(name="polygon_options.get_daily_bars")
    def get_daily_bars(
        self,
        option_ticker: str,
        from_date: str = None,
        to_date: str = None,
        limit: int = 5000,
    ) -> list[dict]:
        """
        Get daily OHLCV bars for a specific options contract.
        option_ticker: Polygon format e.g. 'O:AAPL250321C00200000'
        Returns list of bar dicts with: o, h, l, c, v, vw, n, t
        """
        if not from_date:
            from_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")  # 2 years
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%d")

        # Ensure O: prefix
        if not option_ticker.startswith("O:"):
            option_ticker = f"O:{option_ticker}"

        data = self._request(
            f"/v2/aggs/ticker/{option_ticker}/range/1/day/{from_date}/{to_date}",
            params={"adjusted": "true", "sort": "asc", "limit": limit},
        )

        if "error" in data:
            return []

        return data.get("results", []) or []

    # ── Technical Indicators (for underlying stocks) ─────────────────

    @traceable(name="polygon_options.get_sma")
    def get_sma(self, ticker: str, window: int = 50, timespan: str = "day", limit: int = 500) -> list[dict]:
        """Get Simple Moving Average for a stock ticker."""
        data = self._request(
            f"/v1/indicators/sma/{ticker.upper()}",
            params={
                "timespan": timespan,
                "window": window,
                "series_type": "close",
                "order": "desc",
                "limit": limit,
            },
        )
        if "error" in data:
            return []
        results = data.get("results", {})
        return results.get("values", []) if isinstance(results, dict) else []

    @traceable(name="polygon_options.get_ema")
    def get_ema(self, ticker: str, window: int = 12, timespan: str = "day", limit: int = 500) -> list[dict]:
        """Get Exponential Moving Average for a stock ticker."""
        data = self._request(
            f"/v1/indicators/ema/{ticker.upper()}",
            params={
                "timespan": timespan,
                "window": window,
                "series_type": "close",
                "order": "desc",
                "limit": limit,
            },
        )
        if "error" in data:
            return []
        results = data.get("results", {})
        return results.get("values", []) if isinstance(results, dict) else []

    @traceable(name="polygon_options.get_rsi")
    def get_rsi(self, ticker: str, window: int = 14, timespan: str = "day", limit: int = 500) -> list[dict]:
        """Get Relative Strength Index for a stock ticker."""
        data = self._request(
            f"/v1/indicators/rsi/{ticker.upper()}",
            params={
                "timespan": timespan,
                "window": window,
                "series_type": "close",
                "order": "desc",
                "limit": limit,
            },
        )
        if "error" in data:
            return []
        results = data.get("results", {})
        return results.get("values", []) if isinstance(results, dict) else []

    @traceable(name="polygon_options.get_macd")
    def get_macd(
        self,
        ticker: str,
        short_window: int = 12,
        long_window: int = 26,
        signal_window: int = 9,
        timespan: str = "day",
        limit: int = 500,
    ) -> list[dict]:
        """Get MACD indicator for a stock ticker."""
        data = self._request(
            f"/v1/indicators/macd/{ticker.upper()}",
            params={
                "timespan": timespan,
                "short_window": short_window,
                "long_window": long_window,
                "signal_window": signal_window,
                "series_type": "close",
                "order": "desc",
                "limit": limit,
            },
        )
        if "error" in data:
            return []
        results = data.get("results", {})
        return results.get("values", []) if isinstance(results, dict) else []

    @traceable(name="polygon_options.get_all_technicals")
    def get_all_technicals(self, ticker: str) -> dict:
        """
        Fetch all 4 technical indicators for a ticker in sequence (4 API calls).
        Returns dict with sma_20, sma_50, ema_12, ema_26, rsi_14, macd data.
        """
        ticker = ticker.upper()
        result = {"ticker": ticker, "fetched_at": datetime.now().isoformat()}

        # SMA 20
        sma_20 = self.get_sma(ticker, window=20, limit=250)
        result["sma_20"] = sma_20

        # SMA 50
        sma_50 = self.get_sma(ticker, window=50, limit=250)
        result["sma_50"] = sma_50

        # RSI 14
        rsi_14 = self.get_rsi(ticker, window=14, limit=250)
        result["rsi_14"] = rsi_14

        # MACD (12, 26, 9)
        macd = self.get_macd(ticker, limit=250)
        result["macd"] = macd

        return result

    # ── Snapshot / Last Quote ────────────────────────────────────────

    @traceable(name="polygon_options.get_options_snapshot")
    def get_options_snapshot(self, underlying_ticker: str) -> list[dict]:
        """
        Get current snapshot for all options of an underlying (paid tier).
        Falls back gracefully if not available on free tier.
        """
        data = self._request(
            f"/v3/snapshot/options/{underlying_ticker.upper()}",
            params={"limit": 250},
        )
        if "error" in data:
            return []
        return data.get("results", []) or []
