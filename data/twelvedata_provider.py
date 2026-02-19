import time
import threading
import requests
from datetime import datetime, timedelta


class TwelveDataProvider:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.twelvedata.com"
        self._rate_lock = threading.Lock()
        self._call_times = []
        self._max_per_minute = 8

    def _check_rate_limit(self) -> bool:
        with self._rate_lock:
            now = time.time()
            self._call_times = [t for t in self._call_times if now - t < 60]
            if len(self._call_times) >= self._max_per_minute:
                return False
            self._call_times.append(now)
            return True

    def get_daily_bars(self, symbol: str, days: int = 120) -> list:
        symbol = symbol.upper()

        if not self._check_rate_limit():
            print(f"[TwelveData] Rate limit reached ({self._max_per_minute}/min), skipping {symbol}")
            return {"error": "rate_limited", "status": 429}

        try:
            resp = requests.get(
                f"{self.base_url}/time_series",
                params={
                    "symbol": symbol,
                    "interval": "1day",
                    "outputsize": str(days),
                    "apikey": self.api_key,
                },
                timeout=10,
            )

            if resp.status_code == 401:
                print(f"[TwelveData] 401 auth error for {symbol}")
                return {"error": "auth", "status": 401}

            if resp.status_code == 429:
                print(f"[TwelveData] 429 rate limited for {symbol}")
                return {"error": "rate_limited", "status": 429}

            if resp.status_code != 200:
                print(f"[TwelveData] HTTP {resp.status_code} for {symbol}")
                return {"error": f"HTTP {resp.status_code}", "status": resp.status_code}

            data = resp.json()

            if data.get("status") == "error":
                code = data.get("code", 0)
                msg = data.get("message", "unknown")
                if code == 401 or "api_key" in msg.lower():
                    print(f"[TwelveData] Auth error: {msg}")
                    return {"error": "auth", "status": 401}
                if code == 429 or "minute" in msg.lower() or "credit" in msg.lower():
                    print(f"[TwelveData] Rate limit: {msg}")
                    return {"error": "rate_limited", "status": 429}
                print(f"[TwelveData] API error for {symbol}: {msg}")
                return []

            values = data.get("values", [])
            if not values:
                return []

            bars = []
            for v in reversed(values):
                try:
                    dt = v.get("datetime", "")
                    ts = int(datetime.strptime(dt, "%Y-%m-%d").timestamp()) if dt else 0
                    bars.append({
                        "o": float(v.get("open", 0)),
                        "h": float(v.get("high", 0)),
                        "l": float(v.get("low", 0)),
                        "c": float(v.get("close", 0)),
                        "v": int(float(v.get("volume", 0))),
                        "t": ts,
                    })
                except (ValueError, TypeError):
                    continue

            return bars

        except requests.exceptions.Timeout:
            print(f"[TwelveData] Timeout for {symbol}")
            return []
        except Exception as e:
            print(f"[TwelveData] Error for {symbol}: {e}")
            return []
