"""
Global daily API budget tracker.
Persists call counts per provider per day.
Logs warnings at 70% and hard-stops at 90% to preserve headroom.
"""
from datetime import datetime


class DailyBudgetTracker:
    DAILY_LIMITS = {
        "fmp": 250,
        "alphavantage": 25,
        "twelvedata": 800,
        "coingecko": 333,
        "cmc": 333,
        "finnhub": 3600,
    }

    WARN_PCT = 0.70
    HARD_STOP_PCT = 0.90

    def __init__(self):
        self._counts: dict[str, int] = {}
        self._day: str = ""
        self._reset_if_new_day()

    def _reset_if_new_day(self):
        today = datetime.now().strftime("%Y-%m-%d")
        if today != self._day:
            self._day = today
            self._counts = {k: 0 for k in self.DAILY_LIMITS}

    def spend(self, provider: str, n: int = 1) -> bool:
        self._reset_if_new_day()
        provider = provider.lower()
        if provider not in self.DAILY_LIMITS:
            return True

        limit = self.DAILY_LIMITS[provider]
        current = self._counts.get(provider, 0)

        if current + n > limit * self.HARD_STOP_PCT:
            print(f"[BUDGET] HARD STOP: {provider} at {current}/{limit} "
                  f"({current/limit*100:.0f}%) — refusing {n} calls")
            return False

        self._counts[provider] = current + n

        if self._counts[provider] > limit * self.WARN_PCT:
            print(f"[BUDGET] WARNING: {provider} at {self._counts[provider]}/{limit} "
                  f"({self._counts[provider]/limit*100:.0f}%)")

        return True

    def can_spend(self, provider: str, n: int = 1) -> bool:
        self._reset_if_new_day()
        provider = provider.lower()
        if provider not in self.DAILY_LIMITS:
            return True
        current = self._counts.get(provider, 0)
        return (current + n) <= self.DAILY_LIMITS[provider] * self.HARD_STOP_PCT

    def status(self) -> dict:
        self._reset_if_new_day()
        return {
            "day": self._day,
            "providers": {
                provider: {
                    "used": self._counts.get(provider, 0),
                    "limit": limit,
                    "pct": round(self._counts.get(provider, 0) / limit * 100, 1),
                    "warn_at": int(limit * self.WARN_PCT),
                    "hard_stop_at": int(limit * self.HARD_STOP_PCT),
                }
                for provider, limit in self.DAILY_LIMITS.items()
            },
        }


daily_budget = DailyBudgetTracker()
