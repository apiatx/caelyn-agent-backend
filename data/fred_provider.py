from fredapi import Fred
from datetime import datetime, timedelta


class FredProvider:
    """
    Provides macroeconomic data from the Federal Reserve Economic Data (FRED).
    Completely free, no rate limits, and the most authoritative source
    for US economic data.
    
    This replaces/supplements Alpha Vantage's macro endpoints with
    more comprehensive and unlimited data.
    """

    def __init__(self, api_key: str):
        self.fred = None
        if api_key:
            try:
                self.fred = Fred(api_key=api_key)
            except Exception as e:
                print(f"FRED init error: {e}")

    def _check_init(self):
        if self.fred is None:
            return {"error": "FRED API key not configured"}
        return None

    def get_fed_funds_rate(self) -> dict:
        """Get the effective federal funds rate (what the Fed sets)."""
        check = self._check_init()
        if check:
            return check
        try:
            data = self.fred.get_series(
                "FEDFUNDS",
                observation_start=(datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
            )
            if data is None or data.empty:
                return {"error": "No data"}

            recent = data.tail(6)
            return {
                "current_rate": round(float(data.iloc[-1]), 2),
                "current_date": str(data.index[-1].date()),
                "trend": [
                    {"date": str(idx.date()), "rate": round(float(val), 2)}
                    for idx, val in recent.items()
                ],
            }
        except Exception as e:
            print(f"FRED fed funds rate error: {e}")
            return {"error": str(e)}

    def get_inflation_cpi(self) -> dict:
        """
        Get CPI (Consumer Price Index) and calculate YoY inflation rate.
        This is the headline inflation number everyone references.
        """
        check = self._check_init()
        if check:
            return check
        try:
            data = self.fred.get_series(
                "CPIAUCSL",
                observation_start=(datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d"),
            )
            if data is None or data.empty:
                return {"error": "No data"}

            recent = data.tail(6)
            latest = float(data.iloc[-1])

            yoy_inflation = None
            if len(data) >= 13:
                year_ago = float(data.iloc[-13])
                if year_ago > 0:
                    yoy_inflation = round(((latest - year_ago) / year_ago) * 100, 2)

            mom_change = None
            if len(data) >= 2:
                prev = float(data.iloc[-2])
                if prev > 0:
                    mom_change = round(((latest - prev) / prev) * 100, 2)

            return {
                "latest_cpi": round(latest, 2),
                "latest_date": str(data.index[-1].date()),
                "yoy_inflation_pct": yoy_inflation,
                "mom_change_pct": mom_change,
                "trend": [
                    {"date": str(idx.date()), "cpi": round(float(val), 2)}
                    for idx, val in recent.items()
                ],
            }
        except Exception as e:
            print(f"FRED CPI error: {e}")
            return {"error": str(e)}

    def get_core_pce(self) -> dict:
        """
        Get Core PCE — the Fed's PREFERRED inflation measure.
        This excludes food and energy and is what the Fed actually
        targets at 2%.
        """
        check = self._check_init()
        if check:
            return check
        try:
            data = self.fred.get_series(
                "PCEPILFE",
                observation_start=(datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d"),
            )
            if data is None or data.empty:
                return {"error": "No data"}

            recent = data.tail(6)
            latest = float(data.iloc[-1])

            yoy_change = None
            if len(data) >= 13:
                year_ago = float(data.iloc[-13])
                if year_ago > 0:
                    yoy_change = round(((latest - year_ago) / year_ago) * 100, 2)

            return {
                "latest_value": round(latest, 2),
                "latest_date": str(data.index[-1].date()),
                "yoy_change_pct": yoy_change,
                "fed_target": 2.0,
                "above_target": yoy_change > 2.0 if yoy_change is not None else None,
                "trend": [
                    {"date": str(idx.date()), "value": round(float(val), 2)}
                    for idx, val in recent.items()
                ],
            }
        except Exception as e:
            print(f"FRED Core PCE error: {e}")
            return {"error": str(e)}

    def get_unemployment(self) -> dict:
        """Get the unemployment rate."""
        check = self._check_init()
        if check:
            return check
        try:
            data = self.fred.get_series(
                "UNRATE",
                observation_start=(datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
            )
            if data is None or data.empty:
                return {"error": "No data"}

            recent = data.tail(6)
            return {
                "current_rate": round(float(data.iloc[-1]), 1),
                "current_date": str(data.index[-1].date()),
                "trend": [
                    {"date": str(idx.date()), "rate": round(float(val), 1)}
                    for idx, val in recent.items()
                ],
            }
        except Exception as e:
            print(f"FRED unemployment error: {e}")
            return {"error": str(e)}

    def get_gdp_growth(self) -> dict:
        """Get real GDP growth rate (quarterly, annualized)."""
        check = self._check_init()
        if check:
            return check
        try:
            data = self.fred.get_series(
                "A191RL1Q225SBEA",
                observation_start=(datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d"),
            )
            if data is None or data.empty:
                return {"error": "No data"}

            recent = data.tail(4)
            latest = float(data.iloc[-1])

            return {
                "latest_gdp_growth_pct": round(latest, 1),
                "latest_date": str(data.index[-1].date()),
                "recession_signal": latest < 0,
                "trend": [
                    {"date": str(idx.date()), "growth_pct": round(float(val), 1)}
                    for idx, val in recent.items()
                ],
            }
        except Exception as e:
            print(f"FRED GDP error: {e}")
            return {"error": str(e)}

    def get_ten_year_yield(self) -> dict:
        """Get 10-year Treasury yield — the benchmark for mortgages and valuations."""
        check = self._check_init()
        if check:
            return check
        try:
            data = self.fred.get_series(
                "DGS10",
                observation_start=(datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d"),
            )
            if data is None or data.empty:
                return {"error": "No data"}

            data = data.dropna()
            recent = data.tail(10)

            return {
                "current_yield": round(float(data.iloc[-1]), 2),
                "current_date": str(data.index[-1].date()),
                "trend": [
                    {"date": str(idx.date()), "yield": round(float(val), 2)}
                    for idx, val in recent.items()
                ],
            }
        except Exception as e:
            print(f"FRED 10Y yield error: {e}")
            return {"error": str(e)}

    def get_two_year_yield(self) -> dict:
        """Get 2-year Treasury yield — key for yield curve analysis."""
        check = self._check_init()
        if check:
            return check
        try:
            data = self.fred.get_series(
                "DGS2",
                observation_start=(datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d"),
            )
            if data is None or data.empty:
                return {"error": "No data"}

            data = data.dropna()
            recent = data.tail(10)

            return {
                "current_yield": round(float(data.iloc[-1]), 2),
                "current_date": str(data.index[-1].date()),
                "trend": [
                    {"date": str(idx.date()), "yield": round(float(val), 2)}
                    for idx, val in recent.items()
                ],
            }
        except Exception as e:
            print(f"FRED 2Y yield error: {e}")
            return {"error": str(e)}

    def get_yield_curve_spread(self) -> dict:
        """
        Get the 10Y-2Y yield spread (yield curve).
        Inverted (negative) = recession signal.
        Steepening (positive and rising) = economic expansion signal.
        """
        check = self._check_init()
        if check:
            return check
        try:
            data = self.fred.get_series(
                "T10Y2Y",
                observation_start=(datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
            )
            if data is None or data.empty:
                return {"error": "No data"}

            data = data.dropna()
            recent = data.tail(10)
            latest = float(data.iloc[-1])

            if latest < 0:
                signal = "INVERTED — historically signals recession within 6-18 months"
            elif latest < 0.5:
                signal = "Flat — economy slowing, caution warranted"
            elif latest < 1.5:
                signal = "Normal — healthy economic expansion signal"
            else:
                signal = "Steep — strong growth expectations, bullish for cyclicals"

            return {
                "current_spread": round(latest, 2),
                "current_date": str(data.index[-1].date()),
                "inverted": latest < 0,
                "signal": signal,
                "trend": [
                    {"date": str(idx.date()), "spread": round(float(val), 2)}
                    for idx, val in recent.items()
                ],
            }
        except Exception as e:
            print(f"FRED yield curve error: {e}")
            return {"error": str(e)}

    def get_vix(self) -> dict:
        """Get the VIX (fear index) — measures market volatility expectations."""
        check = self._check_init()
        if check:
            return check
        try:
            data = self.fred.get_series(
                "VIXCLS",
                observation_start=(datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d"),
            )
            if data is None or data.empty:
                return {"error": "No data"}

            data = data.dropna()
            recent = data.tail(10)
            latest = float(data.iloc[-1])

            if latest < 15:
                signal = "Very low fear — market complacency, potential for sharp moves"
            elif latest < 20:
                signal = "Low fear — normal bull market conditions"
            elif latest < 25:
                signal = "Elevated — market uncertainty, proceed with caution"
            elif latest < 30:
                signal = "High fear — significant market stress"
            else:
                signal = "Extreme fear — potential capitulation, contrarian buy signal"

            return {
                "current_vix": round(latest, 2),
                "current_date": str(data.index[-1].date()),
                "signal": signal,
                "trend": [
                    {"date": str(idx.date()), "vix": round(float(val), 2)}
                    for idx, val in recent.items()
                ],
            }
        except Exception as e:
            print(f"FRED VIX error: {e}")
            return {"error": str(e)}

    def get_initial_jobless_claims(self) -> dict:
        """
        Get weekly initial jobless claims.
        This is a leading indicator — rising claims signal economic weakness.
        """
        check = self._check_init()
        if check:
            return check
        try:
            data = self.fred.get_series(
                "ICSA",
                observation_start=(datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d"),
            )
            if data is None or data.empty:
                return {"error": "No data"}

            recent = data.tail(8)
            latest = float(data.iloc[-1])

            four_week_avg = None
            if len(data) >= 4:
                four_week_avg = round(float(data.tail(4).mean()), 0)

            if latest < 220000:
                signal = "Very strong labor market"
            elif latest < 260000:
                signal = "Healthy labor market"
            elif latest < 300000:
                signal = "Labor market softening"
            else:
                signal = "Significant labor market weakness — recessionary signal"

            return {
                "latest_claims": int(latest),
                "latest_date": str(data.index[-1].date()),
                "four_week_average": int(four_week_avg) if four_week_avg else None,
                "signal": signal,
                "trend": [
                    {"date": str(idx.date()), "claims": int(float(val))}
                    for idx, val in recent.items()
                ],
            }
        except Exception as e:
            print(f"FRED jobless claims error: {e}")
            return {"error": str(e)}

    def get_full_macro_dashboard(self) -> dict:
        """
        Complete macroeconomic picture. This is the motherlode —
        everything your agent needs to understand the macro environment.
        """
        return {
            "federal_funds_rate": self.get_fed_funds_rate(),
            "inflation_cpi": self.get_inflation_cpi(),
            "core_pce_feds_preferred_inflation": self.get_core_pce(),
            "unemployment": self.get_unemployment(),
            "gdp_growth": self.get_gdp_growth(),
            "ten_year_treasury_yield": self.get_ten_year_yield(),
            "two_year_treasury_yield": self.get_two_year_yield(),
            "yield_curve_10y_2y": self.get_yield_curve_spread(),
            "vix_fear_index": self.get_vix(),
            "weekly_jobless_claims": self.get_initial_jobless_claims(),
        }

    def get_quick_macro(self) -> dict:
        """
        Lighter version — just the 4 most important indicators.
        Use this for scan_market to keep response sizes manageable.
        """
        return {
            "federal_funds_rate": self.get_fed_funds_rate(),
            "inflation_cpi": self.get_inflation_cpi(),
            "yield_curve_10y_2y": self.get_yield_curve_spread(),
            "vix_fear_index": self.get_vix(),
        }
