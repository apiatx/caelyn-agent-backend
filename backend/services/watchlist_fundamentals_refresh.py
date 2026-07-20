"""
Watchlist Fundamental Screener — weekly FMP refresh service.

Architecture
───────────────────────────────────────────────────────────────────────────────
• Runs as a background task; never called on page render.
• One symbol at a time, paced via asyncio.sleep between calls.
• 10 FMP stable/ endpoints per symbol (all on Starter plan).
• Normalized snapshot stored in watchlist_fundamentals_cache (Neon).
• Merge precedence (in watchlist GET): FMP non-null > CSV value > blank.
• No-null overwrite: null FMP result never erases an existing CSV value.

Endpoint mapping (per symbol, 10 calls)
───────────────────────────────────────────────────────────────────────────────
1  stable/profile                         → Market Cap, implied shares
2  stable/income-statement  period=quarter → Revenue (TTM), Op. Income, EBIT, etc.
3  stable/income-statement-growth period=quarter → EPS Growth [seq QoQ]
4  stable/cash-flow-statement period=quarter → Free CF, FCF Margin, SBC
5  stable/ratios-ttm                      → P/E, P/S, Gross Mgn, D/E, Current Ratio,
                                             Interest Coverage, Operating Margin, P/FCF
6  stable/key-metrics-ttm                 → EV/EBITDA, ND/EBITDA, ROIC, FCF Yield, EV
7  stable/earnings                        → Earn. Date, Rev/EPS estimate growth
8  stable/balance-sheet-statement quarter → Cash, Net Cash / Debt
9  stable/analyst-estimates  annual       → FY1 estimates → Forward P/E upgrade,
                                             Forward Revenue Growth, Fwd P/S,
                                             Fwd EV/Sales, Fwd EV/EBITDA,
                                             Revenue/EPS Estimate Revision 90D
10 stable/financial-scores               → Altman Z-Score, Piotroski Score

Quality fields derived from existing calls (0 additional calls):
  Operating Margin, Current Ratio, Interest Coverage, ROIC, FCF Yield, P/FCF,
  FCF Conversion, Diluted Shares Growth YoY, SBC / Revenue,
  Revenue Acceleration, Gross Margin Change YoY, Incremental Operating Margin

FMP entitlement audit (Starter plan, live-probed):
  analyst-estimates period=quarter → 402 (not entitled).
  Forward P/E priority: (1) FY1 annual consensus EPS  (2) next-quarter EPS × 4 fallback.
  Spec Priority 1 (4-quarter EPS sum) cannot be implemented on this plan.

CSV fallback (FMP Starter cannot supply these):
  Shares Insiders  — no insider ownership endpoint available
  Revenue Growth Est. / Rev Growth This Year / Rev Growth Next Year
  EPS Growth Est.  / EPS Growth This Year  / EPS Growth Next Year
  EPS Growth Next Quarter  (only 1 future quarter in stable/earnings)
  Revenue Growth (YoY)  — derived below from 4Q sums if income stmt has 8Q
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timezone
from typing import Any

import httpx

log = logging.getLogger(__name__)

_FMP_BASE = "https://financialmodelingprep.com/stable"
_CALL_DELAY = 0.45      # seconds between FMP calls ≈ 133 req/min (< 200 req/min Starter)
_TIMEOUT = 18           # seconds per HTTP request

# ── Quality field carry-forward set ─────────────────────────────────────────
# These fields come from the 3 new optional endpoints (calls 8-10) and from
# derived quality computation.  When a new refresh fails to produce any of them
# (transient error / no-data), the prior usable snapshot value is copied forward
# so a single endpoint failure does not erase months of historical data.
_QUALITY_CARRY_FIELDS: frozenset[str] = frozenset({
    # Financial Strength
    "Cash", "Net Cash / Debt", "Current Ratio", "Interest Coverage",
    "Cash Runway Months", "Cash Runway Status",
    "Altman Z-Score", "Altman Z-Risk",
    "_altman_z_not_meaningful_reason",
    "_cash_runway_not_meaningful_reason",
    # Business Quality
    "ROIC", "Operating Margin", "FCF Conversion", "FCF Yield",
    "Diluted Shares Growth YoY", "SBC / Revenue",
    "Piotroski Score",
    # Growth Quality
    "Revenue Acceleration", "Gross Margin Change YoY",
    "Incremental Operating Margin", "Forward Revenue Growth",
    "Revenue Estimate Revision 90D", "EPS Estimate Revision 90D",
    "_rev_revision_prior_date", "_rev_revision_reason",
    "_eps_revision_prior_date", "_eps_revision_reason",
    # Valuation (new)
    "Forward P/S", "Forward EV/Sales", "Forward EV/EBITDA", "P/FCF",
    "_p_fcf_not_meaningful_reason",
    # Estimate provenance
    "_forward_estimate_fy1_date", "_forward_estimate_fy1_n_analysts",
})

# Financial sectors/industry keywords where Altman Z and Cash Runway
# are economically meaningless (deposit-funded; current-ratio irrelevant).
_FIN_SECTOR_NAMES = frozenset({
    "Financial Services", "Finance", "Banking", "Insurance",
    "Real Estate", "Banks",
})
_FIN_SECTOR_KEYWORDS = ("bank", "financ", "insur", "reit", "thrift", "mortgage")
_FIN_INDUSTRY_KEYWORDS = (
    "bank", "insurance", "reit", "real estate investment trust",
    "thrift", "mortgage", "savings",
)


def _is_financial_company(sector: str, industry: str) -> bool:
    """True when Altman Z-Score and Cash Runway are not economically meaningful."""
    s = str(sector or "").strip()
    i = str(industry or "").lower()
    return (
        s in _FIN_SECTOR_NAMES
        or any(kw in s.lower() for kw in _FIN_SECTOR_KEYWORDS)
        or any(kw in i for kw in _FIN_INDUSTRY_KEYWORDS)
    )


class FmpFundamentalsRefresher:
    """
    Refreshes FMP fundamentals for a list of symbols.
    Rate-limited via asyncio.sleep between each HTTP call.
    """

    def __init__(self, fmp_api_key: str):
        self._key = fmp_api_key

    async def _get(self, endpoint: str, params: dict | None = None) -> list | dict:
        p: dict[str, Any] = {**(params or {}), "apikey": self._key}
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.get(f"{_FMP_BASE}/{endpoint}", params=p)
            await asyncio.sleep(_CALL_DELAY)
            if resp.status_code == 200:
                return resp.json()
            log.debug("[FMP_FUND] %s HTTP %s", endpoint, resp.status_code)
            return []
        except Exception as exc:
            log.debug("[FMP_FUND] %s error: %s", endpoint, exc)
            await asyncio.sleep(_CALL_DELAY)
            return []

    # ── Numeric helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _pct(numerator, denominator) -> float | None:
        """Return (num/den - 1) × 100 or None if invalid."""
        try:
            if numerator is None or denominator is None or denominator == 0:
                return None
            return round((numerator / denominator - 1) * 100, 4)
        except Exception:
            return None

    @staticmethod
    def _fmt_pct(value: float | None) -> str | None:
        """Format a float percentage as a string like '12.34%' for CSV compat."""
        if value is None:
            return None
        return f"{value:.2f}%"

    @staticmethod
    def _ttm_sum(rows: list[dict], field: str, n: int = 4) -> float | None:
        """Sum `field` across the n most recent rows (TTM = 4 quarters)."""
        vals = []
        for r in rows[:n]:
            v = r.get(field)
            if v is not None:
                vals.append(float(v))
        return sum(vals) if len(vals) == n else (sum(vals) if vals else None)

    # ── Quality helpers (zero additional FMP calls) ──────────────────────────

    def _compute_derived_quality(
        self,
        is_rows: list[dict],
        cf_rows: list[dict],
        rtm: dict,
        kmtm: dict,
    ) -> dict:
        """
        Derive Quality fields from already-fetched FMP data. Zero additional calls.
        Returns only fields that have non-None values.
        """
        q: dict[str, Any] = {}

        # ── Operating Margin (direct from ratios-ttm) ─────────────────────
        op_m = rtm.get("operatingProfitMarginTTM")
        if op_m is not None:
            try:
                q["Operating Margin"] = self._fmt_pct(float(op_m) * 100)
            except (ValueError, TypeError):
                pass

        # ── Current Ratio (direct from ratios-ttm) ────────────────────────
        cur = rtm.get("currentRatioTTM")
        if cur is not None:
            try:
                q["Current Ratio"] = round(float(cur), 4)
            except (ValueError, TypeError):
                pass

        # ── Interest Coverage ─────────────────────────────────────────────
        # FMP reports 0 as a sentinel when it cannot compute (e.g. AAPL which
        # nets interest income against expense).  Fall back to EBIT / |IE| from
        # income statement; skip entirely if denominator is zero.
        ic_raw = rtm.get("interestCoverageRatioTTM")
        ic_stored = False
        if ic_raw is not None:
            try:
                ic_f = float(ic_raw)
                if ic_f != 0.0:
                    q["Interest Coverage"] = round(ic_f, 4)
                    ic_stored = True
            except (ValueError, TypeError):
                pass
        if not ic_stored and is_rows:
            ttm_ebit = (
                self._ttm_sum(is_rows, "ebit", 4)
                or self._ttm_sum(is_rows, "operatingIncome", 4)
            )
            ttm_ie = self._ttm_sum(is_rows, "interestExpense", 4)
            if ttm_ebit is not None and ttm_ie and abs(float(ttm_ie)) > 0:
                try:
                    q["Interest Coverage"] = round(
                        float(ttm_ebit) / abs(float(ttm_ie)), 4
                    )
                except (ValueError, TypeError, ZeroDivisionError):
                    pass

        # ── ROIC (direct from key-metrics-ttm) ───────────────────────────
        roic = kmtm.get("returnOnInvestedCapitalTTM")
        if roic is not None:
            try:
                q["ROIC"] = self._fmt_pct(float(roic) * 100)
            except (ValueError, TypeError):
                pass

        # ── FCF Yield (direct from key-metrics-ttm) ───────────────────────
        fcf_y = kmtm.get("freeCashFlowYieldTTM")
        if fcf_y is not None:
            try:
                q["FCF Yield"] = self._fmt_pct(float(fcf_y) * 100)
            except (ValueError, TypeError):
                pass

        # ── P/FCF (direct from ratios-ttm) ───────────────────────────────
        p_fcf = rtm.get("priceToFreeCashFlowRatioTTM")
        if p_fcf is not None:
            try:
                p_fcf_f = float(p_fcf)
                if p_fcf_f > 0:
                    q["P/FCF"] = round(p_fcf_f, 4)
                else:
                    q["_p_fcf_not_meaningful_reason"] = "negative_or_zero_fcf"
            except (ValueError, TypeError):
                pass

        if not is_rows:
            return q

        ttm_rev = self._ttm_sum(is_rows, "revenue", 4)
        ttm_op  = self._ttm_sum(is_rows, "operatingIncome", 4)
        ttm_gp  = self._ttm_sum(is_rows, "grossProfit", 4)
        ttm_ni  = self._ttm_sum(is_rows, "netIncome", 4)

        # ── FCF Conversion (TTM FCF / TTM Net Income) ─────────────────────
        # Not meaningful when net income is zero, negative, or immaterial.
        if cf_rows and ttm_ni is not None:
            ttm_fcf = self._ttm_sum(cf_rows, "freeCashFlow", 4)
            if ttm_fcf is not None and abs(float(ttm_ni)) > 1_000_000:
                try:
                    q["FCF Conversion"] = round(float(ttm_fcf) / float(ttm_ni), 4)
                except (ValueError, TypeError, ZeroDivisionError):
                    pass

        # ── Diluted Shares Growth YoY ─────────────────────────────────────
        if len(is_rows) >= 5:
            sh_now = is_rows[0].get("weightedAverageShsOutDil")
            sh_py  = is_rows[4].get("weightedAverageShsOutDil")
            shr_g  = self._pct(sh_now, sh_py)
            if shr_g is not None:
                q["Diluted Shares Growth YoY"] = self._fmt_pct(shr_g)

        # ── SBC / Revenue ─────────────────────────────────────────────────
        # Do not assume missing SBC equals zero.
        if cf_rows and ttm_rev and float(ttm_rev) > 0:
            ttm_sbc = self._ttm_sum(cf_rows, "stockBasedCompensation", 4)
            if ttm_sbc is not None:
                try:
                    q["SBC / Revenue"] = self._fmt_pct(
                        round(float(ttm_sbc) / float(ttm_rev) * 100, 4)
                    )
                except (ValueError, TypeError, ZeroDivisionError):
                    pass

        # ── Revenue Acceleration ──────────────────────────────────────────
        # Latest-Q YoY growth minus previous-Q YoY growth.
        # Requires 6Q (4 recent + 4 prior = positions 0,1,4,5).
        if len(is_rows) >= 6:
            g0 = self._pct(is_rows[0].get("revenue"), is_rows[4].get("revenue"))
            g1 = self._pct(is_rows[1].get("revenue"), is_rows[5].get("revenue"))
            if g0 is not None and g1 is not None:
                q["Revenue Acceleration"] = round(g0 - g1, 4)

        # ── Gross Margin Change YoY (percentage points) ───────────────────
        if len(is_rows) >= 8:
            py_gp  = self._ttm_sum(is_rows[4:], "grossProfit", 4)
            py_rev = self._ttm_sum(is_rows[4:], "revenue", 4)
            if (ttm_gp and ttm_rev and py_gp and py_rev
                    and float(ttm_rev) != 0 and float(py_rev) != 0):
                try:
                    gm_now = float(ttm_gp)  / float(ttm_rev)  * 100
                    gm_py  = float(py_gp)   / float(py_rev)   * 100
                    q["Gross Margin Change YoY"] = round(gm_now - gm_py, 4)
                except (ValueError, TypeError, ZeroDivisionError):
                    pass

        # ── Incremental Operating Margin ──────────────────────────────────
        # Not meaningful when revenue change is zero or immaterial.
        if len(is_rows) >= 8:
            py_op  = self._ttm_sum(is_rows[4:], "operatingIncome", 4)
            py_rev = self._ttm_sum(is_rows[4:], "revenue", 4)
            if (ttm_op is not None and ttm_rev is not None
                    and py_op is not None and py_rev is not None):
                try:
                    d_oi  = float(ttm_op)  - float(py_op)
                    d_rev = float(ttm_rev) - float(py_rev)
                    if abs(d_rev) > 1_000_000:
                        q["Incremental Operating Margin"] = round(d_oi / d_rev * 100, 4)
                except (ValueError, TypeError, ZeroDivisionError):
                    pass

        return q

    async def _fetch_bs_quality(self, sym: str) -> tuple[dict, dict]:
        """
        Call 8: quarterly balance sheet.
        Returns (quality_fields, raw_bs_row).
        """
        raw = await self._get(
            "balance-sheet-statement", {"symbol": sym, "period": "quarter", "limit": 2}
        )
        bs: dict = (raw[0] if isinstance(raw, list) and raw else {})
        q: dict[str, Any] = {}

        # Cash = cashAndShortTermInvestments preferred; fall back to cash + stinv sum.
        cash_st = bs.get("cashAndShortTermInvestments")
        cash_eq = bs.get("cashAndCashEquivalents")
        st_inv  = bs.get("shortTermInvestments")

        cash_val: int | None = None
        if cash_st is not None:
            try:
                cash_val = int(float(cash_st))
            except (ValueError, TypeError):
                pass
        elif cash_eq is not None:
            try:
                cash_val = int(float(cash_eq) + (float(st_inv) if st_inv is not None else 0.0))
            except (ValueError, TypeError):
                pass
        if cash_val is not None:
            q["Cash"] = cash_val

        # Net Cash / Debt = cashAndShortTermInvestments − totalDebt
        # (positive = net cash; negative = net debt)
        # Note: FMP's native `netDebt` uses cash-only (not cashAndShortTermInvestments)
        # as the cash component, producing a different number; do NOT use it here.
        total_debt = bs.get("totalDebt")
        if cash_val is not None and total_debt is not None:
            try:
                q["Net Cash / Debt"] = int(cash_val - float(total_debt))
            except (ValueError, TypeError):
                pass

        return q, bs

    async def _fetch_analyst_estimates_quality(
        self,
        sym: str,
        mkt_cap: int | None,
        ev: float | None,
        ttm_rev: float | None,
    ) -> tuple[dict, dict | None]:
        """
        Call 9: annual analyst estimates.
        Identifies FY1 = nearest future fiscal-year-end date.
        Returns (quality_fields, fy1_row_or_None).
        """
        raw = await self._get(
            "analyst-estimates", {"symbol": sym, "period": "annual", "limit": 6}
        )
        rows: list[dict] = raw if isinstance(raw, list) else []
        q: dict[str, Any] = {}
        if not rows:
            return q, None

        # Sort ascending by FMP date to find the nearest future fiscal year.
        try:
            rows_sorted = sorted(rows, key=lambda r: r.get("date", ""))
        except Exception:
            rows_sorted = rows

        today_str = date.today().isoformat()
        fy1: dict | None = None
        for r in rows_sorted:
            if r.get("date", "") >= today_str:
                fy1 = r
                break

        if fy1 is None:
            return q, None

        fy1_date   = fy1.get("date", "")
        fy1_rev    = fy1.get("revenueAvg")
        fy1_eps    = fy1.get("epsAvg")
        fy1_ebitda = fy1.get("ebitdaAvg")
        n_rev      = fy1.get("numAnalystsRevenue")
        n_eps      = fy1.get("numAnalystsEps")

        # Provenance metadata
        q["_forward_estimate_fy1_date"] = fy1_date
        if n_rev is not None:
            try:
                q["_forward_estimate_fy1_n_analysts"] = int(n_rev)
            except (ValueError, TypeError):
                pass

        # Forward Revenue Growth = FY1 consensus revenue / TTM revenue − 1
        if fy1_rev and ttm_rev and float(ttm_rev) > 0:
            fwd_rev_g = self._pct(float(fy1_rev), float(ttm_rev))
            if fwd_rev_g is not None:
                q["Forward Revenue Growth"] = self._fmt_pct(fwd_rev_g)

        # Forward P/S = market cap / FY1 revenue
        if mkt_cap and fy1_rev and float(fy1_rev) > 0:
            try:
                q["Forward P/S"] = round(float(mkt_cap) / float(fy1_rev), 4)
            except (ValueError, TypeError, ZeroDivisionError):
                pass

        # Forward EV/Sales = enterprise value / FY1 revenue
        if ev and fy1_rev and float(fy1_rev) > 0:
            try:
                q["Forward EV/Sales"] = round(float(ev) / float(fy1_rev), 4)
            except (ValueError, TypeError, ZeroDivisionError):
                pass

        # Forward EV/EBITDA = enterprise value / FY1 EBITDA (only if positive)
        if ev and fy1_ebitda and float(fy1_ebitda) > 0:
            try:
                q["Forward EV/EBITDA"] = round(float(ev) / float(fy1_ebitda), 4)
            except (ValueError, TypeError, ZeroDivisionError):
                pass

        # ── Estimate revision 90D ─────────────────────────────────────────
        # Read BEFORE persisting today's observation (history write happens
        # in refresh_symbols after successful upsert_snapshot).
        try:
            from data.watchlist_estimate_history_store import get_revision_90d as _rev90d
            _loop = asyncio.get_event_loop()

            # Revenue revision
            if fy1_rev is not None:
                rev_90d = await _loop.run_in_executor(
                    None, _rev90d, sym.upper(), "revenue_annual", fy1_date
                )
                rp = rev_90d.get("revision_pct")
                if rp is not None:
                    q["Revenue Estimate Revision 90D"] = self._fmt_pct(rp)
                else:
                    _r = rev_90d.get("reason")
                    if _r:
                        q["_rev_revision_reason"] = _r
                if rev_90d.get("prior_date"):
                    q["_rev_revision_prior_date"] = rev_90d["prior_date"]

            # EPS revision
            if fy1_eps is not None:
                eps_90d = await _loop.run_in_executor(
                    None, _rev90d, sym.upper(), "eps_annual", fy1_date
                )
                ep = eps_90d.get("revision_pct")
                if ep is not None:
                    q["EPS Estimate Revision 90D"] = self._fmt_pct(ep)
                else:
                    _e = eps_90d.get("reason")
                    if _e:
                        q["_eps_revision_reason"] = _e
                if eps_90d.get("prior_date"):
                    q["_eps_revision_prior_date"] = eps_90d["prior_date"]
        except Exception as _rev_err:
            log.debug("[FMP_FUND] revision_90d error for %s: %s", sym, _rev_err)

        return q, fy1

    async def _fetch_scores_quality(
        self,
        sym: str,
        sector: str,
        industry: str,
    ) -> dict:
        """
        Call 10: FMP financial scores.
        Returns quality_fields dict.
        """
        raw = await self._get("financial-scores", {"symbol": sym})
        fs: dict = (
            raw[0] if isinstance(raw, list) and raw else
            (raw if isinstance(raw, dict) else {})
        )
        q: dict[str, Any] = {}
        if not fs:
            return q

        is_fin = _is_financial_company(sector, industry)

        altman_z = fs.get("altmanZScore")
        if is_fin:
            q["Altman Z-Risk"] = "not_meaningful"
            q["_altman_z_not_meaningful_reason"] = "financial_sector"
        elif altman_z is not None:
            try:
                z = float(altman_z)
                q["Altman Z-Score"] = round(z, 4)
                q["Altman Z-Risk"] = (
                    "safe"     if z >= 2.99 else
                    "grey"     if z >= 1.81 else
                    "distress"
                )
            except (ValueError, TypeError):
                pass

        piotroski = fs.get("piotroskiScore")
        if piotroski is not None:
            try:
                q["Piotroski Score"] = int(piotroski)
            except (ValueError, TypeError):
                pass

        return q

    @staticmethod
    def _compute_cash_runway(
        cash: int | None,
        ttm_fcf: int | None,
        sector: str,
        industry: str,
    ) -> dict:
        """
        Compute Cash Runway Months and Status.
        Positive or zero TTM FCF → self_funding.
        Negative TTM FCF → cash / |TTM FCF| × 12 months.
        Financial companies → not_meaningful.
        """
        is_fin = _is_financial_company(sector, industry)
        if is_fin:
            return {
                "Cash Runway Status": "not_meaningful",
                "_cash_runway_not_meaningful_reason": "financial_sector",
            }

        if cash is None or ttm_fcf is None:
            return {}

        try:
            fcf_f = float(ttm_fcf)
            cash_f = float(cash)
        except (ValueError, TypeError):
            return {}

        if fcf_f >= 0:
            return {"Cash Runway Status": "self_funding"}

        if cash_f <= 0:
            return {"Cash Runway Status": "critical", "Cash Runway Months": 0.0}

        months = round(cash_f / abs(fcf_f) * 12, 1)
        status = (
            "critical" if months < 12 else
            "caution"  if months < 24 else
            "adequate"
        )
        return {"Cash Runway Months": months, "Cash Runway Status": status}

    # ── Core normalizer ──────────────────────────────────────────────────────

    async def normalize_symbol(self, symbol: str) -> dict:
        """
        Fetch 10 FMP endpoints and return a dict of normalized fields.
        Returns {"fields": {...}, "missing_fields": [...], "fmp_call_count": N,
                 "_fy1_data": {...}|None}.
        Never raises — errors produce missing fields with CSV fallback.
        """
        sym = symbol.upper()
        fields: dict[str, Any] = {}
        missing: list[str] = []
        calls = 0

        # ── 1. Profile ───────────────────────────────────────────────────────
        raw = await self._get("profile", {"symbol": sym})
        calls += 1
        profile = (raw[0] if isinstance(raw, list) and raw else (raw if isinstance(raw, dict) else {}))
        mkt_cap = profile.get("marketCap")
        if mkt_cap is not None:
            fields["Market Cap"] = int(mkt_cap)
        else:
            missing.append("Market Cap")
        # Store current price for Forward P/E derivation
        _current_price: float | None = None
        try:
            _p = profile.get("price")
            if _p is not None:
                _current_price = float(_p)
        except (ValueError, TypeError):
            pass

        # Store share basis for live market cap resolution
        if mkt_cap is not None and mkt_cap > 0 and _current_price is not None and _current_price > 0:
            _implied = round(float(mkt_cap) / _current_price, 0)
            if _implied > 0:
                fields["_market_cap_implied_shares"]   = _implied
                fields["_market_cap_price_at_refresh"] = round(_current_price, 4)
                fields["_market_cap_static_source"]    = "fmp_profile"

        # Profile metadata (description, website, ceo, etc.)
        _profile_meta: dict[str, Any] = {}
        for _fmp_key, _meta_key in [
            ("description", "description"),
            ("website",     "website"),
            ("ceo",         "ceo"),
            ("image",       "image"),
            ("country",     "country"),
            ("sector",      "sector"),
            ("industry",    "industry"),
        ]:
            _v = profile.get(_fmp_key) or None
            if _v:
                _profile_meta[_meta_key] = str(_v)
        _exch = (
            profile.get("exchangeShortName")
            or profile.get("exchange")
            or None
        )
        if _exch:
            _profile_meta["exchange"] = str(_exch)
        try:
            _beta_raw = profile.get("beta")
            if _beta_raw is not None:
                _beta_f = float(_beta_raw)
                if _beta_f != 0.0:
                    _profile_meta["beta"] = _beta_f
        except (ValueError, TypeError):
            pass
        if _profile_meta:
            fields["profile"] = _profile_meta

        # ── 2. Income Statement (quarterly, 8Q) ──────────────────────────────
        raw = await self._get("income-statement", {"symbol": sym, "period": "quarter", "limit": 8})
        calls += 1
        is_rows: list[dict] = raw if isinstance(raw, list) else []

        if is_rows:
            # Revenue → TTM (sum of 4 most recent quarters)
            ttm_rev = self._ttm_sum(is_rows, "revenue", 4)
            if ttm_rev is not None:
                fields["Revenue"] = int(ttm_rev)
            else:
                missing.append("Revenue")

            # Operating Income → TTM sum
            ttm_op = self._ttm_sum(is_rows, "operatingIncome", 4)
            if ttm_op is not None:
                fields["Operating Income"] = int(ttm_op)
            else:
                missing.append("Operating Income")

            # EBIT → TTM sum (ebit key, fall back to operatingIncome sum)
            ttm_ebit = self._ttm_sum(is_rows, "ebit", 4)
            if ttm_ebit is not None:
                fields["EBIT"] = int(ttm_ebit)
            elif ttm_op is not None:
                fields["EBIT"] = int(ttm_op)
            else:
                missing.append("EBIT")

            # Revenue Growth (YoY): TTM vs prior-year TTM
            if len(is_rows) >= 8:
                ttm_rev_py  = self._ttm_sum(is_rows[4:], "revenue", 4)
                rev_yoy_pct = self._pct(ttm_rev, ttm_rev_py)
                if rev_yoy_pct is not None:
                    fields["Revenue Growth (YoY)"] = self._fmt_pct(rev_yoy_pct)
                else:
                    missing.append("Revenue Growth (YoY)")
            else:
                missing.append("Revenue Growth (YoY)")

            # Revenue Growth (Q): latest quarter vs same quarter prior year
            if len(is_rows) >= 5:
                rev_latest = is_rows[0].get("revenue")
                rev_py_q   = is_rows[4].get("revenue")
                rev_q_pct  = self._pct(rev_latest, rev_py_q)
                if rev_q_pct is not None:
                    fields["Revenue Growth (Q)"] = self._fmt_pct(rev_q_pct)
                else:
                    missing.append("Revenue Growth (Q)")
            else:
                missing.append("Revenue Growth (Q)")
        else:
            for f in ["Revenue", "Operating Income", "EBIT", "Revenue Growth (YoY)", "Revenue Growth (Q)"]:
                missing.append(f)
            is_rows = []

        # ── 3. Income Statement Growth (quarterly, 2Q) — EPS Growth only ─────
        raw = await self._get("income-statement-growth", {"symbol": sym, "period": "quarter", "limit": 2})
        calls += 1
        isg_rows: list[dict] = raw if isinstance(raw, list) else []
        if isg_rows:
            growth_eps = isg_rows[0].get("growthEPSDiluted")
            if growth_eps is not None:
                fields["EPS Growth"] = self._fmt_pct(float(growth_eps) * 100)
            else:
                missing.append("EPS Growth")
        else:
            missing += ["EPS Growth"]

        # ── 4. Cash Flow Statement (quarterly, 5Q) ───────────────────────────
        raw = await self._get("cash-flow-statement", {"symbol": sym, "period": "quarter", "limit": 5})
        calls += 1
        cf_rows: list[dict] = raw if isinstance(raw, list) else []
        if cf_rows:
            # Free Cash Flow → TTM sum
            ttm_fcf = self._ttm_sum(cf_rows, "freeCashFlow", 4)
            if ttm_fcf is not None:
                fields["Free Cash Flow"] = int(ttm_fcf)
            else:
                missing.append("Free Cash Flow")

            # FCF Margin: TTM FCF / TTM Revenue
            ttm_rev_for_margin = (
                self._ttm_sum(is_rows, "revenue", 4) if is_rows else None
            )
            if ttm_fcf is not None and ttm_rev_for_margin:
                try:
                    raw_mgn = (ttm_fcf / ttm_rev_for_margin) * 100  # type: ignore[operator]
                    fields["FCF Margin"] = self._fmt_pct(round(raw_mgn, 2))
                except Exception:
                    missing.append("FCF Margin")
            else:
                missing.append("FCF Margin")
        else:
            missing += ["Free Cash Flow", "FCF Margin"]

        # ── 5. Ratios TTM ────────────────────────────────────────────────────
        raw = await self._get("ratios-ttm", {"symbol": sym})
        calls += 1
        rtm: dict = (raw[0] if isinstance(raw, list) and raw else (raw if isinstance(raw, dict) else {}))
        if rtm and "_status" not in rtm:
            def _map_ratio(fmp_key: str, csv_key: str, fmt: str = "raw"):
                v = rtm.get(fmp_key)
                if v is not None:
                    if fmt == "pct":
                        fields[csv_key] = self._fmt_pct(float(v) * 100)
                    else:
                        fields[csv_key] = round(float(v), 6)
                else:
                    missing.append(csv_key)

            _map_ratio("grossProfitMarginTTM",     "Gross Margin", "pct")
            _map_ratio("priceToEarningsRatioTTM",  "PE Ratio")
            _map_ratio("priceToSalesRatioTTM",     "PS Ratio")
            _map_ratio("debtToEquityRatioTTM",     "Debt / Equity")
        else:
            missing += ["Gross Margin", "PE Ratio", "PS Ratio", "Debt / Equity"]

        # ── 6. Key Metrics TTM ───────────────────────────────────────────────
        raw = await self._get("key-metrics-ttm", {"symbol": sym})
        calls += 1
        kmtm: dict = (raw[0] if isinstance(raw, list) and raw else (raw if isinstance(raw, dict) else {}))
        if kmtm and "_status" not in kmtm:
            ev_ebitda = kmtm.get("evToEBITDATTM")
            if ev_ebitda is not None:
                fields["EV/EBITDA"] = round(float(ev_ebitda), 4)
            else:
                missing.append("EV/EBITDA")

            nd_ebitda = kmtm.get("netDebtToEBITDATTM")
            if nd_ebitda is not None:
                fields["Net Debt / EBITDA"] = round(float(nd_ebitda), 4)
            else:
                missing.append("Net Debt / EBITDA")
        else:
            missing += ["EV/EBITDA", "Net Debt / EBITDA"]

        # ── 7. Earnings (upcoming date + TQ/NQ estimate growth) ──────────────
        raw = await self._get("earnings", {"symbol": sym, "limit": 8})
        calls += 1
        earn_rows: list[dict] = (raw if isinstance(raw, list) else [])
        earn_rows.sort(key=lambda r: r.get("date", ""))

        past_earn   = [r for r in earn_rows if r.get("epsActual") is not None]
        future_earn = [r for r in earn_rows if r.get("epsActual") is None]

        # Earnings Date: next upcoming report date only.
        if future_earn:
            fields["Earnings Date"] = future_earn[0].get("date") or ""
        else:
            missing.append("Earnings Date")

        # TQ estimate (first upcoming quarter vs same quarter prior year)
        if future_earn and len(past_earn) >= 4:
            nxt    = future_earn[0]
            py_nxt = past_earn[-4]

            rev_nq = self._pct(
                nxt.get("revenueEstimated"),
                py_nxt.get("revenueActual"),
            )
            if rev_nq is not None:
                fields["Rev Growth Next Quarter"] = self._fmt_pct(rev_nq)
            else:
                missing.append("Rev Growth Next Quarter")

            eps_nq = self._pct(
                nxt.get("epsEstimated"),
                py_nxt.get("epsActual"),
            )
            if eps_nq is not None:
                fields["EPS Growth This Quarter"] = self._fmt_pct(eps_nq)
            else:
                missing.append("EPS Growth This Quarter")
        else:
            missing += ["Rev Growth Next Quarter", "EPS Growth This Quarter"]

        # ── Forward P/E — Priority 3 (fallback): next-quarter EPS × 4 ────────
        # Priority order: (1) FY1 annual consensus EPS [section 9 below],
        #                 (2) this quarterly×4 approximation [stored now as fallback],
        #                 (3) missing.
        # We store the fallback here if possible, then section 9 may upgrade it.
        _fpe_stored = False
        if future_earn and _current_price is not None and _current_price > 0:
            _nxt_earn_row = future_earn[0]
            _nxt_eps_est  = _nxt_earn_row.get("epsEstimated")
            try:
                _nxt_eps_f = float(_nxt_eps_est) if _nxt_eps_est is not None else None
            except (ValueError, TypeError):
                _nxt_eps_f = None
            if _nxt_eps_f is not None and _nxt_eps_f > 0:
                _fwd_eps_ann   = round(_nxt_eps_f * 4, 4)
                _fwd_pe_val    = round(_current_price / _fwd_eps_ann, 2)
                _fpe_warn: list[str] = []
                if _nxt_eps_f > 20.0:
                    _fpe_warn.append("HIGH_EPS_ESTIMATE")
                if _current_price > 700.0:
                    _fpe_warn.append("HIGH_PRICE_USED")
                fields["forward_pe_price_used"]      = _current_price
                fields["forward_pe_raw_eps_estimate"] = round(_nxt_eps_f, 4)
                fields["forward_pe_raw_period"]      = _nxt_earn_row.get("date") or ""
                if _fpe_warn:
                    fields["forward_pe_warning_codes"] = _fpe_warn
                if 1.0 <= _fwd_pe_val <= 500.0:
                    fields["Forward P/E"]               = _fwd_pe_val
                    fields["forward_eps_estimate"]      = _fwd_eps_ann
                    fields["forward_pe_source"]         = "quarterly_eps_annualized"
                    fields["forward_pe_is_approximate"] = True
                    _fpe_stored = True
        # NOTE: "Forward P/E" missing check deferred to after section 9 upgrade.

        # ── 8. Quality from existing data (0 new calls) ──────────────────────
        _dq = self._compute_derived_quality(is_rows, cf_rows, rtm, kmtm)
        for _k, _v in _dq.items():
            if _v is not None:
                fields[_k] = _v

        # ── 9. Balance sheet (new call) ──────────────────────────────────────
        bs_quality, _bs_row = await self._fetch_bs_quality(sym)
        calls += 1
        for _k, _v in bs_quality.items():
            if _v is not None:
                fields[_k] = _v

        # ── 10. Analyst estimates annual (new call) + Forward P/E upgrade ─────
        _mkt_cap_int: int | None = (
            int(float(mkt_cap)) if mkt_cap is not None and float(mkt_cap) > 0 else None
        )
        _ev_float: float | None = None
        if kmtm:
            _ev_raw = kmtm.get("enterpriseValueTTM")
            if _ev_raw is not None:
                try:
                    _ev_float = float(_ev_raw)
                except (ValueError, TypeError):
                    pass
        _ttm_rev_float: float | None = None
        if is_rows:
            _ttm_rev_float = self._ttm_sum(is_rows, "revenue", 4)

        est_quality, _fy1_row = await self._fetch_analyst_estimates_quality(
            sym, _mkt_cap_int, _ev_float, _ttm_rev_float
        )
        calls += 1
        for _k, _v in est_quality.items():
            if _v is not None:
                fields[_k] = _v

        # Forward P/E upgrade: FY1 annual consensus EPS (Priority 2, better than
        # quarterly×4 when available).  Overrides the fallback stored above;
        # also provides the value when quarterly×4 was unavailable.
        # Negative/zero forward EPS → not meaningful (do not store).
        if _fy1_row is not None and _current_price is not None and _current_price > 0:
            _fy1_eps_raw = _fy1_row.get("epsAvg")
            if _fy1_eps_raw is not None:
                try:
                    _fy1_eps_f = float(_fy1_eps_raw)
                    if _fy1_eps_f > 0:
                        _fwd_pe_fy1 = round(_current_price / _fy1_eps_f, 2)
                        if 1.0 <= _fwd_pe_fy1 <= 500.0:
                            fields["Forward P/E"]               = _fwd_pe_fy1
                            fields["forward_eps_estimate"]      = round(_fy1_eps_f, 4)
                            fields["forward_pe_source"]         = "fy1_annual_consensus_eps"
                            fields["forward_pe_is_approximate"] = False
                            fields["forward_pe_raw_period"]     = _fy1_row.get("date", "")
                            fields["forward_pe_price_used"]     = _current_price
                            # Clear the quarterly×4 approximation warning codes if we
                            # are now using the higher-quality FY1 source.
                            fields.pop("forward_pe_warning_codes", None)
                            _fpe_stored = True
                except (ValueError, TypeError):
                    pass

        # Deferred missing check (after upgrade attempt)
        if not _fpe_stored:
            missing.append("Forward P/E")

        # ── 11. Financial scores (new call) ──────────────────────────────────
        _sector   = str((_profile_meta or {}).get("sector",   "") or "")
        _industry = str((_profile_meta or {}).get("industry", "") or "")
        scores_quality = await self._fetch_scores_quality(sym, _sector, _industry)
        calls += 1
        for _k, _v in scores_quality.items():
            if _v is not None:
                fields[_k] = _v

        # ── 12. Cash Runway (uses Cash from call 8 + TTM FCF from call 4) ────
        _cash_val    = fields.get("Cash")
        _ttm_fcf_val = fields.get("Free Cash Flow")
        runway = self._compute_cash_runway(_cash_val, _ttm_fcf_val, _sector, _industry)
        for _k, _v in runway.items():
            fields[_k] = _v

        # CSV fallback fields — always marked missing; caller preserves CSV values.
        for csv_fallback in [
            "Shares Insiders",
            "Revenue Growth Est.",
            "Rev Growth Next Year",
            "Rev Growth This Year",
            "EPS Growth Est.",
            "EPS Growth Next Quarter",
            "EPS Growth This Year",
            "EPS Growth Next Year",
        ]:
            missing.append(csv_fallback)

        return {
            "fields": fields,
            "missing_fields": list(set(missing)),
            "fmp_call_count": calls,
            "_fy1_data": _fy1_row,   # Raw FY1 row for estimate history persistence
        }

    # ── Batch refresh ────────────────────────────────────────────────────────

    async def refresh_symbols(
        self,
        symbols: list[str],
        watchlist_id: str,
        dev_force: bool = False,
    ) -> dict:
        """
        Refresh FMP fundamentals for a list of symbols.
        Skips symbols refreshed within the last 7 days unless dev_force=True.
        Returns diagnostic summary dict.
        """
        from data.watchlist_fundamentals_store import (
            get_snapshots_bulk, upsert_snapshot,
        )
        from data.watchlist_estimate_history_store import (
            ensure_table as _ensure_hist,
            upsert_estimate_observation as _upsert_obs,
        )
        from services.watchlist_quote_cache import is_fmp_symbol_eligible

        # Ensure estimate history table exists (safe no-op if already created)
        _ensure_hist()

        eligible = [s for s in symbols if is_fmp_symbol_eligible(s)]
        snapshots = get_snapshots_bulk(eligible)

        started_at = datetime.now(timezone.utc)
        refreshed: list[str] = []
        skipped:   list[str] = []
        failed:    list[str] = []
        empty_payload_preserved: list[str] = []
        empty_payload_no_prior:  list[str] = []

        for sym in eligible:
            snap = snapshots.get(sym.upper())
            if not dev_force and snap:
                try:
                    nxt = snap.get("next_refresh_at", "")
                    if nxt:
                        nxt_dt = datetime.fromisoformat(nxt.replace("Z", "+00:00"))
                        if datetime.now(timezone.utc) < nxt_dt:
                            skipped.append(sym)
                            continue
                except Exception:
                    pass

            try:
                result = await self.normalize_symbol(sym)

                # ── No-null-overwrite for profile metadata ────────────────────
                _existing_snap = snapshots.get(sym.upper())
                if _existing_snap and _existing_snap.get("fields"):
                    _old_profile = (_existing_snap["fields"].get("profile") or {})
                    if _old_profile:
                        _new_profile = result["fields"].get("profile") or {}
                        _merged_profile: dict[str, Any] = {**_old_profile}
                        for _k, _v in _new_profile.items():
                            if _v is not None and _v != "":
                                _merged_profile[_k] = _v
                        result["fields"]["profile"] = _merged_profile

                # ── Quality field carry-forward ───────────────────────────────
                # When any optional new endpoint (balance-sheet, analyst-estimates,
                # financial-scores) fails or returns no data, carry forward the prior
                # usable value so a transient error does not erase historical quality
                # fields from the complete fields JSONB overwrite.
                if _existing_snap and _existing_snap.get("fields"):
                    _old_fields = _existing_snap["fields"]
                    for _qk in _QUALITY_CARRY_FIELDS:
                        if result["fields"].get(_qk) is None and _old_fields.get(_qk) is not None:
                            result["fields"][_qk] = _old_fields[_qk]

                outcome = upsert_snapshot(
                    symbol=sym,
                    watchlist_id=watchlist_id,
                    fields=result["fields"],
                    missing_fields=result["missing_fields"],
                    fmp_call_count=result["fmp_call_count"],
                )
                if outcome == "success":
                    refreshed.append(sym)

                    # ── Persist estimate history for revision tracking ────────
                    _fy1 = result.get("_fy1_data")
                    if _fy1:
                        _fy1_date = _fy1.get("date", "")
                        _loop = asyncio.get_event_loop()
                        if _fy1.get("revenueAvg") is not None:
                            await _loop.run_in_executor(
                                None,
                                _upsert_obs,
                                sym.upper(), "revenue_annual", "annual",
                                _fy1_date, _fy1.get("revenueAvg"),
                                _fy1.get("numAnalystsRevenue"), "fmp_analyst_estimates",
                                _fy1_date,
                            )
                        if _fy1.get("epsAvg") is not None:
                            await _loop.run_in_executor(
                                None,
                                _upsert_obs,
                                sym.upper(), "eps_annual", "annual",
                                _fy1_date, _fy1.get("epsAvg"),
                                _fy1.get("numAnalystsEps"), "fmp_analyst_estimates",
                                _fy1_date,
                            )
                        if _fy1.get("ebitdaAvg") is not None:
                            await _loop.run_in_executor(
                                None,
                                _upsert_obs,
                                sym.upper(), "ebitda_annual", "annual",
                                _fy1_date, _fy1.get("ebitdaAvg"),
                                _fy1.get("numAnalystsRevenue"), "fmp_analyst_estimates",
                                _fy1_date,
                            )

                elif outcome == "empty_payload_preserved_lkg":
                    log.warning(
                        "[FMP_FUND] %s: EMPTY_FUNDAMENTALS_PAYLOAD — prior usable "
                        "snapshot preserved, retry scheduled sooner", sym,
                    )
                    empty_payload_preserved.append(sym)
                elif outcome == "empty_payload_no_prior":
                    log.warning(
                        "[FMP_FUND] %s: EMPTY_FUNDAMENTALS_PAYLOAD — no usable prior "
                        "snapshot, remains retryable (no fake fresh row written)", sym,
                    )
                    empty_payload_no_prior.append(sym)
                else:
                    failed.append(sym)
            except Exception as exc:
                log.warning("[FMP_FUND] refresh_symbols(%s) error: %s", sym, exc)
                failed.append(sym)

        finished_at = datetime.now(timezone.utc)
        return {
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "total_required": len(eligible),
            "refreshed_symbols": len(refreshed),
            "skipped_fresh_symbols": len(skipped),
            "failed_symbols": len(failed),
            "empty_payload_preserved_lkg_symbols": len(empty_payload_preserved),
            "empty_payload_no_prior_symbols": len(empty_payload_no_prior),
            "refreshed": refreshed,
            "skipped": skipped,
            "failed": failed,
            "empty_payload_preserved_lkg": empty_payload_preserved,
            "empty_payload_no_prior": empty_payload_no_prior,
        }


def merge_fmp_into_csv_row(csv_row: dict, fmp_fields: dict) -> dict:
    """
    Overlay FMP field values onto a CSV row.
    Rule: FMP non-null value wins; FMP null/missing → preserve existing CSV value.
    Returns a new dict (does not mutate csv_row).
    """
    merged = dict(csv_row)
    for k, v in fmp_fields.items():
        if v is not None and v != "":
            merged[k] = v
    return merged


def apply_fmp_overlays(
    csv_data: list[dict],
    snapshots: dict[str, dict],
) -> list[dict]:
    """
    Apply FMP fundamentals cache to the full CSV data list.
    Returns the updated list with FMP values overlaid per symbol.

    Stale Earnings Date rule (Part C):
      Earnings Date is only meaningful if it is today or in the future.
      If the final value (from FMP or CSV) is before today (ET), blank it.
      A past date is worse than no date — it misrepresents next earnings.
    """
    from zoneinfo import ZoneInfo
    today_et = datetime.now(tz=ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

    out = []
    for row in csv_data:
        sym = (row.get("Symbol") or row.get("symbol") or row.get("Ticker") or "").strip().upper()
        snap = snapshots.get(sym)
        if snap and snap.get("fields"):
            merged = merge_fmp_into_csv_row(row, snap["fields"])
        else:
            merged = dict(row)

        # Stale Earnings Date rule: blank any past date regardless of source.
        earn = str(merged.get("Earnings Date") or "").strip()
        if earn and earn < today_et:
            merged["Earnings Date"] = ""

        out.append(merged)
    return out
