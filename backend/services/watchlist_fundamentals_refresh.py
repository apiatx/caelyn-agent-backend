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
                                             Forward Revenue Growth (FY/FY), Fwd P/S,
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

TTM strictness (Part 1):
  All TTM aggregations require EXACTLY 4 distinct-date quarterly rows with
  non-null values. Partial sums (< 4 quarters) are rejected and treated as
  missing. Use _ttm_sum_strict() for all TTM computations.

FY1 fiscal alignment (Part 4):
  FY1 = first analyst-estimate date STRICTLY AFTER the last completed fiscal
  year-end date from the income-statement rows.  Using today's date as the
  anchor was wrong for non-calendar-year companies (e.g. IREN FY ends June 30;
  when today is July 20 the current-FY estimate date "2026-06-30" was
  incorrectly skipped).  _identify_completed_fiscal_year() reads fiscalYear /
  period from IS rows to determine the anchor date.

Forward Revenue Growth (Part 4):
  Computed as FY1_consensus / completed_FY_actual_revenue − 1, NOT FY1/TTM.
  TTM already includes completed + partial quarters so comparing to it under-
  states growth for non-year-end-aligned stocks.

Carry-forward (Part 7):
  Source-aware: only carry when the responsible endpoint suffered a transient
  failure.  _not_meaningful_active blocks carry of semantically-invalid cached
  values (e.g. stale P/FCF when current FCF is negative).

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
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx

log = logging.getLogger(__name__)

_FMP_BASE   = "https://financialmodelingprep.com/stable"
_CALL_DELAY = 0.45      # seconds between FMP calls ≈ 133 req/min (< 200 req/min Starter)
_TIMEOUT    = 18        # seconds per HTTP request

# Minimum materially-positive net income for FCF Conversion to be meaningful
_FCF_CONVERSION_NI_FLOOR = 5_000_000   # $5 M

# Minimum revenue change (absolute + relative) for Incremental Operating Margin
_INC_OP_MARGIN_MIN_DELTA_ABS = 50_000_000   # $50 M
_INC_OP_MARGIN_MIN_DELTA_REL = 0.01         # 1% of prior-year TTM revenue

# Tolerance window (days) for matching earnings prior-year quarter by report date
_EARNINGS_PY_TOLERANCE_DAYS = 60

# ── Carry-forward field groups — per source endpoint ─────────────────────────
# Fields are ONLY carried forward when the responsible endpoint suffered a
# transient failure (_outcome == "transient_failure").  Semantically-invalid
# fields are additionally blocked by _not_meaningful_active (see Part 7).
#
# Issue 1: Cash Runway fields are NOT carried — they are always recomputed
# after carry-forward using the current refresh's strict TTM FCF.  Carrying
# runway directly could preserve a stale "self_funding" status when FCF has
# since turned negative (or vice-versa).  Only the raw balance-sheet inputs
# (Cash, Net Cash / Debt) are eligible for carry.

_BS_CARRY_FIELDS: frozenset[str] = frozenset({
    "Cash", "Net Cash / Debt",
    # Net Debt / EBITDA is computed post-BS (Part 3) — carry when BS fails transiently.
    "Net Debt / EBITDA",
    # Raw valuation inputs stored for live overlay — carry when BS fails transiently.
    "_valuation_total_debt",
    "_valuation_cash_and_short_term_investments",
    # Cash Runway Months, Cash Runway Status, _cash_runway_not_meaningful_reason
    # intentionally excluded — always recomputed after carry (see Issue 1).
})

_EST_CARRY_FIELDS: frozenset[str] = frozenset({
    "Forward Revenue Growth",
    "Forward P/S", "Forward EV/Sales", "Forward EV/EBITDA",
    "Revenue Estimate Revision 90D", "EPS Estimate Revision 90D",
    "_rev_revision_prior_date", "_rev_revision_reason",
    "_eps_revision_prior_date", "_eps_revision_reason",
    "_forward_estimate_fy1_date", "_forward_estimate_fy1_n_analysts",
    "_forward_fy1_years_ahead", "_forward_fy1_actual_fy_date",
    # FY1 raw inputs for live overlay — carry when estimates call fails transiently.
    "_valuation_fy1_revenue", "_valuation_fy1_ebitda", "_valuation_fy1_eps",
})

_SCORES_CARRY_FIELDS: frozenset[str] = frozenset({
    "Altman Z-Score", "Altman Z-Risk", "_altman_z_not_meaningful_reason",
    "Piotroski Score",
})

# Combined (documentation / backward compat reference)
_QUALITY_CARRY_FIELDS: frozenset[str] = (
    _BS_CARRY_FIELDS | _EST_CARRY_FIELDS | _SCORES_CARRY_FIELDS
)

# ── Metric-specific applicability helpers ─────────────────────────────────────
#
# FMP places operating companies (bitcoin miners, crypto exchanges, brokerage
# platforms) in the same broad "Financial Services" sector as deposit-funded
# banks.  Blocking metrics purely by sector string ("Financial Services" → all
# blocked) mis-classifies IREN, COIN, HOOD, etc.
#
# The correct discriminator is the INDUSTRY string, which FMP populates at a
# granular level (e.g. "Banks - Diversified" vs "Financial - Capital Markets").
#
# Three separate helpers replace the old single _is_financial_company() gate so
# each metric can use its own, narrower applicability rule.

# Deposit-funded or structurally bank-like industries.
# Cash Runway and the BS Current Ratio fallback are not meaningful here.
_DEPOSIT_FUNDED_INDUSTRY_KW: tuple[str, ...] = (
    "banks",            # "Banks - Diversified", "Banks - Regional"
    "thrift",           # "Thrifts & Mortgage Finance"
    "savings",          # savings institutions
    "mortgage finance", # mortgage-balance-sheet lenders
    "credit services",  # SOFI-style regulated bank/deposit businesses
)

# Traditional insurer industries.
_INSURANCE_INDUSTRY_KW: tuple[str, ...] = (
    "insurance",        # "Insurance - Property & Casualty", "Life Insurance", …
)

# REIT industries — structurally incompatible with Altman Z, but not
# necessarily with Cash Runway (e.g. data-center operator EQIX).
_REIT_INDUSTRY_KW: tuple[str, ...] = (
    "reit",
    "real estate investment trust",
)


def _is_cash_runway_not_meaningful(sector: str, industry: str) -> bool:
    """
    Cash Runway is not economically meaningful for deposit-funded banks and
    traditional insurers.

    Operating companies that FMP places in a broad 'Financial Services' sector
    — bitcoin miners (IREN), crypto exchanges (COIN), brokerage platforms
    (HOOD), fintech software companies — return False so that a calculated
    runway or self_funding status is surfaced.

    REITs are NOT blocked here; their cash-management model differs from
    banks, and the metric can be informative for REIT sub-types.
    """
    i = str(industry or "").lower()
    return (
        any(kw in i for kw in _DEPOSIT_FUNDED_INDUSTRY_KW)
        or any(kw in i for kw in _INSURANCE_INDUSTRY_KW)
    )


def _is_altman_z_not_meaningful(sector: str, industry: str) -> bool:
    """
    Altman Z-Score is not meaningful for deposit-funded banks, traditional
    insurers, and REITs — their balance sheets are structurally incompatible
    with the Altman model.

    Operating companies in a broad financial sector that are not deposit-
    funded (exchanges, miners, payment platforms) may still receive a
    provider-supplied Altman Z.
    """
    i = str(industry or "").lower()
    return (
        any(kw in i for kw in _DEPOSIT_FUNDED_INDUSTRY_KW)
        or any(kw in i for kw in _INSURANCE_INDUSTRY_KW)
        or any(kw in i for kw in _REIT_INDUSTRY_KW)
    )


def _is_current_ratio_not_meaningful(sector: str, industry: str) -> bool:
    """
    The balance-sheet-derived Current Ratio fallback is not meaningful for
    deposit-funded banks (demand deposits skew 'current liabilities') and
    insurers (claim reserves distort the ratio).

    Uses the same narrow industry match as _is_cash_runway_not_meaningful.
    REITs are not blocked here.
    """
    i = str(industry or "").lower()
    return (
        any(kw in i for kw in _DEPOSIT_FUNDED_INDUSTRY_KW)
        or any(kw in i for kw in _INSURANCE_INDUSTRY_KW)
    )


def _is_leverage_metrics_not_meaningful(sector: str, industry: str) -> bool:
    """
    Net Debt/EBITDA and Interest Coverage are not economically meaningful for
    deposit-funded banks (liability structure is funding, not leverage) and
    traditional insurers (float is the liability).

    Uses the same narrow industry match as _is_cash_runway_not_meaningful.
    Operating companies in broad 'Financial Services' (IREN, COIN, HOOD, etc.)
    return False — their debt/coverage ratios ARE informative.
    """
    i = str(industry or "").lower()
    return (
        any(kw in i for kw in _DEPOSIT_FUNDED_INDUSTRY_KW)
        or any(kw in i for kw in _INSURANCE_INDUSTRY_KW)
    )


def _is_financial_company(sector: str, industry: str) -> bool:
    """
    Backward-compatible alias — returns True when ANY of the three metric
    gates fires.  New code should call the specific helper directly.
    """
    return _is_altman_z_not_meaningful(sector, industry)


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

    async def _get_quality(
        self, endpoint: str, params: dict | None = None
    ) -> tuple[list | dict, str]:
        """
        Issue 2 — Status-aware HTTP helper for the three optional Quality
        endpoints (balance-sheet, analyst-estimates, financial-scores).

        Returns (data, outcome) where outcome is one of:
          'success_with_data'  — HTTP 200, non-empty payload; use only this data
          'success_no_data'    — HTTP 200 empty payload, or HTTP 404; do NOT carry
          'not_entitled'       — HTTP 402 or 403; do NOT carry
          'transient_failure'  — HTTP 429, 5xx, timeout, connection error,
                                 or invalid JSON; MAY carry prior source values

        Does NOT break the seven existing _get() consumers — those still use
        the backward-compatible method above.
        """
        p: dict[str, Any] = {**(params or {}), "apikey": self._key}
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.get(f"{_FMP_BASE}/{endpoint}", params=p)
            await asyncio.sleep(_CALL_DELAY)
            sc = resp.status_code
            if sc in (402, 403):
                log.debug("[FMP_FUND] %s HTTP %s (not_entitled)", endpoint, sc)
                return [], "not_entitled"
            if sc == 404:
                return [], "success_no_data"
            if sc == 429 or sc >= 500:
                log.debug("[FMP_FUND] %s HTTP %s (transient_failure)", endpoint, sc)
                return [], "transient_failure"
            if sc == 200:
                try:
                    data = resp.json()
                except Exception:
                    log.debug("[FMP_FUND] %s invalid JSON → transient_failure", endpoint)
                    return [], "transient_failure"
                if isinstance(data, list):
                    return (data, "success_with_data") if data else (data, "success_no_data")
                if isinstance(data, dict):
                    return (data, "success_with_data") if data else (data, "success_no_data")
                return [], "success_no_data"
            # Other unexpected status codes
            log.debug("[FMP_FUND] %s HTTP %s (unexpected → transient)", endpoint, sc)
            return [], "transient_failure"
        except httpx.TimeoutException:
            log.debug("[FMP_FUND] %s timeout → transient_failure", endpoint)
            await asyncio.sleep(_CALL_DELAY)
            return [], "transient_failure"
        except Exception as exc:
            log.debug("[FMP_FUND] %s error: %s → transient_failure", endpoint, exc)
            await asyncio.sleep(_CALL_DELAY)
            return [], "transient_failure"

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
        """
        Non-strict sum of `field` across up to n rows (kept for legacy use).
        Prefer _ttm_sum_strict for all primary TTM field computations.
        """
        vals = []
        for r in rows[:n]:
            v = r.get(field)
            if v is not None:
                vals.append(float(v))
        return sum(vals) if len(vals) == n else (sum(vals) if vals else None)

    @staticmethod
    def _ttm_sum_strict(rows: list[dict], field: str) -> float | None:
        """
        Part 1 — strict TTM sum.
        Requires EXACTLY 4 distinct-date quarterly rows each with a non-null
        numeric value for ``field``.  Returns None when:
          • fewer than 4 distinct dates are present
          • any of the 4 most-recent distinct rows has a null value
          • any value cannot be converted to float
        Duplicate/restated rows sharing the same date are deduplicated
        (first occurrence kept, matching FMP's descending sort order).
        """
        seen_dates: set[str] = set()
        unique_rows: list[dict] = []
        for r in rows:
            d = str(r.get("date", "") or "")
            if d and d not in seen_dates:
                seen_dates.add(d)
                unique_rows.append(r)
            if len(unique_rows) == 4:
                break
        if len(unique_rows) < 4:
            return None
        vals: list[float] = []
        for r in unique_rows:
            v = r.get(field)
            if v is None:
                return None
            try:
                vals.append(float(v))
            except (ValueError, TypeError):
                return None
        return sum(vals)

    @staticmethod
    def _ttm_avg_strict(rows: list[dict], field: str) -> float | None:
        """
        Part 2 — strict TTM arithmetic mean (used for share-count averaging).
        Same dedup / completeness rules as _ttm_sum_strict; returns mean of 4.
        """
        seen_dates: set[str] = set()
        unique_rows: list[dict] = []
        for r in rows:
            d = str(r.get("date", "") or "")
            if d and d not in seen_dates:
                seen_dates.add(d)
                unique_rows.append(r)
            if len(unique_rows) == 4:
                break
        if len(unique_rows) < 4:
            return None
        vals: list[float] = []
        for r in unique_rows:
            v = r.get(field)
            if v is None:
                return None
            try:
                vals.append(float(v))
            except (ValueError, TypeError):
                return None
        return sum(vals) / 4.0

    # ── Fiscal-period helpers ────────────────────────────────────────────────

    @staticmethod
    def _get_fiscal_period_row(
        rows: list[dict],
        fiscal_year: str,
        period: str,
    ) -> dict | None:
        """
        Return the first IS row whose ``fiscalYear`` and ``period`` match.
        Used for Revenue Acceleration quarter-to-same-quarter-prior-year matching.
        """
        for r in rows:
            if (str(r.get("fiscalYear", "") or "") == fiscal_year
                    and str(r.get("period", "") or "") == period):
                return r
        return None

    @staticmethod
    def _match_prior_year_earnings(
        nxt_row: dict,
        past_earn: list[dict],
    ) -> dict | None:
        """
        Part 5 — fiscal-period match for earnings comparison.
        Finds the past earnings row whose report date falls within
        ±_EARNINGS_PY_TOLERANCE_DAYS of (nxt_row['date'] − 365 days).
        FMP earnings rows have period=None / fiscalDateEnding=None, so
        positional matching (past_earn[-4]) is replaced by date proximity.
        Returns None when no match is found within tolerance.
        """
        nxt_date_str = nxt_row.get("date", "")
        if not nxt_date_str:
            return None
        try:
            nxt_date = date.fromisoformat(nxt_date_str)
        except ValueError:
            return None

        target = date(nxt_date.year - 1, nxt_date.month, nxt_date.day)

        best_row: dict | None = None
        best_delta = _EARNINGS_PY_TOLERANCE_DAYS + 1
        for r in past_earn:
            r_date_str = r.get("date", "")
            if not r_date_str:
                continue
            try:
                r_date = date.fromisoformat(r_date_str)
            except ValueError:
                continue
            delta = abs((r_date - target).days)
            if delta <= _EARNINGS_PY_TOLERANCE_DAYS and delta < best_delta:
                best_delta = delta
                best_row   = r
        return best_row

    @staticmethod
    def _identify_completed_fiscal_year(
        is_rows: list[dict],
    ) -> tuple[str | None, str | None, float | None]:
        """
        Part 4 / Issue 4 — identify the latest completed fiscal year from IS rows.

        A fiscal year is "complete" when exactly one canonical row exists for
        each of Q1, Q2, Q3, and Q4.  Duplicates (restated quarters sharing the
        same fiscalYear+period) are deduplicated by selecting the row with the
        latest ``date`` value (most-recent restatement wins).  Revenue must be
        non-null for all four quarters.  The FY revenue is the sum of exactly
        those four canonical revenue values — never more.

        Returns (fy_label, fy_end_date_str, fy_actual_revenue).
        All three are None when no complete fiscal year can be determined.
        fy_end_date_str is the date field of the canonical Q4 row.
        """
        by_fy: dict[str, list[dict]] = defaultdict(list)
        for r in is_rows:
            fy     = r.get("fiscalYear")
            period = r.get("period")
            if fy is not None and period:
                by_fy[str(fy)].append(r)

        complete: list[tuple[str, str, float]] = []
        for fy_label, fy_rows in by_fy.items():
            # Issue 4: select exactly one canonical row per quarter.
            # When duplicates exist for the same (fiscalYear, period) pair,
            # prefer the row with the latest date (most-recent restatement).
            canonical: dict[str, dict] = {}
            for r in fy_rows:
                p = str(r.get("period", "") or "")
                if p not in ("Q1", "Q2", "Q3", "Q4"):
                    continue
                d = str(r.get("date", "") or "")
                if p not in canonical or d > str(canonical[p].get("date", "") or ""):
                    canonical[p] = r

            # All four quarters must be present
            if not all(qk in canonical for qk in ("Q1", "Q2", "Q3", "Q4")):
                continue

            # Revenue must be non-null and convertible for every quarter
            revenues: list[float] = []
            valid = True
            for qk in ("Q1", "Q2", "Q3", "Q4"):
                rev = canonical[qk].get("revenue")
                if rev is None:
                    valid = False
                    break
                try:
                    revenues.append(float(rev))
                except (ValueError, TypeError):
                    valid = False
                    break
            if not valid:
                continue

            fy_end_date = str(canonical["Q4"].get("date", "") or "")
            if not fy_end_date:
                continue

            complete.append((fy_label, fy_end_date, sum(revenues)))

        if not complete:
            return None, None, None

        # Latest complete FY = highest Q4 date
        complete.sort(key=lambda x: x[1], reverse=True)
        return complete[0]

    # ── Quality helpers (zero additional FMP calls) ──────────────────────────

    def _compute_derived_quality(
        self,
        is_rows: list[dict],
        cf_rows: list[dict],
        rtm: dict,
        kmtm: dict,
        sector: str = "",
        industry: str = "",
    ) -> tuple[dict, set[str]]:
        """
        Derive Quality fields from already-fetched FMP data. Zero additional calls.
        Returns (fields_dict, not_meaningful_active_set).

        not_meaningful_active contains the names of fields that were explicitly
        determined to be "not meaningful" in this refresh cycle.  Carry-forward
        of old cached values for these fields must be suppressed.
        """
        q: dict[str, Any]  = {}
        nm: set[str]       = set()   # not_meaningful_active

        # ── Operating Margin ─────────────────────────────────────────────
        # Priority 1: direct from ratios-ttm.
        # Priority 2 (Part 2): derive from TTM IS rows when rtm is absent.
        op_m = rtm.get("operatingProfitMarginTTM")
        if op_m is not None:
            try:
                q["Operating Margin"] = self._fmt_pct(float(op_m) * 100)
            except (ValueError, TypeError):
                pass

        # ── Current Ratio ─────────────────────────────────────────────────
        # Direct from ratios-ttm. Balance-sheet fallback applied in
        # normalize_symbol() after _fetch_bs_quality() runs (Part 9).
        cur = rtm.get("currentRatioTTM")
        if cur is not None:
            try:
                q["Current Ratio"] = round(float(cur), 4)
            except (ValueError, TypeError):
                pass

        # ── Interest Coverage ─────────────────────────────────────────────
        # Strict TTM EBIT / TTM interest expense (income-statement basis).
        # N/M for deposit-funded banks and insurers.
        # N/M when interest expense is zero (no debt), negative (net interest
        # income — e.g. AAPL nets interest income against expense), or missing.
        # Do NOT use FMP interestCoverageRatioTTM — it produces economically
        # misleading values for cash-rich companies and financial companies.
        if _is_leverage_metrics_not_meaningful(sector, industry):
            q["_interest_coverage_not_meaningful_reason"] = (
                "not_meaningful_for_financial_company"
            )
            nm.add("Interest Coverage")
        elif is_rows:
            ttm_ebit_ic = (
                self._ttm_sum_strict(is_rows, "ebit")
                or self._ttm_sum_strict(is_rows, "operatingIncome")
            )
            ttm_ie_ic = self._ttm_sum_strict(is_rows, "interestExpense")
            if ttm_ebit_ic is not None and ttm_ie_ic is not None:
                try:
                    ttm_ie_f = float(ttm_ie_ic)
                    if ttm_ie_f > 0:
                        q["Interest Coverage"] = round(
                            float(ttm_ebit_ic) / ttm_ie_f, 4
                        )
                        q["_interest_coverage_method"] = (
                            "strict_ttm_ebit_over_absolute_interest_expense"
                        )
                    elif ttm_ie_f == 0.0:
                        q["_interest_coverage_not_meaningful_reason"] = (
                            "zero_interest_expense"
                        )
                        nm.add("Interest Coverage")
                    else:
                        # Negative = net interest income (cash-rich, no net debt)
                        q["_interest_coverage_not_meaningful_reason"] = (
                            "net_interest_income_not_meaningful"
                        )
                        nm.add("Interest Coverage")
                except (ValueError, TypeError, ZeroDivisionError):
                    pass
            else:
                q["_interest_coverage_not_meaningful_reason"] = (
                    "missing_ebit_or_interest_data"
                )

        # ── ROIC (provider-direct from key-metrics-ttm) ───────────────────
        roic = kmtm.get("returnOnInvestedCapitalTTM")
        if roic is not None:
            try:
                q["ROIC"] = self._fmt_pct(float(roic) * 100)
                q["_roic_source"] = "fmp_key_metrics_ttm_provider_direct"
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
                    nm.add("P/FCF")
            except (ValueError, TypeError):
                pass

        if not is_rows:
            return q, nm

        # Strict TTM aggregates used throughout the derived computations
        ttm_rev = self._ttm_sum_strict(is_rows, "revenue")
        ttm_op  = self._ttm_sum_strict(is_rows, "operatingIncome")
        ttm_gp  = self._ttm_sum_strict(is_rows, "grossProfit")
        ttm_ni  = self._ttm_sum_strict(is_rows, "netIncome")

        # ── Operating Margin fallback (Part 2) ────────────────────────────
        # Derive from TTM IS data when ratios-ttm did not supply it.
        if "Operating Margin" not in q and ttm_op is not None and ttm_rev and float(ttm_rev) != 0:
            try:
                q["Operating Margin"] = self._fmt_pct(
                    round(float(ttm_op) / float(ttm_rev) * 100, 4)
                )
                q["_op_margin_source"] = "derived_from_is"
            except (ValueError, TypeError, ZeroDivisionError):
                pass

        # ── FCF Conversion (TTM FCF / TTM Net Income) ─────────────────────
        # Part 2: gate requires NI > _FCF_CONVERSION_NI_FLOOR (materially
        # positive), NOT abs(NI).  Negative NI → not_meaningful.
        if cf_rows and ttm_ni is not None:
            ttm_fcf_conv = self._ttm_sum_strict(cf_rows, "freeCashFlow")
            try:
                ni_f = float(ttm_ni)
            except (ValueError, TypeError):
                ni_f = None
            if ni_f is not None and ni_f > _FCF_CONVERSION_NI_FLOOR:
                if ttm_fcf_conv is not None:
                    try:
                        q["FCF Conversion"] = round(
                            float(ttm_fcf_conv) / ni_f, 4
                        )
                    except (ValueError, TypeError, ZeroDivisionError):
                        pass
            elif ni_f is not None:
                reason = (
                    "negative_net_income"  if ni_f < 0 else
                    "immaterial_net_income"
                )
                q["_fcf_conversion_not_meaningful_reason"] = reason
                nm.add("FCF Conversion")

        # ── Diluted Shares Growth YoY ─────────────────────────────────────
        # Part 2: compare TTM-average shares (not single quarters) to avoid
        # point-in-time noise from buyback timing.
        # Requires 8 IS rows (4 current + 4 prior).
        if len(is_rows) >= 8:
            curr_avg = self._ttm_avg_strict(is_rows[:4], "weightedAverageShsOutDil")
            prior_avg = self._ttm_avg_strict(is_rows[4:8], "weightedAverageShsOutDil")
            shr_g = self._pct(curr_avg, prior_avg)
            if shr_g is not None:
                q["Diluted Shares Growth YoY"] = self._fmt_pct(shr_g)

        # ── SBC / Revenue ─────────────────────────────────────────────────
        if cf_rows and ttm_rev and float(ttm_rev) > 0:
            ttm_sbc = self._ttm_sum_strict(cf_rows, "stockBasedCompensation")
            if ttm_sbc is not None:
                try:
                    q["SBC / Revenue"] = self._fmt_pct(
                        round(float(ttm_sbc) / float(ttm_rev) * 100, 4)
                    )
                except (ValueError, TypeError, ZeroDivisionError):
                    pass

        # ── Revenue Acceleration ──────────────────────────────────────────
        # Issue 3: latest-Q YoY growth minus previous-Q YoY growth.
        # When both current rows carry fiscal metadata (fiscalYear + period):
        #   • Require an EXACT prior-fiscal-year, same-period match.
        #   • If no match is found, leave Revenue Acceleration missing.
        #   • Do NOT use a positional fallback (rows[4]/[5]).
        # Positional fallback is allowed ONLY when the current row has no
        # fiscal metadata at all, and is clearly marked as approximate.
        if len(is_rows) >= 2:
            q0 = is_rows[0]
            q1 = is_rows[1]

            q0_fy  = str(q0.get("fiscalYear", "") or "")
            q0_per = str(q0.get("period", "") or "")
            q1_fy  = str(q1.get("fiscalYear", "") or "")
            q1_per = str(q1.get("period", "") or "")

            _ra_method = "unavailable"

            if q0_fy and q0_per and q1_fy and q1_per:
                # Both rows have fiscal metadata — exact match required; no fallback
                py_q0: dict | None = None
                py_q1: dict | None = None
                try:
                    py_q0 = self._get_fiscal_period_row(
                        is_rows, str(int(q0_fy) - 1), q0_per
                    )
                except (ValueError, TypeError):
                    pass
                try:
                    py_q1 = self._get_fiscal_period_row(
                        is_rows, str(int(q1_fy) - 1), q1_per
                    )
                except (ValueError, TypeError):
                    pass
                if py_q0 is not None and py_q1 is not None:
                    g0 = self._pct(q0.get("revenue"), py_q0.get("revenue"))
                    g1 = self._pct(q1.get("revenue"), py_q1.get("revenue"))
                    if g0 is not None and g1 is not None:
                        # Store as formatted string — value is already in
                        # percentage-point units; avoids frontend ×100 heuristics.
                        q["Revenue Acceleration"] = self._fmt_pct(round(g0 - g1, 4))
                        _ra_method = "fiscal_year_period_exact"
                # If no exact match found, metric stays missing (no fallback)
            else:
                # No fiscal metadata on current rows → positional fallback allowed
                py_q0_pos: dict | None = is_rows[4] if len(is_rows) >= 5 else None
                py_q1_pos: dict | None = is_rows[5] if len(is_rows) >= 6 else None
                if py_q0_pos is not None and py_q1_pos is not None:
                    g0 = self._pct(q0.get("revenue"), py_q0_pos.get("revenue"))
                    g1 = self._pct(q1.get("revenue"), py_q1_pos.get("revenue"))
                    if g0 is not None and g1 is not None:
                        q["Revenue Acceleration"] = self._fmt_pct(round(g0 - g1, 4))
                        _ra_method = "position_approximate"

            q["_revenue_acceleration_alignment_method"] = _ra_method

        # ── Gross Margin Change YoY (percentage points) ───────────────────
        # Requires complete strict TTM for both current and prior periods.
        if len(is_rows) >= 8:
            py_gp  = self._ttm_sum_strict(is_rows[4:], "grossProfit")
            py_rev = self._ttm_sum_strict(is_rows[4:], "revenue")
            if (ttm_gp and ttm_rev and py_gp and py_rev
                    and float(ttm_rev) != 0 and float(py_rev) != 0):
                try:
                    gm_now = float(ttm_gp)  / float(ttm_rev)  * 100
                    gm_py  = float(py_gp)   / float(py_rev)   * 100
                    # Store as formatted string — value is already in percentage
                    # points; avoids frontend ×100 heuristics.
                    q["Gross Margin Change YoY"] = self._fmt_pct(round(gm_now - gm_py, 4))
                except (ValueError, TypeError, ZeroDivisionError):
                    pass

        # ── Incremental Operating Margin ──────────────────────────────────
        # Part 3: not meaningful when revenue change is zero, negative, or
        # immaterial.  Requires d_rev > 0 AND exceeds both absolute and
        # relative materiality thresholds.
        if len(is_rows) >= 8:
            py_op  = self._ttm_sum_strict(is_rows[4:], "operatingIncome")
            py_rev_iom = self._ttm_sum_strict(is_rows[4:], "revenue")
            if (ttm_op is not None and ttm_rev is not None
                    and py_op is not None and py_rev_iom is not None):
                try:
                    d_oi  = float(ttm_op)     - float(py_op)
                    d_rev = float(ttm_rev)    - float(py_rev_iom)
                    min_delta = max(
                        _INC_OP_MARGIN_MIN_DELTA_ABS,
                        _INC_OP_MARGIN_MIN_DELTA_REL * abs(float(py_rev_iom)),
                    )
                    if d_rev > min_delta:
                        # Store as formatted string — already ×100 → percentage units.
                        q["Incremental Operating Margin"] = self._fmt_pct(round(
                            d_oi / d_rev * 100, 4
                        ))
                    else:
                        reason = (
                            "negative_revenue_change" if d_rev <= 0 else
                            "immaterial_revenue_change"
                        )
                        q["_incr_op_margin_not_meaningful_reason"] = reason
                        nm.add("Incremental Operating Margin")
                except (ValueError, TypeError, ZeroDivisionError):
                    pass

        return q, nm

    async def _fetch_bs_quality(
        self, sym: str
    ) -> tuple[dict, dict, str]:
        """
        Call 8: quarterly balance sheet.
        Returns (quality_fields, raw_bs_row, outcome).
        outcome: 'success_with_data' | 'success_no_data' | 'not_entitled' | 'transient_failure'

        Issue 2: uses _get_quality() so HTTP 200+empty is success_no_data (don't carry)
        and HTTP 402/429/5xx are correctly classified.
        """
        raw, outcome = await self._get_quality(
            "balance-sheet-statement",
            {"symbol": sym, "period": "quarter", "limit": 2},
        )
        bs: dict = (raw[0] if isinstance(raw, list) and raw else {})
        q: dict[str, Any] = {}

        if outcome != "success_with_data" or not bs:
            # Preserve the specific outcome so carry-forward logic can act correctly.
            final_outcome = outcome if outcome != "success_with_data" else "success_no_data"
            return q, {}, final_outcome

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
                cash_val = int(
                    float(cash_eq) + (float(st_inv) if st_inv is not None else 0.0)
                )
            except (ValueError, TypeError):
                pass
        if cash_val is not None:
            q["Cash"] = cash_val

        # Net Cash / Debt = cashAndShortTermInvestments − totalDebt
        total_debt = bs.get("totalDebt")
        if cash_val is not None and total_debt is not None:
            try:
                q["Net Cash / Debt"] = int(cash_val - float(total_debt))
            except (ValueError, TypeError):
                pass

        return q, bs, "success_with_data"

    async def _fetch_analyst_estimates_quality(
        self,
        sym: str,
        mkt_cap: int | None,
        ev: float | None,
        completed_fy_date: str | None,
        completed_fy_revenue: float | None,
    ) -> tuple[dict, dict | None, str]:
        """
        Call 9: annual analyst estimates.

        Part 4 — FY1 selection anchor:
          FY1 = first estimate with date STRICTLY AFTER completed_fy_date,
          NOT after today.  This correctly handles non-calendar companies
          (e.g. IREN FY ends June 30; on July 20 the estimate date
          "2026-06-30" <= completed_fy_date "2025-06-30" is False, so it is
          correctly selected as FY1, rather than incorrectly skipped because
          it is before today).

        Part 4 — Forward Revenue Growth:
          FY1_consensus / completed_FY_actual_revenue − 1  (not FY1/TTM).

        Part 6 — Revision 90D:
          Fresh FY1 estimate passed as current_value to get_revision_90d()
          so the revision does not lag by one refresh cycle.

        Returns (quality_fields, fy1_row_or_None, outcome).
        outcome: 'success_with_data' | 'success_no_data' | 'transient_failure'
        """
        raw, _http_outcome = await self._get_quality(
            "analyst-estimates",
            {"symbol": sym, "period": "annual", "limit": 6},
        )
        rows: list[dict] = raw if isinstance(raw, list) else []
        q: dict[str, Any] = {}

        if _http_outcome != "success_with_data":
            # Propagate: not_entitled / success_no_data / transient_failure
            return q, None, _http_outcome

        # Sort ascending by FMP date
        try:
            rows_sorted = sorted(rows, key=lambda r: r.get("date", ""))
        except Exception:
            rows_sorted = rows

        # Part 4: anchor on completed FY end date, not today
        anchor = completed_fy_date or ""
        fy1: dict | None = None
        for r in rows_sorted:
            if r.get("date", "") > anchor:
                fy1 = r
                break

        if fy1 is None:
            return q, None, "success_no_data"

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

        # How many fiscal years ahead is FY1 relative to completed FY?
        if anchor and fy1_date and len(anchor) >= 4 and len(fy1_date) >= 4:
            try:
                years_ahead = int(fy1_date[:4]) - int(anchor[:4])
                if years_ahead >= 0:
                    q["_forward_fy1_years_ahead"] = years_ahead
            except (ValueError, TypeError):
                pass

        # Store completed FY date for frontend context
        if anchor:
            q["_forward_fy1_actual_fy_date"] = anchor

        # Part 4: Forward Revenue Growth = FY1 / completed_FY_actual − 1
        if (fy1_rev is not None
                and completed_fy_revenue is not None
                and float(completed_fy_revenue) > 0):
            fwd_rev_g = self._pct(float(fy1_rev), float(completed_fy_revenue))
            if fwd_rev_g is not None:
                q["Forward Revenue Growth"] = self._fmt_pct(fwd_rev_g)

        # Forward P/S = market cap / FY1 revenue
        if mkt_cap and fy1_rev and float(fy1_rev) > 0:
            try:
                q["Forward P/S"] = round(float(mkt_cap) / float(fy1_rev), 4)
            except (ValueError, TypeError, ZeroDivisionError):
                pass

        # Forward EV/Sales = enterprise value / FY1 revenue.
        # Only stored when EV > 0 AND result > 0 (Part 4 invalid multiple gate).
        # Negative EV (e.g. heavy net-cash small caps) → N/M.
        if ev and float(ev) > 0 and fy1_rev and float(fy1_rev) > 0:
            try:
                _fwd_ev_sales = round(float(ev) / float(fy1_rev), 4)
                if _fwd_ev_sales > 0:
                    q["Forward EV/Sales"] = _fwd_ev_sales
                else:
                    q["_forward_ev_sales_not_meaningful_reason"] = (
                        "nonpositive_result"
                    )
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        elif ev is not None and float(ev) <= 0:
            q["_forward_ev_sales_not_meaningful_reason"] = "nonpositive_enterprise_value"
        elif not fy1_rev or float(fy1_rev) <= 0:
            q["_forward_ev_sales_not_meaningful_reason"] = "missing_or_nonpositive_fy1_revenue"

        # Forward EV/EBITDA = enterprise value / FY1 EBITDA.
        # Requires: EV > 0, FY1 EBITDA > 0, calculated result > 0 (Part 4 gate).
        if ev and float(ev) > 0 and fy1_ebitda and float(fy1_ebitda) > 0:
            try:
                _fwd_ev_ebitda = round(float(ev) / float(fy1_ebitda), 4)
                if _fwd_ev_ebitda > 0:
                    q["Forward EV/EBITDA"] = _fwd_ev_ebitda
                else:
                    q["_forward_ev_ebitda_not_meaningful_reason"] = (
                        "nonpositive_result"
                    )
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        elif ev is not None and float(ev) <= 0:
            q["_forward_ev_ebitda_not_meaningful_reason"] = "nonpositive_enterprise_value"
        elif not fy1_ebitda or float(fy1_ebitda) <= 0:
            q["_forward_ev_ebitda_not_meaningful_reason"] = (
                "missing_or_nonpositive_fy1_ebitda"
            )

        # ── Estimate revision 90D (Part 6) ────────────────────────────────
        # Pass fresh current_value so revision does not lag by one cycle.
        try:
            from data.watchlist_estimate_history_store import get_revision_90d as _rev90d
            _loop = asyncio.get_event_loop()
            _today = date.today()

            # Revenue revision
            if fy1_rev is not None:
                _fy1_rev_f = float(fy1_rev)
                rev_90d = await _loop.run_in_executor(
                    None,
                    lambda: _rev90d(
                        sym.upper(), "revenue_annual", fy1_date,
                        _fy1_rev_f, _today,
                    ),
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
                _fy1_eps_f = float(fy1_eps)
                eps_90d = await _loop.run_in_executor(
                    None,
                    lambda: _rev90d(
                        sym.upper(), "eps_annual", fy1_date,
                        _fy1_eps_f, _today,
                    ),
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

        return q, fy1, "success_with_data"

    async def _fetch_scores_quality(
        self,
        sym: str,
        sector: str,
        industry: str,
    ) -> tuple[dict, str]:
        """
        Call 10: FMP financial scores.
        Returns (quality_fields, outcome).
        outcome: 'success_with_data' | 'success_no_data' | 'not_entitled' | 'transient_failure'

        Issue 2: HTTP 200 + empty list → 'success_no_data' (NOT transient_failure).
        This prevents a stale Altman Z score from being retained indefinitely when
        FMP stops returning scores data for a symbol.
        """
        raw, _http_outcome = await self._get_quality("financial-scores", {"symbol": sym})
        fs: dict = (
            raw[0] if isinstance(raw, list) and raw else
            (raw if isinstance(raw, dict) else {})
        )
        q: dict[str, Any] = {}

        if _http_outcome != "success_with_data" or not fs:
            final = _http_outcome if _http_outcome != "success_with_data" else "success_no_data"
            return q, final

        _altman_blocked = _is_altman_z_not_meaningful(sector, industry)
        altman_z = fs.get("altmanZScore")

        if _altman_blocked:
            q["Altman Z-Risk"] = "not_meaningful"
            q["_altman_z_not_meaningful_reason"] = "deposit_funded_insurer_or_reit"
        elif altman_z is not None:
            try:
                z = float(altman_z)
                q["Altman Z-Score"] = round(z, 4)
                q["Altman Z-Risk"]  = (
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

        # Provenance tags — these scores are provider-direct, not independently verified
        q["_altman_z_source"]   = "fmp_financial_scores_provider_direct"
        q["_piotroski_source"]  = "fmp_financial_scores_provider_direct"

        return q, "success_with_data"

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
        if _is_cash_runway_not_meaningful(sector, industry):
            return {
                "Cash Runway Status": "not_meaningful",
                "_cash_runway_not_meaningful_reason": "deposit_funded_or_insurer",
            }

        if cash is None or ttm_fcf is None:
            return {}

        try:
            fcf_f  = float(ttm_fcf)
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
        Returns {
          "fields":               {...},
          "missing_fields":       [...],
          "fmp_call_count":       N,
          "_fy1_data":            {...}|None,
          "_not_meaningful_active": set[str],
          "_bs_outcome":          str,
          "_est_outcome":         str,
          "_scores_outcome":      str,
        }.
        Never raises — errors produce missing fields with CSV fallback.
        """
        sym    = symbol.upper()
        fields: dict[str, Any] = {}
        missing: list[str]     = []
        calls  = 0

        # ── 1. Profile ───────────────────────────────────────────────────────
        raw = await self._get("profile", {"symbol": sym})
        calls += 1
        profile = (
            raw[0] if isinstance(raw, list) and raw
            else (raw if isinstance(raw, dict) else {})
        )
        mkt_cap = profile.get("marketCap")
        if mkt_cap is not None:
            fields["Market Cap"] = int(mkt_cap)
        else:
            missing.append("Market Cap")

        # Current price for Forward P/E derivation
        _current_price: float | None = None
        try:
            _p = profile.get("price")
            if _p is not None:
                _current_price = float(_p)
        except (ValueError, TypeError):
            pass

        # Share basis for live market-cap resolution
        if mkt_cap is not None and mkt_cap > 0 and _current_price is not None and _current_price > 0:
            _implied = round(float(mkt_cap) / _current_price, 0)
            if _implied > 0:
                fields["_market_cap_implied_shares"]   = _implied
                fields["_market_cap_price_at_refresh"] = round(_current_price, 4)
                fields["_market_cap_static_source"]    = "fmp_profile"

        # Profile metadata
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

        # Extract sector/industry early — needed for Current Ratio BS fallback
        # (Issue 5) and Cash Runway (step 12).  These are also used in
        # _fetch_scores_quality and _is_financial_company checks below.
        _sector   = str(_profile_meta.get("sector",   "") or "")
        _industry = str(_profile_meta.get("industry", "") or "")

        # ── 2. Income Statement (quarterly, 8Q) ──────────────────────────────
        raw = await self._get(
            "income-statement",
            {"symbol": sym, "period": "quarter", "limit": 8},
        )
        calls += 1
        is_rows: list[dict] = raw if isinstance(raw, list) else []

        if is_rows:
            # Revenue → TTM (strict 4-quarter)
            ttm_rev = self._ttm_sum_strict(is_rows, "revenue")
            if ttm_rev is not None:
                fields["Revenue"] = int(ttm_rev)
            else:
                missing.append("Revenue")

            # Operating Income → TTM strict
            ttm_op = self._ttm_sum_strict(is_rows, "operatingIncome")
            if ttm_op is not None:
                fields["Operating Income"] = int(ttm_op)
            else:
                missing.append("Operating Income")

            # EBIT → TTM strict; fall back to op income sum
            ttm_ebit = self._ttm_sum_strict(is_rows, "ebit")
            if ttm_ebit is not None:
                fields["EBIT"] = int(ttm_ebit)
            elif ttm_op is not None:
                fields["EBIT"] = int(ttm_op)
            else:
                missing.append("EBIT")

            # Revenue Growth (YoY): TTM vs prior-year TTM (both strict)
            if len(is_rows) >= 8:
                ttm_rev_py  = self._ttm_sum_strict(is_rows[4:], "revenue")
                rev_yoy_pct = self._pct(ttm_rev, ttm_rev_py)
                if rev_yoy_pct is not None:
                    fields["Revenue Growth (YoY)"] = self._fmt_pct(rev_yoy_pct)
                else:
                    missing.append("Revenue Growth (YoY)")
            else:
                missing.append("Revenue Growth (YoY)")

            # Revenue Growth (Q): latest Q vs same Q prior year
            # Issue 3: when fiscal metadata (fiscalYear + period) exists on the
            # current row, require an exact prior-FY same-period match.  If no
            # match is found, leave the metric missing — do NOT fall back to a
            # positional row.  Positional fallback is allowed only when the row
            # has no fiscal metadata at all (marked as approximate).
            _rgq_method = "unavailable"
            _rg_q0_fy  = str(is_rows[0].get("fiscalYear", "") or "")
            _rg_q0_per = str(is_rows[0].get("period", "") or "")
            if _rg_q0_fy and _rg_q0_per:
                # Fiscal metadata present → exact match required; no fallback
                try:
                    _rg_py_row = self._get_fiscal_period_row(
                        is_rows, str(int(_rg_q0_fy) - 1), _rg_q0_per
                    )
                    if _rg_py_row is not None:
                        rev_q_pct = self._pct(
                            is_rows[0].get("revenue"), _rg_py_row.get("revenue")
                        )
                        if rev_q_pct is not None:
                            fields["Revenue Growth (Q)"] = self._fmt_pct(rev_q_pct)
                            _rgq_method = "fiscal_year_period_exact"
                        else:
                            missing.append("Revenue Growth (Q)")
                    else:
                        # No exact prior-year match — metric stays missing
                        missing.append("Revenue Growth (Q)")
                except (ValueError, TypeError):
                    missing.append("Revenue Growth (Q)")
            else:
                # No fiscal metadata → positional fallback allowed (approximate)
                if len(is_rows) >= 5:
                    rev_q_pct = self._pct(
                        is_rows[0].get("revenue"), is_rows[4].get("revenue")
                    )
                    if rev_q_pct is not None:
                        fields["Revenue Growth (Q)"] = self._fmt_pct(rev_q_pct)
                        _rgq_method = "position_approximate"
                    else:
                        missing.append("Revenue Growth (Q)")
                else:
                    missing.append("Revenue Growth (Q)")
            fields["_revenue_growth_q_alignment_method"] = _rgq_method

            # Part 4: Identify last completed fiscal year (FY/FY anchor for estimates)
            _completed_fy_label, _completed_fy_date, _completed_fy_revenue = (
                self._identify_completed_fiscal_year(is_rows)
            )

            # ── Raw valuation inputs (Part 2) ─────────────────────────────
            # Store slow-changing TTM inputs so the live overlay can recompute
            # price-sensitive metrics without another FMP call.
            _val_ttm_ni    = self._ttm_sum_strict(is_rows, "netIncome")
            _val_ttm_rev   = self._ttm_sum_strict(is_rows, "revenue")
            # Prefer direct IS ebitda field per spec Part 2 (do not derive from CF D&A)
            _val_ttm_ebitda = self._ttm_sum_strict(is_rows, "ebitda")
            _val_ttm_ebit   = (
                self._ttm_sum_strict(is_rows, "ebit")
                or self._ttm_sum_strict(is_rows, "operatingIncome")
            )
            _val_ttm_ie  = self._ttm_sum_strict(is_rows, "interestExpense")
            _val_stmt_ccy = str(is_rows[0].get("reportedCurrency") or "") if is_rows else ""

        else:
            for f in [
                "Revenue", "Operating Income", "EBIT",
                "Revenue Growth (YoY)", "Revenue Growth (Q)",
            ]:
                missing.append(f)
            is_rows = []
            _completed_fy_label    = None
            _completed_fy_date     = None
            _completed_fy_revenue  = None
            _val_ttm_ni      = None
            _val_ttm_rev     = None
            _val_ttm_ebitda  = None
            _val_ttm_ebit    = None
            _val_ttm_ie      = None
            _val_stmt_ccy    = ""

        # ── 3. Income Statement Growth (quarterly, 2Q) — EPS Growth only ─────
        raw = await self._get(
            "income-statement-growth",
            {"symbol": sym, "period": "quarter", "limit": 2},
        )
        calls += 1
        isg_rows: list[dict] = raw if isinstance(raw, list) else []
        # NOTE: isg_rows retained for backward compat; EPS Growth is now
        # derived from the IS rows fetched in step 2 (true YoY diluted EPS).

        # ── EPS Growth (Part 1) — True quarterly diluted EPS YoY ─────────
        # Replace growthEPSDiluted (sequential QoQ basic EPS) with:
        #   latest-quarter diluted EPS / same fiscal quarter prior year - 1
        # Uses exact fiscalYear + period matching from the 8 IS rows.
        _eps_growth_stored = False
        if is_rows:
            _q0 = is_rows[0]
            _q0_fy  = str(_q0.get("fiscalYear", "") or "")
            _q0_per = str(_q0.get("period", "") or "")
            _q0_eps_raw = _q0.get("epsDiluted")

            fields["_eps_growth_method"] = "diluted_eps_yoy_fiscal_exact"
            if _q0_fy and _q0_per:
                fields["_eps_growth_current_period"] = f"FY{_q0_fy} {_q0_per}"

            if _q0_eps_raw is not None and _q0_fy and _q0_per:
                try:
                    _q0_eps_f = float(_q0_eps_raw)
                    fields["_eps_growth_current_eps"] = round(_q0_eps_f, 4)

                    # Exact prior-year same-period match
                    try:
                        _py_fy = str(int(_q0_fy) - 1)
                    except (ValueError, TypeError):
                        _py_fy = None

                    _py_row = (
                        self._get_fiscal_period_row(is_rows, _py_fy, _q0_per)
                        if _py_fy else None
                    )
                    if _py_row is not None:
                        _py_fy_r  = str(_py_row.get("fiscalYear", "") or "")
                        _py_per_r = str(_py_row.get("period", "") or "")
                        fields["_eps_growth_prior_period"] = f"FY{_py_fy_r} {_py_per_r}"
                        _py_eps_raw = _py_row.get("epsDiluted")
                        if _py_eps_raw is not None:
                            _py_eps_f = float(_py_eps_raw)
                            fields["_eps_growth_prior_eps"] = round(_py_eps_f, 4)
                            if _py_eps_f > 0 and _q0_eps_f >= 0:
                                # Standard case — both positive; calculate YoY
                                _eps_pct = round(
                                    (_q0_eps_f / _py_eps_f - 1) * 100, 4
                                )
                                fields["EPS Growth"] = self._fmt_pct(_eps_pct)
                                _eps_growth_stored = True
                            elif _py_eps_f <= 0 and _q0_eps_f > 0:
                                # Loss → profit turnaround
                                fields["_eps_growth_status"] = "turned_profitable"
                                _eps_growth_stored = True  # meaningful, just not a pct
                            elif _py_eps_f > 0 and _q0_eps_f < 0:
                                # Profit → loss
                                fields["_eps_growth_status"] = "turned_unprofitable"
                                _eps_growth_stored = True
                            else:
                                # Both <= 0 — loss deepening or flat
                                fields["_eps_growth_not_meaningful_reason"] = (
                                    "negative_eps_basis"
                                )
                        else:
                            fields["_eps_growth_not_meaningful_reason"] = (
                                "missing_prior_fiscal_period"
                            )
                    else:
                        fields["_eps_growth_not_meaningful_reason"] = (
                            "missing_prior_fiscal_period"
                        )
                        fields["_eps_growth_prior_period"] = (
                            f"FY{_py_fy} {_q0_per}" if _py_fy else "unknown"
                        )
                except (ValueError, TypeError):
                    pass
            elif not _q0_fy or not _q0_per:
                fields["_eps_growth_not_meaningful_reason"] = "missing_fiscal_metadata"
            elif _q0_eps_raw is None:
                fields["_eps_growth_not_meaningful_reason"] = "missing_diluted_eps"

        else:
            # IS data unavailable — tag method so every snapshot is identifiable
            # as processed by the new engine regardless of data coverage.
            fields["_eps_growth_method"] = "diluted_eps_yoy_fiscal_exact"
            fields["_eps_growth_not_meaningful_reason"] = "missing_income_statement_data"

        if not _eps_growth_stored:
            missing.append("EPS Growth")

        # ── 4. Cash Flow Statement (quarterly, 5Q) ───────────────────────────
        raw = await self._get(
            "cash-flow-statement",
            {"symbol": sym, "period": "quarter", "limit": 5},
        )
        calls += 1
        cf_rows: list[dict] = raw if isinstance(raw, list) else []
        if cf_rows:
            ttm_fcf = self._ttm_sum_strict(cf_rows, "freeCashFlow")
            if ttm_fcf is not None:
                fields["Free Cash Flow"] = int(ttm_fcf)
            else:
                missing.append("Free Cash Flow")

            # FCF Margin: strict TTM FCF / strict TTM Revenue
            ttm_rev_for_margin = (
                self._ttm_sum_strict(is_rows, "revenue") if is_rows else None
            )
            if ttm_fcf is not None and ttm_rev_for_margin and float(ttm_rev_for_margin) > 0:
                try:
                    raw_mgn = (ttm_fcf / ttm_rev_for_margin) * 100
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
        rtm: dict = (
            raw[0] if isinstance(raw, list) and raw
            else (raw if isinstance(raw, dict) else {})
        )
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

            _map_ratio("grossProfitMarginTTM",    "Gross Margin", "pct")
            # PE Ratio: only store strictly positive values.
            # Negative P/E = negative earnings (not a meaningful multiple).
            # Omitting it prevents the frontend from displaying a misleading
            # negative number; the frontend should render null/missing as N/M.
            _pe_raw = rtm.get("priceToEarningsRatioTTM")
            if _pe_raw is not None:
                try:
                    _pe_f = float(_pe_raw)
                    if _pe_f > 0:
                        fields["PE Ratio"] = round(_pe_f, 6)
                    else:
                        fields["_pe_not_meaningful_reason"] = "negative_or_zero_eps"
                        missing.append("PE Ratio")
                except (ValueError, TypeError):
                    missing.append("PE Ratio")
            else:
                missing.append("PE Ratio")
            # PS Ratio: only store strictly positive values (Part 4 gate).
            # Negative P/S arises when revenue is negative (restatement) or
            # FMP uses a negative base — not a meaningful multiple.
            _ps_raw = rtm.get("priceToSalesRatioTTM")
            if _ps_raw is not None:
                try:
                    _ps_f = float(_ps_raw)
                    if _ps_f > 0:
                        fields["PS Ratio"] = round(_ps_f, 6)
                    else:
                        fields["_ps_not_meaningful_reason"] = "negative_or_zero_revenue"
                        missing.append("PS Ratio")
                except (ValueError, TypeError):
                    missing.append("PS Ratio")
            else:
                missing.append("PS Ratio")
            _map_ratio("debtToEquityRatioTTM",    "Debt / Equity")
        else:
            missing += ["Gross Margin", "PE Ratio", "PS Ratio", "Debt / Equity"]

        # ── 6. Key Metrics TTM ───────────────────────────────────────────────
        raw = await self._get("key-metrics-ttm", {"symbol": sym})
        calls += 1
        kmtm: dict = (
            raw[0] if isinstance(raw, list) and raw
            else (raw if isinstance(raw, dict) else {})
        )
        # Also capture EV from key-metrics for _valuation_* inputs and forward multiples
        _val_ev: float | None = None
        _val_shares: float | None = None
        if kmtm and "_status" not in kmtm:
            _ev_raw_km = kmtm.get("enterpriseValueTTM")
            if _ev_raw_km is not None:
                try:
                    _val_ev = float(_ev_raw_km)
                except (ValueError, TypeError):
                    pass

            # EV/EBITDA: positive-only gate per Part 4.
            # Negative EV/EBITDA arises when EBITDA is negative (pre-profitability
            # companies) or very rarely when EV is negative (deep net-cash).
            # Showing a negative multiple is misleading — omit and mark reason.
            ev_ebitda = kmtm.get("evToEBITDATTM")
            if ev_ebitda is not None:
                try:
                    _ev_ebitda_f = float(ev_ebitda)
                    if _ev_ebitda_f > 0:
                        fields["EV/EBITDA"] = round(_ev_ebitda_f, 4)
                    else:
                        fields["_ev_ebitda_not_meaningful_reason"] = (
                            "negative_ebitda_or_negative_ev"
                        )
                        missing.append("EV/EBITDA")
                except (ValueError, TypeError):
                    missing.append("EV/EBITDA")
            else:
                missing.append("EV/EBITDA")

            # Net Debt / EBITDA is now computed from BS data in step 9
            # (see post-BS-call section) to use independently validated inputs.
            # Do NOT source from netDebtToEBITDATTM here.
        else:
            missing += ["EV/EBITDA"]

        # ── 7. Earnings (upcoming date + TQ/NQ estimate growth) ──────────────
        raw = await self._get("earnings", {"symbol": sym, "limit": 8})
        calls += 1
        earn_rows: list[dict] = (raw if isinstance(raw, list) else [])
        earn_rows.sort(key=lambda r: r.get("date", ""))

        past_earn   = [r for r in earn_rows if r.get("epsActual")  is not None]
        future_earn = [r for r in earn_rows if r.get("epsActual")  is     None]

        # Earnings Date: next upcoming report date
        if future_earn:
            fields["Earnings Date"] = future_earn[0].get("date") or ""
        else:
            missing.append("Earnings Date")

        # Part 5: match upcoming quarter to prior-year same quarter by date proximity
        if future_earn and past_earn:
            nxt    = future_earn[0]
            py_nxt = self._match_prior_year_earnings(nxt, past_earn)

            if py_nxt is not None:
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
        else:
            missing += ["Rev Growth Next Quarter", "EPS Growth This Quarter"]

        # ── Forward P/E — Priority 2 (fallback): next-quarter EPS × 4 ────────
        # Priority order: (1) FY1 annual consensus EPS [section 10 upgrade],
        #                 (2) next-quarter EPS × 4 approximation [stored now],
        #                 (3) missing.
        _fpe_stored = False
        if future_earn and _current_price is not None and _current_price > 0:
            _nxt_earn_row = future_earn[0]
            _nxt_eps_est  = _nxt_earn_row.get("epsEstimated")
            try:
                _nxt_eps_f = float(_nxt_eps_est) if _nxt_eps_est is not None else None
            except (ValueError, TypeError):
                _nxt_eps_f = None
            if _nxt_eps_f is not None and _nxt_eps_f > 0:
                _fwd_eps_ann = round(_nxt_eps_f * 4, 4)
                _fwd_pe_val  = round(_current_price / _fwd_eps_ann, 2)
                _fpe_warn: list[str] = []
                if _nxt_eps_f > 20.0:
                    _fpe_warn.append("HIGH_EPS_ESTIMATE")
                if _current_price > 700.0:
                    _fpe_warn.append("HIGH_PRICE_USED")
                fields["forward_pe_price_used"]      = _current_price
                fields["forward_pe_raw_eps_estimate"] = round(_nxt_eps_f, 4)
                fields["forward_pe_raw_period"]       = _nxt_earn_row.get("date") or ""
                if _fpe_warn:
                    fields["forward_pe_warning_codes"] = _fpe_warn
                if 1.0 <= _fwd_pe_val <= 500.0:
                    fields["Forward P/E"]               = _fwd_pe_val
                    fields["forward_eps_estimate"]      = _fwd_eps_ann
                    fields["forward_pe_source"]         = "quarterly_eps_annualized"
                    fields["forward_pe_is_approximate"] = True
                    _fpe_stored = True

        # ── 8. Quality from existing data (0 new calls) ──────────────────────
        _dq, _not_meaningful_active = self._compute_derived_quality(
            is_rows, cf_rows, rtm, kmtm,
            sector=_sector, industry=_industry,
        )
        for _k, _v in _dq.items():
            if _v is not None:
                fields[_k] = _v

        # Augment not_meaningful_active with PE Ratio when it was suppressed
        # for being negative/zero — this blocks carry-forward of a previously
        # stored negative P/E from an old snapshot.
        if fields.get("_pe_not_meaningful_reason"):
            _not_meaningful_active.add("PE Ratio")

        # ── 9. Balance sheet (new call) ──────────────────────────────────────
        bs_quality, _bs_row, _bs_outcome = await self._fetch_bs_quality(sym)
        calls += 1
        for _k, _v in bs_quality.items():
            if _v is not None:
                fields[_k] = _v

        # Issue 5: Current Ratio BS fallback — skip for financial companies.
        # Banks, insurers and REITs are deposit-funded; the current-ratio
        # balance-sheet formula is not economically meaningful for them.
        # The direct FMP TTM ratio from ratios-ttm is also questionable for
        # financial firms but comes from FMP directly (not derived here) and
        # has different consumer expectations, so it is left unchanged.
        if "Current Ratio" not in fields and _bs_row:
            if _is_current_ratio_not_meaningful(_sector, _industry):
                fields["_current_ratio_not_meaningful_reason"] = (
                    "deposit_funded_or_insurer_balance_sheet_fallback_suppressed"
                )
            else:
                _ca = _bs_row.get("totalCurrentAssets")
                _cl = _bs_row.get("totalCurrentLiabilities")
                if _ca is not None and _cl is not None:
                    try:
                        _cl_f = float(_cl)
                        if _cl_f != 0.0:
                            fields["Current Ratio"] = round(float(_ca) / _cl_f, 4)
                            fields["_current_ratio_source"] = "balance_sheet_fallback"
                    except (ValueError, TypeError, ZeroDivisionError):
                        pass

        # ── Post-BS: Net Debt / EBITDA (Part 3) ─────────────────────────────
        # Computed from independently validated inputs (not FMP netDebtToEBITDATTM).
        # Formula: (total_debt - cash_and_short_term_investments) / strict_ttm_ebitda_is
        # N/M for financial companies (deposit-funded / insurers).
        # N/M when TTM EBITDA ≤ 0 (pre-profitability companies).
        # Negative result is valid (net-cash company — more cash than gross debt).
        if _bs_row:
            _bs_total_debt_raw = _bs_row.get("totalDebt")
            _bs_cash_raw       = _bs_row.get("cashAndShortTermInvestments")
            if _bs_total_debt_raw is not None:
                try:
                    _val_total_debt = float(_bs_total_debt_raw)
                    fields["_valuation_total_debt"] = int(_val_total_debt)
                except (ValueError, TypeError):
                    _val_total_debt = None
            else:
                _val_total_debt = None

            if _bs_cash_raw is not None:
                try:
                    _val_cash_stinv = float(_bs_cash_raw)
                    fields["_valuation_cash_and_short_term_investments"] = int(_val_cash_stinv)
                except (ValueError, TypeError):
                    _val_cash_stinv = None
            else:
                _val_cash_stinv = None

            # Compute Net Debt / EBITDA
            if _is_leverage_metrics_not_meaningful(_sector, _industry):
                fields["_net_debt_ebitda_not_meaningful_reason"] = (
                    "not_meaningful_for_financial_company"
                )
                _not_meaningful_active.add("Net Debt / EBITDA")
            elif (
                _val_total_debt is not None
                and _val_cash_stinv is not None
                and _val_ttm_ebitda is not None
            ):
                try:
                    _ebitda_f = float(_val_ttm_ebitda)
                    if _ebitda_f > 0:
                        _net_debt_f = _val_total_debt - _val_cash_stinv
                        fields["Net Debt / EBITDA"] = round(
                            _net_debt_f / _ebitda_f, 4
                        )
                        fields["_net_debt_ebitda_method"] = (
                            "total_debt_minus_cash_stinv_over_strict_ttm_ebitda"
                        )
                    else:
                        fields["_net_debt_ebitda_not_meaningful_reason"] = (
                            "nonpositive_ebitda"
                        )
                        _not_meaningful_active.add("Net Debt / EBITDA")
                except (ValueError, TypeError, ZeroDivisionError):
                    pass
            else:
                fields["_net_debt_ebitda_not_meaningful_reason"] = (
                    "missing_bs_or_ebitda_data"
                )

        # ── 10. Analyst estimates annual + Forward P/E upgrade ──────────────
        _mkt_cap_int: int | None = (
            int(float(mkt_cap)) if mkt_cap is not None and float(mkt_cap) > 0 else None
        )
        _ev_float: float | None = _val_ev  # reuse EV already captured from kmtm

        est_quality, _fy1_row, _est_outcome = await self._fetch_analyst_estimates_quality(
            sym,
            _mkt_cap_int,
            _ev_float,
            _completed_fy_date,
            _completed_fy_revenue,
        )
        calls += 1
        for _k, _v in est_quality.items():
            if _v is not None:
                fields[_k] = _v

        # Part 8 — Forward P/E upgrade: FY1 annual consensus EPS (Priority 1).
        # When FY1 EPS is available, ALL provenance fields must be updated
        # consistently.  forward_pe_raw_eps_estimate is set to FY1 annual EPS
        # (not the quarterly EPS stored in section 7) so provenence is coherent.
        if _fy1_row is not None and _current_price is not None and _current_price > 0:
            _fy1_eps_raw = _fy1_row.get("epsAvg")
            _fy1_n_eps   = _fy1_row.get("numAnalystsEps")
            if _fy1_eps_raw is not None:
                try:
                    _fy1_eps_f = float(_fy1_eps_raw)
                    if _fy1_eps_f > 0:
                        _fwd_pe_fy1 = round(_current_price / _fy1_eps_f, 2)
                        if 1.0 <= _fwd_pe_fy1 <= 500.0:
                            # Update ALL provenance fields for FY1 source
                            fields["Forward P/E"]               = _fwd_pe_fy1
                            fields["forward_eps_estimate"]      = round(_fy1_eps_f, 4)
                            fields["forward_pe_source"]         = "fy1_annual_consensus_eps"
                            fields["forward_pe_is_approximate"] = False
                            fields["forward_pe_raw_eps_estimate"] = round(_fy1_eps_f, 4)
                            fields["forward_pe_raw_period"]     = _fy1_row.get("date", "")
                            fields["forward_pe_price_used"]     = _current_price
                            # FY1 analyst count for EPS
                            if _fy1_n_eps is not None:
                                try:
                                    fields["_forward_pe_fy1_n_eps_analysts"] = int(_fy1_n_eps)
                                except (ValueError, TypeError):
                                    pass
                            # Clear quarterly-era approximation flags
                            fields.pop("forward_pe_warning_codes", None)
                            _fpe_stored = True
                except (ValueError, TypeError):
                    pass

        # Deferred missing check (after upgrade attempt)
        if not _fpe_stored:
            missing.append("Forward P/E")

        # ── 11. Financial scores ─────────────────────────────────────────────
        # _sector/_industry already extracted early (after _profile_meta build)
        scores_quality, _scores_outcome = await self._fetch_scores_quality(
            sym, _sector, _industry
        )
        calls += 1
        for _k, _v in scores_quality.items():
            if _v is not None:
                fields[_k] = _v

        # Altman Z not_meaningful blocks carry of old score
        if _is_altman_z_not_meaningful(_sector, _industry):
            _not_meaningful_active.add("Altman Z-Score")
            _not_meaningful_active.add("Altman Z-Risk")

        # ── 12. Cash Runway (uses Cash from call 9 + TTM FCF from call 4) ────
        _cash_val    = fields.get("Cash")
        _ttm_fcf_val = fields.get("Free Cash Flow")
        runway = self._compute_cash_runway(_cash_val, _ttm_fcf_val, _sector, _industry)
        for _k, _v in runway.items():
            fields[_k] = _v

        # ── Store raw valuation inputs (Part 2) ──────────────────────────────
        # These slow-moving IS + BS inputs let the live overlay in the GET
        # response recompute price-sensitive multiples without a new FMP call.
        # Stored with "_valuation_" prefix so carry-forward logic skips them
        # (they come from the same IS/BS calls that produced the other fields).
        if _val_ttm_ni is not None:
            fields["_valuation_ttm_net_income"]   = int(_val_ttm_ni)
        if _val_ttm_rev is not None:
            fields["_valuation_ttm_revenue"]      = int(_val_ttm_rev)
        if _val_ttm_ebitda is not None:
            fields["_valuation_ttm_ebitda"]       = int(_val_ttm_ebitda)
        if _val_ttm_ebit is not None:
            fields["_valuation_ttm_ebit"]         = int(_val_ttm_ebit)
        if _val_ttm_ie is not None:
            fields["_valuation_ttm_interest_expense"] = int(_val_ttm_ie)
        if _val_ev is not None:
            fields["_valuation_ev_at_refresh"]    = int(_val_ev)
        if _val_stmt_ccy:
            fields["_valuation_stmt_currency"]    = _val_stmt_ccy
        # FCF from CF statement
        _val_ttm_fcf = fields.get("Free Cash Flow")
        if _val_ttm_fcf is not None:
            fields["_valuation_ttm_fcf"]          = int(_val_ttm_fcf)
        # Implied shares (from market cap resolver — set by refresh_symbols post-step)
        # stored as _market_cap_implied_shares (already present if resolver ran)

        # ── FY1 forward inputs (stored for live overlay) ──────────────────
        _fy1_rev_f    = fields.get("_forward_fy1_rev_raw")
        _fy1_ebitda_f = fields.get("_forward_fy1_ebitda_raw")
        _fy1_eps_f    = fields.get("_forward_fy1_eps_raw")
        # Also capture FY1 consensus stored by _fetch_analyst_estimates_quality
        if _fy1_row is not None:
            try:
                _fyr = _fy1_row.get("revenueAvg")
                if _fyr is not None:
                    fields["_valuation_fy1_revenue"] = int(float(_fyr))
            except (ValueError, TypeError):
                pass
            try:
                _fye = _fy1_row.get("ebitdaAvg")
                if _fye is not None:
                    fields["_valuation_fy1_ebitda"] = int(float(_fye))
            except (ValueError, TypeError):
                pass
            try:
                _fyeps = _fy1_row.get("epsAvg")
                if _fyeps is not None:
                    fields["_valuation_fy1_eps"] = round(float(_fyeps), 4)
            except (ValueError, TypeError):
                pass

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
            "fields":                fields,
            "missing_fields":        list(set(missing)),
            "fmp_call_count":        calls,
            "_fy1_data":             _fy1_row,
            "_not_meaningful_active": _not_meaningful_active,
            "_bs_outcome":           _bs_outcome,
            "_est_outcome":          _est_outcome,
            "_scores_outcome":       _scores_outcome,
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

        Part 7 — Source-aware carry-forward:
          Old snapshot values are carried only when the responsible endpoint
          returned a transient failure.  Fields in _not_meaningful_active are
          never carried (they reflect the current semantic state of the company).

        Part 10 — Retention pruning:
          prune_old_observations() is called once per batch after all symbols
          are processed, not per-symbol.
        """
        from data.watchlist_fundamentals_store import (
            get_snapshots_bulk, upsert_snapshot,
        )
        from data.watchlist_estimate_history_store import (
            ensure_table    as _ensure_hist,
            upsert_estimate_observation as _upsert_obs,
            prune_old_observations      as _prune_obs,
        )
        from services.watchlist_quote_cache import is_fmp_symbol_eligible

        # Ensure estimate history table exists (safe no-op if already created)
        _ensure_hist()

        eligible  = [s for s in symbols if is_fmp_symbol_eligible(s)]
        snapshots = get_snapshots_bulk(eligible)

        started_at = datetime.now(timezone.utc)
        refreshed: list[str]             = []
        skipped:   list[str]             = []
        failed:    list[str]             = []
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

                # Extract per-endpoint outcomes + not_meaningful set
                _not_meaningful = result.pop("_not_meaningful_active", set())
                _bs_outcome     = result.pop("_bs_outcome",     "unknown")
                _est_outcome    = result.pop("_est_outcome",    "unknown")
                _scores_outcome = result.pop("_scores_outcome", "unknown")

                # ── No-null-overwrite for profile metadata ────────────────────
                _existing_snap = snapshots.get(sym.upper())
                if _existing_snap and _existing_snap.get("fields"):
                    _old_profile = (_existing_snap["fields"].get("profile") or {})
                    if _old_profile:
                        _new_profile  = result["fields"].get("profile") or {}
                        _merged_profile: dict[str, Any] = {**_old_profile}
                        for _k, _v in _new_profile.items():
                            if _v is not None and _v != "":
                                _merged_profile[_k] = _v
                        result["fields"]["profile"] = _merged_profile

                # ── Part 7: Source-aware quality field carry-forward ──────────
                # Each endpoint group is only carried when that endpoint itself
                # suffered a transient failure.  Semantically-invalid fields
                # (in _not_meaningful) are never carried regardless of outcome.
                if _existing_snap and _existing_snap.get("fields"):
                    _old_fields = _existing_snap["fields"]

                    def _carry_group(field_set: frozenset[str]) -> None:
                        for _qk in field_set:
                            if (
                                _qk not in _not_meaningful
                                and result["fields"].get(_qk) is None
                                and _old_fields.get(_qk) is not None
                            ):
                                result["fields"][_qk] = _old_fields[_qk]

                    # Issue 2: only carry on genuine transient failures.
                    # not_entitled (402/429) and success_no_data (200+empty)
                    # are authoritative: the server responded and data is absent,
                    # so retaining stale values would be misleading.  Record
                    # metadata flags so callers can surface data-quality signals.
                    if _bs_outcome == "transient_failure":
                        _carry_group(_BS_CARRY_FIELDS)
                    elif _bs_outcome == "not_entitled":
                        result["fields"]["_bs_not_entitled"] = True
                    elif _bs_outcome == "success_no_data":
                        result["fields"]["_bs_no_data"] = True

                    if _est_outcome == "transient_failure":
                        _carry_group(_EST_CARRY_FIELDS)
                        # Part 7 mutual exclusivity: do not co-carry revision_pct
                        # alongside a history_building reason from a fresh cycle.
                        if result["fields"].get("_rev_revision_reason") == "history_building":
                            result["fields"].pop("Revenue Estimate Revision 90D", None)
                        if result["fields"].get("_eps_revision_reason") == "history_building":
                            result["fields"].pop("EPS Estimate Revision 90D", None)
                    elif _est_outcome == "not_entitled":
                        result["fields"]["_est_not_entitled"] = True
                    elif _est_outcome == "success_no_data":
                        result["fields"]["_est_no_data"] = True

                    if _scores_outcome == "transient_failure":
                        _carry_group(_SCORES_CARRY_FIELDS)
                    elif _scores_outcome == "not_entitled":
                        result["fields"]["_scores_not_entitled"] = True
                    elif _scores_outcome == "success_no_data":
                        result["fields"]["_scores_no_data"] = True

                    # Issue 1: Recompute Cash Runway after carry-forward.
                    # normalize_symbol computed runway using the Cash value it
                    # fetched live.  If the BS endpoint failed (transient) and
                    # we just carried a prior Cash value, the runway that was
                    # stored in step 12 used the live (possibly stale/absent)
                    # Cash — not the carried value.  We always recompute here
                    # using whatever effective Cash and FCF are now in fields,
                    # so the three runway fields are always internally coherent.
                    _eff_cash    = result["fields"].get("Cash")
                    _eff_ttm_fcf = result["fields"].get("Free Cash Flow")
                    _eff_profile = result["fields"].get("profile") or {}
                    _eff_sector  = str(_eff_profile.get("sector",   "") or "")
                    _eff_industry= str(_eff_profile.get("industry", "") or "")
                    # Clear stale runway fields before recomputing
                    for _rk in ("Cash Runway Months", "Cash Runway Status",
                                "_cash_runway_not_meaningful_reason"):
                        result["fields"].pop(_rk, None)
                    _runway = FmpFundamentalsRefresher._compute_cash_runway(
                        _eff_cash, _eff_ttm_fcf, _eff_sector, _eff_industry
                    )
                    for _rk, _rv in _runway.items():
                        result["fields"][_rk] = _rv

                # ── Prevent empty payload from erasing prior good snapshot ────
                _new_f = result.get("fields") or {}
                _substantive = {
                    k: v for k, v in _new_f.items()
                    if not k.startswith("_") and k not in (
                        "missing_fields", "profile",
                    )
                }
                if not _substantive:
                    if _existing_snap and _existing_snap.get("fields"):
                        log.warning("[FMP_FUND] %s: empty payload — preserving prior snapshot", sym)
                        empty_payload_preserved.append(sym)
                        continue
                    else:
                        empty_payload_no_prior.append(sym)
                        continue

                outcome = upsert_snapshot(
                    symbol=sym,
                    watchlist_id=watchlist_id,
                    fields=result["fields"],
                    missing_fields=result["missing_fields"],
                    fmp_call_count=result["fmp_call_count"],
                )
                if outcome == "success":
                    refreshed.append(sym)

                    # Persist FY1 estimate observation for revision tracking
                    _fy1_d = result.get("_fy1_data")
                    if _fy1_d is not None:
                        _fy1_date_str = _fy1_d.get("date", "")
                        _rev_val      = _fy1_d.get("revenueAvg")
                        _eps_val      = _fy1_d.get("epsAvg")
                        _ebitda_val   = _fy1_d.get("ebitdaAvg")
                        _n_rev        = _fy1_d.get("numAnalystsRevenue")
                        _n_eps        = _fy1_d.get("numAnalystsEps")
                        if _fy1_date_str:
                            if _rev_val is not None:
                                _upsert_obs(
                                    symbol=sym,
                                    metric="revenue_annual",
                                    period_type="annual",
                                    fiscal_period=_fy1_date_str,
                                    consensus_value=float(_rev_val),
                                    num_analysts=(int(_n_rev) if _n_rev is not None else None),
                                    source="fmp_analyst_estimates",
                                    fmp_date=_fy1_date_str,
                                )
                            if _eps_val is not None:
                                _upsert_obs(
                                    symbol=sym,
                                    metric="eps_annual",
                                    period_type="annual",
                                    fiscal_period=_fy1_date_str,
                                    consensus_value=float(_eps_val),
                                    num_analysts=(int(_n_eps) if _n_eps is not None else None),
                                    source="fmp_analyst_estimates",
                                    fmp_date=_fy1_date_str,
                                )
                            if _ebitda_val is not None:
                                _upsert_obs(
                                    symbol=sym,
                                    metric="ebitda_annual",
                                    period_type="annual",
                                    fiscal_period=_fy1_date_str,
                                    consensus_value=float(_ebitda_val),
                                    num_analysts=None,
                                    source="fmp_analyst_estimates",
                                    fmp_date=_fy1_date_str,
                                )
                else:
                    failed.append(sym)

            except Exception as exc:
                log.error("[FMP_FUND] refresh_symbols(%s) error: %s", sym, exc)
                failed.append(sym)

        # ── Part 10: Retention pruning — once per batch ──────────────────────
        if refreshed:
            try:
                pruned = _prune_obs()
                if pruned:
                    log.info("[FMP_FUND] pruned %d stale estimate history rows", pruned)
            except Exception as _pe:
                log.debug("[FMP_FUND] prune_old_observations error: %s", _pe)

        elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
        return {
            "refreshed":              refreshed,
            "skipped":                skipped,
            "failed":                 failed,
            "empty_payload_preserved": empty_payload_preserved,
            "empty_payload_no_prior":  empty_payload_no_prior,
            "elapsed_seconds":         round(elapsed, 1),
            "fmp_calls_per_symbol":    10,
        }


# ── Read-path helpers (imported by watchlist_router GET path) ─────────────────

def merge_fmp_into_csv_row(csv_row: dict, fmp_fields: dict) -> dict:
    """
    Merge FMP snapshot fields into a single CSV row dict.

    Merge precedence: FMP non-null value > existing CSV value.
    Null FMP values never overwrite existing CSV values (no-null-overwrite rule).
    Returns a new dict; does not mutate the caller's csv_row.
    """
    merged = dict(csv_row)
    for k, v in fmp_fields.items():
        if v is not None:
            merged[k] = v
    return merged


def compute_live_valuation_overlay(
    fund_fields: dict,
    live_market_cap: float | None,
    live_price: float | None,
) -> dict:
    """
    Pure function — no I/O, no FMP calls.

    Re-derives all price-sensitive valuation multiples using the live market
    cap / price and the slow-moving income-statement inputs stored during
    the last FMP refresh cycle (_valuation_* fields).

    Returns a dict of overlay fields to MERGE into fundamentals["fields"].
    Only non-None results are included so callers can do a clean dict-update.

    Part 6 — Live Valuation Overlay contract:
      • Market Cap      = live_market_cap (pass-through)
      • EV              = Market Cap + _valuation_total_debt
                         − _valuation_cash_and_short_term_investments
      • PE Ratio        = live_price / (TTM Net Income / implied_shares)  [+ gate]
      • PS Ratio        = live_market_cap / TTM Revenue  [+ gate]
      • EV/EBITDA       = EV / TTM EBITDA  [+ gate]
      • P/FCF           = live_market_cap / TTM FCF  [+ gate]
      • FCF Yield       = TTM FCF / live_market_cap * 100  [no 100% cap]
      • Forward P/E     = live_price / FY1 EPS  [+ gate]
      • Forward P/S     = live_market_cap / FY1 Revenue  [+ gate]
      • Forward EV/Sales  = EV / FY1 Revenue  [EV>0 + result>0 gate]
      • Forward EV/EBITDA = EV / FY1 EBITDA   [EV>0 + result>0 gate]
    """
    overlay: dict = {}

    def _safe(v) -> float | None:
        if v is None:
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    mc   = _safe(live_market_cap)
    px   = _safe(live_price)

    ttm_ni    = _safe(fund_fields.get("_valuation_ttm_net_income"))
    ttm_rev   = _safe(fund_fields.get("_valuation_ttm_revenue"))
    ttm_ebitda= _safe(fund_fields.get("_valuation_ttm_ebitda"))
    ttm_fcf   = _safe(fund_fields.get("_valuation_ttm_fcf"))
    total_debt= _safe(fund_fields.get("_valuation_total_debt"))
    cash_stinv= _safe(fund_fields.get("_valuation_cash_and_short_term_investments"))
    shares    = _safe(fund_fields.get("_market_cap_implied_shares"))
    fy1_rev   = _safe(fund_fields.get("_valuation_fy1_revenue"))
    fy1_ebitda= _safe(fund_fields.get("_valuation_fy1_ebitda"))
    fy1_eps   = _safe(fund_fields.get("_valuation_fy1_eps"))

    # Market Cap
    if mc is not None and mc > 0:
        overlay["Market Cap"] = int(mc)

    # Enterprise Value = Market Cap + Total Debt − Cash & ST Investments
    ev: float | None = None
    if mc is not None and total_debt is not None and cash_stinv is not None:
        ev = mc + total_debt - cash_stinv
        overlay["_valuation_live_ev"] = int(ev)
        overlay["_enterprise_value_method"] = (
            "live_market_cap_plus_debt_minus_cash_stinv"
        )

    # PE Ratio (TTM) — live price / EPS (net income / shares)
    if px is not None and ttm_ni is not None and shares is not None and shares > 0:
        try:
            _ttm_eps = ttm_ni / shares
            if _ttm_eps > 0:
                _pe = round(px / _ttm_eps, 4)
                if _pe > 0:
                    overlay["PE Ratio"] = _pe
        except (ZeroDivisionError, TypeError):
            pass

    # PS Ratio — live market cap / TTM revenue
    if mc is not None and mc > 0 and ttm_rev is not None and ttm_rev > 0:
        try:
            _ps = round(mc / ttm_rev, 4)
            if _ps > 0:
                overlay["PS Ratio"] = _ps
        except (ZeroDivisionError, TypeError):
            pass

    # EV/EBITDA — EV / TTM EBITDA
    if ev is not None and ev > 0 and ttm_ebitda is not None and ttm_ebitda > 0:
        try:
            _ev_ebitda = round(ev / ttm_ebitda, 4)
            if _ev_ebitda > 0:
                overlay["EV/EBITDA"] = _ev_ebitda
        except (ZeroDivisionError, TypeError):
            pass

    # P/FCF — live market cap / TTM FCF
    if mc is not None and mc > 0 and ttm_fcf is not None and ttm_fcf > 0:
        try:
            _pfcf = round(mc / ttm_fcf, 4)
            if _pfcf > 0:
                overlay["P/FCF"] = _pfcf
        except (ZeroDivisionError, TypeError):
            pass

    # FCF Yield — TTM FCF / live market cap * 100 (no 100% cap per spec)
    # Negative FCF Yield may remain visible as a negative cash-burn yield.
    # When |FCF Yield| > 100%, flag as outlier but retain the correct value.
    if mc is not None and mc > 0 and ttm_fcf is not None:
        try:
            _fcf_yield_val = round((ttm_fcf / mc) * 100, 4)
            overlay["FCF Yield"] = _fcf_yield_val
            if abs(_fcf_yield_val) > 100.0:
                overlay["_fcf_yield_outlier"] = True
                overlay["_fcf_yield_outlier_reason"] = "absolute_fcf_exceeds_market_cap"
        except (ZeroDivisionError, TypeError):
            pass

    # Forward P/E — live price / FY1 EPS
    if px is not None and px > 0 and fy1_eps is not None and fy1_eps > 0:
        try:
            _fwd_pe = round(px / fy1_eps, 2)
            if 1.0 <= _fwd_pe <= 500.0:
                overlay["Forward P/E"] = _fwd_pe
                overlay["_valuation_overlay_forward_pe_source"] = "live_price_over_fy1_eps"
        except (ZeroDivisionError, TypeError):
            pass

    # Forward P/S — live market cap / FY1 revenue
    if mc is not None and mc > 0 and fy1_rev is not None and fy1_rev > 0:
        try:
            _fwd_ps = round(mc / fy1_rev, 4)
            if _fwd_ps > 0:
                overlay["Forward P/S"] = _fwd_ps
        except (ZeroDivisionError, TypeError):
            pass

    # Forward EV/Sales — EV / FY1 revenue (EV > 0 AND result > 0 gate)
    if ev is not None and ev > 0 and fy1_rev is not None and fy1_rev > 0:
        try:
            _fwd_ev_s = round(ev / fy1_rev, 4)
            if _fwd_ev_s > 0:
                overlay["Forward EV/Sales"] = _fwd_ev_s
        except (ZeroDivisionError, TypeError):
            pass

    # Forward EV/EBITDA — EV / FY1 EBITDA (EV > 0 AND result > 0 gate)
    if ev is not None and ev > 0 and fy1_ebitda is not None and fy1_ebitda > 0:
        try:
            _fwd_ev_ebitda = round(ev / fy1_ebitda, 4)
            if _fwd_ev_ebitda > 0:
                overlay["Forward EV/EBITDA"] = _fwd_ev_ebitda
        except (ZeroDivisionError, TypeError):
            pass

    # Provenance tag
    if overlay:
        overlay["_valuation_overlay_live_price"] = px
        overlay["_valuation_overlay_live_mc"]    = mc

    return overlay


def apply_fmp_overlays(csv_rows: list, snaps: dict) -> list:
    """
    Merge FMP fundamentals snapshots into every row of a csv_data list.

    csv_rows : list of raw CSV row dicts (each has a Symbol / Ticker key).
    snaps    : {SYMBOL_UPPER: {"fields": {...}, "refreshed_at": ..., "missing_fields": [...]}}
               as returned by watchlist_fundamentals_store.get_snapshots_bulk().

    Returns a new list.  Rows for symbols not in snaps are passed through
    unchanged.  Uses merge_fmp_into_csv_row so FMP non-null > CSV value.
    """
    result: list = []
    for row in csv_rows:
        sym = (
            row.get("Symbol") or row.get("symbol") or row.get("Ticker") or ""
        ).strip().upper()
        snap = snaps.get(sym) if sym else None
        if snap:
            fmp_fields = snap.get("fields") or {}
            if fmp_fields:
                row = merge_fmp_into_csv_row(row, fmp_fields)
        result.append(row)
    return result
