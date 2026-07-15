"""
Watchlist Fundamental Screener — weekly FMP refresh service.

Architecture
───────────────────────────────────────────────────────────────────────────────
• Runs as a background task; never called on page render.
• One symbol at a time, paced via asyncio.sleep between calls.
• 7 FMP stable/ endpoints per symbol (all on Starter plan).
• Normalized snapshot stored in watchlist_fundamentals_cache (Neon).
• Merge precedence (in watchlist GET): FMP non-null > CSV value > blank.
• No-null overwrite: null FMP result never erases an existing CSV value.

Endpoint mapping (per symbol, 7 calls)
───────────────────────────────────────────────────────────────────────────────
1  stable/profile                         → Market Cap
2  stable/income-statement  period=quarter → Revenue (latest Q), Op. Income, EBIT
3  stable/income-statement-growth period=quarter → Rev Grwth (Q) [seq QoQ], EPS Growth [seq QoQ]
4  stable/cash-flow-statement period=quarter → Free CF, FCF Margin denominator
5  stable/ratios-ttm                      → P/E, P/S, Gross Mgn, D/E
6  stable/key-metrics-ttm                 → EV/EBITDA (TTM), ND/EBITDA (TTM)
7  stable/earnings                        → Earn. Date, Rev Grwth NQ estimate, EPS Grwth TQ estimate

CSV fallback (FMP Starter cannot supply these):
  Shares Insiders  — no insider ownership endpoint available (404/institutional-only)
  Revenue Growth Est. / Rev Growth This Year / Rev Growth Next Year
  EPS Growth Est.  / EPS Growth This Year  / EPS Growth Next Year
  EPS Growth Next Quarter  (only 1 future quarter returned by stable/earnings)
  Revenue Growth (YoY)  — derived below from 4Q sums if income stmt has 8Q
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

log = logging.getLogger(__name__)

_FMP_BASE = "https://financialmodelingprep.com/stable"
_CALL_DELAY = 0.45      # seconds between FMP calls ≈ 133 req/min (< 200 req/min Starter)
_TIMEOUT = 18           # seconds per HTTP request

# ── Field mapping audit table ────────────────────────────────────────────────
# Produced from live payload inspection of NVDA, MSFT, AAPL, AAOI, ASTS,
# RKLB, PLTR, IREN, NNE, MU against all FMP stable/ endpoint families.
#
# CSV column             FMP endpoint              FMP key                  Direct/Derived  Confidence  CSV Fallback?
# ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────
# Market Cap             profile                   marketCap                direct          HIGH        NO
# Revenue                income-statement Q        revenue (latest Q)       direct          HIGH        NO
# Revenue Growth (Q)     income-statement-growth Q growthRevenue            direct(seq QoQ) MEDIUM      NO
# Revenue Growth (YoY)   income-statement Q        derive: TTM/prior-TTM   derived         MEDIUM      NO
# Gross Margin           ratios-ttm                grossProfitMarginTTM     direct(TTM)     HIGH        NO
# FCF Margin             CF+IS Q                   freeCashFlow/TTM_rev     derived         MEDIUM      NO
# Free Cash Flow         cash-flow-statement Q     freeCashFlow (latest Q)  direct          HIGH        NO
# Operating Income       income-statement Q        operatingIncome          direct          HIGH        NO
# EBIT                   income-statement Q        ebit                     direct          HIGH        NO
# PE Ratio               ratios-ttm                priceToEarningsRatioTTM  direct(TTM)     HIGH        NO
# PS Ratio               ratios-ttm                priceToSalesRatioTTM     direct(TTM)     HIGH        NO
# EV/EBITDA              key-metrics-ttm           evToEBITDATTM            direct(TTM)     HIGH        NO
# EPS Growth             income-statement-growth Q growthEPSDiluted         direct(seq QoQ) MEDIUM      NO
# Debt / Equity          ratios-ttm                debtToEquityRatioTTM     direct(TTM)     HIGH        NO
# Net Debt / EBITDA      key-metrics-ttm           netDebtToEBITDATTM       direct(TTM)     HIGH        NO
# Shares Insiders        (none)                    —                        —               —           YES (insiders 404; acquisition-of-beneficial-ownership = institutional only)
# Earnings Date          earnings                  date (first future Q)    direct          HIGH        NO
# Revenue Growth Est.    (premium: 402)            —                        —               —           YES
# Rev Growth Next Quarter earnings                 future[0]_est/py_actual  derived         MEDIUM      YES (partial; ~3-5pp off vs StockAnalysis)
# Rev Growth Next Year   (premium: 402)            —                        —               —           YES
# EPS Growth Est.        (premium: 402)            —                        —               —           YES
# EPS Growth This Quarter earnings                 future[0]_eps_est/py_act derived         MEDIUM      YES (blank for loss-making cos)
# EPS Growth Next Quarter (only 1 future Q avail)  —                       —               —           YES
# EPS Growth This Year   (premium: 402)            —                        —               —           YES
# EPS Growth Next Year   (premium: 402)            —                        —               —           YES
#
# Estimate-field formula audit vs CSV baseline (10 tickers):
#   Rev Growth Next Quarter (FMP: future[0] est vs prior-year actual):
#     AAOI:  CSV=88.72%   FMP=85.02%  (Δ≈3.7pp)  ← close match
#     RKLB:  CSV=49.46%   FMP=60.29%  (Δ≈10.8pp) ← direction correct
#     IREN:  CSV=100.68%  FMP=-13.15% (Δ large)   ← calendar mismatch
#     MU:    CSV=275.12%  FMP=345.47% (Δ≈70pp)    ← direction correct
#   Conclusion: formula direction correct; magnitude diverges for non-calendar fiscal years.
#   CSV fallback preserved; FMP estimate overlays only when no CSV value present.
# ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────


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

    # ── Core normalizer ──────────────────────────────────────────────────────

    async def normalize_symbol(self, symbol: str) -> dict:
        """
        Fetch 7 FMP endpoints and return a dict of normalized CSV-keyed fields.
        Returns {"fields": {...}, "missing_fields": [...], "fmp_call_count": N}.
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
        # Store current price for Forward P/E derivation in section 7
        _current_price: float | None = None
        try:
            _p = profile.get("price")
            if _p is not None:
                _current_price = float(_p)
        except (ValueError, TypeError):
            pass

        # ── 2. Income Statement (quarterly, 8Q) ──────────────────────────────
        raw = await self._get("income-statement", {"symbol": sym, "period": "quarter", "limit": 8})
        calls += 1
        is_rows: list[dict] = raw if isinstance(raw, list) else []

        if is_rows:
            # Revenue → TTM (sum of 4 most recent quarters) — matches StockAnalysis CSV
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

            # Revenue Growth (Q): latest quarter vs same quarter prior year (YoY).
            # Requires 5+ quarters; is_rows[0] = latest Q, is_rows[4] = same Q prior year.
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
        # growthRevenue from this endpoint is sequential (Q/Q-1), not YoY.
        # Revenue Growth (Q) is now derived above from income-statement rows directly.
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
            # Free Cash Flow → TTM sum — matches StockAnalysis CSV
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
        rtm = (raw[0] if isinstance(raw, list) and raw else (raw if isinstance(raw, dict) else {}))
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
        kmtm = (raw[0] if isinstance(raw, list) and raw else (raw if isinstance(raw, dict) else {}))
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
        # If no future_earn rows exist (micro-cap with no analyst coverage,
        # or FMP hasn't posted the next estimate yet), mark as missing so
        # the no-null-overwrite rule preserves the CSV filing date.
        # Writing a past fiscal quarter-end date would be WORSE than CSV.
        if future_earn:
            fields["Earnings Date"] = future_earn[0].get("date") or ""
        else:
            missing.append("Earnings Date")

        # TQ estimate (first upcoming quarter vs same quarter prior year)
        # Maps to CSV "EPS Growth This Quarter" / "Rev Growth Next Quarter"
        # NOTE: FMP "earnings" returns the UPCOMING quarter as future_earn[0].
        # StockAnalysis labels this "This Quarter" (currently in progress).
        # CSV "Rev Growth Next Quarter" ≈ FMP future[0] vs prior-year actual (MEDIUM confidence).
        if future_earn and len(past_earn) >= 4:
            nxt    = future_earn[0]
            py_nxt = past_earn[-4]   # prior-year same-quarter slot

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

        # ── Forward P/E derivation ────────────────────────────────────────────
        # Source: current price (from profile) / next-quarter EPS estimate × 4
        # (annualise one forward quarter as NTM proxy).
        # Labelled approximate; stored only when both inputs are positive.
        _fpe_stored = False
        if future_earn and _current_price is not None and _current_price > 0:
            _nxt_eps_est = future_earn[0].get("epsEstimated")
            try:
                _nxt_eps_f = float(_nxt_eps_est) if _nxt_eps_est is not None else None
            except (ValueError, TypeError):
                _nxt_eps_f = None
            if _nxt_eps_f is not None and _nxt_eps_f > 0:
                _fwd_eps_ann = round(_nxt_eps_f * 4, 4)
                _fwd_pe_val  = round(_current_price / _fwd_eps_ann, 2)
                if 1.0 <= _fwd_pe_val <= 500.0:   # sanity bounds
                    fields["Forward P/E"]               = _fwd_pe_val
                    fields["forward_eps_estimate"]      = _fwd_eps_ann
                    fields["forward_pe_source"]         = "quarterly_eps_annualized"
                    fields["forward_pe_is_approximate"] = True
                    _fpe_stored = True
        if not _fpe_stored:
            missing.append("Forward P/E")

        # Fields that require analyst-estimates (premium, 402 on Starter) — always CSV fallback
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
        from services.watchlist_quote_cache import is_fmp_symbol_eligible

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
                # Check TTL
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
                outcome = upsert_snapshot(
                    symbol=sym,
                    watchlist_id=watchlist_id,
                    fields=result["fields"],
                    missing_fields=result["missing_fields"],
                    fmp_call_count=result["fmp_call_count"],
                )
                if outcome == "success":
                    refreshed.append(sym)
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
