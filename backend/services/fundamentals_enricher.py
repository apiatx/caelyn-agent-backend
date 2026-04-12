"""
Fundamentals Enricher — fetches market data from FMP + SEC EDGAR for manually-entered tickers.

Called when a watchlist is created from a ticker list (no CSV fundamentals).
Populates the same column names that CSV-uploaded watchlists have, so the
watchlist analysis (refresh / deep-dive) works identically for both paths.

FMP calls are per-ticker (concurrent); SEC EDGAR uses the CIK from FMP profile.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime
from typing import Dict, List, Optional

import httpx

FMP_BASE = "https://financialmodelingprep.com/stable"
EDGAR_BASE = "https://data.sec.gov"
EDGAR_USER_AGENT = "Caelyn-AI research@caelynai.com"

_FUNDAMENTAL_COLS = [
    "Stock Price",
    "Market Cap",
    "PE Ratio",
    "Revenue Growth (YoY)",
    "Gross Margin",
    "Debt / Equity",
]


def has_fundamental_data(csv_data: List[Dict]) -> bool:
    """
    Return True if the CSV rows already carry substantial fundamental data.

    A CSV file upload typically has many columns filled in; a manual ticker
    entry produces rows with only a 'Symbol' key and empty fundamentals.
    """
    if not csv_data:
        return False
    populated = sum(
        1
        for row in csv_data
        for col in _FUNDAMENTAL_COLS
        if row.get(col) and str(row.get(col, "")).strip()
    )
    return populated > 0


def _fmt_market_cap(value: Optional[float]) -> str:
    if not value:
        return ""
    if value >= 1e12:
        return f"${value / 1e12:.2f}T"
    if value >= 1e9:
        return f"${value / 1e9:.2f}B"
    if value >= 1e6:
        return f"${value / 1e6:.2f}M"
    return f"${value:,.0f}"


def _fmt_pct(ratio: Optional[float]) -> str:
    """Convert a ratio (0–1) to a percentage string."""
    if ratio is None:
        return ""
    return f"{round(ratio * 100, 1)}%"


def _fmt_pct_direct(value: Optional[float]) -> str:
    """Format a value that is already in percentage points."""
    if value is None:
        return ""
    return f"{round(value, 1)}%"


async def fetch_fundamentals(
    tickers: List[str],
    fmp_api_key: str,
) -> Dict[str, Dict]:
    """
    Fetch fundamentals for all tickers concurrently from FMP + SEC EDGAR.

    For each ticker we run three concurrent FMP calls (quote, profile, ratios-ttm),
    then a single EDGAR call using the CIK obtained from the profile call.

    Returns {TICKER: {column_name: value, ...}}.
    """
    results: Dict[str, Dict] = {t.upper(): {} for t in tickers}
    tickers_upper = [t.upper() for t in tickers]

    async with httpx.AsyncClient(timeout=15.0) as client:

        # ── Per-ticker FMP calls (quote + profile + ratios-ttm) ──────────────

        async def fetch_quote(ticker: str):
            try:
                resp = await client.get(
                    f"{FMP_BASE}/quote",
                    params={"symbol": ticker, "apikey": fmp_api_key},
                )
                if resp.status_code != 200:
                    return
                data = resp.json()
                item = data[0] if isinstance(data, list) and data else {}
                if not item:
                    return
                price = item.get("price")
                if price is not None:
                    results[ticker]["Stock Price"] = f"${float(price):.2f}"
                mc = item.get("marketCap")
                if mc:
                    results[ticker]["Market Cap"] = _fmt_market_cap(mc)
                if not results[ticker].get("_name"):
                    results[ticker]["_name"] = item.get("name", "")
            except Exception as exc:
                print(f"[ENRICHER] FMP quote {ticker}: {exc}")

        async def fetch_profile(ticker: str):
            try:
                resp = await client.get(
                    f"{FMP_BASE}/profile",
                    params={"symbol": ticker, "apikey": fmp_api_key},
                )
                if resp.status_code != 200:
                    return
                data = resp.json()
                p = data[0] if isinstance(data, list) and data else {}
                if not p:
                    return
                results[ticker]["Sector"] = p.get("sector", "")
                results[ticker]["Industry"] = p.get("industry", "")
                if p.get("beta") is not None:
                    results[ticker]["Beta"] = str(round(float(p["beta"]), 2))
                cik = p.get("cik", "")
                if cik:
                    results[ticker]["_cik"] = cik
                if not results[ticker].get("_name"):
                    results[ticker]["_name"] = p.get("companyName", "")
                if not results[ticker].get("Market Cap") and p.get("marketCap"):
                    results[ticker]["Market Cap"] = _fmt_market_cap(p["marketCap"])
            except Exception as exc:
                print(f"[ENRICHER] FMP profile {ticker}: {exc}")

        async def fetch_ratios(ticker: str):
            try:
                resp = await client.get(
                    f"{FMP_BASE}/ratios-ttm",
                    params={"symbol": ticker, "apikey": fmp_api_key},
                )
                if resp.status_code != 200:
                    return
                data = resp.json()
                r = data[0] if isinstance(data, list) and data else {}
                if not r:
                    return
                pe = r.get("priceEarningsRatioTTM")
                if pe and float(pe) > 0:
                    results[ticker]["PE Ratio"] = str(round(float(pe), 1))
                ps = r.get("priceToSalesRatioTTM")
                if ps and float(ps) > 0:
                    results[ticker]["PS Ratio"] = str(round(float(ps), 1))
                pb = r.get("priceToBookRatioTTM")
                if pb and float(pb) > 0:
                    results[ticker]["PB Ratio"] = str(round(float(pb), 1))
                de = r.get("debtToEquityRatioTTM")
                if de is not None:
                    results[ticker]["Debt / Equity"] = str(round(float(de), 2))
                gm = r.get("grossProfitMarginTTM")
                if gm is not None:
                    results[ticker]["Gross Margin"] = _fmt_pct(gm)
                roe = r.get("returnOnEquityTTM")
                if roe is not None:
                    results[ticker]["ROE"] = _fmt_pct(roe)
                # FCF Margin ≈ (FCF per share / Price) × PS ratio
                fcf_ps = r.get("freeCashFlowPerShareTTM")
                if fcf_ps is not None and ps:
                    price_str = results[ticker].get("Stock Price", "")
                    price = float(price_str.replace("$", "")) if price_str else 0.0
                    if price > 0:
                        fcf_margin = (float(fcf_ps) / price) * float(ps)
                        results[ticker]["FCF Margin"] = _fmt_pct(fcf_margin)
            except Exception as exc:
                print(f"[ENRICHER] FMP ratios-ttm {ticker}: {exc}")

        # Run all per-ticker FMP calls concurrently
        all_fmp_tasks = []
        for t in tickers_upper:
            all_fmp_tasks += [fetch_quote(t), fetch_profile(t), fetch_ratios(t)]
        await asyncio.gather(*all_fmp_tasks)

        # ── SEC EDGAR: revenue growth + EPS (uses CIK from profile) ─────────

        async def fetch_edgar(ticker: str):
            cik = results[ticker].get("_cik", "")
            if not cik:
                return
            cik_padded = cik.lstrip("0").zfill(10)
            if not cik_padded or cik_padded == "0000000000":
                return
            try:
                resp = await client.get(
                    f"{EDGAR_BASE}/api/xbrl/companyfacts/CIK{cik_padded}.json",
                    headers={"User-Agent": EDGAR_USER_AGENT},
                    timeout=12.0,
                )
                if resp.status_code != 200:
                    return
                facts = resp.json().get("facts", {}).get("us-gaap", {})

                def latest_annual(concept: str):
                    entries = facts.get(concept, {}).get("units", {})
                    usd = entries.get("USD", entries.get("shares", []))
                    annual = [
                        e for e in usd
                        if e.get("form") in ("10-K", "20-F") and e.get("val") is not None
                    ]
                    if not annual:
                        return None
                    annual.sort(key=lambda x: x.get("end", ""), reverse=True)
                    return annual[0].get("val")

                # Revenue growth YoY — pick the concept with the most recent 10-K data
                # (AAPL uses RevenueFromContractWithCustomerExcludingAssessedTax since 2018;
                #  older companies may use Revenues or SalesRevenueNet)
                _rev_candidates = []
                for _concept in [
                    "RevenueFromContractWithCustomerExcludingAssessedTax",
                    "Revenues",
                    "SalesRevenueNet",
                    "RevenueFromContractWithCustomerIncludingAssessedTax",
                ]:
                    _entries = [
                        e for e in facts.get(_concept, {}).get("units", {}).get("USD", [])
                        if e.get("form") in ("10-K", "20-F") and e.get("val")
                    ]
                    if _entries:
                        _rev_candidates.append(sorted(_entries, key=lambda x: x.get("end", ""), reverse=True))
                rev_annual = max(_rev_candidates, key=lambda x: x[0].get("end", "")) if _rev_candidates else []
                if len(rev_annual) >= 2:
                    curr_end = rev_annual[0].get("end", "")
                    prev_end = rev_annual[1].get("end", "")
                    try:
                        curr_dt = datetime.strptime(curr_end, "%Y-%m-%d")
                        prev_dt = datetime.strptime(prev_end, "%Y-%m-%d")
                        days_diff = abs((curr_dt - prev_dt).days)
                        # Only compute if exactly ~1 fiscal year apart (330–400 days)
                        if 330 <= days_diff <= 400:
                            curr_val = float(rev_annual[0].get("val", 0))
                            prev_val = float(rev_annual[1].get("val", 0))
                            if prev_val != 0:
                                rev_growth = round((curr_val - prev_val) / abs(prev_val) * 100, 1)
                                results[ticker]["Revenue Growth (YoY)"] = _fmt_pct_direct(rev_growth)
                    except (ValueError, TypeError):
                        pass

                eps = latest_annual("EarningsPerShareBasic")
                if eps is not None:
                    results[ticker]["EPS Growth Est."] = str(round(float(eps), 2))

                # Net margin from annual revenue + net income
                net_income = latest_annual("NetIncomeLoss")
                revenue = rev_annual[0].get("val") if rev_annual else None
                if net_income is not None and revenue and revenue != 0:
                    net_margin = round(float(net_income) / float(revenue) * 100, 1)
                    results[ticker]["Net Margin"] = _fmt_pct_direct(net_margin)

            except Exception as exc:
                print(f"[ENRICHER] EDGAR {ticker}: {exc}")

        edgar_tasks = [fetch_edgar(t) for t in tickers_upper]
        await asyncio.gather(*edgar_tasks)

    # Strip internal _prefixed tracking keys before returning
    for sym in results:
        results[sym] = {k: v for k, v in results[sym].items() if not k.startswith("_")}

    return results


def enrich_csv_data(
    csv_data: List[Dict],
    fundamentals: Dict[str, Dict],
) -> List[Dict]:
    """
    Merge fetched fundamentals into csv_data rows.

    Existing non-empty values are never overwritten; fetched data fills gaps only.
    """
    enriched = []
    for row in csv_data:
        sym = (
            row.get("Symbol")
            or row.get("symbol")
            or row.get("Ticker")
            or row.get("ticker")
            or ""
        ).strip().upper()
        new_row = dict(row)
        if sym and sym in fundamentals:
            for col, val in fundamentals[sym].items():
                if val and not new_row.get(col):
                    new_row[col] = str(val)
        enriched.append(new_row)
    return enriched


async def enrich_if_needed(
    tickers: List[str],
    csv_data: List[Dict],
    fmp_api_key: Optional[str] = None,
) -> List[Dict]:
    """
    Top-level helper. Detects missing fundamentals and, if absent, fetches them.
    Safe to call unconditionally — no-op when data is already present.
    """
    if has_fundamental_data(csv_data):
        print("[ENRICHER] CSV already has fundamentals — skipping enrichment")
        return csv_data

    key = fmp_api_key or os.getenv("FMP_API_KEY", "")
    if not key:
        print("[ENRICHER] No FMP_API_KEY — cannot enrich fundamentals")
        return csv_data

    print(f"[ENRICHER] Fetching fundamentals for {len(tickers)} ticker(s) via FMP + EDGAR")
    try:
        fundamentals = await fetch_fundamentals(tickers, key)
        enriched = enrich_csv_data(csv_data, fundamentals)
        filled = sum(
            1
            for row in enriched
            for col in _FUNDAMENTAL_COLS
            if row.get(col) and str(row.get(col)).strip()
        )
        print(
            f"[ENRICHER] Done — {filled} fundamental fields populated "
            f"across {len(enriched)} ticker(s)"
        )
        return enriched
    except Exception as exc:
        print(f"[ENRICHER] Enrichment failed ({exc}) — returning original csv_data")
        return csv_data
