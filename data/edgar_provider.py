import httpx
from datetime import datetime, timedelta


class EdgarProvider:
    """
    Provides SEC filing data from EDGAR (Electronic Data Gathering,
    Analysis, and Retrieval). Completely free, no API key needed.
    
    Covers:
    - Recent SEC filings (10-K, 10-Q, 8-K, etc.)
    - Institutional holdings (13-F filings)
    - Insider transactions (Form 3, 4, 5)
    - Company facts and financials
    
    SEC requires a User-Agent header with contact info.
    """

    BASE_URL = "https://efts.sec.gov/LATEST"
    DATA_URL = "https://data.sec.gov"
    HEADERS = {
        "User-Agent": "TradingAgent/1.0 (apixbt@gmail.com)",
        "Accept": "application/json",
    }

    def _get_cik(self, ticker: str) -> str:
        """Convert a ticker symbol to a CIK number (SEC's company ID)."""
        ticker = ticker.upper()
        try:
            resp = httpx.get(
                "https://www.sec.gov/files/company_tickers.json",
                headers=self.HEADERS,
                timeout=15,
            )
            data = resp.json()
            for entry in data.values():
                if entry.get("ticker", "").upper() == ticker:
                    return str(entry["cik_str"]).zfill(10)
        except Exception as e:
            print(f"EDGAR CIK lookup error for {ticker}: {e}")
        return None

    async def get_recent_filings(self, ticker: str, filing_type: str = None) -> list:
        """
        Get recent SEC filings for a company.
        
        Common filing types:
        - 10-K: Annual report (full financials)
        - 10-Q: Quarterly report
        - 8-K: Material events (acquisitions, leadership changes, etc.)
        - 4: Insider transactions
        - SC 13G/A: Institutional ownership changes
        - S-1: IPO registration
        - DEF 14A: Proxy statement
        """
        ticker = ticker.upper()
        try:
            cik = self._get_cik(ticker)
            if not cik:
                return [{"error": f"Could not find CIK for {ticker}"}]

            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.DATA_URL}/submissions/CIK{cik}.json",
                    headers=self.HEADERS,
                    timeout=15,
                )

            if resp.status_code != 200:
                return [{"error": f"SEC returned status {resp.status_code}"}]

            data = resp.json()
            recent = data.get("filings", {}).get("recent", {})

            forms = recent.get("form", [])
            dates = recent.get("filingDate", [])
            descriptions = recent.get("primaryDocDescription", [])
            accession_numbers = recent.get("accessionNumber", [])
            primary_docs = recent.get("primaryDocument", [])

            filings = []
            for i in range(min(len(forms), 50)):
                if filing_type and forms[i] != filing_type:
                    continue

                accession_clean = accession_numbers[i].replace("-", "")
                filing_url = (
                    f"https://www.sec.gov/Archives/edgar/data/"
                    f"{cik.lstrip('0')}/{accession_clean}/{primary_docs[i]}"
                )

                filings.append({
                    "form_type": forms[i],
                    "filing_date": dates[i],
                    "description": descriptions[i] if i < len(descriptions) else "",
                    "url": filing_url,
                })

                if len(filings) >= 10:
                    break

            return filings
        except Exception as e:
            print(f"EDGAR filings error for {ticker}: {e}")
            return [{"error": str(e)}]

    async def get_8k_filings(self, ticker: str) -> list:
        """
        Get recent 8-K filings — these are MATERIAL EVENTS.
        8-Ks are filed when something significant happens:
        earnings releases, acquisitions, executive departures,
        bankruptcy, material agreements, etc.
        This is often the fastest way to find out WHY a stock moved.
        """
        return await self.get_recent_filings(ticker, filing_type="8-K")

    async def get_insider_filings(self, ticker: str) -> list:
        """
        Get Form 4 filings — insider buy/sell transactions.
        Supplements Finnhub's insider data with direct SEC source.
        """
        return await self.get_recent_filings(ticker, filing_type="4")

    async def get_institutional_holders(self, ticker: str) -> dict:
        """
        Get institutional ownership data from company facts.
        Shows major institutional holders from 13-F filings.
        """
        ticker = ticker.upper()
        try:
            cik = self._get_cik(ticker)
            if not cik:
                return {"ticker": ticker, "error": f"Could not find CIK for {ticker}"}

            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.DATA_URL}/api/xbrl/companyfacts/CIK{cik}.json",
                    headers=self.HEADERS,
                    timeout=15,
                )

            if resp.status_code != 200:
                return {"ticker": ticker, "error": f"SEC returned status {resp.status_code}"}

            data = resp.json()
            facts = data.get("facts", {})
            us_gaap = facts.get("us-gaap", {})

            result = {"ticker": ticker}

            shares_data = us_gaap.get("CommonStockSharesOutstanding", {})
            if shares_data:
                units = shares_data.get("units", {})
                shares_list = units.get("shares", [])
                if shares_list:
                    latest = shares_list[-1]
                    result["shares_outstanding"] = latest.get("val")
                    result["shares_date"] = latest.get("end")

            revenue_data = us_gaap.get("Revenues", {}) or us_gaap.get("RevenueFromContractWithCustomerExcludingAssessedTax", {})
            if revenue_data:
                units = revenue_data.get("units", {})
                usd_list = units.get("USD", [])
                annual = [
                    r for r in usd_list
                    if r.get("form") == "10-K"
                ]
                if annual:
                    latest = annual[-1]
                    result["annual_revenue"] = latest.get("val")
                    result["revenue_period"] = latest.get("end")

            net_income_data = us_gaap.get("NetIncomeLoss", {})
            if net_income_data:
                units = net_income_data.get("units", {})
                usd_list = units.get("USD", [])
                annual = [
                    r for r in usd_list
                    if r.get("form") == "10-K"
                ]
                if annual:
                    latest = annual[-1]
                    result["annual_net_income"] = latest.get("val")
                    result["net_income_period"] = latest.get("end")

            assets_data = us_gaap.get("Assets", {})
            if assets_data:
                units = assets_data.get("units", {})
                usd_list = units.get("USD", [])
                if usd_list:
                    latest = usd_list[-1]
                    result["total_assets"] = latest.get("val")

            debt_data = us_gaap.get("LongTermDebt", {}) or us_gaap.get("LongTermDebtNoncurrent", {})
            if debt_data:
                units = debt_data.get("units", {})
                usd_list = units.get("USD", [])
                if usd_list:
                    latest = usd_list[-1]
                    result["long_term_debt"] = latest.get("val")

            cash_data = us_gaap.get("CashAndCashEquivalentsAtCarryingValue", {})
            if cash_data:
                units = cash_data.get("units", {})
                usd_list = units.get("USD", [])
                if usd_list:
                    latest = usd_list[-1]
                    result["cash_and_equivalents"] = latest.get("val")

            return result
        except Exception as e:
            print(f"EDGAR institutional data error for {ticker}: {e}")
            return {"ticker": ticker, "error": str(e)}

    async def get_company_summary(self, ticker: str) -> dict:
        """
        Get a combined summary: recent material filings + key financials.
        This is the main method to call for a quick SEC overview.
        """
        ticker = ticker.upper()

        eight_k = await self.get_8k_filings(ticker)
        financials = await self.get_institutional_holders(ticker)
        recent_all = await self.get_recent_filings(ticker)

        recent_filing_types = {}
        for f in recent_all:
            ftype = f.get("form_type", "")
            if ftype not in recent_filing_types:
                recent_filing_types[ftype] = f.get("filing_date")

        return {
            "ticker": ticker,
            "material_events_8k": eight_k[:5],
            "key_financials_from_sec": financials,
            "recent_filing_types": recent_filing_types,
            "most_recent_filing": recent_all[0] if recent_all else None,
        }

    async def search_filings(self, query: str, date_range: str = None) -> list:
        """
        Full-text search across all SEC filings.
        Useful for finding specific events, companies, or topics.
        """
        try:
            params = {
                "q": query,
                "dateRange": date_range or "custom",
                "startdt": (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
                "enddt": datetime.now().strftime("%Y-%m-%d"),
            }

            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.BASE_URL}/search-index?q={query}&dateRange=custom"
                    f"&startdt={params['startdt']}&enddt={params['enddt']}",
                    headers=self.HEADERS,
                    timeout=15,
                )

            if resp.status_code != 200:
                return []

            data = resp.json()
            hits = data.get("hits", {}).get("hits", [])

            results = []
            for hit in hits[:10]:
                source = hit.get("_source", {})
                results.append({
                    "company": source.get("display_names", ["Unknown"])[0]
                    if source.get("display_names")
                    else "Unknown",
                    "ticker": source.get("tickers", [""])[0]
                    if source.get("tickers")
                    else "",
                    "form_type": source.get("form_type", ""),
                    "filing_date": source.get("file_date", ""),
                    "description": source.get("display_description", ""),
                })
            return results
        except Exception as e:
            print(f"EDGAR search error: {e}")
            return []
