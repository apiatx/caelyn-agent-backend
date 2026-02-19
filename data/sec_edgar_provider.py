import asyncio
import time
import httpx
from datetime import datetime, timedelta
from data.cache import cache

EDGAR_CIK_TTL = 604800
EDGAR_FILINGS_TTL = 900
EDGAR_INSIDER_TTL = 1800
EDGAR_CATALYST_TTL = 900

HEADERS = {
    "User-Agent": "TradingAnalysisPlatform/1.0 (contact: apixbt@gmail.com)",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json",
}

_cik_map: dict | None = None
_cik_map_loaded_at: float = 0.0

_token_bucket_tokens: float = 2.0
_token_bucket_max: float = 2.0
_token_bucket_rate: float = 2.0
_token_bucket_last: float = 0.0

_last_error: str | None = None
_circuit_open: bool = False
_circuit_opened_at: float = 0.0
CIRCUIT_BREAKER_COOLDOWN = 300


def _refill_tokens():
    global _token_bucket_tokens, _token_bucket_last
    now = time.time()
    if _token_bucket_last == 0:
        _token_bucket_last = now
        return
    elapsed = now - _token_bucket_last
    _token_bucket_tokens = min(
        _token_bucket_max,
        _token_bucket_tokens + elapsed * _token_bucket_rate,
    )
    _token_bucket_last = now


async def _acquire_token():
    _refill_tokens()
    global _token_bucket_tokens
    if _token_bucket_tokens >= 1.0:
        _token_bucket_tokens -= 1.0
        return True
    wait = (1.0 - _token_bucket_tokens) / _token_bucket_rate
    await asyncio.sleep(wait)
    _refill_tokens()
    if _token_bucket_tokens >= 1.0:
        _token_bucket_tokens -= 1.0
        return True
    return False


class EdgarBudget:
    def __init__(self, max_requests: int = 3):
        self.max_requests = max_requests
        self.used = 0
        self.cache_hits = 0
        self.blocked = 0

    def can_spend(self) -> bool:
        return self.used < self.max_requests

    def spend(self):
        self.used += 1

    def record_cache_hit(self):
        self.cache_hits += 1

    def record_blocked(self):
        self.blocked += 1

    def summary(self) -> dict:
        return {
            "edgar_requests": self.used,
            "edgar_cache_hits": self.cache_hits,
            "edgar_blocked": self.blocked,
            "edgar_budget_max": self.max_requests,
        }


class SecEdgarProvider:
    DATA_URL = "https://data.sec.gov"
    TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

    def __init__(self):
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                headers=HEADERS,
                timeout=10.0,
                follow_redirects=True,
            )
        return self._client

    async def _fetch(self, url: str, budget: EdgarBudget | None = None) -> dict | None:
        global _last_error, _circuit_open, _circuit_opened_at

        if _circuit_open:
            if time.time() - _circuit_opened_at > CIRCUIT_BREAKER_COOLDOWN:
                _circuit_open = False
                print("[EDGAR] Circuit breaker reset after cooldown")
            else:
                if budget:
                    budget.record_blocked()
                return None

        if budget and not budget.can_spend():
            budget.record_blocked()
            return None

        if not await _acquire_token():
            if budget:
                budget.record_blocked()
            return None

        if budget:
            budget.spend()

        try:
            client = await self._get_client()
            resp = await client.get(url)
            if resp.status_code == 429:
                _circuit_open = True
                _circuit_opened_at = time.time()
                _last_error = "Rate limited (429)"
                print(f"[EDGAR] Rate limited! Circuit breaker opened for {CIRCUIT_BREAKER_COOLDOWN}s")
                return None
            if resp.status_code != 200:
                _last_error = f"HTTP {resp.status_code}"
                return None
            _last_error = None
            return resp.json()
        except Exception as e:
            _last_error = str(e)[:200]
            print(f"[EDGAR] Fetch error: {e}")
            return None

    async def _load_cik_map(self) -> dict:
        global _cik_map, _cik_map_loaded_at
        if _cik_map and (time.time() - _cik_map_loaded_at < EDGAR_CIK_TTL):
            return _cik_map

        try:
            client = await self._get_client()
            resp = await client.get(self.TICKERS_URL)
            if resp.status_code == 200:
                raw = resp.json()
                mapping = {}
                for entry in raw.values():
                    ticker = entry.get("ticker", "").upper()
                    cik = str(entry.get("cik_str", "")).zfill(10)
                    if ticker and cik:
                        mapping[ticker] = cik
                _cik_map = mapping
                _cik_map_loaded_at = time.time()
                print(f"[EDGAR] CIK map loaded: {len(mapping)} tickers")
                return mapping
        except Exception as e:
            print(f"[EDGAR] CIK map load error: {e}")

        return _cik_map or {}

    async def resolve_cik(self, symbol: str) -> str | None:
        symbol = symbol.upper().strip()
        cache_key = f"edgar:cik:{symbol}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        mapping = await self._load_cik_map()
        cik = mapping.get(symbol)
        if cik:
            cache.set(cache_key, cik, EDGAR_CIK_TTL)
        return cik

    async def get_recent_filings(
        self,
        cik: str,
        form_types: list[str] | None = None,
        lookback_days: int = 30,
        limit: int = 10,
        budget: EdgarBudget | None = None,
    ) -> list[dict]:
        cache_key = f"edgar:filings:{cik}:{','.join(form_types or ['all'])}:{lookback_days}"
        cached = cache.get(cache_key)
        if cached is not None:
            if budget:
                budget.record_cache_hit()
            return cached

        url = f"{self.DATA_URL}/submissions/CIK{cik}.json"
        data = await self._fetch(url, budget=budget)
        if not data:
            return []

        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        descriptions = recent.get("primaryDocDescription", [])
        accession_numbers = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])

        cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

        filings = []
        for i in range(min(len(forms), 100)):
            if i >= len(dates):
                break
            filed_at = dates[i]
            if filed_at < cutoff:
                continue
            form = forms[i]
            if form_types and not any(form.startswith(ft) for ft in form_types):
                continue

            accession_clean = accession_numbers[i].replace("-", "") if i < len(accession_numbers) else ""
            doc = primary_docs[i] if i < len(primary_docs) else ""
            cik_num = cik.lstrip("0") or "0"
            url_str = f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{accession_clean}/{doc}" if accession_clean and doc else ""

            filings.append({
                "form": form,
                "filed_at": filed_at,
                "title": descriptions[i] if i < len(descriptions) else "",
                "url": url_str,
            })
            if len(filings) >= limit:
                break

        cache.set(cache_key, filings, EDGAR_FILINGS_TTL)
        return filings

    async def get_form4_insider_summary(
        self,
        cik: str,
        lookback_days: int = 30,
        limit: int = 10,
        budget: EdgarBudget | None = None,
    ) -> dict:
        cache_key = f"edgar:insider:{cik}:{lookback_days}"
        cached = cache.get(cache_key)
        if cached is not None:
            if budget:
                budget.record_cache_hit()
            return cached

        filings = await self.get_recent_filings(
            cik, form_types=["4"], lookback_days=lookback_days, limit=limit, budget=budget,
        )

        if not filings:
            result = {"count": 0, "signal": "unknown", "last_filed_at": None, "summary": "No Form 4 filings found"}
            cache.set(cache_key, result, EDGAR_INSIDER_TTL)
            return result

        count = len(filings)
        last_filed = filings[0]["filed_at"] if filings else None

        buy_keywords = ["acquisition", "purchase", "award", "grant", "exercise"]
        sell_keywords = ["disposition", "sale", "sold"]
        buy_count = 0
        sell_count = 0
        for f in filings:
            title_lower = (f.get("title") or "").lower()
            if any(k in title_lower for k in buy_keywords):
                buy_count += 1
            elif any(k in title_lower for k in sell_keywords):
                sell_count += 1

        if buy_count > sell_count and buy_count > 0:
            signal = "net_buying"
        elif sell_count > buy_count and sell_count > 0:
            signal = "net_selling"
        elif buy_count > 0 and sell_count > 0:
            signal = "mixed"
        else:
            signal = "unknown"

        if signal == "net_buying":
            summary = f"{count} Form 4 filings in {lookback_days}d, net insider buying detected ({buy_count} buy vs {sell_count} sell)"
        elif signal == "net_selling":
            summary = f"{count} Form 4 filings in {lookback_days}d, net insider selling ({sell_count} sell vs {buy_count} buy)"
        elif signal == "mixed":
            summary = f"{count} Form 4 filings in {lookback_days}d, mixed insider activity ({buy_count} buy, {sell_count} sell)"
        else:
            summary = f"{count} Form 4 filings in {lookback_days}d"

        result = {
            "count": count,
            "signal": signal,
            "last_filed_at": last_filed,
            "summary": summary,
        }
        cache.set(cache_key, result, EDGAR_INSIDER_TTL)
        return result

    async def get_8k_s1_catalysts(
        self,
        cik: str,
        lookback_days: int = 14,
        limit: int = 10,
        budget: EdgarBudget | None = None,
    ) -> list[dict]:
        cache_key = f"edgar:catalysts:{cik}:{lookback_days}"
        cached = cache.get(cache_key)
        if cached is not None:
            if budget:
                budget.record_cache_hit()
            return cached

        target_forms = ["8-K", "S-1", "S-3", "424B", "10-Q", "10-K"]
        filings = await self.get_recent_filings(
            cik, form_types=target_forms, lookback_days=lookback_days, limit=limit, budget=budget,
        )

        catalysts = []
        for f in filings:
            catalysts.append({
                "form": f["form"],
                "filed_at": f["filed_at"],
                "title": f.get("title") or f["form"],
                "url": f.get("url", ""),
            })

        cache.set(cache_key, catalysts, EDGAR_CATALYST_TTL)
        return catalysts

    def get_health(self) -> dict:
        return {
            "enabled": True,
            "last_error": _last_error,
            "circuit": "open" if _circuit_open else "closed",
        }
