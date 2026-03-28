import httpx
import asyncio
from data.cache import cache

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop

OPTIONS_CACHE_TTL = 120  # 2 min — options data is time-sensitive
_ACCESS_TOKEN_VALIDITY_MINUTES = 55  # request 55-min tokens, cache for 54 min
_ACCESS_TOKEN_CACHE_TTL = 54 * 60   # 54 minutes in seconds


class PublicComProvider:
    """
    Public.com brokerage API provider for live options data.
    Endpoints: option expirations, option chain, quotes (volume/OI), greeks.
    Auth: Two-step — exchange secret key for access token, then Bearer token.
    Rate limit: 10 req/sec globally.
    """

    BASE_URL = "https://api.public.com/userapigateway"
    AUTH_URL = "https://api.public.com/userapiauthservice/personal/access-tokens"

    def __init__(self, api_key: str):
        self.api_key = api_key          # This is the SECRET key
        self._account_id = None
        self._access_token = None       # Fetched via token exchange
        self._token_lock = asyncio.Lock()
        self._account_lock = asyncio.Lock()

    async def _get_access_token(self) -> str:
        """Exchange the secret key for a time-limited access token.
        Uses a lock to prevent concurrent token exchanges (race condition)."""
        if self._access_token:
            return self._access_token

        cache_key = "public_com:access_token"
        cached = cache.get(cache_key)
        if cached:
            self._access_token = cached
            return cached

        async with self._token_lock:
            # Re-check after acquiring the lock (another coroutine may have set it)
            if self._access_token:
                return self._access_token
            cached = cache.get(cache_key)
            if cached:
                self._access_token = cached
                return cached

            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.post(
                        self.AUTH_URL,
                        headers={"Content-Type": "application/json"},
                        json={
                            "secret": self.api_key,
                            "validityInMinutes": _ACCESS_TOKEN_VALIDITY_MINUTES,
                        },
                    )
                if resp.status_code == 200:
                    token = resp.json().get("accessToken")
                    if token:
                        self._access_token = token
                        cache.set(cache_key, token, _ACCESS_TOKEN_CACHE_TTL)
                        print("[PUBLIC.COM] Access token obtained successfully")
                        return token
                print(f"[PUBLIC.COM] Token exchange failed: {resp.status_code} {resp.text[:300]}")
                return None
            except Exception as e:
                print(f"[PUBLIC.COM] Token exchange error: {e}")
                return None

    def _make_headers(self, access_token: str) -> dict:
        return {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

    async def _get_account_id(self) -> str:
        if self._account_id:
            return self._account_id

        async with self._account_lock:
            # Re-check after acquiring lock
            if self._account_id:
                return self._account_id

            access_token = await self._get_access_token()
            if not access_token:
                return None

            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.get(
                        f"{self.BASE_URL}/trading/account",
                        headers=self._make_headers(access_token),
                    )
                if resp.status_code == 200:
                    data = resp.json()
                    accounts = data.get("accounts", [])
                    if accounts:
                        self._account_id = accounts[0].get("accountId")
                        print(f"[PUBLIC.COM] Account ID resolved: {self._account_id}")
                        return self._account_id
                elif resp.status_code == 401:
                    # Token expired mid-session — clear and retry once
                    print("[PUBLIC.COM] Access token expired, clearing for retry")
                    self._access_token = None
                    if hasattr(cache, 'delete'):
                        cache.delete("public_com:access_token")
                print(f"[PUBLIC.COM] Failed to get account ID: {resp.status_code} {resp.text[:200]}")
                return None
            except Exception as e:
                print(f"[PUBLIC.COM] Account ID error: {e}")
                return None

    @traceable(name="public_com.get_option_expirations")
    async def get_option_expirations(self, symbol: str) -> list:
        """Get available expiration dates for a ticker."""
        symbol = symbol.upper()
        cache_key = f"public_com:expirations:{symbol}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        account_id = await self._get_account_id()
        if not account_id:
            return []

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    f"{self.BASE_URL}/marketdata/{account_id}/option-expirations",
                    headers=self._make_headers(self._access_token),
                    json={"instrument": {"symbol": symbol, "type": "EQUITY"}},
                )
            if resp.status_code == 200:
                data = resp.json()
                expirations = data.get("expirations", [])
                cache.set(cache_key, expirations, OPTIONS_CACHE_TTL * 5)
                return expirations
            print(f"[PUBLIC.COM] Expirations error {resp.status_code}: {resp.text[:200]}")
            return []
        except Exception as e:
            print(f"[PUBLIC.COM] Expirations error for {symbol}: {e}")
            return []

    @traceable(name="public_com.get_option_chain")
    async def get_option_chain(self, symbol: str, expiration: str) -> dict:
        """
        Get full option chain (calls + puts) for a symbol and expiration date.
        expiration format: YYYY-MM-DD
        """
        symbol = symbol.upper()
        cache_key = f"public_com:chain:{symbol}:{expiration}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        account_id = await self._get_account_id()
        if not account_id:
            return {}

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    f"{self.BASE_URL}/marketdata/{account_id}/option-chain",
                    headers=self._make_headers(self._access_token),
                    json={
                        "instrument": {"symbol": symbol, "type": "EQUITY"},
                        "expirationDate": expiration,
                    },
                )
            if resp.status_code == 200:
                data = resp.json()
                cache.set(cache_key, data, OPTIONS_CACHE_TTL)
                return data
            print(f"[PUBLIC.COM] Chain error {resp.status_code}: {resp.text[:200]}")
            return {}
        except Exception as e:
            print(f"[PUBLIC.COM] Chain error for {symbol}/{expiration}: {e}")
            return {}

    @traceable(name="public_com.get_quotes")
    async def get_quotes(self, symbols: list, instrument_type: str = "OPTION") -> list:
        """
        Get real-time quotes (bid, ask, last, volume, openInterest) for option or stock symbols.
        For options, pass OSI-format symbols (e.g. AAPL240216C00140000).
        """
        if not symbols:
            return []

        cache_key = f"public_com:quotes:{','.join(symbols[:10])}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        account_id = await self._get_account_id()
        if not account_id:
            return []

        instruments = [{"symbol": s, "type": instrument_type} for s in symbols]

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    f"{self.BASE_URL}/marketdata/{account_id}/quotes",
                    headers=self._make_headers(self._access_token),
                    json={"instruments": instruments},
                )
            if resp.status_code == 200:
                data = resp.json()
                quotes = data.get("quotes", [])
                cache.set(cache_key, quotes, OPTIONS_CACHE_TTL)
                return quotes
            print(f"[PUBLIC.COM] Quotes error {resp.status_code}: {resp.text[:200]}")
            return []
        except Exception as e:
            print(f"[PUBLIC.COM] Quotes error: {e}")
            return []

    @traceable(name="public_com.get_option_greeks")
    async def get_option_greeks(self, osi_symbols: list) -> list:
        """
        Get greeks (delta, gamma, theta, vega, rho, IV) for up to 250 option contracts.
        osi_symbols: list of OSI-format option symbols.
        """
        if not osi_symbols:
            return []

        cache_key = f"public_com:greeks:{','.join(osi_symbols[:5])}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        account_id = await self._get_account_id()
        if not account_id:
            return []

        # Chunk into batches of 250 (API max)
        batches = [osi_symbols[i:i+250] for i in range(0, len(osi_symbols), 250)]
        all_greeks = []

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                async def _fetch_batch(batch):
                    params = "&".join([f"osiSymbols={s}" for s in batch])
                    resp = await client.get(
                        f"{self.BASE_URL}/option-details/{account_id}/greeks?{params}",
                        headers=self._make_headers(self._access_token),
                    )
                    if resp.status_code == 200:
                        return resp.json().get("greeks", [])
                    return []

                batch_results = await asyncio.gather(*[_fetch_batch(b) for b in batches])
                for greeks in batch_results:
                    all_greeks.extend(greeks)

            cache.set(cache_key, all_greeks, OPTIONS_CACHE_TTL)
            return all_greeks
        except Exception as e:
            print(f"[PUBLIC.COM] Greeks error: {e}")
            return []

    @traceable(name="public_com.get_full_chain_with_greeks")
    async def get_full_chain_with_greeks(self, symbol: str, expiration: str) -> dict:
        """
        Get a complete option chain with greeks merged in.
        Returns {calls: [...], puts: [...]} with each contract enriched with greeks + quotes.
        """
        chain = await self.get_option_chain(symbol, expiration)
        if not chain:
            return {"calls": [], "puts": [], "baseSymbol": symbol}

        calls = chain.get("calls", [])
        puts = chain.get("puts", [])

        # Collect all OSI symbols to fetch greeks + quotes in bulk
        all_osi = []
        for c in calls:
            s = c.get("instrument", {}).get("symbol")
            if s:
                all_osi.append(s)
        for p in puts:
            s = p.get("instrument", {}).get("symbol")
            if s:
                all_osi.append(s)

        # Fetch greeks and volume/OI quotes in parallel
        greeks_list, quotes_list = await asyncio.gather(
            self.get_option_greeks(all_osi),
            self.get_quotes(all_osi, "OPTION"),
        )

        # Index by symbol for fast merge
        greeks_map = {}
        for g in greeks_list:
            sym = g.get("symbol")
            if sym:
                greeks_map[sym] = g.get("greeks", {})

        quotes_map = {}
        for q in quotes_list:
            sym = q.get("instrument", {}).get("symbol")
            if sym:
                quotes_map[sym] = q

        def _enrich(contract):
            sym = contract.get("instrument", {}).get("symbol", "")
            g = greeks_map.get(sym, {})
            q = quotes_map.get(sym, {})
            # Parse strike from OSI symbol (last 8 chars / 1000)
            strike = None
            if len(sym) >= 15:
                try:
                    strike = int(sym[-8:]) / 1000
                except ValueError:
                    pass
            return {
                "symbol": sym,
                "strike": strike,
                "bid": contract.get("bid") or q.get("bid"),
                "ask": contract.get("ask") or q.get("ask"),
                "last": contract.get("last") or q.get("last"),
                "volume": q.get("volume"),
                "openInterest": q.get("openInterest"),
                "delta": g.get("delta"),
                "gamma": g.get("gamma"),
                "theta": g.get("theta"),
                "vega": g.get("vega"),
                "iv": g.get("impliedVolatility"),
            }

        enriched_calls = [_enrich(c) for c in calls]
        enriched_puts = [_enrich(p) for p in puts]

        return {
            "baseSymbol": chain.get("baseSymbol", symbol),
            "expiration": expiration,
            "calls": enriched_calls,
            "puts": enriched_puts,
        }

    # ETFs recognized for category tagging
    _ETF_SET = {
        "SPY", "QQQ", "IWM", "GLD", "TLT", "XLF", "XLK", "XLE", "XLV",
        "HYG", "DIA", "EEM", "ARKK", "SMH", "SOXX", "VXX", "UVXY",
    }

    @traceable(name="public_com.scan_full_screener")
    async def scan_full_screener(self, symbols: list) -> dict:
        """
        Comprehensive screener: fetch full option chains (nearest expiry) for all
        symbols, compute per-ticker summary metrics and build a flat contracts list.
        Concurrency=4 — safe within 10 req/sec limit (4 tickers × 3 sequential
        calls each = 12 calls/~3 sec window, well under the rate cap).

        Returns:
          {
            "tickers": [...],          # per-ticker summary rows (for screener table)
            "all_contracts": [...],    # flat list of every active contract (for flow view)
            "market_summary": {...},   # aggregated stats
          }
        """

        async def _fetch_one(symbol: str) -> dict | None:
            expirations = await self.get_option_expirations(symbol)
            if not expirations:
                return None
            exp = expirations[0]
            chain = await self.get_full_chain_with_greeks(symbol, exp)
            calls = chain.get("calls", [])
            puts  = chain.get("puts", [])
            return {"symbol": symbol, "expiration": exp, "calls": calls, "puts": puts}

        sem = asyncio.Semaphore(4)

        async def _bounded(sym):
            async with sem:
                try:
                    return await _fetch_one(sym)
                except Exception as e:
                    print(f"[PUBLIC.COM] Screener fetch error for {sym}: {e}")
                    return None

        raw_results = await asyncio.gather(*[_bounded(s) for s in symbols])

        def _safe_int(v):
            try:
                return int(v or 0)
            except Exception:
                return 0

        def _safe_float(v):
            try:
                return float(v)
            except Exception:
                return None

        tickers = []
        all_contracts = []

        for res in raw_results:
            if not res:
                continue

            symbol = res["symbol"]
            exp    = res["expiration"]
            calls  = res["calls"]
            puts   = res["puts"]
            category = "etf" if symbol.upper() in self._ETF_SET else "stock"

            # Active contracts (volume > 0)
            active_calls = [c for c in calls if _safe_int(c.get("volume")) > 0]
            active_puts  = [p for p in puts  if _safe_int(p.get("volume")) > 0]

            call_vol = sum(_safe_int(c.get("volume"))       for c in active_calls)
            put_vol  = sum(_safe_int(p.get("volume"))       for p in active_puts)
            call_oi  = sum(_safe_int(c.get("openInterest")) for c in calls)
            put_oi   = sum(_safe_int(p.get("openInterest")) for p in puts)

            # Volume-weighted average IV
            def _vwiv(contracts):
                total_vol = sum(_safe_int(c.get("volume")) for c in contracts)
                if total_vol == 0:
                    return None
                wsum = sum(
                    (_safe_float(c.get("iv")) or 0) * _safe_int(c.get("volume"))
                    for c in contracts
                )
                v = wsum / total_vol
                return round(v, 4) if v > 0 else None

            avg_call_iv = _vwiv(active_calls)
            avg_put_iv  = _vwiv(active_puts)
            iv_skew = (
                round(avg_put_iv - avg_call_iv, 4)
                if avg_call_iv and avg_put_iv else None
            )

            # Max pain: strike with highest combined OI
            oi_by_strike = {}
            for c in calls + puts:
                s = c.get("strike")
                if s is not None:
                    oi_by_strike[s] = oi_by_strike.get(s, 0) + _safe_int(c.get("openInterest"))
            max_pain = max(oi_by_strike, key=oi_by_strike.get) if oi_by_strike else None

            # Top contracts by volume
            top_calls = sorted(active_calls, key=lambda x: _safe_int(x.get("volume")), reverse=True)[:10]
            top_puts  = sorted(active_puts,  key=lambda x: _safe_int(x.get("volume")), reverse=True)[:10]

            ticker_row = {
                "ticker":        symbol,
                "category":      category,
                "expiration":    exp,
                "call_volume":   call_vol,
                "put_volume":    put_vol,
                "total_volume":  call_vol + put_vol,
                "pc_ratio":      round(put_vol / call_vol, 3) if call_vol > 0 else None,
                "call_oi":       call_oi,
                "put_oi":        put_oi,
                "total_oi":      call_oi + put_oi,
                "avg_call_iv":   avg_call_iv,
                "avg_put_iv":    avg_put_iv,
                "iv_skew":       iv_skew,
                "max_pain":      max_pain,
                "top_calls":     top_calls,
                "top_puts":      top_puts,
            }
            tickers.append(ticker_row)

            # Flatten contracts for the "flow" view
            for c in active_calls:
                vol  = _safe_int(c.get("volume"))
                oi_v = _safe_int(c.get("openInterest"))
                all_contracts.append({
                    **c,
                    "underlying": symbol,
                    "category":   category,
                    "expiration": exp,
                    "side":       "call",
                    "vol_oi_ratio": round(vol / oi_v, 2) if oi_v > 0 else None,
                })
            for p in active_puts:
                vol  = _safe_int(p.get("volume"))
                oi_v = _safe_int(p.get("openInterest"))
                all_contracts.append({
                    **p,
                    "underlying": symbol,
                    "category":   category,
                    "expiration": exp,
                    "side":       "put",
                    "vol_oi_ratio": round(vol / oi_v, 2) if oi_v > 0 else None,
                })

        # Sort tickers by total volume descending (default)
        tickers.sort(key=lambda x: x.get("total_volume", 0), reverse=True)
        all_contracts.sort(key=lambda x: _safe_int(x.get("volume")), reverse=True)

        # Aggregated market summary
        total_call_vol = sum(t.get("call_volume", 0) for t in tickers)
        total_put_vol  = sum(t.get("put_volume", 0)  for t in tickers)
        market_summary = {
            "tickers_scanned":   len(tickers),
            "total_call_volume": total_call_vol,
            "total_put_volume":  total_put_vol,
            "market_pc_ratio":   round(total_put_vol / total_call_vol, 3) if total_call_vol > 0 else None,
            "total_contracts":   len(all_contracts),
            "most_active_ticker": tickers[0]["ticker"] if tickers else None,
        }

        return {
            "tickers":        tickers,
            "all_contracts":  all_contracts[:500],  # cap at 500 for payload size
            "market_summary": market_summary,
        }

    @traceable(name="public_com.scan_high_volume_options")
    async def scan_high_volume_options(self, symbols: list) -> tuple:
        """
        For a list of tickers, get the nearest expiration chain and find
        contracts with high volume/OI ratios (unusual activity signal).
        Returns (notable_contracts, all_chains_dict) so callers can reuse
        the fetched chains without a second round of API calls.
        Scans 8 tickers max, nearest expiry only — keeps total API calls ~24.
        """
        notable = []
        all_chains = {}

        async def _scan_one(symbol: str):
            expirations = await self.get_option_expirations(symbol)
            if not expirations:
                return [], {}

            # Nearest expiry only — keeps calls to 3 per ticker (chain+greeks+quotes)
            exp = expirations[0]
            chain = await self.get_full_chain_with_greeks(symbol, exp)
            results = []
            for side in ("calls", "puts"):
                for c in chain.get(side, []):
                    vol = c.get("volume")
                    oi = c.get("openInterest")
                    if vol and oi and int(vol) > 0 and int(oi) > 0:
                        ratio = int(vol) / int(oi)
                        if ratio > 1.5 or int(vol) > 5000:
                            results.append({
                                **c,
                                "underlying": symbol,
                                "expiration": exp,
                                "side": "call" if side == "calls" else "put",
                                "vol_oi_ratio": round(ratio, 2),
                            })
            return results, {symbol: {"expiration": exp, **chain}}

        # Process 3 symbols concurrently — Public.com allows 10 req/sec,
        # each ticker makes 3-4 sequential calls, so 3 concurrent = ~9 req burst
        # Cap at 8 tickers (was 15) — 8 × 3 calls = 24 total API calls
        sem = asyncio.Semaphore(3)

        async def _bounded(sym):
            async with sem:
                return await _scan_one(sym)

        results = await asyncio.gather(*[_bounded(s) for s in symbols[:8]])
        for contracts, chain_map in results:
            notable.extend(contracts)
            all_chains.update(chain_map)

        # Sort by vol/OI ratio descending
        notable.sort(key=lambda x: x.get("vol_oi_ratio", 0), reverse=True)
        return notable[:50], all_chains
