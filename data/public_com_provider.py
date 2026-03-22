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

    async def _get_access_token(self) -> str:
        """Exchange the secret key for a time-limited access token."""
        if self._access_token:
            return self._access_token

        cache_key = "public_com:access_token"
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
                print(f"[PUBLIC.COM] Access token expired, clearing cache for retry")
                self._access_token = None
                cache_key = "public_com:access_token"
                cache.delete(cache_key) if hasattr(cache, 'delete') else None
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
                for batch in batches:
                    params = "&".join([f"osiSymbols={s}" for s in batch])
                    resp = await client.get(
                        f"{self.BASE_URL}/option-details/{account_id}/greeks?{params}",
                        headers=self._make_headers(self._access_token),
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        all_greeks.extend(data.get("greeks", []))

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

    @traceable(name="public_com.scan_high_volume_options")
    async def scan_high_volume_options(self, symbols: list) -> list:
        """
        For a list of tickers, get the nearest expiration chain and find
        contracts with high volume/OI ratios (unusual activity signal).
        Returns a flat list of notable contracts across all symbols.
        """
        notable = []

        async def _scan_one(symbol: str):
            expirations = await self.get_option_expirations(symbol)
            if not expirations:
                return []

            # Get nearest 2 expirations for signal density
            nearest = expirations[:2]
            results = []
            for exp in nearest:
                chain = await self.get_full_chain_with_greeks(symbol, exp)
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
            return results

        # Process up to 5 symbols concurrently to stay within rate limits
        sem = asyncio.Semaphore(5)

        async def _bounded(sym):
            async with sem:
                return await _scan_one(sym)

        results = await asyncio.gather(*[_bounded(s) for s in symbols[:15]])
        for r in results:
            notable.extend(r)

        # Sort by vol/OI ratio descending
        notable.sort(key=lambda x: x.get("vol_oi_ratio", 0), reverse=True)
        return notable[:50]
