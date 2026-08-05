"""
Tests for FMPProvider.search_securities() and the /api/watchlist/security-search endpoint.

Covers:
  1. Both provider endpoints succeed with results
  2. Exact ticker result ranks before prefix/name matches
  3. One endpoint fails, the other succeeds (partial provider success)
  4. Both endpoints time out → FMPSearchProviderError raised
  5. Both endpoints return non-2xx → FMPSearchProviderError raised
  6. Both endpoints return valid empty arrays → [] (genuine zero result)
  7. Canonical registry failure → FMP fallback canonicalization still works
  8. Provider failure is distinguishable from valid zero result (HTTP 503 vs 200)
  9. No Watchlist membership or database mutation during search

All external HTTP calls are mocked. Tests never hit live FMP endpoints.
"""
import asyncio
import sys
import os
import types
import pytest

# ── path setup ────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Minimal cache stub (prevents import errors from data.cache) ───────────────
_cache_store: dict = {}


class _FakeCache:
    def get(self, key):
        return _cache_store.get(key)

    def set(self, key, value, ttl=None):
        _cache_store[key] = value


def _patch_cache(monkeypatch):
    """Replace the shared cache used by fmp_provider with an in-memory stub."""
    import data.fmp_provider as _mod
    monkeypatch.setattr(_mod, "cache", _FakeCache())
    _cache_store.clear()


# ── FMP response factory helpers ──────────────────────────────────────────────

def _make_fmp_item(symbol="NVDA", name="NVIDIA Corporation",
                   exchange="NASDAQ", exchange_full="Nasdaq Global Select Market",
                   currency="USD", country="US"):
    return {
        "symbol": symbol,
        "name": name,
        "exchange": exchange,
        "exchangeFullName": exchange_full,
        "currency": currency,
        "country": country,
        "type": "stock",
        "delistingDate": None,
    }


class _FakeResponse:
    """Minimal httpx.Response stand-in."""

    def __init__(self, status_code: int, body):
        self.status_code = status_code
        self._body = body

    def json(self):
        return self._body

    @property
    def text(self):
        import json
        return json.dumps(self._body)


# ── Canonical adapter stub ────────────────────────────────────────────────────

def _stub_canonical(monkeypatch):
    """
    Replace canonical_security_adapter with a trivial stub:
      - build_canonical_registry() → {}  (empty registry — no watchlist members)
      - resolve_with_registry(bare_sym, exch_code, registry) → bare_sym for NASDAQ/US,
        exch_code + ":" + bare_sym otherwise
    Never touches the database.
    """
    stub = types.ModuleType("services.canonical_security_adapter")

    def _build():
        return {}

    def _resolve(bare_sym, exch_code, registry):
        if exch_code in ("NASDAQ", "NYSE", "AMEX", ""):
            return bare_sym
        return f"{exch_code}:{bare_sym}" if exch_code else bare_sym

    stub.build_canonical_registry = _build
    stub.resolve_with_registry = _resolve
    sys.modules["services.canonical_security_adapter"] = stub
    return stub


# ── Helper: run search_securities with mocked asyncio.gather ─────────────────

async def _run_search(monkeypatch, sym_result, name_result, query="NVDA", limit=10):
    """
    sym_result / name_result may be:
      - a list[dict]   → success (status 200)
      - an Exception   → gather returns that exception for this endpoint
      - an int         → non-2xx status code (body=[])
    """
    _stub_canonical(monkeypatch)
    _patch_cache(monkeypatch)

    import data.fmp_provider as _mod

    def _make_resp(val):
        if isinstance(val, Exception):
            return val
        if isinstance(val, int):
            return _FakeResponse(val, [])
        return _FakeResponse(200, val)

    sym_r = _make_resp(sym_result)
    name_r = _make_resp(name_result)

    async def _fake_gather(*coros, return_exceptions=False):
        return [sym_r, name_r]

    monkeypatch.setattr(asyncio, "gather", _fake_gather)

    # Also patch asyncio inside the module's local alias  (_aio)
    # fmp_provider imports asyncio at the top and uses it as _aio inside the method
    # The module-level asyncio is the same object — patching asyncio.gather is enough.

    provider = _mod.FMPProvider("test-key")
    return await provider.search_securities(query, limit=limit)


# ═════════════════════════════════════════════════════════════════════════════
# 1. Both endpoints succeed with results
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_both_succeed_returns_results(monkeypatch):
    sym_rows = [_make_fmp_item("NVDA", "NVIDIA Corporation", "NASDAQ")]
    name_rows = [_make_fmp_item("NVDA", "NVIDIA Corporation", "NASDAQ")]  # duplicate — deduped
    results = await _run_search(monkeypatch, sym_rows, name_rows, query="NVDA")
    assert len(results) >= 1
    canonical_tickers = [r["canonical_ticker"] for r in results]
    assert "NVDA" in canonical_tickers


# ═════════════════════════════════════════════════════════════════════════════
# 2. Exact ticker ranks before prefix and name matches
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_exact_ticker_ranks_first(monkeypatch):
    sym_rows = [
        _make_fmp_item("NVD",   "Some NVD Corp",     "NASDAQ"),  # prefix match
        _make_fmp_item("NVDA",  "NVIDIA Corporation", "NASDAQ"),  # exact match
        _make_fmp_item("NVDAX", "NVDA Extended Fund", "NASDAQ"),  # prefix match
    ]
    name_rows = [
        _make_fmp_item("FOO", "NVDA Something Inc", "NYSE"),  # name match
    ]
    results = await _run_search(monkeypatch, sym_rows, name_rows, query="NVDA")
    assert results, "Expected at least one result"
    assert results[0]["canonical_ticker"] == "NVDA", (
        f"Exact ticker should rank first; got {results[0]['canonical_ticker']!r}"
    )
    # Verify prefix matches come before name matches
    tickers = [r["canonical_ticker"] for r in results]
    exact_idx   = tickers.index("NVDA")  if "NVDA"  in tickers else None
    prefix_idx  = tickers.index("NVDAX") if "NVDAX" in tickers else None
    name_idx    = tickers.index("FOO")   if "FOO"   in tickers else None
    if prefix_idx is not None and exact_idx is not None:
        assert exact_idx < prefix_idx
    if name_idx is not None and prefix_idx is not None:
        assert prefix_idx < name_idx


# ═════════════════════════════════════════════════════════════════════════════
# 3. One endpoint fails, the other succeeds (partial provider success)
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_partial_failure_sym_fails(monkeypatch):
    """search-symbol times out; search-name returns a result → result preserved."""
    import httpx
    name_rows = [_make_fmp_item("NVDA", "NVIDIA Corporation", "NASDAQ")]
    results = await _run_search(
        monkeypatch,
        sym_result=httpx.ReadTimeout("timeout", request=None),
        name_result=name_rows,
        query="NVDA",
    )
    assert len(results) >= 1, "Partial success: name endpoint result should be returned"
    assert results[0]["canonical_ticker"] == "NVDA"


@pytest.mark.asyncio
async def test_partial_failure_name_fails(monkeypatch):
    """search-name returns 503; search-symbol returns a result → result preserved."""
    sym_rows = [_make_fmp_item("MSFT", "Microsoft Corporation", "NASDAQ")]
    results = await _run_search(
        monkeypatch,
        sym_result=sym_rows,
        name_result=503,   # non-2xx status code
        query="MSFT",
    )
    assert len(results) >= 1, "Partial success: symbol endpoint result should be returned"
    assert results[0]["canonical_ticker"] == "MSFT"


# ═════════════════════════════════════════════════════════════════════════════
# 4. Both endpoints time out → FMPSearchProviderError raised
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_both_timeout_raises_provider_error(monkeypatch):
    import httpx
    from data.fmp_provider import FMPSearchProviderError
    with pytest.raises(FMPSearchProviderError):
        await _run_search(
            monkeypatch,
            sym_result=httpx.ReadTimeout("timeout", request=None),
            name_result=httpx.ConnectTimeout("timeout", request=None),
            query="NVDA",
        )


# ═════════════════════════════════════════════════════════════════════════════
# 5. Both endpoints return non-2xx → FMPSearchProviderError raised
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_both_non_2xx_raises_provider_error(monkeypatch):
    from data.fmp_provider import FMPSearchProviderError
    with pytest.raises(FMPSearchProviderError):
        await _run_search(
            monkeypatch,
            sym_result=429,   # rate-limited
            name_result=503,  # service unavailable
            query="NVDA",
        )


# ═════════════════════════════════════════════════════════════════════════════
# 6. Both endpoints return valid empty arrays → genuine zero result (no error)
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_valid_empty_result_no_exception(monkeypatch):
    """Zero match is not a failure — at least one endpoint responded successfully."""
    results = await _run_search(
        monkeypatch,
        sym_result=[],   # valid 200, empty array
        name_result=[],  # valid 200, empty array
        query="ZZZZNONEXISTENT",
    )
    assert results == [], f"Expected empty list, got {results}"


@pytest.mark.asyncio
async def test_valid_empty_result_one_empty_one_results(monkeypatch):
    """sym returns 0 rows, name returns 1 row → 1 result, no error."""
    name_rows = [_make_fmp_item("CRWV", "CoreWeave Inc", "NASDAQ")]
    results = await _run_search(
        monkeypatch,
        sym_result=[],
        name_result=name_rows,
        query="CoreWeave",
    )
    assert len(results) == 1
    assert results[0]["canonical_ticker"] == "CRWV"


# ═════════════════════════════════════════════════════════════════════════════
# 7. Canonical registry failure → FMP fallback canonicalization still works
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_registry_failure_falls_back_gracefully(monkeypatch):
    """
    When build_canonical_registry() fails (raises), the result set falls back
    to FMP exchange-code → canonical prefix mapping (resolve_with_registry still
    called with empty registry).  Results still returned.
    """
    _patch_cache(monkeypatch)

    import data.fmp_provider as _mod

    # Stub adapter: build_canonical_registry raises; resolve_with_registry still works
    stub = types.ModuleType("services.canonical_security_adapter")

    def _build_fail():
        raise RuntimeError("DB unavailable")

    def _resolve(bare_sym, exch_code, registry):
        return bare_sym  # fallback — plain ticker

    stub.build_canonical_registry = _build_fail
    stub.resolve_with_registry = _resolve
    sys.modules["services.canonical_security_adapter"] = stub

    sym_rows = [_make_fmp_item("NVDA", "NVIDIA Corporation", "NASDAQ")]

    async def _fake_gather(*coros, return_exceptions=False):
        return [_FakeResponse(200, sym_rows), _FakeResponse(200, [])]

    monkeypatch.setattr(asyncio, "gather", _fake_gather)

    provider = _mod.FMPProvider("test-key")
    results = await provider.search_securities("NVDA", limit=10)
    assert len(results) >= 1
    assert results[0]["canonical_ticker"] == "NVDA"


# ═════════════════════════════════════════════════════════════════════════════
# 8. Provider failure is distinguishable from valid zero result
#    — at the provider layer: FMPSearchProviderError vs empty list
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_failure_vs_empty_result_distinguishable(monkeypatch):
    """
    Total failure raises FMPSearchProviderError.
    Valid empty result returns [].
    These must be distinct — the core reliability fix.
    """
    import httpx
    from data.fmp_provider import FMPSearchProviderError

    # Case A: both fail → exception
    with pytest.raises(FMPSearchProviderError):
        await _run_search(
            monkeypatch,
            sym_result=httpx.ReadTimeout("timeout", request=None),
            name_result=httpx.ReadTimeout("timeout", request=None),
            query="NVDA",
        )

    # Case B: both succeed with empty → list (not exception)
    empty = await _run_search(
        monkeypatch,
        sym_result=[],
        name_result=[],
        query="ZZNONEXIST",
    )
    assert empty == []


# ═════════════════════════════════════════════════════════════════════════════
# 9. No Watchlist membership or database mutation during search
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_no_db_mutation_during_search(monkeypatch):
    """
    search_securities() must not call any Watchlist save/mutation functions.
    We verify by patching the save path and asserting it is never called.
    """
    _patch_cache(monkeypatch)

    mutation_called = []

    # Stub the watchlist store to detect any mutation
    wl_stub = types.ModuleType("services.watchlist_service")
    wl_stub.save_watchlist = lambda *a, **kw: mutation_called.append(("save", a, kw))
    wl_stub.load_watchlist = lambda *a, **kw: {}
    sys.modules["services.watchlist_service"] = wl_stub

    _stub_canonical(monkeypatch)

    import data.fmp_provider as _mod

    sym_rows = [_make_fmp_item("NVDA", "NVIDIA Corporation", "NASDAQ")]

    async def _fake_gather(*coros, return_exceptions=False):
        return [_FakeResponse(200, sym_rows), _FakeResponse(200, [])]

    monkeypatch.setattr(asyncio, "gather", _fake_gather)

    provider = _mod.FMPProvider("test-key")
    results = await provider.search_securities("NVDA", limit=10)

    assert len(results) >= 1
    assert mutation_called == [], f"Unexpected mutation calls: {mutation_called}"


# ═════════════════════════════════════════════════════════════════════════════
# 10-12. Endpoint-level HTTP behavior using a minimal FastAPI test app.
#
# The full watchlist_router cannot be imported in isolation (too many
# module-level deps).  Instead we inline the exact error-handling logic
# from security_search_endpoint to prove the HTTP 503 / 200 distinction.
# The logic under test is the try/except FMPSearchProviderError block.
# ═════════════════════════════════════════════════════════════════════════════

def _make_test_app():
    """
    Return a minimal FastAPI app containing only the security-search route
    logic (error-handling path) that mirrors security_search_endpoint.
    This avoids importing the full watchlist_router (too many module-level deps).
    """
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse

    app = FastAPI()

    @app.get("/security-search")
    async def _endpoint(q: str = "", limit: int = 25):
        q = q.strip()
        if len(q) < 1:
            return {"query": q, "results": [], "count": 0, "error": "query_too_short"}
        _effective_limit = min(limit, 50)
        try:
            from data.fmp_provider import FMPProvider, FMPSearchProviderError
            provider = FMPProvider("test-key")
            results = await provider.search_securities(q, limit=_effective_limit)
            return {"query": q, "results": results, "count": len(results)}
        except Exception as exc:
            from data.fmp_provider import FMPSearchProviderError as _PE
            if isinstance(exc, _PE):
                return JSONResponse(
                    status_code=503,
                    content={"query": q, "results": [], "count": 0,
                             "error": "provider_error"},
                )
            return {"query": q, "results": [], "count": 0, "error": "provider_error"}

    return app


@pytest.mark.asyncio
async def test_endpoint_503_on_total_failure(monkeypatch):
    """
    When search_securities raises FMPSearchProviderError, the route must
    return HTTP 503, not 200 with silent empty results.
    """
    from httpx import AsyncClient, ASGITransport
    import data.fmp_provider as _mod
    from data.fmp_provider import FMPSearchProviderError

    async def _fail(self, query, limit=25):
        raise FMPSearchProviderError("all search endpoints failed")

    monkeypatch.setattr(_mod.FMPProvider, "search_securities", _fail)

    app = _make_test_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get("/security-search?q=NVDA")

    assert resp.status_code == 503, f"Expected 503, got {resp.status_code}: {resp.text}"
    body = resp.json()
    assert body["error"] == "provider_error"
    assert body["results"] == []
    assert body["count"] == 0


@pytest.mark.asyncio
async def test_endpoint_200_on_valid_empty(monkeypatch):
    """
    When search_securities returns [] (genuine zero-match), route returns HTTP 200
    with empty results — NOT an error.
    """
    from httpx import AsyncClient, ASGITransport
    import data.fmp_provider as _mod

    async def _empty(self, query, limit=25):
        return []

    monkeypatch.setattr(_mod.FMPProvider, "search_securities", _empty)

    app = _make_test_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get("/security-search?q=ZZNONEXISTENT")

    assert resp.status_code == 200, f"Valid empty should be 200, got {resp.status_code}"
    body = resp.json()
    assert body["results"] == []
    assert body["count"] == 0
    assert "error" not in body, f"Valid empty must not set error field; got {body}"


@pytest.mark.asyncio
async def test_endpoint_200_with_results(monkeypatch):
    """
    When search_securities returns results, route returns HTTP 200 with them.
    """
    from httpx import AsyncClient, ASGITransport
    import data.fmp_provider as _mod

    _results = [{"canonical_ticker": "NVDA", "company_name": "NVIDIA Corporation"}]

    async def _success(self, query, limit=25):
        return list(_results)

    monkeypatch.setattr(_mod.FMPProvider, "search_securities", _success)

    app = _make_test_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get("/security-search?q=NVDA")

    assert resp.status_code == 200
    body = resp.json()
    assert len(body["results"]) == 1
    assert body["count"] == 1
    assert "error" not in body
