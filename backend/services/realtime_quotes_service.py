"""
Centralized real-time quote service.

Vendor priority for live equity quotes:
    1. Tradier (primary live source — TRADIER_API_KEY)
    2. Public.com (live backup, only if usable equity quote integration exists)
    3. FMP (NON-realtime fallback unless freshness can be proven)
    4. Twelve Data (emergency fallback)
    5. LKG cache (stale fallback up to 24h)

This service is additive. It does NOT replace FMP for fundamentals/historical/
enrichment/news — only for current/live equity quotes.

Public surface:
    async get_realtime_quotes(symbols, *, allow_fallback=True) -> dict[str, RealtimeQuote]

Each RealtimeQuote contains:
    symbol, price, last, bid, ask, open, high, low, close, prev_close,
    change, change_percent, volume, trade_timestamp, quote_timestamp,
    source, is_realtime, is_live_backup, is_stale, staleness_seconds,
    market_session, error
"""
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Any

import httpx


try:
    from data.cache import cache  # shared TTL cache
except Exception:  # pragma: no cover — defensive
    cache = None

try:
    from data.tradier_provider import TradierProvider
except Exception:
    TradierProvider = None  # type: ignore

try:
    from data.public_com_provider import PublicComProvider
except Exception:
    PublicComProvider = None  # type: ignore

try:
    from data.fmp_provider import FMPProvider
except Exception:
    FMPProvider = None  # type: ignore


# ── Source labels ─────────────────────────────────────────────────────────────
SOURCE_TRADIER = "tradier"
SOURCE_PUBLIC = "public_fallback"
SOURCE_FMP = "fmp_fallback"
SOURCE_FMP_OTC = "fmp_otc"   # OTC-canonical symbols — FMP is the sole live provider
SOURCE_TWELVE = "twelvedata_fallback"
SOURCE_LKG = "lkg"
SOURCE_NONE = "none"

# ── Cache key namespace ───────────────────────────────────────────────────────
_LIVE_CACHE_PREFIX   = "realtime_quote:live:"
_LKG_CACHE_PREFIX    = "realtime_quote:lkg:"
# Negative/no-data cache for OTC symbols FMP has no quote for.
# Prevents redundant FMP calls on every frontend poll when a symbol is genuinely
# absent from FMP's coverage.  Only set on a confirmed empty FMP response (not
# on network errors, which may be transient).  Key = canonical OTC:SYMBOL.
_OTC_NODATA_PREFIX   = "realtime_quote:nodata:"

# ── Cache TTLs (seconds) ──────────────────────────────────────────────────────
_TTL_REGULAR  = 15
_TTL_EXTENDED = 30
_TTL_CLOSED   = 300
_TTL_LKG      = 24 * 3600  # 24h LKG retention
# No-data TTL: suppress retries for 5 min (matches _TTL_CLOSED for consistency).
# Short enough that a genuinely newly-listed OTC ticker will be retried within
# the same trading session.
_OTC_NODATA_TTL = 300

# Public.com / vendor concurrency
_TRADIER_CONCURRENCY = 6     # ~6 parallel Tradier batch requests max
_PUBLIC_CONCURRENCY = 2
_FMP_CONCURRENCY = 4
_TWELVE_CONCURRENCY = 2

# Tradier batch chunk size (well under defensive limit)
_TRADIER_CHUNK = 80

# Twelve Data tight rate cap (free tier baseline 8/min)
_TWELVE_RATE_PER_MIN = 8

_TIMEOUT = 10  # seconds


# ── Market session helpers ────────────────────────────────────────────────────

def _market_session_now() -> str:
    """Best-effort US equity market session classification.

    Returns one of: regular, premarket, afterhours, closed.
    Not authoritative — used for cache TTL selection and freshness hinting.
    """
    now = datetime.now(timezone.utc)
    # ET conversion (no DST nuance — close enough for TTL bucketing)
    hour_et = (now.hour - 4) % 24
    minute_et = now.minute
    weekday = now.weekday()
    if weekday >= 5:
        return "closed"
    minutes_et = hour_et * 60 + minute_et
    if 9 * 60 + 30 <= minutes_et < 16 * 60:
        return "regular"
    if 4 * 60 <= minutes_et < 9 * 60 + 30:
        return "premarket"
    if 16 * 60 <= minutes_et < 20 * 60:
        return "afterhours"
    return "closed"


def _ttl_for_session(session: str) -> int:
    if session == "regular":
        return _TTL_REGULAR
    if session in ("premarket", "afterhours"):
        return _TTL_EXTENDED
    return _TTL_CLOSED


# ── Quote dataclass ───────────────────────────────────────────────────────────

@dataclass
class RealtimeQuote:
    symbol: str
    price: float | None = None
    last: float | None = None
    bid: float | None = None
    ask: float | None = None
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    prev_close: float | None = None
    change: float | None = None
    change_percent: float | None = None
    volume: int | None = None
    trade_timestamp: int | None = None  # epoch seconds
    quote_timestamp: int | None = None  # epoch seconds (server fetch time)
    source: str = SOURCE_NONE
    is_realtime: bool = False
    is_live_backup: bool = False
    is_stale: bool = False
    staleness_seconds: int | None = None
    market_session: str = "unknown"
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_float(v: Any) -> float | None:
    try:
        if v in (None, "", "-"):
            return None
        return float(v)
    except Exception:
        return None


def _safe_int(v: Any) -> int | None:
    try:
        if v in (None, "", "-"):
            return None
        return int(float(v))
    except Exception:
        return None


def _normalize_symbols(symbols: list[str]) -> list[str]:
    seen = set()
    out: list[str] = []
    for s in symbols or []:
        if not isinstance(s, str):
            continue
        cleaned = s.strip().upper()
        if not cleaned or cleaned in seen:
            continue
        # OTC canonical form (OTC: + up to 12 chars) reaches max 16 chars;
        # all other symbols are capped at 12.
        _max_len = 16 if cleaned.startswith("OTC:") else 12
        if len(cleaned) > _max_len:
            continue
        seen.add(cleaned)
        out.append(cleaned)
    return out


def _chunk(seq: list[str], size: int) -> list[list[str]]:
    return [seq[i : i + size] for i in range(0, len(seq), size)]


# ── Twelve Data simple async quote client ─────────────────────────────────────

class _TwelveQuoteClient:
    """Tiny async TwelveData /quote client (no historical bars)."""

    BASE_URL = "https://api.twelvedata.com"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._lock = asyncio.Lock()
        self._call_times: list[float] = []

    async def _check_rate(self) -> bool:
        async with self._lock:
            now = time.time()
            self._call_times = [t for t in self._call_times if now - t < 60]
            if len(self._call_times) >= _TWELVE_RATE_PER_MIN:
                return False
            self._call_times.append(now)
            return True

    async def get_quote(self, symbol: str) -> dict | None:
        if not await self._check_rate():
            print(f"[REALTIME] Twelve rate-limited, skipping {symbol}")
            return None
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.get(
                    f"{self.BASE_URL}/quote",
                    params={"symbol": symbol, "apikey": self.api_key},
                )
            if resp.status_code != 200:
                print(f"[REALTIME] Twelve {symbol} HTTP {resp.status_code}")
                return None
            data = resp.json()
            if isinstance(data, dict) and data.get("status") == "error":
                print(f"[REALTIME] Twelve {symbol} error: {data.get('message')}")
                return None
            return data if isinstance(data, dict) else None
        except Exception as e:
            print(f"[REALTIME] Twelve {symbol} exception: {e}")
            return None


# ── Service ───────────────────────────────────────────────────────────────────

class RealtimeQuotesService:
    """Vendor-prioritized real-time equity quote service with LKG fallback."""

    def __init__(
        self,
        tradier_provider=None,
        public_provider=None,
        fmp_provider=None,
        twelve_api_key: str | None = None,
    ):
        self.tradier = tradier_provider
        self.public = public_provider
        self.fmp = fmp_provider
        self.twelve = (
            _TwelveQuoteClient(twelve_api_key) if twelve_api_key else None
        )

        self._tradier_sem = asyncio.Semaphore(_TRADIER_CONCURRENCY)
        self._public_sem = asyncio.Semaphore(_PUBLIC_CONCURRENCY)
        self._fmp_sem = asyncio.Semaphore(_FMP_CONCURRENCY)
        self._twelve_sem = asyncio.Semaphore(_TWELVE_CONCURRENCY)

        # Public.com equity quote support is gated behind explicit confirmation
        # — only enabled if PublicComProvider exposes a get_quotes method we can
        # use against EQUITY instruments. The existing provider exposes
        # get_quotes(symbols, instrument_type) — we treat EQUITY as supported
        # but mark responses as live_backup with cautious freshness flags.
        self._public_equity_supported = bool(
            self.public and hasattr(self.public, "get_quotes")
        )

    # ── Public API ───────────────────────────────────────────────────────────
    async def get_realtime_quotes(
        self,
        symbols: list[str],
        *,
        allow_fallback: bool = True,
    ) -> dict[str, RealtimeQuote]:
        normalized = _normalize_symbols(symbols)
        result: dict[str, RealtimeQuote] = {}
        if not normalized:
            return result

        session = _market_session_now()
        cache_ttl = _ttl_for_session(session)

        # 1. Cache lookup
        remaining: list[str] = []
        for sym in normalized:
            cached = self._cache_get_live(sym)
            if cached is not None:
                cached.market_session = session
                result[sym] = cached
            else:
                remaining.append(sym)
        served_from_cache = len(normalized) - len(remaining)

        # ── Phase 4A: sort cache-miss symbols by active demand priority ───────
        # Highest-priority symbols enter the first Tradier batch chunk, which is
        # processed before lower-priority chunks (see _tradier_batch).
        if remaining:
            try:
                import data.quote_demand_registry as _qdr
                remaining.sort(key=lambda s: -_qdr.get_priority(s, default=0.0))
            except Exception:
                pass

        print(
            f"[REALTIME] requested={len(normalized)} cache_hits={served_from_cache} "
            f"to_fetch={len(remaining)} session={session}"
        )

        if not remaining:
            return result

        # ── Partition cache misses: OTC (FMP-only) vs. U.S. (Tradier chain) ──
        # OTC:-prefixed symbols must never enter the Tradier / Public / Twelve
        # path. split_otc_us returns ([OTC:…], [plain US]) and silently discards
        # any other colon format — those were already rejected by the router.
        try:
            from services.otc_service import split_otc_us as _split_otc_us
            _otc_remaining, _us_remaining = _split_otc_us(remaining)
        except Exception as _otc_split_err:
            print(f"[REALTIME_OTC] split error (fallback: treat all as US): {_otc_split_err}")
            _otc_remaining, _us_remaining = [], list(remaining)

        # ── OTC: FMP-only live quote branch ──────────────────────────────────
        if _otc_remaining:
            _otc_quotes = await self._fmp_otc_batch(_otc_remaining, session)
            for _canonical in _otc_remaining:
                _oq = _otc_quotes.get(_canonical)
                if _oq and _oq.price is not None:
                    self._cache_set_live(_canonical, _oq, cache_ttl)
                    self._cache_set_lkg(_canonical, _oq)
                    result[_canonical] = _oq
                else:
                    result[_canonical] = self._lkg_or_empty(
                        _canonical, session, "fmp_otc_no_data"
                    )

        if not _us_remaining:
            return result

        # 2. Tradier primary
        tradier_failures: list[str] = []
        if self.tradier:
            t_quotes = await self._tradier_batch(_us_remaining, session)
            for sym in _us_remaining:
                q = t_quotes.get(sym)
                if q and q.price is not None:
                    self._cache_set_live(sym, q, cache_ttl)
                    self._cache_set_lkg(sym, q)
                    result[sym] = q
                else:
                    tradier_failures.append(sym)
            # ── Phase 4A: record refresh diagnostics ─────────────────────────
            try:
                import data.quote_demand_registry as _qdr
                _live = sum(1 for s in _us_remaining if s in result)
                _qdr.record_refresh_stats(
                    queue_depth=len(_us_remaining),
                    order_sample=_us_remaining[:10],
                    cache_hits=served_from_cache,
                    live_fetches=_live,
                )
            except Exception:
                pass
        else:
            tradier_failures = list(_us_remaining)
            print("[REALTIME] Tradier provider not configured")

        if not allow_fallback or not tradier_failures:
            for sym in tradier_failures:
                result[sym] = self._lkg_or_empty(sym, session, "tradier_failed")
            return result

        # 3. Public.com fallback (only failures)
        public_failures: list[str] = list(tradier_failures)
        if self._public_equity_supported and tradier_failures:
            p_quotes = await self._public_batch(tradier_failures, session)
            public_failures = []
            for sym in tradier_failures:
                q = p_quotes.get(sym)
                if q and q.price is not None:
                    self._cache_set_live(sym, q, cache_ttl)
                    self._cache_set_lkg(sym, q)
                    result[sym] = q
                else:
                    public_failures.append(sym)
        else:
            print("[REALTIME] Public.com equity quote integration not available — skipping")

        if not public_failures:
            return result

        # 4. FMP fallback (NON-realtime unless we can prove freshness)
        fmp_failures: list[str] = list(public_failures)
        if self.fmp and public_failures:
            f_quotes = await self._fmp_batch(public_failures, session)
            fmp_failures = []
            for sym in public_failures:
                q = f_quotes.get(sym)
                if q and q.price is not None:
                    # FMP cached at shorter TTL since not realtime
                    self._cache_set_live(sym, q, min(cache_ttl, 60))
                    self._cache_set_lkg(sym, q)
                    result[sym] = q
                else:
                    fmp_failures.append(sym)

        if not fmp_failures:
            return result

        # 5. Twelve Data emergency
        twelve_failures: list[str] = list(fmp_failures)
        if self.twelve and fmp_failures:
            tw_quotes = await self._twelve_batch(fmp_failures, session)
            twelve_failures = []
            for sym in fmp_failures:
                q = tw_quotes.get(sym)
                if q and q.price is not None:
                    self._cache_set_live(sym, q, min(cache_ttl, 60))
                    self._cache_set_lkg(sym, q)
                    result[sym] = q
                else:
                    twelve_failures.append(sym)

        # 6. Final: LKG or null with error
        for sym in twelve_failures:
            result[sym] = self._lkg_or_empty(sym, session, "all_vendors_failed")

        return result

    # ── OTC FMP live-quote branch ────────────────────────────────────────────
    # Each OTC symbol is fetched individually via fmp_governor-controlled calls.
    # FMP Starter does not support batch quote; individual stable/quote is used.
    # Canonical identity: application/cache/output → OTC:BESIY; FMP HTTP → BESIY.
    # These symbols NEVER enter the Tradier / Public / Twelve path.

    async def _fmp_otc_batch(
        self,
        otc_canonical: list[str],
        session: str,
    ) -> dict[str, "RealtimeQuote"]:
        """Fetch live quotes for OTC:-prefixed symbols using FMP as the sole provider.

        Returns a dict keyed by canonical OTC:BESIY symbols.
        Symbols that FMP returns no price for are absent from the result (caller
        falls back to LKG).  No Tradier / Public / Twelve calls are made here.
        """
        import time as _t
        from services.fmp_governor import fmp_governor

        try:
            from services.otc_service import otc_to_fmp as _otc_to_fmp
        except Exception:
            def _otc_to_fmp(sym: str) -> str:  # type: ignore[misc]
                return sym.split(":", 1)[-1] if ":" in sym else sym

        out: dict[str, RealtimeQuote] = {}
        if not self.fmp or not otc_canonical:
            return out

        print(f"[REALTIME_OTC] fetching {len(otc_canonical)} OTC symbols via FMP")

        for canonical in otc_canonical:
            bare = _otc_to_fmp(canonical)  # "BESIY" (the FMP HTTP parameter)

            # ── Negative-cache check ─────────────────────────────────────────
            # If a previous call confirmed FMP has no data for this symbol,
            # skip the FMP call entirely until the nodata TTL expires.
            # This prevents every frontend poll from consuming a governor slot
            # on a symbol FMP genuinely does not cover.
            # Transient errors (network/timeout) do NOT set the nodata entry,
            # so they are always retried on the next poll.
            if cache is not None:
                try:
                    if cache.get(_OTC_NODATA_PREFIX + canonical):
                        continue   # no-data still fresh — skip FMP call
                except Exception:
                    pass

            ok = await fmp_governor.acquire("realtime_otc_quotes")
            if not ok:
                print(f"[REALTIME_OTC] fmp_governor denied {canonical} — skipping")
                continue

            try:
                q = await asyncio.wait_for(
                    self.fmp.get_quote(bare), timeout=_TIMEOUT
                )
                fmp_governor.record_call()
            except Exception as exc:
                fmp_governor.record_call()
                print(f"[REALTIME_OTC] FMP error for {canonical} ({bare}): {exc}")
                continue  # transient error — do NOT set nodata cache

            if not q:
                # FMP returned an empty response — set nodata cache so repeated
                # polls don't retry within _OTC_NODATA_TTL seconds.
                if cache is not None:
                    try:
                        cache.set(_OTC_NODATA_PREFIX + canonical, True, _OTC_NODATA_TTL)
                    except Exception:
                        pass
                print(f"[REALTIME_OTC] no data from FMP for {canonical} — "
                      f"suppressing for {_OTC_NODATA_TTL}s")
                continue

            price = q.get("price")
            if price is None:
                # FMP returned a response but price field is absent/null.
                if cache is not None:
                    try:
                        cache.set(_OTC_NODATA_PREFIX + canonical, True, _OTC_NODATA_TTL)
                    except Exception:
                        pass
                print(f"[REALTIME_OTC] null price from FMP for {canonical} — "
                      f"suppressing for {_OTC_NODATA_TTL}s")
                continue

            # Derive float/int or None — never synthesize 0 for missing fields.
            price_f    = _safe_float(price)
            change_f   = _safe_float(q.get("change"))
            chg_pct_f  = _safe_float(q.get("changesPercentage"))
            high_f     = _safe_float(q.get("dayHigh"))
            low_f      = _safe_float(q.get("dayLow"))
            prev_f     = _safe_float(q.get("previousClose"))
            volume_i   = _safe_int(q.get("volume"))
            now_ts     = int(_t.time())

            out[canonical] = RealtimeQuote(
                symbol=canonical,        # OTC:BESIY — canonical retained
                price=price_f,
                last=price_f,
                bid=None,                # FMP stable/quote does not return bid/ask
                ask=None,
                open=None,
                high=high_f,
                low=low_f,
                close=None,
                prev_close=prev_f,
                change=change_f,
                change_percent=chg_pct_f,
                volume=volume_i,
                trade_timestamp=None,
                quote_timestamp=now_ts,
                source=SOURCE_FMP_OTC,
                is_realtime=False,       # FMP Starter is delayed/non-realtime
                is_live_backup=False,
                is_stale=False,
                staleness_seconds=0,
                market_session=session,
            )

        hits = len(out)
        misses = len(otc_canonical) - hits
        print(f"[REALTIME_OTC] done — hits={hits} misses={misses}")
        return out

    # ── Tradier batch ────────────────────────────────────────────────────────

    async def _tradier_batch(
        self, symbols: list[str], session: str
    ) -> dict[str, RealtimeQuote]:
        out: dict[str, RealtimeQuote] = {}
        if not self.tradier or not symbols:
            return out

        chunks = _chunk(symbols, _TRADIER_CHUNK)
        print(
            f"[REALTIME] Tradier batch_size={len(symbols)} chunks={len(chunks)}"
        )

        async def _fetch(chunk: list[str]) -> list[dict]:
            from data.tradier_budget import lane as _rqs_lane
            with _rqs_lane("quotes"):
                async with self._tradier_sem:
                    try:
                        return await asyncio.wait_for(
                            self.tradier.get_quotes(chunk), timeout=_TIMEOUT
                        )
                    except asyncio.TimeoutError:
                        print(f"[REALTIME] Tradier timeout for {len(chunk)} symbols")
                        return []
                    except Exception as e:
                        print(f"[REALTIME] Tradier error: {e}")
                        return []

        # ── Phase 4A: priority-first batching ────────────────────────────────
        # symbols are pre-sorted by demand priority (highest first), so chunks[0]
        # contains the highest-priority symbols.  Await it alone so it consumes
        # the quote-lane budget before lower-priority chunks can race ahead.
        # Chunks 1+ are gathered concurrently (same lower-priority tier — racing
        # within that tier is fine).
        if not chunks:
            results: list[list[dict]] = []
        elif len(chunks) == 1:
            results = [await _fetch(chunks[0])]
        else:
            _first = await _fetch(chunks[0])
            _rest  = await asyncio.gather(*[_fetch(c) for c in chunks[1:]])
            results = [_first, *_rest]
        now_ts = int(time.time())
        for chunk_quotes in results:
            for q in chunk_quotes or []:
                sym = (q.get("symbol") or "").upper()
                if not sym:
                    continue
                last = _safe_float(q.get("last"))
                bid = _safe_float(q.get("bid"))
                ask = _safe_float(q.get("ask"))
                close = _safe_float(q.get("close"))
                prev_close = _safe_float(q.get("prevclose"))
                change = _safe_float(q.get("change"))
                change_pct = _safe_float(q.get("change_percentage"))
                price = last if last is not None else (bid if bid is not None else ask)
                if price is None and close is not None:
                    price = close

                # Tradier doesn't always include trade_date as epoch — best-effort
                trade_ts = None
                td_raw = q.get("trade_date") or q.get("last_trade_date")
                if isinstance(td_raw, (int, float)) and td_raw > 0:
                    # Tradier sometimes returns ms epochs
                    trade_ts = int(td_raw / 1000) if td_raw > 1e12 else int(td_raw)

                out[sym] = RealtimeQuote(
                    symbol=sym,
                    price=price,
                    last=last,
                    bid=bid,
                    ask=ask,
                    open=_safe_float(q.get("open")),
                    high=_safe_float(q.get("high")),
                    low=_safe_float(q.get("low")),
                    close=close,
                    prev_close=prev_close,
                    change=change,
                    change_percent=change_pct,
                    volume=_safe_int(q.get("volume")),
                    trade_timestamp=trade_ts,
                    quote_timestamp=now_ts,
                    source=SOURCE_TRADIER,
                    is_realtime=True,
                    is_live_backup=False,
                    is_stale=False,
                    staleness_seconds=0,
                    market_session=session,
                )
        if not out:
            print(f"[REALTIME] Tradier returned no quotes for batch_size={len(symbols)}")
        return out

    # ── Public.com batch ─────────────────────────────────────────────────────

    async def _public_batch(
        self, symbols: list[str], session: str
    ) -> dict[str, RealtimeQuote]:
        out: dict[str, RealtimeQuote] = {}
        if not self._public_equity_supported or not symbols:
            return out
        print(f"[REALTIME] Public.com fallback for {len(symbols)} symbols")

        # Public.com get_quotes has 10 req/sec; respect it via semaphore
        async def _fetch(chunk: list[str]) -> list[dict]:
            async with self._public_sem:
                try:
                    return await asyncio.wait_for(
                        self.public.get_quotes(chunk, "EQUITY"), timeout=_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    print(f"[REALTIME] Public.com timeout ({len(chunk)} symbols)")
                    return []
                except Exception as e:
                    print(f"[REALTIME] Public.com error: {e}")
                    return []

        chunks = _chunk(symbols, 25)
        results = await asyncio.gather(*[_fetch(c) for c in chunks])
        now_ts = int(time.time())
        for chunk_quotes in results:
            for q in chunk_quotes or []:
                instr = q.get("instrument") or {}
                sym = (instr.get("symbol") or q.get("symbol") or "").upper()
                if not sym:
                    continue
                last = _safe_float(q.get("last"))
                bid = _safe_float(q.get("bid"))
                ask = _safe_float(q.get("ask"))
                price = last if last is not None else (bid if bid is not None else ask)
                if price is None:
                    continue

                # Public.com timestamp reliability: unknown — mark cautiously.
                # Treat as live_backup; freshness only true if last seen recently.
                ts_raw = q.get("timestamp") or q.get("lastUpdated")
                trade_ts = None
                if isinstance(ts_raw, (int, float)):
                    trade_ts = int(ts_raw / 1000) if ts_raw > 1e12 else int(ts_raw)

                staleness = None
                is_stale = False
                if trade_ts:
                    staleness = max(0, now_ts - trade_ts)
                    is_stale = staleness > 120  # >2min => stale
                else:
                    # Timestamp unreliable — flag cautiously stale unless quote
                    # is clearly fresh against bid/ask presence
                    is_stale = bid is None and ask is None

                out[sym] = RealtimeQuote(
                    symbol=sym,
                    price=price,
                    last=last,
                    bid=bid,
                    ask=ask,
                    open=_safe_float(q.get("open")),
                    high=_safe_float(q.get("high")),
                    low=_safe_float(q.get("low")),
                    close=_safe_float(q.get("close")),
                    prev_close=_safe_float(q.get("previousClose")),
                    change=_safe_float(q.get("change")),
                    change_percent=_safe_float(q.get("changePercent")),
                    volume=_safe_int(q.get("volume")),
                    trade_timestamp=trade_ts,
                    quote_timestamp=now_ts,
                    source=SOURCE_PUBLIC,
                    is_realtime=not is_stale,
                    is_live_backup=True,
                    is_stale=is_stale,
                    staleness_seconds=staleness,
                    market_session=session,
                )
                if trade_ts is None:
                    print(f"[REALTIME] Public.com {sym} missing timestamp")
        return out

    # ── FMP batch ────────────────────────────────────────────────────────────

    async def _fmp_batch(
        self, symbols: list[str], session: str
    ) -> dict[str, RealtimeQuote]:
        out: dict[str, RealtimeQuote] = {}
        if not self.fmp or not symbols:
            return out
        print(f"[REALTIME] FMP fallback for {len(symbols)} symbols")

        async def _fetch_one(sym: str):
            async with self._fmp_sem:
                try:
                    return sym, await asyncio.wait_for(
                        self.fmp.get_quote(sym), timeout=_TIMEOUT
                    )
                except Exception as e:
                    print(f"[REALTIME] FMP {sym} error: {e}")
                    return sym, None

        results = await asyncio.gather(*[_fetch_one(s) for s in symbols])
        now_ts = int(time.time())
        for sym, q in results:
            if not q:
                continue
            price = _safe_float(q.get("price"))
            if price is None:
                continue
            prev_close = _safe_float(q.get("previousClose"))
            change = _safe_float(q.get("change"))
            change_pct = _safe_float(q.get("changesPercentage"))
            # FMP starter-tier quote endpoint is NOT realtime — mark accordingly
            out[sym] = RealtimeQuote(
                symbol=sym,
                price=price,
                last=price,
                bid=None,
                ask=None,
                open=None,
                high=_safe_float(q.get("dayHigh")),
                low=_safe_float(q.get("dayLow")),
                close=None,
                prev_close=prev_close,
                change=change,
                change_percent=change_pct,
                volume=_safe_int(q.get("volume")),
                trade_timestamp=None,
                quote_timestamp=now_ts,
                source=SOURCE_FMP,
                is_realtime=False,
                is_live_backup=False,
                is_stale=True,
                staleness_seconds=None,
                market_session=session,
            )
        return out

    # ── Twelve Data batch ────────────────────────────────────────────────────

    async def _twelve_batch(
        self, symbols: list[str], session: str
    ) -> dict[str, RealtimeQuote]:
        out: dict[str, RealtimeQuote] = {}
        if not self.twelve or not symbols:
            return out
        print(f"[REALTIME] Twelve Data emergency fallback for {len(symbols)} symbols")

        async def _fetch_one(sym: str):
            async with self._twelve_sem:
                return sym, await self.twelve.get_quote(sym)

        results = await asyncio.gather(*[_fetch_one(s) for s in symbols])
        now_ts = int(time.time())
        for sym, q in results:
            if not q:
                continue
            price = _safe_float(q.get("close")) or _safe_float(q.get("price"))
            if price is None:
                continue
            ts_raw = q.get("timestamp")
            trade_ts = int(ts_raw) if isinstance(ts_raw, (int, float)) else None
            staleness = (now_ts - trade_ts) if trade_ts else None
            is_stale = (staleness is not None and staleness > 300) or staleness is None
            out[sym] = RealtimeQuote(
                symbol=sym,
                price=price,
                last=price,
                bid=None,
                ask=None,
                open=_safe_float(q.get("open")),
                high=_safe_float(q.get("high")),
                low=_safe_float(q.get("low")),
                close=_safe_float(q.get("close")),
                prev_close=_safe_float(q.get("previous_close")),
                change=_safe_float(q.get("change")),
                change_percent=_safe_float(q.get("percent_change")),
                volume=_safe_int(q.get("volume")),
                trade_timestamp=trade_ts,
                quote_timestamp=now_ts,
                source=SOURCE_TWELVE,
                is_realtime=False,
                is_live_backup=False,
                is_stale=is_stale,
                staleness_seconds=staleness,
                market_session=session,
            )
        return out

    # ── Cache helpers ────────────────────────────────────────────────────────

    def _cache_get_live(self, symbol: str) -> RealtimeQuote | None:
        if cache is None:
            return None
        cached = cache.get(_LIVE_CACHE_PREFIX + symbol)
        if not cached:
            return None
        try:
            return RealtimeQuote(**cached) if isinstance(cached, dict) else cached
        except Exception:
            return None

    def _cache_set_live(self, symbol: str, quote: RealtimeQuote, ttl: int) -> None:
        if cache is None:
            return
        try:
            cache.set(_LIVE_CACHE_PREFIX + symbol, quote.to_dict(), ttl)
        except Exception:
            pass

    def _cache_set_lkg(self, symbol: str, quote: RealtimeQuote) -> None:
        if cache is None or quote.price is None:
            return
        try:
            cache.set(_LKG_CACHE_PREFIX + symbol, quote.to_dict(), _TTL_LKG)
        except Exception:
            pass

    def _cache_get_lkg(self, symbol: str) -> RealtimeQuote | None:
        if cache is None:
            return None
        cached = cache.get(_LKG_CACHE_PREFIX + symbol)
        if not cached:
            return None
        try:
            return RealtimeQuote(**cached) if isinstance(cached, dict) else None
        except Exception:
            return None

    def _lkg_or_empty(
        self, symbol: str, session: str, error: str
    ) -> RealtimeQuote:
        lkg = self._cache_get_lkg(symbol)
        now_ts = int(time.time())
        if lkg is not None and lkg.price is not None:
            staleness = (
                now_ts - lkg.quote_timestamp if lkg.quote_timestamp else None
            )
            print(
                f"[REALTIME] LKG used for {symbol} staleness={staleness}s reason={error}"
            )
            return RealtimeQuote(
                symbol=symbol,
                price=lkg.price,
                last=lkg.last,
                bid=lkg.bid,
                ask=lkg.ask,
                open=lkg.open,
                high=lkg.high,
                low=lkg.low,
                close=lkg.close,
                prev_close=lkg.prev_close,
                change=lkg.change,
                change_percent=lkg.change_percent,
                volume=lkg.volume,
                trade_timestamp=lkg.trade_timestamp,
                quote_timestamp=lkg.quote_timestamp,
                source=SOURCE_LKG,
                is_realtime=False,
                is_live_backup=False,
                is_stale=True,
                staleness_seconds=staleness,
                market_session=session,
                error=error,
            )
        print(f"[REALTIME] {symbol} no quote from any vendor reason={error}")
        return RealtimeQuote(
            symbol=symbol,
            source=SOURCE_NONE,
            is_realtime=False,
            is_live_backup=False,
            is_stale=True,
            market_session=session,
            quote_timestamp=now_ts,
            error=error,
        )


# ── Module-level singleton ────────────────────────────────────────────────────

_service_instance: RealtimeQuotesService | None = None
_service_lock = asyncio.Lock()


async def get_service() -> RealtimeQuotesService:
    """Lazy singleton — pulls API keys from env / config and constructs providers."""
    global _service_instance
    if _service_instance is not None:
        return _service_instance
    async with _service_lock:
        if _service_instance is not None:
            return _service_instance

        # Lazy import to avoid circular imports at module import time
        try:
            from config import (
                TRADIER_API_KEY,
                TRADIER_SANDBOX,
                PUBLIC_COM_API_KEY,
                FMP_API_KEY,
                TWELVEDATA_API_KEY,
            )
        except Exception:
            TRADIER_API_KEY = os.getenv("TRADIER_API_KEY")
            TRADIER_SANDBOX = os.getenv("TRADIER_SANDBOX", "").lower() in ("true", "1", "yes")
            PUBLIC_COM_API_KEY = os.getenv("PUBLIC_COM_API_KEY")
            FMP_API_KEY = os.getenv("FMP_API_KEY")
            TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY")

        tradier = None
        if TRADIER_API_KEY and TradierProvider is not None:
            try:
                tradier = TradierProvider(TRADIER_API_KEY, sandbox=TRADIER_SANDBOX)
            except Exception as e:
                print(f"[REALTIME] Tradier init failed: {e}")

        public = None
        if PUBLIC_COM_API_KEY and PublicComProvider is not None:
            try:
                public = PublicComProvider(PUBLIC_COM_API_KEY)
            except Exception as e:
                print(f"[REALTIME] Public.com init failed: {e}")

        fmp = None
        if FMP_API_KEY and FMPProvider is not None:
            try:
                fmp = FMPProvider(FMP_API_KEY)
            except Exception as e:
                print(f"[REALTIME] FMP init failed: {e}")

        _service_instance = RealtimeQuotesService(
            tradier_provider=tradier,
            public_provider=public,
            fmp_provider=fmp,
            twelve_api_key=TWELVEDATA_API_KEY,
        )
        return _service_instance


async def get_realtime_quotes(
    symbols: list[str], *, allow_fallback: bool = True
) -> dict[str, RealtimeQuote]:
    """Module-level convenience used by callers wanting normalized quotes."""
    svc = await get_service()
    return await svc.get_realtime_quotes(symbols, allow_fallback=allow_fallback)


# ── Helper for consumers wiring freshness metadata onto existing payloads ────

def freshness_metadata(quote: RealtimeQuote | dict | None) -> dict[str, Any]:
    """Return the standardized freshness metadata block consumers should
    attach to their current-price payloads.
    """
    if quote is None:
        return {
            "price_source": SOURCE_NONE,
            "price_is_realtime": False,
            "price_is_live_backup": False,
            "price_is_stale": True,
            "price_updated_at": None,
            "quote_timestamp": None,
            "staleness_seconds": None,
        }
    if isinstance(quote, RealtimeQuote):
        d = quote.to_dict()
    else:
        d = quote
    return {
        "price_source": d.get("source"),
        "price_is_realtime": bool(d.get("is_realtime")),
        "price_is_live_backup": bool(d.get("is_live_backup")),
        "price_is_stale": bool(d.get("is_stale")),
        "price_updated_at": d.get("quote_timestamp"),
        "quote_timestamp": d.get("quote_timestamp"),
        "staleness_seconds": d.get("staleness_seconds"),
    }
