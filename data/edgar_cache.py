"""
EDGAR Background Cache — persistent disk cache for SEC data.

Architecture:
  Tier 1 (nightly midnight CST): Financials, institutional data — changes quarterly.
  Tier 2 (every 2h market hours): Recent filings, 8-K catalysts — material events.
  Tier 3 (live, 5-15 min TTL): Form 4 insider trades, earnings-day filings — real-time signals.

The ticker universe auto-expands: every queried ticker gets added to the nightly refresh list.
Data persists to disk (JSON) so it survives Replit restarts.
"""
import json
import os
import time
import asyncio
from datetime import datetime, timezone, timedelta

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop


CACHE_DIR = os.path.join(os.path.dirname(__file__), "edgar_disk_cache")
UNIVERSE_FILE = os.path.join(CACHE_DIR, "ticker_universe.json")
FINANCIALS_FILE = os.path.join(CACHE_DIR, "financials.json")
FILINGS_FILE = os.path.join(CACHE_DIR, "filings.json")
INSIDER_FILE = os.path.join(CACHE_DIR, "insider.json")
CATALYSTS_FILE = os.path.join(CACHE_DIR, "catalysts.json")

# Max staleness before data is considered expired
FINANCIALS_MAX_AGE = 86400      # 24 hours (changes quarterly, nightly refresh is fine)
FILINGS_MAX_AGE = 7200          # 2 hours
CATALYSTS_MAX_AGE = 7200        # 2 hours
INSIDER_MAX_AGE = 300           # 5 minutes — real-time critical

# CST = UTC-6
CST = timezone(timedelta(hours=-6))


@traceable(name="edgar_cache.ensure_dir")
def _ensure_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


@traceable(name="edgar_cache.load_json")
def _load_json(path: str) -> dict:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


@traceable(name="edgar_cache.save_json")
def _save_json(path: str, data: dict):
    _ensure_dir()
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, default=str)
    os.replace(tmp, path)


# ── Ticker Universe ──────────────────────────────────────────

@traceable(name="edgar_cache.get_universe")
def get_universe() -> list[str]:
    """Get the list of tickers to refresh nightly."""
    data = _load_json(UNIVERSE_FILE)
    return list(data.get("tickers", {}).keys())


@traceable(name="edgar_cache.add_to_universe")
def add_to_universe(tickers: list[str]):
    """Add tickers to the universe (called on every user query)."""
    if not tickers:
        return
    data = _load_json(UNIVERSE_FILE)
    universe = data.get("tickers", {})
    now = time.time()
    for t in tickers:
        t = t.upper().strip()
        if t and len(t) <= 10:
            universe[t] = now  # timestamp of last query
    # Prune tickers not queried in 30 days
    cutoff = now - (30 * 86400)
    universe = {k: v for k, v in universe.items() if v > cutoff}
    _save_json(UNIVERSE_FILE, {"tickers": universe, "updated_at": now})


# ── Disk Cache Read/Write ────────────────────────────────────

@traceable(name="edgar_cache.get_cached")
def get_cached(cache_file: str, ticker: str, max_age: float) -> dict | None:
    """Read a ticker's cached data if it exists and isn't stale."""
    data = _load_json(cache_file)
    entry = data.get(ticker.upper())
    if not entry:
        return None
    cached_at = entry.get("_cached_at", 0)
    if time.time() - cached_at > max_age:
        return None
    return entry


@traceable(name="edgar_cache.set_cached")
def set_cached(cache_file: str, ticker: str, value: dict):
    """Write a ticker's data to the disk cache."""
    data = _load_json(cache_file)
    value["_cached_at"] = time.time()
    data[ticker.upper()] = value
    _save_json(cache_file, data)


@traceable(name="edgar_cache.bulk_set_cached")
def bulk_set_cached(cache_file: str, entries: dict[str, dict]):
    """Write multiple tickers at once (used by background jobs)."""
    data = _load_json(cache_file)
    now = time.time()
    for ticker, value in entries.items():
        value["_cached_at"] = now
        data[ticker.upper()] = value
    _save_json(cache_file, data)


# ── Public Cache Accessors ───────────────────────────────────

@traceable(name="edgar_cache.get_financials")
def get_financials(ticker: str) -> dict | None:
    return get_cached(FINANCIALS_FILE, ticker, FINANCIALS_MAX_AGE)


@traceable(name="edgar_cache.get_filings")
def get_filings(ticker: str) -> list | None:
    entry = get_cached(FILINGS_FILE, ticker, FILINGS_MAX_AGE)
    if entry is None:
        return None
    return entry.get("filings", [])


@traceable(name="edgar_cache.get_catalysts")
def get_catalysts(ticker: str) -> list | None:
    entry = get_cached(CATALYSTS_FILE, ticker, CATALYSTS_MAX_AGE)
    if entry is None:
        return None
    return entry.get("catalysts", [])


@traceable(name="edgar_cache.get_insider")
def get_insider(ticker: str) -> dict | None:
    return get_cached(INSIDER_FILE, ticker, INSIDER_MAX_AGE)


# ── Background Refresh Job ───────────────────────────────────

@traceable(name="edgar_cache.refresh_universe")
async def refresh_universe(sec_edgar, mode: str = "full"):
    """
    Refresh EDGAR data for all tickers in the universe.

    mode="full": Financials + filings + catalysts (nightly)
    mode="filings": Filings + catalysts only (intraday refresh)
    """
    from data.sec_edgar_provider import EdgarBudget

    tickers = get_universe()
    if not tickers:
        print("[EDGAR_CACHE] No tickers in universe, skipping refresh")
        return

    print(f"[EDGAR_CACHE] Starting {mode} refresh for {len(tickers)} tickers...")
    start = time.time()
    financials_batch = {}
    filings_batch = {}
    catalysts_batch = {}
    insider_batch = {}
    errors = 0

    for ticker in tickers:
        cik = await sec_edgar.resolve_cik(ticker)
        if not cik:
            continue

        # Use a generous budget per ticker — background job, no user waiting
        budget = EdgarBudget(max_requests=5)

        try:
            if mode == "full":
                # Tier 1: Financials (keyed by CIK to match provider lookups)
                financials = await sec_edgar.get_company_financials(cik, budget=budget)
                if financials:
                    financials_batch[cik] = financials

            # Tier 2: Recent filings
            filings = await sec_edgar.get_recent_filings(
                cik, form_types=None, lookback_days=90, limit=20, budget=budget
            )
            if filings:
                filings_batch[cik] = {"filings": filings}

            # Tier 2: Catalysts (8-K, S-1, etc.)
            catalysts = await sec_edgar.get_8k_s1_catalysts(
                cik, lookback_days=30, limit=10, budget=budget
            )
            if catalysts:
                catalysts_batch[cik] = {"catalysts": catalysts}

            # Tier 3: Insider activity (included in full refresh for baseline)
            if mode == "full":
                insider = await sec_edgar.get_form4_insider_summary(
                    cik, lookback_days=30, limit=15, budget=budget
                )
                if insider and insider.get("count", 0) > 0:
                    insider_batch[cik] = insider

        except Exception as e:
            errors += 1
            print(f"[EDGAR_CACHE] Error refreshing {ticker} (CIK {cik}): {e}")

        # Rate limit: SEC allows 10 req/sec, we stay conservative at ~2/sec
        await asyncio.sleep(0.5)

    # Bulk write to disk
    if financials_batch:
        bulk_set_cached(FINANCIALS_FILE, financials_batch)
    if filings_batch:
        bulk_set_cached(FILINGS_FILE, filings_batch)
    if catalysts_batch:
        bulk_set_cached(CATALYSTS_FILE, catalysts_batch)
    if insider_batch:
        bulk_set_cached(INSIDER_FILE, insider_batch)

    elapsed = round(time.time() - start, 1)
    print(
        f"[EDGAR_CACHE] {mode} refresh complete: "
        f"{len(financials_batch)} financials, {len(filings_batch)} filings, "
        f"{len(catalysts_batch)} catalysts, {len(insider_batch)} insider | "
        f"{errors} errors | {elapsed}s | {len(tickers)} tickers"
    )


@traceable(name="edgar_cache.is_midnight_cst")
def is_midnight_cst() -> bool:
    """Check if current time is within the midnight CST refresh window (00:00-00:05)."""
    now_cst = datetime.now(CST)
    return now_cst.hour == 0 and now_cst.minute < 5


@traceable(name="edgar_cache.is_market_hours")
def is_market_hours() -> bool:
    """Check if within extended market hours (7am-8pm EST, weekdays)."""
    est = timezone(timedelta(hours=-5))
    now_est = datetime.now(est)
    if now_est.weekday() >= 5:  # Saturday/Sunday
        return False
    return 7 <= now_est.hour < 20
