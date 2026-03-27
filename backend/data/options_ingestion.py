"""
Background ingestion service for historic options data from Polygon.

Fetches EOD options bars + technical indicators for the full watchlist,
respecting the Polygon Massive free tier rate limit (5 calls/min).
Stores everything in Neon PostgreSQL for the agent's TA reference.

Designed to run as a background loop from main.py lifespan.
"""

import time
import asyncio
from datetime import datetime, timedelta

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop

# ── Full watchlist (US-listed only) ──────────────────────────────────
# Non-US tickers excluded: GLXY, TSX:GMIN, TSXV:AAG, AMS:BESI,
# ETR:AIXA, OTC:KRKNF, AIM:IQE, ASX:EOS

OPTIONS_WATCHLIST = [
    # Semis & Tech Hardware
    "TSM", "MU", "APH", "KLAC", "ETN", "ANET", "TEL", "WDC", "STX",
    "GLW", "EQIX", "VRT", "NOK", "FSLR", "MRVL", "ON", "SMCI", "GFS",
    "LOGI", "TER", "COHR", "AMKR", "CIEN", "FN", "CRDO", "MCHP",
    "POWL", "ONTO", "TSEM", "ALAB", "MTSI", "SEI", "SMTC", "CAMT",
    "PSTG", "VIAV", "SIMO", "FORM", "UCTT", "VICR", "VSH", "KLIC",
    "ICHR", "LPTH", "AEHR", "AXTI", "AAOI",

    # Energy & Resources
    "EQT", "CEG", "CNX", "CDE", "TPL", "SSRM", "FSM", "USAC", "EXK",
    "SDRL", "BE", "TGB",

    # Software & Growth
    "SHOP", "VIST", "SNDK", "KRMN", "CIFR", "KTOS", "IPGP", "LITE",

    # Small/Micro Cap Tech
    "VOXR", "OPTX", "OSS", "ADUR", "LWLG", "AMPX", "LASR", "UMAC",
    "LAES", "POET", "ONDS", "IPX",

    # Space & Defense
    "AVAV", "AMBA", "APLD", "OUST", "NVTS", "PL", "LUNR", "IREN",
    "TE", "AEVA", "WULF", "RDW", "RKLB", "ASTS",

    # Crypto-adjacent & Digital Infra
    "HUT", "IONQ", "NBIS", "CLSK", "MARA",
]

# How many contracts to fetch daily bars for, per ticker (most liquid/ATM)
MAX_CONTRACTS_PER_TICKER = 10

# Re-fetch interval: 6 hours for tickers already completed, to pick up new EOD data
REFETCH_INTERVAL_HOURS = 6


def _parse_option_ticker(polygon_ticker: str) -> dict:
    """
    Parse a Polygon option ticker like 'O:AAPL250321C00200000' into components.
    Returns {underlying, expiration, option_type, strike} or empty dict on failure.
    """
    t = polygon_ticker
    if t.startswith("O:"):
        t = t[2:]

    # Find where the date portion starts (6 digits for YYMMDD)
    # Format: UNDERLYING + YYMMDD + C/P + 8-digit strike
    # The underlying is everything before the last 15 characters
    if len(t) < 15:
        return {}

    suffix = t[-15:]  # YYMMDD + C/P + 8 digits
    underlying = t[:-15]
    if not underlying:
        return {}

    try:
        yy = int(suffix[0:2])
        mm = int(suffix[2:4])
        dd = int(suffix[4:6])
        option_type = "call" if suffix[6] == "C" else "put"
        strike = int(suffix[7:]) / 1000
        expiration = f"20{yy:02d}-{mm:02d}-{dd:02d}"
        return {
            "underlying": underlying,
            "expiration": expiration,
            "option_type": option_type,
            "strike": strike,
        }
    except (ValueError, IndexError):
        return {}


@traceable(name="options_ingestion.ingest_ticker_options")
def ingest_ticker_options(polygon_opts, ticker: str) -> dict:
    """
    Fetch and store historic options data for a single ticker.
    1. Get key contracts from Polygon reference API
    2. Fetch daily bars for each key contract (up to MAX_CONTRACTS_PER_TICKER)
    3. Store in PostgreSQL

    Returns {contracts_fetched, bars_stored, errors}.
    """
    from data.options_history_store import upsert_options_bars, update_fetch_progress

    ticker = ticker.upper()
    update_fetch_progress(ticker, status="in_progress")

    try:
        # Step 1: Get contracts reference
        contracts = polygon_opts.get_key_contracts(ticker)
        if not contracts:
            print(f"[INGEST] No contracts found for {ticker}")
            update_fetch_progress(ticker, status="complete", contracts_fetched=0)
            return {"contracts_fetched": 0, "bars_stored": 0, "errors": 0}

        # Prioritize contracts: sort by volume/OI if available, take top N
        # Polygon reference data doesn't include volume, so just take nearest expirations
        contracts = contracts[:MAX_CONTRACTS_PER_TICKER]
        print(f"[INGEST] {ticker}: fetching bars for {len(contracts)} contracts")

        total_bars = 0
        errors = 0
        from_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
        to_date = datetime.now().strftime("%Y-%m-%d")

        for contract in contracts:
            opt_ticker = contract.get("ticker", "")
            if not opt_ticker:
                continue

            parsed = _parse_option_ticker(opt_ticker)
            if not parsed:
                continue

            try:
                bars = polygon_opts.get_daily_bars(opt_ticker, from_date=from_date, to_date=to_date)
                if not bars:
                    continue

                # Transform Polygon bars to our DB format
                db_bars = []
                for bar in bars:
                    ts = bar.get("t")
                    if ts:
                        trade_date = datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d")
                    else:
                        continue

                    db_bars.append({
                        "underlying": ticker,
                        "option_ticker": opt_ticker,
                        "expiration": parsed["expiration"],
                        "strike": parsed["strike"],
                        "option_type": parsed["option_type"],
                        "trade_date": trade_date,
                        "open": bar.get("o"),
                        "high": bar.get("h"),
                        "low": bar.get("l"),
                        "close": bar.get("c"),
                        "volume": bar.get("v"),
                        "vwap": bar.get("vw"),
                        "num_trades": bar.get("n"),
                    })

                stored = upsert_options_bars(db_bars)
                total_bars += stored
                print(f"[INGEST] {opt_ticker}: {stored} bars stored")

            except Exception as e:
                print(f"[INGEST] Error fetching bars for {opt_ticker}: {e}")
                errors += 1

        update_fetch_progress(
            ticker,
            status="complete",
            contracts_fetched=len(contracts),
            last_fetched_date=to_date,
        )

        print(f"[INGEST] {ticker}: done — {len(contracts)} contracts, {total_bars} bars, {errors} errors")
        return {"contracts_fetched": len(contracts), "bars_stored": total_bars, "errors": errors}

    except Exception as e:
        print(f"[INGEST] Fatal error for {ticker}: {e}")
        update_fetch_progress(ticker, status="error", error_message=str(e)[:500])
        return {"contracts_fetched": 0, "bars_stored": 0, "errors": 1}


@traceable(name="options_ingestion.ingest_technicals")
def ingest_technicals(polygon_opts, ticker: str) -> int:
    """
    Fetch and store all 4 technical indicators for a ticker.
    Uses 4 API calls (SMA 20, SMA 50, RSI 14, MACD).
    Returns total data points stored.
    """
    from data.options_history_store import upsert_technicals

    ticker = ticker.upper()
    total = 0

    try:
        technicals = polygon_opts.get_all_technicals(ticker)

        # Transform Polygon indicator data to our DB format
        db_rows = []

        # SMA 20
        for pt in technicals.get("sma_20", []):
            ts = pt.get("timestamp")
            if not ts:
                continue
            trade_date = datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d")
            db_rows.append({
                "ticker": ticker,
                "indicator": "sma_20",
                "trade_date": trade_date,
                "value": pt.get("value"),
                "signal_value": None,
                "histogram": None,
            })

        # SMA 50
        for pt in technicals.get("sma_50", []):
            ts = pt.get("timestamp")
            if not ts:
                continue
            trade_date = datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d")
            db_rows.append({
                "ticker": ticker,
                "indicator": "sma_50",
                "trade_date": trade_date,
                "value": pt.get("value"),
                "signal_value": None,
                "histogram": None,
            })

        # RSI 14
        for pt in technicals.get("rsi_14", []):
            ts = pt.get("timestamp")
            if not ts:
                continue
            trade_date = datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d")
            db_rows.append({
                "ticker": ticker,
                "indicator": "rsi_14",
                "trade_date": trade_date,
                "value": pt.get("value"),
                "signal_value": None,
                "histogram": None,
            })

        # MACD
        for pt in technicals.get("macd", []):
            ts = pt.get("timestamp")
            if not ts:
                continue
            trade_date = datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d")
            db_rows.append({
                "ticker": ticker,
                "indicator": "macd",
                "trade_date": trade_date,
                "value": pt.get("value"),
                "signal_value": pt.get("signal"),
                "histogram": pt.get("histogram"),
            })

        if db_rows:
            total = upsert_technicals(db_rows)
            print(f"[INGEST] {ticker} technicals: {total} data points stored")

    except Exception as e:
        print(f"[INGEST] Technicals error for {ticker}: {e}")

    return total


async def run_ingestion_loop(polygon_opts, init_event=None, stop_event=None):
    """
    Main background ingestion loop. Runs continuously:

    Phase 1 (initial): Fetch all watchlist tickers sequentially (respecting 5/min rate limit).
      - For each ticker: fetch contracts + bars (~11 API calls), then technicals (~4 API calls)
      - Total: ~95 tickers × 15 calls = ~1425 calls ÷ 5/min = ~285 minutes = ~4.75 hours

    Phase 2 (maintenance): Re-fetch completed tickers every 6 hours to pick up new EOD data.

    This runs in a background thread via asyncio.to_thread().
    """
    loop = asyncio.get_event_loop()

    # Wait for app init
    if init_event:
        await loop.run_in_executor(None, init_event.wait, 180)

    from data.options_history_store import get_fetch_progress, update_fetch_progress

    print(f"[INGEST_LOOP] Starting options data ingestion for {len(OPTIONS_WATCHLIST)} tickers")

    while True:
        if stop_event and stop_event.is_set():
            print("[INGEST_LOOP] Stop event received, exiting")
            break

        try:
            # Determine which tickers need fetching
            pending_tickers = []
            stale_tickers = []

            for ticker in OPTIONS_WATCHLIST:
                progress = get_fetch_progress(ticker)
                if progress is None or progress.get("status") == "pending":
                    pending_tickers.append(ticker)
                elif progress.get("status") == "error":
                    pending_tickers.append(ticker)  # Retry errors
                elif progress.get("status") == "complete":
                    # Check if stale (>6 hours since last fetch)
                    updated = progress.get("updated_at")
                    if updated:
                        try:
                            last_update = datetime.fromisoformat(updated)
                            if (datetime.now(last_update.tzinfo) - last_update).total_seconds() > REFETCH_INTERVAL_HOURS * 3600:
                                stale_tickers.append(ticker)
                        except Exception:
                            stale_tickers.append(ticker)

            # Prioritize: pending first, then stale
            work_queue = pending_tickers + stale_tickers

            if not work_queue:
                print(f"[INGEST_LOOP] All {len(OPTIONS_WATCHLIST)} tickers up to date. Sleeping 30 min.")
                await asyncio.sleep(1800)
                continue

            phase = "initial" if pending_tickers else "maintenance"
            print(f"[INGEST_LOOP] {phase} phase: {len(pending_tickers)} pending, {len(stale_tickers)} stale")

            for i, ticker in enumerate(work_queue):
                if stop_event and stop_event.is_set():
                    break

                print(f"[INGEST_LOOP] [{i+1}/{len(work_queue)}] Processing {ticker}...")

                # Fetch options data (runs in thread to avoid blocking event loop)
                result = await loop.run_in_executor(
                    None, ingest_ticker_options, polygon_opts, ticker
                )

                # Fetch technicals (4 more API calls)
                tech_count = await loop.run_in_executor(
                    None, ingest_technicals, polygon_opts, ticker
                )

                print(
                    f"[INGEST_LOOP] [{i+1}/{len(work_queue)}] {ticker} done: "
                    f"{result['contracts_fetched']} contracts, {result['bars_stored']} bars, "
                    f"{tech_count} technical points"
                )

                # Small sleep between tickers to be nice to the rate limiter
                await asyncio.sleep(2)

            print(f"[INGEST_LOOP] Cycle complete. Sleeping 30 min before next check.")
            await asyncio.sleep(1800)

        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"[INGEST_LOOP] Error: {e}. Retrying in 5 min.")
            await asyncio.sleep(300)
