"""
Options history storage layer — persists Polygon EOD options data and
technical indicators to Neon PostgreSQL for the agent's TA reference.
"""

import json
from datetime import datetime, date

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop


def _get_conn():
    from data.pg_storage import _get_conn as pg_get_conn
    return pg_get_conn()


def _put_conn(conn):
    from data.pg_storage import _put_conn as pg_put_conn
    pg_put_conn(conn)


# ── Options History (EOD bars) ──────────────────────────────────────

@traceable(name="options_store.upsert_bars")
def upsert_options_bars(bars: list[dict]) -> int:
    """
    Upsert a batch of options daily bars.
    Each bar dict: {underlying, option_ticker, expiration, strike, option_type,
                    trade_date, open, high, low, close, volume, vwap, num_trades}
    Returns number of rows upserted.
    """
    if not bars:
        return 0
    conn = _get_conn()
    if conn is None:
        return 0
    count = 0
    try:
        cur = conn.cursor()
        for bar in bars:
            cur.execute("""
                INSERT INTO public.options_history
                    (underlying, option_ticker, expiration, strike, option_type,
                     trade_date, open, high, low, close, volume, vwap, num_trades, fetched_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (option_ticker, trade_date)
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    vwap = EXCLUDED.vwap,
                    num_trades = EXCLUDED.num_trades,
                    fetched_at = NOW()
            """, (
                bar.get("underlying"),
                bar.get("option_ticker"),
                bar.get("expiration"),
                bar.get("strike"),
                bar.get("option_type"),
                bar.get("trade_date"),
                bar.get("open"),
                bar.get("high"),
                bar.get("low"),
                bar.get("close"),
                bar.get("volume"),
                bar.get("vwap"),
                bar.get("num_trades"),
            ))
            count += 1
        conn.commit()
        cur.close()
        return count
    except Exception as e:
        print(f"[OPTIONS_STORE] upsert_options_bars error: {e}")
        conn.rollback()
        return 0
    finally:
        _put_conn(conn)


@traceable(name="options_store.get_history")
def get_options_history(
    underlying: str,
    option_type: str = None,
    from_date: str = None,
    to_date: str = None,
    limit: int = 1000,
) -> list[dict]:
    """
    Retrieve historic options bars for an underlying ticker.
    Optionally filter by option_type ('call'/'put') and date range.
    """
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        query = """
            SELECT underlying, option_ticker, expiration, strike, option_type,
                   trade_date, open, high, low, close, volume, vwap, num_trades
            FROM public.options_history
            WHERE underlying = %s
        """
        params = [underlying.upper()]

        if option_type:
            query += " AND option_type = %s"
            params.append(option_type)
        if from_date:
            query += " AND trade_date >= %s"
            params.append(from_date)
        if to_date:
            query += " AND trade_date <= %s"
            params.append(to_date)

        query += " ORDER BY trade_date DESC, strike ASC LIMIT %s"
        params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()

        return [
            {
                "underlying": r[0],
                "option_ticker": r[1],
                "expiration": r[2].isoformat() if r[2] else None,
                "strike": float(r[3]) if r[3] is not None else None,
                "option_type": r[4],
                "trade_date": r[5].isoformat() if r[5] else None,
                "open": float(r[6]) if r[6] is not None else None,
                "high": float(r[7]) if r[7] is not None else None,
                "low": float(r[8]) if r[8] is not None else None,
                "close": float(r[9]) if r[9] is not None else None,
                "volume": int(r[10]) if r[10] is not None else None,
                "vwap": float(r[11]) if r[11] is not None else None,
                "num_trades": int(r[12]) if r[12] is not None else None,
            }
            for r in rows
        ]
    except Exception as e:
        print(f"[OPTIONS_STORE] get_options_history error: {e}")
        return []
    finally:
        _put_conn(conn)


@traceable(name="options_store.get_volume_summary")
def get_options_volume_summary(
    underlying: str,
    days: int = 30,
) -> dict:
    """
    Get aggregated options volume summary for the last N days.
    Returns call/put volume totals, avg daily volume, max volume day, etc.
    """
    conn = _get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                option_type,
                COUNT(DISTINCT trade_date) AS trading_days,
                SUM(volume) AS total_volume,
                AVG(volume) AS avg_daily_contract_vol,
                MAX(volume) AS max_single_contract_vol,
                COUNT(DISTINCT option_ticker) AS unique_contracts
            FROM public.options_history
            WHERE underlying = %s
              AND trade_date >= CURRENT_DATE - INTERVAL '%s days'
              AND volume > 0
            GROUP BY option_type
        """, (underlying.upper(), days))
        rows = cur.fetchall()
        cur.close()

        summary = {"underlying": underlying.upper(), "period_days": days}
        for r in rows:
            otype = r[0]
            summary[f"{otype}_trading_days"] = int(r[1])
            summary[f"{otype}_total_volume"] = int(r[2]) if r[2] else 0
            summary[f"{otype}_avg_daily_vol"] = round(float(r[3]), 0) if r[3] else 0
            summary[f"{otype}_max_vol"] = int(r[4]) if r[4] else 0
            summary[f"{otype}_unique_contracts"] = int(r[5])

        return summary
    except Exception as e:
        print(f"[OPTIONS_STORE] get_options_volume_summary error: {e}")
        return {}
    finally:
        _put_conn(conn)


# ── Technical Indicators ─────────────────────────────────────────────

@traceable(name="options_store.upsert_technicals")
def upsert_technicals(rows: list[dict]) -> int:
    """
    Upsert technical indicator data points.
    Each row: {ticker, indicator, trade_date, value, signal_value, histogram}
    """
    if not rows:
        return 0
    conn = _get_conn()
    if conn is None:
        return 0
    count = 0
    try:
        cur = conn.cursor()
        for row in rows:
            cur.execute("""
                INSERT INTO public.stock_technicals
                    (ticker, indicator, trade_date, value, signal_value, histogram, fetched_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (ticker, indicator, trade_date)
                DO UPDATE SET
                    value = EXCLUDED.value,
                    signal_value = EXCLUDED.signal_value,
                    histogram = EXCLUDED.histogram,
                    fetched_at = NOW()
            """, (
                row.get("ticker"),
                row.get("indicator"),
                row.get("trade_date"),
                row.get("value"),
                row.get("signal_value"),
                row.get("histogram"),
            ))
            count += 1
        conn.commit()
        cur.close()
        return count
    except Exception as e:
        print(f"[OPTIONS_STORE] upsert_technicals error: {e}")
        conn.rollback()
        return 0
    finally:
        _put_conn(conn)


@traceable(name="options_store.get_technicals")
def get_technicals(
    ticker: str,
    indicator: str = None,
    from_date: str = None,
    limit: int = 500,
) -> list[dict]:
    """
    Retrieve stored technical indicator data for a ticker.
    """
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        query = """
            SELECT ticker, indicator, trade_date, value, signal_value, histogram
            FROM public.stock_technicals
            WHERE ticker = %s
        """
        params = [ticker.upper()]
        if indicator:
            query += " AND indicator = %s"
            params.append(indicator)
        if from_date:
            query += " AND trade_date >= %s"
            params.append(from_date)
        query += " ORDER BY trade_date DESC LIMIT %s"
        params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()

        return [
            {
                "ticker": r[0],
                "indicator": r[1],
                "trade_date": r[2].isoformat() if r[2] else None,
                "value": float(r[3]) if r[3] is not None else None,
                "signal_value": float(r[4]) if r[4] is not None else None,
                "histogram": float(r[5]) if r[5] is not None else None,
            }
            for r in rows
        ]
    except Exception as e:
        print(f"[OPTIONS_STORE] get_technicals error: {e}")
        return []
    finally:
        _put_conn(conn)


@traceable(name="options_store.get_latest_technicals")
def get_latest_technicals(ticker: str) -> dict:
    """
    Get the latest value for each indicator for a ticker.
    Returns a flat dict: {sma_20: value, sma_50: value, rsi_14: value, macd: value, ...}
    """
    conn = _get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT ON (indicator)
                indicator, trade_date, value, signal_value, histogram
            FROM public.stock_technicals
            WHERE ticker = %s
            ORDER BY indicator, trade_date DESC
        """, (ticker.upper(),))
        rows = cur.fetchall()
        cur.close()

        result = {"ticker": ticker.upper()}
        for r in rows:
            ind = r[0]
            result[ind] = {
                "date": r[1].isoformat() if r[1] else None,
                "value": float(r[2]) if r[2] is not None else None,
            }
            if r[3] is not None:
                result[ind]["signal"] = float(r[3])
            if r[4] is not None:
                result[ind]["histogram"] = float(r[4])
        return result
    except Exception as e:
        print(f"[OPTIONS_STORE] get_latest_technicals error: {e}")
        return {}
    finally:
        _put_conn(conn)


# ── Live Options Flow Snapshots ─────────────────────────────────────

@traceable(name="options_store.store_flow_snapshots")
def store_options_flow_snapshots(rows: list[dict]) -> int:
    """Persist lightweight live options flow snapshots for future history-based scoring."""
    if not rows:
        return 0
    conn = _get_conn()
    if conn is None:
        return 0
    try:
        cur = conn.cursor()
        inserted = 0
        for row in rows:
            cur.execute("""
                INSERT INTO public.options_flow_snapshots (
                    underlying, contract_symbol, expiration, option_type, strike, underlying_price,
                    bid, ask, last, midpoint, volume, open_interest, implied_volatility, delta, gamma,
                    theta, vega, spread_pct, premium_traded_estimate, expected_move_pct
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
            """, (
                row.get("underlying"),
                row.get("contract_symbol"),
                row.get("expiration"),
                row.get("option_type"),
                row.get("strike"),
                row.get("underlying_price"),
                row.get("bid"),
                row.get("ask"),
                row.get("last"),
                row.get("midpoint"),
                row.get("volume"),
                row.get("open_interest"),
                row.get("implied_volatility"),
                row.get("delta"),
                row.get("gamma"),
                row.get("theta"),
                row.get("vega"),
                row.get("spread_pct"),
                row.get("premium_traded_estimate"),
                row.get("expected_move_pct"),
            ))
            inserted += 1
        conn.commit()
        cur.close()
        return inserted
    except Exception as e:
        print(f"[OPTIONS_STORE] store_options_flow_snapshots error: {e}")
        conn.rollback()
        return 0
    finally:
        _put_conn(conn)


@traceable(name="options_store.get_contract_flow_summary")
def get_contract_flow_history_summary(contract_symbol: str, days: int = 30) -> dict:
    """Return recent snapshot summary for a contract to support repeated flow and IV history."""
    if not contract_symbol:
        return {}
    conn = _get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT volume, open_interest, implied_volatility, premium_traded_estimate, captured_at
            FROM public.options_flow_snapshots
            WHERE contract_symbol = %s
              AND captured_at >= NOW() - (%s * INTERVAL '1 day')
            ORDER BY captured_at DESC
            LIMIT 60
        """, (contract_symbol, days))
        rows = cur.fetchall()
        cur.close()
        if len(rows) < 5:
            return {}

        volumes = [int(r[0]) for r in rows if r[0] is not None]
        open_interests = [int(r[1]) for r in rows if r[1] is not None]
        ivs = [float(r[2]) for r in rows if r[2] is not None]
        premiums = [float(r[3]) for r in rows if r[3] is not None]

        latest_volume = volumes[0] if volumes else None
        avg_volume = (sum(volumes[1:]) / len(volumes[1:])) if len(volumes) > 1 else None
        repeated_flow_score = None
        if latest_volume is not None and avg_volume and avg_volume > 0:
            repeated_flow_score = round(min((latest_volume / avg_volume) * 20.0, 100.0), 1)

        iv_percentile = None
        if len(ivs) >= 20:
            latest_iv = ivs[0]
            less_equal = sum(1 for iv in ivs if iv <= latest_iv)
            iv_percentile = round((less_equal / len(ivs)) * 100.0, 1)

        oi_change = None
        if len(open_interests) >= 2 and open_interests[1] is not None:
            oi_change = open_interests[0] - open_interests[1]

        return {
            "snapshot_count": len(rows),
            "avg_volume": round(avg_volume, 1) if avg_volume is not None else None,
            "latest_volume": latest_volume,
            "avg_premium_traded_estimate": round(sum(premiums) / len(premiums), 2) if premiums else None,
            "avg_iv": round(sum(ivs) / len(ivs), 4) if ivs else None,
            "iv_percentile": iv_percentile,
            "latest_open_interest": open_interests[0] if open_interests else None,
            "oi_change": oi_change,
            "repeated_flow_score": repeated_flow_score,
        }
    except Exception as e:
        print(f"[OPTIONS_STORE] get_contract_flow_history_summary error: {e}")
        return {}
    finally:
        _put_conn(conn)


# ── Fetch Progress Tracking ──────────────────────────────────────────

@traceable(name="options_store.get_fetch_progress")
def get_fetch_progress(ticker: str = None) -> list[dict] | dict | None:
    """Get fetch progress for one or all tickers."""
    conn = _get_conn()
    if conn is None:
        return [] if ticker is None else None
    try:
        cur = conn.cursor()
        if ticker:
            cur.execute(
                "SELECT ticker, last_fetched_date, contracts_fetched, status, error_message, updated_at FROM public.options_fetch_progress WHERE ticker = %s",
                (ticker.upper(),),
            )
            row = cur.fetchone()
            cur.close()
            if not row:
                return None
            return {
                "ticker": row[0],
                "last_fetched_date": row[1].isoformat() if row[1] else None,
                "contracts_fetched": row[2],
                "status": row[3],
                "error_message": row[4],
                "updated_at": row[5].isoformat() if row[5] else None,
            }
        else:
            cur.execute(
                "SELECT ticker, last_fetched_date, contracts_fetched, status, error_message, updated_at FROM public.options_fetch_progress ORDER BY updated_at DESC"
            )
            rows = cur.fetchall()
            cur.close()
            return [
                {
                    "ticker": r[0],
                    "last_fetched_date": r[1].isoformat() if r[1] else None,
                    "contracts_fetched": r[2],
                    "status": r[3],
                    "error_message": r[4],
                    "updated_at": r[5].isoformat() if r[5] else None,
                }
                for r in rows
            ]
    except Exception as e:
        print(f"[OPTIONS_STORE] get_fetch_progress error: {e}")
        return [] if ticker is None else None
    finally:
        _put_conn(conn)


@traceable(name="options_store.update_fetch_progress")
def update_fetch_progress(
    ticker: str,
    status: str,
    contracts_fetched: int = 0,
    last_fetched_date: str = None,
    error_message: str = None,
) -> bool:
    """Update fetch progress for a ticker."""
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO public.options_fetch_progress
                (ticker, last_fetched_date, contracts_fetched, status, error_message, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (ticker)
            DO UPDATE SET
                last_fetched_date = COALESCE(EXCLUDED.last_fetched_date, public.options_fetch_progress.last_fetched_date),
                contracts_fetched = EXCLUDED.contracts_fetched,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                updated_at = NOW()
        """, (ticker.upper(), last_fetched_date, contracts_fetched, status, error_message))
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[OPTIONS_STORE] update_fetch_progress error: {e}")
        conn.rollback()
        return False
    finally:
        _put_conn(conn)


# ── Aggregate Queries for Agent ──────────────────────────────────────

@traceable(name="options_store.get_iv_history")
def get_iv_history(underlying: str, days: int = 90) -> list[dict]:
    """
    Get implied volatility history by aggregating option close prices over time.
    Uses ATM options as proxy for IV trends.
    Groups by trade_date and option_type, returning avg close price trends.
    """
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                trade_date,
                option_type,
                AVG(close) AS avg_premium,
                SUM(volume) AS total_volume,
                COUNT(*) AS contracts_traded
            FROM public.options_history
            WHERE underlying = %s
              AND trade_date >= CURRENT_DATE - INTERVAL '%s days'
              AND volume > 0
            GROUP BY trade_date, option_type
            ORDER BY trade_date ASC
        """, (underlying.upper(), days))
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "trade_date": r[0].isoformat() if r[0] else None,
                "option_type": r[1],
                "avg_premium": round(float(r[2]), 4) if r[2] else None,
                "total_volume": int(r[3]) if r[3] else 0,
                "contracts_traded": int(r[4]),
            }
            for r in rows
        ]
    except Exception as e:
        print(f"[OPTIONS_STORE] get_iv_history error: {e}")
        return []
    finally:
        _put_conn(conn)


@traceable(name="options_store.get_data_coverage")
def get_data_coverage() -> dict:
    """Get summary of data coverage across all stored options data."""
    conn = _get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor()

        # Options history coverage
        cur.execute("""
            SELECT
                COUNT(DISTINCT underlying) AS tickers,
                COUNT(DISTINCT option_ticker) AS contracts,
                COUNT(*) AS total_bars,
                MIN(trade_date) AS earliest_date,
                MAX(trade_date) AS latest_date
            FROM public.options_history
        """)
        oh = cur.fetchone()

        # Technical indicators coverage
        cur.execute("""
            SELECT
                COUNT(DISTINCT ticker) AS tickers,
                COUNT(DISTINCT indicator) AS indicators,
                COUNT(*) AS total_points,
                MIN(trade_date) AS earliest_date,
                MAX(trade_date) AS latest_date
            FROM public.stock_technicals
        """)
        st = cur.fetchone()

        # Fetch progress summary
        cur.execute("""
            SELECT status, COUNT(*) FROM public.options_fetch_progress GROUP BY status
        """)
        progress_rows = cur.fetchall()

        cur.close()

        return {
            "options_history": {
                "tickers": oh[0] if oh else 0,
                "contracts": oh[1] if oh else 0,
                "total_bars": oh[2] if oh else 0,
                "earliest_date": oh[3].isoformat() if oh and oh[3] else None,
                "latest_date": oh[4].isoformat() if oh and oh[4] else None,
            },
            "technicals": {
                "tickers": st[0] if st else 0,
                "indicators": st[1] if st else 0,
                "total_points": st[2] if st else 0,
                "earliest_date": st[3].isoformat() if st and st[3] else None,
                "latest_date": st[4].isoformat() if st and st[4] else None,
            },
            "fetch_progress": {r[0]: r[1] for r in progress_rows} if progress_rows else {},
        }
    except Exception as e:
        print(f"[OPTIONS_STORE] get_data_coverage error: {e}")
        return {}
    finally:
        _put_conn(conn)
