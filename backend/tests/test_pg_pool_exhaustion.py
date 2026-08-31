"""
Tests for pg_storage connection pool exhaustion fix and health-check safety.

Validates:
  - pool exhaustion returns None without destroying the pool
  - one stale connection does NOT destroy the whole pool (closeall not called)
  - health-check failure discards only the failed conn, pool survives for retry
  - retry after stale conn can obtain a healthy connection from the SAME pool
  - exhaustion detection is narrow (PoolError + canonical message only)
  - instrumentation stores no connection objects (caller/timestamp metadata only)
  - genuine pool-level errors still trigger pool rebuild
"""
from __future__ import annotations

import sys
import unittest
from unittest.mock import MagicMock, patch, PropertyMock, call

# Ensure backend is importable
sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.dirname(__file__)))

import data.pg_storage as pgs
import services.watchlist_service as watchlist_service
from psycopg2.pool import PoolError
from psycopg2 import OperationalError


# ── Helpers ───────────────────────────────────────────────────────────────────

def _mock_healthy_conn():
    """Return a MagicMock that passes the health check (SELECT 1)."""
    c = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = (1,)
    c.cursor.return_value = cur
    return c


def _mock_stale_conn():
    """Return a MagicMock whose cursor.execute raises on SELECT 1."""
    c = MagicMock()
    cur = MagicMock()
    cur.execute.side_effect = OperationalError("server closed the connection")
    c.cursor.return_value = cur
    return c


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestPoolExhaustionSafeReturn(unittest.TestCase):
    """Pool exhaustion must NOT destroy the pool — return None gracefully."""

    def setUp(self):
        pgs._pool = None
        pgs._available = False
        pgs._last_conn_error = None
        pgs._conn_exhaustion_count = 0
        pgs._conn_checkouts.clear()

    def test_exhaustion_returns_none_pool_untouched(self):
        mock_pool = MagicMock()
        mock_pool.getconn.side_effect = PoolError("connection pool exhausted")
        pgs._pool = mock_pool

        with patch.object(pgs, "_DATABASE_URL", "postgresql://test:test@localhost/test", create=True):
            result = pgs._get_conn()

        self.assertIsNone(result)
        mock_pool.closeall.assert_not_called()
        self.assertIsNotNone(pgs._pool)
        self.assertEqual(pgs._conn_exhaustion_count, 1)

    def test_exhaustion_increments_counter(self):
        mock_pool = MagicMock()
        mock_pool.getconn.side_effect = PoolError("connection pool exhausted")
        pgs._pool = mock_pool

        with patch.object(pgs, "_DATABASE_URL", "postgresql://test:test@localhost/test", create=True):
            pgs._get_conn()
            pgs._get_conn()

        self.assertEqual(pgs._conn_exhaustion_count, 2)

    def test_non_poolerror_with_exhausted_word_not_exhaustion(self):
        """Exception('budget exhausted') is NOT pool capacity exhaustion."""
        mock_pool = MagicMock()
        mock_pool.getconn.side_effect = RuntimeError("worker budget exhausted")
        pgs._pool = mock_pool

        with patch.object(pgs, "_DATABASE_URL", "postgresql://test:test@localhost/test", create=True):
            result = pgs._get_conn()

        # Should NOT be classified as pool exhaustion — destroy was called
        self.assertIsNone(result)
        # Exhaustion counter must NOT increment
        self.assertEqual(pgs._conn_exhaustion_count, 0)
        # Pool-level failure → closeall called, pool set to None
        mock_pool.closeall.assert_called()
        self.assertIsNone(pgs._pool)

    def test_poolerror_wrong_message_not_exhaustion(self):
        """PoolError without 'connection pool exhausted' is not capacity exhaustion."""
        mock_pool = MagicMock()
        mock_pool.getconn.side_effect = PoolError("connection pool is closed")
        pgs._pool = mock_pool

        with patch.object(pgs, "_DATABASE_URL", "postgresql://test:test@localhost/test", create=True):
            result = pgs._get_conn()

        # Should be treated as genuine pool failure
        self.assertIsNone(result)
        self.assertEqual(pgs._conn_exhaustion_count, 0)
        mock_pool.closeall.assert_called()
        self.assertIsNone(pgs._pool)

    def test_genuine_error_still_destroys_pool(self):
        mock_pool = MagicMock()
        mock_pool.getconn.side_effect = OperationalError("could not connect to server")
        pgs._pool = mock_pool

        with patch.object(pgs, "_DATABASE_URL", "postgresql://test:test@localhost/test", create=True):
            result = pgs._get_conn()

        self.assertIsNone(result)
        mock_pool.closeall.assert_called()
        self.assertIsNone(pgs._pool)
        self.assertEqual(pgs._conn_exhaustion_count, 0)

    def test_empty_database_url_returns_none(self):
        with patch.object(pgs, "_DATABASE_URL", "", create=True):
            result = pgs._get_conn()
        self.assertIsNone(result)
        self.assertEqual(pgs._last_conn_error, "No NEON_DATABASE_URL or DATABASE_URL set")


class TestHealthCheckFailurePreservesPool(unittest.TestCase):
    """One stale connection must NOT destroy the pool."""

    def setUp(self):
        pgs._pool = None
        pgs._available = False
        pgs._last_conn_error = None
        pgs._conn_checkouts.clear()
        pgs._conn_exhaustion_count = 0

    def test_stale_conn_does_not_call_closeall(self):
        """Health-check failure discards only the bad conn; pool survives."""
        mock_pool = MagicMock()
        stale = _mock_stale_conn()
        mock_pool.getconn.return_value = stale
        pgs._pool = mock_pool

        with patch.object(pgs, "_DATABASE_URL", "postgresql://test:test@localhost/test", create=True):
            result = pgs._get_conn()

        # After 2 stale attempts, returns None
        self.assertIsNone(result)
        # closeall() must NOT be called — pool is still healthy
        mock_pool.closeall.assert_not_called()
        # putconn(conn, close=True) must have been called on each stale conn
        putconn_calls = [
            c for c in mock_pool.putconn.call_args_list
            if c[1].get("close") is True
        ]
        self.assertGreater(len(putconn_calls), 0, "Expected putconn(conn, close=True) calls")
        # Pool must survive — NOT set to None
        self.assertIsNotNone(pgs._pool)

    def test_stale_then_healthy_retry_same_pool(self):
        """After one stale connection, retry from same pool gets a healthy conn."""
        mock_pool = MagicMock()
        stale = _mock_stale_conn()
        healthy = _mock_healthy_conn()
        mock_pool.getconn.side_effect = [stale, healthy]
        pgs._pool = mock_pool

        with patch.object(pgs, "_DATABASE_URL", "postgresql://test:test@localhost/test", create=True):
            result = pgs._get_conn()

        self.assertIsNotNone(result)
        self.assertIs(result, healthy)
        # Pool survived
        self.assertIsNotNone(pgs._pool)
        mock_pool.closeall.assert_not_called()
        # One putconn(close=True) for the stale conn
        close_calls = [
            c for c in mock_pool.putconn.call_args_list
            if c[1].get("close") is True
        ]
        self.assertEqual(len(close_calls), 1)

    def test_stale_conn_pool_survives_for_next_caller(self):
        """After health-check exhaustion (all retries stale), a subsequent
        caller can still get a healthy connection from the same pool."""
        mock_pool = MagicMock()
        stale = _mock_stale_conn()
        healthy = _mock_healthy_conn()

        # Caller A: gets only stale connections
        caller_a_pool = MagicMock()
        caller_a_pool.getconn.side_effect = [stale, stale]
        pgs._pool = caller_a_pool

        with patch.object(pgs, "_DATABASE_URL", "postgresql://test:test@localhost/test", create=True):
            result_a = pgs._get_conn()
        self.assertIsNone(result_a)
        self.assertIsNotNone(pgs._pool, "Pool must survive for next caller")

        # Caller B: pool still exists, gets a healthy conn
        caller_b_pool = MagicMock()
        caller_b_pool.getconn.return_value = healthy
        pgs._pool = caller_b_pool

        with patch.object(pgs, "_DATABASE_URL", "postgresql://test:test@localhost/test", create=True):
            result_b = pgs._get_conn()
        self.assertIsNotNone(result_b)
        self.assertIs(result_b, healthy)


class TestInstrumentationTracking(unittest.TestCase):
    """Checkout/checkin tracking must be accurate and thread-safe."""

    def setUp(self):
        pgs._conn_checkouts.clear()
        pgs._conn_exhaustion_count = 0

    def test_checkout_and_checkin_tracking(self):
        conn1 = MagicMock()
        conn1_id = id(conn1)

        pgs._record_checkout(conn1, "pg_storage.py:watchlist_read:1780")
        self.assertIn(conn1_id, pgs._conn_checkouts)
        self.assertEqual(len(pgs._conn_checkouts), 1)

        pgs._record_checkin(conn1)
        self.assertNotIn(conn1_id, pgs._conn_checkouts)
        self.assertEqual(len(pgs._conn_checkouts), 0)

    def test_multiple_checkouts_ordered_by_hold_time(self):
        import time
        conn_a = MagicMock()
        conn_b = MagicMock()

        pgs._record_checkout(conn_a, "caller_a")
        time.sleep(0.01)
        pgs._record_checkout(conn_b, "caller_b")

        snap = pgs._pool_exhaustion_snapshot()
        self.assertEqual(snap["held_connections"], 2)
        self.assertEqual(snap["callers"][0]["caller"], "caller_a")
        self.assertGreater(snap["callers"][0]["held_s"], 0)

        pgs._record_checkin(conn_a)
        pgs._record_checkin(conn_b)

        snap_after = pgs._pool_exhaustion_snapshot()
        self.assertEqual(snap_after["held_connections"], 0)

    def test_instrumentation_stores_no_connection_objects(self):
        """Tracked entries contain only caller/timestamp — never the conn."""
        conn = MagicMock()
        pgs._record_checkout(conn, "test_caller")

        entry = pgs._conn_checkouts[id(conn)]
        self.assertIn("acquired_s", entry)
        self.assertIn("caller", entry)
        self.assertEqual(entry["caller"], "test_caller")
        # The connection itself must NOT be in the entry
        self.assertNotIn("conn", entry)
        self.assertNotIn("connection", entry)
        # The value must not be the connection object
        self.assertIsNot(entry, conn)

        pgs._record_checkin(conn)
        self.assertNotIn(id(conn), pgs._conn_checkouts)


class TestIsPoolExhaustedError(unittest.TestCase):
    """Exhaustion detection must be narrow — PoolError + canonical message."""

    def test_recognises_canonical_poolerror(self):
        e = PoolError("connection pool exhausted")
        self.assertTrue(pgs._is_pool_exhausted_error(e))

    def test_poolerror_wrong_message_not_exhaustion(self):
        e = PoolError("connection pool is closed")
        self.assertFalse(pgs._is_pool_exhausted_error(e))

    def test_non_poolerror_with_keyword_not_exhaustion(self):
        e = RuntimeError("worker budget exhausted")
        self.assertFalse(pgs._is_pool_exhausted_error(e))

    def test_operational_error_not_exhaustion(self):
        e = OperationalError("could not connect to server")
        self.assertFalse(pgs._is_pool_exhausted_error(e))

    def test_generic_exception_not_exhaustion(self):
        e = Exception("connection pool exhausted")
        self.assertFalse(pgs._is_pool_exhausted_error(e))


class TestPoolInstrumentation(unittest.TestCase):
    """pool_instrumentation() must return a dict with expected keys."""

    def setUp(self):
        pgs._conn_checkouts.clear()
        pgs._conn_exhaustion_count = 0

    def test_returns_expected_keys(self):
        pgs._conn_exhaustion_count = 3
        info = pgs.pool_instrumentation()
        self.assertIn("held_connections", info)
        self.assertIn("exhaustion_events", info)
        self.assertIn("callers", info)
        self.assertIn("pool_active", info)
        self.assertIn("pool_available", info)
        self.assertIn("last_conn_error", info)
        self.assertEqual(info["exhaustion_events"], 3)


class TestCallerAutoDetection(unittest.TestCase):
    """_get_conn must auto-detect the calling function."""

    def setUp(self):
        pgs._pool = None
        pgs._available = False
        pgs._last_conn_error = None
        pgs._conn_checkouts.clear()
        pgs._conn_exhaustion_count = 0

    def test_exhaustion_sets_error_message(self):
        mock_pool = MagicMock()
        mock_pool.getconn.side_effect = PoolError("connection pool exhausted")
        pgs._pool = mock_pool

        with patch.object(pgs, "_DATABASE_URL", "postgresql://test:test@localhost/test", create=True):
            pgs._get_conn()

        self.assertIn("pool exhausted", pgs._last_conn_error or "")
        self.assertEqual(pgs._conn_exhaustion_count, 1)


class TestPoolConstruction(unittest.TestCase):
    """The shared pool must be the thread-safe psycopg2 implementation."""

    def setUp(self):
        pgs._pool = None
        pgs._available = False
        pgs._last_conn_error = None
        pgs._conn_checkouts.clear()

    def test_lazy_pool_uses_threaded_class_with_existing_capacity(self):
        healthy = _mock_healthy_conn()
        created = {}

        def _threaded_pool(minconn, maxconn, database_url, **kwargs):
            created.update(
                minconn=minconn,
                maxconn=maxconn,
                database_url=database_url,
                kwargs=kwargs,
            )
            pool = MagicMock()
            pool.getconn.return_value = healthy
            return pool

        with patch.object(
            pgs, "_DATABASE_URL", "postgresql://test:test@localhost/test"
        ), patch(
            "psycopg2.pool.ThreadedConnectionPool",
            side_effect=_threaded_pool,
        ) as threaded_pool, patch(
            "psycopg2.pool.SimpleConnectionPool"
        ) as simple_pool:
            result = pgs._get_conn("pool-construction-test")

        self.assertIs(result, healthy)
        threaded_pool.assert_called_once()
        simple_pool.assert_not_called()
        self.assertEqual(created["minconn"], 1)
        self.assertEqual(created["maxconn"], 5)
        self.assertEqual(created["database_url"], "postgresql://test:test@localhost/test")
        self.assertEqual(created["kwargs"], {"connect_timeout": 10})


class TestWatchlistMetadataReadStatus(unittest.TestCase):
    """The list path must distinguish an empty DB from a failed DB read."""

    def _metadata_conn(self, rows):
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchall.return_value = rows
        conn.cursor.return_value = cur
        return conn, cur

    def test_successful_nonempty_metadata_uses_one_db_cycle(self):
        saved_at = __import__("datetime").datetime(2026, 8, 31, 12, 0, 0)
        updated_at = __import__("datetime").datetime(2026, 8, 31, 13, 0, 0)
        conn, cur = self._metadata_conn(
            [("primary", "Primary", 489, saved_at, updated_at)]
        )

        with patch.object(pgs, "_get_conn", return_value=conn) as get_conn, \
             patch.object(pgs, "_put_conn") as put_conn:
            result = pgs._watchlist_list_with_status()

        self.assertEqual(
            result,
            (
                True,
                [{
                    "id": "primary",
                    "name": "Primary",
                    "ticker_count": 489,
                    "saved_at": saved_at.isoformat(),
                    "updated_at": updated_at.isoformat(),
                }],
            ),
        )
        get_conn.assert_called_once()
        cur.execute.assert_called_once()
        cur.fetchall.assert_called_once_with()
        put_conn.assert_called_once_with(conn)

    def test_successful_empty_metadata_does_not_fallback(self):
        conn, cur = self._metadata_conn([])

        with patch.object(pgs, "_get_conn", return_value=conn), \
             patch.object(pgs, "_put_conn"), \
             patch.object(
                 watchlist_service,
                 "_read_store",
                 return_value={
                     "id": "file-only",
                     "name": "File fallback",
                     "tickers": ["FILE"],
                     "saved_at": "file-time",
                 },
             ) as read_store:
            result = watchlist_service.list_watchlists()

        self.assertEqual(result, [])
        read_store.assert_not_called()
        cur.execute.assert_called_once()

    def test_database_failure_preserves_existing_file_fallback(self):
        with patch.object(pgs, "_get_conn", return_value=None), \
             patch.object(
                 watchlist_service,
                 "_read_store",
                 return_value={
                     "id": "file-only",
                     "name": "File fallback",
                     "tickers": ["FILE"],
                     "saved_at": "file-time",
                 },
             ):
            result = watchlist_service.list_watchlists()

        self.assertEqual(
            result,
            [{
                "id": "file-only",
                "name": "File fallback",
                "ticker_count": 1,
                "saved_at": "file-time",
            }],
        )


if __name__ == "__main__":
    unittest.main()
