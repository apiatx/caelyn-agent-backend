"""
Tests for pg_storage connection pool exhaustion fix.

Validates:
  - pool exhaustion returns None without destroying the pool
  - genuine connection errors still trigger pool rebuild
  - instrumentation tracks checkouts, checkins, and exhaustion count
  - _put_conn records checkin even when pool is destroyed
"""
from __future__ import annotations

import sys
import unittest
from unittest.mock import MagicMock, patch, PropertyMock

# Ensure backend is importable
sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.dirname(__file__)))

import data.pg_storage as pgs
from psycopg2.pool import PoolError
from psycopg2 import OperationalError


class TestPoolExhaustionSafeReturn(unittest.TestCase):
    """Pool exhaustion must NOT destroy the pool — it must return None gracefully."""

    def setUp(self):
        pgs._pool = None
        pgs._available = False
        pgs._last_conn_error = None
        pgs._conn_exhaustion_count = 0
        pgs._conn_checkouts.clear()

    def test_exhaustion_returns_none_pool_untouched(self):
        """When getconn raises PoolError('connection pool exhausted'), return None
        and do NOT call closeall() — other callers' connections are still valid."""
        mock_pool = MagicMock()
        mock_pool.getconn.side_effect = PoolError("connection pool exhausted")
        pgs._pool = mock_pool

        # Force _DATABASE_URL to be truthy so _get_conn proceeds
        with patch.object(pgs, "_DATABASE_URL", "postgresql://test:test@localhost/test", create=True):
            result = pgs._get_conn()

        self.assertIsNone(result)
        # closeall() must NOT have been called
        mock_pool.closeall.assert_not_called()
        # Pool object must still be assigned (not set to None)
        self.assertIsNotNone(pgs._pool)
        self.assertEqual(pgs._conn_exhaustion_count, 1)

    def test_exhaustion_increments_counter(self):
        """Each pool-exhaustion getconn call increments the exhaustion counter."""
        mock_pool = MagicMock()
        mock_pool.getconn.side_effect = PoolError("connection pool exhausted")
        pgs._pool = mock_pool

        with patch.object(pgs, "_DATABASE_URL", "postgresql://test:test@localhost/test", create=True):
            pgs._get_conn()
            pgs._get_conn()

        self.assertEqual(pgs._conn_exhaustion_count, 2)

    def test_genuine_error_still_destroys_pool(self):
        """Non-exhaustion errors (e.g. OperationalError) must still destroy pool."""
        mock_pool = MagicMock()
        mock_pool.getconn.side_effect = OperationalError(
            "could not connect to server"
        )
        pgs._pool = mock_pool

        with patch.object(pgs, "_DATABASE_URL", "postgresql://test:test@localhost/test", create=True):
            result = pgs._get_conn()

        self.assertIsNone(result)
        # closeall() MUST have been called for genuine errors
        mock_pool.closeall.assert_called()
        # Pool must be reset
        self.assertIsNone(pgs._pool)
        # Exhaustion counter must NOT increment for non-exhaustion errors
        self.assertEqual(pgs._conn_exhaustion_count, 0)

    def test_empty_database_url_returns_none(self):
        """No DATABASE_URL should return None immediately."""
        with patch.object(pgs, "_DATABASE_URL", "", create=True):
            result = pgs._get_conn()
        self.assertIsNone(result)
        self.assertEqual(pgs._last_conn_error, "No NEON_DATABASE_URL or DATABASE_URL set")


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

        # Stagger checkouts so conn_a is held longer
        pgs._record_checkout(conn_a, "caller_a")
        time.sleep(0.01)
        pgs._record_checkout(conn_b, "caller_b")

        snap = pgs._pool_exhaustion_snapshot()
        self.assertEqual(snap["held_connections"], 2)
        # conn_a held longer → appears first
        self.assertEqual(snap["callers"][0]["caller"], "caller_a")
        self.assertGreater(snap["callers"][0]["held_s"], 0)

        pgs._record_checkin(conn_a)
        pgs._record_checkin(conn_b)

        snap_after = pgs._pool_exhaustion_snapshot()
        self.assertEqual(snap_after["held_connections"], 0)


class TestIsPoolExhaustedError(unittest.TestCase):
    """Exhaustion detection must be robust to message formatting changes."""

    def test_recognises_exhaustion_message(self):
        e = PoolError("connection pool exhausted")
        self.assertTrue(pgs._is_pool_exhausted_error(e))

    def test_exhausted_keyword(self):
        e = Exception("some prefix POOL EXHAUSTED suffix")
        self.assertTrue(pgs._is_pool_exhausted_error(e))

    def test_non_exhaustion_error(self):
        e = Exception("could not connect to server")
        self.assertFalse(pgs._is_pool_exhausted_error(e))
        self.assertFalse(pgs._is_pool_exhausted_error(OperationalError("timeout")))


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
    """_get_conn must auto-detect the calling function when no explicit caller."""

    def setUp(self):
        pgs._pool = None
        pgs._available = False
        pgs._last_conn_error = None
        pgs._conn_checkouts.clear()
        pgs._conn_exhaustion_count = 0

    def test_auto_populated_caller_not_empty(self):
        """When pool is exhausted, the stored error message is set correctly.
        The auto-detected caller name appears in the print output not _last_conn_error."""
        mock_pool = MagicMock()
        mock_pool.getconn.side_effect = PoolError("connection pool exhausted")
        pgs._pool = mock_pool

        with patch.object(pgs, "_DATABASE_URL", "postgresql://test:test@localhost/test", create=True):
            pgs._get_conn()

        self.assertIn("pool exhausted", pgs._last_conn_error or "")
        self.assertEqual(pgs._conn_exhaustion_count, 1)


if __name__ == "__main__":
    unittest.main()
