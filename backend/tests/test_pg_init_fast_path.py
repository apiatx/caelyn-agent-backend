import sys
import threading
import time
from pathlib import Path

import pytest


BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from data import pg_storage


class CatalogCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class CatalogConnection:
    def __init__(self, rows):
        self.catalog_cursor = CatalogCursor(rows)
        self.rollback_count = 0

    def cursor(self):
        return self.catalog_cursor

    def rollback(self):
        self.rollback_count += 1


@pytest.fixture(autouse=True)
def reset_init_state(monkeypatch):
    monkeypatch.setattr(pg_storage, "_init_tables_running", False)
    monkeypatch.setattr(pg_storage, "_init_tables_succeeded", False)


def _current_rows():
    rows = []
    for table in pg_storage._INIT_REQUIRED_TABLES:
        columns = ["id"]
        columns.extend(
            column
            for required_table, column in pg_storage._INIT_REQUIRED_COLUMNS
            if required_table == table
        )
        rows.extend((table, column) for column in columns)
    return rows


def test_current_schema_skips_historical_ddl(monkeypatch):
    conn = CatalogConnection(_current_rows())
    monkeypatch.setattr(pg_storage, "_get_conn", lambda: conn)
    monkeypatch.setattr(pg_storage, "_put_conn", lambda _conn: None)

    assert pg_storage.init_tables() is True
    assert conn.rollback_count == 1
    assert len(conn.catalog_cursor.executed) == 1
    assert "information_schema.columns" in conn.catalog_cursor.executed[0][0]


def test_success_is_cached_in_process(monkeypatch):
    calls = []
    monkeypatch.setattr(
        pg_storage,
        "_init_tables_once",
        lambda started=None: calls.append(started) or True,
    )
    assert pg_storage.init_tables() is True
    assert pg_storage.init_tables() is True
    assert len(calls) == 1


def test_failed_attempt_remains_retryable(monkeypatch):
    results = iter([False, True])
    monkeypatch.setattr(
        pg_storage,
        "_init_tables_once",
        lambda started=None: next(results),
    )
    assert pg_storage.init_tables() is False
    assert pg_storage.init_tables() is True


def test_concurrent_calls_are_single_flight(monkeypatch):
    calls = 0
    calls_lock = threading.Lock()

    def init_once(started=None):
        nonlocal calls
        with calls_lock:
            calls += 1
        time.sleep(0.05)
        return True

    monkeypatch.setattr(pg_storage, "_init_tables_once", init_once)
    results = []
    threads = [
        threading.Thread(target=lambda: results.append(pg_storage.init_tables()))
        for _ in range(5)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert results == [True] * 5
    assert calls == 1


def test_health_wait_path_cannot_start_schema_init():
    source = (BACKEND / "main.py").read_text()
    start = source.index("async def _wait_for_init():")
    end = source.index('@app.get("/")', start)
    assert "_init_postgres_chat_storage_on_startup" not in source[start:end]
