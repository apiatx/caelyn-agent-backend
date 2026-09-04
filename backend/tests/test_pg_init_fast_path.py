"""Regression coverage for startup schema initialization behavior."""

import ast
import importlib
import threading
import time
from pathlib import Path

import pytest


@pytest.fixture()
def pg_storage():
    module = importlib.import_module("data.pg_storage")
    with module._init_tables_condition:
        module._init_tables_running = False
        module._init_tables_succeeded = False
    yield module
    with module._init_tables_condition:
        module._init_tables_running = False
        module._init_tables_succeeded = False


class _Cursor:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class _Connection:
    def __init__(self, rows=None):
        self.cursor_instance = _Cursor(rows)
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _current_schema_rows(module):
    rows = [(table, "id") for table in module._INIT_REQUIRED_TABLES]
    rows.extend(module._INIT_REQUIRED_COLUMNS)
    return rows


def test_current_schema_skips_ddl(pg_storage, monkeypatch):
    conn = _Connection(_current_schema_rows(pg_storage))
    monkeypatch.setattr(pg_storage, "_get_conn", lambda caller="": conn)
    monkeypatch.setattr(pg_storage, "_put_conn", lambda _conn: None)

    assert pg_storage.init_tables() is True
    assert len(conn.cursor_instance.executed) == 1
    assert "information_schema.columns" in conn.cursor_instance.executed[0][0]
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_schema_drift_runs_existing_ddl_path(pg_storage, monkeypatch):
    conn = _Connection([])
    monkeypatch.setattr(pg_storage, "_get_conn", lambda caller="": conn)
    monkeypatch.setattr(pg_storage, "_put_conn", lambda _conn: None)
    monkeypatch.setattr(
        pg_storage, "_schema_current_status", lambda _conn: (False, ["table:watchlist"])
    )

    assert pg_storage.init_tables() is True
    statements = [sql for sql, _params in conn.cursor_instance.executed]
    assert any("CREATE TABLE IF NOT EXISTS public.watchlist" in sql for sql in statements)
    assert any("ALTER TABLE public.watchlist" in sql for sql in statements)
    assert conn.commits == 1


def test_repeated_successful_call_does_not_rerun(pg_storage, monkeypatch):
    calls = 0

    def run_once(_started):
        nonlocal calls
        calls += 1
        return True

    monkeypatch.setattr(pg_storage, "_init_tables_once", run_once)
    assert pg_storage.init_tables() is True
    assert pg_storage.init_tables() is True
    assert calls == 1


def test_concurrent_calls_are_single_flight(pg_storage, monkeypatch):
    calls = 0
    entered = threading.Event()
    release = threading.Event()

    def run_once(_started):
        nonlocal calls
        calls += 1
        entered.set()
        assert release.wait(2)
        return True

    monkeypatch.setattr(pg_storage, "_init_tables_once", run_once)
    results = []
    threads = [
        threading.Thread(target=lambda: results.append(pg_storage.init_tables()))
        for _ in range(2)
    ]
    for thread in threads:
        thread.start()
    assert entered.wait(1)
    time.sleep(0.05)
    assert calls == 1
    release.set()
    for thread in threads:
        thread.join(2)

    assert results == [True, True]
    assert calls == 1


def test_failed_migration_can_be_retried(pg_storage, monkeypatch):
    outcomes = iter([False, True])
    calls = 0

    def run_once(_started):
        nonlocal calls
        calls += 1
        return next(outcomes)

    monkeypatch.setattr(pg_storage, "_init_tables_once", run_once)
    assert pg_storage.init_tables() is False
    assert pg_storage.init_tables() is True
    assert calls == 2


def _function_calls(source, name):
    tree = ast.parse(source)
    node = next(
        item
        for item in ast.walk(tree)
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == name
    )
    return {
        call.func.id
        for call in ast.walk(node)
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
    }


def test_health_and_wait_paths_cannot_start_schema_mutation():
    source = (Path(__file__).parents[1] / "main.py").read_text()
    forbidden = {"_init_postgres_chat_storage_on_startup", "init_tables", "_pg_init"}
    assert _function_calls(source, "health").isdisjoint(forbidden)
    assert _function_calls(source, "health_check").isdisjoint(forbidden)
    assert _function_calls(source, "_wait_for_init").isdisjoint(forbidden)


def test_ping_has_no_database_dependency():
    source = (Path(__file__).parents[1] / "main.py").read_text()
    assert _function_calls(source, "ping").isdisjoint(
        {"_wait_for_init", "_init_postgres_chat_storage_on_startup", "init_tables"}
    )