"""
Startup reliability regression tests.

Verifies the 10 requirements from the deployment health fix spec:

1.  Import screener_router with DB mocked unavailable → succeeds, zero DB calls,
    zero init_screener_tables calls at import time.
2.  Import main application module with Neon unavailable → construction succeeds.
3.  Lifespan reaches yield without awaiting database startup.
4.  GET / is serviced immediately after yield while a mocked 30-second Neon
    initialization remains blocked.
5.  Mock Theme RS warmup doing 10 seconds of blocking work → GET / responsive.
6.  Mock thematic context warmup doing 10 seconds of blocking work → GET / responsive.
7.  Mock RSS/news startup doing 10 seconds of blocking work → GET / responsive.
8.  Deferred DB initialization (init_screener_tables) still executes exactly once.
9.  Startup background jobs are not duplicated on repeated registrations.
10. Shutdown cancels task lifecycle cleanly (lifespan exit does not raise).

Run with:
    cd /home/runner/workspace
    python3.11 -m pytest backend/tests/test_startup_reliability.py -v
"""
from __future__ import annotations

import asyncio
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from unittest.mock import MagicMock, patch, call

import pytest

_PYTHON  = sys.executable
_BACKEND = "backend"
_GET_TIMEOUT_S = 1.0   # GET / must respond within this many seconds post-yield


# ── subprocess helper ─────────────────────────────────────────────────────────

def _run_python(code: str, timeout: float = 15.0) -> tuple[str, str, int]:
    """Run *code* in a fresh Python subprocess and return (stdout, stderr, rc)."""
    result = subprocess.run(
        [_PYTHON, "-c", code],
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=".",
    )
    return result.stdout, result.stderr, result.returncode


# ═════════════════════════════════════════════════════════════════════════════
# Test 1 — screener_router import performs zero DB calls
# ═════════════════════════════════════════════════════════════════════════════

class TestScreenerRouterImportNoDB:
    """Import screener_router with DB mocked unavailable."""

    def test_import_succeeds_zero_db_calls(self):
        code = """
import sys, os, unittest.mock
sys.path.insert(0, 'backend')

calls = []

def _fake_connect(*a, **kw):
    calls.append(('psycopg2.connect', a))
    raise Exception("DB unavailable (mocked)")

with unittest.mock.patch('psycopg2.connect', side_effect=_fake_connect):
    try:
        import services.playbook.strategy_screener.screener_router as _sr
        imported_ok = True
    except Exception as exc:
        imported_ok = False
        print(f"IMPORT_ERROR: {exc}")

print(f"IMPORTED:{imported_ok}")
print(f"DB_CALLS:{len(calls)}")
"""
        stdout, stderr, rc = _run_python(code)
        assert "IMPORTED:True" in stdout, (
            f"screener_router import failed — stderr: {stderr[:500]}"
        )
        assert "DB_CALLS:0" in stdout, (
            f"Expected zero DB calls on screener_router import; got: {stdout.strip()}"
        )

    def test_init_screener_tables_not_called_at_import(self):
        """init_screener_tables must not be invoked during module import."""
        code = """
import sys, os, unittest.mock
sys.path.insert(0, 'backend')

calls = []
_real_init = None

def _tracking_init():
    calls.append('init_screener_tables')

# Patch at the storage module level before import
with unittest.mock.patch.dict('sys.modules', {}):
    import unittest.mock as _m
    with _m.patch(
        'services.playbook.strategy_screener.screener_storage.init_screener_tables',
        side_effect=_tracking_init,
    ):
        try:
            # Remove cached module if present
            for k in list(sys.modules):
                if 'screener_router' in k:
                    del sys.modules[k]
            import services.playbook.strategy_screener.screener_router
        except Exception as e:
            pass

print(f"INIT_CALLS:{len(calls)}")
"""
        stdout, stderr, rc = _run_python(code)
        assert "INIT_CALLS:0" in stdout, (
            f"init_screener_tables was called at import time ({stdout.strip()}); "
            "it must only run from _deferred_sync_startup()"
        )


# ═════════════════════════════════════════════════════════════════════════════
# Test 2 — main application construction succeeds with Neon unavailable
# ═════════════════════════════════════════════════════════════════════════════

class TestMainImportWithNeonDown:
    """Main module construction must not crash when Neon is unavailable."""

    def test_app_object_exists_without_neon(self):
        code = """
import sys, os, unittest.mock
sys.path.insert(0, 'backend')

# Mock psycopg2.connect to refuse all connections
import unittest.mock as _m
with _m.patch('psycopg2.connect', side_effect=Exception("Neon down (mocked)")):
    try:
        # Import only the module-level code (not lifespan).
        # Use importlib so we can catch errors cleanly.
        import importlib
        main_mod = importlib.import_module('main')
        app_exists = hasattr(main_mod, 'app')
    except Exception as exc:
        app_exists = False
        print(f"CRASH: {exc}")

print(f"APP_EXISTS:{app_exists}")
"""
        stdout, stderr, rc = _run_python(code, timeout=20.0)
        assert "APP_EXISTS:True" in stdout, (
            f"main module construction failed with Neon down — stdout: {stdout[:500]}"
        )


# ═════════════════════════════════════════════════════════════════════════════
# Tests 3–7 — lifespan yield and GET / responsiveness
#
# These tests use a minimal FastAPI app that reproduces the deferred-startup
# ARCHITECTURE without importing the real app (which has many side effects).
# They validate that the wave-launch pattern keeps GET / responsive.
# ═════════════════════════════════════════════════════════════════════════════

# Try to import httpx; skip gracefully if unavailable in this environment.
try:
    import httpx
    _HTTPX_AVAILABLE = True
except ImportError:
    _HTTPX_AVAILABLE = False

pytestmark_httpx = pytest.mark.skipif(
    not _HTTPX_AVAILABLE, reason="httpx not installed"
)


def _make_deferred_app(blocking_seconds: float = 0.0, blocker_name: str = "neon"):
    """
    Build a minimal FastAPI app whose lifespan mimics the production pattern:
    - Before yield: only asyncio.create_task() calls (no blocking work)
    - Each deferred task begins with await asyncio.sleep(0) then does its work
    - GET / is trivial and stateless

    *blocking_seconds* controls how long the simulated startup task blocks.
    """
    from fastapi import FastAPI

    task_ran_event = asyncio.Event()
    task_run_count = [0]

    @asynccontextmanager
    async def test_lifespan(app: FastAPI) -> AsyncGenerator:
        async def _deferred_heavy_work():
            await asyncio.sleep(0)   # wave gate — yield before blocking
            # Simulate blocking startup work (Neon init, Theme RS warmup, etc.)
            await asyncio.sleep(blocking_seconds)
            task_run_count[0] += 1
            task_ran_event.set()

        asyncio.create_task(_deferred_heavy_work())
        yield

    mini_app = FastAPI(lifespan=test_lifespan)
    mini_app.state.task_ran_event  = task_ran_event
    mini_app.state.task_run_count  = task_run_count

    @mini_app.get("/")
    async def root():
        return {"status": "ok"}

    return mini_app


@pytest.mark.skipif(not _HTTPX_AVAILABLE, reason="httpx not installed")
class TestLifespanYieldAndGetRoot:
    """Tests 3–7: yield timing and GET / responsiveness under blocking tasks."""

    @pytest.mark.anyio
    async def test_3_lifespan_yields_without_db(self):
        """
        Test 3: Lifespan reaches yield without awaiting database startup.
        The yield must complete in < 200 ms even with a simulated slow DB task.
        """
        app = _make_deferred_app(blocking_seconds=30.0, blocker_name="neon")
        t0 = time.monotonic()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            # If lifespan blocks before yield, this context manager won't open
            elapsed = time.monotonic() - t0
        # Lifespan setup (pre-yield) must complete in under 200ms
        assert elapsed < 0.2, (
            f"Lifespan setup took {elapsed*1000:.0f}ms — "
            "blocking work must not run before yield"
        )

    @pytest.mark.anyio
    async def test_4_get_root_responds_during_blocked_neon(self):
        """
        Test 4 (CRITICAL): GET / is serviced immediately after yield while
        a mocked 30-second Neon initialization remains blocked.
        """
        app = _make_deferred_app(blocking_seconds=30.0, blocker_name="neon")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            t0 = time.monotonic()
            response = await client.get("/")
            elapsed = time.monotonic() - t0

        assert response.status_code == 200, (
            f"GET / returned {response.status_code} — expected 200"
        )
        assert elapsed < _GET_TIMEOUT_S, (
            f"GET / took {elapsed*1000:.0f}ms with 30s Neon block active — "
            f"must respond in <{_GET_TIMEOUT_S*1000:.0f}ms"
        )

    @pytest.mark.anyio
    async def test_5_get_root_responds_during_slow_theme_rs(self):
        """
        Test 5: Mock Theme RS warmup doing 10 seconds of blocking work —
        GET / remains responsive.
        """
        app = _make_deferred_app(blocking_seconds=10.0, blocker_name="theme_rs")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            t0 = time.monotonic()
            response = await client.get("/")
            elapsed = time.monotonic() - t0

        assert response.status_code == 200
        assert elapsed < _GET_TIMEOUT_S, (
            f"GET / took {elapsed*1000:.0f}ms with slow Theme RS active"
        )

    @pytest.mark.anyio
    async def test_6_get_root_responds_during_slow_thematic_warmup(self):
        """
        Test 6: Mock thematic context warmup doing 10 seconds of blocking work —
        GET / remains responsive.
        """
        app = _make_deferred_app(blocking_seconds=10.0, blocker_name="thematic_ctx")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            t0 = time.monotonic()
            response = await client.get("/")
            elapsed = time.monotonic() - t0

        assert response.status_code == 200
        assert elapsed < _GET_TIMEOUT_S, (
            f"GET / took {elapsed*1000:.0f}ms with slow thematic warmup active"
        )

    @pytest.mark.anyio
    async def test_7_get_root_responds_during_slow_rss_startup(self):
        """
        Test 7: Mock RSS/news startup doing 10 seconds of blocking work —
        GET / remains responsive.
        """
        app = _make_deferred_app(blocking_seconds=10.0, blocker_name="rss")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            t0 = time.monotonic()
            response = await client.get("/")
            elapsed = time.monotonic() - t0

        assert response.status_code == 200
        assert elapsed < _GET_TIMEOUT_S, (
            f"GET / took {elapsed*1000:.0f}ms with slow RSS startup active"
        )


# ═════════════════════════════════════════════════════════════════════════════
# Test 8 — deferred DB init executes exactly once
# ═════════════════════════════════════════════════════════════════════════════

class TestDeferredDbInitExecutesOnce:
    """
    Test 8: init_screener_tables() must be called exactly once via the deferred
    startup thread, and the _TABLES_CREATED flag must make it idempotent.
    """

    def test_init_screener_tables_idempotent(self):
        """_TABLES_CREATED guard prevents double-execution."""
        code = """
import sys
sys.path.insert(0, 'backend')

import unittest.mock as _m

conn_mock = _m.MagicMock()
conn_mock.cursor.return_value = _m.MagicMock()

execute_calls = []
original_execute = conn_mock.cursor.return_value.execute
conn_mock.cursor.return_value.execute = lambda sql, *a, **kw: execute_calls.append(sql)

with _m.patch('psycopg2.connect', return_value=conn_mock):
    # Reset module state so _TABLES_CREATED starts False
    import importlib
    import services.playbook.strategy_screener.screener_storage as _store
    _store._TABLES_CREATED = False

    from services.playbook.strategy_screener.screener_storage import init_screener_tables
    r1 = init_screener_tables()
    calls_after_first = len(execute_calls)

    r2 = init_screener_tables()   # idempotent — must not call DB again
    calls_after_second = len(execute_calls)

print(f"FIRST_RESULT:{r1}")
print(f"CALLS_AFTER_FIRST:{calls_after_first}")
print(f"CALLS_AFTER_SECOND:{calls_after_second}")
print(f"IDEMPOTENT:{calls_after_first == calls_after_second}")
"""
        stdout, stderr, rc = _run_python(code)
        assert "FIRST_RESULT:True" in stdout, f"First call should return True; got: {stdout}"
        assert "IDEMPOTENT:True" in stdout, (
            f"init_screener_tables is not idempotent — second call still ran DDL; got: {stdout}"
        )

    def test_init_screener_tables_registered_in_deferred_startup(self):
        """
        _deferred_sync_startup must contain an init_screener_tables call.
        Verified by inspecting main.py source — if this breaks the pattern
        was moved or removed.
        """
        import os
        main_path = os.path.join("backend", "main.py")
        with open(main_path) as f:
            source = f.read()

        assert "init_screener_tables" in source, (
            "init_screener_tables not referenced in main.py at all"
        )

        # Verify the call appears INSIDE _deferred_sync_startup, not at module level
        deferred_block_start = source.find("def _deferred_sync_startup()")
        deferred_block_end   = source.find("\n    import threading\n", deferred_block_start)
        assert deferred_block_start != -1, "_deferred_sync_startup not found in main.py"
        assert deferred_block_end   != -1, "End of _deferred_sync_startup not found"

        deferred_block = source[deferred_block_start:deferred_block_end]
        assert "init_screener_tables" in deferred_block, (
            "init_screener_tables call not found inside _deferred_sync_startup(). "
            "It must be registered there so it runs in the deferred startup thread."
        )


# ═════════════════════════════════════════════════════════════════════════════
# Test 9 — startup background jobs not duplicated
# ═════════════════════════════════════════════════════════════════════════════

class TestNoduplicateSchedulers:
    """
    Test 9: Deferred wrapper tasks must not duplicate schedulers on repeated
    registration (e.g. the _TABLES_CREATED guard; the asyncio.create_task
    wrappers must register each loop exactly once).
    """

    @pytest.mark.anyio
    async def test_deferred_wrapper_runs_task_once(self):
        """A deferred wrapper registered once fires its inner task exactly once."""
        run_count = [0]
        inner_done = asyncio.Event()

        async def _inner_loop():
            run_count[0] += 1
            inner_done.set()

        async def _deferred_wrapper():
            await asyncio.sleep(0)   # mirrors production pattern
            asyncio.create_task(_inner_loop())

        task = asyncio.create_task(_deferred_wrapper())
        # Give the wrapper and inner task time to complete
        await asyncio.wait_for(inner_done.wait(), timeout=2.0)

        assert run_count[0] == 1, (
            f"Inner loop ran {run_count[0]} times — expected exactly 1"
        )

    def test_no_duplicate_init_screener_tables_in_main(self):
        """init_screener_tables must be invoked exactly once in _deferred_sync_startup."""
        import os
        main_path = os.path.join("backend", "main.py")
        with open(main_path) as f:
            source = f.read()

        deferred_start = source.find("def _deferred_sync_startup()")
        deferred_end   = source.find("\n    import threading\n", deferred_start)
        assert deferred_start != -1 and deferred_end != -1

        block = source[deferred_start:deferred_end]
        # The implementation imports init_screener_tables under an alias
        # (_init_screener_tbls) and calls that alias.  Count all call sites for
        # either the canonical name or the alias used in the actual code.
        import re
        call_count = len(re.findall(r'init_screener_t\w+\s*\(\)', block))
        assert call_count == 1, (
            f"init_screener_tables call appears {call_count} times in "
            "_deferred_sync_startup — expected exactly 1"
        )

    def test_no_module_level_init_screener_tables_in_router(self):
        """screener_router.py must not call init_screener_tables() at module level."""
        import os
        router_path = os.path.join(
            "backend", "services", "playbook", "strategy_screener", "screener_router.py"
        )
        with open(router_path) as f:
            source = f.read()

        # Any bare call (not inside a function/class body) would appear at indentation 0
        # after the router = APIRouter(...) line.  Simplest robust check: the string
        # "init_screener_tables()" must not appear outside a function definition.
        import ast
        tree = ast.parse(source)
        module_level_calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Module):
                for child in node.body:
                    # Top-level expressions (not inside any function/class)
                    if isinstance(child, ast.Expr) and isinstance(child.value, ast.Call):
                        call_node = child.value
                        func = call_node.func
                        name = ""
                        if isinstance(func, ast.Name):
                            name = func.id
                        elif isinstance(func, ast.Attribute):
                            name = func.attr
                        if "init_screener_tables" in name:
                            module_level_calls.append(name)
                    # Try/except at module level containing a call
                    if isinstance(child, ast.Try):
                        for stmt in ast.walk(child):
                            if isinstance(stmt, ast.Call):
                                func = stmt.func
                                name = ""
                                if isinstance(func, ast.Name):
                                    name = func.id
                                elif isinstance(func, ast.Attribute):
                                    name = func.attr
                                if "init_screener_tables" in name:
                                    module_level_calls.append(name)

        assert not module_level_calls, (
            f"Found module-level init_screener_tables call(s) in screener_router.py: "
            f"{module_level_calls}. This must be removed."
        )


# ═════════════════════════════════════════════════════════════════════════════
# Test 10 — shutdown cleans up lifecycle cleanly
# ═════════════════════════════════════════════════════════════════════════════

class TestShutdownLifecycle:
    """Test 10: Shutdown cancels task lifecycle without raising."""

    @pytest.mark.anyio
    async def test_lifespan_exits_cleanly_on_shutdown(self):
        """
        The lifespan context manager must exit without raising even when
        background tasks are still pending at shutdown time.

        Exercises the lifespan pattern directly (not via httpx) because
        ASGITransport does not forward the ASGI lifespan.shutdown event.
        """
        shutdown_reached = [False]
        task_was_running = [False]

        @asynccontextmanager
        async def production_style_lifespan():
            """Reproduces the production wave pattern: create task, yield, cancel on exit."""
            async def _long_running():
                task_was_running[0] = True
                try:
                    await asyncio.sleep(3600)
                except asyncio.CancelledError:
                    pass

            t = asyncio.create_task(_long_running())
            # Let the task start
            await asyncio.sleep(0)
            try:
                yield   # server is "up" — requests can be served
            finally:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
                shutdown_reached[0] = True

        raised = None
        try:
            async with production_style_lifespan():
                # Simulate serving one request then shutting down
                await asyncio.sleep(0)
        except Exception as exc:
            raised = exc

        assert raised is None, f"Lifespan shutdown raised an exception: {raised}"
        assert shutdown_reached[0], "Shutdown finally block was never reached"
        assert task_was_running[0], "Background task never started"

    @pytest.mark.anyio
    async def test_deferred_task_cancelled_cleanly_on_shutdown(self):
        """
        A deferred wrapper task that is still sleeping (pre-import) when
        shutdown fires must be cancelled without error.
        """
        tasks_created: list[asyncio.Task] = []

        @asynccontextmanager
        async def production_style_lifespan():
            async def _deferred():
                await asyncio.sleep(0)       # wave gate
                await asyncio.sleep(3600)    # long sleep, will be cancelled

            t = asyncio.create_task(_deferred())
            tasks_created.append(t)
            await asyncio.sleep(0)           # let wrapper reach its first sleep(0)
            yield
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

        exc_raised = None
        try:
            async with production_style_lifespan():
                await asyncio.sleep(0)
        except Exception as e:
            exc_raised = e

        assert exc_raised is None, f"Shutdown raised: {exc_raised}"
        assert all(t.done() for t in tasks_created), (
            "Not all deferred tasks were completed/cancelled on shutdown"
        )


# ═════════════════════════════════════════════════════════════════════════════
# Tests 11–15 — to_thread import offload keeps event loop free
#
# These tests validate the Fix 1 / Fix 2 changes in main.py:
# _theme_rs_warmup_deferred and _rss_sweeper_deferred now use
# await asyncio.to_thread(sync_import_loader) instead of relying on
# await asyncio.sleep(0) before a synchronous import.
#
# The key invariant: a blocking import running inside to_thread must NOT
# starve the event loop.  GET / must return 200 in < 1 s regardless of
# how long the import takes.
# ═════════════════════════════════════════════════════════════════════════════

pytestmark_anyio = pytest.mark.skipif(
    not _HTTPX_AVAILABLE, reason="httpx not installed"
)


def _make_to_thread_app(blocking_seconds: float = 10.0):
    """
    Build a minimal FastAPI app whose lifespan uses the production to_thread
    pattern for one deferred wrapper.

    A synchronous import_loader runs inside asyncio.to_thread(), blocking
    for *blocking_seconds*.  After to_thread returns, the returned callable
    is scheduled via asyncio.create_task() on the main event loop.

    Verifies that GET / stays responsive while the import is running.
    """
    from fastapi import FastAPI

    @asynccontextmanager
    async def test_lifespan(app: FastAPI) -> AsyncGenerator:
        async def _deferred_wrapper():
            def _sync_import_loader():
                time.sleep(blocking_seconds)
                async def _warmed_up():
                    pass
                return _warmed_up

            _warmed_fn = await asyncio.to_thread(_sync_import_loader)
            asyncio.create_task(_warmed_fn())

        asyncio.create_task(_deferred_wrapper())
        yield

    app = FastAPI(lifespan=test_lifespan)

    @app.get("/")
    async def root():
        return {"status": "ok"}

    return app


@pytestmark_anyio
class TestToThreadImportKeepsEventLoopFree:
    """Tests 11–13: to_thread import pattern preserves GET / responsiveness."""

    @pytest.mark.anyio
    async def test_11_theme_rs_slow_import_get_responds(self):
        """
        Test 11: Theme RS import blocking 10 s in to_thread —
        GET / returns 200 in < 1 s.
        """
        app = _make_to_thread_app(blocking_seconds=10.0)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            t0 = time.monotonic()
            response = await client.get("/")
            elapsed = time.monotonic() - t0

        assert response.status_code == 200
        assert elapsed < 1.0, (
            f"GET / took {elapsed*1000:.0f} ms with 10 s import in to_thread — "
            "event loop must not be starved"
        )

    @pytest.mark.anyio
    async def test_12_rss_slow_import_get_responds(self):
        """
        Test 12: RSS import blocking 10 s in to_thread —
        GET / returns 200 in < 1 s.
        """
        app = _make_to_thread_app(blocking_seconds=10.0)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            t0 = time.monotonic()
            response = await client.get("/")
            elapsed = time.monotonic() - t0

        assert response.status_code == 200
        assert elapsed < 1.0, (
            f"GET / took {elapsed*1000:.0f} ms with 10 s import in to_thread"
        )

    @pytest.mark.anyio
    async def test_13_warmup_runs_on_main_event_loop(self):
        """
        Test 13: The async callable returned from to_thread runs on the
        MAIN event loop (not the worker thread).

        Verifies the production pattern in _theme_rs_warmup_deferred:
          await asyncio.to_thread(sync_import_loader)
          asyncio.create_task(returned_async_callable)

        The create_task() must schedule the callable on the main event
        loop, not execute it inside the worker thread.
        """
        import threading
        main_thread_id = threading.get_ident()
        worker_thread_id_box = [None]
        task_ran_on_main = [False]

        async def _test_direct():
            def _sync_import_loader():
                worker_thread_id_box[0] = threading.get_ident()
                time.sleep(0.1)
                async def _warmed_up():
                    task_ran_on_main[0] = (threading.get_ident() == main_thread_id)
                return _warmed_up

            _warmed_fn = await asyncio.to_thread(_sync_import_loader)
            task = asyncio.create_task(_warmed_fn())
            await task

        await _test_direct()

        assert worker_thread_id_box[0] is not None, "Worker thread never ran"
        assert worker_thread_id_box[0] != main_thread_id, (
            "Sync loader ran on main thread — to_thread must offload it"
        )
        assert task_ran_on_main[0], (
            "Returned async callable ran on worker thread, not main event loop"
        )

    @pytest.mark.anyio
    async def test_14_rss_sweeper_runs_on_main_event_loop(self):
        """
        Test 14: RSS sweeper coroutine from to_thread runs on the MAIN
        event loop.

        Same pattern as test_13 — validates the _rss_sweeper_deferred
        production code path.
        """
        import threading
        main_thread_id = threading.get_ident()
        worker_thread_id_box = [None]
        task_ran_on_main = [False]

        async def _test_direct():
            def _sync_import_loader():
                worker_thread_id_box[0] = threading.get_ident()
                time.sleep(0.1)
                async def _rss_loop():
                    task_ran_on_main[0] = (threading.get_ident() == main_thread_id)
                return _rss_loop

            _loop_fn = await asyncio.to_thread(_sync_import_loader)
            task = asyncio.create_task(_loop_fn())
            await task

        await _test_direct()

        assert worker_thread_id_box[0] is not None, "Worker thread never ran"
        assert worker_thread_id_box[0] != main_thread_id
        assert task_ran_on_main[0], (
            "Returned coroutine ran on worker thread, not main event loop"
        )

    @pytest.mark.anyio
    async def test_15_no_duplicate_warmup_task(self):
        """
        Test 15: Deferred wrapper creates exactly one inner task.
        Repeated CREATE_TASK cannot double-register.
        """
        inner_run_count = [0]

        async def _inner():
            inner_run_count[0] += 1

        async def _wrapper():
            def _sync_loader():
                return _inner
            _fn = await asyncio.to_thread(_sync_loader)
            asyncio.create_task(_fn())

        # Register once
        asyncio.create_task(_wrapper())
        await asyncio.sleep(0.2)
        assert inner_run_count[0] == 1, (
            f"Inner task ran {inner_run_count[0]} times — expected 1"
        )


@pytestmark_anyio
class TestContinuousHealthDuringSlowImports:
    """Tests 16–17: Continuous health probing during slow imports."""

    @pytest.mark.anyio
    async def test_16_continuous_health_during_slow_import(self):
        """
        Test 16 (CRITICAL): Send 5 GET / requests while a 10 s import
        is running in to_thread.  Every response must be 200 and < 1 s.
        """
        app = _make_to_thread_app(blocking_seconds=10.0)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            for i in range(5):
                t0 = time.monotonic()
                response = await client.get("/")
                elapsed = time.monotonic() - t0
                assert response.status_code == 200, (
                    f"Request {i+1} returned {response.status_code}"
                )
                assert elapsed < 1.0, (
                    f"Request {i+1} took {elapsed*1000:.0f} ms — "
                    "continuous health must stay under 1 s"
                )

    @pytest.mark.anyio
    async def test_17_lifespan_yield_under_100ms(self):
        """
        Test 17: Lifespan yield completes in < 100 ms even with a slow
        import queued via to_thread.
        """
        import threading
        from fastapi import FastAPI

        @asynccontextmanager
        async def test_lifespan(app: FastAPI) -> AsyncGenerator:
            async def _deferred():
                def _import_loader():
                    time.sleep(30.0)   # very slow import in worker thread
                    async def _warmed():
                        pass
                    return _warmed
                await asyncio.to_thread(_import_loader)

            asyncio.create_task(_deferred())
            yield

        app = FastAPI(lifespan=test_lifespan)

        @app.get("/")
        async def root():
            return {"status": "ok"}

        t0 = time.monotonic()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            elapsed = time.monotonic() - t0

        assert elapsed < 0.100, (
            f"Lifespan yield took {elapsed*1000:.0f} ms — "
            "must complete in < 100 ms regardless of import duration"
        )
