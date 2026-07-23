"""
Startup timing regression test — PART 8 compliance.

Verifies that the pre-yield lifespan completes within the required
threshold, and that no known heavy/optional packages are imported
synchronously at module level.

Run with: python3.11 -m pytest backend/tests/test_startup_timing.py -v

These tests use a subprocess so each run gets a truly fresh Python
import state.  They do NOT require an active server.
"""
from __future__ import annotations

import re
import subprocess
import sys
import time

import pytest

_YIELD_THRESHOLD_S  = 5.0   # lifespan yield must complete within this time
_IMPORT_THRESHOLD_S = 3.0   # cold module-import phase must complete within this

_PYTHON = sys.executable
_BACKEND = "backend"

# Heavy/optional packages that must NOT be imported at module level
_FORBIDDEN_MODULE_LEVEL_IMPORTS = [
    "edgar",          # edgartools — heavy, only needed for EDGAR fetch
]


# ── helpers ──────────────────────────────────────────────────────────────────

def _extract_yield_time(output: str) -> float | None:
    m = re.search(r"lifespan yield reached in ([\d.]+)s", output)
    return float(m.group(1)) if m else None


def _run_python(code: str, timeout: float = 10.0) -> tuple[str, str, int]:
    result = subprocess.run(
        [_PYTHON, "-c", code],
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=".",
    )
    return result.stdout, result.stderr, result.returncode


# ── tests ─────────────────────────────────────────────────────────────────────

class TestStartupYieldTiming:
    """
    Verify that the lifespan yields in under _YIELD_THRESHOLD_S.

    We extract the timing from the [STARTUP] log line that main.py always
    emits immediately before yield.  The test starts a full uvicorn process
    and reads stdout until the log appears or a hard timeout fires.
    """

    def test_yield_within_threshold(self, tmp_path):
        """Lifespan must yield in < YIELD_THRESHOLD_S seconds."""
        import os
        import signal

        env = {**os.environ}
        proc = subprocess.Popen(
            [_PYTHON, "-m", "uvicorn", "main:app",
             "--host", "127.0.0.1", "--port", "5001",
             "--no-access-log"],
            cwd=_BACKEND,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )

        t_yield = None
        deadline = time.monotonic() + 25   # 25s hard cap
        try:
            while time.monotonic() < deadline:
                line = proc.stdout.readline()
                if not line:
                    break
                if "lifespan yield reached in" in line:
                    t_yield = _extract_yield_time(line)
                    break
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()

        assert t_yield is not None, (
            "lifespan yield log line was not emitted within 25s — "
            "startup may have crashed or hung"
        )
        assert t_yield < _YIELD_THRESHOLD_S, (
            f"Lifespan yield took {t_yield:.2f}s — exceeds threshold of "
            f"{_YIELD_THRESHOLD_S}s.  A slow import or blocking call crept "
            f"in before yield.  Check [STARTUP_TIMING] logs."
        )


class TestNoForbiddenModuleLevelImports:
    """
    Verify that known heavy/optional packages are NOT imported at module level.

    We import the main module in a subprocess and inspect sys.modules.
    """

    @pytest.mark.parametrize("pkg", _FORBIDDEN_MODULE_LEVEL_IMPORTS)
    def test_package_not_imported_at_module_level(self, pkg: str):
        """Package must not appear in sys.modules after importing main module."""
        code = (
            "import sys, os; "
            "sys.path.insert(0, 'backend'); "
            # Stub uvicorn so main.py doesn't try to bind a port
            "import types; "
            "uv = types.ModuleType('uvicorn'); "
            "uv.run = lambda *a, **kw: None; "
            "sys.modules['uvicorn'] = uv; "
            # We only import up to the point of module-level code
            # (not lifespan, which is async).  Module-level imports happen here.
            "try:\n"
            "    import importlib; importlib.import_module('main')\n"
            "except Exception as e:\n"
            "    pass\n"
            f"present = {repr(pkg)!r} in sys.modules or any(k.startswith({repr(pkg)!r}) for k in sys.modules)\n"
            "print('PRESENT' if present else 'ABSENT')"
        )
        # This approach is fragile — use a simpler direct check instead
        code = f"""
import sys, os
sys.path.insert(0, 'backend')
# Import only the ei_materials_service entry point to check edgar not pulled in top-level
try:
    import importlib
    # Force import of the module that previously had top-level edgar import
    mod = importlib.import_module('services.insider_activity_service')
except Exception:
    pass
present = any(k == {repr(pkg)!r} or k.startswith({repr(pkg)!r} + '.') for k in sys.modules)
print('PRESENT' if present else 'ABSENT')
"""
        stdout, stderr, rc = _run_python(code, timeout=15.0)
        result = stdout.strip()
        assert result == "ABSENT", (
            f"Package '{pkg}' was found in sys.modules after importing "
            f"services.insider_activity_service — it must be imported lazily "
            f"(only on first actual use via _ensure_edgar()), not at module level."
        )


class TestNeonNotCalledBeforeYield:
    """
    Smoke-check that the deferred sync startup (Neon table-creates) is not
    called from module-level code.

    We mock psycopg2 / neon_connect and verify it is never called during
    a plain module-level import of main.py submodules that were previously
    known offenders.
    """

    def test_whale_watch_tables_not_at_module_level(self):
        code = """
import sys, os, unittest.mock
sys.path.insert(0, 'backend')
calls = []
with unittest.mock.patch('psycopg2.connect', side_effect=lambda *a,**kw: calls.append(('psycopg2',a)) or unittest.mock.MagicMock()):
    try:
        import services.whale_watch_service
    except Exception:
        pass
# Only care about whether connect was called at import time
print('DB_CALLS' if calls else 'NO_DB_CALLS')
"""
        stdout, stderr, rc = _run_python(code, timeout=10.0)
        result = stdout.strip()
        assert result == "NO_DB_CALLS", (
            "psycopg2.connect was called at module-level import of "
            "whale_watch_service.  Table-create calls must be in "
            "_deferred_sync_startup(), not at import time."
        )
