#!/usr/bin/env python3.11
"""
Cold-start timing test — PART 3 compliance.

Measures the time from process launch through FastAPI lifespan yield,
simulating a true cold start (fresh Python process, no warm imports).

Usage:
    python3.11 backend/tools/cold_start_test.py

Runs 3 trials.  Each trial:
  1. Kills any existing process on port 5000
  2. Starts a fresh uvicorn process
  3. Polls GET /health until it responds (max 30s)
  4. Records: import phase, pre-yield phase, health-first-success time
  5. Kills the child process

Requirements: the server must be stopped before running this script.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import time

import httpx

_TRIALS      = 3
_POLL_EVERY  = 0.25   # seconds between /health probes
_MAX_WAIT_S  = 30     # hard timeout per trial

_HEALTH_URL  = "http://localhost:5000/health"
_CMD = [
    "python3.11", "-m", "uvicorn",
    "main:app",
    "--host", "0.0.0.0",
    "--port", "5000",
    "--no-access-log",
]
_CWD = os.path.join(os.path.dirname(__file__), "..", "backend")
_CWD = os.path.abspath(_CWD)


def _kill_port_5000() -> None:
    os.system("fuser -k 5000/tcp 2>/dev/null; sleep 0.4")


def _run_trial(n: int) -> dict:
    _kill_port_5000()
    time.sleep(0.5)

    env = {**os.environ}
    t_launch = time.monotonic()
    proc = subprocess.Popen(
        _CMD,
        cwd=_CWD,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        text=True,
        bufsize=1,
    )

    t_yield = None
    t_health = None
    output_lines: list[str] = []

    deadline = t_launch + _MAX_WAIT_S
    while time.monotonic() < deadline:
        line = proc.stdout.readline()
        if not line:
            rc = proc.poll()
            if rc is not None:
                print(f"  [trial {n}] Process exited with code {rc}")
                break
            time.sleep(0.05)
            continue

        line = line.rstrip()
        output_lines.append(line)

        if "lifespan yield reached in" in line:
            m = re.search(r"in ([\d.]+)s", line)
            if m:
                t_yield = float(m.group(1))

        # Once we have yield, start probing /health
        if t_yield is not None and t_health is None:
            try:
                r = httpx.get(_HEALTH_URL, timeout=1.0)
                if r.status_code == 200:
                    t_health = time.monotonic() - t_launch
            except Exception:
                pass

        if t_health is not None:
            break

    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()

    # Extract import timing if logged
    import_ms = None
    for ln in output_lines:
        m = re.search(r"whale_watch_service import: ([\d.]+)s", ln)
        if m:
            import_ms = float(m.group(1)) * 1000

    return {
        "trial":          n,
        "t_yield_s":      t_yield,
        "t_health_s":     round(t_health, 3) if t_health else None,
        "import_phase_ms": round(import_ms, 0) if import_ms else None,
        "timeout":        t_health is None,
    }


def main() -> None:
    print(f"\n{'='*60}")
    print("COLD-START TIMING TEST  (3 trials)")
    print(f"{'='*60}\n")

    results = []
    for i in range(1, _TRIALS + 1):
        print(f"Trial {i}/{_TRIALS} — launching fresh process …")
        r = _run_trial(i)
        results.append(r)
        status = "TIMEOUT" if r["timeout"] else "OK"
        print(
            f"  yield={r['t_yield_s']:.2f}s  "
            f"health={r['t_health_s']}s  "
            f"[{status}]"
        )
        time.sleep(1.5)

    print(f"\n{'─'*60}")
    print(f"{'Trial':<8} {'Yield (s)':<12} {'Health (s)':<14} {'Status'}")
    print(f"{'─'*60}")
    for r in results:
        print(
            f"{r['trial']:<8} "
            f"{str(r['t_yield_s']):<12} "
            f"{str(r['t_health_s']):<14} "
            f"{'TIMEOUT' if r['timeout'] else 'PASS'}"
        )

    yields = [r["t_yield_s"] for r in results if r["t_yield_s"]]
    if yields:
        print(f"\nYield — min={min(yields):.2f}s  max={max(yields):.2f}s  avg={sum(yields)/len(yields):.2f}s")

    failed = [r for r in results if r["timeout"]]
    if failed:
        print(f"\nWARNING: {len(failed)} trial(s) timed out (>{_MAX_WAIT_S}s)")
        sys.exit(1)
    else:
        print("\nAll trials passed — lifespan yields before health-check deadline.")
        sys.exit(0)


if __name__ == "__main__":
    main()
