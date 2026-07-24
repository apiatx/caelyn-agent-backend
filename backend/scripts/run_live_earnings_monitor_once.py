"""
CLI entry point for the Live Earnings Monitor.

Usage (scheduled worker / cron / Replit Scheduled Deployment):
    python -m backend.scripts.run_live_earnings_monitor_once

Optional args:
    --dry-run            Run in replay/synthetic mode (no real alerts)
    --symbol COIN        Force-check a specific symbol
    --init-tables        Re-run Neon table creation before monitoring
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

# ── path setup ────────────────────────────────────────────────────────────────
# Ensure backend/ is on sys.path so relative imports match the server context
_backend = Path(__file__).resolve().parent.parent
if str(_backend) not in sys.path:
    sys.path.insert(0, str(_backend))

# ── minimal env sanity check ──────────────────────────────────────────────────
_required = ["NEON_DATABASE_URL", "FMP_API_KEY"]
_missing  = [k for k in _required if not os.environ.get(k)]
if _missing:
    print(f"[run_once] WARNING: missing env vars: {_missing}")


async def _main(dry_run: bool, symbol: str | None, init_tables: bool) -> None:
    if init_tables:
        from data.earnings_monitor_store import init_earnings_monitor_tables
        ok = init_earnings_monitor_tables()
        print(f"[run_once] init_earnings_monitor_tables → {ok}")

    from services.earnings_monitor_service import run_live_earnings_monitor_once
    result = await run_live_earnings_monitor_once(
        dry_run=dry_run,
        force_symbol=symbol,
    )
    print(f"[run_once] done: {result}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Caelyn Live Earnings Monitor — single pass")
    parser.add_argument("--dry-run",     action="store_true", help="Synthetic/replay mode — no real alerts")
    parser.add_argument("--symbol",      type=str, default=None, help="Force-check one symbol")
    parser.add_argument("--init-tables", action="store_true", help="Create Neon tables before running")
    args = parser.parse_args()

    asyncio.run(_main(args.dry_run, args.symbol, args.init_tables))


if __name__ == "__main__":
    main()
