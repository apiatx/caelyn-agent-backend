#!/usr/bin/env python3
"""Rate-limited, resumable repair for incomplete watchlist profile/EI snapshots."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

from data.pg_storage import _get_conn, _put_conn
from data.watchlist_fundamentals_store import get_snapshots_bulk
from services.watchlist_fundamentals_refresh import (
    FmpFundamentalsRefresher,
    ei_ineligible_reason,
)
from services.watchlist_quote_cache import is_fmp_symbol_eligible


DEFAULT_CHECKPOINT = Path("logs/profile_earnings_backfill_checkpoint.json")
PROFILE_FIELDS = ("companyName", "description", "sector", "industry")


def current_watchlist_symbols() -> list[str]:
    conn = _get_conn()
    if conn is None:
        raise RuntimeError("Neon is unavailable")
    try:
        cur = conn.cursor()
        cur.execute("SELECT tickers FROM public.watchlist")
        symbols: set[str] = set()
        for (payload,) in cur.fetchall():
            values = payload if isinstance(payload, list) else []
            for value in values:
                symbol = value if isinstance(value, str) else (value or {}).get("symbol")
                if symbol:
                    symbols.add(str(symbol).strip().upper())
        cur.close()
        return sorted(symbols)
    finally:
        _put_conn(conn)


def needs_repair(snapshot: dict | None) -> bool:
    fields = (snapshot or {}).get("fields") or {}
    profile = fields.get("profile") or {}
    return (
        not all(profile.get(key) not in (None, "") for key in PROFILE_FIELDS)
        or not isinstance(fields.get("earnings_intelligence"), dict)
    )


async def run(args) -> dict:
    symbols = (
        sorted({s.strip().upper() for s in args.symbols.split(",") if s.strip()})
        if args.symbols else current_watchlist_symbols()
    )
    unsupported = [s for s in symbols if not is_fmp_symbol_eligible(s)]
    candidates = [s for s in symbols if is_fmp_symbol_eligible(s)]
    snapshots = get_snapshots_bulk(candidates)
    unsupported += [
        s for s in candidates if ei_ineligible_reason(s, snapshots.get(s)) is not None
    ]
    incomplete = [
        s for s in candidates
        if s not in unsupported and needs_repair(snapshots.get(s))
    ]
    complete = [s for s in candidates if s not in unsupported and s not in incomplete]

    checkpoint_path = Path(args.checkpoint)
    completed_before: set[str] = set()
    if checkpoint_path.exists() and not args.restart:
        completed_before = set(json.loads(checkpoint_path.read_text()).get("completed", []))
    pending = [s for s in incomplete if s not in completed_before]

    summary = {
        "total": len(symbols), "complete": len(complete),
        "incomplete": len(incomplete), "failed": 0,
        "unsupported": len(set(unsupported)), "refreshed": 0,
        "pending": len(pending), "failed_symbols": [],
    }
    if args.dry_run:
        summary["repair_symbols"] = pending
        return summary

    key = os.getenv("FMP_API_KEY", "")
    if not key:
        raise RuntimeError("FMP_API_KEY is not configured")
    refresher = FmpFundamentalsRefresher(key)
    completed = set(completed_before)
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    for offset in range(0, len(pending), args.batch_size):
        batch = pending[offset:offset + args.batch_size]
        result = await refresher.refresh_symbols(batch, "profile-earnings-backfill", dev_force=True)
        succeeded = set(result.get("refreshed", [])) | set(result.get("skipped", []))
        completed.update(succeeded)
        summary["refreshed"] += len(succeeded)
        failed = result.get("failed", []) + result.get("empty_payload_no_prior", [])
        summary["failed"] += len(failed)
        summary["failed_symbols"].extend(failed)
        checkpoint_path.write_text(json.dumps({"completed": sorted(completed)}, indent=2))

    summary["pending"] = len([s for s in incomplete if s not in completed])
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", help="Comma-separated targeted symbols")
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--checkpoint", default=str(DEFAULT_CHECKPOINT))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--restart", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 25:
        parser.error("--batch-size must be between 1 and 25")
    print(json.dumps(asyncio.run(run(args)), indent=2))


if __name__ == "__main__":
    main()
