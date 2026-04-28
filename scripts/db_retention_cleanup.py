#!/usr/bin/env python3
"""
Database Retention Cleanup
==========================
Reads enabled rules from public.data_retention_rules and deletes rows older
than each rule's retention_days from the target table.

Usage
-----
    # Preview what would be deleted (safe, no writes)
    DRY_RUN=true python scripts/db_retention_cleanup.py

    # Actually delete old rows
    python scripts/db_retention_cleanup.py

Environment variables
---------------------
    NEON_DATABASE_URL   Preferred — Neon cloud Postgres connection string
    DATABASE_URL        Fallback — Replit internal Helium DB
    DRY_RUN             Set to "true" / "1" / "yes" to skip all deletes (default: false)

Protected tables
----------------
The following table names are unconditionally skipped regardless of what the
data_retention_rules table says:

    users, auth_users, user_sessions, sessions,
    watchlist, watchlists,
    portfolios, portfolio_holdings, portfolio_positions,
    user_settings, user_preferences, preferences, settings,
    subscriptions, billing, billing_events, payment_methods,
    congressional_trades, insider_transactions,
    whales, whale_holdings, whale_transactions, whale_portfolio_returns

Notes
-----
- Run DRY_RUN first and review the output before running the live delete.
- Each table's delete is committed independently; a failure in one table
  does not roll back completed deletes in others.
- This script is safe to run repeatedly — it is idempotent.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# ── Constants ─────────────────────────────────────────────────────────────────

# Tables whose data must never be deleted by automated retention jobs.
# This list is checked case-insensitively against the unqualified table name
# (i.e. the part after any schema prefix like "public.").
_PROTECTED_TABLES: frozenset[str] = frozenset({
    "users",
    "auth_users",
    "user_sessions",
    "sessions",
    "watchlist",
    "watchlists",
    "portfolios",
    "portfolio_holdings",
    "portfolio_positions",
    "user_settings",
    "user_preferences",
    "preferences",
    "settings",
    "subscriptions",
    "billing",
    "billing_events",
    "payment_methods",
    "congressional_trades",
    "insider_transactions",
    "whales",
    "whale_holdings",
    "whale_transactions",
    "whale_portfolio_returns",
})


def _unqualified(table_name: str) -> str:
    """Strip schema prefix — 'public.messages' → 'messages'."""
    return table_name.split(".")[-1].lower()


def _is_protected(table_name: str) -> bool:
    return _unqualified(table_name) in _PROTECTED_TABLES


def _sanitize_url(url: Optional[str]) -> Optional[str]:
    """Strip channel_binding from Neon pooler URLs (psycopg2 compat)."""
    if not url:
        return url
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query, keep_blank_values=True)
        if "channel_binding" in qs:
            del qs["channel_binding"]
            url = urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))
    except Exception:
        pass
    return url


# ── Database helpers ──────────────────────────────────────────────────────────

def _get_connection(db_url: str):
    try:
        import psycopg2
        conn = psycopg2.connect(db_url)
        conn.autocommit = False
        return conn
    except ImportError:
        print("[ERROR] psycopg2 is not installed. Run: pip install psycopg2-binary")
        sys.exit(1)


def _table_exists(cur, table_name: str) -> bool:
    """Return True if the table exists in the database (checks all schemas)."""
    schema, name = ("public", table_name) if "." not in table_name else table_name.split(".", 1)
    cur.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        LIMIT 1
        """,
        (schema, name),
    )
    return cur.fetchone() is not None


def _column_exists(cur, table_name: str, column_name: str) -> bool:
    """Return True if the column exists in the table."""
    schema, name = ("public", table_name) if "." not in table_name else table_name.split(".", 1)
    cur.execute(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
        LIMIT 1
        """,
        (schema, name, column_name),
    )
    return cur.fetchone() is not None


def _count_stale_rows(cur, table_name: str, ts_col: str, retention_days: int) -> int:
    """Count rows older than retention_days without deleting them."""
    cur.execute(
        f'SELECT COUNT(*) FROM {table_name} WHERE "{ts_col}" < NOW() - INTERVAL \'%s days\'',
        (retention_days,),
    )
    row = cur.fetchone()
    return row[0] if row else 0


def _delete_stale_rows(cur, table_name: str, ts_col: str, retention_days: int) -> int:
    """Delete rows older than retention_days. Returns number of deleted rows."""
    cur.execute(
        f'DELETE FROM {table_name} WHERE "{ts_col}" < NOW() - INTERVAL \'%s days\'',
        (retention_days,),
    )
    return cur.rowcount


# ── Main logic ────────────────────────────────────────────────────────────────

def run(dry_run: bool, db_url: str) -> None:
    started_at = datetime.now(timezone.utc).isoformat()
    mode_label = "DRY RUN" if dry_run else "LIVE DELETE"
    print(f"\n{'=' * 60}")
    print(f"  Database Retention Cleanup — {mode_label}")
    print(f"  Started: {started_at}")
    print(f"{'=' * 60}\n")

    conn = _get_connection(db_url)
    try:
        with conn.cursor() as cur:
            # ── Load retention rules ──────────────────────────────────────
            if not _table_exists(cur, "public.data_retention_rules"):
                print("[ERROR] public.data_retention_rules table does not exist.")
                print("        Run the backend server once to create and seed it, then retry.")
                return

            cur.execute("""
                SELECT table_name, timestamp_column, retention_days, description
                FROM public.data_retention_rules
                WHERE enabled = TRUE
                ORDER BY table_name
            """)
            rules = cur.fetchall()

        if not rules:
            print("[INFO] No enabled retention rules found — nothing to do.")
            return

        print(f"[INFO] Loaded {len(rules)} enabled retention rule(s).\n")

        total_deleted = 0
        total_skipped = 0
        total_errors = 0

        for (table_name, ts_col, retention_days, description) in rules:
            print(f"── {table_name}")
            print(f"   column={ts_col}  retention={retention_days}d  {description}")

            # ── Protection check ─────────────────────────────────────────
            if _is_protected(table_name):
                print(f"   [SKIP] Protected table — skipping unconditionally.\n")
                total_skipped += 1
                continue

            # ── Validate table / column ───────────────────────────────────
            with conn.cursor() as cur:
                if not _table_exists(cur, table_name):
                    print(f"   [SKIP] Table does not exist in the database.\n")
                    total_skipped += 1
                    continue

                if not _column_exists(cur, table_name, ts_col):
                    print(f"   [SKIP] Column '{ts_col}' not found in {table_name}.\n")
                    total_skipped += 1
                    continue

                # ── Count stale rows ──────────────────────────────────────
                try:
                    stale = _count_stale_rows(cur, table_name, ts_col, retention_days)
                except Exception as count_err:
                    print(f"   [ERROR] Count failed: {count_err}\n")
                    total_errors += 1
                    continue

            if stale == 0:
                print(f"   [OK] 0 rows older than {retention_days} days — nothing to delete.\n")
                continue

            if dry_run:
                print(f"   [DRY RUN] Would delete {stale:,} row(s) older than {retention_days} days.\n")
                total_deleted += stale
                continue

            # ── Live delete ───────────────────────────────────────────────
            try:
                with conn.cursor() as cur:
                    deleted = _delete_stale_rows(cur, table_name, ts_col, retention_days)
                conn.commit()
                print(f"   [DELETED] {deleted:,} row(s) removed (older than {retention_days} days).\n")
                total_deleted += deleted
            except Exception as del_err:
                try:
                    conn.rollback()
                except Exception:
                    pass
                print(f"   [ERROR] Delete failed (rolled back): {del_err}\n")
                total_errors += 1

        # ── Summary ───────────────────────────────────────────────────────
        print(f"{'=' * 60}")
        if dry_run:
            print(f"  DRY RUN complete — no rows were deleted.")
            print(f"  Rows that WOULD be deleted : {total_deleted:,}")
        else:
            print(f"  LIVE DELETE complete.")
            print(f"  Rows deleted               : {total_deleted:,}")
        print(f"  Rules skipped              : {total_skipped}")
        print(f"  Errors                     : {total_errors}")
        print(f"{'=' * 60}\n")

    finally:
        conn.close()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    raw_url = os.environ.get("NEON_DATABASE_URL") or os.environ.get("DATABASE_URL")
    db_url = _sanitize_url(raw_url)

    if not db_url:
        print("[ERROR] Neither NEON_DATABASE_URL nor DATABASE_URL is set.")
        print("        Export the variable before running this script.")
        sys.exit(1)

    dry_run_env = os.environ.get("DRY_RUN", "false").strip().lower()
    dry_run = dry_run_env in ("true", "1", "yes")

    run(dry_run=dry_run, db_url=db_url)
