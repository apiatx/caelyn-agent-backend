"""Canonical portfolio holdings store — single source of truth.

All portfolio-related backend paths MUST read and write through this module:
  - Portfolio Dashboard  (GET / POST /api/portfolio/holdings)
  - Portfolio Terminal   (GET /api/caelyn-terminal)
  - AI Portfolio Review  (POST /api/portfolio/review)
  - Watchlist comparison (POST /api/portfolio/compare-watchlist/run)
  - Risk / correlation / volatility analytics

Auth is currently disabled — all requests resolve to a single shared
portfolio.  When auth is re-enabled, extend load/save with a user_id param.

Public API
----------
  canonical_file()                  -> Path
  load_active_holdings()            -> list[dict]
  save_active_holdings(holdings)    -> None
  get_holdings_signature(holdings?) -> str  (16-char MD5 hex)
  startup_audit()                   -> dict
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import datetime
from pathlib import Path

# ── Neon PostgreSQL persistence (survives autoscale cold starts) ──────────────
# Uses the same connection URL as pg_storage.py.
# Falls back gracefully to file-only mode when DB is unavailable.

_DB_URL = os.environ.get("NEON_DATABASE_URL") or os.environ.get("DATABASE_URL")
_db_pool = None

import time as _time_pf
_holdings_mem_cache: dict = {"data": None, "ts": 0.0}
_HOLDINGS_CACHE_TTL = 45.0   # seconds — invalidated immediately on save


def _invalidate_holdings_cache() -> None:
    _holdings_mem_cache["data"] = None


def _get_db_conn():
    """Lazy pool init with one retry. Returns None when DB unavailable."""
    global _db_pool
    if not _DB_URL:
        return None
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
    def _clean(url: str) -> str:
        try:
            p = urlparse(url)
            qs = parse_qs(p.query, keep_blank_values=True)
            qs.pop("channel_binding", None)
            return urlunparse(p._replace(query=urlencode(qs, doseq=True)))
        except Exception:
            return url

    for _ in range(2):
        if _db_pool is None:
            try:
                from psycopg2 import pool as _pgpool
                _db_pool = _pgpool.SimpleConnectionPool(1, 3, _clean(_DB_URL))
            except Exception as e:
                print(f"[PORTFOLIO_STORE][DB] pool init failed: {e}")
                return None
        try:
            conn = _db_pool.getconn()
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            conn.commit()
            cur.close()
            return conn
        except Exception:
            try:
                _db_pool.closeall()
            except Exception:
                pass
            _db_pool = None
    return None


def _put_db_conn(conn):
    global _db_pool
    if _db_pool and conn:
        try:
            _db_pool.putconn(conn)
        except Exception:
            pass


def _db_ensure_table(conn) -> None:
    """Create the portfolio_holdings table if it does not exist."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS portfolio_holdings (
            slot_id    INTEGER PRIMARY KEY DEFAULT 1,
            holdings   JSONB       NOT NULL DEFAULT '[]'::jsonb,
            saved_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()


def _db_load_holdings() -> list[dict] | None:
    """Load holdings from Neon DB. Returns None if DB unavailable or empty."""
    conn = _get_db_conn()
    if not conn:
        return None
    try:
        _db_ensure_table(conn)
        cur = conn.cursor()
        cur.execute("SELECT holdings FROM portfolio_holdings WHERE slot_id = 1")
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        raw = row[0]
        if isinstance(raw, str):
            raw = json.loads(raw)
        if isinstance(raw, list) and raw:
            print(f"[PORTFOLIO_STORE][DB] loaded count={len(raw)} symbols={[h.get('ticker','?') for h in raw[:5]]}")
            return raw
        return None
    except Exception as e:
        print(f"[PORTFOLIO_STORE][DB] load error: {e}")
        return None
    finally:
        _put_db_conn(conn)


def _db_save_holdings(holdings: list[dict]) -> bool:
    """Upsert holdings to Neon DB. Returns True on success."""
    conn = _get_db_conn()
    if not conn:
        return False
    try:
        _db_ensure_table(conn)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO portfolio_holdings (slot_id, holdings, saved_at)
            VALUES (1, %s::jsonb, NOW())
            ON CONFLICT (slot_id) DO UPDATE
              SET holdings = EXCLUDED.holdings, saved_at = NOW()
        """, (json.dumps(holdings, default=str),))
        conn.commit()
        cur.close()
        syms = [h.get("ticker", "?") for h in holdings[:5]]
        print(f"[PORTFOLIO_STORE][DB] saved count={len(holdings)} symbols={syms}")
        return True
    except Exception as e:
        print(f"[PORTFOLIO_STORE][DB] save error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_db_conn(conn)

# ── Canonical location ────────────────────────────────────────────────────────
# Relative to backend/ CWD (where uvicorn runs from).
_PORTFOLIO_DIR  = Path("data/portfolio")
_CANONICAL_FILE = _PORTFOLIO_DIR / "active_holdings.json"
_ARCHIVE_DIR    = _PORTFOLIO_DIR / "archive"

# Legacy files that pre-date this consolidation.
# Read once for migration, then archived.  Never written to again.
_LEGACY_PATHS: list[Path] = [
    Path("data/portfolio_holdings_default.json"),  # was user_id=default
    Path("data/portfolio_holdings_aidan.json"),    # old demo file
    Path("data/portfolio_holdings.json"),          # older demo file
]

# Legacy file to promote when migrating (the one _portfolio_file("default")
# previously pointed to — all requests resolved here while auth was disabled).
_ACTIVE_LEGACY = Path("data/portfolio_holdings_default.json")


# ── Internals ────────────────────────────────────────────────────────────────

def _ensure_dirs() -> None:
    _PORTFOLIO_DIR.mkdir(parents=True, exist_ok=True)
    _ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)


def _read_from(path: Path) -> list[dict]:
    """Read a holdings list from any JSON path; [] on failure."""
    try:
        raw = json.loads(path.read_text())
        h = raw.get("holdings", raw) if isinstance(raw, dict) else raw
        if not isinstance(h, list):
            return []
        return [x for x in h if isinstance(x, dict) and (x.get("ticker") or x.get("symbol"))]
    except Exception:
        return []


def _to_date_str(val) -> str | None:
    """Coerce any date/datetime string to a plain YYYY-MM-DD string.

    The frontend sometimes sends full ISO-8601 datetime strings such as
    "2026-05-12T05:27:37.636Z".  Storing those as entry_date breaks
    date.fromisoformat() in the sell endpoint and closed-trade derivation,
    so we strip everything after the first 'T' here at normalisation time.
    Plain date strings ("2024-03-15") pass through unchanged.
    None / empty values return None.
    """
    if not val:
        return None
    s = str(val).strip()
    if "T" in s:
        s = s.split("T")[0]
    if not s:
        return None
    # Quick sanity: must look like YYYY-MM-DD
    try:
        from datetime import date as _d
        _d.fromisoformat(s)
    except ValueError:
        return None
    return s


def _normalise_lot(lot: dict) -> dict:
    """Canonical buy-lot shape: shares, price, date, optional notes, optional account_id."""
    out: dict = {
        "shares": float(lot.get("shares", 0) or 0),
        "price":  float(lot.get("price", lot.get("avg_cost", 0)) or 0),
    }
    d = _to_date_str(lot.get("date") or lot.get("entry_date"))
    if d:
        out["date"] = d
    if lot.get("notes"):
        out["notes"] = lot["notes"]
    # Preserve account_id so multi-account CSV imports can isolate per-account
    # netting — a sell in one account must not close a position in another.
    if lot.get("account_id"):
        out["account_id"] = str(lot["account_id"])
    return out


def compute_lot_totals(lots: list[dict]) -> dict:
    """Given a list of buy lots, return aggregated shares, weighted avg_cost,
    and the earliest entry_date.

    Used by the /buy endpoint and any code that rebuilds top-level holding
    fields from the lots array.
    """
    total_shares = sum(float(l.get("shares", 0)) for l in lots)
    total_cost   = sum(float(l.get("shares", 0)) * float(l.get("price", 0)) for l in lots)
    avg_cost     = round(total_cost / total_shares, 6) if total_shares else 0.0
    dates        = [_to_date_str(l.get("date")) for l in lots]
    valid_dates  = [d for d in dates if d]
    entry_date   = min(valid_dates) if valid_dates else None
    return {
        "shares":     round(total_shares, 8),
        "avg_cost":   avg_cost,
        "entry_date": entry_date,
    }


def _normalise(h: dict) -> dict:
    """Canonical holding shape with well-known field names.

    Preserves `lots` (list of buy lots) through save/load cycles so the
    full purchase history survives persistence. If lots are present, the
    top-level shares / avg_cost / entry_date are always recomputed from
    them to keep the two representations consistent.
    """
    raw_lots = h.get("lots")
    if isinstance(raw_lots, list) and raw_lots:
        lots = [_normalise_lot(lot) for lot in raw_lots if isinstance(lot, dict)]
    else:
        lots = []

    if lots:
        totals = compute_lot_totals(lots)
        shares    = totals["shares"]
        avg_cost  = totals["avg_cost"]
        entry_date = totals["entry_date"]
    else:
        shares     = float(h.get("shares", h.get("qty", 0)) or 0)
        avg_cost   = float(h.get("avg_cost", h.get("avg_price", h.get("cost", 0))) or 0)
        entry_date = _to_date_str(h.get("entry_date"))

    out: dict = {
        "ticker":     (h.get("ticker") or h.get("symbol") or "").upper().strip(),
        "shares":     shares,
        "avg_cost":   avg_cost,
        "asset_type": h.get("asset_type", h.get("type", "stock")),
    }
    if lots:
        out["lots"] = lots
    if entry_date is not None:
        out["entry_date"] = entry_date
    for k in ("date_added", "notes", "id", "trade_group_id",
              "classification", "basis_source", "import_batch_id", "source_file"):
        if h.get(k) is not None:
            out[k] = h[k]
    return out


def _write_canonical(holdings: list[dict]) -> None:
    _ensure_dirs()
    payload = {
        "holdings":  [_normalise(h) for h in holdings],
        "_saved_at": datetime.utcnow().isoformat() + "Z",
        "_source":   "portfolio_store",
    }
    _CANONICAL_FILE.write_text(json.dumps(payload, indent=2))


# ── One-time migration ────────────────────────────────────────────────────────

def _migrate_legacy_if_needed() -> None:
    """If the canonical file is absent, promote the active legacy file and
    archive all legacy files.  Runs automatically on first load/save.
    """
    if _CANONICAL_FILE.exists():
        return
    _ensure_dirs()

    # Prefer the active legacy (data/portfolio_holdings_default.json) — that is
    # what every endpoint was reading while auth was disabled.  If it is empty,
    # start the canonical file empty; the frontend will POST the real holdings.
    seed: list[dict] = []
    seed_src: str = "empty"
    if _ACTIVE_LEGACY.exists():
        seed = _read_from(_ACTIVE_LEGACY)
        seed_src = str(_ACTIVE_LEGACY)

    _write_canonical(seed)
    syms = [h.get("ticker", "?") for h in seed[:25]]
    print(
        f"[portfolio-source-audit] MIGRATE  source={seed_src}  "
        f"count={len(seed)}  symbols={syms}  → {_CANONICAL_FILE}"
    )

    # Archive ALL legacy files (copy, not delete — safe to remove later)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    for leg in _LEGACY_PATHS:
        if not leg.exists():
            continue
        dst = _ARCHIVE_DIR / f"{leg.stem}_legacy_{ts}.json"
        shutil.copy2(leg, dst)
        print(f"[portfolio-source-audit] ARCHIVE  {leg} → {dst}")


# ── Public API ────────────────────────────────────────────────────────────────

def canonical_file() -> Path:
    """Return the canonical holdings file path (used by Terminal cache key)."""
    _ensure_dirs()
    _migrate_legacy_if_needed()
    return _CANONICAL_FILE


def load_active_holdings() -> list[dict]:
    """Load and return the canonical normalised holdings list.

    Priority:
      1. In-memory TTL cache  (45 s) — zero-latency on repeated requests
      2. Neon DB (slot_id=1)  — survives autoscale cold starts
      3. Canonical file       — local dev / cold-start fallback
    Returns [] if all sources are empty or unavailable.
    """
    _now = _time_pf.time()
    if _holdings_mem_cache["data"] is not None and _now - _holdings_mem_cache["ts"] < _HOLDINGS_CACHE_TTL:
        return list(_holdings_mem_cache["data"])   # shallow copy — safe for reads

    _ensure_dirs()
    _migrate_legacy_if_needed()

    # 1. Try Neon DB first (persists across autoscale restarts)
    db_holdings = _db_load_holdings()
    if db_holdings:
        # Write canonical file only on the first DB load (not on every read)
        if not _CANONICAL_FILE.exists():
            _write_canonical(db_holdings)
        syms = [h.get("ticker", "?") for h in db_holdings[:25]]
        print(
            f"[portfolio-source-audit] endpoint=load  source=neon_db  "
            f"count={len(db_holdings)}  symbols={syms}"
        )
        _holdings_mem_cache["data"] = db_holdings
        _holdings_mem_cache["ts"]   = _now
        return db_holdings

    # 2. Fall back to file
    holdings = _read_from(_CANONICAL_FILE) if _CANONICAL_FILE.exists() else []
    syms = [h.get("ticker", "?") for h in holdings[:25]]
    print(
        f"[portfolio-source-audit] endpoint=load  source=file  "
        f"source_file={_CANONICAL_FILE}  "
        f"count={len(holdings)}  symbols={syms}"
    )
    _holdings_mem_cache["data"] = holdings
    _holdings_mem_cache["ts"]   = _now
    return holdings


def save_active_holdings(holdings: list[dict]) -> None:
    """Persist holdings to both Neon DB (primary) and canonical file (fallback).
    Neon DB survives autoscale cold starts; file is a local dev safety net.
    Silently drops entries with no ticker/symbol.

    Also invalidates the portfolio earnings cache so the Earnings Calendar
    reflects the updated holdings on next request.  Earnings sync failure
    never blocks the portfolio save.
    """
    _invalidate_holdings_cache()   # force next read to hit DB
    valid = [h for h in holdings if isinstance(h, dict) and (h.get("ticker") or h.get("symbol"))]
    normalised = [_normalise(h) for h in valid]

    # 1. Write to Neon DB (primary — survives restarts)
    db_ok = _db_save_holdings(normalised)

    # 2. Always write to file as well (local dev / cold-start seed)
    _write_canonical(normalised)

    syms = [h["ticker"] for h in normalised[:25]]
    print(
        f"[portfolio-source-audit] endpoint=save  db={db_ok}  "
        f"source_file={_CANONICAL_FILE}  "
        f"count={len(normalised)}  symbols={syms}"
    )

    # 3. Invalidate Caelyn Terminal cache so the next /api/caelyn-terminal request
    #    rebuilds with the new holdings instead of serving stale data.
    try:
        from data.caelyn_terminal import CaelynTerminalProvider
        from data.cache import cache as _app_cache
        _term_key = CaelynTerminalProvider.cache_key_for(_CANONICAL_FILE)
        _app_cache.delete(_term_key)
        print(
            f"[portfolio-source-audit] terminal cache invalidated "
            f"after save of {len(normalised)} holdings"
        )
    except Exception as _term_e:
        print(f"[portfolio-source-audit] terminal cache invalidation skipped: {_term_e}")

    # 4. Invalidate portfolio earnings cache so Earnings Calendar reflects changes.
    #    Fire-and-forget — never raises even if Neon is unavailable.
    try:
        from services.user_earnings_service import invalidate_user_earnings  # type: ignore
        invalidate_user_earnings("portfolio")
        print(
            f"[portfolio-source-audit] portfolio earnings cache invalidated "
            f"after save of {len(normalised)} holdings"
        )
    except Exception as _inv_e:
        print(f"[portfolio-source-audit] earnings cache invalidation skipped: {_inv_e}")


def get_holdings_signature(holdings: list[dict] | None = None) -> str:
    """Return a stable 16-char MD5 hex hash of ticker+shares+avg_cost.
    Used as a cache key by the Terminal provider.
    """
    if holdings is None:
        holdings = load_active_holdings()
    key = "|".join(
        f"{h.get('ticker', '')}:{h.get('shares', 0)}:{h.get('avg_cost', 0)}"
        for h in sorted(holdings, key=lambda x: x.get("ticker", ""))
    )
    return hashlib.md5(key.encode()).hexdigest()[:16]


def startup_audit() -> dict:
    """Discover all portfolio-related files, run migration if needed, and log
    a full summary.  Called once at application startup.
    Returns the audit dict (also logged to stdout).
    """
    _ensure_dirs()
    _migrate_legacy_if_needed()

    discovered: list[dict] = []
    for p in list(_LEGACY_PATHS) + [_CANONICAL_FILE]:
        if not p.exists():
            continue
        h = _read_from(p)
        syms = [x.get("ticker", x.get("symbol", "?")) for x in h[:25]]
        discovered.append({
            "file":        str(p),
            "count":       len(h),
            "symbols":     syms,
            "modified_at": datetime.utcfromtimestamp(p.stat().st_mtime).isoformat() + "Z",
            "active":      (p == _CANONICAL_FILE),
            "archived":    (p != _CANONICAL_FILE),
        })

    canonical_holdings = load_active_holdings()

    lines = [
        f"  file={d['file']!r}  count={d['count']}  "
        f"symbols={d['symbols'][:5]}  "
        f"modified_at={d['modified_at']!r}  active={d['active']}"
        for d in discovered
    ]
    print(
        "[portfolio-source-audit] discovered_files=[\n"
        + "\n".join(lines)
        + "\n]"
    )
    print(
        f"[portfolio-source-audit] canonical={_CANONICAL_FILE}  "
        f"count={len(canonical_holdings)}  "
        f"symbols={[h.get('ticker') for h in canonical_holdings[:25]]}"
    )

    return {
        "canonical_file":      str(_CANONICAL_FILE),
        "canonical_count":     len(canonical_holdings),
        "canonical_symbols":   [h.get("ticker") for h in canonical_holdings],
        "discovered_files":    discovered,
    }
