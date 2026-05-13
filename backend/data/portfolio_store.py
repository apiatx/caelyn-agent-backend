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
import shutil
from datetime import datetime
from pathlib import Path

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


def _normalise(h: dict) -> dict:
    """Canonical holding shape with well-known field names."""
    out: dict = {
        "ticker":     (h.get("ticker") or h.get("symbol") or "").upper().strip(),
        "shares":     float(h.get("shares", h.get("qty", 0)) or 0),
        "avg_cost":   float(h.get("avg_cost", h.get("avg_price", h.get("cost", 0))) or 0),
        "asset_type": h.get("asset_type", h.get("type", "stock")),
    }
    for k in ("date_added", "notes", "id"):
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
    Returns [] if the file is missing or malformed.
    """
    _ensure_dirs()
    _migrate_legacy_if_needed()
    holdings = _read_from(_CANONICAL_FILE) if _CANONICAL_FILE.exists() else []
    syms = [h.get("ticker", "?") for h in holdings[:25]]
    print(
        f"[portfolio-source-audit] endpoint=load  "
        f"source_file={_CANONICAL_FILE}  "
        f"count={len(holdings)}  symbols={syms}"
    )
    return holdings


def save_active_holdings(holdings: list[dict]) -> None:
    """Persist holdings to the canonical file (normalised).
    Silently drops entries with no ticker/symbol.
    """
    valid = [h for h in holdings if isinstance(h, dict) and (h.get("ticker") or h.get("symbol"))]
    _write_canonical(valid)
    syms = [_normalise(h)["ticker"] for h in valid[:25]]
    print(
        f"[portfolio-source-audit] endpoint=save  "
        f"source_file={_CANONICAL_FILE}  "
        f"count={len(valid)}  symbols={syms}"
    )


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
