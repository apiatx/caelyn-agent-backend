"""
EI Materials Cache — disk-persistent cache for earnings intelligence SEC materials.

Keyed by SYMBOL (not CIK) so ticker_detail_endpoint can read without CIK resolution.
Follows the same pattern as edgar_cache.py.

Storage: backend/data/edgar_disk_cache/ei_materials.json
TTL: 24 hours (daily refresh via background loop)
"""
from __future__ import annotations

import json
import os
import time

CACHE_DIR = os.path.join(os.path.dirname(__file__), "edgar_disk_cache")
MATERIALS_FILE = os.path.join(CACHE_DIR, "ei_materials.json")
MATERIALS_MAX_AGE = 86400       # 24 hours normal TTL
MATERIALS_IMMUTABLE_AGE = 9999999  # retained indefinitely for historical filings


def _ensure_dir() -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)


def _load_json(path: str) -> dict:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_json(path: str, data: dict) -> None:
    _ensure_dir()
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, default=str)
    os.replace(tmp, path)


# ── Public read API ─────────────────────────────────────────────────────────

def get_materials(symbol: str) -> dict | None:
    """
    Read cached materials for a symbol.
    Returns None if cache entry is missing or older than MATERIALS_MAX_AGE.
    """
    data = _load_json(MATERIALS_FILE)
    entry = data.get(symbol.upper())
    if not entry:
        return None
    cached_at = entry.get("_cached_at", 0)
    if time.time() - cached_at > MATERIALS_MAX_AGE:
        return None
    return entry


def get_materials_lkg(symbol: str) -> dict | None:
    """
    Read cached materials regardless of age (last-known-good pattern).
    Used when fresh data is unavailable — stale is better than null.
    """
    data = _load_json(MATERIALS_FILE)
    return data.get(symbol.upper())


def get_all_symbols() -> list[str]:
    """List all symbols with cached materials entries."""
    data = _load_json(MATERIALS_FILE)
    return [k for k in data if not k.startswith("_")]


# ── Public write API ─────────────────────────────────────────────────────────

def set_materials(symbol: str, materials: dict) -> None:
    """Write materials data for a symbol (replaces any existing entry)."""
    data = _load_json(MATERIALS_FILE)
    materials["_cached_at"] = time.time()
    data[symbol.upper()] = materials
    _save_json(MATERIALS_FILE, data)


def bulk_set_materials(entries: dict[str, dict]) -> None:
    """Write multiple symbols atomically (used by backfill jobs)."""
    data = _load_json(MATERIALS_FILE)
    now = time.time()
    for symbol, materials in entries.items():
        m = dict(materials)
        m["_cached_at"] = now
        data[symbol.upper()] = m
    _save_json(MATERIALS_FILE, data)


def needs_refresh(symbol: str, max_age: float = MATERIALS_MAX_AGE) -> bool:
    """Return True when the cache entry is missing or older than max_age seconds."""
    data = _load_json(MATERIALS_FILE)
    entry = data.get(symbol.upper())
    if not entry:
        return True
    return time.time() - entry.get("_cached_at", 0) > max_age
