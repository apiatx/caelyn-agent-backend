"""
EI Materials Cache — disk-persistent cache for earnings intelligence SEC materials.

Keyed by SYMBOL (not CIK) so ticker_detail_endpoint can read without CIK resolution.
Follows the same pattern as edgar_cache.py.

Storage: backend/data/edgar_disk_cache/ei_materials.json
TTL: 24 hours (daily refresh via background loop)

Version gating:
  discovery_version  — bumped when attachment enumeration method changes
  classifier_version — bumped when classification rules change
  Cache entries with versions below current are considered stale regardless of TTL.
  Current: discovery_version=2, classifier_version=3
"""
from __future__ import annotations

import json
import os
import time

CACHE_DIR = os.path.join(os.path.dirname(__file__), "edgar_disk_cache")
MATERIALS_FILE = os.path.join(CACHE_DIR, "ei_materials.json")
MATERIALS_MAX_AGE = 86400       # 24 hours normal TTL

# Must match constants in ei_materials_service.py
_REQUIRED_DISCOVERY_VERSION  = 2
_REQUIRED_CLASSIFIER_VERSION = 3


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
    Returns None if cache entry is missing, older than MATERIALS_MAX_AGE,
    or was created with a stale discovery/classifier version.
    """
    data = _load_json(MATERIALS_FILE)
    entry = data.get(symbol.upper())
    if not entry:
        return None
    cached_at = entry.get("_cached_at", 0)
    if time.time() - cached_at > MATERIALS_MAX_AGE:
        return None
    # Version gate — stale discovery/classifier means the data is unreliable
    ss = entry.get("source_status") or {}
    if ss.get("discovery_version", 0) < _REQUIRED_DISCOVERY_VERSION:
        return None
    if ss.get("classifier_version", 0) < _REQUIRED_CLASSIFIER_VERSION:
        return None
    return entry


def get_materials_lkg(symbol: str) -> dict | None:
    """
    Read cached materials regardless of age (last-known-good pattern).
    Used when fresh data is unavailable — stale is better than null.
    Still rejects entries with wrong version (wrong data is worse than null).
    """
    data = _load_json(MATERIALS_FILE)
    entry = data.get(symbol.upper())
    if not entry:
        return None
    ss = entry.get("source_status") or {}
    if ss.get("discovery_version", 0) < _REQUIRED_DISCOVERY_VERSION:
        return None
    if ss.get("classifier_version", 0) < _REQUIRED_CLASSIFIER_VERSION:
        return None
    return entry


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
    """
    Return True when the cache entry needs refresh:
      - Missing entry
      - Older than max_age seconds
      - discovery_version below current (stale attachment enumeration)
      - classifier_version below current (stale classification rules)
    """
    data = _load_json(MATERIALS_FILE)
    entry = data.get(symbol.upper())
    if not entry:
        return True
    if time.time() - entry.get("_cached_at", 0) > max_age:
        return True
    ss = entry.get("source_status") or {}
    if ss.get("discovery_version", 0) < _REQUIRED_DISCOVERY_VERSION:
        return True
    if ss.get("classifier_version", 0) < _REQUIRED_CLASSIFIER_VERSION:
        return True
    return False


def count_stale_entries() -> dict:
    """Diagnostic: count entries by version staleness."""
    data = _load_json(MATERIALS_FILE)
    total = 0
    version_ok = 0
    version_stale = 0
    ttl_stale = 0
    now = time.time()
    for k, entry in data.items():
        if k.startswith("_"):
            continue
        total += 1
        ss = entry.get("source_status") or {}
        dv = ss.get("discovery_version", 0)
        cv = ss.get("classifier_version", 0)
        age = now - entry.get("_cached_at", 0)
        if dv < _REQUIRED_DISCOVERY_VERSION or cv < _REQUIRED_CLASSIFIER_VERSION:
            version_stale += 1
        elif age > MATERIALS_MAX_AGE:
            ttl_stale += 1
        else:
            version_ok += 1
    return {
        "total":         total,
        "version_ok":    version_ok,
        "version_stale": version_stale,
        "ttl_stale":     ttl_stale,
        "required_discovery_version":  _REQUIRED_DISCOVERY_VERSION,
        "required_classifier_version": _REQUIRED_CLASSIFIER_VERSION,
    }
