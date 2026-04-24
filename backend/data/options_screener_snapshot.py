"""
Lightweight JSON-based snapshot store for computing OI and premium deltas
across master screener cycles.

Layout of the JSON state file:
{
  "<symbol>:<expiry>:<strike>:<otype>": {
    "oi": <int>,
    "premium": <float>,
    "ts": <unix float>
  },
  ...
}

The store keeps the PREVIOUS snapshot values so the next enrichment call can
compute percentage changes.  Only the single most-recent prior snapshot is
stored per contract key (we don't need history, just the delta).
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Optional

_SNAPSHOT_PATH = Path(__file__).parent / "options_screener_state.json"
_MAX_AGE_HOURS = 8          # discard stale prior-snapshots older than this
_WRITE_INTERVAL = 60        # minimum seconds between writes (debounce)

_state: dict = {}           # in-memory prior snapshot values
_last_write: float = 0.0
_dirty: bool = False


def _load() -> None:
    global _state
    if not _SNAPSHOT_PATH.exists():
        _state = {}
        return
    try:
        raw = json.loads(_SNAPSHOT_PATH.read_text())
        _state = raw if isinstance(raw, dict) else {}
    except Exception:
        _state = {}


def _flush(force: bool = False) -> None:
    global _last_write, _dirty
    if not _dirty:
        return
    now = time.time()
    if not force and (now - _last_write) < _WRITE_INTERVAL:
        return
    try:
        _SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
        _SNAPSHOT_PATH.write_text(json.dumps(_state))
        _last_write = now
        _dirty = False
    except Exception as exc:
        print(f"[OPT_SNAPSHOT] Write error: {exc}")


def _key(symbol: str, expiry: str, strike: float, otype: str) -> str:
    return f"{symbol.upper()}:{expiry}:{strike}:{otype}"


def load_state() -> None:
    """Load snapshot state from disk (call once at startup)."""
    _load()
    # Prune old entries
    global _state, _dirty
    cutoff = time.time() - _MAX_AGE_HOURS * 3600
    old_keys = [k for k, v in _state.items() if v.get("ts", 0) < cutoff]
    if old_keys:
        for k in old_keys:
            del _state[k]
        _dirty = True
    _flush(force=True)


def get_deltas(
    symbol: str,
    expiry: str,
    strike: Optional[float],
    otype: str,
    current_oi: Optional[int],
    current_premium: Optional[float],
) -> tuple[Optional[float], Optional[float]]:
    """
    Return (oi_change_pct, premium_change_pct) vs the prior snapshot.
    Returns (None, None) if no prior snapshot exists.
    """
    if strike is None:
        return None, None
    k = _key(symbol, expiry, strike, otype)
    prior = _state.get(k)
    if not prior:
        return None, None

    oi_change = None
    prem_change = None

    prior_oi = prior.get("oi")
    if prior_oi is not None and current_oi is not None and prior_oi > 0:
        oi_change = round(((current_oi - prior_oi) / max(prior_oi, 1)) * 100.0, 1)

    prior_prem = prior.get("premium")
    if prior_prem is not None and current_premium is not None and prior_prem > 0:
        prem_change = round(((current_premium - prior_prem) / max(prior_prem, 1)) * 100.0, 1)

    return oi_change, prem_change


def update_state(
    rows: list[dict],
) -> None:
    """
    Snapshot the current cycle's best-contract values so the NEXT cycle can
    compute deltas.  Call after all enrichment is done.
    """
    global _state, _dirty
    now = time.time()
    for row in rows:
        symbol = row.get("ticker", "")
        if not symbol:
            continue
        for contract in row.get("top_contracts", []):
            expiry = contract.get("expiration") or ""
            strike = contract.get("strike")
            otype = contract.get("type") or contract.get("side") or ""
            oi = contract.get("open_interest")
            prem = contract.get("premium_traded_estimate")
            if not expiry or strike is None or not otype:
                continue
            k = _key(symbol, expiry, strike, otype)
            _state[k] = {"oi": oi, "premium": prem, "ts": now}
    _dirty = True
    _flush()
