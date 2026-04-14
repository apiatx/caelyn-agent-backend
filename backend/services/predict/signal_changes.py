"""
Prophetik Signal Engine — Signal Changes / Snapshot Diffing.

Maintains an in-memory snapshot of the last scored-markets result and detects
meaningful changes between consecutive scoring cycles.  Entirely ephemeral:
state resets on server restart, no database required.

Change types detected:
    new_best_bet            — market entered #1 in best_bet_now bucket
    bucket_entry            — market entered any recommendation bucket
    bucket_exit             — market left a recommendation bucket
    score_jump              — composite score rose >10 points
    score_drop              — composite score fell >10 points
    trap_risk_spike         — trap risk crossed above 60
    trap_risk_drop          — trap risk crossed below 40 from above
    momentum_shift          — momentum label changed
    repricing               — |24h price change| > 10%
    spread_change           — spread widened or compressed >3pp
    flow_spike              — flow score jumped >20 points
    execution_improved      — execution quality rose >15 points

Usage (called from polymarket_intelligence):
    from services.predict.signal_changes import signal_tracker
    signal_tracker.update(scored_markets, recommendation_buckets)
    changes = signal_tracker.get_recent_changes()
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Optional

_MAX_CHANGES = 50
_MAX_AGE_SECONDS = 1800  # 30 min

# Thresholds
_SCORE_JUMP_THRESHOLD = 10
_TRAP_SPIKE_THRESHOLD = 60
_TRAP_DROP_THRESHOLD = 40
_REPRICING_THRESHOLD = 10  # |24h move| > 10%
_SPREAD_CHANGE_THRESHOLD = 3  # spread_pct change > 3pp
_FLOW_SPIKE_THRESHOLD = 20
_EXECUTION_IMPROVE_THRESHOLD = 15


class SignalChange:
    """Single detected change."""

    __slots__ = (
        "timestamp", "market_id", "market_title", "change_type",
        "severity", "description", "old_value", "new_value", "market_slug",
    )

    def __init__(
        self,
        market_id: str,
        market_title: str,
        change_type: str,
        severity: str,
        description: str,
        old_value,
        new_value,
        market_slug: str = "",
    ):
        self.timestamp = datetime.now(timezone.utc).isoformat()
        self.market_id = market_id
        self.market_title = market_title
        self.change_type = change_type
        self.severity = severity
        self.description = description
        self.old_value = old_value
        self.new_value = new_value
        self.market_slug = market_slug

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "market_id": self.market_id,
            "market_title": self.market_title,
            "change_type": self.change_type,
            "severity": self.severity,
            "description": self.description,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "market_slug": self.market_slug,
        }


class SignalTracker:
    """
    In-memory snapshot differ.  Thread-safe via a simple lock — the web
    server may call update() from async tasks concurrently with get().
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._prev_scored: dict[str, dict] = {}  # condition_id -> market
        self._prev_buckets: dict[str, set[str]] = {}  # bucket_name -> {condition_ids}
        self._changes: list[SignalChange] = []
        self._last_updated: Optional[str] = None
        self._last_update_epoch: float = 0.0

    # ── Public API ──────────────────────────────────────────────────────

    def update(
        self,
        scored_markets: list[dict],
        recommendation_buckets: Optional[dict] = None,
    ) -> list[dict]:
        """
        Compare *scored_markets* (and optionally *recommendation_buckets*)
        against the previous snapshot, detect changes, rotate the snapshot,
        and return new changes as dicts.
        """
        now = datetime.now(timezone.utc)
        new_by_id: dict[str, dict] = {
            m["condition_id"]: m for m in scored_markets if "condition_id" in m
        }

        new_bucket_map: dict[str, set[str]] = {}
        if recommendation_buckets:
            for bucket_name, items in recommendation_buckets.items():
                new_bucket_map[bucket_name] = {
                    m["condition_id"] for m in items if "condition_id" in m
                }

        with self._lock:
            new_changes: list[SignalChange] = []

            if self._prev_scored:
                new_changes = self._diff(new_by_id, new_bucket_map)

            # Rotate snapshot
            self._prev_scored = new_by_id
            self._prev_buckets = new_bucket_map
            self._last_updated = now.isoformat()
            self._last_update_epoch = time.time()

            # Append and prune
            self._changes.extend(new_changes)
            self._prune()

        return [c.to_dict() for c in new_changes]

    def get_recent_changes(self) -> dict:
        """Return the latest changes payload for the API endpoint."""
        with self._lock:
            self._prune()
            age = (
                round(time.time() - self._last_update_epoch, 1)
                if self._last_update_epoch
                else None
            )
            return {
                "changes": [c.to_dict() for c in self._changes],
                "change_count": len(self._changes),
                "last_updated": self._last_updated,
                "snapshot_age_seconds": age,
            }

    # ── Diffing Logic ───────────────────────────────────────────────────

    def _diff(
        self,
        new_by_id: dict[str, dict],
        new_bucket_map: dict[str, set[str]],
    ) -> list[SignalChange]:
        changes: list[SignalChange] = []

        for cid, cur in new_by_id.items():
            prev = self._prev_scored.get(cid)
            title = cur.get("question", cid)
            slug = cur.get("market_slug", "")

            if prev is None:
                # Market is new in the scored set — skip detailed diff
                continue

            cur_scores = cur.get("scores", {})
            prev_scores = prev.get("scores", {})

            # 1. Composite score jump / drop
            cur_comp = cur.get("composite_score", 0)
            prev_comp = prev.get("composite_score", 0)
            delta_comp = cur_comp - prev_comp
            if delta_comp > _SCORE_JUMP_THRESHOLD:
                changes.append(SignalChange(
                    market_id=cid,
                    market_title=title,
                    change_type="score_jump",
                    severity="high" if delta_comp > 15 else "medium",
                    description=(
                        f"Composite score rose {delta_comp:.0f} pts "
                        f"({prev_comp:.0f} → {cur_comp:.0f})"
                    ),
                    old_value=round(prev_comp, 1),
                    new_value=round(cur_comp, 1),
                    market_slug=slug,
                ))
            elif delta_comp < -_SCORE_JUMP_THRESHOLD:
                changes.append(SignalChange(
                    market_id=cid,
                    market_title=title,
                    change_type="score_drop",
                    severity="high" if delta_comp < -15 else "medium",
                    description=(
                        f"Composite score fell {abs(delta_comp):.0f} pts "
                        f"({prev_comp:.0f} → {cur_comp:.0f})"
                    ),
                    old_value=round(prev_comp, 1),
                    new_value=round(cur_comp, 1),
                    market_slug=slug,
                ))

            # 2. Trap risk spike (crossed above 60) / drop (crossed below 40)
            cur_trap = cur_scores.get("trap_risk", 0)
            prev_trap = prev_scores.get("trap_risk", 0)
            if cur_trap >= _TRAP_SPIKE_THRESHOLD and prev_trap < _TRAP_SPIKE_THRESHOLD:
                changes.append(SignalChange(
                    market_id=cid,
                    market_title=title,
                    change_type="trap_risk_spike",
                    severity="high",
                    description=(
                        f"Trap risk crossed above {_TRAP_SPIKE_THRESHOLD} "
                        f"({prev_trap:.0f} → {cur_trap:.0f})"
                    ),
                    old_value=round(prev_trap, 1),
                    new_value=round(cur_trap, 1),
                    market_slug=slug,
                ))
            elif cur_trap < _TRAP_DROP_THRESHOLD and prev_trap >= _TRAP_DROP_THRESHOLD:
                changes.append(SignalChange(
                    market_id=cid,
                    market_title=title,
                    change_type="trap_risk_drop",
                    severity="medium",
                    description=(
                        f"Trap risk dropped below {_TRAP_DROP_THRESHOLD} "
                        f"({prev_trap:.0f} → {cur_trap:.0f})"
                    ),
                    old_value=round(prev_trap, 1),
                    new_value=round(cur_trap, 1),
                    market_slug=slug,
                ))

            # 3. Momentum label changed
            cur_mom = cur.get("momentum_label", "flat")
            prev_mom = prev.get("momentum_label", "flat")
            if cur_mom != prev_mom:
                changes.append(SignalChange(
                    market_id=cid,
                    market_title=title,
                    change_type="momentum_shift",
                    severity="high" if "strong" in cur_mom else "medium",
                    description=f"Momentum shifted: {prev_mom} → {cur_mom}",
                    old_value=prev_mom,
                    new_value=cur_mom,
                    market_slug=slug,
                ))

            # 4. Large repricing (>10% 24h move that wasn't there before)
            cur_24h = abs(cur.get("price_change_1d", 0))
            prev_24h = abs(prev.get("price_change_1d", 0))
            if cur_24h > _REPRICING_THRESHOLD and prev_24h <= _REPRICING_THRESHOLD:
                direction = "up" if cur.get("price_change_1d", 0) > 0 else "down"
                changes.append(SignalChange(
                    market_id=cid,
                    market_title=title,
                    change_type="repricing",
                    severity="high",
                    description=(
                        f"Large repricing detected: 24h move {direction} "
                        f"{cur.get('price_change_1d', 0):+.1f}%"
                    ),
                    old_value=round(prev.get("price_change_1d", 0), 1),
                    new_value=round(cur.get("price_change_1d", 0), 1),
                    market_slug=slug,
                ))

            # 5. Spread change > 3pp
            cur_spread = cur.get("spread_pct", 0)
            prev_spread = prev.get("spread_pct", 0)
            spread_delta = cur_spread - prev_spread
            if abs(spread_delta) > _SPREAD_CHANGE_THRESHOLD:
                action = "widened" if spread_delta > 0 else "compressed"
                changes.append(SignalChange(
                    market_id=cid,
                    market_title=title,
                    change_type="spread_change",
                    severity="medium" if abs(spread_delta) < 6 else "high",
                    description=(
                        f"Spread {action} {abs(spread_delta):.1f}pp "
                        f"({prev_spread:.1f}% → {cur_spread:.1f}%)"
                    ),
                    old_value=round(prev_spread, 1),
                    new_value=round(cur_spread, 1),
                    market_slug=slug,
                ))

            # 6. Flow spike (flow score jumped >20)
            cur_flow = cur_scores.get("flow", 0)
            prev_flow = prev_scores.get("flow", 0)
            if cur_flow - prev_flow > _FLOW_SPIKE_THRESHOLD:
                changes.append(SignalChange(
                    market_id=cid,
                    market_title=title,
                    change_type="flow_spike",
                    severity="high" if cur_flow - prev_flow > 30 else "medium",
                    description=(
                        f"Flow score spiked {cur_flow - prev_flow:.0f} pts "
                        f"({prev_flow:.0f} → {cur_flow:.0f})"
                    ),
                    old_value=round(prev_flow, 1),
                    new_value=round(cur_flow, 1),
                    market_slug=slug,
                ))

            # 7. Execution quality improved materially (>15 pts)
            cur_exec = cur_scores.get("execution_quality", 0)
            prev_exec = prev_scores.get("execution_quality", 0)
            if cur_exec - prev_exec > _EXECUTION_IMPROVE_THRESHOLD:
                changes.append(SignalChange(
                    market_id=cid,
                    market_title=title,
                    change_type="execution_improved",
                    severity="low",
                    description=(
                        f"Execution quality improved {cur_exec - prev_exec:.0f} pts "
                        f"({prev_exec:.0f} → {cur_exec:.0f})"
                    ),
                    old_value=round(prev_exec, 1),
                    new_value=round(cur_exec, 1),
                    market_slug=slug,
                ))

        # 8. Bucket entry / exit (only if recommendations are provided)
        if new_bucket_map and self._prev_buckets:
            for bucket_name in set(new_bucket_map) | set(self._prev_buckets):
                cur_ids = new_bucket_map.get(bucket_name, set())
                prev_ids = self._prev_buckets.get(bucket_name, set())

                # Entries
                for cid in cur_ids - prev_ids:
                    m = new_by_id.get(cid, {})
                    title = m.get("question", cid)
                    slug = m.get("market_slug", "")
                    label = _bucket_label(bucket_name)
                    is_best_bet = bucket_name == "best_bet_now"
                    changes.append(SignalChange(
                        market_id=cid,
                        market_title=title,
                        change_type="new_best_bet" if is_best_bet else "bucket_entry",
                        severity="high" if is_best_bet else "medium",
                        description=f"Entered {label}",
                        old_value=None,
                        new_value=bucket_name,
                        market_slug=slug,
                    ))

                # Exits
                for cid in prev_ids - cur_ids:
                    m = self._prev_scored.get(cid, {})
                    title = m.get("question", cid)
                    slug = m.get("market_slug", "")
                    label = _bucket_label(bucket_name)
                    changes.append(SignalChange(
                        market_id=cid,
                        market_title=title,
                        change_type="bucket_exit",
                        severity="low",
                        description=f"Exited {label}",
                        old_value=bucket_name,
                        new_value=None,
                        market_slug=slug,
                    ))

        return changes

    def _prune(self):
        """Drop changes older than _MAX_AGE_SECONDS and trim to _MAX_CHANGES."""
        if not self._changes:
            return
        cutoff = datetime.now(timezone.utc).timestamp() - _MAX_AGE_SECONDS
        self._changes = [
            c for c in self._changes
            if _iso_to_epoch(c.timestamp) > cutoff
        ]
        if len(self._changes) > _MAX_CHANGES:
            self._changes = self._changes[-_MAX_CHANGES:]


# ── Helpers ─────────────────────────────────────────────────────────────────

_BUCKET_LABELS = {
    "best_bet_now": "Best Bet Now",
    "best_yes_setup": "Best YES Setup",
    "best_no_setup": "Best NO Setup",
    "best_momentum_continuation": "Momentum Continuation",
    "best_mean_reversion_candidate": "Mean Reversion",
    "best_whale_follow": "Whale Follow",
    "avoid_or_trap_markets": "Avoid / Trap",
    "best_execution_quality": "Best Execution",
    "strongest_flow_without_confirmation": "Flow w/o Confirmation",
    "strongest_conviction_with_good_execution": "Conviction + Execution",
}


def _bucket_label(name: str) -> str:
    return _BUCKET_LABELS.get(name, name.replace("_", " ").title())


def _iso_to_epoch(iso: str) -> float:
    """Fast ISO 8601 → epoch.  Handles the trailing +00:00 / Z."""
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


# Module-level singleton
signal_tracker = SignalTracker()
