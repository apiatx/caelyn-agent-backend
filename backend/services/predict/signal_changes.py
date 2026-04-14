"""
Prophetik Signal Engine — Signal Changes / Snapshot Diffing + Best Bet Stability.

Maintains an in-memory snapshot of the last scored-markets result and detects
meaningful changes between consecutive scoring cycles.  Entirely ephemeral:
state resets on server restart, no database required.

Change types detected (market-level):
    score_jump              — composite score rose >10 points
    score_drop              — composite score fell >10 points
    trap_risk_spike         — trap risk crossed above 60
    trap_risk_drop          — trap risk crossed below 40 from above
    momentum_shift          — momentum label changed
    repricing               — |24h price change| > 10%
    spread_change           — spread widened or compressed >3pp
    flow_spike              — flow score jumped >20 points
    execution_improved      — execution quality rose >15 points
    bucket_entry            — market entered any recommendation bucket
    bucket_exit             — market left a recommendation bucket

Change types detected (recommendation-level):
    best_bet_changed        — #1 Best Bet market replaced
    best_bet_direction_flip — #1 direction flipped YES/NO
    best_bet_score_change   — #1 composite score moved materially (>= 3 pts)
    best_bet_repricing      — #1 yes_pct moved materially (>= 5 pp)

Stability controls for best_bet_now[0]:
    - Challenger must outscore current by >= _BEST_BET_MIN_GAP (5 pts)
    - Challenger must hold #1 for >= _BEST_BET_PERSISTENCE (2) consecutive cycles
    - After a change, current held for _BEST_BET_HOLD_SECONDS (300 s)
    - Strong challenger (_BEST_BET_STRONG_OVERRIDE = 15 pts gap) bypasses hold
    - Top-2 within gap threshold → confidence = low

Usage (called from polymarket_intelligence):
    from services.predict.signal_changes import signal_tracker
    signal_tracker.update(scored_markets, recommendation_buckets)
    changes = signal_tracker.get_recent_changes()
    pinned  = signal_tracker.get_pinned_best_bet()
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Optional

_MAX_CHANGES = 50
_MAX_AGE_SECONDS = 1800  # 30 min

# Market-level thresholds
_SCORE_JUMP_THRESHOLD = 10
_TRAP_SPIKE_THRESHOLD = 60
_TRAP_DROP_THRESHOLD = 40
_REPRICING_THRESHOLD = 10
_SPREAD_CHANGE_THRESHOLD = 3
_FLOW_SPIKE_THRESHOLD = 20
_EXECUTION_IMPROVE_THRESHOLD = 15

# Recommendation-level thresholds
_REC_SCORE_MATERIAL_DELTA = 3   # composite_score change >= 3 pts → emit event
_REC_PRICE_MATERIAL_DELTA = 5   # yes_pct change >= 5 pp → emit event

# Best-bet stability controls
_BEST_BET_MIN_GAP = 5           # challenger must outscore current by this many pts
_BEST_BET_PERSISTENCE = 2       # challenger must be #1 for this many consecutive cycles
_BEST_BET_HOLD_SECONDS = 300    # cooldown: hold current best bet for 5 min after change
_BEST_BET_STRONG_OVERRIDE = 15  # challenger leading by this much bypasses cooldown


# ── SignalChange ──────────────────────────────────────────────────────────────

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


# ── Best Bet Stabilizer ───────────────────────────────────────────────────────

class _BestBetStabilizer:
    """
    Hysteresis + persistence + cooldown for best_bet_now[0].

    Prevents rapid flipping by requiring a challenger to:
      1. Outscore the current best bet by >= _BEST_BET_MIN_GAP
      2. Hold #1 for >= _BEST_BET_PERSISTENCE consecutive update cycles
      3. Wait out a _BEST_BET_HOLD_SECONDS cooldown after the last change
         (unless the lead is >= _BEST_BET_STRONG_OVERRIDE)

    Returns (stable_market, stability_metadata) on every update.
    """

    def __init__(self):
        self._current: Optional[dict] = None       # confirmed stable best bet
        self._previous: Optional[dict] = None      # previous stable best bet
        self._candidate: Optional[dict] = None     # challenger being tracked
        self._candidate_streak: int = 0
        self._last_change_epoch: float = 0.0
        self._last_change_reason: str = ""
        self._change_timestamp: Optional[str] = None

    def update(self, candidates: list[dict]) -> tuple[Optional[dict], dict]:
        """
        Given top candidates sorted by composite_score (desc),
        return (stable_best_bet_dict, stability_metadata_dict).
        """
        if not candidates:
            return None, {"stability_status": "no_data", "confidence": "none",
                          "changed_recently": False, "previous_best_bet": None,
                          "recent_change_reason": None, "recommendation_timestamp": None}

        now = time.time()
        raw_top = candidates[0]
        raw_top_id = raw_top.get("condition_id")
        raw_top_score = raw_top.get("composite_score", 0)
        second_score = candidates[1].get("composite_score", 0) if len(candidates) > 1 else 0
        low_confidence = (raw_top_score - second_score) < _BEST_BET_MIN_GAP

        # ── First ever call: accept immediately ──────────────────────────────
        if self._current is None:
            self._current = raw_top
            self._last_change_epoch = now
            self._change_timestamp = _now_iso()
            self._last_change_reason = "Initial recommendation"
            return raw_top, self._build_meta("stable", "low" if low_confidence else "high", now)

        current_id = self._current.get("condition_id")
        current_score = self._current.get("composite_score", 0)

        # ── Same market is still #1: refresh data, keep stable ───────────────
        if raw_top_id == current_id:
            self._current = raw_top          # update with fresh prices/scores
            self._candidate = None
            self._candidate_streak = 0
            conf = "low" if low_confidence else "high"
            return raw_top, self._build_meta("stable", conf, now)

        # ── Different market at #1: evaluate challenger ──────────────────────
        gap = raw_top_score - current_score
        in_cooldown = (now - self._last_change_epoch) < _BEST_BET_HOLD_SECONDS
        strong_override = gap >= _BEST_BET_STRONG_OVERRIDE

        # Track challenger streak
        if raw_top_id == (self._candidate or {}).get("condition_id"):
            self._candidate_streak += 1
        else:
            self._candidate = raw_top
            self._candidate_streak = 1

        # Cooldown blocks unless strong override
        if in_cooldown and not strong_override:
            meta = self._build_meta("held", "low" if low_confidence else "medium", now)
            meta["challenger"] = _slim_bet(raw_top)
            meta["challenger_gap"] = round(gap, 1)
            return self._current, meta

        # Gap too small to consider
        if gap < _BEST_BET_MIN_GAP:
            meta = self._build_meta("stable", "low", now)
            return self._current, meta

        # Gap clears but persistence not met
        if self._candidate_streak < _BEST_BET_PERSISTENCE and not strong_override:
            meta = self._build_meta("pending", "low", now)
            meta["challenger"] = _slim_bet(raw_top)
            meta["challenger_streak"] = self._candidate_streak
            meta["challenger_gap"] = round(gap, 1)
            return self._current, meta

        # ── Challenger accepted ───────────────────────────────────────────────
        old = self._current
        old_dir = old.get("direction", "?")
        new_dir = raw_top.get("direction", "?")
        old_title = _short_title(old.get("question", ""))
        new_title = _short_title(raw_top.get("question", ""))

        if old.get("condition_id") != raw_top.get("condition_id"):
            reason = (f"Best Bet changed from {old_dir} {old_title} "
                      f"to {new_dir} {new_title}")
        else:
            reason = (f"Best Bet score changed from "
                      f"{round(current_score):.0f} to {round(raw_top_score):.0f}")

        self._previous = old
        self._current = raw_top
        self._last_change_epoch = now
        self._last_change_reason = reason
        self._change_timestamp = _now_iso()
        self._candidate = None
        self._candidate_streak = 0

        conf = "low" if low_confidence else "high"
        return raw_top, self._build_meta("changed", conf, now)

    def _build_meta(self, status: str, confidence: str, now: float) -> dict:
        changed_recently = (now - self._last_change_epoch) < 600  # within 10 min
        return {
            "stability_status": status,
            "confidence": confidence,
            "changed_recently": changed_recently,
            "previous_best_bet": _slim_bet(self._previous),
            "recent_change_reason": self._last_change_reason if changed_recently else None,
            "recommendation_timestamp": self._change_timestamp,
        }


# ── SignalTracker ─────────────────────────────────────────────────────────────

class SignalTracker:
    """
    In-memory snapshot differ.  Thread-safe via a simple lock.

    Tracks both market-level signals AND recommendation-level changes
    (best_bet_now[0] identity, direction, score, yes_pct).
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._prev_scored: dict[str, dict] = {}
        self._prev_buckets: dict[str, set[str]] = {}
        self._prev_best_bet_rec: Optional[dict] = None   # last best_bet_now[0]
        self._changes: list[SignalChange] = []
        self._last_updated: Optional[str] = None
        self._last_update_epoch: float = 0.0
        self._stabilizer = _BestBetStabilizer()
        self._pinned_best_bet: Optional[dict] = None
        self._pinned_stability: dict = {}

    # ── Public API ───────────────────────────────────────────────────────────

    def update(
        self,
        scored_markets: list[dict],
        recommendation_buckets: Optional[dict] = None,
    ) -> list[dict]:
        """Compare against previous snapshot, detect changes, rotate snapshot."""
        now = datetime.now(timezone.utc)
        new_by_id: dict[str, dict] = {
            m["condition_id"]: m for m in scored_markets if "condition_id" in m
        }

        new_bucket_map: dict[str, set[str]] = {}
        if recommendation_buckets:
            for bucket_name, items in recommendation_buckets.items():
                if isinstance(items, list):
                    new_bucket_map[bucket_name] = {
                        m["condition_id"] for m in items if "condition_id" in m
                    }

        # Run stabilizer on best_bet_now candidates (sorted by composite_score desc)
        best_bet_candidates = recommendation_buckets.get("best_bet_now", []) if recommendation_buckets else []

        with self._lock:
            new_changes: list[SignalChange] = []

            if self._prev_scored:
                new_changes = self._diff(new_by_id, new_bucket_map)

            # Recommendation-level changes (best_bet_now[0] tracking)
            rec_changes = self._diff_best_bet_recommendation(
                best_bet_candidates, new_by_id
            )
            new_changes.extend(rec_changes)

            # Run stabilizer — produces stable best bet + metadata
            pinned, stability = self._stabilizer.update(best_bet_candidates)
            self._pinned_best_bet = pinned
            self._pinned_stability = stability

            # Update previous recommendation snapshot
            self._prev_best_bet_rec = best_bet_candidates[0] if best_bet_candidates else None

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
        with self._lock:
            self._prune()
            age = (
                round(time.time() - self._last_update_epoch, 1)
                if self._last_update_epoch else None
            )
            return {
                "changes": [c.to_dict() for c in self._changes],
                "change_count": len(self._changes),
                "last_updated": self._last_updated,
                "snapshot_age_seconds": age,
            }

    def get_pinned_best_bet(self) -> dict:
        """Return the stability-controlled best bet + metadata for the frontend."""
        with self._lock:
            return {
                "market": self._pinned_best_bet,
                "stability": self._pinned_stability,
            }

    # ── Diffing Logic ─────────────────────────────────────────────────────────

    def _diff_best_bet_recommendation(
        self,
        candidates: list[dict],
        new_by_id: dict[str, dict],
    ) -> list[SignalChange]:
        """
        Diff best_bet_now[0] specifically — tracks market identity, direction,
        composite_score, and yes_pct changes across consecutive cycles.
        """
        changes: list[SignalChange] = []
        if not candidates or self._prev_best_bet_rec is None:
            return changes

        cur = candidates[0]
        prev = self._prev_best_bet_rec
        cur_id = cur.get("condition_id")
        prev_id = prev.get("condition_id")
        cur_slug = cur.get("slug", "")

        # Best Bet market identity changed
        if cur_id != prev_id:
            old_dir = prev.get("direction", "?")
            new_dir = cur.get("direction", "?")
            old_title = _short_title(prev.get("question", prev_id))
            new_title = _short_title(cur.get("question", cur_id))
            changes.append(SignalChange(
                market_id=cur_id,
                market_title=cur.get("question", cur_id),
                change_type="best_bet_changed",
                severity="high",
                description=(
                    f"Best Bet changed from {old_dir} {old_title} "
                    f"to {new_dir} {new_title}"
                ),
                old_value={"id": prev_id, "direction": old_dir, "title": old_title},
                new_value={"id": cur_id, "direction": new_dir, "title": new_title},
                market_slug=cur_slug,
            ))
            return changes  # Don't also emit sub-field changes on market switch

        # Same market — check sub-fields
        # Direction flip
        cur_dir = cur.get("direction", "")
        prev_dir = prev.get("direction", "")
        if cur_dir and prev_dir and cur_dir != prev_dir and prev_dir != "AVOID":
            changes.append(SignalChange(
                market_id=cur_id,
                market_title=cur.get("question", cur_id),
                change_type="best_bet_direction_flip",
                severity="high",
                description=f"Best Bet direction flipped from {prev_dir} to {cur_dir}",
                old_value=prev_dir,
                new_value=cur_dir,
                market_slug=cur_slug,
            ))

        # Score change
        cur_score = cur.get("composite_score", 0)
        prev_score = prev.get("composite_score", 0)
        delta_score = cur_score - prev_score
        if abs(delta_score) >= _REC_SCORE_MATERIAL_DELTA:
            changes.append(SignalChange(
                market_id=cur_id,
                market_title=cur.get("question", cur_id),
                change_type="best_bet_score_change",
                severity="medium" if abs(delta_score) < 8 else "high",
                description=(
                    f"Best Bet score {'rose' if delta_score > 0 else 'fell'} "
                    f"from {prev_score:.0f} to {cur_score:.0f}"
                ),
                old_value=round(prev_score, 1),
                new_value=round(cur_score, 1),
                market_slug=cur_slug,
            ))

        # yes_pct repricing
        cur_yes = cur.get("yes_pct", 0)
        prev_yes = prev.get("yes_pct", 0)
        delta_yes = cur_yes - prev_yes
        if abs(delta_yes) >= _REC_PRICE_MATERIAL_DELTA:
            changes.append(SignalChange(
                market_id=cur_id,
                market_title=cur.get("question", cur_id),
                change_type="best_bet_repricing",
                severity="high" if abs(delta_yes) >= 10 else "medium",
                description=(
                    f"Best Bet odds moved {delta_yes:+.1f}pp "
                    f"(YES: {prev_yes:.1f}% → {cur_yes:.1f}%)"
                ),
                old_value=round(prev_yes, 1),
                new_value=round(cur_yes, 1),
                market_slug=cur_slug,
            ))

        return changes

    def _diff(
        self,
        new_by_id: dict[str, dict],
        new_bucket_map: dict[str, set[str]],
    ) -> list[SignalChange]:
        """Market-level snapshot diff (unchanged logic)."""
        changes: list[SignalChange] = []

        for cid, cur in new_by_id.items():
            prev = self._prev_scored.get(cid)
            title = cur.get("question", cid)
            slug = cur.get("slug", cur.get("market_slug", ""))

            if prev is None:
                continue

            cur_scores = cur.get("scores", {})
            prev_scores = prev.get("scores", {})

            # 1. Composite score jump / drop
            cur_comp = cur.get("composite_score", 0)
            prev_comp = prev.get("composite_score", 0)
            delta_comp = cur_comp - prev_comp
            if delta_comp > _SCORE_JUMP_THRESHOLD:
                changes.append(SignalChange(
                    market_id=cid, market_title=title, change_type="score_jump",
                    severity="high" if delta_comp > 15 else "medium",
                    description=f"Score rose {delta_comp:.0f} pts ({prev_comp:.0f} → {cur_comp:.0f})",
                    old_value=round(prev_comp, 1), new_value=round(cur_comp, 1), market_slug=slug,
                ))
            elif delta_comp < -_SCORE_JUMP_THRESHOLD:
                changes.append(SignalChange(
                    market_id=cid, market_title=title, change_type="score_drop",
                    severity="high" if delta_comp < -15 else "medium",
                    description=f"Score fell {abs(delta_comp):.0f} pts ({prev_comp:.0f} → {cur_comp:.0f})",
                    old_value=round(prev_comp, 1), new_value=round(cur_comp, 1), market_slug=slug,
                ))

            # 2. Trap risk spike / drop
            cur_trap = cur_scores.get("trap_risk", 0)
            prev_trap = prev_scores.get("trap_risk", 0)
            if cur_trap >= _TRAP_SPIKE_THRESHOLD and prev_trap < _TRAP_SPIKE_THRESHOLD:
                changes.append(SignalChange(
                    market_id=cid, market_title=title, change_type="trap_risk_spike",
                    severity="high",
                    description=f"Trap risk crossed {_TRAP_SPIKE_THRESHOLD} ({prev_trap:.0f} → {cur_trap:.0f})",
                    old_value=round(prev_trap, 1), new_value=round(cur_trap, 1), market_slug=slug,
                ))
            elif cur_trap < _TRAP_DROP_THRESHOLD and prev_trap >= _TRAP_DROP_THRESHOLD:
                changes.append(SignalChange(
                    market_id=cid, market_title=title, change_type="trap_risk_drop",
                    severity="medium",
                    description=f"Trap risk dropped below {_TRAP_DROP_THRESHOLD} ({prev_trap:.0f} → {cur_trap:.0f})",
                    old_value=round(prev_trap, 1), new_value=round(cur_trap, 1), market_slug=slug,
                ))

            # 3. Momentum label changed
            cur_mom = cur.get("momentum_label", "flat")
            prev_mom = prev.get("momentum_label", "flat")
            if cur_mom != prev_mom:
                changes.append(SignalChange(
                    market_id=cid, market_title=title, change_type="momentum_shift",
                    severity="high" if "strong" in cur_mom else "medium",
                    description=f"Momentum: {prev_mom} → {cur_mom}",
                    old_value=prev_mom, new_value=cur_mom, market_slug=slug,
                ))

            # 4. Large repricing
            cur_24h = abs(cur.get("price_change_1d", 0))
            prev_24h = abs(prev.get("price_change_1d", 0))
            if cur_24h > _REPRICING_THRESHOLD and prev_24h <= _REPRICING_THRESHOLD:
                direction = "up" if cur.get("price_change_1d", 0) > 0 else "down"
                changes.append(SignalChange(
                    market_id=cid, market_title=title, change_type="repricing",
                    severity="high",
                    description=f"Large repricing {direction} {cur.get('price_change_1d', 0):+.1f}%",
                    old_value=round(prev.get("price_change_1d", 0), 1),
                    new_value=round(cur.get("price_change_1d", 0), 1), market_slug=slug,
                ))

            # 5. Spread change
            cur_spread = cur.get("spread_pct", 0)
            prev_spread = prev.get("spread_pct", 0)
            spread_delta = cur_spread - prev_spread
            if abs(spread_delta) > _SPREAD_CHANGE_THRESHOLD:
                action = "widened" if spread_delta > 0 else "compressed"
                changes.append(SignalChange(
                    market_id=cid, market_title=title, change_type="spread_change",
                    severity="medium" if abs(spread_delta) < 6 else "high",
                    description=f"Spread {action} {abs(spread_delta):.1f}pp ({prev_spread:.1f}% → {cur_spread:.1f}%)",
                    old_value=round(prev_spread, 1), new_value=round(cur_spread, 1), market_slug=slug,
                ))

            # 6. Flow spike
            cur_flow = cur_scores.get("flow", 0)
            prev_flow = prev_scores.get("flow", 0)
            if cur_flow - prev_flow > _FLOW_SPIKE_THRESHOLD:
                changes.append(SignalChange(
                    market_id=cid, market_title=title, change_type="flow_spike",
                    severity="high" if cur_flow - prev_flow > 30 else "medium",
                    description=f"Flow spiked {cur_flow - prev_flow:.0f} pts ({prev_flow:.0f} → {cur_flow:.0f})",
                    old_value=round(prev_flow, 1), new_value=round(cur_flow, 1), market_slug=slug,
                ))

            # 7. Execution quality improved
            cur_exec = cur_scores.get("execution_quality", 0)
            prev_exec = prev_scores.get("execution_quality", 0)
            if cur_exec - prev_exec > _EXECUTION_IMPROVE_THRESHOLD:
                changes.append(SignalChange(
                    market_id=cid, market_title=title, change_type="execution_improved",
                    severity="low",
                    description=f"Execution improved {cur_exec - prev_exec:.0f} pts ({prev_exec:.0f} → {cur_exec:.0f})",
                    old_value=round(prev_exec, 1), new_value=round(cur_exec, 1), market_slug=slug,
                ))

        # 8. Bucket entry / exit
        if new_bucket_map and self._prev_buckets:
            for bucket_name in set(new_bucket_map) | set(self._prev_buckets):
                # Skip best_bet_now — handled by _diff_best_bet_recommendation
                if bucket_name == "best_bet_now":
                    continue
                cur_ids = new_bucket_map.get(bucket_name, set())
                prev_ids = self._prev_buckets.get(bucket_name, set())
                label = _bucket_label(bucket_name)

                for cid in cur_ids - prev_ids:
                    m = new_by_id.get(cid, {})
                    changes.append(SignalChange(
                        market_id=cid, market_title=m.get("question", cid),
                        change_type="bucket_entry", severity="medium",
                        description=f"Entered {label}",
                        old_value=None, new_value=bucket_name,
                        market_slug=m.get("slug", m.get("market_slug", "")),
                    ))

                for cid in prev_ids - cur_ids:
                    m = self._prev_scored.get(cid, {})
                    changes.append(SignalChange(
                        market_id=cid, market_title=m.get("question", cid),
                        change_type="bucket_exit", severity="low",
                        description=f"Exited {label}",
                        old_value=bucket_name, new_value=None,
                        market_slug=m.get("slug", m.get("market_slug", "")),
                    ))

        return changes

    def _prune(self):
        if not self._changes:
            return
        cutoff = datetime.now(timezone.utc).timestamp() - _MAX_AGE_SECONDS
        self._changes = [
            c for c in self._changes
            if _iso_to_epoch(c.timestamp) > cutoff
        ]
        if len(self._changes) > _MAX_CHANGES:
            self._changes = self._changes[-_MAX_CHANGES:]


# ── Helpers ───────────────────────────────────────────────────────────────────

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
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _short_title(question: str, max_len: int = 40) -> str:
    """Truncate a market question for use in change descriptions."""
    q = question.strip()
    return q if len(q) <= max_len else q[:max_len].rstrip() + "…"


def _slim_bet(m: Optional[dict]) -> Optional[dict]:
    """Return a minimal summary of a best-bet market for embedding in metadata."""
    if not m:
        return None
    return {
        "condition_id": m.get("condition_id"),
        "question": m.get("question", ""),
        "yes_pct": m.get("yes_pct"),
        "composite_score": m.get("composite_score"),
        "direction": m.get("direction"),
        "slug": m.get("slug", ""),
        "momentum_label": m.get("momentum_label"),
    }


# Module-level singleton
signal_tracker = SignalTracker()
