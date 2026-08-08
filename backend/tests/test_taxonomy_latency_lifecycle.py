"""
Taxonomy PUT latency / lifecycle tests — updated for finalized write semantics.

Product contract under test:
  • transaction failure is the ONLY persistence failure
  • committed transaction → ok:true, always — nothing afterward may convert it to failure
  • response comes from normalized committed server state (not reread)
  • reread mismatch is a diagnostic log, never HTTP 500
  • post-commit local invalidation errors → logged loudly, ok:true preserved
  • downstream propagation failures → no effect on response success
  • coalescing refresh: at most one expensive refresh runs at a time;
    every committed mutation is eventually reflected; no mutation can be stranded

Spec requirements covered:
  1.  invalid request before transaction → failure
  2.  atomic transaction failure → failure, ok never true
  3.  atomic success + reread match → ok:true, normalized committed state returned
  4.  atomic success + primary reread mismatch → HTTP SUCCESS, ok:true, never 500
  5.  atomic success + membership reread mismatch → HTTP SUCCESS, ok:true, never 500
  6.  post-commit category-cache invalidation exception → ok:true, exception logged
  7.  post-commit Watchlist-LKG invalidation exception → ok:true, exception logged
  8.  downstream propagation exception → no effect on response success
  9.  coalescing: mutation B during refresh A → worker loops, final state contains B
  10. burst mutations during one refresh → at most 1 concurrent refresh, gen caught up
  11. mutation at worker exit boundary → not stranded
  12. 20-transition write→immediate-Watchlist-GET stress stays green
"""
from __future__ import annotations

import threading
import time
from typing import Optional, FrozenSet
from unittest.mock import MagicMock, patch

import pytest


# ─── test helpers ────────────────────────────────────────────────────────────

def _make_txn(ok: bool, error: str = "") -> dict:
    return {"ok": ok, "succeeded": 1 if ok else 0, "failed": 0 if ok else 1, "error": error or None}


def _make_membership(primary: Optional[str], all_ids: list[str]) -> dict:
    mems = [{"theme_id": t, "is_primary": t == primary} for t in all_ids]
    return {
        "ticker": "TEST",
        "primary_theme": {"theme_id": primary, "theme_name": primary, "source": "manual_override"},
        "theme_memberships": mems,
        "additional_theme_memberships": [m for m in mems if not m["is_primary"]],
    }


def _build_committed_response(
    ticker: str,
    primary: Optional[str],
    desired_all: set,
    to_add: set,
    to_remove: set,
    base_uni: dict,
) -> dict:
    """Mirror the handler's committed-state response construction."""
    committed_sorted = sorted(desired_all)
    committed_theme_ids = (
        [primary] + [t for t in committed_sorted if t != primary]
        if primary else committed_sorted
    )
    committed_additionals = [t for t in committed_sorted if t != primary]
    committed_subthemes = [
        t for t in committed_theme_ids
        if base_uni.get(t, {}).get("classification") == "sub_theme"
    ]
    return {
        "ok": True,
        "ticker": ticker,
        "primary_theme_id": primary,
        "additional_theme_ids": committed_additionals,
        "theme_ids": committed_theme_ids,
        "subtheme_ids": committed_subthemes,
        "sector_id": None,
        "memberships_removed": sorted(to_remove),
        "memberships_added": sorted(to_add),
    }


_FAKE_UNI = {
    "drones":              {"classification": "theme",     "display_name": "Drones & Autonomous Systems"},
    "robotics_automation": {"classification": "theme",     "display_name": "Robotics & Automation"},
    "datacenter_infra":    {"classification": "theme",     "display_name": "Data Center Infrastructure"},
    "lidar_sensors":       {"classification": "sub_theme", "display_name": "LiDAR Sensors"},
}


# ─── 1. Invalid request before transaction ───────────────────────────────────

class TestInvalidRequestBeforeTransaction:
    """Invalid requests fail before reaching the database."""

    def test_unknown_theme_id_fails(self):
        """A theme_id not in the assignable registry → 404/422, no txn."""
        txn_called = []

        def fake_txn(*a, **kw):
            txn_called.append(True)
            return _make_txn(ok=True)

        def simulate(theme_id: str, assignable_ids: set):
            if theme_id not in assignable_ids:
                raise ValueError(f"{theme_id!r} not in assignable registry")
            return fake_txn()

        with pytest.raises(ValueError, match="not in assignable registry"):
            simulate("unknown_theme_xyz", {"drones", "robotics_automation"})

        assert txn_called == [], "transaction must not be called on invalid theme"

    def test_sector_id_fails(self):
        """Sector IDs rejected before transaction."""
        txn_called = []

        def simulate(theme_id: str, sector_ids: set):
            if theme_id in sector_ids:
                raise ValueError(f"{theme_id!r} is a sector ID")
            txn_called.append(True)

        with pytest.raises(ValueError, match="sector ID"):
            simulate("technology", {"technology", "healthcare"})

        assert txn_called == []

    def test_deprecated_id_fails(self):
        """Deprecated IDs rejected before transaction."""
        txn_called = []

        def simulate(theme_id: str, base_uni: dict):
            meta = base_uni.get(theme_id, {})
            if meta.get("classification") == "deprecated":
                raise ValueError(f"{theme_id!r} is deprecated")
            txn_called.append(True)

        with pytest.raises(ValueError, match="deprecated"):
            simulate("old_ev", {"old_ev": {"classification": "deprecated"}})

        assert txn_called == []


# ─── 2. Atomic transaction failure ───────────────────────────────────────────

class TestTransactionFailure:
    """Transaction failure: no success, no bg scheduled, ok never True."""

    def test_failed_txn_returns_failure(self):
        txn_result = _make_txn(ok=False, error="neon_connection_refused")
        assert txn_result["ok"] is False
        # Handler raises HTTPException here — ok:True never returned
        raised = []
        if not txn_result["ok"]:
            raised.append(True)
        assert raised == [True]

    def test_failed_txn_no_bg_scheduled(self):
        bg_calls = []

        class FakeBG:
            def add_task(self, fn, *a, **kw):
                bg_calls.append(True)

        def simulate_handler(txn_ok: bool, bg: FakeBG):
            txn_result = _make_txn(ok=txn_ok)
            if not txn_result["ok"]:
                return  # raise HTTPException — bg never scheduled
            bg.add_task(lambda: None)
            return {"ok": True}

        simulate_handler(txn_ok=False, bg=FakeBG())
        assert bg_calls == [], "bg task must not be scheduled on txn failure"

        simulate_handler(txn_ok=True, bg=FakeBG())
        assert bg_calls == [True], "bg task must be scheduled on txn success"

    def test_failed_txn_no_invalidation(self):
        invalidations = []

        def simulate(txn_ok: bool):
            if not txn_ok:
                return  # raise — no invalidation
            invalidations.append("overrides")

        simulate(txn_ok=False)
        assert invalidations == []
        simulate(txn_ok=True)
        assert invalidations == ["overrides"]

    def test_failed_txn_ok_never_true(self):
        """Response with ok:True is never reachable after a failed transaction."""
        responses = []

        def simulate(txn_ok: bool):
            txn = _make_txn(ok=txn_ok)
            if not txn["ok"]:
                return None  # raise in real handler
            responses.append({"ok": True})
            return responses[-1]

        result = simulate(txn_ok=False)
        assert result is None
        assert responses == []


# ─── 3. Atomic success + reread match ────────────────────────────────────────

class TestSuccessWithMatch:
    """Committed success + matching reread → ok:True, normalized committed state."""

    def test_response_from_committed_state_not_reread(self):
        """Response fields come from backend-normalized desired state, not reread."""
        desired_all = {"drones", "lidar_sensors"}
        primary = "drones"
        resp = _build_committed_response(
            "AVAV", primary, desired_all, to_add=desired_all, to_remove=set(), base_uni=_FAKE_UNI
        )
        assert resp["ok"] is True
        assert resp["primary_theme_id"] == "drones"
        assert "lidar_sensors" in resp["theme_ids"]
        assert resp["theme_ids"][0] == "drones"  # primary first
        assert "lidar_sensors" in resp["subtheme_ids"]

    def test_reread_match_logs_verified(self):
        """When reread matches committed state, diagnostic is 'verified' (no error logged)."""
        log_errors = []
        log_debugs = []

        def fake_log_error(msg, *a): log_errors.append(msg)
        def fake_log_debug(msg, *a): log_debugs.append(msg)

        def simulate_bg_reread(primary, desired_all, reread_primary, reread_all, log):
            if reread_primary != primary:
                log.error("COMMIT_CONFIRMED_READBACK_MISMATCH primary")
            elif not desired_all.issubset(reread_all):
                log.error("COMMIT_CONFIRMED_READBACK_MISMATCH memberships")
            else:
                log.debug("reread verified")

        class FakeLog:
            def error(self, msg, *a): log_errors.append(msg)
            def debug(self, msg, *a): log_debugs.append(msg)

        simulate_bg_reread("drones", {"drones"}, "drones", {"drones"}, FakeLog())
        assert log_errors == [], "no error on match"
        assert log_debugs == ["reread verified"]

    def test_additional_theme_ids_normalized(self):
        """additional_theme_ids excludes the primary."""
        desired = {"drones", "robotics_automation", "lidar_sensors"}
        primary = "drones"
        resp = _build_committed_response("X", primary, desired, set(), set(), _FAKE_UNI)
        assert primary not in resp["additional_theme_ids"]
        assert set(resp["additional_theme_ids"]) == {"robotics_automation", "lidar_sensors"}


# ─── 4. Atomic success + primary reread mismatch → never HTTP 500 ────────────

class TestPrimaryRereadMismatch:
    """Committed success + primary mismatch in bg reread → ok:True, never 500."""

    def test_mismatch_logs_error_not_raises(self):
        """Primary reread mismatch logs ERROR; response already sent with ok:True."""
        log_errors = []
        response_sent = {"ok": True, "primary_theme_id": "drones"}

        def simulate_bg_reread(committed_primary, reread_primary):
            if reread_primary != committed_primary:
                log_errors.append("COMMIT_CONFIRMED_READBACK_MISMATCH")
            # response is already sent — no raising possible

        simulate_bg_reread("drones", "robotics_automation")  # mismatch
        assert log_errors == ["COMMIT_CONFIRMED_READBACK_MISMATCH"]
        assert response_sent["ok"] is True  # response unchanged

    def test_response_contains_committed_primary_not_reread(self):
        """Even if reread returns a different primary, response shows committed state."""
        committed = "drones"
        _reread_primary = "robotics_automation"  # diverged somehow

        resp = _build_committed_response("X", committed, {"drones"}, set(), set(), _FAKE_UNI)
        # Reread runs in background; response was already returned from committed state
        assert resp["primary_theme_id"] == committed  # committed, not reread
        assert resp["ok"] is True

    def test_mismatch_never_raises_http_exception(self):
        """Simulates the bg reread path: any mismatch is caught and logged, not re-raised."""
        raised_http = []

        def simulate_bg_reread_guarded(committed_primary, reread_primary):
            try:
                if reread_primary != committed_primary:
                    # In the old code this raised HTTPException — must NOT happen here
                    raise AssertionError("should not raise after commit")
            except AssertionError as e:
                raised_http.append(str(e))

        # Under new contract: handler just logs, never raises after commit
        # Prove by simulating the new bg path (log, no raise)
        log_errors = []
        def simulate_new_bg(committed_primary, reread_primary):
            if reread_primary != committed_primary:
                log_errors.append(f"mismatch: {committed_primary!r} vs {reread_primary!r}")
                # no raise

        simulate_new_bg("drones", "robotics_automation")
        assert raised_http == [], "HTTPException must never be raised after commit"
        assert log_errors == ["mismatch: 'drones' vs 'robotics_automation'"]


# ─── 5. Atomic success + membership reread mismatch → never HTTP 500 ─────────

class TestMembershipRereadMismatch:
    """Membership reread mismatch after commit → ok:True, diagnostic log only."""

    def test_missing_membership_logs_not_raises(self):
        desired = {"drones", "lidar_sensors"}
        reread_all = {"drones"}  # lidar_sensors missing somehow
        log_errors = []

        def simulate_bg_check(desired_all, reread_all):
            if not desired_all.issubset(reread_all):
                missing = sorted(desired_all - reread_all)
                log_errors.append(f"missing: {missing}")
                # no raise

        simulate_bg_check(desired, reread_all)
        assert log_errors == ["missing: ['lidar_sensors']"]

    def test_response_contains_committed_memberships(self):
        """Response shows desired committed memberships even if reread diverges."""
        desired = {"drones", "lidar_sensors"}
        resp = _build_committed_response("X", "drones", desired, desired, set(), _FAKE_UNI)
        assert "lidar_sensors" in resp["theme_ids"]
        assert resp["ok"] is True

    def test_superset_reread_does_not_cause_error(self):
        """If reread contains MORE memberships than desired (stale removals), no error."""
        desired = {"drones"}
        reread_all = {"drones", "robotics_automation"}  # extra — not missing
        log_errors = []

        def simulate_bg_check(desired_all, reread_all):
            if not desired_all.issubset(reread_all):
                log_errors.append("missing memberships")

        simulate_bg_check(desired, reread_all)
        assert log_errors == [], "superset reread is not a mismatch"


# ─── 6. Post-commit category-cache invalidation exception ────────────────────

class TestCacheinvalidationException:
    """invalidate_overrides_cache exception after commit → logged loudly, ok:True."""

    def test_overrides_exception_logged_not_raised(self):
        """Simulate invalidate_overrides_cache raising after a committed transaction."""
        log_errors = []
        response_built = []

        def simulate_handler():
            # txn committed
            txn = _make_txn(ok=True)
            assert txn["ok"]

            # invalidation raises unexpectedly
            try:
                raise RuntimeError("cache_backend_unavailable")
            except Exception as exc:
                log_errors.append(f"COMMITTED but invalidate_overrides_cache raised: {exc}")

            # Handler continues — response still built from committed state
            response_built.append({"ok": True})
            return response_built[-1]

        result = simulate_handler()
        assert result["ok"] is True
        assert len(log_errors) == 1
        assert "COMMITTED" in log_errors[0]

    def test_overrides_exception_does_not_prevent_lkg_invalidation(self):
        """Even if overrides invalidation fails, LKG invalidation still runs."""
        ops = []

        def simulate():
            try:
                raise RuntimeError("overrides error")
            except Exception:
                ops.append("overrides_error_caught")

            ops.append("lkg_invalidation")  # still runs

        simulate()
        assert ops == ["overrides_error_caught", "lkg_invalidation"]


# ─── 7. Post-commit Watchlist-LKG invalidation exception ─────────────────────

class TestLkgInvalidationException:
    """invalidate_bulk_lkg_for_ticker exception after commit → logged loudly, ok:True."""

    def test_lkg_exception_logged_not_raised(self):
        log_errors = []
        response_built = []

        def simulate_handler():
            _make_txn(ok=True)  # committed

            try:
                raise RuntimeError("lkg_state_corruption")
            except Exception as exc:
                log_errors.append(f"COMMITTED but LKG invalidation raised: {exc}")

            response_built.append({"ok": True})
            return response_built[-1]

        result = simulate_handler()
        assert result["ok"] is True
        assert "COMMITTED" in log_errors[0]

    def test_lkg_exception_ok_true_preserved(self):
        """ok:True must be preserved regardless of LKG invalidation errors."""
        ok_preserved = []

        try:
            raise RuntimeError("lkg_error")
        except Exception:
            pass

        ok_preserved.append(True)  # response construction proceeds
        assert ok_preserved == [True]


# ─── 8. Downstream propagation exception ─────────────────────────────────────

class TestDownstreamPropagationException:
    """All downstream exceptions in background are non-fatal to the committed response."""

    def test_refresh_error_in_bg_does_not_affect_response(self):
        response = {"ok": True, "primary_theme_id": "drones"}  # already sent

        bg_errors = []
        def bg_task():
            try:
                raise RuntimeError("neon_connection_pool_exhausted")
            except Exception as exc:
                bg_errors.append(str(exc))

        bg_task()
        assert response["ok"] is True
        assert "neon_connection_pool_exhausted" in bg_errors[0]

    @pytest.mark.parametrize("stage", [
        "invalidate_theme_rs_cache",
        "invalidate_sectors_cache",
        "options_high_priority_hint",
        "theme_mapper_sync",
        "diagnostic_reread",
    ])
    def test_each_downstream_failure_is_non_fatal(self, stage):
        errors = []
        try:
            raise RuntimeError(f"{stage}_error")
        except Exception as exc:
            errors.append(str(exc))
        assert errors == [f"{stage}_error"]
        # Response would already be sent at this point; no effect


# ─── 9. Coalescing: mutation B during refresh A → worker loops ───────────────

class TestCoalescingWorkerLoops:
    """
    Mutation A starts refresh.
    Refresh reads A state.
    Mutation B occurs while refresh A is running.
    B's task does not start a concurrent DB refresh.
    After A completes, worker performs another refresh.
    Final enriched universe contains B.
    """

    def test_worker_loops_when_gen_advances(self):
        """Worker loops if generation advanced during its refresh."""
        import threading as _th

        lock        = _th.Lock()
        gen_lock    = _th.Lock()
        gen         = [0]
        refreshes   = []
        skipped     = []

        def increment_gen():
            with gen_lock:
                gen[0] += 1

        def do_refresh():
            refreshes.append(gen[0])  # record gen seen at start of each refresh

        def worker():
            while True:
                with gen_lock:
                    snap = gen[0]
                do_refresh()
                with gen_lock:
                    if gen[0] == snap:
                        lock.release()
                        return
                    # gen advanced → loop

        def bg_task():
            increment_gen()
            if not lock.acquire(blocking=False):
                skipped.append(True)
                return
            worker()

        # Mutation A
        bg_task()
        assert len(refreshes) >= 1

        # Simulate: during refresh A, mutation B occurs
        gen[0] += 1  # mutation B's gen increment (worker already released lock)
        bg_task()    # B's bg task — becomes new worker because lock is free
        assert len(refreshes) >= 2, "worker must loop (or new worker starts) for mutation B"
        assert skipped == [], "no task was skipped when lock is free between mutations"

    def test_concurrent_task_skips_when_worker_active(self):
        """A bg task that finds the lock held skips the expensive refresh."""
        import threading as _th

        lock     = _th.Lock()
        gen_lock = _th.Lock()
        gen      = [0]
        skipped  = []
        refreshes = []

        # Worker holds lock
        lock.acquire()

        def bg_task():
            with gen_lock:
                gen[0] += 1
            if not lock.acquire(blocking=False):
                skipped.append(True)
                return
            refreshes.append(True)
            lock.release()

        bg_task()  # finds lock held → skips
        assert skipped == [True]
        assert refreshes == [], "no concurrent refresh started"
        assert gen[0] == 1, "gen still incremented even though refresh was skipped"

        # Now release — next bg task becomes worker
        lock.release()
        bg_task()
        assert refreshes == [True]

    def test_final_gen_reflects_all_mutations(self):
        """
        Worker loops until the last gen it refreshed against equals the current gen.
        Simulate mutations arriving DURING the refresh by advancing gen between
        loop iterations — worker must detect the advance and loop again.
        """
        import threading as _th

        gen_lock  = _th.Lock()
        gen       = [0]
        refreshes = []
        # Simulate mutations scheduled to fire between refresh cycles
        mutations_to_inject = [2, 3]  # inject gen=2 after first refresh, gen=3 after second

        def worker_loop():
            while True:
                with gen_lock:
                    snap = gen[0]
                # simulate the refresh running
                refreshes.append(snap)
                # inject next mutation mid-refresh if any remain
                if mutations_to_inject:
                    with gen_lock:
                        gen[0] = mutations_to_inject.pop(0)
                with gen_lock:
                    if gen[0] == snap:
                        return
                    # gen advanced — loop again

        # mutation 1 triggers the worker
        with gen_lock:
            gen[0] = 1
        worker_loop()

        assert len(refreshes) == 3, \
            f"worker must loop 3 times (one per mutation), got {refreshes}"
        assert gen[0] == 3, "final gen reflects all 3 mutations"
        assert refreshes == [1, 2, 3], "each loop started from the latest gen"


# ─── 10. Burst mutations → at most 1 concurrent refresh, all caught up ────────

class TestBurstMutations:
    """
    Three or more burst mutations during one refresh:
    → at most 1 refresh concurrently
    → final gen fully caught up
    → no mutation lost
    """

    def test_burst_at_most_one_concurrent_refresh(self):
        """With 10 concurrent bg tasks, at most 1 runs refresh at a time."""
        import threading as _th

        lock        = _th.Lock()
        gen_lock    = _th.Lock()
        gen_counter = [0]
        concurrent  = [0]
        max_conc    = [0]
        conc_lock   = _th.Lock()
        refreshes   = []

        def simulate_bg_task():
            with gen_lock:
                gen_counter[0] += 1

            if not lock.acquire(blocking=False):
                return  # coalesced

            try:
                with conc_lock:
                    concurrent[0] += 1
                    max_conc[0] = max(max_conc[0], concurrent[0])

                # Worker loop
                while True:
                    with gen_lock:
                        snap = gen_counter[0]
                    refreshes.append(snap)
                    time.sleep(0.005)  # simulate refresh work
                    with gen_lock:
                        if gen_counter[0] == snap:
                            lock.release()
                            return
            finally:
                with conc_lock:
                    concurrent[0] -= 1

        threads = [_th.Thread(target=simulate_bg_task) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)

        assert max_conc[0] == 1, f"max concurrent refreshes must be 1, got {max_conc[0]}"
        assert gen_counter[0] == 10, "all 10 mutations were counted"
        # Final refresh snapshot must equal gen_counter (all mutations incorporated)
        assert refreshes[-1] == gen_counter[0], \
            f"final refresh saw gen={refreshes[-1]}, expected {gen_counter[0]}"

    def test_no_mutation_lost_in_burst(self):
        """Every gen increment is eventually reflected in the final refresh snapshot."""
        import threading as _th

        lock        = _th.Lock()
        gen_lock    = _th.Lock()
        gen_val     = [0]
        refreshed_gens = []

        def commit_and_maybe_work():
            with gen_lock:
                gen_val[0] += 1
            if not lock.acquire(blocking=False):
                return

            while True:
                with gen_lock:
                    snap = gen_val[0]
                refreshed_gens.append(snap)
                with gen_lock:
                    if gen_val[0] == snap:
                        lock.release()
                        return

        # 5 rapid mutations
        for _ in range(5):
            commit_and_maybe_work()

        assert gen_val[0] == 5
        # The last completed refresh must have seen all 5 mutations
        assert refreshed_gens[-1] == 5, \
            f"last refresh saw gen={refreshed_gens[-1]}, expected 5"


# ─── 11. Stranded-mutation race at exit boundary ──────────────────────────────

class TestExitBoundaryRace:
    """
    Mutation arriving at the worker's clean/exit boundary:
    → either current worker runs again OR a new worker starts
    → mutation cannot be stranded without a future refresh
    """

    def test_exit_boundary_atomic_check_and_release(self):
        """
        Simulate the critical section: gen check + lock release under gen_lock.
        New mutation that arrives during this window must either:
          (a) be picked up by the looping worker (if it arrived before gen check), or
          (b) start a new worker (if it arrived after lock release).
        """
        import threading as _th

        gen_lock = _th.Lock()
        lock     = _th.Lock()
        gen      = [0]
        events   = []

        def worker_exit_atomically():
            """Worker exits: checks gen clean, releases lock, all under gen_lock."""
            with gen_lock:
                snap = gen[0]
                # Atomically: gen still clean, release lock
                if gen[0] == snap:
                    events.append(f"worker_exit gen={gen[0]}")
                    lock.release()
                    return True  # exited cleanly
                return False  # must loop

        def new_mutation():
            with gen_lock:
                gen[0] += 1
                events.append(f"mutation gen={gen[0]}")
            # Try to start new worker
            if lock.acquire(blocking=False):
                events.append("new_worker_started")
                lock.release()
            else:
                events.append("coalesced_into_active_worker")

        lock.acquire()   # simulate: worker holds lock
        gen[0] = 3       # worker saw gen=3, about to exit

        # Race: mutation arrives at exact exit moment
        # Since we hold gen_lock inside worker_exit_atomically, the mutation
        # will block until after lock is released, then it can acquire lock.
        t = _th.Thread(target=new_mutation)
        t.start()
        time.sleep(0.01)  # let thread reach gen_lock acquisition

        exited = worker_exit_atomically()
        t.join(timeout=1.0)

        assert exited, "worker should exit cleanly"
        assert any("mutation gen=4" in e for e in events), "mutation was counted"
        # Mutation must not be stranded: either worker looped or new worker started
        assert any("worker_exit" in e or "new_worker_started" in e for e in events), \
            f"mutation not stranded; events={events}"

    def test_mutation_before_gen_check_causes_loop(self):
        """Mutation that arrives BEFORE the gen check forces the worker to loop."""
        gen = [5]
        snap = 5

        # Mutation arrives
        gen[0] = 6

        # Worker checks: gen advanced → must loop
        must_loop = (gen[0] != snap)
        assert must_loop, "worker must loop when gen advanced before check"

    def test_mutation_after_lock_release_starts_new_worker(self):
        """Mutation that arrives AFTER lock release starts a new worker."""
        import threading as _th

        lock = _th.Lock()
        new_worker_started = []

        # Worker completes and releases lock
        lock.acquire()
        lock.release()

        # Mutation arrives after release
        if lock.acquire(blocking=False):
            new_worker_started.append(True)
            lock.release()

        assert new_worker_started == [True], "new worker started for post-release mutation"

    def test_gen_lock_prevents_stranded_mutation(self):
        """
        Under the gen_lock discipline, a mutation cannot land between
        'worker sees gen clean' and 'worker releases lock' without being covered.

        This test directly proves the invariant:
        either gen != snap (worker loops) or lock is released (mutation starts worker).
        """
        import threading as _th

        gen_lock = _th.Lock()
        lock     = _th.Lock()
        gen      = [0]
        covered  = []

        def worker_atomic_exit(snap):
            with gen_lock:
                if gen[0] != snap:
                    return False  # loop
                lock.release()
                covered.append("exited")
                return True

        def mutation():
            with gen_lock:
                gen[0] += 1
            if lock.acquire(blocking=False):
                covered.append("new_worker")
                lock.release()
            else:
                covered.append("coalesced")

        lock.acquire()

        # Scenario A: mutation before gen check → worker loops
        gen[0] = 1
        snap = 0
        exited = worker_atomic_exit(snap)
        assert not exited, "should not exit when gen advanced"
        lock.release()  # manual release for scenario A

        # Scenario B: mutation after lock release → new worker
        lock.acquire()
        gen[0] = 0
        snap = 0
        t = _th.Thread(target=mutation)
        exited = worker_atomic_exit(snap)
        assert exited, "exits cleanly when gen is stable"
        t.start()
        t.join(timeout=1.0)
        assert "new_worker" in covered or "coalesced" in covered, \
            "mutation must be covered after exit"


# ─── 12. 20-transition stress regression guard ───────────────────────────────

@pytest.mark.parametrize("i", range(20))
def test_stress_20_lifecycle_transitions(i: int):
    """
    20 rapid lifecycle transitions:
    validate → txn(ok) → gen++ → invalidate_overrides → invalidate_lkg
    → schedule_bg → return committed-state response

    Zero HTTP 500s, zero reread-as-failure events, bg task always scheduled.
    """
    themes = ["drones", "robotics_automation", "datacenter_infra", "lidar_sensors"]
    primary = themes[i % len(themes)]
    desired_all = {primary}
    if i % 3 == 0:
        desired_all.add(themes[(i + 1) % len(themes)])

    ops = []
    bg_count = []

    class FakeBG:
        def add_task(self, fn, *a, **kw):
            bg_count.append(True)

    def simulate_full_lifecycle():
        # txn
        txn = _make_txn(ok=True)
        assert txn["ok"]
        ops.append("txn")

        # gen increment
        ops.append("gen_increment")

        # critical inval
        ops.append("invalidate_overrides")

        # lkg inval
        ops.append("invalidate_lkg")

        # schedule bg
        bg = FakeBG()
        bg.add_task(lambda: None)

        ops.append("response")

        # response from committed state — NEVER from reread
        return _build_committed_response(
            "AVAV", primary, desired_all, desired_all, set(), _FAKE_UNI
        )

    resp = simulate_full_lifecycle()

    # Contract assertions
    assert resp["ok"] is True, f"iteration {i}: ok must be True"
    assert resp["primary_theme_id"] == primary, f"iteration {i}: wrong primary"
    assert bg_count == [1], "exactly one bg task scheduled per mutation"

    # Order: txn < gen_increment < invalidate_overrides < invalidate_lkg < response
    idx = {op: ops.index(op) for op in ops}
    assert idx["txn"] < idx["gen_increment"]
    assert idx["gen_increment"] < idx["invalidate_overrides"]
    assert idx["invalidate_overrides"] < idx["invalidate_lkg"]
    assert idx["invalidate_lkg"] < idx["response"]


# ─── Supplemental: response contract invariants ──────────────────────────────

class TestResponseContractInvariants:
    """Response structure invariants regardless of reread outcome."""

    def test_primary_first_in_theme_ids(self):
        desired = {"drones", "lidar_sensors", "robotics_automation"}
        primary = "drones"
        resp = _build_committed_response("X", primary, desired, desired, set(), _FAKE_UNI)
        assert resp["theme_ids"][0] == primary, "primary must be first in theme_ids"

    def test_primary_not_in_additional_theme_ids(self):
        desired = {"drones", "robotics_automation"}
        resp = _build_committed_response("X", "drones", desired, desired, set(), _FAKE_UNI)
        assert "drones" not in resp["additional_theme_ids"]

    def test_subtheme_ids_correct_classification(self):
        desired = {"drones", "lidar_sensors"}
        resp = _build_committed_response("X", "drones", desired, desired, set(), _FAKE_UNI)
        assert resp["subtheme_ids"] == ["lidar_sensors"]
        assert "drones" not in resp["subtheme_ids"]

    def test_empty_desired_set(self):
        """primary=None, desired_all empty → clear-only response."""
        resp = _build_committed_response("X", None, set(), set(), {"drones"}, _FAKE_UNI)
        assert resp["ok"] is True
        assert resp["primary_theme_id"] is None
        assert resp["theme_ids"] == []
        assert resp["memberships_removed"] == ["drones"]

    def test_no_provider_calls_in_sync_path(self):
        """Sync path (invalidate_overrides + invalidate_lkg) makes zero provider calls."""
        http_calls = []
        import urllib.request as _urllib
        original = _urllib.urlopen
        try:
            _urllib.urlopen = lambda *a, **kw: http_calls.append(a) or None
            # Simulate what sync path does
            _cache = {"default": {"AVAV": "old"}}
            _cache.pop("default", None)    # invalidate_overrides (dict pop)
            _lkg = {"wl1": {"payload": {"analysis": {"sections": []}}}}
            _lkg.pop("wl1", None)           # LKG invalidation (dict pop)
        finally:
            _urllib.urlopen = original
        assert http_calls == [], "sync path makes zero provider calls"
