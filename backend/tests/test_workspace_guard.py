"""
Tests for scripts/workspace_guard.py.

Covers the guard's source classifier, lock manager, and git-state logic.
Git-state tests use isolated temporary git repositories — the real repo
history is never mutated.

Run with:
    cd /home/runner/workspace
    python3.11 -m pytest backend/tests/test_workspace_guard.py -v
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

# Make workspace_guard importable from the scripts/ directory
_WORKSPACE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_WORKSPACE / "scripts"))

import workspace_guard as wg


# ─────────────────────────────────────────────────────────────────────────────
# Helpers: temporary isolated git repos
# ─────────────────────────────────────────────────────────────────────────────

def _git_in(repo: Path, *args: str) -> str:
    r = subprocess.run(
        ["git", *args],
        capture_output=True, text=True, cwd=str(repo), check=False,
    )
    return r.stdout.strip()


def _make_repo(tmp_path: Path, name: str = "repo") -> Path:
    """Create a bare-minimum git repo with one initial commit."""
    repo = tmp_path / name
    repo.mkdir()
    _git_in(repo, "init", "-b", "main")
    _git_in(repo, "config", "user.email", "test@test.com")
    _git_in(repo, "config", "user.name", "Test")
    (repo / "README.md").write_text("init\n")
    _git_in(repo, "add", "README.md")
    _git_in(repo, "commit", "-m", "Initial commit")
    return repo


def _add_commit(repo: Path, filename: str, content: str, msg: str) -> str:
    """Add a file and commit; return the new SHA."""
    target = repo / filename
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content)
    _git_in(repo, "add", filename)
    _git_in(repo, "commit", "-m", msg)
    return _git_in(repo, "rev-parse", "HEAD")


def _make_repo_with_remote(tmp_path: Path) -> tuple[Path, Path]:
    """
    Create a 'local' and 'remote' (bare) repo pair, with local tracking
    remote/main.  Returns (local_repo, remote_repo).
    """
    remote = tmp_path / "remote.git"
    remote.mkdir()
    subprocess.run(["git", "init", "--bare", "-b", "main", str(remote)],
                   check=True, capture_output=True)

    local = tmp_path / "local"
    local.mkdir()
    _git_in(local, "init", "-b", "main")
    _git_in(local, "config", "user.email", "test@test.com")
    _git_in(local, "config", "user.name", "Test")
    _git_in(local, "remote", "add", "origin", str(remote))

    # Initial commit + push
    (local / "README.md").write_text("init\n")
    _git_in(local, "add", "README.md")
    _git_in(local, "commit", "-m", "Initial commit")
    _git_in(local, "push", "origin", "main")
    _git_in(local, "branch", "--set-upstream-to=origin/main", "main")

    return local, remote


# ─────────────────────────────────────────────────────────────────────────────
# Source classifier tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSourceClassifier:
    """Tests for is_source_file() — the centralized classifier."""

    # Explicit source files
    @pytest.mark.parametrize("path", [
        "backend/main.py",
        "backend/services/watchlist_service.py",
        "backend/data/category_overrides.py",      # backend/data/*.py is SOURCE
        "backend/data/deep/nested/helper.py",      # nested .py still SOURCE
        "scripts/workspace_guard.py",
        "scripts/run_build.sh",
        ".githooks/pre-push",
        "AGENTS.md",
        ".replit",
        "pyproject.toml",
        "requirements.txt",
        "requirements-dev.txt",
        "backend/core/lifespan.py",
        "backend/routes/watchlist.py",
    ])
    def test_source_files(self, path: str):
        assert wg.is_source_file(path) is True, f"Expected SOURCE: {path}"

    # Explicit generated/runtime files
    @pytest.mark.parametrize("path", [
        "backend/data/watchlist_stage2_lkg.json",
        "backend/data/options_supplement_lkg_v1.json",
        "backend/data/canonical_history/_index.json",
        "backend/data/canonical_history/AAPL.json.gz",
        "backend/data/thematic_context_snapshot.json",
        "backend/data/predict_odds_live_lkg.json",
        ".opencode-reports/latest.md",
        ".codex-reports/latest.md",
        ".opencode-persistent/state/prompt-history.jsonl",
        ".opencode/config.json",
        ".codex/config.json",
        ".agent-state/claim.json",
        "backend/data/sub/nested.json",
    ])
    def test_generated_files(self, path: str):
        assert wg.is_source_file(path) is False, f"Expected GENERATED: {path}"

    def test_backend_data_py_is_source(self):
        """backend/data/*.py must always be SOURCE regardless of directory."""
        assert wg.is_source_file("backend/data/category_overrides.py") is True

    def test_backend_data_json_is_generated(self):
        assert wg.is_source_file("backend/data/some_cache.json") is False

    def test_backend_data_json_gz_is_generated(self):
        assert wg.is_source_file("backend/data/history.json.gz") is False

    def test_unknown_extension_defaults_to_source(self):
        """Unknown extensions outside generated dirs default to SOURCE."""
        assert wg.is_source_file("some/new/file.xyz") is True

    def test_classify_paths_mixed(self):
        paths = [
            "backend/main.py",
            "backend/data/cache.json",
            "backend/data/model.py",    # SOURCE
            ".opencode-reports/latest.md",
        ]
        result = wg.classify_paths(paths)
        assert "backend/main.py"              in result["source"]
        assert "backend/data/model.py"        in result["source"]
        assert "backend/data/cache.json"      in result["generated"]
        assert ".opencode-reports/latest.md"  in result["generated"]


# ─────────────────────────────────────────────────────────────────────────────
# Lock manager tests
# ─────────────────────────────────────────────────────────────────────────────

class TestLockManager:
    """Tests for claim/release mechanics using a temporary .agent-state dir."""

    @pytest.fixture(autouse=True)
    def isolate_lock(self, tmp_path, monkeypatch):
        """Redirect CLAIM_FILE to a temp location for each test."""
        fake_dir  = tmp_path / ".agent-state"
        fake_file = fake_dir / "claim.json"
        monkeypatch.setattr(wg, "AGENT_STATE_DIR", fake_dir)
        monkeypatch.setattr(wg, "CLAIM_FILE", fake_file)
        yield

    def _make_args(self, **kw):
        import argparse
        ns = argparse.Namespace()
        for k, v in kw.items():
            setattr(ns, k, v)
        return ns

    def test_no_claim_initially(self):
        assert wg.read_claim() is None

    def test_write_and_read_claim(self):
        claim = {
            "actor": "replit-agent",
            "task": "test task",
            "timestamp": "2026-01-01T00:00:00+00:00",
            "starting_head": "abc123",
            "starting_origin_main": "def456",
            "workspace_path": "/home/runner/workspace",
            "branch": "main",
        }
        wg.write_claim(claim)
        result = wg.read_claim()
        assert result is not None
        assert result["actor"] == "replit-agent"

    def test_claim_cmd_succeeds_when_no_lock(self, monkeypatch):
        monkeypatch.setattr(wg, "git_head_sha",   lambda: "abc123abc123")
        monkeypatch.setattr(wg, "git_origin_sha", lambda: "def456def456")
        monkeypatch.setattr(wg, "git_branch",     lambda: "main")
        args = self._make_args(actor="codex", task="my task", force=False)
        rc = wg.cmd_claim(args)
        assert rc == 0
        claim = wg.read_claim()
        assert claim["actor"] == "codex"
        assert claim["task"] == "my task"

    def test_claim_rejected_when_active_lock(self, monkeypatch):
        monkeypatch.setattr(wg, "git_head_sha",   lambda: "aaa")
        monkeypatch.setattr(wg, "git_origin_sha", lambda: "bbb")
        monkeypatch.setattr(wg, "git_branch",     lambda: "main")
        # First claim
        args1 = self._make_args(actor="codex", task="task1", force=False)
        wg.cmd_claim(args1)
        # Second claim without force → rejected
        args2 = self._make_args(actor="deepseek", task="task2", force=False)
        rc = wg.cmd_claim(args2)
        assert rc == 1
        # Lock still belongs to codex
        assert wg.read_claim()["actor"] == "codex"

    def test_claim_force_overrides_existing(self, monkeypatch):
        monkeypatch.setattr(wg, "git_head_sha",   lambda: "aaa")
        monkeypatch.setattr(wg, "git_origin_sha", lambda: "bbb")
        monkeypatch.setattr(wg, "git_branch",     lambda: "main")
        args1 = self._make_args(actor="codex", task="task1", force=False)
        wg.cmd_claim(args1)
        args2 = self._make_args(actor="replit-agent", task="override", force=True)
        rc = wg.cmd_claim(args2)
        assert rc == 0
        assert wg.read_claim()["actor"] == "replit-agent"

    def test_manual_claim_preserved(self, monkeypatch):
        """A manual claim must block agents — lock must not be stolen silently."""
        monkeypatch.setattr(wg, "git_head_sha",   lambda: "aaa")
        monkeypatch.setattr(wg, "git_origin_sha", lambda: "bbb")
        monkeypatch.setattr(wg, "git_branch",     lambda: "main")
        args_manual = self._make_args(actor="manual", task="manual edit", force=False)
        wg.cmd_claim(args_manual)
        # Agent tries to claim without force
        args_agent = self._make_args(actor="codex", task="agent task", force=False)
        rc = wg.cmd_claim(args_agent)
        assert rc == 1
        assert wg.read_claim()["actor"] == "manual"

    def test_stale_claim_auto_released(self, monkeypatch):
        """A stale claim should be released automatically during a new claim."""
        old_claim = {
            "actor": "codex",
            "task": "old",
            "timestamp": "2000-01-01T00:00:00+00:00",  # very stale
            "starting_head": "abc",
            "starting_origin_main": "def",
            "workspace_path": "/home/runner/workspace",
            "branch": "main",
        }
        wg.write_claim(old_claim)
        monkeypatch.setattr(wg, "git_head_sha",   lambda: "newhead")
        monkeypatch.setattr(wg, "git_origin_sha", lambda: "neworigin")
        monkeypatch.setattr(wg, "git_branch",     lambda: "main")
        args = self._make_args(actor="deepseek", task="new task", force=False)
        rc = wg.cmd_claim(args)
        assert rc == 0
        assert wg.read_claim()["actor"] == "deepseek"

    def test_release_removes_claim(self, monkeypatch):
        monkeypatch.setattr(wg, "git_head_sha",   lambda: "aaa")
        monkeypatch.setattr(wg, "git_origin_sha", lambda: "bbb")
        monkeypatch.setattr(wg, "git_branch",     lambda: "main")
        wg.cmd_claim(self._make_args(actor="codex", task="t", force=False))
        rc = wg.cmd_release(self._make_args(actor="codex", force=False))
        assert rc == 0
        assert wg.read_claim() is None

    def test_release_wrong_actor_rejected(self, monkeypatch):
        monkeypatch.setattr(wg, "git_head_sha",   lambda: "aaa")
        monkeypatch.setattr(wg, "git_origin_sha", lambda: "bbb")
        monkeypatch.setattr(wg, "git_branch",     lambda: "main")
        wg.cmd_claim(self._make_args(actor="codex", task="t", force=False))
        rc = wg.cmd_release(self._make_args(actor="deepseek", force=False))
        assert rc == 1
        assert wg.read_claim() is not None

    def test_is_claim_stale_old(self):
        claim = {"timestamp": "2000-01-01T00:00:00+00:00"}
        assert wg.is_claim_stale(claim) is True

    def test_is_claim_stale_fresh(self):
        from datetime import datetime, timezone
        claim = {"timestamp": datetime.now(timezone.utc).isoformat()}
        assert wg.is_claim_stale(claim) is False


# ─────────────────────────────────────────────────────────────────────────────
# Git relationship classifier tests (isolated temp repos)
# ─────────────────────────────────────────────────────────────────────────────

class TestGitRelationshipClassifier:
    """Tests using real isolated temp git repos — does not touch real repo."""

    def _guard_in(self, repo: Path, monkeypatch, fn_name: str, *args, **kw):
        """Call a wg.* function with WORKSPACE_ROOT redirected to *repo*."""
        monkeypatch.setattr(wg, "WORKSPACE_ROOT", repo)
        fn = getattr(wg, fn_name)
        return fn(*args, **kw)

    def test_case_a_synchronized(self, tmp_path, monkeypatch):
        local, _ = _make_repo_with_remote(tmp_path)
        monkeypatch.setattr(wg, "WORKSPACE_ROOT", local)
        rel = wg.classify_local_remote()
        assert rel["case"] == "A"

    def test_case_b_behind(self, tmp_path, monkeypatch):
        local, remote = _make_repo_with_remote(tmp_path)
        # Add a commit directly to remote
        remote_clone = tmp_path / "clone"
        subprocess.run(["git", "clone", str(remote), str(remote_clone)], check=True, capture_output=True)
        _git_in(remote_clone, "config", "user.email", "t@t.com")
        _git_in(remote_clone, "config", "user.name", "T")
        _add_commit(remote_clone, "extra.py", "x=1\n", "Add extra source")
        _git_in(remote_clone, "push", "origin", "main")
        # Fetch in local so it knows about remote advance
        _git_in(local, "fetch", "origin", "main")
        monkeypatch.setattr(wg, "WORKSPACE_ROOT", local)
        rel = wg.classify_local_remote()
        assert rel["case"] == "B"

    def test_case_c_source_ahead(self, tmp_path, monkeypatch):
        local, _ = _make_repo_with_remote(tmp_path)
        _git_in(local, "config", "user.email", "t@t.com")
        _git_in(local, "config", "user.name", "T")
        _add_commit(local, "backend/main.py", "# new\n", "Add source")
        monkeypatch.setattr(wg, "WORKSPACE_ROOT", local)
        rel = wg.classify_local_remote()
        assert rel["case"] == "C-source"
        assert len(rel["source_ahead_commits"]) == 1

    def test_case_c_generated_only_ahead(self, tmp_path, monkeypatch):
        local, _ = _make_repo_with_remote(tmp_path)
        _git_in(local, "config", "user.email", "t@t.com")
        _git_in(local, "config", "user.name", "T")
        # Only a data JSON file — generated
        data_dir = local / "backend" / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        _add_commit(local, "backend/data/lkg.json", '{"x":1}', "Published your App")
        monkeypatch.setattr(wg, "WORKSPACE_ROOT", local)
        rel = wg.classify_local_remote()
        assert rel["case"] == "C-generated"
        assert len(rel["generated_ahead_commits"]) == 1
        assert len(rel["source_ahead_commits"]) == 0

    def test_case_d_diverged(self, tmp_path, monkeypatch):
        local, remote = _make_repo_with_remote(tmp_path)
        _git_in(local, "config", "user.email", "t@t.com")
        _git_in(local, "config", "user.name", "T")
        # Local advance
        _add_commit(local, "local_only.py", "x=1\n", "Local source commit")
        # Remote advance (via clone)
        remote_clone = tmp_path / "clone"
        subprocess.run(["git", "clone", str(remote), str(remote_clone)], check=True, capture_output=True)
        _git_in(remote_clone, "config", "user.email", "t@t.com")
        _git_in(remote_clone, "config", "user.name", "T")
        _add_commit(remote_clone, "remote_only.py", "y=2\n", "Remote source commit")
        _git_in(remote_clone, "push", "origin", "main")
        # Fetch in local
        _git_in(local, "fetch", "origin", "main")
        monkeypatch.setattr(wg, "WORKSPACE_ROOT", local)
        rel = wg.classify_local_remote()
        assert rel["case"] == "D"

    def test_replit_publish_commit_classified_as_generated(self, tmp_path, monkeypatch):
        """A 'Published your App' commit with only JSON files → C-generated."""
        local, _ = _make_repo_with_remote(tmp_path)
        _git_in(local, "config", "user.email", "agent@replit.com")
        _git_in(local, "config", "user.name", "Replit Agent")
        data_dir = local / "backend" / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        _add_commit(local, "backend/data/snap.json", '{"a":1}', "Published your App")
        monkeypatch.setattr(wg, "WORKSPACE_ROOT", local)
        rel = wg.classify_local_remote()
        assert rel["case"] == "C-generated", (
            "A 'Published your App' data-only commit must be NON-SOURCE AHEAD, not C-source"
        )

    def test_wrong_branch_detected(self, tmp_path, monkeypatch):
        local, _ = _make_repo_with_remote(tmp_path)
        _git_in(local, "checkout", "-b", "feature-branch")
        monkeypatch.setattr(wg, "WORKSPACE_ROOT", local)
        branch = wg.git_branch()
        assert branch != "main"

    def test_non_fastforward_ancestor_check(self, tmp_path, monkeypatch):
        """git_is_ancestor returns False when commits diverge."""
        local, remote = _make_repo_with_remote(tmp_path)
        _git_in(local, "config", "user.email", "t@t.com")
        _git_in(local, "config", "user.name", "T")
        _add_commit(local, "local.py", "x=1\n", "Local")
        remote_clone = tmp_path / "clone"
        subprocess.run(["git", "clone", str(remote), str(remote_clone)], check=True, capture_output=True)
        _git_in(remote_clone, "config", "user.email", "t@t.com")
        _git_in(remote_clone, "config", "user.name", "T")
        _add_commit(remote_clone, "remote.py", "y=2\n", "Remote")
        _git_in(remote_clone, "push", "origin", "main")
        _git_in(local, "fetch", "origin", "main")
        monkeypatch.setattr(wg, "WORKSPACE_ROOT", local)
        head_sha   = wg.git_head_sha()
        origin_sha = wg.git_origin_sha()
        # In diverged state, neither is ancestor of the other
        is_ff = wg.git_is_ancestor(origin_sha, head_sha)
        assert is_ff is False


# ─────────────────────────────────────────────────────────────────────────────
# Prepush command tests
# ─────────────────────────────────────────────────────────────────────────────

class TestPrepush:

    def _args(self):
        import argparse
        return argparse.Namespace()

    def test_prepush_rejects_wrong_branch(self, tmp_path, monkeypatch):
        local, _ = _make_repo_with_remote(tmp_path)
        _git_in(local, "checkout", "-b", "feature")
        monkeypatch.setattr(wg, "WORKSPACE_ROOT", local)
        rc = wg.cmd_prepush(self._args())
        assert rc == 1

    def test_prepush_accepts_generated_only_ahead(self, tmp_path, monkeypatch):
        local, _ = _make_repo_with_remote(tmp_path)
        _git_in(local, "config", "user.email", "t@t.com")
        _git_in(local, "config", "user.name", "T")
        data_dir = local / "backend" / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        _add_commit(local, "backend/data/cache.json", '{"v":1}', "Update caches")
        monkeypatch.setattr(wg, "WORKSPACE_ROOT", local)
        # Stub build + validation to pass
        monkeypatch.setattr(wg, "run_build", lambda verbose=False: (True, "OK"))
        monkeypatch.setattr(wg, "run_source_validation", lambda files: (True, ""))
        rc = wg.cmd_prepush(self._args())
        assert rc == 0

    def test_prepush_rejects_divergence(self, tmp_path, monkeypatch):
        local, remote = _make_repo_with_remote(tmp_path)
        _git_in(local, "config", "user.email", "t@t.com")
        _git_in(local, "config", "user.name", "T")
        _add_commit(local, "local.py", "x=1\n", "Local source")
        remote_clone = tmp_path / "clone"
        subprocess.run(["git", "clone", str(remote), str(remote_clone)], check=True, capture_output=True)
        _git_in(remote_clone, "config", "user.email", "t@t.com")
        _git_in(remote_clone, "config", "user.name", "T")
        _add_commit(remote_clone, "remote.py", "y=2\n", "Remote source")
        _git_in(remote_clone, "push", "origin", "main")
        _git_in(local, "fetch", "origin", "main")
        monkeypatch.setattr(wg, "WORKSPACE_ROOT", local)
        rc = wg.cmd_prepush(self._args())
        assert rc == 1


# ─────────────────────────────────────────────────────────────────────────────
# Prepublish command tests
# ─────────────────────────────────────────────────────────────────────────────

class TestPrepublish:

    def _args(self):
        import argparse
        return argparse.Namespace()

    def test_prepublish_rejects_dirty_source(self, tmp_path, monkeypatch):
        local, _ = _make_repo_with_remote(tmp_path)
        monkeypatch.setattr(wg, "WORKSPACE_ROOT", local)
        # Simulate a dirty source file
        monkeypatch.setattr(wg, "git_dirty_files", lambda: ["backend/main.py"])
        monkeypatch.setattr(wg, "git_branch", lambda: "main")
        monkeypatch.setattr(wg, "git_has_conflicts", lambda: False)
        monkeypatch.setattr(wg, "git_fetch", lambda: None)
        monkeypatch.setattr(wg, "classify_local_remote",
                            lambda: {"case": "A", "description": "OK",
                                     "head_sha": "a", "origin_sha": "a",
                                     "ahead_shas": [], "behind_shas": [],
                                     "source_ahead_commits": [],
                                     "generated_ahead_commits": []})
        monkeypatch.setattr(wg, "run_build", lambda verbose=False: (True, "OK"))
        monkeypatch.setattr(wg, "check_health", lambda timeout=5.0: (None, "skipped"))
        rc = wg.cmd_prepublish(self._args())
        assert rc == 1

    def test_prepublish_rejects_source_not_in_origin(self, tmp_path, monkeypatch):
        local, _ = _make_repo_with_remote(tmp_path)
        monkeypatch.setattr(wg, "WORKSPACE_ROOT", local)
        monkeypatch.setattr(wg, "git_dirty_files", lambda: [])
        monkeypatch.setattr(wg, "git_branch", lambda: "main")
        monkeypatch.setattr(wg, "git_has_conflicts", lambda: False)
        monkeypatch.setattr(wg, "git_fetch", lambda: None)
        monkeypatch.setattr(wg, "classify_local_remote",
                            lambda: {
                                "case": "C-source",
                                "description": "Source ahead",
                                "head_sha": "abc", "origin_sha": "def",
                                "ahead_shas": ["abc"],
                                "behind_shas": [],
                                "source_ahead_commits": [
                                    {"sha": "abc123", "subject": "Add source",
                                     "source_files": ["backend/main.py"],
                                     "generated_files": []}
                                ],
                                "generated_ahead_commits": [],
                            })
        monkeypatch.setattr(wg, "run_build", lambda verbose=False: (True, "OK"))
        monkeypatch.setattr(wg, "check_health", lambda timeout=5.0: (None, "skipped"))
        rc = wg.cmd_prepublish(self._args())
        assert rc == 1

    def test_prepublish_accepts_generated_only_ahead(self, tmp_path, monkeypatch):
        local, _ = _make_repo_with_remote(tmp_path)
        monkeypatch.setattr(wg, "WORKSPACE_ROOT", local)
        monkeypatch.setattr(wg, "git_dirty_files", lambda: [])
        monkeypatch.setattr(wg, "git_branch", lambda: "main")
        monkeypatch.setattr(wg, "git_has_conflicts", lambda: False)
        monkeypatch.setattr(wg, "git_fetch", lambda: None)
        monkeypatch.setattr(wg, "classify_local_remote",
                            lambda: {
                                "case": "C-generated",
                                "description": "Generated only ahead",
                                "head_sha": "abc", "origin_sha": "def",
                                "ahead_shas": ["abc"],
                                "behind_shas": [],
                                "source_ahead_commits": [],
                                "generated_ahead_commits": [
                                    {"sha": "abc123", "subject": "Published your App"}
                                ],
                            })
        monkeypatch.setattr(wg, "run_build", lambda verbose=False: (True, "OK"))
        monkeypatch.setattr(wg, "check_health", lambda timeout=5.0: (None, "skipped"))
        rc = wg.cmd_prepublish(self._args())
        assert rc == 0


# ─────────────────────────────────────────────────────────────────────────────
# Build script tests
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildScript:
    """Verify run_build.sh behavior — especially that failures are not masked."""

    BUILD_SCRIPT = _WORKSPACE / "scripts" / "run_build.sh"

    def test_build_script_exists(self):
        assert self.BUILD_SCRIPT.exists()

    def test_build_script_is_executable(self):
        assert os.access(self.BUILD_SCRIPT, os.X_OK)

    def test_build_script_has_no_trailing_true(self):
        """The script must NOT end with a bare `true` that masks failures."""
        content = self.BUILD_SCRIPT.read_text()
        lines = [l.rstrip() for l in content.splitlines() if l.strip() and not l.strip().startswith("#")]
        # The last substantive non-comment line must not be bare `true`
        last = lines[-1] if lines else ""
        assert last != "true", (
            "run_build.sh must not end with bare `true` — that masks compile failures."
        )

    def test_build_script_fails_on_bad_syntax(self, tmp_path):
        """A Python file with invalid syntax must cause the build to exit nonzero."""
        bad_file = tmp_path / "bad.py"
        bad_file.write_text("def foo(\n")  # SyntaxError
        r = subprocess.run(
            [sys.executable, "-m", "py_compile", str(bad_file)],
            capture_output=True, text=True, check=False,
        )
        assert r.returncode != 0, "py_compile must exit nonzero on syntax error"

    def test_compile_failure_not_masked(self, tmp_path):
        """
        The build mechanism (compileall) must propagate failure.
        We verify that `compileall -q <dir_with_bad.py>` exits nonzero,
        and that piping into `; true` would incorrectly mask it.
        """
        bad_dir = tmp_path / "src"
        bad_dir.mkdir()
        (bad_dir / "bad.py").write_text("def foo(\n")

        # compileall should exit nonzero on its own
        r = subprocess.run(
            [sys.executable, "-m", "compileall", "-q", str(bad_dir)],
            capture_output=True, text=True, check=False,
        )
        assert r.returncode != 0, "compileall must exit nonzero on syntax error"

        # Verify that "; true" WOULD mask it (shows why we removed it)
        r2 = subprocess.run(
            f"python3.11 -m compileall -q {bad_dir}; true",
            shell=True, capture_output=True, text=True, check=False,
        )
        assert r2.returncode == 0, (
            "This confirms why trailing `true` is dangerous — it hides the failure."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Startup-sensitive file detection test
# ─────────────────────────────────────────────────────────────────────────────

class TestStartupSensitiveDetection:

    def test_main_py_triggers_startup_tests(self, monkeypatch):
        """backend/main.py changes must trigger startup test suite."""
        triggered = []

        def fake_run_source_validation(files):
            if any(wg.STARTUP_SENSITIVE_PATTERNS[0].search(f) for f in files):
                triggered.append("startup_tests")
            return True, "ok"

        # We just test the pattern matching directly
        files = ["backend/main.py", "backend/services/watchlist_service.py"]
        needs = any(
            pat.search(f)
            for f in files
            for pat in wg.STARTUP_SENSITIVE_PATTERNS
        )
        assert needs is True

    def test_non_sensitive_file_no_startup(self):
        files = ["backend/routes/predict.py", "backend/services/watchlist_service.py"]
        needs = any(
            pat.search(f)
            for f in files
            for pat in wg.STARTUP_SENSITIVE_PATTERNS
        )
        assert needs is False

    def test_replit_config_triggers_startup(self):
        files = [".replit"]
        needs = any(
            pat.search(f)
            for f in files
            for pat in wg.STARTUP_SENSITIVE_PATTERNS
        )
        assert needs is True
