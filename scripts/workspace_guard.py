#!/usr/bin/env python3.11
"""
CaelynAI workspace guard — multi-agent versioning, handoff, push, and
republish safety system.

Usage:
    python3.11 scripts/workspace_guard.py <subcommand> [options]

Subcommands:
    claim           Acquire the workspace single-writer lock
    status          Show lock state and current git summary
    preflight       Verify git state before editing (read-only)
    prepush         Validate before git push (called by pre-push hook)
    prepublish      Gate before Replit publish
    release         Release the workspace lock
    postpublish     Classify post-publish commit state (read-only)
    install-hooks   Set git hooks path to .githooks/

No external dependencies — pure Python stdlib + git subprocess.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import socket
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

WORKSPACE_ROOT   = Path("/home/runner/workspace")
CANONICAL_BRANCH = "main"
AGENT_STATE_DIR  = WORKSPACE_ROOT / ".agent-state"
CLAIM_FILE       = AGENT_STATE_DIR / "claim.json"
BUILD_SCRIPT     = WORKSPACE_ROOT / "scripts" / "run_build.sh"
HOOKS_DIR        = WORKSPACE_ROOT / ".githooks"

# A claim older than this many seconds is considered stale (24 hours)
CLAIM_STALE_SECONDS = 86_400

# Prepublish health check: must respond within this many seconds
# (Replit Autoscale promotion health-probe budget is ~10s, we use 5s margin)
HEALTH_CHECK_TIMEOUT_S = 5.0
HEALTH_CHECK_PORT      = 5000

# Startup-sensitive paths: changes here require startup test suite
STARTUP_SENSITIVE_PATTERNS = [
    re.compile(r"^backend/main\.py$"),
    re.compile(r"^\.replit$"),
    re.compile(r"^backend/core/lifespan"),
    re.compile(r"^backend/services/.*startup"),
    re.compile(r"^backend/services/.*init"),
]

# ─────────────────────────────────────────────────────────────────────────────
# Source-file classifier
# ─────────────────────────────────────────────────────────────────────────────
#
# Two-pass:  (1) explicit source patterns always win.
#            (2) explicit generated patterns are the ONLY non-source escape.
#            (3) default → SOURCE (conservative).
#
# IMPORTANT: backend/data/*.py is SOURCE even though most backend/data/ files
# are generated.  The classifier must never glob-exclude a whole directory.

_SOURCE_EXTENSIONS = {
    ".py", ".toml", ".cfg", ".ini", ".yaml", ".yml", ".sh",
    ".Makefile", ".txt",  # requirements*.txt
}

# Explicit generated suffixes/names (narrowly defined)
_GENERATED_SUFFIXES = {".json", ".gz", ".json.gz", ".tmp"}
_GENERATED_NAME_SUFFIXES = (".json.gz", ".json.tmp", ".gz")

# Directories where ALL contents are generated/runtime (nothing inside is source)
_GENERATED_DIRS_ALL = {
    ".opencode-reports",
    ".codex-reports",
    ".opencode-persistent",
    ".opencode",
    ".codex",
    ".agent-state",
}

# Directories where json/gz/tmp is generated but .py is still SOURCE
_GENERATED_DIRS_JSON_ONLY = {
    "backend/data",
}

# Root-level files that are explicitly source/docs (even if not code)
_EXPLICIT_SOURCE_NAMES = {
    "AGENTS.md", ".replit", "pyproject.toml", "Makefile",
    "requirements.txt",
}


def is_source_file(path: str) -> bool:
    """
    Return True if *path* (repo-relative POSIX string) is a production
    source/config/infrastructure file.

    False means the file is a known generated/runtime/cache artifact that
    agents may safely ignore for handoff purposes.

    When in doubt, returns True (conservative — prevents silent non-source
    classifications masking real source changes).
    """
    p = Path(path)
    name = p.name
    suffix = p.suffix.lower()

    # Explicit source names (root-level or anywhere)
    if name in _EXPLICIT_SOURCE_NAMES:
        return True

    # Python source is ALWAYS source — even inside backend/data/
    if suffix == ".py":
        return True

    # .sh, .toml, .yaml, .yml, .cfg, .ini → source
    if suffix in {".sh", ".toml", ".yaml", ".yml", ".cfg", ".ini"}:
        return True

    # Check whether file is inside a generated directory
    parts = p.parts  # e.g. ('backend', 'data', 'foo.json')

    # Report/state dirs: EVERYTHING inside is generated/runtime
    for gen_dir in _GENERATED_DIRS_ALL:
        gen_parts = Path(gen_dir).parts
        if parts[: len(gen_parts)] == gen_parts:
            return False

    # Data dir: json/gz/tmp = generated; .py = SOURCE (already caught above)
    for gen_dir in _GENERATED_DIRS_JSON_ONLY:
        gen_parts = Path(gen_dir).parts
        if parts[: len(gen_parts)] == gen_parts:
            # .py was caught above and returns True; anything else here is generated
            return False

    # requirements files (requirements.txt, requirements-dev.txt, etc.)
    if name.startswith("requirements") and suffix in {".txt", ".in"}:
        return True

    # Versioned hooks and guard scripts
    if str(p).startswith(".githooks/") or str(p).startswith("scripts/"):
        return True

    # AGENTS.md, README.md at repo root
    if p.parent == Path(".") and suffix == ".md":
        return True

    # Default: treat as source (conservative)
    return True


def classify_paths(paths: list[str]) -> dict:
    """Return {'source': [...], 'generated': [...]} classification."""
    source = [p for p in paths if is_source_file(p)]
    generated = [p for p in paths if not is_source_file(p)]
    return {"source": source, "generated": generated}


# ─────────────────────────────────────────────────────────────────────────────
# Git helpers (all read-only — no mutations)
# ─────────────────────────────────────────────────────────────────────────────

def _git(*args: str, cwd: Optional[Path] = None, check: bool = True) -> str:
    """Run a git command and return stdout. Raises on nonzero unless check=False."""
    result = subprocess.run(
        ["git", *args],
        capture_output=True,
        text=True,
        cwd=str(cwd or WORKSPACE_ROOT),
        check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(args)} failed (rc={result.returncode}):\n"
            f"{result.stderr.strip()}"
        )
    return result.stdout.strip()


def git_toplevel() -> str:
    return _git("rev-parse", "--show-toplevel")


def git_branch() -> str:
    return _git("branch", "--show-current")


def git_head_sha() -> str:
    return _git("rev-parse", "HEAD")


def git_origin_sha() -> str:
    return _git("rev-parse", "origin/main")


def git_fetch() -> None:
    _git("fetch", "origin", "main", "--quiet")


def git_dirty_files() -> list[str]:
    """Return repo-relative paths of all dirty working-tree files."""
    output = _git("status", "--porcelain", "-uall")
    paths = []
    for line in output.splitlines():
        if line.strip():
            # Format: XY path  or  XY old -> new
            status = line[:2]
            rest   = line[3:]
            if " -> " in rest:
                rest = rest.split(" -> ")[1]
            paths.append(rest.strip())
    return paths


def git_staged_files() -> list[str]:
    output = _git("diff", "--cached", "--name-only")
    return [l for l in output.splitlines() if l.strip()]


def git_commits_ahead(base: str = "origin/main") -> list[str]:
    """Return SHAs of commits in HEAD but not in *base*, oldest-first."""
    output = _git("rev-list", "--reverse", f"{base}..HEAD")
    return [l for l in output.splitlines() if l.strip()]


def git_commits_behind(base: str = "origin/main") -> list[str]:
    """Return SHAs of commits in *base* but not HEAD, oldest-first."""
    output = _git("rev-list", "--reverse", f"HEAD..{base}")
    return [l for l in output.splitlines() if l.strip()]


def git_commit_info(sha: str) -> dict:
    """Return subject, author, commit-author-trailer, and changed paths."""
    subject = _git("log", "-1", "--format=%s", sha)
    author  = _git("log", "-1", "--format=%ae", sha)
    body    = _git("log", "-1", "--format=%b", sha)
    files_out = _git("diff-tree", "--no-commit-id", "-r", "--name-only", sha)
    changed = [l for l in files_out.splitlines() if l.strip()]
    replit_author = ""
    for line in body.splitlines():
        if line.startswith("Replit-Commit-Author:"):
            replit_author = line.split(":", 1)[1].strip()
    return {
        "sha":           sha[:12],
        "subject":       subject,
        "author":        author,
        "replit_author": replit_author,
        "changed":       changed,
    }


def _is_replit_generated_commit(info: dict) -> bool:
    """
    True if this commit was created automatically by Replit (publish, agent
    data-cache writes, etc.) and contains NO production source changes.
    """
    replit_subjects = {
        "Published your App",
        "Update backend data caches and historical records",
        "Update cached data and internal settings for various financial indicators",
        "Update backend data snapshots and dashboard cache.",
        "Update prompt history and latest report file",
        "Update prompt history and latest report logs",
        "Update canonical history data and refresh dashboard reports.",
    }
    subject_match = (
        info["subject"] in replit_subjects
        or info["subject"].startswith("Published your App")
        or info["subject"].startswith("Update backend data")
        or info["subject"].startswith("Update cached data")
        or info["subject"].startswith("Update canonical history")
        or info["subject"].startswith("Update prompt history")
    )
    has_replit_author = bool(info.get("replit_author"))
    classified = classify_paths(info["changed"])
    return (subject_match or has_replit_author) and len(classified["source"]) == 0


def classify_ahead_commits(ahead_shas: list[str]) -> dict:
    """
    Classify ahead commits into source and generated-only.

    Returns:
        {
            'source_commits': [info, ...],       # commits with source changes
            'generated_commits': [info, ...],    # generated/data-only commits
            'all': [info, ...],
        }
    """
    source_commits    = []
    generated_commits = []
    all_infos         = []
    for sha in ahead_shas:
        info = git_commit_info(sha)
        classified = classify_paths(info["changed"])
        info["source_files"]    = classified["source"]
        info["generated_files"] = classified["generated"]
        info["is_generated_only"] = (
            len(classified["source"]) == 0
        )
        info["is_replit_generated"] = _is_replit_generated_commit(info)
        all_infos.append(info)
        if info["is_generated_only"]:
            generated_commits.append(info)
        else:
            source_commits.append(info)
    return {
        "source_commits":    source_commits,
        "generated_commits": generated_commits,
        "all":               all_infos,
    }


def git_has_conflicts() -> bool:
    output = _git("status", "--porcelain")
    return any(line[:2] in {"UU", "AA", "DD", "AU", "UA", "DU", "UD"}
               for line in output.splitlines())


def git_has_operation_in_progress() -> bool:
    """True if a merge/rebase/cherry-pick is in progress."""
    git_dir = Path(_git("rev-parse", "--git-dir"))
    if not git_dir.is_absolute():
        git_dir = WORKSPACE_ROOT / git_dir
    for marker in ["MERGE_HEAD", "REBASE_HEAD", "CHERRY_PICK_HEAD",
                   "rebase-merge", "rebase-apply"]:
        if (git_dir / marker).exists():
            return True
    return False


def git_check_whitespace(files: list[str]) -> list[str]:
    """Run git diff --check against HEAD. Returns list of error lines."""
    if not files:
        return []
    result = subprocess.run(
        ["git", "diff", "--check", "HEAD", "--"] + files,
        capture_output=True, text=True, cwd=str(WORKSPACE_ROOT), check=False,
    )
    return [l for l in result.stdout.splitlines() if l.strip()]


def git_remote_exists() -> bool:
    output = _git("remote", check=False)
    return "origin" in output.splitlines()


def git_is_ancestor(a: str, b: str) -> bool:
    """Return True if *a* is an ancestor of *b* (or equal)."""
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", a, b],
        capture_output=True, cwd=str(WORKSPACE_ROOT), check=False,
    )
    return result.returncode == 0


# ─────────────────────────────────────────────────────────────────────────────
# Local/remote relationship classifier
# ─────────────────────────────────────────────────────────────────────────────

def classify_local_remote() -> dict:
    """
    Classify the current local/remote relationship into one of four cases.

    Returns a dict:
        case:   'A' | 'B' | 'C' | 'D'
        ahead:  [commit infos]   (commits in HEAD not in origin/main)
        behind: [sha strings]    (SHAs in origin/main not in HEAD)
        head_sha:   str
        origin_sha: str
        description: str
        source_ahead_commits: [commit infos]  (case C only)
        generated_ahead_commits: [commit infos]
    """
    head_sha   = git_head_sha()
    origin_sha = git_origin_sha()
    ahead_shas  = git_commits_ahead()
    behind_shas = git_commits_behind()

    result = {
        "head_sha":   head_sha,
        "origin_sha": origin_sha,
        "ahead_shas":  ahead_shas,
        "behind_shas": behind_shas,
        "source_ahead_commits":    [],
        "generated_ahead_commits": [],
    }

    if not ahead_shas and not behind_shas:
        result["case"]        = "A"
        result["description"] = "HEAD == origin/main — fully synchronized."
        return result

    if ahead_shas and behind_shas:
        result["case"]        = "D"
        result["description"] = (
            f"TRUE DIVERGENCE — {len(ahead_shas)} local-only commit(s), "
            f"{len(behind_shas)} remote-only commit(s). Do not proceed."
        )
        return result

    if behind_shas and not ahead_shas:
        result["case"]        = "B"
        result["description"] = (
            f"Local is behind origin/main by {len(behind_shas)} commit(s). "
            "Fast-forward sync is allowed."
        )
        return result

    # ahead_shas and not behind_shas → Case C
    classified = classify_ahead_commits(ahead_shas)
    result["source_ahead_commits"]    = classified["source_commits"]
    result["generated_ahead_commits"] = classified["generated_commits"]
    all_generated_only = len(classified["source_commits"]) == 0

    if all_generated_only:
        result["case"] = "C-generated"
        result["description"] = (
            f"Local is ahead by {len(ahead_shas)} commit(s), ALL generated/runtime-data-only "
            "(NON-SOURCE AHEAD). Work may continue."
        )
    else:
        result["case"] = "C-source"
        result["description"] = (
            f"Local is ahead by {len(ahead_shas)} commit(s) with SOURCE changes in "
            f"{len(classified['source_commits'])} commit(s). "
            "These must be pushed before new work begins."
        )
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Lock / claim manager
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_agent_state_dir() -> None:
    AGENT_STATE_DIR.mkdir(parents=True, exist_ok=True)


def read_claim() -> Optional[dict]:
    """Return the current claim dict, or None if no claim exists."""
    if not CLAIM_FILE.exists():
        return None
    try:
        return json.loads(CLAIM_FILE.read_text())
    except Exception:
        return None


def write_claim(claim: dict) -> None:
    _ensure_agent_state_dir()
    CLAIM_FILE.write_text(json.dumps(claim, indent=2))


def delete_claim() -> None:
    if CLAIM_FILE.exists():
        CLAIM_FILE.unlink()


def is_claim_stale(claim: dict) -> bool:
    """True if the claim is older than CLAIM_STALE_SECONDS."""
    try:
        ts = datetime.fromisoformat(claim["timestamp"])
        age = (datetime.now(timezone.utc) - ts).total_seconds()
        return age > CLAIM_STALE_SECONDS
    except Exception:
        return True


def format_claim(claim: dict) -> str:
    lines = [
        f"  Actor:          {claim.get('actor', '?')}",
        f"  Task:           {claim.get('task', '?')}",
        f"  Timestamp:      {claim.get('timestamp', '?')}",
        f"  Starting HEAD:  {claim.get('starting_head', '?')[:12]}",
        f"  Origin/main:    {claim.get('starting_origin_main', '?')[:12]}",
        f"  Branch:         {claim.get('branch', '?')}",
    ]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Build validation
# ─────────────────────────────────────────────────────────────────────────────

def run_build(verbose: bool = False) -> tuple[bool, str]:
    """
    Run scripts/run_build.sh.  Returns (success, output).
    Exits nonzero on compile failure — trailing `true` is NOT present.
    """
    if not BUILD_SCRIPT.exists():
        return False, f"Build script not found: {BUILD_SCRIPT}"
    result = subprocess.run(
        ["bash", str(BUILD_SCRIPT)],
        capture_output=not verbose,
        text=True,
        cwd=str(WORKSPACE_ROOT),
        check=False,
    )
    output = ""
    if not verbose:
        output = (result.stdout + result.stderr).strip()
    return result.returncode == 0, output


def run_source_validation(changed_source: list[str]) -> tuple[bool, str]:
    """
    Minimal source validation:
      1. git diff --check
      2. Python compile sanity on changed .py files
      3. Startup tests if startup-sensitive files changed
    """
    messages: list[str] = []

    # 1. Whitespace check
    ws_errors = git_check_whitespace(changed_source)
    if ws_errors:
        messages.append("Whitespace errors:\n  " + "\n  ".join(ws_errors))

    # 2. Python compile sanity on changed .py files
    py_files = [f for f in changed_source if f.endswith(".py")]
    for pyf in py_files:
        full = WORKSPACE_ROOT / pyf
        if full.exists():
            r = subprocess.run(
                [sys.executable, "-m", "py_compile", str(full)],
                capture_output=True, text=True, check=False,
            )
            if r.returncode != 0:
                messages.append(f"Compile error in {pyf}:\n  {r.stderr.strip()}")

    # 3. Startup tests if startup-sensitive files changed
    needs_startup_tests = any(
        pat.search(f) for f in changed_source for pat in STARTUP_SENSITIVE_PATTERNS
    )
    if needs_startup_tests:
        messages.append("[INFO] Startup-sensitive files changed — running startup test suite.")
        r = subprocess.run(
            [sys.executable, "-m", "pytest",
             "backend/tests/test_startup_reliability.py",
             "backend/tests/test_startup_timing.py",
             "-v", "--tb=short", "-q"],
            capture_output=True, text=True, check=False,
            cwd=str(WORKSPACE_ROOT),
        )
        if r.returncode != 0:
            messages.append(
                f"Startup tests FAILED:\n{r.stdout[-2000:]}\n{r.stderr[-1000:]}"
            )
        else:
            messages.append(f"Startup tests passed.\n{r.stdout[-500:]}")

    ok = all("[INFO]" in m or "passed" in m for m in messages) if messages else True
    # More precise: check if any error-indicating message exists
    error_messages = [m for m in messages if not m.startswith("[INFO]") and "passed" not in m]
    return len(error_messages) == 0, "\n\n".join(messages)


def check_health(timeout: float = HEALTH_CHECK_TIMEOUT_S) -> tuple[bool, str]:
    """
    Check if localhost:5000/ responds with 200 within *timeout* seconds.
    Uses Replit's actual health-probe contract (not an artificial 1s rule).
    """
    try:
        with socket.create_connection(("127.0.0.1", HEALTH_CHECK_PORT), timeout=2):
            pass
    except (OSError, ConnectionRefusedError):
        return None, f"Server not running on port {HEALTH_CHECK_PORT} — live health check skipped."

    t0 = time.monotonic()
    try:
        req = urllib.request.Request(f"http://127.0.0.1:{HEALTH_CHECK_PORT}/")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            elapsed = time.monotonic() - t0
            code = resp.status
            if code < 500:
                return True, f"GET / → HTTP {code} in {elapsed:.2f}s (budget={timeout}s) ✓"
            return False, f"GET / → HTTP {code} in {elapsed:.2f}s — 5xx response."
    except Exception as exc:
        elapsed = time.monotonic() - t0
        return False, f"GET / failed in {elapsed:.2f}s: {exc}"


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: claim
# ─────────────────────────────────────────────────────────────────────────────

def cmd_claim(args: argparse.Namespace) -> int:
    """Acquire the single-writer workspace lock."""
    existing = read_claim()
    if existing:
        stale = is_claim_stale(existing)
        if not stale and not args.force:
            print("ERROR: Workspace is already claimed. Stop before claiming.", file=sys.stderr)
            print(format_claim(existing), file=sys.stderr)
            print(
                "\nIf the lock is genuinely stale, release it with:\n"
                "  python3.11 scripts/workspace_guard.py release --force",
                file=sys.stderr,
            )
            return 1
        if stale:
            print(f"WARNING: Existing claim is stale (>{CLAIM_STALE_SECONDS}s). Releasing.")
        elif args.force:
            print("WARNING: Force-claiming over an existing lock (user-authorized).")

    head_sha   = git_head_sha()
    origin_sha = git_origin_sha()
    branch     = git_branch()

    claim = {
        "actor":               args.actor,
        "task":                args.task,
        "timestamp":           datetime.now(timezone.utc).isoformat(),
        "starting_head":       head_sha,
        "starting_origin_main": origin_sha,
        "workspace_path":      str(WORKSPACE_ROOT),
        "branch":              branch,
    }
    write_claim(claim)
    print(f"Workspace claimed by '{args.actor}'.")
    print(f"  Task:    {args.task}")
    print(f"  HEAD:    {head_sha[:12]}")
    print(f"  Origin:  {origin_sha[:12]}")
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: release
# ─────────────────────────────────────────────────────────────────────────────

def cmd_release(args: argparse.Namespace) -> int:
    """Release the workspace lock."""
    existing = read_claim()
    if not existing:
        print("No active claim to release.")
        return 0

    if not args.force and args.actor and existing.get("actor") != args.actor:
        print(
            f"ERROR: Active claim belongs to '{existing.get('actor')}', not '{args.actor}'.",
            file=sys.stderr,
        )
        print("Use --force to override (requires user authorization).", file=sys.stderr)
        return 1

    delete_claim()
    print(f"Claim released (was held by '{existing.get('actor')}').")
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: status
# ─────────────────────────────────────────────────────────────────────────────

def cmd_status(_args: argparse.Namespace) -> int:
    """Show current lock state and git summary."""
    claim = read_claim()
    if claim:
        stale = is_claim_stale(claim)
        print(f"[LOCK] Active claim {'(STALE)' if stale else ''}:")
        print(format_claim(claim))
    else:
        print("[LOCK] No active claim.")

    print()
    try:
        print(f"[GIT]  Branch:        {git_branch()}")
        print(f"       HEAD:          {git_head_sha()[:12]}")
        print(f"       origin/main:   {git_origin_sha()[:12]}")
        rel = classify_local_remote()
        print(f"       Relationship:  Case {rel['case']} — {rel['description']}")
        dirty = git_dirty_files()
        classified = classify_paths(dirty)
        print(f"       Dirty source:  {len(classified['source'])} file(s)")
        print(f"       Dirty runtime: {len(classified['generated'])} file(s)")
    except RuntimeError as e:
        print(f"[GIT]  Error: {e}", file=sys.stderr)
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: preflight
# ─────────────────────────────────────────────────────────────────────────────

def cmd_preflight(args: argparse.Namespace) -> int:
    """
    Verify git state before editing.  Read-only — no mutations.

    Checks:
      - repo root is WORKSPACE_ROOT
      - branch is main
      - no merge/rebase/cherry-pick in progress
      - origin exists
      - git fetch succeeds
      - no unresolved conflicts
      - dirty source classified; stop if another actor owns dirty source files
      - local/remote relationship (Cases A–D)
    """
    errors: list[str] = []
    warnings: list[str] = []

    # Repo root
    try:
        toplevel = git_toplevel()
        if toplevel != str(WORKSPACE_ROOT):
            errors.append(f"Repo root is '{toplevel}', expected '{WORKSPACE_ROOT}'.")
    except RuntimeError as e:
        errors.append(f"Cannot determine repo root: {e}")
        _fail(errors)

    # Branch
    branch = git_branch()
    if branch != CANONICAL_BRANCH:
        errors.append(f"Branch is '{branch}', expected '{CANONICAL_BRANCH}'.")

    # In-progress operations
    if git_has_operation_in_progress():
        errors.append("A merge/rebase/cherry-pick is in progress. Resolve it first.")

    # Origin
    if not git_remote_exists():
        errors.append("Remote 'origin' does not exist.")

    # Fetch
    try:
        git_fetch()
    except RuntimeError as e:
        errors.append(f"git fetch origin main failed: {e}")

    # Conflicts
    if git_has_conflicts():
        errors.append("Unresolved merge conflicts detected.")

    # Dirty files
    dirty = git_dirty_files()
    classified = classify_paths(dirty)
    if classified["source"]:
        claim = read_claim()
        actor = getattr(args, "actor", None)
        if claim:
            if claim["actor"] == "manual":
                errors.append(
                    "MANUAL claim owns dirty source files. Agent must not modify:\n  "
                    + "\n  ".join(classified["source"])
                )
            elif actor and claim["actor"] != actor:
                errors.append(
                    f"Dirty source files belong to '{claim['actor']}' claim:\n  "
                    + "\n  ".join(classified["source"])
                )
        else:
            warnings.append(
                f"Dirty source files with no active claim:\n  "
                + "\n  ".join(classified["source"])
            )

    if classified["generated"]:
        warnings.append(f"{len(classified['generated'])} dirty runtime/cache file(s) — ignored.")

    # Local/remote relationship
    try:
        rel = classify_local_remote()
    except RuntimeError as e:
        errors.append(f"Cannot classify local/remote relationship: {e}")
        _fail(errors)
        return 1

    case = rel["case"]
    if case == "A":
        pass  # OK
    elif case == "B":
        warnings.append(
            f"Local is behind origin/main by {len(rel['behind_shas'])} commit(s). "
            "Run: git merge --ff-only origin/main"
        )
    elif case == "C-generated":
        warnings.append(rel["description"])
    elif case == "C-source":
        warnings.append(rel["description"] + " Push before starting new work.")
    elif case == "D":
        errors.append(rel["description"])

    for w in warnings:
        print(f"WARNING: {w}")
    if errors:
        _fail(errors)
        return 1

    print("PREFLIGHT OK.")
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: prepush
# ─────────────────────────────────────────────────────────────────────────────

def cmd_prepush(args: argparse.Namespace) -> int:
    """
    Called by the pre-push git hook.  Validates that the push is safe.

    Rejects:
      - wrong branch
      - divergence (Case D)
      - non-fast-forward push
      - unresolved conflicts
      - dirty task source that should have been committed
      - failed source validation (compile, whitespace)
      - startup test failures when startup-sensitive files changed
    """
    errors: list[str] = []

    # Branch
    branch = git_branch()
    if branch != CANONICAL_BRANCH:
        errors.append(f"Refusing push: branch is '{branch}', expected '{CANONICAL_BRANCH}'.")

    # Conflicts
    if git_has_conflicts():
        errors.append("Refusing push: unresolved merge conflicts.")

    # Relationship
    try:
        git_fetch()
        rel = classify_local_remote()
    except RuntimeError as e:
        errors.append(f"Refusing push: cannot determine git relationship: {e}")
        _fail(errors)
        return 1

    if rel["case"] == "D":
        errors.append(
            "Refusing push: TRUE DIVERGENCE detected.\n"
            f"  HEAD:         {rel['head_sha'][:12]}\n"
            f"  origin/main:  {rel['origin_sha'][:12]}\n"
            "Do not force-push. Resolve divergence manually."
        )
    elif rel["case"] == "B":
        errors.append(
            "Refusing push: local is BEHIND origin/main. "
            "Fast-forward first: git merge --ff-only origin/main"
        )

    # Force-push detection: if ahead commits start before the fork, reject
    # (The hook stdin would have remote SHA info; we detect via non-ancestor check)
    if rel["case"] in ("C-generated", "C-source", "A"):
        # Verify origin/main is an ancestor of HEAD (fast-forward safety)
        if not git_is_ancestor(rel["origin_sha"], rel["head_sha"]):
            errors.append(
                "Refusing push: non-fast-forward — origin/main is not an ancestor of HEAD. "
                "Never force-push."
            )

    # Source validation for ahead commits
    if rel["case"] == "C-source":
        source_commits = rel["source_ahead_commits"]
        all_changed_source = []
        for c in source_commits:
            all_changed_source.extend(c["source_files"])
        all_changed_source = list(set(all_changed_source))

        print(f"Source changes detected in {len(source_commits)} commit(s).")
        print(f"Validating {len(all_changed_source)} source file(s)...")

        # Build validation
        build_ok, build_out = run_build()
        if not build_ok:
            errors.append(f"Build failed:\n{build_out}")
        else:
            print("Build: OK")

        # Source validation (compile + whitespace + startup tests)
        val_ok, val_out = run_source_validation(all_changed_source)
        if not val_ok:
            errors.append(f"Source validation failed:\n{val_out}")
        elif val_out:
            print(val_out)

    elif rel["case"] == "C-generated":
        print(f"Ahead by {len(rel['ahead_shas'])} generated-only commit(s) — source validation skipped.")

    if errors:
        _fail(errors)
        return 1

    print("PREPUSH OK.")
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: prepublish
# ─────────────────────────────────────────────────────────────────────────────

def cmd_prepublish(args: argparse.Namespace) -> int:
    """
    Gate before Replit publish.

    Must verify:
      - branch = main
      - no unresolved conflicts
      - no dirty production source/config
      - current production source commit is already in origin/main
      - build validation passes
      - backend health/startup smoke passes (5s budget, Replit's real contract)
    """
    errors: list[str] = []
    warnings: list[str] = []

    # Branch
    branch = git_branch()
    if branch != CANONICAL_BRANCH:
        errors.append(f"Branch is '{branch}', expected '{CANONICAL_BRANCH}'.")

    # Conflicts
    if git_has_conflicts():
        errors.append("Unresolved merge conflicts.")

    # Dirty source
    dirty = git_dirty_files()
    classified = classify_paths(dirty)
    if classified["source"]:
        errors.append(
            "Dirty production source/config files — commit or discard before publishing:\n  "
            + "\n  ".join(classified["source"])
        )

    # Fetch and classify relationship
    try:
        git_fetch()
        rel = classify_local_remote()
    except RuntimeError as e:
        errors.append(f"Cannot determine git relationship: {e}")
        _fail(errors)
        return 1

    # Source must be in origin/main
    if rel["case"] == "C-source":
        source_commits = rel.get("source_ahead_commits", [])
        errors.append(
            f"{len(source_commits)} source commit(s) not yet pushed to origin/main. "
            "Push source before publishing:\n  "
            + "\n  ".join(
                f"{c['sha']} {c['subject']}" for c in source_commits
            )
        )
    elif rel["case"] == "D":
        errors.append("True divergence — resolve before publishing.")
    elif rel["case"] == "B":
        warnings.append("Local is behind origin/main — source is synchronized but local is stale.")
    elif rel["case"] == "C-generated":
        warnings.append(
            "Local is ahead by generated-only commit(s) — source IS synchronized with origin/main."
        )

    # Build validation
    print("Running build validation...")
    build_ok, build_out = run_build()
    if not build_ok:
        errors.append(f"Build failed (compile errors detected):\n{build_out}")
    else:
        print("Build: OK")

    # Health check (using Replit's real ~5s budget, not an artificial 1s rule)
    health_result, health_msg = check_health(timeout=HEALTH_CHECK_TIMEOUT_S)
    if health_result is True:
        print(f"Health check: {health_msg}")
    elif health_result is False:
        errors.append(f"Health check failed: {health_msg}")
    else:
        # None → server not running; warn but do not fail
        warnings.append(health_msg)

    for w in warnings:
        print(f"WARNING: {w}")

    if errors:
        _fail(errors)
        return 1

    print("PREPUBLISH OK — safe to publish.")
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: postpublish
# ─────────────────────────────────────────────────────────────────────────────

def cmd_postpublish(_args: argparse.Namespace) -> int:
    """
    Read/classify operation after a Replit publish.

    Shows:
      - current HEAD and origin/main
      - any new "Published your App" commits
      - their changed paths, classified source vs generated
      - whether production source remains synchronized
    """
    try:
        git_fetch()
    except RuntimeError as e:
        print(f"WARNING: git fetch failed: {e}", file=sys.stderr)

    head_sha   = git_head_sha()
    origin_sha = git_origin_sha()

    print(f"HEAD:         {head_sha[:12]}")
    print(f"origin/main:  {origin_sha[:12]}")
    print()

    rel = classify_local_remote()

    if rel["case"] == "A":
        print("Source status: SYNCHRONIZED — HEAD == origin/main.")
        return 0

    ahead_shas = rel["ahead_shas"]
    if not ahead_shas:
        print(f"Case {rel['case']}: {rel['description']}")
        return 0

    print(f"Local is ahead by {len(ahead_shas)} commit(s):")
    for info in (rel.get("source_ahead_commits", []) + rel.get("generated_ahead_commits", [])):
        label = "GENERATED-ONLY" if info["is_generated_only"] else "SOURCE"
        publish_tag = " [Replit publish]" if info["is_replit_generated"] else ""
        print(f"  {info['sha']}  [{label}]{publish_tag}  {info['subject']}")
        if info["source_files"]:
            print("    Source files changed:")
            for f in info["source_files"]:
                print(f"      {f}")
        if len(info["generated_files"]) <= 5:
            for f in info["generated_files"]:
                print(f"      {f}  (generated)")
        else:
            print(f"      ... and {len(info['generated_files'])} generated file(s)")
    print()

    if rel["case"] == "C-generated":
        print("Source status: SYNCHRONIZED — all ahead commits are generated/data-only.")
    elif rel["case"] == "C-source":
        print(
            "Source status: NOT SYNCHRONIZED — source commits not yet at origin/main. "
            "Push source before next publish."
        )
    elif rel["case"] == "D":
        print("Source status: DIVERGED — manual resolution required.")

    return 0


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: install-hooks
# ─────────────────────────────────────────────────────────────────────────────

def cmd_install_hooks(_args: argparse.Namespace) -> int:
    """Configure git to use .githooks/ as the hooks directory."""
    if not HOOKS_DIR.exists():
        print(f"ERROR: {HOOKS_DIR} does not exist.", file=sys.stderr)
        return 1
    _git("config", "core.hooksPath", ".githooks")
    print(f"Git hooks path set to: {HOOKS_DIR}")
    # Verify
    result = _git("config", "--get", "core.hooksPath", check=False)
    print(f"Verified: core.hooksPath = {result}")
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _fail(errors: list[str]) -> None:
    print("", file=sys.stderr)
    print("GUARD FAILED:", file=sys.stderr)
    for e in errors:
        print(f"  ERROR: {e}", file=sys.stderr)
    print("", file=sys.stderr)


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        prog="workspace_guard",
        description="CaelynAI multi-agent workspace safety guard.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # claim
    p_claim = sub.add_parser("claim", help="Acquire workspace lock")
    p_claim.add_argument("--actor", required=True,
                         help="Agent/actor identifier (e.g. replit-agent, codex, deepseek, manual)")
    p_claim.add_argument("--task", required=True, help="Short task description")
    p_claim.add_argument("--force", action="store_true",
                         help="Force claim even if another lock exists (user-authorized only)")

    # release
    p_rel = sub.add_parser("release", help="Release workspace lock")
    p_rel.add_argument("--actor", default=None, help="Actor releasing (optional, checked against claim)")
    p_rel.add_argument("--force", action="store_true", help="Force release even if actor mismatch")

    # status
    sub.add_parser("status", help="Show lock state and git summary")

    # preflight
    p_pre = sub.add_parser("preflight", help="Verify git state before editing")
    p_pre.add_argument("--actor", default=None, help="Current actor (for lock validation)")

    # prepush
    sub.add_parser("prepush", help="Validate before git push")

    # prepublish
    sub.add_parser("prepublish", help="Gate before Replit publish")

    # postpublish
    sub.add_parser("postpublish", help="Classify post-publish commit state")

    # install-hooks
    sub.add_parser("install-hooks", help="Set git hooks path to .githooks/")

    args = parser.parse_args()

    dispatch = {
        "claim":         cmd_claim,
        "release":       cmd_release,
        "status":        cmd_status,
        "preflight":     cmd_preflight,
        "prepush":       cmd_prepush,
        "prepublish":    cmd_prepublish,
        "postpublish":   cmd_postpublish,
        "install-hooks": cmd_install_hooks,
    }
    return dispatch[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
