# CaelynAI Backend — Coding Agent Operating Rules

## Agent identity and report routing

These rules apply to all coding agents and the user's own manual edits:

- Codex CLI
- DeepSeek running through OpenCode
- Replit Agent
- Any future coding agent or model provider

Determine the active agent runtime from the environment in which you are
operating.

Use exactly one agent-specific final report path:

- Codex CLI:
  `/home/runner/workspace/.codex-reports/latest.md`
- DeepSeek through OpenCode:
  `/home/runner/workspace/.opencode-reports/latest.md`
- Replit Agent:
  Report is delivered in the agent conversation (chat). Do NOT invent a
  separate Replit-specific file report path — no such path exists in this
  repository. If the user explicitly requests a file, create it at the path
  they specify.
- Future providers:
  Use a new provider-specific report directory only if the user explicitly
  establishes one. Never invent a path.

Never write to or overwrite another agent's report file.

The assigned report file is an operational artifact, not a production file.
Never stage or commit it.

The active agent may create its assigned report directory if it does not exist.

Every completed task, including a read-only audit, must update the active
agent's assigned report (file or conversation, per above) unless the user
explicitly says not to create or update a report.

## Agent report files and final output

Use the report location that matches the active coding agent:

- DeepSeek/OpenCode:
  `/home/runner/workspace/.opencode-reports/latest.md`

- Codex CLI:
  `/home/runner/workspace/.codex-reports/latest.md`

- Replit Agent:
  Deliver report in the agent conversation. No file write required unless
  the user requests one.

After completing the task and creating the approved local commit:

1. overwrite the matching `latest.md` report (Codex/OpenCode only)
2. verify that the report file exists and contains the current task heading
3. print the complete report into the agent conversation before stopping

For DeepSeek/OpenCode, run:

```bash
printf '\n===== BEGIN OPENCODE REPORT =====\n'
cat /home/runner/workspace/.opencode-reports/latest.md
printf '\n===== END OPENCODE REPORT =====\n'
```

For Codex CLI, run:

```bash
printf '\n===== BEGIN CODEX REPORT =====\n'
cat /home/runner/workspace/.codex-reports/latest.md
printf '\n===== END CODEX REPORT =====\n'
```

Do not merely state that the report exists.

Do not stop after showing only:

- the report path
- the line count
- the file size
- the commit summary

The complete report must be printed in the agent output.

## User authority and scope

The user is the pilot and final decision-maker.

Perform only the requested task. Do not expand scope, redesign adjacent systems,
perform opportunistic cleanup, or fix unrelated issues.

Assume the existing architecture, providers, endpoints, stores, caches,
background jobs, schedules, database relationships, and contracts exist for
deliberate reasons unless direct repository evidence proves otherwise.

When an unrelated bug, risk, or major inefficiency is discovered:

1. report it separately
2. explain its impact
3. do not fix it without approval

For ordinary, clearly scoped tasks, inspect the existing path and proceed
without requiring a separate audit-approval round.

Stop for approval when:

- the request would require a meaningful architecture change
- more than two production files appear necessary
- the existing path cannot satisfy the request
- a broad, destructive, or unrelated data mutation appears necessary
- the approved scope is contradicted by repository evidence

## Authorized Replit workspace

Work only in:

`/home/runner/workspace`

Work only on the existing local `main` branch.

Never create or use:

- another branch
- detached HEAD
- another clone
- a Git worktree
- a repository under `/tmp`
- a temporary packaging repository
- a separate copy of the project

Before editing:

1. confirm `git rev-parse --show-toplevel` is `/home/runner/workspace`
2. confirm `git branch --show-current` is `main`
3. run `git status -sb`
4. run `git log -3 --oneline --decorate`
5. run `git diff --cached --name-only`
6. preserve all pre-existing user or agent work

The active coding agent may run `git fetch origin main --quiet` only to refresh
remote tracking information.

## Workspace guard

This repository includes a canonical guard script at:

    scripts/workspace_guard.py

and versioned pre-push hook at:

    .githooks/pre-push

The guard exposes subcommands: `claim`, `status`, `preflight`, `prepush`,
`prepublish`, `release`, `postpublish`, `install-hooks`.

The git hook is installed with:

    python3.11 scripts/workspace_guard.py install-hooks

After installing, every `git push` (from any agent, shell, or manual command)
automatically calls the guard's `prepush` check.

The guard is the authority on all git-state decisions described below.
Do not bypass or duplicate its logic in shell scripts or agent prompts.

## Canonical task lifecycle

For every implementation task, follow this lifecycle in order:

    1. claim      — acquire single-writer lock
                    python3.11 scripts/workspace_guard.py claim \
                        --actor <actor> --task "<description>"

    2. preflight  — verify git state before editing
                    python3.11 scripts/workspace_guard.py preflight \
                        --actor <actor>

    3. edit       — make changes to authorized files only

    4. validate   — run tests, verify behavior

    5. stage      — stage exact approved task files only (never `git add .`)

    6. commit     — one focused commit on local main

    7. prepush    — guard is invoked automatically by the pre-push hook;
                    or run manually before pushing:
                    python3.11 scripts/workspace_guard.py prepush

    8. push       — git push origin main

    9. verify     — confirm HEAD, local main, origin/main, origin/HEAD all match

    10. release   — release workspace lock
                    python3.11 scripts/workspace_guard.py release --actor <actor>

    11. report    — write final report (file or conversation per agent type)

Do NOT leave a successfully completed source task as an unpushed local source
commit.

## Single-writer workspace lock

Only one agent or user may hold the workspace claim at a time.

Valid actor identifiers (free-form — future providers work without code changes):

    deepseek
    codex
    replit-agent
    manual
    gemini
    claude
    <any-future-provider>

If another active claim exists, STOP. Show the holder's actor, task, timestamp,
and starting SHA. Do not silently overwrite the lock.

For stale locks (>24 hours old), the guard auto-releases during a new claim.
For manual force-release: `workspace_guard.py release --force`.
Agents must never force-release without explicit user authorization.

## Manual user workflow

The user may claim the workspace directly:

    python3.11 scripts/workspace_guard.py claim \
        --actor manual --task "description of manual edit"

While a `manual` claim is active:

- All coding agents must refuse to modify production source files.
- Agents may continue read-only inspection and reporting.

After manual edits, the user can:

**Option A** — commit and push manually (the pre-push hook still runs):
```bash
git diff --check
git add <exact paths>
git commit -m "descriptive message"
git push origin main   # hook validates automatically
python3.11 scripts/workspace_guard.py release --actor manual
```

**Option B** — hand dirty files to an agent in a later prompt.
The agent must acknowledge the existing dirty source, claim the workspace,
and treat those files as part of the task scope.

Agents must NEVER silently discard or overwrite manual dirty source.

## Git handling — local/remote relationship

**The guard script decides.** Do not rely on model judgment alone to assess
git state. Run `python3.11 scripts/workspace_guard.py preflight` or inspect
`classify_local_remote()` output before editing.

Four cases are recognized:

**Case A — HEAD == origin/main**
Proceed normally.

**Case B — local behind origin/main, no divergence**
Allow only a true fast-forward synchronization:
```bash
git fetch origin main
git merge --ff-only origin/main
```
No merge commit may ever be created.

**Case C — local ahead of origin/main**
Inspect every local-only commit.

If local-only commits contain production source/config → **C-source**:
STOP unless these are the current actor's already-validated task commits
being explicitly completed.

If ALL local-only commits are Replit "Published your App" commits, reports,
runtime data, generated data, caches, LKG, snapshots, or agent state
→ **C-generated**:
Classify as NON-SOURCE AHEAD. Do not reset/rebase/revert those commits.
Work may continue.

**Case D — true divergence**
STOP. Never automatically:
- merge divergent history
- rebase
- reset
- force-push
- cherry-pick

Report both SHAs and differing commits. Wait for user resolution.

## Replit publish commits

Replit may automatically create `Published your App` commits on local `main`
during publishing. These commits typically contain only:

- runtime data
- cache/LKG files
- reports
- OpenCode state
- generated data

These commits may leave local `main` ahead of `origin/main` even when there is
ZERO unpushed production source.

Do NOT treat:

    local main ahead by a Replit publish/generated-data-only commit

as equivalent to:

    unpushed production source.

Do not delete, reset, rebase, or rewrite Replit publish commits.

After a publish, run:

    python3.11 scripts/workspace_guard.py postpublish

to read and classify the post-publish state. This is a read-only operation.

## Source file classification

The guard's `is_source_file()` function is the centralized classifier.
Use it — do not invent parallel classification logic.

Production/source includes:

- `*.py` anywhere (including `backend/data/*.py`)
- `*.toml`, `*.yaml`, `*.yml` (configuration)
- `.replit`
- `requirements*.txt`, `pyproject.toml`
- `scripts/*.sh`, `scripts/*.py`
- `.githooks/*`
- `AGENTS.md`
- `Makefile`, `*.cfg`, `*.ini`

**`backend/data/*.py` is SOURCE CODE even though the directory contains
generated JSON/gz files. Never classify an entire directory as generated.**

Generated/non-source (explicit narrowlist):

- `backend/data/**/*.json`
- `backend/data/**/*.json.gz`
- `backend/data/**/*.json.tmp`
- `.opencode-reports/**`
- `.codex-reports/**`
- `.opencode-persistent/**`
- `.opencode/**`
- `.codex/**`
- `.agent-state/**`

When in doubt, the classifier defaults to SOURCE (conservative).

## Git workflow

All edits must remain in the existing Replit working tree.

The active coding agent may:

- inspect Git state
- edit local files
- run tests and validation
- stage only exact approved task files
- create exactly one focused commit on `main`
- push the completed task commit to `origin/main`
- write its assigned agent-specific report

The active coding agent must never:

- pull
- merge
- rebase
- cherry-pick
- reset
- clean
- stash
- switch or create branches
- clone
- create worktrees
- modify remotes
- force-push
- push another branch
- open or merge pull requests
- use GitHub or `gh` write operations

For every completed implementation task, the active coding agent must:

1. validate the requested behavior
2. run `git diff --check`
3. stage only the exact approved task files
4. create exactly one focused commit on local `main`
5. push the completed commit using `git push origin main`
6. verify that local `main` and `origin/main` point to the pushed task commit
7. write the final report with the commit SHA and push result

Do not commit or push an incomplete, failing, partially validated, or temporary
debugging state unless the user explicitly requests that behavior.

Do not commit or push for:

- audit-only tasks
- read-only tasks
- tasks where the user explicitly says not to commit or push
- failed validation
- unresolved merge conflicts
- unrelated pre-existing production changes in an authorized task file

The only permitted remote Git write operation is:

`git push origin main`

Before committing:

1. run `git diff --check`
2. stage exact paths only
3. never use `git add .` or `git add -A`
4. never stage runtime data, caches, logs, `.replit`, `.codex-reports`,
   `.opencode-reports`, generated files, or unrelated dirty files
5. show the staged file list and staged diff
6. confirm only task-related files are staged

Create one descriptive local commit only after validation succeeds.

After pushing, verify with:

- `git status -sb`
- `git log -3 --oneline --decorate`

A successful implementation task must end with the new task commit present at:

- `HEAD`
- local `main`
- `origin/main`
- `origin/HEAD`

If validation fails, do not commit or push unless the user explicitly approves
a partial or failing state.

If the user says audit only, read only, do not edit, do not commit, or do not
push, follow that instruction instead.

## Pre-push hook

The pre-push hook at `.githooks/pre-push` delegates entirely to:

    python3.11 scripts/workspace_guard.py prepush

It applies to ALL pushes: DeepSeek, Codex, Replit Agent, manual `git push`,
and any future agent. It is never bypassed.

The prepush guard rejects:

- wrong branch (not `main`)
- true divergence (Case D)
- non-fast-forward push
- unresolved conflicts
- failed build (compile errors)
- failed source validation (whitespace, syntax)
- startup test failures when startup-sensitive files changed

Force-push is never permitted.

## Prepublish gate

Before every Replit publish, run:

    python3.11 scripts/workspace_guard.py prepublish

This gate refuses publish readiness unless:

- branch = main
- no unresolved conflicts
- no dirty production source/config
- all production source commits are already in origin/main
- build validation passes (scripts/run_build.sh exits 0)
- backend health smoke: GET / responds with non-5xx within 5 seconds
  (Replit's actual Autoscale health-probe budget — not an artificial 1s rule)

Do not add provider calls to the health route.
Do not simulate health checks without the running server.

## Build validation

The canonical build command is:

    bash scripts/run_build.sh

This script:

- compiles all backend Python source directories
- compiles `.pythonlibs`
- exits NONZERO on any compile failure
- has NO trailing `true` — compile failures are never masked

The same script is used by:

- Replit deployment build (`.replit [deployment].build`)
- `workspace_guard.py prepush` (when source files changed)
- `workspace_guard.py prepublish`

This eliminates local/deployment build drift.

## Startup-sensitive files

When any of these files change, prepush automatically includes the startup
test suite (`test_startup_reliability.py`, `test_startup_timing.py`):

- `backend/main.py`
- `.replit`
- `backend/core/lifespan*`
- `backend/services/*startup*` or `backend/services/*init*`

## Ground truth and architecture

Use repository files, actual API responses, provider responses, database reads,
logs, and tests as ground truth.

Do not:

- state an unproven hypothesis as the root cause
- infer production state from local code
- infer database state from models or schemas alone
- assume provider data is absent without inspecting the response
- assume local and published behavior are identical
- replace a working path because another design appears cleaner

For bugs, compare a working example with the broken example and identify the
first exact divergence.

Before changing code, trace the current:

- provider or source
- storage path
- writer
- reader
- endpoint
- serializer
- cache
- scheduler or background job
- consumer

Prefer correcting or extending the existing path.

Do not create any of the following without explicit approval containing
`ARCHITECTURE CHANGE APPROVED`:

- parallel data pipeline
- replacement contract
- new endpoint
- new database source, table, or column
- new cache
- new worker or scheduler
- new backfill
- new status-field system
- new provider call
- new dependency
- duplicate source of truth
- replacement persistence model

Default maximum:

- two production files
- one existing data path
- no architecture changes

Do not refactor, rename, reorganize, or broadly format unrelated code.

Do not add generalized infrastructure for a narrow bug or run a broad repair
for a targeted issue.

## Canonical data and API safety

Preserve the existing canonical source for every domain.

In particular:

- `public.watchlist.tickers` is authoritative Watchlist membership
- CSV data is not authoritative Watchlist membership
- caches are not interchangeable with canonical database state
- provider responses are not interchangeable with stored canonical state
- bulk Watchlist and ticker-detail responses serve different purposes
- earnings history and live-event state serve different purposes

Never replace canonical membership with CSV fallback logic.

Before changing a response contract, identify all consumers.

Do not:

- add large nested objects to bulk endpoints without measuring response size
- duplicate large objects in multiple response sections
- remove shared fields without auditing consumers
- add status or provenance fields unless requested
- casually change bulk-versus-detail responsibilities
- treat a timeout as proof that the underlying source is broken

When payload size matters, report HTTP status, elapsed time, response bytes, row
counts, and repeated structures.

## Database and persistent-state operations

The active coding agent may use existing database and application persistence
paths when the requested task clearly requires them.

The user's explicit task is sufficient authorization for ordinary, narrowly
scoped writes required to complete that task.

Stop and obtain approval before any mutation that is:

- not directly implied by the request
- broad rather than targeted
- destructive or difficult to reverse
- a migration or schema change not explicitly requested
- a backfill affecting unrelated records
- a manual production SQL repair
- a reset, deletion, truncation, or historical rewrite
- a broad cache invalidation
- a new source of truth or persistence path

Before requesting approval, report:

- exact table or store
- exact rows and predicate
- before state
- intended after state
- affected entities
- rollback or recovery behavior

Never use a broad database operation to solve a narrow UI or single-record bug.

Never expose secrets, tokens, API keys, passwords, database URLs, or complete
private payloads.

## Runtime and provider safety

Do not start or restart the full application merely for inspection when startup
jobs can mutate databases, caches, LKG files, or scheduled state.

Prefer isolated tests, pure-function tests, mocked providers, read-only public
endpoints, and read-only database inspection.

Do not add provider calls inside request paths when existing stored or cached
data is available.

Preserve:

- rate limits
- market-hours gates
- early-close behavior
- scheduler behavior
- existing cache contracts

For provider-result matching:

- validate event identity
- compare fiscal period and year when available
- use explicit date matching when labels are unavailable
- preserve partial results
- reject stale prior-period data
- never accept the first merely plausible row

## Validation

Validate:

- the reported broken example
- at least one existing working example
- one boundary or negative case
- exact endpoint behavior
- expected row or object counts
- response size and elapsed time when relevant
- no unexpected database writes
- no new provider calls
- no background-job regression
- preservation of existing consumers

Clearly distinguish task-related failures from unrelated pre-existing failures.

## Final report

After completing the task, deliver the report per the active agent's type
(file or conversation — see "Agent identity and report routing" above).

For an implementation task, write the report after the commit has been pushed
successfully to `origin/main`.

For an audit-only, read-only, no-commit, or no-push task, write the report after
the audit and validation are complete. Do not create or push a commit.

The report must contain:

- task requested
- completion status
- proven root cause
- existing path preserved
- exact files changed
- exact behavior changed
- behavior deliberately preserved
- validation commands and results
- database, provider, cache, and runtime effects
- risks and remaining issues
- final `git status -sb`
- commit SHA and message
- push command and result
- confirmation that the task commit is present at `HEAD`, local `main`,
  `origin/main`, and `origin/HEAD`
- complete task commit diff

For an audit-only, read-only, no-commit, or no-push task:

- include the exact files inspected
- state that no production files were modified
- mark the commit SHA and message as not applicable
- mark the push result as not applicable
- mark the complete task commit diff as not applicable

Use the committed patch as the source of truth for the final diff when a commit
exists.

Stop after writing the assigned report and confirming the push succeeded.
