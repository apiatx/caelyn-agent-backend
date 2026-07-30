# CaelynAI Backend — Coding Agent Operating Rules

## Agent identity and report routing

These rules apply to both:

- Codex CLI
- DeepSeek running through OpenCode

Determine the active agent runtime from the environment in which you are
operating.

Use exactly one agent-specific final report path:

- Codex CLI:
  `/home/runner/workspace/.codex-reports/latest.md`
- DeepSeek through OpenCode:
  `/home/runner/workspace/.opencode-reports/latest.md`

Never write to or overwrite the other agent's report file.

The assigned report file is an operational artifact, not a production file.
Never stage or commit it.

The active agent may create its assigned report directory if it does not exist.

Every completed task, including a read-only audit, must update the active
agent's assigned `latest.md` report unless the user explicitly says not to
create or update a report file.

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
4. preserve all pre-existing user or agent work
5. confirm local `main` is not behind or diverged from `origin/main`

The active coding agent may run `git fetch origin main --quiet` only to refresh
remote tracking information before checking ahead/behind status.

If local `main` is behind or diverged, stop and report it. Do not create a
workaround, clone, branch, worktree, merge, or alternate commit path.

## Git workflow

All edits must remain in the existing Replit working tree.

The active coding agent may:

- inspect Git state
- edit local files
- run tests and validation
- stage only exact approved task files
- create exactly one local commit on `main`
- write its assigned agent-specific `latest.md` report

The active coding agent must never:

- push
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
- use GitHub or `gh` write operations

The user personally runs:

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

If validation fails, do not commit unless the user explicitly approves a
partial or failing state.

If the user says audit only, read only, do not edit, or do not commit, follow
that instruction instead.

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

After completing the task, overwrite the report assigned to the active agent:

- Codex CLI:
  `/home/runner/workspace/.codex-reports/latest.md`
- DeepSeek through OpenCode:
  `/home/runner/workspace/.opencode-reports/latest.md`

For an implementation task, write the report after the local commit.

For an audit-only, read-only, or no-commit task, write the report after the
audit and validation are complete. Do not create a commit.

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
- complete task commit diff

For an audit-only, read-only, or no-commit task:

- include the exact files inspected
- state that no production files were modified
- mark the commit SHA and message as not applicable
- mark the complete task commit diff as not applicable

Use the committed patch as the source of truth for the final diff when a commit
exists.

Stop after writing the assigned report. For implementation tasks, this follows
the local commit. Never push.