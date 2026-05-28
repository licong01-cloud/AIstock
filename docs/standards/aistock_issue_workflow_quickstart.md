# AIstock Issue Workflow Quickstart

## Purpose

This quickstart is for Codex, Claude Code, Cursor, and any other CLI/IDE coding agent. When the user asks to submit, fix, triage, batch, finish, close, sync, or merge an AIstock BUG/GitHub Issue, do not improvise manual steps. Start with the repo-level orchestrator:

```powershell
python scripts/aistock_issue_workflow.py doctor
```

The skill/command/prompt layer is intentionally thin. The source of truth is `scripts/aistock_issue_workflow.py`; `scripts/issue_flow.py` remains the lower-level primitive helper.

Next hardening baseline: `docs/architecture/aistock_issue_workflow_hardening_plan_v2_1_20260526.md`. Before continuing lower-priority issue workflow R&D, prioritize the v2.1 client-stale detection, single-active-worktree guard, pre-PR gate, close-sync, cleanup, and timing telemetry phases.

## Trigger Examples

- `按规范修复 BUG-112，不要合入 main`
- `按规范修复 BUG-112，验证通过后创建 PR`
- `处理 open P0，能 batch 的 batch`
- `按 issue workflow 登记一个 GitHub Issue`
- `修复完成后创建 PR，不要合入 main`
- `验证通过后同步本地和 GitHub main`

## Client Entry Rules

### Codex

Use `.codex/skills/fix-aistock-issue` when available. If the skill is not loaded in a fresh window, run the same repo CLI directly:

```powershell
python F:\Dev\AIstock\scripts/aistock_issue_workflow.py doctor
```

### Claude Code

Use the repository command prompt at `.claude/commands/fix-aistock-issue.md`, or paste this minimal instruction:

```text
For AIstock issue work, first run:
python F:\Dev\AIstock\scripts/aistock_issue_workflow.py doctor
Then use run/resume from the same script. Do not manually explore the whole repo before reading the Context Pack.
```

### Generic CLI/IDE Agent

Read this quickstart, run `doctor`, then follow the returned `next_command`. All workflow artifacts are Markdown/JSON under `tmp/issue_workflow/<BUG-ID>/`.

## Install Client Entry Wrappers

After this workflow code is merged into the canonical checkout, install or refresh the global Codex skill wrapper with:

```powershell
python scripts/aistock_issue_workflow.py install-client --apply
```

Before merge, use the dry-run form only:

```powershell
python scripts/aistock_issue_workflow.py install-client
```

The install command copies the repo-local `.codex/skills/fix-aistock-issue` wrapper to `$CODEX_HOME/skills/fix-aistock-issue` and verifies the repo-local Claude Code command exists. It does not modify production runtime or DB.

## Health Check

Before starting a new issue workflow:

```powershell
python scripts/aistock_issue_workflow.py doctor
```

`doctor` checks the repo, GitHub CLI fallback, MCP/Codex config hints, repo/global skill presence, Claude Code command presence, canonical root cleanliness, active standard/design files, client wrapper hashes, and code-intelligence readiness. It returns `workflow_gate=ready|warning|blocked`.

The `client_manifest` block is machine-readable. If `codex_skill_status` is `stale` or `missing_global`, the current repo CLI remains the source of truth, but older Codex windows may not auto-trigger the latest workflow. After the workflow branch is merged into `main`, run `install-client --apply` and restart old client windows before measuring workflow efficiency.

Code intelligence is non-blocking in KG-1/KG-3. If CodeGraph is installed and `.codegraph/` exists, Context Pack, finish artifacts, and PR Quality artifacts include `code_intelligence` refs such as `codegraph-context.md`, `affected-tests.json`, and `code-intelligence-summary.md`. If CodeGraph or Understand Anything is unavailable, continue with the existing issue workflow fallback and record the warning; do not run full-repo exploration by default. PR Quality publishes these artifacts as warning-only acceleration hints; final validation still comes from AIstock nox / pytest / Validation Center gates.

Warnings about a dirty canonical root are not permission to write there. They mean root sync/cleanup must stop until the unrelated work is resolved. New issue registration and fixes should continue only in a clean task or registry worktree.

## Submit Or Register A New BUG

When the user asks to register a new BUG, do not hand-write a local-only BUG JSON. Use the high-level submit command so the developer client creates the same candidate and BUG record format:

```powershell
python scripts/aistock_issue_workflow.py submit-bug `
  --title "<short title>" `
  --module paper_v2 `
  --severity P1 `
  --description "<observed problem>" `
  --reproduce-command "<command or n/a>" `
  --create-github `
  --create-registry-worktree `
  --apply
```

`--apply` requires GitHub linkage. Either pass `--create-github` so the command uses `gh issue create`, or supply both `--github-issue-number` and `--github-issue-url`. If GitHub is unavailable, keep the output as a draft and do not commit BUG JSON.

`submit-bug --apply` also enforces a registry guard:

- it refuses the canonical root checkout
- it refuses `main`
- it refuses a dirty registry target
- it can create a clean registry worktree with `--create-registry-worktree`
- it writes BUG JSON, allocator, candidate, and workflow state under the selected clean task/registry worktree

BUG id allocation is global, not local to one stale worktree. Before creating a
GitHub Issue or writing BUG JSON, `submit-bug --apply` must scan the selected
registry root, canonical root, known worktrees under `AIstock_worktrees`, local
allocator files, active reservations, and GitHub Issue titles when GitHub
linkage is involved. If any existing BUG JSON or GitHub Issue already uses the
requested `BUG-NNN`, the command must fail before `gh issue create`.

Manual `--bug-id BUG-NNN` is only for audited recovery or migration. It does
not bypass uniqueness checks; when accepted, it bumps the selected registry
allocator so later automatic allocations cannot move backward. Do not guess the
next id by reading `.bug_id_allocator.json` in a stale worktree.

MCP intake paths such as `report_bug` and `mcp_github_issue_create` must follow
the same allocation rule. They may write local BUG JSON only from an approved
task/registry worktree, and their allocator must use the shared
`AIstock_worktrees/.locks/bug-id-allocator.lock` plus the global BUG id scan.

Normal BUG intake continues directly into the fix workflow in the same task/registry worktree via the returned `fix_chain.run_next_command`; do not create a separate registry-only PR unless the user explicitly asks for intake-only tracking.

If a client is launched from `F:\Dev\AIstock`, first create or switch to a clean task/registry worktree. Do not use root `main` for BUG JSON writes.

## Start Or Plan A Single BUG Fix

Preferred high-level command:

```powershell
python scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode plan --create-worktree
```

The command enforces a single active workflow per BUG. If an existing clean state/worktree is found, it returns `workflow_gate=resume` plus a `next_command`; follow that instead of creating a duplicate worktree. If an active worktree is dirty, it returns `workflow_gate=blocked` and a rescue checklist. `--force-new-worktree --reason "<why>"` is only for audited recovery exceptions.

For compatibility with older scripts, the lower high-level start command still works:

```powershell
python scripts/aistock_issue_workflow.py start --bug-id BUG-XXX --create-worktree
```

Then switch to the returned worktree and read:

- `context_pack_md`
- `fix_ready_path`
- `state_path`
- `events_path`
- `code_intelligence.context_ref` when present
- `code_intelligence.affected_tests_ref` when present

The agent must obey the returned `allowed_write_scope`, `required_verification`, `recommended_verification`, and `production_gates`. CodeGraph suggestions are acceleration hints only; final validation still comes from AIstock `test_plans.yaml` / nox / Validation Center gates.

## Resume In A New Window

If a Codex or Claude Code window restarts:

```powershell
python scripts/aistock_issue_workflow.py resume --bug-id BUG-XXX
```

The resume output shows the current state, recent events, workflow root, and exact next command.

## Finish A Fix And Draft PR Evidence

After code changes, ask the wrapper to select validation and draft the PR body:

```powershell
python scripts/aistock_issue_workflow.py finish --bug-id BUG-XXX --plan-only
```

Run every required validation plan. Then re-run finish with evidence:

```powershell
python scripts/aistock_issue_workflow.py finish --bug-id BUG-XXX --validation-evidence "python -m nox -s l0 -> passed"
```

Or use the high-level run mode:

```powershell
python scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode pr --validation-evidence "python -m nox -s l0 -> passed"
```

Use `tmp/issue_workflow/<BUG>/pr-body.md` as the PR body base.

To let the wrapper push and create the PR after validation evidence exists, add explicit automation flags:

```powershell
python scripts\aistock_issue_workflow.py run --bug-id BUG-XXX --mode pr --validation-evidence "python -m nox -s l0 -> passed" --push --create-pr
```

Add `--watch-ci` only when the user asked the agent to watch GitHub checks.

Before any push/PR automation, `run --mode pr` runs a pre-PR gate. It blocks missing validation evidence, failed allowed-scope checks, temp/cache artifacts in git status such as `.codex_tmp` or `.coverage`, and changed Python files that fail Ruff when Ruff is available. Fix the issue inside the same task worktree and rerun the command; do not create a PR first and clean it up later with follow-up style/artifact commits.

Do not stop at `validation_passed`. That state means required local evidence exists, but the work is not PR-ready yet. Commit only task files, then run the PR command from the issue worktree. The wrapper blocks PR automation from canonical root or `main` so accidental root pollution cannot become a PR.

## Close And Sync After Merge

After the PR is approved and merged, prepare the close/sync checklist:

```powershell
python scripts/aistock_issue_workflow.py close-sync --bug-id BUG-XXX --pr-url <PR_URL>
```

By default this is a dry-run plan. When the PR is already merged and validation evidence plus production gates are known, use the safe apply gate:

```powershell
python scripts/aistock_issue_workflow.py close-sync --bug-id BUG-XXX --pr-url <PR_URL> --validation-evidence "python -m nox -s l0 -> passed" --create-registry-worktree --apply
```

`--apply` verifies the PR is merged through `gh`, updates the BUG JSON to `fixed`, posts a GitHub Issue close-sync comment, closes the linked GitHub Issue when needed, writes `close-sync-evidence.json`, and records `state=close_synced`. It refuses to write BUG registry files from the canonical root checkout or from `main`; use `--create-registry-worktree` for normal close-sync so the wrapper creates an isolated `chore/BUG-XXX-close-sync-*` branch. It does not merge PRs and does not touch production services.

If the user explicitly asks the workflow to merge after validation, use:

```powershell
python scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode merge --pr-url <PR_URL> --merge --validation-evidence "python -m nox -s l0 -> passed"
```

Without `--merge`, `run --mode merge` stops at an authorization gate. With `--merge`, the wrapper verifies PR checks are green, merges, runs close-sync through an isolated registry worktree, commits the BUG registry close-sync change, opens or reuses a close-sync PR, and prepares cleanup state. If `gh pr merge` exits non-zero after GitHub has already merged the PR, the wrapper must re-check the PR state, record `recovered_from_local_merge_error`, and continue to close-sync instead of leaving a manual fallback. Merge automation still does not touch production runtime or DB, and it commits only `tests/aistock_validation/bugs/**` from the close-sync worktree; unexpected dirty files block the close-sync PR.


## Cleanup After Merge

After a PR is merged and close-sync is complete, dry-run cleanup first:

```powershell
python scripts/aistock_issue_workflow.py cleanup-after-merge --branch bug/BUG-XXX-scope --worktree F:/Dev/AIstock_worktrees/BUG-XXX-scope --sync-root
```

Only add `--apply` when the plan reports `workflow_gate=ready_for_cleanup`. The apply path refuses dirty worktrees, non-equivalent dirty canonical root, or the currently checked-out branch. If the only root dirty files are byte-equivalent to `origin/main` because a previous close-sync wrote the same registry content locally, cleanup records `origin_equivalent_dirty_files` and safely restores those paths from `origin/main` before fast-forwarding. For squash-merged PRs, pass `--pr-url <PR_URL>` so cleanup can verify the merged PR and tree equivalence before deleting the local branch:

```powershell
python scripts/aistock_issue_workflow.py cleanup-after-merge `
  --branch feature/issue-workflow-phase1 `
  --worktree F:/Dev/AIstock_worktrees/issue-workflow-phase1 `
  --pr-url https://github.com/licong01-cloud/AIstock/pull/195 `
  --sync-root
```

## Timing And Postmortem

After a PR is created, merged, or a workflow feels slow, generate the postmortem artifact instead of manually reconstructing timestamps from GitHub and reflog:

```powershell
python scripts/aistock_issue_workflow.py postmortem --bug-id BUG-XXX
```

The command writes `tmp/issue_workflow/<BUG>/postmortem.json` and `postmortem.md` with phase timing, command-duration telemetry, Context Pack token estimates, duplicate active-worktree count, stale PR check, production gates, and recent events. `known_duration_seconds` comes from commands run by the wrapper; `inferred_elapsed_seconds` includes wall-clock gaps such as human review and CI wait time, so do not treat it as pure code-repair time.

## Triage Current P0

```powershell
python scripts/aistock_issue_workflow.py run-p0 --module paper_v2
# or compatibility listing only:
python scripts/aistock_issue_workflow.py triage-p0
```

Batch only same-module issues with compatible validation and write scope. Keep independent closure evidence for every BUG.

## Batch Same-Module BUGs

When `run-p0` shows compatible issues, start a batch only if the BUGs share module, risk tier, required verification, and GitHub linkage:

```powershell
python scripts/aistock_issue_workflow.py start-batch `
  --bug-id BUG-015 `
  --bug-id BUG-016 `
  --create-worktree
```

The command writes one batch state plus per-issue Context Packs under `tmp/issue_workflow/<BATCH-ID>/`. In KG-4, the batch state and per-issue Context Packs also include a shared `code_intelligence` block so Codex / Claude Code can reuse one CodeGraph context and affected-tests artifact instead of repeating code exploration for every BUG. After the shared fix and required validation:

```powershell
python scripts/aistock_issue_workflow.py finish-batch `
  --batch-id BATCH-paper-v2-YYYYMMDD-xxxxxxxx `
  --validation-evidence "python -m nox -s l0 -> passed" `
  --issue-commit BUG-015=<sha> `
  --issue-commit BUG-016=<sha>
```

Batch PR bodies must preserve per-issue closure maps, shared code-intelligence refs, and `Closes #...` lines for every linked GitHub Issue.

## Stop Conditions

Stop and report instead of editing code when:

- BUG JSON lacks `github_issue_number` or `github_issue_url`.
- BUG status is not `open` or `in_progress`.
- The fix needs files outside `allowed_write_scope`.
- Required validation cannot run.
- Production runtime, production DB, or DDL action would be needed without explicit user approval.
- `doctor` reports `workflow_gate=blocked`.

## Required Final Report

Every completed issue-fix PR report must include:

- branch and PR URL
- commit hash
- changed files
- validation commands and results
- `production_ddl_gate`
- `production_frontend_dependency_gate`
- `production_backend_dependency_gate`
- explicit statement that production runtime and production DB were untouched, or a blocking gate if they were not
