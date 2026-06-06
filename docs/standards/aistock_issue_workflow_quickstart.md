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

Code intelligence is non-blocking in KG-1/KG-3. If CodeGraph is installed and `.codegraph/` exists, Context Pack, finish artifacts, and PR Quality artifacts include `code_intelligence` refs such as `codegraph-context.md`, `affected-tests.json`, and `code-intelligence-summary.md`. If CodeGraph or Understand Anything is unavailable, continue with the existing issue workflow fallback and record the warning; do not run full-repo exploration by default. `doctor` and `postmortem` expose `h7_code_intelligence.readiness_next_command`, usually `codegraph init -i` when the index is missing. Run that command only when the current workflow needs graph context; missing graph data must not block ordinary T0/T1 fixes. `python scripts/code_intelligence_adapter.py latest-freshness` is the read-only way for Codex / Claude Code to consume the newest local Nightly CodeGraph freshness artifact before deciding whether live graph probing is useful. Nightly uploads a `code-intelligence-<run_id>` GitHub artifact containing `code-intelligence-run-manifest.json`; agents can download it with the manifest's `gh run download ... -D tmp/validation/code-intelligence/downloaded/<run_id>` command, then rerun `latest-freshness` without invoking CodeGraph or scanning the repository. PR Quality publishes these artifacts as warning-only acceleration hints; final validation still comes from AIstock nox / pytest / Validation Center gates.

Warnings about a dirty canonical root are not permission to write there. They mean root sync/cleanup must stop until the unrelated work is resolved. New issue registration and fixes should continue only in a clean task or registry worktree.

`--output` is a JSON file path, not an output format selector. Omit it for stdout or use `--output -`; do not pass `--output json`. File outputs should use an explicit path, preferably under `tmp/issue_workflow/` or `tmp/validation/`, so a client typo cannot create root-level files such as `json`.

Stdout defaults to compact success output. A passing workflow command should show only the gate, issue id, branch/worktree or PR pointers, validation/CI counts, production gates, and the next action. Do not paste full JSON payloads, full `statusCheckRollup`, `recent_events`, skipped validation maps, or nox internals into chat when the command passed. Use `--output-format full-json` for local debugging, or `--output tmp/issue_workflow/<BUG-ID>/<name>.json` to persist full details as an ignored artifact. Failure output may include the smallest diagnostic signature needed to reproduce or unblock. Registry intake commands must stage only committable BUG registry files, not ignored `tmp/issue_workflow` artifacts.

## Optimized Tool Use Contract

For Codex, Claude Code, Cursor, and other agents, context discovery should be cheap by default:

- Read the workflow output, Context Pack, and `fix-ready.json` before searching code.
- Use `allowed_write_scope` and `changed_files` as the initial search boundary.
- Prefer `rg <pattern> <scoped-file-or-dir>` over broad repository scans; broaden only when the scoped search fails or the Context Pack is stale.
- Do not load archived standards, old design notes, full logs, or module restart plans unless the current BUG explicitly needs that history.
- Treat CodeGraph / Understand Anything references as acceleration hints; they do not replace selected nox / pytest / Validation Center evidence.

Close-sync-only PRs that change only BUG JSON/status evidence are metadata aftercare. AIstock CI uses `scripts/ci_change_classifier.py` to keep the static gate and PR Quality evidence while skipping unrelated backend matrix jobs when every changed BUG JSON is already `fixed`, `closed`, or `verified`. Any allocator change, open BUG registry intake, non-JSON registry file, or non-registry code/doc change keeps the full backend matrix.

GitHub PR Quality now treats the P0/P1 evidence gate as blocking by default, so source fix PRs must provide linked issue context, scope, validation evidence, and production gates before merge. Local `python scripts/issue_flow.py pr-check` still stays warning-only unless `--enforce-p0-p1-evidence` or `AISTOCK_PR_QUALITY_ENFORCE_P0P1=true` is set. Code-intelligence and Semgrep artifacts remain report-only acceleration hints rather than merge blockers.
## Fast Path And Smoke Check

Use `fast-path` when a client needs a cheap, machine-readable plan before loading more context:

```powershell
python scripts/aistock_issue_workflow.py fast-path --bug-id BUG-XXX --changed-file <path>
# or, before a BUG exists:
python scripts/aistock_issue_workflow.py fast-path --module validation.guardrails --changed-file scripts/aistock_issue_workflow.py
```

The output classifies the task as `T0`, `T1`, `T2`, or `T3`, returns selected validation commands, production gates, context strategy, stop conditions, and the next workflow command. Treat it as an optimization layer only: it does not replace GitHub/BUG linkage, validation evidence, PR quality, close-sync, or cleanup.

Fast-path intent:

- `T0`: docs/client/registry metadata changes; use targeted context and changed-file/l0 validation only as selected.
- `T1`: single BUG or single workflow/module code change; use Context Pack plus targeted snippets, not old module histories.
- `T2`: critical or multi-impact/product scope; batch only compatible same-module work and share validation evidence carefully.
- `T3`: design or architecture work; keep an acceptance matrix and broader review.

Use `workflow-smoke` after workflow CLI/client changes, or before judging whether Codex/Claude can still follow the issue flow:

```powershell
python scripts/aistock_issue_workflow.py workflow-smoke --changed-file scripts/aistock_issue_workflow.py --module validation.guardrails
```

`workflow-smoke` dry-runs the core chain with a synthetic ignored issue record: fast-path -> start dry-run -> finish plan-only -> postmortem preview. It must not create GitHub Issues, PRs, production DB writes, runtime restarts, or tracked root files. A passing smoke check reports `workflow_gate=passed` and `unexpected_dirty_paths=[]`.

Use `nightly-intake-smoke` before changing CI/Nightly failure intake or when validating that auto-filed issues can enter the standard workflow without root pollution:

```powershell
python scripts/aistock_issue_workflow.py nightly-intake-smoke
```

`nightly-intake-smoke` builds a synthetic Nightly failure status, writes summary/context/GitHub-issue payload/candidate-history artifacts only under ignored `tmp/validation/nightly_failure_issue/smoke/`, verifies the Agent Handoff contains `triage-ci-issue` and `promote-ci-issue --create-registry-worktree --apply`, and checks `unexpected_dirty_paths=[]`. It does not create GitHub Issues, BUG JSON, PRs, runtime calls, DB writes, or tracked source files.

Use `batch-workflow-smoke` before changing same-module batch handling or when validating that batch issue workflow still produces per-issue context and closure evidence without root pollution:

```powershell
python scripts/aistock_issue_workflow.py batch-workflow-smoke
```

`batch-workflow-smoke` uses two synthetic ignored BUG records, runs batch start plus finish in-process, verifies batch state, per-issue Context Packs, fix-ready JSON, PR body closing keywords, per-issue closure map, validation evidence, and `unexpected_dirty_paths=[]`. It does not create GitHub Issues, BUG JSON, PRs, runtime calls, DB writes, or tracked source files.

## CI / Nightly Failure Intake

Auto-filed CI or Nightly P0/P1 GitHub Issues must contain actionable diagnostics plus an agent handoff. If the failure summary is not actionable yet, the summary tool must skip GitHub issue payload creation and leave only ignored artifacts for later review. A valid actionable issue body includes:

- the Actions run, branch, commit, fingerprint, failed job/session/test, and reproduce command
- an `Agent Handoff` block with `triage-ci-issue`; include `promote-ci-issue` and post-promotion `run --bug-id` only when diagnostics identify a concrete code or test regression
- token policy stating that the issue and Context Pack are the first context source, while full logs and historical design docs are loaded only when triage requires them
- production gates, defaulting to `noop` unless the failure proves otherwise

For an auto-filed issue, start with:

```powershell
python scripts/aistock_issue_workflow.py triage-ci-issue --issue <issue-number>
```

The command writes only ignored workflow artifacts under `tmp/issue_workflow/ci-issue-<issue-number>/`:

- `triage-ci-issue.json`
- `failure-event.json`
- `context-pack.json`
- `context-pack.md`

If the triage result is a real regression candidate and no BUG JSON is linked yet, promote it through a clean registry worktree:

```powershell
python scripts/aistock_issue_workflow.py promote-ci-issue --issue <issue-number> --create-registry-worktree --apply
```

After promotion, continue through the normal BUG workflow returned by `next_command`. CI/Nightly intake must not write BUG JSON directly from GitHub Actions or from the canonical root `main` checkout.
Nightly jobs themselves must only write compact issue context, candidate history, and evidence under ignored `tmp/validation/...` artifact paths plus GitHub Issue comments/updates. If `github-issue-payload.json` is absent because `issue_creation_policy.allowed=false`, the workflow must log the policy reason and skip GitHub issue writes instead of failing. They must not commit BUG JSON, mutate source files, or write tracked root files.

Partial diagnostics without failed tests, error signatures, or suspected files are triage-only. They may be kept as artifacts, but should not present `promote-ci-issue` as a next command and should not consume a repair window until triage identifies a concrete code/test failure. Manual dispatches with only a short summary may create a GitHub Issue for human tracking, but their handoff remains `needs_bug_json=false` and `triage-ci-issue` only until reclassified.

Code intelligence is a warning-only accelerator. A Nightly failure where only `code_intelligence` failed must not create an actionable GitHub Issue or BUG repair flow. Keep the freshness/UA artifacts for later inspection and use `latest-freshness` as a read-only hint; create or promote a BUG only when another actionable Nightly stage also failed or manual triage finds a real code/test regression.

If `triage-ci-issue` returns `classification_recommendation=infra_blocker` or
`infra_flaky`, do not promote it into a code BUG. Follow the returned
`infra_action` instead. Typical examples are missing self-hosted Windows
runners, runner API permission failures, or missing
`AISTOCK_RUNNER_HEALTH_TOKEN`. These issues should restore infrastructure and
rerun CI/Nightly, not consume a developer window as a code repair.
Auto-filed infra-only CI/Nightly issues should therefore expose only the
`triage-ci-issue` entrypoint, `needs_bug_json=false`, and an infra action card.
They must not present `promote-ci-issue` or `run --bug-id` as the next command
unless triage later reclassifies the failure as a real code or test regression.
Use `ci-issue-janitor --issue <issue-number>` to dry-run closure for infra-only
or superseded auto-filed issues; add `--apply` only after the compact output
shows `action=close_infra` or `action=close_superseded`. The janitor must not
create BUG JSON for infra-only issues.

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

For UI/display BUGs, `submit-bug` enriches the record with `ui_intake_hints`: inferred route, focused component/API scope, reproduce requirement, visual acceptance requirement, and recommended frontend/backend validation. Agents must start from those hints before broad frontend exploration, and should request full JSON only when the compact output is insufficient or a failure needs diagnosis.

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

Normal BUG intake continues directly into the fix workflow in the same
task/registry worktree via the returned `fix_chain.run_next_command` and
`fix_chain.next_command`; do not create a separate registry-only PR unless the
user explicitly asks for intake-only tracking. A registry-only PR is an
intake-only exception, not the default fix path.

If a client is launched from `F:\Dev\AIstock`, first create or switch to a clean task/registry worktree. Do not use root `main` for BUG JSON writes.

## Start Or Plan A Single BUG Fix

Preferred high-level command:

```powershell
python scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode plan --create-worktree
```

The command enforces a single active workflow per BUG. If an existing clean state/worktree is found, it returns `workflow_gate=resume` plus a `next_command`; follow that instead of creating a duplicate worktree. If an active worktree is dirty, it returns `workflow_gate=blocked` and a rescue checklist. `--force-new-worktree --reason "<why>"` is only for audited recovery exceptions.

If `run --mode plan` is executed without `--create-worktree`, it writes planning artifacts in the current workflow root only. The state records `planned_worktree` instead of `worktree`, and `resume` will direct the agent to rerun plan with `--create-worktree` before editing code.

For compatibility with older scripts, the lower high-level start command still works:

```powershell
python scripts/aistock_issue_workflow.py start --bug-id BUG-XXX --create-worktree
```

Then switch to the returned worktree and read:

- `task_card_md` first, because it is the compact agent-neutral handoff for Codex, Claude Code, Cursor, and CLI clients
- `task_card_json` when a machine-readable handoff is needed
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

Before any push/PR automation, `run --mode pr` runs a pre-PR gate. It blocks missing validation evidence, failed allowed-scope checks, uncommitted task files, temp/cache artifacts in git status such as `.codex_tmp` or `.coverage`, and changed Python files that fail Ruff when Ruff is available. Fix or commit the issue inside the same task worktree and rerun the command; do not create a PR first and clean it up later with follow-up style/artifact commits.

When `--watch-ci` is used, the wrapper polls a compact check summary through `gh pr view --json statusCheckRollup`. Missing or not-yet-started checks are `checks_pending` with retry instructions, not a business failure. Full check JSON should be requested only when a failed check needs diagnosis.

If the first watch exits while checks are still pending, refresh the workflow state with the compact command instead of manually editing `state.json` or requesting the full check rollup:

```powershell
python scripts/aistock_issue_workflow.py watch-ci --bug-id BUG-XXX --pr-url <PR_URL>
```

When checks pass, `watch-ci` updates the BUG workflow state to `ci_green` and returns `merge_only_if_user_authorized` as the next action.

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

When the user explicitly authorizes merging a PR or branch into `main`, the workflow must complete the full aftercare loop unless the user says otherwise: merge the source PR, persist and merge close-sync when required, fast-forward the canonical root `F:\Dev\AIstock` to `origin/main`, verify local `main` equals GitHub `main`, and clean only the task-scoped branch/worktree that passed the safety checks. Do not report a merge as complete while the canonical root is behind `origin/main`, a required close-sync PR is still open, or a task branch/worktree cleanup step is still blocked.

If the merged change declares `production_ddl_gate=pending` or otherwise includes a committed production DDL/migration requirement, the same explicit merge authorization requires applying that exact committed DDL after `main` is merged and locally synced, then verifying the schema/API evidence before reporting restart readiness. Do not invent ad hoc DDL outside the committed migration or design. If the DDL cannot be applied or verified safely, stop with `production_ddl_gate=pending` and state that the feature is not restart-ready.

If the source/fix PR has already been merged, use the v2.3 finalizer instead of manually chaining close-sync, cleanup, and postmortem commands:

```powershell
python scripts/aistock_issue_workflow.py merge-finalizer `
  --bug-id BUG-XXX `
  --source-pr-url <PR_URL> `
  --source-branch bug/BUG-XXX-scope `
  --source-worktree F:/Dev/AIstock_worktrees/BUG-XXX-scope `
  --validation-evidence "python -m nox -s l0 -> passed" `
  --sync-root `
  --apply
```

The finalizer verifies the source PR is merged, runs close-sync in an isolated registry worktree, commits and opens or reuses the close-sync PR, records postmortem output, and returns the remaining next actions. Retries after GitHub/TLS/CI interruptions must reuse an existing open close-sync PR for the same BUG/source PR instead of appending duplicate registry commits. Add `--merge-close-sync-pr --cleanup` only when the user authorized the full aftercare loop and checks are green; otherwise stop with a merge-ready close-sync PR.


## Cleanup After Merge

After a PR is merged and close-sync is complete, dry-run cleanup first:

```powershell
python scripts/aistock_issue_workflow.py cleanup-after-merge --branch bug/BUG-XXX-scope --worktree F:/Dev/AIstock_worktrees/BUG-XXX-scope --sync-root
```

Only add `--apply` when the plan reports `workflow_gate=ready_for_cleanup`. The apply path refuses dirty worktrees, non-equivalent dirty canonical root, or the currently checked-out branch. If the only root dirty files are byte-equivalent to `origin/main` because a previous close-sync wrote the same registry content locally, cleanup records `origin_equivalent_dirty_files` and safely restores those paths from `origin/main` before fast-forwarding. For squash-merged PRs, pass `--pr-url <PR_URL>` so cleanup can verify the merged PR before deleting the local branch. The verification compares the source head changed paths to the source PR merge commit first, then falls back to current `origin/main`, so later close-sync BUG JSON drift does not force a manual cleanup:

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

The command writes `tmp/issue_workflow/<BUG>/postmortem.json` and `postmortem.md` with phase timing, command-duration telemetry, Context Pack token estimates, duplicate active-worktree count, stale PR check, production gates, and recent events. It also includes `phase_cost_table`, `h6_summary`, and `h7_code_intelligence` so agents can report the top time/token cost and CodeGraph readiness without pasting full JSON. `known_duration_seconds` comes from commands run by the wrapper; `inferred_elapsed_seconds` includes wall-clock gaps such as human review and CI wait time, so do not treat it as pure code-repair time. Prefer the derived fields `queue_seconds`, `active_fix_seconds`, `local_validation_seconds`, `pr_ci_seconds`, and `merge_aftercare_seconds` when explaining slow cases. `code_repair_seconds` is explicit or active-fix derived; do not inflate it with queue, review, or CI wait.

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

The command writes one batch state plus per-issue Context Packs under `tmp/issue_workflow/<BATCH-ID>/`. The batch payload includes `batch_selector`, which records the shared allowed scope, selected required validation plans, production/dependency gates, and per-issue coverage. In KG-4, the batch state and per-issue Context Packs also include a shared `code_intelligence` block so Codex / Claude Code can reuse one CodeGraph context and affected-tests artifact instead of repeating code exploration for every BUG. After the shared fix and required validation:

```powershell
python scripts/aistock_issue_workflow.py finish-batch `
  --batch-id BATCH-paper-v2-YYYYMMDD-xxxxxxxx `
  --validation-evidence "python -m nox -s l0 -> passed" `
  --issue-commit BUG-015=<sha> `
  --issue-commit BUG-016=<sha>
```

`finish-batch` re-checks the actual changed files against the selector scope and returns `scope_check`. If any changed file exceeds the shared scope, or if the selector finds production/dependency gates that cannot be safely shared, the workflow returns `workflow_gate=blocked`; split the batch or update the issue scopes before continuing. Batch PR bodies must preserve per-issue closure maps, shared code-intelligence refs, and `Closes #...` lines for every linked GitHub Issue.

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
- local/GitHub sync proof: canonical `F:\Dev\AIstock` `main` equals `origin/main` after merge
- DDL status: `noop`, `applied_and_verified`, or explicit `pending` blocker; if merge approval included required DDL, include the applied migration and verification evidence
- explicit statement that production runtime and production DB were untouched, or a blocking gate if they were not
