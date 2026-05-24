# AIstock Issue Workflow Quickstart

## Purpose

This quickstart is for Codex, Claude Code, Cursor, and any other CLI/IDE coding agent. When the user asks to submit, fix, triage, batch, finish, close, sync, or merge an AIstock BUG/GitHub Issue, do not improvise manual steps. Start with the repo-level orchestrator:

```powershell
python scripts/aistock_issue_workflow.py doctor
```

The skill/command/prompt layer is intentionally thin. The source of truth is `scripts/aistock_issue_workflow.py`; `scripts/issue_flow.py` remains the lower-level primitive helper.

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

`doctor` checks the repo, GitHub CLI fallback, MCP/Codex config hints, repo/global skill presence, Claude Code command presence, canonical root cleanliness, and active standard/design files. It returns `workflow_gate=ready|warning|blocked`.

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
  --apply
```

`--apply` requires GitHub linkage. Either pass `--create-github` so the command uses `gh issue create`, or supply both `--github-issue-number` and `--github-issue-url`. If GitHub is unavailable, keep the output as a draft and do not commit BUG JSON.

## Start Or Plan A Single BUG Fix

Preferred high-level command:

```powershell
python scripts/aistock_issue_workflow.py run --bug-id BUG-XXX --mode plan --create-worktree
```

For compatibility with older scripts, the lower high-level start command still works:

```powershell
python scripts/aistock_issue_workflow.py start --bug-id BUG-XXX --create-worktree
```

Then switch to the returned worktree and read:

- `context_pack_md`
- `fix_ready_path`
- `state_path`
- `events_path`

The agent must obey the returned `allowed_write_scope`, `required_verification`, `recommended_verification`, and `production_gates`.

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

## Close And Sync After Merge

After the PR is approved and merged, prepare the close/sync checklist:

```powershell
python scripts/aistock_issue_workflow.py close-sync --bug-id BUG-XXX --pr-url <PR_URL>
```

By default this is a dry-run plan. When the PR is already merged and validation evidence plus production gates are known, use the safe apply gate:

```powershell
python scripts/aistock_issue_workflow.py close-sync --bug-id BUG-XXX --pr-url <PR_URL> --validation-evidence "python -m nox -s l0 -> passed" --apply
```

`--apply` verifies the PR is merged through `gh`, updates the BUG JSON to `fixed`, writes `close-sync-evidence.json`, and records `state=close_synced`. It does not merge PRs and does not touch production services.


## Cleanup After Merge

After a PR is merged and close-sync is complete, dry-run cleanup first:

```powershell
python scripts/aistock_issue_workflow.py cleanup-after-merge --branch bug/BUG-XXX-scope --worktree F:/Dev/AIstock_worktrees/BUG-XXX-scope --sync-root
```

Only add `--apply` when the plan reports `workflow_gate=ready_for_cleanup`. The apply path refuses dirty worktrees, unmerged branches, dirty canonical root, or the currently checked-out branch.

## Triage Current P0

```powershell
python scripts/aistock_issue_workflow.py run-p0 --module paper_v2
# or compatibility listing only:
python scripts/aistock_issue_workflow.py triage-p0
```

Batch only same-module issues with compatible validation and write scope. Keep independent closure evidence for every BUG.

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
