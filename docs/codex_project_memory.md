# AIstock Codex Project Memory

## Purpose

This file is the lightweight project-level memory for Codex and other LLM coding clients working on AIstock.
It must stay stable, short, and operational. Do not use it as a changelog, module history, handoff log, or design archive.

Default rule: load this file only for AIstock architecture, backend, frontend, data pipeline, trading, issue workflow, CI/CD, or production-adjacent work. For ordinary issue work, prefer the issue Context Pack and relevant files over full historical documents.

## Repository And Runtime Map

- Canonical repository root: `F:\Dev\AIstock`.
- Normal development must use isolated worktrees under `F:\Dev\AIstock_worktrees\<task-name>`.
- The root checkout `F:\Dev\AIstock` is the sync/runtime baseline, not the default development workspace.
- Main backend: FastAPI under `backend/`, typical production port `8001`.
- Main frontend: Next.js under `frontend/`, typical production port `3000`.
- Market data bridge: TDX Go service under `tdx-api-main/`, typical port `19080`.
- Important domains: `backend/routers`, `backend/services`, `backend/data_service`, `backend/infra`, `frontend/src/app`, `tests/aistock_validation`.

## Active Standards

Use the active standard only when it is relevant to the current task:

- `docs/standards/aistock_development_standard_v1.5_20260523.md`
- `docs/standards/aistock_development_standard_v1.5_20260523.yaml`
- `docs/standards/aistock_issue_fix_parallel_workflow_standard_20260514.md`
- `docs/architecture/aistock_issue_workflow_opensource_cicd_design_v2_20260525.md`

Do not read archived standards, old design notes, historical implementation logs, or module-specific restart plans unless the current task explicitly touches that module or the user asks for historical context.

## Worktree And Branch Rules

- Every non-trivial feature, bugfix, or documentation change uses a new task branch and an isolated worktree from latest `origin/main`.
- Do not develop directly in `F:\Dev\AIstock` when it is on `main` or dirty.
- Do not reuse another active window's physical worktree.
- Before editing, check `git status --short --branch`, current branch, and recent commits.
- Stage and commit only files belonging to the current task.
- Never run destructive Git commands such as `git reset --hard`, `git checkout -- .`, or `git clean -fd` unless the user explicitly approves that exact action.
- If unexpected unrelated changes appear in a touched file or workspace, stop and report before continuing.

## Context Budget Rules

- Classify work before loading context: T0 quick fix, T1 standard issue, T2 same-module batch, T3 design or architecture work.
- T0/T1 work must not load full memory, full standards, full designs, archived docs, or unrelated module plans by default.
- Prefer compact context packs, issue JSON, selected standard sections, ownership catalogs, and relevant code snippets.
- Module-specific design documents are opt-in: read them only when the current task is inside that module or the user explicitly asks.
- Historical notes in this repository may be stale after implementation changes; verify against current code and tests before relying on them.

## Issue Workflow Rules

- Use `scripts/aistock_issue_workflow.py` as the high-level entrypoint for submitting, fixing, triaging, batching, finishing, closing, syncing, or resuming AIstock BUG/GitHub Issues.
- `scripts/issue_flow.py` is a lower-level helper, not the default entrypoint.
- New BUG records must stay synchronized with GitHub Issues; a BUG JSON without `github_issue_number` and `github_issue_url` is only a triage draft and must not be merged into `main`.
- Fix work must respect `allowed_write_scope`. If the fix requires files outside scope, stop and update scope before editing further.
- Same-module issues may use one batch worktree only when module, risk, write scope, and validation chain are compatible. Each issue still needs independent evidence and closure mapping.
- Do not close issues until validation evidence and production gates are recorded.

## Validation And CI/CD Rules

- Use targeted tests first, then the selected module or gate validation required by the issue/design.
- Do not run broad expensive validation repeatedly after every small change; rerun final gates once the patch is stable.
- Before PR, run a preflight appropriate to the change: changed-file lint/static checks, targeted tests, scope check, `git diff --check`, PR body/evidence check, and production gates.
- GitHub Actions, nox sessions, PR Quality, Semgrep, CodeQL, and validation catalogs are the authoritative CI/CD foundation.
- CI failures must be summarized by failed job, failed test, error signature, reproduce command, suspected module/files, and fingerprint before creating or promoting a BUG.

## Production Safety Gates

- Do not restart production backend `8001`, frontend `3000`, TDX `19080`, or other production services unless the user explicitly asks.
- Do not write production DB data or apply DDL without explicit approval.
- Runtime activation and code merge are separate steps.
- Every completion report must state:
  - `production_ddl_gate`: `noop`, `applied_and_verified`, or `pending`.
  - `production_frontend_dependency_gate`: `noop`, `applied_and_verified`, or `pending`.
  - `production_backend_dependency_gate`: `noop`, `applied_and_verified`, or `pending`.
- If a merged change needs new DB objects, apply and verify the committed production migration before claiming production readiness; otherwise report `production_ddl_pending`.

## UI Rules

- New AIstock operator-facing UI defaults to shadcn/ui Blocks visual language, shadcn-compatible tokens, clear component boundaries, and human-friendly layout density.
- Research Assistant may use `assistant-ui` for conversation primitives but should keep shell, cards, forms, drawers, tables, buttons, and status views shadcn-compatible.
- Existing Paper v2 `paper-v2.css`, `pv2-*`, and `frontend/src/components/paper-v2/*` are legacy implementation details and must not spread to new modules.

## Documentation Rules

- Durable project standards live under `docs/standards`.
- Architecture and design docs live under `docs/architecture` or `docs/analysis` as appropriate, but they must not become competing global standards.
- Do not append detailed module changelogs, historical validation narratives, old handoffs, or temporary troubleshooting notes to this file.
- Store task evidence in validation history, PR bodies, issue workflow state, or module-specific docs instead of this project memory.

## Completion Report Rules

For code or workflow changes, report:

- Worktree and branch.
- Commit hash and PR URL when available.
- Files changed.
- Validation commands and results.
- Issue/BUG sync state.
- Production gates.
- Whether production runtime or DB was touched.
- Any remaining blockers before merge or production activation.
