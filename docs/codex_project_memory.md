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

Use the active standard only when it is relevant to the current task. Start from `docs/standards/README.md` when there is any doubt about the current standard set.

- `docs/standards/README.md`
- `docs/standards/aistock_development_standard_v1.5_20260523.md`
- `docs/standards/aistock_development_standard_v1.5_20260523.yaml`
- `docs/standards/aistock_issue_fix_parallel_workflow_standard_20260514.md`
- `docs/architecture/aistock_issue_workflow_opensource_cicd_design_v2_20260525.md`

The current issue Context Pack, explicit user request, and relevant code paths are the default starting point. Open module design documents only when the task scope, issue evidence, or user request points to that module.

## Worktree And Branch Rules

- Every non-trivial feature, bugfix, or documentation change uses a new task branch and an isolated worktree from latest `origin/main`.
- Do not develop directly in `F:\Dev\AIstock` when it is on `main` or dirty.
- Do not reuse another active window's physical worktree.
- Do not create or keep a non-root worktree on local `main`; task worktrees must use task branches. If a stale worktree holds `main`, audit it outside the repo, remove it safely, and restore `F:\Dev\AIstock` to `main...origin/main`.
- Before editing, check `git status --short --branch`, current branch, and recent commits.
- Stage and commit only files belonging to the current task.
- Never run destructive Git commands such as `git reset --hard`, `git checkout -- .`, or `git clean -fd` unless the user explicitly approves that exact action.
- If unexpected unrelated changes appear in a touched file or workspace, stop and report before continuing.

## Context Budget Rules

- Read the project-level rules once, then route into exactly one task-specific skill or Claude command for execution.
- The selected skill is the authority for that scenario; do not also load other scenario skills, quickstarts, full standards, or module designs unless the skill, issue evidence, or user explicitly requires it.
- T0/T1 BUG, docs, cleanup, merge, and read-only tasks use compact context packs, task cards, ownership catalogs, CodeGraph/UA refs, and narrow code snippets instead of full standards.
- After context compaction or client restart, use `resume` plus `task-card.md` Context Resume Digest hashes; do not re-read skills, project memory, standards README, quickstart, or RTK unless a digest changed, state is missing, or the user explicitly asks.
- Keep code exploration bounded: use precise `rg`, avoid reprinting the same source range, and pause to summarize before broad scans when exploratory commands exceed the soft budget.


## Feature Workflow Rules

- New non-trivial feature delivery uses `FEATURE-WORKFLOW-001` in the active development standard; BUG fixes, workflow policy changes, docs cleanup, audits, and generic analysis continue to use the issue/docs lane.
- For LocalSIM or MiniQMT simulation-platform design, feature, BUG, migration, diagnostics, test, or runbook work, `docs/architecture/simulation_platform_unified_authoritative_blueprint_20260715.md` is the sole umbrella authority. Every related code PR must map to its stable acceptance ids and update its current progress ledger with code, test, merge, production-gate, and runtime evidence in the same PR; specialist documents remain subordinate contracts.
- Classify feature work before implementation: `F0` lightweight Feature Card, `F1` standard single-module design, `F2` cross-module or production-critical architecture design.
- F0/F1/F2 feature work must keep a stable `Design Acceptance Index` and a pre-merge design acceptance matrix.
- Before PR or merge, run `python scripts/aistock_feature_workflow.py validate --design <path> --tier F0|F1|F2` for the feature artifact.
- Do not report completion or request merge when the matrix has an unapproved gap, simplified/POC/mock-only/static delivery, or silent fallback.
- Codex uses `.codex/skills/verify-aistock-feature/SKILL.md`; Claude Code uses `.claude/commands/aistock-feature-workflow.md` for the same feature workflow entrypoint. Do not route non-feature maintenance work through this lane just because the word "feature" appears in the request.

## Issue Workflow Rules

- Use `scripts/aistock_issue_workflow.py` as the high-level entrypoint for submitting, fixing, triaging, batching, finishing, closing, syncing, or resuming AIstock BUG/GitHub Issues.
- Developer-client workflow entrypoints are synced by `python scripts/aistock_issue_workflow.py install-client --apply`; after `.codex/**` or `.claude/**` changes, run it after merge before old client windows rely on the new wrappers.
- `scripts/issue_flow.py` is a lower-level helper, not the default entrypoint.
- New BUG records must stay synchronized with GitHub Issues; a BUG JSON without `github_issue_number` and `github_issue_url` is only a triage draft and must not be merged into `main`.
- Fix work must respect `allowed_write_scope`. If the fix requires files outside scope, stop and update scope before editing further.
- Same-module issues may use one batch worktree only when module, risk, write scope, and validation chain are compatible. Each issue still needs independent evidence and closure mapping.
- Do not close issues until validation evidence and production gates are recorded.

## Validation And CI/CD Rules

- Codex keeps the smallest safe local gate: changed-file lint/compile, direct fix-point targeted test or contract smoke, `git diff --check`, scope check, and production gates.
- After a local test failure, rerun the failed nodeid or `pytest --lf` first; do not repeat broad suites for the same failure. Run a related final small matrix at most once after behavior stabilizes.
- If local exploration or validation exceeds about 30 minutes, command count exceeds the task-card soft limit, or coverage must become broad/cross-module/UI/API/business-flow, delegate validation to VC/CI/nightly instead of expanding the interactive window.
- Broad module matrices, UI journeys, API/business-flow E2E, LLM design drift, and cross-module regression are nightly-deferred by default and run once across the day's merged BUG/PR changes.
- For complex validation, Codex should request a pipeline/Validation Center run; DeepSeek may select plans and diagnose failures, while allowlisted deterministic runners execute the tests and return a compact receipt.
- Successful workflow/validation commands should write compact stdout by default; JSON artifacts are diagnostic-only unless a command must persist state/evidence or `AISTOCK_WORKFLOW_ARTIFACTS=1` is set.
- Immediate deep validation remains only for DDL, production writes, order/cash/position invariants, fail-closed safety, or explicit user request.

## Production Safety Gates

- Do not restart production backend `8001`, frontend `3000`, TDX `19080`, or other production services unless the user explicitly asks.
- Do not write production DB data or apply DDL without explicit approval.
- Local validation ports are owned by `noxfile.py`, environment variables, and the active standard; use the workflow-provided defaults instead of hardcoding ad hoc ports here.
- Runtime activation and code merge are separate steps.
- When the user explicitly authorizes merging a PR or branch into `main`, complete the aftercare loop before reporting done: merge/persist close-sync if required, fast-forward `F:\Dev\AIstock` so local `main` equals `origin/main`, clean only safe task branches/worktrees, and apply plus verify any committed production DDL required by the merged change.
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
- `docs/handoff/` is for formal tracked handoff evidence only. Temporary Codex/Claude exchange notes must use ignored scratch paths such as `tmp/handoff/`, `docs/handoff/_scratch/`, or `docs/handoff/local/`.
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
