# AIstock Codex Project Memory

## Purpose

This is stable, lightweight operational context for Codex and other coding clients. It is not a changelog, module-status ledger, handoff, design archive, or second policy source.

Load it once for AIstock architecture, backend, frontend, data pipeline, trading, runtime, workflow, CI/CD, or production-adjacent decisions. Ordinary BUG work starts from its task card, Context Pack, ownership catalog, and direct code references.

## Authority and routing

- The sole human-readable development authority is `docs/standards/aistock_development_standard_v1.5_20260523.md`.
- `docs/standards/aistock_development_standard_v1.5_20260523.yaml` is machine-derived and has no independent policy authority.
- `docs/standards/README.md` routes to one Codex skill or Claude command. Quickstarts and lane files provide procedures only.
- Active module phase, experiment, runtime, BUG, PR, and rollout state lives in the current task card, BUG/feature record, authoritative module design, validation receipt, and live GitHub/runtime evidence. Revalidate it there; never add snapshots here.

## Repository and runtime map

- Canonical sync/runtime root: `F:\Dev\AIstock`.
- Isolated implementation worktrees: `F:\Dev\AIstock_worktrees\<task-name>`.
- FastAPI backend: `backend/`, typical production port `8001`.
- Next.js frontend: `frontend/`, typical production port `3000`.
- TDX Go bridge: `tdx-api-main/`, typical port `19080`.
- Main boundaries: `backend/routers`, `backend/services`, `backend/data_service`, `backend/infra`, `frontend/src/app`, and `tests/aistock_validation`.

## Worktree and scope rules

- Non-trivial feature, BUG, controlled-doc, and workflow changes use a fresh task branch and isolated worktree from latest `origin/main`.
- The canonical root is not the default implementation workspace. Never reuse another active window's physical worktree.
- Before editing, verify branch, HEAD, upstream, and `git status --short --branch`.
- Respect `allowed_write_scope`; update the registered scope before touching additional files.
- Preserve unrelated dirty files and concurrent work. Never use destructive reset/clean or broad deletion without exact user authorization.

## Context and tool budget

- Read project rules once, then load exactly one selected lane plus its direct artifacts.
- T0/T1 work uses compact task cards, Context Packs, ownership/graph summaries, and targeted snippets rather than full standards, quickstarts, history, or unrelated designs.
- After compaction/restart, use `resume` and the Context Resume Digest. Re-read a rule source only when its hash changed, state is missing, or the user asks.
- Follow `TOOL-RTK-001`: eligible supported high-output interactive commands must use RTK. Direct fallback is limited to unsupported/unavailable calls, exact-raw-output diagnostics, or a first wrapper failure; record one concise reason. Never self-authorize `rtk trust`, and never make RTK or telemetry a gate.
- Use `doctor` only for unknown client/bootstrap readiness, workflow/client changes, stale/conflicting resume state, or an explicit diagnostic request. Ordinary work goes directly to `run`, `resume`, `submit-bug`, or its selected lane.

## Feature and issue workflows

- New non-trivial feature delivery uses `FEATURE-WORKFLOW-001`, an F0/F1/F2 artifact, stable Design Acceptance Index, and pre-merge acceptance matrix.
- BUG, workflow-policy, docs-cleanup, audit, and generic maintenance work do not enter the feature lane merely because “feature” appears in the request.
- BUG/GitHub Issue work uses `scripts/aistock_issue_workflow.py`; `scripts/issue_flow.py` is a lower-level helper.
- New BUG JSON and GitHub Issue linkage remain synchronized. A required scope expansion is registered before implementation continues.
- Same-module issues may batch only when module, risk, scope, validation chain, and production gates are compatible; otherwise record a split reason.
- Runtime BUGs retain their lazy runtime contract and remain `fixed_source_pending_user_restart` until the user's restart is followed by complete read-only identity/business-smoke evidence.

## Validation and CI/CD

- Keep the smallest safe local gate: changed-file lint/compile, direct fix-point test or contract smoke, `git diff --check`, scope/ownership check, and production-gate states.
- After failure, rerun the failed nodeid or `pytest --lf`; after stabilization, run one related small matrix.
- Delegate broad module, UI/API/business-flow, LLM drift, and cross-module regression to Validation Center, CI, or Nightly and consume compact receipts first.
- Branch changed-file discovery uses merge-base semantics plus worktree/index/untracked changes; it must not include unrelated files added later to `main`.

## Production and authorization boundaries

- User backend start/stop/restart remains user-owned and target-specific. Validation, merge, finalizer, aftercare, cleanup, or another window never transfers that authority.
- DEV DB validation, production-target/migration authorization, immutable merge confirmation, migration execution, and readback are separate states.
- Authorization is action-scoped rather than message-scoped: one user instruction may bundle merge, exact cleanup targets, and a named production target/migration, with no second prompt after merge. Bare merge covers only source merge and required BUG/metadata synchronization; it implies no cleanup, DDL/DML, dependency install, activation, process control, or deletion.
- Report source merge, close-sync, root sync, cleanup, client install, dependencies, DDL, runtime activation, and post-restart verification independently.

## UI, docs, and evidence

- New operator UI follows shadcn-compatible tokens and component boundaries; protected legacy styling does not spread to new modules.
- Durable standards live under `docs/standards`, design under `docs/architecture` or `docs/analysis`, operations under `docs/operations`, and formal handoff under `docs/handoff`.
- Temporary exchange notes use ignored `tmp/handoff/`, `docs/handoff/_scratch/`, or `docs/handoff/local/` paths.
- Keep secrets out of reports, memory, logs, receipts, and handoffs; record only non-secret credential locations.

## Completion report

Report worktree/branch, commit and PR when available, exact changed files, direct tests, BUG/Issue state, delegated/nightly coverage, production gates, merge/close-sync/root-sync/cleanup separately, runtime/client activation, DB/runtime impact, and remaining blockers.
