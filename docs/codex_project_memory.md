# AIstock Codex Project Memory

## Purpose

This file is the lightweight project-level memory for Codex and other LLM coding clients working on AIstock.
It must stay stable, short, and operational. Do not use it as a changelog, module history, handoff log, or design archive.

Default rule: load this file only for AIstock architecture, backend, frontend, data pipeline, trading, issue workflow, CI/CD, or production-adjacent work. For ordinary issue work, prefer the issue Context Pack and relevant files over full historical documents.

## Active Multi-Alpha QE Foundation Resume Snapshot

This is the compact active-work snapshot as of 2026-07-19. Replace or remove it after the feature is merged and activated; detailed research history and implementation evidence remain in the authoritative design rather than this memory file.

- Design authority: `docs/architecture/multi_alpha_qe_evolution_foundation_f2_design_20260718.md`. It contains the complete progress ledger, experiment-facing constraints, acceptance IDs F-201 through F-218, and P0-1A through P0-4 implementation order.
- Current phase design: `docs/architecture/multi_alpha_p0_1b_durable_execution_orchestrator_f2_design_20260719.md`, subordinate to the parent authority above. Its P0-1B contracts were reviewed against current AIstock source, production schema, live QE Workspace OpenAPI, and the owning RD-Agent source. The design now requires server-side submission receipts, one shared `infra.qe_execution_reservation` ledger, immutable task identity separated from run scenarios, post-terminal Archive delivery events, and separate transport/deadline/wait semantics. Code, reservation DDL, and the RD-Agent companion change are pending.
- P0-1A source is merged: PR #2464, merge commit `5f698b3be504aff9a8a05354240dba84ef68a8e4`; BUG-767 close-sync is merged in PR #2467, commit `d230a33c0d4fe1ab9030401fed5e0cd35b247639`; BUG-767/GitHub issue #2459 are closed.
- P0-1A completed: additive preflight/forward/guarded-rollback SQL; durable task/run/child/attempt/event models and repository; canonical request/artifact identity; explicit state machines; PostgreSQL claim, lease, fencing and row-version CAS; atomic state/event transactions; historical task/run/result-child dry-run, execute and readback without fabricated attempts or changes to historical metrics/status/reason/created_at/Archive.
- P0-1A validation: Ruff and compile passed; existing plus new targeted matrix passed with `82 passed, 4 skipped`; the four opt-in PostgreSQL tests separately passed in a disposable PostgreSQL 16 container. That run verified two consecutive migrations without catalog drift, idempotent historical backfill, eight-worker single claim, event-failure transaction rollback, stale fencing rejection, and child/attempt remote identity/result persistence.
- Production state: on 2026-07-19 the committed P0-1A migration was applied to `127.0.0.1:5432/aistock` and verified by SQL plus `MultiAlphaDurableRepository.preflight_schema(raise_on_error=True)`. Historical backfill completed with 12 tasks, 41/41 run assignments, 138 result children (59 scheme, 79 LOO), zero fabricated attempts/events, zero mismatches/orphans, and protected digest `733d48413364658972bbef1be625b205e1eb191c5df8e9e0f2465d3bea4bffa4` unchanged. No database export or extra backup was created, and no backend/frontend service was restarted. Operationally `production_ddl_gate=applied_and_verified`; frontend/backend dependency gates are `noop`. The tracked BUG-767 JSON still needs a later metadata close-sync from `pending` to the applied state.
- Runtime state: the current combine-backtest path is still process-owned (`daemon=True`, in-memory `_NODE_RESERVATIONS`, child `ThreadPoolExecutor`); new runs do not yet use durable attempt dispatch or restart takeover.
- Remaining order: P0-1B first implements the RD-Agent QE Workspace receipt contract and AIstock reservation migration, then the execution adapter/persistent orchestrator with unified WSL/remote capacity and restart takeover; P0-2 pause/resume/cancel and retry/recovery; P0-3 reuse the existing QE evolution page and complete multi-alpha creator; P0-4 child/attempt grid, DB events/logs, recovery and Archive visibility; then full F2 compliance, PR, merge and separately authorized DDL/deployment/runtime activation.
- Non-negotiable scope: QE-only isolation; reuse the current combine-backtest/QE Workspace/QE UI architecture; no parallel platform, simplified implementation, silent error/fallback, business-logic drift, research admission gate, or approval workflow. Missing data/artifacts must stay visible and recoverable rather than eliminate a research direction. Any future production DDL or runtime restart remains a separate explicit user-authorized step, and no extra DB export is performed before DDL.

## Repository And Runtime Map

- Canonical repository root: `F:\Dev\AIstock`.
- Normal development must use isolated worktrees under `F:\Dev\AIstock_worktrees\<task-name>`.
- The root checkout `F:\Dev\AIstock` is the sync/runtime baseline, not the default development workspace.
- Main backend: FastAPI under `backend/`, typical production port `8001`.
- Main frontend: Next.js under `frontend/`, typical production port `3000`.
- Market data bridge: TDX Go service under `tdx-api-main/`, typical port `19080`.
- Important domains: `backend/routers`, `backend/services`, `backend/data_service`, `backend/infra`, `frontend/src/app`, `tests/aistock_validation`.

## Development Standard

Use the development standard only when it is relevant to the current task. Start from `docs/standards/README.md` when routing is unclear.

- `docs/standards/README.md`
- `docs/standards/aistock_development_standard_v1.5_20260523.md`

The Markdown file is the sole human-readable authority. Its same-version YAML is a machine-derived catalog. Quickstarts, architecture documents, skills, and commands provide scenario guidance and reference the authority.

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

## Advisory Research Program Working Memory

Current durable state as of 2026-07-19. Detailed contracts and acceptance evidence remain in the architecture documents, BUG records, and PR history rather than this file.

### Fixed Product And Isolation Constraints

- Advisory is historical/academic research output only. It may rank candidates and later estimate research-only holding periods, returns, or price ranges, but it must not create orders, execution instructions, or real-trading integration.
- Multiple Advisory Programs and strategy packages run independently at the same time. Each Program binds either one admitted single-Alpha package or one admitted native multi-Alpha parent package. Manual cross-package Alpha composition in the Advisory page is retired and must not return.
- A strategy package is validated when admitted. Advisory, Selection, simulation, and Paper consumers must not add a second package asset/executability admission gate. Runtime validation is limited to the exact data, identity, PIT, lineage, and artifact contracts required for that operation.
- Advisory must remain isolated from Selection, simulation, Paper, QE/RD-Agent, Qlib/backtest, QMT, and trading runtime ownership. Shared production ST PIT data may be used by Selection and simulation, but Advisory/QE research evidence and all backtest PIT files remain isolated. Advisory must not consume backtest results or backtest Parquet as runtime input.
- Advisory market and training inputs come from the configured database and immutable Advisory evidence stores. Database connection details are read from the explicit `.env`; never guess them. Training extraction may materialize files to avoid repeated database reads.
- All Advisory model training runs in the WSL Conda environment, not Windows Python. No trained ranking/return/holding-period/price model is activated yet.
- Do not introduce roles, approvals, acknowledgements, backup gates, enable flags, package re-approval, or other manual workflow gates without explicit user confirmation. DDL is a development/release action only, uses committed migrations and explicit authorization, and does not require a per-DDL full database backup gate.

### Current Phase Status

- Parent authority: `docs/architecture/advisory_strategy_conditioned_model_blueprint_v1_20260710.md`. Real DEV dual-track authority: `docs/architecture/advisory_real_dev_dual_track_input_onboarding_f2_design_20260716.md`.
- Phase 0A multi-package delivery supports admitted single-Alpha packages and admitted native multi-Alpha parent packages with independent Program execution. Historical evidence, PIT lineage, capacity, and compiler work must preserve Program isolation.
- O3 real DEV dual-track onboarding has created two Programs and two non-overlapping ACTIVE bindings effective from 2026-07-20. The exact request hash is `ba3f0c230b2f4efe3f1d85f15b1de2268f0ad2a3622661f83cfe79deaf8eed6f`. The earlier run correctly remained input-pending before that decision date completed.
- O4 Phase 1E implementation is merged. PR #2444 supplied the main O4 orchestration; BUG-764 PR #2449 and close-sync PR #2454 completed the authoritative typed capacity policy producer and the exact historical request/receipt ref chain. `run-historical` now emits typed refs that feed `observe-source`, `build-phase1e-inputs`, `plan-capacity`, and `compile-phase1e` without CAS scans or handwritten artifact payloads.
- BUG-764 is fixed and closed. Source merge commit: `69722f054218c1352f583bc9f9f1022c97821c86`; close-sync/main commit: `247237b75afca7b50937ff3addc6c24959ec5a4c`. Its production DDL and dependency gates were all `noop`; no runtime service or database was changed.
- Real O4 L3/L4 completion is still pending the completed 2026-07-20 ingestion and prospective DSE/receipt. After that, execute the exact sequence `observe-source -> run-historical -> build-phase1e-inputs -> plan-capacity -> compile-phase1e`, preserving every emitted ref. Do not claim Phase 1E or G5 real DEV completion before that evidence exists.
- G5/O5 must consume the exact O4 plan only after the real chain completes. Phase 0B model-quality and trainability work starts after the real sealed input boundary is available; no model training or Advisory runtime activation should be inferred from the completed infrastructure code.

### Workspace Residual

- As of 2026-07-19, `scripts/onboard_multialpha_sim_binding.py`, `scripts/reset_multialpha_sim_run.py`, and `scripts/seed_multialpha_sim_virtual_account.py` are absent and untracked. Do not assume they exist or recreate them from memory; obtain an authoritative source or new explicit design if they are needed.

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
