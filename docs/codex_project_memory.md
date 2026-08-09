# AIstock Codex Project Memory

## Purpose

This file is the lightweight project-level memory for Codex and other LLM coding clients working on AIstock.
It must stay stable, short, and operational. Do not use it as a changelog, module history, handoff log, or design archive.

Default rule: load this file only for AIstock architecture, backend, frontend, data pipeline, trading, issue workflow, CI/CD, or production-adjacent work. For ordinary issue work, prefer the issue Context Pack and relevant files over full historical documents.

## Active Multi-Alpha Strategy Evolution Snapshot

This is the compact active-work snapshot as of 2026-08-09. Detailed evidence remains in the authoritative blueprint; this section records only the current boundary and next order.

- Strategy evolution is the highest priority. The active blueprint is `docs/analysis/sector_rotation_factors_develop_spec_20260710.md` v6.0. MA-E07 four-leg soft de-risk and MA-E10 drop-TCN three-leg are the current full-window Pareto anchors; MA-E12 proved that a generic additional factor/model leg can degrade the portfolio; MA-E13G reclassified static industry-bias as a relation prior/portfolio regularizer rather than direct Alpha; MA-E13R exposed weak recent-regime performance despite better drawdown.
- The active problem is recent-regime adaptation plus genuinely orthogonal Alpha, not platform completion. The next order is: three-leg regime policy; compression-expansion, large-order-versus-small-order participation-gap and leadership-exhaustion experts; multi-model disagreement weighting; dynamic-relation drawdown hazard; then PIT fundamental diffusion and right-tail multitask research. Order-size buckets are proxies and do not identify investor account types.
- Every candidate separates `DIRECT_ALPHA`, `CONDITIONING_STATE`, `RELATION_PRIOR`, `PORTFOLIO_POLICY` and `NEGATIVE_CONTROL` roles. Full, early and late windows are reported together; trained candidates use matched seeds where applicable. These are attribution methods, not research admission gates.
- Multi-Alpha P0-1 through P0-4 remain maintenance-only. Only a reproducible blocker exposed by a real strategy trial, with no safe alternative path, may interrupt strategy evolution. UI, Archive, historical backfill/materialization, F-014 platform completion and generic infrastructure work remain terminated.
- QE training, prediction, backtest, composer and worker data planes consume only versioned, cutoff/hash-pinned H5/Parquet/bin/sidecar files. Database use is limited to control and result planes; missing files, identity drift or runtime DB data access fail closed.
- Each new strategy trial persists only the minimum versioned candidate/config, input identity, metrics, cost, risk and failure reason needed for comparison. Existing historical facts remain read-only; no evidence-completion or provenance project is authorized.
- Non-negotiable scope: QE-only isolation; reuse current QE/combine-backtest execution; no simplified delivery, silent fallback, business-logic drift, new approval workflow, backend process control, production DDL/DML or non-QE side effects.

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
- Do not create or keep a non-root worktree on local `main`; task worktrees must use task branches. If a stale worktree holds `main`, audit it read-only and report its exact absolute path, branch, and SHA. Removing that worktree or cleaning its branch requires separate user authorization for the named targets; merge or aftercare authorization does not imply cleanup authorization.
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
- In interactive windows, prefer RTK wrappers for supported high-output commands. Use direct commands for unsupported PowerShell/file operations, record capability fallback when relevant, never self-authorize `rtk trust`, and do not make RTK availability or telemetry a gate. CI uses original deterministic commands.


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
- Task cards carry a lazy runtime contract. `runtime_impact=unknown` fails closed; backend/worker/scheduler fixes require persistent-source plus fresh-process evidence before PR readiness, then remain `fixed_source_pending_user_restart` until the user's restart is followed by `post-restart-verify` identity and business-smoke evidence.
- `restart-plan` expands only runtime catalog and operator-runbook references. `post-restart-verify` is read-only against runtime/API/DB and writes only ignored workflow receipts; pass the receipt to close-sync to persist final BUG/GitHub state.
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

- Do not start, stop, or restart any user backend unless the user explicitly authorizes this window for that concrete target. BUG repair, validation, delegation, merge, finalizer, aftercare, cleanup, or prior authorization for another target does not transfer process-control authority. Runner-owned temporary processes are permitted only when explicitly marked and isolated on validation ports.
- Database DDL and DML must first be validated in the existing DEV database. Production DDL/DML requires explicit authorization for the specific production target; report DEV validation, production authorization, migration execution, and readback verification as separate states.
- Local validation ports are owned by `noxfile.py`, environment variables, and the active standard; use the workflow-provided defaults instead of hardcoding ad hoc ports here.
- Runtime activation and code merge are separate steps.
- Source merge, source cleanup, backend restart, post-restart verification, frontend activation, client reload, database migration and BUG close-sync are separate states. Source cleanup does not wait for a user restart but retains its own authorization boundary.
- Authorization to merge a PR or branch covers the source merge and required source/metadata synchronization only. It does not authorize production DDL/DML, dependency installation, runtime activation, program control, or deletion of files, worktrees, or branches; report each state separately and request target-specific authorization where required.
- Every completion report must state:
  - `production_ddl_gate`: `noop`, `applied_and_verified`, or `pending`.
  - `production_frontend_dependency_gate`: `noop`, `applied_and_verified`, or `pending`.
  - `production_backend_dependency_gate`: `noop`, `applied_and_verified`, or `pending`.
- If a merged change needs new DB objects, report `production_ddl_gate=pending` until the specific production target is authorized and the committed migration plus readback verification have completed. If there is no schema change, report `production_ddl_gate=noop`.

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
