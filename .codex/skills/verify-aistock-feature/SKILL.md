---
name: verify-aistock-feature
description: "Run AIstock result-oriented validation after backend, frontend, data pipeline, QE/RD-Agent, StrategyPackage, Selection Center, Paper Trading v2, HMM, execution algorithm, or UI changes. Use when Codex must design and execute tests, API/business-flow checks, Playwright UI E2E, log scans, asset-safety checks, and business outcome verification before reporting a feature or bug fix as complete."
---

# Verify AIstock Feature

Use this skill to turn an AIstock code change into a repeatable validation package: risk analysis, test matrix, automated checks, UI E2E, log/DB/API cross-checks, business oracles, and persisted evidence.

## Required First Reads

Read these files before planning or running validation:

1. `AGENTS.override.md`
2. `docs/codex_project_memory.md`
3. `docs/standards/aistock_development_standard_v1.5_20260523.md`
4. The approved feature design document, implementation plan, GitHub Issue / BUG JSON `closure_requirements`, and any explicit user acceptance points.
5. `docs/architecture/aistock_result_oriented_testing_standard.md`, if present.
6. `docs/architecture/aistock_testing_version_management_system_design_20260429.md`, if present.
7. The impacted module matrix under `tests/aistock_validation/modules/`, if present.

Do not modify `AGENTS.md`.

## Workflow

1. Classify impact: frontend, backend API, repository/DB, data pipeline, QE/RD-Agent, StrategyPackage, Selection Center, Paper Trading v2, HMM, execution algorithm, UI, or protected assets.
2. Define the business goal and false-success risks before writing or running tests.
3. Build a design compliance checklist: every approved design item, user acceptance point, issue closure requirement, UI/API/DB/MCP/QE/RP/Paper/HMM behavior, and loop/experiment-level action must map to implementation refs and validation evidence.
4. Build a test matrix using risk-based testing, state transitions, decision tables, equivalence classes, boundary values, and pairwise combinations when needed.
5. Run guardrails first: type/lint checks relevant to changed files, hardcoded-path scan, secret scan, silent-fallback review, and protected-asset diff review.
6. Run backend/API/data tests with pytest or explicit API scripts; verify API response, DB side effects, logs, and persisted events.
7. Run UI E2E when UI or workflow is affected. Use only development ports `8011`/`8012` and `3011`/`3012`; never restart production backend port `8001`.
8. Fail UI tests on page errors, console errors, request failures, and unexpected HTTP 4xx/5xx.
9. Save a test run record under `tests/aistock_validation/history/` with commands, ports, data samples, screenshots/traces, DB/API checks, bugs, fixes, reruns, and residual risks.
10. Fix every failure, add or update regression coverage, and rerun the failing test plus the surrounding module integration path.
11. Stage and commit only files modified for the current task; do not stage unrelated dirty workspace files.

## AIstock Business Oracles

Non-negotiable checks:

- No silent fallback, fake success, default price, default cash, default holdings, or empty-array business success.
- No daily-mode fallback for Paper Trading v2.
- No direct use of QE backtest prediction files as authoritative live selection.
- No silent protected-asset modification.
- UI must expose real backend capabilities with readable Chinese business state, not raw JSON dumps.
- No unapproved simplified, subset, POC, placeholder, mock-only, backend-only, or partial-loop delivery may be reported as complete.
- Pipeline success is insufficient unless the design compliance checklist is fully covered or every gap has explicit user approval.
- Backend fail-fast errors must reach the UI and tests with actionable code/context.
- For the first-stage rollout, treat Selection Center as part of the Paper Trading v2 validation slice.

## Useful Commands

Create a test run record:

```bash
python .codex/skills/verify-aistock-feature/scripts/new_test_run.py --module paper_trading_v2 --level L3 --title "Paper v2 replay regression"
```

Run the first-stage local Paper v2 + Selection Center validation entry points:

```bash
conda run -n AIstock python -m nox -s l0
conda run -n AIstock python -m nox -s paper_v2_backend
set BACKEND_PORT=8012
set FRONTEND_PORT=3011
python scripts/aistock_validate.py services --backend-port 8012 --tdx-port 19080
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s paper_v2_l3
```

Run the trading-hours Paper v2 catch-up-to-live validation after TDX has current-day minute bars:

```bash
set BACKEND_PORT=8012
set FRONTEND_PORT=3011
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s paper_v2_live -- --require-live-bars
```

By default this replays one latest completed historical trading day before live.
Use `--replay-lookback-trading-days N` only for longer catch-up coverage.

Validate this skill metadata:

```bash
python "%USERPROFILE%/.codex/skills/.system/skill-creator/scripts/quick_validate.py" .codex/skills/verify-aistock-feature
```

## Report Format

End every validation with:

- Implemented or fixed scope.
- Design compliance matrix: design item -> implementation refs -> API/DB/UI/log/test evidence -> status -> gap/approved exception.
- Test levels executed and exact commands.
- Business outcomes verified.
- UI/API/DB/log evidence paths.
- Bugs found and rerun results.
- Unimplemented or unverified capabilities with reasons.
- Whether backend/frontend restart is needed, and production port impact.
- Asset-safety status.
