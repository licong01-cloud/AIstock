# Validation Center Real-port UI Clean PR Branch Rerun

- Module: validation_center
- Level: L3
- Date: 2026-05-06T00:49:05+08:00
- Git commit: `085dac9`
- Branch: `codex/validation-real-port-ui-smoke-clean`
- Operator: Codex

## Scope

- Rebased the Validation Center real-port UI smoke work onto current `origin/main` in an isolated worktree so the PR branch contains only Validation Center changes.
- Verified the formal nox entry `validation_center_real_port_ui` against a full `backend.main:app` dev backend on port `8012` and a Playwright-managed dev frontend on port `3012`.
- Verified backend Validation Center contract tests and coverage gates on the same clean branch.
- Out of scope: production backend `8001`, production frontend `3000`, database schema writes, business-state writes, long-running QE/Paper tasks, and GitHub merge/rebase of the dirty main worktree.

## Environment

- Production backend `8001`: running, not restarted, not stopped, and not used.
- Existing dev backend `8011`: left untouched.
- Existing dev frontend `3011`: left untouched.
- Test backend `8012`: started for this rerun and stopped afterward.
- Test frontend `3012`: started by Playwright for this rerun and stopped afterward.
- Conda/env: `AIstock` via `C:/Users/lc999/miniconda3/envs/AIstock/python.exe`.
- Browser/headless: Playwright Chromium headless.
- Clean-worktree bootstrap: copied ignored local `backend/services/rl_execution/*.py` into the temporary worktree only because `origin/main` imports that service but `.gitignore` excludes the directory; no tracked code was changed or staged for this bootstrap.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Clean PR branch | Branch is based on `origin/main` with only Validation Center commits | `git log`, branch `codex/validation-real-port-ui-smoke-clean` | PASS |
| L0 guardrails | No new P1/P0 blocking finding on changed files | targeted `nox -s l0` | PASS |
| Backend tests | Validation Center backend/API/coverage contract remains valid | `validation_center_backend`: 47 passed, line 82.33, branch 65.7 | PASS |
| Real-port UI | Operator page loads Git workspace, commit activity, and module quality data from real dev APIs | `validation_center_real_port_ui`: Playwright 1 passed | PASS |
| API safety | No Validation API writes, no 4xx/5xx, no request/page/console errors | `*-ui-smoke.json` | PASS |
| Production isolation | Production `8001` and `3000` are not touched | port check and smoke JSON | PASS |

## Commands

```powershell
git worktree add -b codex/validation-real-port-ui-smoke-clean F:/Dev/AIstock_validation_clean origin/main
git cherry-pick 2d00052 b2b4391
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/aistock_guardrail_scan.py --baseline --output-json tmp/validation/guardrails/baseline_20260504.json
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- noxfile.py backend/services/validation/plan_catalog.py backend/tests/test_validation_center_api.py frontend/tests/validation-center/validation-center-real-port.spec.ts tests/aistock_validation/catalog/module_registry.yaml tests/aistock_validation/catalog/test_plans.yaml tests/aistock_validation/modules/validation_center.md tests/aistock_validation/history/validation_center/20260505_l3_validation-center-real-port-ui-smoke-nox.md tests/aistock_validation/history/validation_center/20260506_l3_validation-center-real-port-ui-full-backend-rerun.md
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_backend
$env:BACKEND_PORT='8012'; $env:PORT='8012'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m uvicorn backend.main:app --host 127.0.0.1 --port 8012
$env:BACKEND_PORT='8012'; $env:FRONTEND_PORT='3012'; $env:VALIDATION_CENTER_API_BASE='http://127.0.0.1:8012/api/v1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_real_port_ui
```

## Evidence

- Run metadata: `tests/aistock_validation/history/validation_center/20260506_004905_l3_validation-center-real-port-ui-clean-pr-branch-rerun.json`
- UI smoke JSON: `tests/aistock_validation/history/validation_center/20260506_004905_l3_validation-center-real-port-ui-clean-pr-branch-rerun-ui-smoke.json`
- Evidence manifest: `tests/aistock_validation/history/validation_center/20260506_004905_l3_validation-center-real-port-ui-clean-pr-branch-rerun-evidence.json`
- Temporary smoke JSON: `tmp/validation/validation_center/ui_real_port_smoke.json`
- Temporary evidence manifest: `tmp/validation/validation_center/ui_real_port_smoke_evidence.json`
- Smoke summary: status `passed`, validation responses `31`, bad responses `0`, request failures `0`, page errors `0`, console errors `0`, write methods `0`, `production_8001_touched=false`.
- Backend summary: `validation_center_backend` passed with 47 tests; coverage line `82.33`, branch `65.7`.
- Port closure: after rerun, `8012` and `3012` were free; `8001`, `8011`, and `3011` remained running and were not restarted by this step.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| First targeted L0 attempt aborted | Isolated worktree lacked the ignored guardrail baseline JSON | Generated a temporary baseline under `tmp/validation/guardrails/`, then reran targeted L0 | Targeted L0 passed with blocking `0` |
| First full backend startup in isolated worktree failed with missing `backend.services.rl_execution` | `origin/main` imports `backend.services.rl_execution`, but `.gitignore` excludes `rl_execution/`, so the clean worktree did not contain the ignored local service files | Copied ignored local `backend/services/rl_execution/*.py` into the temporary worktree for runtime-only validation; no tracked code was changed | Full backend started on `8012`, and real-port UI smoke passed |
| First real-port nox attempt in isolated worktree failed at `npm exec tsc` | Isolated worktree lacked ignored `frontend/node_modules` | Created a temporary junction to the existing root `frontend/node_modules` for validation only | `validation_center_real_port_ui` passed |

## Result

- Final status: PASS.
- Business outcome: the clean PR branch is verified for Validation Center real-port Git/module-quality UI behavior without using production ports or writing business state.
- Need production backend restart: no.
- Need dev service restart: no; `8012`/`3012` were stopped after validation.
- Remaining risks: GitHub CLI is not authenticated in this environment, so PR creation must be completed via browser/owner credentials or after `gh auth login`; the clean branch avoids the earlier main/origin divergence but does not solve the repository-level ignored `backend/services/rl_execution` packaging issue.
