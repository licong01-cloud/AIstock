# Validation Center Runner Bugfix Code Review

- Module: validation_center
- Level: L3
- Date: 2026-05-04
- Base commit reviewed: `4c74924`
- Operator: Codex

## Review Scope

- `backend/services/validation/execution_runner.py`
- `backend/services/validation/history_store.py`
- `backend/routers/validation.py`
- Validation Center runner smoke scripts, nox sessions, backend tests, and UI contract.

## Findings Fixed

| Finding | Risk | Fix | Regression proof |
|---|---|---|---|
| Executor exceptions could leave jobs stuck in `running` without archive/evidence | Runner queue false-running state and missing evidence | Catch executor exceptions, write explicit failed log/error, and still archive failed job | `test_runner_marks_executor_exception_failed_and_archives` |
| `/executions/{job_id}/evidence` only read transient local runner evidence | Evidence detail became incomplete after local tmp evidence cleanup | Fall back to `archive.runner_evidence_archive_path` when local evidence is missing | `test_runner_executes_allowlisted_plan_and_writes_evidence` deletes local evidence and verifies archive fallback |
| Copied Markdown guardrail artifacts under history were discovered as fake validation runs | Run history/summary polluted by evidence artifacts | Archive new guardrail Markdown artifacts as TXT and ignore legacy `*-guardrail-md.md` / `*-l0-guardrail.md` in run discovery | `test_runs_list_and_detail_preserve_success_scope` |
| Log tail read loaded the whole log before truncation | Large logs could create avoidable memory pressure | Read at most the final 512 KiB from disk before line tailing | backend tests and live smoke |

## Validation Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m compileall backend/services/validation/execution_runner.py backend/services/validation/history_store.py backend/tests/test_validation_execution_runner.py backend/tests/test_validation_center_api.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/test_validation_execution_runner.py backend/tests/test_validation_center_api.py backend/tests/test_validation_center_runner_smoke.py backend/tests/test_validation_center_readonly_smoke.py -q -p no:cacheprovider
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_backend
$env:BACKEND_PORT='8011'; $env:FRONTEND_PORT='3011'; $env:NEXT_PUBLIC_API_BASE='http://127.0.0.1:8011/api/v1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_ui
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/aistock_validate.py ports 8012
$env:PYTHONIOENCODING='utf-8'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m uvicorn backend.main:app --host 127.0.0.1 --port 8012 --log-level warning
$env:BACKEND_PORT='8012'; $env:VALIDATION_CENTER_API_BASE='http://127.0.0.1:8012/api/v1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_runner_smoke -- 8012
$env:BACKEND_PORT='8012'; $env:VALIDATION_CENTER_API_BASE='http://127.0.0.1:8012/api/v1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_live_readonly -- 8012
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/aistock_validate.py ports 8012
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- backend/services/validation/execution_runner.py backend/services/validation/history_store.py backend/tests/test_validation_execution_runner.py backend/tests/test_validation_center_api.py tests/aistock_validation/modules/validation_center.md tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-bugfix-review-validation.md tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-bugfix-review-validation.json tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-bugfix-review-evidence.json tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-bugfix-review-snapshot.json tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-bugfix-review-runner-smoke.json tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-bugfix-review-readonly-smoke.json tests/aistock_validation/history/development-guardrails/20260504_125819_l0_guardrail-changed-files_505ca58a_runner-validation.md
```

## Evidence

- Run metadata: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-bugfix-review-validation.json`
- Evidence manifest: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-bugfix-review-evidence.json`
- Coverage snapshot: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-bugfix-review-snapshot.json`
- Runner live smoke: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-bugfix-review-runner-smoke.json`
- Read-only live smoke: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-bugfix-review-readonly-smoke.json`
- Runner smoke evidence: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-bugfix-review-runner-smoke-evidence.json`
- Read-only smoke evidence: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-bugfix-review-readonly-smoke-evidence.json`
- L0 guardrail JSON/MD: `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-bugfix-review-l0-guardrail.json`, `tests/aistock_validation/history/validation_center/20260504_l3_validation-center-runner-bugfix-review-l0-guardrail.md`
- Archived runner run id: `development-guardrails_20260504_125819_l0_guardrail-changed-files_505ca58a_runner-validation__f97d2cc132`
- Archived runner artifacts: `tests/aistock_validation/history/development-guardrails/20260504_125819_l0_guardrail-changed-files_505ca58a_runner-guardrail-json.json, tests/aistock_validation/history/development-guardrails/20260504_125819_l0_guardrail-changed-files_505ca58a_runner-guardrail-md.txt`
- Backend nox: `32 passed`, line coverage `80.92`, branch coverage `64.4`.
- UI nox: TypeScript passed and Playwright `1 passed`.
- Live runner smoke: `endpoint_count=6`, `failure_count=0`.
- Live read-only smoke: `endpoint_count=18`, `failure_count=0`, `write_methods_sent=[]`.
- L0 guardrail: targeted nox `l0` passed with `blocking=0`; remaining findings are two P2 complexity review items in `execution_runner.py`.

## Result

- Final status: PASS.
- Production impact: no production backend `8001` restart; no production `8001` API touch; no remote API restart; no DB writes; no business schema changes.
- Residual risk: DB-backed runner scheduling/cancel/retry remains a later Validation Center phase.
