# Validation Center Read-only API First-stage Complete Loop

- Module: validation_center
- Level: L2
- Date: 2026-05-04
- Git commit: pre-commit working tree based on current `main`
- Operator: Codex

## Scope

- Changed files:
  - `backend/services/validation/__init__.py`
  - `backend/services/validation/models.py`
  - `backend/services/validation/plan_catalog.py`
  - `backend/services/validation/history_store.py`
  - `backend/routers/validation.py`
  - `backend/tests/test_validation_center_api.py`
  - `tests/aistock_validation/catalog/test_plans.yaml`
  - `tests/aistock_validation/catalog/test_levels.md`
  - `tests/aistock_validation/modules/validation_center.md`
  - `docs/architecture/aistock_internal_validation_center_implementation_plan_20260504.md`
  - `docs/architecture/aistock_automated_testing_coverage_observability_design_20260504.md`
  - `noxfile.py`
  - `backend/main.py` validation-router registration and logging guardrail fix only; unrelated existing dirty hunks were not part of this task.
- Business goal: expose Validation Center plans, run history, coverage snapshots, evidence manifests, and summary through read-only API without executing commands or writing business state.
- Paper v2 gap integration: design documents and module matrix now reserve `pass_scope`, `business_assertion`, mock-vs-real claim boundaries, positive success gates, and future real sample registry.
- Out of scope: Validation Center frontend UI, controlled execution API, Paper v2/QE runtime changes, production `8001` restart, remote API restart, DB schema creation.

## Environment

- Backend service: not started by this validation.
- Frontend service: not started by this validation.
- Production `8001`: not restarted and not touched.
- Database: not used by Validation Center read-only API tests.
- Coverage reports: `tmp/validation/coverage/validation_center_backend.xml` and `tmp/validation/coverage/validation_center_backend.json`.

## Matrix

| Case | Expected result | Evidence | Result |
|---|---|---|---|
| L1 plan catalog allowlist | Valid plans load; unknown command key and production backend port are rejected fail-fast | `backend/tests/test_validation_center_api.py` | PASS |
| L2 health/plans API | `/api/v1/validation/health` and `/plans` return read-only status and plan catalog metadata | TestClient API tests | PASS |
| L2 run list/detail | `/runs` paginates and filters; `/runs/{run_id}` returns Markdown, metadata, coverage/evidence links, `pass_scope`, `business_assertion` | TestClient API tests | PASS |
| L2 missing metadata | Markdown-only run is marked `metadata_missing`; malformed JSON is marked `metadata_parse_error`; coverage is not faked | TestClient API tests | PASS |
| L2 coverage/evidence APIs | Coverage and evidence list/detail endpoints return schema-backed payloads and 404 unknown ids | TestClient API tests | PASS |
| L2 summary | Summary reports run counts, coverage/evidence counts, plan count, and module buckets | TestClient API tests | PASS |
| L2 coverage gate | Validation Center backend coverage snapshot passes line and branch thresholds | `20260504_l2_validation-center-readonly-api-snapshot.json` | PASS |
| L0 guardrail | Changed task files pass skill validation and high-severity guardrail scan | `nox -s l0 -- ...` | PASS |

## Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m compileall backend/routers/validation.py backend/services/validation backend/tests/test_validation_center_api.py noxfile.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/test_validation_center_api.py -q -p no:cacheprovider
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_backend
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_coverage_backend
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_data_contract_backend
$env:PYTHONIOENCODING='utf-8'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -c "from backend.main import create_app; app=create_app(); paths=app.openapi()['paths']; assert '/api/v1/validation/health' in paths; print('validation paths', len([p for p in paths if p.startswith('/api/v1/validation')]))"
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- backend/routers/validation.py backend/services/validation backend/tests/test_validation_center_api.py backend/main.py noxfile.py tests/aistock_validation/catalog/test_plans.yaml tests/aistock_validation/catalog/test_levels.md tests/aistock_validation/modules/validation_center.md docs/architecture/aistock_internal_validation_center_implementation_plan_20260504.md docs/architecture/aistock_automated_testing_coverage_observability_design_20260504.md
```

## Evidence

- Coverage snapshot: `tests/aistock_validation/history/validation_center/20260504_l2_validation-center-readonly-api-snapshot.json`
- Evidence manifest: `tests/aistock_validation/history/validation_center/20260504_l2_validation-center-readonly-api-evidence.json`
- Validation Center backend nox: `18 passed`, coverage line `84.99`, branch `71.74`, status `passed`.
- Validation coverage nox: `10 passed`, coverage line `81.57`, branch `68.55`, status `passed`.
- QE data contract nox: `17 passed`.
- Targeted API pytest: `8 passed`.
- OpenAPI smoke: `10` validation paths registered.
- L0 nox: successful, `0` high guardrail findings.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| `validation_center_backend` coverage run hit a pandas/numpy import error during collection | Test imported `backend.routers` package, which imports unrelated heavy routers through `backend/routers/__init__.py` | Load `backend/routers/validation.py` directly in the test with module name `backend.routers.validation` | `validation_center_backend` rerun passed with coverage |
| L0 guardrail flagged `backend/main.py` logging fallback | Existing safe logging handler had `except Exception: return None`, which matches the silent-empty-success guardrail | Changed the handler to call `self.handleError(record)` and use a bare return | L0 rerun passed with 0 high findings |

## Result

- Final status: PASS.
- Production impact: no production backend `8001` restart; no remote API restart; no DB writes; no business schema changes.
- Residual risks:
  - The current phase is backend read-only API only; UI and controlled execution remain future phases.
  - `pass_scope` / `business_assertion` are exposed when present and documented for future run records, but older history naturally lacks them.
  - `backend/main.py` had unrelated existing dirty hunks from other work; only validation-router registration and the logging guardrail fix belong to this task.
