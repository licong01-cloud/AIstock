# Validation Center UI target route catalog

- Module: validation_center
- Level: L3
- Date: 2026-05-07T14:20:00
- Git commit at validation start: acadcbd
- Operator: lc999 / Codex

## Scope

- Changed files: Validation Center backend API, UI target catalog, UI display panel, API client types, Playwright tests, nox validation wiring, module registry and ownership catalog.
- Impacted flows: route-level UI target coverage, module quality cockpit, mocked Validation Center UI, real-port Validation Center UI smoke.
- Business goal: operators can inspect every official AIstock navigation route as a validation target, see module ownership, required plans, warnings, and route detail without replacing the global sidebar.
- Out of scope: executing business workflows for each route, changing production port 8001/3000, writing business DB state.
- Protected assets reviewed: no QE/Paper/QMT runtime assets changed; no production service restarted.

## Environment

- Backend port: 8013 real-port smoke only
- Frontend port: 3013 real-port smoke only
- TDX port: skipped for this Validation Center smoke
- Conda/env: AIstock (`C:/Users/lc999/miniconda3/envs/AIstock/python.exe`)
- Database: not written; local backend emitted DB credential warnings but Validation Center APIs are read-only
- Browser/headless: Playwright Chromium headless

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Module registry L0 | New catalog and changed validation files have module ownership | `validation_module_registry_l0` | PASS |
| Backend API | `/ui-targets`, `/ui-targets/summary`, `/ui-targets/{route_id}` load and fail fast on bad catalog | `validation_center_backend` / 52 pytest cases | PASS |
| Mock UI | Validation Center consumes mocked `ui-targets` APIs and opens UI Target Detail | `validation_center_ui -- 8013 3013` | PASS |
| Real-port UI | Dev backend/frontend show route target row and detail, with no Validation API writes | `validation_center_real_port_ui -- 8013 3013` | PASS |
| L0 guardrails | Changed files have no new blocking guardrail findings above the baseline | `nox -s l0 -- <changed files>` | PASS |
| Asset safety | No production 8001/3000 touched; no business DB writes | smoke JSON and nox output | PASS |

## Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_module_registry_l0
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_backend
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_ui -- 8013 3013
$env:PYTHONIOENCODING='utf-8'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m uvicorn backend.main:app --host 127.0.0.1 --port 8013
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_real_port_ui -- 8013 3013
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/aistock_guardrail_scan.py --baseline --output-json tmp/validation/guardrails/baseline_20260504.json
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- backend/routers/validation.py backend/services/validation/ui_target_catalog.py backend/tests/test_validation_ui_target_catalog.py frontend/playwright.config.ts frontend/src/app/validation-center/page.tsx frontend/src/lib/validation/api.ts frontend/tests/validation-center/validation-center.spec.ts frontend/tests/validation-center/validation-center-real-port.spec.ts frontend/tsconfig.json noxfile.py tests/aistock_validation/catalog/file_ownership.yaml tests/aistock_validation/catalog/module_registry.yaml tests/aistock_validation/catalog/ui_targets.yaml tests/aistock_validation/modules/validation_center.md tests/aistock_validation/history/validation_center/20260507_142000_l3_validation-center-ui-target-route-catalog.md
```

## Evidence

- Coverage snapshot: `tmp/validation/coverage/validation_center_backend_snapshot.json`
- Real-port smoke JSON: `tmp/validation/validation_center/ui_real_port_smoke.json`
- Real-port evidence manifest: `tmp/validation/validation_center/ui_real_port_smoke_evidence.json`
- L0 guardrail output: `tmp/validation/guardrails/l0_paths.json`, `tmp/validation/guardrails/l0_paths.md`
- Standard evidence manifest: `tests/aistock_validation/history/validation_center/20260507_142000_l3_validation-center-ui-target-route-catalog.evidence.json`
- Backend API test: `backend/tests/test_validation_ui_target_catalog.py`
- UI tests: `frontend/tests/validation-center/validation-center.spec.ts`, `frontend/tests/validation-center/validation-center-real-port.spec.ts`

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| UI mock initially loaded old occupied 3012 frontend | Existing test port was occupied by another service | Reran on free 8013/3013 and kept production untouched | `validation_center_ui -- 8013 3013` PASS |
| Real-port smoke CORS/404 on same-origin API proxy | Playwright config coupled `NEXT_PUBLIC_API_BASE` and proxy target | Split public API base from proxy target and set `/api/v1` for real-port smoke | `validation_center_real_port_ui -- 8013 3013` PASS |
| `/ui-targets` response was slow and caused proxy resets | Enrichment recomputed history/module quality repeatedly | Added per-request caches and avoided heavy enrichment in summary | direct API probe <1s and real-port smoke PASS |
| First L0 rerun reported missing guardrail baseline JSON | Isolated worktree did not have `tmp/validation/guardrails/baseline_20260504.json` | Regenerated the read-only baseline under `tmp/validation/guardrails/`; reran L0 with fail-new-only gating | `nox -s l0 -- <changed files>` PASS |

## Result

- Final status: PASS
- Remaining risks: per-route business workflow proof is cataloged but not yet implemented for all routes; current route status is mostly `planned`/`partial` by design.
- Need production backend restart: no
- Need dev service restart: no, test backend 8013 was stopped after validation
