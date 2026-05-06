# Validation Center module Chinese descriptions and navigation source mapping

- Module: validation_center
- Level: L3
- Date: 2026-05-06T14:52:37+08:00
- Git commit at validation start: 793e196
- Operator: lc999 / Codex

## Scope

- Changed files: module registry/catalog, module quality API payload, Validation Center UI, shared navigation source, Sidebar import, Playwright specs, nox Validation Center UI session.
- Impacted flows: module-based ownership/quality cockpit, Validation Center page route coverage entry, shared left-navigation source, mocked UI E2E, real-port UI smoke.
- Business goal: every module has a readable Chinese description; Validation Center can reuse the same NAV_GROUPS as the official sidebar and show route-to-module quality/test coverage status without covering the original sidebar.
- Out of scope: full route-level business validation, ui_targets.yaml implementation, independent route coverage API, production backend reload.
- Protected assets reviewed: no QE/Paper/Qlib/model/HMM/trading artifacts intentionally modified.

## Environment

- Backend port: 8012 for real-port smoke only; stopped after validation.
- Frontend port: 3012 for validation UI only; Playwright webServer stopped after validation.
- Production ports: 8001/3000 not restarted or stopped.
- Conda/env: `C:/Users/lc999/miniconda3/envs/AIstock/python.exe`.
- Browser/headless: Playwright Chromium headless.
- Database: real-port backend logged local PostgreSQL auth warnings, but Validation Center read-only endpoints returned HTTP 200 and the smoke did not require DB writes.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Module registry L0 | Registry loads, all modules have `description_zh`, file ownership maps changed catalog files | `validation_module_registry_l0` | PASS |
| Backend contract | Validation Center API and coverage parser pass with coverage gates | `validation_center_backend`, coverage line 82.34 / branch 65.70 | PASS |
| Mocked UI E2E | Validation Center displays Git/module quality, navigation source panel, selected route coverage detail, and only controlled runner POST | `validation_center_ui -- 8012 3012` | PASS |
| Real-port UI smoke | Dev backend/frontend on 8012/3012 display Git/module quality and navigation route detail with only GET validation API calls | `validation_center_real_port_ui -- 8012 3012`, `tmp/validation/validation_center/ui_real_port_smoke.json` | PASS |
| L0 guardrails | Skill validation, quality guardrails, changed-path guardrail baseline gate pass | `l0 -- <changed files>` | PASS |
| Diff hygiene | No whitespace errors | `git diff --check` | PASS |

## Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_module_registry_l0
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_backend
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_ui -- 8012 3012
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m uvicorn backend.main:app --host 127.0.0.1 --port 8012
$env:BACKEND_PORT='8012'; $env:FRONTEND_PORT='3012'; $env:VALIDATION_CENTER_API_BASE='http://127.0.0.1:8012/api/v1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_real_port_ui -- 8012 3012
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- backend/services/validation/module_registry.py backend/services/validation/module_quality.py backend/tests/test_validation_module_ownership.py frontend/src/app/Sidebar.tsx frontend/src/app/validation-center/page.tsx frontend/src/lib/navigation/nav-groups.ts frontend/src/lib/validation/api.ts frontend/tests/validation-center/validation-center.spec.ts frontend/tests/validation-center/validation-center-real-port.spec.ts tests/aistock_validation/catalog/module_registry.yaml noxfile.py
git diff --check
```

## Evidence

- Coverage snapshot: `tmp/validation/coverage/validation_center_backend_snapshot.json`.
- Real-port smoke: `tmp/validation/validation_center/ui_real_port_smoke.json`.
- Real-port evidence manifest: `tmp/validation/validation_center/ui_real_port_smoke_evidence.json`.
- L0 guardrail output: `tmp/validation/guardrails/l0_paths.json`, `tmp/validation/guardrails/l0_paths.md`.
- Module ownership output: `tmp/validation/module_ownership/l0_paths.json`, `tmp/validation/module_ownership/l0_paths.md`.
- Standard evidence manifest: `tests/aistock_validation/history/validation_center/20260506_145237_l3_validation-center-module-chinese-descriptions-and-navigation-source-mapping-evidence.json`.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| `validation_center_ui` initially used stale occupied 3011 frontend and also included real-port spec | The mocked UI session targeted the whole `tests/validation-center` directory and reused an occupied frontend port | Limited `validation_center_ui` to `validation-center.spec.ts`; reran on free 3012 | `validation_center_ui -- 8012 3012` PASS |
| Route row test selected the workspace table row containing `frontend/src/app/validation-center/page.tsx` | Locator searched the first `tr` containing `/validation-center` | Scoped locator to the `?????` navigation route row | Mocked UI and real-port UI both PASS |
| Real-port backend in isolated worktree missed ignored `backend/services/rl_execution` files | Repo ignores `rl_execution/`, but `backend.main` imports the router dependency | Copied ignored runtime dependency from production root into the worktree for validation only; not committed | Backend 8012 started, real-port UI PASS |

## Result

- Final status: PASS.
- Business assertions: module Chinese descriptions are available from backend and UI; official Sidebar and Validation Center share `frontend/src/lib/navigation/nav-groups.ts`; Validation Center route coverage panel maps navigation routes to module quality data and shows clear Phase-1 boundaries.
- Remaining risks: route-level business coverage remains a future phase requiring `ui_targets.yaml` and route coverage API; current panel is a source-sharing and module-quality mapping baseline, not proof that every business page flow passed.
- Need production backend restart: no.
- Need dev service restart: no; temporary 8012 backend was stopped.
