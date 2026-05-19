# BUG-060 research pipeline UI target catalog

- Module: validation_center
- Level: L2
- Date: 2026-05-19T15:26:12
- Git commit at validation start: 733be35
- Operator: lc999 / Codex App

## Scope

- Changed files: `tests/aistock_validation/catalog/ui_targets.yaml`, BUG-060 registry JSON, this validation record and its evidence manifest.
- Impacted flows: Validation Center UI target catalog contract, `validation_center_backend` CI job, PRs blocked by nav/catalog drift.
- Business goal: `/research-pipeline` is an official navigation route and must have exactly one route-level validation target so Validation Center can track coverage without breaking unrelated MiniQMT fixes.
- Out of scope: changing Research Pipeline runtime/API/UI behavior, adding new research-pipeline nox sessions, touching production ports `8001/3000`, DB writes/migrations, MiniQMT orders/runtime.
- Protected assets reviewed: no StrategyPackage manifest/model/HMM/QE/Paper/QMT assets changed; no runtime service was restarted.

## Environment

- Backend port: not started; tests are offline/local nox sessions only.
- Frontend port: not started.
- TDX port: not used.
- Conda/env: `C:/Users/lc999/miniconda3/envs/AIstock/python.exe`; also retried one command through `conda run -n AIstock`.
- Database: not accessed for writes; no business schema touched.
- Browser/headless: not used; this fix is catalog-only and the UI contract is covered by backend catalog tests.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Targeted catalog regression | `NAV_GROUPS` href set equals `ui_targets.yaml` href set, including `/research-pipeline` | `pytest backend/tests/test_validation_ui_target_catalog.py::test_default_ui_target_catalog_matches_frontend_nav_groups -q` | PASS, 1 passed |
| Validation Center backend | Full backend contract, catalog schema, API/read-only validation and coverage gate pass | `nox -s validation_center_backend` | PASS, 101 passed, line 80.19%, branch 60.52% |
| Module ownership | Validation catalog and ownership mappings remain mapped and unambiguous | `nox -s validation_module_registry_l0` | PASS, 8 passed, 12/12 mapped |
| Changed-file guardrail | Staged BUG JSON/catalog/history files have no guardrail findings and are mapped | `nox -s guardrail_changed_files` | PASS, 4/4 mapped, 0 findings |
| L0 guardrails | Repository L0 gate has no blocking new P1 finding | `nox -s l0` | PASS; existing P2 raw-JSON warnings remain non-blocking |
| Diff whitespace | No whitespace/error diff issues | `git diff --check` | PASS |
| Asset safety | No production ports, DB writes, MiniQMT actions, or protected assets touched | command scope and git diff | PASS |

## Commands

```powershell
conda run -n AIstock python -m pytest backend/tests/test_validation_ui_target_catalog.py::test_default_ui_target_catalog_matches_frontend_nav_groups -q
conda run -n AIstock python -m nox -s validation_center_backend
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_module_registry_l0
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s guardrail_changed_files
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0
git diff --check
```

## Evidence

- Coverage snapshot: `tmp/validation/coverage/validation_center_backend_snapshot.json`
- Module ownership output: `tmp/validation/module_ownership/l0_paths.json`, `tmp/validation/module_ownership/l0_paths.md`
- Changed-file guardrail output: `tmp/validation/guardrails/changed_files.json`, `tmp/validation/guardrails/changed_files.md`, `tmp/validation/module_ownership/changed_files.json`, `tmp/validation/module_ownership/changed_files.md`
- L0 guardrail output: `tmp/validation/guardrails/l0_paths.json`, `tmp/validation/guardrails/l0_paths.md`
- Standard evidence manifest: `tests/aistock_validation/history/validation_center/20260519_152612_l2_bug-060-research-pipeline-ui-target-catalog.evidence.json`
- BUG source of truth: `tests/aistock_validation/bugs/20260519_BUG-060-validation-center-ui-target-catalog-missing-research-pipelin.json`
- Catalog fix: `tests/aistock_validation/catalog/ui_targets.yaml`

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| `conda run -n AIstock python -m nox -s validation_module_registry_l0` failed before nox start | Concurrent `conda run` temp-file lock while `validation_center_backend` was still running | Reran with the environment Python directly, avoiding the conda temp wrapper | `C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_module_registry_l0` PASS |
| First `guardrail_changed_files` found BUG JSON unmapped | `tests/aistock_validation/bugs/**` had no file ownership rule even though BUG JSON is source-of-truth input | Added `validation_bug_registry` ownership rule and expanded BUG-060 scope to include `file_ownership.yaml` | `C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s guardrail_changed_files` PASS |
| PR #62 CI failure: `nav_hrefs - target_hrefs == {'/research-pipeline'}` | `/research-pipeline` was added to official `NAV_GROUPS` but not to Validation Center `ui_targets.yaml` | Added route target `research.pipeline` for `/research-pipeline` under `Validation Pipeline` with high risk and explicit business operations | `validation_center_backend` PASS |

## Result

- Final status: PASS
- Remaining risks: `/research-pipeline` still has `coverage_status: planned`; this fix only restores the catalog contract and does not claim route-level real business UI proof.
- Need production backend restart: no
- Need frontend restart: no
- Need dev service restart: no
