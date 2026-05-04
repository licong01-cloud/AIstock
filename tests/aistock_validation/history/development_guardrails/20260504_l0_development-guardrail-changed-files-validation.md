# Development Guardrail Changed-files Baseline Gate

- Module: development_guardrails
- Level: L0
- Date: 2026-05-04
- Git commit: based on `19c10a5` plus current guardrail changes
- Operator: Codex

## Scope

- Extended `scripts/aistock_guardrail_scan.py` on top of the existing standards/YAML framework.
- Added finding-level `baseline_status`, summary `by_baseline_status`, gate metadata, `--baseline-json`, `--fail-new-only`, and `--staged-only`.
- Added `guardrail_changed_files` nox session for staged or changed-file validation.
- Updated `l0` so it also runs the standards-aware guardrail scanner with baseline suppression.
- Added unit tests for staged file discovery, baseline classification, new-only blocking, and enriched JSON/Markdown output.
- Out of scope: fixing historical baseline findings, DB-backed quality registry write-back, GitHub issue creation, and UI execution runner.

## Environment

- Backend/frontend services: not started.
- Production backend `8001`: not restarted and not touched.
- Database/business schemas: not touched.
- Guardrail baseline: `tmp/validation/guardrails/baseline_20260504.json`.
- Default local gate mode: `--staged-only`, to avoid scanning unrelated dirty files from other active windows.

## Matrix

| Case | Expected result | Evidence | Result |
|---|---|---|---|
| Staged file discovery | Scanner reads `git diff --cached --name-only --diff-filter=ACMRT` using UTF-8 | `backend/tests/test_aistock_guardrail_scan.py` | PASS |
| Baseline classification | Findings with baseline fingerprints are marked `baseline` | Unit test | PASS |
| New-only blocking | `fail_new_only=True` ignores baseline findings and blocks new P0/P1 | Unit test | PASS |
| Missing baseline classification | Without fingerprints findings are marked `new` | Unit test | PASS |
| JSON/Markdown evidence | Output records gate status and baseline-status summary | Unit test plus tmp outputs | PASS |
| L0 integration | `l0` runs legacy scan and standards-aware scanner | targeted `nox -s l0` | PASS |
| Changed-files nox | `guardrail_changed_files` validates staged current-task files only | staged-only nox run | PASS |

## Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m compileall scripts/aistock_guardrail_scan.py noxfile.py backend/tests/test_aistock_guardrail_scan.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/test_aistock_guardrail_scan.py -q -p no:cacheprovider
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/aistock_guardrail_scan.py scripts/aistock_guardrail_scan.py backend/tests/test_aistock_guardrail_scan.py noxfile.py tests/aistock_validation/modules/development_guardrails.md --baseline-json tmp/validation/guardrails/baseline_20260504.json --fail-new-only --fail-on-severity P1 --output-json tmp/validation/guardrails/development_guardrails_paths.json --summary-md tmp/validation/guardrails/development_guardrails_paths.md
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- scripts/aistock_guardrail_scan.py backend/tests/test_aistock_guardrail_scan.py noxfile.py tests/aistock_validation/modules/development_guardrails.md
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s guardrail_changed_files
```

## Evidence

- Run metadata: `tests/aistock_validation/history/development_guardrails/20260504_l0_development-guardrail-changed-files-validation.json`
- Guardrail scan JSON: `tests/aistock_validation/history/development_guardrails/20260504_l0_development-guardrail-changed-files-scan.json`
- Evidence manifest: `tests/aistock_validation/history/development_guardrails/20260504_l0_development-guardrail-changed-files-evidence.json`
- Module matrix: `tests/aistock_validation/modules/development_guardrails.md`
- Targeted pytest: `12 passed`.
- Targeted L0: passed; one `P1 baseline` finding in `noxfile.py` stayed visible but did not block.
- Staged guardrail nox: passed; `files_scanned=8`, `blocking_count=0`, `by_baseline_status.baseline=1`.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Standards-aware path gate initially failed on legacy selected paths | Existing baseline P0/P1 findings in `completion_contract.py` and `noxfile.py` are historical debt | Added baseline fingerprint classification and `--fail-new-only` gate semantics | Path scan with baseline passed |
| Local changed-only mode would include unrelated dirty files from other active windows | Current repository has concurrent work by other tools/windows | Added default `--staged-only` nox mode so Codex validates only files staged for its commit | `guardrail_changed_files` passed on staged files |

## Result

- Final status: PASS.
- Production impact: no production restart, no API touch, no DB writes, no business asset writes.
- Business outcome: new P0/P1 development-standard violations can now be blocked in the local automated pipeline without treating all historical baseline debt as current-task failure.
- Residual risks:
  - Full `--changed-only` should be reserved for single-task workspaces.
  - Baseline JSON remains local under `tmp`; if missing, the nox gate requires regenerating it before use.
  - Legacy baseline burn-down still needs separate module-by-module remediation tasks.
