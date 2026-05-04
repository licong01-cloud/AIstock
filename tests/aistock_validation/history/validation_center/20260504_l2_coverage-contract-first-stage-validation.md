# Coverage Contract First Stage Complete Loop

- Module: validation_center
- Level: L2
- Date: 2026-05-04
- Git commit: pre-commit working tree based on current `main`
- Operator: Codex

## Scope

- Changed files:
  - `scripts/aistock_validate.py`
  - `backend/tests/test_aistock_validate_metadata.py`
  - `backend/tests/test_aistock_validate_coverage.py`
  - `noxfile.py`
  - `requirements-dev.txt`
  - `tests/aistock_validation/modules/qe_data_completeness.md`
  - `tests/aistock_validation/modules/validation_center.md`
- Impacted flows:
  - validation run metadata coverage placeholder
  - coverage XML/JSON parsing
  - coverage threshold gates
  - diff coverage from unified patch
  - nox validation coverage entry point
- Business goal: make coverage evidence machine-readable and gateable before Validation Center UI/API work.
- Out of scope: Validation Center UI, DB persistence, production hook, QE realtime archive hook, backend/frontend service restart.
- Protected assets reviewed: no StrategyPackage, QE/RD-Agent workspace, model weights, HMM snapshots, paper ledger, or production configs modified.

## Environment

- Backend port: not used
- Frontend port: not used
- TDX port: not used
- Conda/env: `C:/Users/lc999/miniconda3/envs/AIstock/python.exe`
- Database: not used
- Browser/headless: not used

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L1 metadata compatibility | `record` keeps coverage placeholder with schema and `not_collected` status | `backend/tests/test_aistock_validate_metadata.py` | PASS |
| L1 XML parser | Cobertura XML line/branch totals and missing lines are parsed | `backend/tests/test_aistock_validate_coverage.py` | PASS |
| L1 JSON parser | Coverage.py JSON line/branch totals are parsed | `backend/tests/test_aistock_validate_coverage.py` | PASS |
| L1 gate failure | Threshold failure returns nonzero and still writes JSON evidence | `backend/tests/test_aistock_validate_coverage.py` | PASS |
| L1 baseline mode | `--no-fail` records failed gate while returning zero | `backend/tests/test_aistock_validate_coverage.py` | PASS |
| L2 diff gate | Unified patch changed executable lines are checked against coverage | `backend/tests/test_aistock_validate_coverage.py` | PASS |
| L2 missing coverage file | Changed files absent from coverage fail explicitly | `backend/tests/test_aistock_validate_coverage.py` | PASS |
| L2 nox coverage loop | pytest-cov writes XML/JSON, `aistock_validate.py coverage` gates line/branch thresholds | `tmp/validation/coverage/validation_coverage_backend_snapshot.json` | PASS |
| L0 guardrails | Changed files pass existing L0 guardrail scan | nox L0 output | PASS |

## Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m compileall scripts/aistock_validate.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/test_aistock_validate_metadata.py backend/tests/test_aistock_validate_coverage.py -q -p no:cacheprovider
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_coverage_backend
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_data_contract_backend
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- scripts/aistock_validate.py backend/tests/test_aistock_validate_metadata.py backend/tests/test_aistock_validate_coverage.py noxfile.py requirements-dev.txt tests/aistock_validation/modules/qe_data_completeness.md tests/aistock_validation/modules/validation_center.md tests/aistock_validation/history/validation_center/20260504_l2_coverage-contract-first-stage-validation.md tests/aistock_validation/history/validation_center/20260504_l2_coverage-contract-first-stage-evidence.json tests/aistock_validation/history/validation_center/20260504_l2_coverage-contract-first-stage-snapshot.json
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/aistock_guardrail_scan.py --fail-on-severity P0 scripts/aistock_validate.py backend/tests/test_aistock_validate_metadata.py backend/tests/test_aistock_validate_coverage.py noxfile.py requirements-dev.txt tests/aistock_validation/modules/qe_data_completeness.md tests/aistock_validation/modules/validation_center.md tests/aistock_validation/history/validation_center/20260504_l2_coverage-contract-first-stage-validation.md tests/aistock_validation/history/validation_center/20260504_l2_coverage-contract-first-stage-evidence.json tests/aistock_validation/history/validation_center/20260504_l2_coverage-contract-first-stage-snapshot.json
```

## Evidence

- Coverage snapshot: `tests/aistock_validation/history/validation_center/20260504_l2_coverage-contract-first-stage-snapshot.json`
- Evidence manifest: `tests/aistock_validation/history/validation_center/20260504_l2_coverage-contract-first-stage-evidence.json`
- Generated local coverage reports: `tmp/validation/coverage/validation_coverage_backend.xml`, `tmp/validation/coverage/validation_coverage_backend.json`
- Coverage gate result: line `81.57`, branch `68.55`, status `passed`
- Targeted pytest: `10 passed`
- QE data contract nox: `17 passed`
- L0 nox: successful, 0 high findings
- `aistock_guardrail_scan --fail-on-severity P0`: completed with no P0 findings; known non-blocking finding was the existing `noxfile.py` root-location false positive.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| `validation_coverage_backend` initially did not recognize `--cov` | nox session invoked `python` from PATH instead of the conda interpreter that had `pytest-cov` installed | Use `sys.executable` for this coverage session | `validation_coverage_backend` rerun passed |
| `aistock_guardrail_scan` flagged `except Exception` in new coverage path normalization | broad exception could look like silent fallback | Narrowed to `(OSError, RuntimeError, ValueError)` | P0 guardrail rerun passed |

## Result

- Final status: PASS
- Remaining risks:
  - `pytest-cov` / `coverage` were added to `requirements-dev.txt` and installed in the local AIstock conda environment for validation.
  - `aistock_guardrail_scan --fail-on-severity P1` still has a known historical false positive for `noxfile.py`; this should be handled during v1.2 guardrail calibration, not by moving `noxfile.py`.
- Need production backend restart: no
- Need dev service restart: no
