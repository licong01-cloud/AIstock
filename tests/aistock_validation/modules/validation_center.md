# Validation Center / Coverage Contract Validation Matrix

This matrix covers the first-stage complete loop for AIstock validation-pipeline coverage contracts. The scope is coverage parsing, gate evaluation, evidence output, and run-record compatibility. It does not start backend/frontend services and does not touch production port `8001`.

## Production Isolation Rules

- Do not restart production backend `8001`.
- Do not restart remote APIs.
- Do not write to business schemas or modify protected trading/QE assets.
- Coverage parsing must use local report files and synthetic fixtures in tests.
- A failed coverage gate must be explicit in JSON evidence and must not be hidden by a silent fallback.

## L1/L2 Coverage Contract

Required coverage for the first-stage complete loop:

- `scripts/aistock_validate.py coverage` accepts exactly one source report: `--coverage-xml` or `--coverage-json`.
- Coverage snapshot JSON uses schema `aistock_validation_coverage_snapshot_v1`.
- Snapshot includes module, level, title, run id, git commit, operator, source report path, output path, totals, file-level details, diff coverage, quality gates, failed gates, and status.
- XML parser supports Coverage.py/Cobertura line hits and branch `condition-coverage` counts.
- JSON parser supports Coverage.py JSON files with executed/missing lines and summary branch totals.
- Thresholds are percent values in `[0, 100]`; invalid thresholds fail fast.
- `line`, `branch`, and `diff_line` gates fail when configured thresholds are not met.
- Branch thresholds fail when branch coverage is unavailable; they must not silently pass.
- Diff coverage can read a unified patch file or `git diff --unified=0` from a base ref.
- Diff coverage fails when changed files are missing from the coverage report.
- `--no-fail` can record a failed gate as evidence while returning zero for baseline-only runs.
- `record` metadata keeps a coverage placeholder with the same coverage snapshot schema and `status=not_collected`.
- `validation_coverage_backend` runs targeted pytest with `pytest-cov`, writes XML/JSON coverage reports under `tmp/validation/coverage/`, then gates the parser itself with 70% line and 55% branch thresholds.

## Nox Entry Points

```powershell
python -m nox -s validation_coverage_backend
python -m nox -s qe_data_contract_backend
python -m nox -s l0 -- scripts/aistock_validate.py backend/tests/test_aistock_validate_metadata.py backend/tests/test_aistock_validate_coverage.py noxfile.py tests/aistock_validation/modules/validation_center.md
```

## Evidence

Every implementation run should create a record under `tests/aistock_validation/history/validation_center/` with:

- Exact commands.
- Coverage snapshot sample path.
- Pytest and nox results.
- Guardrail result.
- Production impact statement.
- Bugs found, fixes, reruns, and residual risks.
