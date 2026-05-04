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
python -m nox -s validation_center_backend
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


## Future Gap Requirements From Paper v2 Incidents

`docs/architecture/aistock_automation_test_coverage_gap_requirements_20260504.md` is a future-stage requirement input, not extra implementation scope for the current read-only API step. Validation Center contracts must still reserve these semantics now:

- `pass_scope` distinguishes L0/L1/L2/mock/fail-fast/current-commit/real-business proof.
- `business_assertion` records whether a user can complete a named operation and which UI/API/DB/log evidence proves it.
- Mock UI evidence cannot be displayed as real business success.
- Negative fail-fast evidence cannot replace a positive StrategyPackage/Selection/Paper v2 success path.
- Historical L3 evidence is reference only; high-risk modules must rerun relevant paths on the current commit.
- Future sample registry must include complete minute QE, historical QE with missing StaticDataLoader parquet, missing model params/factor source, large Paper v2 portfolio list, and HMM coefficient complete/missing samples.

## L2 Read-only API Contract

The first read-only API loop must expose validation history without executing commands, writing DB rows, or starting services:

- `GET /api/v1/validation/health` returns read-only storage status.
- `GET /api/v1/validation/plans` and `/plans/{plan_key}` read the allowlist catalog and reject unsafe command keys or production backend ports.
- `GET /api/v1/validation/runs` supports pagination plus module/level/status/search filters.
- `GET /api/v1/validation/runs/{run_id}` returns Markdown path/text, metadata, coverage/evidence links, and optional `pass_scope` / `business_assertion` if present.
- `GET /api/v1/validation/coverage` and `/coverage/{snapshot_id}` expose coverage snapshots with explicit missing/parse-error states.
- `GET /api/v1/validation/evidence` and `/evidence/{manifest_id}` expose evidence manifests with `missing_count`.
- `GET /api/v1/validation/summary` provides a lightweight module/status/coverage summary.
- Missing metadata, missing coverage, missing evidence, and malformed JSON must be explicit fields; the API must not fake success.
