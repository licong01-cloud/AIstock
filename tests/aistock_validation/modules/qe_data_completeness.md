# QE Data Completeness Validation Matrix

This matrix covers the first development phase for QE experiment data completeness contracts and AIstock validation-pipeline metadata. It extends the existing `noxfile.py` / `scripts/aistock_validate.py` / run-record workflow instead of introducing a separate test framework.

## Production Isolation Rules

- Do not restart production backend `8001`.
- Do not restart remote worker APIs. WSL-local APIs may be restarted only in later phases when explicitly needed.
- Do not enable QE realtime archive hooks or scheduler hooks in this phase.
- Do not read WSL or remote QE/RD-Agent workspace files directly.
- Contract tests must use synthetic payloads only.

## L1/L2 Backend Coverage

Current required coverage:

- `scripts/aistock_validate.py record` writes the existing Markdown run record and a JSON metadata sidecar by default.
- `record --no-json` preserves legacy Markdown-only behavior.
- `record --history-root` allows tests to isolate generated files outside the repository history tree.
- `scripts/aistock_validate.py evidence` writes evidence manifests with kind, path, existence, size, directory marker, child count, and sha256 for files.
- Evidence manifest supports missing evidence reporting and `--fail-missing` fail-fast mode.
- `scripts/aistock_validate.py coverage` writes first-stage complete coverage snapshots with schema version, totals, file-level lines, diff coverage, threshold gates, failed gates, and explicit failure status.
- QE completion payload contract accepts complete synthetic payloads with config, metrics, position, holding, execution, cost, training source, factor summary, data quality, and artifact manifest sections.
- `collection_status=complete` fails validation when required sections are missing.
- Partial payloads can be parsed and report missing required fields without faking completeness.
- Artifact manifest rejects raw WSL/remote worker workspace paths and invalid sha256 digests.

## Nox Entry Points

```powershell
python -m nox -s qe_data_contract_backend
python -m nox -s validation_coverage_backend
python -m nox -s l0 -- scripts/aistock_validate.py backend/services/quantevolver/completion_contract.py backend/tests/test_aistock_validate_metadata.py backend/tests/test_aistock_validate_coverage.py backend/tests/unified_engine/test_qe_completion_contract.py noxfile.py docs/architecture/qe_data_completeness_phase1_development_plan_20260504.md tests/aistock_validation/modules/qe_data_completeness.md tests/aistock_validation/modules/validation_center.md
```

## Evidence

Every implementation run should create a record under `tests/aistock_validation/history/qe_data_completeness/` with:

- Exact commands.
- Whether production `8001` or remote worker APIs were touched.
- Pytest and nox results.
- Guardrail result.
- Evidence manifest path.
- Bugs found, fixes, reruns, and residual risks.
