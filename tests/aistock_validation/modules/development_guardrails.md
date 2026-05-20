# Development Guardrails Validation Matrix

This matrix covers the development-standard guardrail gate that protects new AIstock code while keeping legacy debt in a reviewed baseline.

## Scope

- Standards authority remains `docs/standards/aistock_development_standard_v1.3_20260520.md` and its same-version YAML catalog.
- The scanner entry point is `scripts/aistock_guardrail_scan.py`.
- The first machine baseline is local `tmp/validation/guardrails/baseline_20260504.json`; the human summary is `docs/analysis/aistock_guardrail_baseline_20260504.md`.
- Historical findings are not silently ignored: they are classified as `baseline` and stay visible in JSON/Markdown output.
- New staged or changed P0/P1 findings must block the gate when `--fail-new-only --fail-on-severity P1` is used.

## Nox Entry Points

```powershell
python -m nox -s l0 -- <changed files>
python -m nox -s guardrail_changed_files
python -m nox -s guardrail_changed_files -- --changed-only
```

## L0 Path Gate Contract

- `l0` still runs the existing Codex skill validator and legacy lightweight scan.
- `l0` also runs `scripts/aistock_guardrail_scan.py` on the same path set.
- `l0` requires `tmp/validation/guardrails/baseline_20260504.json` or the path in `AISTOCK_GUARDRAIL_BASELINE_JSON`.
- `l0` writes `tmp/validation/guardrails/l0_paths.json` and `tmp/validation/guardrails/l0_paths.md`.
- `l0` fails on new P0/P1 findings but does not fail on matched historical baseline findings.

## Changed/Staged Gate Contract

- `guardrail_changed_files` defaults to `--staged-only` so Codex can validate only the files it is about to commit in a multi-window dirty workspace.
- `guardrail_changed_files -- --changed-only` scans all Git changed and untracked files; use it only when the workspace belongs to one active task.
- The gate writes `tmp/validation/guardrails/changed_files.json` and `tmp/validation/guardrails/changed_files.md`.
- The output schema is `aistock_guardrail_scan_result_v1` and includes `baseline`, `gate`, `summary.by_baseline_status`, and finding-level `baseline_status`.

## Required Tests

- Catalog loads and enabled regex/path rules compile.
- Human-readable standard and YAML catalog remain synchronized by rule id/reference.
- DESIGN-COMPLIANCE-001 exists as a P0 manual review control, and completion reports include the required design acceptance matrix.
- Silent fallback, root pollution, and DataFrame concat-in-loop patterns are detected.
- Test paths and `debug_tools` one-off script locations are excluded where required.
- Git changed/staged file discovery uses UTF-8 with replacement for Unicode paths.
- Baseline fingerprints classify findings as `baseline`; missing baseline classifies findings as `new`.
- `blocking_findings(..., fail_new_only=True)` ignores baseline findings and blocks new P0/P1 findings.
- JSON/Markdown output records gate status and baseline-status summary.

## Production Isolation

- This module never starts backend/frontend services.
- It never writes business DB schemas or trading/QE assets.
- It only reads repository files and writes local validation outputs under `tmp/validation/guardrails`.
