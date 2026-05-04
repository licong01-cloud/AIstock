# L1 Standards Legacy Inventory Tool Validation

- Date: 2026-05-04
- Module: development_standards
- Level: L1
- Scope: read-only legacy/dead-code inventory tool, unit tests, baseline Markdown evidence
- Production impact: none; no backend/frontend service restart; no remote API access

## Changed Scope

- Added `scripts/aistock_legacy_inventory.py` as a read-only tracked-file inventory tool.
- Added `backend/tests/test_aistock_legacy_inventory.py` unit tests.
- Generated `docs/analysis/aistock_legacy_inventory_baseline_20260504.md` as the human-readable baseline summary.
- Machine JSON output was written to `tmp/validation/legacy_inventory/aistock_legacy_inventory_20260504.json` and is not intended for Git commit.

## Business Oracles

- The inventory is advisory only and must not delete, move, or rename files.
- Protected paths such as `qe_archive/artifacts` are not emitted as cleanup candidates.
- Root Python files are high-risk review candidates, not safe-delete items.
- Referenced candidates are downgraded to low-confidence cleanup candidates.
- Historical documents and one-off-like scripts are classified for lifecycle review only.

## Commands

```powershell
python -m compileall scripts/aistock_legacy_inventory.py backend/tests/test_aistock_legacy_inventory.py
python -m pytest backend/tests/test_aistock_legacy_inventory.py -q -p no:cacheprovider
python scripts/aistock_legacy_inventory.py --output-json tmp/validation/legacy_inventory/aistock_legacy_inventory_20260504.json --summary-md docs/analysis/aistock_legacy_inventory_baseline_20260504.md --max-items-md 200
python scripts/aistock_guardrail_scan.py --fail-on-severity P1 scripts/aistock_legacy_inventory.py backend/tests/test_aistock_legacy_inventory.py docs/analysis/aistock_legacy_inventory_baseline_20260504.md docs/analysis/aistock_legacy_dead_code_prebaseline_20260504.md docs/architecture/aistock_development_standard_v1_2_calibration_plan_20260504.md
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- scripts/aistock_legacy_inventory.py backend/tests/test_aistock_legacy_inventory.py docs/analysis/aistock_legacy_inventory_baseline_20260504.md docs/analysis/aistock_legacy_dead_code_prebaseline_20260504.md docs/architecture/aistock_development_standard_v1_2_calibration_plan_20260504.md
```

## Results

- `compileall`: passed.
- `pytest`: 7 passed.
- Inventory baseline generation: passed; 2.63 seconds after text-index optimization.
- AIstock guardrail scan: 0 findings at P1 threshold.
- `nox -s l0`: passed with 0 findings.

## Evidence

- Human baseline: `docs/analysis/aistock_legacy_inventory_baseline_20260504.md`
- Local JSON baseline: `tmp/validation/legacy_inventory/aistock_legacy_inventory_20260504.json`

## Residual Risks

- The tool uses static text references only; dynamic imports, scheduler metadata, DB-held references, and frontend runtime usage still require future specialized checks.
- The current baseline is not a deletion list and must be reviewed by module before cleanup.
- The current workspace has unrelated dirty changes from other windows; this validation only covers the files listed above.
