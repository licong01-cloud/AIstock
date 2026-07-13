# StrategyPackage manifest_sha256 integrity repair validation

- Date: 2026-06-30
- Branch: `fix/strategy-package-manifest-sha-integrity-20260630`
- Worktree: `F:\Dev\AIstock_worktrees\strategy-package-manifest-sha-integrity-20260630`
- Design: `docs/architecture/strategy_package_manifest_sha_integrity_repair_design_20260630.md`
- Scope: StrategyPackage manifest hash drift classification, safe repair gate, helper script, production runbook.
- Production runtime: not started/restarted.
- Production DDL: not applicable, `production_ddl_gate=noop`.
- Production DML: not executed; `production_dml_gate=pending_user_authorization`.

## Production Read-only Evidence

Command:

```powershell
rtk python scripts/strategy_package_manifest_hash_repair.py --env-file F:\Dev\AIstock\.env --target-db prod --limit 500 --output debug_tools/strategy_package/20260630_manifest_sha_integrity/prod_manifest_hash_repair_dry_run.json
```

Result:

- `total_scanned=15`
- `clean_count=5`
- `drifted_count=10`
- `filtered_drifted_count=10`
- `repairable_count=10`
- `blocked_count=0`
- All 10 drifted packages classified as `A_schema_evolution_stale_hash`.
- No production write was performed; script ran in dry-run/read-only mode.

## Scratch / Dev DB Evidence

Target: `TDX_DB_DEV_*`, local `127.0.0.1:5433/aistock_dev`, no production DB writes.

### B-class guard

Setup inserted one A-class scratch package and one B-class scratch package under prefix `scratch_manifest_sha_20260630_`.

Dry-run command:

```powershell
rtk python scripts/strategy_package_manifest_hash_repair.py --env-file F:\Dev\AIstock\.env --target-db dev --limit 500 --package-id-prefix scratch_manifest_sha_20260630_ --output debug_tools/strategy_package/20260630_manifest_sha_integrity/scratch_manifest_hash_repair_dry_run_with_blocked.json
```

Result:

- `filtered_drifted_count=2`
- `repairable_count=1`
- `blocked_count=1`
- Blocked package classification: `B_manifest_json_dirty_or_unknown`.

Apply command intentionally tried the mixed prefix and was blocked:

```powershell
rtk python scripts/strategy_package_manifest_hash_repair.py --env-file F:\Dev\AIstock\.env --target-db dev --limit 500 --apply --confirm-scratch-dml --package-id-prefix scratch_manifest_sha_20260630_ --operator scratch_test --output debug_tools/strategy_package/20260630_manifest_sha_integrity/scratch_manifest_hash_repair_apply_blocked.json
```

Result: exit 1, error contained `non-repairable drift exists`; no partial silent repair.

### Production-10 replay

Setup inserted 10 A-class scratch rows under prefix `scratch_manifest_sha_20260630_prod10_`, one for each production drift class.

Dry-run command:

```powershell
rtk python scripts/strategy_package_manifest_hash_repair.py --env-file F:\Dev\AIstock\.env --target-db dev --limit 500 --package-id-prefix scratch_manifest_sha_20260630_prod10_ --output debug_tools/strategy_package/20260630_manifest_sha_integrity/scratch_prod10_manifest_hash_repair_dry_run_before.json
```

Result:

- `filtered_drifted_count=10`
- `repairable_count=10`
- `blocked_count=0`

Apply command:

```powershell
rtk python scripts/strategy_package_manifest_hash_repair.py --env-file F:\Dev\AIstock\.env --target-db dev --limit 500 --apply --confirm-scratch-dml --package-id-prefix scratch_manifest_sha_20260630_prod10_ --operator scratch_prod10_test --output debug_tools/strategy_package/20260630_manifest_sha_integrity/scratch_prod10_manifest_hash_repair_apply.json
```

Result:

- `repaired_count=10`
- `after_filtered_drifted_count=0`
- `after_repairable_count=0`
- `after_blocked_count=0`

Idempotency command:

```powershell
rtk python scripts/strategy_package_manifest_hash_repair.py --env-file F:\Dev\AIstock\.env --target-db dev --limit 500 --apply --confirm-scratch-dml --package-id-prefix scratch_manifest_sha_20260630_prod10_ --operator scratch_prod10_test --output debug_tools/strategy_package/20260630_manifest_sha_integrity/scratch_prod10_manifest_hash_repair_apply_idempotent.json
```

Result:

- `repaired_count=0`
- `after_filtered_drifted_count=0`
- `after_repairable_count=0`
- `after_blocked_count=0`

## Automated Checks

- `rtk python scripts/aistock_feature_workflow.py validate --design docs/architecture/strategy_package_manifest_sha_integrity_repair_design_20260630.md --tier F2` -> PASS, `design_items=12`, `matrix_rows=12`.
- `rtk python -m pytest backend/tests/strategy_package/test_repository_service.py::test_validate_manifest_integrity_classifies_safe_schema_evolution_drift backend/tests/strategy_package/test_repository_service.py::test_validate_manifest_integrity_blocks_dirty_manifest_json_repair backend/tests/strategy_package/test_repository_service.py::test_validate_manifest_integrity_blocks_invalid_manifest_json_repair backend/tests/strategy_package/test_repository_service.py::test_repair_manifest_hash_fixes_a_class_drift backend/tests/strategy_package/test_repository_service.py::test_repair_manifest_hash_requires_explicit_confirmation backend/tests/strategy_package/test_manifest_integrity_router.py backend/tests/scripts/test_strategy_package_manifest_hash_repair.py -q` -> `19 passed`.


Additional final gates:

- `rtk python -m compileall -q backend/services/strategy_package backend/routers scripts` -> passed; existing SyntaxWarning only in unrelated legacy scripts `alter_aistock_loop_catalog_unique.py` and `import_minutes_to_db.py`.
- `rtk python -m pytest backend/tests/strategy_package -q` -> `250 passed`.
- `rtk python -m pytest backend/tests/scripts/test_strategy_package_manifest_hash_repair.py -q` -> `11 passed`.
- `rtk python -m ruff check backend/services/strategy_package/manifest.py backend/services/strategy_package/repository.py scripts/strategy_package_manifest_hash_repair.py backend/tests/strategy_package/test_manifest_integrity_router.py backend/tests/strategy_package/test_repository_service.py backend/tests/scripts/test_strategy_package_manifest_hash_repair.py` -> passed.
- `rtk git diff --check` -> passed.
- Scratch cleanup removed prefix rows from dev DB after evidence capture: `scratch_manifest_sha_20260630_` deleted 12 package rows and 23 event rows; production DB untouched.

## Business Oracles

- A-class repair is allowed only when stored hash equals raw persisted `manifest_json` hash and embedded hash.
- B-class or invalid/unknown drift is reported with explicit classification and blocks apply.
- Repair writes only `strategy_pkg.package.manifest_sha256` and `package_status_event`; it does not mutate `manifest_json`.
- `current_manifest()` overlays DB `manifest_sha256`, so API payload after repair does not expose stale embedded manifest hash.
- Production repair remains pending user authorization; no production DML was executed in this validation run.
