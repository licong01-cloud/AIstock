# Model Registry Phase 5.1 Migration Smoke Validation - 2026-05-09

Task/branch: `codex/qe-phase-5-model-registry-migration-20260509`

Scope: Adds a guarded migration smoke helper and manual rollback SQL for the Phase 5 `model_registry` schema. The default gate is static dry-run only; DB execution is dev-only, explicit, and rolls back unless `--apply` is separately enabled for a non-production-like dev target.

## Safety

- Production 8001 touched: no.
- Production DB written: no.
- Dev DB written: no; only static dry-run was executed in this branch validation.
- Protected assets touched: no StrategyPackage frozen manifest, model weight, HMM snapshot, QE/RD-Agent artifact, Paper ledger, or validated policy modified.
- `AGENTS.md` modified: no.
- `main` merged: no.
- Rollback exposure: rollback SQL exists as a guarded manual plan and fails fast unless `SET LOCAL aistock.model_registry_rollback_confirm = 'DROP_MODEL_REGISTRY_PHASE5_DEV_ONLY'` is set inside the operator transaction.

## Validation Results

| Gate | Evidence | Status |
| --- | --- | --- |
| Static migration smoke | `python scripts/model_registry_migration_smoke.py --json` | Pass: validates expected schema, tables, indexes, views, comments, and guarded rollback plan without DB connection. |
| Compile migration smoke code | `python -m py_compile scripts/model_registry_migration_smoke.py backend/tests/model_registry/test_model_registry_migration_smoke.py` | Pass |
| Migration smoke tests | `pytest backend/tests/model_registry/test_model_registry_migration_smoke.py -q -p no:cacheprovider` | Pass: `10 passed` |
| Model registry test module | `pytest backend/tests/model_registry -q -p no:cacheprovider` | Pass: `23 passed` |
| Governance integration subset | `pytest backend/tests -q -p no:cacheprovider -k "model_registry or seed_contract or promotion_review"` | Pass: `43 passed, 915 deselected` |
| Guardrail changed-files scan | `python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1` | Pass: `findings=0, blocking=0` |
| Whitespace | `git diff --check` | Pass; CRLF warning only for existing validation markdown line-ending behavior |

## Residual Risks

- DB transaction smoke was intentionally not run because no dev DB target was explicitly authorized in this branch.
- Applying the migration remains a separate operator action and must target a dev/test/sandbox DB first.
- Rollback uses `DROP SCHEMA ... CASCADE`; it is documented for manual dev rollback only and must not be used against production.
