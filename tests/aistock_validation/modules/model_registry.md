# Model Registry Validation Matrix

Scope: QE governance Phase 5 model library foundation. This module covers the new `model_registry` schema, model Template/Spec/Trial/Artifact layering, lifecycle audit events, and compatibility with legacy `aistock_model_catalog`.

## Invariants

- New model library tables live under the independent `model_registry` schema, never `public`.
- Every new table and column has PostgreSQL `COMMENT ON TABLE` / `COMMENT ON COLUMN` metadata.
- The default QE selector hides `quarantined`, `training_failed`, `retired`, and deprecated-template specs.
- Paper v2 does not directly select model registry rows; Paper selects StrategyPackage records with promoted artifacts.
- Lifecycle changes append `model_lifecycle_event` rows and must not silently delete model records.
- Legacy `aistock_model_catalog` remains read-compatible and is not rewritten by Phase 5 foundation code.
- Missing referenced templates/specs/trials and invalid transitions fail fast.
- Write APIs are disabled by default and require explicit dev-only `AISTOCK_MODEL_REGISTRY_WRITE_API_ENABLED=true`.
- Migration smoke defaults to static dry-run and must not connect to DB unless an explicit dev DB transaction/apply mode is requested.
- DB transaction smoke must roll back by default, require a confirmation token, and refuse production-like targets unless a second rollback-only override is explicitly present.
- Rollback SQL is destructive and must fail fast unless the operator sets the session confirmation token in a dev transaction.
- Model registry catalog compatibility and legacy bridge APIs are read-only and must not require enabling write APIs.
- Legacy bridge rows preserve `paper_selectable=false`; Paper v2 must continue to select StrategyPackage records, not raw model rows.

## Gates

| Gate | Command / evidence | Expected result |
| --- | --- | --- |
| MODEL-REG-L0-001 schema namespace | `pytest backend/tests/model_registry/test_model_registry_phase5.py -q -p no:cacheprovider` | `model_registry.*` tables and views exist in migration text; no public tables. |
| MODEL-REG-L0-002 DDL comments | same test file | Every new table/column is covered by comments. |
| MODEL-REG-L1-001 QE selector visibility | same test file | Hidden lifecycle statuses are excluded from default QE selectable specs. |
| MODEL-REG-L1-002 four-layer service | same test file | Template, Spec, Trial, and Artifact records can be represented without touching protected assets. |
| MODEL-REG-L1-003 lifecycle audit | same test file | Status transition writes an append-only lifecycle event and does not delete. |
| MODEL-REG-L1-004 fail-fast semantics | same test file | Missing object, empty reason, or invalid target status raises explicit domain errors. |
| MODEL-REG-L2-001 guarded API | same test file | Write routes are present but return disabled-by-default guard unless the dev env flag is set. |
| MODEL-REG-L2-002 API compile/import | `python -m py_compile backend/services/model_registry/registry.py backend/routers/model_registry.py backend/routers/quantevolver.py` | Service/router compile. |
| MODEL-REG-L2-003 integration subset | `pytest backend/tests -q -p no:cacheprovider -k "model_registry or seed_contract or promotion_review"` | Governance subset passes. |
| MODEL-REG-L2-004 migration static smoke | `python scripts/model_registry_migration_smoke.py --json` | Static dry-run validates schema objects, comments, indexes, views, and guarded rollback plan without DB connection. |
| MODEL-REG-L2-005 migration guard tests | `pytest backend/tests/model_registry/test_model_registry_migration_smoke.py -q -p no:cacheprovider` | Default mode is no-write/no-DB; DB transaction and apply modes require tokens/env guards and refuse production-like targets. |
| MODEL-REG-L3-001 dev DB rollback smoke | `AISTOCK_MODEL_REGISTRY_MIGRATION_DEV_DB=true python scripts/model_registry_migration_smoke.py --db-transaction-check --confirm-db-check MODEL_REGISTRY_PHASE5_DEV_ROLLBACK_CHECK --db-name <dev_db>` | Optional dev-only check executes migration in one transaction and rolls back. Never run against production DB. |
| MODEL-REG-L2-006 bridge read API | `pytest backend/tests/model_registry/test_model_registry_phase5.py -q -p no:cacheprovider` | `/model-registry/catalog-compat` and `/model-registry/legacy-catalog-bridge` are read-only, use model_registry views, and do not require the write API guard. |

## Out Of Scope For Phase 5 Foundation

- Executing production DB migration.
- Running destructive rollback outside an explicit dev transaction.
- Copying or modifying model weights, HMM snapshots, QE/RD-Agent artifacts, or StrategyPackage frozen manifests.
- Changing legacy `aistock_model_catalog` delete behavior in existing QuantEvolver endpoints.
- Paper v2 eligibility changes; those remain StrategyPackage-gated.
