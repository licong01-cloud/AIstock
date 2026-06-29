# Multi-Alpha PR Merge And Production DDL Apply Record - 2026-06-29

## Scope And Gates

- Scope: aftercare for multi-alpha PRs #1704/#1705/#1709/#1701/#1722 and the user-authorized production DDL gate.
- production_ddl_gate=applied_and_verified.
- production_frontend_dependency_gate=noop.
- production_backend_dependency_gate=noop.
- Runtime boundary: no backend, frontend, TDX, or worker service was started, stopped, or restarted.
- DB write boundary: only the three authorized forward migration files were executed against production.
- Rollback boundary: rollback files were prepared/validated in scratch only; no production rollback was executed.
- DML boundary: `qe_archive_multi_alpha_phase_a_20260628.sql` contains one `UPDATE`, but the production guard query returned `0` before execution and `0` after execution, so no business rows were rewritten.

## PR Merge And Cleanup Record

| PR | Status | Merge Commit | Notes |
|---|---:|---|---|
| #1704 LocalSim paper admission | merged | `c06a428fd4b64db310970fa19b8ae8224746568c` | already on `main`; production DDL was pending before this gate |
| #1705 three-page disposition | merged | `71636d45b24003fbc9ab34d9683890cf6651d3f2` | already on `main`; no DDL |
| #1709 warehouse archive Phase A | merged | `6a6dcde545ae79d3fb0f5b0631c13f0b158c1270` | already on `main`; production DDL was pending before this gate |
| #1701 gap design doc | merged | `d3960643310c596f48d0a55d6c1dd9f7f6e73a2a` | merged before #1722 |
| #1722 S1 unified export | merged | `70c27b8c1c347572984ec6bfdf93da8caa8f53b0` | merged after adding the rollback migration |

- #1722 rollback added before merge: `backend/migrations/strategy_pkg_multi_alpha_combine_source_type_20260629.rollback.sql`, PR branch commit `74e3a958`.
- Pre-merge CI:
  - #1701: PR Quality, CodeQL, Issue Auto Link, Semgrep, static/docs checks all `SUCCESS`; docs-lane backend jobs skipped as expected.
  - #1722: PR Quality, CodeQL, Semgrep, Static gate, and backend sessions including `paper_v2_backend`, `qe_archive_backend`, `model_registry_backend`, `market_regime_label`, `rl_execution_smoke`, `validation_center_backend`, `qe_data_contract_backend` all `SUCCESS`.
- Root checkout sync: `F:\Dev\AIstock` was on `main`, HEAD `70c27b8c1c347572984ec6bfdf93da8caa8f53b0`, clean before the record files were first written.
- Branch/worktree cleanup performed for the requested PR targets:
  - removed `F:\Dev\AIstock_worktrees\qe-paper-chain-closure-gap-20260628`.
  - removed `F:\Dev\AIstock_worktrees\multi-alpha-unified-export-parity-S1`.
  - removed `F:\Dev\AIstock_worktrees\multi-alpha-warehouse-archive-phase-a-20260628`.
  - deleted/pruned remote branches for #1701/#1722/#1709; #1704/#1705 remote branches were already absent.
- `gh pr view` returned `closingIssuesReferences=[]` for #1701/#1722/#1704/#1705/#1709, so there was no linked GitHub issue close-sync target to mutate.
- One older unrelated worktree remained untouched because it was outside the requested cleanup target set: `F:\Dev\AIstock_worktrees\multi-alpha-unified-export-parity-s1-20260629`.

## Scratch Dry Run Evidence

### Initial Harness Failure

- First scratch harness attempt stopped before any DDL was applied.
- reason_code: `SCRATCH_CREATE_DATABASE_TRANSACTION_BLOCK`.
- error: `CREATE DATABASE cannot run inside a transaction block`.
- correction: admin connection changed to `psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT`.
- production impact: none; no scratch DB or migration object was created by the failed attempt.

### Scratch PASS

- scratch DB: `aistock_codex_multi_alpha_ddl_20260629122032`.
- dev target: `127.0.0.1:5433` using `TDX_DB_DEV_*`.
- production business rows were not copied; only required structures were reconstructed from production metadata.
- base objects used:
  - `strategy_pkg.package`: 29 columns, 7 constraints.
  - `strategy_pkg.multi_alpha_combine_backtest_run`: 12 columns, 5 constraints.
  - `qe_archive.run`: 32 columns, 3 constraints.

| Scratch Step | Result | Elapsed |
|---|---:|---:|
| forward:paper_admission | PASS | 30.60 ms |
| forward:qe_archive_phase_a | PASS | 115.24 ms |
| forward:combine_source_type | PASS | 5.70 ms |
| forward_idempotent:paper_admission | PASS | 4.27 ms |
| forward_idempotent:qe_archive_phase_a | PASS | 8.92 ms |
| forward_idempotent:combine_source_type | PASS | 3.27 ms |
| rollback:combine_source_type | PASS | 4.02 ms |
| rollback:qe_archive_phase_a | PASS | 571.88 ms |
| rollback:paper_admission | PASS | 11.92 ms |
| forward_final:paper_admission | PASS | 25.13 ms |
| forward_final:qe_archive_phase_a | PASS | 109.85 ms |
| forward_final:combine_source_type | PASS | 3.99 ms |

Scratch verification:

- After first forward: `strategy_pkg.multi_alpha_paper_admission` exists; `idx_multi_alpha_paper_admission_lookup` partial index exists; all five `qe_archive.multi_alpha_*` tables exist; `ck_qear_run_status` and `package_source_type_check` include the new values.
- After idempotent rerun: all forward verifications still passed; rerun did not error.
- After rollback: paper admission table absent; five QE multi-alpha tables absent; `package_source_type_check` reverted; `ck_qear_run_status` no longer included `partial_failed`.
- After final forward: all forward verifications passed again.
- Cleanup: `scratch_dropped=true`.

## Production Read-Only Precheck

Connection source: `.env` `TDX_DB_*`. Secrets were not printed.

| Check | Result |
|---|---|
| database | `aistock` |
| server_addr | `172.17.0.3/32` |
| server_port | `5432` |
| db_user | `postgres` |
| `qe_archive.run` | exists |
| `strategy_pkg.package` | exists |
| `strategy_pkg.multi_alpha_combine_backtest_run` | exists |
| `strategy_pkg.multi_alpha_paper_admission` | already existed before the idempotent migration |
| `ck_qear_run_status` | already included `partial_failed` |
| `ck_macb_run_status` | already included `partial_failed` |
| `package_source_type_check` | old three values only before migration 3 |
| `qe_archive_phase_a_update_would_affect` | `0` |
| `strategy_pkg.package alpha_mode='multi_alpha'` | `0` |

Precheck `package_source_type_check` before migration 3:

```sql
CHECK ((source_type = ANY (ARRAY['qe_experiment'::text, 'qe_evolution_loop'::text, 'candidate_strategy_package'::text])))
```

## Production DDL Apply Evidence

Runner notes:

- Exact migration files committed in `main` were executed.
- Migration 1 and 3 were wrapped by the runner in explicit transactions.
- Migration 2 contains its own `BEGIN`/`COMMIT` and was executed as committed.
- Each migration was verified immediately before moving to the next one.

### 1. `strategy_pkg_multi_alpha_paper_admission_20260628.sql`

- file sha256: `f0c515bd412e9fb23cb3ac5da0f730e2acb0a204f74bc438404a302a79329ecb`.
- elapsed: `9.31 ms`.
- PostgreSQL notices:

```text
NOTICE: relation "multi_alpha_paper_admission" already exists, skipping
NOTICE: relation "idx_multi_alpha_paper_admission_lookup" already exists, skipping
```

Verification result:

| Object | Result |
|---|---|
| `strategy_pkg.multi_alpha_paper_admission` | exists |
| `multi_alpha_paper_admission_unique` | `UNIQUE (package_id, manifest_sha256, broker_backend, runtime_variant)` |
| `multi_alpha_paper_admission_broker_backend_chk` | `CHECK ((broker_backend = ANY (ARRAY['local_sim'::text, 'minqmt_sim'::text])))` |
| `idx_multi_alpha_paper_admission_lookup` | `CREATE INDEX ... WHERE (eligible = true)` |

Status: PASS.

### 2. `qe_archive_multi_alpha_phase_a_20260628.sql`

- file sha256: `8ddc96bbafb1b8966d76fa8806fded205d6d60ad096fdb0476db918da5ac1ae3`.
- elapsed: `25.09 ms`.
- DML guard before execution:

```sql
SELECT count(*)
FROM strategy_pkg.multi_alpha_combine_backtest_run
WHERE status='failed'
  AND reason->>'logical_status'='partial_failed';
-- result: 0
```

- DML guard after execution: `remaining_update_candidates=0`.
- PostgreSQL notices:

```text
NOTICE: schema "qe_archive" already exists, skipping
NOTICE: relation "idx_qear_run_type_status" already exists, skipping
NOTICE: relation "multi_alpha_run" already exists, skipping
NOTICE: relation "idx_qear_macb_run_roster" already exists, skipping
NOTICE: relation "idx_qear_macb_run_status" already exists, skipping
NOTICE: relation "multi_alpha_leg" already exists, skipping
NOTICE: relation "idx_qear_macb_leg_factor_hash" already exists, skipping
NOTICE: relation "multi_alpha_leg_source" already exists, skipping
NOTICE: relation "idx_qear_macb_leg_source_exp_loop" already exists, skipping
NOTICE: relation "idx_qear_macb_leg_source_seed" already exists, skipping
NOTICE: relation "multi_alpha_scheme" already exists, skipping
NOTICE: relation "idx_qear_macb_scheme_best" already exists, skipping
NOTICE: relation "multi_alpha_loo" already exists, skipping
NOTICE: relation "idx_qear_macb_loo_leg" already exists, skipping
```

Verification result:

| Object | Result |
|---|---|
| `qe_archive.multi_alpha_run` | exists |
| `qe_archive.multi_alpha_leg` | exists |
| `qe_archive.multi_alpha_leg_source` | exists |
| `qe_archive.multi_alpha_scheme` | exists |
| `qe_archive.multi_alpha_loo` | exists |
| `ck_qear_run_status` | includes `partial_failed` |
| `ck_macb_run_status` | includes `partial_failed`; comment preserved |

`ck_qear_run_status` after migration:

```sql
CHECK ((status = ANY (ARRAY['pending'::text, 'running'::text, 'completed'::text, 'failed'::text, 'interrupted'::text, 'partial_archived'::text, 'archived'::text, 'succeeded'::text, 'partial_failed'::text])))
```

Status: PASS.

### 3. `strategy_pkg_multi_alpha_combine_source_type_20260629.sql`

- file sha256: `b9cb2d7e50832ffca5a19be8ff1287d9ae3c076837c53629711fbe81f65ce69d`.
- elapsed: `7.11 ms`.
- PostgreSQL notices:

```text
NOTICE: schema "strategy_pkg" already exists, skipping
```

Verification result:

| Object | Result |
|---|---|
| `package_source_type_check` | includes `multi_alpha_combine_run` |
| constraint comment | `Allowed StrategyPackage manifest source types, including multi_alpha_combine_run for one-step Multi-Alpha combine export lineage.` |

`package_source_type_check` after migration:

```sql
CHECK ((source_type = ANY (ARRAY['qe_experiment'::text, 'qe_evolution_loop'::text, 'candidate_strategy_package'::text, 'multi_alpha_combine_run'::text])))
```

Status: PASS.

## Production Postcheck Summary

- `paper_admission.pass=true`.
- `qe_archive_phase_a.pass=true`.
- `combine_source_type.pass=true`.
- `qe_archive_phase_a_update_would_affect_after=0`.
- production_ddl_gate=applied_and_verified.
- no production rollback was executed.
- no service was started, stopped, or restarted.

Committed raw output:

```text
tests/aistock_validation/history/production_ddl/20260629_multi_alpha_ddl_apply_production_output.json
```

## User-Owned Restart Required

The merged backend code still needs a user-owned backend restart before the new endpoint/service wiring is live. This run intentionally did not restart or start any service.

After the user restarts backend, verify:

1. Multi-Alpha `POST /strategy-packages/{id}/paper-runtime-dry-run` is reachable and returns a non-silent success/error payload.
2. QE archive outbox handler registration is live; worker `SUPPORTED_WORKER_EVENT_TYPES` includes the multi-alpha combine event type.
3. Combine detail page `导出策略包` calls `from-multi-alpha-combine-run`, and a valid combine run returns HTTP 200.
