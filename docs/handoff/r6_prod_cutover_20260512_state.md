# R6 Prod Cutover State 2026-05-12

**Time**: 2026-05-12 09:08 BJ (post 9:30 实盘窗口已过)
**Operator**: Strategy session (战略 session)
**Authorization**: User explicit (combined option 1+2 plan)

## Summary

R5+R6 schema fully applied to prod DB. `pkg_5a5c` promoted to PAPER_ENABLED with
**synthetic evidence** for 9:30 LocalSim sanity (代码层验证). Real evidence ETL
(Codex Task 9) in progress, ETA 13:00 BJ. After Codex delivers real ETL, run
`scripts/r6_cutover_synthetic_evidence_rollback.py --apply` to remove synthetic
rows BEFORE applying real backfill.

## Prod DB Changes

### Migrations Applied (idempotent, can re-run safely)

R5 (paper-v2 + market + qe_archive ext):
1. `backend/db/add_paper_v2_capture_fields_20260510.sql` ✓
2. `backend/db/add_paper_v2_portfolio_broker_backend_20260509.sql` ✓
3. `backend/db/add_paper_v2_run_model_params_origin_20260510.sql` ✓
4. `backend/db/init_market_regime_label_20260510.sql` ✓
5. `backend/db/init_qe_archive_paper_v2_extension_20260510.sql` ✓ (20 tables)
6. `backend/db/migrate_qe_archive_paper_v2_run_archive_complete_20260511.sql` ✓

R6 governance (6 migrations per runbook §6):
1. `backend/migrations/qe_phase4_master_seed_contract_20260509.sql` ✓
2. `backend/migrations/strategy_pkg_package_asset_20260509.sql` ✓
3. `backend/migrations/strategy_pkg_validation_run_20260509.sql` ✓
4. `backend/migrations/strategy_pkg_runtime_variant_20260509.sql` ✓
5. `backend/migrations/strategy_pkg_promotion_review_20260509.sql` ✓
6. `backend/migrations/model_registry_phase5_20260509.sql` ✓

### Synthetic Evidence Seeded (CAVEAT — must clean before real ETL)

For `pkg_5a5c` (qe_20260508_060509_1268):

| Table | Rows | Filter |
|---|---|---|
| `strategy_pkg.package_asset` | 2 (MODEL_WEIGHT + protected_asset_ledger_evidence) | `metadata->>'caveat'='synthetic_pre_real_etl'` |
| `strategy_pkg.package_runtime_variant` | 1 (risk_policy paper_candidate=true PASSED) | `created_by='strategy_session_9:30'` |
| `strategy_pkg.package_validation_run` | 3 (1 original_fixed_weight + 2 original_retrain seeds, all PASSED) | `created_by='strategy_session_9:30'` |
| `strategy_pkg.package_status_event` | 1 (BACKTEST_APPROVED→PAPER_ENABLED) | `reason='synthetic_evidence_9:30_sanity'` |
| `strategy_pkg.package` | 1 (pkg_5a5c PAPER_ENABLED) | row update + status_event audit |

### DR Snapshot (rollback target)

- Path: `E:/DEV backup/aistock_pg_snapshots/r6_pre_cutover_20260512_0830.dump`
- Format: pg_dump --format=custom
- Size: 220.4 MB
- Timestamp: 2026-05-12 08:30 BJ
- Verified: pg_dump exit 0, file size > threshold

## Reversibility

### Soft rollback (synthetic only, no DR restore)
```bash
python scripts/r6_cutover_synthetic_evidence_rollback.py --apply
```
This removes the 7 synthetic rows + reverts pkg_5a5c to BACKTEST_APPROVED.
Schema migrations remain (they are additive and required for R6).

### Hard rollback (DR restore - only if catastrophic)
```bash
docker exec -i timescaledb pg_restore -U postgres -d aistock -c \
  < "E:/DEV backup/aistock_pg_snapshots/r6_pre_cutover_20260512_0830.dump"
```
This restores pre-cutover state — no R5/R6 migrations, no synthetic data.

## Open Items

1. ⏳ **Codex Task 9** (drawer `eeef7f67`): `scripts/qe_to_evidence_bundle_etl.py`
   on `codex/qe-evidence-etl-20260512`. ETA 13:00 BJ.

2. ⏸️ **Sentinel sanity --mode=prod**: Optional verification of R6 code chain.
   Can be invoked AFTER backend restart. Does NOT depend on market open.

3. ⏸️ **daemon worker enable**: DEFERRED until real evidence ETL is applied.
   Running daemon with synthetic-evidence-PAPER_ENABLED would pollute audit
   trail with `STALE_INITIAL_BACKTEST_MODEL` runs.

4. ⏸️ **R7+ Sprint**: Automated retrain pipeline + regime metrics computer +
   UI/SOP for evidence preparation (currently manual).

## Risk Posture

| Risk | Mitigation |
|---|---|
| Synthetic data pollutes prod audit | Tagged with `caveat=synthetic_pre_real_etl`; rollback script ready |
| daemon runs with stale model | NOT enabling daemon until real ETL done |
| R5/R6 migration regression | DR snapshot taken pre-migration; can restore |
| Codex Task 9 fails to produce real evidence | Synthetic stays as 9:30 demo only; real evidence is R7 Sprint scope |
| Repo loses cutover artifacts | Scripts committed to main (this commit) |

## Files Added (this cutover)

- `scripts/r6_cutover_apply_r5_migrations.py` — R5/qe_archive migrations apply
- `scripts/r6_cutover_synthetic_evidence_pkg_5a5c.py` — synthetic evidence seed
- `scripts/r6_cutover_synthetic_evidence_rollback.py` — synthetic cleanup
- `docs/handoff/r6_prod_cutover_20260512_state.md` — this doc
- `docs/cross_tool/20260512_strategy_DISPATCH_paper_v2_verify_sentinel_endpoint.md` — paper-v2 verify Task 8 dispatch
