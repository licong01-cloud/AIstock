# AIstock production directory reconciliation audit - 2026-05-06

Scope: compare `F:/Dev/AIstock` against `origin/main` without modifying production files.

## Summary

- origin_main: `405adcbf4de1f3acd5cf2d4d8bfb3e9060af4300`
- prod_head: `1534a8c8809de3cdb3dc40ac81cf18665cf1708e`
- tracked_status_count: `15`
- tracked_same_as_origin_count: `13`
- tracked_different_from_origin_count: `2`
- untracked_total: `727`
- untracked_in_origin_same_count: `18`
- untracked_in_origin_different_count: `1`
- untracked_not_in_origin_count: `708`
- local_only_code_config_script_count: `36`
- local_only_doc_count: `51`
- local_only_artifact_temp_count: `621`


## Reconcile actions executed on clean branch

- Preserved production-visible `/paper-trading/package-selection` route by copying `frontend/src/app/paper-trading/package-selection/page.tsx` from the production worktree and validating `npm run build` in the reconcile worktree.
- Preserved RD-Agent dispatch environment coverage by copying `backend/tests/test_dispatch_service_env.py`, updating the local-home fixture to match current `origin/main`, and validating the targeted pytest file.
- Preserved reviewed local-only non-temp docs, validation records, and reusable diagnostic scripts in their original paths so a later production sync does not remove them from `F:/Dev/AIstock`.
- Did not preserve `scripts/v25_verify.py` because it has a syntax error in the production worktree and is not a runnable function; it remains in the backup snapshot for manual recovery if needed.
- Did not preserve `.codex_tmp/`, `.coverage`, `catboost_info/`, `qlib_minute_validation/`, `monitoring/process-exporter/process-exporter`, transient PKL/CSV artifacts, or root deletion-candidate manifests as production runtime functions.

## Tracked files different from origin/main

- ` M` `backend/services/quantevolver/config_composer.py`
- ` M` `backend/tests/unified_engine/test_qe_config_truth.py`

Decision: preserve `origin/main` for tracked differences unless a later review proves a missing function. Current known differences are older local variants.


## Untracked files that already exist in origin/main with identical content

- `backend/routers/prometheus_admin.py`
- `backend/routers/stock_universe.py`
- `backend/services/prometheus_admin.py`
- `backend/services/stock_universe_pit_service.py`
- `backend/tests/test_authoritative_bin_pit_universe.py`
- `backend/tests/test_prometheus_admin.py`
- `backend/tests/test_stock_universe_pit_service.py`
- `backend/tests/test_stock_universe_pit_spans.py`
- `docs/architecture/aistock_automation_test_coverage_gap_requirements_20260504.md`
- `docs/architecture/aistock_high_priority_issue_audit_20260427.md`
- `docs/architecture/aistock_unresolved_quality_issues_20260429.md`
- `scripts/build_stock_universe_pit_spans.py`
- `scripts/create_stock_st_events_table.py`
- `scripts/qe_event_risk_policy.py`
- `tests/aistock_validation/history/qe/20260505_165945_l3_qe-event-risk-policy-forced-exit-runtime.md`
- `tests/aistock_validation/history/qlib_data/20260504_l3_st-pit-active-derived-universe-implementation.md`
- `tests/aistock_validation/history/qlib_data/20260505_l2_rdagent-data-doctor-skill-ui-export-coverage.md`
- `tests/aistock_validation/history/qlib_data/20260505_l4_st-pit-production-replacement-validation.md`

## Untracked files that exist in origin/main but differ

- `tests/aistock_validation/history/qe/20260505_182545_l3_qe-mandatory-st-pit-risk-policy-for-new-and-derived-runs.md`

## Local-only code/config/script candidates

- `backend/tests/test_dispatch_service_env.py` 鈥?preserve_candidate: test coverage for existing dispatch_service functions; safe to add if it passes
- `configs/execution_algos/v25_two_stage.yaml` 鈥?review_v25_legacy: V25 support/validation artifact; origin has newer V25/V25.1 DB/runtime support, check before committing old paths
- `frontend/src/app/paper-trading/package-selection/page.tsx` 鈥?preserve_candidate: production-visible route is linked by paper-trading layout but page is absent from origin/main
- `qe_v25_existing_artifact_audit.py` 鈥?review: local-only non-temp file; requires human/agent review before production sync
- `scripts/add_v25_to_catalog.py` 鈥?review_v25_legacy: V25 support/validation artifact; origin has newer V25/V25.1 DB/runtime support, check before committing old paths
- `scripts/add_v25_to_db.py` 鈥?review_v25_legacy: V25 support/validation artifact; origin has newer V25/V25.1 DB/runtime support, check before committing old paths
- `scripts/automation/hmm_l10_conditional_qe_monitor_20260506.py` 鈥?archive_or_preserve_tool: experiment/diagnostic script; not production runtime, preserve if reusable
- `scripts/automation/hmm_qe_overnight_monitor_20260505.py` 鈥?archive_or_preserve_tool: experiment/diagnostic script; not production runtime, preserve if reusable
- `scripts/create_suspend_d_table.py` 鈥?review: local-only non-temp file; requires human/agent review before production sync
- `scripts/diagnostics/hmm_loop10_centered_attribution.py` 鈥?archive_or_preserve_tool: experiment/diagnostic script; not production runtime, preserve if reusable
- `scripts/diagnostics/hmm_loop10_virtual_candidate_screen.py` 鈥?archive_or_preserve_tool: experiment/diagnostic script; not production runtime, preserve if reusable
- `scripts/diagnostics/hmm_offline_diagnostic.py` 鈥?archive_or_preserve_tool: experiment/diagnostic script; not production runtime, preserve if reusable
- `scripts/hmm_loop10_conditional_sparse_screen_20260506.py` 鈥?archive_or_preserve_tool: experiment/diagnostic script; not production runtime, preserve if reusable
- `scripts/hmm_sector_factor_stage3_screen_20260505.py` 鈥?archive_or_preserve_tool: experiment/diagnostic script; not production runtime, preserve if reusable
- `scripts/hmm_stage3_sparse_penalty_screen_20260505.py` 鈥?archive_or_preserve_tool: experiment/diagnostic script; not production runtime, preserve if reusable
- `scripts/qe_loop_artifact_summary.py` 鈥?review: local-only non-temp file; requires human/agent review before production sync
- `scripts/qlib_full_factor_minute_chain_validate.py` 鈥?review: local-only non-temp file; requires human/agent review before production sync
- `scripts/qlib_multi_dataset_smoke_backtest.py` 鈥?review: local-only non-temp file; requires human/agent review before production sync
- `scripts/register_hmm_loop10_bottom_penalty_candidates_20260505.py` 鈥?archive_or_preserve_tool: experiment/diagnostic script; not production runtime, preserve if reusable
- `scripts/register_hmm_loop10_conditional_sparse_qe_candidates_20260506.py` 鈥?archive_or_preserve_tool: experiment/diagnostic script; not production runtime, preserve if reusable
- `scripts/register_hmm_stage3_qe_candidates_20260505.py` 鈥?archive_or_preserve_tool: experiment/diagnostic script; not production runtime, preserve if reusable
- `scripts/register_hmm_stage3_sparse_qe_candidates_20260505.py` 鈥?archive_or_preserve_tool: experiment/diagnostic script; not production runtime, preserve if reusable
- `scripts/test_minute_tracking.sh` 鈥?review: local-only non-temp file; requires human/agent review before production sync
- `scripts/test_v25_simple.py` 鈥?review: local-only non-temp file; requires human/agent review before production sync
- `scripts/v24_v25_real_test.py` 鈥?review: local-only non-temp file; requires human/agent review before production sync
- `scripts/v24_v25_test.py` 鈥?review: local-only non-temp file; requires human/agent review before production sync
- `scripts/v25_1_smoke_backtest.py` 鈥?review_v25_legacy: V25 support/validation artifact; origin has newer V25/V25.1 DB/runtime support, check before committing old paths
- `scripts/v25_mini_backtest.py` 鈥?review_v25_legacy: V25 support/validation artifact; origin has newer V25/V25.1 DB/runtime support, check before committing old paths
- `scripts/v25_minute_test.py` 鈥?review_v25_legacy: V25 support/validation artifact; origin has newer V25/V25.1 DB/runtime support, check before committing old paths
- `scripts/v25_minute_test_final.py` 鈥?review_v25_legacy: V25 support/validation artifact; origin has newer V25/V25.1 DB/runtime support, check before committing old paths
- `scripts/v25_verify.py` 鈥?review_v25_legacy: V25 support/validation artifact; origin has newer V25/V25.1 DB/runtime support, check before committing old paths
- `scripts/v25_verify_final.py` 鈥?review_v25_legacy: V25 support/validation artifact; origin has newer V25/V25.1 DB/runtime support, check before committing old paths
- `scripts/verify_v25_integration.py` 鈥?review_v25_legacy: V25 support/validation artifact; origin has newer V25/V25.1 DB/runtime support, check before committing old paths
- `scripts/verify_v25_minute_execution.py` 鈥?review_v25_legacy: V25 support/validation artifact; origin has newer V25/V25.1 DB/runtime support, check before committing old paths
- `tests/aistock_validation/history/qlib_data/20260504_l3_pit_bin_lgb_smoke.py` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/qlib_data/20260504_l4_pit_full_bin_lgb_smoke.py` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate

## Local-only docs/records candidates

- `AGENTS.override.md` 鈥?review: local-only non-temp file; requires human/agent review before production sync
- `a_share_duplicate_1_zip_deleted_manifest.txt` 鈥?review: local-only non-temp file; requires human/agent review before production sync
- `docs/add_v25_to_database.md` 鈥?archive_or_preserve_doc: development/operations doc; not runtime, preserve if still authoritative
- `docs/analysis/hmm_latest_one_year_sector_rotation_rough_check_qe_20260502_131502_9b54.md` 鈥?archive_or_preserve_doc: development/operations doc; not runtime, preserve if still authoritative
- `docs/analysis/hmm_offline_diagnostic_qe_20260502_131502_9b54.md` 鈥?archive_or_preserve_doc: development/operations doc; not runtime, preserve if still authoritative
- `docs/analysis/hmm_offline_optimization_qe_20260502_131502_9b54.md` 鈥?archive_or_preserve_doc: development/operations doc; not runtime, preserve if still authoritative
- `docs/analysis/qe_20260430_d55f_deep_analysis_20260501_deepseek_v4.md` 鈥?archive_or_preserve_doc: development/operations doc; not runtime, preserve if still authoritative
- `docs/analysis/qe_20260501_201036_b699_no_alpha_label_horizon_root_cause_20260502.md` 鈥?archive_or_preserve_doc: development/operations doc; not runtime, preserve if still authoritative
- `docs/analysis/qe_v25_strategy_fix_tasks_20260501.md` 鈥?archive_or_preserve_doc: development/operations doc; not runtime, preserve if still authoritative
- `docs/analysis/qe_v25_strategy_issues_analysis_20260501.md` 鈥?archive_or_preserve_doc: development/operations doc; not runtime, preserve if still authoritative
- `docs/analysis/qe_v25_tail_substitute_mechanism_20260501_deepseek_v4.md` 鈥?archive_or_preserve_doc: development/operations doc; not runtime, preserve if still authoritative
- `docs/analysis/tail_substitute_backup_candidates_fix_20260501.md` 鈥?archive_or_preserve_doc: development/operations doc; not runtime, preserve if still authoritative
- `docs/analysis/work_summary_20260501.md` 鈥?archive_or_preserve_doc: development/operations doc; not runtime, preserve if still authoritative
- `docs/architecture/lightweight_strategy_asset_registry_design_20260427.md` 鈥?archive_or_preserve_doc: development/operations doc; not runtime, preserve if still authoritative
- `docs/operations/docker_desktop_disk_cleanup_progress_20260504.md` 鈥?archive_or_preserve_doc: development/operations doc; not runtime, preserve if still authoritative
- `docs/operations_prometheus_cleanup.md` 鈥?archive_or_preserve_doc: development/operations doc; not runtime, preserve if still authoritative
- `docs/v25_integration_guide.md` 鈥?archive_or_preserve_doc: development/operations doc; not runtime, preserve if still authoritative
- `qmt_down_queue_delete_candidates.txt` 鈥?review: local-only non-temp file; requires human/agent review before production sync
- `scripts/MINUTE_TRACKING_README.md` 鈥?review: local-only non-temp file; requires human/agent review before production sync
- `tests/aistock_validation/history/hmm/20260504_172800_l2_hmm-sector-factor-retrain-diagnostic.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/hmm/20260504_174900_l2_hmm-sector-factor-stage2-screening.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/hmm/20260504_181747_l2_hmm-coeff-mapping-stage3.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/hmm/20260504_185500_l3_hmm-utility-aggressive-qe-task.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/hmm/20260504_235944_l2_hmm-loop10-centered-attribution-screen.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/hmm/20260505_004000_l3_hmm-loop10-bottom-penalty-qe-overnight.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/hmm/20260505_1007_l2_hmm-qe-visible-list-prune.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/hmm/20260505_1130_l2_hmm-sector-factor-stage3-retrain-screening.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/hmm/20260505_1235_l3_hmm-stage3-retrained-qe-task.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/hmm/20260505_1330_l3_hmm-stage3-qe-final-analysis.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/hmm/20260505_1625_l2_hmm-stage3-prune-and-sparse-screen.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/hmm/20260505_1715_l3_hmm-backtest-only-remote-qe-task.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/hmm/20260505_1955_l3_hmm-backtest-only-final-analysis.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/hmm/20260505_2110_l3_hmm-stage3-sparse-backtest-only-qe-start.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/hmm/20260505_2350_l3_hmm-stage3-sparse-qe-final-analysis.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/hmm/20260506_0015_l2_hmm-loop10-conditional-sparse-screen.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/hmm/20260506_0055_l3_hmm-l10-conditional-sparse-qe-start.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/monitoring/20260503_141500_l1_prometheus-cleanup-api.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/paper_v2_selection_center/20260505_l2_strategy-package-v25-model-cache-resolution.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/paper_v2_selection_center/20260505_l3_paper-v2-strategy-package-comprehensive-regression.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/qe/20260502_092614_l1_qe-polling-throttle-investigation.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/qe/20260504_061800_l3_hmm-full-validation-custom-evo-qe_20260504_014618_a9ec.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/qlib_data/20260429_190003_l3_qlib-multi-dataset-candidate-smoke-backtest-20260428.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/qlib_data/20260429_192821_l4_full-factor-minute-execution-chain-validation-20260428.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/qlib_data/20260429_211000_l4-official-qlib-rdagent-dataset-promotion-20260428.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/qlib_data/20260430_144250_l4_remote-rdagent-data-api-sync-20260428.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/qlib_data/20260504_l3_pit-bin-lgb-smoke-validation.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/qlib_data/20260504_l4_pit-full-daily-bin-validation.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/qlib_data/20260504_l4_st-pit-active-full-h5-bin-candidate-validation.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/qlib_data/20260505_l4_st-pit-active-manual-qe-validation.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/qlib_data/20260505_l5_st-pit-active-full-candidate-overnight-validation.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate
- `tests/aistock_validation/history/tdx_data/20260504_l3_tdx_delisted_history_interfaces.md` 鈥?archive_or_preserve_record: validation/history record; not runtime, can be committed if curated or archived if duplicate

## Local-only temporary/artifact count

- `621` files are currently classified as temp/artifact/cache and are backed up in `F:\Dev\AIstock_backups\prod_reconcile_preflight_20260506_085409\untracked_files`. They should not be production runtime dependencies unless reclassified.
