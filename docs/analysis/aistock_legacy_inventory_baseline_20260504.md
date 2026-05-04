# AIstock Legacy Inventory Baseline

- Generated at: 2026-05-04T07:04:30.646027+00:00
- Mode: `tracked_files`
- Files scanned: 2399
- Inventory items: 201
- Safety: read-only inventory; this is not a deletion list.

## Summary By Category

| Category | Count |
|---|---:|
| `legacy_doc_review` | 116 |
| `root_document_review` | 6 |
| `root_misc_review` | 1 |
| `root_python_review` | 29 |
| `script_lifecycle_review` | 49 |

## Summary By Lifecycle Status

| Status | Count |
|---|---:|
| `delete_candidate` | 101 |
| `deprecated` | 66 |
| `legacy_readonly` | 34 |

## Summary By Risk

| Risk | Count |
|---|---:|
| `high` | 29 |
| `medium` | 172 |

## Interpretation

This baseline identifies lifecycle-review candidates only.
Do not delete or move any file without module review and targeted validation.
Items with references or high-risk categories require extra review before any cleanup.

## First 200 Items

| Path | Category | Status | Risk | Confidence | References | Recommended Action |
|---|---|---|---|---|---:|---|
| `RD-Agent_Qlib_多因子备忘录.md` | `root_document_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `RDAGENT_LLM_CONFIG_DEPLOYMENT_GUIDE.md` | `root_document_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `__init__.py` | `root_python_review` | `deprecated` | `high` | `low` | 5 | `review_root_python_entrypoint` |
| `ai_agents.py` | `root_python_review` | `deprecated` | `high` | `low` | 5 | `review_root_python_entrypoint` |
| `analysis_current_implementation_problems.md` | `root_document_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `app_pg.py` | `root_python_review` | `deprecated` | `high` | `low` | 5 | `review_root_python_entrypoint` |
| `config.py` | `root_python_review` | `deprecated` | `high` | `low` | 5 | `review_root_python_entrypoint` |
| `create_catalog_tables.sql` | `root_misc_review` | `delete_candidate` | `medium` | `medium` | 2 | `move_to_owned_directory_or_remove_after_review` |
| `create_tables.py` | `root_python_review` | `deprecated` | `high` | `low` | 2 | `review_root_python_entrypoint` |
| `data_source_manager.py` | `root_python_review` | `deprecated` | `high` | `low` | 5 | `review_root_python_entrypoint` |
| `deepseek_client.py` | `root_python_review` | `deprecated` | `high` | `low` | 5 | `review_root_python_entrypoint` |
| `docs/2025-12-24_DataServiceLayer_Implementation_Design_AIstock.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/2025-12-29_RD-Agent_Phase2_HTTP_Sync_Progress.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/AIstock_TaskOnly_TaskSync_Design.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/AIstock_Task选股数据服务层最终设计方案.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/AIstock选股推理资产保障-执行计划.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/AIstock选股推理资产保障详细分析.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/HMM_Optimization_Analysis_Report.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/QE_Analysis_and_Design_v1.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 2 | `classify_document_and_link_from_index` |
| `docs/QE_pending_work_checklist.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/QE_system_analysis_blocking_and_data.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/QE_v3_implementation_plan.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/QE_verification_test_plan.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/QE实验模块整改设计方案_v2.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/QE实验模块整改设计方案_v3.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/RD-Agent_Catalog数据同步操作手册.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/RD-Agent_Workspace_Loop_Factor关联关系分析.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/RD-Agent因子命名区分方案设计.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/RD-Agent资产包打包流程图.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/RD-Agent资产包打包逻辑分析报告.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/SQL_OPTIMIZATION_VALUES_JOIN.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/adj_factor_70_days_analysis.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/adj_factor_analysis.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/aistock_factor_engine_design_2025-12-30.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/aistock_progress_checklist.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/aistock_rdagent_strategy_module_design.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/aistock_sim_trading_architecture_and_open_source_analysis.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/alpha158_memory_optimization_analysis.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/alpha158_memory_optimization_summary.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/api_fix_verification_report.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/api_implementation_issues.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/backend_test_report.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/bak_basic_cyq_perf_integration_plan.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/bak_basic_cyq_perf_integration_plan_complete.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/bin_70_days_nan_analysis.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/bin_data_quality_analysis.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/bin_dates_cross_verify.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/bin_first_valid_date_update_report.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/bin_h5_full_nan_report.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/bin_nan_calendar_analysis.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/bin_nan_impact_analysis.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/bin_nan_verification_report.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/bin_only_dates_vs_calendar.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/code_fix_plan.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/codex_project_memory.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 5 | `classify_document_and_link_from_index` |
| `docs/correlation_gemm_optimization_plan.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/csv_bin_data_difference_analysis.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/csv_weekend_holiday_analysis.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/daily_pv_h5_pandas_structure.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/daily_pv_vs_bin_compare.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/daily_pv_vs_bin_compare_pandas.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/data_source_comparison.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/export_code_analysis.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/factor_calculation_optimization_complete.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/factor_selection_data_service_analysis.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/final_analysis_report.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/final_fix_summary.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/final_fix_verification_report.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/final_implementation_summary.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/fix_summary.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/full_analysis_report.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/hmm_verification_manual.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/hmm_verification_qlib.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 2 | `classify_document_and_link_from_index` |
| `docs/migration_to_F_drive.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/miniqmt_dataset_stats_enhancement_design.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/missing_pattern_analysis.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/missing_stocks_in_h5.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/multi_factor_stock_selection_standard.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/qlib_bin_20251209_data_issue.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/qlib_csv_to_bin_conversion_analysis.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/qlib_daily_export_progress.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/qlib_daily_qfq_design.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/qlib_snapshot_field_map_update.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/qlib_training_backtest_guide.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/qmt_design_confirmation_summary.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/qmt_development_complete.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/qmt_implementation_summary.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/qmt_sim_xtquant_setup.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/qmt_trading_capabilities_analysis.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/qmt_trading_system_design.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/rd_agent_prompt_pack_implementation_progress.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/rdagent_candidate_tables_design.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/rdagent_sota_factor_dedup_design.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/realtime_subscriber_integration.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/rl_execution_v15_roadmap.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/rl_execution_v16_design.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/rl_execution_v17_design.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/root_cause_analysis.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/selection_fix_and_optimization.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 2 | `classify_document_and_link_from_index` |
| `docs/selection_performance_analysis.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/selection_test_results.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/simple_strategy_trading_guide.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/sota_factor_catalog_sync_design.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/startup_guide.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/strategy_auto_execution_explanation.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/strategy_base_class_explanation.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/strategy_config_guide.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/strategy_qa.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/strategy_realtime_data_explanation.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/strategy_trading_implementation_plan.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/task_sync_final_analysis.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/task_sync_strict_analysis.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/task_sync_verification_report.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/tdx_realtime_equivalence_analysis.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/tushare_new_datasource_factor_plan.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/unified_engine_design.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 2 | `classify_document_and_link_from_index` |
| `docs/unified_engine_test_plan.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/v23_residual_correction_plan.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/v23_unfilled_strategy_design.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/v24_asymmetric_execution_design.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `docs/v24_results_and_roadmap.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `medium` | 0 | `classify_archive_or_remove_after_review` |
| `docs/watchlist_enhancement_design.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/xtquant_data_service_integration.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/xtquant_dataset_catalog.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/xtquant_miniqmt_integration_memo.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/xtquant_realtime_quote_guide.md` | `legacy_doc_review` | `delete_candidate` | `medium` | `low` | 0 | `classify_archive_or_remove_after_review` |
| `docs/选股数据服务层分析与设计方案.md` | `legacy_doc_review` | `legacy_readonly` | `medium` | `low` | 1 | `classify_document_and_link_from_index` |
| `export_index_tdx_to_qlib_bin.py` | `root_python_review` | `deprecated` | `high` | `low` | 1 | `review_root_python_entrypoint` |
| `findings.md` | `root_document_review` | `legacy_readonly` | `medium` | `low` | 3 | `classify_document_and_link_from_index` |
| `miniqmt_interface.py` | `root_python_review` | `deprecated` | `high` | `low` | 3 | `review_root_python_entrypoint` |
| `monitor_scheduler.py` | `root_python_review` | `deprecated` | `high` | `low` | 3 | `review_root_python_entrypoint` |
| `monitor_service.py` | `root_python_review` | `deprecated` | `high` | `low` | 5 | `review_root_python_entrypoint` |
| `network_optimizer.py` | `root_python_review` | `deprecated` | `high` | `low` | 5 | `review_root_python_entrypoint` |
| `notification_service.py` | `root_python_review` | `deprecated` | `high` | `low` | 5 | `review_root_python_entrypoint` |
| `pg_monitor_repo.py` | `root_python_review` | `deprecated` | `high` | `low` | 5 | `review_root_python_entrypoint` |
| `pg_portfolio_db.py` | `root_python_review` | `deprecated` | `high` | `low` | 2 | `review_root_python_entrypoint` |
| `pg_smart_monitor_repo.py` | `root_python_review` | `deprecated` | `high` | `low` | 3 | `review_root_python_entrypoint` |
| `pg_watchlist_repo.py` | `root_python_review` | `deprecated` | `high` | `low` | 3 | `review_root_python_entrypoint` |
| `portfolio_manager.py` | `root_python_review` | `deprecated` | `high` | `low` | 5 | `review_root_python_entrypoint` |
| `portfolio_scheduler.py` | `root_python_review` | `deprecated` | `high` | `low` | 3 | `review_root_python_entrypoint` |
| `progress.md` | `root_document_review` | `legacy_readonly` | `medium` | `low` | 2 | `classify_document_and_link_from_index` |
| `run_migration.py` | `root_python_review` | `deprecated` | `high` | `low` | 5 | `review_root_python_entrypoint` |
| `scripts/_backfill_v2_deterministic.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 2 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/_dry_grade_v2_overfit.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 2 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/_inspect_catalog_disabled.py` | `script_lifecycle_review` | `delete_candidate` | `medium` | `medium` | 0 | `move_to_debug_tools_or_remove_after_review` |
| `scripts/_inspect_classification_schema.py` | `script_lifecycle_review` | `delete_candidate` | `medium` | `medium` | 0 | `move_to_debug_tools_or_remove_after_review` |
| `scripts/_inspect_factor_metrics_schema.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 1 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/_inspect_monthly_ic.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 1 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/_inspect_prompts.py` | `script_lifecycle_review` | `delete_candidate` | `medium` | `medium` | 0 | `move_to_debug_tools_or_remove_after_review` |
| `scripts/_inspect_rating_tables.py` | `script_lifecycle_review` | `delete_candidate` | `medium` | `medium` | 0 | `move_to_debug_tools_or_remove_after_review` |
| `scripts/_inspect_v2_samples.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 1 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/_probe_backtest_tables.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 1 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/_probe_metric_schema.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 1 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/_scan_rule_b_v2.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 2 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/_smoke_deletion_candidate.py` | `script_lifecycle_review` | `delete_candidate` | `medium` | `medium` | 0 | `move_to_debug_tools_or_remove_after_review` |
| `scripts/_smoke_test_factor_analyst_v2.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 1 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/_test_classify_rules.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 1 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/_test_pipeline_10_factors.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 1 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/aistock_data_quality_smoke.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 5 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/backfill_monthly_ic_v2.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 2 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/batch_develop_factors_v2.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 1 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/clear_ratings_for_v2.py` | `script_lifecycle_review` | `delete_candidate` | `medium` | `medium` | 0 | `move_to_debug_tools_or_remove_after_review` |
| `scripts/diagnostics/hmm_qe_candidate_attribution.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 2 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/diagnostics/hmm_sector_factor_overlay_diagnostic.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 2 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/diagnostics/sector_factor_rankic_report.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 1 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/hmm_horizon_v2_compare.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 3 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/hmm_horizon_v2_train.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 5 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/inspect_adj_factor_job_errors.py` | `script_lifecycle_review` | `delete_candidate` | `medium` | `medium` | 0 | `move_to_debug_tools_or_remove_after_review` |
| `scripts/inspect_db_locks.py` | `script_lifecycle_review` | `delete_candidate` | `medium` | `medium` | 0 | `move_to_debug_tools_or_remove_after_review` |
| `scripts/inspect_news_latest.py` | `script_lifecycle_review` | `delete_candidate` | `medium` | `medium` | 0 | `move_to_debug_tools_or_remove_after_review` |
| `scripts/inspect_tdx_data_20251128_single_code.py` | `script_lifecycle_review` | `delete_candidate` | `medium` | `medium` | 0 | `move_to_debug_tools_or_remove_after_review` |
| `scripts/migrate_factor_rating_v2.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 1 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/paper_v2_live_validation.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 2 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/qe_archive_data_quality_smoke.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 5 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/qe_backtest_accuracy_materiality_audit.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 1 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/qe_evolution_diagnostic.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 2 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/qe_qlib_minute_gap_diagnosis.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 3 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/qe_v25_existing_artifact_audit.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 1 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/qlib_authoritative_smoke_backtest.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 5 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/qlib_v25_limit_state_smoke.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 4 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/refresh_factor_analyst_v2_prompt.py` | `script_lifecycle_review` | `delete_candidate` | `medium` | `medium` | 0 | `move_to_debug_tools_or_remove_after_review` |
| `scripts/register_score_weighted_strategy_v2.py` | `script_lifecycle_review` | `delete_candidate` | `medium` | `medium` | 0 | `move_to_debug_tools_or_remove_after_review` |
| `scripts/smoke_test_10D_models.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 1 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/tail_twap_v24_strategy.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 4 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/tail_twap_v25_strategy.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 5 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/v24_mini_backtest.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 1 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/verify_hmm_covariance_fix.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 1 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/verify_hmm_direct.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 2 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/verify_hmm_qlib.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 2 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/verify_hmm_simple.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 1 | `promote_to_formal_script_or_keep_with_tests` |
| `scripts/verify_hmm_wsl.py` | `script_lifecycle_review` | `deprecated` | `medium` | `low` | 3 | `promote_to_formal_script_or_keep_with_tests` |
| `sector_strategy_agents.py` | `root_python_review` | `deprecated` | `high` | `low` | 2 | `review_root_python_entrypoint` |
| `sector_strategy_data.py` | `root_python_review` | `deprecated` | `high` | `low` | 4 | `review_root_python_entrypoint` |
| `sector_strategy_db.py` | `root_python_review` | `deprecated` | `high` | `low` | 4 | `review_root_python_entrypoint` |
| `sector_strategy_engine.py` | `root_python_review` | `deprecated` | `high` | `low` | 4 | `review_root_python_entrypoint` |
| `sector_strategy_markdown.py` | `root_python_review` | `deprecated` | `high` | `low` | 2 | `review_root_python_entrypoint` |
| `sector_strategy_pdf.py` | `root_python_review` | `deprecated` | `high` | `low` | 3 | `review_root_python_entrypoint` |
| `sector_strategy_scheduler.py` | `root_python_review` | `deprecated` | `high` | `low` | 3 | `review_root_python_entrypoint` |
| `sector_strategy_ui.py` | `root_python_review` | `deprecated` | `high` | `low` | 1 | `review_root_python_entrypoint` |
| `stock_data.py` | `root_python_review` | `deprecated` | `high` | `low` | 5 | `review_root_python_entrypoint` |

Report truncated to 200 items. See JSON output for full machine-readable details.
