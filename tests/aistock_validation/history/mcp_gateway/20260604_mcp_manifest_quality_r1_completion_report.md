# MCP 统一 Gateway R1 manifest 质量完成报告

## 0. 基本信息

- 任务：MCP 统一 Gateway 改进 R1 —— manifest 数据质量校准（gateway 侧）。
- Worktree：`F:\Dev\AIstock_worktrees\mcp-manifest-quality-20260604`
- Branch：`codex/mcp-manifest-quality-20260604`
- Batch ID：`BATCH-MCP-GATEWAY-R1-20260604`
- 实现提交：`85ca5104f9efe4187395bb51ac9deb91611a28f7`（short：`85ca5104`）
- G1 Validation Center job：`valjob_20260604_064748_c3861e05`
- G1 Validation Center run_id：`platform-mcp-gateway_20260604_064759_l2_mcp-gateway-manifest-quality_c3861e05_runner-validation__1ab0750fee`
- 生产端口边界：仅启动并停止过临时 `8011` 验证后端；未启动、停止或重启 `8001/3000/19080`。
- 范围边界：未修改 Research Assistant service 代码；未做 stock_analysis 模块；未做 DB DDL；未修改 `codex_project_memory.md` / `AGENTS.md` / `AGENTS.override.md`。

## 1. 改动清单与关键 diff 摘要

### M1：risk_level / assistant_usable 质量校准

- 文件：`backend/mcp/tool_manifest.py`
  - 新增 `ToolMetadataOverride`，强制每条 override 必须带一行理由。
  - 新增 `TOOL_METADATA_OVERRIDES`，在启发式基础上对逐工具语义做显式校准。
  - 对 plan-only 工具保留 `read_only/direct_or_catalog`，但必须有证据理由；对确认/写入/长任务/外网/生产邻近工具统一进入 `preflight_required`。
  - `validate_manifest()` 增加 side-effect-looking 工具不得无理由保留 read_only 的 fail-fast 校验。
- 文件：`tests/mcp/test_mcp_tool_manifest.py`
  - 新增 `test_manifest_risk_no_write_as_readonly`。
  - 覆盖 `_confirmed/register/deprecate/promote/retire/bind/apply/toggle/sync/repair/schedule/report_bug/assign/update_bug/start_validation_execution/github_issue_create` 安全网 token。
  - plan-only 工具必须通过 override 白名单且理由含 `plan-only preview`。

### M2：migration_state 诚实化

- 文件：`backend/mcp/tool_manifest.py`
  - 新增 `_migration_state_for(...)`，由 `GATEWAY_MODULES` / `SCRIPT_BACKED_SERVERS` 推导，不再在 `build_tool_manifest()` 中硬编码所有条目为 `gateway`。
  - 新增 `MIGRATION_STATE_OVERRIDES`，保留 `wrapper_compat` / `deprecated_pending_approval` 表达能力。
  - `validate_manifest()` 对非法 migration_state fail-fast。
- 文件：`tests/mcp/test_mcp_tool_manifest.py`
  - 增加 gateway/script-backed/override 推导测试。
  - 增加非法 migration_state 拦截测试。

### M3：validation module 不再 import legacy script 顶层代码

- 文件：`backend/mcp/validation_issue_items.py`
  - 新增共享 `compact_issue_item(...)`。
- 文件：`scripts/aistock_mcp_server.py`
  - `_compact_issue_item(...)` 改为委托共享 helper，保持旧脚本兼容入口。
- 文件：`backend/mcp/legacy_validation_adapter.py`
  - 新增 lazy adapter，仅在实际调用兼容 BUG/GitHub workflow wrapper 时才导入 `scripts.aistock_mcp_server`。
- 文件：`backend/mcp/modules/validation.py`
  - 移除顶层 `from scripts import aistock_mcp_server`。
  - `report_bug`、`mcp_github_issue_*`、`assign_bug`、`update_bug_status` 等 7 个兼容 wrapper 收敛到 lazy adapter。
- 文件：`tests/mcp/test_mcp_inventory_diff.py`
  - 新增 AST 扫描，禁止 `backend/mcp/modules/*.py` import `scripts`。
  - import 全部 gateway modules 后断言未新增 `scripts.aistock_mcp_server`。

### Validation Center 接入

- 文件：`tests/aistock_validation/catalog/test_plans.yaml`
  - 新增 `plan_key=mcp_gateway_manifest_quality`，`runner_enabled: true`。
- 文件：`tests/aistock_validation/catalog/module_registry.yaml`
  - 补充 `platform.mcp_gateway` 推荐计划映射。
- 文件：`backend/services/validation/plan_catalog.py`
  - 新增 `nox_mcp_gateway_manifest_quality` command key 映射。
- 文件：`noxfile.py`
  - 新增 `mcp_gateway_manifest_quality` session：compileall、self-check、doctor、`pytest tests/mcp -q -p no:cacheprovider`。

## 2. 完整 209 工具风险审计表

- 审计口径：`TOOL_MANIFEST` 当前构建结果，包含启发式 + override 后的最终风险标定。
- 总数：209（203 legacy/business tools + 6 catalog tools）。
- risk_level 分布：`catalog=6`、`read_only=113`、`long_running=31`、`external_network=9`、`write_confirmed=23`、`production_adjacent=27`。
- assistant_usable 分布：`direct_or_catalog=119`、`preflight_required=90`。
- 当前 migration_state：209 个均为 `gateway`；表达能力保留 `script_backed/wrapper_compat/deprecated_pending_approval`。

| # | module | tool_name | risk_level | assistant_usable | confirmation | migration_state | response_budget | backend_endpoint | override_reason |
| ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | catalog | `mcp_gateway_health` | catalog | direct_or_catalog | false | gateway | single_resource | `mcp-gateway/catalog` | - |
| 2 | catalog | `mcp_gateway_list_profiles` | catalog | direct_or_catalog | false | gateway | summary_or_paginated | `mcp-gateway/catalog` | - |
| 3 | catalog | `mcp_gateway_list_modules` | catalog | direct_or_catalog | false | gateway | summary_or_paginated | `mcp-gateway/catalog` | - |
| 4 | catalog | `mcp_gateway_list_tools` | catalog | direct_or_catalog | false | gateway | summary_or_paginated | `mcp-gateway/catalog` | - |
| 5 | catalog | `mcp_gateway_search_tools` | catalog | direct_or_catalog | false | gateway | summary_or_paginated | `mcp-gateway/catalog` | - |
| 6 | catalog | `mcp_gateway_preflight_tool` | catalog | direct_or_catalog | false | gateway | bounded_json | `mcp-gateway/catalog` | - |
| 7 | execution_policy | `execution_policy_list_algos` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `execution-policy/*` | - |
| 8 | execution_policy | `execution_policy_get_algo` | read_only | direct_or_catalog | false | gateway | single_resource | `execution-policy/*` | - |
| 9 | execution_policy | `execution_policy_validate_for_strategy` | read_only | direct_or_catalog | false | gateway | bounded_json | `execution-policy/*` | - |
| 10 | execution_policy | `execution_policy_get_market_state_constraints` | read_only | direct_or_catalog | false | gateway | single_resource | `execution-policy/*` | - |
| 11 | execution_policy | `execution_policy_plan_binding` | read_only | direct_or_catalog | false | gateway | bounded_json | `execution-policy/*` | plan-only preview: backend /execution-policy/binding-plan validates and reports will_create/will_enable without persisting a policy binding |
| 12 | execution_policy | `execution_policy_bind_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `execution-policy/*` | - |
| 13 | execution_policy | `execution_policy_retire_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `execution-policy/*` | - |
| 14 | external_research | `external_research_search_web` | external_network | preflight_required | false | gateway | summary_or_paginated | `external-research/*` | - |
| 15 | external_research | `external_research_search_papers` | external_network | preflight_required | false | gateway | summary_or_paginated | `external-research/*` | - |
| 16 | external_research | `external_research_fetch_extract` | external_network | preflight_required | false | gateway | summary_or_paginated | `external-research/*` | - |
| 17 | external_research | `external_research_save_evidence` | external_network | preflight_required | false | gateway | summary_or_paginated | `external-research/*` | - |
| 18 | factor_correlation | `factor_corr_plan` | read_only | direct_or_catalog | false | gateway | bounded_json | `factor-correlation/*` | plan-only preview: backend /factor-correlation/plan only reads factor eligibility and defers async job creation to factor_corr_submit_confirmed |
| 19 | factor_correlation | `factor_corr_validate_inputs` | read_only | direct_or_catalog | false | gateway | bounded_json | `factor-correlation/*` | - |
| 20 | factor_correlation | `factor_corr_submit_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `factor-correlation/*` | - |
| 21 | factor_correlation | `factor_corr_get_job` | read_only | direct_or_catalog | false | gateway | single_resource | `factor-correlation/*` | - |
| 22 | factor_correlation | `factor_corr_get_top_pairs` | read_only | direct_or_catalog | false | gateway | single_resource | `factor-correlation/*` | - |
| 23 | factor_correlation | `factor_corr_get_clusters` | read_only | direct_or_catalog | false | gateway | single_resource | `factor-correlation/*` | - |
| 24 | factor_correlation | `factor_corr_suggest_replacements` | read_only | direct_or_catalog | false | gateway | bounded_json | `factor-correlation/*` | - |
| 25 | factor_correlation | `factor_corr_get_matrix_ref` | read_only | direct_or_catalog | false | gateway | single_resource | `factor-correlation/*` | - |
| 26 | factor_library | `factor_library_list` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `factor-library/*` | - |
| 27 | factor_library | `factor_library_search` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `factor-library/*` | - |
| 28 | factor_library | `factor_library_get` | read_only | direct_or_catalog | false | gateway | single_resource | `factor-library/*` | - |
| 29 | factor_library | `factor_library_get_coverage` | read_only | direct_or_catalog | false | gateway | single_resource | `factor-library/*` | - |
| 30 | factor_library | `factor_library_get_metric_summary` | read_only | direct_or_catalog | false | gateway | single_resource | `factor-library/*` | - |
| 31 | factor_library | `factor_library_get_usage_summary` | read_only | direct_or_catalog | false | gateway | single_resource | `factor-library/*` | - |
| 32 | factor_library | `factor_library_plan_register` | read_only | direct_or_catalog | false | gateway | bounded_json | `factor-library/*` | plan-only preview: backend /factor-library/register-plan reads duplicate state and defers writes to factor_library_register_confirmed |
| 33 | factor_library | `factor_library_register_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `factor-library/*` | - |
| 34 | factor_library | `factor_library_plan_deprecate` | read_only | direct_or_catalog | false | gateway | bounded_json | `factor-library/*` | plan-only preview: backend /factor-library/deprecate-plan reads target state and defers writes to factor_library_deprecate_confirmed |
| 35 | factor_library | `factor_library_deprecate_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `factor-library/*` | - |
| 36 | factor_metrics | `factor_metrics_plan` | read_only | direct_or_catalog | false | gateway | bounded_json | `factor-metrics/*` | plan-only preview: backend /factor-metrics/plan only reads factor eligibility and defers async job creation to factor_metrics_submit_confirmed |
| 37 | factor_metrics | `factor_metrics_validate_inputs` | read_only | direct_or_catalog | false | gateway | bounded_json | `factor-metrics/*` | - |
| 38 | factor_metrics | `factor_metrics_submit_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `factor-metrics/*` | - |
| 39 | factor_metrics | `factor_metrics_get_job` | read_only | direct_or_catalog | false | gateway | single_resource | `factor-metrics/*` | - |
| 40 | factor_metrics | `factor_metrics_get_result` | read_only | direct_or_catalog | false | gateway | single_resource | `factor-metrics/*` | - |
| 41 | factor_metrics | `factor_metrics_compare_versions` | read_only | direct_or_catalog | false | gateway | bounded_json | `factor-metrics/*` | - |
| 42 | factor_metrics | `factor_metrics_export_result_ref` | read_only | direct_or_catalog | false | gateway | bounded_json | `factor-metrics/*` | - |
| 43 | local_data | `local_data_health_overview` | read_only | direct_or_catalog | false | gateway | single_resource | `local-data/*` | - |
| 44 | local_data | `local_data_get_dataset_status` | read_only | direct_or_catalog | false | gateway | single_resource | `local-data/*` | - |
| 45 | local_data | `local_data_list_data_stats` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `local-data/*` | - |
| 46 | local_data | `local_data_check_gaps` | read_only | direct_or_catalog | false | gateway | bounded_json | `local-data/*` | - |
| 47 | local_data | `local_data_compute_auto_range` | read_only | direct_or_catalog | false | gateway | bounded_json | `local-data/*` | - |
| 48 | local_data | `local_data_list_alerts` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `local-data/*` | - |
| 49 | local_data | `local_data_get_unack_alert_count` | read_only | direct_or_catalog | false | gateway | single_resource | `local-data/*` | - |
| 50 | local_data | `local_data_list_sync_targets` | long_running | preflight_required | false | gateway | summary_or_paginated | `local-data/*` | - |
| 51 | local_data | `local_data_get_sync_target` | long_running | preflight_required | false | gateway | single_resource | `local-data/*` | - |
| 52 | local_data | `local_data_list_sync_attempts` | long_running | preflight_required | false | gateway | summary_or_paginated | `local-data/*` | - |
| 53 | local_data | `local_data_list_jobs` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `local-data/*` | - |
| 54 | local_data | `local_data_get_job` | read_only | direct_or_catalog | false | gateway | single_resource | `local-data/*` | - |
| 55 | local_data | `local_data_get_job_logs` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `local-data/*` | - |
| 56 | local_data | `local_data_cancel_job_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 57 | local_data | `local_data_clear_queued_jobs_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 58 | local_data | `local_data_delete_job_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 59 | local_data | `local_data_run_dataset_sync_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 60 | local_data | `local_data_run_incremental_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 61 | local_data | `local_data_run_init_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 62 | local_data | `local_data_run_schedule_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 63 | local_data | `local_data_run_single_preset_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 64 | local_data | `local_data_run_all_presets_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 65 | local_data | `local_data_refresh_stats_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 66 | local_data | `local_data_sync_calendar_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 67 | local_data | `local_data_build_sector_data_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 68 | local_data | `local_data_export_sector_data_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 69 | local_data | `local_data_sync_tushare_all_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 70 | local_data | `local_data_list_schedules` | production_adjacent | preflight_required | false | gateway | summary_or_paginated | `local-data/*` | - |
| 71 | local_data | `local_data_get_schedule_defaults` | production_adjacent | preflight_required | false | gateway | single_resource | `local-data/*` | - |
| 72 | local_data | `local_data_upsert_schedule_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 73 | local_data | `local_data_batch_create_schedules_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 74 | local_data | `local_data_toggle_schedule_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 75 | local_data | `local_data_delete_schedule_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 76 | local_data | `local_data_plan_schedule_reset` | read_only | direct_or_catalog | false | gateway | bounded_json | `local-data/*` | plan-only preview: LocalDataManagementService.plan_schedule_reset returns reset actions and summary says not written |
| 77 | local_data | `local_data_apply_schedule_reset_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 78 | local_data | `local_data_get_preset_stats` | read_only | direct_or_catalog | false | gateway | single_resource | `local-data/*` | - |
| 79 | local_data | `local_data_get_preset_daily_status` | read_only | direct_or_catalog | false | gateway | single_resource | `local-data/*` | - |
| 80 | local_data | `local_data_run_source_test_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 81 | local_data | `local_data_list_source_test_runs` | production_adjacent | preflight_required | false | gateway | summary_or_paginated | `local-data/*` | - |
| 82 | local_data | `local_data_list_source_test_schedules` | production_adjacent | preflight_required | false | gateway | summary_or_paginated | `local-data/*` | - |
| 83 | local_data | `local_data_upsert_source_test_schedule_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 84 | local_data | `local_data_toggle_source_test_schedule_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 85 | local_data | `local_data_run_source_test_schedule_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 86 | local_data | `local_data_plan_repair` | read_only | direct_or_catalog | false | gateway | bounded_json | `local-data/*` | plan-only preview: LocalDataManagementService.plan_repair inspects overview/status and defers execution to local_data_apply_repair_confirmed |
| 87 | local_data | `local_data_apply_repair_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `local-data/*` | - |
| 88 | local_data | `local_data_get_repair_status` | production_adjacent | preflight_required | false | gateway | single_resource | `local-data/*` | - |
| 89 | local_data | `local_data_explain_business_impact` | read_only | direct_or_catalog | false | gateway | bounded_json | `local-data/*` | - |
| 90 | model_registry | `model_registry_list` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `model-registry/*` | - |
| 91 | model_registry | `model_registry_get` | read_only | direct_or_catalog | false | gateway | single_resource | `model-registry/*` | - |
| 92 | model_registry | `model_registry_compare_trials` | read_only | direct_or_catalog | false | gateway | bounded_json | `model-registry/*` | - |
| 93 | model_registry | `model_registry_get_seed_stability` | read_only | direct_or_catalog | false | gateway | single_resource | `model-registry/*` | - |
| 94 | model_registry | `model_registry_get_hyperparam_history` | read_only | direct_or_catalog | false | gateway | single_resource | `model-registry/*` | - |
| 95 | model_registry | `model_registry_get_artifacts` | read_only | direct_or_catalog | false | gateway | single_resource | `model-registry/*` | - |
| 96 | model_registry | `model_registry_plan_register` | read_only | direct_or_catalog | false | gateway | bounded_json | `model-registry/*` | plan-only preview: backend /model-registry/register-plan returns payload summary and defers registry writes to model_registry_register_confirmed |
| 97 | model_registry | `model_registry_register_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `model-registry/*` | - |
| 98 | model_registry | `model_registry_deprecate_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `model-registry/*` | - |
| 99 | qe_archive | `qe_archive_health` | read_only | direct_or_catalog | false | gateway | single_resource | `qe-archive/*` | - |
| 100 | qe_archive | `qe_archive_list_runs` | production_adjacent | preflight_required | false | gateway | summary_or_paginated | `qe-archive/*` | - |
| 101 | qe_archive | `qe_archive_get_run_quality` | production_adjacent | preflight_required | false | gateway | single_resource | `qe-archive/*` | - |
| 102 | qe_archive | `qe_archive_list_outbox` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `qe-archive/*` | - |
| 103 | qe_archive | `qe_archive_list_jobs` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `qe-archive/*` | - |
| 104 | qe_archive | `qe_archive_list_skips` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `qe-archive/*` | - |
| 105 | qe_archive | `qe_archive_backfill_preview` | read_only | direct_or_catalog | false | gateway | bounded_json | `qe-archive/*` | - |
| 106 | qe_archive | `qe_archive_backfill_execute_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `qe-archive/*` | - |
| 107 | qe_archive | `qe_archive_backfill_selection_preview` | read_only | direct_or_catalog | false | gateway | bounded_json | `qe-archive/*` | - |
| 108 | qe_archive | `qe_archive_backfill_selection_execute_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `qe-archive/*` | - |
| 109 | qe_archive | `qe_archive_get_source_status` | read_only | direct_or_catalog | false | gateway | single_resource | `qe-archive/*` | - |
| 110 | qe_archive | `qe_archive_list_backfill_runs` | production_adjacent | preflight_required | false | gateway | summary_or_paginated | `qe-archive/*` | - |
| 111 | qe_archive | `qe_archive_get_backfill_run` | production_adjacent | preflight_required | false | gateway | single_resource | `qe-archive/*` | - |
| 112 | qe_archive | `qe_archive_worker_run_once_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `qe-archive/*` | - |
| 113 | qe_archive | `qe_archive_query_factor_usage` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `qe-archive/*` | - |
| 114 | qe_archive | `qe_archive_query_factor_importance` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `qe-archive/*` | - |
| 115 | qe_archive | `qe_archive_query_factor_importance_stability` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `qe-archive/*` | - |
| 116 | qe_archive | `qe_archive_query_model_trials` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `qe-archive/*` | - |
| 117 | qe_archive | `qe_archive_query_seed_trials` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `qe-archive/*` | - |
| 118 | qe_archive | `qe_archive_query_hyperparam_history` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `qe-archive/*` | - |
| 119 | qe_archive | `qe_archive_query_analytics_view_status` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `qe-archive/*` | - |
| 120 | qe_archive | `qe_archive_query_run_leaderboard` | production_adjacent | preflight_required | false | gateway | summary_or_paginated | `qe-archive/*` | - |
| 121 | qe_archive | `qe_archive_query_seed_robustness` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `qe-archive/*` | - |
| 122 | qe_archive | `qe_archive_query_factor_performance` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `qe-archive/*` | - |
| 123 | qe_archive | `qe_archive_query_model_hyperparam_seed_perf` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `qe-archive/*` | - |
| 124 | qe_archive | `qe_archive_query_overfit_flags` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `qe-archive/*` | - |
| 125 | qe_archive | `qe_archive_query_promotion_candidates` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `qe-archive/*` | - |
| 126 | qe_archive | `qe_archive_query_evolution_lineage` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `qe-archive/*` | - |
| 127 | qe_experiment | `qe_experiment_list` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `quantevolver/*` | - |
| 128 | qe_experiment | `qe_experiment_get` | read_only | direct_or_catalog | false | gateway | single_resource | `quantevolver/*` | - |
| 129 | qe_experiment | `qe_experiment_get_status` | read_only | direct_or_catalog | false | gateway | single_resource | `quantevolver/*` | - |
| 130 | qe_experiment | `qe_experiment_get_logs_tail` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `quantevolver/*` | - |
| 131 | qe_experiment | `qe_experiment_get_enhanced_metrics` | read_only | direct_or_catalog | false | gateway | single_resource | `quantevolver/*` | - |
| 132 | qe_experiment | `qe_experiment_get_trade_stats` | read_only | direct_or_catalog | false | gateway | single_resource | `quantevolver/*` | - |
| 133 | qe_experiment | `qe_experiment_run_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `quantevolver/*` | - |
| 134 | qe_experiment | `qe_experiment_stop_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `quantevolver/*` | - |
| 135 | qe_experiment | `qe_custom_evo_list_tasks` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `quantevolver/*` | - |
| 136 | qe_experiment | `qe_custom_evo_get_task` | read_only | direct_or_catalog | false | gateway | single_resource | `quantevolver/*` | - |
| 137 | qe_experiment | `qe_custom_evo_loop_comparison` | read_only | direct_or_catalog | false | gateway | bounded_json | `quantevolver/*` | - |
| 138 | qe_experiment | `qe_custom_evo_get_loop_config` | read_only | direct_or_catalog | false | gateway | single_resource | `quantevolver/*` | - |
| 139 | qe_experiment | `qe_custom_evo_get_loop_metrics` | read_only | direct_or_catalog | false | gateway | single_resource | `quantevolver/*` | - |
| 140 | qe_experiment | `qe_custom_evo_get_loop_analysis` | read_only | direct_or_catalog | false | gateway | single_resource | `quantevolver/*` | - |
| 141 | qe_experiment | `qe_custom_evo_get_config` | read_only | direct_or_catalog | false | gateway | single_resource | `quantevolver/*` | - |
| 142 | qe_experiment | `qe_custom_evo_get_logs_tail` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `quantevolver/*` | - |
| 143 | qe_experiment | `qe_custom_evo_run_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `quantevolver/*` | - |
| 144 | qe_experiment | `qe_custom_evo_delete_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `quantevolver/*` | - |
| 145 | qe_experiment | `qe_custom_evo_retry_loop_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `quantevolver/*` | - |
| 146 | qe_experiment | `qe_custom_evo_rerun_loop_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `quantevolver/*` | - |
| 147 | qe_experiment | `qe_custom_evo_append_loops_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `quantevolver/*` | - |
| 148 | qe_experiment | `qe_template_create` | production_adjacent | preflight_required | false | gateway | bounded_json | `quantevolver/*` | - |
| 149 | qe_experiment | `qe_template_get` | read_only | direct_or_catalog | false | gateway | single_resource | `quantevolver/*` | - |
| 150 | qe_experiment | `qe_template_validate` | read_only | direct_or_catalog | false | gateway | bounded_json | `quantevolver/*` | - |
| 151 | qe_experiment | `qe_template_materialize_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `quantevolver/*` | - |
| 152 | qe_experiment | `qe_template_run_confirmed` | long_running | preflight_required | true | gateway | bounded_json | `quantevolver/*` | - |
| 153 | research | `research_create_experiment` | production_adjacent | preflight_required | false | gateway | summary_or_paginated | `research/*` | - |
| 154 | research | `research_list_experiments` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `research/*` | - |
| 155 | research | `research_get_experiment` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `research/*` | - |
| 156 | research | `research_run_stage` | long_running | preflight_required | true | gateway | summary_or_paginated | `research/*` | - |
| 157 | research | `research_retry_stage` | write_confirmed | preflight_required | true | gateway | summary_or_paginated | `research/*` | - |
| 158 | research | `research_get_stage_result` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `research/*` | - |
| 159 | research | `research_compare_baseline` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `research/*` | - |
| 160 | research | `research_list_artifact_refs` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `research/*` | - |
| 161 | research | `research_list_backtest_records` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `research/*` | - |
| 162 | research | `research_hmm_backfill_preview` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `research/*` | - |
| 163 | research | `research_hmm_backfill_execute` | long_running | preflight_required | true | gateway | summary_or_paginated | `research/*` | - |
| 164 | research | `research_get_backfill_run` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `research/*` | - |
| 165 | research | `research_get_pipeline_types` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `research/*` | - |
| 166 | research | `research_create_issue` | production_adjacent | preflight_required | false | gateway | summary_or_paginated | `research/*` | - |
| 167 | research | `research_promote` | write_confirmed | preflight_required | true | gateway | summary_or_paginated | `research/*` | - |
| 168 | research | `research_reject` | production_adjacent | preflight_required | false | gateway | summary_or_paginated | `research/*` | - |
| 169 | research_assistant | `assistant_health` | read_only | direct_or_catalog | false | gateway | single_resource | `research-assistant/*` | - |
| 170 | research_assistant | `assistant_create_task` | production_adjacent | preflight_required | false | gateway | bounded_json | `research-assistant/*` | - |
| 171 | research_assistant | `assistant_add_task_event` | production_adjacent | preflight_required | false | gateway | bounded_json | `research-assistant/*` | writes Research Assistant task_events and may update task status through the backend API |
| 172 | research_assistant | `assistant_chat_turn` | external_network | preflight_required | false | gateway | bounded_json | `research-assistant/*` | creates task/conversation records and can invoke model-provider network calls through the Research Assistant backend |
| 173 | research_assistant | `assistant_build_prompt_bundle` | production_adjacent | preflight_required | false | gateway | bounded_json | `research-assistant/*` | writes prompt_bundles and optional task_events instead of returning a pure preview |
| 174 | research_assistant | `assistant_list_prompt_nodes` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `research-assistant/*` | - |
| 175 | research_assistant | `assistant_create_memory_candidate` | production_adjacent | preflight_required | false | gateway | bounded_json | `research-assistant/*` | - |
| 176 | research_assistant | `assistant_build_context_pack` | production_adjacent | preflight_required | false | gateway | bounded_json | `research-assistant/*` | writes context_packs and memory_access_log rows while assembling context |
| 177 | research_assistant | `assistant_list_mcp_tools` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `research-assistant/*` | - |
| 178 | research_assistant | `assistant_preflight_mcp_tool` | production_adjacent | preflight_required | false | gateway | bounded_json | `research-assistant/*` | records mcp_tool_events and optional task_events even though it performs preflight checks |
| 179 | research_assistant | `assistant_create_issue_candidate` | production_adjacent | preflight_required | false | gateway | bounded_json | `research-assistant/*` | - |
| 180 | research_assistant | `assistant_list_approvals` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `research-assistant/*` | - |
| 181 | research_assistant | `assistant_create_temp_memory` | production_adjacent | preflight_required | false | gateway | bounded_json | `research-assistant/*` | - |
| 182 | strategy_governance | `strategy_governance_list_packages` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `strategy-governance/*` | - |
| 183 | strategy_governance | `strategy_governance_get_package` | read_only | direct_or_catalog | false | gateway | single_resource | `strategy-governance/*` | - |
| 184 | strategy_governance | `strategy_governance_get_health` | read_only | direct_or_catalog | false | gateway | single_resource | `strategy-governance/*` | - |
| 185 | strategy_governance | `strategy_governance_get_selection_readiness` | read_only | direct_or_catalog | false | gateway | single_resource | `strategy-governance/*` | - |
| 186 | strategy_governance | `strategy_governance_get_paper_readiness` | read_only | direct_or_catalog | false | gateway | single_resource | `strategy-governance/*` | - |
| 187 | strategy_governance | `strategy_governance_plan_promotion` | read_only | direct_or_catalog | false | gateway | bounded_json | `strategy-governance/*` | plan-only preview: backend promotion-plan reads package health and marks status_transition_only before promote_confirmed writes |
| 188 | strategy_governance | `strategy_governance_plan_retirement` | read_only | direct_or_catalog | false | gateway | bounded_json | `strategy-governance/*` | plan-only preview: backend retirement-plan reads package detail and defers status transition to retire_confirmed |
| 189 | strategy_governance | `strategy_governance_promote_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `strategy-governance/*` | - |
| 190 | strategy_governance | `strategy_governance_retire_confirmed` | write_confirmed | preflight_required | true | gateway | bounded_json | `strategy-governance/*` | - |
| 191 | validation | `health` | read_only | direct_or_catalog | false | gateway | single_resource | `validation/*` | - |
| 192 | validation | `list_plans` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `validation/*` | - |
| 193 | validation | `get_plan` | read_only | direct_or_catalog | false | gateway | single_resource | `validation/*` | - |
| 194 | validation | `list_validation_runs` | production_adjacent | preflight_required | false | gateway | summary_or_paginated | `validation/*` | - |
| 195 | validation | `get_validation_run` | production_adjacent | preflight_required | false | gateway | single_resource | `validation/*` | - |
| 196 | validation | `list_findings` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `validation/*` | - |
| 197 | validation | `list_bugs` | read_only | direct_or_catalog | false | gateway | summary_or_paginated | `validation/*` | - |
| 198 | validation | `get_bug_agent_context` | read_only | direct_or_catalog | false | gateway | single_resource | `validation/*` | - |
| 199 | validation | `get_module_quality_summary` | read_only | direct_or_catalog | false | gateway | single_resource | `validation/*` | - |
| 200 | validation | `start_validation_execution` | long_running | preflight_required | false | gateway | bounded_json | `validation/*` | - |
| 201 | validation | `get_validation_execution_status` | read_only | direct_or_catalog | false | gateway | single_resource | `validation/*` | - |
| 202 | validation | `get_validation_execution_log` | read_only | direct_or_catalog | false | gateway | single_resource | `validation/*` | - |
| 203 | validation | `report_bug` | production_adjacent | preflight_required | false | gateway | bounded_json | `validation/*` | - |
| 204 | validation | `mcp_github_issue_list` | external_network | preflight_required | false | gateway | summary_or_paginated | `validation/*` | - |
| 205 | validation | `mcp_github_issue_search` | external_network | preflight_required | false | gateway | summary_or_paginated | `validation/*` | - |
| 206 | validation | `mcp_github_issue_create` | external_network | preflight_required | false | gateway | bounded_json | `validation/*` | - |
| 207 | validation | `assign_bug` | production_adjacent | preflight_required | false | gateway | bounded_json | `validation/*` | - |
| 208 | validation | `update_bug_status` | production_adjacent | preflight_required | false | gateway | single_resource | `validation/*` | - |
| 209 | validation | `mcp_github_issue_sync_bug` | external_network | preflight_required | false | gateway | bounded_json | `validation/*` | - |


## 3. Override 明细与理由

- 每条 override 都带一行理由。
- plan-only preview 工具的判断依据写在 reason 中：只返回计划/预览/校验结果，真实写入延后到对应 `*_confirmed` 或 apply/submit 工具。

| tool_name | risk_level | assistant_usable | confirmation | reason |
| --- | --- | --- | --- | --- |
| `assistant_add_task_event` | production_adjacent | preflight_required | False | writes Research Assistant task_events and may update task status through the backend API |
| `assistant_build_context_pack` | production_adjacent | preflight_required | False | writes context_packs and memory_access_log rows while assembling context |
| `assistant_build_prompt_bundle` | production_adjacent | preflight_required | False | writes prompt_bundles and optional task_events instead of returning a pure preview |
| `assistant_chat_turn` | external_network | preflight_required | False | creates task/conversation records and can invoke model-provider network calls through the Research Assistant backend |
| `assistant_preflight_mcp_tool` | production_adjacent | preflight_required | False | records mcp_tool_events and optional task_events even though it performs preflight checks |
| `execution_policy_plan_binding` | read_only | direct_or_catalog | False | plan-only preview: backend /execution-policy/binding-plan validates and reports will_create/will_enable without persisting a policy binding |
| `factor_corr_plan` | read_only | direct_or_catalog | False | plan-only preview: backend /factor-correlation/plan only reads factor eligibility and defers async job creation to factor_corr_submit_confirmed |
| `factor_library_plan_deprecate` | read_only | direct_or_catalog | False | plan-only preview: backend /factor-library/deprecate-plan reads target state and defers writes to factor_library_deprecate_confirmed |
| `factor_library_plan_register` | read_only | direct_or_catalog | False | plan-only preview: backend /factor-library/register-plan reads duplicate state and defers writes to factor_library_register_confirmed |
| `factor_metrics_plan` | read_only | direct_or_catalog | False | plan-only preview: backend /factor-metrics/plan only reads factor eligibility and defers async job creation to factor_metrics_submit_confirmed |
| `local_data_plan_repair` | read_only | direct_or_catalog | False | plan-only preview: LocalDataManagementService.plan_repair inspects overview/status and defers execution to local_data_apply_repair_confirmed |
| `local_data_plan_schedule_reset` | read_only | direct_or_catalog | False | plan-only preview: LocalDataManagementService.plan_schedule_reset returns reset actions and summary says not written |
| `model_registry_plan_register` | read_only | direct_or_catalog | False | plan-only preview: backend /model-registry/register-plan returns payload summary and defers registry writes to model_registry_register_confirmed |
| `strategy_governance_plan_promotion` | read_only | direct_or_catalog | False | plan-only preview: backend promotion-plan reads package health and marks status_transition_only before promote_confirmed writes |
| `strategy_governance_plan_retirement` | read_only | direct_or_catalog | False | plan-only preview: backend retirement-plan reads package detail and defers status transition to retire_confirmed |


## 4. 测试与验证结果

### 本地 gate

```text
$ python -m compileall backend/mcp scripts/aistock_mcp_gateway.py scripts/aistock_mcp_gateway_doctor.py
Listing 'backend/mcp'...
Listing 'backend/mcp\\modules'...
Compiling 'backend/mcp\\legacy_validation_adapter.py'...
Compiling 'backend/mcp\\modules\\validation.py'...
Compiling 'backend/mcp\\tool_manifest.py'...
Compiling 'backend/mcp\\validation_issue_items.py'...
exit_code=0
```

```text
$ python -m pytest tests/mcp -q
......................                                                   [100%]
22 passed in 7.00s
exit_code=0
```

```text
$ python scripts/aistock_mcp_gateway.py --self-check --profile=lite
"status": "pass"
"tool_count": 6
"manifest_tool_count": 209
"legacy_tool_count": 203
"platform_tool_count": 6
"errors": []
"warnings": []
exit_code=0
```

```text
$ python scripts/aistock_mcp_gateway_doctor.py --json
"status": "pass"
"repo.head": "85ca5104"
"gateway_lite.status": "pass"
"profiles.default_profile": "lite"
"static_no_llm.findings": []
"errors": []
"warnings": []
exit_code=0
```

```text
$ python -m nox -s mcp_gateway_manifest_quality
nox > Running session mcp_gateway_manifest_quality
nox > python -m compileall backend/mcp scripts/aistock_mcp_gateway.py scripts/aistock_mcp_gateway_doctor.py
nox > python scripts/aistock_mcp_gateway.py --self-check --profile=lite
"status": "pass"
nox > python scripts/aistock_mcp_gateway_doctor.py --json
"status": "pass"
"static_no_llm": { "findings": [] }
nox > python -m pytest tests/mcp -q -p no:cacheprovider
......................                                                   [100%]
22 passed in 6.51s
nox > Session mcp_gateway_manifest_quality was successful in 12 seconds.
exit_code=0
```

```text
$ python scripts/aistock_validation_catalog_integrity.py --output-json F:\Dev\AIstock_artifacts\mcp_gateway_manifest_quality_catalog_integrity_after_rebase.json
"state": "passed"
"error_count": 0
"warning_count": 0
"finding_count": 0
"production_8001_touched": false
"production_db_touched": false
exit_code=0
```

### Validation Center G1

```text
POST /api/v1/validation/executions
plan_key=mcp_gateway_manifest_quality
workspace_path=F:\Dev\AIstock_worktrees\mcp-manifest-quality-20260604
expected_branch=codex/mcp-manifest-quality-20260604

job_id=valjob_20260604_064748_c3861e05
status=passed
return_code=0
workspace_commit=85ca5104
workspace_commit_full=85ca5104f9efe4187395bb51ac9deb91611a28f7
run_id=platform-mcp-gateway_20260604_064759_l2_mcp-gateway-manifest-quality_c3861e05_runner-validation__1ab0750fee
archive.run_record_path=tmp/validation/runner/jobs/history/platform-mcp-gateway/20260604_064759_l2_mcp-gateway-manifest-quality_c3861e05_runner-validation.md
```

## 5. G2 DESIGN-COMPLIANCE-001 逐项矩阵

| design_item | implementation_refs | test_or_evidence | done | gap_or_exception |
| --- | --- | --- | --- | --- |
| M1 risk override + no write-as-readonly | `backend/mcp/tool_manifest.py`、`tests/mcp/test_mcp_tool_manifest.py` | `test_manifest_risk_no_write_as_readonly` PASS；15 条 override 均有理由 | true | 无 |
| M1 plan_* 语义纠正 | `TOOL_METADATA_OVERRIDES` | 10 个 plan-only 工具 reason 指明只预览/校验，写入延后到 confirmed/apply/submit | true | 无 |
| M1 真正写/确认/长任务/外网工具 preflight_required | `TOOL_MANIFEST` | 90 个工具 `assistant_usable=preflight_required`；高风险 token 测试 PASS | true | 无 |
| M2 migration_state 不再硬编码 | `_migration_state_for(...)` | gateway/script-backed/override 推导测试 PASS；非法状态校验 PASS | true | 无 |
| M2 wrapper/deprecated 表达能力 | `MIGRATION_STATE_OVERRIDES` | `wrapper_compat` / `deprecated_pending_approval` override 测试 PASS | true | 无 |
| M3 modules 不 import scripts | `backend/mcp/modules/validation.py`、`backend/mcp/legacy_validation_adapter.py` | AST 扫描 PASS；import modules 后未新增 `scripts.aistock_mcp_server` | true | 无 |
| M3 legacy BUG/GitHub wrapper 不扩大迁移 scope | `backend/mcp/legacy_validation_adapter.py` | 保留 lazy adapter；未大范围迁移 workflow 业务代码 | true | 无 |
| Validation plan 登记并 runner_enabled | `tests/aistock_validation/catalog/test_plans.yaml`、`noxfile.py` | catalog integrity PASS；Validation Center run return_code=0 | true | 无 |
| 无 POC / 简化 / mock-only / 占位 | 全部改动 | pytest/self-check/doctor/nox/Validation Center 真跑通过 | true | 无 |
| 生产边界 | 验证日志、VC job payload | `production_8001_touched=false`；未触碰 8001/3000/19080；无 DB DDL | true | 无 |

## 6. G3 网关设计文档 §10 回填

- 已回填：`docs/architecture/aistock_mcp_unified_gateway_assistant_design_20260604.md:355`
  - 全部 209 工具进入 manifest：`PASS_R1 (85ca5104)`
- 已回填：`docs/architecture/aistock_mcp_unified_gateway_assistant_design_20260604.md:357`
  - validation 19 工具迁移/import 边界：`PASS_R1 (85ca5104)`
- 已回填：`docs/architecture/aistock_mcp_unified_gateway_assistant_design_20260604.md:360`
  - 高风险工具 preflight/approval 标定：`PASS_R1 (85ca5104)`
- 已新增/回填：`docs/architecture/aistock_mcp_unified_gateway_assistant_design_20260604.md:361`
  - migration_state 诚实推导：`PASS_R1 (85ca5104)`
- 已新增/回填：`docs/architecture/aistock_mcp_unified_gateway_assistant_design_20260604.md:362`
  - manifest quality runner 接入：`PASS_R1 (85ca5104)`
- 保持 pending：智能助手读取统一 catalog、禁止后台 LLM/daemon、standalone 默认退役；这些属于 Phase 5 或后续退役窗口，不在 R1 完成口径内。

## 7. 生产 gate 与运行影响

- `production_ddl_gate=noop`：未新增/修改 migration、schema、DDL、DB init。
- `production_frontend_dependency_gate=noop`：未修改 frontend 代码或依赖。
- `production_backend_dependency_gate=noop`：未修改 Python/Conda 依赖清单。
- `frontend=noop`：无前端变更，无需重启或构建前端。
- `backend=noop`：无生产后端依赖/DDL 变更；未重启 8001。仅为 Validation Center 在 8011 启动临时后端并已停止。
- `tdx/noop`：未触碰 19080。

## 8. 停止条件触发记录

- 未触发 “side-effect 工具无法标为非 read_only / 无法进入 preflight_required”。
- 未触发 “plan_* 工具真实副作用无法判定清楚”：10 个 plan-only override 都给出 backend 语义证据理由。
- 未触发 “M3 需大范围迁移 BUG/GitHub workflow”：采用 lazy adapter 保持 gateway-only 边界。
- 未触发 “Validation Center runner 无法在 8011/8012 或 no-backend 机制下验证”：G1 run passed，return_code=0。
- 未触发 “doctor static_no_llm.findings 非空”：`static_no_llm.findings=[]`。
- 未触发 “DESIGN-COMPLIANCE-001 任一项无法 done=true”。
- 未触发 “需要启动/停止/重启 8001/3000/19080，或需要 DB DDL / 生产 runtime 改动”。

## 9. 遗留项与 Phase 5 影响

- Phase 5 可直接消费本轮 `assistant_usable` 标定：`direct_or_catalog` 仅用于 read_only/catalog 或显式 plan-only preview；写入/确认/长任务/外网/生产邻近工具必须走 preflight。
- `assistant_chat_turn`、`assistant_build_prompt_bundle`、`assistant_build_context_pack`、`assistant_preflight_mcp_tool` 等 Research Assistant 相关工具已被标为 `preflight_required`，避免 Phase 5 默认直连高风险路径。
- 10 个 plan-only preview 工具保留直用能力，但必须保持“只预览、不写库/不改状态”的后端语义；若未来实现变为写草稿/暂存行，必须同步改 override 和测试。
- `migration_state` 当前 209 个工具全部推导为 `gateway`；未来如有 wrapper/deprecated 状态，应通过 override 显式登记并保留 reason/evidence。
- 本轮未完成也不宣称完成：Research Assistant service 切换 catalog 源、RA approval UI/API、后台 LLM/daemon 静态门禁、standalone 默认退役。
