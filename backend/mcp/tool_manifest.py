"""Static tool manifest for the AIstock MCP gateway.

The manifest is intentionally data-only. It lets lightweight catalog and
self-check commands inspect all tools without importing every business-facing
MCP module or starting the backend runtime.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class ToolManifestEntry:
    tool_name: str
    module: str
    profile_tags: tuple[str, ...]
    risk_level: str
    backend_endpoint: str
    requires_confirmation: bool
    response_budget: str
    assistant_usable: str
    migration_state: str
    acceptance_refs: tuple[str, ...] = ()


MODULE_TOOL_NAMES: dict[str, tuple[str, ...]] = {'catalog': ('mcp_gateway_health',
             'mcp_gateway_list_profiles',
             'mcp_gateway_list_modules',
             'mcp_gateway_list_tools',
             'mcp_gateway_search_tools',
             'mcp_gateway_preflight_tool'),
 'execution_policy': ('execution_policy_list_algos',
                      'execution_policy_get_algo',
                      'execution_policy_validate_for_strategy',
                      'execution_policy_get_market_state_constraints',
                      'execution_policy_plan_binding',
                      'execution_policy_bind_confirmed',
                      'execution_policy_retire_confirmed'),
 'external_research': ('external_research_search_web',
                       'external_research_search_papers',
                       'external_research_fetch_extract',
                       'external_research_save_evidence'),
 'factor_correlation': ('factor_corr_plan',
                        'factor_corr_validate_inputs',
                        'factor_corr_submit_confirmed',
                        'factor_corr_get_job',
                        'factor_corr_get_top_pairs',
                        'factor_corr_get_clusters',
                        'factor_corr_suggest_replacements',
                        'factor_corr_get_matrix_ref'),
 'factor_library': ('factor_library_list',
                    'factor_library_search',
                    'factor_library_get',
                    'factor_library_get_coverage',
                    'factor_library_get_metric_summary',
                    'factor_library_get_usage_summary',
                    'factor_library_plan_register',
                    'factor_library_register_confirmed',
                    'factor_library_plan_deprecate',
                    'factor_library_deprecate_confirmed'),
 'factor_metrics': ('factor_metrics_plan',
                    'factor_metrics_validate_inputs',
                    'factor_metrics_submit_confirmed',
                    'factor_metrics_get_job',
                    'factor_metrics_get_result',
                    'factor_metrics_compare_versions',
                    'factor_metrics_export_result_ref'),
 'local_data': ('local_data_health_overview',
                'local_data_get_dataset_status',
                'local_data_list_data_stats',
                'local_data_check_gaps',
                'local_data_compute_auto_range',
                'local_data_list_alerts',
                'local_data_get_unack_alert_count',
                'local_data_list_sync_targets',
                'local_data_get_sync_target',
                'local_data_list_sync_attempts',
                'local_data_list_jobs',
                'local_data_get_job',
                'local_data_get_job_logs',
                'local_data_cancel_job_confirmed',
                'local_data_clear_queued_jobs_confirmed',
                'local_data_delete_job_confirmed',
                'local_data_run_dataset_sync_confirmed',
                'local_data_run_incremental_confirmed',
                'local_data_run_init_confirmed',
                'local_data_run_schedule_confirmed',
                'local_data_run_single_preset_confirmed',
                'local_data_run_all_presets_confirmed',
                'local_data_refresh_stats_confirmed',
                'local_data_sync_calendar_confirmed',
                'local_data_build_sector_data_confirmed',
                'local_data_export_sector_data_confirmed',
                'local_data_sync_tushare_all_confirmed',
                'local_data_list_schedules',
                'local_data_get_schedule_defaults',
                'local_data_upsert_schedule_confirmed',
                'local_data_batch_create_schedules_confirmed',
                'local_data_toggle_schedule_confirmed',
                'local_data_delete_schedule_confirmed',
                'local_data_plan_schedule_reset',
                'local_data_apply_schedule_reset_confirmed',
                'local_data_get_preset_stats',
                'local_data_get_preset_daily_status',
                'local_data_run_source_test_confirmed',
                'local_data_list_source_test_runs',
                'local_data_list_source_test_schedules',
                'local_data_upsert_source_test_schedule_confirmed',
                'local_data_toggle_source_test_schedule_confirmed',
                'local_data_run_source_test_schedule_confirmed',
                'local_data_plan_repair',
                'local_data_apply_repair_confirmed',
                'local_data_get_repair_status',
                'local_data_explain_business_impact'),
 'model_registry': ('model_registry_list',
                    'model_registry_get',
                    'model_registry_compare_trials',
                    'model_registry_get_seed_stability',
                    'model_registry_get_hyperparam_history',
                    'model_registry_get_artifacts',
                    'model_registry_plan_register',
                    'model_registry_register_confirmed',
                    'model_registry_deprecate_confirmed'),
 'qe_archive': ('qe_archive_health',
                'qe_archive_list_runs',
                'qe_archive_get_run_quality',
                'qe_archive_list_outbox',
                'qe_archive_list_jobs',
                'qe_archive_list_skips',
                'qe_archive_backfill_preview',
                'qe_archive_backfill_execute_confirmed',
                'qe_archive_backfill_selection_preview',
                'qe_archive_backfill_selection_execute_confirmed',
                'qe_archive_get_source_status',
                'qe_archive_list_backfill_runs',
                'qe_archive_get_backfill_run',
                'qe_archive_worker_run_once_confirmed',
                'qe_archive_query_factor_usage',
                'qe_archive_query_factor_importance',
                'qe_archive_query_factor_importance_stability',
                'qe_archive_query_model_trials',
                'qe_archive_query_seed_trials',
                'qe_archive_query_hyperparam_history',
                'qe_archive_query_analytics_view_status',
                'qe_archive_query_run_leaderboard',
                'qe_archive_query_seed_robustness',
                'qe_archive_query_factor_performance',
                'qe_archive_query_model_hyperparam_seed_perf',
                'qe_archive_query_overfit_flags',
                'qe_archive_query_promotion_candidates',
                'qe_archive_query_evolution_lineage'),
 'qe_experiment': ('qe_experiment_list',
                   'qe_experiment_get',
                   'qe_experiment_get_status',
                   'qe_experiment_get_logs_tail',
                   'qe_experiment_get_enhanced_metrics',
                   'qe_experiment_get_trade_stats',
                   'qe_experiment_run_confirmed',
                   'qe_experiment_stop_confirmed',
                   'qe_custom_evo_list_tasks',
                   'qe_custom_evo_get_task',
                   'qe_custom_evo_loop_comparison',
                   'qe_custom_evo_get_loop_config',
                   'qe_custom_evo_get_loop_metrics',
                   'qe_custom_evo_get_loop_analysis',
                   'qe_custom_evo_get_config',
                   'qe_custom_evo_get_logs_tail',
                   'qe_custom_evo_run_confirmed',
                   'qe_custom_evo_delete_confirmed',
                   'qe_custom_evo_retry_loop_confirmed',
                   'qe_custom_evo_rerun_loop_confirmed',
                   'qe_custom_evo_append_loops_confirmed',
                   'qe_template_create',
                   'qe_template_get',
                   'qe_template_validate',
                   'qe_template_materialize_confirmed',
                   'qe_template_run_confirmed'),
 'research': ('research_create_experiment',
              'research_list_experiments',
              'research_get_experiment',
              'research_run_stage',
              'research_retry_stage',
              'research_get_stage_result',
              'research_compare_baseline',
              'research_list_artifact_refs',
              'research_list_backtest_records',
              'research_hmm_backfill_preview',
              'research_hmm_backfill_execute',
              'research_get_backfill_run',
              'research_get_pipeline_types',
              'research_create_issue',
              'research_promote',
              'research_reject'),
 'research_assistant': ('assistant_health',
                        'assistant_create_task',
                        'assistant_add_task_event',
                        'assistant_chat_turn',
                        'assistant_build_prompt_bundle',
                        'assistant_list_prompt_nodes',
                        'assistant_create_memory_candidate',
                        'assistant_build_context_pack',
                        'assistant_list_mcp_tools',
                        'assistant_preflight_mcp_tool',
                        'assistant_create_issue_candidate',
                        'assistant_list_approvals',
                        'assistant_create_temp_memory'),
 'strategy_governance': ('strategy_governance_list_packages',
                         'strategy_governance_get_package',
                         'strategy_governance_get_health',
                         'strategy_governance_get_selection_readiness',
                         'strategy_governance_get_paper_readiness',
                         'strategy_governance_plan_promotion',
                         'strategy_governance_plan_retirement',
                         'strategy_governance_promote_confirmed',
                         'strategy_governance_retire_confirmed'),
 'validation': ('health',
                'list_plans',
                'get_plan',
                'list_validation_runs',
                'get_validation_run',
                'list_findings',
                'list_bugs',
                'get_bug_agent_context',
                'get_module_quality_summary',
                'start_validation_execution',
                'get_validation_execution_status',
                'get_validation_execution_log',
                'report_bug',
                'mcp_github_issue_list',
                'mcp_github_issue_search',
                'mcp_github_issue_create',
                'assign_bug',
                'update_bug_status',
                'mcp_github_issue_sync_bug')}

LEGACY_TOOL_MODULES = (
    "execution_policy",
    "external_research",
    "factor_correlation",
    "factor_library",
    "factor_metrics",
    "local_data",
    "model_registry",
    "research",
    "research_assistant",
    "strategy_governance",
    "validation",
    "qe_experiment",
    "qe_archive",
)

MODULE_PROFILE_TAGS: dict[str, tuple[str, ...]] = {
    "catalog": ("lite", "full"),
    "research": ("research", "full"),
    "research_assistant": ("assistant", "full"),
    "local_data": ("data", "local_data", "full"),
    "factor_library": ("factor", "factor_library", "factor_research", "full"),
    "factor_metrics": ("factor", "factor_metrics", "factor_research", "full"),
    "factor_correlation": ("factor", "factor_correlation", "factor_research", "full"),
    "model_registry": ("qe", "model_registry", "full"),
    "strategy_governance": ("trading_ops", "strategy_governance", "strategy_ops", "full"),
    "execution_policy": ("trading_ops", "execution_policy", "strategy_ops", "full"),
    "external_research": ("external_research", "full"),
    "validation": ("validation", "full"),
    "qe_experiment": ("qe", "full"),
    "qe_archive": ("qe", "full"),
}

RISK_LEVELS = {
    "read_only",
    "write_confirmed",
    "long_running",
    "production_adjacent",
    "external_network",
    "catalog",
}
MIGRATION_STATES = {"gateway", "script_backed", "wrapper_compat", "deprecated_pending_approval"}
CONFIRM_TOKENS = (
    "_confirmed",
    "research_run_stage",
    "research_retry_stage",
    "research_hmm_backfill_execute",
    "research_promote",
)
WRITE_TOKENS = (
    *CONFIRM_TOKENS,
    "_execute",
    "_create",
    "_promote",
    "_reject",
    "_retire",
    "_bind_",
    "_delete",
    "_cancel",
    "_clear",
    "_sync_",
    "_upsert",
    "_toggle",
    "_apply",
    "report_bug",
    "assign_bug",
    "update_bug_status",
    "start_validation_execution",
    "mcp_github_issue_create",
    "mcp_github_issue_sync_bug",
)
LONG_RUNNING_TOKENS = ("run", "execution", "backfill", "sync", "job", "worker", "custom_evo")
EXTERNAL_NETWORK_PREFIXES = ("external_research_", "mcp_github_issue_")


def _risk_for(tool_name: str, module: str) -> str:
    if module == "catalog":
        return "catalog"
    if tool_name.startswith(EXTERNAL_NETWORK_PREFIXES):
        return "external_network"
    if any(token in tool_name for token in CONFIRM_TOKENS):
        if any(token in tool_name for token in LONG_RUNNING_TOKENS):
            return "long_running"
        return "write_confirmed"
    if any(token in tool_name for token in WRITE_TOKENS):
        if any(token in tool_name for token in LONG_RUNNING_TOKENS):
            return "long_running"
        return "production_adjacent"
    if module in {"execution_policy", "strategy_governance", "local_data", "validation", "qe_experiment", "qe_archive"}:
        if any(token in tool_name for token in ("run", "sync", "repair", "schedule", "promote", "retire", "bind")):
            return "production_adjacent"
    return "read_only"


def _requires_confirmation(tool_name: str) -> bool:
    return any(token in tool_name for token in CONFIRM_TOKENS)


def _response_budget_for(tool_name: str) -> str:
    if any(token in tool_name for token in ("list", "search", "query", "runs", "logs", "tail")):
        return "summary_or_paginated"
    if any(token in tool_name for token in ("get", "health", "status")):
        return "single_resource"
    return "bounded_json"


def _assistant_usable_for(risk_level: str) -> str:
    if risk_level in {"read_only", "catalog"}:
        return "direct_or_catalog"
    return "preflight_required"


def _backend_endpoint_for(tool_name: str, module: str) -> str:
    if module == "catalog":
        return "mcp-gateway/catalog"
    if module == "validation":
        return "validation/*"
    if module == "qe_experiment":
        return "quantevolver/*"
    if module == "qe_archive":
        return "qe-archive/*"
    return f"{module.replace('_', '-')}/*"


def build_tool_manifest() -> tuple[ToolManifestEntry, ...]:
    entries: list[ToolManifestEntry] = []
    for module, tool_names in MODULE_TOOL_NAMES.items():
        profile_tags = MODULE_PROFILE_TAGS[module]
        for tool_name in tool_names:
            risk_level = _risk_for(tool_name, module)
            entries.append(
                ToolManifestEntry(
                    tool_name=tool_name,
                    module=module,
                    profile_tags=profile_tags,
                    risk_level=risk_level,
                    backend_endpoint=_backend_endpoint_for(tool_name, module),
                    requires_confirmation=_requires_confirmation(tool_name),
                    response_budget=_response_budget_for(tool_name),
                    assistant_usable=_assistant_usable_for(risk_level),
                    migration_state="gateway",
                    acceptance_refs=("tests/mcp",),
                )
            )
    return tuple(entries)


TOOL_MANIFEST = build_tool_manifest()
TOOL_MANIFEST_BY_NAME = {entry.tool_name: entry for entry in TOOL_MANIFEST}


def legacy_tool_count() -> int:
    return sum(len(MODULE_TOOL_NAMES[module]) for module in LEGACY_TOOL_MODULES)


def platform_tool_count() -> int:
    return len(MODULE_TOOL_NAMES["catalog"])


def validate_manifest(entries: Iterable[ToolManifestEntry] = TOOL_MANIFEST) -> list[str]:
    errors: list[str] = []
    seen: set[str] = set()
    for entry in entries:
        if entry.tool_name in seen:
            errors.append(f"duplicate tool_name: {entry.tool_name}")
        seen.add(entry.tool_name)
        if entry.module not in MODULE_TOOL_NAMES:
            errors.append(f"unknown module for {entry.tool_name}: {entry.module}")
        if not entry.profile_tags:
            errors.append(f"missing profile_tags for {entry.tool_name}")
        if entry.risk_level not in RISK_LEVELS:
            errors.append(f"invalid risk_level for {entry.tool_name}: {entry.risk_level}")
        if entry.migration_state not in MIGRATION_STATES:
            errors.append(f"invalid migration_state for {entry.tool_name}: {entry.migration_state}")
        if not entry.backend_endpoint:
            errors.append(f"missing backend_endpoint for {entry.tool_name}")
        if not entry.response_budget:
            errors.append(f"missing response_budget for {entry.tool_name}")
        if not entry.assistant_usable:
            errors.append(f"missing assistant_usable for {entry.tool_name}")
    return errors


def manifest_for_modules(modules: Iterable[str]) -> tuple[ToolManifestEntry, ...]:
    selected = list(modules)
    return tuple(entry for module in selected for entry in TOOL_MANIFEST if entry.module == module)


def manifest_for_profile(profile: str) -> tuple[ToolManifestEntry, ...]:
    return tuple(entry for entry in TOOL_MANIFEST if profile in entry.profile_tags)


def tool_names_for_modules(modules: Iterable[str]) -> tuple[str, ...]:
    return tuple(entry.tool_name for entry in manifest_for_modules(modules))
