"""Static tool manifest for the AIstock MCP gateway.

The manifest is intentionally data-only. It lets lightweight catalog and
self-check commands inspect all tools without importing every business-facing
MCP module or starting the backend runtime.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from .profiles import GATEWAY_MODULES, SCRIPT_BACKED_SERVERS


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


@dataclass(frozen=True)
class ToolMetadataOverride:
    risk_level: str | None = None
    assistant_usable: str | None = None
    requires_confirmation: bool | None = None
    reason: str = ""

    def __post_init__(self) -> None:
        if not self.reason:
            raise ValueError("ToolMetadataOverride requires a one-line reason")


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
                   'qe_template_delete_confirmed',
                   'qe_template_run_confirmed',
                   'qe_template_create_and_run_confirmed'),
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
                'schedule_validation_from_llm_advice',
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
ASSISTANT_USABLE_VALUES = {"direct_or_catalog", "preflight_required"}
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
    "schedule_validation_from_llm_advice",
    "mcp_github_issue_create",
    "mcp_github_issue_sync_bug",
)
LONG_RUNNING_TOKENS = ("run", "execution", "backfill", "sync", "job", "worker", "custom_evo")
EXTERNAL_NETWORK_PREFIXES = ("external_research_", "mcp_github_issue_")
SIDE_EFFECT_NAME_TOKENS = (
    "_confirmed",
    "register",
    "deprecate",
    "promote",
    "retire",
    "bind",
    "apply",
    "toggle",
    "sync",
    "repair",
    "schedule",
    "report_bug",
    "assign",
    "update_bug",
    "start_validation_execution",
    "schedule_validation_from_llm_advice",
    "github_issue_create",
)
NON_DIRECT_RISK_LEVELS = {"write_confirmed", "long_running", "production_adjacent", "external_network"}

TOOL_METADATA_OVERRIDES: dict[str, ToolMetadataOverride] = {
    "external_research_search_web": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="L2.5 evidence-first read-only retrieval; results enter external.*/personal.topic.* as candidates, never direct conclusions",
    ),
    "external_research_search_papers": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="L2.5 evidence-first read-only retrieval; results enter external.*/personal.topic.* as candidates, never direct conclusions",
    ),
    "external_research_fetch_extract": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="L2.5 evidence-first read-only retrieval; results enter external.*/personal.topic.* as candidates, never direct conclusions",
    ),
    "factor_library_plan_register": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="plan-only preview: backend /factor-library/register-plan reads duplicate state and defers writes to factor_library_register_confirmed",
    ),
    "factor_library_plan_deprecate": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="plan-only preview: backend /factor-library/deprecate-plan reads target state and defers writes to factor_library_deprecate_confirmed",
    ),
    "factor_metrics_plan": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="plan-only preview: backend /factor-metrics/plan only reads factor eligibility and defers async job creation to factor_metrics_submit_confirmed",
    ),
    "factor_corr_plan": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="plan-only preview: backend /factor-correlation/plan only reads factor eligibility and defers async job creation to factor_corr_submit_confirmed",
    ),
    "model_registry_plan_register": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="plan-only preview: backend /model-registry/register-plan returns payload summary and defers registry writes to model_registry_register_confirmed",
    ),
    "strategy_governance_plan_promotion": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="plan-only preview: backend promotion-plan reads package health and marks status_transition_only before promote_confirmed writes",
    ),
    "strategy_governance_plan_retirement": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="plan-only preview: backend retirement-plan reads package detail and defers status transition to retire_confirmed",
    ),
    "execution_policy_plan_binding": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="plan-only preview: backend /execution-policy/binding-plan validates and reports will_create/will_enable without persisting a policy binding",
    ),
    "local_data_plan_schedule_reset": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="plan-only preview: LocalDataManagementService.plan_schedule_reset returns reset actions and summary says not written",
    ),
    "local_data_plan_repair": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="plan-only preview: LocalDataManagementService.plan_repair inspects overview/status and defers execution to local_data_apply_repair_confirmed",
    ),
    "local_data_list_sync_targets": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="read-only GET evidence: backend/mcp/modules/local_data.py:209 uses GET /targets and LocalDataManagementService.list_sync_targets returns risk_level=read_only",
    ),
    "local_data_get_sync_target": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="read-only GET evidence: backend/mcp/modules/local_data.py:223 uses GET /targets/{target_id} and service returns risk_level=read_only",
    ),
    "local_data_list_sync_attempts": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="read-only GET evidence: backend/mcp/modules/local_data.py:230 uses GET /sync-attempts and service returns risk_level=read_only",
    ),
    "local_data_list_schedules": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="read-only GET evidence: backend/mcp/modules/local_data.py:427 uses GET /schedules and LocalDataManagementService.list_schedules uses read_only",
    ),
    "local_data_get_schedule_defaults": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="read-only GET evidence: backend/mcp/modules/local_data.py:441 uses GET /schedules/defaults and service returns risk_level=read_only",
    ),
    "local_data_list_source_test_runs": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="read-only GET evidence: backend/mcp/modules/local_data.py:530 uses GET /testing/runs and service list_source_test_runs uses read_only",
    ),
    "local_data_list_source_test_schedules": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="read-only GET evidence: backend/mcp/modules/local_data.py:544 uses GET /testing/schedules and service list_source_test_schedules uses read_only",
    ),
    "local_data_get_repair_status": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="read-only status evidence: LocalDataManagementService.get_repair_status returns risk_level=read_only and only summarizes overview/jobs/targets",
    ),
    "qe_archive_list_runs": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="read-only GET evidence: backend/mcp/modules/qe_archive.py:93 uses GET /runs and repository list_runs returns recent archived runs",
    ),
    "qe_archive_get_run_quality": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="read-only GET evidence: backend/mcp/modules/qe_archive.py:97 uses GET /runs/{run_id}/quality and repository returns row-count quality checks",
    ),
    "qe_archive_list_backfill_runs": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="read-only GET evidence: backend/mcp/modules/qe_archive.py:204 uses GET /backfill/runs and router exposes a GET list endpoint",
    ),
    "qe_archive_get_backfill_run": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="read-only GET evidence: backend/mcp/modules/qe_archive.py:208 uses GET /backfill/runs/{backfill_run_id} and router exposes a GET detail endpoint",
    ),
    "qe_archive_query_run_leaderboard": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="read-only GET evidence: backend/mcp/modules/qe_archive.py:274 uses GET /analytics/run-leaderboard and repository only queries leaderboard rows",
    ),
    "list_validation_runs": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="read-only GET evidence: backend/mcp/modules/validation.py:58 uses GET /runs and validation router lists history runs",
    ),
    "get_validation_run": ToolMetadataOverride(
        risk_level="read_only",
        assistant_usable="direct_or_catalog",
        requires_confirmation=False,
        reason="read-only GET evidence: backend/mcp/modules/validation.py:71 uses GET /runs/{run_id} and validation router returns run detail",
    ),
    "assistant_add_task_event": ToolMetadataOverride(
        risk_level="production_adjacent",
        assistant_usable="preflight_required",
        requires_confirmation=False,
        reason="writes Research Assistant task_events and may update task status through the backend API",
    ),
    "assistant_chat_turn": ToolMetadataOverride(
        risk_level="external_network",
        assistant_usable="preflight_required",
        requires_confirmation=False,
        reason="creates task/conversation records and can invoke model-provider network calls through the Research Assistant backend",
    ),
    "assistant_build_prompt_bundle": ToolMetadataOverride(
        risk_level="production_adjacent",
        assistant_usable="preflight_required",
        requires_confirmation=False,
        reason="writes prompt_bundles and optional task_events instead of returning a pure preview",
    ),
    "assistant_build_context_pack": ToolMetadataOverride(
        risk_level="production_adjacent",
        assistant_usable="preflight_required",
        requires_confirmation=False,
        reason="writes context_packs and memory_access_log rows while assembling context",
    ),
    "assistant_preflight_mcp_tool": ToolMetadataOverride(
        risk_level="production_adjacent",
        assistant_usable="preflight_required",
        requires_confirmation=False,
        reason="records mcp_tool_events and optional task_events even though it performs preflight checks",
    ),
}
MIGRATION_STATE_OVERRIDES: dict[str, str] = {}


def _base_risk_for(tool_name: str, module: str) -> str:
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


def _risk_for(tool_name: str, module: str) -> str:
    override = TOOL_METADATA_OVERRIDES.get(tool_name)
    if override and override.risk_level is not None:
        return override.risk_level
    return _base_risk_for(tool_name, module)


def _requires_confirmation(tool_name: str) -> bool:
    override = TOOL_METADATA_OVERRIDES.get(tool_name)
    if override and override.requires_confirmation is not None:
        return override.requires_confirmation
    return any(token in tool_name for token in CONFIRM_TOKENS)


def _response_budget_for(tool_name: str) -> str:
    if any(token in tool_name for token in ("list", "search", "query", "runs", "logs", "tail")):
        return "summary_or_paginated"
    if any(token in tool_name for token in ("get", "health", "status")):
        return "single_resource"
    return "bounded_json"


def _assistant_usable_for(tool_name: str, risk_level: str) -> str:
    override = TOOL_METADATA_OVERRIDES.get(tool_name)
    if override and override.assistant_usable is not None:
        return override.assistant_usable
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


def _migration_state_for(
    tool_name: str,
    module: str,
    *,
    gateway_modules: Iterable[str] | None = None,
    script_backed_servers: Iterable[str] | None = None,
    overrides: Mapping[str, str] | None = None,
) -> str:
    selected_overrides = MIGRATION_STATE_OVERRIDES if overrides is None else overrides
    if tool_name in selected_overrides:
        return selected_overrides[tool_name]
    gateway = set(GATEWAY_MODULES if gateway_modules is None else gateway_modules)
    script_backed = set(SCRIPT_BACKED_SERVERS if script_backed_servers is None else script_backed_servers)
    if module in gateway:
        return "gateway"
    if module in script_backed:
        return "script_backed"
    raise ValueError(
        f"cannot derive migration_state for {tool_name!r}: "
        f"module={module!r} is neither gateway-backed nor script-backed"
    )


def build_tool_manifest() -> tuple[ToolManifestEntry, ...]:
    entries: list[ToolManifestEntry] = []
    for module, tool_names in MODULE_TOOL_NAMES.items():
        profile_tags = MODULE_PROFILE_TAGS[module]
        for tool_name in tool_names:
            risk_level = _risk_for(tool_name, module)
            migration_state = _migration_state_for(tool_name, module)
            entries.append(
                ToolManifestEntry(
                    tool_name=tool_name,
                    module=module,
                    profile_tags=profile_tags,
                    risk_level=risk_level,
                    backend_endpoint=_backend_endpoint_for(tool_name, module),
                    requires_confirmation=_requires_confirmation(tool_name),
                    response_budget=_response_budget_for(tool_name),
                    assistant_usable=_assistant_usable_for(tool_name, risk_level),
                    migration_state=migration_state,
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


def _has_read_only_override_exemption(tool_name: str) -> bool:
    override = TOOL_METADATA_OVERRIDES.get(tool_name)
    return bool(
        override
        and override.risk_level == "read_only"
        and override.assistant_usable == "direct_or_catalog"
        and ("plan-only preview" in override.reason or "read-only" in override.reason.lower() or "GET " in override.reason)
    )


def _has_side_effect_name_token(tool_name: str) -> bool:
    return any(token in tool_name for token in SIDE_EFFECT_NAME_TOKENS)


def validate_manifest(entries: Iterable[ToolManifestEntry] = TOOL_MANIFEST) -> list[str]:
    errors: list[str] = []
    seen: set[str] = set()
    entry_list = list(entries)
    entry_names = {entry.tool_name for entry in entry_list}
    for tool_name, override in sorted(TOOL_METADATA_OVERRIDES.items()):
        if tool_name not in MODULE_TOOL_NAMES.get("catalog", ()) and tool_name not in entry_names and entries is TOOL_MANIFEST:
            errors.append(f"metadata override references unknown tool: {tool_name}")
        if override.risk_level is not None and override.risk_level not in RISK_LEVELS:
            errors.append(f"metadata override invalid risk_level for {tool_name}: {override.risk_level}")
        if override.assistant_usable is not None and override.assistant_usable not in ASSISTANT_USABLE_VALUES:
            errors.append(f"metadata override invalid assistant_usable for {tool_name}: {override.assistant_usable}")
        if not override.reason.strip():
            errors.append(f"metadata override missing reason for {tool_name}")
    for tool_name, migration_state in sorted(MIGRATION_STATE_OVERRIDES.items()):
        if tool_name not in entry_names and entries is TOOL_MANIFEST:
            errors.append(f"migration_state override references unknown tool: {tool_name}")
        if migration_state not in MIGRATION_STATES:
            errors.append(f"migration_state override invalid state for {tool_name}: {migration_state}")
    for entry in entry_list:
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
        if entry.assistant_usable not in ASSISTANT_USABLE_VALUES:
            errors.append(f"invalid assistant_usable for {entry.tool_name}: {entry.assistant_usable}")
        if entry.risk_level in NON_DIRECT_RISK_LEVELS and entry.assistant_usable != "preflight_required":
            errors.append(f"high-risk tool must require preflight: {entry.tool_name}")
        if entry.risk_level == "read_only" and entry.assistant_usable != "direct_or_catalog":
            errors.append(f"read_only tool should be direct_or_catalog unless reclassified: {entry.tool_name}")
        if _has_side_effect_name_token(entry.tool_name) and entry.risk_level == "read_only" and not _has_read_only_override_exemption(entry.tool_name):
            errors.append(f"side-effect-looking tool is read_only without explicit read-only override: {entry.tool_name}")
    return errors


def manifest_for_modules(modules: Iterable[str]) -> tuple[ToolManifestEntry, ...]:
    selected = list(modules)
    return tuple(entry for module in selected for entry in TOOL_MANIFEST if entry.module == module)


def manifest_for_profile(profile: str) -> tuple[ToolManifestEntry, ...]:
    return tuple(entry for entry in TOOL_MANIFEST if profile in entry.profile_tags)


def tool_names_for_modules(modules: Iterable[str]) -> tuple[str, ...]:
    return tuple(entry.tool_name for entry in manifest_for_modules(modules))
