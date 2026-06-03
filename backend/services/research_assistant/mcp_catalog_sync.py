"""Research Assistant MCP catalog synchronization source.

This file is the assistant-facing catalog seed for all AIstock MCP servers. It
keeps the runtime catalog aligned with script-based legacy MCP servers and the
unified MCP gateway modules without requiring the assistant to guess tools from
natural-language prompts.
"""

from __future__ import annotations

from typing import Any

from backend.mcp.modules import external_research, local_data, research, research_assistant

try:  # New gateway modules are imported after their files are generated in this branch.
    from backend.mcp.modules import execution_policy, factor_correlation, factor_library, factor_metrics, model_registry, strategy_governance
except ImportError:  # pragma: no cover - keeps early static imports usable during partial generation.
    execution_policy = factor_correlation = factor_library = factor_metrics = model_registry = strategy_governance = None
from backend.services.research_assistant.domain_ontology import McpDomain, all_domain_specs

SERVER_DEFS: tuple[dict[str, Any], ...] = (
    {"server_key": "research-assistant", "title": "Research Assistant MCP", "display_name_zh": "智能助理", "business_aliases_zh": ["研究助理", "助手工具目录", "MCP能力目录"], "domain": "mcp_capability", "module": "research_assistant", "summary_zh": "Assistant task, prompt, memory, tool catalog and preflight orchestration"},
    {"server_key": "aistock-research", "title": "Research Pipeline MCP", "display_name_zh": "研究流水线", "business_aliases_zh": ["研究管线", "实验流水线", "Research Pipeline"], "domain": "research_pipeline", "module": "research", "summary_zh": "Research pipeline experiments, stages, artifact refs and backtest records"},
    {"server_key": "aistock-local-data", "title": "Local Data Management MCP", "display_name_zh": "本地数据管理", "business_aliases_zh": ["本地数据", "数据同步", "数据集健康"], "domain": "local_data", "module": "local_data", "summary_zh": "Local market-data readiness, sync, schedules, jobs and repair plans"},
    {"server_key": "aistock-qe-experiment", "title": "QE Experiment MCP", "display_name_zh": "QE实验", "business_aliases_zh": ["量化实验", "QE模板", "自定义进化"], "domain": "qe_experiment", "module": "legacy_script", "summary_zh": "QE experiment, custom_evo, loop comparison, template materialization and run management"},
    {"server_key": "aistock-qe-archive", "title": "QE Archive MCP", "display_name_zh": "QE数仓", "business_aliases_zh": ["数仓", "归档", "入仓", "Archive"], "domain": "qe_warehouse", "module": "legacy_script", "summary_zh": "QE archive warehouse, outbox, backfill and archive queries"},
    {"server_key": "aistock-validation", "title": "Validation / Issue MCP", "display_name_zh": "验证与Issue", "business_aliases_zh": ["问题单", "BUG流程", "验证中心"], "domain": "validation_issue", "module": "legacy_script", "summary_zh": "Validation Center, BUG JSON and GitHub issue list/search/create/sync"},
    {"server_key": "aistock-factor-library", "title": "Factor Library MCP", "display_name_zh": "因子库", "business_aliases_zh": ["因子目录", "因子列表", "因子元数据"], "domain": "factor_library", "module": "factor_library", "summary_zh": "Factor catalog, metadata, coverage, metric summary and lifecycle plans"},
    {"server_key": "aistock-factor-metrics", "title": "Factor Metrics MCP", "display_name_zh": "因子独立指标", "business_aliases_zh": ["因子指标计算", "RankIC", "IC", "因子评价"], "domain": "factor_metrics", "module": "factor_metrics", "summary_zh": "Factor independent metrics plan, validate, submit, job, result and export refs"},
    {"server_key": "aistock-factor-correlation", "title": "Factor Correlation MCP", "display_name_zh": "因子相关性", "business_aliases_zh": ["相关矩阵", "高相关因子", "冗余因子"], "domain": "factor_correlation", "module": "factor_correlation", "summary_zh": "Factor correlation top pairs, clusters, replacement suggestions and matrix refs"},
    {"server_key": "aistock-model-registry", "title": "Model Registry MCP", "display_name_zh": "模型库", "business_aliases_zh": ["模型注册", "模型版本", "模型试验", "模型产物"], "domain": "model_registry", "module": "model_registry", "summary_zh": "Model registry trials, seed stability, hyperparameter history, artifact refs and lifecycle"},
    {"server_key": "aistock-strategy-governance", "title": "Strategy Governance MCP", "display_name_zh": "策略库", "business_aliases_zh": ["策略包", "策略治理", "选股就绪", "模拟盘就绪"], "domain": "strategy_governance", "module": "strategy_governance", "summary_zh": "StrategyPackage health, Selection/Paper readiness, promotion and retirement"},
    {"server_key": "aistock-external-research", "title": "External Research MCP", "display_name_zh": "External Research", "business_aliases_zh": ["external search", "web search", "paper search", "academic search"], "domain": "external_research", "module": "external_research", "summary_zh": "External web search, academic paper search, fetch/extract, and draft evidence candidates"},
    {"server_key": "aistock-execution-policy", "title": "Execution Policy MCP", "display_name_zh": "执行策略库", "business_aliases_zh": ["执行策略", "分钟算法", "TWAP", "VWAP", "POV"], "domain": "execution_policy", "module": "execution_policy", "summary_zh": "Execution policy library, minute algos, market-state constraints and binding validation"},
)

TOOL_NAMES_BY_SERVER: dict[str, tuple[str, ...]] = {
    "research-assistant": tuple(research_assistant.TOOL_NAMES),
    "aistock-research": tuple(research.TOOL_NAMES),
    "aistock-local-data": tuple(local_data.TOOL_NAMES),
    "aistock-qe-experiment": (
        "qe_experiment_list",
        "qe_experiment_get",
        "qe_experiment_get_status",
        "qe_experiment_get_logs_tail",
        "qe_experiment_get_enhanced_metrics",
        "qe_experiment_get_trade_stats",
        "qe_experiment_run_confirmed",
        "qe_experiment_stop_confirmed",
        "qe_custom_evo_list_tasks",
        "qe_custom_evo_get_task",
        "qe_custom_evo_loop_comparison",
        "qe_custom_evo_get_loop_config",
        "qe_custom_evo_get_loop_metrics",
        "qe_custom_evo_get_loop_analysis",
        "qe_custom_evo_get_config",
        "qe_custom_evo_get_logs_tail",
        "qe_custom_evo_run_confirmed",
        "qe_custom_evo_delete_confirmed",
        "qe_custom_evo_retry_loop_confirmed",
        "qe_custom_evo_rerun_loop_confirmed",
        "qe_custom_evo_append_loops_confirmed",
        "qe_template_create",
        "qe_template_get",
        "qe_template_validate",
        "qe_template_materialize_confirmed",
        "qe_template_run_confirmed",
    ),
    "aistock-qe-archive": (
        "qe_archive_health",
        "qe_archive_list_runs",
        "qe_archive_get_run_quality",
        "qe_archive_list_outbox",
        "qe_archive_list_jobs",
        "qe_archive_list_skips",
        "qe_archive_backfill_preview",
        "qe_archive_backfill_execute_confirmed",
        "qe_archive_backfill_selection_preview",
        "qe_archive_backfill_selection_execute_confirmed",
        "qe_archive_get_source_status",
        "qe_archive_list_backfill_runs",
        "qe_archive_get_backfill_run",
        "qe_archive_worker_run_once_confirmed",
        "qe_archive_query_factor_usage",
        "qe_archive_query_factor_importance",
        "qe_archive_query_factor_importance_stability",
        "qe_archive_query_model_trials",
        "qe_archive_query_seed_trials",
        "qe_archive_query_hyperparam_history",
        "qe_archive_query_analytics_view_status",
        "qe_archive_query_run_leaderboard",
        "qe_archive_query_seed_robustness",
        "qe_archive_query_factor_performance",
        "qe_archive_query_model_hyperparam_seed_perf",
        "qe_archive_query_overfit_flags",
        "qe_archive_query_promotion_candidates",
        "qe_archive_query_evolution_lineage",
    ),
    "aistock-validation": (
        "health",
        "list_plans",
        "get_plan",
        "list_validation_runs",
        "get_validation_run",
        "list_findings",
        "list_bugs",
        "get_bug_agent_context",
        "get_module_quality_summary",
        "start_validation_execution",
        "get_validation_execution_status",
        "get_validation_execution_log",
        "report_bug",
        "mcp_github_issue_list",
        "mcp_github_issue_search",
        "mcp_github_issue_create",
        "assign_bug",
        "update_bug_status",
        "mcp_github_issue_sync_bug",
    ),
    "aistock-factor-library": tuple(getattr(factor_library, "TOOL_NAMES", (
        "factor_library_list", "factor_library_search", "factor_library_get", "factor_library_get_coverage", "factor_library_get_metric_summary", "factor_library_get_usage_summary", "factor_library_plan_register", "factor_library_register_confirmed", "factor_library_plan_deprecate", "factor_library_deprecate_confirmed"))),
    "aistock-factor-metrics": tuple(getattr(factor_metrics, "TOOL_NAMES", (
        "factor_metrics_plan", "factor_metrics_validate_inputs", "factor_metrics_submit_confirmed", "factor_metrics_get_job", "factor_metrics_get_result", "factor_metrics_compare_versions", "factor_metrics_export_result_ref"))),
    "aistock-factor-correlation": tuple(getattr(factor_correlation, "TOOL_NAMES", (
        "factor_corr_plan", "factor_corr_validate_inputs", "factor_corr_submit_confirmed", "factor_corr_get_job", "factor_corr_get_top_pairs", "factor_corr_get_clusters", "factor_corr_suggest_replacements", "factor_corr_get_matrix_ref"))),
    "aistock-model-registry": tuple(getattr(model_registry, "TOOL_NAMES", (
        "model_registry_list", "model_registry_get", "model_registry_compare_trials", "model_registry_get_seed_stability", "model_registry_get_hyperparam_history", "model_registry_get_artifacts", "model_registry_plan_register", "model_registry_register_confirmed", "model_registry_deprecate_confirmed"))),
    "aistock-strategy-governance": tuple(getattr(strategy_governance, "TOOL_NAMES", (
        "strategy_governance_list_packages", "strategy_governance_get_package", "strategy_governance_get_health", "strategy_governance_get_selection_readiness", "strategy_governance_get_paper_readiness", "strategy_governance_plan_promotion", "strategy_governance_plan_retirement", "strategy_governance_promote_confirmed", "strategy_governance_retire_confirmed"))),
    "aistock-execution-policy": tuple(getattr(execution_policy, "TOOL_NAMES", (
        "execution_policy_list_algos", "execution_policy_get_algo", "execution_policy_validate_for_strategy", "execution_policy_get_market_state_constraints", "execution_policy_plan_binding", "execution_policy_bind_confirmed", "execution_policy_retire_confirmed"))),
    "aistock-external-research": tuple(external_research.TOOL_NAMES),
}

REQUIRED_INPUTS_BY_TOOL: dict[str, list[str]] = {
    "assistant_create_task": ["title"],
    "assistant_create_memory_candidate": ["memory_type", "subject_key", "title"],
    "assistant_create_issue_candidate": ["title", "problem_statement"],
    "qe_template_create": ["template_kind", "title", "config_json"],
    "qe_template_validate": ["template_id"],
    "qe_template_materialize_confirmed": ["template_id", "confirm_template"],
    "qe_template_run_confirmed": ["template_id", "confirm_run"],
    "local_data_get_dataset_status": ["dataset"],
    "local_data_apply_repair_confirmed": ["plan_id", "confirmation_text"],
}

PREFLIGHT_CHECKS_BY_TOOL: dict[str, list[str]] = {
    "assistant_create_issue_candidate": ["dedupe_key", "evidence_refs", "draft_only", "github_formal_issue_blocked"],
    "qe_template_create": ["schema", "fixed_seed", "draft_only"],
    "qe_template_validate": ["template_exists", "schema", "diff_summary"],
    "qe_template_materialize_confirmed": ["stock_pool", "node_health", "cost", "approval"],
    "qe_template_run_confirmed": ["materialized_template", "cost_guard", "node_health", "approval"],
    "local_data_apply_repair_confirmed": ["confirmation_text", "plan_id", "facade", "approval"],
}

DRAFT_ONLY_TOOLS = {"external_research_save_evidence"}

CONFIRMATIONS_BY_TOOL = {
    "local_data_apply_repair_confirmed": ["APPROVE_RESEARCH_ASSISTANT_ACTION"],
    "qe_template_create": ["CONFIRM_QE_DRAFT"],
    "qe_template_validate": ["CONFIRM_QE_VALIDATE"],
    "qe_template_materialize_confirmed": ["CONFIRM_QE_MATERIALIZE", "MATERIALIZE_QE_TEMPLATE"],
    "qe_template_run_confirmed": ["CONFIRM_QE_RUN", "QE_EXPERIMENT_RUN"],
    "factor_library_register_confirmed": ["REGISTER_FACTOR"],
    "factor_library_deprecate_confirmed": ["DEPRECATE_FACTOR"],
    "factor_metrics_submit_confirmed": ["SUBMIT_FACTOR_METRICS"],
    "factor_corr_submit_confirmed": ["SUBMIT_FACTOR_CORRELATION"],
    "model_registry_register_confirmed": ["REGISTER_MODEL"],
    "model_registry_deprecate_confirmed": ["DEPRECATE_MODEL"],
    "strategy_governance_promote_confirmed": ["PROMOTE_STRATEGY"],
    "strategy_governance_retire_confirmed": ["RETIRE_STRATEGY"],
    "execution_policy_bind_confirmed": ["BIND_EXECUTION_POLICY"],
    "execution_policy_retire_confirmed": ["RETIRE_EXECUTION_POLICY"],
}


def _server_def(server_key: str) -> dict[str, Any]:
    for server in SERVER_DEFS:
        if server["server_key"] == server_key:
            return server
    return {"server_key": server_key, "title": server_key, "domain": "unknown", "summary_zh": ""}


def enrich_mcp_server_record(server: dict[str, Any]) -> dict[str, Any]:
    """Overlay canonical display metadata on legacy runtime server rows."""
    item = dict(server)
    server_key = str(item.get("server_key") or "")
    canonical = _server_def(server_key)
    health = dict(item.get("health_json") if isinstance(item.get("health_json"), dict) else {})
    for key in ("display_name_zh", "business_aliases_zh", "summary_zh"):
        if not health.get(key) and canonical.get(key):
            health[key] = canonical[key]
    if canonical.get("domain") and not health.get("domain"):
        health["domain"] = canonical["domain"]
    if canonical.get("module") and not health.get("module"):
        health["module"] = canonical["module"]
    if "summary_first_payload" not in health:
        health["summary_first_payload"] = True
    item["health_json"] = health
    return item


def _tool_metadata(server_key: str, tool_name: str) -> dict[str, Any]:
    confirmed = tool_name.endswith("_confirmed")
    is_plan = "plan" in tool_name or "preview" in tool_name or "validate" in tool_name or tool_name == "qe_template_create"
    server = _server_def(server_key)
    risk = "production_sensitive" if confirmed else "medium" if is_plan else "low"
    side_effect = "production_sensitive" if confirmed else "draft_only" if is_plan else "read_only"
    if server_key == "aistock-qe-experiment" and ("run" in tool_name or "materialize" in tool_name):
        risk = "high" if not confirmed else "production_sensitive"
        side_effect = "high_cost_compute"
    if tool_name == "qe_template_create":
        risk = "medium"
        side_effect = "draft_only"
    if tool_name == "qe_template_validate":
        risk = "medium"
        side_effect = "write_nonprod"
    if server_key == "aistock-validation" and tool_name in {"mcp_github_issue_create", "mcp_github_issue_sync_bug", "update_bug_status", "assign_bug", "start_validation_execution"}:
        risk = "high"
        side_effect = "write_nonprod"
    if tool_name in {"factor_metrics_submit_confirmed", "factor_corr_submit_confirmed"}:
        side_effect = "high_cost_compute"
        risk = "high"
    if tool_name in DRAFT_ONLY_TOOLS:
        risk = "medium"
        side_effect = "draft_only"
    required_inputs = list(REQUIRED_INPUTS_BY_TOOL.get(tool_name, []))
    schema: dict[str, Any] = {"type": "object"}
    if required_inputs:
        schema["required"] = required_inputs
    return {
        "server_key": server_key,
        "tool_name": tool_name,
        "title": tool_name.replace("_", " "),
        "description": f"{server.get('summary_zh') or server_key}: {tool_name}",
        "risk_level": risk,
        "side_effect_level": side_effect,
        "requires_approval": confirmed or risk in {"high", "production_sensitive"},
        "input_schema_json": schema,
        "output_schema_json": {"type": "object"},
        "preflight_schema_json": {"checks": PREFLIGHT_CHECKS_BY_TOOL.get(tool_name, ["catalog", "payload_budget", "confirmation" if confirmed else "read_or_plan_boundary"])},
        "required_confirmations": CONFIRMATIONS_BY_TOOL.get(tool_name, []),
        "status": "enabled",
    }


def default_mcp_servers() -> list[dict[str, Any]]:
    return [
        {
            "server_key": item["server_key"],
            "title": item["title"],
            "status": "ready",
            "health_json": {
                "mode": "loopback",
                "module": item["module"],
                "domain": item["domain"],
                "display_name_zh": item["display_name_zh"],
                "business_aliases_zh": item["business_aliases_zh"],
                "summary_zh": item["summary_zh"],
                "summary_first_payload": True,
                **({"capability_key": "local_data_management"} if item["server_key"] == "aistock-local-data" else {}),
            },
        }
        for item in SERVER_DEFS
    ]


def default_mcp_tools() -> list[dict[str, Any]]:
    tools: list[dict[str, Any]] = []
    for server_key, names in TOOL_NAMES_BY_SERVER.items():
        tools.extend(_tool_metadata(server_key, name) for name in names)
    return tools


def workflow_capabilities() -> list[dict[str, Any]]:
    capabilities: list[dict[str, Any]] = []
    for spec in all_domain_specs():
        if spec.domain == McpDomain.GENERAL:
            continue
        refs = [
            {"server_key": spec.server_key, "tool_name": tool}
            for tool in [*spec.read_tools, *spec.plan_tools, *spec.confirmed_tools]
        ] or [{"server_key": spec.server_key, "tool_name": spec.default_tool}]
        capabilities.append(
            {
                "capability_key": f"{spec.domain.value}.mcp_orchestration",
                "capability_type": "mcp_tool" if not spec.plan_tools else "composite",
                "title": spec.summary_zh,
                "natural_language_triggers": list(spec.synonyms),
                "description_for_llm": f"Route natural-language requests for {spec.summary_zh} to {spec.server_key}; policy={spec.risk_policy}.",
                "risk_level": "medium" if spec.plan_tools else "low",
                "side_effect_level": "draft_only" if spec.plan_tools else "read_only",
                "required_confirmations": sorted({item for tool in spec.confirmed_tools for item in CONFIRMATIONS_BY_TOOL.get(tool, [])}),
                "preferred_model_role": "primary_orchestrator",
                "input_slots": {"required": ["request"]},
                "output_cards": ["route_decision", "summary", "evidence_refs", "next_step"],
                "mcp_tool_refs": refs,
                "skill_refs": [],
                "workflow_pack_ref": f"{spec.domain.value}.mcp_orchestration",
                "status": "approved",
                "source_ref": "docs/architecture/research_assistant_unified_mcp_natural_language_orchestration_design_20260527.md",
            }
        )
    return capabilities


def load_catalog() -> dict[str, Any]:
    servers = default_mcp_servers()
    tools = default_mcp_tools()
    return {
        "servers": servers,
        "tools": tools,
        "workflow_capabilities": workflow_capabilities(),
        "server_count": len(servers),
        "tool_count": len(tools),
        "domain_catalog": {spec.domain.value: spec.summary_zh for spec in all_domain_specs()},
    }
