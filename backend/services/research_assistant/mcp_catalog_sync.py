"""Research Assistant MCP catalog synchronization source.

This file is the assistant-facing catalog seed for all AIstock MCP servers. It
keeps the runtime catalog aligned with script-based legacy MCP servers and the
unified MCP gateway modules without requiring the assistant to guess tools from
natural-language prompts.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from backend.mcp.tool_manifest import TOOL_MANIFEST, ToolManifestEntry
from backend.services.research_assistant.domain_ontology import McpDomain, all_domain_specs

SERVER_DEFS: tuple[dict[str, Any], ...] = ({'server_key': 'aistock-gateway-lite',
  'title': 'AIstock Gateway Catalog MCP',
  'display_name_zh': 'MCP网关目录',
  'business_aliases_zh': ['MCP目录', '工具发现', 'profile预检'],
  'domain': 'mcp_capability',
  'module': 'catalog',
  'summary_zh': 'Unified MCP gateway health, profile, module, tool catalog and preflight metadata'},
 {'server_key': 'research-assistant',
  'title': 'Research Assistant MCP',
  'display_name_zh': '智能助理',
  'business_aliases_zh': ['研究助理', '助手工具目录', 'MCP能力目录'],
  'domain': 'mcp_capability',
  'module': 'research_assistant',
  'summary_zh': 'Assistant task, prompt, memory, tool catalog and preflight orchestration'},
 {'server_key': 'aistock-research',
  'title': 'Research Pipeline MCP',
  'display_name_zh': '研究流水线',
  'business_aliases_zh': ['研究管线', '实验流水线', 'Research Pipeline'],
  'domain': 'research_pipeline',
  'module': 'research',
  'summary_zh': 'Research pipeline experiments, stages, artifact refs and backtest records'},
 {'server_key': 'aistock-local-data',
  'title': 'Local Data Management MCP',
  'display_name_zh': '本地数据管理',
  'business_aliases_zh': ['本地数据', '数据同步', '数据集健康'],
  'domain': 'local_data',
  'module': 'local_data',
  'summary_zh': 'Local market-data readiness, sync, schedules, jobs and repair plans'},
 {'server_key': 'aistock-qe-experiment',
  'title': 'QE Experiment MCP',
  'display_name_zh': 'QE实验',
  'business_aliases_zh': ['量化实验', 'QE模板', '自定义进化'],
  'domain': 'qe_experiment',
  'module': 'qe_experiment',
  'summary_zh': 'QE experiment, custom_evo, loop comparison, template materialization and run management'},
 {'server_key': 'aistock-qe-archive',
  'title': 'QE Archive MCP',
  'display_name_zh': 'QE数仓',
  'business_aliases_zh': ['数仓', '归档', '入仓', 'Archive'],
  'domain': 'qe_warehouse',
  'module': 'qe_archive',
  'summary_zh': 'QE archive warehouse, outbox, backfill and archive queries'},
 {'server_key': 'aistock-validation',
  'title': 'Validation / Issue MCP',
  'display_name_zh': '验证与Issue',
  'business_aliases_zh': ['问题单', 'BUG流程', '验证中心'],
  'domain': 'validation_issue',
  'module': 'validation',
  'summary_zh': 'Validation Center, BUG JSON and GitHub issue list/search/create/sync'},
 {'server_key': 'aistock-factor-library',
  'title': 'Factor Library MCP',
  'display_name_zh': '因子库',
  'business_aliases_zh': ['因子目录', '因子列表', '因子元数据'],
  'domain': 'factor_library',
  'module': 'factor_library',
  'summary_zh': 'Factor catalog, metadata, coverage, metric summary and lifecycle plans'},
 {'server_key': 'aistock-factor-metrics',
  'title': 'Factor Metrics MCP',
  'display_name_zh': '因子独立指标',
  'business_aliases_zh': ['因子指标计算', 'RankIC', 'IC', '因子评价'],
  'domain': 'factor_metrics',
  'module': 'factor_metrics',
  'summary_zh': 'Factor independent metrics plan, validate, submit, job, result and export refs'},
 {'server_key': 'aistock-factor-correlation',
  'title': 'Factor Correlation MCP',
  'display_name_zh': '因子相关性',
  'business_aliases_zh': ['相关矩阵', '高相关因子', '冗余因子'],
  'domain': 'factor_correlation',
  'module': 'factor_correlation',
  'summary_zh': 'Factor correlation top pairs, clusters, replacement suggestions and matrix refs'},
 {'server_key': 'aistock-model-registry',
  'title': 'Model Registry MCP',
  'display_name_zh': '模型库',
  'business_aliases_zh': ['模型注册', '模型版本', '模型试验', '模型产物'],
  'domain': 'model_registry',
  'module': 'model_registry',
  'summary_zh': 'Model registry trials, seed stability, hyperparameter history, artifact refs and lifecycle'},
 {'server_key': 'aistock-strategy-governance',
  'title': 'Strategy Governance MCP',
  'display_name_zh': '策略库',
  'business_aliases_zh': ['策略包', '策略治理', '选股就绪', '模拟盘就绪'],
  'domain': 'strategy_governance',
  'module': 'strategy_governance',
  'summary_zh': 'StrategyPackage health, Selection/Paper readiness, promotion and retirement'},
 {'server_key': 'aistock-external-research',
  'title': 'External Research MCP',
  'display_name_zh': 'External Research',
  'business_aliases_zh': ['external search', 'web search', 'paper search', 'academic search'],
  'domain': 'external_research',
  'module': 'external_research',
  'summary_zh': 'External web search, academic paper search, fetch/extract, and draft evidence candidates'},
 {'server_key': 'aistock-execution-policy',
  'title': 'Execution Policy MCP',
  'display_name_zh': '执行策略库',
  'business_aliases_zh': ['执行策略', '分钟算法', 'TWAP', 'VWAP', 'POV'],
  'domain': 'execution_policy',
  'module': 'execution_policy',
  'summary_zh': 'Execution policy library, minute algos, market-state constraints and binding validation'})

MODULE_SERVER_KEYS: dict[str, str] = {'catalog': 'aistock-gateway-lite',
 'execution_policy': 'aistock-execution-policy',
 'external_research': 'aistock-external-research',
 'factor_correlation': 'aistock-factor-correlation',
 'factor_library': 'aistock-factor-library',
 'factor_metrics': 'aistock-factor-metrics',
 'local_data': 'aistock-local-data',
 'model_registry': 'aistock-model-registry',
 'qe_archive': 'aistock-qe-archive',
 'qe_experiment': 'aistock-qe-experiment',
 'research': 'aistock-research',
 'research_assistant': 'research-assistant',
 'strategy_governance': 'aistock-strategy-governance',
 'validation': 'aistock-validation'}
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

MANIFEST_RISK_TO_RA_PROFILE: dict[str, tuple[str, str]] = {
    "catalog": ("low", "read_only"),
    "read_only": ("low", "read_only"),
    "external_network": ("medium", "read_only"),
    "production_adjacent": ("high", "write_nonprod"),
    "write_confirmed": ("production_sensitive", "production_sensitive"),
    "long_running": ("high", "high_cost_compute"),
}

RA_TOOL_METADATA_OVERRIDES: dict[str, dict[str, Any]] = {
    # Preserve existing Research Assistant draft semantics while still exposing
    # the unified gateway manifest in preflight metadata.
    "assistant_create_issue_candidate": {"risk_level": "low", "side_effect_level": "read_only", "requires_approval": False},
    "qe_template_create": {"risk_level": "medium", "side_effect_level": "draft_only", "requires_approval": False},
    "qe_template_validate": {"risk_level": "medium", "side_effect_level": "write_nonprod", "requires_approval": False},
    "local_data_apply_repair_confirmed": {"risk_level": "production_sensitive", "side_effect_level": "production_sensitive"},
    "factor_metrics_submit_confirmed": {"risk_level": "high", "side_effect_level": "high_cost_compute"},
    "factor_corr_submit_confirmed": {"risk_level": "high", "side_effect_level": "high_cost_compute"},
    "execution_policy_bind_confirmed": {"risk_level": "production_sensitive", "side_effect_level": "production_sensitive"},
    "execution_policy_retire_confirmed": {"risk_level": "production_sensitive", "side_effect_level": "production_sensitive"},
    "external_research_search_web": {"risk_level": "low", "side_effect_level": "read_only", "requires_approval": False},
    "external_research_search_papers": {"risk_level": "low", "side_effect_level": "read_only", "requires_approval": False},
    "external_research_fetch_extract": {"risk_level": "low", "side_effect_level": "read_only", "requires_approval": False},
    "external_research_save_evidence": {"risk_level": "medium", "side_effect_level": "draft_only", "requires_approval": False},
    "mcp_github_issue_create": {"risk_level": "high", "side_effect_level": "write_nonprod"},
    "mcp_github_issue_sync_bug": {"risk_level": "high", "side_effect_level": "write_nonprod"},
}


def _server_def(server_key: str) -> dict[str, Any]:
    for server in SERVER_DEFS:
        if server["server_key"] == server_key:
            return server
    return {"server_key": server_key, "title": server_key, "domain": "unknown", "summary_zh": ""}


def _server_key_for_manifest_entry(entry: ToolManifestEntry) -> str:
    server_key = MODULE_SERVER_KEYS.get(entry.module)
    if not server_key:
        raise KeyError(f"MCP manifest module is not mapped to a Research Assistant server: {entry.module}")
    return server_key


def _side_effect_requires_approval(side_effect_level: str, risk_level: str) -> bool:
    return side_effect_level in {"write_nonprod", "high_cost_compute", "production_sensitive"} or risk_level in {"high", "production_sensitive"}


def _ra_profile_for_entry(entry: ToolManifestEntry) -> tuple[str, str, bool]:
    risk, side_effect = MANIFEST_RISK_TO_RA_PROFILE.get(entry.risk_level, ("medium", "read_only"))
    if entry.module == "qe_experiment" and ("run" in entry.tool_name or "materialize" in entry.tool_name):
        risk = "high" if not entry.requires_confirmation else "production_sensitive"
        side_effect = "high_cost_compute"
    if entry.tool_name in {"mcp_github_issue_create", "mcp_github_issue_sync_bug", "update_bug_status", "assign_bug", "start_validation_execution"}:
        risk = "high"
        side_effect = "write_nonprod"
    override = RA_TOOL_METADATA_OVERRIDES.get(entry.tool_name)
    if override:
        risk = str(override.get("risk_level", risk))
        side_effect = str(override.get("side_effect_level", side_effect))
    requires_approval = bool(entry.requires_confirmation) or _side_effect_requires_approval(side_effect, risk)
    if override and "requires_approval" in override:
        requires_approval = bool(override["requires_approval"])
    return risk, side_effect, requires_approval


def _tool_names_by_server() -> dict[str, tuple[str, ...]]:
    grouped: defaultdict[str, list[str]] = defaultdict(list)
    for entry in TOOL_MANIFEST:
        grouped[_server_key_for_manifest_entry(entry)].append(entry.tool_name)
    return {server_key: tuple(tool_names) for server_key, tool_names in grouped.items()}


TOOL_NAMES_BY_SERVER: dict[str, tuple[str, ...]] = _tool_names_by_server()


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


def _tool_metadata(server_key: str, tool_name: str, manifest_entry: ToolManifestEntry) -> dict[str, Any]:
    server = _server_def(server_key)
    risk, side_effect, requires_approval = _ra_profile_for_entry(manifest_entry)
    required_inputs = list(REQUIRED_INPUTS_BY_TOOL.get(tool_name, []))
    schema: dict[str, Any] = {"type": "object"}
    if required_inputs:
        schema["required"] = required_inputs
    default_checks = [
        "gateway_manifest",
        "payload_budget",
        "profile_recommendation",
        "confirmation" if manifest_entry.requires_confirmation else "read_or_plan_boundary",
    ]
    return {
        "server_key": server_key,
        "tool_name": tool_name,
        "title": tool_name.replace("_", " "),
        "description": f"{server.get('summary_zh') or server_key}: {tool_name}",
        "risk_level": risk,
        "side_effect_level": side_effect,
        "requires_approval": requires_approval,
        "input_schema_json": schema,
        "output_schema_json": {"type": "object"},
        "preflight_schema_json": {
            "checks": PREFLIGHT_CHECKS_BY_TOOL.get(tool_name, default_checks),
            "gateway_manifest": {
                "module": manifest_entry.module,
                "profile_tags": list(manifest_entry.profile_tags),
                "risk_level": manifest_entry.risk_level,
                "backend_endpoint": manifest_entry.backend_endpoint,
                "requires_confirmation": manifest_entry.requires_confirmation,
                "response_budget": manifest_entry.response_budget,
                "assistant_usable": manifest_entry.assistant_usable,
                "migration_state": manifest_entry.migration_state,
                "acceptance_refs": list(manifest_entry.acceptance_refs),
            },
            "recommended_profile_tags": list(manifest_entry.profile_tags),
        },
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
    for entry in TOOL_MANIFEST:
        server_key = _server_key_for_manifest_entry(entry)
        tools.append(_tool_metadata(server_key, entry.tool_name, entry))
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
        if spec.domain == McpDomain.MCP_CAPABILITY:
            refs.extend(
                {"server_key": "aistock-gateway-lite", "tool_name": tool}
                for tool in TOOL_NAMES_BY_SERVER.get("aistock-gateway-lite", ())
            )
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
