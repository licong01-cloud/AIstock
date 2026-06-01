"""Domain ontology for Research Assistant MCP routing."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class McpDomain(str, Enum):
    MCP_CAPABILITY = "mcp_capability"
    LOCAL_DATA = "local_data"
    QE_EXPERIMENT = "qe_experiment"
    QE_WAREHOUSE = "qe_warehouse"
    VALIDATION_ISSUE = "validation_issue"
    RESEARCH_PIPELINE = "research_pipeline"
    FACTOR_LIBRARY = "factor_library"
    FACTOR_METRICS = "factor_metrics"
    FACTOR_CORRELATION = "factor_correlation"
    MODEL_REGISTRY = "model_registry"
    STRATEGY_GOVERNANCE = "strategy_governance"
    EXECUTION_POLICY = "execution_policy"
    EXTERNAL_RESEARCH = "external_research"
    GENERAL = "general"


@dataclass(frozen=True)
class DomainSpec:
    domain: McpDomain
    intent_value: str
    server_key: str
    default_tool: str
    risk_policy: str
    summary_zh: str
    synonyms: tuple[str, ...]
    read_tools: tuple[str, ...] = ()
    plan_tools: tuple[str, ...] = ()
    confirmed_tools: tuple[str, ...] = ()
    prompt_key: str | None = None


DOMAIN_SPECS: dict[McpDomain, DomainSpec] = {
    McpDomain.MCP_CAPABILITY: DomainSpec(
        domain=McpDomain.MCP_CAPABILITY,
        intent_value="mcp_capability_inquiry",
        server_key="research-assistant",
        default_tool="assistant_list_mcp_tools",
        risk_policy="read_only_catalog",
        summary_zh="MCP capability catalog and tool preflight",
        synonyms=("mcp", "tool", "server", "tools", "capability", "capabilities", "catalog", "what can you do", "available tools", "mcp list", "工具", "能力", "可用工具", "工具列表", "gongju", "nengli"),
        read_tools=("assistant_list_mcp_tools",),
        prompt_key="domain.mcp_capability_router",
    ),
    McpDomain.LOCAL_DATA: DomainSpec(
        domain=McpDomain.LOCAL_DATA,
        intent_value="local_data_management_request",
        server_key="aistock-local-data",
        default_tool="local_data_health_overview",
        risk_policy="read_plan_confirmed_write",
        summary_zh="Local market-data readiness, sync target, schedule, job and repair orchestration",
        synonyms=("local data", "local_data", "trade_date", "dataset", "calendar", "tushare", "source test", "sync target", "data_sync", "local-data", "本地数据", "数据同步", "交易日", "数据集", "日历", "ben di shu ju", "bendi shuju"),
        read_tools=("local_data_health_overview", "local_data_get_dataset_status", "local_data_list_sync_targets", "local_data_list_sync_attempts"),
        plan_tools=("local_data_plan_repair",),
        confirmed_tools=("local_data_apply_repair_confirmed",),
        prompt_key="prompt.local_data_management",
    ),
    McpDomain.QE_EXPERIMENT: DomainSpec(
        domain=McpDomain.QE_EXPERIMENT,
        intent_value="experiment_draft_request",
        server_key="aistock-qe-experiment",
        default_tool="qe_experiment_list",
        risk_policy="draft_validate_confirmed_run",
        summary_zh="QE experiment, custom_evo loop, template materialization and run management",
        synonyms=("qe", "quantevolver", "experiment", "custom_evo", "loop", "template", "materialize", "run experiment", "evolution", "qe task"),
        read_tools=("qe_experiment_list", "qe_experiment_get", "qe_custom_evo_get_task", "qe_custom_evo_loop_comparison"),
        plan_tools=("qe_template_create", "qe_template_validate"),
        confirmed_tools=("qe_template_materialize_confirmed", "qe_template_run_confirmed"),
        prompt_key="domain.qe_experiment",
    ),
    McpDomain.QE_WAREHOUSE: DomainSpec(
        domain=McpDomain.QE_WAREHOUSE,
        intent_value="qe_warehouse_request",
        server_key="aistock-qe-archive",
        default_tool="qe_archive_health",
        risk_policy="read_preview_confirmed_backfill",
        summary_zh="QE archive warehouse, ingestion, outbox, backfill and analytics views",
        synonyms=("warehouse", "archive", "outbox", "backfill", "archived", "archive job", "skips", "source status", "ingestion", "data warehouse", "leaderboard", "run leaderboard", "seed robustness", "promotion candidate", "overfit", "lineage", "hyperparam seed", "factor performance", "analytics view", "数仓", "入仓", "归档", "补录", "漏入库", "排行榜", "种子鲁棒性", "晋升候选", "过拟合", "演进血缘", "超参", "因子表现", "分析视图", "ruku", "shucang", "guidang", "bulu", "louruku"),
        read_tools=("qe_archive_health", "qe_archive_list_runs", "qe_archive_list_outbox", "qe_archive_get_source_status", "qe_archive_query_analytics_view_status", "qe_archive_query_run_leaderboard", "qe_archive_query_seed_robustness", "qe_archive_query_factor_performance", "qe_archive_query_model_hyperparam_seed_perf", "qe_archive_query_overfit_flags", "qe_archive_query_promotion_candidates", "qe_archive_query_evolution_lineage"),
        plan_tools=("qe_archive_backfill_preview", "qe_archive_backfill_selection_preview"),
        confirmed_tools=("qe_archive_backfill_execute_confirmed", "qe_archive_backfill_selection_execute_confirmed"),
        prompt_key="domain.qe_warehouse",
    ),
    McpDomain.VALIDATION_ISSUE: DomainSpec(
        domain=McpDomain.VALIDATION_ISSUE,
        intent_value="validation_issue_request",
        server_key="aistock-validation",
        default_tool="mcp_github_issue_search",
        risk_policy="issue_json_then_github_sync_confirmed",
        summary_zh="Validation Center, BUG JSON and GitHub issue lifecycle",
        synonyms=("bug", "issue", "github issue", "validation", "finding", "bug json", "agent context", "assign", "status", "sync issue", "close issue", "同步 issue", "同步状态", "关闭 issue", "问题单", "缺陷"),
        read_tools=("mcp_github_issue_list", "mcp_github_issue_search", "get_bug_agent_context", "list_validation_runs"),
        plan_tools=("report_bug",),
        confirmed_tools=("mcp_github_issue_create", "mcp_github_issue_sync_bug", "update_bug_status", "assign_bug"),
        prompt_key="domain.validation_issue",
    ),
    McpDomain.RESEARCH_PIPELINE: DomainSpec(
        domain=McpDomain.RESEARCH_PIPELINE,
        intent_value="research_pipeline_request",
        server_key="aistock-research",
        default_tool="research_list_experiments",
        risk_policy="read_stage_then_confirmed_stage_run",
        summary_zh="Research Pipeline experiments, stages, artifact refs, backtest records and HMM backfill",
        synonyms=("research pipeline", "research experiment", "research stage", "stage", "artifact", "artifact refs", "hmm backfill", "promote", "reject", "pipeline"),
        read_tools=("research_list_experiments", "research_get_experiment", "research_list_artifact_refs", "research_list_backtest_records"),
        plan_tools=("research_create_experiment", "research_hmm_backfill_preview"),
        confirmed_tools=("research_run_stage", "research_retry_stage", "research_promote", "research_reject"),
        prompt_key="domain.research_pipeline",
    ),
    McpDomain.FACTOR_LIBRARY: DomainSpec(
        domain=McpDomain.FACTOR_LIBRARY,
        intent_value="factor_library_request",
        server_key="aistock-factor-library",
        default_tool="factor_library_list",
        risk_policy="summary_list_detail_on_demand_confirmed_registration",
        summary_zh="Factor catalog, metadata, coverage, quality labels and usage summaries",
        synonyms=("factor library", "factor catalog", "factor list", "factor coverage", "factor quality", "factor registry", "factor metadata", "rankic", "ic", "因子库", "因子目录", "因子列表", "可用因子", "因子覆盖", "因子质量", "因子元数据", "yinzi ku", "yinzi zhibiao"),
        read_tools=("factor_library_list", "factor_library_search", "factor_library_get", "factor_library_get_coverage"),
        plan_tools=("factor_library_plan_register", "factor_library_plan_deprecate"),
        confirmed_tools=("factor_library_register_confirmed", "factor_library_deprecate_confirmed"),
        prompt_key="domain.factor_library",
    ),
    McpDomain.FACTOR_METRICS: DomainSpec(
        domain=McpDomain.FACTOR_METRICS,
        intent_value="factor_metrics_request",
        server_key="aistock-factor-metrics",
        default_tool="factor_metrics_plan",
        risk_policy="async_job_preflight_confirmed_submit",
        summary_zh="Independent factor metric calculation, IC, RankIC, group return and stability jobs",
        synonyms=("factor metrics", "independent metrics", "official evaluation", "rankic calculation", "ic calculation", "group return", "factor score", "stability", "因子独立指标", "因子指标计算", "因子评价", "因子计算", "因子得分", "稳定性", "yinzi duli zhibiao", "calculate factor"),
        read_tools=("factor_metrics_get_job", "factor_metrics_get_result", "factor_metrics_compare_versions"),
        plan_tools=("factor_metrics_plan", "factor_metrics_validate_inputs"),
        confirmed_tools=("factor_metrics_submit_confirmed",),
        prompt_key="domain.factor_metrics",
    ),
    McpDomain.FACTOR_CORRELATION: DomainSpec(
        domain=McpDomain.FACTOR_CORRELATION,
        intent_value="factor_correlation_request",
        server_key="aistock-factor-correlation",
        default_tool="factor_corr_plan",
        risk_policy="async_matrix_job_artifact_ref_only",
        summary_zh="Factor correlation, redundant pairs, clusters, replacement suggestions and matrix refs",
        synonyms=("factor correlation", "correlation", "corr", "factor corr", "top pairs", "cluster", "matrix", "replacement", "redundant factors", "因子相关性", "因子相关", "相关矩阵", "高相关", "冗余因子", "替换因子", "yinzi xiangguan"),
        read_tools=("factor_corr_get_top_pairs", "factor_corr_get_clusters", "factor_corr_suggest_replacements", "factor_corr_get_matrix_ref"),
        plan_tools=("factor_corr_plan", "factor_corr_validate_inputs"),
        confirmed_tools=("factor_corr_submit_confirmed",),
        prompt_key="domain.factor_correlation",
    ),
    McpDomain.MODEL_REGISTRY: DomainSpec(
        domain=McpDomain.MODEL_REGISTRY,
        intent_value="model_registry_request",
        server_key="aistock-model-registry",
        default_tool="model_registry_list",
        risk_policy="summary_detail_artifact_refs_confirmed_lifecycle",
        summary_zh="Model registry, model trials, seed stability, hyperparameter history and artifact refs",
        synonyms=("model registry", "model library", "model trial", "trial", "seed", "seed stability", "hyperparam", "artifact", "model artifact", "模型库", "模型注册", "模型版本", "模型试验", "种子稳定性", "超参", "moxing ku", "model version"),
        read_tools=("model_registry_list", "model_registry_get", "model_registry_compare_trials", "model_registry_get_seed_stability"),
        plan_tools=("model_registry_plan_register",),
        confirmed_tools=("model_registry_register_confirmed", "model_registry_deprecate_confirmed"),
        prompt_key="domain.model_registry",
    ),
    McpDomain.STRATEGY_GOVERNANCE: DomainSpec(
        domain=McpDomain.STRATEGY_GOVERNANCE,
        intent_value="strategy_governance_request",
        server_key="aistock-strategy-governance",
        default_tool="strategy_governance_list_packages",
        risk_policy="readiness_plan_confirmed_promotion_retirement",
        summary_zh="StrategyPackage governance, health, Selection/Paper readiness, promotion and retirement",
        synonyms=("strategy governance", "strategy package", "strategy library", "selection readiness", "paper readiness", "paper v2", "promotion", "retirement", "strategy health", "策略库", "策略包", "策略治理", "选股就绪", "模拟盘就绪", "晋升策略", "退役策略", "celue ku", "package health"),
        read_tools=("strategy_governance_list_packages", "strategy_governance_get_package", "strategy_governance_get_health"),
        plan_tools=("strategy_governance_plan_promotion", "strategy_governance_plan_retirement"),
        confirmed_tools=("strategy_governance_promote_confirmed", "strategy_governance_retire_confirmed"),
        prompt_key="domain.strategy_governance",
    ),
    McpDomain.EXECUTION_POLICY: DomainSpec(
        domain=McpDomain.EXECUTION_POLICY,
        intent_value="execution_policy_request",
        server_key="aistock-execution-policy",
        default_tool="execution_policy_list_algos",
        risk_policy="read_validate_plan_confirmed_binding_no_real_trade",
        summary_zh="Execution policy library, minute algos, market-state constraints and strategy binding validation",
        synonyms=("execution policy", "execution algo", "minute algo", "twap", "vwap", "pov", "algo", "market state", "execution binding", "执行策略", "执行算法", "分钟算法", "市场状态", "策略绑定", "zhixing celue", "fen zhong"),
        read_tools=("execution_policy_list_algos", "execution_policy_get_algo", "execution_policy_get_market_state_constraints"),
        plan_tools=("execution_policy_validate_for_strategy", "execution_policy_plan_binding"),
        confirmed_tools=("execution_policy_bind_confirmed", "execution_policy_retire_confirmed"),
        prompt_key="domain.execution_policy",
    ),
    McpDomain.EXTERNAL_RESEARCH: DomainSpec(
        domain=McpDomain.EXTERNAL_RESEARCH,
        intent_value="external_research_request",
        server_key="aistock-external-research",
        default_tool="external_research_search_web",
        risk_policy="read_only_search_draft_only_evidence_candidates",
        summary_zh="External web and academic research search with provenance-first evidence candidates",
        synonyms=(
            "external research",
            "external search",
            "web search",
            "search web",
            "paper search",
            "academic search",
            "papers",
            "arxiv",
            "semantic scholar",
            "literature",
            "external evidence",
            "fetch extract",
            "save evidence",
            "外部研究",
            "外部检索",
            "网页搜索",
            "论文检索",
            "学术检索",
            "文献",
            "外部证据",
            "保存证据",
        ),
        read_tools=("external_research_search_web", "external_research_search_papers", "external_research_fetch_extract"),
        plan_tools=("external_research_save_evidence",),
        confirmed_tools=(),
        prompt_key="domain.external_research",
    ),
}

WAREHOUSE_TERMS = DOMAIN_SPECS[McpDomain.QE_WAREHOUSE].synonyms


def spec_for_domain(domain: McpDomain | str) -> DomainSpec:
    if not isinstance(domain, McpDomain):
        domain = McpDomain(str(domain))
    return DOMAIN_SPECS[domain]


def all_domain_specs() -> list[DomainSpec]:
    return list(DOMAIN_SPECS.values())


def domain_prompt_key(intent_value: str) -> str | None:
    for spec in DOMAIN_SPECS.values():
        if spec.intent_value == intent_value:
            return spec.prompt_key
    return None


def domain_catalog() -> dict[str, Any]:
    return {
        spec.domain.value: {
            "intent_value": spec.intent_value,
            "server_key": spec.server_key,
            "default_tool": spec.default_tool,
            "risk_policy": spec.risk_policy,
            "summary_zh": spec.summary_zh,
            "synonyms": list(spec.synonyms),
            "read_tools": list(spec.read_tools),
            "plan_tools": list(spec.plan_tools),
            "confirmed_tools": list(spec.confirmed_tools),
            "prompt_key": spec.prompt_key,
        }
        for spec in DOMAIN_SPECS.values()
    }
