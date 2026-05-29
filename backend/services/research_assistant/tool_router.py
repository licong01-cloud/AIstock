"""Natural-language MCP tool router for Research Assistant."""

from __future__ import annotations

import re
from typing import Any

from .domain_ontology import DOMAIN_SPECS, McpDomain, WAREHOUSE_TERMS, spec_for_domain

WRITE_TERMS = (
    "create", "register", "submit", "run", "execute", "sync", "backfill", "bind", "retire", "promote", "deprecate",
    "repair", "materialize", "rerun", "retry", "close", "finish", "update", "write",
    "创建", "登记", "提交", "运行", "执行", "同步", "补录", "绑定", "退役", "晋升", "废弃", "修复", "关闭", "完成", "更新", "写入",
)
PLAN_TERMS = ("plan", "preview", "preflight", "validate", "check", "diagnose", "dry run", "proposal", "prepare", "计划", "预检查", "校验", "检查", "诊断", "方案", "准备")
DETAIL_TERMS = ("get", "detail", "details", "show", "inspect", "explain", "single", "one", "specific", "详情", "查看", "解释", "单个", "具体")
SEARCH_TERMS = ("search", "find", "list", "which", "what", "overview", "summary", "status", "搜索", "查找", "列表", "哪些", "有什么", "有哪些", "概览", "概要", "状态", "可用", "看看")

TOOL_HINTS: tuple[tuple[McpDomain, tuple[str, ...], str], ...] = (
    (McpDomain.MCP_CAPABILITY, ("tool", "server", "capability", "mcp"), "assistant_list_mcp_tools"),
    (McpDomain.LOCAL_DATA, ("repair", "gap"), "local_data_plan_repair"),
    (McpDomain.LOCAL_DATA, ("dataset", "trade_date", "calendar"), "local_data_get_dataset_status"),
    (McpDomain.QE_WAREHOUSE, ("outbox",), "qe_archive_list_outbox"),
    (McpDomain.QE_WAREHOUSE, ("backfill", "ruku", "louruku", "bulu"), "qe_archive_backfill_preview"),
    (McpDomain.QE_WAREHOUSE, ("source status", "source"), "qe_archive_get_source_status"),
    (McpDomain.QE_EXPERIMENT, ("template", "materialize"), "qe_template_create"),
    (McpDomain.QE_EXPERIMENT, ("loop", "compare"), "qe_custom_evo_loop_comparison"),
    (McpDomain.VALIDATION_ISSUE, ("sync", "close", "finish", "同步", "关闭", "完成"), "mcp_github_issue_sync_bug"),
    (McpDomain.VALIDATION_ISSUE, ("agent context", "context"), "get_bug_agent_context"),
    (McpDomain.RESEARCH_PIPELINE, ("artifact", "refs"), "research_list_artifact_refs"),
    (McpDomain.FACTOR_LIBRARY, ("search", "find", "搜索", "查找"), "factor_library_search"),
    (McpDomain.FACTOR_LIBRARY, ("coverage", "覆盖"), "factor_library_get_coverage"),
    (McpDomain.FACTOR_LIBRARY, ("overview", "summary", "list", "available", "catalog", "有哪些", "有什么", "概要", "概览", "列表", "可用", "看看"), "factor_library_list"),
    (McpDomain.FACTOR_LIBRARY, ("register", "登记", "注册"), "factor_library_plan_register"),
    (McpDomain.FACTOR_LIBRARY, ("deprecate", "retire", "废弃", "退役"), "factor_library_plan_deprecate"),
    (McpDomain.FACTOR_METRICS, ("submit", "run", "calculate", "calc", "提交", "运行", "计算"), "factor_metrics_plan"),
    (McpDomain.FACTOR_METRICS, ("result", "ic", "rankic", "结果"), "factor_metrics_get_result"),
    (McpDomain.FACTOR_CORRELATION, ("top", "pairs", "top pairs", "高相关"), "factor_corr_get_top_pairs"),
    (McpDomain.FACTOR_CORRELATION, ("cluster", "聚类"), "factor_corr_get_clusters"),
    (McpDomain.FACTOR_CORRELATION, ("replacement", "replace", "替换"), "factor_corr_suggest_replacements"),
    (McpDomain.FACTOR_CORRELATION, ("matrix", "矩阵"), "factor_corr_get_matrix_ref"),
    (McpDomain.MODEL_REGISTRY, ("trial", "compare", "试验", "比较"), "model_registry_compare_trials"),
    (McpDomain.MODEL_REGISTRY, ("seed", "种子"), "model_registry_get_seed_stability"),
    (McpDomain.MODEL_REGISTRY, ("hyperparam", "超参"), "model_registry_get_hyperparam_history"),
    (McpDomain.MODEL_REGISTRY, ("artifact", "产物"), "model_registry_get_artifacts"),
    (McpDomain.STRATEGY_GOVERNANCE, ("health", "健康"), "strategy_governance_get_health"),
    (McpDomain.STRATEGY_GOVERNANCE, ("selection", "选股"), "strategy_governance_get_selection_readiness"),
    (McpDomain.STRATEGY_GOVERNANCE, ("paper", "模拟盘"), "strategy_governance_get_paper_readiness"),
    (McpDomain.STRATEGY_GOVERNANCE, ("promote", "promotion", "晋升"), "strategy_governance_plan_promotion"),
    (McpDomain.STRATEGY_GOVERNANCE, ("retire", "retirement", "退役"), "strategy_governance_plan_retirement"),
    (McpDomain.EXECUTION_POLICY, ("market state", "constraint", "市场状态", "约束"), "execution_policy_get_market_state_constraints"),
    (McpDomain.EXECUTION_POLICY, ("validate", "fit", "suitable", "校验", "适配", "适合"), "execution_policy_validate_for_strategy"),
    (McpDomain.EXECUTION_POLICY, ("bind", "binding", "绑定"), "execution_policy_plan_binding"),
    (McpDomain.EXECUTION_POLICY, ("execution policy list", "minute algo", "执行策略库", "有什么", "有哪些", "可用"), "execution_policy_list_algos"),
)

def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def _term_in_text(term: str, text: str) -> bool:
    normalized = term.lower()
    if len(normalized) <= 2 and normalized.isalnum():
        return re.search(rf"(?<![a-z0-9_]){re.escape(normalized)}(?![a-z0-9_])", text) is not None
    return normalized in text


def _contains_any(text: str, terms: tuple[str, ...]) -> bool:
    return any(_term_in_text(term, text) for term in terms)


def score_domains(message: str) -> list[dict[str, Any]]:
    lower = _norm(message)
    scores: list[dict[str, Any]] = []
    for spec in DOMAIN_SPECS.values():
        score = 0
        matched: list[str] = []
        for term in spec.synonyms:
            if _term_in_text(term, lower):
                score += 3 if len(term) >= 3 else 1
                matched.append(term)
        if spec.domain == McpDomain.QE_WAREHOUSE and _contains_any(lower, WAREHOUSE_TERMS):
            score += 8
        if spec.domain == McpDomain.LOCAL_DATA and _contains_any(lower, WAREHOUSE_TERMS):
            score -= 6
        if spec.domain == McpDomain.LOCAL_DATA and (
            ("repair" in lower or "gap" in lower or "sync" in lower)
            and any(token in lower for token in ("stock_daily", "trade_date", "dataset", "calendar", "local"))
        ):
            score += 8
        if spec.domain == McpDomain.RESEARCH_PIPELINE and (
            "research" in lower and any(token in lower for token in ("backtest", "record", "stage", "artifact", "hmm"))
        ):
            score += 8
        if spec.domain == McpDomain.FACTOR_METRICS and (
            any(token in lower for token in ("calculate", "calc", "independent", "official evaluation", "compare", "version", "submit", "metrics", "计算", "独立指标", "评价", "版本", "提交"))
            and ("factor" in lower or "因子" in lower or "rankic" in lower or re.search(r"(?<![a-z0-9_])ic(?![a-z0-9_])", lower))
        ):
            score += 10
        if spec.domain == McpDomain.FACTOR_LIBRARY and (
            any(token in lower for token in ("deprecate", "register", "catalog", "library", "coverage", "search", "因子库", "因子目录", "可用", "列表", "覆盖", "搜索"))
            and ("factor" in lower or "因子" in lower)
        ):
            score += 8
        if spec.domain == McpDomain.FACTOR_CORRELATION and (
            ("factor" in lower or "因子" in lower) and any(token in lower for token in ("correlation", "corr", "cluster", "matrix", "replacement", "redundant", "top pairs", "相关", "聚类", "矩阵", "替换", "冗余"))
        ):
            score += 8
        if score > 0:
            scores.append({"domain": spec.domain, "score": score, "matched_terms": matched})
    scores.sort(key=lambda item: item["score"], reverse=True)
    return scores


def classify_domain(message: str) -> McpDomain:
    scores = score_domains(message)
    if not scores:
        return McpDomain.GENERAL
    return scores[0]["domain"]


def select_tool(domain: McpDomain, message: str) -> str:
    lower = _norm(message)
    spec = spec_for_domain(domain)
    for hint_domain, terms, tool in TOOL_HINTS:
        if hint_domain == domain and _contains_any(lower, terms):
            return tool
    if _contains_any(lower, DETAIL_TERMS) and spec.read_tools:
        for tool in spec.read_tools:
            if tool.endswith("_get") or "_get_" in tool:
                return tool
    if _contains_any(lower, PLAN_TERMS) and spec.plan_tools:
        return spec.plan_tools[0]
    if _contains_any(lower, WRITE_TERMS) and spec.confirmed_tools:
        return spec.plan_tools[0] if spec.plan_tools else spec.confirmed_tools[0]
    if _contains_any(lower, SEARCH_TERMS) and spec.read_tools:
        return spec.read_tools[0]
    return spec.default_tool


def route_request(message: str) -> dict[str, Any]:
    domain = classify_domain(message)
    if domain == McpDomain.GENERAL:
        return {
            "domain": McpDomain.GENERAL.value,
            "server_key": None,
            "tool_name": None,
            "reason": "No specific AIstock MCP domain is clear yet; ask a clarifying question before choosing a tool.",
            "policy": "no_tool_until_domain_clear",
            "confidence": 0.35,
            "matched_terms": [],
        }
    spec = spec_for_domain(domain)
    scores = score_domains(message)
    current = next((item for item in scores if item["domain"] == domain), {"score": 1, "matched_terms": []})
    tool = select_tool(domain, message)
    side_effect = "read_only"
    if tool in spec.plan_tools:
        side_effect = "plan_or_preflight"
    if tool in spec.confirmed_tools:
        side_effect = "confirmed_action"
    policy = spec.risk_policy
    if side_effect == "confirmed_action":
        policy = f"{policy}; confirmed tools require preflight + explicit confirmation + approval when configured"
    return {
        "domain": domain.value,
        "intent_value": spec.intent_value,
        "server_key": spec.server_key,
        "tool_name": tool,
        "reason": f"Matched {spec.summary_zh}; route to {spec.server_key}/{tool}.",
        "policy": policy,
        "side_effect": side_effect,
        "confidence": min(0.96, 0.55 + float(current["score"]) / 20.0),
        "matched_terms": list(current.get("matched_terms") or []),
        "read_tools": list(spec.read_tools),
        "plan_tools": list(spec.plan_tools),
        "confirmed_tools": list(spec.confirmed_tools),
    }


def route_examples() -> list[tuple[str, McpDomain]]:
    return [
        ("what MCP tools can you use", McpDomain.MCP_CAPABILITY),
        ("list available AIstock MCP servers", McpDomain.MCP_CAPABILITY),
        ("show your tool catalog", McpDomain.MCP_CAPABILITY),
        ("which capabilities are enabled", McpDomain.MCP_CAPABILITY),
        ("check trade_date dataset readiness", McpDomain.LOCAL_DATA),
        ("local data calendar has gaps", McpDomain.LOCAL_DATA),
        ("tushare source test status", McpDomain.LOCAL_DATA),
        ("plan repair for stock_daily", McpDomain.LOCAL_DATA),
        ("compare QE loop metrics", McpDomain.QE_EXPERIMENT),
        ("create QE template draft", McpDomain.QE_EXPERIMENT),
        ("show custom_evo task status", McpDomain.QE_EXPERIMENT),
        ("materialize this experiment template", McpDomain.QE_EXPERIMENT),
        ("warehouse missing archive runs", McpDomain.QE_WAREHOUSE),
        ("archive outbox pending items", McpDomain.QE_WAREHOUSE),
        ("backfill QE archive for skipped runs", McpDomain.QE_WAREHOUSE),
        ("source status for data warehouse ingestion", McpDomain.QE_WAREHOUSE),
        ("sync BUG-120 GitHub issue", McpDomain.VALIDATION_ISSUE),
        ("create validation issue candidate", McpDomain.VALIDATION_ISSUE),
        ("get BUG agent context", McpDomain.VALIDATION_ISSUE),
        ("list validation run findings", McpDomain.VALIDATION_ISSUE),
        ("research pipeline artifact refs", McpDomain.RESEARCH_PIPELINE),
        ("run research stage preview", McpDomain.RESEARCH_PIPELINE),
        ("show research backtest records", McpDomain.RESEARCH_PIPELINE),
        ("research pipeline HMM artifact refs", McpDomain.RESEARCH_PIPELINE),
        ("factor library momentum factors", McpDomain.FACTOR_LIBRARY),
        ("search factor catalog by price volume", McpDomain.FACTOR_LIBRARY),
        ("factor coverage for alpha_001", McpDomain.FACTOR_LIBRARY),
        ("deprecate a factor plan", McpDomain.FACTOR_LIBRARY),
        ("calculate independent RankIC for a factor", McpDomain.FACTOR_METRICS),
        ("factor metrics job result", McpDomain.FACTOR_METRICS),
        ("compare IC versions", McpDomain.FACTOR_METRICS),
        ("submit official factor evaluation", McpDomain.FACTOR_METRICS),
        ("factor correlation top pairs", McpDomain.FACTOR_CORRELATION),
        ("find redundant factor clusters", McpDomain.FACTOR_CORRELATION),
        ("suggest low correlation replacement factors", McpDomain.FACTOR_CORRELATION),
        ("get correlation matrix ref", McpDomain.FACTOR_CORRELATION),
        ("model registry list CatBoost models", McpDomain.MODEL_REGISTRY),
        ("compare model trials", McpDomain.MODEL_REGISTRY),
        ("model seed stability", McpDomain.MODEL_REGISTRY),
        ("model hyperparam history", McpDomain.MODEL_REGISTRY),
        ("strategy package health", McpDomain.STRATEGY_GOVERNANCE),
        ("strategy selection readiness", McpDomain.STRATEGY_GOVERNANCE),
        ("can this strategy enter Paper v2", McpDomain.STRATEGY_GOVERNANCE),
        ("plan strategy promotion", McpDomain.STRATEGY_GOVERNANCE),
        ("execution policy minute algos", McpDomain.EXECUTION_POLICY),
        ("is POV suitable for this strategy", McpDomain.EXECUTION_POLICY),
        ("market state constraints for execution", McpDomain.EXECUTION_POLICY),
        ("bind execution policy plan", McpDomain.EXECUTION_POLICY),
    ]
