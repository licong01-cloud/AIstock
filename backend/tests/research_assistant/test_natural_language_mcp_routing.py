from __future__ import annotations

from backend.services.research_assistant.domain_ontology import McpDomain
from backend.services.research_assistant.tool_router import route_examples, route_request


def test_route_examples_cover_at_least_40_natural_language_cases() -> None:
    examples = route_examples()
    assert len(examples) >= 40
    covered = {domain for _message, domain in examples}
    required = {
        McpDomain.MCP_CAPABILITY,
        McpDomain.LOCAL_DATA,
        McpDomain.QE_EXPERIMENT,
        McpDomain.QE_WAREHOUSE,
        McpDomain.VALIDATION_ISSUE,
        McpDomain.RESEARCH_PIPELINE,
        McpDomain.FACTOR_LIBRARY,
        McpDomain.FACTOR_METRICS,
        McpDomain.FACTOR_CORRELATION,
        McpDomain.MODEL_REGISTRY,
        McpDomain.STRATEGY_GOVERNANCE,
        McpDomain.EXECUTION_POLICY,
    }
    assert required.issubset(covered)


def test_every_route_decision_contains_server_tool_reason_policy() -> None:
    for message, expected_domain in route_examples():
        route = route_request(message)
        assert route["domain"] == expected_domain.value
        assert route["server_key"]
        assert route["tool_name"]
        assert route["reason"]
        assert route["policy"]
        assert route["confidence"] > 0


def test_warehouse_terms_route_to_qe_archive_not_local_data() -> None:
    messages = [
        "QE warehouse ingestion status",
        "warehouse outbox pending",
        "QE archive backfill skipped runs",
        "data warehouse source status",
        "shucang backfill skipped archive runs",
        "ruku missing archive jobs",
    ]
    for message in messages:
        route = route_request(message)
        assert route["domain"] == "qe_warehouse"
        assert route["server_key"] == "aistock-qe-archive"
        assert route["tool_name"].startswith("qe_archive_")


def test_new_domain_routes_select_expected_tools() -> None:
    cases = {
        "factor library search momentum": ("aistock-factor-library", "factor_library_search"),
        "calculate independent RankIC for factor": ("aistock-factor-metrics", "factor_metrics_plan"),
        "factor correlation top pairs": ("aistock-factor-correlation", "factor_corr_get_top_pairs"),
        "model seed stability": ("aistock-model-registry", "model_registry_get_seed_stability"),
        "strategy package paper readiness": ("aistock-strategy-governance", "strategy_governance_get_paper_readiness"),
        "execution policy market state constraints": ("aistock-execution-policy", "execution_policy_get_market_state_constraints"),
    }
    for message, (server, tool) in cases.items():
        route = route_request(message)
        assert route["server_key"] == server
        assert route["tool_name"] == tool


def test_chinese_catalog_questions_choose_summary_first_read_tools() -> None:
    cases = {
        "数仓有没有漏入仓？": ("qe_warehouse", "aistock-qe-archive", "qe_archive_health", "read_only"),
        "查看因子库概要": ("factor_library", "aistock-factor-library", "factor_library_list", "read_only"),
        "帮我看看因子库有哪些可用因子": ("factor_library", "aistock-factor-library", "factor_library_list", "read_only"),
        "最近因子库哪些因子相关性太高？": ("factor_correlation", "aistock-factor-correlation", "factor_corr_get_top_pairs", "read_only"),
        "这个模型 trial 和之前 seed 表现差异大吗？": ("model_registry", "aistock-model-registry", "model_registry_compare_trials", "read_only"),
        "执行策略库里有什么 minute algo？": ("execution_policy", "aistock-execution-policy", "execution_policy_list_algos", "read_only"),
    }
    for message, (domain, server, tool, side_effect) in cases.items():
        route = route_request(message)
        assert route["domain"] == domain
        assert route["server_key"] == server
        assert route["tool_name"] == tool
        assert route["side_effect"] == side_effect


def test_utf8_chinese_catalog_questions_route_to_summary_first_read_tools() -> None:
    cases = {
        "因子库里有哪些可用因子？先给概要": ("factor_library", "aistock-factor-library", "factor_library_list", "read_only"),
        "模型库里有什么模型？": ("model_registry", "aistock-model-registry", "model_registry_list", "read_only"),
        "策略库目前有哪些策略？": ("strategy_governance", "aistock-strategy-governance", "strategy_governance_list_packages", "read_only"),
    }
    for message, (domain, server, tool, side_effect) in cases.items():
        route = route_request(message)
        assert route["domain"] == domain
        assert route["server_key"] == server
        assert route["tool_name"] == tool
        assert route["side_effect"] == side_effect


def test_bug_158_chinese_business_mcp_overviews_do_not_route_to_local_data() -> None:
    cases = {
        "因子库有哪些因子？只要概要列表，不要全量详情。": ("factor_library", "aistock-factor-library", "factor_library_list"),
        "查看因子独立指标计算能力概要。": ("factor_metrics", "aistock-factor-metrics", "factor_metrics_plan"),
        "查看因子相关性计算能力概要。": ("factor_correlation", "aistock-factor-correlation", "factor_corr_plan"),
        "查看模型库概要。": ("model_registry", "aistock-model-registry", "model_registry_list"),
        "查看策略库概要。": ("strategy_governance", "aistock-strategy-governance", "strategy_governance_list_packages"),
        "查看执行策略库概要。": ("execution_policy", "aistock-execution-policy", "execution_policy_list_algos"),
    }
    for message, (domain, server, tool) in cases.items():
        route = route_request(message)
        assert route["domain"] == domain
        assert route["server_key"] == server
        assert route["tool_name"] == tool
        assert route["side_effect"] in {"read_only", "plan_or_preflight"}
        assert route["server_key"] != "aistock-local-data"

