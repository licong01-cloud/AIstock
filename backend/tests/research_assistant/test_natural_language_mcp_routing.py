from __future__ import annotations

from typing import Any

from backend.services.research_assistant.domain_ontology import McpDomain
from backend.services.research_assistant.models import ChatTurnRequest
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import LlmCallResult, ResearchAssistantService
from backend.services.research_assistant.tool_router import route_examples, route_request


class _LocalDataCheckLlm:
    def complete(self, **kwargs: Any) -> LlmCallResult:
        return LlmCallResult(
            content="Local data sync status summary is available from the selected MCP route.",
            provider="fake",
            model="fake-local-data-route",
            duration_ms=1,
            usage={},
        )


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
        McpDomain.EXTERNAL_RESEARCH,
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
        assert route["server_key"] == "aistock-qe"
        assert route["tool_name"].startswith("qe_archive_")


def test_new_domain_routes_select_expected_tools() -> None:
    cases = {
        "factor library search momentum": ("aistock-factor", "factor_library_search"),
        "calculate independent RankIC for factor": ("aistock-factor", "factor_metrics_plan"),
        "factor correlation top pairs": ("aistock-factor", "factor_corr_get_top_pairs"),
        "model seed stability": ("aistock-qe", "model_registry_get_seed_stability"),
        "strategy package paper readiness": ("aistock-trading-ops", "strategy_governance_get_paper_readiness"),
        "execution policy market state constraints": ("aistock-trading-ops", "execution_policy_get_market_state_constraints"),
        "search external research about HMM factor timing": ("aistock-external-research", "external_research_search_web"),
        "paper search for factor decay literature": ("aistock-external-research", "external_research_search_papers"),
        "fetch extract from this research URL": ("aistock-external-research", "external_research_fetch_extract"),
        "save external evidence candidate": ("aistock-external-research", "external_research_save_evidence"),
    }
    for message, (server, tool) in cases.items():
        route = route_request(message)
        assert route["server_key"] == server
        assert route["tool_name"] == tool


def test_chinese_catalog_questions_choose_summary_first_read_tools() -> None:
    cases = {
        "数仓有没有漏入仓？": ("qe_warehouse", "aistock-qe", "qe_archive_health", "read_only"),
        "查看因子库概要": ("factor_library", "aistock-factor", "factor_library_list", "read_only"),
        "帮我看看因子库有哪些可用因子": ("factor_library", "aistock-factor", "factor_library_list", "read_only"),
        "最近因子库哪些因子相关性太高？": ("factor_correlation", "aistock-factor", "factor_corr_get_top_pairs", "read_only"),
        "这个模型 trial 和之前 seed 表现差异大吗？": ("model_registry", "aistock-qe", "model_registry_compare_trials", "read_only"),
        "执行策略库里有什么 minute algo？": ("execution_policy", "aistock-trading-ops", "execution_policy_list_algos", "read_only"),
    }
    for message, (domain, server, tool, side_effect) in cases.items():
        route = route_request(message)
        assert route["domain"] == domain
        assert route["server_key"] == server
        assert route["tool_name"] == tool
        assert route["side_effect"] == side_effect


def test_utf8_chinese_catalog_questions_route_to_summary_first_read_tools() -> None:
    cases = {
        "因子库里有哪些可用因子？先给概要": ("factor_library", "aistock-factor", "factor_library_list", "read_only"),
        "模型库里有什么模型？": ("model_registry", "aistock-qe", "model_registry_list", "read_only"),
        "策略库目前有哪些策略？": ("strategy_governance", "aistock-trading-ops", "strategy_governance_list_packages", "read_only"),
    }
    for message, (domain, server, tool, side_effect) in cases.items():
        route = route_request(message)
        assert route["domain"] == domain
        assert route["server_key"] == server
        assert route["tool_name"] == tool
        assert route["side_effect"] == side_effect




def test_qe_archive_analytics_terms_select_view_tools() -> None:
    cases = {
        "QE 数仓分析视图是否已经创建": "qe_archive_query_analytics_view_status",
        "查看 QE run leaderboard 最好的模型": "qe_archive_query_run_leaderboard",
        "分析 QE seed robustness 和种子鲁棒性": "qe_archive_query_seed_robustness",
        "最近因子表现 factor performance 怎么样": "qe_archive_query_factor_performance",
        "模型超参和 seed 性能分析": "qe_archive_query_model_hyperparam_seed_perf",
        "检查 QE 过拟合红旗": "qe_archive_query_overfit_flags",
        "有哪些晋升候选配置": "qe_archive_query_promotion_candidates",
        "查看 QE 演进血缘 lineage": "qe_archive_query_evolution_lineage",
    }
    for message, tool in cases.items():
        route = route_request(message)
        assert route["domain"] == "qe_warehouse"
        assert route["server_key"] == "aistock-qe"
        assert route["tool_name"] == tool
        assert route["side_effect"] == "read_only"


def test_bug_160_utf8_chinese_business_mcp_overviews_are_routed() -> None:
    cases = {
        "\u56e0\u5b50\u5e93\u6709\u54ea\u4e9b\u56e0\u5b50\uff1f\u53ea\u8981\u6982\u8981\u5217\u8868\uff0c\u4e0d\u8981\u5168\u91cf\u8be6\u60c5\u3002": ("factor_library", "aistock-factor", "factor_library_list"),
        "\u67e5\u770b\u56e0\u5b50\u72ec\u7acb\u6307\u6807\u8ba1\u7b97\u80fd\u529b\u6982\u8981\u3002": ("factor_metrics", "aistock-factor", "factor_metrics_plan"),
        "\u67e5\u770b\u56e0\u5b50\u76f8\u5173\u6027\u8ba1\u7b97\u80fd\u529b\u6982\u8981\u3002": ("factor_correlation", "aistock-factor", "factor_corr_plan"),
        "\u67e5\u770b\u6a21\u578b\u5e93\u6982\u8981\u3002": ("model_registry", "aistock-qe", "model_registry_list"),
        "\u67e5\u770b\u7b56\u7565\u5e93\u6982\u8981\u3002": ("strategy_governance", "aistock-trading-ops", "strategy_governance_list_packages"),
        "\u67e5\u770b\u6267\u884c\u7b56\u7565\u5e93\u6982\u8981\u3002": ("execution_policy", "aistock-trading-ops", "execution_policy_list_algos"),
    }
    for message, (domain, server, tool) in cases.items():
        route = route_request(message)
        assert route["domain"] == domain
        assert route["server_key"] == server
        assert route["tool_name"] == tool
        assert route["side_effect"] in {"read_only", "plan_or_preflight"}
        assert route["server_key"] != "aistock-local-data"


def test_bug_326_local_data_sync_check_routes_to_read_only_health_overview() -> None:
    cases = [
        "\u68c0\u67e5\u672c\u5730\u6570\u636e\u540c\u6b65\u60c5\u51b5",
        "\u68c0\u67e5\u672c\u5730\u6570\u636e\u540c\u6b65\u72b6\u6001",
        "\u67e5\u770b\u672c\u5730\u6570\u636e\u540c\u6b65\u6982\u89c8",
        "check local data sync status",
        "local data sync readiness overview",
    ]
    for message in cases:
        route = route_request(message)
        assert route["domain"] == "local_data"
        assert route["server_key"] == "aistock-local-data"
        assert route["tool_name"] == "local_data_health_overview"
        assert route["side_effect"] == "read_only"
        assert route["tool_name"] != "local_data_apply_repair_confirmed"


def test_bug_326_local_data_repair_or_sync_execution_stays_preflight_only() -> None:
    cases = [
        "\u4fee\u590d\u672c\u5730\u6570\u636e\u7f3a\u53e3",
        "\u6267\u884c\u672c\u5730\u6570\u636e\u540c\u6b65",
        "repair local data gap",
        "sync local data now",
    ]
    for message in cases:
        route = route_request(message)
        assert route["domain"] == "local_data"
        assert route["server_key"] == "aistock-local-data"
        assert route["tool_name"] == "local_data_plan_repair"
        assert route["side_effect"] == "plan_or_preflight"
        assert route["tool_name"] != "local_data_apply_repair_confirmed"


def test_bug_326_chat_local_data_sync_check_uses_grounded_read_only_route() -> None:
    svc = ResearchAssistantService(
        repository=InMemoryResearchAssistantRepository(),
        llm_client=_LocalDataCheckLlm(),
    )
    svc.seed_catalogs()

    result = svc.chat_turn(ChatTurnRequest(message="\u68c0\u67e5\u672c\u5730\u6570\u636e\u540c\u6b65\u60c5\u51b5"))

    route = result["cards"]["mcp_route_decision"]
    assert route["domain"] == "local_data"
    assert route["server_key"] == "aistock-local-data"
    assert route["tool_name"] == "local_data_health_overview"
    assert route["side_effect"] == "read_only"
    execution = result["cards"]["mcp_execution_result"]
    assert execution["auto_executed"] is True
    assert execution["tool_name"] == "local_data_health_overview"
    assert result["cards"]["react_grounding"]["evidence_guard"]["reason"] == "ok"
    assert "max tool iterations reached without reliable evidence" not in result["assistant_message"]["content_text"]
    assert "source=" in result["assistant_message"]["content_text"]
    assert "as_of=" in result["assistant_message"]["content_text"]


def test_bug_158_chinese_business_mcp_overviews_do_not_route_to_local_data() -> None:
    cases = {
        "因子库有哪些因子？只要概要列表，不要全量详情。": ("factor_library", "aistock-factor", "factor_library_list"),
        "查看因子独立指标计算能力概要。": ("factor_metrics", "aistock-factor", "factor_metrics_plan"),
        "查看因子相关性计算能力概要。": ("factor_correlation", "aistock-factor", "factor_corr_plan"),
        "查看模型库概要。": ("model_registry", "aistock-qe", "model_registry_list"),
        "查看策略库概要。": ("strategy_governance", "aistock-trading-ops", "strategy_governance_list_packages"),
        "查看执行策略库概要。": ("execution_policy", "aistock-trading-ops", "execution_policy_list_algos"),
    }
    for message, (domain, server, tool) in cases.items():
        route = route_request(message)
        assert route["domain"] == domain
        assert route["server_key"] == server
        assert route["tool_name"] == tool
        assert route["side_effect"] in {"read_only", "plan_or_preflight"}
        assert route["server_key"] != "aistock-local-data"

