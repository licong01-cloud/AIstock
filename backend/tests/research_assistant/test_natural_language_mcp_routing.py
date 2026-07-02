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


class _FakeLocalDataDailyStatusService:
    def get_preset_daily_status(self) -> dict[str, object]:
        return {
            "data": {
                "items": {
                    "daily_basic": {"status": "success", "finished_at": "2026-06-12T09:02:00+08:00"},
                    "stock_moneyflow_ts": {"status": "failed", "finished_at": "2026-06-12T09:04:00+08:00"},
                }
            },
            "trace": {"generated_at": "2026-06-12T01:06:00+00:00"},
        }

    def get_preset_stats(self) -> dict[str, object]:
        return {"data": {"items": [{"dataset": "daily_basic"}, {"dataset": "stock_moneyflow_ts"}]}}

    def list_jobs(self, *, limit: int = 50, active_only: bool = False) -> dict[str, object]:
        del limit, active_only
        return {"data": {"items": []}}

    def list_sync_targets(self, *, limit: int = 100) -> dict[str, object]:
        del limit
        return {"data": {"items": []}}


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


def test_bug_357_qe_business_queries_route_without_diagnostic_defaults() -> None:
    cases = {
        "\u76ee\u524d\u6700\u8fd1\u7684 QE \u5b9e\u9a8c\u6709\u54ea\u4e9b\uff1f\u7ed9\u6211\u4e00\u4e2a\u5217\u8868\u548c\u72b6\u6001\u6c47\u603b": ("qe_experiment", "qe_experiment_list", "read_only"),
        "custom_evo \u4efb\u52a1\u6700\u65b0\u8fdb\u5ea6\u600e\u4e48\u6837\uff1f\u7ed9\u6211\u72b6\u6001\u6c47\u603b": ("qe_experiment", "qe_experiment_list", "read_only"),
        "QE \u6570\u4ed3\u73b0\u5728\u662f\u5426\u6b63\u5e38\uff1f\u7ed9\u6211\u5065\u5eb7\u72b6\u6001\u548c\u5165\u4ed3\u6c47\u603b": ("qe_warehouse", "qe_archive_health", "read_only"),
        "\u67e5\u770b QE run leaderboard\uff0c\u544a\u8bc9\u6211\u6700\u597d\u7684\u6a21\u578b\u548c\u5173\u952e\u6307\u6807": ("qe_warehouse", "qe_archive_query_run_leaderboard", "read_only"),
        "\u5e2e\u6211\u8bbe\u8ba1\u4e00\u4e2a QE \u5b9e\u9a8c\u8349\u6848\uff0c\u5148\u4e0d\u8981\u6267\u884c\u3002": ("qe_experiment", "qe_template_create", "plan_or_preflight"),
    }
    for message, (domain, tool, side_effect) in cases.items():
        route = route_request(message)
        assert route["domain"] == domain
        assert route["server_key"] == "aistock-qe"
        assert route["tool_name"] == tool
        assert route["side_effect"] == side_effect


def test_bug_376_qe_archive_return_rank_questions_route_to_leaderboard() -> None:
    cases = [
        "目前进入数仓的 QE 实验，回测效果最好的收益是多少？是哪个实验？",
        "已入仓 QE run 里谁最赚钱？",
        "数仓里 CAGR 最高的是哪个实验？",
        "回测收益第一名是哪条？",
    ]
    for message in cases:
        route = route_request(message)
        assert route["domain"] == "qe_warehouse"
        assert route["server_key"] == "aistock-qe"
        assert route["tool_name"] == "qe_archive_query_run_leaderboard"
        assert route["side_effect"] == "read_only"
        assert "qe_run_leaderboard_intent" in route["matched_terms"]


def test_bug_359_specific_mcp_business_intents_route_to_expected_domains() -> None:
    cases = {
        "\u770b\u4e00\u4e0b QE seed \u7a33\u5b9a\u6027\uff0c\u54ea\u4e9b\u6a21\u578b\u66f4\u7a33\uff1f": ("qe_warehouse", "aistock-qe", "qe_archive_query_seed_robustness", "read_only"),
        "\u5e2e\u6211\u68c0\u7d22\u4e00\u4e0b\u5173\u4e8e A \u80a1\u591a\u56e0\u5b50 seed \u7a33\u5b9a\u6027\u7684\u8bba\u6587\u7ebf\u7d22\uff0c\u53ea\u8981\u6982\u8981\uff0c\u4e0d\u8981\u4fdd\u5b58\u8bc1\u636e": ("external_research", "aistock-external-research", "external_research_search_papers", "read_only"),
        "\u5217\u51fa\u6700\u8fd1\u5931\u8d25\u7684 QE run": ("qe_warehouse", "aistock-qe", "qe_archive_list_runs", "read_only"),
        "\u628a\u8fd9\u4e2a\u7b56\u7565\u664b\u5347\u5230 paper v2": ("strategy_governance", "aistock-trading-ops", "strategy_governance_plan_promotion", "plan_or_preflight"),
        "\u67e5\u770b\u8fd9\u4e2a\u7b56\u7565\u5305\u7684\u6267\u884c\u8d28\u91cf": ("strategy_governance", "aistock-trading-ops", "strategy_governance_list_packages", "read_only"),
        "\u54ea\u4e9b\u7b56\u7565\u5305\u53ef\u4ee5\u8fdb\u5165 paper v2": ("strategy_governance", "aistock-trading-ops", "strategy_governance_list_packages", "read_only"),
    }
    for message, (domain, server, tool, side_effect) in cases.items():
        route = route_request(message)
        assert route["domain"] == domain
        assert route["server_key"] == server
        assert route["tool_name"] == tool
        assert route["side_effect"] == side_effect


def test_bug_356_local_data_sync_check_routes_to_daily_status_list() -> None:
    cases = [
        "\u68c0\u67e5\u672c\u5730\u6570\u636e\u540c\u6b65\u60c5\u51b5",
        "\u68c0\u67e5\u672c\u5730\u6570\u636e\u540c\u6b65\u72b6\u6001",
        "\u67e5\u770b\u672c\u5730\u6570\u636e\u540c\u6b65\u6982\u89c8",
        "check local data sync status",
    ]
    for message in cases:
        route = route_request(message)
        assert route["domain"] == "local_data"
        assert route["server_key"] == "aistock-local-data"
        assert route["tool_name"] == "local_data_get_preset_daily_status"
        assert route["side_effect"] == "read_only"
        assert route["tool_name"] != "local_data_health_overview"
        assert route["tool_name"] != "local_data_apply_repair_confirmed"


def test_bug_356_local_data_each_dataset_sync_detail_routes_to_daily_status_list() -> None:
    route = route_request("\u7ed9\u6211\u8be6\u60c5\u4ecb\u7ecd\uff0c\u6bcf\u4e2a\u6570\u636e\u96c6\u7684\u540c\u6b65\u60c5\u51b5")

    assert route["domain"] == "local_data"
    assert route["server_key"] == "aistock-local-data"
    assert route["tool_name"] == "local_data_get_preset_daily_status"
    assert route["side_effect"] == "read_only"
    assert route["tool_name"] != "local_data_get_dataset_status"
    assert route["tool_name"] != "local_data_health_overview"


def test_local_data_sync_target_requests_route_to_target_list() -> None:
    route = route_request("List local data sync targets.")

    assert route["domain"] == "local_data"
    assert route["server_key"] == "aistock-local-data"
    assert route["tool_name"] == "local_data_list_sync_targets"
    assert route["side_effect"] == "read_only"


def test_bug_343_local_data_today_sync_question_routes_to_daily_status() -> None:
    route = route_request("\u68c0\u67e5\u5f53\u524d\u672c\u5730\u6570\u636e\u540c\u6b65\u4efb\u52a1\u8fd0\u884c\u60c5\u51b5\uff0c\u4eca\u5929\u6570\u636e\u54ea\u4e9b\u5b8c\u6210\u4e86\u540c\u6b65")

    assert route["domain"] == "local_data"
    assert route["server_key"] == "aistock-local-data"
    assert route["tool_name"] == "local_data_get_preset_daily_status"
    assert route["side_effect"] == "read_only"
    assert route["tool_name"] != "local_data_health_overview"
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
    svc.local_data_service_factory = _FakeLocalDataDailyStatusService

    result = svc.chat_turn(ChatTurnRequest(developer_diagnostics=True, message="\u68c0\u67e5\u672c\u5730\u6570\u636e\u540c\u6b65\u60c5\u51b5"))

    route = result["cards"]["mcp_route_decision"]
    assert route["domain"] == "local_data"
    assert route["server_key"] == "aistock-local-data"
    assert route["tool_name"] == "local_data_get_preset_daily_status"
    assert route["side_effect"] == "read_only"
    execution = result["cards"]["mcp_execution_result"]
    assert execution["auto_executed"] is True
    assert execution["tool_name"] == "local_data_get_preset_daily_status"
    assert result["cards"]["react_grounding"]["evidence_guard"]["reason"] == "guard_disabled"
    assert "max tool iterations reached without reliable evidence" not in result["assistant_message"]["content_text"]
    assert "Route decision" not in result["assistant_message"]["content_text"]
    assert "source=" not in result["assistant_message"]["content_text"]


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

