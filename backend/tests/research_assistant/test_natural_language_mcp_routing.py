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
