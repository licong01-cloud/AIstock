"""Isolate unrelated assistant contract tests from the machine's code graph."""

import pytest

from backend.services.research_assistant import service


_OFFLINE_ASSISTANT_TESTS = {
    "test_api.py",
    "test_external_research_react_consumption.py",
    "test_graph_injected_into_context.py",
    "test_memory_autogrow.py",
    "test_memory_tree_retrieval.py",
    "test_natural_language_mcp_routing.py",
    "test_ra_manifest_catalog_consumption.py",
    "test_react_phase1_phase2_context_regression.py",
    "test_react_tool_loop.py",
    "test_reflection_card_loop.py",
    "test_service.py",
    "test_skill_library.py",
    "test_stock_analysis_mcp.py",
    "test_tool_catalog_gate.py",
}


@pytest.fixture(autouse=True)
def isolate_query_code_intelligence(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Query classification, cache and reference provenance are exercised in
    # test_code_intel_context_injection.py and test_code_intel_decomposition.py.
    if request.path.name not in _OFFLINE_ASSISTANT_TESTS:
        return

    def skipped_query_context(**_kwargs: object) -> dict[str, object]:
        return {
            "status": "skipped",
            "reason_codes": ["query_code_intelligence_isolated_for_assistant_test"],
            "code_context_refs": [],
        }

    monkeypatch.setattr(service, "build_query_code_context", skipped_query_context)
