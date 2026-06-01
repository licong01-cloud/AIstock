from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

from backend.services.research_assistant.agent_teams.config import load_agent_teams_config
from backend.services.research_assistant.agent_teams.runtime import assert_worker_tool_allowed, enforce_worker_catalog
from backend.services.research_assistant.react_grounding import McpToolCall, ToolCatalogEntry


CONFIG_PATH = Path(__file__).resolve().parents[3] / "configs/research_assistant/agent_teams.yaml"


def _catalog() -> list[ToolCatalogEntry]:
    return [
        ToolCatalogEntry("aistock-local-data", "local_data_health_overview", "approved", "low", "read_only"),
        ToolCatalogEntry("aistock-local-data", "local_data_apply_repair_confirmed", "approved", "high", "write_nonprod", True),
        ToolCatalogEntry("aistock-qe-experiment", "qe_template_run_confirmed", "approved", "high", "high_cost_compute", True),
        ToolCatalogEntry("aistock-qe-experiment", "qe_template_validate", "approved", "low", "read_only"),
    ]


def test_worker_tool_subset_rejects_catalog_tool_outside_worker_allowlist() -> None:
    config = load_agent_teams_config(CONFIG_PATH)
    factor_worker = config.worker_by_key("factor_developer")
    decision = assert_worker_tool_allowed(
        factor_worker,
        McpToolCall(server_key="aistock-local-data", tool_name="local_data_health_overview", stable_call_id="x"),
        _catalog(),
    )
    assert decision.allowed is False
    assert decision.reason == "tool_not_in_audited_catalog"


def test_worker_high_risk_tool_generates_preflight_not_execution() -> None:
    config = load_agent_teams_config(CONFIG_PATH)
    data_worker = config.worker_by_key("local_data_doctor")
    decision = assert_worker_tool_allowed(
        data_worker,
        McpToolCall(server_key="aistock-local-data", tool_name="local_data_apply_repair_confirmed", stable_call_id="repair"),
        _catalog(),
    )
    execute_action_proposal = Mock()
    if decision.action == "execute_read_only":
        execute_action_proposal()
    assert decision.allowed is True
    assert decision.action == "preflight_confirmation_only"
    execute_action_proposal.assert_not_called()


def test_enforce_worker_catalog_scopes_catalog_before_react_loop() -> None:
    config = load_agent_teams_config(CONFIG_PATH)
    qe_worker = config.worker_by_key("qe_experiment_designer")
    scoped = enforce_worker_catalog(qe_worker, _catalog())
    assert [(entry.server_key, entry.tool_name) for entry in scoped] == [("aistock-qe-experiment", "qe_template_validate")]
