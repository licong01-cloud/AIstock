from __future__ import annotations

import pytest

from backend.services.research_assistant.react_grounding import McpToolCall
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import ResearchAssistantService


EXPECTED_CAPABILITY_CARD_PROFILES = {
    "local_data.apply_repair_confirmed": {
        "risk": "production_sensitive",
        "side_effect": "production_sensitive",
        "required_confirmations": ["APPROVE_RESEARCH_ASSISTANT_ACTION"],
    },
    "qe.run_experiment": {
        "risk": "high",
        "side_effect": "high_cost_compute",
        "required_confirmations": ["CONFIRM_QE_RUN", "QE_EXPERIMENT_RUN"],
    },
    "external_research.mcp_orchestration": {
        "risk": "medium",
        "side_effect": "draft_only",
        "required_confirmations": [],
    },
    "stock_analysis.mcp_orchestration": {
        "risk": "low",
        "side_effect": "read_only",
        "required_confirmations": [],
    },
}

EXPECTED_TOOL_PROFILE_BY_SELECTED_TOOL = {
    "local_data.apply_repair_confirmed": {
        "risk_level": "production_sensitive",
        "side_effect_level": "production_sensitive",
        "required_confirmations": ["APPROVE_RESEARCH_ASSISTANT_ACTION"],
        "requires_approval": True,
    },
    "qe.run_experiment": {
        "risk_level": "high",
        "side_effect_level": "high_cost_compute",
        "required_confirmations": ["CONFIRM_QE_RUN", "QE_EXPERIMENT_RUN"],
        "requires_approval": True,
    },
    "external_research.mcp_orchestration": {
        "risk_level": "high",
        "side_effect_level": "draft_only",
        "required_confirmations": [],
        "requires_approval": True,
    },
    "stock_analysis.mcp_orchestration": {
        "risk_level": "low",
        "side_effect_level": "read_only",
        "required_confirmations": [],
        "requires_approval": False,
    },
}

PROMPT_CHECKSUMS = {
    "root.assistant": "bce85a0f793c68393bd7c6bbbd8a9f8dd18d0715ceae3330946df7b1c2fa63f4",
    "mode.analysis": "13a6105771d438b383fca689c9be7bcfcde2c99f9d5fe219c8f64e64a48fadf1",
    "mode.planning": "f495971cd677edd51f1e1abb89b6115f1aff4be1441e858960d248314319203f",
    "domain.stock_analysis": "64709862585ca17ef151e5c9ab5a0937e1bf8f3cabd5a91a6f70abedff7a3dab",
}

SELECTED_TOOL_ROUTES = {
    "local_data.apply_repair_confirmed": {
        "server_key": "aistock-local-data",
        "tool_name": "local_data_apply_repair_confirmed",
    },
    "qe.run_experiment": {
        "server_key": "aistock-qe",
        "tool_name": "qe_template_run_confirmed",
    },
    "external_research.mcp_orchestration": {
        "server_key": "aistock-external-research",
        "tool_name": "external_research_save_evidence",
    },
    "stock_analysis.mcp_orchestration": {
        "server_key": "aistock-stock-analysis",
        "tool_name": "stock_analysis_get_quote",
    },
}


def _service() -> ResearchAssistantService:
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=object())
    seeded = svc.seed_catalogs()
    assert seeded["seeded"]["runtime_config_activations"] == 0
    assert seeded["seeded"]["capabilities"] == 0
    assert seeded["seeded"]["prompt_nodes"] == 0
    assert seeded["seeded"]["prompt_activations"] == 0
    assert seeded["seeded"]["mcp_tools"] == len(svc._manifest_mcp_catalog_records())
    assert svc.repository.list_records("capabilities", limit=500)["total"] == 0
    assert svc.repository.list_records("prompt_nodes", limit=500)["total"] == 0
    assert svc.repository.list_records("runtime_config_activations", limit=10)["total"] == 0
    return svc


def _snapshot_capabilities_by_key(svc: ResearchAssistantService) -> dict[str, dict[str, object]]:
    return {str(item["capability_key"]): item for item in svc._workflow_capabilities()}


def _cards_by_key(cards: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    return {str(item["capability_key"]): item for item in cards}


def test_tool_lookup_cards_and_prompt_text_match_yaml_pins_after_db_projection_retired() -> None:
    svc = _service()
    snapshot_by_key = _snapshot_capabilities_by_key(svc)
    assert len(snapshot_by_key) == 27
    assert "issue.create_candidate" not in snapshot_by_key

    tool_cases = [
        ("aistock-stock-analysis", "stock_analysis_get_quote", None, "stock_analysis.mcp_orchestration"),
        ("aistock-external-research", "external_research_search_web", None, "external_research.mcp_orchestration"),
        (
            "aistock-external-research",
            "external_research_search_web",
            {"domain": "stock_analysis"},
            "stock_analysis.mcp_orchestration",
        ),
        ("aistock-qe", "qe_template_run_confirmed", None, "qe_experiment.mcp_orchestration"),
        ("aistock-local-data", "local_data_apply_repair_confirmed", None, "local_data.mcp_orchestration"),
        ("research-assistant", "assistant_create_memory_candidate", None, "memory.write_candidate"),
    ]
    for server_key, tool_name, route, expected_capability_key in tool_cases:
        call = McpToolCall(server_key=server_key, tool_name=tool_name, payload_json={})
        assert svc._capability_key_for_tool(call, route=dict(route) if route else None) == expected_capability_key

    has_tool_ref_cases = [
        ("stock_analysis.mcp_orchestration", "aistock-stock-analysis", "stock_analysis_get_quote", True),
        ("stock_analysis.mcp_orchestration", "aistock-external-research", "external_research_search_web", True),
        ("stock_analysis.mcp_orchestration", "aistock-external-research", "external_research_save_evidence", False),
        ("external_research.mcp_orchestration", "aistock-external-research", "external_research_save_evidence", True),
    ]
    for capability_key, server_key, tool_name, expected in has_tool_ref_cases:
        assert svc._capability_has_tool_ref(snapshot_by_key[capability_key], server_key, tool_name) is expected

    card_keys = set(EXPECTED_CAPABILITY_CARD_PROFILES)
    cards = _cards_by_key(svc._capability_cards(list(snapshot_by_key.values()), card_keys))
    for capability_key, expected in EXPECTED_CAPABILITY_CARD_PROFILES.items():
        card = cards[capability_key]
        assert card["status"] == "available"
        assert card["title"]
        assert card["risk"] == expected["risk"]
        assert card["side_effect"] == expected["side_effect"]
        assert card["required_confirmations"] == expected["required_confirmations"]

    for prompt_key, expected_checksum in PROMPT_CHECKSUMS.items():
        snapshot_node = svc.declarative_config.prompt_node(prompt_key)
        assert snapshot_node is not None
        assert snapshot_node["checksum"] == expected_checksum
        assert svc._prompt_text(prompt_key) == snapshot_node["prompt_text"]
        assert svc._prompt_text_for_key(prompt_key) == snapshot_node["prompt_text"]


def test_execution_profiles_digests_and_confirmation_tokens_match_yaml_pins() -> None:
    svc = _service()
    snapshot_by_key = _snapshot_capabilities_by_key(svc)

    for capability_key, selected_tool in SELECTED_TOOL_ROUTES.items():
        capability = snapshot_by_key[capability_key]
        assert svc._capability_tool_refs(capability)

        tool = svc._resolve_capability_tool(capability, payload=selected_tool)
        assert tool is not None
        assert (tool["server_key"], tool["tool_name"]) == (selected_tool["server_key"], selected_tool["tool_name"])

        selected_profile = svc._effective_action_profile(capability, tool)
        assert selected_profile == EXPECTED_TOOL_PROFILE_BY_SELECTED_TOOL[capability_key]

        input_json = {"selected_tool": selected_tool, "fixture": "yaml_authority_pins"}
        assert svc._proposal_digest(
            capability,
            input_json,
            prompt_bundle_signature="prompt-signature",
            runtime_config_activation_id="runtime-activation",
        ) == svc._proposal_digest(
            snapshot_by_key[capability_key],
            input_json,
            prompt_bundle_signature="prompt-signature",
            runtime_config_activation_id="runtime-activation",
        )

    task = svc.repository.create_record(
        "tasks",
        {
            "task_id": "task_db_direct_read_parity",
            "title": "DB direct read parity",
            "task_type": "test",
            "status": "open",
            "created_by": "pytest",
            "input_json": {},
        },
    )
    proposal = svc.create_action_proposal(
        {
            "task_id": task["task_id"],
            "capability_key": "qe.run_experiment",
            "title": "QE run parity proposal",
            "summary": "Characterize confirmation and digest reads for QE run.",
            "input_json": SELECTED_TOOL_ROUTES["qe.run_experiment"],
        }
    )
    assert proposal["risk_level"] == "high"
    assert proposal["side_effect_level"] == "high_cost_compute"
    assert proposal["runtime_config_activation_id"] == svc.active_runtime_config_activation()["activation_id"]
    assert proposal["prompt_bundle_signature"] == svc.active_prompt_activation()["bundle_signature"]
    assert proposal["plan_digest"] == svc._proposal_digest(
        snapshot_by_key["qe.run_experiment"],
        SELECTED_TOOL_ROUTES["qe.run_experiment"],
        prompt_bundle_signature=svc.active_prompt_activation()["bundle_signature"],
        runtime_config_activation_id=svc.active_runtime_config_activation()["activation_id"],
    )

    with pytest.raises(ValueError, match="confirmation_text must be one of capability.required_confirmations"):
        svc.confirm_action_proposal(proposal["action_proposal_id"], {"confirmation_text": "WRONG_CONFIRMATION"})

    confirmed = svc.confirm_action_proposal(
        proposal["action_proposal_id"],
        {"confirmation_text": "CONFIRM_QE_RUN"},
    )
    assert confirmed["status"] == "confirmed"
