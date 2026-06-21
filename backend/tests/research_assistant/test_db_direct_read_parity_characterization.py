from __future__ import annotations

from types import MethodType
from typing import Any

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
    "root.assistant": "b1c474feb9fcc53fd76ed0c66f373b2a9fca075992609caeda52797bda6b99d2",
    "mode.analysis": "d0300f522706bd579d8ce5fa58b914384c0c6cd2bc7c5825e6d2406755bc7e28",
    "mode.planning": "23300cdbe98cf07a6a26c5c01996882a769c58f045ced60df3d510fb2af8fdc1",
    "domain.stock_analysis": "ad269849f7a2cd131cf7b92ccff9a8ab238ed36572fea44931760963d5658480",
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
    assert seeded["seeded"]["runtime_config_activations"] == 1
    assert seeded["seeded"]["capabilities"] == 27
    assert seeded["seeded"]["mcp_tools"] == 378
    assert seeded["seeded"]["prompt_nodes"] == 39
    return svc


def _snapshot_capabilities_by_key(svc: ResearchAssistantService) -> dict[str, dict[str, Any]]:
    return {str(item["capability_key"]): item for item in svc._workflow_capabilities()}


def _seeded_db_capabilities_by_key(svc: ResearchAssistantService) -> dict[str, dict[str, Any]]:
    page = svc.repository.list_records("capabilities", filters={"status": "approved"}, limit=500)
    return {str(item["capability_key"]): item for item in page["items"]}


def _db_backed_capability_lookup_service() -> ResearchAssistantService:
    svc = _service()
    db_by_key = _seeded_db_capabilities_by_key(svc)

    def workflow_capability_by_key(
        self: ResearchAssistantService,
        capability_key: str,
        *,
        approved_only: bool = True,
    ) -> dict[str, Any] | None:
        del self
        capability = db_by_key.get(capability_key)
        if not capability:
            return None
        if approved_only and str(capability.get("status") or "approved") != "approved":
            return None
        return dict(capability)

    def approved_workflow_capabilities(self: ResearchAssistantService) -> list[dict[str, Any]]:
        del self
        return [
            dict(item)
            for item in db_by_key.values()
            if str(item.get("status") or "approved") == "approved"
        ]

    svc._workflow_capability_by_key = MethodType(workflow_capability_by_key, svc)
    svc._approved_workflow_capabilities = MethodType(approved_workflow_capabilities, svc)
    return svc


def _cards_by_key(cards: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(item["capability_key"]): item for item in cards}


def test_tool_lookup_cards_and_prompt_text_match_seeded_db_projection() -> None:
    snapshot_svc = _service()
    db_lookup_svc = _db_backed_capability_lookup_service()
    snapshot_by_key = _snapshot_capabilities_by_key(snapshot_svc)
    db_by_key = _seeded_db_capabilities_by_key(snapshot_svc)
    assert set(snapshot_by_key) == set(db_by_key)
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
        assert snapshot_svc._capability_key_for_tool(call, route=dict(route) if route else None) == expected_capability_key
        assert db_lookup_svc._capability_key_for_tool(call, route=dict(route) if route else None) == expected_capability_key

    has_tool_ref_cases = [
        ("stock_analysis.mcp_orchestration", "aistock-stock-analysis", "stock_analysis_get_quote", True),
        ("stock_analysis.mcp_orchestration", "aistock-external-research", "external_research_search_web", True),
        ("stock_analysis.mcp_orchestration", "aistock-external-research", "external_research_save_evidence", False),
        ("external_research.mcp_orchestration", "aistock-external-research", "external_research_save_evidence", True),
    ]
    for capability_key, server_key, tool_name, expected in has_tool_ref_cases:
        assert snapshot_svc._capability_has_tool_ref(snapshot_by_key[capability_key], server_key, tool_name) is expected
        assert snapshot_svc._capability_has_tool_ref(db_by_key[capability_key], server_key, tool_name) is expected

    card_keys = set(EXPECTED_CAPABILITY_CARD_PROFILES)
    snapshot_cards = _cards_by_key(snapshot_svc._capability_cards(list(snapshot_by_key.values()), card_keys))
    db_cards = _cards_by_key(snapshot_svc._capability_cards(list(db_by_key.values()), card_keys))
    assert snapshot_cards == db_cards
    for capability_key, expected in EXPECTED_CAPABILITY_CARD_PROFILES.items():
        card = snapshot_cards[capability_key]
        assert card["status"] == "available"
        assert card["title"]
        assert card["risk"] == expected["risk"]
        assert card["side_effect"] == expected["side_effect"]
        assert card["required_confirmations"] == expected["required_confirmations"]

    for prompt_key, expected_checksum in PROMPT_CHECKSUMS.items():
        snapshot_node = snapshot_svc.declarative_config.prompt_node(prompt_key)
        db_node = snapshot_svc.repository.find_one("prompt_nodes", {"prompt_key": prompt_key})
        assert snapshot_node is not None
        assert db_node is not None
        assert snapshot_node["checksum"] == expected_checksum
        assert db_node["checksum"] == expected_checksum
        assert snapshot_svc._prompt_text(prompt_key) == db_node["prompt_text"]
        assert snapshot_svc._prompt_text_for_key(prompt_key) == db_node["prompt_text"]


def test_execution_profiles_digests_and_confirmation_tokens_match_seeded_db_projection() -> None:
    svc = _service()
    snapshot_by_key = _snapshot_capabilities_by_key(svc)
    db_by_key = _seeded_db_capabilities_by_key(svc)

    for capability_key, selected_tool in SELECTED_TOOL_ROUTES.items():
        snapshot_capability = snapshot_by_key[capability_key]
        db_capability = db_by_key[capability_key]
        assert svc._capability_tool_refs(snapshot_capability) == svc._capability_tool_refs(db_capability)

        snapshot_tool = svc._resolve_capability_tool(snapshot_capability, payload=selected_tool)
        db_tool = svc._resolve_capability_tool(db_capability, payload=selected_tool)
        assert snapshot_tool is not None
        assert db_tool is not None
        assert (snapshot_tool["server_key"], snapshot_tool["tool_name"]) == (db_tool["server_key"], db_tool["tool_name"])

        assert svc._effective_action_profile(snapshot_capability) == svc._effective_action_profile(db_capability)
        selected_profile = svc._effective_action_profile(snapshot_capability, snapshot_tool)
        assert selected_profile == svc._effective_action_profile(db_capability, db_tool)
        assert selected_profile == EXPECTED_TOOL_PROFILE_BY_SELECTED_TOOL[capability_key]

        input_json = {"selected_tool": selected_tool, "fixture": "db_direct_read_parity"}
        assert svc._proposal_digest(
            snapshot_capability,
            input_json,
            prompt_bundle_signature="prompt-signature",
            runtime_config_activation_id="runtime-activation",
        ) == svc._proposal_digest(
            db_capability,
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
