from __future__ import annotations

import pytest

from backend.services.research_assistant.models import (
    ActionProposalApprovalRequest,
    ActionProposalCreate,
    ActionProposalDecisionRequest,
    ActionProposalExecuteRequest,
    ActionProposalPreflightRequest,
    CapabilitySyncRequest,
    TaskCreate,
)
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import ResearchAssistantService


def _service() -> ResearchAssistantService:
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository())
    svc.seed_catalogs()
    return svc


def _qe_payload() -> dict[str, object]:
    return {
        "template_kind": "custom_evo",
        "title": "QE experiment draft",
        "config_json": {
            "loops": [{"factor_keys": ["alpha001"], "model_id": "lightgbm"}],
            "stock_pool": "fixed_pit_pool",
            "backtest_window": {"start": "2023-01-01", "end": "2024-12-31"},
        },
    }


def _proposal(svc: ResearchAssistantService, *, capability_key: str = "qe.create_experiment_draft", payload: dict[str, object] | None = None) -> dict[str, object]:
    task = svc.create_task(TaskCreate(title="QE execution closure"))
    return svc.create_action_proposal(
        ActionProposalCreate(
            task_id=task["task_id"],
            capability_key=capability_key,
            proposal_type="workflow_pack" if capability_key.endswith("draft") else "mcp_tool",
            title="QE action",
            summary="生成或校验 QE template",
            input_json=payload or _qe_payload(),
        )
    )


def test_capability_sync_dry_run_and_apply_excludes_blocked_catalog_entries() -> None:
    svc = _service()

    dry_run = svc.sync_capabilities(CapabilitySyncRequest(apply=False))
    assert dry_run["dry_run"] is True
    assert dry_run["source_count"] >= 10
    assert dry_run["applied_count"] == 0
    assert all(item["status"] == "approved" for item in dry_run["diff"])

    applied = svc.sync_capabilities(CapabilitySyncRequest(apply=True))
    assert applied["applied_count"] == 0
    capabilities = svc.list_records("capabilities", filters={"status": "approved"})
    assert capabilities["total"] >= 10
    qe_draft = svc.repository.find_one("capabilities", {"capability_key": "qe.create_experiment_draft"})
    assert qe_draft is not None
    assert qe_draft["checksum"]
    assert qe_draft["risk_level"] == "medium"
    assert qe_draft["side_effect_level"] == "draft_only"
    assert "生成 QE" in qe_draft["description_for_llm"]


def test_action_proposal_digest_preflight_and_dry_run_boundaries() -> None:
    svc = _service()
    proposal = _proposal(svc)
    assert proposal["status"] == "proposed"
    assert proposal["plan_digest"]

    dry_run = svc.execute_action_proposal(proposal["action_proposal_id"], ActionProposalExecuteRequest(dry_run=True))
    assert dry_run["executed"] is False
    assert svc.repository.get_record("action_proposals", proposal["action_proposal_id"])["status"] == "proposed"

    with pytest.raises(ValueError, match="confirmation_text"):
        svc.confirm_action_proposal(proposal["action_proposal_id"], ActionProposalDecisionRequest(confirmation_text="WRONG"))
    confirmed = svc.confirm_action_proposal(proposal["action_proposal_id"], ActionProposalDecisionRequest(confirmation_text="CONFIRM_QE_DRAFT"))
    assert confirmed["status"] == "confirmed"

    preflight = svc.preflight_action_proposal(proposal["action_proposal_id"], ActionProposalPreflightRequest())
    assert preflight["proposal"]["status"] == "approval_required"
    assert preflight["preflight"]["approval_required"] is True
    assert preflight["preflight"]["assistant_usable"] == "preflight_required"

    capability = svc.repository.find_one("capabilities", {"capability_key": proposal["capability_key"]})
    svc.repository.update_record("capabilities", capability["capability_id"], {"checksum": "stale-checksum"})
    with pytest.raises(ValueError, match="plan_digest"):
        svc.execute_action_proposal(proposal["action_proposal_id"], ActionProposalExecuteRequest())


def test_execution_gateway_writes_mcp_task_and_trace_events() -> None:
    svc = _service()
    proposal = _proposal(svc)
    svc.confirm_action_proposal(proposal["action_proposal_id"], {"confirmation_text": "CONFIRM_QE_DRAFT"})
    svc.preflight_action_proposal(proposal["action_proposal_id"], {})
    svc.approve_action_proposal(proposal["action_proposal_id"], ActionProposalApprovalRequest(confirmation_text="CONFIRM_QE_DRAFT"))

    result = svc.execute_action_proposal(proposal["action_proposal_id"], {})
    assert result["status"] == "succeeded"
    assert result["executed"] is True
    assert result["human_cards"][0]["template_id"].startswith("qet_")
    assert result["tool_event"]["action_proposal_id"] == proposal["action_proposal_id"]
    assert result["tool_event"]["result_card_json"]["title"] == "QE template 草案已生成"

    events = svc.action_proposal_events(proposal["action_proposal_id"])
    assert events["mcp_tool_events"]
    assert events["trace_events"]
    assert any(event["event_type"] == "mcp_done" for event in events["task_events"])


def test_preflight_failure_blocks_execute_and_records_recovery_details() -> None:
    svc = _service()
    proposal = _proposal(svc, payload={"title": "missing required payload"})
    svc.confirm_action_proposal(proposal["action_proposal_id"], {"confirmation_text": "CONFIRM_QE_DRAFT"})
    preflight = svc.preflight_action_proposal(proposal["action_proposal_id"], {})
    assert preflight["proposal"]["status"] == "preflight_failed"
    assert preflight["preflight"]["failed_checks"][0]["check"] == "input_schema"

    result = svc.execute_action_proposal(proposal["action_proposal_id"], {})
    assert result["executed"] is False
    assert result["error"]["code"] == "preflight_or_approval_missing"
    assert result["error"]["audit_link"].endswith(proposal["action_proposal_id"])


def test_high_risk_approval_gate_multimodel_and_qe_run_guards() -> None:
    svc = _service()
    proposal = _proposal(
        svc,
        capability_key="qe.run_experiment",
        payload={"template_id": "qet_demo", "confirm_run": "QE_EXPERIMENT_RUN"},
    )
    svc.confirm_action_proposal(proposal["action_proposal_id"], {"confirmation_text": "CONFIRM_QE_RUN"})
    preflight = svc.preflight_action_proposal(proposal["action_proposal_id"], {})
    assert preflight["proposal"]["status"] == "approval_required"

    blocked_worker = svc.execute_action_proposal(proposal["action_proposal_id"], {"actor_role": "secondary_worker"})
    assert blocked_worker["error"]["code"] == "multi_model_boundary_blocked"

    fresh = _proposal(
        svc,
        capability_key="qe.run_experiment",
        payload={"template_id": "qet_demo", "confirm_run": "QE_EXPERIMENT_RUN"},
    )
    svc.confirm_action_proposal(fresh["action_proposal_id"], {"confirmation_text": "CONFIRM_QE_RUN"})
    svc.preflight_action_proposal(fresh["action_proposal_id"], {})
    missing_approval = svc.execute_action_proposal(fresh["action_proposal_id"], {})
    assert missing_approval["error"]["code"] == "approval_missing"

    approved = _proposal(
        svc,
        capability_key="qe.run_experiment",
        payload={"template_id": "qet_demo", "confirm_run": "QE_EXPERIMENT_RUN"},
    )
    svc.confirm_action_proposal(approved["action_proposal_id"], {"confirmation_text": "CONFIRM_QE_RUN"})
    svc.preflight_action_proposal(approved["action_proposal_id"], {})
    with pytest.raises(ValueError, match="approval confirmation_text"):
        svc.approve_action_proposal(approved["action_proposal_id"], ActionProposalApprovalRequest())
    approval = svc.approve_action_proposal(approved["action_proposal_id"], ActionProposalApprovalRequest(confirmation_text="CONFIRM_QE_RUN"))
    assert approval["approval"]["plan_digest"] == approved["plan_digest"]
    result = svc.execute_action_proposal(approved["action_proposal_id"], {})
    assert result["executed"] is False
    assert result["error"]["code"] == "adapter_not_enabled_for_high_cost_qe"
    assert "materialize 或 run" in result["error"]["human_reason"]


def test_qe_validate_can_show_summary_without_materialize_or_run() -> None:
    svc = _service()
    proposal = _proposal(
        svc,
        capability_key="qe.validate_template",
        payload={
            "template_id": "qet_demo",
            "template_kind": "custom_evo",
            "config_json": {
                "loops": [{"factor_keys": ["alpha001"], "model_id": "lightgbm"}],
                "stock_pool": "fixed_pit_pool",
                "backtest_window": {"start": "2023-01-01", "end": "2024-12-31"},
            },
        },
    )
    svc.confirm_action_proposal(proposal["action_proposal_id"], {"confirmation_text": "CONFIRM_QE_VALIDATE"})
    preflight = svc.preflight_action_proposal(proposal["action_proposal_id"], {})
    assert preflight["proposal"]["status"] == "approval_required"

    approval = svc.approve_action_proposal(
        proposal["action_proposal_id"],
        ActionProposalApprovalRequest(confirmation_text="CONFIRM_QE_VALIDATE"),
    )
    assert approval["approval"]["plan_digest"] == proposal["plan_digest"]
    result = svc.execute_action_proposal(proposal["action_proposal_id"], {})
    assert result["status"] == "succeeded"
    assert result["tool_event"]["response_json"]["diff_summary"]["materialize"] is False
    assert result["tool_event"]["response_json"]["diff_summary"]["run"] is False


def test_execution_gateway_uses_runtime_retry_policy_for_retryable_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = _service()
    activation = svc.active_runtime_config_activation()
    config = dict(activation["config_json"])
    config["execution"] = dict(config["execution"])
    config["execution"]["max_retries"] = 1
    config["execution"]["retryable_error_codes"] = ["transient_network"]
    svc.repository.update_record("runtime_config_activations", activation["activation_id"], {"config_json": config})

    proposal = _proposal(svc)
    svc.confirm_action_proposal(proposal["action_proposal_id"], {"confirmation_text": "CONFIRM_QE_DRAFT"})
    svc.preflight_action_proposal(proposal["action_proposal_id"], {})
    svc.approve_action_proposal(proposal["action_proposal_id"], ActionProposalApprovalRequest(confirmation_text="CONFIRM_QE_DRAFT"))
    calls = {"count": 0}

    def flaky_tool(tool: dict[str, object], payload: dict[str, object]) -> dict[str, object]:
        calls["count"] += 1
        if calls["count"] == 1:
            return {
                "status": "failed",
                "result_json": {},
                "result_cards": [{"title": "temporary outage", "summary": "transient network"}],
                "artifact_refs": [],
                "error_json": {"code": "transient_network"},
                "retry_count": 0,
            }
        return {
            "status": "succeeded",
            "result_json": {"ok": True},
            "result_cards": [{"title": "retry succeeded", "summary": "ok"}],
            "artifact_refs": ["retry-ok"],
            "error_json": {},
            "retry_count": 1,
        }

    monkeypatch.setattr(svc, "_execute_loopback_tool", flaky_tool)
    result = svc.execute_action_proposal(proposal["action_proposal_id"], {})

    assert result["status"] == "succeeded"
    assert calls["count"] == 2
    events = svc.action_proposal_events(proposal["action_proposal_id"])
    assert any(event["event_type"] == "mcp_retry" for event in events["task_events"])
    failed_events = [event for event in events["mcp_tool_events"] if event["status"] == "failed"]
    assert failed_events[0]["attempt_index"] == 0
    assert failed_events[0]["error_json"]["retry_policy"]["max_retries"] == 1


def test_execution_gateway_does_not_retry_non_retryable_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = _service()
    proposal = _proposal(svc)
    svc.confirm_action_proposal(proposal["action_proposal_id"], {"confirmation_text": "CONFIRM_QE_DRAFT"})
    svc.preflight_action_proposal(proposal["action_proposal_id"], {})
    svc.approve_action_proposal(proposal["action_proposal_id"], ActionProposalApprovalRequest(confirmation_text="CONFIRM_QE_DRAFT"))
    calls = {"count": 0}

    def schema_failure(tool: dict[str, object], payload: dict[str, object]) -> dict[str, object]:
        calls["count"] += 1
        return {
            "status": "failed",
            "result_json": {},
            "result_cards": [{"title": "schema invalid", "summary": "schema_invalid"}],
            "artifact_refs": [],
            "error_json": {"code": "schema_invalid", "retryable": True},
            "retry_count": 0,
        }

    monkeypatch.setattr(svc, "_execute_loopback_tool", schema_failure)
    result = svc.execute_action_proposal(proposal["action_proposal_id"], {})

    assert result["status"] == "failed"
    assert result["executed"] is False
    assert result["error"]["code"] == "schema_invalid"
    assert result["error"]["retryable"] is False
    assert calls["count"] == 1



def _summary_read_proposal(svc: ResearchAssistantService, *, message: str, capability_key: str, route: dict[str, object]) -> dict[str, object]:
    task = svc.create_task(TaskCreate(title=f"Summary read: {capability_key}"))
    return svc.create_action_proposal(
        ActionProposalCreate(
            task_id=task["task_id"],
            capability_key=capability_key,
            proposal_type="mcp_tool",
            title="Summary-first MCP read",
            summary=message,
            input_json={"request": message, "route": route, "limit": 5},
        )
    )


def _assert_summary_response(payload: dict[str, object]) -> None:
    forbidden = {"metrics_json", "config_json", "raw_payload", "matrix", "logs", "rows", "model_weights", "training_curves"}

    def walk(value: object) -> None:
        if isinstance(value, dict):
            assert not (set(value) & forbidden)
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    assert payload["summary_first"] is True
    assert payload["response_mode"] == "summary"
    assert payload["source"] == "research_assistant_catalog_summary_adapter"
    assert payload["live_backend_called"] is False
    assert payload["items"]
    assert payload["omitted_sections"]
    assert payload["artifact_refs"]
    walk(payload)


def test_routed_read_only_mcp_tool_executes_summary_first_without_confirmation() -> None:
    svc = _service()
    route = {
        "domain": "factor_library",
        "server_key": "aistock-factor-library",
        "tool_name": "factor_library_list",
        "side_effect": "read_only",
    }
    proposal = _summary_read_proposal(
        svc,
        message="List available factor library entries as a compact summary.",
        capability_key="factor_library.mcp_orchestration",
        route=route,
    )

    assert proposal["status"] == "proposed"
    assert proposal["risk_level"] == "low"
    assert proposal["side_effect_level"] == "read_only"

    preflight = svc.preflight_action_proposal(proposal["action_proposal_id"], ActionProposalPreflightRequest())
    assert preflight["proposal"]["status"] == "preflight_passed"
    assert preflight["preflight"]["approval_required"] is False
    assert preflight["preflight"]["tool_name"] == "factor_library_list"

    result = svc.execute_action_proposal(proposal["action_proposal_id"], ActionProposalExecuteRequest())
    assert result["executed"] is True
    assert result["tool_event"]["server_key"] == "aistock-factor"
    assert result["tool_event"]["tool_name"] == "factor_library_list"
    assert result["tool_event"]["transport"] == "research_assistant_catalog_summary_adapter"
    _assert_summary_response(result["tool_event"]["response_json"])


def test_routed_read_only_uses_selected_tool_not_first_capability_ref() -> None:
    svc = _service()
    route = {
        "domain": "execution_policy",
        "server_key": "aistock-execution-policy",
        "tool_name": "execution_policy_get_market_state_constraints",
        "side_effect": "read_only",
    }
    proposal = _summary_read_proposal(
        svc,
        message="Show execution policy market-state constraints.",
        capability_key="execution_policy.mcp_orchestration",
        route=route,
    )
    svc.preflight_action_proposal(proposal["action_proposal_id"], {})
    result = svc.execute_action_proposal(proposal["action_proposal_id"], {})

    assert result["executed"] is True
    assert result["tool_event"]["tool_name"] == "execution_policy_get_market_state_constraints"
    assert result["tool_event"]["tool_name"] != "execution_policy_list_algos"
    _assert_summary_response(result["tool_event"]["response_json"])


def test_summary_read_rejects_route_tool_outside_capability_refs() -> None:
    svc = _service()
    task = svc.create_task(TaskCreate(title="Rejected route"))
    with pytest.raises(ValueError, match="selected MCP tool is not allowed"):
        svc.create_action_proposal(
            ActionProposalCreate(
                task_id=task["task_id"],
                capability_key="factor_library.mcp_orchestration",
                proposal_type="mcp_tool",
                title="Bad route",
                summary="should fail",
                input_json={
                    "request": "use the wrong tool",
                    "route": {
                        "server_key": "aistock-execution-policy",
                        "tool_name": "execution_policy_bind_confirmed",
                    },
                },
            )
        )


def test_route_selected_confirmed_tool_still_requires_confirmation_and_approval() -> None:
    svc = _service()
    task = svc.create_task(TaskCreate(title="Confirmed route"))
    proposal = svc.create_action_proposal(
        ActionProposalCreate(
            task_id=task["task_id"],
            capability_key="execution_policy.mcp_orchestration",
            proposal_type="mcp_tool",
            title="Bind execution policy",
            summary="confirmed action remains gated",
            input_json={
                "request": "??????",
                "route": {
                    "domain": "execution_policy",
                    "server_key": "aistock-execution-policy",
                    "tool_name": "execution_policy_bind_confirmed",
                    "side_effect": "confirmed_action",
                },
            },
        )
    )

    assert proposal["risk_level"] == "high"
    assert proposal["side_effect_level"] == "high_cost_compute"
    with pytest.raises(ValueError, match="confirmation_text"):
        svc.confirm_action_proposal(proposal["action_proposal_id"], {"confirmation_text": "WRONG"})
    svc.confirm_action_proposal(proposal["action_proposal_id"], {"confirmation_text": "BIND_EXECUTION_POLICY"})
    preflight = svc.preflight_action_proposal(proposal["action_proposal_id"], {})
    assert preflight["proposal"]["status"] == "approval_required"
    result = svc.execute_action_proposal(proposal["action_proposal_id"], {})
    assert result["executed"] is False
    assert result["error"]["code"] == "approval_missing"
