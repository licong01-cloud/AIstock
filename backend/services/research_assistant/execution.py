"""Execution-closure helpers for the Research Assistant service.

This mixin keeps MCP/Skill execution gates explicit while the service remains
the owner of repositories, task events, trace events, and runtime config.
"""

from __future__ import annotations

from datetime import timedelta
from time import perf_counter
from typing import Any

from .models import (
    ActionProposalApprovalRequest,
    ActionProposalCreate,
    ActionProposalDecisionRequest,
    ActionProposalExecuteRequest,
    ActionProposalPreflightRequest,
    CapabilitySyncRequest,
    ContextPackBuildRequest,
    IssueCandidateCreate,
    McpPreflightRequest,
    MemoryCreate,
    TaskCreate,
    TaskEventCreate,
    TraceEventCreate,
    new_id,
    sha256_json,
    utc_now,
)

ASSISTANT_APPROVAL_CONFIRM_TEXT = "APPROVE_RESEARCH_ASSISTANT_ACTION"


class ResearchAssistantExecutionMixin:
    """Capability sync, Action Proposal, and execution gateway behavior."""

    def _normalize_capability_catalog(self, *, include_disabled: bool = False) -> list[dict[str, Any]]:
        approved_tools = {
            (str(tool.get("server_key")), str(tool.get("tool_name"))): tool
            for tool in self.repository.list_records("mcp_tools", limit=self.configured_limit("api_list_mcp_tools"))["items"]
            if include_disabled or str(tool.get("status")) in {"enabled", "approved", "ready"}
        }
        approved_skills = {
            str(skill.get("skill_key")): skill
            for skill in self.repository.list_records("skills", limit=self.configured_limit("api_list_skills"))["items"]
            if include_disabled or str(skill.get("status")) == "approved"
        }
        now = utc_now().isoformat()
        capabilities: list[dict[str, Any]] = []
        for item in self.default_workflow_capabilities():
            mcp_refs = list(item.get("mcp_tool_refs") or [])
            skill_refs = [str(ref) for ref in item.get("skill_refs") or []]
            missing_refs = [ref for ref in mcp_refs if (str(ref.get("server_key")), str(ref.get("tool_name"))) not in approved_tools]
            missing_skills = [ref for ref in skill_refs if ref not in approved_skills]
            status = str(item.get("status") or "approved")
            if missing_refs or missing_skills:
                status = "blocked"
            if not include_disabled and status in {"disabled", "deprecated", "blocked"}:
                continue
            payload = {
                "capability_id": f"cap_{str(item['capability_key']).replace('.', '_').replace('-', '_')}",
                "last_synced_at": now,
                **item,
                "status": status,
            }
            checksum_payload = {
                k: v
                for k, v in payload.items()
                if k not in {"capability_id", "last_synced_at", "checksum", "created_at", "updated_at"}
            }
            if missing_refs or missing_skills:
                checksum_payload["missing_refs"] = {"mcp": missing_refs, "skills": missing_skills}
            payload["checksum"] = sha256_json(checksum_payload)
            capabilities.append(payload)
        return capabilities

    def sync_capabilities(self, request: CapabilitySyncRequest | dict[str, Any] | None = None) -> dict[str, Any]:
        data = request if isinstance(request, CapabilitySyncRequest) else CapabilitySyncRequest(**(request or {}))
        runtime_config = self.active_runtime_config()
        sync_cfg = runtime_config["capability_sync"]
        if not bool(sync_cfg.get("enabled", True)):
            raise ValueError("capability sync is disabled by runtime config")
        capabilities = self._normalize_capability_catalog(include_disabled=data.include_disabled)
        max_tools = int(sync_cfg["max_tools_per_server"])
        if len(capabilities) > max_tools:
            raise ValueError(f"capability sync exceeded runtime limit: {max_tools}")
        existing = self.repository.list_records("capabilities", limit=self.configured_limit("api_list_capabilities"))["items"]
        existing_by_key = {str(item.get("capability_key")): item for item in existing}
        diff: list[dict[str, Any]] = []
        applied_count = 0
        for capability in capabilities:
            current = existing_by_key.get(str(capability["capability_key"]))
            change = (
                "create"
                if not current
                else "unchanged"
                if current.get("checksum") == capability["checksum"] and current.get("status") == capability["status"]
                else "update"
            )
            diff.append(
                {
                    "capability_key": capability["capability_key"],
                    "change": change,
                    "status": capability["status"],
                    "risk_level": capability["risk_level"],
                    "side_effect_level": capability["side_effect_level"],
                    "checksum": capability["checksum"],
                }
            )
            if data.apply and change in {"create", "update"}:
                self.repository.create_record("capabilities", capability)
                applied_count += 1
        self.create_trace_event(
            TraceEventCreate(
                event_type="capability_sync",
                component="research_assistant.capability_sync",
                status="applied" if data.apply else "dry_run",
                payload_json={"source_count": len(capabilities), "applied_count": applied_count, "diff": diff[:20]},
            )
        )
        return {
            "dry_run": not data.apply,
            "requested_by": data.requested_by,
            "source_count": len(capabilities),
            "applied_count": applied_count,
            "diff": diff,
            "blocked_or_disabled_excluded": not data.include_disabled,
            "runtime_config": {
                "max_tools_per_server": max_tools,
                "timeout_seconds": sync_cfg["timeout_seconds"],
                "require_checksum": sync_cfg["require_checksum"],
            },
        }

    @staticmethod
    def _side_effect_requires_approval(side_effect_level: str, risk_level: str) -> bool:
        return side_effect_level in {"write_nonprod", "high_cost_compute", "production_sensitive"} or risk_level in {"high", "production_sensitive"}

    def _resolve_capability_tool(self, capability: dict[str, Any]) -> dict[str, Any] | None:
        refs = list(capability.get("mcp_tool_refs") or [])
        if not refs:
            return None
        ref = refs[0]
        return self.repository.find_one("mcp_tools", {"server_key": ref["server_key"], "tool_name": ref["tool_name"]})

    def _execution_policy(self, capability: dict[str, Any]) -> dict[str, Any]:
        cfg = self.active_runtime_config()["execution"]
        return {
            "timeout_seconds": int(cfg["high_cost_timeout_seconds"] if capability["side_effect_level"] == "high_cost_compute" else cfg["default_timeout_seconds"]),
            "max_retries": int(cfg["max_retries"]),
            "retryable_error_codes": {str(item) for item in cfg.get("retryable_error_codes", [])},
            "non_retryable_error_codes": {str(item) for item in cfg.get("non_retryable_error_codes", [])},
            "cancel_check_interval_seconds": int(cfg["cancel_check_interval_seconds"]),
        }

    @staticmethod
    def _normalize_adapter_error(adapter_result: dict[str, Any], result_cards: list[dict[str, Any]], action_proposal_id: str) -> dict[str, Any]:
        raw_error = dict(adapter_result.get("error_json") or {})
        error_code = str(raw_error.get("code") or "execution_failed")
        return {
            "code": error_code,
            "human_reason": raw_error.get("human_reason") or (result_cards[0].get("summary") if result_cards else "MCP execution failed."),
            "next_step": raw_error.get("next_step") or "根据审计事件修正输入、重新 preflight 或重新创建 Action Proposal。",
            "audit_link": f"/research-assistant/actions/{action_proposal_id}",
            "retryable": bool(raw_error.get("retryable", adapter_result.get("retryable", False))),
        }

    def _proposal_digest(self, capability: dict[str, Any], input_json: dict[str, Any], *, prompt_bundle_signature: str | None, runtime_config_activation_id: str | None) -> str:
        return sha256_json(
            {
                "capability_key": capability["capability_key"],
                "capability_checksum": capability["checksum"],
                "input_json": input_json,
                "mcp_tool_refs": capability.get("mcp_tool_refs") or [],
                "skill_refs": capability.get("skill_refs") or [],
                "prompt_bundle_signature": prompt_bundle_signature,
                "runtime_config_activation_id": runtime_config_activation_id,
                "risk_level": capability.get("risk_level"),
                "side_effect_level": capability.get("side_effect_level"),
                "required_confirmations": capability.get("required_confirmations") or [],
            }
        )

    def _assert_proposal_digest_current(self, proposal: dict[str, Any], capability: dict[str, Any]) -> None:
        expected = self._proposal_digest(
            capability,
            dict(proposal.get("input_json") or {}),
            prompt_bundle_signature=proposal.get("prompt_bundle_signature"),
            runtime_config_activation_id=proposal.get("runtime_config_activation_id"),
        )
        if expected != proposal.get("plan_digest"):
            raise ValueError("proposal plan_digest is stale; recreate proposal before approval or execution")

    def create_action_proposal(self, request: ActionProposalCreate | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, ActionProposalCreate) else ActionProposalCreate(**request)
        if not self.repository.get_record("tasks", data.task_id):
            raise KeyError(f"task not found: {data.task_id}")
        capability = self.repository.find_one("capabilities", {"capability_key": data.capability_key, "status": "approved"})
        if not capability:
            raise KeyError(f"approved capability not found: {data.capability_key}")
        runtime_activation = self.active_runtime_config_activation()
        prompt_activation = self.active_prompt_activation()
        input_json = dict(data.input_json)
        plan_digest = self._proposal_digest(
            capability,
            input_json,
            prompt_bundle_signature=prompt_activation.get("bundle_signature"),
            runtime_config_activation_id=runtime_activation["activation_id"],
        )
        idempotency_key = data.idempotency_key or sha256_json({"task_id": data.task_id, "capability_key": data.capability_key, "input_json": input_json})
        existing = self.repository.find_one("action_proposals", {"idempotency_key": idempotency_key})
        if existing:
            return existing
        ttl_minutes = data.expires_in_minutes or int(self._config_path_value(self.active_runtime_config(), "approval_policy.approval_ttl_minutes"))
        proposal = self.repository.create_record(
            "action_proposals",
            {
                "action_proposal_id": new_id("actprop"),
                "task_id": data.task_id,
                "conversation_id": data.conversation_id,
                "capability_key": data.capability_key,
                "proposal_type": data.proposal_type,
                "title": data.title,
                "summary": data.summary,
                "risk_level": capability["risk_level"],
                "side_effect_level": capability["side_effect_level"],
                "input_json": input_json,
                "expected_result_json": data.expected_result_json,
                "plan_digest": plan_digest,
                "prompt_bundle_signature": prompt_activation.get("bundle_signature"),
                "runtime_config_activation_id": runtime_activation["activation_id"],
                "context_pack_id": data.context_pack_id,
                "status": "proposed",
                "idempotency_key": idempotency_key,
                "expires_at": (utc_now() + timedelta(minutes=ttl_minutes)).isoformat(),
                "created_by": data.created_by,
            },
        )
        self.add_task_event(
            data.task_id,
            TaskEventCreate(
                event_type="action_proposed",
                severity="warning" if self._side_effect_requires_approval(capability["side_effect_level"], capability["risk_level"]) else "info",
                message=f"已生成 Action Proposal：{data.title}；确认前不会执行。",
                payload_json={"action_proposal_id": proposal["action_proposal_id"], "capability_key": data.capability_key, "plan_digest": plan_digest},
            ),
        )
        return proposal

    def get_action_proposal(self, action_proposal_id: str) -> dict[str, Any]:
        proposal = self.repository.get_record("action_proposals", action_proposal_id)
        if not proposal:
            raise KeyError(f"action proposal not found: {action_proposal_id}")
        capability = self.repository.find_one("capabilities", {"capability_key": proposal["capability_key"]})
        return {"proposal": proposal, "capability": capability, "events": self.action_proposal_events(action_proposal_id)}

    def action_proposal_events(self, action_proposal_id: str) -> dict[str, Any]:
        proposal = self.repository.get_record("action_proposals", action_proposal_id)
        if not proposal:
            raise KeyError(f"action proposal not found: {action_proposal_id}")
        task_events = self.repository.list_records("task_events", filters={"task_id": proposal["task_id"]}, limit=self.configured_limit("task_events_detail"))["items"]
        mcp_events = self.repository.list_records("mcp_tool_events", filters={"task_id": proposal["task_id"]}, limit=self.configured_limit("api_list_mcp_tool_events"))["items"]
        trace_events = self.repository.list_records("trace_events", filters={"task_id": proposal["task_id"]}, limit=self.configured_limit("api_list_trace_events"))["items"]

        def _matches(event: dict[str, Any]) -> bool:
            payloads = [event.get("payload_json"), event.get("request_json"), event.get("response_json"), event.get("error_json")]
            return event.get("action_proposal_id") == action_proposal_id or any(
                isinstance(payload, dict) and payload.get("action_proposal_id") == action_proposal_id for payload in payloads
            )

        return {
            "task_events": [event for event in task_events if _matches(event)],
            "mcp_tool_events": [event for event in mcp_events if _matches(event)],
            "trace_events": [event for event in trace_events if _matches(event)],
        }

    def confirm_action_proposal(self, action_proposal_id: str, request: ActionProposalDecisionRequest | dict[str, Any] | None = None) -> dict[str, Any]:
        data = request if isinstance(request, ActionProposalDecisionRequest) else ActionProposalDecisionRequest(**(request or {}))
        proposal = self.repository.get_record("action_proposals", action_proposal_id)
        if not proposal:
            raise KeyError(f"action proposal not found: {action_proposal_id}")
        if proposal.get("status") not in {"proposed", "preflight_failed"}:
            raise ValueError(f"proposal is not confirmable from status={proposal.get('status')}")
        capability = self.repository.find_one("capabilities", {"capability_key": proposal["capability_key"], "status": "approved"})
        if not capability:
            raise KeyError(f"approved capability not found: {proposal['capability_key']}")
        self._assert_proposal_digest_current(proposal, capability)
        required = list(capability.get("required_confirmations") or [])
        if required and data.confirmation_text not in required:
            raise ValueError(f"confirmation_text must be one of capability.required_confirmations: {required}")
        updated = self.repository.update_record("action_proposals", action_proposal_id, {"status": "confirmed"})
        self.add_task_event(
            proposal["task_id"],
            TaskEventCreate(event_type="approved", message=f"Action Proposal 已确认：{proposal['title']}", payload_json={"action_proposal_id": action_proposal_id, "confirmed_by": data.decided_by}),
        )
        return updated

    def reject_action_proposal(self, action_proposal_id: str, request: ActionProposalDecisionRequest | dict[str, Any] | None = None) -> dict[str, Any]:
        data = request if isinstance(request, ActionProposalDecisionRequest) else ActionProposalDecisionRequest(**(request or {}))
        proposal = self.repository.get_record("action_proposals", action_proposal_id)
        if not proposal:
            raise KeyError(f"action proposal not found: {action_proposal_id}")
        updated = self.repository.update_record("action_proposals", action_proposal_id, {"status": "rejected"})
        self.add_task_event(
            proposal["task_id"],
            TaskEventCreate(event_type="rejected", severity="warning", message=f"Action Proposal 已拒绝：{proposal['title']}", payload_json={"action_proposal_id": action_proposal_id, "decided_by": data.decided_by}),
        )
        return updated

    def preflight_action_proposal(self, action_proposal_id: str, request: ActionProposalPreflightRequest | dict[str, Any] | None = None) -> dict[str, Any]:
        data = request if isinstance(request, ActionProposalPreflightRequest) else ActionProposalPreflightRequest(**(request or {}))
        proposal = self.repository.get_record("action_proposals", action_proposal_id)
        if not proposal:
            raise KeyError(f"action proposal not found: {action_proposal_id}")
        capability = self.repository.find_one("capabilities", {"capability_key": proposal["capability_key"], "status": "approved"})
        if not capability:
            raise KeyError(f"approved capability not found: {proposal['capability_key']}")
        self._assert_proposal_digest_current(proposal, capability)
        if proposal.get("status") not in {"confirmed", "approval_required", "approved", "preflight_failed"}:
            raise ValueError(f"proposal must be confirmed before preflight; status={proposal.get('status')}")
        tool = self._resolve_capability_tool(capability)
        payload = data.payload_json or dict(proposal.get("input_json") or {})
        if tool:
            result = self.preflight_mcp_tool(
                McpPreflightRequest(
                    task_id=proposal["task_id"],
                    server_key=tool["server_key"],
                    tool_name=tool["tool_name"],
                    payload_json=payload,
                    idempotency_key=data.idempotency_key or proposal["idempotency_key"],
                )
            )
            self.repository.update_record(
                "mcp_tool_events",
                result["tool_event_id"],
                {"action_proposal_id": action_proposal_id, "plan_digest": proposal["plan_digest"], "transport": "loopback_http"},
            )
        else:
            result = {
                "passed": True,
                "approval_required": self._side_effect_requires_approval(capability["side_effect_level"], capability["risk_level"]),
                "failed_checks": [],
                "preflight_checks": ["capability_status", "skill_registry"],
                "payload_digest": sha256_json(payload),
            }
        next_status = "preflight_failed" if result.get("failed_checks") else "approval_required" if result.get("approval_required") else "preflight_passed"
        updated = self.repository.update_record("action_proposals", action_proposal_id, {"status": next_status})
        trace = self.create_trace_event(
            TraceEventCreate(
                task_id=proposal["task_id"],
                event_type="action_preflight",
                component="research_assistant.execution_gateway",
                status=next_status,
                payload_json={"action_proposal_id": action_proposal_id, "preflight": result},
            )
        )
        return {"proposal": updated, "preflight": result, "trace_id": trace["trace_id"]}

    def approve_action_proposal(self, action_proposal_id: str, request: ActionProposalApprovalRequest | dict[str, Any] | None = None) -> dict[str, Any]:
        data = request if isinstance(request, ActionProposalApprovalRequest) else ActionProposalApprovalRequest(**(request or {}))
        proposal = self.repository.get_record("action_proposals", action_proposal_id)
        if not proposal:
            raise KeyError(f"action proposal not found: {action_proposal_id}")
        capability = self.repository.find_one("capabilities", {"capability_key": proposal["capability_key"], "status": "approved"})
        if not capability:
            raise KeyError(f"approved capability not found: {proposal['capability_key']}")
        self._assert_proposal_digest_current(proposal, capability)
        if proposal.get("status") not in {"approval_required", "approved"}:
            raise ValueError(f"proposal approval requires approval_required status; status={proposal.get('status')}")
        required_texts = [str(item) for item in (capability.get("required_confirmations") or [ASSISTANT_APPROVAL_CONFIRM_TEXT])]
        if not data.confirmation_text or data.confirmation_text not in required_texts:
            raise ValueError(f"approval confirmation_text must be one of capability.required_confirmations: {required_texts}")
        required_text = data.confirmation_text
        approval = self.repository.create_record(
            "approvals",
            {
                "approval_id": new_id("appr"),
                "task_id": proposal["task_id"],
                "approval_type": "action_proposal.execute",
                "risk_level": proposal["risk_level"],
                "plan_digest": proposal["plan_digest"],
                "config_version_id": proposal.get("runtime_config_activation_id"),
                "summary": f"{proposal['title']} ({proposal['capability_key']})",
                "required_confirmation_text": required_text,
                "status": "approved",
                "approval_context_json": {"action_proposal_id": action_proposal_id, "capability_checksum": capability["checksum"]},
                "approved_by": data.approved_by,
                "approval_text": required_text,
                "decided_at": utc_now().isoformat(),
                "approved_at": utc_now().isoformat(),
                "created_by": "research_assistant_action_gate",
            },
        )
        updated = self.repository.update_record("action_proposals", action_proposal_id, {"status": "approved", "approval_id": approval["approval_id"]})
        self.add_task_event(
            proposal["task_id"],
            TaskEventCreate(event_type="approved", message=f"Action Proposal 已审批：{proposal['title']}", payload_json={"action_proposal_id": action_proposal_id, "approval_id": approval["approval_id"]}),
        )
        return {"proposal": updated, "approval": approval}

    def execute_action_proposal(self, action_proposal_id: str, request: ActionProposalExecuteRequest | dict[str, Any] | None = None) -> dict[str, Any]:
        data = request if isinstance(request, ActionProposalExecuteRequest) else ActionProposalExecuteRequest(**(request or {}))
        proposal = self.repository.get_record("action_proposals", action_proposal_id)
        if not proposal:
            raise KeyError(f"action proposal not found: {action_proposal_id}")
        capability = self.repository.find_one("capabilities", {"capability_key": proposal["capability_key"], "status": "approved"})
        if not capability:
            raise KeyError(f"approved capability not found: {proposal['capability_key']}")
        self._assert_proposal_digest_current(proposal, capability)
        if data.actor_role in {"secondary_worker", "verifier_critic"} and self._side_effect_requires_approval(capability["side_effect_level"], capability["risk_level"]):
            return self._record_action_failure(proposal, capability, "multi_model_boundary_blocked", "secondary/verifier cannot execute high-risk MCP directly.", retryable=False)
        if data.dry_run:
            return {"status": "dry_run", "executed": False, "proposal": proposal, "human_cards": [{"title": proposal["title"], "summary": proposal["summary"], "next_step": "dry-run does not call real MCP."}]}
        if capability["side_effect_level"] == "production_sensitive" or capability["risk_level"] == "production_sensitive":
            return self._record_action_failure(proposal, capability, "production_boundary_blocked", "Phase 1 blocks automatic production_sensitive execution; explicit user authorization is required.", retryable=False)
        if proposal.get("status") == "approval_required":
            return self._record_action_failure(proposal, capability, "approval_missing", "Action Proposal is missing a valid approval bound to the plan digest.", retryable=False)
        if proposal.get("status") not in {"preflight_passed", "approved"}:
            return self._record_action_failure(proposal, capability, "preflight_or_approval_missing", "Execution requires passed preflight and approval when required.", retryable=False)
        requires_approval = self._side_effect_requires_approval(capability["side_effect_level"], capability["risk_level"])
        approval = self.repository.get_record("approvals", str(proposal.get("approval_id"))) if proposal.get("approval_id") else None
        if requires_approval and (not approval or approval.get("status") != "approved" or approval.get("plan_digest") != proposal.get("plan_digest")):
            return self._record_action_failure(proposal, capability, "approval_missing", "High-risk Action Proposal is missing a valid approval bound to the plan digest.", retryable=False)
        tool = self._resolve_capability_tool(capability)
        if not tool:
            return self._execute_skill_or_workflow_only(proposal, capability)

        execution_policy = self._execution_policy(capability)
        timeout_seconds = int(execution_policy["timeout_seconds"])
        payload = data.payload_json or dict(proposal.get("input_json") or {})
        self.repository.update_record("action_proposals", action_proposal_id, {"status": "executing"})
        self.add_task_event(
            proposal["task_id"],
            TaskEventCreate(
                event_type="mcp_started",
                message=f"Start Action Proposal execution: {proposal['title']}",
                payload_json={"action_proposal_id": action_proposal_id, "capability_key": capability["capability_key"]},
            ),
        )

        max_attempts = max(1, int(execution_policy["max_retries"]) + 1)
        last_error: dict[str, Any] | None = None
        for attempt_index in range(max_attempts):
            start = perf_counter()
            try:
                adapter_result = self._execute_loopback_tool(tool, payload)
                duration_ms = int((perf_counter() - start) * 1000)
            except TimeoutError as exc:
                duration_ms = int((perf_counter() - start) * 1000)
                adapter_result = {
                    "status": "failed",
                    "result_json": {},
                    "result_cards": [{"title": "Execution timeout", "summary": str(exc)}],
                    "artifact_refs": [],
                    "error_json": {"code": "timeout", "human_reason": str(exc), "retryable": True},
                    "retry_count": attempt_index,
                }
            except Exception as exc:
                duration_ms = int((perf_counter() - start) * 1000)
                adapter_result = {
                    "status": "failed",
                    "result_json": {},
                    "result_cards": [{"title": "Execution failed", "summary": str(exc)}],
                    "artifact_refs": [],
                    "error_json": {"code": "execution_failed", "human_reason": str(exc), "retryable": False},
                    "retry_count": attempt_index,
                }

            event_status = "succeeded" if str(adapter_result.get("status") or "succeeded") == "succeeded" else "failed"
            result_cards = adapter_result.get("result_cards") or []
            adapter_error = dict(adapter_result.get("error_json") or {})
            if event_status == "failed":
                adapter_error = self._normalize_adapter_error(adapter_result, result_cards, action_proposal_id)
                code = str(adapter_error["code"])
                adapter_error["retry_policy"] = {
                    "attempt_index": attempt_index,
                    "max_retries": execution_policy["max_retries"],
                    "retryable_error_codes": sorted(execution_policy["retryable_error_codes"]),
                    "non_retryable_error_codes": sorted(execution_policy["non_retryable_error_codes"]),
                }
                adapter_error["retryable"] = bool(adapter_error["retryable"] or code in execution_policy["retryable_error_codes"])
                if code in execution_policy["non_retryable_error_codes"]:
                    adapter_error["retryable"] = False

            event = self.repository.create_record(
                "mcp_tool_events",
                {
                    "tool_event_id": new_id("mcptev"),
                    "task_id": proposal["task_id"],
                    "server_key": tool["server_key"],
                    "tool_name": tool["tool_name"],
                    "event_type": "execute",
                    "status": event_status,
                    "idempotency_key": data.idempotency_key or proposal["idempotency_key"],
                    "request_json": payload,
                    "response_json": adapter_result.get("result_json") or {},
                    "error_json": adapter_error,
                    "action_proposal_id": action_proposal_id,
                    "approval_id": proposal.get("approval_id"),
                    "plan_digest": proposal.get("plan_digest"),
                    "transport": "loopback_http",
                    "timeout_ms": timeout_seconds * 1000,
                    "attempt_index": attempt_index,
                    "duration_ms": duration_ms,
                    "result_card_json": result_cards[0] if result_cards else {},
                    "artifact_refs": adapter_result.get("artifact_refs") or [],
                    "started_at": utc_now().isoformat(),
                    "completed_at": utc_now().isoformat(),
                },
            )
            final_status = "succeeded" if event_status == "succeeded" else "failed"
            trace_payload = {"action_proposal_id": action_proposal_id, "tool_event_id": event["tool_event_id"], "human_cards": result_cards}
            if adapter_error:
                trace_payload["error"] = adapter_error
            trace = self.create_trace_event(
                TraceEventCreate(
                    task_id=proposal["task_id"],
                    event_type="action_execute",
                    component="research_assistant.execution_gateway",
                    status=final_status,
                    duration_ms=duration_ms,
                    payload_json=trace_payload,
                )
            )
            should_retry = bool(adapter_error.get("retryable")) and attempt_index + 1 < max_attempts
            if should_retry:
                last_error = adapter_error
                self.add_task_event(
                    proposal["task_id"],
                    TaskEventCreate(
                        event_type="mcp_retry",
                        severity="warning",
                        message=f"Action Proposal execution failed; retrying by runtime config: {proposal['title']}",
                        payload_json={"action_proposal_id": action_proposal_id, "tool_event_id": event["tool_event_id"], "trace_id": trace["trace_id"], "error": adapter_error},
                    ),
                )
                continue

            updated = self.repository.update_record("action_proposals", action_proposal_id, {"status": final_status})
            task_event_type = "mcp_done" if final_status == "succeeded" else "mcp_execution_timeout" if adapter_error.get("code") == "timeout" else "mcp_failed"
            self.add_task_event(
                proposal["task_id"],
                TaskEventCreate(
                    event_type=task_event_type,
                    severity="info" if final_status == "succeeded" else "error",
                    message=f"Action Proposal execution {'succeeded' if final_status == 'succeeded' else 'failed'}: {proposal['title']}",
                    payload_json={"action_proposal_id": action_proposal_id, "tool_event_id": event["tool_event_id"], "trace_id": trace["trace_id"]},
                ),
            )
            response = {"status": final_status, "executed": final_status == "succeeded", "proposal": updated, "tool_event": event, "trace_id": trace["trace_id"], "human_cards": result_cards}
            if adapter_error:
                response["error"] = adapter_error
            return response

        return self._record_action_failure(
            proposal,
            capability,
            str((last_error or {}).get("code") or "execution_failed"),
            str((last_error or {}).get("human_reason") or "MCP execution failed after configured retries."),
            retryable=False,
        )

    def _execute_loopback_tool(self, tool: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        name = str(tool.get("tool_name"))
        if name == "assistant_create_task":
            task = self.create_task(TaskCreate(**payload))
            return {"status": "succeeded", "result_json": task, "result_cards": [{"title": "任务已创建", "summary": task["title"], "task_id": task["task_id"]}], "artifact_refs": [task["task_id"]], "error_json": {}, "retry_count": 0}
        if name == "assistant_build_context_pack":
            pack = self.build_context_pack(ContextPackBuildRequest(**payload))
            return {"status": "succeeded", "result_json": pack, "result_cards": [{"title": "Context Pack 已构建", "summary": pack["pack_summary"], "context_pack_id": pack["context_pack_id"]}], "artifact_refs": [pack["context_pack_id"]], "error_json": {}, "retry_count": 0}
        if name == "assistant_create_memory_candidate":
            memory = self.create_memory(MemoryCreate(**payload))
            return {"status": "succeeded", "result_json": memory, "result_cards": [{"title": "候选记忆已创建", "summary": memory["title"], "memory_id": memory["memory_id"]}], "artifact_refs": [memory["memory_id"]], "error_json": {}, "retry_count": 0}
        if name == "assistant_create_issue_candidate":
            issue = self.create_issue_candidate(IssueCandidateCreate(**payload))
            return {"status": "succeeded", "result_json": issue, "result_cards": [{"title": "候选 Issue 已创建", "summary": issue["title"], "candidate_id": issue["candidate_id"], "next_step": "GitHub 正式同步仍需单独审批。"}], "artifact_refs": [issue["candidate_id"]], "error_json": {}, "retry_count": 0}
        if name == "qe_template_create":
            template = self._qe_template_create_draft(payload)
            return {"status": "succeeded", "result_json": template, "result_cards": [{"title": "QE template 草案已生成", "summary": template["title"], "template_id": template["template_id"], "next_step": "确认后进入 validate；尚未 materialize/run。"}], "artifact_refs": [template["template_id"]], "error_json": {}, "retry_count": 0}
        if name == "qe_template_validate":
            validation = self._qe_template_validate(payload)
            return {"status": "succeeded" if validation["validation"]["valid"] else "failed", "result_json": validation, "result_cards": [{"title": "QE template 校验结果", "summary": validation["human_summary"], "template_id": validation["template"]["template_id"]}], "artifact_refs": [validation["template"]["template_id"]], "error_json": {} if validation["validation"]["valid"] else {"errors": validation["validation"]["errors"]}, "retry_count": 0}
        if name in {"qe_template_materialize_confirmed", "qe_template_run_confirmed"}:
            return {"status": "failed", "result_json": {}, "result_cards": [{"title": "执行被阻断", "summary": "当前本地适配器不会在测试/开发态直接 materialize 或 run QE。"}], "artifact_refs": [], "error_json": {"code": "adapter_not_enabled_for_high_cost_qe"}, "retry_count": 0}
        raise ValueError(f"loopback adapter is not implemented for tool: {name}")

    def _qe_template_create_draft(self, payload: dict[str, Any]) -> dict[str, Any]:
        template_id = new_id("qet")
        config_json = dict(payload.get("config_json") or {})
        validation = self._validate_qe_template_payload(str(payload.get("template_kind") or "custom_evo"), config_json)
        return {
            "template_id": template_id,
            "template_kind": payload.get("template_kind") or "custom_evo",
            "title": payload.get("title") or "QE experiment draft",
            "status": "draft",
            "config_json": config_json,
            "validation": validation,
            "materialized": False,
            "run_requested": False,
            "human_summary": "已生成 QE template 草案；未物化、未运行。",
        }

    @staticmethod
    def _validate_qe_template_payload(template_kind: str, config_json: dict[str, Any]) -> dict[str, Any]:
        errors: list[str] = []
        warnings: list[str] = []
        if template_kind == "custom_evo":
            loops = config_json.get("loops")
            if not isinstance(loops, list) or not loops:
                errors.append("custom_evo config requires non-empty loops")
            if not (config_json.get("stock_pool") or config_json.get("stock_pool_id")):
                warnings.append("stock_pool should be confirmed before materialize")
            if not config_json.get("backtest_window"):
                warnings.append("backtest_window should be confirmed before materialize")
        elif template_kind == "single_experiment":
            for key in ("factor_names", "model_id"):
                if not config_json.get(key):
                    errors.append(f"single_experiment config requires {key}")
        else:
            errors.append(f"unsupported template_kind: {template_kind}")
        return {"valid": not errors, "errors": errors, "warnings": warnings}

    def _qe_template_validate(self, payload: dict[str, Any]) -> dict[str, Any]:
        template_id = str(payload.get("template_id") or "")
        if not template_id:
            template_id = new_id("qet")
        template = {
            "template_id": template_id,
            "template_kind": payload.get("template_kind") or "custom_evo",
            "title": payload.get("title") or f"QE template {template_id}",
            "status": "ready_for_review",
            "config_json": dict(payload.get("config_json") or {"loops": [{"factor_keys": ["pending"], "model_id": "pending"}]}),
        }
        validation = self._validate_qe_template_payload(str(template["template_kind"]), dict(template["config_json"]))
        template["status"] = "ready_for_review" if validation["valid"] else "draft"
        summary = "校验通过，可展示 diff/summary 并等待 materialize 二次确认。" if validation["valid"] else "校验失败：" + "; ".join(validation["errors"])
        return {"template": template, "validation": validation, "diff_summary": {"changed_fields": [], "materialize": False, "run": False}, "human_summary": summary}

    def _execute_skill_or_workflow_only(self, proposal: dict[str, Any], capability: dict[str, Any]) -> dict[str, Any]:
        card = {"title": proposal["title"], "summary": proposal["summary"], "capability_key": capability["capability_key"], "next_step": "该能力为只读 skill/workflow trace，未调用高风险 MCP。"}
        self.repository.update_record("action_proposals", proposal["action_proposal_id"], {"status": "succeeded"})
        event = self.create_trace_event(TraceEventCreate(task_id=proposal["task_id"], event_type="action_execute", component="research_assistant.execution_gateway", status="succeeded", payload_json={"action_proposal_id": proposal["action_proposal_id"], "human_cards": [card]}))
        self.add_task_event(proposal["task_id"], TaskEventCreate(event_type="skill_done", message=f"Action Proposal 已完成：{proposal['title']}", payload_json={"action_proposal_id": proposal["action_proposal_id"], "trace_id": event["trace_id"]}))
        return {"status": "succeeded", "executed": True, "proposal": self.repository.get_record("action_proposals", proposal["action_proposal_id"]), "trace_id": event["trace_id"], "human_cards": [card]}

    def _record_action_failure(self, proposal: dict[str, Any], capability: dict[str, Any], code: str, message: str, *, retryable: bool, event_type: str = "mcp_failed") -> dict[str, Any]:
        updated = self.repository.update_record("action_proposals", proposal["action_proposal_id"], {"status": "failed"})
        error_json = {"code": code, "human_reason": message, "next_step": "修正输入、重新 preflight 或重新创建 Action Proposal。", "audit_link": f"/research-assistant/actions/{proposal['action_proposal_id']}", "retryable": retryable}
        tool = self._resolve_capability_tool(capability)
        if tool:
            self.repository.create_record(
                "mcp_tool_events",
                {
                    "tool_event_id": new_id("mcptev"),
                    "task_id": proposal["task_id"],
                    "server_key": tool["server_key"],
                    "tool_name": tool["tool_name"],
                    "event_type": "execute",
                    "status": "failed",
                    "idempotency_key": proposal["idempotency_key"],
                    "request_json": proposal.get("input_json") or {},
                    "response_json": {},
                    "error_json": error_json,
                    "action_proposal_id": proposal["action_proposal_id"],
                    "approval_id": proposal.get("approval_id"),
                    "plan_digest": proposal.get("plan_digest"),
                    "transport": "loopback_http",
                    "result_card_json": {"title": "执行失败", "summary": message, "next_step": error_json["next_step"]},
                    "artifact_refs": [],
                    "completed_at": utc_now().isoformat(),
                },
            )
        trace = self.create_trace_event(TraceEventCreate(task_id=proposal["task_id"], event_type="action_execute", component="research_assistant.execution_gateway", status="failed", payload_json={"action_proposal_id": proposal["action_proposal_id"], "error": error_json}))
        self.add_task_event(
            proposal["task_id"],
            TaskEventCreate(event_type=event_type if event_type in {"mcp_failed", "mcp_execution_timeout"} else "mcp_failed", severity="error", message=message, payload_json={"action_proposal_id": proposal["action_proposal_id"], "trace_id": trace["trace_id"], "error": error_json}),
        )
        return {"status": "failed", "executed": False, "proposal": updated, "error": error_json, "trace_id": trace["trace_id"]}
