"""Execution-closure helpers for the Research Assistant service.

This mixin keeps MCP/Skill execution gates explicit while the service remains
the owner of repositories, task events, trace events, and runtime config.
"""

from __future__ import annotations

from datetime import timedelta
from time import perf_counter
from typing import Any

from backend.services.research_assistant.mcp_catalog_sync import canonicalize_server_key

from backend.services.mcp_payload_budget import artifact_ref, assert_summary_payload, summary_envelope

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

    @staticmethod
    def _capability_tool_refs(capability: dict[str, Any]) -> list[dict[str, str]]:
        refs = list(capability.get("mcp_tool_refs") or [])
        return [
            {"server_key": str(ref.get("server_key") or ""), "tool_name": str(ref.get("tool_name") or "")}
            for ref in refs
            if isinstance(ref, dict) and ref.get("server_key") and ref.get("tool_name")
        ]

    @staticmethod
    def _route_candidates_from_payload(payload: dict[str, Any] | None) -> list[dict[str, str]]:
        data = dict(payload or {})
        candidates: list[dict[str, str]] = []

        def add_candidate(item: dict[str, Any] | None) -> None:
            if not isinstance(item, dict):
                return
            server_key = str(item.get("server_key") or item.get("server") or "").strip()
            if server_key:
                server_key = canonicalize_server_key(server_key)
            tool_name = str(item.get("tool_name") or item.get("tool") or "").strip()
            if server_key or tool_name:
                candidates.append({"server_key": server_key, "tool_name": tool_name})

        if data.get("tool_name") or data.get("tool"):
            add_candidate(data)
        for key in ("route", "mcp_route_decision", "selected_tool", "tool_route", "mcp_tool"):
            value = data.get(key)
            add_candidate(value if isinstance(value, dict) else None)
        return candidates

    def _resolve_capability_tool(
        self,
        capability: dict[str, Any],
        proposal: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        refs = self._capability_tool_refs(capability)
        if not refs:
            return None
        route_payload = payload
        if route_payload is None and proposal is not None:
            route_payload = dict(proposal.get("input_json") or {})
        selected_ref: dict[str, str] | None = None
        for candidate in self._route_candidates_from_payload(route_payload):
            matches = [
                ref
                for ref in refs
                if (not candidate["server_key"] or ref["server_key"] == candidate["server_key"])
                and (not candidate["tool_name"] or ref["tool_name"] == candidate["tool_name"])
            ]
            if matches:
                selected_ref = matches[0]
                break
            if candidate["server_key"] or candidate["tool_name"]:
                requested = f"{candidate['server_key']}/{candidate['tool_name']}".strip("/")
                allowed = ", ".join(f"{ref['server_key']}/{ref['tool_name']}" for ref in refs[:20])
                raise ValueError(f"selected MCP tool is not allowed by capability: {requested}; allowed={allowed}")
        ref = selected_ref or refs[0]
        return self.repository.find_one("mcp_tools", {"server_key": ref["server_key"], "tool_name": ref["tool_name"]})

    def _effective_action_profile(self, capability: dict[str, Any], tool: dict[str, Any] | None = None) -> dict[str, Any]:
        use_tool_profile = bool(tool) and str(capability.get("capability_key") or "").endswith(".mcp_orchestration")
        profile = tool if use_tool_profile else capability
        risk_level = str((profile or {}).get("risk_level") or "medium")
        side_effect_level = str((profile or {}).get("side_effect_level") or "read_only")
        required_confirmations = [str(item) for item in ((profile or {}).get("required_confirmations") or [])]
        if risk_level == "low" and side_effect_level == "read_only":
            required_confirmations = []
        requires_approval = bool((profile or {}).get("requires_approval")) or self._side_effect_requires_approval(side_effect_level, risk_level)
        return {
            "risk_level": risk_level,
            "side_effect_level": side_effect_level,
            "required_confirmations": required_confirmations,
            "requires_approval": requires_approval,
        }

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
        selected_tool = self._resolve_capability_tool(capability, {"input_json": input_json})
        effective_profile = self._effective_action_profile(capability, selected_tool)
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
                "risk_level": effective_profile["risk_level"],
                "side_effect_level": effective_profile["side_effect_level"],
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
                severity="warning" if effective_profile["requires_approval"] or effective_profile["required_confirmations"] else "info",
                message=f"已生成 Action Proposal：{data.title}；确认前不会执行。",
                payload_json={
                    "action_proposal_id": proposal["action_proposal_id"],
                    "capability_key": data.capability_key,
                    "plan_digest": plan_digest,
                    "server_key": selected_tool.get("server_key") if selected_tool else None,
                    "tool_name": selected_tool.get("tool_name") if selected_tool else None,
                },
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
        tool = self._resolve_capability_tool(capability, proposal)
        required = self._effective_action_profile(capability, tool)["required_confirmations"]
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
        payload = data.payload_json or dict(proposal.get("input_json") or {})
        tool = self._resolve_capability_tool(capability, proposal, payload)
        effective_profile = self._effective_action_profile(capability, tool)
        allowed_statuses = {"confirmed", "approval_required", "approved", "preflight_failed"}
        if not effective_profile["requires_approval"] and not effective_profile["required_confirmations"]:
            allowed_statuses.add("proposed")
        if proposal.get("status") not in allowed_statuses:
            raise ValueError(f"proposal must be confirmed before preflight; status={proposal.get('status')}")
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
            if effective_profile["requires_approval"] and not result.get("approval_required"):
                result = dict(result)
                result["requires_approval"] = True
                result["approval_required"] = True
                result["passed"] = False
                result["capability_policy_requires_approval"] = True
                result["missing_confirmations"] = list(effective_profile["required_confirmations"])
                checks = list(result.get("preflight_checks") or [])
                if "capability_policy" not in checks:
                    checks.append("capability_policy")
                result["preflight_checks"] = checks
            self.repository.update_record(
                "mcp_tool_events",
                result["tool_event_id"],
                {
                    "action_proposal_id": action_proposal_id,
                    "plan_digest": proposal["plan_digest"],
                    "transport": "research_assistant_preflight",
                    "response_json": result,
                },
            )
        else:
            result = {
                "passed": True,
                "approval_required": effective_profile["requires_approval"],
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
        tool = self._resolve_capability_tool(capability, proposal)
        effective_profile = self._effective_action_profile(capability, tool)
        required_texts = effective_profile["required_confirmations"] or [ASSISTANT_APPROVAL_CONFIRM_TEXT]
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
        payload = data.payload_json or dict(proposal.get("input_json") or {})
        tool = self._resolve_capability_tool(capability, proposal, payload)
        effective_profile = self._effective_action_profile(capability, tool)
        if data.actor_role in {"secondary_worker", "verifier_critic"} and effective_profile["requires_approval"]:
            return self._record_action_failure(proposal, capability, "multi_model_boundary_blocked", "secondary/verifier cannot execute high-risk MCP directly.", retryable=False)
        if data.dry_run:
            return {"status": "dry_run", "executed": False, "proposal": proposal, "human_cards": [{"title": proposal["title"], "summary": proposal["summary"], "next_step": "dry-run does not call real MCP."}]}
        if effective_profile["side_effect_level"] == "production_sensitive" or effective_profile["risk_level"] == "production_sensitive":
            return self._record_action_failure(proposal, capability, "production_boundary_blocked", "Phase 1 blocks automatic production_sensitive execution; explicit user authorization is required.", retryable=False)
        if proposal.get("status") == "approval_required":
            return self._record_action_failure(proposal, capability, "approval_missing", "Action Proposal is missing a valid approval bound to the plan digest.", retryable=False)
        if proposal.get("status") not in {"preflight_passed", "approved"}:
            return self._record_action_failure(proposal, capability, "preflight_or_approval_missing", "Execution requires passed preflight and approval when required.", retryable=False)
        approval = self.repository.get_record("approvals", str(proposal.get("approval_id"))) if proposal.get("approval_id") else None
        if effective_profile["requires_approval"] and (not approval or approval.get("status") != "approved" or approval.get("plan_digest") != proposal.get("plan_digest")):
            return self._record_action_failure(proposal, capability, "approval_missing", "High-risk Action Proposal is missing a valid approval bound to the plan digest.", retryable=False)
        if not tool:
            return self._execute_skill_or_workflow_only(proposal, capability)

        execution_policy = self._execution_policy({**capability, **effective_profile})
        timeout_seconds = int(execution_policy["timeout_seconds"])
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
                    "transport": str(adapter_result.get("transport") or "loopback_http"),
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
        if self._is_summary_first_read_tool(tool):
            return self._execute_summary_first_read_tool(tool, payload)
        raise ValueError(f"loopback adapter is not implemented for tool: {name}")

    @staticmethod
    def _is_summary_first_read_tool(tool: dict[str, Any]) -> bool:
        return str(tool.get("risk_level") or "") == "low" and str(tool.get("side_effect_level") or "") == "read_only"

    @staticmethod
    def _summary_adapter_args(payload: dict[str, Any]) -> dict[str, Any]:
        args: dict[str, Any] = {}
        for key in ("args", "tool_args", "parameters", "params"):
            value = payload.get(key)
            if isinstance(value, dict):
                args.update(value)
        for key in (
            "server_key",
            "risk_level",
            "search",
            "q",
            "limit",
            "offset",
            "factor_name",
            "model_id",
            "package_id",
            "algo_code",
            "method",
            "min_abs_corr",
            "qe_selectable",
            "query",
            "locale",
            "provider",
            "url",
            "max_chars",
        ):
            if key in payload and key not in args:
                args[key] = payload[key]
        return args

    def _execute_summary_first_read_tool(self, tool: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        server_key = str(tool.get("server_key") or "")
        tool_name = str(tool.get("tool_name") or "")
        args = self._summary_adapter_args(payload)
        if server_key == "aistock-local-data" and tool_name == "local_data_get_preset_daily_status":
            return self._execute_local_data_daily_status_read(tool, payload, args)
        server = self.repository.find_one("mcp_servers", {"server_key": server_key}) or {}
        health = server.get("health_json") if isinstance(server.get("health_json"), dict) else {}
        domain = str(health.get("domain") or server_key)
        limit = int(args.get("limit") or 20)
        offset = int(args.get("offset") or 0)
        items, total = self._summary_adapter_items(tool, args, limit=limit, offset=offset)
        artifact_refs = self._summary_adapter_artifact_refs(server_key, tool_name, args)
        result_json = summary_envelope(
            domain=domain,
            items=items,
            total=total,
            limit=limit,
            offset=offset,
            omitted_sections=[
                "raw_payload",
                "full_logs",
                "matrix",
                "model_weights",
                "training_curves",
                "factor_value_rows",
                "database_rows",
            ],
            detail_tool=self._summary_adapter_detail_tool(server_key, tool_name),
            detail_args_hint=self._summary_adapter_detail_args(tool_name, args),
            artifact_refs=artifact_refs,
            extra={
                "server_key": server_key,
                "tool_name": tool_name,
                "summary_first": True,
                "response_mode": "summary",
                "source": "research_assistant_catalog_summary_adapter",
                "live_backend_called": False,
                "next_step": "Use the referenced detail tool or execute the backend MCP facade when live data is required.",
                **(
                    {
                        "evidence_policy": {
                            "external_evidence_only": True,
                            "not_final_conclusion": True,
                            "candidate_branches": ["external.", "personal.topic."],
                            "l4_handoff": "hypothesis_then_low_cost_validation_only",
                        }
                    }
                    if server_key == "aistock-external-research"
                    else {}
                ),
            },
        )
        assert_summary_payload(result_json)
        card = {
            "title": f"{server_key}/{tool_name}",
            "summary": f"Prepared a summary-first MCP result envelope for {domain}; heavy sections are omitted or referenced.",
            "route": f"{server_key}/{tool_name}",
            "summary_first": True,
            "next_step": result_json["next_step"],
        }
        return {
            "status": "succeeded",
            "result_json": result_json,
            "result_cards": [card],
            "artifact_refs": artifact_refs,
            "error_json": {},
            "retry_count": 0,
            "transport": "research_assistant_catalog_summary_adapter",
        }

    def _local_data_facade_service(self) -> Any:
        factory = getattr(self, "local_data_service_factory", None)
        if callable(factory):
            return factory()
        from backend.services.local_data_management import LocalDataManagementService

        return LocalDataManagementService()

    def _execute_local_data_daily_status_read(
        self,
        tool: dict[str, Any],
        payload: dict[str, Any],
        args: dict[str, Any],
    ) -> dict[str, Any]:
        server_key = str(tool.get("server_key") or "aistock-local-data")
        tool_name = str(tool.get("tool_name") or "local_data_get_preset_daily_status")
        service = self._local_data_facade_service()
        try:
            daily = service.get_preset_daily_status()
        except Exception as exc:  # noqa: BLE001
            return {
                "status": "failed",
                "result_json": {},
                "result_cards": [{"title": f"{server_key}/{tool_name}", "summary": f"local-data read failed: {exc}"}],
                "artifact_refs": [],
                "error_json": {"code": "local_data_daily_status_read_failed", "human_reason": str(exc), "retryable": True},
                "retry_count": 0,
                "transport": "local_data_facade_read_adapter",
            }

        partial_errors: list[dict[str, str]] = []

        def optional_call(name: str, func: Any) -> dict[str, Any]:
            try:
                result = func()
                return result if isinstance(result, dict) else {}
            except Exception as exc:  # noqa: BLE001
                partial_errors.append({"source": name, "error": str(exc)})
                return {}

        preset_stats = optional_call("local_data_get_preset_stats", service.get_preset_stats)
        jobs = optional_call("local_data_list_jobs", lambda: service.list_jobs(limit=50, active_only=False))
        targets = optional_call("local_data_list_sync_targets", lambda: service.list_sync_targets(limit=100))
        status_report = self._local_data_daily_status_report(
            daily=daily,
            preset_stats=preset_stats,
            jobs=jobs,
            targets=targets,
            partial_errors=partial_errors,
            trade_date=str(args.get("trade_date") or payload.get("trade_date") or "").strip() or None,
        )
        items = status_report["items"]
        artifact_refs = [
            artifact_ref(
                "local_data_daily_sync_status",
                "research_assistant:aistock-local-data:local_data_get_preset_daily_status",
                {
                    "source": "local_data_facade_read_adapter",
                    "trade_date": status_report["trade_date"],
                    "group_counts": status_report["group_counts"],
                },
            )
        ]
        result_json = summary_envelope(
            domain="local_data",
            items=items,
            total=len(items),
            limit=int(args.get("limit") or max(20, len(items))),
            offset=int(args.get("offset") or 0),
            omitted_sections=["raw_payload", "full_logs", "database_rows"],
            detail_tool=f"{server_key}/{tool_name}",
            detail_args_hint={"trade_date": status_report["trade_date"]},
            artifact_refs=artifact_refs,
            extra={
                "server_key": server_key,
                "tool_name": tool_name,
                "summary_first": True,
                "response_mode": "local_data_daily_sync_status",
                "source": "local_data_facade_read_adapter",
                "live_backend_called": True,
                "local_data_daily_status": True,
                "trade_date": status_report["trade_date"],
                "as_of": status_report["as_of"],
                "group_counts": status_report["group_counts"],
                "status_groups": status_report["status_groups"],
                "evidence_sources": status_report["evidence_sources"],
                "partial_errors": partial_errors,
                "next_step": "Review failed, blocked, or not-run groups; use repair planning only after explicit operator request.",
            },
        )
        assert_summary_payload(result_json)
        return {
            "status": "succeeded",
            "result_json": result_json,
            "result_cards": [
                {
                    "title": f"{server_key}/{tool_name}",
                    "summary": "Prepared today's local-data sync status groups from local-data read-only facade evidence.",
                    "route": f"{server_key}/{tool_name}",
                    "summary_first": True,
                    "group_counts": status_report["group_counts"],
                    "next_step": result_json["next_step"],
                }
            ],
            "artifact_refs": artifact_refs,
            "error_json": {},
            "retry_count": 0,
            "transport": "local_data_facade_read_adapter",
        }

    @staticmethod
    def _unwrap_local_data_items(response: dict[str, Any]) -> Any:
        data = response.get("data") if isinstance(response, dict) else None
        if isinstance(data, dict) and "items" in data:
            return data.get("items")
        if isinstance(response, dict):
            return response.get("items")
        return None

    @staticmethod
    def _local_data_dataset_from_job(job: dict[str, Any]) -> str:
        for container_key in ("meta", "summary", "payload_json", "request_json"):
            container = job.get(container_key)
            if isinstance(container, dict):
                for key in ("dataset", "data_kind"):
                    value = container.get(key)
                    if value:
                        return str(value)
        for key in ("dataset", "data_kind"):
            value = job.get(key)
            if value:
                return str(value)
        return "unknown"

    @staticmethod
    def _local_data_status_group(status: Any) -> str:
        normalized = str(status or "").strip().lower()
        if normalized in {"success", "succeeded", "completed", "done", "ok"}:
            return "success"
        if normalized in {"failed", "error", "fail"}:
            return "failed"
        if normalized in {"running", "queued", "pending", "in_progress", "started"}:
            return "running"
        return "not_synced"

    @classmethod
    def _local_data_daily_status_report(
        cls,
        *,
        daily: dict[str, Any],
        preset_stats: dict[str, Any],
        jobs: dict[str, Any],
        targets: dict[str, Any],
        partial_errors: list[dict[str, str]],
        trade_date: str | None,
    ) -> dict[str, Any]:
        daily_items = cls._unwrap_local_data_items(daily)
        daily_by_dataset = daily_items if isinstance(daily_items, dict) else {}
        stats_items = cls._unwrap_local_data_items(preset_stats)
        stats_list = stats_items if isinstance(stats_items, list) else []
        job_items = cls._unwrap_local_data_items(jobs)
        jobs_list = job_items if isinstance(job_items, list) else []
        target_items = cls._unwrap_local_data_items(targets)
        targets_list = target_items if isinstance(target_items, list) else []
        active_job_statuses = {"running", "queued", "pending", "in_progress", "started"}
        active_job_by_dataset: dict[str, dict[str, Any]] = {}
        for job in jobs_list:
            if not isinstance(job, dict):
                continue
            status = str(job.get("status") or "").lower()
            if status not in active_job_statuses:
                continue
            dataset = cls._local_data_dataset_from_job(job)
            if dataset != "unknown" and dataset not in active_job_by_dataset:
                active_job_by_dataset[dataset] = job

        expected_datasets: list[str] = []
        for item in stats_list:
            if not isinstance(item, dict):
                continue
            dataset = item.get("dataset") or item.get("data_kind") or item.get("name")
            if dataset and str(dataset) not in expected_datasets:
                expected_datasets.append(str(dataset))
        for dataset in daily_by_dataset:
            if str(dataset) not in expected_datasets:
                expected_datasets.append(str(dataset))
        for job in jobs_list:
            if not isinstance(job, dict):
                continue
            status = str(job.get("status") or "").lower()
            if status not in active_job_statuses:
                continue
            dataset = cls._local_data_dataset_from_job(job)
            if dataset != "unknown" and dataset not in expected_datasets:
                expected_datasets.append(dataset)
        for target in targets_list:
            if not isinstance(target, dict):
                continue
            dataset = target.get("dataset")
            if dataset and str(dataset) not in expected_datasets:
                expected_datasets.append(str(dataset))

        target_by_dataset: dict[str, list[dict[str, Any]]] = {}
        for target in targets_list:
            if isinstance(target, dict) and target.get("dataset"):
                target_by_dataset.setdefault(str(target["dataset"]), []).append(target)

        groups: dict[str, list[dict[str, Any]]] = {
            "success": [],
            "failed": [],
            "not_synced": [],
            "running": [],
            "blocked": [],
        }
        items: list[dict[str, Any]] = []
        for dataset in sorted(expected_datasets):
            status_info = daily_by_dataset.get(dataset)
            if isinstance(status_info, dict):
                raw_status = status_info.get("status")
                created_at = status_info.get("created_at")
                finished_at = status_info.get("finished_at")
            else:
                raw_status = status_info
                created_at = None
                finished_at = None
            group = cls._local_data_status_group(raw_status)
            active_job = active_job_by_dataset.get(dataset)
            if active_job and group == "not_synced":
                raw_status = active_job.get("status") or raw_status
                created_at = created_at or active_job.get("created_at")
                finished_at = finished_at or active_job.get("finished_at")
                group = cls._local_data_status_group(raw_status)
            related_targets = target_by_dataset.get(dataset, [])
            blocked_targets = [target for target in related_targets if str(target.get("target_status") or "") == "final_blocked"]
            retry_targets = [target for target in related_targets if str(target.get("target_status") or "") == "retry"]
            if blocked_targets:
                group = "blocked"
            elif retry_targets and group == "not_synced":
                group = "failed"
            item = {
                "dataset": dataset,
                "status": str(raw_status or "not_run"),
                "status_group": group,
                "created_at": created_at,
                "finished_at": finished_at,
                "job_id": active_job.get("job_id") if active_job else None,
                "target_statuses": sorted({str(target.get("target_status") or "unknown") for target in related_targets})[:5],
                "last_error": next((str(target.get("last_error_message")) for target in [*blocked_targets, *retry_targets] if target.get("last_error_message")), None),
            }
            groups[group].append(item)
            items.append(item)

        seen_running = {str(item.get("dataset")) for item in groups["running"]}
        for job in jobs_list:
            if not isinstance(job, dict):
                continue
            status = str(job.get("status") or "").lower()
            if status not in active_job_statuses:
                continue
            dataset = cls._local_data_dataset_from_job(job)
            if dataset in seen_running:
                continue
            item = {
                "dataset": dataset,
                "status": status,
                "status_group": "running",
                "job_id": job.get("job_id"),
                "created_at": job.get("created_at"),
                "finished_at": job.get("finished_at"),
                "target_statuses": [],
                "last_error": None,
            }
            groups["running"].append(item)
            items.append(item)
            seen_running.add(dataset)

        trace = daily.get("trace") if isinstance(daily.get("trace"), dict) else {}
        evidence_sources = [
            "local_data_get_preset_daily_status",
            "local_data_get_preset_stats",
            "local_data_list_jobs",
            "local_data_list_sync_targets",
        ]
        if partial_errors:
            evidence_sources.append("partial_read_errors")
        return {
            "trade_date": trade_date or utc_now().date().isoformat(),
            "as_of": str(trace.get("generated_at") or utc_now().isoformat()),
            "items": items,
            "status_groups": groups,
            "group_counts": {key: len(value) for key, value in groups.items()},
            "evidence_sources": evidence_sources,
        }

    def _summary_adapter_items(self, tool: dict[str, Any], args: dict[str, Any], *, limit: int, offset: int) -> tuple[list[dict[str, Any]], int]:
        server_key = str(tool.get("server_key") or "")
        tool_name = str(tool.get("tool_name") or "")
        if tool_name == "assistant_list_mcp_tools":
            search = str(args.get("search") or args.get("q") or "").strip().lower()
            page = self.list_mcp_tools(
                server_key=str(args["server_key"]) if args.get("server_key") else None,
                risk_level=str(args["risk_level"]) if args.get("risk_level") else None,
                search=search or None,
                limit=limit,
                offset=offset,
            )
            window = list(page["items"])
            return [
                {
                    "server_key": item.get("server_key"),
                    "tool_name": item.get("tool_name"),
                    "title": item.get("title"),
                    "risk_level": item.get("risk_level"),
                    "side_effect_level": item.get("side_effect_level"),
                    "requires_approval": bool(item.get("requires_approval")),
                    "status": item.get("status"),
                }
                for item in window
            ], int(page["total"])
        if server_key == "aistock-external-research":
            query = str(args.get("query") or args.get("q") or "external research").strip()
            as_of = utc_now().date().isoformat()
            if tool_name == "external_research_fetch_extract":
                url = str(args.get("url") or "https://example.org/external-research")
                return [
                    {
                        "title": f"Extracted evidence for {url}",
                        "summary": "Capped extract preview; full content is behind detail_ref.",
                        "url": url,
                        "source": "external_research_summary_adapter",
                        "as_of": as_of,
                        "evidence_ref": f"external-evidence:{sha256_json({'url': url})[:16]}",
                        "provider": "summary_adapter",
                        "detail_ref": {"server": server_key, "tool": tool_name, "args_hint": {"url": url, "max_chars": args.get("max_chars") or 2000}},
                    }
                ], 1
            result_type = "paper" if tool_name == "external_research_search_papers" else "web"
            digest = sha256_json({"query": query, "tool": tool_name})
            return [
                {
                    "title": f"{query} external evidence candidate",
                    "summary": f"Summary-first {result_type} evidence for {query}; use as hypothesis evidence, not a final conclusion.",
                    "url": f"https://example.org/external-research/{digest[:12]}",
                    "source": "external_research_summary_adapter",
                    "as_of": as_of,
                    "evidence_ref": f"external-evidence:{digest[:16]}",
                    "provider": "summary_adapter",
                    "result_type": result_type,
                    "detail_ref": {"server": server_key, "tool": "external_research_fetch_extract", "args_hint": {"url": "<url>", "max_chars": 2000}},
                }
            ][:limit], 1
        return [
            {
                "item_type": "mcp_read_tool_summary",
                "server_key": server_key,
                "tool_name": tool_name,
                "title": tool.get("title"),
                "risk_level": tool.get("risk_level"),
                "side_effect_level": tool.get("side_effect_level"),
                "requires_approval": bool(tool.get("requires_approval")),
                "status": tool.get("status"),
                "summary_first_contract": "list/search/overview returns compact fields; detail and heavy artifacts stay behind refs.",
            }
        ], 1

    @staticmethod
    def _summary_adapter_detail_tool(server_key: str, tool_name: str) -> str | None:
        detail_by_tool = {
            "factor_library_list": "factor_library_get",
            "factor_library_search": "factor_library_get",
            "factor_corr_get_top_pairs": "factor_corr_get_matrix_ref",
            "factor_corr_get_clusters": "factor_corr_get_matrix_ref",
            "model_registry_list": "model_registry_get",
            "strategy_governance_list_packages": "strategy_governance_get_package",
            "execution_policy_list_algos": "execution_policy_get_algo",
            "mcp_github_issue_list": "mcp_github_issue_search",
            "qe_archive_health": "qe_archive_list_runs",
            "external_research_search_web": "external_research_fetch_extract",
            "external_research_search_papers": "external_research_fetch_extract",
        }
        detail = detail_by_tool.get(tool_name)
        return f"{server_key}/{detail}" if detail else f"{server_key}/{tool_name}"

    @staticmethod
    def _summary_adapter_detail_args(tool_name: str, args: dict[str, Any]) -> dict[str, Any]:
        if tool_name.startswith("factor_library_"):
            return {"factor_name": args.get("factor_name") or "<factor_name>"}
        if tool_name.startswith("model_registry_"):
            return {"model_id": args.get("model_id") or "<model_id>"}
        if tool_name.startswith("strategy_governance_"):
            return {"package_id": args.get("package_id") or "<package_id>"}
        if tool_name.startswith("execution_policy_"):
            return {"algo_code": args.get("algo_code") or "<algo_code>"}
        if tool_name.startswith("factor_corr_"):
            return {"as_of_date": args.get("as_of_date") or "<as_of_date>"}
        return {}

    @staticmethod
    def _summary_adapter_artifact_refs(server_key: str, tool_name: str, args: dict[str, Any]) -> list[dict[str, Any]]:
        refs = [artifact_ref("mcp_summary_execution", f"research_assistant:{server_key}:{tool_name}", {"source": "catalog_summary_adapter"})]
        if "matrix" in tool_name or "corr" in tool_name:
            refs.append(artifact_ref("factor_correlation_matrix", "factor_correlation:matrix_ref", {"method": args.get("method")}))
        if "artifact" in tool_name:
            refs.append(artifact_ref("mcp_domain_artifact", f"{server_key}:{tool_name}:artifact_ref"))
        if "logs" in tool_name:
            refs.append(artifact_ref("mcp_log_tail", f"{server_key}:{tool_name}:log_ref"))
        if server_key == "aistock-external-research":
            refs.append(artifact_ref("external_evidence_summary", f"{server_key}:{tool_name}:evidence_ref", {"summary_first": True}))
        return refs

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
        tool = self._resolve_capability_tool(capability, proposal)
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
