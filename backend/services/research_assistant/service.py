"""Business service for the Research Assistant Console.

This service keeps Phase 1 state explicit and replayable. It does not execute
long-running experiments, does not write formal GitHub issues, and does not
fall back to in-memory storage unless tests inject that repository explicitly.
"""

from __future__ import annotations

import json
import logging
import os
import re
import hashlib
import subprocess
import threading
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from time import perf_counter
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import jsonschema

from backend.mcp.tool_manifest import TOOL_MANIFEST, TOOL_MANIFEST_BY_NAME
from backend.infra.deepseek_config import DEFAULT_DEEPSEEK_MODEL, resolve_deepseek_config
from backend.services.validation.pipeline_center import ValidationPipelineCenterService

from .context_budget import ContextBudgetPlan, ContextBudgetPlanner
from .code_intelligence import (
    artifact_ref_paths,
    build_code_intelligence_context,
    build_query_code_context,
    code_context_artifact_paths,
)
from .execution import ResearchAssistantExecutionMixin
from .graph_context import expand_neighbors
from .memory_curator import CuratorResult, MemoryCurator
from .memory_tree import select_memory_branches
from .models import (
    ActionProposalCreate,
    ActionProposalExecuteRequest,
    ActionProposalPreflightRequest,
    ApprovalCreate,
    CapabilitySyncRequest,
    ChatTurnRequest,
    ConversationCreate,
    ConversationMessageCreate,
    ContextPackBuildRequest,
    EvolutionPathCreate,
    ExternalAgentEventCreate,
    ExternalAgentSessionCreate,
    GraphEntityCreate,
    GraphRelationCreate,
    IssueCandidateCreate,
    IssueCandidateGithubSyncRequest,
    McpPreflightRequest,
    MemoryCreate,
    ModelRouteRequest,
    PromptBundleBuildRequest,
    TaskCreate,
    SkillUsageCreate,
    TaskEventCreate,
    TraceEventCreate,
    WorkbenchDryRunExecuteRequest,
    new_id,
    sha256_json,
    utc_now,
)
from .proactive_reports import (
    ProactiveReportContext,
    ProactiveReportProviderRegistry,
    build_default_proactive_report_registry,
    generate_proactive_report,
)
from .prompt_lab import (
    OfflinePromptJudge,
    build_prompt_lab_candidate,
    collect_prompt_lab_eval_set,
    judge_prompt_lab_candidate,
    prompt_lab_plan_digest,
)
from .reflection_card import build_reflection_artifacts, reflection_trigger_from_event
from .skill_library import (
    SKILL_LIBRARY_APPROVAL_PREFIX,
    SKILL_LIBRARY_APPROVAL_TYPE,
    SKILL_LIBRARY_REUSE_CAPABILITY_KEY,
    SKILL_LIBRARY_REUSE_CONFIRMATION,
    RepositorySkillLibraryExperienceReplayProvider,
    build_successful_workflow_recipe,
    search_approved_skill_recipes,
    skill_library_plan_digest,
)
from .react_grounding import (
    EvidenceGuardDecision,
    McpToolCall,
    McpToolResult,
    ModelTurn,
    ReactGroundingConfig,
    ReactGroundingResult,
    ToolCatalogEntry,
    ToolGateDecision,
    extract_structured_tool_calls,
    run_react_grounding_loop,
)
from .prompt_pack import (
    DEFAULT_PROMPT_PACK_PATH,
    PromptPackSnapshot,
    load_prompt_pack,
)
from .declarative_config import (
    ResearchAssistantDeclarativeConfigError,
    ResearchAssistantDeclarativeConfigSnapshot,
    load_declarative_config,
)
from .repository import DatabaseResearchAssistantRepository
from .runtime_config import (
    DEFAULT_ENVIRONMENT,
    REPO_ROOT,
    RUNTIME_CONFIG_KEY,
    RuntimeConfigCapabilityValidationError,
    RuntimeConfigSnapshot,
    load_runtime_config,
)
from .domain_ontology import DOMAIN_SPECS, McpDomain, domain_prompt_key
from .mcp_catalog_sync import (
    canonicalize_server_key,
    default_mcp_servers,
    default_mcp_tools,
    enrich_mcp_server_record,
    function_calling_tools_for_mcp,
    gateway_catalog,
    manifest_entry_to_mcp_tool,
    mcp_tool_function_name,
    server_key_for_module,
    workflow_capabilities as catalog_workflow_capabilities,
)
from .semantic_tool_planner import SemanticToolPlan, SemanticToolPlanner
from .tool_router import route_request, score_domains, select_tool
from .agent_teams import AgentTeamsRuntime, AgentTeamsRuntimeProviders, WorkerRunResult, load_agent_teams_config
from .agent_teams.models import WorkerTask
from .qe_autonomy import AutonomousEvolutionProviders, AutonomousEvolutionRuntime, request_from_mapping
from .qe_autonomy.adapter import QeAutonomyAdapter, ResearchAssistantQeAutonomyRunStore


class _ServiceAgentRunStore:
    def __init__(self, service: Any) -> None:
        self.service = service

    def queue_run(self, task: WorkerTask, *, agent_run_id: str, model_profile_id: str | None, trace_id: str | None) -> None:
        self.service.repository.create_record(
            "agent_runs",
            {
                "agent_run_id": agent_run_id,
                "parent_task_id": task.parent_task_id,
                "agent_key": task.agent_key,
                "role": task.role,
                "status": "queued",
                "input_json": task.input_json,
                "result_json": {},
                "model_profile_id": model_profile_id,
                "trace_id": trace_id,
            },
        )

    def finish_run(self, result: WorkerRunResult) -> None:
        status = "succeeded" if result.status == "succeeded" else "failed"
        self.service.repository.update_record(
            "agent_runs",
            result.agent_run_id,
            {
                "status": status,
                "result_json": result.as_reduce_item(),
                "trace_id": result.trace_id,
            },
        )


class _ServiceAgentContextProvider:
    def __init__(self, service: Any) -> None:
        self.service = service

    def build_for_worker(self, task: WorkerTask, agent: Any) -> dict[str, Any]:
        return self.service.build_context_pack(
            ContextPackBuildRequest(
                task_id=task.parent_task_id,
                agent_id=task.agent_key,
                model_profile=agent.model_role,
                user_message=task.objective,
                token_budget=1800,
            )
        )


class _ServiceAgentCatalogProvider:
    def __init__(self, service: Any) -> None:
        self.service = service

    def entries_for_worker(self, agent: Any) -> list[ToolCatalogEntry]:
        all_entries = self.service._react_tool_catalog_entries()
        allowed = agent.allowed_tool_pairs()
        return [entry for entry in all_entries if (entry.server_key, entry.tool_name) in allowed]


class _ServiceAgentWorkerExecutor:
    def __init__(self, service: Any, *, user_message: str) -> None:
        self.service = service
        self.user_message = user_message

    def run_worker(self, task: WorkerTask, agent: Any, context_pack: dict[str, Any], catalog_entries: list[ToolCatalogEntry]) -> WorkerRunResult:
        cards: dict[str, Any] = {"agent_key": task.agent_key, "action_proposals": []}
        code_context_refs = [
            str(ref.get("code_ref_id"))
            for ref in (task.input_json.get("code_context_refs") or [])
            if isinstance(ref, dict) and ref.get("code_ref_id")
        ]
        if task.agent_key == "qe_experiment_designer" and isinstance(task.input_json.get("qe_autonomy_request"), dict):
            report = self.service.run_qe_autonomous_evolution(dict(task.input_json["qe_autonomy_request"]))
            report_dict = report.to_dict() if hasattr(report, "to_dict") else dict(report)
            status = "failed" if report_dict.get("status") == "failed" else "succeeded"
            evidence_refs = tuple(sorted({str(ref) for ref in report_dict.get("evidence_refs", []) or ["qe_autonomy_report"]} | set(code_context_refs)))
            return WorkerRunResult(
                agent_run_id="service_runtime_pending",
                parent_task_id=task.parent_task_id,
                agent_key=task.agent_key,
                role=task.role,
                status=status,
                task_order=task.task_order,
                summary=f"QE autonomy {report_dict.get('status')}: {report_dict.get('stop_reason')}",
                artifacts=tuple(str(ref) for ref in report_dict.get("artifact_refs", []) or []),
                evidence_refs=evidence_refs,
                result_json={"autonomy_report": report_dict, "worker_consumed_autonomy": True, "code_context_ref_ids": code_context_refs},
                context_pack_id=str(context_pack.get("context_pack_id") or ""),
            )
        provider = _ServiceReactMcpProvider(
            service=self.service,
            conversation_id=str(task.input_json.get("conversation_id") or "agent_team"),
            task={"task_id": task.parent_task_id},
            context_pack=context_pack,
            cards=cards,
            user_message=self.user_message,
        )
        def model_complete(messages: list[dict[str, Any]]) -> ModelTurn:
            context_summary = json.dumps(
                {
                    "agent_key": task.agent_key,
                    "context_pack_id": context_pack.get("context_pack_id"),
                    "route_reason": (context_pack.get("pack_json") or {}).get("route_reason"),
                    "graph_relation_refs": (context_pack.get("pack_json") or {}).get("graph_relation_refs", [])[:3],
                    "code_context_ref_ids": code_context_refs[:3],
                    "code_affected_tests": list(task.input_json.get("code_affected_tests") or [])[:5],
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            return ModelTurn(
                content=f"{task.agent_key} completed with isolated context; source=agent_team_context as_of={utc_now().date().isoformat()} {context_summary}",
                provider="agent_team_worker",
                model=agent.model_role,
                duration_ms=0,
                usage={},
            )
        result = run_react_grounding_loop(
            messages=[
                {"role": "system", "content": f"Agent Team worker {task.agent_key}; allowed_tools={sorted(agent.allowed_tool_pairs())}"},
                {"role": "user", "content": task.objective},
            ],
            model_complete=model_complete,
            mcp_provider=provider,
            catalog_entries=catalog_entries,
            config=ReactGroundingConfig(max_tool_iterations=agent.max_tool_iterations, evidence_required=True),
        )
        status = "succeeded" if result.evidence_guard.allowed else "failed"
        evidence_refs = tuple(sorted({ref for tool_result in result.tool_results for ref in tool_result.source_refs} | {"agent_team_context"} | set(code_context_refs)))
        return WorkerRunResult(
            agent_run_id="service_runtime_pending",
            parent_task_id=task.parent_task_id,
            agent_key=task.agent_key,
            role=task.role,
            status=status,
            task_order=task.task_order,
            summary=result.final_text,
            artifacts=tuple(str(ref) for tool_result in result.tool_results for ref in tool_result.artifact_refs),
            evidence_refs=evidence_refs,
            result_json={"react_stopped_reason": result.stopped_reason, "tool_result_count": len(result.tool_results), "cards": cards, "code_context_ref_ids": code_context_refs},
            context_pack_id=str(context_pack.get("context_pack_id") or ""),
        )


class _ServiceAgentCurator:
    def create_candidates(self, parent_task_id: str, reduce_json: dict[str, Any]) -> list[dict[str, Any]]:
        evidence_refs = reduce_json.get("evidence_refs") if isinstance(reduce_json.get("evidence_refs"), list) else []
        if not evidence_refs:
            return []
        return [
            {
                "memory_type": "analysis_note",
                "tree_path": "personal.task.agent_team_progress",
                "approval_status": "draft",
                "content_text": str(reduce_json.get("assistant_text") or ""),
                "provenance_json": {"parent_task_id": parent_task_id, "source": "agent_team_reduce", "evidence_refs": evidence_refs},
            }
        ]


class _ServiceReactMcpProvider:
    def __init__(
        self,
        *,
        service: Any,
        conversation_id: str,
        task: dict[str, Any],
        context_pack: dict[str, Any],
        cards: dict[str, Any],
        user_message: str,
    ) -> None:
        self.service = service
        self.conversation_id = conversation_id
        self.task = task
        self.context_pack = context_pack
        self.cards = cards
        self.user_message = user_message

    @staticmethod
    def _merge_route_defaults(payload: dict[str, Any], route_args: dict[str, Any]) -> None:
        for key, value in route_args.items():
            if key == "tool_args" and isinstance(value, dict):
                existing = payload.get("tool_args") if isinstance(payload.get("tool_args"), dict) else {}
                for arg_key in ("symbol", "ts_code", "stock_code", "analysis_date", "period", "status", "order_by", "limit"):
                    if payload.get(arg_key) not in (None, "", [], {}):
                        existing[arg_key] = payload[arg_key]
                payload["tool_args"] = {**value, **existing}
                continue
            if key not in payload or payload.get(key) in (None, "", [], {}):
                payload[key] = value

    def _execute_manifest_read_only_direct(
        self,
        *,
        call: McpToolCall,
        decision: ToolGateDecision,
        payload: dict[str, Any],
    ) -> McpToolResult:
        tool, _server = self.service._resolve_mcp_catalog_tool(call.server_key, call.tool_name)
        start = perf_counter()
        try:
            adapter_result = self.service._execute_loopback_tool(tool, payload)
        except TimeoutError as exc:
            adapter_result = {
                "status": "failed",
                "result_json": {},
                "result_cards": [{"title": "Execution timeout", "summary": str(exc)}],
                "artifact_refs": [],
                "error_json": {"code": "timeout", "human_reason": str(exc), "exception_type": type(exc).__name__, "retryable": True},
                "retry_count": 0,
                "transport": "manifest_read_only_direct",
            }
        except Exception as exc:  # noqa: BLE001 - direct read failures must surface as tool errors, not crash chat.
            adapter_result = {
                "status": "failed",
                "result_json": {},
                "result_cards": [{"title": "Execution failed", "summary": str(exc)}],
                "artifact_refs": [],
                "error_json": {"code": "execution_failed", "human_reason": str(exc), "exception_type": type(exc).__name__, "retryable": False},
                "retry_count": 0,
                "transport": "manifest_read_only_direct",
            }
        duration_ms = int((perf_counter() - start) * 1000)
        event_status = "succeeded" if str(adapter_result.get("status") or "succeeded") == "succeeded" else "failed"
        result_cards = [dict(item) for item in (adapter_result.get("result_cards") or []) if isinstance(item, dict)]
        summary_result = dict(adapter_result.get("result_json") or {})
        error_json: dict[str, Any] = {}
        if event_status == "failed":
            raw_error = dict(adapter_result.get("error_json") or {})
            error_json = self.service._normalize_tool_error_payload(
                call,
                {
                    **raw_error,
                    "message": raw_error.get("human_reason") or raw_error.get("message") or (result_cards[0].get("summary") if result_cards else "MCP execution failed."),
                },
                stage="tool_execution",
            )
        event = self.service.repository.create_record(
            "mcp_tool_events",
            {
                "tool_event_id": new_id("mcptev"),
                "task_id": self.task["task_id"],
                "server_key": tool["server_key"],
                "tool_name": tool["tool_name"],
                "event_type": "execute",
                "status": event_status,
                "idempotency_key": sha256_json({"task_id": self.task["task_id"], "react_mcp_read": call.server_key, "tool_name": call.tool_name, "payload": payload}),
                "request_json": payload,
                "response_json": summary_result,
                "error_json": error_json,
                "action_proposal_id": None,
                "approval_id": None,
                "plan_digest": None,
                "transport": str(adapter_result.get("transport") or "manifest_read_only_direct"),
                "timeout_ms": int(self.service._execution_policy({"side_effect_level": "read_only"})["timeout_seconds"]) * 1000,
                "attempt_index": 0,
                "duration_ms": duration_ms,
                "result_card_json": result_cards[0] if result_cards else {},
                "artifact_refs": adapter_result.get("artifact_refs") or [],
                "started_at": utc_now().isoformat(),
                "completed_at": utc_now().isoformat(),
            },
        )
        trace_payload: dict[str, Any] = {
            "tool_event_id": event["tool_event_id"],
            "human_cards": result_cards,
            "source": "manifest_read_only_direct",
            "catalog_reason": decision.reason,
        }
        if error_json:
            trace_payload["error"] = error_json
        trace = self.service.create_trace_event(
            TraceEventCreate(
                task_id=self.task["task_id"],
                event_type="action_execute",
                component="research_assistant.execution_gateway",
                status=event_status,
                duration_ms=duration_ms,
                payload_json=trace_payload,
            )
        )
        self.service.add_task_event(
            self.task["task_id"],
            TaskEventCreate(
                event_type="mcp_done" if event_status == "succeeded" else "mcp_failed",
                severity="info" if event_status == "succeeded" else "error",
                message=f"Manifest read-only MCP execution {event_status}: {call.server_key}/{call.tool_name}",
                payload_json={"tool_event_id": event["tool_event_id"], "trace_id": trace["trace_id"], "source": "manifest_read_only_direct"},
            ),
        )
        result = McpToolResult(
            server_key=call.server_key,
            tool_name=call.tool_name,
            status=event_status,
            payload_json=summary_result,
            source_refs=self.service._mcp_result_source_refs(summary_result, event),
            as_of=self.service._mcp_result_as_of(summary_result),
            artifact_refs=list(summary_result.get("artifact_refs") or event.get("artifact_refs") or []),
            summary=json.dumps(self.service._compact_mcp_summary_for_cards(summary_result), ensure_ascii=False, sort_keys=True),
            tool_event_id=event["tool_event_id"],
            action_proposal_id=None,
            preflight={"passed": True, "approval_required": False, "preflight_checks": ["manifest_read_only_catalog"]},
            executed=event_status == "succeeded",
            error_json=error_json,
            side_effect_level=str(decision.side_effect_level or call.side_effect_level or "read_only"),
        )
        if error_json:
            result.blocked_reason = str(error_json.get("reason_code") or error_json.get("code") or "tool_execution_error")
            result.summary = self.service._render_tool_error_reply(error_json)
        self.service._populate_cards_from_tool_execution(
            self.cards,
            {"action_proposal_id": None},
            {
                "status": event_status,
                "executed": event_status == "succeeded",
                "tool_event": event,
                "trace_id": trace["trace_id"],
                "human_cards": result_cards,
                **({"error": error_json} if error_json else {}),
            },
            result,
        )
        return result

    def _route_for_call(self, call: McpToolCall) -> dict[str, Any]:
        route = self.cards.get("mcp_route_decision") if isinstance(self.cards.get("mcp_route_decision"), dict) else {}
        candidates = route.get("route_candidates") if isinstance(route.get("route_candidates"), list) else []
        for candidate in candidates:
            if (
                isinstance(candidate, dict)
                and candidate.get("server_key") == call.server_key
                and candidate.get("tool_name") == call.tool_name
            ):
                return dict(candidate)
        route_copy = dict(route)
        call_domain = self.service._domain_for_mcp_tool(call.tool_name) or route_copy.get("domain") or ""
        route_copy.update(
            {
                "server_key": call.server_key,
                "tool_name": call.tool_name,
                "domain": call_domain,
            }
        )
        return route_copy

    def execute_read_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        try:
            route = self._route_for_call(call)
            payload = dict(call.payload_json)
            payload.setdefault("request", self.user_message)
            payload.setdefault("route", route)
            payload.setdefault("mcp_route_decision", route)
            payload["server_key"] = call.server_key
            payload["tool_name"] = call.tool_name
            call_domain = self.service._domain_for_mcp_tool(call.tool_name) or ""
            payload["selected_tool"] = {
                "server_key": call.server_key,
                "tool_name": call.tool_name,
                "domain": route.get("domain") or call_domain,
            }
            self._merge_route_defaults(payload, self.service._mcp_route_tool_args(route))
            payload.setdefault("limit", self.service._mcp_route_limit(route))
            try:
                capability_key = self.service._capability_key_for_tool(call, route)
            except KeyError:
                if str(decision.side_effect_level or call.side_effect_level or "read_only") == "read_only":
                    return self._execute_manifest_read_only_direct(call=call, decision=decision, payload=payload)
                raise
            proposal = self.service.create_action_proposal(
                ActionProposalCreate(
                    task_id=self.task["task_id"],
                    conversation_id=self.conversation_id,
                    capability_key=capability_key,
                    proposal_type="mcp_tool",
                    title=f"Summary-first MCP read: {call.server_key}/{call.tool_name}",
                    summary=f"Auto-execute low-risk read-only MCP summary for route {call.server_key}/{call.tool_name}.",
                    input_json=payload,
                    expected_result_json={"summary_first": True, "server_key": call.server_key, "tool_name": call.tool_name},
                    context_pack_id=self.context_pack.get("context_pack_id"),
                    idempotency_key=sha256_json({"task_id": self.task["task_id"], "react_mcp_read": call.server_key, "tool_name": call.tool_name, "payload": payload}),
                    created_by="research_assistant_react_grounding",
                )
            )
            preflight = self.service.preflight_action_proposal(
                proposal["action_proposal_id"],
                ActionProposalPreflightRequest(payload_json=payload, idempotency_key=proposal["idempotency_key"]),
            )
            if preflight["proposal"]["status"] != "preflight_passed":
                result = McpToolResult(
                    server_key=call.server_key,
                    tool_name=call.tool_name,
                    status="preflight_blocked",
                    summary="read-only preflight did not pass",
                    source_refs=["preflight"],
                    as_of=utc_now().date().isoformat(),
                    action_proposal_id=proposal["action_proposal_id"],
                    preflight=preflight["preflight"],
                    executed=False,
                    blocked_reason="preflight_blocked",
                    side_effect_level=str(decision.side_effect_level or call.side_effect_level or "read_only"),
                )
                self.cards["mcp_execution_result"] = {
                    "auto_executed": False,
                    "status": "preflight_blocked",
                    "route": f"{call.server_key}/{call.tool_name}",
                    "server_key": call.server_key,
                    "tool_name": call.tool_name,
                    "action_proposal_id": proposal["action_proposal_id"],
                    "preflight": preflight["preflight"],
                    "summary_first": True,
                }
                return result
            executed = self.service.execute_action_proposal(
                proposal["action_proposal_id"],
                ActionProposalExecuteRequest(payload_json=payload, idempotency_key=proposal["idempotency_key"]),
            )
            tool_event = executed.get("tool_event") if isinstance(executed.get("tool_event"), dict) else {}
            summary_result = tool_event.get("response_json") if isinstance(tool_event.get("response_json"), dict) else {}
            result = McpToolResult(
                server_key=call.server_key,
                tool_name=call.tool_name,
                status=str(executed.get("status") or "unknown"),
                payload_json=summary_result,
                source_refs=self.service._mcp_result_source_refs(summary_result, tool_event),
                as_of=self.service._mcp_result_as_of(summary_result),
                artifact_refs=list(summary_result.get("artifact_refs") or tool_event.get("artifact_refs") or []),
                summary=json.dumps(self.service._compact_mcp_summary_for_cards(summary_result), ensure_ascii=False, sort_keys=True),
                tool_event_id=tool_event.get("tool_event_id"),
                action_proposal_id=proposal["action_proposal_id"],
                preflight=preflight["preflight"],
                executed=bool(executed.get("executed")),
                error_json=dict(executed.get("error") or {}),
                side_effect_level=str(decision.side_effect_level or call.side_effect_level or "read_only"),
            )
            if result.status == "failed" and result.error_json:
                result.error_json = self.service._normalize_tool_error_payload(
                    call,
                    result.error_json,
                    stage="tool_execution",
                )
                result.blocked_reason = str(result.error_json.get("reason_code") or result.error_json.get("code") or "tool_execution_error")
                result.summary = self.service._render_tool_error_reply(result.error_json)
            self.service._populate_cards_from_tool_execution(self.cards, proposal, executed, result)
            return result
        except Exception as exc:  # noqa: BLE001 - explicit tool error must be returned to the chat turn.
            return self.service._mcp_tool_failure_result(call, exc, stage="tool_dispatch", cards=self.cards, task_id=self.task["task_id"])

    def preflight_confirmation_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        try:
            route = self._route_for_call(call)
            payload = dict(call.payload_json)
            payload.setdefault("request", self.user_message)
            payload.setdefault("route", route)
            payload.setdefault("mcp_route_decision", route)
            payload["server_key"] = call.server_key
            payload["tool_name"] = call.tool_name
            call_domain = self.service._domain_for_mcp_tool(call.tool_name) or ""
            payload["selected_tool"] = {
                "server_key": call.server_key,
                "tool_name": call.tool_name,
                "domain": route.get("domain") or call_domain,
            }
            self._merge_route_defaults(payload, self.service._mcp_route_tool_args(route))
            capability_key = self.service._capability_key_for_tool(call, route)
            proposal = self.service.create_action_proposal(
                ActionProposalCreate(
                    task_id=self.task["task_id"],
                    conversation_id=self.conversation_id,
                    capability_key=capability_key,
                    proposal_type="mcp_tool",
                    title=f"Preflight MCP action: {call.server_key}/{call.tool_name}",
                    summary=f"Generate preflight and confirmation card for {call.server_key}/{call.tool_name}; ReAct will not execute write/high-risk tools.",
                    input_json=payload,
                    expected_result_json={"preflight_only": True, "server_key": call.server_key, "tool_name": call.tool_name},
                    context_pack_id=self.context_pack.get("context_pack_id"),
                    idempotency_key=sha256_json({"task_id": self.task["task_id"], "react_mcp_preflight": call.server_key, "tool_name": call.tool_name, "payload": payload}),
                    created_by="research_assistant_react_grounding",
                )
            )
        except Exception as exc:  # noqa: BLE001 - capability/preflight setup errors are user-visible tool errors.
            return self.service._mcp_tool_failure_result(call, exc, stage="preflight_setup", cards=self.cards, task_id=self.task["task_id"])
        try:
            preflight = self.service.preflight_action_proposal(
                proposal["action_proposal_id"],
                ActionProposalPreflightRequest(payload_json=payload, idempotency_key=proposal["idempotency_key"]),
            )
        except Exception as exc:
            if "proposal must be confirmed before preflight" in str(exc):
                preflight_payload = self.service.preflight_mcp_tool(
                    McpPreflightRequest(
                        task_id=proposal["task_id"],
                        server_key=call.server_key,
                        tool_name=call.tool_name,
                        payload_json=payload,
                        idempotency_key=proposal["idempotency_key"],
                    )
                )
                if preflight_payload.get("tool_event_id"):
                    self.service.repository.update_record(
                        "mcp_tool_events",
                        preflight_payload["tool_event_id"],
                        {
                            "action_proposal_id": proposal["action_proposal_id"],
                            "plan_digest": proposal["plan_digest"],
                            "transport": "research_assistant_chat_preflight",
                            "response_json": preflight_payload,
                        },
                    )
                next_status = "preflight_failed" if preflight_payload.get("failed_checks") else "approval_required" if preflight_payload.get("approval_required") else "preflight_passed"
                proposal = self.service.repository.update_record("action_proposals", proposal["action_proposal_id"], {"status": next_status})
                preflight = {"proposal": proposal, "preflight": preflight_payload}
            else:
                error_payload = self.service._tool_error_payload(call, exc, stage="preflight")
                logger.exception(
                    "research assistant MCP preflight failed: reason_code=%s tool=%s/%s",
                    error_payload["reason_code"],
                    call.server_key,
                    call.tool_name,
                )
                preflight = {
                    "proposal": proposal,
                    "preflight": {
                        "passed": False,
                        "approval_required": True,
                        "failed_checks": [{"check": "preflight", "detail": error_payload["message"], "reason_code": error_payload["reason_code"]}],
                        "error": error_payload,
                    },
                }
        proposal_state = preflight.get("proposal") if isinstance(preflight.get("proposal"), dict) else proposal
        preflight_payload = preflight.get("preflight") if isinstance(preflight.get("preflight"), dict) else {}
        status = str(proposal_state.get("status") or "preflight_required")
        approval = None
        if status == "approval_required":
            try:
                approval = self.service._ensure_action_proposal_chat_approval(
                    proposal_state,
                    preflight_payload=preflight_payload,
                    decision=decision,
                    call=call,
                )
                proposal_state = self.service.repository.get_record("action_proposals", proposal["action_proposal_id"]) or proposal_state
                preflight_payload = dict(preflight_payload)
                preflight_payload["approval_id"] = approval["approval_id"]
                preflight_payload["required_confirmation_text"] = approval["required_confirmation_text"]
                preflight_payload["approval_type"] = approval["approval_type"]
            except Exception as exc:
                reason_code = "approval_confirmation_token_config_missing" if "missing configured required_confirmations" in str(exc) else "approval_setup_failed"
                error_payload = {
                    "reason_code": reason_code,
                    "code": reason_code,
                    "stage": "approval_setup",
                    "server_key": call.server_key,
                    "tool_name": call.tool_name,
                    "action_proposal_id": proposal.get("action_proposal_id"),
                    "capability_key": proposal.get("capability_key"),
                    "exception_type": type(exc).__name__,
                    "message": str(exc),
                }
                logger.exception(
                    "research assistant approval setup failed: reason_code=%s proposal=%s tool=%s/%s",
                    reason_code,
                    proposal.get("action_proposal_id"),
                    call.server_key,
                    call.tool_name,
                )
                preflight_payload = dict(preflight_payload)
                preflight_payload["approval_required"] = True
                preflight_payload["passed"] = False
                preflight_payload["error"] = error_payload
                failed_checks = list(preflight_payload.get("failed_checks") or [])
                failed_checks.append({"check": "approval_setup", "detail": str(exc), "reason_code": reason_code})
                preflight_payload["failed_checks"] = failed_checks
        self.cards.setdefault("action_proposals", [])
        proposal_card = {
            "title": proposal["title"],
            "risk": decision.risk_level,
            "approval_required": True,
            "status": status,
            "action_proposal_id": proposal["action_proposal_id"],
            "route": f"{call.server_key}/{call.tool_name}",
            "required_confirmations": (decision.catalog_entry.required_confirmations if decision.catalog_entry else ()),
        }
        if approval:
            proposal_card.update(
                {
                    "approval_id": approval["approval_id"],
                    "approval_type": approval["approval_type"],
                    "required_confirmation_text": approval["required_confirmation_text"],
                    "approval_status": approval["status"],
                }
            )
        self.cards["action_proposals"].append(proposal_card)
        self.cards["mcp_execution_result"] = {
            "auto_executed": False,
            "executed": False,
            "status": status if status in {"approval_required", "preflight_failed"} else "preflight_required",
            "route": f"{call.server_key}/{call.tool_name}",
            "server_key": call.server_key,
            "tool_name": call.tool_name,
            "action_proposal_id": proposal["action_proposal_id"],
            "preflight": preflight_payload,
            "summary_first": True,
        }
        if approval:
            self.cards["mcp_execution_result"].update(
                {
                    "approval_id": approval["approval_id"],
                    "approval_type": approval["approval_type"],
                    "required_confirmation_text": approval["required_confirmation_text"],
                    "approval_status": approval["status"],
                }
            )
        self.cards["mcp_preflight_result"] = preflight_payload
        return McpToolResult(
            server_key=call.server_key,
            tool_name=call.tool_name,
            status=self.cards["mcp_execution_result"]["status"],
            payload_json={
                "preflight_only": True,
                "response_mode": "mcp_safe_preflight_summary",
                "preflight": preflight_payload,
            },
            source_refs=["preflight"],
            as_of=utc_now().date().isoformat(),
            summary="preflight confirmation card generated; write/high-risk execution was not called",
            action_proposal_id=proposal["action_proposal_id"],
            preflight=preflight_payload,
            executed=False,
            blocked_reason="preflight_confirmation_required",
            error_json=dict(preflight_payload.get("error") or {}),
            side_effect_level=str(decision.side_effect_level or call.side_effect_level or "read_only"),
        )



logger = logging.getLogger("aistock.research_assistant.service")

ASSISTANT_APPROVAL_CONFIRM = "APPROVE_RESEARCH_ASSISTANT_ACTION"
ACTION_PROPOSAL_EXECUTE_APPROVAL_TYPE = "action_proposal.execute"
PIPELINE_ISSUE_FACT_SOURCE_UNAVAILABLE = "validation_issue_fact_source_unavailable"
PIPELINE_ISSUE_SOURCE_OF_TRUTH = "validation_pipeline_issue_candidates"
RA_DRAFT_STORAGE_NOTICE = "\u975e\u6743\u5a01\u5bf9\u8bdd\u8349\u7a3f/\u89e3\u91ca\u7f13\u5b58\uff0c\u5df2\u9000\u573a\uff1b\u6b63\u5f0f\u4e8b\u5b9e\u6e90=Validation/Nightly/issue workflow"
RA_OFFICIAL_WORKFLOW_NOTICE = "\u6b63\u5f0f\u63d0\u4ea4\u5fc5\u987b\u8d70 AIstock issue workflow / Validation MCP"
PROMPT_CACHE_DIR = Path(os.getenv("AISTOCK_ASSISTANT_PROMPT_CACHE_DIR", "var/research_assistant/prompt_cache"))
CATALOG_BOOTSTRAP_ACTION = "POST /api/v1/research-assistant/catalogs/seed"
SERVICE_MODULE_PATH = Path(__file__).resolve()
MCP_BUSINESS_REPLY_FORBIDDEN_MARKERS = (
    "summary-first",
    "summary_first",
    "Route decision",
    "route decision",
    "artifact_ref",
    "payload budget",
    "raw_payload",
    "omitted_sections",
    "server_key",
    "server_key=",
    "tool_name",
    "tool_name=",
    "selected_tool",
    "detail tool",
    "detail_tool",
    "transport",
    "mcp_tool_event",
    "mcp_summary_result",
    "mcp_execution_result",
    "response_mode",
    "summary_envelope",
    "mcp route",
    "Evidence: source=",
    "source=",
    "as_of=",
    "research_assistant_catalog_summary_adapter",
    "summary_adapter",
    "\u6211\u53ea\u5c55\u793a\u6982\u8981",
)


class IssueCandidateFactSource:
    """Lazy read-only adapter for the Validation candidate fact source."""

    def __init__(self, pipeline_center: Any | None = None, pipeline_center_factory: Any | None = None) -> None:
        self._pipeline_center = pipeline_center
        self._pipeline_center_factory = pipeline_center_factory or ValidationPipelineCenterService

    @property
    def pipeline_center(self) -> Any:
        if self._pipeline_center is None:
            self._pipeline_center = self._pipeline_center_factory()
        return self._pipeline_center

    def issue_candidates(
        self,
        *,
        module: str | None = None,
        severity: str | None = None,
        status: str | None = None,
        search: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        return self.pipeline_center.issue_candidates(
            module=module,
            severity=severity,
            status=status,
            search=search,
            page=page,
            page_size=page_size,
        )

    def issue_candidate_summary(self, *, search: str | None = None) -> dict[str, Any]:
        return self.pipeline_center.issue_candidate_summary(search=search)

BUSINESS_SYNTHESIS_RESPONSE_MODES = {
    "local_data_daily_sync_status",
    "qe_experiment_status_summary",
    "qe_warehouse_business_summary",
    "stock_analysis_evidence_card",
}
AGENTIC_SYNTHESIS_FORBIDDEN_MARKERS = (
    "\u5df2\u6c47\u603b",
    "\u72b6\u6001\u6c47\u603b\uff1a",
    "\u72b6\u6001\u6c47\u603b:",
    "\u5df2\u6c47\u603b QE",
    "\u5df2\u6c47\u603b\u672c\u5730\u6570\u636e",
    "QE \u6570\u4ed3\u5065\u5eb7\u6c47\u603b\u5982\u4e0b",
    "\u672c\u8f6e\u672a\u542f\u52a8\u3001\u6267\u884c\u3001\u7269\u5316\u6216\u4fee\u6539\u4efb\u4f55 QE \u5b9e\u9a8c",
    "\u672c\u8f6e\u672a\u6267\u884c\u4efb\u4f55\u540c\u6b65",
    "\u672c\u8f6e\u672a\u6267\u884c backfill",
)
GRAPH_FIRST_RELATION_TERMS = (
    "关系",
    "怎么用",
    "如何用",
    "怎样用",
    "怎么利用",
    "如何利用",
    "怎么接",
    "如何接",
    "怎么流转",
    "如何流转",
    "链路",
    "路径",
    "打通",
    "串起来",
    "relationship",
    "how to use",
    "use path",
    "flow",
    "lineage",
)
GRAPH_CONTEXT_SOURCE = "graph_context"
GRAPH_CONTEXT_AS_OF = "LIVE"
AGENTIC_SYNTHESIS_SYSTEM_PROMPT = (
    "你是 AIstock 的研究助理 Agent。你是懂这套系统的分析师，不是复述单个工具输出的转述器。"
    "先理解用户真实意图，再自主收集必要信息并综合分析；不要只调一个工具就交差。"
    "跨模块、怎么用、什么关系、如何流转的问题必须先读 graph_context 理清关系，再按需调用多个只读工具。"
    "回答先给 1-2 句 bottom-line，再给支撑细节；多源结果要合成一个判断，不要按每个数据源模板罗列。"
    "每个事实或数字必须引用实际工具或图谱上下文中的 source/as_of/trade_date/report_period。"
    "涉及未来/预测时只讲驱动、情景、风险和边界，不做涨跌或方向预测，不构成投资建议。"
    "写入、提交、训练、晋升和生产变更只说明审批/预检边界，不能绕过确认门。"
    "中文人话作答，禁 raw JSON、server_key、tool_name、reason_code、summary_first、mcp_execution_result、subject_key、memory_type 等内部行话。"
    "不要编造事实、占位符、来源、日期或动作。"
)

MEMORY_CURATOR_SEMANTIC_SYSTEM_PROMPT = (
    "你是 AIstock 研究助理的长期记忆候选提炼器。只输出 JSON，不输出解释。"
    "从 user_message 和 assistant_message 中判断用户是否表达了需要长期记住的偏好、习惯、项目指令或待办。"
    "能力询问或缺少具体内容时返回空 candidates；不要因为用户只问能否记住就创建候选。"
    "英文 seed 语句可作为线索，但不能是唯一依据；中文自然表达也要按语义识别。"
    "助手回复可用于提炼用户确认过或助手复述出的候选内容，但不得编造用户没有表达的记忆。"
    "输出格式：{\"candidates\":[{"
    "\"memory_type\":\"user_preference|habit|directive|task_state|analysis_note\","
    "\"scope\":\"personal|project\","
    "\"tree_path\":\"personal.preference.response 等 scope 内路径\","
    "\"title\":\"中文短标题\","
    "\"content_text\":\"要记住的具体内容\","
    "\"trust_level\":\"user_stated|assistant_inferred\","
    "\"resident\":false,"
    "\"requires_approval\":true,"
    "\"importance\":0.0"
    "}]}。"
    "策略必须保持：project directive 必须 requires_approval=true 且 resident=false；"
    "personal preference/habit 可以 requires_approval=false 且按需 resident=true；"
    "待办或任务状态用 personal.task_state.todo，requires_approval=true，resident=false。"
)

STOCK_DEPTH_STOCK_TOOL_NAMES = (
    "stock_analysis_get_quote",
    "stock_analysis_get_kline",
    "stock_analysis_get_financials",
    "stock_analysis_get_quarterly",
    "stock_analysis_get_margin_financing",
    "stock_analysis_get_fund_flow",
    "stock_analysis_get_technicals",
)
STOCK_DEPTH_EXTERNAL_TOOL_NAMES = (
    "external_research_search_web",
    "external_research_fetch_extract",
)
STOCK_DEPTH_REQUIRED_TOOL_REFS = (
    *(("aistock-stock-analysis", tool_name) for tool_name in STOCK_DEPTH_STOCK_TOOL_NAMES),
    *(("aistock-external-research", tool_name) for tool_name in STOCK_DEPTH_EXTERNAL_TOOL_NAMES),
)
STOCK_DEPTH_SEEDED_TOOL_REFS = (
    *(("aistock-stock-analysis", tool_name) for tool_name in STOCK_DEPTH_STOCK_TOOL_NAMES),
    ("aistock-external-research", "external_research_search_web"),
)
STOCK_DEPTH_MIN_HISTORY_TRADING_DAYS = 60
STOCK_DEPTH_HISTORY_PERIOD = "1y"
STOCK_DEPTH_MIN_TOOL_EXECUTIONS = 8
STOCK_DEPTH_SYMBOL_ALIASES = {
    "\u56fd\u57ce\u77ff\u4e1a": "000688",
}
STOCK_DEPTH_FOCUS_TERMS = (
    "stock depth",
    "all-round",
    "comprehensive",
    "\u4e09\u7ef4",
    "\u7efc\u5408",
    "\u5168\u65b9\u4f4d",
    "\u6df1\u5ea6",
)
STOCK_DEPTH_DIMENSION_TERMS = (
    "limit down",
    "fundamental",
    "fundamentals",
    "future trend",
    "recent trend",
    "\u4e2a\u80a1",
    "\u80a1\u7968",
    "\u8dcc\u505c",
    "\u57fa\u672c\u9762",
    "\u57fa\u672c\u60c5\u51b5",
    "\u8fd1\u671f\u8d70\u52bf",
    "\u672a\u6765\u8d8b\u52bf",
    "\u884c\u4e1a\u5730\u4f4d",
    "\u8d44\u91d1",
    "\u8d22\u52a1",
    "\u6280\u672f",
    "fund flow",
    "financial",
    "technical",
    "industry",
)


def _git_output(args: list[str]) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(REPO_ROOT), *args],
            capture_output=True,
            check=False,
            encoding="utf-8",
            errors="replace",
            timeout=2,
        )
    except Exception:
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def _file_sha256(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def _short_hash(value: str | None) -> str | None:
    return value[:8] if value else None


SERVICE_LOADED_AT = utc_now().isoformat()
SERVICE_LOADED_GIT_COMMIT = _git_output(["rev-parse", "HEAD"])
SERVICE_LOADED_SOURCE_SHA256 = _file_sha256(SERVICE_MODULE_PATH)


class DialogueIntent(str, Enum):
    CAPABILITY_INQUIRY = "capability_inquiry"
    CONCEPT_EXPLANATION = "concept_explanation"
    STATUS_QUERY = "status_query"
    BUG_DIAGNOSIS_REQUEST = "bug_diagnosis_request"
    ISSUE_INTAKE_REQUEST = "issue_intake_request"
    EXPERIMENT_DRAFT_REQUEST = "experiment_draft_request"
    EXPERIMENT_VALIDATION_REQUEST = "experiment_validation_request"
    EXPERIMENT_EXECUTION_REQUEST = "experiment_execution_request"
    LOCAL_DATA_MANAGEMENT_REQUEST = "local_data_management_request"
    MCP_CAPABILITY_INQUIRY = "mcp_capability_inquiry"
    QE_WAREHOUSE_REQUEST = "qe_warehouse_request"
    RESEARCH_PIPELINE_REQUEST = "research_pipeline_request"
    VALIDATION_ISSUE_REQUEST = "validation_issue_request"
    FACTOR_LIBRARY_REQUEST = "factor_library_request"
    FACTOR_METRICS_REQUEST = "factor_metrics_request"
    FACTOR_CORRELATION_REQUEST = "factor_correlation_request"
    MODEL_REGISTRY_REQUEST = "model_registry_request"
    STRATEGY_GOVERNANCE_REQUEST = "strategy_governance_request"
    EXECUTION_POLICY_REQUEST = "execution_policy_request"
    EXTERNAL_RESEARCH_REQUEST = "external_research_request"
    STOCK_ANALYSIS_REQUEST = "stock_analysis_request"
    AMBIGUOUS_REQUEST = "ambiguous_request"
    GENERAL_CHAT = "general_chat"
    AUDIT_REQUEST = "audit_request"
    RECOVERY_REQUEST = "recovery_request"


DIALOGUE_INTENT_CONFIG_KEY = "dialogue_intent"
DIALOGUE_MODES_CONFIG_KEY = "dialogue_modes"
MODE_ROUTER_CONFIG_KEY = "mode_router"


class DialogueMode(str, Enum):
    DIALOGUE = "dialogue"
    ANALYSIS = "analysis"
    PLANNING = "planning"
    PREFLIGHT = "preflight"
    EXECUTION = "execution"
    AUDIT = "audit"
    RECOVERY = "recovery"


@dataclass(frozen=True)
class ModeDecision:
    mode: DialogueMode
    intent_type: DialogueIntent
    confidence: float
    mode_reason: str
    requires_tool: bool
    allowed_tool_side_effect: str
    requires_user_confirmation: bool
    requires_approval: bool
    visible_audit_default: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode.value,
            "intent_type": self.intent_type.value,
            "confidence": self.confidence,
            "mode_reason": self.mode_reason,
            "requires_tool": self.requires_tool,
            "allowed_tool_side_effect": self.allowed_tool_side_effect,
            "requires_user_confirmation": self.requires_user_confirmation,
            "requires_approval": self.requires_approval,
            "visible_audit_default": self.visible_audit_default,
        }


class ResearchAssistantCatalogNotReadyError(RuntimeError):
    """Raised when required assistant catalogs are empty or disabled."""

    def __init__(self, readiness: dict[str, Any]) -> None:
        self.readiness = readiness
        missing = ", ".join(readiness.get("missing_catalogs") or [])
        message = f"Research Assistant catalogs are not ready: {missing or 'unknown'}"
        super().__init__(message)


class ResearchAssistantRuntimeConfigInvalidError(RuntimeError):
    """Raised when the active DB runtime config is invalid and must be reseeded."""

    def __init__(self, error_payload: dict[str, Any]) -> None:
        self.error_payload = error_payload
        super().__init__(str(error_payload.get("message") or error_payload.get("reason_code") or "runtime_config_invalid"))


DEFAULT_SKILLS: list[dict[str, Any]] = [
    {
        "skill_key": "qe-evolution-diagnostics",
        "title": "QE diagnostics",
        "description": "Analyze QE evolution experiments, loop metrics, stability, leakage risk, and evidence.",
        "domain": "qe",
        "risk_level": "medium",
        "permission_scope": "read_analysis",
        "tags_json": ["qe", "diagnostics", "analysis"],
        "input_schema_json": {"type": "object", "required": ["experiment_id"]},
        "output_schema_json": {"type": "object", "required": ["summary", "evidence_refs"]},
    },
    {
        "skill_key": "analyze-factor-library",
        "title": "Factor library analysis",
        "description": "Analyze factor type coverage, IC statistics, correlation, and replacement candidates.",
        "domain": "factor_library",
        "risk_level": "medium",
        "permission_scope": "read_analysis",
        "tags_json": ["factor", "ic", "correlation"],
        "input_schema_json": {"type": "object"},
        "output_schema_json": {"type": "object", "required": ["recommendations"]},
    },
    {
        "skill_key": "develop-factor",
        "title": "Factor research task package",
        "description": "Registers a planning and approval handoff for factor research tasks.",
        "domain": "factor_research",
        "risk_level": "high",
        "permission_scope": "plan_only",
        "tags_json": ["factor", "research", "plan"],
        "input_schema_json": {"type": "object", "required": ["idea"]},
        "output_schema_json": {"type": "object", "required": ["plan", "approval_required"]},
    },
    {
        "skill_key": "rdagent-task-analyzer",
        "title": "RDAgent task analysis",
        "description": "Diagnose RDAgent tasks, model code, convergence, and backtest metrics.",
        "domain": "rdagent",
        "risk_level": "medium",
        "permission_scope": "read_analysis",
        "tags_json": ["rdagent", "task", "metrics"],
        "input_schema_json": {"type": "object", "required": ["task_id"]},
        "output_schema_json": {"type": "object", "required": ["summary"]},
    },
    {
        "skill_key": "rdagent-data-doctor",
        "title": "Create assistant task",
        "description": "Diagnose Qlib, factor production, and factor debug data chains; write repair requires approval.",
        "domain": "data_quality",
        "risk_level": "high",
        "permission_scope": "preflight_required",
        "tags_json": ["data", "quality", "preflight"],
        "input_schema_json": {"type": "object"},
        "output_schema_json": {"type": "object", "required": ["findings"]},
    },
    {
        "skill_key": "local_data_management",
        "title": "Local data management capability",
        "description": "Inspect local data readiness, sync targets, ingestion evidence, and repair plans through the aistock-local-data MCP server; confirmed repair or sync execution requires approval.",
        "domain": "data_sync",
        "risk_level": "production_sensitive",
        "permission_scope": "read_plan_confirmed_write",
        "tags_json": ["local_data", "data_sync", "dataset_readiness", "repair_plan"],
        "input_schema_json": {"type": "object", "properties": {"request": {"type": "string"}}},
        "output_schema_json": {"type": "object", "required": ["capability_summary", "approval_required"]},
        "required_mcp_tools": [
            "aistock-local-data/local_data_health_overview",
            "aistock-local-data/local_data_get_dataset_status",
            "aistock-local-data/local_data_get_preset_daily_status",
            "aistock-local-data/local_data_list_jobs",
            "aistock-local-data/local_data_list_sync_targets",
            "aistock-local-data/local_data_plan_repair",
            "aistock-local-data/local_data_apply_repair_confirmed",
        ],
        "skill_type": "assistant_capability",
        "entrypoint_type": "mcp_composite",
        "entrypoint_ref": "aistock-local-data",
        "allowed_side_effect_level": "controlled_write",
        "required_approval_level": "L2",
        "source_ref": "docs/architecture/local_data_management_mcp_gateway_design_20260523.md#research-assistant-seed",
        "status": "approved",
    },
]


DEFAULT_MCP_SERVERS: list[dict[str, Any]] = default_mcp_servers()


DEFAULT_MCP_TOOLS: list[dict[str, Any]] = default_mcp_tools()

MCP_TOOL_DB_COLUMNS = {
    "tool_id",
    "server_key",
    "tool_name",
    "title",
    "description",
    "risk_level",
    "side_effect_level",
    "requires_approval",
    "input_schema_json",
    "output_schema_json",
    "preflight_schema_json",
    "required_confirmations",
    "status",
}


def _retired_draft_storage_capability(item: dict[str, Any]) -> bool:
    return str(item.get("capability_key") or "") == "issue.create_candidate"


DEFAULT_WORKFLOW_CAPABILITIES: list[dict[str, Any]] = [
    dict(item)
    for item in [
        *load_runtime_config(environment=DEFAULT_ENVIRONMENT).config["planner"].get("workflow_capabilities", []),
        *catalog_workflow_capabilities(),
    ]
    if not _retired_draft_storage_capability(dict(item))
]



DEFAULT_MODEL_PROFILES: list[dict[str, Any]] = [
    {
        "model_profile_id": "model_deepseek_v4_pro_primary",
        "provider": "deepseek",
        "model_name": os.getenv("ASSISTANT_DEEPSEEK_MODEL", DEFAULT_DEEPSEEK_MODEL),
        "role": "primary_reasoner",
        "status": "enabled",
        "capabilities_json": {"long_context": True, "reasoning": True, "language": ["zh", "en"]},
        "cost_json": {"tier": "medium"},
        "limits_json": {"writes_long_term_memory": True},
    },
    {
        "model_profile_id": "model_glm_cheap_worker",
        "provider": "glm",
        "model_name": "glm-low-cost",
        "role": "cheap_worker",
        "status": "disabled",
        "capabilities_json": {"summarization": True, "log_analysis": True},
        "cost_json": {"tier": "low"},
        "limits_json": {"writes_long_term_memory": False, "writes_temp_memory": True},
    },
    {
        "model_profile_id": "model_qwen_long_context",
        "provider": "qwen",
        "model_name": "qwen-long-context",
        "role": "long_context",
        "status": "disabled",
        "capabilities_json": {"long_context": True},
        "cost_json": {"tier": "medium"},
        "limits_json": {"writes_long_term_memory": False, "writes_temp_memory": True},
    },
]


DEFAULT_ROUTING_POLICIES: list[dict[str, Any]] = [
    {
        "policy_id": "route_primary_high_risk",
        "role": "primary_reasoner",
        "risk_level": "high",
        "model_profile_id": "model_deepseek_v4_pro_primary",
        "status": "enabled",
        "selector_json": {"requires_primary_review": True},
        "fallback_json": {"allow_fallback": False},
    },
    {
        "policy_id": "route_cheap_low_risk",
        "role": "cheap_worker",
        "risk_level": "low",
        "model_profile_id": "model_glm_cheap_worker",
        "status": "enabled",
        "selector_json": {"task_types": ["log_summary", "progress_summary"]},
        "fallback_json": {"allow_fallback": True, "fallback_profile_id": "model_deepseek_v4_pro_primary"},
    },
    {
        "policy_id": "route_long_context_medium",
        "role": "long_context",
        "risk_level": "medium",
        "model_profile_id": "model_qwen_long_context",
        "status": "enabled",
        "selector_json": {"token_estimate_gte_config_path": "model_routing.long_context_trigger_tokens"},
        "fallback_json": {"allow_fallback": True, "fallback_profile_id": "model_deepseek_v4_pro_primary"},
    },
]


DEFAULT_MEMORY_SEEDS: list[dict[str, Any]] = [
    {
        "memory_id": "mem_architecture_local_data_management_mcp_gateway",
        "memory_type": "architecture",
        "namespace": "aistock",
        "subject_key": "architecture.local_data_management.mcp_gateway",
        "title": "Local data management MCP gateway",
        "content_text": (
            "本地数据管理能力由 aistock-local-data MCP server 通过统一 MCP Gateway 暴露；"
            "MCP 只能经 backend local-data facade 或受控 job/migration 路径访问，不直接写数据库或脚本。"
        ),
        "content_json": {
            "capability_key": "local_data_management",
            "mcp_server": "aistock-local-data",
            "prompt_branch": "prompt.local_data_management",
            "read_only_tools": [
                "local_data_health_overview",
                "local_data_get_dataset_status",
                "local_data_get_preset_daily_status",
                "local_data_list_jobs",
                "local_data_list_sync_targets",
            ],
            "confirmed_tools": ["local_data_apply_repair_confirmed"],
        },
        "source_type": "design_seed",
        "source_ref": "docs/architecture/local_data_management_mcp_gateway_design_20260523.md",
        "confidence": 0.96,
        "approval_status": "approved",
        "risk_level": "medium",
        "evidence_refs": ["docs/architecture/local_data_management_mcp_gateway_design_20260523.md#research-assistant-seed"],
        "created_by": "system_seed",
        "approved_by": "design_seed",
    },
    {
        "memory_id": "mem_process_local_data_check_repair_confirm",
        "memory_type": "procedural",
        "namespace": "aistock",
        "subject_key": "process.local_data.check_repair_confirm",
        "title": "Local data check and repair confirmation flow",
        "content_text": (
            "本地数据检查/修复流程必须先只读检查 readiness、jobs、alerts、sync targets，"
            "再生成 repair plan；用户确认前不得启动同步、刷新、repair apply 或直接写库；执行后必须复查状态。"
        ),
        "content_json": {
            "steps": ["readiness_check", "repair_plan", "confirmation_gate", "confirmed_execution", "post_repair_recheck"],
            "blocked_before_confirmation": ["local_data_apply_repair_confirmed", "local_data_run_dataset_sync_confirmed"],
        },
        "source_type": "design_seed",
        "source_ref": "docs/architecture/local_data_management_mcp_gateway_design_20260523.md",
        "confidence": 0.96,
        "approval_status": "approved",
        "risk_level": "production_sensitive",
        "evidence_refs": ["docs/architecture/local_data_management_mcp_gateway_design_20260523.md#repair-flow"],
        "created_by": "system_seed",
        "approved_by": "design_seed",
    },
]


DEFAULT_GRAPH_ENTITIES: list[dict[str, Any]] = [
    {
        "entity_key": "module.research_assistant",
        "entity_type": "module",
        "title": "Research Assistant",
        "summary": "Assistant orchestration, prompt, memory, graph, and MCP safety layer.",
    },
    {
        "entity_key": "capability.local_data_management",
        "entity_type": "capability",
        "title": "Local data management capability",
        "summary": "Assistant-facing capability for local data health checks, sync-target inspection, and repair planning.",
    },
    {
        "entity_key": "mcp.local_data",
        "entity_type": "mcp_server",
        "title": "aistock-local-data MCP",
        "summary": "MCP server exposing local_data read-only tools and confirmed repair/sync tools.",
    },
    {
        "entity_key": "api.local_data_facade",
        "entity_type": "api",
        "title": "Local data backend facade",
        "summary": "Backend facade used by MCP tools; direct DB/script writes are not part of assistant execution.",
    },
    {
        "entity_key": "process.local_data_check_repair",
        "entity_type": "process",
        "title": "Local data check and repair flow",
        "summary": "Read-only check, repair plan, confirmation, confirmed execution, and post-repair recheck.",
    },
]


DEFAULT_GRAPH_RELATIONS: list[dict[str, Any]] = [
    {
        "relation_key": "research_assistant_uses_local_data_management",
        "source_entity_key": "module.research_assistant",
        "target_entity_key": "capability.local_data_management",
        "relation_type": "uses",
    },
    {
        "relation_key": "local_data_management_exposes_mcp_local_data",
        "source_entity_key": "capability.local_data_management",
        "target_entity_key": "mcp.local_data",
        "relation_type": "exposes",
    },
    {
        "relation_key": "mcp_local_data_wraps_local_data_facade",
        "source_entity_key": "mcp.local_data",
        "target_entity_key": "api.local_data_facade",
        "relation_type": "wraps",
    },
    {
        "relation_key": "local_data_process_uses_mcp_local_data",
        "source_entity_key": "process.local_data_check_repair",
        "target_entity_key": "mcp.local_data",
        "relation_type": "uses",
    },
]


DEFAULT_PROMPT_PACK: PromptPackSnapshot = load_prompt_pack(DEFAULT_PROMPT_PACK_PATH)
DEFAULT_PROMPT_NODES: list[dict[str, Any]] = DEFAULT_PROMPT_PACK.nodes


CATALOG_READINESS_REQUIREMENTS: list[dict[str, Any]] = [
    {
        "catalog": "skills",
        "label": "Skill Catalog",
        "expected_min": len(DEFAULT_SKILLS),
        "filters": {"status": "approved"},
    },
    {
        "catalog": "mcp_servers",
        "label": "MCP Server Catalog",
        "expected_min": len(DEFAULT_MCP_SERVERS),
        "filters": {"status": "ready"},
    },
    {
        "catalog": "mcp_tools",
        "label": "MCP Tool Catalog",
        "expected_min": len(DEFAULT_MCP_TOOLS),
        "filters": {"status": "enabled"},
    },
    {
        "catalog": "capabilities",
        "label": "Capability Registry",
        "expected_min": len(DEFAULT_WORKFLOW_CAPABILITIES),
        "filters": {"status": "approved"},
    },
    {
        "catalog": "model_profiles",
        "label": "Primary Model Profiles",
        "expected_min": 1,
        "filters": {"status": "enabled", "role": "primary_reasoner"},
    },
    {
        "catalog": "routing_policies",
        "label": "Model Routing Policies",
        "expected_min": 1,
        "filters": {"status": "enabled", "role": "primary_reasoner"},
    },
    {
        "catalog": "prompt_nodes",
        "label": "Prompt Tree",
        "expected_min": len(DEFAULT_PROMPT_NODES),
        "filters": {"status": "enabled"},
    },
    {
        "catalog": "prompt_activations",
        "label": "Prompt Pack Activation",
        "expected_min": 1,
        "filters": {"status": "active", "assistant_key": "research_assistant"},
    },
    {
        "catalog": "runtime_config_activations",
        "label": "Runtime Context Config Activation",
        "expected_min": 1,
        "filters": {"status": "active", "config_key": RUNTIME_CONFIG_KEY},
    },
]


@dataclass
class SkillFunctionCall:
    skill_id: str
    skill_key: str
    payload_json: dict[str, Any]
    stable_call_id: str
    function_name: str


@dataclass
class LlmCallResult:
    content: str
    provider: str
    model: str
    duration_ms: int
    usage: dict[str, Any]
    tool_calls: list[McpToolCall] | None = None
    skill_calls: list[SkillFunctionCall] | None = None
    usage_event: dict[str, Any] | None = None


def _litellm_message_content(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _litellm_compatible_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize internal assistant context before sending it to chat providers.

    Research Assistant's ReAct loop uses JSON observations, not provider-native
    tool calls. DeepSeek rejects `role=tool` messages unless they are paired
    with a native `tool_call_id`, so legacy/internal tool observations are
    converted to ordinary context messages.
    """
    normalized: list[dict[str, Any]] = []
    for raw in messages:
        if not isinstance(raw, dict):
            continue
        message = dict(raw)
        role = str(message.get("role") or "user")
        if role == "tool" and not message.get("tool_call_id"):
            normalized.append(
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "type": "INTERNAL_TOOL_OBSERVATION",
                            "instruction": "Use this as audited context. It is not a provider-native tool response.",
                            "content": _litellm_message_content(message.get("content")),
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                }
            )
            continue
        message["role"] = role
        if message.get("content") is None:
            message["content"] = ""
        elif not isinstance(message.get("content"), (str, list)):
            message["content"] = _litellm_message_content(message.get("content"))
        normalized.append(message)
    return normalized


def _attr_or_key(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def _parse_tool_arguments(arguments: Any) -> dict[str, Any]:
    if isinstance(arguments, dict):
        return dict(arguments)
    if arguments in (None, ""):
        return {}
    try:
        parsed = json.loads(str(arguments))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {"value": arguments}
    return dict(parsed) if isinstance(parsed, dict) else {"value": parsed}


_JSON_OBJECT_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.IGNORECASE | re.DOTALL)


def _parse_json_object(text: str) -> dict[str, Any] | None:
    raw = text.strip()
    candidates = [raw]
    candidates.extend(match.group(1).strip() for match in _JSON_OBJECT_FENCE_RE.finditer(text))
    for candidate in candidates:
        try:
            value = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    return None


class FunctionToolRegistry(dict[str, dict[str, Any]]):
    """Registry with MCP-compatible values() for existing call-surface checks."""

    def items(self) -> list[tuple[str, dict[str, Any]]]:  # type: ignore[override]
        return [
            (name, mapping)
            for name, mapping in super().items()
            if str(mapping.get("kind") or "mcp") == "mcp"
        ]

    def values(self) -> list[dict[str, Any]]:  # type: ignore[override]
        return [mapping for mapping in super().values() if str(mapping.get("kind") or "mcp") == "mcp"]

    def skill_values(self) -> list[dict[str, Any]]:
        return [mapping for mapping in super().values() if str(mapping.get("kind") or "mcp") == "skill"]


def _extract_litellm_tool_calls(message: Any, registry: dict[str, dict[str, Any]] | None) -> list[McpToolCall]:
    raw_calls = _attr_or_key(message, "tool_calls", []) or []
    if not isinstance(raw_calls, list):
        return []
    calls: list[McpToolCall] = []
    for index, raw in enumerate(raw_calls):
        function = _attr_or_key(raw, "function", {}) or {}
        function_name = str(_attr_or_key(function, "name", "") or "").strip()
        mapping = (registry or {}).get(function_name)
        if not mapping:
            continue
        if str(mapping.get("kind") or "mcp") != "mcp":
            continue
        server_key = str(mapping.get("server_key") or "").strip()
        tool_name = str(mapping.get("tool_name") or "").strip()
        if not server_key or not tool_name:
            continue
        arguments = _parse_tool_arguments(_attr_or_key(function, "arguments", {}) or {})
        call_id = str(_attr_or_key(raw, "id", "") or f"tool_call_{index:03d}")
        calls.append(
            McpToolCall(
                server_key=server_key,
                tool_name=tool_name,
                payload_json=arguments,
                stable_call_id=call_id,
                reason=f"native_function_call:{function_name}",
            )
        )
    return sorted(calls, key=lambda call: call.sorted_key())


def _extract_litellm_skill_calls(message: Any, registry: dict[str, dict[str, Any]] | None) -> list[SkillFunctionCall]:
    raw_calls = _attr_or_key(message, "tool_calls", []) or []
    if not isinstance(raw_calls, list):
        return []
    calls: list[SkillFunctionCall] = []
    for index, raw in enumerate(raw_calls):
        function = _attr_or_key(raw, "function", {}) or {}
        function_name = str(_attr_or_key(function, "name", "") or "").strip()
        mapping = (registry or {}).get(function_name)
        if not mapping or str(mapping.get("kind") or "mcp") != "skill":
            continue
        skill_id = str(mapping.get("skill_id") or "").strip()
        skill_key = str(mapping.get("skill_key") or skill_id).strip()
        if not skill_id or not skill_key:
            continue
        arguments = _parse_tool_arguments(_attr_or_key(function, "arguments", {}) or {})
        call_id = str(_attr_or_key(raw, "id", "") or f"skill_call_{index:03d}")
        calls.append(
            SkillFunctionCall(
                skill_id=skill_id,
                skill_key=skill_key,
                payload_json=arguments,
                stable_call_id=call_id,
                function_name=function_name,
            )
        )
    return sorted(calls, key=lambda call: (call.skill_key, call.stable_call_id))


def _safe_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _safe_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_safe_jsonable(item) for item in value]
    if hasattr(value, "model_dump"):
        try:
            return _safe_jsonable(value.model_dump())
        except Exception:  # noqa: BLE001 - preserve explicit unsupported object metadata below.
            pass
    if hasattr(value, "dict"):
        try:
            return _safe_jsonable(value.dict())
        except Exception:  # noqa: BLE001 - preserve explicit unsupported object metadata below.
            pass
    attrs = {
        key: getattr(value, key)
        for key in (
            "prompt_tokens",
            "completion_tokens",
            "total_tokens",
            "reasoning_tokens",
            "cache_creation_input_tokens",
            "cache_read_input_tokens",
            "completion_tokens_details",
            "prompt_tokens_details",
        )
        if hasattr(value, key)
    }
    if attrs:
        return _safe_jsonable(attrs)
    return {"unsupported_usage_object_type": type(value).__name__, "repr": repr(value)[:200]}


def _usage_field(usage: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in usage:
            return usage.get(key)
    return None


def _as_nonnegative_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


def _usage_detail_value(usage: dict[str, Any], detail_key: str, *keys: str) -> int | None:
    detail = usage.get(detail_key)
    if isinstance(detail, dict):
        return _as_nonnegative_int(_usage_field(detail, *keys))
    return None


def _response_choice_finish_reason(response: Any) -> str | None:
    try:
        choice = response.choices[0]
    except Exception:
        return None
    return str(getattr(choice, "finish_reason", "") or "").strip() or None


def _build_request_meta(messages: list[dict[str, Any]], tools: list[dict[str, Any]] | None) -> dict[str, Any]:
    return {
        "message_count": len(messages),
        "tool_schema_count": len(tools or []),
        "estimated_input_chars": sum(len(_litellm_message_content(message.get("content"))) for message in messages),
        "prompt_text_retained": False,
    }


def _build_response_meta(
    content: str,
    tool_calls: list[McpToolCall],
    *,
    skill_calls: list[SkillFunctionCall] | None = None,
    finish_reason: str | None = None,
) -> dict[str, Any]:
    return {
        "content_chars": len(content or ""),
        "tool_call_count": len(tool_calls),
        "skill_call_count": len(skill_calls or []),
        "finish_reason": finish_reason,
        "prompt_text_retained": False,
    }


def _estimate_tokens_with_litellm(*, litellm_module: Any, model_id: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]] | None, content: str) -> dict[str, Any]:
    estimates: dict[str, Any] = {"usage_source": "litellm_token_counter_estimated", "usage_status": "estimated", "usage_reason_code": "provider_usage_missing"}
    try:
        prompt_tokens = litellm_module.token_counter(model=model_id, messages=messages, tools=tools or None)
        estimates["prompt_tokens"] = _as_nonnegative_int(prompt_tokens)
        estimates["prompt_tokens_estimated"] = True
    except Exception as exc:  # noqa: BLE001 - expose estimate failure as reason_code.
        estimates["prompt_tokens"] = None
        estimates["prompt_tokens_estimated"] = False
        estimates["usage_status"] = "unavailable"
        estimates["usage_reason_code"] = f"provider_usage_missing_token_counter_failed:{type(exc).__name__}"
    if content:
        try:
            completion_tokens = litellm_module.token_counter(model=model_id, text=content, count_response_tokens=True)
            estimates["completion_tokens"] = _as_nonnegative_int(completion_tokens)
            estimates["completion_tokens_estimated"] = True
        except Exception:
            estimates["completion_tokens"] = None
            estimates["completion_tokens_estimated"] = False
    else:
        estimates["completion_tokens"] = 0
        estimates["completion_tokens_estimated"] = True
    prompt = estimates.get("prompt_tokens")
    completion = estimates.get("completion_tokens")
    estimates["total_tokens"] = prompt + completion if isinstance(prompt, int) and isinstance(completion, int) else None
    return estimates


def _normalize_litellm_usage(
    usage_raw: Any,
    *,
    litellm_module: Any | None,
    model_id: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None,
    content: str,
) -> dict[str, Any]:
    usage_json = _safe_jsonable(usage_raw)
    usage = usage_json if isinstance(usage_json, dict) else {}
    prompt_tokens = _as_nonnegative_int(_usage_field(usage, "prompt_tokens", "input_tokens"))
    completion_tokens = _as_nonnegative_int(_usage_field(usage, "completion_tokens", "output_tokens"))
    total_tokens = _as_nonnegative_int(_usage_field(usage, "total_tokens"))
    if total_tokens is None and prompt_tokens is not None and completion_tokens is not None:
        total_tokens = prompt_tokens + completion_tokens
    reasoning_tokens = _as_nonnegative_int(_usage_field(usage, "reasoning_tokens"))
    if reasoning_tokens is None:
        reasoning_tokens = _usage_detail_value(usage, "completion_tokens_details", "reasoning_tokens")
    cache_creation = _as_nonnegative_int(_usage_field(usage, "cache_creation_input_tokens"))
    if cache_creation is None:
        cache_creation = _usage_detail_value(usage, "prompt_tokens_details", "cache_creation_input_tokens")
    cache_read = _as_nonnegative_int(_usage_field(usage, "cache_read_input_tokens"))
    if cache_read is None:
        cache_read = _usage_detail_value(usage, "prompt_tokens_details", "cache_read_input_tokens", "cached_tokens")
    has_tokens = any(value is not None for value in (prompt_tokens, completion_tokens, total_tokens, reasoning_tokens, cache_creation, cache_read))
    if has_tokens:
        usage_source = "provider_reported" if isinstance(usage_raw, dict) else "litellm_usage_object"
        return {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
            "reasoning_tokens": reasoning_tokens,
            "cache_creation_input_tokens": cache_creation,
            "cache_read_input_tokens": cache_read,
            "prompt_tokens_estimated": False,
            "completion_tokens_estimated": False,
            "usage_source": usage_source,
            "usage_status": "recorded",
            "usage_reason_code": None,
            "usage_raw_json": usage,
        }
    if litellm_module is not None:
        estimated = _estimate_tokens_with_litellm(litellm_module=litellm_module, model_id=model_id, messages=messages, tools=tools, content=content)
        estimated.update(
            {
                "reasoning_tokens": None,
                "cache_creation_input_tokens": None,
                "cache_read_input_tokens": None,
                "usage_raw_json": usage if usage else {"usage_missing": True, "raw_type": type(usage_raw).__name__},
            }
        )
        return estimated
    return {
        "prompt_tokens": None,
        "completion_tokens": None,
        "total_tokens": None,
        "reasoning_tokens": None,
        "cache_creation_input_tokens": None,
        "cache_read_input_tokens": None,
        "prompt_tokens_estimated": False,
        "completion_tokens_estimated": False,
        "usage_source": "unavailable",
        "usage_status": "unavailable",
        "usage_reason_code": "provider_usage_missing_litellm_unavailable",
        "usage_raw_json": usage if usage else {"usage_missing": True, "raw_type": type(usage_raw).__name__},
    }


def _calculate_litellm_cost(
    *,
    litellm_module: Any | None,
    response: Any | None,
    model_id: str,
    normalized_usage: dict[str, Any],
    duration_ms: int,
) -> dict[str, Any]:
    if litellm_module is None:
        return {"cost_source": "unavailable", "cost_status": "unavailable", "cost_reason_code": "litellm_unavailable", "pricing_snapshot_json": {}}
    prompt_tokens = int(normalized_usage.get("prompt_tokens") or 0)
    completion_tokens = int(normalized_usage.get("completion_tokens") or 0)
    if prompt_tokens == 0 and completion_tokens == 0:
        return {"cost_source": "unavailable", "cost_status": "unavailable", "cost_reason_code": "usage_tokens_unavailable", "pricing_snapshot_json": {"model": model_id}}
    try:
        prompt_cost, completion_cost = litellm_module.cost_per_token(
            model=model_id,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            response_time_ms=float(duration_ms),
            usage_object=normalized_usage.get("usage_raw_json") if isinstance(normalized_usage.get("usage_raw_json"), dict) else None,
        )
        total_cost = float(prompt_cost or 0) + float(completion_cost or 0)
        return {
            "prompt_cost_usd": prompt_cost,
            "completion_cost_usd": completion_cost,
            "total_cost_usd": total_cost,
            "currency": "USD",
            "cost_source": "litellm_model_cost",
            "cost_status": "estimated" if normalized_usage.get("usage_status") == "estimated" else "recorded",
            "cost_reason_code": None,
            "pricing_snapshot_json": {"model": model_id, "source": "litellm.cost_per_token"},
        }
    except Exception as cost_exc:
        reason = f"litellm_cost_per_token_failed:{type(cost_exc).__name__}"
        try:
            total_cost = litellm_module.completion_cost(completion_response=response, model=model_id) if response is not None else None
            return {
                "prompt_cost_usd": None,
                "completion_cost_usd": None,
                "total_cost_usd": total_cost,
                "currency": "USD",
                "cost_source": "litellm_model_cost",
                "cost_status": "estimated" if normalized_usage.get("usage_status") == "estimated" else "recorded",
                "cost_reason_code": None,
                "pricing_snapshot_json": {"model": model_id, "source": "litellm.completion_cost", "split_cost_unavailable_reason": reason},
            }
        except Exception as fallback_exc:  # noqa: BLE001 - record concrete no-silent cost failure.
            return {
                "prompt_cost_usd": None,
                "completion_cost_usd": None,
                "total_cost_usd": None,
                "currency": "USD",
                "cost_source": "unavailable",
                "cost_status": "unavailable",
                "cost_reason_code": f"{reason};completion_cost_failed:{type(fallback_exc).__name__}",
                "pricing_snapshot_json": {"model": model_id, "source": "litellm", "error": str(fallback_exc)[:200]},
            }


def _llm_usage_summary_dict(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "prompt_tokens": payload.get("prompt_tokens"),
        "completion_tokens": payload.get("completion_tokens"),
        "total_tokens": payload.get("total_tokens"),
        "reasoning_tokens": payload.get("reasoning_tokens"),
        "cache_creation_input_tokens": payload.get("cache_creation_input_tokens"),
        "cache_read_input_tokens": payload.get("cache_read_input_tokens"),
        "prompt_tokens_estimated": bool(payload.get("prompt_tokens_estimated")),
        "completion_tokens_estimated": bool(payload.get("completion_tokens_estimated")),
        "usage_source": payload.get("usage_source"),
        "usage_status": payload.get("usage_status"),
        "usage_reason_code": payload.get("usage_reason_code"),
        "prompt_cost_usd": payload.get("prompt_cost_usd"),
        "completion_cost_usd": payload.get("completion_cost_usd"),
        "total_cost_usd": payload.get("total_cost_usd"),
        "currency": payload.get("currency") or "USD",
        "cost_source": payload.get("cost_source"),
        "cost_status": payload.get("cost_status"),
        "cost_reason_code": payload.get("cost_reason_code"),
    }




def _default_workflow_capabilities() -> list[dict[str, Any]]:
    return DEFAULT_WORKFLOW_CAPABILITIES


def _code_context_ref_from_row(row: dict[str, Any]) -> dict[str, Any]:
    manifest = row.get("manifest_json") if isinstance(row.get("manifest_json"), dict) else {}
    provenance = row.get("provenance_json") if isinstance(row.get("provenance_json"), dict) else {}
    affected_tests = manifest.get("affected_tests") if isinstance(manifest.get("affected_tests"), list) else []
    query_scope = str(row.get("query_scope") or manifest.get("query_scope") or "")
    as_of = row.get("as_of")
    as_of_text = as_of.isoformat() if hasattr(as_of, "isoformat") else as_of
    summary_tests = ", ".join(map(str, affected_tests[:3])) if affected_tests else "no affected-test suggestion"
    return {
        "code_ref_id": row.get("code_ref_id"),
        "query_scope": query_scope,
        "query_scope_type": query_scope.split(":", 1)[0] if ":" in query_scope else "unknown",
        "source": row.get("source") or "codegraph",
        "summary": f"Cached code context scoped to {query_scope}; affected tests: {summary_tests}.",
        "provenance": provenance,
        "as_of": as_of_text,
        "manifest_json": manifest,
        "context_artifact_ref": manifest.get("context_artifact_ref"),
        "manifest_artifact_ref": manifest.get("manifest_artifact_ref"),
        "affected_tests_ref": manifest.get("affected_tests_ref"),
        "affected_tests": [str(item) for item in affected_tests],
    }


class ResearchAssistantLlmClient:
    """Small LiteLLM wrapper for assistant chat turns.

    Tests inject a fake client. Production calls fail fast if litellm or model
    credentials are missing; there is no canned success fallback.
    """

    def complete(
        self,
        *,
        messages: list[dict[str, str]],
        model_profile: dict[str, Any],
        temperature: float,
        max_tokens: int | None,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        tool_registry: dict[str, dict[str, Any]] | None = None,
    ) -> LlmCallResult:
        provider = str(model_profile.get("provider") or "").strip()
        model_name = str(model_profile.get("model_name") or "").strip()
        if not provider or not model_name:
            raise RuntimeError("assistant LLM model profile is incomplete")
        completion_kwargs: dict[str, Any] = {}
        if provider == "deepseek":
            resolved = resolve_deepseek_config(model=model_name)
            model_id = resolved.model if "/" in resolved.model else f"deepseek/{resolved.model}"
            completion_kwargs["api_key"] = resolved.api_key
            completion_kwargs["api_base"] = resolved.base_url
        else:
            env_key = f"{provider.upper()}_API_KEY"
            if not os.getenv(env_key):
                raise RuntimeError(f"{env_key} is not configured for Research Assistant LLM calls")
            model_id = model_name if "/" in model_name else f"{provider}/{model_name}"
        try:
            import litellm
        except Exception as exc:  # pragma: no cover
            raise RuntimeError("litellm is not installed; Research Assistant cannot call LLM") from exc
        start = perf_counter()
        provider_messages = _litellm_compatible_messages(messages)
        if tools:
            completion_kwargs["tools"] = tools
            completion_kwargs["tool_choice"] = tool_choice or "auto"
        if max_tokens is not None:
            completion_kwargs["max_tokens"] = max_tokens
        response = litellm.completion(model=model_id, messages=provider_messages, temperature=temperature, **completion_kwargs)
        duration_ms = int((perf_counter() - start) * 1000)
        choice = response.choices[0]
        message = choice.message
        content = str(message.content or "").strip()
        tool_calls = _extract_litellm_tool_calls(message, tool_registry)
        skill_calls = _extract_litellm_skill_calls(message, tool_registry)
        usage_raw = getattr(response, "usage", None)
        finish_reason = str(getattr(choice, "finish_reason", "") or "").strip().lower()
        normalized_usage = _normalize_litellm_usage(
            usage_raw,
            litellm_module=litellm,
            model_id=model_id,
            messages=provider_messages,
            tools=tools,
            content=content,
        )
        cost = _calculate_litellm_cost(
            litellm_module=litellm,
            response=response,
            model_id=model_id,
            normalized_usage=normalized_usage,
            duration_ms=duration_ms,
        )
        usage_event = {
            **normalized_usage,
            **cost,
            "provider": provider,
            "model": model_id,
            "litellm_model": model_id,
            "duration_ms": duration_ms,
            "request_meta_json": _build_request_meta(provider_messages, tools),
            "response_meta_json": _build_response_meta(content, tool_calls, skill_calls=skill_calls, finish_reason=finish_reason or None),
        }
        usage = _llm_usage_summary_dict(usage_event)
        if finish_reason == "length":
            raise RuntimeError(
                "llm_completion_truncated: provider returned finish_reason=length; "
                "Research Assistant refuses to return a silent mid-sentence partial answer"
            )
        if not content and not tool_calls and not skill_calls:
            raise RuntimeError("assistant LLM returned empty content")
        return LlmCallResult(
            content=content,
            provider=provider,
            model=model_id,
            duration_ms=duration_ms,
            usage=usage,
            usage_event=usage_event,
            tool_calls=tool_calls,
            skill_calls=skill_calls,
        )

    def complete_tool_plan(self, *, messages: list[dict[str, str]], model_profile: dict[str, Any], temperature: float, max_tokens: int | None) -> LlmCallResult:
        return self.complete(messages=messages, model_profile=model_profile, temperature=temperature, max_tokens=max_tokens)

    def complete_memory_curation(self, *, messages: list[dict[str, str]], model_profile: dict[str, Any], temperature: float, max_tokens: int | None) -> LlmCallResult:
        return self.complete(messages=messages, model_profile=model_profile, temperature=temperature, max_tokens=max_tokens)


class ResearchAssistantService(ResearchAssistantExecutionMixin):
    PROGRAM_ERROR_REASON_CODES = {
        "capability_not_found",
        "tool_not_in_audited_catalog",
        "tool_execution_error",
        "data_source_unavailable",
        "tool_result_compaction_error",
        "chat_turn_unexpected_error",
    }
    CONFIG_ERROR_REASON_CODES = {
        "capability_registry_invalid_mcp_tool_refs",
        "capability_registry_invalid_skill_refs",
        "capability_registry_repair_failed",
        "runtime_config_invalid_capability_mcp_tool_refs",
        "runtime_config_invalid_workflow_capability",
        "runtime_config_invalid_active_config",
        "declarative_config_invalid_capability_mcp_tool_refs",
        "declarative_config_invalid_workflow_capability",
        "declarative_config_invalid_runtime_context",
        "declarative_config_invalid_prompt_pack",
    }

    @staticmethod
    def default_workflow_capabilities() -> list[dict[str, Any]]:
        return _default_workflow_capabilities()

    def __init__(
        self,
        repository: Any | None = None,
        llm_client: Any | None = None,
        *,
        environment: str = DEFAULT_ENVIRONMENT,
        issue_fact_source: Any | None = None,
        runtime_config_path: Path | None = None,
        prompt_pack_path: Path | None = None,
    ) -> None:
        self.repository = repository or DatabaseResearchAssistantRepository()
        self.llm_client = llm_client or ResearchAssistantLlmClient()
        self.semantic_tool_planner = SemanticToolPlanner(self.llm_client)
        self.environment = environment
        self.context_budget_planner = ContextBudgetPlanner()
        self.issue_fact_source = issue_fact_source or IssueCandidateFactSource()
        self._declarative_config_lock = threading.RLock()
        self._runtime_config_path = runtime_config_path
        self._prompt_pack_path = prompt_pack_path
        try:
            self._declarative_config = load_declarative_config(
                environment=environment,
                runtime_config_path=runtime_config_path,
                prompt_pack_path=prompt_pack_path,
            )
        except ResearchAssistantDeclarativeConfigError as exc:
            raise ResearchAssistantRuntimeConfigInvalidError(exc.error_payload) from exc

    def reload_declarative_config(
        self,
        *,
        runtime_config_path: Path | None = None,
        prompt_pack_path: Path | None = None,
    ) -> dict[str, Any]:
        next_runtime_config_path = runtime_config_path if runtime_config_path is not None else self._runtime_config_path
        next_prompt_pack_path = prompt_pack_path if prompt_pack_path is not None else self._prompt_pack_path
        try:
            snapshot = load_declarative_config(
                environment=self.environment,
                runtime_config_path=next_runtime_config_path,
                prompt_pack_path=next_prompt_pack_path,
            )
        except ResearchAssistantDeclarativeConfigError as exc:
            raise ResearchAssistantRuntimeConfigInvalidError(exc.error_payload) from exc
        with self._declarative_config_lock:
            self._runtime_config_path = next_runtime_config_path
            self._prompt_pack_path = next_prompt_pack_path
            self._declarative_config = snapshot
        logger.warning(
            "research assistant declarative config reloaded: runtime_source=%s prompt_source=%s",
            snapshot.runtime_config.source_path,
            snapshot.prompt_pack.source_path,
        )
        return self.declarative_config_status()

    def reload_declarative_config_with_audit(
        self,
        *,
        actor: str,
        runtime_config_path: Path | None = None,
        prompt_pack_path: Path | None = None,
    ) -> dict[str, Any]:
        started = perf_counter()
        old_status = self.declarative_config_status()
        try:
            new_status = self.reload_declarative_config(
                runtime_config_path=runtime_config_path,
                prompt_pack_path=prompt_pack_path,
            )
        except ResearchAssistantRuntimeConfigInvalidError as exc:
            duration_ms = int((perf_counter() - started) * 1000)
            payload = self._declarative_config_reload_audit_payload(
                actor=actor,
                success=False,
                old_status=old_status,
                new_status=self.declarative_config_status(),
                duration_ms=duration_ms,
                error_payload=exc.error_payload,
            )
            trace = self.create_trace_event(
                TraceEventCreate(
                    event_type="declarative_config_reloaded",
                    component="declarative_config",
                    status="failed",
                    duration_ms=duration_ms,
                    payload_json=payload,
                )
            )
            logger.error(
                "research assistant declarative config reload failed: actor=%s reason_code=%s source_path=%s trace_id=%s",
                actor,
                exc.error_payload.get("reason_code"),
                exc.error_payload.get("source_path"),
                trace.get("trace_id"),
            )
            exc.error_payload.setdefault("audit_trace_id", trace.get("trace_id"))
            exc.error_payload.setdefault("last_good_source_sha256", old_status.get("source_sha256"))
            raise
        duration_ms = int((perf_counter() - started) * 1000)
        payload = self._declarative_config_reload_audit_payload(
            actor=actor,
            success=True,
            old_status=old_status,
            new_status=new_status,
            duration_ms=duration_ms,
        )
        trace = self.create_trace_event(
            TraceEventCreate(
                event_type="declarative_config_reloaded",
                component="declarative_config",
                status="succeeded",
                duration_ms=duration_ms,
                payload_json=payload,
            )
        )
        return {
            "schema_version": "aistock_research_assistant_config_reload_result_v1",
            "status": "succeeded",
            "actor": actor,
            "declarative_config_status": new_status,
            "old_declarative_config_status": old_status,
            "audit_trace_id": trace.get("trace_id"),
            "multi_worker_notice": self._declarative_config_reload_multi_worker_notice(),
        }

    @staticmethod
    def _declarative_config_reload_multi_worker_notice() -> str:
        return (
            "Reload only updates the Research Assistant declarative config snapshot in the current worker process; "
            "multi-worker deployments must call this endpoint once per worker or restart workers to activate the same YAML everywhere."
        )

    def _declarative_config_reload_audit_payload(
        self,
        *,
        actor: str,
        success: bool,
        old_status: dict[str, Any],
        new_status: dict[str, Any],
        duration_ms: int,
        error_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "schema_version": "aistock_research_assistant_config_reload_audit_v1",
            "actor": actor,
            "success": success,
            "status": "succeeded" if success else "failed",
            "duration_ms": duration_ms,
            "old_source_sha256": old_status.get("source_sha256"),
            "new_source_sha256": new_status.get("source_sha256"),
            "old_runtime_source_sha256": (old_status.get("runtime_config") or {}).get("source_sha256"),
            "new_runtime_source_sha256": (new_status.get("runtime_config") or {}).get("source_sha256"),
            "old_prompt_source_sha256": (old_status.get("prompt_pack") or {}).get("source_sha256"),
            "new_prompt_source_sha256": (new_status.get("prompt_pack") or {}).get("source_sha256"),
            "old_runtime_config_version": (old_status.get("runtime_config") or {}).get("config_version"),
            "new_runtime_config_version": (new_status.get("runtime_config") or {}).get("config_version"),
            "old_prompt_pack_version": (old_status.get("prompt_pack") or {}).get("pack_version"),
            "new_prompt_pack_version": (new_status.get("prompt_pack") or {}).get("pack_version"),
            "old_counts": old_status.get("counts") or {},
            "new_counts": new_status.get("counts") or {},
            "old_status": old_status,
            "new_status": new_status,
            "error": dict(error_payload or {}),
            "multi_worker_notice": self._declarative_config_reload_multi_worker_notice(),
        }

    @property
    def declarative_config(self) -> ResearchAssistantDeclarativeConfigSnapshot:
        with self._declarative_config_lock:
            return self._declarative_config

    def declarative_config_status(self) -> dict[str, Any]:
        snapshot = self.declarative_config
        source_sha256 = sha256_json(
            {
                "runtime_config": snapshot.runtime_config.source_sha256,
                "prompt_pack": snapshot.prompt_pack.source_sha256,
            }
        )
        return {
            "schema_version": "aistock_research_assistant_declarative_config_status_v1",
            "authority": "yaml_memory",
            "source_path": {
                "runtime_config": snapshot.runtime_config.source_path,
                "prompt_pack": snapshot.prompt_pack.source_path,
            },
            "source_sha256": source_sha256,
            "runtime_config": {
                "config_key": snapshot.runtime_config.config_key,
                "config_version": snapshot.runtime_config.config_version,
                "source_path": snapshot.runtime_config.source_path,
                "source_sha256": snapshot.runtime_config.source_sha256,
            },
            "prompt_pack": {
                "pack_key": snapshot.prompt_pack.pack_key,
                "pack_version": snapshot.prompt_pack.pack_version,
                "source_path": snapshot.prompt_pack.source_path,
                "source_sha256": snapshot.prompt_pack.source_sha256,
            },
            "workflow_capability_count": len(snapshot.workflow_capabilities),
            "prompt_node_count": len(snapshot.prompt_nodes),
            "counts": {
                "workflow_capabilities": len(snapshot.workflow_capabilities),
                "prompt_nodes": len(snapshot.prompt_nodes),
            },
        }

    @staticmethod
    def _exception_reason_code(exc: BaseException, *, server_key: str = "", tool_name: str = "") -> str:
        message = str(exc)
        lowered = message.lower()
        if isinstance(exc, KeyError) and "approved capability not found for tool" in lowered:
            return "capability_not_found"
        unavailable_terms = (
            "connection refused",
            "refused",
            "connection reset",
            "timed out",
            "timeout",
            "connection",
            "unavailable",
            "offline",
            "database is locked",
            "could not connect",
        )
        if (server_key == "aistock-local-data" or tool_name.startswith("local_data_")) and any(term in lowered for term in unavailable_terms):
            return "data_source_unavailable"
        return "tool_execution_error"

    @classmethod
    def _tool_error_payload(cls, call: McpToolCall, exc: BaseException, *, stage: str) -> dict[str, Any]:
        reason_code = cls._exception_reason_code(exc, server_key=call.server_key, tool_name=call.tool_name)
        return {
            "reason_code": reason_code,
            "code": reason_code,
            "stage": stage,
            "server_key": call.server_key,
            "tool_name": call.tool_name,
            "exception_type": type(exc).__name__,
            "message": str(exc),
        }

    @classmethod
    def _normalize_tool_error_payload(
        cls,
        call: McpToolCall,
        error: dict[str, Any],
        *,
        stage: str,
    ) -> dict[str, Any]:
        raw_code = str(error.get("reason_code") or error.get("code") or "")
        raw_message = str(error.get("human_reason") or error.get("message") or error.get("error_summary") or raw_code)
        reason_code = raw_code
        if not reason_code or reason_code in {"execution_failed", "local_data_daily_status_read_failed"}:
            synthetic = RuntimeError(raw_message)
            reason_code = cls._exception_reason_code(synthetic, server_key=call.server_key, tool_name=call.tool_name)
        return {
            "reason_code": reason_code or "tool_execution_error",
            "code": reason_code or "tool_execution_error",
            "stage": stage,
            "server_key": call.server_key,
            "tool_name": call.tool_name,
            "exception_type": str(error.get("exception_type") or error.get("error_type") or raw_code or "ToolExecutionError"),
            "message": raw_message,
            "retryable": bool(error.get("retryable", False)),
            "audit_link": error.get("audit_link"),
            "next_step": error.get("next_step"),
        }

    @staticmethod
    def _render_tool_error_reply(error: dict[str, Any]) -> str:
        route = f"{error.get('server_key')}/{error.get('tool_name')}"
        catalog_reason = str(error.get("catalog_reason") or "")
        catalog_fragment = f"catalog_reason={catalog_reason}; " if catalog_reason else ""
        return (
            "工具调用失败："
            f"reason_code={error.get('reason_code') or error.get('code')}; "
            f"{catalog_fragment}"
            f"tool={route}; "
            f"exception_type={error.get('exception_type') or 'Error'}; "
            f"error_summary={error.get('message') or error.get('human_reason') or ''}"
        ).strip()

    @staticmethod
    def _render_chat_turn_error_reply(error: dict[str, Any]) -> str:
        return (
            "对话轮处理失败："
            f"reason_code={error.get('reason_code') or error.get('code')}; "
            f"stage={error.get('stage') or 'chat_turn'}; "
            f"exception_type={error.get('exception_type') or 'Error'}; "
            f"error_summary={error.get('message') or error.get('human_reason') or ''}"
        ).strip()

    @staticmethod
    def _render_runtime_config_error_reply(error: dict[str, Any]) -> str:
        return (
            "Research Assistant runtime config 校验失败，已 fail-closed 停止本轮对话："
            f"reason_code={error.get('reason_code') or error.get('code')}; "
            f"stage={error.get('stage') or 'runtime_config_validation'}; "
            f"activation_id={error.get('activation_id')}; "
            f"config_key={error.get('config_key')}; "
            f"capability_key={error.get('capability_key')}; "
            f"field={error.get('field')}; "
            f"actual_type={error.get('actual_type')}; "
            f"operator_action={error.get('operator_action') or ResearchAssistantService._runtime_config_operator_action()}"
        ).strip()

    def _mcp_tool_failure_result(
        self,
        call: McpToolCall,
        exc: BaseException,
        *,
        stage: str,
        cards: dict[str, Any] | None = None,
        task_id: str | None = None,
    ) -> McpToolResult:
        error = self._tool_error_payload(call, exc, stage=stage)
        logger.exception(
            "research assistant MCP tool failed: reason_code=%s tool=%s/%s stage=%s",
            error["reason_code"],
            call.server_key,
            call.tool_name,
            stage,
        )
        result = McpToolResult(
            server_key=call.server_key,
            tool_name=call.tool_name,
            status="failed",
            summary=self._render_tool_error_reply(error),
            error_json=error,
            executed=False,
            blocked_reason=str(error["reason_code"]),
            stable_call_id=call.stable_call_id,
            side_effect_level=str(call.side_effect_level or "read_only"),
        )
        if isinstance(cards, dict):
            cards["mcp_execution_result"] = {
                "auto_executed": False,
                "executed": False,
                "status": "failed",
                "route": f"{call.server_key}/{call.tool_name}",
                "server_key": call.server_key,
                "tool_name": call.tool_name,
                "summary_first": True,
                "error": error,
            }
            cards.setdefault("tool_errors", [])
            if isinstance(cards["tool_errors"], list):
                cards["tool_errors"].append(error)
        if task_id:
            try:
                self.add_task_event(
                    task_id,
                    TaskEventCreate(
                        event_type="mcp_failed",
                        severity="error",
                        message=self._render_tool_error_reply(error),
                        payload_json={"error": error, "route": f"{call.server_key}/{call.tool_name}"},
                    ),
                )
            except Exception:  # noqa: BLE001 - error-card creation must not re-crash chat/turn.
                logger.exception("failed to persist MCP tool failure event for %s/%s", call.server_key, call.tool_name)
        return result

    @classmethod
    def _tool_error_from_cards(cls, cards: dict[str, Any]) -> dict[str, Any] | None:
        candidates: list[dict[str, Any]] = []
        react = cards.get("react_grounding") if isinstance(cards.get("react_grounding"), dict) else {}
        evidence_guard = react.get("evidence_guard") if isinstance(react.get("evidence_guard"), dict) else {}
        react_guard_reason = str(evidence_guard.get("reason") or "")
        react_grounding_allowed = bool(
            evidence_guard.get("allowed")
            and (
                react_guard_reason == "ok"
                or react_guard_reason.startswith("guard_disabled")
                or react_guard_reason.startswith("annotated:")
            )
        )
        if react_grounding_allowed:
            return None
        for item in react.get("tool_errors") or []:
            if isinstance(item, dict):
                if item.get("terminal_program_error") is False:
                    continue
                candidates.append(item)
        execution = cards.get("mcp_execution_result") if isinstance(cards.get("mcp_execution_result"), dict) else {}
        if isinstance(execution.get("error"), dict):
            candidates.append(execution["error"])
        for item in candidates:
            reason_code = str(item.get("reason_code") or item.get("code") or "")
            if reason_code in cls.PROGRAM_ERROR_REASON_CODES:
                return item
        return None

    @classmethod
    def _chat_turn_unexpected_error_payload(cls, exc: BaseException) -> dict[str, Any]:
        message = str(exc)
        reason_code = "llm_completion_truncated" if message.startswith("llm_completion_truncated") else "chat_turn_unexpected_error"
        stage = "llm_completion" if reason_code == "llm_completion_truncated" else "chat_turn"
        return {
            "reason_code": reason_code,
            "code": reason_code,
            "stage": stage,
            "server_key": None,
            "tool_name": None,
            "exception_type": type(exc).__name__,
            "message": message,
        }

    @staticmethod
    def _runtime_config_operator_action() -> str:
        return "fix configs/research_assistant/runtime_context.yaml and reload/restart Research Assistant"

    @staticmethod
    def _capability_registry_operator_action() -> str:
        return "run RA capability sync or apply backend/db/migrations/ra_upgrade/010_repair_capability_registry_mcp_tool_refs.sql, then restart Research Assistant"

    @staticmethod
    def _runtime_config_activation_metadata(activation: dict[str, Any]) -> dict[str, Any]:
        return {
            "activation_id": str(activation.get("activation_id") or ""),
            "config_key": str(activation.get("config_key") or RUNTIME_CONFIG_KEY),
            "config_version": activation.get("config_version"),
            "source_id": activation.get("source_id"),
        }

    @staticmethod
    def _payload_for_runtime_config_error(exc: RuntimeConfigCapabilityValidationError, activation: dict[str, Any]) -> dict[str, Any]:
        reason_code = (
            "runtime_config_invalid_capability_mcp_tool_refs"
            if exc.field == "mcp_tool_refs"
            else "runtime_config_invalid_workflow_capability"
        )
        metadata = ResearchAssistantService._runtime_config_activation_metadata(activation)
        activation_id = str(metadata["activation_id"])
        config_key = str(metadata["config_key"])
        message = (
            f"active RA runtime config is invalid: activation_id={activation_id}; "
            f"config_key={config_key}; capability_index={exc.index}; "
            f"capability_key={exc.capability_key}; field={exc.field}; "
            f"actual_type={exc.actual_type}; operator_action={ResearchAssistantService._runtime_config_operator_action()}"
        )
        if exc.entry_index is not None:
            message = (
                f"active RA runtime config is invalid: activation_id={activation_id}; "
                f"config_key={config_key}; capability_index={exc.index}; "
                f"capability_key={exc.capability_key}; field={exc.field}; "
                f"entry_index={exc.entry_index}; actual_type={exc.actual_type}; "
                f"operator_action={ResearchAssistantService._runtime_config_operator_action()}"
            )
        return {
            "reason_code": reason_code,
            "code": reason_code,
            "stage": "runtime_config_validation",
            **metadata,
            "capability_index": exc.index,
            "capability_key": exc.capability_key,
            "field": exc.field,
            "entry_index": exc.entry_index,
            "actual_type": exc.actual_type,
            "exception_type": type(exc).__name__,
            "message": message,
            "operator_action": ResearchAssistantService._runtime_config_operator_action(),
        }

    @classmethod
    def _payload_for_capability_registry_mcp_refs_error(
        cls,
        *,
        capability_key: str,
        field: str = "mcp_tool_refs",
        actual_type: str,
        detail: str,
        source: str,
        entry_index: int | None = None,
        exception_type: str = "TypeError",
    ) -> dict[str, Any]:
        reason_code = f"capability_registry_invalid_{field}"
        message = (
            f"capability registry is invalid: source={source}; capability_key={capability_key}; "
            f"field={field}; actual_type={actual_type}; detail={detail}; "
            f"operator_action={cls._capability_registry_operator_action()}"
        )
        if entry_index is not None:
            message = (
                f"capability registry is invalid: source={source}; capability_key={capability_key}; "
                f"field={field}; entry_index={entry_index}; actual_type={actual_type}; detail={detail}; "
                f"operator_action={cls._capability_registry_operator_action()}"
            )
        return {
            "reason_code": reason_code,
            "code": reason_code,
            "stage": "capability_registry_validation",
            "activation_id": None,
            "config_key": "assistant_capabilities",
            "config_version": None,
            "source_id": source,
            "capability_key": capability_key,
            "field": field,
            "entry_index": entry_index,
            "actual_type": actual_type,
            "exception_type": exception_type,
            "message": message,
            "operator_action": cls._capability_registry_operator_action(),
        }

    @classmethod
    def _payload_for_capability_registry_repair_error(
        cls,
        *,
        capability_key: str,
        capability_id: str,
        exc: BaseException,
    ) -> dict[str, Any]:
        reason_code = "capability_registry_repair_failed"
        return {
            "reason_code": reason_code,
            "code": reason_code,
            "stage": "capability_registry_validation",
            "activation_id": None,
            "config_key": "assistant_capabilities",
            "config_version": None,
            "source_id": "assistant_capabilities",
            "capability_key": capability_key,
            "capability_id": capability_id,
            "field": "mcp_tool_refs",
            "actual_type": type(exc).__name__,
            "exception_type": type(exc).__name__,
            "message": (
                f"capability registry empty mcp_tool_refs repair failed: capability_key={capability_key}; "
                f"capability_id={capability_id}; error={exc}; operator_action={cls._capability_registry_operator_action()}"
            ),
            "operator_action": cls._capability_registry_operator_action(),
        }

    @classmethod
    def _chat_turn_config_error_payload(cls, exc: ResearchAssistantRuntimeConfigInvalidError) -> dict[str, Any]:
        error = dict(exc.error_payload)
        reason_code = str(error.get("reason_code") or "runtime_config_invalid_active_config")
        error.update(
            {
                "reason_code": reason_code,
                "code": reason_code,
                "stage": error.get("stage") or "runtime_config_validation",
                "server_key": None,
                "tool_name": None,
                "exception_type": error.get("exception_type") or type(exc).__name__,
                "message": error.get("message") or str(exc),
                "operator_action": error.get("operator_action") or cls._runtime_config_operator_action(),
            }
        )
        return error

    @staticmethod
    def _fallback_task_events_detail_limit_for_config_error() -> int:
        return 100


    def run_agent_team(
        self,
        *,
        parent_task_id: str,
        objective: str,
        requested_agent_keys: list[str] | None = None,
        worker_inputs: dict[str, dict[str, object]] | None = None,
        qe_autonomy_request: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        config = load_agent_teams_config(REPO_ROOT / "configs/research_assistant/agent_teams.yaml")
        code_team_context = build_query_code_context(
            user_query=objective,
            task_id=parent_task_id,
            repo_root=REPO_ROOT,
            token_budget=1800,
            cache_lookup=self._lookup_code_context_cache,
        )
        code_team_refs = list(code_team_context.get("code_context_refs") or [])
        runtime = AgentTeamsRuntime(
            config=config,
            providers=AgentTeamsRuntimeProviders(
                run_store=_ServiceAgentRunStore(self),
                context_provider=_ServiceAgentContextProvider(self),
                worker_executor=_ServiceAgentWorkerExecutor(self, user_message=objective),
                tool_catalog_provider=_ServiceAgentCatalogProvider(self),
                curator=_ServiceAgentCurator(),
            ),
            id_factory=lambda task: new_id(f"aar_{task.task_order:03d}_{task.agent_key}"),
        )
        merged_worker_inputs: dict[str, dict[str, object]] = {key: dict(value) for key, value in (worker_inputs or {}).items()}
        if qe_autonomy_request is not None:
            merged_worker_inputs.setdefault("qe_experiment_designer", {})["qe_autonomy_request"] = dict(qe_autonomy_request)
        if code_team_refs:
            self._persist_code_context_refs(task_id=parent_task_id, refs=code_team_refs)
            affected_tests = sorted(
                {
                    str(test)
                    for ref in code_team_refs
                    for test in (ref.get("affected_tests") if isinstance(ref, dict) else []) or []
                }
            )
            for agent in config.workers:
                worker_input = merged_worker_inputs.setdefault(agent.agent_key, {})
                worker_input["code_context_refs"] = code_team_refs
                worker_input["code_affected_tests"] = affected_tests
                worker_input["code_context_reason_codes"] = list(code_team_context.get("reason_codes") or [])
        result = runtime.run(
            parent_task_id=parent_task_id,
            objective=objective,
            requested_agent_keys=requested_agent_keys,
            worker_inputs=merged_worker_inputs,
        )
        for candidate in result.memory_candidates:
            if not candidate.get("provenance_json"):
                continue
            self.create_memory(MemoryCreate(
                memory_type=str(candidate.get("memory_type") or "analysis_note"),
                namespace="personal",
                subject_key=str(candidate.get("tree_path") or "personal.task.agent_team_progress"),
                title="Agent Teams progress candidate",
                content_text=str(candidate.get("content_text") or ""),
                content_json={"source": "agent_team_reduce"},
                evidence_refs=list((candidate.get("provenance_json") or {}).get("evidence_refs") or []),
                approval_status="draft",
                tree_path=str(candidate.get("tree_path") or "personal.task.agent_team_progress"),
                scope="personal",
                node_type="fact",
                provenance_json=dict(candidate.get("provenance_json") or {}),
                trust_level="assistant_inferred",
            ))
        return {
            "schema_version": "research_assistant_agent_team_result_v1",
            "parent_task_id": result.parent_task_id,
            "status": result.status,
            "assistant_text": result.assistant_text,
            "reduce_json": result.reduce_json,
            "worker_results": [item.as_reduce_item() for item in result.worker_results],
            "memory_candidates": list(result.memory_candidates),
            "trace": list(result.trace),
        }

    def run_qe_autonomous_evolution(self, request_payload: dict[str, Any]) -> Any:
        request = request_from_mapping(request_payload)
        adapter = QeAutonomyAdapter()
        runtime = AutonomousEvolutionRuntime(
            providers=AutonomousEvolutionProviders(
                run_store=ResearchAssistantQeAutonomyRunStore(self.repository),
                loop_executor=adapter,
                evaluator=adapter,
                direction_decider=adapter,
                config_generator=adapter,
                submitter=adapter,
            ),
            experience_replay_provider=RepositorySkillLibraryExperienceReplayProvider(self.repository),
            id_factory=lambda prefix, stable_key: new_id(f"{prefix}_{stable_key}"),
        )
        return runtime.autonomous_evolve(request)

    def runtime_code_visibility(self) -> dict[str, Any]:
        current_commit = _git_output(["rev-parse", "HEAD"])
        origin_main = _git_output(["rev-parse", "origin/main"])
        current_source_sha = _file_sha256(SERVICE_MODULE_PATH)
        loaded_source_matches_disk = bool(
            SERVICE_LOADED_SOURCE_SHA256 and current_source_sha and SERVICE_LOADED_SOURCE_SHA256 == current_source_sha
        )
        loaded_commit_matches_repo = bool(
            SERVICE_LOADED_GIT_COMMIT and current_commit and SERVICE_LOADED_GIT_COMMIT == current_commit
        )
        repo_matches_origin_main = bool(current_commit and origin_main and current_commit == origin_main)
        runtime_matches_origin_main = bool(SERVICE_LOADED_GIT_COMMIT and origin_main and SERVICE_LOADED_GIT_COMMIT == origin_main)
        status = "current" if loaded_source_matches_disk and loaded_commit_matches_repo and repo_matches_origin_main else "stale_or_unverified"
        return {
            "schema_version": "aistock_research_assistant_runtime_code_visibility_v1",
            "service": "research-assistant",
            "status": status,
            "runtime_loaded_at": SERVICE_LOADED_AT,
            "runtime_loaded_git_commit": SERVICE_LOADED_GIT_COMMIT,
            "runtime_loaded_git_commit_short": _short_hash(SERVICE_LOADED_GIT_COMMIT),
            "runtime_loaded_source_sha256": SERVICE_LOADED_SOURCE_SHA256,
            "runtime_loaded_source_sha256_short": _short_hash(SERVICE_LOADED_SOURCE_SHA256),
            "current_repo_git_commit": current_commit,
            "current_repo_git_commit_short": _short_hash(current_commit),
            "origin_main_git_commit": origin_main,
            "origin_main_git_commit_short": _short_hash(origin_main),
            "current_source_sha256": current_source_sha,
            "current_source_sha256_short": _short_hash(current_source_sha),
            "loaded_source_matches_disk": loaded_source_matches_disk,
            "loaded_commit_matches_repo": loaded_commit_matches_repo,
            "repo_matches_origin_main": repo_matches_origin_main,
            "runtime_matches_origin_main": runtime_matches_origin_main,
            "restart_required_to_activate_main": not loaded_commit_matches_repo or not loaded_source_matches_disk,
            "operator_message": (
                "Running Research Assistant code matches local/origin main."
                if status == "current"
                else "Running Research Assistant code may not match the synced repository; restart the backend only when you want to activate merged code."
            ),
        }


    def _workflow_capabilities(self) -> list[dict[str, Any]]:
        return self.declarative_config.workflow_capability_list()

    @classmethod
    def _canonicalize_capability_mcp_refs(cls, capability: dict[str, Any], *, source: str = "capability_catalog") -> dict[str, Any]:
        refs = capability.get("mcp_tool_refs")
        capability_key = str(capability.get("capability_key") or "")
        if source == "declarative_yaml_memory_authority" and refs in (None, "", {}):
            error = cls._payload_for_capability_registry_mcp_refs_error(
                capability_key=capability_key,
                actual_type=type(refs).__name__,
                detail="YAML declarative authority requires mcp_tool_refs to be an explicit list",
                source=source,
            )
            error["reason_code"] = "declarative_config_invalid_capability_mcp_tool_refs"
            error["code"] = error["reason_code"]
            error["stage"] = "declarative_config_validation"
            error["config_key"] = RUNTIME_CONFIG_KEY
            raise ResearchAssistantRuntimeConfigInvalidError(error)
        if refs is None or refs == "" or refs == {}:
            logger.warning(
                "research assistant capability mcp_tool_refs empty non-list normalized: capability_key=%s source=%s actual_type=%s",
                capability_key,
                source,
                type(refs).__name__,
            )
            capability["mcp_tool_refs"] = []
            return capability
        if not isinstance(refs, list):
            error = cls._payload_for_capability_registry_mcp_refs_error(
                capability_key=capability_key,
                actual_type=type(refs).__name__,
                detail="must be a list; only empty null/{} or empty string can be normalized to []",
                source=source,
            )
            logger.error(
                "research assistant capability registry invalid: reason_code=%s capability_key=%s source=%s field=mcp_tool_refs actual_type=%s",
                error["reason_code"],
                capability_key,
                source,
                error["actual_type"],
            )
            raise ResearchAssistantRuntimeConfigInvalidError(error)
        canonical_refs: list[dict[str, Any]] = []
        for entry_index, ref in enumerate(refs):
            if not isinstance(ref, dict):
                error = cls._payload_for_capability_registry_mcp_refs_error(
                    capability_key=capability_key,
                    actual_type=type(ref).__name__,
                    detail="entries must be objects",
                    source=source,
                    entry_index=entry_index,
                )
                logger.error(
                    "research assistant capability registry invalid: reason_code=%s capability_key=%s source=%s field=mcp_tool_refs entry_index=%s actual_type=%s",
                    error["reason_code"],
                    capability_key,
                    source,
                    entry_index,
                    error["actual_type"],
                )
                raise ResearchAssistantRuntimeConfigInvalidError(error)
            item = dict(ref)
            if item.get("server_key"):
                item["server_key"] = canonicalize_server_key(str(item["server_key"]))
            canonical_refs.append(item)
        capability["mcp_tool_refs"] = canonical_refs
        return capability

    @classmethod
    def _canonicalize_capability_skill_refs(cls, capability: dict[str, Any], *, source: str = "capability_catalog") -> dict[str, Any]:
        refs = capability.get("skill_refs")
        capability_key = str(capability.get("capability_key") or "")
        if source == "declarative_yaml_memory_authority" and refs in (None, "", {}):
            error = cls._payload_for_capability_registry_mcp_refs_error(
                capability_key=capability_key,
                field="skill_refs",
                actual_type=type(refs).__name__,
                detail="YAML declarative authority requires skill_refs to be an explicit list",
                source=source,
            )
            error["reason_code"] = "declarative_config_invalid_workflow_capability"
            error["code"] = error["reason_code"]
            error["stage"] = "declarative_config_validation"
            error["config_key"] = RUNTIME_CONFIG_KEY
            raise ResearchAssistantRuntimeConfigInvalidError(error)
        if refs is None or refs == "" or refs == {}:
            logger.warning(
                "research assistant capability skill_refs empty non-list normalized: capability_key=%s source=%s actual_type=%s",
                capability_key,
                source,
                type(refs).__name__,
            )
            capability["skill_refs"] = []
            return capability
        if not isinstance(refs, list):
            error = cls._payload_for_capability_registry_mcp_refs_error(
                capability_key=capability_key,
                field="skill_refs",
                actual_type=type(refs).__name__,
                detail="must be a list; only empty null/{} or empty string can be normalized to []",
                source=source,
            )
            logger.error(
                "research assistant capability registry invalid: reason_code=%s capability_key=%s source=%s field=skill_refs actual_type=%s",
                error["reason_code"],
                capability_key,
                source,
                error["actual_type"],
            )
            raise ResearchAssistantRuntimeConfigInvalidError(error)
        canonical_refs: list[str] = []
        for entry_index, ref in enumerate(refs):
            if not isinstance(ref, str) or not ref:
                error = cls._payload_for_capability_registry_mcp_refs_error(
                    capability_key=capability_key,
                    field="skill_refs",
                    actual_type=type(ref).__name__,
                    detail="entries must be non-empty strings",
                    source=source,
                    entry_index=entry_index,
                )
                logger.error(
                    "research assistant capability registry invalid: reason_code=%s capability_key=%s source=%s field=skill_refs entry_index=%s actual_type=%s",
                    error["reason_code"],
                    capability_key,
                    source,
                    entry_index,
                    error["actual_type"],
                )
                raise ResearchAssistantRuntimeConfigInvalidError(error)
            canonical_refs.append(ref)
        capability["skill_refs"] = canonical_refs
        return capability

    @classmethod
    def _canonicalize_capability_refs(cls, capability: dict[str, Any], *, source: str = "capability_catalog") -> dict[str, Any]:
        capability = cls._canonicalize_capability_mcp_refs(capability, source=source)
        return cls._canonicalize_capability_skill_refs(capability, source=source)

    def health(self) -> dict[str, Any]:
        repository_health = self.repository.health()
        if repository_health.get("status") == "ok":
            catalog_readiness = self.catalog_readiness()
            status = "ok" if catalog_readiness["ready"] else "catalog_not_ready"
        else:
            catalog_readiness = {
                "ready": False,
                "status": "schema_missing",
                "checks": [],
                "missing_catalogs": ["research_assistant_schema"],
                "operator_action": "apply backend.db.init_research_assistant_schema_20260521",
                "human_message": "Research Assistant schema is missing or out of date; apply the committed DDL before catalog initialization.",
                "generated_at": utc_now().isoformat(),
            }
            status = "schema_missing"
        return {
            "service": "research-assistant",
            "status": status,
            "repository": repository_health,
            "catalog_readiness": catalog_readiness,
            "runtime_code": self.runtime_code_visibility(),
            "phase": "mcp_skill_execution_closure",
            "implemented_capabilities": {
                "mcp_api_preflight": True,
                "approval_gates": True,
                "trace_audit": True,
                "memory_audit": True,
                "prompt_pack_activation": True,
                "runtime_context_config": True,
                "capability_registry": True,
                "action_proposals": True,
                "execution_gateway": True,
                "qe_create_experiment_workflow": True,
            },
            "governance_boundaries": {
                "formal_github_issue_requires_approval": True,
                "production_trading_requires_external_gate": True,
                "silent_fallback": False,
            },
        }

    def catalog_readiness(self) -> dict[str, Any]:
        checks: list[dict[str, Any]] = []
        missing_catalogs: list[str] = []
        for requirement in CATALOG_READINESS_REQUIREMENTS:
            catalog = str(requirement["catalog"])
            filters = requirement.get("filters") or {}
            manifest_catalogs = {
                "mcp_servers": self._manifest_mcp_server_records,
                "mcp_tools": self._manifest_mcp_catalog_records,
            }
            if catalog in manifest_catalogs:
                records = [
                    item
                    for item in manifest_catalogs[catalog]()
                    if all(value in {None, ""} or item.get(key) == value for key, value in filters.items())
                ]
                present = len(records)
                source = "gateway_manifest_derived_catalog"
            elif (declarative_records := self._declarative_records_for_kind(catalog)) is not None:
                records = [
                    item
                    for item in declarative_records
                    if all(value in {None, ""} or item.get(key) == value for key, value in filters.items())
                ]
                present = len(records)
                source = "yaml_memory_authority"
            else:
                page = self.repository.list_records(
                    catalog,
                    filters=filters,
                    limit=1,
                )
                present = int(page.get("total") or 0)
                source = "repository_cache"
            expected_min = int(requirement["expected_min"])
            ready = present >= expected_min
            check = {
                "catalog": catalog,
                "label": requirement["label"],
                "expected_min": expected_min,
                "present": present,
                "ready": ready,
                "filters": filters,
                "source": source,
            }
            if not ready:
                check["missing_count"] = max(expected_min - present, 0)
                missing_catalogs.append(catalog)
            checks.append(check)
        ready = not missing_catalogs
        return {
            "ready": ready,
            "status": "ready" if ready else "catalog_not_ready",
            "checks": checks,
            "missing_catalogs": missing_catalogs,
            "operator_action": None if ready else CATALOG_BOOTSTRAP_ACTION,
            "human_message": (
                "Research Assistant catalogs are ready."
                if ready
                else "研究助理目录尚未初始化完整；请先初始化 Prompt Tree、MCP、Skill 与模型路由目录。"
            ),
            "generated_at": utc_now().isoformat(),
        }

    def ensure_catalog_ready(self) -> dict[str, Any]:
        readiness = self.catalog_readiness()
        if not readiness["ready"]:
            raise ResearchAssistantCatalogNotReadyError(readiness)
        return readiness

    def _approved_workflow_capabilities(self) -> list[dict[str, Any]]:
        return [
            dict(item)
            for item in self._workflow_capabilities()
            if str(item.get("status") or "approved") == "approved"
        ]

    def _workflow_capability_by_key(self, capability_key: str, *, approved_only: bool = True) -> dict[str, Any] | None:
        capability = self.declarative_config.workflow_capability(capability_key)
        if not capability:
            return None
        if approved_only and str(capability.get("status") or "approved") != "approved":
            return None
        return capability

    def active_runtime_config(self) -> dict[str, Any]:
        return self.declarative_config.runtime_config_payload()

    def active_runtime_config_activation(self) -> dict[str, Any]:
        return self.declarative_config.runtime_activation_record()

    def active_prompt_activation(self) -> dict[str, Any]:
        return self.declarative_config.prompt_activation_record()

    def configured_limit(self, key: str) -> int:
        config = self.active_runtime_config()
        limits = config.get("query_limits") or {}
        if key not in limits:
            raise KeyError(f"Research Assistant runtime query limit is missing: {key}")
        value = int(limits[key])
        if value <= 0:
            raise ValueError(f"Research Assistant runtime query limit must be positive: {key}")
        return value

    def overview(self) -> dict[str, Any]:
        task_status = self.repository.counts("tasks", "status")
        approval_status = self.repository.counts("approvals", "status")
        try:
            issue_summary = self.issue_fact_source.issue_candidate_summary()
            issue_status = dict(issue_summary.get("by_status") or {})
            candidate_issues = int(issue_summary.get("open_count") or issue_summary.get("candidate_count") or 0)
            issue_data_state = str(issue_summary.get("data_state") or "complete")
            issue_reason_codes = list(issue_summary.get("reason_codes") or [])
        except Exception as exc:  # noqa: BLE001 - overview must expose fact-source failures, not hide them.
            reason = f"{PIPELINE_ISSUE_FACT_SOURCE_UNAVAILABLE}: {type(exc).__name__}: {exc}"
            logger.warning("Validation issue fact source unavailable for RA overview: %s", reason)
            issue_status = {}
            candidate_issues = 0
            issue_data_state = "degraded"
            issue_reason_codes = [PIPELINE_ISSUE_FACT_SOURCE_UNAVAILABLE]
        memory_status = self.repository.counts("memory_items", "approval_status")
        trace_status = self.repository.counts("trace_events", "status")
        return {
            "task_status": task_status,
            "approval_status": approval_status,
            "issue_candidate_status": issue_status,
            "issue_candidate_data_state": issue_data_state,
            "issue_candidate_reason_codes": issue_reason_codes,
            "memory_approval_status": memory_status,
            "trace_status": trace_status,
            "running_tasks": task_status.get("running", 0),
            "pending_approvals": approval_status.get("pending", 0),
            "candidate_issues": candidate_issues,
            "approved_memories": memory_status.get("approved", 0),
            "generated_at": utc_now().isoformat(),
        }

    def _consume_approval_gate(
        self,
        *,
        approval_id: str | None,
        confirmation_text: str | None,
        approval_type: str,
        required_summary_fragment: str | None = None,
    ) -> dict[str, Any]:
        if not approval_id:
            raise ValueError(f"{approval_type} requires approval_id")
        approval = self.repository.get_record("approvals", approval_id)
        if not approval:
            raise KeyError(f"approval not found: {approval_id}")
        if approval.get("status") != "pending":
            raise ValueError(f"approval is not pending: {approval.get('status')}")
        if approval.get("approval_type") != approval_type:
            raise ValueError(f"approval_type mismatch: expected {approval_type}, got {approval.get('approval_type')}")
        expected = approval.get("required_confirmation_text")
        if confirmation_text != expected:
            raise ValueError("confirmation_text does not match approval.required_confirmation_text")
        if required_summary_fragment and required_summary_fragment not in str(approval.get("summary") or ""):
            raise ValueError("approval summary does not match requested action")
        return self.decide_approval(
            approval_id,
            action="approve",
            confirmation_text=confirmation_text or "",
            decided_by="research_assistant_gate",
        )

    @staticmethod
    def _approval_context(approval: dict[str, Any]) -> dict[str, Any]:
        context = approval.get("approval_context_json")
        return dict(context) if isinstance(context, dict) else {}

    def _action_proposal_for_approval(self, approval: dict[str, Any]) -> dict[str, Any] | None:
        context = self._approval_context(approval)
        action_proposal_id = str(context.get("action_proposal_id") or "")
        proposal = self.repository.get_record("action_proposals", action_proposal_id) if action_proposal_id else None
        if proposal:
            return proposal
        if approval.get("approval_id"):
            return self.repository.find_one("action_proposals", {"approval_id": str(approval["approval_id"])})
        return None

    @staticmethod
    def _approval_requires_explicit_token(approval: dict[str, Any], proposal: dict[str, Any] | None) -> bool:
        context = ResearchAssistantService._approval_context(approval)
        if str(context.get("required_approval_level") or "").upper() == "L2":
            return True
        risk = str((proposal or {}).get("risk_level") or approval.get("risk_level") or context.get("risk_level") or "")
        side_effect = str((proposal or {}).get("side_effect_level") or context.get("side_effect_level") or "")
        return "production_sensitive" in {risk, side_effect}

    @staticmethod
    def _message_is_clear_approval_affirmation(message: str) -> bool:
        text = message.strip()
        if not text:
            return False
        lower = text.lower()
        negative_terms = ("不同意", "不确认", "不要", "别", "拒绝", "取消", "否", "no", "reject", "cancel")
        if any(term in lower for term in negative_terms):
            return False
        direct = {"同意", "确认", "批准", "确认执行", "同意执行", "可以执行", "继续执行", "approve", "approved", "yes"}
        if lower in direct:
            return True
        affirmative_terms = ("同意", "确认", "批准", "approve", "yes")
        reference_terms = ("执行", "审批", "批准", "这个", "该操作", "上一步", "上一轮", "继续", "确认它")
        return any(term in lower for term in affirmative_terms) and any(term in lower for term in reference_terms)

    @staticmethod
    def _approval_confirmation_reason(exc: BaseException) -> str:
        message = str(exc).lower()
        if isinstance(exc, KeyError) or "not found" in message:
            return "approval_confirmation_approval_not_found"
        if "confirmation_text" in message:
            return "approval_confirmation_text_mismatch"
        if "not pending" in message:
            return "approval_confirmation_not_pending"
        if "approval_type" in message:
            return "approval_confirmation_type_mismatch"
        return "approval_confirmation_rejected"

    @staticmethod
    def _approval_confirmation_error(
        *,
        reason_code: str,
        message: str,
        approval_id: str | None = None,
        action_proposal_id: str | None = None,
        expected_confirmation_text: str | None = None,
    ) -> dict[str, Any]:
        return {
            "reason_code": reason_code,
            "code": reason_code,
            "stage": "chat_approval_confirmation",
            "approval_id": approval_id,
            "action_proposal_id": action_proposal_id,
            "message": message,
            "expected_confirmation_text": expected_confirmation_text,
            "operator_action": "请在同一对话中显式提供 confirm_approval_id，并让 confirmation_text 完全等于审批卡片上的确认口令。",
        }

    def _required_confirmation_text_for_chat_approval(
        self,
        proposal: dict[str, Any],
        *,
        preflight_payload: dict[str, Any],
        decision: ToolGateDecision,
        call: McpToolCall,
    ) -> str:
        candidates: list[str] = []
        for item in preflight_payload.get("missing_confirmations") or []:
            if str(item):
                candidates.append(str(item))
        if decision.catalog_entry:
            candidates.extend(str(item) for item in decision.catalog_entry.required_confirmations if str(item))
        capability = self._workflow_capability_by_key(str(proposal["capability_key"]))
        if not capability:
            raise KeyError(f"approved capability not found: {proposal['capability_key']}")
        tool = self._resolve_capability_tool(capability, proposal)
        effective_profile = self._effective_action_profile(capability, tool)
        candidates.extend(str(item) for item in effective_profile.get("required_confirmations", []) if str(item))
        if not candidates:
            raise ValueError(
                "action proposal approval is missing configured required_confirmations: "
                f"action_proposal_id={proposal.get('action_proposal_id')} "
                f"capability_key={proposal.get('capability_key')} tool={call.server_key}/{call.tool_name}"
            )
        return candidates[0]

    def _ensure_action_proposal_chat_approval(
        self,
        proposal: dict[str, Any],
        *,
        preflight_payload: dict[str, Any],
        decision: ToolGateDecision,
        call: McpToolCall,
    ) -> dict[str, Any]:
        existing_approval_id = str(proposal.get("approval_id") or "")
        if existing_approval_id:
            approval = self.repository.get_record("approvals", existing_approval_id)
            if not approval:
                raise KeyError(f"action proposal references missing approval: {existing_approval_id}")
            if approval.get("approval_type") != ACTION_PROPOSAL_EXECUTE_APPROVAL_TYPE:
                raise ValueError(
                    "action proposal approval_type mismatch: "
                    f"approval_id={existing_approval_id} expected={ACTION_PROPOSAL_EXECUTE_APPROVAL_TYPE} actual={approval.get('approval_type')}"
                )
            if approval.get("status") != "pending":
                raise ValueError(f"action proposal approval is not pending: approval_id={existing_approval_id} status={approval.get('status')}")
            return approval

        required_confirmation_text = self._required_confirmation_text_for_chat_approval(
            proposal,
            preflight_payload=preflight_payload,
            decision=decision,
            call=call,
        )
        required_approval_level = "L2" if "production_sensitive" in {str(proposal.get("risk_level") or ""), str(proposal.get("side_effect_level") or "")} else "L1"
        approval = self.create_approval(
            ApprovalCreate(
                task_id=proposal["task_id"],
                approval_type=ACTION_PROPOSAL_EXECUTE_APPROVAL_TYPE,
                risk_level=str(proposal.get("risk_level") or decision.risk_level),
                plan_digest=str(proposal["plan_digest"]),
                config_version_id=proposal.get("runtime_config_activation_id"),
                summary=(
                    f"Execute action proposal {proposal['action_proposal_id']} "
                    f"for {proposal.get('capability_key')} via {call.server_key}/{call.tool_name}"
                ),
                required_confirmation_text=required_confirmation_text,
                created_by="research_assistant_chat_gate",
            )
        )
        context = {
            "conversation_id": proposal.get("conversation_id"),
            "action_proposal_id": proposal["action_proposal_id"],
            "capability_key": proposal.get("capability_key"),
            "server_key": call.server_key,
            "tool_name": call.tool_name,
            "risk_level": proposal.get("risk_level"),
            "side_effect_level": proposal.get("side_effect_level"),
            "required_approval_level": required_approval_level,
            "source": "chat_preflight_confirmation_card",
        }
        approval = self.repository.update_record("approvals", approval["approval_id"], {"approval_context_json": context})
        self.repository.update_record("action_proposals", proposal["action_proposal_id"], {"approval_id": approval["approval_id"]})
        return approval

    def _ensure_skill_action_proposal_chat_approval(self, proposal: dict[str, Any]) -> dict[str, Any]:
        existing_approval_id = str(proposal.get("approval_id") or "")
        if existing_approval_id:
            approval = self.repository.get_record("approvals", existing_approval_id)
            if not approval:
                raise KeyError(f"skill action proposal references missing approval: {existing_approval_id}")
            if approval.get("approval_type") != ACTION_PROPOSAL_EXECUTE_APPROVAL_TYPE:
                raise ValueError(
                    "skill action proposal approval_type mismatch: "
                    f"approval_id={existing_approval_id} expected={ACTION_PROPOSAL_EXECUTE_APPROVAL_TYPE} actual={approval.get('approval_type')}"
                )
            if approval.get("status") != "pending":
                raise ValueError(f"skill action proposal approval is not pending: approval_id={existing_approval_id} status={approval.get('status')}")
            return approval

        required_approval_level = "L2" if "production_sensitive" in {str(proposal.get("risk_level") or ""), str(proposal.get("side_effect_level") or "")} else "L1"
        approval = self.create_approval(
            ApprovalCreate(
                task_id=proposal["task_id"],
                approval_type=ACTION_PROPOSAL_EXECUTE_APPROVAL_TYPE,
                risk_level=str(proposal.get("risk_level") or "high"),
                plan_digest=str(proposal["plan_digest"]),
                config_version_id=proposal.get("runtime_config_activation_id"),
                summary=(
                    f"Execute action proposal {proposal['action_proposal_id']} "
                    f"for {proposal.get('capability_key')} via selected skill"
                ),
                required_confirmation_text=SKILL_LIBRARY_REUSE_CONFIRMATION,
                created_by="research_assistant_chat_skill_gate",
            )
        )
        context = {
            "conversation_id": proposal.get("conversation_id"),
            "action_proposal_id": proposal["action_proposal_id"],
            "capability_key": proposal.get("capability_key"),
            "proposal_type": proposal.get("proposal_type"),
            "risk_level": proposal.get("risk_level"),
            "side_effect_level": proposal.get("side_effect_level"),
            "required_approval_level": required_approval_level,
            "source": "chat_skill_reuse_confirmation_card",
        }
        approval = self.repository.update_record("approvals", approval["approval_id"], {"approval_context_json": context})
        self.repository.update_record("action_proposals", proposal["action_proposal_id"], {"approval_id": approval["approval_id"]})
        return approval

    def _pending_chat_action_approvals(self, conversation_id: str, *, last_assistant_only: bool) -> list[tuple[dict[str, Any], dict[str, Any]]]:
        approval_page = self.repository.list_records("approvals", filters={"status": "pending"}, limit=self.configured_limit("api_list_approvals"))
        last_ids = self._last_assistant_pending_approval_ids(conversation_id) if last_assistant_only else None
        matches: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for approval in approval_page["items"]:
            if approval.get("approval_type") != ACTION_PROPOSAL_EXECUTE_APPROVAL_TYPE:
                continue
            proposal = self._action_proposal_for_approval(approval)
            if not proposal or proposal.get("conversation_id") != conversation_id:
                continue
            if last_ids is not None and approval.get("approval_id") not in last_ids:
                continue
            matches.append((approval, proposal))
        return matches

    def _last_assistant_pending_approval_ids(self, conversation_id: str) -> set[str]:
        messages = self.repository.list_records(
            "conversation_messages",
            filters={"conversation_id": conversation_id},
            limit=self.configured_limit("conversation_messages_full"),
        )["items"]
        for message in messages:
            if message.get("role") != "assistant":
                continue
            content_json = message.get("content_json") if isinstance(message.get("content_json"), dict) else {}
            cards = content_json.get("cards") if isinstance(content_json.get("cards"), dict) else {}
            ids: set[str] = set()
            for proposal in cards.get("action_proposals") or []:
                if not isinstance(proposal, dict):
                    continue
                if proposal.get("approval_id") and proposal.get("approval_required"):
                    ids.add(str(proposal["approval_id"]))
            execution = cards.get("mcp_execution_result") if isinstance(cards.get("mcp_execution_result"), dict) else {}
            if execution.get("approval_id") and execution.get("status") == "approval_required":
                ids.add(str(execution["approval_id"]))
            return ids
        return set()

    def _chat_action_approval_context(self, approval_id: str, conversation_id: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None]:
        approval = self.repository.get_record("approvals", approval_id)
        if not approval:
            return None, None, self._approval_confirmation_error(
                reason_code="approval_confirmation_approval_not_found",
                message=f"approval_id 不存在：{approval_id}",
                approval_id=approval_id,
            )
        if approval.get("approval_type") != ACTION_PROPOSAL_EXECUTE_APPROVAL_TYPE:
            return approval, None, self._approval_confirmation_error(
                reason_code="approval_confirmation_type_mismatch",
                message=f"approval_type 不匹配：expected={ACTION_PROPOSAL_EXECUTE_APPROVAL_TYPE}, actual={approval.get('approval_type')}",
                approval_id=approval_id,
            )
        proposal = self._action_proposal_for_approval(approval)
        if not proposal:
            return approval, None, self._approval_confirmation_error(
                reason_code="approval_confirmation_action_proposal_missing",
                message=f"approval_id 未绑定 action_proposal：{approval_id}",
                approval_id=approval_id,
            )
        if proposal.get("conversation_id") != conversation_id:
            return approval, proposal, self._approval_confirmation_error(
                reason_code="approval_confirmation_cross_conversation",
                message=f"approval_id 不属于当前对话：approval_id={approval_id}",
                approval_id=approval_id,
                action_proposal_id=str(proposal.get("action_proposal_id") or ""),
                expected_confirmation_text=str(approval.get("required_confirmation_text") or ""),
            )
        return approval, proposal, None

    def _maybe_handle_chat_approval_confirmation(
        self,
        *,
        data: ChatTurnRequest,
        conversation_id: str,
        task: dict[str, Any],
        user_message: dict[str, Any],
        dialogue_intent: DialogueIntent,
        mode_decision: ModeDecision,
    ) -> dict[str, Any] | None:
        if data.confirm_approval_id or data.confirmation_text:
            if data.created_by != "user":
                error = self._approval_confirmation_error(
                    reason_code="approval_confirmation_requires_user_message",
                    message=f"审批确认只能来自用户消息，created_by={data.created_by}",
                    approval_id=data.confirm_approval_id,
                )
                return self._chat_approval_response(data, conversation_id, task, user_message, dialogue_intent, mode_decision, error=error)
            if data.confirm_approval_id:
                approval, proposal, error = self._chat_action_approval_context(data.confirm_approval_id, conversation_id)
                if error:
                    return self._chat_approval_response(data, conversation_id, task, user_message, dialogue_intent, mode_decision, error=error)
            else:
                pending = self._pending_chat_action_approvals(conversation_id, last_assistant_only=True)
                if len(pending) != 1:
                    error = self._approval_confirmation_error(
                        reason_code="approval_confirmation_ambiguous_pending_approval",
                        message=f"当前对话上一轮 pending approval 数量为 {len(pending)}；请显式提供 confirm_approval_id。",
                    )
                    return self._chat_approval_response(data, conversation_id, task, user_message, dialogue_intent, mode_decision, error=error)
                approval, proposal = pending[0]
            if not data.confirmation_text:
                error = self._approval_confirmation_error(
                    reason_code="approval_confirmation_text_required",
                    message="缺少 confirmation_text；必须完全等于审批卡片上的确认口令。",
                    approval_id=str((approval or {}).get("approval_id") or data.confirm_approval_id or ""),
                    action_proposal_id=str((proposal or {}).get("action_proposal_id") or ""),
                    expected_confirmation_text=str((approval or {}).get("required_confirmation_text") or ""),
                )
                return self._chat_approval_response(data, conversation_id, task, user_message, dialogue_intent, mode_decision, error=error)
            return self._consume_and_execute_chat_approval(
                data,
                conversation_id,
                task,
                user_message,
                dialogue_intent,
                mode_decision,
                approval=approval,
                proposal=proposal,
                confirmation_text=data.confirmation_text,
                confirmation_source="explicit_request_field",
            )

        if not self._message_is_clear_approval_affirmation(data.message):
            return None
        pending = self._pending_chat_action_approvals(conversation_id, last_assistant_only=True)
        if not pending:
            return None
        if data.created_by != "user":
            error = self._approval_confirmation_error(
                reason_code="approval_confirmation_requires_user_message",
                message=f"审批确认只能来自用户消息，created_by={data.created_by}",
            )
            return self._chat_approval_response(data, conversation_id, task, user_message, dialogue_intent, mode_decision, error=error)
        if len(pending) != 1:
            error = self._approval_confirmation_error(
                reason_code="approval_confirmation_ambiguous_pending_approval",
                message=f"当前对话上一轮有 {len(pending)} 个 pending approval；“同意/确认”存在歧义，请显式提供 approval_id 和确认口令。",
            )
            return self._chat_approval_response(data, conversation_id, task, user_message, dialogue_intent, mode_decision, error=error)
        approval, proposal = pending[0]
        if self._approval_requires_explicit_token(approval, proposal):
            error = self._approval_confirmation_error(
                reason_code="approval_confirmation_l2_requires_explicit_token",
                message="该审批为 L2/production_sensitive，裸“同意/确认”不能映射为确认口令；必须显式回填确认口令。",
                approval_id=str(approval.get("approval_id") or ""),
                action_proposal_id=str(proposal.get("action_proposal_id") or ""),
                expected_confirmation_text=str(approval.get("required_confirmation_text") or ""),
            )
            return self._chat_approval_response(data, conversation_id, task, user_message, dialogue_intent, mode_decision, error=error)
        return self._consume_and_execute_chat_approval(
            data,
            conversation_id,
            task,
            user_message,
            dialogue_intent,
            mode_decision,
            approval=approval,
            proposal=proposal,
            confirmation_text=str(approval.get("required_confirmation_text") or ""),
            confirmation_source="user_natural_language_affirmation",
        )

    def _consume_and_execute_chat_approval(
        self,
        data: ChatTurnRequest,
        conversation_id: str,
        task: dict[str, Any],
        user_message: dict[str, Any],
        dialogue_intent: DialogueIntent,
        mode_decision: ModeDecision,
        *,
        approval: dict[str, Any],
        proposal: dict[str, Any],
        confirmation_text: str,
        confirmation_source: str,
    ) -> dict[str, Any]:
        approval_id = str(approval.get("approval_id") or "")
        action_proposal_id = str(proposal.get("action_proposal_id") or "")
        try:
            consumed = self._consume_approval_gate(
                approval_id=approval_id,
                confirmation_text=confirmation_text,
                approval_type=ACTION_PROPOSAL_EXECUTE_APPROVAL_TYPE,
                required_summary_fragment=action_proposal_id,
            )
        except (KeyError, ValueError) as exc:
            error = self._approval_confirmation_error(
                reason_code=self._approval_confirmation_reason(exc),
                message=str(exc),
                approval_id=approval_id,
                action_proposal_id=action_proposal_id,
                expected_confirmation_text=str(approval.get("required_confirmation_text") or ""),
            )
            return self._chat_approval_response(data, conversation_id, task, user_message, dialogue_intent, mode_decision, error=error)
        proposal = self.repository.update_record("action_proposals", action_proposal_id, {"status": "approved", "approval_id": approval_id})
        executed = self.execute_action_proposal(
            action_proposal_id,
            ActionProposalExecuteRequest(
                payload_json=dict(proposal.get("input_json") or {}),
                idempotency_key=proposal.get("idempotency_key"),
            ),
        )
        return self._chat_approval_response(
            data,
            conversation_id,
            task,
            user_message,
            dialogue_intent,
            mode_decision,
            approval=consumed,
            proposal=proposal,
            executed=executed,
            confirmation_source=confirmation_source,
        )

    def _chat_approval_response(
        self,
        data: ChatTurnRequest,
        conversation_id: str,
        task: dict[str, Any],
        user_message: dict[str, Any],
        dialogue_intent: DialogueIntent,
        mode_decision: ModeDecision,
        *,
        approval: dict[str, Any] | None = None,
        proposal: dict[str, Any] | None = None,
        executed: dict[str, Any] | None = None,
        confirmation_source: str | None = None,
        error: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        mode_decision_json = mode_decision.as_dict()
        cards: dict[str, Any] = {
            "intent_type": "approval_confirmation",
            "dialogue_mode": mode_decision.mode.value,
            "mode_decision": mode_decision_json,
            "status_rail": [{"label": "审批确认", "status": "failed" if error else "done"}],
            "action_proposals": [],
            "safety": {
                "no_silent_error": True,
                "fail_closed": bool(error),
                "confirmation_text_exact_match": error is None,
                "agent_self_approval_prevented": True,
            },
        }
        if error:
            assistant_text = (
                "审批确认未执行："
                f"reason_code={error['reason_code']}; "
                f"approval_id={error.get('approval_id')}; "
                f"action_proposal_id={error.get('action_proposal_id')}; "
                f"message={error.get('message')}; "
                f"operator_action={error.get('operator_action')}"
            )
            cards["approval_confirmation"] = {"status": "blocked", **error}
            cards["mcp_execution_result"] = {"auto_executed": False, "executed": False, "status": "blocked", "error": error}
            trace_status = "blocked"
        else:
            if approval is None or proposal is None or executed is None:
                raise ValueError(
                    "chat approval response requires approval, proposal, and execution result when no error is present; "
                    f"approval_present={approval is not None} proposal_present={proposal is not None} executed_present={executed is not None}"
                )
            tool_event = executed.get("tool_event") if isinstance(executed.get("tool_event"), dict) else {}
            payload = dict(proposal.get("input_json") or {})
            server_key = str(tool_event.get("server_key") or payload.get("server_key") or "")
            tool_name = str(tool_event.get("tool_name") or payload.get("tool_name") or "")
            result = McpToolResult(
                server_key=server_key,
                tool_name=tool_name,
                status=str(executed.get("status") or "unknown"),
                payload_json=tool_event.get("response_json") if isinstance(tool_event.get("response_json"), dict) else {},
                source_refs=self._mcp_result_source_refs(tool_event.get("response_json") if isinstance(tool_event.get("response_json"), dict) else {}, tool_event),
                as_of=self._mcp_result_as_of(tool_event.get("response_json") if isinstance(tool_event.get("response_json"), dict) else {}),
                action_proposal_id=str(proposal["action_proposal_id"]),
                executed=bool(executed.get("executed")),
                error_json=dict(executed.get("error") or {}),
                side_effect_level=str(proposal.get("side_effect_level") or payload.get("side_effect_level") or "confirmed_action"),
            )
            self._populate_cards_from_tool_execution(cards, proposal, executed, result)
            cards["mcp_execution_result"]["auto_executed"] = False
            cards["mcp_execution_result"]["triggered_by_approval"] = True
            cards["mcp_execution_result"]["approval_id"] = approval["approval_id"]
            cards["approval_confirmation"] = {
                "status": "executed" if executed.get("executed") else "execution_failed",
                "approval_id": approval["approval_id"],
                "approval_type": approval.get("approval_type"),
                "action_proposal_id": proposal["action_proposal_id"],
                "confirmation_source": confirmation_source,
                "proposal_status": (executed.get("proposal") or {}).get("status") if isinstance(executed.get("proposal"), dict) else proposal.get("status"),
            }
            assistant_text = (
                "已在对话内完成审批确认"
                f"（approval_id={approval['approval_id']}，action_proposal_id={proposal['action_proposal_id']}）。"
            )
            if executed.get("executed"):
                assistant_text += " 工具执行已完成。"
                trace_status = "ok"
            else:
                error_payload = dict(executed.get("error") or {})
                assistant_text += (
                    " 但执行被安全门拦截或失败："
                    f"reason_code={error_payload.get('code') or error_payload.get('reason_code')}; "
                    f"message={error_payload.get('human_reason') or error_payload.get('message') or ''}"
                )
                trace_status = "failed"
        trace = self.create_trace_event(
            TraceEventCreate(
                task_id=task["task_id"],
                event_type="chat_approval_confirmation",
                component="research_assistant.chat_turn.approval_gate",
                status=trace_status,
                payload_json={"approval_confirmation": cards.get("approval_confirmation"), "error": error},
            )
        )
        assistant_message = self.add_conversation_message(
            ConversationMessageCreate(
                conversation_id=conversation_id,
                role="assistant",
                content_text=assistant_text,
                content_json={
                    "cards": cards,
                    "dialogue_intent": dialogue_intent.value,
                    "dialogue_mode": mode_decision.mode.value,
                    "mode_decision": mode_decision_json,
                    "audit_summary": {"approval_confirmation": cards.get("approval_confirmation")},
                },
                task_id=task["task_id"],
                trace_id=trace["trace_id"],
                is_visible=True,
            )
        )
        followup_event_type = "rejected" if error else "mcp_done" if executed and executed.get("executed") else "mcp_failed"
        self.add_task_event(
            task["task_id"],
            TaskEventCreate(
                event_type=followup_event_type,
                severity="warning" if error else "info",
                message=assistant_text,
                payload_json={"approval_confirmation": cards.get("approval_confirmation"), "trace_id": trace["trace_id"]},
            ),
        )
        task_events = self.repository.list_records("task_events", filters={"task_id": task["task_id"]}, limit=self.configured_limit("task_events_detail"))["items"]
        return {
            "conversation": self._public_conversation(self.repository.get_record("conversations", conversation_id)),
            "user_message": self._public_conversation_message(user_message),
            "assistant_message": self._public_conversation_message(assistant_message),
            "task": self._public_task(self.repository.get_record("tasks", task["task_id"])),
            "task_events": self._public_task_events(task_events),
            "task_events_ref": {"endpoint": f"/api/v1/research-assistant/tasks/{task['task_id']}/events", "default_limit": self.configured_limit("task_events_detail")},
            "prompt_bundle": None,
            "context_pack": None,
            "trace": {"trace_id": trace["trace_id"], "status": trace["status"], "duration_ms": trace.get("duration_ms"), "model_profile_id": trace.get("model_profile_id")},
            "mode_decision": mode_decision_json,
            "context_health": {"show_badge": False},
            "cards": self._public_chat_cards(cards),
        }

    def seed_catalogs(self) -> dict[str, Any]:
        seeded = {
            "skills": 0,
            "mcp_servers": 0,
            "mcp_tools": 0,
            "capabilities": 0,
            "model_profiles": 0,
            "routing_policies": 0,
            "prompt_nodes": 0,
            "prompt_node_versions": 0,
            "prompt_activations": 0,
            "runtime_config_activations": 0,
            "memory_items": 0,
            "graph_entities": 0,
            "graph_relations": 0,
            "reports": 0,
            "notifications": 0,
        }
        for item in DEFAULT_SKILLS:
            payload = {
                "skill_id": f"skill_{item['skill_key']}",
                "version": "1.0.0",
                "skill_type": "local_codex_skill",
                "entrypoint_type": "local_skill",
                "entrypoint_ref": item["skill_key"],
                "allowed_side_effect_level": "none" if item["permission_scope"] == "read_analysis" else "draft_only",
                "required_approval_level": "L1" if item["risk_level"] == "medium" else "L2",
                "owner": "codex",
                "source_ref": f"C:/Users/lc999/.codex/skills/{item['skill_key']}/SKILL.md",
                "status": "approved",
                "checksum": sha256_json(item),
                "required_mcp_tools": [],
                "skill_key": item["skill_key"],
                "title": item["title"],
                "description": item["description"],
                "domain": item["domain"],
                "risk_level": item["risk_level"],
                "permission_scope": item["permission_scope"],
                "tags_json": item["tags_json"],
                "input_schema_json": item["input_schema_json"],
                "output_schema_json": item["output_schema_json"],
            }
            payload.update(
                {
                    "skill_type": item.get("skill_type", payload["skill_type"]),
                    "entrypoint_type": item.get("entrypoint_type", payload["entrypoint_type"]),
                    "entrypoint_ref": item.get("entrypoint_ref", payload["entrypoint_ref"]),
                    "allowed_side_effect_level": item.get("allowed_side_effect_level", payload["allowed_side_effect_level"]),
                    "required_approval_level": item.get("required_approval_level", payload["required_approval_level"]),
                    "source_ref": item.get("source_ref", payload["source_ref"]),
                    "status": item.get("status", payload["status"]),
                    "required_mcp_tools": item.get("required_mcp_tools", payload["required_mcp_tools"]),
                }
            )
            self.repository.create_record("skills", payload)
            seeded["skills"] += 1
        for item in DEFAULT_MCP_SERVERS:
            self.repository.create_record("mcp_servers", {"server_id": f"mcp_server_{item['server_key']}", **item})
            seeded["mcp_servers"] += 1
        for item in DEFAULT_MCP_TOOLS:
            tool_id = f"mcp_tool_{item['server_key']}_{item['tool_name']}".replace("-", "_")
            payload = {"tool_id": tool_id, "status": "enabled", **item}
            db_payload = {key: value for key, value in payload.items() if key in MCP_TOOL_DB_COLUMNS}
            self.repository.create_record("mcp_tools", db_payload)
            seeded["mcp_tools"] += 1
        for item in DEFAULT_MODEL_PROFILES:
            profile = dict(item)
            profile.setdefault("display_name", f"{profile['provider']} / {profile['model_name']}")
            self.repository.create_record("model_profiles", profile)
            seeded["model_profiles"] += 1
        for item in DEFAULT_ROUTING_POLICIES:
            policy = dict(item)
            policy["primary_profile_id"] = policy.pop("model_profile_id")
            policy["fallback_profile_id"] = policy.get("fallback_json", {}).get("fallback_profile_id")
            self.repository.create_record("routing_policies", policy)
            seeded["routing_policies"] += 1
        self._seed_default_memory_graph(seeded)
        self._ensure_default_reports_and_notifications(seeded)
        return {"seeded": seeded, "catalog_version": "research_assistant_gateway_manifest_20260604"}

    def _seed_default_memory_graph(self, seeded: dict[str, int]) -> None:
        for item in DEFAULT_MEMORY_SEEDS:
            payload = dict(item)
            payload["checksum"] = sha256_json(
                {
                    "memory_type": payload["memory_type"],
                    "subject_key": payload["subject_key"],
                    "content_text": payload["content_text"],
                    "content_json": payload["content_json"],
                }
            )
            self.repository.create_record("memory_items", payload)
            seeded["memory_items"] += 1

        entity_ids: dict[str, str] = {}
        for item in DEFAULT_GRAPH_ENTITIES:
            entity_id = f"entity_{item['entity_key'].replace('.', '_').replace('-', '_')}"
            entity_ids[item["entity_key"]] = entity_id
            self.repository.create_record(
                "entities",
                {
                    "entity_id": entity_id,
                    "namespace": "aistock",
                    "approval_status": "approved",
                    "confidence": 0.96,
                    "source_refs": ["docs/architecture/local_data_management_mcp_gateway_design_20260523.md#memory-graph-seed"],
                    **item,
                },
            )
            seeded["graph_entities"] += 1

        for item in DEFAULT_GRAPH_RELATIONS:
            relation_id = f"rel_{item['relation_key'].replace('.', '_').replace('-', '_')}"
            self.repository.create_record(
                "relations",
                {
                    "relation_id": relation_id,
                    "source_entity_id": entity_ids[item["source_entity_key"]],
                    "target_entity_id": entity_ids[item["target_entity_key"]],
                    "relation_type": item["relation_type"],
                    "evidence_refs": ["docs/architecture/local_data_management_mcp_gateway_design_20260523.md#memory-graph-seed"],
                    "approval_status": "approved",
                    "confidence": 0.96,
                },
            )
            seeded["graph_relations"] += 1


    def _normalize_capability_catalog(self, *, include_disabled: bool = False) -> list[dict[str, Any]]:
        capabilities: list[dict[str, Any]] = []
        approved_tools = {
            (str(tool.get("server_key")), str(tool.get("tool_name"))): tool
            for tool in self._manifest_mcp_catalog_records()
            if include_disabled or str(tool.get("status")) in {"enabled", "approved", "ready"}
        }
        approved_skills = {
            str(skill.get("skill_key")): skill
            for skill in self.repository.list_records("skills", limit=self.configured_limit("api_list_skills"))["items"]
            if include_disabled or str(skill.get("status")) == "approved"
        }
        now = utc_now().isoformat()
        for item in self._workflow_capabilities():
            item = self._canonicalize_capability_refs(dict(item), source="capability_sync_source")
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
            checksum_payload = {k: v for k, v in payload.items() if k not in {"capability_id", "last_synced_at", "checksum", "created_at", "updated_at"}}
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
        diff = [
            {
                "capability_key": capability["capability_key"],
                "change": "retired_db_projection",
                "status": capability["status"],
                "risk_level": capability["risk_level"],
                "side_effect_level": capability["side_effect_level"],
                "checksum": capability["checksum"],
                "reason": "yaml_memory_authority_no_db_write",
            }
            for capability in capabilities
        ]
        result = {
            "dry_run": not data.apply,
            "requested_by": data.requested_by,
            "source_count": len(capabilities),
            "applied_count": 0,
            "diff": diff,
            "blocked_or_disabled_excluded": not data.include_disabled,
            "db_projection_retired": True,
            "declarative_authority": "yaml_memory",
            "runtime_config": {
                "max_tools_per_server": max_tools,
                "timeout_seconds": sync_cfg["timeout_seconds"],
                "require_checksum": sync_cfg["require_checksum"],
            },
        }
        self.create_trace_event(
            TraceEventCreate(
                event_type="capability_sync",
                component="research_assistant.capability_sync",
                status="retired_noop",
                payload_json={"source_count": len(capabilities), "applied_count": 0, "db_projection_retired": True, "diff": diff[:20]},
            )
        )
        return result

    def _seed_prompt_pack(self, prompt_pack: PromptPackSnapshot, seeded: dict[str, int]) -> None:
        del prompt_pack, seeded
        logger.warning("RA prompt-pack DB projection seeding is retired; YAML memory authority is used directly.")

    def _seed_runtime_config(self, runtime_config: RuntimeConfigSnapshot, seeded: dict[str, int]) -> None:
        del runtime_config, seeded
        logger.warning("RA runtime-config DB projection seeding is retired; YAML memory authority is used directly.")

    def list_records(
        self,
        kind: str,
        *,
        filters: dict[str, Any] | None = None,
        search: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        limit_key: str | None = None,
    ) -> dict[str, Any]:
        resolved_limit = int(limit) if limit is not None else self.configured_limit(limit_key or self._default_query_limit_key(kind))
        if resolved_limit < 1:
            raise ValueError("limit must be positive")
        max_limit = self.configured_limit("api_list_max_page_size")
        if resolved_limit > max_limit:
            raise ValueError(f"limit exceeds configured api_list_max_page_size: {max_limit}")
        declarative_records = self._declarative_records_for_kind(kind)
        if declarative_records is not None:
            return self._page_declarative_records(kind, declarative_records, filters=filters, search=search, limit=resolved_limit, offset=offset)
        return self.repository.list_records(kind, filters=filters, search=search, limit=resolved_limit, offset=offset)

    def _declarative_records_for_kind(self, kind: str) -> list[dict[str, Any]] | None:
        if kind == "capabilities":
            return [
                {
                    "capability_id": f"cap_{str(item['capability_key']).replace('.', '_').replace('-', '_')}",
                    "declarative_authority": "yaml_memory",
                    **item,
                }
                for item in self._workflow_capabilities()
            ]
        if kind == "prompt_nodes":
            return [
                {"declarative_authority": "yaml_memory", **item}
                for item in self.declarative_config.prompt_node_list()
            ]
        if kind == "prompt_activations":
            return [{"declarative_authority": "yaml_memory", **self.active_prompt_activation()}]
        if kind == "runtime_config_activations":
            return [{"declarative_authority": "yaml_memory", **self.active_runtime_config_activation()}]
        return None

    @staticmethod
    def _page_declarative_records(
        kind: str,
        records: list[dict[str, Any]],
        *,
        filters: dict[str, Any] | None,
        search: str | None,
        limit: int,
        offset: int,
    ) -> dict[str, Any]:
        items = list(records)
        for key, value in (filters or {}).items():
            if value is None or value == "":
                continue
            items = [item for item in items if item.get(key) == value]
        if search:
            needle = search.lower()
            items = [item for item in items if needle in str(item).lower()]
        offset = max(0, int(offset or 0))
        limit = max(1, int(limit))
        return {
            "items": items[offset:offset + limit],
            "total": len(items),
            "page": offset // limit + 1,
            "page_size": limit,
            "has_more": offset + limit < len(items),
            "declarative_authority": "yaml_memory",
            "projection_kind": kind,
        }

    def list_pipeline_issue_candidates(
        self,
        *,
        status: str | None = None,
        module: str | None = None,
        search: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> dict[str, Any]:
        resolved_limit = int(limit) if limit is not None else self.configured_limit("validation_issue_candidates")
        if resolved_limit < 1:
            raise ValueError("limit must be positive")
        max_limit = self.configured_limit("api_list_max_page_size")
        if resolved_limit > max_limit:
            raise ValueError(f"limit exceeds configured api_list_max_page_size: {max_limit}")
        page = offset // resolved_limit + 1
        try:
            payload = self.issue_fact_source.issue_candidates(
                module=module,
                status=status,
                search=search,
                page=page,
                page_size=resolved_limit,
            )
        except Exception as exc:  # noqa: BLE001 - degraded read is explicit and user-visible.
            return self._degraded_pipeline_issue_candidate_page(exc, page=page, page_size=resolved_limit)
        items = [self._assistant_issue_candidate_view(item) for item in payload.get("items") or [] if isinstance(item, dict)]
        result = dict(payload)
        result.update(
            {
                "items": items,
                "total": int(payload.get("total") or len(items)),
                "page": page,
                "page_size": resolved_limit,
                "has_more": bool(payload.get("has_more")),
            }
        )
        return self._with_pipeline_issue_metadata(result)

    @staticmethod
    def _with_pipeline_issue_metadata(payload: dict[str, Any]) -> dict[str, Any]:
        result = dict(payload)
        result.setdefault("schema_version", "aistock_research_assistant_pipeline_issue_candidate_view_v1")
        result["source_of_truth"] = PIPELINE_ISSUE_SOURCE_OF_TRUTH
        result["source_of_truth_endpoint"] = "/api/v1/validation/issues/candidates"
        result["draft_storage_authoritative"] = False
        result["retired_draft_tables"] = ["assistant_issue_candidates", "assistant_validation_discovery_reports"]
        result["assistant_draft_storage_notice"] = RA_DRAFT_STORAGE_NOTICE
        result["official_submission_required"] = RA_OFFICIAL_WORKFLOW_NOTICE
        result["assistant_draft_substitution_blocked"] = True
        result.setdefault("data_state", "complete")
        return result

    @classmethod
    def _degraded_pipeline_issue_candidate_page(cls, exc: BaseException, *, page: int, page_size: int) -> dict[str, Any]:
        reason = f"{PIPELINE_ISSUE_FACT_SOURCE_UNAVAILABLE}: {type(exc).__name__}: {exc}"
        logger.warning("Validation issue fact source unavailable for RA candidate view: %s", reason)
        return cls._with_pipeline_issue_metadata(
            {
                "items": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "has_more": False,
                "data_state": "degraded",
                "status": "degraded",
                "reason_codes": [PIPELINE_ISSUE_FACT_SOURCE_UNAVAILABLE],
                "warnings": [reason],
                "error": {"reason_code": PIPELINE_ISSUE_FACT_SOURCE_UNAVAILABLE, "exception_type": type(exc).__name__, "message": str(exc)},
            }
        )

    @staticmethod
    def _assistant_issue_candidate_view(item: dict[str, Any]) -> dict[str, Any]:
        candidate_id = str(item.get("candidate_id") or item.get("fingerprint") or item.get("source_path") or "missing_candidate_id")
        source_ref = f"validation_issue_candidates:{candidate_id}"
        evidence_refs = item.get("evidence_refs") if isinstance(item.get("evidence_refs"), list) else []
        view = dict(item)
        view.setdefault("candidate_id", candidate_id)
        view.setdefault("module", item.get("module_id") or item.get("module"))
        view.setdefault("problem_statement", item.get("actual") or item.get("summary") or item.get("expected") or item.get("title"))
        view.setdefault("github_sync_status", "standard_workflow_required")
        view["source_ref"] = source_ref
        view["source_refs"] = [source_ref, *[str(ref) for ref in evidence_refs if str(ref or "").strip()]]
        view["source_of_truth"] = PIPELINE_ISSUE_SOURCE_OF_TRUTH
        view["draft_storage_authoritative"] = False
        view["assistant_draft_storage_notice"] = RA_DRAFT_STORAGE_NOTICE
        view["official_submission_required"] = RA_OFFICIAL_WORKFLOW_NOTICE
        view["direct_github_create_performed"] = False
        return view

    def list_llm_usage_events(
        self,
        *,
        trace_id: str | None = None,
        task_id: str | None = None,
        conversation_id: str | None = None,
        model: str | None = None,
        provider: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> dict[str, Any]:
        filters = {
            "trace_id": trace_id,
            "task_id": task_id,
            "conversation_id": conversation_id,
            "model": model,
            "provider": provider,
        }
        resolved_limit = int(limit) if limit is not None else self.configured_limit("api_list_llm_usage_events")
        if resolved_limit < 1:
            raise ValueError("limit must be positive")
        max_limit = self.configured_limit("api_list_max_page_size")
        if resolved_limit > max_limit:
            raise ValueError(f"limit exceeds configured api_list_max_page_size: {max_limit}")
        if hasattr(self.repository, "list_llm_usage_events"):
            page = self.repository.list_llm_usage_events(
                filters=filters,
                date_from=date_from,
                date_to=date_to,
                limit=resolved_limit,
                offset=offset,
            )
        else:
            page = self.repository.list_records("llm_usage_events", filters=filters, limit=resolved_limit, offset=offset)
        page.setdefault("source_of_truth", "assistant_llm_usage_events")
        page.setdefault("prompt_text_retained", False)
        return page

    def llm_usage_summary(
        self,
        *,
        trace_id: str | None = None,
        task_id: str | None = None,
        conversation_id: str | None = None,
        model: str | None = None,
        provider: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        page = self.list_llm_usage_events(
            trace_id=trace_id,
            task_id=task_id,
            conversation_id=conversation_id,
            model=model,
            provider=provider,
            date_from=date_from,
            date_to=date_to,
            limit=limit or self.configured_limit("api_list_llm_usage_events"),
        )
        filters = {
            "trace_id": trace_id,
            "task_id": task_id,
            "conversation_id": conversation_id,
            "model": model,
            "provider": provider,
        }
        summary = (
            self.repository.summarize_llm_usage_events(filters=filters, date_from=date_from, date_to=date_to)
            if hasattr(self.repository, "summarize_llm_usage_events")
            else self._llm_usage_summary_from_events(page["items"])
        )
        return {
            "schema_version": "aistock_research_assistant_llm_usage_summary_v1",
            "source_of_truth": "assistant_llm_usage_events",
            "filters": {
                "trace_id": trace_id,
                "task_id": task_id,
                "conversation_id": conversation_id,
                "model": model,
                "provider": provider,
                "date_from": date_from,
                "date_to": date_to,
            },
            "summary": summary,
            "events_page": page,
        }

    def llm_usage_report(
        self,
        *,
        trace_id: str | None = None,
        task_id: str | None = None,
        conversation_id: str | None = None,
        model: str | None = None,
        provider: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        granularity: str = "day",
        timezone_name: str = "Asia/Shanghai",
        limit_models: int = 8,
    ) -> dict[str, Any]:
        if granularity not in {"hour", "day"}:
            raise ValueError(f"invalid_granularity: {granularity}; expected hour or day")
        try:
            ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"invalid_timezone: {timezone_name}") from exc
        if limit_models < 1:
            raise ValueError("limit_models must be positive")
        filters = {
            "trace_id": trace_id,
            "task_id": task_id,
            "conversation_id": conversation_id,
            "model": model,
            "provider": provider,
        }
        page = self.list_llm_usage_events(
            trace_id=trace_id,
            task_id=task_id,
            conversation_id=conversation_id,
            model=model,
            provider=provider,
            date_from=date_from,
            date_to=date_to,
            limit=self.configured_limit("api_list_max_page_size"),
        )
        summary = (
            self.repository.summarize_llm_usage_events(filters=filters, date_from=date_from, date_to=date_to)
            if hasattr(self.repository, "summarize_llm_usage_events")
            else self._llm_usage_summary_from_events(page["items"])
        )
        report_parts = self.repository.report_llm_usage_events(
            filters=filters,
            date_from=date_from,
            date_to=date_to,
            granularity=granularity,
            timezone_name=timezone_name,
            limit_models=limit_models,
        )
        status_breakdown = self._llm_usage_status_breakdown_from_report(report_parts.get("time_series") or [], page.get("items") or [])
        summary = dict(summary)
        summary["usage_status"] = self._llm_usage_rollup_status(status_breakdown["usage"])
        summary["cost_status"] = self._llm_usage_rollup_status(status_breakdown["cost"])
        return {
            "schema_version": "aistock_research_assistant_llm_usage_report_v1",
            "source_of_truth": "assistant_llm_usage_events",
            "filters": {
                "trace_id": trace_id,
                "task_id": task_id,
                "conversation_id": conversation_id,
                "model": model,
                "provider": provider,
                "date_from": date_from,
                "date_to": date_to,
                "granularity": granularity,
                "timezone": timezone_name,
                "limit_models": limit_models,
            },
            "summary": summary,
            "time_series": report_parts.get("time_series") or [],
            "model_breakdown": report_parts.get("model_breakdown") or [],
            "status_breakdown": status_breakdown,
            "prompt_text_retained": False,
            "degraded": False,
            "reason_code": None,
        }

    @staticmethod
    def _llm_usage_status_breakdown_from_report(time_series: list[dict[str, Any]], events: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
        usage = {"recorded": 0, "estimated": 0, "unavailable": 0, "failed": 0}
        cost = {"recorded": 0, "estimated": 0, "unavailable": 0, "failed": 0}
        used_bucket_counts = False
        for bucket in time_series:
            usage_counts = bucket.get("usage_status_counts")
            cost_counts = bucket.get("cost_status_counts")
            if isinstance(usage_counts, dict):
                used_bucket_counts = True
                for key, value in usage_counts.items():
                    usage[str(key)] = usage.get(str(key), 0) + int(value or 0)
            if isinstance(cost_counts, dict):
                used_bucket_counts = True
                for key, value in cost_counts.items():
                    cost[str(key)] = cost.get(str(key), 0) + int(value or 0)
        if used_bucket_counts:
            return {"usage": usage, "cost": cost}
        for event in events:
            usage_status = str(event.get("usage_status") or "unavailable")
            cost_status = str(event.get("cost_status") or "unavailable")
            usage[usage_status] = usage.get(usage_status, 0) + 1
            cost[cost_status] = cost.get(cost_status, 0) + 1
        return {"usage": usage, "cost": cost}

    @staticmethod
    def _llm_usage_rollup_status(counts: dict[str, int]) -> str:
        present = {key for key, value in counts.items() if int(value or 0) > 0}
        if not present:
            return "unavailable"
        if len(present) == 1:
            return next(iter(present))
        return "mixed"

    @staticmethod
    def _usage_event_from_turn(
        turn: ModelTurn,
        *,
        trace_id: str,
        task_id: str,
        conversation_id: str,
        model_profile_id: str | None,
        call_group_id: str,
        call_index: int,
    ) -> dict[str, Any]:
        payload = dict(turn.usage_event or {})
        usage = dict(turn.usage or {})
        if not payload:
            prompt_tokens = _as_nonnegative_int(usage.get("prompt_tokens"))
            completion_tokens = _as_nonnegative_int(usage.get("completion_tokens"))
            total_tokens = _as_nonnegative_int(usage.get("total_tokens"))
            if total_tokens is None and prompt_tokens is not None and completion_tokens is not None:
                total_tokens = prompt_tokens + completion_tokens
            has_usage = any(value is not None for value in (prompt_tokens, completion_tokens, total_tokens))
            payload = {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
                "reasoning_tokens": _as_nonnegative_int(usage.get("reasoning_tokens")),
                "cache_creation_input_tokens": _as_nonnegative_int(usage.get("cache_creation_input_tokens")),
                "cache_read_input_tokens": _as_nonnegative_int(usage.get("cache_read_input_tokens")),
                "prompt_tokens_estimated": bool(usage.get("prompt_tokens_estimated")),
                "completion_tokens_estimated": bool(usage.get("completion_tokens_estimated")),
                "usage_source": str(usage.get("usage_source") or ("provider_reported" if has_usage else "unavailable")),
                "usage_status": str(usage.get("usage_status") or ("recorded" if has_usage else "unavailable")),
                "usage_reason_code": usage.get("usage_reason_code") if has_usage else "llm_result_usage_missing",
                "usage_raw_json": usage if usage else {"usage_missing": True, "reason_code": "llm_result_usage_missing"},
                "cost_source": str(usage.get("cost_source") or "unavailable"),
                "cost_status": str(usage.get("cost_status") or "unavailable"),
                "cost_reason_code": usage.get("cost_reason_code") or "cost_not_calculated_for_injected_llm_result",
                "currency": str(usage.get("currency") or "USD"),
                "pricing_snapshot_json": {"source": "injected_llm_result", "cost_calculated": False},
                "request_meta_json": {"prompt_text_retained": False, "source": "injected_llm_result"},
                "response_meta_json": _build_response_meta(turn.content, list(turn.tool_calls or [])),
            }
        payload.setdefault("provider", turn.provider)
        payload.setdefault("model", turn.model)
        payload.setdefault("litellm_model", turn.model)
        payload.setdefault("component", "research_assistant.llm")
        payload.setdefault("phase", "react_iteration")
        payload.setdefault("duration_ms", turn.duration_ms)
        payload.setdefault("currency", "USD")
        payload.setdefault("request_meta_json", {"prompt_text_retained": False})
        payload.setdefault("response_meta_json", _build_response_meta(turn.content, list(turn.tool_calls or [])))
        payload.setdefault("pricing_snapshot_json", {})
        payload.setdefault("usage_raw_json", usage if usage else {})
        return {
            "usage_event_id": new_id("llmu"),
            "trace_id": trace_id,
            "task_id": task_id,
            "conversation_id": conversation_id,
            "call_group_id": call_group_id,
            "call_index": call_index,
            "model_profile_id": model_profile_id,
            **payload,
        }

    def _record_llm_usage_events_for_trace(
        self,
        *,
        trace: dict[str, Any],
        task_id: str,
        conversation_id: str,
        model_profile_id: str | None,
        react_result: ReactGroundingResult,
    ) -> dict[str, Any]:
        turns = [
            turn
            for turn in react_result.model_turns
            if turn.provider != "route_seed" and (turn.usage_event or turn.usage or turn.duration_ms or turn.model)
        ]
        recorded: list[dict[str, Any]] = []
        for index, turn in enumerate(turns, start=1):
            row = self._usage_event_from_turn(
                turn,
                trace_id=str(trace["trace_id"]),
                task_id=task_id,
                conversation_id=conversation_id,
                model_profile_id=model_profile_id,
                call_group_id=task_id,
                call_index=index,
            )
            row["phase"] = "initial_chat" if index == 1 else "react_iteration"
            recorded.append(self.repository.create_record("llm_usage_events", row))
        summary = self._llm_usage_summary_from_events(recorded)
        return {
            "usage_summary": summary,
            "usage_event_refs": [f"assistant_llm_usage_events:{row['usage_event_id']}" for row in recorded],
            "source_of_truth": "assistant_llm_usage_events",
            "prompt_text_retained": False,
        }

    def _record_llm_usage_accounting_failure(
        self,
        *,
        trace_id: str,
        task_id: str,
        exc: BaseException,
    ) -> dict[str, Any]:
        reason = {
            "status": "failed",
            "reason_code": "llm_usage_accounting_failed",
            "exception_type": type(exc).__name__,
            "message": str(exc),
            "source_of_truth": "assistant_llm_usage_events",
            "prompt_text_retained": False,
        }
        logger.exception("Research Assistant LLM usage accounting failed: trace_id=%s task_id=%s", trace_id, task_id)
        self.add_task_event(
            task_id,
            TaskEventCreate(
                event_type="llm_usage_accounting_failed",
                severity="warning",
                message=f"LLM usage accounting failed: {type(exc).__name__}: {exc}",
                payload_json={"trace_id": trace_id, **reason},
            ),
        )
        return {"usage_summary": reason, "source_of_truth": "assistant_llm_usage_events", "usage_event_refs": []}

    @staticmethod
    def _llm_usage_summary_from_events(events: list[dict[str, Any]]) -> dict[str, Any]:
        totals = {
            "call_count": len(events),
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "reasoning_tokens": 0,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0,
            "estimated_usage_event_count": 0,
            "unavailable_usage_event_count": 0,
            "unavailable_cost_event_count": 0,
            "failed_cost_event_count": 0,
            "total_cost_usd": 0.0,
            "currency": "USD",
            "usage_status": "recorded" if events else "unavailable",
            "cost_status": "recorded" if events else "unavailable",
        }
        cost_available = False
        for event in events:
            for key in ("prompt_tokens", "completion_tokens", "total_tokens", "reasoning_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"):
                totals[key] += int(event.get(key) or 0)
            if event.get("prompt_tokens_estimated") or event.get("completion_tokens_estimated") or event.get("usage_status") == "estimated":
                totals["estimated_usage_event_count"] += 1
            if event.get("usage_status") in {"unavailable", "failed"}:
                totals["unavailable_usage_event_count"] += 1
            if event.get("cost_status") == "unavailable":
                totals["unavailable_cost_event_count"] += 1
            if event.get("cost_status") == "failed":
                totals["failed_cost_event_count"] += 1
            if event.get("total_cost_usd") is not None:
                cost_available = True
                totals["total_cost_usd"] += float(event.get("total_cost_usd") or 0)
        if totals["unavailable_usage_event_count"]:
            totals["usage_status"] = "unavailable"
        elif totals["estimated_usage_event_count"]:
            totals["usage_status"] = "estimated"
        if totals["failed_cost_event_count"]:
            totals["cost_status"] = "failed"
        elif totals["unavailable_cost_event_count"]:
            totals["cost_status"] = "unavailable"
        elif not cost_available:
            totals["cost_status"] = "unavailable"
        totals["total_cost_usd"] = f"{totals['total_cost_usd']:.10f}" if cost_available else None
        return totals

    @staticmethod
    def _default_query_limit_key(kind: str) -> str:
        if kind == "mcp_tools":
            return "api_list_mcp_tools"
        return f"api_list_{kind}"

    def create_conversation(self, request: ConversationCreate | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, ConversationCreate) else ConversationCreate(**request)
        return self.repository.create_record("conversations", {"conversation_id": new_id("conv"), **data.model_dump()})

    def get_conversation(self, conversation_id: str) -> dict[str, Any]:
        conversation = self.repository.get_record("conversations", conversation_id)
        if not conversation:
            raise KeyError(f"conversation not found: {conversation_id}")
        messages = self.repository.list_records("conversation_messages", filters={"conversation_id": conversation_id}, limit=self.configured_limit("conversation_messages_full"))["items"]
        messages.sort(key=lambda item: str(item.get("created_at") or ""))
        return {"conversation": conversation, "messages": messages}

    def add_conversation_message(self, request: ConversationMessageCreate | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, ConversationMessageCreate) else ConversationMessageCreate(**request)
        if not self.repository.get_record("conversations", data.conversation_id):
            raise KeyError(f"conversation not found: {data.conversation_id}")
        row = {"message_id": new_id("msg"), **data.model_dump()}
        message = self.repository.create_record("conversation_messages", row)
        self.repository.update_record("conversations", data.conversation_id, {"metadata_json": {"last_role": data.role}})
        return message

    def build_prompt_bundle(self, request: PromptBundleBuildRequest | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, PromptBundleBuildRequest) else PromptBundleBuildRequest(**request)
        self.ensure_catalog_ready()
        dialogue_intent = self._classify_dialogue_intent(data.user_message)
        mode_decision = data.mode_decision or self._decide_dialogue_mode(
            data.user_message,
            dialogue_intent=dialogue_intent,
            phase=data.phase,
            allow_execute=False,
            risk_level="medium",
            override=data.dialogue_mode,
        ).as_dict()
        dialogue_mode = str(data.dialogue_mode or mode_decision.get("mode") or self._dialogue_modes_config().get("default_mode") or DialogueMode.DIALOGUE.value)
        data.mode_decision = mode_decision
        data.dialogue_mode = dialogue_mode
        activation = self.active_prompt_activation()
        version_refs = list(activation.get("version_refs") or [])
        available = [
            item
            for item in self.declarative_config.prompt_node_list()
            if str(item.get("status") or "enabled") == "enabled"
        ]
        if not available:
            raise RuntimeError("Prompt Tree is empty; run /research-assistant/catalogs/seed before chat")
        selected = self._select_prompt_nodes(available, data)
        if not selected:
            raise RuntimeError("Prompt Tree selection returned no nodes")
        node_refs = [
            {
                "prompt_node_id": item["prompt_node_id"],
                "prompt_key": item["prompt_key"],
                "version": item.get("version"),
                "checksum": item.get("checksum"),
                "tree_path": item.get("tree_path"),
                "phase": item.get("phase"),
            }
            for item in selected
        ]
        bundle_text = "\n\n".join(f"### {item['title']}\n{item['prompt_text']}" for item in selected)
        bundle_json = {
            "phase": data.phase,
            "dialogue_mode": dialogue_mode,
            "node_count": len(selected),
            "prompt_keys": [item["prompt_key"] for item in selected],
            "user_message_digest": sha256_json({"message": data.user_message}),
        }
        selection_trace = {
            "algorithm": "mode_routed_prompt_tree_v1",
            "dialogue_intent": dialogue_intent.value,
            "dialogue_mode": dialogue_mode,
            "mode_decision": mode_decision,
            "phase": data.phase,
            "prompt_activation_id": activation["activation_id"],
            "prompt_bundle_signature": activation.get("bundle_signature"),
            "matched_prompt_keys": bundle_json["prompt_keys"],
            "required_prompt_keys": data.required_prompt_keys,
            "cache_enabled": data.cache_enabled,
        }
        checksum = sha256_json({"node_refs": node_refs, "bundle_text": bundle_text, "phase": data.phase, "model_profile_id": data.model_profile_id})
        cache_path = self._write_prompt_cache(checksum, bundle_text, bundle_json, selection_trace) if data.cache_enabled else None
        row = {
            "prompt_bundle_id": new_id("pbundle"),
            "task_id": data.task_id,
            "conversation_id": data.conversation_id,
            "phase": data.phase,
            "model_profile_id": data.model_profile_id,
            "activation_id": activation["activation_id"],
            "version_refs": version_refs,
            "node_refs": node_refs,
            "selection_trace_json": selection_trace,
            "bundle_json": bundle_json,
            "bundle_text": bundle_text,
            "checksum": checksum,
            "cache_path": cache_path,
        }
        bundle = self.repository.create_record("prompt_bundles", row)
        if data.task_id:
            self.add_task_event(
                data.task_id,
                TaskEventCreate(
                    event_type="prompt_bundle_built",
                    message=self._dialogue_event_message("prompt_bundle_built"),
                    payload_json={"prompt_bundle_id": bundle["prompt_bundle_id"], "prompt_keys": bundle_json["prompt_keys"], "checksum": checksum},
                ),
            )
        return bundle

    def _select_prompt_nodes(self, available: list[dict[str, Any]], data: PromptBundleBuildRequest) -> list[dict[str, Any]]:
        by_key = {str(item["prompt_key"]): item for item in available}
        intent = self._classify_dialogue_intent(data.user_message)
        mode = str(data.dialogue_mode or (data.mode_decision or {}).get("mode") or self._dialogue_modes_config().get("default_mode") or DialogueMode.DIALOGUE.value)
        mode_cfg = self._dialogue_mode_config(mode)
        selected_keys: set[str] = set(data.required_prompt_keys)
        configured_prompt_nodes = mode_cfg.get("prompt_nodes")
        if isinstance(configured_prompt_nodes, list) and configured_prompt_nodes:
            selected_keys.update(str(key) for key in configured_prompt_nodes)
        else:
            selected_keys.update({"root.assistant", f"mode.{mode}"})
        task_modes = {DialogueMode.PLANNING.value, DialogueMode.PREFLIGHT.value, DialogueMode.EXECUTION.value}
        qe_prompt_intent = intent in {
            DialogueIntent.EXPERIMENT_DRAFT_REQUEST,
            DialogueIntent.EXPERIMENT_VALIDATION_REQUEST,
            DialogueIntent.EXPERIMENT_EXECUTION_REQUEST,
        } or ("qe" in data.user_message.lower() and "template" in data.user_message.lower())
        if mode in task_modes and qe_prompt_intent:
            selected_keys.add("domain.qe_experiment")
            if qe_prompt_intent:
                selected_keys.add("workflow.qe_draft_then_approval")
            if mode in {DialogueMode.PREFLIGHT.value, DialogueMode.EXECUTION.value} or intent in {DialogueIntent.EXPERIMENT_VALIDATION_REQUEST, DialogueIntent.EXPERIMENT_EXECUTION_REQUEST} or ("template" in data.user_message.lower() and any(token in data.user_message.lower() for token in ("validate", "??", "??"))):
                selected_keys.add("tool_guard.mcp_qe")
                selected_keys.add("mode.preflight")
        if mode in task_modes and intent == DialogueIntent.LOCAL_DATA_MANAGEMENT_REQUEST:
            selected_keys.add("prompt.local_data_management")
            selected_keys.add("workflow.local_data_check_repair")
            selected_keys.add("tool_guard.mcp_local_data")
        prompt_key = domain_prompt_key(intent)
        if prompt_key:
            selected_keys.add(prompt_key)
            selected_keys.add("tool_guard.mcp_payload_budget")
            selected_keys.add("tool_guard.mcp_all")
        if mode in task_modes and intent in {DialogueIntent.ISSUE_INTAKE_REQUEST, DialogueIntent.EXPERIMENT_EXECUTION_REQUEST, DialogueIntent.VALIDATION_ISSUE_REQUEST}:
            selected_keys.add("governance.no_silent_action")
        if data.phase in {"result", "preflight"}:
            selected_keys.add("renderer.human_cards")
            selected_keys.add("renderer.humanized_response")
        closed_keys: set[str] = set()
        for key in list(selected_keys):
            current = by_key.get(key)
            while current:
                current_key = str(current["prompt_key"])
                closed_keys.add(current_key)
                parent_key = current.get("parent_key")
                current = by_key.get(str(parent_key)) if parent_key else None
        ordered = [item for item in available if str(item["prompt_key"]) in closed_keys]
        ordered.sort(key=lambda item: (str(item.get("tree_path") or ""), str(item.get("prompt_key") or "")))
        return ordered

    def _dialogue_intent_config(self) -> dict[str, Any]:
        config = self.active_runtime_config().get(DIALOGUE_INTENT_CONFIG_KEY, {})
        if not isinstance(config, dict):
            return {}
        return {str(key): value for key, value in config.items()}

    def _dialogue_modes_config(self) -> dict[str, Any]:
        config = self.active_runtime_config().get(DIALOGUE_MODES_CONFIG_KEY, {})
        if not isinstance(config, dict):
            return {}
        return {str(key): value for key, value in config.items()}

    def _dialogue_mode_config(self, mode: str) -> dict[str, Any]:
        modes = self._dialogue_modes_config().get("modes", {})
        if not isinstance(modes, dict):
            return {}
        cfg = modes.get(mode, {})
        return dict(cfg) if isinstance(cfg, dict) else {}

    def _mode_router_config(self) -> dict[str, Any]:
        config = self.active_runtime_config().get(MODE_ROUTER_CONFIG_KEY, {})
        if not isinstance(config, dict):
            return {}
        return {str(key): value for key, value in config.items()}

    def _dialogue_event_message(self, event_type: str) -> str:
        event_messages = self._dialogue_intent_config().get("event_messages", {})
        if not isinstance(event_messages, dict):
            return event_type
        return str(event_messages.get(event_type) or event_type)

    @staticmethod
    def _has_any(text: str, terms: list[str]) -> bool:
        return any(term.lower() in text for term in terms)

    def _has_explicit_task_verb(self, text: str, intent_config: dict[str, list[str]]) -> bool:
        return self._has_any(text, intent_config.get("explicit_task_verbs", []))

    def _decide_dialogue_mode(
        self,
        user_message: str,
        *,
        dialogue_intent: DialogueIntent,
        phase: str,
        allow_execute: bool,
        risk_level: str,
        override: str | None = None,
    ) -> ModeDecision:
        lower = user_message.lower()
        router_cfg = self._mode_router_config()
        overrides = router_cfg.get("user_overrides", {}) if isinstance(router_cfg.get("user_overrides"), dict) else {}
        thresholds = router_cfg.get("confidence_thresholds", {}) if isinstance(router_cfg.get("confidence_thresholds"), dict) else {}

        if override:
            mode = DialogueMode(override)
            reason = "user_override"
            confidence = 1.0
        elif self._has_any(lower, list(overrides.get("audit_patterns", []))):
            mode = DialogueMode.AUDIT
            reason = "audit_pattern"
            confidence = 0.92
        elif self._has_any(lower, list(overrides.get("analysis_only_patterns", []))):
            mode = DialogueMode.ANALYSIS
            reason = "analysis_only_override"
            confidence = 0.95
        elif allow_execute or self._has_any(lower, list(overrides.get("execute_patterns", []))):
            mode = DialogueMode.EXECUTION
            reason = "execution_request_requires_existing_proposal"
            confidence = float(thresholds.get("execution_request_min", 0.86))
        elif phase == "preflight" or dialogue_intent in {DialogueIntent.EXPERIMENT_VALIDATION_REQUEST, DialogueIntent.EXPERIMENT_EXECUTION_REQUEST}:
            mode = DialogueMode.PREFLIGHT
            reason = "explicit_preflight_or_validation_request"
            confidence = float(thresholds.get("task_request_min", 0.72))
        elif dialogue_intent in {
            DialogueIntent.EXPERIMENT_DRAFT_REQUEST,
            DialogueIntent.ISSUE_INTAKE_REQUEST,
            DialogueIntent.LOCAL_DATA_MANAGEMENT_REQUEST,
            DialogueIntent.QE_WAREHOUSE_REQUEST,
            DialogueIntent.RESEARCH_PIPELINE_REQUEST,
            DialogueIntent.VALIDATION_ISSUE_REQUEST,
            DialogueIntent.FACTOR_LIBRARY_REQUEST,
            DialogueIntent.FACTOR_METRICS_REQUEST,
            DialogueIntent.FACTOR_CORRELATION_REQUEST,
            DialogueIntent.MODEL_REGISTRY_REQUEST,
            DialogueIntent.STRATEGY_GOVERNANCE_REQUEST,
            DialogueIntent.EXECUTION_POLICY_REQUEST,
            DialogueIntent.EXTERNAL_RESEARCH_REQUEST,
            DialogueIntent.STOCK_ANALYSIS_REQUEST,
        }:
            mode = DialogueMode.PLANNING
            reason = "explicit_task_request"
            confidence = float(thresholds.get("task_request_min", 0.72))
        elif dialogue_intent in {DialogueIntent.BUG_DIAGNOSIS_REQUEST, DialogueIntent.CONCEPT_EXPLANATION, DialogueIntent.STATUS_QUERY, DialogueIntent.AMBIGUOUS_REQUEST}:
            mode = DialogueMode.ANALYSIS
            reason = "read_only_analysis_intent"
            confidence = float(thresholds.get("direct_answer_min", 0.55))
        else:
            mode = DialogueMode.DIALOGUE
            reason = "direct_answer_intent"
            confidence = float(thresholds.get("direct_answer_min", 0.55))

        mode_cfg = self._dialogue_mode_config(mode.value)
        allowed_side_effect = str(mode_cfg.get("allowed_tool_side_effect") or "none")
        requires_approval = bool(mode_cfg.get("approval_required")) or risk_level == "production_sensitive"
        requires_confirmation = mode in {DialogueMode.PREFLIGHT, DialogueMode.EXECUTION} or bool(mode_cfg.get("approval_required"))
        requires_tool = mode in {DialogueMode.PREFLIGHT, DialogueMode.EXECUTION}
        visible_audit = bool(mode_cfg.get("expose_audit_fields")) and mode not in {DialogueMode.DIALOGUE, DialogueMode.ANALYSIS}
        return ModeDecision(
            mode=mode,
            intent_type=dialogue_intent,
            confidence=round(confidence, 3),
            mode_reason=reason,
            requires_tool=requires_tool,
            allowed_tool_side_effect=allowed_side_effect,
            requires_user_confirmation=requires_confirmation,
            requires_approval=requires_approval,
            visible_audit_default=visible_audit,
        )

    def _classify_dialogue_intent(self, user_message: str) -> DialogueIntent:
        lower = user_message.lower()
        intent_config = self._dialogue_intent_config()
        router_cfg = self._mode_router_config()
        overrides = router_cfg.get("user_overrides", {}) if isinstance(router_cfg.get("user_overrides"), dict) else {}
        if self._has_any(lower, list(overrides.get("audit_patterns", []))):
            return DialogueIntent.AUDIT_REQUEST
        if "qe" in lower and "template" in lower:
            return DialogueIntent.EXPERIMENT_VALIDATION_REQUEST

        # Let the unified MCP router choose specific business domains before broad
        # capability-inquiry and local-data keyword fallbacks.
        route = route_request(user_message)
        intent_value = route.get("intent_value")
        if intent_value and str(route.get("domain") or "") != "mcp_capability":
            route_domain = str(route.get("domain") or "")
            concrete_business_route = route_domain not in {"", "general", "validation_issue", "qe_experiment"}
            if intent_value == DialogueIntent.LOCAL_DATA_MANAGEMENT_REQUEST.value or concrete_business_route:
                try:
                    return DialogueIntent(str(intent_value))
                except ValueError:
                    pass

        asks_capability = self._has_any(lower, intent_config.get("capability_inquiry_patterns", []))
        if self._is_mcp_tool_catalog_inquiry(lower):
            return DialogueIntent.CAPABILITY_INQUIRY
        if asks_capability:
            return DialogueIntent.CAPABILITY_INQUIRY
        if intent_value:
            try:
                return DialogueIntent(str(intent_value))
            except ValueError:
                pass

        has_qe = self._has_any(lower, intent_config.get("qe_terms", []))
        has_bug = self._has_any(lower, intent_config.get("bug_terms", []))
        has_issue = self._has_any(lower, intent_config.get("issue_terms", []))
        explicit_task = self._has_explicit_task_verb(lower, intent_config)
        asks_concept = self._has_any(lower, intent_config.get("concept_explanation_patterns", []))
        asks_status = self._has_any(lower, intent_config.get("status_query_patterns", []))

        if has_bug and (explicit_task or asks_concept or asks_status):
            return DialogueIntent.BUG_DIAGNOSIS_REQUEST
        if asks_concept:
            return DialogueIntent.CONCEPT_EXPLANATION
        if asks_status and not explicit_task:
            return DialogueIntent.STATUS_QUERY
        if has_issue and explicit_task:
            return DialogueIntent.ISSUE_INTAKE_REQUEST
        if has_qe and explicit_task:
            if self._has_any(lower, intent_config.get("execution_terms", [])) and not self._has_any(lower, intent_config.get("negated_execution_patterns", [])):
                return DialogueIntent.EXPERIMENT_EXECUTION_REQUEST
            if self._has_any(lower, intent_config.get("validation_terms", [])):
                return DialogueIntent.EXPERIMENT_VALIDATION_REQUEST
            return DialogueIntent.EXPERIMENT_DRAFT_REQUEST
        if explicit_task:
            return DialogueIntent.AMBIGUOUS_REQUEST
        return DialogueIntent.GENERAL_CHAT

    @staticmethod
    def _write_prompt_cache(checksum: str, bundle_text: str, bundle_json: dict[str, Any], selection_trace: dict[str, Any]) -> str:
        PROMPT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        path = PROMPT_CACHE_DIR / f"{checksum}.json"
        payload = {"bundle_text": bundle_text, "bundle_json": bundle_json, "selection_trace_json": selection_trace}
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(path)

    def get_task(self, task_id: str) -> dict[str, Any]:
        task = self.repository.get_record("tasks", task_id)
        if not task:
            raise KeyError(f"task not found: {task_id}")
        events = self.repository.list_records("task_events", filters={"task_id": task_id}, limit=self.configured_limit("task_events_detail"))["items"]
        return {"task": task, "events": events}

    def create_task(self, request: TaskCreate | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, TaskCreate) else TaskCreate(**request)
        if data.idempotency_key:
            existing = self.repository.find_one("tasks", {"idempotency_key": data.idempotency_key})
            if existing:
                return existing
        task_id = new_id("rat")
        row = data.model_dump()
        row.update({"task_id": task_id, "status": "planned"})
        task = self.repository.create_record("tasks", row)
        self.add_task_event(task_id, TaskEventCreate(event_type="planned", message=f"任务已创建：{data.title}", payload_json={"input": data.input_json}))
        return task


    def chat_turn(self, request: ChatTurnRequest | dict[str, Any]) -> dict[str, Any]:
        try:
            return self._chat_turn_impl(request)
        except ResearchAssistantCatalogNotReadyError:
            raise
        except ResearchAssistantRuntimeConfigInvalidError as exc:
            logger.error(
                "research assistant chat_turn stopped by invalid runtime config: reason_code=%s activation_id=%s config_key=%s",
                exc.error_payload.get("reason_code"),
                exc.error_payload.get("activation_id"),
                exc.error_payload.get("config_key"),
            )
            return self._chat_turn_error_response(request, exc)
        except RuntimeError as exc:
            if str(exc).startswith("High-risk Research Assistant task stopped"):
                raise
            logger.exception("research assistant chat_turn failed; returning explicit structured error")
            return self._chat_turn_error_response(request, exc)
        except Exception as exc:  # noqa: BLE001 - chat/turn must return an explicit error message, not HTTP 4xx/5xx.
            logger.exception("research assistant chat_turn failed; returning explicit structured error")
            return self._chat_turn_error_response(request, exc)

    def _chat_turn_impl(self, request: ChatTurnRequest | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, ChatTurnRequest) else ChatTurnRequest(**request)
        self.ensure_catalog_ready()
        runtime_activation = self.active_runtime_config_activation()
        runtime_config = self.active_runtime_config()
        conversation = (
            self.repository.get_record("conversations", data.conversation_id)
            if data.conversation_id
            else self.create_conversation(ConversationCreate(title=self._conversation_title(data.message), user_id=data.user_id))
        )
        if not conversation:
            raise KeyError(f"conversation not found: {data.conversation_id}")
        conversation_id = conversation["conversation_id"]
        dialogue_intent = self._classify_dialogue_intent(data.message)
        mode_decision = self._decide_dialogue_mode(
            data.message,
            dialogue_intent=dialogue_intent,
            phase=data.phase,
            allow_execute=data.allow_execute,
            risk_level=data.risk_level,
            override=data.dialogue_mode_override,
        )
        mode_decision_json = mode_decision.as_dict()
        task = self.create_task(
            TaskCreate(
                title=self._conversation_title(data.message),
                task_type="assistant_chat_turn",
                risk_level=data.risk_level,
                input_json={
                    "user_message": data.message,
                    "phase": data.phase,
                    "allow_execute": data.allow_execute,
                    "dialogue_intent": dialogue_intent.value,
                    "dialogue_mode": mode_decision.mode.value,
                    "mode_decision": mode_decision_json,
                    "confirm_approval_id": data.confirm_approval_id,
                    "confirmation_text_present": bool(data.confirmation_text),
                },
                created_by=data.created_by,
            )
        )
        user_message = self.add_conversation_message(
            ConversationMessageCreate(
                conversation_id=conversation_id,
                role="user",
                content_text=data.message,
                task_id=task["task_id"],
                content_json={
                    "phase": data.phase,
                    "dialogue_intent": dialogue_intent.value,
                    "dialogue_mode": mode_decision.mode.value,
                    "mode_decision": mode_decision_json,
                    "confirm_approval_id": data.confirm_approval_id,
                    "confirmation_text_present": bool(data.confirmation_text),
                },
            )
        )
        self.add_task_event(
            task["task_id"],
            TaskEventCreate(
                event_type="chat_received",
                message=self._dialogue_event_message("chat_received"),
                payload_json={"conversation_id": conversation_id, "dialogue_intent": dialogue_intent.value, "dialogue_mode": mode_decision.mode.value, "mode_decision": mode_decision_json},
            ),
        )
        approval_response = self._maybe_handle_chat_approval_confirmation(
            data=data,
            conversation_id=conversation_id,
            task=task,
            user_message=user_message,
            dialogue_intent=dialogue_intent,
            mode_decision=mode_decision,
        )
        if approval_response is not None:
            return approval_response

        initial_prior_messages = self._fetch_prior_chat_messages(conversation_id, data.message, runtime_config)
        initial_overhead = int(runtime_config["model_routing"]["initial_context_overhead_tokens"])
        history_tokens = sum(self.context_budget_planner.estimate_tokens(m["content"], runtime_config) for m in initial_prior_messages)
        estimated_total_tokens = self.context_budget_planner.estimate_tokens(data.message, runtime_config) + history_tokens + initial_overhead
        route = self.route_model(ModelRouteRequest(role="primary_reasoner", risk_level=data.risk_level, token_estimate=estimated_total_tokens))
        model_profile = route.get("model_profile")
        if not model_profile:
            raise RuntimeError(f"no enabled primary model profile for risk={data.risk_level}: {route.get('route_status')}")
        route_decision = self._semantic_or_legacy_route_decision(data.message, model_profile=model_profile)
        bundle = self.build_prompt_bundle(
            PromptBundleBuildRequest(
                user_message=data.message,
                task_id=task["task_id"],
                conversation_id=conversation_id,
                phase=data.phase,
                dialogue_mode=mode_decision.mode.value,
                mode_decision=mode_decision_json,
                model_profile_id=model_profile["model_profile_id"],
            )
        )
        preliminary_budget = self.context_budget_planner.plan(
            model_profile=model_profile,
            runtime_config=runtime_config,
            prompt_bundle_text=bundle["bundle_text"],
            prior_messages=initial_prior_messages,
            current_user_message=data.message,
        )
        context_pack = self.build_context_pack(
            ContextPackBuildRequest(
                task_id=task["task_id"],
                agent_id="research_assistant_primary",
                model_profile=model_profile["model_profile_id"],
                token_budget=preliminary_budget.context_pack_budget_tokens,
                user_message=data.message,
                dialogue_intent=dialogue_intent.value,
            )
        )
        budget_plan = self.context_budget_planner.plan(
            model_profile=model_profile,
            runtime_config=runtime_config,
            prompt_bundle_text=bundle["bundle_text"],
            context_pack_summary=str(context_pack.get("pack_summary") or ""),
            prior_messages=initial_prior_messages,
            compact_summaries=self._active_context_segments(conversation_id),
            current_user_message=data.message,
        )
        prior_messages = self._prepare_prior_chat_messages(
            conversation_id=conversation_id,
            current_message=data.message,
            candidates=initial_prior_messages,
            budget_plan=budget_plan,
            model_profile=model_profile,
            bundle=bundle,
            task_id=task["task_id"],
            runtime_activation=runtime_activation,
        )
        assembly_trace = self._record_context_assembly_trace(
            conversation_id=conversation_id,
            task_id=task["task_id"],
            bundle=bundle,
            runtime_activation=runtime_activation,
            budget_plan=budget_plan,
            prior_messages=prior_messages,
            context_pack=context_pack,
        )
        messages = self._chat_messages_for_llm(data.message, bundle, context_pack, prior_messages, mode_decision=mode_decision_json, runtime_config=runtime_config)
        function_tools, function_tool_registry = self._agentic_function_tools(mode_decision)
        self.add_task_event(
            task["task_id"],
            TaskEventCreate(
                event_type="llm_started",
                message=self._dialogue_event_message("llm_started"),
                payload_json={"model_profile_id": model_profile["model_profile_id"], "prompt_bundle_id": bundle["prompt_bundle_id"]},
            ),
        )
        llm_result, messages, budget_plan, prior_messages, assembly_trace = self._complete_chat_with_reactive_recovery(
            user_message=data.message,
            conversation_id=conversation_id,
            task_id=task["task_id"],
            risk_level=data.risk_level,
            messages=messages,
            bundle=bundle,
            context_pack=context_pack,
            initial_candidates=initial_prior_messages,
            prior_messages=prior_messages,
            budget_plan=budget_plan,
            model_profile=model_profile,
            runtime_activation=runtime_activation,
            assembly_trace=assembly_trace,
            function_tools=function_tools,
            function_tool_registry=function_tool_registry,
        )
        context_health = self._context_health_payload(conversation_id, budget_plan, mode_decision=mode_decision)
        cards = self._build_human_cards(data.message, task, bundle, route, dialogue_intent, mode_decision, route_decision=route_decision)
        if isinstance(route_decision, dict) and route_decision.get("server_key") and route_decision.get("tool_name"):
            existing_route = cards.get("mcp_route_decision") if isinstance(cards.get("mcp_route_decision"), dict) else {}
            route_card = dict(existing_route)
            route_card.update(route_decision)
            side_effect = str(route_card.get("side_effect") or "read_only")
            route_card.update(
                {
                    "request": data.message,
                    "summary_first": True,
                    "preflight_required": side_effect != "read_only",
                    "confirmation_required": side_effect == "confirmed_action",
                    "ui_card": "mcp_route_decision",
                }
            )
            route_card.setdefault("auto_execute", self._read_only_mcp_auto_execution_eligibility(route_card, mode_decision))
            cards["mcp_route_decision"] = route_card
        elif isinstance(route_decision, dict) and route_decision.get("requires_clarification"):
            cards["mcp_route_decision"] = route_decision
            cards["clarification_card"] = {
                "title": "需要先确认比较口径",
                "questions": list(route_decision.get("clarification_questions") or []),
                "default_collapsed": False,
            }
        self._process_agentic_skill_calls(
            list(llm_result.skill_calls or []),
            task=task,
            conversation_id=conversation_id,
            context_pack=context_pack,
            cards=cards,
        )
        if self._should_run_react_grounding(
            user_message=data.message,
            cards=cards,
            context_pack=context_pack,
            first_llm_result=llm_result,
            mode_decision=mode_decision,
        ):
            llm_result, messages, react_result = self._complete_chat_with_react_grounding(
                user_message=data.message,
                conversation_id=conversation_id,
                task=task,
                context_pack=context_pack,
                messages=messages,
                first_llm_result=llm_result,
                cards=cards,
                model_profile=model_profile,
                budget_plan=budget_plan,
                runtime_config=runtime_config,
                mode_decision=mode_decision,
                function_tools=function_tools,
                function_tool_registry=function_tool_registry,
            )
        else:
            react_result = self._empty_react_grounding_result(llm_result)
        cards["react_grounding"] = self._react_grounding_card(react_result)
        self._populate_cards_from_react_program_error(cards, react_result, task["task_id"])
        trace = self.create_trace_event(
            TraceEventCreate(
                task_id=task["task_id"],
                event_type="llm_call",
                component="research_assistant.chat_turn",
                status="ok",
                duration_ms=llm_result.duration_ms,
                model_profile_id=model_profile["model_profile_id"],
                payload_json={
                    "provider": llm_result.provider,
                    "model": llm_result.model,
                    "prompt_bundle_id": bundle["prompt_bundle_id"],
                    "context_pack_id": context_pack["context_pack_id"],
                    "context_assembly_trace_id": assembly_trace["assembly_trace_id"],
                    "response_preview": self._preview_text(llm_result.content, budget_plan),
                    "dialogue_intent": dialogue_intent.value,
                    "dialogue_mode": mode_decision.mode.value,
                    "mode_decision": mode_decision_json,
                    "react_grounding": {
                        "iterations": react_result.iterations,
                        "tool_call_count": len(react_result.tool_calls),
                        "tool_result_count": len(react_result.tool_results),
                        "evidence_guard": react_result.evidence_guard.reason,
                        "stopped_reason": react_result.stopped_reason,
                    },
                },
                cost_json={"usage_summary": {"status": "pending"}, "source_of_truth": "assistant_llm_usage_events"},
            )
        )
        try:
            cost_json = self._record_llm_usage_events_for_trace(
                trace=trace,
                task_id=task["task_id"],
                conversation_id=conversation_id,
                model_profile_id=model_profile["model_profile_id"],
                react_result=react_result,
            )
        except Exception as exc:  # noqa: BLE001 - no silent accounting failure; chat answer still returns.
            cost_json = self._record_llm_usage_accounting_failure(trace_id=trace["trace_id"], task_id=task["task_id"], exc=exc)
        trace = self.repository.update_record("trace_events", trace["trace_id"], {"cost_json": cost_json})
        cards["context_health"] = context_health
        cards["runtime_code"] = self.runtime_code_visibility()
        assistant_text = self._compose_assistant_reply(data.message, llm_result.content, cards, mode_decision)
        assistant_message = self.add_conversation_message(
            ConversationMessageCreate(
                conversation_id=conversation_id,
                role="assistant",
                content_text=assistant_text,
                content_json={
                    "cards": cards,
                    "dialogue_intent": dialogue_intent.value,
                    "dialogue_mode": mode_decision.mode.value,
                    "mode_decision": mode_decision_json,
                    "audit_summary": {
                        "model_profile": model_profile["display_name"],
                        "prompt_bundle_checksum": bundle["checksum"],
                        "context_pack_checksum": context_pack["checksum"],
                    },
                },
                task_id=task["task_id"],
                model_profile_id=model_profile["model_profile_id"],
                prompt_bundle_id=bundle["prompt_bundle_id"],
                trace_id=trace["trace_id"],
                is_visible=True,
            )
        )
        self.add_task_event(
            task["task_id"],
            TaskEventCreate(
                event_type="llm_done",
                message=self._dialogue_event_message("llm_done"),
                payload_json={
                    "assistant_message_id": assistant_message["message_id"],
                    "trace_id": trace["trace_id"],
                    "dialogue_intent": dialogue_intent.value,
                    "dialogue_mode": mode_decision.mode.value,
                    "mode_decision": mode_decision_json,
                },
            ),
        )
        if cards.get("action_proposals"):
            self.add_task_event(
                task["task_id"],
                TaskEventCreate(
                    event_type="action_proposed",
                    severity="warning",
                    message=self._dialogue_event_message("action_proposed"),
                    payload_json={"proposal_count": len(cards.get("action_proposals", [])), "safety": cards["safety"], "dialogue_intent": dialogue_intent.value},
                ),
            )
        self._schedule_memory_curator(
            user_message=data.message,
            assistant_message=assistant_text,
            conversation_id=conversation_id,
            user_message_id=user_message["message_id"],
            assistant_message_id=assistant_message["message_id"],
            task_id=task["task_id"],
            model_profile=model_profile,
        )
        task_events = self.repository.list_records("task_events", filters={"task_id": task["task_id"]}, limit=self.configured_limit("task_events_detail"))["items"]
        public_cards = self._public_chat_cards(cards)
        return {
            "conversation": self._public_conversation(self.repository.get_record("conversations", conversation_id)),
            "user_message": self._public_conversation_message(user_message),
            "assistant_message": self._public_conversation_message(assistant_message),
            "task": self._public_task(self.repository.get_record("tasks", task["task_id"])),
            "task_events": self._public_task_events(task_events),
            "task_events_ref": {"endpoint": f"/api/v1/research-assistant/tasks/{task['task_id']}/events", "default_limit": self.configured_limit("task_events_detail")},
            "prompt_bundle": self._public_prompt_bundle(bundle),
            "context_pack": {"context_pack_id": context_pack["context_pack_id"], "pack_summary": context_pack["pack_summary"], "checksum": context_pack["checksum"]},
            "trace": {"trace_id": trace["trace_id"], "status": trace["status"], "duration_ms": trace.get("duration_ms"), "model_profile_id": trace.get("model_profile_id")},
            "mode_decision": mode_decision_json,
            "context_health": context_health,
            "cards": public_cards,
        }

    def _chat_turn_error_response(self, request: ChatTurnRequest | dict[str, Any], exc: BaseException) -> dict[str, Any]:
        data = request if isinstance(request, ChatTurnRequest) else ChatTurnRequest(**request)
        is_config_error = isinstance(exc, ResearchAssistantRuntimeConfigInvalidError)
        if is_config_error:
            error = self._chat_turn_config_error_payload(exc)
            assistant_text = self._render_runtime_config_error_reply(error)
            error_title = "Research Assistant runtime config invalid"
            task_events_detail_limit = self._fallback_task_events_detail_limit_for_config_error()
        else:
            error = self._chat_turn_unexpected_error_payload(exc)
            assistant_text = self._render_chat_turn_error_reply(error)
            error_title = "Research Assistant chat_turn failed"
            task_events_detail_limit = self.configured_limit("task_events_detail")
        cards = {
            "intent_type": "error",
            "dialogue_mode": "recovery",
            "status_rail": [{"label": "error", "status": "failed"}],
            "safety": {"no_silent_error": True, "fail_closed": is_config_error},
            "error_card": {
                "title": error_title,
                "summary": assistant_text,
                "reason_code": error["reason_code"],
                "error": error,
            },
            "mcp_execution_result": {
                "auto_executed": False,
                "executed": False,
                "status": "failed",
                "error": error,
            },
        }
        conversation: dict[str, Any] | None = None
        user_message: dict[str, Any] | None = None
        assistant_message: dict[str, Any] | None = None
        task: dict[str, Any] | None = None
        trace: dict[str, Any] | None = None
        try:
            conversation = (
                self.repository.get_record("conversations", data.conversation_id)
                if data.conversation_id
                else self.create_conversation(ConversationCreate(title=self._conversation_title(data.message), user_id=data.user_id))
            )
        except Exception:  # noqa: BLE001
            logger.exception("failed to create fallback conversation for chat_turn error")
        try:
            task = self.create_task(
                TaskCreate(
                    title=self._conversation_title(data.message),
                    task_type="assistant_chat_turn",
                    risk_level=data.risk_level,
                    input_json={"user_message": data.message, "error": error},
                    created_by=data.created_by,
                )
            )
        except Exception:  # noqa: BLE001
            logger.exception("failed to create fallback task for chat_turn error")
        if conversation:
            try:
                user_message = self.add_conversation_message(
                    ConversationMessageCreate(
                        conversation_id=str(conversation["conversation_id"]),
                        role="user",
                        content_text=data.message,
                        task_id=task.get("task_id") if isinstance(task, dict) else None,
                        content_json={"phase": data.phase, "dialogue_intent": "error", "dialogue_mode": "recovery"},
                    )
                )
            except Exception:  # noqa: BLE001
                logger.exception("failed to persist fallback user message for chat_turn error")
            try:
                assistant_message = self.add_conversation_message(
                    ConversationMessageCreate(
                        conversation_id=str(conversation["conversation_id"]),
                        role="assistant",
                        content_text=assistant_text,
                        task_id=task.get("task_id") if isinstance(task, dict) else None,
                        content_json={"cards": cards, "dialogue_intent": "error", "dialogue_mode": "recovery", "audit_summary": {"error": error}},
                    )
                )
            except Exception:  # noqa: BLE001
                logger.exception("failed to persist fallback assistant message for chat_turn error")
        if task:
            try:
                trace = self.create_trace_event(
                    TraceEventCreate(
                        task_id=task["task_id"],
                        event_type="llm_failed",
                        component="research_assistant.chat_turn",
                        status="failed",
                        payload_json={"error": error},
                    )
                )
                self.add_task_event(
                    task["task_id"],
                    TaskEventCreate(
                        event_type="llm_failed",
                        severity="error",
                        message=assistant_text,
                        payload_json={"error": error, "trace_id": trace["trace_id"]},
                    ),
                )
            except Exception:  # noqa: BLE001
                logger.exception("failed to persist fallback task event for chat_turn error")
        conversation_id = (conversation or {}).get("conversation_id") or data.conversation_id or "unpersisted_error_conversation"
        fallback_user = user_message or {
            "message_id": "unpersisted_user_error",
            "conversation_id": conversation_id,
            "role": "user",
            "content_text": data.message,
            "task_id": (task or {}).get("task_id"),
            "content_json": {"phase": data.phase, "dialogue_intent": "error", "dialogue_mode": "recovery"},
            "is_visible": True,
        }
        fallback_assistant = assistant_message or {
            "message_id": "unpersisted_assistant_error",
            "conversation_id": conversation_id,
            "role": "assistant",
            "content_text": assistant_text,
            "task_id": (task or {}).get("task_id"),
            "content_json": {"audit_summary": {"error": error}},
            "is_visible": True,
        }
        task_events = []
        if task:
            try:
                task_events = self.repository.list_records("task_events", filters={"task_id": task["task_id"]}, limit=task_events_detail_limit)["items"]
            except Exception:  # noqa: BLE001
                logger.exception(
                    "failed to list fallback task events for chat_turn error: task_id=%s reason_code=%s",
                    task.get("task_id"),
                    error.get("reason_code"),
                )
                task_events = []
        return {
            "conversation": self._public_conversation(conversation) if conversation else {"conversation_id": conversation_id, "user_id": data.user_id, "title": self._conversation_title(data.message), "status": "error"},
            "user_message": self._public_conversation_message(fallback_user),
            "assistant_message": self._public_conversation_message(fallback_assistant),
            "task": self._public_task(task),
            "task_events": self._public_task_events(task_events),
            "task_events_ref": {"endpoint": f"/api/v1/research-assistant/tasks/{(task or {}).get('task_id', 'unpersisted')}/events", "default_limit": task_events_detail_limit},
            "prompt_bundle": None,
            "context_pack": None,
            "trace": {"trace_id": (trace or {}).get("trace_id"), "status": "failed", "duration_ms": None, "model_profile_id": None},
            "mode_decision": {"mode": "recovery", "intent_type": "error"},
            "context_health": {"show_badge": False},
            "cards": self._public_chat_cards(cards),
        }

    @staticmethod
    def _conversation_title(message: str) -> str:
        return (message.strip().replace("\n", " ")[:48] or "新的对话")

    @staticmethod
    def _public_conversation(conversation: dict[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(conversation, dict):
            return None
        return {
            "conversation_id": conversation.get("conversation_id"),
            "user_id": conversation.get("user_id"),
            "title": conversation.get("title"),
            "status": conversation.get("status"),
            "created_at": conversation.get("created_at"),
            "updated_at": conversation.get("updated_at"),
        }

    @staticmethod
    def _public_conversation_message(message: dict[str, Any]) -> dict[str, Any]:
        content_json = message.get("content_json") if isinstance(message.get("content_json"), dict) else {}
        public: dict[str, Any] = {
            "message_id": message.get("message_id"),
            "conversation_id": message.get("conversation_id"),
            "role": message.get("role"),
            "content_text": message.get("content_text"),
            "task_id": message.get("task_id"),
            "prompt_bundle_id": message.get("prompt_bundle_id"),
            "trace_id": message.get("trace_id"),
            "is_visible": message.get("is_visible"),
            "created_at": message.get("created_at"),
            "updated_at": message.get("updated_at"),
        }
        if message.get("role") == "assistant":
            public["content_json"] = {
                "audit_summary": content_json.get("audit_summary") if isinstance(content_json.get("audit_summary"), dict) else {},
            }
        else:
            public["content_json"] = {
                "phase": content_json.get("phase"),
                "dialogue_intent": content_json.get("dialogue_intent"),
                "dialogue_mode": content_json.get("dialogue_mode"),
            }
        return public

    @staticmethod
    def _public_task(task: dict[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(task, dict):
            return None
        return {
            "task_id": task.get("task_id"),
            "task_type": task.get("task_type"),
            "title": task.get("title"),
            "status": task.get("status"),
            "risk_level": task.get("risk_level"),
            "created_by": task.get("created_by"),
            "created_at": task.get("created_at"),
            "updated_at": task.get("updated_at"),
            "completed_at": task.get("completed_at"),
        }

    @staticmethod
    def _public_task_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "event_id": event.get("event_id"),
                "task_id": event.get("task_id"),
                "event_type": event.get("event_type"),
                "severity": event.get("severity"),
                "message": event.get("message"),
                "created_at": event.get("created_at"),
            }
            for event in events
            if isinstance(event, dict)
        ]

    @classmethod
    def _public_chat_cards(cls, cards: dict[str, Any]) -> dict[str, Any]:
        public_keys = {
            "intent_type",
            "dialogue_mode",
            "mode_decision",
            "action_proposals",
            "capability_cards",
            "missing_capability_keys",
            "status_rail",
            "capability_summary",
            "safety",
            "main_reply_policy",
            "ui_display",
            "mcp_route_decision",
            "runtime_mcp_catalog",
            "plan_card",
            "clarification_card",
            "context_health",
            "runtime_code",
            "local_data_management",
            "local_data_phases",
            "mcp_summary_result",
            "mcp_result_cards",
            "mcp_tool_event",
            "mcp_execution_result",
            "skill_reuse_result",
            "react_grounding",
            "tool_errors",
            "error_card",
            "approval_confirmation",
        }
        public = {key: value for key, value in cards.items() if key in public_keys}
        if isinstance(public.get("mcp_summary_result"), dict):
            public["mcp_summary_result"] = cls._compact_chat_mcp_summary_result(public["mcp_summary_result"])
        if isinstance(public.get("mcp_result_cards"), list):
            public["mcp_result_cards"] = public["mcp_result_cards"][:3]
        if isinstance(public.get("mcp_execution_result"), dict):
            execution = dict(public["mcp_execution_result"])
            if isinstance(execution.get("human_cards"), list):
                execution["human_cards"] = execution["human_cards"][:3]
            public["mcp_execution_result"] = execution
        return public

    @staticmethod
    def _compact_chat_mcp_summary_result(summary: dict[str, Any]) -> dict[str, Any]:
        compact = dict(summary)
        items = compact.get("items") if isinstance(compact.get("items"), list) else []
        compact["items"] = items[:3]
        compact["items_truncated"] = max(0, len(items) - len(compact["items"]))
        if isinstance(compact.get("artifact_refs"), list):
            compact["artifact_refs"] = compact["artifact_refs"][:3]
        return compact

    @staticmethod
    def _public_prompt_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
        selection_trace = bundle.get("selection_trace_json") if isinstance(bundle.get("selection_trace_json"), dict) else {}
        return {
            "prompt_bundle_id": bundle["prompt_bundle_id"],
            "phase": bundle["phase"],
            "checksum": bundle["checksum"],
            "activation_id": bundle.get("activation_id"),
            "node_count": len(bundle.get("node_refs") or []),
            "selected_prompt_keys": [str(item.get("prompt_key")) for item in bundle.get("node_refs", []) if isinstance(item, dict)],
            "selection_trace_json": {
                "algorithm": selection_trace.get("algorithm"),
                "dialogue_intent": selection_trace.get("dialogue_intent"),
                "dialogue_mode": selection_trace.get("dialogue_mode"),
                "phase": selection_trace.get("phase"),
            },
        }

    def _chat_messages_for_llm(
        self,
        user_message: str,
        bundle: dict[str, Any],
        context_pack: dict[str, Any],
        prior_messages: list[dict[str, str]] | None = None,
        *,
        mode_decision: dict[str, Any] | None = None,
        runtime_config: dict[str, Any] | None = None,
    ) -> list[dict[str, str]]:
        system = str(bundle["bundle_text"])
        messages: list[dict[str, str]] = [
            {"role": "system", "content": system},
        ]
        mode = str((mode_decision or {}).get("mode") or DialogueMode.DIALOGUE.value)
        mode_cfg = self._dialogue_mode_config(mode)
        expose_context_pack = bool(mode_cfg.get("expose_context_pack"))
        evidence_manifest = self._context_pack_evidence_manifest(context_pack)
        if expose_context_pack:
            context = (
                f"Context Pack 摘要 / Context Pack summary: {context_pack.get('pack_summary')}\n"
                "Use the audited context below. Ask a clarifying question when evidence is missing.\n"
                f"{json.dumps(evidence_manifest, ensure_ascii=False, sort_keys=True)}"
            )
            messages.append({"role": "user", "content": context})
        else:
            messages.append(
                {
                    "role": "system",
                    "content": (
                        f"Internal Context Pack 摘要 / Context Pack summary: {context_pack.get('pack_summary')}\n"
                        "Do not mention Context Pack, memory counts, token budget, prompt bundle, or compression summaries unless the user asks for audit details.\n"
                        f"Context Pack Evidence Manifest: {json.dumps(evidence_manifest, ensure_ascii=False, sort_keys=True)}"
                    ),
                }
            )
        if prior_messages:
            messages.extend(prior_messages)
        messages.append({"role": "system", "content": AGENTIC_SYNTHESIS_SYSTEM_PROMPT})
        mcp_catalog_context = self._mcp_tool_catalog_context_for_llm(user_message)
        if mcp_catalog_context:
            messages.append({"role": "system", "content": mcp_catalog_context})
        messages.append({"role": "user", "content": user_message})
        return messages

    @staticmethod
    def _context_pack_evidence_manifest(context_pack: dict[str, Any]) -> dict[str, Any]:
        pack_json = context_pack.get("pack_json") if isinstance(context_pack.get("pack_json"), dict) else {}
        memory_route = pack_json.get("memory_route") if isinstance(pack_json.get("memory_route"), dict) else {}
        graph_context = pack_json.get("graph_context") if isinstance(pack_json.get("graph_context"), dict) else {}
        memory_items = pack_json.get("memory_items") if isinstance(pack_json.get("memory_items"), list) else []
        compact_memories: list[dict[str, Any]] = []
        for item in memory_items[:12]:
            if not isinstance(item, dict):
                continue
            compact_memories.append(
                {
                    "memory_id": item.get("memory_id"),
                    "memory_type": item.get("memory_type"),
                    "tree_path": item.get("tree_path"),
                    "scope": item.get("scope"),
                    "resident": bool(item.get("resident")),
                    "route_reason": item.get("route_reason"),
                    "content_text": str(item.get("content_text") or "")[:280],
                    "evidence_refs": list(item.get("evidence_refs") or [])[:4],
                }
            )
        relation_refs = graph_context.get("relation_refs") if isinstance(graph_context.get("relation_refs"), list) else []
        compact_relations: list[dict[str, Any]] = []
        for item in relation_refs[:12]:
            if not isinstance(item, dict):
                continue
            compact_relations.append(
                {
                    "relation_id": item.get("relation_id"),
                    "relation_type": item.get("relation_type"),
                    "source_entity_key": item.get("source_entity_key"),
                    "target_entity_key": item.get("target_entity_key"),
                    "neighbor_entity_key": item.get("neighbor_entity_key"),
                    "neighbor_summary": item.get("neighbor_summary"),
                    "evidence_refs": list(item.get("evidence_refs") or [])[:4],
                }
            )
        graph_sources = ["graph_context"] if compact_relations else []
        graph_as_of = "LIVE" if compact_relations else None
        return {
            "memory_route": {
                "route_reason": memory_route.get("route_reason"),
                "matched_branches": list(memory_route.get("matched_branches") or [])[:12],
            },
            "memory_items": compact_memories,
            "graph_context": {
                "route_reason": graph_context.get("route_reason"),
                "source": graph_sources[0] if graph_sources else None,
                "source_refs": graph_sources,
                "as_of": graph_as_of,
                "seed_entity_keys": list(graph_context.get("seed_entity_keys") or [])[:12],
                "neighbor_entity_keys": list(graph_context.get("neighbor_entity_keys") or [])[:12],
                "relation_refs": compact_relations,
            },
        }

    @staticmethod
    def _is_mcp_tool_catalog_inquiry(user_message: str) -> bool:
        lower = user_message.lower()
        if "mcp" not in lower:
            return False
        tool_terms = ("tool", "tools", "工具", "能力", "列表", "哪些", "可用", "使用", "access", "available", "use")
        return any(term in lower for term in tool_terms)

    def _mcp_runtime_tool_overlays(self) -> dict[tuple[str, str], dict[str, Any]]:
        overlays: dict[tuple[str, str], dict[str, Any]] = {}
        try:
            tools = self.repository.list_records("mcp_tools", limit=max(len(TOOL_MANIFEST) * 3, 1000))["items"]
        except Exception:
            raise
        for tool in tools:
            server_key = str(tool.get("server_key") or "")
            tool_name = str(tool.get("tool_name") or "")
            if not server_key or not tool_name:
                continue
            try:
                canonical = canonicalize_server_key(server_key)
            except KeyError:
                continue
            key = (canonical, tool_name)
            current = overlays.get(key)
            if current is None or str(tool.get("status") or "") in {"disabled", "blocked", "deprecated"}:
                overlays[key] = dict(tool)
        return overlays

    def _mcp_runtime_server_overlays(self) -> dict[str, dict[str, Any]]:
        overlays: dict[str, dict[str, Any]] = {}
        for server in self.repository.list_records("mcp_servers", limit=max(len(default_mcp_servers()) * 3, 100))["items"]:
            server_key = str(server.get("server_key") or "")
            if not server_key:
                continue
            try:
                canonical = canonicalize_server_key(server_key)
            except KeyError:
                continue
            overlays[canonical] = dict(server)
        return overlays

    def _manifest_mcp_server_records(self) -> list[dict[str, Any]]:
        overlays = self._mcp_runtime_server_overlays()
        records: list[dict[str, Any]] = []
        for server in default_mcp_servers():
            item = dict(server)
            overlay = overlays.get(str(item["server_key"]))
            if overlay:
                if str(overlay.get("status") or "") in {"disabled", "blocked", "deprecated"}:
                    item["status"] = overlay["status"]
                health = dict(item.get("health_json") if isinstance(item.get("health_json"), dict) else {})
                overlay_health = overlay.get("health_json") if isinstance(overlay.get("health_json"), dict) else {}
                health.update({key: value for key, value in overlay_health.items() if key not in health})
                item["health_json"] = health
                for key in ("last_checked_at", "created_at", "updated_at"):
                    if overlay.get(key) is not None:
                        item[key] = overlay[key]
            item["server_id"] = f"mcp_server_{item['server_key']}".replace("-", "_")
            records.append(enrich_mcp_server_record(item))
        return records

    def list_mcp_servers(self) -> dict[str, Any]:
        items = self._manifest_mcp_server_records()
        return {
            "items": items,
            "total": len(items),
            "page": 1,
            "page_size": len(items),
            "has_more": False,
            "source": "gateway_manifest_derived_catalog",
            "summary_first": True,
        }

    def _manifest_mcp_catalog_records(self) -> list[dict[str, Any]]:
        catalog = gateway_catalog()
        overlays = self._mcp_runtime_tool_overlays()
        records: list[dict[str, Any]] = []
        for entry in TOOL_MANIFEST:
            server_key = server_key_for_module(entry.module, catalog)
            tool = manifest_entry_to_mcp_tool(entry, overlay=overlays.get((server_key, entry.tool_name)), catalog=catalog)
            tool["tool_id"] = f"mcp_tool_{tool['server_key']}_{tool['tool_name']}".replace("-", "_")
            overlay = overlays.get((server_key, entry.tool_name))
            if overlay:
                for key in ("created_at", "updated_at"):
                    if overlay.get(key) is not None:
                        tool[key] = overlay[key]
            records.append(tool)
        return records

    def list_mcp_tools(
        self,
        *,
        server_key: str | None = None,
        risk_level: str | None = None,
        search: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        items = self._manifest_mcp_catalog_records()
        requested_server_key = server_key
        canonical_server_key: str | None = None
        if server_key:
            canonical_server_key = canonicalize_server_key(server_key)
            items = [item for item in items if item.get("server_key") == canonical_server_key]
        if risk_level:
            items = [item for item in items if item.get("risk_level") == risk_level or item.get("manifest_risk_level") == risk_level]
        if search:
            needle = search.strip().lower()
            items = [
                item
                for item in items
                if needle in str(item.get("tool_name") or "").lower()
                or needle in str(item.get("description") or "").lower()
                or needle in str(item.get("module") or "").lower()
                or needle in str(item.get("backend_endpoint") or "").lower()
                or any(needle in str(tag).lower() for tag in item.get("profile_tags") or [])
            ]
        total = len(items)
        resolved_limit = max(1, int(limit or 50))
        resolved_offset = max(0, int(offset or 0))
        page_items = items[resolved_offset : resolved_offset + resolved_limit]
        risk_distribution: dict[str, int] = {}
        profile_distribution: dict[str, int] = {}
        for item in items:
            risk_key = str(item.get("manifest_risk_level") or item.get("risk_level") or "unknown")
            risk_distribution[risk_key] = risk_distribution.get(risk_key, 0) + 1
            profile_key = str(item.get("profile") or "unknown")
            profile_distribution[profile_key] = profile_distribution.get(profile_key, 0) + 1
        return {
            "items": page_items,
            "total": total,
            "page": resolved_offset // resolved_limit + 1,
            "page_size": resolved_limit,
            "has_more": resolved_offset + resolved_limit < total,
            "source": "gateway_manifest_derived_catalog",
            "catalog_source": "gateway_manifest_derived_catalog",
            "manifest_tool_count": len(TOOL_MANIFEST),
            "server_count": len(default_mcp_servers()),
            "risk_distribution": risk_distribution,
            "profile_distribution": profile_distribution,
            "requested_server_key": requested_server_key,
            "canonical_server_key": canonical_server_key,
            "backend_health": {"status": "not_checked", "reason": "Phase 5 catalog UI gate does not run live backend smoke"},
            "recent_smoke": {"status": "not_run", "reason": "Phase 5 validation uses explicit no-live-smoke status"},
        }

    def _resolve_mcp_catalog_tool(self, server_key: str, tool_name: str) -> tuple[dict[str, Any], dict[str, Any]]:
        catalog = gateway_catalog()
        requested_server_key = str(server_key or "")
        canonical = canonicalize_server_key(requested_server_key, catalog)
        entry = TOOL_MANIFEST_BY_NAME.get(str(tool_name or ""))
        if entry is None:
            raise KeyError(f"MCP tool not registered in gateway manifest: {tool_name}")
        expected_server_key = server_key_for_module(entry.module, catalog)
        if entry.module not in catalog.server_key_to_modules.get(canonical, ()):
            raise KeyError(f"MCP tool {tool_name} belongs to {expected_server_key}, not {server_key}")
        tool = manifest_entry_to_mcp_tool(entry, overlay=self._mcp_runtime_tool_overlays().get((expected_server_key, entry.tool_name)), catalog=catalog)
        tool["tool_id"] = f"mcp_tool_{tool['server_key']}_{tool['tool_name']}".replace("-", "_")
        if requested_server_key != expected_server_key:
            tool["requested_server_key"] = requested_server_key
            tool["canonical_server_key"] = expected_server_key
            tool["legacy_server_alias"] = requested_server_key if requested_server_key in catalog.legacy_aliases else None
        server = next(item for item in self._manifest_mcp_server_records() if item["server_key"] == expected_server_key)
        return tool, server

    def _canonicalize_mcp_route(self, route: dict[str, Any]) -> dict[str, Any]:
        if not route.get("server_key") or not route.get("tool_name"):
            return route
        tool, _server = self._resolve_mcp_catalog_tool(str(route["server_key"]), str(route["tool_name"]))
        original_server_key = str(route.get("server_key") or "")
        route["server_key"] = str(tool["server_key"])
        route["manifest_risk_level"] = tool.get("manifest_risk_level")
        route["risk_level"] = tool.get("risk_level")
        route["side_effect_level"] = tool.get("side_effect_level")
        route["requires_approval"] = tool.get("requires_approval")
        route["assistant_usable"] = tool.get("assistant_usable")
        route["profile"] = tool.get("profile")
        route["module"] = tool.get("module")
        if original_server_key != route["server_key"]:
            route["requested_server_key"] = original_server_key
            route["canonicalized_from"] = original_server_key
        return route

    def _semantic_or_legacy_route_decision(self, user_message: str, *, model_profile: dict[str, Any]) -> dict[str, Any]:
        semantic_plan = self._semantic_tool_plan(user_message, model_profile=model_profile)
        if semantic_plan is not None:
            route = semantic_plan.to_route()
            if route.get("requires_clarification"):
                return route
            if route.get("semantic_status") == "no_tool":
                route = self._agentic_read_route_override(user_message, route)
                return self._with_agentic_route_candidates(user_message, route)
            if route.get("server_key") and route.get("tool_name"):
                route = self._agentic_read_route_override(user_message, self._canonicalize_mcp_route(route))
                return self._with_agentic_route_candidates(user_message, route)
        legacy_route = self._canonicalize_mcp_route(dict(route_request(user_message)))
        legacy_route = self._agentic_read_route_override(user_message, legacy_route)
        return self._with_agentic_route_candidates(user_message, legacy_route)

    def _with_agentic_route_candidates(self, user_message: str, route: dict[str, Any]) -> dict[str, Any]:
        route_card = dict(route)
        route_card.setdefault("request", user_message)
        candidates = self._agentic_route_candidates(user_message, route_card)
        allow_multi_tool = self._should_seed_multi_tool_route(user_message, route_card, candidates)
        if candidates:
            for candidate in candidates:
                candidate.setdefault("request", user_message)
            route_card["route_candidates"] = candidates
            route_card["agentic_route_policy"] = {
                "mode": "sorted_candidate_seeds",
                "primary_seed": candidates[0]["route_key"],
                "allow_multi_tool": allow_multi_tool,
                "legacy_fallback_preserved": True,
            }
        if self._should_prioritize_graph_context(user_message):
            route_card["graph_first"] = True
            route_card["graph_first_reason"] = "cross_module_or_relationship_question"
        return route_card

    def _agentic_route_candidates(self, user_message: str, route: dict[str, Any]) -> list[dict[str, Any]]:
        limit = min(6, max(1, self.configured_limit("graph_summary_paths")))
        if self._stock_analysis_route_seed_context(user_message, route):
            limit = max(limit, len(STOCK_DEPTH_SEEDED_TOOL_REFS))
        candidates: list[dict[str, Any]] = []
        lower = user_message.lower()
        graph_first_qe = "qe" in lower and self._should_prioritize_graph_context(user_message)
        skip_primary_for_graph_synthesis = graph_first_qe and str(route.get("domain") or "") == "qe_experiment"
        if route.get("server_key") and route.get("tool_name") and not skip_primary_for_graph_synthesis:
            primary = self._route_candidate_from_route(route, score=int(float(route.get("confidence") or 0.6) * 100), reason=str(route.get("reason") or "primary route"))
            primary["primary_route"] = True
            candidates.append(primary)

        for item in score_domains(user_message)[:limit]:
            domain = item.get("domain")
            if not isinstance(domain, McpDomain) or domain == McpDomain.GENERAL:
                continue
            try:
                spec = DOMAIN_SPECS[domain]
                tool_name = select_tool(domain, user_message)
                candidate = self._candidate_route_for_spec(spec, tool_name=tool_name, score=int(item.get("score") or 0), reason="legacy_domain_candidate")
            except Exception:  # noqa: BLE001 - candidate expansion must not break the legacy primary route.
                logger.exception("failed to build route candidate for domain=%s", domain)
                continue
            candidate["matched_terms"] = list(item.get("matched_terms") or [])
            candidates.append(candidate)

        if "qe" in lower:
            qe_warehouse = DOMAIN_SPECS.get(McpDomain.QE_WAREHOUSE)
            if qe_warehouse is not None:
                candidates.append(self._candidate_route_for_spec(qe_warehouse, tool_name="qe_archive_query_promotion_candidates", score=70, reason="qe_usage_candidate"))
        if any(term in lower for term in ("策略包", "strategy package", "paper v2", "paper")) or graph_first_qe:
            strategy = DOMAIN_SPECS.get(McpDomain.STRATEGY_GOVERNANCE)
            if strategy is not None:
                candidates.append(self._candidate_route_for_spec(strategy, tool_name="strategy_governance_list_packages", score=66, reason="strategy_package_candidate"))
                candidates.append(self._candidate_route_for_spec(strategy, tool_name="strategy_governance_get_paper_readiness", score=64, reason="paper_v2_candidate"))

        candidates.extend(self._stock_analysis_route_seed_candidates(user_message, route))

        canonical_candidates: list[dict[str, Any]] = []
        for candidate in candidates:
            try:
                canonical_candidates.append(self._canonicalize_mcp_route(candidate) if candidate.get("server_key") and candidate.get("tool_name") else candidate)
            except KeyError:
                logger.warning("skip unavailable route candidate: %s/%s", candidate.get("server_key"), candidate.get("tool_name"))
        candidates = canonical_candidates
        deduped: dict[tuple[str, str], dict[str, Any]] = {}
        for candidate in candidates:
            key = (str(candidate.get("server_key") or ""), str(candidate.get("tool_name") or ""))
            if not key[0] or not key[1]:
                continue
            current = deduped.get(key)
            if current is not None and current.get("primary_route") and not candidate.get("primary_route"):
                current["candidate_score"] = max(int(current.get("candidate_score") or 0), int(candidate.get("candidate_score") or 0))
                continue
            if current is None or candidate.get("primary_route") or int(candidate.get("candidate_score") or 0) > int(current.get("candidate_score") or 0):
                candidate["route_key"] = f"{key[0]}/{key[1]}"
                deduped[key] = candidate
        primary_candidates = [item for item in deduped.values() if item.get("primary_route")]
        secondary_candidates = [item for item in deduped.values() if not item.get("primary_route")]
        ordered = [*primary_candidates[:1], *sorted(secondary_candidates, key=lambda item: (-int(item.get("candidate_score") or 0), str(item.get("route_key") or "")))]
        return ordered[:limit]

    def _should_seed_multi_tool_route(self, user_message: str, route: dict[str, Any], candidates: list[dict[str, Any]]) -> bool:
        if len(candidates) < 2 or route.get("requires_clarification"):
            return False
        if str(route.get("side_effect") or "read_only") != "read_only":
            return False
        if self._stock_analysis_route_seed_context(user_message, route):
            return True
        lower = user_message.lower()
        graph_first_qe = "qe" in lower and self._should_prioritize_graph_context(user_message)
        if graph_first_qe:
            return True
        explicit_synthesis = any(
            term in lower
            for term in (
                "综合",
                "多维",
                "多源",
                "全方位",
                "关系",
                "路径",
                "链路",
                "怎么用",
                "如何用",
                "怎么利用",
                "synthesize",
                "multi-source",
            )
        )
        if not explicit_synthesis:
            return False
        read_only_domains = {
            str(candidate.get("domain") or "")
            for candidate in candidates
            if str(candidate.get("side_effect") or "read_only") == "read_only"
        }
        return len(read_only_domains - {"", "general"}) >= 2

    @staticmethod
    def _contains_any_text(text: str, terms: tuple[str, ...]) -> bool:
        lower = str(text or "").lower()
        return any(term.lower() in lower for term in terms)

    @classmethod
    def _stock_depth_focus_matches(cls, user_message: str) -> bool:
        text = str(user_message or "")
        lower = text.lower()
        has_depth_focus = "stock depth" in lower or "\u6df1\u5ea6" in text
        dimension_hits = {term for term in STOCK_DEPTH_DIMENSION_TERMS if term.lower() in lower}
        has_limit_down_triplet = (
            ("limit down" in lower or "\u8dcc\u505c" in text)
            and ("future" in lower or "\u672a\u6765" in text)
            and ("fundamental" in lower or "\u57fa\u672c\u9762" in text or "\u57fa\u672c\u60c5\u51b5" in text)
        )
        has_stock_depth_phrase = "stock depth" in lower and ("fundamental" in lower or "future" in lower)
        return (has_depth_focus and len(dimension_hits) >= 2) or has_limit_down_triplet or has_stock_depth_phrase

    @classmethod
    def _stock_symbol_from_user_message(cls, user_message: str) -> str | None:
        text = str(user_message or "")
        for alias, symbol in STOCK_DEPTH_SYMBOL_ALIASES.items():
            if alias in text:
                return symbol
        match = re.search(r"\b(?:SH|SZ)?\s*(\d{6})(?:\.(?:SH|SZ))?\b", text, re.IGNORECASE)
        return match.group(1) if match else None

    @classmethod
    def _is_stock_depth_analysis_request(cls, user_message: str, route: dict[str, Any] | None = None) -> bool:
        route_domain = str((route or {}).get("domain") or "")
        route_tool = str((route or {}).get("tool_name") or "")
        stock_context = route_domain == "stock_analysis" or route_tool.startswith("stock_analysis_") or bool(cls._stock_symbol_from_user_message(user_message))
        return bool(stock_context and cls._stock_depth_focus_matches(user_message))

    @classmethod
    def _stock_analysis_route_seed_context(cls, user_message: str, route: dict[str, Any] | None = None) -> bool:
        route_domain = str((route or {}).get("domain") or "")
        route_tool = str((route or {}).get("tool_name") or "")
        return route_domain == "stock_analysis" or route_tool.startswith("stock_analysis_") or bool(cls._stock_symbol_from_user_message(user_message))

    @classmethod
    def _stock_depth_tool_args(cls, user_message: str) -> dict[str, Any]:
        args: dict[str, Any] = {
            "period": STOCK_DEPTH_HISTORY_PERIOD,
            "min_trading_days": STOCK_DEPTH_MIN_HISTORY_TRADING_DAYS,
            "limit": 20,
        }
        symbol = cls._stock_symbol_from_user_message(user_message)
        if symbol:
            args["symbol"] = symbol
        return args

    def _stock_analysis_route_seed_candidates(self, user_message: str, route: dict[str, Any]) -> list[dict[str, Any]]:
        if not self._stock_analysis_route_seed_context(user_message, route):
            return []
        base_score = 99
        candidates: list[dict[str, Any]] = []
        for index, (server_key, tool_name) in enumerate(STOCK_DEPTH_SEEDED_TOOL_REFS):
            candidate = dict(route)
            candidate.update(
                {
                    "domain": "external_research" if server_key == "aistock-external-research" else "stock_analysis",
                    "intent_value": DialogueIntent.STOCK_ANALYSIS_REQUEST.value,
                    "server_key": server_key,
                    "tool_name": tool_name,
                    "side_effect": "read_only",
                    "policy": "stock_analysis_read_only_route_seed",
                    "candidate_score": base_score - index,
                    "candidate_reason": "stock_analysis_route_seed",
                    "min_trading_days": STOCK_DEPTH_MIN_HISTORY_TRADING_DAYS,
                }
            )
            if tool_name == "stock_analysis_get_quote":
                candidate["primary_route"] = True
            tool_args = dict(candidate.get("tool_args") if isinstance(candidate.get("tool_args"), dict) else {})
            tool_args.update(self._stock_depth_tool_args(user_message))
            if server_key == "aistock-external-research":
                tool_args.setdefault("query", str(user_message) + " stock analysis event fundamentals industry context")
                tool_args.setdefault("locale", "zh-CN")
            candidate["tool_args"] = tool_args
            candidate.update({key: value for key, value in tool_args.items() if key not in candidate})
            try:
                candidates.append(self._canonicalize_mcp_route(candidate))
            except KeyError:
                logger.warning("skip unavailable stock analysis route seed: %s/%s", server_key, tool_name)
        return candidates

    @staticmethod
    def _route_candidate_from_route(route: dict[str, Any], *, score: int, reason: str) -> dict[str, Any]:
        candidate = dict(route)
        candidate["candidate_score"] = score
        candidate["candidate_reason"] = reason
        candidate.setdefault("side_effect", str(candidate.get("side_effect") or "read_only"))
        return candidate

    @staticmethod
    def _candidate_route_for_spec(spec: Any, *, tool_name: str, score: int, reason: str) -> dict[str, Any]:
        side_effect = "read_only"
        if tool_name in spec.plan_tools:
            side_effect = "plan_or_preflight"
        elif tool_name in spec.confirmed_tools:
            side_effect = "confirmed_action"
        return {
            "domain": spec.domain.value,
            "intent_value": spec.intent_value,
            "server_key": spec.server_key,
            "tool_name": tool_name,
            "side_effect": side_effect,
            "policy": spec.risk_policy,
            "read_tools": list(spec.read_tools),
            "plan_tools": list(spec.plan_tools),
            "confirmed_tools": list(spec.confirmed_tools),
            "candidate_score": score,
            "candidate_reason": reason,
        }

    @staticmethod
    def _should_prioritize_graph_context(user_message: str) -> bool:
        lower = user_message.lower()
        return any(term in lower for term in GRAPH_FIRST_RELATION_TERMS)

    @staticmethod
    def _is_qe_experiment_status_read_request(user_message: str) -> bool:
        lower = user_message.lower()
        has_qe_scope = "qe" in lower or "quantevolver" in lower or "custom_evo" in lower or "实验" in lower
        status_terms = (
            "哪些",
            "有哪",
            "列表",
            "状态",
            "进度",
            "正在运行",
            "还在运行",
            "运行中",
            "还在跑",
            "completed",
            "running",
            "created",
            "failed",
            "完成",
            "失败",
            "已完成",
        )
        strong_write_terms = (
            "创建",
            "生成",
            "草案",
            "模板",
            "物化",
            "启动",
            "执行实验",
            "运行一个",
            "跑一个",
            "create",
            "generate",
            "materialize",
            "start experiment",
            "run experiment",
        )
        return has_qe_scope and any(term in lower for term in status_terms) and not any(term in lower for term in strong_write_terms)

    def _agentic_read_route_override(self, user_message: str, route: dict[str, Any]) -> dict[str, Any]:
        if (
            str(route.get("domain") or "") in {"qe_experiment", "general"}
            and (not route.get("tool_name") or str(route.get("tool_name") or "") in {"qe_template_create", "qe_template_validate", "qe_template_materialize_confirmed", "qe_template_run_confirmed", "qe_template_create_and_run_confirmed"})
            and self._is_qe_experiment_status_read_request(user_message)
        ):
            spec = next(item for item in DOMAIN_SPECS.values() if item.domain.value == "qe_experiment")
            read_route = dict(route)
            read_route.update(
                {
                    "domain": spec.domain.value,
                    "intent_value": spec.intent_value,
                    "server_key": spec.server_key,
                    "tool_name": "qe_experiment_list",
                    "side_effect": "read_only",
                    "reason": "User asked for QE experiment status/read-only evidence; route to qe_experiment_list before agentic synthesis.",
                    "policy": spec.risk_policy,
                    "read_tools": list(spec.read_tools),
                    "plan_tools": list(spec.plan_tools),
                    "confirmed_tools": list(spec.confirmed_tools),
                }
            )
            return self._canonicalize_mcp_route(read_route)
        return route

    def _semantic_tool_plan(self, user_message: str, *, model_profile: dict[str, Any]) -> SemanticToolPlan | None:
        if not model_profile or not self.semantic_tool_planner.available():
            return None
        try:
            return self.semantic_tool_planner.plan(
                user_message=user_message,
                model_profile=model_profile,
                tool_catalog=self._agentic_mcp_catalog_records_for_mode(
                    ModeDecision(
                        mode=DialogueMode.ANALYSIS,
                        intent_type=DialogueIntent.AMBIGUOUS_REQUEST,
                        confidence=1.0,
                        mode_reason="semantic_tool_planner_catalog",
                        requires_tool=False,
                        allowed_tool_side_effect="read_only",
                        requires_user_confirmation=False,
                        requires_approval=False,
                        visible_audit_default=False,
                    )
                ),
            )
        except Exception:  # noqa: BLE001 - semantic planning is best-effort; legacy route keeps chat usable.
            logger.exception("semantic MCP tool planner failed; falling back to legacy route")
            return None

    @staticmethod
    def _mcp_route_tool_args(route: dict[str, Any]) -> dict[str, Any]:
        args = route.get("tool_args") if isinstance(route.get("tool_args"), dict) else {}
        normalized = {str(key): value for key, value in args.items() if value not in (None, "", [], {})}
        if route.get("limit") not in (None, "", [], {}) and "limit" not in normalized:
            normalized["limit"] = route["limit"]
        for key in (
            "symbol",
            "ts_code",
            "stock_code",
            "analysis_date",
            "trade_date",
            "report_period",
            "period",
            "status",
            "state",
            "order_by",
            "offset",
            "active_only",
            "model_type",
            "experiment_id",
            "task_id",
            "qe_task_id",
            "qe_loop_id",
            "loop_id",
            "loop_index",
            "run_id",
            "query",
            "q",
            "search",
            "locale",
            "provider",
            "url",
            "max_chars",
        ):
            if route.get(key) not in (None, "", [], {}) and key not in normalized:
                normalized[key] = route[key]
        return {"tool_args": normalized, **normalized} if normalized else {}

    @staticmethod
    def _mcp_route_limit(route: dict[str, Any]) -> int:
        args = route.get("tool_args") if isinstance(route.get("tool_args"), dict) else {}
        raw = route.get("limit", args.get("limit"))
        try:
            limit = int(raw)
        except (TypeError, ValueError):
            limit = 20
        return max(1, min(limit, 100))

    def _mcp_tool_catalog_snapshot(self) -> dict[str, Any]:
        servers = [item for item in self._manifest_mcp_server_records() if str(item.get("status") or "") in {"ready", "enabled", "ok"}]
        tools = [item for item in self._manifest_mcp_catalog_records() if str(item.get("status") or "") in {"enabled", "ready", "approved"}]
        capabilities = self._approved_workflow_capabilities()
        tools_by_server: dict[str, list[dict[str, Any]]] = {}
        for tool in sorted(tools, key=lambda item: (str(item.get("server_key") or ""), str(item.get("tool_name") or ""))):
            tools_by_server.setdefault(str(tool.get("server_key") or "unknown"), []).append(tool)
        servers_by_key = {str(server.get("server_key") or ""): server for server in servers}
        return {
            "source": "gateway_manifest_derived_catalog",
            "server_count": len(servers),
            "tool_count": len(tools),
            "manifest_tool_count": len(TOOL_MANIFEST),
            "capability_count": len(capabilities),
            "servers": servers,
            "servers_by_key": servers_by_key,
            "tools": tools,
            "tools_by_server": tools_by_server,
            "capabilities": capabilities,
        }

    def _mcp_tool_catalog_context_for_llm(self, user_message: str) -> str | None:
        if not self._is_mcp_tool_catalog_inquiry(user_message):
            return None
        catalog = self._mcp_tool_catalog_snapshot()
        lines = [
            "Runtime MCP catalog snapshot from the unified gateway manifest; answer only from this audited catalog.",
            f"Enabled servers: {catalog['server_count']}; enabled tools: {catalog['tool_count']}; approved capabilities: {catalog['capability_count']}.",
            "Explain capabilities in a human, task-oriented style. Avoid limitation-first phrasing.",
            "Mention that list/overview tools are summary-first and large matrices/logs/model weights use artifact_ref.",
        ]
        servers_by_key = catalog.get("servers_by_key") if isinstance(catalog.get("servers_by_key"), dict) else {}
        for server_key, tools in catalog["tools_by_server"].items():
            server = servers_by_key.get(server_key) if isinstance(servers_by_key.get(server_key), dict) else {}
            health = server.get("health_json") if isinstance(server.get("health_json"), dict) else {}
            display_name = health.get("display_name_zh") or server.get("title") or server_key
            aliases = health.get("business_aliases_zh") if isinstance(health.get("business_aliases_zh"), list) else []
            alias_text = f" aliases={', '.join(str(item) for item in aliases[:4])}" if aliases else ""
            tool_names = ", ".join(str(tool.get("tool_name") or "") for tool in tools)
            lines.append(f"- {display_name} ({server_key}{alias_text}): {tool_names}")
        return "\n".join(lines)


    @staticmethod
    def _mcp_ref_pair(server_key: Any, tool_name: Any) -> tuple[str, str] | None:
        server = str(server_key or "").strip()
        tool = str(tool_name or "").strip()
        if not server or not tool:
            return None
        try:
            server = canonicalize_server_key(server)
        except KeyError:
            return None
        return server, tool

    @staticmethod
    def _empty_capability_ref_shape(value: Any) -> bool:
        return value is None or value == "" or value == {}

    def _assert_existing_capability_ref_shapes_repairable_for_sync(self, capability: dict[str, Any]) -> None:
        capability_key = str(capability.get("capability_key") or "")
        for field in ("mcp_tool_refs", "skill_refs"):
            value = capability.get(field)
            if isinstance(value, list) or self._empty_capability_ref_shape(value):
                continue
            error = self._payload_for_capability_registry_mcp_refs_error(
                capability_key=capability_key,
                field=field,
                actual_type=type(value).__name__,
                detail="existing registry value is non-empty and non-list; sync will not guess a replacement",
                source="assistant_capabilities_sync",
            )
            logger.error(
                "research assistant capability sync refused invalid registry refs: reason_code=%s capability_key=%s field=%s actual_type=%s",
                error["reason_code"],
                capability_key,
                field,
                error["actual_type"],
            )
            raise ResearchAssistantRuntimeConfigInvalidError(error)

    def _repair_empty_capability_ref_shapes(self, capabilities: list[dict[str, Any]]) -> list[dict[str, Any]]:
        del capabilities
        logger.warning("RA capability registry DB repair is retired; YAML memory authority is used directly.")
        return []

    def _align_capability_registry_ref_shapes_for_catalog_ready(self) -> list[dict[str, Any]]:
        logger.warning("RA capability registry alignment is retired; YAML memory authority is used directly.")
        return []

    def _approved_capability_mcp_tool_refs(self) -> set[tuple[str, str]]:
        capabilities = [
            dict(item)
            for item in self._workflow_capabilities()
            if str(item.get("status") or "approved") == "approved"
        ]
        refs: set[tuple[str, str]] = set()
        for capability in capabilities:
            canonical = self._canonicalize_capability_mcp_refs(capability, source="declarative_yaml_memory_authority")
            for ref in canonical.get("mcp_tool_refs") if isinstance(canonical.get("mcp_tool_refs"), list) else []:
                if not isinstance(ref, dict):
                    continue
                pair = self._mcp_ref_pair(ref.get("server_key"), ref.get("tool_name"))
                if pair:
                    refs.add(pair)
        return refs

    def _capability_backed_mcp_catalog_records(self) -> list[dict[str, Any]]:
        executable_refs = self._approved_capability_mcp_tool_refs()
        return [
            tool
            for tool in self._manifest_mcp_catalog_records()
            if self._mcp_ref_pair(tool.get("server_key"), tool.get("tool_name")) in executable_refs
        ]

    def _capability_backed_side_effect_mcp_catalog_records(self) -> list[dict[str, Any]]:
        executable_refs = self._approved_capability_mcp_tool_refs()
        records: list[dict[str, Any]] = []
        for tool in self._manifest_mcp_catalog_records():
            pair = self._mcp_ref_pair(tool.get("server_key"), tool.get("tool_name"))
            if not pair or pair not in executable_refs:
                continue
            if str(tool.get("side_effect_level") or "read_only") == "read_only":
                continue
            records.append(tool)
        return records

    def _agentic_mcp_catalog_records_for_mode(self, mode_decision: ModeDecision) -> list[dict[str, Any]]:
        if mode_decision.mode not in {DialogueMode.PLANNING, DialogueMode.ANALYSIS, DialogueMode.PREFLIGHT, DialogueMode.EXECUTION}:
            return []
        allowed_side_effect = str(mode_decision.allowed_tool_side_effect or "none")
        if allowed_side_effect == "none":
            return []

        capability_backed_refs = self._approved_capability_mcp_tool_refs()
        records: list[dict[str, Any]] = []
        for tool in self._manifest_mcp_catalog_records():
            pair = self._mcp_ref_pair(tool.get("server_key"), tool.get("tool_name"))
            if not pair:
                continue
            side_effect = str(tool.get("side_effect_level") or "read_only")
            if side_effect == "read_only":
                records.append(tool)
                continue
            if pair in capability_backed_refs:
                records.append(tool)
        return records

    @staticmethod
    def _skill_function_name(skill_key: str) -> str:
        return mcp_tool_function_name(f"skill__{skill_key}")

    def _approved_skill_function_records(self) -> list[dict[str, Any]]:
        return [
            item
            for item in self.repository.list_records("skills", filters={"status": "approved"}, limit=self.configured_limit("api_list_skills"))["items"]
            if str(item.get("skill_id") or "").strip() and str(item.get("skill_key") or "").strip()
        ]

    @staticmethod
    def _skill_function_spec(skill: dict[str, Any]) -> dict[str, Any]:
        skill_key = str(skill.get("skill_key") or skill.get("skill_id") or "").strip()
        schema = skill.get("input_schema_json") if isinstance(skill.get("input_schema_json"), dict) else {"type": "object"}
        description = str(skill.get("description_for_llm") or skill.get("description") or skill.get("title") or skill_key).strip()
        risk = str(skill.get("risk_level") or "medium")
        permission = str(skill.get("permission_scope") or skill.get("allowed_side_effect_level") or "unknown")
        return {
            "type": "function",
            "function": {
                "name": ResearchAssistantService._skill_function_name(skill_key),
                "description": (
                    f"{description} Skill route={skill_key}; risk={risk}; permission_scope={permission}. "
                    "Selecting this function only creates an approval-gated skill reuse proposal; it never executes directly. "
                    "Use only parameters declared in the JSON schema."
                )[:1024],
                "parameters": schema,
            },
        }

    def _agentic_function_tools(self, mode_decision: ModeDecision) -> tuple[list[dict[str, Any]], FunctionToolRegistry]:
        specs, mcp_registry = function_calling_tools_for_mcp(self._agentic_mcp_catalog_records_for_mode(mode_decision))
        registry = FunctionToolRegistry(
            {
                name: {**dict(mapping), "kind": str(mapping.get("kind") or "mcp")}
                for name, mapping in mcp_registry.items()
            }
        )
        for skill in self._approved_skill_function_records():
            skill_key = str(skill.get("skill_key") or "").strip()
            skill_id = str(skill.get("skill_id") or "").strip()
            if not skill_key or not skill_id:
                continue
            function_name = self._skill_function_name(skill_key)
            specs.append(self._skill_function_spec(skill))
            registry[function_name] = {
                "kind": "skill",
                "skill_id": skill_id,
                "skill_key": skill_key,
            }
        return specs, registry

    def _process_agentic_skill_calls(
        self,
        calls: list[SkillFunctionCall],
        *,
        task: dict[str, Any],
        conversation_id: str,
        context_pack: dict[str, Any],
        cards: dict[str, Any],
    ) -> None:
        if not calls:
            return
        cards.setdefault("action_proposals", [])
        cards.setdefault("skill_reuse_result", {})
        for call in calls:
            try:
                reuse = self.propose_skill_reuse(
                    task_id=str(task["task_id"]),
                    skill_id=call.skill_id,
                    input_json={
                        **dict(call.payload_json),
                        "selected_skill": {
                            "skill_id": call.skill_id,
                            "skill_key": call.skill_key,
                            "function_name": call.function_name,
                            "stable_call_id": call.stable_call_id,
                        },
                    },
                    conversation_id=conversation_id,
                    context_pack_id=str(context_pack.get("context_pack_id") or ""),
                )
                proposal = reuse.get("action_proposal") if isinstance(reuse, dict) else None
                if not isinstance(proposal, dict):
                    cards["skill_reuse_result"] = {
                        "auto_executed": False,
                        "executed": False,
                        "status": "blocked",
                        "proposal_type": "skill",
                        "skill_id": call.skill_id,
                        "skill_key": call.skill_key,
                        "reason_codes": list(reuse.get("reason_codes") or ["skill_reuse_proposal_blocked"]) if isinstance(reuse, dict) else ["skill_reuse_proposal_blocked"],
                        "warnings": list(reuse.get("warnings") or []) if isinstance(reuse, dict) else [],
                    }
                    continue
                payload = dict(proposal.get("input_json") or {})
                preflight_payload = {
                    "passed": True,
                    "approval_required": True,
                    "failed_checks": [],
                    "preflight_checks": ["capability_status", "skill_registry"],
                    "payload_digest": sha256_json(payload),
                    "skill_id": call.skill_id,
                    "skill_key": call.skill_key,
                }
                proposal_state = self.repository.update_record("action_proposals", proposal["action_proposal_id"], {"status": "approval_required"})
                self.create_trace_event(
                    TraceEventCreate(
                        task_id=proposal["task_id"],
                        event_type="action_preflight",
                        component="research_assistant.execution_gateway",
                        status="approval_required",
                        payload_json={"action_proposal_id": proposal["action_proposal_id"], "preflight": preflight_payload},
                    )
                )
                approval = None
                if str(proposal_state.get("status") or "") == "approval_required":
                    approval = self._ensure_skill_action_proposal_chat_approval(proposal_state)
                    proposal_state = self.repository.get_record("action_proposals", proposal["action_proposal_id"]) or proposal_state
                    preflight_payload.update(
                        {
                            "approval_id": approval["approval_id"],
                            "required_confirmation_text": approval["required_confirmation_text"],
                            "approval_type": approval["approval_type"],
                        }
                    )
                card = {
                    "title": proposal_state.get("title") or f"复用技能：{call.skill_key}",
                    "proposal_type": "skill",
                    "skill_id": call.skill_id,
                    "skill_key": call.skill_key,
                    "risk": proposal_state.get("risk_level"),
                    "approval_required": True,
                    "status": proposal_state.get("status"),
                    "action_proposal_id": proposal_state["action_proposal_id"],
                    "route": f"skill/{call.skill_key}",
                    "direct_execution_allowed": False,
                    "required_confirmations": [SKILL_LIBRARY_REUSE_CONFIRMATION],
                }
                if approval:
                    card.update(
                        {
                            "approval_id": approval["approval_id"],
                            "approval_type": approval["approval_type"],
                            "required_confirmation_text": approval["required_confirmation_text"],
                            "approval_status": approval["status"],
                        }
                    )
                cards["action_proposals"].append(card)
                cards["skill_reuse_result"] = {
                    "auto_executed": False,
                    "executed": False,
                    "status": proposal_state.get("status"),
                    "proposal_type": "skill",
                    "skill_id": call.skill_id,
                    "skill_key": call.skill_key,
                    "route": f"skill/{call.skill_key}",
                    "action_proposal_id": proposal_state["action_proposal_id"],
                    "preflight": preflight_payload,
                    "direct_execution_allowed": False,
                }
                if approval:
                    cards["skill_reuse_result"].update(
                        {
                            "approval_id": approval["approval_id"],
                            "approval_type": approval["approval_type"],
                            "required_confirmation_text": approval["required_confirmation_text"],
                            "approval_status": approval["status"],
                        }
                    )
            except Exception as exc:  # noqa: BLE001 - skill routing failures must be visible, not silent.
                error = {
                    "reason_code": "skill_reuse_proposal_failed",
                    "code": "skill_reuse_proposal_failed",
                    "stage": "skill_function_call_dispatch",
                    "skill_id": call.skill_id,
                    "skill_key": call.skill_key,
                    "exception_type": type(exc).__name__,
                    "message": str(exc),
                }
                logger.exception("research assistant skill function dispatch failed: skill=%s", call.skill_key)
                cards["skill_reuse_result"] = {
                    "auto_executed": False,
                    "executed": False,
                    "status": "failed",
                    "proposal_type": "skill",
                    "skill_id": call.skill_id,
                    "skill_key": call.skill_key,
                    "error": error,
                }
                cards.setdefault("tool_errors", [])
                if isinstance(cards["tool_errors"], list):
                    cards["tool_errors"].append(error)

    @staticmethod
    def _looks_like_large_tool_payload(payload: dict[str, Any]) -> bool:
        rendered = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        items = payload.get("items") if isinstance(payload.get("items"), list) else []
        sections = payload.get("sections") if isinstance(payload.get("sections"), list) else []
        return len(rendered) > 9000 or len(items) > 12 or len(sections) > 8

    def _cheap_worker_model_profile(self) -> dict[str, Any] | None:
        route = self.route_model(ModelRouteRequest(role="cheap_worker", risk_level="low", token_estimate=2000))
        profile = route.get("model_profile")
        return profile if isinstance(profile, dict) and profile.get("status") == "enabled" and profile.get("role") == "cheap_worker" else None

    def _compact_tool_result_with_auxiliary_model(self, result: McpToolResult) -> McpToolResult:
        if not isinstance(result.payload_json, dict) or not self._looks_like_large_tool_payload(result.payload_json):
            return result
        profile = self._cheap_worker_model_profile()
        if not profile or not callable(getattr(self.llm_client, "complete", None)):
            return result
        prompt = {
            "instruction": "Summarize this tool result for the primary agent. Preserve exact source_refs, as_of, status_counts/group_counts, IDs, metrics, and negative/empty facts. Do not invent.",
            "tool_result": {
                "server_key": result.server_key,
                "tool_name": result.tool_name,
                "payload": result.payload_json,
                "source_refs": result.source_refs,
                "as_of": result.as_of,
            },
        }
        try:
            compacted = self.llm_client.complete(
                messages=[{"role": "system", "content": "You are a cheap_worker compression helper; output compact JSON only."}, {"role": "user", "content": json.dumps(prompt, ensure_ascii=False, sort_keys=True)}],
                model_profile=profile,
                temperature=0.0,
                max_tokens=700,
            )
        except Exception:  # noqa: BLE001 - auxiliary compression must not block primary synthesis.
            logger.exception("cheap_worker tool-result compaction failed; using original result")
            return result
        compact_payload = dict(result.payload_json)
        compact_payload["auxiliary_summary"] = compacted.content[:2400]
        if isinstance(compact_payload.get("items"), list):
            compact_payload["items"] = compact_payload["items"][:5]
        if isinstance(compact_payload.get("sections"), list):
            compact_payload["sections"] = compact_payload["sections"][:8]
        result.payload_json = compact_payload
        result.summary = compacted.content[:1200]
        return result

    def _complete_chat_with_reactive_recovery(
        self,
        *,
        user_message: str,
        conversation_id: str,
        task_id: str,
        risk_level: str,
        messages: list[dict[str, str]],
        bundle: dict[str, Any],
        context_pack: dict[str, Any],
        initial_candidates: list[dict[str, Any]],
        prior_messages: list[dict[str, str]],
        budget_plan: ContextBudgetPlan,
        model_profile: dict[str, Any],
        runtime_activation: dict[str, Any],
        assembly_trace: dict[str, Any],
        function_tools: list[dict[str, Any]] | None = None,
        function_tool_registry: dict[str, dict[str, Any]] | None = None,
    ) -> tuple[LlmCallResult, list[dict[str, str]], ContextBudgetPlan, list[dict[str, str]], dict[str, Any]]:
        try:
            result = self.llm_client.complete(
                messages=messages,
                model_profile=model_profile,
                temperature=budget_plan.llm_temperature,
                max_tokens=budget_plan.llm_max_tokens,
                tools=function_tools,
                tool_choice="auto" if function_tools else None,
                tool_registry=function_tool_registry,
            )
            return result, messages, budget_plan, prior_messages, assembly_trace
        except Exception as exc:
            runtime_config = budget_plan.runtime_config
            if not self._is_reactive_context_error(exc, runtime_config):
                self._record_llm_failure(task_id, model_profile, bundle, exc, status="failed")
                raise
            overflow_trace = self.create_trace_event(
                TraceEventCreate(
                    task_id=task_id,
                    event_type="llm_call",
                    component="research_assistant.chat_turn",
                    status="prompt_too_long_reactive",
                    model_profile_id=model_profile["model_profile_id"],
                    payload_json={"prompt_bundle_id": bundle["prompt_bundle_id"], "error": str(exc), "assembly_trace_id": assembly_trace["assembly_trace_id"]},
                )
            )
            self.add_task_event(
                task_id,
                TaskEventCreate(
                    event_type="llm_failed",
                    severity="warning",
                    message="模型返回上下文超限，已按 runtime config 触发 reactive compact。",
                    payload_json={"error": str(exc), "assembly_trace_id": assembly_trace["assembly_trace_id"], "trace_id": overflow_trace["trace_id"]},
                ),
            )
            last_exc: Exception = exc

        max_retries = int(budget_plan.runtime_config["compaction"]["worker"]["max_retries"])
        for attempt in range(1, max_retries + 1):
            segment = self._maybe_compact_prior_messages(
                conversation_id=conversation_id,
                candidates=initial_candidates,
                budget_plan=budget_plan,
                model_profile=model_profile,
                bundle=bundle,
                task_id=task_id,
                runtime_activation=runtime_activation,
                force=True,
                trigger_reason="reactive_context_overflow",
            )
            if segment is None:
                break
            retry_budget = self.context_budget_planner.plan(
                model_profile=model_profile,
                runtime_config=budget_plan.runtime_config,
                prompt_bundle_text=bundle["bundle_text"],
                context_pack_summary=str(context_pack.get("pack_summary") or ""),
                prior_messages=initial_candidates,
                compact_summaries=self._active_context_segments(conversation_id),
                current_user_message=user_message,
            )
            retry_prior_messages = self._prepare_prior_chat_messages(
                conversation_id=conversation_id,
                current_message=user_message,
                candidates=initial_candidates,
                budget_plan=retry_budget,
                model_profile=model_profile,
                bundle=bundle,
                task_id=task_id,
                runtime_activation=runtime_activation,
            )
            retry_trace = self._record_context_assembly_trace(
                conversation_id=conversation_id,
                task_id=task_id,
                bundle=bundle,
                runtime_activation=runtime_activation,
                budget_plan=retry_budget,
                prior_messages=retry_prior_messages,
                context_pack=context_pack,
                status="retry_after_compaction",
                extra_assembly={"reactive_retry_attempt": attempt, "compaction_segment_id": segment["segment_id"]},
            )
            retry_messages = self._chat_messages_for_llm(
                user_message,
                bundle,
                context_pack,
                retry_prior_messages,
                mode_decision=(bundle.get("selection_trace_json") or {}).get("mode_decision"),
                runtime_config=retry_budget.runtime_config,
            )
            retry_messages.insert(1, {"role": "system", "content": self._prompt_text("context.recovery.prompt_too_long_retry")})
            try:
                result = self.llm_client.complete(
                    messages=retry_messages,
                    model_profile=model_profile,
                    temperature=retry_budget.llm_temperature,
                    max_tokens=retry_budget.llm_max_tokens,
                    tools=function_tools,
                    tool_choice="auto" if function_tools else None,
                    tool_registry=function_tool_registry,
                )
                return result, retry_messages, retry_budget, retry_prior_messages, retry_trace
            except Exception as retry_exc:
                last_exc = retry_exc
                if not self._is_reactive_context_error(retry_exc, retry_budget.runtime_config):
                    self._record_llm_failure(task_id, model_profile, bundle, retry_exc, status="failed_after_reactive_compaction")
                    raise

        self._record_llm_failure(task_id, model_profile, bundle, last_exc, status="context_overflow_fail_fast")
        if risk_level in {"high", "production_sensitive"}:
            raise RuntimeError("High-risk Research Assistant task stopped after context compaction retry; no degraded answer was generated.") from last_exc
        raise last_exc

    def _record_llm_failure(self, task_id: str, model_profile: dict[str, Any], bundle: dict[str, Any], exc: Exception, *, status: str) -> dict[str, Any]:
        trace = self.create_trace_event(
            TraceEventCreate(
                task_id=task_id,
                event_type="llm_call",
                component="research_assistant.chat_turn",
                status=status,
                model_profile_id=model_profile["model_profile_id"],
                payload_json={"prompt_bundle_id": bundle["prompt_bundle_id"], "error": str(exc)},
            )
        )
        self.add_task_event(task_id, TaskEventCreate(event_type="llm_failed", severity="error", message=f"主模型调用失败：{exc}", payload_json={"trace_id": trace["trace_id"], "status": status}))
        return trace

    @staticmethod
    def _is_reactive_context_error(exc: Exception, runtime_config: dict[str, Any]) -> bool:
        error_text = str(exc).lower()
        codes = runtime_config["compaction"]["trigger"]["reactive_error_codes"]
        return any(str(code).lower() in error_text for code in codes)

    def _context_health_payload(self, conversation_id: str, budget_plan: ContextBudgetPlan, *, mode_decision: ModeDecision | None = None) -> dict[str, Any]:
        mode_cfg = self._dialogue_mode_config(mode_decision.mode.value) if mode_decision else {}
        show_badge = bool(budget_plan.runtime_config["ui"]["show_context_health_badge"])
        if "show_context_health_badge" in mode_cfg:
            show_badge = show_badge and bool(mode_cfg.get("show_context_health_badge"))
        return {
            "status": "compacted_or_ready" if budget_plan.should_compact else "healthy",
            "notify_mode": budget_plan.runtime_config["ui"]["notify_auto_compaction"],
            "show_badge": show_badge,
            "allow_user_expand_summary": bool(budget_plan.runtime_config["ui"]["allow_user_expand_summary"]),
            "utilization_ratio": round(budget_plan.utilization_ratio, 4),
            "estimated_input_tokens": budget_plan.estimated_input_tokens,
            "effective_window_tokens": budget_plan.effective_window_tokens,
            "fresh_tail_min_messages": budget_plan.fresh_tail_min_messages,
            "compact_summary_count": len(self._active_context_segments(conversation_id)),
            "key_fact_count": len(self._active_key_facts(conversation_id)),
            "config_driven": True,
        }


    def _active_context_segments(self, conversation_id: str) -> list[dict[str, Any]]:
        return self.repository.list_records(
            "context_segments",
            filters={"conversation_id": conversation_id, "status": "active"},
            limit=self.configured_limit("active_context_segments"),
        )["items"]

    def _active_key_facts(self, conversation_id: str) -> list[dict[str, Any]]:
        return self.repository.list_records(
            "context_key_facts",
            filters={"conversation_id": conversation_id, "status": "active"},
            limit=self.configured_limit("active_context_key_facts"),
        )["items"]

    def _fetch_prior_chat_messages(self, conversation_id: str, current_message: str, runtime_config: dict[str, Any]) -> list[dict[str, Any]]:
        history_cfg = runtime_config["history_fetch"]
        page_size = int(history_cfg["page_size"])
        max_pages = int(history_cfg["max_pages"])
        include_roles = {str(role) for role in history_cfg["include_roles"]}
        items: list[dict[str, Any]] = []
        for page_index in range(max_pages):
            result = self.repository.list_records(
                "conversation_messages",
                filters={"conversation_id": conversation_id},
                limit=page_size,
                offset=page_index * page_size,
            )
            items.extend(result["items"])
            if not result.get("has_more"):
                break
        candidates: list[dict[str, Any]] = []
        for item in sorted(items, key=lambda row: str(row.get("created_at") or "")):
            content = str(item.get("content_text") or "").strip()
            if not content or content == current_message:
                continue
            role = str(item.get("role") or "")
            if role not in include_roles:
                continue
            candidates.append(
                {
                    "message_id": str(item.get("message_id") or ""),
                    "role": role,
                    "content": content,
                    "created_at": item.get("created_at"),
                }
            )
        return candidates

    def _prepare_prior_chat_messages(
        self,
        *,
        conversation_id: str,
        current_message: str,
        candidates: list[dict[str, Any]],
        budget_plan: ContextBudgetPlan,
        model_profile: dict[str, Any],
        bundle: dict[str, Any],
        task_id: str,
        runtime_activation: dict[str, Any],
    ) -> list[dict[str, str]]:
        self._maybe_compact_prior_messages(
            conversation_id=conversation_id,
            candidates=candidates,
            budget_plan=budget_plan,
            model_profile=model_profile,
            bundle=bundle,
            task_id=task_id,
            runtime_activation=runtime_activation,
        )
        summary_messages = [
            {"role": "system", "content": f"Historical compact summary: {segment['content_text']}"}
            for segment in reversed(self._active_context_segments(conversation_id))
        ]
        key_fact_messages = [
            {"role": "system", "content": f"Locked key facts: {fact['fact_text']}"}
            for fact in reversed(self._active_key_facts(conversation_id))
        ]
        raw_messages = self._select_history_window(candidates, budget_plan)
        return [*summary_messages, *key_fact_messages, *raw_messages]

    def _select_history_window(self, candidates: list[dict[str, Any]], budget_plan: ContextBudgetPlan) -> list[dict[str, str]]:
        selected: list[dict[str, str]] = []
        tokens_used = 0
        for msg in reversed(candidates):
            content = str(msg["content"])
            msg_tokens = self.context_budget_planner.estimate_tokens(content, budget_plan.runtime_config)
            if tokens_used + msg_tokens > budget_plan.history_budget_tokens and selected:
                break
            selected.append({"role": str(msg["role"]), "content": content})
            tokens_used += msg_tokens
        selected.reverse()
        if len(selected) < len(candidates):
            logger.info(
                "chat history window: kept %d/%d messages (~%d estimated tokens), dropped %d oldest due to config budget %d",
                len(selected), len(candidates), tokens_used, len(candidates) - len(selected), budget_plan.history_budget_tokens,
            )
        return selected

    def _maybe_compact_prior_messages(
        self,
        *,
        conversation_id: str,
        candidates: list[dict[str, Any]],
        budget_plan: ContextBudgetPlan,
        model_profile: dict[str, Any],
        bundle: dict[str, Any],
        task_id: str,
        runtime_activation: dict[str, Any],
        force: bool = False,
        trigger_reason: str = "proactive_threshold",
    ) -> dict[str, Any] | None:
        if not budget_plan.compaction_allowed_by_config or (not force and not budget_plan.should_compact):
            return None
        fresh_tail_count = max(1, budget_plan.fresh_tail_min_messages)
        old_messages = candidates[:-fresh_tail_count] if len(candidates) > fresh_tail_count else []
        if not old_messages:
            return None
        covered_ids = [item["message_id"] for item in old_messages if item.get("message_id")]
        already_active = self._active_context_segments(conversation_id)
        covered_by_active = {mid for segment in already_active for mid in (segment.get("source_message_ids") or [])}
        if covered_ids and set(covered_ids).issubset(covered_by_active):
            return None
        prompt_text = self._prompt_text("context.compaction.structured_summary")
        compact_input = "\n".join(
            f"[{item.get('message_id')}] {item.get('role')}: {item.get('content')}" for item in old_messages
        )
        compact_result = self.llm_client.complete(
            messages=[
                {"role": "system", "content": prompt_text},
                {"role": "user", "content": compact_input},
            ],
            model_profile=model_profile,
            temperature=budget_plan.llm_temperature,
            max_tokens=budget_plan.compaction_max_output_tokens,
        )
        source_sha = sha256_json([{"message_id": item.get("message_id"), "content": item.get("content")} for item in old_messages])
        segment = self.repository.create_record(
            "context_segments",
            {
                "segment_id": new_id("ctxseg"),
                "conversation_id": conversation_id,
                "segment_type": "compact_summary",
                "summary_depth": 1,
                "content_text": compact_result.content,
                "content_json": {
                    "format": budget_plan.runtime_config["compaction"]["output"]["format"],
                    "source_message_count": len(old_messages),
                    "usage": compact_result.usage,
                },
                "source_message_ids": covered_ids,
                "source_sha256": source_sha,
                "prompt_activation_id": bundle.get("activation_id"),
                "runtime_config_activation_id": runtime_activation["activation_id"],
                "status": "active",
                "metadata_json": {
                    "task_id": task_id,
                    "mandatory_compaction": budget_plan.mandatory_compaction,
                    "tools_enabled": budget_plan.runtime_config["compaction"]["worker"]["tools_enabled"],
                },
            },
        )
        fact_prompt = self._prompt_text("context.compaction.key_fact_extraction")
        fact_result = self.llm_client.complete(
            messages=[
                {"role": "system", "content": fact_prompt},
                {"role": "user", "content": compact_result.content},
            ],
            model_profile=model_profile,
            temperature=budget_plan.llm_temperature,
            max_tokens=budget_plan.compaction_max_output_tokens,
        )
        fact = self.repository.create_record(
            "context_key_facts",
            {
                "fact_id": new_id("ctxfact"),
                "conversation_id": conversation_id,
                "segment_id": segment["segment_id"],
                "fact_type": "key_fact_block",
                "fact_text": fact_result.content,
                "fact_json": {
                    "source_sha256": source_sha,
                    "summary_segment_id": segment["segment_id"],
                    "prompt_key": "context.compaction.key_fact_extraction",
                    "usage": fact_result.usage,
                },
                "source_message_ids": covered_ids,
                "confidence": 0.8,
                "status": "active",
                "metadata_json": {"task_id": task_id},
            },
        )
        self.add_task_event(
            task_id,
            TaskEventCreate(
                event_type="context_compacted",
                message="Active runtime config triggered context compaction.",
                payload_json={
                    "segment_id": segment["segment_id"],
                    "fact_id": fact["fact_id"],
                    "source_message_count": len(old_messages),
                    "trigger_reason": trigger_reason,
                },
            ),
        )
        return segment

    def _record_context_assembly_trace(
        self,
        *,
        conversation_id: str,
        task_id: str,
        bundle: dict[str, Any],
        runtime_activation: dict[str, Any],
        budget_plan: ContextBudgetPlan,
        prior_messages: list[dict[str, str]],
        context_pack: dict[str, Any],
        status: str = "ok",
        extra_assembly: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        assembly_json = {
            "order": budget_plan.runtime_config["assembly"]["order"],
            "prior_message_count": len(prior_messages),
            "context_pack_id": context_pack["context_pack_id"],
            "trace_every_turn": budget_plan.runtime_config["assembly"]["trace_every_turn"],
        }
        if extra_assembly:
            assembly_json.update(extra_assembly)
        return self.repository.create_record(
            "context_assembly_traces",
            {
                "assembly_trace_id": new_id("ctxasm"),
                "conversation_id": conversation_id,
                "task_id": task_id,
                "prompt_activation_id": bundle.get("activation_id"),
                "runtime_config_activation_id": runtime_activation["activation_id"],
                "budget_json": budget_plan.as_trace_payload(),
                "assembly_json": assembly_json,
                "source_refs_json": {
                    "prompt_bundle_id": bundle["prompt_bundle_id"],
                    "prompt_activation_id": bundle.get("activation_id"),
                    "runtime_config_activation_id": runtime_activation["activation_id"],
                },
                "status": status,
            },
        )

    def _prompt_text(self, prompt_key: str) -> str:
        node = self.declarative_config.prompt_node(prompt_key)
        if not node:
            raise RuntimeError(f"active prompt node is missing: {prompt_key}")
        return str(node["prompt_text"])

    @staticmethod
    def _preview_text(text: str, budget_plan: ContextBudgetPlan) -> str:
        return text[: budget_plan.trace_response_preview_chars]

    def _build_human_cards(
        self,
        user_message: str,
        task: dict[str, Any],
        bundle: dict[str, Any],
        route: dict[str, Any],
        dialogue_intent: DialogueIntent,
        mode_decision: ModeDecision,
        route_decision: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        del task
        intent_config = self._dialogue_intent_config()
        template = self._dialogue_card_template(dialogue_intent, intent_config)
        mode_cfg = self._dialogue_mode_config(mode_decision.mode.value)
        capabilities = self._approved_workflow_capabilities()
        qe_capability_keys = set(self.active_runtime_config().get("planner", {}).get("qe_workflow_capability_keys", []))
        available_keys = {str(item.get("capability_key")) for item in capabilities}
        include_qe_capabilities = bool(template.get("include_qe_capabilities"))
        mcp_route = dict(route_decision) if isinstance(route_decision, dict) else self._semantic_or_legacy_route_decision(user_message, model_profile=route.get("model_profile") or {})
        route_domain = str(mcp_route.get("domain") or "general")
        is_local_data = dialogue_intent == DialogueIntent.LOCAL_DATA_MANAGEMENT_REQUEST or route_domain == "local_data"
        local_data_capability_keys = set(self.active_runtime_config().get("planner", {}).get("local_data_workflow_capability_keys", []))
        capability_card_keys: set[str] = set()
        if include_qe_capabilities:
            capability_card_keys.update(qe_capability_keys)
        if is_local_data:
            capability_card_keys.update(local_data_capability_keys)
        route_capability_keys = set()
        if mcp_route.get("domain") and mcp_route.get("domain") != "general" and not mcp_route.get("requires_clarification"):
            route_capability_keys.add(f"{mcp_route['domain']}.mcp_orchestration")
            side_effect = str(mcp_route.get("side_effect") or "read_only")
            mcp_route.update(
                {
                    "summary_first": True,
                    "preflight_required": side_effect != "read_only",
                    "confirmation_required": side_effect == "confirmed_action",
                    "ui_card": "mcp_route_decision",
                }
            )
        if route_capability_keys:
            capability_card_keys.update(route_capability_keys)
        capability_cards = self._capability_cards(capabilities, capability_card_keys) if capability_card_keys else []
        missing_capability_keys = sorted(capability_card_keys - available_keys) if capability_card_keys else []
        prompt_branches = [item["prompt_key"] for item in bundle.get("node_refs", [])]
        capability_summary_cfg = intent_config.get("capability_summary", {})
        capability_summary = {
            "mcp": str(capability_summary_cfg.get("mcp") or ""),
            "skill": str(capability_summary_cfg.get("skill") or ""),
            "model": route.get("model_profile", {}).get("display_name") or route.get("model_profile", {}).get("model_name"),
            "prompt_branches": prompt_branches,
        }
        extra_summary = template.get("capability_summary_extra")
        if isinstance(extra_summary, dict):
            capability_summary.update({str(key): value for key, value in extra_summary.items()})
        status_rail_ref = str(template.get("status_rail_ref") or "answered")
        plan_steps = list(template.get("plan_steps") or [])
        clarification_questions = list(template.get("clarification_questions") or [])
        action_proposals = list(template.get("action_proposals") or [])
        if is_local_data:
            status_rail_ref = "waiting_confirmation"
            plan_steps = [
                "复述本地数据检查范围、影响模块和本轮只读边界。",
                "从 aistock-local-data MCP 目录中选择健康概览、数据集状态、同步目标和修复计划能力。",
                "先读取 readiness、recent jobs、alerts、data_sync_targets 等证据，不直接写库或启动同步。",
                "如需修复，只生成 local_data_plan_repair 计划、确认点、风险和影响说明。",
                "用户确认后才允许进入 *_confirmed 工具或修复执行，并在执行后复查状态。",
            ]
            clarification_questions = [
                "要检查全部本地数据，还是指定 dataset、schedule 或 sync target？",
                "是否确认本轮只做只读检查和修复计划，不启动同步/刷新/repair job？",
            ]
            action_proposals = [
                {"title": "Local data health overview", "risk": "low", "approval_required": False, "status": "read_only"},
                {"title": "生成 local_data_plan_repair 修复计划", "risk": "medium", "approval_required": False, "status": "plan_only"},
                {"title": "local_data_apply_repair_confirmed", "risk": "production_sensitive", "approval_required": True, "status": "waiting_confirmation"},
            ]
        show_plan_card = bool(mode_cfg.get("show_plan_card", True))
        show_clarification_card = bool(mode_cfg.get("show_clarification_card", True))
        details_default_collapsed = bool(mode_cfg.get("details_default_collapsed", mode_decision.mode in {DialogueMode.DIALOGUE, DialogueMode.ANALYSIS}))
        if mode_decision.mode in {DialogueMode.DIALOGUE, DialogueMode.ANALYSIS}:
            max_questions = int(mode_cfg.get("max_clarification_questions", 0))
            clarification_questions = clarification_questions[:max_questions]
            action_proposals = []
            plan_steps = []
        cards = {
            "intent_type": dialogue_intent.value,
            "dialogue_mode": mode_decision.mode.value,
            "mode_decision": mode_decision.as_dict(),
            "action_proposals": action_proposals,
            "capability_cards": capability_cards,
            "missing_capability_keys": missing_capability_keys,
            "status_rail": self._status_rail(status_rail_ref, intent_config),
            "capability_summary": capability_summary,
            "safety": dict(intent_config.get("safety") or {}),
            "main_reply_policy": {
                "expose_context_pack": bool(mode_cfg.get("expose_context_pack")),
                "expose_audit_fields": bool(mode_cfg.get("expose_audit_fields")),
                "raw_json_main_view": bool(mode_cfg.get("raw_json_main_view", False)),
            },
            "ui_display": {
                "show_plan_card": show_plan_card,
                "show_clarification_card": show_clarification_card,
                "show_context_health_badge": bool(mode_cfg.get("show_context_health_badge", True)),
                "details_default_collapsed": details_default_collapsed,
            },
        }
        cards["mcp_route_decision"] = mcp_route
        if mcp_route.get("server_key"):
            cards["capability_summary"]["route"] = f"{mcp_route.get('server_key')}/{mcp_route.get('tool_name')}"
            cards["capability_summary"]["route_reason"] = mcp_route.get("reason")
        if is_local_data:
            cards["capability_summary"].update(
                {
                    "mcp": "已识别 aistock-local-data MCP：优先使用 local_data_health_overview、local_data_get_dataset_status、local_data_list_sync_targets 和 local_data_plan_repair；确认前不调用 repair/sync confirmed 工具。",
                    "skill": "已纳入 local_data_management capability，用于本地数据健康检查、同步目标排查和修复计划。",
                    "local_data_management": "本地数据管理按检查、计划、确认、执行、复查闭环处理。",
                    "mcp_tools": [
                        "local_data_health_overview",
                        "local_data_get_dataset_status",
                        "local_data_get_preset_daily_status",
                        "local_data_list_jobs",
                        "local_data_list_sync_targets",
                        "local_data_plan_repair",
                        "local_data_apply_repair_confirmed",
                    ],
                }
            )
            cards["safety"].update({"local_data_read_only_before_confirmation": True, "no_data_job_before_confirmation": True})
            cards["local_data_management"] = {
                "capability_key": "local_data_management",
                "mcp_server": "aistock-local-data",
                "phases": [
                    {"key": "check", "status": "done"},
                    {"key": "plan", "status": "done"},
                    {"key": "confirm", "status": "current"},
                    {"key": "execute", "status": "locked"},
                    {"key": "review", "status": "locked"},
                ],
            }
            cards["local_data_phases"] = cards["local_data_management"]["phases"]
        if self._is_mcp_tool_catalog_inquiry(user_message):
            cards["runtime_mcp_catalog"] = self._mcp_tool_catalog_snapshot()
        if show_plan_card:
            cards["plan_card"] = {
                "title": str(template.get("plan_title") or ""),
                "steps": plan_steps,
                "default_collapsed": details_default_collapsed,
            }
        if show_clarification_card and clarification_questions:
            cards["clarification_card"] = {
                "title": str(template.get("clarification_title") or ""),
                "questions": clarification_questions,
                "default_collapsed": details_default_collapsed,
            }
        if mcp_route.get("requires_clarification"):
            cards["clarification_card"] = {
                "title": "需要先确认比较口径",
                "questions": list(mcp_route.get("clarification_questions") or []),
                "default_collapsed": False,
            }
        return cards

    def _react_messages_for_agentic_synthesis(
        self,
        messages: list[dict[str, Any]],
        *,
        user_message: str,
        route_seeds: list[McpToolCall],
        route_candidates: list[dict[str, Any]],
        graph_context: dict[str, Any],
    ) -> list[dict[str, Any]]:
        react_messages = [dict(item) for item in messages]
        graph_relation_refs = graph_context.get("relation_refs") if isinstance(graph_context.get("relation_refs"), list) else []
        directive = {
            "type": "AGENTIC_REPLY_SYNTHESIS_DIRECTIVE",
            "instruction": AGENTIC_SYNTHESIS_SYSTEM_PROMPT,
            "user_message": user_message,
            "seeded_tool_calls": [
                {
                    "server_key": seed.server_key,
                    "tool_name": seed.tool_name,
                    "payload_json": seed.payload_json,
                    "reason": seed.reason,
                }
                for seed in route_seeds
            ],
            "route_candidates": [
                {
                    "domain": item.get("domain"),
                    "server_key": item.get("server_key"),
                    "tool_name": item.get("tool_name"),
                    "side_effect": item.get("side_effect"),
                    "candidate_score": item.get("candidate_score"),
                    "candidate_reason": item.get("candidate_reason"),
                }
                for item in route_candidates[:8]
            ],
            "graph_context": {
                "source": GRAPH_CONTEXT_SOURCE,
                "as_of": GRAPH_CONTEXT_AS_OF,
                "seed_entity_keys": list(graph_context.get("seed_entity_keys") or [])[:12],
                "neighbor_entity_keys": list(graph_context.get("neighbor_entity_keys") or [])[:12],
                "relation_refs": graph_relation_refs[:12],
            },
            "output_rules": [
                "If tool_calls are needed, emit native function calls or the structured JSON fallback only.",
                "After tool results are present, write the final answer yourself and address the exact user question.",
                "Do not return a Python-rendered template or copy a raw tool payload.",
                "Cite actual source and as_of/trade_date/report_period values found in tool results.",
                "For graph relationship facts, cite source graph_context and as_of LIVE.",
                "For cross-module/how-to/relationship questions, start from graph_context before tool evidence.",
                "Use more than one read-only tool when the question spans multiple domains and candidates are eligible.",
                "For future-looking questions, frame drivers, scenarios, and risks only; do not predict direction.",
                "If citation_options are present, cite their exact source/as_of strings for the relevant facts.",
            ],
        }
        if self._is_stock_depth_analysis_request(
            user_message,
            route_candidates[0] if route_candidates else {},
        ):
            directive["individual_stock_depth_analysis_policy"] = {
                "required_stock_tools": list(STOCK_DEPTH_STOCK_TOOL_NAMES),
                "required_external_tools": list(STOCK_DEPTH_EXTERNAL_TOOL_NAMES),
                "minimum_kline_trading_days": STOCK_DEPTH_MIN_HISTORY_TRADING_DAYS,
                "preferred_kline_period": STOCK_DEPTH_HISTORY_PERIOD,
                "required_dimensions": [
                    "quote/current market only as starting point",
                    "kline history of at least 60 trading days",
                    "technicals",
                    "fund_flow",
                    "margin_financing",
                    "financials",
                    "quarterly",
                    "external_research_search_web and fetch_extract for event/fundamental/industry context",
                ],
                "answer_policy": (
                    "For individual-stock depth questions, gather the full read-only data surface before drawing conclusions. "
                    "Do not treat a realtime quote or one-day K line as enough evidence. Start with 1-2 bottom-line sentences, "
                    "then synthesize cited observations by dimension. Future-looking content must be drivers/scenarios/risks only, "
                    "with no directional prediction and no investment advice. If a required data source is unavailable, say which one "
                    "is missing with its reason_code instead of inventing facts."
                ),
            }
        react_messages.append({"role": "system", "content": json.dumps(directive, ensure_ascii=False, sort_keys=True)})
        return react_messages

    def _complete_chat_with_react_grounding(
        self,
        *,
        user_message: str,
        conversation_id: str,
        task: dict[str, Any],
        context_pack: dict[str, Any],
        messages: list[dict[str, str]],
        first_llm_result: LlmCallResult,
        cards: dict[str, Any],
        model_profile: dict[str, Any],
        budget_plan: ContextBudgetPlan,
        runtime_config: dict[str, Any],
        mode_decision: ModeDecision,
        function_tools: list[dict[str, Any]] | None = None,
        function_tool_registry: dict[str, dict[str, Any]] | None = None,
    ) -> tuple[LlmCallResult, list[dict[str, str]], Any]:
        route_seeds = self._seeded_react_tool_calls(cards, mode_decision)
        route_candidates = self._route_candidates_from_cards(cards)
        if route_seeds and first_llm_result.tool_calls:
            # Native function calls take precedence over route seed suggestions.
            route_seeds = []
        graph_context = self._graph_context_from_context_pack(context_pack)
        react_messages = self._react_messages_for_agentic_synthesis(
            messages,
            user_message=user_message,
            route_seeds=route_seeds,
            route_candidates=route_candidates,
            graph_context=graph_context,
        )
        first_turn_consumed = False

        def model_complete(next_messages: list[dict[str, Any]]) -> ModelTurn:
            nonlocal first_turn_consumed
            if not first_turn_consumed and not route_seeds:
                first_turn_consumed = True
                return ModelTurn(
                    content=first_llm_result.content,
                    provider=first_llm_result.provider,
                    model=first_llm_result.model,
                    duration_ms=first_llm_result.duration_ms,
                    usage=first_llm_result.usage,
                    usage_event=first_llm_result.usage_event,
                    tool_calls=list(first_llm_result.tool_calls or []),
                )
            first_turn_consumed = True
            result = self.llm_client.complete(
                messages=next_messages,
                model_profile=model_profile,
                temperature=budget_plan.llm_temperature,
                max_tokens=budget_plan.llm_max_tokens,
                tools=function_tools,
                tool_choice="auto" if function_tools else None,
                tool_registry=function_tool_registry,
            )
            return ModelTurn(
                content=result.content,
                provider=result.provider,
                model=result.model,
                duration_ms=result.duration_ms,
                usage=result.usage,
                usage_event=result.usage_event,
                tool_calls=list(result.tool_calls or []),
            )

        provider = _ServiceReactMcpProvider(
            service=self,
            conversation_id=conversation_id,
            task=task,
            context_pack=context_pack,
            cards=cards,
            user_message=user_message,
        )
        def fallback_tool_calls() -> list[McpToolCall]:
            return self._grounded_route_fallback_tool_calls(cards, mode_decision)

        initial_tool_results = [graph_result] if (graph_result := self._graph_context_tool_result(graph_context)) else None
        catalog_entries = self._react_tool_catalog_entries(mode_decision=mode_decision)
        if (
            not catalog_entries
            and str(mode_decision.allowed_tool_side_effect or "none") == "none"
            and (first_llm_result.tool_calls or extract_structured_tool_calls(first_llm_result.content))
        ):
            catalog_entries = self._react_tool_catalog_entries(capability_backed_side_effect_only=True)
        react_result = run_react_grounding_loop(
            messages=react_messages,
            model_complete=model_complete,
            mcp_provider=provider,
            catalog_entries=catalog_entries,
            config=self._react_grounding_config(runtime_config, user_message=user_message, token_budget=budget_plan.effective_window_tokens),
            seeded_tool_calls=route_seeds or None,
            initial_tool_results=initial_tool_results,
            fallback_tool_calls=fallback_tool_calls,
            tool_result_compactor=self._compact_tool_result_with_auxiliary_model,
        )
        final_turn = react_result.model_turns[-1] if react_result.model_turns else ModelTurn(
            content=react_result.final_text,
            provider=first_llm_result.provider,
            model=first_llm_result.model,
            duration_ms=first_llm_result.duration_ms,
            usage=first_llm_result.usage,
            usage_event=first_llm_result.usage_event,
        )
        grounded_llm_result = LlmCallResult(
            content=react_result.final_text,
            provider=final_turn.provider,
            model=final_turn.model,
            duration_ms=sum(turn.duration_ms for turn in react_result.model_turns) or first_llm_result.duration_ms,
            usage=self._merge_llm_usage([turn.usage for turn in react_result.model_turns]),
            tool_calls=list(final_turn.tool_calls or []),
        )
        return grounded_llm_result, [dict(item) for item in react_result.messages], react_result

    def _should_finish_with_business_summary_tool(self, call: McpToolCall, cards: dict[str, Any]) -> bool:
        return False

    def _react_grounding_config(self, runtime_config: dict[str, Any], *, user_message: str = "", token_budget: int | None = None) -> ReactGroundingConfig:
        cfg = runtime_config.get("react_grounding") if isinstance(runtime_config.get("react_grounding"), dict) else {}
        if "max_tool_iterations" not in cfg:
            raise KeyError("Research Assistant runtime config missing react_grounding.max_tool_iterations")
        configured_iterations = int(cfg["max_tool_iterations"])
        ra_cfg = runtime_config.get("research_assistant") if isinstance(runtime_config.get("research_assistant"), dict) else {}
        guard_mode = ra_cfg.get("guard_mode") if "guard_mode" in ra_cfg else cfg.get("guard_mode")
        return ReactGroundingConfig(
            max_tool_iterations=max(configured_iterations, 10),
            evidence_required=bool(cfg.get("evidence_required", True)),
            guard_mode=str(guard_mode) if guard_mode is not None else None,
            user_message=user_message,
            token_budget=int(cfg.get("token_budget") or token_budget) if (cfg.get("token_budget") or token_budget) else None,
            placeholder_patterns=tuple(str(item) for item in cfg.get("placeholder_patterns", [r"\bXX\b", r"\bX%\b", "approxX", "about X"])),
            forbidden_answer_markers=AGENTIC_SYNTHESIS_FORBIDDEN_MARKERS,
        )

    @staticmethod
    def _merge_llm_usage(usages: list[dict[str, Any]]) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        for usage in usages:
            for key, value in (usage or {}).items():
                if isinstance(value, (int, float)):
                    merged[key] = merged.get(key, 0) + value
                else:
                    merged[key] = value
        return merged

    def _react_tool_catalog_entries(
        self,
        *,
        capability_backed_only: bool = False,
        capability_backed_side_effect_only: bool = False,
        mode_decision: ModeDecision | None = None,
    ) -> list[ToolCatalogEntry]:
        if mode_decision is not None:
            tools = self._agentic_mcp_catalog_records_for_mode(mode_decision)
        elif capability_backed_side_effect_only:
            tools = self._capability_backed_side_effect_mcp_catalog_records()
        else:
            tools = self._capability_backed_mcp_catalog_records() if capability_backed_only else self._manifest_mcp_catalog_records()
        return [
            ToolCatalogEntry(
                server_key=str(tool.get("server_key") or ""),
                tool_name=str(tool.get("tool_name") or ""),
                status=str(tool.get("status") or ""),
                risk_level=str(tool.get("risk_level") or "medium"),
                side_effect_level=str(tool.get("side_effect_level") or "read_only"),
                requires_approval=bool(tool.get("requires_approval")),
                required_confirmations=tuple(str(item) for item in (tool.get("required_confirmations") or [])),
            )
            for tool in tools
            if tool.get("server_key") and tool.get("tool_name")
        ]

    @staticmethod
    def _graph_context_from_context_pack(context_pack: dict[str, Any]) -> dict[str, Any]:
        pack_json = context_pack.get("pack_json") if isinstance(context_pack.get("pack_json"), dict) else {}
        graph_context = pack_json.get("graph_context") if isinstance(pack_json.get("graph_context"), dict) else {}
        return dict(graph_context)

    def _should_run_react_grounding(
        self,
        *,
        user_message: str,
        cards: dict[str, Any],
        context_pack: dict[str, Any],
        first_llm_result: LlmCallResult,
        mode_decision: ModeDecision,
    ) -> bool:
        route = cards.get("mcp_route_decision") if isinstance(cards, dict) else {}
        if isinstance(route, dict) and route.get("requires_clarification"):
            return False
        if (
            mode_decision.intent_type == DialogueIntent.EXPERIMENT_DRAFT_REQUEST
            and isinstance(route, dict)
            and str(route.get("domain") or "") == "qe_experiment"
            and str(route.get("tool_name") or "") in set(DOMAIN_SPECS[McpDomain.QE_EXPERIMENT].plan_tools)
        ):
            return bool(first_llm_result.tool_calls)
        if mode_decision.intent_type in {DialogueIntent.CAPABILITY_INQUIRY, DialogueIntent.MCP_CAPABILITY_INQUIRY}:
            return bool(first_llm_result.tool_calls)
        graph_context = self._graph_context_from_context_pack(context_pack)
        if graph_context.get("relation_refs"):
            return True
        if first_llm_result.tool_calls or extract_structured_tool_calls(first_llm_result.content):
            return True
        policy = route.get("agentic_route_policy") if isinstance(route, dict) and isinstance(route.get("agentic_route_policy"), dict) else {}
        if policy.get("allow_multi_tool"):
            return True
        if isinstance(route, dict) and route.get("server_key") and route.get("tool_name"):
            return bool(self._seeded_react_tool_calls(cards, mode_decision))
        return False

    @staticmethod
    def _empty_react_grounding_result(first_llm_result: LlmCallResult) -> ReactGroundingResult:
        return ReactGroundingResult(
            final_text=first_llm_result.content,
            messages=[],
            tool_calls=[],
            tool_results=[],
            trace_steps=[{"iteration": 0, "react_grounding": "skipped"}],
            evidence_guard=EvidenceGuardDecision(True, first_llm_result.content, "skipped", 0, 0),
            iterations=0,
            stopped_reason="skipped",
            model_turns=[
                ModelTurn(
                    content=first_llm_result.content,
                    provider=first_llm_result.provider,
                    model=first_llm_result.model,
                    duration_ms=first_llm_result.duration_ms,
                    usage=first_llm_result.usage,
                    usage_event=first_llm_result.usage_event,
                    tool_calls=list(first_llm_result.tool_calls or []),
                )
            ],
        )

    @staticmethod
    def _route_candidates_from_cards(cards: dict[str, Any]) -> list[dict[str, Any]]:
        route = cards.get("mcp_route_decision") if isinstance(cards, dict) else {}
        if not isinstance(route, dict):
            return []
        raw = route.get("route_candidates") if isinstance(route.get("route_candidates"), list) else []
        candidates = [dict(item) for item in raw if isinstance(item, dict)]
        if not candidates and route.get("server_key") and route.get("tool_name"):
            candidates.insert(0, dict(route))
        deduped: dict[tuple[str, str], dict[str, Any]] = {}
        for candidate in candidates:
            key = (str(candidate.get("server_key") or ""), str(candidate.get("tool_name") or ""))
            if key[0] and key[1] and key not in deduped:
                deduped[key] = candidate
        return list(deduped.values())

    def _react_tool_candidates_for_grounding(self, cards: dict[str, Any], mode_decision: ModeDecision) -> list[dict[str, Any]]:
        route = cards.get("mcp_route_decision") if isinstance(cards, dict) else {}
        candidates = self._route_candidates_from_cards(cards)
        if not candidates:
            return []
        policy = route.get("agentic_route_policy") if isinstance(route, dict) and isinstance(route.get("agentic_route_policy"), dict) else {}
        if policy.get("allow_multi_tool"):
            eligible_candidates = [
                candidate
                for candidate in candidates
                if self._react_tool_call_from_route_candidate(candidate, mode_decision, stable_prefix="probe") is not None
            ]
            stock_analysis_seeds = [
                candidate
                for candidate in eligible_candidates
                if str(candidate.get("candidate_reason") or "") == "stock_analysis_route_seed"
            ]
            if stock_analysis_seeds:
                stock_keys = {("aistock-stock-analysis", tool_name) for tool_name in STOCK_DEPTH_STOCK_TOOL_NAMES}
                external_search_key = ("aistock-external-research", "external_research_search_web")
                stock_selected = [
                    candidate
                    for candidate in stock_analysis_seeds
                    if (str(candidate.get("server_key") or ""), str(candidate.get("tool_name") or "")) in stock_keys
                ]
                external_selected = next(
                    (
                        candidate
                        for candidate in stock_analysis_seeds
                        if (str(candidate.get("server_key") or ""), str(candidate.get("tool_name") or "")) == external_search_key
                    ),
                    None,
                )
                if external_selected is not None:
                    stock_selected.append(external_selected)
                return stock_selected
            return eligible_candidates or candidates[:1]
        return candidates[:1]

    def _react_tool_call_from_route_candidate(
        self,
        candidate: dict[str, Any],
        mode_decision: ModeDecision,
        *,
        stable_prefix: str,
        fallback: bool = False,
    ) -> McpToolCall | None:
        route = dict(candidate)
        if not isinstance(route, dict) or not route.get("server_key") or not route.get("tool_name"):
            return None
        if self._stock_analysis_route_needs_model_symbol(route):
            return None
        if fallback and str(route.get("side_effect") or "read_only") != "read_only":
            return None
        eligibility = route.get("auto_execute") if isinstance(route.get("auto_execute"), dict) else self._read_only_mcp_auto_execution_eligibility(route, mode_decision)
        route["auto_execute"] = eligibility
        if eligibility.get("eligible"):
            return McpToolCall(
                server_key=str(route["server_key"]),
                tool_name=str(route["tool_name"]),
                payload_json={
                    "request": route.get("request") or route.get("user_message") or "",
                    "route": route,
                    "mcp_route_decision": route,
                    **self._mcp_route_tool_args(route),
                    "limit": self._mcp_route_limit(route),
                },
                stable_call_id=f"{stable_prefix}:{route['server_key']}:{route['tool_name']}",
                reason=str(route.get("candidate_reason") or route.get("reason") or ("evidence_guard_route_fallback" if fallback else "route_seed")),
            )
        return None

    def _seeded_react_tool_calls(self, cards: dict[str, Any], mode_decision: ModeDecision) -> list[McpToolCall]:
        calls: list[McpToolCall] = []
        for candidate in self._react_tool_candidates_for_grounding(cards, mode_decision):
            call = self._react_tool_call_from_route_candidate(candidate, mode_decision, stable_prefix="route")
            if call:
                calls.append(call)
        return self._dedupe_mcp_tool_calls(calls)

    def _grounded_route_fallback_tool_calls(self, cards: dict[str, Any], mode_decision: ModeDecision) -> list[McpToolCall]:
        calls: list[McpToolCall] = []
        for candidate in self._react_tool_candidates_for_grounding(cards, mode_decision):
            call = self._react_tool_call_from_route_candidate(candidate, mode_decision, stable_prefix="fallback", fallback=True)
            if call:
                calls.append(call)
        return self._dedupe_mcp_tool_calls(calls)

    @staticmethod
    def _dedupe_mcp_tool_calls(calls: list[McpToolCall]) -> list[McpToolCall]:
        deduped: dict[tuple[str, str], McpToolCall] = {}
        for call in calls:
            key = (call.server_key, call.tool_name)
            deduped.setdefault(key, call)
        return list(deduped.values())

    @staticmethod
    def _graph_context_tool_result(graph_context: dict[str, Any]) -> McpToolResult | None:
        relation_refs = graph_context.get("relation_refs") if isinstance(graph_context.get("relation_refs"), list) else []
        if not relation_refs:
            return None
        payload = {
            "response_mode": "graph_context",
            "source": GRAPH_CONTEXT_SOURCE,
            "source_refs": [GRAPH_CONTEXT_SOURCE],
            "as_of": GRAPH_CONTEXT_AS_OF,
            "graph_context": {
                "seed_entity_keys": list(graph_context.get("seed_entity_keys") or [])[:12],
                "neighbor_entity_keys": list(graph_context.get("neighbor_entity_keys") or [])[:12],
                "relation_refs": relation_refs[:12],
            },
        }
        return McpToolResult(
            server_key="research-assistant",
            tool_name="graph_context",
            status="succeeded",
            payload_json=payload,
            source_refs=[GRAPH_CONTEXT_SOURCE],
            as_of=GRAPH_CONTEXT_AS_OF,
            summary=f"{len(relation_refs)} graph_context relations available for synthesis",
            executed=True,
            stable_call_id="graph_context",
        )

    @staticmethod
    def _stock_analysis_route_needs_model_symbol(route: dict[str, Any]) -> bool:
        if str(route.get("domain") or "") != "stock_analysis":
            return False
        tool_name = str(route.get("tool_name") or "")
        if not tool_name.startswith("stock_analysis_"):
            return False
        args = route.get("tool_args") if isinstance(route.get("tool_args"), dict) else {}
        return not any(route.get(key) or args.get(key) for key in ("symbol", "ts_code", "stock_code"))

    @staticmethod
    def _react_grounding_card(react_result: Any) -> dict[str, Any]:
        tool_errors: list[dict[str, Any]] = []
        tool_results = list(getattr(react_result, "tool_results", []) or [])
        executed_tools: list[dict[str, Any]] = []
        seen_executed: set[tuple[str, str]] = set()
        for index, result in enumerate(tool_results):
            if bool(getattr(result, "executed", False)) and str(getattr(result, "status", "")) in {"succeeded", "success", "ok"}:
                executed_key = (str(getattr(result, "server_key", "")), str(getattr(result, "tool_name", "")))
                if executed_key[0] and executed_key[1] and executed_key not in seen_executed:
                    seen_executed.add(executed_key)
                    executed_tools.append(
                        {
                            "server_key": executed_key[0],
                            "tool_name": executed_key[1],
                            "status": str(getattr(result, "status", "")),
                            "side_effect_level": str(getattr(result, "side_effect_level", "read_only") or "read_only"),
                        }
                    )
            error = result.error_json if isinstance(result.error_json, dict) else {}
            reason_code = str(error.get("reason_code") or error.get("code") or result.blocked_reason or "")
            if result.status in {"failed", "rejected"} and reason_code in ResearchAssistantService.PROGRAM_ERROR_REASON_CODES:
                later_success = any(
                    bool(getattr(item, "executed", False))
                    and str(getattr(item, "status", "")) in {"succeeded", "success", "ok"}
                    for item in tool_results[index + 1 :]
                )
                terminal_program_error = not (later_success and error.get("recoverable_catalog_rejection"))
                item = dict(error)
                item.setdefault("reason_code", reason_code)
                item.setdefault("server_key", result.server_key)
                item.setdefault("tool_name", result.tool_name)
                item.setdefault("message", result.summary)
                item.setdefault("side_effect_level", str(getattr(result, "side_effect_level", "read_only") or "read_only"))
                item["terminal_program_error"] = terminal_program_error
                if not terminal_program_error:
                    item["diagnostic_only"] = True
                tool_errors.append(item)
        return {
            "schema_version": "research_assistant_react_grounding_v1",
            "iterations": react_result.iterations,
            "tool_call_count": len(react_result.tool_calls),
            "tool_result_count": len(react_result.tool_results),
            "stopped_reason": react_result.stopped_reason,
            "executed_tools": executed_tools,
            "tool_errors": tool_errors,
            "evidence_guard": {
                "allowed": react_result.evidence_guard.allowed,
                "reason": react_result.evidence_guard.reason,
                "source_count": react_result.evidence_guard.source_count,
                "as_of_count": react_result.evidence_guard.as_of_count,
            },
        }

    def _populate_cards_from_react_program_error(self, cards: dict[str, Any], react_result: Any, task_id: str) -> None:
        if isinstance(cards.get("mcp_execution_result"), dict):
            return
        react_card = cards.get("react_grounding") if isinstance(cards.get("react_grounding"), dict) else {}
        tool_errors = react_card.get("tool_errors") if isinstance(react_card.get("tool_errors"), list) else []
        error = next(
            (item for item in tool_errors if isinstance(item, dict) and item.get("terminal_program_error") is not False),
            None,
        )
        if not error:
            return
        server_key = str(error.get("server_key") or "")
        tool_name = str(error.get("tool_name") or "")
        cards["mcp_execution_result"] = {
            "auto_executed": False,
            "executed": False,
            "status": "failed",
            "route": f"{server_key}/{tool_name}",
            "server_key": server_key,
            "tool_name": tool_name,
            "summary_first": True,
            "error": error,
        }
        try:
            self.add_task_event(
                task_id,
                TaskEventCreate(
                    event_type="mcp_failed",
                    severity="error",
                    message=self._render_tool_error_reply(error),
                    payload_json={"error": error, "route": f"{server_key}/{tool_name}", "source": "react_grounding"},
                ),
            )
        except Exception:  # noqa: BLE001 - error-card creation must not re-crash chat/turn.
            logger.exception("failed to persist ReAct program-error event for %s/%s", server_key, tool_name)


    @staticmethod
    def _is_business_synthesis_summary(summary: dict[str, Any]) -> bool:
        if not isinstance(summary, dict):
            return False
        if str(summary.get("response_mode") or "") in BUSINESS_SYNTHESIS_RESPONSE_MODES:
            return True
        return bool(summary.get("local_data_daily_status"))

    @staticmethod
    def _contains_agentic_template_marker(text: str) -> bool:
        lowered = text.lower()
        return any(marker and marker.lower() in lowered for marker in AGENTIC_SYNTHESIS_FORBIDDEN_MARKERS)

    @staticmethod
    def _business_synthesis_failure_text(text: str) -> str:
        if ResearchAssistantService._is_insufficient_evidence_text(text):
            return text
        return "Insufficient evidence: business reply synthesis did not pass grounding guard."

    @staticmethod
    def _react_has_grounded_business_tool_execution(
        react_active: bool,
        react_card: dict[str, Any],
        react_guard: dict[str, Any],
    ) -> bool:
        if (
            not react_active
            or react_guard.get("allowed") is not True
        ):
            return False
        guard_reason = str(react_guard.get("reason") or "")
        if not (
            guard_reason in {"ok", "read_only_partial_evidence_degraded"}
            or guard_reason.startswith("guard_disabled")
            or guard_reason.startswith("annotated:")
        ):
            return False
        executed_tools = react_card.get("executed_tools") if isinstance(react_card.get("executed_tools"), list) else []
        for item in executed_tools:
            if not isinstance(item, dict):
                continue
            server_key = str(item.get("server_key") or "")
            tool_name = str(item.get("tool_name") or "")
            side_effect = str(item.get("side_effect_level") or "read_only")
            if not server_key or not tool_name:
                continue
            if server_key == "research-assistant":
                continue
            if side_effect == "read_only":
                return True
        return False

    @staticmethod
    def _strip_mcp_business_forbidden_marker_tokens(text: str) -> str:
        cleaned = text
        for marker in sorted(MCP_BUSINESS_REPLY_FORBIDDEN_MARKERS, key=len, reverse=True):
            if marker:
                cleaned = re.sub(re.escape(marker), "", cleaned, flags=re.IGNORECASE)
        return cleaned

    @staticmethod
    def _mcp_business_text_signal_count(text: str) -> int:
        return len(re.findall(r"[A-Za-z0-9\u4e00-\u9fff]", text))

    @staticmethod
    def _mcp_business_line_has_substantive_residue(line: str) -> bool:
        if not ResearchAssistantService._contains_mcp_business_forbidden_marker(line):
            return True
        stripped = ResearchAssistantService._strip_mcp_business_forbidden_marker_tokens(line)
        stripped_count = ResearchAssistantService._mcp_business_text_signal_count(stripped)
        return stripped_count > 0

    @staticmethod
    def _clean_mcp_business_forbidden_markers(text: str) -> str:
        replacements = {
            "Evidence: source=": "证据来源：",
            "server_key=": "服务：",
            "tool_name=": "工具：",
            "source=": "来源：",
            "as_of=": "截至：",
            "server_key": "服务",
            "tool_name": "工具",
            "selected_tool": "已选工具",
            "detail_tool": "详情工具",
            "detail tool": "详情工具",
            "raw_payload": "原始数据",
            "omitted_sections": "省略的内部字段",
            "mcp_summary_result": "工具摘要",
            "mcp_execution_result": "工具执行结果",
            "response_mode": "回复模式",
            "summary_envelope": "摘要信封",
            "summary_adapter": "摘要适配器",
            "research_assistant_catalog_summary_adapter": "目录摘要适配器",
            "mcp_tool_event": "工具事件",
            "mcp route": "工具路由",
            "Route decision": "路由判断",
            "route decision": "路由判断",
            "artifact_ref": "产物引用",
            "payload budget": "载荷预算",
            "transport": "传输通道",
            "summary-first": "摘要优先",
            "summary_first": "摘要优先",
            "我只展示概要": "仅展示概要",
        }
        kept_lines: list[str] = []
        for line in text.splitlines():
            if not ResearchAssistantService._mcp_business_line_has_substantive_residue(line):
                continue
            cleaned_line = line
            for marker, replacement in sorted(replacements.items(), key=lambda item: len(item[0]), reverse=True):
                cleaned_line = re.sub(re.escape(marker), replacement, cleaned_line, flags=re.IGNORECASE)
            cleaned_line = re.sub(r"[ \t]{2,}", " ", cleaned_line).strip(" \t:：,，;；")
            if cleaned_line:
                kept_lines.append(cleaned_line)
        cleaned = "\n".join(kept_lines).strip()
        if ResearchAssistantService._contains_mcp_business_forbidden_marker(cleaned):
            cleaned = ResearchAssistantService._strip_mcp_business_forbidden_marker_tokens(cleaned)
            cleaned = re.sub(r"[ \t]{2,}", " ", cleaned).strip(" \t:：,，;；")
        return cleaned

    @staticmethod
    def _mcp_business_cleaned_text_has_substance(text: str, *, original_text: str | None = None) -> bool:
        if ResearchAssistantService._mcp_business_text_signal_count(text) < 12:
            return False
        if not original_text or not ResearchAssistantService._contains_mcp_business_forbidden_marker(original_text):
            return True
        stripped_original = ResearchAssistantService._strip_mcp_business_forbidden_marker_tokens(original_text)
        raw_count = max(ResearchAssistantService._mcp_business_text_signal_count(original_text), 1)
        stripped_count = ResearchAssistantService._mcp_business_text_signal_count(stripped_original)
        return stripped_count >= 12 and (stripped_count / raw_count) >= 0.45

    @staticmethod
    def _mcp_result_source_refs(summary_result: dict[str, Any], tool_event: dict[str, Any]) -> list[str]:
        source = summary_result.get("source") or tool_event.get("transport") or tool_event.get("tool_event_id") or "mcp_tool_event"
        refs = [str(source)] if source else []
        source_refs = summary_result.get("source_refs") if isinstance(summary_result.get("source_refs"), list) else []
        for item in source_refs:
            ref = str(item)
            if ref and ref not in refs:
                refs.append(ref)
        evidence_sources = summary_result.get("evidence_sources") if isinstance(summary_result.get("evidence_sources"), list) else []
        for item in evidence_sources:
            ref = str(item)
            if ref and ref not in refs:
                refs.append(ref)
        return refs

    @staticmethod
    def _mcp_result_as_of(summary_result: dict[str, Any]) -> str:
        return str(summary_result.get("as_of") or summary_result.get("trade_date") or utc_now().date().isoformat())


    @staticmethod
    def _capability_has_tool_ref(capability: dict[str, Any], server_key: str, tool_name: str) -> bool:
        refs = capability.get("mcp_tool_refs") if isinstance(capability.get("mcp_tool_refs"), list) else []
        return any(
            isinstance(ref, dict)
            and ref.get("server_key") == server_key
            and ref.get("tool_name") == tool_name
            for ref in refs
        )

    def _refresh_capability_cache_for_tool(
        self,
        *,
        server_key: str,
        tool_name: str,
        capability_key: str | None = None,
    ) -> dict[str, Any] | None:
        del server_key, tool_name, capability_key
        logger.warning("RA capability DB cache refresh is retired; YAML memory authority is used directly.")
        return None

    def _capability_key_for_tool(self, call: McpToolCall, route: dict[str, Any] | None = None) -> str:
        selected_tool = route.get("selected_tool") if isinstance(route, dict) and isinstance(route.get("selected_tool"), dict) else None
        if isinstance(selected_tool, dict) and selected_tool.get("domain"):
            route = selected_tool
        elif isinstance(route, dict):
            selected_tool = call.payload_json.get("selected_tool") if isinstance(call.payload_json.get("selected_tool"), dict) else None
            if isinstance(selected_tool, dict) and selected_tool.get("domain"):
                route.update(selected_tool)
        if isinstance(route, dict) and route.get("domain"):
            candidate = f"{route['domain']}.mcp_orchestration"
            capability = self._workflow_capability_by_key(candidate)
            if capability and self._capability_allows_tool(capability, call):
                return candidate
        inferred_domain = self._domain_for_mcp_tool(call.tool_name)
        if inferred_domain:
            candidate = f"{inferred_domain}.mcp_orchestration"
            capability = self._workflow_capability_by_key(candidate)
            if capability and self._capability_allows_tool(capability, call):
                return candidate
        capabilities = self._approved_workflow_capabilities()
        for capability in capabilities:
            if self._capability_allows_tool(capability, call):
                return str(capability["capability_key"])
        raise KeyError(f"approved capability not found for tool: {call.server_key}/{call.tool_name}")

    @staticmethod
    def _domain_for_mcp_tool(tool_name: str) -> str | None:
        for spec in DOMAIN_SPECS.values():
            if tool_name in {*spec.read_tools, *spec.plan_tools, *spec.confirmed_tools}:
                return spec.domain.value
        return None

    @staticmethod
    def _capability_allows_tool(capability: dict[str, Any], call: McpToolCall) -> bool:
        return ResearchAssistantService._capability_has_tool_ref(capability, call.server_key, call.tool_name)

    def _populate_cards_from_tool_execution(self, cards: dict[str, Any], proposal: dict[str, Any], executed: dict[str, Any], result: McpToolResult) -> None:
        tool_event = executed.get("tool_event") if isinstance(executed.get("tool_event"), dict) else {}
        summary_result = tool_event.get("response_json") if isinstance(tool_event.get("response_json"), dict) else {}
        existing_summary = cards.get("mcp_summary_result") if isinstance(cards.get("mcp_summary_result"), dict) else {}
        preserve_stock_summary = (
            existing_summary.get("response_mode") == "stock_analysis_evidence_card"
            and summary_result.get("response_mode") != "stock_analysis_evidence_card"
            and result.server_key == "aistock-external-research"
        )
        if not preserve_stock_summary:
            cards["mcp_summary_result"] = summary_result
        cards["mcp_result_cards"] = list(executed.get("human_cards") or [])
        cards["mcp_tool_event"] = {
            "tool_event_id": tool_event.get("tool_event_id"),
            "server_key": tool_event.get("server_key"),
            "tool_name": tool_event.get("tool_name"),
            "status": tool_event.get("status"),
            "transport": tool_event.get("transport"),
            "duration_ms": tool_event.get("duration_ms"),
            "artifact_refs": tool_event.get("artifact_refs") or [],
        }
        cards["mcp_execution_result"] = {
            "auto_executed": bool(executed.get("executed")),
            "status": executed.get("status"),
            "executed": bool(executed.get("executed")),
            "route": f"{result.server_key}/{result.tool_name}",
            "server_key": result.server_key,
            "tool_name": result.tool_name,
            "action_proposal_id": proposal["action_proposal_id"],
            "proposal_status": (executed.get("proposal") or {}).get("status") if isinstance(executed.get("proposal"), dict) else None,
            "tool_event_id": tool_event.get("tool_event_id"),
            "trace_id": executed.get("trace_id"),
            "summary_first": bool(summary_result.get("summary_first", True)),
            "response_summary": self._compact_mcp_summary_for_cards(summary_result),
            "human_cards": list(executed.get("human_cards") or []),
            "source_refs": list(result.source_refs),
            "as_of": result.as_of,
            "response_mode": summary_result.get("response_mode"),
        }
        if result.error_json:
            cards["mcp_execution_result"]["error"] = dict(result.error_json)
            cards.setdefault("tool_errors", [])
            if isinstance(cards["tool_errors"], list):
                cards["tool_errors"].append(dict(result.error_json))
        cards["status_rail"] = self._mcp_executed_status_rail()

    def _read_only_mcp_auto_execution_eligibility(self, route: dict[str, Any], mode_decision: ModeDecision) -> dict[str, Any]:
        if not route.get("server_key") or not route.get("tool_name") or not route.get("domain"):
            return {"eligible": False, "reason": "route_missing_tool"}
        if route.get("domain") in {"general"}:
            return {"eligible": False, "reason": "general_route"}
        qe_plan_tools = set(DOMAIN_SPECS[McpDomain.QE_EXPERIMENT].plan_tools)
        if route.get("domain") == "qe_experiment" and str(route.get("tool_name") or "") in qe_plan_tools:
            return {"eligible": False, "reason": "qe_draft_creation_uses_plan_reply"}
        if str(route.get("side_effect") or "read_only") != "read_only":
            return {"eligible": False, "reason": "route_not_read_only"}
        semantic_read_only_route = route.get("planner_source") == "llm_semantic_tool_planner" and str(route.get("side_effect") or "read_only") == "read_only"
        if mode_decision.allowed_tool_side_effect == "none" and not semantic_read_only_route:
            return {"eligible": False, "reason": "dialogue_mode_disallows_tools"}
        try:
            tool, _server = self._resolve_mcp_catalog_tool(str(route["server_key"]), str(route["tool_name"]))
        except KeyError:
            return {"eligible": False, "reason": "tool_not_registered"}
        if str(tool.get("status") or "") not in {"enabled", "ready", "approved"}:
            return {"eligible": False, "reason": "tool_not_enabled", "tool_status": tool.get("status")}
        if str(tool.get("risk_level") or "") != "low" or str(tool.get("side_effect_level") or "") != "read_only":
            return {
                "eligible": False,
                "reason": "tool_requires_gate",
                "risk_level": tool.get("risk_level"),
                "side_effect_level": tool.get("side_effect_level"),
            }
        capability_key = f"{route['domain']}.mcp_orchestration"
        capability = self._workflow_capability_by_key(capability_key)
        if not capability:
            return {"eligible": False, "reason": "capability_not_approved", "capability_key": capability_key}
        if self._mcp_ref_pair(route["server_key"], route["tool_name"]) not in self._approved_capability_mcp_tool_refs():
            return {"eligible": False, "reason": "capability_not_covering_tool", "capability_key": capability_key}
        return {
            "eligible": True,
            "reason": "low_risk_read_only_summary_first",
            "capability_key": capability_key,
            "risk_level": tool.get("risk_level"),
            "side_effect_level": tool.get("side_effect_level"),
        }

    @staticmethod
    def _compact_mcp_summary_for_cards(summary_result: dict[str, Any]) -> dict[str, Any]:
        items = summary_result.get("items") if isinstance(summary_result.get("items"), list) else []
        return {
            "source": summary_result.get("source"),
            "domain": summary_result.get("domain"),
            "total": summary_result.get("total"),
            "returned_count": len(items),
            "limit": summary_result.get("limit"),
            "offset": summary_result.get("offset"),
            "summary_first": summary_result.get("summary_first"),
            "response_mode": summary_result.get("response_mode"),
            "detail_tool": summary_result.get("detail_tool"),
            "detail_args_hint": summary_result.get("detail_args_hint") if isinstance(summary_result.get("detail_args_hint"), dict) else {},
            "artifact_ref_count": len(summary_result.get("artifact_refs") or []),
            "omitted_sections": list(summary_result.get("omitted_sections") or []),
        }

    @staticmethod
    def _mcp_executed_status_rail() -> list[dict[str, str]]:
        return [
            {"label": "接收任务", "status": "done"},
            {"label": "理解意图", "status": "done"},
            {"label": "MCP 路由", "status": "done"},
            {"label": "MCP 预检", "status": "done"},
            {"label": "只读执行", "status": "done"},
            {"label": "详情按需展开", "status": "idle"},
        ]

    @staticmethod
    def _dialogue_card_template(dialogue_intent: DialogueIntent, intent_config: dict[str, Any]) -> dict[str, Any]:
        templates = intent_config.get("card_templates") if isinstance(intent_config, dict) else {}
        if not isinstance(templates, dict):
            return {}
        template = templates.get(dialogue_intent.value) or templates.get(DialogueIntent.GENERAL_CHAT.value) or {}
        return dict(template) if isinstance(template, dict) else {}

    @staticmethod
    def _capability_cards(capabilities: list[dict[str, Any]], capability_keys: set[str]) -> list[dict[str, Any]]:
        return [
            {
                "capability_key": str(item.get("capability_key")),
                "title": str(item.get("title") or item.get("capability_key")),
                "risk": str(item.get("risk_level") or "medium"),
                "side_effect": str(item.get("side_effect_level") or "read_only"),
                "status": "available",
                "required_confirmations": item.get("required_confirmations") or [],
            }
            for item in capabilities
            if str(item.get("capability_key")) in capability_keys
        ]

    @staticmethod
    def _status_rail(mode: str, intent_config: dict[str, Any]) -> list[dict[str, str]]:
        rails = intent_config.get("status_rails") if isinstance(intent_config, dict) else {}
        if not isinstance(rails, dict):
            return []
        selected = rails.get(mode) or rails.get("answered") or []
        if not isinstance(selected, list):
            return []
        return [
            {"label": str(item.get("label") or ""), "status": str(item.get("status") or "idle")}
            for item in selected
            if isinstance(item, dict)
        ]

    def _compose_assistant_reply(self, user_message: str, llm_text: str, cards: dict[str, Any], mode_decision: ModeDecision) -> str:
        raw_text = llm_text.strip()
        text = self._strip_assistant_tool_choice_markup(raw_text)
        react_active = isinstance(cards.get("react_grounding"), dict)
        react_card = cards.get("react_grounding") if isinstance(cards.get("react_grounding"), dict) else {}
        react_guard = react_card.get("evidence_guard") if isinstance(react_card.get("evidence_guard"), dict) else {}
        grounded_business_answer_available = self._react_has_grounded_business_tool_execution(
            react_active,
            react_card,
            react_guard,
        )
        execution = cards.get("mcp_execution_result") if isinstance(cards, dict) else None
        tool_error = self._tool_error_from_cards(cards)
        if tool_error:
            return self._apply_main_reply_policy(self._render_tool_error_reply(tool_error), mode_decision)
        route = cards.get("mcp_route_decision") if isinstance(cards, dict) else None
        if isinstance(route, dict) and route.get("requires_clarification"):
            return self._apply_main_reply_policy(self._render_semantic_clarification_reply(route, cards), mode_decision)
        skill_reuse = cards.get("skill_reuse_result") if isinstance(cards, dict) else None
        if isinstance(skill_reuse, dict) and skill_reuse.get("proposal_type") == "skill":
            return self._apply_main_reply_policy(self._render_skill_reuse_preflight_reply(skill_reuse), mode_decision)
        react_guard_reason = str(react_guard.get("reason") or "")
        if (
            not grounded_business_answer_available
            and mode_decision.intent_type in {DialogueIntent.CAPABILITY_INQUIRY, DialogueIntent.MCP_CAPABILITY_INQUIRY}
            and (self._is_mcp_tool_catalog_inquiry(user_message) or "mcp" in user_message.lower() or "tool" in user_message.lower())
        ):
            catalog = cards.get("runtime_mcp_catalog") if isinstance(cards, dict) else None
            if isinstance(catalog, dict):
                return self._apply_main_reply_policy(self._render_mcp_tool_catalog_reply(catalog), mode_decision)
        if (
            react_active
            and react_guard.get("allowed") is True
            and (
                react_guard_reason in {"ok", "read_only_partial_evidence_degraded"}
                or react_guard_reason.startswith("guard_disabled")
                or react_guard_reason.startswith("annotated:")
            )
            and text
            and not self._is_insufficient_evidence_text(text)
            and not self._contains_agentic_template_marker(text)
        ):
            if self._contains_mcp_business_forbidden_marker(text):
                if grounded_business_answer_available:
                    cleaned_text = self._clean_mcp_business_forbidden_markers(text)
                    if (
                        cleaned_text
                        and self._mcp_business_cleaned_text_has_substance(cleaned_text, original_text=text)
                        and not self._contains_mcp_business_forbidden_marker(cleaned_text)
                    ):
                        return self._apply_main_reply_policy(cleaned_text, mode_decision)
                    if not (isinstance(execution, dict) and execution.get("auto_executed")):
                        return self._apply_main_reply_policy(self._business_synthesis_failure_text(text), mode_decision)
            else:
                return self._apply_main_reply_policy(text, mode_decision)
        if isinstance(execution, dict) and execution.get("auto_executed"):
            summary = cards.get("mcp_summary_result") if isinstance(cards.get("mcp_summary_result"), dict) else {}
            if self._is_business_synthesis_summary(summary):
                if text and not self._is_insufficient_evidence_text(text) and not self._contains_agentic_template_marker(text) and not self._contains_mcp_business_forbidden_marker(text):
                    return self._apply_main_reply_policy(text, mode_decision)
                return self._apply_main_reply_policy(self._business_synthesis_failure_text(text), mode_decision)
            if self._should_render_auto_mcp_execution_reply(execution, summary, react_active=react_active):
                return self._apply_main_reply_policy(self._render_mcp_execution_reply(execution, summary), mode_decision)
        if (
            react_active
            and isinstance(execution, dict)
            and execution.get("auto_executed")
            and react_card.get("stopped_reason") == "evidence_summary_fallback"
        ):
            summary = cards.get("mcp_summary_result") if isinstance(cards.get("mcp_summary_result"), dict) else {}
            if self._is_business_synthesis_summary(summary):
                return self._apply_main_reply_policy(self._business_synthesis_failure_text(text), mode_decision)
            return self._apply_main_reply_policy(self._render_react_execution_fallback_reply(execution, summary), mode_decision)
        if (
            isinstance(execution, dict)
            and execution.get("auto_executed")
            and text
            and self._contains_mcp_business_forbidden_marker(text)
        ):
            summary = cards.get("mcp_summary_result") if isinstance(cards.get("mcp_summary_result"), dict) else {}
            if self._is_business_synthesis_summary(summary):
                return self._apply_main_reply_policy(self._business_synthesis_failure_text(text), mode_decision)
            return self._apply_main_reply_policy(self._render_mcp_execution_reply(execution, summary), mode_decision)
        if mode_decision.intent_type == DialogueIntent.EXPERIMENT_DRAFT_REQUEST and self._is_insufficient_evidence_text(text):
            return self._apply_main_reply_policy(self._render_qe_draft_safe_reply(cards), mode_decision)
        if self._is_insufficient_evidence_text(text):
            if isinstance(execution, dict) and not execution.get("auto_executed"):
                preflight = cards.get("mcp_preflight_result") if isinstance(cards.get("mcp_preflight_result"), dict) else {}
                return self._apply_main_reply_policy(self._render_mcp_safe_preflight_reply(execution, preflight), mode_decision)
            if isinstance(route, dict) and str(route.get("side_effect") or "read_only") != "read_only":
                return self._apply_main_reply_policy(self._render_mcp_safe_preflight_reply(route, {}), mode_decision)
        if (not react_active or "<assistant_tool_choice" in raw_text.lower()) and self._should_render_mcp_route_reply(route, mode_decision, raw_text):
            return self._apply_main_reply_policy(self._render_mcp_route_reply(route), mode_decision)
        if text:
            return self._apply_main_reply_policy(text, mode_decision)
        intent_config = self._dialogue_intent_config()
        return self._apply_main_reply_policy(str(intent_config.get("fallback_reply") or user_message), mode_decision)

    @staticmethod
    def _should_render_auto_mcp_execution_reply(execution: dict[str, Any], summary: dict[str, Any], *, react_active: bool) -> bool:
        del execution
        if ResearchAssistantService._is_business_synthesis_summary(summary):
            return False
        if summary.get("response_mode") == "summary":
            return True
        return not react_active

    @staticmethod
    def _render_mcp_execution_reply(execution: dict[str, Any], summary: dict[str, Any]) -> str:
        return ResearchAssistantService._render_generic_mcp_business_reply(execution, summary)

    @staticmethod
    def _render_semantic_clarification_reply(route: dict[str, Any], cards: dict[str, Any]) -> str:
        clarification = cards.get("clarification_card") if isinstance(cards.get("clarification_card"), dict) else {}
        questions = clarification.get("questions") if isinstance(clarification.get("questions"), list) else route.get("clarification_questions")
        clean_questions = [str(item).strip() for item in (questions or []) if str(item).strip()]
        if not clean_questions:
            clean_questions = ["你希望按哪个指标比较：收益、Sharpe/IR、稳定性，还是最新状态？"]
        lines = [
            "这个问题需要先确认比较口径，我不会直接按默认状态列表或健康检查来回答。",
            "请先确认：" + clean_questions[0],
        ]
        if len(clean_questions) > 1:
            lines.extend(f"- {question}" for question in clean_questions[1:3])
        reason = str(route.get("reason") or "").strip()
        if reason:
            lines.append(f"原因：{reason}")
        return "\n".join(lines)

    @staticmethod
    def _humanize_business_identifier(value: str) -> str:
        raw = value.strip().strip("/")
        if "/" in raw:
            raw = raw.split("/")[-1]
        words = [part for part in raw.replace("-", "_").split("_") if part]
        acronyms = {"api", "bug", "ic", "mcp", "qe", "rankic", "url"}
        rendered = [word.upper() if word.lower() in acronyms else word for word in words]
        return " ".join(rendered) if rendered else "\u4e1a\u52a1\u67e5\u8be2"

    @staticmethod
    def _business_label_for_domain(domain: str, route: str) -> str:
        if domain:
            return ResearchAssistantService._humanize_business_identifier(domain)
        return ResearchAssistantService._humanize_business_identifier(route)

    @staticmethod
    def _friendly_field_label(key: str) -> str:
        labels = {
            "title": "\u540d\u79f0",
            "item_type": "\u7c7b\u578b",
            "risk_level": "\u98ce\u9669",
            "side_effect_level": "\u6267\u884c\u8fb9\u754c",
            "requires_approval": "\u9700\u5ba1\u6279",
            "status": "\u72b6\u6001",
            "summary": "\u6982\u8981",
            "query": "\u68c0\u7d22\u8bcd",
            "result_type": "\u7c7b\u578b",
            "url": "\u94fe\u63a5",
            "safety_boundary": "\u5b89\u5168\u8fb9\u754c",
            "requested_args": "\u6761\u4ef6",
            "next_action": "\u4e0b\u4e00\u6b65",
        }
        return labels.get(key, ResearchAssistantService._humanize_business_identifier(key))

    @staticmethod
    def _render_generic_business_item(item: dict[str, Any], *, max_fields: int = 7) -> str:
        hidden = {
            "server_key",
            "tool_name",
            "item_type",
            "risk_level",
            "side_effect_level",
            "requires_approval",
            "summary_first_contract",
            "detail_ref",
            "detail_tool",
            "detail_args_hint",
            "source",
            "provider",
            "as_of",
            "evidence_ref",
            "evidence_policy",
            "artifact_refs",
            "omitted_sections",
            "pagination",
        }

        def visible_value(value: Any) -> str:
            if value in (None, "", []):
                return ""
            if isinstance(value, dict):
                parts = []
                for nested_key, nested_value in value.items():
                    nested_key_text = str(nested_key)
                    if nested_key_text in hidden or ResearchAssistantService._contains_mcp_business_forbidden_marker(nested_key_text):
                        continue
                    nested_rendered = visible_value(nested_value)
                    if nested_rendered:
                        parts.append(f"{ResearchAssistantService._friendly_field_label(nested_key_text)}={nested_rendered}")
                    if len(parts) >= max_fields:
                        break
                return "\uff1b".join(parts)
            if isinstance(value, list):
                rendered_items = [visible_value(item) for item in value[:max_fields]]
                return "\uff0c".join(item for item in rendered_items if item)
            text = str(value)
            return "" if ResearchAssistantService._contains_mcp_business_forbidden_marker(text) else text

        title = visible_value(item.get("title") or item.get("summary") or item.get("item_type") or item.get("tool_name")) or "\u8bb0\u5f55"
        preferred = (
            "status",
            "summary",
            "result_type",
            "query",
            "url",
            "requested_args",
            "safety_boundary",
            "next_action",
        )
        bits: list[str] = []
        for key in preferred:
            value = item.get(key)
            if key in hidden or value in (None, "", []):
                continue
            rendered_value = visible_value(value)
            if not rendered_value:
                continue
            bits.append(f"{ResearchAssistantService._friendly_field_label(key)}={rendered_value}")
            if len(bits) >= max_fields:
                break
        if not bits:
            for key, value in item.items():
                key_text = str(key)
                if key_text in hidden or ResearchAssistantService._contains_mcp_business_forbidden_marker(key_text) or value in (None, "", [], {}):
                    continue
                rendered_value = visible_value(value)
                if not rendered_value:
                    continue
                bits.append(f"{ResearchAssistantService._friendly_field_label(key_text)}={rendered_value}")
                if len(bits) >= max_fields:
                    break
        return f"{title}\uff1a" + ("\uff1b".join(bits) if bits else "\u6682\u65e0\u53ef\u5c55\u793a\u7684\u5173\u952e\u5b57\u6bb5")

    @staticmethod
    def _render_generic_mcp_business_reply(execution: dict[str, Any], summary: dict[str, Any]) -> str:
        route = str(execution.get("route") or "")
        items = summary.get("items") if isinstance(summary.get("items"), list) else []
        domain = str(summary.get("domain") or "")
        label = ResearchAssistantService._business_label_for_domain(domain, route)
        total = summary.get("total")
        total_count = total if isinstance(total, int) else len(items)
        lines = [
            f"\u5df2\u5b8c\u6210{label}\u67e5\u8be2\u3002",
            f"\u6c47\u603b\uff1a\u5171 {total_count} \u9879\uff0c\u672c\u6b21\u5c55\u793a {len(items)} \u9879\u3002",
        ]
        if not items:
            lines.append("\u6682\u65e0\u53ef\u5c55\u793a\u7684\u4e1a\u52a1\u8bb0\u5f55\uff0c\u53ef\u4ee5\u6362\u68c0\u7d22\u6761\u4ef6\u6216\u6307\u5b9a\u5bf9\u8c61 ID \u7ee7\u7eed\u67e5\u770b\u3002")
        else:
            lines.append("\u660e\u7ec6\uff1a")
            for item in items[:20]:
                if isinstance(item, dict):
                    lines.append(f"- {ResearchAssistantService._render_generic_business_item(item)}")
        if len(items) > 20:
            lines.append(f"\u53e6\u6709 {len(items) - 20} \u9879\u672a\u5728\u4e3b\u56de\u590d\u5c55\u5f00\uff0c\u53ef\u7ee7\u7eed\u6309\u6761\u4ef6\u7b5b\u9009\u6216\u6307\u5b9a\u5355\u9879\u67e5\u770b\u3002")
        if domain == "external_research":
            lines.append("\u8fd9\u4e9b\u53ea\u662f\u5916\u90e8\u7814\u7a76\u7ebf\u7d22\uff0c\u672a\u4fdd\u5b58\u4e3a\u8bc1\u636e\uff0c\u4e0d\u7b49\u540c\u4e8e\u6700\u7ec8\u7ed3\u8bba\u3002")
        lines.append("\u672c\u8f6e\u53ea\u8fdb\u884c\u67e5\u8be2/\u6982\u8981\u6574\u7406\uff0c\u672a\u6267\u884c\u5199\u5165\u3001\u63d0\u4ea4\u3001\u8bad\u7ec3\u3001\u56de\u8865\u6216\u664b\u5347\u64cd\u4f5c\u3002")
        return "\n".join(lines)

    @staticmethod
    def _is_insufficient_evidence_text(text: str) -> bool:
        lower = text.lower()
        return "insufficient evidence" in lower or "max tool iterations reached" in lower

    @staticmethod
    def _contains_mcp_business_forbidden_marker(text: str) -> bool:
        lower = text.lower()
        return any(marker.lower() in lower for marker in MCP_BUSINESS_REPLY_FORBIDDEN_MARKERS)

    @staticmethod
    def _route_needs_concrete_target_hint(route: dict[str, Any]) -> bool:
        tool_name = str(route.get("tool_name") or "").lower()
        side_effect = str(route.get("side_effect") or "read_only")
        if side_effect != "read_only":
            return True
        collection_terms = ("list", "search", "overview", "summary", "health", "catalog", "available")
        if any(term in tool_name for term in collection_terms):
            return False
        object_terms = ("get", "detail", "validate", "bind", "plan", "promote", "retire", "deprecate")
        return any(term in tool_name for term in object_terms)

    @staticmethod
    def _render_mcp_safe_preflight_reply(route_or_execution: dict[str, Any], preflight: dict[str, Any]) -> str:
        route = str(route_or_execution.get("route") or "")
        if not route and route_or_execution.get("server_key") and route_or_execution.get("tool_name"):
            route = f"{route_or_execution.get('server_key')}/{route_or_execution.get('tool_name')}"
        domain = str(route_or_execution.get("domain") or "")
        side_effect = str(route_or_execution.get("side_effect") or route_or_execution.get("status") or "plan_or_preflight")
        label = ResearchAssistantService._business_label_for_domain(domain, route)
        lines = [
            f"{label}需要先做方案/预检；本轮未执行写入、训练、回补或晋升。",
        ]
        if side_effect in {"approval_required", "preflight_required", "preflight_failed", "confirmed_action"}:
            lines.append("安全边界：需先展示预检结果和确认口令，获得明确授权后才能进入下一步。")
        else:
            lines.append("安全边界：只会生成方案或预检说明，不会直接提交高风险操作。")
        failed_checks = preflight.get("failed_checks") if isinstance(preflight.get("failed_checks"), list) else []
        if failed_checks:
            rendered = "; ".join(
                str(item.get("detail") or item.get("check") or item)
                for item in failed_checks[:3]
                if isinstance(item, dict)
            )
            lines.append(f"预检阻断：{rendered}。")
        missing = preflight.get("missing_confirmations") if isinstance(preflight.get("missing_confirmations"), list) else []
        if missing:
            lines.append("需要确认：" + "，".join(str(item) for item in missing[:3]) + "。")
        approval_id = str(route_or_execution.get("approval_id") or preflight.get("approval_id") or "")
        required_confirmation = str(route_or_execution.get("required_confirmation_text") or preflight.get("required_confirmation_text") or "")
        if approval_id and required_confirmation:
            lines.append(f"审批 ID：{approval_id}")
            lines.append(f"确认口令：{required_confirmation}")
            lines.append("如需在对话内执行，请回填该 approval_id，并让 confirmation_text 与确认口令完全一致。")
        lines.append("下一步：请先补充必要 ID/参数，或明确说“先给预检”、“我确认执行”等边界。")
        return "\n".join(lines)

    @staticmethod
    def _render_skill_reuse_preflight_reply(skill_reuse: dict[str, Any]) -> str:
        skill_key = str(skill_reuse.get("skill_key") or "selected_skill")
        lines = [
            f"已将技能 {skill_key} 转成待审批的复用提案；本轮没有直接执行技能。",
            "安全边界：LLM 只能选择技能并生成 Action Proposal，执行必须由用户确认。",
        ]
        approval_id = str(skill_reuse.get("approval_id") or "")
        required_confirmation = str(skill_reuse.get("required_confirmation_text") or "")
        if approval_id and required_confirmation:
            lines.append(f"审批 ID：{approval_id}")
            lines.append(f"确认口令：{required_confirmation}")
            lines.append("如需在对话内继续，请回填该 approval_id，并让 confirmation_text 与确认口令完全一致。")
        else:
            reason_codes = skill_reuse.get("reason_codes") if isinstance(skill_reuse.get("reason_codes"), list) else []
            if reason_codes:
                lines.append("阻断原因：" + "；".join(str(item) for item in reason_codes[:3]))
        return "\n".join(lines)

    @staticmethod
    def _render_qe_draft_safe_reply(cards: dict[str, Any]) -> str:
        plan = cards.get("plan_card") if isinstance(cards.get("plan_card"), dict) else {}
        steps = [str(item) for item in plan.get("steps", []) if str(item)]
        clarification = cards.get("clarification_card") if isinstance(cards.get("clarification_card"), dict) else {}
        questions = [str(item) for item in clarification.get("questions", []) if str(item)]
        lines = [
            "已收到 QE 实验草案需求；本轮只做草案，不执行、物化或启动训练。",
            "本轮只生成方案，不会执行、物化或启动训练。",
            "草案框架：",
        ]
        if steps:
            for step in steps[:5]:
                lines.append(f"- {step}")
        else:
            lines.extend(
                [
                    "- 明确实验目标、股票池、时间窗、频率和标签口径。",
                    "- 固定 seed、成本假设、数据版本和训练/回测边界。",
                    "- 先形成 template draft 与校验项，确认后再进入 preflight。",
                ]
            )
        if questions:
            lines.append("需要你补充的关键参数：" + "；".join(questions[:3]) + "。")
        lines.append("如果你确认目标和约束，我再把它整理成可校验的 QE template 草案。")
        return "\n".join(lines)

    @staticmethod
    def _render_react_execution_fallback_reply(execution: dict[str, Any], summary: dict[str, Any]) -> str:
        return ResearchAssistantService._render_mcp_execution_reply(execution, summary)

    @staticmethod
    def _strip_assistant_tool_choice_markup(text: str) -> str:
        cleaned = re.sub(r"<assistant_tool_choice\b[^>]*>.*?</assistant_tool_choice>", "", text, flags=re.IGNORECASE | re.DOTALL)
        return cleaned.strip()

    @staticmethod
    def _should_render_mcp_route_reply(route: Any, mode_decision: ModeDecision, raw_text: str) -> bool:
        if not isinstance(route, dict) or not route.get("server_key") or not route.get("tool_name"):
            return False
        if "<assistant_tool_choice" in raw_text.lower():
            return True
        guarded_intents = {
            DialogueIntent.VALIDATION_ISSUE_REQUEST,
            DialogueIntent.FACTOR_LIBRARY_REQUEST,
            DialogueIntent.FACTOR_METRICS_REQUEST,
            DialogueIntent.FACTOR_CORRELATION_REQUEST,
            DialogueIntent.MODEL_REGISTRY_REQUEST,
            DialogueIntent.STRATEGY_GOVERNANCE_REQUEST,
            DialogueIntent.EXECUTION_POLICY_REQUEST,
            DialogueIntent.EXTERNAL_RESEARCH_REQUEST,
            DialogueIntent.STOCK_ANALYSIS_REQUEST,
            DialogueIntent.QE_WAREHOUSE_REQUEST,
            DialogueIntent.RESEARCH_PIPELINE_REQUEST,
        }
        return mode_decision.intent_type in guarded_intents

    @staticmethod
    def _render_mcp_route_reply(route: Any) -> str:
        if not isinstance(route, dict):
            return "\u6211\u8fd8\u9700\u8981\u5148\u786e\u8ba4\u4e1a\u52a1\u57df\u548c\u76ee\u6807\uff0c\u4e0d\u4f1a\u7528\u6a21\u578b\u731c\u6d4b\u4e1a\u52a1\u6570\u636e\u3002"
        server_key = str(route.get("server_key") or "")
        tool_name = str(route.get("tool_name") or "")
        domain = str(route.get("domain") or "")
        side_effect = str(route.get("side_effect") or "read_only")
        label = ResearchAssistantService._business_label_for_domain(domain, f"{server_key}/{tool_name}")
        lines = [f"\u5df2\u8bc6\u522b\u4e3a{label}\u9700\u6c42\uff0c\u6211\u4f1a\u6309\u8be5\u4e1a\u52a1\u57df\u5904\u7406\u3002"]
        if side_effect == "read_only":
            lines.append("\u8fd9\u662f\u53ea\u8bfb\u67e5\u8be2\uff0c\u4e0b\u4e00\u6b65\u5e94\u76f4\u63a5\u7ed9\u51fa\u4e1a\u52a1\u5217\u8868\u3001\u72b6\u6001\u6c47\u603b\u6216\u5173\u952e\u6307\u6807\u3002")
        elif side_effect == "plan_or_preflight":
            lines.append("\u8fd9\u662f\u65b9\u6848/\u9884\u68c0\u7c7b\u9700\u6c42\uff0c\u53ea\u751f\u6210\u65b9\u6848\u548c\u5b89\u5168\u6821\u9a8c\uff0c\u4e0d\u4f1a\u6267\u884c\u5199\u5165\u6216\u957f\u4efb\u52a1\u3002")
        else:
            lines.append("\u8fd9\u662f\u9700\u786e\u8ba4\u7684\u64cd\u4f5c\uff0c\u5fc5\u987b\u5148\u5c55\u793a\u9884\u68c0\u3001\u786e\u8ba4\u53e3\u4ee4\u548c\u5ba1\u6279\u8fb9\u754c\uff0c\u786e\u8ba4\u524d\u4e0d\u4f1a\u6267\u884c\u3002")
        if ResearchAssistantService._route_needs_concrete_target_hint(route):
            lines.append("\u5982\u679c\u672a\u6307\u5b9a\u5bf9\u8c61 ID \u6216\u5fc5\u8981\u53c2\u6570\uff0c\u6211\u5e94\u5148\u5217\u51fa\u5019\u9009\u5bf9\u8c61\u6216\u8bf7\u4f60\u8865\u5145\u6761\u4ef6\uff0c\u4e0d\u5bf9\u4e0d\u660e\u786e\u5bf9\u8c61\u505a\u8be6\u60c5\u5224\u65ad\u3001\u53d8\u66f4\u6216\u664b\u5347\u3002")
        return "\n".join(lines)

    @staticmethod
    def _render_mcp_tool_catalog_reply(catalog: dict[str, Any]) -> str:
        lines = [
            "\u53ef\u4ee5\uff0c\u6211\u4f1a\u6309 AIstock \u4e1a\u52a1\u76ee\u6807\u9009\u62e9\u5bf9\u5e94\u4e1a\u52a1\u80fd\u529b\u3002",
            f"\u5f53\u524d\u53ef\u7528\u76ee\u5f55\uff1a{catalog.get('server_count', 0)} \u4e2a\u4e1a\u52a1\u57df\uff0c{catalog.get('tool_count', 0)} \u4e2a\u53ef\u7528\u80fd\u529b\uff0c{catalog.get('capability_count', 0)} \u4e2a\u5df2\u6279\u51c6\u80fd\u529b\u3002",
            "\u4f60\u53ef\u4ee5\u76f4\u63a5\u7528\u81ea\u7136\u8bed\u8a00\u63d0\u9700\u6c42\uff0c\u6211\u5e94\u8be5\u8fd4\u56de\u4e1a\u52a1\u7ed3\u679c\uff0c\u800c\u4e0d\u662f\u8fd0\u884c\u8fc7\u7a0b\u8bf4\u660e\u3002",
        ]
        tools_by_server = catalog.get("tools_by_server") if isinstance(catalog.get("tools_by_server"), dict) else {}
        servers_by_key = catalog.get("servers_by_key") if isinstance(catalog.get("servers_by_key"), dict) else {}
        if tools_by_server:
            lines.append("\u4e1a\u52a1\u5206\u7c7b\u6982\u89c8\uff1a")
            for server_key in sorted(str(key) for key in tools_by_server):
                tools = tools_by_server.get(server_key) or []
                server = servers_by_key.get(server_key) if isinstance(servers_by_key.get(server_key), dict) else {}
                health = server.get("health_json") if isinstance(server.get("health_json"), dict) else {}
                display_name = str(health.get("display_name_zh") or server.get("title") or server_key)
                aliases = health.get("business_aliases_zh") if isinstance(health.get("business_aliases_zh"), list) else []
                alias_text = "\uff08" + "\uff0c".join(str(item) for item in aliases[:3]) + "\uff09" if aliases else ""
                sample = []
                for tool in tools:
                    if not isinstance(tool, dict):
                        continue
                    raw_tool_name = str(tool.get("tool_name") or "")
                    if "artifact_ref" in raw_tool_name.lower():
                        continue
                    display = str(tool.get("title") or "").strip() or ResearchAssistantService._humanize_business_identifier(raw_tool_name)
                    if ResearchAssistantService._contains_mcp_business_forbidden_marker(display):
                        display = ResearchAssistantService._humanize_business_identifier(raw_tool_name)
                    sample.append(display)
                preview_names = []
                for name in sample:
                    if name and name not in preview_names:
                        preview_names.append(name)
                    if len(preview_names) >= 8:
                        break
                preview = "\uff0c".join(preview_names)
                suffix = f"\uff0c\u5171 {len(sample)} \u4e2a" if len(sample) > 8 else f"\uff0c\u5171 {len(sample)} \u4e2a"
                lines.append(f"- {display_name}{alias_text}\uff1a{preview}{suffix}")
        else:
            lines.append("\u6682\u672a\u8bfb\u5230\u53ef\u7528\u5de5\u5177\u76ee\u5f55\uff0c\u9700\u5148\u5237\u65b0\u80fd\u529b\u76ee\u5f55\u3002")
        lines.append("\u793a\u4f8b\uff1a\u68c0\u67e5\u672c\u5730\u6570\u636e\u540c\u6b65\u3001\u67e5\u770b QE \u6570\u4ed3\u5065\u5eb7\u3001\u641c\u7d22\u56e0\u5b50\u5e93\u3001\u6bd4\u8f83\u6a21\u578b\u8868\u73b0\u3001\u5224\u65ad\u7b56\u7565\u5305\u662f\u5426\u53ef\u8fdb\u5165 Paper v2\u3002")
        return "\n".join(lines)




    def _apply_main_reply_policy(self, text: str, mode_decision: ModeDecision) -> str:
        if mode_decision.mode not in {DialogueMode.DIALOGUE, DialogueMode.ANALYSIS}:
            return text
        mode_cfg = self._dialogue_mode_config(mode_decision.mode.value)
        forbidden = [str(item) for item in mode_cfg.get("forbidden_main_reply_phrases", []) if str(item)]
        kept_lines: list[str] = []
        for line in text.splitlines():
            stripped = line.strip()
            if any(stripped.startswith(phrase) or phrase in stripped for phrase in forbidden):
                continue
            kept_lines.append(line)
        cleaned = "\n".join(kept_lines).strip()
        cleaned = self._clean_user_visible_memory_slot_jargon(cleaned or text)
        if cleaned and self._contains_user_visible_memory_slot_jargon(cleaned):
            logger.error(
                "research assistant main reply still contains memory slot jargon after cleanup: mode=%s intent=%s",
                mode_decision.mode.value,
                mode_decision.intent_type.value,
            )
            return "可以。你把要记住的具体内容告诉我，我会按记忆候选处理；需要确认的内容会先让你审批，不会直接写入长期记忆。"
        return cleaned or text

    @staticmethod
    def _contains_user_visible_memory_slot_jargon(text: str) -> bool:
        lowered = text.lower()
        return "subject_key" in lowered or "memory_type" in lowered

    @classmethod
    def _clean_user_visible_memory_slot_jargon(cls, text: str) -> str:
        if not cls._contains_user_visible_memory_slot_jargon(text):
            return text
        cleaned_lines: list[str] = []
        for line in text.splitlines():
            cleaned = re.sub(r"`?\bsubject_key\b`?", "记忆对象", line, flags=re.IGNORECASE)
            cleaned = re.sub(r"`?\bmemory_type\b`?", "记忆类别", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"`?\btitle\b`?", "标题", cleaned, flags=re.IGNORECASE)
            cleaned_lines.append(cleaned)
        return "\n".join(cleaned_lines).strip()

    def add_task_event(self, task_id: str, request: TaskEventCreate | dict[str, Any]) -> dict[str, Any]:
        task = self.repository.get_record("tasks", task_id)
        if not task:
            raise KeyError(f"task not found: {task_id}")
        data = request if isinstance(request, TaskEventCreate) else TaskEventCreate(**request)
        event = self.repository.create_record("task_events", {"event_id": new_id("ratev"), "task_id": task_id, **data.model_dump()})
        status_updates = {
            "mcp_started": "running",
            "mcp_done": "completed",
            "mcp_failed": "triage_required",
            "skill_started": "running",
            "skill_done": "completed",
            "skill_failed": "triage_required",
            "approval_required": "approval_required",
            "approved": "approved",
            "rejected": "blocked",
            "triage_required": "triage_required",
        }
        if data.event_type in status_updates:
            updates: dict[str, Any] = {"status": status_updates[data.event_type]}
            if data.event_type in {"mcp_done", "skill_done"}:
                updates["completed_at"] = utc_now().isoformat()
            if data.event_type in {"mcp_failed", "skill_failed", "triage_required"}:
                updates["triage_json"] = {"last_event_id": event["event_id"], "message": data.message, "payload": data.payload_json}
            task = self.repository.update_record("tasks", task_id, updates)
        trigger = reflection_trigger_from_event(data.event_type, message=data.message, payload_json=data.payload_json)
        if trigger:
            self.generate_reflection_card(task_id=task_id, trigger=trigger, source_event=event, task_snapshot=task)
        return event

    def generate_reflection_card(
        self,
        *,
        task_id: str,
        trigger: str,
        source_event: dict[str, Any] | None = None,
        task_snapshot: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        task = task_snapshot or self.repository.get_record("tasks", task_id)
        if not task:
            raise KeyError(f"task not found: {task_id}")
        card_id = new_id("refcard")
        memory_id = new_id("mem")
        artifacts = build_reflection_artifacts(
            task=task,
            trigger=trigger,
            source_event=source_event,
            card_id=card_id,
            memory_id=memory_id,
            created_at=utc_now().isoformat(),
        )
        memory = MemoryCurator(self.repository).create_reflection_memory(artifacts["memory"])
        card = self.repository.create_record("reflection_cards", {**artifacts["card"], "memory_ref": memory["memory_id"]})
        return card

    def create_memory(self, request: MemoryCreate | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, MemoryCreate) else MemoryCreate(**request)
        payload = data.model_dump(exclude={"approval_id", "confirmation_text"})
        if data.risk_level in {"high", "production_sensitive"} or data.approval_status == "approved":
            self._consume_approval_gate(
                approval_id=data.approval_id,
                confirmation_text=data.confirmation_text,
                approval_type="memory.write",
                required_summary_fragment=data.subject_key,
            )
        payload["memory_id"] = new_id("mem")
        payload["checksum"] = sha256_json({"content_json": payload["content_json"], "content_text": payload["content_text"], "subject_key": payload["subject_key"]})
        return self.repository.create_record("memory_items", payload)

    def update_memory_status(
        self,
        memory_id: str,
        status: str,
        *,
        approved_by: str | None = None,
        approval_id: str | None = None,
        confirmation_text: str | None = None,
    ) -> dict[str, Any]:
        if status not in {"draft", "approved", "rejected", "expired", "superseded"}:
            raise ValueError("invalid memory approval status")
        memory = self.repository.get_record("memory_items", memory_id)
        if not memory:
            raise KeyError(f"memory not found: {memory_id}")
        if status == "approved":
            if not memory.get("source_ref") and not memory.get("evidence_refs"):
                raise ValueError("approved memory requires source_ref or evidence_refs")
            self._consume_approval_gate(
                approval_id=approval_id,
                confirmation_text=confirmation_text,
                approval_type="memory.approve",
                required_summary_fragment=str(memory.get("subject_key") or ""),
            )
        return self.repository.update_record("memory_items", memory_id, {"approval_status": status, "approved_by": approved_by})

    def build_context_pack(self, request: ContextPackBuildRequest | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, ContextPackBuildRequest) else ContextPackBuildRequest(**request)
        temp_refs = []
        if data.task_id:
            temp_page = self.repository.list_records("temp_memories", filters={"task_id": data.task_id}, limit=self.configured_limit("temp_memories_context_pack"))
            temp_refs = [item["temp_memory_id"] for item in temp_page["items"]]
        max_context_budget = self.configured_limit("context_pack_max_token_budget")
        token_budget = data.token_budget or self.configured_limit("default_context_pack_token_budget")
        if token_budget > max_context_budget:
            raise ValueError(f"token_budget exceeds configured context_pack_max_token_budget: {max_context_budget}")
        user_message = data.user_message or self._context_pack_user_message(data.task_id)
        runtime_config = dict(self.active_runtime_config())
        memory_tree_config = dict(runtime_config.get("memory_tree") or {})
        memory_tree_config.update(
            {
                "namespace": data.namespace,
                "token_budget": token_budget,
                "candidate_limit": max(
                    int(memory_tree_config.get("candidate_limit") or 0),
                    self.configured_limit("memory_items_context_pack") * 4,
                ),
                "max_items": int(memory_tree_config.get("max_items") or self.configured_limit("memory_items_context_pack")),
            }
        )
        runtime_config["memory_tree"] = memory_tree_config
        memory_result = select_memory_branches(
            user_message,
            data.dialogue_intent,
            repo=self.repository,
            runtime_config=runtime_config,
        )
        refs_by_type = memory_result.refs_by_type
        memory_items = memory_result.memory_items
        graph_entity_keys = self._graph_entity_keys_for_memory_items(memory_items, user_message=user_message)
        graph_context_config = dict(runtime_config.get("graph_context") or {})
        graph_result = expand_neighbors(
            graph_entity_keys,
            repo=self.repository,
            namespace=data.namespace,
            hops=int(graph_context_config.get("hops") or 2),
            relation_filter=graph_context_config.get("relation_filter"),
            limit=int(graph_context_config.get("limit") or self.configured_limit("graph_summary_relations")),
        )
        code_intelligence_context = build_code_intelligence_context(repo_root=REPO_ROOT)
        code_intelligence_refs = artifact_ref_paths(code_intelligence_context)
        query_code_context = build_query_code_context(
            user_query=user_message,
            task_id=data.task_id,
            repo_root=REPO_ROOT,
            token_budget=token_budget,
            cache_lookup=self._lookup_code_context_cache,
        )
        code_context_refs = list(query_code_context.get("code_context_refs") or [])
        if code_context_refs:
            self._persist_code_context_refs(task_id=data.task_id, refs=code_context_refs)
        code_context_artifacts = code_context_artifact_paths(query_code_context)
        core_refs = [
            *refs_by_type.get("core", []),
            *refs_by_type.get("directive", []),
            *refs_by_type.get("user_preference", []),
            *refs_by_type.get("habit", []),
            *refs_by_type.get("analysis_note", []),
        ]
        pack_json = {
            "mandatory_rules": [
                "Memory Ledger remains the source of truth; retrieval only selects approved memory.",
                "Formal GitHub issue creation still requires explicit approval and sync.",
                "High-risk MCP or skill execution requires preflight and approval.",
            ],
            "memory_items": memory_items,
            "memory_route": {
                "route_reason": memory_result.route_reason,
                "matched_branches": memory_result.matched_branches,
                "omitted_refs": memory_result.omitted_refs,
            },
            "graph_context": {
                "route_reason": graph_result.route_reason,
                "source": GRAPH_CONTEXT_SOURCE if graph_result.relation_refs else None,
                "source_refs": [GRAPH_CONTEXT_SOURCE] if graph_result.relation_refs else [],
                "as_of": GRAPH_CONTEXT_AS_OF if graph_result.relation_refs else None,
                "relation_refs": graph_result.relation_refs,
                "seed_entity_keys": graph_result.seed_entity_keys,
                "neighbor_entity_keys": graph_result.neighbor_entity_keys,
                "omitted_relation_refs": graph_result.omitted_relation_refs,
            },
            "code_intelligence_context": code_intelligence_context,
            "code_context_route": {
                "status": query_code_context.get("status"),
                "reason_codes": query_code_context.get("reason_codes") or [],
                "warnings": query_code_context.get("warnings") or [],
                "scope": query_code_context.get("scope") or {},
                "adapter_contract": query_code_context.get("adapter_contract") or {},
                "as_of": query_code_context.get("as_of"),
            },
            "code_context_refs": code_context_refs,
            "task_id": data.task_id,
            "agent_id": data.agent_id,
            "token_budget": token_budget,
        }
        context_pack_id = new_id("ctx")
        row = {
            "context_pack_id": context_pack_id,
            "task_id": data.task_id,
            "agent_id": data.agent_id,
            "model_profile": data.model_profile,
            "token_budget": token_budget,
            "core_memory_refs": core_refs,
            "procedural_memory_refs": refs_by_type.get("procedural", []),
            "architecture_memory_refs": refs_by_type.get("architecture", []),
            "task_state_refs": refs_by_type.get("task_state", []),
            "experiment_memory_refs": refs_by_type.get("experiment", []),
            "graph_relation_refs": graph_result.graph_relation_refs,
            "external_source_refs": [*code_intelligence_refs, *[ref for ref in code_context_artifacts if ref not in code_intelligence_refs]],
            "temp_memory_refs": temp_refs,
            "omitted_relevant_refs": memory_result.omitted_refs,
            "pack_summary": (
                f"Context Pack: {len(memory_items)} tree-selected memories, "
                f"{len(graph_result.graph_relation_refs)} graph relations, {len(temp_refs)} temp memories, "
                f"code-intelligence {code_intelligence_context.get('data_state') or 'unknown'}, "
                f"{len(code_context_refs)} code context refs"
            ),
            "pack_json": pack_json,
            "checksum": sha256_json(pack_json),
        }
        context_pack = self.repository.create_record("context_packs", row)
        for item in memory_items:
            self.repository.create_record(
                "memory_access_log",
                {
                    "access_id": new_id("memacc"),
                    "memory_id": item["memory_id"],
                    "task_id": data.task_id,
                    "agent_id": data.agent_id,
                    "retrieval_reason": {
                        "context_pack_id": context_pack_id,
                        "memory_type": item.get("memory_type"),
                        "tree_path": item.get("tree_path"),
                        "matched_branches": memory_result.matched_branches,
                        "source": "context_pack_build",
                    },
                    "used_in_prompt": True,
                    "payload_json": {
                        "token_budget": token_budget,
                        "model_profile": data.model_profile,
                    },
                },
            )
            self._mark_memory_used(item)
        if data.task_id:
            self.add_task_event(
                data.task_id,
                TaskEventCreate(
                    event_type="context_pack_built",
                    message="Context Pack built",
                    payload_json={
                        "context_pack_id": row["context_pack_id"],
                        "code_context_ref_count": len(code_context_refs),
                        "code_context_reason_codes": query_code_context.get("reason_codes") or [],
                    },
                ),
            )
        return context_pack

    def _lookup_code_context_cache(self, payload: dict[str, Any]) -> dict[str, Any]:
        task_id = payload.get("task_id")
        expected_ref_ids = [str(ref_id) for ref_id in payload.get("expected_ref_ids") or []]
        if not expected_ref_ids:
            return {
                "status": "miss",
                "code_context_refs": [],
                "reason_codes": ["code_context_cache_miss"],
                "warnings": ["code context cache miss: no deterministic ref ids for query scope"],
            }
        try:
            rows = []
            for code_ref_id in expected_ref_ids:
                row = self.repository.get_record("code_context_refs", code_ref_id)
                if not row or (task_id and row.get("task_id") != task_id):
                    return {
                        "status": "miss",
                        "code_context_refs": [],
                        "reason_codes": ["code_context_cache_miss"],
                        "warnings": [f"code context cache miss: {code_ref_id}"],
                    }
                rows.append(row)
        except Exception as exc:  # noqa: BLE001 - explicit degraded cache route, adapter may still run.
            return {
                "status": "unavailable",
                "code_context_refs": [],
                "reason_codes": ["code_context_cache_unavailable"],
                "warnings": [f"code context cache unavailable: {type(exc).__name__}: {exc}"],
            }

        refs = [_code_context_ref_from_row(row) for row in rows]
        return {
            "status": "hit",
            "code_context_refs": refs,
            "reason_codes": ["code_context_cache_hit"],
            "warnings": [],
            "as_of": refs[0].get("as_of") if refs else None,
        }

    def _persist_code_context_refs(self, *, task_id: str | None, refs: list[dict[str, Any]]) -> None:
        for ref in refs:
            if not isinstance(ref, dict) or not ref.get("code_ref_id"):
                continue
            self.repository.create_record(
                "code_context_refs",
                {
                    "code_ref_id": ref["code_ref_id"],
                    "task_id": task_id,
                    "query_scope": ref.get("query_scope") or "",
                    "manifest_json": ref.get("manifest_json") or {},
                    "source": ref.get("source") or "codegraph",
                    "provenance_json": ref.get("provenance") or {},
                    "as_of": ref.get("as_of"),
                },
            )

    def _context_pack_user_message(self, task_id: str | None) -> str | None:
        if not task_id:
            return None
        task = self.repository.get_record("tasks", task_id)
        if not task:
            return None
        input_json = task.get("input_json") or {}
        if isinstance(input_json, dict):
            value = input_json.get("user_message") or input_json.get("message")
            if value:
                return str(value)
        return None

    def _graph_entity_keys_for_memory_items(self, memory_items: list[dict[str, Any]], *, user_message: str | None = None) -> list[str]:
        keys: set[str] = set()
        query_terms = self._graph_query_terms(user_message)
        for item in memory_items:
            tree_path = str(item.get("tree_path") or item.get("subject_key") or "")
            if tree_path.startswith("personal."):
                continue
            item_keys: set[str] = set()
            item_keys.update(self._graph_entity_keys_from_path(tree_path))
            content_json = item.get("content_json") or {}
            if isinstance(content_json, dict):
                item_keys.update(self._graph_entity_keys_from_content(content_json))
            keys.update(key for key in item_keys if self._graph_entity_key_matches_query(key, query_terms))
        keys.update(self._graph_entity_keys_from_message(user_message))
        return sorted(keys)

    def _graph_entity_keys_from_message(self, user_message: str | None) -> list[str]:
        query_terms = self._graph_message_module_terms(user_message)
        if not query_terms:
            return []
        normalized_message = str(user_message or "").lower()
        limit = self.configured_limit("graph_summary_entities")
        page = self.repository.list_records(
            "entities",
            filters={"namespace": "aistock", "entity_type": "module"},
            limit=limit,
        )
        matches: list[tuple[int, str]] = []
        for entity in page.get("items") or []:
            entity_key = str(entity.get("entity_key") or "").strip()
            if not re.fullmatch(r"module\.[a-z0-9]+(?:_[a-z0-9]+)*", entity_key):
                continue
            entity_terms = self._graph_module_entity_terms(entity)
            cjk_title_match = self._graph_module_title_matches_cjk_substring(entity, normalized_message)
            if query_terms.isdisjoint(entity_terms) and not cjk_title_match:
                continue
            suffix = entity_key.removeprefix("module.")
            score = 2 if cjk_title_match or suffix in query_terms else 1
            matches.append((score, entity_key))
        matches.sort(key=lambda item: (-item[0], item[1]))
        return [entity_key for _, entity_key in matches[:limit]]

    @classmethod
    def _graph_module_entity_terms(cls, entity: dict[str, Any]) -> set[str]:
        entity_key = str(entity.get("entity_key") or "")
        terms: set[str] = set()
        if entity_key.startswith("module."):
            suffix = entity_key.removeprefix("module.")
            terms.add(suffix)
            terms.update(part for part in suffix.split("_") if part)
        terms.update(cls._graph_message_module_terms(str(entity.get("title") or "")))
        return terms

    @staticmethod
    def _graph_module_title_matches_cjk_substring(entity: dict[str, Any], normalized_message: str) -> bool:
        if not normalized_message:
            return False
        title = str(entity.get("title") or "").lower()
        cjk_segments = re.findall(r"[\u4e00-\u9fff]+", title)
        return any(len(segment) >= 3 and segment in normalized_message for segment in cjk_segments)

    @staticmethod
    def _graph_message_module_terms(value: str | None) -> set[str]:
        if not value:
            return set()
        generic_terms = {"module", "capability", "mcp", "api", "process", "dataset", "strategy", "model"}
        tokens = re.findall(r"[a-z0-9]+|[\u4e00-\u9fff]+", str(value).lower())
        return {token for token in tokens if len(token) >= 2 and token not in generic_terms}

    @staticmethod
    def _graph_entity_keys_from_path(path: str) -> set[str]:
        keys: set[str] = set()
        direct_prefixes = ("module.", "capability.", "mcp.", "api.", "process.", "dataset.", "strategy.", "model.")
        if path.startswith(direct_prefixes):
            keys.add(path)
        parts = [part for part in path.split(".") if part]
        if len(parts) >= 3 and parts[0] == "project" and parts[1] in {
            "module",
            "capability",
            "mcp",
            "api",
            "process",
            "dataset",
            "strategy",
            "model",
        }:
            keys.add(".".join(parts[1:]))
        return keys

    @staticmethod
    def _graph_entity_keys_from_content(content_json: dict[str, Any]) -> set[str]:
        keys: set[str] = set()
        raw_entity_keys = content_json.get("entity_keys") or []
        if isinstance(raw_entity_keys, str):
            raw_entity_keys = [raw_entity_keys]
        for value in raw_entity_keys:
            if value:
                keys.add(str(value))
        if content_json.get("entity_key"):
            keys.add(str(content_json["entity_key"]))
        if content_json.get("capability_key"):
            keys.add(f"capability.{content_json['capability_key']}")
        if content_json.get("mcp_server"):
            server_key = str(content_json["mcp_server"]).lower().replace("aistock-", "").replace("-", "_")
            keys.add(f"mcp.{server_key}")
        return keys

    @staticmethod
    def _graph_query_terms(user_message: str | None) -> set[str]:
        if not user_message:
            return set()
        normalized = "".join(ch.lower() if ch.isalnum() else " " for ch in str(user_message))
        generic_terms = {"module", "capability", "mcp", "api", "process", "dataset", "strategy", "model"}
        return {term for term in normalized.split() if len(term) >= 3 and term not in generic_terms}

    @staticmethod
    def _graph_entity_key_matches_query(entity_key: str, query_terms: set[str]) -> bool:
        if not query_terms:
            return True
        generic_terms = {"module", "capability", "mcp", "api", "process", "dataset", "strategy", "model"}
        key_terms = [
            term
            for term in re.split(r"[^a-zA-Z0-9]+", entity_key.lower())
            if len(term) >= 3 and term not in generic_terms
        ]
        return bool(key_terms and any(term in query_terms for term in key_terms))

    def _mark_memory_used(self, item: dict[str, Any]) -> None:
        memory_id = item.get("memory_id")
        if not memory_id:
            return
        self.repository.update_record(
            "memory_items",
            str(memory_id),
            {
                "last_used_at": utc_now().isoformat(),
                "use_count": int(item.get("use_count") or 0) + 1,
            },
        )

    def _schedule_memory_curator(
        self,
        *,
        user_message: str,
        assistant_message: str,
        conversation_id: str,
        user_message_id: str,
        assistant_message_id: str,
        task_id: str,
        model_profile: dict[str, Any] | None = None,
    ) -> None:
        def run_curator() -> CuratorResult:
            try:
                result = MemoryCurator(
                    self.repository,
                    semantic_extractor=self._semantic_memory_candidate_extractor(model_profile=model_profile),
                ).curate_turn(
                    user_message=user_message,
                    assistant_message=assistant_message,
                    conversation_id=conversation_id,
                    user_message_id=user_message_id,
                    assistant_message_id=assistant_message_id,
                    task_id=task_id,
                )
            except Exception as exc:  # noqa: BLE001 - record concrete curator failures; do not silently skip memory.
                payload = {
                    "reason_code": "memory_curator_failed",
                    "exception_type": type(exc).__name__,
                    "error_summary": str(exc)[:500],
                    "conversation_id": conversation_id,
                    "user_message_id": user_message_id,
                    "assistant_message_id": assistant_message_id,
                }
                logger.exception(
                    "research assistant memory curator failed: reason_code=%s conversation_id=%s user_message_id=%s exception_type=%s error=%s",
                    payload["reason_code"],
                    conversation_id,
                    user_message_id,
                    type(exc).__name__,
                    exc,
                )
                self.add_task_event(
                    task_id,
                    TaskEventCreate(
                        event_type="memory_curator_failed",
                        severity="error",
                        message=f"记忆候选提炼失败：{type(exc).__name__}: {exc}",
                        payload_json=payload,
                    ),
                )
                if self.repository.health().get("mode") == "in_memory_test_only":
                    raise
                return CuratorResult(skipped=["memory_curator_failed"])
            changed = result.created_branch_ids or result.created_memory_ids or result.updated_memory_ids
            if changed or result.approval_required_ids:
                self.add_task_event(
                    task_id,
                    TaskEventCreate(
                        event_type="memory_written",
                        message="Memory curator processed chat turn",
                        payload_json={
                            "created_branch_ids": result.created_branch_ids,
                            "created_memory_ids": result.created_memory_ids,
                            "updated_memory_ids": result.updated_memory_ids,
                            "approval_required_ids": result.approval_required_ids,
                            "skipped": result.skipped,
                            "async_worker": self.repository.health().get("mode") != "in_memory_test_only",
                        },
                    ),
                )
            return result

        if self.repository.health().get("mode") == "in_memory_test_only":
            run_curator()
            return
        threading.Thread(target=run_curator, name="research-assistant-memory-curator", daemon=True).start()

    def _semantic_memory_candidate_extractor(self, *, model_profile: dict[str, Any] | None) -> Any | None:
        complete_memory_curation = getattr(self.llm_client, "complete_memory_curation", None)
        if not callable(complete_memory_curation):
            logger.warning(
                "research assistant semantic memory curator unavailable: reason_code=memory_curator_llm_hook_missing; "
                "falling back to seed-only memory extraction for this injected LLM client"
            )
            return None
        if not isinstance(model_profile, dict) or not model_profile:
            raise RuntimeError("reason_code=memory_curator_model_profile_missing; semantic memory curator requires a concrete model_profile")

        def extract(*, user_message: str, assistant_message: str) -> list[dict[str, Any]]:
            payload = {
                "user_message": user_message,
                "assistant_message": assistant_message,
                "rules": {
                    "capability_inquiry_is_not_execution": True,
                    "no_internal_slot_names_in_user_reply": ["subject_key", "memory_type", "title"],
                    "approval_scope_policy_unchanged": True,
                },
            }
            result = complete_memory_curation(
                messages=[
                    {"role": "system", "content": MEMORY_CURATOR_SEMANTIC_SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False, sort_keys=True)},
                ],
                model_profile=model_profile,
                temperature=0.0,
                max_tokens=900,
            )
            parsed = _parse_json_object(str(result.content or ""))
            if parsed is None:
                raise ValueError(
                    "semantic memory curator returned non-json response: "
                    f"reason_code=memory_curator_invalid_json; actual_type=str; preview={str(result.content or '')[:120]!r}"
                )
            candidates = parsed.get("candidates")
            if not isinstance(candidates, list):
                raise ValueError(
                    "semantic memory curator response missing candidates list: "
                    f"reason_code=memory_curator_invalid_candidates; actual_type={type(candidates).__name__}"
                )
            return [dict(item) if isinstance(item, dict) else item for item in candidates]

        return extract



    def create_graph_entity(self, request: GraphEntityCreate | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, GraphEntityCreate) else GraphEntityCreate(**request)
        existing = self.repository.find_one(
            "entities",
            {"namespace": data.namespace, "entity_type": data.entity_type, "entity_key": data.entity_key},
        )
        row = {"entity_id": existing["entity_id"] if existing else new_id("entity"), **data.model_dump()}
        return self.repository.create_record("entities", row)

    def get_graph_entity(self, entity_id: str) -> dict[str, Any]:
        entity = self.repository.get_record("entities", entity_id)
        if not entity:
            raise KeyError(f"entity not found: {entity_id}")
        outgoing = self.repository.list_records("relations", filters={"source_entity_id": entity_id}, limit=self.configured_limit("graph_entity_relations"))["items"]
        incoming = self.repository.list_records("relations", filters={"target_entity_id": entity_id}, limit=self.configured_limit("graph_entity_relations"))["items"]
        return {"entity": entity, "outgoing_relations": outgoing, "incoming_relations": incoming}

    def create_graph_relation(self, request: GraphRelationCreate | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, GraphRelationCreate) else GraphRelationCreate(**request)
        if not self.repository.get_record("entities", data.source_entity_id):
            raise KeyError(f"source entity not found: {data.source_entity_id}")
        if not self.repository.get_record("entities", data.target_entity_id):
            raise KeyError(f"target entity not found: {data.target_entity_id}")
        if not data.evidence_refs:
            raise ValueError("graph relation requires evidence_refs")
        return self.repository.create_record("relations", {"relation_id": new_id("rel"), **data.model_dump()})

    def get_graph_relation(self, relation_id: str) -> dict[str, Any]:
        relation = self.repository.get_record("relations", relation_id)
        if not relation:
            raise KeyError(f"relation not found: {relation_id}")
        return relation

    def create_evolution_path(self, request: EvolutionPathCreate | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, EvolutionPathCreate) else EvolutionPathCreate(**request)
        if data.current_best_entity_id and not self.repository.get_record("entities", data.current_best_entity_id):
            raise KeyError(f"current best entity not found: {data.current_best_entity_id}")
        if not data.evidence_refs:
            raise ValueError("evolution path requires evidence_refs")
        return self.repository.create_record("evolution_paths", {"path_id": new_id("evopath"), **data.model_dump()})

    def get_evolution_path(self, path_id: str) -> dict[str, Any]:
        path = self.repository.get_record("evolution_paths", path_id)
        if not path:
            raise KeyError(f"evolution path not found: {path_id}")
        return path

    def graph_summary(self, *, namespace: str = "aistock") -> dict[str, Any]:
        entities = self.repository.list_records("entities", filters={"namespace": namespace}, limit=self.configured_limit("graph_summary_entities"))
        relations = self.repository.list_records("relations", limit=self.configured_limit("graph_summary_relations"))
        paths = self.repository.list_records("evolution_paths", limit=self.configured_limit("graph_summary_paths"))
        return {
            "namespace": namespace,
            "entity_count": entities["total"],
            "relation_count": relations["total"],
            "evolution_path_count": paths["total"],
            "entities": entities["items"],
            "relations": relations["items"],
            "evolution_paths": paths["items"],
        }

    def preflight_mcp_tool(self, request: McpPreflightRequest | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, McpPreflightRequest) else McpPreflightRequest(**request)
        tool, server = self._resolve_mcp_catalog_tool(data.server_key, data.tool_name)
        canonical_server_key = str(tool["server_key"])
        risk = str(tool.get("risk_level") or "medium")
        side_effect = str(tool.get("side_effect_level") or "read_only")
        requires_approval = bool(tool.get("requires_approval")) or self._side_effect_requires_approval(side_effect, risk)
        failures: list[dict[str, Any]] = []
        if tool.get("status") not in {"enabled", "ready", "approved"}:
            failures.append({"check": "tool_status", "status": "failed", "detail": tool.get("status")})
        if server.get("status") not in {"ready", "enabled", "ok"}:
            failures.append({"check": "server_status", "status": "failed", "detail": server.get("status")})
        schema = tool.get("input_schema_json") or {}
        if schema:
            try:
                jsonschema.validate(instance=data.payload_json, schema=schema)
            except jsonschema.ValidationError as exc:
                failures.append({"check": "input_schema", "status": "failed", "detail": exc.message})
        missing_confirmations = list(tool.get("required_confirmations") or []) if requires_approval else []
        passed = not requires_approval and not failures
        status = "failed" if failures else "approval_required" if requires_approval else "passed"
        result = {
            "server_key": canonical_server_key,
            "requested_server_key": data.server_key,
            "canonical_server_key": canonical_server_key,
            "tool_name": data.tool_name,
            "module": tool.get("module"),
            "profile": tool.get("profile"),
            "risk_level": risk,
            "manifest_risk_level": tool.get("manifest_risk_level"),
            "assistant_usable": tool.get("assistant_usable"),
            "backend_endpoint": tool.get("backend_endpoint"),
            "migration_state": tool.get("migration_state"),
            "side_effect_level": side_effect,
            "requires_approval": requires_approval,
            "passed": passed,
            "approval_required": requires_approval,
            "missing_confirmations": missing_confirmations,
            "preflight_checks": tool.get("preflight_schema_json", {}).get("checks", []),
            "failed_checks": failures,
            "payload_digest": sha256_json(data.payload_json),
            "idempotency_key": data.idempotency_key,
            "catalog_source": "gateway_manifest_derived_catalog",
            "evidence_refs": [f"manifest:{data.tool_name}", f"profile:{tool.get('profile')}"],
        }
        preflight_schema = tool.get("preflight_schema_json") if isinstance(tool.get("preflight_schema_json"), dict) else {}
        gateway_manifest = preflight_schema.get("gateway_manifest") if isinstance(preflight_schema.get("gateway_manifest"), dict) else {}
        if gateway_manifest:
            result.update(
                {
                    "gateway_manifest": gateway_manifest,
                    "manifest_risk_level": gateway_manifest.get("risk_level"),
                    "assistant_usable": gateway_manifest.get("assistant_usable"),
                    "recommended_profile_tags": gateway_manifest.get("profile_tags") or preflight_schema.get("recommended_profile_tags") or [],
                    "backend_endpoint": gateway_manifest.get("backend_endpoint"),
                    "response_budget": gateway_manifest.get("response_budget"),
                    "migration_state": gateway_manifest.get("migration_state"),
                }
            )
        audit_payload = {
            "catalog_source": result["catalog_source"],
            "profile": result.get("profile"),
            "module": result.get("module"),
            "server_key": canonical_server_key,
            "tool_name": data.tool_name,
            "preflight": {
                "passed": passed,
                "status": status,
                "checks": result.get("preflight_checks", []),
                "failed_checks": failures,
            },
            "approval": {
                "required": requires_approval,
                "missing_confirmations": missing_confirmations,
            },
            "evidence_refs": result["evidence_refs"],
        }
        result["audit"] = audit_payload
        result_card = {
            "title": f"MCP preflight: {canonical_server_key}/{data.tool_name}",
            "summary": "Approval pending; execution was not called." if requires_approval else "Read-only tool passed preflight.",
            "status": status,
            "profile": result.get("profile"),
            "approval_required": requires_approval,
            "evidence_refs": result["evidence_refs"],
            "next_step": "Collect confirmations and approval before execution." if requires_approval else "Eligible for read-only automatic execution.",
        }
        event = self.repository.create_record(
            "mcp_tool_events",
            {
                "tool_event_id": new_id("mcptev"),
                "task_id": data.task_id,
                "server_key": canonical_server_key,
                "tool_name": data.tool_name,
                "event_type": "preflight",
                "status": status,
                "idempotency_key": data.idempotency_key,
                "request_json": data.payload_json,
                "response_json": result,
                "result_card_json": result_card,
                "artifact_refs": result["evidence_refs"],
            },
        )
        result["tool_event_id"] = event["tool_event_id"]
        if data.task_id:
            event_type = "mcp_preflight_failed" if failures else "approval_required" if requires_approval else "mcp_preflight_passed"
            self.add_task_event(
                data.task_id,
                TaskEventCreate(
                    event_type=event_type,
                    severity="error" if failures else "warning" if requires_approval else "info",
                    message=f"MCP preflight {status}: {canonical_server_key}/{data.tool_name}",
                    payload_json={"mcp_preflight_audit": audit_payload, "preflight": result},
                ),
            )
        return result

    def create_approval(self, request: ApprovalCreate | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, ApprovalCreate) else ApprovalCreate(**request)
        approval = self.repository.create_record("approvals", {"approval_id": new_id("appr"), "status": "pending", **data.model_dump(), "approval_context_json": {}})
        if data.task_id:
            self.add_task_event(data.task_id, TaskEventCreate(event_type="approval_required", message=f"等待审批：{data.summary}", payload_json={"approval_id": approval["approval_id"]}))
        return approval

    def decide_approval(self, approval_id: str, *, action: str, confirmation_text: str, decided_by: str = "user") -> dict[str, Any]:
        approval = self.repository.get_record("approvals", approval_id)
        if not approval:
            raise KeyError(f"approval not found: {approval_id}")
        if approval.get("status") != "pending":
            raise ValueError(f"approval is not pending: {approval.get('status')}")
        if action == "approve":
            expected = approval.get("required_confirmation_text")
            if confirmation_text != expected:
                raise ValueError("confirmation_text does not match approval.required_confirmation_text")
            status = "approved"
            event_type = "approved"
        elif action == "reject":
            status = "rejected"
            event_type = "rejected"
        else:
            raise ValueError("action must be approve or reject")
        updated = self.repository.update_record(
            "approvals",
            approval_id,
            {
                "status": status,
                "approved_by": decided_by if status == "approved" else None,
                "approved_at": utc_now().isoformat() if status == "approved" else None,
                "decided_at": utc_now().isoformat(),
                "approval_text": confirmation_text,
            },
        )
        if approval.get("task_id"):
            self.add_task_event(str(approval["task_id"]), TaskEventCreate(event_type=event_type, message=f"审批已{status}", payload_json={"approval_id": approval_id}))
        return updated

    def create_issue_candidate(self, request: IssueCandidateCreate | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, IssueCandidateCreate) else IssueCandidateCreate(**request)
        candidate_id = data.dedupe_key or "retired_issue_candidate_no_storage"
        recommended_tools = ["report_bug", "mcp_github_issue_create", "mcp_github_issue_sync_bug"]
        return {
            "candidate_id": candidate_id,
            "title": data.title,
            "severity": data.severity,
            "module": data.module,
            "problem_statement": data.problem_statement,
            "reproduce_command": data.reproduce_command,
            "evidence_refs": list(data.evidence_refs),
            "status": "retired",
            "github_sync_status": "retired",
            "github_sync_json": {
                "reason": "ra_candidate_draft_storage_retired_use_standard_workflow",
                "standard_workflow_required": True,
                "storage_performed": False,
                "direct_github_create_performed": False,
                "recommended_tools": recommended_tools,
            },
            "standard_workflow_required": True,
            "storage_performed": False,
            "direct_github_create_performed": False,
            "recommended_tools": recommended_tools,
            "draft_storage_authoritative": False,
            "retired_draft_tables": ["assistant_issue_candidates", "assistant_validation_discovery_reports"],
            "assistant_draft_storage_notice": RA_DRAFT_STORAGE_NOTICE,
            "official_submission_required": RA_OFFICIAL_WORKFLOW_NOTICE,
            "github_issue_number": None,
            "github_issue_url": None,
        }


    def set_skill_enabled(self, skill_key: str, *, enabled: bool) -> dict[str, Any]:
        skill = self.repository.find_one("skills", {"skill_key": skill_key})
        if not skill:
            raise KeyError(f"skill not found: {skill_key}")
        if not skill.get("checksum") or not skill.get("permission_scope") or not skill.get("risk_level"):
            raise ValueError("skill cannot be enabled without checksum, permission_scope and risk_level")
        status = "approved" if enabled else "blocked"
        return self.repository.update_record("skills", skill["skill_id"], {"status": status})

    def create_skill_usage_event(self, request: SkillUsageCreate | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, SkillUsageCreate) else SkillUsageCreate(**request)
        skill = self.repository.find_one("skills", {"skill_key": data.skill_key})
        if not skill:
            raise KeyError(f"skill not found: {data.skill_key}")
        if skill.get("status") != "approved":
            raise ValueError(f"skill is not enabled: {data.skill_key}")
        if not skill.get("checksum") or not skill.get("permission_scope") or not skill.get("risk_level"):
            raise ValueError("skill usage requires checksum, permission_scope and risk_level")
        row = {"skill_event_id": new_id("skillev"), "skill_id": skill["skill_id"], **data.model_dump()}
        event = self.repository.create_record("skill_events", row)
        if data.task_id:
            event_type = {"started": "skill_started", "completed": "skill_done", "failed": "skill_failed", "cancelled": "triage_required"}[data.status]
            self.add_task_event(
                data.task_id,
                TaskEventCreate(
                    event_type=event_type,
                    severity="error" if data.status == "failed" else "warning" if data.status == "cancelled" else "info",
                    message=f"Skill trace: {data.skill_key}",
                    payload_json={"skill_event_id": event["skill_event_id"], "status": data.status},
                ),
            )
        return event

    def create_external_agent_session(self, request: ExternalAgentSessionCreate | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, ExternalAgentSessionCreate) else ExternalAgentSessionCreate(**request)
        return self.repository.create_record("external_sessions", {"session_id": new_id("extsess"), **data.model_dump(), "last_seen_at": utc_now().isoformat()})

    def create_external_agent_event(self, request: ExternalAgentEventCreate | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, ExternalAgentEventCreate) else ExternalAgentEventCreate(**request)
        if not self.repository.get_record("external_sessions", data.session_id):
            raise KeyError(f"external session not found: {data.session_id}")
        if data.event_type in {"context_pack_written", "evidence_written"} and not data.evidence_refs:
            raise ValueError(f"{data.event_type} requires evidence_refs")
        return self.repository.create_record("external_events", {"external_event_id": new_id("extev"), **data.model_dump()})

    def create_trace_event(self, request: TraceEventCreate | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, TraceEventCreate) else TraceEventCreate(**request)
        return self.repository.create_record("trace_events", {"trace_id": new_id("trace"), **data.model_dump()})

    def github_sync_issue_candidate(self, candidate_id: str, request: IssueCandidateGithubSyncRequest | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, IssueCandidateGithubSyncRequest) else IssueCandidateGithubSyncRequest(**request)
        recommended_tools = ["report_bug", "mcp_github_issue_create", "mcp_github_issue_sync_bug"]
        gate = {
            "mode": data.mode,
            "github_sync_status": "blocked",
            "reason": "ra_github_sync_retired_use_standard_workflow",
            "blocked_reason": "RA GitHub sync is retired; use AIstock issue workflow / Validation MCP.",
            "standard_workflow_required": True,
            "storage_performed": False,
            "direct_github_create_performed": False,
            "approval_id": data.approval_id,
            "requested_by": data.requested_by,
            "assistant_draft_storage_notice": RA_DRAFT_STORAGE_NOTICE,
            "official_submission_required": RA_OFFICIAL_WORKFLOW_NOTICE,
            "recommended_tools": recommended_tools,
        }
        return {
            "candidate_id": candidate_id,
            "status": "retired",
            "github_sync_status": "blocked",
            "github_sync_json": gate,
            "reason": gate["reason"],
            "standard_workflow_required": True,
            "storage_performed": False,
            "direct_github_create_performed": False,
            "recommended_tools": recommended_tools,
            "draft_storage_authoritative": False,
            "retired_draft_tables": ["assistant_issue_candidates", "assistant_validation_discovery_reports"],
            "assistant_draft_storage_notice": RA_DRAFT_STORAGE_NOTICE,
            "official_submission_required": RA_OFFICIAL_WORKFLOW_NOTICE,
            "github_issue_number": None,
            "github_issue_url": None,
        }

    def dry_run_execute_tool(self, request: WorkbenchDryRunExecuteRequest | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, WorkbenchDryRunExecuteRequest) else WorkbenchDryRunExecuteRequest(**request)
        preflight = self.preflight_mcp_tool(McpPreflightRequest(**data.model_dump(exclude={"deep_link"})))
        result = {
            "dry_run": True,
            "status": "approval_required" if preflight["approval_required"] else "ready",
            "preflight": preflight,
            "tool_result": {"executed": False, "reason": "dry_run_execute_only"},
            "deep_link": data.deep_link or f"/research-assistant/workbench?tool_event_id={preflight['tool_event_id']}",
        }
        self.repository.update_record("mcp_tool_events", preflight["tool_event_id"], {"event_type": "dry_run_execute", "response_json": result})
        if data.task_id:
            event_type = "approval_required" if preflight["approval_required"] else "mcp_preflight_failed" if preflight.get("failed_checks") else "mcp_preflight_passed"
            self.add_task_event(data.task_id, TaskEventCreate(event_type=event_type, message=f"Workbench dry-run execute recorded: {result['status']}", payload_json=result))
        return result

    def route_model(self, request: ModelRouteRequest | dict[str, Any]) -> dict[str, Any]:
        data = request if isinstance(request, ModelRouteRequest) else ModelRouteRequest(**request)
        runtime_config = self.active_runtime_config()
        policies = self.repository.list_records(
            "routing_policies",
            filters={"role": data.role, "risk_level": data.risk_level, "status": "enabled"},
            limit=self.configured_limit("routing_policy_primary"),
        )["items"]
        if not policies:
            policies = self.repository.list_records(
                "routing_policies",
                filters={"role": data.role, "status": "enabled"},
                limit=self.configured_limit("routing_policy_role_fallback"),
            )["items"]
        policies = [policy for policy in policies if self._routing_selector_matches(policy, data, runtime_config)]
        if data.role == "primary_reasoner" and data.risk_level not in {"high", "production_sensitive"}:
            long_context_policies = self.repository.list_records(
                "routing_policies",
                filters={"role": "long_context", "status": "enabled"},
                limit=self.configured_limit("routing_policy_role_fallback"),
            )["items"]
            long_context_policies = [policy for policy in long_context_policies if self._routing_selector_matches(policy, data, runtime_config)]
            policies = [*long_context_policies, *policies]
        selected = policies[0] if policies else None
        profile_id = selected.get("model_profile_id") or selected.get("primary_profile_id") if selected else None
        profile = self.repository.get_record("model_profiles", profile_id) if profile_id else None
        route_status = "selected"
        fallback_reason = None
        if profile and profile.get("status") != "enabled":
            if data.risk_level in {"high", "production_sensitive"}:
                route_status = "blocked_disabled_profile"
                fallback_reason = "high risk route cannot fallback from disabled profile"
                profile = None
            else:
                fallback_reason = f"profile {profile_id} is {profile.get('status')}"
                fallback_id = (selected.get("fallback_json") or {}).get("fallback_profile_id") if selected else None
                fallback_profile = self.repository.get_record("model_profiles", fallback_id) if fallback_id else None
                if fallback_profile and fallback_profile.get("status") == "enabled":
                    profile = fallback_profile
                    route_status = "fallback_selected"
                else:
                    selected = None
                    profile = None
                    route_status = "fallback_selected"
                for policy in self.repository.list_records("routing_policies", filters={"status": "enabled"}, limit=self.configured_limit("routing_policy_scan"))["items"]:
                    if profile is not None:
                        break
                    candidate_id = policy.get("model_profile_id") or policy.get("primary_profile_id")
                    candidate = self.repository.get_record("model_profiles", candidate_id) if candidate_id else None
                    if candidate and candidate.get("status") == "enabled":
                        selected = policy
                        profile = candidate
                        break
                if profile is None:
                    route_status = "blocked_no_enabled_profile"
        return {
            "role": data.role,
            "risk_level": data.risk_level,
            "token_estimate": data.token_estimate,
            "policy": selected,
            "model_profile": profile,
            "route_status": route_status,
            "fallback_reason": fallback_reason,
            "temp_memory_only_for_low_cost": bool(profile and profile.get("role") == "cheap_worker"),
        }

    def _routing_selector_matches(self, policy: dict[str, Any], request: ModelRouteRequest, runtime_config: dict[str, Any]) -> bool:
        selector = policy.get("selector_json") or {}
        threshold_path = selector.get("token_estimate_gte_config_path")
        if threshold_path:
            threshold = int(self._config_path_value(runtime_config, str(threshold_path)))
            return request.token_estimate >= threshold
        return True

    @staticmethod
    def _config_path_value(config: dict[str, Any], path: str) -> Any:
        current: Any = config
        for part in path.split("."):
            if not isinstance(current, dict) or part not in current:
                raise KeyError(f"runtime config path not found: {path}")
            current = current[part]
        return current

    def create_temp_memory(self, payload: dict[str, Any]) -> dict[str, Any]:
        content_json = payload.get("content_json") or {}
        content_text = str(payload.get("content_text") or "")
        if not payload.get("task_id") and not payload.get("stream_id"):
            raise ValueError("temp memory requires task_id or stream_id")
        if payload.get("memory_type") in {"core", "procedural", "architecture"}:
            self._consume_approval_gate(
                approval_id=payload.get("approval_id"),
                confirmation_text=payload.get("confirmation_text"),
                approval_type="temp_memory.sensitive",
            )
        return self.repository.create_record(
            "temp_memories",
            {
                "temp_memory_id": new_id("tmpmem"),
                "task_id": payload.get("task_id"),
                "stream_id": payload.get("stream_id"),
                "memory_type": payload.get("memory_type", "task_state"),
                "content_json": content_json,
                "content_text": content_text,
                "evidence_refs": payload.get("evidence_refs") or [],
                "confidence": payload.get("confidence", 0.5),
                "expires_at": payload.get("expires_at") or "2099-12-31T00:00:00+00:00",
                "model_profile_id": payload.get("created_by_model_profile_id") or payload.get("model_profile_id"),
                "created_by_model_profile_id": payload.get("created_by_model_profile_id"),
            },
        )

    def notification_summary(self, user_id: str = "default") -> dict[str, Any]:
        page = self.repository.list_records("notifications", filters={"user_id": user_id}, limit=self.configured_limit("notification_summary_items"))
        counts: dict[str, int] = {}
        for item in page["items"]:
            status = str(item.get("status") or "unknown")
            counts[status] = counts.get(status, 0) + 1
        return {"user_id": user_id, "counts": counts, "unread": counts.get("unread", 0), "items": page["items"][: self.configured_limit("notification_summary_preview")]}

    def validation_discovery_summary(self) -> dict[str, Any]:
        limit = self.configured_limit("validation_issue_candidates")
        try:
            summary = self.issue_fact_source.issue_candidate_summary()
            candidates_page = self.issue_fact_source.issue_candidates(page=1, page_size=limit)
        except Exception as exc:  # noqa: BLE001 - degraded read is explicit and never falls back to RA drafts.
            degraded = self._degraded_pipeline_issue_candidate_page(exc, page=1, page_size=limit)
            return {
                "schema_version": "aistock_research_assistant_validation_discovery_summary_v1",
                "generated_at": utc_now().isoformat(),
                "data_state": "degraded",
                "status": "degraded",
                "source_of_truth": PIPELINE_ISSUE_SOURCE_OF_TRUTH,
                "source_of_truth_endpoint": "/api/v1/validation/issues/candidates(+/summary)",
                "discovery_report_mode": "derived_from_validation_candidates",
                "discovery_manifest_api_available": False,
                "discovery_manifest_api_note": "No canonical backend read API for raw Nightly discovery manifest/report was found; this assistant view is derived from Validation candidate fields.",
                "latest_reports": [],
                "candidate_summary": degraded,
                "candidate_issues_needing_review": [],
                "reason_codes": [PIPELINE_ISSUE_FACT_SOURCE_UNAVAILABLE],
                "warnings": degraded.get("warnings") or [],
                "draft_storage_authoritative": False,
                "retired_draft_tables": ["assistant_issue_candidates", "assistant_validation_discovery_reports"],
                "assistant_draft_storage_notice": RA_DRAFT_STORAGE_NOTICE,
                "official_submission_required": RA_OFFICIAL_WORKFLOW_NOTICE,
                "assistant_draft_substitution_blocked": True,
            }
        candidates = [self._assistant_issue_candidate_view(item) for item in candidates_page.get("items") or [] if isinstance(item, dict)]
        latest_reports = [self._derived_discovery_report_view(item) for item in candidates if self._has_discovery_fields(item)]
        return {
            "schema_version": "aistock_research_assistant_validation_discovery_summary_v1",
            "generated_at": utc_now().isoformat(),
            "data_state": "complete",
            "status": "ok",
            "source_of_truth": PIPELINE_ISSUE_SOURCE_OF_TRUTH,
            "source_of_truth_endpoint": "/api/v1/validation/issues/candidates(+/summary)",
            "discovery_report_mode": "derived_from_validation_candidates",
            "discovery_manifest_api_available": False,
            "discovery_manifest_api_note": "No canonical backend read API for raw Nightly discovery manifest/report was found; this assistant view is derived from Validation candidate fields.",
            "latest_reports": latest_reports,
            "candidate_summary": self._with_pipeline_issue_metadata(dict(summary)),
            "candidate_issues_needing_review": candidates,
            "reason_codes": ["candidate_queue_empty"] if not candidates else [],
            "warnings": [],
            "draft_storage_authoritative": False,
            "retired_draft_tables": ["assistant_issue_candidates", "assistant_validation_discovery_reports"],
            "assistant_draft_storage_notice": RA_DRAFT_STORAGE_NOTICE,
            "official_submission_required": RA_OFFICIAL_WORKFLOW_NOTICE,
            "assistant_draft_substitution_blocked": True,
        }

    @staticmethod
    def _has_discovery_fields(item: dict[str, Any]) -> bool:
        return any(item.get(key) for key in ("source_type", "source_plan_key", "active_discovery_reason", "source_path", "source_paths"))

    @staticmethod
    def _derived_discovery_report_view(item: dict[str, Any]) -> dict[str, Any]:
        candidate_id = str(item.get("candidate_id") or "unknown")
        return {
            "schema_version": "aistock_research_assistant_derived_discovery_report_v1",
            "discovery_report_id": f"derived_validation_candidate_{candidate_id}",
            "title": item.get("title") or candidate_id,
            "status": item.get("status") or "unknown",
            "run_date": item.get("last_seen_at") or item.get("created_at"),
            "candidate_id": candidate_id,
            "source_ref": f"validation_issue_candidates:{candidate_id}",
            "source_refs": item.get("source_refs") or [f"validation_issue_candidates:{candidate_id}"],
            "source_type": item.get("source_type"),
            "source_plan_key": item.get("source_plan_key"),
            "active_discovery_reason": item.get("active_discovery_reason"),
            "source_paths": item.get("source_paths") or ([item.get("source_path")] if item.get("source_path") else []),
            "discovery_report_mode": "derived_from_validation_candidates",
            "draft_storage_authoritative": False,
            "assistant_draft_storage_notice": RA_DRAFT_STORAGE_NOTICE,
        }

    def generate_scheduled_proactive_report(
        self,
        *,
        report_date: date | None = None,
        report_type: str = "morning_brief",
        registry: ProactiveReportProviderRegistry | None = None,
        cheap_worker: Any | None = None,
    ) -> dict[str, Any]:
        """Generate the L3 scheduled morning report from read-only providers."""

        context = ProactiveReportContext(
            repository=self.repository,
            report_date=report_date or date.today(),
            report_type=report_type,
            max_items_per_section=self.configured_limit("validation_reports"),
            issue_fact_source=self.issue_fact_source,
        )
        report = generate_proactive_report(
            context=context,
            registry=registry or build_default_proactive_report_registry(),
            report_id_factory=lambda prefix: f"{prefix}_{context.report_type}_{context.report_date.isoformat()}",
            cheap_worker=cheap_worker,
        )
        return self.repository.create_record(
            "proactive_reports",
            {
                "report_id": report["report_id"],
                "report_type": report["report_type"],
                "report_date": report["report_date"],
                "summary_md": report["summary_md"],
                "sections_json": report["sections_json"],
                "source_refs_json": report["source_refs_json"],
                "status": report["status"],
            },
        )

    def run_prompt_lab_offline(
        self,
        *,
        target_prompt_key: str,
        optimizer: str = "gepa",
        eval_limit: int = 20,
        offline_judge: OfflinePromptJudge | None = None,
    ) -> dict[str, Any]:
        """Create a gated Prompt Lab candidate without changing prompt activation."""

        baseline = self._prompt_text_for_key(target_prompt_key)
        eval_set = collect_prompt_lab_eval_set(
            self.repository,
            target_prompt_key=target_prompt_key,
            limit=eval_limit,
        )
        candidate = build_prompt_lab_candidate(
            target_prompt_key=target_prompt_key,
            baseline_text=baseline,
            optimizer=optimizer,
            eval_set=eval_set,
        )
        source_refs = list(candidate.get("source_refs") or [])
        score = judge_prompt_lab_candidate(
            judge=offline_judge,
            target_prompt_key=target_prompt_key,
            baseline_text=baseline,
            candidate_text=str(candidate["candidate_text"]),
            eval_items=list(eval_set.get("items") or []),
            source_refs=source_refs,
        )
        merged_score = {
            **score,
            "candidate_reason_codes": list(candidate.get("reason_codes") or []),
            "candidate_warnings": list(candidate.get("warnings") or []),
            "activation_changed": False,
            "offline_only": True,
        }
        approval = self.create_approval(
            ApprovalCreate(
                approval_type="prompt_lab.activate",
                risk_level="high",
                plan_digest=prompt_lab_plan_digest(
                    target_prompt_key=target_prompt_key,
                    candidate_text=str(candidate["candidate_text"]),
                    eval_set_ref=str(candidate["eval_set_ref"]),
                ),
                summary=f"Prompt Lab candidate for {target_prompt_key}; human approval required before prompt activation",
                required_confirmation_text=f"APPROVE PROMPT LAB {target_prompt_key}",
                created_by="prompt_lab_offline",
            )
        )
        return self.repository.create_record(
            "prompt_lab_runs",
            {
                "lab_run_id": new_id("plab"),
                "target_prompt_key": target_prompt_key,
                "optimizer": optimizer,
                "eval_set_ref": candidate["eval_set_ref"],
                "candidate_text": candidate["candidate_text"],
                "judge_score_json": merged_score,
                "status": "candidate",
                "approval_request_id": approval["approval_id"],
            },
        )

    def activate_prompt_lab_candidate(
        self,
        lab_run_id: str,
        *,
        approval_id: str | None = None,
        confirmation_text: str | None = None,
    ) -> dict[str, Any]:
        """Gate prompt activation changes; Phase 11 never activates candidates silently."""

        run = self.repository.get_record("prompt_lab_runs", lab_run_id)
        if not run:
            raise KeyError(f"prompt lab run not found: {lab_run_id}")
        if run.get("status") != "candidate":
            raise ValueError(f"prompt lab run is not candidate: {run.get('status')}")
        if approval_id != run.get("approval_request_id"):
            raise ValueError("prompt_lab activation requires the run approval_request_id")
        self._consume_approval_gate(
            approval_id=approval_id,
            confirmation_text=confirmation_text,
            approval_type="prompt_lab.activate",
            required_summary_fragment=str(run.get("target_prompt_key") or ""),
        )
        return {
            "status": "approval_recorded",
            "lab_run_id": lab_run_id,
            "activation_changed": False,
            "reason_codes": ["prompt_lab_activation_not_implemented_in_phase11"],
            "warnings": [
                "Phase 11 records the human approval gate only; applying a new prompt activation remains a separate reviewed Prompt Pack workflow."
            ],
        }

    def deposit_successful_workflow_skill(
        self,
        *,
        task_id: str,
        skill_key: str | None = None,
        description: str | None = None,
    ) -> dict[str, Any]:
        """Deposit a successful workflow as a draft skill; no reuse is enabled here."""

        built = build_successful_workflow_recipe(
            self.repository,
            task_id=task_id,
            skill_key=skill_key,
            description=description,
            event_limit=self.configured_limit("task_events_detail"),
        )
        if built["status"] == "degraded":
            return built
        row = self.repository.create_record(
            "skill_library",
            {
                "skill_id": new_id("sklib"),
                "skill_key": built["skill_key"],
                "description": built["description"],
                "recipe_json": built["recipe_json"],
                "success_count": 1,
                "provenance_json": built["provenance_json"],
                "status": "draft",
            },
        )
        approval = self.create_approval(
            ApprovalCreate(
                task_id=task_id,
                approval_type=SKILL_LIBRARY_APPROVAL_TYPE,
                risk_level="high",
                plan_digest=skill_library_plan_digest(
                    skill_key=str(row["skill_key"]),
                    recipe_json=dict(row.get("recipe_json") or {}),
                    provenance_json=dict(row.get("provenance_json") or {}),
                ),
                summary=f"Skill Library approval required for {row['skill_key']}",
                required_confirmation_text=f"{SKILL_LIBRARY_APPROVAL_PREFIX} {row['skill_key']}",
                created_by="skill_library_deposit",
            )
        )
        recipe_json = dict(row.get("recipe_json") or {})
        recipe_json["approval_request_id"] = approval["approval_id"]
        row = self.repository.update_record(
            "skill_library",
            str(row["skill_id"]),
            {
                "recipe_json": recipe_json,
                "provenance_json": {
                    **dict(row.get("provenance_json") or {}),
                    "approval_request_id": approval["approval_id"],
                    "approval_required": True,
                },
            },
        )
        row["approval_request_id"] = approval["approval_id"]
        return row

    def approve_skill_library_entry(
        self,
        skill_id: str,
        *,
        approval_id: str | None = None,
        confirmation_text: str | None = None,
    ) -> dict[str, Any]:
        """Promote a draft skill only after consuming assistant_approval_requests."""

        skill = self.repository.get_record("skill_library", skill_id)
        if not skill:
            raise KeyError(f"skill library entry not found: {skill_id}")
        if skill.get("status") != "draft":
            raise ValueError(f"skill library entry is not draft: {skill.get('status')}")
        provenance = dict(skill.get("provenance_json") or {})
        expected_approval = provenance.get("approval_request_id") or (skill.get("recipe_json") or {}).get("approval_request_id")
        if approval_id != expected_approval:
            raise ValueError("skill_library approval requires the entry approval_request_id")
        self._consume_approval_gate(
            approval_id=approval_id,
            confirmation_text=confirmation_text,
            approval_type=SKILL_LIBRARY_APPROVAL_TYPE,
            required_summary_fragment=str(skill.get("skill_key") or ""),
        )
        return self.repository.update_record(
            "skill_library",
            skill_id,
            {
                "status": "approved",
                "provenance_json": {
                    **provenance,
                    "approved_at": utc_now().isoformat(),
                    "approved_via": approval_id,
                },
            },
        )

    def search_skill_library_for_curriculum(self, *, query: str, limit: int = 5) -> dict[str, Any]:
        """Read-only L4 curriculum replay over approved Skill Library recipes."""

        return search_approved_skill_recipes(
            self.repository,
            query=query,
            limit=limit,
            evidence_refs=[f"skill_library_query:{sha256_json({'query': query})[:12]}"],
        )

    def propose_skill_reuse(
        self,
        *,
        task_id: str,
        skill_id: str,
        input_json: dict[str, Any] | None = None,
        conversation_id: str | None = None,
        context_pack_id: str | None = None,
    ) -> dict[str, Any]:
        """Create an Action Proposal for skill reuse; it never executes directly."""

        skill = self.repository.get_record("skill_library", skill_id)
        skill_source = "skill_library"
        if not skill:
            skill = self.repository.get_record("skills", skill_id)
            skill_source = "skill_registry"
        if not skill:
            raise KeyError(f"skill entry not found: {skill_id}")
        if skill.get("status") != "approved":
            return {
                "status": "blocked",
                "action_proposal": None,
                "reason_codes": ["skill_library_reuse_requires_approved_skill"],
                "warnings": [f"skill {skill.get('skill_key') or skill_id} is not approved and cannot be reused"],
            }
        recipe_json = skill.get("recipe_json") if isinstance(skill.get("recipe_json"), dict) else {}
        provenance_json = skill.get("provenance_json") if isinstance(skill.get("provenance_json"), dict) else {}
        source_refs = recipe_json.get("source_refs") or provenance_json.get("source_refs") or ([skill["source_ref"]] if skill.get("source_ref") else [])
        payload = dict(input_json or {})
        payload["skill_library_ref"] = {
            "skill_id": skill["skill_id"],
            "skill_key": skill["skill_key"],
            "source": skill_source,
            "source_refs": source_refs,
        }
        proposal = self.create_action_proposal(
            ActionProposalCreate(
                task_id=task_id,
                conversation_id=conversation_id,
                capability_key=SKILL_LIBRARY_REUSE_CAPABILITY_KEY,
                proposal_type="skill",
                title=f"复用技能：{skill['skill_key']}",
                summary="Skill Library 复用必须经 Action Proposal 确认、preflight 和 approval；本步骤不直接执行技能。",
                input_json=payload,
                expected_result_json={
                    "skill_library_ref": payload["skill_library_ref"],
                    "direct_execution_allowed": False,
                    "risk_gate": "action_proposal_preflight_approval",
                },
                context_pack_id=context_pack_id,
                idempotency_key=sha256_json({"task_id": task_id, "skill_id": skill_id, "input_json": payload}),
                created_by="skill_library_reuse",
            )
        )
        return {
            "status": "proposal_created",
            "action_proposal": proposal,
            "reason_codes": [],
            "warnings": [],
        }

    def _prompt_text_for_key(self, target_prompt_key: str) -> str:
        node = self.declarative_config.prompt_node(target_prompt_key)
        if node and str(node.get("prompt_text") or "").strip():
            return str(node["prompt_text"])
        raise KeyError(f"prompt text not found for target_prompt_key={target_prompt_key}; fix prompt pack YAML and reload")

    def _ensure_default_reports_and_notifications(self, seeded: dict[str, int]) -> None:
        if not self.repository.list_records("reports", limit=1)["items"]:
            proactive_report = generate_proactive_report(
                context=ProactiveReportContext(repository=self.repository, report_date=date.today(), report_type="morning_brief"),
                registry=build_default_proactive_report_registry(),
                report_id_factory=lambda prefix: f"{prefix}_morning_brief_{date.today().isoformat()}",
            )
            self.repository.create_record(
                "reports",
                {
                    "report_id": "report_research_assistant_phase1_morning",
                    "report_type": "morning",
                    "title": "研究助理晨报",
                    "body_md": proactive_report["summary_md"],
                    "summary_json": proactive_report["sections_json"],
                    "evidence_refs": proactive_report["source_refs_json"],
                    "status": proactive_report["status"],
                },
            )
            seeded["reports"] += 1
        if not self.repository.list_records("notifications", limit=1)["items"]:
            self.repository.create_record(
                "notifications",
                {
                    "notification_id": "notif_research_assistant_phase1_ready",
                    "user_id": "default",
                    "source_type": "system",
                    "source_id": "research_assistant_phase1",
                    "title": "研究助理阶段一目录已就绪",
                    "message": "MCP、Skill、Memory、审批、候选 Issue 和模型路由目录可检查。",
                    "status": "unread",
                    "severity": "info",
                    "metadata_json": {"phase": "phase1"},
                },
            )
            seeded["notifications"] += 1
