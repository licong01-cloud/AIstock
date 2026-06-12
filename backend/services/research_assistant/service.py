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
from enum import Enum
from datetime import date
from pathlib import Path
from time import perf_counter
from typing import Any

import jsonschema

from backend.mcp.tool_manifest import TOOL_MANIFEST, TOOL_MANIFEST_BY_NAME
from backend.infra.deepseek_config import DEFAULT_DEEPSEEK_MODEL, resolve_deepseek_config

from .context_budget import ContextBudgetPlan, ContextBudgetPlanner
from .code_intelligence import artifact_ref_paths, build_code_intelligence_context
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
    PromptNodeCreate,
    TaskCreate,
    SkillUsageCreate,
    TaskEventCreate,
    TraceEventCreate,
    WorkbenchDryRunExecuteRequest,
    new_id,
    sha256_json,
    utc_now,
)
from .react_grounding import (
    McpToolCall,
    McpToolResult,
    ModelTurn,
    ReactGroundingConfig,
    ToolCatalogEntry,
    ToolGateDecision,
    run_react_grounding_loop,
)
from .prompt_pack import (
    DEFAULT_PROMPT_PACK_PATH,
    PromptPackSnapshot,
    load_prompt_pack,
)
from .repository import DatabaseResearchAssistantRepository
from .runtime_config import DEFAULT_ENVIRONMENT, REPO_ROOT, RUNTIME_CONFIG_KEY, RuntimeConfigSnapshot, load_runtime_config
from .domain_ontology import domain_prompt_key
from .mcp_catalog_sync import (
    canonicalize_server_key,
    default_mcp_servers,
    default_mcp_tools,
    enrich_mcp_server_record,
    gateway_catalog,
    manifest_entry_to_mcp_tool,
    server_key_for_module,
    workflow_capabilities as catalog_workflow_capabilities,
)
from .tool_router import route_request
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
        if task.agent_key == "qe_experiment_designer" and isinstance(task.input_json.get("qe_autonomy_request"), dict):
            report = self.service.run_qe_autonomous_evolution(dict(task.input_json["qe_autonomy_request"]))
            report_dict = report.to_dict() if hasattr(report, "to_dict") else dict(report)
            status = "failed" if report_dict.get("status") == "failed" else "succeeded"
            evidence_refs = tuple(sorted(str(ref) for ref in report_dict.get("evidence_refs", []) or ["qe_autonomy_report"]))
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
                result_json={"autonomy_report": report_dict, "worker_consumed_autonomy": True},
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
        return WorkerRunResult(
            agent_run_id="service_runtime_pending",
            parent_task_id=task.parent_task_id,
            agent_key=task.agent_key,
            role=task.role,
            status=status,
            task_order=task.task_order,
            summary=result.final_text,
            artifacts=tuple(str(ref) for tool_result in result.tool_results for ref in tool_result.artifact_refs),
            evidence_refs=tuple(sorted({ref for tool_result in result.tool_results for ref in tool_result.source_refs} | {"agent_team_context"})),
            result_json={"react_stopped_reason": result.stopped_reason, "tool_result_count": len(result.tool_results), "cards": cards},
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

    def execute_read_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        route = self.cards.get("mcp_route_decision") if isinstance(self.cards.get("mcp_route_decision"), dict) else {}
        payload = dict(call.payload_json)
        payload.setdefault("request", self.user_message)
        payload.setdefault("route", route)
        payload.setdefault("mcp_route_decision", route)
        payload.setdefault("selected_tool", {"server_key": call.server_key, "tool_name": call.tool_name})
        payload.setdefault("limit", 20)
        capability_key = self.service._capability_key_for_tool(call, route)
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
        )
        self.service._populate_cards_from_tool_execution(self.cards, proposal, executed, result)
        return result

    def preflight_confirmation_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        route = self.cards.get("mcp_route_decision") if isinstance(self.cards.get("mcp_route_decision"), dict) else {}
        payload = dict(call.payload_json)
        payload.setdefault("request", self.user_message)
        payload.setdefault("route", route)
        payload.setdefault("mcp_route_decision", route)
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
        try:
            preflight = self.service.preflight_action_proposal(
                proposal["action_proposal_id"],
                ActionProposalPreflightRequest(payload_json=payload, idempotency_key=proposal["idempotency_key"]),
            )
        except Exception as exc:
            preflight = {"proposal": proposal, "preflight": {"passed": False, "approval_required": True, "failed_checks": [{"check": "preflight", "detail": str(exc)}]}}
        proposal_state = preflight.get("proposal") if isinstance(preflight.get("proposal"), dict) else proposal
        preflight_payload = preflight.get("preflight") if isinstance(preflight.get("preflight"), dict) else {}
        status = str(proposal_state.get("status") or "preflight_required")
        self.cards.setdefault("action_proposals", [])
        self.cards["action_proposals"].append(
            {
                "title": proposal["title"],
                "risk": decision.risk_level,
                "approval_required": True,
                "status": status,
                "action_proposal_id": proposal["action_proposal_id"],
                "route": f"{call.server_key}/{call.tool_name}",
                "required_confirmations": (decision.catalog_entry.required_confirmations if decision.catalog_entry else ()),
            }
        )
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
        return McpToolResult(
            server_key=call.server_key,
            tool_name=call.tool_name,
            status=self.cards["mcp_execution_result"]["status"],
            payload_json={"preflight_only": True},
            source_refs=["preflight"],
            as_of=utc_now().date().isoformat(),
            summary="preflight confirmation card generated; write/high-risk execution was not called",
            action_proposal_id=proposal["action_proposal_id"],
            preflight=preflight_payload,
            executed=False,
            blocked_reason="preflight_confirmation_required",
        )



logger = logging.getLogger("aistock.research_assistant.service")

ASSISTANT_APPROVAL_CONFIRM = "APPROVE_RESEARCH_ASSISTANT_ACTION"
PROMPT_CACHE_DIR = Path(os.getenv("AISTOCK_ASSISTANT_PROMPT_CACHE_DIR", "var/research_assistant/prompt_cache"))
CATALOG_BOOTSTRAP_ACTION = "POST /api/v1/research-assistant/catalogs/seed"
SERVICE_MODULE_PATH = Path(__file__).resolve()


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


DEFAULT_WORKFLOW_CAPABILITIES: list[dict[str, Any]] = [
    *[dict(item) for item in load_runtime_config(environment=DEFAULT_ENVIRONMENT).config["planner"].get("workflow_capabilities", [])],
    *catalog_workflow_capabilities(),
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
class LlmCallResult:
    content: str
    provider: str
    model: str
    duration_ms: int
    usage: dict[str, Any]


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




def _default_workflow_capabilities() -> list[dict[str, Any]]:
    return DEFAULT_WORKFLOW_CAPABILITIES


class ResearchAssistantLlmClient:
    """Small LiteLLM wrapper for assistant chat turns.

    Tests inject a fake client. Production calls fail fast if litellm or model
    credentials are missing; there is no canned success fallback.
    """

    def complete(self, *, messages: list[dict[str, str]], model_profile: dict[str, Any], temperature: float, max_tokens: int) -> LlmCallResult:
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
        response = litellm.completion(
            model=model_id,
            messages=provider_messages,
            temperature=temperature,
            max_tokens=max_tokens,
            **completion_kwargs,
        )
        duration_ms = int((perf_counter() - start) * 1000)
        content = str(response.choices[0].message.content or "").strip()
        usage_raw = getattr(response, "usage", None)
        usage = dict(usage_raw) if isinstance(usage_raw, dict) else {}
        if not content:
            raise RuntimeError("assistant LLM returned empty content")
        return LlmCallResult(content=content, provider=provider, model=model_id, duration_ms=duration_ms, usage=usage)


class ResearchAssistantService(ResearchAssistantExecutionMixin):
    @staticmethod
    def default_workflow_capabilities() -> list[dict[str, Any]]:
        return _default_workflow_capabilities()

    def __init__(self, repository: Any | None = None, llm_client: Any | None = None, *, environment: str = DEFAULT_ENVIRONMENT) -> None:
        self.repository = repository or DatabaseResearchAssistantRepository()
        self.llm_client = llm_client or ResearchAssistantLlmClient()
        self.environment = environment
        self.context_budget_planner = ContextBudgetPlanner()


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
        configured = self.active_runtime_config().get("planner", {}).get("workflow_capabilities")
        if configured is None:
            source = self.default_workflow_capabilities()
        else:
            if not isinstance(configured, list):
                raise ValueError("planner.workflow_capabilities must be a list when configured")
            source = [dict(item) for item in configured]
        merged: dict[str, dict[str, Any]] = {
            str(item.get("capability_key")): self._canonicalize_capability_mcp_refs(dict(item))
            for item in source
        }
        for item in catalog_workflow_capabilities():
            merged[str(item["capability_key"])] = self._canonicalize_capability_mcp_refs(dict(item))
        return list(merged.values())

    @staticmethod
    def _canonicalize_capability_mcp_refs(capability: dict[str, Any]) -> dict[str, Any]:
        refs = capability.get("mcp_tool_refs")
        if refs is None:
            return capability
        if not isinstance(refs, list):
            raise ValueError(f"capability {capability.get('capability_key')} mcp_tool_refs must be a list")
        canonical_refs: list[dict[str, Any]] = []
        for ref in refs:
            if not isinstance(ref, dict):
                raise ValueError(f"capability {capability.get('capability_key')} mcp_tool_refs entries must be objects")
            item = dict(ref)
            if item.get("server_key"):
                item["server_key"] = canonicalize_server_key(str(item["server_key"]))
            canonical_refs.append(item)
        capability["mcp_tool_refs"] = canonical_refs
        return capability

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

    def active_runtime_config(self) -> dict[str, Any]:
        activation = self.repository.find_one(
            "runtime_config_activations",
            {"config_key": RUNTIME_CONFIG_KEY, "environment": self.environment, "status": "active"},
        )
        if not activation:
            raise RuntimeError("Research Assistant runtime config activation is missing; run catalog seed/import first")
        return dict(activation["config_json"])

    def active_runtime_config_activation(self) -> dict[str, Any]:
        activation = self.repository.find_one(
            "runtime_config_activations",
            {"config_key": RUNTIME_CONFIG_KEY, "environment": self.environment, "status": "active"},
        )
        if not activation:
            raise RuntimeError("Research Assistant runtime config activation is missing; run catalog seed/import first")
        return activation

    def active_prompt_activation(self) -> dict[str, Any]:
        activation = self.repository.find_one(
            "prompt_activations",
            {"assistant_key": "research_assistant", "environment": self.environment, "status": "active"},
        )
        if not activation:
            raise RuntimeError("Research Assistant prompt activation is missing; run catalog seed/import first")
        return activation

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
        issue_status = self.repository.counts("issue_candidates", "status")
        memory_status = self.repository.counts("memory_items", "approval_status")
        trace_status = self.repository.counts("trace_events", "status")
        return {
            "task_status": task_status,
            "approval_status": approval_status,
            "issue_candidate_status": issue_status,
            "memory_approval_status": memory_status,
            "trace_status": trace_status,
            "running_tasks": task_status.get("running", 0),
            "pending_approvals": approval_status.get("pending", 0),
            "candidate_issues": issue_status.get("needs_review", 0) + issue_status.get("draft", 0),
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
        runtime_config = load_runtime_config(environment=self.environment)
        self._seed_runtime_config(runtime_config, seeded)
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
        prompt_pack = load_prompt_pack(DEFAULT_PROMPT_PACK_PATH)
        self._seed_prompt_pack(prompt_pack, seeded)
        capability_sync = self.sync_capabilities({"apply": True, "requested_by": "seed_catalogs"})
        seeded["capabilities"] += int(capability_sync["applied_count"])
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
        existing_page = self.repository.list_records("capabilities", limit=self.configured_limit("api_list_capabilities"))
        existing_by_key = {str(item.get("capability_key")): item for item in existing_page["items"]}
        diff: list[dict[str, Any]] = []
        applied_count = 0
        for capability in capabilities:
            current = existing_by_key.get(str(capability["capability_key"]))
            change = "create" if not current else "unchanged" if current.get("checksum") == capability["checksum"] and current.get("status") == capability["status"] else "update"
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
        result = {
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
        self.create_trace_event(
            TraceEventCreate(
                event_type="capability_sync",
                component="research_assistant.capability_sync",
                status="applied" if data.apply else "dry_run",
                payload_json={"source_count": len(capabilities), "applied_count": applied_count, "diff": diff[:20]},
            )
        )
        return result

    def _seed_prompt_pack(self, prompt_pack: PromptPackSnapshot, seeded: dict[str, int]) -> None:
        active = self.repository.list_records(
            "prompt_activations",
            filters={"assistant_key": "research_assistant", "environment": self.environment, "status": "active"},
            limit=self.configured_limit("api_list_prompt_activations"),
        )["items"]
        for item in active:
            if item.get("activation_id") != prompt_pack.activation_id:
                self.repository.update_record("prompt_activations", str(item["activation_id"]), {"status": "retired", "active_to": utc_now().isoformat()})
        source = self.repository.create_record(
            "prompt_sources",
            {
                "source_id": prompt_pack.source_id,
                "pack_key": prompt_pack.pack_key,
                "pack_version": prompt_pack.pack_version,
                "source_path": prompt_pack.source_path,
                "source_sha256": prompt_pack.source_sha256,
                "status": "approved",
                "metadata_json": {"schema": "aistock_prompt_pack_v1"},
                "imported_by": "seed_catalogs",
            },
        )
        version_refs: list[dict[str, Any]] = []
        for item in prompt_pack.nodes:
            prompt = PromptNodeCreate(**{k: v for k, v in item.items() if k != "checksum"})
            payload = prompt.model_dump()
            payload["prompt_node_id"] = f"prompt_{prompt.prompt_key.replace('.', '_')}"
            payload["checksum"] = item.get("checksum") or sha256_json({"prompt_key": prompt.prompt_key, "version": prompt.version, "prompt_text": prompt.prompt_text})
            self.repository.create_record("prompt_nodes", payload)
            seeded["prompt_nodes"] += 1
            version_id = f"prompt_version_{prompt.prompt_key.replace('.', '_')}_{payload['checksum'][:16]}"
            version = self.repository.create_record(
                "prompt_node_versions",
                {
                    "version_id": version_id,
                    "source_id": source["source_id"],
                    "pack_key": prompt_pack.pack_key,
                    "pack_version": prompt_pack.pack_version,
                    "prompt_key": prompt.prompt_key,
                    "prompt_node_id": payload["prompt_node_id"],
                    "title": prompt.title,
                    "category": prompt.category,
                    "tree_path": prompt.tree_path,
                    "parent_key": prompt.parent_key,
                    "phase": prompt.phase,
                    "trigger_json": prompt.trigger_json,
                    "prompt_text": prompt.prompt_text,
                    "risk_level": prompt.risk_level,
                    "source_ref": prompt.source_ref or item.get("source_ref") or prompt_pack.source_path,
                    "checksum": payload["checksum"],
                    "status": "approved",
                    "metadata_json": {"source_path": prompt_pack.source_path},
                },
            )
            version_refs.append({"prompt_key": prompt.prompt_key, "version_id": version["version_id"], "checksum": payload["checksum"]})
            seeded["prompt_node_versions"] += 1
        self.repository.create_record(
            "prompt_activations",
            {
                "activation_id": prompt_pack.activation_id,
                "assistant_key": "research_assistant",
                "environment": self.environment,
                "pack_key": prompt_pack.pack_key,
                "pack_version": prompt_pack.pack_version,
                "source_id": source["source_id"],
                "version_refs": version_refs,
                "bundle_signature": sha256_json(version_refs),
                "status": "active",
                "activated_by": "seed_catalogs",
                "activation_metadata_json": {"source_sha256": prompt_pack.source_sha256},
            },
        )
        self.repository.create_record(
            "prompt_activation_events",
            {
                "event_id": new_id("pactevt"),
                "activation_id": prompt_pack.activation_id,
                "event_type": "seed_or_refresh",
                "actor": "seed_catalogs",
                "event_json": {"pack_key": prompt_pack.pack_key, "pack_version": prompt_pack.pack_version},
            },
        )
        seeded["prompt_activations"] += 1

    def _seed_runtime_config(self, runtime_config: RuntimeConfigSnapshot, seeded: dict[str, int]) -> None:
        active = self.repository.list_records(
            "runtime_config_activations",
            filters={"config_key": runtime_config.config_key, "environment": runtime_config.environment, "status": "active"},
            limit=int(runtime_config.config["query_limits"]["api_list_runtime_config_activations"]),
        )["items"]
        for item in active:
            if item.get("activation_id") != runtime_config.activation_id:
                self.repository.update_record("runtime_config_activations", str(item["activation_id"]), {"status": "retired", "active_to": utc_now().isoformat()})
        source = self.repository.create_record(
            "runtime_config_sources",
            {
                "source_id": runtime_config.source_id,
                "config_key": runtime_config.config_key,
                "config_version": runtime_config.config_version,
                "source_path": runtime_config.source_path,
                "source_sha256": runtime_config.source_sha256,
                "config_json": runtime_config.config,
                "status": "approved",
                "metadata_json": {"schema": runtime_config.config.get("schema_version")},
                "imported_by": "seed_catalogs",
            },
        )
        self.repository.create_record(
            "runtime_config_activations",
            {
                "activation_id": runtime_config.activation_id,
                "config_key": runtime_config.config_key,
                "config_version": runtime_config.config_version,
                "environment": runtime_config.environment,
                "source_id": source["source_id"],
                "config_json": runtime_config.config,
                "status": "active",
                "activated_by": "seed_catalogs",
                "activation_metadata_json": {"source_sha256": runtime_config.source_sha256},
            },
        )
        seeded["runtime_config_activations"] += 1

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
        return self.repository.list_records(kind, filters=filters, search=search, limit=resolved_limit, offset=offset)

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
        available = self.repository.list_records("prompt_nodes", filters={"status": "enabled"}, limit=self.configured_limit("prompt_nodes_active"))["items"]
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
            if intent_value == DialogueIntent.LOCAL_DATA_MANAGEMENT_REQUEST.value:
                return DialogueIntent.LOCAL_DATA_MANAGEMENT_REQUEST
            if self._is_business_mcp_overview_request(user_message, route):
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

        if self._is_local_data_management_request(user_message):
            return DialogueIntent.LOCAL_DATA_MANAGEMENT_REQUEST

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
    def _is_business_mcp_overview_request(user_message: str, route: dict[str, Any]) -> bool:
        domain = str(route.get("domain") or "")
        if domain in {"", "general", "mcp_capability", "local_data", "validation_issue", "qe_experiment"}:
            return False
        lower = user_message.lower()
        overview_terms = ('overview', 'summary', 'catalog', 'list', 'available', '概要', '概览', '列表', '有哪些', '有什么', '可用')
        business_terms = ('因子库', '因子独立指标', '因子相关性', '模型库', '策略库', '执行策略库', 'factor library', 'factor metrics', 'factor correlation', 'model registry', 'strategy library', 'execution policy')
        return any(term in lower for term in overview_terms) and any(term in lower for term in business_terms)


    @staticmethod
    def _is_local_data_management_request(user_message: str) -> bool:
        lower = user_message.lower()
        if any(term in lower for term in ("shucang", "guidang", "outbox", "backfill", "warehouse", "archive")):
            return False
        local_markers = [
            "本地数据",
            "数据同步",
            "同步情况",
            "trade_date",
            "数据集",
            "交易日",
            "日历",
            "local_data",
            "local data",
            "data sync",
            "data_sync",
            "data-stats",
            "data_stats",
            "dataset_date_refresh_audit",
            "data_sync_targets",
            "tushare",
            "source test",
            "repair",
            "sync",
            "gap",
            "health",
            "readiness",
        ]
        return any(marker.lower() in lower for marker in local_markers)


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
        data = request if isinstance(request, ChatTurnRequest) else ChatTurnRequest(**request)
        self.ensure_catalog_ready()
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
                content_json={"phase": data.phase, "dialogue_intent": dialogue_intent.value, "dialogue_mode": mode_decision.mode.value, "mode_decision": mode_decision_json},
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

        runtime_activation = self.active_runtime_config_activation()
        runtime_config = dict(runtime_activation["config_json"])
        route_decision = self._canonicalize_mcp_route(dict(route_request(data.message)))
        initial_prior_messages = self._fetch_prior_chat_messages(conversation_id, data.message, runtime_config)
        initial_overhead = int(runtime_config["model_routing"]["initial_context_overhead_tokens"])
        history_tokens = sum(self.context_budget_planner.estimate_tokens(m["content"], runtime_config) for m in initial_prior_messages)
        estimated_total_tokens = self.context_budget_planner.estimate_tokens(data.message, runtime_config) + history_tokens + initial_overhead
        route = self.route_model(ModelRouteRequest(role="primary_reasoner", risk_level=data.risk_level, token_estimate=estimated_total_tokens))
        model_profile = route.get("model_profile")
        if not model_profile:
            raise RuntimeError(f"no enabled primary model profile for risk={data.risk_level}: {route.get('route_status')}")
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
        )
        context_health = self._context_health_payload(conversation_id, budget_plan, mode_decision=mode_decision)
        cards = self._build_human_cards(data.message, task, bundle, route, dialogue_intent, mode_decision)
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
        )
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
                cost_json={"usage": llm_result.usage},
            )
        )
        cards["react_grounding"] = self._react_grounding_card(react_result)
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
            "react_grounding",
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
        return {
            "memory_route": {
                "route_reason": memory_route.get("route_reason"),
                "matched_branches": list(memory_route.get("matched_branches") or [])[:12],
            },
            "memory_items": compact_memories,
            "graph_context": {
                "route_reason": graph_context.get("route_reason"),
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

    def _mcp_tool_catalog_snapshot(self) -> dict[str, Any]:
        servers = [item for item in self._manifest_mcp_server_records() if str(item.get("status") or "") in {"ready", "enabled", "ok"}]
        tools = [item for item in self._manifest_mcp_catalog_records() if str(item.get("status") or "") in {"enabled", "ready", "approved"}]
        capabilities = [
            item
            for item in self.repository.list_records("capabilities", filters={"status": "approved"}, limit=self.configured_limit("api_list_capabilities"))["items"]
        ]
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
    ) -> tuple[LlmCallResult, list[dict[str, str]], ContextBudgetPlan, list[dict[str, str]], dict[str, Any]]:
        try:
            result = self.llm_client.complete(
                messages=messages,
                model_profile=model_profile,
                temperature=budget_plan.llm_temperature,
                max_tokens=budget_plan.llm_max_tokens,
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
        node = self.repository.find_one("prompt_nodes", {"prompt_key": prompt_key, "status": "enabled"})
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
    ) -> dict[str, Any]:
        del task
        intent_config = self._dialogue_intent_config()
        template = self._dialogue_card_template(dialogue_intent, intent_config)
        mode_cfg = self._dialogue_mode_config(mode_decision.mode.value)
        capabilities = self.repository.list_records(
            "capabilities",
            filters={"status": "approved"},
            limit=self.configured_limit("api_list_capabilities"),
        )["items"]
        qe_capability_keys = set(self.active_runtime_config().get("planner", {}).get("qe_workflow_capability_keys", []))
        available_keys = {str(item.get("capability_key")) for item in capabilities}
        include_qe_capabilities = bool(template.get("include_qe_capabilities"))
        mcp_route = self._canonicalize_mcp_route(dict(route_request(user_message)))
        route_domain = str(mcp_route.get("domain") or "general")
        is_local_data = dialogue_intent == DialogueIntent.LOCAL_DATA_MANAGEMENT_REQUEST or (
            route_domain in {"local_data", "general"} and self._is_local_data_management_request(user_message)
        )
        local_data_capability_keys = set(self.active_runtime_config().get("planner", {}).get("local_data_workflow_capability_keys", []))
        capability_card_keys: set[str] = set()
        if include_qe_capabilities:
            capability_card_keys.update(qe_capability_keys)
        if is_local_data:
            capability_card_keys.update(local_data_capability_keys)
        route_capability_keys = set()
        if mcp_route.get("domain") and mcp_route.get("domain") != "general":
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
        return cards

    def _maybe_auto_execute_read_only_mcp_route(
        self,
        *,
        user_message: str,
        conversation_id: str,
        task: dict[str, Any],
        context_pack: dict[str, Any],
        cards: dict[str, Any],
        mode_decision: ModeDecision,
    ) -> None:
        route = cards.get("mcp_route_decision")
        if not isinstance(route, dict):
            return
        eligibility = self._read_only_mcp_auto_execution_eligibility(route, mode_decision)
        route["auto_execute"] = eligibility
        if not eligibility["eligible"]:
            return
        server_key = str(route["server_key"])
        tool_name = str(route["tool_name"])
        capability_key = f"{route['domain']}.mcp_orchestration"
        payload = {
            "request": user_message,
            "route": route,
            "mcp_route_decision": route,
            "limit": 20,
        }
        try:
            proposal = self.create_action_proposal(
                ActionProposalCreate(
                    task_id=task["task_id"],
                    conversation_id=conversation_id,
                    capability_key=capability_key,
                    proposal_type="mcp_tool",
                    title=f"Summary-first MCP read: {server_key}/{tool_name}",
                    summary=f"Auto-execute low-risk read-only MCP summary for route {server_key}/{tool_name}.",
                    input_json=payload,
                    expected_result_json={"summary_first": True, "server_key": server_key, "tool_name": tool_name},
                    context_pack_id=context_pack.get("context_pack_id"),
                    idempotency_key=sha256_json({"task_id": task["task_id"], "auto_mcp_read": server_key, "tool_name": tool_name, "payload": payload}),
                    created_by="research_assistant_auto_read_only_route",
                )
            )
            preflight = self.preflight_action_proposal(
                proposal["action_proposal_id"],
                ActionProposalPreflightRequest(payload_json=payload, idempotency_key=proposal["idempotency_key"]),
            )
            if preflight["proposal"]["status"] != "preflight_passed":
                cards["mcp_execution_result"] = {
                    "auto_executed": False,
                    "status": "preflight_blocked",
                    "route": f"{server_key}/{tool_name}",
                    "server_key": server_key,
                    "tool_name": tool_name,
                    "action_proposal_id": proposal["action_proposal_id"],
                    "preflight": preflight["preflight"],
                    "summary_first": True,
                }
                return
            executed = self.execute_action_proposal(
                proposal["action_proposal_id"],
                ActionProposalExecuteRequest(payload_json=payload, idempotency_key=proposal["idempotency_key"]),
            )
        except Exception as exc:  # pragma: no cover - defensive path keeps chat usable when catalog drifts.
            logger.exception("read-only MCP auto execution failed for %s/%s", server_key, tool_name)
            cards["mcp_execution_result"] = {
                "auto_executed": False,
                "status": "failed",
                "route": f"{server_key}/{tool_name}",
                "server_key": server_key,
                "tool_name": tool_name,
                "summary_first": True,
                "error": {"code": "auto_mcp_execution_failed", "message": str(exc)},
            }
            self.add_task_event(
                task["task_id"],
                TaskEventCreate(
                    event_type="mcp_auto_execute_failed",
                    severity="warning",
                    message=f"Read-only MCP auto execution failed: {server_key}/{tool_name}",
                    payload_json={"server_key": server_key, "tool_name": tool_name, "error": str(exc)},
                ),
            )
            return

        tool_event = executed.get("tool_event") if isinstance(executed.get("tool_event"), dict) else {}
        summary_result = tool_event.get("response_json") if isinstance(tool_event.get("response_json"), dict) else {}
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
            "route": f"{server_key}/{tool_name}",
            "server_key": server_key,
            "tool_name": tool_name,
            "action_proposal_id": proposal["action_proposal_id"],
            "proposal_status": (executed.get("proposal") or {}).get("status") if isinstance(executed.get("proposal"), dict) else None,
            "tool_event_id": tool_event.get("tool_event_id"),
            "trace_id": executed.get("trace_id"),
            "summary_first": bool(summary_result.get("summary_first", True)),
            "response_summary": self._compact_mcp_summary_for_cards(summary_result),
            "human_cards": list(executed.get("human_cards") or []),
        }
        cards["status_rail"] = self._mcp_executed_status_rail()


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
    ) -> tuple[LlmCallResult, list[dict[str, str]], Any]:
        route_seed = self._seeded_react_tool_call(cards, mode_decision)
        first_turn_consumed = False

        def model_complete(next_messages: list[dict[str, Any]]) -> ModelTurn:
            nonlocal first_turn_consumed
            if not first_turn_consumed and not route_seed:
                first_turn_consumed = True
                return ModelTurn(
                    content=first_llm_result.content,
                    provider=first_llm_result.provider,
                    model=first_llm_result.model,
                    duration_ms=first_llm_result.duration_ms,
                    usage=first_llm_result.usage,
                )
            first_turn_consumed = True
            result = self.llm_client.complete(
                messages=next_messages,
                model_profile=model_profile,
                temperature=budget_plan.llm_temperature,
                max_tokens=budget_plan.llm_max_tokens,
            )
            return ModelTurn(
                content=result.content,
                provider=result.provider,
                model=result.model,
                duration_ms=result.duration_ms,
                usage=result.usage,
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
            fallback = self._grounded_route_fallback_tool_call(cards, mode_decision)
            return [fallback] if fallback else []

        react_result = run_react_grounding_loop(
            messages=messages,
            model_complete=model_complete,
            mcp_provider=provider,
            catalog_entries=self._react_tool_catalog_entries(),
            config=self._react_grounding_config(runtime_config),
            seeded_tool_calls=[route_seed] if route_seed else None,
            fallback_tool_calls=fallback_tool_calls,
        )
        final_turn = react_result.model_turns[-1] if react_result.model_turns else ModelTurn(
            content=react_result.final_text,
            provider=first_llm_result.provider,
            model=first_llm_result.model,
            duration_ms=first_llm_result.duration_ms,
            usage=first_llm_result.usage,
        )
        grounded_llm_result = LlmCallResult(
            content=react_result.final_text,
            provider=final_turn.provider,
            model=final_turn.model,
            duration_ms=sum(turn.duration_ms for turn in react_result.model_turns) or first_llm_result.duration_ms,
            usage=self._merge_llm_usage([turn.usage for turn in react_result.model_turns]),
        )
        return grounded_llm_result, [dict(item) for item in react_result.messages], react_result

    def _react_grounding_config(self, runtime_config: dict[str, Any]) -> ReactGroundingConfig:
        cfg = runtime_config.get("react_grounding") if isinstance(runtime_config.get("react_grounding"), dict) else {}
        if "max_tool_iterations" not in cfg:
            raise KeyError("Research Assistant runtime config missing react_grounding.max_tool_iterations")
        return ReactGroundingConfig(
            max_tool_iterations=int(cfg["max_tool_iterations"]),
            evidence_required=bool(cfg.get("evidence_required", True)),
            placeholder_patterns=tuple(str(item) for item in cfg.get("placeholder_patterns", [r"\bXX\b", r"\bX%\b", "approxX", "about X"])),
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

    def _react_tool_catalog_entries(self) -> list[ToolCatalogEntry]:
        tools = self._manifest_mcp_catalog_records()
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

    def _seeded_react_tool_call(self, cards: dict[str, Any], mode_decision: ModeDecision) -> McpToolCall | None:
        route = cards.get("mcp_route_decision") if isinstance(cards, dict) else None
        if not isinstance(route, dict) or not route.get("server_key") or not route.get("tool_name"):
            return None
        eligibility = self._read_only_mcp_auto_execution_eligibility(route, mode_decision)
        route["auto_execute"] = eligibility
        if eligibility.get("eligible"):
            return McpToolCall(
                server_key=str(route["server_key"]),
                tool_name=str(route["tool_name"]),
                payload_json={"request": route.get("request") or "", "route": route, "mcp_route_decision": route, "limit": 20},
                stable_call_id=f"route:{route['server_key']}:{route['tool_name']}",
                reason=str(route.get("reason") or "route_seed"),
            )
        return None

    def _grounded_route_fallback_tool_call(self, cards: dict[str, Any], mode_decision: ModeDecision) -> McpToolCall | None:
        route = cards.get("mcp_route_decision") if isinstance(cards, dict) else None
        if not isinstance(route, dict) or not route.get("server_key") or not route.get("tool_name"):
            return None
        if str(route.get("side_effect") or "read_only") != "read_only":
            return None
        eligibility = route.get("auto_execute") if isinstance(route.get("auto_execute"), dict) else self._read_only_mcp_auto_execution_eligibility(route, mode_decision)
        route["auto_execute"] = eligibility
        if not eligibility.get("eligible"):
            return None
        return McpToolCall(
            server_key=str(route["server_key"]),
            tool_name=str(route["tool_name"]),
            payload_json={"request": route.get("request") or "", "route": route, "mcp_route_decision": route, "limit": 20},
            stable_call_id=f"fallback:{route['server_key']}:{route['tool_name']}",
            reason="evidence_guard_route_fallback",
        )

    @staticmethod
    def _react_grounding_card(react_result: Any) -> dict[str, Any]:
        return {
            "schema_version": "research_assistant_react_grounding_v1",
            "iterations": react_result.iterations,
            "tool_call_count": len(react_result.tool_calls),
            "tool_result_count": len(react_result.tool_results),
            "stopped_reason": react_result.stopped_reason,
            "evidence_guard": {
                "allowed": react_result.evidence_guard.allowed,
                "reason": react_result.evidence_guard.reason,
                "source_count": react_result.evidence_guard.source_count,
                "as_of_count": react_result.evidence_guard.as_of_count,
            },
        }

    @staticmethod
    def _mcp_result_source_refs(summary_result: dict[str, Any], tool_event: dict[str, Any]) -> list[str]:
        source = summary_result.get("source") or tool_event.get("transport") or tool_event.get("tool_event_id") or "mcp_tool_event"
        refs = [str(source)] if source else []
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
        candidate_keys: list[str] = []
        for item in self._workflow_capabilities():
            candidate = self._canonicalize_capability_mcp_refs(dict(item))
            key = str(candidate.get("capability_key") or "")
            if capability_key and key != capability_key:
                continue
            if key and self._capability_has_tool_ref(candidate, server_key, tool_name):
                candidate_keys.append(key)
        if not candidate_keys:
            return None
        try:
            self.sync_capabilities({"apply": True, "requested_by": "capability_tool_lookup_self_heal"})
        except Exception:  # noqa: BLE001
            logger.exception("failed to refresh capability cache for %s/%s", server_key, tool_name)
            return None
        for key in candidate_keys:
            capability = self.repository.find_one("capabilities", {"capability_key": key, "status": "approved"})
            if capability and self._capability_has_tool_ref(capability, server_key, tool_name):
                return capability
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
            capability = self.repository.find_one("capabilities", {"capability_key": candidate, "status": "approved"})
            if capability and self._capability_allows_tool(capability, call):
                return candidate
            capability = self._refresh_capability_cache_for_tool(
                server_key=call.server_key,
                tool_name=call.tool_name,
                capability_key=candidate,
            )
            if capability and self._capability_allows_tool(capability, call):
                return candidate
        capabilities = self.repository.list_records("capabilities", filters={"status": "approved"}, limit=self.configured_limit("api_list_capabilities"))["items"]
        for capability in capabilities:
            if self._capability_allows_tool(capability, call):
                return str(capability["capability_key"])
        capability = self._refresh_capability_cache_for_tool(server_key=call.server_key, tool_name=call.tool_name)
        if capability:
            return str(capability["capability_key"])
        raise KeyError(f"approved capability not found for tool: {call.server_key}/{call.tool_name}")

    @staticmethod
    def _capability_allows_tool(capability: dict[str, Any], call: McpToolCall) -> bool:
        return ResearchAssistantService._capability_has_tool_ref(capability, call.server_key, call.tool_name)

    def _populate_cards_from_tool_execution(self, cards: dict[str, Any], proposal: dict[str, Any], executed: dict[str, Any], result: McpToolResult) -> None:
        tool_event = executed.get("tool_event") if isinstance(executed.get("tool_event"), dict) else {}
        summary_result = tool_event.get("response_json") if isinstance(tool_event.get("response_json"), dict) else {}
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
        cards["status_rail"] = self._mcp_executed_status_rail()

    def _read_only_mcp_auto_execution_eligibility(self, route: dict[str, Any], mode_decision: ModeDecision) -> dict[str, Any]:
        if not route.get("server_key") or not route.get("tool_name") or not route.get("domain"):
            return {"eligible": False, "reason": "route_missing_tool"}
        if route.get("domain") in {"general"}:
            return {"eligible": False, "reason": "general_route"}
        if str(route.get("side_effect") or "read_only") != "read_only":
            return {"eligible": False, "reason": "route_not_read_only"}
        if mode_decision.allowed_tool_side_effect == "none":
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
        capability = self.repository.find_one("capabilities", {"capability_key": capability_key, "status": "approved"})
        if not capability:
            return {"eligible": False, "reason": "capability_not_approved", "capability_key": capability_key}
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
        execution = cards.get("mcp_execution_result") if isinstance(cards, dict) else None
        if isinstance(execution, dict) and execution.get("auto_executed"):
            summary = cards.get("mcp_summary_result") if isinstance(cards.get("mcp_summary_result"), dict) else {}
            if self._should_render_auto_mcp_execution_reply(execution, summary, react_active=react_active):
                return self._apply_main_reply_policy(self._render_mcp_execution_reply(execution, summary), mode_decision)
        react_card = cards.get("react_grounding") if isinstance(cards.get("react_grounding"), dict) else {}
        if (
            react_active
            and isinstance(execution, dict)
            and execution.get("auto_executed")
            and react_card.get("stopped_reason") == "evidence_summary_fallback"
        ):
            summary = cards.get("mcp_summary_result") if isinstance(cards.get("mcp_summary_result"), dict) else {}
            return self._apply_main_reply_policy(self._render_react_execution_fallback_reply(execution, summary), mode_decision)
        if mode_decision.intent_type in {DialogueIntent.CAPABILITY_INQUIRY, DialogueIntent.MCP_CAPABILITY_INQUIRY} and (self._is_mcp_tool_catalog_inquiry(user_message) or "mcp" in user_message.lower() or "tool" in user_message.lower()):
            catalog = cards.get("runtime_mcp_catalog") if isinstance(cards, dict) else None
            if isinstance(catalog, dict):
                return self._apply_main_reply_policy(self._render_mcp_tool_catalog_reply(catalog), mode_decision)
        route = cards.get("mcp_route_decision") if isinstance(cards, dict) else None
        if (not react_active or "<assistant_tool_choice" in raw_text.lower()) and self._should_render_mcp_route_reply(route, mode_decision, raw_text):
            return self._apply_main_reply_policy(self._render_mcp_route_reply(route), mode_decision)
        if text:
            return self._apply_main_reply_policy(text, mode_decision)
        intent_config = self._dialogue_intent_config()
        return self._apply_main_reply_policy(str(intent_config.get("fallback_reply") or user_message), mode_decision)

    @staticmethod
    def _should_render_auto_mcp_execution_reply(execution: dict[str, Any], summary: dict[str, Any], *, react_active: bool) -> bool:
        del execution
        if summary.get("local_data_daily_status") or summary.get("response_mode") == "local_data_daily_sync_status":
            return True
        return not react_active

    @staticmethod
    def _render_mcp_execution_reply(execution: dict[str, Any], summary: dict[str, Any]) -> str:
        if summary.get("local_data_daily_status") or summary.get("response_mode") == "local_data_daily_sync_status":
            return ResearchAssistantService._render_local_data_daily_status_reply(execution, summary)
        route = str(execution.get("route") or "unknown/unknown")
        items = summary.get("items") if isinstance(summary.get("items"), list) else []
        lines = [
            "已通过只读工具完成 MCP summary-first 查询；我只展示概要，不展开原始行、矩阵、日志或模型权重。",
            f"Route decision：{route}。",
            f"结果概要：total={summary.get('total', len(items))}，returned={len(items)}，limit={summary.get('limit')}，offset={summary.get('offset')}。",
        ]
        for item in items[:3]:
            if not isinstance(item, dict):
                continue
            title = item.get("title") or item.get("tool_name") or item.get("item_type") or item.get("server_key") or "item"
            detail = []
            for key in ("server_key", "tool_name", "risk_level", "side_effect_level", "status"):
                if item.get(key) is not None:
                    detail.append(f"{key}={item.get(key)}")
            lines.append(f"- {title}: {', '.join(detail[:5])}")
        if summary.get("detail_tool"):
            lines.append(f"需要单项详情时再调用 detail tool：{summary.get('detail_tool')}。")
        artifact_refs = summary.get("artifact_refs") if isinstance(summary.get("artifact_refs"), list) else []
        omitted = summary.get("omitted_sections") if isinstance(summary.get("omitted_sections"), list) else []
        if artifact_refs:
            lines.append(f"大对象已收敛为 artifact_ref：{len(artifact_refs)} 个。")
        if omitted:
            lines.append(f"已按 payload budget 省略：{', '.join(str(item) for item in omitted[:6])}。")
        return "\n".join(lines)

    @staticmethod
    def _render_local_data_daily_status_reply(execution: dict[str, Any], summary: dict[str, Any]) -> str:
        del execution
        groups = summary.get("status_groups") if isinstance(summary.get("status_groups"), dict) else {}
        counts = summary.get("group_counts") if isinstance(summary.get("group_counts"), dict) else {}
        evidence_sources = summary.get("evidence_sources") if isinstance(summary.get("evidence_sources"), list) else []
        trade_date = str(summary.get("trade_date") or utc_now().date().isoformat())
        as_of = str(summary.get("as_of") or utc_now().isoformat())
        labels = {
            "success": "已同步成功",
            "failed": "同步失败",
            "not_synced": "未同步/未运行",
            "running": "运行中/排队中",
            "blocked": "告警/阻断/需要处理",
        }
        order = ("success", "failed", "not_synced", "running", "blocked")
        lines = [
            "已按本地数据只读证据汇总今天的数据同步情况。",
            f"查询日期：{trade_date}；as_of={as_of}。",
            f"证据来源：{', '.join(str(item) for item in evidence_sources) if evidence_sources else 'local-data facade'}。",
        ]
        if not any(int(counts.get(key) or 0) for key in order):
            lines.append("今天暂无可用的 preset/job/target 同步记录；这通常表示尚未运行，或本地数据 facade 没有返回当天记录。")
        for key in order:
            items = groups.get(key) if isinstance(groups.get(key), list) else []
            lines.append(f"{labels[key]}（{len(items)}）")
            if not items:
                lines.append("- 无")
                continue
            for item in items[:20]:
                if not isinstance(item, dict):
                    continue
                dataset = str(item.get("dataset") or "unknown")
                status = str(item.get("status") or "unknown")
                created_at = str(item.get("created_at") or "")
                finished_at = str(item.get("finished_at") or "")
                last_error = str(item.get("last_error") or "")
                time_bits = []
                if created_at:
                    time_bits.append(f"开始={created_at}")
                if finished_at:
                    time_bits.append(f"结束={finished_at}")
                suffix = f"；{'；'.join(time_bits)}" if time_bits else ""
                if last_error:
                    suffix += f"；错误={last_error}"
                lines.append(f"- {dataset}：{status}{suffix}")
            if len(items) > 20:
                lines.append(f"- 另有 {len(items) - 20} 项已省略，可继续要求展开指定分组。")
        partial_errors = summary.get("partial_errors") if isinstance(summary.get("partial_errors"), list) else []
        if partial_errors:
            rendered_errors = "; ".join(
                f"{item.get('source')}: {item.get('error')}" for item in partial_errors[:3] if isinstance(item, dict)
            )
            lines.append(f"部分只读证据读取失败：{rendered_errors}。")
        lines.append("本轮未执行任何同步、修复、刷新或写库操作。")
        return "\n".join(lines)

    @staticmethod
    def _render_react_execution_fallback_reply(execution: dict[str, Any], summary: dict[str, Any]) -> str:
        reply = ResearchAssistantService._render_mcp_execution_reply(execution, summary)
        source_refs = execution.get("source_refs") if isinstance(execution.get("source_refs"), list) else []
        source = str(source_refs[0] if source_refs else summary.get("source") or execution.get("tool_event_id") or "mcp_tool_event")
        as_of = str(execution.get("as_of") or utc_now().date().isoformat())
        return f"{reply}\nEvidence: source={source} as_of={as_of}."

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
            DialogueIntent.QE_WAREHOUSE_REQUEST,
            DialogueIntent.RESEARCH_PIPELINE_REQUEST,
        }
        return mode_decision.intent_type in guarded_intents

    @staticmethod
    def _render_mcp_route_reply(route: Any) -> str:
        if not isinstance(route, dict):
            return "我会先做 MCP route decision 和 preflight，不会用模型猜业务数据。"
        server_key = str(route.get("server_key") or "unknown-server")
        tool_name = str(route.get("tool_name") or "unknown-tool")
        domain = str(route.get("domain") or "unknown")
        side_effect = str(route.get("side_effect") or "read_only")
        lines = [
            "我先把这类业务问题交给 MCP 路由处理，不会用模型猜因子数量、Issue 状态或其他业务事实。",
            f"Route decision：{domain} -> {server_key}/{tool_name}。",
            "返回边界：summary-first；列表/概览只返回数量、分页和关键字段，具体对象详情再用 get/detail 工具展开，矩阵、日志和原始 payload 只给 artifact_ref 或 detail 引用。",
        ]
        if side_effect == "read_only":
            lines.append("当前选择的是只读工具，可以继续做真实查询；我会只展示 MCP 返回的概要数据。")
        elif side_effect == "plan_or_preflight":
            lines.append("当前选择的是计划/预检查工具，只生成方案和校验结果，不会执行写入或长任务。")
        else:
            lines.append("当前选择涉及 confirmed action；必须先展示 preflight、确认口令和审批边界，确认前不会执行。")
        reason = str(route.get("reason") or "").strip()
        if reason:
            lines.append(f"选择依据：{reason}")
        read_tools = route.get("read_tools") if isinstance(route.get("read_tools"), list) else []
        if read_tools:
            preview = ", ".join(str(item) for item in read_tools[:4])
            lines.append(f"可先使用的只读工具：{preview}。")
        return "\n".join(lines)

    @staticmethod
    def _render_mcp_tool_catalog_reply(catalog: dict[str, Any]) -> str:
        lines = [
            "可以，我会按你的业务目标来选择 MCP：本地数据、QE 实验、QE 数仓、Issue/验证、Research Pipeline、因子、模型、策略和执行策略都在同一套路由里。",
            f"当前目录里有 {catalog.get('server_count', 0)} 个 MCP server、{catalog.get('tool_count', 0)} 个 MCP tool、{catalog.get('capability_count', 0)} 个已批准能力。",
            "默认采用 summary-first：先看概要、状态、top risks 和分页列表；需要某个对象详情时再展开，矩阵、日志、parquet/raw payload 只返回 artifact_ref 或 detail 引用。",
        ]
        tools_by_server = catalog.get("tools_by_server") if isinstance(catalog.get("tools_by_server"), dict) else {}
        servers_by_key = catalog.get("servers_by_key") if isinstance(catalog.get("servers_by_key"), dict) else {}
        if tools_by_server:
            lines.append("已登记 MCP 工具概览：")
            for server_key in sorted(str(key) for key in tools_by_server):
                tools = tools_by_server.get(server_key) or []
                server = servers_by_key.get(server_key) if isinstance(servers_by_key.get(server_key), dict) else {}
                health = server.get("health_json") if isinstance(server.get("health_json"), dict) else {}
                display_name = str(health.get("display_name_zh") or server.get("title") or server_key)
                aliases = health.get("business_aliases_zh") if isinstance(health.get("business_aliases_zh"), list) else []
                alias_text = f"（{', '.join(str(item) for item in aliases[:3])}）" if aliases else ""
                sample = [str(tool.get("tool_name") or "") for tool in tools if isinstance(tool, dict)]
                important = [
                    name
                    for name in ("assistant_create_issue_candidate", "qe_template_create", "mcp_github_issue_create")
                    if name in sample
                ]
                preview_names = []
                for name in [*important, *sample]:
                    if name and name not in preview_names:
                        preview_names.append(name)
                    if len(preview_names) >= 12:
                        break
                preview = ", ".join(preview_names)
                suffix = f" ... count {len(sample)}" if len(sample) > 12 else f"(count {len(sample)})"
                lines.append(f"- {display_name}{alias_text} / {server_key}: {preview}{suffix}")
        else:
            lines.append("我会先刷新 MCP 目录，再给你可调用的工具概览。")
        lines.append("你直接描述目标即可，例如补 trade_date、检查 QE 入仓、计算因子 RankIC、比较模型 seed 稳定性、判断策略能否进入 Paper v2；我会给出 route decision、工具和确认边界。")
        return "\n".join(lines)



    def _apply_main_reply_policy(self, text: str, mode_decision: ModeDecision) -> str:
        if mode_decision.mode not in {DialogueMode.DIALOGUE, DialogueMode.ANALYSIS}:
            return text
        mode_cfg = self._dialogue_mode_config(mode_decision.mode.value)
        forbidden = [str(item) for item in mode_cfg.get("forbidden_main_reply_phrases", []) if str(item)]
        if not forbidden:
            return text
        kept_lines: list[str] = []
        for line in text.splitlines():
            stripped = line.strip()
            if any(stripped.startswith(phrase) or phrase in stripped for phrase in forbidden):
                continue
            kept_lines.append(line)
        cleaned = "\n".join(kept_lines).strip()
        return cleaned or text

    def add_task_event(self, task_id: str, request: TaskEventCreate | dict[str, Any]) -> dict[str, Any]:
        if not self.repository.get_record("tasks", task_id):
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
            self.repository.update_record("tasks", task_id, updates)
        return event

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
            hops=int(graph_context_config.get("hops") or 1),
            relation_filter=graph_context_config.get("relation_filter"),
            limit=int(graph_context_config.get("limit") or self.configured_limit("graph_summary_relations")),
        )
        code_intelligence_context = build_code_intelligence_context(repo_root=REPO_ROOT)
        code_intelligence_refs = artifact_ref_paths(code_intelligence_context)
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
                "relation_refs": graph_result.relation_refs,
                "seed_entity_keys": graph_result.seed_entity_keys,
                "neighbor_entity_keys": graph_result.neighbor_entity_keys,
                "omitted_relation_refs": graph_result.omitted_relation_refs,
            },
            "code_intelligence_context": code_intelligence_context,
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
            "external_source_refs": code_intelligence_refs,
            "temp_memory_refs": temp_refs,
            "omitted_relevant_refs": memory_result.omitted_refs,
            "pack_summary": (
                f"Context Pack: {len(memory_items)} tree-selected memories, "
                f"{len(graph_result.graph_relation_refs)} graph relations, {len(temp_refs)} temp memories, "
                f"code-intelligence {code_intelligence_context.get('data_state') or 'unknown'}"
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
            self.add_task_event(data.task_id, TaskEventCreate(event_type="context_pack_built", message="Context Pack built", payload_json={"context_pack_id": row["context_pack_id"]}))
        return context_pack

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
        return sorted(keys)

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
    ) -> None:
        def run_curator() -> CuratorResult:
            result = MemoryCurator(self.repository).curate_turn(
                user_message=user_message,
                assistant_message=assistant_message,
                conversation_id=conversation_id,
                user_message_id=user_message_id,
                assistant_message_id=assistant_message_id,
                task_id=task_id,
            )
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
        dedupe_key = data.dedupe_key or sha256_json({"title": data.title, "module": data.module, "reproduce_command": data.reproduce_command})
        existing = self.repository.find_one("issue_candidates", {"dedupe_key": dedupe_key})
        if existing:
            existing["deduplicated"] = True
            existing["duplicate_candidate_requested"] = True
            return existing
        if data.approval_id or data.confirmation_text:
            self._consume_approval_gate(
                approval_id=data.approval_id,
                confirmation_text=data.confirmation_text,
                approval_type="issue.candidate",
                required_summary_fragment=data.title,
            )
        created = self.repository.create_record(
            "issue_candidates",
            {
                "candidate_id": new_id("issuecand"),
                "status": "needs_review",
                "dedupe_key": dedupe_key,
                "github_sync_status": "not_requested",
                "github_sync_json": {"formal_github_issue_requires_approval": True},
                **data.model_dump(exclude={"dedupe_key", "approval_id", "confirmation_text"}),
            },
        )
        created.setdefault("github_issue_number", None)
        created.setdefault("github_issue_url", None)
        return created


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
        candidate = self.repository.get_record("issue_candidates", candidate_id)
        if not candidate:
            raise KeyError(f"issue candidate not found: {candidate_id}")
        gate = {
            "mode": data.mode,
            "formal_github_issue_requires_approval": True,
            "direct_github_create_performed": False,
            "approval_id": data.approval_id,
            "requested_by": data.requested_by,
        }
        if data.mode == "dry_run":
            status = "dry_run"
            gate.update({"would_create_github_issue": True, "blocked_reason": None})
        else:
            if not data.approval_id or not data.confirmation_text:
                status = "approval_required"
                gate.update({"blocked_reason": "formal sync requires approval_id and confirmation_text"})
            else:
                self._consume_approval_gate(
                    approval_id=data.approval_id,
                    confirmation_text=data.confirmation_text,
                    approval_type="issue.github_sync",
                    required_summary_fragment=candidate["title"],
                )
                status = "blocked"
                gate.update({"blocked_reason": "Phase 1 records the approval gate only; direct GitHub creation is disabled"})
        updated = self.repository.update_record("issue_candidates", candidate_id, {"github_sync_status": status, "github_sync_json": gate})
        updated.setdefault("github_issue_number", None)
        updated.setdefault("github_issue_url", None)
        return updated

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
        reports = self.repository.list_records("validation_discovery_reports", limit=self.configured_limit("validation_reports"))
        candidates = self.repository.list_records("issue_candidates", filters={"status": "needs_review"}, limit=self.configured_limit("validation_issue_candidates"))
        return {"latest_reports": reports["items"], "candidate_issues_needing_review": candidates["items"], "generated_at": utc_now().isoformat()}

    def _ensure_default_reports_and_notifications(self, seeded: dict[str, int]) -> None:
        if not self.repository.list_records("reports", limit=1)["items"]:
            self.repository.create_record(
                "reports",
                {
                    "report_id": "report_research_assistant_phase1_morning",
                    "report_type": "morning",
                    "title": "研究助理晨报模板",
                    "body_md": "阶段一提供真实报告数据结构，夜间自动任务将在后续阶段写入具体晨报。",
                    "summary_json": {"phase": "phase1", "source": "seed"},
                    "evidence_refs": [],
                    "status": "draft",
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
        if not self.repository.list_records("validation_discovery_reports", limit=1)["items"]:
            self.repository.create_record(
                "validation_discovery_reports",
                {
                    "discovery_report_id": "vdr_research_assistant_phase1_seed",
                    "run_date": date.today().isoformat(),
                    "title": "阶段一流水线发现流种子记录",
                    "status": "draft",
                    "summary_json": {"llm_discovery": "not_started", "issue_gate": "candidate_only"},
                    "candidate_issue_refs": [],
                    "validation_run_refs": [],
                    "evidence_refs": [],
                },
            )
            seeded["validation_discovery_reports"] = seeded.get("validation_discovery_reports", 0) + 1
