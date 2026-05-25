"""Research Assistant Console APIs."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from backend.services.research_assistant.models import (
    ApprovalCreate,
    ChatTurnRequest,
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
    SkillUsageCreate,
    TaskCreate,
    TaskEventCreate,
    TraceEventCreate,
    WorkbenchDryRunExecuteRequest,
)
from backend.services.research_assistant.repository import ResearchAssistantSchemaMissingError
from backend.services.research_assistant.service import ResearchAssistantCatalogNotReadyError, ResearchAssistantService

router = APIRouter(prefix="/research-assistant", tags=["research-assistant"])


class ResearchAssistantResponse(BaseModel):
    status: str = "success"
    data: Any


class MemoryStatusRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str
    approved_by: str | None = None
    approval_id: str | None = None
    confirmation_text: str | None = None


class ApprovalDecisionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    confirmation_text: str = ""
    decided_by: str = "user"


class TempMemoryCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: str | None = None
    stream_id: str | None = None
    memory_type: str = "task_state"
    content_json: dict[str, Any] = Field(default_factory=dict)
    content_text: str = ""
    evidence_refs: list[str] = Field(default_factory=list)
    expires_at: str | None = None
    created_by_model_profile_id: str | None = None
    approval_id: str | None = None
    confirmation_text: str | None = None


def get_research_assistant_service() -> ResearchAssistantService:
    return ResearchAssistantService()


def _success(data: Any) -> ResearchAssistantResponse:
    return ResearchAssistantResponse(data=data)


def _map_error(exc: Exception) -> HTTPException:
    if isinstance(exc, KeyError):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, ResearchAssistantSchemaMissingError):
        return HTTPException(status_code=503, detail=str(exc))
    if isinstance(exc, ResearchAssistantCatalogNotReadyError):
        return HTTPException(
            status_code=409,
            detail={
                "code": "research_assistant_catalog_not_ready",
                "message": "研究助理目录尚未初始化完整，请先初始化 Prompt Tree、MCP、Skill 与模型路由目录。",
                "operator_action": exc.readiness.get("operator_action"),
                "readiness": exc.readiness,
            },
        )
    if isinstance(exc, (ValueError, ValidationError)):
        return HTTPException(status_code=400, detail=str(exc))
    return HTTPException(status_code=500, detail=str(exc))


@router.get("/health", response_model=ResearchAssistantResponse)
def health(service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.health())
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/overview", response_model=ResearchAssistantResponse)
def overview(service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.overview())
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/catalogs/seed", response_model=ResearchAssistantResponse)
def seed_catalogs(service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.seed_catalogs())
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/catalogs/readiness", response_model=ResearchAssistantResponse)
def catalog_readiness(service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.catalog_readiness())
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/tasks", response_model=ResearchAssistantResponse)
def create_task(request: TaskCreate, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.create_task(request))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/tasks", response_model=ResearchAssistantResponse)
def list_tasks(
    status: str | None = Query(None),
    task_type: str | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("tasks", filters={"status": status, "task_type": task_type}, search=search, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/tasks/{task_id}", response_model=ResearchAssistantResponse)
def get_task(task_id: str, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.get_task(task_id))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/tasks/{task_id}/events", response_model=ResearchAssistantResponse)
def add_task_event(task_id: str, request: TaskEventCreate, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.add_task_event(task_id, request))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/chat/turn", response_model=ResearchAssistantResponse)
def chat_turn(request: ChatTurnRequest, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.chat_turn(request))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/conversations/{conversation_id}", response_model=ResearchAssistantResponse)
def get_conversation(conversation_id: str, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.get_conversation(conversation_id))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/conversations/{conversation_id}/messages", response_model=ResearchAssistantResponse)
def list_conversation_messages(
    conversation_id: str,
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("conversation_messages", filters={"conversation_id": conversation_id}, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/prompt-nodes", response_model=ResearchAssistantResponse)
def list_prompt_nodes(
    phase: str | None = Query(None),
    category: str | None = Query(None),
    status: str | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("prompt_nodes", filters={"phase": phase, "category": category, "status": status}, search=search, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/prompt-activations", response_model=ResearchAssistantResponse)
def list_prompt_activations(
    environment: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("prompt_activations", filters={"environment": environment, "status": status}, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/runtime-config/activations", response_model=ResearchAssistantResponse)
def list_runtime_config_activations(
    environment: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("runtime_config_activations", filters={"environment": environment, "status": status}, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/context-segments", response_model=ResearchAssistantResponse)
def list_context_segments(
    conversation_id: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("context_segments", filters={"conversation_id": conversation_id, "status": status}, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/context-key-facts", response_model=ResearchAssistantResponse)
def list_context_key_facts(
    conversation_id: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("context_key_facts", filters={"conversation_id": conversation_id, "status": status}, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/context-assembly-traces", response_model=ResearchAssistantResponse)
def list_context_assembly_traces(
    conversation_id: str | None = Query(None),
    task_id: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("context_assembly_traces", filters={"conversation_id": conversation_id, "task_id": task_id, "status": status}, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/prompt-bundles", response_model=ResearchAssistantResponse)
def build_prompt_bundle(request: PromptBundleBuildRequest, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.build_prompt_bundle(request))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/prompt-bundles", response_model=ResearchAssistantResponse)
def list_prompt_bundles(
    task_id: str | None = Query(None),
    conversation_id: str | None = Query(None),
    phase: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("prompt_bundles", filters={"task_id": task_id, "conversation_id": conversation_id, "phase": phase}, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc




@router.get("/tasks/{task_id}/events", response_model=ResearchAssistantResponse)
def list_task_events(
    task_id: str,
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("task_events", filters={"task_id": task_id}, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc

@router.post("/memories", response_model=ResearchAssistantResponse)
def create_memory(request: MemoryCreate, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.create_memory(request))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/memories", response_model=ResearchAssistantResponse)
def list_memories(
    namespace: str | None = Query(None),
    memory_type: str | None = Query(None),
    approval_status: str | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("memory_items", filters={"namespace": namespace, "memory_type": memory_type, "approval_status": approval_status}, search=search, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/memories/{memory_id}/status", response_model=ResearchAssistantResponse)
def update_memory_status(memory_id: str, request: MemoryStatusRequest, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.update_memory_status(memory_id, request.status, approved_by=request.approved_by, approval_id=request.approval_id, confirmation_text=request.confirmation_text))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/context-packs", response_model=ResearchAssistantResponse)
def build_context_pack(request: ContextPackBuildRequest, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.build_context_pack(request))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/context-packs", response_model=ResearchAssistantResponse)
def list_context_packs(
    task_id: str | None = Query(None),
    agent_id: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("context_packs", filters={"task_id": task_id, "agent_id": agent_id}, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/graph/summary", response_model=ResearchAssistantResponse)
def graph_summary(namespace: str = Query("aistock"), service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.graph_summary(namespace=namespace))
    except Exception as exc:
        raise _map_error(exc) from exc




@router.post("/graph/entities", response_model=ResearchAssistantResponse)
def create_graph_entity(request: GraphEntityCreate, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.create_graph_entity(request))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/graph/entities", response_model=ResearchAssistantResponse)
def list_graph_entities(
    namespace: str | None = Query(None),
    entity_type: str | None = Query(None),
    approval_status: str | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("entities", filters={"namespace": namespace, "entity_type": entity_type, "approval_status": approval_status}, search=search, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/graph/entities/{entity_id}", response_model=ResearchAssistantResponse)
def get_graph_entity(entity_id: str, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.get_graph_entity(entity_id))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/graph/relations", response_model=ResearchAssistantResponse)
def create_graph_relation(request: GraphRelationCreate, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.create_graph_relation(request))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/graph/relations", response_model=ResearchAssistantResponse)
def list_graph_relations(
    source_entity_id: str | None = Query(None),
    target_entity_id: str | None = Query(None),
    relation_type: str | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("relations", filters={"source_entity_id": source_entity_id, "target_entity_id": target_entity_id, "relation_type": relation_type}, search=search, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/graph/relations/{relation_id}", response_model=ResearchAssistantResponse)
def get_graph_relation(relation_id: str, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.get_graph_relation(relation_id))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/graph/evolution-paths", response_model=ResearchAssistantResponse)
def create_evolution_path(request: EvolutionPathCreate, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.create_evolution_path(request))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/graph/evolution-paths", response_model=ResearchAssistantResponse)
def list_evolution_paths(
    stream_id: str | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("evolution_paths", filters={"stream_id": stream_id}, search=search, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/graph/evolution-paths/{path_id}", response_model=ResearchAssistantResponse)
def get_evolution_path(path_id: str, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.get_evolution_path(path_id))
    except Exception as exc:
        raise _map_error(exc) from exc

@router.get("/skills", response_model=ResearchAssistantResponse)
def list_skills(
    status: str | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("skills", filters={"status": status}, search=search, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc




@router.post("/skills/{skill_key}/enable", response_model=ResearchAssistantResponse)
def enable_skill(skill_key: str, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.set_skill_enabled(skill_key, enabled=True))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/skills/{skill_key}/disable", response_model=ResearchAssistantResponse)
def disable_skill(skill_key: str, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.set_skill_enabled(skill_key, enabled=False))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/skills/usage-events", response_model=ResearchAssistantResponse)
def create_skill_usage_event(request: SkillUsageCreate, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.create_skill_usage_event(request))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/skills/usage-events", response_model=ResearchAssistantResponse)
def list_skill_usage_events(
    skill_key: str | None = Query(None),
    task_id: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("skill_events", filters={"skill_key": skill_key, "task_id": task_id, "status": status}, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc

@router.get("/mcp/servers", response_model=ResearchAssistantResponse)
def list_mcp_servers(service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("mcp_servers", limit=service.configured_limit("router_mcp_servers")))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/mcp/tools", response_model=ResearchAssistantResponse)
def list_mcp_tools(
    server_key: str | None = Query(None),
    risk_level: str | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("mcp_tools", filters={"server_key": server_key, "risk_level": risk_level}, search=search, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/mcp/preflight", response_model=ResearchAssistantResponse)
def preflight_mcp_tool(request: McpPreflightRequest, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.preflight_mcp_tool(request))
    except Exception as exc:
        raise _map_error(exc) from exc




@router.post("/workbench/dry-run-execute", response_model=ResearchAssistantResponse)
def dry_run_execute_tool(request: WorkbenchDryRunExecuteRequest, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.dry_run_execute_tool(request))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/mcp/tool-events", response_model=ResearchAssistantResponse)
def list_mcp_tool_events(
    task_id: str | None = Query(None),
    server_key: str | None = Query(None),
    tool_name: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("mcp_tool_events", filters={"task_id": task_id, "server_key": server_key, "tool_name": tool_name, "status": status}, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc

@router.post("/approvals", response_model=ResearchAssistantResponse)
def create_approval(request: ApprovalCreate, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.create_approval(request))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/approvals", response_model=ResearchAssistantResponse)
def list_approvals(
    status: str | None = Query(None),
    risk_level: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("approvals", filters={"status": status, "risk_level": risk_level}, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/approvals/{approval_id}/approve", response_model=ResearchAssistantResponse)
def approve(approval_id: str, request: ApprovalDecisionRequest, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.decide_approval(approval_id, action="approve", confirmation_text=request.confirmation_text, decided_by=request.decided_by))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/approvals/{approval_id}/reject", response_model=ResearchAssistantResponse)
def reject(approval_id: str, request: ApprovalDecisionRequest, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.decide_approval(approval_id, action="reject", confirmation_text=request.confirmation_text, decided_by=request.decided_by))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/issue-candidates", response_model=ResearchAssistantResponse)
def create_issue_candidate(request: IssueCandidateCreate, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.create_issue_candidate(request))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/issue-candidates", response_model=ResearchAssistantResponse)
def list_issue_candidates(
    status: str | None = Query(None),
    module: str | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("issue_candidates", filters={"status": status, "module": module}, search=search, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/issue-candidates/{candidate_id}/github-sync", response_model=ResearchAssistantResponse)
def github_sync_issue_candidate(candidate_id: str, request: IssueCandidateGithubSyncRequest, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.github_sync_issue_candidate(candidate_id, request))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/external-agent/sessions", response_model=ResearchAssistantResponse)
def create_external_agent_session(request: ExternalAgentSessionCreate, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.create_external_agent_session(request))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/external-agent/sessions", response_model=ResearchAssistantResponse)
def list_external_agent_sessions(
    agent_type: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("external_sessions", filters={"agent_type": agent_type, "status": status}, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/external-agent/events", response_model=ResearchAssistantResponse)
def create_external_agent_event(request: ExternalAgentEventCreate, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.create_external_agent_event(request))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/external-agent/events", response_model=ResearchAssistantResponse)
def list_external_agent_events(
    session_id: str | None = Query(None),
    event_type: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("external_events", filters={"session_id": session_id, "event_type": event_type}, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/trace-events", response_model=ResearchAssistantResponse)
def create_trace_event(request: TraceEventCreate, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.create_trace_event(request))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/trace-events", response_model=ResearchAssistantResponse)
def list_trace_events(
    task_id: str | None = Query(None),
    component: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("trace_events", filters={"task_id": task_id, "component": component, "status": status}, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/models/profiles", response_model=ResearchAssistantResponse)
def list_model_profiles(service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("model_profiles", limit=service.configured_limit("router_model_profiles")))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/models/routing-policies", response_model=ResearchAssistantResponse)
def list_model_routing_policies(service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("routing_policies", limit=service.configured_limit("router_routing_policies")))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/models/route", response_model=ResearchAssistantResponse)
def route_model(request: ModelRouteRequest, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.route_model(request))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.post("/temp-memories", response_model=ResearchAssistantResponse)
def create_temp_memory(request: TempMemoryCreateRequest, service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.create_temp_memory(request.model_dump()))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/notifications/summary", response_model=ResearchAssistantResponse)
def notification_summary(user_id: str = Query("default"), service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.notification_summary(user_id=user_id))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/notifications", response_model=ResearchAssistantResponse)
def list_notifications(
    user_id: str = Query("default"),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("notifications", filters={"user_id": user_id, "status": status}, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/reports", response_model=ResearchAssistantResponse)
def list_reports(
    report_type: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("reports", filters={"report_type": report_type, "status": status}, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/agenda", response_model=ResearchAssistantResponse)
def list_agenda(
    namespace: str = Query("aistock"),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ResearchAssistantService = Depends(get_research_assistant_service),
) -> ResearchAssistantResponse:
    try:
        return _success(service.list_records("agenda_items", filters={"namespace": namespace, "status": status}, limit=limit, offset=offset))
    except Exception as exc:
        raise _map_error(exc) from exc


@router.get("/validation-discovery/summary", response_model=ResearchAssistantResponse)
def validation_discovery_summary(service: ResearchAssistantService = Depends(get_research_assistant_service)) -> ResearchAssistantResponse:
    try:
        return _success(service.validation_discovery_summary())
    except Exception as exc:
        raise _map_error(exc) from exc
