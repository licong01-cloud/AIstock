"""Business service for the Research Assistant Console.

This service keeps Phase 1 state explicit and replayable. It does not execute
long-running experiments, does not write formal GitHub issues, and does not
fall back to in-memory storage unless tests inject that repository explicitly.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import jsonschema

from .models import (
    ApprovalCreate,
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
    TaskCreate,
    SkillUsageCreate,
    TaskEventCreate,
    TraceEventCreate,
    WorkbenchDryRunExecuteRequest,
    new_id,
    sha256_json,
    utc_now,
)
from .repository import DatabaseResearchAssistantRepository


ASSISTANT_APPROVAL_CONFIRM = "APPROVE_RESEARCH_ASSISTANT_ACTION"


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
        "description": "Phase 1 registers planning capability only; assistant cannot write or submit code.",
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
]


DEFAULT_MCP_SERVERS: list[dict[str, Any]] = [
    {"server_key": "aistock-qe-experiment", "title": "QE experiment MCP", "status": "ready", "health_json": {"mode": "loopback"}},
    {"server_key": "aistock-qe-archive", "title": "QE archive MCP", "status": "ready", "health_json": {"mode": "loopback"}},
    {"server_key": "aistock-validation", "title": "Validation MCP", "status": "ready", "health_json": {"mode": "loopback"}},
    {"server_key": "research-assistant", "title": "Research assistant MCP", "status": "ready", "health_json": {"mode": "loopback"}},
]


DEFAULT_MCP_TOOLS: list[dict[str, Any]] = [
    {
        "server_key": "research-assistant",
        "tool_name": "assistant_create_task",
        "title": "Data health check",
        "risk_level": "medium",
        "requires_approval": False,
        "input_schema_json": {"type": "object", "required": ["title"]},
        "output_schema_json": {"type": "object", "required": ["task_id"]},
        "preflight_schema_json": {"checks": ["schema", "idempotency"]},
        "required_confirmations": [],
    },
    {
        "server_key": "research-assistant",
        "tool_name": "assistant_build_context_pack",
        "title": "Build Context Pack",
        "risk_level": "low",
        "requires_approval": False,
        "input_schema_json": {"type": "object"},
        "output_schema_json": {"type": "object", "required": ["context_pack_id"]},
        "preflight_schema_json": {"checks": ["token_budget", "source_refs"]},
        "required_confirmations": [],
    },
    {
        "server_key": "research-assistant",
        "tool_name": "assistant_create_memory_candidate",
        "title": "Create memory candidate",
        "risk_level": "medium",
        "requires_approval": False,
        "input_schema_json": {"type": "object", "required": ["memory_type", "subject_key", "title"]},
        "output_schema_json": {"type": "object", "required": ["memory_id"]},
        "preflight_schema_json": {"checks": ["source_ref", "evidence_refs", "draft_only"]},
        "required_confirmations": [],
    },
    {
        "server_key": "research-assistant",
        "tool_name": "assistant_create_issue_candidate",
        "title": "Create issue candidate",
        "risk_level": "medium",
        "requires_approval": False,
        "input_schema_json": {"type": "object", "required": ["title", "problem_statement"]},
        "output_schema_json": {"type": "object", "required": ["candidate_id", "status"]},
        "preflight_schema_json": {"checks": ["dedupe_key", "evidence_refs", "draft_only", "github_formal_issue_blocked"]},
        "required_confirmations": [],
    },
    {
        "server_key": "aistock-qe-experiment",
        "tool_name": "qe_template_materialize_confirmed",
        "title": "Materialize QE pending experiment",
        "risk_level": "production_sensitive",
        "requires_approval": True,
        "input_schema_json": {"type": "object", "required": ["template_id", "confirm_template"]},
        "output_schema_json": {"type": "object"},
        "preflight_schema_json": {"checks": ["stock_pool", "node_health", "cost", "approval"]},
        "required_confirmations": ["MATERIALIZE_QE_TEMPLATE"],
    },
    {
        "server_key": "aistock-validation",
        "tool_name": "mcp_github_issue_create",
        "title": "Create formal GitHub Issue",
        "risk_level": "high",
        "requires_approval": True,
        "input_schema_json": {"type": "object", "required": ["title"]},
        "output_schema_json": {"type": "object"},
        "preflight_schema_json": {"checks": ["github_token", "repository", "human_approval"]},
        "required_confirmations": [ASSISTANT_APPROVAL_CONFIRM],
    },
]


DEFAULT_MODEL_PROFILES: list[dict[str, Any]] = [
    {
        "model_profile_id": "model_deepseek_v4_pro_primary",
        "provider": "deepseek",
        "model_name": "deepseek-v4-pro",
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
        "selector_json": {"token_estimate_gte": 64000},
        "fallback_json": {"allow_fallback": True, "fallback_profile_id": "model_deepseek_v4_pro_primary"},
    },
]


class ResearchAssistantService:
    def __init__(self, repository: Any | None = None) -> None:
        self.repository = repository or DatabaseResearchAssistantRepository()

    def health(self) -> dict[str, Any]:
        repository_health = self.repository.health()
        return {
            "service": "research-assistant",
            "status": "ok" if repository_health.get("status") == "ok" else "schema_missing",
            "repository": repository_health,
            "phase": "phase1",
            "runtime_boundaries": {
                "mouse_keyboard_control": False,
                "code_write": False,
                "auto_github_issue": False,
                "production_trading_path": False,
                "silent_fallback": False,
            },
        }

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
        seeded = {"skills": 0, "mcp_servers": 0, "mcp_tools": 0, "model_profiles": 0, "routing_policies": 0, "reports": 0, "notifications": 0}
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
            self.repository.create_record("skills", payload)
            seeded["skills"] += 1
        for item in DEFAULT_MCP_SERVERS:
            self.repository.create_record("mcp_servers", {"server_id": f"mcp_server_{item['server_key']}", **item})
            seeded["mcp_servers"] += 1
        for item in DEFAULT_MCP_TOOLS:
            tool_id = f"mcp_tool_{item['server_key']}_{item['tool_name']}".replace("-", "_")
            self.repository.create_record("mcp_tools", {"tool_id": tool_id, "status": "enabled", **item})
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
        self._ensure_default_reports_and_notifications(seeded)
        return {"seeded": seeded, "catalog_version": "research_assistant_phase1_catalog_20260521"}

    def list_records(self, kind: str, *, filters: dict[str, Any] | None = None, search: str | None = None, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        return self.repository.list_records(kind, filters=filters, search=search, limit=limit, offset=offset)

    def get_task(self, task_id: str) -> dict[str, Any]:
        task = self.repository.get_record("tasks", task_id)
        if not task:
            raise KeyError(f"task not found: {task_id}")
        events = self.repository.list_records("task_events", filters={"task_id": task_id}, limit=200)["items"]
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
        refs_by_type: dict[str, list[str]] = {}
        memory_items: list[dict[str, Any]] = []
        for memory_type in data.include_memory_types:
            page = self.repository.list_records(
                "memory_items",
                filters={"namespace": data.namespace, "memory_type": memory_type, "approval_status": "approved"},
                limit=50,
            )
            refs = [item["memory_id"] for item in page["items"]]
            refs_by_type[memory_type] = refs
            memory_items.extend(page["items"])
        temp_refs = []
        if data.task_id:
            temp_page = self.repository.list_records("temp_memories", filters={"task_id": data.task_id}, limit=50)
            temp_refs = [item["temp_memory_id"] for item in temp_page["items"]]
        pack_json = {
            "mandatory_rules": [
                "Memory Ledger 是事实源，RAG/向量只能辅助召回。",
                "正式 Issue 必须人工审核并同步 GitHub。",
                "高风险 MCP/Skill 必须 preflight 和 approval。",
            ],
            "memory_items": memory_items,
            "task_id": data.task_id,
            "agent_id": data.agent_id,
            "token_budget": data.token_budget,
        }
        context_pack_id = new_id("ctx")
        row = {
            "context_pack_id": context_pack_id,
            "task_id": data.task_id,
            "agent_id": data.agent_id,
            "model_profile": data.model_profile,
            "token_budget": data.token_budget,
            "core_memory_refs": refs_by_type.get("core", []),
            "procedural_memory_refs": refs_by_type.get("procedural", []),
            "architecture_memory_refs": refs_by_type.get("architecture", []),
            "task_state_refs": refs_by_type.get("task_state", []),
            "experiment_memory_refs": refs_by_type.get("experiment", []),
            "graph_relation_refs": [],
            "external_source_refs": [],
            "temp_memory_refs": temp_refs,
            "omitted_relevant_refs": [],
            "pack_summary": f"Context Pack: {len(memory_items)} approved memories, {len(temp_refs)} temp memories",
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
                        "source": "context_pack_build",
                    },
                    "used_in_prompt": True,
                    "payload_json": {
                        "token_budget": data.token_budget,
                        "model_profile": data.model_profile,
                    },
                },
            )
        if data.task_id:
            self.add_task_event(data.task_id, TaskEventCreate(event_type="context_pack_built", message="Context Pack 已构建", payload_json={"context_pack_id": row["context_pack_id"]}))
        return context_pack


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
        outgoing = self.repository.list_records("relations", filters={"source_entity_id": entity_id}, limit=200)["items"]
        incoming = self.repository.list_records("relations", filters={"target_entity_id": entity_id}, limit=200)["items"]
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
        entities = self.repository.list_records("entities", filters={"namespace": namespace}, limit=500)
        relations = self.repository.list_records("relations", limit=500)
        paths = self.repository.list_records("evolution_paths", limit=100)
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
        tool = self.repository.find_one("mcp_tools", {"server_key": data.server_key, "tool_name": data.tool_name})
        if not tool:
            raise KeyError(f"MCP tool not registered: {data.server_key}/{data.tool_name}")
        server = self.repository.find_one("mcp_servers", {"server_key": data.server_key})
        if not server:
            raise KeyError(f"MCP server not registered: {data.server_key}")
        risk = str(tool.get("risk_level") or "medium")
        requires_approval = bool(tool.get("requires_approval")) or risk in {"high", "production_sensitive"}
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
            "server_key": data.server_key,
            "tool_name": data.tool_name,
            "risk_level": risk,
            "requires_approval": requires_approval,
            "passed": passed,
            "approval_required": requires_approval,
            "missing_confirmations": missing_confirmations,
            "preflight_checks": tool.get("preflight_schema_json", {}).get("checks", []),
            "failed_checks": failures,
            "payload_digest": sha256_json(data.payload_json),
            "idempotency_key": data.idempotency_key,
        }
        event = self.repository.create_record(
            "mcp_tool_events",
            {
                "tool_event_id": new_id("mcptev"),
                "task_id": data.task_id,
                "server_key": data.server_key,
                "tool_name": data.tool_name,
                "event_type": "preflight",
                "status": status,
                "idempotency_key": data.idempotency_key,
                "request_json": data.payload_json,
                "response_json": result,
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
                    message=f"MCP preflight {status}: {data.server_key}/{data.tool_name}",
                    payload_json=result,
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
        policies = self.repository.list_records("routing_policies", filters={"role": data.role, "risk_level": data.risk_level, "status": "enabled"}, limit=20)["items"]
        if not policies:
            policies = self.repository.list_records("routing_policies", filters={"role": data.role, "status": "enabled"}, limit=20)["items"]
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
                selected = None
                profile = None
                route_status = "fallback_selected"
                for policy in self.repository.list_records("routing_policies", filters={"status": "enabled"}, limit=100)["items"]:
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
        page = self.repository.list_records("notifications", filters={"user_id": user_id}, limit=100)
        counts: dict[str, int] = {}
        for item in page["items"]:
            status = str(item.get("status") or "unknown")
            counts[status] = counts.get(status, 0) + 1
        return {"user_id": user_id, "counts": counts, "unread": counts.get("unread", 0), "items": page["items"][:10]}

    def validation_discovery_summary(self) -> dict[str, Any]:
        reports = self.repository.list_records("validation_discovery_reports", limit=20)
        candidates = self.repository.list_records("issue_candidates", filters={"status": "needs_review"}, limit=50)
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
