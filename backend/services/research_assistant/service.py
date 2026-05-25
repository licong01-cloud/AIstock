"""Business service for the Research Assistant Console.

This service keeps Phase 1 state explicit and replayable. It does not execute
long-running experiments, does not write formal GitHub issues, and does not
fall back to in-memory storage unless tests inject that repository explicitly.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from time import perf_counter
from typing import Any

logger = logging.getLogger("aistock.research_assistant.service")

import jsonschema

from .context_budget import ContextBudgetPlan, ContextBudgetPlanner
from .models import (
    ApprovalCreate,
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
from .prompt_pack import DEFAULT_PROMPT_PACK_PATH, FORBIDDEN_UNDEVELOPED_CAPABILITY_PHRASES, PromptPackSnapshot, load_prompt_pack
from .repository import DatabaseResearchAssistantRepository
from .runtime_config import DEFAULT_ENVIRONMENT, RUNTIME_CONFIG_KEY, RuntimeConfigSnapshot, load_runtime_config


ASSISTANT_APPROVAL_CONFIRM = "APPROVE_RESEARCH_ASSISTANT_ACTION"
PROMPT_CACHE_DIR = Path(os.getenv("AISTOCK_ASSISTANT_PROMPT_CACHE_DIR", "var/research_assistant/prompt_cache"))
CATALOG_BOOTSTRAP_ACTION = "POST /api/v1/research-assistant/catalogs/seed"


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
        "model_name": os.getenv("ASSISTANT_DEEPSEEK_MODEL", "deepseek-chat"),
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
        if provider == "deepseek":
            env_key = "DEEPSEEK_API_KEY"
            if not os.getenv(env_key):
                raise RuntimeError("DEEPSEEK_API_KEY is not configured for Research Assistant LLM calls")
            model_id = model_name if "/" in model_name else f"deepseek/{model_name}"
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
        response = litellm.completion(
            model=model_id,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        duration_ms = int((perf_counter() - start) * 1000)
        content = str(response.choices[0].message.content or "").strip()
        usage_raw = getattr(response, "usage", None)
        usage = dict(usage_raw) if isinstance(usage_raw, dict) else {}
        if not content:
            raise RuntimeError("assistant LLM returned empty content")
        return LlmCallResult(content=content, provider=provider, model=model_id, duration_ms=duration_ms, usage=usage)


class ResearchAssistantService:
    def __init__(self, repository: Any | None = None, llm_client: Any | None = None, *, environment: str = DEFAULT_ENVIRONMENT) -> None:
        self.repository = repository or DatabaseResearchAssistantRepository()
        self.llm_client = llm_client or ResearchAssistantLlmClient()
        self.environment = environment
        self.context_budget_planner = ContextBudgetPlanner()

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
            "phase": "prompt_context_runtime",
            "implemented_capabilities": {
                "mcp_api_preflight": True,
                "approval_gates": True,
                "trace_audit": True,
                "memory_audit": True,
                "prompt_pack_activation": True,
                "runtime_context_config": True,
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
            page = self.repository.list_records(
                requirement["catalog"],
                filters=requirement.get("filters") or {},
                limit=1,
            )
            present = int(page.get("total") or 0)
            expected_min = int(requirement["expected_min"])
            ready = present >= expected_min
            check = {
                "catalog": requirement["catalog"],
                "label": requirement["label"],
                "expected_min": expected_min,
                "present": present,
                "ready": ready,
                "filters": requirement.get("filters") or {},
            }
            if not ready:
                check["missing_count"] = max(expected_min - present, 0)
                missing_catalogs.append(requirement["catalog"])
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
            "model_profiles": 0,
            "routing_policies": 0,
            "prompt_nodes": 0,
            "prompt_node_versions": 0,
            "prompt_activations": 0,
            "runtime_config_activations": 0,
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
        prompt_pack = load_prompt_pack(DEFAULT_PROMPT_PACK_PATH)
        self._seed_prompt_pack(prompt_pack, seeded)
        runtime_config = load_runtime_config(environment=self.environment)
        self._seed_runtime_config(runtime_config, seeded)
        self._ensure_default_reports_and_notifications(seeded)
        return {"seeded": seeded, "catalog_version": "research_assistant_prompt_context_runtime_20260525"}

    def _seed_prompt_pack(self, prompt_pack: PromptPackSnapshot, seeded: dict[str, int]) -> None:
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

    def list_records(self, kind: str, *, filters: dict[str, Any] | None = None, search: str | None = None, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        return self.repository.list_records(kind, filters=filters, search=search, limit=limit, offset=offset)

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
            "node_count": len(selected),
            "prompt_keys": [item["prompt_key"] for item in selected],
            "user_message_digest": sha256_json({"message": data.user_message}),
        }
        selection_trace = {
            "algorithm": "ancestor_closed_keyword_multibranch_v1",
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
                    message="已按树型提示词选择必要分支，生成本轮提示词包。",
                    payload_json={"prompt_bundle_id": bundle["prompt_bundle_id"], "prompt_keys": bundle_json["prompt_keys"], "checksum": checksum},
                ),
            )
        return bundle

    def _select_prompt_nodes(self, available: list[dict[str, Any]], data: PromptBundleBuildRequest) -> list[dict[str, Any]]:
        by_key = {str(item["prompt_key"]): item for item in available}
        lower_message = data.user_message.lower()
        selected_keys: set[str] = set(data.required_prompt_keys)
        for item in available:
            trigger = item.get("trigger_json") or {}
            if trigger.get("always"):
                selected_keys.add(str(item["prompt_key"]))
                continue
            if item.get("phase") == data.phase and self._trigger_matches(trigger, lower_message):
                selected_keys.add(str(item["prompt_key"]))
            if str(item.get("phase")) in {"preflight", "result"} and data.phase == str(item.get("phase")) and self._trigger_matches(trigger, lower_message):
                selected_keys.add(str(item["prompt_key"]))
        if any(key.startswith("domain.qe") or key.startswith("workflow.qe") for key in selected_keys):
            selected_keys.add("domain.qe_experiment")
            selected_keys.add("workflow.qe_draft_then_approval")
            selected_keys.add("tool_guard.mcp_qe")
        selected_keys.add("root.assistant")
        selected_keys.add("governance.no_silent_action")
        selected_keys.add("intent.planning")
        selected_keys.add("renderer.human_cards")
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

    @staticmethod
    def _trigger_matches(trigger: dict[str, Any], lower_message: str) -> bool:
        keywords = [str(item).lower() for item in trigger.get("keywords_any") or []]
        return any(keyword in lower_message for keyword in keywords)

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
        if data.allow_execute:
            raise ValueError("Phase 1 chat turn does not execute actions; create approval/preflight plan first")
        self.ensure_catalog_ready()
        conversation = (
            self.repository.get_record("conversations", data.conversation_id)
            if data.conversation_id
            else self.create_conversation(ConversationCreate(title=self._conversation_title(data.message), user_id=data.user_id))
        )
        if not conversation:
            raise KeyError(f"conversation not found: {data.conversation_id}")
        conversation_id = conversation["conversation_id"]
        task = self.create_task(
            TaskCreate(
                title=self._conversation_title(data.message),
                task_type="assistant_chat_turn",
                risk_level=data.risk_level,
                input_json={"user_message": data.message, "phase": data.phase, "allow_execute": data.allow_execute},
                created_by=data.created_by,
            )
        )
        user_message = self.add_conversation_message(
            ConversationMessageCreate(
                conversation_id=conversation_id,
                role="user",
                content_text=data.message,
                task_id=task["task_id"],
                content_json={"phase": data.phase},
            )
        )
        self.add_task_event(task["task_id"], TaskEventCreate(event_type="chat_received", message="已接收用户对话需求，进入理解与计划阶段。", payload_json={"conversation_id": conversation_id}))

        runtime_activation = self.active_runtime_config_activation()
        runtime_config = dict(runtime_activation["config_json"])
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
        messages = self._chat_messages_for_llm(data.message, bundle, context_pack, prior_messages)
        self.add_task_event(task["task_id"], TaskEventCreate(event_type="llm_started", message="主模型调用已开始。", payload_json={"model_profile_id": model_profile["model_profile_id"], "prompt_bundle_id": bundle["prompt_bundle_id"]}))
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
                },
                cost_json={"usage": llm_result.usage},
            )
        )
        context_health = self._context_health_payload(conversation_id, budget_plan)
        cards = self._build_human_cards(data.message, task, bundle, route)
        cards["context_health"] = context_health
        assistant_text = self._compose_assistant_reply(data.message, llm_result.content, cards)
        assistant_message = self.add_conversation_message(
            ConversationMessageCreate(
                conversation_id=conversation_id,
                role="assistant",
                content_text=assistant_text,
                content_json={
                    "cards": cards,
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
                message="主模型已返回，计划卡和确认卡已生成。",
                payload_json={"assistant_message_id": assistant_message["message_id"], "trace_id": trace["trace_id"]},
            ),
        )
        self.add_task_event(
            task["task_id"],
            TaskEventCreate(
                event_type="action_proposed",
                severity="warning",
                message="已提出候选动作；确认前不会调用 materialize/run。",
                payload_json={"proposal_count": len(cards.get("action_proposals", [])), "safety": cards["safety"]},
            ),
        )
        return {
            "conversation": self.repository.get_record("conversations", conversation_id),
            "user_message": user_message,
            "assistant_message": assistant_message,
            "task": self.repository.get_record("tasks", task["task_id"]),
            "task_events": self.repository.list_records("task_events", filters={"task_id": task["task_id"]}, limit=self.configured_limit("task_events_detail"))["items"],
            "prompt_bundle": self._public_prompt_bundle(bundle),
            "context_pack": {"context_pack_id": context_pack["context_pack_id"], "pack_summary": context_pack["pack_summary"], "checksum": context_pack["checksum"]},
            "trace": {"trace_id": trace["trace_id"], "status": trace["status"], "duration_ms": trace.get("duration_ms"), "model_profile_id": trace.get("model_profile_id")},
            "context_health": context_health,
            "cards": cards,
        }

    @staticmethod
    def _conversation_title(message: str) -> str:
        return (message.strip().replace("\n", " ")[:48] or "新的对话")

    @staticmethod
    def _public_prompt_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
        return {
            "prompt_bundle_id": bundle["prompt_bundle_id"],
            "phase": bundle["phase"],
            "checksum": bundle["checksum"],
            "activation_id": bundle.get("activation_id"),
            "version_refs": bundle.get("version_refs") or [],
            "node_refs": bundle["node_refs"],
            "selection_trace_json": bundle["selection_trace_json"],
            "cache_path": bundle.get("cache_path"),
        }

    @staticmethod
    def _chat_messages_for_llm(user_message: str, bundle: dict[str, Any], context_pack: dict[str, Any], prior_messages: list[dict[str, str]] | None = None) -> list[dict[str, str]]:
        system = str(bundle["bundle_text"])
        context = (
            f"Context Pack 摘要：{context_pack.get('pack_summary')}\n"
            "请基于这些已审计上下文进行回答；缺失信息时先向用户确认。"
        )
        messages: list[dict[str, str]] = [
            {"role": "system", "content": system},
            {"role": "user", "content": context},
        ]
        if prior_messages:
            messages.extend(prior_messages)
        messages.append({"role": "user", "content": user_message})
        return messages

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
            retry_messages = self._chat_messages_for_llm(user_message, bundle, context_pack, retry_prior_messages)
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

    def _context_health_payload(self, conversation_id: str, budget_plan: ContextBudgetPlan) -> dict[str, Any]:
        return {
            "status": "compacted_or_ready" if budget_plan.should_compact else "healthy",
            "notify_mode": budget_plan.runtime_config["ui"]["notify_auto_compaction"],
            "show_badge": bool(budget_plan.runtime_config["ui"]["show_context_health_badge"]),
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

    @staticmethod
    def _build_human_cards(user_message: str, task: dict[str, Any], bundle: dict[str, Any], route: dict[str, Any]) -> dict[str, Any]:
        lower = user_message.lower()
        is_qe = any(token in lower for token in ["qe", "loop", "实验", "回测", "演进", "quantevolver"])
        if is_qe:
            plan_steps = [
                "复述 QE 实验目标、收益评估方向和本轮不执行的边界。",
                "从 QE MCP 目录中选择模板创建、验证、预检查相关能力，并确认固定 PIT 股票池要求。",
                "生成 10 个 loop 的草稿结构、候选因子来源、时间窗和成本约束。",
                "等待用户确认后，才允许进入 validate、stock pool、节点健康和成本 preflight。",
                "preflight 全部通过且用户再次确认后，后续阶段才能调用 materialize 或 run。",
            ]
            clarifications = [
                "本次 QE 回测应使用哪个固定 PIT 股票池或默认回测股票池？",
                "10 个 loop 的目标更偏向收益提升、稳定性、因子覆盖，还是模型结构探索？",
                "确认前是否继续保持只生成草稿，不调用 materialize/run？",
            ]
            action_proposals = [
                {"title": "生成 QE 10 loop 实验草稿", "risk": "medium", "approval_required": False, "status": "draft_only"},
                {"title": "QE template validate + MCP preflight", "risk": "high", "approval_required": True, "status": "waiting_confirmation"},
            ]
        else:
            plan_steps = [
                "复述你的研究目标和约束。",
                "选择可能需要的 MCP、Skill、Memory 和模型配置。",
                "列出缺失信息和风险等级。",
                "在你确认后，再进入相应 MCP preflight 或草稿生成。",
            ]
            clarifications = ["请确认这次任务只需要我先规划和提问，还是还要准备某个 MCP 的预检查？"]
            action_proposals = [{"title": "继续澄清并生成计划", "risk": "low", "approval_required": False, "status": "ready"}]
        return {
            "plan_card": {"title": "本轮计划", "steps": plan_steps},
            "clarification_card": {"title": "需要你确认", "questions": clarifications},
            "action_proposals": action_proposals,
            "status_rail": [
                {"label": "接收需求", "status": "done"},
                {"label": "选择提示词", "status": "done"},
                {"label": "构建上下文", "status": "done"},
                {"label": "等待确认", "status": "current"},
                {"label": "MCP 预检查", "status": "locked"},
                {"label": "执行", "status": "locked"},
                {"label": "写入记忆", "status": "locked"},
            ],
            "capability_summary": {
                "mcp": "已识别 Research Assistant、QE、Validation 等 MCP 能力候选。",
                "skill": "已纳入本地 Skill Catalog，后续可按任务加载 QE 诊断、因子分析等能力。",
                "model": route.get("model_profile", {}).get("display_name") or route.get("model_profile", {}).get("model_name"),
                "prompt_branches": [item["prompt_key"] for item in bundle.get("node_refs", [])],
            },
            "safety": {"no_materialize_before_confirmation": True, "no_run_before_confirmation": True, "no_raw_json_in_main_chat": True},
        }

    @staticmethod
    def _compose_assistant_reply(user_message: str, llm_text: str, cards: dict[str, Any]) -> str:
        lines = [llm_text.strip()]
        lines.append("\n我已先把本轮限制在理解、计划和确认阶段；不会在确认前执行 QE materialize/run 或其他高风险 MCP。")
        if "qe" in user_message.lower() or "实验" in user_message or "回测" in user_message:
            lines.append("我会把 QE 回测和实盘严格区分，回测默认要求固定 PIT 股票池或你明确指定的股票池。")
        lines.append(f"请先确认：{cards['clarification_card']['questions'][0]}")
        return "\n".join(lines)

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
                limit=self.configured_limit("memory_items_context_pack"),
            )
            refs = [item["memory_id"] for item in page["items"]]
            refs_by_type[memory_type] = refs
            memory_items.extend(page["items"])
        temp_refs = []
        if data.task_id:
            temp_page = self.repository.list_records("temp_memories", filters={"task_id": data.task_id}, limit=self.configured_limit("temp_memories_context_pack"))
            temp_refs = [item["temp_memory_id"] for item in temp_page["items"]]
        token_budget = data.token_budget or self.configured_limit("default_context_pack_token_budget")
        pack_json = {
            "mandatory_rules": [
                "Memory Ledger 是事实源，RAG/向量只能辅助召回。",
                "正式 Issue 必须人工审核并同步 GitHub。",
                "高风险 MCP/Skill 必须 preflight 和 approval。",
            ],
            "memory_items": memory_items,
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
                        "token_budget": token_budget,
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
