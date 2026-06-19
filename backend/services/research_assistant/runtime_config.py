"""Runtime configuration loader for Research Assistant context assembly."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .models import sha256_json
from .repository import TABLES

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RUNTIME_CONFIG_PATH = REPO_ROOT / "configs" / "research_assistant" / "runtime_context.yaml"
RUNTIME_CONFIG_KEY = "research_assistant.runtime_context"
DEFAULT_ENVIRONMENT = "dev"
WORKFLOW_CAPABILITY_LIST_FIELDS = {
    "natural_language_triggers",
    "required_confirmations",
    "output_cards",
    "mcp_tool_refs",
    "skill_refs",
}


class RuntimeConfigCapabilityValidationError(ValueError):
    """Structured validation error for planner.workflow_capabilities entries."""

    def __init__(
        self,
        *,
        index: int,
        capability_key: str,
        field: str,
        actual_type: str,
        detail: str,
        entry_index: int | None = None,
    ) -> None:
        self.index = index
        self.capability_key = capability_key
        self.field = field
        self.actual_type = actual_type
        self.entry_index = entry_index
        location = f"planner.workflow_capabilities[{index}]"
        if entry_index is not None:
            location = f"{location}.{field}[{entry_index}]"
        else:
            location = f"{location}.{field}"
        message = (
            f"{location} invalid for capability_key={capability_key}: "
            f"field={field}; actual_type={actual_type}; {detail}"
        )
        super().__init__(message)


@dataclass(frozen=True)
class RuntimeConfigSnapshot:
    config_key: str
    config_version: str
    environment: str
    source_path: str
    source_sha256: str
    config: dict[str, Any]

    @property
    def source_id(self) -> str:
        return f"runtime_config_source_{self.config_key.replace('.', '_')}_{self.config_version.replace('.', '_')}"

    @property
    def activation_id(self) -> str:
        return f"runtime_config_activation_{self.config_key.replace('.', '_')}_{self.environment}_active"


def load_runtime_config(path: Path | None = None, *, environment: str = DEFAULT_ENVIRONMENT) -> RuntimeConfigSnapshot:
    config_path = Path(path or DEFAULT_RUNTIME_CONFIG_PATH)
    if not config_path.exists():
        raise FileNotFoundError(f"Research Assistant runtime config not found: {config_path}")
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8-sig")) or {}
    validate_runtime_config_payload(payload, config_path)
    source_sha256 = sha256_json(payload)
    return RuntimeConfigSnapshot(
        config_key=str(payload["config_key"]),
        config_version=str(payload["config_version"]),
        environment=environment,
        source_path=_repo_relative(config_path),
        source_sha256=source_sha256,
        config=payload,
    )


def validate_runtime_config_payload(payload: dict[str, Any], path: Path | str) -> None:
    if not isinstance(payload, dict):
        raise ValueError(f"runtime config {path} root must be an object; actual_type={_runtime_config_type_name(payload)}")
    required = {
        "schema_version",
        "config_key",
        "config_version",
        "model_context",
        "model_routing",
        "budget",
        "history_fetch",
        "fresh_tail",
        "compaction",
        "assembly",
        "trace",
        "ui",
        "dialogue_modes",
        "mode_router",
        "dialogue_intent",
        "capability_sync",
        "planner",
        "execution",
        "approval_policy",
        "ui_execution",
        "query_limits",
    }
    missing = sorted(required - set(payload))
    if missing:
        raise ValueError(f"runtime config {path} missing required keys: {missing}")
    if payload["schema_version"] != "aistock_research_assistant_runtime_config_v1":
        raise ValueError(f"unsupported runtime config schema: {payload['schema_version']}")
    if payload["config_key"] != RUNTIME_CONFIG_KEY:
        raise ValueError(f"unexpected runtime config key: {payload['config_key']}")
    ratio_paths = [
        ("budget.prompt_bundle.max_ratio", payload["budget"]["prompt_bundle"]["max_ratio"]),
        ("budget.context_pack.max_ratio", payload["budget"]["context_pack"]["max_ratio"]),
        ("budget.compact_summaries.max_ratio", payload["budget"]["compact_summaries"]["max_ratio"]),
        ("budget.fresh_tail.max_ratio", payload["budget"]["fresh_tail"]["max_ratio"]),
        ("budget.retrieved_raw_snippets.max_ratio", payload["budget"]["retrieved_raw_snippets"]["max_ratio"]),
        ("budget.history.max_ratio", payload["budget"]["history"]["max_ratio"]),
    ]
    total = 0.0
    for key, value in ratio_paths:
        number = float(value)
        if number < 0 or number > 1:
            raise ValueError(f"{key} must be between 0 and 1")
        total += number
    if total > 1.5:
        raise ValueError("runtime context budget ratios are unexpectedly high")
    if str(payload["compaction"]["worker"].get("tools_enabled")).lower() != "false":
        raise ValueError("compaction.worker.tools_enabled must be false")
    execution_defaults = payload["execution"]
    for key in ("default_timeout_seconds", "high_cost_timeout_seconds", "max_retries", "cancel_check_interval_seconds"):
        if int(execution_defaults[key]) < 0:
            raise ValueError(f"execution.{key} must be non-negative")
    if bool(payload["approval_policy"].get("production_sensitive_auto_execute")):
        raise ValueError("approval_policy.production_sensitive_auto_execute must be false in Phase 1")
    if bool(payload["ui_execution"].get("raw_json_main_view")):
        raise ValueError("ui_execution.raw_json_main_view must be false")
    _validate_dialogue_modes(payload["dialogue_modes"], path)
    _validate_mode_router(payload["mode_router"], path)
    dialogue_intent = payload["dialogue_intent"]
    required_intent_keys = {
        "explicit_task_verbs",
        "capability_inquiry_patterns",
        "concept_explanation_patterns",
        "status_query_patterns",
        "bug_terms",
        "issue_terms",
        "qe_terms",
        "execution_terms",
        "negated_execution_patterns",
        "validation_terms",
        "direct_answer_intents",
        "fallback_reply",
        "capability_summary",
        "safety",
        "status_rails",
        "event_messages",
        "card_templates",
    }
    missing_intent_keys = sorted(required_intent_keys - set(dialogue_intent))
    if missing_intent_keys:
        raise ValueError(f"dialogue_intent missing required keys: {missing_intent_keys}")
    list_intent_keys = {
        "explicit_task_verbs",
        "capability_inquiry_patterns",
        "concept_explanation_patterns",
        "status_query_patterns",
        "bug_terms",
        "issue_terms",
        "qe_terms",
        "execution_terms",
        "negated_execution_patterns",
        "validation_terms",
        "direct_answer_intents",
    }
    for key in list_intent_keys:
        values = dialogue_intent[key]
        if not isinstance(values, list) or not all(isinstance(item, str) and item for item in values):
            raise ValueError(f"dialogue_intent.{key} must be a non-empty string list")
    if not isinstance(dialogue_intent["fallback_reply"], str) or not dialogue_intent["fallback_reply"].strip():
        raise ValueError("dialogue_intent.fallback_reply must be a non-empty string")
    for key in ("capability_summary", "safety", "status_rails", "event_messages", "card_templates"):
        if not isinstance(dialogue_intent[key], dict) or not dialogue_intent[key]:
            raise ValueError(f"dialogue_intent.{key} must be a non-empty object")
    required_event_messages = {
        "prompt_bundle_built",
        "chat_received",
        "llm_started",
        "llm_done",
        "action_proposed",
    }
    event_messages = dialogue_intent["event_messages"]
    missing_event_messages = sorted(required_event_messages - set(event_messages))
    if missing_event_messages:
        raise ValueError(f"dialogue_intent.event_messages missing required keys: {missing_event_messages}")
    for key in required_event_messages:
        if not isinstance(event_messages[key], str) or not event_messages[key].strip():
            raise ValueError(f"dialogue_intent.event_messages.{key} must be a non-empty string")
    if int(payload["capability_sync"]["max_tools_per_server"]) <= 0:
        raise ValueError("capability_sync.max_tools_per_server must be positive")
    if int(payload["planner"]["candidate_capability_top_k"]) <= 0:
        raise ValueError("planner.candidate_capability_top_k must be positive")
    workflow_capabilities = payload["planner"].get("workflow_capabilities", [])
    if not isinstance(workflow_capabilities, list) or not workflow_capabilities:
        raise ValueError("planner.workflow_capabilities must be a non-empty list")
    for index, capability in enumerate(workflow_capabilities):
        if not isinstance(capability, dict):
            raise ValueError(f"planner.workflow_capabilities[{index}] must be an object")
        for key in ("capability_key", "capability_type", "title", "description_for_llm", "risk_level", "side_effect_level", "status"):
            if key not in capability:
                raise ValueError(f"planner.workflow_capabilities[{index}] missing {key}")
        capability_key = str(capability.get("capability_key") or f"index:{index}")
        for field in WORKFLOW_CAPABILITY_LIST_FIELDS:
            if field not in capability:
                continue
            value = capability[field]
            if not isinstance(value, list):
                raise RuntimeConfigCapabilityValidationError(
                    index=index,
                    capability_key=capability_key,
                    field=field,
                    actual_type=_runtime_config_type_name(value),
                    detail="must be a list when present",
                )
            if field == "mcp_tool_refs":
                for entry_index, entry in enumerate(value):
                    if not isinstance(entry, dict):
                        raise RuntimeConfigCapabilityValidationError(
                            index=index,
                            capability_key=capability_key,
                            field=field,
                            entry_index=entry_index,
                            actual_type=_runtime_config_type_name(entry),
                            detail="entries must be objects",
                        )
            else:
                for entry_index, entry in enumerate(value):
                    if not isinstance(entry, str) or not entry:
                        raise RuntimeConfigCapabilityValidationError(
                            index=index,
                            capability_key=capability_key,
                            field=field,
                            entry_index=entry_index,
                            actual_type=_runtime_config_type_name(entry),
                            detail="entries must be non-empty strings",
                        )
        if str(capability["risk_level"]) not in {"low", "medium", "high", "production_sensitive"}:
            raise ValueError(f"planner.workflow_capabilities[{index}].risk_level is invalid")
        if str(capability["side_effect_level"]) not in {"read_only", "draft_only", "write_nonprod", "high_cost_compute", "production_sensitive"}:
            raise ValueError(f"planner.workflow_capabilities[{index}].side_effect_level is invalid")
    qe_keys = payload["planner"].get("qe_workflow_capability_keys", [])
    if not isinstance(qe_keys, list) or not qe_keys:
        raise ValueError("planner.qe_workflow_capability_keys must be a non-empty list")
    query_limits = payload["query_limits"]
    required_limits = {
        "conversation_messages_full",
        "prompt_nodes_active",
        "task_events_detail",
        "active_context_segments",
        "active_context_key_facts",
        "memory_items_context_pack",
        "temp_memories_context_pack",
        "graph_entity_relations",
        "graph_summary_entities",
        "graph_summary_relations",
        "graph_summary_paths",
        "routing_policy_primary",
        "routing_policy_role_fallback",
        "routing_policy_scan",
        "notification_summary_items",
        "notification_summary_preview",
        "validation_reports",
        "validation_issue_candidates",
        "router_mcp_servers",
        "router_model_profiles",
        "router_routing_policies",
        "default_context_pack_token_budget",
        "context_pack_max_token_budget",
        "api_list_max_page_size",
    }
    required_limits.update(f"api_list_{kind}" for kind in TABLES)
    missing_limits = sorted(required_limits - set(query_limits))
    if missing_limits:
        raise ValueError(f"runtime config {path} missing query_limits: {missing_limits}")
    for key in required_limits:
        value = int(query_limits[key])
        if value <= 0:
            raise ValueError(f"query_limits.{key} must be positive")


def _runtime_config_type_name(value: Any) -> str:
    return type(value).__name__


def _validate_dialogue_modes(dialogue_modes: dict[str, Any], path: Path) -> None:
    if not isinstance(dialogue_modes, dict):
        raise ValueError(f"runtime config {path} dialogue_modes must be an object")
    required_modes = {"dialogue", "analysis", "planning", "preflight", "execution", "audit", "recovery"}
    if dialogue_modes.get("default_mode") not in required_modes:
        raise ValueError("dialogue_modes.default_mode must be a known mode")
    modes = dialogue_modes.get("modes")
    if not isinstance(modes, dict):
        raise ValueError("dialogue_modes.modes must be an object")
    missing = sorted(required_modes - set(modes))
    if missing:
        raise ValueError(f"dialogue_modes.modes missing modes: {missing}")
    for mode in required_modes:
        cfg = modes.get(mode)
        if not isinstance(cfg, dict):
            raise ValueError(f"dialogue_modes.modes.{mode} must be an object")
        prompt_nodes = cfg.get("prompt_nodes")
        if not isinstance(prompt_nodes, list) or not all(isinstance(item, str) and item for item in prompt_nodes):
            raise ValueError(f"dialogue_modes.modes.{mode}.prompt_nodes must be a non-empty string list")
        if bool(cfg.get("raw_json_main_view", False)):
            raise ValueError(f"dialogue_modes.modes.{mode}.raw_json_main_view must be false")
        for key in ("show_plan_card", "show_clarification_card", "show_context_health_badge", "details_default_collapsed"):
            if key not in cfg or not isinstance(cfg[key], bool):
                raise ValueError(f"dialogue_modes.modes.{mode}.{key} must be a boolean")


def _validate_mode_router(mode_router: dict[str, Any], path: Path) -> None:
    if not isinstance(mode_router, dict):
        raise ValueError(f"runtime config {path} mode_router must be an object")
    thresholds = mode_router.get("confidence_thresholds")
    if not isinstance(thresholds, dict):
        raise ValueError("mode_router.confidence_thresholds must be an object")
    for key in ("direct_answer_min", "task_request_min", "execution_request_min"):
        value = float(thresholds.get(key))
        if value < 0 or value > 1:
            raise ValueError(f"mode_router.confidence_thresholds.{key} must be between 0 and 1")
    fallback = mode_router.get("fallback")
    if not isinstance(fallback, dict) or int(fallback.get("max_questions", 0)) < 0:
        raise ValueError("mode_router.fallback.max_questions must be non-negative")
    user_overrides = mode_router.get("user_overrides")
    if not isinstance(user_overrides, dict):
        raise ValueError("mode_router.user_overrides must be an object")
    for key in ("analysis_only_patterns", "execute_patterns", "audit_patterns"):
        values = user_overrides.get(key)
        if not isinstance(values, list) or not all(isinstance(item, str) and item for item in values):
            raise ValueError(f"mode_router.user_overrides.{key} must be a non-empty string list")


def _repo_relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return str(path.resolve())
