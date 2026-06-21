"""In-memory authority for Research Assistant declarative configuration.

Runtime state stays in the repository, but YAML-backed runtime context and
prompt packs are loaded and validated here before service reads can proceed.
"""

from __future__ import annotations

import copy
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .mcp_catalog_sync import canonicalize_server_key, workflow_capabilities as catalog_workflow_capabilities
from .models import sha256_json
from .prompt_pack import DEFAULT_PROMPT_PACK_PATH, PromptPackSnapshot, load_prompt_pack
from .runtime_config import (
    DEFAULT_ENVIRONMENT,
    DEFAULT_RUNTIME_CONFIG_PATH,
    RUNTIME_CONFIG_KEY,
    RuntimeConfigCapabilityValidationError,
    RuntimeConfigSnapshot,
    load_runtime_config,
)

logger = logging.getLogger(__name__)


class ResearchAssistantDeclarativeConfigError(RuntimeError):
    """Raised when YAML declarative configuration cannot be loaded safely."""

    def __init__(self, error_payload: dict[str, Any]) -> None:
        self.error_payload = error_payload
        super().__init__(str(error_payload.get("message") or error_payload.get("reason_code") or "declarative_config_invalid"))


@dataclass(frozen=True)
class ResearchAssistantDeclarativeConfigSnapshot:
    runtime_config: RuntimeConfigSnapshot
    prompt_pack: PromptPackSnapshot
    workflow_capabilities: tuple[dict[str, Any], ...]
    prompt_nodes: tuple[dict[str, Any], ...]

    def runtime_config_payload(self) -> dict[str, Any]:
        return copy.deepcopy(self.runtime_config.config)

    def runtime_activation_record(self) -> dict[str, Any]:
        return {
            "activation_id": self.runtime_config.activation_id,
            "config_key": self.runtime_config.config_key,
            "config_version": self.runtime_config.config_version,
            "environment": self.runtime_config.environment,
            "source_id": self.runtime_config.source_id,
            "config_json": self.runtime_config_payload(),
            "status": "active",
            "activated_by": "declarative_yaml_memory_authority",
            "activation_metadata_json": {"source_sha256": self.runtime_config.source_sha256},
        }

    def prompt_activation_record(self) -> dict[str, Any]:
        version_refs = [
            {"prompt_key": node["prompt_key"], "version_id": node["version_id"], "checksum": node["checksum"]}
            for node in self.prompt_nodes
        ]
        return {
            "activation_id": self.prompt_pack.activation_id,
            "assistant_key": "research_assistant",
            "environment": self.runtime_config.environment,
            "pack_key": self.prompt_pack.pack_key,
            "pack_version": self.prompt_pack.pack_version,
            "source_id": self.prompt_pack.source_id,
            "version_refs": copy.deepcopy(version_refs),
            "bundle_signature": sha256_json(version_refs),
            "status": "active",
            "activated_by": "declarative_yaml_memory_authority",
            "activation_metadata_json": {"source_sha256": self.prompt_pack.source_sha256},
        }

    def workflow_capability_list(self) -> list[dict[str, Any]]:
        return copy.deepcopy(list(self.workflow_capabilities))

    def workflow_capability(self, capability_key: str) -> dict[str, Any] | None:
        for capability in self.workflow_capabilities:
            if str(capability.get("capability_key") or "") == capability_key:
                return copy.deepcopy(capability)
        return None

    def prompt_node_list(self) -> list[dict[str, Any]]:
        return copy.deepcopy(list(self.prompt_nodes))

    def prompt_node(self, prompt_key: str) -> dict[str, Any] | None:
        for node in self.prompt_nodes:
            if str(node.get("prompt_key") or "") == prompt_key:
                return copy.deepcopy(node)
        return None


def load_declarative_config(
    *,
    environment: str = DEFAULT_ENVIRONMENT,
    runtime_config_path: Path | None = None,
    prompt_pack_path: Path | None = None,
) -> ResearchAssistantDeclarativeConfigSnapshot:
    runtime_path = Path(runtime_config_path or DEFAULT_RUNTIME_CONFIG_PATH)
    prompt_path = Path(prompt_pack_path or DEFAULT_PROMPT_PACK_PATH)
    try:
        runtime_config = load_runtime_config(runtime_path, environment=environment)
        prompt_pack = load_prompt_pack(prompt_path)
        workflow_capabilities = _build_workflow_capabilities(runtime_config)
        prompt_nodes = _build_prompt_nodes(prompt_pack)
    except RuntimeConfigCapabilityValidationError as exc:
        payload = _runtime_capability_error_payload(exc, runtime_path)
        logger.error(
            "research assistant declarative config invalid: reason_code=%s source_path=%s capability_key=%s field=%s actual_type=%s",
            payload["reason_code"],
            payload["source_path"],
            payload.get("capability_key"),
            payload.get("field"),
            payload.get("actual_type"),
        )
        raise ResearchAssistantDeclarativeConfigError(payload) from exc
    except Exception as exc:
        payload = _generic_declarative_error_payload(exc, runtime_path=runtime_path, prompt_path=prompt_path)
        logger.error(
            "research assistant declarative config load failed: reason_code=%s source_path=%s exception_type=%s error=%s",
            payload["reason_code"],
            payload["source_path"],
            payload["exception_type"],
            exc,
        )
        raise ResearchAssistantDeclarativeConfigError(payload) from exc
    return ResearchAssistantDeclarativeConfigSnapshot(
        runtime_config=runtime_config,
        prompt_pack=prompt_pack,
        workflow_capabilities=tuple(workflow_capabilities),
        prompt_nodes=tuple(prompt_nodes),
    )


def _build_workflow_capabilities(runtime_config: RuntimeConfigSnapshot) -> list[dict[str, Any]]:
    configured = runtime_config.config.get("planner", {}).get("workflow_capabilities")
    if not isinstance(configured, list) or not configured:
        raise ValueError("planner.workflow_capabilities must be a non-empty list")
    merged: dict[str, dict[str, Any]] = {}
    for item in [*configured, *catalog_workflow_capabilities()]:
        if not isinstance(item, dict):
            raise ValueError("workflow capability entries must be objects")
        capability = _canonicalize_workflow_capability(dict(item), source=runtime_config.source_path)
        key = str(capability["capability_key"])
        if key == "issue.create_candidate":
            continue
        merged[key] = capability
    if not merged:
        raise ValueError("workflow capability memory authority cannot be empty")
    return list(merged.values())


def _canonicalize_workflow_capability(capability: dict[str, Any], *, source: str) -> dict[str, Any]:
    required = {"capability_key", "capability_type", "title", "description_for_llm", "risk_level", "side_effect_level", "status"}
    missing = sorted(required - set(capability))
    if missing:
        raise ValueError(f"workflow capability missing required keys: {missing}; source={source}")
    capability["capability_key"] = str(capability["capability_key"])
    capability["risk_level"] = str(capability["risk_level"])
    capability["side_effect_level"] = str(capability["side_effect_level"])
    capability["status"] = str(capability.get("status") or "approved")
    capability["mcp_tool_refs"] = _canonicalize_mcp_refs(capability.get("mcp_tool_refs"), capability_key=capability["capability_key"])
    capability["skill_refs"] = _canonicalize_skill_refs(capability.get("skill_refs"), capability_key=capability["capability_key"])
    for field in ("natural_language_triggers", "required_confirmations", "output_cards"):
        value = capability.get(field)
        if value is None:
            capability[field] = []
        elif not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
            raise ValueError(f"{field} must be a string list for capability_key={capability['capability_key']}")
        else:
            capability[field] = [str(item) for item in value]
    checksum_payload = copy.deepcopy(capability)
    checksum_payload.pop("checksum", None)
    capability["checksum"] = sha256_json(checksum_payload)
    return capability


def _canonicalize_mcp_refs(value: Any, *, capability_key: str) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise ValueError(f"mcp_tool_refs must be a list for capability_key={capability_key}; actual_type={type(value).__name__}")
    refs: list[dict[str, str]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValueError(f"mcp_tool_refs[{index}] must be an object for capability_key={capability_key}")
        server_key = str(item.get("server_key") or "").strip()
        tool_name = str(item.get("tool_name") or "").strip()
        if not server_key or not tool_name:
            raise ValueError(f"mcp_tool_refs[{index}] requires server_key and tool_name for capability_key={capability_key}")
        refs.append({"server_key": canonicalize_server_key(server_key), "tool_name": tool_name})
    return refs


def _canonicalize_skill_refs(value: Any, *, capability_key: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"skill_refs must be a list for capability_key={capability_key}; actual_type={type(value).__name__}")
    refs: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item:
            raise ValueError(f"skill_refs[{index}] must be a non-empty string for capability_key={capability_key}")
        refs.append(item)
    return refs


def _build_prompt_nodes(prompt_pack: PromptPackSnapshot) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in prompt_pack.nodes:
        prompt_key = str(item.get("prompt_key") or "").strip()
        if not prompt_key:
            raise ValueError("prompt pack node missing prompt_key")
        if prompt_key in seen:
            raise ValueError(f"duplicate prompt_key in prompt pack: {prompt_key}")
        seen.add(prompt_key)
        node = copy.deepcopy(item)
        node["prompt_node_id"] = f"prompt_{prompt_key.replace('.', '_')}"
        node["version_id"] = f"prompt_version_{prompt_key.replace('.', '_')}_{str(node['checksum'])[:16]}"
        node["source_id"] = prompt_pack.source_id
        node["pack_key"] = prompt_pack.pack_key
        node["pack_version"] = prompt_pack.pack_version
        node.setdefault("status", "enabled")
        if not str(node.get("prompt_text") or "").strip():
            raise ValueError(f"prompt node has empty text: {prompt_key}")
        nodes.append(node)
    return nodes


def _runtime_capability_error_payload(exc: RuntimeConfigCapabilityValidationError, runtime_path: Path) -> dict[str, Any]:
    reason_code = (
        "declarative_config_invalid_capability_mcp_tool_refs"
        if exc.field == "mcp_tool_refs"
        else "declarative_config_invalid_workflow_capability"
    )
    message = (
        f"RA declarative runtime YAML is invalid: source_path={runtime_path}; capability_index={exc.index}; "
        f"capability_key={exc.capability_key}; field={exc.field}; actual_type={exc.actual_type}; "
        f"operator_action=fix configs/research_assistant/runtime_context.yaml and restart/reload Research Assistant"
    )
    return {
        "reason_code": reason_code,
        "code": reason_code,
        "stage": "declarative_config_load",
        "activation_id": None,
        "config_key": RUNTIME_CONFIG_KEY,
        "config_version": None,
        "source_id": str(runtime_path),
        "source_path": str(runtime_path),
        "capability_index": exc.index,
        "capability_key": exc.capability_key,
        "field": exc.field,
        "entry_index": exc.entry_index,
        "actual_type": exc.actual_type,
        "exception_type": type(exc).__name__,
        "message": message,
        "operator_action": "fix configs/research_assistant/runtime_context.yaml and restart/reload Research Assistant",
    }


def _generic_declarative_error_payload(exc: BaseException, *, runtime_path: Path, prompt_path: Path) -> dict[str, Any]:
    text = str(exc)
    prompt_related = "prompt" in text.lower() or "pack" in text.lower()
    reason_code = "declarative_config_invalid_prompt_pack" if prompt_related else "declarative_config_invalid_runtime_context"
    source_path = prompt_path if prompt_related else runtime_path
    return {
        "reason_code": reason_code,
        "code": reason_code,
        "stage": "declarative_config_load",
        "activation_id": None,
        "config_key": RUNTIME_CONFIG_KEY,
        "config_version": None,
        "source_id": str(source_path),
        "source_path": str(source_path),
        "field": "prompt_pack" if prompt_related else "runtime_context",
        "actual_type": type(exc).__name__,
        "exception_type": type(exc).__name__,
        "message": (
            f"RA declarative YAML load failed: source_path={source_path}; error={exc}; "
            "operator_action=fix YAML and restart/reload Research Assistant"
        ),
        "operator_action": "fix YAML and restart/reload Research Assistant",
    }
