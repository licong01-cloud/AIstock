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
    _validate_runtime_config(payload, config_path)
    source_sha256 = sha256_json(payload)
    return RuntimeConfigSnapshot(
        config_key=str(payload["config_key"]),
        config_version=str(payload["config_version"]),
        environment=environment,
        source_path=_repo_relative(config_path),
        source_sha256=source_sha256,
        config=payload,
    )


def _validate_runtime_config(payload: dict[str, Any], path: Path) -> None:
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


def _repo_relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return str(path.resolve())
