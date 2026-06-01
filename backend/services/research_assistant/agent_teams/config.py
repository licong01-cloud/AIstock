"""Declarative Agent Teams config loader."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .models import AgentDefinition, AgentTeamsConfig


REQUIRED_WORKER_KEYS = {
    "qe_experiment_designer",
    "hmm_evolution",
    "factor_developer",
    "local_data_doctor",
}


def _as_tuple(value: Any, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(item, str) and item.strip() for item in value):
        raise ValueError(f"{field_name} must be a non-empty string list")
    return tuple(item.strip() for item in value)


def load_agent_teams_config(path: str | Path) -> AgentTeamsConfig:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("agent_teams.yaml must contain a mapping")
    team_key = str(raw.get("team_key") or "").strip()
    if not team_key:
        raise ValueError("team_key is required")
    orchestrator = raw.get("orchestrator")
    if not isinstance(orchestrator, dict) or orchestrator.get("model_role") != "primary_reasoner":
        raise ValueError("orchestrator.model_role must be primary_reasoner")
    max_parallel_workers = int(raw.get("max_parallel_workers") or 0)
    if max_parallel_workers <= 0:
        raise ValueError("max_parallel_workers must be positive")
    raw_workers = raw.get("workers")
    if not isinstance(raw_workers, list) or not raw_workers:
        raise ValueError("workers must be a non-empty list")
    workers: list[AgentDefinition] = []
    seen: set[str] = set()
    for index, item in enumerate(raw_workers):
        if not isinstance(item, dict):
            raise ValueError("each worker must be a mapping")
        agent_key = str(item.get("agent_key") or "").strip()
        if not agent_key or agent_key in seen:
            raise ValueError(f"agent_key missing or duplicated: {agent_key}")
        seen.add(agent_key)
        model_role = str(item.get("model_role") or "").strip()
        if model_role != "cheap_worker":
            raise ValueError(f"worker {agent_key} must use cheap_worker")
        max_tool_iterations = int(item.get("max_tool_iterations") or 0)
        if max_tool_iterations <= 0:
            raise ValueError(f"worker {agent_key} max_tool_iterations must be positive")
        workers.append(
            AgentDefinition(
                agent_key=agent_key,
                role=str(item.get("role") or "").strip(),
                goal=str(item.get("goal") or "").strip(),
                allowed_servers=_as_tuple(item.get("allowed_servers"), f"{agent_key}.allowed_servers"),
                allowed_tools=_as_tuple(item.get("allowed_tools"), f"{agent_key}.allowed_tools"),
                model_role=model_role,
                prompt_nodes=_as_tuple(item.get("prompt_nodes"), f"{agent_key}.prompt_nodes"),
                max_tool_iterations=max_tool_iterations,
                output_schema=dict(item.get("output_schema") or {}),
                triggers=tuple(str(value).strip().lower() for value in item.get("triggers", []) if str(value).strip()),
                task_order=int(item.get("task_order", index)),
                concurrency_group=str(item.get("concurrency_group") or "default"),
            )
        )
    missing = REQUIRED_WORKER_KEYS - {worker.agent_key for worker in workers}
    if missing:
        raise ValueError(f"missing required first-wave workers: {sorted(missing)}")
    return AgentTeamsConfig(
        team_key=team_key,
        orchestrator=orchestrator,
        workers=tuple(sorted(workers, key=lambda worker: (worker.task_order, worker.agent_key))),
        max_parallel_workers=max_parallel_workers,
        reduce=dict(raw.get("reduce") or {}),
    )
