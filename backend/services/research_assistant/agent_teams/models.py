"""Domain-neutral Agent Teams models for Research Assistant."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class AgentDefinition:
    agent_key: str
    role: str
    goal: str
    allowed_servers: tuple[str, ...]
    allowed_tools: tuple[str, ...]
    model_role: str
    prompt_nodes: tuple[str, ...]
    max_tool_iterations: int
    output_schema: dict[str, Any]
    triggers: tuple[str, ...] = ()
    task_order: int = 0
    concurrency_group: str = "default"

    def allowed_tool_pairs(self) -> set[tuple[str, str]]:
        pairs: set[tuple[str, str]] = set()
        for tool_ref in self.allowed_tools:
            if "/" in tool_ref:
                server_key, tool_name = tool_ref.split("/", 1)
                pairs.add((server_key, tool_name))
            else:
                for server_key in self.allowed_servers:
                    pairs.add((server_key, tool_ref))
        return pairs


@dataclass(frozen=True)
class AgentTeamsConfig:
    team_key: str
    orchestrator: dict[str, Any]
    workers: tuple[AgentDefinition, ...]
    max_parallel_workers: int
    reduce: dict[str, Any] = field(default_factory=dict)

    def worker_by_key(self, agent_key: str) -> AgentDefinition:
        for worker in self.workers:
            if worker.agent_key == agent_key:
                return worker
        raise KeyError(f"unknown worker agent_key: {agent_key}")


@dataclass(frozen=True)
class WorkerTask:
    parent_task_id: str
    agent_key: str
    role: str
    task_order: int
    objective: str
    input_json: dict[str, Any] = field(default_factory=dict)

    def stable_key(self) -> tuple[int, str]:
        return (self.task_order, self.agent_key)


@dataclass(frozen=True)
class WorkerRunResult:
    agent_run_id: str
    parent_task_id: str
    agent_key: str
    role: str
    status: str
    task_order: int
    summary: str
    artifacts: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    result_json: dict[str, Any] = field(default_factory=dict)
    trace_id: str | None = None
    context_pack_id: str | None = None

    def stable_key(self) -> tuple[int, str]:
        return (self.task_order, self.agent_key)

    def as_reduce_item(self) -> dict[str, Any]:
        return {
            "agent_key": self.agent_key,
            "agent_run_id": self.agent_run_id,
            "artifacts": list(self.artifacts),
            "context_pack_id": self.context_pack_id,
            "evidence_refs": list(self.evidence_refs),
            "role": self.role,
            "result_json": self.result_json,
            "status": self.status,
            "summary": self.summary,
            "task_order": self.task_order,
            "trace_id": self.trace_id,
        }


@dataclass(frozen=True)
class AgentTeamResult:
    parent_task_id: str
    status: str
    assistant_text: str
    worker_results: tuple[WorkerRunResult, ...]
    reduce_json: dict[str, Any]
    memory_candidates: tuple[dict[str, Any], ...] = ()
    trace: tuple[dict[str, Any], ...] = ()

    def canonical_json(self) -> str:
        payload = {
            "assistant_text": self.assistant_text,
            "memory_candidates": list(self.memory_candidates),
            "parent_task_id": self.parent_task_id,
            "reduce_json": self.reduce_json,
            "status": self.status,
            "worker_results": [item.as_reduce_item() for item in self.worker_results],
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
