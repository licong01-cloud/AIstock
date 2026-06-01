"""Provider protocols for the Agent Teams runtime."""

from __future__ import annotations

from typing import Any, Protocol

from .models import AgentDefinition, WorkerRunResult, WorkerTask
from backend.services.research_assistant.react_grounding import ToolCatalogEntry


class AgentRunStore(Protocol):
    def queue_run(self, task: WorkerTask, *, agent_run_id: str, model_profile_id: str | None, trace_id: str | None) -> None:
        ...

    def finish_run(self, result: WorkerRunResult) -> None:
        ...


class ContextPackProvider(Protocol):
    def build_for_worker(self, task: WorkerTask, agent: AgentDefinition) -> dict[str, Any]:
        ...


class WorkerExecutor(Protocol):
    def run_worker(self, task: WorkerTask, agent: AgentDefinition, context_pack: dict[str, Any], catalog_entries: list[ToolCatalogEntry]) -> WorkerRunResult:
        ...


class CuratorWritebackProvider(Protocol):
    def create_candidates(self, parent_task_id: str, reduce_json: dict[str, Any]) -> list[dict[str, Any]]:
        ...


class ToolCatalogProvider(Protocol):
    def entries_for_worker(self, agent: AgentDefinition) -> list[ToolCatalogEntry]:
        ...
