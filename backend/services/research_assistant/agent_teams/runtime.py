"""Domain-neutral orchestrator-worker Agent Teams runtime."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from backend.services.research_assistant.react_grounding import ToolCatalogEntry, assert_tool_in_catalog

from .models import AgentDefinition, AgentTeamResult, AgentTeamsConfig, WorkerRunResult, WorkerTask
from .providers import AgentRunStore, ContextPackProvider, CuratorWritebackProvider, ToolCatalogProvider, WorkerExecutor

IdFactory = Callable[[WorkerTask], str]
Clock = Callable[[], datetime]


@dataclass(frozen=True)
class AgentTeamsRuntimeProviders:
    run_store: AgentRunStore
    context_provider: ContextPackProvider
    worker_executor: WorkerExecutor
    tool_catalog_provider: ToolCatalogProvider
    curator: CuratorWritebackProvider | None = None


class AgentTeamsRuntime:
    def __init__(
        self,
        *,
        config: AgentTeamsConfig,
        providers: AgentTeamsRuntimeProviders,
        id_factory: IdFactory,
        clock: Clock | None = None,
    ) -> None:
        if config.max_parallel_workers <= 0:
            raise ValueError("max_parallel_workers must be positive")
        self.config = config
        self.providers = providers
        self.id_factory = id_factory
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def run(self, *, parent_task_id: str, objective: str, requested_agent_keys: list[str] | None = None) -> AgentTeamResult:
        tasks = self.decompose(parent_task_id=parent_task_id, objective=objective, requested_agent_keys=requested_agent_keys)
        trace: list[dict[str, object]] = [
            {
                "event": "orchestrator_decomposed",
                "parent_task_id": parent_task_id,
                "task_count": len(tasks),
                "agent_keys": [task.agent_key for task in tasks],
                "orchestrator_does_domain_work": False,
            }
        ]
        queued: list[tuple[WorkerTask, AgentDefinition, str]] = []
        for task in tasks:
            agent = self.config.worker_by_key(task.agent_key)
            agent_run_id = self.id_factory(task)
            self.providers.run_store.queue_run(task, agent_run_id=agent_run_id, model_profile_id=agent.model_role, trace_id=None)
            queued.append((task, agent, agent_run_id))
        results = self._dispatch_workers(queued)
        reduced = self.reduce(parent_task_id=parent_task_id, objective=objective, results=results)
        memory_candidates = tuple(self.providers.curator.create_candidates(parent_task_id, reduced) if self.providers.curator else [])
        trace.append({"event": "orchestrator_reduced", "status": reduced["status"], "result_count": len(results)})
        return AgentTeamResult(
            parent_task_id=parent_task_id,
            status=str(reduced["status"]),
            assistant_text=str(reduced["assistant_text"]),
            worker_results=tuple(sorted(results, key=lambda item: item.stable_key())),
            reduce_json=reduced,
            memory_candidates=memory_candidates,
            trace=tuple(trace),
        )

    def decompose(self, *, parent_task_id: str, objective: str, requested_agent_keys: list[str] | None = None) -> list[WorkerTask]:
        selected = self._select_workers(objective, requested_agent_keys)
        return [
            WorkerTask(
                parent_task_id=parent_task_id,
                agent_key=agent.agent_key,
                role=agent.role,
                task_order=order,
                objective=objective,
                input_json={"objective": objective, "agent_key": agent.agent_key, "prompt_nodes": list(agent.prompt_nodes)},
            )
            for order, agent in enumerate(selected)
        ]

    def _select_workers(self, objective: str, requested_agent_keys: list[str] | None) -> list[AgentDefinition]:
        if requested_agent_keys:
            requested = set(requested_agent_keys)
            selected = [worker for worker in self.config.workers if worker.agent_key in requested]
        else:
            lowered = objective.lower()
            selected = [worker for worker in self.config.workers if any(trigger and trigger in lowered for trigger in worker.triggers)]
            if len(selected) < 2:
                selected = list(self.config.workers[: min(2, len(self.config.workers))])
        if not selected:
            raise ValueError("no workers selected for agent team objective")
        return sorted(selected, key=lambda worker: (worker.task_order, worker.agent_key))

    def _dispatch_workers(self, queued: list[tuple[WorkerTask, AgentDefinition, str]]) -> list[WorkerRunResult]:
        results: list[WorkerRunResult] = []
        with ThreadPoolExecutor(max_workers=min(self.config.max_parallel_workers, len(queued) or 1)) as executor:
            futures = {executor.submit(self._run_one_worker, task, agent, agent_run_id): (task, agent, agent_run_id) for task, agent, agent_run_id in queued}
            for future in as_completed(futures):
                task, agent, agent_run_id = futures[future]
                try:
                    result = future.result()
                except Exception as exc:  # worker isolation: failed worker is reduced with others
                    result = WorkerRunResult(
                        agent_run_id=agent_run_id,
                        parent_task_id=task.parent_task_id,
                        agent_key=task.agent_key,
                        role=task.role,
                        status="failed",
                        task_order=task.task_order,
                        summary=f"worker failed: {type(exc).__name__}: {exc}",
                        result_json={"error": str(exc), "error_type": type(exc).__name__},
                    )
                self.providers.run_store.finish_run(result)
                results.append(result)
        return sorted(results, key=lambda item: item.stable_key())

    def _run_one_worker(self, task: WorkerTask, agent: AgentDefinition, agent_run_id: str) -> WorkerRunResult:
        context_pack = self.providers.context_provider.build_for_worker(task, agent)
        catalog_entries = enforce_worker_catalog(agent, self.providers.tool_catalog_provider.entries_for_worker(agent))
        result = self.providers.worker_executor.run_worker(task, agent, context_pack, catalog_entries)
        if result.agent_run_id != agent_run_id:
            result = WorkerRunResult(
                agent_run_id=agent_run_id,
                parent_task_id=result.parent_task_id,
                agent_key=result.agent_key,
                role=result.role,
                status=result.status,
                task_order=result.task_order,
                summary=result.summary,
                artifacts=result.artifacts,
                evidence_refs=result.evidence_refs,
                result_json=result.result_json,
                trace_id=result.trace_id,
                context_pack_id=result.context_pack_id,
            )
        return result

    def reduce(self, *, parent_task_id: str, objective: str, results: list[WorkerRunResult]) -> dict[str, object]:
        ordered = sorted(results, key=lambda item: item.stable_key())
        successful = [item for item in ordered if item.status == "succeeded"]
        failed = [item for item in ordered if item.status != "succeeded"]
        evidence_refs = sorted({ref for item in ordered for ref in item.evidence_refs})
        status = "completed_with_failures" if failed and successful else "failed" if failed and not successful else "completed"
        assistant_lines = [f"Agent Teams 完成 {len(successful)}/{len(ordered)} 个 worker："]
        assistant_lines.extend(f"- {item.agent_key}: {item.status}; {item.summary}" for item in ordered)
        if evidence_refs:
            assistant_lines.append(f"evidence_refs={','.join(evidence_refs)} as_of={self.clock().date().isoformat()}")
        return {
            "schema_version": "research_assistant_agent_team_reduce_v1",
            "parent_task_id": parent_task_id,
            "objective": objective,
            "status": status,
            "assistant_text": "\n".join(assistant_lines),
            "worker_results": [item.as_reduce_item() for item in ordered],
            "evidence_refs": evidence_refs,
            "conflict_arbitration": {"strategy": "stable_agent_order", "failed_workers": [item.agent_key for item in failed]},
        }


def enforce_worker_catalog(agent: AgentDefinition, catalog_entries: list[ToolCatalogEntry]) -> list[ToolCatalogEntry]:
    allowed_pairs = agent.allowed_tool_pairs()
    scoped: list[ToolCatalogEntry] = []
    for entry in catalog_entries:
        if (entry.server_key, entry.tool_name) in allowed_pairs or entry.server_key in agent.allowed_servers and entry.tool_name in agent.allowed_tools:
            scoped.append(entry)
    return sorted(scoped, key=lambda item: (item.server_key, item.tool_name))


def assert_worker_tool_allowed(agent: AgentDefinition, call: object, catalog_entries: list[ToolCatalogEntry]):
    scoped = enforce_worker_catalog(agent, catalog_entries)
    return assert_tool_in_catalog(call, scoped)  # type: ignore[arg-type]
