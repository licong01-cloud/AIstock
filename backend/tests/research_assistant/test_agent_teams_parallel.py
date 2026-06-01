from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from backend.services.research_assistant.agent_teams.config import load_agent_teams_config
from backend.services.research_assistant.agent_teams.models import WorkerRunResult, WorkerTask
from backend.services.research_assistant.agent_teams.runtime import AgentTeamsRuntime, AgentTeamsRuntimeProviders
from backend.services.research_assistant.react_grounding import ToolCatalogEntry


CONFIG_PATH = Path(__file__).resolve().parents[3] / "configs/research_assistant/agent_teams.yaml"


@dataclass
class FakeRunStore:
    queued: list[dict[str, Any]] = field(default_factory=list)
    finished: list[WorkerRunResult] = field(default_factory=list)

    def queue_run(self, task: WorkerTask, *, agent_run_id: str, model_profile_id: str | None, trace_id: str | None) -> None:
        self.queued.append({"agent_run_id": agent_run_id, "task": task, "model_profile_id": model_profile_id, "trace_id": trace_id})

    def finish_run(self, result: WorkerRunResult) -> None:
        self.finished.append(result)


@dataclass
class FakeContextProvider:
    packs: list[dict[str, Any]] = field(default_factory=list)

    def build_for_worker(self, task, agent):
        pack = {
            "context_pack_id": f"ctx_{task.agent_key}",
            "agent_id": task.agent_key,
            "pack_json": {
                "route_reason": f"isolated for {task.agent_key}",
                "graph_relation_refs": [{"relation_id": f"rel_{task.agent_key}", "summary": "dependency neighbor"}],
                "resident_memories": ["personal.directive.answer_with_evidence"],
            },
        }
        self.packs.append(pack)
        return pack


class FakeCatalogProvider:
    def entries_for_worker(self, agent):
        return [
            ToolCatalogEntry(server_key=server, tool_name=tool.split("/", 1)[1] if "/" in tool else tool, status="approved", risk_level="low", side_effect_level="read_only")
            for server in agent.allowed_servers
            for tool in agent.allowed_tools
            if tool.startswith(server + "/") or "/" not in tool
        ]


class FakeCurator:
    def create_candidates(self, parent_task_id: str, reduce_json: dict[str, Any]) -> list[dict[str, Any]]:
        if not reduce_json.get("evidence_refs"):
            return []
        return [
            {
                "memory_type": "analysis_note",
                "tree_path": "personal.task.agent_team_progress",
                "approval_status": "draft",
                "provenance_json": {"parent_task_id": parent_task_id, "source": "agent_team_reduce"},
                "content_text": reduce_json["assistant_text"],
            }
        ]


class FakeWorkerExecutor:
    def __init__(self, *, fail_agent: str | None = None, reverse_delay: bool = False) -> None:
        self.fail_agent = fail_agent
        self.reverse_delay = reverse_delay
        self.calls: list[dict[str, Any]] = []

    def run_worker(self, task, agent, context_pack, catalog_entries):
        self.calls.append({"task": task, "context_pack": context_pack, "catalog_entries": catalog_entries})
        if task.agent_key == self.fail_agent:
            raise RuntimeError("deterministic worker failure")
        return WorkerRunResult(
            agent_run_id="placeholder",
            parent_task_id=task.parent_task_id,
            agent_key=task.agent_key,
            role=task.role,
            status="succeeded",
            task_order=task.task_order,
            summary=f"{task.agent_key} summary source=fixture as_of=2026-06-02",
            artifacts=(f"artifact:{task.agent_key}",),
            evidence_refs=(f"evidence:{task.agent_key}",),
            result_json={"status": "succeeded", "summary": task.agent_key},
            context_pack_id=context_pack["context_pack_id"],
        )


def _runtime(executor: FakeWorkerExecutor):
    config = load_agent_teams_config(CONFIG_PATH)
    run_store = FakeRunStore()
    context = FakeContextProvider()
    providers = AgentTeamsRuntimeProviders(run_store=run_store, context_provider=context, worker_executor=executor, tool_catalog_provider=FakeCatalogProvider(), curator=FakeCurator())
    runtime = AgentTeamsRuntime(
        config=config,
        providers=providers,
        id_factory=lambda task: f"run_{task.task_order:03d}_{task.agent_key}",
    )
    return runtime, run_store, context


def test_agent_teams_parallel_dispatch_uses_isolated_context_and_reduces_workers() -> None:
    executor = FakeWorkerExecutor()
    runtime, run_store, context = _runtime(executor)
    result = runtime.run(parent_task_id="task_1", objective="分析 QE 实验、因子和本地数据", requested_agent_keys=["qe_experiment_designer", "factor_developer", "local_data_doctor"])
    assert result.status == "completed"
    assert len(result.worker_results) == 3
    assert [item.agent_key for item in result.worker_results] == ["qe_experiment_designer", "factor_developer", "local_data_doctor"]
    assert len({pack["agent_id"] for pack in context.packs}) == 3
    assert all(call["context_pack"]["agent_id"] == call["task"].agent_key for call in executor.calls)
    assert "thought:" not in result.assistant_text.lower()
    assert "observation:" not in result.assistant_text.lower()
    assert result.memory_candidates and result.memory_candidates[0]["tree_path"].startswith("personal.task.")
    assert len(run_store.queued) == 3 and len(run_store.finished) == 3


def test_agent_teams_worker_failure_is_isolated_and_still_reduces_successes() -> None:
    runtime, _run_store, _context = _runtime(FakeWorkerExecutor(fail_agent="factor_developer"))
    result = runtime.run(parent_task_id="task_2", objective="分析 QE 和因子", requested_agent_keys=["qe_experiment_designer", "factor_developer"])
    assert result.status == "completed_with_failures"
    statuses = {item.agent_key: item.status for item in result.worker_results}
    assert statuses == {"qe_experiment_designer": "succeeded", "factor_developer": "failed"}
    assert "factor_developer: failed" in result.assistant_text


def test_orchestrator_trace_declares_no_domain_tool_execution() -> None:
    runtime, _run_store, _context = _runtime(FakeWorkerExecutor())
    result = runtime.run(parent_task_id="task_3", objective="跨模块任务", requested_agent_keys=["qe_experiment_designer", "hmm_evolution"])
    assert result.trace[0]["event"] == "orchestrator_decomposed"
    assert result.trace[0]["orchestrator_does_domain_work"] is False
