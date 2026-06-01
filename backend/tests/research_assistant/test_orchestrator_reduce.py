from __future__ import annotations

from pathlib import Path

from backend.services.research_assistant.agent_teams.config import load_agent_teams_config
from backend.services.research_assistant.agent_teams.models import WorkerRunResult
from backend.services.research_assistant.agent_teams.runtime import AgentTeamsRuntime, AgentTeamsRuntimeProviders
from backend.tests.research_assistant.test_agent_teams_parallel import FakeCatalogProvider, FakeContextProvider, FakeCurator, FakeRunStore, FakeWorkerExecutor


CONFIG_PATH = Path(__file__).resolve().parents[3] / "configs/research_assistant/agent_teams.yaml"


def _make_runtime(executor: FakeWorkerExecutor) -> AgentTeamsRuntime:
    return AgentTeamsRuntime(
        config=load_agent_teams_config(CONFIG_PATH),
        providers=AgentTeamsRuntimeProviders(
            run_store=FakeRunStore(),
            context_provider=FakeContextProvider(),
            worker_executor=executor,
            tool_catalog_provider=FakeCatalogProvider(),
            curator=FakeCurator(),
        ),
        id_factory=lambda task: f"det_{task.task_order:03d}_{task.agent_key}",
    )


def test_reduce_is_byte_stable_for_completion_order_reversal_and_repeated_inputs() -> None:
    requested = ["qe_experiment_designer", "hmm_evolution", "factor_developer"]
    first = _make_runtime(FakeWorkerExecutor()).run(parent_task_id="task_reduce", objective="QE HMM 因子", requested_agent_keys=requested)
    reversed_order = _make_runtime(FakeWorkerExecutor()).run(parent_task_id="task_reduce", objective="QE HMM 因子", requested_agent_keys=list(reversed(requested)))
    repeated = _make_runtime(FakeWorkerExecutor()).run(parent_task_id="task_reduce", objective="QE HMM 因子", requested_agent_keys=requested)
    assert first.canonical_json() == reversed_order.canonical_json()
    assert first.canonical_json() == repeated.canonical_json()
    assert [item.agent_run_id for item in first.worker_results] == [
        "det_000_qe_experiment_designer",
        "det_001_hmm_evolution",
        "det_002_factor_developer",
    ]


def test_reduce_order_ignores_agent_run_id_tie_breakers() -> None:
    runtime = _make_runtime(FakeWorkerExecutor())
    unordered = [
        WorkerRunResult("zzz", "task", "factor_developer", "factor_developer", "succeeded", 30, "factor", evidence_refs=("e2",)),
        WorkerRunResult("aaa", "task", "qe_experiment_designer", "qe_experiment_designer", "succeeded", 10, "qe", evidence_refs=("e1",)),
    ]
    reduced = runtime.reduce(parent_task_id="task", objective="obj", results=unordered)
    assert [item["agent_key"] for item in reduced["worker_results"]] == ["qe_experiment_designer", "factor_developer"]
