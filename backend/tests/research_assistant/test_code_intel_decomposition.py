from __future__ import annotations

from pathlib import Path
from typing import Any

from backend.services.research_assistant.agent_teams.config import load_agent_teams_config
from backend.services.research_assistant.agent_teams.models import AgentTeamResult, WorkerRunResult
from backend.services.research_assistant.agent_teams.runtime import AgentTeamsRuntime, AgentTeamsRuntimeProviders
from backend.services.research_assistant.models import CodeContextManifest, CodeContextRef
from backend.services.research_assistant.models import TaskCreate
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.services.research_assistant.service import ResearchAssistantService
from backend.tests.research_assistant.test_agent_teams_parallel import (
    FakeCatalogProvider,
    FakeContextProvider,
    FakeCurator,
    FakeRunStore,
    FakeWorkerExecutor,
)


CONFIG_PATH = Path(__file__).resolve().parents[3] / "configs/research_assistant/agent_teams.yaml"


class FakeCodeProvider:
    def query_code_context(self, request):
        return CodeContextManifest(
            provider="fixture",
            query=request.query,
            status="ok",
            as_of="2026-06-02T10:00:00Z",
            refs=[
                CodeContextRef(
                    file_path="backend/services/research_assistant/service.py",
                    symbol="run_agent_team",
                    edge_refs=[{"edge_id": "edge-l3-consume"}],
                    provenance={"source": "fixture", "edge": "edge-l3-consume"},
                    as_of="2026-06-02T10:00:00Z",
                    summary="L3 orchestrator receives code refs from the context pack.",
                    summary_ref="tmp/summary.md",
                    detail_ref="tmp/detail.md",
                    call_chain=[{"from": "build_context_pack", "to": "run_agent_team"}],
                    impact_radius={"affected_test_count": 1},
                    affected_tests=[
                        {
                            "test_path": "backend/tests/research_assistant/test_code_intel_decomposition.py",
                            "classification": "recommended",
                            "source_ref": "tmp/affected-tests.json",
                        }
                    ],
                )
            ],
        )


def test_service_feeds_context_pack_code_refs_into_l3_worker_inputs(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(self, *, parent_task_id, objective, requested_agent_keys=None, worker_inputs=None):
        captured["worker_inputs"] = worker_inputs
        return AgentTeamResult(
            parent_task_id=parent_task_id,
            status="completed",
            assistant_text="ok",
            worker_results=(
                WorkerRunResult(
                    agent_run_id="run-qe",
                    parent_task_id=parent_task_id,
                    agent_key="qe_experiment_designer",
                    role="qe_research",
                    status="succeeded",
                    task_order=0,
                    summary="worker consumed code refs source=fixture as_of=2026-06-02",
                    result_json={"orchestrator_consumed_code_context_refs": True},
                ),
            ),
            reduce_json={"status": "completed", "assistant_text": "ok", "worker_results": []},
            trace=({"event": "orchestrator_decomposed", "orchestrator_consumed_code_context_refs": True},),
        )

    monkeypatch.setattr(AgentTeamsRuntime, "run", fake_run)
    svc = ResearchAssistantService(
        repository=InMemoryResearchAssistantRepository(),
        code_intelligence_provider=FakeCodeProvider(),
    )
    svc.seed_catalogs()
    task = svc.create_task(TaskCreate(title="L3 code context"))

    result = svc.run_agent_team(
        parent_task_id=task["task_id"],
        objective="Explain backend/services/research_assistant/service.py impact radius",
        requested_agent_keys=["qe_experiment_designer"],
    )

    worker_input = captured["worker_inputs"]["qe_experiment_designer"]
    assert result["trace"][0]["orchestrator_consumed_code_context_refs"] is True
    assert worker_input["orchestrator_consumed_code_context_refs"] is True
    assert worker_input["code_context_refs"][0]["summary_ref"] == "tmp/summary.md"
    assert worker_input["code_context_refs"][0]["detail_ref"] == "tmp/detail.md"
    assert worker_input["affected_tests_summary"][0]["classification"] == "recommended"


def test_agent_teams_runtime_marks_orchestrator_consumption_and_worker_input_without_adapter() -> None:
    executor = FakeWorkerExecutor()
    run_store = FakeRunStore()
    context = FakeContextProvider()
    runtime = AgentTeamsRuntime(
        config=load_agent_teams_config(CONFIG_PATH),
        providers=AgentTeamsRuntimeProviders(
            run_store=run_store,
            context_provider=context,
            worker_executor=executor,
            tool_catalog_provider=FakeCatalogProvider(),
            curator=FakeCurator(),
        ),
        id_factory=lambda task: f"run_{task.task_order:03d}_{task.agent_key}",
    )

    result = runtime.run(
        parent_task_id="task-runtime",
        objective="code task",
        requested_agent_keys=["qe_experiment_designer", "factor_developer"],
        worker_inputs={
            "qe_experiment_designer": {
                "code_context_refs": [{"as_of": "2026-06-02T10:00:00Z", "provenance": {"source": "fixture"}, "summary": "s", "summary_ref": "tmp/s.md", "detail_ref": "tmp/d.md", "affected_tests": []}]
            },
            "factor_developer": {
                "code_context_refs": [{"as_of": "2026-06-02T10:00:00Z", "provenance": {"source": "fixture"}, "summary": "s", "summary_ref": "tmp/s.md", "detail_ref": "tmp/d.md", "affected_tests": []}]
            },
        },
    )

    assert result.trace[0]["orchestrator_consumed_code_context_refs"] is True
    assert all(call["task"].input_json["code_context_refs"][0]["summary_ref"] == "tmp/s.md" for call in executor.calls)


def test_orchestrator_runtime_does_not_import_adapter_provider() -> None:
    runtime_source = (Path(__file__).resolve().parents[3] / "backend/services/research_assistant/agent_teams/runtime.py").read_text(encoding="utf-8")
    assert "code_intelligence_adapter_provider" not in runtime_source
    assert "scripts.code_intelligence_adapter" not in runtime_source
