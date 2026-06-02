from __future__ import annotations

from datetime import datetime, timezone

from backend.services.research_assistant.qe_autonomy.adapter import ResearchAssistantQeAutonomyRunStore
from backend.services.research_assistant.qe_autonomy.models import AutonomousEvolutionState, EvolutionVerdict, LoopObservation
from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
from backend.tests.research_assistant.test_qe_autonomy_fakes import request, runtime_with


def test_runtime_create_update_read_archive_consumes_qe_autonomy_ledger() -> None:
    runtime, store, _loop, _eval, _decider, _generator, _submitter = runtime_with(
        observations=[LoopObservation(1, {"IC": 0.08}, source_refs=("loop:target",), as_of="2026-06-02")],
        verdicts=[EvolutionVerdict(True, "target reached", "fake", metrics={"IC": 0.08}, target_reached=True, source_refs=("verdict:target",))],
    )
    report = runtime.autonomous_evolve(request(stop_conditions={"target_metric": "IC", "target_threshold": 0.05, "max_consecutive_failures": 1}))
    assert ("create", report.auto_run_id) in store.events
    assert ("update", report.auto_run_id) in store.events
    assert ("archive", report.auto_run_id) in store.events
    row = store.get_run(report.auto_run_id)
    assert row and row["status"] == "stopped_target"
    assert store.archived[report.auto_run_id]["status"] == "stopped_target"


def test_repository_store_maps_to_qe_autonomous_evolution_runs_and_archives_report() -> None:
    repository = InMemoryResearchAssistantRepository()
    store = ResearchAssistantQeAutonomyRunStore(repository)
    state = AutonomousEvolutionState(
        auto_run_id="qaer-store",
        qe_task_id="qe-task-store",
        methodology_ref="methodology:store",
        stop_conditions={"max_no_improve_rounds": 1},
        budget={"max_loops": 1},
        started_at=datetime(2026, 6, 2, tzinfo=timezone.utc),
    )
    store.create_run(state)
    state.status = "stopped_budget"
    state.stop_reason = "max loops"
    store.update_run(state)
    assert store.get_run("qaer-store")["status"] == "stopped_budget"
    store.archive_report(state, {"status": "stopped_budget", "evidence_refs": ["e1"]})
    row = store.get_run("qaer-store")
    assert row["last_verdict_json"]["autonomy_report"]["status"] == "stopped_budget"
