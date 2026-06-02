from __future__ import annotations

from backend.services.research_assistant.qe_autonomy.models import EvolutionVerdict, LoopObservation
from backend.tests.research_assistant.test_qe_autonomy_fakes import request, runtime_with


def test_consecutive_no_sota_improvement_stops_and_reports() -> None:
    observations = [
        LoopObservation(1, {"IC": 0.01}, source_refs=("loop:1",), as_of="2026-06-02"),
        LoopObservation(2, {"IC": 0.011}, source_refs=("loop:2",), as_of="2026-06-02"),
    ]
    verdicts = [
        EvolutionVerdict(False, "not sota 1", "fake", source_refs=("verdict:1",), as_of="2026-06-02"),
        EvolutionVerdict(False, "not sota 2", "fake", source_refs=("verdict:2",), as_of="2026-06-02"),
    ]
    runtime, store, loop_executor, _evaluator, _decider, _generator, submitter = runtime_with(observations=observations, verdicts=verdicts)
    report = runtime.autonomous_evolve(request(stop_conditions={"max_no_improve_rounds": 2, "max_consecutive_failures": 1}))
    assert report.status == "stopped_no_improve"
    assert report.triggered_guard == "no_improve"
    assert report.loops_completed == 2
    assert "verdict:2" in report.evidence_refs
    assert submitter.executed_calls == 1
    assert loop_executor.calls == 2
    assert store.archived[report.auto_run_id]["status"] == "stopped_no_improve"


def test_target_reached_stops_before_submit() -> None:
    observations = [LoopObservation(1, {"IC": 0.08}, source_refs=("loop:target",), as_of="2026-06-02")]
    verdicts = [EvolutionVerdict(True, "target reached", "fake", metrics={"IC": 0.08}, target_reached=True, source_refs=("verdict:target",))]
    runtime, _store, _loop, _eval, decider, _generator, submitter = runtime_with(observations=observations, verdicts=verdicts)
    report = runtime.autonomous_evolve(request(stop_conditions={"target_metric": "IC", "target_threshold": 0.05, "max_consecutive_failures": 1}))
    assert report.status == "stopped_target"
    assert report.triggered_guard == "target_reached"
    assert decider.calls == 0
    assert submitter.calls == 0
