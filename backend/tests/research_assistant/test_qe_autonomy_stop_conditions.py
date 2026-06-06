from __future__ import annotations

from backend.services.research_assistant.qe_autonomy.models import EvolutionVerdict, LoopObservation
from backend.tests.research_assistant.test_qe_autonomy_fakes import request, runtime_with


def test_data_gap_fails_fast_without_silent_retry() -> None:
    observations = [LoopObservation(1, {}, source_refs=("loop:data-gap",), data_gap=True, failure_reason="missing labels")]
    runtime, _store, loop_executor, evaluator, _decider, _generator, submitter = runtime_with(
        observations=observations,
        verdicts=[EvolutionVerdict(False, "unused", "fake")],
    )
    report = runtime.autonomous_evolve(request())
    assert report.status == "failed"
    assert report.triggered_guard == "failure_or_data_gap"
    assert "missing labels" in report.stop_reason
    assert loop_executor.calls == 1
    assert evaluator.calls == 0
    assert submitter.calls == 0


def test_verdict_failure_fails_fast() -> None:
    observations = [LoopObservation(1, {"IC": 0.01}, source_refs=("loop:failure",))]
    verdicts = [EvolutionVerdict(False, "evaluator failed", "fake", failure=True, source_refs=("verdict:failure",))]
    runtime, _store, _loop, _eval, decider, _generator, submitter = runtime_with(observations=observations, verdicts=verdicts)
    report = runtime.autonomous_evolve(request())
    assert report.status == "failed"
    assert report.triggered_guard == "failure_or_data_gap"
    assert decider.calls == 0
    assert submitter.calls == 0
