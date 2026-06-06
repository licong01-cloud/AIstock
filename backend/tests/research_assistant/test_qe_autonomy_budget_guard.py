from __future__ import annotations

from backend.services.research_assistant.qe_autonomy.models import EvolutionVerdict, LoopObservation
from backend.tests.research_assistant.test_qe_autonomy_fakes import ScriptClock, request, runtime_with


def test_max_loops_budget_stops_before_next_submit() -> None:
    runtime, _store, _loop, _eval, decider, _generator, submitter = runtime_with(
        observations=[LoopObservation(1, {"IC": 0.01}, source_refs=("loop:1",))],
        verdicts=[EvolutionVerdict(False, "not sota", "fake", source_refs=("verdict:1",))],
    )
    report = runtime.autonomous_evolve(request(budget={"max_loops": 1, "max_total_seconds": 999, "max_gpu_occupancy_pct": 99}))
    assert report.status == "stopped_budget"
    assert report.triggered_guard == "budget_max_loops"
    assert decider.calls == 0
    assert submitter.calls == 0


def test_elapsed_budget_uses_injected_clock_and_stops() -> None:
    clock = ScriptClock([0, 0, 15, 15])
    runtime, *_items, submitter = runtime_with(
        observations=[LoopObservation(1, {"IC": 0.01}, source_refs=("loop:elapsed",))],
        verdicts=[EvolutionVerdict(False, "not sota", "fake", source_refs=("verdict:elapsed",))],
        clock=clock,
    )
    report = runtime.autonomous_evolve(request(budget={"max_loops": 5, "max_total_seconds": 10, "max_gpu_occupancy_pct": 99}))
    assert report.status == "stopped_budget"
    assert report.triggered_guard == "budget_max_elapsed"
    assert submitter.calls == 0


def test_gpu_budget_uses_observation_value_and_stops() -> None:
    runtime, *_items, submitter = runtime_with(
        observations=[LoopObservation(1, {"IC": 0.01}, source_refs=("loop:gpu",), gpu_occupancy_pct=96.0)],
        verdicts=[EvolutionVerdict(False, "not sota", "fake", source_refs=("verdict:gpu",))],
    )
    report = runtime.autonomous_evolve(request(budget={"max_loops": 5, "max_total_seconds": 999, "max_gpu_occupancy_pct": 95}))
    assert report.status == "stopped_budget"
    assert report.triggered_guard == "budget_max_gpu"
    assert submitter.calls == 0
