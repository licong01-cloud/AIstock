from __future__ import annotations

import pytest

from backend.services.research_assistant.qe_autonomy.models import AutonomousEvolutionRequest, EvolutionVerdict, LoopObservation
from backend.tests.research_assistant.test_qe_autonomy_fakes import request, runtime_with


def test_disabled_autonomy_does_not_create_run_or_call_loop_executor() -> None:
    runtime, store, loop_executor, _evaluator, _decider, _generator, submitter = runtime_with(
        observations=[LoopObservation(1, {"IC": 0.01})],
        verdicts=[EvolutionVerdict(False, "unused", "fake")],
    )
    report = runtime.autonomous_evolve(request(enabled=False, stop_conditions={}, budget={}))
    assert report.status == "disabled"
    assert loop_executor.calls == 0
    assert submitter.calls == 0
    assert store.rows == {}


@pytest.mark.parametrize("field", ["stop_conditions", "budget"])
def test_enabled_autonomy_requires_boundaries(field: str) -> None:
    runtime, *_ = runtime_with(
        observations=[LoopObservation(1, {"IC": 0.01})],
        verdicts=[EvolutionVerdict(False, "unused", "fake")],
    )
    payload = {
        "enabled": True,
        "qe_task_id": "qe-task-1",
        "methodology_ref": "methodology:unit",
        "stop_conditions": {"max_no_improve_rounds": 1},
        "budget": {"max_loops": 1},
    }
    payload[field] = {}
    with pytest.raises(ValueError):
        runtime.autonomous_evolve(AutonomousEvolutionRequest(**payload))
