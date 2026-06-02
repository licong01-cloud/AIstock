from __future__ import annotations

from backend.services.research_assistant.qe_autonomy.models import EvolutionVerdict, LoopObservation
from backend.tests.research_assistant.test_qe_autonomy_fakes import request, runtime_with


def _run_once() -> str:
    runtime, *_ = runtime_with(
        observations=[
            LoopObservation(1, {"IC": 0.01}, source_refs=("loop:1",), as_of="2026-06-02", gpu_occupancy_pct=20.0),
            LoopObservation(2, {"IC": 0.011}, source_refs=("loop:2",), as_of="2026-06-02", gpu_occupancy_pct=21.0),
        ],
        verdicts=[
            EvolutionVerdict(False, "not sota 1", "fake", source_refs=("verdict:1",), as_of="2026-06-02"),
            EvolutionVerdict(False, "not sota 2", "fake", source_refs=("verdict:2",), as_of="2026-06-02"),
        ],
    )
    report = runtime.autonomous_evolve(
        request(
            auto_run_id="qaer-byte-stable",
            stop_conditions={"max_no_improve_rounds": 2, "max_consecutive_failures": 1},
            budget={"max_loops": 5, "max_total_seconds": 999, "max_gpu_occupancy_pct": 95},
        )
    )
    return report.canonical_json()


def test_autonomy_report_is_byte_identical_for_repeated_same_input() -> None:
    first = _run_once()
    second = _run_once()
    assert first == second
    assert "qaer-byte-stable" in first
