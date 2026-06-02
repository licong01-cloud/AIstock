from __future__ import annotations

from backend.services.research_assistant.qe_autonomy.models import EvolutionVerdict, LoopObservation
from backend.tests.research_assistant.test_qe_autonomy_fakes import FixedGenerator, hypothesis, request, runtime_with


def test_external_hypothesis_with_source_becomes_low_cost_candidate_only() -> None:
    external = hypothesis()
    runtime, _store, _loop, _eval, _decider, _generator, submitter = runtime_with(
        observations=[
            LoopObservation(1, {"IC": 0.01}, source_refs=("loop:1",), as_of="2026-06-02"),
            LoopObservation(2, {"IC": 0.011}, source_refs=("loop:2",), as_of="2026-06-02"),
        ],
        verdicts=[
            EvolutionVerdict(False, "not sota", "fake", source_refs=("verdict:1",), as_of="2026-06-02"),
            EvolutionVerdict(False, "stop budget", "fake", source_refs=("verdict:2",), as_of="2026-06-02"),
        ],
    )
    report = runtime.autonomous_evolve(request(external_hypotheses=(external,), budget={"max_loops": 2, "max_total_seconds": 999, "max_gpu_occupancy_pct": 99}))
    assert report.proposals[0]["low_cost_intent"] is True
    assert report.proposals[0]["risk_level"] == "low"
    assert submitter.executed_calls == 1


def test_external_hypothesis_rejects_direct_high_cost_qe_run() -> None:
    external = hypothesis()
    runtime, _store, _loop, _eval, _decider, _generator, submitter = runtime_with(
        observations=[LoopObservation(1, {"IC": 0.01}, source_refs=("loop:1",), as_of="2026-06-02")],
        verdicts=[EvolutionVerdict(False, "not sota", "fake", source_refs=("verdict:1",), as_of="2026-06-02")],
        generator=FixedGenerator(high_cost=True),
    )
    try:
        runtime.autonomous_evolve(request(external_hypotheses=(external,)))
    except ValueError as exc:
        assert "external hypotheses" in str(exc)
    else:  # pragma: no cover - fail-fast branch
        raise AssertionError("external hypotheses must not directly schedule high-cost QE runs")
    assert submitter.calls == 0


def test_external_hypothesis_requires_provenance_source_and_as_of() -> None:
    from backend.services.research_assistant.qe_autonomy.models import ExternalHypothesisRef

    try:
        ExternalHypothesisRef(source_ref="", as_of="2026-06-02", hypothesis="h", low_cost_intent="intent", provenance={"source": "x"})
    except ValueError as exc:
        assert "source_ref" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("source_ref is required")
