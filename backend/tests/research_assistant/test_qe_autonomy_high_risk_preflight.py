from __future__ import annotations

from unittest.mock import Mock

from backend.services.research_assistant.qe_autonomy.models import EvolutionVerdict, LoopObservation, SubmitDecision
from backend.tests.research_assistant.test_qe_autonomy_fakes import FixedGenerator, request, runtime_with


class PreflightOnlySubmitter:
    def __init__(self) -> None:
        self.calls = 0
        self.confirmed_executor = Mock()

    def submit_or_preflight_next_loop(self, proposal, state):
        self.calls += 1
        if proposal.is_high_cost():
            return SubmitDecision(
                status="approval_required",
                executed=False,
                approval_required=True,
                preflight={"approval_candidate": True, "proposal_id": proposal.proposal_id},
                source_refs=proposal.source_refs,
                reason="high cost QE run requires explicit confirmation",
            )
        self.confirmed_executor(proposal)
        return SubmitDecision(status="submitted", executed=True, source_refs=proposal.source_refs)


def test_high_cost_qe_proposal_creates_preflight_and_never_executes_unconfirmed_run() -> None:
    submitter = PreflightOnlySubmitter()
    runtime, _store, _loop, _eval, _decider, _generator, _submitter = runtime_with(
        observations=[LoopObservation(1, {"IC": 0.01}, source_refs=("loop:1",), as_of="2026-06-02")],
        verdicts=[EvolutionVerdict(False, "not enough", "fake", source_refs=("verdict:1",), as_of="2026-06-02")],
        generator=FixedGenerator(high_cost=True),
        submitter=submitter,
    )
    report = runtime.autonomous_evolve(request(stop_conditions={"max_no_improve_rounds": 5, "max_consecutive_failures": 1}))
    assert report.status == "failed"
    assert report.triggered_guard == "approval_required"
    assert report.submit_decisions[0]["approval_required"] is True
    assert report.submit_decisions[0]["executed"] is False
    assert report.submit_decisions[0]["preflight"]["approval_candidate"] is True
    submitter.confirmed_executor.assert_not_called()
