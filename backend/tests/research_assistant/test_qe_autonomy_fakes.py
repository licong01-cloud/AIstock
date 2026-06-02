from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any

from backend.services.research_assistant.qe_autonomy.models import (
    AutonomousEvolutionRequest,
    AutonomousEvolutionState,
    EvolutionDirection,
    EvolutionVerdict,
    ExternalHypothesisRef,
    LoopObservation,
    LoopProposal,
    SubmitDecision,
)
from backend.services.research_assistant.qe_autonomy.runtime import AutonomousEvolutionProviders, AutonomousEvolutionRuntime


class ScriptClock:
    def __init__(self, offsets: list[float] | None = None) -> None:
        self.base = datetime(2026, 6, 2, 0, 0, 0, tzinfo=timezone.utc)
        self.offsets = offsets or [0.0]
        self.calls = 0

    def __call__(self) -> datetime:
        idx = min(self.calls, len(self.offsets) - 1)
        self.calls += 1
        return self.base + timedelta(seconds=self.offsets[idx])


class MemoryRunStore:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}
        self.events: list[tuple[str, str]] = []
        self.archived: dict[str, dict[str, Any]] = {}

    def create_run(self, state: AutonomousEvolutionState) -> None:
        self.events.append(("create", state.auto_run_id))
        self.rows[state.auto_run_id] = {
            "auto_run_id": state.auto_run_id,
            "qe_task_id": state.qe_task_id,
            "status": state.status,
            "loops_completed": state.loops_completed,
            "last_verdict_json": state.last_verdict_json(),
        }

    def update_run(self, state: AutonomousEvolutionState) -> None:
        self.events.append(("update", state.auto_run_id))
        self.rows.setdefault(state.auto_run_id, {})
        self.rows[state.auto_run_id].update(
            {"status": state.status, "loops_completed": state.loops_completed, "last_verdict_json": state.last_verdict_json()}
        )

    def get_run(self, auto_run_id: str) -> dict[str, object] | None:
        self.events.append(("get", auto_run_id))
        return self.rows.get(auto_run_id)

    def archive_report(self, state: AutonomousEvolutionState, report: dict[str, object]) -> None:
        self.events.append(("archive", state.auto_run_id))
        self.archived[state.auto_run_id] = report


class ScriptLoopExecutor:
    def __init__(self, observations: list[LoopObservation]) -> None:
        self.observations = observations
        self.calls = 0

    def run_or_wait_loop_n(self, state: AutonomousEvolutionState) -> LoopObservation:
        self.calls += 1
        return self.observations[min(self.calls - 1, len(self.observations) - 1)]


class ScriptEvaluator:
    def __init__(self, verdicts: list[EvolutionVerdict]) -> None:
        self.verdicts = verdicts
        self.calls = 0

    def evaluate_loop(self, observation: LoopObservation, state: AutonomousEvolutionState) -> EvolutionVerdict:
        self.calls += 1
        verdict = self.verdicts[min(self.calls - 1, len(self.verdicts) - 1)]
        if not verdict.metrics:
            return EvolutionVerdict(
                is_sota=verdict.is_sota,
                reason=verdict.reason,
                method=verdict.method,
                metrics=observation.metrics,
                target_reached=verdict.target_reached,
                data_gap=verdict.data_gap,
                failure=verdict.failure,
                source_refs=verdict.source_refs,
                as_of=verdict.as_of,
            )
        return verdict


class FixedDecider:
    def __init__(self) -> None:
        self.calls = 0

    def decide_direction(self, verdict: EvolutionVerdict, state: AutonomousEvolutionState) -> EvolutionDirection:
        self.calls += 1
        return EvolutionDirection("low_cost_validation", "deterministic direction", {"delta": self.calls}, ("direction:1",))


class FixedGenerator:
    def __init__(self, *, high_cost: bool = False) -> None:
        self.high_cost = high_cost
        self.calls = 0

    def generate_next_config(self, direction: EvolutionDirection, state: AutonomousEvolutionState) -> LoopProposal:
        self.calls += 1
        return LoopProposal(
            proposal_id=f"proposal-{self.calls:03d}",
            loop_index=state.loops_completed + 1,
            config_json={"direction": direction.action_type, "source": "unit"},
            risk_level="high" if self.high_cost else "low",
            side_effect_level="high_cost_compute" if self.high_cost else "read_only",
            requires_confirmation=self.high_cost,
            confirmed_tool_name="qe_template_run_confirmed" if self.high_cost else None,
            source_refs=("proposal:source",),
            low_cost_intent=not self.high_cost,
            provenance={"source": "test"},
        )


class RecordingSubmitter:
    def __init__(self, *, approval_only: bool = False) -> None:
        self.approval_only = approval_only
        self.calls = 0
        self.executed_calls = 0

    def submit_or_preflight_next_loop(self, proposal: LoopProposal, state: AutonomousEvolutionState) -> SubmitDecision:
        self.calls += 1
        if self.approval_only or proposal.is_high_cost():
            return SubmitDecision(
                status="approval_required",
                executed=False,
                approval_required=True,
                preflight={"proposal_id": proposal.proposal_id},
                source_refs=proposal.source_refs,
                reason="approval required",
            )
        self.executed_calls += 1
        return SubmitDecision(
            status="submitted",
            executed=True,
            submitted_loop_id=f"loop-{self.calls:03d}",
            source_refs=proposal.source_refs,
            reason="submitted",
        )


def request(**overrides: Any) -> AutonomousEvolutionRequest:
    payload = {
        "enabled": True,
        "qe_task_id": "qe-task-1",
        "methodology_ref": "methodology:unit",
        "stop_conditions": {"max_no_improve_rounds": 5, "max_consecutive_failures": 1},
        "budget": {"max_loops": 5, "max_total_seconds": 999, "max_gpu_occupancy_pct": 95},
        "external_hypotheses": (),
    }
    payload.update(overrides)
    return AutonomousEvolutionRequest(**payload)


def hypothesis() -> ExternalHypothesisRef:
    return ExternalHypothesisRef(
        source_ref="external:paper:1",
        as_of="2026-06-02",
        hypothesis="try a low-cost factor validation",
        low_cost_intent="validate on archived runs first",
        provenance={"source": "external_research"},
    )


def runtime_with(
    *,
    observations: list[LoopObservation],
    verdicts: list[EvolutionVerdict],
    generator: FixedGenerator | None = None,
    submitter: RecordingSubmitter | None = None,
    clock: ScriptClock | None = None,
) -> tuple[AutonomousEvolutionRuntime, MemoryRunStore, ScriptLoopExecutor, ScriptEvaluator, FixedDecider, FixedGenerator, RecordingSubmitter]:
    store = MemoryRunStore()
    loop_executor = ScriptLoopExecutor(observations)
    evaluator = ScriptEvaluator(verdicts)
    decider = FixedDecider()
    generator = generator or FixedGenerator()
    submitter = submitter or RecordingSubmitter()
    runtime = AutonomousEvolutionRuntime(
        providers=AutonomousEvolutionProviders(
            run_store=store,
            loop_executor=loop_executor,
            evaluator=evaluator,
            direction_decider=decider,
            config_generator=generator,
            submitter=submitter,
        ),
        clock=clock or ScriptClock([0, 1, 2, 3, 4, 5]),
        id_factory=lambda prefix, stable_key: f"{prefix}_{stable_key}",
    )
    return runtime, store, loop_executor, evaluator, decider, generator, submitter
