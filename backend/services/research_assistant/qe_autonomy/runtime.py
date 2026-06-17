from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .guards import evaluate_budget, evaluate_stop_conditions
from .models import (
    AutonomousEvolutionRequest,
    AutonomousEvolutionState,
    AutonomyBudgetUsage,
    AutonomyReport,
    LoopProposal,
    StopDecision,
)
from .providers import (
    AutonomyRunStore,
    ClockProvider,
    DirectionDeciderProvider,
    EvaluatorProvider,
    ExperienceReplayProvider,
    IdFactory,
    LoopConfigGeneratorProvider,
    LoopExecutorProvider,
    LoopSubmitterProvider,
)


@dataclass(frozen=True)
class AutonomousEvolutionProviders:
    run_store: AutonomyRunStore
    loop_executor: LoopExecutorProvider
    evaluator: EvaluatorProvider
    direction_decider: DirectionDeciderProvider
    config_generator: LoopConfigGeneratorProvider
    submitter: LoopSubmitterProvider


def default_clock() -> datetime:
    return datetime.now(timezone.utc)


def default_id_factory(prefix: str, stable_key: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in stable_key)[:96]
    return f"{prefix}_{cleaned}"


class AutonomousEvolutionRuntime:
    def __init__(
        self,
        *,
        providers: AutonomousEvolutionProviders,
        experience_replay_provider: ExperienceReplayProvider | None = None,
        clock: ClockProvider | None = None,
        id_factory: IdFactory | None = None,
    ) -> None:
        self.providers = providers
        self.experience_replay_provider = experience_replay_provider
        self.clock = clock or default_clock
        self.id_factory = id_factory or default_id_factory

    def autonomous_evolve(self, request: AutonomousEvolutionRequest) -> AutonomyReport:
        request.validate_for_run()
        auto_run_id = request.auto_run_id or self.id_factory("qaer", request.qe_task_id)
        if not request.enabled:
            return AutonomyReport(
                auto_run_id=auto_run_id,
                qe_task_id=request.qe_task_id,
                status="disabled",
                loops_completed=0,
                stop_reason="autonomous evolution disabled",
                triggered_guard="disabled",
                last_verdict_json={},
                budget_usage=AutonomyBudgetUsage(0, 0.0, None),
                evidence_refs=(),
            )
        state = AutonomousEvolutionState(
            auto_run_id=auto_run_id,
            qe_task_id=request.qe_task_id,
            methodology_ref=request.methodology_ref,
            stop_conditions=dict(request.stop_conditions),
            budget=dict(request.budget),
            started_at=self.clock(),
            external_hypotheses=tuple(request.external_hypotheses),
        )
        self.providers.run_store.create_run(state)
        while True:
            pre_budget = evaluate_budget(state, now=self.clock())
            if pre_budget.should_stop:
                self._stop(state, pre_budget)
                break
            observation = self.providers.loop_executor.run_or_wait_loop_n(state)
            state.record_observation(observation)
            if observation.failure or observation.data_gap:
                self._stop(state, StopDecision(True, "failed", observation.failure_reason or "loop observation failed", "failure_or_data_gap"))
                break
            verdict = self.providers.evaluator.evaluate_loop(observation, state)
            state.record_verdict(verdict)
            stop_decision = evaluate_stop_conditions(state, verdict)
            if stop_decision.should_stop:
                self._stop(state, stop_decision)
                break
            budget_decision = evaluate_budget(state, now=self.clock())
            if budget_decision.should_stop:
                self._stop(state, budget_decision)
                break
            direction = self.providers.direction_decider.decide_direction(verdict, state)
            state.last_direction = direction
            state.evidence_refs.update(direction.evidence_refs)
            proposal = self.providers.config_generator.generate_next_config(direction, state)
            self._validate_low_cost_external_boundary(request, proposal)
            state.proposals.append(proposal)
            state.evidence_refs.update(proposal.source_refs)
            state.artifact_refs.update(str(ref) for ref in proposal.artifact_refs)
            submit_decision = self.providers.submitter.submit_or_preflight_next_loop(proposal, state)
            state.submit_decisions.append(submit_decision)
            state.evidence_refs.update(submit_decision.source_refs)
            state.artifact_refs.update(str(ref) for ref in submit_decision.artifact_refs)
            if submit_decision.approval_required:
                self._stop(state, StopDecision(True, "failed", submit_decision.reason or "approval required", "approval_required"))
                break
            self.providers.run_store.update_run(state)
        report = self._build_report(state, now=self.clock())
        self.providers.run_store.archive_report(state, report.to_dict())
        return report

    def _stop(self, state: AutonomousEvolutionState, decision: StopDecision) -> None:
        state.status = decision.status
        state.stop_reason = decision.reason
        state.triggered_guard = decision.triggered_guard
        self.providers.run_store.update_run(state)

    def _build_report(self, state: AutonomousEvolutionState, *, now: datetime) -> AutonomyReport:
        evidence_refs = tuple(sorted(str(ref) for ref in state.evidence_refs if ref))
        artifact_refs = tuple(sorted(str(ref) for ref in state.artifact_refs if ref))
        curriculum_replay = self._curriculum_replay(state, evidence_refs)
        memory_candidates = ()
        if evidence_refs and state.status not in {"disabled", "running"}:
            memory_candidates = (
                {
                    "approval_status": "draft",
                    "content_text": f"QE autonomy {state.status}: {state.stop_reason}",
                    "memory_type": "analysis_note",
                    "provenance_json": {"auto_run_id": state.auto_run_id, "evidence_refs": list(evidence_refs), "source": "qe_autonomy"},
                    "tree_path": "personal.task.qe_autonomy_progress",
                },
            )
        return AutonomyReport(
            auto_run_id=state.auto_run_id,
            qe_task_id=state.qe_task_id,
            status=state.status,
            loops_completed=state.loops_completed,
            stop_reason=state.stop_reason,
            triggered_guard=state.triggered_guard,
            last_verdict_json=state.last_verdict_json(),
            budget_usage=state.budget_usage(now),
            evidence_refs=evidence_refs,
            artifact_refs=artifact_refs,
            proposals=tuple(proposal.to_dict() for proposal in sorted(state.proposals, key=lambda item: item.sorted_key())),
            submit_decisions=tuple(decision.to_dict() for decision in state.submit_decisions),
            memory_candidates=memory_candidates,
            curriculum_replay=curriculum_replay,
        )

    def _curriculum_replay(self, state: AutonomousEvolutionState, evidence_refs: tuple[str, ...]) -> tuple[dict[str, Any], ...]:
        if not self.experience_replay_provider:
            return ()
        try:
            items = self.experience_replay_provider.find_reusable_skills(
                task_key=state.qe_task_id,
                evidence_refs=list(evidence_refs),
                limit=5,
            )
        except Exception as exc:  # explicit degradation: replay must not fail the QE autonomy report.
            return (
                {
                    "schema_version": "aistock_research_assistant_skill_replay_v1",
                    "status": "degraded",
                    "reason_codes": ["skill_library_curriculum_replay_failed"],
                    "warnings": [f"skill library replay failed: {type(exc).__name__}: {exc}"],
                    "source_refs": list(evidence_refs),
                },
            )
        return tuple(dict(item) for item in items)

    @staticmethod
    def _validate_low_cost_external_boundary(request: AutonomousEvolutionRequest, proposal: LoopProposal) -> None:
        if request.external_hypotheses and proposal.is_high_cost():
            raise ValueError("external hypotheses may only create low-cost validation candidates before approval")
