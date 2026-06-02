from __future__ import annotations

import asyncio
import inspect
from typing import Any, Awaitable, Callable

from backend.services.quantevolver.qe_evolution_agents import EvolutionAgents
from backend.services.quantevolver.qe_evolution_service import AutoEvolutionScheduler

from .models import (
    AutonomousEvolutionState,
    EvolutionDirection,
    EvolutionVerdict,
    LoopObservation,
    LoopProposal,
    SubmitDecision,
)

PreflightCallback = Callable[[str], Awaitable[dict[str, Any]] | dict[str, Any]]


def _run_sync(value: Awaitable[Any] | Any) -> Any:
    if not inspect.isawaitable(value):
        return value
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(value)  # type: ignore[arg-type]
    raise RuntimeError("QeAutonomyAdapter cannot run async QE callbacks inside an already running loop")


class QeAutonomyAdapter:
    """AIstock QE adapter for the provider-only autonomy runtime."""

    def __init__(
        self,
        *,
        scheduler: AutoEvolutionScheduler | None = None,
        agents: EvolutionAgents | None = None,
        preflight_callback: PreflightCallback | None = None,
    ) -> None:
        self.scheduler = scheduler or AutoEvolutionScheduler()
        self.agents = agents or getattr(self.scheduler, "agents", None) or EvolutionAgents()
        self.preflight_callback = preflight_callback

    def run_or_wait_loop_n(self, state: AutonomousEvolutionState) -> LoopObservation:
        metrics_provider = getattr(self.scheduler, "get_latest_loop_metrics", None)
        if callable(metrics_provider):
            raw = metrics_provider(state.qe_task_id)
        else:
            raw = {"loop_index": state.loops_completed + 1, "metrics": {}, "source_refs": [f"qe_task:{state.qe_task_id}"]}
        if raw is None:
            return LoopObservation(
                loop_index=state.loops_completed + 1,
                metrics={},
                source_refs=(f"qe_task:{state.qe_task_id}",),
                failure=True,
                failure_reason="qe loop metrics unavailable",
            )
        loop_index = int(raw.get("loop_index") or state.loops_completed + 1)
        return LoopObservation(
            loop_index=loop_index,
            metrics=dict(raw.get("metrics") or {}),
            source_refs=tuple(str(ref) for ref in raw.get("source_refs") or [f"qe_task:{state.qe_task_id}:loop:{loop_index}"]),
            artifact_refs=tuple(str(ref) for ref in raw.get("artifact_refs") or []),
            as_of=str(raw["as_of"]) if raw.get("as_of") else None,
            gpu_occupancy_pct=float(raw["gpu_occupancy_pct"]) if raw.get("gpu_occupancy_pct") is not None else None,
            data_gap=bool(raw.get("data_gap", False)),
            failure=bool(raw.get("failure", False)),
            failure_reason=str(raw.get("failure_reason") or ""),
        )

    def evaluate_loop(self, observation: LoopObservation, state: AutonomousEvolutionState) -> EvolutionVerdict:
        historical_sota = {}
        history_provider = getattr(self.scheduler, "_build_full_evolution_history", None)
        evolution_history = history_provider(state.qe_task_id) if callable(history_provider) else {}
        result = _run_sync(self.agents.run_evaluator(observation.metrics, historical_sota, evolution_history=evolution_history))
        return EvolutionVerdict(
            is_sota=bool(result.get("is_sota", False)),
            reason=str(result.get("reason") or "qe evaluator verdict"),
            method=str(result.get("method") or "unknown"),
            metrics=dict(observation.metrics),
            target_reached=bool(result.get("target_reached", False)),
            data_gap=observation.data_gap,
            failure=observation.failure,
            source_refs=tuple(sorted(set(observation.source_refs) | {f"qe_evaluator:{state.qe_task_id}"})),
            as_of=observation.as_of,
        )

    def decide_direction(self, verdict: EvolutionVerdict, state: AutonomousEvolutionState) -> EvolutionDirection:
        config_provider = getattr(self.scheduler, "get_current_loop_config", None)
        config = config_provider(state.qe_task_id) if callable(config_provider) else {}
        history_provider = getattr(self.scheduler, "_build_full_evolution_history", None)
        evolution_history = history_provider(state.qe_task_id) if callable(history_provider) else {}
        analyst_result = _run_sync(
            self.agents.run_analyst(
                state.loops_completed,
                config,
                verdict.metrics,
                analysis_context={"evolution_mode": "autonomous"},
                evolution_history=evolution_history,
            )
        )
        direction = getattr(analyst_result, "direction", {}) or {}
        return EvolutionDirection(
            action_type=str(direction.get("action_type") or direction.get("direction") or "low_cost_validation"),
            rationale=str(direction.get("rationale") or getattr(analyst_result, "report_text", "QE analyst direction")),
            config_delta=dict(direction.get("config_delta") or direction),
            evidence_refs=(f"qe_analyst:{state.qe_task_id}",),
        )

    def generate_next_config(self, direction: EvolutionDirection, state: AutonomousEvolutionState) -> LoopProposal:
        proposal_id = f"qe-loop-{state.loops_completed + 1:03d}-{direction.action_type}"
        external_hypotheses = [item.to_dict() for item in state.external_hypotheses]
        external_refs = {item.source_ref for item in state.external_hypotheses}
        return LoopProposal(
            proposal_id=proposal_id,
            loop_index=state.loops_completed + 1,
            config_json={
                "action_type": direction.action_type,
                "config_delta": direction.config_delta,
                "external_hypotheses": external_hypotheses,
                "qe_task_id": state.qe_task_id,
            },
            risk_level="low",
            side_effect_level="read_only",
            requires_confirmation=False,
            confirmed_tool_name=None,
            source_refs=tuple(sorted(set(direction.evidence_refs) | {f"qe_task:{state.qe_task_id}"} | external_refs)),
            low_cost_intent=True,
            provenance={"source": "qe_autonomy_adapter", "methodology_ref": state.methodology_ref},
        )

    def submit_or_preflight_next_loop(self, proposal: LoopProposal, state: AutonomousEvolutionState) -> SubmitDecision:
        if proposal.is_high_cost():
            return SubmitDecision(
                status="approval_required",
                executed=False,
                approval_required=True,
                preflight={"proposal_id": proposal.proposal_id, "confirmed_tool_name": proposal.confirmed_tool_name},
                source_refs=proposal.source_refs,
                artifact_refs=proposal.artifact_refs,
                reason="high cost QE run requires explicit confirmation",
            )
        node_id = str(proposal.config_json.get("node_id") or state.qe_task_id)
        preflight = self._run_preflight(node_id)
        submitted = _run_sync(self.scheduler.submit_next_loop(state.qe_task_id))
        return SubmitDecision(
            status="submitted" if submitted else "no_submission",
            executed=bool(submitted),
            approval_required=False,
            submitted_loop_id=str(submitted) if submitted else None,
            preflight=preflight,
            source_refs=proposal.source_refs,
            artifact_refs=proposal.artifact_refs,
            reason="submitted via AutoEvolutionScheduler.submit_next_loop" if submitted else "scheduler returned no next loop",
        )

    def _run_preflight(self, node_id: str) -> dict[str, Any]:
        if self.preflight_callback is not None:
            result = _run_sync(self.preflight_callback(node_id))
            return dict(result or {})
        from backend.services.quantevolver.node_execution import preflight_qe_node

        result = _run_sync(preflight_qe_node(node_id))
        return dict(result or {})


class ResearchAssistantQeAutonomyRunStore:
    def __init__(self, repository: Any) -> None:
        self.repository = repository

    def create_run(self, state: AutonomousEvolutionState) -> None:
        self.repository.create_record(
            "qe_autonomy_runs",
            {
                "auto_run_id": state.auto_run_id,
                "qe_task_id": state.qe_task_id,
                "methodology_ref": state.methodology_ref,
                "stop_conditions_json": state.stop_conditions,
                "budget_json": state.budget,
                "status": state.status,
                "loops_completed": state.loops_completed,
                "last_verdict_json": state.last_verdict_json(),
            },
        )

    def update_run(self, state: AutonomousEvolutionState) -> None:
        self.repository.update_record(
            "qe_autonomy_runs",
            state.auto_run_id,
            {
                "status": state.status,
                "loops_completed": state.loops_completed,
                "last_verdict_json": state.last_verdict_json(),
            },
        )

    def get_run(self, auto_run_id: str) -> dict[str, object] | None:
        return self.repository.get_record("qe_autonomy_runs", auto_run_id)

    def archive_report(self, state: AutonomousEvolutionState, report: dict[str, object]) -> None:
        current = self.get_run(state.auto_run_id)
        if current is None:
            raise KeyError(f"qe autonomy run not found: {state.auto_run_id}")
        last_verdict = dict(state.last_verdict_json())
        last_verdict["autonomy_report"] = report
        self.repository.update_record("qe_autonomy_runs", state.auto_run_id, {"last_verdict_json": last_verdict})
