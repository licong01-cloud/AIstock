from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

AutonomyStatus = Literal[
    "disabled",
    "running",
    "stopped_target",
    "stopped_no_improve",
    "stopped_budget",
    "failed",
]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _compact(value: Any, *, max_items: int = 12) -> Any:
    if isinstance(value, dict):
        return {str(k): _compact(v, max_items=max_items) for k, v in sorted(value.items())[:max_items]}
    if isinstance(value, (list, tuple)):
        return [_compact(v, max_items=max_items) for v in list(value)[:max_items]]
    if isinstance(value, datetime):
        return value.isoformat()
    return value


@dataclass(frozen=True)
class ExternalHypothesisRef:
    source_ref: str
    as_of: str
    hypothesis: str
    low_cost_intent: str
    provenance: dict[str, Any]

    def __post_init__(self) -> None:
        if not self.source_ref or not self.as_of or not self.hypothesis or not self.provenance:
            raise ValueError("external hypothesis requires source_ref, as_of, hypothesis, and provenance")

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "hypothesis": self.hypothesis,
            "low_cost_intent": self.low_cost_intent,
            "provenance": _compact(self.provenance),
            "source_ref": self.source_ref,
        }


@dataclass(frozen=True)
class AutonomousEvolutionRequest:
    enabled: bool
    qe_task_id: str
    methodology_ref: str | None
    stop_conditions: dict[str, Any]
    budget: dict[str, Any]
    external_hypotheses: tuple[ExternalHypothesisRef, ...] = ()
    auto_run_id: str | None = None

    def validate_for_run(self) -> None:
        if not self.qe_task_id:
            raise ValueError("qe_task_id is required")
        if self.enabled and not self.stop_conditions:
            raise ValueError("enabled autonomous evolution requires stop_conditions")
        if self.enabled and not self.budget:
            raise ValueError("enabled autonomous evolution requires budget")


@dataclass(frozen=True)
class LoopObservation:
    loop_index: int
    metrics: dict[str, Any]
    source_refs: tuple[str, ...] = ()
    artifact_refs: tuple[str, ...] = ()
    as_of: str | None = None
    gpu_occupancy_pct: float | None = None
    data_gap: bool = False
    failure: bool = False
    failure_reason: str = ""

    def to_prompt_safe_dict(self) -> dict[str, Any]:
        return {
            "artifact_refs": list(self.artifact_refs),
            "as_of": self.as_of,
            "data_gap": self.data_gap,
            "failure": self.failure,
            "failure_reason": self.failure_reason,
            "gpu_occupancy_pct": self.gpu_occupancy_pct,
            "loop_index": self.loop_index,
            "metrics": _compact(self.metrics),
            "source_refs": list(self.source_refs),
        }


@dataclass(frozen=True)
class EvolutionVerdict:
    is_sota: bool
    reason: str
    method: str
    metrics: dict[str, Any] = field(default_factory=dict)
    target_reached: bool = False
    data_gap: bool = False
    failure: bool = False
    source_refs: tuple[str, ...] = ()
    as_of: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "data_gap": self.data_gap,
            "failure": self.failure,
            "is_sota": self.is_sota,
            "method": self.method,
            "metrics": _compact(self.metrics),
            "reason": self.reason,
            "source_refs": list(self.source_refs),
            "target_reached": self.target_reached,
        }


@dataclass(frozen=True)
class EvolutionDirection:
    action_type: str
    rationale: str
    config_delta: dict[str, Any] = field(default_factory=dict)
    evidence_refs: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "action_type": self.action_type,
            "config_delta": _compact(self.config_delta),
            "evidence_refs": list(self.evidence_refs),
            "rationale": self.rationale,
        }


@dataclass(frozen=True)
class LoopProposal:
    proposal_id: str
    loop_index: int
    config_json: dict[str, Any]
    risk_level: str = "low"
    side_effect_level: str = "read_only"
    requires_confirmation: bool = False
    confirmed_tool_name: str | None = None
    source_refs: tuple[str, ...] = ()
    artifact_refs: tuple[str, ...] = ()
    low_cost_intent: bool = True
    provenance: dict[str, Any] = field(default_factory=dict)

    def sorted_key(self) -> tuple[int, str, str]:
        first_ref = self.source_refs[0] if self.source_refs else ""
        return (self.loop_index, self.proposal_id, first_ref)

    def is_high_cost(self) -> bool:
        return (
            self.requires_confirmation
            or self.risk_level in {"high", "production_sensitive"}
            or self.side_effect_level in {"high_cost_compute", "production_sensitive", "write_nonprod"}
            or bool(self.confirmed_tool_name and self.confirmed_tool_name.endswith("_confirmed"))
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_refs": list(self.artifact_refs),
            "confirmed_tool_name": self.confirmed_tool_name,
            "config_json": _compact(self.config_json),
            "loop_index": self.loop_index,
            "low_cost_intent": self.low_cost_intent,
            "proposal_id": self.proposal_id,
            "provenance": _compact(self.provenance),
            "requires_confirmation": self.requires_confirmation,
            "risk_level": self.risk_level,
            "side_effect_level": self.side_effect_level,
            "source_refs": list(self.source_refs),
        }


@dataclass(frozen=True)
class SubmitDecision:
    status: str
    executed: bool
    approval_required: bool = False
    submitted_loop_id: str | None = None
    preflight: dict[str, Any] = field(default_factory=dict)
    source_refs: tuple[str, ...] = ()
    artifact_refs: tuple[str, ...] = ()
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "approval_required": self.approval_required,
            "artifact_refs": list(self.artifact_refs),
            "executed": self.executed,
            "preflight": _compact(self.preflight),
            "reason": self.reason,
            "source_refs": list(self.source_refs),
            "status": self.status,
            "submitted_loop_id": self.submitted_loop_id,
        }


@dataclass(frozen=True)
class StopDecision:
    should_stop: bool
    status: AutonomyStatus
    reason: str
    triggered_guard: str

    @staticmethod
    def continue_running() -> "StopDecision":
        return StopDecision(False, "running", "continue", "none")


@dataclass(frozen=True)
class AutonomyBudgetUsage:
    loops_completed: int
    elapsed_seconds: float
    max_gpu_occupancy_pct: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "elapsed_seconds": round(self.elapsed_seconds, 6),
            "loops_completed": self.loops_completed,
            "max_gpu_occupancy_pct": self.max_gpu_occupancy_pct,
        }


@dataclass
class AutonomousEvolutionState:
    auto_run_id: str
    qe_task_id: str
    methodology_ref: str | None
    stop_conditions: dict[str, Any]
    budget: dict[str, Any]
    started_at: datetime
    external_hypotheses: tuple[ExternalHypothesisRef, ...] = ()
    status: AutonomyStatus = "running"
    loops_completed: int = 0
    consecutive_no_improve: int = 0
    consecutive_failures: int = 0
    max_gpu_occupancy_pct: float | None = None
    last_observation: LoopObservation | None = None
    last_verdict: EvolutionVerdict | None = None
    last_direction: EvolutionDirection | None = None
    proposals: list[LoopProposal] = field(default_factory=list)
    submit_decisions: list[SubmitDecision] = field(default_factory=list)
    stop_reason: str = ""
    triggered_guard: str = "none"
    evidence_refs: set[str] = field(default_factory=set)
    artifact_refs: set[str] = field(default_factory=set)

    def record_observation(self, observation: LoopObservation) -> None:
        self.last_observation = observation
        self.loops_completed = max(self.loops_completed, observation.loop_index)
        if observation.gpu_occupancy_pct is not None:
            self.max_gpu_occupancy_pct = max(self.max_gpu_occupancy_pct or 0.0, float(observation.gpu_occupancy_pct))
        self.evidence_refs.update(observation.source_refs)
        self.artifact_refs.update(str(ref) for ref in observation.artifact_refs)

    def record_verdict(self, verdict: EvolutionVerdict) -> None:
        self.last_verdict = verdict
        if verdict.is_sota or verdict.target_reached:
            self.consecutive_no_improve = 0
        else:
            self.consecutive_no_improve += 1
        if verdict.failure or verdict.data_gap:
            self.consecutive_failures += 1
        else:
            self.consecutive_failures = 0
        self.evidence_refs.update(verdict.source_refs)

    def budget_usage(self, now: datetime) -> AutonomyBudgetUsage:
        elapsed = max(0.0, (now - self.started_at).total_seconds())
        return AutonomyBudgetUsage(self.loops_completed, elapsed, self.max_gpu_occupancy_pct)

    def last_verdict_json(self) -> dict[str, Any]:
        return self.last_verdict.to_dict() if self.last_verdict else {}


@dataclass(frozen=True)
class AutonomyReport:
    auto_run_id: str
    qe_task_id: str
    status: AutonomyStatus
    loops_completed: int
    stop_reason: str
    triggered_guard: str
    last_verdict_json: dict[str, Any]
    budget_usage: AutonomyBudgetUsage
    evidence_refs: tuple[str, ...]
    artifact_refs: tuple[str, ...] = ()
    proposals: tuple[dict[str, Any], ...] = ()
    submit_decisions: tuple[dict[str, Any], ...] = ()
    memory_candidates: tuple[dict[str, Any], ...] = ()
    curriculum_replay: tuple[dict[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_refs": list(self.artifact_refs),
            "auto_run_id": self.auto_run_id,
            "budget_usage": self.budget_usage.to_dict(),
            "evidence_refs": list(self.evidence_refs),
            "last_verdict_json": _compact(self.last_verdict_json),
            "loops_completed": self.loops_completed,
            "memory_candidates": list(self.memory_candidates),
            "curriculum_replay": list(self.curriculum_replay),
            "proposals": list(self.proposals),
            "qe_task_id": self.qe_task_id,
            "status": self.status,
            "stop_reason": self.stop_reason,
            "submit_decisions": list(self.submit_decisions),
            "triggered_guard": self.triggered_guard,
        }

    def canonical_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def request_from_mapping(payload: dict[str, Any]) -> AutonomousEvolutionRequest:
    hypotheses = []
    for item in payload.get("external_hypotheses") or []:
        if not isinstance(item, dict):
            raise ValueError("external_hypotheses items must be mappings")
        hypotheses.append(
            ExternalHypothesisRef(
                source_ref=str(item.get("source_ref") or ""),
                as_of=str(item.get("as_of") or ""),
                hypothesis=str(item.get("hypothesis") or ""),
                low_cost_intent=str(item.get("low_cost_intent") or ""),
                provenance=dict(item.get("provenance") or {}),
            )
        )
    return AutonomousEvolutionRequest(
        enabled=bool(payload.get("enabled", False)),
        qe_task_id=str(payload.get("qe_task_id") or ""),
        methodology_ref=str(payload.get("methodology_ref")) if payload.get("methodology_ref") is not None else None,
        stop_conditions=dict(payload.get("stop_conditions") or {}),
        budget=dict(payload.get("budget") or {}),
        external_hypotheses=tuple(hypotheses),
        auto_run_id=str(payload.get("auto_run_id")) if payload.get("auto_run_id") else None,
    )
