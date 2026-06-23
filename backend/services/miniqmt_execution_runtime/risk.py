"""Phase 4 realtime risk hook for the durable MiniQMT event loop."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol


class MiniQMTRiskDecisionAction(str, Enum):
    PASS = "PASS"
    KILL_SWITCH = "KILL_SWITCH"


@dataclass(frozen=True)
class MiniQMTRiskDecision:
    action: MiniQMTRiskDecisionAction
    reason_code: str
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def pass_(cls, *, reason: str = "risk checks passed", metadata: dict[str, Any] | None = None) -> "MiniQMTRiskDecision":
        return cls(
            action=MiniQMTRiskDecisionAction.PASS,
            reason_code="MINIQMT_RISK_PASS",
            reason=reason,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def kill_switch(
        cls,
        *,
        reason_code: str,
        reason: str,
        metadata: dict[str, Any] | None = None,
    ) -> "MiniQMTRiskDecision":
        normalized = str(reason_code or "").strip()
        if not normalized:
            raise ValueError("kill-switch risk decision requires reason_code")
        return cls(
            action=MiniQMTRiskDecisionAction.KILL_SWITCH,
            reason_code=normalized,
            reason=str(reason or "MiniQMT risk kill-switch triggered"),
            metadata=dict(metadata or {}),
        )


class MiniQMTRiskEngine(Protocol):
    def evaluate_event(
        self,
        *,
        runtime_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> MiniQMTRiskDecision:
        ...


class NoopMiniQMTRiskEngine:
    """Default inert hook: explicit Phase 4 risk engines opt in per runtime."""

    def evaluate_event(
        self,
        *,
        runtime_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> MiniQMTRiskDecision:
        return MiniQMTRiskDecision.pass_(metadata={"runtime_id": runtime_id, "event_type": event_type})

