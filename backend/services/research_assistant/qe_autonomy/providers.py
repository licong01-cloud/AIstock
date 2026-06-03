from __future__ import annotations

from datetime import datetime
from typing import Protocol

from .models import (
    AutonomousEvolutionState,
    EvolutionDirection,
    EvolutionVerdict,
    LoopObservation,
    LoopProposal,
    SubmitDecision,
)


class ClockProvider(Protocol):
    def __call__(self) -> datetime:
        ...


class IdFactory(Protocol):
    def __call__(self, prefix: str, stable_key: str) -> str:
        ...


class LoopExecutorProvider(Protocol):
    def run_or_wait_loop_n(self, state: AutonomousEvolutionState) -> LoopObservation:
        ...


class EvaluatorProvider(Protocol):
    def evaluate_loop(self, observation: LoopObservation, state: AutonomousEvolutionState) -> EvolutionVerdict:
        ...


class DirectionDeciderProvider(Protocol):
    def decide_direction(self, verdict: EvolutionVerdict, state: AutonomousEvolutionState) -> EvolutionDirection:
        ...


class LoopConfigGeneratorProvider(Protocol):
    def generate_next_config(self, direction: EvolutionDirection, state: AutonomousEvolutionState) -> LoopProposal:
        ...


class LoopSubmitterProvider(Protocol):
    def submit_or_preflight_next_loop(self, proposal: LoopProposal, state: AutonomousEvolutionState) -> SubmitDecision:
        ...


class AutonomyRunStore(Protocol):
    def create_run(self, state: AutonomousEvolutionState) -> None:
        ...

    def update_run(self, state: AutonomousEvolutionState) -> None:
        ...

    def get_run(self, auto_run_id: str) -> dict[str, object] | None:
        ...

    def archive_report(self, state: AutonomousEvolutionState, report: dict[str, object]) -> None:
        ...


class ApprovalGatewayProvider(Protocol):
    def preflight_confirmation_only(self, proposal: LoopProposal, state: AutonomousEvolutionState) -> SubmitDecision:
        ...
