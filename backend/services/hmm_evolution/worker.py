"""Disabled-by-default durable worker skeleton for Phase 1 P1-A."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from .errors import InvalidSpecError
from .models import LeaseConfig
from .repository import HMMEvolutionRepository


class EvaluationExecutor(Protocol):
    def execute_and_finalize(
        self,
        *,
        batch: dict[str, Any],
        evaluation: dict[str, Any],
        repository: HMMEvolutionRepository,
        owner_id: str,
        lease: LeaseConfig,
    ) -> None:
        """Heartbeat and place the evaluation/batch in an explicit terminal state."""
        ...


@dataclass(frozen=True)
class WorkerConfig:
    runtime_mode: str = "disabled"
    candidate_concurrency: int = 2
    lease: LeaseConfig = LeaseConfig()

    def __post_init__(self) -> None:
        if self.runtime_mode not in {"disabled", "api_only", "api_worker"}:
            raise ValueError("unsupported HMM evolution runtime mode")
        if not 1 <= self.candidate_concurrency <= 4:
            raise ValueError("candidate_concurrency must be between one and four")


class HMMEvolutionWorker:
    """Orchestration seam only; P1-B supplies the real pure evaluator executor."""

    def __init__(
        self,
        repository: HMMEvolutionRepository,
        *,
        owner_id: str,
        config: WorkerConfig | None = None,
        executor: EvaluationExecutor | None = None,
    ) -> None:
        self._repository = repository
        self._owner_id = str(owner_id or "").strip()
        self._config = config or WorkerConfig()
        self._executor = executor
        if not self._owner_id:
            raise ValueError("worker owner_id is required")

    def assert_runnable(self) -> None:
        if self._config.runtime_mode != "api_worker":
            raise InvalidSpecError(
                "HMM evolution worker runtime is disabled",
                context={"runtime_mode": self._config.runtime_mode},
            )
        if self._executor is None:
            raise InvalidSpecError(
                "HMM evolution evaluator is not installed; P1-B is required before execution"
            )

    def run_once(self) -> bool:
        """Claim one durable batch/evaluation only after explicit runtime activation."""

        self.assert_runnable()
        # Recovery is part of every worker cycle, not an optional maintenance
        # command.  A crashed process must leave durable rows that the next
        # worker can terminalize before it claims more work.
        self._repository.mark_expired_leases_timed_out()
        batch = self._repository.claim_batch(
            owner_id=self._owner_id,
            lease_seconds=self._config.lease.lease_seconds,
        )
        if batch is None:
            return False
        evaluation = self._repository.claim_evaluation(
            owner_id=self._owner_id,
            lease_seconds=self._config.lease.lease_seconds,
            batch_id=str(batch["batch_id"]),
        )
        if evaluation is None:
            self._repository.release_batch_after_empty_claim(
                batch_id=str(batch["batch_id"]),
                owner_id=self._owner_id,
                fencing_token=int(batch["fencing_token"]),
                expected_row_version=int(batch["row_version"]),
            )
            return True
        # P1-B executor owns checkpoints, heartbeat and terminal result mapping.
        # The call cannot be reached without a concrete executor and its
        # contract requires durable finalization rather than returning a
        # placeholder result to this skeleton.
        self._executor.execute_and_finalize(  # type: ignore[union-attr]
            batch=batch,
            evaluation=evaluation,
            repository=self._repository,
            owner_id=self._owner_id,
            lease=self._config.lease,
        )
        return True
