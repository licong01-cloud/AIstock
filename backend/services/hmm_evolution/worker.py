"""Disabled-by-default durable worker skeleton for Phase 1 P1-A."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from threading import RLock
from typing import Any, Callable, Mapping, Protocol, Sequence

from .errors import EvaluationCancelledError, HMMEvolutionError, InvalidSpecError
from .input_adapter import BatchExecutionInputs, EvaluationExecutionInputs
from .models import LeaseConfig
from .repository import HMMEvolutionRepository


class EvaluationExecutor(Protocol):
    def prepare_batch_inputs(
        self,
        *,
        evaluations: Sequence[Mapping[str, Any]],
        repository: HMMEvolutionRepository,
        candidate_concurrency: int,
        checkpoint: Callable[[str], None] | None = None,
    ) -> BatchExecutionInputs: ...

    def execute_and_finalize(
        self,
        *,
        batch: dict[str, Any],
        evaluation: dict[str, Any],
        repository: HMMEvolutionRepository,
        owner_id: str,
        lease: LeaseConfig,
        execution_inputs: EvaluationExecutionInputs | None = None,
        checkpoint: Callable[[str], tuple[dict[str, Any], dict[str, Any]]] | None = None,
        defer_batch_recompute: bool = False,
    ) -> None:
        """Heartbeat and place the evaluation/batch in an explicit terminal state."""
        ...

    def fail_preparation(
        self,
        *,
        batch: dict[str, Any],
        evaluation: dict[str, Any],
        repository: HMMEvolutionRepository,
        owner_id: str,
        error: HMMEvolutionError,
        checkpoint: Callable[[str], tuple[dict[str, Any], dict[str, Any]]],
        defer_batch_recompute: bool,
    ) -> None: ...


class _ConcurrentLeaseCoordinator:
    """Serialize fencing-token heartbeats for one concurrently executed slice."""

    def __init__(
        self,
        *,
        repository: HMMEvolutionRepository,
        batch: Mapping[str, Any],
        evaluations: Sequence[Mapping[str, Any]],
        owner_id: str,
        lease: LeaseConfig,
    ) -> None:
        self._repository = repository
        self._batch = dict(batch)
        self._evaluations = {str(evaluation["eval_id"]): dict(evaluation) for evaluation in evaluations}
        self._active_eval_ids = set(self._evaluations)
        self._owner_id = owner_id
        self._lease = lease
        self._lock = RLock()

    def heartbeat_all(self, _phase: str) -> None:
        with self._lock:
            self._heartbeat_batch()
            for eval_id in sorted(self._active_eval_ids):
                self._heartbeat_evaluation(eval_id)
                self._raise_if_cancelled(eval_id)

    def checkpoint(
        self,
        eval_id: str,
    ) -> Callable[[str], tuple[dict[str, Any], dict[str, Any]]]:
        def run(_phase: str) -> tuple[dict[str, Any], dict[str, Any]]:
            with self._lock:
                if eval_id not in self._active_eval_ids:
                    raise InvalidSpecError(
                        "cannot heartbeat an evaluation after terminal finalization",
                        context={"eval_id": eval_id},
                    )
                self._heartbeat_batch()
                self._heartbeat_evaluation(eval_id)
                self._raise_if_cancelled(eval_id)
                return dict(self._batch), dict(self._evaluations[eval_id])

        return run

    def mark_terminal(self, eval_id: str) -> None:
        with self._lock:
            self._active_eval_ids.discard(eval_id)

    def snapshots(self, eval_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        with self._lock:
            return dict(self._batch), dict(self._evaluations[eval_id])

    @property
    def batch(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._batch)

    def _heartbeat_batch(self) -> None:
        self._batch = self._repository.heartbeat_batch(
            batch_id=str(self._batch["batch_id"]),
            owner_id=self._owner_id,
            fencing_token=int(self._batch["fencing_token"]),
            expected_row_version=int(self._batch["row_version"]),
            lease_seconds=self._lease.lease_seconds,
        )

    def _heartbeat_evaluation(self, eval_id: str) -> None:
        evaluation = self._evaluations[eval_id]
        self._evaluations[eval_id] = self._repository.heartbeat_evaluation(
            eval_id=eval_id,
            owner_id=self._owner_id,
            fencing_token=int(evaluation["fencing_token"]),
            expected_row_version=int(evaluation["row_version"]),
            lease_seconds=self._lease.lease_seconds,
        )

    def _raise_if_cancelled(self, eval_id: str) -> None:
        if (
            str(self._batch.get("status")) == "cancel_requested"
            or self._evaluations[eval_id].get("cancel_requested_at") is not None
        ):
            raise EvaluationCancelledError(
                "HMM evaluation was cancelled at a durable checkpoint",
                context={"eval_id": eval_id},
            )


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
            raise InvalidSpecError("HMM evolution evaluator is not installed; P1-B is required before execution")

    def run_once(self) -> bool:
        """Execute one bounded concurrent slice from a durable batch."""

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
        evaluations: list[dict[str, Any]] = []
        for _ in range(self._config.candidate_concurrency):
            evaluation = self._repository.claim_evaluation(
                owner_id=self._owner_id,
                lease_seconds=self._config.lease.lease_seconds,
                batch_id=str(batch["batch_id"]),
            )
            if evaluation is None:
                break
            evaluations.append(evaluation)
        if not evaluations:
            self._repository.release_batch_after_empty_claim(
                batch_id=str(batch["batch_id"]),
                owner_id=self._owner_id,
                fencing_token=int(batch["fencing_token"]),
                expected_row_version=int(batch["row_version"]),
            )
            return True
        executor = self._executor
        if executor is None:  # pragma: no cover - assert_runnable rejects this.
            raise InvalidSpecError("HMM evolution evaluator is not installed")
        leases = _ConcurrentLeaseCoordinator(
            repository=self._repository,
            batch=batch,
            evaluations=evaluations,
            owner_id=self._owner_id,
            lease=self._config.lease,
        )
        try:
            prepared = executor.prepare_batch_inputs(
                evaluations=evaluations,
                repository=self._repository,
                candidate_concurrency=self._config.candidate_concurrency,
                checkpoint=leases.heartbeat_all,
            )
        except HMMEvolutionError as exc:
            prepared = BatchExecutionInputs(
                inputs_by_eval_id={},
                errors_by_eval_id={str(item["eval_id"]): exc for item in evaluations},
            )

        failures: list[BaseException] = []

        def execute(evaluation: dict[str, Any]) -> None:
            eval_id = str(evaluation["eval_id"])
            batch_snapshot, evaluation_snapshot = leases.snapshots(eval_id)
            try:
                preparation_error = prepared.errors_by_eval_id.get(eval_id)
                if preparation_error is not None:
                    executor.fail_preparation(
                        batch=batch_snapshot,
                        evaluation=evaluation_snapshot,
                        repository=self._repository,
                        owner_id=self._owner_id,
                        error=preparation_error,
                        checkpoint=leases.checkpoint(eval_id),
                        defer_batch_recompute=True,
                    )
                else:
                    executor.execute_and_finalize(
                        batch=batch_snapshot,
                        evaluation=evaluation_snapshot,
                        repository=self._repository,
                        owner_id=self._owner_id,
                        lease=self._config.lease,
                        execution_inputs=prepared.inputs_by_eval_id[eval_id],
                        checkpoint=leases.checkpoint(eval_id),
                        defer_batch_recompute=True,
                    )
            finally:
                leases.mark_terminal(eval_id)

        with ThreadPoolExecutor(max_workers=len(evaluations)) as pool:
            futures = [pool.submit(execute, evaluation) for evaluation in evaluations]
            for future in as_completed(futures):
                try:
                    future.result()
                except BaseException as exc:  # re-raised after durable batch recompute.
                    failures.append(exc)

        final_batch = leases.batch
        self._repository.finalize_worker_cycle(
            batch_id=str(final_batch["batch_id"]),
            eval_ids=[str(evaluation["eval_id"]) for evaluation in evaluations],
            owner_id=self._owner_id,
            fencing_token=int(final_batch["fencing_token"]),
            expected_row_version=int(final_batch["row_version"]),
        )
        if failures:
            raise failures[0]
        return True
