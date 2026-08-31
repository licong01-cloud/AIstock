"""Disabled-by-default durable worker skeleton for Phase 1 P1-A."""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from threading import RLock
from typing import Any, Callable, Mapping, Protocol, Sequence

from .errors import (
    EvaluationCancelledError,
    HMMEvolutionError,
    InvalidSpecError,
    sanitized_exception_chain,
)
from .input_adapter import BatchExecutionInputs, EvaluationExecutionInputs
from .models import (
    STAGE_EVALUATION_QUEUE_WAIT,
    ExecutionPurpose,
    LeaseConfig,
    derive_cache_state,
)
from .performance_receipt import (
    StageRecorder,
    cache_evidence_from_artifact_info,
    capture_hardware_identity,
    capture_runtime_identity,
    current_rss_bytes,
    evidence_payload,
    utc_now,
)
from .repository import HMMEvolutionRepository, TERMINAL_BATCH_STATUSES


logger = logging.getLogger(__name__)


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
        receipt_recorder: StageRecorder | None = None,
        compute_started_at: Any = None,
        rss_samples: list[int] | None = None,
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


class SubmissionPreparer(Protocol):
    async def prepare_claimed_submission(
        self,
        *,
        batch: Mapping[str, Any],
        owner_id: str,
        lease_seconds: int,
    ) -> dict[str, Any]: ...


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
        submission_preparer: SubmissionPreparer | None = None,
    ) -> None:
        self._repository = repository
        self._owner_id = str(owner_id or "").strip()
        self._config = config or WorkerConfig()
        self._executor = executor
        self._submission_preparer = submission_preparer
        self._cycle_claimed_batch_id: str | None = None
        self._cycle_terminal_batch_id: str | None = None
        self._cycle_terminal_failed = False
        if not self._owner_id:
            raise ValueError("worker owner_id is required")

    def pop_cycle_status(self) -> tuple[str | None, str | None, bool]:
        """Return and reset the last cycle's claim/terminal supervision evidence."""

        status = (
            self._cycle_claimed_batch_id,
            self._cycle_terminal_batch_id,
            self._cycle_terminal_failed,
        )
        self._cycle_claimed_batch_id = None
        self._cycle_terminal_batch_id = None
        self._cycle_terminal_failed = False
        return status

    def assert_runnable(self) -> None:
        if self._config.runtime_mode != "api_worker":
            raise InvalidSpecError(
                "HMM evolution worker runtime is disabled",
                context={"runtime_mode": self._config.runtime_mode},
            )
        if self._executor is None:
            raise InvalidSpecError("HMM evolution evaluator is not installed; P1-B is required before execution")
        if self._submission_preparer is None:
            raise InvalidSpecError("HMM evolution durable submission preparer is not installed")

    def run_once(self) -> bool:
        """Execute one bounded concurrent slice from a durable batch."""

        self.assert_runnable()
        # Recovery is part of every worker cycle, not an optional maintenance
        # command.  A crashed process must leave durable rows that the next
        # worker can terminalize before it claims more work.
        self._repository.recover_expired_preparations()
        self._repository.mark_expired_leases_timed_out()
        preparation = self._repository.claim_batch_preparation(
            owner_id=self._owner_id,
            lease_seconds=self._config.lease.lease_seconds,
        )
        if preparation is not None:
            self._cycle_claimed_batch_id = str(preparation["batch_id"])
            self._prepare_submission(preparation)
            return True
        batch = self._repository.claim_batch(
            owner_id=self._owner_id,
            lease_seconds=self._config.lease.lease_seconds,
        )
        if batch is None:
            return False
        self._cycle_claimed_batch_id = str(batch["batch_id"])
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
        batch_receipt = self._ensure_batch_receipt(batch)
        compute_started_at = utc_now()
        eval_receipts: dict[str, dict[str, Any]] = {}
        recorders: dict[str, StageRecorder] = {}
        rss_samples: dict[str, list[int]] = {}
        for evaluation in evaluations:
            eval_id = str(evaluation["eval_id"])
            receipt, _created = self._repository.create_performance_receipt(
                receipt_level="evaluation",
                batch_id=str(batch["batch_id"]),
                eval_id=eval_id,
                execution_purpose=str(
                    batch.get("execution_purpose") or ExecutionPurpose.EVALUATION.value
                ),
                benchmark_id=(
                    str(batch["benchmark_id"]) if batch.get("benchmark_id") else None
                ),
                runtime_identity=capture_runtime_identity(
                    owner_id=self._owner_id, role="evaluation_worker"
                ),
                hardware_identity=capture_hardware_identity(),
                input_identity={
                    "logical_evaluation_key": str(evaluation["logical_evaluation_key"]),
                    "candidate_manifest_hash": str(evaluation["candidate_manifest_hash"]),
                    "source_manifest_hash": str(evaluation["source_manifest_hash"]),
                    "evaluation_spec_hash": str(evaluation["evaluation_spec_hash"]),
                    "evaluator_version": str(evaluation["evaluator_version"]),
                    "input_hash": str(evaluation["input_hash"]),
                    "universe_hash": str(evaluation["universe_hash"]),
                    "run_generation": int(evaluation["run_generation"]),
                },
            )
            recorder = StageRecorder()
            queue_wait_end = evaluation.get("started_at") or evaluation.get("updated_at")
            if queue_wait_end is not None and evaluation.get("queued_at") is not None:
                recorder.record(
                    STAGE_EVALUATION_QUEUE_WAIT,
                    started_at=evaluation["queued_at"],
                    completed_at=queue_wait_end,
                )
                receipt = self._repository.merge_performance_receipt_progress(
                    receipt_id=str(receipt["receipt_id"]),
                    expected_row_version=int(receipt["row_version"]),
                    stage_timings=recorder.stage_payload(),
                )
            eval_receipts[eval_id] = receipt
            recorders[eval_id] = recorder
            rss_samples[eval_id] = [current_rss_bytes()]
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
                artifact_source_info={},
            )

        failures: list[BaseException] = []

        def execute(evaluation: dict[str, Any]) -> None:
            eval_id = str(evaluation["eval_id"])
            batch_snapshot, evaluation_snapshot = leases.snapshots(eval_id)
            recorder = recorders[eval_id]
            samples = rss_samples[eval_id]
            samples.append(current_rss_bytes())
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
                    self._merge_evaluation_receipt_progress(
                        receipt=eval_receipts[eval_id],
                        recorder=recorder,
                        artifact_source_info=prepared.artifact_source_info,
                        peak_rss=max(samples),
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
                        receipt_recorder=recorder,
                        compute_started_at=compute_started_at,
                        rss_samples=samples,
                    )
                    eval_receipts[eval_id] = self._finalize_evaluation_receipt(
                        receipt=eval_receipts[eval_id],
                        eval_id=eval_id,
                        recorder=recorder,
                        artifact_source_info=prepared.artifact_source_info,
                        peak_rss=max(samples),
                    )
            except BaseException:
                self._merge_evaluation_receipt_progress_safely(
                    eval_id=eval_id,
                    receipt=eval_receipts[eval_id],
                    recorder=recorder,
                    artifact_source_info=prepared.artifact_source_info,
                    peak_rss=max(samples),
                )
                raise
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
        finalized = self._repository.finalize_worker_cycle(
            batch_id=str(final_batch["batch_id"]),
            eval_ids=[str(evaluation["eval_id"]) for evaluation in evaluations],
            owner_id=self._owner_id,
            fencing_token=int(final_batch["fencing_token"]),
            expected_row_version=int(final_batch["row_version"]),
        )
        self._close_batch_receipt(batch_receipt=batch_receipt, batch=finalized)
        if failures:
            raise failures[0]
        return True

    def _ensure_batch_receipt(self, batch: Mapping[str, Any]) -> dict[str, Any]:
        receipt, _created = self._repository.create_performance_receipt(
            receipt_level="batch",
            batch_id=str(batch["batch_id"]),
            eval_id=None,
            execution_purpose=str(
                batch.get("execution_purpose") or ExecutionPurpose.EVALUATION.value
            ),
            benchmark_id=str(batch["benchmark_id"]) if batch.get("benchmark_id") else None,
            runtime_identity=capture_runtime_identity(
                owner_id=self._owner_id, role="evaluation_worker"
            ),
            hardware_identity=capture_hardware_identity(),
            input_identity={
                "request_hash": str(batch["request_hash"]),
                "candidate_count": int(batch["candidate_count"]),
            },
        )
        return receipt

    def _finalize_evaluation_receipt(
        self,
        *,
        receipt: Mapping[str, Any],
        eval_id: str,
        recorder: StageRecorder,
        artifact_source_info: Mapping[str, Mapping[str, Any]],
        peak_rss: int,
    ) -> dict[str, Any]:
        terminal = self._repository.get_evaluation(eval_id)
        evidence = cache_evidence_from_artifact_info(artifact_source_info)
        completed_at = terminal.get("completed_at")
        queued_at = terminal.get("queued_at")
        if completed_at is None or queued_at is None:  # pragma: no cover - terminal rows carry both.
            raise InvalidSpecError(
                "terminal evaluation is missing durable timestamps",
                context={"eval_id": eval_id},
            )
        request_to_terminal_ms = max(
            0, int(round((completed_at - queued_at).total_seconds() * 1000))
        )
        return self._repository.finalize_performance_receipt(
            receipt_id=str(receipt["receipt_id"]),
            expected_row_version=int(receipt["row_version"]),
            request_to_terminal_ms=request_to_terminal_ms,
            stage_timings=recorder.stage_payload(),
            cache_evidence=evidence_payload(evidence),
            cache_state=derive_cache_state(evidence).value,
            peak_rss_bytes=peak_rss,
            result_hash=(
                str(terminal["result_hash"]) if terminal.get("result_hash") else None
            ),
        )

    def _merge_evaluation_receipt_progress(
        self,
        *,
        receipt: Mapping[str, Any],
        recorder: StageRecorder,
        artifact_source_info: Mapping[str, Mapping[str, Any]],
        peak_rss: int,
    ) -> dict[str, Any]:
        evidence = cache_evidence_from_artifact_info(artifact_source_info)
        return self._repository.merge_performance_receipt_progress(
            receipt_id=str(receipt["receipt_id"]),
            expected_row_version=int(receipt["row_version"]),
            stage_timings=recorder.stage_payload(),
            cache_evidence=evidence_payload(evidence) if evidence else None,
            cache_state=derive_cache_state(evidence).value if evidence else None,
            peak_rss_bytes=peak_rss,
        )

    def _merge_evaluation_receipt_progress_safely(
        self,
        *,
        eval_id: str,
        receipt: Mapping[str, Any],
        recorder: StageRecorder,
        artifact_source_info: Mapping[str, Mapping[str, Any]],
        peak_rss: int,
    ) -> None:
        try:
            self._merge_evaluation_receipt_progress(
                receipt=receipt,
                recorder=recorder,
                artifact_source_info=artifact_source_info,
                peak_rss=peak_rss,
            )
        except Exception:
            logger.exception(
                "failed to merge partial evaluation receipt eval_id=%s receipt_id=%s",
                eval_id,
                receipt.get("receipt_id"),
            )

    def _close_batch_receipt(
        self,
        *,
        batch_receipt: Mapping[str, Any],
        batch: Mapping[str, Any],
    ) -> None:
        """Finalize completed batch receipts; failed/timed-out stay partial."""

        status = str(batch.get("status") or "")
        if status in TERMINAL_BATCH_STATUSES:
            self._cycle_terminal_batch_id = str(batch["batch_id"])
            self._cycle_terminal_failed = status != "completed"
        if status != "completed":
            return
        completed_at = batch.get("completed_at")
        created_at = batch.get("created_at")
        if completed_at is None or created_at is None:  # pragma: no cover
            raise InvalidSpecError(
                "completed batch is missing durable timestamps",
                context={"batch_id": batch.get("batch_id")},
            )
        request_to_terminal_ms = max(
            0, int(round((completed_at - created_at).total_seconds() * 1000))
        )
        latest = self._repository.get_performance_receipt(batch_id=str(batch["batch_id"]))
        if latest is None or str(latest.get("receipt_status")) != "partial":
            return
        self._repository.finalize_performance_receipt(
            receipt_id=str(latest["receipt_id"]),
            expected_row_version=int(latest["row_version"]),
            request_to_terminal_ms=request_to_terminal_ms,
            result_hash=None,
        )

    def _prepare_submission(self, batch: dict[str, Any]) -> None:
        preparer = self._submission_preparer
        if preparer is None:  # pragma: no cover - assert_runnable rejects this.
            raise InvalidSpecError("HMM evolution durable submission preparer is not installed")
        try:
            asyncio.run(
                preparer.prepare_claimed_submission(
                    batch=batch,
                    owner_id=self._owner_id,
                    lease_seconds=self._config.lease.lease_seconds,
                )
            )
        except HMMEvolutionError as exc:
            terminalized = self._fail_claimed_preparation(
                batch_id=str(batch["batch_id"]),
                error_code=exc.error_code,
                reason_code=exc.reason_code,
                error_context={"message": exc.message, **dict(exc.context)},
            )
            if not terminalized:
                raise
        except Exception as exc:
            terminalized = self._fail_claimed_preparation(
                batch_id=str(batch["batch_id"]),
                error_code="HMM_EVOLUTION_ERROR",
                reason_code="hmm_evolution_unexpected_preparation_failure",
                error_context={"exception_chain": sanitized_exception_chain(exc)},
            )
            if not terminalized:
                raise
            logger.exception(
                "HMM evolution batch preparation failed unexpectedly and was terminalized "
                "batch_id=%s owner_id=%s",
                batch["batch_id"],
                self._owner_id,
            )

    def _fail_claimed_preparation(
        self,
        *,
        batch_id: str,
        error_code: str,
        reason_code: str,
        error_context: Mapping[str, Any],
    ) -> bool:
        latest = self._repository.get_batch(batch_id)
        if str(latest.get("status")) != "preparing":
            terminal = str(latest.get("status")) in TERMINAL_BATCH_STATUSES
            if terminal:
                self._cycle_terminal_batch_id = batch_id
                self._cycle_terminal_failed = True
            return terminal
        failed = self._repository.fail_batch_preparation(
            batch_id=batch_id,
            owner_id=self._owner_id,
            fencing_token=int(latest["fencing_token"]),
            expected_row_version=int(latest["row_version"]),
            error_code=error_code,
            reason_code=reason_code,
            error_context=error_context,
        )
        terminal = str(failed.get("status")) in TERMINAL_BATCH_STATUSES
        if terminal:
            self._cycle_terminal_batch_id = batch_id
            self._cycle_terminal_failed = True
        return terminal
