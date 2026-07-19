"""Durable worker executor for the pure HMM evaluator."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

from .errors import (
    EvaluationCancelledError,
    HMMEvolutionError,
    StaleFencingTokenError,
    sanitized_exception_chain,
)
from .evaluator import evaluate_candidate
from .input_adapter import (
    BatchExecutionInputs,
    EvaluationExecutionInputs,
    HMMEvaluationInputAdapter,
)
from .models import EvaluationSpec, EvaluationStatus, LeaseConfig
from .repository import HMMEvolutionRepository

logger = logging.getLogger(__name__)


@dataclass
class _LeaseState:
    batch: dict[str, Any]
    evaluation: dict[str, Any]


class HMMEvaluationExecutor:
    """Replay frozen inputs, checkpoint leases and commit one terminal result."""

    def __init__(self, input_adapter: HMMEvaluationInputAdapter) -> None:
        self._input_adapter = input_adapter

    def prepare_batch_inputs(
        self,
        *,
        evaluations: Sequence[Mapping[str, Any]],
        repository: HMMEvolutionRepository,
        candidate_concurrency: int,
        checkpoint: Callable[[str], None] | None = None,
    ) -> BatchExecutionInputs:
        candidates = [repository.get_candidate(str(evaluation["candidate_id"])) for evaluation in evaluations]
        return asyncio.run(
            self._input_adapter.load_batch_evaluations(
                evaluations=tuple(zip(evaluations, candidates, strict=True)),
                candidate_concurrency=candidate_concurrency,
                checkpoint=checkpoint,
            )
        )

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
    ) -> None:
        state = _LeaseState(batch=dict(batch), evaluation=dict(evaluation))
        state.batch, state.evaluation = checkpoint("before_preparation_failure_commit")
        self._raise_if_cancelled(state)
        self._fail(
            repository=repository,
            state=state,
            owner_id=owner_id,
            error=error,
            terminal_status=EvaluationStatus.FAILED,
            defer_batch_recompute=defer_batch_recompute,
        )

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
        state = _LeaseState(batch=dict(batch), evaluation=dict(evaluation))

        def durable_checkpoint(phase: str) -> None:
            if checkpoint is None:
                self._heartbeat(
                    state=state,
                    repository=repository,
                    owner_id=owner_id,
                    lease=lease,
                )
            else:
                state.batch, state.evaluation = checkpoint(phase)
                self._raise_if_cancelled(state)

        try:
            durable_checkpoint("before_input_load")
            inputs = execution_inputs
            if inputs is None:
                candidate = repository.get_candidate(str(evaluation["candidate_id"]))
                inputs = asyncio.run(
                    self._input_adapter.load_evaluation(
                        evaluation=evaluation,
                        candidate=candidate,
                        checkpoint=durable_checkpoint,
                    )
                )
            spec = EvaluationSpec.model_validate(evaluation["evaluation_spec"])

            def date_checkpoint(index: int, trade_date: date) -> None:
                durable_checkpoint(f"evaluation_day_{index}_{trade_date.isoformat()}")

            computation = evaluate_candidate(
                candidate_id=str(evaluation["candidate_id"]),
                predictions=inputs.predictions,
                labels=inputs.labels,
                coefficients=inputs.coefficients,
                evaluation_dates=inputs.evaluation_dates,
                label_horizon_days=spec.label_horizon_days,
                topk=spec.topk,
                db_forward_returns=inputs.market_returns,
                market_missing_evidence=inputs.market_missing_evidence,
                market_forward_return_mode=str(spec.market_forward_return["mode"]),
                date_coverage_evidence=inputs.date_coverage_evidence,
                checkpoint=date_checkpoint,
            )
            durable_checkpoint("before_result_commit")
            repository.complete_evaluation(
                eval_id=str(state.evaluation["eval_id"]),
                owner_id=owner_id,
                fencing_token=int(state.evaluation["fencing_token"]),
                expected_row_version=int(state.evaluation["row_version"]),
                result=computation.result,
                defer_batch_recompute=defer_batch_recompute,
            )
        except StaleFencingTokenError:
            raise
        except EvaluationCancelledError as exc:
            self._fail(
                repository=repository,
                state=state,
                owner_id=owner_id,
                error=exc,
                terminal_status=EvaluationStatus.CANCELLED,
                defer_batch_recompute=defer_batch_recompute,
            )
        except HMMEvolutionError as exc:
            self._fail(
                repository=repository,
                state=state,
                owner_id=owner_id,
                error=exc,
                terminal_status=EvaluationStatus.FAILED,
                defer_batch_recompute=defer_batch_recompute,
            )
        except Exception as exc:
            logger.exception(
                "unexpected HMM evaluation failure eval_id=%s",
                evaluation.get("eval_id"),
            )
            wrapped = HMMEvolutionError(
                "unexpected HMM evaluation failure",
                context={"exception_chain": sanitized_exception_chain(exc)},
            )
            self._fail(
                repository=repository,
                state=state,
                owner_id=owner_id,
                error=wrapped,
                terminal_status=EvaluationStatus.FAILED,
                defer_batch_recompute=defer_batch_recompute,
            )

    @staticmethod
    def _heartbeat(
        *,
        state: _LeaseState,
        repository: HMMEvolutionRepository,
        owner_id: str,
        lease: LeaseConfig,
    ) -> None:
        state.batch = repository.heartbeat_batch(
            batch_id=str(state.batch["batch_id"]),
            owner_id=owner_id,
            fencing_token=int(state.batch["fencing_token"]),
            expected_row_version=int(state.batch["row_version"]),
            lease_seconds=lease.lease_seconds,
        )
        state.evaluation = repository.heartbeat_evaluation(
            eval_id=str(state.evaluation["eval_id"]),
            owner_id=owner_id,
            fencing_token=int(state.evaluation["fencing_token"]),
            expected_row_version=int(state.evaluation["row_version"]),
            lease_seconds=lease.lease_seconds,
        )
        HMMEvaluationExecutor._raise_if_cancelled(state)

    @staticmethod
    def _raise_if_cancelled(state: _LeaseState) -> None:
        if (
            str(state.batch.get("status")) == "cancel_requested"
            or state.evaluation.get("cancel_requested_at") is not None
        ):
            raise EvaluationCancelledError(
                "HMM evaluation was cancelled at a durable checkpoint",
                context={"eval_id": str(state.evaluation["eval_id"])},
            )

    @staticmethod
    def _fail(
        *,
        repository: HMMEvolutionRepository,
        state: _LeaseState,
        owner_id: str,
        error: HMMEvolutionError,
        terminal_status: EvaluationStatus,
        defer_batch_recompute: bool,
    ) -> None:
        repository.fail_evaluation(
            eval_id=str(state.evaluation["eval_id"]),
            owner_id=owner_id,
            fencing_token=int(state.evaluation["fencing_token"]),
            expected_row_version=int(state.evaluation["row_version"]),
            error_code=error.error_code,
            reason_code=error.reason_code,
            error_message=error.message,
            error_context=error.context,
            terminal_status=terminal_status,
            defer_batch_recompute=defer_batch_recompute,
        )
