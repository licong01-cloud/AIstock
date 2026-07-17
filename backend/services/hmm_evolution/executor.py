"""Durable worker executor for the pure HMM evaluator."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date
from typing import Any

from .errors import (
    EvaluationCancelledError,
    HMMEvolutionError,
    StaleFencingTokenError,
)
from .evaluator import evaluate_candidate
from .input_adapter import HMMEvaluationInputAdapter
from .models import EvaluationSpec, EvaluationStatus, LeaseConfig
from .repository import HMMEvolutionRepository


@dataclass
class _LeaseState:
    batch: dict[str, Any]
    evaluation: dict[str, Any]


class HMMEvaluationExecutor:
    """Replay frozen inputs, checkpoint leases and commit one terminal result."""

    def __init__(self, input_adapter: HMMEvaluationInputAdapter) -> None:
        self._input_adapter = input_adapter

    def execute_and_finalize(
        self,
        *,
        batch: dict[str, Any],
        evaluation: dict[str, Any],
        repository: HMMEvolutionRepository,
        owner_id: str,
        lease: LeaseConfig,
    ) -> None:
        state = _LeaseState(batch=dict(batch), evaluation=dict(evaluation))

        def checkpoint(_phase: str) -> None:
            self._heartbeat(
                state=state,
                repository=repository,
                owner_id=owner_id,
                lease=lease,
            )

        try:
            checkpoint("before_input_load")
            candidate = repository.get_candidate(str(evaluation["candidate_id"]))
            inputs = asyncio.run(
                self._input_adapter.load_evaluation(
                    evaluation=evaluation,
                    candidate=candidate,
                    checkpoint=checkpoint,
                )
            )
            spec = EvaluationSpec.model_validate(evaluation["evaluation_spec"])

            def date_checkpoint(index: int, trade_date: date) -> None:
                checkpoint(f"evaluation_day_{index}_{trade_date.isoformat()}")

            computation = evaluate_candidate(
                candidate_id=str(evaluation["candidate_id"]),
                predictions=inputs.predictions,
                labels=inputs.labels,
                coefficients=inputs.coefficients,
                evaluation_dates=inputs.evaluation_dates,
                label_horizon_days=spec.label_horizon_days,
                topk=spec.topk,
                db_forward_returns=inputs.market_returns,
                market_forward_return_mode=str(spec.market_forward_return["mode"]),
                date_coverage_evidence=inputs.date_coverage_evidence,
                checkpoint=date_checkpoint,
            )
            checkpoint("before_result_commit")
            repository.complete_evaluation(
                eval_id=str(state.evaluation["eval_id"]),
                owner_id=owner_id,
                fencing_token=int(state.evaluation["fencing_token"]),
                expected_row_version=int(state.evaluation["row_version"]),
                result=computation.result,
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
            )
        except HMMEvolutionError as exc:
            self._fail(
                repository=repository,
                state=state,
                owner_id=owner_id,
                error=exc,
                terminal_status=EvaluationStatus.FAILED,
            )
        except Exception as exc:
            wrapped = HMMEvolutionError(
                "unexpected HMM evaluation failure",
                context={"error_type": type(exc).__name__},
            )
            self._fail(
                repository=repository,
                state=state,
                owner_id=owner_id,
                error=wrapped,
                terminal_status=EvaluationStatus.FAILED,
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
        )
