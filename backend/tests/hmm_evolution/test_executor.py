from __future__ import annotations

from datetime import date

import pandas as pd

from backend.services.hmm_evolution.errors import SourceUnavailableError
from backend.services.hmm_evolution.evaluator import CandidateCoefficients
from backend.services.hmm_evolution.executor import HMMEvaluationExecutor
from backend.services.hmm_evolution.input_adapter import EvaluationExecutionInputs
from backend.services.hmm_evolution.models import EvaluationSpec, LeaseConfig


class _Adapter:
    def __init__(self, *, error=None):
        self.error = error

    async def load_evaluation(self, **kwargs):
        if self.error is not None:
            raise self.error
        trade_date = date(2026, 1, 5)
        return EvaluationExecutionInputs(
            predictions=pd.DataFrame(
                [(trade_date, "A", 1.0)], columns=["trade_date", "symbol", "score"]
            ),
            labels=pd.DataFrame(
                [(trade_date, "A", 10, 0.1)],
                columns=["trade_date", "symbol", "horizon_days", "future_return"],
            ),
            coefficients=CandidateCoefficients.from_payload(
                {
                    "daily_coefficients": {trade_date.isoformat(): {"S": 1.0}},
                    "stock_sector_map": {"A": "S"},
                }
            ),
            evaluation_dates=(trade_date,),
            date_coverage_evidence={"evaluation_dates": [trade_date.isoformat()]},
            market_returns=None,
        )


class _Repository:
    def __init__(self, *, cancel=False):
        self.cancel = cancel
        self.completed = None
        self.failed = None
        self.batch_version = 1
        self.eval_version = 1

    def get_candidate(self, candidate_id):
        return object()

    def heartbeat_batch(self, **kwargs):
        self.batch_version += 1
        return {
            "batch_id": kwargs["batch_id"],
            "fencing_token": kwargs["fencing_token"],
            "row_version": self.batch_version,
            "status": "cancel_requested" if self.cancel else "running",
        }

    def heartbeat_evaluation(self, **kwargs):
        self.eval_version += 1
        return {
            "eval_id": kwargs["eval_id"],
            "fencing_token": kwargs["fencing_token"],
            "row_version": self.eval_version,
            "cancel_requested_at": None,
        }

    def complete_evaluation(self, **kwargs):
        self.completed = kwargs

    def fail_evaluation(self, **kwargs):
        self.failed = kwargs


def _evaluation():
    trade_date = date(2026, 1, 5)
    spec = EvaluationSpec(
        base_loop_ref="qe_task/Loop8",
        window_start=trade_date,
        window_end=trade_date,
        as_of={"policy": "explicit", "requested_date": trade_date.isoformat()},
        label_horizon_days=10,
        topk=1,
        market_forward_return={"mode": "disabled", "horizon_trading_days": 10},
    )
    return {
        "eval_id": "hmme_1",
        "candidate_id": "hmmc_1",
        "fencing_token": 2,
        "row_version": 1,
        "evaluation_spec": spec.model_dump(mode="json"),
        "source_manifest": {},
    }


def _batch():
    return {"batch_id": "hmmb_1", "fencing_token": 3, "row_version": 1, "status": "running"}


def test_executor_checkpoints_and_commits_terminal_success() -> None:
    repository = _Repository()
    executor = HMMEvaluationExecutor(_Adapter())

    executor.execute_and_finalize(
        batch=_batch(),
        evaluation=_evaluation(),
        repository=repository,  # type: ignore[arg-type]
        owner_id="worker-1",
        lease=LeaseConfig(),
    )

    assert repository.failed is None
    assert repository.completed is not None
    assert repository.completed["result"]["evidence_quality"] == "insufficient"
    assert repository.completed["expected_row_version"] == repository.eval_version


def test_executor_maps_known_failure_without_fake_success() -> None:
    repository = _Repository()
    executor = HMMEvaluationExecutor(_Adapter(error=SourceUnavailableError("source unavailable")))

    executor.execute_and_finalize(
        batch=_batch(),
        evaluation=_evaluation(),
        repository=repository,  # type: ignore[arg-type]
        owner_id="worker-1",
        lease=LeaseConfig(),
    )

    assert repository.completed is None
    assert repository.failed["reason_code"] == "hmm_evolution_source_unavailable"
    assert repository.failed["terminal_status"].value == "failed"


def test_executor_honors_durable_cancel_checkpoint() -> None:
    repository = _Repository(cancel=True)
    executor = HMMEvaluationExecutor(_Adapter())

    executor.execute_and_finalize(
        batch=_batch(),
        evaluation=_evaluation(),
        repository=repository,  # type: ignore[arg-type]
        owner_id="worker-1",
        lease=LeaseConfig(),
    )

    assert repository.completed is None
    assert repository.failed["reason_code"] == "hmm_evolution_evaluation_cancelled"
    assert repository.failed["terminal_status"].value == "cancelled"
