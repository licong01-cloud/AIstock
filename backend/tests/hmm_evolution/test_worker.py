from __future__ import annotations

from typing import Any

import pytest

from backend.services.hmm_evolution.worker import HMMEvolutionWorker, WorkerConfig


class _UnexpectedPreparationFailure:
    async def prepare_claimed_submission(self, **_kwargs: Any) -> None:
        raise RuntimeError("manifest construction failed")


class _PreparationRepository:
    def __init__(self, *, fail_write_error: Exception | None = None) -> None:
        self._claimed = False
        self._status = "preparing"
        self._fail_write_error = fail_write_error
        self.failure_receipt: dict[str, Any] | None = None

    def recover_expired_preparations(self) -> None:
        return None

    def mark_expired_leases_timed_out(self) -> None:
        return None

    def claim_batch_preparation(self, **_kwargs: Any) -> dict[str, Any] | None:
        if self._claimed:
            return None
        self._claimed = True
        return {"batch_id": "hmmb_terminalized_failure"}

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        assert batch_id == "hmmb_terminalized_failure"
        return {
            "batch_id": batch_id,
            "status": self._status,
            "fencing_token": 7,
            "row_version": 3,
        }

    def fail_batch_preparation(self, **kwargs: Any) -> dict[str, Any]:
        if self._fail_write_error is not None:
            raise self._fail_write_error
        self.failure_receipt = dict(kwargs)
        self._status = "failed"
        return {
            "batch_id": kwargs["batch_id"],
            "status": self._status,
            "fencing_token": kwargs["fencing_token"],
            "row_version": kwargs["expected_row_version"] + 1,
        }

    def claim_batch(self, **_kwargs: Any) -> None:
        return None


def _worker(repository: _PreparationRepository) -> HMMEvolutionWorker:
    return HMMEvolutionWorker(
        repository,  # type: ignore[arg-type]
        owner_id="test-worker",
        config=WorkerConfig(runtime_mode="api_worker"),
        executor=object(),  # type: ignore[arg-type]
        submission_preparer=_UnexpectedPreparationFailure(),
    )


def test_worker_continues_after_unexpected_preparation_failure_is_terminalized(
    caplog: pytest.LogCaptureFixture,
) -> None:
    repository = _PreparationRepository()
    worker = _worker(repository)

    with caplog.at_level("ERROR"):
        assert worker.run_once() is True

    assert repository.failure_receipt is not None
    assert repository.failure_receipt["error_code"] == "HMM_EVOLUTION_ERROR"
    assert (
        repository.failure_receipt["reason_code"]
        == "hmm_evolution_unexpected_preparation_failure"
    )
    assert repository.failure_receipt["error_context"] == {
        "exception_chain": [
            {"error_type": "RuntimeError", "message": "manifest construction failed"}
        ]
    }
    assert "failed unexpectedly and was terminalized" in caplog.text
    assert worker.run_once() is False


def test_worker_propagates_when_unexpected_preparation_failure_cannot_be_terminalized() -> None:
    worker = _worker(
        _PreparationRepository(fail_write_error=RuntimeError("durable terminal write failed"))
    )

    with pytest.raises(RuntimeError, match="durable terminal write failed"):
        worker.run_once()
