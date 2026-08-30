from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest

from backend.services.simulation_execution.localsim.projection import (
    LocalSimProjectionCommitRequest,
    LocalSimProjector,
)


class _RuntimeRepository:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    @contextmanager
    def local_sim_economic_transaction_scope(self):
        self.calls.append("runtime.enter")
        try:
            yield
        finally:
            self.calls.append("runtime.exit")

    def stage_local_sim_projection_commit(self, **_kwargs: Any) -> Any:
        self.calls.append("runtime.stage_projection")
        return SimpleNamespace(projection_receipt_id="projection-1")


class _PaperRepository:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    @contextmanager
    def local_sim_economic_transaction(self, _run_id: str):
        self.calls.append("paper.enter")
        try:
            yield object()
        finally:
            self.calls.append("paper.exit")


def _request(calls: list[str], *, fail_apply: bool = False) -> LocalSimProjectionCommitRequest:
    def apply_projection() -> None:
        calls.append("paper.apply_projection")
        if fail_apply:
            raise RuntimeError("projection write failed")

    def readback(_receipt: Any) -> str:
        calls.append("projection.readback")
        return "projected-run"

    return LocalSimProjectionCommitRequest(
        run_id="run-1",
        outbox_id="outbox-1",
        generation=1,
        final_status=SimpleNamespace(value="SUCCEEDED"),
        projection_result={"schema_version": "projection_result_v1"},
        payload_patch={"last_stage": "SUCCEEDED"},
        payload_unset=(),
        apply_paper_projection=apply_projection,
        readback=readback,
        on_staged=lambda: calls.append("projection.staged"),
    )


def test_projector_consumes_outbox_without_broker_or_signal_access() -> None:
    calls: list[str] = []
    projector = LocalSimProjector(
        runtime_repository=_RuntimeRepository(calls),
        paper_repository=_PaperRepository(calls),
    )

    result = projector.commit(_request(calls))

    assert result.projected == "projected-run"
    assert calls == [
        "runtime.enter",
        "paper.enter",
        "paper.apply_projection",
        "runtime.stage_projection",
        "paper.exit",
        "runtime.exit",
        "projection.staged",
        "projection.readback",
    ]


def test_projector_never_stages_or_reads_back_after_paper_projection_failure() -> None:
    calls: list[str] = []
    projector = LocalSimProjector(
        runtime_repository=_RuntimeRepository(calls),
        paper_repository=_PaperRepository(calls),
    )

    with pytest.raises(RuntimeError, match="projection write failed"):
        projector.commit(_request(calls, fail_apply=True))

    assert "runtime.stage_projection" not in calls
    assert "projection.staged" not in calls
    assert "projection.readback" not in calls

