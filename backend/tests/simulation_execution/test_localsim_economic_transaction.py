from __future__ import annotations

from contextlib import contextmanager
from datetime import date
import inspect
from types import SimpleNamespace
from typing import Any

import pytest

from backend.services.simulation_execution.localsim.economic import (
    LocalSimEconomicCommitRequest,
    LocalSimEconomicCoordinator,
)
from backend.services.simulation_execution.localsim.models import (
    LocalSimEconomicReceiptV1,
    LocalSimProjectionOutboxV1,
)
from backend.services.simulation_execution.localsim.persistence import LocalSimPersistenceCoordinator
from backend.services.simulation_runtime.scheduler import SimulationLifecycleScheduler


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

    def stage_local_sim_economic_commit(self, **kwargs: Any):
        self.calls.append("runtime.stage")
        receipt = LocalSimEconomicReceiptV1(
            run_id=kwargs["run_id"],
            binding_id=kwargs["binding_id"],
            trade_date=kwargs["trade_date"],
            plan_id=kwargs["plan_id"],
            generation=1,
            economic_facts=kwargs["economic_facts"],
        )
        outbox = LocalSimProjectionOutboxV1(
            receipt_id=receipt.receipt_id,
            run_id=kwargs["run_id"],
            plan_id=kwargs["plan_id"],
            generation=receipt.generation,
            economic_hash=receipt.economic_hash,
            projection_payload=kwargs["projection_payload"],
        )
        return receipt, outbox, True

    def readback_local_sim_economic_commit(self, **_kwargs: Any) -> None:
        self.calls.append("runtime.readback")


class _PaperRepository:
    def __init__(self, calls: list[str], *, fail_order: bool = False) -> None:
        self.calls = calls
        self.fail_order = fail_order

    @contextmanager
    def local_sim_economic_transaction(self, _run_id: str):
        self.calls.append("paper.enter")
        try:
            yield object()
        finally:
            self.calls.append("paper.exit")

    def save_order(self, _run_id: str, _order: Any) -> None:
        self.calls.append("paper.order")
        if self.fail_order:
            raise RuntimeError("order write failed")

    def save_fill(self, _run_id: str, _fill: Any) -> None:
        self.calls.append("paper.fill")

    def save_order_event(self, _run_id: str, _event: Any) -> None:
        self.calls.append("paper.event")

    def save_cash_entry(self, _run_id: str, _entry: Any) -> None:
        self.calls.append("paper.cash")

    def save_run_event(self, **_kwargs: Any) -> None:
        self.calls.append("paper.receipt_event")

    def readback_local_sim_economic_facts(self, **_kwargs: Any) -> dict[str, int]:
        self.calls.append("paper.readback")
        return {}


def _request() -> LocalSimEconomicCommitRequest:
    return LocalSimEconomicCommitRequest(
        run_id="run-1",
        binding_id="binding-1",
        trade_date=date(2026, 8, 31),
        plan_id="plan-1",
        states=(),
        expected_versions={},
        economic_facts={"schema_version": "test_economic_v1"},
        projection_payload={"schema_version": "test_projection_v1"},
        status=SimpleNamespace(value="INTRADAY_RUNNING"),
        payload_patch={"last_stage": "LOCAL_SIM_ECONOMIC_COMMITTED"},
        payload_unset=(),
        orders=(SimpleNamespace(order_id="order-1"),),
        fills=(SimpleNamespace(fill_id="fill-1"),),
        events=(SimpleNamespace(event_id="event-1"),),
        cash_entries=(SimpleNamespace(fill_id="fill-1"),),
    )


def test_coordinator_is_the_only_atomic_writer_and_reads_back_after_commit() -> None:
    calls: list[str] = []
    coordinator = LocalSimEconomicCoordinator(
        runtime_repository=_RuntimeRepository(calls),
        paper_repository=_PaperRepository(calls),
        ensure_paper_run=lambda: calls.append("paper.ensure_run"),
    )

    result = coordinator.commit(_request())

    assert result.created is True
    assert calls == [
        "runtime.enter",
        "paper.enter",
        "paper.ensure_run",
        "paper.order",
        "paper.fill",
        "paper.event",
        "paper.cash",
        "runtime.stage",
        "paper.receipt_event",
        "paper.exit",
        "runtime.exit",
        "runtime.readback",
        "paper.readback",
    ]


def test_coordinator_does_not_stage_or_readback_after_paper_write_failure() -> None:
    calls: list[str] = []
    coordinator = LocalSimEconomicCoordinator(
        runtime_repository=_RuntimeRepository(calls),
        paper_repository=_PaperRepository(calls, fail_order=True),
        ensure_paper_run=lambda: calls.append("paper.ensure_run"),
    )

    with pytest.raises(RuntimeError, match="order write failed"):
        coordinator.commit(_request())

    assert "runtime.stage" not in calls
    assert "runtime.readback" not in calls
    assert "paper.readback" not in calls


def test_persistence_owner_has_no_legacy_product_import_or_scheduler_writer() -> None:
    owner_source = inspect.getsource(LocalSimPersistenceCoordinator)
    scheduler_source = inspect.getsource(SimulationLifecycleScheduler)

    assert "from backend.services.paper_trading_v2" not in owner_source
    assert "from backend.services.simulation_runtime" not in owner_source
    assert "stage_local_sim_economic_commit" not in scheduler_source
    assert "stage_local_sim_projection_commit" not in scheduler_source
    assert "local_sim_economic_transaction_scope" not in scheduler_source
