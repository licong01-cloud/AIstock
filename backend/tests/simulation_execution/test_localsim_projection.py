from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from backend.services.simulation_execution.localsim.models import (
    LocalSimEconomicReceiptV1,
    LocalSimExecutionStateV1,
    LocalSimProjectionOutboxV1,
)
from backend.services.simulation_execution.localsim.projection import (
    LocalSimProjectionCommitRequest,
    LocalSimProjector,
    derive_local_sim_projection_state_contract,
    local_sim_projection_paper_error,
)
from backend.services.trading_core.models import AccountSnapshot


@pytest.mark.parametrize("classification,failed", [
    ("MARKET_STATE_RESIDUAL_AT_CLOSE", False), ("SUSPENDED_AT_CLOSE", False),
    ("SCHEDULE_RESIDUAL_AT_CLOSE", True), ("MARKET_DATA_INTEGRITY_FAILURE", True),
])
def test_market_residual_success_preserves_unfilled_facts_and_real_failures(classification: str, failed: bool) -> None:
    state = LocalSimExecutionStateV1(
        run_id="run-1", binding_id="bind-1", trade_date="2026-09-17", plan_id="plan-1",
        intent_id="intent-1", algo_instance_id="algo-1", portfolio_id="paper-1", order_id="order-1",
        symbol="688498.SH", side="BUY", total_quantity=588, filled_quantity=388, remaining_quantity=200,
        algo_code="TWAP", order_status="PARTIALLY_FILLED", schedule_version="twap-v1",
        runtime_status="FAILED_TERMINAL" if classification == "MARKET_DATA_INTEGRITY_FAILURE" else "EXPIRED_WITH_RESIDUAL",
        terminal_reason="CLOSED", residual_classification=classification,
        causality_cursor=datetime(2026, 9, 17, 9, 30), idempotency_key="test",
    )
    contract = derive_local_sim_projection_state_contract(
        execution_states=(state,), historical_residual=None, intraday_status="RUNNING",
        failed_status="FAILED_TERMINAL", succeeded_status="SUCCEEDED",
    )
    assert contract["terminal_failure"] is failed
    assert contract["final_status"] == ("FAILED_TERMINAL" if failed else "SUCCEEDED")
    assert contract["nonfilled_terminal_states"][0].remaining_quantity == 200
    error = local_sim_projection_paper_error(
        historical_residual=None, nonfilled_terminal_states=contract["nonfilled_terminal_states"], terminal_failure=failed,
    )
    assert (error is not None) is failed


class _RuntimeRepository:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.run_payload: dict[str, Any] = {}

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

    def get_simulation_daily_run(self, _run_id: str) -> Any:
        return SimpleNamespace(run_payload_json=self.run_payload)


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


def test_projector_validates_and_dispatches_one_pending_outbox() -> None:
    calls: list[str] = []
    runtime_repository = _RuntimeRepository(calls)
    receipt = LocalSimEconomicReceiptV1(
        run_id="run-1",
        binding_id="binding-1",
        trade_date="2026-08-31",
        plan_id="plan-1",
        generation=1,
        economic_facts={"schema_version": "economic_v1"},
    )
    outbox = LocalSimProjectionOutboxV1(
        receipt_id=receipt.receipt_id,
        run_id="run-1",
        plan_id="plan-1",
        generation=1,
        economic_hash=receipt.economic_hash,
        projection_payload={"schema_version": "projection_v1"},
    )
    runtime_repository.run_payload = {"local_sim_projection_outbox_v1": outbox.model_dump(mode="json")}
    projector = LocalSimProjector(
        runtime_repository=runtime_repository,
        paper_repository=_PaperRepository(calls),
    )

    projector.replay_pending(
        run_id="run-1",
        project_valuation_pending=None,
        project_outbox=lambda run_id: calls.append(f"project:{run_id}"),
    )

    assert calls == ["project:run-1"]


def test_projector_rebuilds_one_projected_generation_without_scheduler_interpretation() -> None:
    calls: list[str] = []
    runtime_repository = _RuntimeRepository(calls)
    receipt = LocalSimEconomicReceiptV1(
        run_id="run-1",
        binding_id="binding-1",
        trade_date="2026-08-31",
        plan_id="plan-1",
        generation=1,
        economic_facts={"schema_version": "economic_v1"},
    )
    account = AccountSnapshot(
        portfolio_id="portfolio-1",
        cash=100_000,
        market_value=0,
        nav=100_000,
        snapshot_time=datetime(2026, 8, 31, 15, 0, tzinfo=UTC),
    )
    outbox = LocalSimProjectionOutboxV1(
        receipt_id=receipt.receipt_id,
        run_id="run-1",
        plan_id="plan-1",
        generation=1,
        economic_hash=receipt.economic_hash,
        projection_payload={
            "schema_version": "local_sim_projection_payload_v1",
            "positions": [],
            "marks": [],
            "account_snapshot": account.model_dump(mode="json"),
        },
        status="PROJECTED",
    )
    runtime_repository.run_payload = {
        "local_sim_projection_outbox_v1": outbox.model_dump(mode="json"),
        "strategy_performance": {"total_return": 0.0},
        "local_sim_persistence": {
            "order_count": 0,
            "fill_count": 0,
            "cash_ledger_count": 0,
            "position_count": 0,
            "active_state_count": 0,
            "residual_state_count": 0,
            "terminal": True,
        },
    }
    projector = LocalSimProjector(runtime_repository=runtime_repository)

    result = projector.existing_projection_result(
        run_id="run-1",
        observed_positions={},
        observed_account=SimpleNamespace(cash=100_000),
    )

    assert result.outbox_id == outbox.outbox_id
    assert result.cash == 100_000
    assert result.payload["terminal"] is True


def test_projector_source_has_no_broker_or_signal_execution_path() -> None:
    import inspect

    source = inspect.getsource(LocalSimProjector)

    assert "local_broker" not in source
    assert "selection" not in source
    assert "submit_order" not in source
