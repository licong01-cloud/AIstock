from __future__ import annotations

from datetime import date

import pytest

from backend.services.miniqmt_execution_runtime import (
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTExecutionEventType,
    MiniQMTShadowCompilerAdapter,
    MiniQMTShadowEventLoopAdapter,
    MiniQMTShadowInputEvent,
    MiniQMTShadowParallelRunner,
    MiniQMTShadowReconciler,
    MiniQMTShadowScenario,
    NoBrokerMutationMiniQMTShadowGateway,
)


def _snapshot(
    *,
    runtime_id: str,
    runtime_kind: str,
    child_status: str = "FILLED",
    child_count: int = 1,
    quantity: int = 100,
    cash: float = 99000.0,
    position_quantity: int = 100,
):
    orders = [
        {
            "shadow_key": f"child_{index}",
            "symbol": "000001.SZ",
            "side": "BUY",
            "quantity": quantity,
            "price": 10.0,
            "status": child_status,
        }
        for index in range(child_count)
    ]
    trades = []
    if child_status in {"FILLED", "PARTIAL", "55"}:
        trades.append(
            {
                "shadow_key": "trade_1",
                "symbol": "000001.SZ",
                "side": "BUY",
                "quantity": quantity if child_status == "FILLED" else int(quantity / 2),
                "price": 10.0,
            }
        )
    return {
        "runtime_id": runtime_id,
        "runtime_kind": runtime_kind,
        "ledger": {
            "child_orders": orders,
            "trades": trades,
            "cash": {"available_cash": cash},
            "positions": {"000001.SZ": {"quantity": position_quantity, "market_value": position_quantity * 10.0}},
        },
    }


def _reconciler() -> tuple[MiniQMTShadowReconciler, InMemoryMiniQMTExecutionRuntimeRepository]:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    return MiniQMTShadowReconciler(repository=repo), repo


def _real_replay_events(scenario: MiniQMTShadowScenario) -> list[MiniQMTShadowInputEvent]:
    events = [
        MiniQMTShadowInputEvent(
            event_type="policy",
            payload={"policy_json": {"algo_code": "SNIPER_MINIQMT", "algo_config": {}}},
        ),
        MiniQMTShadowInputEvent(
            event_type="parent_intent",
            payload={
                "intent_id": "intent_shadow_000001",
                "symbol": "000001.SZ",
                "side": "BUY",
                "quantity": 100,
                "limit_price": 10.0,
            },
        ),
        MiniQMTShadowInputEvent(
            event_type="tick",
            payload={
                "symbol": "000001.SZ",
                "price": 9.99,
                "bid_price_1": 9.98,
                "bid_volume_1": 1000,
                "ask_price_1": 9.99,
                "ask_volume_1": 1000,
            },
        ),
    ]
    if scenario == MiniQMTShadowScenario.FULL_FILL:
        events.append(
            MiniQMTShadowInputEvent(
                event_type="trade_fill",
                payload={"parent_intent_id": "intent_shadow_000001", "quantity": 100, "price": 9.99, "cumulative_quantity": 100},
            )
        )
    elif scenario == MiniQMTShadowScenario.PARTIAL_55_STREAM:
        events.append(
            MiniQMTShadowInputEvent(
                event_type="partial_fill_55",
                payload={"parent_intent_id": "intent_shadow_000001", "quantity": 55, "price": 9.99, "cumulative_quantity": 55},
            )
        )
    elif scenario == MiniQMTShadowScenario.REJECT:
        events.append(MiniQMTShadowInputEvent(event_type="reject", payload={"parent_intent_id": "intent_shadow_000001"}))
    elif scenario == MiniQMTShadowScenario.CANCEL:
        events.append(MiniQMTShadowInputEvent(event_type="cancel", payload={"parent_intent_id": "intent_shadow_000001"}))
    elif scenario == MiniQMTShadowScenario.DISCONNECT:
        events.append(
            MiniQMTShadowInputEvent(event_type="disconnect", payload={"reason": "shadow broker disconnect"})
        )
    elif scenario == MiniQMTShadowScenario.RESTART_RECOVERY:
        events.extend(
            [
                MiniQMTShadowInputEvent(
                    event_type="partial_fill_55",
                    payload={
                        "parent_intent_id": "intent_shadow_000001",
                        "quantity": 55,
                        "price": 9.99,
                        "cumulative_quantity": 55,
                    },
                ),
                MiniQMTShadowInputEvent(event_type="restart_recovery", payload={"reason": "process_restart"}),
            ]
        )
    return events


class _EchoShadowAdapter:
    def __init__(self, *, child_status: str, broker_mutated: bool = False) -> None:
        self.child_status = child_status
        self.broker_mutated = broker_mutated
        self.received_event_types: list[str] = []

    def compute_shadow_snapshot(self, *, runtime_id: str, input_events: tuple[MiniQMTShadowInputEvent, ...], metadata: dict):
        self.received_event_types = [event.event_type for event in input_events]
        snapshot = _snapshot(runtime_id=runtime_id, runtime_kind=str(metadata["runtime_kind"]), child_status=self.child_status)
        snapshot["metadata"] = {"broker_mutated": self.broker_mutated, "input_event_count": len(input_events)}
        return snapshot


@pytest.mark.parametrize(
    "scenario,child_status",
    [
        (MiniQMTShadowScenario.FULL_FILL, "FILLED"),
        (MiniQMTShadowScenario.PARTIAL_55_STREAM, "55"),
        (MiniQMTShadowScenario.DELAY, "SUBMITTED"),
        (MiniQMTShadowScenario.REJECT, "REJECTED"),
        (MiniQMTShadowScenario.CANCEL, "CANCELLED"),
        (MiniQMTShadowScenario.DISCONNECT, "SUBMITTED"),
        (MiniQMTShadowScenario.RESTART_RECOVERY, "PARTIAL"),
    ],
)
def test_phase5_shadow_reconciles_design_matrix_without_fatal_drift(
    scenario: MiniQMTShadowScenario,
    child_status: str,
) -> None:
    reconciler, repo = _reconciler()
    runtime_id = f"mqrt_shadow_{scenario.value}"

    report = reconciler.reconcile(
        runtime_id=runtime_id,
        scenario=scenario,
        a_runtime=_snapshot(runtime_id=runtime_id, runtime_kind="event_loop", child_status=child_status),
        b_runtime=_snapshot(runtime_id=runtime_id, runtime_kind="compiler", child_status=child_status),
        metadata={"trade_date": date(2026, 6, 23).isoformat(), "account_group_id": "shadow_account"},
    )

    assert report.durable_event_id
    assert report.fatal_differences == []
    runtime_record = repo.get_runtime(runtime_id)
    assert runtime_record is not None
    assert runtime_record.metadata["last_shadow_reconciliation"]["scenario"] == scenario.value
    events = repo.list_events(runtime_id)
    assert events[-1].event_type == MiniQMTExecutionEventType.SHADOW_RECONCILIATION_REPORTED
    assert events[-1].source == "shadow"


@pytest.mark.parametrize("scenario", list(MiniQMTShadowScenario))
def test_phase5_shadow_parallel_runner_replays_design_scenarios_through_a_and_b_adapters(
    scenario: MiniQMTShadowScenario,
) -> None:
    reconciler, repo = _reconciler()
    runner = MiniQMTShadowParallelRunner(reconciler=reconciler)

    report = runner.run(
        runtime_id=f"mqrt_shadow_replay_{scenario.value}",
        scenario=scenario,
        input_events=_real_replay_events(scenario),
        event_loop_adapter=MiniQMTShadowEventLoopAdapter(repository=repo),
        compiler_adapter=MiniQMTShadowCompilerAdapter(repository=repo),
        metadata={
            "trade_date": date(2026, 6, 23).isoformat(),
            "account_group_id": "shadow_account",
            "portfolio_id": "portfolio_scope",
            "strategy_slot_id": "slot_scope",
            "binding_id": "binding_scope",
            "run_id": "run_scope",
            "execution_plan_id": "plan_scope",
        },
    )

    assert report.fatal_differences == []
    assert report.metadata["portfolio_id"] == "portfolio_scope"
    assert report.metadata["strategy_slot_id"] == "slot_scope"
    assert report.metadata["binding_id"] == "binding_scope"
    assert report.metadata["run_id"] == "run_scope"
    assert report.metadata["execution_plan_id"] == "plan_scope"
    assert report.a_runtime.metadata["broker_mutated"] is False
    assert report.a_runtime.metadata["broker_called"] is False
    assert report.b_runtime.metadata["broker_mutated"] is False
    assert report.b_runtime.metadata["broker_called"] is False
    assert repo.list_events(f"mqrt_shadow_replay_{scenario.value}")[-1].event_type == (
        MiniQMTExecutionEventType.SHADOW_RECONCILIATION_REPORTED
    )


def test_phase5_shadow_reconciliation_louds_on_child_order_count_drift_and_persists_report() -> None:
    reconciler, repo = _reconciler()

    with pytest.raises(RuntimeError, match="MINIQMT_SHADOW_RECONCILIATION_FATAL"):
        reconciler.reconcile(
            runtime_id="mqrt_shadow_count_drift",
            scenario=MiniQMTShadowScenario.PARTIAL_55_STREAM,
            a_runtime=_snapshot(runtime_id="mqrt_shadow_count_drift", runtime_kind="event_loop", child_count=2),
            b_runtime=_snapshot(runtime_id="mqrt_shadow_count_drift", runtime_kind="compiler", child_count=1),
            metadata={"trade_date": date(2026, 6, 23).isoformat()},
        )

    event = repo.list_events("mqrt_shadow_count_drift")[-1]
    assert event.event_type == MiniQMTExecutionEventType.SHADOW_RECONCILIATION_REPORTED
    assert event.payload["differences"][0]["reason_code"] == "MINIQMT_SHADOW_CHILD_ORDER_COUNT_DRIFT"
    runtime_record = repo.get_runtime("mqrt_shadow_count_drift")
    assert runtime_record is not None
    assert runtime_record.metadata["last_shadow_reconciliation"]["differences"][0]["severity"] == "FATAL"


def test_phase5_parallel_runner_feeds_same_input_to_a_and_b_then_persists_reconciliation() -> None:
    reconciler, repo = _reconciler()
    runner = MiniQMTShadowParallelRunner(reconciler=reconciler)
    a_adapter = _EchoShadowAdapter(child_status="FILLED")
    b_adapter = _EchoShadowAdapter(child_status="FILLED")
    input_events = [
        {"event_type": "tick", "payload": {"symbol": "000001.SZ", "price": 10.0}},
        {"event_type": "partial_fill_55", "payload": {"quantity": 50}},
        {"event_type": "restart_recovery", "payload": {"source": "durable_oms"}},
    ]

    report = runner.run(
        runtime_id="mqrt_shadow_parallel_same_input",
        scenario=MiniQMTShadowScenario.RESTART_RECOVERY,
        input_events=input_events,
        event_loop_adapter=a_adapter,
        compiler_adapter=b_adapter,
        metadata={"trade_date": date(2026, 6, 23).isoformat()},
    )

    assert report.fatal_differences == []
    assert a_adapter.received_event_types == b_adapter.received_event_types
    assert a_adapter.received_event_types == ["tick", "partial_fill_55", "restart_recovery"]
    assert repo.list_events("mqrt_shadow_parallel_same_input")[-1].event_type == (
        MiniQMTExecutionEventType.SHADOW_RECONCILIATION_REPORTED
    )


def test_phase5_parallel_runner_refuses_shadow_adapter_that_mutates_broker() -> None:
    reconciler, _repo = _reconciler()
    runner = MiniQMTShadowParallelRunner(reconciler=reconciler)

    with pytest.raises(RuntimeError, match="MINIQMT_SHADOW_BROKER_MUTATION_DETECTED"):
        runner.run(
            runtime_id="mqrt_shadow_mutation_detected",
            scenario=MiniQMTShadowScenario.FULL_FILL,
            input_events=[{"event_type": "tick", "payload": {"symbol": "000001.SZ", "price": 10.0}}],
            event_loop_adapter=_EchoShadowAdapter(child_status="FILLED", broker_mutated=True),
            compiler_adapter=_EchoShadowAdapter(child_status="FILLED"),
        )


def test_phase5_shadow_reconciliation_detects_cash_position_and_trade_drift_loudly() -> None:
    reconciler, repo = _reconciler()

    with pytest.raises(RuntimeError, match="MINIQMT_SHADOW_RECONCILIATION_FATAL"):
        reconciler.reconcile(
            runtime_id="mqrt_shadow_ledger_drift",
            scenario=MiniQMTShadowScenario.RESTART_RECOVERY,
            a_runtime=_snapshot(
                runtime_id="mqrt_shadow_ledger_drift",
                runtime_kind="event_loop",
                cash=99000.0,
                position_quantity=100,
            ),
            b_runtime=_snapshot(
                runtime_id="mqrt_shadow_ledger_drift",
                runtime_kind="compiler",
                cash=98000.0,
                position_quantity=200,
            ),
            metadata={"trade_date": date(2026, 6, 23).isoformat()},
        )

    reason_codes = {
        item["reason_code"]
        for item in repo.list_events("mqrt_shadow_ledger_drift")[-1].payload["differences"]
    }
    assert "MINIQMT_SHADOW_CASH_DRIFT" in reason_codes
    assert "MINIQMT_SHADOW_POSITION_DRIFT" in reason_codes


def test_phase5_shadow_dry_run_gateway_refuses_real_submit_or_cancel() -> None:
    gateway = NoBrokerMutationMiniQMTShadowGateway()

    with pytest.raises(RuntimeError, match="MINIQMT_SHADOW_BROKER_MUTATION_BLOCKED"):
        gateway.submit_child_order(object())
    with pytest.raises(RuntimeError, match="MINIQMT_SHADOW_BROKER_MUTATION_BLOCKED"):
        gateway.cancel_child_order(object(), reason="operator")
    with pytest.raises(RuntimeError, match="MINIQMT_SHADOW_BROKER_SYNC_UNAVAILABLE"):
        gateway.sync_orders(runtime_id="mqrt_shadow")
    assert gateway.submitted_orders == []
