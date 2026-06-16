from __future__ import annotations

from datetime import date

from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    InMemoryMiniQMTExecutionRuntimeRepository,
    JsonFileMiniQMTExecutionRuntimeRepository,
    MiniQMTAlgoInstanceStatus,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionRuntimeClient,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntime,
    MiniQMTExecutionRuntimeConfig,
    MiniQMTOmsState,
)
from backend.services.miniqmt_execution_runtime.repository import MINIQMT_EXECUTION_RUNTIME_STORE_PATH_ENV
from backend.services.trading_core.models import OrderSide


def _config() -> MiniQMTExecutionRuntimeConfig:
    return MiniQMTExecutionRuntimeConfig(
        runtime_id="mqrt_phase2_restart_recovery",
        account_group_id="ag_minqmt_main_sim",
        trade_date=date(2026, 6, 9),
        runtime_config_hash="runtime_hash_phase2_restart",
    )


def test_restart_recovery_rebuilds_active_state_and_syncs_broker_before_new_orders(tmp_path) -> None:
    store_path = tmp_path / "runtime-store.json"
    repo = JsonFileMiniQMTExecutionRuntimeRepository(store_path)
    first_gateway = FakeMiniQMTGateway()
    first_runtime = MiniQMTExecutionRuntime(config=_config(), repository=repo, gateway=first_gateway)
    first_runtime.start()
    algo = first_runtime.create_algo_instance(
        parent_intent_id="intent_sell_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.SELL,
        target_quantity=1000,
        algo_code="BEST_LIMIT_MINIQMT",
    )
    child = first_runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=500, price=10.1)
    submitted_before_restart = len(first_gateway.submitted_orders)

    recovery_gateway = FakeMiniQMTGateway(
        orders=[
            {
                "broker_order_id": child.broker_order_id,
                "stock_code": "000001.SZ",
                "status": "SUBMITTED",
                "order_volume": 500,
            }
        ],
        trades=[],
        positions=[{"stock_code": "000001.SZ", "can_sell": 500}],
    )
    recovered_repo = JsonFileMiniQMTExecutionRuntimeRepository(store_path)
    restarted_runtime = MiniQMTExecutionRuntime(config=_config(), repository=recovered_repo, gateway=recovery_gateway)

    snapshot = restarted_runtime.recover()

    assert len(recovery_gateway.submitted_orders) == 0
    assert submitted_before_restart == 1
    assert snapshot.runtime.oms_state == MiniQMTOmsState.RECONCILED
    assert [item.algo_instance_id for item in snapshot.active_algo_instances] == [algo.algo_instance_id]
    assert [item.child_order_id for item in snapshot.active_child_orders] == [child.child_order_id]
    assert snapshot.broker_orders[0]["broker_order_id"] == child.broker_order_id
    assert snapshot.broker_synced_before_new_orders is True

    event_types = [event.event_type for event in snapshot.events]
    broker_synced_index = event_types.index(MiniQMTExecutionEventType.BROKER_SYNCED)
    assert MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED in event_types[:broker_synced_index]
    assert MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED not in event_types[broker_synced_index + 1 :]


def test_restart_recovery_terminalizes_active_algo_when_all_children_cancelled(tmp_path) -> None:
    store_path = tmp_path / "runtime-store.json"
    repo = JsonFileMiniQMTExecutionRuntimeRepository(store_path)
    first_runtime = MiniQMTExecutionRuntime(config=_config(), repository=repo, gateway=FakeMiniQMTGateway())
    first_runtime.start()
    algo = first_runtime.create_algo_instance(
        parent_intent_id="intent_flatten_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.SELL,
        target_quantity=1000,
        algo_code="OPERATOR_FLATTEN",
    )
    child = first_runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=1000, price=10.1)
    repo.upsert_child_order(child.model_copy(update={"status": MiniQMTChildOrderStatus.CANCELLED}))

    recovered_repo = JsonFileMiniQMTExecutionRuntimeRepository(store_path)
    restarted_runtime = MiniQMTExecutionRuntime(
        config=_config(),
        repository=recovered_repo,
        gateway=FakeMiniQMTGateway(orders=[], trades=[], positions=[]),
    )

    snapshot = restarted_runtime.recover()

    assert snapshot.active_algo_instances == []
    assert snapshot.active_child_orders == []
    stored_algo = recovered_repo.list_algo_instances(_config().runtime_id, active_only=False)[0]
    assert stored_algo.status == MiniQMTAlgoInstanceStatus.CANCELLED
    assert stored_algo.metadata["terminalized_by_runtime"] is True
    assert stored_algo.metadata["terminalized_reason"] == "process_restart_recovery"
    runtime_record = recovered_repo.get_runtime(_config().runtime_id)
    assert runtime_record is not None
    assert runtime_record.metadata["last_recovery_terminalized_orphaned_algo_instance_ids"] == [algo.algo_instance_id]
    event_payloads = [event.payload for event in snapshot.events if event.event_type == MiniQMTExecutionEventType.ALGO_ACTION_EMITTED]
    assert any(payload.get("action_type") == "TERMINALIZE_ORPHANED_ALGO" for payload in event_payloads)


def test_default_runtime_client_uses_durable_store_and_survives_client_recreation(tmp_path, monkeypatch) -> None:
    store_path = tmp_path / "product-runtime-store.json"
    monkeypatch.setenv(MINIQMT_EXECUTION_RUNTIME_STORE_PATH_ENV, str(store_path))

    first_client = MiniQMTExecutionRuntimeClient()
    runtime = first_client._runtime(
        account_group_id="ag_product_default",
        trade_date=date(2026, 6, 9),
        runtime_config_hash="hash_product_default",
        runtime_id="mqrt_product_default_durable",
        gateway=FakeMiniQMTGateway(),
    )
    runtime.start()
    algo = runtime.create_algo_instance(
        parent_intent_id="intent_product_default_000001",
        strategy_slot_id="slot_product_default",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )
    child = runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=100, price=10.0)

    recreated_client = MiniQMTExecutionRuntimeClient()
    evidence = recreated_client.evidence_for_runtime(runtime.config.runtime_id, source="restart_probe")

    assert isinstance(first_client.repository, JsonFileMiniQMTExecutionRuntimeRepository)
    assert isinstance(recreated_client.repository, JsonFileMiniQMTExecutionRuntimeRepository)
    assert store_path.exists()
    assert evidence.runtime_id == runtime.config.runtime_id
    assert child.child_order_id in evidence.child_order_ids
    assert evidence.submitted_child_count == 1


def test_in_memory_repository_is_explicit_test_only_and_not_default(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(MINIQMT_EXECUTION_RUNTIME_STORE_PATH_ENV, str(tmp_path / "unused-default.json"))

    default_client = MiniQMTExecutionRuntimeClient()
    test_client = MiniQMTExecutionRuntimeClient(repository=InMemoryMiniQMTExecutionRuntimeRepository())

    assert isinstance(default_client.repository, JsonFileMiniQMTExecutionRuntimeRepository)
    assert isinstance(test_client.repository, InMemoryMiniQMTExecutionRuntimeRepository)
