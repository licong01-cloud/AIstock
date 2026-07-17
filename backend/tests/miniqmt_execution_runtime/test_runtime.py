from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import date
from decimal import Decimal

import pytest

import backend.services.miniqmt_execution_runtime.client as miniqmt_runtime_client

from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTAlgoInstanceStatus,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionRuntime,
    MiniQMTExecutionRuntimeClient,
    MiniQMTExecutionRuntimeConfig,
)
from backend.services.qmt_strategy_ledger.models import (
    BUY_ORDER_TYPE,
    OrderBatchStatus,
    OrderLedgerRecord,
    PositionLotRecord,
    STATUS_CANCELLED,
    STATUS_FILLED,
    STATUS_PART_SUCC,
    STATUS_REJECTED,
    VirtualAccount,
    VirtualAccountStatus,
)
from backend.services.qmt_strategy_ledger.order_service import (
    ManagedOrderSubmitResult,
    OrderPreflightResult,
)
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.trading_core.errors import BrokerSubmitError
from backend.services.trading_core.models import OrderIntent, OrderSide, OrderType


TRADE_DATE = date(2026, 6, 22)


def _runtime(
    *,
    gateway: FakeMiniQMTGateway | None = None,
) -> tuple[MiniQMTExecutionRuntime, InMemoryMiniQMTExecutionRuntimeRepository, FakeMiniQMTGateway]:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    broker_gateway = gateway or FakeMiniQMTGateway()
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id="mqrt_bug470_order_lifecycle",
            account_group_id="ag_bug470",
            trade_date=TRADE_DATE,
            runtime_config_hash="runtime_hash_bug470",
        ),
        repository=repo,
        gateway=broker_gateway,
    )
    runtime.start()
    return runtime, repo, broker_gateway


def _submit_child(runtime: MiniQMTExecutionRuntime):
    algo = runtime.create_algo_instance(
        parent_intent_id="intent_bug470",
        strategy_slot_id="slot_bug470",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )
    child = runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=100, price=10.0)
    assert child.broker_order_id is not None
    return algo, child


def _submit_vnpy_child_order(
    *,
    symbol: str,
    side: OrderSide,
    target_quantity: int,
    min_volume: int | None = None,
    volume_increment: int | None = None,
) -> tuple[
    MiniQMTExecutionRuntime,
    InMemoryMiniQMTExecutionRuntimeRepository,
    FakeMiniQMTGateway,
]:
    runtime, repo, gateway = _runtime(gateway=FakeMiniQMTGateway())
    kwargs = {}
    if min_volume is not None:
        kwargs["min_volume"] = min_volume
    if volume_increment is not None:
        kwargs["volume_increment"] = volume_increment
    runtime.create_vnpy_algo_instance(
        parent_intent_id=f"intent_board_lot_{symbol}_{side.value}",
        strategy_slot_id="slot_board_lot",
        symbol=symbol,
        side=side,
        target_quantity=target_quantity,
        algo_code="SNIPER_MINIQMT",
        limit_price=10.0,
        metadata={
            "runtime_child_context": {
                "strategy_id": "strategy_board_lot",
                "strategy_name": "slot_board_lot",
                "order_remark": f"remark_board_lot_{symbol}_{side.value}",
            },
        },
        **kwargs,
    )
    runtime.on_tick(
        symbol=symbol,
        price=10.0,
        payload={
            "bid_price_1": 10.0,
            "bid_volume_1": max(target_quantity * 2, 1000),
            "ask_price_1": 10.0,
            "ask_volume_1": max(target_quantity * 2, 1000),
        },
    )
    return runtime, repo, gateway


def _preflight() -> OrderPreflightResult:
    return OrderPreflightResult(
        allowed=True,
        errors=(),
        strategy_id="strategy_bug470",
        estimated_notional=Decimal("1000"),
        estimated_fee=Decimal("0"),
        freeze_amount=Decimal("1000"),
        available_cash=Decimal("100000"),
        strategy_available_sell_quantity=None,
        pending_sell_quantity=None,
        broker_can_sell=None,
    )


def _ledger_order(*, qmt_order_id: str, order_status: int, traded_volume: int, order_volume: int = 100) -> OrderLedgerRecord:
    return OrderLedgerRecord(
        intent_id="intent_bug470",
        strategy_id="strategy_bug470",
        strategy_name="slot_bug470",
        qmt_order_id=qmt_order_id,
        symbol="000001.SZ",
        order_type=BUY_ORDER_TYPE,
        order_volume=order_volume,
        traded_volume=traded_volume,
        order_status=order_status,
        account_id="QMT_SIM_ACCOUNT",
        trade_date=TRADE_DATE,
        price=Decimal("10.0"),
        status_msg=f"xtquant status {order_status}",
        order_remark="remark_bug470",
    )


def test_runtime_recover_backfills_partial_fill_child_status_from_broker_status_55() -> None:
    runtime, repo, gateway = _runtime()
    _algo, child = _submit_child(runtime)
    gateway._orders[0].update(
        {
            "order_status": STATUS_PART_SUCC,
            "order_volume": 100,
            "traded_volume": 60,
            "status": "PARTIALLY_FILLED",
        }
    )

    snapshot = runtime.recover()

    stored = repo.list_child_orders(runtime.config.runtime_id, active_only=False)[0]
    assert stored.status == MiniQMTChildOrderStatus.PARTIALLY_FILLED
    assert stored.metadata["broker_reconciled_status"] == MiniQMTChildOrderStatus.PARTIALLY_FILLED.value
    assert stored.metadata["broker_reconcile_order"]["order_status"] == STATUS_PART_SUCC
    assert [item.child_order_id for item in snapshot.active_child_orders] == [child.child_order_id]


@pytest.mark.parametrize(
    ("order_status", "expected_child_status", "expected_algo_status", "traded_volume"),
    [
        (STATUS_CANCELLED, MiniQMTChildOrderStatus.CANCELLED, MiniQMTAlgoInstanceStatus.CANCELLED, 60),
        (STATUS_FILLED, MiniQMTChildOrderStatus.FILLED, MiniQMTAlgoInstanceStatus.COMPLETED, 0),
        (STATUS_REJECTED, MiniQMTChildOrderStatus.REJECTED, MiniQMTAlgoInstanceStatus.FAILED, 20),
    ],
)
def test_runtime_recover_treats_xtquant_terminal_statuses_as_terminal(
    order_status: int,
    expected_child_status: MiniQMTChildOrderStatus,
    expected_algo_status: MiniQMTAlgoInstanceStatus,
    traded_volume: int,
) -> None:
    runtime, repo, gateway = _runtime(gateway=FakeMiniQMTGateway())
    algo, _child = _submit_child(runtime)
    gateway._orders[0].update(
        {
            "order_status": order_status,
            "order_volume": 100,
            "traded_volume": traded_volume,
            "status": "SUBMITTED",
        }
    )

    snapshot = runtime.recover()

    stored_child = repo.list_child_orders(runtime.config.runtime_id, active_only=False)[0]
    stored_algo = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[0]
    assert stored_child.status == expected_child_status
    assert stored_algo.algo_instance_id == algo.algo_instance_id
    assert stored_algo.status == expected_algo_status
    assert snapshot.active_child_orders == []


def test_runtime_client_managed_child_sync_uses_ledger_partial_status() -> None:
    runtime, repo, _gateway = _runtime()
    _algo, child = _submit_child(runtime)
    client = MiniQMTExecutionRuntimeClient(repository=repo)
    result = ManagedOrderSubmitResult(
        success=True,
        intent_id="intent_bug470",
        qmt_order_id=child.broker_order_id,
        broker_message="accepted",
        preflight=_preflight(),
        broker_called=True,
    )

    updated = client._sync_managed_child_result(
        runtime_id=runtime.config.runtime_id,
        child_order_id=child.child_order_id,
        managed_result=result,
        ledger_order=_ledger_order(qmt_order_id=child.broker_order_id or "", order_status=STATUS_PART_SUCC, traded_volume=60),
        source="bug470_unit",
    )

    assert updated is not None
    assert updated.status == MiniQMTChildOrderStatus.PARTIALLY_FILLED
    assert updated.metadata["broker_synced_child_status"] == MiniQMTChildOrderStatus.PARTIALLY_FILLED.value
    assert updated.metadata["broker_order_ledger"]["order_status"] == STATUS_PART_SUCC


@pytest.mark.parametrize("symbol", ["688001.SH", "689001.SH"])
def test_vnpy_create_derives_star_market_board_lot_without_flooring(symbol: str) -> None:
    _runtime_obj, repo, gateway = _submit_vnpy_child_order(
        symbol=symbol,
        side=OrderSide.BUY,
        target_quantity=1215,
    )

    child = repo.list_child_orders("mqrt_bug470_order_lifecycle", active_only=False)[0]
    algo = repo.list_algo_instances("mqrt_bug470_order_lifecycle", active_only=False)[0]
    assert child.quantity == 1215
    assert gateway.submitted_orders[0].quantity == 1215
    assert algo.metadata["min_volume"] == 200
    assert algo.metadata["volume_increment"] == 1


@pytest.mark.parametrize("symbol", ["600000.SH", "000001.SZ", "300001.SZ", "301001.SZ"])
def test_vnpy_create_keeps_main_and_chinext_hundred_share_board_lot(symbol: str) -> None:
    _runtime_obj, repo, _gateway = _submit_vnpy_child_order(
        symbol=symbol,
        side=OrderSide.BUY,
        target_quantity=1215,
    )

    child = repo.list_child_orders("mqrt_bug470_order_lifecycle", active_only=False)[0]
    algo = repo.list_algo_instances("mqrt_bug470_order_lifecycle", active_only=False)[0]
    assert child.quantity == 1200
    assert algo.metadata["min_volume"] == 100
    assert algo.metadata["volume_increment"] == 100


def test_vnpy_create_keeps_star_market_sell_residual_exemption() -> None:
    _runtime_obj, repo, _gateway = _submit_vnpy_child_order(
        symbol="688001.SH",
        side=OrderSide.SELL,
        target_quantity=123,
    )

    child = repo.list_child_orders("mqrt_bug470_order_lifecycle", active_only=False)[0]
    assert child.quantity == 123


@pytest.mark.parametrize("symbol", ["999999.SH", "ABC"])
def test_vnpy_create_loudly_rejects_unknown_symbol_instead_of_defaulting_to_hundred_lot(symbol: str) -> None:
    runtime, repo, _gateway = _runtime(gateway=FakeMiniQMTGateway())

    with pytest.raises(RuntimeError, match="MINIQMT_EVENT_LOOP_BOARD_LOT_RULE_UNRESOLVED"):
        runtime.create_vnpy_algo_instance(
            parent_intent_id="intent_unknown_board_lot",
            strategy_slot_id="slot_board_lot",
            symbol=symbol,
            side=OrderSide.BUY,
            target_quantity=1215,
            algo_code="SNIPER_MINIQMT",
            limit_price=10.0,
        )

    assert repo.list_algo_instances(runtime.config.runtime_id, active_only=False) == []
    assert repo.list_child_orders(runtime.config.runtime_id, active_only=False) == []


def test_vnpy_create_respects_explicit_board_lot_override_for_event_loop_path() -> None:
    _runtime_obj, repo, _gateway = _submit_vnpy_child_order(
        symbol="688001.SH",
        side=OrderSide.BUY,
        target_quantity=1215,
        min_volume=100,
        volume_increment=100,
    )

    child = repo.list_child_orders("mqrt_bug470_order_lifecycle", active_only=False)[0]
    algo = repo.list_algo_instances("mqrt_bug470_order_lifecycle", active_only=False)[0]
    assert child.quantity == 1200
    assert algo.metadata["min_volume"] == 100
    assert algo.metadata["volume_increment"] == 100


def test_compiler_adapter_rejects_retired_b_route_loudly() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()

    with pytest.raises(BrokerSubmitError) as exc_info:
        MiniQMTExecutionRuntimeClient(repository=repo, runtime_kind="compiler")

    assert exc_info.value.context["reason_code"] == "MINIQMT_SIM_COMPILER_ROUTE_RETIRED"
    assert exc_info.value.context["stage"] == "MINIQMT_RUNTIME_KIND_REJECTED"
    assert repo.list_child_orders("mqrt_compiler_star_board_lot", active_only=False) == []

def test_event_loop_submit_rejects_non_broker_quote_source_loudly() -> None:
    repo, qmt_repo, qmt_client, intent = _event_loop_client_fixture()
    client = MiniQMTExecutionRuntimeClient(
        repository=repo,
        strategy_ledger_repository=qmt_repo,
        runtime_kind="event_loop",
    )

    with pytest.raises(BrokerSubmitError) as exc_info:
        client.submit_event_loop_vnpy_parent_intents(
            parent_intents=[intent],
            policy_context=_event_loop_policy(),
            account_group_id="acct_event_loop",
            trade_date=TRADE_DATE,
            runtime_config_hash="runtime_hash_event_loop_quote_source",
            runtime_id="mqrt_event_loop_quote_source",
            strategy_slot_id="slot_event_loop",
            qmt_client=qmt_client,
            strategy_name="strategy_event_loop",
            order_remark_prefix="evtloop",
            account_id="acct_event_loop",
            quote_provider=lambda _symbol: {
                "source": "TDX_REALTIME.batch_quote",
                "price": 10.0,
                "ask_price_1": 10.0,
                "ask_volume_1": 1000,
            },
        )

    assert exc_info.value.context["reason_code"] == "MINIQMT_EVENT_LOOP_BROKER_QUOTE_SOURCE_INVALID"
    assert exc_info.value.context["required_quote_source"] == "MINIQMT_REALTIME.broker_quote"
    assert "TDX_REALTIME.batch_quote" in str(exc_info.value.context["quote_source"])


def test_event_loop_submit_missing_broker_quote_fails_loudly_before_order() -> None:
    repo, qmt_repo, qmt_client, intent = _event_loop_client_fixture()
    qmt_client.quotes.clear()
    client = MiniQMTExecutionRuntimeClient(
        repository=repo,
        strategy_ledger_repository=qmt_repo,
        runtime_kind="event_loop",
    )

    with pytest.raises(BrokerSubmitError) as exc_info:
        client.submit_event_loop_vnpy_parent_intents(
            parent_intents=[intent],
            policy_context=_event_loop_policy(),
            account_group_id="acct_event_loop",
            trade_date=TRADE_DATE,
            runtime_config_hash="runtime_hash_event_loop_missing_quote",
            runtime_id="mqrt_event_loop_missing_quote",
            strategy_slot_id="slot_event_loop",
            qmt_client=qmt_client,
            strategy_name="strategy_event_loop",
            order_remark_prefix="evtloop",
            account_id="acct_event_loop",
        )

    assert exc_info.value.context["reason_code"] == "MINIQMT_EVENT_LOOP_BROKER_QUOTE_MISSING"
    assert exc_info.value.context["quote_source"] == "MINIQMT_REALTIME.broker_quote"
    assert qmt_client.place_order_calls == []


def test_event_loop_submit_requires_l1_depth_from_broker_quote() -> None:
    repo, qmt_repo, qmt_client, intent = _event_loop_client_fixture()
    qmt_client.quotes = {
        "000001.SZ": {
            "source": "MINIQMT_REALTIME.broker_quote",
            "price": 10.0,
        }
    }
    client = MiniQMTExecutionRuntimeClient(
        repository=repo,
        strategy_ledger_repository=qmt_repo,
        runtime_kind="event_loop",
    )

    with pytest.raises(BrokerSubmitError) as exc_info:
        client.submit_event_loop_vnpy_parent_intents(
            parent_intents=[intent],
            policy_context=_event_loop_policy(),
            account_group_id="acct_event_loop",
            trade_date=TRADE_DATE,
            runtime_config_hash="runtime_hash_event_loop_depth",
            runtime_id="mqrt_event_loop_depth",
            strategy_slot_id="slot_event_loop",
            qmt_client=qmt_client,
            strategy_name="strategy_event_loop",
            order_remark_prefix="evtloop",
            account_id="acct_event_loop",
        )

    assert exc_info.value.context["reason_code"] == "MINIQMT_EVENT_LOOP_BROKER_QUOTE_DEPTH_MISSING"
    assert exc_info.value.context["missing_fields"] == ["ask_price_1", "ask_volume_1"]
    assert qmt_client.place_order_calls == []


class _EventLoopFakeQmtClient:
    def __init__(self) -> None:
        self.quotes = {
            "000001.SZ": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 10.0,
                "ask_price_1": 10.0,
                "ask_volume_1": 1000,
                "bid_price_1": 9.99,
                "bid_volume_1": 1000,
            }
        }
        self.place_order_calls: list[dict] = []

    def get_orders(self, cancelable_only: bool = False) -> list[dict]:  # noqa: ARG002
        return []

    def get_trades(self) -> list[dict]:
        return []

    def get_positions(self) -> list[dict]:
        return []

    def get_full_tick(self, symbols: list[str]) -> dict[str, dict]:
        return {symbol: dict(self.quotes[symbol]) for symbol in symbols if symbol in self.quotes}

    def place_order(self, **kwargs):
        self.place_order_calls.append(dict(kwargs))
        return 880000000 + len(self.place_order_calls), "accepted"

    def cancel_order(self, order_id: str):
        return True, f"cancelled {order_id}"


def _event_loop_policy(
    algo_code: str = "SNIPER_MINIQMT",
    algo_config: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "policy_json": {
            "algo_code": algo_code,
            "algo_config": dict(algo_config or {}),
        },
        "validated_execution_policy_id": "policy_event_loop",
        "policy_sha256": "policy_sha_event_loop",
    }


def _event_loop_client_fixture() -> tuple[
    InMemoryMiniQMTExecutionRuntimeRepository,
    InMemoryQmtStrategyLedgerRepository,
    _EventLoopFakeQmtClient,
    OrderIntent,
]:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strategy_event_loop",
            strategy_name="strategy_event_loop",
            display_name="EVENT_LOOP strategy",
            account_id="acct_event_loop",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    intent = OrderIntent(
        intent_id="intent_event_loop_buy",
        package_id="pkg_event_loop",
        portfolio_id="portfolio_event_loop",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=100,
        order_type=OrderType.LIMIT,
        limit_price=10.0,
        target_trade_date=TRADE_DATE,
        metadata={
            "strategy_id": "strategy_event_loop",
            "strategy_name": "strategy_event_loop",
            "order_remark_prefix": "evtloop",
        },
    )
    return repo, qmt_repo, _EventLoopFakeQmtClient(), intent


def _durable_event_loop_batch_fixture():
    repo, qmt_repo, qmt_client, first_intent = _event_loop_client_fixture()
    second_intent = first_intent.model_copy(update={"intent_id": "intent_event_loop_buy_second"})
    client = MiniQMTExecutionRuntimeClient(
        repository=repo,
        strategy_ledger_repository=qmt_repo,
        runtime_kind="event_loop",
    )
    result = client.submit_event_loop_vnpy_parent_intents(
        parent_intents=[first_intent, second_intent],
        policy_context=_event_loop_policy(),
        account_group_id="acct_event_loop",
        trade_date=TRADE_DATE,
        runtime_config_hash="runtime_hash_event_loop_durable_batch",
        runtime_id="mqrt_event_loop_durable_batch",
        strategy_slot_id="slot_event_loop",
        qmt_client=qmt_client,
        strategy_name="strategy_event_loop",
        order_remark_prefix="evtloop",
        account_id="acct_event_loop",
    )
    batch = qmt_repo.get_order_batch(result.batch_id or "")
    assert batch is not None
    assert batch.batch_status == OrderBatchStatus.SUCCEEDED
    requests = miniqmt_runtime_client._event_loop_requests_from_batch(batch)
    assert len(requests) == 2
    return client, qmt_repo, qmt_client, batch, requests, len(qmt_client.place_order_calls)


def test_event_loop_valid_durable_batch_replay_preserves_request_result_identity() -> None:
    client, _qmt_repo, qmt_client, batch, requests, initial_broker_calls = _durable_event_loop_batch_fixture()

    replay = client._event_loop_existing_batch_result(
        batch_id=batch.batch_id,
        requests=requests,
        request_count=len(requests),
        managed_order_service=None,
    )

    assert replay is not None
    assert replay.batch_id == batch.batch_id
    assert [item.intent_id for item in replay.results] == [
        request.metadata["runtime_parent_intent_id"] for request in requests
    ]
    assert [item.broker_called for item in replay.results] == [
        item["broker_called"] for item in batch.result_json["results"]
    ]
    assert len(qmt_client.place_order_calls) == initial_broker_calls


@pytest.mark.parametrize(
    ("case", "reason_code", "field_path"),
    [
        (
            "nonmapping_result",
            "MINIQMT_EVENT_LOOP_DURABLE_BATCH_SCHEMA_INVALID",
            "result_json.results[0]",
        ),
        (
            "missing_result",
            "MINIQMT_EVENT_LOOP_DURABLE_BATCH_CARDINALITY_CONFLICT",
            "result_json.results",
        ),
        (
            "intent_identity",
            "MINIQMT_EVENT_LOOP_DURABLE_BATCH_IDENTITY_CONFLICT",
            "result_json.results[0].intent_id",
        ),
        (
            "string_success",
            "MINIQMT_EVENT_LOOP_DURABLE_BATCH_SCHEMA_INVALID",
            "result_json.results[0].success",
        ),
        (
            "string_allowed",
            "MINIQMT_EVENT_LOOP_DURABLE_BATCH_SCHEMA_INVALID",
            "result_json.results[0].preflight",
        ),
        (
            "nonmapping_request",
            "MINIQMT_EVENT_LOOP_DURABLE_BATCH_SCHEMA_INVALID",
            "request_json.orders[0]",
        ),
        (
            "request_batch_identity",
            "MINIQMT_EVENT_LOOP_DURABLE_BATCH_IDENTITY_CONFLICT",
            "request_json.orders[0].metadata.qmt_batch_id",
        ),
        (
            "duplicate_parent",
            "MINIQMT_EVENT_LOOP_DURABLE_BATCH_IDENTITY_CONFLICT",
            "request_json.orders[1].metadata.runtime_parent_intent_id",
        ),
        (
            "string_broker_called",
            "MINIQMT_EVENT_LOOP_DURABLE_BATCH_SCHEMA_INVALID",
            "result_json.results[0].broker_called",
        ),
        (
            "string_amount",
            "MINIQMT_EVENT_LOOP_DURABLE_BATCH_SCHEMA_INVALID",
            "result_json.results[0].preflight.estimated_notional",
        ),
        (
            "success_without_order_id",
            "MINIQMT_EVENT_LOOP_DURABLE_BATCH_IDENTITY_CONFLICT",
            "result_json.results[0]",
        ),
        (
            "no_broker_with_order_id",
            "MINIQMT_EVENT_LOOP_DURABLE_BATCH_IDENTITY_CONFLICT",
            "result_json.results[0].qmt_order_id",
        ),
        (
            "string_request_quantity",
            "MINIQMT_EVENT_LOOP_DURABLE_BATCH_SCHEMA_INVALID",
            "request_json.orders[0].quantity",
        ),
        (
            "boolean_request_quantity",
            "MINIQMT_EVENT_LOOP_DURABLE_BATCH_SCHEMA_INVALID",
            "request_json.orders[0].quantity",
        ),
        (
            "nonfinite_request_price",
            "MINIQMT_EVENT_LOOP_DURABLE_BATCH_SCHEMA_INVALID",
            "request_json.orders[0].price",
        ),
        (
            "parent_alias_conflict",
            "MINIQMT_EVENT_LOOP_DURABLE_BATCH_IDENTITY_CONFLICT",
            "request_json.orders[0].metadata",
        ),
    ],
)
def test_event_loop_durable_batch_replay_rejects_corruption_without_shift_or_padding(
    case: str,
    reason_code: str,
    field_path: str,
) -> None:
    client, qmt_repo, qmt_client, batch, requests, initial_broker_calls = _durable_event_loop_batch_fixture()
    request_json = deepcopy(batch.request_json)
    result_json = deepcopy(batch.result_json)
    raw_results = result_json["results"]
    if case == "nonmapping_result":
        raw_results[0] = "malformed"
    elif case == "missing_result":
        raw_results.pop()
    elif case == "intent_identity":
        raw_results[0]["intent_id"] = "parent_from_another_request"
    elif case == "string_success":
        raw_results[0]["success"] = "false"
    elif case == "string_allowed":
        raw_results[0]["preflight"]["allowed"] = "true"
    elif case == "nonmapping_request":
        request_json["orders"][0] = "malformed"
    elif case == "request_batch_identity":
        request_json["orders"][0]["metadata"]["qmt_batch_id"] = "qmtbatch_other"
    elif case == "duplicate_parent":
        request_json["orders"][1]["metadata"]["runtime_parent_intent_id"] = request_json["orders"][0][
            "metadata"
        ]["runtime_parent_intent_id"]
    elif case == "string_broker_called":
        raw_results[0]["broker_called"] = "true"
    elif case == "string_amount":
        raw_results[0]["preflight"]["estimated_notional"] = "1000"
    elif case == "success_without_order_id":
        raw_results[0]["success"] = True
        raw_results[0]["broker_called"] = True
        raw_results[0]["qmt_order_id"] = None
    elif case == "no_broker_with_order_id":
        raw_results[0]["success"] = False
        raw_results[0]["broker_called"] = False
        raw_results[0]["qmt_order_id"] = "880000001"
    elif case == "string_request_quantity":
        request_json["orders"][0]["quantity"] = "100"
    elif case == "boolean_request_quantity":
        request_json["orders"][0]["quantity"] = True
    elif case == "nonfinite_request_price":
        request_json["orders"][0]["price"] = float("nan")
    elif case == "parent_alias_conflict":
        request_json["orders"][0]["metadata"]["execution_plan_intent_id"] = "parent_other"
    else:  # pragma: no cover - parameter table is closed above.
        raise AssertionError(case)
    qmt_repo.upsert_order_batch(replace(batch, request_json=request_json, result_json=result_json))

    with pytest.raises(BrokerSubmitError) as exc_info:
        client._event_loop_existing_batch_result(
            batch_id=batch.batch_id,
            requests=requests,
            request_count=len(requests),
            managed_order_service=None,
        )

    assert exc_info.value.context["reason_code"] == reason_code
    assert exc_info.value.context["field_path"] == field_path
    assert len(qmt_client.place_order_calls) == initial_broker_calls


def test_event_loop_submit_no_child_order_fails_loudly_without_silent_batch() -> None:
    repo, qmt_repo, qmt_client, intent = _event_loop_client_fixture()
    client = MiniQMTExecutionRuntimeClient(
        repository=repo,
        strategy_ledger_repository=qmt_repo,
        runtime_kind="event_loop",
    )

    with pytest.raises(BrokerSubmitError) as exc_info:
        client.submit_event_loop_vnpy_parent_intents(
            parent_intents=[intent],
            policy_context=_event_loop_policy(
                "TWAP_LITE_MINIQMT",
                {"time": 60, "interval": 60},
            ),
            account_group_id="acct_event_loop",
            trade_date=TRADE_DATE,
            runtime_config_hash="runtime_hash_event_loop_no_child",
            runtime_id="mqrt_event_loop_no_child",
            strategy_slot_id="slot_event_loop",
            qmt_client=qmt_client,
            strategy_name="strategy_event_loop",
            order_remark_prefix="evtloop",
            account_id="acct_event_loop",
        )

    context = exc_info.value.context
    assert context["reason_code"] == "MINIQMT_EVENT_LOOP_NO_CHILD_ORDER"
    assert context["stage"] == "MINIQMT_EVENT_LOOP_SUBMIT_NO_CHILD_ORDER"
    assert context["broker_called"] is False
    assert context["submitted_intents"] == 0
    assert context["failed_intents"] == 1
    assert context["missing_parent_intent_ids"] == [intent.intent_id]
    assert qmt_client.place_order_calls == []


def test_event_loop_first_tick_capture_is_sidecar_only_and_keeps_request_identity() -> None:
    repo, qmt_repo, qmt_client, intent = _event_loop_client_fixture()
    client = MiniQMTExecutionRuntimeClient(
        repository=repo,
        strategy_ledger_repository=qmt_repo,
        runtime_kind="event_loop",
    )
    policy = _event_loop_policy(
        algo_config={
            "tca": {
                "benchmark_policy": {
                    "benchmark_max_age_ms": 10_000,
                    "arrival_forward_window_ms": 2_000,
                    "clock_skew_tolerance_ms": 1_000,
                    "benchmark_max_transport_latency_ms": 3_000,
                    "policy_version": "phase0a_test_v1",
                }
            }
        }
    )

    result = client.submit_event_loop_vnpy_parent_intents(
        parent_intents=[intent],
        policy_context=policy,
        account_group_id="acct_event_loop",
        trade_date=TRADE_DATE,
        runtime_config_hash="runtime_hash_event_loop_tca",
        runtime_id="mqrt_event_loop_tca",
        strategy_slot_id="slot_event_loop",
        qmt_client=qmt_client,
        strategy_name="strategy_event_loop",
        order_remark_prefix="evtloop",
        account_id="acct_event_loop",
        child_context_factory=lambda parent, _index: {
            "execution_plan_id": "plan_tca",
            "execution_plan_hash": "hash_tca",
            "execution_plan_intent_id": parent.intent_id,
        },
    )

    batch = qmt_repo.get_order_batch(result.batch_id or "")
    assert batch is not None
    sidecar = batch.metadata["tca_observation_v1"]
    assert intent.intent_id in sidecar["arrival_capture_by_parent"]
    assert intent.intent_id in sidecar["managed_preflight_eligibility_by_parent"]
    assert "tca_observation_v1" not in batch.request_json["orders"][0]["metadata"]

    rewritten = qmt_repo.upsert_order_batch(replace(batch, metadata={"fresh_runtime_metadata": True}))
    assert rewritten.metadata["tca_observation_v1"] == sidecar

def test_event_loop_tca_policy_missing_is_loud_but_does_not_block_b0() -> None:
    repo, qmt_repo, qmt_client, intent = _event_loop_client_fixture()
    client = MiniQMTExecutionRuntimeClient(
        repository=repo,
        strategy_ledger_repository=qmt_repo,
        runtime_kind="event_loop",
    )

    result = client.submit_event_loop_vnpy_parent_intents(
        parent_intents=[intent],
        policy_context=_event_loop_policy(),
        account_group_id="acct_event_loop",
        trade_date=TRADE_DATE,
        runtime_config_hash="runtime_hash_event_loop_tca_missing",
        runtime_id="mqrt_event_loop_tca_missing",
        strategy_slot_id="slot_event_loop",
        qmt_client=qmt_client,
        strategy_name="strategy_event_loop",
        order_remark_prefix="evtloop",
        account_id="acct_event_loop",
        child_context_factory=lambda parent, _index: {
            "execution_plan_id": "plan_tca_missing",
            "execution_plan_hash": "hash_tca_missing",
            "execution_plan_intent_id": parent.intent_id,
        },
    )

    batch = qmt_repo.get_order_batch(result.batch_id or "")
    assert batch is not None
    error = batch.metadata["tca_observation_v1"]["capture_errors"][intent.intent_id]
    assert error["reason_code"] == "ADAPTIVE_IS_TCA_BENCHMARK_POLICY_MISSING"
    assert result.batch_status == batch.batch_status.value


def test_event_loop_invalid_preflight_evidence_is_durable_error_without_changing_broker_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, qmt_repo, qmt_client, intent = _event_loop_client_fixture()
    client = MiniQMTExecutionRuntimeClient(
        repository=repo,
        strategy_ledger_repository=qmt_repo,
        runtime_kind="event_loop",
    )
    policy = _event_loop_policy(
        algo_config={
            "tca": {
                "benchmark_policy": {
                    "benchmark_max_age_ms": 10_000,
                    "arrival_forward_window_ms": 2_000,
                    "clock_skew_tolerance_ms": 1_000,
                    "benchmark_max_transport_latency_ms": 3_000,
                    "policy_version": "phase0a_test_v1",
                }
            }
        }
    )
    original_to_dict = OrderPreflightResult.to_dict

    def invalid_allowed(self: OrderPreflightResult) -> dict[str, object]:
        payload = original_to_dict(self)
        payload["allowed"] = "false"
        return payload

    monkeypatch.setattr(OrderPreflightResult, "to_dict", invalid_allowed)

    result = client.submit_event_loop_vnpy_parent_intents(
        parent_intents=[intent],
        policy_context=policy,
        account_group_id="acct_event_loop",
        trade_date=TRADE_DATE,
        runtime_config_hash="runtime_hash_event_loop_tca_invalid_evidence",
        runtime_id="mqrt_event_loop_tca_invalid_evidence",
        strategy_slot_id="slot_event_loop",
        qmt_client=qmt_client,
        strategy_name="strategy_event_loop",
        order_remark_prefix="evtloop",
        account_id="acct_event_loop",
        child_context_factory=lambda parent, _index: {
            "execution_plan_id": "plan_tca_invalid_evidence",
            "execution_plan_hash": "hash_tca_invalid_evidence",
            "execution_plan_intent_id": parent.intent_id,
        },
    )

    batch = qmt_repo.get_order_batch(result.batch_id or "")
    assert batch is not None
    sidecar = batch.metadata["tca_observation_v1"]
    error = sidecar["capture_errors"][intent.intent_id]
    assert error["reason_code"] == "ADAPTIVE_IS_TCA_PREFLIGHT_ALLOWED_INVALID"
    assert error["context"]["field"] == "preflight_result.allowed"
    assert error["context"]["raw_type"] == "str"
    assert error["context"]["raw_value"] == "false"
    assert error["retryable"] is False
    assert error["terminal"] is True
    assert error["observation_only"] is True
    assert error["execution_gate"] is False
    assert intent.intent_id in sidecar["arrival_capture_by_parent"]
    assert intent.intent_id not in sidecar["managed_preflight_eligibility_by_parent"]
    assert qmt_client.place_order_calls == []
    assert all(item.broker_called is False for item in result.results)
    assert result.batch_status == batch.batch_status.value


def test_dependent_buy_released_by_sell_trade_event_after_ledger_cash_sufficient() -> None:
    runtime, repo, gateway, qmt_repo = _dependent_runtime(cash=Decimal("0"))
    sell_child = _submit_dependent_sell(runtime, qmt_repo=qmt_repo, quantity=100, price=Decimal("10"))
    buy_algo = _create_deferred_dependent_buy(
        runtime,
        sell_child=sell_child,
        required_cash=Decimal("1000"),
        price=10.0,
        quantity=100,
    )
    assert [order.side for order in gateway.submitted_orders] == [OrderSide.SELL]

    runtime.record_trade_event(
        broker_order_id=sell_child.broker_order_id or "",
        quantity=100,
        price=10.0,
        payload={"trade_id": "trade_d35_full_sell", "cumulative_quantity": 100},
    )

    assert [order.side for order in gateway.submitted_orders] == [OrderSide.SELL, OrderSide.BUY]
    released = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[-1]
    assert released.algo_instance_id == buy_algo.algo_instance_id
    assert released.metadata["dependent_buy_status"] == "RELEASED_SUBMITTED"
    assert released.metadata["dependent_buy_reason_code"] == "MINIQMT_DEPENDENT_BUY_RELEASED_AFTER_SELL_TRADE"
    assert released.metadata["dependent_buy_last_context"]["cash_source"] == "qmt_strategy_ledger.virtual_account.cash"
    assert qmt_repo.get_virtual_account("strategy_bug528").cash == Decimal("1000.000000")


def test_dependent_buy_inferred_from_same_runtime_sell_and_released_by_trade_event() -> None:
    runtime, repo, gateway, qmt_repo = _dependent_runtime(cash=Decimal("0"))
    sell_child = _submit_dependent_sell(runtime, qmt_repo=qmt_repo, quantity=100, price=Decimal("10"))
    buy_algo = _create_inferred_dependent_buy(runtime, price=10.0, quantity=100)
    assert [order.side for order in gateway.submitted_orders] == [OrderSide.SELL]

    runtime.record_trade_event(
        broker_order_id=sell_child.broker_order_id or "",
        quantity=100,
        price=10.0,
        payload={"trade_id": "trade_d35_inferred_sell", "cumulative_quantity": 100},
    )

    assert [order.side for order in gateway.submitted_orders] == [OrderSide.SELL, OrderSide.BUY]
    released = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[-1]
    assert released.algo_instance_id == buy_algo.algo_instance_id
    assert released.metadata["dependent_buy_inferred"] is True
    assert released.metadata["dependent_buy_reason_code"] == "MINIQMT_DEPENDENT_BUY_RELEASED_AFTER_SELL_TRADE"


def test_dependent_buy_partial_sell_keeps_deferred_when_ledger_cash_insufficient() -> None:
    runtime, repo, gateway, qmt_repo = _dependent_runtime(cash=Decimal("0"))
    sell_child = _submit_dependent_sell(runtime, qmt_repo=qmt_repo, quantity=100, price=Decimal("10"))
    buy_algo = _create_deferred_dependent_buy(
        runtime,
        sell_child=sell_child,
        required_cash=Decimal("1000"),
        price=10.0,
        quantity=100,
    )

    runtime.record_trade_event(
        broker_order_id=sell_child.broker_order_id or "",
        quantity=50,
        price=10.0,
        payload={"trade_id": "trade_d35_partial_sell", "cumulative_quantity": 50},
    )

    assert [order.side for order in gateway.submitted_orders] == [OrderSide.SELL]
    deferred = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[-1]
    assert deferred.algo_instance_id == buy_algo.algo_instance_id
    assert deferred.metadata["dependent_buy_status"] == "DEFERRED_WAITING_SELL_PROCEEDS"
    context = deferred.metadata["dependent_buy_last_context"]
    assert deferred.metadata["dependent_buy_reason_code"] == "MINIQMT_DEPENDENT_BUY_CASH_STILL_INSUFFICIENT"
    assert context["available_cash"] == "500.000000"
    assert context["cash_shortfall"] == "500.000000"
    assert context["cash_source"] == "qmt_strategy_ledger.virtual_account.cash"


def test_dependent_buy_blocked_when_dependent_sell_cancelled_without_proceeds() -> None:
    runtime, repo, gateway, qmt_repo = _dependent_runtime(cash=Decimal("0"))
    sell_child = _submit_dependent_sell(runtime, qmt_repo=qmt_repo, quantity=100, price=Decimal("10"))
    buy_algo = _create_deferred_dependent_buy(
        runtime,
        sell_child=sell_child,
        required_cash=Decimal("1000"),
        price=10.0,
        quantity=100,
    )

    runtime.record_order_event(
        broker_order_id=sell_child.broker_order_id or "",
        status="CANCELLED",
        payload={"status": "CANCELLED", "status_msg": "cancelled before any fill"},
    )

    assert [order.side for order in gateway.submitted_orders] == [OrderSide.SELL]
    blocked = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[-1]
    assert blocked.algo_instance_id == buy_algo.algo_instance_id
    assert blocked.status == MiniQMTAlgoInstanceStatus.FAILED
    assert blocked.metadata["dependent_buy_status"] == "BLOCKED_SELL_PROCEEDS_UNAVAILABLE"
    assert (
        blocked.metadata["dependent_buy_reason_code"]
        == "MINIQMT_DEPENDENT_BUY_DEPENDENT_SELL_TERMINAL_WITHOUT_PROCEEDS"
    )


def test_dependent_buy_eod_residual_records_shortfall_without_broker_submit() -> None:
    runtime, repo, gateway, qmt_repo = _dependent_runtime(cash=Decimal("200"))
    sell_child = _submit_dependent_sell(runtime, qmt_repo=qmt_repo, quantity=100, price=Decimal("10"))
    buy_algo = _create_deferred_dependent_buy(
        runtime,
        sell_child=sell_child,
        required_cash=Decimal("1000"),
        price=10.0,
        quantity=100,
    )

    runtime.on_timer(timer_name="EOD_DEPENDENT_BUY_SWEEP", payload={"source": "unit_test"})

    assert [order.side for order in gateway.submitted_orders] == [OrderSide.SELL]
    residual = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[-1]
    assert residual.algo_instance_id == buy_algo.algo_instance_id
    assert residual.status == MiniQMTAlgoInstanceStatus.FAILED
    assert residual.metadata["dependent_buy_status"] == "EOD_RESIDUAL"
    assert residual.metadata["dependent_buy_reason_code"] == "MINIQMT_DEPENDENT_BUY_EOD_RESIDUAL"
    assert residual.metadata["dependent_buy_last_context"]["available_cash"] == "200"
    assert residual.metadata["dependent_buy_last_context"]["cash_shortfall"] == "800"


def test_dependent_buy_release_requires_qmt_strategy_ledger_authority_not_runtime_json_estimate() -> None:
    runtime, repo, gateway = _runtime(gateway=FakeMiniQMTGateway())
    sell_algo = runtime.create_algo_instance(
        parent_intent_id="sell_no_ledger",
        strategy_slot_id="slot_bug528",
        symbol="000001.SZ",
        side=OrderSide.SELL,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )
    sell_child = runtime.submit_child_order(
        algo_instance_id=sell_algo.algo_instance_id,
        quantity=100,
        price=10.0,
        metadata={"strategy_id": "strategy_bug528", "order_remark": "sell_no_ledger"},
    )
    buy_algo = _create_deferred_dependent_buy(
        runtime,
        sell_child=sell_child,
        required_cash=Decimal("1000"),
        price=10.0,
        quantity=100,
        extra_metadata={"estimated_sell_proceeds": "999999999"},
    )

    runtime.record_trade_event(
        broker_order_id=sell_child.broker_order_id or "",
        quantity=100,
        price=10.0,
        payload={"trade_id": "trade_no_ledger", "cumulative_quantity": 100},
    )

    assert [order.side for order in gateway.submitted_orders] == [OrderSide.SELL]
    deferred = repo.list_algo_instances(runtime.config.runtime_id, active_only=False)[-1]
    assert deferred.algo_instance_id == buy_algo.algo_instance_id
    assert deferred.metadata["dependent_buy_status"] == "DEFERRED_WAITING_SELL_PROCEEDS"
    assert deferred.metadata["dependent_buy_reason_code"] == "MINIQMT_DEPENDENT_BUY_LEDGER_AUTHORITY_MISSING"
    assert "estimated_sell_proceeds" not in deferred.metadata["dependent_buy_last_context"]


def _dependent_runtime(
    *,
    cash: Decimal,
) -> tuple[
    MiniQMTExecutionRuntime,
    InMemoryMiniQMTExecutionRuntimeRepository,
    FakeMiniQMTGateway,
    InMemoryQmtStrategyLedgerRepository,
]:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strategy_bug528",
            strategy_name="slot_bug528",
            display_name="BUG-528 strategy",
            account_id="ag_bug528",
            mode="SIM",
            initial_cash=Decimal("1"),
            cash=cash,
            status=VirtualAccountStatus.ENABLED,
        )
    )
    gateway = FakeMiniQMTGateway()
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id="mqrt_bug528_dependent_buy",
            account_group_id="ag_bug528",
            trade_date=TRADE_DATE,
            runtime_config_hash="runtime_hash_bug528",
        ),
        repository=repo,
        gateway=gateway,
        strategy_ledger_repository=qmt_repo,
        account_id="ag_bug528",
    )
    runtime.start()
    return runtime, repo, gateway, qmt_repo


def _submit_dependent_sell(
    runtime: MiniQMTExecutionRuntime,
    *,
    qmt_repo: InMemoryQmtStrategyLedgerRepository,
    quantity: int,
    price: Decimal,
):
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id=f"lot_bug528_{quantity}_{price}",
            strategy_id="strategy_bug528",
            symbol="000001.SZ",
            open_trade_id="open_trade_bug528",
            open_date=TRADE_DATE,
            quantity=quantity,
            available_quantity=quantity,
            remaining_quantity=quantity,
            avg_cost=Decimal("8"),
            cost_amount=Decimal("8") * Decimal(quantity),
            account_id="ag_bug528",
        )
    )
    algo = runtime.create_algo_instance(
        parent_intent_id="sell_parent_bug528",
        strategy_slot_id="slot_bug528",
        symbol="000001.SZ",
        side=OrderSide.SELL,
        target_quantity=quantity,
        algo_code="SNIPER_MINIQMT",
    )
    return runtime.submit_child_order(
        algo_instance_id=algo.algo_instance_id,
        quantity=quantity,
        price=float(price),
        metadata={
            "strategy_id": "strategy_bug528",
            "strategy_name": "slot_bug528",
            "order_remark": "sell_remark_bug528",
        },
    )


def _create_deferred_dependent_buy(
    runtime: MiniQMTExecutionRuntime,
    *,
    sell_child,
    required_cash: Decimal,
    price: float,
    quantity: int,
    extra_metadata: dict | None = None,
):
    metadata = {
        "dependent_buy": True,
        "dependent_buy_required_cash": str(required_cash),
        "dependent_buy_strategy_id": "strategy_bug528",
        "dependent_sell_child_order_ids": [sell_child.child_order_id],
        "dependent_sell_parent_intent_ids": [sell_child.parent_intent_id],
        "dependent_sell_symbols": [sell_child.symbol],
        "runtime_child_context": {
            "strategy_id": "strategy_bug528",
            "strategy_name": "slot_bug528",
            "order_remark": "buy_remark_bug528",
            "parent_intent_metadata": {
                "strategy_id": "strategy_bug528",
                "dependent_buy": True,
            },
        },
        **dict(extra_metadata or {}),
    }
    algo = runtime.create_vnpy_algo_instance(
        parent_intent_id="buy_parent_bug528",
        strategy_slot_id="slot_bug528",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=quantity,
        algo_code="SNIPER_MINIQMT",
        limit_price=price,
        metadata=metadata,
    )
    runtime.on_tick(
        symbol="000001.SZ",
        price=price,
        payload={
            "bid_price_1": price - 0.01,
            "bid_volume_1": quantity,
            "ask_price_1": price,
            "ask_volume_1": quantity,
        },
    )
    stored = runtime.repository.list_algo_instances(runtime.config.runtime_id, active_only=False)[-1]
    assert stored.algo_instance_id == algo.algo_instance_id
    assert stored.metadata["dependent_buy_status"] == "DEFERRED_WAITING_SELL_PROCEEDS"
    assert stored.metadata["dependent_buy_reason_code"] == "MINIQMT_DEPENDENT_BUY_DEFERRED_WAITING_SELL_PROCEEDS"
    return stored


def _create_inferred_dependent_buy(
    runtime: MiniQMTExecutionRuntime,
    *,
    price: float,
    quantity: int,
):
    algo = runtime.create_vnpy_algo_instance(
        parent_intent_id="buy_parent_bug528_inferred",
        strategy_slot_id="slot_bug528",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=quantity,
        algo_code="SNIPER_MINIQMT",
        limit_price=price,
        metadata={
            "runtime_child_context": {
                "strategy_id": "strategy_bug528",
                "strategy_name": "slot_bug528",
                "order_remark": "buy_inferred_bug528",
            },
        },
    )
    runtime.on_tick(
        symbol="000001.SZ",
        price=price,
        payload={
            "bid_price_1": price - 0.01,
            "bid_volume_1": quantity,
            "ask_price_1": price,
            "ask_volume_1": quantity,
        },
    )
    stored = runtime.repository.list_algo_instances(runtime.config.runtime_id, active_only=False)[-1]
    assert stored.algo_instance_id == algo.algo_instance_id
    assert stored.metadata["dependent_buy_status"] == "DEFERRED_WAITING_SELL_PROCEEDS"
    assert stored.metadata["dependent_buy_reason_code"] == "MINIQMT_DEPENDENT_BUY_DEFERRED_WAITING_SELL_PROCEEDS"
    assert stored.metadata["dependent_buy_inferred"] is True
    return stored
