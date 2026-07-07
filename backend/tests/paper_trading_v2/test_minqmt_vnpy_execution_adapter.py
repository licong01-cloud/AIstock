from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from backend.services.paper_trading_v2.broker import (
    BrokerAccountSnapshot,
    CancelAck,
    OrderHandle,
    OrderHandleStatus,
)
from backend.services.paper_trading_v2.day_runner import PaperTradingDayRunner
from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.models import PaperPortfolio, PaperRun
from backend.services.simulation_runtime.models import ExecutionPathNotCanonicalError, MiniQMTUnsupportedExecutionAlgoError
from backend.services.miniqmt_execution_runtime import (
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTExecutionRuntimeClient,
)
from backend.services.qmt_strategy_ledger.models import VirtualAccount, VirtualAccountStatus
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.trading_core.errors import BrokerSubmitError
from backend.services.trading_core.models import Fill, OrderIntent, OrderSide, OrderStatus, OrderType, PositionLot, RunStatus
from backend.tests.paper_trading_v2.test_day_runner import make_paper_enabled_manifest
from backend.tests.paper_trading_v2.test_minqmtsim_backend import TRADE_DATE, _SnapshotOnlyRepository


@pytest.fixture(autouse=True)
def _force_event_loop_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME", "event_loop")


class VnpyRecordingMiniQMTBroker:
    def __init__(self, *, status_state: str = "pending", filled: bool = False, reject_msg: str | None = None) -> None:
        self.submitted: list[OrderIntent] = []
        self.cancelled: list[str] = []
        self._statuses: dict[str, OrderHandleStatus] = {}
        self._trades: dict[str, list[dict[str, Any]]] = {}
        self._status_state = status_state
        self._filled = filled
        self._reject_msg = reject_msg

    def submit_order_intent(self, intent: OrderIntent) -> OrderHandle:
        self.submitted.append(intent)
        handle = OrderHandle(
            handle_id=f"handle_{len(self.submitted)}",
            backend_id="minqmt_sim",
            submitted_at=datetime(2024, 1, 2, 9, 31, tzinfo=UTC),
            intent_id=intent.intent_id,
        )
        state = "filled" if self._filled else self._status_state
        filled_qty = intent.quantity if state == "filled" else 0
        self._statuses[handle.handle_id] = OrderHandleStatus(
            handle_id=handle.handle_id,
            state=state,
            filled_quantity=filled_qty,
            avg_fill_price=Decimal(str(intent.limit_price or "10.0")) if filled_qty else None,
            last_event_at=handle.submitted_at,
            rejection_reason=self._reject_msg if state == "rejected" else None,
            raw_status=57 if state == "rejected" else 56 if state == "filled" else 50,
            status_msg=self._reject_msg or "reported",
            raw={"order_status": 57 if state == "rejected" else 56 if state == "filled" else 50, "status_msg": self._reject_msg or "reported"},
        )
        if state == "filled":
            context = self.order_context(handle)
            self._trades[handle.handle_id] = [
                {
                    "traded_id": f"trade_{handle.intent_id}",
                    "stock_code": intent.symbol,
                    "stock_name": "Unit Test Stock",
                    "order_type": 23 if intent.side == OrderSide.BUY else 24,
                    "traded_time": "093105",
                    "traded_price": float(intent.limit_price or 10.0),
                    "traded_volume": intent.quantity,
                    "traded_amount": float(intent.limit_price or 10.0) * intent.quantity,
                    "order_id": context["miniqmt_order_id"],
                    "order_sysid": f"sys_{handle.intent_id}",
                    "commission": 5.0,
                    "strategy_name": context["strategy_name"],
                    "order_remark": context["order_remark"],
                }
            ]
        return handle

    def cancel(self, handle: OrderHandle) -> CancelAck:
        self.cancelled.append(handle.handle_id)
        self._statuses[handle.handle_id] = self._statuses[handle.handle_id].model_copy(update={"state": "cancelled"})
        return CancelAck(handle_id=handle.handle_id, accepted=True, reason="cancel accepted")

    def order_context(self, handle: OrderHandle) -> dict[str, str]:
        return {
            "handle_id": handle.handle_id,
            "intent_id": handle.intent_id,
            "miniqmt_order_id": f"native_{handle.intent_id}",
            "strategy_name": "slot_alpha",
            "order_remark": f"remark_{handle.intent_id}",
        }

    def query_status(self, handle: OrderHandle) -> OrderHandleStatus:
        return self._statuses[handle.handle_id]

    def query_trades(self, handle: OrderHandle) -> list[dict[str, Any]]:
        return list(self._trades.get(handle.handle_id, []))

    def query_status_from_native(
        self,
        *,
        handle_id: str,
        intent: OrderIntent,
        miniqmt_order_id: str,
        strategy_name: str,
        order_remark: str,
    ) -> OrderHandleStatus:
        return self._statuses[handle_id]

    def query_trades_from_native(
        self,
        *,
        handle_id: str,
        intent: OrderIntent,
        miniqmt_order_id: str,
        strategy_name: str,
        order_remark: str,
    ) -> list[dict[str, Any]]:
        return list(self._trades.get(handle_id, []))

    def query_quote(self, symbol: str) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "bid_price_1": 9.95,
            "bid_volume_1": 500,
            "ask_price_1": 10.0,
            "ask_volume_1": 400,
            "time": "20240102093105",
        }

    def query_account(self) -> BrokerAccountSnapshot:
        return BrokerAccountSnapshot(
            backend_id="minqmt_sim",
            cash=Decimal("100000"),
            nav=Decimal("100000"),
            margin_used=None,
            as_of=datetime(2024, 1, 2, 15, 0, tzinfo=UTC),
        )

    def query_position_marks(self):
        return {
            "000001.SZ": PositionLot(
                portfolio_id="paper_mq_1",
                symbol="000001.SZ",
                quantity=200,
                available_quantity=0,
                avg_cost=10.0,
                trade_date=TRADE_DATE,
            )
        }, {"000001.SZ": 10.0}

    def shutdown(self) -> None:
        return None


class EventLoopCallbackMiniQMTClient:
    def __init__(self) -> None:
        self.place_order_calls: list[dict[str, Any]] = []

    def query_quote(self, symbol: str) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "source": "MINIQMT_REALTIME.broker_quote",
            "price": 10.0,
            "ask_price_1": 10.0,
            "ask_volume_1": 1000,
            "bid_price_1": 9.99,
            "bid_volume_1": 1000,
            "time": "20240102093105",
        }

    def get_orders(self, cancelable_only: bool = False) -> list[dict[str, Any]]:  # noqa: ARG002
        return []

    def get_trades(self) -> list[dict[str, Any]]:
        return []

    def get_positions(self) -> list[dict[str, Any]]:
        return []

    def place_order(self, **kwargs: Any):
        self.place_order_calls.append(dict(kwargs))
        return 880000000 + len(self.place_order_calls), "accepted"


def _portfolio_and_run(policy_json: dict[str, Any]):
    manifest = make_paper_enabled_manifest()
    policy = {
        "validated_execution_policy_id": "execpol_vnpy_unit",
        "policy_sha256": "sha_vnpy_unit",
        "policy_name": "unit vnpy policy",
        "algo_code": policy_json["algo_code"],
        "policy_json": policy_json,
        "source_backtest_id": "bt_unit",
        "source_backtest_status": "BACKTEST_VALIDATED",
        "validation_status": "BACKTEST_VALIDATED",
    }
    portfolio = PaperPortfolio(
        portfolio_id="paper_mq_1",
        portfolio_name="mini qmt vnpy style",
        package_id=manifest.package_id,
        manifest_sha256=manifest.manifest_sha256,
        frozen_manifest=manifest,
        initial_cash=100_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        broker_backend="minqmt_sim",
        execution_policy=policy,
    )
    run = PaperRun(
        portfolio_id=portfolio.portfolio_id,
        trade_date=TRADE_DATE,
        status=RunStatus.RUNNING,
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        runtime_config={"paper_v2_session": {"session_id": "psess_vnpy_unit"}},
    )
    return manifest, portfolio, run, {**policy, "policy_json": policy_json}


def _intent(
    *,
    side: OrderSide = OrderSide.BUY,
    order_type: OrderType = OrderType.LIMIT,
    limit_price: float | None = 10.0,
    quantity: int = 200,
):
    return OrderIntent(
        package_id="pkg_mq_1",
        portfolio_id="paper_mq_1",
        symbol="000001.SZ",
        side=side,
        quantity=quantity,
        order_type=order_type,
        limit_price=limit_price,
        target_trade_date=date(2024, 1, 2),
    )


def _assert_legacy_vnpy_loud_rejects(exc: BrokerSubmitError, *, broker: VnpyRecordingMiniQMTBroker) -> None:
    assert exc.error_code == "BROKER_SUBMIT_ERROR"
    assert exc.context["reason_code"] == "MINIQMT_EVENT_LOOP_REQUIRES_REAL_CALLBACKS"
    assert exc.context["stage"] == "MINIQMT_COMPILER_LIFECYCLE_REJECTED"
    assert exc.context["operation"] == "execute_paper_vnpy_intent"
    assert broker.submitted == []


def _seed_native_order(
    *,
    repo: _SnapshotOnlyRepository,
    run: PaperRun,
    intent: OrderIntent,
    broker: VnpyRecordingMiniQMTBroker,
) -> tuple[Any, str, str]:
    order = PaperTradingDayRunner(repository=repo).oms.create_order(intent)
    handle_id = f"handle_{intent.intent_id}"
    native_id = f"native_{intent.intent_id}"
    order = order.model_copy(
        update={
            "metadata": {
                **dict(order.metadata or {}),
                "broker_backend": "minqmt_sim",
                "authority_source": "MINIQMT_NATIVE_RECONCILE_TEST_SEED",
                "broker_handle_id": handle_id,
                "miniqmt_order_id": native_id,
                "strategy_name": "slot_alpha",
                "order_remark": f"remark_{intent.intent_id}",
            }
        }
    )
    repo.save_order(run.run_id, order)
    broker._statuses[handle_id] = OrderHandleStatus(
        handle_id=handle_id,
        state="pending",
        filled_quantity=0,
        avg_fill_price=None,
        last_event_at=datetime(2024, 1, 2, 9, 31, tzinfo=UTC),
        raw_status=50,
        status_msg="reported",
        raw={"order_status": 50, "status_msg": "reported"},
    )
    return order, handle_id, native_id


def test_event_loop_route_a_submits_parent_intent_through_callback_gateway() -> None:
    runtime_repo = InMemoryMiniQMTExecutionRuntimeRepository()
    ledger_repo = InMemoryQmtStrategyLedgerRepository()
    ledger_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strategy_event_loop",
            strategy_name="strategy_event_loop",
            display_name="Event loop strategy",
            account_id="acct_event_loop",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_client = EventLoopCallbackMiniQMTClient()
    client = MiniQMTExecutionRuntimeClient(
        repository=runtime_repo,
        strategy_ledger_repository=ledger_repo,
        runtime_kind="event_loop",
    )

    result = client.submit_event_loop_vnpy_parent_intents(
        parent_intents=[
            _intent(quantity=100).model_copy(
                update={
                    "intent_id": "intent_event_loop_buy",
                    "metadata": {
                        "strategy_id": "strategy_event_loop",
                        "strategy_name": "strategy_event_loop",
                    },
                }
            )
        ],
        policy_context={
            "policy_json": {"algo_code": "SNIPER_MINIQMT", "algo_config": {}},
            "validated_execution_policy_id": "policy_event_loop",
            "policy_sha256": "sha_event_loop",
        },
        account_group_id="acct_event_loop",
        trade_date=TRADE_DATE,
        runtime_config_hash="runtime_hash_event_loop_route_a",
        runtime_id="mqrt_event_loop_route_a",
        strategy_slot_id="strategy_event_loop",
        qmt_client=qmt_client,
        strategy_name="strategy_event_loop",
        order_remark_prefix="evtloop",
        account_id="acct_event_loop",
        source="paper_v2_event_loop_route_a_unit",
    )

    assert result.success is True
    assert qmt_client.place_order_calls
    runtime = runtime_repo.get_runtime("mqrt_event_loop_route_a")
    assert runtime is not None
    assert runtime.metadata["runtime_kind"] == "event_loop"
    assert runtime.metadata["gateway_class"] == "QmtClientMiniQMTEventLoopGateway"
    algo = runtime_repo.list_algo_instances("mqrt_event_loop_route_a", active_only=False)[0]
    assert algo.metadata["event_loop_submit"] is True
    assert algo.metadata["quote_source"] == "MINIQMT_REALTIME.broker_quote"
    child = runtime_repo.list_child_orders("mqrt_event_loop_route_a", active_only=False)[0]
    assert child.status.value == "SUBMITTED"


def test_minqmt_rejects_v25_policy_before_broker_submit() -> None:
    manifest, portfolio, run, _policy_context = _portfolio_and_run({"algo_code": "SNIPER_MINIQMT", "algo_config": {}})
    repo = _SnapshotOnlyRepository(portfolio)
    broker = VnpyRecordingMiniQMTBroker()

    with pytest.raises(MiniQMTUnsupportedExecutionAlgoError) as exc_info:
        PaperTradingDayRunner(repository=repo)._run_minqmt_sim_orders(
            portfolio=portfolio,
            run=run,
            manifest=manifest,
            trade_date=TRADE_DATE,
            intents=[_intent()],
            broker=broker,  # type: ignore[arg-type]
            execution_policy_context={"algo_code": "V25_1_SMALL_CAP", "policy_json": {"algo_code": "V25_1_SMALL_CAP"}},
        )

    assert exc_info.value.error_code == "MINIQMT_UNSUPPORTED_EXECUTION_ALGO"
    assert exc_info.value.context["inferred_algo_code"] == "V25_1_SMALL_CAP"
    assert broker.submitted == []
    assert repo.orders == []


def test_minqmt_rejects_missing_vnpy_policy_snapshot_before_broker_submit() -> None:
    manifest, portfolio, run, _policy_context = _portfolio_and_run({"algo_code": "SNIPER_MINIQMT", "algo_config": {}})
    repo = _SnapshotOnlyRepository(portfolio)
    broker = VnpyRecordingMiniQMTBroker()

    with pytest.raises(ExecutionPathNotCanonicalError) as exc_info:
        PaperTradingDayRunner(repository=repo)._run_minqmt_sim_orders(
            portfolio=portfolio,
            run=run,
            manifest=manifest,
            trade_date=TRADE_DATE,
            intents=[_intent()],
            broker=broker,  # type: ignore[arg-type]
            execution_policy_context=None,
        )

    assert exc_info.value.error_code == "EXECUTION_PATH_NOT_CANONICAL"
    assert exc_info.value.context["payload_has_policy_json"] is False
    assert broker.submitted == []
    assert repo.orders == []


@pytest.mark.parametrize(
    ("algo_code", "algo_config"),
    [
        ("SNIPER_MINIQMT", {}),
        ("BEST_LIMIT_MINIQMT", {"min_volume": 100, "max_volume": 100}),
        ("TWAP_LITE_MINIQMT", {"time": 2, "interval": 1, "timer_iterations": 1}),
    ],
)
def test_minqmt_vnpy_legacy_compiler_style_path_loud_rejects_before_broker_submit(
    algo_code: str,
    algo_config: dict[str, Any],
) -> None:
    manifest, portfolio, run, policy_context = _portfolio_and_run(
        {"algo_code": algo_code, "algo_config": algo_config}
    )
    repo = _SnapshotOnlyRepository(portfolio)
    broker = VnpyRecordingMiniQMTBroker()

    with pytest.raises(BrokerSubmitError) as exc_info:
        PaperTradingDayRunner(repository=repo)._run_minqmt_sim_orders(
            portfolio=portfolio,
            run=run,
            manifest=manifest,
            trade_date=TRADE_DATE,
            intents=[_intent(order_type=OrderType.LIMIT, limit_price=10.0)],
            broker=broker,  # type: ignore[arg-type]
            execution_policy_context=policy_context,
        )

    _assert_legacy_vnpy_loud_rejects(exc_info.value, broker=broker)
    assert repo.orders == []
    assert not any(event["event_type"] == "MINIQMT_VNPY_STYLE_EXECUTION_COMPLETED" for event in repo.events)

def test_minqmt_vnpy_best_limit_does_not_fallback_to_compiler_route() -> None:
    manifest, portfolio, run, policy_context = _portfolio_and_run(
        {"algo_code": "BEST_LIMIT_MINIQMT", "algo_config": {"min_volume": 100, "max_volume": 100}}
    )
    repo = _SnapshotOnlyRepository(portfolio)
    broker = VnpyRecordingMiniQMTBroker()

    with pytest.raises(BrokerSubmitError) as exc_info:
        PaperTradingDayRunner(repository=repo)._run_minqmt_sim_orders(
            portfolio=portfolio,
            run=run,
            manifest=manifest,
            trade_date=TRADE_DATE,
            intents=[_intent(order_type=OrderType.LIMIT, limit_price=10.5)],
            broker=broker,  # type: ignore[arg-type]
            execution_policy_context=policy_context,
        )

    _assert_legacy_vnpy_loud_rejects(exc_info.value, broker=broker)
    assert repo.orders == []

def test_minqmt_vnpy_twap_lite_does_not_persist_compiler_child_trade() -> None:
    manifest, portfolio, run, policy_context = _portfolio_and_run(
        {"algo_code": "TWAP_LITE_MINIQMT", "algo_config": {"time": 2, "interval": 1, "timer_iterations": 1}}
    )
    repo = _SnapshotOnlyRepository(portfolio)
    broker = VnpyRecordingMiniQMTBroker(filled=True)

    with pytest.raises(BrokerSubmitError) as exc_info:
        PaperTradingDayRunner(repository=repo)._run_minqmt_sim_orders(
            portfolio=portfolio,
            run=run,
            manifest=manifest,
            trade_date=TRADE_DATE,
            intents=[_intent(order_type=OrderType.LIMIT, limit_price=10.0)],
            broker=broker,  # type: ignore[arg-type]
            execution_policy_context=policy_context,
        )

    _assert_legacy_vnpy_loud_rejects(exc_info.value, broker=broker)
    assert repo.fills == []
    assert repo.snapshots == []

def test_minqmt_native_reconcile_applies_only_new_trade_delta_and_caps_overfill() -> None:
    _manifest, portfolio, run, _policy_context = _portfolio_and_run(
        {"algo_code": "BEST_LIMIT_MINIQMT", "algo_config": {"min_volume": 44_000, "max_volume": 44_000}}
    )
    repo = _SnapshotOnlyRepository(portfolio)
    broker = VnpyRecordingMiniQMTBroker()
    intent = _intent(order_type=OrderType.LIMIT, limit_price=82.33, quantity=44_000).model_copy(
        update={"intent_id": "intent_native_delta_cap"}
    )
    order, handle_id, native_id = _seed_native_order(repo=repo, run=run, intent=intent, broker=broker)
    existing_fill = Fill(
        fill_id="fill_minqmt_agg_existing",
        order_id=order.order_id,
        symbol=order.symbol,
        side=order.side,
        quantity=14_600,
        price=82.33,
        trade_time=datetime(2024, 1, 2, 13, 30, tzinfo=UTC),
        bar_time=datetime(2024, 1, 2, 13, 30, tzinfo=UTC),
        reason="miniqmt_trade_reconciliation_aggregate",
        metadata={
            "authority_source": "MINIQMT_TRADE_AGGREGATE",
            "miniqmt_trade_raw_rows": [
                {
                    "traded_id": "trade_seen_1",
                    "stock_code": order.symbol,
                    "order_type": 23,
                    "traded_time": "133000",
                    "traded_price": 82.33,
                    "traded_volume": 14_600,
                    "order_id": native_id,
                    "order_sysid": "sys_seen",
                }
            ],
        },
    )
    repo.save_fill(run.run_id, existing_fill)
    repo.save_order(
        run.run_id,
        order.model_copy(
            update={
                "status": OrderStatus.PARTIALLY_FILLED,
                "filled_quantity": 14_600,
                "avg_fill_price": 82.33,
            }
        ),
    )
    broker._statuses[handle_id] = OrderHandleStatus(
        handle_id=handle_id,
        state="filled",
        filled_quantity=44_000,
        avg_fill_price=Decimal("82.40"),
        last_event_at=datetime(2024, 1, 2, 13, 31, tzinfo=UTC),
        rejection_reason=None,
    )
    broker._trades[handle_id] = [
        {
            "traded_id": "trade_seen_1",
            "stock_code": order.symbol,
            "stock_name": "Unit Test Stock",
            "order_type": 23,
            "traded_time": "133000",
            "traded_price": 82.33,
            "traded_volume": 14_600,
            "traded_amount": 1_201_018.0,
            "order_id": native_id,
            "order_sysid": "sys_seen",
            "commission": 5.0,
            "strategy_name": "slot_alpha",
            "order_remark": order.metadata["order_remark"],
        },
        {
            "traded_id": "trade_new_1",
            "stock_code": order.symbol,
            "stock_name": "Unit Test Stock",
            "order_type": 23,
            "traded_time": "133100",
            "traded_price": 82.41,
            "traded_volume": 44_000,
            "traded_amount": 3_626_040.0,
            "order_id": native_id,
            "order_sysid": "sys_new",
            "commission": 5.0,
            "strategy_name": "slot_alpha",
            "order_remark": order.metadata["order_remark"],
        },
    ]

    reconciled = PaperTradingDayRunner(repository=repo).reconcile_minqmt_native_run(
        portfolio=portfolio,
        run=run,
        trade_date=TRADE_DATE,
        broker=broker,  # type: ignore[arg-type]
    )
    repeated = PaperTradingDayRunner(repository=repo).reconcile_minqmt_native_run(
        portfolio=portfolio,
        run=reconciled.run,
        trade_date=TRADE_DATE,
        broker=broker,  # type: ignore[arg-type]
    )

    assert len(repo.fills) == 2
    assert repo.fills[-1]["fill"].quantity == 29_400
    assert repo.fills[-1]["fill"].metadata["broker_reconcile_delta_capped"] is True
    assert repo.orders[-1].filled_quantity == 44_000
    assert repo.orders[-1].status == OrderStatus.FILLED
    assert repeated.fills == []
    assert len(repo.fills) == 2
    capped_event = next(event for event in repo.events if event["event_type"] == "MINIQMT_NATIVE_RECONCILE_OVERFILL_CAPPED")
    assert capped_event["context"]["broker_reported_fill_quantity"] == 44_000
    assert capped_event["context"]["applied_fill_quantity"] == 29_400

def test_minqmt_native_reconcile_resets_status_only_fill_when_no_local_fill_rows() -> None:
    _manifest, portfolio, run, _policy_context = _portfolio_and_run(
        {"algo_code": "BEST_LIMIT_MINIQMT", "algo_config": {"min_volume": 44_000, "max_volume": 44_000}}
    )
    repo = _SnapshotOnlyRepository(portfolio)
    broker = VnpyRecordingMiniQMTBroker()
    intent = _intent(order_type=OrderType.LIMIT, limit_price=82.33, quantity=44_000).model_copy(
        update={"intent_id": "intent_native_status_only"}
    )
    order, handle_id, native_id = _seed_native_order(repo=repo, run=run, intent=intent, broker=broker)
    repo.save_order(
        run.run_id,
        order.model_copy(
            update={
                "status": OrderStatus.PARTIALLY_FILLED,
                "filled_quantity": 14_600,
                "avg_fill_price": 82.33,
            }
        ),
    )
    broker._statuses[handle_id] = OrderHandleStatus(
        handle_id=handle_id,
        state="filled",
        filled_quantity=44_000,
        avg_fill_price=Decimal("82.40"),
        last_event_at=datetime(2024, 1, 2, 13, 31, tzinfo=UTC),
        rejection_reason=None,
    )
    broker._trades[handle_id] = [
        {
            "traded_id": "trade_full_after_status_only_partial",
            "stock_code": order.symbol,
            "stock_name": "Unit Test Stock",
            "order_type": 23,
            "traded_time": "133100",
            "traded_price": 82.40,
            "traded_volume": 44_000,
            "traded_amount": 3_625_600.0,
            "order_id": native_id,
            "order_sysid": "sys_full_after_status_only_partial",
            "commission": 5.0,
            "strategy_name": "slot_alpha",
            "order_remark": order.metadata["order_remark"],
        },
    ]

    reconciled = PaperTradingDayRunner(repository=repo).reconcile_minqmt_native_run(
        portfolio=portfolio,
        run=run,
        trade_date=TRADE_DATE,
        broker=broker,  # type: ignore[arg-type]
    )
    repeated = PaperTradingDayRunner(repository=repo).reconcile_minqmt_native_run(
        portfolio=portfolio,
        run=reconciled.run,
        trade_date=TRADE_DATE,
        broker=broker,  # type: ignore[arg-type]
    )

    assert len(repo.fills) == 1
    assert repo.fills[0]["fill"].quantity == 44_000
    assert repo.orders[-1].filled_quantity == 44_000
    assert repo.orders[-1].status == OrderStatus.FILLED
    assert repeated.fills == []
    assert not any(event["event_type"] == "MINIQMT_NATIVE_RECONCILE_OVERFILL_CAPPED" for event in repo.events)

def test_minqmt_vnpy_rejected_child_status_does_not_mask_retired_compiler_route() -> None:
    manifest, portfolio, run, policy_context = _portfolio_and_run({"algo_code": "SNIPER_MINIQMT", "algo_config": {}})
    repo = _SnapshotOnlyRepository(portfolio)
    broker = VnpyRecordingMiniQMTBroker(status_state="rejected", reject_msg="[COUNTER][260200] insufficient buying power")

    with pytest.raises(BrokerSubmitError) as exc_info:
        PaperTradingDayRunner(repository=repo)._run_minqmt_sim_orders(
            portfolio=portfolio,
            run=run,
            manifest=manifest,
            trade_date=TRADE_DATE,
            intents=[_intent(order_type=OrderType.LIMIT, limit_price=10.0)],
            broker=broker,  # type: ignore[arg-type]
            execution_policy_context=policy_context,
        )

    _assert_legacy_vnpy_loud_rejects(exc_info.value, broker=broker)
    assert repo.orders == []
    assert repo.fills == []
