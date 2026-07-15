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
from backend.services.miniqmt_execution_runtime import (
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTExecutionRuntimeClient,
)
from backend.services.qmt_strategy_ledger.models import VirtualAccount, VirtualAccountStatus
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.trading_core.errors import BrokerSubmitError
from backend.services.trading_core.models import OrderIntent, OrderSide, OrderType, PositionLot, RunStatus
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


