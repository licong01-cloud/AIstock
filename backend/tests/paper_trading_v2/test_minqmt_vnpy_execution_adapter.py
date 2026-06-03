from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from backend.services.paper_trading_v2.broker import (
    BrokerAccountSnapshot,
    CancelAck,
    OrderHandle,
    OrderHandleStatus,
)
from backend.services.paper_trading_v2.day_runner import PaperTradingDayRunner
from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.models import PaperPortfolio, PaperRun
from backend.services.trading_core.models import Fill, OrderIntent, OrderSide, OrderStatus, OrderType, PositionLot, RunStatus
from backend.tests.paper_trading_v2.test_day_runner import make_paper_enabled_manifest
from backend.tests.paper_trading_v2.test_minqmtsim_backend import TRADE_DATE, _SnapshotOnlyRepository


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


def test_minqmt_vnpy_sniper_policy_routes_child_limit_order_and_diagnostics() -> None:
    manifest, portfolio, run, policy_context = _portfolio_and_run({"algo_code": "SNIPER_MINIQMT", "algo_config": {}})
    repo = _SnapshotOnlyRepository(portfolio)
    broker = VnpyRecordingMiniQMTBroker()

    result = PaperTradingDayRunner(repository=repo)._run_minqmt_sim_orders(
        portfolio=portfolio,
        run=run,
        manifest=manifest,
        trade_date=TRADE_DATE,
        intents=[_intent(order_type=OrderType.LIMIT, limit_price=10.0)],
        broker=broker,  # type: ignore[arg-type]
        execution_policy_context=policy_context,
    )

    assert result.run.status == RunStatus.SUCCEEDED
    assert len(broker.submitted) == 1
    assert broker.submitted[0].order_type == OrderType.LIMIT
    assert broker.submitted[0].limit_price == 10.0
    assert broker.submitted[0].quantity == 200
    assert repo.orders[0].metadata["execution_algo_code"] == "SNIPER_MINIQMT"
    assert repo.orders[0].metadata["execution_policy_id"] == "execpol_vnpy_unit"
    assert repo.orders[0].metadata["broker_raw_status"] == 50
    assert repo.execution_states[0].algo_code == "SNIPER_MINIQMT"
    assert repo.execution_states[0].algo_state["diagnostic"]["source_attribution"]["upstream_source_file"].endswith("sniper_algo.py")
    assert any(event["event_type"] == "MINIQMT_VNPY_STYLE_EXECUTION_COMPLETED" for event in repo.events)


def test_minqmt_vnpy_best_limit_changes_child_price_from_policy_selection() -> None:
    manifest, portfolio, run, policy_context = _portfolio_and_run(
        {"algo_code": "BEST_LIMIT_MINIQMT", "algo_config": {"min_volume": 100, "max_volume": 100}}
    )
    repo = _SnapshotOnlyRepository(portfolio)
    broker = VnpyRecordingMiniQMTBroker()

    PaperTradingDayRunner(repository=repo)._run_minqmt_sim_orders(
        portfolio=portfolio,
        run=run,
        manifest=manifest,
        trade_date=TRADE_DATE,
        intents=[_intent(order_type=OrderType.LIMIT, limit_price=10.5)],
        broker=broker,  # type: ignore[arg-type]
        execution_policy_context=policy_context,
    )

    assert broker.submitted[0].limit_price == 9.95
    assert repo.orders[0].metadata["execution_algo_code"] == "BEST_LIMIT_MINIQMT"


def test_minqmt_vnpy_twap_lite_can_persist_filled_child_trade() -> None:
    manifest, portfolio, run, policy_context = _portfolio_and_run(
        {"algo_code": "TWAP_LITE_MINIQMT", "algo_config": {"time": 2, "interval": 1, "timer_iterations": 1}}
    )
    repo = _SnapshotOnlyRepository(portfolio)
    broker = VnpyRecordingMiniQMTBroker(filled=True)

    result = PaperTradingDayRunner(repository=repo)._run_minqmt_sim_orders(
        portfolio=portfolio,
        run=run,
        manifest=manifest,
        trade_date=TRADE_DATE,
        intents=[_intent(order_type=OrderType.LIMIT, limit_price=10.0)],
        broker=broker,  # type: ignore[arg-type]
        execution_policy_context=policy_context,
    )

    assert len(result.fills) == 1
    assert result.orders[0].status.value == "FILLED"
    assert repo.fills[0]["fill"].metadata["broker_reported_commission"] == 5.0
    assert repo.orders[0].metadata["execution_algo_code"] == "TWAP_LITE_MINIQMT"
    quality_event = [event for event in repo.events if event["event_type"] == "MINIQMT_EXECUTION_QUALITY_REPORTED"][0]
    quality = quality_event["context"]
    assert quality["summary"]["broker_reported_fee_total"] == 5.0
    assert quality["summary"]["cost_precision_counts"] == {"broker_aggregate": 1}
    assert quality["fills"][0]["cost_reconciliation_delta"] == 0.0
    assert repo.snapshots[0]["metadata"]["execution_quality_report"]["schema_version"].endswith("_v1")


def test_minqmt_native_reconcile_applies_only_new_trade_delta_and_caps_overfill() -> None:
    manifest, portfolio, run, _policy_context = _portfolio_and_run({"algo_code": "SNIPER_MINIQMT", "algo_config": {}})
    repo = _SnapshotOnlyRepository(portfolio)
    broker = VnpyRecordingMiniQMTBroker()
    result = PaperTradingDayRunner(repository=repo)._run_minqmt_sim_orders(
        portfolio=portfolio,
        run=run,
        manifest=manifest,
        trade_date=TRADE_DATE,
        intents=[_intent(order_type=OrderType.LIMIT, limit_price=82.33, quantity=44_000)],
        broker=broker,  # type: ignore[arg-type]
        execution_policy_context={"algo_code": "V25_1_SMALL_CAP", "policy_json": {"algo_code": "V25_1_SMALL_CAP"}},
    )
    order = repo.orders[0]
    handle_id = order.metadata["broker_handle_id"]
    native_id = order.metadata["miniqmt_order_id"]
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
    repo.save_fill(result.run.run_id, existing_fill)
    repo.save_order(
        result.run.run_id,
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
        run=result.run,
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
    manifest, portfolio, run, _policy_context = _portfolio_and_run({"algo_code": "SNIPER_MINIQMT", "algo_config": {}})
    repo = _SnapshotOnlyRepository(portfolio)
    broker = VnpyRecordingMiniQMTBroker()
    result = PaperTradingDayRunner(repository=repo)._run_minqmt_sim_orders(
        portfolio=portfolio,
        run=run,
        manifest=manifest,
        trade_date=TRADE_DATE,
        intents=[_intent(order_type=OrderType.LIMIT, limit_price=82.33, quantity=44_000)],
        broker=broker,  # type: ignore[arg-type]
        execution_policy_context={"algo_code": "V25_1_SMALL_CAP", "policy_json": {"algo_code": "V25_1_SMALL_CAP"}},
    )
    order = repo.orders[0]
    handle_id = order.metadata["broker_handle_id"]
    native_id = order.metadata["miniqmt_order_id"]
    repo.save_order(
        result.run.run_id,
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
        run=result.run,
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


def test_minqmt_vnpy_rejected_child_preserves_raw_status_and_status_msg() -> None:
    manifest, portfolio, run, policy_context = _portfolio_and_run({"algo_code": "SNIPER_MINIQMT", "algo_config": {}})
    repo = _SnapshotOnlyRepository(portfolio)
    broker = VnpyRecordingMiniQMTBroker(status_state="rejected", reject_msg="[COUNTER][260200] insufficient buying power")

    result = PaperTradingDayRunner(repository=repo)._run_minqmt_sim_orders(
        portfolio=portfolio,
        run=run,
        manifest=manifest,
        trade_date=TRADE_DATE,
        intents=[_intent(order_type=OrderType.LIMIT, limit_price=10.0)],
        broker=broker,  # type: ignore[arg-type]
        execution_policy_context=policy_context,
    )

    assert result.orders[0].status.value == "REJECTED"
    assert repo.orders[0].metadata["broker_raw_status"] == 57
    assert repo.orders[0].metadata["broker_status_msg"].startswith("[COUNTER][260200]")
    completed = [event for event in repo.events if event["event_type"] == "MINIQMT_VNPY_STYLE_EXECUTION_COMPLETED"][0]
    assert completed["context"]["diagnostic"]["child_orders"][0]["status"]["raw_status"] == 57
