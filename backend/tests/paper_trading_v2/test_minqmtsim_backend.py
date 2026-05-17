from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from backend.infra.qmt_client import QMTStatus
from backend.services.paper_trading_v2.day_runner import PaperTradingDayRunner
from backend.services.paper_trading_v2.models import PaperPortfolio, PaperRun, PortfolioStatus
from backend.services.paper_trading_v2.broker import (
    BrokerAccountSnapshot,
    BrokerBindCapacity,
    CancelAck,
    MarketDataChannel,
    MiniQMTSimBackend,
    OrderHandle,
    OrderHandleStatus,
    SubscriptionHandle,
)
from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.trading_core.errors import (
    BrokerConnectivityError,
    BrokerMarketSourceMismatchError,
    BrokerRejectedError,
    BrokerSubmitError,
)
from backend.services.trading_core.models import OrderIntent, OrderSide, OrderType, PositionLot, RunStatus
from backend.tests.paper_trading_v2.test_day_runner import make_paper_enabled_manifest


TRADE_DATE = date(2024, 1, 2)


class FakeQMTClient:
    def __init__(
        self,
        *,
        connected: bool = True,
        connect_ok: bool = True,
        next_order_id: int = 10001,
        mode: str = "SIM",
    ) -> None:
        self.connected = connected
        self.connect_ok = connect_ok
        self.next_order_id = next_order_id
        self.mode = mode
        self.place_calls: list[dict] = []
        self.cancel_calls: list[str] = []
        self.orders: list[dict] = []
        self.account = {
            "available_cash": 123456.78,
            "total_asset": 234567.89,
            "frozen_cash": 100.0,
        }
        self.positions = [
            {
                "stock_code": "000001.SZ",
                "quantity": 300,
                "can_sell": 200,
                "cost_price": 10.5,
                "current_price": 10.8,
                "market_value": 3240.0,
            }
        ]

    def status(self) -> QMTStatus:
        return QMTStatus(
            enabled=True,
            connected=self.connected,
            mode=self.mode,
            account_id="acct_unit",
            provider="fake",
        )

    def connect(self):
        self.connected = bool(self.connect_ok)
        return self.connected, "connected" if self.connected else "connect failed"

    def place_order(self, **kwargs):
        self.place_calls.append(kwargs)
        if self.next_order_id <= 0:
            return -1, "fake rejected"
        order_id = self.next_order_id
        self.next_order_id += 1
        self.orders.append(
            {
                "order_id": str(order_id),
                "stock_code": kwargs["stock_code"],
                "order_type": kwargs["order_type"],
                "order_volume": kwargs["order_volume"],
                "price_type": kwargs["price_type"],
                "price": kwargs["price"],
                "traded_volume": 0,
                "traded_price": 0.0,
                "order_status": 50,
                "status_msg": "reported",
                "strategy_name": kwargs["strategy_name"],
                "order_remark": kwargs["order_remark"],
            }
        )
        return order_id, "submitted"

    def get_orders(self, cancelable_only: bool = False):
        return list(self.orders)

    def get_account_info(self):
        return dict(self.account)

    def get_positions(self):
        return list(self.positions)

    def cancel_order(self, order_id: str):
        self.cancel_calls.append(str(order_id))
        return True, "cancel accepted"


def _backend(*, client: FakeQMTClient | None = None, auto_connect: bool = True) -> MiniQMTSimBackend:
    return MiniQMTSimBackend(
        portfolio_id="paper_mq_1",
        package_id="pkg_mq_1",
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        qmt_client=client or FakeQMTClient(),
        strategy_slot_id="slot_alpha",
        auto_connect=auto_connect,
    )


def _intent(
    *,
    portfolio_id: str = "paper_mq_1",
    package_id: str = "pkg_mq_1",
    side: OrderSide = OrderSide.BUY,
    order_type: OrderType = OrderType.MARKET,
    limit_price: float | None = None,
    metadata: dict | None = None,
) -> OrderIntent:
    return OrderIntent(
        package_id=package_id,
        portfolio_id=portfolio_id,
        symbol="000001.SZ",
        side=side,
        quantity=200,
        order_type=order_type,
        limit_price=limit_price,
        target_trade_date=TRADE_DATE,
        metadata=metadata or {},
    )


def test_minqmtsim_init_requires_miniqmt_realtime_source() -> None:
    with pytest.raises(BrokerMarketSourceMismatchError) as exc_info:
        MiniQMTSimBackend(
            portfolio_id="paper_bad",
            package_id="pkg_bad",
            data_source=MinuteDataSource.TDX_REALTIME,
            qmt_client=FakeQMTClient(),
        )
    assert exc_info.value.context["broker_id"] == "minqmt_sim"
    assert exc_info.value.context["given_source"] == "TDX_REALTIME"


def test_minqmtsim_init_connect_failure_is_connectivity_error() -> None:
    client = FakeQMTClient(connected=False, connect_ok=False)
    with pytest.raises(BrokerConnectivityError) as exc_info:
        _backend(client=client)
    assert exc_info.value.context["message"] == "connect failed"


def test_minqmtsim_rejects_non_exclusive_account_mode() -> None:
    with pytest.raises(BrokerSubmitError) as exc_info:
        MiniQMTSimBackend(
            portfolio_id="paper_mq_1",
            package_id="pkg_mq_1",
            data_source=MinuteDataSource.MINIQMT_REALTIME,
            qmt_client=FakeQMTClient(),
            account_mode="shared_account_attribution",
        )
    assert exc_info.value.context["supported"] == ["exclusive_account"]


def test_minqmtsim_rejects_non_sim_qmt_mode() -> None:
    with pytest.raises(BrokerConnectivityError) as exc_info:
        _backend(client=FakeQMTClient(mode="LIVE"))
    assert exc_info.value.context["mode"] == "LIVE"


def test_submit_buy_market_maps_to_documented_xtquant_params() -> None:
    client = FakeQMTClient()
    backend = _backend(client=client)
    intent = _intent()

    handle = backend.submit_order_intent(intent)

    assert handle.backend_id == "minqmt_sim"
    assert handle.intent_id == intent.intent_id
    call = client.place_calls[-1]
    assert call["stock_code"] == "000001.SZ"
    assert call["order_type"] == 23  # xtconstant.STOCK_BUY
    assert call["order_volume"] == 200
    assert call["price_type"] == 5  # xtconstant.LATEST_PRICE
    assert call["price"] == 0.0
    assert call["strategy_name"] == "slot_alpha"
    assert intent.intent_id in call["order_remark"]
    status = backend.query_status(handle)
    assert status.state == "pending"


def test_submit_sell_limit_maps_to_documented_xtquant_params() -> None:
    client = FakeQMTClient()
    backend = _backend(client=client)
    intent = _intent(side=OrderSide.SELL, order_type=OrderType.LIMIT, limit_price=10.23)

    backend.submit_order_intent(intent)

    call = client.place_calls[-1]
    assert call["order_type"] == 24  # xtconstant.STOCK_SELL
    assert call["price_type"] == 11  # xtconstant.FIX_PRICE
    assert call["price"] == 10.23


def test_submit_rejects_cross_portfolio_and_cross_package() -> None:
    backend = _backend()
    with pytest.raises(BrokerSubmitError) as portfolio_exc:
        backend.submit_order_intent(_intent(portfolio_id="paper_other"))
    assert portfolio_exc.value.context["backend_portfolio_id"] == "paper_mq_1"

    with pytest.raises(BrokerSubmitError) as package_exc:
        backend.submit_order_intent(_intent(package_id="pkg_other"))
    assert package_exc.value.context["backend_package_id"] == "pkg_mq_1"


def test_submit_rejects_duplicate_intent_without_second_order() -> None:
    client = FakeQMTClient()
    backend = _backend(client=client)
    intent = _intent()
    backend.submit_order_intent(intent)
    with pytest.raises(BrokerSubmitError) as exc_info:
        backend.submit_order_intent(intent)
    assert exc_info.value.context["intent_id"] == intent.intent_id
    assert len(client.place_calls) == 1


def test_submit_rejects_missed_scheduled_deadline_before_miniqmt_call() -> None:
    client = FakeQMTClient()
    backend = _backend(client=client)
    intent = _intent(
        metadata={
            "scheduled_submit_at": (datetime.now(UTC) - timedelta(seconds=10)).isoformat(),
            "max_submit_lag_seconds": 1,
        }
    )

    with pytest.raises(BrokerSubmitError) as exc_info:
        backend.submit_order_intent(intent)

    assert exc_info.value.context["broker_backend"] == "minqmt_sim"
    assert client.place_calls == []


def test_place_order_failure_raises_broker_rejected_and_records_status() -> None:
    client = FakeQMTClient(next_order_id=-1)
    backend = _backend(client=client)
    intent = _intent()
    with pytest.raises(BrokerRejectedError) as exc_info:
        backend.submit_order_intent(intent)

    assert exc_info.value.context["message"] == "fake rejected"
    handle = OrderHandle(
        handle_id=exc_info.value.context["handle_id"],
        backend_id="minqmt_sim",
        submitted_at=datetime.now(UTC),
        intent_id=intent.intent_id,
    )
    status = backend.query_status(handle)
    assert status.state == "rejected"
    assert status.rejection_reason == "fake rejected"


def test_cancel_calls_miniqmt_cancel_order() -> None:
    client = FakeQMTClient()
    backend = _backend(client=client)
    handle = backend.submit_order_intent(_intent())

    ack = backend.cancel(handle)

    assert isinstance(ack, CancelAck)
    assert ack.accepted is True
    assert client.cancel_calls == ["10001"]


@pytest.mark.parametrize(
    "raw_status,filled,price,expected_state,expected_reason",
    [
        (56, 200, 10.8, "filled", None),
        (55, 100, 10.7, "partial_filled", None),
        (54, 0, 0, "cancelled", None),
        (57, 0, 0, "rejected", "lot invalid"),
        (255, 0, 0, "pending", None),
    ],
)
def test_query_status_maps_miniqmt_order_status_values(
    raw_status: int,
    filled: int,
    price: float,
    expected_state: str,
    expected_reason: str | None,
) -> None:
    client = FakeQMTClient()
    backend = _backend(client=client)
    handle = backend.submit_order_intent(_intent())
    client.orders[0].update(
        {
            "order_status": raw_status,
            "traded_volume": filled,
            "traded_price": price,
            "status_msg": expected_reason or "",
        }
    )

    status = backend.query_status(handle)

    assert isinstance(status, OrderHandleStatus)
    assert status.state == expected_state
    assert status.filled_quantity == (200 if expected_state == "filled" and filled == 0 else filled)
    assert status.rejection_reason == expected_reason


def test_query_account_and_positions_map_miniqmt_authority_snapshots() -> None:
    backend = _backend()

    account = backend.query_account()
    positions = backend.query_positions()
    marked_positions, prices = backend.query_position_marks()

    assert isinstance(account, BrokerAccountSnapshot)
    assert account.backend_id == "minqmt_sim"
    assert account.cash == Decimal("123456.78")
    assert account.nav == Decimal("234567.89")
    assert positions["000001.SZ"].quantity == 300
    assert isinstance(positions["000001.SZ"], PositionLot)
    assert positions["000001.SZ"].portfolio_id == "paper_mq_1"
    assert marked_positions["000001.SZ"].quantity == 300
    assert prices["000001.SZ"] == 10.8


def test_query_status_from_native_reconciles_after_process_restart() -> None:
    client = FakeQMTClient()
    backend = _backend(client=client)
    handle = backend.submit_order_intent(_intent())
    context = backend.order_context(handle)
    client.orders[0].update({"order_status": 56, "traded_volume": 200, "traded_price": 10.88})

    restarted = _backend(client=client)
    status = restarted.query_status_from_native(
        handle_id=context["handle_id"],
        intent=_intent(),
        miniqmt_order_id=context["miniqmt_order_id"],
        strategy_name=context["strategy_name"],
        order_remark=context["order_remark"],
    )

    assert status.state == "filled"
    assert status.filled_quantity == 200
    assert status.avg_fill_price == Decimal("10.88")


def test_market_data_channel_and_bind_capacity_document_exclusive_account() -> None:
    backend = _backend()
    channel = backend.market_data_channel()
    capacity = backend.bind_capacity()

    assert isinstance(channel, MarketDataChannel)
    assert channel.backend_id == "minqmt_sim"
    assert channel.source == MinuteDataSource.MINIQMT_REALTIME
    assert channel.channel_kind == "minqmt_xtdata"
    assert isinstance(capacity, BrokerBindCapacity)
    assert capacity.max_concurrent_packages == 1
    assert "exclusive_account" in capacity.rejection_reason_if_exceeded


def test_subscribe_unsubscribe_contract() -> None:
    backend = _backend()
    sub = backend.subscribe_fill_callback(lambda _event: None)
    assert isinstance(sub, SubscriptionHandle)
    assert sub.backend_id == "minqmt_sim"
    backend.unsubscribe_fill_callback(sub)
    backend.unsubscribe_fill_callback(sub)


def test_minqmtsim_backend_does_not_import_localsim_or_minute_engine() -> None:
    source = Path("backend/services/paper_trading_v2/broker/minqmtsim.py").read_text(encoding="utf-8")
    assert "MinuteExecutionEngine" not in source
    assert "LocalSimBackend" not in source
    assert "from .localsim" not in source


def test_day_runner_dispatches_minqmt_before_minute_execution_path() -> None:
    source = Path("backend/services/paper_trading_v2/day_runner.py").read_text(encoding="utf-8")
    dispatch = 'if portfolio.broker_backend == "minqmt_sim":'
    minute_ledger = "ledger = InMemoryLedger("
    assert dispatch in source
    assert source.index(dispatch) < source.index(minute_ledger)
    assert "_run_minqmt_sim_orders" in source
    assert "MINIQMT_ORDER_SUBMITTED" in source


def test_readiness_minqmt_path_skips_localsim_minute_market_preflight() -> None:
    source = Path("backend/services/paper_trading_v2/readiness.py").read_text(encoding="utf-8")
    assert 'portfolio.broker_backend == "minqmt_sim"' in source
    assert "miniqmt_broker_authority" in source
    assert "miniqmt_execution_authority" in source
    assert '"minute_market_data_check": "skipped"' in source


class _SnapshotOnlyRepository:
    def __init__(self, portfolio: PaperPortfolio) -> None:
        self.portfolio = portfolio
        self.events: list[dict] = []
        self.saved_positions: list[dict] = []
        self.snapshots: list[dict] = []

    def save_run_event(self, *, run_id: str, event_type: str, message: str, context: dict | None = None) -> None:
        self.events.append({"run_id": run_id, "event_type": event_type, "message": message, "context": context or {}})

    def save_positions(self, *, run_id: str, trade_date: date, positions: list[PositionLot], prices: dict[str, float]) -> None:
        self.saved_positions.append(
            {"run_id": run_id, "trade_date": trade_date, "positions": positions, "prices": prices}
        )

    def save_daily_snapshot(self, *, run_id: str, trade_date: date, snapshot, metadata: dict) -> None:
        self.snapshots.append({"run_id": run_id, "trade_date": trade_date, "snapshot": snapshot, "metadata": metadata})

    def update_run_status(self, run: PaperRun, status: RunStatus, error: dict | None = None) -> PaperRun:
        return run.model_copy(update={"status": status, "error": error})

    def update_portfolio_status(self, portfolio_id: str, status: PortfolioStatus) -> PaperPortfolio:
        assert portfolio_id == self.portfolio.portfolio_id
        self.portfolio = self.portfolio.model_copy(update={"status": status})
        return self.portfolio


def test_day_runner_minqmt_no_intents_reconciles_broker_snapshot_without_fills() -> None:
    manifest = make_paper_enabled_manifest()
    portfolio = PaperPortfolio(
        portfolio_id="paper_mq_1",
        portfolio_name="mini qmt no-trade day",
        package_id=manifest.package_id,
        manifest_sha256=manifest.manifest_sha256,
        frozen_manifest=manifest,
        initial_cash=100_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        broker_backend="minqmt_sim",
    )
    run = PaperRun(
        portfolio_id=portfolio.portfolio_id,
        trade_date=TRADE_DATE,
        status=RunStatus.RUNNING,
        data_source=MinuteDataSource.MINIQMT_REALTIME,
    )
    repository = _SnapshotOnlyRepository(portfolio)
    runner = PaperTradingDayRunner(repository=repository)

    result = runner._run_minqmt_sim_orders(
        portfolio=portfolio,
        run=run,
        manifest=manifest,
        trade_date=TRADE_DATE,
        intents=[],
        broker=_backend(),
    )

    assert result.run.status == RunStatus.SUCCEEDED
    assert result.portfolio.status == PortfolioStatus.READY
    assert result.orders == []
    assert result.fills == []
    assert result.account_snapshot.nav == 234567.89
    assert repository.saved_positions[0]["prices"] == {"000001.SZ": 10.8}
    assert repository.snapshots[0]["metadata"]["authority_source"] == "MINIQMT_QUERY"
    assert repository.snapshots[0]["metadata"]["miniqmt_no_local_fills"] is True
    assert any(event["event_type"] == "MINIQMT_NO_ORDER_INTENTS" for event in repository.events)
