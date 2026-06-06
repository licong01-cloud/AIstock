from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from backend.infra.qmt_client import (
    QMTNotAvailableError,
    QMTStatus,
    XtQuantQMTClient,
    build_qmt_order_diagnostic,
)
from backend.services.paper_trading_v2.day_runner import PaperTradingDayRunner
from backend.services.paper_trading_v2.readiness import PaperTradingReadinessService
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService
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
from backend.services.selection_center.hmm_runtime import SectorHMMRuntime
from backend.services.selection_center.risk_policy import RiskDecision, StockRiskPolicyService
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.strategy_package.live_inference import AUTHORITATIVE_SELECTION_SCOPE, AUTHORITATIVE_SELECTION_SOURCE_TYPE
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.runtime import StrategyPackageRuntime
from backend.services.strategy_package.selection_artifact import (
    InMemorySelectionScoreArtifactRepository,
    SelectionScoreArtifact,
    selection_artifact_runtime_hash,
)
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import (
    BrokerConnectivityError,
    BrokerMarketSourceMismatchError,
    BrokerRejectedError,
    BrokerSubmitError,
)
from backend.services.trading_core.models import OrderIntent, OrderSide, OrderStatus, OrderType, PositionLot, RunStatus
from backend.tests.paper_trading_v2.test_day_runner import (
    FakeCalendar,
    FakeSuspendLookup,
    make_paper_enabled_manifest,
    save_manifest_with_default_execution_policy,
)


TRADE_DATE = date(2024, 1, 2)


def test_build_qmt_order_diagnostic_marks_stale_cancelable_and_bad_status_msg() -> None:
    stale_epoch = int(datetime(2024, 1, 2, 9, 31, tzinfo=UTC).timestamp())
    diagnostic = build_qmt_order_diagnostic(
        {
            "order_id": "1090519216",
            "order_status": 50,
            "order_time": str(stale_epoch),
            "status_msg": "[COUNTER][260200][\u00e5\u008f",
        },
        cancelable_only=True,
    )

    assert diagnostic["schema_version"] == "qmt_order_diagnostic_v1"
    assert diagnostic["broker_error_code"] == "260200"
    assert diagnostic["status_msg_maybe_truncated"] is True
    assert diagnostic["status_msg_encoding_warning"] is True
    assert diagnostic["diagnostic_gap"] is True
    assert diagnostic["cancelable_stale_warning"] is True
    assert diagnostic["cancelable_stale_reason"] == "historical_cancelable_order_reported_by_broker"

    code_only = build_qmt_order_diagnostic({"order_status": 57, "status_msg": "[COUNTER][260200]"})
    assert code_only["diagnostic_completeness"] == "broker_status_msg_code_only"
    assert code_only["status_msg_maybe_truncated"] is False


class SlowOrderTrader:
    def order_stock(self, *args, **kwargs):
        import time

        time.sleep(0.05)
        return 10001


def test_xtquant_place_order_timeout_default_is_independent_from_query_timeout(monkeypatch) -> None:
    monkeypatch.delenv("MINIQMT_ORDER_TIMEOUT_SECONDS", raising=False)
    monkeypatch.setenv("MINIQMT_QUERY_TIMEOUT_SECONDS", "0.01")
    client = XtQuantQMTClient(
        enabled=True,
        account_id="acct_unit",
        mode="SIM",
        userdata_path=None,
        session_id=1,
    )
    client._connected = True
    client._trader = SlowOrderTrader()
    client._account = object()
    client._ensure_xtquant = lambda: None  # type: ignore[method-assign]

    order_id, _message = client.place_order(
        stock_code="000001.SZ",
        order_type=23,
        order_volume=100,
        price_type=5,
        price=0.0,
        strategy_name="paper_slot",
        order_remark="remark_1",
    )

    diagnostic = client.get_last_order_diagnostic()
    assert order_id == 10001
    assert diagnostic["timeout_seconds"] == 15.0
    assert diagnostic["timeout_env_key"] == "MINIQMT_ORDER_TIMEOUT_SECONDS"
    assert diagnostic["timeout_policy"] == "bounded_order_submit_ack_wait"
    assert diagnostic["strategy_name"] == "paper_slot"
    assert diagnostic["order_remark"] == "remark_1"


def test_xtquant_place_order_timeout_diagnostic_includes_retry_identity(monkeypatch) -> None:
    monkeypatch.setenv("MINIQMT_ORDER_TIMEOUT_SECONDS", "0.01")
    client = XtQuantQMTClient(
        enabled=True,
        account_id="acct_unit",
        mode="SIM",
        userdata_path=None,
        session_id=1,
    )
    client._connected = True
    client._trader = SlowOrderTrader()
    client._account = object()
    client._ensure_xtquant = lambda: None  # type: ignore[method-assign]

    with pytest.raises(QMTNotAvailableError) as exc_info:
        client.place_order(
            stock_code="000001.SZ",
            order_type=23,
            order_volume=100,
            price_type=5,
            price=0.0,
            strategy_name="paper_slot",
            order_remark="remark_1",
        )

    diagnostic = client.get_last_order_diagnostic()
    assert "timed out after 0.01s" in str(exc_info.value)
    assert diagnostic["classification"] == "adapter_timeout"
    assert diagnostic["timeout_seconds"] == 0.01
    assert diagnostic["strategy_name"] == "paper_slot"
    assert diagnostic["order_remark"] == "remark_1"


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
        self.last_order_diagnostic: dict | None = None
        self.orders: list[dict] = []
        self.trades: list[dict] = []
        self.order_query_calls = 0
        self.trade_query_calls = 0
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
            self.last_order_diagnostic = {
                "schema_version": "qmt_order_submit_diagnostic_v1",
                "accepted": False,
                "raw_return_code": -1,
                "classification": "xtquant_nonpositive_return",
            }
            return -1, "fake rejected"
        order_id = self.next_order_id
        self.next_order_id += 1
        self.last_order_diagnostic = {
            "schema_version": "qmt_order_submit_diagnostic_v1",
            "accepted": True,
            "raw_return_code": order_id,
            "classification": "accepted",
        }
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

    def get_last_order_diagnostic(self):
        return dict(self.last_order_diagnostic) if self.last_order_diagnostic else None

    def get_orders(self, cancelable_only: bool = False):
        self.order_query_calls += 1
        return list(self.orders)

    def get_trades(self):
        self.trade_query_calls += 1
        return list(self.trades)

    def get_account_info(self):
        return dict(self.account)

    def get_positions(self):
        return list(self.positions)

    def cancel_order(self, order_id: str):
        self.cancel_calls.append(str(order_id))
        return True, "cancel accepted"


class TimeoutQMTClient(FakeQMTClient):
    def place_order(self, **kwargs):
        self.place_calls.append(kwargs)
        self.last_order_diagnostic = {
            "schema_version": "qmt_order_submit_diagnostic_v1",
            "accepted": False,
            "raw_return_code": None,
            "classification": "adapter_timeout",
            "exception_type": "TimeoutError",
            "exception_message": "call timed out after 2.0s",
            "timeout_seconds": 2.0,
            "timeout_env_key": "MINIQMT_ORDER_TIMEOUT_SECONDS",
            "timeout_policy": "bounded_order_submit_ack_wait",
        }
        raise QMTNotAvailableError("miniQMT order submit timed out after 2.0s")


class NoopRefreshAudit:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def require_success(self, **kwargs):
        self.calls.append(kwargs)
        return None


class RecordingRiskPolicyService(StockRiskPolicyService):
    def __init__(self, decisions: dict[str, RiskDecision] | None = None) -> None:
        self.decisions = decisions or {}
        self.profile_seen = None

    def evaluate(self, *, symbols, trade_date, profile, current_positions=None):  # type: ignore[override]
        self.profile_seen = profile
        return {symbol: self.decisions.get(symbol, RiskDecision(symbol=symbol)) for symbol in symbols}


class FakeHMMSnapshotProvider:
    def __init__(self, snapshots: dict[str, dict]) -> None:
        self.snapshots = snapshots

    def get_snapshot(self, snapshot_id: str) -> dict | None:
        return self.snapshots.get(snapshot_id)


def _backend(
    *,
    client: FakeQMTClient | None = None,
    auto_connect: bool = True,
    account_mode: str = "exclusive_account",
    account_group_id: str | None = None,
    strategy_slot_id: str | None = "slot_alpha",
) -> MiniQMTSimBackend:
    return MiniQMTSimBackend(
        portfolio_id="paper_mq_1",
        package_id="pkg_mq_1",
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        qmt_client=client or FakeQMTClient(),
        strategy_slot_id=strategy_slot_id,
        account_group_id=account_group_id,
        account_mode=account_mode,
        auto_connect=auto_connect,
    )


def _intent(
    *,
    portfolio_id: str = "paper_mq_1",
    package_id: str = "pkg_mq_1",
    symbol: str = "000001.SZ",
    side: OrderSide = OrderSide.BUY,
    order_type: OrderType = OrderType.MARKET,
    limit_price: float | None = None,
    metadata: dict | None = None,
) -> OrderIntent:
    return OrderIntent(
        package_id=package_id,
        portfolio_id=portfolio_id,
        symbol=symbol,
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


def test_minqmtsim_rejects_unsupported_account_mode() -> None:
    with pytest.raises(BrokerSubmitError) as exc_info:
        MiniQMTSimBackend(
            portfolio_id="paper_mq_1",
            package_id="pkg_mq_1",
            data_source=MinuteDataSource.MINIQMT_REALTIME,
            qmt_client=FakeQMTClient(),
            account_mode="shared_account_attribution",
        )
    assert "account_group_slots" in exc_info.value.context["supported"]


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


def test_account_group_slots_mode_uses_slot_attribution_and_allows_cross_portfolio_package() -> None:
    client = FakeQMTClient()
    backend = _backend(
        client=client,
        account_mode="account_group_slots",
        account_group_id="ag_minqmt_62266303_sim",
        strategy_slot_id=None,
    )
    capacity = backend.bind_capacity()
    intent = _intent(
        portfolio_id="paper_other",
        package_id="pkg_other",
        metadata={
            "account_group_id": "ag_minqmt_62266303_sim",
            "strategy_slot_id": "slot_alpha",
            "strategy_name": "UnifiedAlpha",
            "order_remark_prefix": "ag622-alpha",
            "order_remark": "ag622-alpha:intent-1",
        },
    )

    handle = backend.submit_order_intent(intent)

    assert capacity.max_concurrent_packages > 1
    call = client.place_calls[-1]
    assert call["strategy_name"] == "UnifiedAlpha"
    assert call["order_remark"] == "ag622-alpha:intent-1"
    context = backend.order_context(handle)
    assert context["account_group_id"] == "ag_minqmt_62266303_sim"
    assert context["strategy_slot_id"] == "slot_alpha"


def test_account_group_slots_mode_requires_explicit_slot_attribution_without_qmt_call() -> None:
    client = FakeQMTClient()
    backend = _backend(client=client, account_mode="account_group", account_group_id="ag_minqmt_62266303_sim")

    with pytest.raises(BrokerSubmitError) as exc_info:
        backend.submit_order_intent(_intent(metadata={"account_group_id": "ag_minqmt_62266303_sim"}))

    assert exc_info.value.context["missing_metadata_key"] == "strategy_slot_id"
    assert client.place_calls == []


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
    assert status.raw["raw_return_code"] == -1
    assert status.raw["submit_diagnostic"]["classification"] == "xtquant_nonpositive_return"
    assert status.raw["diagnostic_gap_reason"] == "xtquant_nonpositive_return"


def test_place_order_timeout_is_connectivity_error_with_diagnostic() -> None:
    client = TimeoutQMTClient()
    backend = _backend(client=client)
    intent = _intent()

    with pytest.raises(BrokerConnectivityError) as exc_info:
        backend.submit_order_intent(intent)

    assert exc_info.value.context["reason"] == "miniQMT order submit timed out after 2.0s"
    assert exc_info.value.context["submit_diagnostic"]["classification"] == "adapter_timeout"
    assert exc_info.value.context["submit_diagnostic"]["timeout_seconds"] == 2.0
    assert exc_info.value.context["order_remark"]
    assert exc_info.value.context["native_reconcile"]["schema_version"] == "miniqmt_submit_error_native_probe_v1"
    assert exc_info.value.context["native_reconcile"]["orders_query_ok"] is True
    assert exc_info.value.context["native_reconcile"]["trades_query_ok"] is True
    assert client.order_query_calls == 1
    assert client.trade_query_calls == 1
    assert client.place_calls[0]["stock_code"] == "000001.SZ"


def test_day_runner_minqmt_submit_error_persists_rejection_diagnostic() -> None:
    manifest = make_paper_enabled_manifest()
    portfolio = PaperPortfolio(
        portfolio_id="paper_mq_1",
        portfolio_name="mini qmt submit reject diagnostic",
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
        runtime_config={"paper_v2_session": {"session_id": "psess_unit"}},
    )
    repository = _SnapshotOnlyRepository(portfolio)
    broker = MiniQMTSimBackend(
        portfolio_id=portfolio.portfolio_id,
        package_id=manifest.package_id,
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        qmt_client=FakeQMTClient(next_order_id=-1),
    )

    with pytest.raises(BrokerRejectedError):
        PaperTradingDayRunner(repository=repository)._run_minqmt_sim_orders(
            portfolio=portfolio,
            run=run,
            manifest=manifest,
            trade_date=TRADE_DATE,
            intents=[_intent(portfolio_id=portfolio.portfolio_id, package_id=manifest.package_id)],
            broker=broker,
        )

    final_order = repository.orders[0]
    assert final_order.status.value == "REJECTED"
    assert final_order.metadata["broker_status"] == "submit_error"
    assert final_order.metadata["broker_status_msg"] == "MiniQMT rejected the order"
    assert final_order.metadata["broker_error"]["context"]["message"] == "fake rejected"
    assert final_order.metadata["broker_handle_id"]
    assert final_order.metadata["miniqmt_order_id"] == "-1"
    diagnostic = final_order.metadata["broker_diagnostic"]
    assert diagnostic["authority_source"] == "MINIQMT_SUBMIT_ERROR"
    assert diagnostic["broker_status_msg"] == "MiniQMT rejected the order"
    assert diagnostic["broker_handle_id"] == final_order.metadata["broker_handle_id"]
    assert diagnostic["miniqmt_order_id"] == "-1"
    assert diagnostic["broker_audit"]["after_reconcile"]["native"]["broker_submit_message"] == "fake rejected"
    assert diagnostic["broker_audit"]["before_submit"]["account"]["cash"] == 123456.78
    assert repository.order_events[0].metadata["broker_error"]["context"]["message"] == "fake rejected"
    assert repository.events[-1]["event_type"] == "MINIQMT_ORDER_SUBMIT_FAILED"


def test_day_runner_minqmt_timeout_persists_connectivity_diagnostic() -> None:
    manifest = make_paper_enabled_manifest()
    portfolio = PaperPortfolio(
        portfolio_id="paper_mq_1",
        portfolio_name="mini qmt submit timeout diagnostic",
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
        runtime_config={"paper_v2_session": {"session_id": "psess_unit"}},
    )
    repository = _SnapshotOnlyRepository(portfolio)
    broker = MiniQMTSimBackend(
        portfolio_id=portfolio.portfolio_id,
        package_id=manifest.package_id,
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        qmt_client=TimeoutQMTClient(),
    )

    with pytest.raises(BrokerConnectivityError):
        PaperTradingDayRunner(repository=repository)._run_minqmt_sim_orders(
            portfolio=portfolio,
            run=run,
            manifest=manifest,
            trade_date=TRADE_DATE,
            intents=[_intent(portfolio_id=portfolio.portfolio_id, package_id=manifest.package_id)],
            broker=broker,
        )

    final_order = repository.orders[0]
    diagnostic = final_order.metadata["broker_diagnostic"]
    assert final_order.status.value == "REJECTED"
    assert final_order.metadata["broker_error"]["error_code"] == "BROKER_CONNECTIVITY_ERROR"
    assert final_order.metadata["broker_error"]["context"]["submit_diagnostic"]["classification"] == "adapter_timeout"
    assert diagnostic["submit_diagnostic"]["classification"] == "adapter_timeout"
    assert diagnostic["broker_error"]["context"]["native_reconcile"]["schema_version"] == "miniqmt_submit_error_native_probe_v1"
    assert diagnostic["broker_error"]["context"]["native_reconcile"]["orders_query_ok"] is True
    assert diagnostic["broker_error"]["context"]["native_reconcile"]["trades_query_ok"] is True
    assert diagnostic["broker_audit"]["before_submit"]["account"]["cash"] == 123456.78
    assert diagnostic["broker_audit"]["after_reconcile"]["account"]["cash"] == 123456.78
    assert repository.events[-1]["event_type"] == "MINIQMT_ORDER_SUBMIT_FAILED"


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


def _portfolio_backend_factory(client: FakeQMTClient):
    return lambda **kwargs: MiniQMTSimBackend(qmt_client=client, **kwargs)


class _RecordingMiniQMTBroker:
    def __init__(self, *, account=None, positions=None, prices=None) -> None:
        self.submitted: list[OrderIntent] = []
        self._account = account or BrokerAccountSnapshot(
            backend_id="minqmt_sim",
            cash=Decimal("100000"),
            nav=Decimal("100000"),
            margin_used=None,
            as_of=datetime(2024, 1, 2, 15, 0, tzinfo=UTC),
        )
        self._positions = positions or {}
        self._prices = prices or {}
        self._statuses: dict[str, OrderHandleStatus] = {}
        self._trades: dict[str, list[dict]] = {}

    def submit_order_intent(self, intent: OrderIntent) -> OrderHandle:
        self.submitted.append(intent)
        handle = OrderHandle(
            handle_id=f"handle_{len(self.submitted)}",
            backend_id="minqmt_sim",
            submitted_at=datetime(2024, 1, 2, 9, 31, tzinfo=UTC),
            intent_id=intent.intent_id,
        )
        self._statuses[handle.handle_id] = OrderHandleStatus(
            handle_id=handle.handle_id,
            state="pending",
            filled_quantity=0,
            avg_fill_price=None,
            last_event_at=handle.submitted_at,
            rejection_reason=None,
        )
        return handle

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

    def query_trades(self, handle: OrderHandle) -> list[dict]:
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
        for submitted in self.submitted:
            if submitted.intent_id == intent.intent_id:
                return self._statuses.get(
                    handle_id,
                    OrderHandleStatus(
                        handle_id=handle_id,
                        state="pending",
                        filled_quantity=0,
                        avg_fill_price=None,
                        last_event_at=datetime(2024, 1, 2, 9, 31, tzinfo=UTC),
                        rejection_reason=None,
                    ),
                )
        raise AssertionError(f"unknown native intent: {intent.intent_id}")

    def query_trades_from_native(
        self,
        *,
        handle_id: str,
        intent: OrderIntent,
        miniqmt_order_id: str,
        strategy_name: str,
        order_remark: str,
    ) -> list[dict]:
        return list(self._trades.get(handle_id, []))

    def query_account(self) -> BrokerAccountSnapshot:
        return self._account

    def query_position_marks(self):
        return self._positions, self._prices

    def shutdown(self) -> None:
        return None


class _RejectedMiniQMTBroker(_RecordingMiniQMTBroker):
    def submit_order_intent(self, intent: OrderIntent) -> OrderHandle:
        handle = super().submit_order_intent(intent)
        self._statuses[handle.handle_id] = OrderHandleStatus(
            handle_id=handle.handle_id,
            state="rejected",
            filled_quantity=0,
            avg_fill_price=None,
            last_event_at=handle.submitted_at,
            rejection_reason="[COUNTER][260200] insufficient buying power",
            raw_status=57,
            status_msg="[COUNTER][260200] insufficient buying power",
            raw={
                "order_status": 57,
                "status_msg": "[COUNTER][260200] insufficient buying power",
                "order_id": self.order_context(handle)["miniqmt_order_id"],
                "traded_volume": 0,
            },
        )
        return handle


class _FilledTradeMiniQMTBroker(_RecordingMiniQMTBroker):
    def submit_order_intent(self, intent: OrderIntent) -> OrderHandle:
        handle = super().submit_order_intent(intent)
        self._statuses[handle.handle_id] = OrderHandleStatus(
            handle_id=handle.handle_id,
            state="filled",
            filled_quantity=intent.quantity,
            avg_fill_price=Decimal("10.88"),
            last_event_at=handle.submitted_at,
            rejection_reason=None,
        )
        context = self.order_context(handle)
        self._trades[handle.handle_id] = [
            {
                "traded_id": f"trade_{handle.intent_id}",
                "stock_code": intent.symbol,
                "stock_name": "Unit Test Stock",
                "order_type": 23 if intent.side == OrderSide.BUY else 24,
                "traded_time": "093105",
                "traded_price": 10.88,
                "traded_volume": intent.quantity,
                "traded_amount": 10.88 * intent.quantity,
                "order_id": context["miniqmt_order_id"],
                "order_sysid": f"sys_{handle.intent_id}",
                "commission": 5.0,
                "strategy_name": context["strategy_name"],
                "order_remark": context["order_remark"],
            }
        ]
        return handle


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


def test_query_trades_maps_miniqmt_native_trade_rows() -> None:
    client = FakeQMTClient()
    backend = _backend(client=client)
    handle = backend.submit_order_intent(_intent())
    context = backend.order_context(handle)
    client.trades.append(
        {
            "traded_id": "trade_1",
            "stock_code": "000001.SZ",
            "stock_name": "Ping An Bank",
            "order_type": 23,
            "traded_time": "093105",
            "traded_price": 10.88,
            "traded_volume": 200,
            "traded_amount": 2176.0,
            "order_id": context["miniqmt_order_id"],
            "order_sysid": "sys_1",
            "commission": 5.0,
            "strategy_name": context["strategy_name"],
            "order_remark": context["order_remark"],
        }
    )

    trades = backend.query_trades(handle)

    assert len(trades) == 1
    assert trades[0]["traded_id"] == "trade_1"
    assert trades[0]["order_id"] == context["miniqmt_order_id"]


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


def test_router_exposes_order_diagnostic_metadata_as_top_level_fields() -> None:
    from backend.routers.paper_trading_v2 import _expose_order_diagnostics

    row = {
        "order_id": "ord_rejected",
        "status": "REJECTED",
        "metadata": {
            "broker_status": "rejected",
            "broker_raw_status": 57,
            "broker_status_msg": "[COUNTER][260200] insufficient buying power",
            "broker_rejection_reason": "[COUNTER][260200] insufficient buying power",
            "broker_diagnostic": {
                "schema_version": "miniqmt_order_diagnostic_v1",
                "broker_error_code": "260200",
                "broker_rejection_classification": "counter_260200",
                "diagnostic_completeness": "best_available",
                "diagnostic_gap": False,
                "status_msg_best_available": "[COUNTER][260200] insufficient buying power",
            },
        },
    }

    exposed = _expose_order_diagnostics(row)

    assert exposed["broker_raw_status"] == 57
    assert exposed["broker_status_msg"] == "[COUNTER][260200] insufficient buying power"
    assert exposed["status_msg"] == "[COUNTER][260200] insufficient buying power"
    assert exposed["error_msg"] == "[COUNTER][260200] insufficient buying power"
    assert exposed["broker_diagnostic"]["schema_version"] == "miniqmt_order_diagnostic_v1"
    assert exposed["broker_error_code"] == "260200"
    assert exposed["broker_rejection_classification"] == "counter_260200"
    assert exposed["diagnostic_completeness"] == "best_available"
    assert exposed["diagnostic_gap"] is False
    assert exposed["status_msg_best_available"] == "[COUNTER][260200] insufficient buying power"


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
        self.orders: list[Any] = []
        self.fills: list[Any] = []
        self.order_events: list[Any] = []
        self.execution_states: list[Any] = []

    def save_run_event(self, *, run_id: str, event_type: str, message: str, context: dict | None = None) -> None:
        self.events.append({"run_id": run_id, "event_type": event_type, "message": message, "context": context or {}})

    def save_positions(self, *, run_id: str, trade_date: date, positions: list[PositionLot], prices: dict[str, float]) -> None:
        self.saved_positions.append(
            {"run_id": run_id, "trade_date": trade_date, "positions": positions, "prices": prices}
        )

    def save_daily_snapshot(self, *, run_id: str, trade_date: date, snapshot, metadata: dict) -> None:
        self.snapshots.append({"run_id": run_id, "trade_date": trade_date, "snapshot": snapshot, "metadata": metadata})

    def save_order(self, run_id: str, order) -> None:
        self.orders = [item for item in self.orders if item.order_id != order.order_id]
        self.orders.append(order)

    def list_orders_for_run(self, run_id: str) -> list[Any]:
        return list(self.orders)

    def save_fill(self, run_id: str, fill, *, intended_price: float | None = None, fill_market_context: dict | None = None) -> None:
        self.fills.append(
            {
                "run_id": run_id,
                "fill": fill,
                "intended_price": intended_price,
                "fill_market_context": fill_market_context,
            }
        )

    def list_fills_for_run(self, run_id: str) -> list[dict]:
        return [
            {"run_id": item["run_id"], **item["fill"].model_dump(mode="python")}
            for item in self.fills
            if item["run_id"] == run_id
        ]

    def save_order_event(self, run_id: str, event) -> None:
        self.order_events.append(event)

    def save_order_execution_state(self, state):
        self.execution_states.append(state)
        return state

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
    assert repository.events[-1]["event_type"] == "RUN_SUCCEEDED"
    assert repository.events[-1]["context"]["authority_source"] == "MINIQMT_QUERY"


def test_day_runner_minqmt_submits_sell_intents_before_buy_intents() -> None:
    manifest = make_paper_enabled_manifest()
    portfolio = PaperPortfolio(
        portfolio_id="paper_mq_1",
        portfolio_name="mini qmt sell before buy",
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
    buy = _intent(portfolio_id=portfolio.portfolio_id, package_id=manifest.package_id, symbol="000003.SZ", side=OrderSide.BUY)
    sell = _intent(portfolio_id=portfolio.portfolio_id, package_id=manifest.package_id, symbol="000002.SZ", side=OrderSide.SELL)
    broker = _RecordingMiniQMTBroker()
    repository = _SnapshotOnlyRepository(portfolio)

    PaperTradingDayRunner(repository=repository)._run_minqmt_sim_orders(
        portfolio=portfolio,
        run=run,
        manifest=manifest,
        trade_date=TRADE_DATE,
        intents=[buy, sell],
        broker=broker,  # type: ignore[arg-type]
    )

    assert [intent.side for intent in broker.submitted] == [OrderSide.SELL, OrderSide.BUY]
    assert all(order.filled_quantity == 0 for order in repository.orders)
    assert all(order.status.value == "SUBMITTED" for order in repository.orders)


def test_day_runner_minqmt_persists_rejected_order_diagnostic_and_audit() -> None:
    manifest = make_paper_enabled_manifest()
    portfolio = PaperPortfolio(
        portfolio_id="paper_mq_1",
        portfolio_name="mini qmt reject diagnostic",
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
        runtime_config={"paper_v2_session": {"session_id": "psess_unit"}},
    )
    repository = _SnapshotOnlyRepository(portfolio)
    broker = _RejectedMiniQMTBroker()

    result = PaperTradingDayRunner(repository=repository)._run_minqmt_sim_orders(
        portfolio=portfolio,
        run=run,
        manifest=manifest,
        trade_date=TRADE_DATE,
        intents=[_intent(portfolio_id=portfolio.portfolio_id, package_id=manifest.package_id)],
        broker=broker,  # type: ignore[arg-type]
    )

    final_order = result.orders[0]
    assert final_order.status.value == "REJECTED"
    assert final_order.metadata["broker_raw_status"] == 57
    assert final_order.metadata["broker_status_msg"] == "[COUNTER][260200] insufficient buying power"
    assert final_order.metadata["broker_rejection_reason"] == "[COUNTER][260200] insufficient buying power"
    diagnostic = final_order.metadata["broker_diagnostic"]
    assert diagnostic["broker_status"] == "rejected"
    assert diagnostic["broker_status_raw"]["order_status"] == 57
    assert diagnostic["broker_error_code"] == "260200"
    assert diagnostic["broker_rejection_classification"] == "counter_260200"
    assert diagnostic["diagnostic_completeness"] == "best_available"
    assert diagnostic["status_msg_best_available"] == "[COUNTER][260200] insufficient buying power"
    assert diagnostic["broker_audit"]["before_submit"]["account"]["cash"] == 100000.0
    assert diagnostic["broker_audit"]["after_reconcile"]["account"]["cash"] == 100000.0
    assert repository.order_events[-1].metadata["broker_rejection_reason"] == "[COUNTER][260200] insufficient buying power"
    assert repository.execution_states[0].algo_state["broker_status_msg"] == "[COUNTER][260200] insufficient buying power"
    assert repository.events[-1]["event_type"] == "RUN_SUCCEEDED"
    submit_event = next(event for event in repository.events if event["event_type"] == "MINIQMT_ORDER_SUBMITTED")
    assert submit_event["context"]["broker_raw_status"] == 57
    assert submit_event["context"]["broker_diagnostic"]["broker_audit"]["schema_version"] == "miniqmt_broker_audit_v1"


def test_day_runner_minqmt_persists_native_trade_fills_and_order_events() -> None:
    manifest = make_paper_enabled_manifest()
    portfolio = PaperPortfolio(
        portfolio_id="paper_mq_1",
        portfolio_name="mini qmt fill reconciliation",
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
        runtime_config={"paper_v2_session": {"session_id": "psess_unit"}},
    )
    repository = _SnapshotOnlyRepository(portfolio)
    broker = _FilledTradeMiniQMTBroker(
        account=BrokerAccountSnapshot(
            backend_id="minqmt_sim",
            cash=Decimal("97500"),
            nav=Decimal("99700"),
            margin_used=None,
            as_of=datetime(2024, 1, 2, 15, 0, tzinfo=UTC),
        ),
        positions={
            "000001.SZ": PositionLot(
                portfolio_id=portfolio.portfolio_id,
                symbol="000001.SZ",
                quantity=200,
                available_quantity=0,
                avg_cost=10.88,
                trade_date=TRADE_DATE,
            )
        },
        prices={"000001.SZ": 11.0},
    )

    result = PaperTradingDayRunner(repository=repository)._run_minqmt_sim_orders(
        portfolio=portfolio,
        run=run,
        manifest=manifest,
        trade_date=TRADE_DATE,
        intents=[_intent(portfolio_id=portfolio.portfolio_id, package_id=manifest.package_id)],
        broker=broker,  # type: ignore[arg-type]
    )

    assert len(result.fills) == 1
    assert result.fills[0].fill_id.startswith("fill_minqmt_trade_")
    assert result.fills[0].order_id == result.orders[0].order_id
    assert result.orders[0].status.value == "FILLED"
    assert result.orders[0].filled_quantity == 200
    assert repository.fills[0]["fill_market_context"]["data_source"] == "MINIQMT_REALTIME"
    assert repository.order_events[0].event_type.value == "FILLED"
    assert repository.execution_states[0].session_id == "psess_unit"
    assert repository.execution_states[0].status == "FILLED"
    assert repository.snapshots[0]["metadata"]["fill_count"] == 1
    assert repository.snapshots[0]["metadata"]["miniqmt_no_local_fills"] is False


def test_reconcile_minqmt_native_run_updates_existing_order_metadata_with_rejection_diagnostic() -> None:
    manifest = make_paper_enabled_manifest()
    portfolio = PaperPortfolio(
        portfolio_id="paper_mq_1",
        portfolio_name="mini qmt delayed reject diagnostic",
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
        runtime_config={"paper_v2_session": {"session_id": "psess_unit"}},
    )
    repository = _SnapshotOnlyRepository(portfolio)
    broker = _RecordingMiniQMTBroker()
    result = PaperTradingDayRunner(repository=repository)._run_minqmt_sim_orders(
        portfolio=portfolio,
        run=run,
        manifest=manifest,
        trade_date=TRADE_DATE,
        intents=[_intent(portfolio_id=portfolio.portfolio_id, package_id=manifest.package_id)],
        broker=broker,  # type: ignore[arg-type]
    )
    original = repository.orders[0]
    assert original.status.value == "SUBMITTED"
    handle_id = original.metadata["broker_handle_id"]
    broker._statuses[handle_id] = OrderHandleStatus(
        handle_id=handle_id,
        state="rejected",
        filled_quantity=0,
        avg_fill_price=None,
        last_event_at=datetime(2024, 1, 2, 14, 30, tzinfo=UTC),
        rejection_reason="[COUNTER][260200] insufficient buying power",
        raw_status=57,
        status_msg="[COUNTER][260200] insufficient buying power",
        raw={"order_status": 57, "status_msg": "[COUNTER][260200] insufficient buying power"},
    )

    reconciled = PaperTradingDayRunner(repository=repository).reconcile_minqmt_native_run(
        portfolio=portfolio,
        run=result.run,
        trade_date=TRADE_DATE,
        broker=broker,  # type: ignore[arg-type]
    )

    final_order = reconciled.orders[0]
    assert final_order.status.value == "REJECTED"
    assert final_order.metadata["broker_raw_status"] == 57
    assert final_order.metadata["broker_status_msg"] == "[COUNTER][260200] insufficient buying power"
    assert final_order.metadata["broker_diagnostic"]["broker_status_raw"]["order_status"] == 57
    assert final_order.metadata["broker_error_code"] == "260200"
    assert final_order.metadata["broker_rejection_classification"] == "counter_260200"
    assert final_order.metadata["diagnostic_completeness"] == "best_available"
    assert final_order.metadata["broker_audit"]["before_submit"]["phase"] == "before_native_reconcile"
    assert final_order.metadata["broker_audit"]["after_reconcile"]["phase"] == "after_native_reconcile"
    native_reject_event = next(
        event for event in repository.order_events if event.event_id.startswith("evt_minqmt_native_")
    )
    assert native_reject_event.event_type.value == "REJECTED"
    assert native_reject_event.reason == "[COUNTER][260200] insufficient buying power"
    assert native_reject_event.metadata["terminal_reconcile_event"] is True
    assert native_reject_event.metadata["broker_status_raw"]["order_status"] == 57
    assert native_reject_event.metadata["broker_error_code"] == "260200"
    assert native_reject_event.metadata["broker_rejection_classification"] == "counter_260200"
    run_event = next(
        event for event in repository.events if event["event_type"] == "MINIQMT_NATIVE_ORDER_REJECTED_RECONCILED"
    )
    assert run_event["context"]["broker_status_msg"] == "[COUNTER][260200] insufficient buying power"
    assert repository.execution_states[-1].algo_state["broker_status_msg"] == "[COUNTER][260200] insufficient buying power"


def test_reconcile_minqmt_native_run_repairs_prefixed_rejected_order_with_stale_pending_metadata() -> None:
    manifest = make_paper_enabled_manifest()
    portfolio = PaperPortfolio(
        portfolio_id="paper_mq_1",
        portfolio_name="mini qmt prefixed reject diagnostic",
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
        runtime_config={"paper_v2_session": {"session_id": "psess_unit"}},
    )
    repository = _SnapshotOnlyRepository(portfolio)
    broker = _RecordingMiniQMTBroker()
    result = PaperTradingDayRunner(repository=repository)._run_minqmt_sim_orders(
        portfolio=portfolio,
        run=run,
        manifest=manifest,
        trade_date=TRADE_DATE,
        intents=[_intent(portfolio_id=portfolio.portfolio_id, package_id=manifest.package_id)],
        broker=broker,  # type: ignore[arg-type]
    )
    original = repository.orders[0]
    handle_id = original.metadata["broker_handle_id"]
    stale_rejected = original.model_copy(
        update={
            "status": OrderStatus.REJECTED,
            "metadata": {
                **original.metadata,
                "broker_status": "pending",
                "broker_raw_status": "submitted",
                "broker_status_msg": None,
                "broker_rejection_reason": None,
                "broker_diagnostic": None,
            },
        }
    )
    repository.save_order(result.run.run_id, stale_rejected)
    broker._statuses[handle_id] = OrderHandleStatus(
        handle_id=handle_id,
        state="rejected",
        filled_quantity=0,
        avg_fill_price=None,
        last_event_at=datetime(2024, 1, 2, 14, 30, tzinfo=UTC),
        rejection_reason="[COUNTER][260200]",
        raw_status=57,
        status_msg="[COUNTER][260200]",
        raw={"order_status": 57, "status_msg": "[COUNTER][260200]", "order_id": original.metadata["miniqmt_order_id"]},
    )

    reconciled = PaperTradingDayRunner(repository=repository).reconcile_minqmt_native_run(
        portfolio=portfolio,
        run=result.run,
        trade_date=TRADE_DATE,
        broker=broker,  # type: ignore[arg-type]
    )
    repeated = PaperTradingDayRunner(repository=repository).reconcile_minqmt_native_run(
        portfolio=portfolio,
        run=reconciled.run,
        trade_date=TRADE_DATE,
        broker=broker,  # type: ignore[arg-type]
    )

    final_order = reconciled.orders[0]
    assert final_order.status.value == "REJECTED"
    assert final_order.metadata["broker_status"] == "rejected"
    assert final_order.metadata["broker_raw_status"] == 57
    assert final_order.metadata["broker_status_msg"] == "[COUNTER][260200]"
    assert final_order.metadata["broker_error_code"] == "260200"
    assert final_order.metadata["broker_rejection_classification"] == "counter_260200"
    assert final_order.metadata["diagnostic_completeness"] == "broker_status_msg_code_only"
    assert final_order.metadata["diagnostic_gap"] is True
    native_events = [event for event in repository.order_events if event.event_id.startswith("evt_minqmt_native_")]
    assert len(native_events) == 1
    assert native_events[0].metadata["previous_paper_status"] == "REJECTED"
    assert native_events[0].metadata["broker_status_raw"]["order_status"] == 57
    assert repeated.fills == []


def test_reconcile_minqmt_native_run_marks_truncated_or_mojibake_status_msg_gap() -> None:
    manifest = make_paper_enabled_manifest()
    portfolio = PaperPortfolio(
        portfolio_id="paper_mq_1",
        portfolio_name="mini qmt truncated reject diagnostic",
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
        runtime_config={"paper_v2_session": {"session_id": "psess_unit"}},
    )
    repository = _SnapshotOnlyRepository(portfolio)
    broker = _RecordingMiniQMTBroker()
    result = PaperTradingDayRunner(repository=repository)._run_minqmt_sim_orders(
        portfolio=portfolio,
        run=run,
        manifest=manifest,
        trade_date=TRADE_DATE,
        intents=[_intent(portfolio_id=portfolio.portfolio_id, package_id=manifest.package_id)],
        broker=broker,  # type: ignore[arg-type]
    )
    handle_id = repository.orders[0].metadata["broker_handle_id"]
    broker._statuses[handle_id] = OrderHandleStatus(
        handle_id=handle_id,
        state="rejected",
        filled_quantity=0,
        avg_fill_price=None,
        last_event_at=datetime(2024, 1, 2, 14, 30, tzinfo=UTC),
        rejection_reason="[COUNTER][260200][\u00e5\u008f\u00af",
        raw_status=57,
        status_msg="[COUNTER][260200][\u00e5\u008f\u00af",
        raw={"order_status": 57, "status_msg": "[COUNTER][260200][\u00e5\u008f\u00af"},
    )

    reconciled = PaperTradingDayRunner(repository=repository).reconcile_minqmt_native_run(
        portfolio=portfolio,
        run=result.run,
        trade_date=TRADE_DATE,
        broker=broker,  # type: ignore[arg-type]
    )

    diagnostic = reconciled.orders[0].metadata["broker_diagnostic"]
    assert diagnostic["broker_error_code"] == "260200"
    assert diagnostic["broker_rejection_classification"] == "counter_260200"
    assert diagnostic["diagnostic_completeness"] == "broker_status_msg_truncated_or_encoding_uncertain"
    assert diagnostic["diagnostic_gap"] is True
    assert diagnostic["status_msg_maybe_truncated"] is True
    assert diagnostic["status_msg_encoding_warning"] is True
    assert reconciled.orders[0].metadata["diagnostic_gap"] is True


def test_day_runner_minqmt_reconciles_native_fills_after_initial_pending_submit() -> None:
    manifest = make_paper_enabled_manifest()
    portfolio = PaperPortfolio(
        portfolio_id="paper_mq_1",
        portfolio_name="mini qmt delayed fill reconciliation",
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
        runtime_config={"paper_v2_session": {"session_id": "psess_unit"}},
    )
    repository = _SnapshotOnlyRepository(portfolio)
    broker = _RecordingMiniQMTBroker(
        account=BrokerAccountSnapshot(
            backend_id="minqmt_sim",
            cash=Decimal("97500"),
            nav=Decimal("99700"),
            margin_used=None,
            as_of=datetime(2024, 1, 2, 15, 0, tzinfo=UTC),
        ),
        positions={
            "000001.SZ": PositionLot(
                portfolio_id=portfolio.portfolio_id,
                symbol="000001.SZ",
                quantity=200,
                available_quantity=0,
                avg_cost=10.88,
                trade_date=TRADE_DATE,
            )
        },
        prices={"000001.SZ": 11.0},
    )
    result = PaperTradingDayRunner(repository=repository)._run_minqmt_sim_orders(
        portfolio=portfolio,
        run=run,
        manifest=manifest,
        trade_date=TRADE_DATE,
        intents=[_intent(portfolio_id=portfolio.portfolio_id, package_id=manifest.package_id)],
        broker=broker,  # type: ignore[arg-type]
    )
    assert result.fills == []
    assert repository.orders[0].filled_quantity == 0
    handle_id = repository.orders[0].metadata["broker_handle_id"]
    native_id = repository.orders[0].metadata["miniqmt_order_id"]
    broker._statuses[handle_id] = OrderHandleStatus(
        handle_id=handle_id,
        state="filled",
        filled_quantity=200,
        avg_fill_price=Decimal("10.88"),
        last_event_at=datetime(2024, 1, 2, 14, 30, tzinfo=UTC),
        rejection_reason=None,
    )
    broker._trades[handle_id] = [
        {
            "traded_id": "delayed_trade_1",
            "stock_code": "000001.SZ",
            "stock_name": "Unit Test Stock",
            "order_type": 23,
            "traded_time": "143000",
            "traded_price": 10.88,
            "traded_volume": 200,
            "traded_amount": 2176.0,
            "order_id": native_id,
            "order_sysid": "sys_delayed",
            "commission": 5.0,
            "strategy_name": "slot_alpha",
            "order_remark": repository.orders[0].metadata["order_remark"],
        }
    ]

    reconciled = PaperTradingDayRunner(repository=repository).reconcile_minqmt_native_run(
        portfolio=portfolio,
        run=result.run,
        trade_date=TRADE_DATE,
        broker=broker,  # type: ignore[arg-type]
    )
    repeated = PaperTradingDayRunner(repository=repository).reconcile_minqmt_native_run(
        portfolio=portfolio,
        run=reconciled.run,
        trade_date=TRADE_DATE,
        broker=broker,  # type: ignore[arg-type]
    )

    assert len(repository.fills) == 1
    assert repository.fills[0]["fill"].quantity == 200
    assert repository.orders[-1].status.value == "FILLED"
    assert repository.orders[-1].filled_quantity == 200
    assert repository.snapshots[-1]["metadata"]["fill_count"] == 1
    assert repository.snapshots[-1]["metadata"]["miniqmt_no_local_fills"] is False
    assert repeated.fills == []


def _miniqmt_portfolio_fixture(*, custom_params: dict | None = None):
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest(custom_params=custom_params)
    save_manifest_with_default_execution_policy(package_repo, manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="mini qmt runtime contract",
        initial_cash=100_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        broker_backend="minqmt_sim",
    )
    return package_repo, paper_repo, manifest, portfolio


def _runtime_with_artifact(manifest, *, runtime_config: dict | None = None, hmm_runtime=None) -> StrategyPackageRuntime:
    artifact_repo = InMemorySelectionScoreArtifactRepository()
    rows = [
        {
            "symbol": "000002.SZ",
            "score": 0.92,
            "rank": 1,
            "target_weight": 0.03,
            "reference_price": 10.0,
        },
        {
            "symbol": "000003.SZ",
            "score": 0.88,
            "rank": 2,
            "target_weight": 0.03,
            "reference_price": 10.0,
        },
    ]
    artifact_repo.save(
        SelectionScoreArtifact(
            package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256 or "",
            trade_date=TRADE_DATE,
            data_source=MinuteDataSource.DB_HISTORICAL.value,
            runtime_config_hash=selection_artifact_runtime_hash(runtime_config or {}),
            scores_json=rows,
            score_count=len(rows),
            universe_count=len(rows),
            top_score_symbol=rows[0]["symbol"],
            metadata={
                "source_type": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                "authority_scope": AUTHORITATIVE_SELECTION_SCOPE,
                "test_seeded": True,
            },
        )
    )
    return StrategyPackageRuntime(artifact_repository=artifact_repo, hmm_runtime=hmm_runtime)


def test_minqmt_readiness_preserves_disabled_hmm_and_uses_platform_risk_profile() -> None:
    custom_params = {
        "strategy_id": "score_weighted_topk_v2",
        "topk": 2,
        "enable_sector_hmm": True,
        "hmm_model_snapshot_id": "qe_hmm_snapshot_old",
        "hmm_signal_preset": "qe_preset",
        "risk_policy": {"enabled": True, "providers": ["st_pit"]},
    }
    package_repo, paper_repo, manifest, portfolio = _miniqmt_portfolio_fixture(custom_params=custom_params)
    runtime_config = {
        "runtime_profile": {
            "hmm": {"enabled": False},
            "risk_policy": {"enabled": True},
            "tradability": {"exclude_suspended": False},
        }
    }
    profile_service = PaperTradingV2PortfolioService(package_repository=package_repo, repository=paper_repo)
    _profile, version = profile_service.create_runtime_profile(
        portfolio_id=portfolio.portfolio_id,
        profile_name="miniqmt disabled hmm readiness",
        config_json=runtime_config,
        created_by="unit_test",
    )
    activation = profile_service.activate_runtime_config(
        portfolio_id=portfolio.portfolio_id,
        trade_date=TRADE_DATE,
        profile_version_id=version.profile_version_id,
        activated_by="unit_test",
        reason="MiniQMT readiness runtime profile",
    )
    risk_policy = RecordingRiskPolicyService()

    readiness = PaperTradingReadinessService(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        runtime=_runtime_with_artifact(manifest),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
        risk_policy_service=risk_policy,
        minqmt_broker_factory=_portfolio_backend_factory(FakeQMTClient(connected=True)),
    ).check_day(
        portfolio_id=portfolio.portfolio_id,
        trade_date=TRADE_DATE,
        runtime_config={},
    )

    assert "runtime_profile_activation" in readiness.runtime_config_keys
    assert paper_repo.runtime_config_activations[activation.activation_id].profile_version_id == version.profile_version_id
    assert risk_policy.profile_seen is not None
    assert risk_policy.profile_seen.enabled is True
    selection_check = next(check for check in readiness.checks if check.check_name == "selection_runtime")
    assert selection_check.context["runtime_profile"]["hmm"]["enabled"] is False
    assert selection_check.context["runtime_profile"]["hmm"]["model_snapshot_id"] is None
    assert {check.check_name for check in readiness.checks} >= {"miniqmt_broker_authority", "miniqmt_execution_authority"}
    assert "minute_market_data" not in {check.check_name for check in readiness.checks}


def test_minqmt_day_runner_uses_platform_hmm_snapshot_and_versioned_execution_policy(tmp_path) -> None:
    model_path = tmp_path / "platform_hmm_model.json"
    model_path.write_text("{}", encoding="utf-8")
    (tmp_path / "coefficients_platform_preset_2024-01-01_2024-01-31.json").write_text(
        (
            '{"preset_key":"platform_preset",'
            '"daily_coefficients":{"2024-01-02":{"801780.SI":1.2}},'
            '"stock_sector_map":{"000002.SZ":"801780.SI","000003.SZ":"801780.SI"}}'
        ),
        encoding="utf-8",
    )
    custom_params = {
        "strategy_id": "score_weighted_topk_v2",
        "topk": 2,
        "enable_sector_hmm": True,
        "hmm_model_snapshot_id": "qe_hmm_snapshot_old",
        "hmm_signal_preset": "qe_preset",
        "risk_policy": {"enabled": True, "providers": ["st_pit"]},
    }
    package_repo, paper_repo, manifest, portfolio = _miniqmt_portfolio_fixture(custom_params=custom_params)
    policy_json = manifest.minute_execution_policy.model_dump(mode="json")
    policy_json["algo_code"] = "CLOSE_PRICE"
    policy_json["algo_config"] = {}
    policy = StrategyPackageService(repository=package_repo).create_execution_policy(
        package_id=manifest.package_id,
        policy_name="close price miniqmt activation",
        policy_json=policy_json,
        source_backtest_id="bt_close",
        source_backtest_status="COMPLETED",
        paper_enabled=True,
    )
    activation = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).activate_execution_policy(
        portfolio_id=portfolio.portfolio_id,
        trade_date=TRADE_DATE,
        policy_id=policy.policy_id,
        activated_by="unit_test",
        reason="MiniQMT uses versioned policy",
    )
    runtime_config = {
        "runtime_profile": {
            "hmm": {
                "enabled": True,
                "model_snapshot_id": "platform_hmm_snapshot_new",
                "signal_preset": "platform_preset",
            },
            "tradability": {"exclude_suspended": False},
        }
    }
    profile_service = PaperTradingV2PortfolioService(package_repository=package_repo, repository=paper_repo)
    _profile, version = profile_service.create_runtime_profile(
        portfolio_id=portfolio.portfolio_id,
        profile_name="miniqmt platform hmm profile",
        config_json=runtime_config,
        created_by="unit_test",
    )
    runtime_activation = profile_service.activate_runtime_config(
        portfolio_id=portfolio.portfolio_id,
        trade_date=TRADE_DATE,
        profile_version_id=version.profile_version_id,
        activated_by="unit_test",
        reason="MiniQMT HMM runtime profile",
    )
    runtime = _runtime_with_artifact(
        manifest,
        runtime_config=runtime_config,
        hmm_runtime=SectorHMMRuntime(
            snapshot_provider=FakeHMMSnapshotProvider(
                {
                    "platform_hmm_snapshot_new": {
                        "snapshot_id": "platform_hmm_snapshot_new",
                        "model_path": str(model_path),
                        "status": "completed",
                    }
                }
            )
        ),
    )
    client = FakeQMTClient()

    result = PaperTradingDayRunner(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        runtime=runtime,
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
        risk_policy_service=RecordingRiskPolicyService(),
        minqmt_broker_factory=_portfolio_backend_factory(client),
    ).run_day(
        portfolio_id=portfolio.portfolio_id,
        trade_date=TRADE_DATE,
    )

    context = result.run.runtime_config["validated_execution_policy"]
    assert result.run.status == RunStatus.SUCCEEDED
    assert result.run.runtime_config["runtime_profile_activation"]["activation_id"] == runtime_activation.activation_id
    assert context["activation_id"] == activation.activation_id
    assert context["activation_source"] == "trade_date_activation"
    assert context["algo_code"] == "CLOSE_PRICE"
    assert result.run.runtime_config["runtime_profile"]["hmm"]["model_snapshot_id"] == "platform_hmm_snapshot_new"
    assert result.run.runtime_config["qe_backtest_runtime_contract"]["runtime_features"]["hmm"]["package_bound"] is False
    assert client.place_calls, "MiniQMT should receive broker-authoritative order submit"
    assert result.orders[0].metadata["broker_backend"] == "minqmt_sim"
    assert result.fills == []
