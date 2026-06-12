from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from backend.services.miniqmt_execution_runtime import (
    MiniQMTChildOrderStatus,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntimeClient,
)
from backend.services.paper_trading_v2.broker import (
    BrokerAccountSnapshot,
    OrderHandle,
    OrderHandleStatus,
)
from backend.services.paper_trading_v2.day_runner import PaperTradingDayRunner
from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.models import PaperPortfolio, PaperRun
from backend.services.qmt_strategy_ledger.models import (
    PositionLotRecord,
    PositionLotStatus,
    VirtualAccount,
    VirtualAccountStatus,
    new_id,
)
from backend.services.qmt_strategy_ledger.lot_availability import StaticTradingCalendarProvider
from backend.services.qmt_strategy_ledger.order_service import QmtManagedOrderService
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.simulation_runtime import ExecutionPathNotCanonicalError, MiniQMTExecutionBridge, SimulationBrokerBackend
from backend.services.trading_core.models import OrderIntent, OrderSide, OrderType, RunStatus
from backend.tests.paper_trading_v2.test_day_runner import make_paper_enabled_manifest
from backend.tests.paper_trading_v2.test_minqmtsim_backend import _SnapshotOnlyRepository
from backend.tests.simulation_runtime.test_target_rebalance_shared import _compiled_plan_for_bridge


TRADE_DATE = date(2026, 6, 9)
RUNTIME_OWNER = "MiniQMTExecutionRuntime"


class RecordingPaperMiniQMTBroker:
    def __init__(self) -> None:
        self.submitted: list[OrderIntent] = []
        self._statuses: dict[str, OrderHandleStatus] = {}

    def submit_order_intent(self, intent: OrderIntent) -> OrderHandle:
        self.submitted.append(intent)
        handle = OrderHandle(
            handle_id=f"paper_handle_{len(self.submitted)}",
            backend_id="minqmt_sim",
            submitted_at=datetime(2026, 6, 9, 9, 31, tzinfo=UTC),
            intent_id=intent.intent_id,
        )
        self._statuses[handle.handle_id] = OrderHandleStatus(
            handle_id=handle.handle_id,
            state="pending",
            filled_quantity=0,
            avg_fill_price=None,
            last_event_at=handle.submitted_at,
            raw_status=50,
            status_msg="accepted by unit fake",
            raw={"order_status": 50, "status_msg": "accepted by unit fake"},
        )
        return handle

    def order_context(self, handle: OrderHandle) -> dict[str, str]:
        return {
            "handle_id": handle.handle_id,
            "intent_id": handle.intent_id,
            "miniqmt_order_id": f"native_{handle.intent_id}",
            "strategy_name": "slot_single",
            "order_remark": f"remark_{handle.intent_id}",
        }

    def query_status(self, handle: OrderHandle) -> OrderHandleStatus:
        return self._statuses[handle.handle_id]

    def query_trades(self, handle: OrderHandle) -> list[dict[str, Any]]:
        return []

    def query_account(self) -> BrokerAccountSnapshot:
        return BrokerAccountSnapshot(
            backend_id="minqmt_sim",
            cash=Decimal("100000"),
            nav=Decimal("100000"),
            margin_used=None,
            as_of=datetime(2026, 6, 9, 15, 0, tzinfo=UTC),
        )

    def query_position_marks(self):
        return {}, {}

    def shutdown(self) -> None:
        return None


class RecordingManagedOrderBroker:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def get_positions(self) -> list[dict[str, Any]]:
        return [{"stock_code": "000003.SZ", "can_sell": 1000}]

    def place_order(self, **kwargs: Any) -> tuple[int, str]:
        self.calls.append(dict(kwargs))
        return len(self.calls), "accepted by unit fake"


def _paper_portfolio(manifest: Any) -> PaperPortfolio:
    return PaperPortfolio(
        portfolio_id="paper_single_runtime",
        portfolio_name="single strategy runtime convergence",
        package_id=manifest.package_id,
        manifest_sha256=manifest.manifest_sha256,
        frozen_manifest=manifest,
        initial_cash=100_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        broker_backend="minqmt_sim",
    )


def _paper_run(portfolio: PaperPortfolio) -> PaperRun:
    return PaperRun(
        portfolio_id=portfolio.portfolio_id,
        trade_date=TRADE_DATE,
        status=RunStatus.RUNNING,
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        runtime_config={"paper_v2_session": {"session_id": "psess_single_runtime"}},
    )


def _paper_intent(portfolio: PaperPortfolio) -> OrderIntent:
    return OrderIntent(
        package_id=portfolio.package_id,
        portfolio_id=portfolio.portfolio_id,
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=200,
        order_type=OrderType.MARKET,
        limit_price=None,
        target_trade_date=TRADE_DATE,
    )


def _managed_order_service(binding_strategy_id: str, binding_strategy_name: str, account_id: str) -> tuple[QmtManagedOrderService, RecordingManagedOrderBroker]:
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=binding_strategy_id,
            strategy_name=binding_strategy_name,
            display_name="Shared MiniQMT Runtime Strategy",
            account_id=account_id,
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("1000000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id=new_id("lot"),
            strategy_id=binding_strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_open_000003",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=account_id,
            status=PositionLotStatus.OPEN,
        )
    )
    broker = RecordingManagedOrderBroker()
    service = QmtManagedOrderService(
        repository=qmt_repo,
        broker=broker,
        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), date(2026, 5, 21)]),
    )
    return service, broker


def test_paper_v2_n1_and_simulation_runtime_n_many_share_runtime_owner_evidence() -> None:
    runtime_client = MiniQMTExecutionRuntimeClient()
    manifest = make_paper_enabled_manifest()
    portfolio = _paper_portfolio(manifest)
    paper_repo = _SnapshotOnlyRepository(portfolio)
    paper_broker = RecordingPaperMiniQMTBroker()

    paper_result = PaperTradingDayRunner(repository=paper_repo, minqmt_runtime_client=runtime_client)._run_minqmt_sim_orders(
        portfolio=portfolio,
        run=_paper_run(portfolio),
        manifest=manifest,
        trade_date=TRADE_DATE,
        intents=[_paper_intent(portfolio)],
        broker=paper_broker,  # type: ignore[arg-type]
    )

    _release, binding, plan = _compiled_plan_for_bridge(
        backend=SimulationBrokerBackend.MINIQMT_SIM,
        execution_policy_payload={"algo_code": "CLOSE_PRICE", "schedule_window": {"mode": "open_to_close"}},
        execution_policy_version_id="exec_policy_close_price",
        execution_policy_sha256="exec_policy_hash_close_price",
    )
    service, managed_broker = _managed_order_service(
        binding_strategy_id=binding.strategy_id,
        binding_strategy_name=binding.strategy_name or binding.strategy_id,
        account_id=binding.broker_account_id or "QMT_SIM_ACCOUNT",
    )
    simulation_result = MiniQMTExecutionBridge(
        managed_order_service=service,
        runtime_client=runtime_client,
    ).submit_plan(
        plan=plan,
        binding=binding,
        price_by_symbol={"000003.SZ": Decimal("8.00"), "688001.SH": Decimal("20.00")},
    )

    paper_evidence = paper_repo.orders[0].metadata["runtime_evidence"]
    simulation_evidence = simulation_result.runtime_evidence.to_dict()

    assert paper_result.run.status == RunStatus.SUCCEEDED
    assert paper_repo.orders[0].metadata["runtime_owner"] == RUNTIME_OWNER
    assert paper_evidence["runtime_owner"] == RUNTIME_OWNER
    assert paper_evidence["source"] == "paper_v2_direct_miniqmt"
    assert paper_evidence["submitted_child_count"] == 1
    assert paper_evidence["child_order_ids"]
    assert paper_repo.execution_states[0].algo_state["runtime_owner"] == RUNTIME_OWNER
    assert paper_broker.submitted[0].metadata["runtime_owner"] == RUNTIME_OWNER

    assert simulation_result.success is True
    assert simulation_evidence["runtime_owner"] == RUNTIME_OWNER
    assert simulation_evidence["source"] == "simulation_runtime_submit"
    assert simulation_evidence["submitted_child_count"] == 2
    assert len(simulation_evidence["algo_instance_ids"]) == 2
    assert len(simulation_evidence["child_order_ids"]) == 2
    assert len(managed_broker.calls) == 2
    simulation_events = runtime_client.repository.list_events(simulation_evidence["runtime_id"])
    managed_sync_events = [
        event for event in simulation_events if event.payload.get("managed_gateway_sync") is True
    ]
    assert {event.event_type for event in managed_sync_events} == {MiniQMTExecutionEventType.ORDER_EVENT}
    assert all(event.source == "gateway" and event.payload["broker_called"] is True for event in managed_sync_events)
    assert len(managed_sync_events) == len(managed_broker.calls)
    simulation_children = runtime_client.repository.list_child_orders(
        simulation_evidence["runtime_id"], active_only=False
    )
    assert {child.status for child in simulation_children} == {MiniQMTChildOrderStatus.SUBMITTED}
    assert all(child.broker_order_id for child in simulation_children)


def test_product_miniqmt_paths_delegate_to_runtime_client_not_raw_broker_calls() -> None:
    root = Path(__file__).resolve().parents[3]
    product_files = [
        root / "backend/services/paper_trading_v2/day_runner.py",
        root / "backend/services/simulation_runtime/bridges.py",
        root / "backend/services/simulation_runtime/lifecycle.py",
    ]
    forbidden_by_file = {
        "day_runner.py": ("broker.submit_order_intent(", "XtQuantQMTClient(", ".place_order(", "MiniQMTLiveAlgoAdapter"),
        "bridges.py": (
            "self._managed_order_service.submit_batch(",
            "XtQuantQMTClient(",
            ".place_order(",
            "UnifiedMiniQMTVnpyExecutionAdapter",
            "QmtManagedOrderSubmitter",
            "MiniQMTChildOrderRequest",
        ),
        "lifecycle.py": ("QmtManagedOrderService.submit_batch(", "XtQuantQMTClient(", ".place_order(", "MiniQMTLiveAlgoAdapter"),
    }

    for path in product_files:
        text = path.read_text(encoding="utf-8")
        for forbidden in forbidden_by_file[path.name]:
            assert forbidden not in text, f"{path.name} still owns MiniQMT broker submit path via {forbidden}"
        assert "MiniQMTExecutionRuntimeClient" in text or "MiniQMTExecutionBridge" in text

    runtime_client = (root / "backend/services/miniqmt_execution_runtime/client.py").read_text(encoding="utf-8")
    direct_submit_batch_hits = [
        (index, line.strip())
        for index, line in enumerate(runtime_client.splitlines(), start=1)
        if ".submit_batch(" in line
    ]
    assert [line for _, line in direct_submit_batch_hits] == [
        "return order_service.submit_batch(list(self.requests))"
    ], direct_submit_batch_hits
    assert "gateway.submit_managed_batch(" in runtime_client
    assert "managed_order_service.submit_batch(" not in runtime_client
    assert "self.broker.submit_order_intent(child_intent)" in runtime_client


def test_legacy_paper_v2_miniqmt_live_adapter_is_removed() -> None:
    root = Path(__file__).resolve().parents[3]

    execution_init = (root / "backend/services/paper_trading_v2/execution/__init__.py").read_text(encoding="utf-8")
    assert "MiniQMTLiveAlgoAdapter" not in execution_init
    legacy_file = (root / "backend/services/paper_trading_v2/execution/minqmt_live_algo_adapter.py").read_text(encoding="utf-8")
    assert "ExecutionPathNotCanonicalError" in legacy_file

    from backend.services.paper_trading_v2.execution.minqmt_live_algo_adapter import MiniQMTLiveAlgoAdapter

    with pytest.raises(ExecutionPathNotCanonicalError) as exc_info:
        MiniQMTLiveAlgoAdapter(broker=object(), policy_context={})

    assert exc_info.value.error_code == "EXECUTION_PATH_NOT_CANONICAL"
    assert exc_info.value.context["required_runtime_owner"] == "MiniQMTExecutionRuntime"
