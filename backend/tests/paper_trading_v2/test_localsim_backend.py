"""Tests for ``paper_trading_v2.broker.LocalSimBackend`` (Task #20).

Covers Engine §3.6.1 (R-Q9 D1) BrokerBackend protocol:
  - 6 Protocol methods happy path
  - typed error paths: BrokerSubmitError / BrokerRejectedError /
    BrokerConnectivityError
  - multi-portfolio isolation (R-Q9 D2 — N parallel LocalSim instances do not
    share state)
  - market-source cross-pairing rejected at backend init
    (BrokerMarketSourceMismatchError, R-Q9 D3)
  - subscribe_fill_callback delivers + unsubscribe releases
  - historical synchronous semantics and realtime durable minute continuation
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from backend.services.paper_trading_v2.broker import (
    BrokerAccountSnapshot,
    BrokerBindCapacity,
    CancelAck,
    FillEvent,
    LocalSimBackend,
    MarketDataChannel,
    OrderHandle,
    OrderHandleStatus,
    SubscriptionHandle,
)
from backend.services.paper_trading_v2.market_data import (
    MinuteDataSource,
    MinuteExecutionMarketInput,
)
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.trading_core.errors import (
    BrokerConnectivityError,
    BrokerMarketSourceMismatchError,
    BrokerRejectedError,
    BrokerSubmitError,
    DataUnavailableError,
    RiskRuleError,
    RuntimeConfigInvalidError,
)
from backend.services.trading_core.ledger import FeeModel, InMemoryLedger
from backend.services.trading_core.models import (
    Fill,
    MinuteBar,
    OrderEvent,
    OrderEventType,
    OrderIntent,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionLot,
)

from backend.tests.paper_trading_v2.test_day_runner import make_paper_enabled_manifest


TRADE_DATE = date(2024, 1, 2)


def _make_market_input(
    symbol: str,
    *,
    bar_count: int = 3,
    open_price: float = 10.0,
) -> MinuteExecutionMarketInput:
    start = datetime.combine(TRADE_DATE, datetime.min.time()).replace(hour=9, minute=31)
    minute_bars = [
        MinuteBar(
            symbol=symbol,
            bar_time=start + timedelta(minutes=i),
            open=open_price + i * 0.1,
            high=open_price + 0.2 + i * 0.1,
            low=open_price - 0.1 + i * 0.1,
            close=open_price + 0.1 + i * 0.1,
            volume=100_000,
            amount=1_000_000.0,
            limit_up=open_price + 1.0,
            limit_down=open_price - 1.0,
        )
        for i in range(bar_count)
    ]
    return MinuteExecutionMarketInput(
        symbol=symbol,
        trade_date=TRADE_DATE,
        source=MinuteDataSource.DB_HISTORICAL,
        minute_bars=minute_bars,
        market_context={
            "stock_id": symbol,
            "trade_date": TRADE_DATE.isoformat(),
            "data_source": MinuteDataSource.DB_HISTORICAL.value,
            "prev_close": open_price,
            "limit_up": open_price + 1.0,
            "limit_down": open_price - 1.0,
            "suspend_status": {"is_suspended": False},
        },
    )


class FakeMarketDataProvider:
    """Returns canned MinuteExecutionMarketInput; raises on configured symbols."""

    def __init__(
        self,
        *,
        inputs_by_symbol: dict[str, MinuteExecutionMarketInput] | None = None,
        unavailable_symbols: set[str] | None = None,
    ) -> None:
        self.inputs_by_symbol = inputs_by_symbol or {}
        self.unavailable_symbols = unavailable_symbols or set()
        self.calls: list[dict] = []

    def load_symbol_input(
        self,
        *,
        symbol: str,
        trade_date,
        source: MinuteDataSource,
        min_bars: int,
        require_suspend_status: bool = False,
        require_day_features: bool = False,
    ) -> MinuteExecutionMarketInput:
        self.calls.append(
            {
                "symbol": symbol,
                "trade_date": trade_date,
                "source": source,
                "min_bars": min_bars,
                "require_day_features": require_day_features,
            }
        )
        if symbol in self.unavailable_symbols:
            raise DataUnavailableError(
                "fake provider configured to fail",
                context={"symbol": symbol, "trade_date": trade_date.isoformat()},
            )
        if symbol in self.inputs_by_symbol:
            return self.inputs_by_symbol[symbol]
        return _make_market_input(symbol)


def _build_backend(
    *,
    portfolio_id: str = "paper_local_p1",
    initial_cash: float = 1_000_000.0,
    data_source: MinuteDataSource = MinuteDataSource.DB_HISTORICAL,
    provider: FakeMarketDataProvider | None = None,
    execution_engine=None,
    initial_positions: dict[str, PositionLot] | None = None,
    execution_policy: dict | None = None,
    initial_available_cash: float | None = None,
) -> tuple[LocalSimBackend, FakeMarketDataProvider, StrategyPackageManifest]:
    manifest = make_paper_enabled_manifest()
    market_data_provider = provider or FakeMarketDataProvider()
    backend = LocalSimBackend(
        portfolio_id=portfolio_id,
        initial_cash=initial_cash,
        data_source=data_source,
        manifest=manifest,
        market_data_provider=market_data_provider,
        execution_engine=execution_engine,
        initial_positions=initial_positions,
        execution_policy=execution_policy,
        initial_available_cash=initial_available_cash,
    )
    return backend, market_data_provider, manifest


def _buy_intent(
    backend: LocalSimBackend,
    *,
    symbol: str = "000001.SZ",
    quantity: int = 100,
) -> OrderIntent:
    return OrderIntent(
        package_id=backend.package_id,
        portfolio_id=backend.portfolio_id,
        symbol=symbol,
        side=OrderSide.BUY,
        quantity=quantity,
        order_type=OrderType.MARKET,
        target_trade_date=TRADE_DATE,
    )


def _ledger_fill(
    *,
    order_id: str = "ord_ledger_1",
    fill_id: str = "fill_ledger_1",
    symbol: str = "000001.SZ",
    side: OrderSide = OrderSide.BUY,
    quantity: int = 100,
    price: float = 10.0,
    trade_time: datetime | None = None,
) -> Fill:
    return Fill(
        fill_id=fill_id,
        order_id=order_id,
        symbol=symbol,
        side=side,
        quantity=quantity,
        price=price,
        trade_time=trade_time
        or datetime.combine(TRADE_DATE, datetime.min.time()).replace(hour=9, minute=31),
        reason="unit ledger fill",
    )


def _unchecked_ledger_fill(
    *,
    order_id: str = "ord_unchecked",
    fill_id: str = "fill_unchecked",
    symbol: str = "000001.SZ",
    side: OrderSide = OrderSide.BUY,
    quantity: int = 150,
    price: float = 10.0,
    trade_time: datetime | None = None,
) -> Fill:
    return Fill.model_construct(
        fill_id=fill_id,
        order_id=order_id,
        symbol=symbol,
        side=side,
        quantity=quantity,
        price=price,
        trade_time=trade_time
        or datetime.combine(TRADE_DATE, datetime.min.time()).replace(hour=9, minute=31),
        bar_time=None,
        reason="unchecked unit ledger fill",
        metadata={},
    )


class FullFillExecutionEngine:
    def execute_order(self, *, order, minute_bars, algo_code, algo_config, market_context, allow_partial_fill):
        bar = minute_bars[-1]
        fill = Fill(
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            price=bar.close,
            trade_time=bar.bar_time,
            bar_time=bar.bar_time,
            reason="unit full fill",
            metadata={"algo_code": "UNIT_FULL_FILL"},
        )
        final_order = order.model_copy(
            update={
                "status": OrderStatus.FILLED,
                "filled_quantity": order.quantity,
                "avg_fill_price": fill.price,
                "updated_at": fill.trade_time,
            }
        )
        event = OrderEvent(
            order_id=order.order_id,
            event_type=OrderEventType.FILLED,
            fill=fill,
            reason=fill.reason,
        )
        return final_order, [fill], [event]


class TwoFillExecutionEngine:
    def execute_order(self, *, order, minute_bars, algo_code, algo_config, market_context, allow_partial_fill):
        fills = [
            Fill(
                order_id=order.order_id,
                symbol=order.symbol,
                side=order.side,
                quantity=100,
                price=10.0,
                trade_time=minute_bars[index].bar_time,
                bar_time=minute_bars[index].bar_time,
                reason="unit split fill",
            )
            for index in range(2)
        ]
        final_order = order.model_copy(
            update={
                "status": OrderStatus.FILLED,
                "filled_quantity": 200,
                "avg_fill_price": 10.0,
                "updated_at": fills[-1].trade_time,
            }
        )
        return final_order, fills, []


class ObservedMarketDataProvider(FakeMarketDataProvider):
    def load_observed_intraday(
        self,
        *,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource,
        until_time: datetime,
        require_day_features: bool = False,
    ) -> MinuteExecutionMarketInput:
        source_input = self.inputs_by_symbol[symbol]
        return MinuteExecutionMarketInput(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            minute_bars=[bar for bar in source_input.minute_bars if bar.bar_time <= until_time],
            market_context={**source_input.market_context, "data_source": source.value},
        )


def test_localsim_realtime_submission_uses_only_bars_after_plan_cursor() -> None:
    historical = _make_market_input("000001.SZ", bar_count=3)
    provider = ObservedMarketDataProvider(inputs_by_symbol={"000001.SZ": historical})
    as_of = datetime.combine(TRADE_DATE, datetime.min.time()).replace(hour=9, minute=33)
    cursor = as_of - timedelta(minutes=2)
    backend, _, _ = _build_backend(
        data_source=MinuteDataSource.TDX_REALTIME,
        provider=provider,
        execution_policy={
            "validated_execution_policy_id": "exec_policy_twap_unit",
            "policy_sha256": "sha_twap_unit",
            "policy_json": {"algo_code": "TWAP", "algo_config": {"split_count": 1}},
        },
    )
    backend.configure_execution_runtime(run_id="run_causal_unit", binding_id="binding_causal_unit")
    plan = SimpleNamespace(
        plan_id="plan_causal_unit",
        target_trade_date=TRADE_DATE,
        plan_payload_json={
            "local_sim_execution_causality": {
                "schema_version": "local_sim_execution_causality_v1",
                "eligible_bar_after": cursor.isoformat(),
            }
        },
    )

    backend.bind_execution_plan(plan=plan, as_of_time=as_of)
    handle = backend.submit_order_intent(_buy_intent(backend))
    snapshot = backend.export_execution_snapshot(handles=[handle])

    assert len(snapshot["fills"]) == 1
    assert snapshot["fills"][0].bar_time > cursor
    assert len(snapshot["execution_states"]) == 1
    assert snapshot["execution_states"][0].last_processed_bar_time == as_of
    assert snapshot["execution_states"][0].runtime_status.value == "FILLED"


def test_localsim_realtime_submission_persists_waiting_state_before_first_causal_bar() -> None:
    historical = _make_market_input("000001.SZ", bar_count=1)
    provider = ObservedMarketDataProvider(inputs_by_symbol={"000001.SZ": historical})
    as_of = datetime.combine(TRADE_DATE, datetime.min.time()).replace(hour=9, minute=31)
    backend, _, _ = _build_backend(data_source=MinuteDataSource.TDX_REALTIME, provider=provider)
    backend.configure_execution_runtime(run_id="run_waiting_unit", binding_id="binding_waiting_unit")
    backend.bind_execution_plan(
        plan=SimpleNamespace(
            plan_id="plan_waiting_unit",
            target_trade_date=TRADE_DATE,
            plan_payload_json={"local_sim_execution_causality": {
                "schema_version": "local_sim_execution_causality_v1", "eligible_bar_after": as_of.isoformat()
            }},
        ),
        as_of_time=as_of,
    )
    handle = backend.submit_order_intent(_buy_intent(backend))
    snapshot = backend.export_execution_snapshot(handles=[handle])
    assert backend.query_status(handle).state == "pending"
    assert snapshot["fills"] == ()
    state = snapshot["execution_states"][0]
    assert state.runtime_status.value == "WAITING_FOR_CAUSAL_BAR"
    assert state.sequence == 0
    assert state.last_processed_bar_time is None


def test_localsim_streaming_schedule_restarts_from_durable_cursor_without_duplicate_fill() -> None:
    historical = _make_market_input("000001.SZ", bar_count=4)
    provider = ObservedMarketDataProvider(inputs_by_symbol={"000001.SZ": historical})
    cursor = datetime.combine(TRADE_DATE, datetime.min.time()).replace(hour=9, minute=30)
    streaming_policy = {
        "validated_execution_policy_id": "exec_policy_twap_streaming",
        "policy_sha256": "sha_twap_streaming",
        "policy_json": {"algo_code": "TWAP", "algo_config": {"split_count": 4, "allow_partial_fill": True}},
    }
    first_as_of = cursor + timedelta(minutes=2)
    first, _, _ = _build_backend(
        initial_cash=10_000_000, data_source=MinuteDataSource.TDX_REALTIME,
        provider=provider, execution_policy=streaming_policy,
    )
    first.configure_execution_runtime(run_id="run_stream_restart", binding_id="binding_stream_restart")
    plan = SimpleNamespace(
        plan_id="plan_stream_restart", target_trade_date=TRADE_DATE,
        plan_payload_json={"local_sim_execution_causality": {
            "schema_version": "local_sim_execution_causality_v1", "eligible_bar_after": cursor.isoformat()
        }},
    )
    first.bind_execution_plan(plan=plan, as_of_time=first_as_of)
    first_handle = first.submit_order_intent(_buy_intent(first, quantity=10_000))
    first_snapshot = first.export_execution_snapshot(handles=[first_handle])
    first_state = first_snapshot["execution_states"][0]
    first_order = first_snapshot["orders"][0]
    first_fill_ids = {fill.fill_id for fill in first_snapshot["fills"]}
    assert first_state.runtime_status.value == "ACTIVE"
    assert first_state.remaining_quantity > 0
    assert first_state.last_processed_bar_time == first_as_of

    conflicting_bars = list(historical.minute_bars)
    conflicting_bars[1] = conflicting_bars[1].model_copy(update={"close": conflicting_bars[1].close + 0.5})
    conflict_backend, _, _ = _build_backend(
        initial_cash=10_000_000, initial_available_cash=float(first.query_account().cash),
        initial_positions=first.query_positions(), data_source=MinuteDataSource.TDX_REALTIME,
        provider=ObservedMarketDataProvider(
            inputs_by_symbol={"000001.SZ": replace(historical, minute_bars=conflicting_bars)}
        ), execution_policy=streaming_policy,
    )
    conflict_backend.configure_execution_runtime(run_id="run_stream_restart", binding_id="binding_stream_restart")
    conflict_backend.bind_execution_plan(plan=plan, as_of_time=cursor + timedelta(minutes=3))
    conflict_backend.restore_execution_state(order=first_order, state=first_state)
    with pytest.raises(BrokerConnectivityError) as conflict:
        conflict_backend.advance_realtime_execution(as_of_time=cursor + timedelta(minutes=3))
    assert conflict.value.context["reason_code"] == "LOCALSIM_LAST_APPLIED_BAR_PAYLOAD_CONFLICT"

    second_as_of = cursor + timedelta(minutes=4)
    second, _, _ = _build_backend(
        initial_cash=10_000_000, initial_available_cash=float(first.query_account().cash),
        initial_positions=first.query_positions(), data_source=MinuteDataSource.TDX_REALTIME,
        provider=provider, execution_policy=streaming_policy,
    )
    second.configure_execution_runtime(run_id="run_stream_restart", binding_id="binding_stream_restart")
    second.bind_execution_plan(plan=plan, as_of_time=second_as_of)
    restored_handle = second.restore_execution_state(order=first_order, state=first_state)
    second.advance_realtime_execution(as_of_time=second_as_of)
    second_snapshot = second.export_execution_snapshot(handles=[restored_handle])
    second_state = second_snapshot["execution_states"][0]
    second_fill_ids = {fill.fill_id for fill in second_snapshot["fills"]}
    assert second_state.last_processed_bar_time == second_as_of
    assert second_state.sequence == first_state.sequence + 1
    assert second_state.filled_quantity > first_state.filled_quantity
    assert first_fill_ids.isdisjoint(second_fill_ids)
    assert all(fill.bar_time > first_state.last_processed_bar_time for fill in second_snapshot["fills"])
    second.advance_realtime_execution(as_of_time=second_as_of)
    replay_snapshot = second.export_execution_snapshot(handles=[restored_handle])
    assert replay_snapshot["fills"] == ()
    assert replay_snapshot["execution_states"][0] == second_state


def test_localsim_close_terminalizes_remaining_schedule_with_explicit_residual() -> None:
    source_input = _make_market_input("000001.SZ", bar_count=1)
    closing_bar = source_input.minute_bars[0].model_copy(
        update={"bar_time": datetime.combine(TRADE_DATE, datetime.min.time()).replace(hour=15)}
    )
    provider = ObservedMarketDataProvider(
        inputs_by_symbol={"000001.SZ": replace(source_input, minute_bars=[closing_bar])}
    )
    as_of = closing_bar.bar_time + timedelta(minutes=1)
    backend, _, _ = _build_backend(
        initial_cash=10_000_000, data_source=MinuteDataSource.TDX_REALTIME, provider=provider,
        execution_policy={
            "validated_execution_policy_id": "exec_policy_twap_close_residual",
            "policy_sha256": "sha_twap_close_residual",
            "policy_json": {"algo_code": "TWAP", "algo_config": {"split_count": 10}},
        },
    )
    backend.configure_execution_runtime(run_id="run_close_residual", binding_id="binding_close_residual")
    backend.bind_execution_plan(
        plan=SimpleNamespace(
            plan_id="plan_close_residual", target_trade_date=TRADE_DATE,
            plan_payload_json={"local_sim_execution_causality": {
                "schema_version": "local_sim_execution_causality_v1",
                "eligible_bar_after": closing_bar.bar_time.replace(hour=14, minute=59).isoformat(),
            }},
        ), as_of_time=as_of,
    )
    handle = backend.submit_order_intent(_buy_intent(backend, quantity=10_000))
    state = backend.export_execution_snapshot(handles=[handle])["execution_states"][0]
    assert state.runtime_status.value == "EXPIRED_WITH_RESIDUAL"
    assert state.remaining_quantity > 0
    assert state.terminal_reason == "MARKET_SESSION_CLOSED_WITH_REMAINING_QUANTITY"
    assert state.residual_classification == "SCHEDULE_RESIDUAL_AT_CLOSE"


def test_localsim_post_close_fails_loud_when_closing_bar_is_missing() -> None:
    provider = ObservedMarketDataProvider(
        inputs_by_symbol={"000001.SZ": _make_market_input("000001.SZ", bar_count=2)}
    )
    as_of = datetime.combine(TRADE_DATE, datetime.min.time()).replace(hour=15, minute=1)
    backend, _, _ = _build_backend(
        initial_cash=10_000_000, data_source=MinuteDataSource.TDX_REALTIME, provider=provider,
        execution_policy={
            "validated_execution_policy_id": "exec_policy_twap_missing_close",
            "policy_sha256": "sha_twap_missing_close",
            "policy_json": {"algo_code": "TWAP", "algo_config": {"split_count": 10}},
        },
    )
    backend.configure_execution_runtime(run_id="run_missing_close", binding_id="binding_missing_close")
    backend.bind_execution_plan(
        plan=SimpleNamespace(
            plan_id="plan_missing_close", target_trade_date=TRADE_DATE,
            plan_payload_json={"local_sim_execution_causality": {
                "schema_version": "local_sim_execution_causality_v1",
                "eligible_bar_after": datetime.combine(TRADE_DATE, datetime.min.time()).replace(
                    hour=9, minute=30
                ).isoformat(),
            }},
        ), as_of_time=as_of,
    )
    with pytest.raises(BrokerConnectivityError) as exc_info:
        backend.submit_order_intent(_buy_intent(backend, quantity=10_000))
    assert exc_info.value.context["reason_code"] == "LOCALSIM_CLOSE_BAR_MISSING"


def test_localsim_plan_batch_rolls_back_all_orders_and_callbacks_on_later_failure() -> None:
    provider = FakeMarketDataProvider(unavailable_symbols={"000002.SZ"})
    backend, _, _ = _build_backend(provider=provider, execution_engine=FullFillExecutionEngine())
    callback_events: list[FillEvent] = []
    backend.subscribe_fill_callback(callback_events.append)
    cash_before = backend.query_account().cash

    backend.begin_plan_submission(plan_id="plan_atomic_unit")
    backend.submit_order_intent(_buy_intent(backend, symbol="000001.SZ"))
    with pytest.raises(BrokerConnectivityError):
        backend.submit_order_intent(_buy_intent(backend, symbol="000002.SZ"))
    backend.rollback_plan_submission(plan_id="plan_atomic_unit")

    assert backend.query_account().cash == cash_before
    assert backend.export_execution_snapshot()["orders"] == ()
    assert backend.export_execution_snapshot()["fills"] == ()
    assert callback_events == []


def test_localsim_single_order_rolls_back_earlier_fill_when_later_fill_fails() -> None:
    backend, _, _ = _build_backend(initial_cash=1_500, execution_engine=TwoFillExecutionEngine())

    with pytest.raises(BrokerRejectedError):
        backend.submit_order_intent(_buy_intent(backend, quantity=200))

    snapshot = backend.export_execution_snapshot()
    assert backend.query_account().cash == Decimal("1500.0")
    assert snapshot["fills"] == ()
    assert snapshot["cash_entries"] == ()
    assert snapshot["positions"] == {}


# ---------------------------------------------------------------------------
# 1. Constructor + market-source binding
# ---------------------------------------------------------------------------


def test_localsim_init_rejects_miniqmt_realtime_source() -> None:
    manifest = make_paper_enabled_manifest()
    with pytest.raises(BrokerMarketSourceMismatchError) as exc_info:
        LocalSimBackend(
            portfolio_id="p_bad",
            initial_cash=100_000,
            data_source=MinuteDataSource.MINIQMT_REALTIME,
            manifest=manifest,
            market_data_provider=FakeMarketDataProvider(),
        )
    assert exc_info.value.context["broker_id"] == "local_sim"
    assert exc_info.value.context["given_source"] == "MINIQMT_REALTIME"


def test_localsim_init_accepts_tdx_and_db() -> None:
    manifest = make_paper_enabled_manifest()
    for source in (MinuteDataSource.TDX_REALTIME, MinuteDataSource.DB_HISTORICAL):
        LocalSimBackend(
            portfolio_id=f"p_{source.value.lower()}",
            initial_cash=100_000,
            data_source=source,
            manifest=manifest,
            market_data_provider=FakeMarketDataProvider(),
        )


# ---------------------------------------------------------------------------
# 2. submit_order_intent — happy path + synchronous semantics
# ---------------------------------------------------------------------------


def test_submit_order_intent_returns_terminal_status_synchronously() -> None:
    backend, _, _ = _build_backend()
    received: list[FillEvent] = []
    sub = backend.subscribe_fill_callback(received.append)
    handle = backend.submit_order_intent(_buy_intent(backend))
    # Synchronous semantics (Lead decision 4): callback already fired.
    assert received, "fill_callback should fire before submit returns"
    assert all(isinstance(e, FillEvent) for e in received)
    assert all(e.handle_id == handle.handle_id for e in received)
    assert all(e.venue == "local_sim" for e in received)

    status = backend.query_status(handle)
    assert status.state == "filled"
    assert status.filled_quantity == 100
    assert status.avg_fill_price is not None
    assert status.rejection_reason is None
    backend.unsubscribe_fill_callback(sub)


def test_submit_order_intent_allows_star_whole_position_odd_lot_sell() -> None:
    initial_lot = PositionLot(
        portfolio_id="paper_star_exit",
        symbol="688720.SH",
        quantity=1547,
        available_quantity=1547,
        avg_cost=10.0,
        trade_date=TRADE_DATE - timedelta(days=1),
    )
    backend, _, _ = _build_backend(
        portfolio_id="paper_star_exit",
        initial_cash=100_000.0,
        execution_engine=FullFillExecutionEngine(),
        initial_positions={"688720.SH": initial_lot},
    )
    intent = OrderIntent(
        package_id=backend.package_id,
        portfolio_id=backend.portfolio_id,
        symbol="688720.SH",
        side=OrderSide.SELL,
        quantity=1547,
        order_type=OrderType.MARKET,
        target_trade_date=TRADE_DATE,
    )

    handle = backend.submit_order_intent(intent)

    status = backend.query_status(handle)
    assert status.state == "filled"
    assert status.filled_quantity == 1547
    assert "688720.SH" not in backend.query_positions()
    snapshot = backend.export_execution_snapshot(handles=[handle])
    assert [fill.quantity for fill in snapshot["fills"]] == [1547]


def test_localsim_uses_portfolio_validated_execution_policy_snapshot() -> None:
    manifest = make_paper_enabled_manifest().model_copy(update={"minute_execution_policy": None})
    provider = FakeMarketDataProvider()
    backend = LocalSimBackend(
        portfolio_id="paper_local_validated_policy",
        initial_cash=100_000,
        data_source=MinuteDataSource.DB_HISTORICAL,
        manifest=manifest,
        market_data_provider=provider,
        execution_policy={
            "validated_execution_policy_id": "exec_policy_close",
            "policy_sha256": "policy_sha256",
            "policy_json": {
                "algo_code": "CLOSE_PRICE",
                "algo_config": {"allow_partial_fill": True},
            },
        },
    )

    handle = backend.submit_order_intent(_buy_intent(backend, quantity=100))

    assert backend.query_status(handle).state == "filled"
    assert provider.calls[-1]["require_day_features"] is False


def test_localsim_fails_fast_without_execution_policy_snapshot() -> None:
    manifest = make_paper_enabled_manifest().model_copy(update={"minute_execution_policy": None})

    with pytest.raises(RuntimeConfigInvalidError, match="validated execution policy snapshot"):
        LocalSimBackend(
            portfolio_id="paper_local_missing_policy",
            initial_cash=100_000,
            data_source=MinuteDataSource.DB_HISTORICAL,
            manifest=manifest,
            market_data_provider=FakeMarketDataProvider(),
        )


def test_localsim_empty_portfolio_policy_falls_back_to_legacy_manifest_policy() -> None:
    backend, provider, _ = _build_backend()

    assert backend.submit_order_intent(_buy_intent(backend))
    assert provider.calls[-1]["require_day_features"] is False


def test_submit_order_intent_rejects_cross_portfolio_intent() -> None:
    backend, _, _ = _build_backend(portfolio_id="paper_local_p1")
    bad = OrderIntent(
        package_id=backend.package_id,
        portfolio_id="paper_local_other",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=100,
        target_trade_date=TRADE_DATE,
    )
    with pytest.raises(BrokerSubmitError) as exc_info:
        backend.submit_order_intent(bad)
    assert exc_info.value.context["intent_portfolio_id"] == "paper_local_other"
    assert exc_info.value.context["backend_portfolio_id"] == "paper_local_p1"


def test_submit_order_intent_rejects_cross_package_intent() -> None:
    backend, _, _ = _build_backend()
    bad = OrderIntent(
        package_id="some_other_package",
        portfolio_id=backend.portfolio_id,
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=100,
        target_trade_date=TRADE_DATE,
    )
    with pytest.raises(BrokerSubmitError):
        backend.submit_order_intent(bad)


def test_submit_order_intent_rejects_duplicate_intent_id() -> None:
    backend, _, _ = _build_backend()
    intent = _buy_intent(backend)
    backend.submit_order_intent(intent)
    with pytest.raises(BrokerSubmitError) as exc_info:
        backend.submit_order_intent(intent)
    assert exc_info.value.context["intent_id"] == intent.intent_id


# ---------------------------------------------------------------------------
# 3. typed error: BrokerConnectivityError on data-layer outage
# ---------------------------------------------------------------------------


def test_submit_raises_broker_connectivity_when_market_data_unavailable() -> None:
    provider = FakeMarketDataProvider(unavailable_symbols={"600000.SH"})
    backend, _, _ = _build_backend(provider=provider)
    intent = _buy_intent(backend, symbol="600000.SH")
    with pytest.raises(BrokerConnectivityError) as exc_info:
        backend.submit_order_intent(intent)
    assert exc_info.value.context["symbol"] == "600000.SH"
    assert exc_info.value.context["source"] == "DB_HISTORICAL"


def test_submit_raises_broker_connectivity_after_shutdown() -> None:
    backend, _, _ = _build_backend()
    backend.shutdown()
    with pytest.raises(BrokerConnectivityError):
        backend.submit_order_intent(_buy_intent(backend))


# ---------------------------------------------------------------------------
# 4. typed error: BrokerRejectedError when ledger refuses (insufficient cash)
# ---------------------------------------------------------------------------


def test_submit_raises_broker_rejected_when_insufficient_cash() -> None:
    # Fill price ~10.1 -> 10000 shares costs ~101k; 50k cash insufficient.
    backend, _, _ = _build_backend(initial_cash=50_000.0)
    intent = _buy_intent(backend, quantity=10_000)
    with pytest.raises(BrokerRejectedError) as exc_info:
        backend.submit_order_intent(intent)
    assert exc_info.value.context["intent_id"] == intent.intent_id
    # The rejected handle is recorded so query_status returns the rejection
    handle_id = exc_info.value.context["handle_id"]
    rejected_handle = OrderHandle(
        handle_id=handle_id,
        backend_id="local_sim",
        submitted_at=datetime.now(),
        intent_id=intent.intent_id,
    )
    status = backend.query_status(rejected_handle)
    assert status.state == "rejected"
    assert status.filled_quantity == 0
    assert status.rejection_reason


# ---------------------------------------------------------------------------
# 5. cancel — terminal-state path is no-op accepted=False
# ---------------------------------------------------------------------------


def test_cancel_returns_unaccepted_for_filled_order_synchronous() -> None:
    backend, _, _ = _build_backend()
    handle = backend.submit_order_intent(_buy_intent(backend))
    ack = backend.cancel(handle)
    assert isinstance(ack, CancelAck)
    assert ack.accepted is False
    assert ack.reason and "terminal" in ack.reason


def test_cancel_unknown_handle_raises_broker_submit_error() -> None:
    backend, _, _ = _build_backend()
    bogus = OrderHandle(
        handle_id="lsh_does_not_exist",
        backend_id="local_sim",
        submitted_at=datetime.now(),
        intent_id="intent_bogus",
    )
    with pytest.raises(BrokerSubmitError):
        backend.cancel(bogus)


# ---------------------------------------------------------------------------
# 6. query_status / query_account / query_positions
# ---------------------------------------------------------------------------


def test_query_status_returns_recorded_status() -> None:
    backend, _, _ = _build_backend()
    handle = backend.submit_order_intent(_buy_intent(backend))
    status = backend.query_status(handle)
    assert isinstance(status, OrderHandleStatus)
    assert status.handle_id == handle.handle_id
    assert status.state == "filled"


def test_query_account_returns_decimal_snapshot() -> None:
    backend, _, _ = _build_backend(initial_cash=200_000.0)
    snap0 = backend.query_account()
    assert isinstance(snap0, BrokerAccountSnapshot)
    assert snap0.backend_id == "local_sim"
    assert snap0.cash == Decimal("200000.0")
    backend.submit_order_intent(_buy_intent(backend, quantity=100))
    snap1 = backend.query_account()
    assert snap1.cash < snap0.cash


def test_query_positions_returns_position_lot_dict() -> None:
    backend, _, _ = _build_backend()
    assert backend.query_positions() == {}
    backend.submit_order_intent(_buy_intent(backend, quantity=100))
    positions = backend.query_positions()
    assert "000001.SZ" in positions
    lot = positions["000001.SZ"]
    assert isinstance(lot, PositionLot)
    assert lot.quantity == 100
    assert lot.portfolio_id == backend.portfolio_id


# ---------------------------------------------------------------------------
# 7. subscribe_fill_callback / unsubscribe
# ---------------------------------------------------------------------------


def test_subscribe_returns_handle_and_unsubscribe_releases() -> None:
    backend, _, _ = _build_backend()
    received_a: list[FillEvent] = []
    received_b: list[FillEvent] = []
    sub_a = backend.subscribe_fill_callback(received_a.append)
    sub_b = backend.subscribe_fill_callback(received_b.append)
    assert isinstance(sub_a, SubscriptionHandle)
    assert sub_a.subscription_id != sub_b.subscription_id

    backend.submit_order_intent(_buy_intent(backend, symbol="000001.SZ"))
    assert len(received_a) >= 1
    assert len(received_b) >= 1
    a_count_before = len(received_a)

    backend.unsubscribe_fill_callback(sub_a)
    backend.submit_order_intent(_buy_intent(backend, symbol="000002.SZ"))
    # sub_a unsubscribed — no new events delivered to it
    assert len(received_a) == a_count_before
    assert len(received_b) > 1


def test_unsubscribe_unknown_handle_is_silent_noop() -> None:
    backend, _, _ = _build_backend()
    backend.unsubscribe_fill_callback(
        SubscriptionHandle(subscription_id="lsub_unknown", backend_id="local_sim")
    )


# ---------------------------------------------------------------------------
# 8. market_data_channel + bind_capacity
# ---------------------------------------------------------------------------


def test_market_data_channel_reflects_bound_source() -> None:
    backend, _, _ = _build_backend(data_source=MinuteDataSource.DB_HISTORICAL)
    ch = backend.market_data_channel()
    assert isinstance(ch, MarketDataChannel)
    assert ch.backend_id == "local_sim"
    assert ch.source == MinuteDataSource.DB_HISTORICAL
    assert ch.channel_kind == "in_process_db"

    backend2, _, _ = _build_backend(
        portfolio_id="paper_local_p_tdx",
        data_source=MinuteDataSource.TDX_REALTIME,
    )
    ch2 = backend2.market_data_channel()
    assert ch2.channel_kind == "in_process_tdx"


def test_bind_capacity_localsim_is_per_portfolio() -> None:
    backend, _, _ = _build_backend()
    cap = backend.bind_capacity()
    assert isinstance(cap, BrokerBindCapacity)
    assert cap.backend_id == "local_sim"
    assert cap.max_concurrent_packages == 1


# ---------------------------------------------------------------------------
# 9. Multi-portfolio isolation (R-Q9 D2)
# ---------------------------------------------------------------------------


def test_two_localsim_instances_isolate_ledger_and_orders() -> None:
    backend_a, _, _ = _build_backend(portfolio_id="paper_local_a", initial_cash=200_000.0)
    backend_b, _, _ = _build_backend(portfolio_id="paper_local_b", initial_cash=300_000.0)

    backend_a.submit_order_intent(_buy_intent(backend_a, symbol="000001.SZ", quantity=100))

    # Account / positions: B is untouched
    assert backend_a.query_account().cash < Decimal("200000.0")
    assert backend_b.query_account().cash == Decimal("300000.0")
    assert backend_a.query_positions()
    assert backend_b.query_positions() == {}

    # Cross-portfolio handle queries fail
    intent_a_handle = next(iter(backend_a._records.values())).handle  # type: ignore[attr-defined]
    with pytest.raises(BrokerSubmitError):
        backend_b.query_status(intent_a_handle)


def test_localsim_subscriber_isolation() -> None:
    backend_a, _, _ = _build_backend(portfolio_id="paper_local_a")
    backend_b, _, _ = _build_backend(portfolio_id="paper_local_b")

    received_a: list[FillEvent] = []
    received_b: list[FillEvent] = []
    backend_a.subscribe_fill_callback(received_a.append)
    backend_b.subscribe_fill_callback(received_b.append)

    backend_a.submit_order_intent(_buy_intent(backend_a))
    assert received_a and not received_b


# ---------------------------------------------------------------------------
# 10. LocalSim ledger money math, commission, and board-lot guardrails
# ---------------------------------------------------------------------------


def test_inmemoryledger_money_fields_are_decimal_quantized_without_float_drift() -> None:
    ledger = InMemoryLedger(
        portfolio_id="paper_decimal_math",
        initial_cash=10_000_000.0,
        fee_model=FeeModel(open_cost=0.0, close_cost=0.0, min_cost=0.0),
    )

    ledger.apply_fill(
        _ledger_fill(order_id="ord_dec_1", fill_id="fill_dec_1", quantity=100, price=0.1)
    )
    ledger.apply_fill(
        _ledger_fill(order_id="ord_dec_2", fill_id="fill_dec_2", quantity=100, price=0.2)
    )

    assert ledger.cash_decimal == Decimal("9999970.00")
    assert ledger.cash_entries[0].notional == Decimal("10.00")
    assert ledger.cash_entries[0].fee == Decimal("0.00")
    assert ledger.cash_entries[0].cash_delta == Decimal("-10.00")
    assert ledger.cash_entries[0].cash_after == Decimal("9999990.00")
    assert ledger.cash_entries[1].cash_after == Decimal("9999970.00")


def test_inmemoryledger_min_commission_is_charged_incrementally_per_order() -> None:
    ledger = InMemoryLedger(
        portfolio_id="paper_order_fee",
        initial_cash=100_000.0,
        fee_model=FeeModel(open_cost=0.001, close_cost=0.001, min_cost=5.0),
    )

    ledger.apply_fill(
        _ledger_fill(order_id="ord_split", fill_id="fill_split_1", quantity=100, price=10.0)
    )
    ledger.apply_fill(
        _ledger_fill(order_id="ord_split", fill_id="fill_split_2", quantity=100, price=60.0)
    )

    assert [entry.fee for entry in ledger.cash_entries] == [
        Decimal("5.00"),
        Decimal("2.00"),
    ]
    assert sum((entry.fee for entry in ledger.cash_entries), Decimal("0.00")) == Decimal(
        "7.00"
    )
    assert ledger.cash_decimal == Decimal("92993.00")


def test_inmemoryledger_rejects_non_board_lot_buy_loudly_at_apply_fill() -> None:
    ledger = InMemoryLedger(portfolio_id="paper_board_buy", initial_cash=100_000.0)

    with pytest.raises(RiskRuleError, match="LOCAL_SIM_BOARD_LOT_VIOLATION") as exc_info:
        ledger.apply_fill(
            _unchecked_ledger_fill(
                order_id="ord_bad_buy",
                fill_id="fill_bad_buy",
                side=OrderSide.BUY,
                quantity=150,
            )
        )

    assert exc_info.value.context["reason_code"] == "LOCAL_SIM_BOARD_LOT_VIOLATION"
    assert exc_info.value.context["operation"] == "apply_fill"
    assert exc_info.value.context["order_id"] == "ord_bad_buy"
    assert exc_info.value.context["fill_quantity"] == 150


def test_inmemoryledger_rejects_non_board_lot_partial_sell_but_allows_full_exit() -> None:
    ledger = InMemoryLedger(portfolio_id="paper_board_sell", initial_cash=100_000.0)
    ledger.positions["000001.SZ"] = PositionLot(
        portfolio_id=ledger.portfolio_id,
        symbol="000001.SZ",
        quantity=250,
        available_quantity=250,
        avg_cost=10.0,
        trade_date=TRADE_DATE,
    )

    with pytest.raises(RiskRuleError, match="LOCAL_SIM_BOARD_LOT_VIOLATION") as exc_info:
        ledger.apply_fill(
            _ledger_fill(
                order_id="ord_bad_sell",
                fill_id="fill_bad_sell",
                side=OrderSide.SELL,
                quantity=50,
                price=11.0,
            )
        )
    assert exc_info.value.context["reason_code"] == "LOCAL_SIM_BOARD_LOT_VIOLATION"
    assert exc_info.value.context["held_quantity"] == 250

    ledger.apply_fill(
        _unchecked_ledger_fill(
            order_id="ord_full_exit",
            fill_id="fill_full_exit",
            side=OrderSide.SELL,
            quantity=250,
            price=11.0,
        )
    )

    assert "000001.SZ" not in ledger.positions
    assert ledger.cash_entries[-1].notional == Decimal("2750.00")


def test_inmemoryledger_uses_star_board_lot_increment_for_whole_position_sell() -> None:
    ledger = InMemoryLedger(portfolio_id="paper_star_board_sell", initial_cash=100_000.0)
    ledger.positions["688720.SH"] = PositionLot(
        portfolio_id=ledger.portfolio_id,
        symbol="688720.SH",
        quantity=1547,
        available_quantity=1547,
        avg_cost=10.0,
        trade_date=TRADE_DATE,
    )

    ledger.apply_fill(
        _ledger_fill(
            order_id="ord_star_full_exit",
            fill_id="fill_star_full_exit",
            symbol="688720.SH",
            side=OrderSide.SELL,
            quantity=1547,
            price=11.0,
        )
    )

    assert "688720.SH" not in ledger.positions
    assert ledger.cash_entries[-1].notional == Decimal("17017.00")
