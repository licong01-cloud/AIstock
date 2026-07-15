"""LocalSimBackend — in-process broker matching minute bars.

Strategy Engine design 2026-05-08 §3.6 (R-Q9 D1/D2/D3). LocalSim is the
default Paper Trading v2 backend. It binds to a single ``portfolio_id`` and
matches OrderIntents against TDX or DB historical minute bars in-process
via the existing ``MinuteExecutionEngine`` + ``OMS`` + ``InMemoryLedger``
stack.

Execution contract:
  - Historical LocalSIM remains synchronous because the authoritative closed
    day is complete at submission time.
  - Same-day TDX LocalSIM is a scheduler-owned durable minute loop. Submit only
    consumes bars after the frozen causality cursor; partial orders remain
    active and are restored on later scheduler ticks.
  - Fill callbacks produced by the current tick are dispatched before that
    tick returns, but callers must not interpret submit return as terminal.

Multi-package binding (R-Q9 D2): each portfolio creates its own
``LocalSimBackend`` instance; instances do not share state, supporting N
parallel portfolios per process.
"""

from __future__ import annotations

import threading
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, time
from decimal import Decimal
from typing import Any, Callable, Iterable, Mapping
from uuid import uuid4

from backend.services.paper_trading_v2.market_data import (
    MinuteDataSource,
    PaperV2MinuteMarketDataProvider,
    assert_broker_market_source_match,
)
from backend.services.strategy_package.execution_policy import normalize_execution_policy_json
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.simulation_runtime.models import (
    LocalSimExecutionRuntimeStatus,
    LocalSimExecutionStateV1,
    canonical_json_sha256,
)
from backend.services.trading_core.errors import (
    BrokerConnectivityError,
    BrokerRejectedError,
    BrokerSubmitError,
    DataUnavailableError,
    ExecutionAlgoError,
    InvalidStateTransitionError,
    RiskRuleError,
    RuntimeConfigInvalidError,
    TradingCoreError,
)
from backend.services.trading_core.execution_algo_capabilities import require_execution_algo_supports_mode
from backend.services.trading_core.ledger import FeeModel, InMemoryLedger
from backend.services.trading_core.minute_execution import MinuteExecutionEngine
from backend.services.trading_core.models import (
    Fill,
    Order,
    OrderEvent,
    OrderIntent,
    OrderStatus,
    PositionLot,
)
from backend.services.trading_core.oms import OMS

from .base import (
    BackendId,
    BrokerAccountSnapshot,
    BrokerBackend,
    BrokerBindCapacity,
    CancelAck,
    FillEvent,
    MarketDataChannel,
    MarketDataChannelKind,
    OrderHandle,
    OrderHandleStatus,
    OrderHandleStatusState,
    SubscriptionHandle,
)


_BACKEND_ID: BackendId = "local_sim"
_BACKEND_VERSION = "1.0.0"

_CHANNEL_KIND_BY_SOURCE: dict[MinuteDataSource, MarketDataChannelKind] = {
    MinuteDataSource.TDX_REALTIME: "in_process_tdx",
    MinuteDataSource.DB_HISTORICAL: "in_process_db",
}


class _OrderRecord:
    """Internal bookkeeping per submitted OrderIntent.

    Realtime LocalSIM keeps the durable execution state active across scheduler
    ticks; historical execution may still resolve synchronously.
    """

    __slots__ = ("handle", "order", "status", "fills", "events", "execution_state")

    def __init__(
        self,
        handle: OrderHandle,
        order: Order,
        status: OrderHandleStatus,
        fills: list[Fill],
        events: list[OrderEvent],
        execution_state: LocalSimExecutionStateV1 | None = None,
    ) -> None:
        self.handle = handle
        self.order = order
        self.status = status
        self.fills = fills
        self.events = events
        self.execution_state = execution_state


class LocalSimBackend(BrokerBackend):
    backend_id: BackendId = _BACKEND_ID
    backend_version: str = _BACKEND_VERSION

    def __init__(
        self,
        *,
        portfolio_id: str,
        initial_cash: float,
        data_source: MinuteDataSource,
        manifest: StrategyPackageManifest,
        package_id: str | None = None,
        market_data_provider: PaperV2MinuteMarketDataProvider | None = None,
        oms: OMS | None = None,
        execution_engine: MinuteExecutionEngine | None = None,
        fee_model: FeeModel | None = None,
        execution_policy: Mapping[str, Any] | None = None,
        initial_available_cash: float | None = None,
        initial_positions: Mapping[str, PositionLot] | None = None,
        scheduler_as_of_time: datetime | None = None,
    ) -> None:
        if not portfolio_id:
            raise ValueError("portfolio_id is required")
        if initial_cash <= 0:
            raise ValueError("initial_cash must be positive")
        # R-Q9 D3 belt-and-suspenders: even though service.create_portfolio
        # already validated the binding, recheck at backend init so a misuse
        # path that bypasses the service still fails fast.
        assert_broker_market_source_match(self.backend_id, data_source)

        self._portfolio_id = portfolio_id
        self._package_id = package_id or manifest.package_id
        self._manifest = manifest
        self._execution_policy = self._resolve_execution_policy(
            manifest=manifest,
            execution_policy=execution_policy,
        )
        self._data_source = data_source
        self._market_data_provider = market_data_provider or PaperV2MinuteMarketDataProvider()
        self._oms = oms or OMS()
        self._execution_engine = execution_engine or MinuteExecutionEngine(oms=self._oms)
        self._ledger = InMemoryLedger(
            portfolio_id=portfolio_id,
            initial_cash=initial_cash,
            fee_model=fee_model,
        )
        if initial_available_cash is not None:
            if initial_available_cash < 0:
                raise ValueError("initial_available_cash must be non-negative")
            self._ledger.cash = float(initial_available_cash)
        for symbol, lot in dict(initial_positions or {}).items():
            if lot.portfolio_id != portfolio_id:
                raise ValueError(
                    "initial_positions lot portfolio_id must match LocalSim portfolio_id"
                )
            self._ledger.positions[str(symbol)] = lot
        self._records: dict[str, _OrderRecord] = {}  # keyed by handle_id
        self._intent_index: dict[str, str] = {}  # intent_id -> handle_id
        self._subscribers: dict[str, Callable[[FillEvent], None]] = {}
        self._lock = threading.RLock()
        self._closed = False
        self._scheduler_as_of_time = scheduler_as_of_time
        self._eligible_bar_after: datetime | None = None
        self._bound_plan_id: str | None = None
        self._batch_snapshot: dict[str, Any] | None = None
        self._batch_plan_id: str | None = None
        self._deferred_fill_events: list[FillEvent] = []
        self._runtime_run_id: str | None = None
        self._runtime_binding_id: str | None = None

    # ----- Read accessors used by adapter / tests -----
    @property
    def portfolio_id(self) -> str:
        return self._portfolio_id

    @property
    def package_id(self) -> str:
        return self._package_id

    @property
    def data_source(self) -> MinuteDataSource:
        return self._data_source

    def configure_execution_runtime(self, *, run_id: str, binding_id: str) -> None:
        """Bind the immutable scheduler/run identity used by durable state."""
        run_id = str(run_id or "").strip()
        binding_id = str(binding_id or "").strip()
        if not run_id or not binding_id:
            raise BrokerSubmitError(
                "LocalSim durable execution requires run_id and binding_id",
                context={"reason_code": "LOCALSIM_RUNTIME_SCOPE_MISSING"},
            )
        with self._lock:
            if self._runtime_run_id not in {None, run_id} or self._runtime_binding_id not in {None, binding_id}:
                raise BrokerSubmitError(
                    "LocalSim runtime identity cannot be rebound",
                    context={
                        "reason_code": "LOCALSIM_RUNTIME_SCOPE_IDENTITY_CONFLICT",
                        "current_run_id": self._runtime_run_id,
                        "incoming_run_id": run_id,
                        "current_binding_id": self._runtime_binding_id,
                        "incoming_binding_id": binding_id,
                    },
                )
            self._runtime_run_id = run_id
            self._runtime_binding_id = binding_id

    def restore_execution_state(self, *, order: Order, state: LocalSimExecutionStateV1) -> OrderHandle:
        """Restore one order without replaying already applied bars."""
        self._ensure_alive()
        with self._lock:
            self._require_runtime_scope()
            if state.run_id != self._runtime_run_id or state.binding_id != self._runtime_binding_id:
                raise BrokerSubmitError(
                    "LocalSim restored state does not match bound runtime identity",
                    context={
                        "reason_code": "LOCALSIM_RESTORE_RUNTIME_IDENTITY_CONFLICT",
                        "state_id": state.state_id,
                        "state_run_id": state.run_id,
                        "bound_run_id": self._runtime_run_id,
                        "state_binding_id": state.binding_id,
                        "bound_binding_id": self._runtime_binding_id,
                    },
                )
            if order.order_id != state.order_id or order.intent_id != state.intent_id:
                raise BrokerSubmitError(
                    "LocalSim restored order does not match durable state",
                    context={
                        "reason_code": "LOCALSIM_RESTORE_ORDER_IDENTITY_CONFLICT",
                        "state_id": state.state_id,
                        "state_order_id": state.order_id,
                        "order_id": order.order_id,
                        "state_intent_id": state.intent_id,
                        "order_intent_id": order.intent_id,
                    },
                )
            if state.intent_id in self._intent_index:
                raise BrokerSubmitError(
                    "LocalSim durable state was restored more than once",
                    context={"reason_code": "LOCALSIM_RESTORE_DUPLICATE_INTENT", "state_id": state.state_id},
                )
            handle = OrderHandle(
                handle_id=f"lsh_{state.state_id[-32:]}",
                backend_id=self.backend_id,
                submitted_at=state.created_at,
                intent_id=state.intent_id,
            )
            self._records[handle.handle_id] = _OrderRecord(
                handle=handle,
                order=order,
                status=self._build_status(handle.handle_id, order),
                fills=[],
                events=[],
                execution_state=state,
            )
            self._intent_index[state.intent_id] = handle.handle_id
            return handle

    def advance_realtime_execution(self, *, as_of_time: datetime) -> tuple[OrderHandle, ...]:
        """Apply each newly observed causal minute exactly once to restored states."""
        self._ensure_alive()
        if self._data_source != MinuteDataSource.TDX_REALTIME:
            raise BrokerSubmitError(
                "incremental LocalSim advancement is only valid for TDX realtime",
                context={"reason_code": "LOCALSIM_INCREMENTAL_SOURCE_INVALID", "source": self._data_source.value},
            )
        self._scheduler_as_of_time = as_of_time
        self._require_runtime_scope()
        plan_id = self._bound_plan_id
        if plan_id is None:
            raise BrokerSubmitError(
                "LocalSim realtime advancement is missing the bound execution plan",
                context={"reason_code": "LOCALSIM_CAUSALITY_SCOPE_MISSING"},
            )
        self.begin_plan_submission(plan_id=plan_id)
        try:
            with self._lock:
                self._ledger.fills = []
                self._ledger.cash_entries = []
                for record in self._records.values():
                    record.fills = []
                    record.events = []
                handles: list[OrderHandle] = []
                for record in self._records.values():
                    state = record.execution_state
                    if state is None:
                        raise BrokerSubmitError(
                            "realtime LocalSim order is missing durable execution state",
                            context={
                                "reason_code": "LOCALSIM_DURABLE_STATE_MISSING",
                                "order_id": record.order.order_id,
                                "intent_id": record.order.intent_id,
                            },
                        )
                    if state.is_terminal:
                        handles.append(record.handle)
                        continue
                    market_input = self._load_realtime_market_input(
                        symbol=record.order.symbol,
                        trade_date=state.trade_date,
                        as_of_time=as_of_time,
                    )
                    self._validate_replayed_cursor_bar(state=state, observed_bars=market_input.minute_bars)
                    cursor = state.last_processed_bar_time or state.causality_cursor
                    new_bars = [
                        bar for bar in market_input.minute_bars
                        if self._naive_for_compare(bar.bar_time) > self._naive_for_compare(cursor)
                    ]
                    if new_bars:
                        final_order, engine_state, fills, events = self._execution_engine.execute_order_incremental(
                            order=record.order,
                            execution_state=state,
                            new_bars=new_bars,
                            algo_code=str(self._execution_policy["algo_code"]),
                            algo_config=dict(self._execution_policy.get("algo_config") or {}),
                            market_context=self._incremental_market_context(
                                market_input.market_context,
                                observed_until=as_of_time,
                                causal_bar_count=len(new_bars),
                            ),
                        )
                        self._apply_incremental_effects(
                            record=record, final_order=final_order, fills=fills, events=events
                        )
                        record.execution_state = self._next_execution_state(
                            previous=state,
                            engine_state=engine_state,
                            order=final_order,
                            bars=new_bars,
                            fill_count=len(fills),
                        )
                    record.execution_state = self._expire_residual_after_complete_close(
                        state=record.execution_state,
                        observed_bars=market_input.minute_bars,
                        as_of_time=as_of_time,
                        increment_sequence=not bool(new_bars),
                    )
                    handles.append(record.handle)
        except Exception:
            self.rollback_plan_submission(plan_id=plan_id)
            raise
        self.commit_plan_submission(plan_id=plan_id)
        return tuple(handles)

    # ----- BrokerBackend Protocol -----
    def bind_execution_plan(self, *, plan: Any, as_of_time: datetime) -> None:
        """Bind the immutable scheduler cursor used to prevent look-ahead fills."""
        if self._data_source != MinuteDataSource.TDX_REALTIME:
            return
        payload = getattr(plan, "plan_payload_json", {}).get("local_sim_execution_causality")
        if not isinstance(payload, dict) or not payload.get("eligible_bar_after"):
            raise BrokerSubmitError(
                "LocalSim realtime execution plan is missing causality evidence",
                context={"plan_id": getattr(plan, "plan_id", None)},
            )
        try:
            cursor = datetime.fromisoformat(str(payload["eligible_bar_after"]))
        except ValueError as exc:
            raise BrokerSubmitError(
                "LocalSim realtime execution plan has an invalid causality cursor",
                context={
                    "plan_id": getattr(plan, "plan_id", None),
                    "eligible_bar_after": payload.get("eligible_bar_after"),
                },
            ) from exc
        target_trade_date = getattr(plan, "target_trade_date", None)
        if target_trade_date is None or as_of_time.date() != target_trade_date or cursor.date() != target_trade_date:
            raise BrokerSubmitError(
                "LocalSim realtime causality times must match the execution trade date",
                context={
                    "plan_id": getattr(plan, "plan_id", None),
                    "target_trade_date": str(target_trade_date),
                    "as_of_time": as_of_time.isoformat(),
                    "eligible_bar_after": cursor.isoformat(),
                },
            )
        self._bound_plan_id = str(getattr(plan, "plan_id"))
        self._scheduler_as_of_time = as_of_time
        self._eligible_bar_after = cursor

    def begin_plan_submission(self, *, plan_id: str) -> None:
        with self._lock:
            if self._batch_snapshot is not None:
                raise BrokerSubmitError(
                    "LocalSim plan submission transaction is already active",
                    context={"active_plan_id": self._batch_plan_id, "plan_id": plan_id},
                )
            self._batch_snapshot = self._snapshot_mutable_state()
            self._batch_plan_id = plan_id
            self._deferred_fill_events = []

    def commit_plan_submission(self, *, plan_id: str) -> None:
        with self._lock:
            self._require_active_batch(plan_id)
            events = list(self._deferred_fill_events)
            self._batch_snapshot = None
            self._batch_plan_id = None
            self._deferred_fill_events = []
        for event in events:
            self._dispatch_fill(event)

    def rollback_plan_submission(self, *, plan_id: str) -> None:
        with self._lock:
            self._require_active_batch(plan_id)
            snapshot = self._batch_snapshot
            assert snapshot is not None
            self._restore_mutable_state(snapshot)
            self._batch_snapshot = None
            self._batch_plan_id = None
            self._deferred_fill_events = []

    def submit_order_intent(self, intent: OrderIntent) -> OrderHandle:
        self._ensure_alive()
        if intent.portfolio_id != self._portfolio_id:
            raise BrokerSubmitError(
                "OrderIntent.portfolio_id does not match LocalSim binding",
                context={
                    "intent_id": intent.intent_id,
                    "intent_portfolio_id": intent.portfolio_id,
                    "backend_portfolio_id": self._portfolio_id,
                },
            )
        if intent.package_id != self._package_id:
            raise BrokerSubmitError(
                "OrderIntent.package_id does not match LocalSim binding",
                context={
                    "intent_id": intent.intent_id,
                    "intent_package_id": intent.package_id,
                    "backend_package_id": self._package_id,
                },
            )
        with self._lock:
            if intent.intent_id in self._intent_index:
                raise BrokerSubmitError(
                    "OrderIntent already submitted to this LocalSim instance",
                    context={"intent_id": intent.intent_id},
                )

            try:
                if self._data_source == MinuteDataSource.TDX_REALTIME:
                    self._require_runtime_scope()
                    if self._eligible_bar_after is None or self._scheduler_as_of_time is None:
                        raise DataUnavailableError(
                            "LocalSim realtime execution is missing bound plan causality",
                            context={"reason_code": "LOCALSIM_CAUSALITY_SCOPE_MISSING", "intent_id": intent.intent_id},
                        )
                    market_input = self._load_realtime_market_input(
                        symbol=intent.symbol,
                        trade_date=intent.target_trade_date,
                        as_of_time=self._scheduler_as_of_time,
                    )
                    cursor_cmp = self._naive_for_compare(self._eligible_bar_after)
                    causal_bars = [
                        bar
                        for bar in market_input.minute_bars
                        if self._naive_for_compare(bar.bar_time) > cursor_cmp
                    ]
                    market_input = replace(
                        market_input,
                        minute_bars=causal_bars,
                        market_context=self._incremental_market_context(
                            market_input.market_context,
                            observed_until=self._scheduler_as_of_time,
                            causal_bar_count=len(causal_bars),
                        ),
                    )
                else:
                    market_input = self._market_data_provider.load_symbol_input(
                        symbol=intent.symbol,
                        trade_date=intent.target_trade_date,
                        source=self._data_source,
                        min_bars=1,
                        require_day_features=self._algo_requires_day_features(),
                    )
            except DataUnavailableError as exc:
                # Missing minute bars / pre_close / suspend — treat as a
                # connectivity-class fault: the data layer the broker depends
                # on is unavailable, not an order-shape problem.
                raise BrokerConnectivityError(
                    "LocalSim could not load minute market data",
                    context={
                        "intent_id": intent.intent_id,
                        "symbol": intent.symbol,
                        "trade_date": intent.target_trade_date.isoformat(),
                        "source": self._data_source.value,
                        "cause": exc.message,
                    },
                ) from exc

            order = self._oms.create_order(intent)
            execution_state: LocalSimExecutionStateV1 | None = None
            try:
                if self._data_source == MinuteDataSource.TDX_REALTIME:
                    execution_state = self._new_execution_state(intent=intent, order=order)
                    if market_input.minute_bars:
                        final_order, engine_state, fills, events = self._execution_engine.execute_order_incremental(
                            order=order,
                            execution_state=execution_state,
                            new_bars=list(market_input.minute_bars),
                            algo_code=str(self._execution_policy["algo_code"]),
                            algo_config=dict(self._execution_policy.get("algo_config") or {}),
                            market_context=market_input.market_context,
                        )
                        execution_state = self._next_execution_state(
                            previous=execution_state,
                            engine_state=engine_state,
                            order=final_order,
                            bars=list(market_input.minute_bars),
                            fill_count=len(fills),
                        )
                    else:
                        final_order, fills, events = order, [], []
                    execution_state = self._expire_residual_after_complete_close(
                        state=execution_state,
                        observed_bars=market_input.minute_bars,
                        as_of_time=self._scheduler_as_of_time,
                        increment_sequence=not bool(market_input.minute_bars),
                    )
                else:
                    final_order, fills, events = self._execution_engine.execute_order(
                        order=order,
                        minute_bars=market_input.minute_bars,
                        algo_code=str(self._execution_policy["algo_code"]),
                        algo_config=dict(self._execution_policy.get("algo_config") or {}),
                        market_context=market_input.market_context,
                        allow_partial_fill=bool(
                            (self._execution_policy.get("algo_config") or {}).get("allow_partial_fill", True)
                        ),
                    )
            except (ExecutionAlgoError, RiskRuleError, InvalidStateTransitionError) as exc:
                # Backend rejected — distinct from connectivity (data fine,
                # order semantics violated).
                rejection_handle = OrderHandle(
                    handle_id=f"lsh_{uuid4().hex}",
                    backend_id=self.backend_id,
                    submitted_at=datetime.now(UTC),
                    intent_id=intent.intent_id,
                )
                rejection_status = OrderHandleStatus(
                    handle_id=rejection_handle.handle_id,
                    state="rejected",
                    filled_quantity=0,
                    avg_fill_price=None,
                    last_event_at=datetime.now(UTC),
                    rejection_reason=exc.message,
                )
                self._records[rejection_handle.handle_id] = _OrderRecord(
                    handle=rejection_handle,
                    order=order,
                    status=rejection_status,
                    fills=[],
                    events=[],
                )
                self._intent_index[intent.intent_id] = rejection_handle.handle_id
                raise BrokerRejectedError(
                    "LocalSim backend rejected the order",
                    context={
                        "intent_id": intent.intent_id,
                        "handle_id": rejection_handle.handle_id,
                        "symbol": intent.symbol,
                        "side": intent.side.value,
                        "quantity": intent.quantity,
                        "cause": exc.message,
                        "cause_code": exc.error_code,
                    },
                ) from exc

            ledger_snapshot = self._snapshot_ledger_state()
            try:
                for fill in fills:
                    self._ledger.apply_fill(fill)
            except Exception as exc:
                self._restore_ledger_state(ledger_snapshot)
                if not isinstance(exc, (RiskRuleError, TradingCoreError)):
                    raise
                # Ledger refused (e.g. insufficient cash). Order matched at
                # the algo layer but the simulated account cannot absorb it.
                rejection_handle = OrderHandle(
                    handle_id=f"lsh_{uuid4().hex}",
                    backend_id=self.backend_id,
                    submitted_at=datetime.now(UTC),
                    intent_id=intent.intent_id,
                )
                rejection_status = OrderHandleStatus(
                    handle_id=rejection_handle.handle_id,
                    state="rejected",
                    filled_quantity=0,
                    avg_fill_price=None,
                    last_event_at=datetime.now(UTC),
                    rejection_reason=getattr(exc, "message", str(exc)),
                )
                self._records[rejection_handle.handle_id] = _OrderRecord(
                    handle=rejection_handle,
                    order=order,
                    status=rejection_status,
                    fills=[],
                    events=[],
                )
                self._intent_index[intent.intent_id] = rejection_handle.handle_id
                raise BrokerRejectedError(
                    "LocalSim ledger rejected the order",
                    context={
                        "intent_id": intent.intent_id,
                        "handle_id": rejection_handle.handle_id,
                        "cause": getattr(exc, "message", str(exc)),
                        "cause_code": getattr(exc, "error_code", "UNKNOWN"),
                    },
                ) from exc

            handle = OrderHandle(
                handle_id=f"lsh_{uuid4().hex}",
                backend_id=self.backend_id,
                submitted_at=datetime.now(UTC),
                intent_id=intent.intent_id,
            )
            status = self._build_status(handle.handle_id, final_order)
            self._records[handle.handle_id] = _OrderRecord(
                handle=handle,
                order=final_order,
                status=status,
                fills=list(fills),
                events=list(events),
                execution_state=execution_state,
            )
            self._intent_index[intent.intent_id] = handle.handle_id

        # Synchronous fan-out to subscribers (inside the same submit call).
        for fill in fills:
            event = FillEvent(
                handle_id=handle.handle_id,
                intent_id=intent.intent_id,
                fill_quantity=fill.quantity,
                fill_price=Decimal(repr(fill.price)),
                fill_ts=fill.trade_time,
                venue=self.backend_id,
            )
            if self._batch_snapshot is not None:
                self._deferred_fill_events.append(event)
            else:
                self._dispatch_fill(event)
        return handle

    def cancel(self, handle: OrderHandle) -> CancelAck:
        self._ensure_alive()
        with self._lock:
            record = self._records.get(handle.handle_id)
            if record is None:
                raise BrokerSubmitError(
                    "unknown OrderHandle",
                    context={"handle_id": handle.handle_id},
                )
            if record.status.state in {"filled", "rejected", "cancelled"}:
                return CancelAck(
                    handle_id=handle.handle_id,
                    accepted=False,
                    reason=f"order already in terminal state {record.status.state}",
                )
            # Synchronous matching means the only non-terminal state is
            # ``partial_filled``. Cancel the OMS order and update status.
            try:
                cancelled_order, event = self._oms.cancel_order(record.order, "user_cancel")
            except InvalidStateTransitionError as exc:
                raise BrokerSubmitError(
                    "OMS rejected cancel",
                    context={
                        "handle_id": handle.handle_id,
                        "cause": exc.message,
                    },
                ) from exc
            record.order = cancelled_order
            record.events.append(event)
            record.status = OrderHandleStatus(
                handle_id=handle.handle_id,
                state="cancelled",
                filled_quantity=cancelled_order.filled_quantity,
                avg_fill_price=(
                    Decimal(repr(cancelled_order.avg_fill_price))
                    if cancelled_order.avg_fill_price is not None
                    else None
                ),
                last_event_at=datetime.now(UTC),
                rejection_reason=None,
            )
            if record.execution_state is not None:
                state = record.execution_state
                payload = state.model_dump(mode="python")
                sequence = state.sequence + 1
                payload.update(
                    {
                        "order_status": cancelled_order.status.value,
                        "runtime_status": LocalSimExecutionRuntimeStatus.CANCELLED,
                        "terminal_reason": "USER_CANCELLED",
                        "sequence": sequence,
                        "idempotency_key": canonical_json_sha256(
                            ["localsim_state_transition_v1", state.state_id, sequence, "CANCELLED"]
                        ),
                        "state_hash": "",
                        "updated_at": datetime.now(UTC),
                    }
                )
                record.execution_state = LocalSimExecutionStateV1.model_validate(payload)
            return CancelAck(
                handle_id=handle.handle_id,
                accepted=True,
                reason="user_cancel",
            )

    def query_status(self, handle: OrderHandle) -> OrderHandleStatus:
        self._ensure_alive()
        with self._lock:
            record = self._records.get(handle.handle_id)
            if record is None:
                raise BrokerSubmitError(
                    "unknown OrderHandle",
                    context={"handle_id": handle.handle_id},
                )
            return record.status

    def subscribe_fill_callback(
        self, cb: Callable[[FillEvent], None]
    ) -> SubscriptionHandle:
        self._ensure_alive()
        sub_id = f"lsub_{uuid4().hex}"
        with self._lock:
            self._subscribers[sub_id] = cb
        return SubscriptionHandle(subscription_id=sub_id, backend_id=self.backend_id)

    def unsubscribe_fill_callback(self, handle: SubscriptionHandle) -> None:
        with self._lock:
            self._subscribers.pop(handle.subscription_id, None)

    def query_account(self) -> BrokerAccountSnapshot:
        self._ensure_alive()
        with self._lock:
            cash = Decimal(repr(self._ledger.cash))
            # NAV here = cash + cost-basis position value. LocalSim cannot
            # mark-to-market without a price feed; the adapter calls
            # ledger.account_snapshot(prices=...) with explicit prices for the
            # full mtm view. query_account() is a backend-level summary.
            position_cost = sum(
                (
                    Decimal(repr(lot.avg_cost)) * lot.quantity
                    for lot in self._ledger.positions.values()
                ),
                Decimal(0),
            )
            nav = cash + position_cost
            return BrokerAccountSnapshot(
                backend_id=self.backend_id,
                cash=cash,
                nav=nav,
                margin_used=None,
                as_of=datetime.now(UTC),
            )

    def query_positions(self) -> dict[str, PositionLot]:
        self._ensure_alive()
        with self._lock:
            return dict(self._ledger.positions)

    def export_execution_snapshot(self, *, handles: Iterable[OrderHandle] | None = None) -> dict[str, Any]:
        """Export synchronous LocalSim side effects for durable adapter persistence."""

        self._ensure_alive()
        with self._lock:
            if handles is None:
                records = list(self._records.values())
            else:
                records = []
                for handle in handles:
                    record = self._records.get(handle.handle_id)
                    if record is None:
                        raise BrokerSubmitError(
                            "unknown OrderHandle",
                            context={"handle_id": handle.handle_id},
                        )
                    records.append(record)
            return {
                "orders": tuple(record.order for record in records),
                "fills": tuple(fill for record in records for fill in record.fills),
                "events": tuple(event for record in records for event in record.events),
                "cash_entries": tuple(self._ledger.cash_entries),
                "positions": dict(self._ledger.positions),
                "account": self.query_account(),
                "handle_statuses": tuple(record.status for record in records),
                "execution_states": tuple(
                    record.execution_state for record in records if record.execution_state is not None
                ),
            }

    def market_data_channel(self) -> MarketDataChannel:
        return MarketDataChannel(
            backend_id=self.backend_id,
            source=self._data_source,
            channel_kind=_CHANNEL_KIND_BY_SOURCE[self._data_source],
        )

    def bind_capacity(self) -> BrokerBindCapacity:
        # LocalSim is per-portfolio (R-Q9 D2 — multi-package parallelism is
        # achieved by spawning one LocalSim per portfolio, not by sharing a
        # single LocalSim across portfolios).
        return BrokerBindCapacity(
            backend_id=self.backend_id,
            max_concurrent_packages=1,
            rejection_reason_if_exceeded=(
                "LocalSim is per-portfolio; create a new instance per portfolio"
            ),
        )

    # ----- Lifecycle -----
    def shutdown(self) -> None:
        with self._lock:
            self._closed = True
            self._subscribers.clear()

    # ----- Internals -----
    @staticmethod
    def _naive_for_compare(value: datetime) -> datetime:
        return value.replace(tzinfo=None) if value.tzinfo is not None else value

    def _require_runtime_scope(self) -> None:
        if self._runtime_run_id is None or self._runtime_binding_id is None:
            raise BrokerSubmitError(
                "LocalSim realtime execution is missing durable runtime scope",
                context={"reason_code": "LOCALSIM_RUNTIME_SCOPE_MISSING", "plan_id": self._bound_plan_id},
            )

    def _load_realtime_market_input(self, *, symbol: str, trade_date: Any, as_of_time: datetime) -> Any:
        try:
            market_input = self._market_data_provider.load_observed_intraday(
                symbol=symbol,
                trade_date=trade_date,
                source=self._data_source,
                until_time=as_of_time,
                require_day_features=self._algo_requires_day_features(),
            )
        except DataUnavailableError as exc:
            raise BrokerConnectivityError(
                "LocalSim could not load realtime minute market data",
                context={
                    "reason_code": "LOCALSIM_REALTIME_MARKET_DATA_UNAVAILABLE",
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                    "source": self._data_source.value,
                    "observed_until": as_of_time.isoformat(),
                    "cause": exc.message,
                },
            ) from exc
        self._validate_observed_bar_stream(symbol=symbol, trade_date=trade_date, observed_bars=market_input.minute_bars)
        return market_input

    def _validate_observed_bar_stream(self, *, symbol: str, trade_date: Any, observed_bars: Iterable[Any]) -> None:
        previous_time: datetime | None = None
        seen: dict[datetime, str] = {}
        for bar in observed_bars:
            identity = self._bar_identity(bar)
            if bar.symbol != symbol or bar.bar_time.date() != trade_date:
                raise BrokerConnectivityError(
                    "LocalSim realtime minute bar identity conflicts with the requested stream",
                    context={
                        "reason_code": "LOCALSIM_MINUTE_BAR_IDENTITY_CONFLICT",
                        "requested_symbol": symbol,
                        "requested_trade_date": trade_date.isoformat(),
                        "bar_symbol": bar.symbol,
                        "bar_time": bar.bar_time.isoformat(),
                    },
                )
            existing = seen.get(bar.bar_time)
            if existing is not None:
                raise BrokerConnectivityError(
                    "LocalSim realtime minute stream contains a duplicate bar time",
                    context={
                        "reason_code": (
                            "LOCALSIM_MINUTE_BAR_PAYLOAD_CONFLICT" if existing != identity
                            else "LOCALSIM_MINUTE_BAR_DUPLICATE"
                        ),
                        "symbol": symbol,
                        "bar_time": bar.bar_time.isoformat(),
                        "first_identity": existing,
                        "duplicate_identity": identity,
                    },
                )
            if previous_time is not None and bar.bar_time <= previous_time:
                raise BrokerConnectivityError(
                    "LocalSim realtime minute stream is not strictly ordered",
                    context={
                        "reason_code": "LOCALSIM_MINUTE_BAR_OUT_OF_ORDER",
                        "symbol": symbol,
                        "previous_bar_time": previous_time.isoformat(),
                        "bar_time": bar.bar_time.isoformat(),
                    },
                )
            seen[bar.bar_time] = identity
            previous_time = bar.bar_time

    def _validate_replayed_cursor_bar(
        self, *, state: LocalSimExecutionStateV1, observed_bars: Iterable[Any]
    ) -> None:
        if state.last_processed_bar_time is None or state.last_applied_bar_identity is None:
            return
        matching = [bar for bar in observed_bars if bar.bar_time == state.last_processed_bar_time]
        if not matching:
            raise BrokerConnectivityError(
                "LocalSim realtime source cannot read back the last applied minute bar",
                context={
                    "reason_code": "LOCALSIM_LAST_APPLIED_BAR_READBACK_MISSING",
                    "state_id": state.state_id,
                    "symbol": state.symbol,
                    "last_processed_bar_time": state.last_processed_bar_time.isoformat(),
                },
            )
        actual_identity = self._bar_identity(matching[0])
        if actual_identity != state.last_applied_bar_identity:
            raise BrokerConnectivityError(
                "LocalSim last applied minute bar payload changed after persistence",
                context={
                    "reason_code": "LOCALSIM_LAST_APPLIED_BAR_PAYLOAD_CONFLICT",
                    "state_id": state.state_id,
                    "symbol": state.symbol,
                    "bar_time": state.last_processed_bar_time.isoformat(),
                    "expected_identity": state.last_applied_bar_identity,
                    "actual_identity": actual_identity,
                },
            )

    def _bar_identity(self, bar: Any) -> str:
        return canonical_json_sha256(
            {"schema_version": "localsim_minute_bar_identity_v1", "source": self._data_source.value,
             "bar": bar.model_dump(mode="json")}
        )

    def _incremental_market_context(
        self, market_context: Mapping[str, Any], *, observed_until: datetime, causal_bar_count: int,
    ) -> dict[str, Any]:
        capability = require_execution_algo_supports_mode(
            self._execution_policy, mode="LIVE_ONLY", package_id=self._package_id
        )
        return {
            **dict(market_context),
            "eligible_bar_after": self._eligible_bar_after.isoformat() if self._eligible_bar_after else None,
            "observed_until": observed_until.isoformat(),
            "causal_bar_count": causal_bar_count,
            "live_step_mode": capability.live_step_mode,
            "plan_horizon_bars": capability.plan_horizon_bars,
            "v25_realtime_streaming": capability.algo_code in {"V25_TWO_STAGE", "V25_1_SMALL_CAP"},
        }

    def _new_execution_state(self, *, intent: OrderIntent, order: Order) -> LocalSimExecutionStateV1:
        self._require_runtime_scope()
        if self._bound_plan_id is None or self._eligible_bar_after is None:
            raise BrokerSubmitError(
                "LocalSim realtime execution is missing bound plan identity",
                context={"reason_code": "LOCALSIM_CAUSALITY_SCOPE_MISSING", "intent_id": intent.intent_id},
            )
        algo_code = str(self._execution_policy["algo_code"]).strip().upper()
        algo_instance_digest = canonical_json_sha256(
            ["localsim_algo_instance_v1", self._runtime_binding_id, intent.target_trade_date.isoformat(),
             self._bound_plan_id, intent.intent_id, algo_code]
        )
        return LocalSimExecutionStateV1(
            run_id=str(self._runtime_run_id),
            binding_id=str(self._runtime_binding_id),
            trade_date=intent.target_trade_date,
            plan_id=self._bound_plan_id,
            intent_id=intent.intent_id,
            algo_instance_id=f"lsalgo_{algo_instance_digest[:32]}",
            portfolio_id=self._portfolio_id,
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            total_quantity=order.quantity,
            filled_quantity=0,
            remaining_quantity=order.quantity,
            algo_code=algo_code,
            order_status=order.status.value,
            runtime_status=LocalSimExecutionRuntimeStatus.WAITING_FOR_CAUSAL_BAR,
            algo_state={"total_quantity": order.quantity, "executed_quantity": 0, "step": 0,
                        "is_complete": False, "causality_mode": "strict_no_backfill"},
            schedule_version=f"{algo_code}:persisted_plan_v1",
            causality_cursor=self._eligible_bar_after,
            idempotency_key=canonical_json_sha256(
                ["localsim_state_transition_v1", self._bound_plan_id, intent.intent_id, 0,
                 "WAITING_FOR_CAUSAL_BAR"]
            ),
        )

    def _next_execution_state(
        self, *, previous: LocalSimExecutionStateV1, engine_state: Any, order: Order,
        bars: list[Any], fill_count: int,
    ) -> LocalSimExecutionStateV1:
        if not bars:
            return previous
        last_bar = max(bars, key=lambda item: item.bar_time)
        last_bar_identity = self._bar_identity(last_bar)
        algo_state = dict(getattr(engine_state, "algo_state", {}) or {})
        raw_step = algo_state.get("step", previous.next_slice_index)
        if isinstance(raw_step, bool) or not isinstance(raw_step, int) or raw_step < 0:
            raise ExecutionAlgoError(
                "LocalSim execution state contains an invalid next slice index",
                context={"reason_code": "LOCALSIM_NEXT_SLICE_INDEX_INVALID", "state_id": previous.state_id,
                         "raw_step": raw_step},
            )
        sequence = previous.sequence + 1
        payload = previous.model_dump(mode="python")
        payload.update(
            {
                "filled_quantity": order.filled_quantity,
                "remaining_quantity": order.remaining_quantity,
                "order_status": order.status.value,
                "runtime_status": (
                    LocalSimExecutionRuntimeStatus.FILLED if order.status == OrderStatus.FILLED
                    else LocalSimExecutionRuntimeStatus.ACTIVE
                ),
                "algo_state": algo_state,
                "plan": deepcopy(getattr(engine_state, "plan", None)),
                "plan_sha256": None,
                "next_slice_index": raw_step,
                "last_processed_bar_time": getattr(engine_state, "last_processed_bar_time", last_bar.bar_time),
                "last_applied_bar_identity": last_bar_identity,
                "market_session": self._market_session(last_bar.bar_time),
                "latest_order_sequence": previous.latest_order_sequence + 1,
                "latest_fill_sequence": previous.latest_fill_sequence + fill_count,
                "latest_cash_sequence": previous.latest_cash_sequence + fill_count,
                "latest_position_sequence": previous.latest_position_sequence + fill_count,
                "sequence": sequence,
                "idempotency_key": canonical_json_sha256(
                    ["localsim_state_transition_v1", previous.state_id, sequence, last_bar_identity]
                ),
                "state_hash": "",
                "updated_at": datetime.now(UTC),
            }
        )
        return LocalSimExecutionStateV1.model_validate(payload)

    def _expire_residual_after_complete_close(
        self, *, state: LocalSimExecutionStateV1, observed_bars: Iterable[Any],
        as_of_time: datetime, increment_sequence: bool,
    ) -> LocalSimExecutionStateV1:
        if state.is_terminal or as_of_time.time() < time(15, 0):
            return state
        latest_bar_time = max((bar.bar_time for bar in observed_bars), default=None)
        if latest_bar_time is None or latest_bar_time.time() < time(15, 0):
            raise BrokerConnectivityError(
                "LocalSim cannot terminalize residual before the closing minute is observed",
                context={
                    "reason_code": "LOCALSIM_CLOSE_BAR_MISSING",
                    "state_id": state.state_id,
                    "symbol": state.symbol,
                    "trade_date": state.trade_date.isoformat(),
                    "latest_bar_time": latest_bar_time.isoformat() if latest_bar_time else None,
                    "observed_until": as_of_time.isoformat(),
                },
            )
        payload = state.model_dump(mode="python")
        sequence = state.sequence + (1 if increment_sequence else 0)
        payload.update(
            {
                "runtime_status": LocalSimExecutionRuntimeStatus.EXPIRED_WITH_RESIDUAL,
                "terminal_reason": "MARKET_SESSION_CLOSED_WITH_REMAINING_QUANTITY",
                "residual_classification": "SCHEDULE_RESIDUAL_AT_CLOSE",
                "sequence": sequence,
                "idempotency_key": canonical_json_sha256(
                    ["localsim_state_transition_v1", state.state_id, sequence, "EXPIRED_WITH_RESIDUAL"]
                ),
                "state_hash": "",
                "updated_at": datetime.now(UTC),
            }
        )
        return LocalSimExecutionStateV1.model_validate(payload)

    @staticmethod
    def _market_session(bar_time: datetime) -> str:
        return "AM" if bar_time.time() <= time(11, 30) else "PM"

    def _apply_incremental_effects(
        self, *, record: _OrderRecord, final_order: Order, fills: list[Fill], events: list[OrderEvent],
    ) -> None:
        ledger_snapshot = self._snapshot_ledger_state()
        try:
            for fill in fills:
                self._ledger.apply_fill(fill)
        except Exception:
            self._restore_ledger_state(ledger_snapshot)
            raise
        record.order = final_order
        record.status = self._build_status(record.handle.handle_id, final_order)
        record.fills.extend(fills)
        record.events.extend(events)
        for fill in fills:
            fill_event = FillEvent(
                handle_id=record.handle.handle_id,
                intent_id=record.handle.intent_id,
                fill_quantity=fill.quantity,
                fill_price=Decimal(repr(fill.price)),
                fill_ts=fill.trade_time,
                venue=self.backend_id,
            )
            if self._batch_snapshot is not None:
                self._deferred_fill_events.append(fill_event)
            else:
                self._dispatch_fill(fill_event)

    def _snapshot_ledger_state(self) -> dict[str, Any]:
        return {
            "cash": self._ledger._cash,
            "positions": deepcopy(self._ledger.positions),
            "fills": deepcopy(self._ledger.fills),
            "cash_entries": deepcopy(self._ledger.cash_entries),
            "order_fee_state": deepcopy(self._ledger._order_fee_state),
        }

    def _restore_ledger_state(self, snapshot: Mapping[str, Any]) -> None:
        self._ledger._cash = snapshot["cash"]
        self._ledger.positions = deepcopy(snapshot["positions"])
        self._ledger.fills = deepcopy(snapshot["fills"])
        self._ledger.cash_entries = deepcopy(snapshot["cash_entries"])
        self._ledger._order_fee_state = deepcopy(snapshot["order_fee_state"])

    def _snapshot_mutable_state(self) -> dict[str, Any]:
        return {
            "ledger": self._snapshot_ledger_state(),
            "records": deepcopy(self._records),
            "intent_index": deepcopy(self._intent_index),
            "oms_state": deepcopy(self._oms.__dict__),
        }

    def _restore_mutable_state(self, snapshot: Mapping[str, Any]) -> None:
        self._restore_ledger_state(snapshot["ledger"])
        self._records = deepcopy(snapshot["records"])
        self._intent_index = deepcopy(snapshot["intent_index"])
        self._oms.__dict__.clear()
        self._oms.__dict__.update(deepcopy(snapshot["oms_state"]))

    def _require_active_batch(self, plan_id: str) -> None:
        if self._batch_snapshot is None or self._batch_plan_id != plan_id:
            raise BrokerSubmitError(
                "LocalSim plan submission transaction does not match",
                context={"active_plan_id": self._batch_plan_id, "plan_id": plan_id},
            )

    def _ensure_alive(self) -> None:
        if self._closed:
            raise BrokerConnectivityError(
                "LocalSimBackend has been shut down",
                context={
                    "backend_id": self.backend_id,
                    "portfolio_id": self._portfolio_id,
                },
            )

    def _algo_requires_day_features(self) -> bool:
        algo_code = str(self._execution_policy.get("algo_code") or "").strip().upper()
        return algo_code in {"V25_TWO_STAGE", "V25_1_SMALL_CAP"}

    @staticmethod
    def _resolve_execution_policy(
        *,
        manifest: StrategyPackageManifest,
        execution_policy: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        if execution_policy:
            payload = dict(execution_policy)
            policy_json = payload.get("policy_json") if isinstance(payload.get("policy_json"), dict) else payload
            return normalize_execution_policy_json(dict(policy_json))

        minute_policy = getattr(manifest, "minute_execution_policy", None)
        if minute_policy is not None:
            return normalize_execution_policy_json(minute_policy.model_dump(mode="json"))

        raise RuntimeConfigInvalidError(
            "LocalSim execution requires a validated execution policy snapshot",
            context={
                "package_id": manifest.package_id,
                "manifest_sha256": manifest.manifest_sha256,
                "manifest_version": manifest.manifest_version,
            },
        )

    def _build_status(self, handle_id: str, order: Order) -> OrderHandleStatus:
        state = _ORDER_STATUS_TO_HANDLE_STATE.get(order.status)
        if state is None:
            raise BrokerSubmitError(
                "LocalSim received an unexpected OMS order status",
                context={"handle_id": handle_id, "order_status": order.status.value},
            )
        return OrderHandleStatus(
            handle_id=handle_id,
            state=state,
            filled_quantity=order.filled_quantity,
            avg_fill_price=(
                Decimal(repr(order.avg_fill_price))
                if order.avg_fill_price is not None
                else None
            ),
            last_event_at=order.updated_at,
            rejection_reason=None,
        )

    def _dispatch_fill(self, event: FillEvent) -> None:
        with self._lock:
            subscribers: Iterable[Callable[[FillEvent], None]] = list(self._subscribers.values())
        for cb in subscribers:
            cb(event)


_ORDER_STATUS_TO_HANDLE_STATE: dict[OrderStatus, OrderHandleStatusState] = {
    OrderStatus.PENDING: "pending",
    OrderStatus.SUBMITTED: "pending",
    OrderStatus.PARTIALLY_FILLED: "partial_filled",
    OrderStatus.FILLED: "filled",
    OrderStatus.CANCELLED: "cancelled",
    OrderStatus.REJECTED: "rejected",
}
