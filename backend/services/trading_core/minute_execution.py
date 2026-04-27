"""Minute-line execution engine for paper trading v2."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .errors import DataUnavailableError, ExecutionAlgoError, UnsupportedFeatureError
from .execution_algo_adapter import ExecutionAlgoAdapter
from .models import Fill, MinuteBar, Order, OrderEvent, OrderEventType, OrderStatus
from .oms import OMS
from .risk import RiskEngine


class MinuteExecutionEngine:
    """Execute an order over minute bars.

    This engine has no daily fallback. Missing minute bars or unsupported
    algorithms fail the run explicitly.
    """

    def __init__(
        self,
        adapter: ExecutionAlgoAdapter | None = None,
        oms: OMS | None = None,
        risk_engine: RiskEngine | None = None,
    ) -> None:
        self.adapter = adapter or ExecutionAlgoAdapter()
        self.oms = oms or OMS()
        self.risk_engine = risk_engine or RiskEngine()

    def execute_order(
        self,
        *,
        order: Order,
        minute_bars: list[MinuteBar],
        algo_code: str,
        algo_config: dict[str, Any] | None = None,
        market_context: dict[str, Any] | None = None,
        allow_partial_fill: bool = True,
    ) -> tuple[Order, list[Fill], list[OrderEvent]]:
        if order.status not in {OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED}:
            raise ExecutionAlgoError(
                "order must be submitted before minute execution",
                context={"order_id": order.order_id, "status": order.status.value},
            )
        config = dict(algo_config or {})
        algo, state = self.adapter.create_state(order, algo_code, config)
        market_state_aware = bool(getattr(algo, "HANDLES_MARKET_STATE", False))
        if not minute_bars:
            raise DataUnavailableError(
                "minute bars are required for paper trading",
                context={"order_id": order.order_id, "symbol": order.symbol},
            )
        self._validate_bars(order, minute_bars, require_executable=not market_state_aware)
        if not market_state_aware:
            self.risk_engine.validate_order_execution_context(
                order=order,
                minute_bars=minute_bars,
            )
        max_participation_rate = config.get("max_participation_rate")
        if max_participation_rate is not None:
            max_participation_rate = float(max_participation_rate)
            if max_participation_rate <= 0 or max_participation_rate > 1:
                raise ExecutionAlgoError(
                    "max_participation_rate must be in (0, 1]",
                    context={"order_id": order.order_id, "value": max_participation_rate},
                )

        current_order = order
        fills: list[Fill] = []
        events: list[OrderEvent] = []
        context = dict(market_context or {})

        for bar in sorted(minute_bars, key=lambda item: item.bar_time):
            if current_order.status == OrderStatus.FILLED:
                break
            step_fill = self.adapter.compute_step(
                algo=algo,
                state=state,
                bar=bar,
                market_context=context,
            )
            if step_fill is None:
                if market_state_aware and getattr(algo, "_last_no_fill_reason", None):
                    events.append(self._no_fill_event(current_order, algo, bar.bar_time))
                continue
            if max_participation_rate is not None:
                max_qty = int(bar.volume * max_participation_rate)
                max_qty = (max_qty // 100) * 100
                if max_qty <= 0:
                    raise ExecutionAlgoError(
                        "max_participation_rate leaves no executable round lot",
                        context={
                            "order_id": order.order_id,
                            "bar_time": bar.bar_time.isoformat(),
                            "bar_volume": bar.volume,
                            "max_participation_rate": max_participation_rate,
                        },
                    )
                if step_fill.quantity > max_qty:
                    raise ExecutionAlgoError(
                        "execution algorithm exceeds max_participation_rate",
                        context={
                            "order_id": order.order_id,
                            "bar_time": bar.bar_time.isoformat(),
                            "step_quantity": step_fill.quantity,
                            "max_quantity": max_qty,
                            "max_participation_rate": max_participation_rate,
                        },
                    )
            if step_fill.quantity > current_order.remaining_quantity:
                raise ExecutionAlgoError(
                    "step fill exceeds order remaining quantity",
                    context={
                        "order_id": current_order.order_id,
                        "step_quantity": step_fill.quantity,
                        "remaining_quantity": current_order.remaining_quantity,
                    },
                )
            self.risk_engine.validate_step_fill(
                order=current_order,
                step_fill=step_fill,
                bar=bar,
            )
            fill = Fill(
                order_id=current_order.order_id,
                symbol=step_fill.symbol,
                side=step_fill.side,
                quantity=step_fill.quantity,
                price=step_fill.price,
                trade_time=step_fill.bar_time,
                bar_time=step_fill.bar_time,
                reason=step_fill.reason,
                metadata=step_fill.metadata,
            )
            current_order, event = self.oms.apply_fill(current_order, fill)
            fills.append(fill)
            events.append(event)

        if not fills and market_state_aware and events:
            return current_order, fills, events
        if not fills and market_state_aware and getattr(algo, "_last_no_fill_reason", None):
            events.append(self._no_fill_event(current_order, algo, minute_bars[-1].bar_time))
            return current_order, fills, events
        if not fills:
            raise ExecutionAlgoError(
                "minute execution produced no fills",
                context={
                    "order_id": order.order_id,
                    "symbol": order.symbol,
                    "algo_code": algo_code,
                    "bar_count": len(minute_bars),
                },
            )
        if current_order.status != OrderStatus.FILLED and not allow_partial_fill:
            raise ExecutionAlgoError(
                "minute execution left unfilled quantity",
                context={
                    "order_id": current_order.order_id,
                    "remaining_quantity": current_order.remaining_quantity,
                },
            )
        return current_order, fills, events

    def execute_order_incremental(
        self,
        *,
        order: Order,
        execution_state: Any,
        new_bars: list[MinuteBar],
        algo_code: str,
        algo_config: dict[str, Any] | None = None,
        market_context: dict[str, Any] | None = None,
    ) -> tuple[Order, Any, list[Fill], list[OrderEvent]]:
        """Execute only newly observed minute bars and return updated state.

        Unlike the closed-day API, an incremental tick may legitimately produce
        zero fills while still advancing the per-order minute cursor. Repeated
        ticks stay idempotent because callers must persist
        ``last_processed_bar_time`` and pass only strictly newer bars.
        """

        if order.status not in {OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED}:
            raise ExecutionAlgoError(
                "order must be active before incremental minute execution",
                context={"order_id": order.order_id, "status": order.status.value},
            )
        if not new_bars:
            return order, execution_state, [], []
        config = dict(algo_config or {})
        algo, state = self.adapter.create_state(order, algo_code, config)
        self._restore_algo_state(state, execution_state)
        self._restore_persisted_plan(algo, execution_state)
        market_state_aware = bool(getattr(algo, "HANDLES_MARKET_STATE", False))
        self._validate_bars(order, new_bars, require_executable=not market_state_aware)
        if execution_state.order_id != order.order_id:
            raise ExecutionAlgoError(
                "execution state order_id does not match order",
                context={
                    "order_id": order.order_id,
                    "execution_state_order_id": execution_state.order_id,
                },
            )
        last_processed = getattr(execution_state, "last_processed_bar_time", None)
        for bar in sorted(new_bars, key=lambda item: item.bar_time):
            if last_processed is not None and bar.bar_time <= last_processed:
                raise DataUnavailableError(
                    "incremental minute bars must be strictly after execution cursor",
                    context={
                        "order_id": order.order_id,
                        "bar_time": bar.bar_time.isoformat(),
                        "last_processed_bar_time": last_processed.isoformat(),
                    },
                )
        max_participation_rate = config.get("max_participation_rate")
        if max_participation_rate is not None:
            max_participation_rate = float(max_participation_rate)
            if max_participation_rate <= 0 or max_participation_rate > 1:
                raise ExecutionAlgoError(
                    "max_participation_rate must be in (0, 1]",
                    context={"order_id": order.order_id, "value": max_participation_rate},
                )

        current_order = order
        fills: list[Fill] = []
        events: list[OrderEvent] = []
        context = dict(market_context or {})
        processed_time = last_processed

        for bar in sorted(new_bars, key=lambda item: item.bar_time):
            if current_order.status == OrderStatus.FILLED:
                processed_time = bar.bar_time
                continue
            if not market_state_aware:
                self.risk_engine.validate_order_execution_context(
                    order=current_order,
                    minute_bars=[bar],
                )
            step_fill = self.adapter.compute_step(
                algo=algo,
                state=state,
                bar=bar,
                market_context=context,
            )
            processed_time = bar.bar_time
            if step_fill is None:
                if market_state_aware and getattr(algo, "_last_no_fill_reason", None):
                    events.append(
                        self._no_fill_event(
                            current_order,
                            algo,
                            bar.bar_time,
                            event_id=self._incremental_event_id(current_order.order_id, bar.bar_time, suffix="NOFILL"),
                        )
                    )
                continue
            if max_participation_rate is not None:
                max_qty = int(bar.volume * max_participation_rate)
                max_qty = (max_qty // 100) * 100
                if max_qty <= 0:
                    raise ExecutionAlgoError(
                        "max_participation_rate leaves no executable round lot",
                        context={
                            "order_id": order.order_id,
                            "bar_time": bar.bar_time.isoformat(),
                            "bar_volume": bar.volume,
                            "max_participation_rate": max_participation_rate,
                        },
                    )
                if step_fill.quantity > max_qty:
                    raise ExecutionAlgoError(
                        "execution algorithm exceeds max_participation_rate",
                        context={
                            "order_id": order.order_id,
                            "bar_time": bar.bar_time.isoformat(),
                            "step_quantity": step_fill.quantity,
                            "max_quantity": max_qty,
                            "max_participation_rate": max_participation_rate,
                        },
                    )
            if step_fill.quantity > current_order.remaining_quantity:
                raise ExecutionAlgoError(
                    "step fill exceeds order remaining quantity",
                    context={
                        "order_id": current_order.order_id,
                        "step_quantity": step_fill.quantity,
                        "remaining_quantity": current_order.remaining_quantity,
                    },
                )
            self.risk_engine.validate_step_fill(
                order=current_order,
                step_fill=step_fill,
                bar=bar,
            )
            fill = Fill(
                fill_id=self._incremental_fill_id(current_order.order_id, bar.bar_time),
                order_id=current_order.order_id,
                symbol=step_fill.symbol,
                side=step_fill.side,
                quantity=step_fill.quantity,
                price=step_fill.price,
                trade_time=step_fill.bar_time,
                bar_time=step_fill.bar_time,
                reason=step_fill.reason,
                metadata=step_fill.metadata,
            )
            current_order, event = self.oms.apply_fill(current_order, fill)
            events.append(
                event.model_copy(
                    update={
                        "event_id": self._incremental_event_id(current_order.order_id, bar.bar_time),
                    }
                )
            )
            fills.append(fill)

        updated_state = execution_state.model_copy(
            update={
                "algo_state": self._dump_algo_state(state),
                "plan": self._dump_persisted_plan(algo, execution_state),
                "last_processed_bar_time": processed_time,
                "filled_quantity": current_order.filled_quantity,
                "remaining_quantity": current_order.remaining_quantity,
                "status": current_order.status.value,
            }
        )
        return current_order, updated_state, fills, events

    def _validate_bars(
        self,
        order: Order,
        minute_bars: list[MinuteBar],
        *,
        require_executable: bool = True,
    ) -> None:
        previous: datetime | None = None
        for bar in sorted(minute_bars, key=lambda item: item.bar_time):
            if bar.symbol != order.symbol:
                raise DataUnavailableError(
                    "minute bar symbol does not match order",
                    context={
                        "order_id": order.order_id,
                        "order_symbol": order.symbol,
                        "bar_symbol": bar.symbol,
                    },
                )
            if previous and bar.bar_time <= previous:
                raise DataUnavailableError(
                    "minute bars must be strictly increasing",
                    context={"order_id": order.order_id, "bar_time": bar.bar_time.isoformat()},
                )
            previous = bar.bar_time
        if require_executable and all(bar.is_suspended or bar.volume <= 0 for bar in minute_bars):
            raise DataUnavailableError(
                "minute bars contain no executable volume",
                context={"order_id": order.order_id, "symbol": order.symbol},
            )

    @staticmethod
    def _restore_algo_state(state: Any, execution_state: Any) -> None:
        payload = getattr(execution_state, "algo_state", None) or {}
        state.executed_quantity = int(payload.get("executed_quantity", execution_state.filled_quantity))
        state.step = int(payload.get("step", 0))
        state.is_complete = bool(payload.get("is_complete", execution_state.remaining_quantity <= 0))

    @staticmethod
    def _dump_algo_state(state: Any) -> dict[str, Any]:
        return {
            "total_quantity": int(state.total_quantity),
            "executed_quantity": int(state.executed_quantity),
            "step": int(state.step),
            "is_complete": bool(state.is_complete),
        }

    @staticmethod
    def _restore_persisted_plan(algo: Any, execution_state: Any) -> None:
        plan = getattr(execution_state, "plan", None)
        if not isinstance(plan, dict) or "weights" not in plan or not hasattr(algo, "_plan"):
            return
        try:
            import numpy as np

            algo._plan = np.asarray(plan["weights"], dtype=np.float64)
            algo._plan_key = tuple(plan["plan_key"]) if isinstance(plan.get("plan_key"), list) else None
            if isinstance(plan.get("metadata"), dict) and hasattr(algo, "_plan_metadata"):
                algo._plan_metadata = dict(plan["metadata"])
        except Exception as exc:
            raise ExecutionAlgoError(
                "persisted execution plan is invalid",
                context={"order_id": execution_state.order_id, "reason": f"{type(exc).__name__}: {exc}"},
            ) from exc

    @staticmethod
    def _dump_persisted_plan(algo: Any, execution_state: Any) -> dict[str, Any] | None:
        plan = getattr(algo, "_plan", None)
        if plan is None:
            return getattr(execution_state, "plan", None)
        try:
            weights = [float(item) for item in plan.tolist()]
        except AttributeError:
            weights = [float(item) for item in plan]
        plan_key = getattr(algo, "_plan_key", None)
        metadata = getattr(algo, "_plan_metadata", None)
        return {
            "weights": weights,
            "plan_key": list(plan_key) if isinstance(plan_key, tuple) else plan_key,
            "metadata": dict(metadata) if isinstance(metadata, dict) else {},
        }

    @staticmethod
    def _no_fill_event(
        order: Order,
        algo: Any,
        bar_time: datetime,
        *,
        event_id: str | None = None,
    ) -> OrderEvent:
        reason = str(getattr(algo, "_last_no_fill_reason", None) or "no_fill")
        context = getattr(algo, "_last_no_fill_context", None)
        metadata = {
            "algo_code": getattr(algo, "ALGO_CODE", None),
            "no_fill_context": dict(context) if isinstance(context, dict) else {},
        }
        payload: dict[str, Any] = {
            "order_id": order.order_id,
            "event_type": OrderEventType.NO_FILL,
            "event_time": bar_time,
            "reason": reason,
            "metadata": metadata,
        }
        if event_id is not None:
            payload["event_id"] = event_id
        return OrderEvent(**payload)

    @staticmethod
    def _incremental_fill_id(order_id: str, bar_time: datetime) -> str:
        return f"fill_{order_id}_{bar_time.strftime('%Y%m%d%H%M%S')}"

    @staticmethod
    def _incremental_event_id(order_id: str, bar_time: datetime, *, suffix: str | None = None) -> str:
        base = f"evt_{order_id}_{bar_time.strftime('%Y%m%d%H%M%S')}"
        return f"{base}_{suffix}" if suffix else base
