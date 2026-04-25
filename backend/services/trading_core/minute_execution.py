"""Minute-line execution engine for paper trading v2."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .errors import DataUnavailableError, ExecutionAlgoError, UnsupportedFeatureError
from .execution_algo_adapter import ExecutionAlgoAdapter
from .models import Fill, MinuteBar, Order, OrderEvent, OrderStatus
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
        if not minute_bars:
            raise DataUnavailableError(
                "minute bars are required for paper trading",
                context={"order_id": order.order_id, "symbol": order.symbol},
            )
        self._validate_bars(order, minute_bars)
        self.risk_engine.validate_order_execution_context(
            order=order,
            minute_bars=minute_bars,
        )

        config = dict(algo_config or {})
        algo, state = self.adapter.create_state(order, algo_code, config)
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

    def _validate_bars(self, order: Order, minute_bars: list[MinuteBar]) -> None:
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
        if all(bar.is_suspended or bar.volume <= 0 for bar in minute_bars):
            raise DataUnavailableError(
                "minute bars contain no executable volume",
                context={"order_id": order.order_id, "symbol": order.symbol},
            )
