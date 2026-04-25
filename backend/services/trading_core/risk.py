"""Fail-fast A-share risk checks for Trading Core v2.

The rules are intentionally conservative for authoritative paper trading:
missing limit prices, suspended bars, or non-executable limit states must fail
instead of being treated as zero fills or skipped orders.
"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import DataUnavailableError, RiskRuleError
from .limit_price_provider import DailyLimitPrice, StkLimitPriceProvider
from .models import MinuteBar, Order, OrderSide, OrderType, StepFill


LIMIT_EPSILON = 1e-8


@dataclass(frozen=True)
class RiskDecision:
    passed: bool
    reason: str


class RiskEngine:
    """Validate market rules before OMS/execution/ledger mutation."""

    def __init__(self, limit_price_provider: StkLimitPriceProvider | None = None) -> None:
        self.limit_price_provider = limit_price_provider

    def validate_order_execution_context(
        self,
        *,
        order: Order,
        minute_bars: list[MinuteBar],
    ) -> RiskDecision:
        if not minute_bars:
            raise DataUnavailableError(
                "minute bars are required for risk validation",
                context={"order_id": order.order_id, "symbol": order.symbol},
            )
        limit_price = self._resolve_limit_price(order, minute_bars)
        executable_bars = [
            bar for bar in minute_bars
            if not bar.is_suspended and bar.volume > 0
        ]
        if not executable_bars:
            raise RiskRuleError(
                "order has no executable minute bars",
                context={"order_id": order.order_id, "symbol": order.symbol},
            )

        for bar in minute_bars:
            self._validate_bar_limit_consistency(order, bar, limit_price)

        if order.order_type == OrderType.LIMIT and order.limit_price is not None:
            self._validate_limit_order_price(order, limit_price)

        if order.side == OrderSide.BUY and not any(
            not self._is_limit_up(bar, limit_price) for bar in executable_bars
        ):
            raise RiskRuleError(
                "buy order is blocked because all executable bars are at limit up",
                context={
                    "order_id": order.order_id,
                    "symbol": order.symbol,
                    "trade_date": executable_bars[0].bar_time.date().isoformat(),
                    "up_limit": limit_price.up_limit,
                },
            )
        if order.side == OrderSide.SELL and not any(
            not self._is_limit_down(bar, limit_price) for bar in executable_bars
        ):
            raise RiskRuleError(
                "sell order is blocked because all executable bars are at limit down",
                context={
                    "order_id": order.order_id,
                    "symbol": order.symbol,
                    "trade_date": executable_bars[0].bar_time.date().isoformat(),
                    "down_limit": limit_price.down_limit,
                },
            )
        return RiskDecision(passed=True, reason="order execution context passed")

    def validate_step_fill(
        self,
        *,
        order: Order,
        step_fill: StepFill,
        bar: MinuteBar,
    ) -> RiskDecision:
        if bar.symbol != order.symbol:
            raise RiskRuleError(
                "risk validation bar symbol does not match order",
                context={
                    "order_id": order.order_id,
                    "order_symbol": order.symbol,
                    "bar_symbol": bar.symbol,
                },
            )
        if bar.is_suspended:
            raise RiskRuleError(
                "cannot fill on a suspended minute bar",
                context={
                    "order_id": order.order_id,
                    "symbol": order.symbol,
                    "bar_time": bar.bar_time.isoformat(),
                },
            )
        if bar.volume <= 0:
            raise RiskRuleError(
                "cannot fill on a zero-volume minute bar",
                context={
                    "order_id": order.order_id,
                    "symbol": order.symbol,
                    "bar_time": bar.bar_time.isoformat(),
                },
            )
        if step_fill.quantity > bar.volume:
            raise RiskRuleError(
                "step fill quantity exceeds minute bar volume",
                context={
                    "order_id": order.order_id,
                    "symbol": order.symbol,
                    "bar_time": bar.bar_time.isoformat(),
                    "step_quantity": step_fill.quantity,
                    "bar_volume": bar.volume,
                },
            )

        limit_price = self._resolve_limit_price(order, [bar])
        if step_fill.side == OrderSide.BUY and step_fill.price >= limit_price.up_limit - LIMIT_EPSILON:
            raise RiskRuleError(
                "buy fill is blocked at limit up",
                context={
                    "order_id": order.order_id,
                    "symbol": order.symbol,
                    "bar_time": bar.bar_time.isoformat(),
                    "fill_price": step_fill.price,
                    "up_limit": limit_price.up_limit,
                },
            )
        if step_fill.side == OrderSide.SELL and step_fill.price <= limit_price.down_limit + LIMIT_EPSILON:
            raise RiskRuleError(
                "sell fill is blocked at limit down",
                context={
                    "order_id": order.order_id,
                    "symbol": order.symbol,
                    "bar_time": bar.bar_time.isoformat(),
                    "fill_price": step_fill.price,
                    "down_limit": limit_price.down_limit,
                },
            )
        return RiskDecision(passed=True, reason="step fill passed")

    def _resolve_limit_price(
        self,
        order: Order,
        minute_bars: list[MinuteBar],
    ) -> DailyLimitPrice:
        first_bar = minute_bars[0]
        if first_bar.limit_up is not None and first_bar.limit_down is not None:
            return self._limit_from_bar(order, first_bar)
        if self.limit_price_provider is None:
            raise DataUnavailableError(
                "limit price is required for risk validation",
                context={
                    "order_id": order.order_id,
                    "symbol": order.symbol,
                    "trade_date": first_bar.bar_time.date().isoformat(),
                },
            )
        return self.limit_price_provider.get_limit_price(
            order.symbol,
            first_bar.bar_time.date(),
        )

    def _limit_from_bar(self, order: Order, bar: MinuteBar) -> DailyLimitPrice:
        if bar.limit_up is None or bar.limit_down is None:
            raise DataUnavailableError(
                "minute bar is missing limit price fields",
                context={
                    "order_id": order.order_id,
                    "symbol": bar.symbol,
                    "bar_time": bar.bar_time.isoformat(),
                },
            )
        if bar.limit_down >= bar.limit_up:
            raise DataUnavailableError(
                "minute bar has invalid limit price range",
                context={
                    "order_id": order.order_id,
                    "symbol": bar.symbol,
                    "bar_time": bar.bar_time.isoformat(),
                    "limit_up": bar.limit_up,
                    "limit_down": bar.limit_down,
                },
            )
        return DailyLimitPrice(
            symbol=bar.symbol,
            trade_date=bar.bar_time.date(),
            pre_close=None,
            up_limit=bar.limit_up,
            down_limit=bar.limit_down,
        )

    def _validate_bar_limit_consistency(
        self,
        order: Order,
        bar: MinuteBar,
        limit_price: DailyLimitPrice,
    ) -> None:
        if bar.limit_up is not None or bar.limit_down is not None:
            bar_limit = self._limit_from_bar(order, bar)
            if abs(bar_limit.up_limit - limit_price.up_limit) > LIMIT_EPSILON:
                raise DataUnavailableError(
                    "minute bars have inconsistent up_limit values",
                    context={
                        "order_id": order.order_id,
                        "symbol": order.symbol,
                        "bar_time": bar.bar_time.isoformat(),
                        "expected": limit_price.up_limit,
                        "actual": bar_limit.up_limit,
                    },
                )
            if abs(bar_limit.down_limit - limit_price.down_limit) > LIMIT_EPSILON:
                raise DataUnavailableError(
                    "minute bars have inconsistent down_limit values",
                    context={
                        "order_id": order.order_id,
                        "symbol": order.symbol,
                        "bar_time": bar.bar_time.isoformat(),
                        "expected": limit_price.down_limit,
                        "actual": bar_limit.down_limit,
                    },
                )
        if bar.high > limit_price.up_limit + LIMIT_EPSILON:
            raise DataUnavailableError(
                "minute bar high exceeds up_limit",
                context={
                    "order_id": order.order_id,
                    "symbol": order.symbol,
                    "bar_time": bar.bar_time.isoformat(),
                    "high": bar.high,
                    "up_limit": limit_price.up_limit,
                },
            )
        if bar.low < limit_price.down_limit - LIMIT_EPSILON:
            raise DataUnavailableError(
                "minute bar low is below down_limit",
                context={
                    "order_id": order.order_id,
                    "symbol": order.symbol,
                    "bar_time": bar.bar_time.isoformat(),
                    "low": bar.low,
                    "down_limit": limit_price.down_limit,
                },
            )

    def _validate_limit_order_price(
        self,
        order: Order,
        limit_price: DailyLimitPrice,
    ) -> None:
        assert order.limit_price is not None
        if order.side == OrderSide.BUY and order.limit_price >= limit_price.up_limit - LIMIT_EPSILON:
            raise RiskRuleError(
                "buy limit order price is blocked at limit up",
                context={
                    "order_id": order.order_id,
                    "symbol": order.symbol,
                    "limit_price": order.limit_price,
                    "up_limit": limit_price.up_limit,
                },
            )
        if order.side == OrderSide.SELL and order.limit_price <= limit_price.down_limit + LIMIT_EPSILON:
            raise RiskRuleError(
                "sell limit order price is blocked at limit down",
                context={
                    "order_id": order.order_id,
                    "symbol": order.symbol,
                    "limit_price": order.limit_price,
                    "down_limit": limit_price.down_limit,
                },
            )

    @staticmethod
    def _is_limit_up(bar: MinuteBar, limit_price: DailyLimitPrice) -> bool:
        return bar.close >= limit_price.up_limit - LIMIT_EPSILON

    @staticmethod
    def _is_limit_down(bar: MinuteBar, limit_price: DailyLimitPrice) -> bool:
        return bar.close <= limit_price.down_limit + LIMIT_EPSILON
