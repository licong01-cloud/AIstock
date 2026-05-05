"""Qlib Exchange patch for stock-aware A-share board-lot rounding.

Qlib's built-in ``trade_unit`` is global (usually 100 raw shares). V25.1 must
disable that global rounding so STAR-market 200-min/1-share child orders are
not rounded back to 100-share lots, but Qlib can still cash-clip a BUY order to
an illegal partial amount. This Exchange subclass keeps target-position sizing
unrounded and applies stock-aware board-lot rounding only while an actual child
order is being dealt.
"""
from __future__ import annotations

import math

from qlib.backtest.decision import Order
from qlib.backtest.exchange import Exchange as _QlibExchange


def _board_lot_rule(stock_id: str) -> tuple[int, int]:
    code = str(stock_id).split(".")[0].strip()
    if code.startswith(("688", "689")):
        return 200, 1
    if code.startswith(("300", "301", "302", "60", "00")):
        return 100, 100
    raise ValueError(f"board-lot exchange does not recognise A-share code: {stock_id!r}")


def _floor_to_increment(raw_qty: float, increment: int) -> int:
    return int(math.floor(max(float(raw_qty), 0.0) / increment + 1e-9)) * increment


def _legal_raw_child_qty(stock_id: str, side: str, raw_qty: float) -> int:
    min_qty, increment = _board_lot_rule(stock_id)
    raw_floor = _floor_to_increment(raw_qty, increment)
    if raw_floor >= min_qty:
        return raw_floor
    # Full residual sells bypass Qlib's rounding branch because the order
    # amount equals the current position. If Qlib is asking us to round, this
    # is a partial child order and must not become an illegal odd-lot fill.
    return 0


class BoardLotExchange(_QlibExchange):
    """Exchange with stock-aware final child-order amount rounding."""

    def __init__(self, *args, board_lot_trade_unit: bool = True, **kwargs) -> None:
        self.board_lot_trade_unit = bool(board_lot_trade_unit)
        if self.board_lot_trade_unit:
            # Disable Qlib's global 100-share trade unit; this subclass applies
            # the stock-aware rule in ``round_amount_by_trade_unit`` instead.
            kwargs["trade_unit"] = None
        super().__init__(*args, **kwargs)

    def _calc_trade_info_by_order(self, order, position, dealt_order_amount):
        self._board_lot_current_order = order
        try:
            return super()._calc_trade_info_by_order(order, position, dealt_order_amount)
        finally:
            self._board_lot_current_order = None

    def round_amount_by_trade_unit(
        self,
        deal_amount: float,
        factor: float | None = None,
        stock_id: str | None = None,
        start_time=None,
        end_time=None,
    ) -> float:
        if not self.board_lot_trade_unit:
            return super().round_amount_by_trade_unit(
                deal_amount,
                factor=factor,
                stock_id=stock_id,
                start_time=start_time,
                end_time=end_time,
            )

        order = getattr(self, "_board_lot_current_order", None)
        if order is None:
            # Target-position generation happens before child execution and has
            # no order/side context. V25.1 legalizes the emitted child orders.
            return deal_amount

        stock_id = stock_id or order.stock_id
        factor = self._get_factor_or_raise_error(
            factor=factor,
            stock_id=stock_id,
            start_time=start_time,
            end_time=end_time,
        )
        side = "BUY" if order.direction == Order.BUY else "SELL"
        legal_raw = _legal_raw_child_qty(stock_id, side, float(deal_amount) * float(factor))
        return float(legal_raw) / float(factor) if legal_raw > 0 else 0.0


def install_board_lot_exchange_patch() -> None:
    """Install ``BoardLotExchange`` into qlib.backtest for the current process."""

    import qlib.backtest as backtest_module
    import qlib.backtest.exchange as exchange_module

    exchange_module.Exchange = BoardLotExchange
    backtest_module.Exchange = BoardLotExchange
