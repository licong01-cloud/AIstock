"""Sniper execution core derived from vnpy_algotrading sniper_algo.py.

Upstream commit: 4133987530eb28f3538d1983545d81c4f83d7d59.
Original license: MIT License, Copyright (c) 2015-present, Xiaoyou Chen.
AIstock changes: returns action DTOs instead of calling AlgoEngine, keeps broker
submission/cancel outside this core, and uses AIstock direction/DTO types.
"""

from __future__ import annotations

from .base import VnpyAlgoTemplate
from .models import VnpyDirection, VnpyOrderUpdate, VnpyTick, VnpyTradeUpdate


class SniperMiniQMTCore(VnpyAlgoTemplate):
    ALGO_CODE = "SNIPER_MINIQMT"
    display_name = "Sniper MiniQMT"
    default_setting: dict = {}
    variables = ["vt_orderid"]

    def __init__(self, config) -> None:
        super().__init__(config)
        self.vt_orderid = ""

    def on_tick(self, tick: VnpyTick) -> None:
        if self.vt_orderid:
            self.cancel_all(reason="sniper_active_order_cancel_before_requote")
            return

        if self.direction == VnpyDirection.LONG:
            if tick.ask_price_1 <= self.price:
                order_volume = self.volume - self.traded
                order_volume = min(order_volume, int(tick.ask_volume_1))
                self.vt_orderid = self.buy(self.price, order_volume, reason="sniper_ask_crossed_limit")
        else:
            if tick.bid_price_1 >= self.price:
                order_volume = self.volume - self.traded
                order_volume = min(order_volume, int(tick.bid_volume_1))
                self.vt_orderid = self.sell(self.price, order_volume, reason="sniper_bid_crossed_limit")

    def on_order(self, order: VnpyOrderUpdate) -> None:
        if not order.is_active():
            self.vt_orderid = ""

    def on_trade(self, trade: VnpyTradeUpdate) -> None:
        if self.traded >= self.volume:
            self.finish(reason="sniper_target_volume_filled")
        else:
            self.write_log("sniper_trade_update_partial")
