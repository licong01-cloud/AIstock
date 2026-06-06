"""BestLimit execution core derived from vnpy_algotrading best_limit_algo.py.

Upstream commit: 4133987530eb28f3538d1983545d81c4f83d7d59.
Original license: MIT License, Copyright (c) 2015-present, Xiaoyou Chen.
AIstock changes: inject deterministic random provider and return action DTOs;
broker authority, order persistence, and risk checks remain adapter-side.
"""

from __future__ import annotations

from typing import Callable

from .base import VnpyAlgoTemplate, VnpyStyleConfigError
from .models import VnpyDirection, VnpyOrderUpdate, VnpyTick, VnpyTradeUpdate

RandomVolumeProvider = Callable[[int, int], float]


def _default_uniform(min_volume: int, max_volume: int) -> float:
    from random import uniform

    return uniform(min_volume, max_volume)


class BestLimitMiniQMTCore(VnpyAlgoTemplate):
    ALGO_CODE = "BEST_LIMIT_MINIQMT"
    display_name = "BestLimit MiniQMT"
    default_setting = {"min_volume": 0, "max_volume": 0}
    variables = ["vt_orderid", "order_price"]

    def __init__(self, config, *, random_volume_provider: RandomVolumeProvider | None = None) -> None:
        super().__init__(config)
        self.min_volume = int(config.setting.get("min_volume", self.min_volume) or self.min_volume)
        self.max_volume = int(config.setting.get("max_volume", self.min_volume) or 0)
        self._random_volume_provider = random_volume_provider or _default_uniform
        self.vt_orderid = ""
        self.order_price = 0.0
        if self.min_volume <= 0:
            raise VnpyStyleConfigError("BEST_LIMIT_MINIQMT requires min_volume > 0")
        if self.max_volume < self.min_volume:
            raise VnpyStyleConfigError("BEST_LIMIT_MINIQMT requires max_volume >= min_volume")

    def on_tick(self, tick: VnpyTick) -> None:
        if self.direction == VnpyDirection.LONG:
            if not self.vt_orderid:
                self.buy_best_limit(tick.bid_price_1)
            elif self.order_price != tick.bid_price_1:
                self.cancel_all(reason="best_limit_bid_price_changed")
        else:
            if not self.vt_orderid:
                self.sell_best_limit(tick.ask_price_1)
            elif self.order_price != tick.ask_price_1:
                self.cancel_all(reason="best_limit_ask_price_changed")

    def on_trade(self, trade: VnpyTradeUpdate) -> None:
        if self.traded >= self.volume:
            self.finish(reason="best_limit_target_volume_filled")
        else:
            self.write_log("best_limit_trade_update_partial")

    def on_order(self, order: VnpyOrderUpdate) -> None:
        if not order.is_active():
            self.vt_orderid = ""
            self.order_price = 0.0

    def buy_best_limit(self, bid_price_1: float) -> None:
        volume_left = self.volume - self.traded
        rand_volume = self.generate_rand_volume()
        order_volume = min(rand_volume, volume_left)
        self.order_price = float(bid_price_1)
        self.vt_orderid = self.buy(self.order_price, order_volume, reason="best_limit_buy_at_bid_price_1")

    def sell_best_limit(self, ask_price_1: float) -> None:
        volume_left = self.volume - self.traded
        rand_volume = self.generate_rand_volume()
        order_volume = min(rand_volume, volume_left)
        self.order_price = float(ask_price_1)
        self.vt_orderid = self.sell(self.order_price, order_volume, reason="best_limit_sell_at_ask_price_1")

    def generate_rand_volume(self) -> int:
        rand_volume = self._random_volume_provider(self.min_volume, self.max_volume)
        return int(rand_volume)
