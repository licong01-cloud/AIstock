"""TWAP-lite execution core derived from vnpy_algotrading twap_algo.py.

Upstream commit: 4133987530eb28f3538d1983545d81c4f83d7d59.
Original license: MIT License, Copyright (c) 2015-present, Xiaoyou Chen.
AIstock changes: timer/tick inputs are supplied by adapters, child orders are
returned as action DTOs, and board-lot rounding is explicit in core config.
"""

from __future__ import annotations

from .base import VnpyAlgoTemplate, VnpyStyleConfigError
from .models import VnpyDirection, VnpyTick, VnpyTradeUpdate


class TwapLiteMiniQMTCore(VnpyAlgoTemplate):
    ALGO_CODE = "TWAP_LITE_MINIQMT"
    display_name = "TWAP Lite MiniQMT"
    default_setting = {"time": 600, "interval": 60}
    variables = ["order_volume", "timer_count", "total_count"]

    def __init__(self, config) -> None:
        super().__init__(config)
        self.time = int(config.setting.get("time", config.setting.get("duration_seconds", 600)) or 600)
        self.interval = int(config.setting.get("interval", config.setting.get("interval_seconds", 60)) or 60)
        if self.time <= 0:
            raise VnpyStyleConfigError("TWAP_LITE_MINIQMT requires time > 0")
        if self.interval <= 0:
            raise VnpyStyleConfigError("TWAP_LITE_MINIQMT requires interval > 0")
        if self.time < self.interval:
            raise VnpyStyleConfigError("TWAP_LITE_MINIQMT requires time >= interval")
        raw_order_volume = self.volume / (self.time / self.interval)
        self.order_volume = self._round_to_min_volume(raw_order_volume)
        self.timer_count = 0
        self.total_count = 0

    def on_trade(self, trade: VnpyTradeUpdate) -> None:
        if self.traded >= self.volume:
            self.finish(reason="twap_lite_target_volume_filled")
        else:
            self.write_log("twap_lite_trade_update_partial")

    def on_timer(self) -> None:
        self.timer_count += 1
        self.total_count += 1

        if self.total_count >= self.time:
            self.finish(reason="twap_lite_total_time_exhausted")
            return

        if self.timer_count < self.interval:
            return
        self.timer_count = 0

        tick = self.get_tick()
        if not tick:
            self.write_log("twap_lite_waiting_for_tick")
            return

        self.cancel_all(reason="twap_lite_cancel_before_next_slice")

        left_volume = self.volume - self.traded
        order_volume = min(self.order_volume, left_volume)
        if order_volume <= 0:
            return

        if self.direction == VnpyDirection.LONG:
            if tick.ask_price_1 <= self.price:
                self.buy(self.price, order_volume, reason="twap_lite_interval_buy")
        else:
            if tick.bid_price_1 >= self.price:
                self.sell(self.price, order_volume, reason="twap_lite_interval_sell")

    def on_tick(self, tick: VnpyTick) -> None:
        return None

    def _round_to_min_volume(self, raw_volume: float) -> int:
        qty = int(raw_volume)
        if qty <= 0:
            return 0
        if qty < self.min_volume:
            return min(self.volume, self.min_volume)
        rounded = (qty // self.volume_increment) * self.volume_increment
        if rounded < self.min_volume:
            return min(self.volume, self.min_volume)
        return min(self.volume, rounded)
