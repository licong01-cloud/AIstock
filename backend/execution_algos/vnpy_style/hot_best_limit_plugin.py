from __future__ import annotations

from typing import Any

from backend.services.miniqmt_execution_runtime.plugin_canonical import digest_bytes_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import AlgoStartContextV1, SideV1
from backend.execution_algos.hot_market_contracts import (
    HotMarketDataEconomicEffectV1,
    HotMarketDataViewV1,
)

from .hot_plugin_base import CurrentThreeHotPluginBaseV4, CurrentThreeHotTargetV4, _effect_v1


class BestLimitMiniQMTPluginV4(CurrentThreeHotPluginBaseV4):
    ALGO_CODE = "BEST_LIMIT_MINIQMT"
    ALGO_NAME = "BestLimit MiniQMT"

    def _specific_initial_state(self, context: AlgoStartContextV1) -> dict[str, Any]:
        minimum, maximum = self.config["min_volume"], self.config["max_volume"]
        return {
            "vt_orderid": None,
            "order_price_decimal": None,
            "next_draw_ordinal": 0,
            "parameters": {"min_volume": minimum, "max_volume": maximum},
            "variables": {"vt_orderid": None, "order_price_decimal": None, "next_draw_ordinal": 0},
        }

    def _sync_specific_active_state(self, state, items) -> None:
        state["vt_orderid"] = None if not items else items[0].local_vt_orderid
        state["order_price_decimal"] = None if not items else items[0].requested_price_decimal
        state["variables"] = {
            "vt_orderid": state["vt_orderid"],
            "order_price_decimal": state["order_price_decimal"],
            "next_draw_ordinal": state["next_draw_ordinal"],
        }

    def _after_hot_economic_action(self, state, effect, items) -> None:
        if effect["action"] == "SUBMIT_LIMIT":
            state["next_draw_ordinal"] += 1
        super()._after_hot_economic_action(state, effect, items)


class BestLimitHotTargetV4(CurrentThreeHotTargetV4):
    def evaluate_hot_market_data_v1(self, view: HotMarketDataViewV1) -> HotMarketDataEconomicEffectV1 | None:
        state = self._state()
        side = SideV1(state["side"])
        price = self._price(view, side, opposite=True)
        items = self._active(state)
        if items:
            if items[0].requested_price_decimal == format(price, "f"):
                return None
            return self._cancel_or_wait(view=view, state=state, reason_code="best_limit_quote_price_changed")
        minimum, maximum = state["parameters"]["min_volume"], state["parameters"]["max_volume"]
        raw = digest_bytes_v1(
            "miniqmt_best_limit_hot_draw_v1",
            {"algo_instance_id": self.algo_instance_id, "draw_ordinal": state["next_draw_ordinal"]},
        )
        unit = (int.from_bytes(raw[:7], "big") >> 3) / (2**53)
        quantity = min(int(minimum + (maximum - minimum) * unit), state["parent_quantity"] - state["traded_quantity"])
        if quantity <= 0:
            return None
        return _effect_v1(
            algo=self.algo,
            view=view,
            payload={
                "action": "SUBMIT_LIMIT",
                "symbol": state["symbol"],
                "side": side.value,
                "price_decimal": format(price, "f"),
                "quantity": quantity,
                "reason_code": "best_limit_buy_at_bid_price_1"
                if side is SideV1.BUY
                else "best_limit_sell_at_ask_price_1",
                "draw_ordinal": state["next_draw_ordinal"],
            },
        )


__all__ = ["BestLimitHotTargetV4", "BestLimitMiniQMTPluginV4"]
