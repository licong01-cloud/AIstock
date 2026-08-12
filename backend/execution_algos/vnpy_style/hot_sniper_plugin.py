from __future__ import annotations

from decimal import Decimal
from typing import Any

from backend.services.miniqmt_execution_runtime.plugin_contracts import AlgoStartContextV1, SideV1
from backend.execution_algos.hot_market_contracts import (
    HotMarketDataEconomicEffectV1,
    HotMarketDataViewV1,
)

from .hot_plugin_base import CurrentThreeHotPluginBaseV4, CurrentThreeHotTargetV4, _effect_v1


class SniperMiniQMTPluginV4(CurrentThreeHotPluginBaseV4):
    ALGO_CODE = "SNIPER_MINIQMT"
    ALGO_NAME = "Sniper MiniQMT"

    def _specific_initial_state(self, context: AlgoStartContextV1) -> dict[str, Any]:
        return {"vt_orderid": None, "variables": {"vt_orderid": None}}

    def _sync_specific_active_state(self, state, items) -> None:
        vt_orderid = None if not items else items[0].local_vt_orderid
        state["vt_orderid"] = vt_orderid
        state["variables"] = {"vt_orderid": vt_orderid}


class SniperHotTargetV4(CurrentThreeHotTargetV4):
    def evaluate_hot_market_data_v1(self, view: HotMarketDataViewV1) -> HotMarketDataEconomicEffectV1 | None:
        if not self._is_continuous_market_v1(view):
            return None
        state = self._state()
        if state["active_orders"]:
            return self._cancel_or_wait(view=view, state=state, reason_code="sniper_active_order_cancel_before_requote")
        side = SideV1(state["side"])
        quote = self._price(view, side)
        limit = Decimal(state["limit_price_decimal"])
        if not (quote <= limit if side is SideV1.BUY else quote >= limit):
            return None
        quantity = min(state["parent_quantity"] - state["traded_quantity"], self._volume(view, side))
        if quantity <= 0:
            return None
        return _effect_v1(
            algo=self.algo,
            view=view,
            payload={
                "action": "SUBMIT_LIMIT",
                "symbol": state["symbol"],
                "side": side.value,
                "price_decimal": state["limit_price_decimal"],
                "quantity": quantity,
                "reason_code": "sniper_ask_crossed_limit" if side is SideV1.BUY else "sniper_bid_crossed_limit",
            },
        )


__all__ = ["SniperHotTargetV4", "SniperMiniQMTPluginV4"]
