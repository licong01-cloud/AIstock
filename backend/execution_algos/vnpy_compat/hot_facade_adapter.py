"""V4 Iceberg/Stop plugins plus process-local hot decision targets."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from backend.execution_algos.vnpy_style.hot_plugin_base import (
    CurrentThreeHotPluginBaseV4,
    CurrentThreeHotTargetV4,
    _effect_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AlgoReadOnlyServicesV1,
    AlgoStartContextV1,
    RuntimeEventEnvelopeV2,
    SideV1,
    TerminalOutcomeV1,
)
from backend.execution_algos.hot_market_contracts import (
    HotMarketDataEconomicEffectV1,
    HotMarketDataViewV1,
)

from .hot_facade_contracts import hot_facade_manifests_v4, validate_hot_facade_config_v4
from backend.services.miniqmt_execution_runtime.plugin_canonical import thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_registry import PluginProcessBindingsV2
from backend.execution_algos.vnpy_style.plugin_base import EffectCollectorV3, plus_one_second_v1


class IcebergHotPluginV4(CurrentThreeHotPluginBaseV4):
    ALGO_CODE, ALGO_NAME, TIMER_NAME = "ICEBERG", "Iceberg", "ICEBERG_ACTIVE_SECOND"

    def _specific_initial_state(self, context: AlgoStartContextV1) -> dict[str, Any]:
        display = self.config["display_volume"]
        if type(display) is str:
            decimal_display = Decimal(display)
            if decimal_display != decimal_display.to_integral_value():
                raise ValueError("Iceberg display_volume must close to strict integer shares")
            display = int(decimal_display)
        if display <= 0:
            display = context.parent_quantity
        interval = max(1, self.config["interval"])
        return {
            "display_volume": display,
            "interval": interval,
            "timer_count": 0,
            "slice_ready": False,
            "vt_orderid": None,
            "parameters": {"display_volume": display, "interval": interval},
            "variables": {"timer_count": 0, "slice_ready": False, "vt_orderid": None},
        }

    def _initialize_effects(self, *, context: AlgoStartContextV1, collector: EffectCollectorV3) -> None:
        collector.timer(
            timer_name=self.TIMER_NAME,
            schedule_epoch=context.deterministic_context.session_epoch,
            raw_due_at_utc=plus_one_second_v1(context.deterministic_context.logical_time_utc),
        )

    def _handle_timer(
        self,
        state: dict[str, Any],
        event: RuntimeEventEnvelopeV2,
        services: AlgoReadOnlyServicesV1,
        collector: EffectCollectorV3,
    ) -> TerminalOutcomeV1 | None:
        payload = thaw_json_v1(event.payload)
        if payload["timer_name"] != self.TIMER_NAME:
            raise ValueError("Iceberg timer owner conflict")
        state["timer_count"] += 1
        if state["timer_count"] >= state["interval"]:
            state["timer_count"], state["slice_ready"] = 0, True
        collector.timer(
            timer_name=self.TIMER_NAME,
            schedule_epoch=payload["schedule_epoch"],
            raw_due_at_utc=plus_one_second_v1(collector.context.logical_time_utc),
        )
        self._sync_variables(state)
        return None

    def _after_hot_economic_action(self, state, effect, items) -> None:
        state["slice_ready"] = False
        self._sync_variables(state)

    def _sync_specific_active_state(self, state, items) -> None:
        state["vt_orderid"] = None if not items else items[0].local_vt_orderid
        self._sync_variables(state)

    @staticmethod
    def _sync_variables(state) -> None:
        state["variables"] = {key: state[key] for key in ("timer_count", "slice_ready", "vt_orderid")}


class StopHotPluginV4(CurrentThreeHotPluginBaseV4):
    ALGO_CODE, ALGO_NAME = "STOP", "Stop"

    def _specific_initial_state(self, context: AlgoStartContextV1) -> dict[str, Any]:
        add = self.config["price_add"]
        return {
            "price_add_decimal": add,
            "triggered": False,
            "vt_orderid": None,
            "order_status": None,
            "parameters": {"price_add": add},
            "variables": {"triggered": False, "vt_orderid": None, "order_status": None},
        }

    def _after_hot_economic_action(self, state, effect, items) -> None:
        if effect["action"] == "SUBMIT_LIMIT":
            state["triggered"] = True
        self._sync_specific_active_state(state, items)

    def _sync_specific_active_state(self, state, items) -> None:
        state["vt_orderid"] = None if not items else items[0].local_vt_orderid
        state["variables"] = {
            "triggered": state["triggered"],
            "vt_orderid": state["vt_orderid"],
            "order_status": state["order_status"],
        }


class IcebergHotTargetV4(CurrentThreeHotTargetV4):
    def evaluate_hot_market_data_v1(self, view: HotMarketDataViewV1) -> HotMarketDataEconomicEffectV1 | None:
        if not self._is_continuous_market_v1(view):
            return None
        state = self._state()
        if not state["slice_ready"]:
            return None
        items = self._active(state)
        side = SideV1(state["side"])
        limit = Decimal(state["limit_price_decimal"])
        if items:
            crossed = view.ask_price_1 <= limit if side is SideV1.BUY else view.bid_price_1 >= limit
            return (
                self._cancel_or_wait(view=view, state=state, reason_code="iceberg_active_order_crossed_cancel")
                if crossed
                else None
            )
        quantity = min(state["display_volume"], state["parent_quantity"] - state["traded_quantity"])
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
                "reason_code": "iceberg_interval_slice",
            },
        )


class StopHotTargetV4(CurrentThreeHotTargetV4):
    def evaluate_hot_market_data_v1(self, view: HotMarketDataViewV1) -> HotMarketDataEconomicEffectV1 | None:
        if not self._is_continuous_market_v1(view):
            return None
        state = self._state()
        if state["triggered"] or state["active_orders"] or view.last_price is None or view.last_price <= 0:
            return None
        side = SideV1(state["side"])
        trigger = Decimal(state["limit_price_decimal"])
        if not (view.last_price >= trigger if side is SideV1.BUY else view.last_price <= trigger):
            return None
        price = trigger + (
            Decimal(state["price_add_decimal"]) if side is SideV1.BUY else -Decimal(state["price_add_decimal"])
        )
        if side is SideV1.BUY and view.limit_up is not None:
            price = min(price, view.limit_up)
        if side is SideV1.SELL and view.limit_down is not None:
            price = max(price, view.limit_down)
        return _effect_v1(
            algo=self.algo,
            view=view,
            payload={
                "action": "SUBMIT_LIMIT",
                "symbol": state["symbol"],
                "side": side.value,
                "price_decimal": format(price, "f"),
                "quantity": state["parent_quantity"] - state["traded_quantity"],
                "reason_code": "stop_triggered",
            },
        )


def _create(algo_code: str, config: Mapping[str, Any]):
    manifest = next(item for item in hot_facade_manifests_v4() if item.algo_code == algo_code)
    canonical = thaw_json_v1(validate_hot_facade_config_v4(manifest, config))
    return (IcebergHotPluginV4 if algo_code == "ICEBERG" else StopHotPluginV4)(
        manifest=manifest, canonical_config=canonical
    )


def create_iceberg_hot_plugin_v4(canonical_plugin_config: Mapping[str, Any]) -> IcebergHotPluginV4:
    return _create("ICEBERG", canonical_plugin_config)


def create_stop_hot_plugin_v4(canonical_plugin_config: Mapping[str, Any]) -> StopHotPluginV4:
    return _create("STOP", canonical_plugin_config)


def hot_facade_process_bindings_v4() -> PluginProcessBindingsV2:
    from .hot_facade_contracts import validate_hot_facade_state_v4

    return PluginProcessBindingsV2(
        {
            "aistock.vnpy.iceberg.v4.factory": create_iceberg_hot_plugin_v4,
            "aistock.vnpy.stop.v4.factory": create_stop_hot_plugin_v4,
            "aistock.vnpy.iceberg.v4.config_validator": validate_hot_facade_config_v4,
            "aistock.vnpy.stop.v4.config_validator": validate_hot_facade_config_v4,
            "aistock.vnpy.iceberg.v4.state_codec": validate_hot_facade_state_v4,
            "aistock.vnpy.stop.v4.state_codec": validate_hot_facade_state_v4,
        }
    )


__all__ = [
    "IcebergHotPluginV4",
    "IcebergHotTargetV4",
    "StopHotPluginV4",
    "StopHotTargetV4",
    "create_iceberg_hot_plugin_v4",
    "create_stop_hot_plugin_v4",
    "hot_facade_process_bindings_v4",
]
