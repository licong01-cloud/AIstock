from __future__ import annotations

from decimal import Decimal
from typing import Any

from backend.services.miniqmt_execution_runtime.plugin_canonical import thaw_json_v1
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

from .hot_plugin_base import CurrentThreeHotPluginBaseV4, CurrentThreeHotTargetV4, _effect_v1
from .plugin_base import EffectCollectorV3, plus_one_second_v1


class TwapLiteMiniQMTPluginV4(CurrentThreeHotPluginBaseV4):
    ALGO_CODE = "TWAP_LITE_MINIQMT"
    ALGO_NAME = "TWAP Lite MiniQMT"
    TIMER_NAME = "TWAP_ACTIVE_SECOND"

    def _specific_initial_state(self, context: AlgoStartContextV1) -> dict[str, Any]:
        duration, interval = self.config["time"], self.config["interval"]
        raw = context.parent_quantity / (duration / interval)
        quantity = int(raw)
        order_volume = (
            0
            if quantity <= 0
            else min(
                context.parent_quantity,
                max(context.min_volume, (quantity // context.volume_increment) * context.volume_increment),
            )
        )
        variables = {
            "order_volume": order_volume,
            "active_elapsed_seconds": 0,
            "interval_elapsed_seconds": 0,
            "last_timer_occurrence_id": None,
            "slice_ready": False,
        }
        return {
            "duration_seconds": duration,
            "interval_seconds": interval,
            **variables,
            "parameters": {"time": duration, "interval": interval},
            "variables": dict(variables),
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
            raise ValueError("TWAP received a timer owned by another schedule")
        if state["last_timer_occurrence_id"] == payload["timer_occurrence_id"]:
            collector.diagnostic("TWAP_TIMER_DUPLICATE", "duplicate durable timer occurrence was ignored")
            return None
        state["last_timer_occurrence_id"] = payload["timer_occurrence_id"]
        state["active_elapsed_seconds"] += 1
        state["interval_elapsed_seconds"] += 1
        items = self._active_items(state)
        if state["active_elapsed_seconds"] >= state["duration_seconds"]:
            state["active_elapsed_seconds"] = state["duration_seconds"]
            if not items:
                state["status"], state["finished_reason"] = "FINISHED", "twap_lite_total_time_exhausted"
                self._sync_variables(state)
                return TerminalOutcomeV1.EXPIRED_WITH_RESIDUAL
            state["status"] = "STOPPED"
        elif state["interval_elapsed_seconds"] >= state["interval_seconds"]:
            state["interval_elapsed_seconds"] = 0
            state["slice_ready"] = True
        collector.timer(
            timer_name=self.TIMER_NAME,
            schedule_epoch=payload["schedule_epoch"],
            raw_due_at_utc=plus_one_second_v1(collector.context.logical_time_utc),
        )
        self._sync_variables(state)
        return None

    def _after_hot_economic_action(self, state, effect, items) -> None:
        if effect["action"] == "SUBMIT_LIMIT":
            state["slice_ready"] = False
        self._sync_variables(state)

    def _sync_specific_active_state(self, state, items) -> None:
        self._sync_variables(state)

    @staticmethod
    def _sync_variables(state) -> None:
        state["variables"] = {
            key: state[key]
            for key in (
                "order_volume",
                "active_elapsed_seconds",
                "interval_elapsed_seconds",
                "last_timer_occurrence_id",
                "slice_ready",
            )
        }


class TwapLiteHotTargetV4(CurrentThreeHotTargetV4):
    def evaluate_hot_market_data_v1(self, view: HotMarketDataViewV1) -> HotMarketDataEconomicEffectV1 | None:
        state = self._state()
        if not state["slice_ready"]:
            return None
        items = self._active(state)
        if items:
            return self._cancel_or_wait(view=view, state=state, reason_code="twap_lite_cancel_before_next_slice")
        side = SideV1(state["side"])
        quote, limit = self._price(view, side), Decimal(state["limit_price_decimal"])
        if not (quote <= limit if side is SideV1.BUY else quote >= limit):
            return None
        quantity = min(state["order_volume"], state["parent_quantity"] - state["traded_quantity"])
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
                "reason_code": "twap_lite_interval_buy" if side is SideV1.BUY else "twap_lite_interval_sell",
            },
        )


__all__ = ["TwapLiteHotTargetV4", "TwapLiteMiniQMTPluginV4"]
