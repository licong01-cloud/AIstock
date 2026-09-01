from __future__ import annotations

from decimal import Decimal
from typing import Any

from backend.services.miniqmt_execution_runtime.plugin_canonical import thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AlgoReadOnlyServicesV1,
    AlgoStartContextV1,
    CurrentThreeActiveOrderStatusV3,
    RuntimeEventEnvelopeV2,
    SideV1,
    TerminalOutcomeV1,
)

from .plugin_base import CurrentThreePluginBaseV3, EffectCollectorV3, plus_one_second_v1


class TwapLiteMiniQMTPluginV3(CurrentThreePluginBaseV3):
    ALGO_CODE = "TWAP_LITE_MINIQMT"
    ALGO_NAME = "TWAP Lite MiniQMT"
    TIMER_NAME = "TWAP_ACTIVE_SECOND"

    def _specific_initial_state(self, context: AlgoStartContextV1) -> dict[str, Any]:
        duration = self.config["time"]
        interval = self.config["interval"]
        raw_order_volume = context.parent_quantity / (duration / interval)
        quantity = int(raw_order_volume)
        if quantity <= 0:
            order_volume = 0
        elif quantity < context.min_volume:
            order_volume = min(context.parent_quantity, context.min_volume)
        else:
            rounded = (quantity // context.volume_increment) * context.volume_increment
            order_volume = min(context.parent_quantity, max(context.min_volume, rounded))
        variables = {
            "order_volume": order_volume,
            "active_elapsed_seconds": 0,
            "interval_elapsed_seconds": 0,
            "last_timer_occurrence_id": None,
            "last_market_data_lineage": None,
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

    def _handle_tick(
        self,
        state: dict[str, Any],
        event: RuntimeEventEnvelopeV2,
        services: AlgoReadOnlyServicesV1,
        collector: EffectCollectorV3,
    ) -> TerminalOutcomeV1 | None:
        lineage = self._tick_lineage(event)
        state["last_tick_lineage"] = lineage
        state["last_market_data_lineage"] = lineage
        self._sync_variables(state)
        if any(
            item.status is CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING for item in self._active_items(state)
        ):
            collector.diagnostic("K3_TERMINAL_TRADE_PENDING_WAIT", "TWAP waits for exact TRADE closure")
        return None

    def _handle_timer(
        self,
        state: dict[str, Any],
        event: RuntimeEventEnvelopeV2,
        services: AlgoReadOnlyServicesV1,
        collector: EffectCollectorV3,
    ) -> TerminalOutcomeV1 | None:
        payload = thaw_json_v1(event.payload)
        occurrence_id = payload["timer_occurrence_id"]
        if state["last_timer_occurrence_id"] == occurrence_id:
            collector.diagnostic("TWAP_TIMER_DUPLICATE", "duplicate durable timer occurrence was ignored")
            return None
        if payload["timer_name"] != self.TIMER_NAME:
            raise ValueError("TWAP received a timer owned by another schedule")
        state["last_timer_occurrence_id"] = occurrence_id
        state["active_elapsed_seconds"] += 1
        state["interval_elapsed_seconds"] += 1
        items = self._active_items(state)
        if state["active_elapsed_seconds"] >= state["duration_seconds"]:
            state["active_elapsed_seconds"] = state["duration_seconds"]
            if not items:
                state["status"] = "FINISHED"
                state["finished_reason"] = "twap_lite_total_time_exhausted"
                self._sync_variables(state)
                return TerminalOutcomeV1.EXPIRED_WITH_RESIDUAL
            state["status"] = "STOPPED"
            for index, item in enumerate(items):
                if item.status in {
                    CurrentThreeActiveOrderStatusV3.COMMAND_PENDING,
                    CurrentThreeActiveOrderStatusV3.CANCEL_PENDING,
                    CurrentThreeActiveOrderStatusV3.OUTCOME_UNKNOWN,
                    CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING,
                }:
                    continue
                command = collector.cancel(item=item, reason_code="twap_lite_duration_cancel_active_child")
                items[index] = self._replace_item(
                    item,
                    status=CurrentThreeActiveOrderStatusV3.CANCEL_PENDING,
                    pending_command_type=command.command_type,
                    pending_command_id=command.command_id,
                )
            self._write_active_items(state, items)
            self._sync_variables(state)
            return None
        if state["interval_elapsed_seconds"] >= state["interval_seconds"]:
            state["interval_elapsed_seconds"] = 0
            lineage = state["last_market_data_lineage"]
            if lineage is None:
                collector.diagnostic("TWAP_WAITING_FOR_TICK", "slice boundary has no durable market view")
            elif any(
                item.status
                in {
                    CurrentThreeActiveOrderStatusV3.COMMAND_PENDING,
                    CurrentThreeActiveOrderStatusV3.CANCEL_PENDING,
                    CurrentThreeActiveOrderStatusV3.OUTCOME_UNKNOWN,
                    CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING,
                }
                for item in items
            ):
                collector.diagnostic("K3_COMMAND_LIFECYCLE_WAIT", "TWAP slice waits for command lifecycle closure")
            else:
                for index, item in enumerate(items):
                    command = collector.cancel(item=item, reason_code="twap_lite_cancel_before_next_slice")
                    items[index] = self._replace_item(
                        item,
                        status=CurrentThreeActiveOrderStatusV3.CANCEL_PENDING,
                        pending_command_type=command.command_type,
                        pending_command_id=command.command_id,
                    )
                side = SideV1(state["side"])
                if services.market_data_projection_id != lineage["market_data_id"]:
                    raise ValueError("TWAP durable market projection identity conflicts with state lineage")
                quote_payload = thaw_json_v1(services.market_data_projection or {})
                quote_price, _ = self._quote_payload(quote_payload, side=side, need_volume=False)
                limit_price = state["limit_price_decimal"]
                quote_value = Decimal(quote_price)
                limit_value = Decimal(limit_price)
                crossed = quote_value <= limit_value if side is SideV1.BUY else quote_value >= limit_value
                quantity = min(state["order_volume"], state["parent_quantity"] - state["traded_quantity"])
                if quantity <= 0:
                    collector.diagnostic("TWAP_SLICE_VOLUME_ROUNDED_ZERO", "TWAP slice quantity is zero")
                elif crossed:
                    command = collector.submit(
                        symbol=state["symbol"],
                        side=side,
                        price_decimal=limit_price,
                        quantity=quantity,
                        reason_code=("twap_lite_interval_buy" if side is SideV1.BUY else "twap_lite_interval_sell"),
                        metadata={"market_data_lineage": lineage},
                    )
                    items.append(self._pending_submit_item(command=command, lineage=lineage))
                self._write_active_items(state, items)
        collector.timer(
            timer_name=self.TIMER_NAME,
            schedule_epoch=payload["schedule_epoch"],
            raw_due_at_utc=plus_one_second_v1(collector.context.logical_time_utc),
        )
        self._sync_variables(state)
        return None

    def _handle_eod(self, state: dict[str, Any], collector: EffectCollectorV3) -> TerminalOutcomeV1 | None:
        collector.cancel_timer(
            timer_name=self.TIMER_NAME,
            schedule_epoch=collector.context.session_epoch,
            reason_code="TWAP_EOD_CANCEL_ACTIVE_TIMER",
        )
        return super()._handle_eod(state, collector)

    def _sync_specific_active_state(self, state: dict[str, Any], items: list[Any]) -> None:
        self._sync_variables(state)

    @staticmethod
    def _sync_variables(state: dict[str, Any]) -> None:
        state["variables"] = {
            "order_volume": state["order_volume"],
            "active_elapsed_seconds": state["active_elapsed_seconds"],
            "interval_elapsed_seconds": state["interval_elapsed_seconds"],
            "last_timer_occurrence_id": state["last_timer_occurrence_id"],
            "last_market_data_lineage": state["last_market_data_lineage"],
        }


__all__ = ["TwapLiteMiniQMTPluginV3"]
