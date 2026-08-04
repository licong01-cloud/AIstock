from __future__ import annotations

from typing import Any

from backend.services.miniqmt_execution_runtime.deterministic_context import best_limit_quantity_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AlgoReadOnlyServicesV1,
    AlgoStartContextV1,
    CurrentThreeActiveOrderStatusV3,
    RuntimeEventEnvelopeV2,
    SideV1,
    TerminalOutcomeV1,
)

from .plugin_base import CurrentThreePluginBaseV3, EffectCollectorV3


class BestLimitMiniQMTPluginV3(CurrentThreePluginBaseV3):
    ALGO_CODE = "BEST_LIMIT_MINIQMT"
    ALGO_NAME = "BestLimit MiniQMT"

    def _specific_initial_state(self, context: AlgoStartContextV1) -> dict[str, Any]:
        minimum = self.config["min_volume"]
        maximum = self.config["max_volume"]
        return {
            "vt_orderid": None,
            "order_price_decimal": None,
            "next_draw_ordinal": 0,
            "parameters": {"min_volume": minimum, "max_volume": maximum},
            "variables": {"vt_orderid": None, "order_price_decimal": None, "next_draw_ordinal": 0},
        }

    def _handle_tick(
        self,
        state: dict[str, Any],
        event: RuntimeEventEnvelopeV2,
        services: AlgoReadOnlyServicesV1,
        collector: EffectCollectorV3,
    ) -> TerminalOutcomeV1 | None:
        lineage = self._tick_lineage(event)
        state["last_tick_lineage"] = lineage
        items = self._active_items(state)
        side = SideV1(state["side"])
        quote = self._quote_or_wait(
            event,
            side=SideV1.SELL if side is SideV1.BUY else SideV1.BUY,
            need_volume=False,
            collector=collector,
        )
        if quote is None:
            return None
        quote_price, _ = quote
        if items:
            item = items[0]
            if item.status in {
                CurrentThreeActiveOrderStatusV3.COMMAND_PENDING,
                CurrentThreeActiveOrderStatusV3.CANCEL_PENDING,
                CurrentThreeActiveOrderStatusV3.OUTCOME_UNKNOWN,
                CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING,
            }:
                collector.diagnostic("K3_COMMAND_LIFECYCLE_WAIT", "BestLimit does not duplicate pending effects")
                return None
            if item.requested_price_decimal == quote_price:
                return None
            command = collector.cancel(
                item=item,
                reason_code=("best_limit_bid_price_changed" if side is SideV1.BUY else "best_limit_ask_price_changed"),
            )
            items[0] = self._replace_item(
                item,
                status=CurrentThreeActiveOrderStatusV3.CANCEL_PENDING,
                pending_command_type=command.command_type,
                pending_command_id=command.command_id,
            )
            self._write_active_items(state, items)
            self._sync_specific_active_state(state, items)
            return None
        draw_ordinal = state["next_draw_ordinal"]
        quantity = min(
            best_limit_quantity_v1(
                context=collector.context,
                draw_ordinal=draw_ordinal,
                min_volume=state["parameters"]["min_volume"],
                max_volume=state["parameters"]["max_volume"],
            ),
            state["parent_quantity"] - state["traded_quantity"],
        )
        if quantity <= 0:
            collector.diagnostic("BEST_LIMIT_QUANTITY_ZERO", "deterministic draw produced no remaining quantity")
            return None
        command = collector.submit(
            symbol=state["symbol"],
            side=side,
            price_decimal=quote_price,
            quantity=quantity,
            reason_code=("best_limit_buy_at_bid_price_1" if side is SideV1.BUY else "best_limit_sell_at_ask_price_1"),
            metadata={"draw_ordinal": draw_ordinal, "market_data_lineage": lineage},
        )
        item = self._pending_submit_item(command=command, lineage=lineage)
        state["next_draw_ordinal"] = draw_ordinal + 1
        self._write_active_items(state, [item])
        self._sync_specific_active_state(state, [item])
        return None

    def _sync_specific_active_state(self, state: dict[str, Any], items: list[Any]) -> None:
        vt_orderid = None if not items else items[0].local_vt_orderid
        order_price = None if not items else items[0].requested_price_decimal
        state["vt_orderid"] = vt_orderid
        state["order_price_decimal"] = order_price
        state["variables"] = {
            "vt_orderid": vt_orderid,
            "order_price_decimal": order_price,
            "next_draw_ordinal": state["next_draw_ordinal"],
        }


__all__ = ["BestLimitMiniQMTPluginV3"]
