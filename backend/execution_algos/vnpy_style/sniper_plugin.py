from __future__ import annotations

from decimal import Decimal
from typing import Any

from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AlgoReadOnlyServicesV1,
    AlgoStartContextV1,
    CurrentThreeActiveOrderStatusV3,
    RuntimeEventEnvelopeV2,
    SideV1,
    TerminalOutcomeV1,
)

from .plugin_base import CurrentThreePluginBaseV3, EffectCollectorV3


class SniperMiniQMTPluginV3(CurrentThreePluginBaseV3):
    ALGO_CODE = "SNIPER_MINIQMT"
    ALGO_NAME = "Sniper MiniQMT"

    def _specific_initial_state(self, context: AlgoStartContextV1) -> dict[str, Any]:
        return {"vt_orderid": None, "variables": {"vt_orderid": None}}

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
        if any(item.status is CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING for item in items):
            collector.diagnostic("K3_TERMINAL_TRADE_PENDING_WAIT", "Sniper waits for exact TRADE closure")
            return None
        if items:
            item = items[0]
            if item.status in {
                CurrentThreeActiveOrderStatusV3.COMMAND_PENDING,
                CurrentThreeActiveOrderStatusV3.CANCEL_PENDING,
                CurrentThreeActiveOrderStatusV3.OUTCOME_UNKNOWN,
            }:
                collector.diagnostic("K3_COMMAND_LIFECYCLE_WAIT", "Sniper does not duplicate a pending command")
                return None
            command = collector.cancel(item=item, reason_code="sniper_active_order_cancel_before_requote")
            items[0] = self._replace_item(
                item,
                status=CurrentThreeActiveOrderStatusV3.CANCEL_PENDING,
                pending_command_type=command.command_type,
                pending_command_id=command.command_id,
            )
            self._write_active_items(state, items)
            self._sync_specific_active_state(state, items)
            return None
        side = SideV1(state["side"])
        quote = self._quote_or_wait(event, side=side, need_volume=True, collector=collector)
        if quote is None:
            return None
        quote_price, quote_volume = quote
        limit_price = state["limit_price_decimal"]
        quote_value = Decimal(quote_price)
        limit_value = Decimal(limit_price)
        crossed = quote_value <= limit_value if side is SideV1.BUY else quote_value >= limit_value
        if not crossed:
            return None
        quantity = min(state["parent_quantity"] - state["traded_quantity"], quote_volume or 0)
        if quantity <= 0:
            collector.diagnostic("SNIPER_DEPTH_QUANTITY_ZERO", "crossed quote has no executable depth")
            return None
        command = collector.submit(
            symbol=state["symbol"],
            side=side,
            price_decimal=limit_price,
            quantity=quantity,
            reason_code=("sniper_ask_crossed_limit" if side is SideV1.BUY else "sniper_bid_crossed_limit"),
            metadata={"market_data_lineage": lineage},
        )
        item = self._pending_submit_item(command=command, lineage=lineage)
        self._write_active_items(state, [item])
        self._sync_specific_active_state(state, [item])
        return None

    def _sync_specific_active_state(self, state: dict[str, Any], items: list[Any]) -> None:
        vt_orderid = None if not items else items[0].local_vt_orderid
        state["vt_orderid"] = vt_orderid
        state["variables"] = {"vt_orderid": vt_orderid}


__all__ = ["SniperMiniQMTPluginV3"]
