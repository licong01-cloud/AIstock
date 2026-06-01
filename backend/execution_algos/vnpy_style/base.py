"""vn.py AlgoTemplate-derived core without vn.py runtime imports.

Derived from vn.py/vnpy_algotrading template.py and base.py at commit
4133987530eb28f3538d1983545d81c4f83d7d59. Preserved semantics include active
order tracking, tick/order/trade/timer dispatch gates, buy/sell/cancel helpers,
and finish cancelling active children. See attribution.py for license data.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from .attribution import AISTOCK_ASSET_VERSION, source_attribution
from .models import (
    VnpyAction,
    VnpyActionType,
    VnpyAlgoConfig,
    VnpyAlgoSnapshot,
    VnpyAlgoStatus,
    VnpyDirection,
    VnpyOrderUpdate,
    VnpyTick,
    VnpyTradeUpdate,
)

UTC = timezone.utc


class VnpyStyleConfigError(ValueError):
    """Raised when a vn.py-style execution asset cannot run authoritatively."""


class VnpyAlgoTemplate:
    """Adapter-independent execution core preserving vn.py AlgoTemplate shape."""

    ALGO_CODE = "VNPY_TEMPLATE"
    display_name = "vn.py style template"
    default_setting: dict[str, Any] = {}
    variables: list[str] = []

    def __init__(self, config: VnpyAlgoConfig) -> None:
        self.algo_code = str(config.algo_code).upper()
        self.algo_name = config.algo_name or f"{self.algo_code}_{uuid4().hex[:8]}"
        self.vt_symbol = config.symbol
        self.direction = config.direction
        self.offset = "NONE"
        self.price = _positive_float(config.price, "price")
        self.volume = _positive_int(config.volume, "volume")
        self.min_volume = _positive_int(config.min_volume, "min_volume")
        self.volume_increment = _positive_int(config.volume_increment, "volume_increment")
        self.setting = dict(config.setting or {})
        self.status = VnpyAlgoStatus.PAUSED
        self.traded = 0
        self.traded_price = 0.0
        self.active_orders: dict[str, VnpyOrderUpdate] = {}
        self._actions: list[VnpyAction] = []
        self._logs: list[str] = []
        self._last_tick: VnpyTick | None = None
        self._finished_reason: str | None = None

    def start(self) -> list[VnpyAction]:
        self.status = VnpyAlgoStatus.RUNNING
        self.write_log("algorithm started")
        return self.drain_actions()

    def stop(self) -> list[VnpyAction]:
        self.status = VnpyAlgoStatus.STOPPED
        self.cancel_all(reason="algorithm stopped")
        self.write_log("algorithm stopped")
        return self.drain_actions()

    def finish(self, reason: str = "algorithm finished") -> None:
        self.status = VnpyAlgoStatus.FINISHED
        self._finished_reason = reason
        self.cancel_all(reason=reason)
        self._actions.append(
            VnpyAction(
                action_type=VnpyActionType.FINISH,
                reason=reason,
                metadata={"traded": self.traded, "volume": self.volume},
            )
        )
        self.write_log(reason)

    def pause(self) -> None:
        self.status = VnpyAlgoStatus.PAUSED
        self.write_log("algorithm paused")

    def resume(self) -> None:
        self.status = VnpyAlgoStatus.RUNNING
        self.write_log("algorithm resumed")

    def update_tick(self, tick: VnpyTick) -> list[VnpyAction]:
        self._last_tick = tick
        if self.status == VnpyAlgoStatus.RUNNING:
            self.on_tick(tick)
        return self.drain_actions()

    def update_order(self, order: VnpyOrderUpdate) -> list[VnpyAction]:
        if order.is_active():
            self.active_orders[order.vt_orderid] = order
        else:
            self.active_orders.pop(order.vt_orderid, None)
        self.on_order(order)
        return self.drain_actions()

    def update_trade(self, trade: VnpyTradeUpdate) -> list[VnpyAction]:
        if trade.volume <= 0:
            raise VnpyStyleConfigError("trade volume must be positive")
        if trade.price <= 0 or not math.isfinite(trade.price):
            raise VnpyStyleConfigError("trade price must be positive finite")
        cost = self.traded_price * self.traded + float(trade.price) * int(trade.volume)
        self.traded += int(trade.volume)
        self.traded_price = cost / self.traded
        self.on_trade(trade)
        return self.drain_actions()

    def update_timer(self) -> list[VnpyAction]:
        if self.status == VnpyAlgoStatus.RUNNING:
            self.on_timer()
        return self.drain_actions()

    def on_tick(self, tick: VnpyTick) -> None:
        return None

    def on_order(self, order: VnpyOrderUpdate) -> None:
        return None

    def on_trade(self, trade: VnpyTradeUpdate) -> None:
        return None

    def on_timer(self) -> None:
        return None

    def buy(self, price: float, volume: int, *, reason: str = "buy") -> str:
        return self._submit(VnpyDirection.LONG, price, volume, reason=reason)

    def sell(self, price: float, volume: int, *, reason: str = "sell") -> str:
        return self._submit(VnpyDirection.SHORT, price, volume, reason=reason)

    def cancel_order(self, vt_orderid: str, *, reason: str = "cancel_order") -> None:
        if not vt_orderid:
            return
        self._actions.append(
            VnpyAction(
                action_type=VnpyActionType.CANCEL,
                vt_orderid=vt_orderid,
                reason=reason,
                metadata={"active_order_ids": list(self.active_orders)},
            )
        )

    def cancel_all(self, *, reason: str = "cancel_all") -> None:
        if not self.active_orders:
            return
        for vt_orderid in list(self.active_orders):
            self.cancel_order(vt_orderid, reason=reason)
        self._actions.append(
            VnpyAction(
                action_type=VnpyActionType.CANCEL_ALL,
                reason=reason,
                metadata={"active_order_ids": list(self.active_orders)},
            )
        )

    def get_tick(self) -> VnpyTick | None:
        return self._last_tick

    def get_parameters(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in self.default_setting if hasattr(self, name)}

    def get_variables(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in self.variables if hasattr(self, name)}

    def get_data(self) -> VnpyAlgoSnapshot:
        return VnpyAlgoSnapshot(
            algo_name=self.algo_name,
            algo_code=self.algo_code,
            symbol=self.vt_symbol,
            direction=self.direction.value,
            price=self.price,
            volume=self.volume,
            status=self.status.value,
            traded=self.traded,
            left=max(0, self.volume - self.traded),
            traded_price=self.traded_price,
            active_order_ids=list(self.active_orders),
            parameters=self.get_parameters(),
            variables=self.get_variables(),
        )

    def audit_metadata(self) -> dict[str, Any]:
        snapshot = self.get_data()
        return {
            "asset_version": AISTOCK_ASSET_VERSION,
            "source_attribution": source_attribution(self.algo_code),
            "snapshot": {
                "algo_name": snapshot.algo_name,
                "algo_code": snapshot.algo_code,
                "symbol": snapshot.symbol,
                "direction": snapshot.direction,
                "price": snapshot.price,
                "volume": snapshot.volume,
                "status": snapshot.status,
                "traded": snapshot.traded,
                "left": snapshot.left,
                "traded_price": snapshot.traded_price,
                "active_order_ids": list(snapshot.active_order_ids),
                "parameters": dict(snapshot.parameters),
                "variables": dict(snapshot.variables),
            },
            "finished_reason": self._finished_reason,
        }

    def write_log(self, msg: str) -> None:
        message = str(msg)
        self._logs.append(message)
        self._actions.append(
            VnpyAction(
                action_type=VnpyActionType.LOG,
                reason=message,
                metadata={"logged_at": datetime.now(UTC).isoformat()},
            )
        )

    def drain_actions(self) -> list[VnpyAction]:
        actions = list(self._actions)
        self._actions.clear()
        return actions

    def _submit(self, direction: VnpyDirection, price: float, volume: int, *, reason: str) -> str:
        if self.status != VnpyAlgoStatus.RUNNING:
            return ""
        if self.traded >= self.volume:
            return ""
        child_volume = self._legal_child_volume(volume)
        if child_volume <= 0:
            return ""
        submit_price = _positive_float(price, "order price")
        vt_orderid = f"vord_{uuid4().hex}"
        self._actions.append(
            VnpyAction(
                action_type=VnpyActionType.SUBMIT,
                vt_orderid=vt_orderid,
                direction=direction,
                price=submit_price,
                volume=child_volume,
                reason=reason,
                metadata={
                    "algo_code": self.algo_code,
                    "algo_name": self.algo_name,
                    "symbol": self.vt_symbol,
                    "parent_volume": self.volume,
                    "traded": self.traded,
                    "left": max(0, self.volume - self.traded),
                },
            )
        )
        return vt_orderid

    def _legal_child_volume(self, requested: int | float) -> int:
        remaining = max(0, int(self.volume) - int(self.traded))
        qty = min(int(requested), remaining)
        if qty <= 0:
            return 0
        if self.direction == VnpyDirection.SHORT and qty < self.min_volume:
            return qty
        qty = (qty // self.volume_increment) * self.volume_increment
        if qty < self.min_volume:
            return 0
        return qty


def _positive_int(value: Any, name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise VnpyStyleConfigError(f"{name} must be an integer") from exc
    if parsed <= 0:
        raise VnpyStyleConfigError(f"{name} must be positive")
    return parsed


def _positive_float(value: Any, name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise VnpyStyleConfigError(f"{name} must be numeric") from exc
    if parsed <= 0 or not math.isfinite(parsed):
        raise VnpyStyleConfigError(f"{name} must be positive finite")
    return parsed
