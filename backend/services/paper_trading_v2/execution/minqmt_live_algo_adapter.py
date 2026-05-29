"""MiniQMT adapter for vn.py-style execution strategy assets.

This adapter borrows vnpy_algotrading engine responsibilities (event routing,
order ownership, and timer/tick dispatch) without introducing EventEngine,
AlgoEngine, MainEngine, or gateway runtime. MiniQMT remains the broker authority.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any, Callable

from backend.execution_algos.vnpy_style import (
    VnpyAction,
    VnpyActionType,
    VnpyOrderUpdate,
    VnpyStyleConfigError,
    VnpyTick,
    VnpyTradeUpdate,
    create_vnpy_style_core,
    get_vnpy_style_asset,
    is_vnpy_style_algo,
)
from backend.services.paper_trading_v2.broker.base import BrokerBackend, OrderHandle, OrderHandleStatus
from backend.services.trading_core.errors import BrokerConnectivityError, TradingCoreError
from backend.services.trading_core.models import Fill, OrderIntent, OrderSide, OrderType

from .minqmt_order_state import (
    board_lot_for_symbol,
    limit_price_for_intent,
    synthetic_tick_from_intent,
    tick_from_quote,
)


@dataclass
class MiniQMTAlgoChildOrder:
    vt_orderid: str
    handle: OrderHandle | None
    intent: OrderIntent
    submitted_at: datetime
    native_context: dict[str, Any] = field(default_factory=dict)
    status: OrderHandleStatus | None = None
    trades: list[dict[str, Any]] = field(default_factory=list)
    submit_error: dict[str, Any] | None = None


@dataclass
class MiniQMTAlgoExecutionResult:
    parent_intent: OrderIntent
    algo_code: str
    policy_context: dict[str, Any]
    policy_sha256: str | None
    asset_metadata: dict[str, Any]
    actions: list[VnpyAction]
    child_orders: list[MiniQMTAlgoChildOrder]
    algo_state: dict[str, Any]
    terminal_state: str
    diagnostic: dict[str, Any]

    @property
    def submitted_child_count(self) -> int:
        return sum(1 for child in self.child_orders if child.handle is not None)


class MiniQMTLiveAlgoAdapter:
    """Route one OrderIntent through a selected vn.py-style execution asset."""

    def __init__(
        self,
        *,
        broker: BrokerBackend,
        policy_context: dict[str, Any],
        quote_provider: Callable[[str], dict[str, Any] | None] | None = None,
        now_provider: Callable[[], datetime] | None = None,
        random_volume_provider: Callable[[int, int], float] | None = None,
    ) -> None:
        policy_json = policy_context.get("policy_json") if isinstance(policy_context, dict) else None
        if not isinstance(policy_json, dict):
            raise VnpyStyleConfigError("MiniQMT vn.py-style adapter requires policy_context.policy_json")
        algo_code = str(policy_json.get("algo_code") or "").strip().upper()
        if not is_vnpy_style_algo(algo_code):
            raise VnpyStyleConfigError(f"unsupported MiniQMT vn.py-style policy: {algo_code}")
        self.broker = broker
        self.policy_context = dict(policy_context)
        self.policy_json = dict(policy_json)
        self.algo_code = algo_code
        self.algo_config = dict(self.policy_json.get("algo_config") or {})
        self.spec = get_vnpy_style_asset(algo_code)
        self.quote_provider = quote_provider
        self.now_provider = now_provider or (lambda: datetime.now(UTC))
        self.random_volume_provider = random_volume_provider

    def execute_intent(self, intent: OrderIntent, *, trade_date: date) -> MiniQMTAlgoExecutionResult:
        min_volume, volume_increment = board_lot_for_symbol(intent.symbol)
        quote = self._load_quote(intent)
        price = limit_price_for_intent(intent, fallback_price=_limit_fallback_price(intent, quote))
        tick = tick_from_quote(intent.symbol, quote) if quote is not None else synthetic_tick_from_intent(intent, price=price)
        core = create_vnpy_style_core(
            algo_code=self.algo_code,
            symbol=intent.symbol,
            side=intent.side.value,
            price=price,
            volume=int(intent.quantity),
            algo_config=self.algo_config,
            algo_name=f"{self.algo_code}_{intent.intent_id}",
            min_volume=min_volume,
            volume_increment=volume_increment,
            random_volume_provider=self.random_volume_provider,
        )
        actions: list[VnpyAction] = []
        child_orders: list[MiniQMTAlgoChildOrder] = []
        actions.extend(core.start())
        actions.extend(core.update_tick(tick))
        for _ in range(_timer_iterations(self.algo_code, self.algo_config)):
            actions.extend(core.update_timer())

        index = 0
        while index < len(actions):
            action = actions[index]
            if action.action_type == VnpyActionType.SUBMIT:
                child = self._submit_child_action(intent, action, trade_date=trade_date)
                child_orders.append(child)
                if child.handle is not None:
                    active = _status_is_active(child.status)
                    actions.extend(
                        core.update_order(
                            VnpyOrderUpdate(
                                vt_orderid=action.vt_orderid or child.intent.intent_id,
                                active=active,
                                traded=int(child.status.filled_quantity if child.status else 0),
                                price=float(child.status.avg_fill_price) if child.status and child.status.avg_fill_price else None,
                                raw_status=child.status.state if child.status else None,
                                status_msg=child.status.rejection_reason if child.status else None,
                                raw=_status_raw(child.status),
                            )
                        )
                    )
                    for trade in child.trades:
                        volume = int(trade.get("traded_volume") or 0)
                        price_raw = float(trade.get("traded_price") or 0.0)
                        if volume > 0 and price_raw > 0:
                            actions.extend(
                                core.update_trade(
                                    VnpyTradeUpdate(
                                        vt_orderid=action.vt_orderid or child.intent.intent_id,
                                        volume=volume,
                                        price=price_raw,
                                        trade_time=_trade_time(trade),
                                        raw=dict(trade),
                                    )
                                )
                            )
                else:
                    actions.extend(
                        core.update_order(
                            VnpyOrderUpdate(
                                vt_orderid=action.vt_orderid or child.intent.intent_id,
                                active=False,
                                raw_status="submit_error",
                                status_msg=_nested_reason(child.submit_error),
                                raw=dict(child.submit_error or {}),
                            )
                        )
                    )
            elif action.action_type in {VnpyActionType.CANCEL, VnpyActionType.CANCEL_ALL}:
                self._cancel_matching_child(child_orders, action)
            index += 1

        terminal_state = _terminal_state(child_orders)
        algo_state = core.audit_metadata()
        diagnostic = self._diagnostic(intent, tick, child_orders, actions, terminal_state, algo_state)
        return MiniQMTAlgoExecutionResult(
            parent_intent=intent,
            algo_code=self.algo_code,
            policy_context=self.policy_context,
            policy_sha256=str(self.policy_context.get("policy_sha256") or "") or None,
            asset_metadata=self.spec.metadata(),
            actions=actions,
            child_orders=child_orders,
            algo_state=algo_state,
            terminal_state=terminal_state,
            diagnostic=diagnostic,
        )

    def _load_quote(self, intent: OrderIntent) -> dict[str, Any] | None:
        if self.quote_provider is not None:
            quote = self.quote_provider(intent.symbol)
            if quote:
                return dict(quote)
        if intent.limit_price is not None:
            return None
        raise VnpyStyleConfigError(
            f"{self.algo_code} requires quote_provider or intent.limit_price for {intent.symbol}; no fallback is allowed"
        )

    def _submit_child_action(
        self,
        parent: OrderIntent,
        action: VnpyAction,
        *,
        trade_date: date,
    ) -> MiniQMTAlgoChildOrder:
        if action.price is None or action.volume is None or action.volume <= 0:
            raise VnpyStyleConfigError(f"{self.algo_code} generated invalid submit action")
        child_side = _side_from_action(action, parent.side)
        child_intent = OrderIntent(
            package_id=parent.package_id,
            portfolio_id=parent.portfolio_id,
            symbol=parent.symbol,
            side=child_side,
            quantity=int(action.volume),
            order_type=OrderType.LIMIT,
            limit_price=float(action.price),
            target_trade_date=trade_date,
            metadata={
                **dict(parent.metadata or {}),
                "parent_intent_id": parent.intent_id,
                "vnpy_action_id": action.action_id,
                "vnpy_vt_orderid": action.vt_orderid,
                "execution_algo_code": self.algo_code,
                "execution_policy_id": self.policy_context.get("validated_execution_policy_id"),
                "execution_policy_sha256": self.policy_context.get("policy_sha256"),
                "execution_asset_version": self.spec.version,
                "source_attribution": self.spec.metadata()["source_attribution"],
            },
        )
        submitted_at = self.now_provider()
        try:
            handle = self.broker.submit_order_intent(child_intent)
            native = _safe_order_context(self.broker, handle)
            status = self.broker.query_status(handle)
            trades = self.broker.query_trades(handle)
            return MiniQMTAlgoChildOrder(
                vt_orderid=action.vt_orderid or child_intent.intent_id,
                handle=handle,
                intent=child_intent,
                submitted_at=submitted_at,
                native_context=native,
                status=status,
                trades=[dict(row) for row in trades],
            )
        except TradingCoreError as exc:
            return MiniQMTAlgoChildOrder(
                vt_orderid=action.vt_orderid or child_intent.intent_id,
                handle=None,
                intent=child_intent,
                submitted_at=submitted_at,
                submit_error=exc.to_dict(),
            )
        except Exception as exc:
            error = {
                "error_code": "MINIQMT_ALGO_CHILD_SUBMIT_FAILED",
                "message": "MiniQMT child order submit failed",
                "context": {"reason": f"{type(exc).__name__}: {exc}"},
            }
            return MiniQMTAlgoChildOrder(
                vt_orderid=action.vt_orderid or child_intent.intent_id,
                handle=None,
                intent=child_intent,
                submitted_at=submitted_at,
                submit_error=error,
            )

    def _cancel_matching_child(self, children: list[MiniQMTAlgoChildOrder], action: VnpyAction) -> None:
        for child in children:
            if child.handle is None:
                continue
            if action.action_type == VnpyActionType.CANCEL and action.vt_orderid != child.vt_orderid:
                continue
            if child.status is not None and not _status_is_active(child.status):
                continue
            try:
                ack = self.broker.cancel(child.handle)
                child.native_context = {
                    **dict(child.native_context),
                    "cancel_ack": {"accepted": bool(ack.accepted), "reason": ack.reason},
                }
                child.status = self.broker.query_status(child.handle)
            except TradingCoreError as exc:
                child.native_context = {**dict(child.native_context), "cancel_error": exc.to_dict()}
            except Exception as exc:
                child.native_context = {
                    **dict(child.native_context),
                    "cancel_error": {"reason": f"{type(exc).__name__}: {exc}"},
                }

    def _diagnostic(
        self,
        intent: OrderIntent,
        tick: VnpyTick,
        children: list[MiniQMTAlgoChildOrder],
        actions: list[VnpyAction],
        terminal_state: str,
        algo_state: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "execution_algo_code": self.algo_code,
            "execution_asset_version": self.spec.version,
            "execution_policy_id": self.policy_context.get("validated_execution_policy_id"),
            "execution_policy_sha256": self.policy_context.get("policy_sha256"),
            "execution_policy_hash": _json_sha256(self.policy_json),
            "source_attribution": self.spec.metadata()["source_attribution"],
            "parent_intent_id": intent.intent_id,
            "symbol": intent.symbol,
            "side": intent.side.value,
            "quantity": intent.quantity,
            "terminal_state": terminal_state,
            "quote": {
                "bid_price_1": tick.bid_price_1,
                "bid_volume_1": tick.bid_volume_1,
                "ask_price_1": tick.ask_price_1,
                "ask_volume_1": tick.ask_volume_1,
                "datetime": tick.datetime.isoformat(),
                "raw": dict(tick.raw),
            },
            "actions": [_action_payload(action) for action in actions],
            "child_orders": [_child_payload(child) for child in children],
            "algo_state": algo_state,
        }


def _timer_iterations(algo_code: str, config: dict[str, Any]) -> int:
    if algo_code != "TWAP_LITE_MINIQMT":
        return int(config.get("timer_iterations", 1) or 1)
    interval = int(config.get("interval", config.get("interval_seconds", 60)) or 60)
    default_iterations = max(1, interval)
    return int(config.get("timer_iterations", default_iterations) or default_iterations)


def _limit_fallback_price(intent: OrderIntent, quote: dict[str, Any] | None) -> float | None:
    if intent.limit_price is not None:
        return float(intent.limit_price)
    if quote is None:
        return None
    if intent.side == OrderSide.BUY:
        return float(quote.get("ask_price_1") or quote.get("ask") or 0.0) or None
    return float(quote.get("bid_price_1") or quote.get("bid") or 0.0) or None


def _side_from_action(action: VnpyAction, fallback: OrderSide) -> OrderSide:
    if action.direction is None:
        return fallback
    if action.direction.value == "LONG":
        return OrderSide.BUY
    if action.direction.value == "SHORT":
        return OrderSide.SELL
    return fallback


def _safe_order_context(broker: BrokerBackend, handle: OrderHandle) -> dict[str, Any]:
    if hasattr(broker, "order_context"):
        try:
            return dict(broker.order_context(handle))  # type: ignore[attr-defined]
        except Exception as exc:
            return {"order_context_error": f"{type(exc).__name__}: {exc}"}
    return {"handle_id": handle.handle_id, "intent_id": handle.intent_id}


def _status_is_active(status: OrderHandleStatus | None) -> bool:
    if status is None:
        return False
    return status.state in {"pending", "partial_filled"}


def _status_raw(status: OrderHandleStatus | None) -> dict[str, Any]:
    if status is None:
        return {}
    return status.model_dump(mode="json")


def _nested_reason(error: dict[str, Any] | None) -> str | None:
    if not error:
        return None
    return str(error.get("message") or error.get("error_code") or error)


def _trade_time(trade: dict[str, Any]) -> datetime:
    raw = str(trade.get("traded_time") or "")
    if raw and len(raw) >= 6 and raw[:6].isdigit():
        now = datetime.now(UTC)
        return now.replace(hour=int(raw[:2]), minute=int(raw[2:4]), second=int(raw[4:6]), microsecond=0)
    return datetime.now(UTC)


def _terminal_state(children: list[MiniQMTAlgoChildOrder]) -> str:
    if not children:
        return "NO_ACTION"
    if any(child.submit_error for child in children):
        return "SUBMIT_REJECTED"
    if any(child.status and child.status.state == "rejected" for child in children):
        return "REJECTED"
    if any(child.status and child.status.state == "cancelled" for child in children):
        return "CANCELLED"
    if any(child.status and child.status.state == "filled" for child in children):
        return "FILLED"
    if any(child.status and child.status.state == "partial_filled" for child in children):
        return "PARTIAL"
    return "PENDING"


def _action_payload(action: VnpyAction) -> dict[str, Any]:
    return {
        "action_id": action.action_id,
        "action_type": action.action_type.value,
        "vt_orderid": action.vt_orderid,
        "direction": action.direction.value if action.direction else None,
        "price": action.price,
        "volume": action.volume,
        "reason": action.reason,
        "metadata": dict(action.metadata),
    }


def _child_payload(child: MiniQMTAlgoChildOrder) -> dict[str, Any]:
    return {
        "vt_orderid": child.vt_orderid,
        "handle_id": child.handle.handle_id if child.handle else None,
        "intent_id": child.intent.intent_id,
        "symbol": child.intent.symbol,
        "side": child.intent.side.value,
        "quantity": child.intent.quantity,
        "limit_price": child.intent.limit_price,
        "submitted_at": child.submitted_at.isoformat(),
        "native_context": dict(child.native_context),
        "status": child.status.model_dump(mode="json") if child.status else None,
        "trades": [dict(row) for row in child.trades],
        "submit_error": child.submit_error,
    }


def _json_sha256(value: dict[str, Any]) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
