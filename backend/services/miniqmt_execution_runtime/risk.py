"""Phase 4 realtime risk hooks for the durable MiniQMT event loop."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

from backend.services.trading_core.models import OrderSide

from .models import MiniQMTChildOrder, MiniQMTExecutionAlgoInstance


class MiniQMTRiskDecisionAction(str, Enum):
    PASS = "PASS"
    KILL_SWITCH = "KILL_SWITCH"


@dataclass(frozen=True)
class MiniQMTRiskDecision:
    action: MiniQMTRiskDecisionAction
    reason_code: str
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def pass_(cls, *, reason: str = "risk checks passed", metadata: dict[str, Any] | None = None) -> "MiniQMTRiskDecision":
        return cls(
            action=MiniQMTRiskDecisionAction.PASS,
            reason_code="MINIQMT_RISK_PASS",
            reason=reason,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def kill_switch(
        cls,
        *,
        reason_code: str,
        reason: str,
        metadata: dict[str, Any] | None = None,
    ) -> "MiniQMTRiskDecision":
        normalized = str(reason_code or "").strip()
        if not normalized:
            raise ValueError("kill-switch risk decision requires reason_code")
        return cls(
            action=MiniQMTRiskDecisionAction.KILL_SWITCH,
            reason_code=normalized,
            reason=str(reason or "MiniQMT risk kill-switch triggered"),
            metadata=dict(metadata or {}),
        )


class MiniQMTRiskEngine(Protocol):
    def evaluate_event(
        self,
        *,
        runtime_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> MiniQMTRiskDecision:
        ...

    def evaluate_pre_submit(
        self,
        *,
        runtime_id: str,
        order: MiniQMTChildOrder,
        active_child_orders: list[MiniQMTChildOrder],
        active_algo_instances: list[MiniQMTExecutionAlgoInstance],
    ) -> MiniQMTRiskDecision:
        ...


class NoopMiniQMTRiskEngine:
    """Default inert hook: explicit Phase 4 risk engines opt in per runtime."""

    def evaluate_event(
        self,
        *,
        runtime_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> MiniQMTRiskDecision:
        return MiniQMTRiskDecision.pass_(metadata={"runtime_id": runtime_id, "event_type": event_type})

    def evaluate_pre_submit(
        self,
        *,
        runtime_id: str,
        order: MiniQMTChildOrder,
        active_child_orders: list[MiniQMTChildOrder],
        active_algo_instances: list[MiniQMTExecutionAlgoInstance],
    ) -> MiniQMTRiskDecision:
        return MiniQMTRiskDecision.pass_(
            metadata={
                "runtime_id": runtime_id,
                "event_type": "PRE_SUBMIT",
                "child_order_id": order.child_order_id,
                "active_child_count": len(active_child_orders),
                "active_algo_count": len(active_algo_instances),
            }
        )


@dataclass(frozen=True)
class MiniQMTRiskPriceBand:
    min_price: float | None = None
    max_price: float | None = None

    def __post_init__(self) -> None:
        min_price = _positive_float_or_none(self.min_price, field_name="min_price", allow_zero=True)
        max_price = _positive_float_or_none(self.max_price, field_name="max_price", allow_zero=True)
        if min_price is not None and max_price is not None and min_price > max_price:
            raise ValueError(
                "MiniQMT risk price band min_price cannot exceed max_price; "
                "reason_code=MINIQMT_RISK_PRICE_BAND_INVALID"
            )
        object.__setattr__(self, "min_price", min_price)
        object.__setattr__(self, "max_price", max_price)


@dataclass(frozen=True)
class MiniQMTRiskRuleSet:
    """Configurable realtime rule set mounted by the event-loop runtime."""

    enabled: bool = True
    kill_on_disconnect: bool = True
    max_child_order_quantity: int | None = None
    max_child_order_notional: float | None = None
    max_total_exposure: float | None = None
    max_symbol_exposure: dict[str, float] = field(default_factory=dict)
    max_loss: float | None = None
    max_realized_loss: float | None = None
    max_unrealized_loss: float | None = None
    price_bands: dict[str, MiniQMTRiskPriceBand] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "max_child_order_quantity",
            _positive_int_or_none(self.max_child_order_quantity, field_name="max_child_order_quantity"),
        )
        for field_name in (
            "max_child_order_notional",
            "max_total_exposure",
            "max_loss",
            "max_realized_loss",
            "max_unrealized_loss",
        ):
            object.__setattr__(
                self,
                field_name,
                _positive_float_or_none(getattr(self, field_name), field_name=field_name, allow_zero=False),
            )
        object.__setattr__(
            self,
            "max_symbol_exposure",
            {
                _normalize_symbol(symbol): _positive_float(value, field_name=f"max_symbol_exposure[{symbol!r}]")
                for symbol, value in dict(self.max_symbol_exposure or {}).items()
            },
        )
        object.__setattr__(
            self,
            "price_bands",
            {
                _normalize_symbol(symbol): band
                if isinstance(band, MiniQMTRiskPriceBand)
                else MiniQMTRiskPriceBand(**dict(band or {}))
                for symbol, band in dict(self.price_bands or {}).items()
            },
        )

    @classmethod
    def from_config(cls, config: dict[str, Any] | None) -> "MiniQMTRiskRuleSet":
        payload = dict(config or {})
        bands = payload.get("price_bands") or {}
        payload["price_bands"] = {
            str(symbol): band if isinstance(band, MiniQMTRiskPriceBand) else MiniQMTRiskPriceBand(**dict(band or {}))
            for symbol, band in dict(bands).items()
        }
        return cls(**payload)


class ConfigurableMiniQMTRiskEngine:
    """Realtime risk engine for Phase 4 event-loop validation.

    It keeps a small in-process view of latest account/tick facts, while order,
    trade, cash, and position authority remains in qmt_strategy_ledger.
    """

    def __init__(self, rules: MiniQMTRiskRuleSet | dict[str, Any] | None = None) -> None:
        self.rules = rules if isinstance(rules, MiniQMTRiskRuleSet) else MiniQMTRiskRuleSet.from_config(rules)
        self._latest_prices: dict[str, float] = {}
        self._account_snapshots: dict[str, dict[str, Any]] = {}

    def evaluate_event(
        self,
        *,
        runtime_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> MiniQMTRiskDecision:
        if not self.rules.enabled:
            return MiniQMTRiskDecision.pass_(metadata={"runtime_id": runtime_id, "event_type": event_type})
        normalized_type = str(event_type or "").strip().upper()
        if normalized_type == "GATEWAY_DISCONNECTED" and self.rules.kill_on_disconnect:
            return MiniQMTRiskDecision.kill_switch(
                reason_code="MINIQMT_RISK_DISCONNECT_KILL_SWITCH",
                reason="MiniQMT gateway disconnect breached realtime risk policy",
                metadata={"runtime_id": runtime_id, "event_type": normalized_type, "payload": dict(payload)},
            )
        if normalized_type == "TICK":
            return self._evaluate_tick(runtime_id=runtime_id, event_type=normalized_type, payload=payload)
        if normalized_type == "ACCOUNT_EVENT":
            return self._evaluate_account(runtime_id=runtime_id, event_type=normalized_type, payload=payload)
        return MiniQMTRiskDecision.pass_(metadata={"runtime_id": runtime_id, "event_type": normalized_type})

    def evaluate_pre_submit(
        self,
        *,
        runtime_id: str,
        order: MiniQMTChildOrder,
        active_child_orders: list[MiniQMTChildOrder],
        active_algo_instances: list[MiniQMTExecutionAlgoInstance],
    ) -> MiniQMTRiskDecision:
        if not self.rules.enabled:
            return MiniQMTRiskDecision.pass_(metadata={"runtime_id": runtime_id, "event_type": "PRE_SUBMIT"})
        if (
            self.rules.max_child_order_quantity is not None
            and int(order.quantity) > self.rules.max_child_order_quantity
        ):
            return MiniQMTRiskDecision.kill_switch(
                reason_code="MINIQMT_RISK_ORDER_QUANTITY_LIMIT_BREACH",
                reason="MiniQMT child order quantity breached realtime risk limit",
                metadata={
                    "runtime_id": runtime_id,
                    "child_order_id": order.child_order_id,
                    "quantity": int(order.quantity),
                    "max_child_order_quantity": self.rules.max_child_order_quantity,
                },
            )
        notional = _order_notional(order)
        if self.rules.max_child_order_notional is not None and notional > self.rules.max_child_order_notional:
            return MiniQMTRiskDecision.kill_switch(
                reason_code="MINIQMT_RISK_ORDER_NOTIONAL_LIMIT_BREACH",
                reason="MiniQMT child order notional breached realtime risk limit",
                metadata={
                    "runtime_id": runtime_id,
                    "child_order_id": order.child_order_id,
                    "notional": notional,
                    "max_child_order_notional": self.rules.max_child_order_notional,
                },
            )
        exposure_decision = self._evaluate_pre_submit_exposure(
            runtime_id=runtime_id,
            order=order,
            active_child_orders=active_child_orders,
            active_algo_instances=active_algo_instances,
        )
        if exposure_decision.action == MiniQMTRiskDecisionAction.KILL_SWITCH:
            return exposure_decision
        return MiniQMTRiskDecision.pass_(
            metadata={
                "runtime_id": runtime_id,
                "event_type": "PRE_SUBMIT",
                "child_order_id": order.child_order_id,
                "notional": notional,
            }
        )

    def _evaluate_tick(self, *, runtime_id: str, event_type: str, payload: dict[str, Any]) -> MiniQMTRiskDecision:
        symbol = _normalize_symbol(payload.get("symbol") or payload.get("stock_code") or payload.get("code"))
        price = _float_or_none(payload.get("price") or payload.get("last_price") or payload.get("current_price"))
        if symbol and price is not None:
            self._latest_prices[symbol] = price
        band = self.rules.price_bands.get(symbol)
        if band is None or price is None:
            return MiniQMTRiskDecision.pass_(metadata={"runtime_id": runtime_id, "event_type": event_type, "symbol": symbol})
        if band.max_price is not None and price > band.max_price:
            return MiniQMTRiskDecision.kill_switch(
                reason_code="MINIQMT_RISK_TICK_PRICE_LIMIT_BREACH",
                reason="MiniQMT tick price breached realtime upper limit",
                metadata={"runtime_id": runtime_id, "symbol": symbol, "price": price, "max_price": band.max_price},
            )
        if band.min_price is not None and price < band.min_price:
            return MiniQMTRiskDecision.kill_switch(
                reason_code="MINIQMT_RISK_TICK_PRICE_LIMIT_BREACH",
                reason="MiniQMT tick price breached realtime lower limit",
                metadata={"runtime_id": runtime_id, "symbol": symbol, "price": price, "min_price": band.min_price},
            )
        return MiniQMTRiskDecision.pass_(metadata={"runtime_id": runtime_id, "event_type": event_type, "symbol": symbol})

    def _evaluate_account(self, *, runtime_id: str, event_type: str, payload: dict[str, Any]) -> MiniQMTRiskDecision:
        snapshot = _account_snapshot(payload)
        self._account_snapshots[runtime_id] = snapshot
        loss_decision = self._evaluate_loss(runtime_id=runtime_id, snapshot=snapshot)
        if loss_decision.action == MiniQMTRiskDecisionAction.KILL_SWITCH:
            return loss_decision
        exposure_decision = self._evaluate_account_exposure(runtime_id=runtime_id, snapshot=snapshot)
        if exposure_decision.action == MiniQMTRiskDecisionAction.KILL_SWITCH:
            return exposure_decision
        return MiniQMTRiskDecision.pass_(metadata={"runtime_id": runtime_id, "event_type": event_type})

    def _evaluate_loss(self, *, runtime_id: str, snapshot: dict[str, Any]) -> MiniQMTRiskDecision:
        realized_pnl = _float_or_none(snapshot.get("realized_pnl"))
        unrealized_pnl = _float_or_none(snapshot.get("unrealized_pnl"))
        total_pnl = _float_or_none(snapshot.get("total_pnl"))
        if total_pnl is None and (realized_pnl is not None or unrealized_pnl is not None):
            total_pnl = float(realized_pnl or 0.0) + float(unrealized_pnl or 0.0)
        checks = [
            ("MINIQMT_RISK_REALIZED_LOSS_LIMIT_BREACH", self.rules.max_realized_loss, realized_pnl, "realized_pnl"),
            ("MINIQMT_RISK_UNREALIZED_LOSS_LIMIT_BREACH", self.rules.max_unrealized_loss, unrealized_pnl, "unrealized_pnl"),
            ("MINIQMT_RISK_LOSS_LIMIT_BREACH", self.rules.max_loss, total_pnl, "total_pnl"),
        ]
        for reason_code, limit, pnl, field_name in checks:
            if limit is None:
                continue
            if pnl is None:
                return MiniQMTRiskDecision.kill_switch(
                    reason_code="MINIQMT_RISK_ACCOUNT_SNAPSHOT_MISSING",
                    reason="MiniQMT account snapshot is missing configured loss field",
                    metadata={"runtime_id": runtime_id, "field": field_name, "configured_limit": limit},
                )
            if pnl < -float(limit):
                return MiniQMTRiskDecision.kill_switch(
                    reason_code=reason_code,
                    reason="MiniQMT account loss breached realtime risk limit",
                    metadata={"runtime_id": runtime_id, "field": field_name, "pnl": pnl, "max_loss": limit},
                )
        return MiniQMTRiskDecision.pass_(metadata={"runtime_id": runtime_id, "event_type": "ACCOUNT_LOSS_CHECK"})

    def _evaluate_account_exposure(self, *, runtime_id: str, snapshot: dict[str, Any]) -> MiniQMTRiskDecision:
        exposure_by_symbol = _position_exposure_by_symbol(snapshot.get("positions"), latest_prices=self._latest_prices)
        market_value = _float_or_none(snapshot.get("market_value"))
        total_exposure = market_value if market_value is not None else sum(exposure_by_symbol.values())
        if self.rules.max_total_exposure is not None and total_exposure > self.rules.max_total_exposure:
            return MiniQMTRiskDecision.kill_switch(
                reason_code="MINIQMT_RISK_EXPOSURE_LIMIT_BREACH",
                reason="MiniQMT account total exposure breached realtime risk limit",
                metadata={
                    "runtime_id": runtime_id,
                    "total_exposure": total_exposure,
                    "max_total_exposure": self.rules.max_total_exposure,
                },
            )
        for symbol, max_exposure in self.rules.max_symbol_exposure.items():
            exposure = exposure_by_symbol.get(symbol, 0.0)
            if exposure > max_exposure:
                return MiniQMTRiskDecision.kill_switch(
                    reason_code="MINIQMT_RISK_EXPOSURE_LIMIT_BREACH",
                    reason="MiniQMT symbol exposure breached realtime risk limit",
                    metadata={
                        "runtime_id": runtime_id,
                        "symbol": symbol,
                        "symbol_exposure": exposure,
                        "max_symbol_exposure": max_exposure,
                    },
                )
        return MiniQMTRiskDecision.pass_(metadata={"runtime_id": runtime_id, "event_type": "ACCOUNT_EXPOSURE_CHECK"})

    def _evaluate_pre_submit_exposure(
        self,
        *,
        runtime_id: str,
        order: MiniQMTChildOrder,
        active_child_orders: list[MiniQMTChildOrder],
        active_algo_instances: list[MiniQMTExecutionAlgoInstance],
    ) -> MiniQMTRiskDecision:
        del active_algo_instances
        if self.rules.max_total_exposure is None and not self.rules.max_symbol_exposure:
            return MiniQMTRiskDecision.pass_(metadata={"runtime_id": runtime_id, "event_type": "PRE_SUBMIT_EXPOSURE"})
        snapshot = self._account_snapshots.get(runtime_id, {})
        base_by_symbol = _position_exposure_by_symbol(snapshot.get("positions"), latest_prices=self._latest_prices)
        active_by_symbol: dict[str, float] = {}
        for child in active_child_orders:
            if child.side != OrderSide.BUY:
                continue
            active_by_symbol[_normalize_symbol(child.symbol)] = active_by_symbol.get(_normalize_symbol(child.symbol), 0.0) + _order_notional(child)
        new_by_symbol = dict(base_by_symbol)
        for symbol, active_notional in active_by_symbol.items():
            new_by_symbol[symbol] = new_by_symbol.get(symbol, 0.0) + active_notional
        if order.side == OrderSide.BUY:
            symbol = _normalize_symbol(order.symbol)
            new_by_symbol[symbol] = new_by_symbol.get(symbol, 0.0) + _order_notional(order)
        if self.rules.max_total_exposure is not None:
            total = sum(new_by_symbol.values())
            if total > self.rules.max_total_exposure:
                return MiniQMTRiskDecision.kill_switch(
                    reason_code="MINIQMT_RISK_EXPOSURE_LIMIT_BREACH",
                    reason="MiniQMT pre-submit total exposure breached realtime risk limit",
                    metadata={
                        "runtime_id": runtime_id,
                        "child_order_id": order.child_order_id,
                        "projected_total_exposure": total,
                        "max_total_exposure": self.rules.max_total_exposure,
                    },
                )
        for symbol, max_exposure in self.rules.max_symbol_exposure.items():
            exposure = new_by_symbol.get(symbol, 0.0)
            if exposure > max_exposure:
                return MiniQMTRiskDecision.kill_switch(
                    reason_code="MINIQMT_RISK_EXPOSURE_LIMIT_BREACH",
                    reason="MiniQMT pre-submit symbol exposure breached realtime risk limit",
                    metadata={
                        "runtime_id": runtime_id,
                        "child_order_id": order.child_order_id,
                        "symbol": symbol,
                        "projected_symbol_exposure": exposure,
                        "max_symbol_exposure": max_exposure,
                    },
                )
        return MiniQMTRiskDecision.pass_(metadata={"runtime_id": runtime_id, "event_type": "PRE_SUBMIT_EXPOSURE"})


def _account_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    snapshot = dict(payload.get("account") if isinstance(payload.get("account"), dict) else payload)
    if "total_pnl" not in snapshot and "pnl" in snapshot:
        snapshot["total_pnl"] = snapshot.get("pnl")
    return snapshot


def _position_exposure_by_symbol(value: Any, *, latest_prices: dict[str, float]) -> dict[str, float]:
    if value is None:
        return {}
    positions = value.values() if isinstance(value, dict) else value
    exposure: dict[str, float] = {}
    for item in positions or []:
        if not isinstance(item, dict):
            continue
        symbol = _normalize_symbol(item.get("symbol") or item.get("stock_code") or item.get("code"))
        if not symbol:
            continue
        explicit = _float_or_none(item.get("market_value") or item.get("exposure") or item.get("notional"))
        if explicit is not None:
            exposure[symbol] = exposure.get(symbol, 0.0) + max(explicit, 0.0)
            continue
        quantity = _float_or_none(item.get("quantity") or item.get("volume") or item.get("current_amount"))
        price = _float_or_none(item.get("price") or item.get("last_price") or item.get("cost_price"))
        if price is None:
            price = latest_prices.get(symbol)
        if quantity is None or price is None:
            continue
        exposure[symbol] = exposure.get(symbol, 0.0) + max(quantity, 0.0) * max(price, 0.0)
    return exposure


def _order_notional(order: MiniQMTChildOrder) -> float:
    return max(float(order.quantity), 0.0) * max(float(order.price), 0.0)


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _positive_int_or_none(value: Any, *, field_name: str) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer; reason_code=MINIQMT_RISK_CONFIG_INVALID") from exc
    if parsed <= 0:
        raise ValueError(f"{field_name} must be positive; reason_code=MINIQMT_RISK_CONFIG_INVALID")
    return parsed


def _positive_float_or_none(value: Any, *, field_name: str, allow_zero: bool) -> float | None:
    if value is None:
        return None
    parsed = _positive_float(value, field_name=field_name, allow_zero=allow_zero)
    return parsed


def _positive_float(value: Any, *, field_name: str, allow_zero: bool = False) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be numeric; reason_code=MINIQMT_RISK_CONFIG_INVALID") from exc
    if allow_zero:
        if parsed < 0:
            raise ValueError(f"{field_name} must be non-negative; reason_code=MINIQMT_RISK_CONFIG_INVALID")
    elif parsed <= 0:
        raise ValueError(f"{field_name} must be positive; reason_code=MINIQMT_RISK_CONFIG_INVALID")
    return parsed

