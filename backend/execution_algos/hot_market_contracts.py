"""Pure process-local market observation and economic-effect contracts.

This module is owned by the execution-algorithm boundary.  It contains no
runtime service, repository, SQL, outbox, reconciliation, gateway or broker
dependency.  Runtime actors may route these values, while algorithms may
consume them without importing a runtime service package.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Protocol

from backend.services.miniqmt_execution_runtime.plugin_canonical import (
    FrozenJsonObjectV1,
    freeze_json_v1,
    thaw_json_v1,
)


_FORBIDDEN_DURABLE_MARKET_KEYS = frozenset(
    {
        "market_data_id",
        "market_data_lineage",
        "last_market_data_lineage",
        "last_tick_lineage",
        "normalized_quote_sha256",
        "payload_sha256",
        "raw_quote",
        "quote_payload",
        "bid_prices",
        "ask_prices",
        "bid_quantities",
        "ask_quantities",
    }
)
_COMMON_EFFECT_KEYS = frozenset({"action", "action_time_utc", "exchange_trade_date", "session_epoch", "session_phase"})
_SUBMIT_EFFECT_KEYS = _COMMON_EFFECT_KEYS | frozenset(
    {"symbol", "side", "price_decimal", "quantity", "reason_code", "draw_ordinal"}
)
_CANCEL_EFFECT_KEYS = _COMMON_EFFECT_KEYS | frozenset({"reason_code"})
_A_SHARE_SYMBOL_PATTERN = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")


@dataclass(frozen=True)
class HotMarketDataViewV1:
    runtime_id: str
    symbol: str
    generation: int
    sequence: int
    observed_at_utc: datetime
    exchange_time_utc: datetime
    exchange_trade_date: str
    session_epoch: str
    session_phase: str
    bid_price_1: Decimal
    ask_price_1: Decimal
    bid_volume_1: int
    ask_volume_1: int
    last_price: Decimal | None
    pre_close: Decimal | None
    limit_up: Decimal | None
    limit_down: Decimal | None

    def __post_init__(self) -> None:
        for name in ("runtime_id", "symbol", "exchange_trade_date", "session_epoch", "session_phase"):
            value = getattr(self, name)
            if type(value) is not str or not value or value != value.strip():
                raise TypeError(f"{name} must be a canonical identity")
        if type(self.generation) is not int or self.generation <= 0:
            raise TypeError("generation must be a positive strict integer")
        if type(self.sequence) is not int or self.sequence <= 0:
            raise TypeError("sequence must be a positive strict integer")
        for name in ("observed_at_utc", "exchange_time_utc"):
            value = getattr(self, name)
            if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
                raise TypeError(f"{name} must be timezone-aware")
            object.__setattr__(self, name, value.astimezone(UTC))
        for name in ("bid_price_1", "ask_price_1"):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
                raise TypeError(f"{name} must be a positive finite Decimal")
        for name in ("bid_volume_1", "ask_volume_1"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise TypeError(f"{name} must be a nonnegative strict integer")
        for name in ("last_price", "pre_close", "limit_up", "limit_down"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, Decimal) or not value.is_finite() or value < 0):
                raise TypeError(f"{name} must be a nonnegative finite Decimal or None")


def _assert_economic_payload_v1(value: Any, *, path: str = "effect") -> None:
    if value is None or type(value) in (bool, int, str):
        return
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError(f"{path} contains a non-finite decimal")
        return
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{path} contains a naive datetime")
        return
    if type(value) in (tuple, list):
        for index, item in enumerate(value):
            _assert_economic_payload_v1(item, path=f"{path}[{index}]")
        return
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str or not key or key != key.strip():
                raise ValueError(f"{path} contains an invalid field name")
            lowered = key.lower()
            if lowered in _FORBIDDEN_DURABLE_MARKET_KEYS or any(
                token in lowered for token in ("quote_hash", "market_payload", "tick_payload")
            ):
                raise ValueError(f"{path}.{key} is prohibited hot market-data evidence")
            _assert_economic_payload_v1(item, path=f"{path}.{key}")
        return
    raise TypeError(f"{path} contains unsupported value type {type(value).__name__}")


def validate_hot_market_economic_payload_v1(value: dict[str, Any] | FrozenJsonObjectV1) -> dict[str, Any]:
    plain = thaw_json_v1(value) if isinstance(value, FrozenJsonObjectV1) else value
    if type(plain) is not dict or not plain:
        raise TypeError("economic payload must be a nonempty strict dict")
    _assert_economic_payload_v1(plain)
    action = plain.get("action")
    if action not in {"SUBMIT_LIMIT", "CANCEL_ORDER"}:
        raise ValueError("economic payload action is unsupported")
    allowed = _SUBMIT_EFFECT_KEYS if action == "SUBMIT_LIMIT" else _CANCEL_EFFECT_KEYS
    required = _SUBMIT_EFFECT_KEYS - {"draw_ordinal"} if action == "SUBMIT_LIMIT" else _CANCEL_EFFECT_KEYS
    if not required.issubset(plain) or not set(plain).issubset(allowed):
        raise ValueError("economic payload fields do not close to the action schema")
    for name in ("action_time_utc", "exchange_trade_date", "session_epoch", "session_phase", "reason_code"):
        member = plain.get(name)
        if type(member) is not str or not member or member != member.strip():
            raise ValueError(f"economic payload {name} must be a canonical string")
    action_time = plain["action_time_utc"]
    if not action_time.endswith("Z"):
        raise ValueError("economic payload action_time_utc must be canonical UTC")
    try:
        parsed_action_time = datetime.fromisoformat(action_time[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError("economic payload action_time_utc is invalid") from exc
    if parsed_action_time.utcoffset() != UTC.utcoffset(parsed_action_time):
        raise ValueError("economic payload action_time_utc must be canonical UTC")
    try:
        parsed_trade_date = date.fromisoformat(plain["exchange_trade_date"])
    except ValueError as exc:
        raise ValueError("economic payload exchange_trade_date is invalid") from exc
    if parsed_trade_date.isoformat() != plain["exchange_trade_date"]:
        raise ValueError("economic payload exchange_trade_date is not canonical")
    if action == "SUBMIT_LIMIT":
        if (
            type(plain.get("symbol")) is not str
            or _A_SHARE_SYMBOL_PATTERN.fullmatch(plain["symbol"]) is None
            or type(plain.get("side")) is not str
        ):
            raise ValueError("SUBMIT economic payload owner fields are invalid")
        if plain["side"] not in {"BUY", "SELL"}:
            raise ValueError("SUBMIT economic payload side is invalid")
        try:
            price = Decimal(plain["price_decimal"])
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("SUBMIT economic payload price is invalid") from exc
        if not price.is_finite() or price <= 0 or format(price, "f") != plain["price_decimal"]:
            raise ValueError("SUBMIT economic payload price is not canonical positive decimal")
        if type(plain.get("quantity")) is not int or plain["quantity"] <= 0:
            raise ValueError("SUBMIT economic payload quantity is invalid")
        if "draw_ordinal" in plain and (type(plain["draw_ordinal"]) is not int or plain["draw_ordinal"] < 0):
            raise ValueError("SUBMIT economic payload draw ordinal is invalid")
    return plain


@dataclass(frozen=True)
class HotMarketDataEconomicEffectV1:
    runtime_id: str
    algo_instance_id: str
    expected_algo_row_version: int
    effect_identity: str
    economic_payload: FrozenJsonObjectV1 | dict[str, Any]

    def __post_init__(self) -> None:
        for name in ("runtime_id", "algo_instance_id", "effect_identity"):
            value = getattr(self, name)
            if type(value) is not str or not value or value != value.strip():
                raise TypeError(f"{name} must be a canonical identity")
        if type(self.expected_algo_row_version) is not int or self.expected_algo_row_version <= 0:
            raise TypeError("expected_algo_row_version must be a positive strict integer")
        plain = validate_hot_market_economic_payload_v1(self.economic_payload)
        frozen = freeze_json_v1(plain)
        if not isinstance(frozen, FrozenJsonObjectV1) or not frozen:
            raise TypeError("economic_payload must be a nonempty strict dict")
        object.__setattr__(self, "economic_payload", frozen)


class HotMarketDataTargetV1(Protocol):
    runtime_id: str
    algo_instance_id: str
    symbol: str

    def evaluate_hot_market_data_v1(self, view: HotMarketDataViewV1) -> HotMarketDataEconomicEffectV1 | None: ...

    def accept_committed_effect_v1(self, effect: HotMarketDataEconomicEffectV1, readback: Any) -> None: ...


__all__ = [
    "HotMarketDataEconomicEffectV1",
    "HotMarketDataTargetV1",
    "HotMarketDataViewV1",
    "validate_hot_market_economic_payload_v1",
]
