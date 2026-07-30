"""Exact transition-local vn.py DTO/enum projection for K4."""

from __future__ import annotations

import ast
import math
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable

from backend.services.miniqmt_execution_runtime.plugin_canonical import (
    canonical_decimal_string_v1,
    canonical_utc_datetime_v1,
    hash_hex_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    NormalizedOrderStatusV1,
)

from .facade_contracts import VnpyFacadeContractError, VnpyFacadeDtoMappingV1
from .locked_surface import PINNED_SOURCE_ROOT

_UTILITY_PATH = "vnpy_core/vnpy/trader/utility.py"
_UTILITY_SIZE = 32957
_UTILITY_SHA256 = "9bce3f6e18c84668b0ffadd717f0b6fd4ca2b454dc748dad6572af78c850608d"


class Direction(Enum):
    LONG = "多"
    SHORT = "空"


class Offset(Enum):
    NONE = ""


class OrderType(Enum):
    LIMIT = "限价"


class AlgoStatus(Enum):
    RUNNING = "运行"
    PAUSED = "暂停"
    STOPPED = "停止"
    FINISHED = "结束"


class Exchange(Enum):
    SSE = "SSE"
    SZSE = "SZSE"
    BSE = "BSE"


class Status(Enum):
    NOTTRADED = "未成交"
    PARTTRADED = "部分成交"
    ALLTRADED = "全部成交"
    CANCELLED = "已撤销"
    REJECTED = "拒单"


def _strict_finite_number(value: Any, *, field_name: str, positive: bool = False) -> float:
    if type(value) not in (int, float):
        if type(value) is bool:
            raise TypeError(f"{field_name} must not be bool")
        raise TypeError(f"{field_name} must be an int or float")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"{field_name} must be finite")
    if positive and normalized <= 0:
        raise ValueError(f"{field_name} must be finite positive")
    return normalized


@dataclass(frozen=True, slots=True)
class TickData:
    vt_symbol: str
    datetime: datetime
    bid_price_1: float
    bid_volume_1: float
    ask_price_1: float
    ask_volume_1: float
    last_price: float
    limit_up: float
    limit_down: float

    def __post_init__(self) -> None:
        if type(self.vt_symbol) is not str or not self.vt_symbol:
            raise TypeError("TickData.vt_symbol must be a non-empty string")
        if not isinstance(self.datetime, datetime) or self.datetime.tzinfo is None:
            raise ValueError("TickData.datetime must be timezone aware")
        for field in (
            "bid_price_1",
            "bid_volume_1",
            "ask_price_1",
            "ask_volume_1",
            "last_price",
            "limit_up",
            "limit_down",
        ):
            value = _strict_finite_number(getattr(self, field), field_name=f"TickData.{field}")
            if field.endswith("volume_1") and value < 0:
                raise ValueError(f"TickData.{field} must be non-negative")
            object.__setattr__(self, field, value)


@dataclass(frozen=True, slots=True)
class OrderData:
    vt_orderid: str
    status: Status
    traded: float
    price: float

    def __post_init__(self) -> None:
        if type(self.vt_orderid) is not str or not self.vt_orderid:
            raise TypeError("OrderData.vt_orderid must be a non-empty string")
        if not isinstance(self.status, Status):
            raise TypeError("OrderData.status must be Status")
        traded = _strict_finite_number(self.traded, field_name="OrderData.traded")
        price = _strict_finite_number(self.price, field_name="OrderData.price", positive=True)
        if traded < 0:
            raise ValueError("OrderData.traded must be non-negative")
        object.__setattr__(self, "traded", traded)
        object.__setattr__(self, "price", price)

    def is_active(self) -> bool:
        return self.status in (Status.NOTTRADED, Status.PARTTRADED)


@dataclass(frozen=True, slots=True)
class TradeData:
    vt_orderid: str
    vt_tradeid: str
    price: float
    volume: float
    datetime: datetime | None

    def __post_init__(self) -> None:
        for field in ("vt_orderid", "vt_tradeid"):
            value = getattr(self, field)
            if type(value) is not str or not value:
                raise TypeError(f"TradeData.{field} must be a non-empty string")
        object.__setattr__(
            self,
            "price",
            _strict_finite_number(self.price, field_name="TradeData.price", positive=True),
        )
        object.__setattr__(
            self,
            "volume",
            _strict_finite_number(self.volume, field_name="TradeData.volume", positive=True),
        )
        if self.datetime is not None and (not isinstance(self.datetime, datetime) or self.datetime.tzinfo is None):
            raise ValueError("TradeData.datetime must be null or timezone aware")


@dataclass(frozen=True, slots=True)
class ContractData:
    symbol: str
    exchange: Exchange
    gateway_name: str
    min_volume: float
    pricetick: float

    def __post_init__(self) -> None:
        if type(self.symbol) is not str or len(self.symbol) != 6 or not self.symbol.isdigit():
            raise ValueError("ContractData.symbol must be a six-digit code")
        if not isinstance(self.exchange, Exchange):
            raise TypeError("ContractData.exchange must be Exchange")
        if (
            type(self.gateway_name) is not str
            or not self.gateway_name
            or self.gateway_name != self.gateway_name.strip()
        ):
            raise ValueError("ContractData.gateway_name must be trim-stable")
        object.__setattr__(
            self,
            "min_volume",
            _strict_finite_number(self.min_volume, field_name="ContractData.min_volume", positive=True),
        )
        object.__setattr__(
            self,
            "pricetick",
            _strict_finite_number(self.pricetick, field_name="ContractData.pricetick", positive=True),
        )


_ORDER_STATUS = {
    NormalizedOrderStatusV1.ACCEPTED: Status.NOTTRADED,
    NormalizedOrderStatusV1.PARTIALLY_FILLED: Status.PARTTRADED,
    NormalizedOrderStatusV1.FILLED: Status.ALLTRADED,
    NormalizedOrderStatusV1.CANCELLED: Status.CANCELLED,
    NormalizedOrderStatusV1.REJECTED: Status.REJECTED,
}
_EXCHANGE_BY_SUFFIX = {"SH": Exchange.SSE, "SZ": Exchange.SZSE, "BJ": Exchange.BSE}


def project_order_status_v1(value: NormalizedOrderStatusV1) -> Status:
    if not isinstance(value, NormalizedOrderStatusV1):
        raise TypeError("normalized order status must be NormalizedOrderStatusV1")
    try:
        return _ORDER_STATUS[value]
    except KeyError as exc:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_DTO_MAPPING_INVALID",
            "normalized order status is not mapped",
            context={"status": getattr(value, "value", None)},
        ) from exc


def project_contract_data_v1(
    *,
    symbol: str,
    gateway_name: str,
    min_volume: Any,
    pricetick_decimal: Any,
) -> ContractData:
    if type(symbol) is not str or len(symbol) != 9 or symbol[6] != ".":
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_DTO_MAPPING_INVALID",
            "contract symbol is not normalized",
            context={"symbol": symbol},
        )
    try:
        exchange = _EXCHANGE_BY_SUFFIX[symbol[7:]]
    except KeyError as exc:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_DTO_MAPPING_INVALID",
            "contract exchange suffix is unsupported",
            context={"symbol": symbol},
        ) from exc
    min_volume_text = canonical_decimal_string_v1(
        min_volume,
        field_name="min_volume",
        allow_zero=False,
    )
    pricetick_text = canonical_decimal_string_v1(
        pricetick_decimal,
        field_name="pricetick_decimal",
        allow_zero=False,
    )
    return ContractData(
        symbol=symbol[:6],
        exchange=exchange,
        gateway_name=gateway_name,
        min_volume=float(min_volume_text),
        pricetick=float(pricetick_text),
    )


def project_tick_data_v1(*, symbol: str, payload: Any) -> TickData:
    if not isinstance(payload, dict):
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_MARKET_DATA_INVALID",
            "immutable market-data projection must be a strict object",
            context={"symbol": symbol, "payload_type": type(payload).__name__},
        )
    required = {
        "symbol",
        "logical_at_utc",
        "bid_price_1",
        "bid_volume_1",
        "ask_price_1",
        "ask_volume_1",
        "last_price",
        "limit_up",
        "limit_down",
    }
    missing = sorted(required - set(payload))
    try:
        if missing:
            raise ValueError("market-data projection is missing required fields")
        if type(payload["symbol"]) is not str or payload["symbol"] != symbol:
            raise ValueError("market-data projection symbol conflicts with transition owner")
        logical = datetime.fromisoformat(
            canonical_utc_datetime_v1(payload["logical_at_utc"], field_name="market_data.logical_at_utc").replace(
                "Z", "+00:00"
            )
        )
        prices = {
            field: float(
                canonical_decimal_string_v1(
                    payload[field],
                    field_name=f"market_data.{field}",
                    allow_zero=True,
                )
            )
            for field in ("bid_price_1", "ask_price_1", "last_price", "limit_up", "limit_down")
        }
        volumes: dict[str, float] = {}
        for field in ("bid_volume_1", "ask_volume_1"):
            value = payload[field]
            if type(value) is not int or value < 0:
                raise TypeError(f"market_data.{field} must be a non-negative strict integer share quantity")
            volumes[field] = float(value)
        return TickData(
            vt_symbol=symbol.replace(".SH", ".SSE").replace(".SZ", ".SZSE").replace(".BJ", ".BSE"),
            datetime=logical,
            bid_price_1=prices["bid_price_1"],
            bid_volume_1=volumes["bid_volume_1"],
            ask_price_1=prices["ask_price_1"],
            ask_volume_1=volumes["ask_volume_1"],
            last_price=prices["last_price"],
            limit_up=prices["limit_up"],
            limit_down=prices["limit_down"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_MARKET_DATA_INVALID",
            "immutable market-data projection is malformed",
            context={
                "symbol": symbol,
                "missing_fields": missing,
                "error_type": f"{type(exc).__module__}.{type(exc).__qualname__}",
                "error_message": str(exc),
            },
        ) from exc


def _round_to_node_v1(source_root: Path) -> ast.FunctionDef:
    path = source_root / _UTILITY_PATH
    if not path.is_file():
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_SOURCE_INVALID",
            "pinned utility source is missing",
            context={"source_path": _UTILITY_PATH},
        )
    payload = path.read_bytes()
    import hashlib

    actual_hash = hashlib.sha256(payload).hexdigest()
    if len(payload) != _UTILITY_SIZE or actual_hash != _UTILITY_SHA256:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_SOURCE_INVALID",
            "pinned utility source bytes drifted",
            context={
                "source_path": _UTILITY_PATH,
                "expected_size": _UTILITY_SIZE,
                "actual_size": len(payload),
                "expected_sha256": _UTILITY_SHA256,
                "actual_sha256": actual_hash,
            },
        )
    try:
        tree = ast.parse(payload.decode("utf-8"), filename=_UTILITY_PATH)
    except (UnicodeDecodeError, SyntaxError) as exc:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_SOURCE_INVALID",
            "pinned utility source cannot be parsed",
            context={"source_path": _UTILITY_PATH, "error": exc},
        ) from exc
    nodes = [item for item in tree.body if isinstance(item, ast.FunctionDef) and item.name == "round_to"]
    if len(nodes) != 1:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_SOURCE_INVALID",
            "pinned utility source must define exactly one round_to",
            context={"source_path": _UTILITY_PATH, "definition_count": len(nodes)},
        )
    return nodes[0]


def build_pinned_round_to_v1(
    source_root: Path = PINNED_SOURCE_ROOT,
) -> Callable[[float, float], float]:
    if not isinstance(source_root, Path):
        raise TypeError("source_root must be pathlib.Path")
    node = _round_to_node_v1(source_root)
    module = ast.Module(
        body=[ast.ImportFrom(module="decimal", names=[ast.alias(name="Decimal")], level=0), node],
        type_ignores=[],
    )
    ast.fix_missing_locations(module)
    namespace: dict[str, Any] = {}
    exec(compile(module, filename="<pinned-round-to>", mode="exec"), namespace)  # noqa: S102
    extracted = namespace["round_to"]

    def pinned_round_to(value: float, target: float) -> float:
        normalized_value = _strict_finite_number(value, field_name="value")
        normalized_target = _strict_finite_number(target, field_name="target", positive=True)
        result = extracted(normalized_value, normalized_target)
        if type(result) is not float or not math.isfinite(result):
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_DTO_MAPPING_INVALID",
                "pinned round_to produced an invalid result",
                context={"value": normalized_value, "target": normalized_target},
            )
        return result

    pinned_round_to.__name__ = "pinned_round_to"
    pinned_round_to.__qualname__ = "build_pinned_round_to_v1.<locals>.pinned_round_to"
    return pinned_round_to


def dto_mapping_set_sha256_v1(mappings: tuple[VnpyFacadeDtoMappingV1, ...]) -> str:
    ordered = tuple(sorted(mappings, key=lambda item: (item.object_name, item.field_name)))
    if mappings != ordered or len({(item.object_name, item.field_name) for item in ordered}) != len(ordered):
        raise ValueError("DTO mappings must be unique and sorted")
    return hash_hex_v1(
        "miniqmt_vnpy_facade_dto_mapping_set_v1",
        [item.canonical_payload_v1() for item in ordered],
    )


def build_vnpy_facade_dto_mappings_v1() -> tuple[VnpyFacadeDtoMappingV1, ...]:
    """Build the exact K1/K2-to-vn.py projection table used by K4."""

    if (
        Direction.LONG.value != "\u591a"
        or Direction.SHORT.value != "\u7a7a"
        or Offset.NONE.value != ""
        or OrderType.LIMIT.value != "\u9650\u4ef7"
        or Exchange.SSE.value != "SSE"
        or Exchange.SZSE.value != "SZSE"
        or Exchange.BSE.value != "BSE"
    ):
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_DTO_MAPPING_INVALID",
            "selected enum values drifted from pinned core authority",
            context={},
        )
    specs = (
        (
            "ContractData",
            "exchange",
            "VnpyFacadeContractViewV1",
            "exchange_member",
            "EXACT_SELECTED_MEMBER",
            "FAIL",
            {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"},
        ),
        (
            "ContractData",
            "gateway_name",
            "VnpyFacadeContractViewV1",
            "gateway_name",
            "IDENTITY",
            "NONE_WITH_DIAGNOSTIC",
            {},
        ),
        (
            "ContractData",
            "min_volume",
            "VnpyFacadeContractViewV1",
            "min_volume",
            "CANONICAL_DECIMAL_TO_FINITE_FLOAT",
            "NONE_WITH_DIAGNOSTIC",
            {},
        ),
        (
            "ContractData",
            "pricetick",
            "VnpyFacadeContractViewV1",
            "pricetick_decimal",
            "CANONICAL_DECIMAL_TO_FINITE_FLOAT",
            "NONE_WITH_DIAGNOSTIC",
            {},
        ),
        (
            "ContractData",
            "symbol",
            "VnpyFacadeContractViewV1",
            "symbol",
            "NORMALIZED_A_SHARE_CODE",
            "NONE_WITH_DIAGNOSTIC",
            {},
        ),
        ("OrderData", "price", "OrderEventV1", "price_decimal", "CANONICAL_DECIMAL_TO_FINITE_FLOAT", "FAIL", {}),
        (
            "OrderData",
            "status",
            "OrderEventV1",
            "normalized_status",
            "EXACT_STATUS_TABLE",
            "FAIL",
            {item.value: _ORDER_STATUS[item].value for item in _ORDER_STATUS},
        ),
        ("OrderData", "traded", "OrderEventV1", "cumulative_quantity", "INTEGRAL_SHARES_TO_FLOAT", "FAIL", {}),
        ("OrderData", "vt_orderid", "ExecutionCommandChildMappingV1", "local_vt_orderid", "IDENTITY", "FAIL", {}),
        (
            "TickData",
            "ask_price_1",
            "MarketDataProjectionV2",
            "ask_price_1",
            "CANONICAL_DECIMAL_TO_FINITE_FLOAT",
            "NONE_WITH_DIAGNOSTIC",
            {},
        ),
        (
            "TickData",
            "ask_volume_1",
            "MarketDataProjectionV2",
            "ask_volume_1",
            "INTEGRAL_SHARES_TO_FLOAT",
            "NONE_WITH_DIAGNOSTIC",
            {},
        ),
        (
            "TickData",
            "bid_price_1",
            "MarketDataProjectionV2",
            "bid_price_1",
            "CANONICAL_DECIMAL_TO_FINITE_FLOAT",
            "NONE_WITH_DIAGNOSTIC",
            {},
        ),
        (
            "TickData",
            "bid_volume_1",
            "MarketDataProjectionV2",
            "bid_volume_1",
            "INTEGRAL_SHARES_TO_FLOAT",
            "NONE_WITH_DIAGNOSTIC",
            {},
        ),
        (
            "TickData",
            "datetime",
            "MarketDataProjectionV2",
            "logical_at_utc",
            "CANONICAL_UTC_DATETIME",
            "NONE_WITH_DIAGNOSTIC",
            {},
        ),
        (
            "TickData",
            "last_price",
            "MarketDataProjectionV2",
            "last_price",
            "CANONICAL_DECIMAL_TO_FINITE_FLOAT",
            "NONE_WITH_DIAGNOSTIC",
            {},
        ),
        (
            "TickData",
            "limit_down",
            "MarketDataProjectionV2",
            "limit_down",
            "CANONICAL_DECIMAL_TO_FINITE_FLOAT",
            "NONE_WITH_DIAGNOSTIC",
            {},
        ),
        (
            "TickData",
            "limit_up",
            "MarketDataProjectionV2",
            "limit_up",
            "CANONICAL_DECIMAL_TO_FINITE_FLOAT",
            "NONE_WITH_DIAGNOSTIC",
            {},
        ),
        (
            "TickData",
            "vt_symbol",
            "RuntimeEventEnvelopeV2",
            "symbol",
            "NORMALIZED_VT_SYMBOL",
            "NONE_WITH_DIAGNOSTIC",
            {},
        ),
        ("TradeData", "datetime", "TradeEventV1", "event_time_utc", "CANONICAL_UTC_DATETIME", "FAIL", {}),
        ("TradeData", "price", "TradeEventV1", "price_decimal", "CANONICAL_DECIMAL_TO_FINITE_FLOAT", "FAIL", {}),
        ("TradeData", "volume", "TradeEventV1", "quantity", "INTEGRAL_SHARES_TO_FLOAT", "FAIL", {}),
        ("TradeData", "vt_orderid", "ExecutionCommandChildMappingV1", "local_vt_orderid", "IDENTITY", "FAIL", {}),
        ("TradeData", "vt_tradeid", "TradeEventV1", "trade_id", "IDENTITY", "FAIL", {}),
    )
    mappings = tuple(
        sorted(
            (
                VnpyFacadeDtoMappingV1.create(
                    object_name=object_name,
                    field_name=field_name,
                    source_projection_type=source_type,
                    source_field_path=source_path,
                    conversion_rule=conversion,
                    missing_disposition=missing,
                    allowed_enum_mapping=enum_mapping,
                )
                for object_name, field_name, source_type, source_path, conversion, missing, enum_mapping in specs
            ),
            key=lambda item: (item.object_name, item.field_name),
        )
    )
    dto_mapping_set_sha256_v1(mappings)
    return mappings


def readback_vnpy_facade_dto_mappings_v1(payload: Any) -> tuple[VnpyFacadeDtoMappingV1, ...]:
    if type(payload) not in (tuple, list):
        raise TypeError("DTO mapping payload must be a tuple or JSON list")
    supplied = tuple(VnpyFacadeDtoMappingV1.model_validate(item, strict=True) for item in payload)
    expected = build_vnpy_facade_dto_mappings_v1()
    if supplied != expected:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_DTO_MAPPING_INVALID",
            "DTO mapping readback conflicts with live projection authority",
            context={
                "expected_set_sha256": dto_mapping_set_sha256_v1(expected),
                "actual_set_sha256": dto_mapping_set_sha256_v1(supplied),
            },
        )
    return expected


__all__ = [
    "AlgoStatus",
    "ContractData",
    "Direction",
    "Exchange",
    "Offset",
    "OrderData",
    "OrderType",
    "Status",
    "TickData",
    "TradeData",
    "build_pinned_round_to_v1",
    "build_vnpy_facade_dto_mappings_v1",
    "dto_mapping_set_sha256_v1",
    "project_contract_data_v1",
    "project_order_status_v1",
    "project_tick_data_v1",
    "readback_vnpy_facade_dto_mappings_v1",
]
