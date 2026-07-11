"""Algorithm-neutral Phase 1 quote, clock, tradability, and evidence DTOs.

Nothing in this module imports a runtime adapter, database, HTTP layer, or
broker SDK.  It is the stable contract shared by future MiniQMT adapters and
the eventual Adaptive IS core; it does not submit, cancel, subscribe, or
persist anything.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field, is_dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal, InvalidOperation
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping, Protocol, Sequence, TypeVar, runtime_checkable

from .reasons import QuoteContractReasonCode, QuoteContractStage, failure_definition, quote_contract_error


QUOTE_CONTRACT_SCHEMA_VERSION = "miniqmt_quote_contract_v2"
MARKET_DATA_EVIDENCE_SCHEMA_VERSION = "miniqmt_market_data_evidence_v1"
CLOSING_AUCTION_SCHEMA_VERSION = "miniqmt_closing_auction_v1"
SYMBOL_PATTERN = re.compile(r"^(?P<code>\d{6})\.(?P<market>SH|SZ|BJ)$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class MarketCode(str, Enum):
    SH = "SH"
    SZ = "SZ"
    BJ = "BJ"


class PriceBasis(str, Enum):
    RAW_CNY_PER_SHARE = "RAW_CNY_PER_SHARE"


class DepthQuantityUnit(str, Enum):
    SHARES = "SHARES"
    LOTS = "LOTS"
    UNKNOWN = "UNKNOWN"


class QuoteSource(str, Enum):
    MINIQMT_REALTIME_BROKER_QUOTE = "MINIQMT_REALTIME.broker_quote"


class QuoteSourceMethod(str, Enum):
    WHOLE_QUOTE_CALLBACK = "WHOLE_QUOTE_CALLBACK"
    BOOTSTRAP_FULL_TICK = "BOOTSTRAP_FULL_TICK"


class QuoteCapability(str, Enum):
    FIVE_LEVEL_DEPTH = "FIVE_LEVEL_DEPTH"
    EXCHANGE_TIMESTAMP = "EXCHANGE_TIMESTAMP"
    RAW_PRICE_BASIS = "RAW_PRICE_BASIS"
    DEPTH_UNIT_SHARES = "DEPTH_UNIT_SHARES"
    TRADABILITY = "TRADABILITY"
    CALENDAR = "CALENDAR"
    CLOSING_AUCTION_INDICATOR = "CLOSING_AUCTION_INDICATOR"


class QuoteValidationState(str, Enum):
    VALID = "VALID"
    INVALID = "INVALID"
    CAPABILITY_MISSING = "CAPABILITY_MISSING"


class BookState(str, Enum):
    NORMAL = "NORMAL"
    LOCKED = "LOCKED"
    UNAVAILABLE = "UNAVAILABLE"
    INVALID = "INVALID"


class TradabilityState(str, Enum):
    TRADABLE = "TRADABLE"
    SUSPENDED = "SUSPENDED"
    INTRADAY_HALT = "INTRADAY_HALT"
    LIMIT_UP_BUY_BLOCKED = "LIMIT_UP_BUY_BLOCKED"
    LIMIT_DOWN_SELL_BLOCKED = "LIMIT_DOWN_SELL_BLOCKED"
    STATUS_UNKNOWN = "STATUS_UNKNOWN"
    DATA_INVALID = "DATA_INVALID"


class MarketPhase(str, Enum):
    PRE_OPEN = "PRE_OPEN"
    CONTINUOUS = "CONTINUOUS"
    CLOSING_AUCTION = "CLOSING_AUCTION"
    CLOSED = "CLOSED"
    UNKNOWN = "UNKNOWN"


class EligibilityState(str, Enum):
    READY = "READY"
    WAITING_FIRST_QUOTE = "WAITING_FIRST_QUOTE"
    STALE = "STALE"
    INVALID = "INVALID"
    CAPABILITY_MISSING = "CAPABILITY_MISSING"
    NO_OPPOSITE_DEPTH = "NO_OPPOSITE_DEPTH"
    SUSPENDED = "SUSPENDED"
    LIMIT_BLOCKED = "LIMIT_BLOCKED"
    WRONG_SESSION = "WRONG_SESSION"
    CLOCK_INVALID = "CLOCK_INVALID"


class AuctionCapabilityState(str, Enum):
    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"
    INVALID = "INVALID"


class ControlRevision(str, Enum):
    LEGACY_B0 = "LEGACY_B0"
    B0_QUOTE_V2 = "B0_QUOTE_V2"


class AuctionMode(str, Enum):
    OBSERVE_ONLY = "OBSERVE_ONLY"


class EvidenceCaptureType(str, Enum):
    ACTION_INPUT = "ACTION_INPUT"
    ACTION_REJECT = "ACTION_REJECT"
    CHILD_RECEIPT = "CHILD_RECEIPT"
    MARKOUT_60S = "MARKOUT_60S"
    MARKOUT_300S = "MARKOUT_300S"
    MARKOUT_900S = "MARKOUT_900S"
    PROTECTION_BAND_TRIGGER = "PROTECTION_BAND_TRIGGER"
    CADENCE_AGGREGATE = "CADENCE_AGGREGATE"


class QuoteBatchAggregateState(str, Enum):
    OBSERVED = "OBSERVED"
    PARTIAL = "PARTIAL"
    INVALID = "INVALID"
    NO_ACTIVE_SYMBOLS = "NO_ACTIVE_SYMBOLS"


def ensure_utc(value: datetime, *, field_name: str) -> datetime:
    """Require an explicit timezone and normalize it to the one internal wall clock."""

    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise quote_contract_error(
            QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
            f"{field_name} must be an offset-aware datetime",
            context={"field": field_name},
        )
    return value.astimezone(UTC)


def exact_symbol(value: str) -> tuple[str, MarketCode]:
    """Validate the design's exact six-digit-plus-market identity contract."""

    if not isinstance(value, str):
        raise quote_contract_error(
            QuoteContractReasonCode.SYMBOL_INVALID,
            "symbol must be a string with an exact SH/SZ/BJ suffix",
            context={"value_type": type(value).__name__},
        )
    symbol = value.strip().upper()
    match = SYMBOL_PATTERN.fullmatch(symbol)
    if match is None:
        raise quote_contract_error(
            QuoteContractReasonCode.SYMBOL_INVALID,
            "symbol must be an exact six-digit SH/SZ/BJ identifier; fuzzy six-digit matching is forbidden",
            context={"symbol": value},
        )
    return symbol, MarketCode(match.group("market"))


def canonical_json_bytes(value: Any) -> bytes:
    """Canonical JSON used for content hashes; unsupported values fail loudly."""

    return json.dumps(
        _canonical_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def require_sha256(value: str, *, field_name: str) -> str:
    if not isinstance(value, str) or SHA256_PATTERN.fullmatch(value) is None:
        raise quote_contract_error(
            QuoteContractReasonCode.PAYLOAD_INVALID,
            f"{field_name} must be a lowercase SHA-256 digest",
            context={"field": field_name, "value": str(value)},
        )
    return value


def require_identity(value: str, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise quote_contract_error(
            QuoteContractReasonCode.PAYLOAD_INVALID,
            f"{field_name} is required",
            context={"field": field_name},
        )
    return value.strip()


def _non_negative_int_mapping(values: Mapping[str, int], *, field_name: str) -> dict[str, int]:
    normalized: dict[str, int] = {}
    for key, value in values.items():
        normalized_key = require_identity(str(key), field_name=f"{field_name}.key")
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                f"{field_name} values must be non-negative integers",
                context={"key": normalized_key, "value": value},
            )
        normalized[normalized_key] = value
    return normalized


def _canonical_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return ensure_utc(value, field_name="canonical_datetime").isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("non-finite Decimal is not canonical JSON")
        return format(value, "f")
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite float is not canonical JSON")
        return value
    if is_dataclass(value):
        if hasattr(value, "canonical_payload") and callable(value.canonical_payload):
            return _canonical_value(value.canonical_payload())
        return {name: _canonical_value(getattr(value, name)) for name in value.__dataclass_fields__}
    if isinstance(value, Mapping):
        return {str(key): _canonical_value(item) for key, item in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple, frozenset, set)):
        sequence = value if not isinstance(value, (frozenset, set)) else sorted(value, key=str)
        return [_canonical_value(item) for item in sequence]
    if value is None or isinstance(value, (str, bool, int)):
        return value
    raise TypeError(f"unsupported canonical hash value: {type(value).__name__}")


def _decimal(value: Decimal | int | str | float | None, *, field_name: str, allow_none: bool = True) -> Decimal | None:
    if value is None:
        if allow_none:
            return None
        raise quote_contract_error(
            QuoteContractReasonCode.PAYLOAD_INVALID,
            f"{field_name} is required",
            context={"field": field_name},
        )
    if isinstance(value, bool):
        raise quote_contract_error(
            QuoteContractReasonCode.PAYLOAD_INVALID,
            f"{field_name} must be numeric, not boolean",
            context={"field": field_name},
        )
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise quote_contract_error(
            QuoteContractReasonCode.PAYLOAD_INVALID,
            f"{field_name} must be a finite decimal",
            context={"field": field_name, "value": str(value)},
        ) from exc
    if not parsed.is_finite():
        raise quote_contract_error(
            QuoteContractReasonCode.PAYLOAD_INVALID,
            f"{field_name} must be finite",
            context={"field": field_name, "value": str(value)},
        )
    return parsed


def _enum_or_error(enum_type: type[Enum], value: Any, *, field_name: str) -> Any:
    try:
        return enum_type(value)
    except (TypeError, ValueError) as exc:
        raise quote_contract_error(
            QuoteContractReasonCode.PAYLOAD_INVALID,
            f"{field_name} is not a supported {enum_type.__name__}",
            context={"field": field_name, "value": str(value)},
        ) from exc


T = TypeVar("T")


def _tuple5(
    value: Sequence[T] | None,
    *,
    field_name: str,
) -> tuple[T, T, T, T, T] | None:
    if value is None:
        return None
    if isinstance(value, (str, bytes)) or len(value) != 5:
        raise quote_contract_error(
            QuoteContractReasonCode.DEPTH_SCHEMA_INVALID,
            f"{field_name} must contain exactly five levels",
            context={"field": field_name, "length": len(value) if hasattr(value, "__len__") else None},
        )
    return tuple(value)  # type: ignore[return-value]


@dataclass(frozen=True)
class SessionSegment:
    start_local: time
    end_local: time

    def __post_init__(self) -> None:
        if not isinstance(self.start_local, time) or not isinstance(self.end_local, time) or self.start_local >= self.end_local:
            raise quote_contract_error(
                QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                "calendar session segment must have an ordered local-time range",
            )

    def canonical_payload(self) -> dict[str, str]:
        return {"start_local": self.start_local.isoformat(), "end_local": self.end_local.isoformat()}


@dataclass(frozen=True)
class FiveLevelQuote:
    """Normalized five-level quote without a runtime, broker, or DB dependency.

    Missing arrays remain ``None``.  They are never fabricated as five zero
    levels, which keeps a source-capability failure distinguishable from an
    observed empty book.
    """

    schema_version: str
    normalizer_map_version: str
    timestamp_parser_version: str
    source: QuoteSource
    source_session_id: str
    ingress_generation: int
    ingress_sequence: int
    source_method: QuoteSourceMethod
    symbol: str
    market: MarketCode
    board: str
    source_exchange_time_utc: datetime | None
    source_trade_date: date | None
    clock_trade_date: date
    received_at_utc: datetime
    received_monotonic_ns: int
    clock_domain_id: str
    last_price: Decimal | None
    pre_close: Decimal | None
    total_volume: Decimal | None
    total_amount: Decimal | None
    security_status: str | None
    openint_status: str | None
    price_basis: PriceBasis
    depth_quantity_unit: DepthQuantityUnit
    unit_evidence_version: str
    bid_prices: tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None] | None
    bid_quantities: tuple[int, int, int, int, int] | None
    bid_quantities_raw: tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None] | None
    ask_prices: tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None] | None
    ask_quantities: tuple[int, int, int, int, int] | None
    ask_quantities_raw: tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None] | None
    quote_capabilities: frozenset[QuoteCapability] = field(default_factory=frozenset)
    validation_reasons: tuple[QuoteContractReasonCode, ...] = field(default_factory=tuple)
    normalization_notes: tuple[str, ...] = field(default_factory=tuple)
    source_payload_sha256: str = ""
    validation_state: QuoteValidationState = field(init=False)
    book_state: BookState = field(init=False)
    normalized_quote_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        symbol, market = exact_symbol(self.symbol)
        explicit_market = _enum_or_error(MarketCode, self.market, field_name="market")
        if explicit_market != market:
            raise quote_contract_error(
                QuoteContractReasonCode.SYMBOL_INVALID,
                "symbol market suffix conflicts with explicit market",
                context={"symbol": symbol, "market": self.market.value},
            )
        if self.schema_version != QUOTE_CONTRACT_SCHEMA_VERSION:
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "unsupported quote contract schema version",
                context={"schema_version": self.schema_version},
            )
        if not self.normalizer_map_version.strip() or not self.timestamp_parser_version.strip():
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "normalizer map and timestamp parser versions are required",
            )
        if not self.source_session_id.strip() or not self.clock_domain_id.strip() or not self.unit_evidence_version.strip():
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "source session, clock domain, and unit evidence version are required",
            )
        if self.ingress_generation < 0 or self.ingress_sequence < 0 or self.received_monotonic_ns < 0:
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "generation, sequence, and monotonic receive time must be non-negative",
            )
        require_sha256(self.source_payload_sha256, field_name="source_payload_sha256")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "market", explicit_market)
        object.__setattr__(self, "source", _enum_or_error(QuoteSource, self.source, field_name="source"))
        object.__setattr__(self, "source_method", _enum_or_error(QuoteSourceMethod, self.source_method, field_name="source_method"))
        object.__setattr__(self, "price_basis", _enum_or_error(PriceBasis, self.price_basis, field_name="price_basis"))
        object.__setattr__(self, "depth_quantity_unit", _enum_or_error(DepthQuantityUnit, self.depth_quantity_unit, field_name="depth_quantity_unit"))
        object.__setattr__(self, "received_at_utc", ensure_utc(self.received_at_utc, field_name="received_at_utc"))
        if self.source_exchange_time_utc is not None:
            object.__setattr__(self, "source_exchange_time_utc", ensure_utc(self.source_exchange_time_utc, field_name="source_exchange_time_utc"))
        for field_name in ("last_price", "pre_close", "total_volume", "total_amount"):
            object.__setattr__(self, field_name, _decimal(getattr(self, field_name), field_name=field_name))
        object.__setattr__(self, "bid_prices", _tuple5(self.bid_prices, field_name="bid_prices"))
        object.__setattr__(self, "ask_prices", _tuple5(self.ask_prices, field_name="ask_prices"))
        object.__setattr__(self, "bid_quantities", _tuple5(self.bid_quantities, field_name="bid_quantities"))
        object.__setattr__(self, "ask_quantities", _tuple5(self.ask_quantities, field_name="ask_quantities"))
        object.__setattr__(self, "bid_quantities_raw", _tuple5(self.bid_quantities_raw, field_name="bid_quantities_raw"))
        object.__setattr__(self, "ask_quantities_raw", _tuple5(self.ask_quantities_raw, field_name="ask_quantities_raw"))
        object.__setattr__(self, "quote_capabilities", frozenset(_enum_or_error(QuoteCapability, item, field_name="quote_capability") for item in self.quote_capabilities))
        object.__setattr__(
            self,
            "validation_reasons",
            tuple(_enum_or_error(QuoteContractReasonCode, item, field_name="validation_reason") for item in self.validation_reasons),
        )
        object.__setattr__(self, "normalization_notes", tuple(str(item) for item in self.normalization_notes))
        reasons, book_state = self._validation_outcome()
        object.__setattr__(self, "validation_reasons", tuple(dict.fromkeys(reasons)))
        object.__setattr__(self, "validation_state", self._validation_state(reasons))
        object.__setattr__(self, "book_state", book_state)
        object.__setattr__(self, "normalized_quote_sha256", canonical_sha256(self.canonical_payload()))

    def _validation_outcome(self) -> tuple[list[QuoteContractReasonCode], BookState]:
        reasons = list(self.validation_reasons)
        if self.price_basis != PriceBasis.RAW_CNY_PER_SHARE:
            reasons.append(QuoteContractReasonCode.UNIT_UNPROVEN)
        if self.depth_quantity_unit == DepthQuantityUnit.UNKNOWN:
            reasons.append(QuoteContractReasonCode.UNIT_UNPROVEN)
        if self.source_exchange_time_utc is None:
            reasons.append(QuoteContractReasonCode.TIMESTAMP_INVALID)
        elif self.source_trade_date is not None and self.source_trade_date != self.clock_trade_date:
            reasons.append(QuoteContractReasonCode.CLOCK_CALENDAR_INVALID)
        normalized_arrays = (self.bid_prices, self.bid_quantities, self.ask_prices, self.ask_quantities)
        raw_arrays = (self.bid_quantities_raw, self.ask_quantities_raw)
        if all(value is None for value in normalized_arrays):
            reasons.append(QuoteContractReasonCode.DEPTH_CAPABILITY_MISSING)
            return reasons, BookState.UNAVAILABLE
        if any(value is None for value in normalized_arrays) or any(value is None for value in raw_arrays):
            reasons.append(QuoteContractReasonCode.DEPTH_SCHEMA_INVALID)
            return reasons, BookState.INVALID
        assert self.bid_prices is not None and self.bid_quantities is not None and self.bid_quantities_raw is not None
        assert self.ask_prices is not None and self.ask_quantities is not None and self.ask_quantities_raw is not None
        try:
            _validate_side_levels(self.bid_prices, self.bid_quantities, side="bid")
            _validate_side_levels(self.ask_prices, self.ask_quantities, side="ask")
            _validate_raw_quantities(self.bid_quantities_raw, side="bid")
            _validate_raw_quantities(self.ask_quantities_raw, side="ask")
        except ValueError:
            reasons.append(QuoteContractReasonCode.DEPTH_SCHEMA_INVALID)
            return reasons, BookState.INVALID
        bid_one = self.bid_prices[0]
        ask_one = self.ask_prices[0]
        if bid_one is not None and ask_one is not None:
            if bid_one > ask_one:
                reasons.append(QuoteContractReasonCode.DEPTH_SCHEMA_INVALID)
                return reasons, BookState.INVALID
            if bid_one == ask_one:
                return reasons, BookState.LOCKED
        return reasons, BookState.NORMAL

    @staticmethod
    def _validation_state(reasons: Sequence[QuoteContractReasonCode]) -> QuoteValidationState:
        if not reasons:
            return QuoteValidationState.VALID
        capability_reasons = {
            QuoteContractReasonCode.DEPTH_CAPABILITY_MISSING,
            QuoteContractReasonCode.TIMESTAMP_INVALID,
            QuoteContractReasonCode.UNIT_UNPROVEN,
        }
        if all(reason in capability_reasons for reason in reasons):
            return QuoteValidationState.CAPABILITY_MISSING
        return QuoteValidationState.INVALID

    @property
    def has_five_level_depth(self) -> bool:
        return QuoteCapability.FIVE_LEVEL_DEPTH in self.quote_capabilities and self.validation_state == QuoteValidationState.VALID

    def canonical_payload(self) -> dict[str, Any]:
        """Hash payload excludes local receive wall time and ingress sequence."""

        return {
            "schema_version": self.schema_version,
            "normalizer_map_version": self.normalizer_map_version,
            "timestamp_parser_version": self.timestamp_parser_version,
            "source": self.source,
            "source_session_id": self.source_session_id,
            "ingress_generation": self.ingress_generation,
            "source_method": self.source_method,
            "symbol": self.symbol,
            "market": self.market,
            "board": self.board,
            "source_exchange_time_utc": self.source_exchange_time_utc,
            "source_trade_date": self.source_trade_date,
            "clock_trade_date": self.clock_trade_date,
            "last_price": self.last_price,
            "pre_close": self.pre_close,
            "total_volume": self.total_volume,
            "total_amount": self.total_amount,
            "security_status": self.security_status,
            "openint_status": self.openint_status,
            "price_basis": self.price_basis,
            "depth_quantity_unit": self.depth_quantity_unit,
            "unit_evidence_version": self.unit_evidence_version,
            "bid_prices": self.bid_prices,
            "bid_quantities": self.bid_quantities,
            "bid_quantities_raw": self.bid_quantities_raw,
            "ask_prices": self.ask_prices,
            "ask_quantities": self.ask_quantities,
            "ask_quantities_raw": self.ask_quantities_raw,
            "quote_capabilities": self.quote_capabilities,
            "validation_reasons": self.validation_reasons,
            "normalization_notes": self.normalization_notes,
            "source_payload_sha256": self.source_payload_sha256,
        }


def _validate_side_levels(
    prices: tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None],
    quantities: tuple[int, int, int, int, int],
    *,
    side: str,
) -> None:
    seen_empty = False
    previous_price: Decimal | None = None
    for price, quantity in zip(prices, quantities, strict=True):
        if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity < 0:
            raise ValueError("quantity must be a non-negative integer")
        if price is None:
            if quantity != 0:
                raise ValueError("empty depth level must have zero quantity")
            seen_empty = True
            continue
        if not price.is_finite() or price <= 0 or quantity <= 0 or seen_empty:
            raise ValueError("depth levels must be a positive contiguous prefix")
        if previous_price is not None:
            if side == "bid" and not previous_price > price:
                raise ValueError("bid prices must strictly decrease")
            if side == "ask" and not previous_price < price:
                raise ValueError("ask prices must strictly increase")
        previous_price = price


def _validate_raw_quantities(
    quantities: tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None],
    *,
    side: str,
) -> None:
    for quantity in quantities:
        if quantity is not None and (not quantity.is_finite() or quantity < 0):
            raise ValueError(f"{side} raw quantities must be non-negative finite decimals")


@dataclass(frozen=True)
class TradabilitySnapshot:
    schema_version: str
    tradability_id: str
    symbol: str
    market: MarketCode
    board: str
    trade_date: date
    price_basis: PriceBasis
    pre_close: Decimal | None
    limit_up: Decimal | None
    limit_down: Decimal | None
    price_tick: Decimal | None
    lot_size: int | None
    is_suspended: bool | None
    suspension_source: str | None
    security_status: str | None
    openint_status: str | None
    observed_at_utc: datetime
    source: str
    source_version: str
    state: TradabilityState
    validation_reasons: tuple[QuoteContractReasonCode, ...] = field(default_factory=tuple)
    evidence_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        symbol, market = exact_symbol(self.symbol)
        explicit_market = _enum_or_error(MarketCode, self.market, field_name="tradability.market")
        if explicit_market != market:
            raise quote_contract_error(QuoteContractReasonCode.SYMBOL_INVALID, "tradability market conflicts with symbol", context={"symbol": symbol})
        if not self.schema_version.strip() or not self.tradability_id.strip() or not self.source.strip() or not self.source_version.strip():
            raise quote_contract_error(QuoteContractReasonCode.TRADABILITY_DATA_INVALID, "tradability identity and source version are required")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "market", explicit_market)
        object.__setattr__(self, "price_basis", _enum_or_error(PriceBasis, self.price_basis, field_name="tradability.price_basis"))
        object.__setattr__(self, "state", _enum_or_error(TradabilityState, self.state, field_name="tradability.state"))
        object.__setattr__(self, "observed_at_utc", ensure_utc(self.observed_at_utc, field_name="tradability.observed_at_utc"))
        for field_name in ("pre_close", "limit_up", "limit_down", "price_tick"):
            object.__setattr__(self, field_name, _decimal(getattr(self, field_name), field_name=f"tradability.{field_name}"))
        reasons = [
            _enum_or_error(QuoteContractReasonCode, item, field_name="tradability.validation_reason")
            for item in self.validation_reasons
        ]
        required = (self.pre_close, self.limit_up, self.limit_down, self.price_tick, self.lot_size)
        if self.price_basis != PriceBasis.RAW_CNY_PER_SHARE or any(value is None for value in required):
            if self.state == TradabilityState.TRADABLE:
                reasons.append(QuoteContractReasonCode.TRADABILITY_DATA_INVALID)
                object.__setattr__(self, "state", TradabilityState.DATA_INVALID)
        if self.lot_size is not None and (isinstance(self.lot_size, bool) or self.lot_size <= 0):
            reasons.append(QuoteContractReasonCode.TRADABILITY_DATA_INVALID)
            object.__setattr__(self, "state", TradabilityState.DATA_INVALID)
        for field_name in ("pre_close", "limit_up", "limit_down", "price_tick"):
            value = getattr(self, field_name)
            if value is not None and value <= 0:
                reasons.append(QuoteContractReasonCode.TRADABILITY_DATA_INVALID)
                object.__setattr__(self, "state", TradabilityState.DATA_INVALID)
        object.__setattr__(self, "validation_reasons", tuple(dict.fromkeys(reasons)))
        object.__setattr__(self, "evidence_sha256", canonical_sha256(self.canonical_payload()))

    @property
    def supports_lot_conversion(self) -> bool:
        return self.state != TradabilityState.DATA_INVALID and self.price_basis == PriceBasis.RAW_CNY_PER_SHARE and bool(self.lot_size and self.lot_size > 0)

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "tradability_id": self.tradability_id,
            "symbol": self.symbol,
            "market": self.market,
            "board": self.board,
            "trade_date": self.trade_date,
            "price_basis": self.price_basis,
            "pre_close": self.pre_close,
            "limit_up": self.limit_up,
            "limit_down": self.limit_down,
            "price_tick": self.price_tick,
            "lot_size": self.lot_size,
            "is_suspended": self.is_suspended,
            "suspension_source": self.suspension_source,
            "security_status": self.security_status,
            "openint_status": self.openint_status,
            "observed_at_utc": self.observed_at_utc,
            "source": self.source,
            "source_version": self.source_version,
            "state": self.state,
            "validation_reasons": self.validation_reasons,
        }


@dataclass(frozen=True)
class CalendarSnapshot:
    calendar_id: str
    market: MarketCode
    trade_date: date
    timezone: str
    session_segments: tuple[SessionSegment, ...]
    effective_at_utc: datetime
    source_version: str
    calendar_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "market", _enum_or_error(MarketCode, self.market, field_name="calendar.market"))
        if self.timezone != "Asia/Shanghai" or not self.calendar_id.strip() or not self.source_version.strip():
            raise quote_contract_error(
                QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                "calendar requires Asia/Shanghai, an id, and a source version",
                stage=QuoteContractStage.CALENDAR,
            )
        if not self.session_segments:
            raise quote_contract_error(
                QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                "calendar requires at least one session segment",
                stage=QuoteContractStage.CALENDAR,
            )
        object.__setattr__(self, "session_segments", tuple(self.session_segments))
        object.__setattr__(self, "effective_at_utc", ensure_utc(self.effective_at_utc, field_name="calendar.effective_at_utc"))
        object.__setattr__(self, "calendar_sha256", canonical_sha256(self.canonical_payload()))

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "calendar_id": self.calendar_id,
            "market": self.market,
            "trade_date": self.trade_date,
            "timezone": self.timezone,
            "session_segments": self.session_segments,
            "effective_at_utc": self.effective_at_utc,
            "source_version": self.source_version,
        }


@dataclass(frozen=True)
class CalendarSnapshotSet:
    snapshot_set_id: str
    snapshot_by_market: Mapping[MarketCode, CalendarSnapshot]
    set_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        snapshots = {_enum_or_error(MarketCode, market, field_name="calendar.market"): snapshot for market, snapshot in self.snapshot_by_market.items()}
        if set(snapshots) != set(MarketCode):
            raise quote_contract_error(
                QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                "calendar snapshot set must contain SH, SZ, and BJ",
                stage=QuoteContractStage.CALENDAR,
            )
        if not self.snapshot_set_id.strip():
            raise quote_contract_error(
                QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                "calendar snapshot set id is required",
                stage=QuoteContractStage.CALENDAR,
            )
        if any(snapshot.market != market for market, snapshot in snapshots.items()):
            raise quote_contract_error(
                QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                "calendar snapshot key must match its market",
                stage=QuoteContractStage.CALENDAR,
            )
        if len({snapshot.trade_date for snapshot in snapshots.values()}) != 1:
            raise quote_contract_error(
                QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
                "calendar snapshot set must use one trade date",
                stage=QuoteContractStage.CALENDAR,
            )
        object.__setattr__(self, "snapshot_by_market", MappingProxyType(snapshots))
        object.__setattr__(self, "set_sha256", canonical_sha256(self.canonical_payload()))

    def canonical_payload(self) -> dict[str, Any]:
        return {"snapshot_set_id": self.snapshot_set_id, "snapshot_by_market": self.snapshot_by_market}


@dataclass(frozen=True)
class ExecutionClockEvent:
    clock_event_id: str
    clock_at_utc: datetime
    clock_monotonic_ns: int
    clock_domain_id: str
    clock_trade_date: date
    calendar_snapshot_set_id: str
    phase_by_market: Mapping[MarketCode, MarketPhase]
    source: str
    observed_at_utc: datetime

    def __post_init__(self) -> None:
        if self.clock_monotonic_ns < 0 or not self.clock_event_id.strip() or not self.clock_domain_id.strip() or not self.calendar_snapshot_set_id.strip() or not self.source.strip():
            raise quote_contract_error(QuoteContractReasonCode.CLOCK_CALENDAR_INVALID, "clock identity and monotonic time are required")
        phases = {
            _enum_or_error(MarketCode, market, field_name="clock.market"): _enum_or_error(MarketPhase, phase, field_name="clock.market_phase")
            for market, phase in self.phase_by_market.items()
        }
        if set(phases) != set(MarketCode):
            raise quote_contract_error(QuoteContractReasonCode.CLOCK_CALENDAR_INVALID, "clock phase mapping must contain SH, SZ, and BJ")
        object.__setattr__(self, "clock_at_utc", ensure_utc(self.clock_at_utc, field_name="clock_at_utc"))
        object.__setattr__(self, "observed_at_utc", ensure_utc(self.observed_at_utc, field_name="clock.observed_at_utc"))
        object.__setattr__(self, "phase_by_market", MappingProxyType(phases))


@dataclass(frozen=True)
class ActionQuoteEligibility:
    runtime_id: str
    parent_intent_id: str
    algo_instance_id: str
    symbol: str
    side: str
    market_data_id: str | None
    clock_event_id: str
    tradability_id: str | None
    control_revision: ControlRevision
    policy_sha256: str
    config_sha256: str
    adapter_sha256: str
    state: EligibilityState
    reason_code: QuoteContractReasonCode | None
    stage: str | None
    evaluated_at_utc: datetime

    def __post_init__(self) -> None:
        for field_name in ("runtime_id", "parent_intent_id", "algo_instance_id", "clock_event_id"):
            object.__setattr__(self, field_name, require_identity(getattr(self, field_name), field_name=f"eligibility.{field_name}"))
        object.__setattr__(self, "symbol", exact_symbol(self.symbol)[0])
        side = str(self.side).strip().upper()
        if side not in {"BUY", "SELL"}:
            raise quote_contract_error(QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE, "eligibility side must be BUY or SELL")
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "evaluated_at_utc", ensure_utc(self.evaluated_at_utc, field_name="eligibility.evaluated_at_utc"))
        object.__setattr__(self, "control_revision", _enum_or_error(ControlRevision, self.control_revision, field_name="eligibility.control_revision"))
        object.__setattr__(self, "state", _enum_or_error(EligibilityState, self.state, field_name="eligibility.state"))
        for field_name in ("policy_sha256", "config_sha256", "adapter_sha256"):
            object.__setattr__(self, field_name, require_sha256(getattr(self, field_name), field_name=f"eligibility.{field_name}"))
        if self.reason_code is not None:
            object.__setattr__(self, "reason_code", _enum_or_error(QuoteContractReasonCode, self.reason_code, field_name="eligibility.reason_code"))
        if self.state == EligibilityState.READY and self.reason_code is not None:
            raise quote_contract_error(QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE, "READY eligibility cannot carry a failure reason")
        if self.state != EligibilityState.READY and self.reason_code is None:
            raise quote_contract_error(QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE, "non-READY eligibility requires a registered reason")
        if self.reason_code is not None:
            expected_stage = failure_definition(self.reason_code).stage.value
            if self.stage != expected_stage:
                raise quote_contract_error(
                    QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE,
                    "eligibility reason must use the registered failure stage",
                    context={"reason_code": self.reason_code.value, "stage": self.stage, "expected_stage": expected_stage},
                )
        elif self.stage is not None:
            raise quote_contract_error(QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE, "eligibility stage requires a registered reason")
        if self.state == EligibilityState.READY and (not self.market_data_id or not self.tradability_id):
            raise quote_contract_error(
                QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE,
                "READY eligibility requires market_data_id and tradability_id",
            )


@dataclass(frozen=True)
class QuoteSnapshotBatch:
    batch_id: str
    runtime_id: str
    clock_event_id: str
    policy_sha256: str
    active_symbols: tuple[str, ...]
    dependency_groups: Mapping[str, tuple[str, ...]]
    eligibility_by_symbol: Mapping[str, ActionQuoteEligibility]
    quote_by_symbol: Mapping[str, FiveLevelQuote]
    group_watermark_ms: Mapping[str, int]
    group_max_skew_ms: Mapping[str, int]
    aggregate_state: QuoteBatchAggregateState

    def __post_init__(self) -> None:
        for field_name in ("batch_id", "runtime_id", "clock_event_id"):
            object.__setattr__(self, field_name, require_identity(getattr(self, field_name), field_name=f"batch.{field_name}"))
        object.__setattr__(self, "policy_sha256", require_sha256(self.policy_sha256, field_name="batch.policy_sha256"))
        exact = tuple(exact_symbol(symbol)[0] for symbol in self.active_symbols)
        if len(set(exact)) != len(exact):
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "active symbols must be unique")
        object.__setattr__(self, "active_symbols", exact)
        active = set(exact)
        aggregate_state = _enum_or_error(QuoteBatchAggregateState, self.aggregate_state, field_name="batch.aggregate_state")
        if not active and aggregate_state != QuoteBatchAggregateState.NO_ACTIVE_SYMBOLS:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "empty batch must use NO_ACTIVE_SYMBOLS")
        if active and aggregate_state == QuoteBatchAggregateState.NO_ACTIVE_SYMBOLS:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "non-empty batch cannot use NO_ACTIVE_SYMBOLS")

        eligibility: dict[str, ActionQuoteEligibility] = {}
        for raw_symbol, item in self.eligibility_by_symbol.items():
            symbol = exact_symbol(raw_symbol)[0]
            if symbol in eligibility:
                raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "eligibility symbol aliases collide after normalization")
            eligibility[symbol] = item
        quotes: dict[str, FiveLevelQuote] = {}
        for raw_symbol, item in self.quote_by_symbol.items():
            symbol = exact_symbol(raw_symbol)[0]
            if symbol in quotes:
                raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "quote symbol aliases collide after normalization")
            quotes[symbol] = item
        if set(eligibility) != active:
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "eligibility_by_symbol must contain every active symbol exactly once",
                context={"active_symbols": sorted(active), "eligibility_symbols": sorted(eligibility)},
            )
        if not set(quotes).issubset(active):
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "quote_by_symbol contains a non-active symbol")
        for symbol, item in eligibility.items():
            if item.symbol != symbol or item.runtime_id != self.runtime_id or item.clock_event_id != self.clock_event_id:
                raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "eligibility identity conflicts with its batch")
        for symbol, quote in quotes.items():
            if quote.symbol != symbol:
                raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "quote identity conflicts with its batch key")

        dependencies: dict[str, tuple[str, ...]] = {}
        assigned_symbols: set[str] = set()
        for group_id, symbols in self.dependency_groups.items():
            normalized_group_id = require_identity(str(group_id), field_name="batch.dependency_group_id")
            if normalized_group_id in dependencies:
                raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "dependency group ids collide after normalization")
            normalized_symbols = tuple(exact_symbol(symbol)[0] for symbol in symbols)
            if not normalized_symbols or len(set(normalized_symbols)) != len(normalized_symbols):
                raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "dependency group must contain unique symbols")
            if not set(normalized_symbols).issubset(active):
                raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "dependency group contains a non-active symbol")
            overlap = assigned_symbols.intersection(normalized_symbols)
            if overlap:
                raise quote_contract_error(
                    QuoteContractReasonCode.PAYLOAD_INVALID,
                    "an active symbol cannot belong to multiple dependency groups",
                    context={"symbols": sorted(overlap)},
                )
            assigned_symbols.update(normalized_symbols)
            dependencies[normalized_group_id] = normalized_symbols

        watermarks = _non_negative_int_mapping(self.group_watermark_ms, field_name="batch.group_watermark_ms")
        skews = _non_negative_int_mapping(self.group_max_skew_ms, field_name="batch.group_max_skew_ms")
        if set(watermarks) != set(dependencies) or set(skews) != set(dependencies):
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "group watermark and skew keys must match dependency groups",
            )
        if not active and any((dependencies, eligibility, quotes, watermarks, skews)):
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "NO_ACTIVE_SYMBOLS batch cannot carry symbol state")

        object.__setattr__(self, "dependency_groups", MappingProxyType(dependencies))
        object.__setattr__(self, "eligibility_by_symbol", MappingProxyType(eligibility))
        object.__setattr__(self, "quote_by_symbol", MappingProxyType(quotes))
        object.__setattr__(self, "group_watermark_ms", MappingProxyType(watermarks))
        object.__setattr__(self, "group_max_skew_ms", MappingProxyType(skews))
        object.__setattr__(self, "aggregate_state", aggregate_state)


@dataclass(frozen=True)
class ClosingAuctionSnapshot:
    schema_version: str
    symbol: str
    clock_event_id: str
    market_phase: MarketPhase
    capability_state: AuctionCapabilityState
    exchange_time_utc: datetime | None
    received_at_utc: datetime
    source: QuoteSource
    normalized_quote_sha256: str | None
    indicative_match_price: Decimal | None
    indicative_match_volume: Decimal | None
    unmatched_side: str | None
    unmatched_quantity: Decimal | None
    reasons: tuple[QuoteContractReasonCode, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.schema_version != CLOSING_AUCTION_SCHEMA_VERSION:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "unsupported closing-auction schema version")
        object.__setattr__(self, "clock_event_id", require_identity(self.clock_event_id, field_name="auction.clock_event_id"))
        object.__setattr__(self, "symbol", exact_symbol(self.symbol)[0])
        object.__setattr__(self, "market_phase", _enum_or_error(MarketPhase, self.market_phase, field_name="auction.market_phase"))
        if self.market_phase != MarketPhase.CLOSING_AUCTION:
            raise quote_contract_error(QuoteContractReasonCode.CLOSING_AUCTION_CAPABILITY_UNAVAILABLE, "auction snapshot requires CLOSING_AUCTION phase")
        object.__setattr__(self, "capability_state", _enum_or_error(AuctionCapabilityState, self.capability_state, field_name="auction.capability_state"))
        object.__setattr__(self, "source", _enum_or_error(QuoteSource, self.source, field_name="auction.source"))
        reasons = tuple(_enum_or_error(QuoteContractReasonCode, item, field_name="auction.reason") for item in self.reasons)
        object.__setattr__(self, "reasons", tuple(dict.fromkeys(reasons)))
        object.__setattr__(self, "received_at_utc", ensure_utc(self.received_at_utc, field_name="auction.received_at_utc"))
        if self.exchange_time_utc is not None:
            object.__setattr__(self, "exchange_time_utc", ensure_utc(self.exchange_time_utc, field_name="auction.exchange_time_utc"))
        for field_name in ("indicative_match_price", "indicative_match_volume", "unmatched_quantity"):
            object.__setattr__(self, field_name, _decimal(getattr(self, field_name), field_name=f"auction.{field_name}"))
        if self.normalized_quote_sha256 is not None:
            object.__setattr__(
                self,
                "normalized_quote_sha256",
                require_sha256(self.normalized_quote_sha256, field_name="auction.normalized_quote_sha256"),
            )
        if self.indicative_match_price is not None and self.indicative_match_price <= 0:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "auction indicative price must be positive")
        for field_name in ("indicative_match_volume", "unmatched_quantity"):
            value = getattr(self, field_name)
            if value is not None and value < 0:
                raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, f"auction {field_name} must be non-negative")
        if self.unmatched_side is not None:
            side = str(self.unmatched_side).strip().upper()
            if side not in {"BUY", "SELL", "NONE"}:
                raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "auction unmatched_side must be BUY, SELL, or NONE")
            object.__setattr__(self, "unmatched_side", side)
        auction_fields = (self.indicative_match_price, self.indicative_match_volume, self.unmatched_side, self.unmatched_quantity)
        if self.capability_state == AuctionCapabilityState.UNAVAILABLE:
            if any(value is not None for value in auction_fields):
                raise quote_contract_error(QuoteContractReasonCode.CLOSING_AUCTION_CAPABILITY_UNAVAILABLE, "unavailable auction capability must not contain synthesized fields")
            if QuoteContractReasonCode.CLOSING_AUCTION_CAPABILITY_UNAVAILABLE not in self.reasons:
                raise quote_contract_error(
                    QuoteContractReasonCode.CLOSING_AUCTION_CAPABILITY_UNAVAILABLE,
                    "unavailable auction capability requires the registered loud reason",
                )
        elif self.capability_state == AuctionCapabilityState.AVAILABLE:
            if self.exchange_time_utc is None or self.normalized_quote_sha256 is None or any(value is None for value in auction_fields):
                raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "available auction capability requires complete observed fields")
            if self.reasons:
                raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "available auction capability cannot carry failure reasons")
        elif not self.reasons:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "invalid auction capability requires a registered reason")


@dataclass(frozen=True)
class MarketDataEvidenceV1:
    """Complete algorithm-neutral evidence shape; P1-D owns persistence only."""

    market_data_id: str
    evidence_schema_version: str
    capture_type: EvidenceCaptureType
    runtime_id: str
    binding_id: str
    trade_date: date
    parent_intent_id: str | None
    child_order_id: str | None
    action_id: str | None
    quote: FiveLevelQuote | None
    tradability: TradabilitySnapshot | None
    clock_event_id: str | None
    quality_reason_code: QuoteContractReasonCode | None
    stage: str | None
    control_revision: ControlRevision
    policy_sha256: str
    config_sha256: str
    adapter_sha256: str
    code_sha256: str
    schema_sha256: str
    captured_at_utc: datetime
    persisted_at_utc: datetime | None
    quote_age_ms: int | None
    source_lag_ms: int | None
    transport_lag_ms: int | None
    benchmark_policy_version: str
    mark_policy_version: str
    source_input_sha256: str
    evidence_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        if self.evidence_schema_version != MARKET_DATA_EVIDENCE_SCHEMA_VERSION:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "unsupported market-data evidence schema version")
        for field_name in ("market_data_id", "runtime_id", "binding_id", "clock_event_id", "benchmark_policy_version", "mark_policy_version"):
            object.__setattr__(self, field_name, require_identity(getattr(self, field_name), field_name=f"evidence.{field_name}"))
        for field_name in ("parent_intent_id", "child_order_id", "action_id"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, require_identity(value, field_name=f"evidence.{field_name}"))
        object.__setattr__(self, "capture_type", _enum_or_error(EvidenceCaptureType, self.capture_type, field_name="evidence.capture_type"))
        object.__setattr__(self, "control_revision", _enum_or_error(ControlRevision, self.control_revision, field_name="evidence.control_revision"))
        if self.control_revision != ControlRevision.B0_QUOTE_V2:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "Phase 1 market-data evidence requires B0_QUOTE_V2")
        for field_name in (
            "policy_sha256",
            "config_sha256",
            "adapter_sha256",
            "code_sha256",
            "schema_sha256",
            "source_input_sha256",
        ):
            object.__setattr__(self, field_name, require_sha256(getattr(self, field_name), field_name=f"evidence.{field_name}"))
        object.__setattr__(self, "captured_at_utc", ensure_utc(self.captured_at_utc, field_name="evidence.captured_at_utc"))
        if self.persisted_at_utc is not None:
            persisted_at = ensure_utc(self.persisted_at_utc, field_name="evidence.persisted_at_utc")
            if persisted_at < self.captured_at_utc:
                raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "persisted_at cannot precede captured_at")
            object.__setattr__(self, "persisted_at_utc", persisted_at)
        for field_name in ("quote_age_ms", "source_lag_ms", "transport_lag_ms"):
            value = getattr(self, field_name)
            if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
                raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, f"evidence {field_name} must be an integer preserving its sign")
        if self.quality_reason_code is not None:
            reason = _enum_or_error(QuoteContractReasonCode, self.quality_reason_code, field_name="evidence.quality_reason_code")
            object.__setattr__(self, "quality_reason_code", reason)
            if self.stage != failure_definition(reason).stage.value and self.stage not in {
                stage.value for stage in failure_definition(reason).allowed_stages
            }:
                raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "evidence reason and stage must match the registry")
        elif self.stage is not None:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "evidence stage requires a registered reason")
        if self.quote is not None and not isinstance(self.quote, FiveLevelQuote):
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "evidence quote must be FiveLevelQuote")
        if self.tradability is not None and not isinstance(self.tradability, TradabilitySnapshot):
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "evidence tradability must be TradabilitySnapshot")
        if self.quote is not None and self.quote.clock_trade_date != self.trade_date:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "evidence quote trade date conflicts with evidence")
        if self.tradability is not None and self.tradability.trade_date != self.trade_date:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "evidence tradability trade date conflicts with evidence")
        if self.quote is not None and self.tradability is not None and self.quote.symbol != self.tradability.symbol:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "evidence quote and tradability symbols conflict")
        if self.capture_type == EvidenceCaptureType.ACTION_INPUT:
            if self.quote is None or self.tradability is None or not self.parent_intent_id or not self.action_id:
                raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "ACTION_INPUT evidence requires parent/action/quote/tradability")
            if self.quote.validation_state != QuoteValidationState.VALID or self.tradability.state != TradabilityState.TRADABLE:
                raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "ACTION_INPUT evidence requires valid quote and tradable state")
            if self.quality_reason_code is not None:
                raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "ACTION_INPUT evidence cannot carry a rejection reason")
        if self.capture_type == EvidenceCaptureType.ACTION_REJECT and (not self.parent_intent_id or self.quality_reason_code is None):
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "ACTION_REJECT evidence requires parent identity and loud reason")
        if self.capture_type == EvidenceCaptureType.CHILD_RECEIPT and not self.child_order_id:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "CHILD_RECEIPT evidence requires child_order_id")
        if self.capture_type in {
            EvidenceCaptureType.MARKOUT_60S,
            EvidenceCaptureType.MARKOUT_300S,
            EvidenceCaptureType.MARKOUT_900S,
        } and not self.child_order_id:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "markout evidence requires child_order_id")
        object.__setattr__(self, "evidence_sha256", canonical_sha256(self.canonical_payload()))

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "market_data_id": self.market_data_id,
            "evidence_schema_version": self.evidence_schema_version,
            "capture_type": self.capture_type,
            "runtime_id": self.runtime_id,
            "binding_id": self.binding_id,
            "trade_date": self.trade_date,
            "parent_intent_id": self.parent_intent_id,
            "child_order_id": self.child_order_id,
            "action_id": self.action_id,
            "quote": self.quote,
            "tradability": self.tradability,
            "clock_event_id": self.clock_event_id,
            "source": self.source,
            "source_exchange_time_utc": self.source_exchange_time_utc,
            "received_at_utc": self.received_at_utc,
            "persisted_at_utc": self.persisted_at_utc,
            "mid_price": self.mid_price,
            "bid_price_1": self.bid_price_1,
            "ask_price_1": self.ask_price_1,
            "last_price": self.last_price,
            "bid_prices": self.bid_prices,
            "bid_quantities": self.bid_quantities,
            "ask_prices": self.ask_prices,
            "ask_quantities": self.ask_quantities,
            "normalized_quote_sha256": self.normalized_quote_sha256,
            "tradability_evidence_sha256": self.tradability_evidence_sha256,
            "quote_age_ms": self.quote_age_ms,
            "source_lag_ms": self.source_lag_ms,
            "transport_lag_ms": self.transport_lag_ms,
            "quality_state": self.quote.validation_state if self.quote is not None else None,
            "quality_reason_code": self.quality_reason_code,
            "stage": self.stage,
            "benchmark_policy_version": self.benchmark_policy_version,
            "mark_policy_version": self.mark_policy_version,
            "control_revision": self.control_revision,
            "policy_sha256": self.policy_sha256,
            "config_sha256": self.config_sha256,
            "adapter_sha256": self.adapter_sha256,
            "code_sha256": self.code_sha256,
            "schema_sha256": self.schema_sha256,
            "source_input_sha256": self.source_input_sha256,
            "captured_at_utc": self.captured_at_utc,
        }

    @property
    def source(self) -> QuoteSource | None:
        return self.quote.source if self.quote is not None else None

    @property
    def source_exchange_time_utc(self) -> datetime | None:
        return self.quote.source_exchange_time_utc if self.quote is not None else None

    @property
    def received_at_utc(self) -> datetime | None:
        return self.quote.received_at_utc if self.quote is not None else None

    @property
    def mid_price(self) -> Decimal | None:
        if self.quote is None or self.quote.bid_prices is None or self.quote.ask_prices is None:
            return None
        bid_one = self.quote.bid_prices[0]
        ask_one = self.quote.ask_prices[0]
        if bid_one is None or ask_one is None:
            return None
        return (bid_one + ask_one) / Decimal(2)

    @property
    def bid_price_1(self) -> Decimal | None:
        return self.quote.bid_prices[0] if self.quote is not None and self.quote.bid_prices is not None else None

    @property
    def ask_price_1(self) -> Decimal | None:
        return self.quote.ask_prices[0] if self.quote is not None and self.quote.ask_prices is not None else None

    @property
    def last_price(self) -> Decimal | None:
        return self.quote.last_price if self.quote is not None else None

    @property
    def bid_prices(self) -> tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None] | None:
        return self.quote.bid_prices if self.quote is not None else None

    @property
    def bid_quantities(self) -> tuple[int, int, int, int, int] | None:
        return self.quote.bid_quantities if self.quote is not None else None

    @property
    def ask_prices(self) -> tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None] | None:
        return self.quote.ask_prices if self.quote is not None else None

    @property
    def ask_quantities(self) -> tuple[int, int, int, int, int] | None:
        return self.quote.ask_quantities if self.quote is not None else None

    @property
    def normalized_quote_sha256(self) -> str | None:
        return self.quote.normalized_quote_sha256 if self.quote is not None else None

    @property
    def tradability_evidence_sha256(self) -> str | None:
        return self.tradability.evidence_sha256 if self.tradability is not None else None


@runtime_checkable
class QuoteSnapshotProvider(Protocol):
    """Future core boundary; adapters must provide only validated snapshots."""

    def quote_snapshot(self, symbol: str, *, clock_event_id: str) -> FiveLevelQuote | None:
        ...

    def tradability_snapshot(self, symbol: str, *, clock_event_id: str) -> TradabilitySnapshot | None:
        ...


__all__ = [
    "ActionQuoteEligibility",
    "AuctionCapabilityState",
    "AuctionMode",
    "BookState",
    "CalendarSnapshot",
    "CalendarSnapshotSet",
    "CLOSING_AUCTION_SCHEMA_VERSION",
    "ClosingAuctionSnapshot",
    "ControlRevision",
    "DepthQuantityUnit",
    "EligibilityState",
    "EvidenceCaptureType",
    "ExecutionClockEvent",
    "FiveLevelQuote",
    "MarketCode",
    "MarketDataEvidenceV1",
    "MARKET_DATA_EVIDENCE_SCHEMA_VERSION",
    "MarketPhase",
    "PriceBasis",
    "QUOTE_CONTRACT_SCHEMA_VERSION",
    "QuoteCapability",
    "QuoteBatchAggregateState",
    "QuoteSnapshotBatch",
    "QuoteSnapshotProvider",
    "QuoteSource",
    "QuoteSourceMethod",
    "QuoteValidationState",
    "SessionSegment",
    "TradabilitySnapshot",
    "TradabilityState",
    "canonical_json_bytes",
    "canonical_sha256",
    "ensure_utc",
    "exact_symbol",
    "require_identity",
    "require_sha256",
]
