"""Pure MiniQMT payload adapter for the Phase 1 quote contract.

This module knows the documented MiniQMT field aliases, but it neither imports
xtquant nor invokes subscriptions, a broker, a database, or a scheduler.  A
future P1-B ingress owns callback delivery; this adapter only captures and
normalizes one already-received payload.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
import math
from types import MappingProxyType
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from backend.execution_algos.adaptive_is.contracts import (
    QUOTE_CONTRACT_SCHEMA_VERSION,
    DepthQuantityUnit,
    FiveLevelQuote,
    PriceBasis,
    QuoteCapability,
    QuoteSource,
    QuoteSourceMethod,
    TradabilitySnapshot,
    canonical_sha256,
    ensure_utc,
    exact_symbol,
)
from backend.execution_algos.adaptive_is.reasons import QuoteContractError, QuoteContractReasonCode, quote_contract_error


RAW_QUOTE_FRAME_SCHEMA_VERSION = "miniqmt_raw_quote_frame_v1"
MINIQMT_NORMALIZER_MAP_VERSION = "miniqmt_quote_normalizer_map_v2"
MINIQMT_TIMESTAMP_PARSER_VERSION = "miniqmt_quote_timestamp_parser_v2"
CHINA_TZ = ZoneInfo("Asia/Shanghai")

SYMBOL_ALIASES = ("stock_code", "symbol")
TIMESTAMP_ALIASES = ("time", "timetag", "datetime", "quote_time", "quoteTime", "timestamp", "ServerTime")
LAST_PRICE_ALIASES = ("lastPrice", "last_price", "price")
PRE_CLOSE_ALIASES = ("lastClose", "preClose", "pre_close")
BID_PRICE_ARRAY_ALIASES = ("bidPrice",)
BID_VOLUME_ARRAY_ALIASES = ("bidVol",)
ASK_PRICE_ARRAY_ALIASES = ("askPrice",)
ASK_VOLUME_ARRAY_ALIASES = ("askVol",)
L1_DEPTH_ALIASES = (
    "bid_price_1",
    "bidPrice1",
    "bid_volume_1",
    "bidVolume1",
    "bidVol1",
    "ask_price_1",
    "askPrice1",
    "ask_volume_1",
    "askVolume1",
    "askVol1",
)
STATUS_ALIASES = ("stockStatus",)
OPENINT_ALIASES = ("openint",)
WHITELISTED_RAW_KEYS = frozenset(
    {
        "stock_code",
        "symbol",
        *TIMESTAMP_ALIASES,
        *LAST_PRICE_ALIASES,
        *PRE_CLOSE_ALIASES,
        *BID_PRICE_ARRAY_ALIASES,
        *BID_VOLUME_ARRAY_ALIASES,
        *ASK_PRICE_ARRAY_ALIASES,
        *ASK_VOLUME_ARRAY_ALIASES,
        *L1_DEPTH_ALIASES,
        "volume",
        "amount",
        *STATUS_ALIASES,
        *OPENINT_ALIASES,
        "auctionPrice",
        "auctionVolume",
        "unmatchedSide",
        "unmatchedVolume",
    }
)


@dataclass(frozen=True)
class RawQuoteFrame:
    """Immutable whitelist-only capture at the callback boundary."""

    schema_version: str
    normalizer_map_version: str
    timestamp_parser_version: str
    source: QuoteSource
    source_method: QuoteSourceMethod
    source_session_id: str
    ingress_generation: int
    ingress_sequence: int
    symbol_raw: str
    symbol: str
    received_at_utc: datetime
    received_monotonic_ns: int
    clock_domain_id: str
    source_timestamp_raw: Any | None
    whitelisted_raw_fields: Mapping[str, Any]
    source_payload_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        symbol, _ = exact_symbol(self.symbol)
        if self.schema_version != RAW_QUOTE_FRAME_SCHEMA_VERSION:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "unsupported RawQuoteFrame schema", context={"schema_version": self.schema_version})
        if self.normalizer_map_version != MINIQMT_NORMALIZER_MAP_VERSION or self.timestamp_parser_version != MINIQMT_TIMESTAMP_PARSER_VERSION:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "RawQuoteFrame must declare the current normalizer and timestamp parser versions")
        if not self.source_session_id.strip() or not self.clock_domain_id.strip() or self.ingress_generation < 0 or self.ingress_sequence < 0 or self.received_monotonic_ns < 0:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "RawQuoteFrame identity and monotonic receive time are required")
        frozen_fields = _freeze_mapping(self.whitelisted_raw_fields)
        try:
            source = QuoteSource(self.source)
            source_method = QuoteSourceMethod(self.source_method)
        except (TypeError, ValueError) as exc:
            raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "RawQuoteFrame source and source_method must be registered") from exc
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "source_method", source_method)
        object.__setattr__(self, "received_at_utc", ensure_utc(self.received_at_utc, field_name="raw_quote.received_at_utc"))
        object.__setattr__(self, "source_timestamp_raw", _freeze_value(self.source_timestamp_raw))
        object.__setattr__(self, "whitelisted_raw_fields", frozen_fields)
        object.__setattr__(self, "source_payload_sha256", canonical_sha256(frozen_fields))


def capture_raw_quote_frame(
    payload: Mapping[str, Any],
    *,
    callback_symbol: str,
    source_session_id: str,
    ingress_generation: int,
    ingress_sequence: int,
    received_at_utc: datetime,
    received_monotonic_ns: int,
    clock_domain_id: str,
    source_method: QuoteSourceMethod,
) -> RawQuoteFrame:
    """Copy one callback payload into an immutable, whitelisted envelope.

    Every time source is injected by the caller.  In particular, this function
    never uses ``datetime.now`` as a fallback for a missing exchange timestamp.
    """

    if not isinstance(payload, Mapping):
        raise quote_contract_error(
            QuoteContractReasonCode.PAYLOAD_INVALID,
            "quote payload must be a mapping",
            context={"payload_type": type(payload).__name__},
        )
    symbol = _resolve_symbol(payload, callback_symbol=callback_symbol)
    fields: dict[str, Any] = {"callback_symbol": callback_symbol}
    for key in WHITELISTED_RAW_KEYS:
        if key in payload:
            fields[key] = _freeze_value(payload[key])
    timestamp_raw = _raw_timestamp_alias(payload)
    return RawQuoteFrame(
        schema_version=RAW_QUOTE_FRAME_SCHEMA_VERSION,
        normalizer_map_version=MINIQMT_NORMALIZER_MAP_VERSION,
        timestamp_parser_version=MINIQMT_TIMESTAMP_PARSER_VERSION,
        source=QuoteSource.MINIQMT_REALTIME_BROKER_QUOTE,
        source_method=source_method,
        source_session_id=source_session_id,
        ingress_generation=ingress_generation,
        ingress_sequence=ingress_sequence,
        symbol_raw=callback_symbol,
        symbol=symbol,
        received_at_utc=received_at_utc,
        received_monotonic_ns=received_monotonic_ns,
        clock_domain_id=clock_domain_id,
        source_timestamp_raw=timestamp_raw,
        whitelisted_raw_fields=fields,
    )


def normalize_raw_quote_frame(
    frame: RawQuoteFrame,
    *,
    clock_trade_date: date,
    board: str,
    depth_quantity_unit: DepthQuantityUnit,
    unit_evidence_version: str,
    tradability: TradabilitySnapshot | None = None,
) -> FiveLevelQuote:
    """Normalize a captured frame without making any runtime side effect."""

    if not isinstance(frame, RawQuoteFrame):
        raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "normalize requires RawQuoteFrame")
    if not board.strip() or not unit_evidence_version.strip():
        raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "board and unit evidence version are required")
    payload = frame.whitelisted_raw_fields
    timestamp_raw, source_exchange_time_utc = _resolve_timestamp(payload, trade_date=clock_trade_date, allow_missing=True)
    last_price = _resolve_decimal_alias(payload, LAST_PRICE_ALIASES, semantic="last_price")
    pre_close = _resolve_decimal_alias(payload, PRE_CLOSE_ALIASES, semantic="pre_close")
    total_volume = _resolve_decimal_alias(payload, ("volume",), semantic="volume")
    total_amount = _resolve_decimal_alias(payload, ("amount",), semantic="amount")
    security_status = _resolve_text_alias(payload, STATUS_ALIASES, semantic="stock_status")
    openint_status = _resolve_text_alias(payload, OPENINT_ALIASES, semantic="openint")
    arrays = _resolve_depth_arrays(payload)
    reasons: list[QuoteContractReasonCode] = []
    notes: list[str] = []
    capabilities: set[QuoteCapability] = {QuoteCapability.RAW_PRICE_BASIS}
    if source_exchange_time_utc is None:
        reasons.append(QuoteContractReasonCode.TIMESTAMP_INVALID)
        notes.append("exchange_timestamp_unavailable")
    else:
        capabilities.add(QuoteCapability.EXCHANGE_TIMESTAMP)
    bid_prices: tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None] | None = None
    ask_prices: tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None] | None = None
    bid_raw: tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None] | None = None
    ask_raw: tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None] | None = None
    bid_quantities: tuple[int, int, int, int, int] | None = None
    ask_quantities: tuple[int, int, int, int, int] | None = None
    if arrays is None:
        reasons.append(QuoteContractReasonCode.DEPTH_CAPABILITY_MISSING)
        notes.append("five_level_depth_unavailable")
    else:
        bid_prices, bid_raw, ask_prices, ask_raw = arrays
        capabilities.add(QuoteCapability.FIVE_LEVEL_DEPTH)
        bid_quantities, ask_quantities, unit_reason = _convert_depth_quantities(
            bid_raw,
            ask_raw,
            depth_quantity_unit=depth_quantity_unit,
            tradability=tradability,
        )
        if unit_reason is not None:
            reasons.append(unit_reason)
            notes.append("depth_unit_unproven")
        else:
            capabilities.add(QuoteCapability.DEPTH_UNIT_SHARES)
    if tradability is not None:
        if tradability.symbol != frame.symbol:
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "tradability snapshot symbol conflicts with quote frame",
                context={"quote_symbol": frame.symbol, "tradability_symbol": tradability.symbol},
            )
        capabilities.add(QuoteCapability.TRADABILITY)
    symbol, market = exact_symbol(frame.symbol)
    source_trade_date = source_exchange_time_utc.astimezone(CHINA_TZ).date() if source_exchange_time_utc is not None else None
    return FiveLevelQuote(
        schema_version=QUOTE_CONTRACT_SCHEMA_VERSION,
        normalizer_map_version=MINIQMT_NORMALIZER_MAP_VERSION,
        timestamp_parser_version=MINIQMT_TIMESTAMP_PARSER_VERSION,
        source=frame.source,
        source_session_id=frame.source_session_id,
        ingress_generation=frame.ingress_generation,
        ingress_sequence=frame.ingress_sequence,
        source_method=frame.source_method,
        symbol=symbol,
        market=market,
        board=board.strip(),
        source_exchange_time_utc=source_exchange_time_utc,
        source_trade_date=source_trade_date,
        clock_trade_date=clock_trade_date,
        received_at_utc=frame.received_at_utc,
        received_monotonic_ns=frame.received_monotonic_ns,
        clock_domain_id=frame.clock_domain_id,
        last_price=last_price,
        pre_close=pre_close,
        total_volume=total_volume,
        total_amount=total_amount,
        security_status=security_status,
        openint_status=openint_status,
        price_basis=PriceBasis.RAW_CNY_PER_SHARE,
        depth_quantity_unit=depth_quantity_unit,
        unit_evidence_version=unit_evidence_version,
        bid_prices=bid_prices,
        bid_quantities=bid_quantities,
        bid_quantities_raw=bid_raw,
        ask_prices=ask_prices,
        ask_quantities=ask_quantities,
        ask_quantities_raw=ask_raw,
        quote_capabilities=frozenset(capabilities),
        validation_reasons=tuple(reasons),
        normalization_notes=tuple(notes),
        source_payload_sha256=frame.source_payload_sha256,
    )


def parse_miniqmt_quote_timestamp_v2(value: Any, *, trade_date: date | None) -> datetime:
    """Parse registered MiniQMT timestamp forms without normalizing bad clocks."""

    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=CHINA_TZ).astimezone(UTC)
    if value is None or (isinstance(value, str) and not value.strip()):
        raise quote_contract_error(QuoteContractReasonCode.TIMESTAMP_INVALID, "quote timestamp is missing")
    text = str(value).strip()
    try:
        if text.isdigit():
            if len(text) == 14:
                return datetime.strptime(text, "%Y%m%d%H%M%S").replace(tzinfo=CHINA_TZ).astimezone(UTC)
            if trade_date is not None and len(text) in (7, 8):
                compact = text.zfill(8)
                hour, minute, second, centisecond = int(compact[:2]), int(compact[2:4]), int(compact[4:6]), int(compact[6:8])
                if hour > 23 or minute > 59 or second > 59 or centisecond > 99:
                    raise ValueError("invalid compact HHMMSScc timestamp")
                return datetime(trade_date.year, trade_date.month, trade_date.day, hour, minute, second, centisecond * 10_000, tzinfo=CHINA_TZ).astimezone(UTC)
            if trade_date is not None and len(text) <= 6:
                compact = text.zfill(6)
                hour, minute, second = int(compact[:2]), int(compact[2:4]), int(compact[4:6])
                if hour > 23 or minute > 59 or second > 59:
                    raise ValueError("invalid compact HHMMSS timestamp")
                return datetime(trade_date.year, trade_date.month, trade_date.day, hour, minute, second, tzinfo=CHINA_TZ).astimezone(UTC)
            numeric = int(text)
            if numeric >= 10**12:
                return datetime.fromtimestamp(numeric / 1000, tz=UTC)
            if numeric >= 10**9:
                return datetime.fromtimestamp(numeric, tz=UTC)
        normalized = text.replace("/", "-")
        if normalized.endswith("Z"):
            normalized = f"{normalized[:-1]}+00:00"
        if "T" not in normalized and " " not in normalized:
            raise ValueError("timestamp must include a time-of-day component")
        parsed = datetime.fromisoformat(normalized)
        return parsed.astimezone(UTC) if parsed.tzinfo is not None else parsed.replace(tzinfo=CHINA_TZ).astimezone(UTC)
    except (OverflowError, OSError, ValueError) as exc:
        raise quote_contract_error(
            QuoteContractReasonCode.TIMESTAMP_INVALID,
            "quote timestamp does not match parser v2",
            context={"value": text, "parser_version": MINIQMT_TIMESTAMP_PARSER_VERSION},
        ) from exc


def _resolve_symbol(payload: Mapping[str, Any], *, callback_symbol: str) -> str:
    candidates: list[tuple[str, str]] = [("callback_symbol", callback_symbol)]
    for key in SYMBOL_ALIASES:
        value = payload.get(key)
        if value not in (None, ""):
            candidates.append((key, str(value)))
    normalized = [(key, exact_symbol(value)[0]) for key, value in candidates]
    unique = {value for _, value in normalized}
    if len(unique) != 1:
        raise quote_contract_error(
            QuoteContractReasonCode.ALIAS_CONFLICT,
            "symbol aliases conflict; fuzzy symbol reconciliation is forbidden",
            context={"aliases": normalized},
        )
    return normalized[0][1]


def _resolve_timestamp(
    payload: Mapping[str, Any],
    *,
    trade_date: date | None,
    allow_missing: bool,
) -> tuple[Any | None, datetime | None]:
    values = [(key, payload[key]) for key in TIMESTAMP_ALIASES if key in payload and payload[key] not in (None, "")]
    if not values:
        if allow_missing:
            return None, None
        raise quote_contract_error(QuoteContractReasonCode.TIMESTAMP_INVALID, "quote timestamp is required")
    parsed = [(key, raw, parse_miniqmt_quote_timestamp_v2(raw, trade_date=trade_date)) for key, raw in values]
    if len({value for _, _, value in parsed}) != 1:
        raise quote_contract_error(
            QuoteContractReasonCode.ALIAS_CONFLICT,
            "timestamp aliases resolve to different exchange times",
            context={"aliases": [(key, str(value)) for key, _, value in parsed]},
        )
    return parsed[0][1], parsed[0][2]


def _raw_timestamp_alias(payload: Mapping[str, Any]) -> Any | None:
    """Capture raw timestamp data before a clock trade date is available.

    Compact MiniQMT timestamps cannot be parsed without the authoritative trade
    date.  Their semantic comparison therefore happens in normalization, not
    at the callback capture boundary.
    """

    values = [payload[key] for key in TIMESTAMP_ALIASES if key in payload and payload[key] not in (None, "")]
    return values[0] if values else None


def _resolve_decimal_alias(payload: Mapping[str, Any], aliases: Sequence[str], *, semantic: str) -> Decimal | None:
    values = [(key, payload[key]) for key in aliases if key in payload and payload[key] not in (None, "")]
    if not values:
        return None
    parsed = [(key, _finite_decimal(value, field_name=semantic)) for key, value in values]
    if len({value for _, value in parsed}) != 1:
        raise quote_contract_error(QuoteContractReasonCode.ALIAS_CONFLICT, f"{semantic} aliases conflict", context={"aliases": [(key, str(value)) for key, value in parsed]})
    return parsed[0][1]


def _resolve_text_alias(payload: Mapping[str, Any], aliases: Sequence[str], *, semantic: str) -> str | None:
    values = [(key, str(payload[key]).strip()) for key in aliases if key in payload and payload[key] not in (None, "")]
    if not values:
        return None
    if len({value for _, value in values}) != 1:
        raise quote_contract_error(QuoteContractReasonCode.ALIAS_CONFLICT, f"{semantic} aliases conflict", context={"aliases": values})
    return values[0][1]


def _resolve_depth_arrays(
    payload: Mapping[str, Any],
) -> tuple[
    tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None],
    tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None],
    tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None],
    tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None],
] | None:
    definitions = (
        ("bid_prices", BID_PRICE_ARRAY_ALIASES),
        ("bid_volumes", BID_VOLUME_ARRAY_ALIASES),
        ("ask_prices", ASK_PRICE_ARRAY_ALIASES),
        ("ask_volumes", ASK_VOLUME_ARRAY_ALIASES),
    )
    resolved: list[tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None] | None] = []
    for semantic, aliases in definitions:
        values = [(key, payload[key]) for key in aliases if key in payload and payload[key] is not None]
        if not values:
            resolved.append(None)
            continue
        parsed = [(key, _five_decimal_array(value, field_name=semantic)) for key, value in values]
        if len({value for _, value in parsed}) != 1:
            raise quote_contract_error(QuoteContractReasonCode.ALIAS_CONFLICT, f"{semantic} aliases conflict")
        resolved.append(parsed[0][1])
    if all(value is None for value in resolved):
        return None
    if any(value is None for value in resolved):
        raise quote_contract_error(QuoteContractReasonCode.DEPTH_SCHEMA_INVALID, "all four five-level arrays are required together")
    bid_prices, bid_volumes, ask_prices, ask_volumes = resolved
    assert bid_prices is not None and bid_volumes is not None and ask_prices is not None and ask_volumes is not None
    return bid_prices, bid_volumes, ask_prices, ask_volumes


def _convert_depth_quantities(
    bid_raw: tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None],
    ask_raw: tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None],
    *,
    depth_quantity_unit: DepthQuantityUnit,
    tradability: TradabilitySnapshot | None,
) -> tuple[tuple[int, int, int, int, int] | None, tuple[int, int, int, int, int] | None, QuoteContractReasonCode | None]:
    if depth_quantity_unit == DepthQuantityUnit.UNKNOWN:
        return None, None, QuoteContractReasonCode.UNIT_UNPROVEN
    multiplier = Decimal(1)
    if depth_quantity_unit == DepthQuantityUnit.LOTS:
        if tradability is None or not tradability.supports_lot_conversion:
            return None, None, QuoteContractReasonCode.UNIT_UNPROVEN
        assert tradability.lot_size is not None
        multiplier = Decimal(tradability.lot_size)
    try:
        return _as_share_quantities(bid_raw, multiplier=multiplier), _as_share_quantities(ask_raw, multiplier=multiplier), None
    except QuoteContractError as exc:
        if exc.reason_code != QuoteContractReasonCode.UNIT_UNPROVEN:
            raise
        return None, None, QuoteContractReasonCode.UNIT_UNPROVEN


def _as_share_quantities(
    values: tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None],
    *,
    multiplier: Decimal,
) -> tuple[int, int, int, int, int]:
    converted: list[int] = []
    for value in values:
        if value is None:
            converted.append(0)
            continue
        shares = value * multiplier
        if shares != shares.to_integral_value() or shares < 0:
            raise quote_contract_error(QuoteContractReasonCode.UNIT_UNPROVEN, "depth quantity cannot be proven as an integral share count", context={"value": str(value), "multiplier": str(multiplier)})
        converted.append(int(shares))
    return tuple(converted)  # type: ignore[return-value]


def _five_decimal_array(value: Any, *, field_name: str) -> tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence) or len(value) != 5:
        raise quote_contract_error(QuoteContractReasonCode.DEPTH_SCHEMA_INVALID, f"{field_name} must be an array of exactly five values")
    return tuple(_finite_decimal(item, field_name=field_name, allow_none=True) for item in value)  # type: ignore[return-value]


def _finite_decimal(value: Any, *, field_name: str, allow_none: bool = False) -> Decimal | None:
    if value is None:
        if allow_none:
            return None
        raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, f"{field_name} is required")
    if isinstance(value, bool):
        raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, f"{field_name} must be numeric")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, f"{field_name} must be a finite decimal", context={"value": str(value)}) from exc
    if not parsed.is_finite():
        raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, f"{field_name} must be finite", context={"value": str(value)})
    return parsed


def _freeze_mapping(value: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType({str(key): _freeze_value(item) for key, item in value.items()})


def _freeze_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _freeze_mapping(value)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_value(item) for item in value)
    if isinstance(value, float) and not math.isfinite(value):
        raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "raw quote field cannot be a non-finite float")
    if isinstance(value, Decimal) and not value.is_finite():
        raise quote_contract_error(QuoteContractReasonCode.PAYLOAD_INVALID, "raw quote field cannot be a non-finite decimal")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool, Decimal)) or value is None:
        return value
    raise quote_contract_error(
        QuoteContractReasonCode.PAYLOAD_INVALID,
        "whitelisted raw quote field has an unsupported type",
        context={"value_type": type(value).__name__},
    )


__all__ = [
    "CHINA_TZ",
    "MINIQMT_NORMALIZER_MAP_VERSION",
    "MINIQMT_TIMESTAMP_PARSER_VERSION",
    "RAW_QUOTE_FRAME_SCHEMA_VERSION",
    "RawQuoteFrame",
    "capture_raw_quote_frame",
    "normalize_raw_quote_frame",
    "parse_miniqmt_quote_timestamp_v2",
]
