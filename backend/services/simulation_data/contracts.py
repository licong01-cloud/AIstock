"""Immutable data-source contracts shared by LocalSIM and MiniQMT."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
import hashlib
import json
import math
from types import MappingProxyType
from typing import Any, Callable, Iterator, Mapping, Protocol
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from backend.services.trading_core.errors import (
    BrokerMarketSourceMismatchError,
    DataUnavailableError,
)
from backend.services.trading_core.limit_price_provider import DailyLimitPrice
from backend.services.trading_core.models import MinuteBar


_ASIA_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _shanghai_naive(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value
    return value.astimezone(_ASIA_SHANGHAI).replace(tzinfo=None)


def _canonical_json_sha256(payload: dict[str, Any] | list[Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


PRICE_UNIT_DIVISOR = 1000.0
MINUTE_VOLUME_HAND_SIZE = 100
PRICE_TICK = Decimal("0.01")
TDX_REALTIME_QUOTE_MAX_AGE = timedelta(minutes=5)
TDX_REALTIME_QUOTE_MAX_FUTURE_SKEW = timedelta(seconds=30)
TDX_REALTIME_BATCH_QUOTE_LIMIT = 50
PRICE_COMPARE_EPSILON = 1e-6
DAILY_PRE_CLOSE_QUOTE_SOURCES = frozenset(
    {
        "TDX_REALTIME.batch_quote.pre_close",
        "MINIQMT_REALTIME.broker_quote.pre_close",
    }
)

TdxMinuteFetcher = Callable[[str, date], list[dict[str, Any]]]
RealtimeQuoteFetcher = Callable[[list[str]], dict[str, dict[str, Any]]]
ConnFactory = Callable[[], Iterator[Any]]


class MinuteDataSource(str, Enum):
    """Supported authoritative minute data sources.

    MINIQMT_REALTIME is the channel emitted by the miniQMT-bound BrokerBackend
    (Strategy Engine design 2026-05-08 鎼?.6.4, R-Q9 D3). The xtdata fetch
    function for it is intentionally NOT wired here yet; this enum value plus
    ``ALLOWED_MARKET_SOURCES`` / ``assert_broker_market_source_match`` only
    establish the strong-binding invariant. Concrete miniQMT minute fetch will
    be added when MiniQMTSim BrokerBackend lands (Task #20 follow-up after
    PoC re-test, task #10).
    """

    TDX_REALTIME = "TDX_REALTIME"
    DB_HISTORICAL = "DB_HISTORICAL"
    MINIQMT_REALTIME = "MINIQMT_REALTIME"


# Strong binding between BrokerBackend and minute data channel
# (Strategy Engine design 2026-05-08 鎼?.6.4, R-Q9 D3). Cross-pairing is a
# fail-fast invariant; do NOT add a fallback path.
ALLOWED_MARKET_SOURCES: dict[str, set[MinuteDataSource]] = {
    "local_sim": {MinuteDataSource.TDX_REALTIME, MinuteDataSource.DB_HISTORICAL},
    "minqmt_sim": {MinuteDataSource.MINIQMT_REALTIME},
    "minqmt_live": {MinuteDataSource.MINIQMT_REALTIME},
}


def assert_broker_market_source_match(
    broker_id: str,
    source: MinuteDataSource,
) -> None:
    """Validate broker_id <-> MinuteDataSource binding (R-Q9 D3 fail-fast).

    Called at portfolio bootstrap, live_session bootstrap, and Engine.init().
    Raises BrokerMarketSourceMismatchError on any mismatch. Never silently
    falls back (feedback_no_silent_errors).
    """

    if not isinstance(source, MinuteDataSource):
        raise BrokerMarketSourceMismatchError(
            "minute data source must be a MinuteDataSource enum value",
            context={"broker_id": broker_id, "given_source": repr(source)},
        )
    allowed = ALLOWED_MARKET_SOURCES.get(broker_id)
    if allowed is None:
        raise BrokerMarketSourceMismatchError(
            f"unknown broker_id {broker_id!r}",
            context={
                "broker_id": broker_id,
                "given_source": source.value,
                "known_broker_ids": sorted(ALLOWED_MARKET_SOURCES.keys()),
            },
        )
    if source not in allowed:
        raise BrokerMarketSourceMismatchError(
            f"broker_id {broker_id!r} requires market source in {sorted(s.value for s in allowed)}; got {source.value}",
            context={
                "broker_id": broker_id,
                "given_source": source.value,
                "allowed": sorted(s.value for s in allowed),
            },
        )


@dataclass(frozen=True)
class MinuteExecutionMarketInput:
    """Minute bars and execution context for one symbol/trade date."""

    symbol: str
    trade_date: date
    source: MinuteDataSource
    minute_bars: list[MinuteBar]
    market_context: dict[str, Any]


@dataclass(frozen=True)
class LocalSimMarketSnapshotV1:
    """One immutable LocalSIM market-data view for a scheduler tick.

    Each unique symbol is loaded at most once for the snapshot.  Successful
    inputs and explicit typed failures are carried together so a broker can
    isolate one symbol without refetching or silently replacing its data.
    """

    trade_date: date
    as_of_time: datetime
    source: MinuteDataSource
    market_inputs: Mapping[str, MinuteExecutionMarketInput]
    errors: Mapping[str, Mapping[str, Any]]
    schema_version: str = "local_sim_market_snapshot_v1"
    snapshot_id: str = ""
    snapshot_hash: str = ""

    def __post_init__(self) -> None:
        if self.source != MinuteDataSource.TDX_REALTIME:
            raise ValueError("LocalSimMarketSnapshotV1 requires TDX_REALTIME")
        normalized_inputs = {
            str(symbol): MinuteExecutionMarketInput(
                symbol=item.symbol,
                trade_date=item.trade_date,
                source=item.source,
                minute_bars=tuple(bar.model_copy(deep=True) for bar in item.minute_bars),
                market_context=_freeze_local_sim_snapshot_value(item.market_context),
            )
            for symbol, item in self.market_inputs.items()
        }
        normalized_errors = {
            str(symbol): _freeze_local_sim_snapshot_value(payload) for symbol, payload in self.errors.items()
        }
        overlap = set(normalized_inputs).intersection(normalized_errors)
        if overlap:
            raise ValueError(f"LocalSimMarketSnapshotV1 symbol has both input and error: {sorted(overlap)}")
        for symbol, item in normalized_inputs.items():
            if item.symbol != symbol or item.trade_date != self.trade_date or item.source != self.source:
                raise ValueError(f"LocalSimMarketSnapshotV1 input identity mismatch for {symbol}")
        payload = {
            "schema_version": self.schema_version,
            "trade_date": self.trade_date.isoformat(),
            "as_of_time": self.as_of_time.isoformat(),
            "source": self.source.value,
            "market_inputs": {
                symbol: {
                    "bars": [bar.model_dump(mode="json") for bar in item.minute_bars],
                    "market_context": _local_sim_snapshot_json_value(item.market_context),
                }
                for symbol, item in sorted(normalized_inputs.items())
            },
            "errors": {
                symbol: _local_sim_snapshot_json_value(error) for symbol, error in sorted(normalized_errors.items())
            },
        }
        if self.schema_version == "local_sim_market_snapshot_v2":
            context_id = str(getattr(self, "daily_trading_context_id", "") or "").strip()
            context_hash = str(getattr(self, "daily_trading_context_hash", "") or "").strip()
            symbol_set_hash = str(getattr(self, "symbol_set_hash", "") or "").strip()
            if not context_id or not context_hash or not symbol_set_hash:
                raise ValueError("LocalSimMarketSnapshotV2 requires frozen daily context identity")
            payload.update(
                {
                    "daily_trading_context_id": context_id,
                    "daily_trading_context_hash": context_hash,
                    "symbol_set": sorted(set(normalized_inputs).union(normalized_errors)),
                    "symbol_set_hash": symbol_set_hash,
                }
            )
        canonical_payload = _local_sim_snapshot_json_value(payload)
        digest = hashlib.sha256(
            json.dumps(
                canonical_payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        if self.snapshot_hash and self.snapshot_hash != digest:
            raise ValueError("LocalSimMarketSnapshotV1 snapshot_hash mismatch")
        expected_id = f"lsmd_{digest}"
        if self.snapshot_id and self.snapshot_id != expected_id:
            raise ValueError("LocalSimMarketSnapshotV1 snapshot_id mismatch")
        object.__setattr__(self, "market_inputs", MappingProxyType(normalized_inputs))
        object.__setattr__(self, "errors", MappingProxyType(normalized_errors))
        object.__setattr__(self, "snapshot_hash", digest)
        object.__setattr__(self, "snapshot_id", expected_id)


@dataclass(frozen=True)
class LocalSimMarketSnapshotV2(LocalSimMarketSnapshotV1):
    """TDX-only cadence snapshot referencing one immutable daily context."""

    schema_version: str = "local_sim_market_snapshot_v2"
    snapshot_id: str = ""
    snapshot_hash: str = ""
    daily_trading_context_id: str = ""
    daily_trading_context_hash: str = ""
    symbol_set_hash: str = ""


def _freeze_local_sim_snapshot_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        invalid_keys = [key for key in value if not isinstance(key, str)]
        if invalid_keys:
            raise TypeError(
                f"LocalSimMarketSnapshotV1 mappings require string keys; got {type(invalid_keys[0]).__name__}"
            )
        return MappingProxyType({key: _freeze_local_sim_snapshot_value(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_local_sim_snapshot_value(item) for item in value)
    if isinstance(value, Enum):
        return _freeze_local_sim_snapshot_value(value.value)
    if isinstance(value, (datetime, date, Decimal, str, bool, int)) or value is None:
        return deepcopy(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("LocalSimMarketSnapshotV1 numeric values must be finite")
        return value
    raise TypeError(f"LocalSimMarketSnapshotV1 only accepts canonical JSON-like values; got {type(value).__name__}")


def _local_sim_snapshot_json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        invalid_keys = [key for key in value if not isinstance(key, str)]
        if invalid_keys:
            raise TypeError(
                f"LocalSimMarketSnapshotV1 mappings require string keys; got {type(invalid_keys[0]).__name__}"
            )
        return {key: _local_sim_snapshot_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_local_sim_snapshot_json_value(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("LocalSimMarketSnapshotV1 Decimal values must be finite")
        return str(value)
    if isinstance(value, Enum):
        return _local_sim_snapshot_json_value(value.value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("LocalSimMarketSnapshotV1 numeric values must be finite")
        return value
    if isinstance(value, (str, bool, int)) or value is None:
        return value
    raise TypeError(f"LocalSimMarketSnapshotV1 only accepts canonical JSON-like values; got {type(value).__name__}")


@dataclass(frozen=True)
class DailySuspendStatus:
    """Explicit daily suspension status loaded from the authoritative source."""

    symbol: str
    trade_date: date
    is_suspended: bool
    suspend_type: str | None = None
    suspend_timing: str | None = None
    source: str = "market.suspend_d"


@dataclass(frozen=True)
class PreTradeTradabilityStatus:
    """Daily pre-trade tradability evidence for order-generation gates."""

    symbol: str
    trade_date: date
    is_tradable: bool
    reason_code: str
    source: str
    suspend_status: dict[str, Any] | None = None
    quote_evidence: dict[str, Any] | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": "pre_trade_tradability_status_v1",
            "symbol": self.symbol,
            "trade_date": self.trade_date.isoformat(),
            "is_tradable": self.is_tradable,
            "reason_code": self.reason_code,
            "source": self.source,
            "suspend_status": self.suspend_status,
            "quote_evidence": self.quote_evidence,
        }


def pre_trade_tradability_is_suspended(
    tradability: Mapping[str, Any] | None,
    *,
    symbol: str,
) -> bool:
    """Read canonical suspension evidence without truthy coercion or aliases."""

    if not tradability:
        return False
    if "suspend_status" not in tradability:
        legacy_keys = sorted(key for key in ("is_suspended", "suspended", "suspend_d") if key in tradability)
        if not legacy_keys:
            return False
        raise DataUnavailableError(
            "LocalSim pre-trade suspension evidence uses a non-canonical schema",
            context={
                "reason_code": "LOCALSIM_PRE_TRADE_SUSPEND_SCHEMA_INVALID",
                "symbol": symbol,
                "legacy_keys": legacy_keys,
            },
        )
    suspend_status = tradability.get("suspend_status")
    if not isinstance(suspend_status, Mapping):
        raise DataUnavailableError(
            "LocalSim pre-trade suspension evidence must be an object",
            context={
                "reason_code": "LOCALSIM_PRE_TRADE_SUSPEND_SCHEMA_INVALID",
                "symbol": symbol,
                "suspend_status_type": type(suspend_status).__name__,
            },
        )
    is_suspended = suspend_status.get("is_suspended")
    if not isinstance(is_suspended, bool):
        raise DataUnavailableError(
            "LocalSim pre-trade suspension evidence requires a boolean is_suspended",
            context={
                "reason_code": "LOCALSIM_PRE_TRADE_SUSPEND_SCHEMA_INVALID",
                "symbol": symbol,
                "is_suspended_type": type(is_suspended).__name__,
            },
        )
    return is_suspended


@dataclass(frozen=True)
class DailyStStatus:
    """Point-in-time ST/*ST status from the authoritative local market table."""

    symbol: str
    trade_date: date
    is_st: bool
    source: str = "market.stock_st"
    start_date: date | None = None
    end_date: date | None = None


@dataclass(frozen=True)
class EquityInstrumentMetadata:
    """Exact-symbol stock-basic authority for Phase 1 quote context preload."""

    symbol: str
    market: str
    exchange: str
    list_status: str
    list_date: date | None
    delist_date: date | None
    product_type: str
    source: str
    source_version: str

    @property
    def is_listed_a_share_equity(self) -> bool:
        expected_exchange = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}.get(self.symbol.rsplit(".", 1)[-1])
        return (
            self.product_type == "EQUITY"
            and self.list_status == "L"
            and expected_exchange is not None
            and self.exchange == expected_exchange
        )


class SuspendStatusProvider(Protocol):
    """Provider boundary for daily suspension status."""

    def get_suspend_status(self, symbol: str, trade_date: date) -> DailySuspendStatus: ...


class StStatusProvider(Protocol):
    """Provider boundary for point-in-time ST/*ST status."""

    def get_st_status(self, symbol: str, trade_date: date) -> DailyStStatus: ...


class LimitPriceProvider(Protocol):
    """Provider boundary for daily limit prices."""

    def get_limit_price(self, symbol: str, trade_date: date) -> DailyLimitPrice: ...


class EquityInstrumentMetadataProvider(Protocol):
    """Read exact stock_basic authority; callers must not infer a product type."""

    def get_equity_metadata(self, symbol: str, trade_date: date) -> EquityInstrumentMetadata: ...


class TradingCalendarSnapshot(BaseModel):
    """Immutable result obtained from the global Trading Calendar Service."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    trade_date: date
    is_trading_day: bool
    previous_trading_date: date | None = None
    next_trading_date: date | None = None
    source: str = "TradingCalendarStatusService"
    snapshot_id: str

    @field_validator("snapshot_id", "source")
    @classmethod
    def _required_calendar_text(cls, value: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("calendar snapshot identity is required")
        return text

    @classmethod
    def build(
        cls,
        *,
        trade_date: date,
        is_trading_day: bool,
        previous_trading_date: date | None,
        next_trading_date: date | None,
        source: str,
    ) -> "TradingCalendarSnapshot":
        payload = {
            "trade_date": trade_date.isoformat(),
            "is_trading_day": is_trading_day,
            "previous_trading_date": previous_trading_date.isoformat() if previous_trading_date else None,
            "next_trading_date": next_trading_date.isoformat() if next_trading_date else None,
            "source": source,
        }
        return cls(
            trade_date=trade_date,
            is_trading_day=is_trading_day,
            previous_trading_date=previous_trading_date,
            next_trading_date=next_trading_date,
            source=source,
            snapshot_id=f"tcs_{_canonical_json_sha256(payload)[:16]}",
        )

    @model_validator(mode="after")
    def _identity_matches_content(self) -> "TradingCalendarSnapshot":
        payload = {
            "trade_date": self.trade_date.isoformat(),
            "is_trading_day": self.is_trading_day,
            "previous_trading_date": self.previous_trading_date.isoformat() if self.previous_trading_date else None,
            "next_trading_date": self.next_trading_date.isoformat() if self.next_trading_date else None,
            "source": self.source,
        }
        if self.snapshot_id != f"tcs_{_canonical_json_sha256(payload)[:16]}":
            raise ValueError("calendar snapshot_id does not match content")
        return self


class SelectionInputSnapshot(BaseModel):
    """Frozen PIT input boundary consumed by the signal layer."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    trade_date: date
    cutoff_at: datetime
    source: str
    source_version: str
    symbol_set: tuple[str, ...]
    payload_hash: str

    @field_validator("source", "source_version")
    @classmethod
    def _required_selection_input_text(cls, value: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("selection input source identity is required")
        return text

    @classmethod
    def build(
        cls,
        *,
        trade_date: date,
        cutoff_at: datetime,
        source: str,
        source_version: str,
        symbol_set: tuple[str, ...],
    ) -> "SelectionInputSnapshot":
        canonical_symbols = tuple(sorted(set(symbol_set)))
        payload = {
            "trade_date": trade_date.isoformat(),
            "cutoff_at": cutoff_at.isoformat(),
            "source": source,
            "source_version": source_version,
            "symbol_set": list(canonical_symbols),
        }
        return cls(
            trade_date=trade_date,
            cutoff_at=cutoff_at,
            source=source,
            source_version=source_version,
            symbol_set=canonical_symbols,
            payload_hash=_canonical_json_sha256(payload),
        )

    @model_validator(mode="after")
    def _canonical_selection_input(self) -> "SelectionInputSnapshot":
        if self.cutoff_at.tzinfo is None or self.cutoff_at.utcoffset() is None:
            raise ValueError("selection cutoff_at must be timezone-aware")
        if self.symbol_set != tuple(sorted(set(self.symbol_set))):
            raise ValueError("selection symbol_set must be unique and sorted")
        payload = {
            "trade_date": self.trade_date.isoformat(),
            "cutoff_at": self.cutoff_at.isoformat(),
            "source": self.source,
            "source_version": self.source_version,
            "symbol_set": list(self.symbol_set),
        }
        if self.payload_hash != _canonical_json_sha256(payload):
            raise ValueError("selection payload_hash does not match content")
        return self


class _FrozenMinuteBar(MinuteBar):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _freeze_minute_bars(bars: tuple[MinuteBar, ...]) -> tuple[_FrozenMinuteBar, ...]:
    return tuple(_FrozenMinuteBar.model_validate(bar.model_dump(mode="python")) for bar in bars)


class CausalMinuteBatch(BaseModel):
    """TDX current-day bars observed no later than observed_until."""

    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    symbol: str
    trade_date: date
    observed_until: datetime
    bars: tuple[MinuteBar, ...]
    source: MinuteDataSource = MinuteDataSource.TDX_REALTIME
    batch_hash: str

    @model_validator(mode="after")
    def _causal_boundary(self) -> "CausalMinuteBatch":
        if self.source is not MinuteDataSource.TDX_REALTIME:
            raise ValueError("CausalMinuteBatch requires TDX_REALTIME")
        if self.observed_until.tzinfo is None or self.observed_until.utcoffset() is None:
            raise ValueError("observed_until must be timezone-aware")
        timestamps = [bar.bar_time for bar in self.bars]
        if timestamps != sorted(timestamps) or any(value.date() != self.trade_date for value in timestamps):
            raise ValueError("causal minute bars must be ordered and belong to trade_date")
        cursor = _shanghai_naive(self.observed_until)
        if any(_shanghai_naive(value) > cursor for value in timestamps):
            raise ValueError("causal minute batch contains a future bar")
        payload = {
            "symbol": self.symbol,
            "trade_date": self.trade_date.isoformat(),
            "observed_until": self.observed_until.isoformat(),
            "source": self.source.value,
            "bars": [bar.model_dump(mode="json") for bar in self.bars],
        }
        if self.batch_hash != _canonical_json_sha256(payload):
            raise ValueError("causal minute batch_hash does not match content")
        object.__setattr__(self, "bars", _freeze_minute_bars(self.bars))
        return self


class HistoricalMinuteBatch(BaseModel):
    """Completed historical-day bars; never valid for the current trading day."""

    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    symbol: str
    trade_date: date
    current_trading_date: date
    bars: tuple[MinuteBar, ...]
    source: MinuteDataSource = MinuteDataSource.DB_HISTORICAL
    batch_hash: str

    @model_validator(mode="after")
    def _completed_day_boundary(self) -> "HistoricalMinuteBatch":
        if self.source is not MinuteDataSource.DB_HISTORICAL:
            raise ValueError("HistoricalMinuteBatch requires DB_HISTORICAL")
        if self.trade_date >= self.current_trading_date:
            raise ValueError("historical minute source is forbidden for the current or future trading day")
        timestamps = [bar.bar_time for bar in self.bars]
        if timestamps != sorted(timestamps) or any(value.date() != self.trade_date for value in timestamps):
            raise ValueError("historical minute bars must be ordered and belong to trade_date")
        payload = {
            "symbol": self.symbol,
            "trade_date": self.trade_date.isoformat(),
            "current_trading_date": self.current_trading_date.isoformat(),
            "source": self.source.value,
            "bars": [bar.model_dump(mode="json") for bar in self.bars],
        }
        if self.batch_hash != _canonical_json_sha256(payload):
            raise ValueError("historical minute batch_hash does not match content")
        object.__setattr__(self, "bars", _freeze_minute_bars(self.bars))
        return self
