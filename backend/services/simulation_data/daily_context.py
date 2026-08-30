"""Immutable broker-specific daily simulation data contracts.

These contracts belong to the data layer.  They are frozen before execution
planning and must never be rebuilt by an intraday cadence.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


def canonical_json_sha256(payload: dict[str, Any] | list[Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _require_sha256_text(value: str, *, field: str) -> str:
    if type(value) is not str:
        raise ValueError(f"{field} must be a lowercase SHA-256 hex digest")
    text = value.strip().lower()
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise ValueError(f"{field} must be a lowercase SHA-256 hex digest")
    return text


def _require_strict_json_value(value: Any, *, path: str) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError(f"{path} requires exact string keys")
            _require_strict_json_value(item, path=f"{path}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _require_strict_json_value(item, path=f"{path}[{index}]")
        return
    if type(value) is float and not math.isfinite(value):
        raise ValueError(f"{path} contains a non-finite number")
    if value is None or type(value) in {str, bool, int, float}:
        return
    raise ValueError(f"{path} contains a non-canonical JSON value")


class SimulationBrokerBackend(str, Enum):
    LOCAL_SIM = "local_sim"
    MINIQMT_SIM = "minqmt_sim"


class DailyTradingSymbolFactV1(BaseModel):
    """Immutable per-symbol daily trading facts captured before plan compile."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    symbol: str
    trade_date: date
    pre_close: float
    pre_close_source: Literal[
        "market.stk_limit.pre_close",
        "TDX_REALTIME.batch_quote.pre_close",
        "MINIQMT_REALTIME.broker_quote.pre_close",
    ] = "market.stk_limit.pre_close"
    pre_close_evidence_hash: str | None = None
    up_limit: float
    down_limit: float
    price_basis: Literal["raw"] = "raw"
    stk_limit_row_hash: str
    is_st: bool
    st_source: str
    st_evidence_hash: str
    is_suspended: bool
    suspend_type: str | None = None
    suspend_timing: str | None = None
    suspend_source: str
    board: str
    lot_rule: dict[str, int]

    @field_validator(
        "symbol",
        "pre_close_source",
        "stk_limit_row_hash",
        "st_source",
        "st_evidence_hash",
        "suspend_source",
        "board",
    )
    @classmethod
    def _daily_fact_required_text(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("daily trading fact text field is required")
        return value

    @field_validator("pre_close", "up_limit", "down_limit")
    @classmethod
    def _daily_fact_positive_finite_price(cls, value: float) -> float:
        value = float(value)
        if not math.isfinite(value) or value <= 0:
            raise ValueError("daily trading fact prices must be positive and finite")
        return value

    @model_validator(mode="after")
    def _daily_fact_price_and_lot_contract(self) -> "DailyTradingSymbolFactV1":
        if not self.down_limit < self.pre_close < self.up_limit:
            raise ValueError("daily trading fact requires down_limit < pre_close < up_limit")
        if set(self.lot_rule) != {"min_quantity", "increment"}:
            raise ValueError("daily trading fact lot_rule has invalid fields")
        if any(type(value) is not int or value <= 0 for value in self.lot_rule.values()):
            raise ValueError("daily trading fact lot_rule values must be positive exact integers")
        row_payload: dict[str, Any] = {
            "source": "market.stk_limit",
            "symbol": self.symbol,
            "trade_date": self.trade_date.isoformat(),
            "pre_close": self.pre_close,
            "up_limit": self.up_limit,
            "down_limit": self.down_limit,
            "price_basis": self.price_basis,
        }
        if self.pre_close_source == "market.stk_limit.pre_close":
            if self.pre_close_evidence_hash is not None:
                raise ValueError("raw stk_limit pre_close must not carry quote evidence")
        else:
            if not self.pre_close_evidence_hash:
                raise ValueError("broker quote pre_close requires evidence hash")
            row_payload["pre_close_source"] = self.pre_close_source
            row_payload["pre_close_evidence_hash"] = self.pre_close_evidence_hash
        if self.stk_limit_row_hash != canonical_json_sha256(row_payload):
            raise ValueError("daily trading fact stk_limit_row_hash mismatch")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        payload = self.model_dump(mode="json")
        if self.pre_close_source == "market.stk_limit.pre_close":
            payload.pop("pre_close_source", None)
            payload.pop("pre_close_evidence_hash", None)
        return payload


class DailyTradingContextV1(BaseModel):
    """Plan-bound daily market authority; never rebuilt by the live cadence."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["daily_trading_context_v1"] = "daily_trading_context_v1"
    context_id: str
    context_hash: str
    trade_date: date
    timezone: Literal["Asia/Shanghai"] = "Asia/Shanghai"
    plan_identity: str
    binding_identity: str
    package_identity: str
    symbol_set: tuple[str, ...]
    symbol_set_hash: str
    calendar_service_snapshot_id: str
    captured_at: datetime
    sources: dict[str, dict[str, Any]]
    symbols: dict[str, DailyTradingSymbolFactV1]

    @field_validator(
        "context_id",
        "context_hash",
        "plan_identity",
        "binding_identity",
        "package_identity",
        "symbol_set_hash",
        "calendar_service_snapshot_id",
    )
    @classmethod
    def _daily_context_required_text(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("daily trading context identity field is required")
        return value

    @model_validator(mode="after")
    def _daily_context_identity_matches_payload(self) -> "DailyTradingContextV1":
        if self.captured_at.tzinfo is None or self.captured_at.utcoffset() is None:
            raise ValueError("daily trading context captured_at must be timezone-aware")
        _require_strict_json_value(self.sources, path="daily_trading_context.sources")
        normalized = tuple(sorted(set(self.symbol_set)))
        if normalized != self.symbol_set or not normalized:
            raise ValueError("daily trading context symbol_set must be non-empty, unique, and sorted")
        if set(self.symbols) != set(normalized):
            raise ValueError("daily trading context symbols must exactly cover symbol_set")
        if set(self.sources) != {"stk_limit", "stock_st", "suspend_d"}:
            raise ValueError("daily trading context sources are incomplete")
        if self.sources["stk_limit"].get("source") != "market.stk_limit":
            raise ValueError("daily trading context limit authority must be market.stk_limit")
        for symbol, fact in self.symbols.items():
            if fact.symbol != symbol or fact.trade_date != self.trade_date:
                raise ValueError("daily trading context symbol fact identity mismatch")
        expected_symbol_hash = canonical_json_sha256(list(normalized))
        if self.symbol_set_hash != expected_symbol_hash:
            raise ValueError("daily trading context symbol_set_hash mismatch")
        digest = canonical_json_sha256(self.canonical_payload())
        if self.context_hash != digest:
            raise ValueError("daily trading context context_hash mismatch")
        if self.context_id != f"dtc_{digest[:16]}":
            raise ValueError("daily trading context context_id mismatch")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "trade_date": self.trade_date.isoformat(),
            "timezone": self.timezone,
            "plan_identity": self.plan_identity,
            "binding_identity": self.binding_identity,
            "package_identity": self.package_identity,
            "symbol_set": list(self.symbol_set),
            "symbol_set_hash": self.symbol_set_hash,
            "calendar_service_snapshot_id": self.calendar_service_snapshot_id,
            "captured_at": self.captured_at.isoformat(),
            "sources": self.sources,
            "symbols": {symbol: fact.canonical_payload() for symbol, fact in sorted(self.symbols.items())},
        }

    def carrier_payload(self) -> dict[str, Any]:
        return {
            **self.canonical_payload(),
            "context_id": self.context_id,
            "context_hash": self.context_hash,
        }


class DailyLimitAuthorityV2(str, Enum):
    TUSHARE_STK_LIMIT = "TUSHARE_STK_LIMIT"
    TDX_REFERENCE_DERIVED_V1 = "TDX_REFERENCE_DERIVED_V1"
    MINIQMT_INSTRUMENT_DETAIL_V1 = "MINIQMT_INSTRUMENT_DETAIL_V1"
    NO_DAILY_LIMIT = "NO_DAILY_LIMIT"
    UNAVAILABLE = "UNAVAILABLE"


class DailyTradingAuthorityStateV2(str, Enum):
    READY = "READY"
    NO_DAILY_LIMIT = "NO_DAILY_LIMIT"
    SYMBOL_FAILED = "SYMBOL_FAILED"


class DailyLimitResolverV2(str, Enum):
    LOCALSIM_STK_LIMIT_TDX_V1 = "LOCALSIM_STK_LIMIT_TDX_V1"
    MINIQMT_INSTRUMENT_DETAIL_V1 = "MINIQMT_INSTRUMENT_DETAIL_V1"


DAILY_LIMIT_AUTHORITY_BY_BROKER_V2: dict[SimulationBrokerBackend, frozenset[DailyLimitAuthorityV2]] = {
    SimulationBrokerBackend.LOCAL_SIM: frozenset(
        {
            DailyLimitAuthorityV2.TUSHARE_STK_LIMIT,
            DailyLimitAuthorityV2.TDX_REFERENCE_DERIVED_V1,
            DailyLimitAuthorityV2.NO_DAILY_LIMIT,
            DailyLimitAuthorityV2.UNAVAILABLE,
        }
    ),
    SimulationBrokerBackend.MINIQMT_SIM: frozenset(
        {
            DailyLimitAuthorityV2.MINIQMT_INSTRUMENT_DETAIL_V1,
            DailyLimitAuthorityV2.NO_DAILY_LIMIT,
            DailyLimitAuthorityV2.UNAVAILABLE,
        }
    ),
}

DAILY_LIMIT_RESOLVER_BY_BROKER_V2: dict[SimulationBrokerBackend, DailyLimitResolverV2] = {
    SimulationBrokerBackend.LOCAL_SIM: DailyLimitResolverV2.LOCALSIM_STK_LIMIT_TDX_V1,
    SimulationBrokerBackend.MINIQMT_SIM: DailyLimitResolverV2.MINIQMT_INSTRUMENT_DETAIL_V1,
}


def _price_is_tick_aligned(value: float, tick: float) -> bool:
    try:
        price = Decimal(str(value))
        price_tick = Decimal(str(tick))
    except (InvalidOperation, TypeError, ValueError):
        return False
    return price.is_finite() and price_tick.is_finite() and price_tick > 0 and price % price_tick == 0


class DailyTradingSymbolFactV2(BaseModel):
    """Broker-bound immutable daily trading facts for one exact symbol."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    symbol: str
    trade_date: date
    authority_state: DailyTradingAuthorityStateV2
    limit_authority: DailyLimitAuthorityV2
    has_daily_limit: bool
    pre_close: float | None = None
    up_limit: float | None = None
    down_limit: float | None = None
    price_tick: float | None = None
    price_basis: Literal["raw"] = "raw"
    source_evidence_hash: str
    rule_version: str | None = None
    derivation_hash: str | None = None
    authority_reason_code: str | None = None
    is_st: bool
    st_source: str
    st_evidence_hash: str
    is_suspended: bool
    suspend_type: str | None = None
    suspend_timing: str | None = None
    suspend_source: str
    board: str
    lot_rule: dict[str, int]

    @field_validator("has_daily_limit", "is_st", "is_suspended", mode="before")
    @classmethod
    def _daily_fact_v2_exact_boolean(cls, value: Any) -> bool:
        if type(value) is not bool:
            raise ValueError("daily trading V2 boolean fields require exact booleans")
        return value

    @field_validator("symbol", "st_source", "suspend_source", "board", mode="before")
    @classmethod
    def _daily_fact_v2_required_text(cls, value: Any) -> str:
        if type(value) is not str:
            raise ValueError("daily trading V2 text fields require exact strings")
        text = value.strip()
        if not text:
            raise ValueError("daily trading V2 text field is required")
        return text

    @field_validator("rule_version", "authority_reason_code", "suspend_type", "suspend_timing", mode="before")
    @classmethod
    def _daily_fact_v2_optional_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        if type(value) is not str:
            raise ValueError("daily trading V2 optional text fields require exact strings")
        text = value.strip()
        return text or None

    @field_validator("source_evidence_hash", "st_evidence_hash")
    @classmethod
    def _daily_fact_v2_required_hash(cls, value: str, info: Any) -> str:
        return _require_sha256_text(value, field=info.field_name)

    @field_validator("derivation_hash")
    @classmethod
    def _daily_fact_v2_optional_hash(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_sha256_text(value, field="derivation_hash")

    @field_validator("pre_close", "up_limit", "down_limit", "price_tick", mode="before")
    @classmethod
    def _daily_fact_v2_optional_positive_price(cls, value: Any) -> float | None:
        if value is None:
            return None
        if type(value) is bool:
            raise ValueError("daily trading V2 prices must not be booleans")
        number = float(value)
        if not math.isfinite(number) or number <= 0:
            raise ValueError("daily trading V2 prices must be positive and finite")
        return number

    @field_validator("lot_rule", mode="before")
    @classmethod
    def _daily_fact_v2_exact_lot_rule(cls, value: Any) -> dict[str, int]:
        if type(value) is not dict or set(value) != {"min_quantity", "increment"}:
            raise ValueError("daily trading V2 lot_rule has invalid fields")
        if any(type(item) is not int or item <= 0 for item in value.values()):
            raise ValueError("daily trading V2 lot_rule values must be positive exact integers")
        return dict(value)

    @model_validator(mode="after")
    def _daily_fact_v2_contract(self) -> "DailyTradingSymbolFactV2":
        price_fields = (self.pre_close, self.up_limit, self.down_limit, self.price_tick)
        if self.authority_state is DailyTradingAuthorityStateV2.READY:
            if self.limit_authority not in {
                DailyLimitAuthorityV2.TUSHARE_STK_LIMIT,
                DailyLimitAuthorityV2.TDX_REFERENCE_DERIVED_V1,
                DailyLimitAuthorityV2.MINIQMT_INSTRUMENT_DETAIL_V1,
            }:
                raise ValueError("READY daily trading V2 fact has an invalid limit authority")
            if not self.has_daily_limit or any(value is None for value in price_fields):
                raise ValueError("READY daily trading V2 fact requires complete daily limit prices")
            assert self.pre_close is not None
            assert self.up_limit is not None
            assert self.down_limit is not None
            assert self.price_tick is not None
            if not self.down_limit < self.pre_close < self.up_limit:
                raise ValueError("READY daily trading V2 fact requires down_limit < pre_close < up_limit")
            if not all(
                _price_is_tick_aligned(value, self.price_tick)
                for value in (self.pre_close, self.up_limit, self.down_limit)
            ):
                raise ValueError("READY daily trading V2 prices must align to price_tick")
            if self.authority_reason_code is not None:
                raise ValueError("READY daily trading V2 fact must not carry a failure reason")
            if self.limit_authority is DailyLimitAuthorityV2.TDX_REFERENCE_DERIVED_V1:
                if not self.rule_version or not self.derivation_hash:
                    raise ValueError("TDX-derived daily trading V2 fact requires rule and derivation hashes")
            elif self.rule_version is not None or self.derivation_hash is not None:
                raise ValueError("direct daily limit authorities must not carry derivation fields")
        elif self.authority_state is DailyTradingAuthorityStateV2.NO_DAILY_LIMIT:
            if self.limit_authority is not DailyLimitAuthorityV2.NO_DAILY_LIMIT or self.has_daily_limit:
                raise ValueError("NO_DAILY_LIMIT state requires the matching authority and no limit")
            if (
                self.pre_close is None
                or self.price_tick is None
                or self.up_limit is not None
                or self.down_limit is not None
            ):
                raise ValueError("NO_DAILY_LIMIT fact requires pre_close/price_tick and no limit bounds")
            if not _price_is_tick_aligned(self.pre_close, self.price_tick):
                raise ValueError("NO_DAILY_LIMIT pre_close must align to price_tick")
            if not self.rule_version or not self.authority_reason_code or self.derivation_hash is not None:
                raise ValueError("NO_DAILY_LIMIT fact requires versioned reason evidence without derivation hash")
        else:
            if self.limit_authority is not DailyLimitAuthorityV2.UNAVAILABLE or self.has_daily_limit:
                raise ValueError("SYMBOL_FAILED state requires UNAVAILABLE authority")
            if any(value is not None for value in price_fields):
                raise ValueError("SYMBOL_FAILED fact must not expose partial price authority")
            if not self.authority_reason_code:
                raise ValueError("SYMBOL_FAILED fact requires a stable reason code")
            if self.rule_version is not None or self.derivation_hash is not None:
                raise ValueError("SYMBOL_FAILED fact must not carry derived-success fields")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class DailyLimitResolutionV2(BaseModel):
    """Root identity for one broker-specific daily-limit resolution attempt."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    resolver: DailyLimitResolverV2
    allowed_source_kinds: tuple[DailyLimitAuthorityV2, ...]
    actual_source_kinds: tuple[DailyLimitAuthorityV2, ...]
    trade_date: date
    read_at: datetime
    root_batch_hash: str
    rule_versions: tuple[str, ...] = ()

    @field_validator("root_batch_hash")
    @classmethod
    def _daily_limit_resolution_hash(cls, value: str) -> str:
        return _require_sha256_text(value, field="root_batch_hash")

    @model_validator(mode="after")
    def _daily_limit_resolution_contract(self) -> "DailyLimitResolutionV2":
        if self.read_at.tzinfo is None or self.read_at.utcoffset() is None:
            raise ValueError("daily limit resolution read_at must be timezone-aware")
        for field_name, values in (
            ("allowed_source_kinds", self.allowed_source_kinds),
            ("actual_source_kinds", self.actual_source_kinds),
        ):
            canonical = tuple(sorted(set(values), key=lambda value: value.value))
            if values != canonical or not values:
                raise ValueError(f"{field_name} must be non-empty, unique, and sorted")
        if not set(self.actual_source_kinds).issubset(self.allowed_source_kinds):
            raise ValueError("actual daily limit sources must be allowed by the resolver")
        normalized_versions = tuple(sorted(set(str(value or "").strip() for value in self.rule_versions)))
        if self.rule_versions != normalized_versions or any(not value for value in normalized_versions):
            raise ValueError("daily limit rule_versions must be non-empty strings, unique, and sorted")
        return self


class DailyTradingContextSourcesV2(BaseModel):
    """Strict source evidence referenced by a V2 daily trading context."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    limit_resolution: DailyLimitResolutionV2
    stock_st: dict[str, Any]
    suspend_d: dict[str, Any]
    stk_limit: dict[str, Any] | None = None
    tdx_reference: dict[str, Any] | None = None
    miniqmt_instrument: dict[str, Any] | None = None

    @staticmethod
    def _root_payload(
        *,
        resolver: DailyLimitResolverV2,
        allowed_source_kinds: tuple[DailyLimitAuthorityV2, ...],
        actual_source_kinds: tuple[DailyLimitAuthorityV2, ...],
        trade_date: date,
        read_at: datetime,
        rule_versions: tuple[str, ...],
        stock_st: dict[str, Any],
        suspend_d: dict[str, Any],
        stk_limit: dict[str, Any] | None,
        tdx_reference: dict[str, Any] | None,
        miniqmt_instrument: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return {
            "schema_version": "daily_trading_context_sources_v2",
            "resolver": resolver.value,
            "allowed_source_kinds": [value.value for value in allowed_source_kinds],
            "actual_source_kinds": [value.value for value in actual_source_kinds],
            "trade_date": trade_date.isoformat(),
            "read_at": read_at.isoformat(),
            "rule_versions": list(rule_versions),
            "stock_st": stock_st,
            "suspend_d": suspend_d,
            "stk_limit": stk_limit,
            "tdx_reference": tdx_reference,
            "miniqmt_instrument": miniqmt_instrument,
        }

    @classmethod
    def build(
        cls,
        *,
        resolver: DailyLimitResolverV2,
        allowed_source_kinds: tuple[DailyLimitAuthorityV2, ...],
        actual_source_kinds: tuple[DailyLimitAuthorityV2, ...],
        trade_date: date,
        read_at: datetime,
        rule_versions: tuple[str, ...],
        stock_st: dict[str, Any],
        suspend_d: dict[str, Any],
        stk_limit: dict[str, Any] | None = None,
        tdx_reference: dict[str, Any] | None = None,
        miniqmt_instrument: dict[str, Any] | None = None,
    ) -> "DailyTradingContextSourcesV2":
        root_payload = cls._root_payload(
            resolver=resolver,
            allowed_source_kinds=allowed_source_kinds,
            actual_source_kinds=actual_source_kinds,
            trade_date=trade_date,
            read_at=read_at,
            rule_versions=rule_versions,
            stock_st=stock_st,
            suspend_d=suspend_d,
            stk_limit=stk_limit,
            tdx_reference=tdx_reference,
            miniqmt_instrument=miniqmt_instrument,
        )
        resolution = DailyLimitResolutionV2(
            resolver=resolver,
            allowed_source_kinds=allowed_source_kinds,
            actual_source_kinds=actual_source_kinds,
            trade_date=trade_date,
            read_at=read_at,
            root_batch_hash=canonical_json_sha256(root_payload),
            rule_versions=rule_versions,
        )
        return cls(
            limit_resolution=resolution,
            stock_st=stock_st,
            suspend_d=suspend_d,
            stk_limit=stk_limit,
            tdx_reference=tdx_reference,
            miniqmt_instrument=miniqmt_instrument,
        )

    @model_validator(mode="after")
    def _daily_trading_context_sources_contract(self) -> "DailyTradingContextSourcesV2":
        for name in ("stock_st", "suspend_d"):
            evidence = getattr(self, name)
            if not evidence:
                raise ValueError(f"daily trading context V2 {name} evidence is required")
            _require_strict_json_value(evidence, path=f"daily_trading_context_v2.sources.{name}")
        for name in ("stk_limit", "tdx_reference", "miniqmt_instrument"):
            evidence = getattr(self, name)
            if evidence is not None:
                if not evidence:
                    raise ValueError(f"daily trading context V2 {name} evidence must not be empty")
                _require_strict_json_value(evidence, path=f"daily_trading_context_v2.sources.{name}")
        actual = set(self.limit_resolution.actual_source_kinds)
        if DailyLimitAuthorityV2.TUSHARE_STK_LIMIT in actual and self.stk_limit is None:
            raise ValueError("TUSHARE_STK_LIMIT authority requires stk_limit source evidence")
        if DailyLimitAuthorityV2.TDX_REFERENCE_DERIVED_V1 in actual and self.tdx_reference is None:
            raise ValueError("TDX_REFERENCE_DERIVED_V1 authority requires TDX source evidence")
        if DailyLimitAuthorityV2.MINIQMT_INSTRUMENT_DETAIL_V1 in actual and self.miniqmt_instrument is None:
            raise ValueError("MINIQMT_INSTRUMENT_DETAIL_V1 authority requires instrument evidence")
        resolution = self.limit_resolution
        expected_root_hash = canonical_json_sha256(
            self._root_payload(
                resolver=resolution.resolver,
                allowed_source_kinds=resolution.allowed_source_kinds,
                actual_source_kinds=resolution.actual_source_kinds,
                trade_date=resolution.trade_date,
                read_at=resolution.read_at,
                rule_versions=resolution.rule_versions,
                stock_st=self.stock_st,
                suspend_d=self.suspend_d,
                stk_limit=self.stk_limit,
                tdx_reference=self.tdx_reference,
                miniqmt_instrument=self.miniqmt_instrument,
            )
        )
        if resolution.root_batch_hash != expected_root_hash:
            raise ValueError("daily trading context V2 root_batch_hash mismatch")
        return self


class DailyTradingContextV2(BaseModel):
    """Broker-specific plan-bound daily authority for new simulation plans."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["daily_trading_context_v2"] = "daily_trading_context_v2"
    context_id: str
    context_hash: str
    trade_date: date
    timezone: Literal["Asia/Shanghai"] = "Asia/Shanghai"
    plan_identity: str
    binding_identity: str
    package_identity: str
    symbol_set: tuple[str, ...]
    symbol_set_hash: str
    calendar_service_snapshot_id: str
    captured_at: datetime
    broker_backend: SimulationBrokerBackend
    sources: DailyTradingContextSourcesV2
    symbols: dict[str, DailyTradingSymbolFactV2]

    @model_validator(mode="before")
    @classmethod
    def _daily_context_v2_raw_mapping_contract(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        symbol_set = value.get("symbol_set")
        if isinstance(symbol_set, (list, tuple)) and any(type(symbol) is not str for symbol in symbol_set):
            raise ValueError("daily trading context V2 symbol_set requires exact string identities")
        symbols = value.get("symbols")
        if isinstance(symbols, dict) and any(type(symbol) is not str for symbol in symbols):
            raise ValueError("daily trading context V2 symbols require exact string keys")
        return value

    @field_validator(
        "context_id",
        "context_hash",
        "plan_identity",
        "binding_identity",
        "package_identity",
        "symbol_set_hash",
        "calendar_service_snapshot_id",
        mode="before",
    )
    @classmethod
    def _daily_context_v2_required_text(cls, value: Any) -> str:
        if type(value) is not str:
            raise ValueError("daily trading context V2 identity fields require exact strings")
        text = value.strip()
        if not text:
            raise ValueError("daily trading context V2 identity field is required")
        return text

    @field_validator("context_hash", "symbol_set_hash")
    @classmethod
    def _daily_context_v2_hash(cls, value: str, info: Any) -> str:
        return _require_sha256_text(value, field=info.field_name)

    @model_validator(mode="after")
    def _daily_context_v2_identity_matches_payload(self) -> "DailyTradingContextV2":
        if self.captured_at.tzinfo is None or self.captured_at.utcoffset() is None:
            raise ValueError("daily trading context V2 captured_at must be timezone-aware")
        normalized = tuple(sorted(set(self.symbol_set)))
        if normalized != self.symbol_set or not normalized:
            raise ValueError("daily trading context V2 symbol_set must be non-empty, unique, and sorted")
        if set(self.symbols) != set(normalized):
            raise ValueError("daily trading context V2 symbols must exactly cover symbol_set")
        for symbol, fact in self.symbols.items():
            if fact.symbol != symbol or fact.trade_date != self.trade_date:
                raise ValueError("daily trading context V2 symbol fact identity mismatch")

        resolution = self.sources.limit_resolution
        expected_resolver = DAILY_LIMIT_RESOLVER_BY_BROKER_V2[self.broker_backend]
        expected_allowed = tuple(
            sorted(DAILY_LIMIT_AUTHORITY_BY_BROKER_V2[self.broker_backend], key=lambda value: value.value)
        )
        if resolution.resolver is not expected_resolver or resolution.allowed_source_kinds != expected_allowed:
            raise ValueError("daily trading context V2 resolver does not match broker authority")
        if resolution.trade_date != self.trade_date:
            raise ValueError("daily trading context V2 resolution trade_date mismatch")
        actual_authorities = tuple(
            sorted({fact.limit_authority for fact in self.symbols.values()}, key=lambda value: value.value)
        )
        if resolution.actual_source_kinds != actual_authorities:
            raise ValueError("daily trading context V2 actual source set does not match symbol facts")
        if not set(actual_authorities).issubset(DAILY_LIMIT_AUTHORITY_BY_BROKER_V2[self.broker_backend]):
            raise ValueError("daily trading context V2 carries a cross-broker limit authority")
        actual_rule_versions = tuple(
            sorted({fact.rule_version for fact in self.symbols.values() if fact.rule_version is not None})
        )
        if resolution.rule_versions != actual_rule_versions:
            raise ValueError("daily trading context V2 rule versions do not match symbol facts")
        if self.broker_backend is SimulationBrokerBackend.LOCAL_SIM:
            if self.sources.miniqmt_instrument is not None:
                raise ValueError("LocalSIM daily trading context must not carry MiniQMT evidence")
            if self.sources.stk_limit is None and self.sources.tdx_reference is None:
                raise ValueError("LocalSIM daily trading context requires Tushare or TDX attempt evidence")
        else:
            if self.sources.stk_limit is not None or self.sources.tdx_reference is not None:
                raise ValueError("MiniQMT daily trading context must not carry Tushare or TDX evidence")
            if self.sources.miniqmt_instrument is None:
                raise ValueError("MiniQMT daily trading context requires instrument-detail evidence")

        expected_symbol_hash = canonical_json_sha256(list(normalized))
        if self.symbol_set_hash != expected_symbol_hash:
            raise ValueError("daily trading context V2 symbol_set_hash mismatch")
        digest = canonical_json_sha256(self.canonical_payload())
        if self.context_hash != digest:
            raise ValueError("daily trading context V2 context_hash mismatch")
        if self.context_id != f"dtc_{digest[:16]}":
            raise ValueError("daily trading context V2 context_id mismatch")
        return self

    @classmethod
    def build(
        cls,
        *,
        trade_date: date,
        plan_identity: str,
        binding_identity: str,
        package_identity: str,
        calendar_service_snapshot_id: str,
        captured_at: datetime,
        broker_backend: SimulationBrokerBackend,
        sources: DailyTradingContextSourcesV2,
        symbols: dict[str, DailyTradingSymbolFactV2],
    ) -> "DailyTradingContextV2":
        if type(symbols) is not dict or any(
            type(symbol) is not str or not symbol or symbol != symbol.strip() for symbol in symbols
        ):
            raise ValueError("daily trading context V2 build requires canonical string symbol keys")
        canonical_symbols = {
            symbol: DailyTradingSymbolFactV2.model_validate(fact) for symbol, fact in sorted(symbols.items())
        }
        symbol_set = tuple(canonical_symbols)
        symbol_set_hash = canonical_json_sha256(list(symbol_set))
        payload = {
            "schema_version": "daily_trading_context_v2",
            "trade_date": trade_date.isoformat(),
            "timezone": "Asia/Shanghai",
            "plan_identity": plan_identity,
            "binding_identity": binding_identity,
            "package_identity": package_identity,
            "symbol_set": list(symbol_set),
            "symbol_set_hash": symbol_set_hash,
            "calendar_service_snapshot_id": calendar_service_snapshot_id,
            "captured_at": captured_at.isoformat(),
            "broker_backend": broker_backend.value,
            "sources": sources.model_dump(mode="json"),
            "symbols": {symbol: fact.canonical_payload() for symbol, fact in canonical_symbols.items()},
        }
        digest = canonical_json_sha256(payload)
        return cls(
            context_id=f"dtc_{digest[:16]}",
            context_hash=digest,
            trade_date=trade_date,
            plan_identity=plan_identity,
            binding_identity=binding_identity,
            package_identity=package_identity,
            symbol_set=symbol_set,
            symbol_set_hash=symbol_set_hash,
            calendar_service_snapshot_id=calendar_service_snapshot_id,
            captured_at=captured_at,
            broker_backend=broker_backend,
            sources=sources,
            symbols=canonical_symbols,
        )

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "trade_date": self.trade_date.isoformat(),
            "timezone": self.timezone,
            "plan_identity": self.plan_identity,
            "binding_identity": self.binding_identity,
            "package_identity": self.package_identity,
            "symbol_set": list(self.symbol_set),
            "symbol_set_hash": self.symbol_set_hash,
            "calendar_service_snapshot_id": self.calendar_service_snapshot_id,
            "captured_at": self.captured_at.isoformat(),
            "broker_backend": self.broker_backend.value,
            "sources": self.sources.model_dump(mode="json"),
            "symbols": {symbol: fact.canonical_payload() for symbol, fact in sorted(self.symbols.items())},
        }

    def carrier_payload(self) -> dict[str, Any]:
        return {
            **self.canonical_payload(),
            "context_id": self.context_id,
            "context_hash": self.context_hash,
        }
