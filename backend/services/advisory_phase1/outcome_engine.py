"""Pure, frozen-input outcome calculations for Advisory Phase 1C-3.

The engine has one code path for CANDIDATE and UNIVERSE owners.  It receives
only frozen policy, calendar, source-revision and price-path inputs; it never
queries current data or routes through Selection, Paper, simulation or QMT.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_EVEN, localcontext
from enum import Enum
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase1.label_policy import (
    ExitBasis,
    OutcomePolicySet,
    Projection,
)
from backend.services.advisory_phase1.source_ledger import SourceLedgerError
from backend.services.advisory_phase1.source_revision import SourceRevisionSet, build_source_revision_set


OUTCOME_CALCULATION_SCHEMA_VERSION = "advisory_phase1_outcome_calculation_v1"
CALCULATION_EVIDENCE_SCHEMA_VERSION = "advisory_phase1_calculation_evidence_v1"

REASON_POLICY_INVALID = "ADVISORY_PHASE1C3_POLICY_INVALID"
REASON_CALENDAR_INVALID = "ADVISORY_PHASE1C3_CALENDAR_INVALID"
REASON_SOURCE_INCOMPLETE = "ADVISORY_PHASE1C3_SOURCE_INCOMPLETE"
REASON_ENTRY_UNAVAILABLE = "ADVISORY_PHASE1C3_ENTRY_UNAVAILABLE"
REASON_PATH_ORDER_UNAVAILABLE = "ADVISORY_PHASE1C3_PATH_ORDER_UNAVAILABLE"
REASON_BARRIER_ORDER_AMBIGUOUS = "ADVISORY_PHASE1C3_BARRIER_ORDER_AMBIGUOUS"
REASON_COST_UNAVAILABLE = "ADVISORY_PHASE1C3_COST_UNAVAILABLE"
REASON_BENCHMARK_UNAVAILABLE = "ADVISORY_PHASE1C3_BENCHMARK_UNAVAILABLE"
REASON_TERMINAL_SETTLEMENT_UNAVAILABLE = "ADVISORY_PHASE1C3_TERMINAL_SETTLEMENT_UNAVAILABLE"


class OutcomeContractError(ValueError):
    """Stable fail-closed error for invalid immutable outcome input."""

    def __init__(self, reason_code: str, detail: str) -> None:
        self.reason_code = reason_code
        super().__init__(f"{reason_code}: {detail}")


class OwnerType(str, Enum):
    CANDIDATE = "CANDIDATE"
    UNIVERSE = "UNIVERSE"


class MaturityStatus(str, Enum):
    PENDING = "PENDING"
    MATURED = "MATURED"
    RIGHT_CENSORED = "RIGHT_CENSORED"
    UNAVAILABLE = "UNAVAILABLE"


class OutcomeEventStatus(str, Enum):
    NONE = "NONE"
    TERMINAL = "TERMINAL"
    BARRIER = "BARRIER"


class EntryStatus(str, Enum):
    EXECUTABLE = "EXECUTABLE"
    NOT_EXECUTABLE = "NOT_EXECUTABLE"
    EXECUTION_AMBIGUOUS = "EXECUTION_AMBIGUOUS"
    UNAVAILABLE = "UNAVAILABLE"


class BarrierStatus(str, Enum):
    NONE = "NONE"
    PATH_TOUCH_NOT_SELLABLE = "PATH_TOUCH_NOT_SELLABLE"
    HIT_TARGET = "HIT_TARGET"
    HIT_STOP = "HIT_STOP"
    ORDER_AMBIGUOUS = "ORDER_AMBIGUOUS"
    UNAVAILABLE = "UNAVAILABLE"


class TerminalDisposition(str, Enum):
    NONE = "NONE"
    TERMINAL = "TERMINAL"
    RIGHT_CENSORED = "RIGHT_CENSORED"


def _require_sha256(value: str, *, field_name: str) -> str:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{field_name} must be lowercase sha256 hex")
    return value


def _require_aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include an explicit timezone")
    return value.astimezone(timezone.utc)


def _normalized_reasons(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({str(value) for value in values if str(value)}))


def _bind_model_hash(model: BaseModel, *, field_name: str) -> str:
    payload = canonicalize(model.model_dump(mode="python", exclude={field_name}))
    digest = canonical_json_sha256(payload)
    supplied = getattr(model, field_name)
    if supplied is not None and supplied != digest:
        raise ValueError(f"{field_name} does not match canonical content")
    object.__setattr__(model, field_name, digest)
    return digest


class OutcomeOwner(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    owner_type: OwnerType
    owner_key: str = Field(min_length=1, max_length=200)
    canonical_signal_id: str = Field(min_length=1, max_length=160)
    observation_version_id: str | None = Field(default=None, min_length=1, max_length=160)
    candidate_stage_evidence_id: str | None = Field(default=None, min_length=1, max_length=160)
    symbol: str = Field(min_length=1, max_length=32)
    decision_as_of_trade_date: date
    universe_layer: str | None = Field(default=None, min_length=1, max_length=160)
    evidence_scope: str = "RETROSPECTIVE_RESEARCH_ONLY"

    @model_validator(mode="after")
    def _owner_boundary(self) -> "OutcomeOwner":
        if self.evidence_scope != "RETROSPECTIVE_RESEARCH_ONLY":
            raise ValueError("outcome owner must remain retrospective research only")
        if self.owner_type is OwnerType.CANDIDATE:
            if self.observation_version_id is None or self.candidate_stage_evidence_id is None:
                raise ValueError("candidate owner requires observation and stage evidence identities")
            if self.universe_layer is not None:
                raise ValueError("candidate owner cannot carry a universe layer")
        else:
            if self.observation_version_id is not None or self.candidate_stage_evidence_id is not None:
                raise ValueError("universe owner cannot carry candidate observation/stage identity")
            if self.universe_layer is None:
                raise ValueError("universe owner requires its frozen universe layer")
        return self


class MissingSourceReceipt(BaseModel):
    """Exact evidence for a declared unavailable projection dependency."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_role: str = Field(min_length=1, max_length=80)
    source_revision_set_hash: str = Field(min_length=64, max_length=64)
    receipt_hash: str | None = Field(default=None, min_length=64, max_length=64)
    failure_observed_at: datetime
    reason_code: str = Field(min_length=1, max_length=160)

    @field_validator("receipt_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return _require_sha256(value, field_name="receipt_hash") if value is not None else None

    @field_validator("source_revision_set_hash")
    @classmethod
    def _source_set_hash(cls, value: str) -> str:
        return _require_sha256(value, field_name="source_revision_set_hash")

    @field_validator("failure_observed_at")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="failure_observed_at")

    @model_validator(mode="after")
    def _canonical_receipt(self) -> "MissingSourceReceipt":
        _bind_model_hash(self, field_name="receipt_hash")
        return self


class SourceMemberBinding(BaseModel):
    """Exact SourceRevisionSet member claimed by one immutable input field."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_role: str = Field(min_length=1, max_length=80)
    source_member_key: str = Field(min_length=64, max_length=64)
    partition_content_hash: str = Field(min_length=64, max_length=64)

    @field_validator("source_member_key", "partition_content_hash")
    @classmethod
    def _hashes(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name)


class DailyPriceBar(BaseModel):
    """One exact historical daily bar and its frozen availability/tradability facts."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    trade_date: date
    open_li: Decimal | None = Field(default=None, gt=Decimal("0"))
    high_li: Decimal | None = Field(default=None, gt=Decimal("0"))
    low_li: Decimal | None = Field(default=None, gt=Decimal("0"))
    close_li: Decimal | None = Field(default=None, gt=Decimal("0"))
    adj_factor: Decimal | None = Field(default=None, gt=Decimal("0"))
    entry_executable: bool
    sell_executable: bool
    source_available_at: datetime
    price_source: SourceMemberBinding
    adjustment_source: SourceMemberBinding
    tradability_source: SourceMemberBinding
    source_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("source_available_at")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="source_available_at")

    @field_validator("source_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return _require_sha256(value, field_name="source_hash") if value is not None else None

    @model_validator(mode="after")
    def _ohlc_order(self) -> "DailyPriceBar":
        values = (self.open_li, self.high_li, self.low_li, self.close_li)
        if all(value is not None for value in values):
            assert self.high_li is not None and self.low_li is not None
            assert self.open_li is not None and self.close_li is not None
            if self.high_li < max(self.open_li, self.close_li) or self.low_li > min(self.open_li, self.close_li):
                raise ValueError("daily OHLC values are inconsistent")
        _bind_model_hash(self, field_name="source_hash")
        return self

    def normalized(self, raw_li: Decimal | None) -> Decimal | None:
        if raw_li is None or self.adj_factor is None:
            return None
        return (raw_li / Decimal("1000")) * self.adj_factor


class PricePath(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    symbol: str = Field(min_length=1, max_length=32)
    bars: tuple[DailyPriceBar, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique_sorted(self) -> "PricePath":
        dates = tuple(item.trade_date for item in self.bars)
        if tuple(sorted(dates)) != dates or len(set(dates)) != len(dates):
            raise ValueError("price path bars must be unique and sorted by trade date")
        return self

    def bar_for(self, trade_date: date) -> DailyPriceBar | None:
        for bar in self.bars:
            if bar.trade_date == trade_date:
                return bar
        return None


class CorporateActionEffect(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    symbol: str = Field(min_length=1, max_length=32)
    effective_trade_date: date
    quantity_multiplier: Decimal = Field(gt=Decimal("0"))
    cashflow_yuan_per_share: Decimal
    rights_subscription_cash_required_yuan_per_share: Decimal = Field(ge=Decimal("0"))
    source_available_at: datetime
    source: SourceMemberBinding
    source_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("source_available_at")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="source_available_at")

    @field_validator("source_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return _require_sha256(value, field_name="corporate_action_source_hash") if value is not None else None

    @model_validator(mode="after")
    def _canonical_hash(self) -> "CorporateActionEffect":
        _bind_model_hash(self, field_name="source_hash")
        return self


class TerminalResolution(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    disposition: TerminalDisposition
    symbol: str | None = Field(default=None, min_length=1, max_length=32)
    event_trade_date: date | None = None
    event_closed_at: datetime | None = None
    source: SourceMemberBinding | None = None
    source_hash: str | None = Field(default=None, min_length=64, max_length=64)
    settlement_raw_li: Decimal | None = Field(default=None, gt=Decimal("0"))
    settlement_adj_factor: Decimal | None = Field(default=None, gt=Decimal("0"))
    settlement_quantity_multiplier: Decimal | None = Field(default=None, gt=Decimal("0"))
    settlement_cashflow_yuan_per_share: Decimal | None = None
    censor_reason_code: str | None = Field(default=None, min_length=1, max_length=160)

    @field_validator("event_closed_at")
    @classmethod
    def _aware(cls, value: datetime | None) -> datetime | None:
        return _require_aware(value, field_name="event_closed_at") if value is not None else None

    @field_validator("source_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return _require_sha256(value, field_name="terminal_source_hash") if value is not None else None

    @model_validator(mode="after")
    def _closure(self) -> "TerminalResolution":
        if self.disposition is TerminalDisposition.NONE:
            values = (
                self.event_trade_date,
                self.event_closed_at,
                self.symbol,
                self.source,
                self.source_hash,
                self.settlement_raw_li,
                self.settlement_adj_factor,
                self.settlement_quantity_multiplier,
                self.settlement_cashflow_yuan_per_share,
                self.censor_reason_code,
            )
            if any(value is not None for value in values):
                raise ValueError("non-terminal resolution cannot carry terminal or censor evidence")
            return self
        if self.event_trade_date is None or self.event_closed_at is None or self.symbol is None or self.source is None:
            raise ValueError("terminal or censor resolution requires symbol, event date, closure time and source binding")
        if self.disposition is TerminalDisposition.RIGHT_CENSORED:
            if self.censor_reason_code is None:
                raise ValueError("right-censored resolution requires a censor reason")
            if any(
                value is not None
                for value in (
                    self.settlement_raw_li,
                    self.settlement_adj_factor,
                    self.settlement_quantity_multiplier,
                    self.settlement_cashflow_yuan_per_share,
                )
            ):
                raise ValueError("right-censored resolution cannot carry terminal settlement fields")
        elif self.censor_reason_code is not None:
            raise ValueError("terminal settlement cannot carry a censor reason")
        _bind_model_hash(self, field_name="source_hash")
        return self


class FrozenEqualWeight(BaseModel):
    """Exact rational 1/N weight; Decimal 1/3 is never treated as exact."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    numerator: int = Field(default=1, ge=1)
    denominator: int = Field(ge=1)

    @model_validator(mode="after")
    def _unit_numerator(self) -> "FrozenEqualWeight":
        if self.numerator != 1:
            raise ValueError("equal-weight numerator must be one")
        return self

    def as_decimal(self) -> Decimal:
        with localcontext() as context:
            context.prec = 50
            return Decimal(self.numerator) / Decimal(self.denominator)


class BenchmarkLeg(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    symbol: str = Field(min_length=1, max_length=32)
    frozen_weight: FrozenEqualWeight
    price_path: PricePath
    corporate_actions: tuple[CorporateActionEffect, ...] = ()
    terminal: TerminalResolution

    @model_validator(mode="after")
    def _symbol_closure(self) -> "BenchmarkLeg":
        if self.price_path.symbol != self.symbol:
            raise ValueError("benchmark price path symbol does not match its leg")
        if any(action.symbol != self.symbol for action in self.corporate_actions):
            raise ValueError("benchmark corporate action symbol does not match its leg")
        if self.terminal.disposition is not TerminalDisposition.NONE and self.terminal.symbol != self.symbol:
            raise ValueError("benchmark terminal symbol does not match its leg")
        return self


class BenchmarkPortfolio(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    universe_layer: str = Field(min_length=1, max_length=160)
    constituent_source: SourceMemberBinding
    constituent_hash: str | None = Field(default=None, min_length=64, max_length=64)
    legs: tuple[BenchmarkLeg, ...] = Field(min_length=1)

    @field_validator("constituent_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return _require_sha256(value, field_name="benchmark_constituent_hash") if value is not None else None

    @model_validator(mode="after")
    def _weights(self) -> "BenchmarkPortfolio":
        if len({leg.symbol for leg in self.legs}) != len(self.legs):
            raise ValueError("benchmark symbols must be unique")
        if any(leg.frozen_weight.denominator != len(self.legs) for leg in self.legs):
            raise ValueError("benchmark legs must carry exact frozen 1/N equal weights")
        constituent_payload = {
            "universe_layer": self.universe_layer,
            "constituent_source": self.constituent_source.model_dump(mode="python"),
            "constituents": [
                {
                    "symbol": leg.symbol,
                    "frozen_weight": leg.frozen_weight.model_dump(mode="python"),
                }
                for leg in sorted(self.legs, key=lambda item: item.symbol)
            ],
        }
        digest = canonical_json_sha256(canonicalize(constituent_payload))
        if self.constituent_hash is not None and self.constituent_hash != digest:
            raise ValueError("constituent_hash does not match frozen symbols and weights")
        object.__setattr__(self, "constituent_hash", digest)
        return self


class OutcomeCalculationRequest(BaseModel):
    """One fully frozen request for one owner/horizon/projection."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = OUTCOME_CALCULATION_SCHEMA_VERSION
    owner: OutcomeOwner
    policies: OutcomePolicySet
    horizon_trading_days: int = Field(ge=0)
    projection: Projection
    label_as_of_ts: datetime
    label_source_revision_set: SourceRevisionSet
    price_path: PricePath
    corporate_actions: tuple[CorporateActionEffect, ...] = ()
    terminal: TerminalResolution
    benchmark: BenchmarkPortfolio | None = None
    missing_source_receipts: tuple[MissingSourceReceipt, ...] = ()

    @field_validator("label_as_of_ts")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="label_as_of_ts")

    @model_validator(mode="after")
    def _frozen_request(self) -> "OutcomeCalculationRequest":
        if self.schema_version != OUTCOME_CALCULATION_SCHEMA_VERSION:
            raise ValueError("unsupported outcome calculation schema version")
        if not self.label_source_revision_set.research_only:
            raise ValueError("outcome calculation source revision set must be research only")
        if self.label_source_revision_set.label_as_of_ts != self.label_as_of_ts:
            raise ValueError("label as-of must exactly match the frozen source revision set")
        if self.projection is Projection.GAP_1D:
            if self.horizon_trading_days != 0 or not self.policies.bundle.gap_1d_enabled:
                raise ValueError("GAP_1D requires frozen horizon zero policy")
        elif self.horizon_trading_days not in self.policies.bundle.horizons:
            raise ValueError("return horizon is absent from the frozen label policy bundle")
        elif self.projection not in self.policies.bundle.projections_by_horizon[self.horizon_trading_days]:
            raise ValueError("projection is absent from the frozen horizon policy")
        if len({receipt.source_role for receipt in self.missing_source_receipts}) != len(self.missing_source_receipts):
            raise ValueError("source failure roles must be unique")
        if any(
            receipt.source_revision_set_hash != self.label_source_revision_set.source_revision_set_hash
            for receipt in self.missing_source_receipts
        ):
            raise ValueError("source failure receipt does not belong to the frozen source revision set")
        if self.price_path.symbol != self.owner.symbol:
            raise ValueError("owner symbol does not match the frozen price path")
        if any(action.symbol != self.owner.symbol for action in self.corporate_actions):
            raise ValueError("candidate corporate action symbol does not match the outcome owner")
        if self.terminal.disposition is not TerminalDisposition.NONE and self.terminal.symbol != self.owner.symbol:
            raise ValueError("candidate terminal symbol does not match the outcome owner")
        self._validate_source_revision_set_identity()
        self._validate_source_closure()
        self._validate_terminal_window()
        if self.benchmark is not None and self.benchmark.universe_layer != self.policies.benchmark.universe_layer:
            raise ValueError("benchmark universe layer does not match the frozen benchmark policy")
        return self

    def _validate_source_revision_set_identity(self) -> None:
        source_set = self.label_source_revision_set
        try:
            rebuilt = build_source_revision_set(
                query_registry_hash=source_set.query_registry_hash,
                requested_source_cutoff=source_set.requested_source_cutoff,
                label_as_of_ts=source_set.label_as_of_ts,
                research_only=source_set.research_only,
                members=list(source_set.members),
            )
        except SourceLedgerError as error:
            raise ValueError(f"invalid source revision set: {error}") from error
        if rebuilt.model_dump(mode="python") != source_set.model_dump(mode="python"):
            raise ValueError("source revision set hash or canonical member order does not match its content")

    def _validate_source_closure(self) -> None:
        members = {member.member_key: member for member in self.label_source_revision_set.members}

        def require(binding: SourceMemberBinding) -> None:
            member = members.get(binding.source_member_key)
            if member is None:
                raise ValueError("input source binding is absent from the frozen source revision set")
            if member.source_role != binding.source_role or member.partition_content_hash != binding.partition_content_hash:
                raise ValueError("input source binding does not match its frozen source revision member")

        for bar in self.price_path.bars:
            require(bar.price_source)
            require(bar.adjustment_source)
            require(bar.tradability_source)
        for action in self.corporate_actions:
            require(action.source)
        if self.terminal.source is not None:
            require(self.terminal.source)
        if self.benchmark is None:
            return
        require(self.benchmark.constituent_source)
        for leg in self.benchmark.legs:
            for bar in leg.price_path.bars:
                require(bar.price_source)
                require(bar.adjustment_source)
                require(bar.tradability_source)
            for action in leg.corporate_actions:
                require(action.source)
            if leg.terminal.source is not None:
                require(leg.terminal.source)

    def _validate_terminal_window(self) -> None:
        if self.terminal.disposition is TerminalDisposition.NONE:
            pass
        elif self.projection is Projection.GAP_1D:
            raise ValueError("GAP_1D cannot carry a terminal event")
        else:
            _, entry_date, _, exit_date = self.policies.calendar.timeline(
                decision_date=self.owner.decision_as_of_trade_date,
                horizon_trading_days=self.horizon_trading_days,
            )
            assert self.terminal.event_trade_date is not None
            assert self.terminal.event_closed_at is not None
            if self.terminal.event_closed_at > self.label_as_of_ts:
                raise ValueError("terminal event cannot be observed after label as-of")
            if not entry_date <= self.terminal.event_trade_date <= exit_date:
                raise ValueError("terminal event must fall between entry and the frozen horizon exit")
        if self.benchmark is None or self.projection is Projection.GAP_1D:
            return
        _, entry_date, _, exit_date = self.policies.calendar.timeline(
            decision_date=self.owner.decision_as_of_trade_date,
            horizon_trading_days=self.horizon_trading_days,
        )
        for leg in self.benchmark.legs:
            if leg.terminal.disposition is TerminalDisposition.NONE:
                continue
            assert leg.terminal.event_trade_date is not None
            assert leg.terminal.event_closed_at is not None
            if leg.terminal.event_closed_at > self.label_as_of_ts:
                raise ValueError("benchmark terminal event cannot be observed after label as-of")
            if not entry_date <= leg.terminal.event_trade_date <= exit_date:
                raise ValueError("benchmark terminal event must fall inside the frozen outcome horizon")

    def missing_receipt_for(self, source_role: str) -> MissingSourceReceipt | None:
        return next((item for item in self.missing_source_receipts if item.source_role == source_role), None)


class CashflowResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    entry_quantity: Decimal = Field(ge=Decimal("0"))
    exit_quantity: Decimal = Field(ge=Decimal("0"))
    buy_execution_price_yuan: Decimal = Field(gt=Decimal("0"))
    sell_execution_price_yuan: Decimal = Field(gt=Decimal("0"))
    buy_notional_yuan: Decimal = Field(ge=Decimal("0"))
    sell_notional_yuan: Decimal = Field(ge=Decimal("0"))
    buy_fee_yuan: Decimal = Field(ge=Decimal("0"))
    sell_fee_yuan: Decimal = Field(ge=Decimal("0"))
    entry_cash_yuan: Decimal = Field(ge=Decimal("0"))
    residual_cash_yuan: Decimal = Field(ge=Decimal("0"))
    exit_cash_yuan: Decimal
    terminal_value_yuan: Decimal
    cost_breakdown_hash: str = Field(min_length=64, max_length=64)

    @field_validator("cost_breakdown_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _require_sha256(value, field_name="cost_breakdown_hash")


class BarrierResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    entry_day_touch_status: BarrierStatus = BarrierStatus.NONE
    executable_status: BarrierStatus = BarrierStatus.NONE
    executable_event_trade_date: date | None = None
    time_to_executable_hit_trading_days: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def _closure(self) -> "BarrierResult":
        hit = self.executable_status in {BarrierStatus.HIT_TARGET, BarrierStatus.HIT_STOP}
        if hit != (self.executable_event_trade_date is not None and self.time_to_executable_hit_trading_days is not None):
            raise ValueError("barrier hit date and holding days must appear together")
        if self.executable_status is BarrierStatus.ORDER_AMBIGUOUS and (
            self.executable_event_trade_date is not None or self.time_to_executable_hit_trading_days is not None
        ):
            raise ValueError("ambiguous barrier order cannot choose an event date")
        return self


class CalculationEvidenceBundle(BaseModel):
    """Canonical bytes are persisted by ``calculation_evidence`` local CAS adapter."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = CALCULATION_EVIDENCE_SCHEMA_VERSION
    evidence_payload: dict[str, Any]
    evidence_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("evidence_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return _require_sha256(value, field_name="evidence_hash") if value is not None else None

    @model_validator(mode="after")
    def _derive_hash(self) -> "CalculationEvidenceBundle":
        if self.schema_version != CALCULATION_EVIDENCE_SCHEMA_VERSION:
            raise ValueError("unsupported calculation evidence schema version")
        digest = canonical_json_sha256({"schema_version": self.schema_version, "evidence_payload": self.evidence_payload})
        if self.evidence_hash is not None and self.evidence_hash != digest:
            raise ValueError("calculation evidence hash does not match canonical payload")
        object.__setattr__(self, "evidence_hash", digest)
        return self

    def canonical_bytes(self) -> bytes:
        from backend.services.advisory_phase0a.policy import canonical_json_text

        return canonical_json_text(
            {"schema_version": self.schema_version, "evidence_payload": self.evidence_payload}
        ).encode("utf-8")


class OutcomeCalculationResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    owner: OutcomeOwner
    projection: Projection
    horizon_trading_days: int
    decision_trade_date: date
    intended_entry_trade_date: date
    earliest_sell_eligible_trade_date: date
    exit_trade_date: date | None
    scheduled_maturity_ts: datetime
    maturity_status: MaturityStatus
    outcome_event_status: OutcomeEventStatus
    entry_status: EntryStatus
    projection_value_decimal: Decimal | None = None
    projection_event_code: str | None = None
    entry_price_raw_yuan: Decimal | None = None
    entry_adj_factor: Decimal | None = None
    exit_price_raw_yuan: Decimal | None = None
    exit_adj_factor: Decimal | None = None
    source_closed_at: datetime | None = None
    event_closed_at: datetime | None = None
    failure_observed_at: datetime | None = None
    missing_source_receipt_hash: str | None = None
    cashflow: CashflowResult | None = None
    benchmark_gross_total_return: Decimal | None = None
    benchmark_net_total_return: Decimal | None = None
    barrier: BarrierResult | None = None
    observed_holding_trading_days: int | None = Field(default=None, ge=0)
    reason_codes: tuple[str, ...] = ()
    projection_payload_hash: str | None = Field(default=None, min_length=64, max_length=64)
    calculation_evidence: CalculationEvidenceBundle

    @field_validator("scheduled_maturity_ts", "source_closed_at", "event_closed_at", "failure_observed_at")
    @classmethod
    def _aware(cls, value: datetime | None, info) -> datetime | None:  # type: ignore[no-untyped-def]
        return _require_aware(value, field_name=info.field_name) if value is not None else None

    @field_validator("missing_source_receipt_hash", "projection_payload_hash")
    @classmethod
    def _hash(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _projection_closure(self) -> "OutcomeCalculationResult":
        if self.maturity_status is MaturityStatus.PENDING:
            if self.projection_value_decimal is not None or self.projection_event_code is not None:
                raise ValueError("pending result cannot carry projection value or event")
        elif self.maturity_status is MaturityStatus.UNAVAILABLE:
            if self.failure_observed_at is None or self.missing_source_receipt_hash is None:
                raise ValueError("unavailable result requires failure evidence")
            if self.projection_value_decimal is not None or self.projection_event_code is not None:
                raise ValueError("unavailable result cannot carry projection value or event")
        elif self.maturity_status is MaturityStatus.RIGHT_CENSORED:
            if self.event_closed_at is None:
                raise ValueError("right-censored result requires event closure")
            if self.projection_event_code != "RIGHT_CENSORED":
                raise ValueError("right-censored result requires RIGHT_CENSORED event code")
            if self.projection is Projection.SURVIVAL and self.projection_value_decimal is None:
                raise ValueError("right-censored survival result requires observed holding value")
            if self.projection is not Projection.SURVIVAL and self.projection_value_decimal is not None:
                raise ValueError("fixed-horizon right-censored result cannot carry a return value")
        else:
            if self.source_closed_at is None:
                raise ValueError("matured result requires closed source timestamp")
            if self.projection is Projection.BARRIER:
                if self.projection_value_decimal is not None or self.projection_event_code is None:
                    raise ValueError("matured barrier result must contain an event code only")
            elif self.projection is Projection.SURVIVAL:
                if self.projection_value_decimal is None or self.projection_event_code is None:
                    raise ValueError("matured survival result requires value and event code")
            elif self.projection_value_decimal is None or self.projection_event_code is not None:
                raise ValueError("matured numeric projection requires one value and no event code")
        payload = self.canonical_projection_payload()
        digest = canonical_json_sha256(payload)
        if self.projection_payload_hash is not None and self.projection_payload_hash != digest:
            raise ValueError("projection_payload_hash does not match immutable result payload")
        object.__setattr__(self, "projection_payload_hash", digest)
        return self

    def canonical_projection_payload(self) -> dict[str, Any]:
        return canonicalize(
            self.model_dump(
                mode="python",
                exclude={"projection_payload_hash", "calculation_evidence"},
            )
        )


@dataclass(frozen=True)
class _PositionValues:
    entry_bar: DailyPriceBar
    exit_bar: DailyPriceBar | None
    exit_raw_yuan: Decimal | None
    exit_adj_factor: Decimal | None
    exit_trade_date: date | None
    event_status: OutcomeEventStatus
    event_closed_at: datetime | None
    terminal_cashflow_yuan_per_share: Decimal
    quantity_multiplier: Decimal


class _MissingInput(RuntimeError):
    def __init__(self, source_role: str, reason_code: str) -> None:
        self.source_role = source_role
        self.reason_code = reason_code
        super().__init__(source_role)


class _KnownUnavailable(_MissingInput):
    def __init__(
        self,
        source_role: str,
        reason_code: str,
        *,
        evidence_observed_at: datetime,
        evidence_hash: str,
        entry_status: EntryStatus,
    ) -> None:
        super().__init__(source_role, reason_code)
        self.evidence_observed_at = evidence_observed_at
        self.evidence_hash = evidence_hash
        self.entry_status = entry_status


class OutcomeEngine:
    """Single frozen-input implementation shared by candidate and universe owners."""

    def calculate(self, request: OutcomeCalculationRequest) -> OutcomeCalculationResult:
        try:
            request = OutcomeCalculationRequest.model_validate(request.model_dump(mode="python"))
        except ValueError as error:
            raise OutcomeContractError(REASON_POLICY_INVALID, f"frozen request failed canonical revalidation: {error}") from error
        try:
            decision_date, entry_date, sell_date, exit_date = request.policies.calendar.timeline(
                decision_date=request.owner.decision_as_of_trade_date,
                horizon_trading_days=max(request.horizon_trading_days, 1),
            )
        except ValueError as error:
            raise OutcomeContractError(REASON_CALENDAR_INVALID, str(error)) from error
        if request.projection is Projection.GAP_1D:
            exit_date_for_projection: date | None = None
            scheduled = self._timestamp(entry_date, request.policies.execution.entry_time)
        else:
            exit_date_for_projection = exit_date
            scheduled = self._timestamp(exit_date, request.policies.execution.exit_time)

        terminal_is_known = (
            request.terminal.disposition is not TerminalDisposition.NONE
            and request.terminal.event_closed_at is not None
            and request.terminal.event_closed_at <= request.label_as_of_ts
        )
        if (
            request.label_as_of_ts < scheduled
            and not terminal_is_known
            and request.projection not in {Projection.BARRIER, Projection.SURVIVAL}
        ):
            return self._result(
                request,
                decision_date=decision_date,
                entry_date=entry_date,
                sell_date=sell_date,
                exit_date=exit_date_for_projection,
                scheduled=scheduled,
                maturity=MaturityStatus.PENDING,
                event_status=OutcomeEventStatus.NONE,
                entry_status=EntryStatus.UNAVAILABLE,
                reason_codes=(REASON_SOURCE_INCOMPLETE,),
            )

        try:
            entry_bar = self._entry_bar(request, entry_date)
            if request.projection is Projection.GAP_1D:
                self._require_action_sources(request, decision_date, entry_date)
                return self._gap_result(request, decision_date, entry_date, sell_date, scheduled, entry_bar)
            if request.terminal.disposition is TerminalDisposition.RIGHT_CENSORED and terminal_is_known:
                return self._right_censored_result(
                    request, decision_date, entry_date, sell_date, exit_date_for_projection, scheduled, entry_bar
                )
            if request.projection is Projection.BARRIER:
                return self._barrier_result(
                    request, decision_date, entry_date, sell_date, exit_date_for_projection, scheduled, entry_bar
                )
            values = self._position_values(request, entry_bar, exit_date)
            if values.exit_trade_date is not None:
                self._require_action_sources(request, entry_date, values.exit_trade_date)
            if request.projection is Projection.RETURN_GROSS:
                return self._gross_result(
                    request, decision_date, entry_date, sell_date, exit_date_for_projection, scheduled, values
                )
            if request.projection in {Projection.PATH_MFE, Projection.PATH_MAE}:
                return self._path_result(
                    request, decision_date, entry_date, sell_date, exit_date_for_projection, scheduled, values
                )
            if request.projection in {Projection.EXECUTABLE_MFE, Projection.EXECUTABLE_MAE}:
                return self._executable_path_result(
                    request, decision_date, entry_date, sell_date, exit_date_for_projection, scheduled, values
                )
            if request.projection is Projection.SURVIVAL:
                return self._survival_result(
                    request, decision_date, entry_date, sell_date, exit_date_for_projection, scheduled, values
                )
            if request.projection in {Projection.RETURN_NET_ABSOLUTE, Projection.RETURN_NET_EXCESS}:
                return self._net_result(
                    request, decision_date, entry_date, sell_date, exit_date_for_projection, scheduled, values
                )
            raise OutcomeContractError(REASON_POLICY_INVALID, f"unsupported projection {request.projection.value}")
        except _KnownUnavailable as missing:
            return self._result(
                request,
                decision_date=decision_date,
                entry_date=entry_date,
                sell_date=sell_date,
                exit_date=exit_date_for_projection,
                scheduled=scheduled,
                maturity=MaturityStatus.UNAVAILABLE,
                event_status=OutcomeEventStatus.NONE,
                entry_status=missing.entry_status,
                failure_observed_at=missing.evidence_observed_at,
                missing_source_receipt_hash=missing.evidence_hash,
                reason_codes=(missing.reason_code,),
            )
        except _MissingInput as missing:
            return self._source_state(
                request,
                decision_date=decision_date,
                entry_date=entry_date,
                sell_date=sell_date,
                exit_date=exit_date_for_projection,
                scheduled=scheduled,
                source_role=missing.source_role,
                reason_code=missing.reason_code,
            )

    def _timestamp(self, trade_date: date, at: time) -> datetime:
        return datetime.combine(trade_date, at, tzinfo=ZoneInfo("Asia/Shanghai")).astimezone(timezone.utc)

    def _entry_bar(self, request: OutcomeCalculationRequest, entry_date: date) -> DailyPriceBar:
        bar = request.price_path.bar_for(entry_date)
        if bar is None or bar.open_li is None or bar.adj_factor is None:
            raise _MissingInput("ENTRY_QUOTE", REASON_ENTRY_UNAVAILABLE)
        if bar.source_available_at > request.label_as_of_ts:
            raise _MissingInput("ENTRY_QUOTE", REASON_SOURCE_INCOMPLETE)
        if not bar.entry_executable:
            raise _KnownUnavailable(
                "ENTRY_EXECUTION",
                REASON_ENTRY_UNAVAILABLE,
                evidence_observed_at=bar.source_available_at,
                evidence_hash=bar.source_hash,
                entry_status=EntryStatus.NOT_EXECUTABLE,
            )
        return bar

    def _position_values(
        self,
        request: OutcomeCalculationRequest,
        entry_bar: DailyPriceBar,
        scheduled_exit_date: date,
    ) -> _PositionValues:
        terminal = request.terminal
        if terminal.disposition is TerminalDisposition.TERMINAL:
            assert terminal.event_trade_date is not None and terminal.event_closed_at is not None
            if terminal.event_closed_at > request.label_as_of_ts:
                raise _MissingInput("TERMINAL_SETTLEMENT", REASON_TERMINAL_SETTLEMENT_UNAVAILABLE)
            if (
                terminal.settlement_raw_li is None
                or terminal.settlement_adj_factor is None
                or terminal.settlement_quantity_multiplier is None
                or terminal.settlement_cashflow_yuan_per_share is None
            ):
                raise _MissingInput("TERMINAL_SETTLEMENT", REASON_TERMINAL_SETTLEMENT_UNAVAILABLE)
            return _PositionValues(
                entry_bar=entry_bar,
                exit_bar=None,
                exit_raw_yuan=terminal.settlement_raw_li / Decimal("1000"),
                exit_adj_factor=terminal.settlement_adj_factor,
                exit_trade_date=terminal.event_trade_date,
                event_status=OutcomeEventStatus.TERMINAL,
                event_closed_at=terminal.event_closed_at,
                terminal_cashflow_yuan_per_share=terminal.settlement_cashflow_yuan_per_share,
                quantity_multiplier=terminal.settlement_quantity_multiplier,
            )
        exit_bar = request.price_path.bar_for(scheduled_exit_date)
        if exit_bar is None or exit_bar.adj_factor is None:
            raise _MissingInput("EXIT_QUOTE", REASON_SOURCE_INCOMPLETE)
        raw_li = exit_bar.close_li if request.policies.execution.exit_basis is ExitBasis.HORIZON_CLOSE_V1 else exit_bar.open_li
        if raw_li is None:
            raise _MissingInput("EXIT_QUOTE", REASON_SOURCE_INCOMPLETE)
        if exit_bar.source_available_at > request.label_as_of_ts:
            raise _MissingInput("EXIT_QUOTE", REASON_SOURCE_INCOMPLETE)
        return _PositionValues(
            entry_bar=entry_bar,
            exit_bar=exit_bar,
            exit_raw_yuan=raw_li / Decimal("1000"),
            exit_adj_factor=exit_bar.adj_factor,
            exit_trade_date=scheduled_exit_date,
            event_status=OutcomeEventStatus.NONE,
            event_closed_at=None,
            terminal_cashflow_yuan_per_share=Decimal("0"),
            quantity_multiplier=Decimal("1"),
        )

    def _source_state(
        self,
        request: OutcomeCalculationRequest,
        *,
        decision_date: date,
        entry_date: date,
        sell_date: date,
        exit_date: date | None,
        scheduled: datetime,
        source_role: str,
        reason_code: str,
    ) -> OutcomeCalculationResult:
        receipt = request.missing_receipt_for(source_role)
        if receipt is not None and receipt.failure_observed_at <= request.label_as_of_ts:
            return self._result(
                request,
                decision_date=decision_date,
                entry_date=entry_date,
                sell_date=sell_date,
                exit_date=exit_date,
                scheduled=scheduled,
                maturity=MaturityStatus.UNAVAILABLE,
                event_status=(OutcomeEventStatus.TERMINAL if source_role == "TERMINAL_SETTLEMENT" else OutcomeEventStatus.NONE),
                entry_status=EntryStatus.UNAVAILABLE if source_role.startswith("ENTRY") else EntryStatus.EXECUTABLE,
                failure_observed_at=receipt.failure_observed_at,
                missing_source_receipt_hash=receipt.receipt_hash,
                event_closed_at=(
                    request.terminal.event_closed_at if source_role == "TERMINAL_SETTLEMENT" else None
                ),
                reason_codes=(reason_code, receipt.reason_code),
            )
        if (
            source_role == "TERMINAL_SETTLEMENT"
            and request.terminal.disposition is TerminalDisposition.TERMINAL
            and request.terminal.event_closed_at is not None
            and request.terminal.event_closed_at <= request.label_as_of_ts
            and request.terminal.source_hash is not None
        ):
            return self._result(
                request,
                decision_date=decision_date,
                entry_date=entry_date,
                sell_date=sell_date,
                exit_date=request.terminal.event_trade_date,
                scheduled=scheduled,
                maturity=MaturityStatus.UNAVAILABLE,
                event_status=OutcomeEventStatus.TERMINAL,
                entry_status=EntryStatus.EXECUTABLE,
                failure_observed_at=request.terminal.event_closed_at,
                missing_source_receipt_hash=request.terminal.source_hash,
                event_closed_at=request.terminal.event_closed_at,
                reason_codes=(reason_code,),
            )
        if request.label_as_of_ts < scheduled:
            return self._result(
                request,
                decision_date=decision_date,
                entry_date=entry_date,
                sell_date=sell_date,
                exit_date=exit_date,
                scheduled=scheduled,
                maturity=MaturityStatus.PENDING,
                event_status=OutcomeEventStatus.NONE,
                entry_status=EntryStatus.UNAVAILABLE if source_role.startswith("ENTRY") else EntryStatus.EXECUTABLE,
                reason_codes=(reason_code,),
            )
        # The source is not known bad without an immutable failure receipt.  It remains
        # PENDING rather than being misreported as a permanent failure or zero return.
        return self._result(
            request,
            decision_date=decision_date,
            entry_date=entry_date,
            sell_date=sell_date,
            exit_date=exit_date,
            scheduled=scheduled,
            maturity=MaturityStatus.PENDING,
            event_status=OutcomeEventStatus.NONE,
            entry_status=EntryStatus.UNAVAILABLE if source_role.startswith("ENTRY") else EntryStatus.EXECUTABLE,
            reason_codes=(reason_code, REASON_SOURCE_INCOMPLETE),
        )

    def _gap_result(
        self,
        request: OutcomeCalculationRequest,
        decision_date: date,
        entry_date: date,
        sell_date: date,
        scheduled: datetime,
        entry_bar: DailyPriceBar,
    ) -> OutcomeCalculationResult:
        decision_bar = request.price_path.bar_for(decision_date)
        if decision_bar is None or decision_bar.close_li is None or decision_bar.adj_factor is None:
            raise _MissingInput("DECISION_CLOSE", REASON_SOURCE_INCOMPLETE)
        if decision_bar.source_available_at > request.label_as_of_ts:
            raise _MissingInput("DECISION_CLOSE", REASON_SOURCE_INCOMPLETE)
        assert entry_bar.open_li is not None and entry_bar.adj_factor is not None
        decision_value = decision_bar.normalized(decision_bar.close_li)
        entry_value = entry_bar.normalized(entry_bar.open_li)
        assert decision_value is not None and entry_value is not None
        return self._result(
            request,
            decision_date=decision_date,
            entry_date=entry_date,
            sell_date=sell_date,
            exit_date=None,
            scheduled=scheduled,
            maturity=MaturityStatus.MATURED,
            event_status=OutcomeEventStatus.NONE,
            entry_status=EntryStatus.EXECUTABLE,
            projection_value=(entry_value / decision_value) - Decimal("1"),
            entry_raw=entry_bar.open_li / Decimal("1000"),
            entry_adj=entry_bar.adj_factor,
            source_closed_at=max(
                [decision_bar.source_available_at, entry_bar.source_available_at]
                + self._action_source_times(request, decision_date, entry_date)
            ),
        )

    def _gross_result(
        self,
        request: OutcomeCalculationRequest,
        decision_date: date,
        entry_date: date,
        sell_date: date,
        exit_date: date | None,
        scheduled: datetime,
        values: _PositionValues,
    ) -> OutcomeCalculationResult:
        assert values.entry_bar.open_li is not None and values.entry_bar.adj_factor is not None
        assert values.exit_raw_yuan is not None and values.exit_adj_factor is not None
        entry_value = values.entry_bar.normalized(values.entry_bar.open_li)
        assert entry_value is not None
        exit_value = values.exit_raw_yuan * values.exit_adj_factor
        return self._result(
            request,
            decision_date=decision_date,
            entry_date=entry_date,
            sell_date=sell_date,
            exit_date=values.exit_trade_date or exit_date,
            scheduled=scheduled,
            maturity=MaturityStatus.MATURED,
            event_status=values.event_status,
            entry_status=EntryStatus.EXECUTABLE,
            projection_value=(exit_value / entry_value) - Decimal("1"),
            entry_raw=values.entry_bar.open_li / Decimal("1000"),
            entry_adj=values.entry_bar.adj_factor,
            exit_raw=values.exit_raw_yuan,
            exit_adj=values.exit_adj_factor,
            source_closed_at=self._source_closed_at(request, values),
            event_closed_at=values.event_closed_at,
        )

    def _path_result(
        self,
        request: OutcomeCalculationRequest,
        decision_date: date,
        entry_date: date,
        sell_date: date,
        exit_date: date | None,
        scheduled: datetime,
        values: _PositionValues,
    ) -> OutcomeCalculationResult:
        if values.exit_trade_date is None:
            raise _MissingInput("PATH", REASON_PATH_ORDER_UNAVAILABLE)
        extrema = self._path_extrema(request, start=entry_date, end=values.exit_trade_date, executable_only=False)
        assert values.entry_bar.open_li is not None
        entry_value = values.entry_bar.normalized(values.entry_bar.open_li)
        assert entry_value is not None
        value = (extrema[0] if request.projection is Projection.PATH_MFE else extrema[1]) / entry_value - Decimal("1")
        return self._result(
            request,
            decision_date=decision_date,
            entry_date=entry_date,
            sell_date=sell_date,
            exit_date=values.exit_trade_date or exit_date,
            scheduled=scheduled,
            maturity=MaturityStatus.MATURED,
            event_status=values.event_status,
            entry_status=EntryStatus.EXECUTABLE,
            projection_value=value,
            entry_raw=values.entry_bar.open_li / Decimal("1000"),
            entry_adj=values.entry_bar.adj_factor,
            exit_raw=values.exit_raw_yuan,
            exit_adj=values.exit_adj_factor,
            source_closed_at=self._source_closed_at(request, values, include_path=True),
            event_closed_at=values.event_closed_at,
        )

    def _executable_path_result(
        self,
        request: OutcomeCalculationRequest,
        decision_date: date,
        entry_date: date,
        sell_date: date,
        exit_date: date | None,
        scheduled: datetime,
        values: _PositionValues,
    ) -> OutcomeCalculationResult:
        if values.exit_trade_date is None:
            raise _MissingInput("EXECUTABLE_PATH", REASON_PATH_ORDER_UNAVAILABLE)
        extrema = self._path_extrema(request, start=sell_date, end=values.exit_trade_date, executable_only=True)
        assert values.entry_bar.open_li is not None
        entry_value = values.entry_bar.normalized(values.entry_bar.open_li)
        assert entry_value is not None
        value = (
            extrema[0] if request.projection is Projection.EXECUTABLE_MFE else extrema[1]
        ) / entry_value - Decimal("1")
        return self._result(
            request,
            decision_date=decision_date,
            entry_date=entry_date,
            sell_date=sell_date,
            exit_date=values.exit_trade_date or exit_date,
            scheduled=scheduled,
            maturity=MaturityStatus.MATURED,
            event_status=values.event_status,
            entry_status=EntryStatus.EXECUTABLE,
            projection_value=value,
            entry_raw=values.entry_bar.open_li / Decimal("1000"),
            entry_adj=values.entry_bar.adj_factor,
            exit_raw=values.exit_raw_yuan,
            exit_adj=values.exit_adj_factor,
            source_closed_at=self._source_closed_at(request, values, include_path=True),
            event_closed_at=values.event_closed_at,
        )

    def _barrier_result(
        self,
        request: OutcomeCalculationRequest,
        decision_date: date,
        entry_date: date,
        sell_date: date,
        exit_date: date | None,
        scheduled: datetime,
        entry_bar: DailyPriceBar,
    ) -> OutcomeCalculationResult:
        if exit_date is None:
            raise _MissingInput("BARRIER_PATH", REASON_PATH_ORDER_UNAVAILABLE)
        assert entry_bar.open_li is not None
        entry_value = entry_bar.normalized(entry_bar.open_li)
        assert entry_value is not None
        self._require_action_sources(request, entry_date, entry_date)
        entry_touch = self._bar_touches(request, entry_bar, entry_value)
        entry_status = (
            BarrierStatus.PATH_TOUCH_NOT_SELLABLE if entry_touch != BarrierStatus.NONE else BarrierStatus.NONE
        )
        terminal = request.terminal
        terminal_date = (
            terminal.event_trade_date
            if terminal.disposition is TerminalDisposition.TERMINAL
            else None
        )
        observed_source_times = [entry_bar.source_available_at]
        for trade_date in request.policies.calendar.trading_days_inclusive(sell_date, exit_date):
            if terminal_date is not None and trade_date >= terminal_date:
                break
            bar = request.price_path.bar_for(trade_date)
            if bar is None or bar.high_li is None or bar.low_li is None or bar.adj_factor is None:
                raise _MissingInput("BARRIER_PATH", REASON_PATH_ORDER_UNAVAILABLE)
            if bar.source_available_at > request.label_as_of_ts:
                raise _MissingInput("BARRIER_PATH", REASON_SOURCE_INCOMPLETE)
            observed_source_times.append(bar.source_available_at)
            if not bar.sell_executable:
                continue
            self._require_action_sources(request, entry_date, trade_date)
            touch = self._bar_touches(request, bar, entry_value)
            if touch is BarrierStatus.ORDER_AMBIGUOUS:
                return self._unavailable_barrier(
                    request,
                    decision_date,
                    entry_date,
                    sell_date,
                    exit_date,
                    scheduled,
                    entry_bar,
                    entry_status,
                    bar,
                )
            if touch in {BarrierStatus.HIT_TARGET, BarrierStatus.HIT_STOP}:
                offset = request.policies.calendar.trading_days_inclusive(entry_date, trade_date)
                barrier = BarrierResult(
                    entry_day_touch_status=entry_status,
                    executable_status=touch,
                    executable_event_trade_date=trade_date,
                    time_to_executable_hit_trading_days=len(offset) - 1,
                )
                return self._result(
                    request,
                    decision_date=decision_date,
                    entry_date=entry_date,
                    sell_date=sell_date,
                    exit_date=exit_date,
                    scheduled=scheduled,
                    maturity=MaturityStatus.MATURED,
                    event_status=OutcomeEventStatus.BARRIER,
                    entry_status=EntryStatus.EXECUTABLE,
                    projection_event_code=touch.value,
                    entry_raw=entry_bar.open_li / Decimal("1000"),
                    entry_adj=entry_bar.adj_factor,
                    source_closed_at=self._path_source_closed_at(request, entry_date, trade_date),
                    event_closed_at=bar.source_available_at,
                    barrier=barrier,
                )
        if terminal_date is not None:
            assert terminal.event_closed_at is not None
            self._require_action_sources(request, entry_date, terminal_date)
            observed_source_times.extend(self._action_source_times(request, entry_date, terminal_date))
            observed_source_times.append(terminal.event_closed_at)
            return self._result(
                request,
                decision_date=decision_date,
                entry_date=entry_date,
                sell_date=sell_date,
                exit_date=terminal_date,
                scheduled=scheduled,
                maturity=MaturityStatus.MATURED,
                event_status=OutcomeEventStatus.TERMINAL,
                entry_status=EntryStatus.EXECUTABLE,
                projection_event_code="TERMINAL",
                entry_raw=entry_bar.open_li / Decimal("1000"),
                entry_adj=entry_bar.adj_factor,
                source_closed_at=max(observed_source_times),
                event_closed_at=terminal.event_closed_at,
                barrier=BarrierResult(entry_day_touch_status=entry_status),
            )
        self._require_action_sources(request, entry_date, exit_date)
        return self._result(
            request,
            decision_date=decision_date,
            entry_date=entry_date,
            sell_date=sell_date,
            exit_date=exit_date,
            scheduled=scheduled,
            maturity=MaturityStatus.MATURED,
            event_status=OutcomeEventStatus.NONE,
            entry_status=EntryStatus.EXECUTABLE,
            projection_event_code="NO_HIT",
            entry_raw=entry_bar.open_li / Decimal("1000"),
            entry_adj=entry_bar.adj_factor,
            source_closed_at=self._path_source_closed_at(request, entry_date, exit_date),
            barrier=BarrierResult(entry_day_touch_status=entry_status),
        )

    def _unavailable_barrier(
        self,
        request: OutcomeCalculationRequest,
        decision_date: date,
        entry_date: date,
        sell_date: date,
        exit_date: date,
        scheduled: datetime,
        entry_bar: DailyPriceBar,
        entry_touch: BarrierStatus,
        bar: DailyPriceBar,
    ) -> OutcomeCalculationResult:
        return self._result(
            request,
            decision_date=decision_date,
            entry_date=entry_date,
            sell_date=sell_date,
            exit_date=exit_date,
            scheduled=scheduled,
            maturity=MaturityStatus.UNAVAILABLE,
            event_status=OutcomeEventStatus.NONE,
            entry_status=EntryStatus.EXECUTABLE,
            entry_raw=entry_bar.open_li / Decimal("1000") if entry_bar.open_li else None,
            entry_adj=entry_bar.adj_factor,
            failure_observed_at=bar.source_available_at,
            missing_source_receipt_hash=bar.source_hash,
            reason_codes=(REASON_BARRIER_ORDER_AMBIGUOUS,),
            barrier=BarrierResult(
                entry_day_touch_status=entry_touch,
                executable_status=BarrierStatus.ORDER_AMBIGUOUS,
            ),
        )

    def _path_source_closed_at(self, request: OutcomeCalculationRequest, start: date, end: date) -> datetime:
        timestamps: list[datetime] = []
        for trade_date in request.policies.calendar.trading_days_inclusive(start, end):
            bar = request.price_path.bar_for(trade_date)
            if bar is None:
                raise _MissingInput("BARRIER_PATH", REASON_PATH_ORDER_UNAVAILABLE)
            timestamps.append(bar.source_available_at)
        timestamps.extend(self._action_source_times(request, start, end))
        return max(timestamps)

    def _require_action_sources(self, request: OutcomeCalculationRequest, start: date, end: date) -> None:
        for action in request.corporate_actions:
            if start <= action.effective_trade_date <= end and action.source_available_at > request.label_as_of_ts:
                raise _MissingInput("CORPORATE_ACTION", REASON_SOURCE_INCOMPLETE)

    def _action_source_times(self, request: OutcomeCalculationRequest, start: date, end: date) -> list[datetime]:
        return [
            action.source_available_at
            for action in request.corporate_actions
            if start <= action.effective_trade_date <= end
        ]

    def _survival_result(
        self,
        request: OutcomeCalculationRequest,
        decision_date: date,
        entry_date: date,
        sell_date: date,
        exit_date: date | None,
        scheduled: datetime,
        values: _PositionValues,
    ) -> OutcomeCalculationResult:
        terminal = request.terminal
        if terminal.disposition is TerminalDisposition.RIGHT_CENSORED:
            return self._right_censored_result(
                request, decision_date, entry_date, sell_date, exit_date, scheduled, values.entry_bar
            )
        observed_end = values.exit_trade_date or exit_date
        if observed_end is None:
            raise _MissingInput("SURVIVAL", REASON_SOURCE_INCOMPLETE)
        days = len(request.policies.calendar.trading_days_inclusive(entry_date, observed_end)) - 1
        event_code = "TERMINAL" if values.event_status is OutcomeEventStatus.TERMINAL else "SURVIVED_HORIZON"
        return self._result(
            request,
            decision_date=decision_date,
            entry_date=entry_date,
            sell_date=sell_date,
            exit_date=observed_end,
            scheduled=scheduled,
            maturity=MaturityStatus.MATURED,
            event_status=values.event_status,
            entry_status=EntryStatus.EXECUTABLE,
            projection_value=Decimal(days),
            projection_event_code=event_code,
            entry_raw=values.entry_bar.open_li / Decimal("1000") if values.entry_bar.open_li else None,
            entry_adj=values.entry_bar.adj_factor,
            exit_raw=values.exit_raw_yuan,
            exit_adj=values.exit_adj_factor,
            source_closed_at=self._source_closed_at(request, values),
            event_closed_at=values.event_closed_at,
            observed_holding_days=days,
        )

    def _right_censored_result(
        self,
        request: OutcomeCalculationRequest,
        decision_date: date,
        entry_date: date,
        sell_date: date,
        exit_date: date | None,
        scheduled: datetime,
        entry_bar: DailyPriceBar,
    ) -> OutcomeCalculationResult:
        terminal = request.terminal
        assert terminal.event_trade_date is not None and terminal.event_closed_at is not None and terminal.source_hash is not None
        days = len(request.policies.calendar.trading_days_inclusive(entry_date, terminal.event_trade_date)) - 1
        return self._result(
            request,
            decision_date=decision_date,
            entry_date=entry_date,
            sell_date=sell_date,
            exit_date=exit_date,
            scheduled=scheduled,
            maturity=MaturityStatus.RIGHT_CENSORED,
            event_status=OutcomeEventStatus.NONE,
            entry_status=EntryStatus.EXECUTABLE,
            projection_value=Decimal(days) if request.projection is Projection.SURVIVAL else None,
            projection_event_code="RIGHT_CENSORED",
            entry_raw=entry_bar.open_li / Decimal("1000") if entry_bar.open_li else None,
            entry_adj=entry_bar.adj_factor,
            event_closed_at=terminal.event_closed_at,
            source_closed_at=max(entry_bar.source_available_at, terminal.event_closed_at),
            observed_holding_days=days,
            reason_codes=(terminal.censor_reason_code or "RIGHT_CENSORED",),
        )

    def _net_result(
        self,
        request: OutcomeCalculationRequest,
        decision_date: date,
        entry_date: date,
        sell_date: date,
        exit_date: date | None,
        scheduled: datetime,
        values: _PositionValues,
    ) -> OutcomeCalculationResult:
        cost_failure = request.missing_receipt_for("COST")
        if cost_failure is not None:
            return self._source_state(
                request,
                decision_date=decision_date,
                entry_date=entry_date,
                sell_date=sell_date,
                exit_date=values.exit_trade_date or exit_date,
                scheduled=scheduled,
                source_role="COST",
                reason_code=REASON_COST_UNAVAILABLE,
            )
        if values.exit_bar is not None and not values.exit_bar.sell_executable:
            return self._known_unavailable(
                request,
                decision_date,
                entry_date,
                sell_date,
                values.exit_trade_date or exit_date,
                scheduled,
                values,
                REASON_ENTRY_UNAVAILABLE,
                values.exit_bar,
            )
        candidate = self._cashflow(
            request,
            actions=request.corporate_actions,
            values=values,
            initial_notional=request.policies.bundle.candidate_reference_notional,
        )
        candidate_return = candidate.terminal_value_yuan / request.policies.bundle.candidate_reference_notional - Decimal("1")
        if request.projection is Projection.RETURN_NET_ABSOLUTE:
            return self._result(
                request,
                decision_date=decision_date,
                entry_date=entry_date,
                sell_date=sell_date,
                exit_date=values.exit_trade_date or exit_date,
                scheduled=scheduled,
                maturity=MaturityStatus.MATURED,
                event_status=values.event_status,
                entry_status=EntryStatus.EXECUTABLE,
                projection_value=candidate_return,
                entry_raw=values.entry_bar.open_li / Decimal("1000") if values.entry_bar.open_li else None,
                entry_adj=values.entry_bar.adj_factor,
                exit_raw=values.exit_raw_yuan,
                exit_adj=values.exit_adj_factor,
                source_closed_at=self._source_closed_at(request, values),
                event_closed_at=values.event_closed_at,
                cashflow=candidate,
            )
        benchmark_failure = request.missing_receipt_for("BENCHMARK")
        if benchmark_failure is not None or request.benchmark is None:
            return self._source_state(
                request,
                decision_date=decision_date,
                entry_date=entry_date,
                sell_date=sell_date,
                exit_date=values.exit_trade_date or exit_date,
                scheduled=scheduled,
                source_role="BENCHMARK",
                reason_code=REASON_BENCHMARK_UNAVAILABLE,
            )
        benchmark_return, benchmark_gross, benchmark_source_closed_at = self._benchmark_returns(
            request, entry_date, values.exit_trade_date or exit_date
        )
        return self._result(
            request,
            decision_date=decision_date,
            entry_date=entry_date,
            sell_date=sell_date,
            exit_date=values.exit_trade_date or exit_date,
            scheduled=scheduled,
            maturity=MaturityStatus.MATURED,
            event_status=values.event_status,
            entry_status=EntryStatus.EXECUTABLE,
            projection_value=candidate_return - benchmark_return,
            entry_raw=values.entry_bar.open_li / Decimal("1000") if values.entry_bar.open_li else None,
            entry_adj=values.entry_bar.adj_factor,
            exit_raw=values.exit_raw_yuan,
            exit_adj=values.exit_adj_factor,
            source_closed_at=max(self._source_closed_at(request, values), benchmark_source_closed_at),
            event_closed_at=values.event_closed_at,
            cashflow=candidate,
            benchmark_gross=benchmark_gross,
            benchmark_net=benchmark_return,
        )

    def _known_unavailable(
        self,
        request: OutcomeCalculationRequest,
        decision_date: date,
        entry_date: date,
        sell_date: date,
        exit_date: date | None,
        scheduled: datetime,
        values: _PositionValues,
        reason_code: str,
        evidence_bar: DailyPriceBar,
    ) -> OutcomeCalculationResult:
        return self._result(
            request,
            decision_date=decision_date,
            entry_date=entry_date,
            sell_date=sell_date,
            exit_date=exit_date,
            scheduled=scheduled,
            maturity=MaturityStatus.UNAVAILABLE,
            event_status=values.event_status,
            entry_status=EntryStatus.NOT_EXECUTABLE,
            entry_raw=values.entry_bar.open_li / Decimal("1000") if values.entry_bar.open_li else None,
            entry_adj=values.entry_bar.adj_factor,
            exit_raw=values.exit_raw_yuan,
            exit_adj=values.exit_adj_factor,
            failure_observed_at=evidence_bar.source_available_at,
            missing_source_receipt_hash=evidence_bar.source_hash,
            reason_codes=(reason_code,),
        )

    def _cashflow(
        self,
        request: OutcomeCalculationRequest,
        *,
        actions: tuple[CorporateActionEffect, ...],
        values: _PositionValues,
        initial_notional: Decimal,
        zero_lot_as_cash: bool = False,
    ) -> CashflowResult:
        assert values.entry_bar.open_li is not None and values.exit_raw_yuan is not None
        cost = request.policies.cost
        buy_price = (values.entry_bar.open_li / Decimal("1000")) * (Decimal("1") + cost.slippage_bps / Decimal("10000"))
        sell_price = values.exit_raw_yuan * (Decimal("1") - cost.slippage_bps / Decimal("10000"))
        quantity = self._max_lot_quantity(initial_notional, buy_price, request)
        if quantity == 0:
            if zero_lot_as_cash:
                return self._cash_only_cashflow(
                    request,
                    initial_notional=initial_notional,
                    buy_price=buy_price,
                    sell_price=sell_price,
                )
            raise _KnownUnavailable(
                "COST",
                REASON_COST_UNAVAILABLE,
                evidence_observed_at=values.entry_bar.source_available_at,
                evidence_hash=values.entry_bar.source_hash,
                entry_status=EntryStatus.EXECUTABLE,
            )
        buy_notional = quantity * buy_price
        buy_fee = self._buy_fee(buy_notional, request)
        entry_cash = buy_notional + buy_fee
        residual = initial_notional - entry_cash
        current_quantity = quantity
        action_cashflow = Decimal("0")
        rights_required = Decimal("0")
        action_available = [values.entry_bar.source_available_at]
        last_action: CorporateActionEffect | None = None
        for action in actions:
            if values.exit_trade_date is not None and not (values.entry_bar.trade_date <= action.effective_trade_date <= values.exit_trade_date):
                continue
            if action.source_available_at > request.label_as_of_ts:
                raise _MissingInput("CORPORATE_ACTION", REASON_SOURCE_INCOMPLETE)
            action_cashflow += current_quantity * action.cashflow_yuan_per_share
            rights_required += current_quantity * action.rights_subscription_cash_required_yuan_per_share
            current_quantity *= action.quantity_multiplier
            action_available.append(action.source_available_at)
            last_action = action
        if rights_required > residual:
            assert last_action is not None
            raise _KnownUnavailable(
                "CORPORATE_ACTION",
                REASON_COST_UNAVAILABLE,
                evidence_observed_at=last_action.source_available_at,
                evidence_hash=last_action.source_hash,
                entry_status=EntryStatus.EXECUTABLE,
            )
        action_cashflow += current_quantity * values.terminal_cashflow_yuan_per_share
        exit_quantity = current_quantity * values.quantity_multiplier
        sell_notional = exit_quantity * sell_price
        sell_fee = self._sell_fee(sell_notional, request)
        exit_cash = sell_notional - sell_fee + action_cashflow - rights_required
        terminal_value = residual * (Decimal("1") + request.policies.cash_return.cash_return_rate) + exit_cash
        payload = {
            "entry_quantity": quantity,
            "exit_quantity": exit_quantity,
            "buy_execution_price_yuan": buy_price,
            "sell_execution_price_yuan": sell_price,
            "buy_notional_yuan": buy_notional,
            "sell_notional_yuan": sell_notional,
            "buy_fee_yuan": buy_fee,
            "sell_fee_yuan": sell_fee,
            "entry_cash_yuan": entry_cash,
            "residual_cash_yuan": residual,
            "exit_cash_yuan": exit_cash,
            "terminal_value_yuan": terminal_value,
            "action_source_available": action_available,
        }
        persisted_fields = {key: value for key, value in payload.items() if key != "action_source_available"}
        return CashflowResult(**persisted_fields, cost_breakdown_hash=canonical_json_sha256(payload))

    def _cash_only_cashflow(
        self,
        request: OutcomeCalculationRequest,
        *,
        initial_notional: Decimal,
        buy_price: Decimal,
        sell_price: Decimal,
    ) -> CashflowResult:
        terminal_value = initial_notional * (Decimal("1") + request.policies.cash_return.cash_return_rate)
        payload = {
            "entry_quantity": Decimal("0"),
            "exit_quantity": Decimal("0"),
            "buy_execution_price_yuan": buy_price,
            "sell_execution_price_yuan": sell_price,
            "buy_notional_yuan": Decimal("0"),
            "sell_notional_yuan": Decimal("0"),
            "buy_fee_yuan": Decimal("0"),
            "sell_fee_yuan": Decimal("0"),
            "entry_cash_yuan": Decimal("0"),
            "residual_cash_yuan": initial_notional,
            "exit_cash_yuan": Decimal("0"),
            "terminal_value_yuan": terminal_value,
            "cash_retention_rule": request.policies.cash_return.policy_id.value,
        }
        persisted = {key: value for key, value in payload.items() if key != "cash_retention_rule"}
        return CashflowResult(**persisted, cost_breakdown_hash=canonical_json_sha256(payload))

    def _max_lot_quantity(self, notional: Decimal, buy_price: Decimal, request: OutcomeCalculationRequest) -> Decimal:
        lot = Decimal(request.policies.cost.lot_size)
        raw_lots = (notional / (lot * buy_price)).to_integral_value(rounding=ROUND_FLOOR)
        quantity = raw_lots * lot
        while quantity > 0:
            buy_notional = quantity * buy_price
            if buy_notional + self._buy_fee(buy_notional, request) <= notional:
                return quantity
            quantity -= lot
        return Decimal("0")

    def _buy_fee(self, notional: Decimal, request: OutcomeCalculationRequest) -> Decimal:
        cost = request.policies.cost
        commission = max(notional * cost.commission_buy_rate, cost.minimum_commission)
        return (commission + notional * cost.transfer_fee_buy_rate).quantize(Decimal("0.000001"), rounding=ROUND_HALF_EVEN)

    def _sell_fee(self, notional: Decimal, request: OutcomeCalculationRequest) -> Decimal:
        cost = request.policies.cost
        commission = max(notional * cost.commission_sell_rate, cost.minimum_commission)
        return (
            commission + notional * cost.stamp_duty_sell_rate + notional * cost.transfer_fee_sell_rate
        ).quantize(Decimal("0.000001"), rounding=ROUND_HALF_EVEN)

    def _benchmark_returns(
        self, request: OutcomeCalculationRequest, entry_date: date, exit_date: date | None
    ) -> tuple[Decimal, Decimal, datetime]:
        if exit_date is None or request.benchmark is None:
            raise _MissingInput("BENCHMARK", REASON_BENCHMARK_UNAVAILABLE)
        leg_net_returns: list[Decimal] = []
        leg_gross_returns: list[Decimal] = []
        constituent_member = next(
            member
            for member in request.label_source_revision_set.members
            if member.member_key == request.benchmark.constituent_source.source_member_key
        )
        source_closed_at: list[datetime] = [constituent_member.available_at_max]
        for leg in request.benchmark.legs:
            entry = leg.price_path.bar_for(entry_date)
            if entry is None or entry.open_li is None or entry.adj_factor is None:
                raise _MissingInput("BENCHMARK", REASON_BENCHMARK_UNAVAILABLE)
            if entry.source_available_at > request.label_as_of_ts:
                raise _MissingInput("BENCHMARK", REASON_BENCHMARK_UNAVAILABLE)
            if not entry.entry_executable:
                cash_return = request.policies.cash_return.cash_return_rate
                leg_net_returns.append(cash_return)
                leg_gross_returns.append(cash_return)
                source_closed_at.append(entry.source_available_at)
                continue
            terminal_applies = (
                leg.terminal.disposition is not TerminalDisposition.NONE
                and leg.terminal.event_trade_date is not None
                and leg.terminal.event_trade_date <= exit_date
            )
            if terminal_applies and leg.terminal.disposition is TerminalDisposition.RIGHT_CENSORED:
                raise _MissingInput("BENCHMARK", REASON_BENCHMARK_UNAVAILABLE)
            if terminal_applies and leg.terminal.disposition is TerminalDisposition.TERMINAL:
                if (
                    leg.terminal.event_closed_at is None
                    or leg.terminal.event_closed_at > request.label_as_of_ts
                    or leg.terminal.settlement_raw_li is None
                    or leg.terminal.settlement_adj_factor is None
                    or leg.terminal.settlement_quantity_multiplier is None
                    or leg.terminal.settlement_cashflow_yuan_per_share is None
                ):
                    assert leg.terminal.event_closed_at is not None and leg.terminal.source_hash is not None
                    raise _KnownUnavailable(
                        "BENCHMARK",
                        REASON_BENCHMARK_UNAVAILABLE,
                        evidence_observed_at=leg.terminal.event_closed_at,
                        evidence_hash=leg.terminal.source_hash,
                        entry_status=EntryStatus.EXECUTABLE,
                    )
                exit_bar = None
                leg_exit_raw = leg.terminal.settlement_raw_li / Decimal("1000")
                leg_exit_adj = leg.terminal.settlement_adj_factor
                leg_exit_date = leg.terminal.event_trade_date
                leg_event_status = OutcomeEventStatus.TERMINAL
                leg_event_closed_at = leg.terminal.event_closed_at
                terminal_cashflow = leg.terminal.settlement_cashflow_yuan_per_share
                quantity_multiplier = leg.terminal.settlement_quantity_multiplier
            else:
                exit_bar = leg.price_path.bar_for(exit_date)
                raw_li = (
                    exit_bar.close_li
                    if exit_bar is not None and request.policies.execution.exit_basis is ExitBasis.HORIZON_CLOSE_V1
                    else exit_bar.open_li if exit_bar is not None else None
                )
                if (
                    exit_bar is None
                    or raw_li is None
                    or exit_bar.adj_factor is None
                    or not exit_bar.sell_executable
                    or exit_bar.source_available_at > request.label_as_of_ts
                ):
                    raise _MissingInput("BENCHMARK", REASON_BENCHMARK_UNAVAILABLE)
                leg_exit_raw = raw_li / Decimal("1000")
                leg_exit_adj = exit_bar.adj_factor
                leg_exit_date = exit_date
                leg_event_status = OutcomeEventStatus.NONE
                leg_event_closed_at = None
                terminal_cashflow = Decimal("0")
                quantity_multiplier = Decimal("1")
            values = _PositionValues(
                entry_bar=entry,
                exit_bar=exit_bar,
                exit_raw_yuan=leg_exit_raw,
                exit_adj_factor=leg_exit_adj,
                exit_trade_date=leg_exit_date,
                event_status=leg_event_status,
                event_closed_at=leg_event_closed_at,
                terminal_cashflow_yuan_per_share=terminal_cashflow,
                quantity_multiplier=quantity_multiplier,
            )
            with localcontext() as context:
                context.prec = 50
                allocation = request.policies.bundle.benchmark_portfolio_notional * leg.frozen_weight.as_decimal()
            try:
                cashflow = self._cashflow(
                    request,
                    actions=leg.corporate_actions,
                    values=values,
                    initial_notional=allocation,
                    zero_lot_as_cash=True,
                )
            except _KnownUnavailable as error:
                raise _KnownUnavailable(
                    "BENCHMARK",
                    REASON_BENCHMARK_UNAVAILABLE,
                    evidence_observed_at=error.evidence_observed_at,
                    evidence_hash=error.evidence_hash,
                    entry_status=EntryStatus.EXECUTABLE,
                ) from error
            except _MissingInput as error:
                raise _MissingInput("BENCHMARK", REASON_BENCHMARK_UNAVAILABLE) from error
            entry_norm = entry.normalized(entry.open_li)
            exit_norm = leg_exit_raw * leg_exit_adj
            assert entry_norm is not None
            leg_net_returns.append(cashflow.terminal_value_yuan / allocation - Decimal("1"))
            leg_gross_returns.append((exit_norm / entry_norm) - Decimal("1"))
            source_closed_at.append(entry.source_available_at)
            if exit_bar is not None:
                source_closed_at.append(exit_bar.source_available_at)
            if leg_event_closed_at is not None:
                source_closed_at.append(leg_event_closed_at)
        with localcontext() as context:
            context.prec = 50
            count = Decimal(len(request.benchmark.legs))
            benchmark_net = sum(leg_net_returns, Decimal("0")) / count
            benchmark_gross = sum(leg_gross_returns, Decimal("0")) / count
        return benchmark_net, benchmark_gross, max(source_closed_at)

    def _path_extrema(
        self,
        request: OutcomeCalculationRequest,
        *,
        start: date,
        end: date,
        executable_only: bool,
    ) -> tuple[Decimal, Decimal]:
        highs: list[Decimal] = []
        lows: list[Decimal] = []
        for trade_date in request.policies.calendar.trading_days_inclusive(start, end):
            bar = request.price_path.bar_for(trade_date)
            if bar is None or bar.adj_factor is None:
                raise _MissingInput("PATH", REASON_PATH_ORDER_UNAVAILABLE)
            if bar.source_available_at > request.label_as_of_ts:
                raise _MissingInput("PATH", REASON_SOURCE_INCOMPLETE)
            if executable_only and not bar.sell_executable:
                continue
            if request.policies.execution.exit_basis is ExitBasis.HORIZON_OPEN_V1 and trade_date == end:
                high = bar.normalized(bar.open_li)
                low = high
            else:
                high = bar.normalized(bar.high_li)
                low = bar.normalized(bar.low_li)
            if high is None or low is None:
                raise _MissingInput("PATH", REASON_PATH_ORDER_UNAVAILABLE)
            highs.append(high)
            lows.append(low)
        if not highs or not lows:
            raise _MissingInput("PATH", REASON_PATH_ORDER_UNAVAILABLE)
        return max(highs), min(lows)

    def _bar_touches(
        self,
        request: OutcomeCalculationRequest,
        bar: DailyPriceBar,
        entry_normalized: Decimal,
    ) -> BarrierStatus:
        high = bar.normalized(bar.high_li)
        low = bar.normalized(bar.low_li)
        if high is None or low is None:
            raise _MissingInput("BARRIER_PATH", REASON_PATH_ORDER_UNAVAILABLE)
        target = entry_normalized * (Decimal("1") + request.policies.barrier.target_return)
        stop = entry_normalized * (Decimal("1") + request.policies.barrier.stop_return)
        target_hit = high >= target
        stop_hit = low <= stop
        if target_hit and stop_hit:
            return BarrierStatus.ORDER_AMBIGUOUS
        if target_hit:
            return BarrierStatus.HIT_TARGET
        if stop_hit:
            return BarrierStatus.HIT_STOP
        return BarrierStatus.NONE

    def _source_closed_at(
        self,
        request: OutcomeCalculationRequest,
        values: _PositionValues,
        *,
        include_path: bool = False,
        path_end: date | None = None,
    ) -> datetime:
        timestamps = [values.entry_bar.source_available_at]
        if values.exit_bar is not None:
            timestamps.append(values.exit_bar.source_available_at)
        if values.event_closed_at is not None:
            timestamps.append(values.event_closed_at)
        for action in request.corporate_actions:
            if values.exit_trade_date is not None and values.entry_bar.trade_date <= action.effective_trade_date <= values.exit_trade_date:
                timestamps.append(action.source_available_at)
        if include_path and values.exit_trade_date is not None:
            end = path_end or values.exit_trade_date
            for trade_date in request.policies.calendar.trading_days_inclusive(
                values.entry_bar.trade_date, end
            ):
                bar = request.price_path.bar_for(trade_date)
                if bar is not None:
                    timestamps.append(bar.source_available_at)
        return max(timestamps)

    def _result(
        self,
        request: OutcomeCalculationRequest,
        *,
        decision_date: date,
        entry_date: date,
        sell_date: date,
        exit_date: date | None,
        scheduled: datetime,
        maturity: MaturityStatus,
        event_status: OutcomeEventStatus,
        entry_status: EntryStatus,
        projection_value: Decimal | None = None,
        projection_event_code: str | None = None,
        entry_raw: Decimal | None = None,
        entry_adj: Decimal | None = None,
        exit_raw: Decimal | None = None,
        exit_adj: Decimal | None = None,
        source_closed_at: datetime | None = None,
        event_closed_at: datetime | None = None,
        failure_observed_at: datetime | None = None,
        missing_source_receipt_hash: str | None = None,
        cashflow: CashflowResult | None = None,
        benchmark_gross: Decimal | None = None,
        benchmark_net: Decimal | None = None,
        barrier: BarrierResult | None = None,
        observed_holding_days: int | None = None,
        reason_codes: tuple[str, ...] = (),
    ) -> OutcomeCalculationResult:
        base: dict[str, Any] = {
            "owner": request.owner,
            "projection": request.projection,
            "horizon_trading_days": request.horizon_trading_days,
            "decision_trade_date": decision_date,
            "intended_entry_trade_date": entry_date,
            "earliest_sell_eligible_trade_date": sell_date,
            "exit_trade_date": exit_date,
            "scheduled_maturity_ts": scheduled,
            "maturity_status": maturity,
            "outcome_event_status": event_status,
            "entry_status": entry_status,
            "projection_value_decimal": projection_value,
            "projection_event_code": projection_event_code,
            "entry_price_raw_yuan": entry_raw,
            "entry_adj_factor": entry_adj,
            "exit_price_raw_yuan": exit_raw,
            "exit_adj_factor": exit_adj,
            "source_closed_at": source_closed_at,
            "event_closed_at": event_closed_at,
            "failure_observed_at": failure_observed_at,
            "missing_source_receipt_hash": missing_source_receipt_hash,
            "cashflow": cashflow,
            "benchmark_gross_total_return": benchmark_gross,
            "benchmark_net_total_return": benchmark_net,
            "barrier": barrier,
            "observed_holding_trading_days": observed_holding_days,
            "reason_codes": _normalized_reasons(reason_codes),
        }
        projection_hash = canonical_json_sha256(canonicalize(base))
        evidence = CalculationEvidenceBundle(
            evidence_payload={
                "owner": request.owner.model_dump(mode="python"),
                "policy_bundle_hash": request.policies.bundle.label_policy_bundle_hash,
                "label_source_revision_set_id": request.label_source_revision_set.source_revision_set_id,
                "label_source_revision_set_hash": request.label_source_revision_set.source_revision_set_hash,
                "price_path": request.price_path.model_dump(mode="python"),
                "corporate_actions": [item.model_dump(mode="python") for item in request.corporate_actions],
                "terminal": request.terminal.model_dump(mode="python"),
                "benchmark": request.benchmark.model_dump(mode="python") if request.benchmark else None,
                "projection_payload_hash": projection_hash,
                "formula_schema_version": OUTCOME_CALCULATION_SCHEMA_VERSION,
            }
        )
        return OutcomeCalculationResult(
            **base,
            projection_payload_hash=projection_hash,
            calculation_evidence=evidence,
        )
