"""Frozen policy and calendar contracts for Advisory Phase 1C-3 outcomes.

This module is intentionally pure.  It does not read current market state,
Selection, Paper, simulation, or a database.  Callers must pass a frozen
policy bundle and an exact trading calendar before an outcome can be computed.
"""

from __future__ import annotations

from datetime import date, time
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize


LABEL_POLICY_BUNDLE_SCHEMA_VERSION = "advisory_phase1_label_policy_bundle_v1"
OUTCOME_POLICY_SET_SCHEMA_VERSION = "advisory_phase1_outcome_policy_set_v1"


class LabelPolicyError(ValueError):
    """Raised when an immutable Phase 1C-3 policy contract is incomplete."""


class Projection(str, Enum):
    GAP_1D = "GAP_1D"
    RETURN_GROSS = "RETURN_GROSS"
    RETURN_NET_ABSOLUTE = "RETURN_NET_ABSOLUTE"
    RETURN_NET_EXCESS = "RETURN_NET_EXCESS"
    PATH_MFE = "PATH_MFE"
    PATH_MAE = "PATH_MAE"
    EXECUTABLE_MFE = "EXECUTABLE_MFE"
    EXECUTABLE_MAE = "EXECUTABLE_MAE"
    BARRIER = "BARRIER"
    SURVIVAL = "SURVIVAL"


class StyleFamily(str, Enum):
    SHORT_REBOUND = "SHORT_REBOUND"
    LONG_TREND = "LONG_TREND"
    OTHER_DECLARED = "OTHER_DECLARED"


class EntryBasis(str, Enum):
    NEXT_OPEN_EXECUTABLE_V1 = "NEXT_OPEN_EXECUTABLE_V1"


class ExitBasis(str, Enum):
    HORIZON_CLOSE_V1 = "HORIZON_CLOSE_V1"
    HORIZON_OPEN_V1 = "HORIZON_OPEN_V1"


class CashReturnRule(str, Enum):
    CASH_RETURN_ZERO_V1 = "CASH_RETURN_ZERO_V1"
    FIXED_CASH_RETURN_V1 = "FIXED_CASH_RETURN_V1"


def _require_sha256(value: str, *, field_name: str) -> str:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{field_name} must be lowercase sha256 hex")
    return value


def _bind_content_hash(model: BaseModel, *, field_name: str) -> str:
    """Derive one semantic identity and reject caller-supplied drift."""

    payload = canonicalize(model.model_dump(mode="python", exclude={field_name}))
    digest = canonical_json_sha256(payload)
    supplied = getattr(model, field_name)
    if supplied is not None and supplied != digest:
        raise ValueError(f"{field_name} does not match canonical policy content")
    object.__setattr__(model, field_name, digest)
    return digest


class TradingCalendar(BaseModel):
    """One frozen trading calendar slice used by every outcome projection."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    calendar_version: str = Field(min_length=1, max_length=160)
    calendar_hash: str | None = Field(default=None, min_length=64, max_length=64)
    trading_dates: tuple[date, ...] = Field(min_length=3)

    @field_validator("calendar_hash")
    @classmethod
    def _calendar_hash(cls, value: str | None) -> str | None:
        return _require_sha256(value, field_name="calendar_hash") if value is not None else None

    @model_validator(mode="after")
    def _ordered_dates(self) -> "TradingCalendar":
        if tuple(sorted(self.trading_dates)) != self.trading_dates:
            raise ValueError("trading_dates must be sorted")
        if len(set(self.trading_dates)) != len(self.trading_dates):
            raise ValueError("trading_dates must be unique")
        _bind_content_hash(self, field_name="calendar_hash")
        return self

    def next_trading_day(self, current: date) -> date:
        try:
            index = self.trading_dates.index(current)
        except ValueError as exc:
            raise LabelPolicyError("decision date is absent from the frozen trading calendar") from exc
        if index + 1 >= len(self.trading_dates):
            raise LabelPolicyError("frozen trading calendar lacks the next trading day")
        return self.trading_dates[index + 1]

    def shift_from_entry(self, entry_date: date, horizon_trading_days: int) -> date:
        if horizon_trading_days < 1:
            raise LabelPolicyError("return horizon must be at least one trading day")
        try:
            index = self.trading_dates.index(entry_date)
        except ValueError as exc:
            raise LabelPolicyError("entry date is absent from the frozen trading calendar") from exc
        target = index + horizon_trading_days
        if target >= len(self.trading_dates):
            raise LabelPolicyError("frozen trading calendar lacks the requested horizon")
        return self.trading_dates[target]

    def trading_days_inclusive(self, start: date, end: date) -> tuple[date, ...]:
        try:
            start_index = self.trading_dates.index(start)
            end_index = self.trading_dates.index(end)
        except ValueError as exc:
            raise LabelPolicyError("path boundary is absent from the frozen trading calendar") from exc
        if end_index < start_index:
            raise LabelPolicyError("path end precedes path start in the frozen trading calendar")
        return self.trading_dates[start_index : end_index + 1]

    def timeline(self, *, decision_date: date, horizon_trading_days: int) -> tuple[date, date, date, date]:
        entry_date = self.next_trading_day(decision_date)
        sell_date = self.next_trading_day(entry_date)
        exit_date = self.shift_from_entry(entry_date, horizon_trading_days)
        return decision_date, entry_date, sell_date, exit_date


class LabelPolicyBundle(BaseModel):
    """The immutable semantic identity of one outcome-label policy bundle."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = LABEL_POLICY_BUNDLE_SCHEMA_VERSION
    label_policy_bundle_id: str | None = Field(default=None, min_length=1, max_length=160)
    label_policy_bundle_hash: str | None = Field(default=None, min_length=64, max_length=64)
    label_policy_id: str = Field(min_length=1, max_length=160)
    label_policy_hash: str = Field(min_length=64, max_length=64)
    label_policy_schema_version: str = Field(min_length=1, max_length=160)
    phase1_handoff_bundle_hash: str = Field(min_length=64, max_length=64)
    handoff_readiness_hash: str = Field(min_length=64, max_length=64)
    admission_scope_id: str = Field(min_length=1, max_length=160)
    admission_scope_hash: str = Field(min_length=64, max_length=64)
    audit_target_id: str = Field(min_length=1, max_length=160)
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: str = Field(pattern="^(single_alpha|multi_alpha)$")
    style_family: StyleFamily
    style_assignment_policy_id: str = Field(min_length=1, max_length=160)
    style_assignment_policy_hash: str = Field(min_length=64, max_length=64)
    style_decided_at: date
    calendar_version: str = Field(min_length=1, max_length=160)
    calendar_hash: str = Field(min_length=64, max_length=64)
    price_policy_hash: str = Field(min_length=64, max_length=64)
    adjustment_policy_hash: str = Field(min_length=64, max_length=64)
    entry_execution_policy_hash: str = Field(min_length=64, max_length=64)
    cost_policy_hash: str = Field(min_length=64, max_length=64)
    benchmark_policy_hash: str = Field(min_length=64, max_length=64)
    cash_return_policy_hash: str = Field(min_length=64, max_length=64)
    terminal_return_policy_hash: str = Field(min_length=64, max_length=64)
    barrier_policy_hash: str = Field(min_length=64, max_length=64)
    corporate_action_policy_hash: str = Field(min_length=64, max_length=64)
    symbol_normalization_policy_hash: str = Field(min_length=64, max_length=64)
    horizons: tuple[int, ...] = Field(min_length=1)
    projections_by_horizon: dict[int, tuple[Projection, ...]]
    gap_1d_enabled: bool
    candidate_reference_notional: Decimal = Field(gt=Decimal("0"))
    benchmark_portfolio_notional: Decimal = Field(gt=Decimal("0"))
    currency: str = "CNY"
    price_unit: str = "yuan"
    storage_scale: str = "li_to_yuan_1000"
    research_only: bool = True
    execution_prohibited: bool = True

    @field_validator(
        "label_policy_hash",
        "phase1_handoff_bundle_hash",
        "handoff_readiness_hash",
        "admission_scope_hash",
        "manifest_sha256",
        "style_assignment_policy_hash",
        "calendar_hash",
        "price_policy_hash",
        "adjustment_policy_hash",
        "entry_execution_policy_hash",
        "cost_policy_hash",
        "benchmark_policy_hash",
        "cash_return_policy_hash",
        "terminal_return_policy_hash",
        "barrier_policy_hash",
        "corporate_action_policy_hash",
        "symbol_normalization_policy_hash",
        "label_policy_bundle_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        if value is None:
            return None
        return _require_sha256(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _derive_identity(self) -> "LabelPolicyBundle":
        if self.schema_version != LABEL_POLICY_BUNDLE_SCHEMA_VERSION:
            raise ValueError("unsupported label policy bundle schema version")
        if self.horizons != tuple(sorted(set(self.horizons))) or any(value < 1 for value in self.horizons):
            raise ValueError("horizons must be sorted, unique, and at least one")
        if set(self.projections_by_horizon) != set(self.horizons):
            raise ValueError("projections_by_horizon keys must exactly match horizons")
        for horizon, projections in self.projections_by_horizon.items():
            if not projections or len(set(projections)) != len(projections):
                raise ValueError(f"horizon {horizon} must have unique non-empty projections")
            if Projection.GAP_1D in projections:
                raise ValueError("GAP_1D has fixed horizon zero and is not a return-horizon projection")
        if self.currency != "CNY" or self.price_unit != "yuan" or self.storage_scale != "li_to_yuan_1000":
            raise ValueError("Phase 1C-3 policy bundle must use frozen CNY/yuan/li scale semantics")
        if not self.research_only or not self.execution_prohibited:
            raise ValueError("Advisory outcome policies are historical research only and execution prohibited")
        payload = self.canonical_payload()
        digest = canonical_json_sha256(payload)
        if self.label_policy_bundle_hash is not None and self.label_policy_bundle_hash != digest:
            raise ValueError("label_policy_bundle_hash does not match canonical payload")
        expected_id = f"lpb_{digest[:20]}"
        if self.label_policy_bundle_id is not None and self.label_policy_bundle_id != expected_id:
            raise ValueError("label_policy_bundle_id does not match canonical payload")
        object.__setattr__(self, "label_policy_bundle_hash", digest)
        object.__setattr__(self, "label_policy_bundle_id", expected_id)
        return self

    def canonical_payload(self) -> dict[str, Any]:
        return canonicalize(
            self.model_dump(
                mode="python",
                exclude={"label_policy_bundle_id", "label_policy_bundle_hash"},
            )
        )


class EntryExecutionPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = "advisory_phase1_entry_execution_policy_v1"
    policy_id: str = Field(min_length=1, max_length=160)
    policy_hash: str | None = Field(default=None, min_length=64, max_length=64)
    entry_basis: EntryBasis
    exit_basis: ExitBasis
    market_timezone: str = "Asia/Shanghai"
    entry_time: time
    exit_time: time

    @field_validator("policy_hash")
    @classmethod
    def _policy_hash(cls, value: str | None) -> str | None:
        return _require_sha256(value, field_name="entry_execution_policy_hash") if value is not None else None

    @model_validator(mode="after")
    def _v1_contract(self) -> "EntryExecutionPolicy":
        if self.schema_version != "advisory_phase1_entry_execution_policy_v1":
            raise ValueError("unsupported entry execution policy schema version")
        if self.entry_basis is not EntryBasis.NEXT_OPEN_EXECUTABLE_V1:
            raise ValueError("Phase 1C-3 supports only the frozen next-open entry policy")
        if self.market_timezone != "Asia/Shanghai":
            raise ValueError("entry execution timestamps must use Asia/Shanghai")
        if self.entry_time != time(9, 30):
            raise ValueError("NEXT_OPEN_EXECUTABLE_V1 requires the frozen 09:30 entry timestamp")
        expected_exit_time = time(15, 0) if self.exit_basis is ExitBasis.HORIZON_CLOSE_V1 else time(9, 30)
        if self.exit_time != expected_exit_time:
            raise ValueError("exit timestamp does not match the frozen exit basis")
        _bind_content_hash(self, field_name="policy_hash")
        return self


class MarketDataPolicy(BaseModel):
    """Explicit price, adjustment, corporate-action and symbol semantics."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = "advisory_phase1_market_data_policy_v1"
    price_policy_hash: str = Field(min_length=64, max_length=64)
    adjustment_policy_hash: str = Field(min_length=64, max_length=64)
    corporate_action_policy_hash: str = Field(min_length=64, max_length=64)
    symbol_normalization_policy_hash: str = Field(min_length=64, max_length=64)
    price_reference_basis: str = "RAW_LI_TO_YUAN_V1"
    adjustment_basis: str = "CORPORATE_ACTION_NORMALIZED_FROM_RAW_V1"
    currency: str = "CNY"
    price_unit: str = "yuan"
    storage_scale: str = "li_to_yuan_1000"

    @field_validator(
        "price_policy_hash",
        "adjustment_policy_hash",
        "corporate_action_policy_hash",
        "symbol_normalization_policy_hash",
    )
    @classmethod
    def _hashes(cls, value: str, info) -> str:  # type: ignore[no-untyped-def]
        return _require_sha256(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _frozen_semantics(self) -> "MarketDataPolicy":
        if self.schema_version != "advisory_phase1_market_data_policy_v1":
            raise ValueError("unsupported market-data policy schema version")
        if (
            self.price_reference_basis != "RAW_LI_TO_YUAN_V1"
            or self.adjustment_basis != "CORPORATE_ACTION_NORMALIZED_FROM_RAW_V1"
            or self.currency != "CNY"
            or self.price_unit != "yuan"
            or self.storage_scale != "li_to_yuan_1000"
        ):
            raise ValueError("market-data policy must use frozen Phase 1C-3 price and adjustment semantics")
        return self


class CostPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = "advisory_phase1_cost_policy_v1"
    policy_id: str = Field(min_length=1, max_length=160)
    policy_hash: str | None = Field(default=None, min_length=64, max_length=64)
    commission_buy_rate: Decimal = Field(ge=Decimal("0"))
    commission_sell_rate: Decimal = Field(ge=Decimal("0"))
    minimum_commission: Decimal = Field(ge=Decimal("0"))
    stamp_duty_sell_rate: Decimal = Field(ge=Decimal("0"))
    transfer_fee_buy_rate: Decimal = Field(ge=Decimal("0"))
    transfer_fee_sell_rate: Decimal = Field(ge=Decimal("0"))
    slippage_bps: Decimal = Field(ge=Decimal("0"))
    lot_size: int = Field(ge=1)
    quantity_rounding: str = "LOT_FLOOR_V1"
    fee_rounding: str = "ROUND_HALF_EVEN"

    @field_validator("policy_hash")
    @classmethod
    def _policy_hash(cls, value: str | None) -> str | None:
        return _require_sha256(value, field_name="cost_policy_hash") if value is not None else None

    @model_validator(mode="after")
    def _frozen_semantics(self) -> "CostPolicy":
        if self.schema_version != "advisory_phase1_cost_policy_v1":
            raise ValueError("unsupported cost policy schema version")
        if self.quantity_rounding != "LOT_FLOOR_V1" or self.fee_rounding != "ROUND_HALF_EVEN":
            raise ValueError("cost policy must explicitly use Phase 1C-3 fixed rounding semantics")
        _bind_content_hash(self, field_name="policy_hash")
        return self


class BenchmarkPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = "advisory_phase1_benchmark_policy_v1"
    policy_id: str = "PIT_ELIGIBLE_UNIVERSE_EQ_WEIGHT_TOTAL_RETURN_V1"
    policy_hash: str | None = Field(default=None, min_length=64, max_length=64)
    universe_layer: str = Field(min_length=1, max_length=160)
    frozen_weight_policy: str = "T_CUTOFF_FROZEN_WEIGHT_V1"

    @field_validator("policy_hash")
    @classmethod
    def _policy_hash(cls, value: str | None) -> str | None:
        return _require_sha256(value, field_name="benchmark_policy_hash") if value is not None else None

    @model_validator(mode="after")
    def _frozen_semantics(self) -> "BenchmarkPolicy":
        if self.schema_version != "advisory_phase1_benchmark_policy_v1":
            raise ValueError("unsupported benchmark policy schema version")
        if self.policy_id != "PIT_ELIGIBLE_UNIVERSE_EQ_WEIGHT_TOTAL_RETURN_V1":
            raise ValueError("Phase 1C-3 benchmark identity must be PIT eligible equal weight total return")
        if self.frozen_weight_policy != "T_CUTOFF_FROZEN_WEIGHT_V1":
            raise ValueError("benchmark weights must be frozen at the decision cutoff")
        _bind_content_hash(self, field_name="policy_hash")
        return self


class CashReturnPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = "advisory_phase1_cash_return_policy_v1"
    policy_id: CashReturnRule
    policy_hash: str | None = Field(default=None, min_length=64, max_length=64)
    cash_return_rate: Decimal = Field(gt=Decimal("-1"))

    @field_validator("policy_hash")
    @classmethod
    def _policy_hash(cls, value: str | None) -> str | None:
        return _require_sha256(value, field_name="cash_return_policy_hash") if value is not None else None

    @model_validator(mode="after")
    def _explicit_rate(self) -> "CashReturnPolicy":
        if self.schema_version != "advisory_phase1_cash_return_policy_v1":
            raise ValueError("unsupported cash return policy schema version")
        if self.policy_id is CashReturnRule.CASH_RETURN_ZERO_V1 and self.cash_return_rate != Decimal("0"):
            raise ValueError("CASH_RETURN_ZERO_V1 requires an explicit zero rate")
        _bind_content_hash(self, field_name="policy_hash")
        return self


class BarrierPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = "advisory_phase1_barrier_policy_v1"
    policy_id: str = Field(min_length=1, max_length=160)
    policy_hash: str | None = Field(default=None, min_length=64, max_length=64)
    target_return: Decimal = Field(gt=Decimal("0"))
    stop_return: Decimal = Field(lt=Decimal("0"))
    order_policy: str = "ORDER_AMBIGUOUS_ON_SAME_BAR_V1"

    @field_validator("policy_hash")
    @classmethod
    def _policy_hash(cls, value: str | None) -> str | None:
        return _require_sha256(value, field_name="barrier_policy_hash") if value is not None else None

    @model_validator(mode="after")
    def _frozen_order(self) -> "BarrierPolicy":
        if self.schema_version != "advisory_phase1_barrier_policy_v1":
            raise ValueError("unsupported barrier policy schema version")
        if self.order_policy != "ORDER_AMBIGUOUS_ON_SAME_BAR_V1":
            raise ValueError("barrier policy cannot choose target-first or stop-first")
        _bind_content_hash(self, field_name="policy_hash")
        return self


class TerminalPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = "advisory_phase1_terminal_policy_v1"
    policy_id: str = Field(min_length=1, max_length=160)
    policy_hash: str | None = Field(default=None, min_length=64, max_length=64)
    terminal_return_rule: str = Field(min_length=1, max_length=160)
    censor_rule: str = Field(min_length=1, max_length=160)

    @field_validator("policy_hash")
    @classmethod
    def _policy_hash(cls, value: str | None) -> str | None:
        return _require_sha256(value, field_name="terminal_return_policy_hash") if value is not None else None

    @model_validator(mode="after")
    def _schema(self) -> "TerminalPolicy":
        if self.schema_version != "advisory_phase1_terminal_policy_v1":
            raise ValueError("unsupported terminal policy schema version")
        if self.terminal_return_rule != "EXACT_SETTLEMENT_OR_UNAVAILABLE_V1":
            raise ValueError("unsupported terminal return rule")
        if self.censor_rule != "EXPLICIT_RIGHT_CENSOR_REASON_V1":
            raise ValueError("unsupported terminal censor rule")
        _bind_content_hash(self, field_name="policy_hash")
        return self


class OutcomePolicySet(BaseModel):
    """Executable policy values whose identities must match the frozen bundle."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = OUTCOME_POLICY_SET_SCHEMA_VERSION
    bundle: LabelPolicyBundle
    calendar: TradingCalendar
    market_data: MarketDataPolicy
    execution: EntryExecutionPolicy
    cost: CostPolicy
    benchmark: BenchmarkPolicy
    cash_return: CashReturnPolicy
    barrier: BarrierPolicy
    terminal: TerminalPolicy

    @model_validator(mode="after")
    def _identity_closure(self) -> "OutcomePolicySet":
        if self.schema_version != OUTCOME_POLICY_SET_SCHEMA_VERSION:
            raise ValueError("unsupported outcome policy set schema version")
        if self.bundle.calendar_version != self.calendar.calendar_version or self.bundle.calendar_hash != self.calendar.calendar_hash:
            raise ValueError("policy bundle and trading calendar identity do not match")
        identities = (
            (self.bundle.price_policy_hash, self.market_data.price_policy_hash, "price"),
            (self.bundle.adjustment_policy_hash, self.market_data.adjustment_policy_hash, "adjustment"),
            (self.bundle.corporate_action_policy_hash, self.market_data.corporate_action_policy_hash, "corporate action"),
            (self.bundle.symbol_normalization_policy_hash, self.market_data.symbol_normalization_policy_hash, "symbol normalization"),
            (self.bundle.entry_execution_policy_hash, self.execution.policy_hash, "entry execution"),
            (self.bundle.cost_policy_hash, self.cost.policy_hash, "cost"),
            (self.bundle.benchmark_policy_hash, self.benchmark.policy_hash, "benchmark"),
            (self.bundle.cash_return_policy_hash, self.cash_return.policy_hash, "cash return"),
            (self.bundle.barrier_policy_hash, self.barrier.policy_hash, "barrier"),
            (self.bundle.terminal_return_policy_hash, self.terminal.policy_hash, "terminal"),
        )
        for expected, actual, label in identities:
            if expected != actual:
                raise ValueError(f"policy bundle {label} hash does not match executable policy")
        return self
