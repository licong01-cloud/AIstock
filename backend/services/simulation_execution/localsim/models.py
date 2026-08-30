"""Durable LocalSIM execution, economic, and projection contracts."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.simulation_data.daily_context import canonical_json_sha256
from backend.services.trading_core.models import OrderSide, PositionLot


class LocalSimExecutionRuntimeStatus(str, Enum):
    WAITING_FOR_CAUSAL_BAR = "WAITING_FOR_CAUSAL_BAR"
    WAITING_FOR_MARKET_DATA = "WAITING_FOR_MARKET_DATA"
    WAITING_FOR_MARKET_STATE = "WAITING_FOR_MARKET_STATE"
    WAITING_FOR_CAPITAL = "WAITING_FOR_CAPITAL"
    ACTIVE = "ACTIVE"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED_WITH_RESIDUAL = "EXPIRED_WITH_RESIDUAL"
    FAILED_TERMINAL = "FAILED_TERMINAL"


class LocalSimMarketMarkProvenance(str, Enum):
    REALTIME_MINUTE_CLOSE = "REALTIME_MINUTE_CLOSE"
    HISTORICAL_MINUTE_CLOSE = "HISTORICAL_MINUTE_CLOSE"
    SUSPENDED_PREV_CLOSE = "SUSPENDED_PREV_CLOSE"


class LocalSimProjectionOutboxStatus(str, Enum):
    PENDING = "PENDING"
    PROJECTION_RETRYABLE = "PROJECTION_RETRYABLE"
    PROJECTED = "PROJECTED"


class LocalSimMarketMarkV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["local_sim_market_mark_v1"] = "local_sim_market_mark_v1"
    symbol: str
    price: float
    as_of_time: datetime
    source: str
    provenance: LocalSimMarketMarkProvenance
    reuse_reason_code: str | None = None
    source_error_reason_code: str | None = None
    reused_from_mark_hash: str | None = None
    mark_hash: str = ""

    @model_validator(mode="after")
    def _validate_identity_and_hash(self) -> "LocalSimMarketMarkV1":
        symbol = str(self.symbol or "").strip()
        source = str(self.source or "").strip()
        if not symbol or not source:
            raise ValueError("LocalSIM market mark symbol and source are required")
        if not math.isfinite(float(self.price)) or float(self.price) <= 0:
            raise ValueError("LocalSIM market mark price must be finite and positive")
        normalized_reuse_fields: list[str | None] = []
        for field_name in (
            "reuse_reason_code",
            "source_error_reason_code",
            "reused_from_mark_hash",
        ):
            raw_value = getattr(self, field_name)
            if raw_value is None:
                normalized_reuse_fields.append(None)
                continue
            normalized = str(raw_value).strip()
            if not normalized:
                raise ValueError(f"{field_name} cannot be blank")
            object.__setattr__(self, field_name, normalized)
            normalized_reuse_fields.append(normalized)
        reuse_fields = tuple(normalized_reuse_fields)
        if any(reuse_fields) and not all(reuse_fields):
            raise ValueError("reused LocalSIM market marks require complete reuse evidence")
        if all(reuse_fields) and self.provenance != LocalSimMarketMarkProvenance.REALTIME_MINUTE_CLOSE:
            raise ValueError("only realtime LocalSIM market marks can carry reuse evidence")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "source", source)
        expected = canonical_json_sha256(self.model_dump(mode="json", exclude={"mark_hash"}))
        if self.mark_hash and self.mark_hash != expected:
            raise ValueError("mark_hash does not match LocalSimMarketMarkV1 payload")
        object.__setattr__(self, "mark_hash", expected)
        return self


class LocalSimEconomicReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["local_sim_economic_receipt_v1"] = "local_sim_economic_receipt_v1"
    receipt_id: str = ""
    run_id: str
    binding_id: str
    trade_date: date
    plan_id: str
    generation: int = Field(gt=0)
    economic_facts: dict[str, Any]
    economic_hash: str = ""
    idempotency_key: str = ""
    receipt_hash: str = ""
    committed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def _validate_identity_and_hashes(self) -> "LocalSimEconomicReceiptV1":
        economic_hash = canonical_json_sha256(self.economic_facts)
        if self.economic_hash and self.economic_hash != economic_hash:
            raise ValueError("economic_hash does not match LocalSIM economic facts")
        object.__setattr__(self, "economic_hash", economic_hash)
        idempotency_key = canonical_json_sha256(
            ["local_sim_economic_event_v1", self.run_id, self.plan_id, economic_hash]
        )
        if self.idempotency_key and self.idempotency_key != idempotency_key:
            raise ValueError("idempotency_key does not match LocalSIM economic event")
        object.__setattr__(self, "idempotency_key", idempotency_key)
        receipt_id = "lsec_" + canonical_json_sha256([self.run_id, self.generation, idempotency_key])
        if self.receipt_id and self.receipt_id != receipt_id:
            raise ValueError("receipt_id does not match LocalSIM economic receipt identity")
        object.__setattr__(self, "receipt_id", receipt_id)
        receipt_hash = canonical_json_sha256(self.model_dump(mode="json", exclude={"receipt_hash", "committed_at"}))
        if self.receipt_hash and self.receipt_hash != receipt_hash:
            raise ValueError("receipt_hash does not match LocalSIM economic receipt")
        object.__setattr__(self, "receipt_hash", receipt_hash)
        return self


class LocalSimProjectionOutboxV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["local_sim_projection_outbox_v1"] = "local_sim_projection_outbox_v1"
    outbox_id: str = ""
    receipt_id: str
    run_id: str
    plan_id: str
    generation: int = Field(gt=0)
    economic_hash: str
    projection_payload: dict[str, Any]
    projection_payload_hash: str = ""
    status: LocalSimProjectionOutboxStatus = LocalSimProjectionOutboxStatus.PENDING
    attempt_count: int = Field(default=0, ge=0)
    last_error: dict[str, Any] | None = None
    outbox_hash: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def _validate_identity_and_hashes(self) -> "LocalSimProjectionOutboxV1":
        payload_hash = canonical_json_sha256(self.projection_payload)
        if self.projection_payload_hash and self.projection_payload_hash != payload_hash:
            raise ValueError("projection_payload_hash does not match LocalSIM outbox payload")
        object.__setattr__(self, "projection_payload_hash", payload_hash)
        outbox_id = "lsout_" + canonical_json_sha256(
            [self.run_id, self.generation, self.receipt_id, self.economic_hash, payload_hash]
        )
        if self.outbox_id and self.outbox_id != outbox_id:
            raise ValueError("outbox_id does not match LocalSIM projection outbox identity")
        object.__setattr__(self, "outbox_id", outbox_id)
        outbox_hash = canonical_json_sha256(
            {
                "schema_version": self.schema_version,
                "outbox_id": outbox_id,
                "receipt_id": self.receipt_id,
                "run_id": self.run_id,
                "plan_id": self.plan_id,
                "generation": self.generation,
                "economic_hash": self.economic_hash,
                "projection_payload_hash": payload_hash,
            }
        )
        if self.outbox_hash and self.outbox_hash != outbox_hash:
            raise ValueError("outbox_hash does not match LocalSIM projection outbox")
        object.__setattr__(self, "outbox_hash", outbox_hash)
        return self


class LocalSimProjectionReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["local_sim_projection_receipt_v1"] = "local_sim_projection_receipt_v1"
    projection_receipt_id: str = ""
    outbox_id: str
    run_id: str
    generation: int = Field(gt=0)
    economic_hash: str
    projection_payload_hash: str
    projection_hash: str
    projected_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    receipt_hash: str = ""

    @model_validator(mode="after")
    def _validate_identity_and_hashes(self) -> "LocalSimProjectionReceiptV1":
        receipt_id = "lsproj_" + canonical_json_sha256(
            [self.run_id, self.generation, self.outbox_id, self.projection_hash]
        )
        if self.projection_receipt_id and self.projection_receipt_id != receipt_id:
            raise ValueError("projection_receipt_id does not match LocalSIM projection identity")
        object.__setattr__(self, "projection_receipt_id", receipt_id)
        receipt_hash = canonical_json_sha256(self.model_dump(mode="json", exclude={"receipt_hash", "projected_at"}))
        if self.receipt_hash and self.receipt_hash != receipt_hash:
            raise ValueError("receipt_hash does not match LocalSIM projection receipt")
        object.__setattr__(self, "receipt_hash", receipt_hash)
        return self


LOCAL_SIM_TERMINAL_RUNTIME_STATUSES = frozenset(
    {
        LocalSimExecutionRuntimeStatus.FILLED,
        LocalSimExecutionRuntimeStatus.CANCELLED,
        LocalSimExecutionRuntimeStatus.REJECTED,
        LocalSimExecutionRuntimeStatus.EXPIRED_WITH_RESIDUAL,
        LocalSimExecutionRuntimeStatus.FAILED_TERMINAL,
    }
)


class LocalSimExecutionStateV1(BaseModel):
    """Durable per-intent state for the LocalSIM minute loop."""

    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["local_sim_execution_state_v1"] = "local_sim_execution_state_v1"
    state_id: str = ""
    run_id: str
    binding_id: str
    trade_date: date
    plan_id: str
    intent_id: str
    algo_instance_id: str
    portfolio_id: str
    order_id: str
    symbol: str
    side: OrderSide
    total_quantity: int = Field(gt=0)
    filled_quantity: int = Field(ge=0)
    remaining_quantity: int = Field(ge=0)
    algo_code: str
    order_status: str
    runtime_status: LocalSimExecutionRuntimeStatus
    algo_state: dict[str, Any] = Field(default_factory=dict)
    plan: dict[str, Any] | None = None
    plan_sha256: str | None = None
    schedule_version: str
    next_slice_index: int = Field(default=0, ge=0)
    causality_cursor: datetime
    last_processed_bar_time: datetime | None = None
    last_applied_bar_identity: str | None = None
    market_session: str | None = None
    latest_order_sequence: int = Field(default=0, ge=0)
    latest_fill_sequence: int = Field(default=0, ge=0)
    latest_cash_sequence: int = Field(default=0, ge=0)
    latest_position_sequence: int = Field(default=0, ge=0)
    terminal_reason: str | None = None
    residual_classification: str | None = None
    waiting_reason_code: str | None = None
    waiting_context: dict[str, Any] | None = None
    sequence: int = Field(default=0, ge=0)
    idempotency_key: str
    state_hash: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def _identity_quantity_and_hash_are_canonical(self) -> "LocalSimExecutionStateV1":
        if self.filled_quantity + self.remaining_quantity != self.total_quantity:
            raise ValueError("filled_quantity + remaining_quantity must equal total_quantity")
        expected_state_id = local_sim_execution_state_id(
            binding_id=self.binding_id,
            trade_date=self.trade_date,
            plan_id=self.plan_id,
            intent_id=self.intent_id,
            algo_instance_id=self.algo_instance_id,
        )
        if self.state_id and self.state_id != expected_state_id:
            raise ValueError("state_id does not match LocalSimExecutionStateV1 identity")
        object.__setattr__(self, "state_id", expected_state_id)
        if self.plan is None:
            if self.plan_sha256 is not None:
                raise ValueError("plan_sha256 requires plan")
        else:
            expected_plan_sha256 = canonical_json_sha256(self.plan)
            if self.plan_sha256 is not None and self.plan_sha256 != expected_plan_sha256:
                raise ValueError("plan_sha256 does not match plan")
            object.__setattr__(self, "plan_sha256", expected_plan_sha256)
        if self.runtime_status == LocalSimExecutionRuntimeStatus.FILLED and self.remaining_quantity != 0:
            raise ValueError("FILLED LocalSIM state cannot retain remaining quantity")
        if self.runtime_status == LocalSimExecutionRuntimeStatus.EXPIRED_WITH_RESIDUAL:
            if self.remaining_quantity <= 0:
                raise ValueError("EXPIRED_WITH_RESIDUAL requires remaining quantity")
            if not self.terminal_reason or not self.residual_classification:
                raise ValueError("EXPIRED_WITH_RESIDUAL requires terminal reason and residual classification")
        if (
            self.runtime_status
            in {
                LocalSimExecutionRuntimeStatus.WAITING_FOR_MARKET_DATA,
                LocalSimExecutionRuntimeStatus.WAITING_FOR_MARKET_STATE,
                LocalSimExecutionRuntimeStatus.WAITING_FOR_CAPITAL,
            }
            and not self.waiting_reason_code
        ):
            raise ValueError(f"{self.runtime_status.value} requires waiting_reason_code")
        if self.runtime_status == LocalSimExecutionRuntimeStatus.FAILED_TERMINAL:
            if not self.terminal_reason or not self.residual_classification:
                raise ValueError("FAILED_TERMINAL requires terminal reason and residual classification")
        expected_hash = local_sim_execution_state_hash(self)
        if self.state_hash and self.state_hash != expected_hash:
            raise ValueError("state_hash does not match LocalSimExecutionStateV1 payload")
        object.__setattr__(self, "state_hash", expected_hash)
        return self

    @property
    def is_terminal(self) -> bool:
        return self.runtime_status in LOCAL_SIM_TERMINAL_RUNTIME_STATUSES


def local_sim_execution_state_id(
    *,
    binding_id: str,
    trade_date: date,
    plan_id: str,
    intent_id: str,
    algo_instance_id: str,
) -> str:
    digest = canonical_json_sha256(
        ["localsim_execution_state_v1", binding_id, trade_date.isoformat(), plan_id, intent_id, algo_instance_id]
    )
    return f"lsstate_{digest}"


def local_sim_execution_state_hash(state: LocalSimExecutionStateV1) -> str:
    payload = state.model_dump(mode="json", exclude={"state_hash", "updated_at"})
    return canonical_json_sha256(payload)


@dataclass(frozen=True)
class LocalSimPersistenceResult:
    payload: dict[str, Any]
    positions: dict[str, PositionLot]
    marks: dict[str, float]
    cash: float
    economic_receipt_id: str
    outbox_id: str
    generation: int
    performance_payload: dict[str, Any]


__all__ = [
    "LOCAL_SIM_TERMINAL_RUNTIME_STATUSES",
    "LocalSimEconomicReceiptV1",
    "LocalSimExecutionRuntimeStatus",
    "LocalSimExecutionStateV1",
    "LocalSimMarketMarkProvenance",
    "LocalSimMarketMarkV1",
    "LocalSimPersistenceResult",
    "LocalSimProjectionOutboxStatus",
    "LocalSimProjectionOutboxV1",
    "LocalSimProjectionReceiptV1",
    "local_sim_execution_state_hash",
    "local_sim_execution_state_id",
]
